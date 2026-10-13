#include "header.h"
#include <cctype>

// Global state, protected by dataMutex
map<string, User> users;
map<string, Group> groups;
pthread_mutex_t dataMutex = PTHREAD_MUTEX_INITIALIZER;

bool isLoggedIn(ClientSession *session) {
    if (session->currentUser.empty()) {
        session->responses.push_back("Error: No user logged in\n");
        return false;
    }
    return true;
}

// IDs end up inside "groupId:fileName" keys and tab/0x1f separated protocol fields.
static bool validId(const string &id) {
    if (id.empty() || id.size() > 64) return false;
    for (unsigned char c : id) {
        if (c < 0x21 || c == ':' || c == 0x7f) return false;
    }
    return true;
}

// Passwords are stored (and replicated) as "salt$sha256(salt + password)", never in plaintext.
static string makePassHash(const string &password) {
    string salt = randomHex(8);
    return salt + "$" + sha256Hex(salt + password);
}

static bool checkPassword(const string &stored, const string &password) {
    size_t sep = stored.find('$');
    if (sep == string::npos) return false;
    string salt = stored.substr(0, sep);
    return sha256Hex(salt + password) == stored.substr(sep + 1);
}

// Shared by "logout" and by a dropped connection so both trackers always end up in the same state.
// Caller holds dataMutex.
static void applyLogoutLocked(const string &userId) {
    if (!users.count(userId)) return;
    removeUserAsSeederFromAllFiles(userId);
    setLogin(userId, false, "", "");
    users[userId].activeSocket = -1;  // connection state is local to this tracker
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

static void createUser(const string &id, const string &pass, ClientSession *session) {
    if (!validId(id)) {
        session->responses.push_back("Error: Invalid user id\n");
        return;
    }

    string passHash = makePassHash(pass);
    {
        DataLock lock;
        if (users.count(id)) {
            session->responses.push_back("Error: User already exists\n");
            return;
        }
        putUser(id, passHash);
        session->responses.push_back("User created: " + id + "\n");
    }
}

static void login(const string &id, const string &pass, const string &peerAddr, ClientSession *session) {
    if (!session->currentUser.empty()) {
        session->responses.push_back("Error: Another user already logged in on this connection\n");
        return;
    }

    string token = randomHex(16);
    {
        DataLock lock;
        auto it = users.find(id);
        if (it == users.end() || !checkPassword(it->second.passHash, pass)) {
            session->responses.push_back("Error: Invalid user or password\n");
            return;
        }
        User &u = it->second;

        // Reject only if another live connection on this tracker owns the session. A session that
        // was merely replicated from the other tracker (activeSocket == -1) can be re-claimed with
        // the password, e.g. after the client crashed while its tracker was down.
        if (u.loggedIn && u.activeSocket >= 0 && u.activeSocket != session->socketFd) {
            session->responses.push_back("Error: User already logged in from another connection\n");
            return;
        }

        setLogin(id, true, peerAddr, token);
        u.activeSocket = session->socketFd;
        session->currentUser = id;

        // The user's previously shared files become available again.
        addUserAsSeederForUploadedFiles(id);

        session->responses.push_back("Login successful: " + id + "\n");
        session->responses.push_back("TOKEN " + token + "\n");
    }
}

// Failover: the client reconnects to this tracker and proves it owns the session with the token
// it was issued at login (the token was replicated here by the sync channel).
static void restoreSession(const string &userId, const string &token, const string &peerAddr,
                           ClientSession *session) {
    DataLock lock;
    auto it = users.find(userId);
    if (it == users.end()) {
        session->responses.push_back("Error: User not found\n");
        return;
    }
    User &u = it->second;
    if (!u.loggedIn || u.token.empty() || u.token != token) {
        session->responses.push_back("Error: Session not found. Please login again\n");
        return;
    }

    u.activeSocket = session->socketFd;
    if (u.peerAddress != peerAddr) setLogin(userId, true, peerAddr, token);
    session->currentUser = userId;
    session->responses.push_back("Session restored: " + userId + "\n");
}

static void logout(ClientSession *session) {
    if (!isLoggedIn(session)) return;

    string loggedOutUser = session->currentUser;
    {
        DataLock lock;
        applyLogoutLocked(loggedOutUser);
    }
    session->responses.push_back("User logged out: " + loggedOutUser + "\n");
    session->currentUser.clear();
}

void logoutOnDisconnect(ClientSession *session) {
    if (session->currentUser.empty()) return;

    string userId = session->currentUser;
    {
        DataLock lock;
        // Only the connection that currently owns the session may log it out; a user that has
        // already re-attached through another connection must not be kicked.
        auto it = users.find(userId);
        if (it != users.end() && it->second.loggedIn && it->second.activeSocket == session->socketFd) {
            applyLogoutLocked(userId);
        }
    }
    session->currentUser.clear();
}

static void createGroup(const string &gid, ClientSession *session) {
    if (!isLoggedIn(session)) return;
    if (!validId(gid)) {
        session->responses.push_back("Error: Invalid group id\n");
        return;
    }

    {
        DataLock lock;
        if (groups.count(gid)) {
            session->responses.push_back("Error: Group already exists\n");
            return;
        }
        putGroup(gid, session->currentUser);
        putMember(gid, session->currentUser, true);
        session->responses.push_back("Group created: " + gid + " (Owner: " + session->currentUser + ")\n");
    }
}

static void joinGroup(const string &gid, ClientSession *session) {
    if (!isLoggedIn(session)) return;

    {
        DataLock lock;
        auto it = groups.find(gid);
        if (it == groups.end()) {
            session->responses.push_back("Error: Group not found\n");
            return;
        }
        Group &g = it->second;
        if (g.members.count(session->currentUser)) {
            session->responses.push_back("Error: Already a member\n");
            return;
        }
        if (g.joinRequests.count(session->currentUser)) {
            session->responses.push_back("Error: Join request already pending\n");
            return;
        }
        putRequest(gid, session->currentUser, true);
        session->responses.push_back("Join request sent to group: " + gid + "\n");
    }
}

static void leaveGroup(const string &gid, ClientSession *session) {
    if (!isLoggedIn(session)) return;

    {
        DataLock lock;
        auto it = groups.find(gid);
        if (it == groups.end()) {
            session->responses.push_back("Error: Group not found\n");
            return;
        }
        Group &g = it->second;
        if (!g.members.count(session->currentUser)) {
            session->responses.push_back("Error: Not a member of group\n");
            return;
        }
        if (g.ownerId == session->currentUser) {
            session->responses.push_back("Error: Owner cannot leave their own group\n");
            return;
        }

        removeUserAsSeederFromGroupFiles(gid, session->currentUser);
        putMember(gid, session->currentUser, false);
        session->responses.push_back("Left group: " + gid + "\n");
    }
}

static void listGroups(ClientSession *session) {
    DataLock lock;

    if (groups.empty()) {
        session->responses.push_back("No groups available\n");
        return;
    }

    session->responses.push_back("Groups:\n");
    for (auto &g : groups) {
        session->responses.push_back("- " + g.first + " (Owner: " + g.second.ownerId +
                                     ", Members: " + to_string(g.second.members.size()) + ")\n");
    }
}

static void listRequests(const string &gid, ClientSession *session) {
    if (!isLoggedIn(session)) return;

    DataLock lock;
    auto it = groups.find(gid);
    if (it == groups.end()) {
        session->responses.push_back("Error: Group not found\n");
        return;
    }
    if (it->second.ownerId != session->currentUser) {
        session->responses.push_back("Error: Only owner can view requests\n");
        return;
    }
    if (it->second.joinRequests.empty()) {
        session->responses.push_back("No pending requests\n");
        return;
    }

    session->responses.push_back("Pending requests for group " + gid + ":\n");
    for (auto &req : it->second.joinRequests) {
        session->responses.push_back("- " + req + "\n");
    }
}

static void acceptRequest(const string &gid, const string &uid, ClientSession *session) {
    if (!isLoggedIn(session)) return;

    {
        DataLock lock;
        auto it = groups.find(gid);
        if (it == groups.end()) {
            session->responses.push_back("Error: Group not found\n");
            return;
        }
        Group &g = it->second;
        if (g.ownerId != session->currentUser) {
            session->responses.push_back("Error: Only owner can accept requests\n");
            return;
        }
        if (!g.joinRequests.count(uid)) {
            session->responses.push_back("Error: No such request\n");
            return;
        }

        putRequest(gid, uid, false);
        putMember(gid, uid, true);

        // A logged-in user who already shares files of this group is a seeder again.
        if (users.count(uid) && users[uid].loggedIn) {
            addUserAsSeederForGroupFiles(gid, uid);
        }
        session->responses.push_back("User " + uid + " added to group " + gid + "\n");
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------
//
// Plain commands are space separated. Commands that carry file names (which may contain
// spaces) are sent by the client as TAB separated fields: upload_file, file_info, add_seeder,
// stop_share.

void handle_input(const string &cmd, ClientSession *session) {
    if (cmd.find('\t') != string::npos) {
        vector<string> f = splitStr(cmd, '\t');
        const string &action = f[0];

        if (action == "upload_file") {
            uploadFile(f, session);
        } else if (action == "file_info") {
            fileInfoCommand(f, session);
        } else if (action == "add_seeder") {
            addSeederCommand(f, session);
        } else if (action == "stop_share") {
            stopShare(f, session);
        } else {
            session->responses.push_back("Unknown command: " + action + "\n");
        }
        return;
    }

    stringstream ss(cmd);
    string action;
    ss >> action;

    if (action == "create_user") {
        string uid, pass;
        ss >> uid >> pass;
        if (uid.empty() || pass.empty()) {
            session->responses.push_back("Usage: create_user <user_id> <password>\n");
            return;
        }
        createUser(uid, pass, session);
    }
    else if (action == "login") {
        string uid, pass, peerAddr;
        ss >> uid >> pass >> peerAddr;
        if (uid.empty() || pass.empty()) {
            session->responses.push_back("Usage: login <user_id> <password> [peer_address]\n");
            return;
        }
        for (unsigned char c : peerAddr) {
            if (!(isalnum(c) || c == '.' || c == ':' || c == '-' || c == '_')) {
                session->responses.push_back("Error: Invalid peer address\n");
                return;
            }
        }
        login(uid, pass, peerAddr, session);
    }
    else if (action == "restore_session") {
        string uid, token, peerAddr;
        ss >> uid >> token >> peerAddr;
        if (uid.empty() || token.empty() || peerAddr.empty()) {
            session->responses.push_back("Usage: restore_session <user_id> <token> <peer_address>\n");
            return;
        }
        restoreSession(uid, token, peerAddr, session);
    }
    else if (action == "create_group") {
        string gid;
        ss >> gid;
        if (gid.empty()) {
            session->responses.push_back("Usage: create_group <group_id>\n");
            return;
        }
        createGroup(gid, session);
    }
    else if (action == "join_group") {
        string gid;
        ss >> gid;
        if (gid.empty()) {
            session->responses.push_back("Usage: join_group <group_id>\n");
            return;
        }
        joinGroup(gid, session);
    }
    else if (action == "leave_group") {
        string gid;
        ss >> gid;
        if (gid.empty()) {
            session->responses.push_back("Usage: leave_group <group_id>\n");
            return;
        }
        leaveGroup(gid, session);
    }
    else if (action == "list_groups") {
        listGroups(session);
    }
    else if (action == "list_requests") {
        string gid;
        ss >> gid;
        if (gid.empty()) {
            session->responses.push_back("Usage: list_requests <group_id>\n");
            return;
        }
        listRequests(gid, session);
    }
    else if (action == "accept_request") {
        string gid, uid;
        ss >> gid >> uid;
        if (gid.empty() || uid.empty()) {
            session->responses.push_back("Usage: accept_request <group_id> <user_id>\n");
            return;
        }
        acceptRequest(gid, uid, session);
    }
    else if (action == "list_files") {
        string gid;
        ss >> gid;
        if (gid.empty()) {
            session->responses.push_back("Usage: list_files <group_id>\n");
            return;
        }
        listFiles(gid, session);
    }
    else if (action == "logout") {
        logout(session);
    }
    else if (action == "check_access") {
        // Used by peer servers to authorise a piece request: <group_id> <user_id> <token>
        string gid, uid, token;
        ss >> gid >> uid >> token;
        checkAccess(gid, uid, token, session);
    }
    else {
        session->responses.push_back("Unknown command: " + action + "\n");
    }
}
