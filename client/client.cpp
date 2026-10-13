// P2P client: command line, tracker connection (with failover), local hashing for uploads.
// The peer server lives in peer_server.cpp and the downloader in downloader.cpp.
#include "client.h"
#include "net_util.h"
#include "tracker_info.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <csignal>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <iostream>
#include <mutex>
#include <readline/history.h>
#include <readline/readline.h>

using namespace std;

string myIP = "127.0.0.1";
int myListenPort = -1;

// ---------------------------------------------------------------------------
// Session (logged-in user + token)
// ---------------------------------------------------------------------------

static string currentUser, sessionToken;
static mutex sessionMutex;

void setSession(const string &user, const string &token) {
    lock_guard<mutex> lk(sessionMutex);
    currentUser = user;
    sessionToken = token;
}

void getSession(string &user, string &token) {
    lock_guard<mutex> lk(sessionMutex);
    user = currentUser;
    token = sessionToken;
}

// ---------------------------------------------------------------------------
// Tracker connection
// ---------------------------------------------------------------------------

static vector<TrackerEndpoint> trackers;
static int currentTrackerIndex = 0;
static int trackerFd = -1;
static LineReader trackerReader;
static mutex trackerMutex;  // serialises request/response exchanges on the single tracker socket

static void dropTrackerLocked() {
    if (trackerFd >= 0) close(trackerFd);
    trackerFd = -1;
}

static bool connectToTrackerLocked(int index) {
    dropTrackerLocked();

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(trackers[index].port);
    if (inet_pton(AF_INET, trackers[index].ip.c_str(), &serv_addr.sin_addr) <= 0) {
        fprintf(stderr, "Invalid tracker address %s\n", trackers[index].ip.c_str());
        close(fd);
        return false;
    }

    // Responses are length-prefixed, so the timeout is only a liveness check on the tracker, not
    // a guess about how long a command takes.
    struct timeval tv;
    tv.tv_sec = 30;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
#ifdef SO_NOSIGPIPE
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif

    if (connect(fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        close(fd);
        return false;
    }

    int keepalive = 1;
    setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(keepalive));

    trackerFd = fd;
    trackerReader.reset(fd);
    currentTrackerIndex = index;
    printf("\n[TRACKER] Connected to tracker %s:%d\n", trackers[index].ip.c_str(), trackers[index].port);
    return true;
}

static bool connectToAnyTrackerLocked() {
    if (connectToTrackerLocked(currentTrackerIndex)) return true;

    for (size_t i = 0; i < trackers.size(); i++) {
        if ((int)i == currentTrackerIndex) continue;
        printf("[TRACKER] Trying alternate tracker %zu...\n", i + 1);
        if (connectToTrackerLocked((int)i)) return true;
    }
    printf("[TRACKER] ERROR: Could not connect to any tracker!\n");
    return false;
}

static bool exchangeLocked(const string &cmd, string &response, int *status = nullptr) {
    if (trackerFd < 0) return false;
    if (!sendAll(trackerFd, cmd + "\n")) return false;
    return recvFramed(trackerReader, response, status);
}

// After failing over, prove to the new tracker that we own the session (token issued at login).
static void restoreSessionLocked() {
    string user, token;
    getSession(user, token);
    if (user.empty()) return;

    string resp;
    string cmd = "restore_session " + user + " " + token + " " + myIP + ":" + to_string(myListenPort);
    if (exchangeLocked(cmd, resp) && resp.compare(0, 16, "Session restored") == 0) {
        printf("[TRACKER] Session restored for user: %s\n", user.c_str());
    } else {
        printf("[TRACKER] Could not restore session (%s). Please login again.\n",
               resp.empty() ? "no reply" : resp.substr(0, resp.find('\n')).c_str());
        setSession("", "");
    }
}

bool trackerRpc(const string &cmd, string &response, int *status) {
    lock_guard<mutex> lk(trackerMutex);

    // Try the current tracker, then fail over to the others (one full sweep).
    for (size_t attempt = 0; attempt <= trackers.size(); attempt++) {
        if (trackerFd < 0) {
            if (!connectToAnyTrackerLocked()) return false;
            restoreSessionLocked();
        }
        if (exchangeLocked(cmd, response, status)) return true;

        printf("\n[TRACKER] Connection to tracker lost. Attempting failover...\n");
        dropTrackerLocked();
    }
    return false;
}

// ---------------------------------------------------------------------------
// Command parsing helpers
// ---------------------------------------------------------------------------

// Whitespace separated tokens; "double" or 'single' quotes group a token that contains spaces.
static vector<string> tokenize(const string &line) {
    vector<string> out;
    string cur;
    bool inToken = false;
    char quote = 0;
    for (size_t i = 0; i < line.size(); i++) {
        char c = line[i];
        if (quote) {
            if (c == quote) quote = 0;
            else cur += c;
        } else if (c == '"' || c == '\'') {
            quote = c;
            inToken = true;
        } else if (c == '\\' && i + 1 < line.size()) {
            cur += line[++i];
            inToken = true;
        } else if (c == ' ' || c == '\t') {
            if (inToken) out.push_back(cur);
            cur.clear();
            inToken = false;
        } else {
            cur += c;
            inToken = true;
        }
    }
    if (inToken) out.push_back(cur);
    return out;
}

// Tokens [from, end) rejoined with single spaces: lets an unquoted path contain spaces.
static string restOf(const vector<string> &t, size_t from) {
    string out;
    for (size_t i = from; i < t.size(); i++) {
        if (i > from) out += ' ';
        out += t[i];
    }
    return out;
}

static string expandHome(const string &p) {
    if (p.compare(0, 2, "~/") == 0) {
        if (const char *home = getenv("HOME")) return string(home) + p.substr(1);
    }
    return p;
}

static string baseName(const string &path) {
    size_t slash = path.find_last_of('/');
    return slash == string::npos ? path : path.substr(slash + 1);
}

static string absolutePath(const string &path) {
    char buf[4096];
    if (realpath(path.c_str(), buf)) return string(buf);
    return path;
}

static bool startsWith(const string &s, const char *prefix) { return s.compare(0, strlen(prefix), prefix) == 0; }

static void printResponse(const string &resp) {
    if (resp.empty()) return;
    printf("%s", resp.c_str());
    if (resp.back() != '\n') printf("\n");
}

// ---------------------------------------------------------------------------
// Commands that need client-side work
// ---------------------------------------------------------------------------

static bool requireLogin() {
    string user, token;
    getSession(user, token);
    if (user.empty()) {
        printf("Error: No user logged in\n");
        return false;
    }
    return true;
}

// upload_file <group_id> <file_path>: hash locally, send only metadata to the tracker.
static void cmdUpload(const vector<string> &t) {
    if (t.size() < 3) {
        printf("Usage: upload_file <group_id> <file_path>\n");
        return;
    }
    if (!requireLogin()) return;

    const string &gid = t[1];
    string path = expandHome(restOf(t, 2));

    struct stat st;
    if (stat(path.c_str(), &st) != 0) {
        printf("Error: File not found\n");
        return;
    }
    if (!S_ISREG(st.st_mode)) {
        printf("Error: Not a regular file\n");
        return;
    }
    path = absolutePath(path);
    string name = baseName(path);
    if (name.find('\t') != string::npos || name.find('\n') != string::npos) {
        printf("Error: File names containing tabs or newlines are not supported\n");
        return;
    }

    printf("Hashing %s (%lld bytes)...\n", name.c_str(), (long long)st.st_size);
    fflush(stdout);
    string fileHash, err;
    vector<string> pieceHashes;
    long long size = 0;
    if (!hashFilePieces(path, fileHash, pieceHashes, size, err)) {
        printf("Error: Could not hash file (%s)\n", err.c_str());
        return;
    }

    string resp;
    string cmd = "upload_file\t" + gid + "\t" + name + "\t" + to_string(size) + "\t" + fileHash + "\t" +
                 joinStr(pieceHashes, " ");
    if (!trackerRpc(cmd, resp)) {
        printf("[ERROR] Could not communicate with any tracker. Please try again.\n");
        return;
    }
    // Only serve the file once the tracker has accepted it.
    if (startsWith(resp, "File uploaded successfully")) registerLocalFile(gid, name, path);
    printResponse(resp);
}

// become_seeder <group_id> <file_name> <file_path>: prove we hold an identical copy, then register.
static void cmdBecomeSeeder(const vector<string> &t) {
    if (t.size() < 4) {
        printf("Usage: become_seeder <group_id> <file_name> <file_path>\n");
        return;
    }
    if (!requireLogin()) return;

    const string &gid = t[1];
    const string &name = t[2];
    string path = expandHome(restOf(t, 3));

    string resp;
    if (!trackerRpc("file_info\t" + gid + "\t" + name, resp)) {
        printf("[ERROR] Could not communicate with any tracker. Please try again.\n");
        return;
    }
    if (!startsWith(resp, "FILEINFO ")) {
        printResponse(resp);
        return;
    }
    vector<string> info = splitStr(resp.substr(0, resp.find('\n')), ' ');
    string expectedHash = info.size() >= 4 ? info[3] : "";

    struct stat st;
    if (stat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
        printf("Error: File not found at specified path\n");
        return;
    }
    path = absolutePath(path);

    printf("Verifying %s...\n", path.c_str());
    fflush(stdout);
    string actual;
    if (!hashFile(path, actual) || actual != expectedHash) {
        printf("Error: File hash mismatch. File may be corrupted.\n");
        return;
    }

    // Register locally first so peers can fetch as soon as the tracker lists us.
    registerLocalFile(gid, name, path);
    if (!trackerRpc("add_seeder\t" + gid + "\t" + name + "\t" + expectedHash, resp)) {
        unregisterLocalFile(gid, name);
        printf("[ERROR] Could not communicate with any tracker. Please try again.\n");
        return;
    }
    // "Already seeding" keeps the local registration: that is how a restarted client re-attaches
    // a file the tracker still lists it as seeding.
    if (!startsWith(resp, "Now seeding") && !startsWith(resp, "Already seeding")) unregisterLocalFile(gid, name);
    printResponse(resp);
}

static void cmdStopShare(const vector<string> &t) {
    if (t.size() < 3) {
        printf("Usage: stop_share <group_id> <file_name>\n");
        return;
    }
    string resp;
    if (!trackerRpc("stop_share\t" + t[1] + "\t" + restOf(t, 2), resp)) {
        printf("[ERROR] Could not communicate with any tracker. Please try again.\n");
        return;
    }
    if (startsWith(resp, "Stopped sharing")) unregisterLocalFile(t[1], restOf(t, 2));
    printResponse(resp);
}

static void cmdDownload(const vector<string> &t) {
    if (t.size() < 4) {
        printf("Usage: download_file <group_id> <file_name> <destination_path>\n");
        return;
    }
    string message;
    startDownload(t[1], t[2], expandHome(restOf(t, 3)), message);
    printResponse(message);
}

static void cmdLogin(const string &line, const vector<string> &t) {
    // Forward "login <user> <pass> <ip:port>"; the tracker learns where our peer server listens.
    string cmd = t.size() >= 3 ? "login " + t[1] + " " + t[2] + " " + myIP + ":" + to_string(myListenPort) : line;
    string resp;
    if (!trackerRpc(cmd, resp)) {
        printf("[ERROR] Could not communicate with any tracker. Please try again.\n");
        return;
    }

    // Pull the session token out of the reply; the user does not need to see it.
    string visible;
    string token;
    for (const string &ln : splitStr(resp, '\n')) {
        if (startsWith(ln, "TOKEN ")) token = ln.substr(6);
        else if (!ln.empty()) visible += ln + "\n";
    }
    if (startsWith(resp, "Login successful") && !token.empty()) setSession(t[1], token);
    printResponse(visible);
}

static void printHelp() {
    printf("\nAvailable commands:\n");
    printf("  create_user <user_id> <password>\n");
    printf("  login <user_id> <password>\n");
    printf("  logout\n");
    printf("  create_group <group_id>\n");
    printf("  join_group <group_id>\n");
    printf("  leave_group <group_id>\n");
    printf("  list_groups\n");
    printf("  list_requests <group_id>\n");
    printf("  accept_request <group_id> <user_id>\n");
    printf("  upload_file <group_id> <file_path>\n");
    printf("  list_files <group_id>\n");
    printf("  download_file <group_id> <file_name> <destination_path>\n");
    printf("  show_downloads\n");
    printf("  stop_share <group_id> <file_name>\n");
    printf("  become_seeder <group_id> <file_name> <file_path>\n");
    printf("  exit\n");
    printf("Quote names that contain spaces:  download_file g \"my file.txt\" \"/tmp/my dir\"\n\n");
}

static bool parseHostPort(const string &s, string &host, int &port) {
    size_t colon = s.rfind(':');
    if (colon == string::npos || colon == 0 || colon + 1 >= s.size()) return false;
    host = s.substr(0, colon);
    port = atoi(s.c_str() + colon + 1);
    return port > 0 && port < 65536;
}

int main(int argc, char *argv[]) {
    // Writing to a closed tracker/peer socket must surface as an error, not kill the process.
    signal(SIGPIPE, SIG_IGN);
    setvbuf(stdout, NULL, _IOLBF, 0);  // keep log lines visible when output is a pipe/file

    if (argc != 3) {
        fprintf(stderr, "Usage: %s <IP>:<PORT> <tracker_info.txt>\n", argv[0]);
        fprintf(stderr, "  IP:PORT - This client's listening address and port for P2P connections\n");
        fprintf(stderr, "  tracker_info.txt - File containing tracker addresses\n");
        return argc < 2 ? 0 : 1;
    }

    if (!parseHostPort(argv[1], myIP, myListenPort)) {
        fprintf(stderr, "Invalid IP:PORT format. Use: IP:PORT\n");
        return 1;
    }
    if (!startPeerServer(myListenPort)) {
        fprintf(stderr, "Failed to setup peer listener\n");
        return 1;
    }
    printf("[P2P] Peer server started on %s:%d\n", myIP.c_str(), myListenPort);

    trackers = loadTrackerInfo(argv[2]);
    if (trackers.empty()) {
        fprintf(stderr, "ERROR: No trackers found in %s\n", argv[2]);
        return 1;
    }
    printf("Loaded %zu tracker(s) from file\n", trackers.size());
    for (size_t i = 0; i < trackers.size(); i++) {
        printf("  Tracker %zu: %s:%d\n", i + 1, trackers[i].ip.c_str(), trackers[i].port);
    }

    {
        lock_guard<mutex> lk(trackerMutex);
        if (!connectToAnyTrackerLocked()) {
            fprintf(stderr, "ERROR: Could not connect to any tracker\n");
            return 1;
        }
    }

    printf("\n=== Peer-to-Peer File Sharing Client ===\n");
    printf("Client listening on: %s:%d\n", myIP.c_str(), myListenPort);
    printf("Type 'help' for available commands, 'exit' to quit\n");
    printf("Use Up/Down arrows for command history\n\n");

    using_history();

    while (true) {
        char *raw = readline("> ");
        if (!raw) {
            printf("\nClosing connection.\n");
            break;
        }
        string line(raw);
        free(raw);

        vector<string> t = tokenize(line);
        if (t.empty()) continue;
        add_history(line.c_str());

        const string &cmd = t[0];
        if (cmd == "exit") {
            printf("Closing connection.\n");
            break;
        } else if (cmd == "help") {
            printHelp();
        } else if (cmd == "upload_file") {
            cmdUpload(t);
        } else if (cmd == "download_file") {
            cmdDownload(t);
        } else if (cmd == "show_downloads") {
            showDownloads();
        } else if (cmd == "stop_share") {
            cmdStopShare(t);
        } else if (cmd == "become_seeder") {
            cmdBecomeSeeder(t);
        } else if (cmd == "login") {
            cmdLogin(line, t);
        } else {
            // Everything else is a plain tracker command.
            string resp;
            if (!trackerRpc(joinStr(t, " "), resp)) {
                printf("[ERROR] Could not communicate with any tracker. Please try again.\n");
                continue;
            }
            if (cmd == "logout" && startsWith(resp, "User logged out")) setSession("", "");
            printResponse(resp);
        }
    }

    lock_guard<mutex> lk(trackerMutex);
    dropTrackerLocked();
    return 0;
}
