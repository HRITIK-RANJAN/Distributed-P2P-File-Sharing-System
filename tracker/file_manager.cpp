// File *metadata* operations. The tracker never opens, hashes, serves or writes file contents:
// the client hashes the file and sends sizes/hashes here; downloads happen peer-to-peer.
#include "header.h"

static bool isHexHash(const string &h) {
    if (h.size() != 40) return false;
    for (char c : h) {
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
    }
    return true;
}

static bool validFileName(const string &name) {
    if (name.empty() || name.size() > 255) return false;
    for (unsigned char c : name) {
        if (c < 0x20 || c == 0x7f || c == '/') return false;
    }
    return true;
}

// args: upload_file <gid> <fileName> <size> <fileHash> <pieceHash...space separated>
void uploadFile(const vector<string> &args, ClientSession *session) {
    if (!isLoggedIn(session)) return;
    if (args.size() != 6) {
        session->responses.push_back("Usage: upload_file <group_id> <file_path>\n");
        return;
    }

    const string &gid = args[1];
    const string &fileName = args[2];

    if (!validFileName(fileName)) {
        session->responses.push_back("Error: Invalid file name\n");
        return;
    }

    FileInfo info;
    info.fileName = fileName;
    info.fileSize = atoll(args[3].c_str());
    info.fileHash = args[4];
    info.uploaderId = session->currentUser;
    if (info.fileSize < 0 || !isHexHash(info.fileHash)) {
        session->responses.push_back("Error: Malformed file metadata\n");
        return;
    }
    if (!args[5].empty()) {
        info.pieceHashes = splitStr(args[5], ' ');
    }
    info.totalPieces = (int)((info.fileSize + PIECE_SIZE - 1) / PIECE_SIZE);
    if ((int)info.pieceHashes.size() != info.totalPieces) {
        session->responses.push_back("Error: Piece count does not match file size\n");
        return;
    }
    for (const string &h : info.pieceHashes) {
        if (!isHexHash(h)) {
            session->responses.push_back("Error: Malformed piece hash\n");
            return;
        }
    }
    info.seeders.insert(session->currentUser);

    {
        DataLock lock;

        auto git = groups.find(gid);
        if (git == groups.end()) {
            session->responses.push_back("Error: Group not found\n");
            return;
        }
        if (!git->second.members.count(session->currentUser)) {
            session->responses.push_back("Error: Not a member of this group\n");
            return;
        }
        if (git->second.files.count(fileName)) {
            session->responses.push_back("Error: File already shared in this group\n");
            return;
        }

        putFile(gid, info);
        for (const string &seeder : info.seeders) putSeeder(gid, fileName, seeder, true);
        putUploaded(session->currentUser, gid, fileName, true);
    }

    session->responses.push_back("File uploaded successfully: " + fileName + "\n");
    session->responses.push_back("Total pieces: " + to_string(info.totalPieces) + "\n");
    session->responses.push_back("File hash: " + info.fileHash + "\n");
}

void listFiles(const string &gid, ClientSession *session) {
    if (!isLoggedIn(session)) return;

    DataLock lock;

    auto git = groups.find(gid);
    if (git == groups.end()) {
        session->responses.push_back("Error: Group not found\n");
        return;
    }
    if (!git->second.members.count(session->currentUser)) {
        session->responses.push_back("Error: Not a member of this group\n");
        return;
    }
    if (git->second.files.empty()) {
        session->responses.push_back("No files shared in this group\n");
        return;
    }

    session->responses.push_back("Files in group " + gid + ":\n");

    for (auto &filePair : git->second.files) {
        const FileInfo &info = filePair.second;

        string displayUploader;
        if (info.seeders.count(info.uploaderId)) {
            displayUploader = info.uploaderId;
        } else if (!info.seeders.empty()) {
            displayUploader = *(info.seeders.begin());
        } else {
            displayUploader = info.uploaderId + " (offline)";
        }

        session->responses.push_back("- " + info.fileName +
                                     " (Size: " + to_string(info.fileSize) + " bytes, " +
                                     "Pieces: " + to_string(info.totalPieces) + ", " +
                                     "Uploader: " + displayUploader + ")\n");
    }
}

// args: file_info <gid> <fileName>
// Reply (machine readable, consumed by the client's downloader):
//   FILEINFO <size> <totalPieces> <fileHash>
//   HASHES <pieceHash> <pieceHash> ...
//   PEER <ip:port> <userId>        (one per online seeder)
void fileInfoCommand(const vector<string> &args, ClientSession *session) {
    if (!isLoggedIn(session)) return;
    if (args.size() != 3) {
        session->responses.push_back("Error: Malformed request\n");
        return;
    }
    const string &gid = args[1];
    const string &fileName = args[2];

    DataLock lock;

    auto git = groups.find(gid);
    if (git == groups.end()) {
        session->responses.push_back("Error: Group not found\n");
        return;
    }
    if (!git->second.members.count(session->currentUser)) {
        session->responses.push_back("Error: Not a member of this group\n");
        return;
    }
    auto fit = git->second.files.find(fileName);
    if (fit == git->second.files.end()) {
        session->responses.push_back("Error: File not found in group\n");
        return;
    }

    const FileInfo &info = fit->second;
    session->responses.push_back("FILEINFO " + to_string(info.fileSize) + " " +
                                 to_string(info.totalPieces) + " " + info.fileHash + "\n");
    session->responses.push_back("HASHES " + joinStr(info.pieceHashes, " ") + "\n");
    for (const string &seeder : info.seeders) {
        auto uit = users.find(seeder);
        if (uit != users.end() && uit->second.loggedIn && !uit->second.peerAddress.empty()) {
            session->responses.push_back("PEER " + uit->second.peerAddress + " " + seeder + "\n");
        }
    }
}

// args: add_seeder <gid> <fileName> <fileHash>
// Sent by a client that holds a complete, verified copy (after a download or become_seeder).
void addSeederCommand(const vector<string> &args, ClientSession *session) {
    if (!isLoggedIn(session)) return;
    if (args.size() != 4) {
        session->responses.push_back("Error: Malformed request\n");
        return;
    }
    const string &gid = args[1];
    const string &fileName = args[2];
    const string &fileHash = args[3];

    {
        DataLock lock;

        auto git = groups.find(gid);
        if (git == groups.end()) {
            session->responses.push_back("Error: Group not found\n");
            return;
        }
        if (!git->second.members.count(session->currentUser)) {
            session->responses.push_back("Error: Not a member of this group\n");
            return;
        }
        auto fit = git->second.files.find(fileName);
        if (fit == git->second.files.end()) {
            session->responses.push_back("Error: File not found in group\n");
            return;
        }

        // The entry is only touched while dataMutex is held, so a concurrent stop_share that
        // erases the file cannot leave us with a dangling reference.
        FileInfo &info = fit->second;
        if (info.seeders.count(session->currentUser)) {
            session->responses.push_back("Already seeding: " + fileName + "\n");
            return;
        }
        if (fileHash != info.fileHash) {
            session->responses.push_back("Error: File hash mismatch. File may be corrupted.\n");
            return;
        }

        putSeeder(gid, fileName, session->currentUser, true);
        putUploaded(session->currentUser, gid, fileName, true);

        session->responses.push_back("Now seeding: " + fileName + "\n");
        session->responses.push_back("Total seeders: " + to_string(info.seeders.size()) + "\n");
    }
}

// args: stop_share <gid> <fileName>
void stopShare(const vector<string> &args, ClientSession *session) {
    if (!isLoggedIn(session)) return;
    if (args.size() != 3) {
        session->responses.push_back("Error: Malformed request\n");
        return;
    }
    const string &groupId = args[1];
    const string &fileName = args[2];

    {
        DataLock lock;

        auto git = groups.find(groupId);
        if (git == groups.end()) {
            session->responses.push_back("Error: Group not found\n");
            return;
        }
        if (!git->second.members.count(session->currentUser)) {
            session->responses.push_back("Error: Not a member of this group\n");
            return;
        }
        auto fit = git->second.files.find(fileName);
        if (fit == git->second.files.end()) {
            session->responses.push_back("Error: File not found in group\n");
            return;
        }

        FileInfo &fileInfo = fit->second;
        if (!fileInfo.seeders.count(session->currentUser)) {
            session->responses.push_back("Error: You are not sharing this file\n");
            return;
        }

        putUploaded(session->currentUser, groupId, fileName, false);
        removeSeeder(groupId, fileName, session->currentUser);  // also drops the file if it was the last seeder
        session->responses.push_back("Stopped sharing: " + fileName + "\n");
        if (!git->second.files.count(fileName)) {
            session->responses.push_back("File removed from group (no seeders left)\n");
        }
    }
}

// Authorises a peer-to-peer piece request. A peer server asks the tracker whether <uid> (holding
// <token>) is a logged-in member of <gid> before it sends any bytes.
void checkAccess(const string &gid, const string &uid, const string &token, ClientSession *session) {
    if (!isLoggedIn(session)) return;

    DataLock lock;
    auto uit = users.find(uid);
    auto git = groups.find(gid);
    bool ok = uit != users.end() && git != groups.end() && uit->second.loggedIn &&
              !token.empty() && uit->second.token == token && git->second.members.count(uid);
    session->responses.push_back(ok ? "OK\n" : "DENIED\n");
}
