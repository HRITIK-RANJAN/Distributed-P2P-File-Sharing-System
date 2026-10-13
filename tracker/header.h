#ifndef HEADER_H
#define HEADER_H

#include <iostream>
#include <string>
#include <map>
#include <set>
#include <vector>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sstream>
#include <errno.h>
#include <algorithm>
#include <sys/time.h>

#include "net_util.h"
#include "tracker_info.h"

using namespace std;

// The tracker stores metadata only. File bytes are hashed, served and downloaded by clients.

// File metadata
struct FileInfo {
    string fileName;
    long long fileSize = 0;
    int totalPieces = 0;
    string fileHash;                 // SHA1 of the complete file
    vector<string> pieceHashes;      // SHA1 of each 512 KB piece
    string uploaderId;               // user who first shared the file
    set<string> seeders;             // user IDs of peers that have the complete file
};

struct User {
    string userId;
    string passHash;                 // "salt$sha256(salt+password)"
    bool loggedIn = false;
    int activeSocket = -1;           // client connection on THIS tracker (-1 if none)
    string peerAddress;              // IP:PORT of the user's peer server
    string token;                    // session token issued at login (needed for restore_session)
    set<string> uploadedFiles;       // "groupId:fileName" entries the user shares
};

struct Group {
    string groupId;
    string ownerId;
    set<string> members;
    set<string> joinRequests;
    map<string, FileInfo> files;     // fileName -> FileInfo
};

// Per-connection state
struct ClientSession {
    int socketFd;
    string currentUser;
    vector<string> responses;
};

extern map<string, User> users;
extern map<string, Group> groups;
extern pthread_mutex_t dataMutex;

// Replication: mutations queue CELL messages; they are sent once dataMutex is released.
void replicatePending();

// RAII guard for dataMutex. On scope exit it unlocks FIRST and then sends any replication
// messages the critical section queued, so no lock is ever held while talking to the network.
struct DataLock {
    DataLock() { pthread_mutex_lock(&dataMutex); }
    ~DataLock() {
        pthread_mutex_unlock(&dataMutex);
        replicatePending();
    }
    DataLock(const DataLock &) = delete;
    DataLock &operator=(const DataLock &) = delete;
};

// Same lock without the flush. Used by the replication code itself (it may already hold
// syncMutex, and applying a *remote* change must not be echoed back).
struct PlainLock {
    PlainLock() { pthread_mutex_lock(&dataMutex); }
    ~PlainLock() { pthread_mutex_unlock(&dataMutex); }
    PlainLock(const PlainLock &) = delete;
    PlainLock &operator=(const PlainLock &) = delete;
};

// Separator for lists embedded inside one tab-separated protocol field.
static const char LIST_SEP = '\x1f';

// ---- command handling (user_manager.cpp) ----
void handle_input(const string &cmd, ClientSession *session);
bool isLoggedIn(ClientSession *session);
void logoutOnDisconnect(ClientSession *session);

// ---- file metadata operations (file_manager.cpp) ----
void uploadFile(const vector<string> &args, ClientSession *session);
void listFiles(const string &gid, ClientSession *session);
void fileInfoCommand(const vector<string> &args, ClientSession *session);
void addSeederCommand(const vector<string> &args, ClientSession *session);
void stopShare(const vector<string> &args, ClientSession *session);
void checkAccess(const string &gid, const string &uid, const string &token, ClientSession *session);

// ---- replicated store (store.cpp) ----
//
// All shared state is kept as versioned "cells" (key -> value + hybrid-logical-clock stamp, or a
// tombstone). users/groups above are views materialised from the cells. Every mutation goes
// through the put*/remove* functions below, which stamp the cell, update the view and queue a
// replication message. Merging two trackers' states is "highest stamp wins, per cell", which is
// commutative and idempotent, so a partition heals without losing changes or resurrecting
// deletions. Callers hold dataMutex.
void setNodeId(unsigned id);
void putUser(const string &uid, const string &passHash);
void setLogin(const string &uid, bool loggedIn, const string &peerAddr, const string &token);
void putGroup(const string &gid, const string &owner);
void putMember(const string &gid, const string &uid, bool present);
void putRequest(const string &gid, const string &uid, bool present);
void putFile(const string &gid, const FileInfo &info);
void removeFile(const string &gid, const string &name);
void putSeeder(const string &gid, const string &name, const string &uid, bool present);
void putUploaded(const string &uid, const string &gid, const string &name, bool present);
// Removes a seeder and, if it was the last one, the file itself.
void removeSeeder(const string &gid, const string &name, const string &uid);

// Seeder bookkeeping built on the above
void removeUserAsSeederFromAllFiles(const string &userId);
void addUserAsSeederForUploadedFiles(const string &userId);
void removeUserAsSeederFromGroupFiles(const string &groupId, const string &userId);
void addUserAsSeederForGroupFiles(const string &groupId, const string &userId);

// Replication plumbing (take dataMutex themselves)
bool applyRemoteCell(const string &key, const string &stamp, bool present, const string &value);
vector<string> snapshotMessages();      // every cell, tombstones included
vector<string> takePendingMessages();   // queued by local mutations
void enableReplication();

// ---- tracker <-> tracker connection (sync_trackers.cpp) ----
void initTrackerSync(const string &trackerInfoFile, int currentTrackerId);

#endif // HEADER_H
