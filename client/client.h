#ifndef CLIENT_H
#define CLIENT_H

#include <string>

// ---- state shared between the REPL, the peer server and the downloader ----

extern std::string myIP;
extern int myListenPort;

// Logged-in user and the session token issued by the tracker (needed for failover restore and
// for authorising peer-to-peer piece requests).
void setSession(const std::string &user, const std::string &token);
void getSession(std::string &user, std::string &token);

// One request/response exchange with the tracker, with automatic failover to another tracker.
// `cmd` has no trailing newline. Thread safe.
bool trackerRpc(const std::string &cmd, std::string &response, int *status = nullptr);

// ---- local share registry (peer_server.cpp) ----
// Files this client can serve to other peers, keyed by (group, file name).
void registerLocalFile(const std::string &groupId, const std::string &fileName, const std::string &path);
void unregisterLocalFile(const std::string &groupId, const std::string &fileName);
bool lookupLocalFile(const std::string &groupId, const std::string &fileName, std::string &path);

// ---- peer server (peer_server.cpp) ----
// Starts listening for GET_PIECE requests from other clients.
bool startPeerServer(int port);

// ---- downloader (downloader.cpp) ----
// Fetches metadata from the tracker and starts a background multi-peer download.
// Returns false (with an explanation in `message`) if it could not be started.
bool startDownload(const std::string &groupId, const std::string &fileName,
                   const std::string &destination, std::string &message);
void showDownloads();

#endif  // CLIENT_H
