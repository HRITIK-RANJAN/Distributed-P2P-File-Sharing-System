// Peer server: answers GET_PIECE requests from other clients.
//
// Request  (one line, TAB separated):  GET_PIECE <group> <file> <pieceIndex> <userId> <token>
// Response: "PIECE_DATA <n>\n" followed by exactly n bytes, or "ERROR <code> <reason>\n".
// A connection stays open and serves any number of requests, so a downloader needs one TCP
// connection per peer rather than one per piece.
#include "client.h"
#include "net_util.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <chrono>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <map>
#include <mutex>
#include <thread>

using namespace std;

// ---------------------------------------------------------------------------
// Registry of files this client serves
// ---------------------------------------------------------------------------

static map<string, string> localFiles;  // "group\tfile" -> path
static mutex localFilesMutex;

static string fileKey(const string &g, const string &f) { return g + "\t" + f; }

void registerLocalFile(const string &groupId, const string &fileName, const string &path) {
    lock_guard<mutex> lk(localFilesMutex);
    localFiles[fileKey(groupId, fileName)] = path;
}

void unregisterLocalFile(const string &groupId, const string &fileName) {
    lock_guard<mutex> lk(localFilesMutex);
    localFiles.erase(fileKey(groupId, fileName));
}

bool lookupLocalFile(const string &groupId, const string &fileName, string &path) {
    lock_guard<mutex> lk(localFilesMutex);
    auto it = localFiles.find(fileKey(groupId, fileName));
    if (it == localFiles.end()) return false;
    path = it->second;
    return true;
}

// ---------------------------------------------------------------------------
// Access control: ask the tracker whether the requester is a logged-in member of the group.
// Results are cached briefly so a download of thousands of pieces costs one lookup.
// ---------------------------------------------------------------------------

static map<string, chrono::steady_clock::time_point> accessCache;
static mutex accessCacheMutex;

static bool authorised(const string &groupId, const string &userId, const string &token) {
    if (userId.empty() || token.empty()) return false;
    string key = groupId + "\t" + userId + "\t" + token;
    auto now = chrono::steady_clock::now();
    {
        lock_guard<mutex> lk(accessCacheMutex);
        auto it = accessCache.find(key);
        if (it != accessCache.end() && it->second > now) return true;
    }

    string resp;
    if (!trackerRpc("check_access " + groupId + " " + userId + " " + token, resp)) return false;
    if (resp.compare(0, 2, "OK") != 0) return false;

    lock_guard<mutex> lk(accessCacheMutex);
    if (accessCache.size() > 1024) accessCache.clear();
    accessCache[key] = now + chrono::seconds(30);
    return true;
}

// ---------------------------------------------------------------------------
// Optional upload throttle, used by the benchmark to emulate a limited uplink per peer.
// P2P_UPLOAD_KBPS=<kilobytes per second> caps the total bytes/s this client serves.
// ---------------------------------------------------------------------------

static double uploadBytesPerSec() {
    static double rate = [] {
        const char *e = getenv("P2P_UPLOAD_KBPS");
        double kb = e ? atof(e) : 0.0;
        return kb > 0 ? kb * 1024.0 : 0.0;
    }();
    return rate;
}

static void throttleUpload(size_t bytes) {
    double rate = uploadBytesPerSec();
    if (rate <= 0) return;

    static mutex m;
    static chrono::steady_clock::time_point nextFree = chrono::steady_clock::now();
    chrono::steady_clock::time_point mySlotEnd;
    {
        lock_guard<mutex> lk(m);
        auto now = chrono::steady_clock::now();
        if (nextFree < now) nextFree = now;
        nextFree += chrono::duration_cast<chrono::steady_clock::duration>(chrono::duration<double>(bytes / rate));
        mySlotEnd = nextFree;
    }
    this_thread::sleep_until(mySlotEnd);
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

static const size_t HEADER_RESERVE = 64;

static bool sendError(int fd, int code, const string &why) {
    return sendAll(fd, "ERROR " + to_string(code) + " " + why + "\n");
}

// Returns false if the connection should be closed.
static bool servePiece(int fd, const vector<string> &req, vector<unsigned char> &buf) {
    if (req.size() != 6) return sendError(fd, ST_BAD_REQUEST, "Malformed request") && false;

    const string &groupId = req[1];
    const string &fileName = req[2];
    const string &userId = req[4];
    const string &token = req[5];

    char *end = nullptr;
    long long pieceIndex = strtoll(req[3].c_str(), &end, 10);
    if (end == req[3].c_str() || *end != '\0' || pieceIndex < 0) return sendError(fd, ST_BAD_REQUEST, "Bad piece index") && false;

    string path;
    if (!lookupLocalFile(groupId, fileName, path)) return sendError(fd, ST_NOT_FOUND, "File not found");

    if (!authorised(groupId, userId, token)) return sendError(fd, ST_FORBIDDEN, "Access denied");

    int ffd = open(path.c_str(), O_RDONLY);
    if (ffd < 0) return sendError(fd, ST_INTERNAL, "Cannot open file");

    struct stat st;
    if (fstat(ffd, &st) != 0) {
        close(ffd);
        return sendError(fd, ST_INTERNAL, "Cannot stat file");
    }

    // Bounds check against the real file size.
    long long offset = pieceIndex * (long long)PIECE_SIZE;
    if (offset >= st.st_size) {
        close(ffd);
        return sendError(fd, ST_RANGE, "Piece index out of range");
    }
    size_t len = (size_t)min<long long>(PIECE_SIZE, st.st_size - offset);

    unsigned char *data = buf.data() + HEADER_RESERVE;
    size_t got = 0;
    while (got < len) {
        ssize_t n = pread(ffd, data + got, len - got, offset + (long long)got);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) break;
        got += (size_t)n;
    }
    close(ffd);
    if (got != len) return sendError(fd, ST_INTERNAL, "Could not read piece");

    // Header is written immediately in front of the payload so header + data go out in one send.
    string header = "PIECE_DATA " + to_string(len) + "\n";
    unsigned char *start = data - header.size();
    memcpy(start, header.data(), header.size());

    throttleUpload(len);
    return sendAll(fd, start, header.size() + len);
}

static void *handlePeerConnection(void *arg) {
    int fd = *(int *)arg;
    free(arg);

    struct timeval tv;
    tv.tv_sec = 120;  // drop connections that sit idle
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    vector<unsigned char> buf(HEADER_RESERVE + PIECE_SIZE);  // heap, not stack
    LineReader reader(fd, 64 * 1024);
    string line;
    while (reader.readLine(line)) {
        vector<string> req = splitStr(line, '\t');
        bool keep;
        if (req[0] == "GET_PIECE") {
            keep = servePiece(fd, req, buf);
        } else {
            keep = sendAll(fd, string("ACK\n"));
        }
        if (!keep) break;
    }
    close(fd);
    return NULL;
}

static void *peerListenerThread(void *arg) {
    int serverSocket = *(int *)arg;
    free(arg);
    printf("[P2P] Listening for peer connections on port %d\n", myListenPort);

    while (true) {
        struct sockaddr_in cli_addr;
        socklen_t clilen = sizeof(cli_addr);
        int newsockfd = accept(serverSocket, (struct sockaddr *)&cli_addr, &clilen);
        if (newsockfd < 0) {
            if (errno == EMFILE || errno == ENFILE) usleep(100000);
            continue;
        }

        int *sock = (int *)malloc(sizeof(int));
        *sock = newsockfd;
        pthread_t tid;
        if (pthread_create(&tid, NULL, handlePeerConnection, sock) == 0) {
            pthread_detach(tid);
        } else {
            close(newsockfd);
            free(sock);
        }
    }
    return NULL;
}

bool startPeerServer(int port) {
    int *serverSocket = (int *)malloc(sizeof(int));
    *serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (*serverSocket < 0) {
        perror("ERROR opening peer server socket");
        free(serverSocket);
        return false;
    }

    int opt = 1;
    setsockopt(*serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);

    if (::bind(*serverSocket, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("ERROR binding peer server socket");
        close(*serverSocket);
        free(serverSocket);
        return false;
    }
    listen(*serverSocket, 64);

    pthread_t tid;
    if (pthread_create(&tid, NULL, peerListenerThread, serverSocket) != 0) {
        perror("ERROR creating peer listener thread");
        close(*serverSocket);
        free(serverSocket);
        return false;
    }
    pthread_detach(tid);
    return true;
}
