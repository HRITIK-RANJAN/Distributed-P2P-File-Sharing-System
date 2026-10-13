// Multi-peer downloader. Runs entirely inside the client: the tracker only supplies metadata
// (piece hashes + the list of online seeders); bytes flow peer to peer.
//
// Design
//   * A fixed pool of N workers pulls piece indices from one shared queue (mutex + condition
//     variable). No thread per piece.
//   * Load balancing: piece i is first requested from peers[(i + attempt) % numPeers], so pieces
//     are spread round-robin over all seeders.
//   * Retry: a piece that fails (connection error, bad header, hash mismatch) goes back on the
//     queue with the failing peer marked as excluded and its attempt count incremented, up to
//     MAX_ATTEMPTS. A peer that fails several times in a row is skipped for the rest of the
//     download.
//   * Each worker keeps one persistent TCP connection per peer.
//   * Verified pieces are written with pwrite() at their offset: no global file lock.
//   * Completion is decided by counters (completed + permanentlyFailed == total), not by
//     inspecting how many threads happen to be alive.
#include "client.h"
#include "net_util.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <atomic>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <cstdlib>
#include <cstring>
#include <vector>

using namespace std;

namespace {

const int MAX_ATTEMPTS = 6;          // per piece
const int PEER_DEAD_AFTER = 3;       // consecutive failures before a peer is skipped

struct Peer {
    string addr;
    string ip;
    int port = 0;
    string user;
    atomic<int> consecutiveFailures{0};
    atomic<int> piecesServed{0};
};

struct WorkItem {
    int piece;
    int attempts;
    vector<char> excluded;  // excluded[p] != 0: peer p already failed for this piece
};

struct Download {
    int id = 0;
    string groupId, fileName, destPath;
    long long fileSize = 0;
    int totalPieces = 0;
    string fileHash;
    vector<string> pieceHashes;
    vector<unique_ptr<Peer>> peers;
    string user, token;
    int fd = -1;

    mutex m;                          // guards everything below
    condition_variable cv;
    deque<WorkItem> queue;
    int completed = 0;
    int permanentlyFailed = 0;
    bool fatal = false;               // e.g. disk write error: stop everything
    string status = "Downloading";    // Downloading | Completed | Failed
    string detail;

    bool resolved() const { return completed + permanentlyFailed >= totalPieces; }
};

mutex registryMutex;
vector<shared_ptr<Download>> registry;
int nextDownloadId = 1;

size_t pieceLength(const Download &d, int idx) {
    long long off = (long long)idx * PIECE_SIZE;
    return (size_t)min<long long>(PIECE_SIZE, d.fileSize - off);
}

struct PeerConn {
    int fd = -1;
    LineReader reader;
    void close_() {
        if (fd >= 0) close(fd);
        fd = -1;
    }
};

bool connectPeer(const Peer &p, PeerConn &c) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(p.port);
    if (inet_pton(AF_INET, p.ip.c_str(), &addr.sin_addr) <= 0) {
        close(fd);
        return false;
    }

    struct timeval tv;
    tv.tv_sec = 10;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#ifdef SO_NOSIGPIPE
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return false;
    }
    c.fd = fd;
    c.reader.reset(fd);
    return true;
}

// Requests one piece over the (persistent) connection, checks size and SHA-1.
bool fetchPiece(const Download &d, const Peer &p, PeerConn &c, int idx, unsigned char *buf) {
    if (c.fd < 0 && !connectPeer(p, c)) return false;

    string req = "GET_PIECE\t" + d.groupId + "\t" + d.fileName + "\t" + to_string(idx) + "\t" +
                 d.user + "\t" + d.token + "\n";
    if (!sendAll(c.fd, req)) return false;

    // Header is a full line, so a header split across TCP segments is handled by the LineReader.
    string header;
    if (!c.reader.readLine(header)) return false;
    if (header.compare(0, 11, "PIECE_DATA ") != 0) return false;

    char *end = nullptr;
    unsigned long long n = strtoull(header.c_str() + 11, &end, 10);
    if (end == header.c_str() + 11 || *end != '\0') return false;
    if (n != pieceLength(d, idx)) return false;

    if (!c.reader.readExact(buf, (size_t)n)) return false;
    return sha1Hex(buf, (size_t)n) == d.pieceHashes[idx];
}

bool writePiece(const Download &d, int idx, const unsigned char *buf, size_t len) {
    long long off = (long long)idx * PIECE_SIZE;
    size_t done = 0;
    while (done < len) {
        ssize_t n = pwrite(d.fd, buf + done, len - done, off + (long long)done);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        done += (size_t)n;
    }
    return true;
}

void worker(Download *d) {
    const int numPeers = (int)d->peers.size();
    vector<unsigned char> buf(PIECE_SIZE);
    vector<PeerConn> conns(numPeers);

    while (true) {
        WorkItem item;
        {
            unique_lock<mutex> lk(d->m);
            d->cv.wait(lk, [&] { return !d->queue.empty() || d->resolved() || d->fatal; });
            if (d->fatal || d->queue.empty()) break;  // queue empty here means everything is resolved
            item = std::move(d->queue.front());
            d->queue.pop_front();
        }

        // Pick a peer: start at (piece + attempt) % n for even spread, prefer peers that have not
        // already failed this piece, skip peers that look dead.
        int chosen = -1;
        for (int pass = 0; pass < 2 && chosen < 0; pass++) {
            for (int k = 0; k < numPeers; k++) {
                int p = (item.piece + item.attempts + k) % numPeers;
                if (d->peers[p]->consecutiveFailures >= PEER_DEAD_AFTER) continue;
                if (pass == 0 && item.excluded[p]) continue;
                chosen = p;
                break;
            }
        }

        bool ok = false;
        if (chosen >= 0) {
            Peer &peer = *d->peers[chosen];
            ok = fetchPiece(*d, peer, conns[chosen], item.piece, buf.data());
            if (ok) {
                if (!writePiece(*d, item.piece, buf.data(), pieceLength(*d, item.piece))) {
                    lock_guard<mutex> lk(d->m);
                    d->fatal = true;
                    d->detail = string("write failed: ") + strerror(errno);
                    d->cv.notify_all();
                    break;
                }
                peer.consecutiveFailures = 0;
                peer.piecesServed++;
            } else {
                conns[chosen].close_();  // next attempt reconnects
                peer.consecutiveFailures++;
                item.excluded[chosen] = 1;
            }
        }

        lock_guard<mutex> lk(d->m);
        if (ok) {
            d->completed++;
        } else {
            item.attempts++;
            if (chosen < 0 || item.attempts >= MAX_ATTEMPTS) {
                d->permanentlyFailed++;
            } else {
                d->queue.push_back(std::move(item));  // retry later, preferably from another peer
            }
        }
        d->cv.notify_all();
    }

    for (auto &c : conns) c.close_();
}

void runDownload(shared_ptr<Download> d) {
    auto t0 = chrono::steady_clock::now();

    int numWorkers = 8;
    if (const char *e = getenv("P2P_WORKERS")) numWorkers = max(1, atoi(e));
    numWorkers = max(1, min(numWorkers, d->totalPieces));

    vector<thread> pool;
    if (d->totalPieces > 0) {
        for (int i = 0; i < numWorkers; i++) pool.emplace_back(worker, d.get());
        for (auto &t : pool) t.join();
    }
    double transferSecs = chrono::duration<double>(chrono::steady_clock::now() - t0).count();

    bool failed;
    string reason;
    {
        lock_guard<mutex> lk(d->m);
        failed = d->fatal || d->permanentlyFailed > 0 || d->completed < d->totalPieces;
        if (failed) {
            reason = d->detail.empty() ? to_string(d->permanentlyFailed) + " piece(s) could not be fetched from any peer"
                                       : d->detail;
        }
    }

    close(d->fd);
    d->fd = -1;

    if (!failed) {
        // Defence in depth: every piece was verified, but also check the assembled file.
        string h;
        if (!hashFile(d->destPath, h) || h != d->fileHash) {
            failed = true;
            reason = "assembled file failed SHA-1 verification";
        }
    }

    if (failed) {
        unlink(d->destPath.c_str());  // never leave a corrupt partial file behind
        {
            lock_guard<mutex> lk(d->m);
            d->status = "Failed";
            d->detail = reason;
        }
        printf("\n[DOWNLOAD] Failed: %s (%s)\n", d->fileName.c_str(), reason.c_str());
        return;
    }

    // Serve the new copy and tell the tracker we are a seeder now. The file is registered
    // locally first so that no peer can be sent to us before we can answer.
    registerLocalFile(d->groupId, d->fileName, d->destPath);
    string resp;
    bool registered = trackerRpc("add_seeder\t" + d->groupId + "\t" + d->fileName + "\t" + d->fileHash, resp) &&
                      (resp.compare(0, 11, "Now seeding") == 0 || resp.compare(0, 7, "Already") == 0);
    if (!registered) unregisterLocalFile(d->groupId, d->fileName);

    double totalSecs = chrono::duration<double>(chrono::steady_clock::now() - t0).count();
    {
        lock_guard<mutex> lk(d->m);
        d->status = "Completed";
        d->detail = registered ? "now seeding" : "downloaded, but could not register as seeder";
    }

    double mb = d->fileSize / (1024.0 * 1024.0);
    printf("\n[DOWNLOAD] Completed: %s  (%.1f MB in %.3f s transfer = %.1f MB/s; %.3f s incl. verification) - %s\n",
           d->fileName.c_str(), mb, transferSecs, transferSecs > 0 ? mb / transferSecs : 0.0, totalSecs,
           registered ? "now seeding" : "NOT registered as seeder");
    printf("[DOWNLOAD] Pieces per peer:");
    for (auto &p : d->peers) printf("  %s=%d", p->addr.c_str(), p->piecesServed.load());
    printf("\n");
    fflush(stdout);
}

}  // namespace

// ---------------------------------------------------------------------------

bool startDownload(const string &groupId, const string &fileName, const string &destination,
                   string &message) {
    string user, token;
    getSession(user, token);
    if (user.empty()) {
        message = "Error: No user logged in\n";
        return false;
    }

    // 1. Metadata from the tracker.
    string resp;
    if (!trackerRpc("file_info\t" + groupId + "\t" + fileName, resp)) {
        message = "Error: Could not reach any tracker\n";
        return false;
    }
    vector<string> lines = splitStr(resp, '\n');
    if (lines.empty() || lines[0].compare(0, 9, "FILEINFO ") != 0) {
        message = resp.empty() ? "Error: Empty reply from tracker\n" : resp;
        return false;
    }

    auto d = make_shared<Download>();
    d->groupId = groupId;
    d->fileName = fileName;
    d->user = user;
    d->token = token;

    {
        vector<string> f = splitStr(lines[0], ' ');
        if (f.size() != 4) {
            message = "Error: Malformed file info from tracker\n";
            return false;
        }
        d->fileSize = atoll(f[1].c_str());
        d->totalPieces = atoi(f[2].c_str());
        d->fileHash = f[3];
    }
    for (size_t i = 1; i < lines.size(); i++) {
        const string &ln = lines[i];
        if (ln.compare(0, 7, "HASHES ") == 0 || ln == "HASHES") {
            if (ln.size() > 7) d->pieceHashes = splitStr(ln.substr(7), ' ');
        } else if (ln.compare(0, 5, "PEER ") == 0) {
            vector<string> f = splitStr(ln, ' ');
            if (f.size() < 3) continue;
            if (f[2] == user || f[1] == myIP + ":" + to_string(myListenPort)) continue;  // not ourselves
            size_t colon = f[1].find(':');
            if (colon == string::npos) continue;
            auto p = make_unique<Peer>();
            p->addr = f[1];
            p->ip = f[1].substr(0, colon);
            p->port = atoi(f[1].substr(colon + 1).c_str());
            p->user = f[2];
            d->peers.push_back(std::move(p));
        }
    }
    if ((int)d->pieceHashes.size() != d->totalPieces) {
        message = "Error: Inconsistent piece list from tracker\n";
        return false;
    }
    if (d->peers.empty()) {
        message = "Error: No peers available for download\n";
        return false;
    }

    // 2. Destination. Never overwrite an existing file (it could be the uploader's own copy).
    string dest = destination;
    struct stat st;
    if (stat(dest.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
        if (dest.back() != '/') dest += '/';
        dest += fileName;
    } else {
        size_t slash = dest.find_last_of('/');
        if (slash != string::npos) {
            string parent = dest.substr(0, slash);
            if (parent.empty()) parent = "/";
            if (stat(parent.c_str(), &st) != 0 || !S_ISDIR(st.st_mode)) {
                message = "Error: Destination directory does not exist\n";
                return false;
            }
        }
    }
    d->destPath = dest;
    d->fd = open(dest.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (d->fd < 0) {
        message = errno == EEXIST ? "Error: Destination file already exists: " + dest + "\n"
                                  : "Error: Cannot create destination file: " + dest + " (" + strerror(errno) + ")\n";
        return false;
    }
    if (ftruncate(d->fd, d->fileSize) != 0) {
        message = string("Error: Cannot allocate file space: ") + strerror(errno) + "\n";
        close(d->fd);
        unlink(dest.c_str());
        return false;
    }

    // 3. Queue every piece and run in the background.
    for (int i = 0; i < d->totalPieces; i++) {
        d->queue.push_back(WorkItem{i, 0, vector<char>(d->peers.size(), 0)});
    }
    {
        lock_guard<mutex> lk(registryMutex);
        d->id = nextDownloadId++;
        registry.push_back(d);
    }

    message = "Download started: " + fileName + "\n" +
              "Download ID: " + to_string(d->id) + "\n" +
              "Total pieces: " + to_string(d->totalPieces) + "\n" +
              "Saving to: " + dest + "\n" +
              "Downloading from:\n";
    for (auto &p : d->peers) message += " - " + p->addr + " (" + p->user + ")\n";

    thread(runDownload, d).detach();
    return true;
}

void showDownloads() {
    string user, token;
    getSession(user, token);

    lock_guard<mutex> lk(registryMutex);
    bool any = false;
    for (auto &d : registry) {
        if (d->user != user) continue;  // only this client's own downloads
        if (!any) printf("Downloads:\n");
        any = true;

        lock_guard<mutex> dl(d->m);
        int pct = d->totalPieces > 0 ? (int)(100.0 * d->completed / d->totalPieces) : 100;
        const char *tag = d->status == "Completed" ? "[C]" : d->status == "Failed" ? "[F]" : "[D]";
        printf("%s [%s] %s - %d%% (%d/%d pieces)", tag, d->groupId.c_str(), d->fileName.c_str(), pct,
               d->completed, d->totalPieces);
        if (!d->detail.empty()) printf(" - %s", d->detail.c_str());
        printf("\n");
    }
    if (!any) printf("No downloads\n");
}
