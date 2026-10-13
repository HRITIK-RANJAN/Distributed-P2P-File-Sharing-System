// Tracker <-> tracker replication transport. The state itself (versioned cells, merge rule) is in
// store.cpp; this file only moves cells between the two trackers.
//
// Wire format: one message per line, '\n' terminated, TAB separated fields:
//
//     CELL <key> <stamp> <0|1> <value>     one versioned fact (see store.cpp)
//     SYNC_SNAPSHOT_REQ                    "please send me everything you have"
//     SYNC_HEARTBEAT
//
// Receivers use a buffered LineReader, so messages that arrive glued together or split across
// reads are always delimited correctly.
//
// Each tracker keeps one outgoing connection to the peer's sync port and accepts incoming
// connections on its own sync port.
//
// Catch-up and partition healing: whenever the outgoing connection is (re)established, a tracker
// sends every cell it has (tombstones included) and asks the peer to do the same. Because cells
// merge by "highest stamp wins", the exchange is symmetric and order-independent: changes made
// on both sides while they could not talk are all kept, and deletions stay deleted.
//
// Lock order: syncMutex -> dataMutex. Nothing may call replicatePending() (or be inside a
// DataLock destructor) while holding syncMutex; the replication code itself uses PlainLock.
#include "header.h"
#include <csignal>
#include <ctime>

static int otherTrackerSocket = -1;
static pthread_mutex_t syncMutex = PTHREAD_MUTEX_INITIALIZER;
static bool isSyncEnabled = false;
static string otherTrackerIP;
static int otherTrackerSyncPort;
static int mySyncListenPort;
static time_t lastConnectAttempt = 0;

#define SYNC_HEARTBEAT "SYNC_HEARTBEAT"
#define SYNC_SNAPSHOT_REQ "SYNC_SNAPSHOT_REQ"

// ---------------------------------------------------------------------------
// Outgoing connection
// ---------------------------------------------------------------------------

// Caller holds syncMutex.
static bool connectToOtherTrackerLocked() {
    if (otherTrackerSocket >= 0) {
        close(otherTrackerSocket);
        otherTrackerSocket = -1;
    }
    lastConnectAttempt = time(NULL);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(otherTrackerSyncPort);
    if (inet_pton(AF_INET, otherTrackerIP.c_str(), &serv_addr.sin_addr) <= 0) {
        close(fd);
        return false;
    }

    struct timeval timeout;
    timeout.tv_sec = 3;
    timeout.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
#ifdef SO_NOSIGPIPE
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif

    if (connect(fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        close(fd);
        return false;
    }
    otherTrackerSocket = fd;

    // New connection: ask the peer for its cells, and send ours.
    bool ok = sendAll(fd, string(SYNC_SNAPSHOT_REQ) + "\n");
    if (ok) {
        // snapshotMessages() takes dataMutex while we hold syncMutex (the documented lock order).
        for (const string &m : snapshotMessages()) {
            if (!sendAll(fd, m + "\n")) {
                ok = false;
                break;
            }
        }
    }
    if (!ok) {
        close(fd);
        otherTrackerSocket = -1;
        return false;
    }
    printf("[SYNC] Connected to other tracker %s:%d (state exchanged)\n", otherTrackerIP.c_str(),
           otherTrackerSyncPort);
    return true;
}

// force = true is used by the heartbeat thread; ordinary updates do not hammer a dead peer with
// connect() attempts more than once every 2 seconds.
static void sendSyncMessage(const string &message, bool force = false) {
    if (!isSyncEnabled) return;

    pthread_mutex_lock(&syncMutex);

    if (otherTrackerSocket < 0) {
        if (!force && time(NULL) - lastConnectAttempt < 2) {
            pthread_mutex_unlock(&syncMutex);
            return;
        }
        if (!connectToOtherTrackerLocked()) {
            pthread_mutex_unlock(&syncMutex);
            return;
        }
    }

    // A write to a dead peer fails with EPIPE/ECONNRESET (SIGPIPE is ignored process-wide).
    if (!sendAll(otherTrackerSocket, message + "\n")) {
        close(otherTrackerSocket);
        otherTrackerSocket = -1;
        // One immediate reconnect; the cells it sends already contain this change.
        connectToOtherTrackerLocked();
    }

    pthread_mutex_unlock(&syncMutex);
}

// Called (via DataLock's destructor) after a handler released dataMutex.
void replicatePending() {
    if (!isSyncEnabled) return;
    for (const string &m : takePendingMessages()) sendSyncMessage(m);
}

static void pushSnapshot() {
    pthread_mutex_lock(&syncMutex);
    if (otherTrackerSocket >= 0) {
        for (const string &m : snapshotMessages()) {
            if (!sendAll(otherTrackerSocket, m + "\n")) {
                close(otherTrackerSocket);
                otherTrackerSocket = -1;
                break;
            }
        }
    }
    pthread_mutex_unlock(&syncMutex);
}

// ---------------------------------------------------------------------------
// Incoming messages
// ---------------------------------------------------------------------------

// Runs on its own thread: if the listener thread pushed the snapshot itself, two trackers
// answering each other's requests could block in send() while neither is reading.
static void *pushSnapshotThread(void *arg) {
    (void)arg;
    pushSnapshot();
    return NULL;
}

// Applies one replicated message. It only touches local state through applyRemoteCell(), which
// never queues anything for sending, so changes are not echoed back.
static void processSyncMessage(const string &message) {
    vector<string> f = splitStr(message, '\t');

    if (f[0] == "CELL" && f.size() == 5) {
        applyRemoteCell(f[1], f[2], f[3] == "1", f[4]);
    } else if (f[0] == SYNC_SNAPSHOT_REQ) {
        pthread_t tid;
        if (pthread_create(&tid, NULL, pushSnapshotThread, NULL) == 0) pthread_detach(tid);
    }
    // SYNC_HEARTBEAT and unknown types: nothing to do.
}

static void *syncConnectionThread(void *arg) {
    int fd = *(int *)arg;
    free(arg);

    LineReader reader(fd);
    string line;
    while (reader.readLine(line)) {
        if (!line.empty()) processSyncMessage(line);
    }
    close(fd);
    return NULL;
}

static void *syncListenerThread(void *arg) {
    int serverSocket = *(int *)arg;
    free(arg);

    while (true) {
        struct sockaddr_in cli_addr;
        socklen_t clilen = sizeof(cli_addr);
        int newsockfd = accept(serverSocket, (struct sockaddr *)&cli_addr, &clilen);
        if (newsockfd < 0) {
            if (errno != EINTR) usleep(100000);
            continue;
        }

        // One thread per incoming connection: a stale connection from a crashed peer can never
        // block a fresh one.
        int *sock = (int *)malloc(sizeof(int));
        *sock = newsockfd;
        pthread_t tid;
        if (pthread_create(&tid, NULL, syncConnectionThread, sock) == 0) {
            pthread_detach(tid);
        } else {
            close(newsockfd);
            free(sock);
        }
    }
    return NULL;
}

static void *heartbeatThread(void *arg) {
    (void)arg;
    while (isSyncEnabled) {
        sleep(3);
        // Doubles as the reconnect timer: while the peer is unreachable this retries every 3
        // seconds, and the first successful connect exchanges all state.
        sendSyncMessage(SYNC_HEARTBEAT, true);
    }
    return NULL;
}

void initTrackerSync(const string &trackerInfoFile, int currentTrackerId) {
    setNodeId((unsigned)currentTrackerId);

    vector<TrackerEndpoint> trackers = loadTrackerInfo(trackerInfoFile);
    if (trackers.size() < 2) {
        printf("Info: Only one tracker configured. Running in standalone mode.\n");
        return;
    }

    // Replicate with the first tracker that is not us (the assignment uses exactly two).
    int mine = currentTrackerId - 1;
    for (int i = 0; i < (int)trackers.size(); i++) {
        if (i != mine) {
            otherTrackerIP = trackers[i].ip;
            otherTrackerSyncPort = trackers[i].syncPort;
            break;
        }
    }
    mySyncListenPort = trackers[mine].syncPort;

    int *serverSocket = (int *)malloc(sizeof(int));
    *serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (*serverSocket < 0) {
        printf("Warning: Could not create sync listener socket\n");
        free(serverSocket);
        return;
    }

    int opt = 1;
    setsockopt(*serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(mySyncListenPort);

    if (::bind(*serverSocket, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        printf("Warning: Could not bind sync listener on port %d\n", mySyncListenPort);
        close(*serverSocket);
        free(serverSocket);
        return;
    }
    listen(*serverSocket, 8);

    pthread_t tid;
    if (pthread_create(&tid, NULL, syncListenerThread, serverSocket) != 0) {
        printf("Warning: Could not create sync listener thread\n");
        close(*serverSocket);
        free(serverSocket);
        return;
    }
    pthread_detach(tid);

    isSyncEnabled = true;
    enableReplication();
    printf("Tracker sync initialized. Listening on port %d\n", mySyncListenPort);
    printf("Will sync with: %s:%d\n", otherTrackerIP.c_str(), otherTrackerSyncPort);

    pthread_t hbThread;
    if (pthread_create(&hbThread, NULL, heartbeatThread, NULL) == 0) {
        pthread_detach(hbThread);
    }

    // First connection attempt now (exchanges state if the peer is already running).
    pthread_mutex_lock(&syncMutex);
    connectToOtherTrackerLocked();
    pthread_mutex_unlock(&syncMutex);
}
