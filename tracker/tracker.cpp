#include "header.h"
#include <atomic>
#include <csignal>
#include <poll.h>

// Set by the console thread when the operator types "quit".
static atomic<bool> shutdownRequested(false);

static bool has(const string &s, const char *needle) { return s.find(needle) != string::npos; }

// Maps a handler's reply to a numeric status. Handlers report problems as "Error: ..." /
// "Usage: ..." text (shown to the user as is); the code lets programs react without parsing it.
static int statusFor(const string &payload)
{
    string first = payload.substr(0, payload.find('\n'));
    if (first == "DENIED") return ST_FORBIDDEN;
    bool isError = first.compare(0, 6, "Error:") == 0 || first.compare(0, 6, "Usage:") == 0 ||
                   first.compare(0, 15, "Unknown command") == 0;
    if (!isError) return ST_OK;

    if (first.compare(0, 6, "Usage:") == 0 || first.compare(0, 15, "Unknown command") == 0 ||
        has(first, "Invalid user id") || has(first, "Invalid group id") || has(first, "Invalid file name") ||
        has(first, "Invalid peer address") || has(first, "Malformed") || has(first, "does not match") ||
        has(first, "hash mismatch")) return ST_BAD_REQUEST;
    if (has(first, "No user logged in") || has(first, "Invalid user or password") ||
        has(first, "Session not found")) return ST_UNAUTHORIZED;
    if (has(first, "Not a member") || has(first, "Only owner") || has(first, "Owner cannot") ||
        has(first, "not sharing")) return ST_FORBIDDEN;
    if (has(first, "not found") || has(first, "No such request")) return ST_NOT_FOUND;
    if (has(first, "already") || has(first, "Already")) return ST_CONFLICT;
    return ST_BAD_REQUEST;
}

static void *handle_client(void *arg)
{
    int fd = *(int *)arg;
    free(arg);

    int keepalive = 1;
    setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(keepalive));
    int nodelay = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    ClientSession session;
    session.socketFd = fd;

    // One request per line; one length-prefixed response per request. Because the reader is
    // buffered, several pipelined commands in a single TCP segment are all processed.
    LineReader reader(fd);
    string line;
    while (!shutdownRequested && reader.readLine(line)) {
        if (line.empty()) continue;

        session.responses.clear();
        handle_input(line, &session);

        string payload;
        for (const string &part : session.responses) payload += part;
        if (payload.empty()) payload = "OK\n";

        if (!sendFramed(fd, payload, statusFor(payload))) {
            perror("ERROR writing to socket");
            break;
        }
    }

    printf("Client disconnected.\n");
    // A connection that goes away without "logout" is treated exactly like a logout, on this
    // tracker and (via sync) on the other one, so the two never diverge.
    if (!shutdownRequested) logoutOnDisconnect(&session);

    close(fd);
    return NULL;
}

// Console input thread: "quit" shuts the tracker down.
static void *consoleInputThread(void *arg)
{
    (void)arg;
    char input[256];

    // fgets returns NULL on EOF (e.g. stdin is /dev/null when run in the background). Just stop
    // listening for commands instead of spinning.
    while (fgets(input, sizeof(input), stdin) != NULL) {
        input[strcspn(input, "\r\n")] = 0;

        if (strcmp(input, "quit") == 0) {
            printf("\n[TRACKER] Shutdown requested. Closing tracker...\n");
            shutdownRequested = true;
            break;
        } else if (strlen(input) > 0) {
            printf("[TRACKER] Unknown command: %s\n", input);
            printf("[TRACKER] Available commands: quit\n");
        }
    }
    return NULL;
}

int main(int argc, char *argv[])
{
    // A peer (or the other tracker) closing its socket must not kill this process.
    signal(SIGPIPE, SIG_IGN);
    setvbuf(stdout, NULL, _IOLBF, 0);  // keep log lines visible when output is a pipe/file

    if (argc < 3) {
        fprintf(stderr, "Usage: %s <trackerinfo.txt> <tracker_id>\n", argv[0]);
        return 1;
    }

    vector<TrackerEndpoint> trackers = loadTrackerInfo(argv[1]);
    if (trackers.empty()) {
        fprintf(stderr, "No trackers found in %s\n", argv[1]);
        return 1;
    }

    int tracker_id = atoi(argv[2]);
    if (tracker_id <= 0 || tracker_id > (int)trackers.size()) {
        fprintf(stderr, "Invalid tracker_id %d (must be between 1 and %d)\n", tracker_id, (int)trackers.size());
        return 1;
    }

    int portno = trackers[tracker_id - 1].port;
    printf("Using tracker #%d: IP=%s, Port=%d\n", tracker_id, trackers[tracker_id - 1].ip.c_str(), portno);

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("ERROR opening socket");
        return 1;
    }

    int opt = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt failed");
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(portno);

    if (::bind(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("ERROR on binding");
        return 1;
    }
    if (listen(sockfd, 128) < 0) {
        perror("ERROR on listen");
        return 1;
    }

    // Start replication only once our client port is bound, so a failed start never leaves a
    // half-alive tracker advertising state to its peer.
    initTrackerSync(argv[1], tracker_id);

    printf("Server listening on port %d...\n", portno);
    printf("Type 'quit' to shutdown tracker gracefully.\n\n");

    pthread_t console_tid;
    if (pthread_create(&console_tid, NULL, consoleInputThread, NULL) == 0) {
        pthread_detach(console_tid);
    }

    while (!shutdownRequested) {
        // poll() so the loop can notice a shutdown request without a connection arriving.
        struct pollfd pfd = {sockfd, POLLIN, 0};
        int pr = poll(&pfd, 1, 1000);
        if (pr <= 0) continue;  // timeout or EINTR

        struct sockaddr_in cli_addr;
        socklen_t clilen = sizeof(cli_addr);
        int cfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clilen);
        if (cfd < 0) {
            // Transient accept failures must not take the tracker down.
            if (errno == EMFILE || errno == ENFILE || errno == ENOBUFS || errno == ENOMEM) {
                perror("accept (resource exhaustion, backing off)");
                usleep(100000);
            } else if (errno != EINTR && errno != EAGAIN && errno != ECONNABORTED) {
                perror("accept");
            }
            continue;
        }

        int *arg = (int *)malloc(sizeof(int));
        if (!arg) {
            close(cfd);
            continue;
        }
        *arg = cfd;

        pthread_t thread_id;
        if (pthread_create(&thread_id, NULL, handle_client, arg) != 0) {
            perror("ERROR creating thread");
            close(cfd);
            free(arg);
        } else {
            pthread_detach(thread_id);
        }
    }

    // Deliberately no logouts and no sync traffic here: the other tracker keeps every session
    // alive so clients can fail over to it with restore_session.
    printf("[TRACKER] Shutting down gracefully...\n");
    close(sockfd);
    printf("[TRACKER] Tracker shutdown complete.\n");
    fflush(stdout);
    _exit(0);  // detached client threads may still be running; skip static destructors
}
