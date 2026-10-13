# Distributed P2P File Sharing System

A BitTorrent-style file sharing system written from scratch in C++17 (POSIX sockets, pthreads,
no networking libraries). Users form groups and share files inside them; files are cut into 512 KB
pieces, verified with SHA-1, and downloaded in parallel from every peer that has them.

**Highlights**

* **Peer-to-peer data path** – two replicated *trackers* hold only metadata (users, groups, piece
  hashes, who is seeding); file bytes move directly between clients.
* **Multi-source downloads** – worker pool + shared piece queue, round-robin load balancing across
  seeders, per-piece retry on a different peer, `pwrite()` straight to the final file, whole-file
  verification, and automatic seeding of the finished download.
* **Fault-tolerant control plane** – clients fail over between trackers with token-proven sessions; a
  restarted tracker catches up automatically; trackers that lose contact with each other while both
  stay up merge cleanly afterwards (versioned records with a hybrid logical clock and tombstones, so
  nothing is lost and nothing deleted comes back).
* **Robust protocol layer** – partial reads/writes handled everywhere, length-prefixed framed replies
  with numeric status codes, authorised peer requests, `SIGPIPE`-safe.
* **Verified** – 96 end-to-end checks against real processes (killing seeders and trackers, cutting the
  sync link, edge-case file sizes), also run under ThreadSanitizer, AddressSanitizer + UBSan, and a leak check.
* **Measured** – ~5.8 GB/s on loopback for a 1 GB file (CPU-bound on SHA-1); scaling with seeders is
  shown with an emulated per-seeder uplink cap (see Benchmark – clearly labelled as emulation).

```
                  replicate versioned cells over the sync port; full exchange on every reconnect
        ┌───────────────────────────┐ <──────────────────────────> ┌───────────────────────────┐
        │ Tracker 1   :8001         │                              │ Tracker 2   :8002         │
        │ users · groups · files    │                              │ users · groups · files    │
        │ piece hashes · seeders    │                              │ piece hashes · seeders    │
        └────────────▲──────────────┘                              └──────────────▲────────────┘
                     │  commands + framed replies; failover + restore_session(token)
        ┌────────────┴──────────┐                                  ┌──────────────┴────────────┐
        │ Client A (seeder)     │                                  │ Client B (downloader)     │
        │  hashes files         │   GET_PIECE over persistent TCP  │  worker pool + queue      │
        │  peer server :8003    │ <────────────────────────────────│  verify SHA-1, pwrite()   │
        └───────────────────────┘      (piece bytes, peer-to-peer) └───────────────────────────┘
```

## 1. Build and run

Requirements: a C++17 compiler (g++ or clang++), OpenSSL ≥ 1.1 (`libcrypto`), GNU readline.

```bash
# Debian/Ubuntu:  sudo apt install g++ make libssl-dev libreadline-dev
# macOS:          brew install openssl readline
make -C tracker          # produces tracker/tracker
make -C client           # produces client/client
```

`tracker/` and `client/` are self-contained (each carries its own copy of `net_util.h` and
`tracker_info.h`; CI checks that the copies are identical). The Makefiles find Homebrew's
OpenSSL/readline automatically; on Linux the system packages are used.

```bash
./tracker/tracker tracker_info.txt 1        # terminal 1   (type "quit" to stop it)
./tracker/tracker tracker_info.txt 2        # terminal 2
./client/client 127.0.0.1:8003 tracker_info.txt   # terminal 3
./client/client 127.0.0.1:8004 tracker_info.txt   # terminal 4
```

`tracker_info.txt` has one tracker per line: `ip client_port [sync_port]`. The optional third
column is the port the *other tracker* uses to replicate; it defaults to `client_port + 1000`.

Other make targets (both directories): `make debug` (ThreadSanitizer), `make asan`
(AddressSanitizer + UBSan), `make clean`.

### Commands

```
create_user <user_id> <password>             login <user_id> <password>        logout
create_group <group_id>                      join_group <group_id>             leave_group <group_id>
list_groups                                  list_requests <group_id>          accept_request <group_id> <user_id>
upload_file <group_id> <file_path>           list_files <group_id>
download_file <group_id> <file_name> <destination_path>                        show_downloads
stop_share <group_id> <file_name>            become_seeder <group_id> <file_name> <file_path>
```

Quote names/paths that contain spaces: `download_file g1 "my file.bin" "/tmp/my dir"`.
`show_downloads` prints `[D]`/`[C]`/`[F]` `[group] filename - N% (done/total pieces)` for the
current user's downloads only. `become_seeder` hash-verifies your copy before registering you.

## 2. Architecture and modules

| Concern | Where |
|---|---|
| Hashing files into 512 KB pieces | client (`upload_file`, `become_seeder`), `net_util.h` |
| Serving pieces to other peers | client `peer_server.cpp` |
| Download scheduling, retries, file writes | client `downloader.cpp` |
| CLI, tracker connection, failover | client `client.cpp` |
| Users, groups, command dispatch | tracker `user_manager.cpp` |
| File metadata commands | tracker `file_manager.cpp` |
| Replicated versioned state | tracker `store.cpp` |
| Tracker ↔ tracker transport | tracker `sync_trackers.cpp` |
| Accept loop, per-connection thread, status codes | tracker `tracker.cpp` |
| `sendAll`/`recvAll`/`LineReader`, framing, SHA-1 (OpenSSL EVP) | `net_util.h` |

The tracker never opens, hashes, serves or writes file contents. Everything byte-related happens
in the client.

## 3. Data structures and why

| Structure | Purpose / rationale |
|---|---|
| `map<string, Cell> cells` (tracker) | Source of truth: every fact is a versioned cell `{stamp, present, value}`; deletions are tombstones. Ordered so that "everything under group g" is a `lower_bound` prefix scan. |
| `map<string, User>`, `map<string, Group>` (tracker) | *Views* derived from the cells; what command handlers read. Group → `members`, `joinRequests`, `files`; file → `pieceHashes`, `seeders` (`set<string>`: O(log n) membership, ordered output). |
| one `pthread_mutex_t dataMutex` + RAII `DataLock` | Single coarse lock: critical sections are short map operations, and a single lock rules out lock-order bugs between users/groups/files. The guard flushes the replication queue *after* unlocking. |
| `User.token`, `User.activeSocket` | Session token (random 128-bit) proves ownership on failover; `activeSocket` is purely local connection state (never replicated). |
| `LineReader` | Per-connection buffer: yields lines and exact-length blobs without losing bytes that arrived together — the basis of partial-read safety. |
| `Download` (client) | Piece queue (`deque<WorkItem>`) + counters under one mutex/condition variable; per-peer `Peer` objects with atomic failure/served counters. |
| `map<group\tfile, path>` (client) | What this client may serve. Filled only after the tracker accepted an upload / a download verified / `become_seeder` succeeded. |

## 4. Key algorithms

### Download engine and piece selection

* **Worker pool**: `P2P_WORKERS` (default 8) threads pull piece indices from one shared queue
  (mutex + condition variable). A 1 GB file is 2,048 queue entries, not 2,048 threads.
* **Piece/peer selection — round-robin load balancing**: piece *i* is first requested from
  `peers[(i + attempt) % numPeers]`. Every live seeder serves an equal share of the pieces (the
  `Pieces per peer` line after each download shows it). Chosen over rarest-first because every seeder
  holds the complete file, so there is no rarity to exploit; round-robin spreads load, keeps each
  peer's connection busy and never requests a piece twice.
* **Retry**: a piece that fails (connect/read error, wrong length, SHA-1 mismatch) goes back on the
  queue with that peer excluded and `attempts + 1`, up to 6 attempts; a peer with 3 consecutive
  failures is skipped for the rest of the download. Corrupt data is discarded, never written.
* **One persistent TCP connection per (worker, peer)**.
* **Lock-free writes**: verified pieces are `pwrite()`n at `index * 512 KB`.
* **Completion** is `completed + permanentlyFailed == total`; then a whole-file SHA-1 check; then
  the client registers the file with its own peer server **and** the tracker (`add_seeder`).
* Existing destination files are never overwritten (`O_EXCL`); failed downloads delete their partial file.
* Several files can be downloaded at once (each `download_file` starts its own pool).

### Tracker synchronization (partition-safe)

All state is a set of **cells** (`user`, `login`, `group`, `member`, `join-request`, `file`,
`seeder`, `shared-file`). A cell carries a **hybrid logical clock stamp** (wall-clock ms, counter,
tracker id); a delete is a cell with `present = 0` and its own stamp. Merging two trackers is
"per cell, the higher stamp wins" — commutative, associative and idempotent.

* Each local change is stamped and queued; once `dataMutex` is released it is sent as one
  `CELL` line to the other tracker, which applies it with the same merge rule.
* Whenever the outgoing connection is (re)established, a tracker sends **every cell it has,
  tombstones included**, and asks the peer to do the same. This one mechanism covers restart
  catch-up (empty tracker is refilled) and **partition healing** (both live trackers keep serving
  clients while cut off; afterwards each holds the union of all changes and a deletion made on one
  side is not undone by the other side's older copy of the record).
* The HLC receive rule guarantees a change made after observing another tracker's change always
  gets a higher stamp, even if the two machines' clocks disagree.
* Heartbeats every 3 s double as the reconnect timer.
* A cell that arrives before its parent (e.g. a seeder before its file) is stored and attached when
  the parent arrives.

### Sessions, failover and logout

* `login` returns a random token. After a tracker dies, the client reconnects to the other one and
  calls `restore_session <user> <token> <addr>`; the token (replicated) is the proof.
* A dropped client connection is processed exactly like `logout` and replicated, so both trackers stay identical.
* `logout` removes the user as seeder from all their files (files left with no seeder disappear);
  `login` re-adds them for the files they share.
* Tracker `quit` deliberately does **not** log users out: their sessions survive on the other tracker.

## 5. Network protocols

All traffic is TCP. Text lines are `\n` terminated; TAB separates fields wherever a file name may appear.

### Client → tracker

Request: one line. Plain commands are space separated (`login alice pw 127.0.0.1:8003`). Commands
carrying a file name use TABs:
`upload_file⇥group⇥name⇥size⇥fileSHA1⇥pieceSHA1 pieceSHA1 …`, `file_info⇥group⇥name`,
`add_seeder⇥group⇥name⇥fileSHA1`, `stop_share⇥group⇥name`. Internal: `check_access group user token`,
`restore_session user token addr`.

Reply: a frame `"<payload bytes> <status>\n<payload>"` — the reader never guesses where a reply
ends, and long replies are never truncated.

| Status | Meaning | Example |
|---|---|---|
| 200 | success | `Login successful`, `Now seeding` |
| 400 | usage / malformed / invalid argument | `Usage: ...`, `Unknown command`, `Invalid group id` |
| 401 | not logged in / bad credentials / no such session | `No user logged in`, `Invalid user or password` |
| 403 | logged in but not allowed | `Not a member of this group`, `Only owner can ...` |
| 404 | does not exist | `Group not found`, `File not found in group` |
| 409 | conflict | `User already exists`, `File already shared in this group` |
| 416 | piece index outside the file (peer protocol) | |
| 500 | internal error (peer protocol) | |

`file_info` replies with `FILEINFO <size> <pieces> <sha1>`, `HASHES <sha1> <sha1> …`, and one
`PEER <ip:port> <user>` per online seeder.

### Client ↔ client (peer protocol)

`GET_PIECE⇥group⇥file⇥pieceIndex⇥user⇥token\n` → `PIECE_DATA <n>\n` + exactly *n* bytes, or
`ERROR <code> <reason>\n`. The connection stays open for further requests. The serving client asks
its tracker (`check_access`, cached 30 s) whether `(user, token)` is a logged-in member of the group,
checks the index against the real file size, and serves only files the tracker accepted.

### Tracker ↔ tracker

`CELL⇥key⇥wall.counter.node⇥0|1⇥value\n`, `SYNC_SNAPSHOT_REQ\n`, `SYNC_HEARTBEAT\n` on the sync port.
Key parts are joined with `0x1f`, value fields with `0x1e` (neither can occur in IDs or file names).

### Robustness rules

Every `send`/`recv` loops until the full length is transferred (`sendAll`/`recvAll`/`LineReader`);
`SIGPIPE` is ignored in both programs; no 512 KB buffer lives on a thread stack; transient `accept`
failures never stop a tracker; malformed sync cells are ignored.

## 6. Assumptions

* Exactly two trackers; clients try them in the order of `tracker_info.txt`.
* IDs contain no whitespace or `:`; file names contain no `/` or control characters; paths with spaces are quoted.
* A user is logged in on one client at a time; re-logging in from another client claims the session
  if the previous connection is gone.
* The same source file may be registered by several local users (the benchmark does this).
* Clients and trackers share a trusted network: no TLS (see limitations).
* A file disappears from a group when its last seeder logs out or stops sharing (a design choice: a listed file should always be downloadable).

## 7. Implemented features

Account, group and file commands (see above); 512 KB pieces with SHA-1 per piece and per file; multi-peer parallel
download with load balancing and retry; concurrent downloads; automatic seeding after a download;
`stop_share`/`become_seeder`; two-tracker replication with failover, restart catch-up and partition
healing; `quit` on the tracker; peer authorization; status codes.

## 8. Limitations

* Exactly two trackers. Concurrent *conflicting* edits to the same single record during a
  partition resolve by latest stamp (e.g. the same user name created on both sides — one wins; a
  seeder re-registration racing with the removal of its file — the later event wins).
* Cell tombstones are never garbage-collected.
* Passwords cross the network in clear text between client and tracker (no TLS); they are
  stored and replicated as `salt$SHA-256(salt‖password)`.
* What a client is serving lives in memory. After restarting a client the tracker still lists you as a
  seeder, but your peer server forgot the paths: run `become_seeder` once. Downloaders meanwhile fall through to other peers.
* Only complete files are seeded (no per-peer piece bitmaps, no rarest-first).
* Developed and tested on macOS (Apple clang, arm64). The code is POSIX-only and CI
  (`.github/workflows/ci.yml`) builds it with g++ on Ubuntu, but I have not seen a Linux run yet.

## 9. Libraries

C/C++ standard library and POSIX system calls, plus **OpenSSL libcrypto** (SHA-1/SHA-256/random bytes
through the `EVP_*` API) and **GNU readline** (command-line editing/history). No filesystem library
(`<filesystem>`), no database or torrent library, and no `system()`, `exec*()`, `popen()` or `fork()`
anywhere. OpenSSL is confined to `net_util.h`, so it could be swapped for a self-contained SHA-1 without touching anything else.

## 10. Testing

```bash
python3 scripts/integration_test.py          # 96 checks against real tracker + client processes
make -C tracker asan && make -C client asan  # then run the same suite under AddressSanitizer + UBSan:
P2P_TRACKER_BIN=$PWD/tracker/tracker-asan P2P_CLIENT_BIN=$PWD/client/client-asan python3 scripts/integration_test.py
# likewise `make debug` + tracker-debug/client-debug for ThreadSanitizer
P2P_LEAKCHECK=1 python3 scripts/integration_test.py   # macOS: runs `leaks <pid>` on every live process
```

What the suite covers: upload/download with spaces in names and byte-for-byte comparison; file sizes
of 0 bytes, 84 bytes, exactly 1 piece, 1 piece + 1 byte, exactly 3 pieces, ~1 MB executable, PNG-like
binary, `.tar.gz`, `.zip`; auto-seeding (a third user downloads from the first downloader alone); even
load balancing; two simultaneous downloads on one client; retry when a seeder is killed mid-download;
clean failure when none is left (no partial file); tracker kill → failover → SIGPIPE survival → restart
catch-up; **network partition between two live trackers with changes and deletions on both sides, then
heal** (a TCP proxy cuts the sync channel); disconnect ≡ logout on both trackers; pipelined and glued
messages on the client and sync channels (including a 200 KB message dribbled in 1.5 KB pieces and
cells arriving before their parents); stale-stamp / tombstone rules; malformed sync cells; peer-server
authorization, bounds, persistent connections; destination protection; status codes 200/400/401/403/404/409/416.

Manual smoke test: start both trackers, two clients; on A `create_user`, `login`, `create_group g`,
`upload_file g <file>`; on B `create_user`, `login`, `join_group g`; A `accept_request g B`; B
`download_file g <name> <dir>` and `show_downloads`; kill tracker 1 and keep typing commands on both clients.

### Benchmark

`scripts/benchmark.py` downloads a random 1 GB file with 1, 2 and 4 seeders (3 measured runs each,
one discarded warm-up, median). Apple M4 (10 cores), macOS 27, loopback, 8 workers:

**Unthrottled** (`python3 scripts/benchmark.py`):

| Seeders | Median transfer time | Throughput | vs 1 seeder |
|---|---|---|---|
| 1 | 0.175 s | 5,851 MB/s | 1.00× |
| 2 | 0.181 s | 5,657 MB/s | 0.97× |
| 4 | 0.185 s | 5,535 MB/s | 0.95× |

On loopback the limit is the downloader's own CPU (SHA-1 of every piece) and memcpy, so extra seeders
do **not** help; each seeder still serves exactly 1/N of the pieces. Whole-file verification adds ~0.9 s.

**Seeder uplink capped at 100 MB/s each** (`python3 scripts/benchmark.py --upload-mbps 100`):

| Seeders | Median transfer time | Throughput | vs 1 seeder |
|---|---|---|---|
| 1 | 10.248 s | 100 MB/s | 1.00× |
| 2 | 5.128 s | 200 MB/s | 2.00× |
| 4 | 2.572 s | 398 MB/s | 3.98× |

⚠ This table is an **emulation**: a token bucket in the peer server (`P2P_UPLOAD_KBPS`) makes each
seeder behave like a 100 MB/s uplink. It shows the engine keeps every seeder busy and splits work
evenly; it is not a measurement of a real network.

## 11. Repository layout

```
tracker/   tracker.cpp  user_manager.cpp  file_manager.cpp  store.cpp  sync_trackers.cpp  header.h  net_util.h  tracker_info.h  Makefile
client/    client.cpp  peer_server.cpp  downloader.cpp  client.h  net_util.h  tracker_info.h  Makefile
scripts/   integration_test.py  benchmark.py  harness.py
docs/      TECHNICAL_REPORT.tex  (compiled copy: Technical_Report.pdf in the repository root)
.github/workflows/ci.yml
tracker_info.txt
```
