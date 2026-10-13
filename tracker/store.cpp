// Replicated, versioned state.
//
// Why cells: two trackers that keep serving clients while they cannot reach each other both
// change state. When they reconnect, a plain "copy the other side's snapshot" would drop one
// side's changes and bring deleted things back. Instead every fact is a *cell*:
//
//     key  ->  { stamp, present?, value }
//
// A deletion is a cell with present=false (a tombstone) and its own stamp. Merging is "per cell,
// the higher stamp wins". That operation is commutative, associative and idempotent, so both
// trackers converge to the same state regardless of message order, duplication or how long they
// were apart.
//
// Stamps are hybrid logical clocks (wall-clock milliseconds, a counter for ties, then the
// tracker id as final tie-break), so a change made after observing another tracker's change
// always gets a higher stamp even if the machines' clocks disagree.
//
// users/groups (header.h) are *views* derived from the cells and are what the command handlers
// read. Cell kinds (key parts are joined with 0x1f, which IDs and file names can never contain):
//
//   u uid                 value = password hash           user exists
//   l uid                 value = peerAddr 0x1e token     user logged in (tombstone = logged out)
//   g gid                 value = owner id                group exists
//   m gid uid             -                               group member
//   r gid uid             -                               pending join request
//   f gid name            value = file metadata           file shared in group
//   s gid name uid        -                               uid has the complete file (seeder)
//   x uid gid name        -                               uid shares this file (restores seeding at login)
//
// A cell can arrive before the cell it hangs off (e.g. a seeder before its file during a
// snapshot). It is then only stored; when the parent cell arrives the view is filled in by a
// prefix scan over the ordered cell map.
#include "header.h"
#include <chrono>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <tuple>

namespace {

const char SEP = '\x1f';   // between key parts
const char FSEP = '\x1e';  // between fields inside a value

struct Stamp {
    unsigned long long wall = 0;
    unsigned int counter = 0;
    unsigned int node = 0;
    bool operator<(const Stamp &o) const {
        return std::tie(wall, counter, node) < std::tie(o.wall, o.counter, o.node);
    }
};

struct Cell {
    Stamp stamp;
    bool present = false;
    string value;
};

map<string, Cell> cells;   // ordered: prefix scans are lower_bound walks
Stamp lastStamp;
unsigned myNode = 1;
bool replicationEnabled = false;
vector<string> pendingMessages;

string stampToString(const Stamp &s) {
    return to_string(s.wall) + "." + to_string(s.counter) + "." + to_string(s.node);
}

bool parseStamp(const string &str, Stamp &s) {
    unsigned long long w;
    unsigned c, n;
    if (sscanf(str.c_str(), "%llu.%u.%u", &w, &c, &n) != 3) return false;
    s.wall = w;
    s.counter = c;
    s.node = n;
    return true;
}

Stamp nextStamp() {
    unsigned long long now = (unsigned long long)chrono::duration_cast<chrono::milliseconds>(
                                 chrono::system_clock::now().time_since_epoch()).count();
    if (now > lastStamp.wall) {
        lastStamp.wall = now;
        lastStamp.counter = 0;
    } else {
        lastStamp.counter++;  // same millisecond, or the clock went backwards
    }
    lastStamp.node = myNode;
    return lastStamp;
}

// Hybrid-logical-clock receive rule: never issue a stamp lower than anything we have seen.
void observeStamp(const Stamp &s) {
    if (lastStamp.wall < s.wall || (lastStamp.wall == s.wall && lastStamp.counter < s.counter)) {
        lastStamp.wall = s.wall;
        lastStamp.counter = s.counter;
    }
}

string makeKey(char kind, const string &a, const string &b = "", const string &c = "", int parts = 1) {
    string k(1, kind);
    k += SEP;
    k += a;
    if (parts >= 2) { k += SEP; k += b; }
    if (parts >= 3) { k += SEP; k += c; }
    return k;
}

string formatMessage(const string &key, const Cell &c) {
    return "CELL\t" + key + "\t" + stampToString(c.stamp) + "\t" + (c.present ? "1" : "0") + "\t" + c.value;
}

// ---- file metadata <-> cell value ----

string serializeFile(const FileInfo &f) {
    return f.uploaderId + FSEP + to_string(f.fileSize) + FSEP + to_string(f.totalPieces) + FSEP +
           f.fileHash + FSEP + joinStr(f.pieceHashes, " ");
}

bool parseFile(const string &name, const string &value, FileInfo &f) {
    vector<string> p = splitStr(value, FSEP);
    if (p.size() != 5) return false;
    f.fileName = name;
    f.uploaderId = p[0];
    f.fileSize = atoll(p[1].c_str());
    f.totalPieces = atoi(p[2].c_str());
    f.fileHash = p[3];
    f.pieceHashes.clear();
    if (!p[4].empty()) f.pieceHashes = splitStr(p[4], ' ');
    return true;
}

// ---- view maintenance (dataMutex held) ----

bool startsWith(const string &s, const string &prefix) { return s.compare(0, prefix.size(), prefix) == 0; }

// Calls fn(remainderOfKey, cell) for every cell whose key starts with `prefix`.
template <typename Fn>
void forEachWithPrefix(const string &prefix, Fn fn) {
    for (auto it = cells.lower_bound(prefix); it != cells.end() && startsWith(it->first, prefix); ++it) {
        fn(it->first.substr(prefix.size()), it->second);
    }
}

void fillSeeders(const string &gid, FileInfo &f) {
    f.seeders.clear();
    forEachWithPrefix(makeKey('s', gid, f.fileName, "", 2) + SEP, [&](const string &uid, const Cell &c) {
        if (c.present) f.seeders.insert(uid);
    });
}

void viewLogin(const string &uid, const Cell *c) {
    auto uit = users.find(uid);
    if (uit == users.end()) return;
    User &u = uit->second;
    if (c && c->present) {
        vector<string> p = splitStr(c->value, FSEP);
        u.loggedIn = true;
        u.peerAddress = p.size() > 0 ? p[0] : "";
        u.token = p.size() > 1 ? p[1] : "";
    } else {
        u.loggedIn = false;
        u.peerAddress.clear();
        u.token.clear();
    }
}

void viewFile(const string &gid, const string &name, const Cell &c) {
    auto git = groups.find(gid);
    if (git == groups.end()) return;
    if (!c.present) {
        git->second.files.erase(name);
        return;
    }
    FileInfo f;
    if (!parseFile(name, c.value, f)) return;
    fillSeeders(gid, f);
    git->second.files[name] = f;
}

void applyToView(const string &key, const Cell &c) {
    vector<string> k = splitStr(key, SEP);
    char kind = k[0][0];

    switch (kind) {
    case 'u': {
        const string &uid = k[1];
        if (!c.present) { users.erase(uid); break; }
        bool fresh = !users.count(uid);
        User &u = users[uid];
        u.userId = uid;
        u.passHash = c.value;
        if (fresh) {
            auto lit = cells.find(makeKey('l', uid));
            viewLogin(uid, lit == cells.end() ? nullptr : &lit->second);
            forEachWithPrefix(makeKey('x', uid) + SEP, [&](const string &rest, const Cell &xc) {
                if (!xc.present) return;
                vector<string> gn = splitStr(rest, SEP);  // gid, name
                if (gn.size() == 2) u.uploadedFiles.insert(gn[0] + ":" + gn[1]);
            });
        }
        break;
    }
    case 'l':
        viewLogin(k[1], &c);
        break;
    case 'g': {
        const string &gid = k[1];
        if (!c.present) { groups.erase(gid); break; }
        bool fresh = !groups.count(gid);
        Group &g = groups[gid];
        g.groupId = gid;
        g.ownerId = c.value;
        if (fresh) {
            forEachWithPrefix(makeKey('m', gid) + SEP, [&](const string &uid, const Cell &mc) {
                if (mc.present) g.members.insert(uid);
            });
            forEachWithPrefix(makeKey('r', gid) + SEP, [&](const string &uid, const Cell &rc) {
                if (rc.present) g.joinRequests.insert(uid);
            });
            forEachWithPrefix(makeKey('f', gid) + SEP, [&](const string &name, const Cell &fc) {
                if (fc.present) viewFile(gid, name, fc);
            });
        }
        break;
    }
    case 'm': {
        auto git = groups.find(k[1]);
        if (git == groups.end()) break;
        if (c.present) git->second.members.insert(k[2]);
        else git->second.members.erase(k[2]);
        break;
    }
    case 'r': {
        auto git = groups.find(k[1]);
        if (git == groups.end()) break;
        if (c.present) git->second.joinRequests.insert(k[2]);
        else git->second.joinRequests.erase(k[2]);
        break;
    }
    case 'f':
        viewFile(k[1], k[2], c);
        break;
    case 's': {
        auto git = groups.find(k[1]);
        if (git == groups.end()) break;
        auto fit = git->second.files.find(k[2]);
        if (fit == git->second.files.end()) break;
        if (c.present) fit->second.seeders.insert(k[3]);
        else fit->second.seeders.erase(k[3]);
        break;
    }
    case 'x': {
        auto uit = users.find(k[1]);
        if (uit == users.end()) break;
        string entry = k[2] + ":" + k[3];
        if (c.present) uit->second.uploadedFiles.insert(entry);
        else uit->second.uploadedFiles.erase(entry);
        break;
    }
    default:
        break;
    }
}

// Local mutation: stamp, store, update the view, queue for the other tracker.
void setCell(const string &key, bool present, const string &value) {
    auto it = cells.find(key);
    if (it == cells.end() ? !present : (it->second.present == present && it->second.value == value)) {
        return;  // nothing to change (including "delete something that does not exist")
    }
    Cell c;
    c.stamp = nextStamp();
    c.present = present;
    c.value = value;
    cells[key] = c;
    applyToView(key, cells[key]);
    if (replicationEnabled) pendingMessages.push_back(formatMessage(key, c));
}

}  // namespace

// ---------------------------------------------------------------------------
// Mutation API (callers hold dataMutex)
// ---------------------------------------------------------------------------

void setNodeId(unsigned id) { myNode = id; }

void enableReplication() {
    PlainLock lock;
    replicationEnabled = true;
}

void putUser(const string &uid, const string &passHash) { setCell(makeKey('u', uid), true, passHash); }

void setLogin(const string &uid, bool loggedIn, const string &peerAddr, const string &token) {
    setCell(makeKey('l', uid), loggedIn, loggedIn ? peerAddr + FSEP + token : "");
}

void putGroup(const string &gid, const string &owner) { setCell(makeKey('g', gid), true, owner); }

void putMember(const string &gid, const string &uid, bool present) {
    setCell(makeKey('m', gid, uid, "", 2), present, "");
}

void putRequest(const string &gid, const string &uid, bool present) {
    setCell(makeKey('r', gid, uid, "", 2), present, "");
}

void putFile(const string &gid, const FileInfo &info) {
    setCell(makeKey('f', gid, info.fileName, "", 2), true, serializeFile(info));
}

void removeFile(const string &gid, const string &name) {
    setCell(makeKey('f', gid, name, "", 2), false, "");
}

void putSeeder(const string &gid, const string &name, const string &uid, bool present) {
    setCell(makeKey('s', gid, name, uid, 3), present, "");
}

void putUploaded(const string &uid, const string &gid, const string &name, bool present) {
    setCell(makeKey('x', uid, gid, name, 3), present, "");
}

void removeSeeder(const string &gid, const string &name, const string &uid) {
    putSeeder(gid, name, uid, false);
    auto git = groups.find(gid);
    if (git == groups.end()) return;
    auto fit = git->second.files.find(name);
    if (fit != git->second.files.end() && fit->second.seeders.empty()) removeFile(gid, name);
}

// ---------------------------------------------------------------------------
// Seeder bookkeeping
// ---------------------------------------------------------------------------

static bool splitFileKey(const string &fileKey, string &gid, string &name) {
    size_t colon = fileKey.find(':');
    if (colon == string::npos) return false;
    gid = fileKey.substr(0, colon);
    name = fileKey.substr(colon + 1);
    return true;
}

void addUserAsSeederForUploadedFiles(const string &userId) {
    auto uit = users.find(userId);
    if (uit == users.end()) return;
    vector<pair<string, string>> todo;
    for (const string &fileKey : uit->second.uploadedFiles) {
        string gid, name;
        if (!splitFileKey(fileKey, gid, name)) continue;
        auto git = groups.find(gid);
        if (git != groups.end() && git->second.files.count(name) && git->second.members.count(userId)) {
            todo.push_back({gid, name});
        }
    }
    for (auto &t : todo) putSeeder(t.first, t.second, userId, true);
}

void removeUserAsSeederFromAllFiles(const string &userId) {
    vector<pair<string, string>> todo;
    for (auto &gp : groups) {
        for (auto &fp : gp.second.files) {
            if (fp.second.seeders.count(userId)) todo.push_back({gp.first, fp.first});
        }
    }
    for (auto &t : todo) removeSeeder(t.first, t.second, userId);
}

void addUserAsSeederForGroupFiles(const string &groupId, const string &userId) {
    auto uit = users.find(userId);
    if (uit == users.end() || !groups.count(groupId)) return;
    vector<string> names;
    for (const string &fileKey : uit->second.uploadedFiles) {
        string gid, name;
        if (splitFileKey(fileKey, gid, name) && gid == groupId && groups[groupId].files.count(name)) {
            names.push_back(name);
        }
    }
    for (const string &n : names) putSeeder(groupId, n, userId, true);
}

void removeUserAsSeederFromGroupFiles(const string &groupId, const string &userId) {
    auto git = groups.find(groupId);
    if (git == groups.end()) return;
    vector<string> names;
    for (auto &fp : git->second.files) {
        if (fp.second.seeders.count(userId)) names.push_back(fp.first);
    }
    for (const string &n : names) removeSeeder(groupId, n, userId);
}

// ---------------------------------------------------------------------------
// Replication plumbing
// ---------------------------------------------------------------------------

// A malformed key from the wire must never reach applyToView(), which indexes into its parts.
static bool wellFormedKey(const string &key) {
    if (key.size() < 3 || key[1] != SEP) return false;
    size_t parts = splitStr(key, SEP).size();
    switch (key[0]) {
    case 'u': case 'l': case 'g': return parts == 2;
    case 'm': case 'r': case 'f': return parts == 3;
    case 's': case 'x': return parts == 4;
    default: return false;
    }
}

bool applyRemoteCell(const string &key, const string &stampStr, bool present, const string &value) {
    Stamp s;
    if (!wellFormedKey(key) || !parseStamp(stampStr, s)) return false;

    PlainLock lock;
    observeStamp(s);
    auto it = cells.find(key);
    if (it != cells.end() && !(it->second.stamp < s)) return false;  // we already have this or newer

    Cell c;
    c.stamp = s;
    c.present = present;
    c.value = value;
    cells[key] = c;
    applyToView(key, cells[key]);
    return true;
}

vector<string> snapshotMessages() {
    PlainLock lock;
    vector<string> out;
    out.reserve(cells.size());
    for (auto &kv : cells) out.push_back(formatMessage(kv.first, kv.second));
    return out;
}

vector<string> takePendingMessages() {
    PlainLock lock;
    vector<string> out;
    out.swap(pendingMessages);
    return out;
}
