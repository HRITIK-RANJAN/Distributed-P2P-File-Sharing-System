// Shared helpers used by both the tracker and the client:
//   - sendAll / recvAll / LineReader : full-transfer socket I/O (handles partial reads/writes)
//   - length-prefixed response framing
//   - SHA-1 / SHA-256 via the OpenSSL EVP API, random tokens
#ifndef NET_UTIL_H
#define NET_UTIL_H

#include <openssl/evp.h>
#include <openssl/rand.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

// 512 KB pieces, as required by the assignment.
static const int PIECE_SIZE = 512 * 1024;

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0  // macOS: SIGPIPE is ignored process-wide in main() instead
#endif

// ---------------------------------------------------------------------------
// Socket I/O
// ---------------------------------------------------------------------------

// Writes exactly len bytes, retrying on short writes and EINTR.
inline bool sendAll(int fd, const void *buf, size_t len) {
    const char *p = static_cast<const char *>(buf);
    while (len > 0) {
        ssize_t n = send(fd, p, len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;
        p += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

inline bool sendAll(int fd, const std::string &s) { return sendAll(fd, s.data(), s.size()); }

// Reads exactly len bytes. Returns false on EOF, timeout or error.
inline bool recvAll(int fd, void *buf, size_t len) {
    char *p = static_cast<char *>(buf);
    while (len > 0) {
        ssize_t n = recv(fd, p, len, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;
        p += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

// Buffered reader: yields '\n'-terminated lines and exact-length blobs from the same
// stream without losing bytes that arrived together with a line.
class LineReader {
public:
    explicit LineReader(int fd = -1, size_t maxLine = 64u * 1024 * 1024) : fd_(fd), maxLine_(maxLine) {}

    void reset(int fd) {
        fd_ = fd;
        buf_.clear();
        pos_ = 0;
    }

    bool readLine(std::string &out) {
        while (true) {
            size_t nl = buf_.find('\n', pos_);
            if (nl != std::string::npos) {
                out.assign(buf_, pos_, nl - pos_);
                pos_ = nl + 1;
                compact();
                if (!out.empty() && out.back() == '\r') out.pop_back();
                return true;
            }
            if (buf_.size() - pos_ > maxLine_) return false;
            if (!fill()) return false;
        }
    }

    bool readExact(void *dst, size_t len) {
        char *p = static_cast<char *>(dst);
        size_t have = buf_.size() - pos_;
        size_t take = have < len ? have : len;
        if (take > 0) {
            memcpy(p, buf_.data() + pos_, take);
            pos_ += take;
            compact();
            p += take;
            len -= take;
        }
        return len == 0 || recvAll(fd_, p, len);
    }

    bool readExact(std::string &out, size_t len) {
        out.resize(len);
        return len == 0 || readExact(&out[0], len);
    }

private:
    bool fill() {
        char tmp[8192];
        while (true) {
            ssize_t n = recv(fd_, tmp, sizeof(tmp), 0);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) return false;
            buf_.append(tmp, static_cast<size_t>(n));
            return true;
        }
    }
    void compact() {
        if (pos_ > 65536 || pos_ == buf_.size()) {
            buf_.erase(0, pos_);
            pos_ = 0;
        }
    }

    int fd_;
    size_t maxLine_;
    std::string buf_;
    size_t pos_ = 0;
};

// Status codes carried in every tracker -> client frame and in peer ERROR replies (HTTP-like).
enum StatusCode {
    ST_OK = 200,
    ST_BAD_REQUEST = 400,   // usage error, malformed or invalid argument
    ST_UNAUTHORIZED = 401,  // not logged in / bad credentials / unknown session
    ST_FORBIDDEN = 403,     // logged in but not allowed (not a member, not the owner, ...)
    ST_NOT_FOUND = 404,
    ST_CONFLICT = 409,      // already exists / already logged in elsewhere
    ST_RANGE = 416,         // piece index outside the file
    ST_INTERNAL = 500
};

// Frame: "<payload length> <status code>\n<payload>". Used for every tracker -> client response
// so the receiver never has to guess where a response ends or whether it succeeded.
inline bool sendFramed(int fd, const std::string &payload, int status = ST_OK) {
    std::string frame = std::to_string(payload.size()) + " " + std::to_string(status) + "\n";
    frame += payload;
    return sendAll(fd, frame);
}

inline bool recvFramed(LineReader &reader, std::string &payload, int *status = nullptr) {
    std::string header;
    if (!reader.readLine(header)) return false;
    unsigned long long len = 0;
    int code = 0;
    if (sscanf(header.c_str(), "%llu %d", &len, &code) != 2 || len > (256ull << 20)) return false;
    if (status) *status = code;
    return reader.readExact(payload, static_cast<size_t>(len));
}

// ---------------------------------------------------------------------------
// String helpers
// ---------------------------------------------------------------------------

inline std::vector<std::string> splitStr(const std::string &s, char delim) {
    std::vector<std::string> parts;
    size_t start = 0;
    while (true) {
        size_t pos = s.find(delim, start);
        if (pos == std::string::npos) {
            parts.push_back(s.substr(start));
            break;
        }
        parts.push_back(s.substr(start, pos - start));
        start = pos + 1;
    }
    return parts;
}

inline std::string joinStr(const std::vector<std::string> &v, const std::string &sep) {
    std::string out;
    for (size_t i = 0; i < v.size(); i++) {
        if (i) out += sep;
        out += v[i];
    }
    return out;
}

// ---------------------------------------------------------------------------
// Hashing (OpenSSL EVP API; the SHA1_* functions are deprecated in OpenSSL 3)
// ---------------------------------------------------------------------------

inline std::string toHex(const unsigned char *data, size_t len) {
    static const char *digits = "0123456789abcdef";
    std::string out(len * 2, '0');
    for (size_t i = 0; i < len; i++) {
        out[2 * i] = digits[data[i] >> 4];
        out[2 * i + 1] = digits[data[i] & 0x0f];
    }
    return out;
}

class Digest {
public:
    explicit Digest(const EVP_MD *md) : ctx_(EVP_MD_CTX_new()) { EVP_DigestInit_ex(ctx_, md, nullptr); }
    ~Digest() { EVP_MD_CTX_free(ctx_); }
    Digest(const Digest &) = delete;
    Digest &operator=(const Digest &) = delete;

    void update(const void *data, size_t len) { EVP_DigestUpdate(ctx_, data, len); }

    std::string finalHex() {
        unsigned char out[EVP_MAX_MD_SIZE];
        unsigned int outLen = 0;
        EVP_DigestFinal_ex(ctx_, out, &outLen);
        return toHex(out, outLen);
    }

private:
    EVP_MD_CTX *ctx_;
};

inline std::string sha1Hex(const void *data, size_t len) {
    Digest d(EVP_sha1());
    d.update(data, len);
    return d.finalHex();
}

inline std::string sha256Hex(const std::string &s) {
    Digest d(EVP_sha256());
    d.update(s.data(), s.size());
    return d.finalHex();
}

inline std::string randomHex(size_t bytes) {
    std::vector<unsigned char> raw(bytes);
    if (RAND_bytes(raw.data(), static_cast<int>(bytes)) != 1) {
        // Extremely unlikely; fall back to the kernel RNG.
        FILE *f = fopen("/dev/urandom", "rb");
        if (f) {
            size_t got = fread(raw.data(), 1, bytes, f);
            (void)got;
            fclose(f);
        }
    }
    return toHex(raw.data(), bytes);
}

// Hashes a whole file and each PIECE_SIZE piece in a single pass.
// Returns false (and sets err) if the file cannot be read.
inline bool hashFilePieces(const std::string &path, std::string &fileHash,
                           std::vector<std::string> &pieceHashes, long long &fileSize,
                           std::string &err) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        err = std::string("cannot open file: ") + strerror(errno);
        return false;
    }
    struct stat st;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)) {
        close(fd);
        err = "not a regular file";
        return false;
    }
    fileSize = st.st_size;

    std::vector<unsigned char> buf(PIECE_SIZE);  // heap: a 512 KB stack array overflows secondary thread stacks
    Digest whole(EVP_sha1());
    pieceHashes.clear();
    long long done = 0;
    while (done < fileSize) {
        size_t want = static_cast<size_t>(fileSize - done < PIECE_SIZE ? fileSize - done : PIECE_SIZE);
        size_t got = 0;
        while (got < want) {
            ssize_t n = pread(fd, buf.data() + got, want - got, done + static_cast<long long>(got));
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) {
                close(fd);
                err = "short read while hashing (file changed?)";
                return false;
            }
            got += static_cast<size_t>(n);
        }
        whole.update(buf.data(), got);
        pieceHashes.push_back(sha1Hex(buf.data(), got));
        done += static_cast<long long>(got);
    }
    close(fd);
    fileHash = whole.finalHex();
    return true;
}

inline bool hashFile(const std::string &path, std::string &fileHash) {
    std::vector<std::string> pieces;
    long long size;
    std::string err;
    return hashFilePieces(path, fileHash, pieces, size, err);
}

#endif  // NET_UTIL_H
