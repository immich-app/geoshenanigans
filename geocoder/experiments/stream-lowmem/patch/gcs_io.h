// In-process codec I/O for the low-memory client update: buffered pread / pwrite over regular
// files (the encoded installed set and the new encoded set), byte queues between the patcher and
// the codecs, and the gc manifest. Small buffers: the kernel's sequential readahead decides the
// request size, not the buffer (the md4 small-device profile caps a request at 128 KiB anyway).
#pragma once

#include <algorithm>
#include <cerrno>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace gcs {

[[noreturn]] inline void die(const char* fmt, ...);
static const uint32_t NO = 0xFFFFFFFFu;
// Default buffer sizes (bytes).
static const size_t READ_BUF = size_t(64) << 10, WRITE_BUF = size_t(64) << 10, PLANE_BUF = size_t(16) << 10;
// A polygon block up to this size is transformed in memory; a larger one plane by plane through
// positional readers / writers, so no block is ever held whole. GCS_BIG_BLOCK overrides it (tests).
inline size_t big_block() {
    static const size_t b = [] {
        const char* e = getenv("GCS_BIG_BLOCK");
        return e ? size_t(strtoull(e, nullptr, 10)) : (size_t(256) << 10);
    }();
    return b;
}

[[noreturn]] inline void die(const char* fmt, ...) {
    char msg[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof msg, fmt, ap);
    va_end(ap);
    throw std::runtime_error(std::string("gcs: ") + msg);
}

// Exactly n bytes at `at`: interrupted and short transfers go on where they stopped; an error, or a
// transfer of nothing (a read past the end), fails.
inline void pread_all(int fd, void* p, size_t n, uint64_t at, const std::string& path) {
    uint8_t* b = static_cast<uint8_t*>(p);
    while (n) {
        const ssize_t r = pread(fd, b, n, off_t(at));
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) die("read %s: %s", path.c_str(), r ? strerror(errno) : "unexpected end of file");
        b += r;
        n -= size_t(r);
        at += uint64_t(r);
    }
}
inline void pwrite_all(int fd, const void* p, size_t n, uint64_t at, const std::string& path) {
    const uint8_t* b = static_cast<const uint8_t*>(p);
    while (n) {
        const ssize_t w = pwrite(fd, b, n, off_t(at));
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) die("write %s: %s", path.c_str(), w ? strerror(errno) : "nothing written");
        b += w;
        n -= size_t(w);
        at += uint64_t(w);
    }
}

static inline uint16_t rd16(const uint8_t* p) { uint16_t v; memcpy(&v, p, 2); return v; }
static inline uint32_t rd32(const uint8_t* p) { uint32_t v; memcpy(&v, p, 4); return v; }
static inline uint64_t rd64(const uint8_t* p) { uint64_t v; memcpy(&v, p, 8); return v; }
static inline void wr16(uint8_t* p, uint16_t v) { memcpy(p, &v, 2); }
static inline void wr32(uint8_t* p, uint32_t v) { memcpy(p, &v, 4); }
static inline void wr64(uint8_t* p, uint64_t v) { memcpy(p, &v, 8); }
static inline uint64_t zz64(int64_t v) { return (uint64_t(v) << 1) ^ uint64_t(v >> 63); }
static inline int64_t unzz64(uint64_t z) { return int64_t(z >> 1) ^ -int64_t(z & 1); }

inline void put_uv(std::vector<uint8_t>& o, uint64_t v) {
    while (v >= 0x80) { o.push_back(uint8_t(v) | 0x80); v >>= 7; }
    o.push_back(uint8_t(v));
}

inline uint64_t file_size(const std::string& p) {
    struct stat st;
    if (stat(p.c_str(), &st)) die("stat %s: %s", p.c_str(), strerror(errno));
    return uint64_t(st.st_size);
}

// Sequential reader over [start, stop) of a regular file.
struct Reader {
    int fd = -1;
    std::string path;
    uint64_t end = 0, fpos = 0;  // fpos: file offset of buf[0]
    std::vector<uint8_t> buf;
    size_t len = 0, cur = 0, cap;

    explicit Reader(const std::string& p, size_t cap_ = READ_BUF, uint64_t start = 0, uint64_t stop = ~0ull) : path(p), cap(cap_) {
        fd = open(p.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) die("open %s: %s", p.c_str(), strerror(errno));
        struct stat st;
        fstat(fd, &st);
        end = std::min<uint64_t>(uint64_t(st.st_size), stop);
        posix_fadvise(fd, off_t(start), stop == ~0ull ? 0 : off_t(stop - start), POSIX_FADV_SEQUENTIAL);
        fpos = start;
    }
    Reader(const Reader&) = delete;
    ~Reader() {
        if (fd >= 0) close(fd);
    }
    uint64_t size() const { return end; }
    uint64_t tell() const { return fpos + cur; }
    uint64_t left() const { return end - tell(); }
    bool eof() const { return tell() >= end; }
    // Moves the cursor forward without reading what it passes.
    void seek(uint64_t to) {
        if (to < tell()) die("%s: backward seek", path.c_str());
        if (to <= fpos + len) { cur = size_t(to - fpos); return; }
        fpos = to;
        len = cur = 0;
    }
    inline const uint8_t* need(size_t n, bool pad = false) {
        if (cur + n <= len) return &buf[cur];
        return refill(n, pad);
    }
    const uint8_t* refill(size_t n, bool pad) {
        if (buf.empty()) buf.resize(cap + 64);
        if (cur) {
            memmove(&buf[0], &buf[cur], len - cur);
            fpos += cur;
            len -= cur;
            cur = 0;
        }
        if (n > cap) {  // a view larger than the buffer grows it (only a single large record does)
            cap = n;
            buf.resize(cap + 64);
        }
        while (len < cap) {
            const uint64_t at = fpos + len;
            if (at >= end) break;
            const size_t want = size_t(std::min<uint64_t>(cap - len, end - at));
            const ssize_t r = pread(fd, &buf[len], want, off_t(at));
            if (r < 0) {
                if (errno == EINTR) continue;
                die("read %s: %s", path.c_str(), strerror(errno));
            }
            if (r == 0) break;
            len += size_t(r);
        }
        if (len < n) {
            if (!pad) die("%s: unexpected end of file at %llu (need %zu)", path.c_str(), (unsigned long long)tell(), n);
            memset(&buf[len], 0, n - len);
        }
        return &buf[0];
    }
    inline const uint8_t* take(size_t n) {
        const uint8_t* p = need(n);
        cur += n;
        return p;
    }
    inline void skip(size_t n) { take(n); }
    inline uint8_t u8() { return *take(1); }
    inline uint16_t u16() { return rd16(take(2)); }
    inline uint32_t u32() { return rd32(take(4)); }
    inline uint64_t uv() {
        if (cur + 10 <= len) {
            const uint8_t* p = &buf[cur];
            uint64_t v = 0;
            int sh = 0, i = 0;
            for (;;) {
                const uint8_t c = p[i++];
                v |= uint64_t(c & 0x7f) << sh;
                if (!(c & 0x80)) break;
                sh += 7;
                if (sh > 63) die("%s: bad varint", path.c_str());
            }
            cur += size_t(i);
            return v;
        }
        uint64_t v = 0;
        int sh = 0;
        for (;;) {
            const uint8_t c = u8();
            v |= uint64_t(c & 0x7f) << sh;
            if (!(c & 0x80)) return v;
            sh += 7;
            if (sh > 63) die("%s: bad varint", path.c_str());
        }
    }
};

struct Writer {
    int fd = -1;
    bool own = true;
    std::string path;
    uint64_t base = 0, written = 0;
    std::vector<uint8_t> buf;
    size_t len = 0, cap;

    // A fresh inode (unlink first). rw: also readable (a writer that rereads what it wrote).
    explicit Writer(const std::string& p, size_t cap_ = WRITE_BUF, bool rw = false) : path(p), cap(cap_) {
        if (unlink(p.c_str()) && errno != ENOENT) die("unlink %s: %s", p.c_str(), strerror(errno));
        fd = open(p.c_str(), (rw ? O_RDWR : O_WRONLY) | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
        if (fd < 0) die("create %s: %s", p.c_str(), strerror(errno));
        buf.resize(cap);
    }
    // A region of a file the caller owns.
    Writer(int shared_fd, const std::string& p, uint64_t region_base, size_t cap_)
        : fd(shared_fd), own(false), path(p), base(region_base), cap(cap_) {
        buf.resize(cap);
    }
    Writer(const Writer&) = delete;
    // Cleanup only: what finish() has not written is dropped. A destructor runs inside
    // unique_ptr::reset() and vector::clear() (noexcept) and while an error unwinds, where a failed
    // write could only terminate the process; so every writer's last bytes go out through finish().
    ~Writer() {
        if (fd >= 0 && own) close(fd);
    }
    uint64_t tell() const { return written + len; }
    // Moves the write position forward to `at` (what lies between was written positionally).
    void skip_to(uint64_t at) {
        flush();
        written = at - base;
    }
    void flush() {
        pwrite_all(fd, buf.data(), len, base + written, path);
        written += len;
        len = 0;
    }
    // Writes the buffered bytes and closes the file (a region writer leaves the shared fd open).
    // A failed write leaves the descriptor to the destructor, which closes it without retrying.
    void finish() {
        if (fd < 0) return;
        flush();
        const int f = fd;
        fd = -1;
        if (own && close(f)) die("close %s: %s", path.c_str(), strerror(errno));
    }
    inline void put(const void* p, size_t n) {
        if (len + n > cap) {
            flush();
            if (n > cap) {  // larger than the buffer: straight through
                pwrite_all(fd, p, n, base + written, path);
                written += n;
                return;
            }
        }
        memcpy(&buf[len], p, n);
        len += n;
    }
    inline void put(const std::vector<uint8_t>& v) { if (!v.empty()) put(v.data(), v.size()); }
    inline void u8(uint8_t v) {
        if (len + 1 > cap) flush();
        buf[len++] = v;
    }
    inline void u16(uint16_t v) { put(&v, 2); }
    inline void u32(uint32_t v) { put(&v, 4); }
    inline void u64(uint64_t v) { put(&v, 8); }
    inline void uv(uint64_t v) {
        if (len + 10 > cap) flush();
        while (v >= 0x80) { buf[len++] = uint8_t(v) | 0x80; v >>= 7; }
        buf[len++] = uint8_t(v);
    }
};

// FIFO of bytes: appended at the back, consumed from the front. peak: the most it ever held;
// cap_peak: the largest buffer it ever had.
struct ByteQueue {
    uint8_t* b = nullptr;
    size_t cap = 0, head = 0, tail = 0, peak = 0, cap_peak = 0;
    ByteQueue() = default;
    ByteQueue(const ByteQueue&) = delete;
    ByteQueue& operator=(const ByteQueue&) = delete;
    ByteQueue(ByteQueue&& o) noexcept : b(o.b), cap(o.cap), head(o.head), tail(o.tail), peak(o.peak), cap_peak(o.cap_peak) {
        o.b = nullptr;
        o.cap = o.head = o.tail = 0;
    }
    ByteQueue& operator=(ByteQueue&& o) noexcept {
        if (this != &o) {
            free(b);
            b = o.b; cap = o.cap; head = o.head; tail = o.tail; peak = std::max(peak, o.peak); cap_peak = std::max(cap_peak, o.cap_peak);
            o.b = nullptr;
            o.cap = o.head = o.tail = 0;
        }
        return *this;
    }
    ~ByteQueue() { free(b); }
    size_t size() const { return tail - head; }
    const uint8_t* data() const { return b + head; }
    // Room for n more bytes at the back, for the caller to fill (uninitialized).
    uint8_t* reserve(size_t n) {
        if (head == tail) head = tail = 0;
        if (tail + n > cap) {
            if (head) {  // compact before growing
                memmove(b, b + head, tail - head);
                tail -= head;
                head = 0;
            }
            if (tail + n > cap) {
                size_t nc = std::max<size_t>(std::max<size_t>(cap * 2, tail + n), 4096);
                uint8_t* nb = static_cast<uint8_t*>(realloc(b, nc));
                if (!nb) die("out of memory (%zu bytes)", nc);
                b = nb;
                cap = nc;
                cap_peak = std::max(cap_peak, cap);
            }
        }
        uint8_t* p = b + tail;
        tail += n;
        peak = std::max(peak, tail - head);
        return p;
    }
    void push(const void* p, size_t n) {
        if (n) memcpy(reserve(n), p, n);
    }
    // Drops the contents and the buffer (the peaks stay).
    void reset() {
        free(b);
        b = nullptr;
        cap = head = tail = 0;
    }
    void pop(size_t n) {
        head += n;
        if (head >= tail) {
            head = tail = 0;
            // A queue that once held a large unit gives the memory back when it drains.
            if (cap > (size_t(256) << 10)) {
                free(b);
                b = nullptr;
                cap = 0;
            }
        }
    }
};

// key value pairs kept next to the encoded files (gc.manifest)
struct Manifest {
    std::map<std::string, std::string> kv;
    void set(const std::string& k, const std::string& v) { kv[k] = v; }
    void set(const std::string& k, uint64_t v) { kv[k] = std::to_string(v); }
    bool has(const std::string& k) const { return kv.count(k) != 0; }
    const std::string& get(const std::string& k) const {
        auto it = kv.find(k);
        if (it == kv.end()) die("manifest: missing %s", k.c_str());
        return it->second;
    }
    uint64_t num(const std::string& k) const { return std::stoull(get(k)); }
    void save(const std::string& path) const {
        std::string s;
        for (auto& [k, v] : kv) s += k + " " + v + "\n";
        Writer w(path, 1 << 12);
        w.put(s.data(), s.size());
        w.finish();
    }
    void load(const std::string& path) {
        FILE* f = fopen(path.c_str(), "r");
        if (!f) die("open %s", path.c_str());
        char line[4096];
        while (fgets(line, sizeof line, f)) {
            std::string l = line;
            while (!l.empty() && (l.back() == '\n' || l.back() == '\r')) l.pop_back();
            const size_t sp = l.find(' ');
            if (sp == std::string::npos) continue;
            kv[l.substr(0, sp)] = l.substr(sp + 1);
        }
        fclose(f);
    }
};

}  // namespace gcs
