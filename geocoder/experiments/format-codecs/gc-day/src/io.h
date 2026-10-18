// Streaming file IO for the gc codecs: every encoder and decoder reads its inputs and writes its
// outputs sequentially through large buffers (pread / pwrite), so memory stays bounded by the
// buffers and the per-family state, whatever the file sizes.
#pragma once

#include <algorithm>
#include <cerrno>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <map>
#include <string>
#include <vector>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

static const uint32_t NO = 0xFFFFFFFFu;

[[noreturn]] inline void die(const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "gc: FATAL: ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
    exit(2);
}

inline bool exists(const std::string& p) {
    struct stat st;
    return stat(p.c_str(), &st) == 0;
}

inline uint64_t file_size(const std::string& p) {
    struct stat st;
    if (stat(p.c_str(), &st)) die("stat %s: %s", p.c_str(), strerror(errno));
    return uint64_t(st.st_size);
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

struct Reader {
    int fd = -1;
    std::string path;
    uint64_t end = 0;   // region end (file size by default)
    uint64_t fpos = 0;  // file offset of buf[0]
    std::vector<uint8_t> buf;
    size_t len = 0, cur = 0, cap;

    explicit Reader(const std::string& p, size_t cap_ = size_t(4) << 20, uint64_t start = 0, uint64_t stop = ~0ull)
        : path(p), cap(cap_) {
        fd = open(p.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) die("open %s: %s", p.c_str(), strerror(errno));
        struct stat st;
        fstat(fd, &st);
        end = std::min<uint64_t>(uint64_t(st.st_size), stop);
        posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
        buf.resize(cap + 64);
        fpos = start;
    }
    Reader(const Reader&) = delete;
    ~Reader() { if (fd >= 0) close(fd); }
    uint64_t size() const { return end; }
    uint64_t tell() const { return fpos + cur; }
    uint64_t left() const { return end - tell(); }
    bool eof() const { return tell() >= end; }

    // n contiguous bytes at the cursor; with pad, bytes past the end read as zero (not consumed).
    inline const uint8_t* need(size_t n, bool pad = false) {
        if (cur + n <= len) return &buf[cur];
        return refill(n, pad);
    }
    const uint8_t* refill(size_t n, bool pad) {
        if (cur) {
            memmove(&buf[0], &buf[cur], len - cur);
            fpos += cur;
            len -= cur;
            cur = 0;
        }
        if (n > cap) {
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
    inline uint64_t u64() { return rd64(take(8)); }
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

    // A fresh inode (unlink first): output dirs hold hardlinks of input files, which truncating in
    // place would rewrite.
    explicit Writer(const std::string& p, size_t cap_ = size_t(4) << 20) : path(p), cap(cap_) {
        if (unlink(p.c_str()) && errno != ENOENT) die("unlink %s: %s", p.c_str(), strerror(errno));
        fd = open(p.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
        if (fd < 0) die("create %s: %s", p.c_str(), strerror(errno));
        buf.resize(cap);
    }
    // A region of a file another Writer (or the caller) owns.
    Writer(int shared_fd, const std::string& p, uint64_t region_base, size_t cap_)
        : fd(shared_fd), own(false), path(p), base(region_base), cap(cap_) {
        buf.resize(cap);
    }
    Writer(const Writer&) = delete;
    ~Writer() { finish(); }
    uint64_t tell() const { return written + len; }
    void flush() {
        size_t off = 0;
        while (off < len) {
            const ssize_t w = pwrite(fd, &buf[off], len - off, off_t(base + written + off));
            if (w < 0) {
                if (errno == EINTR) continue;
                die("write %s: %s", path.c_str(), strerror(errno));
            }
            off += size_t(w);
        }
        written += len;
        len = 0;
    }
    void finish() {
        if (fd < 0) return;
        flush();
        if (own && close(fd)) die("close %s: %s", path.c_str(), strerror(errno));
        fd = -1;
    }
    inline void put(const void* p, size_t n) {
        if (len + n > cap) {
            flush();
            if (n > cap) {
                cap = n;
                buf.resize(cap);
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

// Copies a file through the buffers (fallback when a hardlink is not possible).
inline void copy_file(const std::string& src, const std::string& dst) {
    Reader r(src);
    Writer w(dst);
    while (!r.eof()) {
        const size_t n = size_t(std::min<uint64_t>(r.left(), 1 << 20));
        w.put(r.take(n), n);
    }
}

inline void link_or_copy(const std::string& src, const std::string& dst) {
    if (unlink(dst.c_str()) && errno != ENOENT) die("unlink %s: %s", dst.c_str(), strerror(errno));
    if (link(src.c_str(), dst.c_str()) == 0) return;
    if (errno != EXDEV && errno != EPERM) die("link %s -> %s: %s", src.c_str(), dst.c_str(), strerror(errno));
    copy_file(src, dst);
}

inline std::vector<std::string> list_files(const std::string& dir) {
    std::vector<std::string> out;
    DIR* d = opendir(dir.c_str());
    if (!d) die("opendir %s: %s", dir.c_str(), strerror(errno));
    while (dirent* e = readdir(d)) {
        const std::string n = e->d_name;
        if (n == "." || n == "..") continue;
        struct stat st;
        if (stat((dir + "/" + n).c_str(), &st) == 0 && S_ISREG(st.st_mode)) out.push_back(n);
    }
    closedir(d);
    std::sort(out.begin(), out.end());
    return out;
}

inline double wall_now() {
    timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return double(t.tv_sec) + 1e-9 * double(t.tv_nsec);
}
inline double cpu_now() {
    timespec t;
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t);
    return double(t.tv_sec) + 1e-9 * double(t.tv_nsec);
}

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
        Writer w(path, 1 << 16);
        w.put(s.data(), s.size());
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
