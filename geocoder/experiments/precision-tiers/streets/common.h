// Shared helpers for the precision study tools (streets / interps / address points).
#pragma once
#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

[[noreturn]] static void die(const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "FATAL: ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
    exit(2);
}

struct MF {
    const uint8_t* p = nullptr;
    size_t n = 0;
    int fd = -1;
    template <class T> const T* as() const { return reinterpret_cast<const T*>(p); }
    template <class T> size_t count() const {
        if (n % sizeof(T)) die("size %zu not a multiple of %zu", n, sizeof(T));
        return n / sizeof(T);
    }
};

static MF map_ro(const std::string& path, bool random = false) {
    MF m;
    m.fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (m.fd < 0) die("open %s: %s", path.c_str(), strerror(errno));
    struct stat st;
    fstat(m.fd, &st);
    m.n = size_t(st.st_size);
    if (m.n) {
        void* p = mmap(nullptr, m.n, PROT_READ, MAP_SHARED, m.fd, 0);
        if (p == MAP_FAILED) die("mmap %s", path.c_str());
        if (random) madvise(p, m.n, MADV_RANDOM);
        m.p = static_cast<const uint8_t*>(p);
    }
    return m;
}

static uint64_t file_size_or(const std::string& p) {
    struct stat st;
    if (stat(p.c_str(), &st)) die("stat %s: %s", p.c_str(), strerror(errno));
    return uint64_t(st.st_size);
}

static void write_file(const std::string& path, const void* data, size_t n) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) die("create %s", path.c_str());
    if (n && fwrite(data, 1, n, f) != n) die("write %s", path.c_str());
    if (fclose(f)) die("close %s", path.c_str());
}

static inline uint16_t rd16(const uint8_t* p) { uint16_t v; memcpy(&v, p, 2); return v; }
static inline uint32_t rd32(const uint8_t* p) { uint32_t v; memcpy(&v, p, 4); return v; }
static inline uint64_t rd64(const uint8_t* p) { uint64_t v; memcpy(&v, p, 8); return v; }
static inline uint32_t fbits(float f) { uint32_t b; memcpy(&b, &f, 4); return b; }
static inline float bitsf(uint32_t b) { float f; memcpy(&f, &b, 4); return f; }

// OSM 1e-7 integer -> today's f32 (what the builder stores).
static inline float f32_of(int32_t e) { return float(double(e) / 1e7); }
// 1.1 m base key: floor((E + 50) / 100), residual E - 100 k in [-50, 49].
static inline int32_t e5_of(int32_t e) {
    const int64_t x = int64_t(e) + 50;
    return int32_t(x >= 0 ? x / 100 : -((-x + 99) / 100));
}
static inline int32_t res_of(int32_t e) { return int32_t(int64_t(e) - 100 * int64_t(e5_of(e))); }
// 11 cm key: floor((E + 5) / 10).
static inline int32_t e6_of(int32_t e) {
    const int64_t x = int64_t(e) + 5;
    return int32_t(x >= 0 ? x / 10 : -((-x + 9) / 10));
}
static inline float f32_of_e5(int32_t k) { return float(double(k) / 1e5); }

static inline uint32_t zz(uint32_t d) { return (d << 1) ^ uint32_t(int32_t(d) >> 31); }
static inline uint32_t unzz(uint32_t z) { return (z >> 1) ^ uint32_t(-int32_t(z & 1)); }
static inline int bitlen(uint32_t v) { return v ? 32 - __builtin_clz(v) : 0; }
static inline int bytelen(uint32_t v) { return (bitlen(v) + 7) / 8; }

// LSB-first bit writer into a byte vector.
struct BitW {
    std::vector<uint8_t>& o;
    uint64_t acc = 0;
    int nb = 0;
    explicit BitW(std::vector<uint8_t>& out) : o(out) {}
    inline void put(uint64_t v, int w) {
        if (!w) return;
        acc |= v << nb;
        nb += w;
        while (nb >= 8) { o.push_back(uint8_t(acc)); acc >>= 8; nb -= 8; }
    }
    void flush() {
        if (nb) o.push_back(uint8_t(acc));
        acc = 0;
        nb = 0;
    }
};
static inline uint32_t getbits(const uint8_t* base, uint64_t bitpos, int w) {
    if (!w) return 0;
    const uint64_t v = rd64(base + (bitpos >> 3)) >> (bitpos & 7);
    return uint32_t(w == 32 ? v & 0xffffffffull : v & ((1ull << w) - 1));
}

static const double M_PER_DEG = 6371008.8 * M_PI / 180.0;
static inline double dist_m(double lat1, double lng1, double lat2, double lng2) {
    const double c = cos(lat1 * M_PI / 180.0);
    double dl = lng2 - lng1;
    if (dl > 180) dl -= 360;
    if (dl < -180) dl += 360;
    return M_PER_DEG * sqrt((lat2 - lat1) * (lat2 - lat1) + (dl * c) * (dl * c));
}

// Today's record layouts.
struct WayHeader { uint32_t off; uint16_t cnt; uint16_t pad; uint32_t name; };
struct InterpWay { uint32_t off; uint16_t cnt; uint16_t pad1; uint32_t street; uint32_t start, end; uint8_t interp, p4, p5, p6; };
struct AddrPoint { float lat, lng; uint32_t hn, street, parent, voff, vcnt; };
struct NodeCoord { float lat, lng; };
struct E7 { int32_t lat, lng; };
static_assert(sizeof(WayHeader) == 12 && sizeof(InterpWay) == 24 && sizeof(AddrPoint) == 28, "layout");

// Way list view over either record type (cnt at +4, off at +0).
struct Ways {
    MF m;
    size_t stride = 12, n = 0;
    void open(const std::string& path, bool street) {
        m = map_ro(path);
        stride = street ? 12 : 24;
        if (m.n % stride) die("%s size", path.c_str());
        n = m.n / stride;
    }
    uint32_t off(size_t i) const { return rd32(m.p + i * stride); }
    uint16_t cnt(size_t i) const { return rd16(m.p + i * stride + 4); }
};

// Simple fixed-bin histogram for metre distances: 0.1 mm bins up to 4 m.
struct Hist {
    static const int B = 40000;
    std::vector<uint64_t> h = std::vector<uint64_t>(B + 1, 0);
    uint64_t n = 0;
    double mx = 0, sum = 0;
    void add(double m) {
        n++;
        sum += m;
        if (m > mx) mx = m;
        int b = int(m * 1e4);
        h[b > B ? B : b]++;
    }
    void merge(const Hist& o) {
        for (int i = 0; i <= B; i++) h[i] += o.h[i];
        n += o.n;
        sum += o.sum;
        mx = std::max(mx, o.mx);
    }
    double q(double f) const {
        uint64_t t = uint64_t(f * double(n)), c = 0;
        for (int i = 0; i <= B; i++) {
            c += h[i];
            if (c > t) return (i + 0.5) / 1e4;
        }
        return mx;
    }
    void print(const char* name) const {
        printf("%-34s n=%llu mean=%.4f p50=%.4f p90=%.4f p99=%.4f max=%.4f m\n", name, (unsigned long long)n,
               n ? sum / double(n) : 0.0, q(0.5), q(0.9), q(0.99), mx);
    }
};
