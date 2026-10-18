// bench: random-access read cost of one street / interp way or one address point, 1 core.
//
//   bench ENC SORTED warm N SEED     every variant, files pre-faulted; ns per lookup
//   bench ENC SORTED cold N SEED     every variant, its files evicted first (fadvise DONTNEED, checked with
//                                    mincore), MADV_RANDOM mmap; us per lookup and major faults per lookup
//
// A lookup decodes the record to double degrees (all nodes of a way) and adds them to a checksum.
#include "common.h"
#include <chrono>
#include <functional>
#include <random>
#include <sys/resource.h>

static const size_t G = 16;
static inline size_t block_size(uint16_t cnt, uint16_t code) {
    if (!cnt) return 0;
    const size_t wl = code & 63, wg = (code >> 6) & 63, wf = (code >> 13) & 7;
    return 2 * wf + ((size_t(cnt) - 1) * (wl + wg) + 7) / 8;
}
static inline void decode_way(const uint8_t* q, uint16_t cnt, uint16_t code, int32_t al, int32_t ag, int32_t* out) {
    const int wl = code & 63, wg = (code >> 6) & 63, wf = (code >> 13) & 7;
    const bool u10 = (code >> 12) & 1;
    uint32_t zl = 0, zg = 0;
    for (int b = 0; b < wf; b++) zl |= uint32_t(q[b]) << (8 * b);
    for (int b = 0; b < wf; b++) zg |= uint32_t(q[wf + b]) << (8 * b);
    const uint8_t* bp = q + 2 * wf;
    uint32_t kl = uint32_t(al) + unzz(zl), kg = uint32_t(ag) + unzz(zg);
    out[0] = int32_t(kl);
    out[1] = int32_t(kg);
    const size_t m = size_t(cnt) - 1;
    uint64_t gl = 0, gg = m * size_t(wl);
    uint32_t dl = 0, dg = 0;
    for (size_t j = 0; j < m; j++) {
        uint32_t vl = unzz(getbits(bp, gl, wl)), vg = unzz(getbits(bp, gg, wg));
        if (u10) { vl *= 10; vg *= 10; }
        gl += wl;
        gg += wg;
        if (j == 0) { dl = vl; dg = vg; } else { dl += vl; dg += vg; }
        kl += dl;
        kg += dg;
        out[2 * (j + 1)] = int32_t(kl);
        out[2 * (j + 1) + 1] = int32_t(kg);
    }
}

struct Files {
    std::vector<std::string> paths;
    std::vector<MF> m;
    const uint8_t* get(size_t i) const { return m[i].p; }
    void open(bool random) {
        m.clear();
        for (auto& p : paths) m.push_back(map_ro(p, random));
    }
    void close() {
        for (auto& x : m) {
            if (x.p) munmap((void*)x.p, x.n);
            ::close(x.fd);
        }
        m.clear();
    }
    uint64_t bytes() const {
        uint64_t t = 0;
        for (auto& x : m) t += x.n;
        return t;
    }
    void evict() {
        for (auto& p : paths) {
            int fd = ::open(p.c_str(), O_RDONLY);
            fdatasync(fd);
            posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
            ::close(fd);
        }
    }
    uint64_t resident_pages() const {
        uint64_t r = 0;
        for (auto& x : m) {
            const size_t pages = (x.n + 4095) / 4096;
            std::vector<unsigned char> v(pages);
            if (mincore((void*)x.p, x.n, v.data())) die("mincore");
            for (auto c : v) r += c & 1;
        }
        return r;
    }
    double touch() const {
        double s = 0;
        for (auto& x : m)
            for (size_t i = 0; i < x.n; i += 4096) s += x.p[i];
        return s;
    }
};

struct Variant {
    std::string name;
    Files f;
    size_t n = 0;  // records
    std::function<double(size_t)> look;
};

static double nowd() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
static long majflt() { rusage r; getrusage(RUSAGE_SELF, &r); return r.ru_majflt; }

int main(int argc, char** argv) {
    if (argc < 6) die("usage: bench ENC SORTED warm|cold N SEED [filter]");
    const std::string enc = argv[1], sorted = argv[2], mode = argv[3];
    const size_t N = strtoull(argv[4], nullptr, 10);
    const uint64_t seed = strtoull(argv[5], nullptr, 10);
    const std::string filter = argc > 6 ? argv[6] : "";
    const bool cold = mode == "cold";
    std::vector<Variant*> vs;
    std::vector<int32_t> kb(2 * 70000), kx(2 * 70000);

    for (int fam = 0; fam < 2; fam++) {
        const bool street = fam == 0;
        const std::string pre = enc + (street ? "/street" : "/interp");
        const std::string fn = street ? "streets" : "interps";
        const size_t stride = street ? 12 : 24;
        const size_t nways = file_size_or(sorted + (street ? "/street_ways.bin" : "/interp_ways.bin")) / stride;
        {  // today's flat f32
            auto* v = new Variant{fn + " today f32 flat", {}, nways, {}};
            v->f.paths = {sorted + (street ? "/street_ways.bin" : "/interp_ways.bin"),
                          sorted + (street ? "/street_nodes.bin" : "/interp_nodes.bin")};
            v->look = [v, stride](size_t j) {
                const uint8_t* w = v->f.get(0) + j * stride;
                const uint32_t off = rd32(w);
                const uint16_t c = rd16(w + 4);
                const float* nd = reinterpret_cast<const float*>(v->f.get(1)) + 2 * size_t(off);
                double s = 0;
                for (size_t k = 0; k < 2 * size_t(c); k++) s += double(nd[k]);
                return s;
            };
            vs.push_back(v);
        }
        // packed ways: files cnt, groups, code, nodes2 [, ref idx, ref bits]
        for (int kind = 0; kind < 4; kind++) {
            // 0 e5 base, 1 e5 + ref14, 2 e5 + refA, 3 e7 separate
            const char* kn[4] = {"base 1e-5", "base 1e-5 + refinement (fixed 14 bit)", "base 1e-5 + refinement (unit per way)",
                                 "separate exact 1e-7"};
            const std::string k = kind == 3 ? "e7" : "e5";
            auto* v = new Variant{fn + " " + kn[kind], {}, nways, {}};
            v->f.paths = {pre + ".cnt", pre + "." + k + ".groups", pre + "." + k + ".code", pre + "." + k + ".nodes2"};
            if (kind == 1) { v->f.paths.push_back(pre + ".ref14.idx"); v->f.paths.push_back(pre + ".ref14"); }
            if (kind == 2) { v->f.paths.push_back(pre + ".refA.idx"); v->f.paths.push_back(pre + ".refA"); }
            v->look = [v, kind, &kb](size_t j) {
                const uint16_t* cnt = reinterpret_cast<const uint16_t*>(v->f.get(0));
                const uint8_t* g = v->f.get(1) + 12 * (j / G);
                const uint16_t* code = reinterpret_cast<const uint16_t*>(v->f.get(2));
                uint64_t off = rd32(g);
                uint64_t nig = 0, refbits = 0;
                const uint32_t units = kind == 2 ? rd32(v->f.get(4) + 8 * (j / G) + 4) : 0;
                for (size_t w = j - j % G; w < j; w++) {
                    off += block_size(cnt[w], code[w]);
                    nig += cnt[w];
                    if (kind == 2) {
                        const uint32_t u = (units >> (2 * (w % G))) & 3;
                        refbits += uint64_t(cnt[w]) * (u == 0 ? 14 : u == 1 ? 8 : 0);
                    }
                }
                const uint16_t c = cnt[j];
                if (!c) return 0.0;
                decode_way(v->f.get(3) + off, c, code[j], int32_t(rd32(g + 4)), int32_t(rd32(g + 8)), kb.data());
                double s = 0;
                if (kind == 0) {
                    for (size_t q = 0; q < 2 * size_t(c); q++) s += kb[q] * 1e-5;
                } else if (kind == 3) {
                    for (size_t q = 0; q < 2 * size_t(c); q++) s += kb[q] * 1e-7;
                } else if (kind == 1) {
                    const uint64_t node = rd32(v->f.get(4) + 4 * (j / G)) + nig;
                    const uint8_t* rb = v->f.get(5);
                    uint64_t bp = 14 * node;
                    for (size_t q = 0; q < 2 * size_t(c); q++, bp += 7)
                        s += (int64_t(kb[q]) * 100 + int64_t(getbits(rb, bp, 7)) - 50) * 1e-7;
                } else {
                    const uint8_t* rb = v->f.get(5) + rd32(v->f.get(4) + 8 * (j / G));
                    const uint32_t u = (units >> (2 * (j % G))) & 3;
                    uint64_t bp = refbits;
                    for (size_t q = 0; q < 2 * size_t(c); q++) {
                        int64_t r = 0;
                        if (u == 0) { r = int64_t(getbits(rb, bp, 7)) - 50; bp += 7; }
                        else if (u == 1) { r = (int64_t(getbits(rb, bp, 4)) - 5) * 10; bp += 4; }
                        s += (int64_t(kb[q]) * 100 + r) * 1e-7;
                    }
                }
                return s;
            };
            vs.push_back(v);
        }
    }
    {
        const size_t n = file_size_or(sorted + "/addr_points.bin") / 28;
        auto* v0 = new Variant{"addr today f32 flat", {}, n, {}};
        v0->f.paths = {sorted + "/addr_points.bin"};
        v0->look = [v0](size_t i) {
            const float* p = reinterpret_cast<const float*>(v0->f.get(0) + 28 * i);
            return double(p[0]) + double(p[1]);
        };
        vs.push_back(v0);
        for (int kind = 0; kind < 3; kind++) {
            // FOR-64 blocks: 0 e5, 1 e5 + ref14, 2 e7
            const char* kn[3] = {"base 1e-5 (FOR-64)", "base 1e-5 (FOR-64) + refinement", "separate exact 1e-7 (FOR-64)"};
            const std::string k = kind == 2 ? "e7" : "e5";
            auto* v = new Variant{std::string("addr ") + kn[kind], {}, n, {}};
            v->f.paths = {enc + "/addr.for64." + k + ".hdr", enc + "/addr.for64." + k + ".bits"};
            if (kind == 1) v->f.paths.push_back(enc + "/addr.ref14");
            v->look = [v, kind](size_t i) {
                const uint8_t* h = v->f.get(0) + 16 * (i / 64);
                const int wl = h[12], wg = h[13];
                const uint64_t bp = 8 * uint64_t(rd32(h + 8)) + (i % 64) * uint64_t(wl + wg);
                const uint8_t* b = v->f.get(1);
                const int32_t la = int32_t((rd32(h) + getbits(b, bp, wl)) ^ 0x80000000u);
                const int32_t lg = int32_t((rd32(h + 4) + getbits(b, bp + wl, wg)) ^ 0x80000000u);
                if (kind == 0) return la * 1e-5 + lg * 1e-5;
                if (kind == 2) return la * 1e-7 + lg * 1e-7;
                const uint8_t* r = v->f.get(2);
                const int64_t rl = int64_t(getbits(r, 14 * uint64_t(i), 7)) - 50, rg = int64_t(getbits(r, 14 * uint64_t(i) + 7, 7)) - 50;
                return (int64_t(la) * 100 + rl) * 1e-7 + (int64_t(lg) * 100 + rg) * 1e-7;
            };
            vs.push_back(v);
        }
        for (int kind = 0; kind < 2; kind++) {
            const std::string k = kind ? "e7" : "e5";
            auto* v = new Variant{std::string("addr byte planes ") + (kind ? "exact 1e-7" : "base 1e-5"), {}, n, {}};
            v->f.paths = {enc + "/addr.planes." + k};
            v->look = [v, kind, n](size_t i) {
                const uint8_t* p = v->f.get(0);
                uint32_t a = 0, b = 0;
                for (int q = 0; q < 4; q++) {
                    a |= uint32_t(p[q * n + i]) << (8 * q);
                    b |= uint32_t(p[(4 + q) * n + i]) << (8 * q);
                }
                const double sc = kind ? 1e-7 : 1e-5;
                return int32_t(a) * sc + int32_t(b) * sc;
            };
            vs.push_back(v);
        }
    }

    printf("mode %s, %zu lookups per variant, seed %llu\n", mode.c_str(), N, (unsigned long long)seed);
    for (auto* v : vs) {
        if (!filter.empty() && v->name.find(filter) == std::string::npos) continue;
        std::mt19937_64 rng(seed);
        std::vector<size_t> ids(N);
        for (auto& x : ids) x = rng() % v->n;
        double chk = 0;
        if (!cold) {
            v->f.open(false);
            double warm = v->f.touch();
            for (size_t i = 0; i < std::min<size_t>(N, 100000); i++) warm += v->look(ids[i]);  // warm-up
            if (warm == 1.5) printf("-");
            const double t0 = nowd();
            for (size_t i = 0; i < N; i++) chk += v->look(ids[i]);
            const double t = nowd() - t0;
            printf("%-58s warm %8.1f ns/lookup   files %.1f MB   (checksum %.17g)\n", v->name.c_str(), 1e9 * t / double(N),
                   v->f.bytes() / 1e6, chk);
        } else {
            v->f.evict();
            v->f.open(true);
            const uint64_t res = v->f.resident_pages();
            const long f0 = majflt();
            const double t0 = nowd();
            for (size_t i = 0; i < N; i++) chk += v->look(ids[i]);
            const double t = nowd() - t0;
            const long f1 = majflt();
            printf("%-58s cold %8.1f us/lookup   %.2f major faults/lookup   resident before %llu pages   (checksum %.17g)\n",
                   v->name.c_str(), 1e6 * t / double(N), double(f1 - f0) / double(N), (unsigned long long)res, chk);
        }
        v->f.close();
        fflush(stdout);
    }
    return 0;
}
