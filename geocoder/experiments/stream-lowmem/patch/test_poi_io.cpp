// test_poi_io: the POI codec round trips while every positional read and write of the process is
// interrupted (EINTR) or cut short in turn. The big blocks' plane relocation (open rings) and header
// writes go on where a transfer stopped, as the buffered readers and writers do. Built with
// _FILE_OFFSET_BITS=64, where pread / pwrite are pread64 / pwrite64: this file's definitions replace
// libc's for the whole process.
#include <filesystem>
#include <random>
#include <sys/syscall.h>
#include "gcs_codec.h"

#if !defined(_FILE_OFFSET_BITS) || _FILE_OFFSET_BITS != 64
#error "test_poi_io interposes pread64 / pwrite64"
#endif

using namespace gcs;

static bool armed = false;
static uint64_t calls = 0, interrupted = 0, cut = 0;

// Every third call fails with EINTR, the next one moves half the bytes asked for.
static size_t inject(size_t n) {
    if (!armed || !n) return n;
    switch (calls++ % 3) {
    case 0: interrupted++; errno = EINTR; return 0;
    case 1: if (n > 1) { cut++; return n / 2; } return n;
    default: return n;
    }
}

extern "C" ssize_t pread64(int fd, void* buf, size_t n, __off64_t off) {
    const size_t k = inject(n);
    if (n && !k) return -1;
    return syscall(SYS_pread64, fd, buf, k, off);
}

extern "C" ssize_t pwrite64(int fd, const void* buf, size_t n, __off64_t off) {
    const size_t k = inject(n);
    if (n && !k) return -1;
    return syscall(SYS_pwrite64, fd, buf, k, off);
}

static int round_trip(const std::string& dir) {
    std::mt19937 rng(20261027);
    std::vector<uint8_t> recs, verts;
    struct Spec { uint32_t vc; int w; bool closed; };
    // past big_block (256 KiB): open rings of both widths (relocated planes), closed ones (header only)
    const Spec specs[] = {{0, 0, false}, {70000, 2, false}, {5, 2, true}, {70000, 4, false}, {80000, 2, true}, {0, 0, false},
                          {100000, 4, true}, {65537, 4, false}, {7, 4, false}};
    for (const Spec& sp : specs) {
        uint8_t r[36];
        for (auto& b : r) b = uint8_t(rng());
        if (!sp.vc) {
            wr32(r + 8, NO);
            wr32(r + 12, 0);
        } else {
            wr32(r + 8, uint32_t(verts.size()));
            wr32(r + 12, sp.vc);
            uint8_t h[10];
            for (auto& b : h) b = uint8_t(rng());
            h[0] = sp.w == 4 ? 3 : uint8_t(rng() % 3);
            h[1] = 0;
            verts.insert(verts.end(), h, h + 10);
            const size_t vs = 2 * size_t(sp.w), first = verts.size();
            for (uint32_t k = 0; k < sp.vc; k++) {
                uint8_t v[8];
                for (auto& b : v) b = uint8_t(rng());
                if (k + 1 == sp.vc && sp.closed) memcpy(v, &verts[first], vs);
                if (k + 1 == sp.vc && !sp.closed && memcmp(v, &verts[first], vs) == 0) v[0] ^= 1;
                verts.insert(verts.end(), v, v + vs);
            }
        }
        recs.insert(recs.end(), r, r + 36);
    }
    try {
        armed = true;
        poly::PoiEnc enc(dir);
        enc.state = {1, 1};
        size_t vp = 0;
        for (size_t i = 0; i < recs.size() / 36; i++) {
            enc.write(0, &recs[36 * i], 36);
            const uint32_t vc = rd32(&recs[36 * i + 12]);
            if (!vc) continue;
            const size_t bs = 10 + size_t(vc) * (verts[vp] == 3 ? 8 : 4);
            for (size_t o = 0; o < bs;) {
                const size_t c = std::min<size_t>(bs - o, 1 + rng() % 65521);
                enc.write(1, &verts[vp + o], c);
                o += c;
            }
            vp += bs;
        }
        enc.state = {2, 2};
        enc.pump();
        Manifest m;
        enc.finish(m);
        poly::PoiDec dec(dir, m);
        dec.state = {1, 1};
        while (dec.step()) {}
        armed = false;
        const bool ok_r = dec.out[0].size() == recs.size() && memcmp(dec.out[0].data(), recs.data(), recs.size()) == 0;
        const bool ok_v = dec.out[1].size() == verts.size() && memcmp(dec.out[1].data(), verts.data(), verts.size()) == 0;
        printf("records %s, vertices %s; %llu reads and writes, %llu interrupted, %llu cut short\n", ok_r ? "ok" : "DIFFER",
               ok_v ? "ok" : "DIFFER", (unsigned long long)calls, (unsigned long long)interrupted, (unsigned long long)cut);
        return ok_r && ok_v && interrupted && cut ? 0 : 1;
    } catch (const std::exception& e) {
        armed = false;
        printf("FAIL: %s (after %llu reads and writes)\n", e.what(), (unsigned long long)calls);
        return 1;
    }
}

int main() {
    const char* tmp = getenv("TMPDIR");
    std::string dir = std::string(tmp ? tmp : "/tmp") + "/poi-io-XXXXXX";
    if (!mkdtemp(dir.data())) return 1;
    const int rc = round_trip(dir);
    std::filesystem::remove_all(dir);
    return rc;
}
