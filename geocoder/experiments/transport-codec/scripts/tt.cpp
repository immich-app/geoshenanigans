// tt: lossless, size-preserving Parquet-style transport transform for
// geocoder index files (struct -> columns, byte-stream-split, zigzag delta on
// raw bit patterns), meant to run before zstd and be undone after zstd -d.
//
//   tt enc KIND SPEC IN OUT [--block BYTES] [--side PATH:STRIDE:CNTOFF]
//   tt dec IN OUT [--side PATH:STRIDE:CNTOFF]     IN may be a .zst (streamed)
//   tt dec2 RECIN RECOUT VIN VOUT CNTOFF          records + their vertex file in one pass
//   tt cols KIND IN OUTDIR [--side ...]           every column, every candidate
//                                                 encoding, whole file
//
// KIND: r            raw (no transform)
//       s:W,W,...    fixed-stride struct with these field widths (1/2/4/8)
//       e            cell entry list: u16 count then count x u32, repeated
//       v            polygon vertex blobs (u8 tag, u8 pad, f32 bml, f32 bmg,
//                    count x {u16,u16} or {u32,u32}); counts come from the
//                    records file given by --side (u32 count at CNTOFF of each
//                    STRIDE-byte record; records with count 0 own no blob)
// SPEC: one char per column (s: per field; e: counts,items; v: tag,pad,bml,
//       bmg,lat16,lng16,lat32,lng32):
//   R raw            B byte-stream-split
//   D zigzag delta + BSS            d zigzag delta, no BSS
//   S step from previous valid (all-ones = NO_DATA kept) + BSS, s without BSS
//   P zigzag delta restarting at each polygon + BSS, p without BSS (v only)
// --block 0 (default) = one block for the whole file; otherwise blocks of
// about BYTES raw bytes, each decodable alone (deltas restart per block).
#include <zstd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <type_traits>
#include <unistd.h>
#include <vector>
#include <deque>
#include <functional>
#include <algorithm>

using u8 = uint8_t;
using u16 = uint16_t;
using u32 = uint32_t;
using u64 = uint64_t;

[[noreturn]] static void die(const std::string& m) {
    fprintf(stderr, "tt: %s\n", m.c_str());
    exit(2);
}

enum Kind { K_RAW, K_STRUCT, K_ENTRIES, K_VERTS };

struct Layout {
    Kind kind = K_RAW;
    std::vector<int> widths;  // per column
    std::vector<int> offs;    // struct field offsets
    int stride = 0;
};

static Layout parse_kind(const std::string& k) {
    Layout l;
    if (k == "r") return l;
    if (k == "e") { l.kind = K_ENTRIES; l.widths = {2, 4}; return l; }
    if (k == "v") { l.kind = K_VERTS; l.widths = {1, 1, 4, 4, 2, 2, 4, 4}; return l; }
    if (k.rfind("s:", 0) == 0) {
        l.kind = K_STRUCT;
        size_t p = 2;
        while (p < k.size()) {
            size_t q = k.find(',', p);
            if (q == std::string::npos) q = k.size();
            int w = atoi(k.substr(p, q - p).c_str());
            if (w != 1 && w != 2 && w != 4 && w != 8) die("bad width in " + k);
            l.offs.push_back(l.stride);
            l.widths.push_back(w);
            l.stride += w;
            p = q + 1;
        }
        return l;
    }
    die("bad kind " + k);
}

static bool is_bss(char e) { return e == 'B' || e == 'D' || e == 'S' || e == 'P'; }

template <class T> static inline T zz(T d) {
    using S = std::make_signed_t<T>;
    return T(T(d << 1) ^ T(S(d) >> (sizeof(T) * 8 - 1)));
}
template <class T> static inline T unzz(T z) { return T((z >> 1) ^ T(T(0) - T(z & 1))); }

// Forward transform of one column in place; returns the encoding used
// (S/s fall back to D/d when a step would collide with NO_DATA).
template <class T>
static char fwd(T* v, size_t n, char enc, const std::vector<u32>* segs) {
    const T ALL1 = T(~T(0));
    switch (enc) {
    case 'R': case 'B': return enc;
    case 'D': case 'd': {
        T prev = 0;
        for (size_t i = 0; i < n; i++) { T x = v[i]; v[i] = zz<T>(T(x - prev)); prev = x; }
        return enc;
    }
    case 'S': case 's': {
        T prev = 0;
        for (size_t i = 0; i < n; i++) {
            T x = v[i];
            if (x == ALL1) continue;
            if (T(x - prev) == ALL1) return fwd(v, n, enc == 'S' ? 'D' : 'd', segs);
            prev = x;
        }
        prev = 0;
        for (size_t i = 0; i < n; i++) {
            T x = v[i];
            if (x == ALL1) continue;
            v[i] = T(x - prev);
            prev = x;
        }
        return enc;
    }
    case 'P': case 'p': {
        if (!segs) die("P needs polygons");
        size_t i = 0;
        for (u32 s : *segs) {
            T prev = 0;
            for (u32 k = 0; k < s; k++, i++) { T x = v[i]; v[i] = zz<T>(T(x - prev)); prev = x; }
        }
        if (i != n) die("segment sum mismatch");
        return enc;
    }
    }
    die(std::string("bad encoding ") + enc);
}

template <class T>
static void inv(T* v, size_t n, char enc, const std::vector<u32>* segs) {
    const T ALL1 = T(~T(0));
    switch (enc) {
    case 'R': case 'B': return;
    case 'D': case 'd': {
        T prev = 0;
        for (size_t i = 0; i < n; i++) { prev = T(prev + unzz<T>(v[i])); v[i] = prev; }
        return;
    }
    case 'S': case 's': {
        T prev = 0;
        for (size_t i = 0; i < n; i++) {
            if (v[i] == ALL1) continue;
            prev = T(prev + v[i]);
            v[i] = prev;
        }
        return;
    }
    case 'P': case 'p': {
        size_t i = 0;
        for (u32 s : *segs) {
            T prev = 0;
            for (u32 k = 0; k < s; k++, i++) { prev = T(prev + unzz<T>(v[i])); v[i] = prev; }
        }
        return;
    }
    }
    die(std::string("bad encoding ") + enc);
}

// Column values (little-endian) -> encoded bytes at out (n*W bytes).
template <class T>
static char put_col(std::vector<T>& v, char enc, const std::vector<u32>* segs, u8* out) {
    const size_t n = v.size();
    char used = fwd(v.data(), n, enc, segs);
    if (is_bss(used) && sizeof(T) > 1) {
        for (size_t k = 0; k < sizeof(T); k++) {
            u8* plane = out + k * n;
            for (size_t i = 0; i < n; i++) plane[i] = u8(v[i] >> (8 * k));
        }
    } else {
        memcpy(out, v.data(), n * sizeof(T));
    }
    return used;
}

template <class T>
static void get_col(const u8* in, size_t n, char enc, const std::vector<u32>* segs, std::vector<T>& v) {
    v.resize(n);
    if (is_bss(enc) && sizeof(T) > 1) {
        for (size_t i = 0; i < n; i++) v[i] = 0;
        for (size_t k = 0; k < sizeof(T); k++) {
            const u8* plane = in + k * n;
            for (size_t i = 0; i < n; i++) v[i] |= T(T(plane[i]) << (8 * k));
        }
    } else {
        memcpy(v.data(), in, n * sizeof(T));
    }
    inv(v.data(), n, enc, segs);
}

// ---- side records (vertex counts) ----
struct SideSpec {
    std::string path;
    int stride = 0, cntoff = 0;
    bool set = false;
};
static SideSpec parse_side(const std::string& s) {
    SideSpec sp;
    size_t a = s.rfind(':');
    size_t b = s.rfind(':', a - 1);
    if (a == std::string::npos || b == std::string::npos) die("bad --side");
    sp.path = s.substr(0, b);
    sp.stride = atoi(s.substr(b + 1, a - b - 1).c_str());
    sp.cntoff = atoi(s.substr(a + 1).c_str());
    sp.set = true;
    return sp;
}
// Streams the non-zero counts of a records file, in record order.
struct SideReader {
    FILE* f = nullptr;
    int stride = 0, cntoff = 0;
    std::vector<u8> buf;
    size_t pos = 0, len = 0;
    void open(const SideSpec& sp) {
        f = fopen(sp.path.c_str(), "rb");
        if (!f) die("open side " + sp.path);
        stride = sp.stride; cntoff = sp.cntoff;
        buf.resize(size_t(stride) * 65536);
    }
    bool next(u32& c) {
        for (;;) {
            if (pos + stride > len) {
                size_t keep = len - pos;
                memmove(buf.data(), buf.data() + pos, keep);
                len = keep + fread(buf.data() + keep, 1, buf.size() - keep, f);
                pos = 0;
                if (len < size_t(stride)) return false;
            }
            memcpy(&c, buf.data() + pos + cntoff, 4);
            pos += stride;
            if (c) return true;
        }
    }
};

// ---- output ----
struct Out {
    FILE* f;
    explicit Out(const char* path) {
        f = strcmp(path, "-") ? fopen(path, "wb") : stdout;
        if (!f) die(std::string("open out ") + path);
        setvbuf(f, nullptr, _IOFBF, 1 << 20);
    }
    void put(const void* p, size_t n) { if (n && fwrite(p, 1, n, f) != n) die("write"); }
    void close() { if (fclose(f)) die("close"); }
};

// ---- input (mmap) ----
struct Map {
    const u8* p = nullptr;
    size_t n = 0;
    explicit Map(const char* path) {
        int fd = open(path, O_RDONLY);
        if (fd < 0) die(std::string("open ") + path);
        struct stat st;
        fstat(fd, &st);
        n = st.st_size;
        if (n) {
            p = (const u8*)mmap(nullptr, n, PROT_READ, MAP_PRIVATE, fd, 0);
            if (p == MAP_FAILED) die("mmap");
            madvise((void*)p, n, MADV_SEQUENTIAL);
        }
        ::close(fd);
    }
};

// Encodes one column given as a width-W gather; dispatch on width.
template <class Gather>
static char enc_col_w(int w, size_t n, Gather g, char enc, const std::vector<u32>* segs, std::vector<u8>& out) {
    size_t at = out.size();
    out.resize(at + n * w);
    switch (w) {
    case 1: { std::vector<u8> v(n); for (size_t i = 0; i < n; i++) v[i] = u8(g(i)); return put_col(v, enc, segs, out.data() + at); }
    case 2: { std::vector<u16> v(n); for (size_t i = 0; i < n; i++) v[i] = u16(g(i)); return put_col(v, enc, segs, out.data() + at); }
    case 4: { std::vector<u32> v(n); for (size_t i = 0; i < n; i++) v[i] = u32(g(i)); return put_col(v, enc, segs, out.data() + at); }
    case 8: { std::vector<u64> v(n); for (size_t i = 0; i < n; i++) v[i] = u64(g(i)); return put_col(v, enc, segs, out.data() + at); }
    }
    die("width");
}

static u64 rd(const u8* p, int w) {
    u64 x = 0;
    memcpy(&x, p, w);
    return x;
}

// One unit range [b, e) of the input -> encoded block (header + columns).
struct Encoder {
    Layout L;
    std::string spec;
    const u8* in;
    size_t n;
    std::vector<u32> poly_cnt;  // vertex counts per polygon (K_VERTS)
    std::vector<u64> unit_off;  // byte offset of each cell / polygon (+ end)
    int only = -1;              // cols mode: encode just this column
    bool want(int c) const { return only < 0 || only == c; }

    void index_units() {
        if (L.kind == K_ENTRIES) {
            size_t pos = 0;
            while (pos < n) {
                if (pos + 2 > n) die("entries: truncated count");
                u16 c; memcpy(&c, in + pos, 2);
                unit_off.push_back(pos);
                pos += 2 + 4 * size_t(c);
            }
            if (pos != n) die("entries: overrun");
            unit_off.push_back(pos);
        } else if (L.kind == K_VERTS) {
            size_t pos = 0;
            for (size_t i = 0; i < poly_cnt.size(); i++) {
                if (pos + 10 > n) die("verts: truncated header");
                u8 tag = in[pos];
                if (tag > 3) die("verts: bad tag");
                unit_off.push_back(pos);
                pos += 10 + size_t(poly_cnt[i]) * (tag == 3 ? 8 : 4);
            }
            if (pos != n) die("verts: size mismatch " + std::to_string(pos) + " vs " + std::to_string(n));
            unit_off.push_back(pos);
        }
    }

    // Appends the block for units [ub, ue) to out.
    void block(size_t ub, size_t ue, std::vector<u8>& out) {
        const int nc = (int)L.widths.size();
        u32 nu = u32(ue - ub);
        size_t hdr = out.size();
        out.resize(hdr + 4 + nc);
        memcpy(out.data() + hdr, &nu, 4);
        auto setenc = [&](int c, char e) { out[hdr + 4 + c] = u8(e); };
        if (L.kind == K_STRUCT) {
            const u8* base = in + ub * L.stride;
            for (int c = 0; c < nc; c++) {
                if (!want(c)) continue;
                int w = L.widths[c], o = L.offs[c];
                char e = enc_col_w(w, nu, [&](size_t i) { return rd(base + i * L.stride + o, w); }, spec[c], nullptr, out);
                setenc(c, e);
            }
        } else if (L.kind == K_ENTRIES) {
            std::vector<u64> item_at;  // byte offsets of items
            if (want(0)) setenc(0, enc_col_w(2, nu, [&](size_t i) { return rd(in + unit_off[ub + i], 2); }, spec[0], nullptr, out));
            if (!want(1)) return;
            size_t items = 0;
            for (size_t u = ub; u < ue; u++) items += (unit_off[u + 1] - unit_off[u] - 2) / 4;
            item_at.reserve(items);
            for (size_t u = ub; u < ue; u++)
                for (u64 p = unit_off[u] + 2; p < unit_off[u + 1]; p += 4) item_at.push_back(p);
            if (want(1)) setenc(1, enc_col_w(4, items, [&](size_t i) { return rd(in + item_at[i], 4); }, spec[1], nullptr, out));
        } else {  // K_VERTS
            if (want(0)) setenc(0, enc_col_w(1, nu, [&](size_t i) { return u64(in[unit_off[ub + i]]); }, spec[0], nullptr, out));
            if (want(1)) setenc(1, enc_col_w(1, nu, [&](size_t i) { return u64(in[unit_off[ub + i] + 1]); }, spec[1], nullptr, out));
            if (want(2)) setenc(2, enc_col_w(4, nu, [&](size_t i) { return rd(in + unit_off[ub + i] + 2, 4); }, spec[2], nullptr, out));
            if (want(3)) setenc(3, enc_col_w(4, nu, [&](size_t i) { return rd(in + unit_off[ub + i] + 6, 4); }, spec[3], nullptr, out));
            std::vector<u32> seg16, seg32;
            std::vector<u64> at16, at32;
            for (size_t u = ub; u < ue; u++) {
                bool wide = in[unit_off[u]] == 3;
                (wide ? seg32 : seg16).push_back(poly_cnt[u]);
                u64 p = unit_off[u] + 10;
                for (u32 k = 0; k < poly_cnt[u]; k++, p += wide ? 8 : 4) (wide ? at32 : at16).push_back(p);
            }
            if (want(4)) setenc(4, enc_col_w(2, at16.size(), [&](size_t i) { return rd(in + at16[i], 2); }, spec[4], &seg16, out));
            if (want(5)) setenc(5, enc_col_w(2, at16.size(), [&](size_t i) { return rd(in + at16[i] + 2, 2); }, spec[5], &seg16, out));
            if (want(6)) setenc(6, enc_col_w(4, at32.size(), [&](size_t i) { return rd(in + at32[i], 4); }, spec[6], &seg32, out));
            if (want(7)) setenc(7, enc_col_w(4, at32.size(), [&](size_t i) { return rd(in + at32[i] + 4, 4); }, spec[7], &seg32, out));
        }
    }
};

static const char MAGIC[4] = {'G', 'T', 'T', '1'};

static int cmd_enc(const std::string& kind, const std::string& spec, const char* inpath, const char* outpath,
                   u64 block_bytes, const SideSpec& side) {
    Encoder E;
    E.L = parse_kind(kind);
    E.spec = spec;
    if (E.L.kind != K_RAW && spec.size() != E.L.widths.size()) die("spec length mismatch for " + kind);
    Map m(inpath);
    E.in = m.p;
    E.n = m.n;
    if (E.L.kind == K_VERTS) {
        if (!side.set) die("v needs --side");
        SideReader sr;
        sr.open(side);
        u32 c;
        while (sr.next(c)) E.poly_cnt.push_back(c);
    }
    E.index_units();
    Out o(outpath);
    o.put(MAGIC, 4);
    u8 kl = u8(kind.size()), sl = u8(spec.size());
    o.put(&kl, 1); o.put(kind.data(), kl);
    o.put(&sl, 1); o.put(spec.data(), sl);
    u64 raw = m.n;
    o.put(&raw, 8);
    o.put(&block_bytes, 8);
    if (E.L.kind == K_RAW) {
        o.put(m.p, m.n);
        o.close();
        return 0;
    }
    std::vector<u8> buf;
    const u64 cap = block_bytes ? block_bytes : ~u64(0);
    if (E.L.kind == K_STRUCT) {
        size_t nrec = m.n / E.L.stride;
        size_t per = block_bytes ? std::max<size_t>(1, block_bytes / E.L.stride) : std::max<size_t>(nrec, 1);
        for (size_t b = 0; b < nrec; b += per) {
            buf.clear();
            E.block(b, std::min(nrec, b + per), buf);
            o.put(buf.data(), buf.size());
        }
        o.put(m.p + nrec * E.L.stride, m.n - nrec * E.L.stride);
    } else {
        size_t units = E.unit_off.size() - 1;
        size_t b = 0;
        while (b < units) {
            size_t e = b + 1;
            while (e < units && E.unit_off[e + 1] - E.unit_off[b] <= cap) e++;
            buf.clear();
            E.block(b, e, buf);
            o.put(buf.data(), buf.size());
            b = e;
        }
    }
    o.close();
    return 0;
}

// ---- streaming input for dec (raw or zstd) ----
struct In {
    FILE* f;
    bool z = false;
    ZSTD_DStream* ds = nullptr;
    std::vector<u8> ibuf;
    ZSTD_inBuffer zin{nullptr, 0, 0};
    bool eof = false;
    explicit In(const char* path) {
        f = fopen(path, "rb");
        if (!f) die(std::string("open ") + path);
        size_t L = strlen(path);
        z = L > 4 && !strcmp(path + L - 4, ".zst");
        if (z) {
            ds = ZSTD_createDStream();
            ZSTD_initDStream(ds);
            ibuf.resize(ZSTD_DStreamInSize() * 4);
            zin = {ibuf.data(), 0, 0};
        } else {
            setvbuf(f, nullptr, _IOFBF, 1 << 20);
        }
    }
    // Reads up to n bytes; returns bytes read (short only at end of stream).
    size_t read(void* dst, size_t n) {
        if (!z) return fread(dst, 1, n, f);
        ZSTD_outBuffer out{dst, n, 0};
        while (out.pos < n) {
            if (zin.pos == zin.size) {
                if (eof) break;
                zin.size = fread(ibuf.data(), 1, ibuf.size(), f);
                zin.pos = 0;
                if (zin.size == 0) { eof = true; break; }
            }
            size_t r = ZSTD_decompressStream(ds, &out, &zin);
            if (ZSTD_isError(r)) die(std::string("zstd: ") + ZSTD_getErrorName(r));
        }
        return out.pos;
    }
    void need(void* dst, size_t n) { if (read(dst, n) != n) die("truncated input"); }
};

template <class T>
static void dec_col(In& in, std::vector<u8>& tmp, size_t n, char enc, const std::vector<u32>* segs, std::vector<T>& v) {
    tmp.resize(n * sizeof(T));
    in.need(tmp.data(), tmp.size());
    get_col<T>(tmp.data(), n, enc, segs, v);
}

// One tt stream being decoded block by block. A vertex stream takes its
// polygon counts from `counts`; a records stream can hand the non-zero u32
// counts at `sink_off` of every record it decodes to `sink` (dec2).
struct Dec {
    In in;
    Out o;
    Layout L;
    std::string spec;
    u64 raw = 0, block_bytes = 0, done = 0, body_end = 0;
    int nc = 0;
    bool have_hdr = false;
    u32 nu = 0;
    std::vector<char> enc;
    std::function<bool(u32&)> counts;
    std::deque<u32>* sink = nullptr;
    int sink_off = 0;
    std::vector<u8> tmp, outb;
    std::vector<u8> v8a, v8b;
    std::vector<u16> v16a, v16b, cnts;
    std::vector<u32> v32a, v32b, v32c, v32d, items;
    std::vector<u64> v64;

    Dec(const char* inpath, const char* outpath) : in(inpath), o(outpath) {
        char mg[4];
        in.need(mg, 4);
        if (memcmp(mg, MAGIC, 4)) die("not a tt stream");
        u8 kl, sl;
        in.need(&kl, 1);
        std::string kind(kl, ' ');
        in.need(kind.data(), kl);
        in.need(&sl, 1);
        spec.assign(sl, ' ');
        in.need(spec.data(), sl);
        in.need(&raw, 8);
        in.need(&block_bytes, 8);
        L = parse_kind(kind);
        nc = (int)L.widths.size();
        enc.resize(nc);
        body_end = L.kind == K_STRUCT ? (raw / L.stride) * L.stride : raw;
    }
    bool more() const { return done < body_end; }
    // Reads the next block's header (unit count, encodings).
    void peek() {
        if (have_hdr) return;
        if (L.kind == K_RAW) { nu = 0; have_hdr = true; return; }
        in.need(&nu, 4);
        in.need(enc.data(), nc);
        have_hdr = true;
    }
    void body() {
        peek();
        have_hdr = false;
        if (L.kind == K_RAW) {
            outb.resize(std::min<u64>(body_end - done, 1 << 22));
            in.need(outb.data(), outb.size());
        } else if (L.kind == K_STRUCT) {
            outb.resize(size_t(nu) * L.stride);
            for (int c = 0; c < nc; c++) {
                int w = L.widths[c], off = L.offs[c];
                u8* dst = outb.data() + off;
                switch (w) {
                case 1: dec_col(in, tmp, nu, enc[c], nullptr, v8a); for (u32 i = 0; i < nu; i++) dst[size_t(i) * L.stride] = v8a[i]; break;
                case 2: dec_col(in, tmp, nu, enc[c], nullptr, v16a); for (u32 i = 0; i < nu; i++) memcpy(dst + size_t(i) * L.stride, &v16a[i], 2); break;
                case 4: dec_col(in, tmp, nu, enc[c], nullptr, v32a); for (u32 i = 0; i < nu; i++) memcpy(dst + size_t(i) * L.stride, &v32a[i], 4); break;
                case 8: dec_col(in, tmp, nu, enc[c], nullptr, v64); for (u32 i = 0; i < nu; i++) memcpy(dst + size_t(i) * L.stride, &v64[i], 8); break;
                }
            }
            if (sink)
                for (u32 i = 0; i < nu; i++) {
                    u32 c;
                    memcpy(&c, outb.data() + size_t(i) * L.stride + sink_off, 4);
                    if (c) sink->push_back(c);
                }
        } else if (L.kind == K_ENTRIES) {
            dec_col(in, tmp, nu, enc[0], nullptr, cnts);
            size_t ni = 0;
            for (u32 i = 0; i < nu; i++) ni += cnts[i];
            dec_col(in, tmp, ni, enc[1], nullptr, items);
            outb.resize(2 * size_t(nu) + 4 * ni);
            u8* p = outb.data();
            size_t k = 0;
            for (u32 i = 0; i < nu; i++) {
                memcpy(p, &cnts[i], 2); p += 2;
                memcpy(p, items.data() + k, 4 * size_t(cnts[i])); p += 4 * size_t(cnts[i]);
                k += cnts[i];
            }
        } else {
            dec_col(in, tmp, nu, enc[0], nullptr, v8a);
            dec_col(in, tmp, nu, enc[1], nullptr, v8b);
            dec_col(in, tmp, nu, enc[2], nullptr, v32a);
            dec_col(in, tmp, nu, enc[3], nullptr, v32b);
            std::vector<u32> pc(nu), seg16, seg32;
            size_t n16 = 0, n32 = 0, bytes = 0;
            for (u32 i = 0; i < nu; i++) {
                if (!counts(pc[i])) die("side ran out");
                bool wide = v8a[i] == 3;
                (wide ? seg32 : seg16).push_back(pc[i]);
                (wide ? n32 : n16) += pc[i];
                bytes += 10 + size_t(pc[i]) * (wide ? 8 : 4);
            }
            dec_col(in, tmp, n16, enc[4], &seg16, v16a);
            dec_col(in, tmp, n16, enc[5], &seg16, v16b);
            dec_col(in, tmp, n32, enc[6], &seg32, v32c);
            dec_col(in, tmp, n32, enc[7], &seg32, v32d);
            outb.resize(bytes);
            u8* p = outb.data();
            size_t k16 = 0, k32 = 0;
            for (u32 i = 0; i < nu; i++) {
                p[0] = v8a[i]; p[1] = v8b[i];
                memcpy(p + 2, &v32a[i], 4); memcpy(p + 6, &v32b[i], 4);
                p += 10;
                if (v8a[i] == 3) {
                    for (u32 k = 0; k < pc[i]; k++, k32++) { memcpy(p, &v32c[k32], 4); memcpy(p + 4, &v32d[k32], 4); p += 8; }
                } else {
                    for (u32 k = 0; k < pc[i]; k++, k16++) { memcpy(p, &v16a[k16], 2); memcpy(p + 2, &v16b[k16], 2); p += 4; }
                }
            }
        }
        o.put(outb.data(), outb.size());
        done += outb.size();
        if (done > body_end) die("size mismatch");
    }
    void finish() {
        if (raw > done) {
            tmp.resize(raw - done);
            in.need(tmp.data(), tmp.size());
            o.put(tmp.data(), tmp.size());
        }
        o.close();
    }
};

static int cmd_dec(const char* inpath, const char* outpath, const SideSpec& side) {
    Dec d(inpath, outpath);
    SideReader sr;
    if (d.L.kind == K_VERTS) {
        if (!side.set) die("v needs --side");
        sr.open(side);
        d.counts = [&](u32& c) { return sr.next(c); };
    }
    while (d.more()) d.body();
    d.finish();
    return 0;
}

// Records file and its vertex file in one pass: the vertex decoder takes the
// counts straight from the records being decoded, so the decoded records file
// is never read back (bounded memory: only counts not yet consumed are held).
static int cmd_dec2(const char* rin, const char* rout, const char* vin, const char* vout, int cntoff) {
    Dec R(rin, rout), V(vin, vout);
    if (R.L.kind != K_STRUCT || V.L.kind != K_VERTS) die("dec2 needs a struct records stream and a v stream");
    std::deque<u32> q;
    size_t peak = 0;
    R.sink = &q;
    R.sink_off = cntoff;
    V.counts = [&](u32& c) {
        if (q.empty()) return false;
        c = q.front();
        q.pop_front();
        return true;
    };
    while (V.more()) {
        V.peek();
        while (q.size() < V.nu && R.more()) R.body();
        peak = std::max(peak, q.size());
        V.body();
    }
    while (R.more()) R.body();
    R.finish();
    V.finish();
    if (!q.empty()) die("records have more polygons than the vertex file");
    fprintf(stderr, "dec2: peak pending counts %zu\n", peak);
    return 0;
}

// Dumps each column under each candidate encoding (one block, whole file).
static int cmd_cols(const std::string& kind, const char* inpath, const std::string& outdir, const SideSpec& side) {
    Layout L = parse_kind(kind);
    if (L.kind == K_RAW) return 0;
    const int nc = (int)L.widths.size();
    Encoder E;
    E.L = L;
    Map m(inpath);
    E.in = m.p;
    E.n = m.n;
    if (L.kind == K_VERTS) {
        if (!side.set) die("v needs --side");
        SideReader sr;
        sr.open(side);
        u32 x;
        while (sr.next(x)) E.poly_cnt.push_back(x);
    }
    E.index_units();
    size_t units = L.kind == K_STRUCT ? m.n / L.stride : E.unit_off.size() - 1;
    std::vector<u8> buf;
    for (int c = 0; c < nc; c++) {
        int w = L.widths[c];
        std::string cands = w == 1 ? "RD" : (w == 2 ? "RBDd" : "RBDdSs");
        if (L.kind == K_VERTS && c >= 4) cands += "Pp";
        for (char e : cands) {
            E.spec = std::string(nc, 'R');
            E.spec[c] = e;
            E.only = c;
            buf.clear();
            E.block(0, units, buf);
            char used = char(buf[4 + c]);
            std::string path = outdir + "/c" + std::to_string(c) + "_" + std::string(1, e) + (used != e ? "~" : "") + ".col";
            Out o(path.c_str());
            o.put(buf.data() + 4 + nc, buf.size() - 4 - nc);
            o.close();
            fprintf(stderr, "%s %zu bytes\n", path.c_str(), buf.size() - 4 - nc);
        }
    }
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 2) die("usage: see header");
    std::string cmd = argv[1];
    u64 block = 0;
    SideSpec side;
    std::vector<std::string> pos;
    for (int i = 2; i < argc; i++) {
        std::string a = argv[i];
        if (a == "--block" && i + 1 < argc) block = strtoull(argv[++i], nullptr, 10);
        else if (a == "--side" && i + 1 < argc) side = parse_side(argv[++i]);
        else pos.push_back(a);
    }
    if (cmd == "enc" && pos.size() == 4) return cmd_enc(pos[0], pos[1], pos[2].c_str(), pos[3].c_str(), block, side);
    if (cmd == "dec2" && pos.size() == 5) return cmd_dec2(pos[0].c_str(), pos[1].c_str(), pos[2].c_str(), pos[3].c_str(), atoi(pos[4].c_str()));
    if (cmd == "dec" && pos.size() == 2) return cmd_dec(pos[0].c_str(), pos[1].c_str(), side);
    if (cmd == "cols" && pos.size() == 3) return cmd_cols(pos[0], pos[1].c_str(), pos[2], side);
    die("bad arguments");
}
