// Address points and footprints (the addresses track's Tier B, max/addresses/tierb.py 'abs2' +
// final_build.py 'col' / 'pax170'), on the sort step's cell-ordered records (not re-sorted).
//
//   addr_vertices.abs2  one blob per footprint, in record order:
//       tag u8 = enc | wide << 2 | small << 3 | closed << 4, count LEB128 (stored = open-ring vertices),
//       bmin: small -> i8 lat, i8 lng residuals (f32 order-key units) against
//             f32(f64(point) - (sum of q over the closed ring / n) * scale[enc]), else raw f32 lat, lng;
//       then count q_lat values, then count q_lng values, u8 (wide = 0) or u16 LE (wide = 1).
//       closed (count >= 2 and first == last in q): the closing vertex is dropped and re-appended.
//       Escape (tag 0x20, never hit on the planet days): LEB128(vertex_count), today's blob verbatim,
//       for encodings or header bytes abs2 does not model (enc 3, non-zero pad byte).
//   addr_points.col     six u32 columns of N: lat | lng | housenumber | street | parent_way | poly_offset
//       (byte offset of the record's blob in addr_vertices.abs2, NO_DATA = none); lat, lng and
//       poly_offset as 4 byte planes each (b0 of every record, then b1, ...), the rest plain u32.
//   addr_points.pax170  the same columns in blocks of 170 records (4080 B, one page): per block
//       lat planes | lng planes | housenumber | street | parent_way | poly_offset planes; the last
//       block zero-padded.
// Today's vertex_offset (prefix sum of blob sizes in record order) and vertex_count (count + closed)
// are derived; the encoder refuses records whose offsets are anything else.
// addr_postcodes.bin is kept as is (u32 per record, in the sorted order), as the addresses track had it.
#pragma once
#include "io.h"

namespace addr {

static const double SCALE[3] = {1e-5, 1e-4, 1e-6};
static const size_t PAX = 170;

static inline int64_t fkey(uint32_t b) { return (b & 0x80000000u) ? int64_t(0xFFFFFFFFu) - b : int64_t(b ^ 0x80000000u); }
static inline uint32_t unfkey(int64_t k) { return (k & 0x80000000) ? uint32_t(k ^ 0x80000000) : uint32_t(0xFFFFFFFFu - k); }
static inline uint32_t pred_bits(uint32_t point_bits, int64_t sum_q, uint32_t n, int enc) {
    float p;
    memcpy(&p, &point_bits, 4);
    const float f = float(double(p) - (double(sum_q) / double(n)) * SCALE[enc]);
    uint32_t b;
    memcpy(&b, &f, 4);
    return b;
}

// Column sink / source over the two containers.
struct ColWriter {
    bool pax;
    uint64_t N;
    int fd = -1;
    std::vector<Writer*> w;  // col: 15 region writers
    Writer* pw = nullptr;    // pax: one sequential writer
    std::vector<uint32_t> blk[6];
    size_t inblk = 0;
    ColWriter(const std::string& path, uint64_t n, bool pax_) : pax(pax_), N(n) {
        if (pax) {
            pw = new Writer(path);
            for (auto& b : blk) b.assign(PAX, 0);
            return;
        }
        if (unlink(path.c_str()) && errno != ENOENT) die("unlink %s", path.c_str());
        fd = open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
        if (fd < 0) die("create %s", path.c_str());
        // regions: lat b0..b3, lng b0..b3, hn, st, pw, vo b0..b3
        uint64_t at = 0;
        for (int k = 0; k < 15; k++) {
            const bool word = k >= 8 && k <= 10;
            w.push_back(new Writer(fd, path, at, 1 << 20));
            at += word ? 4 * N : N;
        }
    }
    void put(const uint32_t v[6]) {
        if (pax) {
            for (int k = 0; k < 6; k++) blk[k][inblk] = v[k];
            if (++inblk == PAX) flush_blk();
            return;
        }
        for (int b = 0; b < 4; b++) w[b]->u8(uint8_t(v[0] >> (8 * b)));
        for (int b = 0; b < 4; b++) w[4 + b]->u8(uint8_t(v[1] >> (8 * b)));
        w[8]->u32(v[2]);
        w[9]->u32(v[3]);
        w[10]->u32(v[4]);
        for (int b = 0; b < 4; b++) w[11 + b]->u8(uint8_t(v[5] >> (8 * b)));
    }
    void flush_blk() {
        uint8_t out[24 * PAX];
        size_t o = 0;
        for (int k : {0, 1}) for (int b = 0; b < 4; b++) for (size_t j = 0; j < PAX; j++) out[o++] = uint8_t(blk[k][j] >> (8 * b));
        for (int k : {2, 3, 4}) { memcpy(out + o, blk[k].data(), 4 * PAX); o += 4 * PAX; }
        for (int b = 0; b < 4; b++) for (size_t j = 0; j < PAX; j++) out[o++] = uint8_t(blk[5][j] >> (8 * b));
        pw->put(out, sizeof out);
        for (auto& bb : blk) std::fill(bb.begin(), bb.end(), 0);
        inblk = 0;
    }
    ~ColWriter() {
        if (pax) {
            if (inblk) flush_blk();
            delete pw;
            return;
        }
        for (auto* x : w) delete x;
        if (close(fd)) die("close");
    }
};

struct ColReader {
    bool pax;
    uint64_t N, i = 0;
    std::vector<Reader*> r;
    Reader* pr = nullptr;
    uint32_t blk[6][PAX];
    ColReader(const std::string& path, uint64_t n, bool pax_) : pax(pax_), N(n) {
        if (pax) {
            pr = new Reader(path);
            if (pr->size() != ((N + PAX - 1) / PAX) * 24 * PAX) die("%s size", path.c_str());
            return;
        }
        if (file_size(path) != 24 * N) die("%s size", path.c_str());
        uint64_t at = 0;
        for (int k = 0; k < 15; k++) {
            const bool word = k >= 8 && k <= 10;
            const uint64_t len = word ? 4 * N : N;
            r.push_back(new Reader(path, 1 << 20, at, at + len));
            at += len;
        }
    }
    void get(uint32_t v[6]) {
        if (pax) {
            const size_t j = size_t(i % PAX);
            if (j == 0) {
                const uint8_t* p = pr->take(24 * PAX);
                size_t o = 0;
                for (int k : {0, 1}) {
                    for (size_t x = 0; x < PAX; x++) blk[k][x] = 0;
                    for (int b = 0; b < 4; b++) for (size_t x = 0; x < PAX; x++) blk[k][x] |= uint32_t(p[o++]) << (8 * b);
                }
                for (int k : {2, 3, 4}) { memcpy(blk[k], p + o, 4 * PAX); o += 4 * PAX; }
                for (size_t x = 0; x < PAX; x++) blk[5][x] = 0;
                for (int b = 0; b < 4; b++) for (size_t x = 0; x < PAX; x++) blk[5][x] |= uint32_t(p[o++]) << (8 * b);
            }
            for (int k = 0; k < 6; k++) v[k] = blk[k][j];
            i++;
            return;
        }
        v[0] = v[1] = v[5] = 0;
        for (int b = 0; b < 4; b++) v[0] |= uint32_t(r[b]->u8()) << (8 * b);
        for (int b = 0; b < 4; b++) v[1] |= uint32_t(r[4 + b]->u8()) << (8 * b);
        v[2] = r[8]->u32();
        v[3] = r[9]->u32();
        v[4] = r[10]->u32();
        for (int b = 0; b < 4; b++) v[5] |= uint32_t(r[11 + b]->u8()) << (8 * b);
        i++;
    }
    ~ColReader() {
        delete pr;
        for (auto* x : r) delete x;
    }
};

// ---- Streaming (col layout only) ----
// The col file's region k starts at col_region(k, N): lat b0..b3, lng b0..b3, housenumber, street,
// parent_way, poly_offset b0..b3 (ColWriter's layout).
static inline uint64_t col_region(int k, uint64_t N) {
    uint64_t at = 0;
    for (int j = 0; j < k; j++) at += (j >= 8 && j <= 10) ? 4 * N : N;
    return at;
}

// Pass 1 (addr_points.bin, as the patcher writes it first): regions 0-10 of addr_points.col, and
// addr.pass.tmp: per record LEB128(vertex_count), then for a footprint zigzag LEB128 of its
// vertex_offset minus the previous footprint's. The file is sized for all 15 regions.
inline void encode_points_pass(const std::string& src, const std::string& out, Manifest& m) {
    Reader P(src + "/addr_points.bin");
    if (P.size() % 28) die("addr_points.bin size");
    const uint64_t N = P.size() / 28;
    const std::string cp = out + "/addr_points.col";
    if (unlink(cp.c_str()) && errno != ENOENT) die("unlink %s", cp.c_str());
    const int fd = open(cp.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (fd < 0) die("create %s", cp.c_str());
    std::vector<Writer*> w;
    for (int k = 0; k <= 10; k++) w.push_back(new Writer(fd, cp, col_region(k, N), 1 << 20));
    Writer T(out + "/addr.pass.tmp");
    int64_t last = 0;
    for (uint64_t i = 0; i < N; i++) {
        const uint8_t* r = P.take(28);
        const uint32_t lat = rd32(r), lng = rd32(r + 4), voff = rd32(r + 20), vcnt = rd32(r + 24);
        for (int b = 0; b < 4; b++) w[b]->u8(uint8_t(lat >> (8 * b)));
        for (int b = 0; b < 4; b++) w[4 + b]->u8(uint8_t(lng >> (8 * b)));
        w[8]->put(r + 8, 4);
        w[9]->put(r + 12, 4);
        w[10]->put(r + 16, 4);
        T.uv(vcnt);
        if (!vcnt) {
            if (voff != NO) die("addr_points %llu: vertex_count 0 with vertex_offset %u", (unsigned long long)i, voff);
            continue;
        }
        T.uv(zz64(int64_t(voff) - last));
        last = int64_t(voff);
    }
    for (auto* x : w) delete x;
    if (ftruncate(fd, off_t(24 * N))) die("truncate %s", cp.c_str());
    if (close(fd)) die("close");
    m.set("addr.n", N);
    m.set("addr.layout", "col");
    fprintf(stderr, "  addr pass 1 (points): records %llu\n", (unsigned long long)N);
}

// Pass 2 (addr_vertices.bin): addr_vertices.abs2 and regions 11-14 (poly_offset) of the col file,
// with the points' lat / lng read back from regions 0-7 and counts / offsets from addr.pass.tmp.
inline void encode_vertices_pass(const std::string& src, const std::string& out, Manifest& m) {
    const std::string cp = out + "/addr_points.col";
    const uint64_t N = file_size(cp) / 24;
    Reader V(src + "/addr_vertices.bin"), T(out + "/addr.pass.tmp");
    std::vector<Reader*> pl;
    for (int k = 0; k < 8; k++) pl.push_back(new Reader(cp, 1 << 20, col_region(k, N), col_region(k + 1, N)));
    const int fd = open(cp.c_str(), O_WRONLY | O_CLOEXEC);
    if (fd < 0) die("open %s", cp.c_str());
    std::vector<Writer*> po;
    for (int k = 11; k <= 14; k++) po.push_back(new Writer(fd, cp, col_region(k, N), 1 << 20));
    Writer A(out + "/addr_vertices.abs2");
    std::vector<uint32_t> ql, qg;
    uint64_t vexp = 0, polys = 0, nraw = 0;
    int64_t last = 0;
    auto put_po = [&](uint32_t v) { for (int b = 0; b < 4; b++) po[b]->u8(uint8_t(v >> (8 * b))); };
    for (uint64_t i = 0; i < N; i++) {
        uint32_t v[2] = {0, 0};
        for (int b = 0; b < 4; b++) v[0] |= uint32_t(pl[b]->u8()) << (8 * b);
        for (int b = 0; b < 4; b++) v[1] |= uint32_t(pl[4 + b]->u8()) << (8 * b);
        const uint32_t vcnt = uint32_t(T.uv());
        if (!vcnt) { put_po(NO); continue; }
        const int64_t voff = last + unzz64(T.uv());
        last = voff;
        if (uint64_t(voff) != vexp) die("addr_points %llu: vertex_offset %lld is not the running offset %llu", (unsigned long long)i,
                                        (long long)voff, (unsigned long long)vexp);
        if (A.tell() >= 0xFFFFFFFFull) die("addr_vertices.abs2 offset overflow");
        put_po(uint32_t(A.tell()));
        const uint8_t* h = V.need(10);
        const uint8_t enc = h[0];
        if (enc > 3) die("addr_vertices: unknown encoding %u", enc);
        const size_t size = 10 + size_t(vcnt) * (enc == 3 ? 8 : 4);
        const uint8_t* b = V.take(size);
        polys++;
        vexp += size;
        if (enc == 3 || b[1] != 0) {
            nraw++;
            A.u8(0x20);
            A.uv(vcnt);
            A.put(b, size);
            continue;
        }
        const uint32_t bl = rd32(b + 2), bg = rd32(b + 6);
        ql.resize(vcnt);
        qg.resize(vcnt);
        int64_t sl = 0, sg = 0;
        uint32_t span = 0;
        for (uint32_t k = 0; k < vcnt; k++) {
            ql[k] = rd16(b + 10 + 4 * size_t(k));
            qg[k] = rd16(b + 12 + 4 * size_t(k));
            sl += ql[k];
            sg += qg[k];
            span = std::max(span, std::max(ql[k], qg[k]));
        }
        const bool closed = vcnt >= 2 && ql[0] == ql[vcnt - 1] && qg[0] == qg[vcnt - 1];
        const uint32_t oc = vcnt - (closed ? 1 : 0);
        const bool wide = span >= 256;
        const int64_t rl = fkey(bl) - fkey(pred_bits(v[0], sl, vcnt, enc));
        const int64_t rg = fkey(bg) - fkey(pred_bits(v[1], sg, vcnt, enc));
        const bool small = rl > -128 && rl < 128 && rg > -128 && rg < 128;
        A.u8(uint8_t(enc | (wide << 2) | (small << 3) | (closed << 4)));
        A.uv(oc);
        if (small) {
            A.u8(uint8_t(int8_t(rl)));
            A.u8(uint8_t(int8_t(rg)));
        } else {
            A.u32(bl);
            A.u32(bg);
        }
        for (const auto* q : {&ql, &qg})
            for (uint32_t k = 0; k < oc; k++) {
                if (wide) A.u16(uint16_t((*q)[k]));
                else A.u8(uint8_t((*q)[k]));
            }
    }
    if (!V.eof()) die("addr_vertices.bin: bytes not covered by any record");
    for (auto* x : po) delete x;
    for (auto* x : pl) delete x;
    if (close(fd)) die("close");
    unlink((out + "/addr.pass.tmp").c_str());
    m.set("addr.abs2_bytes", A.tell());
    fprintf(stderr, "  addr pass 2 (vertices): footprints %llu raw %llu abs2 %llu B\n", (unsigned long long)polys,
            (unsigned long long)nraw, (unsigned long long)A.tell());
}

inline void encode(const std::string& src, const std::string& out, bool pax, Manifest& m) {
    if (want("addr_points.bin") != want("addr_vertices.bin")) {
        if (pax) die("streaming addr encode: col layout only");
        if (want("addr_points.bin")) encode_points_pass(src, out, m);
        else encode_vertices_pass(src, out, m);
        return;
    }
    Reader P(src + "/addr_points.bin"), V(src + "/addr_vertices.bin");
    if (P.size() % 28) die("addr_points.bin size");
    const uint64_t N = P.size() / 28;
    Writer A(out + "/addr_vertices.abs2");
    ColWriter C(out + (pax ? "/addr_points.pax170" : "/addr_points.col"), N, pax);
    std::vector<uint32_t> ql, qg;
    uint64_t vexp = 0, polys = 0, verts = 0, nsmall = 0, nclosed = 0, nwide = 0, nraw = 0;
    std::vector<uint8_t> hdr;
    for (uint64_t i = 0; i < N; i++) {
        const uint8_t* r = P.take(28);
        uint32_t v[6] = {rd32(r), rd32(r + 4), rd32(r + 8), rd32(r + 12), rd32(r + 16), NO};
        const uint32_t voff = rd32(r + 20), vcnt = rd32(r + 24);
        if (!vcnt) {
            if (voff != NO) die("addr_points %llu: vertex_count 0 with vertex_offset %u", (unsigned long long)i, voff);
            C.put(v);
            continue;
        }
        if (voff != vexp) die("addr_points %llu: vertex_offset %u is not the running offset %llu", (unsigned long long)i, voff,
                              (unsigned long long)vexp);
        if (A.tell() >= 0xFFFFFFFFull) die("addr_vertices.abs2 offset overflow");
        v[5] = uint32_t(A.tell());
        const uint8_t* h = V.need(10);
        const uint8_t enc = h[0];
        if (enc > 3) die("addr_vertices: unknown encoding %u", enc);
        const size_t size = 10 + size_t(vcnt) * (enc == 3 ? 8 : 4);
        const uint8_t* b = V.take(size);
        polys++;
        verts += vcnt;
        vexp += size;
        if (enc == 3 || b[1] != 0) {
            nraw++;
            A.u8(0x20);
            A.uv(vcnt);
            A.put(b, size);
            C.put(v);
            continue;
        }
        const uint32_t bl = rd32(b + 2), bg = rd32(b + 6);
        ql.resize(vcnt);
        qg.resize(vcnt);
        int64_t sl = 0, sg = 0;
        uint32_t span = 0;
        for (uint32_t k = 0; k < vcnt; k++) {
            ql[k] = rd16(b + 10 + 4 * size_t(k));
            qg[k] = rd16(b + 12 + 4 * size_t(k));
            sl += ql[k];
            sg += qg[k];
            span = std::max(span, std::max(ql[k], qg[k]));
        }
        const bool closed = vcnt >= 2 && ql[0] == ql[vcnt - 1] && qg[0] == qg[vcnt - 1];
        const uint32_t oc = vcnt - (closed ? 1 : 0);
        const bool wide = span >= 256;
        const int64_t rl = fkey(bl) - fkey(pred_bits(v[0], sl, vcnt, enc));
        const int64_t rg = fkey(bg) - fkey(pred_bits(v[1], sg, vcnt, enc));
        const bool small = rl > -128 && rl < 128 && rg > -128 && rg < 128;
        nsmall += small;
        nclosed += closed;
        nwide += wide;
        A.u8(uint8_t(enc | (wide << 2) | (small << 3) | (closed << 4)));
        A.uv(oc);
        if (small) {
            A.u8(uint8_t(int8_t(rl)));
            A.u8(uint8_t(int8_t(rg)));
        } else {
            A.u32(bl);
            A.u32(bg);
        }
        for (const auto* q : {&ql, &qg})
            for (uint32_t k = 0; k < oc; k++) {
                if (wide) A.u16(uint16_t((*q)[k]));
                else A.u8(uint8_t((*q)[k]));
            }
        C.put(v);
    }
    if (!V.eof()) die("addr_vertices.bin: bytes not covered by any record");
    m.set("addr.n", N);
    m.set("addr.layout", pax ? "pax170" : "col");
    m.set("addr.abs2_bytes", A.tell());
    fprintf(stderr, "  addr: records %llu footprints %llu vertices %llu small_bmin %llu closed %llu wide %llu raw %llu abs2 %llu B\n",
            (unsigned long long)N, (unsigned long long)polys, (unsigned long long)verts, (unsigned long long)nsmall,
            (unsigned long long)nclosed, (unsigned long long)nwide, (unsigned long long)nraw, (unsigned long long)A.tell());
}

// Streaming: addr_points.bin alone (every column, and the abs2 headers for vertex_offset /
// vertex_count: blob payloads are read past, not decoded).
inline void decode_points_only(const std::string& src, const Manifest& m, const std::string& out) {
    const uint64_t N = m.num("addr.n");
    ColReader C(src + "/addr_points.col", N, false);
    Reader A(src + "/addr_vertices.abs2");
    Writer P(out + "/addr_points.bin");
    uint64_t raw = 0;
    for (uint64_t i = 0; i < N; i++) {
        uint32_t v[6];
        C.get(v);
        uint32_t rec[7] = {v[0], v[1], v[2], v[3], v[4], NO, 0};
        if (v[5] != NO) {
            if (v[5] != A.tell()) die("addr %llu: poly_offset %u is not the stream position", (unsigned long long)i, v[5]);
            if (raw >= 0xFFFFFFFFull) die("addr_vertices offset overflow");
            rec[5] = uint32_t(raw);
            const uint8_t tag = A.u8();
            if (tag == 0x20) {
                const uint64_t vcnt = A.uv();
                const uint8_t enc = *A.need(1);
                const size_t size = 10 + size_t(vcnt) * (enc == 3 ? 8 : 4);
                A.skip(size);
                rec[6] = uint32_t(vcnt);
                raw += size;
            } else {
                const int enc = tag & 3, wide = (tag >> 2) & 1, small = (tag >> 3) & 1, closed = (tag >> 4) & 1;
                if (enc > 2 || (tag & 0xE0)) die("addr %llu: bad tag", (unsigned long long)i);
                const uint64_t cnt = A.uv();
                const uint32_t full = uint32_t(cnt) + uint32_t(closed);
                if (full > 69999 || (closed && !cnt)) die("addr %llu: bad count", (unsigned long long)i);
                A.skip((small ? 2 : 8) + 2 * size_t(cnt) * (wide ? 2 : 1));
                rec[6] = full;
                raw += 10 + 4 * uint64_t(full);
            }
        }
        P.put(rec, 28);
    }
    if (!A.eof()) die("addr_vertices.abs2 not fully consumed");
}

inline void decode(const std::string& src, const std::string& out, const Manifest& m) {
    const uint64_t N = m.num("addr.n");
    const bool pax = m.get("addr.layout") == "pax170";
    if (!want("addr_vertices.bin")) {
        if (pax) die("streaming addr decode: col layout only");
        decode_points_only(src, m, out);
        return;
    }
    // Streaming, addr_vertices.bin alone: only the lat, lng and poly_offset regions are read.
    const bool write_points = want("addr_points.bin");
    if (!write_points && pax) die("streaming addr decode: col layout only");
    ColReader* C = write_points ? new ColReader(src + (pax ? "/addr_points.pax170" : "/addr_points.col"), N, pax) : nullptr;
    std::vector<Reader*> rg;  // vertices alone: lat b0..b3, lng b0..b3, poly_offset b0..b3
    if (!write_points)
        for (int k : {0, 1, 2, 3, 4, 5, 6, 7, 11, 12, 13, 14})
            rg.push_back(new Reader(src + "/addr_points.col", 1 << 20, col_region(k, N), col_region(k + 1, N)));
    Reader A(src + "/addr_vertices.abs2");
    Writer* P = write_points ? new Writer(out + "/addr_points.bin") : nullptr;
    Writer V(out + "/addr_vertices.bin");
    std::vector<uint32_t> ql(70000), qg(70000);
    std::vector<uint8_t> blob;
    for (uint64_t i = 0; i < N; i++) {
        uint32_t v[6] = {0, 0, 0, 0, 0, 0};
        if (C) {
            C->get(v);
        } else {
            for (int b = 0; b < 4; b++) v[0] |= uint32_t(rg[b]->u8()) << (8 * b);
            for (int b = 0; b < 4; b++) v[1] |= uint32_t(rg[4 + b]->u8()) << (8 * b);
            for (int b = 0; b < 4; b++) v[5] |= uint32_t(rg[8 + b]->u8()) << (8 * b);
        }
        uint32_t rec[7] = {v[0], v[1], v[2], v[3], v[4], NO, 0};
        if (v[5] != NO) {
            if (v[5] != A.tell()) die("addr %llu: poly_offset %u is not the stream position", (unsigned long long)i, v[5]);
            if (V.tell() >= 0xFFFFFFFFull) die("addr_vertices offset overflow");
            rec[5] = uint32_t(V.tell());
            const uint8_t tag = A.u8();
            if (tag == 0x20) {
                const uint64_t vcnt = A.uv();
                const uint8_t enc = *A.need(1);
                const size_t size = 10 + size_t(vcnt) * (enc == 3 ? 8 : 4);
                V.put(A.take(size), size);
                rec[6] = uint32_t(vcnt);
            } else {
                const int enc = tag & 3, wide = (tag >> 2) & 1, small = (tag >> 3) & 1, closed = (tag >> 4) & 1;
                if (enc > 2 || (tag & 0xE0)) die("addr %llu: bad tag", (unsigned long long)i);
                const uint64_t cnt = A.uv();
                const uint32_t full = uint32_t(cnt) + uint32_t(closed);
                if (full > 69999 || (closed && !cnt)) die("addr %llu: bad count", (unsigned long long)i);
                uint8_t bm[8];
                memcpy(bm, A.take(small ? 2 : 8), small ? 2 : 8);
                int64_t sl = 0, sg = 0;
                for (auto* q : {&ql, &qg}) {
                    const uint8_t* p = A.take(size_t(cnt) * (wide ? 2 : 1));
                    int64_t s = 0;
                    for (uint64_t k = 0; k < cnt; k++) {
                        (*q)[k] = wide ? rd16(p + 2 * k) : p[k];
                        s += (*q)[k];
                    }
                    if (closed) { (*q)[cnt] = (*q)[0]; s += (*q)[0]; }
                    (q == &ql ? sl : sg) = s;
                }
                uint32_t bl, bg;
                if (small) {
                    bl = unfkey(fkey(pred_bits(v[0], sl, full, enc)) + int8_t(bm[0]));
                    bg = unfkey(fkey(pred_bits(v[1], sg, full, enc)) + int8_t(bm[1]));
                } else {
                    bl = rd32(bm);
                    bg = rd32(bm + 4);
                }
                blob.assign(10 + 4 * size_t(full), 0);
                blob[0] = uint8_t(enc);
                wr32(&blob[2], bl);
                wr32(&blob[6], bg);
                for (uint32_t k = 0; k < full; k++) {
                    wr16(&blob[10 + 4 * size_t(k)], uint16_t(ql[k]));
                    wr16(&blob[12 + 4 * size_t(k)], uint16_t(qg[k]));
                }
                V.put(blob);
                rec[6] = full;
            }
        }
        if (P) P->put(rec, 28);
    }
    if (!A.eof()) die("addr_vertices.abs2 not fully consumed");
    delete P;
    delete C;
    for (auto* x : rg) delete x;
}

}  // namespace addr
