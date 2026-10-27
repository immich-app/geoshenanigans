// In-process incremental codecs for the low-memory client update. The same formats as gc
// (sortpatch/codecs, e2e gcx) byte for byte, restructured so the patcher drives them:
//   Decoder: produces today's-format bytes on demand, one unit (way, row, record, block piece) per
//            step(), into one queue per today's file (a coupled family fills several queues).
//   Encoder: consumes today's-format bytes as the patcher writes them, one queue per today's file,
//            and processes every unit whose bytes are all there (pump()).
// Nothing is held beyond the buffers, the unit in flight and the queues' skew. A unit held whole is
// bounded by the format (a way <= 65535 nodes, an address footprint <= 69999 vertices) or, for POI
// polygons, by the data (the encoder needs the closing vertex before it can lay out the planes);
// admin polygons, which are uncapped, go plane by plane through positional readers / writers.
#pragma once
#include <cmath>
#include <memory>
#include "gcs_io.h"

namespace gcs {

// ------------------------------------------------------------------ framework
struct Decoder {
    std::vector<ByteQueue> out;
    std::vector<int> state;  // 0 not opened yet (kept), 1 open, 2 closed (dropped)
    std::vector<uint64_t> drop;  // per port: bytes its reader skips, dropped as they are decoded
    size_t dropping = 0;         // ports with bytes to drop
    std::string name;
    std::vector<uint8_t> scratch;
    explicit Decoder(const std::string& n, int ports)
        : out(size_t(ports)), state(size_t(ports), 0), drop(size_t(ports), 0), name(n) {}
    virtual ~Decoder() = default;
    virtual bool step() = 0;  // emits the next unit; false at the end
    // The decoded file's size when the manifest gives it, else ~0.
    virtual uint64_t decoded_size(int) const { return ~0ull; }
    inline void emit(int port, const void* p, size_t n) {
        if (state[size_t(port)] != 2) out[size_t(port)].push(p, n);
    }
    // n bytes at the back of a port's queue to fill (scratch when the port is closed).
    inline uint8_t* emit_reserve(int port, size_t n) {
        if (state[size_t(port)] == 2) {
            if (scratch.size() < n) scratch.resize(n);
            return scratch.data();
        }
        return out[size_t(port)].reserve(n);
    }
    // Decodes ahead until the port holds up to READ_AHEAD bytes (or max, if less), so a reader's
    // refill takes many small units at once; a coupled family's other queues grow by as much.
    // It stops early, once the port has something, when another open port already holds SKEW_SOFT
    // (its reader is behind: decoding further would only grow that queue).
    static constexpr size_t READ_AHEAD = size_t(8) << 10, SKEW_SOFT = size_t(64) << 10;
    bool others_full(int port) const {
        for (size_t p = 0; p < out.size(); p++)
            if (int(p) != port && state[p] != 2 && out[p].size() >= SKEW_SOFT) return true;
        return false;
    }
    size_t read(int port, char* dst, size_t max) {
        ByteQueue& q = out[size_t(port)];
        const size_t want = std::min(max, READ_AHEAD);
        while (q.size() < want) {
            if (q.size() && out.size() > 1 && others_full(port)) break;
            if (!step()) break;
            if (dropping) apply_drops();
        }
        if (q.size() == 0) return 0;
        const size_t k = std::min(max, q.size());
        memcpy(dst, q.data(), k);
        q.pop(k);
        return k;
    }
    // The port's reader skips its next n bytes: queued ones go now, the rest as they are decoded
    // (a reader of a coupled family that skips, e.g. the child of deleted parents, holds nothing
    // while the other reader drives the decoder through them).
    void skip(int port, uint64_t n) {
        ByteQueue& q = out[size_t(port)];
        const size_t k = size_t(std::min<uint64_t>(n, q.size()));
        q.pop(k);
        if (n > k) {
            if (!drop[size_t(port)]) dropping++;
            drop[size_t(port)] += n - k;
        }
    }
    void apply_drops() {
        for (size_t p = 0; p < out.size(); p++) {
            if (!drop[p] || !out[p].size()) continue;
            const size_t k = size_t(std::min<uint64_t>(drop[p], out[p].size()));
            out[p].pop(k);
            drop[p] -= k;
            if (!drop[p]) dropping--;
        }
    }
    size_t peak(int port) const { return out[size_t(port)].peak; }
};

struct Encoder {
    std::vector<ByteQueue> in;
    std::vector<int> state;  // 0 not opened, 1 open, 2 closed
    std::vector<uint64_t> got, expect;
    std::string name, out_dir;
    explicit Encoder(const std::string& n, const std::string& od, int ports)
        : in(size_t(ports)), state(size_t(ports), 0), got(size_t(ports), 0), expect(size_t(ports), ~0ull), name(n), out_dir(od) {}
    virtual ~Encoder() = default;
    virtual void pump() = 0;               // processes every complete unit
    virtual void finish(Manifest& m) = 0;  // every port closed and pumped
    // Optional: processes complete units straight from the writer's bytes (the port's queue is
    // empty); returns the bytes taken.
    virtual size_t consume(int, const uint8_t*, size_t) { return 0; }
    void write(int port, const void* p, size_t n) {
        got[size_t(port)] += n;
        if (in[size_t(port)].size() == 0) {
            const size_t k = consume(port, static_cast<const uint8_t*>(p), n);
            p = static_cast<const uint8_t*>(p) + k;
            n -= k;
            if (!n) return;
        }
        in[size_t(port)].push(p, n);
        pump();
    }
    bool all_closed() const {
        for (int s : state)
            if (s != 2) return false;
        return true;
    }
    size_t peak(int port) const { return in[size_t(port)].peak; }
    void require_empty() const {
        for (size_t p = 0; p < in.size(); p++)
            if (in[p].size()) die("%s: %zu bytes left over on input %zu", name.c_str(), in[p].size(), p);
    }
};

// ------------------------------------------------------------------ streets / interps (packed ways)
namespace ways {
static const size_t G = 16;
static inline uint32_t ulp_key(uint32_t b) { return (b & 0x80000000u) ? ~b : (b | 0x80000000u); }
static inline uint32_t ulp_unkey(uint32_t k) { return (k & 0x80000000u) ? (k & 0x7fffffffu) : ~k; }
static constexpr uint32_t ONE_BITS = 0x3f800000u, TEN_MILLION = 10000000u;
static inline uint32_t hyb_decode(uint32_t k) {
    const int32_t s = int32_t(k);
    const uint32_t sign = s < 0 ? 0x80000000u : 0;
    const uint32_t mag = s < 0 ? uint32_t(-int64_t(s)) : uint32_t(s);
    if (mag >= TEN_MILLION) return (mag - TEN_MILLION + ONE_BITS) | sign;
    const float f = float(double(mag) / 1e7);
    uint32_t b;
    memcpy(&b, &f, 4);
    return b | sign;
}
static inline bool hyb_key(uint32_t b, uint32_t& k) {
    const uint32_t a = b & 0x7fffffffu;
    uint32_t mag;
    if (a >= ONE_BITS) {
        const uint64_t m = uint64_t(a) - ONE_BITS + TEN_MILLION;
        if (m > 0x7fffffffu) return false;
        mag = uint32_t(m);
    } else {
        float f;
        memcpy(&f, &a, 4);
        const double n = std::nearbyint(double(f) * 1e7);
        if (n >= TEN_MILLION) return false;
        mag = uint32_t(n);
    }
    if ((b >> 31) && mag == 0) return false;
    k = (b >> 31) ? uint32_t(-int64_t(mag)) : mag;
    return hyb_decode(k) == b;
}
static inline uint32_t zz(uint32_t d) { return (d << 1) ^ uint32_t(int32_t(d) >> 31); }
static inline uint32_t unzz(uint32_t z) { return (z >> 1) ^ uint32_t(-int32_t(z & 1)); }
static inline int bitlen(uint32_t v) { return v ? 32 - __builtin_clz(v) : 0; }
static inline int bytelen(uint32_t v) { return (bitlen(v) + 7) / 8; }
static inline size_t block_size(uint16_t cnt, uint16_t code) {
    if (!cnt) return 0;
    const size_t wl = code & 63, wg = (code >> 6) & 63, wf = (code >> 13) & 7;
    return 2 * wf + ((size_t(cnt) - 1) * (wl + wg) + 7) / 8;
}
struct Names { const char *ways, *nodes, *pre; size_t stride; };
static Names names(bool street) {
    return street ? Names{"street_ways.bin", "street_nodes.bin", "street", 12} : Names{"interp_ways.bin", "interp_nodes.bin", "interp", 24};
}

// One way's nodes (2 u32 per node, unaligned) into blk (appended). Returns the code.
static uint16_t encode_way(const uint8_t* nodes, size_t cnt, uint32_t al, uint32_t ag, std::vector<uint8_t>& blk,
                           std::vector<uint32_t>& kl, std::vector<uint32_t>& kg) {
    auto bits = [&](size_t j, size_t c) { return rd32(nodes + 8 * j + 4 * c); };
    const uint32_t zfl = zz(ulp_key(bits(0, 0)) - ulp_key(al)), zfg = zz(ulp_key(bits(0, 1)) - ulp_key(ag));
    const int wf = std::max(bytelen(zfl), bytelen(zfg));
    for (int b = 0; b < wf; b++) blk.push_back(uint8_t(zfl >> (8 * b)));
    for (int b = 0; b < wf; b++) blk.push_back(uint8_t(zfg >> (8 * b)));
    kl.resize(cnt);
    kg.resize(cnt);
    bool hyb = true;
    for (size_t j = 0; j < cnt && hyb; j++) hyb = hyb_key(bits(j, 0), kl[j]) && hyb_key(bits(j, 1), kg[j]);
    if (!hyb)
        for (size_t j = 0; j < cnt; j++) { kl[j] = ulp_key(bits(j, 0)); kg[j] = ulp_key(bits(j, 1)); }
    for (size_t j = cnt - 1; j >= 1; j--) { kl[j] -= kl[j - 1]; kg[j] -= kg[j - 1]; }
    for (size_t j = cnt - 1; j >= 2; j--) { kl[j] -= kl[j - 1]; kg[j] -= kg[j - 1]; }
    uint32_t ml = 0, mg = 0;
    for (size_t j = 1; j < cnt; j++) { kl[j] = zz(kl[j]); kg[j] = zz(kg[j]); ml |= kl[j]; mg |= kg[j]; }
    const int wl = bitlen(ml), wg = bitlen(mg);
    uint64_t acc = 0;
    int nb = 0;
    auto put = [&](uint32_t v, int w) {
        acc |= uint64_t(v) << nb;
        nb += w;
        while (nb >= 8) { blk.push_back(uint8_t(acc)); acc >>= 8; nb -= 8; }
    };
    if (wl) for (size_t j = 1; j < cnt; j++) put(kl[j], wl);
    if (wg) for (size_t j = 1; j < cnt; j++) put(kg[j], wg);
    if (nb) blk.push_back(uint8_t(acc));
    return uint16_t(wl | (wg << 6) | (hyb ? 1 << 12 : 0) | (wf << 13));
}

// Ports: 0 = ways file, 1 = nodes file. A step emits one way's record and its nodes.
struct Dec : Decoder {
    bool street;
    Names nm;
    uint64_t nw, nn, i = 0, node_off = 0;
    std::unique_ptr<Reader> groups, nodes2, c0, c1, c2, c3, c4;
    uint32_t al = 0, ag = 0;
    Dec(const std::string& src, bool st, const Manifest& m) : Decoder(st ? "streets" : "interps", 2), street(st), nm(names(st)) {
        const std::string k = nm.pre, p = src + "/" + nm.pre;
        nw = m.num(k + ".ways");
        nn = m.num(k + ".nodes");
        if (m.num(k + ".G") != G) die("unsupported G");
        groups.reset(new Reader(p + ".groups", 16 << 10));
        nodes2.reset(new Reader(p + ".nodes2"));
        c0.reset(new Reader(p + (street ? ".cnt" : ".cntcode"), 32 << 10));
        c1.reset(new Reader(p + (street ? ".code" : ".street"), 32 << 10));
        c2.reset(new Reader(p + (street ? ".name" : ".start"), 32 << 10));
        if (!street) {
            c3.reset(new Reader(p + ".span", 32 << 10));
            c4.reset(new Reader(p + ".interp", 16 << 10));
        }
        if (groups->size() != 12 * ((nw + G - 1) / G)) die("%s.groups size", nm.pre);
    }
    uint64_t decoded_size(int port) const override { return port == 0 ? nw * nm.stride : nn * 8; }
    bool step() override {
        if (i == nw) {
            if (!nodes2->eof() || node_off != nn) die("%s: nodes2 not fully consumed", nm.pre);
            return false;
        }
        if (i % G == 0) {
            const uint32_t goff = groups->u32();
            al = groups->u32();
            ag = groups->u32();
            if (goff != nodes2->tell()) die("%s: group offset %u is not the stream position", nm.pre, goff);
        }
        uint8_t rec[24];
        memset(rec, 0, sizeof rec);
        uint16_t cnt, code;
        if (street) {
            cnt = c0->u16();
            code = c1->u16();
            memcpy(rec + 8, c2->take(4), 4);
        } else {
            cnt = c0->u16();
            code = c0->u16();
            memcpy(rec + 8, c1->take(4), 4);
            const uint32_t start = c2->u32();
            wr32(rec + 12, start);
            wr32(rec + 16, start + c3->u32());
            rec[20] = c4->u8();
        }
        wr16(rec + 4, cnt);
        if (cnt) {
            wr32(rec, uint32_t(node_off));
            const size_t bs = block_size(cnt, code);
            const uint8_t* q = nodes2->need(bs + 8, true);
            const int wl = code & 63, wg = (code >> 6) & 63, wf = (code >> 13) & 7;
            const bool hyb = (code >> 12) & 1;
            uint32_t zl = 0, zg = 0;
            for (int b = 0; b < wf; b++) zl |= uint32_t(q[b]) << (8 * b);
            for (int b = 0; b < wf; b++) zg |= uint32_t(q[wf + b]) << (8 * b);
            const uint8_t* bp = q + 2 * wf;
            const uint32_t fl = ulp_unkey(ulp_key(al) + unzz(zl)), fg = ulp_unkey(ulp_key(ag) + unzz(zg));
            emit(0, rec, nm.stride);  // the record first: the queue reservation below may move
            uint8_t* o = emit_reserve(1, 8 * size_t(cnt));
            wr32(o, fl);
            wr32(o + 4, fg);
            if (cnt > 1) {
                uint32_t kl, kg;
                if (hyb) {
                    if (!hyb_key(fl, kl) || !hyb_key(fg, kg)) die("%s: way %llu hybrid key", nm.pre, (unsigned long long)i);
                } else {
                    kl = ulp_key(fl);
                    kg = ulp_key(fg);
                }
                const size_t mcount = size_t(cnt) - 1;
                const uint64_t mlk = wl == 32 ? 0xffffffffull : ((1ull << wl) - 1);
                const uint64_t mgk = wg == 32 ? 0xffffffffull : ((1ull << wg) - 1);
                size_t gl = 0, gg = mcount * size_t(wl);
                uint32_t dl = 0, dg = 0;
                for (size_t j = 0; j < mcount; j++) {
                    const uint32_t vl = unzz(uint32_t((rd64(bp + (gl >> 3)) >> (gl & 7)) & mlk));
                    const uint32_t vg = unzz(uint32_t((rd64(bp + (gg >> 3)) >> (gg & 7)) & mgk));
                    gl += size_t(wl);
                    gg += size_t(wg);
                    if (j == 0) { dl = vl; dg = vg; } else { dl += vl; dg += vg; }
                    kl += dl;
                    kg += dg;
                    wr32(o + 8 * (j + 1), hyb ? hyb_decode(kl) : ulp_unkey(kl));
                    wr32(o + 8 * (j + 1) + 4, hyb ? hyb_decode(kg) : ulp_unkey(kg));
                }
            }
            nodes2->skip(bs);
            node_off += cnt;
        } else {
            emit(0, rec, nm.stride);
        }
        i++;
        return true;
    }
};

// Ports: 0 = ways, 1 = nodes. A way is encoded once its record and all its nodes are in. The group
// header (nodes2 offset at the group start, anchor = first node of the group's first way with
// nodes) is written when that way arrives, or at the group's end when it has none.
struct Enc : Encoder {
    bool street;
    Names nm;
    size_t S;
    std::unique_ptr<Writer> groups, nodes2, c0, c1, c2, c3, c4;
    std::vector<uint8_t> blk;
    std::vector<uint32_t> kl, kg;
    uint64_t i = 0, expect_off = 0, nodes = 0, group_start = 0;
    bool group_pending = false;
    uint32_t al = 0, ag = 0;
    Enc(const std::string& od, bool st) : Encoder(st ? "streets" : "interps", od, 2), street(st), nm(names(st)), S(nm.stride) {
        const std::string p = od + "/" + nm.pre;
        groups.reset(new Writer(p + ".groups", 16 << 10));
        nodes2.reset(new Writer(p + ".nodes2"));
        c0.reset(new Writer(p + (street ? ".cnt" : ".cntcode"), 32 << 10));
        c1.reset(new Writer(p + (street ? ".code" : ".street"), 32 << 10));
        c2.reset(new Writer(p + (street ? ".name" : ".start"), 32 << 10));
        if (!street) {
            c3.reset(new Writer(p + ".span", 32 << 10));
            c4.reset(new Writer(p + ".interp", 16 << 10));
        }
    }
    void write_group(uint32_t l, uint32_t g) {
        if (group_start > 0xFFFFFFFFull) die("%s: nodes2 offset overflow", nm.pre);
        groups->u32(uint32_t(group_start));
        groups->u32(l);
        groups->u32(g);
        al = l;
        ag = g;
        group_pending = false;
    }
    void pump() override {
        ByteQueue &W = in[0], &N = in[1];
        while (W.size() >= S) {
            const uint8_t* r = W.data();
            const uint32_t off = rd32(r);
            const uint16_t cnt = rd16(r + 4);
            if (cnt && N.size() < 8 * size_t(cnt)) return;  // its nodes are not all in yet
            if (rd16(r + 6) || (!street && (r[21] | r[22] | r[23])))
                die("%s: way %llu has non-zero padding", nm.ways, (unsigned long long)i);
            if (cnt ? off != expect_off : off != 0)
                die("%s: way %llu node_offset %u is not the prefix sum %llu", nm.ways, (unsigned long long)i, off,
                    (unsigned long long)expect_off);
            if (i % G == 0) {
                if (group_pending) write_group(0, 0);
                group_pending = true;
                group_start = nodes2->tell();
            }
            uint16_t code = 0;
            if (cnt) {
                const uint8_t* nb = N.data();
                if (group_pending) write_group(rd32(nb), rd32(nb + 4));
                blk.clear();
                code = encode_way(nb, cnt, al, ag, blk, kl, kg);
                nodes2->put(blk);
                N.pop(8 * size_t(cnt));
                expect_off += cnt;
                nodes += cnt;
            }
            if (street) {
                c0->u16(cnt);
                c1->u16(code);
                c2->put(r + 8, 4);
            } else {
                c0->u16(cnt);
                c0->u16(code);
                c1->put(r + 8, 4);
                c2->put(r + 12, 4);
                c3->u32(rd32(r + 16) - rd32(r + 12));
                c4->u8(r[20]);
            }
            W.pop(S);
            i++;
        }
    }
    void finish(Manifest& m) override {
        require_empty();
        if (group_pending) write_group(0, 0);
        const std::string k = nm.pre;
        m.set(k + ".ways", i);
        m.set(k + ".nodes", nodes);
        m.set(k + ".G", G);
        m.set(k + ".nodes2_bytes", nodes2->tell());
        for (Writer* x : {groups.get(), nodes2.get(), c0.get(), c1.get(), c2.get(), c3.get(), c4.get()})
            if (x) x->finish();
        groups.reset(); nodes2.reset(); c0.reset(); c1.reset(); c2.reset(); c3.reset(); c4.reset();
    }
};
}  // namespace ways

// ------------------------------------------------------------------ u32 sidecars
namespace side {
static const size_t CHUNK = 8192;  // values per decoder step

// way_parents.u24: 3 bytes per value, NO_DATA -> 0xFFFFFF.
struct U24Dec : Decoder {
    uint64_t n, i = 0;
    std::unique_ptr<Reader> r;
    U24Dec(const std::string& src, const Manifest& m) : Decoder("way_parents", 1) {
        n = m.num("way_parents.n");
        r.reset(new Reader(src + "/way_parents.u24", 48 << 10));
        if (r->size() != 3 * n) die("way_parents.u24 size");
    }
    uint64_t decoded_size(int) const override { return 4 * n; }
    bool step() override {
        if (i == n) return false;
        const size_t k = size_t(std::min<uint64_t>(CHUNK, n - i));
        uint8_t* o = emit_reserve(0, 4 * k);
        for (size_t j = 0; j < k; j++) {
            const uint8_t* b = r->take(3);
            uint32_t v = uint32_t(b[0]) | (uint32_t(b[1]) << 8) | (uint32_t(b[2]) << 16);
            if (v == 0xFFFFFF) v = NO;
            wr32(o + 4 * j, v);
        }
        i += k;
        return true;
    }
};

struct U24Enc : Encoder {
    std::unique_ptr<Writer> w;
    uint64_t n = 0;
    explicit U24Enc(const std::string& od) : Encoder("way_parents", od, 1) { w.reset(new Writer(od + "/way_parents.u24", 48 << 10)); }
    size_t consume(int, const uint8_t* p, size_t bytes) override {
        const size_t k = bytes / 4;
        for (size_t j = 0; j < k; j++) {
            uint32_t v = rd32(p + 4 * j);
            if (v == NO) v = 0xFFFFFF;
            else if (v >= 0xFFFFFF) die("way_parents value %u does not fit 24 bits", v);
            const uint8_t b[3] = {uint8_t(v), uint8_t(v >> 8), uint8_t(v >> 16)};
            w->put(b, 3);
        }
        n += k;
        return 4 * k;
    }
    void pump() override { in[0].pop(consume(0, in[0].data(), in[0].size())); }
    void finish(Manifest& m) override {
        require_empty();
        m.set("way_parents.n", n);
        w->finish();
        w.reset();
    }
};

// <name>.u16 (index per value, 0xFFFF = NO_DATA) + <name>.dict (the distinct values, ascending).
struct DictDec : Decoder {
    uint64_t n, i = 0;
    std::vector<uint32_t> dict;
    std::unique_ptr<Reader> r;
    DictDec(const std::string& src, const std::string& nm, const Manifest& m) : Decoder(nm, 1) {
        n = m.num(nm + ".n");
        {
            Reader d(src + "/" + nm + ".dict", 16 << 10);
            dict.reserve(size_t(d.size() / 4));
            while (!d.eof()) dict.push_back(d.u32());
        }
        r.reset(new Reader(src + "/" + nm + ".u16", 32 << 10));
        if (r->size() != 2 * n) die("%s.u16 size", nm.c_str());
    }
    uint64_t decoded_size(int) const override { return 4 * n; }
    bool step() override {
        if (i == n) return false;
        const size_t k = size_t(std::min<uint64_t>(CHUNK, n - i));
        uint8_t* o = emit_reserve(0, 4 * k);
        for (size_t j = 0; j < k; j++) {
            const uint16_t x = r->u16();
            if (x == 0xFFFF) { wr32(o + 4 * j, NO); continue; }
            if (x >= dict.size()) die("%s: index out of range", name.c_str());
            wr32(o + 4 * j, dict[x]);
        }
        i += k;
        return true;
    }
};

// One pass: each value is written as its first-seen index; at the end the dictionary is sorted and
// the .u16 file is renumbered in place (a sequential read and write of it, no memory per value).
struct DictEnc : Encoder {
    size_t slots = 1 << 12;  // doubles to keep the load under a half (at most 2^17 for 65535 values)
    std::vector<uint32_t> keys;
    std::vector<uint16_t> vals;
    std::vector<uint32_t> dict;  // first-seen order
    std::unique_ptr<Writer> w;
    uint64_t n = 0, nd = 0;
    double renumber_s = 0;
    DictEnc(const std::string& od, const std::string& nm) : Encoder(nm, od, 1), keys(slots, NO), vals(slots, 0) {
        w.reset(new Writer(od + "/" + nm + ".u16", 32 << 10));
    }
    static inline size_t h(uint32_t k) { return size_t((uint64_t(k) * 0x9E3779B97F4A7C15ull) >> 32); }
    // Doubles the table and reinserts every value but the newest (the caller places it).
    void grow() {
        slots *= 2;
        keys.assign(slots, NO);
        vals.assign(slots, 0);
        for (size_t j = 0; j + 1 < dict.size(); j++) {
            size_t s = h(dict[j]) & (slots - 1);
            while (keys[s] != NO) s = (s + 1) & (slots - 1);
            keys[s] = dict[j];
            vals[s] = uint16_t(j);
        }
    }
    void pump() override { in[0].pop(consume(0, in[0].data(), in[0].size())); }
    size_t consume(int, const uint8_t* p, size_t bytes) override {
        const size_t k = bytes / 4;
        for (size_t j = 0; j < k; j++) {
            const uint32_t v = rd32(p + 4 * j);
            if (v == NO) { w->u16(0xFFFF); nd++; continue; }
            size_t s = h(v) & (slots - 1);
            while (keys[s] != NO && keys[s] != v) s = (s + 1) & (slots - 1);
            if (keys[s] == NO) {
                if (dict.size() >= 0xFFFF) die("%s: too many distinct values for a u16 index", name.c_str());
                const uint16_t id = uint16_t(dict.size());
                dict.push_back(v);
                if (2 * dict.size() > slots) {
                    grow();
                    s = h(v) & (slots - 1);
                    while (keys[s] != NO) s = (s + 1) & (slots - 1);
                }
                keys[s] = v;
                vals[s] = id;
            }
            w->u16(vals[s]);
        }
        n += k;
        return 4 * k;
    }
    void finish(Manifest& m) override {
        require_empty();
        w->finish();
        w.reset();
        std::vector<uint32_t> order(dict.size());
        for (size_t j = 0; j < order.size(); j++) order[j] = uint32_t(j);
        std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) { return dict[a] < dict[b]; });
        std::vector<uint16_t> final_of(dict.size());
        bool identity = true;
        for (size_t j = 0; j < order.size(); j++) {
            final_of[order[j]] = uint16_t(j);
            identity &= order[j] == j;
        }
        {
            Writer d(out_dir + "/" + name + ".dict", 16 << 10);
            for (uint32_t j : order) d.u32(dict[j]);
            d.finish();
        }
        if (!identity) {
            const std::string p = out_dir + "/" + name + ".u16";
            const int fd = open(p.c_str(), O_RDWR | O_CLOEXEC);
            if (fd < 0) die("open %s: %s", p.c_str(), strerror(errno));
            posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
            std::vector<uint16_t> b(16 << 10);
            for (uint64_t at = 0; at < 2 * n;) {
                const size_t want = size_t(std::min<uint64_t>(2 * b.size(), 2 * n - at));
                size_t got_b = 0;
                while (got_b < want) {
                    const ssize_t r = pread(fd, reinterpret_cast<char*>(b.data()) + got_b, want - got_b, off_t(at + got_b));
                    if (r < 0 && errno == EINTR) continue;
                    if (r <= 0) die("read %s", p.c_str());
                    got_b += size_t(r);
                }
                for (size_t j = 0; j < want / 2; j++)
                    if (b[j] != 0xFFFF) b[j] = final_of[b[j]];
                size_t put_b = 0;
                while (put_b < want) {
                    const ssize_t r = pwrite(fd, reinterpret_cast<char*>(b.data()) + put_b, want - put_b, off_t(at + put_b));
                    if (r < 0 && errno == EINTR) continue;
                    if (r <= 0) die("write %s", p.c_str());
                    put_b += size_t(r);
                }
                at += want;
            }
            if (close(fd)) die("close %s", p.c_str());
        }
        m.set(name + ".n", n);
        m.set(name + ".dict", dict.size());
        std::vector<uint32_t>().swap(keys);
        std::vector<uint16_t>().swap(vals);
    }
};
}  // namespace side

// ------------------------------------------------------------------ combined geo index (gxc)
namespace gxc {
static const uint64_t B = 128;
static const int CELL_SHIFT = 27;
struct __attribute__((packed)) Top { uint64_t cell; uint32_t roff, s, i, afirst, sprev, iprev; };
static_assert(sizeof(Top) == 32, "Top");
static const char* ENTRY[3] = {"street_entries.bin", "addr_entries.bin", "interp_entries.bin"};

// Ports: 0 = geo_cells, 1 = street_entries, 2 = addr_entries, 3 = interp_entries. A step: one row.
struct Dec : Decoder {
    uint64_t R, r = 0, next = 0, cell = 0, eoff[3] = {0, 0, 0}, ids;
    std::unique_ptr<Reader> top, rows, st[2];
    std::vector<uint32_t> cur[2], list;
    bool have[2] = {false, false};
    uint32_t cur_off[2] = {NO, NO}, cur_first[2] = {0, 0};
    uint64_t esize[3];
    Dec(const std::string& src, const Manifest& m) : Decoder("gxc", 4) {
        R = m.num("gxc.rows");
        ids = m.num("gxc.addr_ids");
        if (m.num("gxc.B") != B) die("unsupported B");
        for (int t = 0; t < 3; t++) esize[t] = m.num(std::string("gxc.") + ENTRY[t]);
        top.reset(new Reader(src + "/gxc_top.bin", 4 << 10));
        rows.reset(new Reader(src + "/gxc_rows.bin"));
        st[0].reset(new Reader(src + "/gxc_street.bin"));
        st[1].reset(new Reader(src + "/gxc_interp.bin"));
        if (top->size() != 32 * ((R + B - 1) / B)) die("gxc_top.bin size");
    }
    uint64_t decoded_size(int port) const override { return port == 0 ? 20 * R : esize[port - 1]; }
    // A list holds 1 to 65535 ids (today's u16 count); every count is checked before it is used.
    static void decode_list(Reader& s, bool strict, uint32_t prev_first, std::vector<uint32_t>& l) {
        const uint64_t cm1 = s.uv();  // count - 1
        if (cm1 >= 0xFFFF) die("gxc: list too long");
        const size_t c = size_t(cm1) + 1;
        l.resize(c);
        l[0] = uint32_t(int64_t(prev_first) + unzz64(s.uv()));
        for (size_t j = 1; j < c; j++) l[j] = l[j - 1] + uint32_t(s.uv()) + (strict ? 1 : 0);
    }
    uint32_t emit_list(int t, const std::vector<uint32_t>& l) {
        if (eoff[t] > 0xFFFFFFFFull) die("gxc: entries offset overflow");
        const uint32_t o = uint32_t(eoff[t]);
        uint8_t* p = emit_reserve(1 + t, 2 + 4 * l.size());
        wr16(p, uint16_t(l.size()));
        memcpy(p + 2, l.data(), 4 * l.size());
        eoff[t] += 2 + 4 * l.size();
        return o;
    }
    bool step() override {
        if (r == R) {
            if (!rows->eof() || !st[0]->eof() || !st[1]->eof() || !top->eof()) die("gxc: streams not fully consumed");
            if (next != ids) die("gxc: address id count");
            return false;
        }
        if (r % B == 0) {
            Top t;
            memcpy(&t, top->take(32), 32);
            if (t.roff != rows->tell() || t.s != cur_off[0] || t.i != cur_off[1] || t.afirst != uint32_t(next) ||
                t.sprev != cur_first[0] || t.iprev != cur_first[1])
                die("gxc: block %llu entry state differs from the running state", (unsigned long long)(r / B));
            cell = t.cell;
        }
        const uint8_t h = rows->u8();
        const int d = h >> 6;
        if (r % B) {
            const uint64_t step = d < 3 ? uint64_t(d) + 1 : rows->uv() + 4;
            cell += step << CELL_SHIFT;
        } else if (d) {
            die("gxc: step on a block's first row");
        }
        uint32_t off[3] = {NO, NO, NO};
        const int as = (h >> 2) & 3;
        if (as) {
            list.clear();
            auto bad = [&] { die("gxc: bad address list at row %llu", (unsigned long long)r); };
            if (as == 1) {
                const uint64_t k = rows->uv();
                if (k == 0 || k > 0xFFFF) bad();
                for (uint64_t j = 0; j < k; j++) list.push_back(uint32_t(next + j));
                next += k;
            } else if (as == 2) {
                const uint64_t K = rows->uv(), D = rows->uv();
                if (K == 0 || K > 0xFFFF || D > K) bad();
                uint64_t pj = 0, j = 0;  // pj: one past the previous repeated index
                for (uint64_t x = 0; x < D; x++) {
                    const uint64_t gap = rows->uv(), cm2 = rows->uv();  // copies - 2
                    if (gap >= K - pj || cm2 >= 0xFFFF) bad();
                    const uint64_t idx = pj + gap, copies = cm2 + 2;
                    if (list.size() + (idx - j) + copies > 0xFFFF) bad();
                    for (; j < idx; j++) list.push_back(uint32_t(next + j));
                    for (uint64_t c = 0; c < copies; c++) list.push_back(uint32_t(next + j));
                    j++;
                    pj = idx + 1;
                }
                for (; j < K; j++) list.push_back(uint32_t(next + j));
                next += K;
            } else {
                const uint64_t k = rows->uv(), cm1 = rows->uv();  // count - 1
                if (cm1 >= 0xFFFF) bad();
                const uint64_t c = cm1 + 1;
                list.push_back(uint32_t(int64_t(next) + unzz64(rows->uv())));
                for (uint64_t j = 1; j < c; j++) list.push_back(list.back() + uint32_t(rows->uv()));
                next += k;
            }
            if (list.empty() || list.size() > 0xFFFF) die("gxc: bad address list at row %llu", (unsigned long long)r);
            off[1] = emit_list(1, list);
        }
        for (int k = 0; k < 2; k++) {
            const int s = (h >> (k ? 4 : 0)) & 3;
            if (!s) continue;
            if (s == 2) {
                if (!have[k]) die("gxc: repeat without a list at row %llu", (unsigned long long)r);
            } else {
                cur_off[k] = uint32_t(st[k]->tell());
                decode_list(*st[k], s == 1, cur_first[k], cur[k]);
                cur_first[k] = cur[k][0];
                have[k] = true;
            }
            off[k ? 2 : 0] = emit_list(k ? 2 : 0, cur[k]);
        }
        uint8_t row[20];
        wr64(row, cell);
        wr32(row + 8, off[0]);
        wr32(row + 12, off[1]);
        wr32(row + 16, off[2]);
        emit(0, row, 20);
        r++;
        return true;
    }
};

// Ports as the decoder's. A row is encoded once it and the lists it names are all in (the patcher
// writes a cell's lists before its row, through separately buffered streams).
struct Enc : Encoder {
    std::unique_ptr<Writer> top, rows, st[2];
    uint64_t eoff[3] = {0, 0, 0}, consumed[3] = {0, 0, 0};
    std::vector<uint32_t> list, prev[2];
    bool have_prev[2] = {false, false};
    uint32_t cur_off[2] = {NO, NO}, cur_first[2] = {0, 0};
    uint64_t next = 0, prev_cell = 0, r = 0;
    std::vector<uint8_t> pay;
    explicit Enc(const std::string& od) : Encoder("gxc", od, 4) {
        top.reset(new Writer(od + "/gxc_top.bin", 8 << 10));
        rows.reset(new Writer(od + "/gxc_rows.bin"));
        st[0].reset(new Writer(od + "/gxc_street.bin"));
        st[1].reset(new Writer(od + "/gxc_interp.bin"));
    }
    // The next list of entries stream t, if all of it is in: count, then a pointer to its ids.
    bool list_ready(int t, uint32_t off) const {
        const ByteQueue& q = in[size_t(1 + t)];
        if (eoff[t] > 0xFFFFFFFFull || uint32_t(eoff[t]) != off)
            die("%s: list offset %u is not the running offset %llu", ENTRY[t], off, (unsigned long long)eoff[t]);
        if (q.size() < 2) return false;
        const uint16_t c = rd16(q.data());
        return q.size() >= 2 + 4 * size_t(c);
    }
    void take_list(int t, std::vector<uint32_t>& l) {
        ByteQueue& q = in[size_t(1 + t)];
        const uint16_t c = rd16(q.data());
        if (!c) die("%s: empty list", ENTRY[t]);
        l.resize(c);
        memcpy(l.data(), q.data() + 2, 4 * size_t(c));
        q.pop(2 + 4 * size_t(c));
        eoff[t] += 2 + 4 * uint64_t(c);
    }
    void pump() override {
        ByteQueue& G = in[0];
        while (G.size() >= 20) {
            const uint8_t* g = G.data();
            const uint64_t cell = rd64(g);
            const uint32_t off[3] = {rd32(g + 8), rd32(g + 12), rd32(g + 16)};
            for (int t = 0; t < 3; t++)
                if (off[t] != NO && !list_ready(t, off[t])) return;
            G.pop(20);  // (g stays valid: nothing is pushed while the row is processed)
            uint8_t d = 0;
            pay.clear();
            if (r % B == 0) {
                if (rows->tell() > 0xFFFFFFFFull || next > 0xFFFFFFFFull) die("gxc: top field overflow");
                const Top t{cell, uint32_t(rows->tell()), cur_off[0], cur_off[1], uint32_t(next), cur_first[0], cur_first[1]};
                top->put(&t, sizeof t);
            } else {
                if (cell <= prev_cell) die("gxc: rows out of order at %llu", (unsigned long long)r);
                const uint64_t diff = cell - prev_cell;
                if (diff & ((uint64_t(1) << CELL_SHIFT) - 1)) die("gxc: cell step not a multiple of 2^27 at row %llu", (unsigned long long)r);
                const uint64_t step = diff >> CELL_SHIFT;
                d = uint8_t(step <= 3 ? step - 1 : 3);
                if (d == 3) put_uv(pay, step - 4);
            }
            prev_cell = cell;
            uint8_t ls[2] = {0, 0};
            for (int k = 0; k < 2; k++) {
                const int t = k ? 2 : 0;
                if (off[t] == NO) continue;
                take_list(t, list);
                if (have_prev[k] && list == prev[k]) {
                    ls[k] = 2;
                } else {
                    bool strict = true;
                    for (size_t j = 1; j < list.size(); j++) {
                        if (list[j] < list[j - 1]) die("%s: unsorted list at row %llu", ENTRY[t], (unsigned long long)r);
                        if (list[j] == list[j - 1]) strict = false;
                    }
                    ls[k] = strict ? 1 : 3;
                    if (st[k]->tell() >= 0xFFFFFFFFull) die("gxc: stream offset overflow");
                    cur_off[k] = uint32_t(st[k]->tell());
                    st[k]->uv(list.size() - 1);
                    st[k]->uv(zz64(int64_t(list[0]) - int64_t(cur_first[k])));
                    for (size_t j = 1; j < list.size(); j++) st[k]->uv(list[j] - list[j - 1] - (strict ? 1 : 0));
                    cur_first[k] = list[0];
                    prev[k].swap(list);
                    have_prev[k] = true;
                }
            }
            uint8_t as = 0;
            if (off[1] != NO) {
                take_list(1, list);
                const size_t c = list.size();
                bool run = list[0] == next, exact = true;
                for (size_t j = 1; j < c && run; j++) {
                    const uint32_t s = list[j] - list[j - 1];
                    if (list[j] < list[j - 1] || s > 1) run = false;
                    else if (s == 0) exact = false;
                }
                if (run && exact) {
                    as = 1;
                    put_uv(pay, c);
                    next += c;
                } else if (run) {
                    as = 2;
                    const uint64_t K = uint64_t(list.back()) - next + 1;
                    put_uv(pay, K);
                    size_t ndup = 0;
                    for (size_t j = 0; j < c;) {
                        size_t e = j + 1;
                        while (e < c && list[e] == list[j]) e++;
                        if (e - j > 1) ndup++;
                        j = e;
                    }
                    put_uv(pay, ndup);
                    int64_t pj = -1;
                    for (size_t j = 0; j < c;) {
                        size_t e = j + 1;
                        while (e < c && list[e] == list[j]) e++;
                        if (e - j > 1) {
                            const uint64_t idx = list[j] - next;
                            put_uv(pay, uint64_t(int64_t(idx) - pj - 1));
                            put_uv(pay, e - j - 2);
                            pj = int64_t(idx);
                        }
                        j = e;
                    }
                    next += K;
                } else {
                    as = 3;
                    for (size_t j = 1; j < c; j++)
                        if (list[j] < list[j - 1]) die("addr_entries: unsorted list at row %llu", (unsigned long long)r);
                    const uint64_t k = uint64_t(list.back()) + 1 > next ? uint64_t(list.back()) + 1 - next : 0;
                    put_uv(pay, k);
                    put_uv(pay, c - 1);
                    put_uv(pay, zz64(int64_t(list[0]) - int64_t(next)));
                    for (size_t j = 1; j < c; j++) put_uv(pay, list[j] - list[j - 1]);
                    next += k;
                }
            }
            rows->u8(uint8_t(ls[0] | (as << 2) | (ls[1] << 4) | (d << 6)));
            rows->put(pay);
            r++;
        }
    }
    void finish(Manifest& m) override {
        require_empty();
        m.set("gxc.rows", r);
        m.set("gxc.B", B);
        m.set("gxc.addr_ids", next);
        for (int t = 0; t < 3; t++) m.set(std::string("gxc.") + ENTRY[t], eoff[t]);
        for (Writer* x : {top.get(), rows.get(), st[0].get(), st[1].get()}) x->finish();
        top.reset(); rows.reset(); st[0].reset(); st[1].reset();
    }
};
}  // namespace gxc

// ------------------------------------------------------------------ address points + footprints
namespace addr {
static const double SCALE[3] = {1e-5, 1e-4, 1e-6};
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
// The col file's region k: lat b0..b3, lng b0..b3, housenumber, street, parent_way, poly_offset b0..b3.
static inline uint64_t col_region(int k, uint64_t N) {
    uint64_t at = 0;
    for (int j = 0; j < k; j++) at += (j >= 8 && j <= 10) ? 4 * N : N;
    return at;
}
static inline size_t region_buf(int k) { return (k >= 8 && k <= 10) ? size_t(32) << 10 : size_t(PLANE_BUF); }

// Ports: 0 = addr_points, 1 = addr_vertices. A step: one record and its footprint.
struct Dec : Decoder {
    uint64_t N, i = 0, raw = 0;
    std::vector<std::unique_ptr<Reader>> rg;
    std::unique_ptr<Reader> A;
    Dec(const std::string& src, const Manifest& m) : Decoder("addr", 2) {
        N = m.num("addr.n");
        if (m.get("addr.layout") != "col") die("addr: only the col layout streams");
        const std::string cp = src + "/addr_points.col";
        if (file_size(cp) != 24 * N) die("addr_points.col size");
        for (int k = 0; k < 15; k++) rg.emplace_back(new Reader(cp, region_buf(k), col_region(k, N), col_region(k + 1, N)));
        A.reset(new Reader(src + "/addr_vertices.abs2"));
    }
    uint64_t decoded_size(int port) const override { return port == 0 ? 28 * N : ~0ull; }
    bool step() override {
        if (i == N) {
            if (!A->eof()) die("addr_vertices.abs2 not fully consumed");
            return false;
        }
        uint32_t v[6] = {0, 0, 0, 0, 0, 0};
        for (int b = 0; b < 4; b++) v[0] |= uint32_t(rg[size_t(b)]->u8()) << (8 * b);
        for (int b = 0; b < 4; b++) v[1] |= uint32_t(rg[size_t(4 + b)]->u8()) << (8 * b);
        v[2] = rg[8]->u32();
        v[3] = rg[9]->u32();
        v[4] = rg[10]->u32();
        for (int b = 0; b < 4; b++) v[5] |= uint32_t(rg[size_t(11 + b)]->u8()) << (8 * b);
        uint32_t rec[7] = {v[0], v[1], v[2], v[3], v[4], NO, 0};
        if (v[5] == NO) {
            emit(0, rec, 28);
            i++;
            return true;
        }
        if (v[5] != A->tell()) die("addr %llu: poly_offset %u is not the stream position", (unsigned long long)i, v[5]);
        if (raw >= 0xFFFFFFFFull) die("addr_vertices offset overflow");
        rec[5] = uint32_t(raw);
        const uint8_t tag = A->u8();
        if (tag == 0x20) {
            const uint64_t vcnt = A->uv();
            const uint8_t enc = *A->need(1);
            const size_t size = 10 + size_t(vcnt) * (enc == 3 ? 8 : 4);
            rec[6] = uint32_t(vcnt);
            emit(0, rec, 28);
            emit(1, A->take(size), size);
            raw += size;
            i++;
            return true;
        }
        const int enc = tag & 3, wide = (tag >> 2) & 1, small = (tag >> 3) & 1, closed = (tag >> 4) & 1;
        if (enc > 2 || (tag & 0xE0)) die("addr %llu: bad tag", (unsigned long long)i);
        const uint64_t cnt = A->uv();
        if (cnt > 69999 - uint64_t(closed) || (closed && !cnt)) die("addr %llu: bad count", (unsigned long long)i);
        const uint32_t full = uint32_t(cnt) + uint32_t(closed);
        rec[6] = full;
        emit(0, rec, 28);
        const size_t bsz = 10 + 4 * size_t(full);
        uint8_t bm[8];
        memcpy(bm, A->take(small ? 2 : 8), small ? 2 : 8);
        const uint8_t* p = A->take(2 * size_t(cnt) * (wide ? 2 : 1));
        uint8_t* blob = emit_reserve(1, bsz);
        memset(blob, 0, 10);
        blob[0] = uint8_t(enc);
        int64_t sl = 0, sg = 0;
        for (uint64_t k = 0; k < cnt; k++) {
            const uint16_t ql = wide ? rd16(p + 2 * k) : p[k];
            const uint16_t qg = wide ? rd16(p + 2 * (cnt + k)) : p[cnt + k];
            sl += ql;
            sg += qg;
            wr16(blob + 10 + 4 * k, ql);
            wr16(blob + 12 + 4 * k, qg);
        }
        if (closed) {
            memcpy(blob + 10 + 4 * cnt, blob + 10, 4);
            sl += rd16(blob + 10);
            sg += rd16(blob + 12);
        }
        uint32_t bl, bg;
        if (small) {
            bl = unfkey(fkey(pred_bits(v[0], sl, full, enc)) + int8_t(bm[0]));
            bg = unfkey(fkey(pred_bits(v[1], sg, full, enc)) + int8_t(bm[1]));
        } else {
            bl = rd32(bm);
            bg = rd32(bm + 4);
        }
        wr32(blob + 2, bl);
        wr32(blob + 6, bg);
        raw += bsz;
        i++;
        return true;
    }
};

// Ports: 0 = addr_points, 1 = addr_vertices; a record is encoded once its footprint is all in.
// N (for the col layout's regions) comes from the expected size of addr_points.bin.
struct Enc : Encoder {
    uint64_t N = 0, i = 0, vexp = 0;
    int fd = -1;
    std::vector<std::unique_ptr<Writer>> w;
    std::unique_ptr<Writer> A;
    Enc(const std::string& od, uint64_t points_size) : Encoder("addr", od, 2) {
        if (points_size % 28) die("addr_points.bin size");
        N = points_size / 28;
        const std::string cp = od + "/addr_points.col";
        if (unlink(cp.c_str()) && errno != ENOENT) die("unlink %s", cp.c_str());
        fd = open(cp.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
        if (fd < 0) die("create %s", cp.c_str());
        for (int k = 0; k < 15; k++) w.emplace_back(new Writer(fd, cp, col_region(k, N), region_buf(k)));
        A.reset(new Writer(od + "/addr_vertices.abs2"));
    }
    ~Enc() override {
        if (fd >= 0) close(fd);
    }
    void put_po(uint32_t v) { for (int b = 0; b < 4; b++) w[size_t(11 + b)]->u8(uint8_t(v >> (8 * b))); }
    void pump() override {
        ByteQueue &P = in[0], &V = in[1];
        while (P.size() >= 28) {
            const uint8_t* r = P.data();
            const uint32_t lat = rd32(r), lng = rd32(r + 4), voff = rd32(r + 20), vcnt = rd32(r + 24);
            size_t size = 0;
            if (vcnt) {
                if (V.size() < 1) return;
                const uint8_t enc = V.data()[0];
                if (enc > 3) die("addr_vertices: unknown encoding %u", enc);
                size = 10 + size_t(vcnt) * (enc == 3 ? 8 : 4);
                if (V.size() < size) return;
            }
            if (i >= N) die("addr: more records than addr_points.bin's size gives");
            for (int b = 0; b < 4; b++) w[size_t(b)]->u8(uint8_t(lat >> (8 * b)));
            for (int b = 0; b < 4; b++) w[size_t(4 + b)]->u8(uint8_t(lng >> (8 * b)));
            w[8]->put(r + 8, 4);
            w[9]->put(r + 12, 4);
            w[10]->put(r + 16, 4);
            if (!vcnt) {
                if (voff != NO) die("addr_points %llu: vertex_count 0 with vertex_offset %u", (unsigned long long)i, voff);
                put_po(NO);
                P.pop(28);
                i++;
                continue;
            }
            if (voff != vexp) die("addr_points %llu: vertex_offset %u is not the running offset %llu", (unsigned long long)i, voff,
                                  (unsigned long long)vexp);
            if (A->tell() >= 0xFFFFFFFFull) die("addr_vertices.abs2 offset overflow");
            put_po(uint32_t(A->tell()));
            const uint8_t* b = V.data();
            const uint8_t enc = b[0];
            vexp += size;
            if (enc == 3 || b[1] != 0) {
                A->u8(0x20);
                A->uv(vcnt);
                A->put(b, size);
            } else {
                const uint32_t bl = rd32(b + 2), bg = rd32(b + 6);
                int64_t sl = 0, sg = 0;
                uint32_t span = 0;
                for (uint32_t k = 0; k < vcnt; k++) {
                    const uint32_t ql = rd16(b + 10 + 4 * size_t(k)), qg = rd16(b + 12 + 4 * size_t(k));
                    sl += ql;
                    sg += qg;
                    span = std::max(span, std::max(ql, qg));
                }
                const bool closed = vcnt >= 2 && rd16(b + 10) == rd16(b + 10 + 4 * size_t(vcnt - 1)) &&
                                    rd16(b + 12) == rd16(b + 12 + 4 * size_t(vcnt - 1));
                const uint32_t oc = vcnt - (closed ? 1 : 0);
                const bool wide = span >= 256;
                const int64_t rl = fkey(bl) - fkey(pred_bits(lat, sl, vcnt, enc));
                const int64_t rgg = fkey(bg) - fkey(pred_bits(lng, sg, vcnt, enc));
                const bool small = rl > -128 && rl < 128 && rgg > -128 && rgg < 128;
                A->u8(uint8_t(enc | (wide << 2) | (small << 3) | (closed << 4)));
                A->uv(oc);
                if (small) {
                    A->u8(uint8_t(int8_t(rl)));
                    A->u8(uint8_t(int8_t(rgg)));
                } else {
                    A->u32(bl);
                    A->u32(bg);
                }
                for (int c = 0; c < 2; c++)
                    for (uint32_t k = 0; k < oc; k++) {
                        const uint16_t q = rd16(b + 10 + 4 * size_t(k) + 2 * size_t(c));
                        if (wide) A->u16(q);
                        else A->u8(uint8_t(q));
                    }
            }
            V.pop(size);
            P.pop(28);
            i++;
        }
    }
    void finish(Manifest& m) override {
        require_empty();
        if (i != N) die("addr: %llu records, addr_points.bin's size gives %llu", (unsigned long long)i, (unsigned long long)N);
        for (auto& x : w) x->finish();
        w.clear();
        const int f = fd;
        fd = -1;
        if (close(f)) die("close addr_points.col");
        m.set("addr.n", N);
        m.set("addr.layout", "col");
        m.set("addr.abs2_bytes", A->tell());
        A->finish();
        A.reset();
    }
};
}  // namespace addr

// ------------------------------------------------------------------ admin layout B, POI R3 + B-open
namespace poly {
static inline uint32_t zzw(uint32_t d, int bits) {  // d: wrapped delta in [0, 2^bits)
    const int64_t full = bits == 32 ? 0x100000000LL : (1LL << bits);
    const int64_t s = d >= uint64_t(full / 2) ? int64_t(d) - full : int64_t(d);
    return uint32_t(s >= 0 ? 2 * s : -2 * s - 1);
}
static inline uint32_t unzz(uint32_t z) { return (z >> 1) ^ uint32_t(-int32_t(z & 1)); }
static inline uint64_t wmask(int w) { return w == 4 ? 0xFFFFFFFFull : ((1ull << (8 * w)) - 1); }
static inline uint32_t getw(const uint8_t* p, int w) {
    uint32_t q = 0;
    for (int b = 0; b < w; b++) q |= uint32_t(p[b]) << (8 * b);
    return q;
}
static void planes_enc(const uint8_t* src, uint8_t* dst, uint64_t c, int w) {
    const uint64_t mask = wmask(w);
    for (int comp = 0; comp < 2; comp++) {
        uint32_t prev = 0;
        for (uint64_t k = 0; k < c; k++) {
            const uint32_t q = getw(src + k * 2 * size_t(w) + size_t(comp * w), w);
            const uint32_t z = zzw(uint32_t((uint64_t(q) - prev) & mask), 8 * w);
            for (int b = 0; b < w; b++) dst[size_t(comp * w + b) * c + k] = uint8_t(z >> (8 * b));
            prev = q;
        }
    }
}
static void planes_dec(const uint8_t* src, uint8_t* dst, uint64_t c, int w) {
    const uint64_t mask = wmask(w);
    for (int comp = 0; comp < 2; comp++) {
        uint32_t prev = 0;
        for (uint64_t k = 0; k < c; k++) {
            uint32_t z = 0;
            for (int b = 0; b < w; b++) z |= uint32_t(src[size_t(comp * w + b) * c + k]) << (8 * b);
            const uint32_t q = uint32_t((uint64_t(prev) + unzz(z)) & mask);
            for (int b = 0; b < w; b++) dst[k * 2 * size_t(w) + size_t(comp * w + b)] = uint8_t(q >> (8 * b));
            prev = q;
        }
    }
}

// The next polygon of an admin_polygons.bin with vertices: offset and count; false at the end.
struct PolyCursor {
    std::unique_ptr<Reader> r;
    uint64_t n = 0;
    explicit PolyCursor(const std::string& path) {
        r.reset(new Reader(path, 24 << 10));
        if (r->size() % 24) die("admin_polygons.bin size");
    }
    bool next(uint64_t& off, uint64_t& cnt) {
        while (!r->eof()) {
            const uint8_t* p = r->take(24);
            const uint32_t vo = rd32(p), vc = rd32(p + 4);
            if (vc > 0 && vo != NO) {
                off = vo;
                cnt = vc;
                n++;
                return true;
            }
        }
        return false;
    }
};

static const size_t VERTS_PER_STEP = 8192;

// admin_vertices.B -> admin_vertices.bin (port 0). The polygons come in offset order (the sort step's
// admin_polygons.bin is; a gap or overlap fails). A big block is read plane by plane.
struct AdminDec : Decoder {
    PolyCursor pc;
    std::unique_ptr<Reader> R;
    std::vector<std::unique_ptr<Reader>> pl;  // big block: its 2w planes
    std::vector<uint8_t> blk;
    uint64_t pos = 0, big_cnt = 0, big_k = 0, big_end = 0;
    int big_w = 0;
    uint32_t prev[2] = {0, 0};
    std::string bpath;
    AdminDec(const std::string& src) : Decoder("admin", 1), pc(src + "/admin_polygons.bin") {
        bpath = src + "/admin_vertices.B";
        R.reset(new Reader(bpath));
    }
    uint64_t decoded_size(int) const override { return R->size(); }
    bool step() override {
        if (!pl.empty()) {  // inside a big block
            const uint64_t k = std::min<uint64_t>(VERTS_PER_STEP, big_cnt - big_k);
            const int w = big_w;
            const uint64_t mask = wmask(w);
            uint8_t* o = emit_reserve(0, size_t(k) * 2 * size_t(w));
            for (uint64_t j = 0; j < k; j++)
                for (int comp = 0; comp < 2; comp++) {
                    uint32_t z = 0;
                    for (int b = 0; b < w; b++) z |= uint32_t(pl[size_t(comp * w + b)]->u8()) << (8 * b);
                    const uint32_t q = uint32_t((uint64_t(prev[comp]) + unzz(z)) & mask);
                    for (int b = 0; b < w; b++) o[j * 2 * size_t(w) + size_t(comp * w + b)] = uint8_t(q >> (8 * b));
                    prev[comp] = q;
                }
            big_k += k;
            if (big_k == big_cnt) {
                pl.clear();
                R->seek(big_end);
            }
            return true;
        }
        uint64_t off, cnt;
        if (!pc.next(off, cnt)) {
            if (!R->eof()) die("admin_vertices: bytes after the last polygon");
            return false;
        }
        if (off != pos) die("admin_vertices: gap or overlap at offset %llu (expected %llu)", (unsigned long long)off, (unsigned long long)pos);
        const uint8_t e = *R->need(1);
        if (e > 3) die("admin_vertices: bad encoding %u", e);
        const int w = e == 3 ? 4 : 2;
        const size_t size = 10 + size_t(cnt) * 2 * size_t(w);
        pos += size;
        if (size <= big_block()) {
            const uint8_t* s = R->take(size);
            uint8_t* o = emit_reserve(0, size);
            memcpy(o, s, 10);
            planes_dec(s + 10, o + 10, cnt, w);
            return true;
        }
        emit(0, R->take(10), 10);
        const uint64_t base = R->tell();
        for (int j = 0; j < 2 * w; j++)
            pl.emplace_back(new Reader(bpath, PLANE_BUF, base + uint64_t(j) * cnt, base + uint64_t(j + 1) * cnt));
        big_cnt = cnt;
        big_k = 0;
        big_w = w;
        big_end = base + 2 * uint64_t(w) * cnt;
        prev[0] = prev[1] = 0;
        return true;
    }
};

// admin_vertices.bin (port 0) -> admin_vertices.B, with the new admin_polygons.bin (the patcher has
// written it before admin_vertices.bin starts). A big block is written plane by plane.
struct AdminEnc : Encoder {
    std::unique_ptr<PolyCursor> pc;
    int fd = -1;
    std::unique_ptr<Writer> W;
    std::vector<std::unique_ptr<Writer>> pl;
    std::vector<uint8_t> blk;
    std::string path;
    bool in_poly = false, done = false;
    uint64_t pos = 0, cnt = 0, k = 0, size = 0;
    int w = 0;
    uint32_t prev[2] = {0, 0};
    explicit AdminEnc(const std::string& od) : Encoder("admin", od, 1) {
        path = od + "/admin_vertices.B";
        if (unlink(path.c_str()) && errno != ENOENT) die("unlink %s", path.c_str());
        fd = open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
        if (fd < 0) die("create %s", path.c_str());
    }
    ~AdminEnc() override {
        if (fd >= 0) close(fd);
    }
    void pump() override {
        if (!pc) pc.reset(new PolyCursor(out_dir + "/admin_polygons.bin"));
        ByteQueue& q = in[0];
        for (;;) {
            if (!in_poly) {
                if (q.size() < 1) return;
                uint64_t off;
                if (!pc->next(off, cnt)) die("admin_vertices: bytes after the last polygon");
                if (off != pos) die("admin_vertices: gap or overlap at offset %llu (expected %llu)", (unsigned long long)off,
                                    (unsigned long long)pos);
                const uint8_t e = q.data()[0];
                if (e > 3) die("admin_vertices: bad encoding %u", e);
                w = e == 3 ? 4 : 2;
                size = 10 + size_t(cnt) * 2 * size_t(w);
                in_poly = true;
                k = 0;
            }
            if (size <= big_block()) {
                if (q.size() < size) return;
                blk.resize(size);
                memcpy(blk.data(), q.data(), 10);
                planes_enc(q.data() + 10, blk.data() + 10, cnt, w);
                W_put(blk.data(), size);
                q.pop(size);
                pos += size;
                in_poly = false;
                continue;
            }
            if (pl.empty() && k == 0) {  // big block: header, then plane writers at their offsets
                if (q.size() < 10) return;
                W_put(q.data(), 10);
                q.pop(10);
                for (int j = 0; j < 2 * w; j++)
                    pl.emplace_back(new Writer(fd, path, pos + 10 + uint64_t(j) * cnt, PLANE_BUF));
                prev[0] = prev[1] = 0;
            }
            const size_t vs = 2 * size_t(w);
            const uint64_t avail = std::min<uint64_t>(q.size() / vs, cnt - k);
            const uint64_t mask = wmask(w);
            const uint8_t* s = q.data();
            for (uint64_t j = 0; j < avail; j++)
                for (int comp = 0; comp < 2; comp++) {
                    const uint32_t v = getw(s + j * vs + size_t(comp * w), w);
                    const uint32_t z = zzw(uint32_t((uint64_t(v) - prev[comp]) & mask), 8 * w);
                    for (int b = 0; b < w; b++) pl[size_t(comp * w + b)]->u8(uint8_t(z >> (8 * b)));
                    prev[comp] = v;
                }
            q.pop(size_t(avail) * vs);
            k += avail;
            if (k < cnt) return;
            for (auto& x : pl) x->finish();
            pl.clear();
            pos += size;
            in_poly = false;
            W_skip_to(pos);
        }
    }
    // The block writer: sequential at pos (a big block's planes go around it).
    void W_put(const uint8_t* p, size_t n) {
        if (!W) W.reset(new Writer(fd, path, 0, WRITE_BUF));
        W->put(p, n);
    }
    void W_skip_to(uint64_t at) {
        if (!W) W.reset(new Writer(fd, path, 0, WRITE_BUF));
        W->skip_to(at);
    }
    void finish(Manifest& m) override {
        require_empty();
        if (!pc) pc.reset(new PolyCursor(out_dir + "/admin_polygons.bin"));
        if (in_poly) die("admin_vertices: stream ends inside a polygon");
        uint64_t off, c;
        if (pc->next(off, c)) die("admin_vertices: stream ends before the last polygon");
        if (W) W->finish();
        W.reset();
        const int f = fd;
        fd = -1;
        if (close(f)) die("close %s", path.c_str());
        m.set("admin.polygons", pc->n);
    }
};

// Ports: 0 = poi_records (36 B), 1 = poi_vertices. A step: one record and its block, or a piece of a
// big block (read plane by plane).
struct PoiDec : Decoder {
    uint64_t n, i = 0, total, vt = 0;
    std::unique_ptr<Reader> R, V;
    uint8_t cur[32], nxt[32];
    std::string vpath;
    // big block in progress
    std::vector<std::unique_ptr<Reader>> pl;
    uint64_t big_c2 = 0, big_k = 0, big_end = 0;
    int big_w = 0;
    bool big_flag = false;
    uint8_t big_first[8];
    uint32_t prev[2] = {0, 0};
    PoiDec(const std::string& src, const Manifest& m) : Decoder("poi", 2) {
        n = m.num("poi.n");
        vpath = src + "/poi_vertices.bopen";
        R.reset(new Reader(src + "/poi_records.r3", 32 << 10));
        V.reset(new Reader(vpath));
        if (R->size() != 32 * n) die("poi_records.r3 size");
        total = V->size();
        if (n) memcpy(cur, R->take(32), 32);
    }
    uint64_t decoded_size(int port) const override { return port == 0 ? 36 * n : ~0ull; }
    void next_record() {
        memcpy(cur, nxt, 32);
        i++;
    }
    bool step() override {
        if (!pl.empty()) {
            const int w = big_w;
            const size_t vs = 2 * size_t(w);
            const uint64_t k = std::min<uint64_t>(VERTS_PER_STEP, big_c2 - big_k);
            const uint64_t mask = wmask(w);
            uint8_t* o = emit_reserve(1, size_t(k) * vs);
            for (uint64_t j = 0; j < k; j++)
                for (int comp = 0; comp < 2; comp++) {
                    uint32_t z = 0;
                    for (int b = 0; b < w; b++) z |= uint32_t(pl[size_t(comp * w + b)]->u8()) << (8 * b);
                    const uint32_t q = uint32_t((uint64_t(prev[comp]) + unzz(z)) & mask);
                    for (int b = 0; b < w; b++) o[j * vs + size_t(comp * w + b)] = uint8_t(q >> (8 * b));
                    prev[comp] = q;
                }
            if (big_k == 0 && k) memcpy(big_first, o, vs);
            big_k += k;
            if (big_k == big_c2) {
                if (big_flag) emit(1, big_first, vs);
                pl.clear();
                V->seek(big_end);
                next_record();
            }
            return true;
        }
        if (i == n) {
            if (!V->eof() || !R->eof()) die("poi: streams not fully consumed");
            return false;
        }
        uint64_t mono_next = total;
        if (i + 1 < n) {
            memcpy(nxt, R->take(32), 32);
            mono_next = rd32(nxt + 8);
        }
        const uint64_t mono = rd32(cur + 8);
        uint8_t rec[36];
        memcpy(rec, cur, 8);
        memcpy(rec + 16, cur + 12, 20);
        if (mono_next == mono) {
            wr32(rec + 8, NO);
            wr32(rec + 12, 0);
            emit(0, rec, 36);
            next_record();
            return true;
        }
        if (mono != V->tell() || mono_next < mono) die("poi %llu: block offset %llu is not the stream position", (unsigned long long)i,
                                                       (unsigned long long)mono);
        const uint64_t bsize = mono_next - mono;
        if (bsize < 10) die("poi %llu: bad block size", (unsigned long long)i);
        const uint8_t* h = V->need(10);
        const uint8_t e = h[0], flag = h[1];
        if (e > 3 || flag > 1) die("poi %llu: bad block header", (unsigned long long)i);
        const int w = e == 3 ? 4 : 2;
        if ((bsize - 10) % (2 * size_t(w))) die("poi %llu: bad block size", (unsigned long long)i);
        const uint64_t c2 = (bsize - 10) / (2 * size_t(w)), vc = c2 + flag;
        if (vt >= 0xFFFFFFFFull) die("poi_vertices offset overflow");
        wr32(rec + 8, uint32_t(vt));
        wr32(rec + 12, uint32_t(vc));
        const size_t osz = 10 + size_t(vc) * 2 * size_t(w);
        vt += osz;
        emit(0, rec, 36);
        if (bsize <= big_block()) {
            h = V->take(size_t(bsize));
            uint8_t* o = emit_reserve(1, osz);
            memcpy(o, h, 10);
            o[1] = 0;
            planes_dec(h + 10, o + 10, c2, w);
            if (flag) memcpy(o + 10 + c2 * 2 * size_t(w), o + 10, 2 * size_t(w));
            next_record();
            return true;
        }
        uint8_t hdr[10];
        memcpy(hdr, V->take(10), 10);
        hdr[1] = 0;
        emit(1, hdr, 10);
        const uint64_t base = V->tell();
        for (int j = 0; j < 2 * w; j++) pl.emplace_back(new Reader(vpath, PLANE_BUF, base + uint64_t(j) * c2, base + uint64_t(j + 1) * c2));
        big_c2 = c2;
        big_k = 0;
        big_w = w;
        big_flag = flag != 0;
        big_end = base + 2 * uint64_t(w) * c2;
        prev[0] = prev[1] = 0;
        if (c2 == 0) {  // (a block of only its closing vertex can't be: the flag needs 2 vertices)
            pl.clear();
            V->seek(big_end);
            next_record();
        }
        return true;
    }
};

// Ports as the decoder's. One pass: a polygon's R3 offset is where its block starts, a point's is
// where the next block will start, which is the B-open file's position at that record. A big block
// goes plane by plane as its vertices arrive, laid out as if its ring were closed (the closing
// vertex dropped, as on every POI polygon of the planet); if the last vertex turns out to differ
// from the first, the planes are moved to the open layout in the file.
struct PoiEnc : Encoder {
    std::unique_ptr<Writer> O, RW;
    std::vector<uint8_t> blk;
    uint64_t n = 0, vexp = 0;
    std::string opath;
    // big block in progress
    std::vector<std::unique_ptr<Writer>> pl;
    bool big = false;
    uint64_t big_vc = 0, big_k = 0, big_start = 0, big_size = 0;
    int big_w = 0;
    uint8_t big_hdr[10], big_first[8];
    uint32_t prev[2] = {0, 0};
    explicit PoiEnc(const std::string& od) : Encoder("poi", od, 2) {
        opath = od + "/poi_vertices.bopen";
        O.reset(new Writer(opath, WRITE_BUF, true));
        RW.reset(new Writer(od + "/poi_records.r3", 32 << 10));
    }
    void plane_vertex(const uint8_t* v, uint64_t k_at) {
        const int w = big_w;
        const uint64_t mask = wmask(w);
        (void)k_at;
        for (int comp = 0; comp < 2; comp++) {
            const uint32_t q = getw(v + size_t(comp * w), w);
            const uint32_t z = zzw(uint32_t((uint64_t(q) - prev[comp]) & mask), 8 * w);
            for (int b = 0; b < w; b++) pl[size_t(comp * w + b)]->u8(uint8_t(z >> (8 * b)));
            prev[comp] = q;
        }
    }
    // The block's last vertex is in: closed (dropped) or open (the planes move to stride vc).
    void finish_big(const uint8_t* last) {
        const int w = big_w;
        const size_t vs = 2 * size_t(w);
        const uint64_t vc = big_vc, base = big_start + 10;
        const bool closed = vc > 1 && memcmp(last, big_first, vs) == 0;
        for (auto& x : pl) x->finish();
        pl.clear();
        uint64_t c2 = vc - 1;
        if (!closed) {
            c2 = vc;
            std::vector<uint8_t> tmp(size_t(64) << 10);
            for (int j = 2 * w - 1; j >= 1; j--) {  // plane j: from base + j (vc - 1) to base + j vc
                const uint64_t src = base + uint64_t(j) * (vc - 1), dst = base + uint64_t(j) * vc;
                uint64_t left = vc - 1;
                while (left) {
                    const size_t c = size_t(std::min<uint64_t>(left, tmp.size()));
                    left -= c;
                    if (pread(O->fd, tmp.data(), c, off_t(src + left)) != ssize_t(c)) die("poi_vertices.bopen: reread");
                    if (pwrite(O->fd, tmp.data(), c, off_t(dst + left)) != ssize_t(c)) die("poi_vertices.bopen: write");
                }
            }
            const uint64_t mask = wmask(w);
            for (int comp = 0; comp < 2; comp++) {
                const uint32_t q = getw(last + size_t(comp * w), w);
                const uint32_t z = zzw(uint32_t((uint64_t(q) - prev[comp]) & mask), 8 * w);
                for (int b = 0; b < w; b++) {
                    const uint8_t byte = uint8_t(z >> (8 * b));
                    if (pwrite(O->fd, &byte, 1, off_t(base + uint64_t(comp * w + b) * vc + (vc - 1))) != 1) die("poi_vertices.bopen: write");
                }
            }
        }
        big_hdr[1] = closed ? 1 : 0;
        if (pwrite(O->fd, big_hdr, 10, off_t(big_start)) != 10) die("poi_vertices.bopen: write");
        O->skip_to(base + vs * c2);
        vexp += big_size;
        big = false;
    }
    void pump() override {
        ByteQueue &R = in[0], &V = in[1];
        for (;;) {
            if (big) {
                const size_t vs = 2 * size_t(big_w);
                while (V.size() >= vs && big_k + 1 < big_vc) {
                    if (big_k == 0) memcpy(big_first, V.data(), vs);
                    plane_vertex(V.data(), big_k);
                    V.pop(vs);
                    big_k++;
                }
                if (V.size() < vs) return;
                uint8_t last[8];
                memcpy(last, V.data(), vs);
                V.pop(vs);
                finish_big(last);
                continue;
            }
            if (R.size() < 36) return;
            const uint8_t* r = R.data();
            const uint32_t vo = rd32(r + 8), vc = rd32(r + 12);
            if ((vc == 0) != (vo == NO)) die("poi_records %llu: vertex_count %u with vertex_offset %u", (unsigned long long)n, vc, vo);
            size_t size = 0;
            int w = 2;
            if (vc) {
                if (V.size() < 1) return;
                const uint8_t e = V.data()[0];
                if (e > 3) die("poi_vertices: bad encoding %u", e);
                w = e == 3 ? 4 : 2;
                size = 10 + size_t(vc) * 2 * size_t(w);
                if (size <= big_block() ? V.size() < size : V.size() < 10 + 2 * size_t(w)) return;
            }
            if (O->tell() >= 0xFFFFFFFFull) die("poi_vertices.bopen offset overflow");
            uint8_t rec[32];
            memcpy(rec, r, 8);
            wr32(rec + 8, uint32_t(O->tell()));
            memcpy(rec + 12, r + 16, 20);
            RW->put(rec, 32);
            R.pop(36);
            n++;
            if (!vc) continue;
            if (vo != vexp) die("poi_records %llu: polygons not back to back in record order", (unsigned long long)(n - 1));
            const uint8_t* s = V.data();
            if (s[1] != 0) die("poi_vertices: non-zero pad byte at %llu", (unsigned long long)vexp);
            if (size <= big_block()) {
                const bool closed = vc > 1 && memcmp(s + 10, s + 10 + size_t(vc - 1) * 2 * size_t(w), 2 * size_t(w)) == 0;
                const uint64_t c2 = vc - (closed ? 1 : 0);
                blk.resize(10 + size_t(c2) * 2 * size_t(w));
                memcpy(blk.data(), s, 10);
                blk[1] = closed ? 1 : 0;
                planes_enc(s + 10, blk.data() + 10, c2, w);
                O->put(blk);
                V.pop(size);
                vexp += size;
                continue;
            }
            // big block: header kept for the end, plane writers laid out for vc - 1 vertices
            memcpy(big_hdr, s, 10);
            V.pop(10);
            big = true;
            big_vc = vc;
            big_k = 0;
            big_w = w;
            big_size = size;
            big_start = O->tell();
            O->flush();
            for (int j = 0; j < 2 * w; j++)
                pl.emplace_back(new Writer(O->fd, opath, big_start + 10 + uint64_t(j) * (vc - 1), PLANE_BUF));
            prev[0] = prev[1] = 0;
        }
    }
    void finish(Manifest& m) override {
        require_empty();
        if (big) die("poi_vertices: stream ends inside a block");
        m.set("poi.bopen_bytes", O->tell());
        m.set("poi.n", n);
        O->finish();
        RW->finish();
        O.reset();
        RW.reset();
    }
};
}  // namespace poly

}  // namespace gcs
