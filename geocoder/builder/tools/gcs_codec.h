// In-process incremental codecs for the low-memory client update. The same formats as gc
// (tools/gc.cpp) byte for byte, restructured so the patcher drives them:
//   Decoder: produces today's-format bytes on demand, one unit (way, row, record, block piece) per
//            step(), into one queue per today's file (a coupled family fills several queues).
//   Encoder: consumes today's-format bytes as the patcher writes them, one queue per today's file,
//            and processes every unit whose bytes are all there (pump()).
// Nothing is held beyond the buffers, the unit in flight and the queues' skew. A unit held whole is
// bounded by the format (a way <= 65535 nodes, an abs2 address footprint <= 69999 vertices; a
// footprint escaped raw, of any size, is copied through in pieces) or, for a polygon block, by
// big_block() (256 KiB): a larger POI block is decoded a piece at a time, and encoded through the
// new file (its vertices written as they come, then packed in place).
#pragma once
#include <cmath>
#include <functional>
#include <memory>
#include "gcs_io.h"
#include "place_files.h"
#include "poi_files.h"

namespace gcs {

// ------------------------------------------------------------------ framework
struct Decoder {
    std::vector<ByteQueue> out;
    std::vector<int> state;  // 0 not opened yet (kept), 1 open, 2 closed (dropped)
    std::vector<uint64_t> drop;  // per port: bytes its reader skips, dropped as they are decoded
    size_t dropping = 0;         // ports with bytes to drop
    std::string name;
    std::vector<std::string> files;  // per port: the today's-format file it decodes to
    std::vector<uint8_t> scratch;
    Decoder(const std::string& n, std::vector<std::string> port_files)
        : out(port_files.size()), state(port_files.size(), 0), drop(port_files.size(), 0), name(n), files(std::move(port_files)) {}
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
    // (its reader is behind: decoding further would only grow that queue). With nothing for the
    // port yet it decodes on, unless another port holds MAX_DECODER_SKEW (require_in_step).
    static constexpr size_t READ_AHEAD = size_t(8) << 10, SKEW_SOFT = size_t(64) << 10;
    bool others_full(int port) const {
        for (size_t p = 0; p < out.size(); p++)
            if (int(p) != port && state[p] != 2 && out[p].size() >= SKEW_SOFT) return true;
        return false;
    }
    // The reader of `reading` drives the decoder while the one of `held` has left that file's
    // bytes queued: refused.
    [[noreturn]] void runs_ahead(int reading, int held) const {
        die("Malformed merge sequence: %s runs ahead of %s", files[size_t(reading)].c_str(), files[size_t(held)].c_str());
    }
    // Before a step for the port's reader: no other open port holds more than MAX_DECODER_SKEW.
    void require_in_step(int port) const {
        for (size_t p = 0; p < out.size(); p++)
            if (int(p) != port && state[p] != 2 && out[p].size() > MAX_DECODER_SKEW) runs_ahead(port, int(p));
    }
    [[gnu::always_inline]] size_t read(int port, char* dst, size_t max) {
        ByteQueue& q = out[size_t(port)];
        const size_t want = std::min(max, READ_AHEAD);
        while (q.size() < want) {
            if (out.size() > 1 && others_full(port)) {
                if (q.size()) break;
                require_in_step(port);
            }
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
        const size_t pt = size_t(port);
        const uint8_t* b = static_cast<const uint8_t*>(p);
        while (n) {
            size_t k = in[pt].size() == 0 ? consume(port, b, n) : 0;
            if (!k) {
                k = std::min(n, ENCODER_PIECE);
                in[pt].push(b, k);
            }
            got[pt] += k;  // by the piece, not the write: unit_size() puts the queue's front at got - queued
            b += k;
            n -= k;
            if (in[pt].size()) pump();
        }
    }
    bool all_closed() const {
        for (int s : state)
            if (s != 2) return false;
        return true;
    }
    size_t peak(int port) const { return in[size_t(port)].peak; }
    // The bytes of the unit at the front of the port's queue that a count sizes, head + count * item
    // (checked_size.h): one past the end of the port's file (its size as the patch gives it) fails
    // as its first bytes come, instead of the queue taking the rest of the file while it waits.
    size_t unit_size(int port, uint64_t head, uint64_t count, uint64_t item) const {
        const size_t p = size_t(port);
        const uint64_t at = got[p] - in[p].size();
        const uint64_t left = expect[p] == ~0ull ? ~0ull : expect[p] - std::min(at, expect[p]);
        const auto n = checked_size(head, count, item, left);
        if (!n)
            die("%s: a unit of %llu + %llu x %llu bytes at %llu runs past the end of input %zu (%llu bytes left)", name.c_str(),
                (unsigned long long)head, (unsigned long long)count, (unsigned long long)item, (unsigned long long)at, p,
                (unsigned long long)left);
        return *n;
    }
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
// The widths an encoder writes: the first node in at most 4 bytes, the deltas in at most 32 bits.
static inline bool valid_code(uint16_t code) { return (code & 63) <= 32 && ((code >> 6) & 63) <= 32 && (code >> 13) <= 4; }
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
    Dec(const std::string& src, bool st, const Manifest& m)
        : Decoder(st ? "streets" : "interps", {names(st).ways, names(st).nodes}), street(st), nm(names(st)) {
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
            if (!c0->eof() || !c1->eof() || !c2->eof() || (c3 && !c3->eof()) || (c4 && !c4->eof()))
                die("%s: columns not fully consumed", nm.pre);
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
            if (!valid_code(code)) die("%s: way %llu has a bad code %#x", nm.pre, (unsigned long long)i, unsigned(code));
            // node_offset is a u32: the counts can sum past it while the total matches the manifest's.
            if (node_off > UINT32_MAX) die("%s: way %llu starts past node %u", nm.pre, (unsigned long long)i, UINT32_MAX);
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
    U24Dec(const std::string& src, const Manifest& m) : Decoder("way_parents", {"way_parents.bin"}) {
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

// <name>.u16 (index per value, 0xFFFF = NO_DATA) + <name>.dict (the distinct values, ascending;
// fewer than 0xFFFF of them).
struct DictDec : Decoder {
    uint64_t n, i = 0;
    std::vector<uint32_t> dict;
    std::unique_ptr<Reader> r;
    DictDec(const std::string& src, const std::string& nm, const Manifest& m) : Decoder(nm, {nm + ".bin"}) {
        n = m.num(nm + ".n");
        // The dictionary holds the manifest's count of values, fewer than the u16 index's 0xFFFF:
        // checked before it is read, so a malformed file can't make it any size.
        const uint64_t nd = m.num(nm + ".dict");
        if (nd >= 0xFFFF) die("%s: %llu dictionary values, past a u16 index", nm.c_str(), (unsigned long long)nd);
        {
            Reader d(src + "/" + nm + ".dict", 16 << 10);
            if (d.size() != 4 * nd) die("%s.dict size", nm.c_str());
            dict.reserve(size_t(nd));
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
                // Fewer than 0xFFFF values, as gc's encoder writes and both decoders take.
                if (dict.size() + 1 >= 0xFFFF) die("%s: too many distinct values for a u16 index", name.c_str());
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
                pread_all(fd, b.data(), want, at, p);
                for (size_t j = 0; j < want / 2; j++)
                    if (b[j] != 0xFFFF) b[j] = final_of[b[j]];
                pwrite_all(fd, b.data(), want, at, p);
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

// ------------------------------------------------------------------ address points + footprints
namespace addr {
static const double SCALE[3] = {1e-5, 1e-4, 1e-6};
// The most vertices (closing vertex included) an abs2 blob holds; a larger footprint is escaped.
static const uint32_t ABS2_MAX_VERTICES = 69999;
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

// Ports: 0 = addr_points, 1 = addr_vertices. A step: one record and its footprint, or a piece of an
// escaped footprint (copied through in READ_BUF pieces, so neither the reader nor a queue holds it).
struct Dec : Decoder {
    uint64_t N, i = 0, raw = 0;
    uint64_t escaped_left = 0;  // bytes of the escaped footprint in progress still to emit
    std::vector<std::unique_ptr<Reader>> rg;
    std::unique_ptr<Reader> A;
    Dec(const std::string& src, const Manifest& m) : Decoder("addr", {"addr_points.bin", "addr_vertices.bin"}) {
        N = m.num("addr.n");
        const std::string cp = src + "/addr_points.col";
        if (file_size(cp) != 24 * N) die("addr_points.col size");
        for (int k = 0; k < 15; k++) rg.emplace_back(new Reader(cp, region_buf(k), col_region(k, N), col_region(k + 1, N)));
        A.reset(new Reader(src + "/addr_vertices.abs2"));
    }
    uint64_t decoded_size(int port) const override { return port == 0 ? 28 * N : ~0ull; }
    void emit_escaped_piece() {
        const size_t k = size_t(std::min<uint64_t>(escaped_left, READ_BUF));
        emit(1, A->take(k), k);
        escaped_left -= k;
    }
    bool step() override {
        if (escaped_left) {
            emit_escaped_piece();
            return true;
        }
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
            if (!vcnt || vcnt > 0xFFFFFFFFull) die("addr %llu: bad count", (unsigned long long)i);
            const uint8_t enc = *A->need(1);
            const size_t size = A->unit_size(10, vcnt, enc == 3 ? 8 : 4);
            rec[6] = uint32_t(vcnt);
            emit(0, rec, 28);
            raw += size;
            i++;
            escaped_left = size;
            emit_escaped_piece();
            return true;
        }
        const int enc = tag & 3, wide = (tag >> 2) & 1, small = (tag >> 3) & 1, closed = (tag >> 4) & 1;
        if (enc > 2 || (tag & 0xE0)) die("addr %llu: bad tag", (unsigned long long)i);
        const uint64_t cnt = A->uv();
        if (cnt > ABS2_MAX_VERTICES - uint64_t(closed) || (closed && !cnt)) die("addr %llu: bad count", (unsigned long long)i);
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

// Ports: 0 = addr_points, 1 = addr_vertices; a record is encoded once its footprint is all in, or,
// when its footprint is escaped (which its first two bytes and vertex_count decide), once those are,
// the footprint then copied through as it comes.
// N (for addr_points.col's regions) comes from the expected size of addr_points.bin.
struct Enc : Encoder {
    uint64_t N = 0, i = 0, vexp = 0;
    uint64_t escaped_left = 0;  // bytes of the escaped footprint in progress still to copy
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
        for (;;) {
            if (escaped_left) {
                if (!V.size()) return;
                const size_t k = size_t(std::min<uint64_t>(escaped_left, V.size()));
                A->put(V.data(), k);
                V.pop(k);
                escaped_left -= k;
                if (escaped_left) return;
            }
            if (P.size() < 28) return;
            const uint8_t* r = P.data();
            const uint32_t lat = rd32(r), lng = rd32(r + 4), voff = rd32(r + 20), vcnt = rd32(r + 24);
            size_t size = 0;
            bool escaped = false;
            if (vcnt) {
                if (V.size() < 1) return;
                const uint8_t enc = V.data()[0];
                if (enc > 3) die("addr_vertices: unknown encoding %u", enc);
                size = unit_size(1, 10, vcnt, enc == 3 ? 8 : 4);
                if (V.size() < 2) return;
                escaped = enc == 3 || V.data()[1] != 0 || vcnt > ABS2_MAX_VERTICES;
                if (!escaped && V.size() < size) return;
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
            if (escaped) {
                A->u8(0x20);
                A->uv(vcnt);
                escaped_left = size;
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
                V.pop(size);
            }
            P.pop(28);
            i++;
        }
    }
    void finish(Manifest& m) override {
        require_empty();
        if (escaped_left) die("addr_vertices: stream ends inside a footprint");
        if (i != N) die("addr: %llu records, addr_points.bin's size gives %llu", (unsigned long long)i, (unsigned long long)N);
        for (auto& x : w) x->finish();
        w.clear();
        const int f = fd;
        fd = -1;
        if (close(f)) die("close addr_points.col");
        m.set("addr.n", N);
        m.set("addr.abs2_bytes", A->tell());
        A->finish();
        A.reset();
    }
};
}  // namespace addr

// ------------------------------------------------------------------ POI R2 + V2
// The POI files are R2 and V2 (poi_files.h) in every dir, not only an encoded set: these codecs
// turn them into today's logical records and vertex stream for the replay and back.
namespace poi {
// Vertices a step decodes or packs of a block too large to take whole: a multiple of 8, so each
// piece's bits start on a byte.
static const size_t VERTS_PER_STEP = 8192;

// The footer and dictionaries of an R2 file of size bytes.
inline poi_files::RowDecoder open_rows(const std::string& path, uint64_t size) {
    const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) die("open %s: %s", path.c_str(), strerror(errno));
    struct Close {
        int fd;
        ~Close() { close(fd); }
    } closer{fd};
    uint8_t f[poi_files::FOOTER_BYTES] = {};
    if (size >= poi_files::FOOTER_BYTES) pread_all(fd, f, poi_files::FOOTER_BYTES, size - poi_files::FOOTER_BYTES, path);
    const poi_files::Footer ft = poi_files::read_footer(f, size);
    std::vector<uint8_t> dicts(size_t(ft.dict_bytes()));
    pread_all(fd, dicts.data(), dicts.size(), ft.rows_end(), path);
    return poi_files::RowDecoder(ft, dicts.data());
}

// A dir's POI files' decoded sizes (Dec::decoded_size), from the R2 footer alone: 0 without the
// file.
inline uint64_t decoded_size(const std::string& dir, int port) {
    const std::string rp = dir + "/poi_records.bin";
    struct stat st;
    if (stat(rp.c_str(), &st) != 0) return 0;
    const uint64_t size = uint64_t(st.st_size);
    uint8_t f[poi_files::FOOTER_BYTES] = {};
    if (size >= poi_files::FOOTER_BYTES) {
        const int fd = open(rp.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) die("open %s: %s", rp.c_str(), strerror(errno));
        pread_all(fd, f, poi_files::FOOTER_BYTES, size - poi_files::FOOTER_BYTES, rp);
        close(fd);
    }
    const poi_files::Footer ft = poi_files::read_footer(f, size);
    return port == 0 ? poi_files::LOGICAL_STRIDE * ft.rows : ft.logical_vertex_bytes;
}

// Ports: 0 = poi_records (36 B), 1 = poi_vertices. A step: one record and its block, or a piece of a
// block too large to take whole. A dir without the two files decodes to none. vertices_only: the
// POI replay's vertex cursor (poi_replay.h), which reads port 1 alone (port 0 closed) and skips
// ahead: a block its pending skip covers whole passes undecoded.
struct Dec : Decoder {
    poi_files::RowDecoder rows;
    std::unique_ptr<Reader> R, V;
    uint64_t n = 0, i = 0, logical = 0, blocks_end = 0;
    bool vertices_only = false;
    // a block in pieces
    std::unique_ptr<poi_files::VertexDecoder> big;
    poi_files::Block blk;
    explicit Dec(const std::string& src) : Decoder("poi", {"poi_records.bin", "poi_vertices.bin"}) {
        const std::string rp = src + "/poi_records.bin", vp = src + "/poi_vertices.bin";
        struct stat st;
        const bool has_r = stat(rp.c_str(), &st) == 0, has_v = stat(vp.c_str(), &st) == 0;
        if (!has_r && !has_v) return;
        if (!has_r || !has_v) die("%s has one of poi_records.bin and poi_vertices.bin", src.c_str());
        rows = open_rows(rp, file_size(rp));
        n = rows.footer().rows;
        R.reset(new Reader(rp, 32 << 10, 0, rows.footer().rows_end()));
        V.reset(new Reader(vp));
        if (V->size() < poi_files::VERTEX_PAD) poi_files::malformed("poi_vertices.bin is shorter than its pad");
        blocks_end = V->size() - poi_files::VERTEX_PAD;
    }
    uint64_t decoded_size(int port) const override {
        if (!R) return 0;
        return port == 0 ? poi_files::LOGICAL_STRIDE * n : rows.footer().logical_vertex_bytes;
    }
    bool step() override {
        if (big) {
            const size_t vs = poi_files::vertex_bytes(blk.encoding);
            const uint64_t k = big->done(), c = std::min<uint64_t>(VERTS_PER_STEP, blk.m - k);
            const size_t nb = size_t((c * (blk.wl + blk.wg) + 7) / 8);
            const uint8_t* p = nb ? V->take(nb) : nullptr;
            const bool last = k + c == blk.m;
            uint8_t* o = emit_reserve(1, size_t(c + (last && blk.closed)) * vs);
            big->decode(p, nb, c, o);
            if (last) {
                big->finish(nb ? p[nb - 1] : 0, o + c * vs);
                big.reset();
            }
            return true;
        }
        if (i == n) {
            if (!R) return false;
            rows.finish();
            if (V->tell() != blocks_end) poi_files::malformed("poi_vertices.bin has bytes past its last block");
            const uint8_t* pad = V->take(poi_files::VERTEX_PAD);
            for (size_t k = 0; k < poi_files::VERTEX_PAD; k++)
                if (pad[k]) poi_files::malformed("poi_vertices.bin's pad is not zero");
            if (logical != rows.footer().logical_vertex_bytes)
                poi_files::malformed("the vertex stream is " + std::to_string(logical) + " bytes, the footer gives " +
                                     std::to_string(rows.footer().logical_vertex_bytes));
            R.reset();
            V.reset();
            return false;
        }
        uint8_t rec[poi_files::LOGICAL_STRIDE];
        bool polygon;
        const uint32_t voff = rows.decode(R->take(poi_files::ROW_BYTES), rec, polygon);
        if (voff != V->tell())
            poi_files::malformed("row " + std::to_string(i) + ": voff " + std::to_string(voff) + " where the blocks are at " +
                                 std::to_string(V->tell()));
        i++;
        if (!polygon) {
            emit(0, rec, poi_files::LOGICAL_STRIDE);
            return true;
        }
        const uint64_t left = blocks_end - std::min(V->tell(), blocks_end);
        const uint8_t* h = V->need(poi_files::MAX_BLOCK_HEADER, true);
        blk = poi_files::read_block_header(h, size_t(std::min<uint64_t>(left, poi_files::MAX_BLOCK_HEADER)), rd32(rec), rd32(rec + 4));
        if (blk.bytes() > left) poi_files::malformed("row " + std::to_string(i - 1) + ": a block past the end of poi_vertices.bin");
        if (blk.logical_bytes() > rows.footer().logical_vertex_bytes - logical) poi_files::past_vertex_stream(i - 1, rows.footer());
        if (logical >= NO) poi_files::malformed("a logical vertex stream of 4 GiB or more");
        if (vertices_only && drop[1] >= blk.logical_bytes()) {
            logical += blk.logical_bytes();
            V->seek(V->tell() + blk.bytes());
            drop[1] -= blk.logical_bytes();
            if (!drop[1]) dropping--;
            return true;
        }
        wr32(rec + poi_files::OFF_VERTEX_OFFSET, uint32_t(logical));
        wr32(rec + poi_files::OFF_VERTEX_COUNT, uint32_t(blk.vertex_count()));
        emit(0, rec, poi_files::LOGICAL_STRIDE);
        logical += blk.logical_bytes();
        V->skip(blk.header);
        const size_t bvs = poi_files::vertex_bytes(blk.encoding);
        if (blk.logical_bytes() <= big_block()) {
            const size_t pb = size_t(blk.payload());
            const uint8_t* p = pb ? V->take(pb) : nullptr;
            uint8_t* o = emit_reserve(1, size_t(blk.logical_bytes()));
            poi_files::logical_header(blk, o);
            poi_files::VertexDecoder vd(blk);
            vd.decode(p, pb, blk.m, o + poi_files::LOGICAL_HEADER);
            vd.finish(pb ? p[pb - 1] : 0, o + poi_files::LOGICAL_HEADER + blk.m * bvs);
            return true;
        }
        uint8_t hdr[poi_files::LOGICAL_HEADER];
        poi_files::logical_header(blk, hdr);
        emit(1, hdr, poi_files::LOGICAL_HEADER);
        big.reset(new poi_files::VertexDecoder(blk));
        return true;
    }
};

// Ports as the decoder's. One pass: a record's row as it comes (its block starts where the V2 file
// is), its block once the block is all in. A block too large to take whole goes to the V2 file as
// it arrives, its vertices as they are VERTEX_RAW bytes past the block's start (room for the
// longest header), then is packed in place, front to back: a vertex's bits never take more bytes
// than its raw bytes, so the packed bytes never reach the raw ones still to be read.
struct Enc : Encoder {
    std::unique_ptr<Writer> O, RW;
    poi_files::RowEncoder rows;
    std::vector<uint8_t> blk;
    uint64_t logical = 0;
    std::string opath;
    // a block in pieces
    bool big = false;
    poi_files::Block bb;
    uint64_t big_vc = 0, big_k = 0, big_start = 0, big_size = 0;
    uint32_t big_lat = 0, big_lng = 0, max_l = 0, max_g = 0;
    uint8_t first[8], last[8];
    std::unique_ptr<Writer> raw;
    explicit Enc(const std::string& od) : Encoder("poi", od, 2) {
        opath = od + "/poi_vertices.bin";
        O.reset(new Writer(opath, WRITE_BUF, true));
        RW.reset(new Writer(od + "/poi_records.bin", 32 << 10));
    }
    void finish_big() {
        const size_t vs = poi_files::vertex_bytes(bb.encoding);
        raw->finish();
        raw.reset();
        bb.closed = big_vc >= 2 && memcmp(first, last, vs) == 0;
        bb.m = big_vc - bb.closed;
        poi_files::set_widths(bb, max_l, max_g);
        uint8_t h[poi_files::MAX_BLOCK_HEADER];
        const size_t hl = poi_files::write_block_header(bb, big_lat, big_lng, h);
        O->skip_to(big_start);
        O->put(h, hl);
        const uint64_t raw_at = big_start + poi_files::MAX_BLOCK_HEADER;
        std::vector<uint8_t> tmp(VERTS_PER_STEP * vs);
        poi_files::BitPacker pack([&](uint8_t c) { O->u8(c); });
        for (uint64_t k = 0; k < bb.m;) {
            const uint64_t c = std::min<uint64_t>(VERTS_PER_STEP, bb.m - k);
            pread_all(O->fd, tmp.data(), size_t(c * vs), raw_at + k * vs, opath);
            for (uint64_t j = 0; j < c; j++) {
                pack.add(poi_files::q_at(&tmp[j * vs], bb.encoding, 0), bb.wl);
                pack.add(poi_files::q_at(&tmp[j * vs], bb.encoding, 1), bb.wg);
            }
            k += c;
        }
        pack.finish();
        logical += big_size;
        big = false;
    }
    // A record whose block is an old one as it is (the POI replay copies the blocks a patch leaves
    // whole): put_coded_row(rec) with nothing queued, the block's bytes through put_coded_bytes,
    // then end_coded(its logical bytes).
    void put_coded_row(const uint8_t* rec) {
        if (big || in[0].size() || in[1].size()) die("poi: a copied block among queued records");
        const uint32_t vo = rd32(rec + poi_files::OFF_VERTEX_OFFSET);
        if (vo != logical) die("poi_records %llu: polygons not back to back in record order", (unsigned long long)rows.rows());
        uint8_t row[poi_files::ROW_BYTES];
        rows.encode(rec, O->tell(), row);
        RW->put(row, poi_files::ROW_BYTES);
    }
    void put_coded_bytes(const uint8_t* p, size_t n) { O->put(p, n); }
    void end_coded(uint64_t logical_bytes) { logical += logical_bytes; }
    void pump() override {
        ByteQueue &R = in[0], &V = in[1];
        for (;;) {
            if (big) {
                const size_t vs = poi_files::vertex_bytes(bb.encoding);
                while (V.size() >= vs && big_k < big_vc) {
                    const uint8_t* v = V.data();
                    if (big_k == 0) memcpy(first, v, vs);
                    memcpy(last, v, vs);
                    max_l = std::max(max_l, poi_files::q_at(v, bb.encoding, 0));
                    max_g = std::max(max_g, poi_files::q_at(v, bb.encoding, 1));
                    raw->put(v, vs);
                    V.pop(vs);
                    big_k++;
                }
                if (big_k < big_vc) return;
                finish_big();
                continue;
            }
            if (R.size() < poi_files::LOGICAL_STRIDE) return;
            const uint8_t* r = R.data();
            const uint32_t vo = rd32(r + poi_files::OFF_VERTEX_OFFSET), vc = rd32(r + poi_files::OFF_VERTEX_COUNT);
            size_t size = 0;
            uint8_t e = 0;
            if (vc && vo != NO) {
                if (V.size() < 1) return;
                e = V.data()[0];
                if (e > 3) die("poi_vertices: bad encoding %u", e);
                size = unit_size(1, poi_files::LOGICAL_HEADER, vc, poi_files::vertex_bytes(e));
                if (size <= big_block() ? V.size() < size : V.size() < poi_files::LOGICAL_HEADER) return;
            }
            uint8_t row[poi_files::ROW_BYTES];
            rows.encode(r, O->tell(), row);
            RW->put(row, poi_files::ROW_BYTES);
            const uint32_t lat = rd32(r), lng = rd32(r + 4);
            R.pop(poi_files::LOGICAL_STRIDE);
            if (!vc) continue;
            if (vo != logical) die("poi_records %llu: polygons not back to back in record order", (unsigned long long)(rows.rows() - 1));
            const uint8_t* s = V.data();
            if (size <= big_block()) {
                blk.clear();
                poi_files::encode_block(s, vc, lat, lng, blk);
                O->put(blk);
                V.pop(size);
                logical += size;
                continue;
            }
            if (s[1] != 0) die("poi_vertices: non-zero pad byte at %llu", (unsigned long long)logical);
            bb = poi_files::Block();
            bb.encoding = e;
            bb.bmin_lat = rd32(s + 2);
            bb.bmin_lng = rd32(s + 6);
            V.pop(poi_files::LOGICAL_HEADER);
            big = true;
            big_vc = vc;
            big_k = 0;
            big_size = size;
            big_lat = lat;
            big_lng = lng;
            max_l = max_g = 0;
            O->flush();
            big_start = O->tell();
            raw.reset(new Writer(O->fd, opath, big_start + poi_files::MAX_BLOCK_HEADER, PLANE_BUF));
        }
    }
    void finish(Manifest&) override {
        require_empty();
        if (big) die("poi_vertices: stream ends inside a block");
        const std::vector<uint8_t> t = rows.tail(logical);
        RW->put(t);
        const uint8_t pad[poi_files::VERTEX_PAD] = {};
        O->put(pad, poi_files::VERTEX_PAD);
        O->flush();
        // A block packed in place leaves raw bytes past the end when it was the last.
        if (ftruncate(O->fd, off_t(O->tell()))) die("truncate %s: %s", opath.c_str(), strerror(errno));
        O->finish();
        RW->finish();
        O.reset();
        RW.reset();
    }
};
}  // namespace poi

// ------------------------------------------------------------------ places (place_files.h)
namespace place {
namespace pf = place_files;
// The codec's buffers: the place files stream front to back in units of a block (under 1 KiB) or a run
// of hidden rows, so a few KiB each do; the codec holds about 56 KiB.
static const size_t SMALL_BUF = size_t(4) << 10, BLOCKS_BUF = size_t(16) << 10;

// A dir's place files' decoded size, LOGICAL_STRIDE a node, from place_nodes.bin's footer and
// place_hidden.bin's size alone: 0 without place_nodes.bin.
inline uint64_t decoded_size(const std::string& dir) {
    const std::string np = dir + "/" + pf::NODES_FILE, hp = dir + "/" + pf::HIDDEN_FILE;
    struct stat st;
    if (stat(np.c_str(), &st) != 0) return 0;
    const uint64_t size = uint64_t(st.st_size);
    uint8_t f[pf::FOOTER_BYTES] = {};
    if (size >= pf::FOOTER_BYTES) {
        const int fd = open(np.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) die("open %s: %s", np.c_str(), strerror(errno));
        pread_all(fd, f, pf::FOOTER_BYTES, size - pf::FOOTER_BYTES, np);
        close(fd);
    }
    const pf::Footer ft = pf::read_footer(f, size);
    const uint64_t hidden = stat(hp.c_str(), &st) == 0 ? uint64_t(st.st_size) / pf::HIDDEN_ROW_BYTES : 0;
    return pf::LOGICAL_STRIDE * (uint64_t(ft.rows) + hidden);
}

// Port 0 = place_nodes.bin's logical stream (pf::LogicalPlace). A step: a block of visible rows, or
// a run of hidden rows. A dir without the three files decodes to none.
struct Dec : Decoder {
    static constexpr size_t HIDDEN_PER_STEP = SMALL_BUF / pf::HIDDEN_ROW_BYTES;
    std::unique_ptr<Reader> C, N, T, H;  // place_cells.bin, place_nodes.bin's blocks and its table, place_hidden.bin
    pf::Footer f;
    std::unique_ptr<pf::CellCursor> cells;
    size_t b = 0;
    uint64_t at = 0;
    explicit Dec(const std::string& src) : Decoder("places", {pf::NODES_FILE}) {
        const std::string cp = src + "/" + pf::CELLS_FILE, np = src + "/" + pf::NODES_FILE, hp = src + "/" + pf::HIDDEN_FILE;
        struct stat st;
        const int has = (stat(cp.c_str(), &st) == 0) + (stat(np.c_str(), &st) == 0) + (stat(hp.c_str(), &st) == 0);
        if (!has) return;
        if (has != 3) die("%s has some of the place files", src.c_str());
        const uint64_t size = file_size(np), cs = file_size(cp), hs = file_size(hp);
        uint8_t ft[pf::FOOTER_BYTES] = {};
        if (size >= pf::FOOTER_BYTES) {
            Reader footer(np, pf::FOOTER_BYTES, size - pf::FOOTER_BYTES);
            memcpy(ft, footer.take(pf::FOOTER_BYTES), pf::FOOTER_BYTES);
        }
        f = pf::read_footer(ft, size);
        if (cs % pf::CELL_ROW_BYTES) pf::malformed("place_cells.bin is not whole rows");
        if (hs % pf::HIDDEN_ROW_BYTES) pf::malformed("place_hidden.bin is not whole rows");
        cells.reset(new pf::CellCursor(f.rows, cs / pf::CELL_ROW_BYTES));
        C.reset(new Reader(cp, SMALL_BUF));
        N.reset(new Reader(np, BLOCKS_BUF, 0, f.table_at));
        T.reset(new Reader(np, SMALL_BUF, f.table_at, size - pf::FOOTER_BYTES));
        H.reset(new Reader(hp, SMALL_BUF));
    }
    uint64_t decoded_size(int) const override {
        if (!N) return 0;
        return pf::LOGICAL_STRIDE * (uint64_t(f.rows) + H->size() / pf::HIDDEN_ROW_BYTES);
    }
    bool step() override {
        if (!N) return false;
        if (b < f.blocks) {
            const pf::BlockEntry e = pf::read_entry(T->take(pf::TABLE_ENTRY_BYTES));
            const size_t rows = f.rows_of(b), bytes = e.bytes(rows);
            if (e.offset != at) pf::malformed("block " + std::to_string(b) + " is not where the blocks before it end");
            if (at + bytes > f.table_at) pf::malformed("block " + std::to_string(b) + " runs into the block table");
            const uint8_t* p = N->take(bytes);
            uint8_t* o = emit_reserve(0, rows * pf::LOGICAL_STRIDE);
            pf::decode_block(
                b, e, rows, p, [&](uint32_t row) { return cells->key_of(row, [&] { return C->take(pf::CELL_ROW_BYTES); }); },
                [&](const pf::LogicalPlace& pl) {
                    memcpy(o, &pl, pf::LOGICAL_STRIDE);
                    o += pf::LOGICAL_STRIDE;
                });
            at += bytes;
            b++;
            return true;
        }
        if (!H->eof()) {
            const size_t n = size_t(std::min<uint64_t>(H->left() / pf::HIDDEN_ROW_BYTES, HIDDEN_PER_STEP));
            const uint8_t* p = H->take(n * pf::HIDDEN_ROW_BYTES);
            uint8_t* o = emit_reserve(0, n * pf::LOGICAL_STRIDE);
            for (size_t k = 0; k < n; k++) {
                const pf::LogicalPlace pl = pf::hidden_place(p + k * pf::HIDDEN_ROW_BYTES);
                memcpy(o + k * pf::LOGICAL_STRIDE, &pl, pf::LOGICAL_STRIDE);
            }
            return true;
        }
        if (at != f.table_at) pf::malformed("place_nodes.bin has bytes between its blocks and its table");
        cells->finish();
        C.reset();
        N.reset();
        T.reset();
        H.reset();
        return false;
    }
};

// Port 0 = the logical stream in; the three files out. The blocks go to place_nodes.bin as they are
// packed and their table entries to a scratch file beside it, which finish appends to it with the
// footer: nothing grows with the set but the files.
struct Enc : Encoder {
    using Sink = std::function<void(const uint8_t*, size_t)>;
    std::unique_ptr<Writer> CW, NW, HW, TW;
    std::string tpath;
    pf::Encoder<Sink> enc;
    explicit Enc(const std::string& od)
        : Encoder("places", od, 1),
          CW(new Writer(od + "/" + pf::CELLS_FILE, SMALL_BUF)),
          NW(new Writer(od + "/" + pf::NODES_FILE, BLOCKS_BUF)),
          HW(new Writer(od + "/" + pf::HIDDEN_FILE, SMALL_BUF)),
          TW(new Writer(od + "/.place_nodes.table", SMALL_BUF, true)),
          tpath(od + "/.place_nodes.table"),
          enc([this](const uint8_t* p, size_t n) { CW->put(p, n); }, [this](const uint8_t* p, size_t n) { NW->put(p, n); },
              [this](const uint8_t* p, size_t n) { HW->put(p, n); }) {}
    void spill() {
        if (enc.table().empty()) return;
        for (const auto& e : enc.table()) TW->put(e.data(), e.size());
        enc.clear_table();
    }
    void add(const uint8_t* row) {
        pf::LogicalPlace p;
        memcpy(&p, row, pf::LOGICAL_STRIDE);
        enc.add(p);
        spill();
    }
    // Every byte straight from the writer (nothing queued): a row a write cuts waits in carry.
    uint8_t carry[pf::LOGICAL_STRIDE];
    size_t carried = 0;
    size_t consume(int, const uint8_t* p, size_t n) override {
        size_t used = 0;
        if (carried) {
            used = std::min(pf::LOGICAL_STRIDE - carried, n);
            memcpy(carry + carried, p, used);
            carried += used;
            if (carried < pf::LOGICAL_STRIDE) return n;
            add(carry);
            carried = 0;
        }
        for (; n - used >= pf::LOGICAL_STRIDE; used += pf::LOGICAL_STRIDE) add(p + used);
        memcpy(carry, p + used, n - used);
        carried = n - used;
        return n;
    }
    void pump() override {
        ByteQueue& q = in[0];
        while (q.size() >= pf::LOGICAL_STRIDE) {
            add(q.data());
            q.pop(pf::LOGICAL_STRIDE);
        }
    }
    void finish(Manifest&) override {
        require_empty();
        if (carried) die("places: the stream ends %zu bytes into a row", carried);
        enc.finish();
        spill();
        TW->flush();
        std::vector<uint8_t> buf(BLOCKS_BUF);
        for (uint64_t off = 0, n = TW->tell(); off < n;) {
            const size_t k = size_t(std::min<uint64_t>(buf.size(), n - off));
            pread_all(TW->fd, buf.data(), k, off, tpath);
            NW->put(buf.data(), k);
            off += k;
        }
        const auto footer = enc.footer();
        NW->put(footer.data(), footer.size());
        TW->finish();
        if (unlink(tpath.c_str())) die("unlink %s: %s", tpath.c_str(), strerror(errno));
        CW->finish();
        NW->finish();
        HW->finish();
        TW.reset();
        CW.reset();
        NW.reset();
        HW.reset();
    }
};
}  // namespace place

}  // namespace gcs
