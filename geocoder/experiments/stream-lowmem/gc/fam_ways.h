// Street and interpolation ways: the streets track's "packed ways" (max/streets/waypack.cpp), G = 16,
// with the per-way header split into columns (max/verify/scripts/split_cols.py). The way order is the
// input's (the sort step's spatial order); nothing is re-sorted.
//
//   <p>.groups  per 16 ways: nodes2 byte offset u32 | anchor lat f32 bits | anchor lng f32 bits
//               (anchor = first node of the group's first way with nodes; 0,0 when none)
//   <p>.nodes2  per way with nodes: first node zigzag(ulp(v) - ulp(anchor)) at wf bytes, lat then lng;
//               then (cnt-1) lat values at wl bits and (cnt-1) lng values at wg bits, one LSB-first
//               bit stream byte-aligned at the block end. Value 1 is the first-order delta, 2.. are
//               second-order deltas of the integer key (hybrid fixed-point key when every value of the
//               way round-trips, else the ulp key).
//   street:  street.cnt u16 | street.code u16 | street.name u32         (columns, one value per way)
//   interp:  interp.cntcode (cnt u16, code u16) | interp.street u32 | interp.start u32 |
//            interp.span u32 (= end - start mod 2^32) | interp.interp u8
//   code = wl | wg << 6 | hybrid << 12 | wf << 13. Block size = 2 wf + ceil((cnt-1)(wl+wg)/8), so a
//   way's block is its group offset plus the sizes of the earlier ways of the group.
// Dropped from today's records: node_offset (the prefix sum of counts; 0 for a way without nodes) and
// the zero padding; the encoder refuses input where they are anything else.
#pragma once
#include <cmath>
#include "io.h"

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

struct Names {
    const char *ways, *nodes, *pre;
    size_t stride;
};
static Names names(bool street) {
    return street ? Names{"street_ways.bin", "street_nodes.bin", "street", 12}
                  : Names{"interp_ways.bin", "interp_nodes.bin", "interp", 24};
}

struct Stats { uint64_t ways = 0, nodes = 0, hyb = 0, wf[5] = {0, 0, 0, 0, 0}; };

// Encodes one way's nodes (bits: 2 u32 per node) into blk (appended). Returns the code.
static uint16_t encode_way(const uint32_t* bits, size_t cnt, uint32_t al, uint32_t ag, std::vector<uint8_t>& blk,
                           std::vector<uint32_t>& kl, std::vector<uint32_t>& kg, Stats& st) {
    const uint32_t zfl = zz(ulp_key(bits[0]) - ulp_key(al)), zfg = zz(ulp_key(bits[1]) - ulp_key(ag));
    const int wf = std::max(bytelen(zfl), bytelen(zfg));
    st.wf[wf]++;
    for (int b = 0; b < wf; b++) blk.push_back(uint8_t(zfl >> (8 * b)));
    for (int b = 0; b < wf; b++) blk.push_back(uint8_t(zfg >> (8 * b)));
    kl.resize(cnt);
    kg.resize(cnt);
    bool hyb = true;
    for (size_t j = 0; j < cnt && hyb; j++) hyb = hyb_key(bits[2 * j], kl[j]) && hyb_key(bits[2 * j + 1], kg[j]);
    if (!hyb)
        for (size_t j = 0; j < cnt; j++) { kl[j] = ulp_key(bits[2 * j]); kg[j] = ulp_key(bits[2 * j + 1]); }
    st.hyb += hyb;
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

// Streaming (--files with one of the two files): the patcher writes the ways file whole, then the
// nodes file, so the encoder runs as two passes. Pass 1 (ways) writes the per-way columns that need
// no nodes (street: cnt, name; interp: street, start, span, interp, plus cnt in <p>.cnt.tmp); pass 2
// (nodes) reads the counts back and writes groups, nodes2 and the code column (interp: cntcode). The
// files are byte for byte those of the one-pass encoder.
inline void encode_ways_pass(const std::string& src, const std::string& out, bool street, Manifest& m) {
    const Names nm = names(street);
    const size_t S = nm.stride;
    Reader W(src + "/" + nm.ways);
    if (W.size() % S) die("%s: size not a multiple of the record size", nm.ways);
    const uint64_t nw = W.size() / S;
    const std::string p = out + "/" + nm.pre;
    Writer cnt(p + (street ? ".cnt" : ".cnt.tmp"));
    Writer* c1 = street ? nullptr : new Writer(p + ".street");
    Writer c2(p + (street ? ".name" : ".start"), 1 << 20);
    Writer* c3 = street ? nullptr : new Writer(p + ".span");
    Writer* c4 = street ? nullptr : new Writer(p + ".interp", 1 << 20);
    uint64_t expect = 0;
    for (uint64_t i = 0; i < nw; i++) {
        const uint8_t* r = W.take(S);
        const uint32_t off = rd32(r);
        const uint16_t c = rd16(r + 4);
        if (rd16(r + 6) || (!street && (r[21] | r[22] | r[23])))
            die("%s: way %llu has non-zero padding", nm.ways, (unsigned long long)i);
        if (c ? off != expect : off != 0)
            die("%s: way %llu node_offset %u is not the prefix sum %llu", nm.ways, (unsigned long long)i, off,
                (unsigned long long)expect);
        expect += c;
        cnt.u16(c);
        if (street) {
            c2.put(r + 8, 4);
        } else {
            c1->put(r + 8, 4);
            c2.put(r + 12, 4);
            c3->u32(rd32(r + 16) - rd32(r + 12));
            c4->u8(r[20]);
        }
    }
    delete c1;
    delete c3;
    delete c4;
    m.set(std::string(nm.pre) + ".ways", nw);
    m.set(std::string(nm.pre) + ".G", G);
    fprintf(stderr, "  %s pass 1 (ways): %llu ways, %llu nodes by count\n", nm.pre, (unsigned long long)nw, (unsigned long long)expect);
}

inline void encode_nodes_pass(const std::string& src, const std::string& out, bool street, Manifest& m) {
    const Names nm = names(street);
    const std::string p = out + "/" + nm.pre;
    const std::string cnt_path = p + (street ? ".cnt" : ".cnt.tmp");
    Reader N(src + "/" + nm.nodes), C(cnt_path, 1 << 20);
    if (N.size() % 8) die("%s: size not a multiple of the record size", nm.nodes);
    const uint64_t nw = C.size() / 2, nn = N.size() / 8;
    Writer groups(p + ".groups"), nodes2(p + ".nodes2");
    Writer code_col(p + (street ? ".code" : ".cntcode"));
    std::vector<uint16_t> cn(G);
    std::vector<uint32_t> nb, kl, kg;
    std::vector<uint8_t> blk;
    Stats st;
    uint64_t expect = 0;
    for (uint64_t w0 = 0; w0 < nw; w0 += G) {
        const size_t gn = size_t(std::min<uint64_t>(G, nw - w0));
        for (size_t j = 0; j < gn; j++) cn[j] = C.u16();
        uint32_t al = 0, ag = 0;
        for (size_t j = 0; j < gn; j++)
            if (cn[j]) {
                const uint8_t* q = N.need(8);
                al = rd32(q);
                ag = rd32(q + 4);
                break;
            }
        if (nodes2.tell() > 0xFFFFFFFFull) die("%s: nodes2 offset overflow", nm.pre);
        groups.u32(uint32_t(nodes2.tell()));
        groups.u32(al);
        groups.u32(ag);
        for (size_t j = 0; j < gn; j++) {
            const uint16_t c = cn[j];
            uint16_t code = 0;
            if (c) {
                nb.resize(2 * size_t(c));
                memcpy(nb.data(), N.take(8 * size_t(c)), 8 * size_t(c));
                blk.clear();
                code = encode_way(nb.data(), c, al, ag, blk, kl, kg, st);
                nodes2.put(blk);
                expect += c;
            }
            st.ways++;
            st.nodes += c;
            if (street) {
                code_col.u16(code);
            } else {
                code_col.u16(c);
                code_col.u16(code);
            }
        }
    }
    if (!N.eof() || expect != nn) die("%s: nodes not exactly covered by the ways", nm.nodes);
    if (!street) unlink(cnt_path.c_str());
    m.set(std::string(nm.pre) + ".nodes", nn);
    m.set(std::string(nm.pre) + ".nodes2_bytes", nodes2.tell());
    fprintf(stderr, "  %s pass 2 (nodes): ways %llu nodes %llu hybrid %llu nodes2 %llu B\n", nm.pre, (unsigned long long)st.ways,
            (unsigned long long)st.nodes, (unsigned long long)st.hyb, (unsigned long long)nodes2.tell());
}

inline void encode(const std::string& src, const std::string& out, bool street, Manifest& m) {
    const Names nm = names(street);
    if (want(nm.ways) != want(nm.nodes)) {
        if (want(nm.ways)) encode_ways_pass(src, out, street, m);
        else encode_nodes_pass(src, out, street, m);
        return;
    }
    const size_t S = nm.stride;
    Reader W(src + "/" + nm.ways), N(src + "/" + nm.nodes);
    if (W.size() % S || N.size() % 8) die("%s: size not a multiple of the record size", nm.ways);
    const uint64_t nw = W.size() / S, nn = N.size() / 8;
    const std::string p = out + "/" + nm.pre;
    Writer groups(p + ".groups"), nodes2(p + ".nodes2");
    Writer c0(p + (street ? ".cnt" : ".cntcode")), c1(p + (street ? ".code" : ".street"));
    Writer c2(p + (street ? ".name" : ".start"), 1 << 20);
    Writer* c3 = street ? nullptr : new Writer(p + ".span");
    Writer* c4 = street ? nullptr : new Writer(p + ".interp", 1 << 20);
    std::vector<uint8_t> grp(G * S), blk;
    std::vector<uint32_t> nb, kl, kg;
    Stats st;
    uint64_t expect = 0;
    for (uint64_t w0 = 0; w0 < nw; w0 += G) {
        const size_t gn = size_t(std::min<uint64_t>(G, nw - w0));
        memcpy(grp.data(), W.take(gn * S), gn * S);
        uint32_t al = 0, ag = 0;
        for (size_t j = 0; j < gn; j++)
            if (rd16(&grp[j * S + 4])) {
                const uint8_t* q = N.need(8);
                al = rd32(q);
                ag = rd32(q + 4);
                break;
            }
        if (nodes2.tell() > 0xFFFFFFFFull) die("%s: nodes2 offset overflow", nm.pre);
        groups.u32(uint32_t(nodes2.tell()));
        groups.u32(al);
        groups.u32(ag);
        for (size_t j = 0; j < gn; j++) {
            const uint8_t* r = &grp[j * S];
            const uint32_t off = rd32(r);
            const uint16_t cnt = rd16(r + 4);
            if (rd16(r + 6) || (!street && (r[21] | r[22] | r[23])))
                die("%s: way %llu has non-zero padding", nm.ways, (unsigned long long)(w0 + j));
            if (cnt ? off != expect : off != 0)
                die("%s: way %llu node_offset %u is not the prefix sum %llu", nm.ways, (unsigned long long)(w0 + j), off,
                    (unsigned long long)expect);
            uint16_t code = 0;
            if (cnt) {
                nb.resize(2 * size_t(cnt));
                memcpy(nb.data(), N.take(8 * size_t(cnt)), 8 * size_t(cnt));
                blk.clear();
                code = encode_way(nb.data(), cnt, al, ag, blk, kl, kg, st);
                nodes2.put(blk);
                expect += cnt;
            }
            st.ways++;
            st.nodes += cnt;
            if (street) {
                c0.u16(cnt);
                c1.u16(code);
                c2.put(r + 8, 4);
            } else {
                c0.u16(cnt);
                c0.u16(code);
                c1.put(r + 8, 4);
                c2.put(r + 12, 4);
                c3->u32(rd32(r + 16) - rd32(r + 12));
                c4->u8(r[20]);
            }
        }
    }
    if (!N.eof() || expect != nn) die("%s: nodes not exactly covered by the ways", nm.nodes);
    delete c3;
    delete c4;
    const std::string k = nm.pre;
    m.set(k + ".ways", nw);
    m.set(k + ".nodes", nn);
    m.set(k + ".G", G);
    m.set(k + ".nodes2_bytes", nodes2.tell());
    fprintf(stderr, "  %s: ways %llu nodes %llu hybrid %llu wf 0/1/2/3/4 = %llu/%llu/%llu/%llu/%llu nodes2 %llu B\n", nm.pre,
            (unsigned long long)st.ways, (unsigned long long)st.nodes, (unsigned long long)st.hyb,
            (unsigned long long)st.wf[0], (unsigned long long)st.wf[1], (unsigned long long)st.wf[2],
            (unsigned long long)st.wf[3], (unsigned long long)st.wf[4], (unsigned long long)nodes2.tell());
}

// Streaming: the ways file alone (columns only; node_offset is the prefix sum of the counts) or the
// nodes file alone (groups, nodes2 and the count / code columns), each for its own FIFO.
inline void decode_ways_only(const std::string& src, const std::string& out, bool street, const Manifest& m) {
    const Names nm = names(street);
    const std::string k = nm.pre, p = src + "/" + nm.pre;
    const uint64_t nw = m.num(k + ".ways"), nn = m.num(k + ".nodes");
    Reader c0(p + (street ? ".cnt" : ".cntcode"), 1 << 20);
    Reader* c1 = street ? nullptr : new Reader(p + ".street", 1 << 20);
    Reader c2(p + (street ? ".name" : ".start"), 1 << 20);
    Reader* c3 = street ? nullptr : new Reader(p + ".span", 1 << 20);
    Reader* c4 = street ? nullptr : new Reader(p + ".interp", 1 << 20);
    Writer W(out + "/" + nm.ways);
    uint8_t rec[24];
    uint64_t node_off = 0;
    for (uint64_t i = 0; i < nw; i++) {
        memset(rec, 0, sizeof rec);
        uint16_t cnt;
        if (street) {
            cnt = c0.u16();
            memcpy(rec + 8, c2.take(4), 4);
        } else {
            cnt = c0.u16();
            c0.u16();  // code
            memcpy(rec + 8, c1->take(4), 4);
            const uint32_t start = c2.u32();
            wr32(rec + 12, start);
            wr32(rec + 16, start + c3->u32());
            rec[20] = c4->u8();
        }
        wr16(rec + 4, cnt);
        if (cnt) {
            wr32(rec, uint32_t(node_off));
            node_off += cnt;
        }
        W.put(rec, nm.stride);
    }
    if (node_off != nn || !c0.eof() || (c1 && !c1->eof()) || !c2.eof() || (c3 && !c3->eof()) || (c4 && !c4->eof()))
        die("%s: columns not fully consumed", nm.pre);
    delete c1;
    delete c3;
    delete c4;
}

inline void decode(const std::string& src, const std::string& out, bool street, const Manifest& m) {
    const Names nm = names(street);
    if (want(nm.ways) != want(nm.nodes) && want(nm.ways)) {
        decode_ways_only(src, out, street, m);
        return;
    }
    const bool write_ways = want(nm.ways);  // false: the nodes file alone
    const std::string k = nm.pre, p = src + "/" + nm.pre;
    const uint64_t nw = m.num(k + ".ways"), nn = m.num(k + ".nodes");
    if (m.num(k + ".G") != G) die("unsupported G");
    Reader groups(p + ".groups", 1 << 20), nodes2(p + ".nodes2");
    Reader c0(p + (street ? ".cnt" : ".cntcode"), 1 << 20);
    Reader* c1 = street || write_ways ? new Reader(p + (street ? ".code" : ".street"), 1 << 20) : nullptr;
    Reader* c2 = write_ways ? new Reader(p + (street ? ".name" : ".start"), 1 << 20) : nullptr;
    Reader* c3 = !street && write_ways ? new Reader(p + ".span", 1 << 20) : nullptr;
    Reader* c4 = !street && write_ways ? new Reader(p + ".interp", 1 << 20) : nullptr;
    if (groups.size() != 12 * ((nw + G - 1) / G)) die("%s.groups size", nm.pre);
    Writer* W = write_ways ? new Writer(out + "/" + nm.ways) : nullptr;
    Writer N(out + "/" + nm.nodes);
    std::vector<uint32_t> o(2 * 65536);
    uint8_t rec[24];
    uint64_t node_off = 0;
    uint32_t al = 0, ag = 0;
    for (uint64_t i = 0; i < nw; i++) {
        if (i % G == 0) {
            const uint32_t goff = groups.u32();
            al = groups.u32();
            ag = groups.u32();
            if (goff != nodes2.tell()) die("%s: group %llu offset %u != stream position %llu", nm.pre,
                                           (unsigned long long)(i / G), goff, (unsigned long long)nodes2.tell());
        }
        memset(rec, 0, sizeof rec);
        uint16_t cnt, code;
        if (street) {
            cnt = c0.u16();
            code = c1->u16();
            if (write_ways) memcpy(rec + 8, c2->take(4), 4);
        } else {
            cnt = c0.u16();
            code = c0.u16();
            if (write_ways) {
                memcpy(rec + 8, c1->take(4), 4);
                const uint32_t start = c2->u32();
                wr32(rec + 12, start);
                wr32(rec + 16, start + c3->u32());
                rec[20] = c4->u8();
            }
        }
        wr16(rec + 4, cnt);
        if (cnt) {
            if (!valid_code(code)) die("%s: way %llu has a bad code %#x", nm.pre, (unsigned long long)i, unsigned(code));
            wr32(rec, uint32_t(node_off));
            const size_t bs = block_size(cnt, code);
            const uint8_t* q = nodes2.need(bs + 8, true);
            const int wl = code & 63, wg = (code >> 6) & 63, wf = (code >> 13) & 7;
            const bool hyb = (code >> 12) & 1;
            uint32_t zl = 0, zg = 0;
            for (int b = 0; b < wf; b++) zl |= uint32_t(q[b]) << (8 * b);
            for (int b = 0; b < wf; b++) zg |= uint32_t(q[wf + b]) << (8 * b);
            const uint8_t* bp = q + 2 * wf;
            const uint32_t fl = ulp_unkey(ulp_key(al) + unzz(zl)), fg = ulp_unkey(ulp_key(ag) + unzz(zg));
            o[0] = fl;
            o[1] = fg;
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
                    o[2 * (j + 1)] = kl;
                    o[2 * (j + 1) + 1] = kg;
                }
                uint32_t* v = o.data() + 2;
                const size_t nv = 2 * mcount;
                if (hyb) for (size_t j = 0; j < nv; j++) v[j] = hyb_decode(v[j]);
                else for (size_t j = 0; j < nv; j++) v[j] = ulp_unkey(v[j]);
            }
            nodes2.skip(bs);
            N.put(o.data(), 8 * size_t(cnt));
            node_off += cnt;
        }
        if (W) W->put(rec, nm.stride);
    }
    if (!nodes2.eof() || node_off != nn) die("%s: nodes2 not fully consumed", nm.pre);
    if (!c0.eof() || (c1 && !c1->eof()) || (c2 && !c2->eof()) || (c3 && !c3->eof()) || (c4 && !c4->eof()))
        die("%s: columns not fully consumed", nm.pre);
    delete W;
    delete c1;
    delete c2;
    delete c3;
    delete c4;
}

}  // namespace ways
