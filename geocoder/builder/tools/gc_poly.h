// Admin outlines, layout B (max/verify/scripts/layoutb.c, four-changes change 3), and POI records +
// vertices (the index track's R3 records and B-open vertices, max/index-poi-strings/scripts/poi_rec.py,
// poi_vert.py), neither re-sorted.
//
// admin_vertices.B: every polygon block keeps today's offset and size (admin_polygons.bin unchanged):
//   10-byte header verbatim, then the wrapping zigzag deltas (first vertex absolute) of q_lat then q_lng
//   at the polygon's width w (2 B for enc 0-2, 4 B for enc 3) as byte planes:
//   lat b0 | lat b1 [| b2 | b3] | lng b0 | ...
//
// poi_vertices.bopen: polygons back to back in record order; each block: today's 10-byte header with
//   the pad byte (byte 1) = 1 when the ring's closing vertex (== first, count > 1) was dropped; then
//   the wrapping zigzag deltas of the stored vertices as byte planes at width w, as layout B.
// poi_records.r3 (32 B): today's 36-byte record without vertex_count, with vertex_offset replaced by a
//   monotone offset into poi_vertices.bopen (a polygon: its block; a point: where the next polygon's
//   block starts, or the file size). A record is a polygon iff its offset differs from the next
//   record's; vertex_count = (block size - 10) / (2 w) + closing flag. Unlike the index track's R3, no
//   field is zeroed (its R1 step zeroed the never-read lat/lng of polygon POIs and dead records'
//   fields), so the records round-trip byte for byte.
#pragma once
#include "gc_io.h"

namespace gcset::poly {

static inline uint32_t zzw(uint32_t d, int bits) {  // d: wrapped delta in [0, 2^bits)
    const int64_t full = bits == 32 ? 0x100000000LL : (1LL << bits);
    const int64_t s = d >= uint64_t(full / 2) ? int64_t(d) - full : int64_t(d);
    return uint32_t(s >= 0 ? 2 * s : -2 * s - 1);
}
static inline uint32_t unzz(uint32_t z) { return (z >> 1) ^ uint32_t(-int32_t(z & 1)); }

// Plane-encode c vertices of width w (src: today's interleaved vertices) into dst (2*w*c bytes).
static void planes_enc(const uint8_t* src, uint8_t* dst, uint64_t c, int w) {
    const int bits = 8 * w;
    const uint64_t mask = bits == 32 ? 0xFFFFFFFFull : ((1ull << bits) - 1);
    for (int comp = 0; comp < 2; comp++) {
        uint32_t prev = 0;
        for (uint64_t k = 0; k < c; k++) {
            uint32_t q = 0;
            for (int b = 0; b < w; b++) q |= uint32_t(src[k * 2 * w + comp * w + b]) << (8 * b);
            const uint32_t z = zzw(uint32_t((uint64_t(q) - prev) & mask), bits);
            for (int b = 0; b < w; b++) dst[(comp * w + b) * c + k] = uint8_t(z >> (8 * b));
            prev = q;
        }
    }
}
static void planes_dec(const uint8_t* src, uint8_t* dst, uint64_t c, int w) {
    const int bits = 8 * w;
    const uint64_t mask = bits == 32 ? 0xFFFFFFFFull : ((1ull << bits) - 1);
    for (int comp = 0; comp < 2; comp++) {
        uint32_t prev = 0;
        for (uint64_t k = 0; k < c; k++) {
            uint32_t z = 0;
            for (int b = 0; b < w; b++) z |= uint32_t(src[(comp * w + b) * c + k]) << (8 * b);
            const uint32_t q = uint32_t((uint64_t(prev) + unzz(z)) & mask);
            for (int b = 0; b < w; b++) dst[k * 2 * w + comp * w + b] = uint8_t(q >> (8 * b));
            prev = q;
        }
    }
}

// ---------------------------------------------------------------- admin layout B
struct PolyRef { uint64_t off, cnt; };

static std::vector<PolyRef> admin_polys(const std::string& dir) {
    Reader r(dir + "/admin_polygons.bin", 1 << 20);
    if (r.size() % 24) die("admin_polygons.bin size");
    std::vector<PolyRef> ps;
    while (!r.eof()) {
        const uint8_t* p = r.take(24);
        const uint32_t vo = rd32(p), vc = rd32(p + 4);
        if (vc > 0 && vo != NO) ps.push_back({vo, vc});
    }
    std::sort(ps.begin(), ps.end(), [](const PolyRef& a, const PolyRef& b) { return a.off < b.off; });
    return ps;
}

static void admin_run(const std::string& polys_dir, const std::string& in, const std::string& outp, bool enc, Manifest* m) {
    // Streaming: the vertices are opened first, so a FIFO open waits until the patcher has written
    // admin_polygons.bin (its section comes first) and moved on to admin_vertices.bin.
    Reader R(in);
    const std::vector<PolyRef> ps = admin_polys(polys_dir);
    Writer W(outp);
    std::vector<uint8_t> blk;
    uint64_t pos = 0, verts = 0, by_enc[4] = {0, 0, 0, 0};
    for (const PolyRef& p : ps) {
        if (p.off != pos) die("admin_vertices: gap or overlap at offset %llu (expected %llu)", (unsigned long long)p.off,
                              (unsigned long long)pos);
        const uint8_t e = *R.need(1);
        if (e > 3) die("admin_vertices: bad encoding %u", e);
        const int w = e == 3 ? 4 : 2;
        const size_t size = R.unit_size(10, p.cnt, 2 * uint64_t(w));
        const uint8_t* s = R.take(size);
        blk.resize(size);
        memcpy(blk.data(), s, 10);
        if (enc) planes_enc(s + 10, blk.data() + 10, p.cnt, w);
        else planes_dec(s + 10, blk.data() + 10, p.cnt, w);
        W.put(blk);
        pos += size;
        verts += p.cnt;
        by_enc[e]++;
    }
    if (!R.eof()) die("admin_vertices: bytes after the last polygon");
    if (m) {
        m->set("admin.polygons", ps.size());
        fprintf(stderr, "  admin layout B: polygons %zu vertices %llu enc0/1/2/3 %llu/%llu/%llu/%llu bytes %llu\n", ps.size(),
                (unsigned long long)verts, (unsigned long long)by_enc[0], (unsigned long long)by_enc[1],
                (unsigned long long)by_enc[2], (unsigned long long)by_enc[3], (unsigned long long)pos);
    }
}

inline void encode_admin(const std::string& src, const std::string& out, Manifest& m) {
    admin_run(src, src + "/admin_vertices.bin", out + "/admin_vertices.B", true, &m);
}
inline void decode_admin(const std::string& src, const std::string& out, const Manifest&) {
    admin_run(src, src + "/admin_vertices.B", out + "/admin_vertices.bin", false, nullptr);
}

// ---------------------------------------------------------------- POI R3 + B-open
// Streaming: the patcher writes poi_records.bin whole, then poi_vertices.bin, and R3 needs the
// B-open offsets, so pass 1 (records) only copies the records to poi_records.pass.tmp and pass 2
// (vertices) is the one-pass encoder reading the records from there.
inline void encode_poi(const std::string& src, const std::string& out, Manifest& m) {
    std::string rp = src + "/poi_records.bin";
    if (want("poi_records.bin") != want("poi_vertices.bin")) {
        const std::string tmp = out + "/poi_records.pass.tmp";
        if (want("poi_records.bin")) {
            copy_file(rp, tmp);
            fprintf(stderr, "  poi pass 1 (records): %llu B to %s\n", (unsigned long long)file_size(tmp), tmp.c_str());
            return;
        }
        rp = tmp;
    }
    if (file_size(rp) % 36) die("poi_records.bin size");
    const uint64_t n = file_size(rp) / 36;
    std::vector<uint32_t> polyoff;  // B-open offset of each polygon, in record order
    uint64_t closed_n = 0, verts = 0;
    {
        Reader R(rp), V(src + "/poi_vertices.bin");
        Writer O(out + "/poi_vertices.bopen");
        std::vector<uint8_t> blk;
        uint64_t vexp = 0;
        for (uint64_t i = 0; i < n; i++) {
            const uint8_t* r = R.take(36);
            const uint32_t vo = rd32(r + 8), vc = rd32(r + 12);
            if ((vc == 0) != (vo == NO)) die("poi_records %llu: vertex_count %u with vertex_offset %u", (unsigned long long)i, vc, vo);
            if (!vc) continue;
            if (vo != vexp) die("poi_records %llu: polygons not back to back in record order", (unsigned long long)i);
            const uint8_t e = *V.need(1);
            if (e > 3) die("poi_vertices: bad encoding %u", e);
            const int w = e == 3 ? 4 : 2;
            const size_t size = V.unit_size(10, vc, 2 * uint64_t(w));
            const uint8_t* s = V.take(size);
            if (s[1] != 0) die("poi_vertices: non-zero pad byte at %llu", (unsigned long long)vexp);
            const uint8_t* first = s + 10;
            const uint8_t* last = s + 10 + size_t(vc - 1) * 2 * w;
            const bool closed = vc > 1 && memcmp(first, last, 2 * w) == 0;
            const uint64_t c2 = vc - (closed ? 1 : 0);
            if (O.tell() >= 0xFFFFFFFFull) die("poi_vertices.bopen offset overflow");
            polyoff.push_back(uint32_t(O.tell()));
            blk.resize(10 + c2 * 2 * w);
            memcpy(blk.data(), s, 10);
            blk[1] = closed ? 1 : 0;
            planes_enc(s + 10, blk.data() + 10, c2, w);
            O.put(blk);
            vexp += size;
            closed_n += closed;
            verts += vc;
        }
        if (!V.eof()) die("poi_vertices.bin: bytes not covered by any record");
        m.set("poi.bopen_bytes", O.tell());
    }
    const uint64_t total = m.num("poi.bopen_bytes");
    Reader R(rp);
    Writer O(out + "/poi_records.r3");
    size_t j = 0;
    uint8_t rec[32];
    for (uint64_t i = 0; i < n; i++) {
        const uint8_t* r = R.take(36);
        const bool isp = rd32(r + 12) != 0;
        const uint64_t mono = isp ? polyoff[j++] : (j < polyoff.size() ? polyoff[j] : total);
        // A point after the last polygon holds the file size, which the 32-bit field must hold too
        // (as the patcher's PoiEnc checks before every record).
        if (mono >= 0xFFFFFFFFull) die("poi_vertices.bopen offset overflow at record %llu", (unsigned long long)i);
        memcpy(rec, r, 8);
        wr32(rec + 8, uint32_t(mono));
        memcpy(rec + 12, r + 16, 20);
        O.put(rec, 32);
    }
    if (rp == out + "/poi_records.pass.tmp") unlink(rp.c_str());
    m.set("poi.n", n);
    fprintf(stderr, "  poi: records %llu polygons %zu vertices %llu closed rings dropped %llu bopen %llu B\n", (unsigned long long)n,
            polyoff.size(), (unsigned long long)verts, (unsigned long long)closed_n, (unsigned long long)total);
}

// Streaming: either file alone (the records still read every block header for vertex_count; the
// vertices skip the records' writer).
inline void decode_poi(const std::string& src, const std::string& out, const Manifest& m) {
    const uint64_t n = m.num("poi.n");
    Reader R(src + "/poi_records.r3"), V(src + "/poi_vertices.bopen");
    if (R.size() != 32 * n) die("poi_records.r3 size");
    const uint64_t total = V.size();
    const bool wr = want("poi_records.bin"), wv = want("poi_vertices.bin");
    Writer* ORp = wr ? new Writer(out + "/poi_records.bin") : nullptr;
    Writer* OVp = wv ? new Writer(out + "/poi_vertices.bin") : nullptr;
    uint64_t vt = 0;  // poi_vertices.bin bytes so far
    std::vector<uint8_t> blk;
    uint8_t cur[32], nxt[32], rec[36];
    if (n) memcpy(cur, R.take(32), 32);
    for (uint64_t i = 0; i < n; i++) {
        uint64_t mono_next = total;
        if (i + 1 < n) {
            memcpy(nxt, R.take(32), 32);
            mono_next = rd32(nxt + 8);
        }
        const uint64_t mono = rd32(cur + 8);
        memcpy(rec, cur, 8);
        memcpy(rec + 16, cur + 12, 20);
        if (mono_next != mono) {
            if (mono != V.tell() || mono_next < mono) die("poi %llu: block offset %llu is not the stream position", (unsigned long long)i,
                                                          (unsigned long long)mono);
            const uint64_t bsize = mono_next - mono;
            if (bsize < 10) die("poi %llu: bad block size", (unsigned long long)i);
            const uint8_t* h = V.need(10);
            const uint8_t e = h[0], flag = h[1];
            if (e > 3 || flag > 1) die("poi %llu: bad block header", (unsigned long long)i);
            const int w = e == 3 ? 4 : 2;
            if ((bsize - 10) % (2 * w)) die("poi %llu: bad block size", (unsigned long long)i);
            const uint64_t c2 = (bsize - 10) / (2 * w), vc = c2 + flag;
            // The encoder stores every vertex but a closed ring's closing one (and a closed ring has 2 or
            // more), so a block stores at least one, and vertex_count is a u32.
            if (c2 == 0) die("poi %llu: block with no stored vertex", (unsigned long long)i);
            if (vc > 0xFFFFFFFFull) die("poi %llu: block of more vertices than a record holds", (unsigned long long)i);
            h = V.take(V.unit_size(bsize));
            if (vt >= 0xFFFFFFFFull) die("poi_vertices offset overflow");
            wr32(rec + 8, uint32_t(vt));
            wr32(rec + 12, uint32_t(vc));
            vt += 10 + vc * 2 * w;
            if (OVp) {
                blk.resize(10 + vc * 2 * w);
                memcpy(blk.data(), h, 10);
                blk[1] = 0;
                planes_dec(h + 10, blk.data() + 10, c2, w);
                if (flag) memcpy(blk.data() + 10 + c2 * 2 * w, blk.data() + 10, 2 * w);
                OVp->put(blk);
            }
        } else {
            wr32(rec + 8, NO);
            wr32(rec + 12, 0);
        }
        if (ORp) ORp->put(rec, 36);
        memcpy(cur, nxt, 32);
    }
    if (!V.eof() || !R.eof()) die("poi: streams not fully consumed");
    delete ORp;
    delete OVp;
}

}  // namespace gcset::poly
