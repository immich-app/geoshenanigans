// gen_poi_vectors: writes geocoder/testdata/poi_files.tsv, the POI files the builder's encoder
// (builder/src/poi_files.h) writes for a fixed set of logical POI sets, which the builder's and
// the server's tests both check (the server's transport must give the same files back byte for
// byte).
//   g++ -std=c++20 -O1 -I geocoder/builder/src geocoder/testdata/gen_poi_vectors.cpp -o gen
//   ./gen > geocoder/testdata/poi_files.tsv
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "poi_files.h"
#include "types.h"

namespace {

std::string hex(const std::vector<uint8_t>& b) {
    if (b.empty()) return "-";
    std::string out;
    char c[3];
    for (uint8_t x : b) {
        snprintf(c, sizeof c, "%02x", x);
        out += c;
    }
    return out;
}

uint32_t bits(float f) {
    uint32_t b;
    memcpy(&b, &f, 4);
    return b;
}

struct Polygon {
    uint8_t encoding;
    uint32_t bmin_lat, bmin_lng;  // f32 bit patterns
    std::vector<std::pair<uint32_t, uint32_t>> q;
};

// A logical POI set, its records and the vertex stream they point into.
struct Set {
    std::vector<uint8_t> recs, verts;
    void add(PoiRecord r, const Polygon* p = nullptr) {
        r.vertex_offset = NO_DATA;
        r.vertex_count = 0;
        if (p) {
            r.vertex_offset = uint32_t(verts.size());
            r.vertex_count = uint32_t(p->q.size());
            const size_t vs = poi_files::vertex_bytes(p->encoding), at = verts.size();
            verts.resize(at + poi_files::LOGICAL_HEADER + p->q.size() * vs);
            verts[at] = p->encoding;
            poi_files::wr32(&verts[at + 2], p->bmin_lat);
            poi_files::wr32(&verts[at + 6], p->bmin_lng);
            for (size_t k = 0; k < p->q.size(); k++)
                poi_files::put_q(&verts[at + poi_files::LOGICAL_HEADER + k * vs], p->encoding, p->q[k].first, p->q[k].second);
        }
        const uint8_t* b = reinterpret_cast<const uint8_t*>(&r);
        recs.insert(recs.end(), b, b + sizeof r);
    }
};

PoiRecord record(float lat, float lng, uint32_t name, uint8_t category, uint8_t importance) {
    PoiRecord r{};
    r.lat = lat;
    r.lng = lng;
    r.name_id = name;
    r.category = category;
    r.tier = 2;
    r.flags = 1;
    r.importance = importance;
    r.parent_street_id = 1000 + name;
    r.parent_postcode_id = 2000;
    r.parent_poly_id = 3000;
    return r;
}

// One row: the case, the logical records and vertex stream (or * where the vertex stream is not
// listed: the builder's decoder gives it from the files), and the two files.
void row(const char* what, const Set& s, bool list_vertices = true) {
    const poi_files::Files f = poi_files::encode(s.recs.data(), s.recs.size() / poi_files::LOGICAL_STRIDE, s.verts.data(), s.verts.size());
    printf("%s\t%s\t%s\t%s\t%s\n", what, hex(s.recs).c_str(), list_vertices ? hex(s.verts).c_str() : "*", hex(f.records).c_str(),
           hex(f.vertices).c_str());
}

}  // namespace

int main() {
    printf("# POI files as the builder's encoder writes them (builder/src/poi_files.h): what the case\n");
    printf("# covers, the logical records (36-byte PoiRecords, types.h) and the logical vertex stream\n");
    printf("# they point into as hex (- for no bytes, * for a stream not listed: the files' decode), and\n");
    printf("# poi_records.bin and poi_vertices.bin as hex. The builder's tests encode the logical set to\n");
    printf("# the files and decode them back; the server's transport must give the files back byte for\n");
    printf("# byte. Regenerate with geocoder/testdata/gen_poi_vectors.cpp.\n");
    printf("case\trecords\tvertices\tpoi_records\tpoi_vertices\n");
    row("no records", Set{});
    {
        // Every field set; none of street, postcode, admin polygon; a tombstone; the largest
        // admin polygon R2 holds; repeated combos and postcodes.
        Set s;
        s.add(record(47.37f, 8.54f, 12, 7, 0));
        PoiRecord bare = record(-33.9f, 151.2f, 24, 7, 0);
        bare.parent_street_id = bare.parent_postcode_id = bare.parent_poly_id = NO_DATA;
        s.add(bare);
        PoiRecord dead{};
        dead.name_id = NO_DATA;
        s.add(dead);
        PoiRecord far = record(0.0f, -0.0f, 36, 9, 255);
        far.parent_poly_id = 0xFFFFFE;
        s.add(far);
        s.add(record(47.37f, 8.54f, 48, 7, 0));
        row("points only", s);
    }
    {
        // Each encoding, open and closed, the closed ones' last vertex the first.
        std::mt19937 rng(20261117);
        Set s;
        for (uint8_t e = 0; e < 4; e++)
            for (bool closed : {false, true}) {
                Polygon p{e, bits(47.0f + e), bits(8.0f - e), {}};
                for (int k = 0; k < 12; k++) {
                    const uint32_t a = uint32_t(rng()), b = uint32_t(rng());
                    p.q.push_back({e == 3 ? a : a >> 20, e == 3 ? b >> 4 : b >> 22});
                }
                if (closed) p.q.push_back(p.q[0]);
                s.add(record(47.5f, 7.5f, uint32_t(60 + 2 * e + closed), 3, e), &p);
            }
        row("a polygon of each encoding, open and closed", s);
    }
    {
        // One vertex; two equal ones (closed, one stored); two that differ; all-zero vertices (no
        // bits); u32 vertices at full width and at 63 bits; a bbox far from its record (5-byte
        // varints); a tombstone keeping its block.
        const std::vector<Polygon> polys = {
            {0, bits(47.0f), bits(8.0f), {{5, 9}}},
            {1, bits(47.0f), bits(8.0f), {{5, 9}, {5, 9}}},
            {2, bits(47.0f), bits(8.0f), {{5, 9}, {9, 5}}},
            {2, bits(47.0f), bits(8.0f), {{0, 0}, {0, 0}, {0, 0}}},
            {3, bits(47.0f), bits(8.0f), {{0xFFFFFFFFu, 0xFFFFFFFFu}, {0, 1}, {0x80000000u, 0x7FFFFFFFu}}},
            {3, bits(47.0f), bits(8.0f), {{0xFFFFFFFFu, 0x7FFFFFFFu}, {1, 2}, {3, 4}, {0xFFFFFFFFu, 0x7FFFFFFFu}}},
            {0, 0x7FFFFFFFu, 0x80000000u, {{1, 2}, {3, 4}}},
        };
        Set s;
        for (size_t i = 0; i < polys.size(); i++) s.add(record(-47.0f, -8.0f, uint32_t(100 + i), 4, uint8_t(i)), &polys[i]);
        PoiRecord dead{};
        dead.name_id = NO_DATA;
        s.add(dead, &polys[0]);
        row("the widths' and varints' edges", s);
    }
    {
        // Points and polygons mixed, a ring of 300 vertices per encoding among them, dictionaries
        // of several entries.
        std::mt19937 rng(20261118);
        Set s;
        for (int i = 0; i < 200; i++) {
            // One draw a statement, so the vectors don't depend on the order a compiler evaluates
            // arguments in.
            const float lat = float(int(rng() % 1800) - 900) / 10.0f;
            const float lng = float(int(rng() % 3600) - 1800) / 10.0f;
            const uint32_t name = uint32_t(rng() % 1000000);
            const uint8_t category = uint8_t(rng() % 40);
            const uint8_t importance = uint8_t(rng() % 4);
            PoiRecord r = record(lat, lng, name, category, importance);
            r.parent_postcode_id = rng() % 3 ? uint32_t(rng() % 50) * 7 : NO_DATA;
            r.parent_poly_id = rng() % 4 ? uint32_t(rng() % 100000) : NO_DATA;
            if (i % 50 == 25) {
                const uint8_t e = uint8_t(i / 50);
                Polygon p{e, bits(r.lat - 0.01f), bits(r.lng - 0.01f), {}};
                for (int k = 0; k < 300; k++) p.q.push_back({uint32_t(rng() >> (e == 3 ? 8 : 18)), uint32_t(rng() >> (e == 3 ? 10 : 19))});
                p.q.push_back(p.q[0]);
                s.add(r, &p);
            } else if (i % 7 == 3) {
                Polygon p{uint8_t(i % 3), bits(r.lat), bits(r.lng), {}};
                for (int k = 0; k < 1 + i % 9; k++) p.q.push_back({uint32_t(rng() % 4000), uint32_t(rng() % 4000)});
                s.add(r, &p);
            } else {
                s.add(r);
            }
        }
        row("200 records, points and polygons mixed", s);
    }
    {
        // 140,000 stored u32 vertices: 1,120,023 bytes as columns, past a transport chunk's 1 MiB,
        // in a block of 17,509 bytes. Open: the last vertex, (1, 0), is not the first.
        Polygon p{3, bits(46.0f), bits(7.0f), {}};
        for (uint32_t k = 0; k < 140000; k++) p.q.push_back({k & 1, 0});
        Set s;
        s.add(record(46.5f, 7.5f, 77, 5, 1), &p);
        s.add(record(46.6f, 7.6f, 78, 5, 1));
        row("a polygon of 140000 vertices", s, false);
    }
    return 0;
}
