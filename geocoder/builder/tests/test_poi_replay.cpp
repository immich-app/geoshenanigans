// Unit tests for the old POI files as the patcher's POI replay reads them
// (tools/poi_replay.h): front to back, every byte of both files hashed in
// order, the hashes the diff sends of them.
#include "poi_replay.h"

#include <cstdint>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "patch_format.h"
#include "scratch_dir.h"
#include "test_framework.h"

namespace {

// n logical POI records and their vertex stream: one in three a point, the
// others polygons of 1 to 24 vertices of either width, those of two or more
// open or closed.
struct Day {
    std::vector<uint8_t> recs, verts;
};

Day random_day(std::mt19937& rng, uint32_t n) {
    Day d;
    for (uint32_t i = 0; i < n; i++) {
        uint8_t r[poi_files::LOGICAL_STRIDE];
        for (auto& b : r) b = uint8_t(rng());
        poi_files::wr32(r + 32, rng() % 0xFFFFFF);  // parent_poly_id: R2 holds 24 bits
        const uint32_t vc = i % 3 == 0 ? 0 : 1 + rng() % 24;
        poi_files::wr32(r + poi_files::OFF_VERTEX_OFFSET, vc ? uint32_t(d.verts.size()) : poi_files::NO);
        poi_files::wr32(r + poi_files::OFF_VERTEX_COUNT, vc);
        if (vc) {
            const bool wide = rng() % 2, closed = rng() % 2;
            uint8_t h[poi_files::LOGICAL_HEADER];
            for (auto& b : h) b = uint8_t(rng());
            h[0] = wide ? 3 : uint8_t(rng() % 3);
            h[1] = 0;
            d.verts.insert(d.verts.end(), h, h + sizeof h);
            const size_t vs = wide ? 8 : 4, first = d.verts.size();
            for (uint32_t k = 0; k < vc; k++) {
                uint8_t v[8];
                for (auto& b : v) b = uint8_t(rng());
                if (k && k + 1 == vc && closed) memcpy(v, &d.verts[first], vs);
                if (k && k + 1 == vc && !closed && memcmp(v, &d.verts[first], vs) == 0) v[0] ^= 1;
                d.verts.insert(d.verts.end(), v, v + vs);
            }
        }
        d.recs.insert(d.recs.end(), r, r + sizeof r);
    }
    return d;
}

}  // namespace

// The hashes of the old poi_records.bin and poi_vertices.bin the replay gives
// once it has passed both are lane_hash of each whole file, as the diff hashes
// them: over 5,000 rows, which the rows' reader takes 32 KiB at a time, so
// that they are hashed across many of its refills, their blocks decoded,
// copied and passed by turns; and over the same files with every row passed
// as the replay finishes.
TEST(poi_replay_hashes_each_old_file_whole) {
    std::mt19937 rng(20261117);
    const Day day = random_day(rng, 5000);
    const poi_files::Files files =
        poi_files::encode(day.recs.data(), day.recs.size() / poi_files::LOGICAL_STRIDE, day.verts.data(), day.verts.size());
    REQUIRE(files.records.size() > 4 * (32 << 10));
    ScratchDir dir("poi-replay-hash");
    const std::vector<char> records(files.records.begin(), files.records.end()), vertices(files.vertices.begin(), files.vertices.end());
    REQUIRE(write_file(dir.path() + "/poi_records.bin", records));
    REQUIRE(write_file(dir.path() + "/poi_vertices.bin", vertices));
    const uint64_t records_hash = lane_hash(records.data(), records.size()), vertices_hash = lane_hash(vertices.data(), vertices.size());

    poi_replay::OldRows walked(dir.path());
    uint64_t decoded = 0, copied = 0;
    for (uint64_t i = 0; i < walked.rows(); i++) {
        walked.next();
        if (!walked.block()) continue;
        if (i % 3 == 1 && walked.logical_block()) decoded++;
        if (i % 3 == 2 && walked.whole()) {
            walked.copy_block([](const uint8_t*, size_t) {});
            copied++;
        }
    }
    const poi_replay::Hashes got = walked.finish();
    CHECK(decoded > 1000);
    CHECK(copied > 1000);
    CHECK_EQ(got.records, records_hash);
    CHECK_EQ(got.vertices, vertices_hash);

    poi_replay::OldRows passed(dir.path());
    const poi_replay::Hashes all = passed.finish();
    CHECK_EQ(all.records, records_hash);
    CHECK_EQ(all.vertices, vertices_hash);
}
