// Continent subset copies (subset_copy.h) must match the serial loops they
// replaced: kept records in id order, coordinate runs appended as they go.
#include "subset_copy.h"

#include <random>

#include "test_framework.h"

namespace {

struct Rec {
    uint32_t offset;
    uint32_t count;
    uint32_t tag;
};

}  // namespace

TEST(copy_subset_matches_a_serial_append) {
    std::mt19937 rng(12);
    std::vector<NodeCoord> coords(4000);
    for (size_t i = 0; i < coords.size(); i++) coords[i] = {static_cast<float>(i), static_cast<float>(i) * 2};
    std::vector<Rec> full(3000);
    for (uint32_t i = 0; i < full.size(); i++) {
        uint32_t count = rng() % 4 == 0 ? 0 : rng() % 6;
        full[i] = {static_cast<uint32_t>(rng() % 3990), count, i};
    }
    std::vector<uint32_t> ids;
    for (uint32_t i = 0; i < full.size(); i++)
        if (rng() % 3 != 0) ids.push_back(i);
    std::vector<uint64_t> full_osm(2000);  // shorter than full: the rest get 0
    for (size_t i = 0; i < full_osm.size(); i++) full_osm[i] = 1000 + i;
    auto keep = [&](uint32_t id) { return full[id].tag % 5 != 0; };

    std::vector<Rec> want;
    std::vector<NodeCoord> want_coords;
    std::vector<uint32_t> want_remap(full.size(), NO_DATA);
    std::vector<uint64_t> want_osm;
    for (uint32_t id : ids) {
        if (!keep(id)) continue;
        want_remap[id] = static_cast<uint32_t>(want.size());
        Rec r = full[id];
        r.offset = static_cast<uint32_t>(want_coords.size());
        for (uint32_t j = 0; j < r.count; j++) want_coords.push_back(coords[full[id].offset + j]);
        want.push_back(r);
        want_osm.push_back(id < full_osm.size() ? full_osm[id] : 0);
    }

    std::vector<Rec> got;
    std::vector<NodeCoord> got_coords;
    auto remap = copy_subset(ids, full, coords, keep,
        [&](uint32_t id) { return CoordRun{full[id].offset, full[id].count}; },
        [](Rec& r, uint32_t offset, uint32_t) { r.offset = offset; },
        got, got_coords);
    CHECK(remap == want_remap);
    REQUIRE(got.size() == want.size());
    for (size_t i = 0; i < got.size(); i++) {
        CHECK_EQ(got[i].offset, want[i].offset);
        CHECK_EQ(got[i].tag, want[i].tag);
    }
    REQUIRE(got_coords.size() == want_coords.size());
    for (size_t i = 0; i < got_coords.size(); i++) CHECK_EQ(got_coords[i].lat, want_coords[i].lat);
    CHECK(subset_osm_ids(ids, remap, got.size(), full_osm) == want_osm);
}
