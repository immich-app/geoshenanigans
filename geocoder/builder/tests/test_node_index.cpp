// SparseNodeIndex: a marked node resolves exactly as an id-indexed array
// would, and nothing else is stored.
#include "node_index.h"

#include <algorithm>
#include <cstdint>
#include <random>
#include <set>
#include <vector>

#include "test_framework.h"

namespace {

bool same(PackedLocation a, PackedLocation b) { return a.lat_e7 == b.lat_e7 && a.lon_e7 == b.lon_e7; }

}  // namespace

TEST(pack_location_rounds_to_nearest_e7) {
    CHECK_EQ(pack_location(-33.86882, 151.20929).lat_e7, -338688200);
    CHECK_EQ(pack_location(-33.86882, 151.20929).lon_e7, 1512092900);
    CHECK_EQ(pack_location(3e-7, -3e-7).lat_e7, 3);
    CHECK_EQ(pack_location(3e-7, -3e-7).lon_e7, -3);
    CHECK_EQ(pack_location(0.74e-7, -0.74e-7).lat_e7, 1);
    CHECK_EQ(pack_location(0.74e-7, -0.74e-7).lon_e7, -1);
    CHECK(!pack_location(0, 0).valid());
    CHECK(pack_location(0, 1e-7).valid());
}

TEST(sparse_node_index_round_trips_marked_ids_across_blocks) {
    SparseNodeIndex idx(100000);
    std::vector<uint64_t> ids = {1, 63, 64, 511, 512, 513, 4095, 4096, 99999};
    for (uint64_t id : ids) idx.mark(id);
    idx.mark(64);  // marking twice is harmless
    idx.finalize();
    CHECK_EQ(idx.marked(), ids.size());
    for (uint64_t id : ids) idx.set(id, id * 1e-4, -(id * 1e-4));
    for (uint64_t id : ids) CHECK(same(idx.get(id), pack_location(id * 1e-4, -(id * 1e-4))));
    CHECK_EQ(idx.unmarked_lookups(), 0u);
}

TEST(sparse_node_index_unset_unmarked_and_out_of_range_ids_are_invalid) {
    SparseNodeIndex idx(1000);
    idx.mark(10);
    idx.mark(11);
    idx.mark(5000);  // past capacity: ignored
    idx.finalize();
    CHECK_EQ(idx.marked(), 2u);

    idx.set(10, 1.5, 2.5);
    idx.set(12, 3.5, 4.5);    // unmarked: dropped
    idx.set(1000, 5.5, 6.5);  // past capacity: counted
    idx.set(uint64_t(-7), 5.5, 6.5);
    CHECK_EQ(idx.over_capacity(), 2u);

    CHECK(same(idx.get(10), pack_location(1.5, 2.5)));
    CHECK(!idx.get(11).valid());  // marked, never set
    CHECK_EQ(idx.unmarked_lookups(), 0u);
    CHECK(!idx.get(5000).valid());  // past capacity: not an unmarked lookup
    CHECK(!idx.get(uint64_t(-7)).valid());
    CHECK_EQ(idx.unmarked_lookups(), 0u);
    CHECK(!idx.get(12).valid());
    CHECK_EQ(idx.unmarked_lookups(), 1u);
}

TEST(sparse_node_index_matches_a_dense_array) {
    const size_t capacity = 200000;
    std::mt19937_64 rng(7);
    std::set<uint64_t> wanted;
    while (wanted.size() < 5000) wanted.insert(rng() % capacity);
    // Marks arrive from many threads in any order.
    std::vector<uint64_t> marks(wanted.begin(), wanted.end());
    std::shuffle(marks.begin(), marks.end(), rng);

    SparseNodeIndex idx(capacity);
    parallel_for(marks.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++) idx.mark(marks[i]);
    }, 8);
    idx.finalize(3);
    CHECK_EQ(idx.marked(), wanted.size());

    std::vector<PackedLocation> dense(capacity, PackedLocation{0, 0});
    for (uint64_t id = 1; id < capacity; id += 3) {
        double lat = std::uniform_real_distribution<double>(-90, 90)(rng);
        double lng = std::uniform_real_distribution<double>(-180, 180)(rng);
        dense[id] = pack_location(lat, lng);
        idx.set(id, lat, lng);
    }
    for (uint64_t id : wanted) CHECK(same(idx.get(id), dense[id]));
    CHECK_EQ(idx.unmarked_lookups(), 0u);
    CHECK_EQ(idx.over_capacity(), 0u);
}

TEST(sparse_node_index_release_is_idempotent) {
    SparseNodeIndex idx(4096);
    idx.mark(3);
    idx.finalize();
    CHECK(idx.bytes() > 0);
    idx.release();
    idx.release();
    CHECK_EQ(idx.bytes(), 0u);
    CHECK(!idx.get(3).valid());
    idx.set(3, 1, 1);
    CHECK_EQ(idx.over_capacity(), 1u);
}

TEST(sparse_node_index_with_nothing_marked) {
    SparseNodeIndex idx(4096);
    idx.finalize();
    CHECK_EQ(idx.marked(), 0u);
    idx.set(5, 1, 1);
    CHECK(!idx.get(5).valid());
    CHECK_EQ(idx.unmarked_lookups(), 1u);
}
