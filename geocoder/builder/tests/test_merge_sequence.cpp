// Unit tests for merge_sequence.h: the child-stream merge geocoder-diff emits
// for street/interp nodes and the *_vertices byte streams.
#include "merge_sequence.h"

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "test_framework.h"

namespace {

struct Op {
    uint8_t op;
    uint32_t count;
    std::string data;
    bool operator==(const Op& o) const { return op == o.op && count == o.count && data == o.data; }
};

std::vector<Op> ops_of(const MergeSequence& s, size_t stride) {
    std::vector<Op> out;
    size_t pos = 0;
    while (pos < s.data.size()) {
        Op o{static_cast<uint8_t>(s.data[pos]), 0, {}};
        std::memcpy(&o.count, s.data.data() + pos + 1, 4);
        pos += 5;
        if (o.op == OP_INSERT_RUN) {
            o.data.assign(s.data.data() + pos, o.count * stride);
            pos += o.count * stride;
        }
        out.push_back(o);
    }
    return out;
}

constexpr size_t PARENT_STRIDE = 4;
const char PARENT[PARENT_STRIDE * 4] = {};  // inline INSERT records; their bytes don't matter here

// Child blocks of a stride-1 stream, one per parent record.
struct Stream {
    std::string bytes;
    std::vector<ChildBlock> blocks;
};

MergeSequence merge(const MergeSequence& parent, const Stream& old_s, const Stream& new_s) {
    return merge_child_blocks(parent, PARENT_STRIDE, 1, old_s.bytes.data(), old_s.bytes.size(),
                              new_s.bytes.data(), new_s.bytes.size(),
                              [&](size_t i) { return old_s.blocks[i]; },
                              [&](size_t i) { return new_s.blocks[i]; });
}

}  // namespace

TEST(merge_child_blocks_match_keeps_equal_blocks_and_resends_changed_ones) {
    MergeSequence parent;
    parent.add_match(2);
    Stream old_s{"AAAABBBB", {{0, 4}, {4, 4}}}, new_s{"AAAACCCC", {{0, 4}, {4, 4}}};
    auto got = ops_of(merge(parent, old_s, new_s), 1);
    std::vector<Op> want = {{OP_MATCH_RUN, 4, ""}, {OP_DELETE_RUN, 4, ""}, {OP_INSERT_RUN, 4, "CCCC"}};
    CHECK(got == want);
}

TEST(merge_child_blocks_follows_inserted_and_deleted_parents) {
    MergeSequence parent;
    parent.add_delete(1);
    parent.add_match(1);
    parent.add_insert(PARENT, 1, PARENT_STRIDE);
    Stream old_s{"XXAAAA", {{0, 2}, {2, 4}}}, new_s{"AAAAYYY", {{0, 4}, {4, 3}}};
    auto got = ops_of(merge(parent, old_s, new_s), 1);
    std::vector<Op> want = {{OP_DELETE_RUN, 2, ""}, {OP_MATCH_RUN, 4, ""}, {OP_INSERT_RUN, 3, "YYY"}};
    CHECK(got == want);
}

TEST(merge_child_blocks_coalesces_runs_and_skips_empty_blocks) {
    // Point records (empty blocks) between polygons don't split the runs.
    MergeSequence parent;
    parent.add_match(3);
    Stream old_s{"AABB", {{0, 2}, {0, 0}, {2, 2}}}, new_s{"AABB", {{0, 2}, {0, 0}, {2, 2}}};
    auto got = ops_of(merge(parent, old_s, new_s), 1);
    CHECK(got == std::vector<Op>({{OP_MATCH_RUN, 4, ""}}));
}

TEST(merge_child_blocks_counts_in_units) {
    // Node streams count 8-byte nodes, not bytes.
    MergeSequence parent;
    parent.add_insert(PARENT, 1, PARENT_STRIDE);
    std::string nodes(16, 'n');
    std::vector<ChildBlock> none, one = {{0, 16}};
    auto seq = merge_child_blocks(parent, PARENT_STRIDE, 8, nullptr, 0, nodes.data(), nodes.size(),
                                  [&](size_t i) { return none[i]; }, [&](size_t i) { return one[i]; });
    CHECK(ops_of(seq, 8) == std::vector<Op>({{OP_INSERT_RUN, 2, nodes}}));
}
