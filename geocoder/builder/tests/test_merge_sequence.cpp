// Unit tests for merge_sequence.h: the child-stream merge geocoder-diff emits
// for street/interp nodes and the *_vertices byte streams.
#include "merge_sequence.h"

#include <cstdint>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "in_place_edits.h"
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
        if (o.op == OP_INSERT_RUN || o.op == OP_REPLACE_RUN) {
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

namespace {

// 12-byte records (past the 8-byte stride that turns hash jumps on), each
// labelled by one character.
std::string records(const std::string& labels) {
    std::string out;
    for (char c : labels) out += std::string(12, c);
    return out;
}

std::vector<Op> merge_of(const std::string& o, const std::string& n) {
    return ops_of(build_merge_seq(o.data(), o.size(), n.data(), n.size(), 12), 12);
}

std::vector<Op> record_merge(const std::string& old_labels, const std::string& new_labels) {
    return merge_of(records(old_labels), records(new_labels));
}

// n distinct 12-byte records numbered from first, none of them a tombstone.
std::string numbered(uint32_t first, uint32_t n) {
    std::string out;
    for (uint32_t i = 0; i < n; i++) {
        std::string rec(12, 'r');
        const uint32_t v = first + i;
        std::memcpy(&rec[0], &v, 4);
        out += rec;
    }
    return out;
}

// n tombstones, all alike: zeroed records, which the jump index leaves out,
// or (fill 0xff) records of NO_DATA ids like a POI's, which it holds.
std::string tombstones(uint32_t n, char fill) { return std::string(size_t(n) * 12, fill); }

// The first nightly without unnamed POIs tombstoned 24,543 contiguous planet
// poi/all records in place, behind and ahead of unchanged ones.
constexpr uint32_t HEAD = 100, DEAD = 24543, TAIL = 5000;

// old_f rebuilt from seq (stride 12): MATCH copies its records, DELETE skips
// them, INSERT appends the sequence's; recs[op] counts each op's records.
std::string replay(const std::string& old_f, const MergeSequence& seq, size_t recs[4]) {
    std::string out;
    size_t at = 0;
    for_each_merge_op(seq, 12, [&](uint8_t op, uint32_t count, const char* inline_records) {
        recs[op] += count;
        if (op == OP_MATCH_RUN) out.append(old_f, at * 12, size_t(count) * 12);
        if (inline_records) out.append(inline_records, size_t(count) * 12);
        if (op != OP_INSERT_RUN) at += count;
    });
    return out;
}

}  // namespace

TEST(vertex_blocks_run_to_the_next_record_with_an_offset) {
    // 8-byte records, the vertex offset at byte 4; a 50-byte vertex stream.
    const uint32_t offs[] = {0, 0xFFFFFFFFu, 10, 5, 30, 100, 40};
    std::string parent(sizeof(offs) / 4 * 8, 'x');
    for (size_t i = 0; i < sizeof(offs) / 4; i++) std::memcpy(&parent[i * 8 + 4], &offs[i], 4);
    VertexBlocks b = vertex_blocks(parent.data(), parent.size(), 8, 4, 50);
    CHECK(b.offsets == std::vector<uint32_t>(std::begin(offs), std::end(offs)));
    // NO_DATA and past-the-stream offsets own nothing; 10 lies past its
    // successor's 5 and owns nothing either.
    CHECK(b.sizes == std::vector<uint32_t>({10, 0, 0, 25, 10, 0, 10}));
}

TEST(vertex_blocks_same_for_any_thread_count) {
    // Mostly NO_DATA (point records) with long gaps, so chunks often hold
    // no block and take their next offset from a later chunk.
    std::mt19937 rng(12);
    for (size_t n : {size_t(0), size_t(1), size_t(3), size_t(20000)}) {
        std::string parent(n * 12, '\0');
        uint32_t at = 0;
        for (size_t i = 0; i < n; i++) {
            uint32_t off = 0xFFFFFFFFu;
            int kind = static_cast<int>(rng() % 50);
            if (kind == 0) off = rng() % 400000;
            else if (kind < 4) off = at += rng() % 60;
            std::memcpy(&parent[i * 12 + 8], &off, 4);
        }
        VertexBlocks one = vertex_blocks(parent.data(), parent.size(), 12, 8, 300000, 1);
        for (unsigned threads : {2u, 7u, 64u}) {
            VertexBlocks many = vertex_blocks(parent.data(), parent.size(), 12, 8, 300000, threads);
            CHECK(many.offsets == one.offsets);
            CHECK(many.sizes == one.sizes);
        }
    }
}

TEST(record_jump_index_finds_the_first_equal_record_in_range) {
    // Few distinct 12-byte records, zeroed ones among them: duplicates and
    // tombstones are what the index must get right.
    constexpr size_t STRIDE = 12;
    std::mt19937 rng(9);
    for (size_t n : {size_t(0), size_t(1), size_t(50), size_t(300000)}) {
        std::string data(n * STRIDE, '\0');
        for (size_t i = 0; i < n; i++) {
            uint32_t v = rng() % 40;
            if (v != 0) std::memcpy(&data[i * STRIDE + 4], &v, 4);
        }
        for (unsigned threads : {1u, 5u}) {
            RecordJumpIndex index(data.data(), n, STRIDE, threads);
            for (int q = 0; q < 2000; q++) {
                std::string rec(STRIDE, '\0');
                uint32_t v = rng() % 41;
                if (v != 0) std::memcpy(&rec[4], &v, 4);
                size_t lo = n ? rng() % n : 0, hi = lo + rng() % 3000;
                uint32_t expect = UINT32_MAX;
                for (size_t j = lo; j < std::min(hi, n) && expect == UINT32_MAX; j++)
                    if (v != 0 && std::memcmp(rec.data(), data.data() + j * STRIDE, STRIDE) == 0) expect = j;
                CHECK_EQ(index.first_equal(rec.data(), lo, hi), expect);
            }
        }
    }
}

TEST(build_merge_seq_far_jump_needs_a_run_behind_it) {
    // X and Y moved behind 30 unchanged records. Jumping to their copies
    // would re-send all 30; deleting and re-inserting the pair costs two.
    std::string body = "abcdefghijklmnopqrstuvwxyz0123";
    auto got = record_merge("XY" + body + "Z", body + "XYZ");
    std::vector<Op> want = {{OP_DELETE_RUN, 2, ""}, {OP_MATCH_RUN, 30, ""},
                            {OP_INSERT_RUN, 2, records("XY")}, {OP_MATCH_RUN, 1, ""}};
    CHECK(got == want);
}

TEST(build_merge_seq_jumps_over_inserted_records) {
    std::string body = "abcdefghijklmnopqrstuvwxyz0123";
    auto got = record_merge(body, "ABCDEFGHIJKLMNOPQRST" + body);
    std::vector<Op> want = {{OP_INSERT_RUN, 20, records("ABCDEFGHIJKLMNOPQRST")}, {OP_MATCH_RUN, 30, ""}};
    CHECK(got == want);
}

TEST(jump_window_holds_a_run_of_identical_records_as_one) {
    // Records of three values in runs of random length, the window asked
    // for from a cursor that only moves forward, its end checked against
    // counting the runs from the cursor.
    constexpr size_t STRIDE = 12;
    std::mt19937 rng(14);
    for (size_t n : {size_t(1), size_t(70), size_t(5000)}) {
        std::string data;
        while (data.size() < n * STRIDE) {
            const std::string rec(STRIDE, static_cast<char>('a' + rng() % 3));
            for (size_t k = rng() % 3 == 0 ? rng() % 200 : 1; k-- > 0 && data.size() < n * STRIDE;) data += rec;
        }
        for (size_t runs : {size_t(1), size_t(4), size_t(100)}) {
            for (unsigned threads : {1u, 5u}) {
                JumpWindow window(data.data(), n, STRIDE, runs, threads);
                for (size_t at = 0; at < n; at += rng() % 40) {
                    size_t end = at + 1;
                    for (size_t seen = 1; end < n; end++) {
                        const bool starts = data.compare(end * STRIDE, STRIDE, data, (end - 1) * STRIDE, STRIDE) != 0;
                        if (starts && seen == runs) break;
                        seen += starts;
                    }
                    CHECK_EQ(window.end(at), end);
                }
            }
        }
    }
}

TEST(build_merge_seq_matches_the_records_after_a_long_run_of_tombstones) {
    // While the dead run is deleted the cursor waits on the first
    // tombstone; the records after the run must still be found from there.
    for (char fill : {'\0', '\xff'}) {
        auto got = merge_of(numbered(0, HEAD) + numbered(1000000, DEAD) + numbered(HEAD, TAIL),
                            numbered(0, HEAD) + tombstones(DEAD, fill) + numbered(HEAD, TAIL));
        std::vector<Op> want = {{OP_MATCH_RUN, HEAD, ""}, {OP_DELETE_RUN, DEAD, ""},
                                {OP_INSERT_RUN, DEAD, tombstones(DEAD, fill)}, {OP_MATCH_RUN, TAIL, ""}};
        CHECK(got == want);
    }
}

TEST(build_merge_seq_matches_past_tombstones_behind_a_changed_record) {
    // The record in front of the dead run changed as well, so the cursor
    // waits on its new version, not on a tombstone.
    for (char fill : {'\0', '\xff'}) {
        auto got = merge_of(numbered(0, HEAD) + numbered(2000000, 1) + numbered(1000000, DEAD) + numbered(HEAD, TAIL),
                            numbered(0, HEAD) + numbered(3000000, 1) + tombstones(DEAD, fill) + numbered(HEAD, TAIL));
        std::vector<Op> want = {{OP_MATCH_RUN, HEAD, ""}, {OP_DELETE_RUN, 1 + DEAD, ""},
                                {OP_INSERT_RUN, 1 + DEAD, numbered(3000000, 1) + tombstones(DEAD, fill)},
                                {OP_MATCH_RUN, TAIL, ""}};
        CHECK(got == want);
    }
}

TEST(build_merge_seq_lands_no_far_jump_on_copies_of_a_tombstone) {
    // Yesterday's tombstones in front of unchanged records; past a long
    // run of zeroed slots the new side holds tombstones of the same kind,
    // further than 10,000 records ahead. Copies of a tombstone place
    // nothing: landing on them would re-send the unchanged records.
    const std::string t = tombstones(16, '\xff');
    auto got = merge_of(t + numbered(0, TAIL) + t, numbered(0, TAIL) + tombstones(DEAD, '\0') + t);
    std::vector<Op> want = {{OP_DELETE_RUN, 16, ""}, {OP_MATCH_RUN, TAIL, ""}, {OP_DELETE_RUN, 16, ""},
                            {OP_INSERT_RUN, DEAD + 16, tombstones(DEAD, '\0') + t}};
    CHECK(got == want);
}

TEST(build_merge_seq_jumps_far_to_a_tombstone_the_records_after_it_place) {
    // Past the dead run, a tombstone followed by unchanged records: they
    // place it, so the jump onto it still goes further than 10,000 records.
    auto got = merge_of(numbered(1000000, DEAD) + tombstones(1, '\xff') + numbered(0, TAIL),
                        tombstones(DEAD, '\0') + tombstones(1, '\xff') + numbered(0, TAIL));
    std::vector<Op> want = {{OP_DELETE_RUN, DEAD, ""}, {OP_INSERT_RUN, DEAD, tombstones(DEAD, '\0')},
                            {OP_MATCH_RUN, 1 + TAIL, ""}};
    CHECK(got == want);
}

TEST(build_merge_seq_still_matches_the_tombstones_both_days_hold) {
    // A run of yesterday's tombstones behind a deleted record: the window
    // reaching past it doesn't make it an insert.
    for (char fill : {'\0', '\xff'}) {
        auto got = merge_of(numbered(2000000, 1) + tombstones(DEAD, fill) + numbered(0, TAIL),
                            tombstones(DEAD, fill) + numbered(0, TAIL));
        std::vector<Op> want = {{OP_DELETE_RUN, 1, ""}, {OP_MATCH_RUN, DEAD + TAIL, ""}};
        CHECK(got == want);
    }
}

TEST(build_merge_seq_matches_the_records_kept_among_records_edited_in_place) {
    // A changed record at the cursor is passed only with the old record
    // after it, which a stretch of edits too dense for a jump to be taken
    // there never lets the cursor do: 12,000 such records leave the old
    // records' counterparts behind a window of 10,000, and the rest of the
    // file goes as DELETE + INSERT. Every unchanged record stays a MATCH.
    constexpr uint32_t N = 200000, DENSE = 20000, DENSE_N = 12000;
    std::mt19937 rng(20261103);
    const auto edited = gctest::edited_in_place(rng, N, DENSE, DENSE_N);
    const std::string old_f = numbered(0, N);
    std::string new_f = old_f;
    uint32_t n_edited = 0;
    for (uint32_t i = 0; i < N; i++)
        if (edited[i]) { new_f[size_t(i) * 12 + 4] = 'e'; n_edited++; }
    size_t recs[4] = {};
    const std::string got = replay(old_f, build_merge_seq(old_f.data(), old_f.size(), new_f.data(), new_f.size(), 12), recs);

    CHECK(got == new_f);
    CHECK_EQ(recs[OP_MATCH_RUN], size_t(N - n_edited));
    CHECK_EQ(recs[OP_DELETE_RUN], size_t(n_edited));
    CHECK_EQ(recs[OP_INSERT_RUN], size_t(n_edited));
}

TEST(build_merge_seq_matches_a_block_both_files_hold_twice_behind_deleted_records) {
    // Two records deleted in front of a block of 12,000 that both files hold
    // twice. Each record at the cursor has a copy further on in new, but the
    // records after its old copy two records on are the ones that follow:
    // sent in place of the deleted records, the block's records would put
    // the cursor past their old copies, with the second copy past the
    // window, and 12,000 unchanged records would go as DELETE + INSERT.
    constexpr uint32_t B = 12000;
    const std::string block = numbered(0, B);
    auto got = merge_of(numbered(900000, 2) + block + block, block + block);
    std::vector<Op> want = {{OP_DELETE_RUN, 2, ""}, {OP_MATCH_RUN, 2 * B, ""}};
    CHECK(got == want);
}

TEST(build_merge_seq_matches_the_kept_records_of_a_block_held_twice_and_edited_in_place) {
    // The block both files hold twice behind two deleted records, with 7 in
    // 10 of its records edited in place on the new side: the records after
    // an unchanged one's old copy still follow it better than those after
    // the cursor, so it waits for that copy and every unchanged record of
    // both copies stays a MATCH.
    constexpr uint32_t B = 12000;
    std::mt19937 rng(5);
    const auto edited = gctest::edited_in_place(rng, B, 0, B);
    const std::string block = numbered(0, B);
    std::string block_new = block;
    uint32_t n_edited = 0;
    for (uint32_t i = 0; i < B; i++)
        if (edited[i]) { block_new[size_t(i) * 12 + 4] = 'e'; n_edited++; }
    const std::string old_f = numbered(900000, 2) + block + block, new_f = block_new + block_new;
    size_t recs[4] = {};
    const std::string got = replay(old_f, build_merge_seq(old_f.data(), old_f.size(), new_f.data(), new_f.size(), 12), recs);

    CHECK(got == new_f);
    CHECK_EQ(recs[OP_MATCH_RUN], size_t(2 * (B - n_edited)));
    CHECK_EQ(recs[OP_DELETE_RUN], size_t(2 + 2 * n_edited));
    CHECK_EQ(recs[OP_INSERT_RUN], size_t(2 * n_edited));
}

TEST(build_merge_seq_sends_deaths_in_place_among_yesterdays_tombstones) {
    // Runs of 20 records that died in place, every 300 records, among runs
    // of 30 of yesterday's tombstones, every 1000, and records edited in
    // place, densely in [10000, 22000). A new tombstone at the cursor has
    // copies further on in both files, and the old ones are other slots'
    // tombstones: it goes in place of the record that died there, and every
    // slot the two days share stays a MATCH.
    constexpr uint32_t N = 100000;
    for (char fill : {'\0', '\xff'}) {
        std::mt19937 rng(91);
        const auto edited = gctest::edited_in_place(rng, N, 10000, 12000);
        std::string old_f = numbered(0, N), new_f = old_f;
        const std::string t = tombstones(1, fill);
        uint32_t kept = 0;
        for (uint32_t i = 0; i < N; i++) {
            if (i % 1000 < 30) { old_f.replace(size_t(i) * 12, 12, t); new_f.replace(size_t(i) * 12, 12, t); kept++; }
            else if (i % 300 < 20) new_f.replace(size_t(i) * 12, 12, t);
            else if (edited[i]) new_f[size_t(i) * 12 + 4] = 'e';
            else kept++;
        }
        size_t recs[4] = {};
        const std::string got = replay(old_f, build_merge_seq(old_f.data(), old_f.size(), new_f.data(), new_f.size(), 12), recs);

        CHECK(got == new_f);
        CHECK_EQ(recs[OP_MATCH_RUN], size_t(kept));
        CHECK_EQ(recs[OP_DELETE_RUN], size_t(N - kept));
        CHECK_EQ(recs[OP_INSERT_RUN], size_t(N - kept));
    }
}

TEST(build_merge_seq_sends_edited_records_as_one_delete_then_one_insert) {
    // Records sent in place of deleted ones join the INSERT after the DELETE
    // run, with a jump's records and with those the new file ends with: the
    // child streams pair the two runs.
    std::string body = "abcdefghijklmnopqrstuvwxyz0123";
    std::vector<Op> want = {{OP_MATCH_RUN, 3, ""}, {OP_DELETE_RUN, 2, ""}, {OP_INSERT_RUN, 2, records("DE")},
                            {OP_MATCH_RUN, 25, ""}};
    CHECK(record_merge(body, "abcDE" + body.substr(5)) == want);
    want = {{OP_MATCH_RUN, 3, ""}, {OP_DELETE_RUN, 2, ""}, {OP_INSERT_RUN, 4, records("DE+!")}, {OP_MATCH_RUN, 25, ""}};
    CHECK(record_merge(body, "abcDE+!" + body.substr(5)) == want);
    want = {{OP_MATCH_RUN, 28, ""}, {OP_DELETE_RUN, 2, ""}, {OP_INSERT_RUN, 3, records("XY!")}};
    CHECK(record_merge(body, body.substr(0, 28) + "XY!") == want);
}

TEST(for_each_merge_op_steps_over_inline_records) {
    std::string rec = records("pqr");
    MergeSequence s;
    s.add_match(2);
    s.add_insert(rec.data(), 1, 12);
    s.add_replace(rec.data() + 12, 2, 12);
    s.add_delete(3);
    std::vector<Op> got;
    for_each_merge_op(s, 12, [&](uint8_t op, uint32_t count, const char* recs) {
        got.push_back({op, count, recs ? std::string(recs, count * 12) : std::string()});
    });
    std::vector<Op> want = {{OP_MATCH_RUN, 2, ""}, {OP_INSERT_RUN, 1, records("p")},
                            {OP_REPLACE_RUN, 2, records("qr")}, {OP_DELETE_RUN, 3, ""}};
    CHECK(got == want);
}

TEST(identity_in_order_keeps_the_longest_increasing_run) {
    // New record 1 holds old entity 3 ahead of entities 1 and 2: the run
    // 0, 1, 2, 4 stays in order and entity 3 is the move.
    const uint32_t N = UINT32_MAX;
    auto got = identity_in_order({0, 3, 1, 2, N, 4});
    CHECK(got == std::vector<uint8_t>({1, 0, 1, 1, 0, 1}));
    CHECK(identity_in_order({}).empty());
    CHECK(identity_in_order({N, N}) == std::vector<uint8_t>({0, 0}));
}

TEST(build_identity_merge_seq_matches_replaces_inserts_and_deletes_by_identity) {
    // old a b c d e; new a d b C X e: d moved forward (INSERT plus a moved
    // pair), c changed in place (REPLACE), X is new, d's old slot is DELETEd.
    const uint32_t N = UINT32_MAX;
    std::string o = records("abcde"), n = records("adbCXe");
    std::vector<uint32_t> old_of_new = {0, 3, 1, 2, N, 4};
    auto in_order = identity_in_order(old_of_new);
    auto r = build_identity_merge_seq(o.data(), o.size(), n.data(), n.size(), 12, old_of_new, in_order);
    std::vector<Op> want = {{OP_MATCH_RUN, 1, ""},          {OP_INSERT_RUN, 1, records("d")},
                            {OP_MATCH_RUN, 1, ""},          {OP_REPLACE_RUN, 1, records("C")},
                            {OP_INSERT_RUN, 1, records("X")}, {OP_DELETE_RUN, 1, ""},
                            {OP_MATCH_RUN, 1, ""}};
    CHECK(ops_of(r.seq, 12) == want);
    CHECK(r.moved == (std::vector<std::pair<uint32_t, uint32_t>>{{3, 1}}));
    CHECK(r.matched_old == std::vector<uint8_t>({1, 1, 0, 0, 1}));
    CHECK_EQ(r.n_match, size_t(3));
    CHECK_EQ(r.n_replace, size_t(1));
    CHECK_EQ(r.n_insert, size_t(2));
    CHECK_EQ(r.n_delete, size_t(1));
}

TEST(build_identity_merge_seq_deletes_trailing_old_records) {
    std::string o = records("abc"), n = records("a");
    std::vector<uint32_t> old_of_new = {0};
    auto r = build_identity_merge_seq(o.data(), o.size(), n.data(), n.size(), 12, old_of_new,
                                      identity_in_order(old_of_new));
    std::vector<Op> want = {{OP_MATCH_RUN, 1, ""}, {OP_DELETE_RUN, 2, ""}};
    CHECK(ops_of(r.seq, 12) == want);
    CHECK(r.moved.empty());
}

TEST(merge_child_blocks_match_keeps_equal_blocks_and_resends_changed_ones) {
    MergeSequence parent;
    parent.add_match(2);
    Stream old_s{"AAAABBBB", {{0, 4}, {4, 4}}}, new_s{"AAAACCCC", {{0, 4}, {4, 4}}};
    auto got = ops_of(merge(parent, old_s, new_s), 1);
    std::vector<Op> want = {{OP_MATCH_RUN, 4, ""}, {OP_DELETE_RUN, 4, ""}, {OP_INSERT_RUN, 4, "CCCC"}};
    CHECK(got == want);
}

TEST(merge_child_blocks_edited_block_keeps_its_head_and_tail) {
    // One vertex moved inside a long ring: only the middle travels.
    MergeSequence parent;
    parent.add_match(1);
    Stream old_s{"headXXtail", {{0, 10}}}, new_s{"headYYYtail", {{0, 11}}};
    auto got = ops_of(merge(parent, old_s, new_s), 1);
    std::vector<Op> want = {{OP_MATCH_RUN, 4, ""}, {OP_DELETE_RUN, 2, ""}, {OP_INSERT_RUN, 3, "YYY"},
                            {OP_MATCH_RUN, 4, ""}};
    CHECK(got == want);
}

TEST(merge_child_blocks_trims_whole_units_only) {
    // Node streams keep whole 8-byte nodes: a node whose first byte matches
    // still travels whole.
    MergeSequence parent;
    parent.add_match(1);
    std::string o = std::string(8, 'a') + "bxxxxxxx" + std::string(8, 'c');
    std::string n = std::string(8, 'a') + "byyyyyyy" + std::string(8, 'c');
    std::vector<ChildBlock> ob = {{0, 24}}, nb = {{0, 24}};
    auto seq = merge_child_blocks(parent, PARENT_STRIDE, 8, o.data(), o.size(), n.data(), n.size(),
                                  [&](size_t i) { return ob[i]; }, [&](size_t i) { return nb[i]; });
    std::vector<Op> want = {{OP_MATCH_RUN, 1, ""}, {OP_DELETE_RUN, 1, ""}, {OP_INSERT_RUN, 1, "byyyyyyy"},
                            {OP_MATCH_RUN, 1, ""}};
    CHECK(ops_of(seq, 8) == want);
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

TEST(merge_child_blocks_replaced_records_keep_unchanged_blocks) {
    // A renamed road or re-tagged POI is a DELETE+INSERT of its record; its
    // geometry block is the same bytes and stays a MATCH.
    MergeSequence parent;
    parent.add_delete(3);
    parent.add_insert(PARENT, 2, PARENT_STRIDE);
    Stream old_s{"AAAABBBBZZ", {{0, 4}, {4, 4}, {8, 2}}}, new_s{"AAAACCCC", {{0, 4}, {4, 4}}};
    auto got = ops_of(merge(parent, old_s, new_s), 1);
    std::vector<Op> want = {{OP_MATCH_RUN, 4, ""}, {OP_DELETE_RUN, 6, ""}, {OP_INSERT_RUN, 4, "CCCC"}};
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

TEST(merge_child_blocks_replace_keeps_blocks_like_a_match) {
    MergeSequence parent;
    parent.add_replace(PARENT, 2, PARENT_STRIDE);
    parent.add_insert(PARENT, 1, PARENT_STRIDE);
    Stream old_s{"AAAAheadXXtail", {{0, 4}, {4, 10}}};
    Stream new_s{"AAAAheadYYYtailZZ", {{0, 4}, {4, 11}, {15, 2}}};
    auto got = ops_of(merge(parent, old_s, new_s), 1);
    std::vector<Op> want = {{OP_MATCH_RUN, 8, ""}, {OP_DELETE_RUN, 2, ""}, {OP_INSERT_RUN, 3, "YYY"},
                            {OP_MATCH_RUN, 4, ""}, {OP_INSERT_RUN, 2, "ZZ"}};
    CHECK(got == want);
}
