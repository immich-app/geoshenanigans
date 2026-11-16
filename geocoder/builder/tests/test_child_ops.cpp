// Unit tests for ChildOps (tools/child_ops.h): the section of a family's
// child stream, read where it sits in the patch.
#include "child_ops.h"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "test_framework.h"

using gctest::refusal_of;

namespace {

template <typename T>
void append(std::vector<char>& out, const T& v) {
    const char* p = reinterpret_cast<const char*>(&v);
    out.insert(out.end(), p, p + sizeof v);
}

// A section's first words as the diff writes them: its file, its stride or
// its kind, and the old and the new file's bytes.
std::vector<char> section_head(PatchFileId file, uint32_t stride, uint64_t old_size, uint64_t new_size) {
    std::vector<char> s;
    append(s, static_cast<uint32_t>(file));
    append(s, stride);
    append(s, old_size);
    append(s, new_size);
    return s;
}

// A merge section of seq: the head, the offset fixups' two counts and two
// lengths, the string fixups' count and length (fixups, none unless given),
// the sequence's length and the sequence.
std::vector<char> merge_section(PatchFileId file, uint32_t stride, uint64_t old_size, uint64_t new_size, const std::vector<char>& seq,
                                const std::vector<uint32_t>& fixups = {0, 0, 0, 0, 0, 0}) {
    std::vector<char> s = section_head(file, stride, old_size, new_size);
    for (const uint32_t w : fixups) append(s, w);
    append(s, static_cast<uint64_t>(seq.size()));
    s.insert(s.end(), seq.begin(), seq.end());
    return s;
}

// A COPY_OLD section, or a full replacement by bytes (declared as `declared`
// bytes when given).
std::vector<char> whole_section(PatchFileId file, uint32_t kind, uint64_t old_size, uint64_t new_size, const std::vector<char>& bytes = {},
                                uint64_t declared = UINT64_MAX) {
    std::vector<char> s = section_head(file, kind, old_size, new_size);
    append(s, uint32_t{0});
    append(s, declared == UINT64_MAX ? static_cast<uint64_t>(bytes.size()) : declared);
    s.insert(s.end(), bytes.begin(), bytes.end());
    return s;
}

// A run of a merge sequence: its op, its count, and an INSERT's or a
// REPLACE's bytes.
void run(std::vector<char>& seq, uint8_t op, uint32_t count, const std::vector<char>& bytes = {}) {
    append(seq, op);
    append(seq, count);
    seq.insert(seq.end(), bytes.begin(), bytes.end());
}

// n bytes counting up from first.
std::vector<char> bytes_from(char first, size_t n) {
    std::vector<char> b(n);
    for (size_t i = 0; i < n; i++) b[i] = static_cast<char>(first + static_cast<char>(i));
    return b;
}

// The section parsed at the start of a buffer that holds it alone, which the
// ops read in place: the buffer outlives them.
ChildOps parsed(PatchFileId file, uint32_t stride, const std::vector<char>& section, uint32_t max = FILE_RECORDS_MAX) {
    ChildOps c(file, stride);
    c.parse(section.data(), section.size(), 0, max);
    return c;
}
ChildOps parsed(PatchFileId, uint32_t, std::vector<char>&&, uint32_t = FILE_RECORDS_MAX) = delete;

// How parsing the section is refused; "" when it is not.
std::string parse_refusal(PatchFileId file, uint32_t stride, const std::vector<char>& section, uint32_t max = FILE_RECORDS_MAX) {
    return refusal_of([&] { parsed(file, stride, section, max); });
}

}  // namespace

// A merge sequence of a child whose ops count units of stride bytes (a node's
// 8, a vertex stream's 1): the ops come in bytes, a DELETE run and a run of
// nothing passed on the way, an INSERT's and a REPLACE's bytes where they sit
// in the patch, and old and written follow the bytes taken, in any pieces.
// The section's ends are where the next section starts.
TEST(child_ops_walk_a_merge_sequence_in_bytes_of_the_childs_stride) {
    struct Child {
        PatchFileId file;
        uint32_t stride;
        const char* name;
    };
    const Child children[] = {{PatchFileId::STREET_NODES, 8, "street_nodes.bin"},
                              {PatchFileId::INTERP_NODES, 8, "interp_nodes.bin"},
                              {PatchFileId::ADDR_VERTICES, 1, "addr_vertices.bin"},
                              {PatchFileId::POI_VERTICES, 1, "poi_vertices.bin"}};
    for (const Child& child : children) {
        const uint64_t u = child.stride;
        const std::vector<char> inserted = bytes_from('a', 1 * u), replaced = bytes_from('A', 2 * u);
        std::vector<char> seq;
        run(seq, OP_MATCH_RUN, 3);
        run(seq, OP_DELETE_RUN, 2);
        run(seq, OP_INSERT_RUN, 1, inserted);
        run(seq, OP_INSERT_RUN, 0);
        run(seq, OP_REPLACE_RUN, 2, replaced);
        run(seq, OP_MATCH_RUN, 0);
        run(seq, OP_MATCH_RUN, 2);
        run(seq, OP_DELETE_RUN, 1);
        // Behind 5 bytes of another section, another one after it.
        std::vector<char> patch = bytes_from('x', 5);
        const std::vector<char> section = merge_section(child.file, child.stride, 10 * u, 8 * u, seq);
        patch.insert(patch.end(), section.begin(), section.end());
        patch.insert(patch.end(), 9, 'y');

        ChildOps sut(child.file, child.stride);
        CHECK_EQ(sut.name, std::string(child.name));
        CHECK_EQ(sut.fid, static_cast<uint32_t>(child.file));
        CHECK_EQ(sut.stride, child.stride);
        sut.parse(patch.data(), patch.size(), 5, FILE_RECORDS_MAX);
        CHECK(sut.kind == ChildOps::MERGE);
        CHECK_EQ(sut.old_size, 10 * u);
        CHECK_EQ(sut.new_size, 8 * u);
        CHECK_EQ(sut.seq, size_t(5 + 24 + 24 + 8));
        CHECK_EQ(sut.seq_end, sut.seq + seq.size());
        CHECK_EQ(sut.section_end, patch.size() - 9);

        sut.at_bytes();
        CHECK(sut.op == OP_MATCH_RUN);
        CHECK_EQ(sut.left, 3 * u);
        CHECK_EQ(sut.old, uint64_t(0));
        sut.took(u + 1);
        sut.at_bytes();
        CHECK(sut.op == OP_MATCH_RUN);
        sut.took(sut.left);
        CHECK_EQ(sut.old, 3 * u);
        CHECK_EQ(sut.written, 3 * u);

        sut.at_bytes();
        CHECK(sut.op == OP_INSERT_RUN);
        CHECK_EQ(sut.left, 1 * u);
        CHECK_EQ(sut.old, 5 * u);
        CHECK(memcmp(patch.data() + sut.data, inserted.data(), inserted.size()) == 0);
        sut.took(sut.left);
        CHECK_EQ(sut.old, 5 * u);
        CHECK_EQ(sut.written, 4 * u);

        sut.at_bytes();
        CHECK(sut.op == OP_REPLACE_RUN);
        CHECK_EQ(sut.left, 2 * u);
        CHECK(memcmp(patch.data() + sut.data, replaced.data(), replaced.size()) == 0);
        sut.took(u);
        CHECK(memcmp(patch.data() + sut.data, replaced.data() + u, u) == 0);
        sut.took(u);
        CHECK_EQ(sut.old, 7 * u);
        CHECK_EQ(sut.written, 6 * u);

        sut.at_bytes();
        CHECK(sut.op == OP_MATCH_RUN);
        CHECK_EQ(sut.left, 2 * u);
        CHECK_EQ(sut.old, 7 * u);
        sut.took(sut.left);
        CHECK_EQ(sut.written, sut.new_size);

        sut.finish();
        CHECK_EQ(sut.old, sut.old_size);
        CHECK_EQ(refusal_of([&] { sut.at_bytes(); }), std::string(child.name) + " ends before its records do");
    }
}

// A child the patch keeps as the old stream is one MATCH of its old bytes,
// and one it sends whole is one INSERT of the section's bytes, which no old
// byte goes with; a stream of nothing has no op.
TEST(child_ops_read_an_unchanged_child_as_one_match_and_a_replaced_one_as_one_insert) {
    const PatchFileId file = PatchFileId::STREET_NODES;
    {
        std::vector<char> patch = whole_section(file, COPY_OLD_STRIDE, 72, 72);
        const size_t end = patch.size();
        patch.insert(patch.end(), 3, 'y');
        ChildOps sut(file, 8);
        sut.parse(patch.data(), patch.size(), 0, FILE_RECORDS_MAX);
        CHECK(sut.kind == ChildOps::COPY_OLD);
        CHECK_EQ(sut.old_size, uint64_t(72));
        CHECK_EQ(sut.section_end, end);
        sut.at_bytes();
        CHECK(sut.op == OP_MATCH_RUN);
        CHECK_EQ(sut.left, uint64_t(72));
        sut.took(72);
        CHECK_EQ(sut.old, uint64_t(72));
        CHECK_EQ(sut.written, uint64_t(72));
        sut.finish();
    }
    {
        const std::vector<char> bytes = bytes_from('a', 24);
        std::vector<char> patch = whole_section(file, 0, 40, 24, bytes);
        const size_t end = patch.size();
        patch.insert(patch.end(), 3, 'y');
        ChildOps sut(file, 8);
        sut.parse(patch.data(), patch.size(), 0, FILE_RECORDS_MAX);
        CHECK(sut.kind == ChildOps::FULL);
        CHECK_EQ(sut.section_end, end);
        sut.at_bytes();
        CHECK(sut.op == OP_INSERT_RUN);
        CHECK_EQ(sut.left, uint64_t(24));
        CHECK(memcmp(patch.data() + sut.data, bytes.data(), 24) == 0);
        sut.took(16);
        CHECK(memcmp(patch.data() + sut.data, bytes.data() + 16, 8) == 0);
        sut.took(8);
        CHECK_EQ(sut.old, uint64_t(0));
        CHECK_EQ(sut.written, uint64_t(24));
        sut.finish();
    }
    for (const uint32_t kind : {uint32_t{COPY_OLD_STRIDE}, uint32_t{0}}) {
        const std::vector<char> none = whole_section(file, kind, 0, 0);
        ChildOps sut = parsed(file, 8, none);
        sut.finish();
        CHECK_EQ(refusal_of([&] { sut.at_bytes(); }), std::string("street_nodes.bin ends before its records do"));
    }
    {
        const std::vector<char> copy = whole_section(file, COPY_OLD_STRIDE, 72, 72);
        ChildOps sut = parsed(file, 8, copy);
        sut.at_bytes();
        sut.took(64);
        CHECK_EQ(refusal_of([&] { sut.finish(); }), std::string("street_nodes.bin has bytes past its last record"));
    }
}

// What is not the child's section is refused by name as it is parsed: the
// section of another file, a merge at another stride than the child's or of
// another kind, one with offset fixups or string fixups, a full replacement
// that is not the new file's bytes, a copy of the old stream to a new one of
// another size, a file of more records than a patch indexes, and a section
// cut anywhere.
TEST(child_ops_refuse_a_section_that_is_not_the_childs) {
    const PatchFileId file = PatchFileId::STREET_NODES;
    std::vector<char> seq;
    run(seq, OP_MATCH_RUN, 9);
    const std::vector<char> good = merge_section(file, 8, 72, 72, seq);
    CHECK(parsed(file, 8, good).kind == ChildOps::MERGE);

    struct Case {
        std::string name;
        std::vector<char> section;
        std::string refusal;
    };
    const std::string kind = "Encoded family: unexpected section kind for a child stream";
    const std::string offset_fixups = "Encoded family: a child stream with offset fixups";
    const Case cases[] = {
        {"another file", merge_section(PatchFileId::INTERP_NODES, 8, 72, 72, seq),
         "Encoded family: street_nodes.bin does not follow its parent in the patch"},
        {"its parent", merge_section(PatchFileId::STREET_WAYS, 8, 72, 72, seq),
         "Encoded family: street_nodes.bin does not follow its parent in the patch"},
        {"bytes", merge_section(file, 1, 72, 72, seq), kind},
        {"records", merge_section(file, 12, 72, 72, seq), kind},
        {"sparse delta", merge_section(file, SPARSE_DELTA_STRIDE, 72, 72, seq), kind},
        {"parallel delta", merge_section(file, PARALLEL_DELTA_STRIDE, 72, 72, seq), kind},
        {"cell list delta", merge_section(file, CELL_LIST_DELTA_STRIDE, 72, 72, seq), kind},
        {"offset fixup runs", merge_section(file, 8, 72, 72, seq, {1, 0, 0, 0, 0, 0}), offset_fixups},
        {"offset fixup values", merge_section(file, 8, 72, 72, seq, {0, 1, 0, 0, 0, 0}), offset_fixups},
        {"offset fixup run bytes", merge_section(file, 8, 72, 72, seq, {0, 0, 4, 0, 0, 0}), offset_fixups},
        {"offset fixup value bytes", merge_section(file, 8, 72, 72, seq, {0, 0, 0, 4, 0, 0}), offset_fixups},
        {"string fixups", merge_section(file, 8, 72, 72, seq, {0, 0, 0, 0, 1, 0}), "Encoded family: a child stream with string fixups"},
        {"string fixup bytes", merge_section(file, 8, 72, 72, seq, {0, 0, 0, 0, 0, 4}), "Encoded family: a child stream with string fixups"},
        {"short replacement", whole_section(file, 0, 72, 72, bytes_from('a', 64)), "Malformed child full replacement"},
        {"long replacement", whole_section(file, 0, 72, 72, bytes_from('a', 80)), "Malformed child full replacement"},
        {"longer copy", whole_section(file, COPY_OLD_STRIDE, 72, 80), "Malformed copy-old section: street_nodes.bin is 72 bytes old and 80 new"},
        {"shorter copy", whole_section(file, COPY_OLD_STRIDE, 72, 64), "Malformed copy-old section: street_nodes.bin is 72 bytes old and 64 new"},
    };
    for (const Case& c : cases) {
        const std::string got = parse_refusal(file, 8, c.section);
        if (got != c.refusal) std::printf("    %s: %s\n", c.name.c_str(), got.c_str());
        CHECK_EQ(got, c.refusal);
    }
    CHECK_EQ(parse_refusal(file, 8, good, 8), std::string("Old street_nodes.bin holds 9 records, more than the 8 a patch can index"));
    CHECK_EQ(parse_refusal(file, 8, merge_section(file, 8, 64, 72, seq), 8),
             std::string("New street_nodes.bin holds 9 records, more than the 8 a patch can index"));
    CHECK(parsed(file, 8, good, 9).kind == ChildOps::MERGE);

    const std::vector<char> copy = whole_section(file, COPY_OLD_STRIDE, 72, 72), full = whole_section(file, 0, 72, 16, bytes_from('a', 16));
    for (const std::vector<char>* whole : {&good, &copy, &full}) {
        size_t refused = 0;
        for (size_t cut = 0; cut < whole->size(); cut++) {
            const std::vector<char> part(whole->begin(), whole->begin() + static_cast<std::ptrdiff_t>(cut));
            refused += parse_refusal(file, 8, part) == "Truncated patch: child section";
        }
        CHECK_EQ(refused, whole->size());
    }
    // A full replacement that declares bytes past the patch.
    CHECK_EQ(parse_refusal(file, 8, whole_section(file, 0, 72, 16, bytes_from('a', 16), uint64_t{1} << 40)),
             std::string("Truncated patch: child section"));
}

// A malformed merge sequence is refused by the child's name when its ops are
// reached: an op cut short, an INSERT or a REPLACE of more bytes than the
// sequence holds (a count whose bytes pass 32 bits among them: 2^29 nodes are
// 2^32 bytes, not none), an op no sequence has; and bytes the records leave
// behind when the walk ends.
TEST(child_ops_refuse_a_malformed_merge_sequence_as_they_reach_it) {
    const PatchFileId file = PatchFileId::STREET_NODES;
    const std::string malformed = "Malformed merge sequence: street_nodes.bin";
    struct Case {
        std::string name;
        std::vector<char> seq;
    };
    std::vector<Case> cases;
    auto add = [&](const std::string& name, uint8_t op, uint32_t count, size_t payload, size_t cut = 0) {
        std::vector<char> seq;
        run(seq, OP_MATCH_RUN, 1);
        run(seq, op, count, bytes_from('a', payload));
        seq.resize(seq.size() - cut);
        cases.push_back({name, seq});
    };
    add("an op cut short", OP_MATCH_RUN, 1, 0, 2);
    add("an insert past the sequence", OP_INSERT_RUN, 2, 8);
    add("a replace past the sequence", OP_REPLACE_RUN, 3, 16);
    add("an insert of 2^32 bytes", OP_INSERT_RUN, uint32_t{1} << 29, 8);
    add("a replace of 2^32 bytes", OP_REPLACE_RUN, uint32_t{1} << 29, 8);
    add("an unknown op", 4, 1, 8);
    for (const Case& c : cases) {
        const std::vector<char> section = merge_section(file, 8, 800, 800, c.seq);
        ChildOps sut = parsed(file, 8, section);
        sut.at_bytes();
        CHECK(sut.op == OP_MATCH_RUN);
        sut.took(8);
        const std::string got = refusal_of([&] { sut.at_bytes(); });
        if (got != malformed) std::printf("    %s: %s\n", c.name.c_str(), got.c_str());
        CHECK_EQ(got, malformed);
        ChildOps at_end = parsed(file, 8, section);
        at_end.at_bytes();
        at_end.took(8);
        CHECK_EQ(refusal_of([&] { at_end.finish(); }), malformed);
    }

    std::vector<char> seq;
    run(seq, OP_MATCH_RUN, 1);
    run(seq, OP_DELETE_RUN, 4);
    run(seq, OP_INSERT_RUN, 1, bytes_from('a', 8));
    const std::vector<char> section = merge_section(file, 8, 40, 16, seq);
    ChildOps sut = parsed(file, 8, section);
    sut.at_bytes();
    sut.took(8);
    CHECK_EQ(refusal_of([&] { sut.finish(); }), std::string("street_nodes.bin has bytes past its last record"));
    sut.took(8);
    sut.finish();
    CHECK_EQ(sut.old, uint64_t(40));
}
