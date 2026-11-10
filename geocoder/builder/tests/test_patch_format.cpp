// Unit tests for the pure helpers in patch_format.h.
// These lock in the CURRENT behaviour of the varint codec, the grid
// fingerprint quantiser, and the stride/marker sentinel constants so a future
// refactor of the patch tooling can't silently shift them (the constants are
// load-bearing: diff and patch must agree on them byte-for-byte).
#include "cell_index_io.h"
#include "patch_format.h"
#include "scratch_dir.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <map>
#include <numeric>
#include <optional>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "sequential_file_reader.h"
#include "test_framework.h"
#include "types.h"

using gctest::refusal_of;

// --- write_varint / SectionReader::varint round-trip ---

static uint32_t varint_roundtrip(uint32_t value) {
    std::vector<char> buf;
    write_varint(buf, value);
    size_t pos = 0;
    uint32_t got = SectionReader(buf.data(), buf.size(), pos, "varint").varint();
    // read must consume exactly the bytes that write produced.
    return (pos == buf.size()) ? got : 0xDEADBEEFu;
}

TEST(patch_format_varint_roundtrip_zero_and_small) {
    CHECK_EQ(varint_roundtrip(0u), 0u);
    CHECK_EQ(varint_roundtrip(1u), 1u);
    CHECK_EQ(varint_roundtrip(2u), 2u);
    CHECK_EQ(varint_roundtrip(63u), 63u);
    CHECK_EQ(varint_roundtrip(127u), 127u);   // last 1-byte value
    CHECK_EQ(varint_roundtrip(128u), 128u);   // first 2-byte value
    CHECK_EQ(varint_roundtrip(129u), 129u);
}

TEST(patch_format_varint_roundtrip_boundaries) {
    // 7-bit group boundaries: each is the last n-byte / first (n+1)-byte value.
    CHECK_EQ(varint_roundtrip(127u), 127u);
    CHECK_EQ(varint_roundtrip(128u), 128u);
    CHECK_EQ(varint_roundtrip(16383u), 16383u);   // 2^14 - 1
    CHECK_EQ(varint_roundtrip(16384u), 16384u);
    CHECK_EQ(varint_roundtrip(2097151u), 2097151u);   // 2^21 - 1
    CHECK_EQ(varint_roundtrip(2097152u), 2097152u);
    CHECK_EQ(varint_roundtrip(268435455u), 268435455u);  // 2^28 - 1
    CHECK_EQ(varint_roundtrip(268435456u), 268435456u);
}

TEST(patch_format_varint_roundtrip_large) {
    CHECK_EQ(varint_roundtrip(1000000u), 1000000u);
    CHECK_EQ(varint_roundtrip(0x7FFFFFFFu), 0x7FFFFFFFu);
    CHECK_EQ(varint_roundtrip(0x80000000u), 0x80000000u);
    CHECK_EQ(varint_roundtrip(0xFFFFFFFEu), 0xFFFFFFFEu);
    CHECK_EQ(varint_roundtrip(0xFFFFFFFFu), 0xFFFFFFFFu);  // u32 max
}

TEST(patch_format_varint_byte_lengths) {
    // The encoded length follows the standard LEB128 grouping: ceil(bits/7),
    // with at least 1 byte for value 0.
    auto enc_len = [](uint32_t v) {
        std::vector<char> buf;
        write_varint(buf, v);
        return buf.size();
    };
    CHECK_EQ(enc_len(0u), size_t(1));
    CHECK_EQ(enc_len(127u), size_t(1));
    CHECK_EQ(enc_len(128u), size_t(2));
    CHECK_EQ(enc_len(16383u), size_t(2));
    CHECK_EQ(enc_len(16384u), size_t(3));
    CHECK_EQ(enc_len(2097151u), size_t(3));
    CHECK_EQ(enc_len(2097152u), size_t(4));
    CHECK_EQ(enc_len(268435455u), size_t(4));
    CHECK_EQ(enc_len(268435456u), size_t(5));
    CHECK_EQ(enc_len(0xFFFFFFFFu), size_t(5));  // u32 max needs 5 bytes
}

TEST(patch_format_varint_continuation_bits) {
    // Multi-byte encodings set the high (continuation) bit on every byte
    // except the final one. Verify on 128 -> {0x80, 0x01}.
    std::vector<char> buf;
    write_varint(buf, 128u);
    CHECK_EQ(buf.size(), size_t(2));
    CHECK_EQ(static_cast<uint8_t>(buf[0]), uint8_t(0x80));
    CHECK_EQ(static_cast<uint8_t>(buf[1]), uint8_t(0x01));
    // Value 1 is a single byte with the continuation bit clear.
    std::vector<char> buf1;
    write_varint(buf1, 1u);
    CHECK_EQ(buf1.size(), size_t(1));
    CHECK_EQ(static_cast<uint8_t>(buf1[0]), uint8_t(0x01));
}

TEST(patch_format_varint_sequence_in_one_buffer) {
    // Multiple varints packed back-to-back decode in order, each advancing pos.
    std::vector<uint32_t> vals = {0u, 130u, 5u, 300000u, 0xFFFFFFFFu, 64u};
    std::vector<char> buf;
    for (uint32_t v : vals) write_varint(buf, v);
    size_t pos = 0;
    SectionReader in(buf.data(), buf.size(), pos, "varint");
    for (uint32_t v : vals) {
        CHECK_EQ(in.varint(), v);
    }
    CHECK_EQ(pos, buf.size());
}

// A part of the patch whose faults are all malformed (as the runs are), read
// from data[pos, size).
static SectionReader runs_reader(const char* data, size_t size, size_t& pos) {
    return SectionReader(data, size, pos, "runs", PastEnd::MALFORMED);
}

TEST(patch_format_bounded_varint_rejects_truncated_and_overlong_values) {
    std::vector<char> buf;
    write_varint(buf, 300000u);
    for (size_t cut = 0; cut < buf.size(); cut++) {
        size_t pos = 0;
        CHECK_EQ(refusal_of([&] { runs_reader(buf.data(), cut, pos).varint(); }), std::string("Malformed runs"));
    }
    size_t pos = 0;
    CHECK_EQ(runs_reader(buf.data(), buf.size(), pos).varint(), 300000u);
    CHECK_EQ(pos, buf.size());
    const char overlong[6] = {'\x80', '\x80', '\x80', '\x80', '\x80', '\x01'};
    pos = 0;
    CHECK_EQ(refusal_of([&] { runs_reader(overlong, sizeof(overlong), pos).varint(); }), std::string("Malformed runs"));
}

TEST(patch_format_bounded_varint_rejects_bits_past_32) {
    // A 5th byte holds bits 28..31; anything above is 2^32 or more, which
    // must not wrap to a small value (5 + 2^32 would read as 5).
    const char too_big[5] = {'\x85', '\x80', '\x80', '\x80', '\x10'};
    size_t pos = 0;
    CHECK_EQ(refusal_of([&] { runs_reader(too_big, sizeof(too_big), pos).varint(); }), std::string("Malformed runs"));
    std::vector<char> max;
    write_varint(max, 0xFFFFFFFFu);
    pos = 0;
    CHECK_EQ(runs_reader(max.data(), max.size(), pos).varint(), 0xFFFFFFFFu);
}

// --- write_varint64 / SectionReader::varint64 ---

TEST(patch_format_varint64_round_trips_every_length) {
    // Each 7-bit boundary (the last value of n bytes, the first of n + 1), up to the 10-byte 2^64 - 1.
    std::vector<uint64_t> values = {0, 1, 127, 128, 300000, 0xFFFFFFFFull, 0x100000000ull, ~0ull};
    for (unsigned bits = 7; bits < 64; bits += 7) {
        values.push_back((uint64_t{1} << bits) - 1);
        values.push_back(uint64_t{1} << bits);
    }
    for (const uint64_t v : values) {
        std::vector<char> buf;
        write_varint64(buf, v);
        size_t pos = 0;
        CHECK_EQ(SectionReader(buf.data(), buf.size(), pos, "varint").varint64(), v);
        CHECK_EQ(pos, buf.size());
    }
    std::vector<char> max;
    write_varint64(max, ~0ull);
    CHECK_EQ(max.size(), size_t(10));
}

TEST(patch_format_varint64_rejects_truncated_overlong_and_values_past_64_bits) {
    std::vector<char> buf;
    write_varint64(buf, 0x123456789ABCDEFull);
    for (size_t cut = 0; cut < buf.size(); cut++) {
        size_t pos = 0;
        CHECK_EQ(refusal_of([&] { runs_reader(buf.data(), cut, pos).varint64(); }), std::string("Malformed runs"));
    }
    // An 11th byte, and a 10th byte holding a bit past bit 63 (2^64 + 5 would read as 5).
    const char overlong[11] = {'\x80', '\x80', '\x80', '\x80', '\x80', '\x80', '\x80', '\x80', '\x80', '\x80', '\x01'};
    const char too_big[10] = {'\x85', '\x80', '\x80', '\x80', '\x80', '\x80', '\x80', '\x80', '\x80', '\x02'};
    for (const auto& [bytes, n] : {std::pair<const char*, size_t>{overlong, sizeof overlong}, {too_big, sizeof too_big}}) {
        size_t pos = 0;
        CHECK_EQ(refusal_of([&] { runs_reader(bytes, n, pos).varint64(); }), std::string("Malformed runs"));
    }
}

// --- SectionReader ---

// One read of each kind, and the bytes it reads.
struct PartRead {
    std::vector<char> bytes;
    std::function<void(SectionReader&)> read;
};

static std::vector<PartRead> part_reads() {
    std::vector<char> varint;
    write_varint(varint, 0xFFFFFFFFu);
    return {
        {std::vector<char>(1, 'x'), [](SectionReader& in) { in.u8(); }},
        {std::vector<char>(2, 'x'), [](SectionReader& in) { in.u16(); }},
        {std::vector<char>(4, 'x'), [](SectionReader& in) { in.u32(); }},
        {std::vector<char>(8, 'x'), [](SectionReader& in) { in.u64(); }},
        {std::vector<char>(5, 'x'), [](SectionReader& in) { in.take(5); }},
        {std::vector<char>(12, 'x'), [](SectionReader& in) { in.take_items(3, 4); }},
        {varint, [](SectionReader& in) { in.varint(); }},
        {{'a', 'b', '\0'}, [](SectionReader& in) { in.cstring(); }},
    };
}

// Three bytes already read, then the read's bytes: none of the arithmetic
// starts at offset 0.
static std::vector<char> behind_three_bytes(const std::vector<char>& bytes) {
    std::vector<char> part = {'p', 'r', 'e'};
    part.insert(part.end(), bytes.begin(), bytes.end());
    return part;
}

TEST(section_reader_reads_a_part_to_its_exact_end) {
    for (const PartRead& r : part_reads()) {
        const std::vector<char> part = behind_three_bytes(r.bytes);
        size_t pos = 3;
        SectionReader in(part.data(), part.size(), pos, "test part");
        CHECK_EQ(refusal_of([&] { in.require(r.bytes.size()); r.read(in); }), std::string());
        CHECK_EQ(pos, part.size());
        CHECK(in.at_end());
        CHECK_EQ(in.left(), size_t{0});
        // Nothing is still there to read.
        CHECK_EQ(refusal_of([&] {
            in.require(0);
            CHECK(in.take(0) == part.data() + part.size());
            in.take_items(0, 8);
        }), std::string());
        CHECK_EQ(pos, part.size());
    }
}

TEST(section_reader_refuses_a_read_one_byte_past_its_end) {
    for (const PartRead& r : part_reads()) {
        std::vector<char> part = behind_three_bytes(r.bytes);
        part.pop_back();
        size_t pos = 3;
        SectionReader in(part.data(), part.size(), pos, "test part");
        CHECK_EQ(refusal_of([&] { in.require(r.bytes.size()); }), std::string("Truncated test part"));
        CHECK_EQ(pos, size_t{3});
        CHECK_EQ(refusal_of([&] { r.read(in); }), std::string("Truncated test part"));
    }
}

TEST(section_reader_refuses_lengths_that_wrap) {
    const std::vector<char> part(16, 'x');
    size_t pos = 8;
    SectionReader in(part.data(), part.size(), pos, "test part");
    const uint64_t wrapping[] = {
        UINT64_MAX,
        UINT64_MAX - 7,                // pos + n wraps to 0
        uint64_t{SIZE_MAX} - 7,        // and in size_t
        (uint64_t{1} << 32) + 4,       // a 32-bit size_t narrows it to 4
    };
    for (uint64_t n : wrapping) {
        CHECK_EQ(refusal_of([&] { in.require(n); }), std::string("Truncated test part"));
        CHECK_EQ(refusal_of([&] { in.take(n); }), std::string("Truncated test part"));
    }
    // Counts whose bytes pass 32 bits: (2^30 + 1) * 4 is 2^32 + 4.
    CHECK_EQ(refusal_of([&] { in.take_items((1u << 30) + 1, 4); }), std::string("Truncated test part"));
    CHECK_EQ(refusal_of([&] { in.take_items(UINT32_MAX, 8); }), std::string("Truncated test part"));
    CHECK_EQ(pos, size_t{8});
    CHECK(in.take(8) == part.data() + 8);

    // Past the part's end, size - pos wraps: not even nothing is there to read.
    size_t past = part.size() + 1;
    SectionReader beyond(part.data(), part.size(), past, "test part");
    CHECK_EQ(beyond.left(), size_t{0});
    CHECK(!beyond.at_end());
    CHECK_EQ(refusal_of([&] { beyond.require(0); }), std::string("Truncated test part"));
    CHECK_EQ(refusal_of([&] { beyond.take(0); }), std::string("Truncated test part"));
    CHECK_EQ(refusal_of([&] { beyond.varint(); }), std::string("Truncated test part"));
    CHECK_EQ(refusal_of([&] { beyond.cstring(); }), std::string("Truncated test part"));
    CHECK_EQ(past, part.size() + 1);
}

TEST(section_reader_words_its_refusals_as_past_end_says) {
    const std::vector<char> part(6, 'x');
    auto refusal = [&](PastEnd past_end, const std::function<void(SectionReader&)>& read) {
        size_t pos = 2;
        SectionReader in(part.data(), part.size(), pos, "test part", past_end);
        return refusal_of([&] { read(in); });
    };
    auto past = [](SectionReader& in) { in.take(5, "the field"); };
    CHECK_EQ(refusal(PastEnd::TRUNCATED, past), std::string("Truncated test part"));
    CHECK_EQ(refusal(PastEnd::MALFORMED, past), std::string("Malformed test part"));
    CHECK_EQ(refusal(PastEnd::TRUNCATED_AT, past), std::string("Truncated test part: need 5 bytes for the field at offset 2 (size 6)"));
    CHECK_EQ(refusal(PastEnd::TRUNCATED_AT, [](SectionReader& in) { in.u64(); }),
             std::string("Truncated test part: need 8 bytes for u64 at offset 2 (size 6)"));
    // Its other faults are malformed however a part words its end.
    const std::vector<char> overlong = {'\x80', '\x80', '\x80', '\x80', '\x80', '\x01'};
    for (PastEnd past_end : {PastEnd::TRUNCATED, PastEnd::MALFORMED, PastEnd::TRUNCATED_AT}) {
        CHECK_EQ(refusal(past_end, [](SectionReader& in) { in.malformed(); }), std::string("Malformed test part"));
        size_t pos = 0;
        SectionReader in(overlong.data(), overlong.size(), pos, "test part", past_end);
        CHECK_EQ(refusal_of([&] { in.varint(); }), std::string("Malformed test part"));
    }
}

// --- to_grid quantiser ---

TEST(patch_format_to_grid_basic) {
    // Multiplies by 1e5 and rounds half away from zero.
    CHECK_EQ(to_grid(0.0f), 0);
    CHECK_EQ(to_grid(1.0f), 100000);
    CHECK_EQ(to_grid(-1.0f), -100000);
    CHECK_EQ(to_grid(0.00001f), 1);
    CHECK_EQ(to_grid(-0.00001f), -1);
}

TEST(patch_format_to_grid_rounding_half_away_from_zero) {
    // 0.000015 * 1e5 = 1.5 -> +0.5 -> truncate to 2 (away from zero).
    CHECK_EQ(to_grid(0.000015f), 2);
    // -0.000015 * 1e5 = -1.5 -> -0.5 -> truncate to -2 (away from zero).
    CHECK_EQ(to_grid(-0.000015f), -2);
    // typical coordinate
    CHECK_EQ(to_grid(51.5074f), 5150740);
    CHECK_EQ(to_grid(-0.1278f), -12780);
}

// --- Stride sentinel constants ---

TEST(patch_format_stride_sentinel_values) {
    // Concrete values locked in (diff/patch wire contract), distinct.
    CHECK_EQ(SPARSE_DELTA_STRIDE, uint32_t(0xFC));
    CHECK_EQ(COPY_OLD_STRIDE, uint32_t(0xFD));
    CHECK_EQ(CELL_LIST_DELTA_STRIDE, uint32_t(0xFB));
}

TEST(patch_format_stride_sentinels_no_collision_with_real_strides) {
    // Real per-record strides used across the format (the record files, the
    // *_cells and entry lists). None may equal a sentinel, else a genuine
    // record stride would be misread as a sparse/copy/cell list section, and
    // none passes the widest stride the patcher takes.
    const uint32_t real_strides[] = {
        1,   // vertex byte streams
        2,   // entry count prefix (u16)
        4,   // u32 id arrays (entries, parents, postcodes)
        8,   // NodeCoord, cell_id
        12,  // WayHeader / admin_cells / poi_cells
        16,  // postcode_centroid value_stride, empty addr_points
        20,  // PlaceNode / geo_cell
        24,  // InterpWay / AdminPolygon
        28,  // AddrPoint
        36,  // PoiRecord
    };
    for (uint32_t s : real_strides) {
        CHECK(s != SPARSE_DELTA_STRIDE);
        CHECK(s != COPY_OLD_STRIDE);
        CHECK(s != CELL_LIST_DELTA_STRIDE);
        CHECK(s <= MAX_RECORD_STRIDE);
    }
}

// --- Section marker constants ---

TEST(patch_format_section_marker_values) {
    // Concrete values locked in (diff/patch wire contract).
    CHECK_EQ(CELL_CHANGES_GEO_MARKER, uint32_t(0xFFFFFFFB));
    CHECK_EQ(CELL_CHANGES_ADMIN_MARKER, uint32_t(0xFFFFFFFA));
    CHECK_EQ(CELL_CHANGES_POI_MARKER, uint32_t(0xFFFFFFF5));
    CHECK_EQ(GEO_ENTRY_DELTA_MARKER, uint32_t(0xFFFFFFF0));
    CHECK_EQ(CELL_INDEX_DELTA_MARKER, uint32_t(0xFFFFFFF7));
    CHECK_EQ(SECONDARY_REMAP_MARKER, uint32_t(0xFFFFFFF6));
    CHECK_EQ(POI_PARENT_REMAP_MARKER, uint32_t(0xFFFFFFF3));
    CHECK_EQ(CLIENT_FILES_MARKER, uint32_t(0xFFFFFFF2));
}

// --- Format identity / enum constants ---

TEST(patch_format_magic_and_version) {
    // GCPATCH_VERSION is bound to the value actually emitted/checked by the
    // diff/patch tools. v15 = cell list deltas as runs (v14: admin geometry as
    // arcs, merged by slot). The patcher accepts this version only.
    CHECK_EQ(GCPATCH_VERSION, uint32_t(15));
    const char expect[8] = {'G','C','P','A','T','C','H','\0'};
    for (int i = 0; i < 8; i++) CHECK_EQ(GCPATCH_MAGIC[i], expect[i]);
}

TEST(patch_format_fileid_count_and_names_aligned) {
    // The names table must have exactly COUNT entries (one per PatchFileId).
    CHECK_EQ(static_cast<uint32_t>(PatchFileId::COUNT), uint32_t(36));
    const size_t n_names = sizeof(patch_file_names) / sizeof(patch_file_names[0]);
    CHECK_EQ(n_names, size_t(36));
    // Spot-check that index lines up with the enum value.
    CHECK(std::string(patch_file_names[static_cast<uint32_t>(PatchFileId::STRINGS)]) == "strings.bin");
    CHECK(std::string(patch_file_names[static_cast<uint32_t>(PatchFileId::POI_RECORDS)]) == "poi_records.bin");
    CHECK(std::string(patch_file_names[static_cast<uint32_t>(PatchFileId::STRINGS_POI)]) == "strings_poi.bin");
    CHECK(std::string(patch_file_names[static_cast<uint32_t>(PatchFileId::INTERP_POSTCODES)]) == "interp_postcodes.bin");
    // The geo index's two files; its lists by table are no files.
    CHECK(std::string(patch_file_names[static_cast<uint32_t>(PatchFileId::GEO_CELLS)]) == "geo_cells.bin");
    CHECK(std::string(patch_file_names[static_cast<uint32_t>(PatchFileId::GEO_ENTRIES)]) == "geo_entries.bin");
    for (PatchFileId fid : GEO_ENTRIES_FILES) CHECK(!has_suffix(patch_file_names[static_cast<uint32_t>(fid)], ".bin"));
}

// --- node_byte_offset ---

TEST(patch_format_node_byte_offset_past_2_pow_29) {
    // Planet street_nodes holds ~600M 8-byte nodes; 32-bit `idx * 8`
    // wraps at 2^29 and reads the wrong coordinates.
    CHECK_EQ(node_byte_offset(0u), (size_t)0);
    CHECK_EQ(node_byte_offset(1u), (size_t)8);
    CHECK_EQ(node_byte_offset(1u << 29), (size_t)1 << 32);
    CHECK_EQ(node_byte_offset(0xFFFFFFFFu), (size_t)0xFFFFFFFFu * 8);
}

// --- record layouts ---

TEST(patch_format_record_strides_match_the_builder) {
    CHECK_EQ(WAY_HEADER_STRIDE, sizeof(WayHeader));
    CHECK_EQ(ADDR_POINT_STRIDE, sizeof(AddrPoint));
    CHECK_EQ(INTERP_WAY_STRIDE, sizeof(InterpWay));
    CHECK_EQ(ADMIN_POLYGON_STRIDE, sizeof(AdminPolygon));
    CHECK_EQ(POI_RECORD_STRIDE, sizeof(PoiRecord));
    CHECK_EQ(PLACE_NODE_STRIDE, sizeof(PlaceNode));
}

TEST(patch_format_record_node_count_reads_u16) {
    // WayHeader / InterpWay: u16 at byte 4, so roads past 255 nodes keep
    // their full count.
    char way[12] = {};
    uint16_t n = 1358;
    std::memcpy(way + 4, &n, 2);
    CHECK_EQ(record_node_count(way), 1358u);
}

TEST(patch_format_template_patch_version_matches_gcpatch_version) {
    // Clients compare build.patch_version to decide between patching and a
    // fresh install, so it must name the format the tools emit.
    std::ifstream f(GEOCODER_TEMPLATE_PATH);
    REQUIRE(f.good());
    std::string json((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    const std::string key = "\"patch_version\":";
    size_t at = json.find(key);
    REQUIRE(at != std::string::npos);
    CHECK_EQ(static_cast<uint32_t>(std::strtoul(json.c_str() + at + key.size(), nullptr, 10)), GCPATCH_VERSION);
}

// --- client file set ---

TEST(patch_format_client_file_names) {
    struct Case { const char* name; bool client; bool inline_; };
    const Case cases[] = {
        {"street_ways.bin", true, false},
        {"strings_layout.json", true, true},
        {"poi_meta.json", true, true},
        {"street_ways.osm_ids", false, false},
        {"patch.gcpatch", false, false},
        {"street_ways.bin.zst", false, false},
        {".hidden.bin", false, false},
        {"", false, false},
        {"sub/escape.bin", false, false},
    };
    for (const auto& c : cases) {
        CHECK_EQ(is_client_file(c.name), c.client);
        if (c.client) CHECK_EQ(is_inline_client_file(c.name), c.inline_);
    }
}

static const char SAMPLE_LAYOUT[] = "{ }\n";

// Sorted by name, as the diff lists them.
static std::vector<ClientFile> sample_client_files() {
    ClientFile ways{"street_ways.bin", 12345678901ull, nullptr};
    ClientFile layout{"strings_layout.json", 4, SAMPLE_LAYOUT};
    return {ways, layout};
}

// An inline file's bytes are read where they sit: the layout's are the
// section's last four.
TEST(patch_format_client_files_round_trip) {
    std::vector<char> buf = {'x'};  // the section is read by position
    append_client_files(buf, sample_client_files());
    size_t pos = 1;
    auto got = parse_client_files(buf.data(), buf.size(), pos);
    CHECK_EQ(pos, buf.size());
    REQUIRE(got.size() == 2);
    CHECK(got[0].name == "street_ways.bin");
    CHECK_EQ(got[0].size, uint64_t(12345678901ull));
    CHECK(got[0].bytes == nullptr);
    CHECK(got[1].name == "strings_layout.json");
    CHECK_EQ(got[1].size, uint64_t(4));
    CHECK(got[1].bytes == buf.data() + buf.size() - 4);
    CHECK(std::string(got[1].bytes, 4) == SAMPLE_LAYOUT);
}

TEST(patch_format_client_files_round_trip_an_empty_inline_file) {
    std::vector<char> buf;
    append_client_files(buf, {ClientFile{"poi_meta.json", 0, nullptr}});
    size_t pos = 0;
    auto got = parse_client_files(buf.data(), buf.size(), pos);
    CHECK_EQ(pos, buf.size());
    REQUIRE(got.size() == 1);
    CHECK(got[0].name == "poi_meta.json");
    CHECK_EQ(got[0].size, uint64_t(0));
    CHECK(got[0].bytes == buf.data() + buf.size());
}

// What parse_client_files refused buf with, or "".
static std::string client_files_refusal(const std::vector<char>& buf) {
    size_t pos = 0;
    return refusal_of([&] { parse_client_files(buf.data(), buf.size(), pos); });
}

TEST(patch_format_client_files_rejects_missing_truncated_and_bad_names) {
    std::vector<char> good;
    append_client_files(good, sample_client_files());

    std::vector<char> no_marker(good);
    no_marker[0] ^= 1;
    CHECK_EQ(client_files_refusal(no_marker), std::string("Patch has no client file list"));
    CHECK_EQ(client_files_refusal(std::vector<char>(good.begin(), good.end() - 1)), std::string("Truncated client file list"));

    std::vector<char> escape;
    append_client_files(escape, {ClientFile{"x.bin", 0, nullptr}});
    const size_t name_at = 4 + 4 + 2;  // marker, n, name_len
    escape[name_at] = '/';
    CHECK_EQ(client_files_refusal(escape), std::string("Bad client file name: /.bin"));
}

// The section naming names as given, each an empty .bin file: the bytes
// append_client_files writes for a list it takes, and those of a list it
// refuses.
static std::vector<char> raw_client_files(const std::vector<std::string>& names) {
    std::vector<char> buf;
    auto put = [&](const void* p, size_t n) { buf.insert(buf.end(), (const char*)p, (const char*)p + n); };
    const uint32_t marker = CLIENT_FILES_MARKER, n = static_cast<uint32_t>(names.size());
    put(&marker, 4);
    put(&n, 4);
    for (const std::string& name : names) {
        const uint16_t len = static_cast<uint16_t>(name.size());
        const uint64_t size = 0;
        const uint8_t is_inline = 0;
        put(&len, 2);
        put(name.data(), len);
        put(&size, 8);
        put(&is_inline, 1);
    }
    return buf;
}

// 1025 empty .bin files: one more than a list may name.
static std::vector<std::string> too_many_client_file_names() {
    std::vector<std::string> names;
    for (int i = 0; i < 1025; i++) {
        char name[16];
        std::snprintf(name, sizeof name, "f%04d.bin", i);
        names.push_back(name);
    }
    return names;
}

// A list names at most 1024 files (MAX_CLIENT_FILES), each name at most 255
// bytes long (a file name's), sorted and each once: the patcher keeps every
// name, so a list may not make it hold more.
TEST(patch_format_client_files_refuse_too_many_a_long_name_or_one_out_of_order) {
    const std::vector<std::string> names = too_many_client_file_names();
    const std::string longest = std::string(251, 'a') + ".bin";
    CHECK_EQ(client_files_refusal(raw_client_files({names.begin(), names.begin() + 1024})), std::string());
    CHECK_EQ(client_files_refusal(raw_client_files(names)), std::string("Too many client files: 1025"));
    CHECK_EQ(client_files_refusal(raw_client_files({longest})), std::string());
    CHECK_EQ(client_files_refusal(raw_client_files({"a" + longest})), std::string("Client file name too long: 256 bytes"));
    CHECK_EQ(client_files_refusal(raw_client_files({"b.bin", "a.bin"})), std::string("Client file listed out of order or twice: a.bin"));
    CHECK_EQ(client_files_refusal(raw_client_files({"a.bin", "a.bin"})), std::string("Client file listed out of order or twice: a.bin"));
}

// The diff writes only a list the patcher takes back: append_client_files
// refuses each list parse_client_files refuses, in its words, before it
// appends a byte. 1024 files of 255-byte names, the most of both, go through
// and parse back as written.
TEST(patch_format_client_files_append_refuses_a_list_the_parse_refuses) {
    auto files_of = [](const std::vector<std::string>& names) {
        std::vector<ClientFile> files;
        for (const std::string& name : names) files.push_back(ClientFile{name, 0, nullptr});
        return files;
    };
    // What append_client_files refused files with, or "", and whether it
    // left what came before the section as it was.
    auto append_refusal = [](const std::vector<ClientFile>& files) {
        std::vector<char> buf = {'x'};
        const std::string refusal = refusal_of([&] { append_client_files(buf, files); });
        CHECK(buf == std::vector<char>{'x'});
        return refusal;
    };
    const std::string longest = std::string(251, 'a') + ".bin";
    const std::vector<char> too_large(MAX_INLINE_CLIENT_FILE_BYTES + 1, ' ');
    CHECK_EQ(append_refusal(files_of(too_many_client_file_names())), std::string("Too many client files: 1025"));
    CHECK_EQ(append_refusal(files_of({"a" + longest})), std::string("Client file name too long: 256 bytes"));
    CHECK_EQ(append_refusal(files_of({"b.bin", "a.bin"})), std::string("Client file listed out of order or twice: a.bin"));
    CHECK_EQ(append_refusal(files_of({"a.bin", "a.bin"})), std::string("Client file listed out of order or twice: a.bin"));
    CHECK_EQ(append_refusal(files_of({"sub/a.bin"})), std::string("Bad client file name: sub/a.bin"));
    CHECK_EQ(append_refusal({ClientFile{"poi_meta.json", too_large.size(), too_large.data()}}),
             std::string("Inline client file too large: poi_meta.json"));

    std::vector<std::string> most;
    for (int i = 0; i < 1024; i++) {
        char tail[16];
        std::snprintf(tail, sizeof tail, "%04d.bin", i);
        most.push_back(std::string(MAX_CLIENT_FILE_NAME - 8, 'a') + tail);
    }
    REQUIRE(most.back().size() == MAX_CLIENT_FILE_NAME);
    std::vector<char> buf;
    append_client_files(buf, files_of(most));
    CHECK(buf == raw_client_files(most));
    size_t pos = 0;
    const std::vector<ClientFile> got = parse_client_files(buf.data(), buf.size(), pos);
    CHECK_EQ(pos, buf.size());
    std::vector<std::string> got_names;
    for (const ClientFile& f : got) got_names.push_back(f.name);
    CHECK(got_names == most);
}

// --- content_hash ---

TEST(patch_format_content_hash_sees_every_byte) {
    const std::string base("core\0street\0addr\0x", 18);  // two words + a 2-byte tail
    const uint64_t h = content_hash(base.data(), base.size());
    CHECK_EQ(content_hash(base.data(), base.size()), h);
    for (size_t i = 0; i < base.size(); i++) {
        std::string changed = base;
        changed[i] ^= 1;
        CHECK(content_hash(changed.data(), changed.size()) != h);
    }
    CHECK(content_hash(base.data(), base.size() - 1) != h);
}

TEST(patch_format_content_hash_of_nothing_is_stable) {
    // An absent tier (nullptr, 0) and an empty file hash alike.
    char none[1] = {0};
    CHECK_EQ(content_hash(nullptr, 0), content_hash(none, 0));
}

// content_hash as the patch format defines it, in one pass: the diff wrote
// every planet patch's tier hashes so.
static uint64_t one_pass_content_hash(const char* data, size_t n) {
    uint64_t h = 0x9E3779B97F4A7C15ull ^ n;
    size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        uint64_t w;
        memcpy(&w, data + i, 8);
        h = (h ^ w) * 0xFF51AFD7ED558CCDull;
        h ^= h >> 32;
    }
    uint64_t tail = 0;
    if (n > i) memcpy(&tail, data + i, n - i);
    h = (h ^ tail) * 0xC4CEB9FE1A85EC53ull;
    return h ^ (h >> 29);
}

TEST(patch_format_content_hasher_equals_content_hash_over_any_split) {
    // Random bytes of every length up to 200 and a few longer, fed in random
    // pieces (empty ones too): each hash is the one-pass content_hash.
    std::mt19937 rng(20261029);
    std::vector<size_t> sizes(201);
    std::iota(sizes.begin(), sizes.end(), size_t(0));
    for (const size_t n : {size_t(4095), size_t(4096), size_t(65537)}) sizes.push_back(n);
    size_t wrong = 0;
    for (const size_t n : sizes) {
        std::vector<char> data(n);
        for (char& c : data) c = static_cast<char>(rng());
        const uint64_t want = one_pass_content_hash(data.data(), n);
        CHECK_EQ(content_hash(data.data(), n), want);
        for (int split = 0; split < 20; split++) {
            ContentHasher sut(n);
            for (size_t at = 0; at < n;) {
                const size_t piece = std::min<size_t>(n - at, rng() % (split % 2 ? 20 : 3000));
                sut.update(data.data() + at, piece);
                at += piece;
            }
            wrong += sut.hash() != want;
        }
    }
    CHECK_EQ(wrong, size_t(0));
    CHECK_EQ(ContentHasher(0).hash(), one_pass_content_hash(nullptr, 0));
}

// --- string offset fields ---

TEST(patch_format_string_fields_match_the_record_layouts) {
    using V = std::vector<size_t>;
    CHECK(string_field_offsets(PatchFileId::ADDR_POINTS) == V({8, 12}));  // not parent_way_id at 16
    CHECK(string_field_offsets(PatchFileId::STREET_WAYS) == V({8}));
    CHECK(string_field_offsets(PatchFileId::INTERP_WAYS) == V({8}));
    CHECK(string_field_offsets(PatchFileId::ADMIN_POLYGONS) == V({8}));
    CHECK(string_field_offsets(PatchFileId::POI_RECORDS) == V({16, 24, 28}));  // not parent_poly_id at 32
    CHECK(string_field_offsets(PatchFileId::PLACE_NODES) == V({8}));  // not parent_poly_id at 16
    CHECK(string_field_offsets(PatchFileId::STREET_NODES).empty());
    CHECK(string_field_offsets(PatchFileId::POI_VERTICES).empty());

    // Sparse files: string offsets are kind 2 (the u32 itself) and kind 3
    // (byte 8 of a postcode centroid); kind 0 holds admin slots, raw.
    std::vector<std::string> strings, raw;
    for (const auto& f : SPARSE_DELTA_FILES) {
        const std::string name = patch_file_names[(uint32_t)f.fid];
        if (f.remap_kind == 2) { CHECK_EQ(f.value_stride, 4u); strings.push_back(name); }
        if (f.remap_kind == 3) { CHECK_EQ(f.value_stride, 16u); strings.push_back(name); }
        if (f.remap_kind == 0) raw.push_back(name);
    }
    CHECK(strings == std::vector<std::string>({"addr_postcodes.bin", "way_postcodes.bin",
                                               "interp_postcodes.bin", "postcode_centroids.bin"}));
    CHECK(raw == std::vector<std::string>({"admin_parents.bin", "way_parents.bin"}));
    CHECK_EQ(POSTCODE_CENTROID_POSTCODE_ID_OFF, size_t(8));
}

// v14: file ids 7, 26 and 27 name the arc files, of no string field, which
// the diff merges by slot (ADMIN_SLOT_FAMILIES: the arc index at 4 bytes a
// record, the streams at 1, after their slot files): no table that remaps or
// scans a file by its records at the admin stride (as the postal polygons
// were) names them, nor remap kind 1 (an admin id) any file.
TEST(patch_format_v14_names_the_arc_files_and_keeps_admin_slots) {
    CHECK_EQ(std::string(patch_file_names[7]), std::string("admin_rings.bin"));
    CHECK_EQ(std::string(patch_file_names[26]), std::string("arc_index.bin"));
    CHECK_EQ(std::string(patch_file_names[27]), std::string("arc_vertices.bin"));
    CHECK(uint32_t(PatchFileId::ADMIN_RINGS) == 7 && uint32_t(PatchFileId::ARC_INDEX) == 26 &&
          uint32_t(PatchFileId::ARC_VERTICES) == 27);
    for (const PatchFileId arc_file : {PatchFileId::ADMIN_RINGS, PatchFileId::ARC_INDEX, PatchFileId::ARC_VERTICES}) {
        CHECK(string_field_offsets(arc_file).empty());
        for (const auto& f : STRING_RECORD_FILES) CHECK(f.fid != arc_file);
        CHECK(sparse_delta_file(arc_file) == nullptr);
        CHECK(parallel_delta_file(arc_file) == nullptr);
    }
    for (const auto& f : SPARSE_DELTA_FILES) CHECK(f.remap_kind != 1);
    for (const auto& f : PARALLEL_DELTA_FILES) CHECK(f.remap_kind != 1);
    std::vector<std::string> sent;
    for (const AdminSlotFamily& family : ADMIN_SLOT_FAMILIES)
        for (const PatchFileId fid : {family.slots, family.blocks}) sent.push_back(patch_file_names[uint32_t(fid)]);
    CHECK(sent == std::vector<std::string>({"arc_index.bin", "arc_vertices.bin", "admin_polygons.bin", "admin_rings.bin"}));
    CHECK_EQ(ADMIN_SLOT_FAMILIES[0].stride, size_t(4));
    CHECK_EQ(ADMIN_SLOT_FAMILIES[1].stride, ADMIN_POLYGON_STRIDE);
}

// A sparse delta's file, value stride and remap kind are a row of
// SPARSE_DELTA_FILES: kind 1 (an admin id, remapped before v14) is refused
// for every file, as are a value stride or kind another file's, and a file
// that sends no sparse delta.
TEST(patch_format_sparse_delta_header_is_a_row_of_its_table) {
    for (const auto& f : SPARSE_DELTA_FILES)
        CHECK_EQ(refusal_of([&] { check_sparse_delta_header(uint32_t(f.fid), f.value_stride, f.remap_kind); }), std::string());
    auto refusal = [](PatchFileId fid, uint32_t stride, uint32_t kind) {
        return refusal_of([&] { check_sparse_delta_header(uint32_t(fid), stride, kind); });
    };
    const std::string malformed = "Malformed sparse delta";
    CHECK_EQ(refusal(PatchFileId::ADMIN_PARENTS, 4, 1), malformed);
    CHECK_EQ(refusal(PatchFileId::WAY_PARENTS, 4, 1), malformed);
    CHECK_EQ(refusal(PatchFileId::ADMIN_PARENTS, 4, 2), malformed);
    CHECK_EQ(refusal(PatchFileId::ADDR_POSTCODES, 4, 0), malformed);
    CHECK_EQ(refusal(PatchFileId::POSTCODE_CENTROIDS, 4, 3), malformed);
    CHECK_EQ(refusal(PatchFileId::ADDR_POSTCODES, 16, 2), malformed);
    CHECK_EQ(refusal(PatchFileId::ARC_INDEX, 4, 0), malformed);
    CHECK_EQ(refusal_of([] { check_sparse_delta_header(uint32_t(PatchFileId::COUNT), 4, 0); }), malformed);
    CHECK_EQ(refusal_of([] { check_sparse_delta_header(0xFFFFFFFFu, 4, 0); }), malformed);
}

TEST(patch_format_string_record_files_cover_every_string_field) {
    // The diff's reference scan reads STRING_RECORD_FILES: a record file with
    // string fields missing there would send no runs for the tiers it uses.
    std::unordered_set<uint32_t> listed;
    for (const auto& f : STRING_RECORD_FILES) {
        listed.insert((uint32_t)f.fid);
        const auto fields = string_field_offsets(f.fid);
        CHECK(!fields.empty());
        for (size_t off : fields) CHECK(off + 4 <= f.stride);
    }
    for (uint32_t fid = 0; fid < (uint32_t)PatchFileId::COUNT; fid++)
        if (!string_field_offsets((PatchFileId)fid).empty()) CHECK(listed.count(fid) == 1);
    CHECK_EQ(listed.size(), std::size(STRING_RECORD_FILES));
}

// --- old file identity ---

TEST(patch_format_old_file_must_be_the_size_the_patch_was_made_from) {
    ScratchDir dir("old-size");
    REQUIRE(write_file(dir.path() + "/admin_polygons.bin", std::vector<char>(48, 'x')));
    REQUIRE(write_file(dir.path() + "/arc_index.bin", std::vector<char>()));
    auto message = [&](const std::string& name, uint64_t expected) {
        return refusal_of([&] { require_old_file_size(dir.path(), name, expected); });
    };
    CHECK_EQ(message("admin_polygons.bin", 48), std::string());
    CHECK_EQ(message("arc_index.bin", 0), std::string());
    // The diff writes 0 for a file the old dir lacks.
    CHECK_EQ(message("absent.bin", 0), std::string());
    CHECK_EQ(message("admin_polygons.bin", 72),
             std::string("Old admin_polygons.bin is not the one the patch was made from (48 bytes, the patch expects 72)"));
    CHECK(message("admin_polygons.bin", 0) != "");
    CHECK(message("absent.bin", 24) != "");
}

TEST(patch_format_old_file_size_of_a_virtual_file_comes_from_the_hook) {
    // Low-memory mode: an encoded family's old file isn't on disk; the hook
    // checks its decoded size and the reader takes the patch's.
    ScratchDir dir("old-size");
    virtual_size_hook() = [](const std::string&, const std::string& name, uint64_t expected) -> bool {
        if (name != "street_ways.bin") return false;
        if (expected != 120) throw std::runtime_error("decodes to 120 bytes");
        return true;
    };
    require_old_file_size(dir.path(), "street_ways.bin", 120);
    CHECK_EQ(virtual_file_sizes()[dir.path() + "/street_ways.bin"], uint64_t(120));
    CHECK_EQ(refusal_of([&] { require_old_file_size(dir.path(), "street_ways.bin", 96); }), std::string("decodes to 120 bytes"));
    // Not virtual: checked on disk, where it is absent.
    CHECK_EQ(refusal_of([&] { require_old_file_size(dir.path(), "admin_cells.bin", 12); }),
             std::string("Old admin_cells.bin is not the one the patch was made from (0 bytes, the patch expects 12)"));
    CHECK(!virtual_file_sizes().count(dir.path() + "/admin_cells.bin"));
    virtual_size_hook() = nullptr;
    virtual_file_sizes().clear();
}

TEST(patch_format_old_file_sizes_cover_the_files_no_section_names) {
    ScratchDir old_dir("old-sizes");
    REQUIRE(write_file(old_dir.path() + "/geo_cells.bin", std::vector<char>(40, 'g')));
    REQUIRE(write_file(old_dir.path() + "/geo_entries.bin", std::vector<char>(30, 'e')));
    REQUIRE(write_file(old_dir.path() + "/poi_entries.bin", std::vector<char>()));
    std::vector<char> buf;
    append_old_file_sizes(buf, old_dir.path());
    CHECK_EQ(buf.size(), 4 + std::size(UNSECTIONED_OLD_FILES) * 12);
    auto check_in = [&](const std::string& dir, const std::vector<char>& b) {
        size_t pos = 0;
        const std::string refusal = refusal_of([&] { check_old_file_sizes(b.data(), b.size(), pos, dir); });
        return refusal.empty() && pos != b.size() ? std::string("pos") : refusal;
    };
    CHECK_EQ(check_in(old_dir.path(), buf), std::string());
    // Another day's index: same name, another size.
    ScratchDir other("old-sizes-other");
    REQUIRE(write_file(other.path() + "/geo_cells.bin", std::vector<char>(60, 'g')));
    REQUIRE(write_file(other.path() + "/geo_entries.bin", std::vector<char>(30, 'e')));
    REQUIRE(write_file(other.path() + "/poi_entries.bin", std::vector<char>()));
    CHECK_EQ(check_in(other.path(), buf),
             std::string("Old geo_cells.bin is not the one the patch was made from (60 bytes, the patch expects 40)"));
    REQUIRE(write_file(other.path() + "/geo_cells.bin", std::vector<char>(40, 'g')));
    REQUIRE(write_file(other.path() + "/geo_entries.bin", std::vector<char>(31, 'e')));
    CHECK_EQ(check_in(other.path(), buf),
             std::string("Old geo_entries.bin is not the one the patch was made from (31 bytes, the patch expects 30)"));
    REQUIRE(write_file(other.path() + "/geo_entries.bin", std::vector<char>(30, 'e')));
    // A file the old dir lacked must still be absent (or empty).
    REQUIRE(write_file(other.path() + "/geo_cells.bin", std::vector<char>(40, 'g')));
    REQUIRE(write_file(other.path() + "/admin_cells.bin", std::vector<char>(12, 'a')));
    CHECK(!check_in(other.path(), buf).empty());
    for (size_t cut = 0; cut < buf.size(); cut++)
        CHECK_EQ(check_in(old_dir.path(), std::vector<char>(buf.begin(), buf.begin() + cut)),
                 std::string("Truncated old file sizes"));
    std::vector<char> bad(16, 0);
    const uint32_t one = 1, past_last = (uint32_t)PatchFileId::COUNT;
    memcpy(bad.data(), &one, 4);
    memcpy(bad.data() + 4, &past_last, 4);
    CHECK_EQ(check_in(old_dir.path(), bad), std::string("Malformed old file sizes"));
}

// --- offset fixups ---

static constexpr uint32_t NO_OFFSET = 0xFFFFFFFFu;

static OffsetFixups encode_fixed(const std::vector<uint32_t>& old_offsets, const std::vector<uint32_t>& fixed) {
    return encode_offset_fixups(old_offsets, [&](size_t i) { return fixed[i]; });
}

static OffsetFixupReader reader_of(const OffsetFixups& f) {
    return OffsetFixupReader(f.runs.data(), f.runs.size(), f.n_runs,
                             f.values.data(), f.values.size(), f.n_values);
}

// What the patcher writes for every record, visiting them in order, and the
// end of its replay.
static std::vector<uint32_t> replay(const std::vector<uint32_t>& old_offsets, const OffsetFixups& f) {
    auto reader = reader_of(f);
    std::vector<uint32_t> out;
    for (uint32_t i = 0; i < old_offsets.size(); i++) out.push_back(reader.apply(i, old_offsets[i]));
    reader.finish(old_offsets.size());
    return out;
}

// Offset fixups as given, whatever they hold: runs of (records skipped since
// the previous run's end, length, change of the shift) and values of (index
// delta, offset).
struct FixupRun {
    uint32_t skipped, length;
    int32_t shift_change;
};
static OffsetFixups fixups_of(const std::vector<FixupRun>& runs, const std::vector<std::pair<uint32_t, uint32_t>>& values) {
    OffsetFixups f;
    for (const FixupRun& r : runs) {
        write_varint(f.runs, r.skipped);
        write_varint(f.runs, r.length);
        write_varint(f.runs, zigzag32(static_cast<uint32_t>(r.shift_change)));
        f.n_runs++;
    }
    for (const auto& [delta, offset] : values) {
        write_varint(f.values, delta);
        write_varint(f.values, offset);
        f.n_values++;
    }
    return f;
}

// Every record of a file of n_records through reader: those in no_data
// without an offset (NO_DATA), as the records values name are, the others
// each with one.
static void walk(OffsetFixupReader& reader, uint32_t n_records, const std::set<uint32_t>& no_data) {
    for (uint32_t i = 0; i < n_records; i++) reader.apply(i, no_data.count(i) ? NO_OFFSET : 100 * i);
}

// The refusal of a walk of f, or "".
static std::string refusal_of_walk(const OffsetFixups& f, uint32_t n_records, const std::set<uint32_t>& no_data = {}) {
    return refusal_of([&] {
        auto reader = reader_of(f);
        walk(reader, n_records, no_data);
    });
}

// The refusal of a replay of f, or "": the walk, then its end.
static std::string refusal_of_replay(const OffsetFixups& f, uint32_t n_records, const std::set<uint32_t>& no_data = {}) {
    return refusal_of([&] {
        auto reader = reader_of(f);
        walk(reader, n_records, no_data);
        reader.finish(n_records);
    });
}

TEST(patch_format_offset_fixups_round_trip) {
    struct Case { const char* name; std::vector<uint32_t> old_offsets, fixed; uint32_t runs, values; };
    const Case cases[] = {
        {"nothing moved", {0, 5, 9}, {0, 5, 9}, 0, 0},
        {"one shift for every later block", {0, 5, 9, 14}, {0, 8, 12, 17}, 1, 0},
        {"no-data records inside a run", {0, NO_OFFSET, 5, NO_OFFSET, 9}, {3, NO_OFFSET, 8, NO_OFFSET, 12}, 1, 0},
        {"an unmoved record splits the run", {0, 5, 9}, {3, 5, 12}, 2, 0},
        {"shrinking shift", {10, 20, 30}, {4, 14, 30}, 1, 0},
        {"shift changes", {10, 20, 30, 40}, {12, 22, 25, 35}, 2, 0},
        {"record loses its block", {10, 20}, {10, NO_OFFSET}, 1, 0},
        {"no-data record gains an offset", {NO_OFFSET, 4, NO_OFFSET}, {7, 9, 13}, 1, 2},
    };
    for (const auto& c : cases) {
        auto f = encode_fixed(c.old_offsets, c.fixed);
        CHECK_EQ(f.n_runs, c.runs);
        CHECK_EQ(f.n_values, c.values);
        CHECK(replay(c.old_offsets, f) == c.fixed);
    }
}

TEST(patch_format_offset_fixups_skip_records_the_merge_drops) {
    // MATCH replay visits only the kept old records, still ascending.
    std::vector<uint32_t> old_offsets = {0, 4, 8, 12, 16, 20, 24};
    std::vector<uint32_t> fixed = {0, 4, 9, 13, 30, 34, 38};
    auto f = encode_fixed(old_offsets, fixed);
    auto reader = reader_of(f);
    for (uint32_t i : {1u, 3u, 6u}) CHECK_EQ(reader.apply(i, old_offsets[i]), fixed[i]);
    CHECK_EQ(refusal_of([&] { reader.finish(old_offsets.size()); }), std::string());
}

TEST(patch_format_offset_fixups_round_trip_at_the_edges) {
    // Record 0 given an offset, then runs of one record each.
    const std::vector<uint32_t> old_offsets = {NO_OFFSET, 0, 4, 8};
    const std::vector<uint32_t> fixed = {7, 1, 6, 11};
    auto f = encode_fixed(old_offsets, fixed);
    CHECK_EQ(f.n_values, uint32_t(1));
    CHECK_EQ(f.n_runs, uint32_t(3));
    CHECK(replay(old_offsets, f) == fixed);
    // A run of one record at record 0.
    f = encode_fixed({0, 4}, {3, 4});
    CHECK_EQ(f.n_runs, uint32_t(1));
    CHECK(replay({0, 4}, f) == std::vector<uint32_t>({3, 4}));

    // The last record index, 0xFFFFFFFE, of a file of 2^32 - 1 records (by
    // hand: the encoder would take an offset per record): a run of one record
    // at record 0 and one at the last, values for records 1 and 0xFFFFFFFD.
    f = fixups_of({{0, 1, 3}, {0xFFFFFFFDu, 1, 2}}, {{1, 7}, {0xFFFFFFFCu, 9}});
    auto reader = reader_of(f);
    CHECK_EQ(reader.apply(0, 10), uint32_t(13));
    CHECK_EQ(reader.apply(1, NO_OFFSET), uint32_t(7));
    CHECK_EQ(reader.apply(0xFFFFFFFDu, NO_OFFSET), uint32_t(9));
    CHECK_EQ(reader.apply(0xFFFFFFFEu, 20), uint32_t(25));
    CHECK_EQ(refusal_of([&] { reader.finish(0xFFFFFFFFu); }), std::string());
    // The first value at the last record index.
    f = fixups_of({}, {{0xFFFFFFFEu, 9}});
    reader = reader_of(f);
    CHECK_EQ(reader.apply(0xFFFFFFFEu, NO_OFFSET), uint32_t(9));
    CHECK_EQ(refusal_of([&] { reader.finish(0xFFFFFFFFu); }), std::string());
}

// What the patcher's MATCH replay writes for a run of old records from
// record first on, given their old offsets: apply's offset for each record
// the fixups reach (OffsetFixupReader::reach), the old one for the rest.
static std::vector<uint32_t> replay_run(OffsetFixupReader& reader, uint64_t first, const std::vector<uint32_t>& old_offsets) {
    const uint32_t count = static_cast<uint32_t>(old_offsets.size());
    const uint32_t fixed_records = OffsetFixupReader::reach(first, count);
    std::vector<uint32_t> out;
    for (uint32_t k = 0; k < count; k++)
        out.push_back(k < fixed_records ? reader.apply(static_cast<uint32_t>(first + k), old_offsets[k]) : old_offsets[k]);
    return out;
}

// A MATCH run reaches the records below the last u32 record index,
// 0xFFFFFFFE, wherever it starts and however long it is: none past it, whose
// u32 index would be END (0xFFFFFFFF, NO_DATA) or wrap onto record 0 on.
TEST(patch_format_offset_fixups_reach_a_run_up_to_the_last_index) {
    const uint64_t past = 0xFFFFFFFFu;
    CHECK_EQ(OffsetFixupReader::reach(0, 0), uint32_t(0));
    CHECK_EQ(OffsetFixupReader::reach(0, 5), uint32_t(5));
    CHECK_EQ(OffsetFixupReader::reach(7, 0xFFFFFFF0u), uint32_t(0xFFFFFFF0u));
    // A run of every u32 record index, and the same run a record later.
    CHECK_EQ(OffsetFixupReader::reach(0, 0xFFFFFFFFu), uint32_t(0xFFFFFFFFu));
    CHECK_EQ(OffsetFixupReader::reach(1, 0xFFFFFFFFu), uint32_t(0xFFFFFFFEu));
    // Runs that end at the last index or cross it.
    CHECK_EQ(OffsetFixupReader::reach(past - 3, 3), uint32_t(3));
    CHECK_EQ(OffsetFixupReader::reach(past - 3, 5), uint32_t(3));
    CHECK_EQ(OffsetFixupReader::reach(past - 1, 0xFFFFFFFFu), uint32_t(1));
    // Runs from END on, including those whose first index wraps a u32.
    CHECK_EQ(OffsetFixupReader::reach(past, 1), uint32_t(0));
    CHECK_EQ(OffsetFixupReader::reach(past + 1, 5), uint32_t(0));
    CHECK_EQ(OffsetFixupReader::reach((uint64_t{1} << 32) + 2, 5), uint32_t(0));
    CHECK_EQ(OffsetFixupReader::reach(UINT64_MAX, 0xFFFFFFFFu), uint32_t(0));
}

// A file of more than 0xFFFFFFFF records (by hand again) has records at and
// past 0xFFFFFFFF (NO_DATA), which no run or value reaches: the encoder
// takes one only unmoved (else past_last_index), so the replay leaves its
// offset, or its NO_DATA, as it is, after runs and values it read or skipped
// (a merge that drops the records between), whether its MATCH run starts
// there or crosses the last index.
TEST(patch_format_offset_fixups_leave_records_past_the_last_index_as_they_are) {
    // A run of record 0 (shift 3), a value for record 1 and a run of the last
    // record index, 0xFFFFFFFE (shift 5).
    const OffsetFixups f = fixups_of({{0, 1, 3}, {0xFFFFFFFDu, 1, 2}}, {{1, 7}});
    const uint64_t past = 0xFFFFFFFFu;
    // What a replay of the MATCH runs (first record index, its records' old
    // offsets) given writes, "-" for NO_DATA, then its end in a file of
    // 2^32 + 2 records; or its refusal.
    auto replay_of = [&](const std::vector<std::pair<uint64_t, std::vector<uint32_t>>>& runs) {
        std::string out;
        const std::string refusal = refusal_of([&] {
            auto reader = reader_of(f);
            for (const auto& [first, old_offsets] : runs)
                for (const uint32_t fixed : replay_run(reader, first, old_offsets))
                    out += (out.empty() ? "" : " ") + (fixed == NO_OFFSET ? std::string("-") : std::to_string(fixed));
            reader.finish(past + 3);
        });
        return refusal.empty() ? out : refusal;
    };
    // Every record from the last index on, with offsets and without, in a
    // run across the last index and in runs of their own.
    CHECK_EQ(replay_of({{0, {10, NO_OFFSET}}, {past - 1, {20, NO_OFFSET, 30, NO_OFFSET}}}), std::string("13 7 25 - 30 -"));
    CHECK_EQ(replay_of({{0, {10, NO_OFFSET}}, {past - 1, {20}}, {past, {NO_OFFSET}}, {past + 1, {30}}, {past + 2, {NO_OFFSET}}}),
             std::string("13 7 25 - 30 -"));
    CHECK_EQ(replay_of({{0, {10, NO_OFFSET}}, {past - 1, {20, 30}}}), std::string("13 7 25 30"));
    // A replay from record 0 straight past 0xFFFFFFFF, which reads neither
    // the value nor the second run.
    CHECK_EQ(replay_of({{0, {10}}, {past + 1, {30, NO_OFFSET}}}), std::string("13 30 -"));
}

// encode_offset_fixups writes nothing the reader refuses: files of records
// with and without offsets, shifted in runs, losing their blocks or given
// one, replayed record by record or only some records (the merge's kept
// ones), to the end.
TEST(patch_format_offset_fixups_encode_only_what_the_reader_takes) {
    std::mt19937 rng(20261031);
    for (int round = 0; round < 2000; round++) {
        const uint32_t n = rng() % 64;
        std::vector<uint32_t> old_offsets(n), fixed(n);
        uint32_t shift = 0;
        for (uint32_t i = 0; i < n; i++) {
            if (rng() % 6 == 0) shift = rng() % 3 == 0 ? 0 : static_cast<uint32_t>(rng());
            const bool no_data = rng() % 4 == 0;
            old_offsets[i] = no_data ? NO_OFFSET : 16 * i;
            if (no_data) fixed[i] = rng() % 2 ? NO_OFFSET : static_cast<uint32_t>(rng() % 1000);
            else fixed[i] = rng() % 10 == 0 ? NO_OFFSET : 16 * i + shift;
        }
        const OffsetFixups f = encode_fixed(old_offsets, fixed);
        CHECK(replay(old_offsets, f) == fixed);
        auto reader = reader_of(f);
        bool kept_right = true;
        for (uint32_t i = 0; i < n; i++)
            if (rng() % 3 == 0) kept_right = reader.apply(i, old_offsets[i]) == fixed[i] && kept_right;
        CHECK(kept_right);
        CHECK_EQ(refusal_of([&] { reader.finish(n); }), std::string());
    }
}

TEST(patch_format_offset_fixups_one_edit_is_one_run) {
    // An edit at record 500000 moves every later block by the same amount;
    // the patch carries that as one run of a few bytes, not 500000 entries.
    std::vector<uint32_t> old_offsets(1000000), fixed(1000000);
    for (uint32_t i = 0; i < old_offsets.size(); i++) {
        old_offsets[i] = i * 3;
        fixed[i] = i < 500000 ? i * 3 : i * 3 + 7;
    }
    auto f = encode_fixed(old_offsets, fixed);
    CHECK_EQ(f.n_runs, uint32_t(1));
    CHECK(f.runs.size() <= size_t(8));
    CHECK(replay(old_offsets, f) == fixed);
}

TEST(patch_format_offset_fixups_reject_truncated_runs) {
    auto f = encode_fixed({0, 5, 9}, {3, 5, 12});
    f.runs.pop_back();
    CHECK_EQ(refusal_of_walk(f, 3), std::string("Malformed offset fixups"));
}

// A value index that repeats (a delta of 0 after the first), or that passes
// the last record index (0xFFFFFFFE; summed in 32 bits it would wrap, or
// reach NO_DATA), is refused when the replay reads it, not skipped.
TEST(patch_format_offset_fixups_refuse_a_value_index_that_repeats_or_passes_32_bits) {
    const std::string malformed = "Malformed offset fixups";
    // Records 0 and 3 have no offsets.
    const std::set<uint32_t> no_data = {0, 3};
    // The first value may be record 0's, the others ascend.
    CHECK_EQ(refusal_of_walk(fixups_of({}, {{0, 7}, {3, 8}}), 10, no_data), std::string());
    // Record 0's twice, record 3's twice.
    CHECK_EQ(refusal_of_walk(fixups_of({}, {{0, 7}, {0, 8}}), 10, no_data), malformed);
    CHECK_EQ(refusal_of_walk(fixups_of({}, {{3, 7}, {0, 8}}), 10, no_data), malformed);
    // Past 32 bits (2^32 + 1), and at 0xFFFFFFFF, after a value or first.
    CHECK_EQ(refusal_of_walk(fixups_of({}, {{3, 7}, {0xFFFFFFFEu, 8}}), 10, no_data), malformed);
    CHECK_EQ(refusal_of_walk(fixups_of({}, {{3, 7}, {0xFFFFFFFCu, 8}}), 10, no_data), malformed);
    CHECK_EQ(refusal_of_walk(fixups_of({}, {{0xFFFFFFFFu, 7}, {0, 8}}), 10, no_data), malformed);
}

// A value for a record with an offset is refused when the replay reaches
// it, not taken: encode_offset_fixups gives values only to records without
// one (NO_DATA), which runs pass over.
TEST(patch_format_offset_fixups_refuse_a_value_for_a_record_with_an_offset) {
    const std::string malformed = "Malformed offset fixups";
    // Values for records 0 and 3: without offsets they take them; with one,
    // either is refused.
    const OffsetFixups values = fixups_of({}, {{0, 7}, {3, 8}});
    CHECK_EQ(refusal_of_replay(values, 10, {0, 3}), std::string());
    CHECK_EQ(refusal_of_walk(values, 10, {3}), malformed);
    CHECK_EQ(refusal_of_walk(values, 10, {0}), malformed);
    // Record 3 in a run of records 2-5: a run passes over it without an
    // offset, but its value is refused once it has one.
    const OffsetFixups in_run = fixups_of({{2, 4, 5}}, {{3, 8}});
    CHECK_EQ(refusal_of_replay(in_run, 10, {3}), std::string());
    CHECK_EQ(refusal_of_walk(in_run, 10), malformed);
    // The value of a record the merge drops is never checked against its
    // offset, which the replay doesn't read.
    CHECK_EQ(refusal_of([&] {
                 auto reader = reader_of(in_run);
                 for (uint32_t i : {0u, 2u, 4u}) reader.apply(i, 100 * i);
                 reader.finish(10);
             }),
             std::string());
}

// A run of no records, or one whose start or end passes the last record
// index (summed in 32 bits its start would wrap to before the previous run's
// end, or its end to before its start), is refused when the replay reads it,
// not skipped.
TEST(patch_format_offset_fixups_refuse_an_empty_run_or_a_run_past_32_bits) {
    const std::string malformed = "Malformed offset fixups";
    // Runs of one record, each right after the one before, then a longer one.
    CHECK_EQ(refusal_of_walk(fixups_of({{2, 1, 4}, {0, 1, -4}, {0, 6, 1}}, {}), 10), std::string());
    // A run of no records, first or after another.
    CHECK_EQ(refusal_of_walk(fixups_of({{2, 0, 4}}, {}), 10), malformed);
    CHECK_EQ(refusal_of_walk(fixups_of({{0, 4, 3}, {1, 0, 1}}, {}), 10), malformed);
    // A start past 32 bits (2^32 + 2, and 0xFFFFFFFF with its end at 2^32).
    CHECK_EQ(refusal_of_walk(fixups_of({{0, 4, 3}, {0xFFFFFFFEu, 2, 1}}, {}), 10), malformed);
    CHECK_EQ(refusal_of_walk(fixups_of({{0, 4, 3}, {0xFFFFFFFBu, 1, 1}}, {}), 10), malformed);
    // An end past 32 bits (2^32 + 4); one right after the last record index,
    // 0xFFFFFFFE, ends a run of every record.
    CHECK_EQ(refusal_of_walk(fixups_of({{5, 0xFFFFFFFFu, 3}}, {}), 10), malformed);
    CHECK_EQ(refusal_of_walk(fixups_of({{0, 0xFFFFFFFFu, 3}}, {}), 10), std::string());
}

// The replay's end (finish) refuses what it never read: a value or a run past
// the old file's records, and bytes past the last value or run. Values and
// runs within the file that it skips (a deleted or replaced record's) are
// not malformed.
TEST(patch_format_offset_fixups_refuse_values_runs_or_bytes_the_replay_never_reaches) {
    const std::string malformed = "Malformed offset fixups";
    // Runs of records 1-2 and 9, values of records 0 and 9: a file of 10
    // records, but not of 9 (or none, which leaves every one).
    // Records 0 and 9, which the values name, have no offsets.
    const OffsetFixups within = fixups_of({{1, 2, 5}, {6, 1, 1}}, {{0, 7}, {9, 8}});
    const std::set<uint32_t> no_data = {0, 9};
    CHECK_EQ(refusal_of_replay(within, 10, no_data), std::string());
    CHECK_EQ(refusal_of_replay(within, 9, no_data), malformed);
    CHECK_EQ(refusal_of_replay(within, 0, no_data), malformed);
    // The runs alone, past a file of 9 records; a run past the file after
    // one in it; a value past it after one in it.
    CHECK_EQ(refusal_of_replay(fixups_of({{1, 2, 5}, {6, 1, 1}}, {}), 9), malformed);
    CHECK_EQ(refusal_of_replay(fixups_of({{1, 2, 5}, {1000, 1, 1}}, {}), 10), malformed);
    CHECK_EQ(refusal_of_replay(fixups_of({}, {{4, 7}, {6, 8}}), 10, {4}), malformed);
    // Records the merge drops: a replay that never asks for records 1-2 and
    // 9 leaves their fixups, within the file.
    CHECK_EQ(refusal_of([&] {
                 auto reader = reader_of(within);
                 for (uint32_t i : {0u, 3u, 8u}) reader.apply(i, no_data.count(i) ? NO_OFFSET : 100 * i);
                 reader.finish(10);
             }),
             std::string());
    // A byte past the last run or value; bytes without a run or value.
    for (const bool runs : {true, false}) {
        OffsetFixups longer = within, bare;
        (runs ? longer.runs : longer.values).push_back(0);
        (runs ? bare.runs : bare.values).push_back(0);
        CHECK_EQ(refusal_of_replay(longer, 10, no_data), malformed);
        CHECK_EQ(refusal_of_replay(bare, 10, no_data), malformed);
    }
}

// Offset fixups a merge section's header can't give are refused, naming the
// file: more runs or values than the limits' count, more bytes of runs or of
// values than their bytes (lower for tests; the header gives each as a u32,
// which fixups past the real limits would wrap), or a record moved past the
// last u32 record index, which the encoder can't give. At the limits they
// pass.
TEST(patch_format_offset_fixups_refuse_one_past_the_sections_limits) {
    auto refusal = [](const OffsetFixups& f, OffsetFixupLimits limits) {
        return refusal_of([&] { check_offset_fixups(f, "street_ways.bin", limits); });
    };
    const std::string past = "Offset fixups of street_ways.bin: more than the section's ";
    // Two runs (records 1 and 3-4, 6 bytes) and a value (record 5, 2 bytes),
    // as encoded; one run (3 bytes) and two values (records 0 and 5, 4 bytes).
    const OffsetFixups runs = encode_fixed({0, 4, 8, 12, 16, NO_OFFSET}, {0, 5, 8, 14, 18, 7});
    const OffsetFixups values = encode_fixed({NO_OFFSET, 4, 8, 12, 16, NO_OFFSET}, {3, 5, 8, 12, 16, 7});
    CHECK_EQ(runs.n_runs, uint32_t(2));
    CHECK_EQ(runs.runs.size(), size_t(6));
    CHECK_EQ(runs.n_values, uint32_t(1));
    CHECK_EQ(runs.values.size(), size_t(2));
    CHECK_EQ(values.n_runs, uint32_t(1));
    CHECK_EQ(values.runs.size(), size_t(3));
    CHECK_EQ(values.n_values, uint32_t(2));
    CHECK_EQ(values.values.size(), size_t(4));
    for (const OffsetFixups* f : {&runs, &values}) {
        CHECK_EQ(refusal(*f, {}), std::string());
        CHECK_EQ(refusal(*f, {2, 6}), std::string());
    }
    CHECK_EQ(refusal(runs, {1, 6}), past + "1 runs");
    CHECK_EQ(refusal(values, {1, 6}), past + "1 values");
    CHECK_EQ(refusal(runs, {2, 5}), past + "5 bytes of runs");
    CHECK_EQ(refusal(values, {2, 3}), past + "3 bytes of values");
    // None at all, under limits of 0.
    CHECK_EQ(refusal(OffsetFixups{}, {0, 0}), std::string());
    OffsetFixups unindexed = runs;
    unindexed.past_last_index = true;
    CHECK_EQ(refusal(unindexed, {}),
             std::string("Offset fixups of street_ways.bin: a record past the last u32 record index moved"));
}

// --- string fixups ---

// Each slot's value as a replay that visits `slots` (ascending) takes it:
// the fixup's where one is given, else none; then the end is checked.
static std::vector<std::optional<uint32_t>> take_string_fixups(const std::vector<char>& bytes, uint32_t n,
                                                               const std::vector<uint64_t>& slots) {
    StringFixupReader reader(bytes.data(), bytes.size(), n);
    std::vector<std::optional<uint32_t>> out;
    for (const uint64_t slot : slots) out.push_back(reader.fixes(slot) ? std::optional<uint32_t>(reader.take()) : std::nullopt);
    reader.finish();
    return out;
}

TEST(patch_format_string_fixups_round_trip) {
    // The first slot, its neighbours, a far one and slots past 32 bits;
    // values that rise, fall, wrap and are NO_DATA.
    const std::map<uint64_t, uint32_t> fixed = {{0, 7}, {1, 3}, {2, 0xFFFFFFF0u}, {9, 5}, {100000, 5},
                                                {0xFFFFFFFEull, 0}, {0x100000004ull, NO_OFFSET}};
    StringFixups f;
    for (const auto& [slot, value] : fixed) f.add(slot, value);
    CHECK_EQ(f.count(), uint32_t(fixed.size()));
    std::vector<uint64_t> slots;
    std::vector<std::optional<uint32_t>> expect;
    for (uint64_t slot = 0; slot <= 0x100000005ull; slot = slot < 100001 || slot >= 0xFFFFFFFDull ? slot + 1 : 0xFFFFFFFDull) {
        slots.push_back(slot);
        const auto it = fixed.find(slot);
        expect.push_back(it == fixed.end() ? std::nullopt : std::optional<uint32_t>(it->second));
    }
    CHECK(take_string_fixups(f.bytes(), f.count(), slots) == expect);

    // 20,000 at random, every slot visited.
    std::mt19937 rng(20261031);
    StringFixups random;
    std::map<uint64_t, uint32_t> random_fixed;
    for (uint64_t slot = rng() % 3; random_fixed.size() < 20000; slot += 1 + (rng() % 8 == 0 ? rng() % 1000 : rng() % 3)) {
        random_fixed[slot] = static_cast<uint32_t>(rng());
        random.add(slot, random_fixed[slot]);
    }
    slots.clear();
    expect.clear();
    for (uint64_t slot = 0; slot <= random_fixed.rbegin()->first + 2; slot++) {
        slots.push_back(slot);
        const auto it = random_fixed.find(slot);
        expect.push_back(it == random_fixed.end() ? std::nullopt : std::optional<uint32_t>(it->second));
    }
    CHECK(take_string_fixups(random.bytes(), random.count(), slots) == expect);
}

TEST(patch_format_string_fixups_refuse_a_slot_passed_a_fixup_left_over_and_bytes_past_the_last) {
    // Records of three string fields: fixups for record 1's first (slot 3)
    // and record 2's last (slot 8).
    StringFixups f;
    f.add(3, 30);
    f.add(8, 80);
    const std::vector<uint64_t> every = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    auto replay = [](const std::vector<char>& bytes, uint32_t n, const std::vector<uint64_t>& slots) {
        return refusal_of([&] { take_string_fixups(bytes, n, slots); });
    };
    CHECK_EQ(replay(f.bytes(), 2, every), std::string());
    // A replay that deletes record 1 passes slot 3 without matching it.
    CHECK_EQ(replay(f.bytes(), 2, {0, 1, 2, 6, 7, 8}), std::string("Malformed string fixups"));
    // One that ends after record 1 leaves slot 8's.
    CHECK_EQ(replay(f.bytes(), 2, {0, 1, 2, 3, 4, 5}), std::string("Malformed string fixups"));
    // A byte past the last, a last cut short, and a count past the bytes.
    std::vector<char> longer = f.bytes(), shorter = f.bytes();
    longer.push_back(0);
    shorter.pop_back();
    CHECK_EQ(replay(longer, 2, every), std::string("Malformed string fixups"));
    CHECK_EQ(replay(shorter, 2, every), std::string("Malformed string fixups"));
    CHECK_EQ(replay(f.bytes(), 3, every), std::string("Malformed string fixups"));
    // The diff writes them in slot order only, each slot once.
    CHECK_EQ(refusal_of<std::logic_error>([&] { f.add(8, 1); }), std::string("String fixups out of slot order"));
}

// The section's header gives the fixups' count and byte size as u32s, which a
// merge of big enough files would pass. Under lower limits, a fixup past
// either is refused before a byte of it is written, and the fixups before it
// read back as they were; one that fits the room left still goes in. So is
// one that skips more slots than a u32 varint holds, after one that skips
// the most.
TEST(patch_format_string_fixups_refuse_one_past_the_sections_limits) {
    using Taken = std::vector<std::optional<uint32_t>>;
    // At most 2 fixups.
    StringFixups counted("test.bin", {2, UINT32_MAX});
    counted.add(0, 1);
    counted.add(1, 2);
    CHECK_EQ(refusal_of([&] { counted.add(2, 3); }), std::string("String fixups of test.bin: more than the section's 2 fixups"));
    CHECK_EQ(counted.count(), 2u);
    CHECK_EQ(counted.size(), 4u);
    CHECK(take_string_fixups(counted.bytes(), counted.count(), {0, 1, 2}) == Taken({1u, 2u, std::nullopt}));

    // At most 6 bytes: fixups of a byte for the slots skipped and one for the
    // value's change, but 64 up zigzags to 128, two bytes.
    StringFixups sized("test.bin", {UINT32_MAX, 6});
    sized.add(0, 1);
    sized.add(1, 2);
    const std::string past_6_bytes = "String fixups of test.bin: more than the section's 6 bytes";
    CHECK_EQ(refusal_of([&] { sized.add(2, 66); }), past_6_bytes);
    CHECK_EQ(sized.size(), 4u);
    sized.add(2, 3);
    CHECK_EQ(refusal_of([&] { sized.add(3, 4); }), past_6_bytes);
    CHECK_EQ(sized.count(), 3u);
    CHECK_EQ(sized.size(), 6u);
    CHECK_EQ(sized.bytes().size(), size_t{6});
    CHECK(take_string_fixups(sized.bytes(), sized.count(), {0, 1, 2, 3}) == Taken({1u, 2u, 3u, std::nullopt}));

    // A skip of UINT32_MAX slots, then one more than that.
    StringFixups skipping;
    const uint64_t far = 1 + uint64_t{UINT32_MAX}, too_far = far + 1 + uint64_t{UINT32_MAX} + 1;
    skipping.add(0, 1);
    skipping.add(far, 2);
    CHECK_EQ(refusal_of([&] { skipping.add(too_far, 3); }),
             std::string("String fixups of a merge: more than 4294967295 slots skipped between two"));
    CHECK(take_string_fixups(skipping.bytes(), skipping.count(), {0, far, too_far}) == Taken({1u, 2u, std::nullopt}));
}

// --- ascending ids, sections by entries file ---

// The ids of v where they sit, as the patcher reads a cell change list.
static AscendingIds ascending(const std::vector<uint64_t>& v) {
    return AscendingIds(reinterpret_cast<const char*>(v.data()), v.size(), "cell changes");
}

TEST(patch_format_ascending_ids_skip_to_the_first_id_not_below_the_one_asked_for) {
    const std::vector<uint64_t> ids = {3, 5, 5, 9};
    AscendingIds sut = ascending(ids);

    CHECK(!sut.skip_to(1));
    CHECK_EQ(sut.front(), uint64_t(3));
    CHECK(sut.skip_to(5));
    CHECK(sut.skip_to(5));  // an id stays until the walk passes it
    CHECK(!sut.skip_to(7));
    CHECK_EQ(sut.front(), uint64_t(9));
    CHECK(sut.skip_to(9));
    CHECK(!sut.skip_to(10));
    CHECK(sut.done());
}

TEST(patch_format_ascending_ids_pop_equal_ids_one_by_one) {
    const std::vector<uint64_t> ids = {4, 4, 6};
    std::vector<uint64_t> got;

    for (AscendingIds sut = ascending(ids); !sut.done(); sut.pop()) got.push_back(sut.front());

    CHECK(got == ids);
    CHECK(AscendingIds().done());
}

TEST(patch_format_ascending_ids_refuse_a_step_back_when_the_walk_reaches_it) {
    const std::vector<uint64_t> ids = {3, 9, 4};
    AscendingIds sut = ascending(ids);
    CHECK(sut.skip_to(9));

    CHECK_EQ(refusal_of([&] { sut.skip_to(10); }), std::string("Malformed cell changes"));
}

TEST(patch_format_ascending_ids_refuse_a_step_back_among_the_ids_the_walk_left) {
    const std::vector<uint64_t> ids = {3, 9, 4};
    AscendingIds sut = ascending(ids);
    CHECK(sut.skip_to(9));

    CHECK_EQ(refusal_of([&] { sut.require_ascending(); }), std::string("Malformed cell changes"));
}

TEST(patch_format_entries_file_sections_keep_the_section_of_each_file) {
    EntriesFileSections sut("cell index delta", CELL_INDEX_ENTRIES_FILES);

    sut.add(static_cast<uint32_t>(PatchFileId::POI_ENTRIES), {40, 8});

    const PatchRef* poi = sut.find(PatchFileId::POI_ENTRIES);
    REQUIRE(poi != nullptr);
    CHECK_EQ(poi->pos, size_t(40));
    CHECK_EQ(poi->size, size_t(8));
    CHECK(sut.find(PatchFileId::ADMIN_ENTRIES) == nullptr);
    CHECK(sut.find(PatchFileId::STREET_LISTS) == nullptr);
}

TEST(patch_format_entries_file_sections_refuse_a_second_section_or_another_file) {
    struct Case { uint32_t fid; std::string refusal; };
    const Case cases[] = {
        {static_cast<uint32_t>(PatchFileId::POI_ENTRIES), "Malformed cell index delta: a second one for poi_entries.bin"},
        {static_cast<uint32_t>(PatchFileId::STREET_LISTS), "Malformed cell index delta for file 9"},
        {999, "Malformed cell index delta for file 999"},
    };
    for (const Case& c : cases) {
        EntriesFileSections sut("cell index delta", CELL_INDEX_ENTRIES_FILES);
        sut.add(static_cast<uint32_t>(PatchFileId::POI_ENTRIES), {40, 8});

        CHECK_EQ(refusal_of([&] { sut.add(c.fid, {60, 8}); }), c.refusal);
        CHECK_EQ(sut.find(PatchFileId::POI_ENTRIES)->pos, size_t(40));
    }
}

// --- sorted tables, parent-id remap ---

// pairs as the patch carries them, in the order given.
static std::vector<char> pair_bytes(const std::vector<RemapPair>& pairs) {
    std::vector<char> out(pairs.size() * sizeof(RemapPair));
    if (!out.empty()) memcpy(out.data(), pairs.data(), out.size());
    return out;
}

// The pairs of bytes where they sit, as the patcher reads a remap's.
static SortedTable<RemapPair> pair_table(const std::vector<char>& bytes) {
    return SortedTable<RemapPair>(bytes.data(), bytes.size() / sizeof(RemapPair), "test pairs");
}

// The dir the tests' scratch tables spill to, the remaps' runs among them.
static const std::string& scratch_tables() {
    static const ScratchDir dir("scratch-tables");
    return dir.path();
}

TEST(patch_format_sorted_table_finds_every_key_and_no_other) {
    // Sizes around the directory's: every key in it, then every second one.
    for (const size_t n : {size_t(0), size_t(1), TABLE_DIRECTORY_KEYS - 1, TABLE_DIRECTORY_KEYS, TABLE_DIRECTORY_KEYS + 1,
                           size_t(100000)}) {
        std::vector<RemapPair> pairs;
        for (uint32_t i = 0; i < n; i++) pairs.push_back({3 * i + 1, i ^ 0x5a5a5a5au});
        const std::vector<char> bytes = pair_bytes(pairs);

        const SortedTable<RemapPair> sut = pair_table(bytes);

        CHECK_EQ(sut.size(), n);
        CHECK(sut.data() == bytes.data());
        size_t wrong = 0;
        for (const RemapPair& p : pairs) {
            const std::optional<RemapPair> hit = sut.find(p.from);
            if (!hit || hit->to != p.to) wrong++;
            if (sut.find(p.from - 1) || sut.find(p.from + 1)) wrong++;
        }
        CHECK_EQ(wrong, size_t(0));
        CHECK(!sut.find(UINT32_MAX));
    }
}

// n strictly ascending keys from a seeded generator: 0, then gaps of 1 to
// 64K, the last key moved to UINT32_MAX when n > 1.
static std::vector<uint32_t> spread_keys(size_t n, uint32_t seed) {
    std::mt19937 rng(seed);
    std::vector<uint32_t> keys;
    uint32_t key = 0;
    for (size_t i = 0; i < n; i++) {
        keys.push_back(key);
        const uint32_t widest = 2u << (rng() % 16);
        key += 1 + rng() % widest;
    }
    if (n > 1) keys.back() = UINT32_MAX;
    return keys;
}

// A table of keys whose records map each key to its complement.
static std::vector<char> complement_pair_bytes(const std::vector<uint32_t>& keys) {
    std::vector<RemapPair> pairs;
    for (const uint32_t key : keys) pairs.push_back({key, ~key});
    return pair_bytes(pairs);
}

TEST(patch_format_sorted_table_find_and_filter_agree_with_upper_bound) {
    // Sizes around the directory's (every key in it up to TABLE_DIRECTORY_KEYS)
    // and a normal day's 90 moved strings.
    for (const size_t n : {size_t(0), size_t(1), size_t(90), TABLE_DIRECTORY_KEYS, TABLE_DIRECTORY_KEYS + 1, size_t(100000)}) {
        const std::vector<uint32_t> keys = spread_keys(n, static_cast<uint32_t>(n));
        const std::vector<char> bytes = complement_pair_bytes(keys);
        std::vector<uint32_t> probes = {0, 1, UINT32_MAX - 1, UINT32_MAX};
        for (const uint32_t key : keys) probes.insert(probes.end(), {key - 1, key, key + 1});
        std::mt19937 rng(7);
        for (int i = 0; i < 100000; i++) probes.push_back(rng());

        const SortedTable<RemapPair> sut = pair_table(bytes);

        size_t wrong = 0, held_rejected = 0;
        for (const uint32_t probe : probes) {
            const auto above = std::upper_bound(keys.begin(), keys.end(), probe);
            const bool held = above != keys.begin() && above[-1] == probe;
            const std::optional<RemapPair> hit = sut.find(probe);
            if (hit.has_value() != held || (held && (hit->from != probe || hit->to != ~probe))) wrong++;
            if (held && !sut.might_hold(probe)) held_rejected++;
        }
        CHECK_EQ(wrong, size_t(0));
        CHECK_EQ(held_rejected, size_t(0));
    }
}

TEST(patch_format_sorted_table_filter_passes_few_keys_a_small_table_lacks) {
    // A normal day's 90 moved strings.
    const std::vector<uint32_t> keys = spread_keys(90, 3);
    const std::vector<char> bytes = complement_pair_bytes(keys);
    std::mt19937 rng(11);

    const SortedTable<RemapPair> sut = pair_table(bytes);

    size_t lacked = 0, passed = 0;
    for (int i = 0; i < 1000000; i++) {
        const uint32_t key = rng();
        if (std::binary_search(keys.begin(), keys.end(), key)) continue;
        lacked++;
        passed += sut.might_hold(key);
    }
    CHECK(passed * 100 < lacked);
    CHECK(!SortedTable<RemapPair>().might_hold(0));
}

TEST(patch_format_sorted_table_refuses_a_key_out_of_order_or_twice) {
    std::vector<RemapPair> many;
    for (uint32_t i = 0; i < 3 * TABLE_DIRECTORY_KEYS; i++) many.push_back({2 * i, i});
    std::vector<RemapPair> many_twice = many;
    many_twice[5000].from = many_twice[4999].from;  // between two keys of the directory
    const std::vector<RemapPair> cases[] = {
        {{5, 0}, {2, 0}, {9, 0}},
        {{2, 0}, {9, 0}, {5, 0}},
        {{2, 0}, {5, 0}, {5, 1}},
        many_twice,
    };
    for (const auto& pairs : cases) {
        const std::vector<char> bytes = pair_bytes(pairs);

        CHECK_EQ(refusal_of([&] { pair_table(bytes); }), std::string("Malformed test pairs"));
    }
}

TEST(patch_format_sorted_and_scratch_tables_floor_as_upper_bound_does) {
    // Sizes around the directory's (every key in it up to
    // TABLE_DIRECTORY_KEYS), the scratch table's 100K records across many
    // buffers. Probes go in key order, as the hint is for, and shuffled.
    for (const size_t n : {size_t(0), size_t(1), TABLE_DIRECTORY_KEYS - 1, TABLE_DIRECTORY_KEYS, TABLE_DIRECTORY_KEYS + 1,
                           size_t(100000)}) {
        const std::vector<uint32_t> keys = spread_keys(n, static_cast<uint32_t>(n) + 1);
        const std::vector<char> bytes = complement_pair_bytes(keys);
        std::vector<uint32_t> probes = {0, 1, UINT32_MAX - 1, UINT32_MAX};
        for (const uint32_t key : keys) probes.insert(probes.end(), {key - 1, key, key + 1});
        std::sort(probes.begin(), probes.end());
        std::vector<uint32_t> shuffled = probes;
        std::shuffle(shuffled.begin(), shuffled.end(), std::mt19937(19));
        ScratchTable<RemapPair> scratch(scratch_tables(), "test pairs");
        for (const uint32_t key : keys) scratch.append({key, ~key});

        const SortedTable<RemapPair> in_place = pair_table(bytes);
        const SortedTable<RemapPair>& spilled = scratch.finish();

        CHECK_EQ(spilled.size(), n);
        size_t wrong = 0;
        for (size_t i = 0; i < n; i++) wrong += spilled[i].from != keys[i] || spilled[i].to != ~keys[i];
        for (const SortedTable<RemapPair>* sut : {&in_place, &spilled})
            for (const std::vector<uint32_t>* order : {&probes, &shuffled}) {
                size_t hint = 0;
                for (const uint32_t probe : *order) {
                    const size_t above = static_cast<size_t>(std::upper_bound(keys.begin(), keys.end(), probe) - keys.begin());
                    const size_t want = above == 0 ? n : above - 1;
                    wrong += sut->floor(probe) != want;
                    wrong += sut->floor(probe, hint) != want || hint != want;
                }
            }
        CHECK_EQ(wrong, size_t(0));
    }
}

TEST(patch_format_scratch_table_refuses_a_key_out_of_order_or_twice) {
    // The last case steps back past the first buffer, which went to the file.
    std::vector<uint32_t> many(3 * SCRATCH_TABLE_BUF / sizeof(RemapPair));
    std::iota(many.begin(), many.end(), 0u);
    many.back() = 7;
    const std::vector<uint32_t> cases[] = {{5, 2}, {2, 5, 5}, many};
    for (const auto& keys : cases) {
        ScratchTable<RemapPair> sut(scratch_tables(), "test runs");

        CHECK_EQ(refusal_of([&] { for (const uint32_t key : keys) sut.append({key, 0}); }), std::string("Malformed test runs"));
    }
}

TEST(patch_format_scratch_table_holds_its_records_in_an_unnamed_file) {
    // Three buffers' worth: the table is mapped from a file the dir no
    // longer names, and a move keeps the mapping.
    ScratchDir dir("scratch-table");
    const uint32_t n = 3 * SCRATCH_TABLE_BUF / sizeof(RemapPair) + 1;
    ScratchTable<RemapPair> sut(dir.path(), "test pairs");
    for (uint32_t i = 0; i < n; i++) sut.append({2 * i, i});

    const char* data = sut.finish().data();

    CHECK(std::filesystem::is_empty(dir.path()));
    const ScratchTable<RemapPair> moved = std::move(sut);
    CHECK(moved.table().data() == data);
    REQUIRE(moved.table().size() == size_t(n));
    CHECK_EQ(moved.table()[n - 1].to, n - 1);
    CHECK_EQ(moved.table().floor(2 * n), size_t(n - 1));
}

TEST(patch_format_scratch_table_maps_nothing_until_a_record_comes) {
    ScratchDir dir("scratch-table");
    ScratchTable<RemapPair> none(dir.path(), "test pairs");

    CHECK(none.finish().empty());
    CHECK(none.table().data() == nullptr);
    CHECK(ScratchTable<RemapPair>().finish().empty());
}

TEST(patch_format_scratch_table_refuses_a_dir_it_cannot_write) {
    ScratchDir dir("scratch-table");
    ScratchTable<RemapPair> gone(dir.path() + "/gone", "test pairs"), none;
    gone.append({1, 1});
    none.append({1, 1});

    CHECK_EQ(refusal_of([&] { gone.finish(); }),
             "Cannot write test pairs to " + dir.path() + "/gone: No such file or directory");
    CHECK_EQ(refusal_of([&] { none.finish(); }), std::string("No scratch dir for scratch table"));
}

// A filesystem that flushes late writes (NFS, FUSE) may report a failed one
// only when the file closes: the table is refused rather than read back.
TEST(patch_format_scratch_table_refuses_a_file_it_cannot_close) {
    ScratchDir dir("scratch-table");
    ScratchTable<RemapPair> sut(dir.path(), "test pairs");
    for (uint32_t i = 0; i <= 3 * SCRATCH_TABLE_BUF / sizeof(RemapPair); i++) sut.append({2 * i, i});
    scratch_table_close() = [](int fd) {
        ::close(fd);
        errno = EIO;
        return -1;
    };

    const std::string refusal = refusal_of([&] { sut.finish(); });
    scratch_table_close() = ::close;

    CHECK_EQ(refusal, "Cannot write test pairs to " + dir.path() + ": Input/output error");
    CHECK(sut.table().empty());
}

// Legs of a parent-id remap after its marker, each a count and its pairs:
// v14's is the street leg alone; v13's were the admin, street and reserved
// postcode legs.
static std::vector<char> parent_id_remap_bytes(const std::vector<std::vector<RemapPair>>& legs) {
    std::vector<char> out;
    for (const std::vector<RemapPair>& leg : legs) {
        const uint32_t n = static_cast<uint32_t>(leg.size());
        out.insert(out.end(), reinterpret_cast<const char*>(&n), reinterpret_cast<const char*>(&n) + 4);
        const std::vector<char> pairs = pair_bytes(leg);
        out.insert(out.end(), pairs.begin(), pairs.end());
    }
    return out;
}

// The street leg is the whole section: the parse reads its pairs where they
// sit and stops past them, so the words of a three-leg (v13) remap after its
// first leg are left to the section loop (which refuses them,
// patch_refuses_a_three_leg_parent_id_remap).
TEST(patch_format_parent_id_remap_reads_its_street_leg_where_it_sits) {
    const std::vector<char> bytes = parent_id_remap_bytes({{{1, 0}, {7, 3}}});
    size_t pos = 0;

    const SortedTable<RemapPair> sut = parse_parent_id_remap(bytes.data(), bytes.size(), pos);

    CHECK_EQ(pos, bytes.size());
    CHECK(sut.data() == bytes.data() + 4);
    CHECK_EQ(sut.size(), size_t(2));
    CHECK_EQ(sut.find(7)->to, 3u);
    CHECK(!sut.find(4));

    const std::vector<char> three_legs = parent_id_remap_bytes({{{1, 0}, {7, 3}}, {{4, 9}}, {}});
    pos = 0;
    CHECK_EQ(parse_parent_id_remap(three_legs.data(), three_legs.size(), pos).size(), size_t(2));
    CHECK_EQ(pos, 4 + 2 * sizeof(RemapPair));
}

TEST(patch_format_parent_id_remap_refuses_a_leg_out_of_order_or_cut_short) {
    struct Case { std::vector<char> bytes; std::string refusal; };
    const std::vector<char> whole = parent_id_remap_bytes({{{4, 9}, {6, 2}}});
    std::vector<Case> cases = {
        {parent_id_remap_bytes({{{6, 2}, {4, 9}}}), "Malformed POI parent-id remap"},
        {parent_id_remap_bytes({{{4, 9}, {4, 2}}}), "Malformed POI parent-id remap"},
    };
    for (size_t cut = 0; cut < whole.size(); cut++)
        cases.push_back({std::vector<char>(whole.begin(), whole.begin() + cut), "Truncated POI parent-id remap"});
    for (const Case& c : cases) {
        size_t pos = 0;

        CHECK_EQ(refusal_of([&] { parse_parent_id_remap(c.bytes.data(), c.bytes.size(), pos); }), c.refusal);
    }
}

// --- cell index rebuild from an id remap ---

// The level-10 cell of number n: the cell pair files hold cells of one level.
static uint64_t c10(uint64_t n) { return cell_pairs::cell_of(n, 10); }

// lists by cell number, at their level-10 cells.
static CellLists at10(const CellLists& lists) {
    CellLists out;
    for (const auto& [n, ids] : lists) out.emplace_hint(out.end(), c10(n), ids);
    return out;
}

TEST(patch_format_empty_cell_lists_round_trip) {
    // An emptied cell keeps an empty list; parsing and rebuilding it copy
    // no ids (its vector's data() is null).
    for (cell_pairs::Format format : {cell_pairs::ADMIN, cell_pairs::POI, cell_pairs::POSTCODE_CENTROID}) {
        auto [cells, entries] = write_cell_lists(at10({{7, {}}, {9, {4}}}), format);
        CHECK(parse_cell_lists(cells, entries, format) == at10({{7, {}}, {9, {4}}}));
    }
    // The cell stays, with its list empty.
    CHECK(rebuild_cell_lists(at10({{42, {}}}), {{3, 2}}) == at10({{42, {}}}));
    CHECK(rebuild_cell_lists({}, {{3, 2}}).empty());
}

TEST(patch_format_rebuild_cells_remaps_interior_entries) {
    // Interior entries carry INTERIOR_FLAG in the top bit; the remap is keyed
    // by the bare id and the flag survives it.
    const uint32_t interior = 0x80000000u;
    const std::unordered_map<uint32_t, uint32_t> rm = {{3, 2}, {5, 4}};
    CHECK(rebuild_cell_lists(at10({{42, {3 | interior, 5}}}), rm) == at10({{42, {4, 2 | interior}}}));
}

TEST(patch_format_rebuild_cells_drops_removed_and_adds_empty_cells) {
    // Removed cells go, added ones come with empty lists; a cell the old index
    // holds can't be added, as the patcher's walk refuses it (removed or not).
    const CellLists old = at10({{1, {5}}, {2, {6}}, {3, {7}}});
    CHECK(rebuild_cell_lists(old, {}, {c10(0), c10(4)}, {c10(2)}) == at10({{0, {}}, {1, {5}}, {3, {7}}, {4, {}}}));
    CHECK_EQ(refusal_of([&] { rebuild_cell_lists(old, {}, {c10(3)}); }), std::string("Malformed cell index"));
    CHECK_EQ(refusal_of([&] { rebuild_cell_lists(old, {}, {c10(3)}, {c10(3)}); }), std::string("Malformed cell index"));
}

// --- cell list delta ---

static void write_bytes(const std::string& path, const std::vector<char>& bytes) {
    std::ofstream(path, std::ios::binary).write(bytes.data(), bytes.size());
}

TEST(patch_format_cell_lists_write_the_cell_pair_files) {
    // The bytes a cell_pairs::Writer writes for the lists in cell order, which
    // parse back to them.
    const CellLists lists = at10({{9, {4}}, {7, {1, 2}}});
    std::vector<char> cells, entries;
    cell_pairs::Writer writer(
        cell_pairs::ADMIN, [&](const char* p, size_t n) { cells.insert(cells.end(), p, p + n); },
        [&](const char* p, size_t n) { entries.insert(entries.end(), p, p + n); });
    const std::vector<uint32_t> a = {1, 2}, b = {4};
    writer.add(c10(7), a.data(), a.size());
    writer.add(c10(9), b.data(), b.size());
    writer.finish();
    CHECK(write_cell_lists(lists, cell_pairs::ADMIN) == std::make_pair(cells, entries));
    CHECK(parse_cell_lists(cells, entries, cell_pairs::ADMIN) == lists);
    CHECK_EQ(refusal_of([&] { parse_cell_lists(cells, entries, cell_pairs::POI); }),
             std::string("Malformed cell index: a cells file of another kind of index"));
}

// The index stream_cell_list_delta writes for the old index plus the delta
// from old_lists to new_lists.
static std::pair<std::vector<char>, std::vector<char>> apply_delta(
        const std::pair<std::vector<char>, std::vector<char>>& old_index, const std::vector<char>& delta, size_t delta_size,
        cell_pairs::Format format = cell_pairs::POSTCODE_CENTROID) {
    std::pair<std::vector<char>, std::vector<char>> out;
    stream_cell_list_delta(ByteSpan{old_index.first.data(), old_index.first.size()},
                           ByteSpan{old_index.second.data(), old_index.second.size()}, format,
                           delta.data(), delta_size,
                           [&](const char* p, size_t n) { out.first.insert(out.first.end(), p, p + n); },
                           [&](const char* p, size_t n) { out.second.insert(out.second.end(), p, p + n); });
    return out;
}

// Applying the delta from old_lists to new_lists to the old index gives the new index, in each format.
static bool delta_applies(const CellLists& old_lists, const CellLists& new_lists) {
    std::vector<char> delta;
    append_cell_list_delta(delta, old_lists, new_lists);
    bool ok = true;
    for (cell_pairs::Format format : {cell_pairs::POSTCODE_CENTROID, cell_pairs::ADMIN, cell_pairs::POI})
        ok &= apply_delta(write_cell_lists(old_lists, format), delta, delta.size(), format) ==
              write_cell_lists(new_lists, format);
    return ok;
}

TEST(patch_format_cell_list_delta_round_trip) {
    const CellLists old_lists = {{1, {10, 11}}, {2, {20, 21, 22}}, {3, {30}}, {5, {50}}};
    const CellLists new_lists = {{0, {7}}, {1, {10, 11}}, {2, {20, 22, 23}}, {4, {40, 41}}, {5, {}}, {9, {90}}};
    std::vector<char> delta;
    append_cell_list_delta(delta, old_lists, new_lists);
    CHECK(delta_applies(at10(old_lists), at10(new_lists)));
    // Unchanged cells don't travel. Source ranks 1 2 3 5 -> 0 1 2 3, new ranks 0 1 2 4 5 9 -> 0 1 2 3 4 5:
    // gone source rank 2 (cell 3); new cells 0, 4 and 9; lost 21 at rank 2 and 50 at rank 4; gained 7 at rank 0, 23 at
    // rank 2, 40 and 41 at rank 3 and 90 at rank 5, each run (start gap, length - 1, id << 1).
    const std::vector<uint8_t> want = {1, 2,                                                // gone: rank 2
                                       3, 0, 3, 4,                                          // new: 0, 4 (1 + 3), 9 (5 + 4)
                                       2, 2, 0, 42, 2, 0, 100,                              // lost
                                       5, 0, 0, 14, 2, 0, 46, 1, 0, 80, 0, 0, 82, 2, 0, 0xB4, 0x01};  // gained
    CHECK(std::vector<uint8_t>(delta.begin(), delta.end()) == want);
}

// The old index (old_lists by cell number) plus a delta of hand-written varints (the gone ranks and runs as their
// values, the new cells by number, as gaps between their level-10 cells), applied: the refusal, or "" when it
// applies.
static std::string refusal_applying(const CellLists& old_lists, const std::vector<uint64_t>& gone, const std::vector<uint64_t>& fresh,
                                    const std::vector<std::array<uint64_t, 3>>& lost, const std::vector<std::array<uint64_t, 3>>& gained) {
    std::vector<char> delta;
    write_varint64(delta, gone.size());
    for (const uint64_t v : gone) write_varint64(delta, v);
    write_varint64(delta, fresh.size());
    uint64_t next = 0;
    for (const uint64_t n : fresh) {
        write_varint64(delta, c10(n) - next);
        next = c10(n) + 1;
    }
    for (const auto* runs : {&lost, &gained}) {
        write_varint64(delta, runs->size());
        for (const auto& r : *runs) {
            write_varint64(delta, r[0]);
            write_varint64(delta, r[1]);
            write_varint(delta, static_cast<uint32_t>(r[2]));
        }
    }
    return refusal_of([&] { apply_delta(write_cell_lists(at10(old_lists), cell_pairs::ADMIN), delta, delta.size(), cell_pairs::ADMIN); });
}

// A polygon that leaves 800 cells in a row and becomes interior in 100 of them costs one lost and one gained run,
// not 900 cell lists.
TEST(patch_format_cell_list_delta_sends_a_polygon_as_runs) {
    constexpr uint32_t INTERIOR = 0x80000000u;
    CellLists old_lists, new_lists;
    for (uint64_t k = 0; k < 1000; k++) {
        old_lists[c10(k * 4)] = {3, 7};
        new_lists[c10(k * 4)] = k < 100 || k >= 900 ? std::vector<uint32_t>{3, 7} : std::vector<uint32_t>{3};
        if (k >= 400 && k < 500) new_lists[c10(k * 4)] = {3, 7 | INTERIOR};
    }
    std::vector<char> delta;
    append_cell_list_delta(delta, old_lists, new_lists);
    CHECK(delta_applies(old_lists, new_lists));
    // No gone or new cells; lost 7 over ranks 100-899, gained 7 | INTERIOR (7 << 1 | 1) over 400-499.
    const std::vector<uint8_t> want = {0, 0, 1, 100, 0x9F, 0x06, 14, 1, 0x90, 0x03, 99, 15};
    CHECK(std::vector<uint8_t>(delta.begin(), delta.end()) == want);
}

// A cell list delta read as the layout has it, with this test's own reader: the gone ranks, the new cell ids, and
// the lost and gained runs as (start, last, id), each list in order.
struct DecodedDelta {
    std::vector<uint64_t> gone, fresh;
    std::vector<std::array<uint64_t, 3>> runs[2];
};

static DecodedDelta decode_delta(const std::vector<char>& delta) {
    DecodedDelta d;
    size_t pos = 0;
    SectionReader in(delta.data(), delta.size(), pos, "decoded delta", PastEnd::MALFORMED);
    for (auto* list : {&d.gone, &d.fresh}) {
        uint64_t next = 0;
        for (uint64_t k = in.varint64(); k > 0; k--) {
            list->push_back(next + in.varint64());
            next = list->back() + 1;
        }
    }
    for (auto& runs : d.runs) {
        uint64_t start = 0;
        for (uint64_t k = in.varint64(); k > 0; k--) {
            start += in.varint64();
            const uint64_t last = start + in.varint64();
            const uint32_t v = in.varint();
            runs.push_back({start, last, (v >> 1) | (uint64_t{v & 1} << 31)});
        }
    }
    if (!in.at_end()) in.malformed();
    return d;
}

// The number of runs an id needs at a list of (rank, count) pairs: a run starts wherever its count rises.
static uint64_t runs_needed(const std::map<uint64_t, uint32_t>& counts) {
    uint64_t n = 0, prev_rank = 0;
    uint32_t prev = 0;
    for (const auto& [rank, c] : counts) {
        const uint32_t before = rank == prev_rank + 1 ? prev : 0;
        if (c > before) n += c - before;
        prev_rank = rank;
        prev = c;
    }
    return n;
}

// On random days with gone and new cells, interior flags, long runs and ids a list holds twice (lost once, or
// both times): the delta applies to the new index, its runs expand to exactly the (rank, id) changes of the lists, and
// it has as few runs as an id's counts allow.
TEST(patch_format_cell_list_delta_runs_match_the_list_changes_on_random_days) {
    std::mt19937 rng(20261108);
    for (int round = 0; round < 300; round++) {
        CellLists old_lists, new_lists;
        const uint32_t n_ids = 1 + rng() % 12;
        auto list = [&] {
            std::vector<uint32_t> ids;
            for (uint32_t id = 0; id < n_ids; id++)
                for (uint32_t copies = rng() % 7 == 0 ? 2 : 1; copies > 0; copies--)
                    if (rng() % 3 == 0) ids.push_back(id | (rng() % 5 == 0 ? 0x80000000u : 0));
            std::sort(ids.begin(), ids.end());
            return ids;
        };
        std::vector<uint32_t> stretch;
        for (uint64_t cid = 0; cid < 120; cid++) {
            if (rng() % 10 == 0) stretch = list();  // a stretch of cells listing the same ids
            const uint32_t kind = rng() % 8;
            if (kind != 0) old_lists[c10(cid)] = rng() % 2 ? stretch : list();
            if (kind == 1) continue;  // gone
            new_lists[c10(cid)] = kind == 2 || kind == 3 ? old_lists[c10(cid)] : rng() % 2 ? stretch : list();
        }
        std::vector<char> delta;
        append_cell_list_delta(delta, old_lists, new_lists);
        CHECK(delta_applies(old_lists, new_lists));

        // The gone source ranks and the new cells; the changes by new rank: the lost and gained (id, rank) multisets
        // of each cell's lists.
        std::vector<uint64_t> gone, fresh;
        uint64_t rank = 0;
        for (const auto& kv : old_lists) {
            if (!new_lists.count(kv.first)) gone.push_back(rank);
            rank++;
        }
        for (const auto& kv : new_lists)
            if (!old_lists.count(kv.first)) fresh.push_back(kv.first);
        std::map<std::pair<uint32_t, uint64_t>, uint32_t> want[2];
        rank = 0;
        for (const auto& [cid, ids] : new_lists) {
            auto it = old_lists.find(cid);
            const std::vector<uint32_t> was = it == old_lists.end() ? std::vector<uint32_t>{} : it->second;
            std::vector<uint32_t> d[2];
            std::set_difference(was.begin(), was.end(), ids.begin(), ids.end(), std::back_inserter(d[0]));
            std::set_difference(ids.begin(), ids.end(), was.begin(), was.end(), std::back_inserter(d[1]));
            for (int s = 0; s < 2; s++)
                for (const uint32_t id : d[s]) want[s][{id, rank}]++;
            rank++;
        }
        const DecodedDelta d = decode_delta(delta);
        CHECK(d.gone == gone);
        CHECK(d.fresh == fresh);
        for (int s = 0; s < 2; s++) {
            std::map<std::pair<uint32_t, uint64_t>, uint32_t> got;
            for (const auto& [start, last, id] : d.runs[s])
                for (uint64_t k = start; k <= last; k++) got[{static_cast<uint32_t>(id), k}]++;
            CHECK(std::is_sorted(d.runs[s].begin(), d.runs[s].end(), [](const auto& a, const auto& b) {
                return std::tie(a[0], a[2]) < std::tie(b[0], b[2]);
            }));
            CHECK(got == want[s]);
            std::map<uint32_t, std::map<uint64_t, uint32_t>> by_id;
            for (const auto& [key, c] : want[s]) by_id[key.first][key.second] = c;
            uint64_t needed = 0;
            for (const auto& [id, counts] : by_id) needed += runs_needed(counts);
            CHECK_EQ(uint64_t{d.runs[s].size()}, needed);
        }
    }
}

// Refused, each before the walk writes the cell it is about (the patcher drops what it wrote): a lost run over a
// cell that doesn't hold its id, over a cell new to the index, or past the cell that holds it; the ids of the runs
// over a cell past 0xFFFF, lost or gained (0xFFFF - 2 gained by a cell of 2 apply); a run below the previous one's
// (start, id) (the same start, a lower id) and a new cell the source holds; a run past the new index's last rank (the
// old index's 3 cells, one gone, so ranks 0 and 1) and a gone rank past the source's last cell. The same delta's
// well-formed runs apply.
TEST(patch_format_cell_list_delta_refuses_each_malformed_run) {
    const CellLists old_lists = {{10, {1, 2}}, {20, {2, 3}}, {30, {3}}};
    auto many = [](uint64_t n) {
        std::vector<std::array<uint64_t, 3>> runs;
        for (uint64_t k = 0; k < n; k++) runs.push_back({0, 0, (k + 100) << 1});
        return runs;
    };
    const std::string missing = "Malformed cell list delta: a lost id missing from its cell";
    const std::string active = "Malformed cell list delta: an active set above 65535 ids";
    const std::string back = "Malformed cell list delta: a step back";
    const std::string past = "Malformed cell list delta: a run past the last rank";
    CHECK_EQ(refusal_applying(old_lists, {}, {}, {{0, 0, 3 << 1}}, {}), missing);              // 3 not in cell 10
    CHECK_EQ(refusal_applying(old_lists, {}, {}, {{0, 0, (1 << 1) | 1}}, {}), missing);        // 1 | INTERIOR
    CHECK_EQ(refusal_applying(old_lists, {}, {15}, {{1, 0, 2 << 1}}, {}), missing);            // new cell 15
    CHECK_EQ(refusal_applying(old_lists, {}, {}, {{0, 1, 1 << 1}}, {}), missing);              // 1 not in cell 20
    CHECK_EQ(refusal_applying(old_lists, {}, {}, many(0x10000), {}), active);
    CHECK_EQ(refusal_applying(old_lists, {}, {}, {}, many(0x10000)), active);
    CHECK_EQ(refusal_applying(old_lists, {}, {}, {}, many(0xFFFD)), std::string());  // cell 10 then lists 0xFFFF ids
    CHECK_EQ(refusal_applying(old_lists, {}, {}, {{1, 0, 3 << 1}, {0, 0, 2 << 1}}, {}), back);
    CHECK_EQ(refusal_applying(old_lists, {}, {}, {}, {{0, 0, 9 << 1}, {0, 0, (9 << 1) | 1}, {0, 0, 8 << 1}}), back);
    CHECK_EQ(refusal_applying(old_lists, {}, {20}, {}, {}), back);
    CHECK_EQ(refusal_applying(old_lists, {1}, {}, {}, {{2, 0, 5 << 1}}), past);
    CHECK_EQ(refusal_applying(old_lists, {1}, {}, {}, {{1, 1, 5 << 1}}), past);
    CHECK_EQ(refusal_applying(old_lists, {1}, {}, {}, {{1, 0, 5 << 1}}), std::string());
    CHECK_EQ(refusal_applying(old_lists, {3}, {}, {}, {}), std::string("Malformed cell list delta: a gone rank past the last source cell"));
    CHECK_EQ(refusal_applying(old_lists, {2}, {}, {}, {}), std::string());
}

// A run's ranks and the gone ranks are u64s on any size_t, so none wraps into the index on a 32-bit build: a run
// starting at 2^32 + 1, or at 1 for 2^32 cells, is past the last rank as is one whose start or length passes 2^64 (it
// would wrap to rank 0); a gone rank of 2^32 is past the last cell; 2^32 + 1 runs where one is (the count a 32-bit
// size_t would wrap to 1) is a delta cut short; a new cell id past 2^64 is malformed. Each over an index of 3 cells.
TEST(patch_format_cell_list_delta_bounds_ranks_in_64_bits) {
    const CellLists old_lists = {{10, {1, 2}}, {20, {2, 3}}, {30, {3}}};
    const std::string past = "Malformed cell list delta: a run past the last rank";
    const uint64_t two32 = uint64_t{1} << 32;
    CHECK_EQ(refusal_applying(old_lists, {}, {}, {}, {{two32 + 1, 0, 5 << 1}}), past);
    CHECK_EQ(refusal_applying(old_lists, {}, {}, {}, {{1, two32 - 1, 5 << 1}}), past);
    CHECK_EQ(refusal_applying(old_lists, {}, {}, {}, {{1, 0, 5 << 1}, {~0ull, 0, 6 << 1}}), past);
    CHECK_EQ(refusal_applying(old_lists, {}, {}, {}, {{1, ~0ull, 5 << 1}}), past);
    CHECK_EQ(refusal_applying(old_lists, {two32}, {}, {}, {}), std::string("Malformed cell list delta: a gone rank past the last source cell"));
    const auto old_index = write_cell_lists(at10(old_lists), cell_pairs::POSTCODE_CENTROID);
    // A new cell past 2^64: cell 5, then a gap past what is left.
    std::vector<char> beyond = {0, 2};
    write_varint64(beyond, c10(5));
    write_varint64(beyond, ~0ull - c10(5) - 1);
    beyond.insert(beyond.end(), {0, 0});
    CHECK_EQ(refusal_of([&] { apply_delta(old_index, beyond, beyond.size()); }), std::string("Malformed cell list delta"));
    // n_gained 2^32 + 1 over one run.
    std::vector<char> delta = {0, 0, 0};
    write_varint64(delta, two32 + 1);
    write_varint64(delta, 1);
    write_varint64(delta, 0);
    write_varint(delta, 5 << 1);
    CHECK_EQ(refusal_of([&] { apply_delta(old_index, delta, delta.size()); }), std::string("Malformed cell list delta"));
}

TEST(patch_format_cell_list_delta_matches_the_new_index_on_random_days) {
    // Many random old/new indexes: applying the delta to the old bytes gives
    // exactly the bytes write_cell_index writes for the new lists.
    uint64_t seed = 42;
    auto rnd = [&](uint32_t n) { seed = seed * 6364136223846793005ull + 1442695040888963407ull; return (uint32_t)(seed >> 33) % n; };
    for (int round = 0; round < 200; round++) {
        CellLists old_lists, new_lists;
        for (uint64_t cid = 0; cid < 40; cid++) {
            auto make = [&] { std::vector<uint32_t> ids; for (uint32_t id = 0; id < 30; id++) if (rnd(4) == 0) ids.push_back(id); return ids; };
            uint32_t kind = rnd(5);
            if (kind != 0) old_lists[c10(cid)] = make();
            if (kind == 1 || kind == 2) new_lists[c10(cid)] = old_lists[c10(cid)];
            else if (kind != 0 || rnd(2)) new_lists[c10(cid)] = make();
        }
        CHECK(delta_applies(old_lists, new_lists));
    }
}

TEST(patch_format_stream_cell_index_matches_rebuild_then_delta) {
    // The patcher's one-pass rebuild (old index + remap + added/removed +
    // delta) writes exactly the new index the diff aimed at, the bytes the
    // builder writes for its lists, on random days with interior flags,
    // removed and added cells, over cells of several parents, a third of
    // the cells sharing the list of the cell before (a direct table's rows
    // that share a list, which the rebuild remaps once).
    uint64_t seed = 99;
    auto rnd = [&](uint32_t n) { seed = seed * 6364136223846793005ull + 1442695040888963407ull; return (uint32_t)(seed >> 33) % n; };
    // Cell k: two parents of 1024 cells and their neighbours apart, in steps of up to 300 cells.
    auto cell = [](uint64_t k) { return c10(k * 97); };
    ScratchDir dir("cell-index-test");
    for (int round = 0; round < 200; round++) {
        const cell_pairs::Format format = round % 2 ? cell_pairs::POI : cell_pairs::ADMIN;
        CellLists old_lists;
        for (uint64_t cid = 1; cid < 30; cid++) {
            if (rnd(3) == 0) continue;
            std::vector<uint32_t> ids;
            for (uint32_t id = 0; id < 40; id++) if (rnd(5) == 0) ids.push_back(id | (rnd(4) == 0 ? 0x80000000u : 0));
            std::sort(ids.begin(), ids.end());
            if (!old_lists.empty() && rnd(3) == 0) ids = old_lists.rbegin()->second;
            old_lists[cell(cid)] = ids;
        }
        std::unordered_map<uint32_t, uint32_t> rm;
        for (uint32_t id = 0; id < 40; id++) if (rnd(3) == 0) rm[id] = 100 + rnd(60);
        std::vector<uint64_t> added, removed;
        for (uint64_t cid = 1; cid < 34; cid++) {
            if (!old_lists.count(cell(cid)) && rnd(3) == 0) added.push_back(cell(cid));
            else if (old_lists.count(cell(cid)) && rnd(6) == 0) removed.push_back(cell(cid));
        }
        auto [cells, entries] = write_cell_lists(old_lists, format);
        const CellLists derived = rebuild_cell_lists(parse_cell_lists(cells, entries, format), rm, added, removed);
        CellLists new_lists = derived;
        for (auto it = new_lists.begin(); it != new_lists.end();) {
            if (rnd(5) == 0) { it = new_lists.erase(it); continue; }
            if (rnd(3) == 0) { it->second.push_back(500 + rnd(9)); std::sort(it->second.begin(), it->second.end()); }
            if (rnd(4) == 0 && !it->second.empty()) it->second.erase(it->second.begin());
            ++it;
        }
        if (rnd(2)) new_lists[cell(40 + rnd(5))] = {7};
        std::vector<char> delta;
        append_cell_list_delta(delta, derived, new_lists);
        std::pair<std::vector<char>, std::vector<char>> got;
        stream_cell_index(ByteSpan{cells.data(), cells.size()}, ByteSpan{entries.data(), entries.size()}, format,
                          ascending(added), ascending(removed),
                          [&](uint32_t id) { auto f = rm.find(id); return f != rm.end() ? f->second : id; },
                          delta.data(), delta.size(),
                          [&](const char* p, size_t n) { got.first.insert(got.first.end(), p, p + n); },
                          [&](const char* p, size_t n) { got.second.insert(got.second.end(), p, p + n); });
        CHECK(got == write_cell_lists(new_lists, format));
        // The builder's writer (its map one: a cell's list can be empty here).
        std::unordered_map<uint64_t, std::vector<uint32_t>> map(new_lists.begin(), new_lists.end());
        write_cell_index(dir.path() + "/built_cells.bin", dir.path() + "/built_entries.bin", format, map);
        CHECK(read_file(dir.path() + "/built_cells.bin") == got.first);
        CHECK(read_file(dir.path() + "/built_entries.bin") == got.second);

        // The patcher reads the old index from files; a 16-byte window
        // refills mid-list, grows for longer lists and skips removed cells.
        write_bytes(dir.path() + "/cells.bin", cells);
        write_bytes(dir.path() + "/entries.bin", entries);
        SequentialFileReader cells_file(dir.path() + "/cells.bin", 16), entries_file(dir.path() + "/entries.bin", 16);
        std::pair<std::vector<char>, std::vector<char>> streamed;
        stream_cell_index(cells_file, entries_file, format, ascending(added), ascending(removed),
                          [&](uint32_t id) { auto f = rm.find(id); return f != rm.end() ? f->second : id; },
                          delta.data(), delta.size(),
                          [&](const char* p, size_t n) { streamed.first.insert(streamed.first.end(), p, p + n); },
                          [&](const char* p, size_t n) { streamed.second.insert(streamed.second.end(), p, p + n); });
        CHECK(streamed == got);
        CHECK_EQ(cells_file.rewinds() + entries_file.rewinds(), uint64_t(0));
    }
}

TEST(patch_format_stream_cell_index_sorts_the_lists_a_remap_reorders) {
    // The rebuild sorts a remapped list again only where the remap moved an
    // id before the one ahead of it: cells 5, 9 and 11 keep their order
    // through it, cell 7's ids 1 and 3 swap places (its flagged 1 stays
    // last). A block-coded (admin) and a direct (POI) index alike.
    constexpr uint32_t F = 0x80000000u;
    const std::unordered_map<uint32_t, uint32_t> rm = {{1, 30}, {3, 10}, {4, 40}};
    const CellLists old_lists = at10({{5, {2, 4, F | 6}}, {7, {1, 3, F | 1}}, {9, {F | 3, F | 4}}, {11, {4, F | 3}}});
    const CellLists want = at10({{5, {2, 40, F | 6}}, {7, {10, 30, F | 30}}, {9, {F | 10, F | 40}}, {11, {40, F | 10}}});
    CHECK(rebuild_cell_lists(old_lists, rm) == want);
    std::vector<char> delta;
    append_cell_list_delta(delta, {}, {});
    const std::vector<uint64_t> none;
    for (const cell_pairs::Format format : {cell_pairs::ADMIN, cell_pairs::POI}) {
        const auto old_index = write_cell_lists(old_lists, format);
        std::pair<std::vector<char>, std::vector<char>> got;
        stream_cell_index(ByteSpan{old_index.first.data(), old_index.first.size()},
                          ByteSpan{old_index.second.data(), old_index.second.size()}, format, ascending(none),
                          ascending(none), [&](uint32_t id) { auto f = rm.find(id); return f != rm.end() ? f->second : id; },
                          delta.data(), delta.size(),
                          [&](const char* p, size_t n) { got.first.insert(got.first.end(), p, p + n); },
                          [&](const char* p, size_t n) { got.second.insert(got.second.end(), p, p + n); });
        CHECK(got == write_cell_lists(want, format));
    }
}

TEST(patch_format_cell_list_delta_drops_an_emptied_cell) {
    // An index can hold a cell with an empty list; the delta removes it.
    const CellLists old_lists = at10({{7, {1}}, {9, {}}}), new_lists = at10({{7, {1}}});
    std::vector<char> delta;
    append_cell_list_delta(delta, old_lists, new_lists);
    CHECK(apply_delta(write_cell_lists(old_lists, cell_pairs::POSTCODE_CENTROID), delta, delta.size()) ==
          write_cell_lists(new_lists, cell_pairs::POSTCODE_CENTROID));
}

TEST(patch_format_stream_cell_index_refuses_cell_changes_out_of_order) {
    // Over an old index of cells 5, 7 and 9: added cells 3 then 2; removed
    // cells 7 then 5, a step back the walk reaches at cell 9; removed cells 9
    // then 7, one past the last cell the walk asks for.
    const auto old_index = write_cell_lists(at10({{5, {1}}, {7, {2}}, {9, {3}}}), cell_pairs::ADMIN);
    std::vector<char> delta;
    append_cell_list_delta(delta, {}, {});
    struct Case { std::vector<uint64_t> added, removed; };
    for (const Case& c : {Case{{c10(3), c10(2)}, {}}, Case{{}, {c10(7), c10(5)}}, Case{{}, {c10(9), c10(7)}}}) {
        auto walk = [&] {
            stream_cell_index(ByteSpan{old_index.first.data(), old_index.first.size()},
                              ByteSpan{old_index.second.data(), old_index.second.size()}, cell_pairs::ADMIN,
                              ascending(c.added), ascending(c.removed), [](uint32_t id) { return id; }, delta.data(),
                              delta.size(), [](const char*, size_t) {}, [](const char*, size_t) {});
        };

        CHECK_EQ(refusal_of(walk), std::string("Malformed cell changes"));
    }
}

TEST(patch_format_stream_cell_index_refuses_a_malformed_old_index_or_new_cell) {
    // The old index as the walk reads it (a malformed block: a code of 3), and
    // a new cell of another level than the index's: refused before a list of
    // the new index is written past them.
    auto old_index = write_cell_lists(at10({{5, {1, 2}}, {7, {2}}}), cell_pairs::ADMIN);
    old_index.second[0] = static_cast<char>(old_index.second[0] | 0x0C);  // row 1: code 3
    std::vector<char> none;
    append_cell_list_delta(none, {}, {});
    auto walk = [&](const std::pair<std::vector<char>, std::vector<char>>& index, const std::vector<char>& delta) {
        return refusal_of([&] {
            stream_cell_index(ByteSpan{index.first.data(), index.first.size()},
                              ByteSpan{index.second.data(), index.second.size()}, cell_pairs::ADMIN, {}, {},
                              [](uint32_t id) { return id; }, delta.data(), delta.size(), [](const char*, size_t) {},
                              [](const char*, size_t) {});
        });
    };
    CHECK_EQ(walk(old_index, none), std::string("Malformed cell index: a row code of 3"));
    const auto good = write_cell_lists(at10({{5, {1, 2}}}), cell_pairs::ADMIN);
    std::vector<char> fresh;
    append_cell_list_delta(fresh, at10({{5, {1, 2}}}), {{c10(5), {1, 2}}, {cell_pairs::cell_of(9, 11), {3}}});
    CHECK_EQ(walk(good, fresh), std::string("cell_pairs::Writer: cells of two levels"));
}

TEST(patch_format_cell_list_delta_rejects_truncation) {
    std::vector<char> delta;
    append_cell_list_delta(delta, at10({{1, {10}}}), at10({{1, {11}}, {2, {5}}}));
    auto old_index = write_cell_lists(at10({{1, {10}}}), cell_pairs::POSTCODE_CENTROID);
    for (size_t cut = 0; cut < delta.size(); cut++)
        CHECK_EQ(refusal_of([&] { apply_delta(old_index, delta, cut); }), std::string("Malformed cell list delta"));
}

// --- string remap runs ---

namespace {

// A tier pool in the given order: each string once, NUL-terminated.
std::string pool_in_order(const std::vector<std::string>& words) {
    std::unordered_set<std::string> seen;
    std::string out;
    for (const auto& w : words)
        if (seen.insert(w).second) { out += w; out.push_back('\0'); }
    return out;
}

// A tier pool: sorted unique strings, each NUL-terminated.
std::string pool_of(std::vector<std::string> words) {
    std::sort(words.begin(), words.end());
    return pool_in_order(words);
}

// d as the strings section carries a shipped tier.
std::vector<char> bytes_of(const StringTierDiff& d) {
    std::vector<char> out;
    append_string_tier_diff(out, d);
    return out;
}

// A shipped tier's bytes as the patcher reads them, where they sit.
StringTierEdit edit_of(const std::vector<char>& bytes) {
    size_t pos = 0;
    const StringTierEdit e = parse_string_tier_edit(bytes.data(), bytes.size(), pos);
    CHECK_EQ(pos, bytes.size());
    return e;
}

// The n u32 indices of a list where it sits.
std::vector<uint32_t> indices_of(const char* list, uint32_t n) {
    std::vector<uint32_t> out;
    for (uint32_t i = 0; i < n; i++) out.push_back(string_index_at(list, i));
    return out;
}

// One tier's edit between two pools, as the diff makes it (the kept strings
// and the shipped tier's diff) and as the patcher reads its bytes.
struct TierEdit {
    std::vector<std::pair<uint32_t, uint32_t>> kept;
    StringTierDiff diff;
    std::vector<char> bytes;
    StringTierEdit edit() const { return edit_of(bytes); }
};

TierEdit tier_edit(const std::string& o, const std::string& n) {
    std::unordered_map<std::string, uint32_t> new_at;
    for (size_t i = 0; i < n.size(); i += strlen(n.c_str() + i) + 1) new_at[n.c_str() + i] = (uint32_t)i;
    std::vector<std::pair<uint32_t, uint32_t>> common;
    for (size_t i = 0; i < o.size(); i += strlen(o.c_str() + i) + 1) {
        auto it = new_at.find(o.c_str() + i);
        if (it != new_at.end()) common.push_back({(uint32_t)i, it->second});
    }
    TierEdit e;
    e.kept = kept_strings(common);
    e.diff = string_tier_diff(o.data(), o.size(), n.data(), n.size(), e.kept);
    e.bytes = bytes_of(e.diff);
    return e;
}

// The per-string pairs the patcher used to hold: every surviving string's old
// start → new start.
std::unordered_map<uint32_t, uint32_t> pairs_of(const std::string& o, const std::string& n, uint32_t ob, uint32_t nb) {
    std::unordered_map<std::string, uint32_t> new_at;
    for (size_t i = 0; i < n.size(); i += strlen(n.c_str() + i) + 1) new_at[n.c_str() + i] = nb + (uint32_t)i;
    std::unordered_map<uint32_t, uint32_t> out;
    for (size_t i = 0; i < o.size(); i += strlen(o.c_str() + i) + 1) {
        auto it = new_at.find(o.c_str() + i);
        if (it != new_at.end() && it->second != ob + i) out[ob + (uint32_t)i] = it->second;
    }
    return out;
}

// Every string start of a pool, as global offsets from base.
std::vector<uint32_t> starts_of(const std::string& pool, uint32_t base) {
    std::vector<uint32_t> out;
    for (size_t i = 0; i < pool.size(); i += strlen(pool.c_str() + i) + 1) out.push_back(base + (uint32_t)i);
    return out;
}

// A shipped tier's walk (rebuild_string_tier) over an old pool's FCC2 file
// in memory, its runs added to remap: the new pool it wrote, and what it read
// and wrote.
struct Rebuilt {
    std::string pool;
    StringTierRebuild walk;
};

Rebuilt rebuild(std::string_view old_pool, const StringTierEdit& edit, const StringTierPlace& at, StringRemap& remap) {
    Rebuilt out;
    const std::vector<uint8_t> file = string_files::encode(old_pool.data(), old_pool.size());
    string_files::Decoder old(file.data(), file.size());
    out.walk = rebuild_string_tier(old, edit, at, remap, [&](const char* p, size_t n) { out.pool.append(p, n); },
                                   [](size_t, size_t) {});
    return out;
}

// The place of a tier at old offset ob and new offset nb whose new pool is n.
StringTierPlace place_of(uint32_t ob, uint32_t nb, const std::string& n) {
    return {ob, nb, static_cast<uint32_t>(n.size())};
}

}  // namespace

TEST(patch_format_string_remap_runs_match_per_string_pairs) {
    // Random edits to two tiers: every string start (kept, deleted or
    // unmoved) and every offset past the tiers maps exactly as the pairs did.
    uint64_t seed = 7;
    auto rnd = [&](uint32_t n) { seed = seed * 6364136223846793005ull + 1442695040888963407ull; return (uint32_t)(seed >> 33) % n; };
    auto word = [&] { std::string w; for (uint32_t k = 0, len = 1 + rnd(5); k < len; k++) w.push_back('a' + rnd(6)); return w; };
    for (int round = 0; round < 100; round++) {
        std::string olds[2], news[2];
        for (int t = 0; t < 2; t++) {
            std::vector<std::string> o, n;
            for (int i = 0; i < 40; i++) {
                std::string w = word();
                uint32_t kind = rnd(10);
                if (kind != 0) o.push_back(w);           // kind 0: added
                if (kind != 1) n.push_back(w);           // kind 1: deleted
            }
            olds[t] = pool_of(o); news[t] = pool_of(n);
        }
        StringRemap remap(scratch_tables());
        uint32_t ob = 0, nb = 0;
        std::unordered_map<uint32_t, uint32_t> want;
        std::vector<uint32_t> looked_up;
        for (int t = 0; t < 2; t++) {
            rebuild(olds[t], tier_edit(olds[t], news[t]).edit(), place_of(ob, nb, news[t]), remap);
            for (auto& kv : pairs_of(olds[t], news[t], ob, nb)) want.insert(kv);
            for (uint32_t v : starts_of(olds[t], ob)) looked_up.push_back(v);
            ob += olds[t].size(); nb += news[t].size();
        }
        const std::vector<char> moved = pair_bytes({{ob + 100, 3}});  // a string that moved tier
        remap.finish(pair_table(moved));
        want[ob + 100] = 3;
        for (uint32_t off = ob; off < ob + 110; off++) looked_up.push_back(off);
        for (uint32_t off : looked_up) {
            auto it = want.find(off);
            CHECK_EQ(remap.lookup(off), it != want.end() ? it->second : off);
        }
        CHECK_EQ(remap.lookup(0xFFFFFFFFu), 0xFFFFFFFFu);
    }
}

TEST(patch_format_derived_and_sent_string_runs_map_every_offset_alike) {
    // Random edits to a tier: the runs the patcher derives from the tier's
    // old pool and the runs the diff sends for it shift every offset alike,
    // string start or not.
    uint64_t seed = 13;
    auto rnd = [&](uint32_t n) { seed = seed * 6364136223846793005ull + 1442695040888963407ull; return (uint32_t)(seed >> 33) % n; };
    auto word = [&] { std::string w; for (uint32_t k = 0, len = 1 + rnd(5); k < len; k++) w.push_back('a' + rnd(6)); return w; };
    for (int round = 0; round < 100; round++) {
        std::vector<std::string> o, n;
        for (int i = 0; i < 40; i++) {
            std::string w = word();
            uint32_t kind = rnd(10);
            if (kind != 0) o.push_back(w);  // kind 0: added
            if (kind != 1) n.push_back(w);  // kind 1: deleted
        }
        const std::string old_pool = pool_of(o), new_pool = pool_of(n);
        const uint32_t ob = 1000 + rnd(50), nb = 1000 + rnd(50), end = ob + (uint32_t)old_pool.size();
        const TierEdit e = tier_edit(old_pool, new_pool);
        const StringTierRuns runs = encode_string_tier_runs(old_pool.data(), old_pool.size(), ob, nb, e.kept);
        StringRemap derived(scratch_tables()), sent(scratch_tables());
        rebuild(old_pool, e.edit(), place_of(ob, nb, new_pool), derived);
        sent.add_sent_tier(runs.bytes.data(), runs.bytes.size(), runs.n_runs, ob, end);
        derived.finish();
        sent.finish();

        CHECK_EQ(derived.run_count(), sent.run_count());
        std::vector<uint32_t> differ;
        for (uint32_t off = ob - 1; off <= end; off++)
            if (derived.lookup(off) != sent.lookup(off)) differ.push_back(off);
        CHECK(differ.empty());
    }
}

TEST(patch_format_string_remap_lookups_never_read_the_old_pool) {
    // The patcher unmaps a shipped tier's old pool once its walk is done:
    // scribbled over afterwards, every string start still maps as the
    // per-string pairs did.
    const std::string o = pool_of({"a", "bb", "c", "dd", "e", "ff", "g"});
    const std::string n = pool_of({"a", "aa", "bb", "c", "e", "eee", "ff", "g", "h"});
    const uint32_t ob = 1000, nb = 990;
    std::vector<char> old_pool(o.begin(), o.end());
    StringRemap remap(scratch_tables());
    rebuild({old_pool.data(), old_pool.size()}, tier_edit(o, n).edit(), place_of(ob, nb, n), remap);
    remap.finish();

    std::fill(old_pool.begin(), old_pool.end(), 'x');

    const auto want = pairs_of(o, n, ob, nb);
    for (uint32_t v : starts_of(o, ob)) {
        auto it = want.find(v);
        CHECK_EQ(remap.lookup(v), it != want.end() ? it->second : v);
    }
}

TEST(patch_format_sent_string_runs_map_every_string_start) {
    // A tier the client doesn't hold: its runs travel encoded and are looked
    // up without the old pool, exactly for every string start.
    const std::string o = pool_of({"a", "bb", "c", "dd", "e", "ff", "g"});
    const std::string n = pool_of({"a", "aa", "bb", "c", "e", "eee", "ff", "g", "h"});
    const uint32_t ob = 1000, nb = 990;
    auto runs = encode_string_tier_runs(o.data(), o.size(), ob, nb, tier_edit(o, n).kept);
    StringRemap remap(scratch_tables());
    remap.add_sent_tier(runs.bytes.data(), runs.bytes.size(), runs.n_runs, ob, ob + (uint32_t)o.size());
    remap.finish();
    CHECK(!remap.empty());
    CHECK_EQ(remap.run_count(), size_t(runs.n_runs));
    auto want = pairs_of(o, n, ob, nb);
    for (uint32_t v : starts_of(o, ob)) {
        auto it = want.find(v);
        CHECK_EQ(remap.lookup(v), it != want.end() ? it->second : v);
    }
    CHECK_EQ(remap.lookup(ob - 1), ob - 1);
    CHECK_EQ(remap.lookup(ob + (uint32_t)o.size()), ob + (uint32_t)o.size());
}

TEST(patch_format_sent_string_runs_reject_malformed_input) {
    const std::string o = pool_of({"a", "b", "c", "d", "e", "f"});
    const std::string n = pool_of({"0", "a", "b", "bb", "c", "d", "e", "f"});
    auto runs = encode_string_tier_runs(o.data(), o.size(), 100, 100, tier_edit(o, n).kept);
    REQUIRE(runs.n_runs == 2);
    const uint32_t end = 100 + (uint32_t)o.size();
    // What add_sent_tier refused the runs with, or "".
    auto add = [](const std::vector<char>& bytes, uint32_t n_runs, uint32_t tier_start, uint32_t tier_end) {
        StringRemap remap(scratch_tables());
        return refusal_of([&] { remap.add_sent_tier(bytes.data(), bytes.size(), n_runs, tier_start, tier_end); });
    };
    const std::string malformed = "Malformed string runs";
    CHECK_EQ(add(runs.bytes, 2, 100, end), std::string());
    for (size_t cut = 0; cut < runs.bytes.size(); cut++) {
        std::vector<char> short_bytes(runs.bytes.begin(), runs.bytes.begin() + cut);
        CHECK_EQ(add(short_bytes, 2, 100, end), malformed);
    }
    std::vector<char> extra = runs.bytes;
    extra.push_back(0);
    CHECK_EQ(add(extra, 2, 100, end), malformed);           // leftover bytes
    CHECK_EQ(add(runs.bytes, 1, 100, end), malformed);      // fewer runs than bytes
    CHECK_EQ(add(runs.bytes, 2, 100, end - 1), malformed);  // past the tier
    CHECK_EQ(add(runs.bytes, 1000, 100, end), malformed);   // more runs than bytes allow
    auto encode = [](std::vector<uint32_t> values) {
        std::vector<char> out;
        for (uint32_t v : values) write_varint(out, v);
        return out;
    };
    CHECK_EQ(add(encode({0, 0, 2}), 1, 100, end), malformed);  // empty run
    // A start past 2^32 must not wrap back into the tier.
    CHECK_EQ(add(encode({0xFFFFFFF0u, 4, 2}), 1, 100, end), malformed);
    // Tiers are added in offset order; a tier below earlier runs is refused.
    StringRemap remap(scratch_tables());
    remap.add_sent_tier(runs.bytes.data(), runs.bytes.size(), 2, 100, end);
    CHECK_EQ(refusal_of([&] { remap.add_sent_tier(runs.bytes.data(), runs.bytes.size(), 2, 50, end); }), malformed);
}

TEST(patch_format_string_remap_maps_shipped_and_sent_tiers_and_tier_moves) {
    // Random builds of up to 5 tiers: some shipped (derived runs over the
    // client's own pools), the rest sent (runs decoded from the patch), and
    // strings that move tier (pairs). Every old string start maps to where
    // the diff's per-string remap (build_string_remap) sends it, and no run
    // covers a pair's offset, so the lookup order can't matter.
    uint64_t seed = 11;
    auto rnd = [&](uint32_t n) { seed = seed * 6364136223846793005ull + 1442695040888963407ull; return (uint32_t)(seed >> 33) % n; };
    auto word = [&] { std::string w; for (uint32_t k = 0, len = 1 + rnd(4); k < len; k++) w.push_back('a' + rnd(5)); return w; };
    for (int round = 0; round < 300; round++) {
        const int n_tiers = 3 + (int)rnd(3);
        // Each word lives in at most one tier per side.
        std::map<std::string, std::pair<int, int>> home;  // word → (old tier, new tier), -1 = absent
        for (int i = 0; i < 60; i++) {
            std::string w = word();
            if (home.count(w)) continue;
            int ot = (int)rnd(n_tiers + 1) - 1, nt = ot;
            uint32_t kind = rnd(10);
            if (kind == 0) nt = (int)rnd(n_tiers + 1) - 1;  // moved tier, added or deleted
            home[w] = {ot, nt};
        }
        std::vector<std::string> olds(n_tiers), news(n_tiers);
        {
            std::vector<std::vector<std::string>> o(n_tiers), n(n_tiers);
            for (const auto& [w, tiers] : home) {
                if (tiers.first >= 0) o[tiers.first].push_back(w);
                if (tiers.second >= 0) n[tiers.second].push_back(w);
            }
            for (int t = 0; t < n_tiers; t++) { olds[t] = pool_of(o[t]); news[t] = pool_of(n[t]); }
        }
        std::vector<uint32_t> ob(n_tiers + 1, 0), nb(n_tiers + 1, 0);
        for (int t = 0; t < n_tiers; t++) {
            ob[t + 1] = ob[t] + (uint32_t)olds[t].size();
            nb[t + 1] = nb[t] + (uint32_t)news[t].size();
        }
        // D: old start → new start of the same string, wherever it lives.
        std::unordered_map<std::string, uint32_t> new_at;
        for (int t = 0; t < n_tiers; t++)
            for (uint32_t v : starts_of(news[t], nb[t])) new_at[news[t].c_str() + (v - nb[t])] = v;
        std::unordered_map<uint32_t, uint32_t> D;
        std::vector<RemapPair> moves;  // by old start
        auto tier_of = [&](uint32_t v, const std::vector<uint32_t>& b) {
            for (int t = 0; t < n_tiers; t++) if (v >= b[t] && v < b[t + 1]) return t;
            return -1;
        };
        for (int t = 0; t < n_tiers; t++)
            for (uint32_t v : starts_of(olds[t], ob[t])) {
                auto it = new_at.find(olds[t].c_str() + (v - ob[t]));
                if (it == new_at.end()) continue;
                D[v] = it->second;
                if (tier_of(it->second, nb) != t) moves.push_back({v, it->second});
            }

        const uint32_t shipped = rnd(1u << n_tiers);
        StringRemap remap(scratch_tables()), runs_only(scratch_tables());
        for (StringRemap* r : {&remap, &runs_only}) {
            for (int t = 0; t < n_tiers; t++) {
                const TierEdit e = tier_edit(olds[t], news[t]);
                if (shipped >> t & 1) {
                    rebuild(olds[t], e.edit(), place_of(ob[t], nb[t], news[t]), *r);
                } else {
                    auto runs = encode_string_tier_runs(olds[t].data(), olds[t].size(), ob[t], nb[t], e.kept);
                    r->add_sent_tier(runs.bytes.data(), runs.bytes.size(), runs.n_runs, ob[t], ob[t + 1]);
                }
            }
        }
        const std::vector<char> moved = pair_bytes(moves);
        remap.finish(pair_table(moved));
        runs_only.finish();
        // The hinted lookup answers alike, front to back as a replay looks
        // offsets up in record order, and back to front.
        size_t forward = 0, backward = 0;
        for (int t = 0; t < n_tiers; t++)
            for (uint32_t v : starts_of(olds[t], ob[t])) {
                auto it = D.find(v);
                CHECK_EQ(remap.lookup(v), it != D.end() ? it->second : v);
                CHECK_EQ(remap.lookup(v, forward), remap.lookup(v));
            }
        for (int t = n_tiers - 1; t >= 0; t--) {
            const std::vector<uint32_t> starts = starts_of(olds[t], ob[t]);
            for (auto v = starts.rbegin(); v != starts.rend(); ++v) CHECK_EQ(remap.lookup(*v, backward), remap.lookup(*v));
        }
        for (auto [a, b] : moves) CHECK_EQ(runs_only.lookup(a), a);
    }
}

TEST(patch_format_referenced_string_tiers_reads_the_listed_fields) {
    // Tiers: core [0, 10), street [10, 30), addr empty, postcode [30, 40),
    // poi [40, 50). Records of stride 12 with string fields at 4 and 8.
    const uint32_t ends[STRING_TIER_COUNT] = {10, 30, 30, 40, 50};
    auto records = [](std::vector<std::array<uint32_t, 3>> recs) {
        std::vector<char> out(recs.size() * 12);
        if (!out.empty()) memcpy(out.data(), recs.data(), out.size());
        return out;
    };
    const std::vector<size_t> fields = {4, 8};
    auto mask_of = [&](const std::vector<char>& data) {
        return referenced_string_tiers(data.data(), data.size(), 12, fields, ends, "test.bin");
    };
    // Byte 0 isn't a string field: 45 there references nothing.
    CHECK_EQ(mask_of(records({{45, 0, 0xFFFFFFFFu}})), 1u);
    CHECK_EQ(mask_of(records({{0, 9, 10}, {0, 0xFFFFFFFFu, 39}})), 0b1011u);
    CHECK_EQ(mask_of(records({{0, 30, 49}})), 0b11000u);  // 30 opens postcode: addr is empty
    CHECK_EQ(mask_of(records({})), 0u);
    CHECK_EQ(refusal_of([&] { mask_of(records({{0, 1, 50}})); }),
             std::string("test.bin references string offset 50 past the old pool (50 bytes)"));
}

namespace {

// A random build of 5 tiers for the strings section tests: each word lives in
// at most one tier per side, and some change tier.
struct RandomTiers {
    std::array<std::string, STRING_TIER_COUNT> olds, news;
    std::array<uint32_t, STRING_TIER_COUNT + 1> ob{}, nb{};
    std::unordered_map<uint32_t, uint32_t> moved;  // old start → new start, every survivor (the diff's str_remap)
    // Fills `moved` from the pools.
    void find_moved() {
        std::unordered_map<std::string, uint32_t> new_at;
        for (int t = 0; t < STRING_TIER_COUNT; t++)
            for (uint32_t v : starts_of(news[t], nb[t])) new_at[news[t].c_str() + (v - nb[t])] = v;
        for (int t = 0; t < STRING_TIER_COUNT; t++)
            for (uint32_t v : starts_of(olds[t], ob[t])) {
                auto it = new_at.find(olds[t].c_str() + (v - ob[t]));
                if (it != new_at.end()) moved[v] = it->second;
            }
    }
    std::array<StringTierPools, STRING_TIER_COUNT> pools() const {
        std::array<StringTierPools, STRING_TIER_COUNT> p;
        for (int t = 0; t < STRING_TIER_COUNT; t++)
            p[t] = {olds[t].data(), olds[t].size(), news[t].data(), news[t].size()};
        return p;
    }
    int old_tier_of(uint32_t v) const {
        for (int t = 0; t < STRING_TIER_COUNT; t++) if (v >= ob[t] && v < ob[t + 1]) return t;
        return -1;
    }
    int new_tier_of(uint32_t v) const {
        for (int t = 0; t < STRING_TIER_COUNT; t++) if (v >= nb[t] && v < nb[t + 1]) return t;
        return -1;
    }
};

// Sorted: every tier alphabetical. Shuffled: every tier in an order of its
// own that keeps most survivors' relative order and moves some, as the POI
// tier's record order does.
enum class PoolOrder { Sorted, Shuffled };

template <typename Rnd>
RandomTiers random_tiers(Rnd& rnd, PoolOrder order = PoolOrder::Sorted) {
    auto word = [&] { std::string w; for (uint32_t k = 0, len = 1 + rnd(4); k < len; k++) w.push_back('a' + rnd(5)); return w; };
    std::map<std::string, std::pair<int, int>> home;  // word → (old tier, new tier), -1 = absent
    for (int i = 0; i < 80; i++) {
        std::string w = word();
        if (home.count(w)) continue;
        int ot = (int)rnd(STRING_TIER_COUNT + 1) - 1, nt = ot;
        if (rnd(8) == 0) nt = (int)rnd(STRING_TIER_COUNT + 1) - 1;
        home[w] = {ot, nt};
    }
    RandomTiers r;
    std::array<std::vector<std::string>, STRING_TIER_COUNT> o, n;
    for (const auto& [w, tiers] : home) {
        if (tiers.first >= 0) o[tiers.first].push_back(w);
        if (tiers.second >= 0) n[tiers.second].push_back(w);
    }
    if (order == PoolOrder::Shuffled) {
        std::map<std::string, std::pair<uint32_t, uint32_t>> key;  // word → (old, new) place
        for (const auto& [w, tiers] : home) {
            const uint32_t k = rnd(1000);
            key[w] = {k, rnd(4) == 0 ? rnd(1000) : k};
        }
        for (int t = 0; t < STRING_TIER_COUNT; t++) {
            std::stable_sort(o[t].begin(), o[t].end(), [&](const std::string& a, const std::string& b) { return key[a].first < key[b].first; });
            std::stable_sort(n[t].begin(), n[t].end(), [&](const std::string& a, const std::string& b) { return key[a].second < key[b].second; });
        }
    }
    for (int t = 0; t < STRING_TIER_COUNT; t++) {
        r.olds[t] = order == PoolOrder::Sorted ? pool_of(o[t]) : pool_in_order(o[t]);
        r.news[t] = order == PoolOrder::Sorted ? pool_of(n[t]) : pool_in_order(n[t]);
        r.ob[t + 1] = r.ob[t] + (uint32_t)r.olds[t].size();
        r.nb[t + 1] = r.nb[t] + (uint32_t)r.news[t].size();
    }
    r.find_moved();
    return r;
}

// The patcher's side: parse the section, rebuild each shipped tier from its
// old pool (checked against the new one) and load the remap.
struct DecodedStrings {
    StringsSection section;
    std::array<std::string, STRING_TIER_COUNT> rebuilt;
};

void decode_strings(const std::vector<char>& buf, const RandomTiers& r, DecodedStrings& out, StringRemap& remap) {
    size_t pos = 0;
    out.section = parse_strings_section(buf.data(), buf.size(), pos);
    CHECK_EQ(pos, buf.size());
    load_string_remap(out.section, remap, [&](int t) {
        const StringTierEdit& d = out.section.edits[t];
        CHECK_EQ(d.old_hash, content_hash(r.olds[t].data(), r.olds[t].size()));
        const Rebuilt b = rebuild(r.olds[t], d, out.section.place(t), remap);
        out.rebuilt[t] = b.pool;
        CHECK_EQ(content_hash(out.rebuilt[t].data(), out.rebuilt[t].size()), d.new_hash);
        CHECK_EQ(b.walk.old_hash, d.old_hash);
        CHECK_EQ(b.walk.new_hash, d.new_hash);
        CHECK_EQ(refusal_of([&] { require_string_tier_rebuilt(b.walk, d, out.section.place(t), STRING_TIER_FILES[t]); }),
                 std::string());
    });
}

// Random builds with every mix of shipped, referenced (sent) and neither
// tiers: shipped tiers rebuild byte for byte, every string start of a shipped
// or referenced tier maps exactly where the diff's per-string remap sends it,
// and no pair leaves a tier nobody looks up. Returns the pairs of strings
// that kept their tier.
size_t check_strings_section_round_trip(uint64_t seed, PoolOrder order) {
    auto rnd = [&](uint32_t n) { seed = seed * 6364136223846793005ull + 1442695040888963407ull; return (uint32_t)(seed >> 33) % n; };
    size_t order_moves = 0;
    for (int round = 0; round < 400; round++) {
        const RandomTiers r = random_tiers(rnd, order);
        const uint32_t shipped = rnd(32), referenced = rnd(32);
        std::vector<char> buf;
        const auto stats = append_strings_section(buf, r.pools(), shipped, referenced, r.moved);
        DecodedStrings dec;
        StringRemap remap(scratch_tables());
        decode_strings(buf, r, dec, remap);
        const StringsSection& s = dec.section;
        CHECK_EQ(s.shipped, shipped);
        CHECK_EQ(s.sent, stats.sent);
        CHECK_EQ(s.pairs.size(), size_t(stats.n_pairs));
        const uint32_t looked_up = shipped | referenced;
        for (int t = 0; t < STRING_TIER_COUNT; t++) {
            CHECK_EQ(s.old_size(t), uint32_t(r.olds[t].size()));
            CHECK_EQ(s.new_size(t), uint32_t(r.news[t].size()));
            CHECK_EQ(bool(s.sent >> t & 1), !(shipped >> t & 1) && (referenced >> t & 1) && !r.olds[t].empty());
            if (shipped >> t & 1) CHECK(dec.rebuilt[t] == r.news[t]);
            if (!(looked_up >> t & 1)) continue;
            for (uint32_t v : starts_of(r.olds[t], r.ob[t])) {
                auto it = r.moved.find(v);
                CHECK_EQ(remap.lookup(v), it != r.moved.end() ? it->second : v);
            }
        }
        // Pairs: every survivor that changed tier out of a looked-up tier,
        // and in another order, some that kept it (never in a sorted tier).
        size_t tier_moves = 0;
        for (const auto& [o, n] : r.moved) {
            int ot = r.old_tier_of(o);
            if (ot != r.new_tier_of(n) && (looked_up >> ot & 1)) tier_moves++;
        }
        size_t in_tier = 0;
        s.pairs.for_each([&](RemapPair p) {
            CHECK(looked_up >> r.old_tier_of(p.from) & 1);
            auto it = r.moved.find(p.from);
            CHECK(it != r.moved.end() && it->second == p.to);
            in_tier += r.old_tier_of(p.from) == r.new_tier_of(p.to);
        });
        CHECK_EQ(s.pairs.size() - in_tier, tier_moves);
        if (order == PoolOrder::Sorted) CHECK_EQ(in_tier, size_t(0));
        order_moves += in_tier;
        // Every cut of the section is refused.
        for (size_t cut = 0; cut < buf.size(); cut += 1 + buf.size() / 50) {
            size_t pos = 0;
            CHECK(!refusal_of([&] { parse_strings_section(buf.data(), cut, pos); }).empty());
        }
    }
    return order_moves;
}

}  // namespace

TEST(patch_format_strings_section_round_trip) {
    check_strings_section_round_trip(23, PoolOrder::Sorted);
}

TEST(patch_format_strings_section_round_trips_tiers_in_any_order) {
    // The POI tier is in record order: a string whose first record moves
    // past others is deleted and added, and its offset goes as a pair.
    CHECK(check_strings_section_round_trip(29, PoolOrder::Shuffled) > 0);
}

TEST(patch_format_string_tier_edit_moves_a_string_out_of_order) {
    // old: zoo cafe bank   new: zoo bank cafe tea. zoo and bank keep their
    // order, so cafe moves (deleted at 1, added at 2) and tea is new (3).
    const std::string o = pool_in_order({"zoo", "cafe", "bank"});
    const std::string n = pool_in_order({"zoo", "bank", "cafe", "tea"});
    const TierEdit e = tier_edit(o, n);
    CHECK(e.kept == (std::vector<std::pair<uint32_t, uint32_t>>{{0, 0}, {9, 4}}));
    CHECK(e.diff.deleted == std::vector<uint32_t>({1}));
    CHECK(e.diff.added_at == std::vector<uint32_t>({2, 3}));
    REQUIRE(e.diff.added.size() == 2);
    CHECK_EQ(std::string(e.diff.added[0]), std::string("cafe"));
    CHECK_EQ(std::string(e.diff.added[1]), std::string("tea"));
    // bank shifts by a run; cafe, which the edit doesn't keep, by a pair.
    StringRemap remap(scratch_tables());
    CHECK(rebuild(o, e.edit(), place_of(100, 100, n), remap).pool == n);
    const std::vector<char> moved = pair_bytes({{104, 109}});
    remap.finish(pair_table(moved));
    CHECK_EQ(remap.run_count(), size_t(1));
    CHECK_EQ(remap.lookup(100), 100u);
    CHECK_EQ(remap.lookup(109), 104u);
    CHECK_EQ(remap.lookup(104), 109u);
}

TEST(patch_format_rebuild_finds_a_diff_the_old_pool_cannot_place) {
    const std::string o = pool_in_order({"a", "b"});
    auto rebuilds = [&](const StringTierDiff& d) {
        const std::vector<char> bytes = bytes_of(d);
        StringRemap remap(scratch_tables());
        return rebuild(o, edit_of(bytes), {}, remap).walk.placed;
    };
    const char* x = "x";
    CHECK(rebuilds({0, 0, false, {x}, {2}, {}}));          // after both old strings
    CHECK(!rebuilds({0, 0, false, {x}, {3}, {}}));         // past the end
    CHECK(rebuilds({0, 0, false, {}, {}, {1}}));
    CHECK(!rebuilds({0, 0, false, {}, {}, {2}}));          // no third old string
    CHECK(!rebuilds({0, 0, false, {x}, {2}, {0, 1}}));     // one string left, x can't be third
    CHECK(rebuilds({0, 0, true, {x}, {}, {0}}));           // sorted: x after b
    CHECK(!rebuilds({0, 0, true, {x}, {}, {2}}));
}

TEST(patch_format_string_tier_walk_writes_strings_and_reads_nothing_it_passed) {
    // 120,000 names, one of the longest length a tier holds. Mixed: every
    // seventh deleted and every fifth followed by an added one (sorted, or
    // put before it, which an indexed tier's order allows). Deleted suffix:
    // the same up to name 10,000, then every name deleted, the long one too.
    // All deleted: no new string at all. The one walk writes the new pool a
    // string at a time, hashes both pools as content_hash does and derives
    // the runs. It gives the old file's body back as it decodes it, deleted
    // strings as well as kept ones: each call at the end of the first bucket
    // that puts a piece behind the last call, and under a piece left for the
    // end. It reads no body byte before an offset it passed, nor a byte of the
    // bucket table before the entry of the bucket after: scribbled over as it
    // passes them, the old file still rebuilds into the new pool, under the
    // old pool's hash.
    enum class Shape { Mixed, DeletedSuffix, AllDeleted };
    struct Case {
        const char* name;
        Shape shape;
        bool sorted;
    };
    for (const Case c : {Case{"mixed sorted", Shape::Mixed, true}, Case{"mixed indexed", Shape::Mixed, false},
                         Case{"deleted suffix sorted", Shape::DeletedSuffix, true},
                         Case{"deleted suffix indexed", Shape::DeletedSuffix, false},
                         Case{"all deleted", Shape::AllDeleted, true}}) {
        const int failures_before = gctest::failures();
        std::vector<std::string> o, n;
        for (uint32_t i = 0; i < 120000; i++) {
            char b[16];
            std::snprintf(b, sizeof b, "name %07u", i);
            const std::string w = i == 12345 ? b + std::string(string_files::MAX_STRING - 12, 'z') : b;
            o.push_back(w);
            if (c.shape == Shape::AllDeleted || (c.shape == Shape::DeletedSuffix && i >= 10000)) continue;
            if (i % 5 == 1 && !c.sorted) n.push_back(w + "+");
            if (i % 7 != 3) n.push_back(w);
            if (i % 5 == 1 && c.sorted) n.push_back(w + "+");
        }
        const std::string old_pool = pool_in_order(o), new_pool = pool_in_order(n);
        const TierEdit e = tier_edit(old_pool, new_pool);
        REQUIRE(e.diff.sorted == c.sorted);
        std::vector<uint8_t> file = string_files::encode(old_pool.data(), old_pool.size());
        const string_files::Footer ft = string_files::read_footer(file.data(), file.size());
        REQUIRE(ft.body >= 6 * STRING_WALK_PIECE);
        // The body offset each bucket ends at, and the one it starts at and
        // its index.
        std::map<uint64_t, std::pair<uint64_t, uint64_t>> bucket_at_end;
        for (uint64_t b = 0, at = 0; b < ft.buckets; b++) {
            const uint64_t size = string_files::rd16(&file[ft.body + 4 * b + 2]);
            bucket_at_end[at + size] = {at, b};
            at += size;
        }
        string_files::Decoder old(file.data(), file.size());
        std::string rebuilt;
        size_t not_one = 0, passed = 0, scribbled = 0, late = 0, off_end = 0, table_wrong = 0, table_scribbled = 0;
        StringRemap remap(scratch_tables());

        const StringTierRebuild walk = rebuild_string_tier(
            old, e.edit(), place_of(1000, 900, new_pool), remap,
            [&](const char* p, size_t len) {
                rebuilt.append(p, len);
                not_one += len == 0 || p[len - 1] != '\0' || memchr(p, 0, len - 1) != nullptr;
            },
            [&](size_t off, size_t table) {
                CHECK(off >= scribbled + STRING_WALK_PIECE && off <= ft.body);
                // A bucket ends at off, and started under a piece past the last call; the
                // table is passed up to the entry of the bucket after it.
                const auto it = bucket_at_end.find(off);
                off_end += it == bucket_at_end.end();
                late += it != bucket_at_end.end() && it->second.first >= scribbled + STRING_WALK_PIECE;
                table_wrong += it != bucket_at_end.end() && table != 4 * (it->second.second + 1);
                std::fill(file.begin(), file.begin() + static_cast<std::ptrdiff_t>(off), uint8_t('#'));
                std::fill(file.begin() + static_cast<std::ptrdiff_t>(ft.body),
                          file.begin() + static_cast<std::ptrdiff_t>(ft.body + table), uint8_t('#'));
                scribbled = off;
                table_scribbled = table;
                passed++;
            });

        CHECK(rebuilt == new_pool);
        CHECK(walk.placed);
        CHECK_EQ(walk.new_size, uint64_t(new_pool.size()));
        CHECK_EQ(walk.old_hash, content_hash(old_pool.data(), old_pool.size()));
        CHECK_EQ(walk.new_hash, content_hash(new_pool.data(), new_pool.size()));
        CHECK_EQ(not_one, size_t(0));
        CHECK_EQ(off_end, size_t(0));
        CHECK_EQ(late, size_t(0));
        CHECK_EQ(table_wrong, size_t(0));
        CHECK(ft.body - scribbled < STRING_WALK_PIECE);
        CHECK(table_scribbled > 0 && table_scribbled <= uint64_t(ft.buckets) * 4);
        CHECK(passed + 2 >= ft.body / STRING_WALK_PIECE);
        CHECK(passed >= 4);
        remap.finish();
        const auto want = pairs_of(old_pool, new_pool, 1000, 900);
        size_t wrong = 0;
        for (uint32_t v : starts_of(old_pool, 1000)) {
            auto it = want.find(v);
            wrong += remap.lookup(v) != (it != want.end() ? it->second : v);
        }
        CHECK_EQ(wrong, size_t(0));
        if (gctest::failures() > failures_before) std::printf("    in the %s case\n", c.name);
    }
}

TEST(patch_format_string_tier_rebuild_is_refused_for_its_first_fault) {
    // The walk reads the old pool before anything checks it. A stale pool is
    // named as such, even when the edit doesn't fit it; then an edit the
    // right pool can't place is a malformed patch; then a new pool other
    // than the new build's is named. The walk hashes the whole old pool in
    // every case.
    const std::string o = pool_of({"a", "bb", "c", "dd", "e"}), n = pool_of({"a", "c", "dd", "ff"});
    const TierEdit e = tier_edit(o, n);
    REQUIRE(e.diff.deleted == std::vector<uint32_t>({1, 4}));
    auto refusal = [](const std::string& old_pool, const StringTierDiff& d, const StringTierPlace& at) {
        const std::vector<char> bytes = bytes_of(d);
        const StringTierEdit edit = edit_of(bytes);
        StringRemap remap(scratch_tables());
        const Rebuilt b = rebuild(old_pool, edit, at, remap);
        CHECK_EQ(b.walk.old_hash, content_hash(old_pool.data(), old_pool.size()));
        return refusal_of([&] { require_string_tier_rebuilt(b.walk, edit, at, "strings_core.bin"); });
    };
    const StringTierPlace at = place_of(0, 0, n);
    std::string letter = o, fewer = o;
    letter[2] = 'x';               // bb -> xb: as many strings
    fewer[fewer.size() - 3] = 'x';  // dd e -> ddxe: no fifth string to delete
    StringTierDiff unplaced = e.diff, other_new = e.diff;
    unplaced.deleted.push_back(5);  // the right pool has no sixth string
    other_new.new_hash ^= 1;
    const std::string stale = "Old strings_core.bin is not the one the patch was made from";
    const std::string rebuilt = "Rebuilt strings_core.bin does not match the new build";

    CHECK_EQ(refusal(o, e.diff, at), std::string());
    CHECK_EQ(refusal(letter, e.diff, at), stale);
    CHECK_EQ(refusal(fewer, e.diff, at), stale);
    CHECK_EQ(refusal(fewer, unplaced, at), stale);
    CHECK_EQ(refusal(o, unplaced, at), std::string("Malformed strings section"));
    CHECK_EQ(refusal(letter, other_new, at), stale);
    CHECK_EQ(refusal(o, other_new, at), rebuilt);
    CHECK_EQ(refusal(o, e.diff, place_of(0, 0, n + "x")), rebuilt);
}

TEST(patch_format_sorted_tier_sends_no_indices) {
    // A sorted new pool takes its added strings where they sort: the diff
    // carries no indices, and the old pool rebuilds into the new one.
    const std::string o = pool_of({"b", "d", "f"}), n = pool_of({"a", "b", "c", "f", "g"});
    const TierEdit e = tier_edit(o, n);
    CHECK(e.diff.sorted);
    CHECK(e.diff.added_at.empty());
    CHECK(e.diff.deleted == std::vector<uint32_t>({1}));
    StringRemap remap(scratch_tables());
    CHECK(rebuild(o, e.edit(), place_of(0, 0, n), remap).pool == n);
    // The section holds the order flag but no index of the three added strings.
    std::array<StringTierPools, STRING_TIER_COUNT> tiers{};
    tiers[0] = {o.data(), o.size(), n.data(), n.size()};
    const std::vector<std::pair<uint32_t, uint32_t>> moved = {{0, 2}, {4, 6}};
    std::vector<char> buf;
    append_strings_section(buf, tiers, 1, 0, moved);
    const size_t header = 4 + STRING_TIER_COUNT * 8 + 8, tier = 16 + 12 + (2 * 3) + 4, pairs = 4;
    CHECK_EQ(buf.size(), header + tier + pairs);
    size_t pos = 0;
    const StringsSection s = parse_strings_section(buf.data(), buf.size(), pos);
    CHECK(s.edits[0].sorted);
    CHECK_EQ(s.edits[0].n_added, 3u);
    // An unsorted new pool sends them.
    const std::string u = pool_in_order({"g", "b", "a", "c", "f"});
    CHECK(!string_pool_sorted(u.data(), u.size()));
    CHECK(!tier_edit(o, u).diff.sorted);
}

TEST(patch_format_strings_section_refuses_indices_out_of_order) {
    // One shipped tier "a b" → "d c" (not sorted, so indexed): both deleted,
    // both added.
    const std::string o = pool_of({"a", "b"}), n = pool_in_order({"d", "c"});
    std::array<StringTierPools, STRING_TIER_COUNT> tiers{};
    tiers[0] = {o.data(), o.size(), n.data(), n.size()};
    std::vector<char> buf;
    append_strings_section(buf, tiers, 1, 0, std::vector<std::pair<uint32_t, uint32_t>>());
    size_t pos = 0;
    const StringsSection s = parse_strings_section(buf.data(), buf.size(), pos);
    REQUIRE(indices_of(s.edits[0].added_at, s.edits[0].n_added) == std::vector<uint32_t>({0, 1}));
    REQUIRE(indices_of(s.edits[0].deleted, s.edits[0].n_deleted) == std::vector<uint32_t>({0, 1}));
    // The section ends with added_at (8 bytes), deleted (8) and n_pairs (4).
    for (size_t list_at : {buf.size() - 20, buf.size() - 12}) {
        std::vector<char> bad = buf;
        const uint32_t descending[2] = {1, 0};
        memcpy(bad.data() + list_at, descending, 8);
        pos = 0;
        CHECK_EQ(refusal_of([&] { parse_strings_section(bad.data(), bad.size(), pos); }), std::string("Malformed strings section"));
    }
    // A sorted tier's added strings must ascend: "a b" → "c d" sends c, d
    // before deleted (8 bytes) and n_pairs (4); swapped, they are refused.
    const std::string sorted_new = pool_of({"c", "d"});
    tiers[0] = {o.data(), o.size(), sorted_new.data(), sorted_new.size()};
    std::vector<char> sorted_buf;
    append_strings_section(sorted_buf, tiers, 1, 0, std::vector<std::pair<uint32_t, uint32_t>>());
    pos = 0;
    REQUIRE(parse_strings_section(sorted_buf.data(), sorted_buf.size(), pos).edits[0].sorted);
    REQUIRE(std::string(sorted_buf.data() + sorted_buf.size() - 16, 4) == std::string("c\0d\0", 4));
    memcpy(sorted_buf.data() + sorted_buf.size() - 16, "d\0c\0", 4);
    pos = 0;
    CHECK_EQ(refusal_of([&] { parse_strings_section(sorted_buf.data(), sorted_buf.size(), pos); }),
             std::string("Malformed strings section"));
}

TEST(patch_format_strings_section_reads_tier_edits_where_they_sit) {
    // The POI tier zoo cafe bank → zoo bank cafe tea (indexed): cafe deleted
    // at 1 and added at 2, tea added at 3, and cafe's offset as a pair.
    const std::string o = pool_in_order({"zoo", "cafe", "bank"}), n = pool_in_order({"zoo", "bank", "cafe", "tea"});
    std::array<StringTierPools, STRING_TIER_COUNT> tiers{};
    tiers[4] = {o.data(), o.size(), n.data(), n.size()};
    const std::vector<std::pair<uint32_t, uint32_t>> moved = {{0, 0}, {4, 9}, {9, 4}};
    std::vector<char> buf;
    append_strings_section(buf, tiers, 1u << 4, 0, moved);
    size_t pos = 0;

    const StringsSection s = parse_strings_section(buf.data(), buf.size(), pos);

    auto in_buf = [&](const char* p, size_t len) { return p >= buf.data() && p + len <= buf.data() + buf.size(); };
    const StringTierEdit& e = s.edits[4];
    REQUIRE(e.n_added == 2 && e.n_deleted == 1 && !e.sorted);
    CHECK(in_buf(e.added, 9));
    CHECK_EQ(std::string(e.added, 9), std::string("cafe\0tea\0", 9));
    CHECK(in_buf(e.added_at, 8));
    CHECK(indices_of(e.added_at, 2) == std::vector<uint32_t>({2, 3}));
    CHECK(in_buf(e.deleted, 4));
    CHECK(indices_of(e.deleted, 1) == std::vector<uint32_t>({1}));
    REQUIRE(s.pairs.size() == 1);
    CHECK(in_buf(s.pairs.data(), 8));
    CHECK_EQ(s.pairs.find(4)->to, 9u);
}

TEST(patch_format_strings_section_refuses_moved_strings_out_of_order_or_twice) {
    // One shipped tier "a b c" → "c b a" (indexed): its edit keeps one
    // string, and the other two go as the pairs that end the section.
    const std::string o = pool_in_order({"a", "b", "c"}), n = pool_in_order({"c", "b", "a"});
    std::array<StringTierPools, STRING_TIER_COUNT> tiers{};
    tiers[0] = {o.data(), o.size(), n.data(), n.size()};
    const std::vector<std::pair<uint32_t, uint32_t>> moved = {{0, 4}, {2, 2}, {4, 0}};
    std::vector<char> buf;
    append_strings_section(buf, tiers, 1, 0, moved);
    size_t pos = 0;
    REQUIRE(parse_strings_section(buf.data(), buf.size(), pos).pairs.size() == 2);
    const size_t first = buf.size() - 16, second = buf.size() - 8;
    std::vector<char> swapped = buf, twice = buf;
    std::swap_ranges(swapped.begin() + first, swapped.begin() + second, swapped.begin() + second);
    memcpy(twice.data() + second, buf.data() + first, 4);

    for (const std::vector<char>* bad : {&swapped, &twice}) {
        pos = 0;
        CHECK_EQ(refusal_of([&] { parse_strings_section(bad->data(), bad->size(), pos); }), std::string("Malformed strings section"));
    }
}

TEST(patch_format_strings_section_sends_no_pair_past_its_cap) {
    // One shipped tier "a b c" → "c b a" (indexed): its edit keeps "c", and
    // "a" and "b" leave it as two pairs. Under a cap of 2 they are sent; under
    // a cap of 1 neither is, and both come back unsent, ascending: the
    // patcher's remap then keeps their old offsets, which the merges' string
    // fixups give new values.
    const std::string o = pool_in_order({"a", "b", "c"}), n = pool_in_order({"c", "b", "a"});
    std::array<StringTierPools, STRING_TIER_COUNT> tiers{};
    tiers[0] = {o.data(), o.size(), n.data(), n.size()};
    const std::vector<std::pair<uint32_t, uint32_t>> moved = {{0, 4}, {2, 2}, {4, 0}};
    std::vector<char> sent, unsent;
    const StringsSectionStats at_cap = append_strings_section(sent, tiers, 1, 0, moved, 2);
    const StringsSectionStats past_cap = append_strings_section(unsent, tiers, 1, 0, moved, 1);
    CHECK_EQ(at_cap.n_pairs, 2u);
    CHECK(at_cap.unsent_pairs.empty());
    CHECK_EQ(past_cap.n_pairs, 0u);
    const std::vector<std::pair<uint32_t, uint32_t>> a_and_b = {{0, 4}, {2, 2}};
    CHECK(past_cap.unsent_pairs == a_and_b);
    CHECK_EQ(unsent.size(), sent.size() - 2 * sizeof(RemapPair));
    for (const auto& [buf, a, b] : {std::tuple{&sent, 4u, 2u}, std::tuple{&unsent, 0u, 2u}}) {
        size_t pos = 0;
        const StringsSection s = parse_strings_section(buf->data(), buf->size(), pos);
        StringRemap remap(scratch_tables());
        load_string_remap(s, remap, [&](int t) { CHECK(rebuild(o, s.edits[t], s.place(t), remap).pool == n); });
        CHECK_EQ(remap.lookup(0), a);
        CHECK_EQ(remap.lookup(2), b);
        CHECK_EQ(remap.lookup(4), 0u);
    }
}

TEST(patch_format_strings_section_refuses_more_pairs_than_its_cap) {
    // A section of no tier and STRING_PAIRS_MAX pairs parses; one more is
    // refused before the pairs are read.
    std::vector<char> buf;
    std::array<StringTierPools, STRING_TIER_COUNT> none{};
    append_strings_section(buf, none, 0, 0, std::vector<std::pair<uint32_t, uint32_t>>());
    buf.resize(buf.size() - 4);
    auto with_pairs = [&](uint32_t n) {
        std::vector<char> b = buf;
        b.insert(b.end(), reinterpret_cast<const char*>(&n), reinterpret_cast<const char*>(&n) + 4);
        for (uint32_t i = 0; i < n; i++)
            for (const uint32_t w : {2 * i, 2 * i + 1}) b.insert(b.end(), reinterpret_cast<const char*>(&w), reinterpret_cast<const char*>(&w) + 4);
        return b;
    };
    auto parses = [](const std::vector<char>& b) {
        size_t pos = 0;
        return refusal_of([&] { parse_strings_section(b.data(), b.size(), pos); });
    };
    CHECK_EQ(parses(with_pairs(STRING_PAIRS_MAX)), std::string());
    CHECK_EQ(parses(with_pairs(STRING_PAIRS_MAX + 1)), std::string("Malformed strings section"));
}

TEST(patch_format_strings_section_rejects_bad_masks) {
    std::vector<char> buf;
    std::array<StringTierPools, STRING_TIER_COUNT> none{};
    append_strings_section(buf, none, 0, 0, std::vector<std::pair<uint32_t, uint32_t>>());
    const size_t shipped_at = 4 + STRING_TIER_COUNT * 8;
    auto with_masks = [&](uint32_t shipped, uint32_t sent) {
        std::vector<char> b = buf;
        memcpy(b.data() + shipped_at, &shipped, 4);
        memcpy(b.data() + shipped_at + 4, &sent, 4);
        return b;
    };
    auto parses = [](const std::vector<char>& b) {
        size_t pos = 0;
        return refusal_of([&] { parse_strings_section(b.data(), b.size(), pos); });
    };
    CHECK_EQ(parses(buf), std::string());
    CHECK_EQ(parses(with_masks(1u << STRING_TIER_COUNT, 0)), std::string("Malformed strings section"));
    CHECK_EQ(parses(with_masks(0, 1u << STRING_TIER_COUNT)), std::string("Malformed strings section"));
    std::vector<char> wrong_marker = buf;
    wrong_marker[0] ^= 1;
    CHECK_EQ(parses(wrong_marker), std::string("Patch has no strings section"));
}

TEST(patch_format_newly_shipped_tier_is_sent_and_arrives_whole) {
    // An admin dir that starts shipping strings_postcode.bin: the old dir
    // lacks it, so it can't be rebuilt. It counts as unshipped (its runs are
    // sent, since the dir's postal polygons reference it) and its file comes
    // in a section of its own.
    ScratchDir old_dir("newly-shipped-old"), new_dir("newly-shipped-new");
    for (const char* f : {"strings_core.bin"}) REQUIRE(write_file(old_dir.path() + "/" + f, std::vector<char>(2, 'a')));
    for (const char* f : {"strings_core.bin", "strings_postcode.bin"})
        REQUIRE(write_file(new_dir.path() + "/" + f, std::vector<char>(2, 'a')));
    const ShippedStringTiers tiers = shipped_string_tiers(old_dir.path(), new_dir.path());
    CHECK_EQ(tiers.shipped, 1u);         // core
    CHECK_EQ(tiers.newly_shipped, 8u);   // postcode
    CHECK_EQ(std::string(patch_file_names[(uint32_t)string_tier_file_id(3)]), std::string("strings_postcode.bin"));
    for (int t = 0; t < STRING_TIER_COUNT; t++)
        CHECK_EQ(std::string(patch_file_names[(uint32_t)string_tier_file_id(t)]), std::string(STRING_TIER_FILES[t]));

    // The diff borrowed the old postcode tier from ../full.
    RandomTiers r;
    r.olds = {pool_of({"a", "c"}), pool_of({"main st"}), pool_of({"12"}), pool_of({"2000", "2010"}), ""};
    r.news = {pool_of({"a", "b", "c"}), pool_of({"main st"}), pool_of({"12"}), pool_of({"2000", "2005", "2010"}), ""};
    for (int t = 0; t < STRING_TIER_COUNT; t++) {
        r.ob[t + 1] = r.ob[t] + (uint32_t)r.olds[t].size();
        r.nb[t + 1] = r.nb[t] + (uint32_t)r.news[t].size();
    }
    r.find_moved();
    const uint32_t old_2010 = r.ob[3] + 5, new_2010 = r.nb[3] + 10;
    const uint32_t referenced = 0b1001;  // core, postcode
    std::vector<char> buf;
    const auto stats = append_strings_section(buf, r.pools(), tiers.shipped, referenced, r.moved);
    CHECK_EQ(stats.sent, 8u);
    DecodedStrings dec;
    StringRemap remap(scratch_tables());
    decode_strings(buf, r, dec, remap);
    CHECK_EQ(dec.section.shipped, 1u);
    CHECK_EQ(dec.section.sent, 8u);
    CHECK(dec.rebuilt[0] == r.news[0]);
    CHECK_EQ(remap.lookup(r.ob[3]), r.nb[3]);
    CHECK_EQ(remap.lookup(old_2010), new_2010);

    // The patcher: a shipped tier must be listed; a listed one without the
    // bit (postcode) is fine, its per-file section writes it.
    const std::unordered_set<std::string> listed = {"strings_core.bin", "strings_postcode.bin", "strings_layout.json"};
    CHECK_EQ(refusal_of([&] { require_shipped_tiers_listed(dec.section.shipped, listed); }), std::string());
    CHECK_EQ(refusal_of([&] { require_shipped_tiers_listed(0b1001, {"strings_core.bin"}); }),
             std::string("Strings section ships strings_postcode.bin, which the client files don't list"));
}

// The builder's STR_TIER_FILENAMES (test_string_tiers.cpp: parsed_data.h
// can't share a translation unit with patch_format.h).
std::vector<std::string> builder_string_tier_files();

TEST(patch_format_string_tier_files_stack_in_the_builder_order) {
    // Global string offsets stack the tiers in the builder's order.
    const std::vector<std::string> builder_order = builder_string_tier_files();
    REQUIRE(builder_order.size() == size_t(STRING_TIER_COUNT));
    for (int t = 0; t < STRING_TIER_COUNT; t++) CHECK_EQ(std::string(STRING_TIER_FILES[t]), builder_order[t]);
}

TEST(patch_format_string_tier_runs_are_maximal_and_skip_unmoved_strings) {
    // old: a c e g i   new: a c d e i  (d added, g deleted).
    // a, c don't move; e moves by +2 ("d\0"); g is gone; i moves by 0
    // (d's +2 cancels g's -2) and opens no run.
    const std::string o = pool_of({"a", "c", "e", "g", "i"});
    const std::string n = pool_of({"a", "c", "d", "e", "i"});
    std::vector<std::array<uint32_t, 3>> runs;
    auto runs_of = [&](const std::string& old_pool, const std::string& new_pool, uint32_t ob, uint32_t nb) {
        runs.clear();
        StringRunJoiner joiner([&](uint32_t s, uint32_t e, uint32_t sh) { runs.push_back({s, e, sh}); });
        for (const auto& [os, ns] : tier_edit(old_pool, new_pool).kept)
            joiner.kept(ob + os, ob + os + (uint32_t)strlen(old_pool.c_str() + os) + 1, nb + ns);
        joiner.finish();
    };
    runs_of(o, n, 100, 100);
    REQUIRE(runs.size() == 1);
    CHECK(runs[0] == (std::array<uint32_t, 3>{104, 106, 2}));
    // Two tiers' worth of bases: every surviving string moves by the base
    // difference, so the whole pool is one run.
    runs_of(o, o, 10, 7);
    REQUIRE(runs.size() == 1);
    CHECK(runs[0] == (std::array<uint32_t, 3>{10, 20, uint32_t(-3)}));
}

TEST(patch_format_longest_increasing_subsequence) {
    struct Case { std::vector<uint32_t> v; size_t len; };
    const Case cases[] = {
        {{}, 0}, {{7}, 1}, {{1, 2, 3, 4}, 4}, {{4, 3, 2, 1}, 1}, {{2, 2, 2}, 1},
        {{5, 1, 6, 2, 7, 3, 8}, 4}, {{0, 9, 4, 5, 6, 1}, 4},
    };
    for (const auto& c : cases) {
        const std::vector<uint32_t> idx = longest_increasing_subsequence(c.v);
        CHECK_EQ(idx.size(), c.len);
        for (size_t k = 1; k < idx.size(); k++) CHECK(idx[k - 1] < idx[k] && c.v[idx[k - 1]] < c.v[idx[k]]);
    }
    // Against the quadratic length on random input.
    uint64_t seed = 5;
    auto rnd = [&](uint32_t n) { seed = seed * 6364136223846793005ull + 1442695040888963407ull; return (uint32_t)(seed >> 33) % n; };
    for (int round = 0; round < 200; round++) {
        std::vector<uint32_t> v(rnd(40));
        for (auto& x : v) x = rnd(30);
        std::vector<size_t> best(v.size(), 1);
        size_t want = 0;
        for (size_t i = 0; i < v.size(); i++) {
            for (size_t j = 0; j < i; j++)
                if (v[j] < v[i]) best[i] = std::max(best[i], best[j] + 1);
            want = std::max(want, best[i]);
        }
        const std::vector<uint32_t> idx = longest_increasing_subsequence(v);
        CHECK_EQ(idx.size(), want);
        for (size_t k = 1; k < idx.size(); k++) CHECK(idx[k - 1] < idx[k] && v[idx[k - 1]] < v[idx[k]]);
    }
}

// --- record id remap ---

TEST(patch_format_record_remap_matches_and_moved_pairs) {
    // MATCH runs map kept records; unmatched ones map to NONE; a moved
    // record's pair overrides the runs; ids past the old file and pairs for
    // them map to nothing.
    RecordRemap rm(10, scratch_tables());
    rm.add_match(0, 0, 3);    // 0..2 → 0..2
    rm.add_match(3, 3, 1);    // extends the run
    rm.add_match(6, 9, 10);   // 6..9 → 9..12, clipped at the old size
    rm.finish_runs();
    const std::vector<char> pairs = pair_bytes({{1, 61}, {4, 50}, {12, 7}});
    rm.set_pairs(pair_table(pairs));
    CHECK(rm.has_pairs());
    CHECK_EQ(rm.run_count(), size_t(2));
    const uint32_t N = RecordRemap::NONE;
    const std::vector<uint32_t> want = {0, 61, 2, 3, 50, N, 9, 10, 11, 12};
    for (uint32_t i = 0; i < 10; i++) CHECK_EQ(rm[i], want[i]);
    CHECK_EQ(rm[10], N);
    CHECK_EQ(RecordRemap()[0], N);
}

TEST(patch_format_record_remap_lookup_with_a_hint_matches_the_index) {
    // The hinted lookup the v8 address replay uses must answer like
    // operator[] whatever hint it starts from, pairs taking precedence.
    RecordRemap rm(40, scratch_tables());
    rm.add_match(0, 5, 10);
    rm.add_match(12, 20, 8);
    rm.add_match(30, 2, 3);
    rm.finish_runs();
    const std::vector<char> pairs = pair_bytes({{3, 99}, {25, 7}});
    rm.set_pairs(pair_table(pairs));
    CHECK(!rm.empty());
    for (size_t start : {size_t(0), size_t(1), size_t(2), size_t(50)}) {
        size_t hint = start, back = start;
        for (uint32_t i = 0; i < 45; i++) CHECK_EQ(rm.lookup(i, hint), rm[i]);
        for (uint32_t i = 45; i-- > 0;) CHECK_EQ(rm.lookup(i, back), rm[i]);
    }
    CHECK(RecordRemap(5, scratch_tables()).empty());
}

TEST(patch_format_parallel_delta_files_follow_their_parent) {
    // v8 side files: one u32 per parent record, remapped like their sparse
    // delta (way_parents' admin slots raw, string offsets for postcodes).
    const ParallelDeltaFile* f = parallel_delta_file(PatchFileId::WAY_PARENTS);
    REQUIRE(f != nullptr);
    CHECK(f->parent == PatchFileId::STREET_WAYS);
    CHECK_EQ(f->remap_kind, 0u);
    f = parallel_delta_file(PatchFileId::ADDR_POSTCODES);
    REQUIRE(f != nullptr);
    CHECK(f->parent == PatchFileId::ADDR_POINTS);
    CHECK_EQ(f->remap_kind, 2u);
    for (const auto& p : PARALLEL_DELTA_FILES) {
        CHECK_EQ(p.value_stride, 4u);
        bool sparse = false;
        for (const auto& s : SPARSE_DELTA_FILES)
            if (s.fid == p.fid) sparse = s.value_stride == p.value_stride && s.remap_kind == p.remap_kind;
        CHECK(sparse);
    }
    CHECK(parallel_delta_file(PatchFileId::POI_RECORDS) == nullptr);
}

// --- geo entry list deltas ---

TEST(patch_format_geo_list_delta_round_trip) {
    // derived → target by lost/gained ids; an unsorted target, or one whose
    // delta overflows the u16 counts, travels whole.
    struct Case { std::vector<uint32_t> derived, target; bool replace; };
    std::vector<uint32_t> huge(GEO_LIST_REPLACE);
    std::iota(huge.begin(), huge.end(), 0);
    const Case cases[] = {
        {{1, 2, 3, 5, 6, 7}, {1, 3, 4, 5, 6, 7}, false},
        {{}, {5, 6}, false},                             // a new cell: all gains
        {{5, 6}, {}, false},                             // emptied: all losses
        {{1, 2, 2, 3, 8, 9, 10}, {2, 3, 3, 8, 9, 10}, false},  // duplicates are multiset differences
        {{1, 2}, {9, 4}, true},                          // unsorted: replaced whole
        {{1, 2}, {1, 3}, false},                         // a delta even where the list is as short
        {huge, {70000}, true},                           // 0xFFFF lost ids don't fit: replaced whole
    };
    std::vector<char> buf;
    uint64_t cid = 10;
    for (const auto& c : cases) append_geo_list_delta(buf, cid++, c.derived, c.target);
    size_t pos = 0;
    const PatchRef ref = scan_geo_list_deltas(buf.data(), buf.size(), pos, 7);
    CHECK_EQ(pos, buf.size());
    CHECK_EQ(ref.pos, size_t(0));
    CHECK_EQ(ref.size, buf.size());
    GeoListDeltaCursor sut(buf.data(), ref);
    std::vector<uint32_t> scratch;
    CHECK(!sut.at(9));
    for (size_t i = 0; i < 7; i++) {
        REQUIRE(sut.at(10 + i));
        CHECK_EQ(sut.replaces(), cases[i].replace);
        std::vector<uint32_t> ids = cases[i].derived;
        sut.apply(ids, scratch);
        CHECK(ids == cases[i].target);
    }
    CHECK(!sut.at(17));
}

TEST(patch_format_geo_list_delta_cursor_skips_cells_the_walk_passes) {
    std::vector<char> buf;
    append_geo_list_delta(buf, 3, {1}, {2});
    append_geo_list_delta(buf, 5, {}, {7});
    append_geo_list_delta(buf, 8, {4}, {});
    GeoListDeltaCursor sut(buf.data(), {0, buf.size()});
    CHECK(!sut.at(1));
    CHECK(sut.at(5));  // 3 passed unread
    CHECK(sut.at(5));
    CHECK(!sut.at(6));
    CHECK(sut.at(8));
    CHECK(!sut.at(9));
    GeoListDeltaCursor none;
    CHECK(!none.at(0));
    CHECK(!none.at(UINT64_MAX));
}

TEST(patch_format_geo_list_delta_rejects_truncation_and_disorder) {
    std::vector<char> buf;
    append_geo_list_delta(buf, 7, {1}, {2});
    append_geo_list_delta(buf, 9, {}, {3});
    const std::string malformed = "Malformed geo entry delta";
    for (size_t cut = 0; cut < buf.size(); cut++) {
        size_t pos = 0;
        CHECK_EQ(refusal_of([&] { scan_geo_list_deltas(buf.data(), cut, pos, 2); }), malformed);
    }
    std::vector<char> back;
    append_geo_list_delta(back, 9, {}, {3});
    append_geo_list_delta(back, 7, {1}, {2});
    size_t pos = 0;
    CHECK_EQ(refusal_of([&] { scan_geo_list_deltas(back.data(), back.size(), pos, 2); }), malformed);
}

TEST(patch_parallel_delta_rejects_invalid_headers) {
    for (const auto& f : PARALLEL_DELTA_FILES)
        check_parallel_delta_header(uint32_t(f.fid), uint32_t(f.parent), f.value_stride, f.remap_kind);
    auto rejects = [](PatchFileId fid, uint32_t parent, uint32_t stride, uint32_t remap) {
        CHECK_EQ(refusal_of([&] { check_parallel_delta_header(uint32_t(fid), parent, stride, remap); }),
                 std::string("Malformed parallel delta"));
    };
    rejects(PatchFileId::WAY_PARENTS, uint32_t(PatchFileId::COUNT), 4, 0);
    rejects(PatchFileId::WAY_PARENTS, 0xFFFFFFFFu, 4, 0);
    rejects(PatchFileId::WAY_PARENTS, uint32_t(PatchFileId::ADDR_POINTS), 4, 0);
    rejects(PatchFileId::WAY_PARENTS, uint32_t(PatchFileId::STREET_WAYS), 4, 3);
    rejects(PatchFileId::WAY_PARENTS, uint32_t(PatchFileId::STREET_WAYS), 16, 0);
    // v14: way_parents holds admin slots, which no patch remaps.
    rejects(PatchFileId::WAY_PARENTS, uint32_t(PatchFileId::STREET_WAYS), 4, 1);
    rejects(PatchFileId::POI_RECORDS, uint32_t(PatchFileId::POI_RECORDS), 4, 0);
}

TEST(patch_output_reports_final_flush_failure) {
    FILE* out = fopen("/dev/full", "wb");
    REQUIRE(out != nullptr);
    char buf[8192];
    setvbuf(out, buf, _IOFBF, sizeof(buf));
    CHECK_EQ(fwrite("test", 1, 4, out), size_t(4));
    CHECK_EQ(ferror(out), 0);
    CHECK_EQ(refusal_of([&] { close_patch_outputs({{out, "test"}}); }), std::string("Cannot write test"));
}

TEST(patch_outputs_close_every_one_and_name_the_failed) {
    // The full device's flush fails; the file after it is still flushed and
    // closed, and only the failed one is named.
    ScratchDir dir("close-outputs");
    const std::string kept = dir.path() + "/kept.bin";
    FILE* full = fopen("/dev/full", "wb");
    FILE* file = fopen(kept.c_str(), "wb");
    REQUIRE(full != nullptr);
    REQUIRE(file != nullptr);
    char full_buf[8192], file_buf[8192];
    setvbuf(full, full_buf, _IOFBF, sizeof(full_buf));
    setvbuf(file, file_buf, _IOFBF, sizeof(file_buf));
    fwrite("lost", 1, 4, full);
    fwrite("kept", 1, 4, file);
    CHECK_EQ(refusal_of([&] { close_patch_outputs({{full, "full.bin"}, {file, "kept.bin"}}); }), std::string("Cannot write full.bin"));
    CHECK_EQ(std::filesystem::file_size(kept), uintmax_t(4));
}

TEST(patch_format_ensure_dir_creates_the_path_as_named) {
    // Quotes and shell syntax are part of the name: nothing runs, every level is created.
    ScratchDir dir("ensure-dir");
    for (const std::string name : {"user's update/new", "x'; touch pwned; 'y/new"}) {
        const std::string path = dir.path() + "/" + name;
        ensure_dir(path);
        CHECK(std::filesystem::is_directory(path));
        ensure_dir(path);  // an existing directory is fine
    }
    CHECK(!std::filesystem::exists("pwned"));
    CHECK(!std::filesystem::exists(dir.path() + "/pwned"));
}

TEST(patch_format_ensure_dir_throws_when_it_cannot_create) {
    ScratchDir dir("ensure-dir-file");
    const std::string file = dir.path() + "/a-file";
    std::ofstream(file) << "x";
    CHECK_EQ(refusal_of([&] { ensure_dir(file + "/sub"); }).find("Cannot create directory " + file + "/sub"), size_t(0));
}
