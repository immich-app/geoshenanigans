// Unit tests for the pure helpers in patch_format.h.
// These lock in the CURRENT behaviour of the varint codec, the grid
// fingerprint quantiser, and the stride/marker sentinel constants so a future
// refactor of the patch tooling can't silently shift them (the constants are
// load-bearing: diff and patch must agree on them byte-for-byte).
#include "patch_format.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "test_framework.h"

// --- write_varint / read_varint round-trip ---

static uint32_t varint_roundtrip(uint32_t value) {
    std::vector<char> buf;
    write_varint(buf, value);
    size_t pos = 0;
    uint32_t got = read_varint(buf.data(), pos);
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
    for (uint32_t v : vals) {
        CHECK_EQ(read_varint(buf.data(), pos), v);
    }
    CHECK_EQ(pos, buf.size());
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

TEST(patch_format_stride_sentinels_distinct) {
    CHECK(SPARSE_DELTA_STRIDE != COPY_OLD_STRIDE);
    CHECK(SPARSE_DELTA_STRIDE != LEGACY_SKIP_STRIDE);
    CHECK(COPY_OLD_STRIDE != LEGACY_SKIP_STRIDE);
    CHECK(CELL_LIST_DELTA_STRIDE != SPARSE_DELTA_STRIDE);
    CHECK(CELL_LIST_DELTA_STRIDE != COPY_OLD_STRIDE);
    CHECK(CELL_LIST_DELTA_STRIDE != LEGACY_SKIP_STRIDE);
    // Current concrete values (locked in).
    CHECK_EQ(SPARSE_DELTA_STRIDE, uint32_t(0xFC));
    CHECK_EQ(COPY_OLD_STRIDE, uint32_t(0xFD));
    CHECK_EQ(LEGACY_SKIP_STRIDE, uint32_t(0xFE));
    CHECK_EQ(CELL_LIST_DELTA_STRIDE, uint32_t(0xFB));
}

TEST(patch_format_stride_sentinels_no_collision_with_real_strides) {
    // Real per-record strides used across the format (see read_* helpers and
    // the *_cells / entry structs). None may equal a sentinel, else a genuine
    // record stride would be misread as a sparse/copy/skip section.
    const uint32_t real_strides[] = {
        2,   // entry count prefix (u16)
        4,   // u32 id arrays (entries, parents, postcodes)
        8,   // NodeCoord, cell_id
        9,   // packed WayHeader
        12,  // padded WayHeader / admin_cells / poi_cells / place_cells
        16,  // postcode_centroid value_stride
        18,  // packed InterpWay
        28,  // AddrPoint v15 (polygon footprint fields)
        32,  // PoiRecord (build_version 10-14)
        36,  // PoiRecord (current, parent ids)
        19,  // raw AdminPolygon
        20,  // padded InterpWay / legacy AddrPoint / current PlaceNode / geo_cell
        24,  // padded AdminPolygon / PoiRecord
    };
    for (uint32_t s : real_strides) {
        CHECK(s != SPARSE_DELTA_STRIDE);
        CHECK(s != COPY_OLD_STRIDE);
        CHECK(s != LEGACY_SKIP_STRIDE);
        CHECK(s != CELL_LIST_DELTA_STRIDE);
    }
}

// --- Section marker constants ---

TEST(patch_format_section_markers_distinct) {
    const uint32_t markers[] = {
        FIXUP_MARKER,                 // 0xFFFFFFFD
        CELL_CHANGES_GEO_MARKER,      // 0xFFFFFFFB
        CELL_CHANGES_ADMIN_MARKER,    // 0xFFFFFFFA
        CELL_CHANGES_POI_MARKER,      // 0xFFFFFFF5
        CELL_CHANGES_PLACE_MARKER,    // 0xFFFFFFF4
        ENTRY_CORRECTION_MARKER,      // 0xFFFFFFF8
        CELL_INDEX_DELTA_MARKER,      // 0xFFFFFFF7
        CELL_FLAGS_MARKER,            // 0xFFFFFFF9
        SECONDARY_REMAP_MARKER,       // 0xFFFFFFF6
        POI_PARENT_REMAP_MARKER,      // 0xFFFFFFF3
        CLIENT_FILES_MARKER,          // 0xFFFFFFF2
    };
    const size_t n = sizeof(markers) / sizeof(markers[0]);
    for (size_t i = 0; i < n; i++)
        for (size_t j = i + 1; j < n; j++)
            CHECK(markers[i] != markers[j]);
}

TEST(patch_format_section_marker_values) {
    // Concrete values locked in (diff/patch wire contract).
    CHECK_EQ(FIXUP_MARKER, uint32_t(0xFFFFFFFD));
    CHECK_EQ(CELL_CHANGES_GEO_MARKER, uint32_t(0xFFFFFFFB));
    CHECK_EQ(CELL_CHANGES_ADMIN_MARKER, uint32_t(0xFFFFFFFA));
    CHECK_EQ(CELL_CHANGES_POI_MARKER, uint32_t(0xFFFFFFF5));
    CHECK_EQ(CELL_CHANGES_PLACE_MARKER, uint32_t(0xFFFFFFF4));
    CHECK_EQ(ENTRY_CORRECTION_MARKER, uint32_t(0xFFFFFFF8));
    CHECK_EQ(CELL_INDEX_DELTA_MARKER, uint32_t(0xFFFFFFF7));
    CHECK_EQ(CELL_FLAGS_MARKER, uint32_t(0xFFFFFFF9));
    CHECK_EQ(SECONDARY_REMAP_MARKER, uint32_t(0xFFFFFFF6));
    CHECK_EQ(POI_PARENT_REMAP_MARKER, uint32_t(0xFFFFFFF3));
    CHECK_EQ(CLIENT_FILES_MARKER, uint32_t(0xFFFFFFF2));
}

// --- Format identity / enum constants ---

TEST(patch_format_magic_and_version) {
    // GCPATCH_VERSION is bound to the value actually emitted/checked by the
    // diff/patch tools. v5 = offset fixups as shift runs; v4 listed them one
    // by one, so v4 patches are unreadable (MIN_READ_VERSION).
    CHECK_EQ(GCPATCH_VERSION, uint32_t(5));
    CHECK_EQ(GCPATCH_MIN_READ_VERSION, uint32_t(5));
    const char expect[8] = {'G','C','P','A','T','C','H','\0'};
    for (int i = 0; i < 8; i++) CHECK_EQ(GCPATCH_MAGIC[i], expect[i]);
}

TEST(patch_format_fileid_count_and_names_aligned) {
    // The names table must have exactly COUNT entries (one per PatchFileId).
    CHECK_EQ(static_cast<uint32_t>(PatchFileId::COUNT), uint32_t(37));
    const size_t n_names = sizeof(patch_file_names) / sizeof(patch_file_names[0]);
    CHECK_EQ(n_names, size_t(37));
    // Spot-check that index lines up with the enum value.
    CHECK(std::string(patch_file_names[static_cast<uint32_t>(PatchFileId::STRINGS)]) == "strings.bin");
    CHECK(std::string(patch_file_names[static_cast<uint32_t>(PatchFileId::POI_RECORDS)]) == "poi_records.bin");
    CHECK(std::string(patch_file_names[static_cast<uint32_t>(PatchFileId::STRINGS_POI)]) == "strings_poi.bin");
    CHECK(std::string(patch_file_names[static_cast<uint32_t>(PatchFileId::INTERP_POSTCODES)]) == "interp_postcodes.bin");
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

// --- record_node_count ---

TEST(patch_format_record_node_count_reads_u16_and_legacy_u8) {
    // Padded layouts (stride 12 / 24): u16 at byte 4, so roads past 255
    // nodes keep their full count.
    char way[12] = {};
    uint16_t n = 1358;
    std::memcpy(way + 4, &n, 2);
    CHECK_EQ(record_node_count(way, 12, WAY_HEADER_STRIDE_PACKED), 1358u);
    CHECK_EQ(record_node_count(way, 24, INTERP_WAY_STRIDE_PACKED), 1358u);
    // Legacy packed layouts (stride 9 / 18): u8 at byte 4, next byte is data.
    char packed[9] = {};
    packed[4] = static_cast<char>(200);
    packed[5] = static_cast<char>(0x7F);
    CHECK_EQ(record_node_count(packed, 9, WAY_HEADER_STRIDE_PACKED), 200u);
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

static std::vector<ClientFile> sample_client_files() {
    ClientFile layout{"strings_layout.json", 4, {'{', ' ', '}', '\n'}};
    ClientFile ways{"street_ways.bin", 12345678901ull, {}};
    return {layout, ways};
}

TEST(patch_format_client_files_round_trip) {
    std::vector<char> buf = {'x'};  // the section is read by position
    append_client_files(buf, sample_client_files());
    size_t pos = 1;
    auto got = parse_client_files(buf.data(), buf.size(), pos);
    CHECK_EQ(pos, buf.size());
    REQUIRE(got.size() == 2);
    CHECK(got[0].name == "strings_layout.json");
    CHECK_EQ(got[0].size, uint64_t(4));
    CHECK(got[0].bytes == std::vector<char>({'{', ' ', '}', '\n'}));
    CHECK(got[1].name == "street_ways.bin");
    CHECK_EQ(got[1].size, uint64_t(12345678901ull));
    CHECK(got[1].bytes.empty());
}

static bool parse_throws(const std::vector<char>& buf) {
    size_t pos = 0;
    try { parse_client_files(buf.data(), buf.size(), pos); } catch (const std::runtime_error&) { return true; }
    return false;
}

TEST(patch_format_client_files_rejects_missing_truncated_and_bad_names) {
    std::vector<char> good;
    append_client_files(good, sample_client_files());

    std::vector<char> no_marker(good);
    no_marker[0] ^= 1;
    CHECK(parse_throws(no_marker));
    CHECK(parse_throws(std::vector<char>(good.begin(), good.end() - 1)));

    std::vector<char> escape;
    append_client_files(escape, {ClientFile{"x.bin", 0, {}}});
    const size_t name_at = 4 + 4 + 2;  // marker, n, name_len
    escape[name_at] = '/';
    CHECK(parse_throws(escape));
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

// --- offset fixups ---

static constexpr uint32_t NO_OFFSET = 0xFFFFFFFFu;

static OffsetFixups encode_fixed(const std::vector<uint32_t>& old_offsets, const std::vector<uint32_t>& fixed) {
    return encode_offset_fixups(old_offsets, [&](uint32_t i) { return fixed[i]; });
}

static OffsetFixupReader reader_of(const OffsetFixups& f) {
    return OffsetFixupReader(f.runs.data(), f.runs.size(), f.n_runs,
                             f.values.data(), f.values.size(), f.n_values);
}

// What the patcher writes for every record, visiting them in order.
static std::vector<uint32_t> replay(const std::vector<uint32_t>& old_offsets, const OffsetFixups& f) {
    auto reader = reader_of(f);
    std::vector<uint32_t> out;
    for (uint32_t i = 0; i < old_offsets.size(); i++) out.push_back(reader.apply(i, old_offsets[i]));
    return out;
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
    bool threw = false;
    try {
        auto reader = reader_of(f);
        for (uint32_t i = 0; i < 3; i++) reader.apply(i, 0);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
}

// --- cell index rebuild from an id remap ---

static void append_u32(std::vector<char>& buf, uint32_t v) { buf.insert(buf.end(), (const char*)&v, (const char*)&v + 4); }

// One cell holding `ids`, as cells (cell_id, offset) + entries (count, ids).
static std::pair<std::vector<char>, std::vector<char>> one_cell(uint64_t cell_id, const std::vector<uint32_t>& ids) {
    std::vector<char> cells((const char*)&cell_id, (const char*)&cell_id + 8), entries;
    append_u32(cells, 0);
    uint16_t n = static_cast<uint16_t>(ids.size());
    entries.insert(entries.end(), (const char*)&n, (const char*)&n + 2);
    for (uint32_t id : ids) append_u32(entries, id);
    return {cells, entries};
}

static std::vector<uint32_t> ids_of_first_cell(const std::vector<char>& cells, const std::vector<char>& entries) {
    uint32_t off; std::memcpy(&off, cells.data() + 8, 4);
    uint16_t n; std::memcpy(&n, entries.data() + off, 2);
    std::vector<uint32_t> ids(n);
    std::memcpy(ids.data(), entries.data() + off + 2, n * 4);
    return ids;
}

TEST(patch_format_rebuild_cells_remaps_interior_entries) {
    // Interior entries carry INTERIOR_FLAG in the top bit; the remap is keyed
    // by the bare id and the flag survives it.
    const uint32_t interior = 0x80000000u;
    auto [cells, entries] = one_cell(42, {3 | interior, 5});
    const std::unordered_map<uint32_t, uint32_t> rm = {{3, 2}, {5, 4}};
    auto rebuilt = rebuild_cells_from_remap(cells, entries, rm);
    CHECK(ids_of_first_cell(rebuilt.cells_data, rebuilt.entries_data) == std::vector<uint32_t>({4, 2 | interior}));
}

// --- cell list delta ---

TEST(patch_format_cell_lists_write_the_cell_index_layout) {
    // Cells sorted by id with contiguous entry offsets; entries are
    // (u16 count, ids). Matches write_cell_index.
    auto [cells, entries] = write_cell_lists({{9, {4}}, {7, {1, 2}}});
    auto [one_c, one_e] = one_cell(7, {1, 2});
    CHECK(std::vector<char>(cells.begin(), cells.begin() + 12) == one_c);
    CHECK(std::vector<char>(entries.begin(), entries.begin() + 10) == one_e);
    uint64_t second; std::memcpy(&second, cells.data() + 12, 8);
    uint32_t off; std::memcpy(&off, cells.data() + 20, 4);
    CHECK_EQ(second, uint64_t(9));
    CHECK_EQ(off, uint32_t(10));
    CHECK(parse_cell_lists(cells, entries) == CellLists({{7, {1, 2}}, {9, {4}}}));
}

// The index stream_cell_list_delta writes for the old index plus the delta
// from old_lists to new_lists.
static std::pair<std::vector<char>, std::vector<char>> apply_delta(
        const std::pair<std::vector<char>, std::vector<char>>& old_index, const std::vector<char>& delta, size_t delta_size) {
    std::pair<std::vector<char>, std::vector<char>> out;
    stream_cell_list_delta(old_index.first.data(), old_index.first.size(), old_index.second.data(), old_index.second.size(),
                           delta.data(), delta_size,
                           [&](const char* p, size_t n) { out.first.insert(out.first.end(), p, p + n); },
                           [&](const char* p, size_t n) { out.second.insert(out.second.end(), p, p + n); });
    return out;
}

TEST(patch_format_cell_list_delta_round_trip) {
    const CellLists old_lists = {{1, {10, 11}}, {2, {20, 21, 22}}, {3, {30}}, {5, {50}}};
    const CellLists new_lists = {{0, {7}}, {1, {10, 11}}, {2, {20, 22, 23}}, {4, {40, 41}}, {5, {}}, {9, {90}}};
    std::vector<char> delta;
    append_cell_list_delta(delta, old_lists, new_lists);
    CHECK(apply_delta(write_cell_lists(old_lists), delta, delta.size()) == write_cell_lists(new_lists));
    // Unchanged cells don't travel: 3 removed; 0 / 2 / 4 / 5 / 9 set; 1 skipped.
    uint32_t n_removed; std::memcpy(&n_removed, delta.data(), 4);
    uint32_t n_set; std::memcpy(&n_set, delta.data() + 4 + n_removed * 8, 4);
    CHECK_EQ(n_removed, uint32_t(1));
    CHECK_EQ(n_set, uint32_t(5));
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
            if (kind != 0) old_lists[cid] = make();
            if (kind == 1 || kind == 2) new_lists[cid] = old_lists[cid];
            else if (kind != 0 || rnd(2)) new_lists[cid] = make();
        }
        std::vector<char> delta;
        append_cell_list_delta(delta, old_lists, new_lists);
        CHECK(apply_delta(write_cell_lists(old_lists), delta, delta.size()) == write_cell_lists(new_lists));
    }
}

TEST(patch_format_stream_cell_index_matches_rebuild_then_delta) {
    // The patcher's one-pass rebuild (old index + remap + added/removed +
    // delta) writes exactly the new index the diff aimed at, on random days
    // with interior flags, removed and added cells.
    uint64_t seed = 99;
    auto rnd = [&](uint32_t n) { seed = seed * 6364136223846793005ull + 1442695040888963407ull; return (uint32_t)(seed >> 33) % n; };
    for (int round = 0; round < 200; round++) {
        CellLists old_lists;
        for (uint64_t cid = 1; cid < 30; cid++) {
            if (rnd(3) == 0) continue;
            std::vector<uint32_t> ids;
            for (uint32_t id = 0; id < 40; id++) if (rnd(5) == 0) ids.push_back(id | (rnd(4) == 0 ? 0x80000000u : 0));
            std::sort(ids.begin(), ids.end());
            old_lists[cid] = ids;
        }
        std::unordered_map<uint32_t, uint32_t> rm;
        for (uint32_t id = 0; id < 40; id++) if (rnd(3) == 0) rm[id] = 100 + rnd(60);
        std::vector<uint64_t> added, removed;
        for (uint64_t cid = 1; cid < 34; cid++) {
            if (!old_lists.count(cid) && rnd(3) == 0) added.push_back(cid);
            else if (old_lists.count(cid) && rnd(6) == 0) removed.push_back(cid);
        }
        auto [cells, entries] = write_cell_lists(old_lists);
        auto derived = rebuild_cells_from_remap(cells, entries, rm, added, removed);
        CellLists new_lists = parse_cell_lists(derived.cells_data, derived.entries_data);
        for (auto it = new_lists.begin(); it != new_lists.end();) {
            if (rnd(5) == 0) { it = new_lists.erase(it); continue; }
            if (rnd(3) == 0) { it->second.push_back(500 + rnd(9)); std::sort(it->second.begin(), it->second.end()); }
            if (rnd(4) == 0 && !it->second.empty()) it->second.erase(it->second.begin());
            ++it;
        }
        if (rnd(2)) new_lists[40 + rnd(5)] = {7};
        std::vector<char> delta;
        append_cell_list_delta(delta, parse_cell_lists(derived.cells_data, derived.entries_data), new_lists);
        std::pair<std::vector<char>, std::vector<char>> got;
        stream_cell_index(cells.data(), cells.size(), entries.data(), entries.size(), added, removed,
                          [&](uint32_t id) { auto f = rm.find(id); return f != rm.end() ? f->second : id; },
                          delta.data(), delta.size(),
                          [&](const char* p, size_t n) { got.first.insert(got.first.end(), p, p + n); },
                          [&](const char* p, size_t n) { got.second.insert(got.second.end(), p, p + n); });
        CHECK(got == write_cell_lists(new_lists));
    }
}

TEST(patch_format_cell_list_delta_drops_removed_no_data_cells) {
    // A rebuilt index can hold an emptied cell (offset NO_DATA); the delta removes it.
    auto [cells, entries] = one_cell(7, {1});
    uint64_t gone = 9; uint32_t no_data = 0xFFFFFFFFu;
    cells.insert(cells.end(), (const char*)&gone, (const char*)&gone + 8);
    cells.insert(cells.end(), (const char*)&no_data, (const char*)&no_data + 4);
    std::vector<char> delta;
    append_cell_list_delta(delta, parse_cell_lists(cells, entries), {{7, {1}}});
    CHECK(apply_delta({cells, entries}, delta, delta.size()) == write_cell_lists({{7, {1}}}));
}

TEST(patch_format_cell_list_delta_rejects_truncation) {
    std::vector<char> delta;
    append_cell_list_delta(delta, {{1, {10}}}, {{1, {11}}, {2, {5}}});
    auto old_index = write_cell_lists({{1, {10}}});
    for (size_t cut = 0; cut < delta.size(); cut++) {
        bool threw = false;
        try { apply_delta(old_index, delta, cut); } catch (const std::runtime_error&) { threw = true; }
        CHECK(threw);
    }
}

// --- string remap runs ---

namespace {

// A tier pool: sorted unique strings, each NUL-terminated.
std::string pool_of(std::vector<std::string> words) {
    std::sort(words.begin(), words.end());
    words.erase(std::unique(words.begin(), words.end()), words.end());
    std::string out;
    for (auto& w : words) { out += w; out.push_back('\0'); }
    return out;
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

}  // namespace

TEST(patch_format_string_remap_runs_match_per_string_pairs) {
    // Random edits to two tiers: every offset (string starts, mid-string
    // bytes, deleted strings, past the end) maps exactly as the pairs did.
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
        StringRemap remap;
        uint32_t ob = 0, nb = 0;
        std::unordered_map<uint32_t, uint32_t> want;
        for (int t = 0; t < 2; t++) {
            remap.add_tier(olds[t].data(), olds[t].size(), ob, news[t].data(), news[t].size(), nb);
            for (auto& kv : pairs_of(olds[t], news[t], ob, nb)) want.insert(kv);
            ob += olds[t].size(); nb += news[t].size();
        }
        remap.add_pair(ob + 100, 3);  // a string that moved tier
        want[ob + 100] = 3;
        remap.finish();
        for (uint32_t off = 0; off < ob + 110; off++) {
            auto it = want.find(off);
            CHECK_EQ(remap.lookup(off), it != want.end() ? it->second : off);
        }
        CHECK_EQ(remap.lookup(0xFFFFFFFFu), 0xFFFFFFFFu);
    }
}

// --- record id remap ---

TEST(patch_format_record_remap_matches_and_secondary_pairs) {
    // MATCH runs map kept records; unmatched ones map to NONE; a secondary
    // match overrides (the last one for an id wins); ids past the old file
    // and pairs for them are ignored.
    RecordRemap rm(10);
    rm.add_match(0, 0, 3);    // 0..2 → 0..2
    rm.add_match(3, 3, 1);    // extends the run
    rm.add_match(6, 9, 10);   // 6..9 → 9..12, clipped at the old size
    rm.add_pair(4, 50);
    rm.add_pair(1, 60);
    rm.add_pair(1, 61);
    rm.add_pair(12, 7);
    rm.finish();
    const uint32_t N = RecordRemap::NONE;
    const std::vector<uint32_t> want = {0, 61, 2, 3, 50, N, 9, 10, 11, 12};
    for (uint32_t i = 0; i < 10; i++) CHECK_EQ(rm[i], want[i]);
    CHECK_EQ(rm[10], N);
    CHECK_EQ(RecordRemap()[0], N);
}
