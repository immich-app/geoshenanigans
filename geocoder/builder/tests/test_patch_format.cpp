// Unit tests for the pure helpers in patch_format.h.
// These lock in the CURRENT behaviour of the varint codec, the grid
// fingerprint quantiser, and the stride/marker sentinel constants so a future
// refactor of the patch tooling can't silently shift them (the constants are
// load-bearing: diff and patch must agree on them byte-for-byte).
#include "patch_format.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
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
    // Current concrete values (locked in).
    CHECK_EQ(SPARSE_DELTA_STRIDE, uint32_t(0xFC));
    CHECK_EQ(COPY_OLD_STRIDE, uint32_t(0xFD));
    CHECK_EQ(LEGACY_SKIP_STRIDE, uint32_t(0xFE));
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
