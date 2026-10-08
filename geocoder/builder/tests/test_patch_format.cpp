// Unit tests for the pure helpers in patch_format.h.
// These lock in the CURRENT behaviour of the varint codec, the grid
// fingerprint quantiser, and the stride/marker sentinel constants so a future
// refactor of the patch tooling can't silently shift them (the constants are
// load-bearing: diff and patch must agree on them byte-for-byte).
#include "patch_format.h"
#include "scratch_dir.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <iterator>
#include <map>
#include <numeric>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "scratch_dir.h"
#include "sequential_file_reader.h"
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

TEST(patch_format_bounded_varint_rejects_truncated_and_overlong_values) {
    std::vector<char> buf;
    write_varint(buf, 300000u);
    for (size_t cut = 0; cut < buf.size(); cut++) {
        size_t pos = 0;
        bool threw = false;
        try { read_varint_bounded(buf.data(), pos, cut, "runs"); } catch (const std::runtime_error& e) {
            threw = std::string(e.what()) == "Malformed runs";
        }
        CHECK(threw);
    }
    size_t pos = 0;
    CHECK_EQ(read_varint_bounded(buf.data(), pos, buf.size(), "runs"), 300000u);
    CHECK_EQ(pos, buf.size());
    const char overlong[6] = {'\x80', '\x80', '\x80', '\x80', '\x80', '\x01'};
    pos = 0;
    bool threw = false;
    try { read_varint_bounded(overlong, pos, sizeof(overlong), "runs"); } catch (const std::runtime_error&) { threw = true; }
    CHECK(threw);
}

TEST(patch_format_bounded_varint_rejects_bits_past_32) {
    // A 5th byte holds bits 28..31; anything above is 2^32 or more, which
    // must not wrap to a small value (5 + 2^32 would read as 5).
    const char too_big[5] = {'\x85', '\x80', '\x80', '\x80', '\x10'};
    size_t pos = 0;
    bool threw = false;
    try { read_varint_bounded(too_big, pos, sizeof(too_big), "runs"); } catch (const std::runtime_error&) { threw = true; }
    CHECK(threw);
    std::vector<char> max;
    write_varint(max, 0xFFFFFFFFu);
    pos = 0;
    CHECK_EQ(read_varint_bounded(max.data(), pos, max.size(), "runs"), 0xFFFFFFFFu);
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
        GEO_ENTRY_DELTA_MARKER,       // 0xFFFFFFF0
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
    CHECK_EQ(GEO_ENTRY_DELTA_MARKER, uint32_t(0xFFFFFFF0));
    CHECK_EQ(CELL_INDEX_DELTA_MARKER, uint32_t(0xFFFFFFF7));
    CHECK_EQ(CELL_FLAGS_MARKER, uint32_t(0xFFFFFFF9));
    CHECK_EQ(SECONDARY_REMAP_MARKER, uint32_t(0xFFFFFFF6));
    CHECK_EQ(POI_PARENT_REMAP_MARKER, uint32_t(0xFFFFFFF3));
    CHECK_EQ(CLIENT_FILES_MARKER, uint32_t(0xFFFFFFF2));
}

// --- Format identity / enum constants ---

TEST(patch_format_magic_and_version) {
    // GCPATCH_VERSION is bound to the value actually emitted/checked by the
    // diff/patch tools. v7 = every variant dir patches from its own files;
    // v6 patches need ../full's string tiers, so they are unreadable
    // (MIN_READ_VERSION).
    CHECK_EQ(GCPATCH_VERSION, uint32_t(7));
    CHECK_EQ(GCPATCH_MIN_READ_VERSION, uint32_t(7));
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

// --- string offset fields ---

TEST(patch_format_string_fields_match_the_record_layouts) {
    using V = std::vector<size_t>;
    CHECK(string_field_offsets(PatchFileId::ADDR_POINTS, 28) == V({8, 12}));  // not parent_way_id at 16
    CHECK(string_field_offsets(PatchFileId::ADDR_POINTS, 20) == V({8, 12}));
    CHECK(string_field_offsets(PatchFileId::STREET_WAYS, 12) == V({8}));
    CHECK(string_field_offsets(PatchFileId::STREET_WAYS, 9) == V({5}));
    CHECK(string_field_offsets(PatchFileId::INTERP_WAYS, 24) == V({8}));
    CHECK(string_field_offsets(PatchFileId::INTERP_WAYS, 20) == V({8}));
    CHECK(string_field_offsets(PatchFileId::INTERP_WAYS, 18) == V({5}));
    CHECK(string_field_offsets(PatchFileId::ADMIN_POLYGONS, 24) == V({8}));
    CHECK(string_field_offsets(PatchFileId::POSTAL_POLYGONS, 24) == V({8}));
    CHECK(string_field_offsets(PatchFileId::POI_RECORDS, 36) == V({16, 24, 28}));  // not parent_poly_id at 32
    CHECK(string_field_offsets(PatchFileId::POI_RECORDS, 32) == V({16, 24, 28}));
    CHECK(string_field_offsets(PatchFileId::POI_RECORDS, 28) == V({16, 24}));
    CHECK(string_field_offsets(PatchFileId::POI_RECORDS, 24) == V({16}));
    CHECK(string_field_offsets(PatchFileId::PLACE_NODES, 20) == V({8}));  // not parent_poly_id at 16
    CHECK(string_field_offsets(PatchFileId::STREET_NODES, 8).empty());
    CHECK(string_field_offsets(PatchFileId::ADMIN_VERTICES, 1).empty());
    CHECK(string_field_offsets(PatchFileId::POI_VERTICES, 1).empty());

    // Sparse files: string offsets are kind 2 (the u32 itself) and kind 3
    // (byte 8 of a postcode centroid); kind 1 holds admin polygon ids.
    std::vector<std::string> strings, admin_ids;
    for (const auto& f : SPARSE_DELTA_FILES) {
        const std::string name = patch_file_names[(uint32_t)f.fid];
        if (f.remap_kind == 2) { CHECK_EQ(f.value_stride, 4u); strings.push_back(name); }
        if (f.remap_kind == 3) { CHECK_EQ(f.value_stride, 16u); strings.push_back(name); }
        if (f.remap_kind == 1) admin_ids.push_back(name);
    }
    CHECK(strings == std::vector<std::string>({"addr_postcodes.bin", "way_postcodes.bin",
                                               "interp_postcodes.bin", "postcode_centroids.bin"}));
    CHECK(admin_ids == std::vector<std::string>({"admin_parents.bin", "way_parents.bin"}));
    CHECK_EQ(POSTCODE_CENTROID_POSTCODE_ID_OFF, size_t(8));
}

TEST(patch_format_string_record_files_cover_every_string_field) {
    // The diff's reference scan reads STRING_RECORD_FILES: a record file with
    // string fields missing there would send no runs for the tiers it uses.
    std::unordered_set<uint32_t> listed;
    for (const auto& f : STRING_RECORD_FILES) {
        listed.insert((uint32_t)f.fid);
        REQUIRE(f.n_strides >= 1 && f.n_strides <= 4);
        for (size_t i = 0; i < f.n_strides; i++) {
            const auto fields = string_field_offsets(f.fid, f.strides[i]);
            CHECK(!fields.empty());
            for (size_t off : fields) CHECK(off + 4 <= f.strides[i]);
        }
    }
    for (uint32_t fid = 0; fid < (uint32_t)PatchFileId::COUNT; fid++)
        for (size_t stride = 1; stride <= 64; stride++)
            if (!string_field_offsets((PatchFileId)fid, stride).empty()) CHECK(listed.count(fid) == 1);
    CHECK_EQ(listed.size(), std::size(STRING_RECORD_FILES));
    // Postal polygons sit at the admin stride.
    for (const auto& f : STRING_RECORD_FILES)
        if (f.fid == PatchFileId::POSTAL_POLYGONS) CHECK(f.stride_from == PatchFileId::ADMIN_POLYGONS);
}

// --- old file identity ---

TEST(patch_format_old_file_must_be_the_size_the_patch_was_made_from) {
    ScratchDir dir("old-size");
    REQUIRE(write_file(dir.path() + "/admin_polygons.bin", std::vector<char>(48, 'x')));
    REQUIRE(write_file(dir.path() + "/postal_polygons.bin", std::vector<char>()));
    auto message = [&](const std::string& name, uint64_t expected) -> std::string {
        try {
            require_old_file_size(dir.path(), name, expected);
        } catch (const std::runtime_error& e) {
            return e.what();
        }
        return "";
    };
    CHECK_EQ(message("admin_polygons.bin", 48), std::string());
    CHECK_EQ(message("postal_polygons.bin", 0), std::string());
    // The diff writes 0 for a file the old dir lacks.
    CHECK_EQ(message("absent.bin", 0), std::string());
    CHECK_EQ(message("admin_polygons.bin", 72),
             std::string("Old admin_polygons.bin is not the one the patch was made from (48 bytes, the patch expects 72)"));
    CHECK(message("admin_polygons.bin", 0) != "");
    CHECK(message("absent.bin", 24) != "");
}

TEST(patch_format_old_file_sizes_cover_the_files_no_section_names) {
    ScratchDir old_dir("old-sizes");
    REQUIRE(write_file(old_dir.path() + "/geo_cells.bin", std::vector<char>(40, 'g')));
    REQUIRE(write_file(old_dir.path() + "/poi_entries.bin", std::vector<char>()));
    std::vector<char> buf;
    append_old_file_sizes(buf, old_dir.path());
    CHECK_EQ(buf.size(), 4 + std::size(UNSECTIONED_OLD_FILES) * 12);
    auto check_in = [&](const std::string& dir, const std::vector<char>& b) -> std::string {
        size_t pos = 0;
        try {
            check_old_file_sizes(b.data(), b.size(), pos, dir);
        } catch (const std::runtime_error& e) {
            return e.what();
        }
        return pos == b.size() ? "" : "pos";
    };
    CHECK_EQ(check_in(old_dir.path(), buf), std::string());
    // Another day's index: same name, another size.
    ScratchDir other("old-sizes-other");
    REQUIRE(write_file(other.path() + "/geo_cells.bin", std::vector<char>(60, 'g')));
    REQUIRE(write_file(other.path() + "/poi_entries.bin", std::vector<char>()));
    CHECK_EQ(check_in(other.path(), buf),
             std::string("Old geo_cells.bin is not the one the patch was made from (60 bytes, the patch expects 40)"));
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

// Bytes in memory behind the size() / at(off, n) interface the cell index
// streams read their old files through.
struct ByteSpan {
    const char* data;
    size_t bytes;
    uint64_t size() const { return bytes; }
    const char* at(uint64_t off, size_t) const { return data + off; }
};

static void write_bytes(const std::string& path, const std::vector<char>& bytes) {
    std::ofstream(path, std::ios::binary).write(bytes.data(), bytes.size());
}

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
    stream_cell_list_delta(ByteSpan{old_index.first.data(), old_index.first.size()},
                           ByteSpan{old_index.second.data(), old_index.second.size()},
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
    ScratchDir dir("cell-index-test");
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
        stream_cell_index(ByteSpan{cells.data(), cells.size()}, ByteSpan{entries.data(), entries.size()}, added, removed,
                          [&](uint32_t id) { auto f = rm.find(id); return f != rm.end() ? f->second : id; },
                          delta.data(), delta.size(),
                          [&](const char* p, size_t n) { got.first.insert(got.first.end(), p, p + n); },
                          [&](const char* p, size_t n) { got.second.insert(got.second.end(), p, p + n); });
        CHECK(got == write_cell_lists(new_lists));

        // The patcher reads the old index from files; a 16-byte window
        // refills mid-list, grows for longer lists and skips removed cells.
        write_bytes(dir.path() + "/cells.bin", cells);
        write_bytes(dir.path() + "/entries.bin", entries);
        SequentialFileReader cells_file(dir.path() + "/cells.bin", 16), entries_file(dir.path() + "/entries.bin", 16);
        std::pair<std::vector<char>, std::vector<char>> streamed;
        stream_cell_index(cells_file, entries_file, added, removed,
                          [&](uint32_t id) { auto f = rm.find(id); return f != rm.end() ? f->second : id; },
                          delta.data(), delta.size(),
                          [&](const char* p, size_t n) { streamed.first.insert(streamed.first.end(), p, p + n); },
                          [&](const char* p, size_t n) { streamed.second.insert(streamed.second.end(), p, p + n); });
        CHECK(streamed == got);
        CHECK_EQ(cells_file.rewinds() + entries_file.rewinds(), uint64_t(0));
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

namespace {

// Every string start of a pool, as global offsets from base.
std::vector<uint32_t> starts_of(const std::string& pool, uint32_t base) {
    std::vector<uint32_t> out;
    for (size_t i = 0; i < pool.size(); i += strlen(pool.c_str() + i) + 1) out.push_back(base + (uint32_t)i);
    return out;
}

bool throws_runtime_error(const std::function<void()>& f) {
    try { f(); } catch (const std::runtime_error&) { return true; }
    return false;
}

}  // namespace

TEST(patch_format_sent_string_runs_map_every_string_start) {
    // A tier the client doesn't hold: its runs travel encoded and are looked
    // up without the old pool, exactly for every string start.
    const std::string o = pool_of({"a", "bb", "c", "dd", "e", "ff", "g"});
    const std::string n = pool_of({"a", "aa", "bb", "c", "e", "eee", "ff", "g", "h"});
    const uint32_t ob = 1000, nb = 990;
    auto runs = encode_string_tier_runs(o.data(), o.size(), ob, n.data(), n.size(), nb);
    StringRemap remap;
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
    auto runs = encode_string_tier_runs(o.data(), o.size(), 100, n.data(), n.size(), 100);
    REQUIRE(runs.n_runs == 2);
    const uint32_t end = 100 + (uint32_t)o.size();
    auto add = [](const std::vector<char>& bytes, uint32_t n_runs, uint32_t tier_start, uint32_t tier_end) {
        StringRemap remap;
        remap.add_sent_tier(bytes.data(), bytes.size(), n_runs, tier_start, tier_end);
    };
    CHECK(!throws_runtime_error([&] { add(runs.bytes, 2, 100, end); }));
    for (size_t cut = 0; cut < runs.bytes.size(); cut++) {
        std::vector<char> short_bytes(runs.bytes.begin(), runs.bytes.begin() + cut);
        CHECK(throws_runtime_error([&] { add(short_bytes, 2, 100, end); }));
    }
    std::vector<char> extra = runs.bytes;
    extra.push_back(0);
    CHECK(throws_runtime_error([&] { add(extra, 2, 100, end); }));      // leftover bytes
    CHECK(throws_runtime_error([&] { add(runs.bytes, 1, 100, end); }));  // fewer runs than bytes
    CHECK(throws_runtime_error([&] { add(runs.bytes, 2, 100, end - 1); }));  // past the tier
    CHECK(throws_runtime_error([&] { add(runs.bytes, 1000, 100, end); }));   // more runs than bytes allow
    auto encode = [](std::vector<uint32_t> values) {
        std::vector<char> out;
        for (uint32_t v : values) write_varint(out, v);
        return out;
    };
    CHECK(throws_runtime_error([&] { add(encode({0, 0, 2}), 1, 100, end); }));  // empty run
    // A start past 2^32 must not wrap back into the tier.
    CHECK(throws_runtime_error([&] { add(encode({0xFFFFFFF0u, 4, 2}), 1, 100, end); }));
    // Tiers are added in offset order; a tier below earlier runs is refused.
    StringRemap remap;
    remap.add_sent_tier(runs.bytes.data(), runs.bytes.size(), 2, 100, end);
    CHECK(throws_runtime_error([&] { remap.add_sent_tier(runs.bytes.data(), runs.bytes.size(), 2, 50, end); }));
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
        std::vector<std::pair<uint32_t, uint32_t>> moves;
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
        StringRemap remap, runs_only;
        for (StringRemap* r : {&remap, &runs_only}) {
            for (int t = 0; t < n_tiers; t++) {
                if (shipped >> t & 1) {
                    r->add_tier(olds[t].data(), olds[t].size(), ob[t], news[t].data(), news[t].size(), nb[t]);
                } else {
                    auto runs = encode_string_tier_runs(olds[t].data(), olds[t].size(), ob[t],
                                                        news[t].data(), news[t].size(), nb[t]);
                    r->add_sent_tier(runs.bytes.data(), runs.bytes.size(), runs.n_runs, ob[t], ob[t + 1]);
                }
            }
        }
        for (auto [a, b] : moves) remap.add_pair(a, b);
        remap.finish();
        runs_only.finish();
        for (int t = 0; t < n_tiers; t++)
            for (uint32_t v : starts_of(olds[t], ob[t])) {
                auto it = D.find(v);
                CHECK_EQ(remap.lookup(v), it != D.end() ? it->second : v);
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
        memcpy(out.data(), recs.data(), out.size());
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
    std::string what;
    try { mask_of(records({{0, 1, 50}})); } catch (const std::runtime_error& e) { what = e.what(); }
    CHECK_EQ(what, std::string("test.bin references string offset 50 past the old pool (50 bytes)"));
}

namespace {

// A random build of 5 tiers for the strings section tests: each word lives in
// at most one tier per side, and some change tier.
struct RandomTiers {
    std::array<std::string, STRING_TIER_COUNT> olds, news;
    std::array<uint32_t, STRING_TIER_COUNT + 1> ob{}, nb{};
    std::unordered_map<uint32_t, uint32_t> moved;  // old start → new start, every survivor (the diff's str_remap)
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

template <typename Rnd>
RandomTiers random_tiers(Rnd& rnd) {
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
    for (int t = 0; t < STRING_TIER_COUNT; t++) {
        r.olds[t] = pool_of(o[t]);
        r.news[t] = pool_of(n[t]);
        r.ob[t + 1] = r.ob[t] + (uint32_t)r.olds[t].size();
        r.nb[t + 1] = r.nb[t] + (uint32_t)r.news[t].size();
    }
    std::unordered_map<std::string, uint32_t> new_at;
    for (int t = 0; t < STRING_TIER_COUNT; t++)
        for (uint32_t v : starts_of(r.news[t], r.nb[t])) new_at[r.news[t].c_str() + (v - r.nb[t])] = v;
    for (int t = 0; t < STRING_TIER_COUNT; t++)
        for (uint32_t v : starts_of(r.olds[t], r.ob[t])) {
            auto it = new_at.find(r.olds[t].c_str() + (v - r.ob[t]));
            if (it != new_at.end()) r.moved[v] = it->second;
        }
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
        const StringTierDiff& d = out.section.diffs[t];
        CHECK_EQ(d.old_hash, content_hash(r.olds[t].data(), r.olds[t].size()));
        rebuild_string_tier(r.olds[t].data(), r.olds[t].size(), d,
                            [&](const char* p, size_t n) { out.rebuilt[t].append(p, n); });
        CHECK_EQ(content_hash(out.rebuilt[t].data(), out.rebuilt[t].size()), d.new_hash);
        remap.add_tier(r.olds[t].data(), r.olds[t].size(), out.section.old_base[t],
                       out.rebuilt[t].data(), out.rebuilt[t].size(), out.section.new_base[t]);
    });
}

}  // namespace

TEST(patch_format_strings_section_round_trip) {
    // Random builds with every mix of shipped, referenced (sent) and neither
    // tiers: shipped tiers rebuild byte for byte, every string start of a
    // shipped or referenced tier maps exactly where the diff's per-string
    // remap sends it, and no pair leaves a tier nobody looks up.
    uint64_t seed = 23;
    auto rnd = [&](uint32_t n) { seed = seed * 6364136223846793005ull + 1442695040888963407ull; return (uint32_t)(seed >> 33) % n; };
    for (int round = 0; round < 400; round++) {
        const RandomTiers r = random_tiers(rnd);
        const uint32_t shipped = rnd(32), referenced = rnd(32);
        std::vector<char> buf;
        const auto stats = append_strings_section(buf, r.pools(), shipped, referenced, r.moved);
        DecodedStrings dec;
        StringRemap remap;
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
        size_t want_pairs = 0;
        for (const auto& [o, n] : r.moved) {
            int ot = r.old_tier_of(o);
            if (ot != r.new_tier_of(n) && (looked_up >> ot & 1)) want_pairs++;
        }
        CHECK_EQ(s.pairs.size(), want_pairs);
        for (const auto& [o, n] : s.pairs) CHECK(looked_up >> r.old_tier_of(o) & 1);
        CHECK(std::is_sorted(s.pairs.begin(), s.pairs.end()));
        // Every cut of the section is refused.
        for (size_t cut = 0; cut < buf.size(); cut += 1 + buf.size() / 50) {
            size_t pos = 0;
            CHECK(throws_runtime_error([&] { parse_strings_section(buf.data(), cut, pos); }));
        }
    }
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
        try { parse_strings_section(b.data(), b.size(), pos); } catch (const std::runtime_error& e) { return std::string(e.what()); }
        return std::string();
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
    const uint32_t old_2010 = r.ob[3] + 5, new_2010 = r.nb[3] + 10;
    const uint32_t referenced = 0b1001;  // core, postcode
    std::vector<char> buf;
    const auto stats = append_strings_section(buf, r.pools(), tiers.shipped, referenced,
                                              std::vector<std::pair<uint32_t, uint32_t>>());
    CHECK_EQ(stats.sent, 8u);
    DecodedStrings dec;
    StringRemap remap;
    decode_strings(buf, r, dec, remap);
    CHECK_EQ(dec.section.shipped, 1u);
    CHECK_EQ(dec.section.sent, 8u);
    CHECK(dec.rebuilt[0] == r.news[0]);
    CHECK_EQ(remap.lookup(r.ob[3]), r.nb[3]);
    CHECK_EQ(remap.lookup(old_2010), new_2010);

    // The patcher: a shipped tier must be listed; a listed one without the
    // bit (postcode) is fine, its per-file section writes it.
    const std::unordered_set<std::string> listed = {"strings_core.bin", "strings_postcode.bin", "strings_layout.json"};
    CHECK(!throws_runtime_error([&] { require_shipped_tiers_listed(dec.section.shipped, listed); }));
    std::string what;
    try { require_shipped_tiers_listed(0b1001, {"strings_core.bin"}); } catch (const std::runtime_error& e) { what = e.what(); }
    CHECK_EQ(what, std::string("Strings section ships strings_postcode.bin, which the client files don't list"));
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
    for_each_string_tier_run(o.data(), o.size(), 100, n.data(), n.size(), 100,
                             [&](uint32_t s, uint32_t e, uint32_t sh) { runs.push_back({s, e, sh}); });
    REQUIRE(runs.size() == 1);
    CHECK(runs[0] == (std::array<uint32_t, 3>{104, 106, 2}));
    // Two tiers' worth of bases: every surviving string moves by the base
    // difference, so the whole pool is one run.
    runs.clear();
    for_each_string_tier_run(o.data(), o.size(), 10, o.data(), o.size(), 7,
                             [&](uint32_t s, uint32_t e, uint32_t sh) { runs.push_back({s, e, sh}); });
    REQUIRE(runs.size() == 1);
    CHECK(runs[0] == (std::array<uint32_t, 3>{10, 20, uint32_t(-3)}));
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
    auto deltas = parse_geo_list_deltas(buf.data(), buf.size(), pos, 7);
    CHECK_EQ(pos, buf.size());
    REQUIRE(deltas.size() == 7);
    std::vector<uint32_t> scratch;
    for (size_t i = 0; i < 7; i++) {
        CHECK_EQ(deltas[i].cell_id, uint64_t(10 + i));
        CHECK_EQ(deltas[i].replace, cases[i].replace);
        std::vector<uint32_t> ids = cases[i].derived;
        deltas[i].apply(ids, scratch);
        CHECK(ids == cases[i].target);
    }
}

TEST(patch_format_geo_list_delta_rejects_truncation_and_disorder) {
    std::vector<char> buf;
    append_geo_list_delta(buf, 7, {1}, {2});
    append_geo_list_delta(buf, 9, {}, {3});
    for (size_t cut = 0; cut < buf.size(); cut++) {
        size_t pos = 0;
        bool threw = false;
        try { parse_geo_list_deltas(buf.data(), cut, pos, 2); } catch (const std::runtime_error&) { threw = true; }
        CHECK(threw);
    }
    std::vector<char> back;
    append_geo_list_delta(back, 9, {}, {3});
    append_geo_list_delta(back, 7, {1}, {2});
    size_t pos = 0;
    bool threw = false;
    try { parse_geo_list_deltas(back.data(), back.size(), pos, 2); } catch (const std::runtime_error&) { threw = true; }
    CHECK(threw);
}

// --- read_entry_list ---

TEST(patch_format_read_entry_list_reads_the_list_at_an_offset) {
    // An empty list at 0, then {5, 6} at 2.
    std::vector<char> entries = {0, 0, 2, 0, 5, 0, 0, 0, 6, 0, 0, 0};
    ByteSpan sut{entries.data(), entries.size()};
    std::vector<uint32_t> ids = {9};
    read_entry_list(sut, 2, ids);
    CHECK(ids == std::vector<uint32_t>({5, 6}));
    read_entry_list(sut, 0, ids);
    CHECK(ids.empty());
}

TEST(patch_format_read_entry_list_reads_bad_offsets_as_empty) {
    std::vector<char> entries = {3, 0, 5, 0, 0, 0, 6, 0, 0, 0};  // count 3, only 2 ids
    ByteSpan sut{entries.data(), entries.size()};
    for (uint32_t off : {0xFFFFFFFFu,    // NO_DATA
                         0u,             // the list overruns the file
                         9u,             // the count runs past the end
                         0xFFFFFFFEu}) { // off + 2 wraps in 32 bits
        std::vector<uint32_t> ids = {9};
        read_entry_list(sut, off, ids);
        CHECK(ids.empty());
    }
}
