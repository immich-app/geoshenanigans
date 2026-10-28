// geocoder-gc, the encoder and decoder of installed sets (tools/gc.cpp and
// tools/gc_*.h): malformed encoded sets fail through gc's die() (exit 2) in
// its decoders, before anything is sized from a bad count, each case in a
// child process; address footprints past abs2's bound round trip; and the
// tool never writes its source, refuses an encoded dir as encode input, an
// output dir holding files, and a POI set whose offsets do not fit the
// encoding. (The patcher's decoders: tests/test_gcs_codec.cpp.)
#include "gc_addr.h"
#include "gc_gxc.h"
#include "gc_poly.h"
#include "gc_ways.h"

#include <csignal>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <grp.h>
#include <iterator>
#include <new>
#include <random>
#include <string>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include "child_process.h"
#include "scratch_dir.h"
#include "test_framework.h"
#include "tool_runs.h"

using gctest::exit_status_in_child;

namespace {

void write_bytes(const std::string& path, const std::vector<uint8_t>& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()), std::streamsize(bytes.size()));
}

std::vector<uint8_t> read_bytes(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return std::vector<uint8_t>(std::istreambuf_iterator<char>(in), {});
}

// n little-endian bytes of v.
std::vector<uint8_t> le(uint64_t v, int n) {
    std::vector<uint8_t> b;
    for (int k = 0; k < n; k++) b.push_back(uint8_t(v >> (8 * k)));
    return b;
}

// LEB128, as the encoded columns hold counts.
void uv(std::vector<uint8_t>& o, uint64_t v) {
    while (v >= 0x80) {
        o.push_back(uint8_t(v) | 0x80);
        v >>= 7;
    }
    o.push_back(uint8_t(v));
}

void put32(std::vector<uint8_t>& o, uint32_t v) {
    for (int b = 0; b < 4; b++) o.push_back(uint8_t(v >> (8 * b)));
}

uint32_t f32_bits(float f) {
    uint32_t b;
    memcpy(&b, &f, 4);
    return b;
}

void putf(std::vector<uint8_t>& o, float f) { put32(o, f32_bits(f)); }

// 40 street (or interpolation) ways of 0 to 3 nodes; node_offset is the
// running node count (0 for a way without nodes).
void ways_today_set(const std::string& dir, bool street) {
    std::vector<uint8_t> ways, nodes;
    uint32_t off = 0;
    for (uint32_t w = 0; w < 40; w++) {
        const uint16_t cnt = uint16_t(w % 4);
        const size_t at = ways.size();
        put32(ways, cnt ? off : 0);
        ways.push_back(uint8_t(cnt));
        ways.push_back(uint8_t(cnt >> 8));
        ways.push_back(0);
        ways.push_back(0);
        put32(ways, 1000 + w);  // name, or the interpolation's street
        if (!street) {
            put32(ways, 2 * w);      // start
            put32(ways, 2 * w + 9);  // end
            ways.push_back(uint8_t(w % 3));
            ways.resize(at + 24, 0);
        }
        for (uint16_t j = 0; j < cnt; j++) {
            putf(nodes, 47.0f + float(w) * 1e-4f + float(j) * 1e-5f);
            putf(nodes, 8.0f + float(w) * 1e-4f - float(j) * 1e-5f);
        }
        off += cnt;
    }
    std::filesystem::create_directories(dir);
    write_bytes(dir + (street ? "/street_ways.bin" : "/interp_ways.bin"), ways);
    write_bytes(dir + (street ? "/street_nodes.bin" : "/interp_nodes.bin"), nodes);
}

// Address points with footprints of up to 120000 vertices: 69999 still fit
// abs2, 70000 open and 70001 closed (70000 once the closing vertex is
// dropped) do not. Narrow (u8) and wide (u16) values, every encoding.
struct AddrSet {
    std::vector<uint8_t> points, verts;
};

AddrSet big_footprints() {
    std::mt19937 rng(20261027);
    struct Spec { uint32_t vc; uint8_t enc; bool closed; uint32_t span; };
    const Spec specs[] = {{0, 0, false, 0},        {70000, 0, false, 65536}, {5, 1, true, 200},          {69999, 2, false, 65536},
                          {70001, 0, true, 65536},  {0, 0, false, 0},         {70000, 1, false, 256 - 1}, {3, 3, false, 65536},
                          {80000, 3, false, 65536}, {120000, 2, true, 200}};
    AddrSet s;
    for (const Spec& sp : specs) {
        uint32_t rec[7] = {f32_bits(float(rng() % 1800000) / 1e4f - 90.f), f32_bits(float(rng() % 3600000) / 1e4f - 180.f),
                           uint32_t(rng()), uint32_t(rng()), uint32_t(rng()), gcset::NO, 0};
        if (sp.vc) {
            rec[5] = uint32_t(s.verts.size());
            rec[6] = sp.vc;
            uint8_t h[10] = {sp.enc, 0};
            gcset::wr32(h + 2, f32_bits(float(rng() % 1800000) / 1e4f - 90.f));
            gcset::wr32(h + 6, f32_bits(float(rng() % 3600000) / 1e4f - 180.f));
            s.verts.insert(s.verts.end(), h, h + 10);
            const size_t vs = sp.enc == 3 ? 8 : 4, first = s.verts.size();
            for (uint32_t k = 0; k < sp.vc; k++) {
                uint8_t v[8];
                if (vs == 8) {
                    for (auto& b : v) b = uint8_t(rng());
                } else {
                    gcset::wr16(v, uint16_t(rng() % sp.span));
                    gcset::wr16(v + 2, uint16_t(rng() % sp.span));
                }
                if (k + 1 == sp.vc && sp.closed) memcpy(v, &s.verts[first], vs);
                s.verts.insert(s.verts.end(), v, v + vs);
            }
        }
        const uint8_t* r = reinterpret_cast<const uint8_t*>(rec);
        s.points.insert(s.points.end(), r, r + 28);
    }
    return s;
}

// A POI record of today's format: id, vertex offset and count, the rest 0.
std::vector<uint8_t> poi_record(uint32_t offset, uint32_t count) {
    std::vector<uint8_t> r = le(7, 8);
    put32(r, offset);
    put32(r, count);
    r.resize(36, 0);
    return r;
}

// polygons: vertex counts of all-zero closed rings (enc 0), back to back,
// then trailing_points point records. poi_vertices.bin is a sparse file.
void poi_today_set(const std::string& dir, const std::vector<uint32_t>& polygons, size_t trailing_points) {
    std::filesystem::create_directories(dir);
    std::vector<uint8_t> recs;
    uint64_t off = 0;
    for (uint32_t vc : polygons) {
        auto r = poi_record(uint32_t(off), vc);
        recs.insert(recs.end(), r.begin(), r.end());
        off += 10 + 4 * uint64_t(vc);
    }
    for (size_t i = 0; i < trailing_points; i++) {
        auto r = poi_record(gcset::NO, 0);
        recs.insert(recs.end(), r.begin(), r.end());
    }
    write_bytes(dir + "/poi_records.bin", recs);
    write_bytes(dir + "/poi_vertices.bin", {});
    std::filesystem::resize_file(dir + "/poi_vertices.bin", off);
}

// Three street ways of two nodes each and a file of no family.
void street_today_set(const std::string& dir) {
    std::vector<uint8_t> ways, nodes;
    for (uint32_t w = 0; w < 3; w++) {
        put32(ways, 2 * w);
        ways.push_back(2);
        ways.insert(ways.end(), {0, 0, 0});
        put32(ways, 100 + w);
        for (const float f : {47.0f + float(w) * 1e-4f, 8.0f, 47.001f, 8.001f + float(w) * 1e-4f}) putf(nodes, f);
    }
    std::filesystem::create_directories(dir);
    write_bytes(dir + "/a.bin", {'k', 'e', 'e', 'p'});
    write_bytes(dir + "/street_ways.bin", ways);
    write_bytes(dir + "/street_nodes.bin", nodes);
}

// A gc run: exit status 0, and its log.
struct GcRun {
    bool ok;
    std::string log;
};

GcRun run_gc(const std::vector<std::string>& args, const std::string& log) {
    std::vector<std::string> words{GEOCODER_GC_BIN};
    words.insert(words.end(), args.begin(), args.end());
    std::filesystem::remove(log);
    GcRun r;
    r.ok = gctest::run(words, log);
    auto text = read_file(log);
    r.log.assign(text.begin(), text.end());
    return r;
}

}  // namespace

// A child whose body throws (a bad_alloc from an unchecked size, say) ends
// there with its own status: the cases here run their decoders in children.
TEST(exit_status_in_child_ends_a_child_whose_body_throws) {
    CHECK_EQ(exit_status_in_child([]() -> int { throw std::bad_alloc(); }, 5), gctest::EXCEPTION_ESCAPED);
    CHECK_EQ(exit_status_in_child([] { return 7; }, 5), 7);
}

// tag 0x04: an abs2 blob (wide, 2 + 2 count bytes per vertex after an 8-byte
// bmin); 0x20: the raw escape (today's blob, 10 + 4 count bytes), whose count
// must fit the record's u32. Both decoders: points only and whole.
TEST(gc_address_decoders_refuse_a_footprint_count_past_u32) {
    ScratchDir dir("gc-addr-count");
    const std::string src = dir.path() + "/in", out = dir.path() + "/out";
    std::filesystem::create_directories(src);
    std::filesystem::create_directories(out);
    write_bytes(src + "/addr_points.col", std::vector<uint8_t>(24));
    for (const uint8_t tag : {uint8_t(0x04), uint8_t(0x20)}) {
        for (const uint64_t count : {uint64_t(5), (uint64_t(1) << 32) + 5, (uint64_t(1) << 63) + 5}) {
            std::vector<uint8_t> bytes{tag};
            uv(bytes, count);
            bytes.resize(bytes.size() + (tag == 0x20 ? 30 : 28));
            write_bytes(src + "/addr_vertices.abs2", bytes);
            for (const bool whole : {false, true}) {
                const int status = exit_status_in_child(
                    [&] {
                        gcset::Manifest m;
                        m.set("addr.n", 1);
                        if (whole) gcset::addr::decode(src, out, m);
                        else gcset::addr::decode_points_only(src, m, out);
                        return 0;
                    },
                    5);

                CHECK_EQ(status, count == 5 ? 0 : 2);
            }
        }
    }
}

// An escaped footprint (0x20) whose count fits the record's u32 but not the
// file is refused as a truncated file before the reader's buffer grows to the
// footprint's 16 GiB, which the child's capped address space can't hold: and
// 2^30 vertices in a ten-byte payload (10 + 4 x 2^30 bytes, which a 32-bit
// size_t wraps to the payload's 10). Both decoders: points only and whole.
TEST(gc_address_decoders_refuse_a_footprint_past_the_end_before_reading_it) {
    ScratchDir dir("gc-addr-end");
    const std::string src = dir.path() + "/in", out = dir.path() + "/out";
    std::filesystem::create_directories(src);
    std::filesystem::create_directories(out);
    write_bytes(src + "/addr_points.col", std::vector<uint8_t>(24));
    for (const auto& [count, payload] : {std::pair<uint64_t, size_t>{0xFFFFFFFF, 30}, {0x40000000, 10}}) {
        std::vector<uint8_t> bytes{0x20};
        uv(bytes, count);
        bytes.resize(bytes.size() + payload);
        write_bytes(src + "/addr_vertices.abs2", bytes);
        for (const bool whole : {false, true}) {
            const int status = exit_status_in_child(
                [&] {
#ifndef __SANITIZE_ADDRESS__
                    struct rlimit as{size_t(4) << 30, size_t(4) << 30};
                    setrlimit(RLIMIT_AS, &as);
#endif
                    gcset::Manifest m;
                    m.set("addr.n", 1);
                    if (whole) gcset::addr::decode(src, out, m);
                    else gcset::addr::decode_points_only(src, m, out);
                    return 0;
                },
                5);

            if (status != 2)
                std::printf("    count %llu, %s decode: exit %d\n", (unsigned long long)count, whole ? "whole" : "points-only", status);
            CHECK_EQ(status, 2);
        }
    }
}

// A record of 2^30 vertices with a ten-byte addr_vertices.bin (10 + 4 x 2^30
// bytes, which a 32-bit size_t wraps to the file's 10) is refused as a
// truncated file by both encoders: in one pass and its vertices pass.
TEST(gc_address_encoders_refuse_a_footprint_past_the_end_of_the_file) {
    ScratchDir dir("gc-addr-enc-end");
    const std::string src = dir.path() + "/in";
    std::filesystem::create_directories(src);
    std::vector<uint8_t> rec(20, 0);
    put32(rec, 0);           // vertex_offset
    put32(rec, 0x40000000);  // vertex_count
    write_bytes(src + "/addr_points.bin", rec);
    write_bytes(src + "/addr_vertices.bin", std::vector<uint8_t>(10));
    for (const bool passes : {false, true}) {
        const std::string out = dir.path() + (passes ? "/passes" : "/one");
        std::filesystem::create_directories(out);
        const int status = exit_status_in_child(
            [&] {
                gcset::Manifest m;
                if (passes) {
                    gcset::want_files() = {"addr_points.bin"};
                    gcset::addr::encode(src, out, m);
                    gcset::want_files() = {"addr_vertices.bin"};
                }
                gcset::addr::encode(src, out, m);
                return 0;
            },
            5);

        if (status != 2) std::printf("    %s: exit %d\n", passes ? "two passes" : "one pass", status);
        CHECK_EQ(status, 2);
    }
}

// Malformed list and address counts in a gxc row, each a one-row encoded set.
// The child's address space is capped: an unchecked count runs out of it.
TEST(gc_gxc_decoder_refuses_malformed_counts) {
    constexpr uint64_t MAX = ~uint64_t(0);  // LEB128 FF x9 01: count - 1 + 1 wraps to 0
    struct Case {
        const char* name;
        int s, a;                      // street list state, address state of the row
        std::vector<uint64_t> street;  // gxc_street.bin varints
        std::vector<uint64_t> addr;    // address payload varints
        uint64_t addr_ids;             // next address id after the row
        bool valid;
    };
    const Case cases[] = {
        {"valid row", 1, 1, {1, 4, 0}, {2}, 2, true},
        {"street count - 1 = 2^64 - 1", 1, 0, {MAX, 0}, {}, 0, false},
        {"street count 65536", 3, 0, {0xFFFF, 0}, {}, 0, false},
        {"address state 1, 2^40 ids", 0, 1, {}, {uint64_t(1) << 40}, 0, false},
        {"address state 1, no ids", 0, 1, {}, {0}, 0, false},
        {"address state 2, index gap 2^40", 0, 2, {}, {3, 1, uint64_t(1) << 40, 0}, 3, false},
        {"address state 2, 2^40 copies", 0, 2, {}, {1, 1, 0, uint64_t(1) << 40}, 1, false},
        {"address state 2, more repeats than ids", 0, 2, {}, {1, 2, 0, 0, 0, 0}, 1, false},
        {"address state 3, count - 1 = 2^64 - 1", 0, 3, {}, {1, MAX, 0}, 1, false},
    };
    ScratchDir dir("gc-gxc-counts");
    int n = 0;
    for (const Case& c : cases) {
        const std::string src = dir.path() + "/" + std::to_string(n++), out = src + "/out";
        std::filesystem::create_directories(out);
        uint8_t top[32] = {};
        gcset::wr64(top, uint64_t(1) << 40);
        gcset::wr32(top + 12, gcset::NO);  // no street / interp list in effect at the block start
        gcset::wr32(top + 16, gcset::NO);
        write_bytes(src + "/gxc_top.bin", std::vector<uint8_t>(top, top + 32));
        std::vector<uint8_t> rows{uint8_t(c.s | c.a << 2)}, street;
        for (uint64_t v : c.addr) uv(rows, v);
        for (uint64_t v : c.street) uv(street, v);
        write_bytes(src + "/gxc_rows.bin", rows);
        write_bytes(src + "/gxc_street.bin", street);
        write_bytes(src + "/gxc_interp.bin", {});

        const int status = exit_status_in_child(
            [&] {
#ifndef __SANITIZE_ADDRESS__
                struct rlimit as{size_t(4) << 30, size_t(4) << 30};
                setrlimit(RLIMIT_AS, &as);
#endif
                gcset::Manifest m;
                m.set("gxc.rows", 1);
                m.set("gxc.B", 128);
                m.set("gxc.addr_ids", c.addr_ids);
                gcset::gxc::decode(src, out, m);
                return 0;
            },
            20);

        if (status != (c.valid ? 0 : 2)) std::printf("    %s: exit %d\n", c.name, status);
        CHECK_EQ(status, c.valid ? 0 : 2);
    }
}

namespace {

// A gxc row: its header byte (list states, address state, cell step) and the
// varints after it (an escaped step, then the address payload).
struct GxcRow {
    uint8_t h;
    std::vector<uint64_t> after;
};

// An encoded geo index of rows at dir: blocks of 128 rows starting at cells
// (2^40 by default), no list in effect at a block's start (a later block's
// rows list nothing before it), street: gxc_street.bin's varints.
void write_gxc(const std::string& dir, const std::vector<GxcRow>& rows, const std::vector<uint64_t>& street,
               const std::vector<uint64_t>& cells = {uint64_t(1) << 40}) {
    std::filesystem::create_directories(dir);
    std::vector<uint8_t> top, r, s;
    for (size_t i = 0; i < rows.size(); i++) {
        if (i % 128 == 0) {
            uint8_t t[32] = {};
            gcset::wr64(t, cells[i / 128]);
            gcset::wr32(t + 8, uint32_t(r.size()));
            gcset::wr32(t + 12, gcset::NO);
            gcset::wr32(t + 16, gcset::NO);
            top.insert(top.end(), t, t + 32);
        }
        r.push_back(rows[i].h);
        for (uint64_t v : rows[i].after) uv(r, v);
    }
    for (uint64_t v : street) uv(s, v);
    write_bytes(dir + "/gxc_top.bin", top);
    write_bytes(dir + "/gxc_rows.bin", r);
    write_bytes(dir + "/gxc_street.bin", s);
    write_bytes(dir + "/gxc_interp.bin", {});
}

// gcset's zigzag coding of a signed delta.
uint64_t zz(int64_t v) { return gcset::zz64(v); }

}  // namespace

// List and address ids that leave the u32 range, or pass the set's address id
// count, are refused, each case a one- or two-row encoded set (street list
// state 1 or address state 1 or 3 in each row): a first id 2^32 (narrowed
// to 0), 1 + (2^63 - 1) (a signed overflow), a gap from 2^32 - 1 (wrapped to
// 0), an address advance of 2^64 - 1 (next wrapped to 0 by the next row), an
// id at or past the count, a count past 2^32. Ids up to 2^32 - 1 decode.
TEST(gc_gxc_decoder_refuses_ids_past_u32_and_the_id_count) {
    struct Case {
        const char* name;
        std::vector<GxcRow> rows;
        std::vector<uint64_t> street;
        uint64_t addr_ids;
        bool valid;
    };
    const uint64_t MAX = ~uint64_t(0), U32 = uint64_t(1) << 32;
    const Case cases[] = {
        {"valid ids", {{1 | 1 << 2, {2}}, {1 | 3 << 2, {0, 0, zz(-1)}}}, {0, zz(1), 1, zz(0xFFFFFFFC), 1}, 2, true},
        {"street first id 2^32", {{1, {}}}, {0, zz(int64_t(U32))}, 0, false},
        {"street first id 1 + 2^63 - 1", {{1, {}}, {1, {}}}, {0, zz(1), 0, zz(INT64_MAX)}, 0, false},
        {"street id 2^32 - 1 + 1", {{1, {}}}, {1, zz(int64_t(U32 - 1)), 0}, 0, false},
        {"address advance 2^64 - 1", {{3 << 2, {MAX, 0, zz(0)}}, {1 << 2, {1}}}, {}, 0, false},
        {"address first id 2^32", {{3 << 2, {1, 0, zz(int64_t(U32))}}}, {}, 1, false},
        {"address id past the count", {{3 << 2, {0, 0, zz(5)}}}, {}, 0, false},
        {"address id count 2^32 + 1", {{3 << 2, {U32 + 1, 0, zz(0)}}}, {}, U32 + 1, false},
    };
    ScratchDir dir("gc-gxc-ids");
    int n = 0;
    for (const Case& c : cases) {
        const std::string src = dir.path() + "/" + std::to_string(n++), out = src + "/out";
        write_gxc(src, c.rows, c.street);
        std::filesystem::create_directories(out);

        const int status = exit_status_in_child(
            [&] {
                gcset::Manifest m;
                m.set("gxc.rows", c.rows.size());
                m.set("gxc.B", 128);
                m.set("gxc.addr_ids", c.addr_ids);
                gcset::gxc::decode(src, out, m);
                return 0;
            },
            20);

        if (status != (c.valid ? 0 : 2)) std::printf("    %s: exit %d\n", c.name, status);
        CHECK_EQ(status, c.valid ? 0 : 2);
    }
}

// gxc rows out of cell order are refused. By the decoder: an escaped cell step
// of 2^64 - 1 (its + 4 wrapped to 3) or 2^37 - 4 (2^37, shifted past the u64 to
// 0: the same cell again), a step from the last cell id (wrapped to 0), and a
// block starting at its previous row's cell; a step of 2^20 and a block past
// its previous row decode. By the encoder: a block's first row at the cell of
// the row before it.
TEST(gc_gxc_rows_out_of_cell_order_are_refused) {
    const uint64_t MAX = ~uint64_t(0), C = uint64_t(1) << 40, LAST = MAX - ((uint64_t(1) << 27) - 1);
    std::vector<GxcRow> blocks(129, GxcRow{0, {}});  // steps of one cell, no lists
    struct Case {
        const char* name;
        std::vector<GxcRow> rows;
        std::vector<uint64_t> cells;
        bool valid;
    };
    const Case cases[] = {
        {"escaped step 2^20", {{0, {}}, {3 << 6, {(1 << 20) - 4}}}, {C}, true},
        {"block past its previous row", blocks, {C, C + (uint64_t(128) << 27)}, true},
        {"escaped step 2^64 - 1 + 4", {{0, {}}, {3 << 6, {MAX}}}, {C}, false},
        {"escaped step 2^37", {{0, {}}, {3 << 6, {(uint64_t(1) << 37) - 4}}}, {C}, false},
        {"step from the last cell id", {{0, {}}, {0, {}}}, {LAST}, false},
        {"block at its previous row's cell", blocks, {C, C + (uint64_t(127) << 27)}, false},
    };
    ScratchDir dir("gc-gxc-cells");
    int n = 0;
    for (const Case& c : cases) {
        const std::string src = dir.path() + "/" + std::to_string(n++), out = src + "/out";
        write_gxc(src, c.rows, {}, c.cells);
        std::filesystem::create_directories(out);

        const int status = exit_status_in_child(
            [&] {
                gcset::Manifest m;
                m.set("gxc.rows", c.rows.size());
                m.set("gxc.B", 128);
                m.set("gxc.addr_ids", 0);
                gcset::gxc::decode(src, out, m);
                return 0;
            },
            20);

        if (status != (c.valid ? 0 : 2)) std::printf("    %s: exit %d\n", c.name, status);
        CHECK_EQ(status, c.valid ? 0 : 2);
    }
    for (const bool sorted : {true, false}) {
        const std::string src = dir.path() + (sorted ? "/sorted" : "/unsorted"), out = src + ".enc";
        std::filesystem::create_directories(src);
        std::filesystem::create_directories(out);
        std::vector<uint8_t> geo;
        for (uint64_t i = 0; i < 129; i++) {
            const std::vector<uint8_t> cell = le(C + ((i - (!sorted && i == 128)) << 27), 8), none = le(gcset::NO, 4);
            geo.insert(geo.end(), cell.begin(), cell.end());
            for (int t = 0; t < 3; t++) geo.insert(geo.end(), none.begin(), none.end());
        }
        write_bytes(src + "/geo_cells.bin", geo);
        for (const char* e : gcset::gxc::ENTRY) write_bytes(src + "/" + e, {});

        const int status = exit_status_in_child(
            [&] {
                gcset::Manifest m;
                gcset::gxc::encode(src, out, m);
                return 0;
            },
            20);

        if (status != (sorted ? 0 : 2)) std::printf("    encoder, %s rows: exit %d\n", sorted ? "sorted" : "unsorted", status);
        CHECK_EQ(status, sorted ? 0 : 2);
    }
}

// A way's code with widths the encoder never writes (first coordinate past 4
// bytes, deltas past 32 bits) is refused instead of shifting a u32 past its
// width; the widest valid code decodes. One street way of two nodes: its
// block is 2 wf bytes then (wl + wg) bits.
TEST(gc_ways_decoder_refuses_codes_it_never_writes) {
    struct Case { int wf, wl, wg; bool valid; };
    const Case cases[] = {{4, 32, 32, true}, {0, 0, 0, true}, {5, 0, 0, false}, {7, 0, 0, false},
                          {0, 33, 0, false}, {4, 32, 63, false}};
    ScratchDir dir("gc-ways-code");
    const std::string src = dir.path() + "/in", out = dir.path() + "/out";
    std::filesystem::create_directories(src);
    std::filesystem::create_directories(out);
    for (const Case& c : cases) {
        const uint16_t cnt = 2, code = uint16_t(c.wl | c.wg << 6 | c.wf << 13);
        write_bytes(src + "/street.groups", std::vector<uint8_t>(12));
        write_bytes(src + "/street.nodes2", std::vector<uint8_t>(gcset::ways::block_size(cnt, code), 0x5A));
        write_bytes(src + "/street.cnt", le(cnt, 2));
        write_bytes(src + "/street.code", le(code, 2));
        write_bytes(src + "/street.name", le(7, 4));

        const int status = exit_status_in_child(
            [&] {
                gcset::Manifest m;
                m.set("street.ways", 1);
                m.set("street.nodes", cnt);
                m.set("street.G", 16);
                gcset::ways::decode(src, out, true, m);
                return 0;
            },
            5);

        if (status != (c.valid ? 0 : 2)) std::printf("    wf=%d wl=%d wg=%d: exit %d\n", c.wf, c.wl, c.wg, status);
        CHECK_EQ(status, c.valid ? 0 : 2);
    }
}

// An encoded street or interpolation set with bytes past the end of any one
// of its columns fails in both ways decoders (the full and the ways-only
// decode) instead of decoding and dropping them; the set as encoded decodes
// in both. (A streets ways-only decode doesn't read street.code: the
// nodes-only decode it runs beside does, and checks it.)
TEST(gc_ways_decoders_refuse_bytes_past_a_column) {
    ScratchDir dir("gc-ways-columns");
    const std::string& root = dir.path();
    for (const bool street : {true, false}) {
        const std::string fam = street ? "street" : "interp";
        const std::string src = root + "/" + fam + ".src", enc = root + "/" + fam + ".enc";
        ways_today_set(src, street);
        std::filesystem::create_directories(enc);
        gcset::Manifest m;
        gcset::ways::encode(src, enc, street, m);
        const std::vector<std::string> columns = street ? std::vector<std::string>{"cnt", "code", "name"}
                                                        : std::vector<std::string>{"cntcode", "street", "start", "span", "interp"};
        std::vector<std::string> cases{""};  // "": the set as encoded
        cases.insert(cases.end(), columns.begin(), columns.end());
        for (const std::string& column : cases) {
            const std::string bad = root + "/" + fam + "." + (column.empty() ? "as-encoded" : column);
            std::filesystem::copy(enc, bad);
            if (!column.empty()) {
                std::ofstream f(bad + "/" + fam + "." + column, std::ios::binary | std::ios::app);
                f.put('\x5a');
            }
            for (const bool ways_only : {false, true}) {
                if (ways_only && street && column == "code") continue;
                const std::string out = bad + (ways_only ? ".ways-only" : ".full");
                std::filesystem::create_directories(out);

                const int status = exit_status_in_child(
                    [&] {
                        if (ways_only) gcset::want_files() = {street ? "street_ways.bin" : "interp_ways.bin"};
                        gcset::ways::decode(bad, out, street, m);
                        return 0;
                    },
                    20);

                if (status != (column.empty() ? 0 : 2))
                    std::printf("    %s.%s (%s decode): exit %d\n", fam.c_str(), column.empty() ? "<as encoded>" : column.c_str(),
                                ways_only ? "ways-only" : "full", status);
                CHECK_EQ(status, column.empty() ? 0 : 2);
            }
        }
    }
}

// Footprints past abs2's bound round trip through the raw escape, byte for
// byte: the encoder in one pass and in its two streamed passes, and the
// decoders whole, points only and vertices only.
TEST(gc_address_footprints_past_abs2s_bound_round_trip) {
    ScratchDir dir("gc-addr-big");
    const std::string& root = dir.path();
    const std::string src = root + "/src", col = root + "/col";
    std::filesystem::create_directories(src);
    const AddrSet set = big_footprints();
    write_bytes(src + "/addr_points.bin", set.points);
    write_bytes(src + "/addr_vertices.bin", set.verts);
    {
        const std::string dec = col + ".dec";
        std::filesystem::create_directories(col);
        std::filesystem::create_directories(dec);
        gcset::Manifest m;
        gcset::addr::encode(src, col, m);
        gcset::addr::decode(col, dec, m);

        CHECK(read_bytes(dec + "/addr_points.bin") == set.points);
        CHECK(read_bytes(dec + "/addr_vertices.bin") == set.verts);
    }
    const std::string split = root + "/split";
    std::filesystem::create_directories(split);
    gcset::Manifest ms;
    gcset::want_files() = {"addr_points.bin"};
    gcset::addr::encode(src, split, ms);
    gcset::want_files() = {"addr_vertices.bin"};
    gcset::addr::encode(src, split, ms);
    gcset::want_files().clear();
    for (const char* f : {"/addr_points.col", "/addr_vertices.abs2"}) CHECK(read_bytes(split + f) == read_bytes(col + f));
    gcset::Manifest m;  // the keys the decoders read
    m.set("addr.n", uint64_t(set.points.size() / 28));
    for (const std::string f : {"addr_points.bin", "addr_vertices.bin"}) {
        const std::string dec = root + "/only." + f;
        std::filesystem::create_directories(dec);
        gcset::want_files() = {f};
        gcset::addr::decode(col, dec, m);
        gcset::want_files().clear();

        CHECK(read_bytes(dec + "/" + f) == (f == "addr_points.bin" ? set.points : set.verts));
    }
}

// When a FIFO's pipe can't be enlarged, the grouped FIFO writers fail at once
// instead of deadlocking with their reader. A writer child drops to nobody
// (when run as root), holds pipes until it is past its pipe page limit
// (fs.pipe-user-pages-soft), then writes gxc-decode-shaped rows (geo row,
// 100-id street list, 1-id address list) through four FIFOs to a reader in
// the patcher's order (row, then its lists). gc's die (exit 2) on the pipe
// size passes; a writer still going after 20 s (the deadlock) fails.
TEST(gc_fifo_writers_fail_when_the_pipe_cannot_grow) {
    static const char* const NAMES[4] = {"geo", "street", "addr", "interp"};
    // A 64 KiB window per FIFO, one read() until n bytes are in.
    struct Window {
        int fd = -1;
        std::vector<char> b = std::vector<char>(64 << 10);
        size_t off = 0, len = 0;
        const char* need(size_t n) {
            if (len - off < n) {
                memmove(&b[0], &b[off], len - off);
                len -= off;
                off = 0;
                while (len < n) {
                    const ssize_t r = read(fd, &b[len], b.size() - len);
                    if (r <= 0) _exit(0);  // the writer went away
                    len += size_t(r);
                }
            }
            off += n;
            return &b[off - n];
        }
    };
    ScratchDir scratch("gc-fifo-capacity");
    const std::string& dir = scratch.path();
    chmod(dir.c_str(), 0777);
    for (const char* n : NAMES) {
        REQUIRE(mkfifo((dir + "/" + n).c_str(), 0666) == 0);
        chmod((dir + "/" + n).c_str(), 0666);
    }
    const std::string log = dir + "/writer.log";
    const int log_fd = open(log.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
    REQUIRE(log_fd >= 0);
    std::fflush(nullptr);
    const pid_t writer = fork();
    REQUIRE(writer >= 0);
    if (!writer) {
        setpgid(0, 0);  // with its reader: the parent ends both
        dup2(log_fd, 2);
        if (geteuid() == 0 && (setgroups(0, nullptr) || setgid(65534) || setuid(65534))) _exit(5);
        // (The uid change made it undumpable: LeakSanitizer's exit check
        // traces the process, and fails where it can't.)
        prctl(PR_SET_DUMPABLE, 1);
        // Pipes of up to 1 MiB until the user is past its pipe page limit,
        // where a new pipe gets the minimum size and none can grow (stopping
        // at the first refused 1 MiB would leave up to 1 MiB minus a page
        // free, enough for a FIFO's enlargement).
        for (int k = 0;; k++) {
            if (k == 4096) {
                std::fprintf(stderr, "no pipe page limit reached, no refusal to test\n");
                _exit(77);
            }
            int p[2];
            if (pipe(p)) {
                std::fprintf(stderr, "the descriptor limit comes before the pipe page limit\n");
                _exit(77);
            }
            if (fcntl(p[1], F_GETPIPE_SZ) < 65536) break;
            fcntl(p[1], F_SETPIPE_SZ, int(gcset::FIFO_PIPE));
        }
        const pid_t reader = fork();
        if (reader < 0) _exit(6);
        if (!reader) {
            alarm(30);
            Window w[4];
            for (int k = 0; k < 4; k++)
                if ((w[k].fd = open((dir + "/" + NAMES[k]).c_str(), O_RDONLY)) < 0) _exit(7);
            for (;;) {
                w[0].need(20);
                uint16_t n;
                memcpy(&n, w[1].need(2), 2);
                w[1].need(4 * size_t(n));
                memcpy(&n, w[2].need(2), 2);
                w[2].need(4 * size_t(n));
            }
        }
        alarm(20);
        gcset::Writer geo(dir + "/geo"), street(dir + "/street"), addr(dir + "/addr"), interp(dir + "/interp");
        gcset::FlushGroup fg({&geo, &street, &addr, &interp});
        const std::vector<uint32_t> ids(100, 7);
        uint8_t row[20] = {};
        for (int r = 0; r < 20000; r++) {
            street.u16(uint16_t(ids.size()));
            street.put(ids.data(), 4 * ids.size());
            addr.u16(1);
            addr.u32(1);
            geo.put(row, 20);
            fg.row_done();
        }
        _exit(0);  // (the pipes were wide after all: no deadlock to look for)
    }
    int status = 0;
    while (waitpid(writer, &status, 0) < 0) {}
    kill(-writer, SIGKILL);
    std::string said(4096, '\0');
    const ssize_t got = pread(log_fd, &said[0], said.size(), 0);
    said.resize(got > 0 ? size_t(got) : 0);
    close(log_fd);
    if (WIFEXITED(status) && WEXITSTATUS(status) == 77) {
        std::printf("    SKIP: %s", said.c_str());
        return;
    }

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 2) std::printf("    writer status %d: %s\n", status, said.c_str());
    CHECK(WIFEXITED(status));
    CHECK_EQ(WEXITSTATUS(status), 2);
    CHECK(said.find("the pipe can't hold") != std::string::npos);
}

// A source dir, named as itself or through a symlink, is never written to:
// the encode cases use a today's-format dir, the decode cases gc's encoding
// of it, so only the alias check stops them. Into a separate dir both round
// trip.
TEST(gc_never_writes_its_source_dir) {
    ScratchDir dir("gc-cli");
    const std::string& root = dir.path();
    const std::string src = root + "/source", encoded = root + "/encoded", log = root + "/gc.log";
    street_today_set(src);
    REQUIRE(run_gc({"encode", src, encoded}, log).ok);
    REQUIRE(std::filesystem::exists(encoded + "/gc.manifest"));
    for (const auto& [mode, source] : {std::pair<std::string, std::string>{"encode", src}, {"decode", encoded}}) {
        const std::string alias = source + ".alias";
        std::filesystem::create_directory_symlink(source, alias);
        for (const std::string& dest : {source, alias}) {
            const auto before = gctest::read_dir(source);

            auto r = run_gc({mode, source, dest}, log);

            CHECK(!r.ok);
            CHECK(gctest::read_dir(source) == before);
        }
    }
    REQUIRE(run_gc({"encode", src, root + "/separate.encode"}, log).ok);
    REQUIRE(run_gc({"decode", encoded, root + "/separate.decode"}, log).ok);
    CHECK(gctest::read_dir(root + "/separate.encode") == gctest::read_dir(encoded));
    CHECK(gctest::read_dir(root + "/separate.decode") == gctest::read_dir(src));
}

// A dir decoded in part (--only), the other families' encoded files linked
// across, is an encoded set of those families: its gc.manifest names them,
// and it decodes to the whole today's dir.
TEST(gc_decode_of_some_families_leaves_an_encoded_set_of_the_rest) {
    ScratchDir dir("gc-decode-part");
    const std::string& root = dir.path();
    const std::string src = root + "/source", enc = root + "/encoded", part = root + "/part", log = root + "/gc.log";
    street_today_set(src);
    poi_today_set(src, {3, 5}, 2);
    REQUIRE(run_gc({"encode", src, enc}, log).ok);
    REQUIRE(run_gc({"decode", enc, part, "--only", "poi"}, log).ok);
    const auto manifest = read_file(part + "/gc.manifest");

    auto r = run_gc({"decode", part, root + "/whole"}, log);

    CHECK(std::string(manifest.begin(), manifest.end()).find("gc.families streets\n") != std::string::npos);
    CHECK(r.ok);
    CHECK(r.ok && gctest::read_dir(root + "/whole") == gctest::read_dir(src));
}

// A run writing a set refuses an OUT_DIR holding files (here an earlier
// encoded set: a decode beside its gc.manifest would read as that set) and
// leaves it as it was; an empty one it uses. A streamed pass (--files) writes
// into a dir its caller owns: the second pass of a two-pass decode, beside
// the first's file.
TEST(gc_refuses_an_out_dir_holding_files) {
    ScratchDir dir("gc-out-dir");
    const std::string& root = dir.path();
    const std::string src = root + "/source", enc = root + "/encoded", log = root + "/gc.log";
    street_today_set(src);
    REQUIRE(run_gc({"encode", src, enc}, log).ok);
    const std::vector<std::vector<std::string>> runs = {{"decode", enc}, {"decode", enc, "--only", "streets"}, {"encode", src}};
    for (size_t k = 0; k < runs.size(); k++) {
        const std::string reused = root + "/reused" + std::to_string(k);
        std::filesystem::copy(enc, reused);
        const auto before = gctest::read_dir(reused);
        std::vector<std::string> args = runs[k];
        args.insert(args.begin() + 2, reused);

        auto r = run_gc(args, log);

        CHECK(!r.ok);
        CHECK(r.log.find("OUT_DIR " + reused + " is not empty") != std::string::npos);
        CHECK(gctest::read_dir(reused) == before);
    }
    const std::string empty = root + "/empty", streamed = root + "/streamed";
    std::filesystem::create_directories(empty);
    REQUIRE(run_gc({"decode", enc, empty}, log).ok);
    CHECK(gctest::read_dir(empty) == gctest::read_dir(src));
    REQUIRE(run_gc({"decode", enc, streamed, "--files", "street_nodes.bin", "--no-link"}, log).ok);
    auto r = run_gc({"decode", enc, streamed, "--files", "street_ways.bin", "--no-link"}, log);
    CHECK(r.ok);
    for (const char* f : {"/street_ways.bin", "/street_nodes.bin"}) CHECK(read_bytes(streamed + f) == read_bytes(src + f));
}

// An encoded dir is refused as encode input, whole or one more family with
// --only, before the destination is made: its families have no today's-format
// files, and its gc.manifest would be replaced by one naming none of them.
TEST(gc_refuses_to_encode_an_encoded_dir) {
    ScratchDir dir("gc-encoded-input");
    const std::string& root = dir.path();
    const std::string src = root + "/source", encoded = root + "/encoded", log = root + "/gc.log";
    std::filesystem::create_directories(src);
    write_bytes(src + "/way_parents.bin", le(0x00ABCDEF, 4));
    REQUIRE(run_gc({"encode", src, encoded}, log).ok);
    for (const std::vector<std::string>& extra : {std::vector<std::string>{}, {"--only", "way_parents"}}) {
        const std::string dest = root + "/again" + std::to_string(extra.size());
        std::vector<std::string> args{"encode", encoded, dest};
        args.insert(args.end(), extra.begin(), extra.end());

        auto r = run_gc(args, log);

        CHECK(!r.ok);
        CHECK(r.log.find("is already encoded (it has a gc.manifest)") != std::string::npos);
        CHECK(!std::filesystem::exists(dest));
    }
}

// R3 stores a POI record's B-open offset in 32 bits: a set whose B-open file
// ends past 4 GiB with a point record after the last polygon (whose offset is
// that file's size) must fail to encode rather than write a set its decoder
// rejects, and a small set with a trailing point round trips. The big set is
// 1024 closed all-zero rings of 2^20 vertices: every polygon starts below
// 2^32, the B-open file ends at 4,294,973,440 bytes (written under TMPDIR,
// removed at the end; the input is a sparse file).
TEST(gc_refuses_a_poi_offset_past_u32) {
    ScratchDir dir("gc-poi-offset");
    const std::string& root = dir.path();
    const std::string small = root + "/small", enc = root + "/small.enc", dec = root + "/small.dec", log = root + "/gc.log";
    poi_today_set(small, {3, 5}, 2);
    REQUIRE(run_gc({"encode", small, enc, "--only", "poi", "--no-link"}, log).ok);
    REQUIRE(run_gc({"decode", enc, dec, "--only", "poi", "--no-link"}, log).ok);
    for (const char* f : {"/poi_records.bin", "/poi_vertices.bin"}) CHECK(read_bytes(dec + f) == read_bytes(small + f));

    const std::string big = root + "/big";
    poi_today_set(big, std::vector<uint32_t>(1024, 1u << 20), 1);

    auto r = run_gc({"encode", big, root + "/big.enc", "--only", "poi", "--no-link"}, log);

    CHECK(!r.ok);
    CHECK(r.log.find("offset overflow") != std::string::npos);
}

// POI blocks the encoder never writes, each the one block of a one-record
// encoded set: a header without a stored vertex, with the closing flag (whose
// closing vertex would be copied from a vertex never decoded) or without it,
// and 2^32 stored vertices (a sparse poi_vertices.bopen), more than the
// record's u32 vertex_count holds. Refused before the block is read; a block
// of one vertex decodes. The child's address space is capped: reading the
// 16 GiB block runs out of it.
TEST(gc_poi_decoder_refuses_blocks_the_encoder_never_writes) {
    struct Case {
        const char* name;
        uint8_t flag;
        uint64_t bytes;
        bool valid;
    };
    const Case cases[] = {{"one vertex", 0, 14, true},
                          {"closing flag, no stored vertex", 1, 10, false},
                          {"no stored vertex", 0, 10, false},
                          {"2^32 stored vertices", 0, 10 + (uint64_t(4) << 32), false}};
    ScratchDir dir("gc-poi-blocks");
    int n = 0;
    for (const Case& c : cases) {
        const std::string src = dir.path() + "/" + std::to_string(n++), out = src + "/out";
        std::filesystem::create_directories(out);
        std::vector<uint8_t> rec = le(7, 8);
        put32(rec, 0);
        rec.resize(32, 0);
        write_bytes(src + "/poi_records.r3", rec);
        write_bytes(src + "/poi_vertices.bopen", {0, c.flag});
        std::filesystem::resize_file(src + "/poi_vertices.bopen", c.bytes);

        const int status = exit_status_in_child(
            [&] {
#ifndef __SANITIZE_ADDRESS__
                struct rlimit as{size_t(4) << 30, size_t(4) << 30};
                setrlimit(RLIMIT_AS, &as);
#endif
                gcset::Manifest m;
                m.set("poi.n", 1);
                gcset::poly::decode_poi(src, out, m);
                return 0;
            },
            20);

        if (status != (c.valid ? 0 : 2)) std::printf("    %s: exit %d\n", c.name, status);
        CHECK_EQ(status, c.valid ? 0 : 2);
    }
}

// A varint's tenth byte holds bit 63 alone: one past 1 would lose its other
// bits off the u64 (FF x9 02 read as 2^63 - 1), so it is refused; FF x9 01 is
// 2^64 - 1. Through the reader's buffered path (the varint already in the
// buffer, after a byte read first) and its byte-by-byte one (a fresh reader).
TEST(gc_reader_refuses_a_varint_past_64_bits) {
    ScratchDir dir("gc-varint");
    const std::string path = dir.path() + "/v";
    for (const uint8_t last : {uint8_t(0x01), uint8_t(0x02), uint8_t(0x7F)}) {
        for (const bool buffered : {false, true}) {
            std::vector<uint8_t> bytes;
            if (buffered) bytes.push_back(0);
            bytes.insert(bytes.end(), 9, 0xFF);
            bytes.push_back(last);
            write_bytes(path, bytes);

            const int status = exit_status_in_child(
                [&] {
                    gcset::Reader r(path);
                    if (buffered) r.u8();
                    return r.uv() == ~uint64_t(0) && r.eof() ? 0 : 1;
                },
                5);

            if (status != (last == 1 ? 0 : 2)) std::printf("    tenth byte %#x, %s: exit %d\n", last, buffered ? "buffered" : "byte by byte", status);
            CHECK_EQ(status, last == 1 ? 0 : 2);
        }
    }
}

// A unit past what the reader's buffer holds with its slack is refused before
// the buffer grows: a footprint of 0x3FFFFFFD four-byte vertices after its
// header, 0xFFFFFFFE bytes, in a sparse file of as many. Only a 32-bit build
// has such a unit (its buffer holds 2 GiB, its size_t the unit), where cap +
// 64 wrapped to a 62-byte buffer that the file was then read into.
TEST(gc_reader_refuses_a_unit_past_what_its_buffer_holds) {
    if (sizeof(size_t) > 4) return;
    ScratchDir dir("gc-unit");
    const std::string path = dir.path() + "/addr_vertices.bin";
    write_bytes(path, {});
    std::filesystem::resize_file(path, 0xFFFFFFFE);

    const int status = exit_status_in_child(
        [&] {
            gcset::Reader r(path);
            r.take(r.unit_size(10, 0x3FFFFFFD, 4));
            return 0;
        },
        20);

    CHECK_EQ(status, 2);
}
