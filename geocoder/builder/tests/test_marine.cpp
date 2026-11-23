// marine.h: the marine areas file tools/marine_import.py writes, and the
// stretches a marine ring's arcs are cut at.
#include "marine.h"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "marine_test_file.h"
#include "scratch_dir.h"
#include "test_framework.h"

namespace {

using gctest::bytes_of_hex;
using gctest::kTwoAreasHex;

std::string write(const ScratchDir& dir, const std::string& name, const std::string& bytes) {
    const std::string path = dir.path() + "/" + name;
    std::ofstream(path, std::ios::binary) << bytes;
    return path;
}

std::string error_of(const std::string& path) {
    try {
        marine::read_areas(path);
    } catch (const std::runtime_error& e) {
        return e.what();
    }
    return "";
}

PackedLocation pt(int32_t lat, int32_t lng) { return {lat, lng}; }

// A closed ring of n vertices round (lat, lng), counterclockwise.
std::vector<PackedLocation> round_ring(int32_t lat, int32_t lng, uint32_t n) {
    std::vector<PackedLocation> r;
    for (uint32_t i = 0; i < n; i++) {
        const double a = 2 * M_PI * i / n;
        r.push_back(pt(lat + int32_t(std::lround(1e6 * std::sin(a))), lng + int32_t(std::lround(1e6 * std::cos(a)))));
    }
    r.push_back(r.front());
    return r;
}

// The vertices where a ring's runs change way (the arc stage's way ends).
std::set<uint64_t> way_ends(const std::vector<PackedLocation>& ring, const std::vector<WayRun>& runs) {
    std::set<uint64_t> out;
    uint32_t at = 0;
    for (size_t k = 0; k < runs.size(); k++) {
        if (runs.size() > 1 && runs[k].way != runs[(k + runs.size() - 1) % runs.size()].way) out.insert(vertex_key(ring[at]));
        at += runs[k].edges;
    }
    return out;
}

// Each stretch's way id and its edges, whichever vertex the ring starts at.
std::map<int64_t, uint32_t> stretches(const std::vector<WayRun>& runs) {
    std::map<int64_t, uint32_t> out;
    for (const WayRun& r : runs) out[int64_t(r.way)] += r.edges;  // by value: WayRun packs it to 4 bytes
    return out;
}

}  // namespace

TEST(marine_areas_read_as_marine_import_writes_them) {
    ScratchDir dir("marine");
    const std::vector<marine::Area> areas = marine::read_areas(write(dir, "two.bin", bytes_of_hex(kTwoAreasHex)));
    REQUIRE(areas.size() == 2);
    CHECK_EQ(areas[0].id, 1902u);
    CHECK_EQ(int(areas[0].kind), int(marine::kOcean));
    CHECK(areas[0].name == "North Atlantic Ocean");
    REQUIRE(areas[0].polygons.size() == 2);
    REQUIRE(areas[0].polygons[1].size() == 2);
    // Each ring closed, its first vertex again at its end.
    const std::vector<PackedLocation>& hole = areas[0].polygons[1][1];
    REQUIRE(hole.size() == 4);
    CHECK(hole[0] == pt(-5000000, -5000000));
    CHECK(hole[2] == pt(0, 5000000));
    CHECK(hole[3] == hole[0]);
    CHECK_EQ(areas[1].id, 2350u);
    CHECK_EQ(int(areas[1].kind), int(marine::kSea));
    CHECK(areas[1].name == "North Sea");
    REQUIRE(areas[1].polygons.size() == 1);
    REQUIRE(areas[1].polygons[0].size() == 1);
    REQUIRE(areas[1].polygons[0][0].size() == 4);
    CHECK(areas[1].polygons[0][0][1] == pt(530000000, 40000000));
    marine::check_file(dir.path() + "/two.bin");
}

TEST(a_marine_areas_file_of_another_shape_is_refused) {
    ScratchDir dir("marine");
    const std::string good = bytes_of_hex(kTwoAreasHex);
    std::string magic = good;
    magic[2] = 'X';
    CHECK(error_of(write(dir, "magic.bin", magic)).find("not a marine areas file") != std::string::npos);
    std::string version = good;
    version[8] = 2;
    CHECK(error_of(write(dir, "version.bin", version)).find("not of version 1") != std::string::npos);
    CHECK(error_of(write(dir, "short.bin", good.substr(0, good.size() - 3))).find("cut short") != std::string::npos);
    CHECK(error_of(write(dir, "long.bin", good + "x")).find("1 bytes past the last object") != std::string::npos);
    // The second area's id (at byte 151) made smaller than the first's.
    std::string order = good;
    order[151] = 1;
    order[152] = 0;
    CHECK(error_of(write(dir, "order.bin", order)).find("out of order") != std::string::npos);
    // The first area's first ring (its count at byte 55) said to be of 2 vertices.
    std::string ring = good;
    ring[55] = 2;
    CHECK(error_of(write(dir, "ring.bin", ring)).find("a ring of 2 vertices") != std::string::npos);
    // A latitude past 90 degrees.
    std::string off = good;
    off[62] = '\x7f';
    CHECK(error_of(write(dir, "off.bin", off)).find("off the world") != std::string::npos);
    CHECK(error_of(dir.path() + "/none.bin").find("can't open") != std::string::npos);
    bool refused = false;
    try {
        marine::check_file(dir.path() + "/magic.bin");
    } catch (const std::runtime_error&) {
        refused = true;
    }
    CHECK(refused);
}

TEST(a_marine_count_past_what_the_rest_of_the_file_can_hold_is_cut_short_where_it_stands) {
    ScratchDir dir("marine");
    const std::string good = bytes_of_hex(kTwoAreasHex);
    // Each count set to 1000, more than the bytes after it hold: the object
    // count (at byte 12), the first area's polygon count (at 47) and its
    // first polygon's ring count (at 51). Refused as it is read, before
    // anything is made of it.
    for (const auto& [at, after] : std::vector<std::pair<size_t, size_t>>{{12, 16}, {47, 51}, {51, 55}}) {
        std::string count = good;
        count[at] = '\xe8';
        count[at + 1] = '\x03';
        count[at + 2] = count[at + 3] = 0;
        CHECK(error_of(write(dir, "count.bin", count)).find("cut short (byte " + std::to_string(after) + ")") !=
              std::string::npos);
    }
}

TEST(a_marine_ring_is_cut_into_stretches_by_its_own_vertices) {
    const std::vector<PackedLocation> ring = round_ring(-400000000, 1700000000, 5000);
    const std::vector<WayRun> runs = marine::ring_runs(ring);
    // Every edge once, in runs of negative ways, no two in a row alike.
    uint64_t edges = 0;
    for (size_t k = 0; k < runs.size(); k++) {
        edges += runs[k].edges;
        CHECK(runs[k].way < 0);
        CHECK(runs[k].edges > 0);
        if (k > 0) CHECK(runs[k].way != runs[k - 1].way);
    }
    CHECK_EQ(edges, uint64_t(5000));
    // About one vertex in kMarineCutEvery starts a stretch, and a stretch
    // starts at each of them.
    std::set<uint64_t> starts;
    for (size_t i = 0; i + 1 < ring.size(); i++)
        if (marine::starts_stretch(ring[i])) starts.insert(vertex_key(ring[i]));
    REQUIRE(starts.size() > 1);
    CHECK(starts.size() >= 5000 / marine::kMarineCutEvery / 2 && starts.size() <= 5000 / marine::kMarineCutEvery * 2);
    const std::set<uint64_t> ends = way_ends(ring, runs);
    CHECK(ends == starts);

    // The same ring from another vertex, and the other way round: the same
    // cuts and the same stretches.
    std::vector<PackedLocation> turned(ring.begin() + 1234, ring.end() - 1);
    turned.insert(turned.end(), ring.begin(), ring.begin() + 1234);
    turned.push_back(turned.front());
    std::vector<PackedLocation> back(ring.rbegin(), ring.rend());
    for (const auto& other : {turned, back}) {
        const std::vector<WayRun> r = marine::ring_runs(other);
        CHECK(way_ends(other, r) == ends);
        CHECK(stretches(r) == stretches(runs));
    }
}

TEST(a_marine_ring_no_vertex_cuts_is_one_stretch) {
    // A ring of vertices none of which starts a stretch.
    std::vector<PackedLocation> ring;
    for (PackedLocation v : {pt(100000000, 50000000), pt(100000000, 50010000), pt(100010000, 50010000),
                             pt(100010000, 50000000)}) {
        while (marine::starts_stretch(v)) v.lat_e7++;
        ring.push_back(v);
    }
    ring.push_back(ring.front());
    const std::vector<WayRun> runs = marine::ring_runs(ring);
    REQUIRE(runs.size() == 1);
    CHECK_EQ(runs[0].edges, 4u);
    CHECK(runs[0].way < 0);
    // The same way from any vertex, either way round.
    const std::vector<PackedLocation> turned = {ring[2], ring[3], ring[0], ring[1], ring[2]};
    const std::vector<PackedLocation> back(ring.rbegin(), ring.rend());
    CHECK_EQ(marine::ring_runs(turned)[0].way, runs[0].way);
    CHECK_EQ(marine::ring_runs(back)[0].way, runs[0].way);
}
