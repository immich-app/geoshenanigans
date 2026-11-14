// The record files' writers (record_files_io.h): a side file and a pair of
// way files are the bytes their serial encoders give, however many threads
// encode them and wherever their slices are cut; the threads encode no more
// than the slice window ahead of the write; and a side array that is not a
// value a record, nodes that are not the ways' and a pair the way files
// can't hold are refused.
#include "record_files_io.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <random>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "coord_keys.h"
#include "scratch_dir.h"
#include "side_blocks.h"
#include "test_framework.h"
#include "types.h"
#include "way_files.h"

using gctest::refusal_of;

namespace {

// NO_DATA, which a side file holds as it holds any value.
constexpr uint32_t NONE = 0xFFFFFFFFu;
const char* const NAME = "way_postcodes.bin";
const char* const PARENT = "street_ways.bin";

// n values in stretches of three kinds, so their blocks are of both modes
// and many widths: a postcode column (runs of one id, NO_DATA among them),
// parents (ids near each other) and any 32 bits.
std::vector<uint32_t> side_values(std::mt19937_64& rng, size_t n) {
    std::vector<uint32_t> out;
    uint32_t run = uint32_t(rng() % 100000);
    int kind = 0;
    // One draw a statement: the same values whatever order a compiler evaluates arguments in.
    for (size_t i = 0; i < n; i++) {
        const uint64_t r = rng();
        if (i % 500 == 0) kind = int((r >> 40) % 3);
        if (kind == 0) {
            if (r % 9 == 0) run = uint32_t((r >> 20) % 100000);
            out.push_back((r >> 8) % 6 == 0 ? NONE : run);
        } else if (kind == 1) {
            run += uint32_t(r % 5);
            out.push_back((r >> 8) % 40 == 0 ? NONE : run - uint32_t((r >> 16) % 3));
        } else {
            out.push_back(uint32_t(r));
        }
    }
    return out;
}

std::vector<uint8_t> serial(const std::vector<uint32_t>& values) {
    return side_blocks::encode(NAME, values.data(), values.size());
}

// What the side writer puts for the values.
std::vector<uint8_t> written(const std::vector<uint32_t>& values, unsigned threads, size_t slice_blocks) {
    std::vector<uint8_t> out;
    write_side_blocks(NAME, values, [&](const uint8_t* p, size_t n) { out.insert(out.end(), p, p + n); }, threads,
                      slice_blocks);
    return out;
}

std::vector<uint8_t> file_bytes(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    return std::vector<uint8_t>(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

// Notes how many slices ahead of those written a slice being read is.
void note_ahead(size_t slice, const std::atomic<size_t>& written, std::atomic<size_t>& furthest) {
    const size_t done = written.load();
    const size_t ahead = slice > done ? slice - done : 0;
    size_t seen = furthest.load();
    while (ahead > seen && !furthest.compare_exchange_weak(seen, ahead)) {}
}

// A side array that notes, as each value is read, how many slices ahead of
// those written its slice is.
struct Watched {
    const std::vector<uint32_t>& values;
    size_t slice_values;
    const std::atomic<size_t>& written;
    std::atomic<size_t>& furthest;

    size_t size() const { return values.size(); }
    uint32_t operator[](size_t i) const {
        note_ahead(i / slice_values, written, furthest);
        return values[i];
    }
};

// The nodes of ways of one node count, which note the same as each way's
// first node is read.
struct WatchedNodes {
    const std::vector<NodeCoord>& nodes;
    size_t slice_nodes;
    const std::atomic<size_t>& written;
    std::atomic<size_t>& furthest;

    size_t size() const { return nodes.size(); }
    const NodeCoord& operator[](size_t i) const {
        note_ahead(i / slice_nodes, written, furthest);
        return nodes[i];
    }
};

// A logical pair of way files: the ways' records, WayHeaders or InterpWays,
// and their nodes back to back.
template <class Record>
struct Ways {
    std::vector<Record> ways;
    std::vector<NodeCoord> nodes;
};

// The rows file and the nodes file.
struct WayFiles {
    std::vector<uint8_t> rows, nodes;
    bool operator==(const WayFiles& o) const { return rows == o.rows && nodes == o.nodes; }
};

auto into(std::vector<uint8_t>& v) {
    return [&v](const uint8_t* p, size_t n) { v.insert(v.end(), p, p + n); };
}

float of_bits(uint32_t bits) {
    float f;
    std::memcpy(&f, &bits, 4);
    return f;
}

// n ways, each record's node_offset the nodes before it: every eighth
// without nodes, some of one node and of two, the rest of up to `longest`;
// walks over coordinates of seven decimals, which take the hybrid key, and
// any bit patterns, which take the order key; an interpolation's numbers
// either way round.
template <class Record>
Ways<Record> random_ways(std::mt19937_64& rng, size_t n, size_t longest = 40) {
    Ways<Record> out;
    // One draw a statement: the same ways whatever order a compiler evaluates operands in.
    for (size_t i = 0; i < n; i++) {
        const uint64_t pick = rng() % 16;
        const size_t more = 3 + size_t(rng() % (longest - 2));
        const size_t count = pick < 2 ? 0 : pick == 2 ? 1 : pick == 3 ? 2 : more;
        const bool any_bits = rng() % 6 == 0;
        int64_t lat = int64_t(rng() % 1700000001) - 850000000;
        int64_t lng = int64_t(rng() % 3600000001) - 1800000000;
        Record w{};
        w.node_offset = count ? uint32_t(out.nodes.size()) : 0;
        w.node_count = uint16_t(count);
        const uint32_t name = uint32_t(rng() >> 40);
        if constexpr (std::is_same_v<Record, InterpWay>) {
            w.street_id = name;
            w.start_number = uint32_t(rng() % 3000);
            w.end_number = uint32_t(rng() % 3000);
            w.interpolation = uint8_t(rng() % 4);
        } else {
            w.name_id = count ? name : NO_DATA;
        }
        for (size_t j = 0; j < count; j++) {
            if (any_bits) {
                const uint32_t l = uint32_t(rng());
                const uint32_t g = uint32_t(rng());
                out.nodes.push_back({of_bits(l), of_bits(g)});
            } else {
                out.nodes.push_back({float(double(lat) / 1e7), float(double(lng) / 1e7)});
                lat += int64_t(rng() % 4001) - 2000;
                lng += int64_t(rng() % 4001) - 2000;
            }
        }
        out.ways.push_back(w);
    }
    return out;
}

const uint8_t* bytes_of(const NodeCoord* nodes) { return reinterpret_cast<const uint8_t*>(nodes); }

// The pair through one encoder, a way at a time, footers and all.
template <class Record>
WayFiles serial(const Ways<Record>& p) {
    WayFiles f;
    way_files::Encoder enc(way_kind(Record{}), into(f.rows), into(f.nodes));
    size_t node = 0;
    for (const Record& w : p.ways) {
        enc.add(reinterpret_cast<const uint8_t*>(&w), bytes_of(p.nodes.data() + node));
        node += w.node_count;
    }
    enc.finish();
    const auto ways_footer = enc.ways_footer();
    const auto nodes_footer = enc.nodes_footer();
    f.rows.insert(f.rows.end(), ways_footer.begin(), ways_footer.end());
    f.nodes.insert(f.nodes.end(), nodes_footer.begin(), nodes_footer.end());
    return f;
}

// What the way writer puts for the pair.
template <class Record>
WayFiles written(const Ways<Record>& p, unsigned threads, size_t slice_groups) {
    WayFiles f;
    write_way_pair(p.ways, p.nodes, into(f.rows), into(f.nodes), threads, slice_groups);
    return f;
}

// The pair's files as write_way_files leaves them in a dir.
template <class Record>
WayFiles files_of(const std::string& dir) {
    const way_files::Kind& kind = way_kind(Record{});
    return {file_bytes(dir + "/" + kind.ways_file), file_bytes(dir + "/" + kind.nodes_file)};
}

// The two footers of a pair without ways.
template <class Record>
WayFiles no_ways() {
    const way_files::Kind& kind = way_kind(Record{});
    WayFiles f;
    f.rows = {0, 0, 0, 0};
    f.rows.insert(f.rows.end(), kind.ways_magic, kind.ways_magic + 4);
    f.nodes.assign(12, 0);
    f.nodes.insert(f.nodes.end(), kind.nodes_magic, kind.nodes_magic + 4);
    return f;
}

template <class Record>
void check_the_way_writer_puts_the_serial_encoders_files(uint64_t seed) {
    std::mt19937_64 rng(seed);
    // Around a group's edge, a last group that is short, and past a slice of
    // the builder's size.
    for (size_t n : {size_t(0), size_t(1), size_t(15), size_t(16), size_t(17), size_t(1000), size_t(70001)}) {
        const Ways<Record> p = random_ways<Record>(rng, n, n > 1000 ? 6 : 40);
        const WayFiles want = serial(p);
        for (unsigned threads : {1u, 2u, 7u})
            for (size_t slice_groups : {size_t(1), size_t(3), kWaySliceGroups})
                CHECK(written(p, threads, slice_groups) == want);
    }
    CHECK(written(Ways<Record>{}, 2, 1) == no_ways<Record>());
}

template <class Record>
void check_way_files_are_the_serial_encoders_files(uint64_t seed) {
    const way_files::Kind& kind = way_kind(Record{});
    const ScratchDir dir(std::string("way-files-") + (kind.interp ? "interps" : "streets"));
    std::mt19937_64 rng(seed);
    // Past a slice.
    const Ways<Record> p = random_ways<Record>(rng, 150000, 6);
    write_way_files(dir.path(), p.ways, p.nodes);
    CHECK(files_of<Record>(dir.path()) == serial(p));
    // The files decode to the pair.
    const WayFiles f = files_of<Record>(dir.path());
    std::vector<uint8_t> ways, nodes;
    way_files::decode(kind, f.rows.data(), f.rows.size(), f.nodes.data(), f.nodes.size(),
                      [&](const uint8_t* r) { ways.insert(ways.end(), r, r + kind.stride); },
                      [&](const uint8_t* n, size_t bytes) { nodes.insert(nodes.end(), n, n + bytes); });
    REQUIRE(ways.size() == p.ways.size() * sizeof(Record) && nodes.size() == p.nodes.size() * sizeof(NodeCoord));
    CHECK(std::memcmp(ways.data(), p.ways.data(), ways.size()) == 0);
    CHECK(std::memcmp(nodes.data(), p.nodes.data(), nodes.size()) == 0);

    // No ways: the two footers, in place of the files of the pair before.
    write_way_files(dir.path(), std::vector<Record>{}, std::vector<NodeCoord>{});
    CHECK(files_of<Record>(dir.path()) == no_ways<Record>());
}

}  // namespace

TEST(the_side_writer_puts_the_serial_encoders_bytes_at_any_thread_count_and_slice_size) {
    std::mt19937_64 rng(23);
    // Around a block's edge, and past a slice of the builder's size.
    for (size_t n : {size_t(0), size_t(1), size_t(63), size_t(64), size_t(65), size_t(4000), size_t(70001)}) {
        const std::vector<uint32_t> values = side_values(rng, n);
        const std::vector<uint8_t> want = serial(values);
        for (unsigned threads : {1u, 2u, 7u})
            for (size_t slice_blocks : {size_t(1), size_t(3), kSideSliceBlocks})
                CHECK(written(values, threads, slice_blocks) == want);
    }
    CHECK(written({}, 2, 1) == (std::vector<uint8_t>{0, 0, 0, 0, 'S', 'D', 'B', '1'}));
}

TEST(a_side_file_is_the_serial_encoders_bytes) {
    const ScratchDir dir("side-file");
    std::mt19937_64 rng(29);
    const std::vector<uint32_t> values = side_values(rng, 150000);
    write_side_file(dir.path(), NAME, values, PARENT, values.size());
    CHECK(file_bytes(dir.path() + "/" + NAME) == serial(values));
}

TEST(a_side_array_that_is_not_a_value_a_record_is_refused_before_its_file_exists) {
    const ScratchDir dir("side-count");
    const std::string path = dir.path() + "/" + NAME;
    const std::vector<uint32_t> values(100, 7);
    for (uint64_t records : {uint64_t(99), uint64_t(101), uint64_t(0)}) {
        CHECK_EQ(refusal_of([&] { write_side_file(dir.path(), NAME, values, PARENT, records); }),
                 path + ": 100 values for the " + std::to_string(records) + " records of street_ways.bin");
        CHECK(!std::filesystem::exists(path));
    }
    CHECK_EQ(refusal_of([&] { write_side_file(dir.path(), NAME, {}, PARENT, 3); }),
             path + ": 0 values for the 3 records of street_ways.bin");
    CHECK(!std::filesystem::exists(path));

    // A value a record is written, and a dir that is not there is an error.
    write_side_file(dir.path(), NAME, values, PARENT, 100);
    CHECK(file_bytes(path) == serial(values));
    CHECK_EQ(refusal_of([&] { write_side_file(dir.path() + "/none", NAME, values, PARENT, 100); }),
             "failed to write " + dir.path() + "/none/" + NAME);
}

TEST(the_way_writer_puts_the_serial_encoders_files_at_any_thread_count_and_slice_size) {
    check_the_way_writer_puts_the_serial_encoders_files<WayHeader>(31);
    check_the_way_writer_puts_the_serial_encoders_files<InterpWay>(37);
}

TEST(way_files_are_the_serial_encoders_files_and_two_footers_without_ways) {
    check_way_files_are_the_serial_encoders_files<WayHeader>(41);
    check_way_files_are_the_serial_encoders_files<InterpWay>(43);
}

TEST(the_way_writer_refuses_what_the_serial_encoder_refuses_and_nodes_that_are_not_the_ways) {
    // 100 streets of two nodes, but for ways 40 and 98, which have none.
    Ways<WayHeader> good;
    for (uint32_t i = 0; i < 100; i++) {
        const uint16_t count = i == 40 || i == 98 ? 0 : 2;
        good.ways.push_back({count ? uint32_t(good.nodes.size()) : 0, count, 0, i});
        for (uint16_t j = 0; j < count; j++) good.nodes.push_back({47.0f + float(i) * 1e-4f, 8.0f + float(j) * 1e-4f});
    }
    auto refusal = [](const Ways<WayHeader>& p, unsigned threads, size_t slice_groups) {
        return refusal_of([&] { written(p, threads, slice_groups); });
    };
    auto refused_alike = [&](const Ways<WayHeader>& p, const std::string& want) {
        CHECK_EQ(refusal_of([&] { serial(p); }), want);
        for (unsigned threads : {1u, 4u})
            for (size_t slice_groups : {size_t(1), size_t(2), kWaySliceGroups}) CHECK_EQ(refusal(p, threads, slice_groups), want);
    };
    CHECK_EQ(refusal(good, 4, 1), "");

    // A node_offset that is not the nodes before the way: of a slice's first
    // way (way 32 at one group or two a slice), of a way inside a slice, and
    // of a way without nodes.
    {
        Ways<WayHeader> p = good;
        p.ways[32].node_offset = 65;
        refused_alike(p, "Malformed way files: street_ways.bin way 32 has a node_offset of 65, not the 64 nodes before it");
        p = good;
        p.ways[37].node_offset = 0;
        refused_alike(p, "Malformed way files: street_ways.bin way 37 has a node_offset of 0, not the 74 nodes before it");
        p = good;
        p.ways[98].node_offset = 194;
        refused_alike(p, "Malformed way files: street_ways.bin way 98 has a node_offset of 194, not 0");
    }
    // Padding that is set.
    {
        Ways<WayHeader> p = good;
        p.ways[50]._pad = 1;
        refused_alike(p, "Malformed way files: street_ways.bin way 50 has its padding set");
    }

    // Nodes that are not the ways': one more, one fewer, none.
    for (size_t nodes : {good.nodes.size() + 1, good.nodes.size() - 1, size_t(0)}) {
        Ways<WayHeader> p = good;
        p.nodes.resize(nodes);
        for (unsigned threads : {1u, 4u})
            for (size_t slice_groups : {size_t(1), kWaySliceGroups})
                CHECK_EQ(refusal(p, threads, slice_groups), "Malformed way files: street_nodes.bin has " + std::to_string(nodes) +
                                                                " nodes and the ways of street_ways.bin have 196");
    }
    Ways<InterpWay> interps;
    interps.ways.resize(3);
    interps.nodes.push_back({47.0f, 8.0f});
    CHECK_EQ(refusal_of([&] { written(interps, 2, 1); }),
             "Malformed way files: interp_nodes.bin has 1 nodes and the ways of interp_ways.bin have 0");
}

TEST(way_files_refused_or_not_written_name_their_dir) {
    const ScratchDir dir("way-files-refused");
    std::mt19937_64 rng(59);
    Ways<InterpWay> p = random_ways<InterpWay>(rng, 40);
    CHECK_EQ(refusal_of([&] { write_way_files(dir.path() + "/none", p.ways, p.nodes); }),
             "failed to write " + dir.path() + "/none/interp_ways.bin");
    p.ways[20]._pad4 = 1;
    CHECK_EQ(refusal_of([&] { write_way_files(dir.path(), p.ways, p.nodes); }),
             dir.path() + ": Malformed way files: interp_ways.bin way 20 has its padding set");
}

TEST(a_slice_writer_runs_no_more_than_its_window_ahead_of_the_consumer) {
    // Two slices a thread, and no more than kSliceWindow however many threads.
    CHECK_EQ(slice_window(1), size_t(2));
    CHECK_EQ(slice_window(15), size_t(30));
    CHECK_EQ(slice_window(16), kSliceWindow);
    CHECK_EQ(slice_window(64), kSliceWindow);
    CHECK_EQ(kSliceWindow, size_t(32));

    // 600 slices of a block each, one written every 100 microseconds: the
    // threads encode as far ahead of the write as their window lets them.
    const size_t slices = 600;
    const std::vector<uint32_t> values(slices * side_blocks::BLOCK_VALUES, 7);
    for (unsigned threads : {3u, 64u}) {
        std::atomic<size_t> slices_written{0}, furthest{0};
        const Watched watched{values, side_blocks::BLOCK_VALUES, slices_written, furthest};
        write_side_blocks(NAME, watched, [&](const uint8_t*, size_t) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            slices_written++;
        }, threads, 1);
        // Each slice's blocks, then the table and the footer.
        CHECK_EQ(slices_written.load(), slices + 2);
        CHECK(furthest.load() < slice_window(threads));
    }

    // The way writer the same: 600 slices of a group of ways of two nodes.
    const size_t way_nodes = 2, slice_nodes = way_files::GROUP_WAYS * way_nodes;
    Ways<WayHeader> p;
    for (size_t i = 0; i < slices * way_files::GROUP_WAYS; i++) {
        p.ways.push_back({uint32_t(p.nodes.size()), uint16_t(way_nodes), 0, uint32_t(i)});
        for (size_t j = 0; j < way_nodes; j++) p.nodes.push_back({47.0f + float(i) * 1e-4f, 8.0f + float(j) * 1e-4f});
    }
    for (unsigned threads : {3u, 64u}) {
        std::atomic<size_t> slices_written{0}, furthest{0};
        size_t block_puts = 0;
        const WatchedNodes watched{p.nodes, slice_nodes, slices_written, furthest};
        write_way_pair(p.ways, watched, [&](const uint8_t*, size_t) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            slices_written++;
        }, [&](const uint8_t*, size_t) { block_puts++; }, threads, 1);
        // Each slice's rows and blocks, then each file's footer.
        CHECK_EQ(slices_written.load(), slices + 1);
        CHECK_EQ(block_puts, slices + 1);
        CHECK(furthest.load() < slice_window(threads));
    }
}
