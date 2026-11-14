// The built tools run from a test: the dirs they read and write as maps of
// file name to bytes, and one command's words run with each word quoted, so
// no word reaches the shell as anything but itself (under the heap shim for a
// test of the tool's memory).
#pragma once

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "patch_format.h"
#include "poi_files.h"
#include "sanitizer_report.h"
#include "side_blocks.h"
#include "way_files.h"

namespace gctest {

// A build dir's files by name. Its POI files, way pairs, side files and
// string tiers are logical: poi_records.bin the 36-byte records,
// poi_vertices.bin the vertex stream they point into, street_ways.bin and
// interp_ways.bin the 12- and 24-byte records, street_nodes.bin and
// interp_nodes.bin their ways' nodes back to back, a side file
// (way_parents.bin, way_postcodes.bin, interp_postcodes.bin,
// addr_postcodes.bin) its u32 values, a strings_*.bin its pool.
using BuildDir = std::map<std::string, std::vector<char>>;

inline bool is_string_tier(const std::string& name) {
    for (const char* t : STRING_TIER_FILES)
        if (name == t) return true;
    return false;
}

// A family of files a fixture names in logical form and the builder writes
// encoded. records: the logical file a fixture with the family names.
// encode: the files the builder writes for the family of `files`, into out
// in place of its logical ones; it takes the family's own row, so families
// of one format share it.
struct WrittenFamily {
    const char* records;
    void (*encode)(const WrittenFamily& family, const BuildDir& files, BuildDir& out);
};

// The POI files as R2 and V2 (poi_files.h), a missing poi_vertices.bin an
// empty vertex stream.
inline void write_poi_family(const WrittenFamily& family, const BuildDir& files, BuildDir& out) {
    const std::vector<char>& records = files.at(family.records);
    if (records.size() % poi_files::LOGICAL_STRIDE) throw std::runtime_error("poi_records.bin is not whole records");
    auto v = files.find("poi_vertices.bin");
    static const std::vector<char> none;
    const std::vector<char>& verts = v == files.end() ? none : v->second;
    const poi_files::Files f =
        poi_files::encode(reinterpret_cast<const uint8_t*>(records.data()), records.size() / poi_files::LOGICAL_STRIDE,
                          reinterpret_cast<const uint8_t*>(verts.data()), verts.size());
    out["poi_records.bin"].assign(f.records.begin(), f.records.end());
    out["poi_vertices.bin"].assign(f.vertices.begin(), f.vertices.end());
}

// A side file as side blocks (side_blocks.h) of its u32 values.
inline void write_side_file(const WrittenFamily& family, const BuildDir& files, BuildDir& out) {
    const std::vector<char>& logical = files.at(family.records);
    if (logical.size() % 4) throw std::runtime_error(std::string(family.records) + " is not whole values");
    std::vector<uint32_t> values(logical.size() / 4);
    if (!values.empty()) memcpy(values.data(), logical.data(), logical.size());
    const std::vector<uint8_t> f = side_blocks::encode(family.records, values.data(), values.size());
    out[family.records].assign(f.begin(), f.end());
}

// A way pair, the family's ways file and its nodes file, as way files
// (way_files.h), a missing nodes file an empty node stream.
inline void write_way_pair(const WrittenFamily& family, const BuildDir& files, BuildDir& out) {
    const way_files::Kind& kind = family.records == std::string(way_files::STREETS.ways_file) ? way_files::STREETS : way_files::INTERPS;
    const std::vector<char>& ways = files.at(kind.ways_file);
    if (ways.size() % kind.stride) throw std::runtime_error(std::string(kind.ways_file) + " is not whole records");
    auto n = files.find(kind.nodes_file);
    static const std::vector<char> none;
    const std::vector<char>& nodes = n == files.end() ? none : n->second;
    std::vector<char>& rows = out[kind.ways_file];
    std::vector<char>& blocks = out[kind.nodes_file];
    rows.clear();
    blocks.clear();
    auto into = [](std::vector<char>& v) {
        return [&v](const uint8_t* p, size_t bytes) { v.insert(v.end(), p, p + bytes); };
    };
    way_files::Encoder enc(kind, into(rows), into(blocks));
    size_t at = 0;
    for (size_t w = 0; w < ways.size(); w += kind.stride) {
        const uint8_t* record = reinterpret_cast<const uint8_t*>(ways.data()) + w;
        const size_t bytes = way_files::NODE_BYTES * way_files::get16(record + way_files::NODE_COUNT_AT);
        if (bytes > nodes.size() - at) throw std::runtime_error(std::string(kind.nodes_file) + " ends before its ways' nodes do");
        enc.add(record, reinterpret_cast<const uint8_t*>(nodes.data()) + at);
        at += bytes;
    }
    if (at != nodes.size()) throw std::runtime_error(std::string(kind.nodes_file) + " holds nodes past its ways'");
    enc.finish();
    const auto ways_footer = enc.ways_footer();
    const auto nodes_footer = enc.nodes_footer();
    rows.insert(rows.end(), ways_footer.begin(), ways_footer.end());
    blocks.insert(blocks.end(), nodes_footer.begin(), nodes_footer.end());
}

inline const std::vector<WrittenFamily>& written_families() {
    static const std::vector<WrittenFamily> f = {
        {"poi_records.bin", write_poi_family},
        {"street_ways.bin", write_way_pair},
        {"interp_ways.bin", write_way_pair},
        {"way_parents.bin", write_side_file},
        {"way_postcodes.bin", write_side_file},
        {"interp_postcodes.bin", write_side_file},
        {"addr_postcodes.bin", write_side_file},
    };
    return f;
}

// The dir as the builder writes it: its string tiers as FCC2
// (string_files.h), and each family of written_families it holds encoded.
inline BuildDir as_written(const BuildDir& files) {
    BuildDir out = files;
    for (auto& [name, bytes] : out) {
        if (!is_string_tier(name)) continue;
        const std::vector<uint8_t> f = string_files::encode(bytes.data(), bytes.size());
        bytes.assign(f.begin(), f.end());
    }
    for (const WrittenFamily& f : written_families())
        if (files.count(f.records)) f.encode(f, files, out);
    return out;
}

inline bool write_dir(const std::string& path, const BuildDir& files) {
    std::filesystem::create_directories(path);
    for (const auto& [name, bytes] : as_written(files))
        if (!write_file(path + "/" + name, bytes)) return false;
    return true;
}

// A dir's files by name.
inline BuildDir read_dir(const std::string& path) {
    BuildDir files;
    for (const auto& e : std::filesystem::directory_iterator(path)) files[e.path().filename().string()] = read_file(e.path().string());
    return files;
}

// s as one shell word.
inline std::string quoted(const std::string& s) {
    std::string out = "'";
    for (char c : s) out += c == '\'' ? std::string("'\\''") : std::string(1, c);
    return out + "'";
}

// Runs the words (in dir cwd when given) with their output appended to log;
// true on exit status 0. A run a sanitizer report ended throws
// SanitizerReported with the log, whatever the test expected of it. Stdin is
// /dev/null: after a usage error zstd waits for Enter at a console.
inline bool run(const std::vector<std::string>& words, const std::string& log, const std::string& cwd = "") {
    std::string cmd = cwd.empty() ? "" : "cd " + quoted(cwd) + " && ";
    for (const auto& w : words) cmd += quoted(w) + " ";
    const int status = std::system((cmd + "< /dev/null >> " + quoted(log) + " 2>&1").c_str());
    if (ended_by_sanitizer_report(status)) {
        const std::vector<char> text = read_file(log);
        throw SanitizerReported(words[0] + " ended with a sanitizer report:\n" + std::string(text.begin(), text.end()));
    }
    return status == 0;
}

// The heap shim (tests/heap_peak_shim.c) this build made, or nullptr: none
// is built under the sanitizers, whose allocator it can't stand in for.
inline const char* heap_peak_shim() {
#ifdef GEOCODER_HEAP_PEAK_SHIM
    return GEOCODER_HEAP_PEAK_SHIM;
#else
    return nullptr;
#endif
}

// A run under the heap shim: exit status 0, and the peak in bytes of the
// heap plus anonymous mappings of the process run (not of its children).
// None without a shim, or when the process left without exit().
struct HeapRun {
    bool ok;
    std::optional<uint64_t> heap_peak;
};

// Runs the words as run() does, the first word's process under the heap
// shim when the build has one.
inline HeapRun run_with_heap_peak(const std::vector<std::string>& words, const std::string& log) {
    if (!heap_peak_shim()) return {run(words, log), std::nullopt};
    const std::string peak_out = log + ".heap-peak";
    std::filesystem::remove(peak_out);
    std::vector<std::string> shimmed = {"env", std::string("LD_PRELOAD=") + heap_peak_shim(), "GEOCODER_HEAP_PEAK_OUT=" + peak_out};
    shimmed.insert(shimmed.end(), words.begin(), words.end());
    HeapRun r{run(shimmed, log), std::nullopt};
    const std::vector<char> text = read_file(peak_out);
    const std::string line(text.begin(), text.end());
    // One line of digits: a second process under the shim would add a line.
    if (line.size() > 1 && line.find_first_not_of("0123456789") == line.size() - 1 && line.back() == '\n')
        r.heap_peak = std::stoull(line);
    return r;
}

}  // namespace gctest
