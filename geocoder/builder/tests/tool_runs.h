// The built tools run from a test: the dirs they read and write as maps of
// file name to bytes, and one command's words run with each word quoted, so
// no word reaches the shell as anything but itself (under the heap shim for a
// test of the tool's memory).
#pragma once

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "patch_format.h"
#include "poi_files.h"
#include "sanitizer_report.h"

namespace gctest {

// A build dir's files by name. Its POI files and string tiers are logical:
// poi_records.bin the 36-byte records, poi_vertices.bin the vertex stream they
// point into, a strings_*.bin its pool.
using BuildDir = std::map<std::string, std::vector<char>>;

inline bool is_string_tier(const std::string& name) {
    for (const char* t : STRING_TIER_FILES)
        if (name == t) return true;
    return false;
}

// The dir as the builder writes it: its string tiers as FCC2
// (string_files.h), and the POI files of a dir with poi_records.bin as R2 and
// V2 (poi_files.h), a missing poi_vertices.bin an empty vertex stream.
inline BuildDir as_written(const BuildDir& files) {
    BuildDir out = files;
    for (auto& [name, bytes] : out) {
        if (!is_string_tier(name)) continue;
        const std::vector<uint8_t> f = string_files::encode(bytes.data(), bytes.size());
        bytes.assign(f.begin(), f.end());
    }
    auto r = files.find("poi_records.bin");
    if (r == files.end()) return out;
    if (r->second.size() % poi_files::LOGICAL_STRIDE) throw std::runtime_error("poi_records.bin is not whole records");
    auto v = files.find("poi_vertices.bin");
    static const std::vector<char> none;
    const std::vector<char>& verts = v == files.end() ? none : v->second;
    const poi_files::Files f =
        poi_files::encode(reinterpret_cast<const uint8_t*>(r->second.data()), r->second.size() / poi_files::LOGICAL_STRIDE,
                          reinterpret_cast<const uint8_t*>(verts.data()), verts.size());
    out["poi_records.bin"].assign(f.records.begin(), f.records.end());
    out["poi_vertices.bin"].assign(f.vertices.begin(), f.vertices.end());
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
