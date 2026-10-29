// The built tools run from a test: the dirs they read and write as maps of
// file name to bytes, and one command's words run with each word quoted, so
// no word reaches the shell as anything but itself.
#pragma once

#include <cstdlib>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include "patch_format.h"
#include "sanitizer_report.h"

namespace gctest {

// A build dir's files by name.
using BuildDir = std::map<std::string, std::vector<char>>;

inline bool write_dir(const std::string& path, const BuildDir& files) {
    std::filesystem::create_directories(path);
    for (const auto& [name, bytes] : files)
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

}  // namespace gctest
