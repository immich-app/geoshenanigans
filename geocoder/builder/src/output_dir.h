#pragma once

#include <filesystem>
#include <stdexcept>
#include <string>
#include <system_error>

// A build writes only the files its flags make, but everything under its output dir is published
// and diffed as that build's: a file an earlier build left there (one this build no longer writes,
// or an optional one its flags skip) would ship as today's. So build-index starts only in a dir
// that is absent (it makes it) or empty.
inline void require_new_or_empty_output_dir(const std::string& dir) {
    namespace fs = std::filesystem;
    std::error_code ec;
    (void)fs::symlink_status(dir, ec);
    if (ec == std::errc::no_such_file_or_directory) return;
    std::string refusal;
    if (ec) refusal = "unreadable (" + ec.message() + ")";
    else if (!fs::is_directory(dir, ec)) refusal = "is not a directory";
    else if (!fs::is_empty(dir, ec)) refusal = ec ? "unreadable (" + ec.message() + ")" : "is not empty";
    if (!refusal.empty()) throw std::runtime_error("Output dir " + dir + " " + refusal + ": build into a new or empty dir");
}
