#pragma once

#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <system_error>

// A private scratch directory under $TMPDIR (temp_directory_path honours it;
// /tmp only when unset), removed with its contents when the object goes out
// of scope. CI points TMPDIR at disk: /tmp on the runner is a RAM disk that
// counts against the job's memory limit.
class ScratchDir {
public:
    explicit ScratchDir(const std::string& prefix) {
        std::string tmpl = (std::filesystem::temp_directory_path() / (prefix + "-XXXXXX")).string();
        if (!mkdtemp(tmpl.data())) throw std::runtime_error("Scratch dir not created: " + tmpl);
        path_ = tmpl;
    }
    ~ScratchDir() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }
    ScratchDir(const ScratchDir&) = delete;
    ScratchDir& operator=(const ScratchDir&) = delete;

    const std::string& path() const { return path_; }

private:
    std::string path_;
};
