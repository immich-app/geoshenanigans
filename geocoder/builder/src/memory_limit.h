// The memory a build may use, and the budget its write phase schedules
// against. No S2 here, so the unit tests can check the parsing.
#pragma once

#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

// MemTotal from /proc/meminfo, in bytes; 0 if missing.
inline uint64_t meminfo_total_bytes(const std::string& meminfo) {
    std::istringstream in(meminfo);
    for (std::string line; std::getline(in, line);) {
        if (line.rfind("MemTotal:", 0) != 0) continue;

        std::istringstream value(line.substr(9));
        uint64_t kib = 0;
        if (value >> kib) return kib * 1024;
    }
    return 0;
}

// The memory limit files over this process, from /proc/self/cgroup: v2
// memory.max for the unified ("0::") hierarchy, v1 memory.limit_in_bytes for
// a hierarchy that has the memory controller, each for the process's cgroup
// and then every ancestor (a parent's limit caps its children too).
inline std::vector<std::string> cgroup_limit_files(const std::string& proc_self_cgroup) {
    std::vector<std::string> files;
    std::istringstream in(proc_self_cgroup);
    for (std::string line; std::getline(in, line);) {
        size_t first = line.find(':');
        size_t second = first == std::string::npos ? first : line.find(':', first + 1);
        if (second == std::string::npos) continue;

        std::string controllers = line.substr(first + 1, second - first - 1);
        std::string path = line.substr(second + 1);
        std::string root, file;
        if (controllers.empty()) {
            root = "/sys/fs/cgroup";
            file = "/memory.max";
        } else if (("," + controllers + ",").find(",memory,") != std::string::npos) {
            root = "/sys/fs/cgroup/memory";
            file = "/memory.limit_in_bytes";
        } else {
            continue;
        }
        while (!path.empty() && path.back() == '/') path.pop_back();
        for (;;) {
            files.push_back(root + path + file);
            if (path.empty()) break;
            path.erase(path.find_last_of('/'));
        }
    }
    return files;
}

// A cgroup limit file's value in bytes; none for no limit: v2 writes "max",
// v1 a page-rounded value near 2^63.
inline std::optional<uint64_t> cgroup_limit_bytes(const std::string& contents) {
    constexpr uint64_t kNoLimitFrom = uint64_t(1) << 62;
    const char* text = contents.c_str();
    char* end = nullptr;
    unsigned long long value = std::strtoull(text, &end, 10);
    if (end == text || value >= kNoLimitFrom) return std::nullopt;

    return value;
}

// A file's contents, none when it can't be read.
using ReadFile = std::function<std::optional<std::string>(const std::string&)>;

// The tightest of MemTotal and every cgroup memory limit over this process,
// in bytes (0 when none is known).
inline uint64_t memory_limit_bytes(const ReadFile& read) {
    uint64_t limit = meminfo_total_bytes(read("/proc/meminfo").value_or(""));
    std::optional<std::string> cgroups = read("/proc/self/cgroup");
    if (!cgroups) return limit;

    for (const std::string& file : cgroup_limit_files(*cgroups)) {
        std::optional<std::string> contents = read(file);
        std::optional<uint64_t> value = contents ? cgroup_limit_bytes(*contents) : std::nullopt;
        if (value && (limit == 0 || *value < limit)) limit = *value;
    }
    return limit;
}

inline std::optional<std::string> read_text_file(const std::string& path) {
    std::ifstream f(path);
    if (!f) return std::nullopt;

    return std::string(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

// A GC_MEMORY_LIMIT_MIB value in bytes; 0 unless it is a positive whole
// number of MiB, so an unusable cap never lifts the detected limit.
inline uint64_t memory_cap_bytes(const char* mib) {
    constexpr uint64_t kMaxMib = uint64_t(1) << 40;  // 1 EiB: past any real host
    if (!mib || !*mib) return 0;
    uint64_t v = 0;
    for (const char* c = mib; *c; c++) {
        if (*c < '0' || *c > '9') return 0;
        v = v * 10 + uint64_t(*c - '0');
        if (v > kMaxMib) return 0;
    }
    return v << 20;
}

// memory_limit_bytes for this process, capped further by GC_MEMORY_LIMIT_MIB
// when set (room kept for other work on the host).
inline uint64_t memory_limit_bytes() {
    uint64_t limit = memory_limit_bytes(read_text_file);
    const char* env = std::getenv("GC_MEMORY_LIMIT_MIB");
    if (!env) return limit;

    uint64_t cap = memory_cap_bytes(env);
    if (cap == 0) {
        std::cerr << "Ignoring GC_MEMORY_LIMIT_MIB=" << env << ": not a positive number of MiB" << std::endl;
        return limit;
    }
    return limit == 0 || cap < limit ? cap : limit;
}

// Bytes the write phase may hold beyond what it already uses, handed to the
// regions and stages that run at once. acquire blocks until the request fits
// beside what is held, or nothing is held: a request past the whole budget
// then runs alone.
class MemoryBudget {
public:
    explicit MemoryBudget(uint64_t capacity) : capacity_(capacity) {}

    uint64_t capacity() const { return capacity_; }

    void acquire(uint64_t bytes) {
        std::unique_lock<std::mutex> lock(mtx_);
        freed_.wait(lock, [&] { return held_ == 0 || held_ + bytes <= capacity_; });
        held_ += bytes;
    }

    // Takes the first of `requests` that fits, waiting for one to, or the
    // first when nothing is held; returns its index. requests is not empty.
    size_t acquire_first(const std::vector<uint64_t>& requests) {
        std::unique_lock<std::mutex> lock(mtx_);
        size_t pick = 0;
        freed_.wait(lock, [&] {
            for (pick = 0; pick < requests.size(); pick++)
                if (held_ == 0 || held_ + requests[pick] <= capacity_) return true;
            return false;
        });
        held_ += requests[pick];
        return pick;
    }

    void release(uint64_t bytes) {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            held_ -= bytes;
        }
        freed_.notify_all();
    }

    uint64_t held() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return held_;
    }

private:
    const uint64_t capacity_;
    uint64_t held_ = 0;
    mutable std::mutex mtx_;
    std::condition_variable freed_;
};

// Holds bytes of a MemoryBudget for its lifetime (already acquired ones with
// std::adopt_lock).
class BudgetLease {
public:
    BudgetLease(MemoryBudget& budget, uint64_t bytes) : budget_(budget), bytes_(bytes) { budget_.acquire(bytes_); }
    BudgetLease(MemoryBudget& budget, uint64_t bytes, std::adopt_lock_t) : budget_(budget), bytes_(bytes) {}
    ~BudgetLease() { budget_.release(bytes_); }
    BudgetLease(const BudgetLease&) = delete;
    BudgetLease& operator=(const BudgetLease&) = delete;

private:
    MemoryBudget& budget_;
    uint64_t bytes_;
};
