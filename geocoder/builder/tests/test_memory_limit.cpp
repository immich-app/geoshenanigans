// Memory limit discovery (memory_limit.h): MemTotal capped by every cgroup
// limit over the process, and the budget the write phase schedules against.
#include "memory_limit.h"

#include <atomic>
#include <chrono>
#include <map>
#include <thread>

#include "test_framework.h"

namespace {

constexpr uint64_t kGiB = uint64_t(1) << 30;

// A ReadFile over a fixed set of files.
ReadFile files_of(std::map<std::string, std::string> files) {
    return [files](const std::string& path) -> std::optional<std::string> {
        auto it = files.find(path);
        if (it == files.end()) return std::nullopt;
        return it->second;
    };
}

}  // namespace

TEST(meminfo_total_bytes_reads_mem_total_in_kib) {
    CHECK_EQ(meminfo_total_bytes("MemFree: 5 kB\nMemTotal:       263813012 kB\nSwapTotal: 1 kB\n"),
             uint64_t(263813012) * 1024);
    CHECK_EQ(meminfo_total_bytes("MemFree: 5 kB\n"), uint64_t(0));
}

TEST(cgroup_limit_files_walks_the_cgroup_and_its_ancestors) {
    CHECK((cgroup_limit_files("0::/system.slice/run-1.scope\n") == std::vector<std::string>{
        "/sys/fs/cgroup/system.slice/run-1.scope/memory.max",
        "/sys/fs/cgroup/system.slice/memory.max",
        "/sys/fs/cgroup/memory.max"}));
    CHECK((cgroup_limit_files("0::/\n") == std::vector<std::string>{"/sys/fs/cgroup/memory.max"}));
}

TEST(cgroup_limit_files_takes_v1_only_from_the_memory_hierarchy) {
    CHECK((cgroup_limit_files("12:cpu,cpuacct:/a\n4:memory:/docker/x\n1:name=systemd:/b\n") ==
           std::vector<std::string>{"/sys/fs/cgroup/memory/docker/x/memory.limit_in_bytes",
                                    "/sys/fs/cgroup/memory/docker/memory.limit_in_bytes",
                                    "/sys/fs/cgroup/memory/memory.limit_in_bytes"}));
    CHECK(cgroup_limit_files("3:memoryx:/a\n").empty());
}

TEST(cgroup_limit_bytes_reads_a_limit_and_skips_no_limit) {
    CHECK(cgroup_limit_bytes("137438953472\n") == std::optional<uint64_t>(137438953472ull));
    CHECK(!cgroup_limit_bytes("max\n"));
    CHECK(!cgroup_limit_bytes("9223372036854771712\n"));
    CHECK(!cgroup_limit_bytes(""));
}

TEST(memory_limit_bytes_takes_the_tightest_limit) {
    const std::string meminfo = "MemTotal: 268435456 kB\n";  // 256 GiB
    CHECK_EQ(memory_limit_bytes(files_of({{"/proc/meminfo", meminfo}})), 256 * kGiB);
    CHECK_EQ(memory_limit_bytes(files_of({
        {"/proc/meminfo", meminfo},
        {"/proc/self/cgroup", "0::/a/b\n"},
        {"/sys/fs/cgroup/a/b/memory.max", "max\n"},
        {"/sys/fs/cgroup/a/memory.max", std::to_string(128 * kGiB) + "\n"},
    })), 128 * kGiB);
    // A cgroup limit past MemTotal doesn't raise it.
    CHECK_EQ(memory_limit_bytes(files_of({
        {"/proc/meminfo", meminfo},
        {"/proc/self/cgroup", "0::/a\n"},
        {"/sys/fs/cgroup/a/memory.max", std::to_string(512 * kGiB) + "\n"},
    })), 256 * kGiB);
}

TEST(memory_cap_bytes_takes_only_a_positive_whole_mib) {
    CHECK_EQ(memory_cap_bytes("131072"), uint64_t(131072) << 20);
    CHECK_EQ(memory_cap_bytes("1"), uint64_t(1) << 20);
    // Unset, empty, zero, junk, negative and overflowing caps are no cap.
    CHECK_EQ(memory_cap_bytes(nullptr), uint64_t(0));
    CHECK_EQ(memory_cap_bytes(""), uint64_t(0));
    CHECK_EQ(memory_cap_bytes("0"), uint64_t(0));
    CHECK_EQ(memory_cap_bytes("128G"), uint64_t(0));
    CHECK_EQ(memory_cap_bytes("-5"), uint64_t(0));
    CHECK_EQ(memory_cap_bytes("99999999999999999999999"), uint64_t(0));
}

TEST(memory_budget_runs_a_request_past_capacity_alone) {
    MemoryBudget budget(10);
    budget.acquire(25);
    CHECK_EQ(budget.held(), uint64_t(25));
    std::atomic<bool> second{false};
    std::thread t([&] {
        budget.acquire(1);
        second = true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    CHECK(!second.load());
    budget.release(25);
    t.join();
    CHECK(second.load());
    CHECK_EQ(budget.held(), uint64_t(1));
}

TEST(memory_budget_acquire_first_takes_the_first_request_that_fits) {
    MemoryBudget budget(10);
    budget.acquire(6);
    CHECK_EQ(budget.acquire_first({8, 5, 3}), size_t(2));
    CHECK_EQ(budget.held(), uint64_t(9));
    budget.release(9);
    // Nothing held: the first request, past the budget or not.
    CHECK_EQ(budget.acquire_first({12, 1}), size_t(0));
    CHECK_EQ(budget.held(), uint64_t(12));
}

TEST(memory_budget_acquire_first_waits_until_one_fits) {
    MemoryBudget budget(10);
    budget.acquire(8);
    std::atomic<size_t> picked{SIZE_MAX};
    std::thread t([&] { picked = budget.acquire_first({7, 5}); });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    CHECK_EQ(picked.load(), size_t(SIZE_MAX));
    budget.release(4);
    t.join();
    CHECK_EQ(picked.load(), size_t(1));
    CHECK_EQ(budget.held(), uint64_t(9));
}

TEST(memory_budget_holds_requests_that_fit_together) {
    MemoryBudget budget(10);
    {
        BudgetLease a(budget, 4), b(budget, 6);
        CHECK_EQ(budget.held(), uint64_t(10));
    }
    CHECK_EQ(budget.held(), uint64_t(0));
}
