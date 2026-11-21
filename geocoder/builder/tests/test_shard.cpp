// Unit tests for the share of the registry a geocoder-tests run takes and
// the runner that runs it (tests/test_shard.h), and for geocoder-tests' own
// answers to a shard it refuses and to --count, which the nightly's sharded
// run checks its shards against (tools/ci_build_tools.sh).
#include "test_shard.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include "child_process.h"
#include "scratch_dir.h"
#include "test_framework.h"

using gctest::refusal_of;

namespace {

// The exit status of this binary run again with `arg` (none when null) and
// GEOCODER_TEST_SHARD set to `shard` (unset when null), its stdout written
// to `out` and its stderr dropped.
int rerun_self(const char* arg, const char* shard, const std::string& out) {
    return gctest::exit_status_in_child([&] {
        if (shard) setenv("GEOCODER_TEST_SHARD", shard, 1);
        else unsetenv("GEOCODER_TEST_SHARD");
        int stdout_fd = open(out.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        int null_fd = open("/dev/null", O_WRONLY);
        if (stdout_fd < 0 || null_fd < 0) return 126;
        dup2(stdout_fd, 1);
        dup2(null_fd, 2);
        execl("/proc/self/exe", "geocoder-tests", arg, static_cast<char*>(nullptr));
        return 127;
    }, 120);
}

std::string read_text(const std::string& path) {
    std::ifstream f(path);
    std::stringstream s;
    s << f.rdbuf();
    return s.str();
}

}  // namespace

TEST(shard_runs_the_tests_whose_index_is_its_own_modulo_the_count) {
    gctest::Shard shard = gctest::parse_shard("3/16");
    CHECK_EQ(shard.index, size_t(3));
    CHECK_EQ(shard.count, size_t(16));
    CHECK(shard.runs(3));
    CHECK(shard.runs(19));
    CHECK(!shard.runs(0));
    CHECK(!shard.runs(4));
    CHECK(gctest::parse_shard("15/16").runs(31));

    gctest::Shard whole = gctest::parse_shard(nullptr);
    bool every = true;
    for (size_t i = 0; i < 40; i++) every = every && whole.runs(i);
    CHECK(every);
}

TEST(shard_refuses_an_index_past_its_count_a_zero_count_and_any_other_text) {
    for (const char* text : {"16/16", "17/16", "0/0", "1/0", "", "3", "3/", "/16", "a/16", "3/16x", "-1/16",
                             "3/-16", " 3/16", "3/16/2", "1234567890/2"}) {
        std::string why = refusal_of<std::invalid_argument>([&] { gctest::parse_shard(text); });
        if (why.empty()) std::printf("    accepted \"%s\"\n", text);
        CHECK(!why.empty());
    }
}

// A refused shard ends the run before its first test, with an error: a
// wrong index in the nightly fails the night instead of passing on no tests.
TEST(geocoder_tests_exits_with_an_error_for_a_shard_it_refuses) {
    ScratchDir dir("test-shard");
    const std::string out = dir.path() + "/out.txt";
    for (const char* shard : {"16/16", "0/0", "x"}) {
        int status = rerun_self(nullptr, shard, out);
        if (status != 2) std::printf("    GEOCODER_TEST_SHARD=%s: exit status %d\n", shard, status);
        CHECK_EQ(status, 2);
        CHECK_EQ(read_text(out), std::string());
    }
}

TEST(geocoder_tests_count_prints_the_size_of_the_registry) {
    ScratchDir dir("test-shard");
    const std::string out = dir.path() + "/out.txt";
    CHECK_EQ(rerun_self("--count", nullptr, out), 0);
    CHECK_EQ(read_text(out), std::to_string(gctest::registry().size()) + "\n");
    // Whatever shard the run was given: --count speaks for the registry.
    CHECK_EQ(rerun_self("--count", "3/16", out), 0);
    CHECK_EQ(read_text(out), std::to_string(gctest::registry().size()) + "\n");
}

// geocoder-tests' runner (main() runs its shard with it): across the shards
// of a count, every test of the registry runs once, in the shard its index
// names, and each shard's counts are of the tests it ran.
TEST(the_runner_runs_every_test_once_across_a_counts_shards) {
    for (size_t count : {size_t(1), size_t(2), size_t(3), size_t(16), size_t(40)}) {
        std::vector<std::vector<size_t>> ran_in(37);  // each test's shards
        size_t shard = 0;
        std::vector<gctest::Test> tests;
        for (size_t i = 0; i < ran_in.size(); i++)
            tests.push_back({"registered", [&, i] { ran_in[i].push_back(shard); }});
        size_t ran = 0;
        for (shard = 0; shard < count; shard++) {
            const gctest::ShardRun run = gctest::run_shard(tests, {shard, count});
            size_t its = 0;
            for (const auto& shards : ran_in) its += size_t(std::count(shards.begin(), shards.end(), shard));
            CHECK_EQ(run.ran, its);
            CHECK_EQ(run.passed, int(run.ran));
            ran += run.ran;
        }
        CHECK_EQ(ran, ran_in.size());
        for (size_t i = 0; i < ran_in.size(); i++) {
            if (ran_in[i] != std::vector<size_t>{i % count})
                std::printf("    test %zu of %zu shards ran %zu times\n", i, count, ran_in[i].size());
            CHECK(ran_in[i] == std::vector<size_t>{i % count});
        }
    }
}
