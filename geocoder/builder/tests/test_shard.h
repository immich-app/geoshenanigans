// The share of the test registry one geocoder-tests run takes, and the
// runner that runs it: the nightly runs the suite as GEOCODER_TEST_SHARD=i/n
// for i in 0..n-1 at once.
#pragma once

#include <cstddef>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <vector>

#include "test_framework.h"

namespace gctest {

// The tests whose index in the registry is `index` modulo `count`.
struct Shard {
    size_t index = 0, count = 1;
    bool runs(size_t test) const { return test % count == index; }
};

// The shard GEOCODER_TEST_SHARD names ("i/n", decimal, 0 <= i < n), the
// whole registry when it is unset. Anything else throws
// std::invalid_argument: a shard past the count runs no test and passes, and
// one that reads as the whole registry runs every test in place of its share.
inline Shard parse_shard(const char* text) {
    if (!text) return {};
    const std::string s = text;
    auto refuse = [&]() -> std::invalid_argument {
        return std::invalid_argument("GEOCODER_TEST_SHARD must be i/n with 0 <= i < n, not \"" + s + "\"");
    };
    auto number = [&](const std::string& digits) {
        if (digits.empty() || digits.size() > 9 || digits.find_first_not_of("0123456789") != std::string::npos)
            throw refuse();
        return size_t(std::stoul(digits));
    };
    const size_t slash = s.find('/');
    if (slash == std::string::npos) throw refuse();
    Shard shard{number(s.substr(0, slash)), number(s.substr(slash + 1))};
    if (shard.count == 0 || shard.index >= shard.count) throw refuse();
    return shard;
}

// The tests a shard ran and the ones of them that passed.
struct ShardRun {
    int passed = 0;
    size_t ran = 0;
};

// Runs the tests of `tests` that `shard` takes, in their order: a test that
// fails a check, or throws, prints its name.
inline ShardRun run_shard(const std::vector<Test>& tests, Shard shard) {
    ShardRun run;
    for (size_t i = 0; i < tests.size(); i++) {
        if (!shard.runs(i)) continue;
        run.ran++;
        const Test& t = tests[i];
        int before = failures();
        try {
            t.fn();
        } catch (const RequireFailed&) {
            // failure already recorded by REQUIRE
        } catch (const std::exception& e) {
            std::printf("    FAIL %s: uncaught exception: %s\n", t.name, e.what());
            failures()++;
        }
        if (failures() == before) {
            run.passed++;
        } else {
            std::printf("FAILED  %s\n", t.name);
        }
    }
    return run;
}

}  // namespace gctest
