// Entry point for the geocoder-tests target. Runs every TEST() registered
// across the other test*.cpp translation units and reports pass/fail.
#include <cstdlib>
#include <stdexcept>
#include <string>

#include "sanitizer_report.h"
#include "test_framework.h"

// LeakSanitizer (the sanitizer build): a test's forked child that ends
// through exit() checks for leaks without the threads the ThreadCache
// (src/parallel.h) keeps for the process's life, since fork copies only the
// forking thread; the thread state only their stacks hold then reads as
// leaked, and the report would end the child with LeakSanitizer's status.
extern "C" const char* __lsan_default_suppressions() { return "leak:ThreadCache::run\n"; }

// A sanitizer report ends this binary, and the children it forks, with
// SANITIZER_REPORTED (tests/sanitizer_report.h): AddressSanitizer's (and so
// LeakSanitizer's) and UBSan's, which takes its own options.
extern "C" const char* __asan_default_options() { return gctest::SANITIZER_EXIT_OPTION; }
extern "C" const char* __ubsan_default_options() { return gctest::SANITIZER_EXIT_OPTION; }

// And one in a tool it runs: the tools' sanitizer options are these defaults
// with the environment's after them, which win as they do for this binary.
static void pass_sanitizer_options_to_tools() {
    for (const char* name : {"ASAN_OPTIONS", "UBSAN_OPTIONS"}) {
        const char* given = std::getenv(name);
        setenv(name, (std::string(gctest::SANITIZER_EXIT_OPTION) + ":" + (given ? given : "")).c_str(), 1);
    }
}

int main() {
    pass_sanitizer_options_to_tools();
    int passed = 0;
    auto& reg = gctest::registry();
    for (auto& t : reg) {
        int before = gctest::failures();
        try {
            t.fn();
        } catch (const gctest::RequireFailed&) {
            // failure already recorded by REQUIRE
        } catch (const std::exception& e) {
            std::printf("    FAIL %s: uncaught exception: %s\n", t.name, e.what());
            gctest::failures()++;
        }
        if (gctest::failures() == before) {
            passed++;
        } else {
            std::printf("FAILED  %s\n", t.name);
        }
    }
    std::printf("\n%d/%zu tests passed  (%d checks, %d failures)\n", passed,
                reg.size(), gctest::checks(), gctest::failures());
    return gctest::failures() == 0 ? 0 : 1;
}
