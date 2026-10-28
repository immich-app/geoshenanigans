// Entry point for the geocoder-tests target. Runs every TEST() registered
// across the other test*.cpp translation units and reports pass/fail.
#include <stdexcept>

#include "test_framework.h"

// LeakSanitizer (the sanitizer build): a test's forked child that ends
// through exit() checks for leaks without the threads the ThreadCache
// (src/parallel.h) keeps for the process's life, since fork copies only the
// forking thread; the thread state only their stacks hold then reads as
// leaked, and the report would end the child with LeakSanitizer's status.
extern "C" const char* __lsan_default_suppressions() { return "leak:ThreadCache::run\n"; }

int main() {
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
