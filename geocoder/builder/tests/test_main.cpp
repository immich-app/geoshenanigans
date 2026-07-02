// Entry point for the geocoder-tests target. Runs every TEST() registered
// across the other test*.cpp translation units and reports pass/fail.
#include "test_framework.h"

int main() {
    int passed = 0;
    auto& reg = gctest::registry();
    for (auto& t : reg) {
        int before = gctest::failures();
        t.fn();
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
