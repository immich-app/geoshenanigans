// A sanitized process ended by the sanitizer report it is asked for: in a
// test's forked child, and as the geocoder-sanitizer-reports tool
// (sanitizer_reports.cpp). The sanitized build's only: elsewhere these are
// undefined behaviour that nothing reports.
#pragma once

#include <climits>
#include <cstddef>
#include <cstdlib>
#include <string>

namespace gctest {

// The kinds, by the sanitizer that reports each.
const char* const ASAN_HEAP_OVERFLOW = "heap-overflow";
const char* const UBSAN_SIGNED_OVERFLOW = "signed-overflow";
const char* const LSAN_LEAK_AT_EXIT = "leak";

// The leak's block, held and then dropped.
inline void* volatile leaked_block;

// Ends this process with kind's report; returns 1, as a tool refusing its
// input exits, for any other kind. Volatile throughout, so the compiler
// neither sees the bug nor drops it: and UBSan's object-size check, which
// would report the overflow first, can't see the allocation.
inline int provoke_sanitizer_report(const std::string& kind) {
    volatile size_t one = 1;
    if (kind == ASAN_HEAP_OVERFLOW) {
        volatile char* volatile bytes = new char[one];
        bytes[one] = 0;
        delete[] bytes;
    } else if (kind == UBSAN_SIGNED_OVERFLOW) {
        volatile int most = INT_MAX;
        return most + int(one);
    } else if (kind == LSAN_LEAK_AT_EXIT) {
        leaked_block = std::malloc(64);
        leaked_block = nullptr;
        std::exit(0);
    }
    return 1;
}

}  // namespace gctest
