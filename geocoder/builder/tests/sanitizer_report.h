// A sanitizer report (AddressSanitizer's, LeakSanitizer's or UBSan's) ends a
// sanitized process with SANITIZER_REPORTED: the test binary, a child it
// forks and a tool it runs (tests/test_main.cpp gives them the option). No
// tool or child exits with it otherwise, so a run it ends fails the test
// whatever the test expected of the run: the sanitizers' own status, 1, would
// pass as a tool refusing its input.
#pragma once

#include <stdexcept>
#include <sys/wait.h>

#define GCTEST_SANITIZER_REPORTED 86
#define GCTEST_STRINGIFY_(x) #x
#define GCTEST_STRINGIFY(x) GCTEST_STRINGIFY_(x)

namespace gctest {

constexpr int SANITIZER_REPORTED = GCTEST_SANITIZER_REPORTED;
// The sanitizers' option for it, as their *_OPTIONS take it.
constexpr char SANITIZER_EXIT_OPTION[] = "exitcode=" GCTEST_STRINGIFY(GCTEST_SANITIZER_REPORTED);

// Thrown, failing the test, for a run a sanitizer report ended.
struct SanitizerReported : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// Whether wait_status (waitpid's or std::system's) is a process's that a
// sanitizer report ended.
inline bool ended_by_sanitizer_report(int wait_status) {
    return WIFEXITED(wait_status) && WEXITSTATUS(wait_status) == SANITIZER_REPORTED;
}

}  // namespace gctest
