// Runs a test body in a forked child: for cases that may exhaust memory,
// hang or end the process, none of which may take the test run with it.
#pragma once

#include <cstdio>
#include <functional>
#include <sys/wait.h>
#include <unistd.h>

#include "sanitizer_report.h"

namespace gctest {

// The child's exit status, EXCEPTION_ESCAPED when body throws (the child
// never unwinds into the test runner, to run the tests after this one), or
// 128 + the signal that ended it (SIGALRM after timeout_s seconds); -1 when
// fork fails. A child a sanitizer report ended throws SanitizerReported,
// whatever the test expected of it. Output buffered so far goes out first,
// so a child that leaves through exit() does not write it again.
constexpr int EXCEPTION_ESCAPED = 125;
inline int exit_status_in_child(const std::function<int()>& body, unsigned timeout_s) {
    std::fflush(nullptr);
    const pid_t child = fork();
    if (child < 0) return -1;
    if (child == 0) {
        alarm(timeout_s);
        int status = EXCEPTION_ESCAPED;
        try {
            status = body();
        } catch (...) {
        }
        _exit(status);
    }
    int status = 0;
    while (waitpid(child, &status, 0) < 0) {}
    if (ended_by_sanitizer_report(status)) throw SanitizerReported("A forked child ended with a sanitizer report, printed above");
    return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
}

}  // namespace gctest
