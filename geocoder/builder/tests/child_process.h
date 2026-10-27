// Runs a test body in a forked child: for cases that may exhaust memory,
// hang or end the process, none of which may take the test run with it.
#pragma once

#include <cstdio>
#include <functional>
#include <sys/wait.h>
#include <unistd.h>

namespace gctest {

// The child's exit status, or 128 + the signal that ended it (SIGALRM after
// timeout_s seconds); -1 when fork fails. Output buffered so far goes out
// first, so a child that leaves through exit() does not write it again.
inline int exit_status_in_child(const std::function<int()>& body, unsigned timeout_s) {
    std::fflush(nullptr);
    const pid_t child = fork();
    if (child < 0) return -1;
    if (child == 0) {
        alarm(timeout_s);
        _exit(body());
    }
    int status = 0;
    while (waitpid(child, &status, 0) < 0) {}
    return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
}

}  // namespace gctest
