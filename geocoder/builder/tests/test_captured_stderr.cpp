// gctest::CapturedStderr (tests/captured_stderr.h): what the code a test runs
// writes to std::cerr, and std::cerr's own buffer back however the capture's
// scope ends.
#include "captured_stderr.h"

#include <iostream>
#include <stdexcept>
#include <string>

#include "test_framework.h"

using gctest::refusal_of;

TEST(captured_stderr_holds_what_cerr_writes_and_gives_cerr_its_buffer_back) {
    std::streambuf* own = std::cerr.rdbuf();
    std::string text;
    {
        gctest::CapturedStderr captured;
        std::cerr << "[pbf-apply] a line\n";
        text = captured.str();
    }
    CHECK(text == "[pbf-apply] a line\n");
    // Given back here too whatever the capture did, for the tests after this.
    CHECK(std::cerr.rdbuf(own) == own);
}

// The code under test throws, and the runner goes on to the next test: what
// that test writes to std::cerr goes to std::cerr's own buffer, not to the
// capture the throw destroyed.
TEST(captured_stderr_gives_cerr_its_buffer_back_when_the_code_throws) {
    std::streambuf* own = std::cerr.rdbuf();
    const std::string refused = refusal_of([] {
        gctest::CapturedStderr captured;
        std::cerr << "[pbf-apply] a line\n";
        throw std::runtime_error("input truncated");
    });
    CHECK(refused == "input truncated");
    CHECK(std::cerr.rdbuf(own) == own);
}
