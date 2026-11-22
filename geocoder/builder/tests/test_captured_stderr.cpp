// captured_stderr.h: std::cerr written into a capture while it lives, and
// back on its own buffer once it goes, however its scope ends.
#include "captured_stderr.h"

#include <iostream>
#include <stdexcept>

#include "test_framework.h"

TEST(a_capture_takes_what_stderr_writes_and_gives_stderr_back_however_its_scope_ends) {
    std::streambuf* const before = std::cerr.rdbuf();
    {
        gctest::CapturedStderr captured;
        std::cerr << "kept from the run";
        CHECK(captured.str() == "kept from the run");
        CHECK(std::cerr.rdbuf() != before);
    }
    CHECK(std::cerr.rdbuf() == before);
    // A scope a throw ends, as a REQUIRE's does.
    bool threw = false;
    try {
        gctest::CapturedStderr captured;
        std::cerr << "kept from the run";
        throw std::runtime_error("the code under test throws");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
    CHECK(std::cerr.rdbuf() == before);
}
