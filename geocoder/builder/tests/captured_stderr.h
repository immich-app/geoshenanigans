// What a test's code writes to std::cerr, kept from the test run's output:
// std::cerr writes into the capture while it lives and to its own buffer
// again once it goes, however the scope ends. A test that throws (a REQUIRE,
// or an exception the runner catches) leaves std::cerr as it found it for
// the tests after it.
#pragma once

#include <iostream>
#include <sstream>
#include <string>

namespace gctest {

class CapturedStderr {
public:
    CapturedStderr() : was_(std::cerr.rdbuf(text_.rdbuf())) {}
    ~CapturedStderr() { std::cerr.rdbuf(was_); }
    CapturedStderr(const CapturedStderr&) = delete;
    CapturedStderr& operator=(const CapturedStderr&) = delete;

    // What std::cerr took so far.
    std::string str() const { return text_.str(); }

private:
    std::ostringstream text_;
    std::streambuf* was_;
};

}  // namespace gctest
