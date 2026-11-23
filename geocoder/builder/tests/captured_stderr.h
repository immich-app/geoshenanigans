// What the code a test runs writes to std::cerr, held in a string.
#pragma once

#include <iostream>
#include <sstream>
#include <string>

namespace gctest {

// std::cerr writes to the capture from its construction, and has its own
// buffer back when the capture goes out of scope: at the scope's end, or as
// a REQUIRE or the code under test throws through it. Given back by hand
// after the code, the buffer would stay the capture's past a throw, and the
// runner, which catches it and goes on to the next test, would leave
// std::cerr writing to a destroyed capture.
class CapturedStderr {
public:
    CapturedStderr() : own_(std::cerr.rdbuf(text_.rdbuf())) {}
    ~CapturedStderr() { std::cerr.rdbuf(own_); }
    CapturedStderr(const CapturedStderr&) = delete;
    CapturedStderr& operator=(const CapturedStderr&) = delete;

    // What std::cerr has written to it so far.
    std::string str() const { return text_.str(); }

private:
    std::ostringstream text_;
    std::streambuf* own_;
};

}  // namespace gctest
