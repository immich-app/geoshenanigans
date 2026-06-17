// Minimal dependency-free unit-test harness for the geocoder builder.
//
// Why hand-rolled rather than gtest/doctest: the builder has no existing test
// dependency and these tests cover small pure functions; a ~60-line registry +
// a couple of CHECK macros keeps the toolchain free of a vendored mega-header.
//
// Usage:
//   #include "test_framework.h"
//   TEST(my_case) { CHECK(1 + 1 == 2); CHECK_EQ(foo(), 42); }
// One translation unit (test_main.cpp) defines main(); each test*.cpp just
// registers TEST()s. Run target `geocoder-tests` (ctest or directly); a
// non-zero exit status means a failing check.
#pragma once

#include <cmath>
#include <cstdio>
#include <functional>
#include <string>
#include <vector>

namespace gctest {

struct Test {
    const char* name;
    const char* file;
    std::function<void()> fn;
};

inline std::vector<Test>& registry() {
    static std::vector<Test> r;
    return r;
}
inline int& failures() {
    static int f = 0;
    return f;
}
inline int& checks() {
    static int c = 0;
    return c;
}

struct Registrar {
    Registrar(const char* name, const char* file, std::function<void()> fn) {
        registry().push_back({name, file, std::move(fn)});
    }
};

}  // namespace gctest

#define TEST(name)                                                          \
    static void name();                                                     \
    static ::gctest::Registrar gc_reg_##name(#name, __FILE__, name);        \
    static void name()

#define CHECK(cond)                                                         \
    do {                                                                    \
        ::gctest::checks()++;                                               \
        if (!(cond)) {                                                      \
            std::printf("    FAIL %s:%d  CHECK(%s)\n", __FILE__, __LINE__,  \
                        #cond);                                             \
            ::gctest::failures()++;                                         \
        }                                                                   \
    } while (0)

// Equality check that prints both operands when they differ (works for any
// type streamable to a std::string via std::to_string, plus const char*).
#define CHECK_EQ(a, b)                                                      \
    do {                                                                    \
        ::gctest::checks()++;                                               \
        if (!((a) == (b))) {                                                \
            std::printf("    FAIL %s:%d  CHECK_EQ(%s, %s)\n", __FILE__,     \
                        __LINE__, #a, #b);                                  \
            ::gctest::failures()++;                                         \
        }                                                                   \
    } while (0)

// Approximate equality for floating point.
#define CHECK_NEAR(a, b, eps)                                               \
    do {                                                                    \
        ::gctest::checks()++;                                               \
        if (std::fabs((double)(a) - (double)(b)) > (double)(eps)) {         \
            std::printf("    FAIL %s:%d  CHECK_NEAR(%s, %s, %s)\n",         \
                        __FILE__, __LINE__, #a, #b, #eps);                  \
            ::gctest::failures()++;                                         \
        }                                                                   \
    } while (0)
