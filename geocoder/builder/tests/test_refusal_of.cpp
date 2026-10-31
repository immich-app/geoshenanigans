// Unit tests for gctest::refusal_of (test_framework.h), the one way the tests
// check that a call is refused.
#include <new>
#include <stdexcept>
#include <string>

#include "test_framework.h"

using gctest::refusal_of;

TEST(refusal_of_gives_the_message_the_call_threw) {
    CHECK_EQ(refusal_of([] { throw std::runtime_error("Malformed patch"); }), std::string("Malformed patch"));
    CHECK_EQ(refusal_of([] { throw std::range_error("past the end"); }), std::string("past the end"));
}

TEST(refusal_of_is_empty_when_the_call_returns) {
    bool ran = false;
    CHECK_EQ(refusal_of([&] { ran = true; }), std::string());
    CHECK(ran);
}

TEST(refusal_of_takes_the_exception_type_it_is_given) {
    CHECK_EQ(refusal_of<std::logic_error>([] { throw std::logic_error("table out of order"); }),
             std::string("table out of order"));
    CHECK_EQ(refusal_of<std::bad_alloc>([] { throw std::bad_alloc(); }), std::string(std::bad_alloc().what()));
}

TEST(refusal_of_lets_an_exception_of_another_type_through) {
    CHECK_EQ(refusal_of([] { refusal_of<std::logic_error>([] { throw std::runtime_error("not a logic error"); }); }),
             std::string("not a logic error"));
    CHECK_EQ(refusal_of<std::logic_error>([] { refusal_of([] { throw std::logic_error("not a runtime error"); }); }),
             std::string("not a runtime error"));
}
