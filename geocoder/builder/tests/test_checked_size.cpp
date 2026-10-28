// checked_size (tools/checked_size.h): the size of a unit a count in an input
// file sizes, computed in 64 bits and checked against what the input has left
// and the size type. Size = uint32_t stands for a 32-bit build's size_t. And
// checked_id: an id from a delta in an input file, checked against u32.
#include "checked_size.h"

#include <cstdint>

#include "test_framework.h"

// An escaped footprint of 2^30 four-byte vertices after its ten-byte header:
// 2^32 + 10 bytes, which a 32-bit size_t wraps to 10, so a ten-byte payload
// passed for it. Refused in 32 bits whatever the input has left, and against
// ten bytes in any size type; the same for the 8-byte vertices of encoding 3.
TEST(checked_size_refuses_a_unit_that_wraps_a_32_bit_size) {
    CHECK(!checked_size<uint32_t>(10, 0x40000000, 4, UINT64_MAX));
    CHECK(!checked_size<uint32_t>(10, 0x40000000, 4, 10));
    CHECK(!checked_size<uint64_t>(10, 0x40000000, 4, 10));
    CHECK(!checked_size<uint32_t>(10, 0x20000000, 8, UINT64_MAX));
    CHECK(!checked_size<uint32_t>(10, 0xFFFFFFFF, 4, UINT64_MAX));
}

// Up to the last byte the input has and the size type holds.
TEST(checked_size_gives_a_unit_that_fits) {
    CHECK(checked_size<uint32_t>(10, 0x3FFFFFFD, 4, UINT64_MAX) == 0xFFFFFFFEu);
    CHECK(checked_size<uint64_t>(10, 0x40000000, 4, UINT64_MAX) == (uint64_t(1) << 32) + 10);
    CHECK(checked_size(10, 3, 4, 22) == size_t(22));
    CHECK(!checked_size(10, 3, 4, 21));
    CHECK(checked_size(36, 0, 0, 36) == size_t(36));
}

// A count whose product (or the header on top) passes 64 bits is refused, not
// wrapped.
TEST(checked_size_refuses_a_unit_past_64_bits) {
    CHECK(!checked_size<uint64_t>(10, uint64_t(1) << 62, 4, UINT64_MAX));
    CHECK(!checked_size<uint64_t>(10, UINT64_MAX / 4, 4, UINT64_MAX));
    CHECK(checked_size<uint64_t>(3, UINT64_MAX / 4, 4, UINT64_MAX) == UINT64_MAX);
}

// An id from a delta or a gap stays a u32: 0 and 2^32 - 1 are reached, one
// past either is refused (no signed overflow, no narrowing), and so is any id
// from a base past u32 but one a negative delta brings back into it.
TEST(checked_id_keeps_ids_in_u32) {
    CHECK(checked_id(1, int64_t(-1)) == 0u);
    CHECK(checked_id(0, int64_t(UINT32_MAX)) == UINT32_MAX);
    CHECK(checked_id(uint64_t(1) << 32, int64_t(-1)) == UINT32_MAX);
    CHECK(!checked_id(0, int64_t(-1)));
    CHECK(!checked_id(0, int64_t(1) << 32));
    CHECK(!checked_id(1, INT64_MAX));
    CHECK(!checked_id(5, INT64_MIN));
    CHECK(!checked_id(UINT32_MAX, int64_t(1)));
    CHECK(checked_id(UINT32_MAX - 3, uint64_t(3)) == UINT32_MAX);
    CHECK(!checked_id(UINT32_MAX, uint64_t(1)));
    CHECK(!checked_id(0, ~uint64_t(0)));
    CHECK(!checked_id(uint64_t(1) << 32, uint64_t(0)));
}
