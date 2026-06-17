// Unit tests for StringPool (string_pool.h).
// StringPool interns strings into a flat NUL-terminated byte buffer and hands
// back the byte offset of each string's first character. These tests lock in
// the current behaviour (dedup returns the same offset, distinct strings get
// distinct sequential offsets, the empty string interns to offset 0 on a fresh
// pool, and every returned offset reads back the original string from data()).
#include "string_pool.h"

#include <cstring>
#include <string>

#include "test_framework.h"

// Read a NUL-terminated C string out of the pool buffer at `off`.
static std::string read_at(const StringPool& p, uint32_t off) {
    const std::vector<char>& d = p.data();
    CHECK(off <= d.size());
    return std::string(d.data() + off);  // stops at the '\0' written by intern
}

TEST(string_pool_intern_same_string_returns_same_offset) {
    StringPool p;
    uint32_t a = p.intern("hello");
    uint32_t b = p.intern("hello");
    CHECK_EQ(a, b);
    // Re-interning must not append a second copy.
    CHECK_EQ(p.data().size(), size_t(6));  // "hello" + '\0'
}

TEST(string_pool_distinct_strings_get_distinct_offsets) {
    StringPool p;
    uint32_t a = p.intern("alpha");  // offset 0,  bytes 0..5  (len 5 + NUL)
    uint32_t b = p.intern("beta");   // offset 6,  bytes 6..10 (len 4 + NUL)
    uint32_t c = p.intern("gamma");  // offset 11, bytes 11..16
    CHECK_EQ(a, 0u);
    CHECK_EQ(b, 6u);
    CHECK_EQ(c, 11u);
    CHECK(a != b);
    CHECK(b != c);
    CHECK_EQ(p.data().size(), size_t(17));
}

TEST(string_pool_first_intern_is_offset_zero) {
    StringPool p;
    CHECK_EQ(p.intern("first"), 0u);
}

TEST(string_pool_empty_string_interns_at_offset_zero) {
    // Empty string on a fresh pool: offset 0, appends a single '\0'.
    StringPool p;
    uint32_t e = p.intern("");
    CHECK_EQ(e, 0u);
    CHECK_EQ(p.data().size(), size_t(1));
    CHECK_EQ(p.data()[0], '\0');
    // Interning it again is a no-op that returns the same offset.
    CHECK_EQ(p.intern(""), 0u);
    CHECK_EQ(p.data().size(), size_t(1));
}

TEST(string_pool_empty_string_after_other_data) {
    // When the empty string is interned after content, it gets the next offset
    // and is distinct from offset 0 (which now holds a non-empty string).
    StringPool p;
    uint32_t a = p.intern("xyz");  // 0
    uint32_t e = p.intern("");     // 4 ("xyz" + NUL = 4 bytes)
    CHECK_EQ(a, 0u);
    CHECK_EQ(e, 4u);
    CHECK_EQ(read_at(p, e), std::string(""));
    // Reading offset 0 yields the original non-empty string, not "".
    CHECK_EQ(read_at(p, a), std::string("xyz"));
}

TEST(string_pool_offsets_read_back_original_strings) {
    StringPool p;
    const char* words[] = {"one", "two", "three", "four", "two", "one"};
    uint32_t offs[6];
    for (int i = 0; i < 6; ++i) offs[i] = p.intern(words[i]);
    // Every offset must read back exactly the string that produced it.
    for (int i = 0; i < 6; ++i) {
        CHECK_EQ(read_at(p, offs[i]), std::string(words[i]));
    }
    // Duplicates ("two", "one") collapse onto their first occurrence's offset.
    CHECK_EQ(offs[4], offs[1]);  // "two"
    CHECK_EQ(offs[5], offs[0]);  // "one"
}

TEST(string_pool_handles_embedded_and_unicode_bytes) {
    // intern copies raw bytes including high-bit (UTF-8) bytes; the terminator
    // is the appended '\0'. A string containing an interior NUL is stored in
    // full, though read_at (a C-string read) would stop at the interior NUL.
    StringPool p;
    std::string utf8 = "caf\xc3\xa9";  // "café"
    uint32_t u = p.intern(utf8);
    CHECK_EQ(u, 0u);
    CHECK_EQ(read_at(p, u), utf8);
    CHECK_EQ(p.data().size(), utf8.size() + 1);

    std::string with_nul("a\0b", 3);  // 3 bytes, interior NUL
    uint32_t w = p.intern(with_nul);
    CHECK_EQ(w, (uint32_t)(utf8.size() + 1));
    // The raw buffer holds all 3 bytes + the appended terminator.
    CHECK_EQ(p.data().size(), utf8.size() + 1 + 3 + 1);
    CHECK_EQ(p.data()[w + 0], 'a');
    CHECK_EQ(p.data()[w + 1], '\0');
    CHECK_EQ(p.data()[w + 2], 'b');
    CHECK_EQ(p.data()[w + 3], '\0');  // appended terminator
}

TEST(string_pool_mutable_data_is_same_buffer) {
    StringPool p;
    p.intern("zzz");
    // mutable_data() exposes the same underlying buffer as data().
    CHECK_EQ(p.mutable_data().size(), p.data().size());
    CHECK_EQ(&p.mutable_data()[0], &p.data()[0]);
}
