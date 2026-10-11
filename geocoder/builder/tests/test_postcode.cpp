// Unit tests for postcode_validation.h.
//
// validate_postcode_for_country(cc, pc) validates a postcode against a
// per-country pattern (auto-generated from Nominatim country_settings.yaml).
// These tests lock in the CURRENT behaviour: input normalization (lowercasing,
// stripping spaces/hyphens), per-country length+char rules, the "always true"
// and "always false" country buckets, the unknown-country default, and the
// null/empty/length guard rails. They are a regression net for refactors, not
// a spec.
#include "postcode_validation.h"

#include "test_framework.h"

// ---- Guard rails: null / empty / too-short cc, empty pc -------------------

TEST(postcode_null_cc_is_false) {
    CHECK(!validate_postcode_for_country(nullptr, "12345"));
}

TEST(postcode_null_pc_is_false) {
    CHECK(!validate_postcode_for_country("us", nullptr));
}

TEST(postcode_empty_cc_is_false) {
    CHECK(!validate_postcode_for_country("", "12345"));
}

TEST(postcode_single_char_cc_is_false) {
    // cc must be at least 2 chars (cc[1] must be non-zero).
    CHECK(!validate_postcode_for_country("u", "12345"));
}

TEST(postcode_empty_pc_is_false) {
    CHECK(!validate_postcode_for_country("us", ""));
}

// ---- US: exactly 5 digits -------------------------------------------------

TEST(postcode_us_valid_5_digits) {
    CHECK(validate_postcode_for_country("us", "12345"));
    CHECK(validate_postcode_for_country("us", "90210"));
    CHECK(validate_postcode_for_country("us", "00000"));
}

TEST(postcode_us_wrong_length_is_false) {
    CHECK(!validate_postcode_for_country("us", "1234"));    // too short
    CHECK(!validate_postcode_for_country("us", "123456"));  // too long
}

TEST(postcode_us_non_digit_is_false) {
    CHECK(!validate_postcode_for_country("us", "1234A"));
    CHECK(!validate_postcode_for_country("us", "ABCDE"));
}

TEST(postcode_us_zip_plus_four_with_hyphen) {
    // "12345-6789" -> hyphen stripped -> "123456789" (9 digits) -> not len 5.
    CHECK(!validate_postcode_for_country("us", "12345-6789"));
}

// ---- UK / GB: len 5..7, ends with digit + letter + letter -----------------

TEST(postcode_gb_valid_examples) {
    // GB pattern: bare len 5..7, last3 = d L L.
    CHECK(validate_postcode_for_country("gb", "EC1A 1BB"));  // -> EC1A1BB (7)
    CHECK(validate_postcode_for_country("gb", "W1A 0AX"));   // -> W1A0AX (6)
    CHECK(validate_postcode_for_country("gb", "M1 1AE"));    // -> M11AE (5)
    CHECK(validate_postcode_for_country("gb", "B33 8TH"));   // -> B338TH (6)
}

TEST(postcode_gb_lowercase_is_normalized) {
    // lowercase letters are uppercased before validation.
    CHECK(validate_postcode_for_country("gb", "m1 1ae"));
}

TEST(postcode_gb_too_short_is_false) {
    CHECK(!validate_postcode_for_country("gb", "1AA"));  // bare len 3
}

TEST(postcode_gb_bad_tail_is_false) {
    // last3 must be digit,letter,letter. "AE1" fails (last char is digit).
    CHECK(!validate_postcode_for_country("gb", "M1 AE1"));
}

// ---- Canada: 6 chars, L D L D L D -----------------------------------------

TEST(postcode_ca_valid) {
    CHECK(validate_postcode_for_country("ca", "K1A 0B1"));  // -> K1A0B1
    CHECK(validate_postcode_for_country("ca", "M5V 3L9"));
}

TEST(postcode_ca_wrong_pattern_is_false) {
    CHECK(!validate_postcode_for_country("ca", "12345"));   // all digits
    CHECK(!validate_postcode_for_country("ca", "K1A0B"));   // len 5
}

// ---- Netherlands: 6 chars, D D D D L L -------------------------------------

TEST(postcode_nl_valid) {
    CHECK(validate_postcode_for_country("nl", "1234 AB"));  // -> 1234AB
}

TEST(postcode_nl_separators_stripped_still_valid) {
    // "12 34AB" -> strip space -> "1234AB" = D D D D L L -> valid.
    CHECK(validate_postcode_for_country("nl", "12 34AB"));
}

TEST(postcode_nl_wrong_length_is_false) {
    CHECK(!validate_postcode_for_country("nl", "123AB"));    // len 5
    CHECK(!validate_postcode_for_country("nl", "12345A"));   // last2 not both letters
}

TEST(postcode_nl_letters_then_digits_is_false) {
    // pattern is digits-then-letters; reversed should fail.
    CHECK(!validate_postcode_for_country("nl", "AB1234"));
}

// ---- Brazil: exactly 8 digits ---------------------------------------------

TEST(postcode_br_valid_8_digits) {
    CHECK(validate_postcode_for_country("br", "01310-100"));  // -> 01310100 (8)
    CHECK(validate_postcode_for_country("br", "12345678"));
}

TEST(postcode_br_wrong_length_is_false) {
    CHECK(!validate_postcode_for_country("br", "1234567"));  // 7 digits
}

// ---- 4-digit countries (AT, AU, BE, CH, DK, ...) --------------------------

TEST(postcode_at_4_digits) {
    CHECK(validate_postcode_for_country("at", "1010"));
    CHECK(!validate_postcode_for_country("at", "101"));
    CHECK(!validate_postcode_for_country("at", "10101"));
}

TEST(postcode_de_5_digits) {
    CHECK(validate_postcode_for_country("de", "10115"));
    CHECK(!validate_postcode_for_country("de", "1011"));
}

// ---- Bahrain: 4 OR 5 digits (dual-length branch) --------------------------

TEST(postcode_bh_4_or_5_digits) {
    CHECK(validate_postcode_for_country("bh", "1234"));
    CHECK(validate_postcode_for_country("bh", "12345"));
    CHECK(!validate_postcode_for_country("bh", "123"));     // 3 too short
    CHECK(!validate_postcode_for_country("bh", "123456"));  // 6 too long
    CHECK(!validate_postcode_for_country("bh", "1234A"));   // non-digit
}

// ---- Ukraine: 6 OR 7 digits ------------------------------------------------

TEST(postcode_ua_6_or_7_digits) {
    CHECK(validate_postcode_for_country("ua", "123456"));
    CHECK(validate_postcode_for_country("ua", "1234567"));
    CHECK(!validate_postcode_for_country("ua", "12345"));
}

// ---- "Always false" bucket (postcode not used in that country) -------------

TEST(postcode_always_false_countries) {
    // A representative sample of the case-returns-false countries.
    CHECK(!validate_postcode_for_country("ae", "12345"));
    CHECK(!validate_postcode_for_country("ao", "1234"));
    CHECK(!validate_postcode_for_country("zw", "00263"));
    CHECK(!validate_postcode_for_country("qa", "12345"));
    CHECK(!validate_postcode_for_country("ye", "12345"));
    // even a plausible-looking value is rejected outright.
    CHECK(!validate_postcode_for_country("bs", "N1234"));
}

// ---- "Always true" bucket (pattern present but loosely accepted) -----------

TEST(postcode_always_true_countries) {
    // These return true for any non-empty bare string of len 1..10.
    CHECK(validate_postcode_for_country("ai", "2640"));
    CHECK(validate_postcode_for_country("ar", "C1425"));
    CHECK(validate_postcode_for_country("bb", "BB12345"));
    CHECK(validate_postcode_for_country("ie", "D02 AF30"));
    CHECK(validate_postcode_for_country("lv", "1234"));
    CHECK(validate_postcode_for_country("md", "MD2001"));
    // arbitrary junk still passes for an "always true" country (within len).
    CHECK(validate_postcode_for_country("ai", "ZZZ"));
}

// ---- Unknown country: default branch len 3..10 -----------------------------

TEST(postcode_unknown_country_default_len_range) {
    // "zz" is not a switch case -> default: len 3..10.
    CHECK(validate_postcode_for_country("zz", "123"));      // len 3 ok
    CHECK(validate_postcode_for_country("zz", "ABCDEFGHIJ"));// len 10 ok
    CHECK(!validate_postcode_for_country("zz", "12"));      // len 2 too short
    CHECK(validate_postcode_for_country("zz", "AB-CD"));    // hyphen stripped -> ABCD (4)
}

TEST(postcode_unknown_country_too_short_after_strip) {
    // "z z" -> strip space -> "ZZ" (len 2) -> below default min.
    CHECK(!validate_postcode_for_country("zz", "z z"));
}

// ---- Global length guard: bare > 10 always rejected ------------------------

TEST(postcode_over_10_chars_rejected_even_unknown) {
    // 11 bare chars exceeds the hard cap regardless of country.
    CHECK(!validate_postcode_for_country("zz", "ABCDEFGHIJK"));
    // and for an "always true" country the >10 cap still applies.
    CHECK(!validate_postcode_for_country("ai", "ABCDEFGHIJK"));
}

TEST(postcode_only_separators_strips_to_empty) {
    // pc is non-empty (passes the pc[0] guard) but strips to "" -> len 0 false.
    CHECK(!validate_postcode_for_country("us", "   "));
    CHECK(!validate_postcode_for_country("us", "---"));
    CHECK(!validate_postcode_for_country("zz", " - "));
}

// ---- Normalization: spaces and hyphens are equivalent / removed ------------

TEST(postcode_separators_are_stripped) {
    // US 5-digit with embedded separators that vanish.
    CHECK(validate_postcode_for_country("us", "1 2 3 4 5"));  // -> 12345
    CHECK(validate_postcode_for_country("us", "12-345"));     // -> 12345
}
