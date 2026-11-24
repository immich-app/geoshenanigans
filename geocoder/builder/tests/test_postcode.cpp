// Unit tests for postcode_validation.h.
//
// validate_postcode_for_country(cc, pc) validates a postcode against a
// per-country pattern (auto-generated from Nominatim country_settings.yaml).
// These tests lock in the CURRENT behaviour: input normalization (lowercasing,
// stripping spaces/hyphens, the country's code before the postcode),
// per-country length+char rules, the "always false" country bucket, the
// unknown-country default, and the null/empty/length guard rails. They are a
// regression net for refactors, not a spec.
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

// ---- Bahrain: 3 OR 4 digits (d?ddd) -----------------------------------------

TEST(postcode_bh_3_or_4_digits) {
    CHECK(validate_postcode_for_country("bh", "317"));
    CHECK(validate_postcode_for_country("bh", "1234"));
    CHECK(!validate_postcode_for_country("bh", "12"));     // 2 too short
    CHECK(!validate_postcode_for_country("bh", "31700"));  // 5 too long
    CHECK(!validate_postcode_for_country("bh", "123A"));   // non-digit
}

// ---- Ukraine: 5 OR 6 digits (d?ddddd) --------------------------------------

TEST(postcode_ua_5_or_6_digits) {
    CHECK(validate_postcode_for_country("ua", "01001"));
    CHECK(validate_postcode_for_country("ua", "123456"));
    CHECK(!validate_postcode_for_country("ua", "0100"));
    CHECK(!validate_postcode_for_country("ua", "0100123"));
}

// ---- Nigeria: dddddd, or (ll)[ -]?(dd)[ -]?([A-Z0-9]{3})[ -]?(ll)[ -]?(dd) --

TEST(postcode_ng_6_digits_or_the_lettered_pattern) {
    CHECK(validate_postcode_for_country("ng", "100001"));
    CHECK(!validate_postcode_for_country("ng", "10001"));
    // The lettered pattern is 11 characters past its separators, over the 10 of every other.
    CHECK(validate_postcode_for_country("ng", "AB 12 C3D EF 45"));
    CHECK(validate_postcode_for_country("ng", "ab-12-123-ef-45"));
    CHECK(validate_postcode_for_country("ng", "AB12XYZEF45"));
    CHECK(!validate_postcode_for_country("ng", "A1 12 C3D EF 45"));
    CHECK(!validate_postcode_for_country("ng", "AB 12 C3D E5 45"));
    CHECK(!validate_postcode_for_country("ng", "AB 12 C3D EF 4X"));
    CHECK(!validate_postcode_for_country("ng", "AB 12 C3D EF 456"));
}

// A house's own tag passes in these countries as it does in their patterns.
TEST(postcode_tag_ua_bh_ng) {
    CHECK(postcode_tag_valid_for_country("ua", "01001"));
    CHECK(postcode_tag_valid_for_country("ua", "UA-01001"));
    CHECK(!postcode_tag_valid_for_country("ua", "0100123"));
    CHECK(postcode_tag_valid_for_country("bh", "317"));
    CHECK(!postcode_tag_valid_for_country("bh", "31700"));
    CHECK(postcode_tag_valid_for_country("ng", "AB 12 C3D EF 45"));
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

// ---- Patterns with letters, prefixes, choices of length ---------------------

TEST(postcode_saudi_arabia_takes_five_digits_and_four_more) {
    // ddddd(?:-dddd)?: 12345 and 12345-6789, but no letters after the hyphen.
    CHECK(validate_postcode_for_country("sa", "12345"));
    CHECK(validate_postcode_for_country("sa", "12345-6789"));
    CHECK(!validate_postcode_for_country("sa", "12345-ABCD"));
    CHECK(!validate_postcode_for_country("sa", "1234"));
    CHECK(!validate_postcode_for_country("sa", "12345-678"));
}

TEST(postcode_patterns_of_letters_and_fixed_codes) {
    CHECK(validate_postcode_for_country("ai", "2640"));
    CHECK(!validate_postcode_for_country("ai", "ZZZ"));
    CHECK(validate_postcode_for_country("ar", "C1425"));
    CHECK(validate_postcode_for_country("ar", "C1425DKA"));
    CHECK(validate_postcode_for_country("ar", "1425"));
    CHECK(!validate_postcode_for_country("ar", "C1425DK"));
    CHECK(validate_postcode_for_country("ie", "D02 AF30"));
    CHECK(!validate_postcode_for_country("ie", "D02 AB30"));  // B is no Eircode letter
    CHECK(validate_postcode_for_country("gg", "GY1 1AA"));
    CHECK(validate_postcode_for_country("je", "JE2 3AB"));
    CHECK(validate_postcode_for_country("im", "IM99 1AA"));
    CHECK(!validate_postcode_for_country("im", "GY1 1AA"));
    CHECK(validate_postcode_for_country("fk", "FIQQ 1ZZ"));
    CHECK(!validate_postcode_for_country("fk", "FIQQ 1ZY"));
    CHECK(validate_postcode_for_country("sh", "STHL 1ZZ"));
    CHECK(validate_postcode_for_country("mt", "VLT 1010"));
    CHECK(!validate_postcode_for_country("mt", "VL 1010"));
    CHECK(validate_postcode_for_country("gh", "GA-123-4567"));
    CHECK(validate_postcode_for_country("gh", "GA-1234-5678"));
    CHECK(!validate_postcode_for_country("gh", "GA-12-3456"));
    CHECK(validate_postcode_for_country("kz", "A10A3B5"));
    CHECK(validate_postcode_for_country("kz", "010000"));
    CHECK(!validate_postcode_for_country("kz", "A10A3BB"));
}

TEST(postcode_patterns_of_digits_and_choices_of_length) {
    CHECK(validate_postcode_for_country("pl", "00-950"));
    CHECK(!validate_postcode_for_country("pl", "00-95"));
    CHECK(validate_postcode_for_country("pt", "1000"));
    CHECK(validate_postcode_for_country("pt", "1000-001"));
    CHECK(!validate_postcode_for_country("pt", "1000-01"));
    CHECK(validate_postcode_for_country("tw", "100"));
    CHECK(validate_postcode_for_country("tw", "10048"));
    CHECK(validate_postcode_for_country("tw", "100048"));
    CHECK(!validate_postcode_for_country("tw", "1004"));
    CHECK(validate_postcode_for_country("lb", "1103 2010"));
    CHECK(!validate_postcode_for_country("lb", "110320"));
    CHECK(validate_postcode_for_country("mz", "1100-01"));
    CHECK(validate_postcode_for_country("cy", "1010"));
    CHECK(validate_postcode_for_country("cy", "99010"));
    CHECK(!validate_postcode_for_country("cy", "98010"));
    CHECK(validate_postcode_for_country("mc", "98000"));
    CHECK(!validate_postcode_for_country("mc", "97000"));
    CHECK(validate_postcode_for_country("sm", "47890"));
    CHECK(validate_postcode_for_country("va", "00120"));
    CHECK(!validate_postcode_for_country("va", "00121"));
    CHECK(validate_postcode_for_country("pw", "96940"));
    CHECK(!validate_postcode_for_country("pw", "96941"));
    // Iran's (ddddd)[-_ ]?(ddddd), whose underscore stays: 11 characters.
    CHECK(validate_postcode_for_country("ir", "1234567890"));
    CHECK(validate_postcode_for_country("ir", "12345-67890"));
    CHECK(validate_postcode_for_country("ir", "12345_67890"));
    CHECK(!validate_postcode_for_country("ir", "12345_6789"));
}

TEST(postcode_the_country_s_code_may_lead) {
    // CountryPostcodeMatcher matches \s*(?:CC[ -]?)?(pattern)\s*.
    CHECK(validate_postcode_for_country("lv", "1050"));
    CHECK(validate_postcode_for_country("lv", "LV-1050"));
    CHECK(validate_postcode_for_country("md", "MD2001"));
    CHECK(validate_postcode_for_country("bb", "BB12345"));
    CHECK(validate_postcode_for_country("ky", "KY1-1000"));
    CHECK(validate_postcode_for_country("lt", "LT-01100"));
    CHECK(validate_postcode_for_country("de", "DE 10115"));
    CHECK(!validate_postcode_for_country("lv", "LT-1050"));
    CHECK(!validate_postcode_for_country("lv", "LV-105"));
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
