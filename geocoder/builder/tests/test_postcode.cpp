// Unit tests for postcode_validation.h.
//
// match_postcode(cc, pc, default) is Nominatim's PostcodeFormatter.match on
// the patterns of country_settings.yaml: in upper case, the tag matches
// \s*(?:CC[ -]?)?(pattern)\s* whole, the pattern its group whole, a group of
// zeros, hyphens and spaces is none, and the postcode is the pattern's output
// (match.expand). A country without a pattern takes the default: .* where
// update_postcodes matches postal areas and centroids, [A-Z0-9- ]{3,12} where
// the clean-postcodes sanitizer matches a feature's own tag. A country without
// postcodes, or no country, has none.
#include "postcode_validation.h"

#include <optional>
#include <string>

#include "test_framework.h"

namespace {

uint16_t code(const char* cc) { return pack_country_code(cc[0], cc[1]); }

// The postcode of the tag pc in the country cc (upper case), as postal
// areas and centroids take it.
std::optional<std::string> area(const char* cc, const char* pc) {
    return match_postcode(code(cc), pc, PostcodeDefault::Any);
}

// The postcode of the tag pc in the country cc, as a feature's own tag.
std::optional<std::string> tag(const char* cc, const char* pc) {
    return match_postcode(code(cc), pc, PostcodeDefault::Sanitizer);
}

bool is(const std::optional<std::string>& got, const char* want) {
    if (got == std::string(want)) return true;
    std::printf("    got %s%s%s, want \"%s\"\n", got ? "\"" : "", got ? got->c_str() : "none", got ? "\"" : "", want);
    return false;
}

bool none(const std::optional<std::string>& got) {
    if (!got) return true;
    std::printf("    got \"%s\", want none\n", got->c_str());
    return false;
}

}  // namespace

TEST(postcode_of_no_country_or_a_country_without_postcodes_is_none) {
    // get_matcher(None) is None, and so is a country of postcode: no.
    CHECK(none(match_postcode(0, "12345", PostcodeDefault::Any)));
    CHECK(none(match_postcode(0, "12345", PostcodeDefault::Sanitizer)));
    CHECK(none(area("AE", "12345")));
    CHECK(none(tag("AO", "1234")));
    CHECK(none(area("ZW", "00263")));
    CHECK(none(tag("QA", "12345")));
    CHECK(none(area("BS", "N1234")));
    CHECK(none(area("US", "")));
}

TEST(postcode_is_the_patterns_match_in_upper_case_past_the_white_space_round_it) {
    CHECK(is(area("DE", "10115"), "10115"));
    CHECK(is(tag("DE", " 10115 "), "10115"));
    CHECK(is(tag("DE", "\t10115\n"), "10115"));
    // Python's \s takes Unicode white space: a no-break space and a thin one.
    CHECK(is(tag("DE", "\xc2\xa0" "10115\xe2\x80\x89"), "10115"));
    CHECK(is(area("GB", "m1 1ae"), "M1 1AE"));
    CHECK(none(area("DE", "1011")));
    CHECK(none(area("DE", "101155")));
    CHECK(none(area("US", "1234A")));
    CHECK(none(area("US", "ABCDE")));
    CHECK(none(area("CA", "12345")));
}

TEST(postcode_drops_the_country_code_before_it) {
    // \s*(?:CC[ -]?)?: the country's own code, then one space or hyphen at most.
    CHECK(is(tag("DE", "DE 10115"), "10115"));
    CHECK(is(tag("DE", "de-10115"), "10115"));
    CHECK(is(tag("DE", "DE10115"), "10115"));
    CHECK(is(tag("UA", "UA-01001"), "01001"));
    CHECK(is(area("LT", "LT-01100"), "01100"));
    CHECK(none(tag("DE", "D-E-10115")));
    CHECK(none(tag("DE", "DE  10115")));
    CHECK(none(tag("DE", "US-10115")));
    CHECK(none(area("LV", "LT-1050")));
}

TEST(postcode_takes_a_separator_only_where_its_pattern_has_one) {
    CHECK(none(tag("DE", "DE-10-115")));
    CHECK(none(tag("DE", "10-115")));
    CHECK(none(tag("DE", "1 0115")));
    CHECK(is(area("PL", "12-345"), "12-345"));
    CHECK(is(area("PL", "12 345"), "12-345"));
    CHECK(none(area("PL", "123-45")));
    CHECK(none(area("NL", "1234-AB")));
    CHECK(none(area("NL", "12 34AB")));
    CHECK(none(area("GB", "SW1A-1AA")));
    CHECK(none(area("US", "12-345")));
    CHECK(none(area("US", "1 2 3 4 5")));
    CHECK(is(area("IR", "12345_67890"), "12345-67890"));
    CHECK(is(area("IR", "12345 67890"), "12345-67890"));
    CHECK(none(area("IR", "12345__67890")));
}

TEST(postcode_is_the_patterns_output) {
    CHECK(is(area("NL", "1234AB"), "1234 AB"));
    CHECK(is(area("NL", "nl 1234ab"), "1234 AB"));
    CHECK(is(area("PL", "12345"), "12-345"));
    CHECK(is(area("LV", "1050"), "LV-1050"));
    CHECK(is(area("LV", "LV-1050"), "LV-1050"));
    CHECK(is(area("MD", "MD2001"), "MD-2001"));
    CHECK(is(area("US", "12345-6789"), "12345"));
    CHECK(is(tag("US", "US 12345-6789"), "12345"));
    CHECK(is(area("IR", "1234567890"), "12345-67890"));
    CHECK(is(area("GB", "SW1A1AA"), "SW1A 1AA"));
    CHECK(is(area("CA", "k1a0b1"), "K1A 0B1"));
    CHECK(is(area("BR", "01310100"), "01310-100"));
    CHECK(is(area("AD", "100"), "AD100"));
    CHECK(is(area("AD", "AD100"), "AD100"));
    CHECK(is(area("KY", "1-1000"), "KY1-1000"));
    CHECK(is(area("KY", "KY1-1000"), "KY1-1000"));
    CHECK(is(area("LC", "12345"), "LC12 345"));
    CHECK(is(area("AI", "2640"), "AI-2640"));
    CHECK(is(area("FK", "FIQQ1ZZ"), "FIQQ 1ZZ"));
    CHECK(is(area("BN", "BS 8811"), "BS8811"));
    CHECK(is(area("IN", "110 001"), "110001"));
    // A pattern without an output keeps the match.
    CHECK(is(area("SA", "12345-6789"), "12345-6789"));
    CHECK(none(area("SA", "12345-ABCD")));
    CHECK(is(area("PT", "1000-001"), "1000-001"));
    CHECK(is(area("AR", "c1425dka"), "C1425DKA"));
    CHECK(none(area("AR", "C1425DK")));
}

TEST(postcode_of_several_patterns_is_the_first_that_matches) {
    // ListPostcodeMatcher: Nigeria's dddddd, then its lettered pattern.
    CHECK(is(area("NG", "100001"), "100001"));
    CHECK(none(area("NG", "10001")));
    CHECK(is(area("NG", "AB 12 C3D EF 45"), "AB-12-C3D-EF-45"));
    CHECK(is(area("NG", "ab-12-123-ef-45"), "AB-12-123-EF-45"));
    CHECK(is(area("NG", "AB12XYZEF45"), "AB-12-XYZ-EF-45"));
    CHECK(none(area("NG", "A1 12 C3D EF 45")));
    CHECK(none(area("NG", "AB 12 C3D EF 456")));
}

TEST(postcode_of_zeros_is_none) {
    // zero_pattern ^[0\- ]+$ over the pattern's match.
    CHECK(none(area("US", "00000")));
    CHECK(none(area("AT", "0000")));
    CHECK(none(area("PL", "00-000")));
    CHECK(none(area("PL", "00000")));
    CHECK(is(area("PL", "00-950"), "00-950"));
    CHECK(is(area("VA", "00120"), "00120"));
    CHECK(none(tag("MS", "000")));
    CHECK(none(area("MS", "0-0")));
}

TEST(postcode_of_a_country_without_a_pattern_takes_the_default) {
    // Montserrat has postcodes and no pattern; nor has a code the yaml lacks.
    CHECK(is(area("MS", "msr 1110"), "MSR 1110"));
    CHECK(is(tag("MS", "msr 1110"), "MSR 1110"));
    CHECK(is(area("MS", "AB"), "AB"));
    CHECK(none(tag("MS", "AB")));
    CHECK(is(area("MS", "ABCDEFGHIJKLM"), "ABCDEFGHIJKLM"));
    CHECK(none(tag("MS", "ABCDEFGHIJKLM")));
    CHECK(is(tag("MS", "ABCDEFGHIJKL"), "ABCDEFGHIJKL"));
    CHECK(is(area("MS", "1234;5678"), "1234;5678"));
    CHECK(none(tag("MS", "1234;5678")));
    CHECK(is(tag("ZZ", "AB-CD"), "AB-CD"));
    CHECK(is(tag("ZZ", "-ABC"), "ABC"));
    CHECK(is(tag("ZZ", "-AB"), "-AB"));
}

TEST(postcode_patterns_take_their_countries_postcodes) {
    CHECK(is(area("BH", "317"), "317"));
    CHECK(is(area("BH", "1234"), "1234"));
    CHECK(none(area("BH", "31700")));
    CHECK(is(area("UA", "123456"), "123456"));
    CHECK(none(area("UA", "0100123")));
    CHECK(is(area("IE", "D02 AF30"), "D02 AF30"));
    CHECK(none(area("IE", "D02 AB30")));
    CHECK(is(area("GG", "GY1 1AA"), "GY1 1AA"));
    CHECK(is(area("IM", "IM99 1AA"), "IM99 1AA"));
    CHECK(none(area("IM", "GY1 1AA")));
    CHECK(is(area("MT", "VLT 1010"), "VLT 1010"));
    CHECK(none(area("MT", "VL 1010")));
    CHECK(is(area("GH", "GA-123-4567"), "GA-123-4567"));
    CHECK(none(area("GH", "GA-12-3456")));
    CHECK(is(area("KZ", "A10A3B5"), "A10A3B5"));
    CHECK(is(area("KZ", "010000"), "010000"));
    CHECK(is(area("TW", "100048"), "100048"));
    CHECK(none(area("TW", "1004")));
    CHECK(is(area("LB", "1103 2010"), "1103 2010"));
    CHECK(is(area("CY", "99010"), "99010"));
    CHECK(none(area("CY", "98010")));
    CHECK(is(area("MC", "98000"), "98000"));
    CHECK(none(area("PW", "96941")));
    CHECK(is(area("SH", "STHL 1ZZ"), "STHL 1ZZ"));
}
