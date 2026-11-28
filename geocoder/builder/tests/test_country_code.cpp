// pack_country_code: the country_code field layout, from unsigned bytes.
#include "country_code.h"

#include "admin_rank_config.h"
#include "postcode_validation.h"
#include "test_framework.h"

TEST(country_code_packs_first_letter_high) {
    CHECK_EQ(pack_country_code('U', 'S'), uint16_t(0x5553));
    CHECK_EQ(pack_country_code('a', 'e'), uint16_t(('a' << 8) | 'e'));
    static_assert(pack_country_code('N', 'L') == 0x4E4C, "usable in case labels");
}

TEST(country_code_keeps_non_ascii_bytes_whole) {
    // A negative char once sign-extended over the first byte (and its shift
    // was undefined); both bytes now survive as they are.
    CHECK_EQ(pack_country_code('\xC3', '\x9C'), uint16_t(0xC39C));
    CHECK_EQ(pack_country_code('U', '\xFF'), uint16_t(0x55FF));
}

TEST(country_code_callers_read_the_same_packing) {
    // admin_rank_config's per-country overrides and the postcode validator's
    // switch both key on this layout.
    CHECK_EQ(admin_rank_address(pack_country_code('A', 'U'), 6), uint8_t(0));
    CHECK(!match_postcode(pack_country_code('A', 'E'), "12345", PostcodeDefault::Any));
}

TEST(a_country_of_one_language_has_it_and_one_of_several_none) {
    struct Case {
        const char* code;
        const char* language;
    };
    const Case cases[] = {{"DE", "de"}, {"US", "en"}, {"NL", "nl"}, {"CZ", "cs"}, {"TW", "zh-hant"},
                          {"BE", ""},   {"CH", ""},   {"CA", ""},   {"ZZ", ""}};
    for (const Case& c : cases) CHECK_EQ(country_language(pack_country_code(c.code[0], c.code[1])), std::string_view(c.language));
    CHECK_EQ(country_language(0), std::string_view());
}

TEST(the_countries_of_english_alone_are_country_settings_ones) {
    const char* const english[] = {"AG", "AI", "AU", "BB", "BM", "BS", "BZ", "DM", "FJ", "FK", "FM", "GB", "GD", "GG",
                                   "GH", "GI", "GM", "GS", "GY", "IM", "IO", "JE", "JM", "KI", "KN", "KY", "LC", "LR",
                                   "MS", "NG", "SB", "SH", "SL", "SS", "TC", "TO", "TT", "TV", "US", "VC", "VG", "ZM"};
    size_t found = 0;
    for (char a = 'A'; a <= 'Z'; a++)
        for (char b = 'A'; b <= 'Z'; b++) found += country_language(pack_country_code(a, b)) == "en";
    CHECK_EQ(found, std::size(english));
    for (const char* code : english) CHECK_EQ(country_language(pack_country_code(code[0], code[1])), std::string_view("en"));
}
