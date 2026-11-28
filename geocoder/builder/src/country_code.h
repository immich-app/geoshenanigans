#pragma once

#include <cstdint>
#include <string_view>

// A two-letter country code packed as (first << 8) | second, the layout of
// every country_code field. The bytes are taken unsigned: a plain char from
// an OSM tag or a CSV can be negative (a non-ASCII byte), and shifting a
// negative value is undefined.
constexpr uint16_t pack_country_code(char first, char second) {
    return static_cast<uint16_t>((static_cast<unsigned char>(first) << 8) | static_cast<unsigned char>(second));
}

// The country code of an OSM tag, its ASCII letters upper-cased: Nominatim
// lower-cases every code it reads, so a tag's case means nothing, and the
// builder's own codes are upper case.
constexpr uint16_t pack_tag_country_code(char first, char second) {
    auto upper = [](char c) { return c >= 'a' && c <= 'z' ? static_cast<char>(c - 'a' + 'A') : c; };
    return pack_country_code(upper(first), upper(second));
}

// The language of a country of one language, as country_settings.yaml lists
// its languages, which Nominatim takes as the country's default language
// (country_info.py); "" for a country of several or none.
inline std::string_view country_language(uint16_t cc) {
    struct Row {
        char code[3];
        const char* language;
    };
    static constexpr Row kRows[] = {
    {"AD", "ca"}, {"AE", "ar"}, {"AG", "en"}, {"AI", "en"}, {"AL", "sq"}, {"AM", "hy"}, {"AO", "pt"}, {"AR", "es"},
    {"AT", "de"}, {"AU", "en"}, {"AZ", "az"}, {"BB", "en"}, {"BD", "bn"}, {"BF", "fr"}, {"BG", "bg"}, {"BH", "ar"},
    {"BI", "fr"}, {"BJ", "fr"}, {"BM", "en"}, {"BN", "ms"}, {"BR", "pt"}, {"BS", "en"}, {"BT", "dz"}, {"BZ", "en"},
    {"CD", "fr"}, {"CG", "fr"}, {"CI", "fr"}, {"CL", "es"}, {"CN", "zh"}, {"CO", "es"}, {"CR", "es"}, {"CU", "es"},
    {"CV", "pt"}, {"CZ", "cs"}, {"DE", "de"}, {"DK", "da"}, {"DM", "en"}, {"DO", "es"}, {"EC", "es"}, {"EE", "et"},
    {"EG", "ar"}, {"FJ", "en"}, {"FK", "en"}, {"FM", "en"}, {"FR", "fr"}, {"GA", "fr"}, {"GB", "en"}, {"GD", "en"},
    {"GE", "ka"}, {"GG", "en"}, {"GH", "en"}, {"GI", "en"}, {"GM", "en"}, {"GN", "fr"}, {"GR", "el"}, {"GS", "en"},
    {"GT", "es"}, {"GW", "pt"}, {"GY", "en"}, {"HN", "es"}, {"HR", "hr"}, {"HU", "hu"}, {"ID", "id"}, {"IL", "he"},
    {"IM", "en"}, {"IO", "en"}, {"IR", "fa"}, {"IS", "is"}, {"JE", "en"}, {"JM", "en"}, {"JO", "ar"}, {"JP", "ja"},
    {"KH", "km"}, {"KI", "en"}, {"KN", "en"}, {"KP", "ko"}, {"KR", "ko"}, {"KW", "ar"}, {"KY", "en"}, {"LA", "lo"},
    {"LC", "en"}, {"LI", "de"}, {"LR", "en"}, {"LT", "lt"}, {"LV", "lv"}, {"LY", "ar"}, {"MC", "fr"}, {"MK", "mk"},
    {"ML", "fr"}, {"MM", "my"}, {"MN", "mn"}, {"MS", "en"}, {"MV", "dv"}, {"MX", "es"}, {"MY", "ms"}, {"MZ", "pt"},
    {"NE", "fr"}, {"NG", "en"}, {"NI", "es"}, {"NL", "nl"}, {"NP", "ne"}, {"OM", "ar"}, {"PA", "es"}, {"PE", "es"}, {"PL", "pl"},
    {"PT", "pt"}, {"QA", "ar"}, {"RO", "ro"}, {"RS", "sr"}, {"RU", "ru"}, {"SA", "ar"}, {"SB", "en"}, {"SE", "sv"},
    {"SH", "en"}, {"SI", "sl"}, {"SK", "sk"}, {"SL", "en"}, {"SM", "it"}, {"SN", "fr"}, {"SR", "nl"}, {"SS", "en"},
    {"ST", "pt"}, {"SV", "es"}, {"SY", "ar"}, {"TC", "en"}, {"TG", "fr"}, {"TH", "th"}, {"TM", "tk"}, {"TO", "en"},
    {"TR", "tr"}, {"TT", "en"}, {"TV", "en"}, {"TW", "zh-hant"}, {"UA", "uk"}, {"US", "en"}, {"UY", "es"},
    {"VA", "it"}, {"VC", "en"}, {"VE", "es"}, {"VG", "en"}, {"VN", "vi"}, {"YE", "ar"}, {"ZM", "en"},
    };
    for (const Row& r : kRows)
        if (pack_country_code(r.code[0], r.code[1]) == cc) return r.language;
    return {};
}
