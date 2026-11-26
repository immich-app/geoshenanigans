#pragma once

#include <cstdint>

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
