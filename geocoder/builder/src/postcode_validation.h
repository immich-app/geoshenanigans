#pragma once
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>

#include "country_code.h"

// From Nominatim's settings/country_settings.yaml.

// Whether the country cc (a lower-case code) has no postcodes (postcode: no),
// none of which Nominatim takes.
inline bool country_without_postcodes(const char* cc) {
    if (!cc || !cc[0] || !cc[1]) return false;
    switch (pack_country_code(cc[0], cc[1])) {
        case ('a' << 8 | 'e'):
        case ('a' << 8 | 'g'):
        case ('a' << 8 | 'o'):
        case ('b' << 8 | 'f'):
        case ('b' << 8 | 'i'):
        case ('b' << 8 | 'j'):
        case ('b' << 8 | 'o'):
        case ('b' << 8 | 's'):
        case ('b' << 8 | 'w'):
        case ('b' << 8 | 'z'):
        case ('c' << 8 | 'd'):
        case ('c' << 8 | 'f'):
        case ('c' << 8 | 'g'):
        case ('c' << 8 | 'i'):
        case ('c' << 8 | 'k'):
        case ('c' << 8 | 'm'):
        case ('d' << 8 | 'j'):
        case ('d' << 8 | 'm'):
        case ('e' << 8 | 'r'):
        case ('f' << 8 | 'j'):
        case ('g' << 8 | 'a'):
        case ('g' << 8 | 'd'):
        case ('g' << 8 | 'm'):
        case ('g' << 8 | 'q'):
        case ('g' << 8 | 'y'):
        case ('j' << 8 | 'm'):
        case ('k' << 8 | 'i'):
        case ('k' << 8 | 'm'):
        case ('k' << 8 | 'p'):
        case ('l' << 8 | 'y'):
        case ('m' << 8 | 'l'):
        case ('m' << 8 | 'r'):
        case ('m' << 8 | 'w'):
        case ('n' << 8 | 'r'):
        case ('n' << 8 | 'u'):
        case ('q' << 8 | 'a'):
        case ('r' << 8 | 'w'):
        case ('s' << 8 | 'b'):
        case ('s' << 8 | 'c'):
        case ('s' << 8 | 'l'):
        case ('s' << 8 | 'r'):
        case ('s' << 8 | 's'):
        case ('s' << 8 | 't'):
        case ('s' << 8 | 'y'):
        case ('t' << 8 | 'd'):
        case ('t' << 8 | 'g'):
        case ('t' << 8 | 'k'):
        case ('t' << 8 | 'l'):
        case ('t' << 8 | 'o'):
        case ('t' << 8 | 'v'):
        case ('u' << 8 | 'g'):
        case ('v' << 8 | 'u'):
        case ('y' << 8 | 'e'):
        case ('z' << 8 | 'w'):
            return true;
        default:
            return false;
    }
}

// Whether `bare`, a postcode in upper case past its spaces and hyphens, is
// of the pattern of the country `key` (pack_country_code of its lower-case
// code), as its spaces and hyphens leave it: dd[ -]?ddd as ddddd.
inline bool postcode_pattern_takes(uint16_t key, const std::string& bare) {
    const size_t len = bare.size();
    auto is_d = [](char c) { return c >= '0' && c <= '9'; };
    auto is_l = [](char c) { return c >= 'A' && c <= 'Z'; };
    auto digits = [&](size_t from, size_t to) { return std::all_of(bare.begin() + from, bare.begin() + to, is_d); };
    auto letters = [&](size_t from, size_t to) { return std::all_of(bare.begin() + from, bare.begin() + to, is_l); };
    // Nigeria's second pattern, (ll)[ -]?(dd)[ -]?([A-Z0-9]{3})[ -]?(ll)[ -]?(dd),
    // is 11 characters past its separators, over the cap of every other, and
    // so is Iran's (ddddd)[-_ ]?(ddddd) with an underscore.
    if (key == pack_country_code('n', 'g') && len == 11)
        return letters(0, 2) && digits(2, 4) &&
               std::all_of(bare.begin() + 4, bare.begin() + 7, [&](char c) { return is_l(c) || is_d(c); }) &&
               letters(7, 9) && digits(9, 11);
    if (key == pack_country_code('i', 'r') && len == 11) return digits(0, 5) && bare[5] == '_' && digits(6, 11);
    if (len == 0 || len > 10) return false;
    // Argentina's l?dddd(?:lll)?.
    auto argentine = [&] {
        const size_t at = is_l(bare[0]) ? 1 : 0;
        return len >= at + 4 && digits(at, at + 4) && (len == at + 4 || (len == at + 7 && letters(at + 4, at + 7)));
    };
    // Guernsey's, the Isle of Man's and Jersey's (XXdd?) ?(dll), XX their prefix.
    auto crown_dependency = [&](const char* prefix) {
        return (len == 6 || len == 7) && bare.compare(0, 2, prefix) == 0 && digits(2, len - 2) && letters(len - 2, len);
    };
    switch (key) {
        case ('a' << 8 | 'd'): return len == 3 && digits(0, 3); // (ddd)
        case ('a' << 8 | 'f'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('a' << 8 | 'i'): return bare == "2640"; // 2640
        case ('a' << 8 | 'l'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('a' << 8 | 'm'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('a' << 8 | 'r'): return argentine(); // l?dddd(?:lll)?
        case ('a' << 8 | 't'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('a' << 8 | 'u'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('a' << 8 | 'z'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('b' << 8 | 'a'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('b' << 8 | 'b'): return len == 5 && digits(0, 5); // (ddddd)
        case ('b' << 8 | 'd'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('b' << 8 | 'e'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('b' << 8 | 'g'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('b' << 8 | 'h'): return (len == 3 || len == 4) && std::all_of(bare.begin(), bare.end(), is_d);
        case ('b' << 8 | 'm'): return len == 4 && letters(0, 2) && digits(2, 4); // (ll)[ -]?(dd)
        case ('b' << 8 | 'n'): return len == 6 && letters(0, 2) && digits(2, 6); // (ll) ?(dddd)
        case ('b' << 8 | 'r'): return len == 8 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('b' << 8 | 't'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('b' << 8 | 'y'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('c' << 8 | 'a'): return len == 6 && is_l(bare[0]) && is_d(bare[1]) && is_l(bare[2]) && is_d(bare[3]) && is_l(bare[4]) && is_d(bare[5]);
        case ('c' << 8 | 'h'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('c' << 8 | 'l'): return len == 7 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('c' << 8 | 'n'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('c' << 8 | 'o'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('c' << 8 | 'r'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('c' << 8 | 'u'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('c' << 8 | 'v'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('c' << 8 | 'y'): return (len == 4 && digits(0, 4)) || (len == 5 && bare[0] == '9' && bare[1] == '9' && digits(2, 5)); // (?:99|d)ddd
        case ('c' << 8 | 'z'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('d' << 8 | 'e'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('d' << 8 | 'k'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('d' << 8 | 'o'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('d' << 8 | 'z'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('e' << 8 | 'c'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('e' << 8 | 'e'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('e' << 8 | 'g'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('e' << 8 | 's'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('e' << 8 | 't'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('f' << 8 | 'i'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('f' << 8 | 'k'): return bare == "FIQQ1ZZ"; // FIQQ ?1ZZ
        case ('f' << 8 | 'm'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('f' << 8 | 'o'): return len == 3 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('f' << 8 | 'r'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('g' << 8 | 'b'): return len >= 5 && len <= 7 && is_d(bare[len-3]) && is_l(bare[len-2]) && is_l(bare[len-1]);
        case ('g' << 8 | 'e'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('g' << 8 | 'g'): return crown_dependency("GY"); // (GYdd?) ?(dll)
        case ('g' << 8 | 'h'): return (len == 9 || len == 10) && letters(0, 2) && digits(2, len); // ll-d?ddd-dddd
        case ('g' << 8 | 'i'): return bare == "GX111AA"; // (GX11) ?(1AA)
        case ('g' << 8 | 'l'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('g' << 8 | 'n'): return len == 3 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('g' << 8 | 'r'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('g' << 8 | 's'): return bare == "SIQQ1ZZ"; // (SIQQ) ?(1ZZ)
        case ('g' << 8 | 't'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('g' << 8 | 'w'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('h' << 8 | 'n'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('h' << 8 | 'r'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('h' << 8 | 't'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('h' << 8 | 'u'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('i' << 8 | 'd'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('i' << 8 | 'e'): return len == 7 && is_l(bare[0]) && digits(1, 3) && std::all_of(bare.begin() + 3, bare.end(), [](char c) { return std::strchr("0123456789ACDEFHKNPRTVWXY", c) != nullptr; }); // (ldd) ?([0123456789ACDEFHKNPRTVWXY]{4})
        case ('i' << 8 | 'l'): return len == 7 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('i' << 8 | 'm'): return crown_dependency("IM"); // (IMdd?) ?(dll)
        case ('i' << 8 | 'n'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('i' << 8 | 'o'): return bare == "BBND1ZZ"; // (BBND) ?(1ZZ)
        case ('i' << 8 | 'q'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('i' << 8 | 'r'): return len == 10 && digits(0, 10); // (ddddd)[-_ ]?(ddddd)
        case ('i' << 8 | 's'): return len == 3 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('i' << 8 | 't'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('j' << 8 | 'e'): return crown_dependency("JE"); // (JEdd?) ?(dll)
        case ('j' << 8 | 'o'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('j' << 8 | 'p'): return len == 7 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('k' << 8 | 'e'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('k' << 8 | 'g'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('k' << 8 | 'h'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('k' << 8 | 'n'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('k' << 8 | 'r'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('k' << 8 | 'w'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('k' << 8 | 'y'): return len == 5 && digits(0, 5); // (d)-(dddd)
        case ('k' << 8 | 'z'): return (len == 7 && is_l(bare[0]) && digits(1, 3) && is_l(bare[3]) && is_d(bare[4]) && is_l(bare[5]) && is_d(bare[6])) || (len == 6 && digits(0, 6)); // (?:lddldld|dddddd)
        case ('l' << 8 | 'a'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('l' << 8 | 'b'): return (len == 4 || len == 8) && digits(0, len); // (dddd)(?: ?dddd)?
        case ('l' << 8 | 'c'): return len == 5 && digits(0, 5); // (dd) ?(ddd)
        case ('l' << 8 | 'i'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('l' << 8 | 'k'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('l' << 8 | 'r'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('l' << 8 | 's'): return len == 3 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('l' << 8 | 't'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('l' << 8 | 'u'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('l' << 8 | 'v'): return len == 4 && digits(0, 4); // (dddd)
        case ('m' << 8 | 'a'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'c'): return len == 5 && bare.compare(0, 3, "980") == 0 && digits(3, 5); // 980dd
        case ('m' << 8 | 'd'): return len == 4 && digits(0, 4); // (dddd)
        case ('m' << 8 | 'e'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'g'): return len == 3 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'h'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'k'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'm'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'n'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 't'): return len == 7 && letters(0, 3) && digits(3, 7); // (lll) ?(dddd)
        case ('m' << 8 | 'u'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'v'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'x'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'y'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'z'): return (len == 4 || len == 6) && digits(0, len); // (dddd)(?:-dd)?
        case ('n' << 8 | 'a'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('n' << 8 | 'e'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('n' << 8 | 'g'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d); // or the lettered pattern above
        case ('n' << 8 | 'i'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('n' << 8 | 'l'): return len == 6 && is_d(bare[0]) && is_d(bare[1]) && is_d(bare[2]) && is_d(bare[3]) && is_l(bare[4]) && is_l(bare[5]);
        case ('n' << 8 | 'o'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('n' << 8 | 'p'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('n' << 8 | 'z'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('o' << 8 | 'm'): return len == 3 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('p' << 8 | 'a'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('p' << 8 | 'e'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('p' << 8 | 'g'): return len == 3 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('p' << 8 | 'h'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('p' << 8 | 'k'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('p' << 8 | 'l'): return len == 5 && digits(0, 5); // (dd)[ -]?(ddd)
        case ('p' << 8 | 'n'): return bare == "PCRN1ZZ"; // (PCRN) ?(1ZZ)
        case ('p' << 8 | 's'): return len == 3 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('p' << 8 | 't'): return (len == 4 || len == 7) && digits(0, len); // dddd(?:-ddd)?
        case ('p' << 8 | 'w'): return bare == "96939" || bare == "96940"; // 969(39|40)
        case ('p' << 8 | 'y'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('r' << 8 | 'o'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('r' << 8 | 's'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('r' << 8 | 'u'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('s' << 8 | 'a'): return (len == 5 || len == 9) && digits(0, len); // ddddd(?:-dddd)?
        case ('s' << 8 | 'd'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('s' << 8 | 'e'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('s' << 8 | 'g'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('s' << 8 | 'h'): return bare == "ASCN1ZZ" || bare == "STHL1ZZ" || bare == "TDCU1ZZ"; // (ASCN|STHL|TDCU) ?(1ZZ)
        case ('s' << 8 | 'i'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('s' << 8 | 'k'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('s' << 8 | 'm'): return len == 5 && bare.compare(0, 4, "4789") == 0 && is_d(bare[4]); // 4789d
        case ('s' << 8 | 'n'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('s' << 8 | 'o'): return len == 7 && letters(0, 2) && digits(2, 7); // (ll) ?(ddddd)
        case ('s' << 8 | 'v'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('s' << 8 | 'z'): return len == 4 && is_l(bare[0]) && digits(1, 4); // lddd
        case ('t' << 8 | 'c'): return bare == "TKCA1ZZ"; // (TKCA) ?(1ZZ)
        case ('t' << 8 | 'h'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('t' << 8 | 'j'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('t' << 8 | 'm'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('t' << 8 | 'n'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('t' << 8 | 'r'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('t' << 8 | 't'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('t' << 8 | 'w'): return (len == 3 || len == 5 || len == 6) && digits(0, len); // ddd(?:ddd?)?
        case ('t' << 8 | 'z'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('u' << 8 | 'a'): return (len == 5 || len == 6) && std::all_of(bare.begin(), bare.end(), is_d);
        case ('u' << 8 | 's'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('u' << 8 | 'y'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('u' << 8 | 'z'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('v' << 8 | 'a'): return bare == "00120"; // 00120
        case ('v' << 8 | 'c'): return len == 4 && digits(0, 4); // (dddd)
        case ('v' << 8 | 'e'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('v' << 8 | 'g'): return len == 4 && digits(0, 4); // (dddd)
        case ('v' << 8 | 'n'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('x' << 8 | 'k'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('z' << 8 | 'a'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('z' << 8 | 'm'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        default: return len >= 3 && len <= 10;
    }
}

// Whether the postcode pc is one the pattern of the country cc (a lower-case
// code) takes, as CountryPostcodeMatcher.match does: in upper case, past its
// spaces and hyphens and an optional code of the country before it
// (\s*(?:CC[ -]?)?(pattern)\s*); none of a country without postcodes.
inline bool validate_postcode_for_country(const char* cc, const char* pc) {
    if (!cc || !cc[0] || !cc[1] || !pc || !pc[0] || country_without_postcodes(cc)) return false;
    const uint16_t key = pack_country_code(cc[0], cc[1]);
    std::string bare;
    for (const char* p = pc; *p; p++) {
        char c = *p;
        if (c >= 'a' && c <= 'z') c -= 32;
        if (c != ' ' && c != '-') bare += c;
    }
    if (postcode_pattern_takes(key, bare)) return true;
    auto upper = [](char c) { return c >= 'a' && c <= 'z' ? static_cast<char>(c - 32) : c; };
    return bare.size() > 2 && bare[0] == upper(cc[0]) && bare[1] == upper(cc[1]) &&
           postcode_pattern_takes(key, bare.substr(2));
}

// Whether a postcode is a US ZIP+4, ddddd-dddd: the US pattern is
// (ddddd)(?:-dddd)? with output \1 (country_settings.yaml), so Nominatim's
// postcode is the ZIP's.
inline bool is_zip_plus_4(const char* postcode) {
    for (int k = 0; k < 10; k++)
        if (k == 5 ? postcode[k] != '-' : !(postcode[k] >= '0' && postcode[k] <= '9')) return false;
    return postcode[10] == 0;
}

// Whether a feature's own postcode tag pc is one the clean-postcodes
// sanitizer keeps in the country cc (lower-case), as CountryPostcodeMatcher
// matches it: past the spaces round it and an optional code of the country
// with a space or a hyphen after it, a postcode the country's pattern takes,
// a ZIP+4 in the US. One it rejects becomes an address term
// (unofficial_postcode), no postcode of the feature's.
inline bool postcode_tag_valid_for_country(const char* cc, const char* pc) {
    if (!cc || !cc[0] || !cc[1] || !pc) return false;
    auto white = [](char c) { return c == ' ' || (c >= '\t' && c <= '\r'); };
    const char* begin = pc;
    while (white(*begin)) begin++;
    const char* end = begin + std::strlen(begin);
    while (end > begin && white(end[-1])) end--;
    const std::string tag(begin, end);
    const bool us = cc[0] == 'u' && cc[1] == 's';
    auto takes = [&](const char* p) { return validate_postcode_for_country(cc, p) || (us && is_zip_plus_4(p)); };
    if (takes(tag.c_str())) return true;
    auto upper = [](char c) { return c >= 'a' && c <= 'z' ? static_cast<char>(c - 32) : c; };
    if (tag.size() < 2 || upper(tag[0]) != upper(cc[0]) || upper(tag[1]) != upper(cc[1])) return false;
    const size_t rest = tag.size() > 2 && (tag[2] == ' ' || tag[2] == '-') ? 3 : 2;
    return takes(tag.c_str() + rest);
}
