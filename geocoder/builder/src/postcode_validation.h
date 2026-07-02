#pragma once
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>

// Auto-generated from Nominatim settings/country_settings.yaml
// Validates postcodes against per-country patterns.
// cc must be lowercase 2-char country code.
inline bool validate_postcode_for_country(const char* cc, const char* pc) {
    if (!cc || !cc[0] || !cc[1] || !pc || !pc[0]) return false;
    uint16_t key = (static_cast<uint16_t>(cc[0]) << 8) | cc[1];
    std::string bare;
    for (const char* p = pc; *p; p++) {
        char c = *p;
        if (c >= 'a' && c <= 'z') c -= 32;
        if (c != ' ' && c != '-') bare += c;
    }
    size_t len = bare.size();
    if (len == 0 || len > 10) return false;
    auto is_d = [](char c) { return c >= '0' && c <= '9'; };
    auto is_l = [](char c) { return c >= 'A' && c <= 'Z'; };
    (void)is_d; (void)is_l;
    switch (key) {
        case ('a' << 8 | 'e'): return false;
        case ('a' << 8 | 'g'): return false;
        case ('a' << 8 | 'o'): return false;
        case ('b' << 8 | 'f'): return false;
        case ('b' << 8 | 'i'): return false;
        case ('b' << 8 | 'j'): return false;
        case ('b' << 8 | 'o'): return false;
        case ('b' << 8 | 's'): return false;
        case ('b' << 8 | 'w'): return false;
        case ('b' << 8 | 'z'): return false;
        case ('c' << 8 | 'd'): return false;
        case ('c' << 8 | 'f'): return false;
        case ('c' << 8 | 'g'): return false;
        case ('c' << 8 | 'i'): return false;
        case ('c' << 8 | 'k'): return false;
        case ('c' << 8 | 'm'): return false;
        case ('d' << 8 | 'j'): return false;
        case ('d' << 8 | 'm'): return false;
        case ('e' << 8 | 'r'): return false;
        case ('f' << 8 | 'j'): return false;
        case ('g' << 8 | 'a'): return false;
        case ('g' << 8 | 'd'): return false;
        case ('g' << 8 | 'm'): return false;
        case ('g' << 8 | 'q'): return false;
        case ('g' << 8 | 'y'): return false;
        case ('j' << 8 | 'm'): return false;
        case ('k' << 8 | 'i'): return false;
        case ('k' << 8 | 'm'): return false;
        case ('k' << 8 | 'p'): return false;
        case ('l' << 8 | 'y'): return false;
        case ('m' << 8 | 'l'): return false;
        case ('m' << 8 | 'r'): return false;
        case ('m' << 8 | 'w'): return false;
        case ('n' << 8 | 'r'): return false;
        case ('n' << 8 | 'u'): return false;
        case ('q' << 8 | 'a'): return false;
        case ('r' << 8 | 'w'): return false;
        case ('s' << 8 | 'b'): return false;
        case ('s' << 8 | 'c'): return false;
        case ('s' << 8 | 'l'): return false;
        case ('s' << 8 | 'r'): return false;
        case ('s' << 8 | 's'): return false;
        case ('s' << 8 | 't'): return false;
        case ('s' << 8 | 'y'): return false;
        case ('t' << 8 | 'd'): return false;
        case ('t' << 8 | 'g'): return false;
        case ('t' << 8 | 'k'): return false;
        case ('t' << 8 | 'l'): return false;
        case ('t' << 8 | 'o'): return false;
        case ('t' << 8 | 'v'): return false;
        case ('u' << 8 | 'g'): return false;
        case ('v' << 8 | 'u'): return false;
        case ('y' << 8 | 'e'): return false;
        case ('z' << 8 | 'w'): return false;
        case ('a' << 8 | 'd'): return true; // (ddd)
        case ('a' << 8 | 'f'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('a' << 8 | 'i'): return true; // 2640
        case ('a' << 8 | 'l'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('a' << 8 | 'm'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('a' << 8 | 'r'): return true; // l?dddd(?:lll)?
        case ('a' << 8 | 't'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('a' << 8 | 'u'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('a' << 8 | 'z'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('b' << 8 | 'a'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('b' << 8 | 'b'): return true; // (ddddd)
        case ('b' << 8 | 'd'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('b' << 8 | 'e'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('b' << 8 | 'g'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('b' << 8 | 'h'): return (len == 4 || len == 5) && std::all_of(bare.begin(), bare.end(), is_d);
        case ('b' << 8 | 'm'): return true; // (ll)[ -]?(dd)
        case ('b' << 8 | 'n'): return true; // (ll) ?(dddd)
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
        case ('c' << 8 | 'y'): return true; // (?:99|d)ddd
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
        case ('f' << 8 | 'k'): return true; // FIQQ 1ZZ
        case ('f' << 8 | 'm'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('f' << 8 | 'o'): return len == 3 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('f' << 8 | 'r'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('g' << 8 | 'b'): return len >= 5 && len <= 7 && is_d(bare[len-3]) && is_l(bare[len-2]) && is_l(bare[len-1]);
        case ('g' << 8 | 'e'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('g' << 8 | 'g'): return true; // (GYdd?) ?(dll)
        case ('g' << 8 | 'h'): return true; // ll-d?ddd-dddd
        case ('g' << 8 | 'i'): return true; // (GX11) ?(1AA)
        case ('g' << 8 | 'l'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('g' << 8 | 'n'): return len == 3 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('g' << 8 | 'r'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('g' << 8 | 's'): return true; // (SIQQ) ?(1ZZ)
        case ('g' << 8 | 't'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('g' << 8 | 'w'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('h' << 8 | 'n'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('h' << 8 | 'r'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('h' << 8 | 't'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('h' << 8 | 'u'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('i' << 8 | 'd'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('i' << 8 | 'e'): return true; // (ldd) ?([0123456789ACDEFHKNPRTVWXY]{4})
        case ('i' << 8 | 'l'): return len == 7 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('i' << 8 | 'm'): return true; // (IMdd?) ?(dll)
        case ('i' << 8 | 'n'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('i' << 8 | 'o'): return true; // (BBND) ?(1ZZ)
        case ('i' << 8 | 'q'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('i' << 8 | 'r'): return true; // (ddddd)[-_ ]?(ddddd)
        case ('i' << 8 | 's'): return len == 3 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('i' << 8 | 't'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('j' << 8 | 'e'): return true; // (JEdd?) ?(dll)
        case ('j' << 8 | 'o'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('j' << 8 | 'p'): return len == 7 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('k' << 8 | 'e'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('k' << 8 | 'g'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('k' << 8 | 'h'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('k' << 8 | 'n'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('k' << 8 | 'r'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('k' << 8 | 'w'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('k' << 8 | 'y'): return true; // (d)-(dddd)
        case ('k' << 8 | 'z'): return true; // (?:lddldld|dddddd)
        case ('l' << 8 | 'a'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('l' << 8 | 'b'): return true; // (dddd)(?: ?dddd)?
        case ('l' << 8 | 'c'): return true; // (dd) ?(ddd)
        case ('l' << 8 | 'i'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('l' << 8 | 'k'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('l' << 8 | 'r'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('l' << 8 | 's'): return len == 3 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('l' << 8 | 't'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('l' << 8 | 'u'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('l' << 8 | 'v'): return true; // (dddd)
        case ('m' << 8 | 'a'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'c'): return true; // 980dd
        case ('m' << 8 | 'd'): return true; // (dddd)
        case ('m' << 8 | 'e'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'g'): return len == 3 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'h'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'k'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'm'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'n'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 't'): return true; // (lll) ?(dddd)
        case ('m' << 8 | 'u'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'v'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'x'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'y'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('m' << 8 | 'z'): return true; // (dddd)(?:-dd)?
        case ('n' << 8 | 'a'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('n' << 8 | 'e'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('n' << 8 | 'g'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
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
        case ('p' << 8 | 'l'): return true; // (dd)[ -]?(ddd)
        case ('p' << 8 | 'n'): return true; // (PCRN) ?(1ZZ)
        case ('p' << 8 | 's'): return len == 3 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('p' << 8 | 't'): return true; // dddd(?:-ddd)?
        case ('p' << 8 | 'w'): return true; // 969(39|40)
        case ('p' << 8 | 'y'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('r' << 8 | 'o'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('r' << 8 | 's'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('r' << 8 | 'u'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('s' << 8 | 'a'): return true; // ddddd(?:-dddd)?
        case ('s' << 8 | 'd'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('s' << 8 | 'e'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('s' << 8 | 'g'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('s' << 8 | 'h'): return true; // (ASCN|STHL|TDCU) ?(1ZZ)
        case ('s' << 8 | 'i'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('s' << 8 | 'k'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('s' << 8 | 'm'): return true; // 4789d
        case ('s' << 8 | 'n'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('s' << 8 | 'o'): return true; // (ll) ?(ddddd)
        case ('s' << 8 | 'v'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('s' << 8 | 'z'): return true; // lddd
        case ('t' << 8 | 'c'): return true; // (TKCA) ?(1ZZ)
        case ('t' << 8 | 'h'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('t' << 8 | 'j'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('t' << 8 | 'm'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('t' << 8 | 'n'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('t' << 8 | 'r'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('t' << 8 | 't'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('t' << 8 | 'w'): return true; // ddd(?:ddd?)?
        case ('t' << 8 | 'z'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('u' << 8 | 'a'): return (len == 6 || len == 7) && std::all_of(bare.begin(), bare.end(), is_d);
        case ('u' << 8 | 's'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('u' << 8 | 'y'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('u' << 8 | 'z'): return len == 6 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('v' << 8 | 'a'): return true; // 00120
        case ('v' << 8 | 'c'): return true; // (dddd)
        case ('v' << 8 | 'e'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('v' << 8 | 'g'): return true; // (dddd)
        case ('v' << 8 | 'n'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('x' << 8 | 'k'): return len == 5 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('z' << 8 | 'a'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        case ('z' << 8 | 'm'): return len == 4 && std::all_of(bare.begin(), bare.end(), is_d);
        default: return len >= 3 && len <= 10;
    }
}
