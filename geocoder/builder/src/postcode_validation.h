#pragma once
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <optional>
#include <regex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "country_code.h"

// From Nominatim's settings/country_settings.yaml, matched as its
// src/nominatim_db/data/postcode_format.py matches them.

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

// A postcode entry of country_settings.yaml as written there: the country
// (lower case), its pattern (d a digit, l a letter) and its output, null for
// none (\g<0>, the match). A country of a list of patterns
// (ListPostcodeMatcher) has an entry of each, in the list's order; a country
// of no entry and of postcodes takes the default.
struct PostcodeFormat {
    const char* cc;
    const char* pattern;
    const char* output;
};
inline constexpr PostcodeFormat kPostcodeFormats[] = {
    {"ad", "(ddd)", "AD\\1"},
    {"af", "dddd", nullptr},
    {"ai", "2640", "AI-2640"},
    {"al", "dddd", nullptr},
    {"am", "dddd", nullptr},
    {"ar", "l?dddd(?:lll)?", nullptr},
    {"at", "dddd", nullptr},
    {"au", "dddd", nullptr},
    {"az", "dddd", nullptr},
    {"ba", "ddddd", nullptr},
    {"bb", "(ddddd)", "BB\\1"},
    {"bd", "dddd", nullptr},
    {"be", "dddd", nullptr},
    {"bg", "dddd", nullptr},
    {"bh", "d?ddd", nullptr},
    {"bm", "(ll)[ -]?(dd)", "\\1 \\2"},
    {"bn", "(ll) ?(dddd)", "\\1\\2"},
    {"br", "(ddddd)-?(ddd)", "\\1-\\2"},
    {"bt", "ddddd", nullptr},
    {"by", "dddddd", nullptr},
    {"ca", "(ldl) ?(dld)", "\\1 \\2"},
    {"ch", "dddd", nullptr},
    {"cl", "ddddddd", nullptr},
    {"cn", "dddddd", nullptr},
    {"co", "dddddd", nullptr},
    {"cr", "ddddd", nullptr},
    {"cu", "ddddd", nullptr},
    {"cv", "dddd", nullptr},
    {"cy", "(?:99|d)ddd", nullptr},
    {"cz", "(ddd) ?(dd)", "\\1 \\2"},
    {"de", "ddddd", nullptr},
    {"dk", "dddd", nullptr},
    {"do", "ddddd", nullptr},
    {"dz", "ddddd", nullptr},
    {"ec", "dddddd", nullptr},
    {"ee", "ddddd", nullptr},
    {"eg", "ddddd", nullptr},
    {"es", "ddddd", nullptr},
    {"et", "dddd", nullptr},
    {"fi", "ddddd", nullptr},
    {"fk", "FIQQ ?1ZZ", "FIQQ 1ZZ"},
    {"fm", "ddddd", nullptr},
    {"fo", "ddd", nullptr},
    {"fr", "ddddd", nullptr},
    {"gb", "(l?ld[A-Z0-9]?) ?(dll)", "\\1 \\2"},
    {"ge", "dddd", nullptr},
    {"gg", "(GYdd?) ?(dll)", "\\1 \\2"},
    {"gh", "ll-d?ddd-dddd", nullptr},
    {"gi", "(GX11) ?(1AA)", "GX11 1AA"},
    {"gl", "dddd", nullptr},
    {"gn", "ddd", nullptr},
    {"gr", "(ddd) ?(dd)", "\\1 \\2"},
    {"gs", "(SIQQ) ?(1ZZ)", "\\1 \\2"},
    {"gt", "ddddd", nullptr},
    {"gw", "dddd", nullptr},
    {"hn", "ddddd", nullptr},
    {"hr", "ddddd", nullptr},
    {"ht", "dddd", nullptr},
    {"hu", "dddd", nullptr},
    {"id", "ddddd", nullptr},
    {"ie", "(ldd) ?([0123456789ACDEFHKNPRTVWXY]{4})", "\\1 \\2"},
    {"il", "ddddddd", nullptr},
    {"im", "(IMdd?) ?(dll)", "\\1 \\2"},
    {"in", "(ddd) ?(ddd)", "\\1\\2"},
    {"io", "(BBND) ?(1ZZ)", "\\1 \\2"},
    {"iq", "ddddd", nullptr},
    {"ir", "(ddddd)[-_ ]?(ddddd)", "\\1-\\2"},
    {"is", "ddd", nullptr},
    {"it", "ddddd", nullptr},
    {"je", "(JEdd?) ?(dll)", "\\1 \\2"},
    {"jo", "ddddd", nullptr},
    {"jp", "(ddd)-?(dddd)", "\\1-\\2"},
    {"ke", "ddddd", nullptr},
    {"kg", "dddddd", nullptr},
    {"kh", "dddddd", nullptr},
    {"kn", "dddd", nullptr},
    {"kr", "ddddd", nullptr},
    {"kw", "ddddd", nullptr},
    {"ky", "(d)-(dddd)", "KY\\1-\\2"},
    {"kz", "(?:lddldld|dddddd)", nullptr},
    {"la", "ddddd", nullptr},
    {"lb", "(dddd)(?: ?dddd)?", nullptr},
    {"lc", "(dd) ?(ddd)", "LC\\1 \\2"},
    {"li", "dddd", nullptr},
    {"lk", "ddddd", nullptr},
    {"lr", "dddd", nullptr},
    {"ls", "ddd", nullptr},
    {"lt", "ddddd", nullptr},
    {"lu", "dddd", nullptr},
    {"lv", "(dddd)", "LV-\\1"},
    {"ma", "ddddd", nullptr},
    {"mc", "980dd", nullptr},
    {"md", "(dddd)", "MD-\\1"},
    {"me", "ddddd", nullptr},
    {"mg", "ddd", nullptr},
    {"mh", "ddddd", nullptr},
    {"mk", "dddd", nullptr},
    {"mm", "ddddd", nullptr},
    {"mn", "ddddd", nullptr},
    {"mt", "(lll) ?(dddd)", "\\1 \\2"},
    {"mu", "ddddd", nullptr},
    {"mv", "ddddd", nullptr},
    {"mx", "ddddd", nullptr},
    {"my", "ddddd", nullptr},
    {"mz", "(dddd)(?:-dd)?", nullptr},
    {"na", "ddddd", nullptr},
    {"ne", "dddd", nullptr},
    {"ng", "dddddd", nullptr},
    {"ng", "(ll)[ -]?(dd)[ -]?([A-Z0-9]{3})[ -]?(ll)[ -]?(dd)", "\\1-\\2-\\3-\\4-\\5"},
    {"ni", "ddddd", nullptr},
    {"nl", "(dddd) ?(ll)", "\\1 \\2"},
    {"no", "dddd", nullptr},
    {"np", "ddddd", nullptr},
    {"nz", "dddd", nullptr},
    {"om", "ddd", nullptr},
    {"pa", "dddd", nullptr},
    {"pe", "ddddd", nullptr},
    {"pg", "ddd", nullptr},
    {"ph", "dddd", nullptr},
    {"pk", "ddddd", nullptr},
    {"pl", "(dd)[ -]?(ddd)", "\\1-\\2"},
    {"pn", "(PCRN) ?(1ZZ)", "\\1 \\2"},
    {"ps", "ddd", nullptr},
    {"pt", "dddd(?:-ddd)?", nullptr},
    {"pw", "969(39|40)", nullptr},
    {"py", "dddddd", nullptr},
    {"ro", "dddddd", nullptr},
    {"rs", "ddddd", nullptr},
    {"ru", "dddddd", nullptr},
    {"sa", "ddddd(?:-dddd)?", nullptr},
    {"sd", "ddddd", nullptr},
    {"se", "(ddd) ?(dd)", "\\1 \\2"},
    {"sg", "dddddd", nullptr},
    {"sh", "(ASCN|STHL|TDCU) ?(1ZZ)", "\\1 \\2"},
    {"si", "dddd", nullptr},
    {"sk", "(ddd) ?(dd)", "\\1 \\2"},
    {"sm", "4789d", nullptr},
    {"sn", "ddddd", nullptr},
    {"so", "(ll) ?(ddddd)", "\\1 \\2"},
    {"sv", "dddd", nullptr},
    {"sz", "lddd", nullptr},
    {"tc", "(TKCA) ?(1ZZ)", "\\1 \\2"},
    {"th", "ddddd", nullptr},
    {"tj", "dddddd", nullptr},
    {"tm", "dddddd", nullptr},
    {"tn", "dddd", nullptr},
    {"tr", "ddddd", nullptr},
    {"tt", "dddddd", nullptr},
    {"tw", "ddd(?:ddd?)?", nullptr},
    {"tz", "ddddd", nullptr},
    {"ua", "d?ddddd", nullptr},
    {"us", "(ddddd)(?:-dddd)?", "\\1"},
    {"uy", "ddddd", nullptr},
    {"uz", "dddddd", nullptr},
    {"va", "00120", nullptr},
    {"vc", "(dddd)", "VC\\1"},
    {"ve", "dddd", nullptr},
    {"vg", "(dddd)", "VG\\1"},
    {"vn", "ddddd", nullptr},
    {"xk", "ddddd", nullptr},
    {"za", "dddd", nullptr},
    {"zm", "dddd", nullptr},
};

// The pattern of a country without one of its own: PostcodeFormatter's
// default, .*, which update_postcodes keeps for postal areas and centroids,
// or the clean-postcodes sanitizer's default-pattern, [A-Z0-9- ]{3,12}
// (settings/sanitizers.yaml), for a feature's own tag.
enum class PostcodeDefault : uint8_t { Any, Sanitizer };

namespace postcode_detail {

// Python's \s in a str pattern, over UTF-8: the ASCII white space and
// \x1c-\x1f, U+0085, U+00A0, U+1680, U+2000-U+200A, U+2028, U+2029, U+202F,
// U+205F and U+3000.
inline const std::string kWhite =
    "(?:[\\t\\n\\v\\f\\r \\x1c-\\x1f]|\\xc2[\\x85\\xa0]|\\xe1\\x9a\\x80|"
    "\\xe2\\x80[\\x80\\x81\\x82\\x83\\x84\\x85\\x86\\x87\\x88\\x89\\x8a\\xa8\\xa9\\xaf]|\\xe2\\x81\\x9f|\\xe3\\x80\\x80)*";

// A SimplePostcodeMatcher: norm_pattern \s*(?:CC[ -]?)?(pattern)\s*, the
// pattern, and the output.
struct Matcher {
    std::regex norm, pattern;
    const char* output;  // null: the match
};

inline Matcher compile(const char* cc_upper, const char* pattern, const char* output) {
    std::string pc;
    for (const char* p = pattern; *p; p++) pc += *p == 'd' ? "[0-9]" : *p == 'l' ? "[A-Z]" : std::string(1, *p);
    Matcher m{std::regex(kWhite + "(?:" + cc_upper + "[ -]?)?(" + pc + ")" + kWhite), std::regex(pc), output};
    // The outputs refer to groups as \1 to \9, one digit, which expand reads.
    for (const char* o = output ? output : ""; *o; o++) {
        if (*o != '\\') continue;
        if (o[1] < '1' || o[1] > '9' || (o[2] >= '0' && o[2] <= '9') ||
            static_cast<unsigned>(o[1] - '0') > m.pattern.mark_count())
            throw std::logic_error(std::string("postcode output of an unread escape: ") + output);
        o++;
    }
    return m;
}

// match.expand(output): the output with each \N its group N, empty where
// the group took no part.
inline std::string expand(const std::smatch& m, const char* output) {
    if (!output) return m.str(0);
    std::string out;
    for (const char* o = output; *o; o++) {
        if (*o == '\\') out += m.str(static_cast<size_t>(*++o - '0'));
        else out += *o;
    }
    return out;
}

struct Matchers {
    // By the country's pack_country_code in upper case, ascending.
    std::vector<std::pair<uint16_t, std::vector<Matcher>>> countries;
    std::vector<Matcher> any, sanitizer;

    Matchers() {
        for (const PostcodeFormat& f : kPostcodeFormats) {
            const char upper[3] = {static_cast<char>(f.cc[0] - 32), static_cast<char>(f.cc[1] - 32), 0};
            const uint16_t key = pack_country_code(upper[0], upper[1]);
            if (countries.empty() || countries.back().first != key) countries.push_back({key, {}});
            countries.back().second.push_back(compile(upper, f.pattern, f.output));
        }
        if (!std::is_sorted(countries.begin(), countries.end(),
                            [](const auto& a, const auto& b) { return a.first < b.first; }))
            throw std::logic_error("kPostcodeFormats out of country order");
        // Python's . takes any character but a line feed.
        any.push_back(compile("", "[^\\n]*", nullptr));
        sanitizer.push_back(compile("", "[A-Z0-9- ]{3,12}", nullptr));
    }

    const std::vector<Matcher>& of(uint16_t cc, PostcodeDefault fallback) const {
        const auto it = std::lower_bound(countries.begin(), countries.end(), cc,
                                         [](const auto& c, uint16_t key) { return c.first < key; });
        if (it != countries.end() && it->first == cc) return it->second;
        return fallback == PostcodeDefault::Any ? any : sanitizer;
    }
};

// Compiled once, at the first match.
inline const Matchers& matchers() {
    static const Matchers m;
    return m;
}

}  // namespace postcode_detail

// The postcode Nominatim makes of the tag pc in the country cc
// (pack_country_code of its upper-case code, 0 for none), or none where it
// takes none, as PostcodeFormatter.match: none of no country or of one
// without postcodes; else, of the country's patterns in order (its default
// where it has none), the first whose norm_pattern matches the tag in upper
// case whole and whose pattern matches its group whole, neither all zeros,
// hyphens and spaces (zero_pattern), its output of that match. Only the
// ASCII letters go to upper case, all a pattern reads: the default .* keeps
// the others of its match as they are, which Python's upper raises too.
inline std::optional<std::string> match_postcode(uint16_t cc, std::string_view pc, PostcodeDefault fallback) {
    const char lower[3] = {static_cast<char>(std::tolower(cc >> 8)), static_cast<char>(std::tolower(cc & 0xFF)), 0};
    if (cc == 0 || country_without_postcodes(lower)) return std::nullopt;
    std::string upper(pc);
    for (char& c : upper)
        if (c >= 'a' && c <= 'z') c = static_cast<char>(c - 32);
    for (const postcode_detail::Matcher& f : postcode_detail::matchers().of(cc, fallback)) {
        std::smatch norm;
        if (!std::regex_match(upper, norm, f.norm)) continue;
        const std::string group = norm.str(1);
        std::smatch m;
        if (!std::regex_match(group, m, f.pattern)) continue;
        if (!group.empty() && group.find_first_not_of("0- ") == std::string::npos) continue;
        return postcode_detail::expand(m, f.output);
    }
    return std::nullopt;
}
