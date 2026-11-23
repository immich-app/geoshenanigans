// Which name each postcode of the postal cities (postal_cities.h) gets: in
// the countries the postcode grid covers (the United States, its ZIPs), the
// name mail to the ZIP is addressed with, as most of the ZIP's mapped
// addresses spell it in addr:city, and GeoNames' USPS place name where too
// few addresses name one.
//
// A vote is an address point of the grid's (postcode_grid_build.h: in a grid
// country, of a postcode its pattern takes) that carries addr:city. Its value
// is cleaned (clean_value) and grouped by a lenient key (lenient_key), under
// which Roy City is Roy and Barre Town is Barre. Values that name no postal
// place (a digit, a state's name, two letters, a Township, Twp, County or
// Borough last word) don't vote unless GeoNames' name of the ZIP is theirs.
//
// The choice (choose): a ZIP of 5 votes or more takes the key of the most
// votes, GeoNames' key counting max(2, 10% of the votes) more, and
// yesterday's name's key as many more again where it still holds a vote or is
// GeoNames'; a tie goes to GeoNames', then yesterday's, then the least key.
// Below 5 votes the ZIP takes GeoNames' name, or, without one, a key of more
// than half of 3 or 4 votes; else it has no row. Yesterday's name never
// counts towards the 5, so a ZIP of few votes keeps GeoNames' name whatever
// yesterday held. A build given no GeoNames entry at all keeps yesterday's
// name of such a ZIP instead.
//
// The name is the winning key's spelling most of the ZIP's votes use
// (pick_spelling): a mixed-case one over one all in capitals, yesterday's where it is
// within the same margin of the top, and, for GeoNames' key, none ending in
// a municipal word GeoNames' name lacks. A key the ZIP spells nowhere takes
// the spelling most votes of its GeoNames state use, else GeoNames' text.
#pragma once

#include <algorithm>
#include <cstdint>
#include <istream>
#include <iterator>
#include <map>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "country_code.h"
#include "postal_cities.h"

namespace postal_cities {

// The fewest votes a ZIP's own keys decide its name from.
inline constexpr uint64_t MIN_VOTES = 5;
// Without GeoNames' name, the fewest votes a key holding more than half of
// them names the ZIP from.
inline constexpr uint64_t MIN_MAJORITY_VOTES = 3;

inline bool ascii_space(char c) { return c == ' ' || (c >= '\t' && c <= '\r'); }

inline std::string_view trim(std::string_view s) {
    while (!s.empty() && ascii_space(s.front())) s.remove_prefix(1);
    while (!s.empty() && ascii_space(s.back())) s.remove_suffix(1);
    return s;
}

// s upper-cased: ASCII, and the Latin-1 letters U+00E0 to U+00FE but
// U+00F7, as two bytes of UTF-8.
inline std::string upper(std::string_view s) {
    std::string out(s);
    for (size_t i = 0; i < out.size(); i++) {
        const unsigned char c = static_cast<unsigned char>(out[i]);
        if (c >= 'a' && c <= 'z') {
            out[i] = char(c - 32);
        } else if (c == 0xC3 && i + 1 < out.size()) {
            const unsigned char d = static_cast<unsigned char>(out[i + 1]);
            if (d >= 0xA0 && d <= 0xBE && d != 0xB7) out[i + 1] = char(d - 0x20);
            i++;
        }
    }
    return out;
}

inline bool starts_with_ci(std::string_view s, std::string_view prefix) {
    if (s.size() < prefix.size()) return false;
    for (size_t i = 0; i < prefix.size(); i++) {
        char a = s[i], b = prefix[i];
        if (a >= 'a' && a <= 'z') a = char(a - 32);
        if (b >= 'a' && b <= 'z') b = char(b - 32);
        if (a != b) return false;
    }
    return true;
}

// The value of an addr:city tag as a name: its first value of a ';' list, cut
// at the first ',', trimmed, without a leading "City of", "Town of",
// "Village of", "Borough of" or "Township of" in any case.
inline std::string clean_value(std::string_view raw) {
    std::string_view s = raw.substr(0, raw.find(';'));
    s = trim(s.substr(0, s.find(',')));
    for (std::string_view prefix : {"City of ", "Town of ", "Village of ", "Borough of ", "Township of "})
        if (starts_with_ci(s, prefix)) {
            s = trim(s.substr(prefix.size()));
            break;
        }
    return std::string(s);
}

// The words a strict key spells out, and the first word's.
inline constexpr std::pair<std::string_view, std::string_view> WORD_FORMS[] = {
    {"ST", "SAINT"},      {"STE", "SAINTE"},  {"MT", "MOUNT"},   {"FT", "FORT"},     {"PT", "POINT"},
    {"HTS", "HEIGHTS"},   {"HGTS", "HEIGHTS"}, {"SPGS", "SPRINGS"}, {"SPG", "SPRING"}, {"TWP", "TOWNSHIP"},
    {"VLG", "VILLAGE"},   {"CTR", "CENTER"},  {"JCT", "JUNCTION"}, {"LK", "LAKE"},     {"BCH", "BEACH"},
    {"MTN", "MOUNTAIN"},  {"SPRGS", "SPRINGS"}, {"PK", "PARK"},   {"VLY", "VALLEY"},  {"CY", "CITY"},
    {"IS", "ISLAND"},     {"HBR", "HARBOR"},  {"HL", "HILL"},     {"HLS", "HILLS"},   {"GRV", "GROVE"},
    {"PRK", "PARK"},      {"CRK", "CREEK"},
};
inline constexpr std::pair<std::string_view, std::string_view> FIRST_WORD_FORMS[] = {
    {"N", "NORTH"}, {"S", "SOUTH"}, {"E", "EAST"}, {"W", "WEST"}};

// The key two spellings of one name share: the cleaned value upper-cased,
// without '.', apostrophes (' ’ `) and a leading "CITY OF" (or TOWN,
// VILLAGE, BOROUGH, TOWNSHIP), '-', '/' and '_' as spaces, '&' as AND, a
// word "MC" joined to the next and a first word "O" too, abbreviations
// spelled out (WORD_FORMS; N, S, E and W first), single spaces.
inline std::string strict_key(std::string_view value) {
    std::string s = upper(clean_value(value));
    std::string t;
    for (size_t i = 0; i < s.size(); i++) {
        const char c = s[i];
        if (c == '.' || c == '\'' || c == '`') continue;
        if (c == '\xE2' && i + 2 < s.size() && s[i + 1] == '\x80' && s[i + 2] == '\x99') {
            i += 2;
            continue;
        }
        if (c == '-' || c == '/' || c == '_') t += ' ';
        else if (c == '&') t += "AND";
        else t += c;
    }
    std::vector<std::string> words;
    for (size_t i = 0; i < t.size();) {
        while (i < t.size() && ascii_space(t[i])) i++;
        size_t j = i;
        while (j < t.size() && !ascii_space(t[j])) j++;
        if (j > i) words.emplace_back(t.substr(i, j - i));
        i = j;
    }
    for (std::string_view of : {"CITY", "TOWN", "VILLAGE", "BOROUGH", "TOWNSHIP"})
        if (words.size() > 2 && words[0] == of && words[1] == "OF") {
            words.erase(words.begin(), words.begin() + 2);
            break;
        }
    std::vector<std::string> joined;
    for (size_t i = 0; i < words.size(); i++) {
        if (i + 1 < words.size() && (words[i] == "MC" || (i == 0 && words[i] == "O"))) {
            joined.push_back(words[i] + words[i + 1]);
            i++;
        } else {
            joined.push_back(words[i]);
        }
    }
    for (std::string& w : joined)
        for (const auto& [from, to] : WORD_FORMS)
            if (w == from) {
                w = std::string(to);
                break;
            }
    if (!joined.empty())
        for (const auto& [from, to] : FIRST_WORD_FORMS)
            if (joined[0] == from) joined[0] = std::string(to);
    std::string key;
    for (const std::string& w : joined) key += (key.empty() ? "" : " ") + w;
    return key;
}

// The municipal words a lenient key drops from the end of a strict one, the
// longer first.
inline constexpr std::string_view MUNICIPAL_WORDS[] = {"CHARTER TOWNSHIP", "TOWNSHIP", "BOROUGH", "CITY",
                                                       "VILLAGE",          "TOWN",     "PLANTATION", "CDP"};

// The municipal word a strict key ends in after a word of its own, or "".
inline std::string_view municipal_word(std::string_view strict) {
    for (std::string_view w : MUNICIPAL_WORDS)
        if (strict.size() > w.size() + 1 && strict.substr(strict.size() - w.size()) == w &&
            strict[strict.size() - w.size() - 1] == ' ')
            return w;
    return {};
}

// The key a ZIP's votes group by: the strict key without its municipal word
// (Roy City is Roy, Cherry Hill Township Cherry Hill).
inline std::string lenient_key(std::string_view value) {
    std::string strict = strict_key(value);
    const std::string_view word = municipal_word(strict);
    if (!word.empty()) strict.resize(strict.size() - word.size() - 1);
    return strict;
}

inline constexpr std::string_view STATE_NAMES[] = {
    "ALABAMA",        "ALASKA",         "ARIZONA",        "ARKANSAS",      "CALIFORNIA",   "COLORADO",
    "CONNECTICUT",    "DELAWARE",       "DISTRICT OF COLUMBIA", "FLORIDA", "GEORGIA",      "HAWAII",
    "IDAHO",          "ILLINOIS",       "INDIANA",        "IOWA",          "KANSAS",       "KENTUCKY",
    "LOUISIANA",      "MAINE",          "MARYLAND",       "MASSACHUSETTS", "MICHIGAN",     "MINNESOTA",
    "MISSISSIPPI",    "MISSOURI",       "MONTANA",        "NEBRASKA",      "NEVADA",       "NEW HAMPSHIRE",
    "NEW JERSEY",     "NEW MEXICO",     "NEW YORK",       "NORTH CAROLINA", "NORTH DAKOTA", "OHIO",
    "OKLAHOMA",       "OREGON",         "PENNSYLVANIA",   "RHODE ISLAND",  "SOUTH CAROLINA", "SOUTH DAKOTA",
    "TENNESSEE",      "TEXAS",          "UTAH",           "VERMONT",       "VIRGINIA",     "WASHINGTON",
    "WEST VIRGINIA",  "WISCONSIN",      "WYOMING",        "PUERTO RICO",
};

// Whether a strict key names no postal place: it has a digit, is a state's
// name or two letters, or ends in a word of TOWNSHIP, COUNTY or BOROUGH (TWP
// is TOWNSHIP by then).
inline bool names_no_postal_place(std::string_view strict) {
    for (char c : strict)
        if (c >= '0' && c <= '9') return true;
    for (std::string_view state : STATE_NAMES)
        if (strict == state) return true;
    if (strict.size() == 2 && strict[0] >= 'A' && strict[0] <= 'Z' && strict[1] >= 'A' && strict[1] <= 'Z') return true;
    for (std::string_view w : {"TOWNSHIP", "COUNTY", "BOROUGH"})
        if (strict == w || (strict.size() > w.size() && strict.substr(strict.size() - w.size()) == w &&
                            strict[strict.size() - w.size() - 1] == ' '))
            return true;
    return false;
}

// What a value is to its ZIP's choice.
enum class Kind : uint8_t {
    VOTE,
    // Names no postal place (names_no_postal_place) and is not GeoNames'.
    FILTERED,
    // Longer than a name can be, or no text a name can hold.
    UNFIT,
};
inline const char* kind_name(Kind k) { return k == Kind::VOTE ? "vote" : k == Kind::FILTERED ? "filtered" : "unfit"; }

// A ZIP's value as its votes cast it: the cleaned text and the votes of it.
struct Value {
    std::string text;
    uint64_t n = 0;
};

// A value's text, keys and kind beside GeoNames' lenient key of the ZIP
// ("" for none).
struct Classified {
    std::string text, strict, lenient;
    uint64_t n = 0;
    Kind kind = Kind::VOTE;
};
inline Classified classify(const Value& v, std::string_view geonames_key) {
    Classified c{v.text, strict_key(v.text), lenient_key(v.text), v.n, Kind::VOTE};
    if (v.text.empty() || v.text.size() > NAME_BYTES || !name_text(v.text) || c.strict.empty()) c.kind = Kind::UNFIT;
    else if (names_no_postal_place(c.strict) && c.lenient != geonames_key) c.kind = Kind::FILTERED;
    return c;
}

// A ZIP's GeoNames entry: its place name and its state ("" for none).
struct Place {
    std::string name, state;
};

// The GeoNames entries by (country, code), of a TSV of country, code, name
// and state, or of GeoNames' postal code files themselves (12 fields: the
// state is the admin code, or the country of a territory). Every row is
// taken as the United States': the territories' ZIPs are of its ZIP space,
// and the answers there give its country. A row of no code, a name the
// format can't hold, or a code the grid's pattern doesn't take is skipped.
using Places = std::map<std::pair<uint16_t, std::string>, Place>;
template <class CodeOf>
inline Places parse_places(std::istream& in, CodeOf code_of_zip) {
    Places out;
    std::string line;
    const uint16_t US = pack_country_code('U', 'S');
    while (std::getline(in, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        std::vector<std::string_view> f;
        for (size_t at = 0;;) {
            const size_t tab = line.find('\t', at);
            f.emplace_back(std::string_view(line).substr(at, tab == std::string::npos ? std::string::npos : tab - at));
            if (tab == std::string::npos) break;
            at = tab + 1;
        }
        if (f.size() < 4) continue;
        const std::string_view country = f[0], name = trim(f[2]);
        const std::string_view state = f.size() >= 12 ? (country == "US" ? f[4] : country) : f[3];
        const std::optional<std::string> code = code_of_zip(US, f[1]);
        if (!code || name.empty() || name.size() > NAME_BYTES || !name_text(name)) continue;
        out.emplace(std::make_pair(US, *code), Place{std::string(name), std::string(trim(state))});
    }
    return out;
}

// The names of a postal_cities.bin, by (country, code).
using Names = std::map<std::pair<uint16_t, std::string>, std::string>;
inline Names names_of(const std::vector<uint8_t>& file) {
    Names out;
    decode(file.data(), file.size(), [&](const LogicalRow& r) {
        out.emplace(std::make_pair(r.country_code, std::string(code_of(r))), std::string(name_of(r)));
    });
    return out;
}

// How a ZIP's name was chosen.
enum class Step : uint8_t {
    NONE,
    // Its votes, of 5 or more, with GeoNames' and yesterday's margins.
    VOTES,
    // GeoNames' name, below 5 votes.
    GEONAMES,
    // Without GeoNames' name, a key of more than half of 3 or 4 votes.
    MAJORITY,
    // Below 5 votes in a build without GeoNames' entries, yesterday's name.
    PREVIOUS,
};
inline const char* step_name(Step s) {
    return s == Step::VOTES      ? "votes"
           : s == Step::GEONAMES ? "geonames"
           : s == Step::MAJORITY ? "majority"
           : s == Step::PREVIOUS ? "previous"
                                 : "none";
}

// The margin GeoNames' key and yesterday's take over `votes` votes:
// max(2, 10% of them, rounded up).
inline uint64_t margin(uint64_t votes) { return std::max<uint64_t>(2, (votes + 9) / 10); }

// Whether a spelling is all in capitals: no lower-case ASCII letter, and an
// upper-case one.
inline bool all_capitals(std::string_view s) {
    bool upper_seen = false;
    for (char c : s) {
        if (c >= 'a' && c <= 'z') return false;
        upper_seen |= c >= 'A' && c <= 'Z';
    }
    return upper_seen;
}

// Spellings with their votes, by text.
using Spellings = std::map<std::string, uint64_t>;

// The spelling of `spellings` a name takes: a mixed-case one over one all in
// capitals, of the most votes, the least text of those; `prev` instead where
// it is one of them within margin(key_votes) of the top. None of no
// spelling.
inline std::optional<std::string> pick_spelling(const Spellings& spellings, uint64_t key_votes,
                                                const std::string* prev) {
    bool mixed = false;
    for (const auto& [text, n] : spellings) mixed |= !all_capitals(text);
    const std::pair<const std::string, uint64_t>* top = nullptr;
    for (const auto& s : spellings) {
        if (mixed && all_capitals(s.first)) continue;
        if (!top || s.second > top->second) top = &s;
    }
    if (!top) return std::nullopt;
    if (prev) {
        const auto it = spellings.find(*prev);
        if (it != spellings.end() && !(mixed && all_capitals(it->first)) &&
            it->second + margin(key_votes) >= top->second)
            return it->first;
    }
    return top->first;
}

// Drops the spellings ending in a municipal word GeoNames' name lacks, for
// the key of GeoNames' name: Roy City where GeoNames says Roy.
inline Spellings without_municipal_forms(Spellings spellings, std::string_view geonames_strict) {
    for (auto it = spellings.begin(); it != spellings.end();) {
        const std::string strict = strict_key(it->first);
        const std::string_view word = municipal_word(strict);
        const bool lacks = !word.empty() && municipal_word(geonames_strict) != word;
        it = lacks ? spellings.erase(it) : std::next(it);
    }
    return spellings;
}

// The spellings of each lenient key a state's votes use, the state's votes
// together: the ZIP's GeoNames state, then the key.
using StateSpellings = std::map<std::pair<std::string, std::string>, Spellings>;

// The name of a ZIP and how it was chosen.
struct Choice {
    Step step = Step::NONE;
    std::string key, name;
    // Yesterday's margin chose the key or its spelling: without it, another.
    bool held = false;
};

// The key a ZIP of `votes` (lenient key -> votes, T in all, T of at least
// MIN_VOTES) takes, GeoNames' key g ("" for none) and yesterday's p ("" for
// none, or where it is not to count) each counting margin(T) more.
inline std::string top_key(const std::map<std::string, uint64_t>& votes, uint64_t total, const std::string& g,
                           const std::string& p) {
    const uint64_t h = margin(total);
    std::map<std::string, uint64_t> score = votes;
    if (!g.empty()) score[g] += h;
    if (!p.empty()) score[p] += h;
    const std::string* best = nullptr;
    uint64_t best_score = 0;
    auto rank = [&](const std::string& k) { return k == g && !g.empty() ? 0 : k == p && !p.empty() ? 1 : 2; };
    for (const auto& [k, s] : score) {
        if (!best || s > best_score || (s == best_score && rank(k) < rank(*best))) best = &k, best_score = s;
    }
    return best ? *best : std::string();
}

// A ZIP's name from its classified values, GeoNames' entry (null for none),
// yesterday's name (null for none) and the states' spellings, which a key
// the ZIP's votes don't spell takes its spelling from. `no_geonames`: the
// build has no GeoNames entry at all, and a ZIP below 5 votes keeps
// yesterday's name, for want of GeoNames' (a local build's).
inline Choice choose(const std::vector<Classified>& values, const Place* geonames, const std::string* prev,
                     const StateSpellings& state_spellings, bool no_geonames = false) {
    std::map<std::string, uint64_t> votes;
    uint64_t total = 0;
    for (const Classified& v : values)
        if (v.kind == Kind::VOTE) votes[v.lenient] += v.n, total += v.n;
    const std::string g = geonames ? lenient_key(geonames->name) : std::string();
    const std::string p = prev ? lenient_key(*prev) : std::string();
    Choice c;
    if (total >= MIN_VOTES) {
        // Yesterday's key counts where a vote still names it or it is GeoNames'.
        const bool eligible = !p.empty() && (votes.count(p) || p == g);
        c.key = top_key(votes, total, g, eligible ? p : std::string());
        c.step = Step::VOTES;
        c.held = eligible && top_key(votes, total, g, std::string()) != c.key;
    } else if (geonames) {
        c.key = g;
        c.step = Step::GEONAMES;
    } else if (no_geonames && prev) {
        c.key = p;
        c.name = *prev;
        c.step = Step::PREVIOUS;
        return c;
    } else if (total >= MIN_MAJORITY_VOTES) {
        for (const auto& [k, n] : votes)
            if (2 * n > total) c.key = k;
        if (!c.key.empty()) c.step = Step::MAJORITY;
    }
    if (c.step == Step::NONE) return c;

    // The spelling: the ZIP's own, else its state's, else GeoNames' text.
    const bool of_geonames = geonames && c.key == g;
    const std::string geonames_strict = of_geonames ? strict_key(geonames->name) : std::string();
    auto spellings_of = [&](Spellings s) { return of_geonames ? without_municipal_forms(std::move(s), geonames_strict) : s; };
    Spellings own;
    for (const Classified& v : values)
        if (v.kind == Kind::VOTE && v.lenient == c.key) own[v.text] += v.n;
    const uint64_t key_votes = votes.count(c.key) ? votes.at(c.key) : 0;
    const std::string* prev_spelling = prev && p == c.key ? prev : nullptr;
    const Spellings spelt = spellings_of(std::move(own));
    std::optional<std::string> name = pick_spelling(spelt, key_votes, prev_spelling);
    if (prev_spelling && name != pick_spelling(spelt, key_votes, nullptr)) c.held = true;
    if (!name && geonames && !geonames->state.empty()) {
        const auto it = state_spellings.find({geonames->state, c.key});
        if (it != state_spellings.end()) name = pick_spelling(spellings_of(it->second), 0, nullptr);
    }
    if (!name && of_geonames) name = geonames->name;
    if (!name) {
        c.step = Step::NONE;
        return c;
    }
    c.name = *name;
    return c;
}

// The postal cities of a build: its rows, ascending, each with the index of
// its postcode among the codes, the choice made for each code, and counts
// for the log.
struct Table {
    std::vector<LogicalRow> rows;
    std::vector<uint32_t> row_codes;
    std::vector<Choice> choices;
    std::vector<std::vector<Classified>> values;
    uint64_t votes = 0, zips = 0, by_votes = 0, by_geonames = 0, by_majority = 0, by_previous = 0, names = 0;
    uint64_t kept = 0, switched = 0, held = 0;
};

// The table of `codes` ((country, code) ascending, grid codes), of
// `city_votes` (postcode_grid::city_vote, ascending), a city's text by
// city_of(id), GeoNames' entries and yesterday's names. A code of a country
// the grid has no pattern for takes no row.
template <class Code, class CityOf>
Table choose_all(const std::vector<Code>& codes, const std::vector<uint64_t>& city_votes, CityOf city_of,
                 const Places& places, const Names& prev) {
    Table t;
    // Each code's values: the votes of each cleaned text.
    std::vector<std::map<std::string, uint64_t>> texts(codes.size());
    for (size_t i = 0; i < city_votes.size();) {
        size_t j = i;
        while (j < city_votes.size() && city_votes[j] == city_votes[i]) j++;
        const uint32_t code = uint32_t(city_votes[i] >> 32), city = uint32_t(city_votes[i]);
        if (code < codes.size()) texts[code][clean_value(city_of(city))] += j - i;
        i = j;
    }
    auto entry = [&](size_t c) -> const Place* {
        const auto it = places.find({codes[c].country, codes[c].code});
        return it == places.end() ? nullptr : &it->second;
    };
    t.values.resize(codes.size());
    StateSpellings states;
    for (size_t c = 0; c < codes.size(); c++) {
        const Place* g = entry(c);
        const std::string gkey = g ? lenient_key(g->name) : std::string();
        for (const auto& [text, n] : texts[c]) {
            t.values[c].push_back(classify(Value{text, n}, gkey));
            const Classified& v = t.values[c].back();
            if (v.kind != Kind::VOTE) continue;
            t.votes += n;
            if (g && !g->state.empty()) states[{g->state, v.lenient}][v.text] += n;
        }
        t.zips += !texts[c].empty();
        std::map<std::string, uint64_t>().swap(texts[c]);
    }
    t.choices.resize(codes.size());
    std::vector<std::string> names;
    for (size_t c = 0; c < codes.size(); c++) {
        const auto p = prev.find({codes[c].country, codes[c].code});
        const std::string* yesterday = p == prev.end() ? nullptr : &p->second;
        Choice& ch = t.choices[c] = choose(t.values[c], entry(c), yesterday, states, places.empty());
        if (ch.step == Step::NONE) continue;
        t.rows.push_back(row_of(codes[c].country, codes[c].code, ch.name));
        t.row_codes.push_back(uint32_t(c));
        names.push_back(ch.name);
        t.by_votes += ch.step == Step::VOTES;
        t.by_geonames += ch.step == Step::GEONAMES;
        t.by_majority += ch.step == Step::MAJORITY;
        t.by_previous += ch.step == Step::PREVIOUS;
        t.held += ch.held;
        if (yesterday) (*yesterday == ch.name ? t.kept : t.switched)++;
    }
    std::sort(names.begin(), names.end());
    t.names = uint64_t(std::unique(names.begin(), names.end()) - names.begin());
    return t;
}

// The table's votes as lines of TSV, a line per value of each code with a
// vote (one of no value for a code of none but a row), for a look at how
// each name was chosen: country, code, GeoNames' name, yesterday's name, the
// step, the name chosen, then the value's kind, lenient key, text and votes.
template <class Code>
void write_votes(std::ostream& out, const std::vector<Code>& codes, const Table& t, const Places& places,
                 const Names& prev) {
    out << "country\tcode\tgeonames\tprevious\tstep\tname\tkind\tkey\ttext\tvotes\n";
    for (size_t c = 0; c < codes.size(); c++) {
        const Choice& ch = t.choices[c];
        if (t.values[c].empty() && ch.step == Step::NONE) continue;
        const auto g = places.find({codes[c].country, codes[c].code});
        const auto p = prev.find({codes[c].country, codes[c].code});
        const char country[3] = {char(codes[c].country >> 8), char(codes[c].country & 0xFF), 0};
        auto head = [&] {
            out << country << '\t' << codes[c].code << '\t' << (g == places.end() ? "" : g->second.name) << '\t'
                << (p == prev.end() ? "" : p->second) << '\t' << step_name(ch.step) << '\t' << ch.name << '\t';
        };
        if (t.values[c].empty()) {
            head();
            out << "\t\t\t0\n";
        }
        for (const Classified& v : t.values[c]) {
            // A text of a tab or a line break would cut its line: such text names nothing anyway.
            if (v.text.find_first_of("\t\n\r") != std::string::npos) continue;
            head();
            out << kind_name(v.kind) << '\t' << v.lenient << '\t' << v.text << '\t' << v.n << '\n';
        }
    }
}

}  // namespace postal_cities
