// The water layer's kinds and the type code each water record carries (in
// its AdminPolygonRecord's country_code): Nominatim's class and type, which
// a water answer names, and its rank_search (settings/address-levels.json),
// which breaks ties between areas of one size. Codes 1 to 255 are a fixed
// table, append-only. Any other water=* value is its own type, as it is in
// Nominatim (class water, the value verbatim), with a code from 0x8000 up:
// the code the previous build gave it (its water_meta.json), else the code
// of its hash, else the next free one; so a value keeps its code from one
// build to the next and a new one never takes another's. A patch carries
// water_meta.json whole, as it carries any JSON file (1 MiB at most), so the
// values it lists are as many as fit in kMetaVerbatimBytes: the previous
// build's first, then those of most objects; any other is natural/water.
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

namespace water {

struct Kind {
    const char* cls;
    const char* type;
    uint8_t rank;
};

// Code k is kFixedKinds[k - 1]. Append only: a code is never reused.
inline constexpr Kind kFixedKinds[] = {
    {"natural", "water", 22},     // 1: natural=water with no water=*, or yes, intermittent, tidal
    {"water", "lake", 20},        // 2
    {"water", "reservoir", 20},   // 3
    {"water", "pond", 24},        // 4
    {"water", "wastewater", 24},  // 5
    {"water", "fountain", 24},    // 6
    {"water", "oxbow", 22},       // 7
    {"water", "lagoon", 22},      // 8
    {"water", "basin", 22},       // 9
    {"water", "harbour", 22},     // 10
    {"water", "moat", 22},        // 11
    {"water", "fishpond", 22},    // 12
    {"water", "lock", 22},        // 13
    {"water", "salt_pool", 22},   // 14
    {"water", "cove", 22},        // 15
    {"water", "bay", 22},         // 16
    {"water", "fjord", 22},       // 17
    {"water", "sound", 22},       // 18
    {"water", "strait", 22},      // 19
    {"water", "sea", 22},         // 20
    {"natural", "bay", 22},       // 21
    {"natural", "strait", 22},    // 22
    {"natural", "wetland", 22},   // 23
    {"natural", "glacier", 22},   // 24
    {"place", "sea", 2},          // 25
    {"place", "ocean", 22},       // 26: no OSM area; marine areas
    {"landuse", "reservoir", 24}, // 27
    {"landuse", "basin", 24},     // 28
    {"waterway", "river", 19},    // 29: every river area, named by itself or its waterway
    {"waterway", "canal", 20},    // 30: every canal area
};
inline constexpr uint16_t kFixedKindCount = sizeof(kFixedKinds) / sizeof(kFixedKinds[0]);
static_assert(kFixedKindCount <= 255, "the fixed water kinds take codes 1 to 255");

// The codes of the kinds the classifier and the naming name directly.
namespace code {
inline constexpr uint16_t kNaturalWater = 1;
inline constexpr uint16_t kLake = 2;
inline constexpr uint16_t kNaturalBay = 21;
inline constexpr uint16_t kNaturalStrait = 22;
inline constexpr uint16_t kWetland = 23;
inline constexpr uint16_t kGlacier = 24;
inline constexpr uint16_t kPlaceSea = 25;
inline constexpr uint16_t kPlaceOcean = 26;
inline constexpr uint16_t kLanduseReservoir = 27;
inline constexpr uint16_t kLanduseBasin = 28;
inline constexpr uint16_t kRiver = 29;
inline constexpr uint16_t kCanal = 30;
}  // namespace code

// The first code of the water=* values outside the fixed table.
inline constexpr uint16_t kVerbatimBase = 0x8000;

// Nominatim's rank of a water=* value outside the fixed table: the water
// class's default.
inline constexpr uint8_t kVerbatimRank = 22;

// The fixed code of class `cls` and type `type`, or 0.
inline uint16_t fixed_code(const char* cls, const char* type) {
    for (uint16_t k = 0; k < kFixedKindCount; k++)
        if (std::strcmp(kFixedKinds[k].cls, cls) == 0 && std::strcmp(kFixedKinds[k].type, type) == 0) return k + 1;
    return 0;
}

// FNV-1a over the bytes, then a mix: the start of a value's code search.
inline uint64_t value_hash(const std::string& value) {
    uint64_t h = 0xcbf29ce484222325ull;
    for (unsigned char c : value) h = (h ^ c) * 0x100000001b3ull;
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdull;
    h ^= h >> 33;
    return h;
}

// One kind a dir's water_meta.json lists.
struct MetaType {
    uint16_t code;
    std::string cls, type;
    uint8_t rank;
};

// What water_meta.json's kinds outside the fixed table take at most: its
// fixed kinds and its frame take the rest of the 1 MiB a patch carries.
inline constexpr size_t kMetaVerbatimBytes = 768 * 1024;

inline void json_escape(std::string& out, const std::string& s) {
    static const char* hex = "0123456789abcdef";
    for (unsigned char c : s) {
        if (c == '"' || c == '\\') {
            out += '\\';
            out += static_cast<char>(c);
        } else if (c < 0x20) {
            out += "\\u00";
            out += hex[c >> 4];
            out += hex[c & 15];
        } else {
            out += static_cast<char>(c);
        }
    }
}

// A kind's line in water_meta.json, without the comma and line end between
// two.
inline std::string meta_entry(const MetaType& k) {
    std::string out = "  {\"code\": " + std::to_string(k.code) + ", \"class\": \"";
    json_escape(out, k.cls);
    out += "\", \"type\": \"";
    json_escape(out, k.type);
    out += "\", \"rank\": " + std::to_string(k.rank) + "}";
    return out;
}

// The kinds of a build: the fixed ones, and the water=* values outside them
// with their codes.
class TypeTable {
public:
    // Gives the water=* values outside the fixed table (`uses`: each value
    // and how many objects carry it) codes, as many as water_meta.json lists
    // in kMetaVerbatimBytes, taken in turn while the next fits: the values
    // `previous` (the previous build's kinds, water_meta.json) has, in its
    // order, then those of most objects, the smaller of a tie. A value keeps
    // the code `previous` gave it; the others take, in value order, their
    // hash's, else the next free one round the verbatim codes. A value past
    // the budget is natural/water. Throws past the 32,768 verbatim codes.
    void assign(const std::map<std::string, size_t>& uses, const std::vector<MetaType>& previous) {
        verbatim_.clear();
        generic_.clear();
        std::map<std::string, uint16_t> before;
        std::set<uint16_t> codes_before;
        std::vector<std::string> order;
        for (const MetaType& m : previous) {
            if (m.code < kVerbatimBase || m.cls != "water" || !uses.count(m.type) || before.count(m.type) ||
                codes_before.count(m.code))
                continue;
            before[m.type] = m.code;
            codes_before.insert(m.code);
            order.push_back(m.type);
        }
        std::vector<std::pair<std::string, size_t>> others;
        for (const auto& [v, n] : uses)
            if (!before.count(v)) others.push_back({v, n});
        std::stable_sort(others.begin(), others.end(), [](const auto& a, const auto& b) { return a.second > b.second; });
        for (const auto& [v, n] : others) order.push_back(v);
        std::set<std::string> listed;
        size_t bytes = 0;
        for (const std::string& v : order) {
            const size_t entry = meta_entry({kVerbatimBase, "water", v, kVerbatimRank}).size() + 2;
            if (bytes + entry > kMetaVerbatimBytes) break;
            bytes += entry;
            listed.insert(v);
        }
        std::set<uint16_t> taken;
        for (const auto& [v, c] : before)
            if (listed.count(v)) {
                verbatim_[v] = c;
                taken.insert(c);
            }
        for (const std::string& v : listed) {
            if (verbatim_.count(v)) continue;
            if (taken.size() >= 0x8000) throw std::length_error("water types past 32,768 verbatim codes");
            uint16_t c = static_cast<uint16_t>(kVerbatimBase | (value_hash(v) & 0x7FFF));
            while (taken.count(c)) c = static_cast<uint16_t>(kVerbatimBase | ((c + 1) & 0x7FFF));
            verbatim_[v] = c;
            taken.insert(c);
        }
        for (const auto& [v, n] : uses)
            if (!listed.count(v)) generic_.insert(v);
        by_code_.clear();
        for (const auto& [v, c] : verbatim_) by_code_[c] = v;
    }

    // The code of a water=* value outside the fixed table: its own, or
    // natural/water's for one past water_meta.json's budget; throws for one
    // assign() was not given.
    uint16_t verbatim_code(const std::string& value) const {
        auto it = verbatim_.find(value);
        if (it != verbatim_.end()) return it->second;
        if (generic_.count(value)) return code::kNaturalWater;
        throw std::logic_error("water type '" + value + "' has no code");
    }

    // The kind of a code: fixed or assigned. Throws for an unknown one.
    MetaType kind(uint16_t code) const {
        if (code >= 1 && code <= kFixedKindCount) {
            const Kind& k = kFixedKinds[code - 1];
            return {code, k.cls, k.type, k.rank};
        }
        auto it = by_code_.find(code);
        if (it == by_code_.end()) throw std::logic_error("unknown water type code " + std::to_string(code));
        return {code, "water", it->second, kVerbatimRank};
    }

    uint8_t rank(uint16_t code) const { return kind(code).rank; }

    size_t verbatim_count() const { return verbatim_.size(); }
    // The values past water_meta.json's budget, natural/water.
    size_t generic_count() const { return generic_.size(); }

private:
    std::map<std::string, uint16_t> verbatim_;
    std::map<uint16_t, std::string> by_code_;
    std::set<std::string> generic_;
};

// --- water_meta.json ---

// The water_meta.json of a dir whose records use `codes`: each kind by code,
// ascending, one a line.
inline std::string meta_json(const TypeTable& table, const std::set<uint16_t>& codes) {
    std::string out = "{\"types\": [\n";
    bool first = true;
    for (uint16_t c : codes) {
        if (!first) out += ",\n";
        first = false;
        out += meta_entry(table.kind(c));
    }
    out += "\n]}\n";
    return out;
}

// The kinds of a water_meta.json meta_json wrote, with nothing but white
// space after it; none (nullopt) for any other text, one cut short, and the
// empty text of a missing file, among them.
inline std::optional<std::vector<MetaType>> parse_meta_json(const std::string& text) {
    std::vector<MetaType> out;
    size_t at = 0;
    auto skip_space = [&] {
        while (at < text.size() && (text[at] == ' ' || text[at] == '\n' || text[at] == '\t' || text[at] == '\r')) at++;
    };
    auto expect = [&](const char* word) {
        skip_space();
        const size_t n = std::strlen(word);
        if (text.compare(at, n, word) != 0) return false;
        at += n;
        return true;
    };
    auto number = [&](long& v) {
        skip_space();
        size_t start = at;
        while (at < text.size() && text[at] >= '0' && text[at] <= '9') at++;
        if (at == start || at - start > 6) return false;
        v = std::stol(text.substr(start, at - start));
        return true;
    };
    auto string = [&](std::string& v) {
        skip_space();
        if (at >= text.size() || text[at] != '"') return false;
        at++;
        v.clear();
        while (at < text.size() && text[at] != '"') {
            if (text[at] == '\\') {
                if (at + 1 >= text.size()) return false;
                const char e = text[at + 1];
                if (e == 'u') {
                    if (at + 6 > text.size()) return false;
                    int c = 0;
                    for (size_t k = at + 2; k < at + 6; k++) {
                        const char h = text[k];
                        const int d = h >= '0' && h <= '9' ? h - '0' : h >= 'a' && h <= 'f' ? h - 'a' + 10 : -1;
                        if (d < 0) return false;
                        c = c * 16 + d;
                    }
                    if (c >= 0x20) return false;
                    v += static_cast<char>(c);
                    at += 6;
                    continue;
                }
                if (e != '"' && e != '\\') return false;
                v += e;
                at += 2;
                continue;
            }
            if (static_cast<unsigned char>(text[at]) < 0x20) return false;
            v += text[at++];
        }
        if (at >= text.size()) return false;
        at++;
        return true;
    };
    auto end = [&] {
        if (!expect("}")) return false;
        skip_space();
        return at == text.size();
    };
    if (!expect("{") || !expect("\"types\"") || !expect(":") || !expect("[")) return std::nullopt;
    skip_space();
    if (expect("]")) return end() ? std::optional<std::vector<MetaType>>(out) : std::nullopt;
    while (true) {
        MetaType m;
        long code = 0, rank = 0;
        if (!expect("{") || !expect("\"code\"") || !expect(":") || !number(code) || !expect(",") ||
            !expect("\"class\"") || !expect(":") || !string(m.cls) || !expect(",") || !expect("\"type\"") ||
            !expect(":") || !string(m.type) || !expect(",") || !expect("\"rank\"") || !expect(":") || !number(rank) ||
            !expect("}"))
            return std::nullopt;
        if (code <= 0 || code > 0xFFFF || rank > 255) return std::nullopt;
        m.code = static_cast<uint16_t>(code);
        m.rank = static_cast<uint8_t>(rank);
        out.push_back(m);
        if (expect(",")) continue;
        if (expect("]")) break;
        return std::nullopt;
    }
    if (!end()) return std::nullopt;
    return out;
}

}  // namespace water
