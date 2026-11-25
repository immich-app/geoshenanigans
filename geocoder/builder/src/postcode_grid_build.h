// What each cell of the postcode grid (postcode_grid.h) holds, from a build's
// votes: in the countries it covers (GRID_COUNTRIES, the United States, whose
// postcodes are areas and which has next to no postal boundaries), the
// postcode of the houses round a point. Nominatim's zoom-18 answer there is
// the postcode of the house or TIGER line it lands on; the centroid of a
// large ZIP lies farther from its edge than a neighbour's, so the nearest
// centroid misses near the edge.
//
// A vote is a point of a grid country with its postcode: an address point
// with an addr:postcode its country's pattern takes (grid_postcode), or the
// point halfway along a TIGER line (line_middle) with the line's ZIP. A cell
// holding votes takes the postcode most of them name, the least (country,
// code) of those named most. A cell holding none takes the postcode of the
// nearer of the cells holding votes before and after it along the curve (the
// earlier one at the same distance) that reaches it: a cell reaches the cells
// at most its country's reach away (reach_cells), so a run ends there and the
// cells past it have no postcode. A step along the curve is to a cell sharing an edge,
// so a cell's postcode comes from votes within the reach of it, the
// country's postcode extent, as far as get_nearest_postcode's box of a
// centroid reaches.
#pragma once

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "country_code.h"
#include "line_middle.h"
#include "parallel.h"
#include "postcode_grid.h"
#include "types.h"

namespace postcode_grid {

// The level of the grid's cells, about 140 m across.
inline constexpr int GRID_LEVEL = 16;

// A country the grid covers, and its postcodes' extent in metres
// (Nominatim's country_settings.yaml, `extent`).
struct GridCountry {
    uint16_t code;
    double extent_m;
};
inline constexpr GridCountry GRID_COUNTRIES[] = {{pack_country_code('U', 'S'), 25000.0}};

inline const GridCountry* grid_country(uint16_t code) {
    for (const GridCountry& c : GRID_COUNTRIES)
        if (c.code == code) return &c;
    return nullptr;
}

// The longest edge a cell of the level has, in metres: S2's kMaxEdge (of the
// quadratic projection) on S2Earth's radius.
inline constexpr double max_edge_m(int level) { return 1.704897179199218452 / double(uint64_t(1) << level) * 6371010.0; }
// The cells along the curve a cell of votes reaches, the most steps of an
// edge each that stay within extent_m.
inline uint64_t reach_cells(double extent_m, int level) { return uint64_t(extent_m / max_edge_m(level)); }

// The bytes (1 to 3) of the whitespace character at p, of the `left` there,
// or 0: whitespace as Python's str.isspace() has it, the characters a
// regular expression's \s matches.
inline size_t space_at(const unsigned char* p, size_t left) {
    const unsigned char c = p[0];
    if (c == ' ' || (c >= 0x09 && c <= 0x0d) || (c >= 0x1c && c <= 0x1f)) return 1;
    if (left >= 2 && c == 0xC2 && (p[1] == 0x85 || p[1] == 0xA0)) return 2;
    if (left >= 3 && c == 0xE1 && p[1] == 0x9A && p[2] == 0x80) return 3;  // U+1680
    if (left >= 3 && c == 0xE2 && p[1] == 0x80 && ((p[2] >= 0x80 && p[2] <= 0x8A) || p[2] == 0xA8 || p[2] == 0xA9 || p[2] == 0xAF))
        return 3;  // U+2000 to U+200A, U+2028, U+2029, U+202F
    if (left >= 3 && c == 0xE2 && p[1] == 0x81 && p[2] == 0x9F) return 3;  // U+205F
    if (left >= 3 && c == 0xE3 && p[1] == 0x80 && p[2] == 0x80) return 3;  // U+3000
    return 0;
}
// The bytes of whitespace that end s (whole characters).
inline size_t trailing_space(std::string_view s) {
    size_t end = s.size();
    for (bool more = true; more;) {
        more = false;
        for (size_t n = 1; n <= 3 && n <= end; n++) {
            const auto* p = reinterpret_cast<const unsigned char*>(s.data()) + end - n;
            if (space_at(p, n) == n) {
                end -= n;
                more = true;
                break;
            }
        }
    }
    return s.size() - end;
}

// The postcode Nominatim takes for an addr:postcode of a grid country
// (postcode_format.py, SimplePostcodeMatcher, of country_settings.yaml's
// pattern and output): the tag upper-cased fully matches
// \s*(?:CC[ -]?)?(PATTERN)\s*, the group isn't all of 0, - and space, and
// the output is that group's expansion. The United States: pattern
// (ddddd)(?:-dddd)?, output \1. The country prefix is matched in ASCII.
inline std::optional<std::string> grid_postcode(uint16_t country, std::string_view raw) {
    if (country != pack_country_code('U', 'S')) return std::nullopt;
    size_t at = 0;
    while (at < raw.size()) {
        const size_t n = space_at(reinterpret_cast<const unsigned char*>(raw.data()) + at, raw.size() - at);
        if (!n) break;
        at += n;
    }
    std::string_view s = raw.substr(at);
    s.remove_suffix(trailing_space(s));
    auto digits = [](std::string_view d) {
        for (char c : d)
            if (c < '0' || c > '9') return false;
        return true;
    };
    // (ddddd)(?:-dddd)? fully, not all zeros and dashes: the five digits.
    auto zip = [&](std::string_view g) -> std::optional<std::string> {
        const bool five = g.size() == 5 && digits(g);
        const bool nine = g.size() == 10 && digits(g.substr(0, 5)) && g[5] == '-' && digits(g.substr(6));
        if (!five && !nine) return std::nullopt;
        if (g.find_first_not_of("0- ") == std::string_view::npos) return std::nullopt;
        return std::string(g.substr(0, 5));
    };
    if (auto z = zip(s)) return z;
    if (s.size() >= 2 && (s[0] == 'U' || s[0] == 'u') && (s[1] == 'S' || s[1] == 's')) {
        const std::string_view rest = s.substr(2);
        if (auto z = zip(rest)) return z;
        if (!rest.empty() && (rest[0] == ' ' || rest[0] == '-')) return zip(rest.substr(1));
    }
    return std::nullopt;
}

// A postcode the grid names: its country and code. A build's codes are kept
// in (country, code) order, so a code's index orders as it does.
struct GridCode {
    uint16_t country = 0;
    std::string code;
};

// A vote, and a cell of the grid's: the cell's position along the curve
// (its id >> cell_shift(level)) and the index of a postcode among the
// build's codes, as pos << CODE_BITS | index.
inline constexpr int CODE_BITS = 20;
inline constexpr uint64_t MAX_CODES = uint64_t(1) << CODE_BITS;
static_assert(2 * MAX_LEVEL + 3 + CODE_BITS <= 64, "a vote's position and code share a u64");
inline constexpr uint64_t vote(uint64_t pos, uint32_t code) { return pos << CODE_BITS | code; }
inline constexpr uint64_t pos_of(uint64_t v) { return v >> CODE_BITS; }
inline constexpr uint32_t code_index(uint64_t v) { return uint32_t(v & (MAX_CODES - 1)); }

// The cells holding votes, ascending, each with its postcode: the one most
// of its votes name, the least index of those named most. Sorts `votes`.
inline std::vector<uint64_t> cell_postcodes(std::vector<uint64_t>& votes, unsigned threads = 0) {
    parallel_sort(votes.begin(), votes.end(), [](uint64_t a, uint64_t b) { return a < b; }, threads);
    std::vector<uint64_t> cells;
    for (size_t i = 0; i < votes.size();) {
        const uint64_t pos = pos_of(votes[i]);
        uint64_t best = votes[i];
        size_t best_n = 0;
        while (i < votes.size() && pos_of(votes[i]) == pos) {
            size_t j = i;
            while (j < votes.size() && votes[j] == votes[i]) j++;
            if (j - i > best_n) best = votes[i], best_n = j - i;
            i = j;
        }
        cells.push_back(best);
    }
    return cells;
}

// The grid's rows at `level` of its cells (cell_postcodes, of `codes`): a
// run from each cell holding votes (or from the first cell it reaches)
// whose postcode isn't the run's before it, and one of no postcode where a
// gap's cells are past the reach of the cells either side of it.
inline std::vector<LogicalRow> grid_rows(const std::vector<uint64_t>& cells, const std::vector<GridCode>& codes, int level) {
    std::vector<LogicalRow> rows;
    if (cells.empty()) return rows;
    if (level < 0 || level > MAX_LEVEL) throw std::logic_error("postcode_grid::grid_rows: a level past the grid's");
    const uint64_t end = cells_at(level);
    std::vector<uint64_t> reach(codes.size());
    for (size_t i = 0; i < codes.size(); i++) {
        const GridCountry* c = grid_country(codes[i].country);
        if (!c) throw std::logic_error("postcode_grid::grid_rows: a code of a country the grid doesn't cover");
        reach[i] = reach_cells(c->extent_m, level);
    }
    // A code of no postcode, for the gaps.
    constexpr uint32_t NONE = UINT32_MAX;
    uint32_t last = NONE;
    auto run = [&](uint64_t pos, uint32_t code) {
        if (pos >= end || (!rows.empty() && code == last)) return;
        if (rows.empty() && code == NONE) return;
        rows.push_back(code == NONE ? row_of(cell_at(pos, level), 0, {}) : row_of(cell_at(pos, level), codes[code].country, codes[code].code));
        last = code;
    };
    const uint32_t first = code_index(cells[0]);
    run(pos_of(cells[0]) - std::min(pos_of(cells[0]), reach[first]), first);
    for (size_t i = 1; i < cells.size(); i++) {
        const uint64_t a = pos_of(cells[i - 1]), b = pos_of(cells[i]);
        const uint32_t ca = code_index(cells[i - 1]), cb = code_index(cells[i]);
        // The gap's cells a + t, t of 1 to l: a's while t is at most the
        // last it takes (nearer a, or out of b's reach, and within a's), b's
        // from the first b takes, none between.
        const uint64_t l = b - a - 1, half = (l + 1) / 2;
        const uint64_t a_to = std::min({reach[ca], std::max(l - std::min(l, reach[cb]), half), l});
        const uint64_t b_from = std::max(l + 1 - std::min(l, reach[cb]), std::min(reach[ca] + 1, half + 1));
        if (b_from > a_to + 1) run(a + a_to + 1, NONE);
        run(a + b_from, cb);
    }
    const uint64_t z = pos_of(cells.back());
    run(z + reach[code_index(cells.back())] + 1, NONE);
    return rows;
}

// The rows a region's grid holds of the planet's (grid_rows, at `level`):
// each of the planet's runs that holds a cell a query routed to the region
// reads, whole, and none in place of the others. served(id) says whether the
// region serves the cell of id, a cell at served_level (0 to 30, coarser or
// finer than the grid's): the server routes the queries in it there. A run's cells come from votes on
// either side of the region's edge, so the region answers as the planet does
// at every point routed to it, which a grid of the region's votes alone
// would not. served may be called more than once for a cell.
template <class Served>
std::vector<LogicalRow> region_rows(const std::vector<LogicalRow>& rows, int level, int served_level, Served served) {
    std::vector<LogicalRow> out;
    if (level < 0 || level > MAX_LEVEL || served_level < 0 || served_level > 30)
        throw std::logic_error("postcode_grid::region_rows: levels past the grid's or S2's");
    const bool coarser = served_level <= level;
    const int shift = 2 * (coarser ? level - served_level : served_level - level);
    const uint64_t end = cells_at(level);
    for (size_t i = 0; i < rows.size(); i++) {
        // The run's cells [from, to), and the served cells over them [lo, hi).
        const uint64_t from = rows[i].cell >> cell_shift(level);
        const uint64_t to = i + 1 < rows.size() ? rows[i + 1].cell >> cell_shift(level) : end;
        const uint64_t lo = coarser ? from >> shift : from << shift, hi = coarser ? ((to - 1) >> shift) + 1 : to << shift;
        bool keep = false;
        for (uint64_t up = lo; rows[i].country_code && !keep && up < hi; up++) keep = served(cell_at(up, served_level));
        const LogicalRow r = keep ? rows[i] : row_of(rows[i].cell, 0, {});
        if (out.empty() ? !keep : same_postcode(out.back(), r)) continue;
        out.push_back(r);
    }
    return out;
}

// The grid's postcodes, (country, code) ascending, and its cells holding
// votes (cell_postcodes, of those codes), with the votes of each kind.
struct GridCells {
    std::vector<GridCode> codes;
    std::vector<uint64_t> cells;
    size_t address_votes = 0, tiger_votes = 0;
};

// The cells of a build's votes: each address point with a postcode string
// (addr_postcodes, parallel to addrs) and each line of a ZIP
// (interp_postcodes, parallel to interps, NO_DATA for none: TIGER's lines),
// at the point halfway along it, whose country (country_at(lat, lng), a
// packed code) is a grid country that takes the string as a postcode
// (grid_postcode), in the cell pos_at(lat, lng) gives the position of.
// string_of(id) gives a postcode string's text. The same whatever `threads`
// runs it.
template <class StringOf, class CountryAt, class PosAt>
GridCells grid_cells(const std::vector<AddrPoint>& addrs, const std::vector<uint32_t>& addr_postcodes,
                     const std::vector<InterpWay>& interps, const std::vector<NodeCoord>& interp_nodes,
                     const std::vector<uint32_t>& interp_postcodes, StringOf string_of, CountryAt country_at, PosAt pos_at,
                     unsigned threads = 0) {
    constexpr size_t COUNTRIES = std::size(GRID_COUNTRIES);
    constexpr uint32_t NONE = UINT32_MAX;
    const size_t n_addrs = std::min(addrs.size(), addr_postcodes.size()), n_interps = std::min(interps.size(), interp_postcodes.size());
    // The strings the points name, each once: each range's, then all.
    const unsigned workers = threads ? threads : parallel_threads();
    std::vector<std::vector<uint32_t>> parts(workers);
    parallel_for(n_addrs + n_interps, [&](size_t begin, size_t end, unsigned w) {
        std::vector<uint32_t>& part = parts[w];
        for (size_t i = begin; i < end; i++) {
            const uint32_t id = i < n_addrs ? addr_postcodes[i] : interp_postcodes[i - n_addrs];
            if (id != NO_DATA) part.push_back(id);
        }
        std::sort(part.begin(), part.end());
        part.erase(std::unique(part.begin(), part.end()), part.end());
    }, workers);
    std::vector<uint32_t> ids;
    for (auto& part : parts) ids.insert(ids.end(), part.begin(), part.end()), std::vector<uint32_t>().swap(part);
    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
    // Each string's postcode in each grid country, and the codes they make.
    GridCells out;
    std::vector<std::pair<GridCode, size_t>> named;  // code, ids index * COUNTRIES + country
    for (size_t k = 0; k < ids.size(); k++)
        for (size_t c = 0; c < COUNTRIES; c++)
            if (auto code = grid_postcode(GRID_COUNTRIES[c].code, string_of(ids[k])))
                named.push_back({GridCode{GRID_COUNTRIES[c].code, std::move(*code)}, k * COUNTRIES + c});
    std::sort(named.begin(), named.end(), [](const auto& a, const auto& b) {
        return postcode_less(a.first.country, a.first.code, b.first.country, b.first.code) ||
               (!postcode_less(b.first.country, b.first.code, a.first.country, a.first.code) && a.second < b.second);
    });
    std::vector<uint32_t> code_at(ids.size() * COUNTRIES, NONE);
    for (const auto& [code, at] : named) {
        if (out.codes.empty() || out.codes.back().country != code.country || out.codes.back().code != code.code) {
            if (out.codes.size() == MAX_CODES) throw std::runtime_error("postcode grid: more than 2^20 postcodes");
            out.codes.push_back(code);
        }
        code_at[at] = uint32_t(out.codes.size() - 1);
    }
    // A point's vote, or none.
    auto vote_at = [&](double lat, double lng, uint32_t id, std::vector<uint64_t>& votes) {
        const size_t k = size_t(std::lower_bound(ids.begin(), ids.end(), id) - ids.begin());
        bool any = false;
        for (size_t c = 0; c < COUNTRIES; c++) any |= code_at[k * COUNTRIES + c] != NONE;
        if (!any) return;
        const uint16_t cc = country_at(lat, lng);
        for (size_t c = 0; c < COUNTRIES; c++)
            if (GRID_COUNTRIES[c].code == cc && code_at[k * COUNTRIES + c] != NONE)
                votes.push_back(vote(pos_at(lat, lng), code_at[k * COUNTRIES + c]));
    };
    std::vector<uint64_t> votes = parallel_collect<uint64_t>(n_addrs, [&](size_t i, std::vector<uint64_t>& v) {
        if (addr_postcodes[i] != NO_DATA) vote_at(addrs[i].lat, addrs[i].lng, addr_postcodes[i], v);
    }, threads);
    out.address_votes = votes.size();
    std::vector<uint64_t> lines = parallel_collect<uint64_t>(n_interps, [&](size_t i, std::vector<uint64_t>& v) {
        const InterpWay& w = interps[i];
        if (interp_postcodes[i] == NO_DATA || !w.node_count || size_t(w.node_offset) + w.node_count > interp_nodes.size()) return;
        const PackedLocation mid = line_middle(interp_nodes.data() + w.node_offset, w.node_count);
        vote_at(mid.lat(), mid.lon(), interp_postcodes[i], v);
    }, threads);
    out.tiger_votes = lines.size();
    votes.insert(votes.end(), lines.begin(), lines.end());
    std::vector<uint64_t>().swap(lines);
    out.cells = cell_postcodes(votes, threads);
    return out;
}

}  // namespace postcode_grid
