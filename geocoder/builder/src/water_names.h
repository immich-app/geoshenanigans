// How a river or canal area with no name of its own takes one: the waterway
// with the most length inside it. Each candidate line is a waterway=river or
// canal way (a run of one, where the input lacks a node of it), keyed by the
// type=waterway relation it belongs to (the lowest relation id, where it
// belongs to two) or else by its own local name (the name tag, not
// name:en), so a river whose ways vary in name:en is one key.
// Each segment of a candidate line credits each area polygon the length of
// its stretches inside the polygon's rings, in integer millimetres, to the
// polygon's object (SegmentClip). The object takes the key of the largest
// total, a tie going to the smaller key; its name is then the display name
// (name:en, else name) with the most length among that key's lines inside
// it, a tie going to the smaller name, and its wikidata item the lowest of
// the key's lines that credit it. Integer sums and total orders: the same
// answer on any number of threads.
//
// A name so taken is weak where its key's lines run only a short way into
// the object and their waterway is short beside it (weak_name): a creek or
// tributary whose line ends at the middle of the river it flows into names
// that river's unnamed area, which the location line then doesn't trust (the
// East River as "Newtown Creek"). A river's own line runs the length of its
// area, which is longer than it is wide, or the river runs on far past a
// piece of its delta, estuary or bank its line crosses a short way in.
#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <string>
#include <vector>

#include "packed_location.h"
#include "parallel.h"

namespace water {

// A candidate line: `count` vertices from `first` in its set's vertices, its
// key and display name (indices into the set's sorted key and display lists),
// its wikidata item (0 for none) and the length of its waterway
// (measure_waterways).
struct NamingLine {
    int64_t way;
    uint32_t key;
    uint32_t display;
    uint32_t qid;
    uint64_t first;
    uint32_t count;
    uint64_t waterway_mm = 0;
};

// Every candidate line of a build, in way order, and its keys and names,
// each list sorted and unique: key and display indices compare as the
// strings do.
struct NamingLines {
    std::vector<NamingLine> lines;
    std::vector<PackedLocation> vertices;
    std::vector<std::string> keys;       // "r<relation id>" or "n<local name>"
    std::vector<std::string> key_local;  // parallel to keys: the local name of its relation or the name itself
    std::vector<std::string> displays;   // what an area named by a key shows
};

// A segment's length on the sphere in millimetres, and in whole ones.
inline double segment_length_mm(PackedLocation a, PackedLocation b) {
    constexpr double kEarthRadiusMm = 6371008.8 * 1000.0;
    constexpr double kRad = 3.14159265358979323846 / 180.0 / 1e7;
    const double lat1 = a.lat_e7 * kRad, lat2 = b.lat_e7 * kRad;
    const double dlat = lat2 - lat1, dlng = (double(b.lon_e7) - a.lon_e7) * kRad;
    const double h = std::sin(dlat / 2) * std::sin(dlat / 2) +
                     std::cos(lat1) * std::cos(lat2) * std::sin(dlng / 2) * std::sin(dlng / 2);
    return 2 * kEarthRadiusMm * std::asin(std::min(1.0, std::sqrt(h)));
}
inline uint64_t segment_mm(PackedLocation a, PackedLocation b) {
    return static_cast<uint64_t>(std::llround(segment_length_mm(a, b)));
}

// Whether a segment's ends lie more than 180 degrees of longitude apart: a
// way across the antimeridian, which OSM splits there, whose straight line
// in longitude runs the other way round from its length. Such a segment
// credits nothing.
inline bool segment_wraps(PackedLocation a, PackedLocation b) {
    return std::abs(int64_t(b.lon_e7) - a.lon_e7) > 1800000000;
}

// A line's length: its segments', those across the antimeridian aside, as
// they credit areas.
inline uint64_t naming_line_mm(const NamingLines& lines, const NamingLine& line) {
    const PackedLocation* v = lines.vertices.data() + line.first;
    uint64_t mm = 0;
    for (uint32_t i = 0; i + 1 < line.count; i++)
        if (!segment_wraps(v[i], v[i + 1])) mm += segment_mm(v[i], v[i + 1]);
    return mm;
}

// Sets each line's waterway_mm: the length of the waterway it is part of. A
// key of a waterway relation is one waterway, all its lines. A key of a name
// is every waterway of that name: there the lines of the key that join the
// line, one joining another where an end of either is a vertex of the other
// (OSM's ways of a river meet at their end nodes), and so on along them.
// Sums over sets, so the same on any number of threads.
inline void measure_waterways(NamingLines& lines, unsigned threads = 0) {
    const size_t n = lines.lines.size();
    auto of_relation = [&](uint32_t key) {
        return key < lines.keys.size() && !lines.keys[key].empty() && lines.keys[key][0] == 'r';
    };
    struct End {
        uint32_t key;
        int32_t lat, lon;
        uint32_t line;
    };
    auto end_less = [](const End& a, const End& b) {
        if (a.key != b.key) return a.key < b.key;
        if (a.lat != b.lat) return a.lat < b.lat;
        if (a.lon != b.lon) return a.lon < b.lon;
        return a.line < b.line;
    };
    std::vector<End> ends;
    ends.reserve(2 * n);
    for (size_t l = 0; l < n; l++) {
        const NamingLine& line = lines.lines[l];
        if (line.count == 0 || of_relation(line.key)) continue;
        const PackedLocation a = lines.vertices[line.first], b = lines.vertices[line.first + line.count - 1];
        ends.push_back({line.key, a.lat_e7, a.lon_e7, static_cast<uint32_t>(l)});
        ends.push_back({line.key, b.lat_e7, b.lon_e7, static_cast<uint32_t>(l)});
    }
    parallel_sort(ends.begin(), ends.end(), end_less, threads);
    // Each (line, other line) an end of the other meets at a vertex of the
    // line.
    const std::vector<std::pair<uint32_t, uint32_t>> joins =
        parallel_collect<std::pair<uint32_t, uint32_t>>(n, [&](size_t l, std::vector<std::pair<uint32_t, uint32_t>>& out) {
            const NamingLine& line = lines.lines[l];
            if (of_relation(line.key)) return;
            const PackedLocation* v = lines.vertices.data() + line.first;
            for (uint32_t i = 0; i < line.count; i++) {
                const End probe{line.key, v[i].lat_e7, v[i].lon_e7, 0};
                for (auto it = std::lower_bound(ends.begin(), ends.end(), probe, end_less);
                     it != ends.end() && it->key == line.key && it->lat == v[i].lat_e7 && it->lon == v[i].lon_e7; ++it)
                    if (it->line != l) out.push_back({static_cast<uint32_t>(l), it->line});
            }
        }, threads);
    std::vector<End>().swap(ends);
    std::vector<uint32_t> root(n);
    for (size_t l = 0; l < n; l++) root[l] = static_cast<uint32_t>(l);
    auto find = [&](uint32_t x) {
        while (root[x] != x) x = root[x] = root[root[x]];
        return x;
    };
    auto join = [&](uint32_t a, uint32_t b) {
        const uint32_t ra = find(a), rb = find(b);
        if (ra != rb) root[std::max(ra, rb)] = std::min(ra, rb);
    };
    for (const auto& [a, b] : joins) join(a, b);
    std::vector<uint32_t> first_of_key(lines.keys.size(), UINT32_MAX);
    for (size_t l = 0; l < n; l++) {
        const uint32_t key = lines.lines[l].key;
        if (!of_relation(key)) continue;
        if (first_of_key[key] == UINT32_MAX)
            first_of_key[key] = static_cast<uint32_t>(l);
        else
            join(first_of_key[key], static_cast<uint32_t>(l));
    }
    std::vector<uint64_t> own(n), mm(n, 0);
    parallel_for_dynamic(n, 4096, [&](size_t begin, size_t end, unsigned) {
        for (size_t l = begin; l < end; l++) own[l] = naming_line_mm(lines, lines.lines[l]);
    }, threads);
    for (size_t l = 0; l < n; l++) mm[find(static_cast<uint32_t>(l))] += own[l];
    for (size_t l = 0; l < n; l++) lines.lines[l].waterway_mm = mm[find(static_cast<uint32_t>(l))];
}

// The share of a segment, a straight line in (lat, lng), inside rings
// counted even-odd, from the rings' edges: where they cross it, and whether
// its start is inside, both in integers. Each edge is fed as add_crossing
// takes it, and the ring's edges that can cross the ray of start_crossed_by
// (those whose longitudes reach the start's) to it. The segment runs from
// its end of smaller latitude, then longitude, so either way round gives the
// same share; a stretch of it along an edge counts as inside where the side
// to its left is (the start and the line taken just to the left of it).
class SegmentClip {
public:
    SegmentClip(PackedLocation a, PackedLocation b) {
        if (b.lat_e7 < a.lat_e7 || (b.lat_e7 == a.lat_e7 && b.lon_e7 < a.lon_e7)) std::swap(a, b);
        ax_ = a.lon_e7;
        ay_ = a.lat_e7;
        dx_ = int64_t(b.lon_e7) - a.lon_e7;
        dy_ = int64_t(b.lat_e7) - a.lat_e7;
    }

    // The end the segment runs from.
    PackedLocation start() const { return {static_cast<int32_t>(ay_), static_cast<int32_t>(ax_)}; }

    // Whether edge p-q crosses the ray towards +lat from the point just past
    // the start along the segment and just to its left, as
    // exact_edge_crosses takes a ray (either direction of the edge alike):
    // an odd number of a ring's edges puts the start inside the ring.
    bool start_crossed_by(PackedLocation p, PackedLocation q) const {
        // East of the point: east of the start, or at its longitude and the
        // segment (else its left) running west.
        auto east = [&](int64_t x) { return x != ax_ ? x > ax_ : dx_ != 0 ? dx_ < 0 : dy_ > 0; };
        if (east(p.lon_e7) == east(q.lon_e7)) return false;
        // Which side of the edge the point is: the start's, else the side the
        // segment leaves it to, else the side to its left.
        const int64_t ex = int64_t(q.lon_e7) - p.lon_e7, ey = int64_t(q.lat_e7) - p.lat_e7;
        __int128 f = static_cast<__int128>(ay_ - p.lat_e7) * ex - static_cast<__int128>(ey) * (ax_ - p.lon_e7);
        if (f == 0) f = static_cast<__int128>(dy_) * ex - static_cast<__int128>(ey) * dx_;
        if (f == 0) f = static_cast<__int128>(dx_) * ex + static_cast<__int128>(dy_) * ey;
        return (f < 0) != (q.lon_e7 < p.lon_e7);
    }

    // Notes where edge p-q crosses the segment strictly between its ends,
    // the segment's line taken just to its left: a vertex on the line is
    // right of it.
    void add_crossing(PackedLocation p, PackedLocation q) {
        auto left = [&](PackedLocation v) {
            return static_cast<__int128>(dx_) * (int64_t(v.lat_e7) - ay_) -
                       static_cast<__int128>(dy_) * (int64_t(v.lon_e7) - ax_) > 0;
        };
        if (left(p) == left(q)) return;
        const int64_t ex = int64_t(q.lon_e7) - p.lon_e7, ey = int64_t(q.lat_e7) - p.lat_e7;
        __int128 num = static_cast<__int128>(int64_t(p.lon_e7) - ax_) * ey -
                       static_cast<__int128>(int64_t(p.lat_e7) - ay_) * ex;
        __int128 den = static_cast<__int128>(dx_) * ey - static_cast<__int128>(dy_) * ex;
        if (den < 0) {
            num = -num;
            den = -den;
        }
        // Each a difference of two products under 2^63, so under 2^64.
        if (num > 0 && num < den) crossings_.push_back({static_cast<uint64_t>(num), static_cast<uint64_t>(den)});
    }

    // The share inside, from 0 to 1: the stretches between the crossings,
    // in order along the segment, alternately inside and out from the start
    // (`start_inside`). Uses up the crossings.
    long double share(bool start_inside) {
        std::sort(crossings_.begin(), crossings_.end(), [](const Crossing& x, const Crossing& y) {
            return static_cast<unsigned __int128>(x.num) * y.den < static_cast<unsigned __int128>(y.num) * x.den;
        });
        long double inside = 0, from = 0;
        bool in = start_inside;
        for (const Crossing& c : crossings_) {
            const long double t = static_cast<long double>(c.num) / static_cast<long double>(c.den);
            if (in) inside += t - from;
            from = t;
            in = !in;
        }
        if (in) inside += 1 - from;
        crossings_.clear();
        return inside;
    }

private:
    struct Crossing {
        uint64_t num, den;  // along the segment at num / den, 0 < num < den
    };
    int64_t ax_, ay_, dx_, dy_;
    std::vector<Crossing> crossings_;
};

// A waterway is long beside an area from this many sides of a square of the
// area. Calibrated on the planet's waterway names: of the names whose lines
// run under half a side in, those of a waterway shorter than this mostly
// have an end of the waterway inside the area (a tributary's mouth), and
// those of a longer one mostly not (a river's own line past a piece of its
// delta, estuary or bank). Rivers and canals split at the same length.
inline constexpr double kLongWaterwaySides = 64.0;

// Whether a name an object took from lines of `mm` millimetres inside it is
// weak, the object `km2` in area and the waterway those lines are part of
// `waterway_mm` long (measure_waterways; as long as its lines inside at
// least): the lines run under half the side of a square of that area inside
// it, and the waterway under kLongWaterwaySides sides of it all told. So a
// creek whose line ends in the river it flows into names that river's
// unnamed area weakly (the East River as "Newtown Creek": 424 m into
// 6.67 km², 5.6 km long), and a river's own line through a piece of its
// delta at its side names it as any other (the Lena's: 500 m into 537 km²,
// 6,730 km long).
inline bool weak_name(uint64_t mm, double km2, uint64_t waterway_mm) {
    const double km = static_cast<double>(mm) / 1e6;
    const double waterway_km = static_cast<double>(std::max(mm, waterway_mm)) / 1e6;
    return 4 * km * km < km2 && waterway_km * waterway_km < kLongWaterwaySides * kLongWaterwaySides * km2;
}

// The whole millimetres of segment a-b that `share` of it (SegmentClip)
// holds: the segment's own (segment_mm) for all of it.
inline uint64_t share_mm(PackedLocation a, PackedLocation b, long double share) {
    if (share >= 1) return segment_mm(a, b);
    return static_cast<uint64_t>(std::llround(static_cast<double>(share * segment_length_mm(a, b))));
}

// A line's length inside an area object: `mm` of a line of `key` and
// `display`, whose wikidata item is `qid` (0 for none), credited to
// `object`, and the length of the line's waterway (the longest of the lines
// a credit sums).
struct Credit {
    uint32_t object;
    uint32_t key;
    uint32_t display;
    uint64_t mm;
    uint32_t qid = 0;
    uint64_t waterway_mm = 0;
};

// The name an object takes: its winning key and display, the key's total
// and the runner-up key's (0 for none), the lowest wikidata item of the
// key's lines that credit it (0 for none) and the length of the longest
// waterway of those lines.
struct NameChoice {
    uint32_t key = UINT32_MAX;
    uint32_t display = UINT32_MAX;
    uint64_t mm = 0;
    uint64_t runner_up_mm = 0;
    uint32_t qid = 0;
    uint64_t waterway_mm = 0;

    bool named() const { return key != UINT32_MAX; }
};

// Each of `objects` objects' choice from the credits (in any order): sorted
// by object, key and display, then summed. Uses up `credits`.
inline std::vector<NameChoice> pick_names(std::vector<Credit>& credits, size_t objects, unsigned threads = 0) {
    parallel_sort(credits.begin(), credits.end(), [](const Credit& a, const Credit& b) {
        if (a.object != b.object) return a.object < b.object;
        if (a.key != b.key) return a.key < b.key;
        return a.display < b.display;
    }, threads);
    std::vector<NameChoice> out(objects);
    size_t i = 0;
    while (i < credits.size()) {
        const uint32_t object = credits[i].object;
        NameChoice& best = out.at(object);
        while (i < credits.size() && credits[i].object == object) {
            const uint32_t key = credits[i].key;
            uint64_t total = 0, top_mm = 0, waterway_mm = 0;
            uint32_t top_display = UINT32_MAX, qid = 0;
            while (i < credits.size() && credits[i].object == object && credits[i].key == key) {
                const uint32_t display = credits[i].display;
                uint64_t display_mm = 0;
                while (i < credits.size() && credits[i].object == object && credits[i].key == key &&
                       credits[i].display == display) {
                    if (credits[i].qid && (!qid || credits[i].qid < qid)) qid = credits[i].qid;
                    waterway_mm = std::max(waterway_mm, credits[i].waterway_mm);
                    display_mm += credits[i++].mm;
                }
                total += display_mm;
                if (display_mm > top_mm) {
                    top_mm = display_mm;
                    top_display = display;
                }
            }
            if (total == 0) continue;
            // Keys arrive ascending: a later one wins only with more.
            if (total > best.mm) {
                best.runner_up_mm = best.mm;
                best = {key, top_display, total, best.runner_up_mm, qid, waterway_mm};
            } else if (total > best.runner_up_mm) {
                best.runner_up_mm = total;
            }
        }
    }
    std::vector<Credit>().swap(credits);
    return out;
}

}  // namespace water
