// How a river or canal area with no name of its own takes one: the waterway
// with the most length inside it. Each candidate line is a waterway=river or
// canal way (a run of one, where the input lacks a node of it), keyed by the
// type=waterway relation it belongs to (the lowest relation id, where it
// belongs to two) or else by its own local name (the name tag, not
// name:en), so a river whose ways vary in name:en is one key.
// Each segment of a candidate line credits its length, in integer
// millimetres, to the area object that holds its midpoint (an object's
// outer rings together). The object takes the key of the largest total, a
// tie going to the smaller key; its name is then the display name (name:en,
// else name) with the most length among that key's lines inside it, a tie
// going to the smaller name, and its wikidata item the lowest of the key's
// lines that credit it. Integer sums and total orders: the same answer on
// any number of threads.
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
// key and display name (indices into the set's sorted key and display lists)
// and its wikidata item (0 for none).
struct NamingLine {
    int64_t way;
    uint32_t key;
    uint32_t display;
    uint32_t qid;
    uint64_t first;
    uint32_t count;
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

// A segment's length on the sphere, in whole millimetres.
inline uint64_t segment_mm(PackedLocation a, PackedLocation b) {
    constexpr double kEarthRadiusMm = 6371008.8 * 1000.0;
    constexpr double kRad = 3.14159265358979323846 / 180.0 / 1e7;
    const double lat1 = a.lat_e7 * kRad, lat2 = b.lat_e7 * kRad;
    const double dlat = lat2 - lat1, dlng = (double(b.lon_e7) - a.lon_e7) * kRad;
    const double h = std::sin(dlat / 2) * std::sin(dlat / 2) +
                     std::cos(lat1) * std::cos(lat2) * std::sin(dlng / 2) * std::sin(dlng / 2);
    return static_cast<uint64_t>(std::llround(2 * kEarthRadiusMm * std::asin(std::min(1.0, std::sqrt(h)))));
}

// A segment's midpoint on the e7 grid, rounded down: along the shorter way
// round, across the antimeridian for ends more than 180 degrees of
// longitude apart, and back on the world there.
inline PackedLocation segment_midpoint(PackedLocation a, PackedLocation b) {
    constexpr int64_t kHalfTurn = 1800000000, kTurn = 2 * kHalfTurn;
    int64_t lng_b = b.lon_e7;
    if (lng_b - a.lon_e7 > kHalfTurn) lng_b -= kTurn;
    else if (a.lon_e7 - lng_b > kHalfTurn) lng_b += kTurn;
    int64_t lng = (int64_t(a.lon_e7) + lng_b) >> 1;
    if (lng > kHalfTurn) lng -= kTurn;
    else if (lng < -kHalfTurn) lng += kTurn;
    return {static_cast<int32_t>((int64_t(a.lat_e7) + b.lat_e7) >> 1), static_cast<int32_t>(lng)};
}

// A line's length inside an area object: `mm` of a line of `key` and
// `display`, whose wikidata item is `qid` (0 for none), credited to
// `object`.
struct Credit {
    uint32_t object;
    uint32_t key;
    uint32_t display;
    uint64_t mm;
    uint32_t qid = 0;
};

// The name an object takes: its winning key and display, the key's total
// and the runner-up key's (0 for none), and the lowest wikidata item of the
// key's lines that credit it (0 for none).
struct NameChoice {
    uint32_t key = UINT32_MAX;
    uint32_t display = UINT32_MAX;
    uint64_t mm = 0;
    uint64_t runner_up_mm = 0;
    uint32_t qid = 0;

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
            uint64_t total = 0, top_mm = 0;
            uint32_t top_display = UINT32_MAX, qid = 0;
            while (i < credits.size() && credits[i].object == object && credits[i].key == key) {
                const uint32_t display = credits[i].display;
                uint64_t display_mm = 0;
                while (i < credits.size() && credits[i].object == object && credits[i].key == key &&
                       credits[i].display == display) {
                    if (credits[i].qid && (!qid || credits[i].qid < qid)) qid = credits[i].qid;
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
                best = {key, top_display, total, best.runner_up_mm, qid};
            } else if (total > best.runner_up_mm) {
                best.runner_up_mm = total;
            }
        }
    }
    std::vector<Credit>().swap(credits);
    return out;
}

}  // namespace water
