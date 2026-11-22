// Which OSM areas the water layer holds, and as what: the first row of the
// table below an object's tags match decides its kind (water_types.h), so
// the Caspian (natural=water, water=lake, place=sea) is a lake. Areas only:
// closed ways and multipolygon (or boundary) relations. A kind needs a name
// (name:en or name), but for river and canal areas, which can take the name
// of the waterway running through them (water_names.h). Out of scope:
// water=stream, ditch, drain and fish_pass (Nominatim imports none of them),
// water=* without natural=water (Nominatim needs natural=water), and
// natural=coastline.
//
//   natural=water + water=river, or waterway=riverbank   waterway/river (a river area)
//   natural=water + water=canal                           waterway/canal (a canal area)
//   natural=water + water=stream|ditch|drain|fish_pass    none
//   natural=water + water=<a fixed type>                  water/<type>
//   natural=water + water=<other>                         water/<value>, verbatim
//   natural=water (water= absent, yes, intermittent, tidal)  natural/water
//   landuse=reservoir                                     landuse/reservoir
//   landuse=basin                                         landuse/basin
//   natural=bay | strait | wetland | glacier              natural/<value>
//   place=sea                                             place/sea
//
// The major tier (water_major_keeps) holds areas of 100 km² or more and
// whole rivers: a river's areas together, by its waterway relation, its
// wikidata item or its name along connected areas, when they sum to 100 km²
// or more, or 5 km² or more with a wikidata item. An area or river in the
// previous build's major tier stays while it holds kMajorKeepPercent of its
// threshold, so one at the threshold doesn't come and go.
#pragma once

#include <cstdint>
#include <cstring>
#include <string>

#include "water_types.h"

namespace water {

// The tags the classifier reads.
struct Tags {
    const char* natural = nullptr;
    const char* water = nullptr;
    const char* waterway = nullptr;
    const char* landuse = nullptr;
    const char* place = nullptr;
};

// An object's kind: a fixed code, or a water=* value outside the fixed
// table (code 0, `verbatim` the value). A river or canal area can be named
// by its waterway.
struct Class {
    uint16_t code = 0;
    const char* verbatim = nullptr;
    bool river_area = false;

    bool any() const { return code != 0 || verbatim != nullptr; }
};

inline bool is(const char* v, const char* want) { return v && std::strcmp(v, want) == 0; }

inline Class classify(const Tags& t) {
    Class c;
    if (is(t.natural, "water")) {
        const char* w = t.water;
        if (is(w, "river")) return {code::kRiver, nullptr, true};
        if (is(w, "canal")) return {code::kCanal, nullptr, true};
        if (is(w, "stream") || is(w, "ditch") || is(w, "drain") || is(w, "fish_pass")) return c;
        if (!w || !*w || is(w, "yes") || is(w, "intermittent") || is(w, "tidal")) return {code::kNaturalWater, nullptr, false};
        if (const uint16_t fixed = fixed_code("water", w)) return {fixed, nullptr, false};
        return {0, w, false};
    }
    if (is(t.waterway, "riverbank")) return {code::kRiver, nullptr, true};
    if (is(t.landuse, "reservoir")) return {code::kLanduseReservoir, nullptr, false};
    if (is(t.landuse, "basin")) return {code::kLanduseBasin, nullptr, false};
    if (is(t.natural, "bay")) return {code::kNaturalBay, nullptr, false};
    if (is(t.natural, "strait")) return {code::kNaturalStrait, nullptr, false};
    if (is(t.natural, "wetland")) return {code::kWetland, nullptr, false};
    if (is(t.natural, "glacier")) return {code::kGlacier, nullptr, false};
    if (is(t.place, "sea")) return {code::kPlaceSea, nullptr, false};
    return c;
}

// Whether the layer keeps an area of class `c`: a named one, or a river or
// canal area, which its waterway may name.
inline bool in_scope(const Class& c, bool named) { return c.any() && (named || c.river_area); }

// Whether a way names river and canal areas it runs through (water_names.h):
// waterway=river or canal.
inline bool is_naming_waterway(const char* waterway) { return is(waterway, "river") || is(waterway, "canal"); }

// --- The major tier ---

inline constexpr double kMajorAreaKm2 = 100.0;
inline constexpr double kMajorWikidataRiverKm2 = 5.0;
inline constexpr double kMajorKeepPercent = 90.0;

// Whether an area (or a river's areas together) of `km2` is in the major
// tier: `river` a river's, `wikidata` with a wikidata item, `was_major` in
// the previous build's major tier.
inline bool water_major_keeps(double km2, bool river, bool wikidata, bool was_major) {
    const double scale = was_major ? kMajorKeepPercent / 100.0 : 1.0;
    if (km2 >= kMajorAreaKm2 * scale) return true;
    return river && wikidata && km2 >= kMajorWikidataRiverKm2 * scale;
}

}  // namespace water
