#pragma once
// A node location as the PBF carries it, in 1e-7 degree units: 8 B against
// the 16 of a (lat, lng) double pair. Way and ring coordinates stay packed;
// every double the builder computes with is lat() / lon() of one, so it is
// the same double wherever it is made.
#include <cstdint>

struct PackedLocation {
    int32_t lat_e7;  // latitude * 10^7
    int32_t lon_e7;  // longitude * 10^7
    bool valid() const { return lat_e7 != 0 || lon_e7 != 0; }
    double lat() const { return lat_e7 / 10000000.0; }
    double lon() const { return lon_e7 / 10000000.0; }
};

inline bool operator==(const PackedLocation& a, const PackedLocation& b) {
    return a.lat_e7 == b.lat_e7 && a.lon_e7 == b.lon_e7;
}
inline bool operator!=(const PackedLocation& a, const PackedLocation& b) { return !(a == b); }
// By latitude, then longitude: the order of the (lat(), lon()) pairs, as
// lat() and lon() are strictly increasing in the e7 values.
inline bool operator<(const PackedLocation& a, const PackedLocation& b) {
    return a.lat_e7 != b.lat_e7 ? a.lat_e7 < b.lat_e7 : a.lon_e7 < b.lon_e7;
}

inline PackedLocation pack_location(double lat, double lng) {
    return {static_cast<int32_t>(lat * 10000000.0 + (lat >= 0 ? 0.5 : -0.5)),
            static_cast<int32_t>(lng * 10000000.0 + (lng >= 0 ? 0.5 : -0.5))};
}
