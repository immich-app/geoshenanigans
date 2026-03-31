#include "interpolation.h"
#include "geometry.h"

#include <cstdint>
#include <cstring>
#include <iostream>
#include <unordered_map>

void resolve_interpolation_endpoints(ParsedData& data) {
    struct CoordKey {
        int32_t lat;
        int32_t lng;
        bool operator==(const CoordKey& o) const { return lat == o.lat && lng == o.lng; }
    };
    struct CoordHash {
        size_t operator()(const CoordKey& k) const {
            return std::hash<int64_t>()(((int64_t)k.lat << 32) | (uint32_t)k.lng);
        }
    };

    const auto& sp = data.string_pool.data();
    auto get_str = [&](uint32_t off) -> const char* {
        return sp.data() + off;
    };

    // Deterministic ordering: compare housenumber first, then street name
    auto addr_less = [&](uint32_t a, uint32_t b) -> bool {
        int cmp = strcmp(get_str(data.addr_points[a].housenumber_id),
                         get_str(data.addr_points[b].housenumber_id));
        if (cmp != 0) return cmp < 0;
        return strcmp(get_str(data.addr_points[a].street_id),
                      get_str(data.addr_points[b].street_id)) < 0;
    };

    std::unordered_map<CoordKey, uint32_t, CoordHash> addr_by_coord;
    for (uint32_t i = 0; i < data.addr_points.size(); i++) {
        CoordKey key{
            static_cast<int32_t>(data.addr_points[i].lat * 100000),
            static_cast<int32_t>(data.addr_points[i].lng * 100000)
        };
        auto [it, inserted] = addr_by_coord.emplace(key, i);
        if (!inserted) {
            // Collision: keep the deterministically "smallest" address
            if (addr_less(i, it->second))
                it->second = i;
        }
    }

    uint32_t resolved = 0;
    for (auto& iw : data.interp_ways) {
        if (iw.node_count < 2) continue;

        const auto& start = data.interp_nodes[iw.node_offset];
        CoordKey start_key{
            static_cast<int32_t>(start.lat * 100000),
            static_cast<int32_t>(start.lng * 100000)
        };
        auto it_start = addr_by_coord.find(start_key);

        const auto& end = data.interp_nodes[iw.node_offset + iw.node_count - 1];
        CoordKey end_key{
            static_cast<int32_t>(end.lat * 100000),
            static_cast<int32_t>(end.lng * 100000)
        };
        auto it_end = addr_by_coord.find(end_key);

        if (it_start != addr_by_coord.end()) {
            const char* hn = data.string_pool.data().data() + data.addr_points[it_start->second].housenumber_id;
            iw.start_number = parse_house_number(hn);
        }
        if (it_end != addr_by_coord.end()) {
            const char* hn = data.string_pool.data().data() + data.addr_points[it_end->second].housenumber_id;
            iw.end_number = parse_house_number(hn);
        }

        if (iw.start_number > 0 && iw.end_number > 0) resolved++;
    }

    std::cerr << "Resolved " << resolved << "/" << data.interp_ways.size()
              << " interpolation ways" << std::endl;
}
