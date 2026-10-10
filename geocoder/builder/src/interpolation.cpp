#include "interpolation.h"
#include "geometry.h"
#include "parallel.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <iostream>

void resolve_interpolation_endpoints(ParsedData& data) {
    const auto& sp = data.string_pool.data();
    auto get_str = [&](uint32_t off) -> const char* {
        return sp.data() + off;
    };

    // Deterministic ordering: compare housenumber first, then street name.
    // Guards against street_id == NO_DATA (sentinel for addr_points with
    // no addr:street, pending parent-street backfill) — dereferencing a
    // NO_DATA string offset would read out of bounds.
    auto addr_less = [&](uint32_t a, uint32_t b) -> bool {
        int cmp = strcmp(get_str(data.addr_points[a].housenumber_id),
                         get_str(data.addr_points[b].housenumber_id));
        if (cmp != 0) return cmp < 0;
        uint32_t sa = data.addr_points[a].street_id;
        uint32_t sb = data.addr_points[b].street_id;
        if (sa == NO_DATA || sb == NO_DATA) return sa < sb;
        return strcmp(get_str(sa), get_str(sb)) < 0;
    };

    // A point's coordinate bucket: lat/lng truncated to 1e-5 degrees, packed
    // so equal buckets give equal keys.
    auto key_of = [](float lat, float lng) -> uint64_t {
        auto la = static_cast<int32_t>(lat * 100000);
        auto ln = static_cast<int32_t>(lng * 100000);
        return (uint64_t(uint32_t(la)) << 32) | uint32_t(ln);
    };

    // Every address by bucket, then index: within a bucket the address that
    // stands for it is the smallest under addr_less, the earliest on ties.
    struct Keyed { uint64_t key; uint32_t index; };
    const size_t n = data.addr_points.size();
    std::vector<Keyed> keyed(n);
    parallel_for(n, [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++)
            keyed[i] = {key_of(data.addr_points[i].lat, data.addr_points[i].lng), static_cast<uint32_t>(i)};
    });
    parallel_sort(keyed.begin(), keyed.end(), [](const Keyed& x, const Keyed& y) {
        return x.key != y.key ? x.key < y.key : x.index < y.index;
    });
    // Collapse each bucket's run to its representative, in place: a run's
    // first slot takes the winner, the rest are dropped below.
    parallel_for_runs(n, [&](size_t i) { return keyed[i].key == keyed[i - 1].key; },
        [&](size_t b, size_t e, unsigned) {
            for (size_t run = b; run < e;) {
                size_t next = run + 1;
                uint32_t best = keyed[run].index;
                for (; next < e && keyed[next].key == keyed[run].key; next++)
                    if (addr_less(keyed[next].index, best)) best = keyed[next].index;
                keyed[run].index = best;
                for (size_t k = run + 1; k < next; k++) keyed[k].index = NO_DATA;
                run = next;
            }
        });
    keyed.erase(std::remove_if(keyed.begin(), keyed.end(), [](const Keyed& k) { return k.index == NO_DATA; }),
                keyed.end());
    auto find = [&](uint64_t key) -> const Keyed* {
        auto it = std::lower_bound(keyed.begin(), keyed.end(), key,
                                   [](const Keyed& k, uint64_t v) { return k.key < v; });
        return it != keyed.end() && it->key == key ? &*it : nullptr;
    };

    std::atomic<uint32_t> resolved{0};
    parallel_for(data.interp_ways.size(), [&](size_t b, size_t e, unsigned) {
        uint32_t local = 0;
        for (size_t w = b; w < e; w++) {
            auto& iw = data.interp_ways[w];
            if (iw.node_count < 2) continue;
            const auto& start = data.interp_nodes[iw.node_offset];
            const auto& end = data.interp_nodes[iw.node_offset + iw.node_count - 1];
            if (const Keyed* s = find(key_of(start.lat, start.lng)))
                iw.start_number = parse_house_number(get_str(data.addr_points[s->index].housenumber_id));
            if (const Keyed* t = find(key_of(end.lat, end.lng)))
                iw.end_number = parse_house_number(get_str(data.addr_points[t->index].housenumber_id));
            if (iw.start_number > 0 && iw.end_number > 0) local++;
        }
        resolved += local;
    });

    std::cerr << "Resolved " << resolved << "/" << data.interp_ways.size()
              << " interpolation ways" << std::endl;
}
