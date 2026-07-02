#include "ring_assembly.h"
#include "geometry.h"

#include <unordered_set>

std::vector<std::vector<std::pair<double,double>>> assemble_outer_rings(
    const std::vector<std::pair<int64_t, std::string>>& members,
    const std::unordered_map<int64_t, ParsedData::WayGeometry>& way_geoms,
    bool include_all_roles)
{
    // Coordinate-based ring assembly: split ways at shared internal nodes,
    // then stitch sub-ways at endpoints with backtracking.
    // coord_key() from geometry.h provides coordinate hashing.

    std::unordered_map<int64_t, int> coord_count;
    std::vector<const std::vector<std::pair<double,double>>*> way_coords;
    for (const auto& [way_id, role] : members) {
        if (!include_all_roles && role != "outer" && !role.empty()) continue;
        auto it = way_geoms.find(way_id);
        if (it == way_geoms.end() || it->second.coords.empty()) continue;
        way_coords.push_back(&it->second.coords);
        for (const auto& [lat, lng] : it->second.coords) {
            coord_count[coord_key(lat, lng)]++;
        }
    }

    std::unordered_set<int64_t> split_points;
    for (auto* coords : way_coords) {
        split_points.insert(coord_key(coords->front().first, coords->front().second));
        split_points.insert(coord_key(coords->back().first, coords->back().second));
    }
    for (auto& [ck, cnt] : coord_count) {
        if (cnt > 1) split_points.insert(ck);
    }

    struct SubWay {
        std::vector<std::pair<double,double>> coords;
    };
    std::vector<SubWay> sub_ways;
    for (auto* coords : way_coords) {
        size_t seg_start = 0;
        for (size_t i = 1; i < coords->size(); i++) {
            int64_t ck = coord_key((*coords)[i].first, (*coords)[i].second);
            if (split_points.count(ck) && i > seg_start) {
                SubWay sw;
                sw.coords.assign(coords->begin() + seg_start, coords->begin() + i + 1);
                if (sw.coords.size() >= 2) {
                    sub_ways.push_back(std::move(sw));
                }
                seg_start = i;
            }
        }
        if (seg_start < coords->size() - 1) {
            SubWay sw;
            sw.coords.assign(coords->begin() + seg_start, coords->end());
            if (sw.coords.size() >= 2) {
                sub_ways.push_back(std::move(sw));
            }
        }
    }

    std::unordered_map<int64_t, std::vector<std::pair<size_t, bool>>> coord_adj;
    for (size_t i = 0; i < sub_ways.size(); i++) {
        auto& first = sub_ways[i].coords.front();
        auto& last = sub_ways[i].coords.back();
        coord_adj[coord_key(first.first, first.second)].push_back({i, false});
        coord_adj[coord_key(last.first, last.second)].push_back({i, true});
    }

    std::vector<bool> used(sub_ways.size(), false);
    std::vector<std::vector<std::pair<double,double>>> rings;

    struct BacktrackState {
        const std::vector<SubWay>& sways;
        const std::unordered_map<int64_t, std::vector<std::pair<size_t, bool>>>& adj;
        std::vector<bool>& used;
        int calls;

        bool try_close(int64_t first_key, int64_t last_key,
                       std::vector<std::pair<size_t, bool>>& path, int depth) {
            if (++calls > BACKTRACK_CALL_BUDGET) return false;
            if (!path.empty() && first_key == last_key) return true;
            if (depth > BACKTRACK_MAX_DEPTH) return false;

            auto it = adj.find(last_key);
            if (it == adj.end()) return false;

            for (const auto& [wi, is_last] : it->second) {
                if (used[wi]) continue;
                auto& endpoint = is_last ? sways[wi].coords.front() : sways[wi].coords.back();
                int64_t new_key = coord_key(endpoint.first, endpoint.second);

                used[wi] = true;
                path.push_back({wi, is_last});

                if (try_close(first_key, new_key, path, depth + 1)) return true;

                path.pop_back();
                used[wi] = false;
            }
            return false;
        }
    };

    BacktrackState bt{sub_ways, coord_adj, used, 0};

    // Pass 1: Greedy
    for (size_t start_idx = 0; start_idx < sub_ways.size(); start_idx++) {
        if (used[start_idx]) continue;

        const auto& sg = sub_ways[start_idx];
        int64_t first_key = coord_key(sg.coords.front().first, sg.coords.front().second);
        int64_t last_key = coord_key(sg.coords.back().first, sg.coords.back().second);

        if (sg.coords.size() >= 4 && first_key == last_key) {
            used[start_idx] = true;
            rings.push_back(sg.coords);
            continue;
        }

        used[start_idx] = true;
        std::vector<size_t> attempt_used = {start_idx};
        std::vector<std::pair<double,double>> ring = sg.coords;
        int64_t current_last = last_key;
        bool extended = true;
        while (extended && current_last != first_key) {
            extended = false;
            auto it = coord_adj.find(current_last);
            if (it == coord_adj.end()) break;
            for (const auto& [wi, is_last] : it->second) {
                if (used[wi]) continue;
                const auto& sw = sub_ways[wi];
                bool reversed = is_last;
                auto& endpoint = reversed ? sw.coords.front() : sw.coords.back();
                used[wi] = true;
                attempt_used.push_back(wi);
                if (!reversed) ring.insert(ring.end(), sw.coords.begin() + 1, sw.coords.end());
                else for (auto rit = sw.coords.rbegin() + 1; rit != sw.coords.rend(); ++rit) ring.push_back(*rit);
                current_last = coord_key(endpoint.first, endpoint.second);
                extended = true;
                break;
            }
        }
        if (ring.size() >= 4 && current_last == first_key &&
            !ring_has_self_intersection(ring)) {
            rings.push_back(std::move(ring));
        } else {
            for (auto wi : attempt_used) used[wi] = false;
        }
    }

    // Pass 2: Backtracking retry
    {
        bool any_unused = false;
        for (size_t i = 0; i < used.size(); i++) {
            if (!used[i]) { any_unused = true; break; }
        }
        if (any_unused && sub_ways.size() <= MAX_SUBWAYS_FOR_RETRY) {
            std::vector<bool> bt_used(sub_ways.size(), false);
            std::vector<std::vector<std::pair<double,double>>> bt_rings;

            BacktrackState bt2{sub_ways, coord_adj, bt_used, 0};
            int total_bt_calls = 0;
            constexpr int MAX_TOTAL_BT_CALLS = BACKTRACK_TOTAL_BUDGET;

            for (size_t si = 0; si < sub_ways.size(); si++) {
                if (bt_used[si]) continue;
                if (total_bt_calls >= MAX_TOTAL_BT_CALLS) break;
                const auto& sg = sub_ways[si];
                int64_t fk = coord_key(sg.coords.front().first, sg.coords.front().second);
                int64_t lk = coord_key(sg.coords.back().first, sg.coords.back().second);
                if (sg.coords.size() >= 4 && fk == lk) {
                    bt_used[si] = true;
                    bt_rings.push_back(sg.coords);
                    continue;
                }
                bt_used[si] = true;
                bt2.calls = 0;
                std::vector<std::pair<size_t, bool>> path;
                if (bt2.try_close(fk, lk, path, 0)) {
                    std::vector<std::pair<double,double>> r = sg.coords;
                    for (const auto& [wi, rev] : path) {
                        const auto& c = sub_ways[wi].coords;
                        if (!rev) r.insert(r.end(), c.begin() + 1, c.end());
                        else for (auto it2 = c.rbegin() + 1; it2 != c.rend(); ++it2) r.push_back(*it2);
                    }
                    if (r.size() >= 4 && !ring_has_self_intersection(r)) {
                        bt_rings.push_back(std::move(r));
                        continue;
                    }
                }
                bt_used[si] = false;
                total_bt_calls += bt2.calls;
            }

            if (bt_rings.size() > rings.size()) {
                rings = std::move(bt_rings);
            }
        }
    }

    return rings;
}
