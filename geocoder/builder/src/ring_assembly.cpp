#include "ring_assembly.h"
#include "geometry.h"

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

namespace {

using Coords = std::vector<std::pair<double,double>>;

struct SubWay {
    Coords coords;
    uint8_t roles = 0;  // RingRole bit of the member way it comes from
};

int64_t key_of(const std::pair<double,double>& p) { return coord_key(p.first, p.second); }

// Whether a coordinate follows itself somewhere along the way (a node listed
// twice in a row, or two nodes at one location).
bool repeats_a_coordinate(const CoordSpan& coords) {
    for (size_t i = 1; i < coords.size(); i++)
        if (key_of(coords[i]) == key_of(coords[i - 1])) return true;
    return false;
}

Coords without_repeats(const CoordSpan& coords) {
    Coords out;
    out.reserve(coords.size());
    for (const auto& p : coords)
        if (out.empty() || key_of(out.back()) != key_of(p)) out.push_back(p);
    return out;
}

uint8_t role_bit(const std::string& role) {
    if (role == "outer" || role.empty()) return kRoleOuter;
    if (role == "inner") return kRoleInner;
    return kRoleOther;
}

// The ways cut at their endpoints and at every coordinate more than one way
// (or one way twice) passes through, so sub-ways only meet at their ends.
std::vector<SubWay> split_sub_ways(const std::vector<CoordSpan>& way_coords, const std::vector<uint8_t>& way_roles) {
    std::unordered_map<int64_t, int> coord_count;
    for (const CoordSpan& coords : way_coords)
        for (const auto& p : coords) coord_count[key_of(p)]++;

    std::unordered_set<int64_t> split_points;
    for (const CoordSpan& coords : way_coords) {
        split_points.insert(key_of(coords.front()));
        split_points.insert(key_of(coords.back()));
    }
    for (auto& [ck, cnt] : coord_count) {
        if (cnt > 1) split_points.insert(ck);
    }

    std::vector<SubWay> sub_ways;
    for (size_t w = 0; w < way_coords.size(); w++) {
        const CoordSpan& coords = way_coords[w];
        size_t seg_start = 0;
        for (size_t i = 1; i < coords.size(); i++) {
            if (split_points.count(key_of(coords[i])) && i > seg_start) {
                sub_ways.push_back({Coords(coords.begin() + seg_start, coords.begin() + i + 1), way_roles[w]});
                seg_start = i;
            }
        }
        if (seg_start < coords.size() - 1) {
            sub_ways.push_back({Coords(coords.begin() + seg_start, coords.end()), way_roles[w]});
        }
    }
    return sub_ways;
}

// Sub-ways running over the same coordinates (either way round) cancel in
// pairs, as osmium's erase_duplicate_segments drops duplicate segments: a way
// listed twice, or two ways mapped on top of each other, leaves no edge.
uint32_t cancel_duplicate_sub_ways(std::vector<SubWay>& sub_ways) {
    auto same = [](const Coords& a, const Coords& b) {
        if (a.size() != b.size()) return false;
        if (std::equal(a.begin(), a.end(), b.begin())) return true;
        return std::equal(a.begin(), a.end(), b.rbegin());
    };
    // Group by the unordered pair of end coordinates and the length.
    std::unordered_map<uint64_t, std::vector<uint32_t>> groups;
    for (uint32_t i = 0; i < sub_ways.size(); i++) {
        const Coords& c = sub_ways[i].coords;
        const int64_t a = key_of(c.front()), b = key_of(c.back());
        const uint64_t lo = static_cast<uint64_t>(std::min(a, b)), hi = static_cast<uint64_t>(std::max(a, b));
        groups[(lo * 0x9E3779B97F4A7C15ull) ^ hi ^ (static_cast<uint64_t>(c.size()) << 1)].push_back(i);
    }
    std::vector<char> cancelled(sub_ways.size(), 0);
    uint32_t count = 0;
    for (auto& [hash, members] : groups) {
        for (size_t a = 0; a < members.size(); a++) {
            if (cancelled[members[a]]) continue;
            for (size_t b = a + 1; b < members.size(); b++) {
                if (cancelled[members[b]] || !same(sub_ways[members[a]].coords, sub_ways[members[b]].coords)) continue;
                cancelled[members[a]] = cancelled[members[b]] = 1;
                count += 2;
                break;
            }
        }
    }
    if (count == 0) return 0;
    size_t kept = 0;
    for (size_t k = 0; k < sub_ways.size(); k++) {
        if (cancelled[k]) continue;
        if (kept != k) sub_ways[kept] = std::move(sub_ways[k]);
        kept++;
    }
    sub_ways.resize(kept);
    return count;
}

// Which member ways a ring assembly stitches, by role.
enum class RingRoles { Outer, All };

bool takes_role(RingRoles roles, const std::string& role) {
    return roles == RingRoles::All || role_bit(role) == kRoleOuter;
}

std::vector<std::vector<std::pair<double,double>>> assemble_rings(
    const std::vector<std::pair<int64_t, std::string>>& members,
    const WayGeometries& way_geoms,
    RingRoles roles)
{
    // Coordinate-based ring assembly: split ways at shared internal nodes,
    // then stitch sub-ways at endpoints with backtracking.
    // coord_key() from geometry.h provides coordinate hashing.

    std::vector<CoordSpan> way_coords;
    for (const auto& [way_id, role] : members) {
        if (!takes_role(roles, role)) continue;
        const auto* way = way_geoms.find(way_id);
        if (!way || way->count == 0) continue;
        way_coords.push_back(way_geoms.coords_of(*way));
    }
    const std::vector<SubWay> sub_ways = split_sub_ways(way_coords, std::vector<uint8_t>(way_coords.size(), 0));

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
                    // Ring rejected (self-intersecting): try_close left every
                    // path way marked used — release them, or later seeds can
                    // never close rings through those ways.
                    for (const auto& [wi, rev] : path) bt_used[wi] = false;
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

// A sub-way end: twice the sub-way's index, plus one at its back.
using End = uint32_t;

End end_of(uint32_t sub_way, bool back) { return sub_way * 2 + (back ? 1 : 0); }

// The way a sub-way leaves one of its end coordinates: (longitude, latitude)
// steps of 1e-7 degrees to its nearest coordinate that differs from the end's
// (zero when none does).
struct Heading {
    int64_t x = 0, y = 0;
};

Heading heading_of(const Coords& c, bool back) {
    const auto& at = back ? c.back() : c.front();
    for (size_t k = 1; k < c.size(); k++) {
        const auto& p = back ? c[c.size() - 1 - k] : c[k];
        const Heading h{e7(p.second) - e7(at.second), e7(p.first) - e7(at.first)};
        if (h.x != 0 || h.y != 0) return h;
    }
    return {};
}

// Whether heading a comes before heading b counterclockwise from east (a zero
// heading first), exactly: half-planes, then the sign of the cross product.
bool turns_before(const Heading& a, const Heading& b) {
    const bool a_zero = a.x == 0 && a.y == 0, b_zero = b.x == 0 && b.y == 0;
    if (a_zero || b_zero) return a_zero && !b_zero;
    const bool a_south = a.y < 0 || (a.y == 0 && a.x < 0);
    const bool b_south = b.y < 0 || (b.y == 0 && b.x < 0);
    if (a_south != b_south) return b_south;
    return static_cast<__int128>(a.x) * b.y > static_cast<__int128>(a.y) * b.x;
}

// The sub-way ends at each coordinate where rings touch (more than two ends),
// counterclockwise from east, ties by end.
std::unordered_map<int64_t, std::vector<End>> turn_orders(
    const std::vector<SubWay>& sub_ways, const std::unordered_map<int64_t, std::vector<uint32_t>>& ends) {
    std::unordered_map<int64_t, std::vector<End>> turns;
    std::vector<std::pair<Heading, End>> around;
    for (const auto& [at, subs] : ends) {
        if (subs.size() <= 2) continue;
        around.clear();
        for (size_t k = 0; k < subs.size(); k++) {
            // A closed sub-way is listed twice in a row: front, then back.
            const Coords& c = sub_ways[subs[k]].coords;
            const bool back = key_of(c.front()) != at || (k > 0 && subs[k - 1] == subs[k]);
            around.push_back({heading_of(c, back), end_of(subs[k], back)});
        }
        std::sort(around.begin(), around.end(), [](const auto& a, const auto& b) {
            if (turns_before(a.first, b.first)) return true;
            if (turns_before(b.first, a.first)) return false;
            return a.second < b.second;
        });
        std::vector<End>& order = turns[at];
        for (const auto& [heading, end] : around) order.push_back(end);
    }
    return turns;
}

// The coordinates where two rings cross rather than touch: going round one,
// the other's two ends there lie on different sides of the first's. A ring
// passes a coordinate once at most, so it has two ends there (or none).
uint32_t crossing_coordinates(const std::unordered_map<int64_t, std::vector<End>>& turns,
                              const std::vector<int32_t>& ring_of) {
    uint32_t crossings = 0;
    std::vector<std::pair<int32_t, uint32_t>> at;  // (ring, position in turn order)
    for (const auto& [coord, order] : turns) {
        at.clear();
        for (uint32_t k = 0; k < order.size(); k++)
            if (ring_of[order[k] / 2] >= 0) at.push_back({ring_of[order[k] / 2], k});
        std::sort(at.begin(), at.end());
        bool cross = at.size() % 2 != 0;
        for (size_t r = 0; r + 1 < at.size() && !cross; r += 2) {
            cross = at[r].first != at[r + 1].first;
            for (size_t s = r + 2; s + 1 < at.size() && !cross; s += 2) {
                const auto inside = [&](uint32_t k) { return k > at[r].second && k < at[r + 1].second; };
                cross = inside(at[s].second) != inside(at[s + 1].second);
            }
        }
        crossings += cross;
    }
    return crossings;
}

struct Walk {
    std::vector<Coords> rings;
    std::vector<uint8_t> roles;
    std::vector<int32_t> ring_of;  // per sub-way: its ring, or -1
};

// Walk unused sub-ways end to end. Arriving back at a coordinate the walk has
// passed closes a ring there, so every ring is simple at its vertices (rings
// touching at a node come out separate, as osmium splits them at such
// locations). Every coordinate joins an even number of ends, so a walk can
// always go on until it closes. Leaving a coordinate, the walk takes the
// first unused sub-way there, or with `paired` the end paired with the one it
// arrived by where that has a pair.
Walk walk_rings(const std::vector<SubWay>& sub_ways, const std::unordered_map<int64_t, std::vector<uint32_t>>& ends,
                const std::unordered_map<End, End>* paired) {
    Walk out;
    out.ring_of.assign(sub_ways.size(), -1);
    std::vector<char> used(sub_ways.size(), 0);
    auto unused_at = [&](int64_t v) -> int64_t {
        for (uint32_t i : ends.at(v))
            if (!used[i]) return i;
        return -1;
    };
    struct Step {
        uint32_t sub_way;
        bool reversed;
    };
    std::vector<int64_t> trail;  // trail[k] -> trail[k + 1] along steps[k]
    std::vector<Step> steps;
    std::unordered_map<int64_t, size_t> trail_index;
    for (uint32_t start = 0; start < sub_ways.size(); start++) {
        if (used[start]) continue;
        trail.assign(1, key_of(sub_ways[start].coords.front()));
        steps.clear();
        trail_index.clear();
        trail_index[trail[0]] = 0;
        int64_t next = start;
        bool reversed = false;
        End arrived = 0;
        while (true) {
            const int64_t at = trail.back();
            if (next < 0) {
                const auto pair = paired ? paired->find(arrived) : std::unordered_map<End, End>::const_iterator{};
                if (paired && pair != paired->end()) {
                    // Only the walk's first end can be used already: the
                    // walk is back where it started, its rings all closed.
                    if (used[pair->second / 2]) break;
                    next = pair->second / 2;
                    reversed = pair->second % 2;
                } else {
                    next = unused_at(at);
                    if (next < 0) break;
                    reversed = key_of(sub_ways[next].coords.front()) != at;
                }
            }
            const Coords& c = sub_ways[next].coords;
            const int64_t to = key_of(reversed ? c.front() : c.back());
            used[next] = 1;
            steps.push_back({static_cast<uint32_t>(next), reversed});
            arrived = end_of(static_cast<uint32_t>(next), !reversed);
            next = -1;
            const auto seen = trail_index.find(to);
            if (seen == trail_index.end()) {
                trail_index[to] = trail.size();
                trail.push_back(to);
                continue;
            }
            const size_t from = seen->second;
            Coords ring;
            uint8_t roles = 0;
            for (size_t k = from; k < steps.size(); k++) {
                const SubWay& sw = sub_ways[steps[k].sub_way];
                roles |= sw.roles;
                const size_t skip = ring.empty() ? 0 : 1;
                if (!steps[k].reversed) ring.insert(ring.end(), sw.coords.begin() + skip, sw.coords.end());
                else ring.insert(ring.end(), sw.coords.rbegin() + skip, sw.coords.rend());
            }
            if (ring.size() >= 4)
                for (size_t k = from; k < steps.size(); k++) out.ring_of[steps[k].sub_way] = static_cast<int32_t>(out.rings.size());
            for (size_t k = from + 1; k < trail.size(); k++) trail_index.erase(trail[k]);
            trail.resize(from + 1);
            steps.resize(from);
            if (ring.size() < 4) continue;
            out.rings.push_back(std::move(ring));
            out.roles.push_back(roles);
        }
    }
    return out;
}

}  // namespace

std::vector<std::vector<std::pair<double,double>>> assemble_outer_rings(
    const std::vector<std::pair<int64_t, std::string>>& members,
    const WayGeometries& way_geoms,
    bool include_all_roles) {
    return assemble_rings(members, way_geoms, include_all_roles ? RingRoles::All : RingRoles::Outer);
}

AreaRings assemble_area_rings(const std::vector<std::pair<int64_t, std::string>>& members,
                              const WayGeometries& way_geoms) {
    AreaRings out;
    std::vector<CoordSpan> way_coords;
    std::vector<uint8_t> way_roles;
    // osmium makes no segment of a coordinate and its repeat straight after
    // it (SegmentList::extract_segments_from_way_impl), so such repeats are
    // no coordinates rings touch at: the ways that have them are copied
    // without them, and way_coords points at the copies.
    std::vector<Coords> repeats_dropped;
    repeats_dropped.reserve(members.size());
    for (const auto& [way_id, role] : members) {
        const auto* way = way_geoms.find(way_id);
        if (!way) continue;
        CoordSpan coords = way_geoms.coords_of(*way);
        if (repeats_a_coordinate(coords)) {
            repeats_dropped.push_back(without_repeats(coords));
            coords = {repeats_dropped.back().data(), repeats_dropped.back().size()};
        }
        if (coords.size() < 2) continue;  // no segment
        way_coords.push_back(coords);
        way_roles.push_back(role_bit(role));
    }
    std::vector<SubWay> sub_ways = split_sub_ways(way_coords, way_roles);
    out.cancelled_sub_ways = cancel_duplicate_sub_ways(sub_ways);

    // Sub-ways by end coordinate, in sub-way order (a closed one twice).
    std::unordered_map<int64_t, std::vector<uint32_t>> ends;
    for (uint32_t i = 0; i < sub_ways.size(); i++) {
        ends[key_of(sub_ways[i].coords.front())].push_back(i);
        ends[key_of(sub_ways[i].coords.back())].push_back(i);
    }
    // osmium gives up on an area whose rings don't all close (a coordinate
    // with an odd number of segment ends) or with more than 100 coordinates
    // where rings touch (BasicAssembler::create_rings); Nominatim then has
    // no geometry for the relation, and neither has this.
    uint32_t touching = 0;
    for (const auto& [at, subs] : ends) {
        if (subs.size() % 2) {
            out.failure = AreaFailure::OpenRing;
            return out;
        }
        touching += subs.size() > 2;
    }
    if (touching > MAX_TOUCHING_LOCATIONS) {
        out.failure = AreaFailure::TooManyTouches;
        return out;
    }

    Walk walk = walk_rings(sub_ways, ends, nullptr);
    // Nor does osmium take an area whose segments intersect (cross, or meet
    // other than at an end both have): it looks for those
    // (SegmentList::find_intersections) before it joins any rings.
    if (rings_cross(walk.rings)) {
        out.failure = AreaFailure::Crossing;
        return out;
    }
    // Walked in member order, two rings can cross at a coordinate they share
    // (two ways overlapping, noded where their outlines meet), and nesting
    // rings needs them not to. osmium joins the sub-ways at such coordinates
    // into rings that don't cross (BasicAssembler::join_connected_rings), so
    // its area is the even-odd one of all the segments. So is this one: the
    // walk is redone with the ends at every touching coordinate paired with
    // their neighbours in turn order, pairs that never interleave. Should
    // rings still cross, there is no area, as for crossing segments.
    if (touching > 0) {
        const auto turns = turn_orders(sub_ways, ends);
        out.crossing_coordinates = crossing_coordinates(turns, walk.ring_of);
        if (out.crossing_coordinates > 0) {
            std::unordered_map<End, End> paired;
            for (const auto& [at, order] : turns)
                for (size_t k = 0; k + 1 < order.size(); k += 2) {
                    paired[order[k]] = order[k + 1];
                    paired[order[k + 1]] = order[k];
                }
            walk = walk_rings(sub_ways, ends, &paired);
            if (crossing_coordinates(turns, walk.ring_of) > 0) {
                out.failure = AreaFailure::Crossing;
                return out;
            }
        }
    }
    out.rings = std::move(walk.rings);
    out.roles = std::move(walk.roles);
    return out;
}
