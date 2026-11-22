// Cuts a water polygon with many holes (a lake with thousands of islands)
// into pieces along a fixed grid, so that a point's ring test visits only
// the holes near it: each piece holds at most kSplitMaxHoles holes, or is
// as small as the grid goes. A piece is the polygon (its outer ring less its
// holes) within a grid rectangle; together the pieces hold every point the
// polygon holds, each once (but on a cut line itself). The grid lines are
// those of a power of two of e7 units: a piece of too many holes is cut by
// the line of the coarsest such grid that crosses its box, along its longer
// side, so a cut stays where it is while the polygon's box stays round it.
// Where a ring crosses a line it gains a vertex there (OSM's edge rounded to
// the e7 grid at the line), and the cut runs along the line from one such
// vertex to the next as a way of its own (a negative id of the line), so
// each stretch of cut is one arc of the two pieces beside it.
//
// The rings must be valid: holes inside the outer ring and apart from one
// another. split_polygon checks its pieces against the polygon at sample
// points and by area, and gives the polygon back whole when they disagree.
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <utility>
#include <vector>

#include "arc_blocks.h"
#include "exact_pip.h"
#include "packed_location.h"
#include "polygon_rings.h"

namespace water {

inline constexpr uint32_t kSplitMaxHoles = 64;
// The finest grid a cut takes: 2^16 e7 units, about 7 m.
inline constexpr int kSplitFinestShift = 16;
inline constexpr int kSplitCoarsestShift = 30;

// A ring as the split walks it: its vertices, not closed, and the way of the
// edge from each to the next.
struct SplitRing {
    std::vector<PackedLocation> v;
    std::vector<int64_t> way;
};

struct SplitPolygon {
    SplitRing outer;
    std::vector<SplitRing> holes;
    uint64_t key = 0;  // the piece's cuts, hashed: 0 for a polygon never cut
};

namespace split_detail {

// A point in the frame the cut is made in: a horizontal line y = L. A cut
// along a meridian turns the plane a quarter clockwise first, which keeps
// every ring's direction.
struct P {
    int64_t x, y;
    bool operator==(const P& o) const { return x == o.x && y == o.y; }
    bool operator!=(const P& o) const { return !(*this == o); }
};

inline P to_frame(PackedLocation p, bool meridian) {
    return meridian ? P{p.lat_e7, -int64_t(p.lon_e7)} : P{p.lon_e7, p.lat_e7};
}
inline PackedLocation from_frame(P p, bool meridian) {
    return meridian ? PackedLocation{static_cast<int32_t>(p.x), static_cast<int32_t>(-p.y)}
                    : PackedLocation{static_cast<int32_t>(p.y), static_cast<int32_t>(p.x)};
}

struct FrameRing {
    std::vector<P> v;
    std::vector<int64_t> way;
};

// Twice the signed area: > 0 counterclockwise.
inline __int128 area2(const std::vector<P>& v) {
    __int128 s = 0;
    for (size_t i = 0, n = v.size(); i < n; i++) {
        const P& a = v[i];
        const P& b = v[(i + 1) % n];
        s += static_cast<__int128>(a.x) * b.y - static_cast<__int128>(b.x) * a.y;
    }
    return s;
}

inline void reverse_ring(FrameRing& r) {
    // The edge from v[i] to v[i + 1] is way[i]; reversed, the edge from the
    // new v[j] to v[j + 1] is the old edge from v[n - 2 - j]... (mod n).
    const size_t n = r.v.size();
    std::vector<P> v(n);
    std::vector<int64_t> way(n);
    for (size_t j = 0; j < n; j++) {
        v[j] = r.v[(n - j) % n];
        way[j] = r.way[(2 * n - j - 1) % n];
    }
    r.v = std::move(v);
    r.way = std::move(way);
}

// Where the edge a-b, which runs from one side of y = L to the other (a.y < L
// <= b.y, or b.y < L <= a.y), meets the line: x rounded to the nearest unit,
// half away from zero; an end on the line is itself.
inline P crossing(P a, P b, int64_t L) {
    if (a.y == L) return a;
    if (b.y == L) return b;
    const __int128 num = static_cast<__int128>(b.x - a.x) * (L - a.y);
    const __int128 den = b.y - a.y;
    __int128 q = num / den, r = num % den;
    // Round the quotient half away from zero.
    const bool negative = (num < 0) != (den < 0);
    const __int128 r2 = (r < 0 ? -r : r) * 2, d = den < 0 ? -den : den;
    if (r2 >= d) q += negative ? -1 : 1;
    return {a.x + static_cast<int64_t>(q), L};
}

// A stretch of a ring on one side of the line: from where it crosses onto
// that side to where it crosses back, both on the line; way[i] is the edge
// from v[i] to v[i + 1].
struct Chain {
    std::vector<P> v;
    std::vector<int64_t> way;
};

inline void push(Chain& c, P p, int64_t way_on) {
    if (!c.v.empty() && c.v.back() == p) {
        c.way.back() = way_on;
        return;
    }
    c.v.push_back(p);
    c.way.push_back(way_on);
}

// The two sides' parts of a set of rings cut by y = L (side 1 holds y >= L):
// each side's rings, outer counterclockwise and holes clockwise, nested into
// polygons. The cut's way is `cut_way`. False where the rings don't make
// pieces it can tell apart: a chain left without its next, or a hole in no
// outer ring.
struct Side {
    std::vector<FrameRing> outers;
    std::vector<std::vector<FrameRing>> holes;  // by outer
};

inline bool cut(const std::vector<FrameRing>& rings, int64_t L, int64_t cut_way, Side sides[2]) {
    std::vector<Chain> chains[2];
    std::vector<FrameRing> whole[2];
    for (const FrameRing& r : rings) {
        const size_t n = r.v.size();
        auto side = [&](size_t i) { return r.v[i % n].y >= L ? 1 : 0; };
        size_t s = n;
        for (size_t i = 0; i < n; i++)
            if (side(i) != side(i + n - 1)) {
                s = i;
                break;
            }
        if (s == n) {
            whole[side(0)].push_back(r);
            continue;
        }
        // From the vertex after the first crossing, round the ring.
        Chain c;
        int current = side(s);
        const size_t prev = (s + n - 1) % n;
        push(c, crossing(r.v[prev], r.v[s], L), r.way[prev]);
        for (size_t k = 0; k < n; k++) {
            const size_t i = (s + k) % n, j = (i + 1) % n;
            push(c, r.v[i], r.way[i]);
            if (side(j) != current) {
                const P x = crossing(r.v[i], r.v[j], L);
                push(c, x, cut_way);
                if (c.v.size() >= 2) chains[current].push_back(std::move(c));
                c = Chain{};
                current = side(j);
                push(c, x, r.way[i]);
            }
        }
        if (c.v.size() > 1) return false;  // the walk ends at its first crossing
    }
    for (int s = 0; s < 2; s++) {
        Side& out = sides[s];
        // Each chain's start on the line, by x: side 0 goes west along the
        // line to the next start below its end, side 1 east to the next above.
        std::vector<std::pair<int64_t, size_t>> starts;
        for (size_t k = 0; k < chains[s].size(); k++) starts.push_back({chains[s][k].v.front().x, k});
        std::sort(starts.begin(), starts.end());
        std::vector<uint8_t> used(chains[s].size(), 0);
        for (size_t first = 0; first < chains[s].size(); first++) {
            if (used[first]) continue;
            FrameRing ring;
            size_t k = first;
            while (!used[k]) {
                used[k] = 1;
                const Chain& c = chains[s][k];
                for (size_t i = 0; i + 1 < c.v.size(); i++) {
                    if (!ring.v.empty() && ring.v.back() == c.v[i]) {
                        ring.way.back() = c.way[i];
                        continue;
                    }
                    ring.v.push_back(c.v[i]);
                    ring.way.push_back(c.way[i]);
                }
                const P end = c.v.back();
                // The next start along the line from `end`.
                size_t next = SIZE_MAX;
                if (s == 0) {
                    auto it = std::upper_bound(starts.begin(), starts.end(), std::make_pair(end.x, SIZE_MAX));
                    if (it == starts.begin()) return false;
                    next = std::prev(it)->second;
                } else {
                    auto it = std::lower_bound(starts.begin(), starts.end(), std::make_pair(end.x, size_t(0)));
                    if (it == starts.end()) return false;
                    next = it->second;
                }
                // The cut from `end` to the next chain's start.
                if (ring.v.empty() || ring.v.back() != end) {
                    ring.v.push_back(end);
                    ring.way.push_back(cut_way);
                } else {
                    ring.way.back() = cut_way;
                }
                k = next;
                if (k != first && used[k]) return false;
            }
            // Drop a closing repeat.
            while (ring.v.size() > 1 && ring.v.front() == ring.v.back()) {
                ring.v.pop_back();
                ring.way.pop_back();
            }
            if (ring.v.size() < 3 || area2(ring.v) == 0) continue;
            if (area2(ring.v) < 0) return false;
            out.outers.push_back(std::move(ring));
        }
        std::vector<FrameRing> holes;
        for (FrameRing& r : whole[s]) {
            if (area2(r.v) > 0) out.outers.push_back(std::move(r));
            else holes.push_back(std::move(r));
        }
        out.holes.assign(out.outers.size(), {});
        for (FrameRing& h : holes) {
            // A vertex of the hole on no outer ring tells which holds it.
            size_t owner = SIZE_MAX;
            for (const P& p : h.v) {
                bool shared = false;
                for (const FrameRing& o : out.outers)
                    if (std::find(o.v.begin(), o.v.end(), p) != o.v.end()) shared = true;
                if (shared) continue;
                for (size_t o = 0; o < out.outers.size(); o++) {
                    const std::vector<P>& ov = out.outers[o].v;
                    bool in = false;
                    for (size_t a = 0, b = ov.size() - 1; a < ov.size(); b = a++) {
                        const P &pa = ov[b], &pb = ov[a];
                        if ((pa.x > p.x) == (pb.x > p.x)) continue;
                        const __int128 lhs = static_cast<__int128>(p.y - pa.y) * (pb.x - pa.x);
                        const __int128 rhs = static_cast<__int128>(pb.y - pa.y) * (p.x - pa.x);
                        if (lhs != rhs && ((lhs < rhs) != (pb.x < pa.x))) in = !in;
                    }
                    if (in) {
                        owner = o;
                        break;
                    }
                }
                break;
            }
            if (owner == SIZE_MAX) return false;
            out.holes[owner].push_back(std::move(h));
        }
    }
    return true;
}

inline uint64_t mix_key(uint64_t key, uint64_t line, bool meridian, int side) {
    return mix64(key ^ mix64(line * 4 + (meridian ? 2 : 0) + uint64_t(side) + 0x51a1f00dull));
}

// The negative way id of the cut along a line.
inline int64_t cut_way_of(int64_t line, bool meridian) {
    return -static_cast<int64_t>((mix64(uint64_t(line) * 2 + (meridian ? 1 : 0)) >> 2) + 1);
}

struct Box {
    int64_t lat_lo = INT64_MAX, lat_hi = INT64_MIN, lon_lo = INT64_MAX, lon_hi = INT64_MIN;
    void add(PackedLocation p) {
        lat_lo = std::min<int64_t>(lat_lo, p.lat_e7);
        lat_hi = std::max<int64_t>(lat_hi, p.lat_e7);
        lon_lo = std::min<int64_t>(lon_lo, p.lon_e7);
        lon_hi = std::max<int64_t>(lon_hi, p.lon_e7);
    }
};

// The line of the coarsest grid of 2^shift units strictly inside (lo, hi),
// or false.
inline bool coarsest_line(int64_t lo, int64_t hi, int64_t& line) {
    for (int shift = kSplitCoarsestShift; shift >= kSplitFinestShift; shift--) {
        const int64_t step = int64_t(1) << shift;
        // The first multiple of step above lo.
        int64_t m = lo >= 0 ? (lo / step + 1) * step : -((-lo) / step) * step;
        if (m <= lo) m += step;
        if (m < hi) {
            line = m;
            return true;
        }
    }
    return false;
}

inline bool ring_contains(const SplitRing& r, PackedLocation p) {
    return exact_ring_contains(r.v.data(), static_cast<uint32_t>(r.v.size()), p);
}

inline bool polygon_contains(const SplitPolygon& poly, PackedLocation p) {
    bool in = ring_contains(poly.outer, p);
    for (const SplitRing& h : poly.holes) in = in != ring_contains(h, p);
    return in;
}

inline __int128 polygon_area2(const SplitPolygon& poly) {
    auto mag = [](const SplitRing& r) {
        const __int128 a = exact_ring_area2(r.v.data(), static_cast<uint32_t>(r.v.size()));
        return a < 0 ? -a : a;
    };
    __int128 a = mag(poly.outer);
    for (const SplitRing& h : poly.holes) a -= mag(h);
    return a;
}

}  // namespace split_detail

// The pieces of the polygon (its rings and their edges' ways), each with at
// most max_holes holes or as small as the grid goes; the polygon alone, key
// 0, when it has no more than max_holes or its pieces fail the check.
// `cut_lines` counts the lines it cut along.
inline std::vector<SplitPolygon> split_polygon(const SplitPolygon& polygon, uint32_t max_holes = kSplitMaxHoles,
                                               size_t* cut_lines = nullptr) {
    using namespace split_detail;
    if (polygon.holes.size() <= max_holes) return {polygon};
    std::vector<SplitPolygon> done, todo = {polygon};
    todo[0].key = 0;
    size_t lines = 0;
    while (!todo.empty()) {
        SplitPolygon piece = std::move(todo.back());
        todo.pop_back();
        if (piece.holes.size() <= max_holes) {
            done.push_back(std::move(piece));
            continue;
        }
        Box box;
        for (PackedLocation p : piece.outer.v) box.add(p);
        const bool wide = box.lon_hi - box.lon_lo >= box.lat_hi - box.lat_lo;
        int64_t line = 0;
        bool meridian = wide;
        bool found = wide ? coarsest_line(box.lon_lo, box.lon_hi, line) : coarsest_line(box.lat_lo, box.lat_hi, line);
        if (!found) {
            meridian = !wide;
            found = meridian ? coarsest_line(box.lon_lo, box.lon_hi, line) : coarsest_line(box.lat_lo, box.lat_hi, line);
        }
        if (!found) {
            done.push_back(std::move(piece));
            continue;
        }
        // In the frame: a cut along a meridian lon = line is y = -line there.
        const int64_t L = meridian ? -line : line;
        std::vector<FrameRing> rings;
        auto add = [&](const SplitRing& r, bool hole) {
            FrameRing f;
            for (PackedLocation p : r.v) f.v.push_back(to_frame(p, meridian));
            f.way = r.way;
            const __int128 a = area2(f.v);
            if ((a > 0) == hole && a != 0) reverse_ring(f);
            rings.push_back(std::move(f));
        };
        add(piece.outer, false);
        for (const SplitRing& h : piece.holes) add(h, true);
        Side sides[2];
        if (!cut(rings, L, cut_way_of(line, meridian), sides)) return {polygon};
        lines++;
        for (int s = 0; s < 2; s++)
            for (size_t o = 0; o < sides[s].outers.size(); o++) {
                SplitPolygon out;
                auto back = [&](const FrameRing& f) {
                    SplitRing r;
                    for (const P& p : f.v) r.v.push_back(from_frame(p, meridian));
                    r.way = f.way;
                    return r;
                };
                out.outer = back(sides[s].outers[o]);
                for (const FrameRing& h : sides[s].holes[o]) out.holes.push_back(back(h));
                out.key = mix_key(piece.key, static_cast<uint64_t>(line), meridian, s) + o;
                todo.push_back(std::move(out));
            }
    }

    // The check: the pieces' area is the polygon's, to the rounding of the
    // cut vertices, and at sample points over its box exactly the pieces
    // the polygon holds hold the point, one.
    const __int128 whole = polygon_area2(polygon);
    __int128 parts = 0;
    for (const SplitPolygon& p : done) parts += polygon_area2(p);
    const __int128 diff = whole > parts ? whole - parts : parts - whole;
    if (diff * 100000 > (whole < 0 ? -whole : whole) + 1000000000) return {polygon};
    Box box;
    for (PackedLocation p : polygon.outer.v) box.add(p);
    std::vector<Box> boxes(done.size());
    for (size_t k = 0; k < done.size(); k++)
        for (PackedLocation p : done[k].outer.v) boxes[k].add(p);
    // The polygon's own test, its outer ring's edges indexed and each hole
    // behind its box.
    const ExactRingEdgeIndex outer_index(polygon.outer.v.data(), static_cast<uint32_t>(polygon.outer.v.size()));
    std::vector<ExactRingBox> hole_boxes;
    for (const SplitRing& h : polygon.holes) hole_boxes.push_back(exact_ring_box(h.v.data(), static_cast<uint32_t>(h.v.size())));
    auto whole_contains = [&](PackedLocation p) {
        bool in = outer_index.contains(p);
        for (size_t h = 0; h < polygon.holes.size(); h++)
            if (!exact_ring_box_excludes(hole_boxes[h], p)) in = in != ring_contains(polygon.holes[h], p);
        return in;
    };
    uint64_t state = mix64(static_cast<uint64_t>(box.lat_lo) * 31 + static_cast<uint64_t>(box.lon_lo));
    auto next = [&](int64_t lo, int64_t hi) {
        state = mix64(state + 0x9e3779b97f4a7c15ull);
        return lo + static_cast<int64_t>(state % static_cast<uint64_t>(hi - lo + 1));
    };
    for (int k = 0; k < 512; k++) {
        const PackedLocation p{static_cast<int32_t>(next(box.lat_lo, box.lat_hi)),
                               static_cast<int32_t>(next(box.lon_lo, box.lon_hi))};
        const bool in = whole_contains(p);
        int held = 0;
        for (size_t q = 0; q < done.size(); q++) {
            const Box& b = boxes[q];
            if (p.lat_e7 < b.lat_lo || p.lat_e7 > b.lat_hi || p.lon_e7 < b.lon_lo || p.lon_e7 > b.lon_hi) continue;
            held += polygon_contains(done[q], p);
        }
        // A point on a cut line may be held by both pieces or neither.
        if (held != (in ? 1 : 0)) {
            bool on_cut = false;
            for (size_t q = 0; q < done.size() && !on_cut; q++)
                for (size_t i = 0; i < done[q].outer.v.size() && !on_cut; i++)
                    on_cut = done[q].outer.way[i] < 0 &&
                             (done[q].outer.v[i].lat_e7 == p.lat_e7 || done[q].outer.v[i].lon_e7 == p.lon_e7);
            if (!on_cut) return {polygon};
        }
    }
    if (cut_lines) *cut_lines += lines;
    return done;
}

}  // namespace water
