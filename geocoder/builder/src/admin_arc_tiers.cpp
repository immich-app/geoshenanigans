// The arc sets' tiers (build_arc_tier): Douglas-Peucker once per arc, the
// snap, the rounds that give a degenerate ring vertices back, the repair of
// crossings simplification makes, and the nesting counts.
#include <algorithm>
#include <cmath>
#include <iterator>
#include <stdexcept>
#include <unordered_map>

#include "admin_arcs.h"
#include "exact_pip.h"
#include "parallel.h"
#include "parsed_data.h"

namespace {

// Whether a -> b -> c goes back along the line it came out on, b the tip of
// a spike. In 128 bits: a product of two longitude steps passes 2^63.
bool turns_back(PackedLocation a, PackedLocation b, PackedLocation c) {
    const __int128 lat1 = int64_t(b.lat_e7) - a.lat_e7, lon1 = int64_t(b.lon_e7) - a.lon_e7;
    const __int128 lat2 = int64_t(c.lat_e7) - b.lat_e7, lon2 = int64_t(c.lon_e7) - b.lon_e7;
    return lat1 * lon2 == lon1 * lat2 && lat1 * lat2 + lon1 * lon2 < 0;
}

// `ring` (as ring_folds_over has it) without its spikes, into out: each tip
// dropped (turns_back), and then a vertex its neighbour now repeats, the
// ring's end joined back to its start, until no spike is left. The ring
// winds round every point off them as it did.
void without_spikes(const std::vector<PackedLocation>& ring, std::vector<PackedLocation>& out) {
    out.clear();
    for (const PackedLocation& p : ring) {
        while (out.size() >= 2 && out.back() != p && turns_back(out[out.size() - 2], out.back(), p)) out.pop_back();
        if (out.empty() || out.back() != p) out.push_back(p);
    }
    size_t first = 0;
    while (out.size() - first >= 3) {
        if (out.back() == out[first] || turns_back(out[out.size() - 2], out.back(), out[first])) out.pop_back();
        else if (turns_back(out.back(), out[first], out[first + 1])) first++;
        else break;
    }
    out.erase(out.begin(), out.begin() + static_cast<std::ptrdiff_t>(first));
}

}  // namespace

bool ring_folds_over(const std::vector<PackedLocation>& walked, FoldScratch& s) {
    auto repeats_none = [&](const std::vector<PackedLocation>& v) {
        s.keys.clear();
        for (const PackedLocation& p : v) s.keys.push_back(vertex_key(p));
        std::sort(s.keys.begin(), s.keys.end());
        return std::adjacent_find(s.keys.begin(), s.keys.end()) == s.keys.end();
    };
    if (repeats_none(walked)) return false;
    // Taking the spikes off only drops vertices: where it drops none, keys
    // holds the ring's still.
    without_spikes(walked, s.ring);
    const std::vector<PackedLocation>& ring = s.ring;
    if (ring.size() < 3 || (ring.size() < walked.size() && repeats_none(ring))) return false;
    // Which ways the loops with an area wind, the rest last: rest[from, end)
    // joined back to rest[from].
    s.rest.clear();
    bool positive = false, negative = false;
    auto add_loop = [&](uint32_t from) {
        const __int128 a2 = exact_ring_area2(s.rest.data() + from, static_cast<uint32_t>(s.rest.size() - from));
        positive |= a2 > 0;
        negative |= a2 < 0;
    };
    std::unordered_map<uint64_t, uint32_t> at;  // a vertex of rest: its index
    for (const PackedLocation& p : ring) {
        const auto [it, added] = at.try_emplace(vertex_key(p), static_cast<uint32_t>(s.rest.size()));
        if (added) {
            s.rest.push_back(p);
            continue;
        }
        const uint32_t i = it->second;
        add_loop(i);
        for (size_t k = i + 1; k < s.rest.size(); k++) at.erase(vertex_key(s.rest[k]));
        s.rest.resize(i + 1);
    }
    add_loop(0);
    if (!positive || !negative) return false;

    // The vertices it comes back to, each once, in keys.
    size_t repeats = 0;
    for (size_t k = 0; k + 1 < s.keys.size(); k++)
        if (s.keys[k] == s.keys[k + 1] && (repeats == 0 || s.keys[repeats - 1] != s.keys[k])) s.keys[repeats++] = s.keys[k];
    s.keys.resize(repeats);
    const size_t n = ring.size();
    auto comes_back = [&](size_t k) { return std::binary_search(s.keys.begin(), s.keys.end(), vertex_key(ring[k % n])); };
    // A probe beside each stretch, from the first edge of it off one
    // meridian; a stretch of the ring runs from one vertex it comes back to
    // to the next.
    size_t first = 0;
    while (!comes_back(first)) first++;
    s.probes.clear();
    bool probed = false;  // the stretch the walk is on
    for (size_t k = first; k < first + n; k++) {
        const PackedLocation from = ring[k % n], to = ring[(k + 1) % n];
        if (!probed && from.lon_e7 != to.lon_e7) {
            s.probes.push_back({int64_t(from.lat_e7) + to.lat_e7, int64_t(from.lon_e7) + to.lon_e7, 0, 0});
            probed = true;
        }
        if (comes_back(k + 1)) probed = false;
    }
    // Each edge c -> d off one meridian straddles, as exact_edge_crosses has
    // it (one end east, the other at or west), the probes at longitudes in
    // [2 min, 2 max) of its ends', and passes north of, through or south of
    // each probe's midpoint there: the sign of (d - c) x (midpoint - c), in
    // (lat, lon), times east's. +1 for an edge running east, -1 west, so
    // that a ring of positive area (exact_ring_area2) winds round its inside
    // +1 times. In doubled units the cross products need 128 bits.
    std::sort(s.probes.begin(), s.probes.end(),
              [](const FoldScratch::Probe& a, const FoldScratch::Probe& b) { return a.lon < b.lon; });
    auto before = [](const FoldScratch::Probe& p, int64_t lon) { return p.lon < lon; };
    for (size_t k = 0, j = n - 1; k < n; j = k++) {
        const PackedLocation c = ring[j], d = ring[k];
        if (c.lon_e7 == d.lon_e7) continue;

        const int east = d.lon_e7 > c.lon_e7 ? 1 : -1;
        const int64_t lo = 2 * int64_t(std::min(c.lon_e7, d.lon_e7)), hi = 2 * int64_t(std::max(c.lon_e7, d.lon_e7));
        const auto end = std::lower_bound(s.probes.begin(), s.probes.end(), hi, before);
        for (auto it = std::lower_bound(s.probes.begin(), end, lo, before); it != end; ++it) {
            const __int128 cross = __int128(int64_t(d.lat_e7) - c.lat_e7) * (it->lon - 2 * int64_t(c.lon_e7)) -
                                   __int128(it->lat - 2 * int64_t(c.lat_e7)) * (int64_t(d.lon_e7) - c.lon_e7);
            if (cross == 0) it->through += east;
            else if ((cross > 0) == (east > 0)) it->crossed += east;
        }
    }
    // North of a probe's edge the ray from the point crosses `crossed`
    // edges' worth; south, the edges through the midpoint too.
    bool winds_positively = false, winds_negatively = false;
    for (const FoldScratch::Probe& p : s.probes)
        for (const int w : {p.crossed, p.crossed + p.through}) {
            winds_positively |= w > 0;
            winds_negatively |= w < 0;
        }
    return winds_positively && winds_negatively;
}

namespace {

// Pairs of raw edges past which the repair takes two segments' raw spans for
// apart without comparing them, so their crossing counts as made by
// simplifying: each repair round shortens the spans.
constexpr uint64_t kRawSpanPairsMax = 4000000;

// One arc set's arcs and rings, read alike for either set.
class SetArcs {
public:
    SetArcs(const AdminArcs& arcs, const AdminExactRings& store, ArcSet set) : arcs_(arcs), store_(store), set_(set) {}

    size_t size() const { return set_ == ArcSet::Full ? arcs_.arcs.size() : arcs_.minimal.arcs.size(); }
    const std::vector<uint32_t>& ring_refs() const {
        return set_ == ArcSet::Full ? arcs_.ring_refs : arcs_.minimal.ring_refs;
    }
    const std::vector<ArcRef>& refs() const { return set_ == ArcSet::Full ? arcs_.refs : arcs_.minimal.refs; }
    bool has_ring(uint32_t r) const { return ring_refs()[r + 1] > ring_refs()[r]; }

    // fn(vertices): arc a's vertices in its order.
    template <class Fn>
    void with(uint32_t a, Fn&& fn) const {
        if (set_ == ArcSet::Full) fn(arc_vertices(store_, arcs_.arcs[a].view));
        else fn(MinimalArcVertices(store_, arcs_, a));
    }
    uint32_t count(uint32_t a) const {
        return set_ == ArcSet::Full ? arcs_.arcs[a].view.count : arcs_.minimal.arcs[a].count;
    }
    // Whether arc a is a ring with no junction.
    bool closed(uint32_t a) const {
        return set_ == ArcSet::Full ? arcs_.arcs[a].view.closed : arcs_.minimal.arcs[a].closed;
    }
    // Arc a's raw vertices [from, to].
    void read(uint32_t a, std::vector<PackedLocation>& out, uint32_t from = 0, uint32_t to = UINT32_MAX) const {
        with(a, [&](const auto& v) {
            to = std::min(to, v.size() - 1);
            out.resize(to - from + 1);
            for (uint32_t i = from; i <= to; i++) out[i - from] = v[i];
        });
    }

private:
    const AdminArcs& arcs_;
    const AdminExactRings& store_;
    ArcSet set_;
};

// tier_vertices for arc a's raw vertices `raw` and its arc_grid marks.
void stored_vertices(const std::vector<PackedLocation>& raw, const ArcKeep& keep, uint32_t a, const ArcTier& tier,
                     uint8_t grid, std::vector<StoredVertex>& out) {
    out.clear();
    const uint32_t n = static_cast<uint32_t>(raw.size());
    if (!tier.simplifies()) {
        for (uint32_t i = 0; i < n; i++) out.push_back({i, raw[i]});
        return;
    }
    for (uint32_t i = 0; i < n; i++) {
        if (!keep.test(a, i)) continue;

        const PackedLocation p = snapped(raw[i], vertex_grid_e7(tier, grid, i, n));
        if (!out.empty() && out.back().at == p) {
            if (i + 1 == n && out.size() > 1) out.back().raw = i;
            continue;
        }
        out.push_back({i, p});
    }
}

void degrees_of(const std::vector<PackedLocation>& raw, std::vector<LatLng>& out) {
    out.resize(raw.size());
    for (size_t i = 0; i < raw.size(); i++) out[i] = {raw[i].lat(), raw[i].lon()};
}

// A dropped raw vertex and how far it lies from the kept segment over it.
struct Farthest {
    uint32_t at = 0;
    double distance = -1;  // none
};

// The farthest vertex of `pts` strictly between `from` and `to` that arc a
// doesn't keep, from the segment pts[from]-pts[to].
Farthest farthest_dropped(const std::vector<LatLng>& pts, const ArcKeep& keep, uint32_t a, uint32_t from, uint32_t to) {
    Farthest best;
    for (uint32_t k = from + 1; k < to; k++) {
        if (keep.test(a, k)) continue;

        const double d = segment_distance(pts[from], pts[to], pts[k]);
        if (d > best.distance) best = {k, d};
    }
    return best;
}

// The farthest of arc a's dropped vertices from its kept segments.
Farthest farthest_dropped(const std::vector<LatLng>& pts, const ArcKeep& keep, uint32_t a) {
    Farthest best;
    uint32_t prev = 0;
    for (uint32_t i = 1; i < pts.size(); i++) {
        if (!keep.test(a, i)) continue;

        if (i > prev + 1) {
            const Farthest f = farthest_dropped(pts, keep, a, prev, i);
            if (f.distance > best.distance) best = f;
        }
        prev = i;
    }
    return best;
}

// Twice the signed area of a, b, c: > 0 when c is left of a -> b, 0 in line.
__int128 orientation(PackedLocation a, PackedLocation b, PackedLocation c) {
    return static_cast<__int128>(int64_t(b.lon_e7) - a.lon_e7) * (int64_t(c.lat_e7) - a.lat_e7) -
           static_cast<__int128>(int64_t(b.lat_e7) - a.lat_e7) * (int64_t(c.lon_e7) - a.lon_e7);
}

int sign(__int128 v) { return (v > 0) - (v < 0); }

// Whether c, in line with a-b, lies on the closed segment.
bool within(PackedLocation a, PackedLocation b, PackedLocation c) {
    return std::min(a.lat_e7, b.lat_e7) <= c.lat_e7 && c.lat_e7 <= std::max(a.lat_e7, b.lat_e7) &&
           std::min(a.lon_e7, b.lon_e7) <= c.lon_e7 && c.lon_e7 <= std::max(a.lon_e7, b.lon_e7);
}

// Whether segments a-b and c-d meet anywhere but at an end they share: a
// crossing, an end on the other's inside, or segments in line overlapping.
// The same segment (either way) meets nothing.
bool segments_meet(PackedLocation a, PackedLocation b, PackedLocation c, PackedLocation d) {
    const bool ac = a == c, ad = a == d, bc = b == c, bd = b == d;
    const int shared = ac + ad + bc + bd;
    if (shared >= 2) return false;
    const int o1 = sign(orientation(a, b, c)), o2 = sign(orientation(a, b, d));
    const int o3 = sign(orientation(c, d, a)), o4 = sign(orientation(c, d, b));
    if (shared == 1) {
        if (o1 != 0 || o2 != 0) return false;
        // In line from the shared end: they overlap when either's other end
        // lies on the other segment.
        if ((ac || ad) && within(c, d, b)) return true;
        if ((bc || bd) && within(c, d, a)) return true;
        if ((ac || bc) && within(a, b, d)) return true;
        if ((ad || bd) && within(a, b, c)) return true;
        return false;
    }
    if (o1 != o2 && o3 != o4) return true;
    return (o1 == 0 && within(a, b, c)) || (o2 == 0 && within(a, b, d)) || (o3 == 0 && within(c, d, a)) ||
           (o4 == 0 && within(c, d, b));
}

struct Box {
    int32_t lat_lo = INT32_MAX, lat_hi = INT32_MIN, lng_lo = INT32_MAX, lng_hi = INT32_MIN;
    void add(PackedLocation p) {
        lat_lo = std::min(lat_lo, p.lat_e7);
        lat_hi = std::max(lat_hi, p.lat_e7);
        lng_lo = std::min(lng_lo, p.lon_e7);
        lng_hi = std::max(lng_hi, p.lon_e7);
    }
    bool overlaps(const Box& o) const {
        return lat_lo <= o.lat_hi && o.lat_lo <= lat_hi && lng_lo <= o.lng_hi && o.lng_lo <= lng_hi;
    }
};

Box box_of(PackedLocation a, PackedLocation b) {
    Box box;
    box.add(a);
    box.add(b);
    return box;
}

// A stored segment of a tier: between the stored vertices at raw places
// `from` and `to` of its arc.
struct Segment {
    uint32_t arc, from, to;
    PackedLocation a, b;
};

// Two segments a crossing can be between: not the same, nor one after the
// other along one arc.
bool segments_conflict(const Segment& p, const Segment& q) {
    if (p.arc == q.arc && (p.to == q.from || q.to == p.from)) return false;
    if (!box_of(p.a, p.b).overlaps(box_of(q.a, q.b))) return false;
    return segments_meet(p.a, p.b, q.a, q.b);
}

// The repair grid's cells: kArcRepairCellE7 squares from (-90, -180).
constexpr int64_t kCellColumns = int64_t(3600000000) / kArcRepairCellE7 + 1;

// Every cell segment a-b passes through (and perhaps one or two beside it),
// column by column: fn(cell).
template <class Fn>
void cells_of(PackedLocation a, PackedLocation b, Fn&& fn) {
    int64_t x0 = int64_t(a.lon_e7) + 1800000000, y0 = int64_t(a.lat_e7) + 900000000;
    int64_t x1 = int64_t(b.lon_e7) + 1800000000, y1 = int64_t(b.lat_e7) + 900000000;
    if (x0 > x1) {
        std::swap(x0, x1);
        std::swap(y0, y1);
    }
    constexpr int64_t C = kArcRepairCellE7;
    // The segment's latitude at x, rounded down or up.
    auto y_at = [&](int64_t x, bool up) {
        const __int128 num = static_cast<__int128>(y1 - y0) * (x - x0);
        const int64_t den = x1 - x0;
        __int128 q = num / den;
        if (num % den != 0 && ((num < 0) != up)) q += up ? 1 : -1;
        return y0 + static_cast<int64_t>(q);
    };
    for (int64_t cx = x0 / C; cx <= x1 / C; cx++) {
        int64_t lo = std::min(y0, y1), hi = std::max(y0, y1);
        if (x1 > x0) {
            const int64_t xa = std::max(x0, cx * C), xb = std::min(x1, (cx + 1) * C);
            lo = std::min(y_at(xa, false), y_at(xb, false));
            hi = std::max(y_at(xa, true), y_at(xb, true));
        }
        for (int64_t cy = lo / C; cy <= hi / C; cy++) fn(static_cast<uint32_t>(cy * kCellColumns + cx));
    }
}

uint64_t cell_entry(uint32_t cell, uint32_t segment) { return (uint64_t(cell) << 32) | segment; }
uint32_t entry_cell(uint64_t e) { return static_cast<uint32_t>(e >> 32); }
uint32_t entry_segment(uint64_t e) { return static_cast<uint32_t>(e); }

// What emit(i, out, scratch) appends for each i in [0, n), in index order:
// chunks of kCollectChunk indices handed out as workers free up, each worker
// with a Scratch of its own.
constexpr size_t kCollectChunk = 1024;

template <class T, class Scratch, class Emit>
std::vector<T> collect_in_order(size_t n, Emit emit, unsigned threads) {
    const size_t chunks = (n + kCollectChunk - 1) / kCollectChunk;
    std::vector<std::vector<T>> parts(chunks);
    std::vector<Scratch> scratch(threads);
    parallel_for_dynamic(chunks, 1, [&](size_t begin, size_t end, unsigned worker) {
        for (size_t c = begin; c < end; c++)
            for (size_t i = c * kCollectChunk; i < std::min(n, (c + 1) * kCollectChunk); i++)
                emit(i, parts[c], scratch[worker]);
    }, threads);
    size_t total = 0;
    for (const auto& part : parts) total += part.size();
    std::vector<T> out;
    out.reserve(total);
    for (auto& part : parts) {
        out.insert(out.end(), part.begin(), part.end());
        free_storage(part);
    }
    return out;
}

template <class T>
void sort_unique(std::vector<T>& v, unsigned threads) {
    parallel_sort(v.begin(), v.end(), std::less<T>(), threads);
    v.erase(std::unique(v.begin(), v.end()), v.end());
}

struct Scratch {
    std::vector<PackedLocation> raw, other;
    std::vector<LatLng> pts;
    std::vector<StoredVertex> stored;
    std::vector<PackedLocation> ring;
    FoldScratch fold;  // folds_over
};

// Which way a ring winds: the sign of its exact area, 0 for none.
int winding(const PackedLocation* v, size_t n) {
    const __int128 a2 = exact_ring_area2(v, static_cast<uint32_t>(n));
    return (a2 > 0) - (a2 < 0);
}

bool folds_over(const std::vector<PackedLocation>& ring, Scratch& s) {
    return ring_folds_over(ring, s.fold);
}

std::vector<LatLng> lat_lng_ring(const PackedLocation* v, size_t n, bool close) {
    std::vector<LatLng> out;
    out.reserve(n + 1);
    for (size_t i = 0; i < n; i++) out.push_back({double(v[i].lat_e7), double(v[i].lon_e7)});
    if (close && n > 0) out.push_back(out.front());
    return out;
}

// One simplified tier of one set, built in place: on its fine grid, until
// coarsen() puts it on its own.
class TierBuilder {
public:
    TierBuilder(const AdminArcs& arcs, const AdminExactRings& store, const ArcTier& tier, unsigned threads)
        : store_(store), set_(arcs, store, tier.set), tier_(tier), coarse_grid_e7_(tier.grid_e7), threads_(threads),
          arc_count_(set_.size()), keep_(arc_count_, [&](size_t a) { return set_.count(static_cast<uint32_t>(a)); }),
          grid_(arc_count_, 0) {
        tier_.grid_e7 = tier.fine_grid_e7;
        for (uint32_t r = 0; r < store.rings.size(); r++)
            if (set_.has_ring(r)) rings_.push_back(r);
        // Each arc's rings.
        user_at_.assign(arc_count_ + 1, 0);
        for (uint32_t r : rings_)
            for (uint32_t k = set_.ring_refs()[r]; k < set_.ring_refs()[r + 1]; k++) user_at_[set_.refs()[k].arc + 1]++;
        for (size_t a = 0; a < arc_count_; a++) user_at_[a + 1] += user_at_[a];
        users_.resize(user_at_.back());
        std::vector<uint32_t> next(user_at_.begin(), user_at_.end() - 1);
        for (uint32_t r : rings_)
            for (uint32_t k = set_.ring_refs()[r]; k < set_.ring_refs()[r + 1]; k++) users_[next[set_.refs()[k].arc]++] = r;
    }

    // Douglas-Peucker on every arc at its tolerance.
    void simplify() {
        std::vector<double> ring_cap(store_.rings.size(), INFINITY);
        if (tier_.cap_factor > 0)
            parallel_for(rings_.size(), [&](size_t begin, size_t end, unsigned) {
                for (size_t k = begin; k < end; k++) {
                    const RingSpan& ring = store_.rings[rings_[k]];
                    const __int128 a2 = exact_ring_area2(store_.vertices.data() + ring.begin, ring.count);
                    const double area = static_cast<double>(a2 < 0 ? -a2 : a2) * 0.5e-14;
                    ring_cap[rings_[k]] = tier_.cap_factor * std::sqrt(area);
                }
            }, threads_);
        parallel_for_dynamic(arc_count_, 64, [&](size_t begin, size_t end, unsigned) {
            Scratch s;
            for (size_t i = begin; i < end; i++) {
                const uint32_t a = static_cast<uint32_t>(i);
                set_.read(a, s.raw);
                degrees_of(s.raw, s.pts);
                double epsilon = meters_to_degrees(tier_.tolerance_m, s.pts[0].first);
                for (uint32_t u = user_at_[a]; u < user_at_[a + 1]; u++) epsilon = std::min(epsilon, ring_cap[users_[u]]);
                const bool ends_meet = s.raw.front() == s.raw.back();
                const std::vector<bool> kept = dp_keep_epsilon(s.pts, epsilon, ends_meet ? kMinClosedRing : 2);
                for (uint32_t k = 0; k < kept.size(); k++)
                    if (kept[k]) keep_.set(a, k);
            }
        }, threads_);
    }

    // Gives a ring that is degenerate once snapped (under 3 vertices, or no
    // area) the farthest dropped vertex of its arc that strays most, round
    // after round, every ring checked first and then those a round touched.
    // Returns the arcs it changed.
    std::vector<uint32_t> restore_degenerate_rings(ArcTierCounts& counts) {
        std::vector<uint32_t> suspects = rings_, changed;
        for (int round = 0;; round++) {
            std::vector<uint32_t> bad = collect_in_order<uint32_t, Scratch>(suspects.size(),
                [&](size_t k, std::vector<uint32_t>& out, Scratch& s) {
                    if (!stored_ring_valid(suspects[k], s)) out.push_back(suspects[k]);
                }, threads_);
            counts.degenerate_rings = bad.size();
            if (bad.empty() || round == kArcValidityRounds) break;

            // Each bad ring's choice, then all of them, in ring order.
            std::vector<std::pair<uint32_t, uint32_t>> back(bad.size(), {UINT32_MAX, 0});
            parallel_for_dynamic(bad.size(), 16, [&](size_t begin, size_t end, unsigned) {
                Scratch s;
                for (size_t k = begin; k < end; k++) {
                    Farthest best;
                    for (uint32_t i = set_.ring_refs()[bad[k]]; i < set_.ring_refs()[bad[k] + 1]; i++) {
                        const uint32_t a = set_.refs()[i].arc;
                        set_.read(a, s.raw);
                        degrees_of(s.raw, s.pts);
                        const Farthest f = farthest_dropped(s.pts, keep_, a);
                        if (f.distance > best.distance) {
                            best = f;
                            back[k] = {a, f.at};
                        }
                    }
                }
            }, threads_);
            std::vector<uint32_t> touched;
            for (const auto& [a, at] : back) {
                if (a == UINT32_MAX || keep_.test(a, at)) continue;

                keep_.set(a, at);
                counts.validity_restored++;
                touched.push_back(a);
            }
            if (touched.empty()) break;
            counts.validity_rounds++;
            sort_unique(touched, threads_);
            changed.insert(changed.end(), touched.begin(), touched.end());
            suspects = bad;
            for (uint32_t a : touched) suspects.insert(suspects.end(), users_.begin() + user_at_[a], users_.begin() + user_at_[a + 1]);
            sort_unique(suspects, threads_);
        }
        sort_unique(changed, threads_);
        return changed;
    }

    // Lays every stored segment into the grid and finds every pair that meets.
    void build_grid() {
        std::vector<uint32_t> segments_of(arc_count_);
        segs_ = collect_in_order<Segment, Scratch>(arc_count_, [&](size_t a, std::vector<Segment>& out, Scratch& s) {
            const size_t before = out.size();
            add_segments(static_cast<uint32_t>(a), s, out);
            segments_of[a] = static_cast<uint32_t>(out.size() - before);
        }, threads_);
        if (segs_.size() >= UINT32_MAX) throw std::length_error("arc tier segments past 2^32");
        base_at_ = parallel_offsets<uint32_t>(arc_count_, [&](size_t a) { return segments_of[a]; }, threads_);
        alive_.assign(segs_.size(), 1);
        base_cells_ = cells_from(0);
        add_conflicts(meetings_in_cells());
    }

    // Repair rounds: each gives the spans of the crossings simplification
    // made their farthest dropped vertices back, and the polygons whose
    // rings stopped nesting theirs (restore_unnested). Then the validity
    // rounds once more (a vertex given back can cross). Once coarsened, each
    // round keeps the rings of the arcs it changed fine where they collapse
    // instead, so no validity rounds follow.
    void repair(const std::vector<bool>& nests_exact, ArcTierCounts& counts) {
        size_t& made = coarse_ ? counts.crossings_coarse : counts.crossings_simplified;
        for (const Conflict& c : conflicts_) made += !c.raw;
        nesting_candidates(nests_exact);
        std::vector<uint32_t> suspects = candidates_;
        for (int round = 0; round < kArcRepairRounds; round++) {
            std::vector<uint32_t> dirty = restore_crossed_spans(counts);
            const std::vector<uint32_t> unnested = not_nesting(suspects);
            const std::vector<uint32_t> nested = restore_unnested(unnested, counts);
            dirty.insert(dirty.end(), nested.begin(), nested.end());
            sort_unique(dirty, threads_);
            if (dirty.empty()) break;
            counts.repair_rounds++;
            if (coarse_) with_collapsing_rings_kept_fine(dirty, counts);
            add_conflicts(meetings_of(update(dirty)));
            suspects = unnested;
            for (uint32_t a : dirty)
                for (uint32_t u = user_at_[a]; u < user_at_[a + 1]; u++)
                    if (polygon_of_[users_[u]] != UINT32_MAX) suspects.push_back(polygon_of_[users_[u]]);
            sort_unique(suspects, threads_);
        }
        if (coarse_) return;
        const std::vector<uint32_t> changed = restore_degenerate_rings(counts);
        if (!changed.empty()) add_conflicts(meetings_of(update(changed)));
    }

    // What the repair leaves: crossings, the segments of those with nothing
    // dropped, and polygons whose rings don't nest.
    void count_left(ArcTierCounts& counts) {
        std::vector<uint32_t> left;
        for (const Conflict& c : conflicts_) {
            if (!alive_[c.s] || !alive_[c.t]) continue;
            if (c.raw) {
                counts.raw_crossings++;
                continue;
            }
            counts.crossings_left++;
            left.push_back(c.s);
            left.push_back(c.t);
        }
        sort_unique(left, threads_);
        counts.unrepairable_segments = parallel_count(left.size(), [&](size_t k) {
            const Segment& g = segs_[left[k]];
            for (uint32_t i = g.from + 1; i < g.to; i++)
                if (!keep_.test(g.arc, i)) return false;
            return true;
        }, threads_);
        counts.rings_nest_introduced = not_nesting(candidates_).size();
        free_grid();
    }

    void count_vertices(ArcTierCounts& counts) const {
        std::vector<size_t> kept(arc_count_), stored(arc_count_);
        std::vector<uint8_t> one_point(arc_count_);
        parallel_for_dynamic(arc_count_, 256, [&](size_t begin, size_t end, unsigned) {
            Scratch s;
            for (size_t i = begin; i < end; i++) {
                const uint32_t a = static_cast<uint32_t>(i);
                set_.read(a, s.raw);
                stored_vertices(s.raw, keep_, a, tier_, grid_[a], s.stored);
                size_t k = 0;
                for (uint32_t v = 0; v < s.raw.size(); v++) k += keep_.test(a, v);
                kept[a] = k;
                stored[a] = s.stored.size();
                one_point[a] = s.stored.size() == 1;
            }
        }, threads_);
        for (size_t a = 0; a < arc_count_; a++) {
            counts.kept_vertices += kept[a];
            counts.stored_vertices += stored[a];
            counts.single_point_arcs += one_point[a];
        }
    }

    ArcKeep take_keep() { return std::move(keep_); }
    std::vector<uint8_t> take_grid() { return std::move(grid_); }
    std::vector<uint64_t> take_fine_junctions() { return std::move(fine_junctions_); }
    std::vector<uint8_t> take_fine_valid() { return std::move(fine_valid_); }

    // Polygons whose rings nest in exact but still not once stored after the
    // repair keep their arcs and those arcs' junctions on the e7 grid
    // (keep_on_e7_grid), round after round while that changes an arc: the
    // snap alone moved a ring onto another, with no dropped vertex to give
    // back. Once coarsened, each round keeps the rings of the arcs it
    // changed fine where they collapse.
    void keep_unnested_on_e7_grid(ArcTierCounts& counts) {
        std::vector<uint32_t> unnested = not_nesting(candidates_);
        while (!unnested.empty()) {
            std::vector<uint32_t> changed = keep_on_e7_grid(unnested);
            if (changed.empty()) break;
            counts.e7_grid_rounds++;
            if (coarse_) with_collapsing_rings_kept_fine(changed, counts);
            add_conflicts(meetings_of(update(changed)));
            std::vector<uint32_t> suspects = unnested;
            for (uint32_t a : changed)
                for (uint32_t u = user_at_[a]; u < user_at_[a + 1]; u++)
                    if (polygon_of_[users_[u]] != UINT32_MAX) suspects.push_back(polygon_of_[users_[u]]);
            sort_unique(suspects, threads_);
            unnested = not_nesting(suspects);
        }
        counts.e7_grid_arcs =
            parallel_count(arc_count_, [&](size_t a) { return (grid_[a] & arc_grid::kE7Whole) != 0; }, threads_);
    }

    // Notes the rings of the set valid on the fine grid, then puts the tier
    // on its own grid, every arc and junction there, and keeps the rings
    // that collapses fine (keep_collapsing_rings_fine). The segment grid
    // must be gone (count_left).
    void coarsen(ArcTierCounts& counts) {
        std::vector<Scratch> scratch(threads_);
        fine_valid_.assign(store_.rings.size(), 0);
        fine_winding_.assign(store_.rings.size(), 0);
        fine_folds_.assign(store_.rings.size(), 0);
        parallel_for_dynamic(rings_.size(), 64, [&](size_t begin, size_t end, unsigned worker) {
            Scratch& s = scratch[worker];
            for (size_t k = begin; k < end; k++) {
                const uint32_t r = rings_[k];
                fine_valid_[r] = stored_ring_valid(r, s);
                fine_winding_[r] = static_cast<int8_t>(winding(s.ring.data(), s.ring.size()));
                fine_folds_[r] = fine_valid_[r] && folds_over(s.ring, s);
            }
        }, threads_);
        size_t valid = 0;
        for (uint32_t r : rings_) valid += fine_valid_[r];
        counts.fine_degenerate_rings = rings_.size() - valid;
        fine_keep_ = keep_;
        put_back_.assign(arc_count_, 0);
        tier_.grid_e7 = coarse_grid_e7_;
        coarse_ = true;
        ends_ = collect_in_order<ArcEnd, Scratch>(arc_count_, [&](size_t i, std::vector<ArcEnd>& out, Scratch&) {
            const uint32_t a = static_cast<uint32_t>(i);
            if (set_.closed(a)) return;
            set_.with(a, [&](const auto& v) {
                out.push_back({vertex_key(v[0]), a, false});
                out.push_back({vertex_key(v[v.size() - 1]), a, true});
            });
        }, threads_);
        parallel_sort(ends_.begin(), ends_.end(), std::less<ArcEnd>(), threads_);
        kept_fine_.assign(store_.rings.size(), 0);
        keep_collapsing_rings_fine(rings_, counts);
    }

    // After the repair on the tier's own grid: puts the arcs of every
    // crossing it leaves and of every polygon whose rings stop nesting back
    // as the fine build left them (put_back), round after round over the
    // crossings and polygons a round makes, the rings of the arcs it changes
    // kept fine where they collapse, until a round has nothing to put back.
    // A crossing between two arcs put back is one the fine build has, and a
    // polygon all of whose arcs are put back nests as it does there.
    void put_back_crossed_and_unnested(ArcTierCounts& counts) {
        std::vector<uint32_t> suspects = candidates_;
        for (int round = 0; round < kArcRepairRounds; round++) {
            std::vector<uint32_t> arcs;
            for (const Conflict& c : conflicts_)
                if (!c.raw && alive_[c.s] && alive_[c.t]) {
                    arcs.push_back(segs_[c.s].arc);
                    arcs.push_back(segs_[c.t].arc);
                }
            const std::vector<uint32_t> unnested = not_nesting(suspects);
            for (uint32_t p : unnested) {
                const auto& poly = store_.polygons[p];
                for (uint32_t r = poly.first_ring; r < poly.first_ring + poly.ring_count; r++)
                    for (uint32_t k = set_.ring_refs()[r]; k < set_.ring_refs()[r + 1]; k++) arcs.push_back(set_.refs()[k].arc);
            }
            std::vector<uint32_t> changed = put_back(arcs, counts);
            if (changed.empty()) break;
            counts.put_back_rounds++;
            with_collapsing_rings_kept_fine(changed, counts);
            add_conflicts(meetings_of(update(changed)));
            suspects = unnested;
            for (uint32_t a : changed)
                for (uint32_t u = user_at_[a]; u < user_at_[a + 1]; u++)
                    if (polygon_of_[users_[u]] != UINT32_MAX) suspects.push_back(polygon_of_[users_[u]]);
            sort_unique(suspects, threads_);
        }
    }

    // Last, the rings valid on the fine grid that collapse still, though
    // their arcs and junctions are all fine: the repair on the tier's grid
    // gave one a vertex back that takes its area away (a bowtie the raw ring
    // already is), and the loop has nothing left to move. And those that
    // fold (folds): a strip narrower than the tier's grid that it turns
    // over, which the ring and its neighbours on both sides then all cover;
    // no crossing shows it, and the ring keeps an area. Puts their arcs
    // back as the fine build left them, then puts back the arcs of the
    // crossings and polygons not nesting that makes and runs the e7 grid
    // rounds once more, round after round until none collapses or folds or a
    // round has nothing to put back. Nothing gives a vertex back to an arc
    // put back, so a ring all of whose arcs are put back is the fine
    // build's. One that still collapses (an arc of it on the e7 grid since)
    // is left for the check encode_admin_arcs makes; one that still folds
    // is counted (folds_left).
    void put_back_collapsing_rings(ArcTierCounts& counts) {
        for (int round = 0; round < kArcRepairRounds; round++) {
            const std::vector<uint32_t> collapsing = collect_in_order<uint32_t, Scratch>(rings_.size(),
                [&](size_t k, std::vector<uint32_t>& out, Scratch& s) {
                    const uint32_t r = rings_[k];
                    if (fine_valid_[r] && (!stored_ring_valid(r, s) || folds(r, s))) out.push_back(r);
                }, threads_);
            if (collapsing.empty()) break;

            std::vector<uint32_t> arcs;
            size_t folding = 0;
            Scratch s;
            for (uint32_t r : collapsing) {
                folding += stored_ring_valid(r, s);
                for (uint32_t k = set_.ring_refs()[r]; k < set_.ring_refs()[r + 1]; k++) arcs.push_back(set_.refs()[k].arc);
            }
            std::vector<uint32_t> changed = put_back(arcs, counts);
            if (changed.empty()) break;
            counts.collapsed_put_back_rounds++;
            counts.collapsed_put_back_rings += collapsing.size();
            counts.folded_put_back_rings += folding;
            with_collapsing_rings_kept_fine(changed, counts);
            add_conflicts(meetings_of(update(changed)));
            put_back_crossed_and_unnested(counts);
            keep_unnested_on_e7_grid(counts);
        }
    }

    // The degenerate rings left: under 3 vertices or of no area.
    void count_degenerate(ArcTierCounts& counts) const {
        counts.degenerate_rings = collect_in_order<uint32_t, Scratch>(rings_.size(),
            [&](size_t k, std::vector<uint32_t>& out, Scratch& s) {
                if (!stored_ring_valid(rings_[k], s)) out.push_back(rings_[k]);
            }, threads_).size();
    }

    // The rings valid on the fine grid that fold still (folds).
    void count_folds(ArcTierCounts& counts) const {
        counts.folds_left = collect_in_order<uint32_t, Scratch>(rings_.size(),
            [&](size_t k, std::vector<uint32_t>& out, Scratch& s) {
                const uint32_t r = rings_[k];
                if (fine_valid_[r] && stored_ring_valid(r, s) && folds(r, s)) out.push_back(r);
            }, threads_).size();
    }

private:
    struct Conflict {
        uint32_t s, t;
        bool raw;
    };

    // The polygons of the set with holes whose exact rings nest, and the
    // polygon of each of their rings.
    void nesting_candidates(const std::vector<bool>& nests_exact) {
        candidates_.clear();
        polygon_of_.assign(store_.rings.size(), UINT32_MAX);
        for (uint32_t p = 0; p < store_.polygons.size(); p++) {
            const auto& poly = store_.polygons[p];
            if (poly.ring_count < 2 || !nests_exact[p] || !set_.has_ring(poly.first_ring)) continue;
            candidates_.push_back(p);
            for (uint32_t r = 0; r < poly.ring_count; r++) polygon_of_[poly.first_ring + r] = p;
        }
    }

    // Whether polygon p's stored rings nest (rings_nest), a degenerate ring
    // left out (none at all for a degenerate outer ring).
    bool stored_rings_nest(uint32_t p, Scratch& s) const {
        const auto& poly = store_.polygons[p];
        stored_ring(poly.first_ring, s);
        if (!valid(s.ring)) return true;
        const std::vector<LatLng> outer = lat_lng_ring(s.ring.data(), s.ring.size(), true);
        std::vector<std::vector<LatLng>> holes;
        for (uint32_t r = 1; r < poly.ring_count; r++) {
            stored_ring(poly.first_ring + r, s);
            if (valid(s.ring)) holes.push_back(lat_lng_ring(s.ring.data(), s.ring.size(), true));
        }
        return rings_nest(outer, holes);
    }

    // Those of `polygons` whose stored rings don't nest, in order; the
    // largest checked first.
    std::vector<uint32_t> not_nesting(const std::vector<uint32_t>& polygons) const {
        const std::vector<uint32_t> by_cost = costliest_first(polygons.size(), [&](size_t k) {
            const auto& poly = store_.polygons[polygons[k]];
            const RingSpan& last = store_.rings[poly.first_ring + poly.ring_count - 1];
            return float(last.begin + last.count - store_.rings[poly.first_ring].begin);
        }, threads_);
        std::vector<uint8_t> unnested(polygons.size(), 0);
        std::vector<Scratch> scratch(threads_);
        parallel_for_dynamic(polygons.size(), 1, [&](size_t begin, size_t end, unsigned worker) {
            for (size_t k = begin; k < end; k++) unnested[by_cost[k]] = !stored_rings_nest(polygons[by_cost[k]], scratch[worker]);
        }, threads_);
        std::vector<uint32_t> out;
        for (size_t k = 0; k < polygons.size(); k++)
            if (unnested[k]) out.push_back(polygons[k]);
        return out;
    }

    // Gives back, for each polygon of `unnested`, the farthest dropped vertex
    // of every stored segment of one ring whose raw span's box holds a stored
    // vertex of another ring (one not on an arc the first ring uses): a ring
    // simplified across another without crossing it. Returns the arcs it
    // changed.
    std::vector<uint32_t> restore_unnested(const std::vector<uint32_t>& unnested, ArcTierCounts& counts) {
        std::vector<std::vector<std::pair<uint32_t, uint32_t>>> back(unnested.size());
        parallel_for_dynamic(unnested.size(), 1, [&](size_t begin, size_t end, unsigned) {
            Scratch s;
            struct Point {
                PackedLocation at;
                uint32_t ring, arc;
            };
            for (size_t k = begin; k < end; k++) {
                const auto& poly = store_.polygons[unnested[k]];
                std::vector<Point> points;
                std::vector<std::vector<uint32_t>> ring_arcs(poly.ring_count);
                for (uint32_t r = 0; r < poly.ring_count; r++)
                    for (uint32_t i = set_.ring_refs()[poly.first_ring + r]; i < set_.ring_refs()[poly.first_ring + r + 1]; i++) {
                        const uint32_t a = set_.refs()[i].arc;
                        ring_arcs[r].push_back(a);
                        set_.read(a, s.raw);
                        stored_vertices(s.raw, keep_, a, tier_, grid_[a], s.stored);
                        for (const StoredVertex& v : s.stored) points.push_back({v.at, r, a});
                    }
                for (auto& arcs_of : ring_arcs) std::sort(arcs_of.begin(), arcs_of.end());
                std::sort(points.begin(), points.end(), [](const Point& x, const Point& y) {
                    return x.at.lon_e7 < y.at.lon_e7;
                });
                for (uint32_t r = 0; r < poly.ring_count; r++)
                    for (uint32_t a : ring_arcs[r]) {
                        set_.read(a, s.raw);
                        stored_vertices(s.raw, keep_, a, tier_, grid_[a], s.stored);
                        degrees_of(s.raw, s.pts);
                        for (size_t j = 1; j < s.stored.size(); j++) {
                            const uint32_t from = s.stored[j - 1].raw, to = s.stored[j].raw;
                            if (to <= from + 1) continue;
                            Box span;
                            for (uint32_t i = from; i <= to; i++) span.add(s.raw[i]);
                            auto it = std::lower_bound(points.begin(), points.end(), span.lng_lo,
                                                       [](const Point& x, int32_t lng) { return x.at.lon_e7 < lng; });
                            bool holds = false;
                            for (; it != points.end() && it->at.lon_e7 <= span.lng_hi && !holds; ++it)
                                holds = it->ring != r && it->at.lat_e7 >= span.lat_lo && it->at.lat_e7 <= span.lat_hi &&
                                        !std::binary_search(ring_arcs[r].begin(), ring_arcs[r].end(), it->arc);
                            if (!holds) continue;
                            const Farthest f = farthest_dropped(s.pts, keep_, a, from, to);
                            if (f.distance >= 0) back[k].push_back({a, f.at});
                        }
                    }
            }
        }, threads_);
        std::vector<uint32_t> changed;
        for (const auto& polygon_back : back)
            for (const auto& [a, at] : polygon_back) {
                if (keep_.test(a, at)) continue;
                keep_.set(a, at);
                counts.nesting_restored++;
                changed.push_back(a);
            }
        sort_unique(changed, threads_);
        return changed;
    }

    // Puts every arc of `polygons`' rings wholly on the e7 grid, and the end
    // of every other arc of the set at one of their junctions; returns the
    // arcs it changed, ascending.
    std::vector<uint32_t> keep_on_e7_grid(const std::vector<uint32_t>& polygons) {
        std::vector<uint32_t> whole;
        for (uint32_t p : polygons) {
            const auto& poly = store_.polygons[p];
            for (uint32_t r = poly.first_ring; r < poly.first_ring + poly.ring_count; r++)
                for (uint32_t k = set_.ring_refs()[r]; k < set_.ring_refs()[r + 1]; k++) whole.push_back(set_.refs()[k].arc);
        }
        sort_unique(whole, threads_);
        std::vector<uint64_t> junctions;
        for (uint32_t a : whole) {
            if (set_.closed(a)) continue;
            set_.with(a, [&](const auto& v) {
                junctions.push_back(vertex_key(v[0]));
                junctions.push_back(vertex_key(v[v.size() - 1]));
            });
        }
        sort_unique(junctions, threads_);
        std::vector<uint8_t> grid(arc_count_);
        parallel_for(arc_count_, [&](size_t begin, size_t end, unsigned) {
            for (size_t i = begin; i < end; i++) {
                const uint32_t a = static_cast<uint32_t>(i);
                grid[a] = grid_[a];
                if (std::binary_search(whole.begin(), whole.end(), a)) {
                    grid[a] |= arc_grid::kE7Whole;
                    continue;
                }
                if (set_.closed(a)) continue;
                set_.with(a, [&](const auto& v) {
                    if (std::binary_search(junctions.begin(), junctions.end(), vertex_key(v[0])))
                        grid[a] |= arc_grid::kE7First;
                    if (std::binary_search(junctions.begin(), junctions.end(), vertex_key(v[v.size() - 1])))
                        grid[a] |= arc_grid::kE7Last;
                });
            }
        }, threads_);
        std::vector<uint32_t> changed;
        for (uint32_t a = 0; a < arc_count_; a++)
            if (grid[a] != grid_[a]) changed.push_back(a);
        grid_ = std::move(grid);
        return changed;
    }

    // The rings that use `arcs`, ascending.
    std::vector<uint32_t> rings_of(const std::vector<uint32_t>& arcs) const {
        std::vector<uint32_t> out;
        for (uint32_t a : arcs) out.insert(out.end(), users_.begin() + user_at_[a], users_.begin() + user_at_[a + 1]);
        sort_unique(out, threads_);
        return out;
    }

    // Keeps fine those of `suspects` (rings, ascending) that the fine grid
    // holds valid and that collapse as stored, under 3 vertices or of no
    // area: each one's arcs, and its junctions in every arc that ends there,
    // that arc kept fine too. Then the same for the rings of the arcs that
    // changed, round after round, until none collapses or a round changes
    // nothing: a collapsing ring all fine already is left for the check
    // encode_admin_arcs makes. Returns the arcs it changed, ascending.
    std::vector<uint32_t> keep_collapsing_rings_fine(std::vector<uint32_t> suspects, ArcTierCounts& counts) {
        std::vector<uint32_t> changed;
        while (!suspects.empty()) {
            const std::vector<uint32_t> collapsing = collect_in_order<uint32_t, Scratch>(suspects.size(),
                [&](size_t k, std::vector<uint32_t>& out, Scratch& s) {
                    if (fine_valid_[suspects[k]] && !stored_ring_valid(suspects[k], s)) out.push_back(suspects[k]);
                }, threads_);
            if (collapsing.empty()) break;

            std::vector<uint32_t> arcs;
            for (uint32_t r : collapsing) {
                counts.fine_rings += !kept_fine_[r];
                kept_fine_[r] = 1;
                for (uint32_t k = set_.ring_refs()[r]; k < set_.ring_refs()[r + 1]; k++) arcs.push_back(set_.refs()[k].arc);
            }
            const std::vector<uint32_t> round = keep_fine(arcs);
            if (round.empty()) break;

            counts.coarse_rounds++;
            changed.insert(changed.end(), round.begin(), round.end());
            suspects = rings_of(round);
        }
        sort_unique(changed, threads_);
        return changed;
    }

    // Keeps `arcs` fine, and their junctions in every arc that ends there,
    // that arc kept fine too. Returns the arcs whose marks changed,
    // ascending.
    std::vector<uint32_t> keep_fine(std::vector<uint32_t> arcs) {
        sort_unique(arcs, threads_);
        std::vector<uint64_t> junctions;
        for (uint32_t a : arcs) {
            if (set_.closed(a)) continue;
            set_.with(a, [&](const auto& v) {
                junctions.push_back(vertex_key(v[0]));
                junctions.push_back(vertex_key(v[v.size() - 1]));
            });
        }
        sort_unique(junctions, threads_);
        std::vector<uint32_t> changed;
        auto mark = [&](uint32_t a, uint8_t bits) {
            if ((grid_[a] | bits) == grid_[a]) return;
            grid_[a] |= bits;
            changed.push_back(a);
        };
        // A closed arc has no junction: all of it is the arc's.
        for (uint32_t a : arcs) mark(a, set_.closed(a) ? arc_grid::kFineWhole : arc_grid::kFineBetween);
        std::vector<uint64_t> added;
        std::set_difference(junctions.begin(), junctions.end(), fine_junctions_.begin(), fine_junctions_.end(),
                            std::back_inserter(added));
        for (uint64_t j : added)
            for (auto it = std::lower_bound(ends_.begin(), ends_.end(), ArcEnd{j, 0, false});
                 it != ends_.end() && it->junction == j; ++it)
                mark(it->arc, arc_grid::kFineBetween | (it->last ? arc_grid::kFineLast : arc_grid::kFineFirst));
        std::vector<uint64_t> merged(fine_junctions_.size() + added.size());
        std::merge(fine_junctions_.begin(), fine_junctions_.end(), added.begin(), added.end(), merged.begin());
        fine_junctions_ = std::move(merged);
        sort_unique(changed, threads_);
        return changed;
    }

    // Puts those of `arcs` not yet put back as the fine build left them:
    // its keep marks (the vertices the repair on the tier's grid gave back
    // dropped again), kept fine with their junctions (keep_fine). Returns
    // the arcs that changed, ascending.
    std::vector<uint32_t> put_back(std::vector<uint32_t> arcs, ArcTierCounts& counts) {
        sort_unique(arcs, threads_);
        arcs.erase(std::remove_if(arcs.begin(), arcs.end(), [&](uint32_t a) { return put_back_[a] != 0; }), arcs.end());
        keep_.copy_arcs(fine_keep_, arcs);
        for (uint32_t a : arcs) put_back_[a] = 1;
        counts.put_back_arcs += arcs.size();
        std::vector<uint32_t> changed = keep_fine(arcs);
        changed.insert(changed.end(), arcs.begin(), arcs.end());
        sort_unique(changed, threads_);
        return changed;
    }

    // Adds to `arcs` (ascending) the arcs keeping the rings that use them
    // fine changes (keep_collapsing_rings_fine).
    void with_collapsing_rings_kept_fine(std::vector<uint32_t>& arcs, ArcTierCounts& counts) {
        const std::vector<uint32_t> fine = keep_collapsing_rings_fine(rings_of(arcs), counts);
        arcs.insert(arcs.end(), fine.begin(), fine.end());
        sort_unique(arcs, threads_);
    }

    // Ring r as the tier stores it, into s.ring: its arcs' stored vertices
    // chained, each vertex once where its neighbour repeats it, without the
    // closing one.
    void stored_ring(uint32_t r, Scratch& s) const {
        s.ring.clear();
        for (uint32_t k = set_.ring_refs()[r]; k < set_.ring_refs()[r + 1]; k++) {
            const ArcRef ref = set_.refs()[k];
            set_.read(ref.arc, s.raw);
            stored_vertices(s.raw, keep_, ref.arc, tier_, grid_[ref.arc], s.stored);
            const size_t n = s.stored.size();
            for (size_t i = 0; i < n; i++) {
                const PackedLocation p = s.stored[ref.reversed ? n - 1 - i : i].at;
                if (s.ring.empty() || s.ring.back() != p) s.ring.push_back(p);
            }
        }
        if (s.ring.size() >= 2 && s.ring.front() == s.ring.back()) s.ring.pop_back();
    }
    static bool valid(const std::vector<PackedLocation>& ring) {
        return ring.size() >= 3 && exact_ring_area2(ring.data(), static_cast<uint32_t>(ring.size())) != 0;
    }
    bool stored_ring_valid(uint32_t r, Scratch& s) const {
        stored_ring(r, s);
        return valid(s.ring);
    }
    // Whether ring r, as s.ring holds it on the tier's grid (stored_ring),
    // folds where the fine build's doesn't: it winds the other way, or
    // folds over itself (folds_over).
    bool folds(uint32_t r, Scratch& s) const {
        const int w = winding(s.ring.data(), s.ring.size());
        return (w != 0 && fine_winding_[r] != 0 && w != fine_winding_[r]) || (!fine_folds_[r] && folds_over(s.ring, s));
    }

    void add_segments(uint32_t a, Scratch& s, std::vector<Segment>& out) const {
        set_.read(a, s.raw);
        stored_vertices(s.raw, keep_, a, tier_, grid_[a], s.stored);
        for (size_t k = 1; k < s.stored.size(); k++)
            out.push_back({a, s.stored[k - 1].raw, s.stored[k].raw, s.stored[k - 1].at, s.stored[k].at});
    }

    // The grid entries of segments [first, end), sorted.
    std::vector<uint64_t> cells_from(uint32_t first) const {
        std::vector<uint64_t> cells = collect_in_order<uint64_t, Scratch>(segs_.size() - first,
            [&](size_t k, std::vector<uint64_t>& out, Scratch&) {
                const uint32_t s = first + static_cast<uint32_t>(k);
                cells_of(segs_[s].a, segs_[s].b, [&](uint32_t cell) { out.push_back(cell_entry(cell, s)); });
            }, threads_);
        parallel_sort(cells.begin(), cells.end(), std::less<uint64_t>(), threads_);
        return cells;
    }

    // Every pair of live segments meeting in a cell of the base grid, each
    // cell swept by longitude.
    std::vector<std::pair<uint32_t, uint32_t>> meetings_in_cells() const {
        std::vector<size_t> runs;
        for (size_t i = 0; i < base_cells_.size(); i++)
            if (i == 0 || entry_cell(base_cells_[i]) != entry_cell(base_cells_[i - 1])) runs.push_back(i);
        runs.push_back(base_cells_.size());
        std::vector<std::vector<std::pair<uint32_t, uint32_t>>> found(threads_);
        parallel_for_dynamic(runs.size() - 1, 16, [&](size_t begin, size_t end, unsigned worker) {
            struct Item {
                int32_t lng_lo, lng_hi;
                uint32_t s;
            };
            std::vector<Item> items;
            for (size_t run = begin; run < end; run++) {
                items.clear();
                for (size_t i = runs[run]; i < runs[run + 1]; i++) {
                    const uint32_t s = entry_segment(base_cells_[i]);
                    if (!alive_[s]) continue;
                    const Segment& g = segs_[s];
                    items.push_back({std::min(g.a.lon_e7, g.b.lon_e7), std::max(g.a.lon_e7, g.b.lon_e7), s});
                }
                std::sort(items.begin(), items.end(), [](const Item& x, const Item& y) {
                    return x.lng_lo != y.lng_lo ? x.lng_lo < y.lng_lo : x.s < y.s;
                });
                for (size_t i = 0; i < items.size(); i++)
                    for (size_t j = i + 1; j < items.size() && items[j].lng_lo <= items[i].lng_hi; j++)
                        if (segments_conflict(segs_[items[i].s], segs_[items[j].s]))
                            found[worker].push_back(std::minmax(items[i].s, items[j].s));
            }
        }, threads_);
        return joined(found);
    }

    // Every pair of live segments meeting that has a segment from `first` on.
    std::vector<std::pair<uint32_t, uint32_t>> meetings_of(uint32_t first) const {
        std::vector<std::vector<std::pair<uint32_t, uint32_t>>> found(threads_);
        parallel_for_dynamic(segs_.size() - first, 16, [&](size_t begin, size_t end, unsigned worker) {
            for (size_t k = begin; k < end; k++) {
                const uint32_t s = first + static_cast<uint32_t>(k);
                if (!alive_[s]) continue;
                cells_of(segs_[s].a, segs_[s].b, [&](uint32_t cell) {
                    for (const std::vector<uint64_t>* cells : {&base_cells_, &overlay_cells_}) {
                        auto it = std::lower_bound(cells->begin(), cells->end(), cell_entry(cell, 0));
                        for (; it != cells->end() && entry_cell(*it) == cell; ++it) {
                            const uint32_t t = entry_segment(*it);
                            if (t == s || !alive_[t] || (t >= first && t < s)) continue;
                            if (segments_conflict(segs_[s], segs_[t])) found[worker].push_back(std::minmax(s, t));
                        }
                    }
                });
            }
        }, threads_);
        return joined(found);
    }

    std::vector<std::pair<uint32_t, uint32_t>> joined(std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& found) const {
        std::vector<std::pair<uint32_t, uint32_t>> all;
        for (auto& f : found) {
            all.insert(all.end(), f.begin(), f.end());
            free_storage(f);
        }
        sort_unique(all, threads_);
        return all;
    }

    // Whether two segments' raw spans already meet: the crossing is in the
    // data, not made by simplifying.
    bool raw_spans_meet(const Segment& p, const Segment& q, Scratch& s) const {
        set_.read(p.arc, s.raw, p.from, p.to);
        set_.read(q.arc, s.other, q.from, q.to);
        Box bp, bq;
        for (const PackedLocation& v : s.raw) bp.add(v);
        for (const PackedLocation& v : s.other) bq.add(v);
        auto edges_over = [](const std::vector<PackedLocation>& v, const Box& box) {
            std::vector<uint32_t> out;
            for (uint32_t i = 0; i + 1 < v.size(); i++)
                if (box_of(v[i], v[i + 1]).overlaps(box)) out.push_back(i);
            return out;
        };
        const std::vector<uint32_t> ep = edges_over(s.raw, bq), eq = edges_over(s.other, bp);
        if (uint64_t(ep.size()) * eq.size() > kRawSpanPairsMax) return false;
        for (uint32_t i : ep)
            for (uint32_t j : eq) {
                if (p.arc == q.arc) {
                    const uint32_t gi = p.from + i, gj = q.from + j;
                    if (gi == gj || gi + 1 == gj || gj + 1 == gi) continue;
                }
                const PackedLocation a = s.raw[i], b = s.raw[i + 1], c = s.other[j], d = s.other[j + 1];
                if (box_of(a, b).overlaps(box_of(c, d)) && segments_meet(a, b, c, d)) return true;
            }
        return false;
    }

    void add_conflicts(const std::vector<std::pair<uint32_t, uint32_t>>& pairs) {
        std::vector<uint8_t> raw(pairs.size(), 0);
        parallel_for_dynamic(pairs.size(), 16, [&](size_t begin, size_t end, unsigned) {
            Scratch s;
            for (size_t k = begin; k < end; k++) raw[k] = raw_spans_meet(segs_[pairs[k].first], segs_[pairs[k].second], s);
        }, threads_);
        for (size_t k = 0; k < pairs.size(); k++) conflicts_.push_back({pairs[k].first, pairs[k].second, raw[k] != 0});
    }

    // Gives each span in a live crossing simplification made the farthest of
    // its dropped vertices; returns the arcs it changed.
    std::vector<uint32_t> restore_crossed_spans(ArcTierCounts& counts) {
        std::vector<uint32_t> spans;
        for (const Conflict& c : conflicts_)
            if (!c.raw && alive_[c.s] && alive_[c.t]) {
                spans.push_back(c.s);
                spans.push_back(c.t);
            }
        sort_unique(spans, threads_);
        std::sort(spans.begin(), spans.end(), [&](uint32_t x, uint32_t y) {
            return segs_[x].arc != segs_[y].arc ? segs_[x].arc < segs_[y].arc : segs_[x].from < segs_[y].from;
        });
        std::vector<size_t> arc_runs;
        for (size_t k = 0; k < spans.size(); k++)
            if (k == 0 || segs_[spans[k]].arc != segs_[spans[k - 1]].arc) arc_runs.push_back(k);
        arc_runs.push_back(spans.size());
        std::vector<size_t> restored(arc_runs.size() - 1, 0);
        parallel_for_dynamic(arc_runs.size() - 1, 4, [&](size_t begin, size_t end, unsigned) {
            Scratch s;
            for (size_t run = begin; run < end; run++) {
                const uint32_t a = segs_[spans[arc_runs[run]]].arc;
                set_.read(a, s.raw);
                degrees_of(s.raw, s.pts);
                for (size_t k = arc_runs[run]; k < arc_runs[run + 1]; k++) {
                    const Segment& g = segs_[spans[k]];
                    const Farthest f = farthest_dropped(s.pts, keep_, a, g.from, g.to);
                    if (f.distance < 0) continue;
                    keep_.set(a, f.at);
                    restored[run]++;
                }
            }
        }, threads_);
        std::vector<uint32_t> dirty;
        for (size_t run = 0; run + 1 < arc_runs.size(); run++) {
            counts.repair_restored += restored[run];
            if (restored[run]) dirty.push_back(segs_[spans[arc_runs[run]]].arc);
        }
        return dirty;
    }

    // Re-lays the segments of arcs `dirty` (sorted): a segment that stays
    // (the same raw places, stored at the same points) keeps its number, one
    // that goes dies, and a new one is laid in the overlay. Returns the first
    // new number.
    uint32_t update(const std::vector<uint32_t>& dirty) {
        std::vector<std::vector<Segment>> now(dirty.size());
        parallel_for_dynamic(dirty.size(), 4, [&](size_t begin, size_t end, unsigned) {
            Scratch s;
            for (size_t d = begin; d < end; d++) add_segments(dirty[d], s, now[d]);
        }, threads_);
        const uint32_t first = static_cast<uint32_t>(segs_.size());
        for (size_t d = 0; d < dirty.size(); d++) {
            const uint32_t a = dirty[d];
            std::vector<uint32_t> was;
            if (auto it = rebuilt_.find(a); it != rebuilt_.end()) was = std::move(it->second);
            else
                for (uint32_t s = base_at_[a]; s < base_at_[a + 1]; s++) was.push_back(s);
            std::vector<uint32_t> live;
            size_t i = 0;
            for (const Segment& g : now[d]) {
                while (i < was.size() && segs_[was[i]].from < g.from) alive_[was[i++]] = 0;
                if (i < was.size() && segs_[was[i]].from == g.from && segs_[was[i]].to == g.to &&
                    segs_[was[i]].a == g.a && segs_[was[i]].b == g.b) {
                    live.push_back(was[i++]);
                    continue;
                }
                live.push_back(static_cast<uint32_t>(segs_.size()));
                segs_.push_back(g);
                alive_.push_back(1);
            }
            for (; i < was.size(); i++) alive_[was[i]] = 0;
            rebuilt_[a] = std::move(live);
        }
        if (segs_.size() >= UINT32_MAX) throw std::length_error("arc tier segments past 2^32");
        const std::vector<uint64_t> fresh = cells_from(first);
        std::vector<uint64_t> merged(overlay_cells_.size() + fresh.size());
        std::merge(overlay_cells_.begin(), overlay_cells_.end(), fresh.begin(), fresh.end(), merged.begin());
        overlay_cells_ = std::move(merged);
        return first;
    }

    // Hands the grid's storage back before count_vertices takes its own:
    // `= {}` would clear the containers and keep it all.
    void free_grid() {
        free_storage(segs_);
        free_storage(alive_);
        free_storage(base_at_);
        free_storage(base_cells_);
        free_storage(overlay_cells_);
        free_storage(rebuilt_);
        free_storage(conflicts_);
    }

    // A junction's end of an arc that ends there.
    struct ArcEnd {
        uint64_t junction;  // vertex_key
        uint32_t arc;
        bool last;
        bool operator<(const ArcEnd& o) const {
            return junction != o.junction ? junction < o.junction : arc != o.arc ? arc < o.arc : last < o.last;
        }
    };

    const AdminExactRings& store_;
    SetArcs set_;
    ArcTier tier_;  // its grid_e7 the fine grid's until coarsen()
    int32_t coarse_grid_e7_;
    bool coarse_ = false;
    unsigned threads_;
    size_t arc_count_;
    ArcKeep keep_;
    std::vector<uint8_t> grid_;  // arc_grid marks, by arc
    std::vector<uint8_t> fine_valid_;  // by ring: in the set and valid on the fine grid (coarsen)
    std::vector<int8_t> fine_winding_;  // by ring: which way it winds there (winding)
    std::vector<uint8_t> fine_folds_;   // by ring: valid there and folding over itself (folds_over)
    std::vector<uint8_t> kept_fine_;   // by ring: kept fine (keep_collapsing_rings_fine)
    std::vector<uint64_t> fine_junctions_;  // ascending
    std::vector<ArcEnd> ends_;  // every open arc's two, ascending
    ArcKeep fine_keep_;  // keep_ as the fine build left it (coarsen)
    std::vector<uint8_t> put_back_;  // by arc: put back as the fine build left it
    std::vector<uint32_t> rings_;  // the set's
    std::vector<uint32_t> user_at_, users_;  // arc a's rings: users_[user_at_[a], user_at_[a + 1])
    std::vector<uint32_t> candidates_;  // nesting_candidates
    std::vector<uint32_t> polygon_of_;  // a candidate's rings' polygon, by ring

    std::vector<Segment> segs_;
    std::vector<uint8_t> alive_;
    std::vector<uint32_t> base_at_;  // arc a's segments as first laid: [base_at_[a], base_at_[a + 1])
    std::unordered_map<uint32_t, std::vector<uint32_t>> rebuilt_;  // an arc re-laid: its live segments
    std::vector<uint64_t> base_cells_, overlay_cells_;  // cell_entry, sorted
    std::vector<Conflict> conflicts_;
};

}  // namespace

std::vector<bool> exact_rings_nest(const AdminExactRings& store, unsigned threads) {
    if (threads == 0) threads = parallel_threads();
    const size_t n = store.polygons.size();
    std::vector<uint8_t> nests(n, 1);
    const std::vector<uint32_t> by_cost = costliest_first(n, [&](size_t p) {
        const auto& poly = store.polygons[p];
        if (poly.ring_count < 2) return 0.0f;
        float vertices = 0;
        for (uint32_t r = 0; r < poly.ring_count; r++) vertices += float(store.rings[poly.first_ring + r].count);
        return vertices;
    }, threads);
    parallel_for_dynamic(n, 1, [&](size_t begin, size_t end, unsigned) {
        for (size_t k = begin; k < end; k++) {
            const auto& poly = store.polygons[by_cost[k]];
            if (poly.ring_count < 2) continue;
            auto ring = [&](uint32_t r) {
                const RingSpan& span = store.rings[poly.first_ring + r];
                return lat_lng_ring(store.vertices.data() + span.begin, span.count, false);
            };
            std::vector<std::vector<LatLng>> holes;
            for (uint32_t r = 1; r < poly.ring_count; r++) holes.push_back(ring(r));
            nests[by_cost[k]] = rings_nest(ring(0), holes);
        }
    }, threads);
    return std::vector<bool>(nests.begin(), nests.end());
}

ArcTierState build_arc_tier(const AdminArcs& arcs, const AdminExactRings& store, const ArcTier& tier,
                            const std::vector<bool>& nests_exact, unsigned threads,
                            const std::function<void(const char*)>& phase_done) {
    auto done = [&](const char* step) {
        if (phase_done) phase_done(step);
    };
    if (threads == 0) threads = parallel_threads();
    if (nests_exact.size() != store.polygons.size())
        throw std::logic_error("exact nesting marks out of step with the polygons");
    ArcTierState state;
    if (!tier.simplifies()) {
        const SetArcs set(arcs, store, tier.set);
        state.counts.kept_vertices = state.counts.stored_vertices =
            parallel_sum<size_t>(set.size(), [&](size_t a) { return size_t(set.count(static_cast<uint32_t>(a))); }, threads);
        state.counts.single_point_arcs = parallel_count(set.size(), [&](size_t a) {
            bool one = true;
            set.with(static_cast<uint32_t>(a), [&](const auto& v) {
                for (uint32_t i = 1; i < v.size() && one; i++) one = v[i] == v[0];
            });
            return one;
        }, threads);
        return state;
    }
    TierBuilder builder(arcs, store, tier, threads);
    builder.simplify();
    done("simplify");
    builder.restore_degenerate_rings(state.counts);
    done("validity");
    builder.build_grid();
    done("segment grid");
    builder.repair(nests_exact, state.counts);
    done("repair");
    builder.keep_unnested_on_e7_grid(state.counts);
    done("e7 grid");
    if (tier.coarsens()) {
        ArcTierCounts fine;
        builder.count_left(fine);
        state.counts.fine_crossings_left = fine.crossings_left;
        done("fine grid left");
        builder.coarsen(state.counts);
        done("coarsen");
        builder.build_grid();
        done("coarse segment grid");
        builder.repair(nests_exact, state.counts);
        done("coarse repair");
        builder.put_back_crossed_and_unnested(state.counts);
        done("coarse put back");
        builder.keep_unnested_on_e7_grid(state.counts);
        done("coarse e7 grid");
        builder.put_back_collapsing_rings(state.counts);
        done("coarse collapsing put back");
        builder.count_degenerate(state.counts);
        builder.count_folds(state.counts);
    }
    builder.count_left(state.counts);
    done("what is left");
    builder.count_vertices(state.counts);
    state.kept = builder.take_keep();
    state.grid = builder.take_grid();
    if (tier.coarsens()) {
        state.fine_junctions = builder.take_fine_junctions();
        state.fine_valid = builder.take_fine_valid();
        state.counts.fine_junctions = state.fine_junctions.size();
        state.counts.fine_arcs = parallel_count(state.grid.size(), [&](size_t a) {
            return (state.grid[a] & arc_grid::kFineWhole) && !(state.grid[a] & arc_grid::kE7Whole);
        }, threads);
    }
    return state;
}

void tier_vertices(const AdminArcs& arcs, const AdminExactRings& store, const ArcTier& tier,
                   const ArcTierState& state, uint32_t arc, std::vector<PackedLocation>& raw,
                   std::vector<StoredVertex>& out) {
    SetArcs(arcs, store, tier.set).read(arc, raw);
    stored_vertices(raw, state.kept, arc, tier, state.grid_of(arc), out);
}

void tier_vertices(const AdminArcs& arcs, const AdminExactRings& store, const ArcTier& tier,
                   const ArcTierState& state, uint32_t arc, std::vector<StoredVertex>& out) {
    std::vector<PackedLocation> raw;
    tier_vertices(arcs, store, tier, state, arc, raw, out);
}

void tier_ring(const AdminArcs& arcs, const AdminExactRings& store, const ArcTier& tier, const ArcTierState& state,
               uint32_t ring, std::vector<PackedLocation>& out, std::vector<PackedLocation>& raw,
               std::vector<StoredVertex>& stored) {
    const SetArcs set(arcs, store, tier.set);
    out.clear();
    for (uint32_t k = set.ring_refs()[ring]; k < set.ring_refs()[ring + 1]; k++) {
        const ArcRef ref = set.refs()[k];
        set.read(ref.arc, raw);
        stored_vertices(raw, state.kept, ref.arc, tier, state.grid_of(ref.arc), stored);
        const size_t n = stored.size();
        for (size_t i = out.empty() ? 0 : 1; i < n; i++) out.push_back(stored[ref.reversed ? n - 1 - i : i].at);
    }
}
