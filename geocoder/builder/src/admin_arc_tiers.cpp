// The arc sets' tiers (build_arc_tier): Douglas-Peucker once per arc, the
// snap, and the rounds that give a degenerate ring vertices back.
#include <algorithm>
#include <cmath>
#include <stdexcept>

#include "admin_arcs.h"
#include "exact_pip.h"
#include "parallel.h"
#include "parsed_data.h"

namespace {

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

// A coordinate on a grid of `step` e7 units, half up: floordiv(c + step / 2, step).
int32_t snap(int32_t c, int32_t step) {
    const int64_t x = int64_t(c) + step / 2;
    int64_t q = x / step;
    if (x % step != 0 && x < 0) q--;
    return static_cast<int32_t>(q * step);
}

PackedLocation snapped(PackedLocation p, int32_t step) { return {snap(p.lat_e7, step), snap(p.lon_e7, step)}; }

// tier_vertices for arc a's raw vertices `raw`.
void stored_vertices(const std::vector<PackedLocation>& raw, const ArcKeep& keep, uint32_t a, const ArcTier& tier,
                     std::vector<StoredVertex>& out) {
    out.clear();
    const uint32_t n = static_cast<uint32_t>(raw.size());
    if (!tier.simplifies()) {
        for (uint32_t i = 0; i < n; i++) out.push_back({i, raw[i]});
        return;
    }
    for (uint32_t i = 0; i < n; i++) {
        if (!keep.test(a, i)) continue;

        const PackedLocation p = snapped(raw[i], tier.grid_e7);
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
        part = {};
    }
    return out;
}

template <class T>
void sort_unique(std::vector<T>& v, unsigned threads) {
    parallel_sort(v.begin(), v.end(), std::less<T>(), threads);
    v.erase(std::unique(v.begin(), v.end()), v.end());
}

struct Scratch {
    std::vector<PackedLocation> raw;
    std::vector<LatLng> pts;
    std::vector<StoredVertex> stored;
    std::vector<PackedLocation> ring;
};

// One simplified tier of one set, built in place.
class TierBuilder {
public:
    TierBuilder(const AdminArcs& arcs, const AdminExactRings& store, const ArcTier& tier, unsigned threads)
        : store_(store), set_(arcs, store, tier.set), tier_(tier), threads_(threads), arc_count_(set_.size()),
          keep_(arc_count_, [&](size_t a) { return set_.count(static_cast<uint32_t>(a)); }) {
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

    void count_vertices(ArcTierCounts& counts) const {
        std::vector<size_t> kept(arc_count_), stored(arc_count_);
        std::vector<uint8_t> one_point(arc_count_);
        parallel_for_dynamic(arc_count_, 256, [&](size_t begin, size_t end, unsigned) {
            Scratch s;
            for (size_t i = begin; i < end; i++) {
                const uint32_t a = static_cast<uint32_t>(i);
                set_.read(a, s.raw);
                stored_vertices(s.raw, keep_, a, tier_, s.stored);
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

private:
    // Ring r as the tier stores it, into s.ring: its arcs' stored vertices
    // chained, each vertex once where its neighbour repeats it, without the
    // closing one.
    void stored_ring(uint32_t r, Scratch& s) const {
        s.ring.clear();
        for (uint32_t k = set_.ring_refs()[r]; k < set_.ring_refs()[r + 1]; k++) {
            const ArcRef ref = set_.refs()[k];
            set_.read(ref.arc, s.raw);
            stored_vertices(s.raw, keep_, ref.arc, tier_, s.stored);
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

    const AdminExactRings& store_;
    SetArcs set_;
    const ArcTier& tier_;
    unsigned threads_;
    size_t arc_count_;
    ArcKeep keep_;
    std::vector<uint32_t> rings_;  // the set's
    std::vector<uint32_t> user_at_, users_;  // arc a's rings: users_[user_at_[a], user_at_[a + 1])
};

}  // namespace

ArcTierState build_arc_tier(const AdminArcs& arcs, const AdminExactRings& store, const ArcTier& tier,
                            unsigned threads, const std::function<void(const char*)>& phase_done) {
    auto done = [&](const char* step) {
        if (phase_done) phase_done(step);
    };
    if (threads == 0) threads = parallel_threads();
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
    builder.count_vertices(state.counts);
    state.kept = builder.take_keep();
    return state;
}

void tier_vertices(const AdminArcs& arcs, const AdminExactRings& store, const ArcTier& tier,
                   const ArcTierState& state, uint32_t arc, std::vector<StoredVertex>& out) {
    std::vector<PackedLocation> raw;
    SetArcs(arcs, store, tier.set).read(arc, raw);
    stored_vertices(raw, state.kept, arc, tier, out);
}
