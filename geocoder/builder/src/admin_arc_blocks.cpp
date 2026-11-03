// The arc stage's encoding (encode_admin_arcs): every tier's arc blocks and
// every polygon's ring blocks (arc_blocks.h), the checks that abort the build,
// a continent's share of them, and the arc files of a dir.
#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include "admin_arcs.h"
#include "cell_index_io.h"
#include "parallel.h"
#include "parsed_data.h"
#include "ranked_bits.h"

uint64_t AdminArcBlocks::bytes() const {
    auto held = [](const auto& v) { return uint64_t(v.size()) * sizeof(v[0]); };
    uint64_t total = held(polygons);
    for (const ArcSetBlocks& s : sets)
        total += held(s.boxes) + held(s.keys) + held(s.planet_slots) + held(s.slots) + held(s.sidecar) +
                 held(s.ring_refs) + held(s.refs) + held(s.ring_blocks);
    for (const ArcBlocks& t : tiers) total += held(t.bytes) + held(t.at);
    return total;
}

namespace {

using arc_blocks::DecodedArc;

[[noreturn]] void check_failed(const std::string& what) { throw std::runtime_error("admin arc check failed: " + what); }

constexpr size_t set_index(ArcSet s) { return static_cast<size_t>(s); }
const char* set_name(ArcSet s) { return s == ArcSet::Full ? "full" : "minimal"; }

// The tier that stores every raw vertex of the full set's arcs.
size_t exact_tier() {
    for (size_t t = 0; t < kArcTierCount; t++)
        if (kArcTiers[t].set == ArcSet::Full && !kArcTiers[t].simplifies()) return t;
    throw std::logic_error("no exact tier of the full set");
}

// Blocks of n items back to back on every core: size(i, worker) gives item
// i's size, then place(i, out, worker) writes it at `out` and returns how
// many bytes it wrote, which must be that size. Throws past a 32-bit file
// offset, which the arc files' readers hold.
template <class Size, class Place>
void lay_out_blocks(size_t n, std::vector<uint8_t>& bytes, std::vector<uint32_t>& at, Size size, Place place,
                    const std::string& what, unsigned threads) {
    std::vector<uint64_t> sizes(n);
    parallel_for_dynamic(n, 256, [&](size_t begin, size_t end, unsigned worker) {
        for (size_t i = begin; i < end; i++) sizes[i] = size(i, worker);
    }, threads);
    const std::vector<uint64_t> offsets = parallel_offsets<uint64_t>(n, [&](size_t i) { return sizes[i]; }, threads);
    if (offsets.back() > UINT32_MAX) throw std::length_error(what + " past 4 GiB: its readers hold 32-bit offsets");
    free_storage(sizes);
    parallel_resize(at, n + 1);
    parallel_for(n + 1, [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++) at[i] = static_cast<uint32_t>(offsets[i]);
    }, threads);
    parallel_resize(bytes, offsets.back());
    parallel_for_dynamic(n, 256, [&](size_t begin, size_t end, unsigned worker) {
        for (size_t i = begin; i < end; i++)
            if (place(i, bytes.data() + at[i], worker) != at[i + 1] - at[i])
                throw std::logic_error(what + ": a block's bytes differ from its size");
    }, threads);
}

struct EncodeScratch {
    std::vector<PackedLocation> raw;
    std::vector<StoredVertex> stored;
    DecodedArc decoded;
};

// Tier t's arc blocks, each checked: it decodes to the vertices the tier
// stores, and those are raw vertices of the arc, ascending from its first,
// snapped to the tier's grid, within kChunkBoxSlackE7 of them, ending where
// the arc does.
ArcBlocks encode_tier(const AdminArcs& arcs, const AdminExactRings& store, size_t t, unsigned threads) {
    const ArcTier& tier = kArcTiers[t];
    const ArcTierState& state = arcs.tiers[t];
    const size_t n = tier.set == ArcSet::Full ? arcs.arcs.size() : arcs.minimal.arcs.size();
    const uint8_t k = arc_blocks::grid_exponent(tier.grid_e7);
    std::vector<EncodeScratch> scratch(threads);
    auto stored_of = [&](size_t a, EncodeScratch& s) {
        tier_vertices(arcs, store, tier, state, static_cast<uint32_t>(a), s.raw, s.stored);
        return s.stored.size();
    };
    auto at_of = [](const EncodeScratch& s) { return [&s](size_t i) { return s.stored[i].at; }; };
    ArcBlocks out;
    lay_out_blocks(n, out.bytes, out.at,
        [&](size_t a, unsigned worker) {
            EncodeScratch& s = scratch[worker];
            return arc_blocks::encode_arc(stored_of(a, s), at_of(s), k, nullptr);
        },
        [&](size_t a, uint8_t* to, unsigned worker) {
            EncodeScratch& s = scratch[worker];
            const size_t count = stored_of(a, s);
            const size_t bytes = arc_blocks::encode_arc(count, at_of(s), k, to);
            auto fail = [&](const std::string& what) {
                check_failed("arc " + std::to_string(a) + " of " + tier.dir + " " + what);
            };
            if (!arc_blocks::decode_arc(to, bytes, s.decoded) || s.decoded.vertices.size() != count)
                fail("doesn't decode to the " + std::to_string(count) + " vertices it stores");
            for (size_t i = 0; i < count; i++) {
                const StoredVertex& v = s.stored[i];
                if (s.decoded.vertices[i] != v.at) fail("decodes vertex " + std::to_string(i) + " elsewhere");
                if (v.raw >= s.raw.size() || (i > 0 && v.raw <= s.stored[i - 1].raw) || (i == 0 && v.raw != 0))
                    fail("stores its raw vertices out of order");
                const PackedLocation raw = s.raw[v.raw];
                if (v.at != snapped(raw, tier.grid_e7)) fail("stores a vertex no raw one snaps to");
                if (std::abs(int64_t(v.at.lat_e7) - raw.lat_e7) > kChunkBoxSlackE7 ||
                    std::abs(int64_t(v.at.lon_e7) - raw.lon_e7) > kChunkBoxSlackE7)
                    fail("stores a vertex past the chunk slack from its raw one");
            }
            if (s.stored.back().at != snapped(s.raw.back(), tier.grid_e7)) fail("doesn't end where its raw vertices do");
            return bytes;
        },
        std::string(tier.dir) + " arc_vertices", threads);
    return out;
}

// Whether each ring's refs chain: every arc starts where the one before it
// ends, the last ending where the first starts.
void check_chains_close(const AdminArcs& arcs, const AdminExactRings& store, ArcSet set, unsigned threads) {
    const bool full = set == ArcSet::Full;
    const std::vector<uint32_t>& ring_refs = full ? arcs.ring_refs : arcs.minimal.ring_refs;
    const std::vector<ArcRef>& refs = full ? arcs.refs : arcs.minimal.refs;
    // The first and last vertex of a ref, as the ring walks it.
    auto ends = [&](const ArcRef& ref) {
        auto of = [&](const auto& v) {
            const PackedLocation a = v[0], b = v[v.size() - 1];
            return ref.reversed ? std::make_pair(b, a) : std::make_pair(a, b);
        };
        if (full) return of(arc_vertices(store, arcs.arcs[ref.arc].view));
        return of(MinimalArcVertices(store, arcs, ref.arc));
    };
    parallel_for(store.rings.size(), [&](size_t begin, size_t end, unsigned) {
        for (size_t r = begin; r < end; r++) {
            const uint32_t first = ring_refs[r], n = ring_refs[r + 1] - first;
            for (uint32_t i = 0; i < n; i++)
                if (ends(refs[first + i]).second != ends(refs[first + (i + 1) % n]).first)
                    check_failed(std::string("ring ") + std::to_string(r) + "'s " + set_name(set) +
                                 " refs don't chain at ref " + std::to_string(i));
        }
    }, threads);
}

}  // namespace

std::vector<uint32_t> arc_at_slot(const ArcSetBlocks& set) {
    std::vector<uint32_t> out(set.sidecar.size(), UINT32_MAX);
    for (size_t a = 0; a < set.slots.size(); a++) {
        if (set.slots[a] >= out.size() || out[set.slots[a]] != UINT32_MAX)
            throw std::logic_error("arc slots outside their table or shared");
        out[set.slots[a]] = static_cast<uint32_t>(a);
    }
    return out;
}

void encode_ring_blocks(AdminArcBlocks& blocks, ArcSet set, unsigned threads) {
    if (threads == 0) threads = parallel_threads();
    ArcSetBlocks& s = blocks.set(set);
    if (s.slots.size() != s.arcs()) throw std::logic_error("arc slots out of step with the arcs");
    for (size_t a = 0; a < s.arcs(); a++) {
        const uint32_t slot = s.slots[a];
        const bool live = slot < s.sidecar.size() && !gc::id_alloc::is_tombstone(s.sidecar[slot]) &&
                          s.sidecar[slot].object_type == uint8_t(gc::id_alloc::ObjectType::ARC) &&
                          s.sidecar[slot].stable_id == s.keys[a];
        if (!live) check_failed(std::string(set_name(set)) + " arc " + std::to_string(a) + " has no live slot of its key");
    }
    for (const ArcRef& ref : s.refs)
        if (ref.arc >= s.arcs()) check_failed(std::string(set_name(set)) + " ref past the arcs");
    auto ring_size = [&](uint32_t r, uint8_t* out) {
        const uint32_t first = s.ring_refs[r];
        return arc_blocks::encode_ring(s.ring_refs[r + 1] - first, [&](size_t i) {
            const ArcRef ref = s.refs[first + i];
            return arc_blocks::RingRef{s.slots[ref.arc], ref.reversed, s.planet_slots[ref.arc], s.boxes[ref.arc]};
        }, out);
    };
    // A polygon's rings are all in the set or none is.
    auto in_set = [&](const AdminArcPolygon& p) {
        if (p.ring_count == 0 || !s.has_ring(p.first_ring)) return false;
        for (uint32_t k = 1; k < p.ring_count; k++)
            if (!s.has_ring(p.first_ring + k)) throw std::logic_error("a polygon partly in an arc set");
        return true;
    };
    std::vector<uint32_t> at;
    lay_out_blocks(blocks.polygons.size(), s.ring_blocks, at,
        [&](size_t p, unsigned) {
            const AdminArcPolygon& poly = blocks.polygons[p];
            size_t bytes = 0;
            if (in_set(poly))
                for (uint32_t k = 0; k < poly.ring_count; k++) bytes += ring_size(poly.first_ring + k, nullptr);
            return bytes;
        },
        [&](size_t p, uint8_t* out, unsigned) {
            const AdminArcPolygon& poly = blocks.polygons[p];
            size_t bytes = 0;
            if (in_set(poly))
                for (uint32_t k = 0; k < poly.ring_count; k++) bytes += ring_size(poly.first_ring + k, out + bytes);
            return bytes;
        },
        std::string(set_name(set)) + " admin_rings", threads);
    for (size_t p = 0; p < blocks.polygons.size(); p++) {
        const uint32_t size = at[p + 1] - at[p];
        blocks.polygons[p].blocks[set_index(set)] = size ? BlockSpan{at[p], size} : BlockSpan{};
    }
}

void check_lossless(const AdminArcs& arcs, const AdminArcBlocks& blocks, const AdminExactRings& store, unsigned threads) {
    if (threads == 0) threads = parallel_threads();
    if (blocks.polygons.size() != store.polygons.size()) throw std::logic_error("encoded arcs out of step with the store");
    const ArcBlocks& exact = blocks.tiers[exact_tier()];
    struct Scratch {
        DecodedArc decoded;
        std::vector<PackedLocation> arc, part, ring;
        std::vector<std::vector<arc_blocks::SlotRef>> rings;
    };
    // Full arc a's exact vertices, decoded from its block.
    auto full_vertices = [&](uint32_t a, Scratch& s, std::vector<PackedLocation>& out) {
        if (!arc_blocks::decode_arc(exact.data(a), exact.bytes_of(a), s.decoded) || s.decoded.k != 0)
            check_failed("the exact tier's block of arc " + std::to_string(a) + " doesn't decode at e7");
        out.swap(s.decoded.vertices);
    };
    // Appends a walk of v to `into`, the vertex it shares with the walk
    // before it once; false when it doesn't start where that one ends.
    auto chain = [](const std::vector<PackedLocation>& v, bool reversed, std::vector<PackedLocation>& into) {
        const size_t n = v.size();
        for (size_t i = 0; i < n; i++) {
            const PackedLocation p = v[reversed ? n - 1 - i : i];
            if (i == 0 && !into.empty()) {
                if (into.back() != p) return false;
                continue;
            }
            into.push_back(p);
        }
        return true;
    };
    // Minimal arc m's exact vertices: its full arcs' chained, a closed one
    // rotated to start where it does.
    auto minimal_vertices = [&](uint32_t m, Scratch& s, std::vector<PackedLocation>& out) {
        const MinimalArc& arc = arcs.minimal.arcs[m];
        out.clear();
        for (uint32_t j = 0; j < arc.part_count; j++) {
            const ArcRef part = arcs.minimal.parts[arc.first_part + j];
            full_vertices(part.arc, s, s.part);
            if (!chain(s.part, part.reversed, out)) check_failed("minimal arc " + std::to_string(m) + "'s parts don't chain");
        }
        if (!arc.closed) return;
        if (out.size() < 2 || out.front() != out.back()) check_failed("closed minimal arc " + std::to_string(m) + " doesn't close");
        const size_t places = out.size() - 1;
        std::rotate(out.begin(), out.begin() + arc.start % places, out.end() - 1);
        out.back() = out.front();
    };
    const std::vector<uint32_t> by_cost = costliest_first(store.polygons.size(), [&](size_t p) {
        float vertices = 0;
        for (uint32_t k = 0; k < store.polygons[p].ring_count; k++) vertices += float(store.rings_of(uint32_t(p))[k].count);
        return vertices;
    }, threads);
    for (ArcSet set : {ArcSet::Full, ArcSet::Minimal}) {
        const ArcSetBlocks& s = blocks.set(set);
        const std::vector<uint32_t> arc_of = arc_at_slot(s);
        std::vector<Scratch> scratch(threads);
        parallel_for_dynamic(by_cost.size(), 16, [&](size_t begin, size_t end, unsigned worker) {
            Scratch& w = scratch[worker];
            for (size_t k = begin; k < end; k++) {
                const uint32_t p = by_cost[k];
                const AdminArcPolygon& poly = blocks.polygons[p];
                const BlockSpan span = poly.blocks[set_index(set)];
                auto fail = [&](const std::string& what) {
                    check_failed("polygon " + std::to_string(p) + " in the " + set_name(set) + " set " + what);
                };
                if (poly.first_ring != store.polygons[p].first_ring || poly.ring_count != store.polygons[p].ring_count)
                    fail("numbers other rings than the store");
                if (span.size == 0) {
                    if (set == ArcSet::Full && poly.ring_count > 0) fail("has no ring block");
                    continue;
                }
                if (!arc_blocks::decode_rings(s.ring_blocks.data() + span.offset, span.size, w.rings) ||
                    w.rings.size() != poly.ring_count)
                    fail("has a ring block of other than its " + std::to_string(poly.ring_count) + " rings");
                for (uint32_t r = 0; r < poly.ring_count; r++) {
                    w.ring.clear();
                    for (const arc_blocks::SlotRef& ref : w.rings[r]) {
                        if (ref.slot >= arc_of.size() || arc_of[ref.slot] == UINT32_MAX)
                            fail("refers to slot " + std::to_string(ref.slot) + ", which holds no arc");
                        if (set == ArcSet::Full) full_vertices(arc_of[ref.slot], w, w.arc);
                        else minimal_vertices(arc_of[ref.slot], w, w.arc);
                        if (!chain(w.arc, ref.reversed, w.ring)) fail("ring " + std::to_string(r) + ": its arcs don't chain");
                    }
                    const RingSpan& raw = store.rings_of(p)[r];
                    if (!same_closed_ring(w.ring.data(), static_cast<uint32_t>(w.ring.size()),
                                          store.vertices.data() + raw.begin, raw.count))
                        fail("ring " + std::to_string(r) + " doesn't come back exactly from its exact arcs");
                }
            }
        }, threads);
    }
}

namespace {

std::string slot_line(const AdminArcSlotCounts& c, size_t slots) {
    std::ostringstream line;
    line << c.kept << " kept, " << c.recycled << " recycled, " << c.appended << " appended, " << c.tombstones
         << " tombstones (" << slots << " slots)";
    return line.str();
}

}  // namespace

AdminArcBlocks encode_admin_arcs(const AdminArcs& arcs, const AdminExactRings& store, unsigned threads) {
    if (threads == 0) threads = parallel_threads();
    if (arcs.tiers.size() != kArcTierCount) throw std::logic_error("admin arcs without their tiers");
    auto t = std::chrono::steady_clock::now();
    auto cpu = CpuTicks::now();
    AdminArcBlocks out;
    out.polygons.resize(store.polygons.size());
    for (size_t p = 0; p < store.polygons.size(); p++)
        out.polygons[p] = {store.polygons[p].first_ring, store.polygons[p].ring_count, {}};

    ArcSetBlocks& full = out.set(ArcSet::Full);
    for (const Arc& a : arcs.arcs) {
        full.boxes.push_back(a.box);
        full.keys.push_back(a.key);
        full.planet_slots.push_back(a.planet_slot);
    }
    full.sidecar = arcs.planet_sidecar;
    full.slot_counts = arcs.slot_counts;
    full.ring_refs = arcs.ring_refs;
    full.refs = arcs.refs;
    ArcSetBlocks& minimal = out.set(ArcSet::Minimal);
    for (const MinimalArc& a : arcs.minimal.arcs) {
        minimal.boxes.push_back(a.box);
        minimal.keys.push_back(a.key);
        minimal.planet_slots.push_back(a.planet_slot);
    }
    minimal.sidecar = arcs.minimal.planet_sidecar;
    minimal.slot_counts = arcs.minimal.slot_counts;
    minimal.ring_refs = arcs.minimal.ring_refs;
    minimal.refs = arcs.minimal.refs;
    for (ArcSetBlocks* s : {&full, &minimal}) s->slots = s->planet_slots;

    for (ArcSet set : {ArcSet::Full, ArcSet::Minimal}) check_chains_close(arcs, store, set, threads);
    log_phase("      Admin arcs: chains close", t, cpu);
    for (size_t k = 0; k < kArcTierCount; k++) {
        out.tiers[k] = encode_tier(arcs, store, k, threads);
        log_phase((std::string("      Admin arcs: encode ") + kArcTiers[k].dir).c_str(), t, cpu);
    }
    for (ArcSet set : {ArcSet::Full, ArcSet::Minimal}) encode_ring_blocks(out, set, threads);
    log_phase("      Admin arcs: ring blocks", t, cpu);
    check_lossless(arcs, out, store, threads);
    log_phase("      Admin arcs: lossless", t, cpu);

    // The counts, as ARC_CHECK lines (tools/o11y_report.py).
    size_t saturated = 0, polygons = 0, minimal_polygons = 0;
    for (const AdminArcPolygon& p : out.polygons) {
        saturated += p.ring_count > 0 && p.ring_count - 1 > UINT16_MAX;
        polygons += p.blocks[0].size > 0;
        minimal_polygons += p.blocks[1].size > 0;
    }
    const AdminArcCounts& c = arcs.counts;
    const MinimalArcCounts& m = arcs.minimal.counts;
    std::ostringstream line;
    line << "ARC_CHECK set=full junctions=" << c.junctions << " arcs=" << c.arcs << " closed_arcs=" << c.closed_arcs
         << " refs=" << c.refs << " rings=" << store.rings.size() << " polygons=" << polygons
         << " hash_collisions=" << c.hash_collisions << " duplicate_keys=" << c.duplicate_keys
         << " saturated_hole_counts=" << saturated << " rings_nest_raw=" << arcs.rings_nest_raw
         << " slots=" << full.sidecar.size() << " ring_block_bytes=" << full.ring_blocks.size() << "\n";
    line << "ARC_CHECK set=minimal arcs=" << m.arcs << " closed_arcs=" << m.closed_arcs << " refs=" << m.refs
         << " rings=" << m.rings << " polygons=" << minimal_polygons << " joined_junctions=" << m.joined_junctions
         << " turnback_junctions=" << m.turnback_junctions << " duplicate_keys=" << m.duplicate_keys << " slots=" << minimal.sidecar.size()
         << " ring_block_bytes=" << minimal.ring_blocks.size() << "\n";
    for (size_t k = 0; k < kArcTierCount; k++) {
        const ArcTierCounts& tc = arcs.tiers[k].counts;
        line << "ARC_CHECK tier=" << kArcTiers[k].dir << " set=" << set_name(kArcTiers[k].set)
             << " stored_vertices=" << tc.stored_vertices << " single_point_arcs=" << tc.single_point_arcs
             << " block_bytes=" << out.tiers[k].bytes.size();
        if (kArcTiers[k].simplifies())
            line << " kept_vertices=" << tc.kept_vertices << " validity_rounds=" << tc.validity_rounds
                 << " validity_restored=" << tc.validity_restored << " degenerate_rings=" << tc.degenerate_rings
                 << " crossings_simplified=" << tc.crossings_simplified << " repair_rounds=" << tc.repair_rounds
                 << " repair_restored=" << tc.repair_restored << " nesting_restored=" << tc.nesting_restored
                 << " crossings_left=" << tc.crossings_left << " unrepairable_segments=" << tc.unrepairable_segments
                 << " raw_crossings=" << tc.raw_crossings << " rings_nest_introduced=" << tc.rings_nest_introduced;
        line << "\n";
    }
    std::cerr << line.str();
    return out;
}

AdminArcBlocks subset_admin_arcs(const AdminArcBlocks& planet, const std::vector<uint32_t>& polygons, unsigned threads) {
    if (threads == 0) threads = parallel_threads();
    AdminArcBlocks out;
    out.polygons.resize(polygons.size());
    std::vector<uint32_t> planet_ring;  // each of the subset's rings' number in the planet
    for (size_t k = 0; k < polygons.size(); k++) {
        if (polygons[k] >= planet.polygons.size()) throw std::out_of_range("a continent polygon past the planet's");
        const AdminArcPolygon& p = planet.polygons[polygons[k]];
        out.polygons[k] = {static_cast<uint32_t>(planet_ring.size()), p.ring_count, {}};
        for (uint32_t r = 0; r < p.ring_count; r++) planet_ring.push_back(p.first_ring + r);
    }
    std::vector<uint32_t> kept_arcs[2];
    for (ArcSet set : {ArcSet::Full, ArcSet::Minimal}) {
        const ArcSetBlocks& from = planet.set(set);
        ArcSetBlocks& to = out.set(set);
        std::vector<std::atomic<uint64_t>> used(from.arcs() / 64 + 1);
        parallel_for(planet_ring.size(), [&](size_t begin, size_t end, unsigned) {
            for (size_t r = begin; r < end; r++)
                for (uint32_t i = from.ring_refs[planet_ring[r]]; i < from.ring_refs[planet_ring[r] + 1]; i++)
                    mark_bit(used, from.refs[i].arc);
        }, threads);
        const RankedBits kept(from.arcs(), used, threads);
        std::vector<uint32_t>& arcs = kept_arcs[set_index(set)];
        arcs = kept.ones(threads);
        for (uint32_t a : arcs) {
            to.boxes.push_back(from.boxes[a]);
            to.keys.push_back(from.keys[a]);
            to.planet_slots.push_back(from.planet_slots[a]);
        }
        to.ring_refs = parallel_offsets<uint32_t>(planet_ring.size(), [&](size_t r) {
            return from.ring_refs[planet_ring[r] + 1] - from.ring_refs[planet_ring[r]];
        }, threads);
        parallel_resize(to.refs, to.ring_refs.back());
        parallel_for(planet_ring.size(), [&](size_t begin, size_t end, unsigned) {
            for (size_t r = begin; r < end; r++) {
                const uint32_t first = from.ring_refs[planet_ring[r]];
                for (uint32_t i = 0; i < to.ring_refs[r + 1] - to.ring_refs[r]; i++) {
                    const ArcRef ref = from.refs[first + i];
                    to.refs[to.ring_refs[r] + i] = {static_cast<uint32_t>(kept.index(ref.arc)), ref.reversed};
                }
            }
        }, threads);
    }
    for (size_t t = 0; t < kArcTierCount; t++) {
        const std::vector<uint32_t>& arcs = kept_arcs[set_index(kArcTiers[t].set)];
        const ArcBlocks& from = planet.tiers[t];
        lay_out_blocks(arcs.size(), out.tiers[t].bytes, out.tiers[t].at,
            [&](size_t i, unsigned) { return from.bytes_of(arcs[i]); },
            [&](size_t i, uint8_t* to, unsigned) {
                std::copy_n(from.data(arcs[i]), from.bytes_of(arcs[i]), to);
                return from.bytes_of(arcs[i]);
            },
            std::string(kArcTiers[t].dir) + " arc_vertices", threads);
    }
    return out;
}

void assign_region_arc_slots(AdminArcBlocks& blocks, const std::string& prev_region_dir, const std::string& region,
                             unsigned threads) {
    std::ostringstream line;
    line << "  " << region << " arcs:";
    for (ArcSet set : {ArcSet::Full, ArcSet::Minimal}) {
        ArcSetBlocks& s = blocks.set(set);
        ArcSlots slots = allocate_arc_slots(s.keys, prev_region_dir.empty() ? "" : arc_sidecar_path(prev_region_dir, set));
        s.slots = std::move(slots.slots);
        s.sidecar = std::move(slots.sidecar);
        s.slot_counts = slots.counts;
        encode_ring_blocks(blocks, set, threads);
        line << " " << set_name(set) << " " << s.arcs() << " arcs, slots " << slot_line(s.slot_counts, s.sidecar.size())
             << ";";
    }
    std::cerr << line.str() << "\n";
}

void write_arc_files(const std::string& dir, const ArcSetBlocks& set, const ArcBlocks& tier) {
    if (tier.size() != set.arcs()) throw std::logic_error("arc blocks out of step with their set");
    const std::vector<uint32_t> arc_of = arc_at_slot(set);
    std::vector<uint32_t> index(arc_of.size());
    const std::string path = dir + "/arc_vertices.bin";
    std::ofstream f(path, std::ios::binary);
    uint64_t at = 0;
    for (size_t slot = 0; slot < arc_of.size(); slot++) {
        index[slot] = static_cast<uint32_t>(at);
        if (arc_of[slot] == UINT32_MAX) continue;
        const size_t n = tier.bytes_of(arc_of[slot]);
        f.write(reinterpret_cast<const char*>(tier.data(arc_of[slot])), static_cast<std::streamsize>(n));
        at += n;
        if (at > UINT32_MAX) throw std::length_error(path + " past 4 GiB: arc_index.bin holds 32-bit offsets");
    }
    f.flush();
    if (!f) throw std::runtime_error("failed to write " + path);
    write_binary_file(dir + "/arc_index.bin", reinterpret_cast<const char*>(index.data()), index.size() * sizeof(uint32_t));
}

size_t write_ring_files(const std::string& dir, const AdminArcBlocks& blocks, ArcSet set,
                        std::vector<AdminPolygon> records, const std::vector<uint32_t>& polygon_of) {
    if (records.size() != polygon_of.size()) throw std::logic_error("admin records out of step with their polygons");
    const ArcSetBlocks& s = blocks.set(set);
    const std::string path = dir + "/admin_rings.bin";
    std::ofstream f(path, std::ios::binary);
    uint64_t at = 0;
    for (size_t slot = 0; slot < records.size(); slot++) {
        if (polygon_of[slot] == NO_DATA) continue;
        const AdminArcPolygon& p = blocks.polygons[polygon_of[slot]];
        const BlockSpan span = p.blocks[set_index(set)];
        AdminPolygon& r = records[slot];
        r.vertex_offset = static_cast<uint32_t>(at);
        r.vertex_count = span.size;
        r.hole_count = static_cast<uint16_t>(std::min<uint32_t>(p.ring_count ? p.ring_count - 1 : 0, UINT16_MAX));
        f.write(reinterpret_cast<const char*>(s.ring_blocks.data() + span.offset), span.size);
        at += span.size;
        if (at > UINT32_MAX) throw std::length_error(path + " past 4 GiB: admin_polygons.bin holds 32-bit offsets");
    }
    f.flush();
    if (!f) throw std::runtime_error("failed to write " + path);
    write_binary_file(dir + "/admin_polygons.bin", reinterpret_cast<const char*>(records.data()),
                      records.size() * sizeof(AdminPolygon));
    return at;
}
