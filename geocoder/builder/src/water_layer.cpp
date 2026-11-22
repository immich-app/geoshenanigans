// The water layer's build steps and its dirs (water_layer.h).
#include "water_layer.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <fstream>
#include <iostream>
#include <map>
#include <numeric>
#include <set>
#include <sstream>
#include <stdexcept>

#include "cell_index.h"
#include "cell_index_io.h"
#include "parallel.h"
#include "ranked_bits.h"
#include "ring_assembly.h"
#include "string_files.h"
#include "string_subset.h"
#include "water_names_s2.h"
#include "water_split.h"

namespace water {

namespace {

using gc::id_alloc::IdAllocator;
using gc::id_alloc::ObjectType;
using gc::id_alloc::SidecarSlot;

constexpr double kKm2PerDeg2AtEquator = 111.32 * 111.32;

// An exact ring's area in km², by the cosine of its middle latitude.
double ring_km2(const PackedLocation* v, uint32_t n) {
    const __int128 a2 = exact_ring_area2(v, n);
    const double deg2 = static_cast<double>(a2 < 0 ? -a2 : a2) * 0.5e-14;
    int32_t lo = INT32_MAX, hi = INT32_MIN;
    for (uint32_t i = 0; i < n; i++) {
        lo = std::min(lo, v[i].lat_e7);
        hi = std::max(hi, v[i].lat_e7);
    }
    const double mid = (double(lo) + double(hi)) / 2e7 * 3.14159265358979323846 / 180.0;
    return deg2 * kKm2PerDeg2AtEquator * std::cos(mid);
}

// A polygon's area in km², its holes taken off.
double polygon_km2(const AdminExactRings& rings, uint32_t p) {
    const RingSpan* r = rings.rings_of(p);
    double km2 = ring_km2(rings.vertices.data() + r[0].begin, r[0].count);
    for (uint32_t k = 1; k < rings.polygons[p].ring_count; k++)
        km2 -= ring_km2(rings.vertices.data() + r[k].begin, r[k].count);
    return std::max(0.0, km2);
}

std::string read_text(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) return "";
    std::ostringstream s;
    s << f.rdbuf();
    return s.str();
}

void write_text(const std::string& path, const std::string& text) {
    std::ofstream f(path, std::ios::binary);
    f << text;
    f.flush();
    if (!f) throw std::runtime_error("failed to write " + path);
}

// admin_polygons.fallback.osm_ids: u32 magic, u32 version, u32 slot count,
// then each slot's fallback key (u64), kNoFallbackKey for a slot with none.
constexpr uint32_t kFallbackSidecarMagic = 0xFA11BA7E;
constexpr uint32_t kFallbackSidecarVersion = 1;

void write_fallback_sidecar(const std::string& path, const std::vector<uint64_t>& keys) {
    std::ofstream f(path, std::ios::binary);
    const uint32_t head[3] = {kFallbackSidecarMagic, kFallbackSidecarVersion, static_cast<uint32_t>(keys.size())};
    f.write(reinterpret_cast<const char*>(head), sizeof head);
    f.write(reinterpret_cast<const char*>(keys.data()), static_cast<std::streamsize>(keys.size() * sizeof(uint64_t)));
    f.flush();
    if (!f) throw std::runtime_error("failed to write " + path);
}

// The fallback keys of a sidecar of `slots` slots; none (every slot without
// one) for a missing file. One of another slot count, or cut short, is refused.
std::vector<uint64_t> read_fallback_sidecar(const std::string& path, size_t slots) {
    std::ifstream f(path, std::ios::binary);
    if (!f) return std::vector<uint64_t>(slots, gc::id_alloc::kNoFallbackKey);
    uint32_t head[3];
    std::vector<uint64_t> keys;
    if (f.read(reinterpret_cast<char*>(head), sizeof head) && head[0] == kFallbackSidecarMagic &&
        head[1] == kFallbackSidecarVersion && head[2] == slots) {
        keys.resize(slots);
        f.read(reinterpret_cast<char*>(keys.data()), static_cast<std::streamsize>(slots * sizeof(uint64_t)));
        if (f && f.peek() == std::char_traits<char>::eof()) return keys;
    }
    throw std::runtime_error(path + " is no fallback sidecar of " + std::to_string(slots) + " slots");
}

// Slots for `ids` (polygons of the layer, ascending) against the previous
// build's sidecars at `prev` (none when empty), written to `dir`; each id's
// slot, and the slot count.
struct PolygonSlots {
    std::vector<uint32_t> slot_of;  // parallel to the layer's records, NO_DATA for a polygon not here
    uint32_t count = 0;
    size_t kept = 0, by_fallback = 0;
};

PolygonSlots polygon_slots(const Layer& layer, const std::vector<uint32_t>& ids, const std::string& prev_dir,
                           const std::string& dir) {
    IdAllocator alloc;
    const bool had = !prev_dir.empty() && alloc.load_previous(prev_dir + "/admin_polygons.osm_ids");
    const std::vector<uint64_t> prev_fallback =
        had ? read_fallback_sidecar(prev_dir + "/admin_polygons.fallback.osm_ids", alloc.total_slots())
            : std::vector<uint64_t>();
    std::vector<gc::id_alloc::FallbackPass> passes;
    passes.push_back({[&](size_t k) { return layer.fallbacks[ids[k]]; },
                      [&](uint32_t s) { return s < prev_fallback.size() ? prev_fallback[s] : gc::id_alloc::kNoFallbackKey; }});
    const uint32_t previous_slots = alloc.total_slots();
    const std::vector<uint32_t> slots = alloc.allocate_all(ids.size(), [&](size_t k) {
        const uint64_t key = layer.identities[ids[k]];
        return gc::id_alloc::SlotIdentity{static_cast<ObjectType>(key >> 56), key & 0x00FFFFFFFFFFFFFFull, 0};
    }, gc::id_alloc::kRecordsPerMatchPass, passes);
    PolygonSlots out;
    for (size_t m : alloc.fallback_matches()) out.by_fallback += m;
    alloc.finalize();
    IdAllocator::write_sidecar(dir + "/admin_polygons.osm_ids", alloc.slots());
    out.count = alloc.total_slots();
    out.slot_of.assign(layer.size(), NO_DATA);
    for (size_t k = 0; k < ids.size(); k++) {
        out.slot_of[ids[k]] = slots[k];
        out.kept += slots[k] < previous_slots;
    }
    std::vector<uint64_t> at_slot(out.count, gc::id_alloc::kNoFallbackKey);
    for (size_t k = 0; k < ids.size(); k++) at_slot[slots[k]] = layer.fallbacks[ids[k]];
    write_fallback_sidecar(dir + "/admin_polygons.fallback.osm_ids", at_slot);
    return out;
}

const std::vector<uint64_t> kNone;

}  // namespace

void index_collected(Collected& c) {
    std::sort(c.relations.begin(), c.relations.end(),
              [](const CollectedArea& a, const CollectedArea& b) { return a.id < b.id; });
    std::sort(c.waterways.begin(), c.waterways.end(),
              [](const CollectedWaterway& a, const CollectedWaterway& b) { return a.id < b.id; });
    c.waterway_of_way.clear();
    for (uint32_t i = 0; i < c.waterways.size(); i++)
        for (int64_t way : c.waterways[i].ways) c.waterway_of_way.emplace(way, i);
}

bool line_key(const Collected& c, int64_t way, const char* name, const char* name_en, uint32_t way_qid, LineIn& line) {
    if (name && !*name) name = nullptr;
    if (name_en && !*name_en) name_en = nullptr;
    line.way = way;
    auto it = c.waterway_of_way.find(way);
    if (it != c.waterway_of_way.end()) {
        const CollectedWaterway& w = c.waterways[it->second];
        line.key = "r" + std::to_string(w.id);
        line.local = w.local_name;
        line.display = w.name;
        line.qid = w.qid;
        return true;
    }
    if (!name && !name_en) return false;
    line.local = name ? name : name_en;
    line.key = "n" + line.local;
    line.display = name_en ? name_en : name;
    line.qid = way_qid;
    return true;
}

Build assemble(Collected& c, unsigned threads) {
    if (threads == 0) threads = parallel_threads();
    Build b;

    // The objects: closed ways by id, then relations by id.
    std::sort(c.closed_ways.begin(), c.closed_ways.end(),
              [](const ClosedWayArea& x, const ClosedWayArea& y) { return x.way < y.way; });
    c.closed_ways.erase(std::unique(c.closed_ways.begin(), c.closed_ways.end(),
                                    [](const ClosedWayArea& x, const ClosedWayArea& y) { return x.way == y.way; }),
                        c.closed_ways.end());

    // Each relation's polygons, assembled on every core.
    struct Assembled {
        std::vector<NestedPolygon<PackedLocation>> polygons;
        std::vector<NestedPolygon<WayRun>> runs;
        bool missing = false;
    };
    std::vector<Assembled> assembled(c.relations.size());
    std::atomic<size_t> next{0};
    parallel_for_each(threads, [&](size_t, unsigned) {
        for (size_t i; (i = next.fetch_add(1)) < c.relations.size();) {
            const CollectedArea& rel = c.relations[i];
            Assembled& out = assembled[i];
            for (const auto& [way, role] : rel.members)
                if (!c.member_geometries.find(way)) {
                    out.missing = true;
                    break;
                }
            if (out.missing) continue;
            AreaRings rings = assemble_area_rings(rel.members, c.member_geometries);
            const std::vector<int> around = enclosing_rings(rings.rings);
            const std::vector<int> depth = ring_depths(around);
            out.polygons = nest_rings(std::move(rings.rings), around, depth);
            out.runs = nest_rings(std::move(rings.runs), around, depth);
            for (size_t k = 0; k < out.polygons.size(); k++) {
                canonicalize_ring_rotation(out.polygons[k].outer, out.runs[k].outer);
                for (size_t h = 0; h < out.polygons[k].holes.size(); h++)
                    canonicalize_ring_rotation(out.polygons[k].holes[h], out.runs[k].holes[h]);
            }
        }
    }, threads);
    free_storage(c.member_geometries.coords);
    free_storage(c.member_geometries.ways);

    auto add_object = [&](ObjectType type, int64_t id, ObjectTags&& tags) {
        Object o;
        o.type = type;
        o.id = id;
        o.tags = std::move(tags);
        b.objects.push_back(std::move(o));
        return static_cast<uint32_t>(b.objects.size() - 1);
    };
    auto add_polygon = [&](uint32_t object, uint32_t rank, const std::vector<PackedLocation>& outer,
                           const std::vector<std::vector<PackedLocation>>& holes,
                           const std::vector<std::vector<WayRun>>& runs) {
        b.rings.append(outer, holes, runs);
        b.object_of.push_back(object);
        b.outer_rank.push_back(rank);
    };
    for (ClosedWayArea& w : c.closed_ways) {
        if (w.vertices.size() < 4) continue;
        canonicalize_ring_rotation(w.vertices);
        const uint32_t object = add_object(ObjectType::OSM_WAY, w.way, std::move(w.tags));
        add_polygon(object, 0, w.vertices, {}, {{{w.way, static_cast<uint32_t>(w.vertices.size() - 1)}}});
        std::vector<PackedLocation>().swap(w.vertices);
    }
    free_storage(c.closed_ways);
    for (size_t i = 0; i < c.relations.size(); i++) {
        Assembled& a = assembled[i];
        if (a.missing) {
            b.relations_missing_ways++;
            continue;
        }
        uint32_t object = UINT32_MAX, rank = 0;
        for (size_t k = 0; k < a.polygons.size(); k++) {
            NestedPolygon<PackedLocation>& p = a.polygons[k];
            if (p.outer.size() < 4) continue;
            if (object == UINT32_MAX) object = add_object(ObjectType::OSM_RELATION, c.relations[i].id, std::move(c.relations[i].tags));
            std::vector<std::vector<WayRun>> runs;
            runs.push_back(std::move(a.runs[k].outer));
            for (auto& h : a.runs[k].holes) runs.push_back(std::move(h));
            add_polygon(object, rank++, p.outer, p.holes, runs);
        }
        if (object == UINT32_MAX) b.relations_without_rings++;
        a = {};
    }
    free_storage(assembled);
    free_storage(c.relations);

    // The candidate lines, by way, their keys and names as sorted indices.
    std::sort(c.lines.begin(), c.lines.end(), [](const LineIn& x, const LineIn& y) { return x.way < y.way; });
    c.lines.erase(std::unique(c.lines.begin(), c.lines.end(), [](const LineIn& x, const LineIn& y) { return x.way == y.way; }),
                  c.lines.end());
    {
        std::map<std::string, std::string> keys;
        std::set<std::string> displays;
        for (const LineIn& l : c.lines) {
            keys.emplace(l.key, l.local);
            displays.insert(l.display);
        }
        for (auto& [key, local] : keys) {
            b.lines.keys.push_back(key);
            b.lines.key_local.push_back(local);
        }
        b.lines.displays.assign(displays.begin(), displays.end());
    }
    auto index_of = [](const std::vector<std::string>& sorted, const std::string& s) {
        return static_cast<uint32_t>(std::lower_bound(sorted.begin(), sorted.end(), s) - sorted.begin());
    };
    for (LineIn& l : c.lines) {
        if (l.vertices.size() < 2) continue;
        b.lines.lines.push_back({l.way, index_of(b.lines.keys, l.key), index_of(b.lines.displays, l.display), l.qid,
                                 b.lines.vertices.size(), static_cast<uint32_t>(l.vertices.size())});
        b.lines.vertices.insert(b.lines.vertices.end(), l.vertices.begin(), l.vertices.end());
        std::vector<PackedLocation>().swap(l.vertices);
    }
    free_storage(c.lines);
    return b;
}

size_t apply_names(Build& b, const std::vector<NameChoice>& choices) {
    size_t named = 0;
    std::vector<uint8_t> keep(b.objects.size(), 0);
    for (size_t o = 0; o < b.objects.size(); o++) {
        Object& obj = b.objects[o];
        if (o < choices.size() && choices[o].named()) {
            obj.name_key = choices[o].key;
            // The key's wikidata item: a line of it inside carries it.
            if (obj.tags.name.empty()) {
                obj.tags.name = b.lines.displays[choices[o].display];
                obj.tags.local_name = b.lines.key_local[choices[o].key];
                named++;
            }
        }
        keep[o] = !obj.tags.name.empty();
    }
    // The key's wikidata items, from its lines (the lowest one).
    {
        std::vector<uint32_t> key_qid(b.lines.keys.size(), 0);
        for (const NamingLine& l : b.lines.lines)
            if (l.qid && (!key_qid[l.key] || l.qid < key_qid[l.key])) key_qid[l.key] = l.qid;
        for (Object& obj : b.objects)
            if (obj.name_key != UINT32_MAX) obj.key_qid = key_qid[obj.name_key];
    }

    // Compacts the polygons of the kept objects in place: their rings, way
    // runs and vertices move down, in order.
    AdminExactRings& r = b.rings;
    std::vector<uint32_t> new_object(b.objects.size(), UINT32_MAX);
    size_t objects = 0;
    for (size_t o = 0; o < b.objects.size(); o++) {
        if (!keep[o]) continue;
        new_object[o] = static_cast<uint32_t>(objects);
        if (objects != o) b.objects[objects] = std::move(b.objects[o]);
        objects++;
    }
    b.objects.resize(objects);
    size_t polygons = 0, rings = 0, vertices = 0, runs = 0;
    const bool with_runs = !r.first_run.empty();
    for (size_t p = 0; p < r.polygons.size(); p++) {
        const uint32_t o = new_object[b.object_of[p]];
        if (o == UINT32_MAX) continue;
        const AdminExactRings::Polygon poly = r.polygons[p];
        r.polygons[polygons] = {static_cast<uint32_t>(rings), poly.ring_count};
        for (uint32_t k = 0; k < poly.ring_count; k++) {
            const uint32_t ring = poly.first_ring + k;
            const RingSpan span = r.rings[ring];
            std::copy(r.vertices.begin() + span.begin, r.vertices.begin() + span.begin + span.count,
                      r.vertices.begin() + static_cast<std::ptrdiff_t>(vertices));
            if (with_runs) {
                const auto [first, last] = r.runs_of_ring(ring);
                const size_t n = static_cast<size_t>(last - first);
                const size_t from = static_cast<size_t>(first - r.runs.data());
                std::copy(r.runs.begin() + static_cast<std::ptrdiff_t>(from),
                          r.runs.begin() + static_cast<std::ptrdiff_t>(from + n),
                          r.runs.begin() + static_cast<std::ptrdiff_t>(runs));
                r.first_run[rings] = static_cast<uint32_t>(runs);
                runs += n;
            }
            r.rings[rings] = {static_cast<uint32_t>(vertices), span.count};
            vertices += span.count;
            rings++;
        }
        b.object_of[polygons] = o;
        b.outer_rank[polygons] = b.outer_rank[p];
        if (!b.piece_key.empty()) b.piece_key[polygons] = b.piece_key[p];
        polygons++;
    }
    r.polygons.resize(polygons);
    r.rings.resize(rings);
    r.vertices.resize(vertices);
    if (with_runs) {
        r.runs.resize(runs);
        r.first_run.resize(rings);
    }
    b.object_of.resize(polygons);
    b.outer_rank.resize(polygons);
    if (!b.piece_key.empty()) b.piece_key.resize(polygons);
    r.polygons.shrink_to_fit();
    r.rings.shrink_to_fit();
    r.vertices.shrink_to_fit();
    r.runs.shrink_to_fit();
    r.first_run.shrink_to_fit();

    for (Object& obj : b.objects) obj.area_deg2 = obj.km2 = 0;
    for (size_t p = 0; p < polygons; p++) {
        Object& obj = b.objects[b.object_of[p]];
        obj.area_deg2 += r.area(static_cast<uint32_t>(p));
        obj.km2 += polygon_km2(r, static_cast<uint32_t>(p));
    }
    return named;
}

void split_polygons(Build& b, unsigned threads) {
    if (threads == 0) threads = parallel_threads();
    AdminExactRings& r = b.rings;
    const size_t n = r.polygons.size();
    b.piece_key.assign(n, 0);
    std::vector<uint32_t> heavy;
    for (uint32_t p = 0; p < n; p++)
        if (r.polygons[p].ring_count > kSplitMaxHoles + 1) heavy.push_back(p);
    if (heavy.empty()) return;
    std::vector<uint32_t> heavy_rings;
    for (uint32_t p : heavy) heavy_rings.push_back(r.polygons[p].ring_count);
    std::vector<uint32_t> heavy_object;
    for (uint32_t p : heavy) heavy_object.push_back(b.object_of[p]);
    // Each heavy polygon's pieces, on every core, the costliest first.
    std::vector<std::vector<SplitPolygon>> pieces(heavy.size());
    std::vector<size_t> lines(heavy.size(), 0);
    std::vector<SplitRefusal> refusals(heavy.size(), SplitRefusal::None);
    const std::vector<uint32_t> order = costliest_first(heavy.size(), [&](size_t k) {
        return static_cast<float>(r.occurrences(heavy[k]));
    }, threads);
    parallel_for_dynamic(order.size(), 1, [&](size_t begin, size_t end, unsigned) {
        for (size_t i = begin; i < end; i++) {
            const size_t k = order[i];
            const uint32_t p = heavy[k];
            SplitPolygon poly;
            bool runs_whole = true;
            for (uint32_t q = 0; q < r.polygons[p].ring_count && runs_whole; q++) {
                const uint32_t ring = r.polygons[p].first_ring + q;
                const RingSpan span = r.rings[ring];
                SplitRing sr;
                sr.v.assign(r.vertices.begin() + span.begin, r.vertices.begin() + span.begin + span.count - 1);
                const auto [first, last] = r.runs_of_ring(ring);
                for (const WayRun* run = first; run != last; ++run) {
                    const int64_t way = run->way;  // by value: WayRun packs it to 4 bytes
                    sr.way.insert(sr.way.end(), run->edges, way);
                }
                runs_whole = sr.way.size() == sr.v.size();
                if (q == 0) poly.outer = std::move(sr);
                else poly.holes.push_back(std::move(sr));
            }
            if (!runs_whole) continue;
            pieces[k] = split_polygon(poly, kSplitMaxHoles, &lines[k], &refusals[k]);
            if (pieces[k].size() == 1) pieces[k].clear();
        }
    }, threads);

    // The store again: each cut polygon's pieces in its place.
    AdminExactRings out;
    std::vector<uint32_t> object_of, outer_rank;
    std::vector<uint64_t> piece_key;
    size_t cut = 0, made = 0, cut_lines = 0, kept_whole = 0;
    auto to_ring = [](const SplitRing& s, std::vector<PackedLocation>& v, std::vector<WayRun>& runs) {
        v = s.v;
        v.push_back(s.v.front());
        runs.clear();
        for (size_t i = 0; i < s.way.size(); i++) {
            if (!runs.empty() && runs.back().way == s.way[i]) runs.back().edges++;
            else runs.push_back({s.way[i], 1});
        }
        canonicalize_ring_rotation(v, runs);
    };
    size_t k = 0;
    for (uint32_t p = 0; p < n; p++) {
        const bool is_heavy = k < heavy.size() && heavy[k] == p;
        const bool split = is_heavy && !pieces[k].empty();
        if (split) {
            cut++;
            made += pieces[k].size();
            cut_lines += lines[k];
            for (const SplitPolygon& piece : pieces[k]) {
                std::vector<PackedLocation> outer;
                std::vector<std::vector<PackedLocation>> holes(piece.holes.size());
                std::vector<std::vector<WayRun>> runs(1 + piece.holes.size());
                to_ring(piece.outer, outer, runs[0]);
                for (size_t h = 0; h < piece.holes.size(); h++) to_ring(piece.holes[h], holes[h], runs[h + 1]);
                out.append(outer, holes, runs);
                object_of.push_back(b.object_of[p]);
                outer_rank.push_back(b.outer_rank[p]);
                piece_key.push_back(piece.key);
            }
            std::vector<SplitPolygon>().swap(pieces[k]);
        } else {
            kept_whole += is_heavy;
            const AdminExactRings::Polygon poly = r.polygons[p];
            std::vector<PackedLocation> outer;
            std::vector<std::vector<PackedLocation>> holes;
            std::vector<std::vector<WayRun>> runs;
            for (uint32_t q = 0; q < poly.ring_count; q++) {
                const uint32_t ring = poly.first_ring + q;
                const RingSpan span = r.rings[ring];
                std::vector<PackedLocation> v(r.vertices.begin() + span.begin, r.vertices.begin() + span.begin + span.count);
                const auto [first, last] = r.runs_of_ring(ring);
                runs.emplace_back(first, last);
                if (q == 0) outer = std::move(v);
                else holes.push_back(std::move(v));
            }
            out.append(outer, holes, runs);
            object_of.push_back(b.object_of[p]);
            outer_rank.push_back(b.outer_rank[p]);
            piece_key.push_back(0);
        }
        if (is_heavy) k++;
    }
    b.rings = std::move(out);
    b.object_of = std::move(object_of);
    b.outer_rank = std::move(outer_rank);
    b.piece_key = std::move(piece_key);
    std::ostringstream whole;
    for (size_t h = 0; h < heavy.size(); h++)
        if (refusals[h] != SplitRefusal::None)
            whole << "  Water split: " << (b.objects[heavy_object[h]].type == ObjectType::OSM_WAY ? "w" : "r")
                  << b.objects[heavy_object[h]].id << " (" << b.objects[heavy_object[h]].tags.name << ", "
                  << heavy_rings[h] - 1 << " holes) kept whole: " << refusal_name(refusals[h]) << "\n";
    std::cerr << whole.str() << "  Water split: " << heavy.size() << " polygons of more than " << kSplitMaxHoles
              << " holes; " << cut << " cut along " << cut_lines << " lines into " << made << " pieces, " << kept_whole
              << " kept whole; " << b.rings.polygons.size() << " polygons" << std::endl;
}

std::shared_ptr<TypeTable> type_table(const Build& b, const std::string& prev_planet_dir) {
    std::set<std::string> values;
    for (const Object& o : b.objects)
        if (!o.tags.verbatim.empty()) values.insert(o.tags.verbatim);
    std::vector<MetaType> previous;
    if (!prev_planet_dir.empty())
        previous = parse_meta_json(read_text(prev_planet_dir + "/" + kWaterArcTiers[1].dir + "/water_meta.json"));
    auto table = std::make_shared<TypeTable>();
    table->assign(values, previous);
    return table;
}

namespace {

// Union-find over the objects.
struct Groups {
    explicit Groups(size_t n) : parent(n) { std::iota(parent.begin(), parent.end(), 0u); }
    uint32_t find(uint32_t x) {
        while (parent[x] != x) x = parent[x] = parent[parent[x]];
        return x;
    }
    void join(uint32_t a, uint32_t b) {
        a = find(a);
        b = find(b);
        if (a != b) parent[std::max(a, b)] = std::min(a, b);
    }
    std::vector<uint32_t> parent;
};

// The live slot identities of a sidecar; none for a missing one.
std::set<uint64_t> sidecar_identities(const std::string& path) {
    std::set<uint64_t> out;
    IdAllocator alloc;
    if (!alloc.load_previous(path)) return out;
    for (const SidecarSlot& s : alloc.slots())
        if (gc::id_alloc::is_reclaimable(s))
            out.insert(gc::id_alloc::make_key(static_cast<ObjectType>(s.object_type), s.stable_id));
    return out;
}

// Each polygon's slot identity and fallback key.
void identities_of(const Build& b, std::vector<uint64_t>& ids, std::vector<uint64_t>& fallbacks) {
    const size_t n = b.rings.polygons.size();
    ids.resize(n);
    fallbacks.resize(n);
    parallel_for(n, [&](size_t begin, size_t end, unsigned) {
        for (size_t p = begin; p < end; p++) {
            const Object& o = b.objects[b.object_of[p]];
            if (o.type == ObjectType::OSM_WAY) {
                ids[p] = way_identity(o.id);
                fallbacks[p] = gc::id_alloc::kNoFallbackKey;
                continue;
            }
            const RingSpan& outer = b.rings.rings_of(static_cast<uint32_t>(p))[0];
            uint64_t smallest = UINT64_MAX;
            for (uint32_t i = 0; i < outer.count; i++)
                smallest = std::min(smallest, vertex_key(b.rings.vertices[outer.begin + i]));
            ids[p] = relation_ring_identity(o.id, smallest);
            fallbacks[p] = piece_fallback(relation_ring_fallback(o.id, b.outer_rank[p]),
                                          b.piece_key.empty() ? 0 : b.piece_key[p]);
        }
    });
}

}  // namespace

std::vector<uint8_t> major_polygons(const Build& b, const AdminArcs& arcs, const std::string& prev_planet_dir) {
    const size_t n_objects = b.objects.size(), n_polygons = b.rings.polygons.size();
    std::vector<uint64_t> ids, fallbacks;
    identities_of(b, ids, fallbacks);
    const std::set<uint64_t> previous =
        prev_planet_dir.empty() ? std::set<uint64_t>()
                                : sidecar_identities(prev_planet_dir + "/water/major/simplified/admin_polygons.osm_ids");
    std::vector<uint8_t> was_major(n_objects, 0);
    for (size_t p = 0; p < n_polygons; p++)
        if (previous.count(ids[p])) was_major[b.object_of[p]] = 1;

    // The rivers: river objects joined by key, by wikidata item, and by
    // local name across a shared arc.
    Groups groups(n_objects);
    std::map<uint32_t, uint32_t> by_key, by_qid;
    for (uint32_t o = 0; o < n_objects; o++) {
        const Object& obj = b.objects[o];
        if (!obj.tags.river_area) continue;
        if (obj.name_key != UINT32_MAX) {
            auto [it, fresh] = by_key.emplace(obj.name_key, o);
            if (!fresh) groups.join(it->second, o);
        }
        for (uint32_t q : {obj.tags.qid, obj.key_qid}) {
            if (!q) continue;
            auto [it, fresh] = by_qid.emplace(q, o);
            if (!fresh) groups.join(it->second, o);
        }
    }
    {
        // Ring -> object, then every arc's river objects of one local name.
        std::vector<uint32_t> ring_object(b.rings.rings.size(), UINT32_MAX);
        for (size_t p = 0; p < n_polygons; p++)
            for (uint32_t k = 0; k < b.rings.polygons[p].ring_count; k++)
                ring_object[b.rings.polygons[p].first_ring + k] = b.object_of[p];
        std::vector<std::pair<uint32_t, uint32_t>> arc_objects;  // (arc, object)
        for (uint32_t r = 0; r < b.rings.rings.size(); r++) {
            const uint32_t o = ring_object[r];
            if (o == UINT32_MAX || !b.objects[o].tags.river_area) continue;
            for (uint32_t k = arcs.ring_refs[r]; k < arcs.ring_refs[r + 1]; k++) arc_objects.push_back({arcs.refs[k].arc, o});
        }
        std::sort(arc_objects.begin(), arc_objects.end());
        for (size_t i = 0; i < arc_objects.size();) {
            size_t end = i + 1;
            while (end < arc_objects.size() && arc_objects[end].first == arc_objects[i].first) end++;
            for (size_t a = i; a < end; a++)
                for (size_t z = a + 1; z < end; z++) {
                    const Object &x = b.objects[arc_objects[a].second], &y = b.objects[arc_objects[z].second];
                    if (x.tags.local_name == y.tags.local_name) groups.join(arc_objects[a].second, arc_objects[z].second);
                }
            i = end;
        }
    }
    std::vector<double> river_km2(n_objects, 0);
    std::vector<uint8_t> river_wikidata(n_objects, 0), river_was_major(n_objects, 0);
    for (uint32_t o = 0; o < n_objects; o++) {
        const Object& obj = b.objects[o];
        if (!obj.tags.river_area) continue;
        const uint32_t g = groups.find(o);
        river_km2[g] += obj.km2;
        river_wikidata[g] |= obj.tags.qid || obj.key_qid;
        river_was_major[g] |= was_major[o];
    }
    std::vector<uint8_t> object_major(n_objects, 0);
    size_t objects = 0, rivers = 0, kept_by_margin = 0;
    std::set<uint32_t> river_groups;
    for (uint32_t o = 0; o < n_objects; o++) {
        const Object& obj = b.objects[o];
        auto keeps = [&](bool margin) {
            if (water_major_keeps(obj.km2, false, false, margin && was_major[o])) return true;
            if (!obj.tags.river_area) return false;
            const uint32_t g = groups.find(o);
            return water_major_keeps(river_km2[g], true, river_wikidata[g], margin && river_was_major[g]);
        };
        const bool keep = keeps(true);
        if (keep && obj.tags.river_area) river_groups.insert(groups.find(o));
        kept_by_margin += keep && !keeps(false);
        object_major[o] = keep;
        objects += keep;
        rivers += keep && obj.tags.river_area;
    }
    std::vector<uint8_t> out(n_polygons);
    size_t polygons = 0;
    for (size_t p = 0; p < n_polygons; p++) polygons += out[p] = object_major[b.object_of[p]];
    std::cerr << "  Water major tier: " << objects << " objects of " << n_objects << " (" << rivers
              << " river and canal areas of " << river_groups.size() << " rivers), " << polygons << " polygons of "
              << n_polygons << "; " << kept_by_margin << " kept by the margin, " << previous.size()
              << " polygons in the previous build's" << std::endl;
    return out;
}

Layer planet_layer(const Build& b, std::shared_ptr<const TypeTable> types, std::vector<uint8_t> major,
                   AdminArcBlocks arcs, std::vector<CellItemPair> cells) {
    Layer out;
    out.types = std::move(types);
    const size_t n = b.rings.polygons.size();
    // The names, alphabetical, each once.
    std::vector<std::string> names;
    names.reserve(b.objects.size());
    for (const Object& o : b.objects) names.push_back(o.tags.name);
    std::sort(names.begin(), names.end());
    names.erase(std::unique(names.begin(), names.end()), names.end());
    std::vector<uint32_t> offset(names.size());
    for (size_t k = 0; k < names.size(); k++) {
        offset[k] = static_cast<uint32_t>(out.names.size());
        out.names.insert(out.names.end(), names[k].begin(), names[k].end());
        out.names.push_back('\0');
    }
    if (out.names.size() >= NO_DATA) throw std::length_error("water names past 4 GiB");
    identities_of(b, out.identities, out.fallbacks);
    out.records.resize(n);
    for (size_t p = 0; p < n; p++) {
        const Object& o = b.objects[b.object_of[p]];
        AdminPolygon rec{};
        rec.ring_offset = 0;
        rec.name_id = offset[static_cast<size_t>(std::lower_bound(names.begin(), names.end(), o.tags.name) - names.begin())];
        rec.admin_level = 0;
        rec.area = static_cast<float>(o.area_deg2);
        rec.country_code = o.tags.code ? o.tags.code : out.types->verbatim_code(o.tags.verbatim);
        out.records[p] = rec;
    }
    out.major = std::move(major);
    out.arcs = std::move(arcs);
    out.cells = std::move(cells);
    if (out.major.size() != n || out.arcs.polygons.size() != n)
        throw std::logic_error("water layer parts out of step with its polygons");
    return out;
}

Layer subset_layer(const Layer& planet, const std::vector<uint8_t>& kept, unsigned threads) {
    if (threads == 0) threads = parallel_threads();
    if (kept.size() != planet.cells.size()) throw std::logic_error("water cell marks out of step with the cells");
    Layer out;
    out.types = planet.types;
    std::vector<std::atomic<uint64_t>> used(planet.size() / 64 + 1);
    parallel_for(kept.size(), [&](size_t begin, size_t end, unsigned) {
        for (size_t i = begin; i < end; i++)
            if (kept[i]) mark_bit(used, planet.cells[i].item_id & ID_MASK);
    }, threads);
    const RankedBits polygons(planet.size(), used, threads);
    const std::vector<uint32_t> ids = polygons.ones(threads);
    std::vector<std::atomic<uint64_t>>().swap(used);
    std::vector<std::atomic<uint64_t>> name_used(planet.names.size() / 64 + 1);
    for (uint32_t id : ids) {
        out.records.push_back(planet.records[id]);
        out.identities.push_back(planet.identities[id]);
        out.fallbacks.push_back(planet.fallbacks[id]);
        out.major.push_back(planet.major[id]);
        mark_bit(name_used, planet.records[id].name_id);
    }
    const StringSubset names(planet.names, name_used);
    for (AdminPolygon& r : out.records) r.name_id = names.offset_of(r.name_id);
    out.names = names.bytes();
    out.arcs = subset_admin_arcs(planet.arcs, ids, threads);
    out.cells = parallel_collect<CellItemPair>(kept.size(), [&](size_t i, std::vector<CellItemPair>& found) {
        if (!kept[i]) return;
        const CellItemPair& c = planet.cells[i];
        found.push_back({c.cell_id, static_cast<uint32_t>(polygons.index(c.item_id & ID_MASK)) | (c.item_id & INTERIOR_FLAG)});
    }, threads);
    return out;
}

std::string sitelinks_rows(const Build& b, const Layer& planet, const std::vector<int>& min_sitelinks,
                           const std::function<uint32_t(uint32_t)>& sitelinks) {
    std::ostringstream out;
    const ArcSetBlocks& set = planet.arcs.set(ArcSet::Full);
    const ArcBlocks& simplified = planet.arcs.tiers[0];
    for (int min : min_sitelinks) {
        std::vector<uint8_t> keep(b.objects.size(), 0);
        size_t objects = 0;
        for (size_t o = 0; o < b.objects.size(); o++) {
            const Object& obj = b.objects[o];
            const uint32_t links = std::max(obj.tags.qid ? sitelinks(obj.tags.qid) : 0,
                                            obj.key_qid ? sitelinks(obj.key_qid) : 0);
            keep[o] = obj.km2 >= kMajorAreaKm2 || (links > 0 && links >= static_cast<uint32_t>(min));
            objects += keep[o];
        }
        std::vector<uint8_t> arc_used(set.arcs(), 0);
        uint64_t ring_bytes = 0, polygons = 0;
        for (size_t p = 0; p < planet.size(); p++) {
            if (!keep[b.object_of[p]]) continue;
            polygons++;
            const AdminArcPolygon& poly = planet.arcs.polygons[p];
            ring_bytes += poly.blocks[0].size;
            for (uint32_t r = poly.first_ring; r < poly.first_ring + poly.ring_count; r++)
                for (uint32_t k = set.ring_refs[r]; k < set.ring_refs[r + 1]; k++) arc_used[set.refs[k].arc] = 1;
        }
        uint64_t arc_bytes = 0, arcs = 0;
        for (size_t a = 0; a < set.arcs(); a++)
            if (arc_used[a]) {
                arcs++;
                arc_bytes += simplified.bytes_of(a) + 4;
            }
        out << "WATER_RULE rule=sitelinks min_sitelinks=" << min << " min_km2=" << kMajorAreaKm2 << " objects=" << objects
            << " polygons=" << polygons << " arcs=" << arcs << " simplified_arc_bytes=" << arc_bytes
            << " ring_bytes=" << ring_bytes << " record_bytes=" << polygons * sizeof(AdminPolygonRecord) << "\n";
    }
    return out.str();
}

std::string attribution_json() {
    return "{\"attribution\": [\n"
           "  {\"id\": \"osm\", \"text\": \"\xC2\xA9 OpenStreetMap contributors\", \"licence\": \"ODbL 1.0\", "
           "\"licence_url\": \"https://opendatacommons.org/licenses/odbl/1-0/\", "
           "\"url\": \"https://www.openstreetmap.org/copyright\", "
           "\"notice\": \"This dataset is a derivative database of OpenStreetMap, available under the ODbL.\"}\n"
           "]}\n";
}

namespace {

// One water dir set: the records of `ids` (polygons of the layer) in their
// slots, with their ring blocks in `blocks` (whose polygons are `ids`'s, in
// order), its cells, strings, kinds and credit, into each of `dirs`; tier
// t's arcs into tier_dirs[t]. Returns its summary.
std::string write_set(const Layer& layer, const std::vector<uint32_t>& ids, const AdminArcBlocks& blocks,
                      const PolygonSlots& slots, const std::vector<std::string>& dirs,
                      const std::vector<std::pair<size_t, std::string>>& tier_dirs) {
    // The names its records use, in the layer's order.
    std::vector<std::atomic<uint64_t>> used(layer.names.size() / 64 + 1);
    for (uint32_t id : ids) mark_bit(used, layer.records[id].name_id);
    const StringSubset names(layer.names, used);

    std::vector<AdminPolygon> at_slot(slots.count, admin_polygon_tombstone());
    std::vector<uint32_t> polygon_of(slots.count, NO_DATA);
    std::vector<uint32_t> local_of(layer.size(), NO_DATA);
    std::set<uint16_t> codes;
    for (size_t k = 0; k < ids.size(); k++) {
        const uint32_t id = ids[k], slot = slots.slot_of[id];
        at_slot[slot] = layer.records[id];
        at_slot[slot].name_id = names.offset_of(layer.records[id].name_id);
        polygon_of[slot] = static_cast<uint32_t>(k);
        local_of[id] = slot;
        codes.insert(layer.records[id].country_code);
    }
    const size_t ring_bytes = write_ring_files(dirs, blocks, ArcSet::Full, at_slot, polygon_of);
    for (const auto& [t, dir] : tier_dirs) write_arc_files(dir, blocks.set(ArcSet::Full), blocks.tiers[t]);

    std::vector<CellItemPair> cells = parallel_collect<CellItemPair>(layer.cells.size(),
        [&](size_t i, std::vector<CellItemPair>& out) {
            const CellItemPair& c = layer.cells[i];
            const uint32_t slot = local_of[c.item_id & ID_MASK];
            if (slot != NO_DATA) out.push_back({c.cell_id, slot | (c.item_id & INTERIOR_FLAG)});
        });
    parallel_sort(cells.begin(), cells.end(), cell_item_less);
    const std::string meta = meta_json(*layer.types, codes), credit = attribution_json();
    std::array<uint32_t, STR_TIER_COUNT + 1> bases;
    bases.fill(static_cast<uint32_t>(names.bytes().size()));
    bases[0] = 0;
    for (const std::string& dir : dirs) {
        write_cell_index_sorted(dir + "/admin_cells.bin", dir + "/admin_entries.bin", cell_pairs::ADMIN, cells);
        string_files::write_file(dir + "/" + STR_TIER_FILENAMES[0], names.bytes().data(), names.bytes().size());
        write_strings_layout(dir, bases);
        write_text(dir + "/water_meta.json", meta);
        write_text(dir + "/attribution.json", credit);
    }
    size_t arc_bytes = 0;
    for (const auto& [t, dir] : tier_dirs) arc_bytes += blocks.tiers[t].bytes.size();
    std::ostringstream line;
    line << ids.size() << " polygons in " << slots.count << " slots (" << slots.kept << " kept, " << slots.by_fallback
         << " by fallback), " << ring_bytes << " ring bytes, " << arc_bytes << " arc bytes, " << count_cells(cells)
         << " cells, " << cells.size() << " entries, " << names.bytes().size() << " name bytes, " << codes.size()
         << " types";
    return line.str();
}

}  // namespace

std::string write_water_dirs(const Layer& layer, const std::string& region_dir, const std::string& region_prev,
                             const std::string& region) {
    const std::string all_s = region_dir + "/" + kWaterArcTiers[0].dir, all_e = region_dir + "/" + kWaterArcTiers[1].dir;
    const std::string major_dir = region_dir + "/water/major/simplified";
    const std::string prev_all = region_prev.empty() ? "" : region_prev + "/" + kWaterArcTiers[1].dir;
    const std::string prev_major = region_prev.empty() ? "" : region_prev + "/water/major/simplified";
    for (const std::string& d : {all_s, all_e, major_dir}) ensure_dir(d);
    std::ostringstream summary;

    std::vector<uint32_t> all(layer.size());
    std::iota(all.begin(), all.end(), 0u);
    const PolygonSlots slots = polygon_slots(layer, all, prev_all, all_e);
    summary << "  " << region << " water/all: "
            << write_set(layer, all, layer.arcs, slots, {all_s, all_e}, {{0, all_s}, {1, all_e}}) << "\n";
    gc::id_alloc::IdAllocator::write_sidecar(all_e + "/arc_index.osm_ids", layer.arcs.set(ArcSet::Full).sidecar);
    write_arc_fallback_sidecar(all_e + "/arc_index.fallback.osm_ids", layer.arcs.set(ArcSet::Full));

    std::vector<uint32_t> major;
    for (uint32_t p = 0; p < layer.size(); p++)
        if (layer.major[p]) major.push_back(p);
    AdminArcBlocks major_arcs = subset_admin_arcs(layer.arcs, major, 0);
    std::cerr << assign_full_set_slots(major_arcs, prev_major.empty() ? "" : prev_major + "/arc_index.osm_ids",
                                       prev_major.empty() ? "" : prev_major + "/arc_index.fallback.osm_ids",
                                       region, "dir=major", 0);
    const PolygonSlots major_slots = polygon_slots(layer, major, prev_major, major_dir);
    summary << "  " << region << " water/major: "
            << write_set(layer, major, major_arcs, major_slots, {major_dir}, {{0, major_dir}}) << "\n";
    gc::id_alloc::IdAllocator::write_sidecar(major_dir + "/arc_index.osm_ids", major_arcs.set(ArcSet::Full).sidecar);
    write_arc_fallback_sidecar(major_dir + "/arc_index.fallback.osm_ids", major_arcs.set(ArcSet::Full));
    return summary.str();
}

Layer build_layer(Collected& c, const std::string& prev_planet_dir, const std::function<uint32_t(uint32_t)>& sitelinks,
                  unsigned threads) {
    if (threads == 0) threads = parallel_threads();
    auto t = std::chrono::steady_clock::now();
    auto cpu = CpuTicks::now();
    const size_t relations = c.relations.size(), closed_ways = c.closed_ways.size(), waterways = c.waterways.size();
    Build b = assemble(c, threads);
    std::cerr << "  Water: " << b.objects.size() << " objects (" << closed_ways << " closed ways, " << relations
              << " relations, " << b.relations_missing_ways << " missing member ways, " << b.relations_without_rings
              << " without rings), " << b.rings.polygons.size() << " polygons, " << b.rings.rings.size() << " rings, "
              << b.rings.vertices.size() << " vertices; " << b.lines.lines.size() << " naming lines ("
              << b.lines.vertices.size() << " vertices, " << b.lines.keys.size() << " keys, " << waterways
              << " waterway relations)" << std::endl;
    log_phase("      Water: assembly", t, cpu);

    // The naming join over the river and canal areas.
    std::vector<uint32_t> river_polygons;
    for (uint32_t p = 0; p < b.rings.polygons.size(); p++)
        if (b.objects[b.object_of[p]].tags.river_area) river_polygons.push_back(p);
    NamingCounts counts;
    std::vector<Credit> credits = credit_naming_lines(b.rings, river_polygons, b.object_of, b.lines, counts, threads);
    const std::vector<NameChoice> choices = pick_names(credits, b.objects.size(), threads);
    size_t unnamed_rivers = 0, river_objects = 0, contested = 0;
    for (size_t o = 0; o < b.objects.size(); o++) {
        if (!b.objects[o].tags.river_area) continue;
        river_objects++;
        unnamed_rivers += b.objects[o].tags.name.empty();
        contested += choices[o].named() && choices[o].runner_up_mm * 4 >= choices[o].mm;
    }
    const size_t before = b.objects.size();
    const size_t named = apply_names(b, choices);
    std::cerr << "  Water names: " << river_polygons.size() << " river and canal polygons of " << river_objects
              << " objects, " << unnamed_rivers << " without a name of their own; " << counts.cover_entries
              << " cover entries, " << counts.segments_in_cover << " segments in their cells, " << counts.probes
              << " probed (" << counts.probes_inside << " inside), " << counts.credits << " credits; " << named
              << " named by their waterway, " << before - b.objects.size() << " left unnamed and out, " << contested
              << " with a runner-up of a quarter or more" << std::endl;
    free_storage(b.lines.lines);
    free_storage(b.lines.vertices);
    log_phase("      Water: names", t, cpu);

    split_polygons(b, threads);
    log_phase("      Water: split", t, cpu);
    std::shared_ptr<TypeTable> types = type_table(b, prev_planet_dir);
    AdminArcs arcs = build_arc_stage(kWaterArcStage, b.rings, {}, prev_planet_dir, threads);
    b.rings.drop_way_runs();
    AdminArcBlocks blocks = encode_admin_arcs(arcs, b.rings, threads);
    log_phase("      Water: arcs", t, cpu);
    std::vector<CellItemPair> cells = compute_water_index_cells(arcs, b.rings, threads);
    log_phase("      Water: cells", t, cpu);
    std::vector<uint8_t> major = major_polygons(b, arcs, prev_planet_dir);
    Layer layer = planet_layer(b, types, std::move(major), std::move(blocks), std::move(cells));
    std::cerr << sitelinks_rows(b, layer, {10, 20, 40}, sitelinks);
    std::map<uint16_t, size_t> by_code;
    for (const AdminPolygon& r : layer.records) by_code[r.country_code]++;
    std::ostringstream kinds;
    for (const auto& [code, n] : by_code) {
        const MetaType k = types->kind(code);
        kinds << " " << k.cls << "/" << k.type << "=" << n;
    }
    std::cerr << "  Water layer: " << layer.size() << " polygons of " << b.objects.size() << " objects, "
              << layer.names.size() << " name bytes, " << types->verbatim_count() << " verbatim types;" << kinds.str()
              << std::endl;
    log_phase("      Water: major tier and records", t, cpu);
    return layer;
}

void write_water_exact(const Layer& layer, const std::string& dir) {
    const std::string out = dir + "/water";
    ensure_dir(out);
    std::vector<uint32_t> all(layer.size());
    std::iota(all.begin(), all.end(), 0u);
    PolygonSlots slots;
    slots.count = static_cast<uint32_t>(layer.size());
    slots.slot_of = all;
    std::cerr << "  " << out << ": " << write_set(layer, all, layer.arcs, slots, {out}, {{1, out}}) << std::endl;
}

}  // namespace water
