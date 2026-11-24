// The water layer's build steps and its dirs (water_layer.h).
#include "water_layer.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <unordered_map>

#include "admin_cover.h"
#include "cell_index.h"
#include "cell_index_io.h"
#include "exact_pip.h"
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

// An exact ring's area in km² on the sphere of 111.32 km a degree: R² times
// the sum over its edges of each one's longitude step (radians) by the mean
// of the sines of its ends' latitudes, each latitude where it lies. The
// sines are taken less the first vertex's, which a closed ring's steps,
// summing to nothing, leave out of the sum, so that a ring far from the
// equator keeps its digits. `v` holds `n` vertices, the first repeated at
// the end.
double ring_km2(const PackedLocation* v, uint32_t n) {
    if (n < 2) return 0;
    constexpr double kRad = 3.14159265358979323846 / 180.0 / 1e7;
    const double base = std::sin(v[0].lat_e7 * kRad);
    double sum = 0, before = 0;
    for (uint32_t i = 1; i < n; i++) {
        const double here = std::sin(v[i].lat_e7 * kRad) - base;
        sum += static_cast<double>(int64_t(v[i].lon_e7) - v[i - 1].lon_e7) * (before + here);
        before = here;
    }
    // R = 111.32 km × 180 / π, and a step's unit 1e-7 × π / 180 radians.
    return std::fabs(sum) / 2 * 1e-7 * kKm2PerDeg2AtEquator * (180.0 / 3.14159265358979323846);
}

// A polygon's area in km², its holes taken off.
double polygon_km2(const AdminExactRings& rings, uint32_t p) {
    const RingSpan* r = rings.rings_of(p);
    double km2 = ring_km2(rings.vertices.data() + r[0].begin, r[0].count);
    for (uint32_t k = 1; k < rings.polygons[p].ring_count; k++)
        km2 -= ring_km2(rings.vertices.data() + r[k].begin, r[k].count);
    return std::max(0.0, km2);
}

// How the log names an object: w<id>, r<id> or m<MRGID>.
std::string object_ref(const Object& o) {
    const char* kind = o.type == ObjectType::OSM_WAY ? "w" : o.type == ObjectType::OSM_RELATION ? "r" : "m";
    return kind + std::to_string(o.id);
}

// The bytes of the file at `path`; none for one it can't read.
std::optional<std::string> read_text(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) return std::nullopt;
    std::ostringstream s;
    s << f.rdbuf();
    if (f.bad()) return std::nullopt;
    return s.str();
}

// Copies the file written at `from` to `to`; a copy that fails is refused.
void copy_written(const std::string& from, const std::string& to) {
    std::error_code ec;
    std::filesystem::copy_file(from, to, std::filesystem::copy_options::overwrite_existing, ec);
    if (ec) throw std::runtime_error("failed to copy " + from + " to " + to + ": " + ec.message());
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
// one) for a missing file. One there that can't be opened, of another slot
// count, or cut short, is refused.
std::vector<uint64_t> read_fallback_sidecar(const std::string& path, size_t slots) {
    if (path_absent(path)) return std::vector<uint64_t>(slots, gc::id_alloc::kNoFallbackKey);
    std::ifstream f(path, std::ios::binary);
    if (!f) throw std::runtime_error("unreadable water polygon fallback sidecar " + path);
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

// Loads the previous build's polygon sidecar at `path`: false for none
// there; one there that can't be read is refused.
bool load_polygon_sidecar(IdAllocator& alloc, const std::string& path) {
    if (path_absent(path)) return false;
    if (!alloc.load_previous(path)) throw std::runtime_error("unreadable water polygon sidecar " + path);
    return true;
}

// Slots for `ids` (polygons of the layer, ascending) against the previous
// build's sidecars at `prev` (none when empty), written to `dir`; each id's
// slot, and the slot count.
struct PolygonSlots {
    std::vector<uint32_t> slot_of;  // parallel to the layer's records, NO_DATA for a polygon not here
    uint32_t count = 0;
    // The polygons that kept a live slot of the previous build's (by_fallback
    // of them by their fallback keys), and those that took a freed one.
    size_t kept = 0, by_fallback = 0, recycled = 0;
};

PolygonSlots polygon_slots(const Layer& layer, const std::vector<uint32_t>& ids, const std::string& prev_dir,
                           const std::string& dir) {
    IdAllocator alloc;
    const bool had = !prev_dir.empty() && load_polygon_sidecar(alloc, prev_dir + "/admin_polygons.osm_ids");
    const std::vector<uint64_t> prev_fallback =
        had ? read_fallback_sidecar(prev_dir + "/admin_polygons.fallback.osm_ids", alloc.total_slots())
            : std::vector<uint64_t>();
    std::vector<gc::id_alloc::FallbackPass> passes;
    passes.push_back({[&](size_t k) { return layer.fallbacks[ids[k]]; },
                      [&](uint32_t s) { return s < prev_fallback.size() ? prev_fallback[s] : gc::id_alloc::kNoFallbackKey; }});
    // The previous slots a polygon kept: those it can claim, live ones
    // (IdAllocator recycles the others).
    std::vector<uint8_t> was_live(alloc.total_slots());
    for (size_t s = 0; s < was_live.size(); s++) was_live[s] = gc::id_alloc::is_reclaimable(alloc.slots()[s]);
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
        if (slots[k] >= was_live.size()) continue;
        out.kept += was_live[slots[k]];
        out.recycled += !was_live[slots[k]];
    }
    std::vector<uint64_t> at_slot(out.count, gc::id_alloc::kNoFallbackKey);
    for (size_t k = 0; k < ids.size(); k++) at_slot[slots[k]] = layer.fallbacks[ids[k]];
    write_fallback_sidecar(dir + "/admin_polygons.fallback.osm_ids", at_slot);
    return out;
}

const std::vector<uint64_t> kNone;

// A line's length in whole millimetres: its runs' segments (segment_mm),
// none across the gap between two runs.
uint64_t line_mm(const LineIn& l) {
    uint64_t mm = 0;
    size_t at = 0;
    auto run_mm = [&](size_t count) {
        for (size_t i = at + 1; i < at + count; i++) mm += segment_mm(l.vertices[i - 1], l.vertices[i]);
        at += count;
    };
    if (l.runs.empty()) run_mm(l.vertices.size());
    for (const uint32_t run : l.runs) run_mm(run);
    return mm;
}

// Sorts `v` by id (`id_of`), each id once: of copies, the first in `v`.
template <class T, class Id>
void first_of_each_id(std::vector<T>& v, Id id_of) {
    std::stable_sort(v.begin(), v.end(), [&](const T& a, const T& b) { return id_of(a) < id_of(b); });
    v.erase(std::unique(v.begin(), v.end(), [&](const T& a, const T& b) { return id_of(a) == id_of(b); }), v.end());
}

}  // namespace

void index_collected(Collected& c) {
    if (c.relations.size() == c.indexed_relations && c.waterways.size() == c.indexed_waterways) return;
    first_of_each_id(c.relations, [](const CollectedArea& a) { return a.id; });
    first_of_each_id(c.waterways, [](const CollectedWaterway& w) { return w.id; });
    c.waterway_of_way.clear();
    for (uint32_t i = 0; i < c.waterways.size(); i++)
        for (int64_t way : c.waterways[i].ways) c.waterway_of_way.emplace(way, i);
    c.indexed_relations = c.relations.size();
    c.indexed_waterways = c.waterways.size();
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
        line.qid = w.qid ? w.qid : way_qid;
        if (!w.english && name_en) line.way_en = name_en;
        return true;
    }
    if (!name && !name_en) return false;
    line.local = name ? name : name_en;
    line.key = "n" + line.local;
    line.display = name_en ? name_en : name;
    line.qid = way_qid;
    return true;
}

void key_lines(Collected& c) {
    index_collected(c);
    size_t kept = 0;
    for (LineIn& l : c.lines) {
        const bool keyed = line_key(c, l.way, l.name.empty() ? nullptr : l.name.c_str(),
                                    l.name_en.empty() ? nullptr : l.name_en.c_str(), l.way_qid, l);
        std::string().swap(l.name);
        std::string().swap(l.name_en);
        if (!keyed) continue;
        if (&c.lines[kept] != &l) c.lines[kept] = std::move(l);
        kept++;
    }
    c.lines.resize(kept);
}

Build assemble(Collected& c, unsigned threads) {
    if (threads == 0) threads = parallel_threads();
    Build b;

    // The objects: closed ways by id, then relations by id; of an object two
    // inputs hold, the first input's copy.
    first_of_each_id(c.closed_ways, [](const ClosedWayArea& w) { return w.way; });
    first_of_each_id(c.relations, [](const CollectedArea& a) { return a.id; });

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

    // The outer rings of assembled relations that are one closed way's
    // (every way run of the ring theirs), by way: the relation's index and
    // the polygon's.
    std::unordered_map<int64_t, std::vector<std::pair<uint32_t, uint32_t>>> outer_of;
    for (uint32_t i = 0; i < c.relations.size(); i++) {
        const Assembled& a = assembled[i];
        if (a.missing) continue;
        for (size_t k = 0; k < a.polygons.size(); k++) {
            const std::vector<WayRun>& runs = a.runs[k].outer;
            if (a.polygons[k].outer.size() < 4 || runs.empty()) continue;
            const int64_t way = runs[0].way;
            bool whole = true;
            for (const WayRun& r : runs) whole &= r.way == way;
            if (whole) outer_of[way].push_back({i, static_cast<uint32_t>(k)});
        }
    }
    // The lowest wikidata item of the closed ways left to each relation, by
    // relation index (0 for none).
    std::vector<uint32_t> twin_qid(c.relations.size(), 0);
    // Whether closed way w is the whole outer ring of a relation of its kind
    // and names: one body tagged twice, which the relation gives, its
    // islands left out. Its ring as the assembly reads it (each run of
    // vertices at one place one vertex) must be that outer ring, from any
    // vertex and either way round: an outer ring the assembly cut a stretch
    // of the way out of is less than the way. Notes w's wikidata item for
    // each such relation.
    auto twin = [&](const ClosedWayArea& w) {
        auto it = outer_of.find(w.way);
        if (it == outer_of.end()) return false;
        std::vector<PackedLocation> ring;
        ring.reserve(w.vertices.size());
        for (const PackedLocation& p : w.vertices)
            if (ring.empty() || ring.back() != p) ring.push_back(p);
        bool found = false;
        for (const auto& [i, k] : it->second) {
            const ObjectTags& t = c.relations[i].tags;
            const std::vector<PackedLocation>& outer = assembled[i].polygons[k].outer;
            if (t.code != w.tags.code || t.verbatim != w.tags.verbatim || t.name != w.tags.name ||
                t.local_name != w.tags.local_name ||
                !same_closed_ring(outer.data(), static_cast<uint32_t>(outer.size()), ring.data(),
                                  static_cast<uint32_t>(ring.size())))
                continue;
            found = true;
            if (w.tags.qid && (!twin_qid[i] || w.tags.qid < twin_qid[i])) twin_qid[i] = w.tags.qid;
        }
        return found;
    };

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
        if (twin(w)) {
            b.outer_twins++;
            continue;
        }
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
            if (object == UINT32_MAX) {
                // A relation of no wikidata item of its own takes that of
                // the ways left to it.
                ObjectTags& t = c.relations[i].tags;
                if (!t.qid) t.qid = twin_qid[i];
                object = add_object(ObjectType::OSM_RELATION, c.relations[i].id, std::move(t));
            }
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
    // The marine areas, each polygon as the file gives it, each ring's runs
    // its stretches; each shows the name it goes by (marine::shown_name),
    // its IHO name its local one.
    for (marine::Area& area : c.marine) {
        if (area.polygons.empty()) continue;
        ObjectTags t;
        t.code = area.kind == marine::kOcean ? code::kPlaceOcean : code::kPlaceSea;
        t.name = marine::shown_name(area.name);
        t.local_name = area.name;
        const uint32_t object = add_object(ObjectType::SYNTHETIC, static_cast<int64_t>(area.id), std::move(t));
        uint32_t rank = 0;
        for (auto& rings : area.polygons) {
            std::vector<std::vector<WayRun>> runs;
            for (auto& ring : rings) {
                runs.push_back(marine::ring_runs(ring));
                canonicalize_ring_rotation(ring, runs.back());
            }
            const std::vector<std::vector<PackedLocation>> holes(std::make_move_iterator(rings.begin() + 1),
                                                                 std::make_move_iterator(rings.end()));
            add_polygon(object, rank++, rings[0], holes, runs);
            std::vector<std::vector<PackedLocation>>().swap(rings);
        }
        b.marine_objects++;
    }
    free_storage(c.marine);

    // The candidate lines, by way (the first input's copy), their keys and
    // names as sorted indices.
    first_of_each_id(c.lines, [](const LineIn& l) { return l.way; });
    // A waterway relation with only a local name shows the name:en its river
    // ways carry over the most length, the smaller of a tie, if all its ways
    // with a name:en together cover more than half its river ways' length.
    {
        struct Lengths {
            std::map<std::string, uint64_t> en;  // name:en -> millimetres
            uint64_t en_mm = 0, all_mm = 0;      // the ways of a name:en, all its ways
        };
        std::map<std::string, Lengths> lengths;  // the keys a way's name:en is kept for
        for (const LineIn& l : c.lines)
            if (!l.way_en.empty()) lengths[l.key];
        for (const LineIn& l : c.lines) {
            auto it = lengths.find(l.key);
            if (it == lengths.end()) continue;
            const uint64_t mm = line_mm(l);
            Lengths& k = it->second;
            k.all_mm += mm;
            if (!l.way_en.empty()) {
                k.en[l.way_en] += mm;
                k.en_mm += mm;
            }
        }
        std::map<std::string, std::string> shown;
        for (const auto& [key, k] : lengths) {
            if (k.en_mm <= k.all_mm - k.en_mm) continue;  // half of it or less
            uint64_t most = 0;
            for (const auto& [name, mm] : k.en)
                if (mm > most) {
                    most = mm;
                    shown[key] = name;
                }
        }
        for (LineIn& l : c.lines) {
            auto it = shown.find(l.key);
            if (it != shown.end()) l.display = it->second;
        }
    }
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
        if (l.runs.empty()) l.runs.push_back(static_cast<uint32_t>(l.vertices.size()));
        const uint32_t key = index_of(b.lines.keys, l.key), display = index_of(b.lines.displays, l.display);
        size_t at = 0;
        for (const uint32_t run : l.runs) {
            if (run >= 2) {
                b.lines.lines.push_back({l.way, key, display, l.qid, b.lines.vertices.size(), run});
                b.lines.vertices.insert(b.lines.vertices.end(), l.vertices.begin() + static_cast<std::ptrdiff_t>(at),
                                        l.vertices.begin() + static_cast<std::ptrdiff_t>(at + run));
            }
            at += run;
        }
        std::vector<PackedLocation>().swap(l.vertices);
    }
    free_storage(c.lines);
    measure_waterways(b.lines, threads);
    return b;
}

namespace {

// Keeps the objects `keep` marks and their polygons, in place and in order:
// their rings, way runs and vertices move down.
void keep_objects(Build& b, const std::vector<uint8_t>& keep) {
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
            // A ring in its place stays; one further on moves down to it.
            if (span.begin != vertices)
                std::copy(r.vertices.begin() + span.begin, r.vertices.begin() + span.begin + span.count,
                          r.vertices.begin() + static_cast<std::ptrdiff_t>(vertices));
            if (with_runs) {
                const auto [first, last] = r.runs_of_ring(ring);
                const size_t n = static_cast<size_t>(last - first);
                const size_t from = static_cast<size_t>(first - r.runs.data());
                if (from != runs)
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
        if (!b.piece_path.empty()) b.piece_path[polygons] = b.piece_path[p];
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
    if (!b.piece_path.empty()) b.piece_path.resize(polygons);
    r.polygons.shrink_to_fit();
    r.rings.shrink_to_fit();
    r.vertices.shrink_to_fit();
    r.runs.shrink_to_fit();
    r.first_run.shrink_to_fit();
}

// A name as namesakes compare: ASCII letters in lower case, each run of
// ASCII characters that are neither letters nor digits one space (none at
// either end), every other byte as it is.
std::string namesake_key(const std::string& name) {
    std::string out;
    bool gap = false;
    for (const char ch : name) {
        const unsigned char c = static_cast<unsigned char>(ch);
        if (c < 0x80 && !std::isalnum(c)) {
            gap = true;
            continue;
        }
        if (gap && !out.empty()) out += ' ';
        gap = false;
        out += c < 0x80 ? static_cast<char>(std::tolower(c)) : ch;
    }
    return out;
}

// A longitude along an edge at some latitude, num / den (den > 0).
struct Frac {
    __int128 num, den;
};

bool frac_less(const Frac& a, const Frac& b) { return a.num * b.den < b.num * a.den; }

// Whether the polygons `cover` (each object's of `object_of` its rings
// together counted even-odd: polygons of one object that don't overlap, so
// their union; then the union of the objects', which may overlap) hold all
// the water of the polygons `area` (their rings together counted even-odd):
// no open stretch of a parallel that `area` holds and no object of `cover`
// does. Between two latitudes a vertex of either lies on, each edge spans
// the band or misses it, and an edge of `cover` and one of `area` along one
// line across it (a shared shore), or two of `cover`, are taken once each
// or, two of one object's (the side two of its polygons share), cancel. An
// edge of `area` crossing one of `cover` (in one order at the band's foot,
// the other at its top) has water of `area` beside it that `cover` doesn't
// hold; two edges of one side crossing (rings that aren't valid, or two
// objects' that overlap there) are taken as alike. With no crossing, the
// edges keep their order across the band, which holds just what its middle
// parallel does. Edges east of the area's box hold nothing of it, and those
// wholly west of it only whether its west side starts in `cover`. Every
// value is exact: a longitude's numerator is under 2^65, its denominator
// 2^32.
bool polygons_cover(const AdminExactRings& rings, const std::vector<uint32_t>& object_of,
                    const std::vector<uint32_t>& cover, const std::vector<uint32_t>& area) {
    struct Edge {
        int64_t lat0, lat1, lng0, lng1;  // lat0 < lat1
        int of;                          // 0 for the area, 1 for the cover
        uint32_t object;                 // of the cover: its object's place among the cover's objects
        bool west;                       // wholly west of the area's box
    };
    // The cover's objects, each in a place of its own.
    std::vector<uint32_t> objects;
    for (uint32_t p : cover) objects.push_back(object_of[p]);
    std::sort(objects.begin(), objects.end());
    objects.erase(std::unique(objects.begin(), objects.end()), objects.end());
    auto place_of = [&](uint32_t p) {
        return static_cast<uint32_t>(std::lower_bound(objects.begin(), objects.end(), object_of[p]) - objects.begin());
    };
    std::vector<Edge> edges;
    std::vector<int64_t> lats;
    int64_t lat_lo = INT64_MAX, lat_hi = INT64_MIN, lng_lo = INT64_MAX, lng_hi = INT64_MIN;
    auto each_edge = [&](uint32_t p, auto fn) {
        const RingSpan* span = rings.rings_of(p);
        for (uint32_t k = 0; k < rings.polygons[p].ring_count; k++) {
            const PackedLocation* v = rings.vertices.data() + span[k].begin;
            for (uint32_t i = 0; i + 1 < span[k].count; i++) fn(v[i], v[i + 1]);
        }
    };
    for (uint32_t p : area)
        each_edge(p, [&](PackedLocation s, PackedLocation t) {
            lat_lo = std::min<int64_t>(lat_lo, s.lat_e7), lat_hi = std::max<int64_t>(lat_hi, s.lat_e7);
            lng_lo = std::min<int64_t>(lng_lo, s.lon_e7), lng_hi = std::max<int64_t>(lng_hi, s.lon_e7);
            lats.push_back(s.lat_e7);
            if (s.lat_e7 < t.lat_e7) edges.push_back({s.lat_e7, t.lat_e7, s.lon_e7, t.lon_e7, 0, 0, false});
            if (t.lat_e7 < s.lat_e7) edges.push_back({t.lat_e7, s.lat_e7, t.lon_e7, s.lon_e7, 0, 0, false});
        });
    if (lat_lo >= lat_hi) return true;
    for (uint32_t p : cover) {
        const uint32_t object = place_of(p);
        each_edge(p, [&](PackedLocation s, PackedLocation t) {
            if (s.lat_e7 == t.lat_e7 || std::max(s.lat_e7, t.lat_e7) <= lat_lo || std::min(s.lat_e7, t.lat_e7) >= lat_hi ||
                std::min(s.lon_e7, t.lon_e7) > lng_hi)
                return;
            const bool west = std::max(s.lon_e7, t.lon_e7) < lng_lo;
            for (const int32_t lat : {s.lat_e7, t.lat_e7})
                if (lat > lat_lo && lat < lat_hi) lats.push_back(lat);
            if (s.lat_e7 < t.lat_e7) edges.push_back({s.lat_e7, t.lat_e7, s.lon_e7, t.lon_e7, 1, object, west});
            else edges.push_back({t.lat_e7, s.lat_e7, t.lon_e7, s.lon_e7, 1, object, west});
        });
    }
    std::sort(lats.begin(), lats.end());
    lats.erase(std::unique(lats.begin(), lats.end()), lats.end());
    std::sort(edges.begin(), edges.end(), [](const Edge& x, const Edge& y) { return x.lat0 < y.lat0; });
    // An edge's longitude at latitude lat2 / scale.
    auto at = [](const Edge& e, int64_t lat2, int64_t scale) -> Frac {
        const int64_t d = e.lat1 - e.lat0;
        return {static_cast<__int128>(e.lng0) * d * scale + static_cast<__int128>(e.lng1 - e.lng0) * (lat2 - e.lat0 * scale),
                static_cast<__int128>(d) * scale};
    };
    struct Ends {
        Frac foot, top;
        int of;
        uint32_t object;
    };
    auto less = [](const Ends& x, const Ends& y) {
        if (frac_less(x.foot, y.foot)) return true;
        if (frac_less(y.foot, x.foot)) return false;
        return frac_less(x.top, y.top);
    };
    auto same = [&](const Ends& x, const Ends& y) { return !less(x, y) && !less(y, x); };
    std::vector<uint32_t> active;
    std::vector<Ends> band, once;
    // Whether each object of the cover holds the stretch, and how many do.
    std::vector<uint8_t> in_object(objects.size());
    size_t in_cover = 0;
    auto toggle = [&](uint32_t object) {
        in_object[object] ^= 1;
        in_cover = in_object[object] ? in_cover + 1 : in_cover - 1;
    };
    size_t next = 0;
    for (size_t k = 0; k + 1 < lats.size() && lats[k] < lat_hi; k++) {
        const int64_t foot = lats[k], top = lats[k + 1];
        while (next < edges.size() && edges[next].lat0 <= foot) active.push_back(static_cast<uint32_t>(next++));
        active.erase(std::remove_if(active.begin(), active.end(), [&](uint32_t e) { return edges[e].lat1 <= foot; }),
                     active.end());
        if (top <= lat_lo) continue;
        // In each object of the cover west of the area's box, and the other
        // edges, each line of the area or of one object across the band
        // taken as often as it is, mod 2.
        std::fill(in_object.begin(), in_object.end(), 0);
        in_cover = 0;
        band.clear();
        for (uint32_t e : active) {
            if (edges[e].west) toggle(edges[e].object);
            else band.push_back({at(edges[e], foot, 1), at(edges[e], top, 1), edges[e].of, edges[e].object});
        }
        std::sort(band.begin(), band.end(), [&](const Ends& x, const Ends& y) {
            if (x.of != y.of) return x.of < y.of;
            if (x.object != y.object) return x.object < y.object;
            return less(x, y);
        });
        once.clear();
        for (size_t i = 0; i < band.size();) {
            size_t j = i + 1;
            while (j < band.size() && band[j].of == band[i].of && band[j].object == band[i].object && same(band[j], band[i]))
                j++;
            if ((j - i) % 2) once.push_back(band[i]);
            i = j;
        }
        std::sort(once.begin(), once.end(), less);
        bool in_area = false;
        for (size_t i = 0; i < once.size(); i++) {
            if (i + 1 < once.size() && frac_less(once[i + 1].top, once[i].top)) return false;
            if (once[i].of) toggle(once[i].object);
            else in_area = !in_area;
            if (i + 1 < once.size() && !same(once[i], once[i + 1]) && in_area && in_cover == 0) return false;
        }
    }
    return true;
}

}  // namespace

size_t drop_marine_namesakes(Build& b) {
    // The marine areas by their names' keys, the name each shows and its IHO
    // name, and each object's polygons.
    std::map<std::string, std::vector<uint32_t>> marine;
    for (uint32_t o = 0; o < b.objects.size(); o++) {
        const Object& obj = b.objects[o];
        if (obj.type != ObjectType::SYNTHETIC) continue;
        marine[namesake_key(obj.tags.name)].push_back(o);
        if (namesake_key(obj.tags.local_name) != namesake_key(obj.tags.name))
            marine[namesake_key(obj.tags.local_name)].push_back(o);
    }
    if (marine.empty()) return 0;
    std::vector<uint32_t> first(b.objects.size() + 1, 0);
    for (uint32_t o : b.object_of) first[o + 1]++;
    std::partial_sum(first.begin(), first.end(), first.begin());
    // Each polygon's box, its outer ring's, as asked for.
    std::unordered_map<uint32_t, ExactRingBox> boxes;
    auto box_of = [&](uint32_t p) {
        auto it = boxes.find(p);
        if (it != boxes.end()) return it->second;
        const RingSpan& outer = b.rings.rings_of(p)[0];
        return boxes[p] = exact_ring_box(b.rings.vertices.data() + outer.begin, outer.count);
    };
    auto boxes_meet = [](const ExactRingBox& x, const ExactRingBox& y) {
        return x.lat_lo <= y.lat_hi && y.lat_lo <= x.lat_hi && x.lng_lo <= y.lng_hi && y.lng_lo <= x.lng_hi;
    };
    std::vector<uint8_t> keep(b.objects.size(), 1);
    std::ostringstream log;
    size_t dropped = 0;
    for (uint32_t o = 0; o < b.objects.size(); o++) {
        const Object& obj = b.objects[o];
        if (obj.type == ObjectType::SYNTHETIC || (obj.tags.code != code::kNaturalBay && obj.tags.code != code::kNaturalStrait))
            continue;
        std::set<uint32_t> namesakes;
        for (const std::string* name : {&obj.tags.name, &obj.tags.local_name}) {
            auto it = marine.find(namesake_key(*name));
            if (it != marine.end()) namesakes.insert(it->second.begin(), it->second.end());
        }
        if (namesakes.empty() || first[o] == first[o + 1]) continue;
        // Its polygons, and its namesakes' that meet their box.
        std::vector<uint32_t> area, cover;
        ExactRingBox box = box_of(first[o]);
        for (uint32_t p = first[o]; p < first[o + 1]; p++) {
            area.push_back(p);
            const ExactRingBox q = box_of(p);
            box = {std::min(box.lat_lo, q.lat_lo), std::max(box.lat_hi, q.lat_hi), std::min(box.lng_lo, q.lng_lo),
                   std::max(box.lng_hi, q.lng_hi)};
        }
        // Each namesake's alone first, which no other's edges cross, then
        // all of theirs together.
        std::vector<std::pair<uint32_t, std::vector<uint32_t>>> each;
        for (uint32_t m : namesakes) {
            std::vector<uint32_t> own;
            for (uint32_t q = first[m]; q < first[m + 1]; q++)
                if (boxes_meet(box, box_of(q))) own.push_back(q);
            if (own.empty()) continue;
            cover.insert(cover.end(), own.begin(), own.end());
            each.emplace_back(m, std::move(own));
        }
        if (cover.empty()) continue;
        uint32_t named = UINT32_MAX;
        for (const auto& [m, own] : each)
            if (polygons_cover(b.rings, b.object_of, own, area)) {
                named = m;
                break;
            }
        if (named == UINT32_MAX && each.size() > 1 && polygons_cover(b.rings, b.object_of, cover, area))
            named = each.front().first;
        if (named == UINT32_MAX) continue;
        keep[o] = 0;
        dropped++;
        log << "  Water: " << object_ref(obj) << " (" << obj.tags.name << ") left out as the namesake of marine area "
            << object_ref(b.objects[named]) << ": the marine areas of its name hold all of it\n";
    }
    if (dropped) keep_objects(b, keep);
    std::cerr << log.str();
    return dropped;
}

std::pair<size_t, size_t> apply_names(Build& b, const std::vector<NameChoice>& choices) {
    size_t named = 0;
    std::vector<uint8_t> keep(b.objects.size(), 0);
    // The length inside of the key that named each object so, and the length
    // of its waterway, by object.
    std::vector<uint64_t> named_mm(b.objects.size(), 0), waterway_mm(b.objects.size(), 0);
    for (size_t o = 0; o < b.objects.size(); o++) {
        Object& obj = b.objects[o];
        if (o < choices.size() && choices[o].named()) {
            // An area takes its line's river only where that is its own: of
            // its wikidata item where it has one, else of its local name;
            // any, of neither. One of no name takes the line's name all the
            // same, and the line's local name, by which the major tier
            // joins river areas along a shared arc, only with its river.
            const NameChoice& c = choices[o];
            const bool own = obj.tags.qid ? c.qid == obj.tags.qid
                                          : obj.tags.name.empty() || b.lines.key_local[c.key] == obj.tags.local_name;
            if (own) {
                obj.name_key = c.key;
                obj.key_qid = c.qid;
            }
            if (obj.tags.name.empty()) {
                obj.tags.name = b.lines.displays[choices[o].display];
                if (own) obj.tags.local_name = b.lines.key_local[choices[o].key];
                named_mm[o] = choices[o].mm;
                waterway_mm[o] = choices[o].waterway_mm;
                named++;
            }
        }
        keep[o] = !obj.tags.name.empty();
    }
    // In the kept objects' order, as keep_objects leaves them.
    size_t kept = 0;
    for (size_t o = 0; o < b.objects.size(); o++)
        if (keep[o]) {
            waterway_mm[kept] = waterway_mm[o];
            named_mm[kept++] = named_mm[o];
        }
    named_mm.resize(kept);
    waterway_mm.resize(kept);
    keep_objects(b, keep);
    AdminExactRings& r = b.rings;
    for (Object& obj : b.objects) obj.area_deg2 = obj.km2 = 0;
    for (size_t p = 0; p < r.polygons.size(); p++) {
        Object& obj = b.objects[b.object_of[p]];
        obj.area_deg2 += r.area(static_cast<uint32_t>(p));
        obj.km2 += polygon_km2(r, static_cast<uint32_t>(p));
    }
    size_t weak = 0;
    for (size_t o = 0; o < b.objects.size(); o++) {
        Object& obj = b.objects[o];
        obj.weak_name = named_mm[o] != 0 && weak_name(named_mm[o], obj.km2, waterway_mm[o]);
        weak += obj.weak_name;
    }
    return {named, weak};
}

void split_polygons(Build& b, unsigned threads) {
    if (threads == 0) threads = parallel_threads();
    AdminExactRings& r = b.rings;
    const size_t n = r.polygons.size();
    b.piece_key.assign(n, 0);
    b.piece_path.assign(n, 0);
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
            if (!runs_whole) {
                refusals[k] = SplitRefusal::WayRuns;
                continue;
            }
            pieces[k] = split_polygon(poly, kSplitMaxHoles, &lines[k], &refusals[k]);
            if (pieces[k].size() == 1) pieces[k].clear();
        }
    }, threads);

    // The polygons kept whole and why, and what it cut.
    auto report = [&](size_t cut, size_t cut_lines, size_t made, size_t kept_whole) {
        std::ostringstream whole;
        for (size_t h = 0; h < heavy.size(); h++)
            if (refusals[h] != SplitRefusal::None)
                whole << "  Water split: " << object_ref(b.objects[heavy_object[h]]) << " ("
                      << b.objects[heavy_object[h]].tags.name << ", "
                      << heavy_rings[h] - 1 << " holes) kept whole: " << refusal_name(refusals[h]) << "\n";
        std::cerr << whole.str() << "  Water split: " << heavy.size() << " polygons of more than " << kSplitMaxHoles
                  << " holes; " << cut << " cut along " << cut_lines << " lines into " << made << " pieces, "
                  << kept_whole << " kept whole; " << b.rings.polygons.size() << " polygons" << std::endl;
    };
    // None cut: the store stays as it is, every piece key 0.
    if (std::all_of(pieces.begin(), pieces.end(), [](const std::vector<SplitPolygon>& p) { return p.empty(); })) {
        report(0, 0, 0, heavy.size());
        return;
    }

    // The store again: each cut polygon's pieces in its place.
    AdminExactRings out;
    std::vector<uint32_t> object_of, outer_rank;
    std::vector<uint64_t> piece_key, piece_path;
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
                piece_path.push_back(piece.path);
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
            piece_path.push_back(0);
        }
        if (is_heavy) k++;
    }
    b.rings = std::move(out);
    b.object_of = std::move(object_of);
    b.outer_rank = std::move(outer_rank);
    b.piece_key = std::move(piece_key);
    b.piece_path = std::move(piece_path);
    report(cut, cut_lines, made, kept_whole);
}

std::shared_ptr<TypeTable> type_table(const Build& b, const std::string& prev_planet_dir) {
    std::map<std::string, size_t> uses;
    for (const Object& o : b.objects)
        if (!o.tags.verbatim.empty()) uses[o.tags.verbatim]++;
    std::vector<MetaType> previous;
    const std::string dir = prev_planet_dir.empty() ? "" : prev_planet_dir + "/" + kWaterArcTiers[1].dir;
    // A previous build that wrote water dirs (its polygon sidecar there) has
    // the codes its records carry in its water_meta.json. A sidecar there
    // that can't be opened is no sign of a build without them.
    const std::string sidecar = dir.empty() ? "" : dir + "/admin_polygons.osm_ids";
    if (!sidecar.empty() && !path_absent(sidecar)) {
        if (!std::ifstream(sidecar).good())
            throw std::runtime_error(sidecar + " is there but can't be opened: whether its build wrote water dirs, "
                                               "and so the water=* values' codes, is unknown");
        const std::string path = dir + "/water_meta.json";
        const std::optional<std::string> text = read_text(path);
        if (!text)
            throw std::runtime_error(path + " can't be read, though its build wrote water dirs: the water=* values' "
                                            "codes would move");
        const std::optional<std::vector<MetaType>> kinds = parse_meta_json(*text);
        if (!kinds)
            throw std::runtime_error(path + " is no water_meta.json the builder writes: the water=* values' codes "
                                            "would move");
        previous = *kinds;
    }
    auto table = std::make_shared<TypeTable>();
    table->assign(uses, previous);
    if (table->generic_count())
        std::cerr << "  Water types: " << table->generic_count()
                  << " water=* values past what water_meta.json lists are natural/water" << std::endl;
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

// Which of the polygons of `ids` and `fallbacks` (their slot identities
// and fallback keys) held a live slot of the dir's previous slots: matched
// as polygon_slots matches them, by identity, else by a fallback key one
// unclaimed slot alone shares. None without the dir's sidecar (one there
// that can't be read is refused); `live` counts its live slots.
std::vector<uint8_t> held_before(const std::vector<uint64_t>& ids, const std::vector<uint64_t>& fallbacks,
                                 const std::string& dir, size_t& live) {
    std::vector<uint8_t> out(ids.size(), 0);
    live = 0;
    IdAllocator alloc;
    if (dir.empty() || !load_polygon_sidecar(alloc, dir + "/admin_polygons.osm_ids")) return out;
    std::vector<uint8_t> was_live(alloc.total_slots());
    for (size_t s = 0; s < was_live.size(); s++) live += was_live[s] = gc::id_alloc::is_reclaimable(alloc.slots()[s]);
    const std::vector<uint64_t> prev_fallback = read_fallback_sidecar(dir + "/admin_polygons.fallback.osm_ids", was_live.size());
    std::vector<gc::id_alloc::FallbackPass> passes;
    passes.push_back({[&](size_t k) { return fallbacks[k]; }, [&](uint32_t s) { return prev_fallback[s]; }});
    const std::vector<uint32_t> slots = alloc.allocate_all(ids.size(), [&](size_t k) {
        return gc::id_alloc::SlotIdentity{static_cast<ObjectType>(ids[k] >> 56), ids[k] & 0x00FFFFFFFFFFFFFFull, 0};
    }, gc::id_alloc::kRecordsPerMatchPass, passes);
    for (size_t k = 0; k < ids.size(); k++) out[k] = slots[k] < was_live.size() && was_live[slots[k]];
    return out;
}

}  // namespace

PolygonIdentities polygon_identities(const Build& b) {
    PolygonIdentities out;
    std::vector<uint64_t>& ids = out.ids;
    std::vector<uint64_t>& fallbacks = out.fallbacks;
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
            const uint64_t piece = b.piece_key.empty() ? 0 : b.piece_key[p];
            const uint64_t path = b.piece_path.empty() ? 0 : b.piece_path[p];
            const uint64_t corner = ring_corner(b.rings.vertices.data() + outer.begin, outer.count, piece);
            if (o.type == ObjectType::SYNTHETIC) {
                ids[p] = marine_identity(static_cast<uint64_t>(o.id), corner);
                fallbacks[p] = piece_fallback(marine_fallback(static_cast<uint64_t>(o.id), b.outer_rank[p]), path);
                continue;
            }
            ids[p] = relation_ring_identity(o.id, corner);
            fallbacks[p] = piece_fallback(relation_ring_fallback(o.id, b.outer_rank[p]), path);
        }
    });
    std::vector<uint32_t> order(n);
    std::iota(order.begin(), order.end(), 0u);
    parallel_sort(order.begin(), order.end(), [&](uint32_t x, uint32_t y) {
        return ids[x] != ids[y] ? ids[x] < ids[y] : fallbacks[x] < fallbacks[y];
    });
    uint64_t before = 0;
    for (size_t k = 0; k < n; k++) {
        const uint32_t p = order[k];
        const uint64_t id = ids[p];
        if (k > 0 && id == before) {
            ids[p] = gc::id_alloc::make_key(ObjectType::SYNTHETIC, mix64(id ^ mix64(fallbacks[p] + 1)));
            out.told_apart++;
        }
        before = id;
    }
    return out;
}

std::vector<uint8_t> major_polygons(const Build& b, const AdminArcs& arcs, const PolygonIdentities& identities,
                                    const std::string& prev_planet_dir) {
    const size_t n_objects = b.objects.size(), n_polygons = b.rings.polygons.size();
    size_t previous = 0;
    const std::vector<uint8_t> held = held_before(identities.ids, identities.fallbacks,
        prev_planet_dir.empty() ? "" : prev_planet_dir + "/water/major/simplified", previous);
    std::vector<uint8_t> was_major(n_objects, 0);
    for (size_t p = 0; p < n_polygons; p++)
        if (held[p]) was_major[b.object_of[p]] = 1;

    // The rivers: river objects joined by waterway relation, by wikidata
    // item, and by local name across a shared arc. A key of a local name
    // alone joins nothing by itself: rivers of one name far apart are
    // rivers of their own.
    Groups groups(n_objects);
    std::map<uint32_t, uint32_t> by_key, by_qid;
    for (uint32_t o = 0; o < n_objects; o++) {
        const Object& obj = b.objects[o];
        if (!obj.tags.river_area) continue;
        if (obj.name_key != UINT32_MAX && b.lines.keys[obj.name_key][0] == 'r') {
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
        arc_objects.erase(std::unique(arc_objects.begin(), arc_objects.end()), arc_objects.end());
        // On each arc, each object joins the one before it of its local
        // name: the groups all pairs of one name would make. One of no
        // local name (an area another river's line named) joins none.
        auto local_of = [&](const std::pair<uint32_t, uint32_t>& e) -> const std::string& {
            return b.objects[e.second].tags.local_name;
        };
        for (size_t i = 0; i < arc_objects.size();) {
            size_t end = i + 1;
            while (end < arc_objects.size() && arc_objects[end].first == arc_objects[i].first) end++;
            if (end - i > 2)
                std::sort(arc_objects.begin() + static_cast<std::ptrdiff_t>(i),
                          arc_objects.begin() + static_cast<std::ptrdiff_t>(end), [&](const auto& x, const auto& y) {
                              const int by_name = local_of(x).compare(local_of(y));
                              return by_name != 0 ? by_name < 0 : x.second < y.second;
                          });
            for (size_t a = i + 1; a < end; a++)
                if (!local_of(arc_objects[a]).empty() && local_of(arc_objects[a]) == local_of(arc_objects[a - 1]))
                    groups.join(arc_objects[a - 1].second, arc_objects[a].second);
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
            if (obj.type == ObjectType::SYNTHETIC) return true;
            if (water_major_keeps(obj.km2, false, false, margin && was_major[o], is_coastal(obj.tags.code)))
                return true;
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
              << n_polygons << "; " << kept_by_margin << " kept by the margin, " << previous
              << " polygons in the previous build's" << std::endl;
    return out;
}

Layer planet_layer(const Build& b, std::shared_ptr<const TypeTable> types, std::vector<uint8_t> major,
                   PolygonIdentities identities, AdminArcBlocks arcs, std::vector<CellItemPair> cells) {
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
    if (identities.told_apart)
        std::cerr << "  Water identities: " << identities.told_apart
                  << " polygons that shared one told apart by their fallback keys" << std::endl;
    out.identities = std::move(identities.ids);
    out.fallbacks = std::move(identities.fallbacks);
    out.records.resize(n);
    for (size_t p = 0; p < n; p++) {
        const Object& o = b.objects[b.object_of[p]];
        AdminPolygon rec{};
        rec.ring_offset = 0;
        rec.name_id = offset[static_cast<size_t>(std::lower_bound(names.begin(), names.end(), o.tags.name) - names.begin())];
        rec.admin_level = 0;
        rec.place_type_override = o.weak_name ? kWeakName : 0;
        rec.area = static_cast<float>(o.area_deg2);
        rec.country_code = o.tags.code ? o.tags.code : out.types->verbatim_code(o.tags.verbatim);
        out.records[p] = rec;
    }
    out.major = std::move(major);
    out.marine.resize(n);
    for (size_t p = 0; p < n; p++) out.marine[p] = b.objects[b.object_of[p]].type == ObjectType::SYNTHETIC;
    out.objects = b.object_of;
    out.arcs = std::move(arcs);
    out.cells = std::move(cells);
    if (out.major.size() != n || out.objects.size() != n || out.arcs.polygons.size() != n)
        throw std::logic_error("water layer parts out of step with its polygons");
    return out;
}

Layer subset_layer(const Layer& planet, const std::vector<uint8_t>& masks, uint8_t bit, unsigned threads) {
    if (threads == 0) threads = parallel_threads();
    if (masks.size() != planet.cells.size()) throw std::logic_error("water cell marks out of step with the cells");
    Layer out;
    out.types = planet.types;
    std::vector<std::atomic<uint64_t>> used(planet.size() / 64 + 1);
    parallel_for(masks.size(), [&](size_t begin, size_t end, unsigned) {
        for (size_t i = begin; i < end; i++)
            if (masks[i] & bit) mark_bit(used, planet.cells[i].item_id & ID_MASK);
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
        out.marine.push_back(planet.marine[id]);
        out.objects.push_back(planet.objects[id]);
        mark_bit(name_used, planet.records[id].name_id);
    }
    StringSubset names(planet.names, name_used);
    for (AdminPolygon& r : out.records) r.name_id = names.offset_of(r.name_id);
    out.names = names.take_bytes();
    out.arcs = subset_admin_arcs(planet.arcs, ids, threads);
    out.cells = parallel_collect<CellItemPair>(masks.size(), [&](size_t i, std::vector<CellItemPair>& found) {
        if (!(masks[i] & bit)) return;
        const CellItemPair& c = planet.cells[i];
        found.push_back({c.cell_id, static_cast<uint32_t>(polygons.index(c.item_id & ID_MASK)) | (c.item_id & INTERIOR_FLAG)});
    }, threads);
    return out;
}

std::vector<uint8_t> routed_cell_masks(const std::vector<CellItemPair>& cells, size_t regions,
                                       const std::function<bool(size_t, uint64_t)>& holds, unsigned threads) {
    if (regions > 8) throw std::logic_error("routed_cell_masks: more than 8 regions");
    if (threads == 0) threads = parallel_threads();
    std::vector<uint8_t> masks(cells.size(), 0);
    parallel_for_runs(cells.size(), [&](size_t i) { return cells[i].cell_id == cells[i - 1].cell_id; },
                      [&](size_t begin, size_t end, unsigned) {
        std::vector<std::function<bool(uint64_t)>> held;
        for (size_t r = 0; r < regions; r++) held.push_back([&holds, r](uint64_t cell) { return holds(r, cell); });
        for (size_t i = begin; i < end;) {
            const uint64_t cell = cells[i].cell_id;
            uint8_t mask = 0;
            for (size_t r = 0; r < regions; r++)
                if (admin_cell_routed(cell, held[r])) mask |= static_cast<uint8_t>(1u << r);
            for (; i < end && cells[i].cell_id == cell; i++) masks[i] = mask;
        }
    }, threads);
    return masks;
}

std::vector<uint64_t> share_bytes(const Layer& planet, const std::vector<uint8_t>& masks, size_t regions,
                                  std::vector<ArcCounts>* arcs) {
    if (regions > 8) throw std::logic_error("share_bytes: more than 8 regions");
    if (masks.size() != planet.cells.size()) throw std::logic_error("water cell masks out of step with the cells");
    std::vector<uint64_t> out(regions, 0);
    auto add = [&](uint8_t mask, uint64_t bytes) {
        for (size_t r = 0; r < regions; r++)
            if (mask >> r & 1) out[r] += bytes;
    };
    // The regions of each polygon its cells give it, and the entries.
    std::vector<uint8_t> of_polygon(planet.size(), 0);
    uint8_t any = 0;
    for (size_t i = 0; i < masks.size(); i++) {
        of_polygon[planet.cells[i].item_id & ID_MASK] |= masks[i];
        any |= masks[i];
        add(masks[i], sizeof(CellItemPair));
    }
    // The last offsets of a region's ring refs and of each tier's blocks.
    const ArcStage& stage = *planet.arcs.stage;
    add(any, (1 + stage.tier_count) * sizeof(uint32_t));
    // Each polygon's: its record, identities, marks and object, its name, its
    // rings' refs and its ring blocks; and the regions of each arc its rings
    // use.
    const ArcSetBlocks& set = planet.arcs.set(ArcSet::Full);
    std::vector<uint8_t> of_arc(set.arcs(), 0);
    constexpr uint64_t kPolygonBytes = sizeof(AdminPolygon) + 2 * sizeof(uint64_t) + 2 * sizeof(uint8_t) +
                                       sizeof(uint32_t) + sizeof(AdminArcPolygon);
    for (size_t p = 0; p < planet.size(); p++) {
        const uint8_t mask = of_polygon[p];
        if (!mask) continue;
        const AdminArcPolygon& poly = planet.arcs.polygons[p];
        uint64_t bytes = kPolygonBytes + poly.blocks[0].size + poly.blocks[1].size +
                         std::strlen(planet.names.data() + planet.records[p].name_id) + 1;
        for (uint32_t r = poly.first_ring; r < poly.first_ring + poly.ring_count; r++) {
            bytes += sizeof(uint32_t) + (set.ring_refs[r + 1] - set.ring_refs[r]) * sizeof(ArcRef);
            for (uint32_t k = set.ring_refs[r]; k < set.ring_refs[r + 1]; k++) of_arc[set.refs[k].arc] |= mask;
        }
        add(mask, bytes);
    }
    // Each arc's: its box, keys and slots, and its blocks in every tier.
    constexpr uint64_t kArcBytes = sizeof(ArcBox) + sizeof(uint64_t) + sizeof(ArcFallbackKeys) + 2 * sizeof(uint32_t) +
                                   sizeof(gc::id_alloc::SidecarSlot);
    for (size_t a = 0; a < of_arc.size(); a++) {
        if (!of_arc[a]) continue;
        uint64_t bytes = kArcBytes;
        for (size_t t = 0; t < stage.tier_count; t++)
            if (!planet.arcs.tiers[t].at.empty()) bytes += planet.arcs.tiers[t].bytes_of(a) + sizeof(uint32_t);
        add(of_arc[a], bytes);
    }
    if (arcs) {
        // The regions of each arc the major tier's polygons use.
        std::vector<uint8_t> of_major(set.arcs(), 0);
        for (size_t p = 0; p < planet.size(); p++) {
            if (!of_polygon[p] || !planet.major[p]) continue;
            const AdminArcPolygon& poly = planet.arcs.polygons[p];
            for (uint32_t r = poly.first_ring; r < poly.first_ring + poly.ring_count; r++)
                for (uint32_t k = set.ring_refs[r]; k < set.ring_refs[r + 1]; k++) of_major[set.refs[k].arc] |= of_polygon[p];
        }
        arcs->assign(regions, ArcCounts{});
        for (size_t a = 0; a < of_arc.size(); a++)
            for (size_t r = 0; r < regions; r++) {
                (*arcs)[r].all += of_arc[a] >> r & 1;
                (*arcs)[r].major += of_major[a] >> r & 1;
            }
    }
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

std::string attribution_json(bool marine) {
    std::string out =
        "[\n"
        "  {\n"
        "    \"id\": \"osm\",\n"
        "    \"text\": \"\xC2\xA9 OpenStreetMap contributors\",\n"
        "    \"url\": \"https://www.openstreetmap.org/copyright\",\n"
        "    \"licence\": \"ODbL 1.0\",\n"
        "    \"licence_url\": \"https://opendatacommons.org/licenses/odbl/1-0/\",\n"
        "    \"notices\": [\n"
        "      \"This dataset is a derivative database of OpenStreetMap, available under the ODbL 1.0.\"\n"
        "    ]\n"
        "  }";
    if (marine)
        out +=
            ",\n"
            "  {\n"
            "    \"id\": \"marine-regions\",\n"
            "    \"text\": \"Flanders Marine Institute (2024). The intersect of the Exclusive Economic Zones and IHO "
            "sea areas, version 5. Available online at https://www.marineregions.org/. "
            "https://doi.org/10.14284/699\",\n"
            "    \"url\": \"https://www.marineregions.org/\",\n"
            "    \"licence\": \"CC BY 4.0\",\n"
            "    \"licence_url\": \"https://creativecommons.org/licenses/by/4.0/\",\n"
            "    \"notices\": [\n"
            "      \"Changes: each IHO sea area's parts in every EEZ dissolved into one, cut to the water off "
            "OpenStreetMap's land polygons and along a 1-degree grid, the coastal water the sea areas leave out "
            "given to the nearest sea area, and simplified in the simplified tiers. Cut to OpenStreetMap's land, "
            "the areas are part of this dataset's ODbL derivative database.\"\n"
            "    ]\n"
            "  }";
    return out + "\n]\n";
}

namespace {

// water_cut_arcs.bin of the polygons `ids` of the layer, whose ring blocks
// are in `blocks` (its polygons `ids`'s, in order): a bit per slot of its
// full set, bit s % 32 of u32 word s / 32 for slot s, set for an arc that is
// a cut, one that only one object's polygons use and that they use twice or
// more (two pieces of a lake split along the grid, two tiles of a marine
// area), so that the object's water lies on both sides of it. An arc of two
// objects, or of one ring once, is a shore.
std::vector<uint32_t> cut_arc_words(const Layer& layer, const std::vector<uint32_t>& ids,
                                    const AdminArcBlocks& blocks) {
    const ArcSetBlocks& set = blocks.set(ArcSet::Full);
    constexpr uint32_t kNone = UINT32_MAX, kShared = UINT32_MAX - 1;
    std::vector<uint32_t> object(set.arcs(), kNone);
    std::vector<uint8_t> uses(set.arcs(), 0);
    for (size_t k = 0; k < ids.size(); k++) {
        const AdminArcPolygon& p = blocks.polygons[k];
        const uint32_t o = layer.objects[ids[k]];
        for (uint32_t r = p.first_ring; r < p.first_ring + p.ring_count; r++) {
            if (!set.has_ring(r)) continue;
            for (uint32_t i = set.ring_refs[r]; i < set.ring_refs[r + 1]; i++) {
                const uint32_t a = set.refs[i].arc;
                object[a] = object[a] == kNone || object[a] == o ? o : kShared;
                uses[a] = static_cast<uint8_t>(std::min(uses[a] + 1, 2));
            }
        }
    }
    const std::vector<uint32_t> arc_of = arc_at_slot(set);
    std::vector<uint32_t> words((arc_of.size() + 31) / 32, 0);
    for (size_t slot = 0; slot < arc_of.size(); slot++) {
        const uint32_t a = arc_of[slot];
        if (a != UINT32_MAX && uses[a] >= 2 && object[a] != kShared) words[slot / 32] |= 1u << (slot % 32);
    }
    return words;
}

// One water dir set: the records of `ids` (polygons of the layer) in their
// slots, with their ring blocks in `blocks` (whose polygons are `ids`'s, in
// order), its cut arcs, cells, strings, kinds and credit, into each of
// `dirs`; tier t's arcs into tier_dirs[t]. Returns its summary.
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
    const std::vector<uint32_t> cuts = cut_arc_words(layer, ids, blocks);
    size_t cut_arcs = 0;
    for (uint32_t w : cuts) cut_arcs += static_cast<size_t>(__builtin_popcount(w));
    for (const std::string& dir : dirs)
        write_binary_file(dir + "/water_cut_arcs.bin", reinterpret_cast<const char*>(cuts.data()),
                          cuts.size() * sizeof(uint32_t));

    std::vector<CellItemPair> cells = parallel_collect<CellItemPair>(layer.cells.size(),
        [&](size_t i, std::vector<CellItemPair>& out) {
            const CellItemPair& c = layer.cells[i];
            const uint32_t slot = local_of[c.item_id & ID_MASK];
            if (slot != NO_DATA) out.push_back({c.cell_id, slot | (c.item_id & INTERIOR_FLAG)});
        });
    parallel_sort(cells.begin(), cells.end(), cell_item_less);
    bool marine = false;
    for (uint32_t id : ids) marine |= layer.marine[id] != 0;
    const std::string meta = meta_json(*layer.types, codes), credit = attribution_json(marine);
    std::array<uint32_t, STR_TIER_COUNT + 1> bases;
    bases.fill(static_cast<uint32_t>(names.bytes().size()));
    bases[0] = 0;
    // The cell index encoded once, its files copied to the other dirs.
    write_cell_index_sorted(dirs[0] + "/admin_cells.bin", dirs[0] + "/admin_entries.bin", cell_pairs::ADMIN, cells);
    for (size_t d = 1; d < dirs.size(); d++)
        for (const char* f : {"/admin_cells.bin", "/admin_entries.bin"}) copy_written(dirs[0] + f, dirs[d] + f);
    for (const std::string& dir : dirs) {
        string_files::write_file(dir + "/" + STR_TIER_FILENAMES[0], names.bytes().data(), names.bytes().size());
        write_strings_layout(dir, bases);
        write_text(dir + "/water_meta.json", meta);
        write_text(dir + "/attribution.json", credit);
    }
    size_t arc_bytes = 0;
    for (const auto& [t, dir] : tier_dirs) arc_bytes += blocks.tiers[t].bytes.size();
    std::ostringstream line;
    line << ids.size() << " polygons in " << slots.count << " slots (" << slots.kept << " kept, " << slots.by_fallback
         << " by fallback, " << slots.recycled << " recycled), " << ring_bytes << " ring bytes, " << arc_bytes
         << " arc bytes, " << cut_arcs << " cut arcs, " << count_cells(cells) << " cells, " << cells.size()
         << " entries, " << names.bytes().size() << " name bytes, " << codes.size() << " types";
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
    // Its arcs in the simplified tier alone, the one it writes.
    AdminArcBlocks major_arcs = subset_admin_arcs(layer.arcs, major, 0, 1u << 0);
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
              << " without rings, " << b.marine_objects << " marine areas; " << b.outer_twins
              << " closed ways left to the relations of their names whose outer rings they are), " << b.rings.polygons.size()
              << " polygons, " << b.rings.rings.size() << " rings, "
              << b.rings.vertices.size() << " vertices; " << b.lines.lines.size() << " naming lines ("
              << b.lines.vertices.size() << " vertices, " << b.lines.keys.size() << " keys, " << waterways
              << " waterway relations)" << std::endl;
    log_phase("      Water: assembly", t, cpu);
    const size_t namesakes = drop_marine_namesakes(b);
    std::cerr << "  Water: " << namesakes << " OSM bays and straits left out as marine areas' namesakes" << std::endl;

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
    const auto [named, weak] = apply_names(b, choices);
    std::cerr << "  Water names: " << river_polygons.size() << " river and canal polygons of " << river_objects
              << " objects, " << unnamed_rivers << " without a name of their own; " << counts.cover_entries
              << " cover entries, " << counts.segments_in_cover << " segments in their cells, " << counts.probes
              << " probed (" << counts.probes_inside << " inside), " << counts.credits << " credits; " << named
              << " named by their waterway (" << weak << " weakly, by a short one that runs a short way in), "
              << before - b.objects.size() << " left unnamed and out, " << contested
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
    PolygonIdentities identities = polygon_identities(b);
    std::vector<uint8_t> major = major_polygons(b, arcs, identities, prev_planet_dir);
    Layer layer = planet_layer(b, types, std::move(major), std::move(identities), std::move(blocks), std::move(cells));
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

namespace {

// The slots of the sidecar (IdAllocator's) at `path`, from its header: 0 for
// none there and for one that is no sidecar.
uint32_t sidecar_slots(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    uint32_t head[3];
    if (f.read(reinterpret_cast<char*>(head), sizeof head) && head[0] == gc::id_alloc::SIDECAR_MAGIC &&
        head[1] == gc::id_alloc::SIDECAR_VERSION)
        return head[2];
    return 0;
}

}  // namespace

uint32_t previous_slots(const std::string& region_prev) {
    if (region_prev.empty()) return 0;
    uint32_t most = 0;
    for (const char* dir : {kWaterArcTiers[1].dir, "water/major/simplified"})
        most = std::max(most, sidecar_slots(region_prev + "/" + dir + "/admin_polygons.osm_ids"));
    return most;
}

ArcCounts previous_arc_slots(const std::string& region_prev) {
    if (region_prev.empty()) return {};
    return {sidecar_slots(region_prev + "/" + kWaterArcTiers[1].dir + "/arc_index.osm_ids"),
            sidecar_slots(region_prev + "/water/major/simplified/arc_index.osm_ids")};
}

ArcCounts arc_counts(const Layer& layer) {
    const ArcSetBlocks& set = layer.arcs.set(ArcSet::Full);
    std::vector<uint8_t> major(set.arcs(), 0);
    for (size_t p = 0; p < layer.size(); p++) {
        if (!layer.major[p]) continue;
        const AdminArcPolygon& poly = layer.arcs.polygons[p];
        for (uint32_t r = poly.first_ring; r < poly.first_ring + poly.ring_count; r++)
            for (uint32_t k = set.ring_refs[r]; k < set.ring_refs[r + 1]; k++) major[set.refs[k].arc] = 1;
    }
    return {set.arcs(), static_cast<uint64_t>(std::count(major.begin(), major.end(), uint8_t(1)))};
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
