// The water layer: the named bodies of water of water_classify.h as areas of
// their own, in dirs of their own under each region,
// {region}/water/{all,major}/simplified/ and water/all/exact/, made of the
// admin geometry's file names. Each holds:
//   admin_polygons.bin + admin_rings.bin  one record per outer ring of an OSM
//                                         object, with its holes, carrying
//                                         the object's name, type code (in
//                                         country_code) and area (all its
//                                         outer rings', square degrees)
//   arc_index.bin + arc_vertices.bin      the tier's arcs (kWaterArcStage:
//                                         rings cut at member way ends)
//   admin_cells.bin + admin_entries.bin   the L10 cell index, interior where
//                                         both tiers' rings hold the cell
//   strings_core.bin + strings_layout.json  its own names, alphabetical
//   water_meta.json                       each type code it uses: class,
//                                         type and Nominatim rank
//   attribution.json                      the credit its data needs: OSM's,
//                                         and Marine Regions' where it holds
//                                         a marine area
// Beside OSM's water areas the layer holds the marine areas (marine.h), the
// seas and oceans of --marine, as objects of their own (place/sea and
// place/ocean).
// water/all/exact/ and water/major/simplified/ hold their set's sidecars
// too: admin_polygons.osm_ids (a polygon's slot by its OSM identity) and
// admin_polygons.fallback.osm_ids (the key that keeps a relation's outer
// ring in its slot when its smallest vertex moves), arc_index.osm_ids and
// arc_index.fallback.osm_ids. No file of another dir changes, and no dir
// points into one of these.
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "admin_arcs.h"
#include "id_allocator.h"
#include "marine.h"
#include "parsed_data.h"
#include "water_classify.h"
#include "water_layer_data.h"
#include "water_names.h"
#include "water_types.h"

namespace water {

// --- What the PBF passes collect ---

// An object's tags as the layer reads them: its kind (water_classify.h),
// its name as shown (name:en, else name; empty for an unnamed river or
// canal area), its local name (name, else name:en) and its wikidata item.
struct ObjectTags {
    uint16_t code = 0;
    std::string verbatim;  // a water=* value outside the fixed table
    bool river_area = false;
    std::string name;
    std::string local_name;
    uint32_t qid = 0;
};

// The Q number of a wikidata tag ("Q123" -> 123), 0 for none.
inline uint32_t qid_of(const char* wikidata) {
    if (!wikidata || wikidata[0] != 'Q') return 0;
    return static_cast<uint32_t>(std::strtoul(wikidata + 1, nullptr, 10));
}

// The tags of an object in scope, or none (code 0, no verbatim).
template <class Tag>
ObjectTags object_tags(Tag tag) {
    Tags t;
    t.natural = tag("natural");
    t.water = tag("water");
    t.waterway = tag("waterway");
    t.landuse = tag("landuse");
    t.place = tag("place");
    const Class c = classify(t);
    const char* name_en = tag("name:en");
    const char* name = tag("name");
    if (name_en && !*name_en) name_en = nullptr;
    if (name && !*name) name = nullptr;
    ObjectTags out;
    if (!in_scope(c, name_en || name)) return out;
    out.code = c.code;
    if (c.verbatim) out.verbatim = c.verbatim;
    out.river_area = c.river_area;
    if (name_en || name) out.name = name_en ? name_en : name;
    if (name || name_en) out.local_name = name ? name : name_en;
    out.qid = qid_of(tag("wikidata"));
    return out;
}

inline bool in_scope(const ObjectTags& t) { return t.code != 0 || !t.verbatim.empty(); }

// An area relation (type=multipolygon or boundary) in scope.
struct CollectedArea {
    int64_t id = 0;
    ObjectTags tags;
    std::vector<std::pair<int64_t, std::string>> members;  // its ways and their roles
};

// A named type=waterway relation of waterway=river or canal: what its
// member ways are keyed by when they name an area.
struct CollectedWaterway {
    int64_t id = 0;
    std::string name;        // name:en, else name
    std::string local_name;  // name, else name:en
    uint32_t qid = 0;
    std::vector<int64_t> ways;
};

// A closed way in scope.
struct ClosedWayArea {
    int64_t way = 0;
    ObjectTags tags;
    std::vector<PackedLocation> vertices;
};

// A candidate line as the way pass finds it.
struct LineIn {
    int64_t way = 0;
    std::string key;
    std::string local;  // the key's local name
    std::string display;
    uint32_t qid = 0;
    std::vector<PackedLocation> vertices;
};

// Everything the passes collect for the layer.
struct Collected {
    std::vector<CollectedArea> relations;
    std::vector<CollectedWaterway> waterways;
    // A waterway relation's member way: the index of the one of lowest id
    // it belongs to, in `waterways` sorted by id (index_waterways).
    std::unordered_map<int64_t, uint32_t> waterway_of_way;
    std::vector<ClosedWayArea> closed_ways;
    std::vector<LineIn> lines;
    // The relations' member ways' coordinates (ring assembly).
    WayGeometries member_geometries;
    // The marine areas (--marine), by id.
    std::vector<marine::Area> marine;
};

// Sorts the relations and waterways by id and indexes the waterways' member
// ways (Collected::waterway_of_way).
void index_collected(Collected& c);

// The key and display name of a waterway=river or canal way, given its
// tags: its waterway relation's id and name (a member of one), else its
// local name and its name:en (else name); false for neither.
bool line_key(const Collected& c, int64_t way, const char* name, const char* name_en, uint32_t way_qid, LineIn& line);

// --- The layer ---

// One object the layer holds: its identity and tags, its area summed over
// its outer rings, and the name its waterway gave it. An OSM way's or
// relation's, or a marine area's (type SYNTHETIC, id its MRGID).
struct Object {
    gc::id_alloc::ObjectType type = gc::id_alloc::ObjectType::NONE;
    int64_t id = 0;
    ObjectTags tags;
    double area_deg2 = 0;  // its polygons' exact areas, square degrees
    double km2 = 0;
    uint32_t name_key = UINT32_MAX;  // the naming key it took (NamingLines::keys), UINT32_MAX for none
    uint32_t key_qid = 0;            // that key's wikidata item, 0 for none
};

// The layer's build over the planet's objects, step by step (build_index.cpp
// runs them in order).
struct Build {
    std::vector<Object> objects;
    // Every polygon's rings, outer first, in polygon order; their way runs.
    AdminExactRings rings;
    std::vector<uint32_t> object_of;  // by polygon
    std::vector<uint32_t> outer_rank;  // by polygon: its place among its object's outer rings
    std::vector<uint64_t> piece_key;   // by polygon: its cut (water_split.h), 0 for an outer ring whole
    NamingLines lines;
    size_t relations_missing_ways = 0, relations_without_rings = 0, marine_objects = 0;
};

// Assembles every collected relation (assemble_area_rings, nested by
// geometry as admin relations are: one polygon per outer ring with the holes
// in it) and closed way into polygons, then takes the marine areas' as they
// are, in object order (ways by id, relations by id, then marine areas by
// id), each ring rotated as canonicalize_ring_rotation puts it, with its way
// runs (a marine ring's marine::ring_runs). A relation with a member way the
// passes didn't find is left out. Uses up the collected areas and
// geometries; moves the lines into the build's NamingLines, keys and names
// sorted.
Build assemble(Collected& c, unsigned threads);

// Names each unnamed river or canal object by `choices` (pick_names, by
// object) and drops the objects with no name and their polygons, in place;
// sums each object's area. Returns how many objects it named.
size_t apply_names(Build& b, const std::vector<NameChoice>& choices);

// Cuts each polygon of more than kSplitMaxHoles holes into pieces along the
// grid (water_split.h), in place: each piece a polygon of the same object
// and outer rank, with its cut's key. Logs what it cut.
void split_polygons(Build& b, unsigned threads);

// The layer's type table: the water=* values outside the fixed table among
// the objects, against the previous build's kinds (the planet's
// water/all/exact/water_meta.json under prev_planet_dir; none when empty).
std::shared_ptr<TypeTable> type_table(const Build& b, const std::string& prev_planet_dir);

// The major tier of the planet (water_classify.h): each object by its own
// area, and each river object by its river's, the river objects grouped by
// their naming key (a waterway relation or a local name), by wikidata item
// and by local name along river polygons that share an arc; an object or
// river whose polygon the previous planet build's major tier held (its
// sidecar, water/major/simplified/admin_polygons.osm_ids under
// prev_planet_dir) keeps it at kMajorKeepPercent. Logs how many it holds.
std::vector<uint8_t> major_polygons(const Build& b, const AdminArcs& arcs, const std::string& prev_planet_dir);

// The records, identities and names of the planet's layer (Layer), its
// arcs and cells moved in.
Layer planet_layer(const Build& b, std::shared_ptr<const TypeTable> types, std::vector<uint8_t> major,
                   AdminArcBlocks arcs, std::vector<CellItemPair> cells);

// A region's share of the planet's layer: the polygons listed in the cells
// `kept` marks (parallel to planet.cells), whole, with their arcs
// (subset_admin_arcs, no slots yet), names and those cells' entries.
Layer subset_layer(const Layer& planet, const std::vector<uint8_t>& kept, unsigned threads = 0);

// The rows of the fame rule the major tier could take instead: objects of
// kMajorAreaKm2 or more, and those whose wikidata item has `min_sitelinks`
// sitelinks or more (`sitelinks` of a QID, 0 for none), with their simplified
// tier's arc and ring block bytes. One line each to the log.
std::string sitelinks_rows(const Build& b, const Layer& planet, const std::vector<int>& min_sitelinks,
                           const std::function<uint32_t(uint32_t)>& sitelinks);

// Writes a region's water dirs under region_dir/water (Layer, the region's
// arc slots assigned): the all set's two tiers and the major tier, each
// polygon in its slot against the previous build's sidecars under
// region_prev (none when empty), a tombstone in each slot freed. `region`
// labels the log lines. Returns the bytes written by dir.
std::string write_water_dirs(const Layer& layer, const std::string& region_dir, const std::string& region_prev,
                             const std::string& region);

// The planet's layer from what the passes collected, step by step: the
// assembly, the naming join (water_names_s2.h) and the names, the arc stage
// (kWaterArcStage) against the previous planet build's sidecars under
// prev_planet_dir (none when empty), its encoding, the cells, the major tier
// and the records. `sitelinks` gives a wikidata item's sitelink count (0 for
// none) for the fame rule's rows (sitelinks_rows). Logs each step and its
// counts. Uses up `c`.
Layer build_layer(Collected& c, const std::string& prev_planet_dir, const std::function<uint32_t(uint32_t)>& sitelinks,
                  unsigned threads);

// The single-output build's water: the all set's exact tier, in dir (the
// region's dir), as water/ beside its mode's files.
void write_water_exact(const Layer& layer, const std::string& dir);

// attribution.json of a water dir, with Marine Regions' entry when it holds
// a marine area: a JSON array of the entries, as the mode dirs' are.
std::string attribution_json(bool marine);

}  // namespace water
