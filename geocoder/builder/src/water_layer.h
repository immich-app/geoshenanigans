// The water layer: the named bodies of water of water_classify.h as areas of
// their own, in dirs of their own under each region,
// {region}/water/{all,major}/simplified/ and water/all/exact/, made of the
// admin geometry's file names. Each holds:
//   admin_polygons.bin + admin_rings.bin  one record per polygon it holds
//                                         (an OSM object's outer ring, a
//                                         marine area's polygon, or a grid
//                                         piece of either, water_split.h),
//                                         with its holes, carrying the
//                                         object's name, type code (in
//                                         country_code), whole area (its
//                                         polygons' exact areas less their
//                                         holes, summed, square degrees), so
//                                         records' areas don't add up, and
//                                         flags (kWeakName, in the place type)
//   arc_index.bin + arc_vertices.bin      the tier's arcs (kWaterArcStage:
//                                         rings cut at member way ends)
//   water_cut_arcs.bin                    a bit per arc slot, set for a cut:
//                                         an arc only one object's polygons
//                                         use, twice or more
//   admin_cells.bin + admin_entries.bin   the cell index at the admin cell
//                                         level (--admin-level, L10 by
//                                         default), interior where both
//                                         tiers' rings hold the cell
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

#include <charconv>
#include <cstdint>
#include <cstring>
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

// The Q number of a wikidata tag ("Q123" -> 123), 0 for none and for a tag
// that is more than a Q and the digits of a 32-bit number: two items, a
// number past 32 bits, a sign or a space.
inline uint32_t qid_of(const char* wikidata) {
    if (!wikidata || wikidata[0] != 'Q') return 0;
    const char* end = wikidata + std::strlen(wikidata);
    uint32_t q = 0;
    const auto [at, error] = std::from_chars(wikidata + 1, end, q);
    return error == std::errc() && at == end ? q : 0;
}

// The tags of an object in scope, or none (code 0, no verbatim).
template <class Tag>
ObjectTags object_tags(Tag tag) {
    Tags t;
    t.natural = tag("natural");
    t.water = tag("water");
    t.waterway = tag("waterway");
    t.landuse = tag("landuse");
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
    bool english = false;  // whether `name` is its name:en
};

// A closed way in scope.
struct ClosedWayArea {
    int64_t way = 0;
    ObjectTags tags;
    std::vector<PackedLocation> vertices;
};

// A candidate line as the way pass finds it: its way's own name, name:en
// and wikidata item, then its key and names once key_lines has keyed it.
struct LineIn {
    int64_t way = 0;
    std::string name, name_en;  // the way's tags, empty for none; cleared by key_lines
    uint32_t way_qid = 0;
    std::string key;
    std::string local;  // the key's local name
    std::string display;
    uint32_t qid = 0;
    std::vector<PackedLocation> vertices;
    // Its vertices' runs between nodes the input lacks, their counts in
    // order, each a line of its own; empty for one run of them all.
    std::vector<uint32_t> runs;
    // The way's own name:en, kept for a member of a waterway relation of a
    // local name alone (CollectedWaterway::english false); else empty.
    std::string way_en;
};

// Everything the passes collect for the layer, over every input: each
// input's objects after the last one's, so of an object two inputs hold
// the first input's copy comes first (and its way geometry is the one kept).
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

// Sorts the relations and waterways by id, each once (of copies, the first
// input's), and indexes the waterways' member ways
// (Collected::waterway_of_way).
void index_collected(Collected& c);

// The key and display name of a waterway=river or canal way, given its
// tags: its waterway relation's id and name (a member of one), else its
// local name and its name:en (else name); false for neither. Its wikidata
// item is its relation's, else its own. A member of a relation of a local
// name alone keeps its own name:en (LineIn::way_en), which assemble may show
// in the relation's place.
bool line_key(const Collected& c, int64_t way, const char* name, const char* name_en, uint32_t way_qid, LineIn& line);

// Keys every line by its way's tags (line_key) against the waterway
// relations of every input (index_collected), once all are read, so that a
// way's key doesn't hang on which input holds its relation; drops a line
// that names nothing.
void key_lines(Collected& c);

// --- The layer ---

// One object the layer holds: its identity and tags, its area (each of its
// polygons' exact area less its holes, summed over the object), and the
// name its waterway gave it. An OSM way's or relation's, or a marine
// area's (type SYNTHETIC, id its MRGID).
struct Object {
    gc::id_alloc::ObjectType type = gc::id_alloc::ObjectType::NONE;
    int64_t id = 0;
    ObjectTags tags;
    double area_deg2 = 0;  // its polygons' exact areas less their holes, square degrees
    double km2 = 0;
    uint32_t name_key = UINT32_MAX;  // the naming key it took (NamingLines::keys), UINT32_MAX for none
    uint32_t key_qid = 0;            // the lowest wikidata item of that key's lines in it, 0 for none
    bool weak_name = false;          // its name is its key's, taken weakly (water_names.h's weak_name)
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
    size_t outer_twins = 0;  // closed ways left to a relation of their names whose outer ring they are
};

// Assembles every collected relation (assemble_area_rings, nested by
// geometry as admin relations are: one polygon per outer ring with the holes
// in it) and closed way into polygons, but a closed way an assembled
// relation of its kind and names (name and local name) takes whole as an
// outer ring, that ring the way's own: one body tagged twice, whose islands
// the relation leaves out, and whose wikidata item (the lowest of such ways')
// the relation takes where it has none of its own. Then takes
// the marine areas' as they are, in object order (ways by id, relations by
// id, then marine areas by id; of an object or line two inputs hold, the
// first input's copy alone), each ring rotated as canonicalize_ring_rotation
// puts it, with its way runs (a marine ring's marine::ring_runs). A relation
// with a member way the passes didn't find is left out. Uses up the
// collected areas and geometries; moves the lines (keyed, key_lines) into
// the build's NamingLines, keys and names sorted. The names follow the
// dataset's (name:en, else name): a relation with a name:en of its own
// shows that; the lines of a waterway relation of a local name alone show
// the name:en its river ways carry (LineIn::way_en) over the most length
// (the smaller of a tie) where the ways of one hold more than half of its
// river ways' length, else the relation's name. Lengths are in whole
// millimetres, none across a node the input lacks.
Build assemble(Collected& c, unsigned threads);

// Leaves out each OSM natural=bay or strait that is a marine area's
// namesake, in place: its name or local name (name:en or name) is the
// marine area's (the name it shows or its IHO name, marine::shown_name),
// ASCII case, spaces and punctuation aside, and the polygons
// of the marine areas of its name hold all its water (polygons_cover: its
// polygons' rings even-odd, its holes left out, within theirs, a shared
// shore aside). Those marine areas answer there; every other bay and strait
// stays, the water of one that reaches past them, or round an island they
// leave out, among them. Logs each it leaves out and returns how many.
size_t drop_marine_namesakes(Build& b);

// Names each unnamed river or canal object by `choices` (pick_names, by
// object) and drops the objects with no name and their polygons, in place;
// sums each object's area, then marks each name so taken that is weak
// (weak_name: its key's length inside the object against the object's
// area). Returns how many objects it named and, of those, how many weakly.
std::pair<size_t, size_t> apply_names(Build& b, const std::vector<NameChoice>& choices);

// Cuts each polygon of more than kSplitMaxHoles holes into pieces along the
// grid (water_split.h), in place: each piece a polygon of the same object
// and outer rank, with its cut's key. Logs what it cut.
void split_polygons(Build& b, unsigned threads);

// The layer's type table: the water=* values outside the fixed table among
// the objects, against the previous build's kinds (the planet's
// water/all/exact/water_meta.json under prev_planet_dir; none when empty).
// A previous build that wrote water dirs (their polygon sidecar there) whose
// water_meta.json can't be read, or is none the builder writes, stops the
// build (std::runtime_error), as the codes its records carry would move.
std::shared_ptr<TypeTable> type_table(const Build& b, const std::string& prev_planet_dir);

// Each polygon's slot identity and fallback key, by polygon: a way's by its
// id; a relation's outer ring, or a marine area's, by the object and the
// ring's smallest vertex (and its cut), with a fallback key by the object
// and the ring's place. Two polygons that still share an identity (rings
// alike at their corners) are told apart by their fallback keys, which no
// two share: the one of the smaller keeps it, the other takes one of its own
// key mixed in, the same from day to day; `told_apart` counts those.
struct PolygonIdentities {
    std::vector<uint64_t> ids, fallbacks;
    size_t told_apart = 0;
};
PolygonIdentities polygon_identities(const Build& b);

// The major tier of the planet (water_classify.h): every marine area, each
// other object by its own area and kind, and each river object by its
// river's, the
// river objects grouped by their waterway relation (a naming key of one), by
// wikidata item (theirs, or that of a line of their key in them) and by
// local name along river polygons that share an arc. An object or river
// whose polygon the previous planet build's major tier held keeps it at
// kMajorKeepPercent: the polygon (its `identities`) matched to a slot of the
// sidecars under water/major/simplified/ of prev_planet_dir as that dir's
// slots match it, by identity, else by its fallback key. Logs how many it
// holds.
std::vector<uint8_t> major_polygons(const Build& b, const AdminArcs& arcs, const PolygonIdentities& identities,
                                    const std::string& prev_planet_dir);

// The records, identities and names of the planet's layer (Layer), its
// identities, arcs and cells moved in.
Layer planet_layer(const Build& b, std::shared_ptr<const TypeTable> types, std::vector<uint8_t> major,
                   PolygonIdentities identities, AdminArcBlocks arcs, std::vector<CellItemPair> cells);

// A region's share of the planet's layer: the polygons listed in the cells
// `kept` marks (parallel to planet.cells), whole, with their arcs
// (subset_admin_arcs, no slots yet), names and those cells' entries.
Layer subset_layer(const Layer& planet, const std::vector<uint8_t>& kept, unsigned threads = 0);

// Which of `regions` regions (up to 8) keep each entry of a water cell index
// `cells` (sorted by cell) as a continent's split does (subset_layer's
// `kept`): bit r where region r holds the entry's cell or one around it
// (admin_cell_routed), `holds(r, cell)` saying whether it holds a cell.
std::vector<uint8_t> routed_cell_masks(const std::vector<CellItemPair>& cells, size_t regions,
                                       const std::function<bool(size_t, uint64_t)>& holds, unsigned threads = 0);

// About what each of `regions` regions' shares of `planet` hold (up to 8;
// region r's share subset_layer of the cells whose masks[i] has bit r) once
// its arc slots and ring blocks are made: every polygon those cells list,
// whole, with its record, identities, name, rings and ring blocks, every
// arc of its rings with that arc's keys, slots and blocks in every tier,
// and the entries. A name a few polygons share counts for each: a little
// over, never under.
std::vector<uint64_t> share_bytes(const Layer& planet, const std::vector<uint8_t>& masks, size_t regions);

// The rows of the fame rule the major tier could take instead: objects of
// kMajorAreaKm2 or more, and those whose wikidata item has `min_sitelinks`
// sitelinks or more (`sitelinks` of a QID, 0 for none), with their simplified
// tier's arc and ring block bytes. One line each to the log.
std::string sitelinks_rows(const Build& b, const Layer& planet, const std::vector<int>& min_sitelinks,
                           const std::function<uint32_t(uint32_t)>& sitelinks);

// Writes a region's water dirs under region_dir/water (Layer, the region's
// arc slots assigned): the all set's two tiers and the major tier, each
// polygon in its slot against the previous build's sidecars under
// region_prev (none when empty; one there that can't be read is refused), a
// tombstone in each slot freed. Returns its log lines, labelled `region`:
// one for water/all and one for water/major, each with its polygons and
// slots, ring and arc bytes, cells and entries, name bytes and types.
std::string write_water_dirs(const Layer& layer, const std::string& region_dir, const std::string& region_prev,
                             const std::string& region);

// The planet's layer from what the passes collected, step by step: the
// assembly, the naming join (water_names_s2.h) and the names, the arc stage
// (kWaterArcStage) against the previous planet build's sidecars under
// prev_planet_dir (none when empty; a polygon sidecar there that can't be
// read is refused), its encoding, the cells, the major tier and the records. `sitelinks` gives a wikidata item's sitelink count (0 for
// none) for the fame rule's rows (sitelinks_rows). Logs each step and its
// counts. Uses up `c`.
Layer build_layer(Collected& c, const std::string& prev_planet_dir, const std::function<uint32_t(uint32_t)>& sitelinks,
                  unsigned threads);

// The single-output build's water: the all set's exact tier, in dir (the
// region's dir), as water/ beside its mode's files.
void write_water_exact(const Layer& layer, const std::string& dir);

// attribution.json of a water dir, with Marine Regions' entry when it holds
// a marine area: a JSON array of the entries as configurations.template.json's
// attribution list gives them, without their applies_to.
std::string attribution_json(bool marine);

}  // namespace water
