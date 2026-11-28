#pragma once

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <stdexcept>
#include <vector>
#include <string>

// --- Binary format structs (must match server's index reader) ---

struct WayHeader {
    uint32_t node_offset;
    uint16_t node_count;
    uint16_t _pad = 0;  // explicit so the written bytes are deterministic
    uint32_t name_id;
};
static_assert(sizeof(WayHeader) == 12, "WayHeader must be 12 bytes");
static_assert(offsetof(WayHeader, node_count) == 4 && offsetof(WayHeader, name_id) == 8,
              "geocoder-diff reads WayHeader fields at fixed offsets");

struct AddrPoint {
    float lat;
    float lng;
    uint32_t housenumber_id;
    uint32_t street_id;
    uint32_t parent_way_id;   // nearest street way index (for housenumber refinement)
    uint32_t vertex_offset;   // into the footprint stream below (NO_DATA for node-sourced points)
    uint32_t vertex_count;    // 0 for nodes, >0 for building polygons
};
static_assert(sizeof(AddrPoint) == 28, "AddrPoint must be 28 bytes");

struct InterpWay {
    uint32_t node_offset;
    uint16_t node_count;
    uint16_t _pad1 = 0; // explicit padding
    uint32_t street_id;
    uint32_t start_number;
    uint32_t end_number;
    uint8_t interpolation;
    uint8_t _pad4 = 0, _pad5 = 0, _pad6 = 0; // explicit padding
};
static_assert(sizeof(InterpWay) == 24, "InterpWay must be 24 bytes");
static_assert(offsetof(InterpWay, node_count) == 4 && offsetof(InterpWay, street_id) == 8,
              "geocoder-diff reads InterpWay fields at fixed offsets");

// Place type override for admin polygons: the type of the place node the
// polygon links (place_links.h), else of its own place tag.
// Must stay in sync with geocoder/server/src/main.rs place_type_to_field.
// Nominatim's get_label_tag (classtypes.py) returns extratags['place']
// or extratags['linked_place'] verbatim as the JSON field key, so each
// value here maps 1:1 to a distinct Nominatim field name.
enum class AdminPlaceType : uint8_t {
    NONE = 0,       // use admin_level mapping
    CITY = 1,
    TOWN = 2,
    VILLAGE = 3,
    SUBURB = 4,     // place=suburb
    NEIGHBOURHOOD = 5,
    QUARTER = 6,
    STATE = 7,
    PROVINCE = 8,
    REGION = 9,
    COUNTY = 10,
    DISTRICT = 11,
    BOROUGH = 12,   // place=borough (distinct field from suburb in Nominatim)
    HAMLET = 13,
    MUNICIPALITY = 14,  // place=municipality (e.g. Sydney Council L6)
    // place=locality: what labels a boundary of a locality (an Australian one linking its
    // locality node, an Irish townland tagged so) and a named place=locality area as
    // get_label_tag does, one of rank 25 (address-levels.json), in no street's or house's
    // address: it answers a point no street, house or POI is near.
    LOCALITY = 15,
};

// Nominatim's administrative levels run 2 to 12 (address-levels.json's
// administrative2 to 12); a closed way is a boundary from level 5, its
// import dropping one of level 4 or less (placex_insert,
// placex_triggers.sql:687-690).
constexpr uint8_t kMaxBoundaryLevel = 12;
// The highest admin level of a POI's parent polygon, the jurisdiction a query
// must lie in for the POI to answer it (the server's poi_chain_contains_query):
// a city's district at most, not the blocks and neighbourhoods of 11 and 12.
constexpr uint8_t kMaxPoiParentLevel = 10;
constexpr uint8_t kMinBoundaryWayLevel = 5;

// The admin_level of a postal area's polygon (boundary=postal_code), above
// the administrative levels and below the place areas' 15.
constexpr uint8_t POSTAL_AREA_LEVEL = 13;

// The place type of a postal area (POSTAL_AREA_LEVEL) whose postcode its
// country's pattern rejects: Nominatim makes no postcode area of it, and it
// names no street, POI or point.
constexpr uint8_t POSTAL_AREA_INVALID = 15;

// Vertex encoding tag: picks the bytes-per-vertex / precision tradeoff
// for this polygon.  Stored as a 1-byte tag in the per-polygon header
// at the start of the polygon's vertex slice.
enum class VertexEncoding : uint8_t {
    U16_1M    = 0,  // u16 @ 1e-5° (1.1 m), fits 73 km × 73 km bbox  → suburbs/cities
    U16_11M   = 1,  // u16 @ 1e-4° (11 m),  fits 730 km × 730 km     → states/regions
    U16_011M  = 2,  // u16 @ 1e-6° (11 cm), fits 7.3 km × 7.3 km     → POI buildings
    U32_1CM   = 3,  // u32 @ 1e-7° (1.1 cm), fits any polygon        → countries / fallback
};
static constexpr uint8_t VERTEX_STRIDE[4] = {4, 4, 4, 8};
static constexpr double VERTEX_SCALE[4] = {1e-5, 1e-4, 1e-6, 1e-7};  // degrees per delta step

// A polygon in the builder's poi_vertices and addr_vertices streams, which
// the patch tools merge and the dirs' files store encoded (addr_files.h for
// the addresses); only present when vertex_count > 0:
//
//   byte 0     : VertexEncoding tag
//   byte 1     : padding (alignment)
//   bytes 2..6 : bbox_min_lat (f32)
//   bytes 6..10: bbox_min_lng (f32)
//   bytes 10.. : vertex_count vertices, packed at the encoding's stride
//
// vertex_offset on the record is the BYTE offset to byte 0 of this
// header, which an address's logical record counts in units of
// addr_files::VERTEX_OFFSET_UNIT bytes (the builder's own AddrPoint holds
// the footprint's first vertex in its addr_vertices instead).  Header is
// omitted for vertex_count == 0 (point-only records,
// vertex_offset = NO_DATA).  Storing the header inline saves the
// per-record 12-byte tax on PoiRecord/AddrPoint where most records are
// point-only and would otherwise carry unused bbox_min fields.
static constexpr uint32_t POLY_HEADER_BYTES = 10;

// An admin polygon as the builder holds it. Its dirs store it as an
// AdminPolygonRecord, its rings the record's ring block in the dir's
// admin_rings.bin (arc_blocks.h): the outer ring, then the holes.
struct AdminPolygon {
    // NO_DATA for a strategy-2 tombstone, else 0: where the ring block lies
    // is each dir's (write_ring_files).
    uint32_t ring_offset;
    uint32_t name_id;
    uint8_t admin_level;
    uint8_t place_type_override = 0; // AdminPlaceType — overrides admin_level for field mapping
    // The polygon's own place tag (PlaceValue, admin_rank_config.h), 0 where
    // it has none: a place area's (admin_level 15) is its place type. Its
    // label is place_type_override, the type of a place node it links where
    // it links one (place_links.h), else of this.
    uint8_t own_place_value = 0;
    // Nominatim's rank_address of the polygon (rank_address.h); 0 for a
    // postal area and a tombstone.
    uint8_t rank_address = 0;
    float area;                   // of the exact rings, holes subtracted
    uint16_t country_code;
    uint8_t flags = 0;            // ADMIN_FLAG_* bits, which no dir holds
    uint8_t _pad4 = 0;
    // Its own name where it goes by the English name of the place node it
    // links (place_links.h linked_english_name), NO_DATA else: a core
    // string, which its dirs hold in admin_own_names.bin.
    uint32_t own_name_id = 0xFFFFFFFFu;
};
static_assert(sizeof(AdminPolygon) == 24, "AdminPolygon must be 24 bytes");

// AdminPolygon.flags bits.
// A place area whose boundary tag Nominatim keeps as a category of its own
// (boundary=historic, ceremonial: relation_import.h is_boundary_category).
static constexpr uint8_t ADMIN_FLAG_BOUNDARY = 0x02;

// An admin_polygons.bin record. Its ring block runs from its ring_offset to
// the next record's (the last one's to the end of admin_rings.bin): every
// record's offset is the running one, a tombstone's included, whose block is
// empty, as is that of a polygon a tier left without rings. A tombstone's
// name_id is NO_DATA. level_override is admin_level << 4 | the
// place_type_override, both under 16, and rank_address is the polygon's.
struct AdminPolygonRecord {
    uint32_t ring_offset;
    uint32_t name_id;
    float area;
    uint16_t country_code;
    uint8_t level_override;
    uint8_t rank_address = 0;
};
static_assert(sizeof(AdminPolygonRecord) == 16, "AdminPolygonRecord must be 16 bytes");

// An admin_polygons.bin's own names, the file beside it: an AdminOwnName for
// each polygon of an own name (AdminPolygon::own_name_id), ascending by slot.
// Nominatim's addr:city crosscheck compares every name of a polygon, its own
// and its linked node's (results.py:606), and its search finds it by them, so
// the server's crosscheck and the place search read it beside the name the
// polygon goes by.
inline constexpr const char* kAdminOwnNamesFile = "admin_own_names.bin";
struct AdminOwnName {
    uint32_t slot;
    uint32_t name_id;
};
static_assert(sizeof(AdminOwnName) == 8, "AdminOwnName must be 8 bytes");

inline AdminPolygonRecord admin_polygon_record(const AdminPolygon& p, uint32_t ring_offset) {
    if (p.admin_level > 15 || p.place_type_override > 15)
        throw std::logic_error("an admin level or place type past 4 bits");
    AdminPolygonRecord r;
    r.ring_offset = ring_offset;
    r.name_id = p.name_id;
    r.area = p.area;
    r.country_code = p.country_code;
    r.level_override = static_cast<uint8_t>(p.admin_level << 4 | p.place_type_override);
    r.rank_address = p.rank_address;
    return r;
}

struct NodeCoord {
    float lat;
    float lng;
};

// Sorted cell-item pair for direct entry writing (avoids hash map)
struct CellItemPair {
    uint64_t cell_id;
    uint32_t item_id;
};

// Canonical ordering for cell→item index tables: by cell_id, then item_id.
// Hoisted here because the deterministic-ordering passes sorted these tables
// with an identical inline lambda in 7 places; the exact tiebreak defines
// on-disk byte layout, so keeping it single-sourced prevents drift. A
// function object rather than a function, so the sorts of the planet's
// billions of pairs inline it instead of calling through a pointer.
struct CellItemLess {
    bool operator()(const CellItemPair& a, const CellItemPair& b) const {
        return a.cell_id < b.cell_id || (a.cell_id == b.cell_id && a.item_id < b.item_id);
    }
};
inline constexpr CellItemLess cell_item_less{};

// parallel_for_runs predicate over a cell-grouped table: pair i is in the
// cell of pair i - 1.
inline auto same_cell(const std::vector<CellItemPair>& pairs) {
    return [&pairs](size_t i) { return pairs[i].cell_id == pairs[i - 1].cell_id; };
}

// --- Constants ---

static const uint32_t INTERIOR_FLAG = 0x80000000u;
static const uint32_t ID_MASK       = 0x7FFFFFFFu;
static const uint32_t NO_DATA       = 0xFFFFFFFFu;

// A strategy-2 slot with no living polygon: no ring block (ring_offset
// NO_DATA) and no name.
inline AdminPolygon admin_polygon_tombstone() {
    AdminPolygon p{};
    p.ring_offset = NO_DATA;
    p.name_id = NO_DATA;
    return p;
}

// The strategy-2 slots with no living street way, interpolation way or
// address point: every byte zero but the name, the street, and the parent
// way and footprint offset, which are NO_DATA. The patch tools know a
// tombstone by these bytes.
inline WayHeader street_way_tombstone() {
    WayHeader w{};
    w.name_id = NO_DATA;
    return w;
}
inline InterpWay interp_way_tombstone() {
    InterpWay w{};
    w.street_id = NO_DATA;
    return w;
}
inline AddrPoint addr_point_tombstone() {
    AddrPoint p{};
    p.parent_way_id = NO_DATA;
    p.vertex_offset = NO_DATA;
    return p;
}

// FNV-1a 64-bit hash parameters, used to derive deterministic SYNTHETIC
// stable ids for TIGER interpolation ways and place nodes (build_index.cpp).
static const uint64_t FNV1A_OFFSET_BASIS = 14695981039346656037ULL;
static const uint64_t FNV1A_PRIME        = 1099511628211ULL;

// Processing limits
static constexpr int    MAX_NODE_COUNT          = 65535; // uint16_t node_count (OSM caps ways at 2000)
static constexpr int    MAX_S2_CELLS_PER_POLY   = 200;   // S2RegionCoverer max_cells
static constexpr int    BACKTRACK_CALL_BUDGET   = 100000; // per-ring backtracking limit
static constexpr int    BACKTRACK_TOTAL_BUDGET  = 500000; // per-relation backtracking limit
static constexpr int    BACKTRACK_MAX_DEPTH     = 200;    // recursion depth limit
static constexpr size_t MAX_SUBWAYS_FOR_RETRY   = 30;     // only retry small relations
static constexpr uint32_t MAX_TOUCHING_LOCATIONS = 100;   // osmium BasicAssembler::max_split_locations
static constexpr size_t MAX_NODE_ID_DEFAULT     = 15000000000ULL; // node index capacity (ids)

// Admin cell index covering (admin_cover.h). Each straight lat/lng ring edge
// is followed in pieces whose great-circle chords lie within the chord
// tolerance (6.4 m) of them and which span at most the max piece (64 km) of
// latitude or longitude; every cell within the reach of a piece is listed.
// The reader and the builder's links test a point by the exact e7 ray cast
// at the point's e7 rounding, which has no error of its own. The slack only
// covers the rounding of a cell centre to e7 for its interior test, at most
// 0.71e-7 deg, so a centre never crosses an edge.
static constexpr double ADMIN_COVER_CHORD_TOLERANCE_RAD = 1e-6;
static constexpr double ADMIN_COVER_MAX_PIECE_RAD       = 0.01;
static constexpr double ADMIN_COVER_READER_SLACK_DEG    = 1e-7;
static constexpr double ADMIN_COVER_REACH_RAD =
    ADMIN_COVER_CHORD_TOLERANCE_RAD + ADMIN_COVER_READER_SLACK_DEG * (3.14159265358979323846 / 180.0);

// --- Admin arcs (admin_arcs.h) ---

// The arc topology's coordinate-hash partitions of the vertices: its junction
// pass sorts each partition's neighbour lists on one thread, small enough to
// sort in cache and with no merge.
static constexpr uint32_t kArcPartitions = 4096;

// The arc sets: every admin polygon's rings, and admin-minimal's
// (admin_minimal_keeps), which admin-minimal-poi shares.
enum class ArcSet : uint8_t { Full, Minimal };

// The simplified tiers' Douglas-Peucker tolerance at every level, and its cap
// for an arc of small rings: kArcCapFactor times the square root of the
// smallest ring's area that uses the arc.
static constexpr double kArcSimplifyToleranceM = 37.5;
static constexpr double kArcCapFactor = 0.1;
// The simplified tiers' grid, in e7 units: 1e-5 degrees, 1.1 m.
static constexpr int32_t kSimplifiedGridE7 = 100;
// admin-minimal's grid, in e7 units: 1e-4 degrees, 11 m. A ring it would
// collapse keeps the simplified grid (build_arc_tier).
static constexpr int32_t kMinimalCoarseGridE7 = 1000;
// Rounds that give a degenerate simplified ring vertices back.
static constexpr int kArcValidityRounds = 64;
// Rounds that give vertices back where simplified segments cross or a
// polygon's rings stop nesting, and the grid they find crossings on, in e7
// units: 0.02 degrees.
static constexpr int kArcRepairRounds = 60;
static constexpr int64_t kArcRepairCellE7 = 200000;

// A boundary tier: the arcs of one set at one tolerance and grid. A tier
// with no tolerance stores every raw vertex as OSM has it: no simplifying,
// snapping or cap. A tier whose grid is coarser than its fine grid is first
// built on the fine one, then coarsened: a ring valid on the fine grid that
// its grid would collapse keeps its arcs and their junctions there.
struct ArcTier {
    ArcSet set;
    const char* dir;       // under a region's dir
    double tolerance_m;    // 0: none
    double cap_factor;     // 0: none
    int32_t grid_e7;       // the snap grid's step; 1: OSM's own
    int32_t fine_grid_e7;  // the grid a collapsing ring keeps: grid_e7 for a tier that doesn't coarsen
    // Whether the crossing repair counts only segments that one polygon's
    // rings use (its outer ring and its holes, a ring with itself): areas
    // that may overlap, whose borders cross one another where they lie.
    bool crossings_within_polygons = false;
    // Whether a ring valid as OSM has it that the tier's grid collapses with
    // every vertex kept (under 3 vertices or of no area: a pond within a
    // step or two of the grid) keeps its arcs, and their junctions, on the e7
    // grid: areas small enough to fit between the grid's points. So does a
    // ring that such a junction, on the e7 grid among its vertices on the
    // tier's, collapses in turn.
    bool collapsing_rings_on_e7 = false;
    bool simplifies() const { return tolerance_m > 0; }
    bool coarsens() const { return grid_e7 > fine_grid_e7; }
};

inline constexpr ArcTier kArcTiers[] = {
    {ArcSet::Full, "quality/simplified", kArcSimplifyToleranceM, kArcCapFactor, kSimplifiedGridE7, kSimplifiedGridE7},
    {ArcSet::Full, "quality/exact", 0, 0, 1, 1},
    {ArcSet::Minimal, "admin-minimal", kArcSimplifyToleranceM, kArcCapFactor, kMinimalCoarseGridE7, kSimplifiedGridE7},
};
inline constexpr size_t kArcTierCount = sizeof(kArcTiers) / sizeof(kArcTiers[0]);

// The water layer's tiers (water_layer.h), under a region's dir: the
// boundary tiers' tolerance, cap and grid, and every raw vertex. Water areas
// overlap (a lake in a wetland, a river in a bay), so the repair counts the
// crossings of each polygon's own rings only; a pond the grid would collapse
// keeps its shore on the e7 grid.
inline constexpr ArcTier kWaterArcTiers[] = {
    {ArcSet::Full, "water/all/simplified", kArcSimplifyToleranceM, kArcCapFactor, kSimplifiedGridE7, kSimplifiedGridE7,
     true, true},
    {ArcSet::Full, "water/all/exact", 0, 0, 1, 1, true},
};
inline constexpr size_t kWaterArcTierCount = sizeof(kWaterArcTiers) / sizeof(kWaterArcTiers[0]);

// Where the arc stage cuts its rings into arcs: at their junctions, or at
// those and at the ends of their member ways too (each vertex where a
// ring's way changes), so that an edit to one way changes only the arcs
// along it.
enum class ArcCuts : uint8_t { Junctions, JunctionsAndWayEnds };

// One run of the arc stage: the tiers it builds (the minimal set only when
// one of them is of it), where it cuts rings, the dir under a region's that
// holds its full set's sidecars, and what its log lines call it: `name` in
// the phase lines, `set_word` as the full set's set= in its ARC_CHECK lines.
struct ArcStage {
    const char* name;
    const char* set_word;
    const ArcTier* tiers;
    size_t tier_count;
    ArcCuts cuts;
    const char* sidecar_dir;
    bool has_minimal() const {
        for (size_t t = 0; t < tier_count; t++)
            if (tiers[t].set == ArcSet::Minimal) return true;
        return false;
    }
};

inline constexpr ArcStage kAdminArcStage{"Admin", "full", kArcTiers, kArcTierCount, ArcCuts::Junctions, "quality/exact"};
inline constexpr ArcStage kWaterArcStage{"Water", "water", kWaterArcTiers, kWaterArcTierCount,
                                         ArcCuts::JunctionsAndWayEnds, "water/all/exact"};
static_assert(kWaterArcTierCount <= kArcTierCount, "the encoded arcs hold kArcTierCount tiers at most");

// The dir beside admin-minimal that holds the minimal set's arcs too, in the
// shared polygon slots a POI's parent names (write_admin_minimal).
inline constexpr const char* kAdminMinimalPoiDir = "admin-minimal-poi";

// The arc files' blocks (arc_blocks.h). An arc block of this many stored
// vertices or more carries its box.
static constexpr uint32_t kArcBboxMinVertices = 16;
// A ring of more refs than kChunkBoxMinRefs is cut into chunks of refs, each
// with the box of its arcs' raw vertices: after a ref whose planet slot's
// mix64 has kRefChunkCutMask's bits all set once the chunk holds
// kRefChunkMin refs, after kRefChunkMax refs, and at the ring's end.
static constexpr uint32_t kChunkBoxMinRefs = 8;
static constexpr uint32_t kRefChunkMin = 4;
static constexpr uint32_t kRefChunkMax = 16;
static constexpr uint64_t kRefChunkCutMask = 7;
// How far, per axis in e7 units, a stored vertex may lie from its raw one:
// a chunk's raw box widened by this holds every vertex any tier stores of
// its arcs (half the 1e-4 degree grid, the coarsest a tier may take).
static constexpr int32_t kChunkBoxSlackE7 = 500;
constexpr bool every_tier_snaps_within_the_chunk_slack() {
    for (const ArcStage* stage : {&kAdminArcStage, &kWaterArcStage})
        for (size_t t = 0; t < stage->tier_count; t++) {
            const ArcTier& tier = stage->tiers[t];
            if (tier.grid_e7 / 2 > kChunkBoxSlackE7 || tier.fine_grid_e7 > tier.grid_e7) return false;
        }
    return true;
}
static_assert(every_tier_snaps_within_the_chunk_slack(),
              "a tier's grid snaps a vertex past the chunk slack, or its fine grid is coarser than its grid");

// --- Index mode ---

enum class IndexMode { Full, NoAddresses, AdminOnly };

// Whether independent write steps run at once or one after another (to hold
// less memory at a time). The output is the same either way.
enum class RunOrder { Concurrent, Serial };

// --- Deferred S2 work items ---

struct DeferredWay {
    uint32_t way_id;
    uint32_t node_offset;
    uint16_t node_count;
};

struct DeferredInterp {
    uint32_t interp_id;
    uint32_t node_offset;
    uint16_t node_count;
};

// Collected relation data for parallel admin assembly
struct CollectedRelation {
    int64_t id;
    // Its role=label node members, in relation order (Nominatim's first link
    // target: the first of them that is a place node).
    std::vector<int64_t> label_node_ids;
    uint8_t admin_level;
    // Its own place tag (PlaceValue), which labels it where it links no
    // place node of a type of AdminPlaceType's and ranks it, and whether a
    // place area's boundary tag is a category of its own
    // (is_boundary_category).
    uint8_t own_place_value = 0;
    bool boundary_category = false;
    std::string name;
    // Its name and wikidata tags, which it links a place node by
    // (place_links.h), and whether `name` is its own name:en.
    std::string name_tag, wikidata;
    bool name_en = false;
    std::string country_code;
    bool is_postal;
    std::vector<std::pair<int64_t, std::string>> members; // (way_id, role)
};

// --- Place nodes (settlements: city, town, village, suburb, etc.) ---

enum class PlaceType : uint8_t {
    CITY = 0, TOWN = 1, VILLAGE = 2, SUBURB = 3,
    HAMLET = 4, NEIGHBOURHOOD = 5, QUARTER = 6,
    UNKNOWN = 255
};

// PlaceNode._pad1 bits (place_claims.h; read by the server's place_node()
// and geocoder/search/src/model.rs).
// A boundary claims the node (Nominatim's linked_place_id): every reverse
// lookup skips it, as it skips a nameless node; it keeps its name for search.
static constexpr uint8_t PLACE_FLAG_HIDDEN = 0x02;
// parent_poly_id is the claiming boundary's polygon.
static constexpr uint8_t PLACE_FLAG_LABEL_OF = 0x01;

struct PlaceNode {
    float lat;
    float lng;
    uint32_t name_id;       // into strings.bin
    uint8_t place_type;     // PlaceType
    uint8_t _pad1 = 0;      // PLACE_FLAG_* bits
    // Nominatim's rank_address of the node as a place of its own
    // (rank_address.h): its type's (place_node_rank), two more where a
    // relation of that rank holds it. A hidden node's row holds none
    // (place_files.h), and an unhidden one has it.
    uint8_t rank_address = 0;
    uint8_t _pad2 = 0;
    // With PLACE_FLAG_LABEL_OF: the polygon of the boundary that claims
    // the node, which place search labels with it. Else NO_DATA.
    uint32_t parent_poly_id = NO_DATA;
};  // 20 bytes
static_assert(sizeof(PlaceNode) == 20, "PlaceNode must be 20 bytes");

// --- POI (Points of Interest) ---

enum class PoiCategory : uint8_t {
    // tourism — one category per OSM tag value
    MUSEUM = 0, ATTRACTION = 1, VIEWPOINT = 2, THEME_PARK = 3, ZOO = 4,
    GALLERY = 5, ARTWORK = 6, ALPINE_HUT = 7, AQUARIUM = 8, CAMP_SITE = 9,
    PICNIC_SITE = 10, RESORT = 11,
    HOTEL = 12, INFORMATION = 13, MOTEL = 14, GUEST_HOUSE = 15,
    HOSTEL = 16, APARTMENT = 17, CHALET = 18, CARAVAN_SITE = 19,
    // historic — each OSM value gets its own category
    CASTLE = 20, MONUMENT = 21, RUINS = 22, ARCHAEOLOGICAL_SITE = 23,
    MEMORIAL = 24, BATTLEFIELD = 25, FORT = 26, SHIP = 27,
    WAYSIDE_CROSS = 28, WAYSIDE_SHRINE = 29, CITY_GATE = 30,
    CITYWALLS = 31, BOUNDARY_STONE = 32, MILESTONE = 33,
    HISTORIC_MINE = 34, HISTORIC_AIRCRAFT = 35, LOCOMOTIVE = 36,
    CANNON = 37, TOMB = 38, MANOR = 39,
    // amenity — landmarks
    PLACE_OF_WORSHIP = 40, UNIVERSITY = 41, COLLEGE = 42, HOSPITAL = 43,
    THEATRE = 44, CINEMA = 45, LIBRARY = 46, MARKETPLACE = 47, EMBASSY = 48,
    FOUNTAIN = 49, CASINO = 50, CEMETERY = 51, FERRY_TERMINAL = 52,
    PLANETARIUM = 53, PRISON = 54,
    // amenity — civic/services (one per OSM value)
    RESTAURANT = 55, SCHOOL = 56, PHARMACY = 57, BANK = 58, POST_OFFICE = 59,
    POLICE = 67, FIRE_STATION = 68, TOWNHALL = 69,
    // leisure
    PARK = 60, NATURE_RESERVE = 61, STADIUM = 62, GARDEN = 63,
    WATER_PARK = 64, GOLF_COURSE = 65, MARINA = 66,
    PLAYGROUND = 70, PITCH = 71, SHELTER = 72,
    // transport / commerce / infrastructure roots
    BUS_STOP = 73, PARKING = 74, FUEL = 75, SHOP = 76,
    SPORTS_CENTRE = 77, FITNESS_CENTRE = 78, SPORTS_HALL = 79,
    // ── Sub-categories (171+) — each maps 1:1 to an OSM tag value
    //    so the surfaced `category` string is always meaningful.
    // Small amenities that show up as named rank-30 features
    TOILETS = 171, DRINKING_WATER = 172, BENCH = 173,
    VENDING_MACHINE = 174, WASTE_BASKET = 175, RECYCLING = 176,
    CLOCK = 177, TELEPHONE = 178, BBQ = 179,
    // Food & drink (amenity)
    CAFE = 180, BAR = 181, PUB = 182, FAST_FOOD = 183,
    ICE_CREAM = 184, BIERGARTEN = 185, FOOD_COURT = 186, NIGHTCLUB = 187,
    ARTS_CENTRE = 188, STUDIO = 189,
    // Education (amenity)
    KINDERGARTEN = 190, DRIVING_SCHOOL = 191, MUSIC_SCHOOL = 192,
    LANGUAGE_SCHOOL = 193, TRAINING = 194,
    // Health (amenity)
    DOCTORS = 200, DENTIST = 201, CLINIC = 202, VETERINARY = 203,
    NURSING_HOME = 204, AMBULANCE_STATION = 205, HOSPICE = 206,
    // Finance (amenity)
    ATM = 210, BUREAU_DE_CHANGE = 211,
    // Mail (amenity)
    POST_BOX = 215, PARCEL_LOCKER = 216,
    // Civic extras (amenity)
    COURTHOUSE = 220, COMMUNITY_CENTRE = 221, SOCIAL_CENTRE = 222,
    MONASTERY = 223, GRAVE_YARD = 224,
    // Transport extras (amenity)
    BUS_STATION = 225, TAXI = 226, CHARGING_STATION = 227,
    BICYCLE_PARKING = 228, MOTORCYCLE_PARKING = 229,
    CAR_WASH = 230, CAR_RENTAL = 231, BICYCLE_RENTAL = 232,
    BICYCLE_REPAIR_STATION = 233, VEHICLE_INSPECTION = 234,
    // Leisure extras
    FITNESS_STATION = 235, TRACK = 236, SAUNA = 237,
    HORSE_RIDING = 238, BIRD_HIDE = 239, MINIATURE_GOLF = 240,
    // Shop sub-categories — keep SHOP=76 as the generic fallback for
    // shop=* values we don't split below. These cover the commonly
    // surfaced shops so "shop" isn't a single fuzzy bucket.
    SHOP_SUPERMARKET = 241, SHOP_CONVENIENCE = 242,
    SHOP_CLOTHES = 243, SHOP_MALL = 244,
    SHOP_DEPARTMENT_STORE = 245, SHOP_BAKERY = 246,
    SHOP_BUTCHER = 247, SHOP_HARDWARE = 248,
    SHOP_ELECTRONICS = 249, SHOP_FURNITURE = 250,
    SHOP_JEWELRY = 251, SHOP_BOOKS = 252, SHOP_PET = 253,
    // natural
    PEAK = 80, VOLCANO = 81, BEACH = 82, CAVE_ENTRANCE = 83, SPRING = 84,
    WATERFALL = 85, GLACIER = 86, CLIFF = 87, ARCH = 88, HOT_SPRING = 89,
    GEYSER = 90, BAY = 91, CAPE = 92, ISLAND = 93,
    // Woodland / forest — `natural=wood`, `boundary=forest`, and
    // `landuse=forest` all fold into WOOD since from a reverse-
    // geocode perspective they're the same feature (named forested
    // area).
    WOOD = 94, HEATH = 95, SCRUB = 96, WETLAND = 97, GRASSLAND = 98,
    NATURAL_WATER = 99,
    // Extra natural features — landforms people name. Skip
    // `natural=tree` / `tree_row` which are ubiquitous and rarely
    // named; an unnamed tree as a landmark is noise.
    VALLEY = 132, RIDGE = 133, SADDLE = 134, GORGE = 135,
    BARE_ROCK = 136,
    // Landuse — only NAMED landuse polygons surface (unnamed
    // residential / industrial / farmland would dominate otherwise).
    MEADOW = 142, ORCHARD = 143, VINEYARD = 144, FARMLAND = 145,
    ALLOTMENTS = 146, QUARRY = 147, RESERVOIR = 148,
    RECREATION_GROUND = 149,
    MILITARY = 151, RELIGIOUS_LANDUSE = 152,
    // residential / industrial / commercial / retail landuse zones
    // are admin-hierarchy concepts, not user-facing landmarks — the
    // normal address chain (suburb / borough / city_district) already
    // covers them. Left out of the enum on purpose.
    // aeroway
    AERODROME = 100,
    // railway
    STATION = 105,
    // man_made
    TOWER = 110, LIGHTHOUSE = 111, WINDMILL = 112, BRIDGE = 113, PIER = 114,
    DAM = 115, OBSERVATORY = 116,
    SILO = 117, CHIMNEY = 118, WATERMILL = 119,
    // building
    CATHEDRAL = 120, PALACE = 121,
    // boundary
    NATIONAL_PARK = 130, PROTECTED_AREA = 131,
    // craft
    WINERY = 140, BREWERY = 141,
    // power
    POWER_PLANT = 150,
    // office
    GOVERNMENT = 160,
    // leisure=swimming_area and emergency=fire_water_pond, most often on
    // named water, which Nominatim imports as rank-30 POIs of these tags
    // (poi_classify.h has_poi_main_tag).
    SWIMMING_AREA = 161, FIRE_WATER_POND = 162,

    // Generic rank-30 node (amenity/shop/tourism/historic/leisure/man_made/
    // craft/office/waterway/natural/aeroway/railway/power) that didn't
    // match a specific PoiCategory above: Nominatim's reverse.py
    // DataLayer.POI filter (class_ NOT IN ('place', 'building'),
    // rank_search==30, geometry not line-like). Its default tier is below
    // every POI dir, so only a wikipedia or wikidata tag makes one a POI
    // (classify_poi).
    GENERIC_RANK30 = 170,

    UNKNOWN = 255
};

// Highest POI tier a poi/<tier> dir ships (poi/all). classify_poi gives no
// POI a higher one; a tombstone can remember one from an older build.
constexpr uint8_t POI_MAX_SHIPPED_TIER = 3;

// POI tier: 1=major, 2=notable, 3=everything. Returns 0 for unknown categories.
inline uint8_t poi_get_default_tier(PoiCategory cat) {
    switch (cat) {
        // tier 1 (major)
        case PoiCategory::VOLCANO: case PoiCategory::GLACIER: case PoiCategory::ISLAND:
        case PoiCategory::AERODROME: case PoiCategory::STATION:
        case PoiCategory::CATHEDRAL: case PoiCategory::PALACE:
        case PoiCategory::NATIONAL_PARK: case PoiCategory::PROTECTED_AREA:
            return 1;
        // tier 2 (notable)
        case PoiCategory::MUSEUM: case PoiCategory::ATTRACTION: case PoiCategory::VIEWPOINT:
        case PoiCategory::THEME_PARK: case PoiCategory::ZOO: case PoiCategory::AQUARIUM:
        case PoiCategory::RESORT:
        case PoiCategory::CASTLE: case PoiCategory::MONUMENT: case PoiCategory::RUINS:
        case PoiCategory::ARCHAEOLOGICAL_SITE: case PoiCategory::FORT: case PoiCategory::SHIP:
        case PoiCategory::UNIVERSITY: case PoiCategory::HOSPITAL: case PoiCategory::CASINO:
        case PoiCategory::PARK: case PoiCategory::NATURE_RESERVE: case PoiCategory::STADIUM:
        case PoiCategory::WATER_PARK:
        case PoiCategory::BEACH: case PoiCategory::WATERFALL: case PoiCategory::GEYSER:
        case PoiCategory::BAY: case PoiCategory::CAPE:
        case PoiCategory::LIGHTHOUSE: case PoiCategory::DAM: case PoiCategory::OBSERVATORY:
        case PoiCategory::POWER_PLANT: case PoiCategory::GOVERNMENT:
            return 2;
        // tier 3 (everything)
        case PoiCategory::GALLERY: case PoiCategory::ARTWORK: case PoiCategory::ALPINE_HUT:
        case PoiCategory::CAMP_SITE: case PoiCategory::PICNIC_SITE:
        case PoiCategory::MEMORIAL: case PoiCategory::BATTLEFIELD:
        case PoiCategory::PLACE_OF_WORSHIP: case PoiCategory::COLLEGE: case PoiCategory::THEATRE:
        case PoiCategory::CINEMA: case PoiCategory::LIBRARY: case PoiCategory::MARKETPLACE:
        case PoiCategory::EMBASSY: case PoiCategory::FOUNTAIN: case PoiCategory::CEMETERY:
        case PoiCategory::FERRY_TERMINAL: case PoiCategory::PLANETARIUM: case PoiCategory::PRISON:
        case PoiCategory::GARDEN: case PoiCategory::GOLF_COURSE: case PoiCategory::MARINA:
        case PoiCategory::PEAK: case PoiCategory::CAVE_ENTRANCE: case PoiCategory::SPRING:
        case PoiCategory::CLIFF: case PoiCategory::ARCH: case PoiCategory::HOT_SPRING:
        case PoiCategory::TOWER: case PoiCategory::WINDMILL: case PoiCategory::BRIDGE:
        case PoiCategory::PIER:
        case PoiCategory::WINERY: case PoiCategory::BREWERY:
        case PoiCategory::HOTEL: case PoiCategory::MOTEL:
        case PoiCategory::GUEST_HOUSE: case PoiCategory::HOSTEL:
        case PoiCategory::APARTMENT: case PoiCategory::CHALET:
        case PoiCategory::CARAVAN_SITE:
        case PoiCategory::INFORMATION:
        case PoiCategory::RESTAURANT: case PoiCategory::CAFE:
        case PoiCategory::BAR: case PoiCategory::PUB:
        case PoiCategory::FAST_FOOD: case PoiCategory::ICE_CREAM:
        case PoiCategory::BIERGARTEN: case PoiCategory::FOOD_COURT:
        case PoiCategory::NIGHTCLUB:
        case PoiCategory::SCHOOL: case PoiCategory::KINDERGARTEN:
        case PoiCategory::DRIVING_SCHOOL: case PoiCategory::MUSIC_SCHOOL:
        case PoiCategory::PHARMACY: case PoiCategory::DOCTORS:
        case PoiCategory::DENTIST: case PoiCategory::CLINIC:
        case PoiCategory::VETERINARY:
        case PoiCategory::BANK: case PoiCategory::ATM:
        case PoiCategory::BUREAU_DE_CHANGE:
        case PoiCategory::POST_OFFICE: case PoiCategory::POST_BOX:
        case PoiCategory::PARCEL_LOCKER:
        case PoiCategory::POLICE: case PoiCategory::FIRE_STATION:
        case PoiCategory::TOWNHALL: case PoiCategory::COURTHOUSE:
        case PoiCategory::COMMUNITY_CENTRE: case PoiCategory::SOCIAL_CENTRE:
        case PoiCategory::PLAYGROUND: case PoiCategory::PITCH:
        case PoiCategory::TRACK: case PoiCategory::FITNESS_STATION:
        case PoiCategory::FITNESS_CENTRE: case PoiCategory::SHELTER:
        case PoiCategory::SPORTS_CENTRE: case PoiCategory::SPORTS_HALL:
        case PoiCategory::BUS_STOP: case PoiCategory::BUS_STATION:
        case PoiCategory::TAXI:
        case PoiCategory::PARKING: case PoiCategory::BICYCLE_PARKING:
        case PoiCategory::MOTORCYCLE_PARKING:
        case PoiCategory::FUEL: case PoiCategory::CHARGING_STATION:
        case PoiCategory::SHOP:
        case PoiCategory::WAYSIDE_CROSS: case PoiCategory::WAYSIDE_SHRINE:
        case PoiCategory::CITY_GATE: case PoiCategory::CITYWALLS:
        case PoiCategory::BOUNDARY_STONE: case PoiCategory::MILESTONE:
        case PoiCategory::HISTORIC_MINE: case PoiCategory::HISTORIC_AIRCRAFT:
        case PoiCategory::LOCOMOTIVE: case PoiCategory::CANNON:
        case PoiCategory::TOMB: case PoiCategory::MANOR:
        case PoiCategory::SILO: case PoiCategory::CHIMNEY:
        case PoiCategory::WATERMILL:
        case PoiCategory::TOILETS: case PoiCategory::DRINKING_WATER:
        case PoiCategory::BENCH: case PoiCategory::VENDING_MACHINE:
        case PoiCategory::WASTE_BASKET: case PoiCategory::RECYCLING:
        case PoiCategory::CLOCK: case PoiCategory::TELEPHONE:
        case PoiCategory::BBQ:
        case PoiCategory::ARTS_CENTRE: case PoiCategory::STUDIO:
        case PoiCategory::LANGUAGE_SCHOOL: case PoiCategory::TRAINING:
        case PoiCategory::NURSING_HOME: case PoiCategory::AMBULANCE_STATION:
        case PoiCategory::HOSPICE:
        case PoiCategory::MONASTERY: case PoiCategory::GRAVE_YARD:
        case PoiCategory::CAR_WASH: case PoiCategory::CAR_RENTAL:
        case PoiCategory::BICYCLE_RENTAL: case PoiCategory::BICYCLE_REPAIR_STATION:
        case PoiCategory::VEHICLE_INSPECTION:
        case PoiCategory::SAUNA: case PoiCategory::HORSE_RIDING:
        case PoiCategory::BIRD_HIDE: case PoiCategory::MINIATURE_GOLF:
        case PoiCategory::SWIMMING_AREA: case PoiCategory::FIRE_WATER_POND:
        case PoiCategory::SHOP_SUPERMARKET: case PoiCategory::SHOP_CONVENIENCE:
        case PoiCategory::SHOP_CLOTHES: case PoiCategory::SHOP_MALL:
        case PoiCategory::SHOP_DEPARTMENT_STORE: case PoiCategory::SHOP_BAKERY:
        case PoiCategory::SHOP_BUTCHER: case PoiCategory::SHOP_HARDWARE:
        case PoiCategory::SHOP_ELECTRONICS: case PoiCategory::SHOP_FURNITURE:
        case PoiCategory::SHOP_JEWELRY: case PoiCategory::SHOP_BOOKS:
        case PoiCategory::SHOP_PET:
        case PoiCategory::WOOD: case PoiCategory::HEATH:
        case PoiCategory::SCRUB: case PoiCategory::WETLAND:
        case PoiCategory::GRASSLAND: case PoiCategory::NATURAL_WATER:
        case PoiCategory::VALLEY: case PoiCategory::RIDGE:
        case PoiCategory::SADDLE: case PoiCategory::GORGE:
        case PoiCategory::BARE_ROCK:
        case PoiCategory::MEADOW: case PoiCategory::ORCHARD:
        case PoiCategory::VINEYARD: case PoiCategory::FARMLAND:
        case PoiCategory::ALLOTMENTS: case PoiCategory::QUARRY:
        case PoiCategory::RESERVOIR: case PoiCategory::RECREATION_GROUND:
        case PoiCategory::MILITARY: case PoiCategory::RELIGIOUS_LANDUSE:
            return 3;
        // Generic rank-30 feature: below every POI dir until a wikipedia or
        // wikidata tag lifts it into poi/all.
        case PoiCategory::GENERIC_RANK30:
            return 4;
        default: return 0;
    }
}


static constexpr uint8_t POI_FLAG_WIKIPEDIA = 0x01;
static constexpr uint8_t POI_FLAG_WIKIDATA  = 0x02;
// Feature is a highway-class polygon (highway=pedestrian, footway, square,
// service yard). Nominatim assigns these rank_search=26 (road rank) rather
// than the default rank_search=30 for POIs, so the feature's own name is
// surfaced as the `road` field when it wins the reverse primary. Without
// the flag we can't tell Mexico City's "Constitution Square" (a
// highway=pedestrian relation with tourism=attraction, rank 26 in
// nominatim) apart from Buenos Aires's Obelisco (historic=monument with
// tourism=attraction, rank 30 in nominatim) — both land in our POI index
// as PoiCategory::ATTRACTION.
static constexpr uint8_t POI_FLAG_HIGHWAY   = 0x04;
// A secondary ring of a multi-ring POI, which the place-search index leaves
// out (server/src/lib.rs POI_FLAG_SECONDARY_RING). No build writes it yet,
// but no other bit of a file may take it.
static constexpr uint8_t POI_FLAG_SECONDARY_RING = 0x08;
// A generic rank-30 feature (PoiCategory::GENERIC_RANK30) of a tag
// address-levels.json ranks below 30 alone: a natural=* other than tree,
// spring, cave_entrance, geyser, hot_spring, rock and stone, a waterway=*,
// leisure=common or historic=neighbourhood. Nominatim makes no rank-30 row
// of it, so it is no feature near a point (the server's
// poi_is_reverse_feature), though it stays a POI near one, and one the
// place-search index names. A file's bit: while the build runs, the water
// bits hold it (POI_BUILD_BELOW_RANK30).
static constexpr uint8_t POI_FLAG_BELOW_RANK30 = 0x10;
static_assert((POI_FLAG_BELOW_RANK30 & (POI_FLAG_WIKIPEDIA | POI_FLAG_WIKIDATA | POI_FLAG_HIGHWAY |
                                        POI_FLAG_SECONDARY_RING)) == 0,
              "a file's POI flags must be bits of their own");
// While the build runs, a water object that takes its category from a
// rank-30 tag (poi_classify.h classify_poi) carries its water kind in these
// bits where the kind ships in a lower tier than the category: the dirs of
// the tiers in between ship it as that kind (poi_record_in_tier), which
// strips the bits, so no file holds them. 0 is none, else 1 + the kind's
// index in kPoiWaterKinds.
static constexpr uint8_t POI_FLAG_WATER_SHIFT = 3;
static constexpr uint8_t POI_FLAG_WATER_MASK  = 0x7 << POI_FLAG_WATER_SHIFT;
inline constexpr PoiCategory kPoiWaterKinds[] = {PoiCategory::NATURAL_WATER, PoiCategory::WETLAND, PoiCategory::BAY,
                                                 PoiCategory::GLACIER, PoiCategory::RESERVOIR};
// While the build runs, too: a POI that never takes the primary slot, a
// water multipolygon the relation scan takes by its own categories, or one
// that could whose holes its polygon leaves out (build_index.cpp), which
// the build links no parent street, so that the server's primary lane,
// which needs one, takes none of it (compute_poi_parent_streets).
static constexpr uint8_t POI_FLAG_PLACES_ONLY = 0x40;
// A POI whose importance took the sitelink count of its Wikidata item
// (reorder_pois), which the tier dirs holding one credit.
static constexpr uint8_t POI_FLAG_SITELINKS = 0x80;
// The flags no file holds.
static constexpr uint8_t POI_FLAG_BUILD_MASK = POI_FLAG_WATER_MASK | POI_FLAG_PLACES_ONLY | POI_FLAG_SITELINKS;

// The tier of a POI of `cat`: its category's, one more important (lower)
// with a wiki tag, but for tier 1.
inline uint8_t poi_tier_of(PoiCategory cat, uint8_t flags) {
    const uint8_t tier = poi_get_default_tier(cat);
    return (flags & (POI_FLAG_WIKIPEDIA | POI_FLAG_WIKIDATA)) && tier > 1 ? tier - 1 : tier;
}

// The water kind `flags` carry, UNKNOWN for none.
inline PoiCategory poi_water_of(uint8_t flags) {
    const unsigned at = (flags & POI_FLAG_WATER_MASK) >> POI_FLAG_WATER_SHIFT;
    return at == 0 || at > std::size(kPoiWaterKinds) ? PoiCategory::UNKNOWN : kPoiWaterKinds[at - 1];
}

// `flags` carrying the water kind `water`, one of kPoiWaterKinds.
inline uint8_t poi_flags_with_water(uint8_t flags, PoiCategory water) {
    for (size_t k = 0; k < std::size(kPoiWaterKinds); k++)
        if (kPoiWaterKinds[k] == water)
            return static_cast<uint8_t>((flags & ~POI_FLAG_WATER_MASK) | ((k + 1) << POI_FLAG_WATER_SHIFT));
    throw std::logic_error("poi_flags_with_water: no water kind");
}

// While the build runs, a generic feature of a lower rank (POI_FLAG_BELOW_RANK30) has every
// water bit set, a value no water kind takes: a water object that yields to a rank-30 tag is
// a rank-30 POI, and one that stays water is of its kind (classify_poi), so no record needs
// both. poi_record_in_tier writes it as POI_FLAG_BELOW_RANK30.
static constexpr uint8_t POI_BUILD_BELOW_RANK30 = POI_FLAG_WATER_MASK;
static_assert(std::size(kPoiWaterKinds) < (POI_BUILD_BELOW_RANK30 >> POI_FLAG_WATER_SHIFT),
              "a water kind's bits must differ from a generic feature of a lower rank's");

// Whether `flags`, as the build holds them, mark a generic feature of a lower rank.
inline bool poi_build_below_rank30(uint8_t flags) {
    return (flags & POI_FLAG_WATER_MASK) == POI_BUILD_BELOW_RANK30;
}

inline const char* poi_category_label(PoiCategory cat) {
    switch (cat) {
        case PoiCategory::MUSEUM: return "museum"; case PoiCategory::ATTRACTION: return "attraction";
        case PoiCategory::VIEWPOINT: return "viewpoint"; case PoiCategory::THEME_PARK: return "theme_park";
        case PoiCategory::ZOO: return "zoo"; case PoiCategory::GALLERY: return "gallery";
        case PoiCategory::ARTWORK: return "artwork"; case PoiCategory::ALPINE_HUT: return "alpine_hut";
        case PoiCategory::AQUARIUM: return "aquarium"; case PoiCategory::CAMP_SITE: return "camp_site";
        case PoiCategory::PICNIC_SITE: return "picnic_site"; case PoiCategory::RESORT: return "resort";
        case PoiCategory::CASTLE: return "castle"; case PoiCategory::MONUMENT: return "monument";
        case PoiCategory::RUINS: return "ruins"; case PoiCategory::ARCHAEOLOGICAL_SITE: return "archaeological_site";
        case PoiCategory::MEMORIAL: return "memorial"; case PoiCategory::BATTLEFIELD: return "battlefield";
        case PoiCategory::FORT: return "fort"; case PoiCategory::SHIP: return "ship";
        case PoiCategory::WAYSIDE_CROSS: return "wayside_cross";
        case PoiCategory::WAYSIDE_SHRINE: return "wayside_shrine";
        case PoiCategory::CITY_GATE: return "city_gate";
        case PoiCategory::CITYWALLS: return "citywalls";
        case PoiCategory::BOUNDARY_STONE: return "boundary_stone";
        case PoiCategory::MILESTONE: return "milestone";
        case PoiCategory::HISTORIC_MINE: return "mine";
        case PoiCategory::HISTORIC_AIRCRAFT: return "aircraft";
        case PoiCategory::LOCOMOTIVE: return "locomotive";
        case PoiCategory::CANNON: return "cannon";
        case PoiCategory::TOMB: return "tomb";
        case PoiCategory::MANOR: return "manor";
        case PoiCategory::PLACE_OF_WORSHIP: return "place_of_worship"; case PoiCategory::UNIVERSITY: return "university";
        case PoiCategory::COLLEGE: return "college"; case PoiCategory::HOSPITAL: return "hospital";
        case PoiCategory::THEATRE: return "theatre"; case PoiCategory::CINEMA: return "cinema";
        case PoiCategory::LIBRARY: return "library"; case PoiCategory::MARKETPLACE: return "marketplace";
        case PoiCategory::EMBASSY: return "embassy"; case PoiCategory::FOUNTAIN: return "fountain";
        case PoiCategory::CASINO: return "casino"; case PoiCategory::CEMETERY: return "cemetery";
        case PoiCategory::FERRY_TERMINAL: return "ferry_terminal"; case PoiCategory::PLANETARIUM: return "planetarium";
        case PoiCategory::PRISON: return "prison";
        case PoiCategory::PARK: return "park"; case PoiCategory::NATURE_RESERVE: return "nature_reserve";
        case PoiCategory::STADIUM: return "stadium"; case PoiCategory::GARDEN: return "garden";
        case PoiCategory::WATER_PARK: return "water_park"; case PoiCategory::GOLF_COURSE: return "golf_course";
        case PoiCategory::MARINA: return "marina";
        case PoiCategory::PEAK: return "peak"; case PoiCategory::VOLCANO: return "volcano";
        case PoiCategory::BEACH: return "beach"; case PoiCategory::CAVE_ENTRANCE: return "cave_entrance";
        case PoiCategory::SPRING: return "spring"; case PoiCategory::WATERFALL: return "waterfall";
        case PoiCategory::GLACIER: return "glacier"; case PoiCategory::CLIFF: return "cliff";
        case PoiCategory::ARCH: return "arch"; case PoiCategory::HOT_SPRING: return "hot_spring";
        case PoiCategory::GEYSER: return "geyser"; case PoiCategory::BAY: return "bay";
        case PoiCategory::CAPE: return "cape"; case PoiCategory::ISLAND: return "island";
        case PoiCategory::WOOD: return "wood";
        case PoiCategory::HEATH: return "heath";
        case PoiCategory::SCRUB: return "scrub";
        case PoiCategory::WETLAND: return "wetland";
        case PoiCategory::GRASSLAND: return "grassland";
        case PoiCategory::NATURAL_WATER: return "water";
        case PoiCategory::VALLEY: return "valley";
        case PoiCategory::RIDGE: return "ridge";
        case PoiCategory::SADDLE: return "saddle";
        case PoiCategory::GORGE: return "gorge";
        case PoiCategory::BARE_ROCK: return "bare_rock";
        case PoiCategory::MEADOW: return "meadow";
        case PoiCategory::ORCHARD: return "orchard";
        case PoiCategory::VINEYARD: return "vineyard";
        case PoiCategory::FARMLAND: return "farmland";
        case PoiCategory::ALLOTMENTS: return "allotments";
        case PoiCategory::QUARRY: return "quarry";
        case PoiCategory::RESERVOIR: return "reservoir";
        case PoiCategory::RECREATION_GROUND: return "recreation_ground";
        case PoiCategory::MILITARY: return "military";
        case PoiCategory::RELIGIOUS_LANDUSE: return "religious";
        case PoiCategory::AERODROME: return "aerodrome"; case PoiCategory::STATION: return "station";
        case PoiCategory::TOWER: return "tower"; case PoiCategory::LIGHTHOUSE: return "lighthouse";
        case PoiCategory::WINDMILL: return "windmill"; case PoiCategory::BRIDGE: return "bridge";
        case PoiCategory::PIER: return "pier"; case PoiCategory::DAM: return "dam";
        case PoiCategory::OBSERVATORY: return "observatory";
        case PoiCategory::SILO: return "silo";
        case PoiCategory::CHIMNEY: return "chimney";
        case PoiCategory::WATERMILL: return "watermill";
        case PoiCategory::CATHEDRAL: return "cathedral"; case PoiCategory::PALACE: return "palace";
        case PoiCategory::NATIONAL_PARK: return "national_park"; case PoiCategory::PROTECTED_AREA: return "protected_area";
        case PoiCategory::WINERY: return "winery"; case PoiCategory::BREWERY: return "brewery";
        case PoiCategory::POWER_PLANT: return "power_plant";
        case PoiCategory::GOVERNMENT: return "government";
        case PoiCategory::SWIMMING_AREA: return "swimming_area";
        case PoiCategory::FIRE_WATER_POND: return "fire_water_pond";
        case PoiCategory::HOTEL: return "hotel";
        case PoiCategory::MOTEL: return "motel";
        case PoiCategory::GUEST_HOUSE: return "guest_house";
        case PoiCategory::HOSTEL: return "hostel";
        case PoiCategory::APARTMENT: return "apartment";
        case PoiCategory::CHALET: return "chalet";
        case PoiCategory::CARAVAN_SITE: return "caravan_site";
        case PoiCategory::INFORMATION: return "information";
        case PoiCategory::RESTAURANT: return "restaurant";
        case PoiCategory::CAFE: return "cafe";
        case PoiCategory::BAR: return "bar";
        case PoiCategory::PUB: return "pub";
        case PoiCategory::FAST_FOOD: return "fast_food";
        case PoiCategory::ICE_CREAM: return "ice_cream";
        case PoiCategory::BIERGARTEN: return "biergarten";
        case PoiCategory::FOOD_COURT: return "food_court";
        case PoiCategory::NIGHTCLUB: return "nightclub";
        case PoiCategory::SCHOOL: return "school";
        case PoiCategory::KINDERGARTEN: return "kindergarten";
        case PoiCategory::DRIVING_SCHOOL: return "driving_school";
        case PoiCategory::MUSIC_SCHOOL: return "music_school";
        case PoiCategory::PHARMACY: return "pharmacy";
        case PoiCategory::DOCTORS: return "doctors";
        case PoiCategory::DENTIST: return "dentist";
        case PoiCategory::CLINIC: return "clinic";
        case PoiCategory::VETERINARY: return "veterinary";
        case PoiCategory::BANK: return "bank";
        case PoiCategory::ATM: return "atm";
        case PoiCategory::BUREAU_DE_CHANGE: return "bureau_de_change";
        case PoiCategory::POST_OFFICE: return "post_office";
        case PoiCategory::POST_BOX: return "post_box";
        case PoiCategory::PARCEL_LOCKER: return "parcel_locker";
        case PoiCategory::POLICE: return "police";
        case PoiCategory::FIRE_STATION: return "fire_station";
        case PoiCategory::TOWNHALL: return "townhall";
        case PoiCategory::COURTHOUSE: return "courthouse";
        case PoiCategory::COMMUNITY_CENTRE: return "community_centre";
        case PoiCategory::SOCIAL_CENTRE: return "social_centre";
        case PoiCategory::PLAYGROUND: return "playground";
        case PoiCategory::PITCH: return "pitch";
        case PoiCategory::TRACK: return "track";
        case PoiCategory::FITNESS_STATION: return "fitness_station";
        case PoiCategory::FITNESS_CENTRE: return "fitness_centre";
        case PoiCategory::SHELTER: return "shelter";
        case PoiCategory::SPORTS_CENTRE: return "sports_centre";
        case PoiCategory::SPORTS_HALL: return "sports_hall";
        case PoiCategory::BUS_STOP: return "bus_stop";
        case PoiCategory::BUS_STATION: return "bus_station";
        case PoiCategory::TAXI: return "taxi";
        case PoiCategory::PARKING: return "parking";
        case PoiCategory::BICYCLE_PARKING: return "bicycle_parking";
        case PoiCategory::MOTORCYCLE_PARKING: return "motorcycle_parking";
        case PoiCategory::FUEL: return "fuel";
        case PoiCategory::CHARGING_STATION: return "charging_station";
        case PoiCategory::SHOP: return "shop";
        // Small amenities
        case PoiCategory::TOILETS: return "toilets";
        case PoiCategory::DRINKING_WATER: return "drinking_water";
        case PoiCategory::BENCH: return "bench";
        case PoiCategory::VENDING_MACHINE: return "vending_machine";
        case PoiCategory::WASTE_BASKET: return "waste_basket";
        case PoiCategory::RECYCLING: return "recycling";
        case PoiCategory::CLOCK: return "clock";
        case PoiCategory::TELEPHONE: return "telephone";
        case PoiCategory::BBQ: return "bbq";
        // Arts/culture + education extras
        case PoiCategory::ARTS_CENTRE: return "arts_centre";
        case PoiCategory::STUDIO: return "studio";
        case PoiCategory::LANGUAGE_SCHOOL: return "language_school";
        case PoiCategory::TRAINING: return "training";
        // Health extras
        case PoiCategory::NURSING_HOME: return "nursing_home";
        case PoiCategory::AMBULANCE_STATION: return "ambulance_station";
        case PoiCategory::HOSPICE: return "hospice";
        // Civic extras
        case PoiCategory::MONASTERY: return "monastery";
        case PoiCategory::GRAVE_YARD: return "grave_yard";
        // Transport extras
        case PoiCategory::CAR_WASH: return "car_wash";
        case PoiCategory::CAR_RENTAL: return "car_rental";
        case PoiCategory::BICYCLE_RENTAL: return "bicycle_rental";
        case PoiCategory::BICYCLE_REPAIR_STATION: return "bicycle_repair_station";
        case PoiCategory::VEHICLE_INSPECTION: return "vehicle_inspection";
        // Leisure extras
        case PoiCategory::SAUNA: return "sauna";
        case PoiCategory::HORSE_RIDING: return "horse_riding";
        case PoiCategory::BIRD_HIDE: return "bird_hide";
        case PoiCategory::MINIATURE_GOLF: return "miniature_golf";
        // Shop sub-types
        case PoiCategory::SHOP_SUPERMARKET: return "supermarket";
        case PoiCategory::SHOP_CONVENIENCE: return "convenience";
        case PoiCategory::SHOP_CLOTHES: return "clothes";
        case PoiCategory::SHOP_MALL: return "mall";
        case PoiCategory::SHOP_DEPARTMENT_STORE: return "department_store";
        case PoiCategory::SHOP_BAKERY: return "bakery";
        case PoiCategory::SHOP_BUTCHER: return "butcher";
        case PoiCategory::SHOP_HARDWARE: return "hardware";
        case PoiCategory::SHOP_ELECTRONICS: return "electronics";
        case PoiCategory::SHOP_FURNITURE: return "furniture";
        case PoiCategory::SHOP_JEWELRY: return "jewelry";
        case PoiCategory::SHOP_BOOKS: return "books";
        case PoiCategory::SHOP_PET: return "pet";
        // Named rank-30 feature that didn't match a specific
        // PoiCategory. "feature" reads better than "unknown" when
        // an occasional OSM tagging that we don't special-case
        // bubbles up to the client (e.g. a named office=ngo or
        // amenity=studio).
        case PoiCategory::GENERIC_RANK30: return "feature";
        default: return "unknown";
    }
}

inline uint8_t category_base_importance(PoiCategory cat) {
    switch (cat) {
        case PoiCategory::VOLCANO: case PoiCategory::GLACIER:
            return 40;
        case PoiCategory::AERODROME: case PoiCategory::NATIONAL_PARK:
        case PoiCategory::CATHEDRAL: case PoiCategory::PALACE:
        case PoiCategory::ISLAND:
            return 30;
        case PoiCategory::MUSEUM: case PoiCategory::ATTRACTION:
        case PoiCategory::CASTLE: case PoiCategory::STADIUM:
        case PoiCategory::UNIVERSITY:
        case PoiCategory::LIGHTHOUSE: case PoiCategory::WATERFALL:
            return 20;
        case PoiCategory::PARK: case PoiCategory::NATURE_RESERVE:
        case PoiCategory::BEACH: case PoiCategory::RUINS:
        case PoiCategory::FORT: case PoiCategory::MONUMENT:
        case PoiCategory::VIEWPOINT: case PoiCategory::DAM:
        case PoiCategory::OBSERVATORY: case PoiCategory::PROTECTED_AREA:
        case PoiCategory::MEMORIAL:
            return 15;
        case PoiCategory::PLACE_OF_WORSHIP: case PoiCategory::HOSPITAL:
        case PoiCategory::THEATRE: case PoiCategory::CAVE_ENTRANCE:
        case PoiCategory::BRIDGE: case PoiCategory::TOWER:
        case PoiCategory::PEAK: case PoiCategory::GOVERNMENT:
        // STATION demoted from 20 → 10: transit stations are landmarks
        // but subway entrances in dense cities otherwise shadow
        // actual tourist landmarks in the places[] list (72nd Street
        // beating Strawberry Fields at the Central Park memorial).
        case PoiCategory::STATION:
            return 10;
        case PoiCategory::LIBRARY: case PoiCategory::CINEMA:
        case PoiCategory::GALLERY:
        case PoiCategory::GARDEN: case PoiCategory::MARINA:
        case PoiCategory::PIER: case PoiCategory::SPRING:
        case PoiCategory::CAMP_SITE: case PoiCategory::BREWERY:
        case PoiCategory::WINERY:
            return 5;
        case PoiCategory::ARTWORK: case PoiCategory::PICNIC_SITE:
        case PoiCategory::FOUNTAIN: case PoiCategory::HOT_SPRING:
        case PoiCategory::CLIFF: case PoiCategory::ARCH:
        case PoiCategory::EMBASSY: case PoiCategory::CEMETERY:
            return 2;
        default: return 5;
    }
}

// Half-life distance: score decays to 50% at this distance
inline uint16_t category_reference_distance(PoiCategory cat) {
    switch (cat) {
        // Containment categories — ref_dist used for padding around polygon
        case PoiCategory::THEME_PARK: case PoiCategory::ZOO: case PoiCategory::CAMP_SITE:
        case PoiCategory::RESORT: case PoiCategory::BATTLEFIELD:
        case PoiCategory::UNIVERSITY: case PoiCategory::COLLEGE: case PoiCategory::HOSPITAL:
        case PoiCategory::MARKETPLACE: case PoiCategory::CEMETERY: case PoiCategory::PRISON:
        case PoiCategory::PARK: case PoiCategory::NATURE_RESERVE: case PoiCategory::STADIUM:
        case PoiCategory::GARDEN: case PoiCategory::WATER_PARK: case PoiCategory::GOLF_COURSE:
        case PoiCategory::BAY: case PoiCategory::ISLAND:
        case PoiCategory::AERODROME: case PoiCategory::NATIONAL_PARK:
        case PoiCategory::PROTECTED_AREA: case PoiCategory::POWER_PLANT:
            return 100;
        case PoiCategory::FOUNTAIN:
        case PoiCategory::ARTWORK: case PoiCategory::GEYSER: case PoiCategory::PICNIC_SITE:
        case PoiCategory::EMBASSY:
            return 50;
        case PoiCategory::MONUMENT: case PoiCategory::MEMORIAL:
            // Memorials / monuments (Strawberry Fields, JFK memorial,
            // Holocaust Memorial) are landmarks people recognise from
            // further away than a 50 m fountain. Extend their ref
            // distance to 100 m so they surface in places[] for
            // queries a short walk away, not just on top of them.
            return 100;
        case PoiCategory::MUSEUM: case PoiCategory::ATTRACTION: case PoiCategory::GALLERY:
        case PoiCategory::AQUARIUM: case PoiCategory::CASTLE: case PoiCategory::SHIP:
        case PoiCategory::PLACE_OF_WORSHIP: case PoiCategory::THEATRE: case PoiCategory::CINEMA:
        case PoiCategory::LIBRARY: case PoiCategory::CASINO: case PoiCategory::PLANETARIUM:
        case PoiCategory::CATHEDRAL: case PoiCategory::PALACE:
        case PoiCategory::GOVERNMENT:
            return 100;
        case PoiCategory::ALPINE_HUT: case PoiCategory::RUINS: case PoiCategory::ARCHAEOLOGICAL_SITE:
        case PoiCategory::FORT: case PoiCategory::FERRY_TERMINAL: case PoiCategory::MARINA:
        case PoiCategory::SPRING: case PoiCategory::HOT_SPRING:
        case PoiCategory::STATION: case PoiCategory::WINDMILL: case PoiCategory::BRIDGE:
        case PoiCategory::PIER: case PoiCategory::WINERY: case PoiCategory::BREWERY:
            return 200;
        case PoiCategory::VIEWPOINT: case PoiCategory::TOWER: case PoiCategory::DAM:
        case PoiCategory::OBSERVATORY:
            return 300;
        case PoiCategory::BEACH: case PoiCategory::CAVE_ENTRANCE: case PoiCategory::WATERFALL:
        case PoiCategory::CLIFF: case PoiCategory::ARCH: case PoiCategory::LIGHTHOUSE:
            return 500;
        case PoiCategory::PEAK:    return 1000;
        case PoiCategory::VOLCANO: case PoiCategory::GLACIER: case PoiCategory::CAPE:
            return 1500;
        default: return 100;
    }
}

// Hard max distance before importance scaling (0 = containment only)
inline uint16_t category_max_distance(PoiCategory cat) {
    switch (cat) {
        // Containment only
        case PoiCategory::THEME_PARK: case PoiCategory::ZOO: case PoiCategory::CAMP_SITE:
        case PoiCategory::RESORT: case PoiCategory::BATTLEFIELD:
        case PoiCategory::UNIVERSITY: case PoiCategory::COLLEGE: case PoiCategory::HOSPITAL:
        case PoiCategory::MARKETPLACE: case PoiCategory::CEMETERY: case PoiCategory::PRISON:
        case PoiCategory::PARK: case PoiCategory::NATURE_RESERVE: case PoiCategory::STADIUM:
        case PoiCategory::GARDEN: case PoiCategory::WATER_PARK: case PoiCategory::GOLF_COURSE:
        case PoiCategory::BAY: case PoiCategory::ISLAND:
        case PoiCategory::AERODROME: case PoiCategory::NATIONAL_PARK:
        case PoiCategory::PROTECTED_AREA: case PoiCategory::POWER_PLANT:
            return 0;
        // Tiny
        case PoiCategory::ARTWORK: case PoiCategory::FOUNTAIN:
        case PoiCategory::PICNIC_SITE: case PoiCategory::GEYSER:
        case PoiCategory::EMBASSY:
            return 200;
        // Memorials / monuments get landmark-scale max (500 m) since
        // their names travel further than the fountain-tier tiny POIs.
        case PoiCategory::MEMORIAL: case PoiCategory::MONUMENT:
            return 500;
        // Small
        case PoiCategory::MUSEUM: case PoiCategory::ATTRACTION: case PoiCategory::CASTLE:
        case PoiCategory::CATHEDRAL: case PoiCategory::PALACE: case PoiCategory::THEATRE:
        case PoiCategory::CINEMA: case PoiCategory::LIBRARY: case PoiCategory::GALLERY:
        case PoiCategory::AQUARIUM: case PoiCategory::CASINO: case PoiCategory::PLANETARIUM:
        case PoiCategory::SHIP: case PoiCategory::PLACE_OF_WORSHIP:
        case PoiCategory::ALPINE_HUT: case PoiCategory::RUINS:
        case PoiCategory::ARCHAEOLOGICAL_SITE: case PoiCategory::STATION:
        case PoiCategory::FERRY_TERMINAL: case PoiCategory::MARINA:
        case PoiCategory::WINDMILL: case PoiCategory::WINERY: case PoiCategory::BREWERY:
        case PoiCategory::FORT: case PoiCategory::SPRING: case PoiCategory::HOT_SPRING:
        case PoiCategory::BRIDGE: case PoiCategory::PIER: case PoiCategory::GOVERNMENT:
            return 500;
        // Medium
        case PoiCategory::VIEWPOINT: case PoiCategory::TOWER: case PoiCategory::DAM:
        case PoiCategory::OBSERVATORY: case PoiCategory::BEACH: case PoiCategory::CAVE_ENTRANCE:
        case PoiCategory::WATERFALL: case PoiCategory::CLIFF: case PoiCategory::ARCH:
        case PoiCategory::LIGHTHOUSE:
            return 1000;
        // Large
        case PoiCategory::PEAK: case PoiCategory::VOLCANO: case PoiCategory::GLACIER:
        case PoiCategory::CAPE:
            return 3000;
        default: return 500;
    }
}

struct PoiClassification {
    PoiCategory category;
    uint8_t tier;
    uint8_t flags;
    // The water kind of a water object that takes its category from a
    // rank-30 tag instead (poi_classify.h classify_poi), UNKNOWN for any
    // other.
    PoiCategory water = PoiCategory::UNKNOWN;
    const char* class_key = nullptr;
};

struct PoiRecord {
    float lat;                  // exact coord for points, centroid for polygons
    float lng;
    uint32_t vertex_offset;     // into poi_vertices.bin (NO_DATA for point POIs)
    uint32_t vertex_count;      // 0 for points, >0 for polygons
    uint32_t name_id;           // offset into strings.bin
    uint8_t category;           // PoiCategory
    uint8_t tier;               // 1=major, 2=notable, 3=everything
    uint8_t flags;              // POI_FLAG_WIKIPEDIA, POI_FLAG_WIKIDATA
    uint8_t importance = 0;     // 0-255, computed at build time
    // Name offset of the nearest named street to this POI, pre-
    // computed at build time. Used by the server's query_geo to
    // populate the `road` field when the POI wins primary-feature
    // selection. Mirrors Nominatim's parent_place_id chain from a
    // rank-30 POI back to its rank-26 road. NO_DATA
    // if no named street was found in the POI's search radius.
    uint32_t parent_street_id = NO_DATA;
    // String offset of the POI's calculated postcode, inherited
    // from the smallest containing boundary=postal_code polygon
    // at build time. Mirrors Nominatim's placex.postcode chain
    // for rank-30 POI rows whose postcode comes via their
    // parent_place_id. Used by the server's resolve_postcode as
    // a primary-feature tier when the POI wins selection.
    // NO_DATA if the POI isn't inside any postal
    // boundary.
    uint32_t parent_postcode_id = NO_DATA;
    // Polygon id of the smallest admin boundary (admin_level 2 to
    // kMaxPoiParentLevel, 10) containing the POI. The server walks admin_parents from here
    // to get the POI's administrative chain — a cheap approximation
    // of Nominatim's parent_place_id walk. Used to demote POIs
    // whose chain doesn't match the query's admin chain (e.g. a
    // cross-border landmark that happens to be spatially nearest).
    // NO_DATA if no containing admin polygon found.
    uint32_t parent_poly_id = NO_DATA;
    // For polygon POIs (vertex_count > 0), vertex_offset is a BYTE
    // offset into poi_vertices.bin pointing to a 10-byte polygon
    // header (encoding tag + bbox_min_lat/lng) followed by the
    // packed vertex bytes.  Point POIs have vertex_count = 0 and
    // vertex_offset = NO_DATA; no header is written for them.
};
static_assert(sizeof(PoiRecord) == 36, "PoiRecord must be 36 bytes");

// The lowest tier whose dir ships `pr`: its own, or its water kind's where
// it carries one (POI_FLAG_WATER_MASK).
inline uint8_t poi_lowest_tier(const PoiRecord& pr) {
    const PoiCategory water = poi_water_of(pr.flags);
    if (water == PoiCategory::UNKNOWN) return pr.tier;

    const uint8_t tier = poi_tier_of(water, pr.flags);
    return tier < pr.tier ? tier : pr.tier;
}

// `pr` as the dir of the POIs of tier <= max_tier writes it: as its water
// kind, of that kind's tier and of `water_importance`, its importance as
// that kind (ParsedData::poi_water_importance), where the dir ships the kind
// but not its own category, with the rest of it as it is; without the flags
// of the build alone (POI_FLAG_BUILD_MASK) in any dir, a generic feature of
// a lower rank's (POI_BUILD_BELOW_RANK30) written as POI_FLAG_BELOW_RANK30. A reservoir point
// there, which stands in for a POI the dir doesn't ship, links no parent
// street, so that it never takes the primary slot, which the server lets a
// RESERVOIR point take where it has one.
inline PoiRecord poi_record_in_tier(PoiRecord pr, uint8_t max_tier, uint8_t water_importance = 0) {
    const PoiCategory water = poi_water_of(pr.flags);
    const bool below_rank30 = poi_build_below_rank30(pr.flags);
    pr.flags &= static_cast<uint8_t>(~POI_FLAG_BUILD_MASK);
    if (below_rank30) pr.flags |= POI_FLAG_BELOW_RANK30;
    if (water != PoiCategory::UNKNOWN && pr.tier > max_tier) {
        pr.category = static_cast<uint8_t>(water);
        pr.tier = poi_tier_of(water, pr.flags);
        if (water_importance) pr.importance = water_importance;
        if (water == PoiCategory::RESERVOIR && pr.vertex_count == 0) pr.parent_street_id = NO_DATA;
    }
    return pr;
}

// Collected POI relation data for parallel polygon assembly
struct CollectedPoiRelation {
    int64_t id;
    PoiCategory category;
    uint8_t tier;
    uint8_t flags;
    uint32_t qid;
    std::string name;
    // Relation-level addr tags. When set, the polygon-assembly pass
    // also emits an AddrPoint with the polygon geometry so that the
    // relation's addr:street / addr:housenumber / addr:postcode win
    // as the primary feature's road/house_number/postcode fields.
    // Mirrors Nominatim's rank-30 row creation for relations with
    // addr:housenumber (e.g. White House R19761182). Its addr:city goes
    // with the point too.
    std::string addr_housenumber;
    std::string addr_street;
    std::string addr_postcode;
    std::string addr_city;
    std::vector<std::pair<int64_t, std::string>> members; // (way_id, role)
};

// Search tag files share these wire constants with place-search.
inline constexpr char TAG_KEY_SEP = '\x1f', TAG_PAIR_SEP = '\x1e';
inline constexpr size_t TAG_ARENA_BLOCK_BYTES = size_t(1) << 20;
inline constexpr uint32_t TAG_BLOB_BLOCK_ROWS = 4096;
inline constexpr char EN_NAMES_DIR[] = "names-en", ALL_NAMES_DIR[] = "names-all";
inline constexpr size_t STR_TIER_TAGS_CORE = 5, STR_TIER_TAGS_POI = 6;
