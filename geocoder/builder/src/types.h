#pragma once

#include <cstdint>
#include <vector>

// --- Binary format structs (must match server's index reader) ---

struct WayHeader {
    uint32_t node_offset;
    uint8_t node_count;
    uint32_t name_id;
};

struct AddrPoint {
    float lat;
    float lng;
    uint32_t housenumber_id;
    uint32_t street_id;
    uint32_t parent_way_id;   // nearest street way index (for housenumber refinement)
    uint32_t vertex_offset;   // into addr_vertices.bin (NO_DATA for node-sourced points)
    uint32_t vertex_count;    // 0 for nodes, >0 for building polygons
};

// Postcode centroid: one per unique postcode string, stored in
// postcode_centroids.bin. Matches Nominatim's location_postcode table.
struct PostcodeCentroid {
    float lat;
    float lng;
    uint32_t postcode_id;  // offset into strings.bin
    uint16_t country_code;
    uint16_t _pad = 0;
};

struct InterpWay {
    uint32_t node_offset;
    uint8_t node_count;
    uint8_t _pad1 = 0, _pad2 = 0, _pad3 = 0; // explicit padding
    uint32_t street_id;
    uint32_t start_number;
    uint32_t end_number;
    uint8_t interpolation;
    uint8_t _pad4 = 0, _pad5 = 0, _pad6 = 0; // explicit padding
};

// Place type override for admin polygons, derived from either a place/
// linked_place tag on the boundary itself, or the place type of a
// label-role member node (Nominatim's find_linked_place flow).
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
};

struct AdminPolygon {
    uint32_t vertex_offset;
    uint32_t vertex_count;
    uint32_t name_id;
    uint8_t admin_level;
    uint8_t place_type_override = 0; // AdminPlaceType — overrides admin_level for field mapping
    uint8_t _pad2 = 0, _pad3 = 0;
    float area;
    uint16_t country_code;
    uint16_t _pad4 = 0;
};

struct NodeCoord {
    float lat;
    float lng;
};

// Sorted cell-item pair for direct entry writing (avoids hash map)
struct CellItemPair {
    uint64_t cell_id;
    uint32_t item_id;
};

// --- Constants ---

static const uint32_t INTERIOR_FLAG = 0x80000000u;
static const uint32_t ID_MASK       = 0x7FFFFFFFu;
static const uint32_t NO_DATA       = 0xFFFFFFFFu;

// Simplification mode
enum class SimplifyMode { MaxVertices, ErrorBounded };

// Processing limits
static constexpr size_t MAX_BLOCK_QUEUE         = 64;    // producer-consumer queue depth
static constexpr int    MAX_NODE_COUNT          = 255;   // uint8_t node_count
static constexpr int    MAX_VERTEX_COUNT        = 0x7FFFFFFF; // uint32_t vertex_count
static constexpr int    MAX_S2_CELLS_PER_POLY   = 200;   // S2RegionCoverer max_cells
static constexpr int    BACKTRACK_CALL_BUDGET   = 100000; // per-ring backtracking limit
static constexpr int    BACKTRACK_TOTAL_BUDGET  = 500000; // per-relation backtracking limit
static constexpr int    BACKTRACK_MAX_DEPTH     = 200;    // recursion depth limit
static constexpr size_t MAX_SUBWAYS_FOR_RETRY   = 30;     // only retry small relations
static constexpr size_t MAX_NODE_ID_DEFAULT     = 15000000000ULL; // dense index capacity

// --- Index mode ---

enum class IndexMode { Full, NoAddresses, AdminOnly };

// --- Deferred S2 work items ---

struct DeferredWay {
    uint32_t way_id;
    uint32_t node_offset;
    uint8_t node_count;
};

struct DeferredInterp {
    uint32_t interp_id;
    uint32_t node_offset;
    uint8_t node_count;
};

// Collected relation data for parallel admin assembly
struct CollectedRelation {
    int64_t id;
    int64_t label_node_id = -1;  // role=label place node (Nominatim's primary link target)
    uint8_t admin_level;
    // Fallback place-type override from the boundary's own tags
    // (linked_place / place). Nominatim's find_linked_place tries label
    // role → wikidata → place-type match → name match; the boundary's
    // own place tag is only step 3, so we store it as a *fallback*
    // applied after label / wikidata have been checked.
    uint8_t fallback_place_type = 0;  // AdminPlaceType
    std::string name;
    std::string country_code;
    bool is_postal;
    std::vector<std::pair<int64_t, std::string>> members; // (way_id, role)
};

// boundary=census / boundary=census_district relations carrying a
// postal_code tag. Used at build time to inherit postcodes to
// addr_points and ways inside the CDP — mirroring Nominatim's
// calculated_postcode propagation through parent_place_id. These
// polygons are discarded after the inheritance pass; they never
// appear in admin_polygons.bin / postal_polygons.bin.
struct CdpPostcodeRelation {
    int64_t id;
    std::string postcode;
    std::vector<std::pair<int64_t, std::string>> members; // (way_id, role)
};

struct CdpPostcodePoly {
    std::vector<std::pair<double,double>> vertices;  // outer ring, (lat, lng)
    std::string postcode;
    double min_lat, max_lat, min_lng, max_lng;       // axis-aligned bbox
};

// --- Place nodes (settlements: city, town, village, suburb, etc.) ---

enum class PlaceType : uint8_t {
    CITY = 0, TOWN = 1, VILLAGE = 2, SUBURB = 3,
    HAMLET = 4, NEIGHBOURHOOD = 5, QUARTER = 6,
    // These are only used for the label-role lookup on admin boundaries —
    // they never end up in place_nodes.bin itself. A label member with
    // place=state/province/region/etc. lets us honour Nominatim's
    // get_label_tag() fallthrough to extratags['linked_place'].
    STATE = 7, PROVINCE = 8, REGION = 9, COUNTY = 10, DISTRICT = 11,
    BOROUGH = 12,
    UNKNOWN = 255
};

struct PlaceNode {
    float lat;
    float lng;
    uint32_t name_id;       // into strings.bin
    uint8_t place_type;     // PlaceType
    uint8_t _pad1 = 0;
    uint16_t _pad2 = 0;
    // Smallest admin polygon (by area) that contains this place node's
    // centroid. Used to gate nearest-place fallback lookups at query
    // time: a quarter/neighbourhood candidate is only returned if the
    // query point is inside the polygon referenced here. Mirrors
    // Nominatim's insert_addresslines containment check at indexing.
    // 0xFFFFFFFF means "unknown / no containing admin polygon".
    uint32_t parent_poly_id = 0xFFFFFFFF;
};  // 20 bytes

// --- POI (Points of Interest) ---

enum class PoiCategory : uint8_t {
    // tourism
    MUSEUM = 0, ATTRACTION = 1, VIEWPOINT = 2, THEME_PARK = 3, ZOO = 4,
    GALLERY = 5, ARTWORK = 6, ALPINE_HUT = 7, AQUARIUM = 8, CAMP_SITE = 9,
    PICNIC_SITE = 10, RESORT = 11,
    // historic
    CASTLE = 20, MONUMENT = 21, RUINS = 22, ARCHAEOLOGICAL_SITE = 23,
    MEMORIAL = 24, BATTLEFIELD = 25, FORT = 26, SHIP = 27,
    // amenity
    PLACE_OF_WORSHIP = 40, UNIVERSITY = 41, COLLEGE = 42, HOSPITAL = 43,
    THEATRE = 44, CINEMA = 45, LIBRARY = 46, MARKETPLACE = 47, EMBASSY = 48,
    FOUNTAIN = 49, CASINO = 50, CEMETERY = 51, FERRY_TERMINAL = 52,
    PLANETARIUM = 53, PRISON = 54,
    // leisure
    PARK = 60, NATURE_RESERVE = 61, STADIUM = 62, GARDEN = 63,
    WATER_PARK = 64, GOLF_COURSE = 65, MARINA = 66,
    // natural
    PEAK = 80, VOLCANO = 81, BEACH = 82, CAVE_ENTRANCE = 83, SPRING = 84,
    WATERFALL = 85, GLACIER = 86, CLIFF = 87, ARCH = 88, HOT_SPRING = 89,
    GEYSER = 90, BAY = 91, CAPE = 92, ISLAND = 93,
    // aeroway
    AERODROME = 100,
    // railway
    STATION = 105,
    // man_made
    TOWER = 110, LIGHTHOUSE = 111, WINDMILL = 112, BRIDGE = 113, PIER = 114,
    DAM = 115, OBSERVATORY = 116,
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

    // Generic rank-30 node (amenity/shop/tourism/historic/leisure/man_made/
    // craft/office/waterway/natural/aeroway/railway/power) that didn't
    // match a specific PoiCategory above. Mirrors Nominatim's reverse.py
    // DataLayer.POI filter (class_ NOT IN ('place', 'building'),
    // rank_search==30, geometry not line-like) — every such node is a
    // primary-feature candidate in _find_closest_street_or_pois, which
    // drives the Moscow vending_machine / SF waste_basket / Sydney
    // toilets / Paris clock primary selections. Typically unnamed; we
    // still store the category so the server can surface the POI's
    // parent_street as the road.
    UNNAMED_RANK30 = 170,

    UNKNOWN = 255
};

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
            return 3;
        // Generic rank-30 unnamed node — lowest priority for display/
        // ranking but eligible as a primary-feature candidate.
        case PoiCategory::UNNAMED_RANK30:
            return 4;
        default: return 0;
    }
}

// Proximity radius in meters (0 = polygon containment only)
inline uint16_t poi_get_proximity_meters(PoiCategory cat) {
    switch (cat) {
        // polygon containment (0m)
        case PoiCategory::THEME_PARK: case PoiCategory::ZOO: case PoiCategory::CAMP_SITE:
        case PoiCategory::RESORT: case PoiCategory::BATTLEFIELD:
        case PoiCategory::UNIVERSITY: case PoiCategory::COLLEGE: case PoiCategory::HOSPITAL:
        case PoiCategory::MARKETPLACE: case PoiCategory::CEMETERY: case PoiCategory::PRISON:
        case PoiCategory::PARK: case PoiCategory::NATURE_RESERVE: case PoiCategory::STADIUM:
        case PoiCategory::GARDEN: case PoiCategory::WATER_PARK: case PoiCategory::GOLF_COURSE:
        case PoiCategory::BAY: case PoiCategory::ISLAND:
        case PoiCategory::AERODROME:
        case PoiCategory::NATIONAL_PARK: case PoiCategory::PROTECTED_AREA:
        case PoiCategory::POWER_PLANT:
            return 0;
        // tiny (50m)
        case PoiCategory::MONUMENT: case PoiCategory::MEMORIAL: case PoiCategory::FOUNTAIN:
        case PoiCategory::ARTWORK: case PoiCategory::GEYSER: case PoiCategory::PICNIC_SITE:
            return 50;
        // small (100m)
        case PoiCategory::MUSEUM: case PoiCategory::ATTRACTION: case PoiCategory::GALLERY:
        case PoiCategory::AQUARIUM: case PoiCategory::CASTLE: case PoiCategory::SHIP:
        case PoiCategory::PLACE_OF_WORSHIP: case PoiCategory::THEATRE: case PoiCategory::CINEMA:
        case PoiCategory::LIBRARY: case PoiCategory::EMBASSY: case PoiCategory::CASINO:
        case PoiCategory::PLANETARIUM:
        case PoiCategory::CATHEDRAL: case PoiCategory::PALACE:
        case PoiCategory::GOVERNMENT:
            return 100;
        // medium (200m)
        case PoiCategory::ALPINE_HUT: case PoiCategory::RUINS: case PoiCategory::ARCHAEOLOGICAL_SITE:
        case PoiCategory::FORT: case PoiCategory::FERRY_TERMINAL: case PoiCategory::MARINA:
        case PoiCategory::SPRING: case PoiCategory::HOT_SPRING:
        case PoiCategory::STATION: case PoiCategory::WINDMILL: case PoiCategory::BRIDGE:
        case PoiCategory::PIER: case PoiCategory::WINERY: case PoiCategory::BREWERY:
            return 200;
        // medium-large (300m)
        case PoiCategory::VIEWPOINT: case PoiCategory::TOWER: case PoiCategory::DAM:
        case PoiCategory::OBSERVATORY:
            return 300;
        // large (500m)
        case PoiCategory::BEACH: case PoiCategory::CAVE_ENTRANCE: case PoiCategory::WATERFALL:
        case PoiCategory::CLIFF: case PoiCategory::ARCH: case PoiCategory::LIGHTHOUSE:
            return 500;
        // very large (1000m+)
        case PoiCategory::PEAK:    return 2000;
        case PoiCategory::VOLCANO: return 3000;
        case PoiCategory::GLACIER: return 1000;
        case PoiCategory::CAPE:    return 1000;
        default: return 100;
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
        case PoiCategory::AERODROME: return "aerodrome"; case PoiCategory::STATION: return "station";
        case PoiCategory::TOWER: return "tower"; case PoiCategory::LIGHTHOUSE: return "lighthouse";
        case PoiCategory::WINDMILL: return "windmill"; case PoiCategory::BRIDGE: return "bridge";
        case PoiCategory::PIER: return "pier"; case PoiCategory::DAM: return "dam";
        case PoiCategory::OBSERVATORY: return "observatory";
        case PoiCategory::CATHEDRAL: return "cathedral"; case PoiCategory::PALACE: return "palace";
        case PoiCategory::NATIONAL_PARK: return "national_park"; case PoiCategory::PROTECTED_AREA: return "protected_area";
        case PoiCategory::WINERY: return "winery"; case PoiCategory::BREWERY: return "brewery";
        case PoiCategory::POWER_PLANT: return "power_plant";
        case PoiCategory::GOVERNMENT: return "government";
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
    // rank-30 POI back to its rank-26 road. NO_DATA (0xFFFFFFFF)
    // if no named street was found in the POI's search radius.
    uint32_t parent_street_id = 0xFFFFFFFF;
    // String offset of the POI's calculated postcode, inherited
    // from the smallest containing boundary=postal_code polygon
    // at build time. Mirrors Nominatim's placex.postcode chain
    // for rank-30 POI rows whose postcode comes via their
    // parent_place_id. Used by the server's resolve_postcode as
    // a primary-feature tier when the POI wins selection.
    // NO_DATA (0xFFFFFFFF) if the POI isn't inside any postal
    // boundary.
    uint32_t parent_postcode_id = 0xFFFFFFFF;
    // Polygon id of the smallest admin boundary (admin_level <= 10)
    // containing the POI. The server walks admin_parents from here
    // to get the POI's administrative chain — a cheap approximation
    // of Nominatim's parent_place_id walk. Used to demote POIs
    // whose chain doesn't match the query's admin chain (e.g. a
    // cross-border landmark that happens to be spatially nearest).
    // NO_DATA (0xFFFFFFFF) if no containing admin polygon found.
    uint32_t parent_poly_id = 0xFFFFFFFF;
};

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
    // addr:housenumber (e.g. White House R19761182).
    std::string addr_housenumber;
    std::string addr_street;
    std::string addr_postcode;
    std::vector<std::pair<int64_t, std::string>> members; // (way_id, role)
};

struct DeferredPoi {
    uint32_t poi_id;
    uint32_t vertex_offset;
    uint32_t vertex_count;
};
