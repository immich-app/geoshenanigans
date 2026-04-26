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

// Vertex encoding tag: picks the bytes-per-vertex / precision tradeoff
// for this polygon.  Builder picks the finest scale that fits the
// polygon's bbox; reader decodes per-vertex via the inverse scale.
//
// Stored values per vertex are unsigned deltas relative to bbox_min:
//   v_lat = bbox_min_lat + (stored_dlat * scale)
//   v_lng = bbox_min_lng + (stored_dlng * scale)
//
// All schemes encode vertex_offset as a BYTE offset into the vertices
// file (not a vertex index) since the per-vertex stride varies.
enum class VertexEncoding : uint8_t {
    U16_1M    = 0,  // u16 @ 1e-5° (1.1 m), fits 73 km × 73 km bbox  → suburbs/cities
    U16_11M   = 1,  // u16 @ 1e-4° (11 m),  fits 730 km × 730 km     → states/regions
    U16_011M  = 2,  // u16 @ 1e-6° (11 cm), fits 7.3 km × 7.3 km     → POI buildings
    U32_1CM   = 3,  // u32 @ 1e-7° (1.1 cm), fits any polygon        → countries / fallback
    F32       = 4,  // raw f32 lat/lng (current format)              → escape hatch
};
static constexpr uint8_t VERTEX_STRIDE[5] = {4, 4, 4, 8, 8};

struct AdminPolygon {
    uint32_t vertex_offset;       // BYTE offset into admin_vertices.bin
    uint32_t vertex_count;
    uint32_t name_id;
    uint8_t admin_level;
    uint8_t place_type_override = 0; // AdminPlaceType — overrides admin_level for field mapping
    uint8_t encoding = 4;         // VertexEncoding (default F32 for legacy code paths)
    uint8_t _pad3 = 0;
    float area;
    uint16_t country_code;
    uint16_t _pad4 = 0;
    // Reference point for delta-encoded schemes (encoding != F32).
    // Vertices reconstruct as bbox_min + (stored_delta * scale_for_encoding).
    float bbox_min_lat = 0.0f;
    float bbox_min_lng = 0.0f;
};
static_assert(sizeof(AdminPolygon) == 32, "AdminPolygon must be 32 bytes");

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
        case PoiCategory::UNNAMED_RANK30: return "feature";
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
    // VertexEncoding tag (default F32 for point POIs / legacy data).
    // Builder picks the finest scale that fits the polygon's bbox.
    // vertex_offset is a BYTE offset into poi_vertices.bin when
    // encoding != F32 (variable per-vertex stride).
    uint8_t encoding = 4;
    uint8_t _pad_enc1 = 0, _pad_enc2 = 0, _pad_enc3 = 0;
    float bbox_min_lat = 0.0f;
    float bbox_min_lng = 0.0f;
};
static_assert(sizeof(PoiRecord) == 48, "PoiRecord must be 48 bytes");

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
