#pragma once
// POI classification: maps raw OSM tag values to (category, tier, flags).
// Extracted verbatim from build_index.cpp so the mapping — which encodes
// hundreds of Nominatim-parity decisions and evolves frequently — can be
// unit-tested (tests/test_poi_classify.cpp) without a full build+diff cycle.
#include <cstring>
#include <optional>
#include "types.h"

// POI classification: maps a node/way's raw OSM tag values to a
// (category, tier, flags) triple (PoiClassification, defined in types.h),
// or nullopt when nothing matches. Pure function of its tag arguments (no
// shared state) — lifted out of main()'s node/way passes, which call it
// identically.
inline std::optional<PoiClassification> classify_poi(
    const char* t_tourism, const char* t_historic, const char* t_boundary,
    const char* t_amenity, const char* t_leisure, const char* t_natural,
    const char* t_railway, const char* t_aeroway, const char* t_man_made,
    const char* t_building, const char* t_craft, const char* t_power,
    const char* t_place, const char* t_waterway, const char* t_office,
    const char* t_wikipedia, const char* t_wikidata, const char* t_highway,
    const char* t_shop, const char* t_landuse) {
    PoiCategory cat = PoiCategory::UNKNOWN;

    // tourism (highest priority)
    if (t_tourism) {
        if (std::strcmp(t_tourism, "museum") == 0) cat = PoiCategory::MUSEUM;
        else if (std::strcmp(t_tourism, "attraction") == 0) cat = PoiCategory::ATTRACTION;
        else if (std::strcmp(t_tourism, "viewpoint") == 0) cat = PoiCategory::VIEWPOINT;
        else if (std::strcmp(t_tourism, "theme_park") == 0) cat = PoiCategory::THEME_PARK;
        else if (std::strcmp(t_tourism, "zoo") == 0) cat = PoiCategory::ZOO;
        else if (std::strcmp(t_tourism, "gallery") == 0) cat = PoiCategory::GALLERY;
        else if (std::strcmp(t_tourism, "artwork") == 0) cat = PoiCategory::ARTWORK;
        else if (std::strcmp(t_tourism, "alpine_hut") == 0) cat = PoiCategory::ALPINE_HUT;
        else if (std::strcmp(t_tourism, "aquarium") == 0) cat = PoiCategory::AQUARIUM;
        else if (std::strcmp(t_tourism, "camp_site") == 0) cat = PoiCategory::CAMP_SITE;
        else if (std::strcmp(t_tourism, "picnic_site") == 0) cat = PoiCategory::PICNIC_SITE;
        else if (std::strcmp(t_tourism, "resort") == 0) cat = PoiCategory::RESORT;
        // Lodging — all variants fold into HOTEL so a hostel / apartment
        // / guesthouse shows as "hotel" class rather than "feature".
        else if (std::strcmp(t_tourism, "hotel") == 0) cat = PoiCategory::HOTEL;
        else if (std::strcmp(t_tourism, "motel") == 0) cat = PoiCategory::HOTEL;
        else if (std::strcmp(t_tourism, "guest_house") == 0) cat = PoiCategory::HOTEL;
        else if (std::strcmp(t_tourism, "hostel") == 0) cat = PoiCategory::HOTEL;
        else if (std::strcmp(t_tourism, "apartment") == 0) cat = PoiCategory::HOTEL;
        else if (std::strcmp(t_tourism, "chalet") == 0) cat = PoiCategory::HOTEL;
        else if (std::strcmp(t_tourism, "caravan_site") == 0) cat = PoiCategory::CAMP_SITE;
        else if (std::strcmp(t_tourism, "wilderness_hut") == 0) cat = PoiCategory::ALPINE_HUT;
        // Information / visitor-services
        else if (std::strcmp(t_tourism, "information") == 0) cat = PoiCategory::INFORMATION;
    }
    // historic
    if (cat == PoiCategory::UNKNOWN && t_historic) {
        if (std::strcmp(t_historic, "castle") == 0) cat = PoiCategory::CASTLE;
        else if (std::strcmp(t_historic, "monument") == 0) cat = PoiCategory::MONUMENT;
        else if (std::strcmp(t_historic, "ruins") == 0) cat = PoiCategory::RUINS;
        else if (std::strcmp(t_historic, "archaeological_site") == 0) cat = PoiCategory::ARCHAEOLOGICAL_SITE;
        else if (std::strcmp(t_historic, "memorial") == 0) cat = PoiCategory::MEMORIAL;
        else if (std::strcmp(t_historic, "battlefield") == 0) cat = PoiCategory::BATTLEFIELD;
        else if (std::strcmp(t_historic, "fort") == 0) cat = PoiCategory::FORT;
        else if (std::strcmp(t_historic, "ship") == 0) cat = PoiCategory::SHIP;
        else if (std::strcmp(t_historic, "wayside_cross") == 0) cat = PoiCategory::WAYSIDE_CROSS;
        else if (std::strcmp(t_historic, "wayside_shrine") == 0) cat = PoiCategory::WAYSIDE_SHRINE;
        else if (std::strcmp(t_historic, "city_gate") == 0) cat = PoiCategory::CITY_GATE;
        else if (std::strcmp(t_historic, "citywalls") == 0) cat = PoiCategory::CITYWALLS;
        else if (std::strcmp(t_historic, "boundary_stone") == 0) cat = PoiCategory::BOUNDARY_STONE;
        else if (std::strcmp(t_historic, "milestone") == 0) cat = PoiCategory::MILESTONE;
        else if (std::strcmp(t_historic, "mine") == 0) cat = PoiCategory::HISTORIC_MINE;
        else if (std::strcmp(t_historic, "aircraft") == 0) cat = PoiCategory::HISTORIC_AIRCRAFT;
        else if (std::strcmp(t_historic, "locomotive") == 0) cat = PoiCategory::LOCOMOTIVE;
        else if (std::strcmp(t_historic, "cannon") == 0) cat = PoiCategory::CANNON;
        else if (std::strcmp(t_historic, "tomb") == 0) cat = PoiCategory::TOMB;
        else if (std::strcmp(t_historic, "manor") == 0) cat = PoiCategory::MANOR;
    }
    // boundary
    if (cat == PoiCategory::UNKNOWN && t_boundary) {
        if (std::strcmp(t_boundary, "national_park") == 0) cat = PoiCategory::NATIONAL_PARK;
        else if (std::strcmp(t_boundary, "protected_area") == 0) cat = PoiCategory::PROTECTED_AREA;
        // `boundary=forest` (managed-forest admin) folds
        // into WOOD since both surface as "named forested
        // area" in the landmark line.
        else if (std::strcmp(t_boundary, "forest") == 0) cat = PoiCategory::WOOD;
    }
    // landuse — only named landuse polygons get indexed.
    // Note: classify_poi still falls through for unnamed
    // features to UNNAMED_RANK30 below, but landuse is
    // deliberately NOT in that fallback's trigger list so
    // unnamed residential/industrial zones don't flood
    // the index.
    if (cat == PoiCategory::UNKNOWN && t_landuse) {
        if (std::strcmp(t_landuse, "forest") == 0) cat = PoiCategory::WOOD;
        else if (std::strcmp(t_landuse, "meadow") == 0) cat = PoiCategory::MEADOW;
        else if (std::strcmp(t_landuse, "orchard") == 0) cat = PoiCategory::ORCHARD;
        else if (std::strcmp(t_landuse, "vineyard") == 0) cat = PoiCategory::VINEYARD;
        else if (std::strcmp(t_landuse, "farmland") == 0) cat = PoiCategory::FARMLAND;
        else if (std::strcmp(t_landuse, "allotments") == 0) cat = PoiCategory::ALLOTMENTS;
        else if (std::strcmp(t_landuse, "quarry") == 0) cat = PoiCategory::QUARRY;
        else if (std::strcmp(t_landuse, "reservoir") == 0) cat = PoiCategory::RESERVOIR;
        else if (std::strcmp(t_landuse, "basin") == 0) cat = PoiCategory::RESERVOIR;
        else if (std::strcmp(t_landuse, "recreation_ground") == 0) cat = PoiCategory::RECREATION_GROUND;
        else if (std::strcmp(t_landuse, "military") == 0) cat = PoiCategory::MILITARY;
        else if (std::strcmp(t_landuse, "religious") == 0) cat = PoiCategory::RELIGIOUS_LANDUSE;
        else if (std::strcmp(t_landuse, "cemetery") == 0) cat = PoiCategory::CEMETERY;
        // residential / industrial / commercial / retail
        // landuse zones intentionally skipped — they're
        // admin-hierarchy concepts already covered by the
        // address chain (suburb / borough / city_district).
    }
    // amenity
    if (cat == PoiCategory::UNKNOWN && t_amenity) {
        if (std::strcmp(t_amenity, "place_of_worship") == 0) cat = PoiCategory::PLACE_OF_WORSHIP;
        else if (std::strcmp(t_amenity, "university") == 0) cat = PoiCategory::UNIVERSITY;
        else if (std::strcmp(t_amenity, "college") == 0) cat = PoiCategory::COLLEGE;
        else if (std::strcmp(t_amenity, "hospital") == 0) cat = PoiCategory::HOSPITAL;
        else if (std::strcmp(t_amenity, "theatre") == 0) cat = PoiCategory::THEATRE;
        else if (std::strcmp(t_amenity, "cinema") == 0) cat = PoiCategory::CINEMA;
        else if (std::strcmp(t_amenity, "library") == 0) cat = PoiCategory::LIBRARY;
        else if (std::strcmp(t_amenity, "marketplace") == 0) cat = PoiCategory::MARKETPLACE;
        else if (std::strcmp(t_amenity, "embassy") == 0) cat = PoiCategory::EMBASSY;
        else if (std::strcmp(t_amenity, "fountain") == 0) cat = PoiCategory::FOUNTAIN;
        else if (std::strcmp(t_amenity, "casino") == 0) cat = PoiCategory::CASINO;
        else if (std::strcmp(t_amenity, "cemetery") == 0) cat = PoiCategory::CEMETERY;
        else if (std::strcmp(t_amenity, "ferry_terminal") == 0) cat = PoiCategory::FERRY_TERMINAL;
        else if (std::strcmp(t_amenity, "planetarium") == 0) cat = PoiCategory::PLANETARIUM;
        else if (std::strcmp(t_amenity, "prison") == 0) cat = PoiCategory::PRISON;
        // Food & drink (each OSM value → distinct category)
        else if (std::strcmp(t_amenity, "restaurant") == 0) cat = PoiCategory::RESTAURANT;
        else if (std::strcmp(t_amenity, "cafe") == 0) cat = PoiCategory::CAFE;
        else if (std::strcmp(t_amenity, "bar") == 0) cat = PoiCategory::BAR;
        else if (std::strcmp(t_amenity, "pub") == 0) cat = PoiCategory::PUB;
        else if (std::strcmp(t_amenity, "fast_food") == 0) cat = PoiCategory::FAST_FOOD;
        else if (std::strcmp(t_amenity, "biergarten") == 0) cat = PoiCategory::BIERGARTEN;
        else if (std::strcmp(t_amenity, "food_court") == 0) cat = PoiCategory::FOOD_COURT;
        else if (std::strcmp(t_amenity, "ice_cream") == 0) cat = PoiCategory::ICE_CREAM;
        else if (std::strcmp(t_amenity, "nightclub") == 0) cat = PoiCategory::NIGHTCLUB;
        // Education
        else if (std::strcmp(t_amenity, "school") == 0) cat = PoiCategory::SCHOOL;
        else if (std::strcmp(t_amenity, "kindergarten") == 0) cat = PoiCategory::KINDERGARTEN;
        else if (std::strcmp(t_amenity, "driving_school") == 0) cat = PoiCategory::DRIVING_SCHOOL;
        else if (std::strcmp(t_amenity, "music_school") == 0) cat = PoiCategory::MUSIC_SCHOOL;
        // Health / care
        else if (std::strcmp(t_amenity, "pharmacy") == 0) cat = PoiCategory::PHARMACY;
        else if (std::strcmp(t_amenity, "doctors") == 0) cat = PoiCategory::DOCTORS;
        else if (std::strcmp(t_amenity, "dentist") == 0) cat = PoiCategory::DENTIST;
        else if (std::strcmp(t_amenity, "clinic") == 0) cat = PoiCategory::CLINIC;
        else if (std::strcmp(t_amenity, "veterinary") == 0) cat = PoiCategory::VETERINARY;
        // Finance
        else if (std::strcmp(t_amenity, "bank") == 0) cat = PoiCategory::BANK;
        else if (std::strcmp(t_amenity, "atm") == 0) cat = PoiCategory::ATM;
        else if (std::strcmp(t_amenity, "bureau_de_change") == 0) cat = PoiCategory::BUREAU_DE_CHANGE;
        // Mail / parcel
        else if (std::strcmp(t_amenity, "post_office") == 0) cat = PoiCategory::POST_OFFICE;
        else if (std::strcmp(t_amenity, "post_box") == 0) cat = PoiCategory::POST_BOX;
        else if (std::strcmp(t_amenity, "parcel_locker") == 0) cat = PoiCategory::PARCEL_LOCKER;
        // Emergency services
        else if (std::strcmp(t_amenity, "police") == 0) cat = PoiCategory::POLICE;
        else if (std::strcmp(t_amenity, "fire_station") == 0) cat = PoiCategory::FIRE_STATION;
        // Civic
        else if (std::strcmp(t_amenity, "townhall") == 0) cat = PoiCategory::TOWNHALL;
        else if (std::strcmp(t_amenity, "courthouse") == 0) cat = PoiCategory::COURTHOUSE;
        else if (std::strcmp(t_amenity, "community_centre") == 0) cat = PoiCategory::COMMUNITY_CENTRE;
        else if (std::strcmp(t_amenity, "social_centre") == 0) cat = PoiCategory::SOCIAL_CENTRE;
        // Transport
        else if (std::strcmp(t_amenity, "bus_station") == 0) cat = PoiCategory::BUS_STATION;
        else if (std::strcmp(t_amenity, "taxi") == 0) cat = PoiCategory::TAXI;
        else if (std::strcmp(t_amenity, "parking") == 0) cat = PoiCategory::PARKING;
        else if (std::strcmp(t_amenity, "bicycle_parking") == 0) cat = PoiCategory::BICYCLE_PARKING;
        else if (std::strcmp(t_amenity, "motorcycle_parking") == 0) cat = PoiCategory::MOTORCYCLE_PARKING;
        else if (std::strcmp(t_amenity, "fuel") == 0) cat = PoiCategory::FUEL;
        else if (std::strcmp(t_amenity, "charging_station") == 0) cat = PoiCategory::CHARGING_STATION;
        // Shelter-class
        else if (std::strcmp(t_amenity, "shelter") == 0) cat = PoiCategory::SHELTER;
        // Small named amenities
        else if (std::strcmp(t_amenity, "toilets") == 0) cat = PoiCategory::TOILETS;
        else if (std::strcmp(t_amenity, "drinking_water") == 0) cat = PoiCategory::DRINKING_WATER;
        else if (std::strcmp(t_amenity, "bench") == 0) cat = PoiCategory::BENCH;
        else if (std::strcmp(t_amenity, "vending_machine") == 0) cat = PoiCategory::VENDING_MACHINE;
        else if (std::strcmp(t_amenity, "waste_basket") == 0) cat = PoiCategory::WASTE_BASKET;
        else if (std::strcmp(t_amenity, "recycling") == 0) cat = PoiCategory::RECYCLING;
        else if (std::strcmp(t_amenity, "clock") == 0) cat = PoiCategory::CLOCK;
        else if (std::strcmp(t_amenity, "telephone") == 0) cat = PoiCategory::TELEPHONE;
        else if (std::strcmp(t_amenity, "bbq") == 0) cat = PoiCategory::BBQ;
        // Arts / culture / studio
        else if (std::strcmp(t_amenity, "arts_centre") == 0) cat = PoiCategory::ARTS_CENTRE;
        else if (std::strcmp(t_amenity, "studio") == 0) cat = PoiCategory::STUDIO;
        // Education extras
        else if (std::strcmp(t_amenity, "language_school") == 0) cat = PoiCategory::LANGUAGE_SCHOOL;
        else if (std::strcmp(t_amenity, "training") == 0) cat = PoiCategory::TRAINING;
        // Health extras
        else if (std::strcmp(t_amenity, "nursing_home") == 0) cat = PoiCategory::NURSING_HOME;
        else if (std::strcmp(t_amenity, "ambulance_station") == 0) cat = PoiCategory::AMBULANCE_STATION;
        else if (std::strcmp(t_amenity, "hospice") == 0) cat = PoiCategory::HOSPICE;
        // Religious extra / grave
        else if (std::strcmp(t_amenity, "monastery") == 0) cat = PoiCategory::MONASTERY;
        else if (std::strcmp(t_amenity, "grave_yard") == 0) cat = PoiCategory::GRAVE_YARD;
        // Vehicle services
        else if (std::strcmp(t_amenity, "car_wash") == 0) cat = PoiCategory::CAR_WASH;
        else if (std::strcmp(t_amenity, "car_rental") == 0) cat = PoiCategory::CAR_RENTAL;
        else if (std::strcmp(t_amenity, "bicycle_rental") == 0) cat = PoiCategory::BICYCLE_RENTAL;
        else if (std::strcmp(t_amenity, "bicycle_repair_station") == 0) cat = PoiCategory::BICYCLE_REPAIR_STATION;
        else if (std::strcmp(t_amenity, "vehicle_inspection") == 0) cat = PoiCategory::VEHICLE_INSPECTION;
    }
    // leisure
    if (cat == PoiCategory::UNKNOWN && t_leisure) {
        if (std::strcmp(t_leisure, "park") == 0) cat = PoiCategory::PARK;
        else if (std::strcmp(t_leisure, "nature_reserve") == 0) cat = PoiCategory::NATURE_RESERVE;
        else if (std::strcmp(t_leisure, "stadium") == 0) cat = PoiCategory::STADIUM;
        // Each OSM leisure value → distinct category.
        else if (std::strcmp(t_leisure, "sports_centre") == 0) cat = PoiCategory::SPORTS_CENTRE;
        else if (std::strcmp(t_leisure, "garden") == 0) cat = PoiCategory::GARDEN;
        else if (std::strcmp(t_leisure, "water_park") == 0) cat = PoiCategory::WATER_PARK;
        else if (std::strcmp(t_leisure, "golf_course") == 0) cat = PoiCategory::GOLF_COURSE;
        else if (std::strcmp(t_leisure, "marina") == 0) cat = PoiCategory::MARINA;
        else if (std::strcmp(t_leisure, "playground") == 0) cat = PoiCategory::PLAYGROUND;
        else if (std::strcmp(t_leisure, "fitness_station") == 0) cat = PoiCategory::FITNESS_STATION;
        else if (std::strcmp(t_leisure, "fitness_centre") == 0) cat = PoiCategory::FITNESS_CENTRE;
        else if (std::strcmp(t_leisure, "pitch") == 0) cat = PoiCategory::PITCH;
        else if (std::strcmp(t_leisure, "track") == 0) cat = PoiCategory::TRACK;
        else if (std::strcmp(t_leisure, "sports_hall") == 0) cat = PoiCategory::SPORTS_HALL;
        else if (std::strcmp(t_leisure, "sauna") == 0) cat = PoiCategory::SAUNA;
        else if (std::strcmp(t_leisure, "horse_riding") == 0) cat = PoiCategory::HORSE_RIDING;
        else if (std::strcmp(t_leisure, "bird_hide") == 0) cat = PoiCategory::BIRD_HIDE;
        else if (std::strcmp(t_leisure, "miniature_golf") == 0) cat = PoiCategory::MINIATURE_GOLF;
    }
    // natural
    if (cat == PoiCategory::UNKNOWN && t_natural) {
        if (std::strcmp(t_natural, "peak") == 0) cat = PoiCategory::PEAK;
        else if (std::strcmp(t_natural, "volcano") == 0) cat = PoiCategory::VOLCANO;
        else if (std::strcmp(t_natural, "beach") == 0) cat = PoiCategory::BEACH;
        else if (std::strcmp(t_natural, "cave_entrance") == 0) cat = PoiCategory::CAVE_ENTRANCE;
        else if (std::strcmp(t_natural, "spring") == 0) cat = PoiCategory::SPRING;
        else if (std::strcmp(t_natural, "wood") == 0) cat = PoiCategory::WOOD;
        else if (std::strcmp(t_natural, "heath") == 0) cat = PoiCategory::HEATH;
        else if (std::strcmp(t_natural, "scrub") == 0) cat = PoiCategory::SCRUB;
        else if (std::strcmp(t_natural, "wetland") == 0) cat = PoiCategory::WETLAND;
        else if (std::strcmp(t_natural, "grassland") == 0) cat = PoiCategory::GRASSLAND;
        else if (std::strcmp(t_natural, "water") == 0) cat = PoiCategory::NATURAL_WATER;
        else if (std::strcmp(t_natural, "valley") == 0) cat = PoiCategory::VALLEY;
        else if (std::strcmp(t_natural, "ridge") == 0) cat = PoiCategory::RIDGE;
        else if (std::strcmp(t_natural, "saddle") == 0) cat = PoiCategory::SADDLE;
        else if (std::strcmp(t_natural, "gorge") == 0) cat = PoiCategory::GORGE;
        else if (std::strcmp(t_natural, "bare_rock") == 0) cat = PoiCategory::BARE_ROCK;
        else if (std::strcmp(t_natural, "cliff") == 0) cat = PoiCategory::CLIFF;
        else if (std::strcmp(t_natural, "arch") == 0) cat = PoiCategory::ARCH;
        else if (std::strcmp(t_natural, "hot_spring") == 0) cat = PoiCategory::HOT_SPRING;
        else if (std::strcmp(t_natural, "geyser") == 0) cat = PoiCategory::GEYSER;
        else if (std::strcmp(t_natural, "bay") == 0) cat = PoiCategory::BAY;
        else if (std::strcmp(t_natural, "cape") == 0) cat = PoiCategory::CAPE;
        else if (std::strcmp(t_natural, "island") == 0) cat = PoiCategory::ISLAND;
        else if (std::strcmp(t_natural, "glacier") == 0) cat = PoiCategory::GLACIER;
    }
    // railway
    if (cat == PoiCategory::UNKNOWN && t_railway) {
        if (std::strcmp(t_railway, "station") == 0) cat = PoiCategory::STATION;
    }
    // aeroway
    if (cat == PoiCategory::UNKNOWN && t_aeroway) {
        if (std::strcmp(t_aeroway, "aerodrome") == 0) cat = PoiCategory::AERODROME;
    }
    // man_made
    if (cat == PoiCategory::UNKNOWN && t_man_made) {
        if (std::strcmp(t_man_made, "tower") == 0) cat = PoiCategory::TOWER;
        else if (std::strcmp(t_man_made, "lighthouse") == 0) cat = PoiCategory::LIGHTHOUSE;
        else if (std::strcmp(t_man_made, "windmill") == 0) cat = PoiCategory::WINDMILL;
        else if (std::strcmp(t_man_made, "bridge") == 0) cat = PoiCategory::BRIDGE;
        else if (std::strcmp(t_man_made, "pier") == 0) cat = PoiCategory::PIER;
        else if (std::strcmp(t_man_made, "dam") == 0) cat = PoiCategory::DAM;
        else if (std::strcmp(t_man_made, "observatory") == 0) cat = PoiCategory::OBSERVATORY;
        else if (std::strcmp(t_man_made, "silo") == 0) cat = PoiCategory::SILO;
        else if (std::strcmp(t_man_made, "chimney") == 0) cat = PoiCategory::CHIMNEY;
        else if (std::strcmp(t_man_made, "watermill") == 0) cat = PoiCategory::WATERMILL;
    }
    // building
    if (cat == PoiCategory::UNKNOWN && t_building) {
        if (std::strcmp(t_building, "cathedral") == 0) cat = PoiCategory::CATHEDRAL;
        else if (std::strcmp(t_building, "palace") == 0) cat = PoiCategory::PALACE;
    }
    // craft
    if (cat == PoiCategory::UNKNOWN && t_craft) {
        if (std::strcmp(t_craft, "winery") == 0) cat = PoiCategory::WINERY;
        else if (std::strcmp(t_craft, "brewery") == 0) cat = PoiCategory::BREWERY;
    }
    // power
    if (cat == PoiCategory::UNKNOWN && t_power) {
        if (std::strcmp(t_power, "plant") == 0) cat = PoiCategory::POWER_PLANT;
    }
    // place
    if (cat == PoiCategory::UNKNOWN && t_place) {
        if (std::strcmp(t_place, "island") == 0 || std::strcmp(t_place, "islet") == 0)
            cat = PoiCategory::ISLAND;
    }
    // waterway
    if (cat == PoiCategory::UNKNOWN && t_waterway) {
        if (std::strcmp(t_waterway, "waterfall") == 0) cat = PoiCategory::WATERFALL;
    }
    // office
    if (cat == PoiCategory::UNKNOWN && t_office) {
        if (std::strcmp(t_office, "government") == 0) cat = PoiCategory::GOVERNMENT;
    }
    // shop — split common values, fall back to generic SHOP.
    if (cat == PoiCategory::UNKNOWN && t_shop) {
        if (std::strcmp(t_shop, "supermarket") == 0) cat = PoiCategory::SHOP_SUPERMARKET;
        else if (std::strcmp(t_shop, "convenience") == 0) cat = PoiCategory::SHOP_CONVENIENCE;
        else if (std::strcmp(t_shop, "clothes") == 0) cat = PoiCategory::SHOP_CLOTHES;
        else if (std::strcmp(t_shop, "mall") == 0) cat = PoiCategory::SHOP_MALL;
        else if (std::strcmp(t_shop, "department_store") == 0) cat = PoiCategory::SHOP_DEPARTMENT_STORE;
        else if (std::strcmp(t_shop, "bakery") == 0) cat = PoiCategory::SHOP_BAKERY;
        else if (std::strcmp(t_shop, "butcher") == 0) cat = PoiCategory::SHOP_BUTCHER;
        else if (std::strcmp(t_shop, "hardware") == 0) cat = PoiCategory::SHOP_HARDWARE;
        else if (std::strcmp(t_shop, "doityourself") == 0) cat = PoiCategory::SHOP_HARDWARE;
        else if (std::strcmp(t_shop, "electronics") == 0) cat = PoiCategory::SHOP_ELECTRONICS;
        else if (std::strcmp(t_shop, "furniture") == 0) cat = PoiCategory::SHOP_FURNITURE;
        else if (std::strcmp(t_shop, "jewelry") == 0) cat = PoiCategory::SHOP_JEWELRY;
        else if (std::strcmp(t_shop, "books") == 0) cat = PoiCategory::SHOP_BOOKS;
        else if (std::strcmp(t_shop, "pet") == 0) cat = PoiCategory::SHOP_PET;
        else cat = PoiCategory::SHOP; // generic fallback
    }

    // Fallback: any rank-30 addressable feature that didn't
    // hit a specific PoiCategory above. Mirrors Nominatim's
    // reverse.py DataLayer.POI filter (class_ NOT IN
    // ('place', 'building'), rank_search==30) which drives
    // the Moscow vending_machine / SF waste_basket / Sydney
    // toilets / Paris clock primary selections. UNNAMED_RANK30
    // entries carry no name_id but still have a parent_street
    // that surfaces as `road` when they win primary contest.
    if (cat == PoiCategory::UNKNOWN) {
        if (t_amenity || t_tourism || t_historic || t_leisure ||
            t_man_made || t_craft || t_office || t_waterway ||
            t_natural || t_aeroway || t_railway || t_power ||
            t_shop) {
            cat = PoiCategory::UNNAMED_RANK30;
        }
    }

    if (cat == PoiCategory::UNKNOWN) return std::optional<PoiClassification>{};

    uint8_t tier = poi_get_default_tier(cat);
    uint8_t flags = 0;
    if (t_wikipedia) flags |= POI_FLAG_WIKIPEDIA;
    if (t_wikidata)  flags |= POI_FLAG_WIKIDATA;
    // Nominatim treats highway=pedestrian/footway/living_street/
    // path/service areas as rank_search=26 (road rank) rather
    // than rank_search=30. Mark so the server can surface the
    // feature's own name as `road` rather than its parent street.
    if (t_highway && (
            std::strcmp(t_highway, "pedestrian") == 0 ||
            std::strcmp(t_highway, "footway") == 0 ||
            std::strcmp(t_highway, "living_street") == 0 ||
            std::strcmp(t_highway, "path") == 0 ||
            std::strcmp(t_highway, "cycleway") == 0 ||
            std::strcmp(t_highway, "service") == 0)) {
        flags |= POI_FLAG_HIGHWAY;
    }
    if ((flags & (POI_FLAG_WIKIPEDIA | POI_FLAG_WIKIDATA)) && tier > 1) tier--;

    return PoiClassification{cat, tier, flags};
}
