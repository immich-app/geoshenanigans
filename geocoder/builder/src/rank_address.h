// Nominatim's rank_address of every admin polygon and place node, as
// placex_update gives it at index time (placex_triggers.sql:806-1172), in
// the order Nominatim's indexer runs it on a fresh import:
//
//   1. Boundaries (boundary=administrative relations and closed ways), by
//      rank_search, then admin_level, then osm id: one BoundaryRunner per
//      rank_search, its rows by admin level. A relation of a rank other than
//      0 takes the rank of its parent + 2 where the parent, the relation of
//      the highest level below its own round its point on surface, ranks as
//      high (:877-907); none where that parent has its wikidata and its
//      geometry (:896-899). From rank 10 a place polygon of rank 1 to 23
//      containing it lifts it to that polygon's rank + 2 (:912-929). A way
//      with a place tag of rank 16 to 23 takes the place-area step instead.
//      Then the place node it links ranks it where that rank is above its
//      parent's (:1109-1133), or, where none links it, its own place tag
//      raises it (:1157-1172; Nominatim also lowers it there, once every
//      link find_linked_place makes is known).
//   2. Place areas (admin_level 15), by rank_address, the larger first: one
//      RankRunner per rank. One of rank 16 to 23 contained in a polygon of a
//      place rank from its own up to 23 takes that polygon's rank + 2
//      (:930-950); any other the rank of the place node it links.
//   3. Place nodes of rank 16 to 23: two more where a relation of their rank
//      holds them (:951-971), other than a boundary that links them: a linked
//      node is a place of its own only in a dir that leaves out the
//      boundaries standing for it.
//
// Each step reads the others' ranks as the indexer leaves them when it runs:
// a polygon indexed later still has its initial rank. Postal areas rank 0.
// The geometry is worked out on every core beforehand; only the rank reads
// run in sequence, so the ranks don't depend on the thread count.
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

#include "admin_rank_config.h"
#include "parsed_data.h"

// What the pass reads beyond the polygons' rings and tags: the admin cell
// index and the countries. Called on every core at once.
struct RankLookups {
    // Calls visit(polygon) for every admin polygon whose rings may hold p.
    std::function<void(PackedLocation p, const std::function<void(uint32_t)>& visit)> polygons_near;
    // Whether the rings of polygon pid hold p.
    std::function<bool(uint32_t pid, PackedLocation p)> holds;
    // The country at p (packed upper case), 0 where none is known.
    std::function<uint16_t(PackedLocation p)> country_at;
};

// What the pass did, for its log line.
struct RankStats {
    size_t boundaries = 0, place_areas = 0;
    size_t nested = 0, duplicates = 0, contained = 0, linked = 0, lowered = 0, own_tags = 0;
    size_t nodes = 0, nodes_promoted = 0;
    size_t pairs = 0;            // the containment tests
    double pair_seconds = 0;     // and their time
};

// How one boundary or place area came to its rank, for a dump of the pass.
struct RankTrace {
    uint64_t id = 0;  // its admin_boundary_id
    uint8_t level = 0;
    uint16_t country = 0;
    PlaceRanks initial;
    uint64_t parent = 0;  // the admin-nested parent, 0 for none
    uint8_t parent_rank = 0;  // the parent's rank as the step read it
    bool duplicate = false;
    uint64_t container = 0;  // the polygon that lifted it, 0 for none
    uint8_t container_rank = 0;
    uint8_t before_link = 0;  // its rank before the link or own tag step
    char step = '-';  // 'L' where a link ranked it, 'O' where its own tag did, else '-'
    uint8_t rank = 0;
};

// Gives every admin polygon (AdminPolygon::rank_address) and place node
// (PlaceNode::rank_address, a hidden one's as a place of its own) its
// rank_address, reading admin_osm_ids (which rings make a boundary),
// admin_wikidata and place_node_links, which it frees. With `trace`, a
// RankTrace for each boundary and place area, by id.
RankStats compute_rank_addresses(ParsedData& data, const RankLookups& lookups, unsigned threads,
                                 std::vector<RankTrace>* trace = nullptr);
