// Nominatim's find_linked_place (placex_triggers.sql:271-391): the place node
// each boundary and place area links, as its indexer finds them on a fresh
// import. The indexer runs the countries first (rank_address 1 to 4), then
// one BoundaryRunner per rank_search for the administrative boundaries,
// relations and closed ways, then one RankRunner per rank_address for the
// other polygons, the place areas (indexer.py index_full). A runner's rows
// all look for their node against the links as they stood when it started
// (one cursor over placex_indexing_prepare); each then links its node in the
// runner's order, the last to link a node keeping it (placex_update's
// UPDATE, :1143). So two polygons of one runner may link one node, and each
// takes its rank (:1109-1133).
//
// A polygon of rank_search 26 or more or of rank_address 0 links nothing
// (:285). Any other links the first of:
//   1. a relation's first label member that is a place node, of any value,
//      linked already or not (:291-312);
//   2. a place node of its wikidata, which it covers, of rank_search below 26
//      and not linked yet; one named as it is first, then the lower osm id
//      (:323-337);
//   3. with a name and a place value of its own, a place node of that value
//      whose name holds its name or lies in it, both lower-cased, covered, of
//      rank_search below 26, unlinked, and of its wikidata or none
//      (:339-361);
//   4. with a name, a place node of that name, lower-cased, ranking in its own
//      country as the polygon does in its (its initial rank), covered, of
//      rank_search below 26, unlinked, and of its wikidata or none
//      (:363-386).
// Ties between nodes go to the lower osm id. A polygon's name is its name
// tag, lower-cased; a value outside address-levels.json's (PlaceValue's
// OTHER) links no node by step 3.
//
// A node that a relation links as its label member is linked to no other
// polygon by steps 2 to 4, whichever runner it is in. On a fresh import a
// polygon of an earlier runner would link it, and the label relation take it
// over; the public server indexes each polygon again after its import, and
// finds the node linked then: Den Haag's level 8 boundary, whose wikidata
// node is the label member of its level 10 one, links nothing and keeps its
// 14.
#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "parsed_data.h"
#include "place_link_types.h"
#include "rank_address.h"

// What the pass did, for its log line.
struct LinkStats {
    size_t candidates = 0;               // place nodes a polygon may link
    size_t polygons = 0;                 // boundaries and place areas
    size_t runners = 0;                  // Nominatim's runners they fell in
    size_t by_step[5] = {0, 0, 0, 0, 0};  // links, by LinkStep
    size_t claimed = 0;                  // nodes linked
    size_t taken = 0;                    // links of a node a later runner's label link took
    size_t covers = 0;                   // covers tests
};

// Links every boundary and place area of the admin polygons (rank_address.h's
// objects) to its place node of data.link_candidates, which it frees, and
// gives each polygon its label: place_type_override is the linked node's
// type where AdminPlaceType holds it, else its own place tag's. It fills
// data.admin_links (each linking polygon's link), data.place_claims (each
// linked node's claimant, the polygon of the last link of it) and
// data.place_node_links (every boundary linking each place node). Reads
// admin_tags; `lookups.country_at` gives the nodes their countries.
LinkStats find_linked_places(ParsedData& data, const RankLookups& lookups, unsigned threads);
