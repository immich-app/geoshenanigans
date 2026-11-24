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
// A polygon that links a node takes the node's names it lacks as _place_
// names (placex_indexing_prepare, :76-108), which an English answer reads
// after its own name:en (localization.py Locales, _mingle_name_tags):
// linked_english_name.
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
    size_t named = 0;                    // linking polygons the node renames (linked_english_name)
};

// Whether the country `cc` (packed upper case) has English for its one
// language (country_settings.yaml), Nominatim's default language of the
// country (country_info.py setup_country_tables), which gives a name tag
// there its name:en too (add_default_place_name, utils.sql:121-139).
bool english_only_country(uint16_t cc);

// The name an English answer gives a polygon that links a place node, where
// it is the node's; empty where the polygon keeps its own. The polygon goes
// by its own name:en where it has one (`own_name_en`); in an English-only
// country (english_only_country) its name tag is its name:en
// (placex_update's add_default_place_name, placex_triggers.sql:1271); else
// the node's name:en comes before its name tag, and failing that the node's
// name tag where the node's country is English-only, which gives the node
// that name:en (placex_indexing_prepare, :86-90). `node_name_en` and
// `node_name` are the node's tags, empty for none.
std::string_view linked_english_name(bool own_name_en, uint16_t polygon_country, std::string_view node_name_en,
                                     std::string_view node_name, uint16_t node_country);

// Links every boundary and place area of the admin polygons (rank_address.h's
// objects) to its place node of data.link_candidates, which it frees, and
// gives each polygon its label: place_type_override is the linked node's
// type where AdminPlaceType holds it, else its own place tag's. It fills
// data.admin_links (each linking polygon's link), data.place_claims (each
// linked node's claimant, the polygon of the last link of it) and
// data.place_node_links (every boundary linking each place node), and names
// a linking polygon by its node where Nominatim's English answer does
// (linked_english_name), interning the name in data.string_pool, its own
// name kept as its own_name_id where that is another string. Reads
// admin_tags; `lookups.country_at` gives the nodes their countries.
LinkStats find_linked_places(ParsedData& data, const RankLookups& lookups, unsigned threads);
