// Unit tests for place_claims.h: boundaries claiming place nodes.
#include "place_claims.h"

#include "test_framework.h"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

namespace {

using gc::id_alloc::ObjectType;

AdminPolygon polygon(uint8_t admin_level, float area) {
    AdminPolygon p{};
    p.admin_level = admin_level;
    p.area = area;
    return p;
}

PlaceNode place(uint32_t name_id, uint32_t parent) {
    PlaceNode pn{};
    pn.name_id = name_id;
    pn.parent_poly_id = parent;
    return pn;
}

uint64_t node_id(int64_t id) { return pack_osm_id(ObjectType::OSM_NODE, id); }

// Six boundaries: relation 100 (two rings, the second larger), relations
// 200 (L4), 400 (L6), 500 (L8), closed way 300 (L6).
struct Boundaries {
    std::vector<AdminPolygon> polygons{polygon(8, 1.0f), polygon(8, 5.0f), polygon(4, 50.0f),
                                       polygon(6, 3.0f), polygon(6, 2.0f), polygon(8, 2.0f)};
    std::vector<uint64_t> ids{admin_ring_id(100, 0), admin_ring_id(100, 1), admin_ring_id(200, 0),
                              way_boundary_id(300), admin_ring_id(400, 0), admin_ring_id(500, 0)};
};

}  // namespace

TEST(place_claims_admin_ids_match_the_admin_osm_ids_packing) {
    // build_index packed relation rings as type << 56 | (relation << 16 | ring).
    const uint64_t packed = (uint64_t(3) << 56) | ((uint64_t(4242) << 16) | 7);
    CHECK_EQ(admin_ring_id(4242, 7), packed);
    CHECK_EQ(admin_boundary_id(admin_ring_id(4242, 7)), relation_boundary_id(4242));
    CHECK_EQ(admin_boundary_id(admin_ring_id(4242, 0)), relation_boundary_id(4242));
    CHECK(relation_boundary_id(4242) != relation_boundary_id(4243));
    // A closed way is its own boundary, and never a relation's.
    CHECK_EQ(admin_boundary_id(way_boundary_id(4242)), way_boundary_id(4242));
    CHECK(way_boundary_id(4242) != relation_boundary_id(4242));
}

TEST(place_claims_boundary_polygon_is_the_largest_ring) {
    Boundaries b;
    const auto bp = boundary_polygons(b.polygons, b.ids);
    REQUIRE(bp.size() == 5u);
    CHECK_EQ(bp[0].first, way_boundary_id(300));
    CHECK_EQ(bp[0].second, 3u);
    CHECK_EQ(bp[1].first, relation_boundary_id(100));
    CHECK_EQ(bp[1].second, 1u);  // 5.0 over 1.0
    // Equal areas go to the lower ring.
    b.polygons[0].area = b.polygons[1].area;
    CHECK_EQ(boundary_polygons(b.polygons, b.ids)[1].second, 0u);
}

TEST(place_claims_hidden_node_keeps_its_name_and_names_its_claimant) {
    Boundaries b;
    std::vector<PlaceNode> nodes{place(10, 0), place(11, 2), place(12, NO_DATA)};
    const std::vector<uint64_t> ids{node_id(1), node_id(2), node_id(3)};
    // Node 1 is relation 100's label, node 2 claimed by a relation without
    // a polygon; node 3 unclaimed.
    const std::vector<PlaceClaim> claims{{node_id(1), relation_boundary_id(100), true},
                                         {node_id(2), relation_boundary_id(999), true}};
    CHECK_EQ(hide_claimed_place_nodes(nodes, ids, claims, b.polygons, b.ids), 2u);

    CHECK_EQ(nodes[0].name_id, 10u);
    CHECK_EQ(nodes[0]._pad1, PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF);
    CHECK_EQ(nodes[0].parent_poly_id, 1u);  // relation 100's largest ring
    CHECK(place_node_hidden(nodes[0]));

    // Hidden, but no boundary to name: the containing polygon stays.
    CHECK_EQ(nodes[1].name_id, 11u);
    CHECK_EQ(nodes[1]._pad1, PLACE_FLAG_HIDDEN);
    CHECK_EQ(nodes[1].parent_poly_id, 2u);
    CHECK(place_node_hidden(nodes[1]));

    CHECK_EQ(nodes[2].name_id, 12u);
    CHECK_EQ(nodes[2]._pad1, 0);
    CHECK_EQ(nodes[2].parent_poly_id, NO_DATA);
    CHECK(!place_node_hidden(nodes[2]));
}

TEST(place_claims_several_claimants_resolve_as_nominatim_links_them) {
    Boundaries b;
    const std::vector<uint64_t> ids{node_id(1), node_id(2), node_id(3), node_id(4)};
    std::vector<PlaceClaim> claims{
        // Two labels: the higher level links last and keeps the node.
        {node_id(1), relation_boundary_id(200), true}, {node_id(1), relation_boundary_id(500), true},
        // Two wikidata matches: the lower level links first, the other
        // finds the node taken.
        {node_id(2), relation_boundary_id(500), false}, {node_id(2), way_boundary_id(300), false},
        // A label beats a wikidata match at any level.
        {node_id(3), relation_boundary_id(200), false}, {node_id(3), relation_boundary_id(400), true},
        // Equal levels: the lower boundary id (a way's sorts before a relation's).
        {node_id(4), relation_boundary_id(400), true}, {node_id(4), way_boundary_id(300), true},
        // A claimant without a polygon never wins.
        {node_id(4), relation_boundary_id(999), true},
    };
    for (int order = 0; order < 2; order++) {
        std::vector<PlaceNode> nodes(4, place(7, NO_DATA));
        CHECK_EQ(hide_claimed_place_nodes(nodes, ids, claims, b.polygons, b.ids, 2), 4u);
        CHECK_EQ(nodes[0].parent_poly_id, 5u);  // relation 500, L8
        CHECK_EQ(nodes[1].parent_poly_id, 3u);  // way 300, L6
        CHECK_EQ(nodes[2].parent_poly_id, 4u);  // relation 400's label
        CHECK_EQ(nodes[3].parent_poly_id, 3u);  // way 300
        for (const auto& pn : nodes) {
            CHECK_EQ(pn._pad1, PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF);
            CHECK_EQ(pn.name_id, 7u);
        }
        std::reverse(claims.begin(), claims.end());
    }
}

TEST(place_claims_a_claimant_that_links_nothing_claims_nothing) {
    // A boundary of rank 0 in its country links no place node (find_linked_place): its claims are
    // none. Node 1 is the label of relation 400 (L6, of rank 0 here) alone and stays a place;
    // node 2 is relation 400's label and relation 200's (L4), which takes it, though a label
    // claim of a higher level wins.
    Boundaries b;
    std::vector<PlaceNode> nodes{place(10, NO_DATA), place(11, NO_DATA)};
    const std::vector<uint64_t> ids{node_id(1), node_id(2)};
    const std::vector<PlaceClaim> claims{{node_id(1), relation_boundary_id(400), true},
                                         {node_id(2), relation_boundary_id(400), true},
                                         {node_id(2), relation_boundary_id(200), true}};
    const auto links = [&](uint32_t p) { return b.polygons[p].admin_level != 6; };
    CHECK_EQ(hide_claimed_place_nodes(nodes, ids, claims, b.polygons, b.ids, 0, links), 1u);
    CHECK(!place_node_hidden(nodes[0]));
    CHECK_EQ(nodes[0]._pad1, 0);
    CHECK_EQ(nodes[0].parent_poly_id, NO_DATA);
    CHECK(place_node_hidden(nodes[1]));
    CHECK_EQ(nodes[1].parent_poly_id, 2u);  // relation 200
}

TEST(place_claims_name_links_keep_the_name_and_pick_the_lowest_level) {
    Boundaries b;
    std::vector<PlaceNode> nodes{place(10, 2), place(11, 2), place(12, 2)};
    // Node 0: L8 relation 500 and L6 relation 400; node 1: both rings of
    // relation 100.
    std::vector<std::pair<uint32_t, uint32_t>> links{{0, 5}, {1, 1}, {0, 4}, {1, 0}};
    CHECK_EQ(hide_name_linked_place_nodes(nodes, links, b.polygons, b.ids), 2u);
    CHECK_EQ(nodes[0].name_id, 10u);
    CHECK_EQ(nodes[0]._pad1, PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF);
    CHECK_EQ(nodes[0].parent_poly_id, 4u);
    CHECK_EQ(nodes[1].name_id, 11u);
    CHECK_EQ(nodes[1].parent_poly_id, 1u);  // relation 100's largest ring
    CHECK_EQ(nodes[2]._pad1, 0);
    CHECK_EQ(nodes[2].parent_poly_id, 2u);
    std::reverse(links.begin(), links.end());
    std::vector<PlaceNode> again{place(10, 2), place(11, 2), place(12, 2)};
    hide_name_linked_place_nodes(again, links, b.polygons, b.ids);
    CHECK_EQ(again[0].parent_poly_id, 4u);
    CHECK_EQ(again[1].parent_poly_id, 1u);
}

TEST(place_claims_a_name_link_names_the_largest_ring_whichever_ring_matched) {
    Boundaries b;
    std::vector<PlaceNode> nodes{place(10, 2)};
    // Only relation 100's smaller ring matched the node.
    CHECK_EQ(hide_name_linked_place_nodes(nodes, {{0, 0}}, b.polygons, b.ids), 1u);
    CHECK_EQ(nodes[0]._pad1, PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF);
    CHECK_EQ(nodes[0].parent_poly_id, 1u);
}

TEST(place_claims_subset_without_the_claimant_names_no_boundary) {
    PlaceNode labelled = place(10, 1);
    labelled._pad1 = PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF;
    PlaceNode kept = labelled;

    remap_place_parent(kept, {NO_DATA, 0});
    CHECK_EQ(kept.parent_poly_id, 0u);
    CHECK_EQ(kept._pad1, PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF);

    remap_place_parent(labelled, {0, NO_DATA});
    CHECK_EQ(labelled.parent_poly_id, NO_DATA);
    CHECK_EQ(labelled._pad1, PLACE_FLAG_HIDDEN);
    CHECK_EQ(labelled.name_id, 10u);

    PlaceNode visible = place(11, 5);
    remap_place_parent(visible, {0, 1});
    CHECK_EQ(visible.parent_poly_id, NO_DATA);
    CHECK_EQ(visible._pad1, 0);
}

TEST(place_claims_admin_minimal_shows_a_node_whose_claimant_it_leaves_out) {
    // admin-minimal's remap: polygon 0 (an L8) ships in slot 0, polygon 1 (an
    // L9 or L10) is left out.
    const std::vector<uint32_t> minimal{0, NO_DATA};

    // The claimant ships: the node stays hidden, its label point.
    PlaceNode shipped = place(10, 0);
    shipped._pad1 = PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF;
    remap_place_parent_unhiding_orphans(shipped, minimal);
    CHECK_EQ(shipped.parent_poly_id, 0u);
    CHECK_EQ(shipped._pad1, PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF);
    CHECK(place_node_hidden(shipped));

    // The claimant is left out: nothing stands for the node, which answers as
    // a place again, under its own name.
    PlaceNode orphan = place(11, 1);
    orphan._pad1 = PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF;
    remap_place_parent_unhiding_orphans(orphan, minimal);
    CHECK_EQ(orphan.parent_poly_id, NO_DATA);
    CHECK_EQ(orphan._pad1, 0);
    CHECK_EQ(orphan.name_id, 11u);
    CHECK(!place_node_hidden(orphan));

    // Claimed by a boundary without a polygon (its parent is the containing
    // polygon, left out here): hidden in every set.
    PlaceNode no_polygon = place(12, 1);
    no_polygon._pad1 = PLACE_FLAG_HIDDEN;
    remap_place_parent_unhiding_orphans(no_polygon, minimal);
    CHECK_EQ(no_polygon.parent_poly_id, NO_DATA);
    CHECK_EQ(no_polygon._pad1, PLACE_FLAG_HIDDEN);
    CHECK(place_node_hidden(no_polygon));

    PlaceNode visible = place(13, 1);
    remap_place_parent_unhiding_orphans(visible, minimal);
    CHECK_EQ(visible.parent_poly_id, NO_DATA);
    CHECK_EQ(visible._pad1, 0);
    CHECK(!place_node_hidden(visible));
}

TEST(place_claims_lower_name_lowers_as_postgres_lower) {
    CHECK(lower_name(nullptr).empty());
    CHECK(lower_name("").empty());
    CHECK_EQ(lower_name("Lent"), std::string("lent"));
    CHECK_EQ(lower_name("ÉVORA"), std::string("évora"));
    CHECK_EQ(lower_name("ŁÓDŹ"), std::string("łódź"));
    CHECK_EQ(lower_name("İZMİR"), std::string("izmir"));
    CHECK_EQ(lower_name("ΆΘΗΝΑ"), std::string("άθηνα"));
    CHECK_EQ(lower_name("МОСКВА"), std::string("москва"));
    CHECK_EQ(lower_name("ЁЛКИ"), std::string("ёлки"));
    CHECK_EQ(lower_name("ԵՐԵՎԱՆ"), std::string("երեվան"));
    // ß, × and characters of three bytes stay as they are.
    CHECK_EQ(lower_name("Straße ×2 東京"), std::string("straße ×2 東京"));
}

TEST(place_claims_a_wikidata_link_takes_one_node_in_the_steps_order) {
    // find_linked_place's wikidata step: ORDER BY lower(name->'name') = bnd_name desc, nulls
    // first, the first node found linked.
    CHECK_EQ(wikidata_order("", "riverside"), 0);
    CHECK_EQ(wikidata_order("riverside", ""), 0);
    CHECK_EQ(wikidata_order("riverside", "riverside"), 1);
    CHECK_EQ(wikidata_order("riverside town", "riverside"), 2);

    // A place=suburb area, relation 100, covers the town node 1 and the village node 2 of its
    // wikidata; node 2 has the area's name and node 3, outside it, none.
    const auto town = static_cast<uint8_t>(AdminPlaceType::TOWN), village = static_cast<uint8_t>(AdminPlaceType::VILLAGE),
               suburb = static_cast<uint8_t>(AdminPlaceType::SUBURB);
    std::vector<AdminPolygon> polygons{polygon(15, 1.0f), polygon(15, 2.0f)};
    for (auto& p : polygons) p.place_type_override = suburb;
    const std::vector<uint64_t> ids{admin_ring_id(100, 0), admin_ring_id(100, 1)};
    std::vector<WikidataCandidate> candidates{{relation_boundary_id(100), node_id(1), 2, town},
                                              {relation_boundary_id(100), node_id(2), 1, village}};
    std::vector<PlaceClaim> claims;
    CHECK_EQ(link_wikidata_places(candidates, polygons, ids, claims), 1u);
    REQUIRE(claims.size() == 1u);
    CHECK_EQ(claims[0].node, node_id(2));
    CHECK_EQ(claims[0].boundary, relation_boundary_id(100));
    CHECK(!claims[0].label);
    for (const auto& p : polygons) {
        CHECK_EQ(p.place_type_override, village);
        CHECK_EQ(p.own_place_type, suburb);
    }

    // A node without a name comes first, and equals go to the lower node id.
    std::vector<AdminPolygon> again{polygon(8, 1.0f)};
    std::vector<PlaceClaim> again_claims;
    candidates = {{relation_boundary_id(100), node_id(7), 1, town},
                  {relation_boundary_id(100), node_id(6), 0, village},
                  {relation_boundary_id(100), node_id(5), 0, town}};
    CHECK_EQ(link_wikidata_places(candidates, again, {admin_ring_id(100, 0)}, again_claims), 1u);
    REQUIRE(again_claims.size() == 1u);
    CHECK_EQ(again_claims[0].node, node_id(5));
    CHECK_EQ(again[0].place_type_override, town);
    CHECK_EQ(again[0].own_place_type, 0);
}

TEST(place_claims_a_node_a_boundary_before_links_is_no_later_ones) {
    // Nominatim indexes the boundaries by rank, the place areas after them: the wijk, relation 200
    // (L10), links the village node 5 first, the quarter area, relation 100, passes it over for
    // node 6, and the quarter area of way 300 finds none left and stays a quarter.
    const auto village = static_cast<uint8_t>(AdminPlaceType::VILLAGE),
               quarter = static_cast<uint8_t>(AdminPlaceType::QUARTER),
               hamlet = static_cast<uint8_t>(AdminPlaceType::HAMLET);
    std::vector<AdminPolygon> polygons{polygon(15, 1.0f), polygon(10, 2.0f), polygon(15, 3.0f)};
    polygons[0].place_type_override = quarter;
    polygons[2].place_type_override = quarter;
    const std::vector<uint64_t> ids{admin_ring_id(100, 0), admin_ring_id(200, 0), way_boundary_id(300)};
    std::vector<WikidataCandidate> candidates{{way_boundary_id(300), node_id(5), 1, village},
                                              {relation_boundary_id(100), node_id(5), 1, village},
                                              {relation_boundary_id(100), node_id(6), 2, hamlet},
                                              {relation_boundary_id(200), node_id(5), 1, village},
                                              {relation_boundary_id(999), node_id(6), 1, village}};
    for (int order = 0; order < 2; order++) {
        std::vector<AdminPolygon> linked = polygons;
        std::vector<PlaceClaim> claims;
        CHECK_EQ(link_wikidata_places(candidates, linked, ids, claims), 2u);
        CHECK_EQ(linked[1].place_type_override, village);
        CHECK_EQ(linked[1].own_place_type, 0);
        CHECK_EQ(linked[0].place_type_override, hamlet);
        CHECK_EQ(linked[0].own_place_type, quarter);
        CHECK_EQ(linked[2].place_type_override, quarter);
        CHECK_EQ(linked[2].own_place_type, 0);
        REQUIRE(claims.size() == 2u);
        std::sort(claims.begin(), claims.end(), [](const PlaceClaim& a, const PlaceClaim& b) { return a.node < b.node; });
        CHECK_EQ(claims[0].boundary, relation_boundary_id(200));
        CHECK_EQ(claims[1].boundary, relation_boundary_id(100));
        std::reverse(candidates.begin(), candidates.end());
    }
}

TEST(place_claims_a_boundary_of_rank_0_reserves_no_node_of_its_wikidata) {
    // find_linked_place returns at once for a boundary of rank_address 0 (placex_triggers.sql:284),
    // so it links nothing and leaves the node to the boundaries after it: an Australian local
    // government area (admin_level 6, rank 0 there), relation 100, and a place=suburb area,
    // relation 200, both cover the town node 5 of their wikidata. The area links it.
    const auto town = static_cast<uint8_t>(AdminPlaceType::TOWN), suburb = static_cast<uint8_t>(AdminPlaceType::SUBURB);
    std::vector<AdminPolygon> polygons{polygon(6, 2.0f), polygon(15, 1.0f)};
    polygons[1].place_type_override = suburb;
    const std::vector<uint64_t> ids{admin_ring_id(100, 0), admin_ring_id(200, 0)};
    const std::vector<WikidataCandidate> candidates{{relation_boundary_id(100), node_id(5), 2, town},
                                                    {relation_boundary_id(200), node_id(5), 2, town}};
    std::vector<PlaceClaim> claims;
    const auto rank = [](uint32_t p, const AdminPolygon&) { return p == 0 ? uint8_t(0) : uint8_t(20); };
    CHECK_EQ(link_wikidata_places(candidates, polygons, ids, claims, rank), 1u);
    REQUIRE(claims.size() == 1u);
    CHECK_EQ(claims[0].node, node_id(5));
    CHECK_EQ(claims[0].boundary, relation_boundary_id(200));
    CHECK_EQ(polygons[0].place_type_override, 0);
    CHECK_EQ(polygons[1].place_type_override, town);
    CHECK_EQ(polygons[1].own_place_type, suburb);
}

TEST(place_claims_a_label_member_is_left_to_the_wikidata_step_only_by_a_boundary_of_rank_0) {
    // The label step comes first, but find_linked_place returns at once for a boundary of
    // rank_address 0: an Australian local government area (admin_level 6, rank 0 there), relation
    // 100, names the town node 5 as its label, which the place=suburb area, relation 200, of its
    // wikidata links; the label claim of the LGA goes. A boundary of rank 16, relation 300, links
    // its label member, the town node 6, which the suburb area 400 of its wikidata passes over.
    const auto town = static_cast<uint8_t>(AdminPlaceType::TOWN), suburb = static_cast<uint8_t>(AdminPlaceType::SUBURB);
    std::vector<AdminPolygon> polygons{polygon(6, 2.0f), polygon(15, 1.0f), polygon(8, 2.0f), polygon(15, 1.0f)};
    polygons[1].place_type_override = suburb;
    polygons[3].place_type_override = suburb;
    const std::vector<uint64_t> ids{admin_ring_id(100, 0), admin_ring_id(200, 0), admin_ring_id(300, 0),
                                    admin_ring_id(400, 0)};
    const std::vector<WikidataCandidate> candidates{{relation_boundary_id(200), node_id(5), 2, town},
                                                    {relation_boundary_id(400), node_id(6), 2, town}};
    std::vector<PlaceClaim> claims{{node_id(5), relation_boundary_id(100), true},
                                   {node_id(6), relation_boundary_id(300), true}};
    const auto rank = [](uint32_t p, const AdminPolygon&) { return p == 0 ? uint8_t(0) : uint8_t(16); };
    CHECK_EQ(link_wikidata_places(candidates, polygons, ids, claims, rank), 1u);
    REQUIRE(claims.size() == 2u);
    std::sort(claims.begin(), claims.end(), [](const PlaceClaim& a, const PlaceClaim& b) { return a.node < b.node; });
    CHECK_EQ(claims[0].node, node_id(5));
    CHECK_EQ(claims[0].boundary, relation_boundary_id(200));
    CHECK(!claims[0].label);
    CHECK_EQ(claims[1].node, node_id(6));
    CHECK_EQ(claims[1].boundary, relation_boundary_id(300));
    CHECK_EQ(polygons[1].place_type_override, town);
    CHECK_EQ(polygons[3].place_type_override, suburb);
}

TEST(place_claims_place_areas_link_in_the_order_of_their_address_ranks) {
    // Nominatim indexes the place areas after every boundary, one rank_address after another
    // (indexer.py index_by_rank, RankRunner): a place=suburb area (rank 20), relation 300, takes
    // the village node 5 of its wikidata before a place=quarter area (rank 22) of a lower id,
    // relation 100, which links nothing and stays a quarter.
    const auto village = static_cast<uint8_t>(AdminPlaceType::VILLAGE),
               suburb = static_cast<uint8_t>(AdminPlaceType::SUBURB), quarter = static_cast<uint8_t>(AdminPlaceType::QUARTER);
    std::vector<AdminPolygon> polygons{polygon(15, 1.0f), polygon(15, 2.0f)};
    polygons[0].place_type_override = quarter;
    polygons[1].place_type_override = suburb;
    const std::vector<uint64_t> ids{admin_ring_id(100, 0), admin_ring_id(300, 0)};
    const std::vector<WikidataCandidate> candidates{{relation_boundary_id(100), node_id(5), 2, village},
                                                    {relation_boundary_id(300), node_id(5), 2, village}};
    std::vector<PlaceClaim> claims;
    CHECK_EQ(link_wikidata_places(candidates, polygons, ids, claims), 1u);
    REQUIRE(claims.size() == 1u);
    CHECK_EQ(claims[0].boundary, relation_boundary_id(300));
    CHECK_EQ(polygons[1].place_type_override, village);
    CHECK_EQ(polygons[1].own_place_type, suburb);
    CHECK_EQ(polygons[0].place_type_override, quarter);
    CHECK_EQ(polygons[0].own_place_type, 0);
}

TEST(place_claims_of_nodes_equal_in_the_steps_order_the_one_of_the_lowest_address_rank_links) {
    // The step's ORDER BY leaves nodes of the same name unordered: Espanola (relation 100) covers
    // the town node 20 and the hamlet node 10 of its wikidata, both named Española. The town, of
    // address rank 16, comes before the hamlet's 20 whatever their ids, and equals go to the lower.
    const auto town = static_cast<uint8_t>(AdminPlaceType::TOWN), hamlet = static_cast<uint8_t>(AdminPlaceType::HAMLET),
               village = static_cast<uint8_t>(AdminPlaceType::VILLAGE);
    std::vector<AdminPolygon> polygons{polygon(8, 1.0f)};
    const std::vector<uint64_t> ids{admin_ring_id(100, 0)};
    std::vector<WikidataCandidate> candidates{{relation_boundary_id(100), node_id(10), 2, hamlet},
                                              {relation_boundary_id(100), node_id(20), 2, town}};
    std::vector<PlaceClaim> claims;
    CHECK_EQ(link_wikidata_places(candidates, polygons, ids, claims), 1u);
    REQUIRE(claims.size() == 1u);
    CHECK_EQ(claims[0].node, node_id(20));
    CHECK_EQ(polygons[0].place_type_override, town);

    polygons = {polygon(8, 1.0f)};
    claims.clear();
    candidates = {{relation_boundary_id(100), node_id(30), 2, town}, {relation_boundary_id(100), node_id(40), 2, village}};
    CHECK_EQ(link_wikidata_places(candidates, polygons, ids, claims), 1u);
    REQUIRE(claims.size() == 1u);
    CHECK_EQ(claims[0].node, node_id(30));
}
