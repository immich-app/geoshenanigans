// Unit tests for place_claims.h: boundaries claiming place nodes.
#include "place_claims.h"

#include "test_framework.h"

#include <algorithm>
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

TEST(place_claims_name_links_keep_the_name_and_pick_the_lowest_level) {
    Boundaries b;
    std::vector<PlaceNode> nodes{place(10, 2), place(11, 2), place(12, 2)};
    // Node 0: L8 relation 500 and L6 relation 400; node 1: both rings of
    // relation 100 (equal levels, the lower ring id).
    std::vector<std::pair<uint32_t, uint32_t>> links{{0, 5}, {1, 1}, {0, 4}, {1, 0}};
    CHECK_EQ(hide_name_linked_place_nodes(nodes, links, b.polygons, b.ids), 2u);
    CHECK_EQ(nodes[0].name_id, 10u);
    CHECK_EQ(nodes[0]._pad1, PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF);
    CHECK_EQ(nodes[0].parent_poly_id, 4u);
    CHECK_EQ(nodes[1].name_id, 11u);
    CHECK_EQ(nodes[1].parent_poly_id, 0u);
    CHECK_EQ(nodes[2]._pad1, 0);
    CHECK_EQ(nodes[2].parent_poly_id, 2u);
    std::reverse(links.begin(), links.end());
    std::vector<PlaceNode> again{place(10, 2), place(11, 2), place(12, 2)};
    hide_name_linked_place_nodes(again, links, b.polygons, b.ids);
    CHECK_EQ(again[0].parent_poly_id, 4u);
    CHECK_EQ(again[1].parent_poly_id, 0u);
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
