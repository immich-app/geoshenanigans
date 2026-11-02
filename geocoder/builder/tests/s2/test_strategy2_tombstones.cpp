// apply_strategy2_remaps (cell_index.h) keeps every record in the slot the
// previous build gave it; the slot of a record that is gone holds a tombstone.
#include <cstdint>
#include <filesystem>
#include <string>

#include "cell_index.h"
#include "id_allocator.h"
#include "parsed_data.h"
#include "scratch_dir.h"
#include "test_framework.h"
#include "types.h"

using gc::id_alloc::IdAllocator;
using gc::id_alloc::ObjectType;

TEST(a_place_tombstone_names_no_polygon) {
    // The previous build held nodes 1 and 2, this one holds node 2: node 1's
    // slot becomes a tombstone.
    const ScratchDir prev("strategy2-tombstones");
    std::filesystem::create_directories(prev.path() + "/admin");
    IdAllocator::write_sidecar(prev.path() + "/admin/place_nodes.osm_ids",
                               {{uint8_t(ObjectType::OSM_NODE), 0, 0, 0, 1}, {uint8_t(ObjectType::OSM_NODE), 0, 0, 0, 2}});
    ParsedData data;
    PlaceNode node{};
    node.name_id = 0;
    data.place_nodes.push_back(node);
    data.place_osm_ids.push_back(pack_osm_id(ObjectType::OSM_NODE, 2));

    apply_strategy2_remaps(data, prev.path(), RunOrder::Serial);

    REQUIRE(data.place_nodes.size() == 2u);
    CHECK_EQ(data.place_nodes[1].name_id, 0u);
    CHECK_EQ(data.place_nodes[0].name_id, NO_DATA);
    CHECK_EQ(data.place_nodes[0].parent_poly_id, NO_DATA);
}
