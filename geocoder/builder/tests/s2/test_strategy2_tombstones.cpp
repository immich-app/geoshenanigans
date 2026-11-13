// apply_strategy2_remaps (cell_index.h) keeps every record but the place
// nodes in the slot the previous build gave it; the slot of a record that is
// gone holds a tombstone.
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>

#include "cell_index.h"
#include "id_allocator.h"
#include "parsed_data.h"
#include "scratch_dir.h"
#include "test_framework.h"
#include "types.h"

using gc::id_alloc::IdAllocator;
using gc::id_alloc::ObjectType;

TEST(place_nodes_keep_no_slots) {
    // The place files hold their nodes in cell order, so no place node keeps
    // the slot an earlier build gave it: one gone since leaves no tombstone,
    // whatever sidecar the earlier build left.
    const ScratchDir prev("strategy2-places");
    std::filesystem::create_directories(prev.path() + "/admin");
    IdAllocator::write_sidecar(prev.path() + "/admin/place_nodes.osm_ids",
                               {{uint8_t(ObjectType::OSM_NODE), 0, 0, 0, 1}, {uint8_t(ObjectType::OSM_NODE), 0, 0, 0, 2}});
    ParsedData data;
    PlaceNode node{};
    node.name_id = 0;
    data.place_nodes.push_back(node);
    data.place_osm_ids.push_back(pack_osm_id(ObjectType::OSM_NODE, 2));

    apply_strategy2_remaps(data, prev.path(), RunOrder::Serial);

    REQUIRE(data.place_nodes.size() == 1u);
    CHECK_EQ(data.place_nodes[0].name_id, 0u);
}

TEST(each_admin_polygons_arc_rings_follow_it_into_its_slot) {
    // The previous build held relations 7, 9 and 5 in slots 0 to 2; this one
    // holds 5 and 7: 9's slot becomes a tombstone, which has no rings.
    const ScratchDir prev("strategy2-arc-rings");
    std::filesystem::create_directories(prev.path() + "/quality/exact");
    auto slot = [](int64_t relation) {
        return gc::id_alloc::SidecarSlot{uint8_t(ObjectType::OSM_RELATION), 0, 0, 0, uint64_t(relation) << 16};
    };
    IdAllocator::write_sidecar(admin_sidecar_path(prev.path()), {slot(7), slot(9), slot(5)});
    ParsedData data;
    for (int64_t relation : {5, 7}) {
        AdminPolygon p{};
        p.name_id = static_cast<uint32_t>(relation);
        data.admin_polygons.push_back(p);
        data.admin_osm_ids.push_back(admin_ring_id(relation, 0));
    }
    data.admin_arcs.polygons = {{0, 2}, {2, 1}};

    apply_strategy2_remaps(data, prev.path(), RunOrder::Serial);

    REQUIRE(data.admin_polygons.size() == 3u);
    CHECK_EQ(data.admin_polygons[0].name_id, 7u);
    REQUIRE(data.admin_arcs.polygons.size() == 3u);
    CHECK(data.admin_arcs.polygons[0] == (AdminArcPolygon{2, 1}));
    CHECK(data.admin_arcs.polygons[1] == AdminArcPolygon{});
    CHECK(data.admin_arcs.polygons[2] == (AdminArcPolygon{0, 2}));
}

TEST(admin_slots_come_from_the_exact_tiers_sidecar_and_a_cold_start_is_counted) {
    // Relations 5 and 7, which the previous build held at slots 1 and 0.
    auto admins = [] {
        ParsedData data;
        for (int64_t relation : {5, 7}) {
            AdminPolygon p{};
            p.name_id = static_cast<uint32_t>(relation);
            data.admin_polygons.push_back(p);
            data.admin_osm_ids.push_back(admin_ring_id(relation, 0));
        }
        return data;
    };
    auto slot = [](int64_t relation) {
        return gc::id_alloc::SidecarSlot{uint8_t(ObjectType::OSM_RELATION), 0, 0, 0, uint64_t(relation) << 16};
    };
    auto remapped = [&](const std::string& prev, std::string& log) {
        ParsedData data = admins();
        std::ostringstream err;
        std::streambuf* was = std::cerr.rdbuf(err.rdbuf());
        apply_strategy2_remaps(data, prev, RunOrder::Serial);
        std::cerr.rdbuf(was);
        log = err.str();
        return data;
    };
    const ScratchDir prev("strategy2-admin-sidecar");
    const std::string region = std::filesystem::path(prev.path()).filename().string();
    // A sidecar anywhere but beside the exact tier is no previous build's:
    // every slot is allocated cold, and the count says so.
    std::filesystem::create_directories(prev.path() + "/quality/uncapped");
    IdAllocator::write_sidecar(prev.path() + "/quality/uncapped/admin_polygons.osm_ids", {slot(7), slot(5)});
    std::string log;
    ParsedData cold = remapped(prev.path(), log);
    CHECK_EQ(cold.admin_polygons[0].name_id, 5u);
    CHECK(log.find("ARC_CHECK slots=admin_polygons region=" + region + " previous_sidecar_missing=1") !=
          std::string::npos);
    // Beside the exact tier it gives the slots.
    std::filesystem::create_directories(prev.path() + "/quality/exact");
    IdAllocator::write_sidecar(admin_sidecar_path(prev.path()), {slot(7), slot(5)});
    ParsedData kept = remapped(prev.path(), log);
    CHECK_EQ(kept.admin_polygons[0].name_id, 7u);
    CHECK_EQ(kept.admin_polygons[1].name_id, 5u);
    CHECK(log.find("previous_sidecar_missing=0") != std::string::npos);
    CHECK_EQ(admin_sidecar_path("prev/planet"), std::string("prev/planet/quality/exact/admin_polygons.osm_ids"));
}
