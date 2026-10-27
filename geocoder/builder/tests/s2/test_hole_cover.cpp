// cover_polygon (s2_helpers.h) makes the one admin cell index every quality
// variant and admin-minimal read, while each of them simplifies a polygon's
// holes its own way: the index must list the polygon wherever one of them
// can put a point inside.
#include <s2/s2cell.h>
#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>
#include <s2/s2loop.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <unistd.h>

#include "cell_index.h"
#include "parsed_data.h"
#include "polygon_rings.h"
#include "ring_contains.h"
#include "s2_helpers.h"
#include "test_framework.h"
#include "types.h"
#include "vertex_pack.h"

namespace {

using Ring = std::vector<LatLng>;
using Cells = std::vector<std::pair<S2CellId, bool>>;

// kAdminCellLevel for the length of one test.
struct AdminCellLevel {
    int saved = kAdminCellLevel;
    explicit AdminCellLevel(int level) { kAdminCellLevel = level; }
    ~AdminCellLevel() { kAdminCellLevel = saved; }
};

// A directory removed with what's in it at the end of the test.
struct TempDir {
    std::filesystem::path path =
        std::filesystem::temp_directory_path() / ("hole-cover-test-" + std::to_string(getpid()));
    TempDir() { std::filesystem::create_directories(path); }
    ~TempDir() { std::filesystem::remove_all(path); }
};

// Polygon 0's keyhole range in the variant written to dir, as the reader
// decodes it.
std::vector<NodeCoord> written_range(const std::string& dir) {
    std::ifstream polys(dir + "/admin_polygons.bin", std::ios::binary);
    AdminPolygon ap{};
    polys.read(reinterpret_cast<char*>(&ap), sizeof ap);
    std::ifstream verts(dir + "/admin_vertices.bin", std::ios::binary);
    const std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(verts)), std::istreambuf_iterator<char>());
    REQUIRE(polys && ap.vertex_count > 0 && ap.vertex_offset < bytes.size());
    return unpack_polygon(bytes.data() + ap.vertex_offset, ap.vertex_count);
}

const std::pair<S2CellId, bool>* cell_entry(const Cells& cells, S2CellId cell) {
    auto it = std::lower_bound(cells.begin(), cells.end(), std::make_pair(cell, false));
    return it != cells.end() && it->first == cell ? &*it : nullptr;
}

// The point's cell and its neighbours at kAdminCellLevel, the cells the
// reader's admin lookup reads (Index::find_admin).
std::vector<S2CellId> lookup_cells(double lat, double lng) {
    const S2CellId cell = S2CellId(S2LatLng::FromDegrees(lat, lng)).parent(kAdminCellLevel);
    std::vector<S2CellId> out{cell};
    cell.AppendAllNeighbors(kAdminCellLevel, &out);
    return out;
}

std::unique_ptr<S2Loop> loop_of(const Ring& ring) {
    std::vector<S2Point> points;
    for (size_t k = 0; k + 1 < ring.size(); k++)
        points.push_back(S2LatLng::FromDegrees(ring[k].first, ring[k].second).ToPoint());
    auto loop = std::make_unique<S2Loop>(points);
    loop->Normalize();
    return loop;
}

bool reader_inside(const std::vector<NodeCoord>& range, double lat, double lng) {
    return ring_contains(range.data(), static_cast<uint32_t>(range.size()), static_cast<float>(lat),
                         static_cast<float>(lng));
}

}  // namespace

TEST(simplified_variants_find_a_polygon_where_their_holes_gave_way) {
    // A country with an enclave whose north edge has a 1 km deep bulge.
    // Its q2.5 variant and admin-minimal (tolerance 1250 m) straighten that
    // edge, so the bulge becomes the country's in their rings. With 150 m
    // admin cells, the middle of the bulge is 400 m and more from the
    // enclave's edge: the cell of a point there and all its neighbours lie
    // inside the enclave as the index was covered.
    const AdminCellLevel level(16);
    const Ring outer = {{10.0, 20.0}, {10.0, 20.05}, {10.075, 20.05}, {10.075, 20.0}, {10.0, 20.0}};
    const Ring hole = {{10.05, 20.04}, {10.02, 20.04}, {10.02, 20.01}, {10.05, 20.01}, {10.05, 20.019},
                       {10.059, 20.022}, {10.059, 20.028}, {10.05, 20.031}, {10.05, 20.04}};
    const Ring range = encode_rings(outer, {hole});

    ParsedData data;
    for (const auto& [lat, lng] : range) data.admin_vertices.push_back({static_cast<float>(lat), static_cast<float>(lng)});
    AdminPolygon ap{};
    ap.vertex_offset = 0;
    ap.vertex_count = static_cast<uint32_t>(range.size());
    ap.admin_level = 2;
    ap.area = 1;
    ap.hole_count = 1;
    data.admin_polygons.push_back(ap);
    data.admin_osm_ids.push_back((static_cast<uint64_t>(gc::id_alloc::ObjectType::OSM_RELATION) << 56) | (7 << 16));

    std::vector<LatLng> base;
    for (const NodeCoord& v : data.admin_vertices) base.emplace_back(v.lat, v.lng);
    Cells cells = cover_polygon(base, 1);
    std::sort(cells.begin(), cells.end());

    const TempDir tmp;
    const std::string q25 = (tmp.path / "q2.5").string(), minimal = (tmp.path / "admin-minimal").string();
    write_quality_variant(data, tmp.path.string(), q25, 2.5);
    std::vector<uint32_t> remap;
    write_admin_minimal_polygons(data, minimal, "", 2.5, remap);

    const std::unique_ptr<S2Loop> enclave = loop_of(hole);
    for (const std::string& dir : {q25, minimal}) {
        const std::vector<NodeCoord> variant = written_range(dir);
        REQUIRE(variant.size() < range.size());  // the bulge simplified away
        size_t deep = 0;
        for (int i = 0; i <= 4; i++) {
            for (int j = 0; j <= 4; j++) {
                const double lat = 10.0535 + 0.0005 * i, lng = 20.024 + 0.0005 * j;
                REQUIRE(!reader_inside(data.admin_vertices, lat, lng));  // in the enclave as given
                REQUIRE(reader_inside(variant, lat, lng));               // the country's in the variant
                const std::vector<S2CellId> lookup = lookup_cells(lat, lng);
                deep += std::all_of(lookup.begin(), lookup.end(),
                                    [&](S2CellId c) { return enclave->Contains(S2Cell(c)); });
                // The reader's lookup finds the polygon, in the point's own
                // cell, with its ring to test.
                CHECK(std::any_of(lookup.begin(), lookup.end(),
                                  [&](S2CellId c) { return cell_entry(cells, c) != nullptr; }));
                const auto* entry = cell_entry(cells, lookup[0]);
                CHECK(entry != nullptr);
                if (entry) CHECK(!entry->second);
            }
        }
        CHECK(deep > 0);
    }
}
