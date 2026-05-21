// geocoder-canonicalize: Converts a geocoder index to canonical (deterministic) format.
//
// Canonical format properties:
// - strings.bin: strings sorted alphabetically
// - Data files: records sorted by content fingerprint
// - Node/vertex files: grouped by parent record in canonical order
// - Entries/cells: derived from canonical data ordering + original cell assignments
//
// Usage: geocoder-canonicalize <input-dir> <output-dir>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <numeric>
#include <string>
#include <unordered_map>
#include <vector>

#include "patch_format.h"

// --- Canonical string pool ---
// Sort all strings alphabetically, build old_offset -> new_offset remap.

struct CanonicalStringPool {
    std::vector<char> data;
    std::unordered_map<uint32_t, uint32_t> remap; // old_offset -> new_offset

    void build(const std::vector<char>& old_pool) {
        // Extract all strings with their offsets
        std::vector<std::pair<uint32_t, const char*>> strings;
        size_t pos = 0;
        while (pos < old_pool.size()) {
            const char* s = old_pool.data() + pos;
            strings.emplace_back(static_cast<uint32_t>(pos), s);
            pos += strlen(s) + 1;
        }

        // Sort alphabetically
        std::sort(strings.begin(), strings.end(), [](const auto& a, const auto& b) {
            return strcmp(a.second, b.second) < 0;
        });

        // Build new pool + remap
        data.clear();
        data.reserve(old_pool.size());
        remap.reserve(strings.size());
        for (auto& [old_off, str] : strings) {
            uint32_t new_off = static_cast<uint32_t>(data.size());
            remap[old_off] = new_off;
            size_t len = strlen(str);
            data.insert(data.end(), str, str + len + 1);
        }
    }

    uint32_t map(uint32_t old_off) const {
        auto it = remap.find(old_off);
        return (it != remap.end()) ? it->second : 0;
    }
};

// --- Cell assignment extraction ---
// Parse geo_cells.bin + entries files to extract cell -> [item_id] mappings.

struct GeoCellEntry {
    uint64_t cell_id;
    uint32_t street_offset;
    uint32_t addr_offset;
    uint32_t interp_offset;
};

struct CellAssignments {
    // For each item type: cell_id -> sorted list of item_ids
    std::unordered_map<uint64_t, std::vector<uint32_t>> street_cells;
    std::unordered_map<uint64_t, std::vector<uint32_t>> addr_cells;
    std::unordered_map<uint64_t, std::vector<uint32_t>> interp_cells;
    std::unordered_map<uint64_t, std::vector<uint32_t>> admin_cells; // with flags
};

static std::vector<uint32_t> parse_entry(const std::vector<char>& entries, uint32_t offset) {
    if (offset == 0xFFFFFFFF || offset + 2 > entries.size()) return {};
    uint16_t count;
    memcpy(&count, entries.data() + offset, 2);
    std::vector<uint32_t> ids(count);
    if (offset + 2 + count * 4 > entries.size()) return {};
    memcpy(ids.data(), entries.data() + offset + 2, count * 4);
    return ids;
}

static CellAssignments extract_cell_assignments(const std::string& dir) {
    CellAssignments ca;

    // Geo cells
    auto geo_data = read_file(dir + "/geo_cells.bin");
    auto street_entries = read_file(dir + "/street_entries.bin");
    auto addr_entries = read_file(dir + "/addr_entries.bin");
    auto interp_entries = read_file(dir + "/interp_entries.bin");

    size_t n_geo = geo_data.size() / 20;
    for (size_t i = 0; i < n_geo; i++) {
        GeoCellEntry e;
        memcpy(&e.cell_id, geo_data.data() + i * 20, 8);
        memcpy(&e.street_offset, geo_data.data() + i * 20 + 8, 4);
        memcpy(&e.addr_offset, geo_data.data() + i * 20 + 12, 4);
        memcpy(&e.interp_offset, geo_data.data() + i * 20 + 16, 4);

        auto s_ids = parse_entry(street_entries, e.street_offset);
        if (!s_ids.empty()) ca.street_cells[e.cell_id] = std::move(s_ids);

        auto a_ids = parse_entry(addr_entries, e.addr_offset);
        if (!a_ids.empty()) ca.addr_cells[e.cell_id] = std::move(a_ids);

        auto i_ids = parse_entry(interp_entries, e.interp_offset);
        if (!i_ids.empty()) ca.interp_cells[e.cell_id] = std::move(i_ids);
    }

    // Admin cells
    auto admin_cell_data = read_file(dir + "/admin_cells.bin");
    auto admin_entry_data = read_file(dir + "/admin_entries.bin");
    size_t n_admin = admin_cell_data.size() / 12;
    for (size_t i = 0; i < n_admin; i++) {
        uint64_t cell_id;
        uint32_t offset;
        memcpy(&cell_id, admin_cell_data.data() + i * 12, 8);
        memcpy(&offset, admin_cell_data.data() + i * 12 + 8, 4);
        auto ids = parse_entry(admin_entry_data, offset);
        if (!ids.empty()) ca.admin_cells[cell_id] = std::move(ids);
    }

    return ca;
}

// --- Remap cell assignments with old->new ID permutation ---
static void remap_cells(std::unordered_map<uint64_t, std::vector<uint32_t>>& cells,
                         const std::vector<uint32_t>& old_to_new) {
    for (auto& [cell_id, ids] : cells) {
        for (auto& id : ids) {
            uint32_t masked = id & 0x7FFFFFFF;
            uint32_t flags = id & 0x80000000;
            if (masked < old_to_new.size()) {
                id = old_to_new[masked] | flags;
            }
        }
        std::sort(ids.begin(), ids.end());
    }
}

// --- Write entries + cells files ---
static void write_entries_and_cells(
    const std::string& cells_path,
    const std::string& entries_path,
    const std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map,
    size_t cell_entry_size) // 20 for geo, 12 for admin
{
    // Collect and sort cells
    std::vector<uint64_t> sorted_cells;
    sorted_cells.reserve(cell_map.size());
    for (auto& [id, _] : cell_map) sorted_cells.push_back(id);
    std::sort(sorted_cells.begin(), sorted_cells.end());

    // Write entries file
    std::vector<char> entries_buf;
    std::unordered_map<uint64_t, uint32_t> cell_offsets;
    for (uint64_t cell_id : sorted_cells) {
        auto it = cell_map.find(cell_id);
        if (it == cell_map.end() || it->second.empty()) continue;
        cell_offsets[cell_id] = static_cast<uint32_t>(entries_buf.size());
        uint16_t count = static_cast<uint16_t>(it->second.size());
        entries_buf.insert(entries_buf.end(), reinterpret_cast<const char*>(&count),
                          reinterpret_cast<const char*>(&count) + 2);
        entries_buf.insert(entries_buf.end(),
                          reinterpret_cast<const char*>(it->second.data()),
                          reinterpret_cast<const char*>(it->second.data()) + it->second.size() * 4);
    }
    write_file(entries_path, entries_buf);

    // Write cells file
    std::vector<char> cells_buf;
    uint32_t no_data = 0xFFFFFFFF;
    for (uint64_t cell_id : sorted_cells) {
        cells_buf.insert(cells_buf.end(), reinterpret_cast<const char*>(&cell_id),
                        reinterpret_cast<const char*>(&cell_id) + 8);
        auto it = cell_offsets.find(cell_id);
        uint32_t off = (it != cell_offsets.end()) ? it->second : no_data;
        cells_buf.insert(cells_buf.end(), reinterpret_cast<const char*>(&off),
                        reinterpret_cast<const char*>(&off) + 4);
    }
    write_file(cells_path, cells_buf);
}

// Write merged geo_cells with 3 offset columns
static void write_geo_cells_and_entries(
    const std::string& dir,
    const std::unordered_map<uint64_t, std::vector<uint32_t>>& street_cells,
    const std::unordered_map<uint64_t, std::vector<uint32_t>>& addr_cells,
    const std::unordered_map<uint64_t, std::vector<uint32_t>>& interp_cells)
{
    // Collect all unique cell IDs
    std::vector<uint64_t> all_cells;
    auto add = [&](const auto& m) { for (auto& [id, _] : m) all_cells.push_back(id); };
    add(street_cells); add(addr_cells); add(interp_cells);
    std::sort(all_cells.begin(), all_cells.end());
    all_cells.erase(std::unique(all_cells.begin(), all_cells.end()), all_cells.end());

    // Write each entry file and collect offsets
    auto write_entries = [&](const std::string& path,
                              const std::unordered_map<uint64_t, std::vector<uint32_t>>& cells)
        -> std::unordered_map<uint64_t, uint32_t>
    {
        std::vector<char> buf;
        std::unordered_map<uint64_t, uint32_t> offsets;
        for (uint64_t cell_id : all_cells) {
            auto it = cells.find(cell_id);
            if (it == cells.end() || it->second.empty()) continue;
            offsets[cell_id] = static_cast<uint32_t>(buf.size());
            uint16_t count = static_cast<uint16_t>(it->second.size());
            buf.insert(buf.end(), reinterpret_cast<const char*>(&count),
                      reinterpret_cast<const char*>(&count) + 2);
            buf.insert(buf.end(), reinterpret_cast<const char*>(it->second.data()),
                      reinterpret_cast<const char*>(it->second.data()) + it->second.size() * 4);
        }
        write_file(path, buf);
        return offsets;
    };

    auto s_off = write_entries(dir + "/street_entries.bin", street_cells);
    auto a_off = write_entries(dir + "/addr_entries.bin", addr_cells);
    auto i_off = write_entries(dir + "/interp_entries.bin", interp_cells);

    // Write geo_cells.bin
    uint32_t no_data = 0xFFFFFFFF;
    std::vector<char> buf;
    buf.reserve(all_cells.size() * 20);
    for (uint64_t cell_id : all_cells) {
        buf.insert(buf.end(), reinterpret_cast<const char*>(&cell_id),
                  reinterpret_cast<const char*>(&cell_id) + 8);
        auto get = [&](const auto& m) -> uint32_t {
            auto it = m.find(cell_id);
            return (it != m.end()) ? it->second : no_data;
        };
        uint32_t so = get(s_off), ao = get(a_off), io = get(i_off);
        buf.insert(buf.end(), reinterpret_cast<const char*>(&so), reinterpret_cast<const char*>(&so) + 4);
        buf.insert(buf.end(), reinterpret_cast<const char*>(&ao), reinterpret_cast<const char*>(&ao) + 4);
        buf.insert(buf.end(), reinterpret_cast<const char*>(&io), reinterpret_cast<const char*>(&io) + 4);
    }
    write_file(dir + "/geo_cells.bin", buf);
}

// --- Main ---

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: geocoder-canonicalize <input-dir> <output-dir>" << std::endl;
        return 1;
    }

    std::string input_dir = argv[1];
    std::string output_dir = argv[2];
    ensure_dir(output_dir);

    std::cerr << "Loading index from " << input_dir << "..." << std::endl;

    // 1. Sort string pool
    std::cerr << "  Sorting string pool..." << std::endl;
    auto old_strings = read_file(input_dir + "/strings.bin");
    CanonicalStringPool pool;
    pool.build(old_strings);
    write_file(output_dir + "/strings.bin", pool.data);
    std::cerr << "    " << pool.remap.size() << " strings" << std::endl;

    // 2. Extract cell assignments before reordering
    std::cerr << "  Extracting cell assignments..." << std::endl;
    auto ca = extract_cell_assignments(input_dir);

    // 3. Canonicalize addr_points
    std::cerr << "  Sorting addr_points..." << std::endl;
    {
        auto points = read_structs<PatchAddrPoint>(input_dir + "/addr_points.bin");
        size_t n = points.size();
        std::vector<uint32_t> order(n);
        std::iota(order.begin(), order.end(), 0);

        // Sort by canonical key: (street, housenumber, lat_grid, lng_grid)
        std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
            const auto& pa = points[a];
            const auto& pb = points[b];
            // Compare by remapped street name
            uint32_t sa = pool.map(pa.street_id), sb = pool.map(pb.street_id);
            int c = strcmp(pool.data.data() + sa, pool.data.data() + sb);
            if (c != 0) return c < 0;
            // Then housenumber
            uint32_t ha = pool.map(pa.housenumber_id), hb = pool.map(pb.housenumber_id);
            c = strcmp(pool.data.data() + ha, pool.data.data() + hb);
            if (c != 0) return c < 0;
            // Then coordinates
            int la = to_grid(pa.lat), lb = to_grid(pb.lat);
            if (la != lb) return la < lb;
            return to_grid(pa.lng) < to_grid(pb.lng);
        });

        // Build old->new mapping and write
        std::vector<uint32_t> old_to_new(n);
        for (uint32_t i = 0; i < n; i++) old_to_new[order[i]] = i;

        std::vector<PatchAddrPoint> sorted(n);
        for (uint32_t i = 0; i < n; i++) {
            sorted[i] = points[order[i]];
            sorted[i].housenumber_id = pool.map(sorted[i].housenumber_id);
            sorted[i].street_id = pool.map(sorted[i].street_id);
        }
        write_file(output_dir + "/addr_points.bin",
                   reinterpret_cast<const char*>(sorted.data()), n * sizeof(PatchAddrPoint));
        remap_cells(ca.addr_cells, old_to_new);
        std::cerr << "    " << n << " addr points" << std::endl;
    }

    // 4. Canonicalize street_ways + street_nodes
    std::cerr << "  Sorting street_ways..." << std::endl;
    {
        auto ways = read_ways(input_dir + "/street_ways.bin");
        auto nodes = read_structs<PatchNodeCoord>(input_dir + "/street_nodes.bin");
        size_t n = ways.size();
        std::vector<uint32_t> order(n);
        std::iota(order.begin(), order.end(), 0);

        // Sort by (name, node_count, first_node_lat, first_node_lng)
        std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
            const auto& wa = ways[a];
            const auto& wb = ways[b];
            uint32_t na = pool.map(wa.name_id), nb = pool.map(wb.name_id);
            int c = strcmp(pool.data.data() + na, pool.data.data() + nb);
            if (c != 0) return c < 0;
            if (wa.node_count != wb.node_count) return wa.node_count < wb.node_count;
            // Compare first node
            if (wa.node_count > 0 && wb.node_count > 0) {
                int la = to_grid(nodes[wa.node_offset].lat);
                int lb = to_grid(nodes[wb.node_offset].lat);
                if (la != lb) return la < lb;
                int lga = to_grid(nodes[wa.node_offset].lng);
                int lgb = to_grid(nodes[wb.node_offset].lng);
                if (lga != lgb) return lga < lgb;
            }
            return false;
        });

        std::vector<uint32_t> old_to_new(n);
        for (uint32_t i = 0; i < n; i++) old_to_new[order[i]] = i;

        // Write sorted ways + nodes, preserving original stride
        auto orig_way_data = read_file(input_dir + "/street_ways.bin");
        size_t way_stride = (orig_way_data.size() % 12 == 0 && n > 0) ? 12 : 9;

        std::vector<char> ways_buf;
        ways_buf.reserve(n * way_stride);
        std::vector<PatchNodeCoord> sorted_nodes;
        sorted_nodes.reserve(nodes.size());

        for (uint32_t i = 0; i < n; i++) {
            const char* src = orig_way_data.data() + order[i] * way_stride;
            char record[12] = {};
            memcpy(record, src, way_stride);

            // Update node_offset (offset 0)
            uint32_t new_node_offset = static_cast<uint32_t>(sorted_nodes.size());
            memcpy(record, &new_node_offset, 4);

            // Update name_id
            size_t name_off = (way_stride == 12) ? 8 : 5;
            uint32_t old_name_id;
            memcpy(&old_name_id, record + name_off, 4);
            uint32_t new_name_id = pool.map(old_name_id);
            memcpy(record + name_off, &new_name_id, 4);

            ways_buf.insert(ways_buf.end(), record, record + way_stride);

            PatchWayHeader w = ways[order[i]];
            for (uint8_t j = 0; j < w.node_count; j++) {
                if (w.node_offset + j < nodes.size())
                    sorted_nodes.push_back(nodes[w.node_offset + j]);
            }
        }
        write_file(output_dir + "/street_ways.bin", ways_buf);
        write_file(output_dir + "/street_nodes.bin",
                   reinterpret_cast<const char*>(sorted_nodes.data()),
                   sorted_nodes.size() * sizeof(PatchNodeCoord));
        remap_cells(ca.street_cells, old_to_new);
        std::cerr << "    " << n << " ways, " << sorted_nodes.size() << " nodes" << std::endl;
    }

    // 5. Canonicalize interp_ways + interp_nodes
    std::cerr << "  Sorting interp_ways..." << std::endl;
    {
        auto interps = read_interps(input_dir + "/interp_ways.bin");
        auto nodes = read_structs<PatchNodeCoord>(input_dir + "/interp_nodes.bin");
        size_t n = interps.size();
        std::vector<uint32_t> order(n);
        std::iota(order.begin(), order.end(), 0);

        std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
            const auto& ia = interps[a];
            const auto& ib = interps[b];
            uint32_t sa = pool.map(ia.street_id), sb = pool.map(ib.street_id);
            int c = strcmp(pool.data.data() + sa, pool.data.data() + sb);
            if (c != 0) return c < 0;
            if (ia.start_number != ib.start_number) return ia.start_number < ib.start_number;
            if (ia.end_number != ib.end_number) return ia.end_number < ib.end_number;
            return ia.interpolation < ib.interpolation;
        });

        std::vector<uint32_t> old_to_new(n);
        for (uint32_t i = 0; i < n; i++) old_to_new[order[i]] = i;

        // Write sorted interps + nodes using builder's native struct size
        auto orig_data = read_file(input_dir + "/interp_ways.bin");
        size_t stride = (orig_data.size() % 20 == 0 && n > 0) ? 20 :
                        (orig_data.size() % 18 == 0 && n > 0) ? 18 : 20;

        std::vector<char> interp_buf;
        interp_buf.reserve(n * stride);
        std::vector<PatchNodeCoord> sorted_nodes;
        sorted_nodes.reserve(nodes.size());

        for (uint32_t i = 0; i < n; i++) {
            // Copy the raw bytes from original file for correct padding
            const char* src = orig_data.data() + order[i] * stride;
            char record[24] = {};
            memcpy(record, src, stride);

            // Update node_offset
            uint32_t new_node_offset = static_cast<uint32_t>(sorted_nodes.size());
            memcpy(record, &new_node_offset, 4);

            // Update street_id (at offset 5 or 8 depending on padding)
            size_t street_off = (stride == 20) ? 8 : 5;
            uint32_t old_street_id;
            memcpy(&old_street_id, record + street_off, 4);
            uint32_t new_street_id = pool.map(old_street_id);
            memcpy(record + street_off, &new_street_id, 4);

            interp_buf.insert(interp_buf.end(), record, record + stride);

            // Copy nodes
            const auto& iw = interps[order[i]];
            for (uint8_t j = 0; j < iw.node_count; j++) {
                if (iw.node_offset + j < nodes.size())
                    sorted_nodes.push_back(nodes[iw.node_offset + j]);
            }
        }
        write_file(output_dir + "/interp_ways.bin", interp_buf);
        write_file(output_dir + "/interp_nodes.bin",
                   reinterpret_cast<const char*>(sorted_nodes.data()),
                   sorted_nodes.size() * sizeof(PatchNodeCoord));
        remap_cells(ca.interp_cells, old_to_new);
        std::cerr << "    " << n << " interps, " << sorted_nodes.size() << " nodes" << std::endl;
    }

    // 6. Canonicalize admin_polygons + admin_vertices
    std::cerr << "  Sorting admin_polygons..." << std::endl;
    {
        auto polygons = read_admin_polygons(input_dir + "/admin_polygons.bin");
        auto vertices = read_structs<PatchNodeCoord>(input_dir + "/admin_vertices.bin");
        size_t n = polygons.size();
        std::vector<uint32_t> order(n);
        std::iota(order.begin(), order.end(), 0);

        std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
            const auto& pa = polygons[a];
            const auto& pb = polygons[b];
            uint32_t na = pool.map(pa.name_id), nb = pool.map(pb.name_id);
            int c = strcmp(pool.data.data() + na, pool.data.data() + nb);
            if (c != 0) return c < 0;
            if (pa.admin_level != pb.admin_level) return pa.admin_level < pb.admin_level;
            if (pa.country_code != pb.country_code) return pa.country_code < pb.country_code;
            return pa.vertex_count < pb.vertex_count;
        });

        std::vector<uint32_t> old_to_new(n);
        for (uint32_t i = 0; i < n; i++) old_to_new[order[i]] = i;

        // Write sorted polygons + vertices using builder's native struct size
        auto orig_data = read_file(input_dir + "/admin_polygons.bin");
        size_t stride = 0;
        for (size_t s : {24, 20, 19}) {
            if (orig_data.size() % s == 0 && n > 0) { stride = s; break; }
        }
        if (stride == 0) stride = 24;

        std::vector<char> poly_buf;
        poly_buf.reserve(n * stride);
        std::vector<PatchNodeCoord> sorted_verts;
        sorted_verts.reserve(vertices.size());

        for (uint32_t i = 0; i < n; i++) {
            const char* src = orig_data.data() + order[i] * stride;
            char record[32] = {};
            memcpy(record, src, stride);

            // Update vertex_offset
            uint32_t new_vert_offset = static_cast<uint32_t>(sorted_verts.size());
            memcpy(record, &new_vert_offset, 4);

            // Update name_id (at offset 8)
            uint32_t old_name_id;
            memcpy(&old_name_id, record + 8, 4);
            uint32_t new_name_id = pool.map(old_name_id);
            memcpy(record + 8, &new_name_id, 4);

            // Recompute area for canonical output
            // (area depends on vertex order which hasn't changed, so copy as-is)

            poly_buf.insert(poly_buf.end(), record, record + stride);

            // Copy vertices
            const auto& p = polygons[order[i]];
            for (uint32_t j = 0; j < p.vertex_count; j++) {
                if (p.vertex_offset + j < vertices.size())
                    sorted_verts.push_back(vertices[p.vertex_offset + j]);
            }
        }
        write_file(output_dir + "/admin_polygons.bin", poly_buf);
        write_file(output_dir + "/admin_vertices.bin",
                   reinterpret_cast<const char*>(sorted_verts.data()),
                   sorted_verts.size() * sizeof(PatchNodeCoord));
        remap_cells(ca.admin_cells, old_to_new);
        std::cerr << "    " << n << " polygons, " << sorted_verts.size() << " vertices" << std::endl;
    }

    // 7. Write canonical entries and cell index files
    std::cerr << "  Writing cell indexes..." << std::endl;
    write_geo_cells_and_entries(output_dir, ca.street_cells, ca.addr_cells, ca.interp_cells);
    write_entries_and_cells(output_dir + "/admin_cells.bin", output_dir + "/admin_entries.bin",
                           ca.admin_cells, 12);

    std::cerr << "Done. Canonical output in " << output_dir << std::endl;
    return 0;
}
