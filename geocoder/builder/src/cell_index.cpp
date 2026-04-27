#include "cell_index.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <fstream>
#include <future>
#include <iostream>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <unistd.h>

#include "geometry.h"
#include "postcode_validation.h"
#include "s2_helpers.h"

#include <s2/s2latlng.h>

// Write strings_layout.json — records each tier's [start, end) in the
// global string-offset space so the server can route a global offset to
// the right tier file. Same content regardless of which subset of tier
// files a given dir contains.
static inline void write_strings_layout(const std::string& dir, const ParsedData& data) {
    std::ofstream f(dir + "/strings_layout.json");
    f << "{\n  \"tiers\": [\n";
    for (size_t t = 0; t < STR_TIER_COUNT; t++) {
        f << "    {\"name\": \"" << STR_TIER_NAMES[t]
          << "\", \"file\": \"" << STR_TIER_FILENAMES[t]
          << "\", \"start\": " << data.strings_tier_bases[t]
          << ", \"end\": " << data.strings_tier_bases[t + 1] << "}";
        if (t + 1 < STR_TIER_COUNT) f << ",";
        f << "\n";
    }
    f << "  ]\n}\n";
}

std::vector<uint32_t> write_entries(
    const std::string& path,
    const std::vector<uint64_t>& sorted_cells,
    const std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map
) {
    struct CellRef { uint64_t cell_id; const std::vector<uint32_t>* ids; };
    std::vector<CellRef> sorted_refs;
    sorted_refs.reserve(cell_map.size());
    for (auto& [id, ids] : cell_map) sorted_refs.push_back({id, &ids});
    std::sort(sorted_refs.begin(), sorted_refs.end(),
        [](const CellRef& a, const CellRef& b) { return a.cell_id < b.cell_id; });

    std::vector<uint32_t> offsets(sorted_cells.size(), NO_DATA);
    size_t total_size = 0;
    for (auto& r : sorted_refs) total_size += sizeof(uint16_t) + r.ids->size() * sizeof(uint32_t);

    std::vector<char> buf;
    buf.reserve(total_size);
    uint32_t current = 0;
    size_t ri = 0;
    for (uint32_t si = 0; si < sorted_cells.size() && ri < sorted_refs.size(); si++) {
        if (sorted_cells[si] < sorted_refs[ri].cell_id) continue;
        if (sorted_cells[si] > sorted_refs[ri].cell_id) { si--; ri++; continue; }
        offsets[si] = current;
        const auto& ids = *sorted_refs[ri].ids;
        uint16_t count = static_cast<uint16_t>(std::min(ids.size(), size_t(MAX_VERTEX_COUNT)));
        buf.insert(buf.end(), reinterpret_cast<const char*>(&count),
                   reinterpret_cast<const char*>(&count) + sizeof(count));
        buf.insert(buf.end(), reinterpret_cast<const char*>(ids.data()),
                   reinterpret_cast<const char*>(ids.data()) + ids.size() * sizeof(uint32_t));
        current += sizeof(uint16_t) + ids.size() * sizeof(uint32_t);
        ri++;
    }
    std::ofstream f(path, std::ios::binary);
    f.write(buf.data(), buf.size());
    return offsets;
}

std::vector<uint32_t> write_entries_from_sorted(
    const std::string& path,
    const std::vector<uint64_t>& sorted_cells,
    const std::vector<CellItemPair>& sorted_pairs
) {
    std::vector<uint32_t> offsets(sorted_cells.size(), NO_DATA);
    if (sorted_pairs.empty()) {
        std::ofstream f(path, std::ios::binary);
        return offsets;
    }

    unsigned int nthreads = std::thread::hardware_concurrency();
    if (nthreads == 0) nthreads = 4;
    size_t cells_per_chunk = (sorted_cells.size() + nthreads - 1) / nthreads;

    struct ChunkResult {
        std::vector<char> buf;
        size_t cell_start, cell_end;
        uint32_t local_size;
    };
    std::vector<ChunkResult> chunks(nthreads);

    std::vector<std::thread> threads;
    for (unsigned int t = 0; t < nthreads; t++) {
        size_t cs = t * cells_per_chunk;
        size_t ce = std::min(cs + cells_per_chunk, sorted_cells.size());
        if (cs >= sorted_cells.size()) break;

        threads.emplace_back([&, t, cs, ce]() {
            auto& chunk = chunks[t];
            chunk.cell_start = cs;
            chunk.cell_end = ce;
            chunk.local_size = 0;

            size_t pi = std::lower_bound(sorted_pairs.begin(), sorted_pairs.end(),
                sorted_cells[cs], [](const CellItemPair& p, uint64_t id) {
                    return p.cell_id < id;
                }) - sorted_pairs.begin();

            for (size_t si = cs; si < ce && pi < sorted_pairs.size(); si++) {
                if (sorted_cells[si] < sorted_pairs[pi].cell_id) continue;
                while (pi < sorted_pairs.size() && sorted_pairs[pi].cell_id < sorted_cells[si]) pi++;
                if (pi >= sorted_pairs.size() || sorted_pairs[pi].cell_id != sorted_cells[si]) continue;

                offsets[si] = chunk.local_size;
                size_t start = pi;
                while (pi < sorted_pairs.size() && sorted_pairs[pi].cell_id == sorted_cells[si]) pi++;
                uint16_t count = static_cast<uint16_t>(std::min(pi - start, size_t(MAX_VERTEX_COUNT)));
                size_t entry_size = sizeof(uint16_t) + (pi - start) * sizeof(uint32_t);
                size_t buf_pos = chunk.buf.size();
                chunk.buf.resize(buf_pos + entry_size);
                memcpy(chunk.buf.data() + buf_pos, &count, sizeof(count));
                for (size_t k = start; k < pi; k++) {
                    memcpy(chunk.buf.data() + buf_pos + sizeof(uint16_t) + (k - start) * sizeof(uint32_t),
                           &sorted_pairs[k].item_id, sizeof(uint32_t));
                }
                chunk.local_size += entry_size;
            }
        });
    }
    for (auto& t : threads) t.join();

    uint32_t global_offset = 0;
    for (auto& chunk : chunks) {
        for (size_t si = chunk.cell_start; si < chunk.cell_end; si++) {
            if (offsets[si] != NO_DATA) offsets[si] += global_offset;
        }
        global_offset += chunk.local_size;
    }

    std::vector<char> buf;
    buf.reserve(global_offset);
    for (auto& chunk : chunks) buf.insert(buf.end(), chunk.buf.begin(), chunk.buf.end());

    std::ofstream f(path, std::ios::binary);
    f.write(buf.data(), buf.size());
    return offsets;
}

void write_cell_index(
    const std::string& cells_path,
    const std::string& entries_path,
    const std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map
) {
    std::vector<std::pair<uint64_t, std::vector<uint32_t>>> sorted(cell_map.begin(), cell_map.end());
    std::sort(sorted.begin(), sorted.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    // Determinism safety net: sort each cell's inner entry vector by
    // value. Callers that build the map from parallel workers can push
    // entries in thread-scheduling order, which leaks non-determinism
    // into the on-disk layout and breaks incremental patching. Sorting
    // here is cheap compared to the rest of the index write.
    for (auto& [cell_id, ids] : sorted) {
        std::sort(ids.begin(), ids.end());
    }

    { std::ofstream f(cells_path, std::ios::binary);
      uint32_t current_offset = 0;
      for (const auto& [cell_id, ids] : sorted) {
          f.write(reinterpret_cast<const char*>(&cell_id), sizeof(cell_id));
          f.write(reinterpret_cast<const char*>(&current_offset), sizeof(current_offset));
          current_offset += sizeof(uint16_t) + ids.size() * sizeof(uint32_t);
      } }

    { std::ofstream f(entries_path, std::ios::binary);
      for (const auto& [cell_id, ids] : sorted) {
          uint16_t count = static_cast<uint16_t>(std::min(ids.size(), size_t(MAX_VERTEX_COUNT)));
          f.write(reinterpret_cast<const char*>(&count), sizeof(count));
          f.write(reinterpret_cast<const char*>(ids.data()), ids.size() * sizeof(uint32_t));
      } }
}

void write_index(const ParsedData& data, const std::string& output_dir, IndexMode mode) {
    ensure_dir(output_dir);
    auto _wt = std::chrono::steady_clock::now();

    bool write_streets = (mode != IndexMode::AdminOnly);
    bool write_addresses = (mode == IndexMode::Full);

    if (write_streets) {
        std::vector<uint64_t> sorted_geo_cells;
        {
            auto extract_unique_cells = [](const std::vector<CellItemPair>& pairs) {
                std::vector<uint64_t> cells;
                cells.reserve(pairs.size() / 2);
                for (size_t i = 0; i < pairs.size(); ) {
                    cells.push_back(pairs[i].cell_id);
                    uint64_t cur = pairs[i].cell_id;
                    while (i < pairs.size() && pairs[i].cell_id == cur) i++;
                }
                return cells;
            };
            auto extract_from_map = [](const std::unordered_map<uint64_t, std::vector<uint32_t>>& m) {
                std::vector<uint64_t> cells;
                cells.reserve(m.size());
                for (auto& [id, _] : m) cells.push_back(id);
                std::sort(cells.begin(), cells.end());
                return cells;
            };

            std::vector<uint64_t> way_cells, addr_cells, interp_cells;
            {
                auto f1 = std::async(std::launch::async, [&] {
                    return !data.sorted_way_cells.empty()
                        ? extract_unique_cells(data.sorted_way_cells)
                        : extract_from_map(data.cell_to_ways);
                });
                if (write_addresses) {
                    auto f2 = std::async(std::launch::async, [&] {
                        return !data.sorted_addr_cells.empty()
                            ? extract_unique_cells(data.sorted_addr_cells)
                            : extract_from_map(data.cell_to_addrs);
                    });
                    auto f3 = std::async(std::launch::async, [&] {
                        return !data.sorted_interp_cells.empty()
                            ? extract_unique_cells(data.sorted_interp_cells)
                            : extract_from_map(data.cell_to_interps);
                    });
                    addr_cells = f2.get();
                    interp_cells = f3.get();
                }
                way_cells = f1.get();
            }

            sorted_geo_cells.reserve(way_cells.size() + addr_cells.size() + interp_cells.size());
            std::merge(way_cells.begin(), way_cells.end(), addr_cells.begin(), addr_cells.end(),
                       std::back_inserter(sorted_geo_cells));
            if (!interp_cells.empty()) {
                std::vector<uint64_t> tmp;
                tmp.reserve(sorted_geo_cells.size() + interp_cells.size());
                std::merge(sorted_geo_cells.begin(), sorted_geo_cells.end(),
                           interp_cells.begin(), interp_cells.end(), std::back_inserter(tmp));
                sorted_geo_cells = std::move(tmp);
            }
            sorted_geo_cells.erase(std::unique(sorted_geo_cells.begin(), sorted_geo_cells.end()),
                                    sorted_geo_cells.end());
        }

        std::vector<uint32_t> street_offsets, addr_offsets, interp_offsets;
        {
            auto f1 = std::async(std::launch::async, [&]() {
                if (!data.sorted_way_cells.empty())
                    return write_entries_from_sorted(output_dir + "/street_entries.bin", sorted_geo_cells, data.sorted_way_cells);
                return write_entries(output_dir + "/street_entries.bin", sorted_geo_cells, data.cell_to_ways);
            });
            if (write_addresses) {
                auto f2 = std::async(std::launch::async, [&]() {
                    if (!data.sorted_addr_cells.empty())
                        return write_entries_from_sorted(output_dir + "/addr_entries.bin", sorted_geo_cells, data.sorted_addr_cells);
                    return write_entries(output_dir + "/addr_entries.bin", sorted_geo_cells, data.cell_to_addrs);
                });
                auto f3 = std::async(std::launch::async, [&]() {
                    if (!data.sorted_interp_cells.empty())
                        return write_entries_from_sorted(output_dir + "/interp_entries.bin", sorted_geo_cells, data.sorted_interp_cells);
                    return write_entries(output_dir + "/interp_entries.bin", sorted_geo_cells, data.cell_to_interps);
                });
                addr_offsets = f2.get();
                interp_offsets = f3.get();
            }
            street_offsets = f1.get();
        }

        log_phase("  Write: entry files", _wt);
        {
            size_t n = sorted_geo_cells.size();
            size_t row_size = sizeof(uint64_t) + 3 * sizeof(uint32_t);
            std::vector<char> buf(n * row_size);
            if (addr_offsets.empty()) addr_offsets.resize(n, NO_DATA);
            if (interp_offsets.empty()) interp_offsets.resize(n, NO_DATA);

            unsigned int nthreads = std::thread::hardware_concurrency();
            if (nthreads == 0) nthreads = 4;
            size_t chunk = (n + nthreads - 1) / nthreads;
            std::vector<std::thread> fill_threads;
            for (unsigned int t = 0; t < nthreads; t++) {
                size_t start = t * chunk;
                size_t end = std::min(start + chunk, n);
                if (start >= n) break;
                fill_threads.emplace_back([&, start, end]() {
                    char* ptr = buf.data() + start * row_size;
                    for (size_t i = start; i < end; i++) {
                        memcpy(ptr, &sorted_geo_cells[i], sizeof(uint64_t)); ptr += sizeof(uint64_t);
                        memcpy(ptr, &street_offsets[i], sizeof(uint32_t)); ptr += sizeof(uint32_t);
                        memcpy(ptr, &addr_offsets[i], sizeof(uint32_t)); ptr += sizeof(uint32_t);
                        memcpy(ptr, &interp_offsets[i], sizeof(uint32_t)); ptr += sizeof(uint32_t);
                    }
                });
            }
            for (auto& t : fill_threads) t.join();

            std::ofstream f(output_dir + "/geo_cells.bin", std::ios::binary);
            f.write(buf.data(), buf.size());
        }

        std::cerr << "geo index: " << sorted_geo_cells.size() << " cells ("
                  << data.ways.size() << " ways, " << data.addr_points.size() << " addrs, "
                  << data.interp_ways.size() << " interps)" << std::endl;
    }

    log_phase("  Write: geo_cells.bin", _wt);
    auto admin_future = std::async(std::launch::async, [&] {
        write_cell_index(output_dir + "/admin_cells.bin", output_dir + "/admin_entries.bin", data.cell_to_admin);
        std::cerr << "admin index: " << data.cell_to_admin.size() << " cells, " << data.admin_polygons.size() << " polygons" << std::endl;
    });

    std::vector<std::future<void>> write_futures;
    if (write_streets) {
        write_futures.push_back(std::async(std::launch::async, [&] {
            std::ofstream f(output_dir + "/street_ways.bin", std::ios::binary);
            f.write(reinterpret_cast<const char*>(data.ways.data()), data.ways.size() * sizeof(WayHeader));
        }));
        write_futures.push_back(std::async(std::launch::async, [&] {
            std::ofstream f(output_dir + "/street_nodes.bin", std::ios::binary);
            f.write(reinterpret_cast<const char*>(data.street_nodes.data()), data.street_nodes.size() * sizeof(NodeCoord));
        }));
        // Way parent chain (parallel array indexed by way_id)
        if (!data.way_parent_ids.empty()) {
            write_futures.push_back(std::async(std::launch::async, [&] {
                std::ofstream f(output_dir + "/way_parents.bin", std::ios::binary);
                f.write(reinterpret_cast<const char*>(data.way_parent_ids.data()),
                        data.way_parent_ids.size() * sizeof(uint32_t));
            }));
        }
        // Admin parent chain (parallel array indexed by polygon_id)
        if (!data.admin_parent_ids.empty()) {
            write_futures.push_back(std::async(std::launch::async, [&] {
                std::ofstream f(output_dir + "/admin_parents.bin", std::ios::binary);
                f.write(reinterpret_cast<const char*>(data.admin_parent_ids.data()),
                        data.admin_parent_ids.size() * sizeof(uint32_t));
            }));
        }
        // Per-way postcode (parallel array indexed by way_id)
        if (!data.way_postcode_ids.empty()) {
            write_futures.push_back(std::async(std::launch::async, [&] {
                std::ofstream f(output_dir + "/way_postcodes.bin", std::ios::binary);
                f.write(reinterpret_cast<const char*>(data.way_postcode_ids.data()),
                        data.way_postcode_ids.size() * sizeof(uint32_t));
            }));
        }
        if (write_addresses) {
            // Pack addr_point polygon footprints with the same encoding
            // scheme used for admin/POI vertices.  Most addr_points are
            // single-coord (vertex_count == 0) and contribute nothing
            // to the byte stream; the ~5% with building footprints get
            // a 10-byte header + delta-encoded vertices.  Cuts the
            // 5 GB planet addr_vertices.bin to ~2.5 GB.
            write_futures.push_back(std::async(std::launch::async, [&] {
                std::vector<AddrPoint> packed_points;
                packed_points.reserve(data.addr_points.size());
                std::vector<uint8_t> packed_bytes;
                // Only ~5% of addr_points carry footprints; reserve a
                // small fraction to avoid the over-allocation that
                // contributed to OOM on planet-scale runs.
                packed_bytes.reserve(data.addr_points.size() / 16);
                for (size_t i = 0; i < data.addr_points.size(); i++) {
                    AddrPoint ap = data.addr_points[i];
                    if (ap.vertex_count == 0 || ap.vertex_offset == NO_DATA
                        || (size_t)ap.vertex_offset + ap.vertex_count > data.addr_vertices.size()) {
                        // Point address, polygon footprint missing, or
                        // out-of-range index (defends against caller
                        // passing addr_points without matching addr_vertices).
                        ap.vertex_offset = NO_DATA;
                        ap.vertex_count = 0;
                        packed_points.push_back(ap);
                        continue;
                    }
                    uint32_t old_off = ap.vertex_offset;
                    uint32_t vc = ap.vertex_count;
                    // Compute bbox
                    double min_lat = data.addr_vertices[old_off].lat;
                    double max_lat = min_lat;
                    double min_lng = data.addr_vertices[old_off].lng;
                    double max_lng = min_lng;
                    for (uint32_t j = 1; j < vc; j++) {
                        const auto& v = data.addr_vertices[old_off + j];
                        if (v.lat < min_lat) min_lat = v.lat;
                        if (v.lat > max_lat) max_lat = v.lat;
                        if (v.lng < min_lng) min_lng = v.lng;
                        if (v.lng > max_lng) max_lng = v.lng;
                    }
                    double max_span = std::max(max_lat - min_lat, max_lng - min_lng);
                    VertexEncoding enc;
                    double scale;
                    // Building footprints prefer the 0.11 m grid for
                    // sub-meter GPS edge precision; fall back as needed.
                    if (max_span < 65535.0 * 1e-6) {
                        enc = VertexEncoding::U16_011M; scale = 1e-6;
                    } else if (max_span < 65535.0 * 1e-5) {
                        enc = VertexEncoding::U16_1M; scale = 1e-5;
                    } else if (max_span < 65535.0 * 1e-4) {
                        enc = VertexEncoding::U16_11M; scale = 1e-4;
                    } else {
                        enc = VertexEncoding::U32_1CM; scale = 1e-7;
                    }
                    ap.vertex_offset = static_cast<uint32_t>(packed_bytes.size());
                    // 10-byte polygon header
                    packed_bytes.push_back(static_cast<uint8_t>(enc));
                    packed_bytes.push_back(0);
                    float bml = static_cast<float>(min_lat);
                    float bmg = static_cast<float>(min_lng);
                    auto* lp = reinterpret_cast<const uint8_t*>(&bml);
                    packed_bytes.insert(packed_bytes.end(), lp, lp + 4);
                    auto* gp = reinterpret_cast<const uint8_t*>(&bmg);
                    packed_bytes.insert(packed_bytes.end(), gp, gp + 4);
                    // Delta-encoded vertices
                    for (uint32_t j = 0; j < vc; j++) {
                        const auto& v = data.addr_vertices[old_off + j];
                        if (enc == VertexEncoding::U32_1CM) {
                            uint32_t dlat = static_cast<uint32_t>(std::lround((v.lat - min_lat) / scale));
                            uint32_t dlng = static_cast<uint32_t>(std::lround((v.lng - min_lng) / scale));
                            auto* dp = reinterpret_cast<const uint8_t*>(&dlat);
                            packed_bytes.insert(packed_bytes.end(), dp, dp + 4);
                            auto* gp2 = reinterpret_cast<const uint8_t*>(&dlng);
                            packed_bytes.insert(packed_bytes.end(), gp2, gp2 + 4);
                        } else {
                            uint16_t dlat = static_cast<uint16_t>(std::lround((v.lat - min_lat) / scale));
                            uint16_t dlng = static_cast<uint16_t>(std::lround((v.lng - min_lng) / scale));
                            auto* dp = reinterpret_cast<const uint8_t*>(&dlat);
                            packed_bytes.insert(packed_bytes.end(), dp, dp + 2);
                            auto* gp2 = reinterpret_cast<const uint8_t*>(&dlng);
                            packed_bytes.insert(packed_bytes.end(), gp2, gp2 + 2);
                        }
                    }
                    packed_points.push_back(ap);
                }
                {
                    std::ofstream f(output_dir + "/addr_points.bin", std::ios::binary);
                    f.write(reinterpret_cast<const char*>(packed_points.data()),
                            packed_points.size() * sizeof(AddrPoint));
                }
                {
                    std::ofstream f(output_dir + "/addr_vertices.bin", std::ios::binary);
                    f.write(reinterpret_cast<const char*>(packed_bytes.data()),
                            packed_bytes.size());
                }
            }));
            // Per-addr postcode (optional separate file, parallel to addr_points)
            if (!data.addr_postcode_ids.empty()) {
                write_futures.push_back(std::async(std::launch::async, [&] {
                    std::ofstream f(output_dir + "/addr_postcodes.bin", std::ios::binary);
                    f.write(reinterpret_cast<const char*>(data.addr_postcode_ids.data()),
                            data.addr_postcode_ids.size() * sizeof(uint32_t));
                }));
            }
            write_futures.push_back(std::async(std::launch::async, [&] {
                std::ofstream f(output_dir + "/interp_ways.bin", std::ios::binary);
                f.write(reinterpret_cast<const char*>(data.interp_ways.data()), data.interp_ways.size() * sizeof(InterpWay));
            }));
            write_futures.push_back(std::async(std::launch::async, [&] {
                std::ofstream f(output_dir + "/interp_nodes.bin", std::ios::binary);
                f.write(reinterpret_cast<const char*>(data.interp_nodes.data()), data.interp_nodes.size() * sizeof(NodeCoord));
            }));
        }
    }
    // Per-tier strings files. Each mode writes only the tiers its records
    // need to resolve: admin → CORE, no-addresses → CORE+STREET, full →
    // CORE+STREET+ADDR. The postcode (3) and POI (4) tiers are written
    // alongside their respective opt-in files below (postcode_centroids /
    // poi_records).  Admin polygons / vertices live in the quality/
    // directory and don't affect string tiers.
    auto write_tier = [&](size_t tier_idx) {
        const auto& buf = data.strings_tiers[tier_idx];
        write_futures.push_back(std::async(std::launch::async, [&, tier_idx] {
            std::ofstream f(output_dir + "/" + STR_TIER_FILENAMES[tier_idx], std::ios::binary);
            f.write(buf.data(), buf.size());
        }));
    };
    write_tier(0);  // core — always
    if (write_streets) write_tier(1);   // street
    if (write_addresses) write_tier(2); // addr

    // strings_layout.json records each tier's start/end offset in the
    // global string-offset space so the server can translate a record's
    // name_id into the correct tier file. Written unconditionally into
    // every directory that contains a strings_*.bin so the server can
    // load from any mode/opt-in dir without cross-dir discovery.
    write_strings_layout(output_dir, data);

    // Write postcode centroid index (optional files)
    // Validate each centroid's postcode against the country's pattern
    // (matching Nominatim's clean_postcodes sanitizer).
    if (!data.postcode_accum.empty()) {
        write_futures.push_back(std::async(std::launch::async, [&] {
            // Nominatim's _PostcodeCollector accumulates per-country:
            // the same postcode string in different countries produces
            // separate centroids. This prevents a "90012" tagged on a
            // building in Sicily from shifting the US 90012 centroid.
            //
            // Re-accumulate from raw postcode_accum, splitting by the
            // country of each individual addr_point contribution.
            // Since postcode_accum only stores aggregate (sum_lat,
            // sum_lng, count), we need to re-scan addr_points.
            auto get_str = [&](uint32_t off) -> const char* {
                return data.get_string(off);
            };

            // Per-country accumulator: (country_code, postcode_id) → centroid
            struct CountryPcKey {
                uint16_t cc;
                uint32_t pc_id;
                bool operator==(const CountryPcKey& o) const { return cc == o.cc && pc_id == o.pc_id; }
            };
            struct CountryPcHash {
                size_t operator()(const CountryPcKey& k) const {
                    return std::hash<uint64_t>()(((uint64_t)k.cc << 32) | k.pc_id);
                }
            };
            struct PcAccum { double sum_lat = 0, sum_lng = 0; uint64_t count = 0; };
            std::unordered_map<CountryPcKey, PcAccum, CountryPcHash> country_accum;

            // Re-scan addr_points + addr_postcodes to split by country
            for (uint32_t i = 0; i < data.addr_points.size(); i++) {
                if (i >= data.addr_postcode_ids.size()) break;
                uint32_t pc_id = data.addr_postcode_ids[i];
                if (pc_id == NO_DATA) continue;
                const auto& ap = data.addr_points[i];
                // Look up country for this addr_point
                S2CellId cell = S2CellId(S2LatLng::FromDegrees(ap.lat, ap.lng)).parent(kAdminCellLevel);
                uint16_t cc = 0;
                auto it = data.cell_to_admin.find(cell.id());
                if (it != data.cell_to_admin.end()) {
                    for (uint32_t raw_id : it->second) {
                        uint32_t pid = raw_id & 0x7FFFFFFF;
                        if (pid >= data.admin_polygons.size()) continue;
                        const auto& poly = data.admin_polygons[pid];
                        if (poly.admin_level == 2 && poly.country_code != 0) {
                            cc = poly.country_code;
                            break;
                        }
                    }
                }
                if (cc == 0) continue; // skip if no country found
                auto& acc = country_accum[{cc, pc_id}];
                acc.sum_lat += ap.lat;
                acc.sum_lng += ap.lng;
                acc.count++;
            }

            // Also add entries from postcode_accum that aren't already
            // in country_accum (TIGER entries, GeoNames external data).
            // These don't have addr_points so the per-addr re-scan above
            // misses them. Look up country from the centroid location.
            for (const auto& [pc_id, acc] : data.postcode_accum) {
                if (acc.count == 0) continue;
                float clat = static_cast<float>(acc.sum_lat / acc.count);
                float clng = static_cast<float>(acc.sum_lng / acc.count);
                // Look up country for this centroid
                S2CellId pcell = S2CellId(S2LatLng::FromDegrees(clat, clng)).parent(kAdminCellLevel);
                uint16_t cc = 0;
                auto pit = data.cell_to_admin.find(pcell.id());
                if (pit != data.cell_to_admin.end()) {
                    for (uint32_t raw_id : pit->second) {
                        uint32_t pid2 = raw_id & 0x7FFFFFFF;
                        if (pid2 >= data.admin_polygons.size()) continue;
                        const auto& poly = data.admin_polygons[pid2];
                        if (poly.admin_level == 2 && poly.country_code != 0) {
                            cc = poly.country_code;
                            break;
                        }
                    }
                }
                if (cc == 0) continue;
                auto key = CountryPcKey{cc, pc_id};
                if (country_accum.count(key) > 0) continue; // OSM data takes priority
                country_accum[key] = {acc.sum_lat, acc.sum_lng, acc.count};
            }

            // Build centroid vector with validation
            std::vector<PostcodeCentroid> centroids;
            centroids.reserve(country_accum.size());
            uint32_t rejected = 0;
            for (const auto& [key, acc] : country_accum) {
                if (acc.count == 0) continue;
                char cc_str[3] = {
                    static_cast<char>(std::tolower(key.cc >> 8)),
                    static_cast<char>(std::tolower(key.cc & 0xFF)),
                    0
                };
                const char* pc_str = get_str(key.pc_id);
                if (!validate_postcode_for_country(cc_str, pc_str)) {
                    rejected++;
                    continue;
                }
                PostcodeCentroid c{};
                c.lat = static_cast<float>(acc.sum_lat / acc.count);
                c.lng = static_cast<float>(acc.sum_lng / acc.count);
                c.postcode_id = key.pc_id;
                c.country_code = key.cc;
                centroids.push_back(c);
            }
            std::cerr << "Postcode centroids: " << centroids.size() << " valid, "
                      << rejected << " rejected by country pattern"
                      << " (from " << country_accum.size() << " country+postcode pairs)" << std::endl;
            // Determinism: iterate-then-sort over `country_accum` (an
            // unordered_map) produces a run-dependent insertion order.
            // Sorting on postcode_id alone leaves collisions (same pc_id
            // in different countries, e.g. "90012" in US and FR) in an
            // undefined relative order — their lat/lng bits end up
            // run-dependent. Tiebreak by country_code then by lat/lng
            // for a total order.
            std::sort(centroids.begin(), centroids.end(),
                [](const PostcodeCentroid& a, const PostcodeCentroid& b) {
                    if (a.postcode_id != b.postcode_id) return a.postcode_id < b.postcode_id;
                    if (a.country_code != b.country_code) return a.country_code < b.country_code;
                    uint32_t la, lb, ga, gb;
                    std::memcpy(&la, &a.lat, 4); std::memcpy(&lb, &b.lat, 4);
                    if (la != lb) return la < lb;
                    std::memcpy(&ga, &a.lng, 4); std::memcpy(&gb, &b.lng, 4);
                    return ga < gb;
                });

            // Write flat centroid array
            {
                std::ofstream f(output_dir + "/postcode_centroids.bin", std::ios::binary);
                f.write(reinterpret_cast<const char*>(centroids.data()),
                        centroids.size() * sizeof(PostcodeCentroid));
            }

            // Build S2 cell index for spatial lookup
            std::unordered_map<uint64_t, std::vector<uint32_t>> centroid_cells;
            for (uint32_t i = 0; i < centroids.size(); i++) {
                S2CellId cell = S2CellId(S2LatLng::FromDegrees(
                    centroids[i].lat, centroids[i].lng)).parent(kAdminCellLevel);
                centroid_cells[cell.id()].push_back(i);
            }
            write_cell_index(output_dir + "/postcode_centroid_cells.bin",
                             output_dir + "/postcode_centroid_entries.bin",
                             centroid_cells);

            // Write postcode tier strings alongside the postcode files
            // so clients opting in to postcodes also get the names. Note
            // the mode dir already has strings_layout.json from the main
            // per-tier write above, which covers this file too.
            {
                const auto& buf = data.strings_tiers[3];
                std::ofstream f(output_dir + "/" + STR_TIER_FILENAMES[3], std::ios::binary);
                f.write(buf.data(), buf.size());
            }

            std::cerr << "postcode centroids: " << centroids.size() << " entries, "
                      << centroid_cells.size() << " cells" << std::endl;
        }));
    }

    log_phase("  Write: parallel data files launched", _wt);
    admin_future.get();
    for (auto& f : write_futures) f.get();
}

// Pack one polygon's vertices into the byte stream.  Writes a 10-byte
// header (encoding tag + bbox_min lat/lng) followed by packed unsigned
// deltas.  Returns the byte offset to the header (caller stores on the
// record). Inline header keeps PoiRecord/AdminPolygon at their original
// sizes — no per-record bbox_min tax. Shared between write_quality_variant
// and write_admin_minimal_polygons.
static uint32_t pack_polygon_bytes(std::vector<uint8_t>& out_bytes,
                                   const std::vector<std::pair<double,double>>& verts) {
    // Compute bbox min/max
    double min_lat = verts[0].first, max_lat = verts[0].first;
    double min_lng = verts[0].second, max_lng = verts[0].second;
    for (size_t k = 1; k < verts.size(); k++) {
        if (verts[k].first  < min_lat) min_lat = verts[k].first;
        if (verts[k].first  > max_lat) max_lat = verts[k].first;
        if (verts[k].second < min_lng) min_lng = verts[k].second;
        if (verts[k].second > max_lng) max_lng = verts[k].second;
    }
    double dlat_span = max_lat - min_lat;
    double dlng_span = max_lng - min_lng;
    double max_span = std::max(dlat_span, dlng_span);

    // Pick smallest encoding that fits.  u16 max = 65535. Span thresholds
    // use 65535 * scale to leave no headroom.
    VertexEncoding enc;
    double scale;
    if (max_span < 65535.0 * 1e-6) {
        // 11 cm grid, 7.3 km bbox — POI building footprints
        enc = VertexEncoding::U16_011M;
        scale = 1e-6;
    } else if (max_span < 65535.0 * 1e-5) {
        // 1.1 m grid, 73 km bbox — most cities, suburbs, neighbourhoods
        enc = VertexEncoding::U16_1M;
        scale = 1e-5;
    } else if (max_span < 65535.0 * 1e-4) {
        // 11 m grid, 730 km bbox — counties, regions, small countries
        enc = VertexEncoding::U16_11M;
        scale = 1e-4;
    } else {
        // u32 @ 1 cm — fits any polygon (max span ~214 deg)
        enc = VertexEncoding::U32_1CM;
        scale = 1e-7;
    }

    uint32_t byte_offset = static_cast<uint32_t>(out_bytes.size());
    // 10-byte polygon header: encoding (1) + pad (1) + bbox_min_lat (4) + bbox_min_lng (4)
    uint8_t enc_byte = static_cast<uint8_t>(enc);
    out_bytes.push_back(enc_byte);
    out_bytes.push_back(0); // padding
    float bml = static_cast<float>(min_lat);
    float bmg = static_cast<float>(min_lng);
    auto* lp = reinterpret_cast<const uint8_t*>(&bml);
    out_bytes.insert(out_bytes.end(), lp, lp + 4);
    auto* gp_ = reinterpret_cast<const uint8_t*>(&bmg);
    out_bytes.insert(out_bytes.end(), gp_, gp_ + 4);
    if (enc == VertexEncoding::U32_1CM) {
        for (const auto& [lat, lng] : verts) {
            uint32_t dlat = static_cast<uint32_t>(std::lround((lat - min_lat) / scale));
            uint32_t dlng = static_cast<uint32_t>(std::lround((lng - min_lng) / scale));
            out_bytes.insert(out_bytes.end(),
                reinterpret_cast<const uint8_t*>(&dlat),
                reinterpret_cast<const uint8_t*>(&dlat) + 4);
            out_bytes.insert(out_bytes.end(),
                reinterpret_cast<const uint8_t*>(&dlng),
                reinterpret_cast<const uint8_t*>(&dlng) + 4);
        }
    } else {
        for (const auto& [lat, lng] : verts) {
            uint16_t dlat = static_cast<uint16_t>(std::lround((lat - min_lat) / scale));
            uint16_t dlng = static_cast<uint16_t>(std::lround((lng - min_lng) / scale));
            out_bytes.insert(out_bytes.end(),
                reinterpret_cast<const uint8_t*>(&dlat),
                reinterpret_cast<const uint8_t*>(&dlat) + 2);
            out_bytes.insert(out_bytes.end(),
                reinterpret_cast<const uint8_t*>(&dlng),
                reinterpret_cast<const uint8_t*>(&dlng) + 2);
        }
    }
    return byte_offset;
}

// Simplify one polygon's vertices at a given epsilon (or pass through if
// epsilon_scale == 0). Helper used by both quality variant + admin-minimal.
static std::vector<std::pair<double,double>>
simplify_admin_polygon(const ParsedData& data,
                       const AdminPolygon& ap,
                       double epsilon_scale) {
    std::vector<std::pair<double,double>> pts;
    pts.reserve(ap.vertex_count);
    for (uint32_t j = 0; j < ap.vertex_count; j++) {
        const auto& v = data.admin_vertices[ap.vertex_offset + j];
        pts.emplace_back(v.lat, v.lng);
    }
    if (epsilon_scale <= 0) return pts;
    double eps_m = admin_epsilon_meters(ap.admin_level) * epsilon_scale;
    double lat = pts.empty() ? 0.0 : pts[0].first;
    double eps_deg = meters_to_degrees(eps_m, lat);
    return simplify_polygon_epsilon(pts, eps_deg);
}

void write_quality_variant(const ParsedData& data, const std::string& source_dir,
                           const std::string& output_dir, double epsilon_scale) {
    ensure_dir(output_dir);

    // Re-simplify admin polygons at the given epsilon scale
    std::vector<AdminPolygon> new_polys;
    std::vector<NodeCoord> new_verts;
    new_polys.reserve(data.admin_polygons.size());

    // Parallel simplification
    struct SimplifiedPoly {
        std::vector<std::pair<double,double>> verts;
    };
    std::vector<SimplifiedPoly> simplified(data.admin_polygons.size());

    {
        std::atomic<size_t> idx{0};
        unsigned nthreads = std::thread::hardware_concurrency();
        if (nthreads == 0) nthreads = 4;
        std::vector<std::thread> workers;
        for (unsigned t = 0; t < nthreads; t++) {
            workers.emplace_back([&]() {
                while (true) {
                    size_t i = idx.fetch_add(1);
                    if (i >= data.admin_polygons.size()) break;
                    simplified[i].verts = simplify_admin_polygon(data, data.admin_polygons[i], epsilon_scale);
                }
            });
        }
        for (auto& w : workers) w.join();
    }

    // Sequential: build new polygon/vertex arrays. Postal boundaries
    // (admin_level=11) are kept in the main arrays (cell index references
    // them by ID) AND also written to separate optional files.
    std::vector<AdminPolygon> postal_polys;
    std::vector<uint8_t> new_verts_bytes;       // packed: variable stride per polygon
    std::vector<uint8_t> postal_verts_bytes;
    // Don't pre-reserve — overestimates lead to large unused
    // allocations across multiple concurrent continent writers and can
    // OOM the GH runner.  Vector growth is amortized cheap.

    for (size_t i = 0; i < data.admin_polygons.size(); i++) {
        auto& sv = simplified[i].verts;
        if (sv.size() < 3) continue;

        AdminPolygon np = data.admin_polygons[i];

        np.vertex_offset = pack_polygon_bytes(new_verts_bytes, sv);
        np.vertex_count = static_cast<uint32_t>(sv.size());
        np.area = polygon_area(sv);
        new_polys.push_back(np);

        // Postal also go into separate files (for optional loading)
        if (np.admin_level == 11) {
            AdminPolygon pp = np;
            pp.vertex_offset = pack_polygon_bytes(postal_verts_bytes, sv);
            postal_polys.push_back(pp);
        }
    }

    std::cerr << "Quality " << epsilon_scale << "x: " << new_polys.size()
              << " admin polygons, " << new_verts_bytes.size() / 1024 / 1024
              << " MiB packed vertices, " << postal_polys.size() << " postal polygons" << std::endl;

    // Write admin files (excluding postal)
    {
        std::ofstream f(output_dir + "/admin_polygons.bin", std::ios::binary);
        f.write(reinterpret_cast<const char*>(new_polys.data()), new_polys.size() * sizeof(AdminPolygon));
    }
    {
        std::ofstream f(output_dir + "/admin_vertices.bin", std::ios::binary);
        f.write(reinterpret_cast<const char*>(new_verts_bytes.data()), new_verts_bytes.size());
    }

    // Write postal boundary files (optional, admin_level=11 only)
    if (!postal_polys.empty()) {
        {
            std::ofstream f(output_dir + "/postal_polygons.bin", std::ios::binary);
            f.write(reinterpret_cast<const char*>(postal_polys.data()), postal_polys.size() * sizeof(AdminPolygon));
        }
        {
            std::ofstream f(output_dir + "/postal_vertices.bin", std::ios::binary);
            f.write(reinterpret_cast<const char*>(postal_verts_bytes.data()), postal_verts_bytes.size());
        }
    }

    // Quality directories only contain the files that change.
    // Shared files (admin_cells.bin, admin_entries.bin, strings.bin)
    // stay in the parent directory.
}

void write_admin_minimal_polygons(const ParsedData& data,
                                  const std::string& output_dir,
                                  double epsilon_scale,
                                  std::vector<uint32_t>& id_remap) {
    ensure_dir(output_dir);

    // Collect old indices we want to keep (admin_level in [2, 8]).
    id_remap.assign(data.admin_polygons.size(), NO_DATA);
    std::vector<uint32_t> kept_idx;
    kept_idx.reserve(data.admin_polygons.size() / 2);
    for (size_t i = 0; i < data.admin_polygons.size(); i++) {
        uint8_t lvl = data.admin_polygons[i].admin_level;
        if (lvl >= 2 && lvl <= 8) kept_idx.push_back(static_cast<uint32_t>(i));
    }

    // Parallel simplification of just the kept polygons.
    struct SimplifiedPoly { std::vector<std::pair<double,double>> verts; };
    std::vector<SimplifiedPoly> simplified(kept_idx.size());
    {
        std::atomic<size_t> idx{0};
        unsigned nthreads = std::thread::hardware_concurrency();
        if (nthreads == 0) nthreads = 4;
        std::vector<std::thread> workers;
        for (unsigned t = 0; t < nthreads; t++) {
            workers.emplace_back([&]() {
                while (true) {
                    size_t k = idx.fetch_add(1);
                    if (k >= kept_idx.size()) break;
                    simplified[k].verts = simplify_admin_polygon(
                        data, data.admin_polygons[kept_idx[k]], epsilon_scale);
                }
            });
        }
        for (auto& w : workers) w.join();
    }

    // Pack survivors into a fresh dense ID space; write id_remap so the
    // caller can rewrite the admin cell index.
    std::vector<AdminPolygon> new_polys;
    std::vector<uint8_t> new_verts_bytes;
    new_polys.reserve(kept_idx.size());

    for (size_t k = 0; k < kept_idx.size(); k++) {
        auto& sv = simplified[k].verts;
        if (sv.size() < 3) continue;
        AdminPolygon np = data.admin_polygons[kept_idx[k]];
        np.vertex_offset = pack_polygon_bytes(new_verts_bytes, sv);
        np.vertex_count = static_cast<uint32_t>(sv.size());
        np.area = polygon_area(sv);
        id_remap[kept_idx[k]] = static_cast<uint32_t>(new_polys.size());
        new_polys.push_back(np);
    }

    {
        std::ofstream f(output_dir + "/admin_polygons.bin", std::ios::binary);
        f.write(reinterpret_cast<const char*>(new_polys.data()),
                new_polys.size() * sizeof(AdminPolygon));
    }
    {
        std::ofstream f(output_dir + "/admin_vertices.bin", std::ios::binary);
        f.write(reinterpret_cast<const char*>(new_verts_bytes.data()),
                new_verts_bytes.size());
    }

    std::cerr << "  Admin-minimal polygons: " << new_polys.size()
              << " kept (of " << data.admin_polygons.size() << "), "
              << new_verts_bytes.size() / 1024 / 1024 << " MiB packed vertices"
              << std::endl;
}
