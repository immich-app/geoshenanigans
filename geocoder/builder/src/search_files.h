#pragma once

#include "cell_index.h"
#include "record_files_io.h"

namespace search_files {

using Layout = std::array<uint32_t, STR_TIER_COUNT + 1>;
using tag_bundles::LaidOutTier;
using tag_bundles::Use;

inline uint32_t tier_end(uint32_t base, size_t bytes) {
    if (bytes > UINT32_MAX - base) throw std::length_error("search strings exceed the global offset space");
    return base + static_cast<uint32_t>(bytes);
}

inline uint64_t object(const std::vector<uint64_t>& ids, size_t i) { return i < ids.size() ? ids[i] : 0; }

inline std::string_view names(const ParsedData& d, Use use, uint64_t id, uint32_t name) {
    const auto b = d.search_tags->get(use, id);
    if (tag_bundles::name_alone(b)) {
        const char* stored = d.get_string(name);
        if (stored && b.substr(b.find(TAG_KEY_SEP) + 1) == stored) return {};
    }
    return b;
}

template <class Place, class Admin>
LaidOutTier core(const ParsedData& d, size_t places, Place place, size_t admins, Admin admin) {
    return tag_bundles::lay_out(places + admins, 1, [&](size_t row) -> std::string_view {
        if (row < places) {
            const auto i = place(row);
            return names(d, Use::Core, object(d.place_osm_ids, i), d.place_nodes[i].name_id);
        }
        const auto i = admin(row - places);
        return i == NO_DATA ? std::string_view() : names(d, Use::Core, object(d.admin_osm_ids, i), d.admin_polygons[i].name_id);
    }, [](size_t) { return 0; });
}

inline LaidOutTier poi(const ParsedData& d) {
    return tag_bundles::lay_out(d.poi_records.size(), POI_MAX_SHIPPED_TIER + 1,
        [&](size_t i) { return names(d, Use::Poi, object(d.poi_osm_ids, i), d.poi_records[i].name_id); },
        [&](size_t i) { return poi_lowest_tier(d.poi_records[i]); });
}

template <class Index>
void write_rows(const std::string& dir, const std::string& kind, const LaidOutTier& tier,
                size_t count, Index index) {
    std::vector<uint32_t> rows(count);
    for (size_t i = 0; i < count; ++i) {
        rows[i] = tier.offsets[index(i)];
    }
    write_side_file(dir, kind + "_tags.bin", rows, kind, count);
    std::fill(rows.begin(), rows.end(), 0);
    write_side_file(dir, kind + "_imp.bin", rows, kind, count);
}

inline void write_core(const std::string& dir, const LaidOutTier& tier, size_t places) {
    string_files::write_file(dir + "/" + STR_TIER_FILENAMES[STR_TIER_TAGS_CORE], tier.bytes.data(), tier.bytes.size());
    write_rows(dir, "place", tier, places, [](size_t i) { return i; });
    write_rows(dir, "admin", tier, tier.offsets.size() - places, [&](size_t i) { return places + i; });
}

// A block's offsets fit u32 even when the complete planet address blob exceeds 4 GiB.
// Each block holds (rows+1) local offsets, then its UTF-8 payload. The footer
// lists the blocks' u64 starts and the u32 row count.
template <class Value>
void write_blob(const std::string& path, size_t count, Value value) {
    if (count > UINT32_MAX) throw std::length_error(path);
    std::ofstream out(path, std::ios::binary);
    std::vector<uint64_t> blocks;
    std::vector<uint32_t> offsets;
    std::string bytes;
    for (size_t first = 0; first < count; first += TAG_BLOB_BLOCK_ROWS) {
        blocks.push_back(static_cast<uint64_t>(out.tellp()));
        offsets.assign(1, 0);
        bytes.clear();
        for (size_t i = first; i < std::min(count, first + TAG_BLOB_BLOCK_ROWS); ++i) {
            bytes.append(value(i));
            offsets.push_back(tier_end(0, bytes.size()));
        }
        out.write(reinterpret_cast<const char*>(offsets.data()), offsets.size() * sizeof(uint32_t));
        out.write(bytes.data(), bytes.size());
    }
    out.write(reinterpret_cast<const char*>(blocks.data()), blocks.size() * sizeof(uint64_t));
    const auto n = static_cast<uint32_t>(count);
    out.write(reinterpret_cast<const char*>(&n), sizeof(n));
    out.close();
    if (!out) throw std::runtime_error("failed to write " + path);
}

inline void write_host_mode(const ParsedData& d, const std::string& dir, IndexMode mode) {
    if (!d.search_tags->host || mode == IndexMode::AdminOnly) return;
    write_blob(dir + "/street_tags.host", d.ways.size(), [&](size_t i) {
        return d.search_tags->get(Use::Street, i < d.way_osm_ids.size() ? way_boundary_id(d.way_osm_ids[i]) : 0);
    });
    if (mode != IndexMode::Full) return;
    write_blob(dir + "/addr_tags.host", d.addr_points.size(), [&](size_t i) {
        return d.search_tags->get(Use::Address, object(d.addr_osm_ids, i));
    });
    write_blob(dir + "/interp_tags.host", d.interp_ways.size(), [&](size_t i) {
        return d.search_tags->get(Use::Interpolation, object(d.interp_osm_ids, i));
    });
}

inline void write_host_poi(const ParsedData& d, const std::string& dir, const std::vector<uint32_t>& indices) {
    std::vector<uint64_t> ids;
    for (uint32_t i : indices) ids.push_back(object(d.poi_osm_ids, i));
    emit_strategy2_sidecar(dir + "/poi_rows.osm_ids", {}, ids);
    write_blob(dir + "/poi_addr.host", ids.size(), [&](size_t i) { return d.search_tags->get(Use::PoiAddress, ids[i]); });
    std::unordered_map<std::string_view, uint32_t> index;
    std::vector<std::string_view> classes;
    std::vector<uint32_t> rows;
    for (uint64_t id : ids) {
        auto cls = d.search_tags->get(Use::PoiClass, id);
        if (cls.empty()) { rows.push_back(NO_DATA); continue; }
        const auto [it, added] = index.emplace(cls, classes.size());
        if (added) classes.push_back(cls);
        rows.push_back(it->second);
    }
    write_blob(dir + "/poi_class_names.host", classes.size(), [&](size_t i) { return classes[i]; });
    write_side_file(dir, "poi_classes.host", rows, "poi_records.bin", ids.size());
}

}  // namespace search_files
