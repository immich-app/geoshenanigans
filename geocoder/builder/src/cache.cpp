#include "cache.h"

#include <cstring>
#include <fstream>
#include <iostream>

enum class SectionType : uint32_t {
    STRING_POOL = 0, WAYS = 1, STREET_NODES = 2, CELL_TO_WAYS = 3,
    ADDR_POINTS = 4, CELL_TO_ADDRS = 5, INTERP_WAYS = 6, INTERP_NODES = 7,
    CELL_TO_INTERPS = 8, ADMIN_POLYGONS = 9, ADMIN_VERTICES = 10, CELL_TO_ADMIN = 11,
};

static const char CACHE_MAGIC[8] = {'T','G','C','A','C','H','E','\0'};
static const uint32_t CACHE_VERSION = 1;
static const uint32_t CACHE_SECTION_COUNT = 12;

template<typename T>
static std::vector<char> serialize_vector(const std::vector<T>& vec) {
    std::vector<char> buf;
    uint64_t count = vec.size();
    buf.resize(sizeof(uint64_t) + count * sizeof(T));
    std::memcpy(buf.data(), &count, sizeof(uint64_t));
    if (count > 0) std::memcpy(buf.data() + sizeof(uint64_t), vec.data(), count * sizeof(T));
    return buf;
}

template<typename T>
static bool deserialize_vector(const char* data, uint64_t length, std::vector<T>& vec) {
    if (length < sizeof(uint64_t)) return false;
    uint64_t count;
    std::memcpy(&count, data, sizeof(uint64_t));
    if (length < sizeof(uint64_t) + count * sizeof(T)) return false;
    vec.resize(count);
    if (count > 0) std::memcpy(vec.data(), data + sizeof(uint64_t), count * sizeof(T));
    return true;
}

static std::vector<char> serialize_cell_map(const std::unordered_map<uint64_t, std::vector<uint32_t>>& map) {
    size_t total = sizeof(uint64_t);
    for (const auto& [cell_id, ids] : map)
        total += sizeof(uint64_t) + sizeof(uint32_t) + ids.size() * sizeof(uint32_t);
    std::vector<char> buf(total);
    char* ptr = buf.data();
    uint64_t entry_count = map.size();
    std::memcpy(ptr, &entry_count, sizeof(uint64_t)); ptr += sizeof(uint64_t);
    for (const auto& [cell_id, ids] : map) {
        std::memcpy(ptr, &cell_id, sizeof(uint64_t)); ptr += sizeof(uint64_t);
        uint32_t id_count = static_cast<uint32_t>(ids.size());
        std::memcpy(ptr, &id_count, sizeof(uint32_t)); ptr += sizeof(uint32_t);
        if (id_count > 0) { std::memcpy(ptr, ids.data(), id_count * sizeof(uint32_t)); ptr += id_count * sizeof(uint32_t); }
    }
    return buf;
}

static bool deserialize_cell_map(const char* data, uint64_t length,
                                  std::unordered_map<uint64_t, std::vector<uint32_t>>& map) {
    if (length < sizeof(uint64_t)) return false;
    const char* ptr = data;
    const char* end = data + length;
    uint64_t entry_count;
    std::memcpy(&entry_count, ptr, sizeof(uint64_t)); ptr += sizeof(uint64_t);
    map.reserve(entry_count);
    for (uint64_t i = 0; i < entry_count; i++) {
        if (ptr + sizeof(uint64_t) + sizeof(uint32_t) > end) return false;
        uint64_t cell_id;
        std::memcpy(&cell_id, ptr, sizeof(uint64_t)); ptr += sizeof(uint64_t);
        uint32_t id_count;
        std::memcpy(&id_count, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
        if (ptr + id_count * sizeof(uint32_t) > end) return false;
        std::vector<uint32_t> ids(id_count);
        if (id_count > 0) { std::memcpy(ids.data(), ptr, id_count * sizeof(uint32_t)); ptr += id_count * sizeof(uint32_t); }
        map[cell_id] = std::move(ids);
    }
    return true;
}

void serialize_cache(const ParsedData& data, const std::string& path) {
    std::cerr << "Saving cache to " << path << "..." << std::endl;
    std::ofstream f(path, std::ios::binary);
    if (!f.is_open()) { std::cerr << "Error: could not open cache file for writing: " << path << std::endl; return; }
    f.write(CACHE_MAGIC, 8);
    f.write(reinterpret_cast<const char*>(&CACHE_VERSION), sizeof(uint32_t));
    f.write(reinterpret_cast<const char*>(&CACHE_SECTION_COUNT), sizeof(uint32_t));
    auto write_section = [&](SectionType type, const std::vector<char>& blob) {
        uint32_t t = static_cast<uint32_t>(type);
        uint64_t len = blob.size();
        f.write(reinterpret_cast<const char*>(&t), sizeof(uint32_t));
        f.write(reinterpret_cast<const char*>(&len), sizeof(uint64_t));
        f.write(blob.data(), len);
    };
    { const auto& sp = data.string_pool.data();
      std::vector<char> blob(sizeof(uint64_t) + sp.size());
      uint64_t sz = sp.size();
      std::memcpy(blob.data(), &sz, sizeof(uint64_t));
      if (sz > 0) std::memcpy(blob.data() + sizeof(uint64_t), sp.data(), sz);
      write_section(SectionType::STRING_POOL, blob); }
    write_section(SectionType::WAYS, serialize_vector(data.ways));
    write_section(SectionType::STREET_NODES, serialize_vector(data.street_nodes));
    write_section(SectionType::CELL_TO_WAYS, serialize_cell_map(data.cell_to_ways));
    write_section(SectionType::ADDR_POINTS, serialize_vector(data.addr_points));
    write_section(SectionType::CELL_TO_ADDRS, serialize_cell_map(data.cell_to_addrs));
    write_section(SectionType::INTERP_WAYS, serialize_vector(data.interp_ways));
    write_section(SectionType::INTERP_NODES, serialize_vector(data.interp_nodes));
    write_section(SectionType::CELL_TO_INTERPS, serialize_cell_map(data.cell_to_interps));
    write_section(SectionType::ADMIN_POLYGONS, serialize_vector(data.admin_polygons));
    write_section(SectionType::ADMIN_VERTICES, serialize_vector(data.admin_vertices));
    write_section(SectionType::CELL_TO_ADMIN, serialize_cell_map(data.cell_to_admin));
    std::cerr << "Cache saved." << std::endl;
}

bool deserialize_cache(ParsedData& data, const std::string& path) {
    std::cerr << "Loading cache from " << path << "..." << std::endl;
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) { std::cerr << "Error: could not open cache file: " << path << std::endl; return false; }
    char magic[8]; f.read(magic, 8);
    if (std::memcmp(magic, CACHE_MAGIC, 8) != 0) { std::cerr << "Error: invalid cache magic" << std::endl; return false; }
    uint32_t version; f.read(reinterpret_cast<char*>(&version), sizeof(uint32_t));
    if (version != CACHE_VERSION) { std::cerr << "Error: unsupported cache version " << version << std::endl; return false; }
    uint32_t section_count; f.read(reinterpret_cast<char*>(&section_count), sizeof(uint32_t));
    for (uint32_t s = 0; s < section_count; s++) {
        uint32_t type; uint64_t length;
        f.read(reinterpret_cast<char*>(&type), sizeof(uint32_t));
        f.read(reinterpret_cast<char*>(&length), sizeof(uint64_t));
        std::vector<char> blob(length); f.read(blob.data(), length);
        switch (static_cast<SectionType>(type)) {
        case SectionType::STRING_POOL: {
            if (length < sizeof(uint64_t)) return false;
            uint64_t sz; std::memcpy(&sz, blob.data(), sizeof(uint64_t));
            auto& sp = data.string_pool.mutable_data(); sp.resize(sz);
            if (sz > 0) std::memcpy(sp.data(), blob.data() + sizeof(uint64_t), sz);
            break; }
        case SectionType::WAYS: if (!deserialize_vector(blob.data(), length, data.ways)) return false; break;
        case SectionType::STREET_NODES: if (!deserialize_vector(blob.data(), length, data.street_nodes)) return false; break;
        case SectionType::CELL_TO_WAYS: if (!deserialize_cell_map(blob.data(), length, data.cell_to_ways)) return false; break;
        case SectionType::ADDR_POINTS: if (!deserialize_vector(blob.data(), length, data.addr_points)) return false; break;
        case SectionType::CELL_TO_ADDRS: if (!deserialize_cell_map(blob.data(), length, data.cell_to_addrs)) return false; break;
        case SectionType::INTERP_WAYS: if (!deserialize_vector(blob.data(), length, data.interp_ways)) return false; break;
        case SectionType::INTERP_NODES: if (!deserialize_vector(blob.data(), length, data.interp_nodes)) return false; break;
        case SectionType::CELL_TO_INTERPS: if (!deserialize_cell_map(blob.data(), length, data.cell_to_interps)) return false; break;
        case SectionType::ADMIN_POLYGONS: if (!deserialize_vector(blob.data(), length, data.admin_polygons)) return false; break;
        case SectionType::ADMIN_VERTICES: if (!deserialize_vector(blob.data(), length, data.admin_vertices)) return false; break;
        case SectionType::CELL_TO_ADMIN: if (!deserialize_cell_map(blob.data(), length, data.cell_to_admin)) return false; break;
        default: std::cerr << "Warning: unknown cache section type " << type << ", skipping" << std::endl; break;
        }
    }
    std::cerr << "Cache loaded: " << data.ways.size() << " ways, " << data.addr_points.size() << " addrs, "
              << data.interp_ways.size() << " interps, " << data.admin_polygons.size() << " admin polygons" << std::endl;
    return true;
}
