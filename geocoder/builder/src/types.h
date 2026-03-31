#pragma once

#include <cstdint>
#include <vector>

// --- Binary format structs (must match server's index reader) ---

struct WayHeader {
    uint32_t node_offset;
    uint8_t node_count;
    uint32_t name_id;
};

struct AddrPoint {
    float lat;
    float lng;
    uint32_t housenumber_id;
    uint32_t street_id;
};

struct InterpWay {
    uint32_t node_offset;
    uint8_t node_count;
    uint8_t _pad1 = 0, _pad2 = 0, _pad3 = 0; // explicit padding
    uint32_t street_id;
    uint32_t start_number;
    uint32_t end_number;
    uint8_t interpolation;
    uint8_t _pad4 = 0, _pad5 = 0, _pad6 = 0; // explicit padding
};

struct AdminPolygon {
    uint32_t vertex_offset;
    uint32_t vertex_count;
    uint32_t name_id;
    uint8_t admin_level;
    uint8_t _pad1 = 0, _pad2 = 0, _pad3 = 0; // explicit padding (zeroed)
    float area;
    uint16_t country_code;
    uint16_t _pad4 = 0; // explicit padding
};

struct NodeCoord {
    float lat;
    float lng;
};

// Sorted cell-item pair for direct entry writing (avoids hash map)
struct CellItemPair {
    uint64_t cell_id;
    uint32_t item_id;
};

// --- Constants ---

static const uint32_t INTERIOR_FLAG = 0x80000000u;
static const uint32_t ID_MASK       = 0x7FFFFFFFu;
static const uint32_t NO_DATA       = 0xFFFFFFFFu;

// Simplification mode
enum class SimplifyMode { MaxVertices, ErrorBounded };

// Processing limits
static constexpr size_t MAX_BLOCK_QUEUE         = 64;    // producer-consumer queue depth
static constexpr int    MAX_NODE_COUNT          = 255;   // uint8_t node_count
static constexpr int    MAX_VERTEX_COUNT        = 0x7FFFFFFF; // uint32_t vertex_count
static constexpr int    MAX_S2_CELLS_PER_POLY   = 200;   // S2RegionCoverer max_cells
static constexpr int    BACKTRACK_CALL_BUDGET   = 100000; // per-ring backtracking limit
static constexpr int    BACKTRACK_TOTAL_BUDGET  = 500000; // per-relation backtracking limit
static constexpr int    BACKTRACK_MAX_DEPTH     = 200;    // recursion depth limit
static constexpr size_t MAX_SUBWAYS_FOR_RETRY   = 30;     // only retry small relations
static constexpr size_t MAX_NODE_ID_DEFAULT     = 15000000000ULL; // dense index capacity

// --- Index mode ---

enum class IndexMode { Full, NoAddresses, AdminOnly };

// --- Deferred S2 work items ---

struct DeferredWay {
    uint32_t way_id;
    uint32_t node_offset;
    uint8_t node_count;
};

struct DeferredInterp {
    uint32_t interp_id;
    uint32_t node_offset;
    uint8_t node_count;
};

// Collected relation data for parallel admin assembly
struct CollectedRelation {
    int64_t id;
    uint8_t admin_level;
    std::string name;
    std::string country_code;
    bool is_postal;
    std::vector<std::pair<int64_t, std::string>> members; // (way_id, role)
};
