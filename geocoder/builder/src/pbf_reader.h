#pragma once

// Parallel PBF reader — replaces osmium for PBF file reading.
// Reads all blob offsets upfront, then dispatches decompression + parsing
// to a thread pool with zero sequential bottleneck.

#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

// --- Raw PBF data structures ---

// Tags are stored as index pairs into the block's string table.
// Avoids all string copying during decode.
struct PbfTags {
    const std::vector<std::string>* string_table = nullptr;
    std::vector<std::pair<uint32_t, uint32_t>> indices; // (key_idx, val_idx)

    const char* get(const char* key) const {
        if (!string_table) return nullptr;
        for (const auto& [ki, vi] : indices) {
            if (ki < string_table->size() && (*string_table)[ki] == key) {
                return vi < string_table->size() ? (*string_table)[vi].c_str() : nullptr;
            }
        }
        return nullptr;
    }

    bool empty() const { return indices.empty(); }
};

struct PbfNode {
    int64_t id;
    double lat;
    double lng;
    PbfTags tags;

    const char* tag(const char* key) const { return tags.get(key); }
};

struct PbfWay {
    int64_t id;
    uint32_t refs_offset; // offset into PbfBlock::way_refs
    uint32_t refs_count;  // number of node refs
    PbfTags tags;

    const char* tag(const char* key) const { return tags.get(key); }
};

struct PbfRelation {
    struct Member {
        char type; // 'n', 'w', 'r'
        int64_t ref;
        uint32_t role_sid; // index into block string table
    };
    int64_t id;
    std::vector<Member> members;
    PbfTags tags;

    const char* tag(const char* key) const { return tags.get(key); }
    // Resolve member role from string table
    const std::string& member_role(size_t i) const {
        static const std::string empty;
        if (!tags.string_table || i >= members.size()) return empty;
        uint32_t sid = members[i].role_sid;
        return sid < tags.string_table->size() ? (*tags.string_table)[sid] : empty;
    }
};

// A decoded PBF block containing nodes, ways, and/or relations.
// Owns the string table and flat ref arrays — structs reference via offsets.
struct PbfBlock {
    std::vector<std::string> string_table;
    std::vector<PbfNode> nodes;
    std::vector<PbfWay> ways;
    std::vector<int64_t> way_refs;  // flat array, PbfWay references via offset+count
    std::vector<PbfRelation> relations;

    // Convenience: get node refs for a way
    const int64_t* refs(const PbfWay& w) const { return way_refs.data() + w.refs_offset; }
};

// Blob location within the file
struct BlobInfo {
    size_t offset;       // file offset to start of BlobHeader length prefix
    size_t header_size;  // BlobHeader size
    size_t data_size;    // Blob data size
    std::string type;    // "OSMHeader" or "OSMData"
};

// --- PBF Reader ---

// Scan the file to find all blob offsets
std::vector<BlobInfo> scan_pbf_blobs(const std::string& filename);

// Decode a single blob from raw file data into a PbfBlock.
PbfBlock decode_pbf_blob(const char* blob_data, size_t blob_size);

// Decode into existing block (reuses vector capacity, avoids reallocation).
void decode_pbf_blob_into(const char* blob_data, size_t blob_size, PbfBlock& block);

// Streaming node decode — calls callback directly for each node during decode.
// Avoids creating PbfNode objects entirely. String table is passed for tag lookup.
// Callback: (id, lat, lng, tag_keys_ptr, tag_vals_ptr, ntags, string_table)
using NodeCallback = std::function<void(int64_t id, double lat, double lng,
    const uint32_t* tag_keys, const uint32_t* tag_vals, size_t ntags,
    const std::vector<std::string>& string_table)>;

void decode_nodes_streaming(const char* data, size_t size, const NodeCallback& callback);

// Streaming way decode — calls callback directly for each way during decode.
// Avoids creating PbfWay objects. Node refs passed as pointer+count.
using WayCallback = std::function<void(int64_t id,
    const int64_t* refs, size_t nrefs,
    const uint32_t* tag_keys, const uint32_t* tag_vals, size_t ntags,
    const std::vector<std::string>& string_table)>;

void decode_ways_streaming(const char* data, size_t size, const WayCallback& callback);

// Read and decompress a single blob from file.
// Returns the decompressed PrimitiveBlock data.
std::string read_and_decompress_blob(int fd, const BlobInfo& info);

// High-level parallel reader: scans file, decodes all blocks in parallel,
// calls the callback for each decoded block (in arbitrary order).
// `num_threads` controls parallelism (0 = hardware_concurrency).
// `entity_filter` controls which entity types to parse: 'n' = nodes, 'w' = ways, 'r' = relations
void read_pbf_parallel(const std::string& filename,
                       std::function<void(PbfBlock&, size_t block_index)> callback,
                       unsigned num_threads = 0,
                       const std::string& entity_filter = "nwr");

// Managed PBF file — opens once, provides phased parallel reading.
class PbfFile {
public:
    explicit PbfFile(const std::string& filename, unsigned num_threads = 0);
    ~PbfFile();

    // No-ops (mmap removed — using pread for memory efficiency)
    void release_pages() {}
    void unmap() {}

    // Read all blocks containing the specified entity types in parallel.
    // Callback is called from worker threads with (block, thread_index).
    // The callback must be thread-safe OR use the thread_index for thread-local storage.
    // If ordered=true: decompresses in parallel but calls callback in file order
    // (useful for nodes where sequential ID locality matters for mmap performance).
    // Callback receives a reference — block is reused across iterations.
    // Do NOT move from the block; it will be overwritten on next decode.
    void read_blocks(std::function<void(PbfBlock&, unsigned thread_idx)> callback,
                     const std::string& entity_filter = "nwr",
                     bool ordered = false);

    // Convenience: read only relations, only nodes, only ways
    unsigned thread_count() const { return num_threads_; }

    // Streaming readers — process entities directly during decode.
    void read_nodes_streaming(const NodeCallback& callback);
    void read_ways_streaming(const WayCallback& callback);

    const std::vector<BlobInfo>& blobs() const { return blobs_; }

private:
    std::string filename_;
    unsigned num_threads_;
    std::vector<BlobInfo> blobs_;
    // Pre-classified blob indices by content type
    std::vector<size_t> node_blobs_;
    std::vector<size_t> way_blobs_;
    std::vector<size_t> relation_blobs_;
    bool classified_ = false;
    void classify_blobs();
};
