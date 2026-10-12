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

// A decoded PBF block's relations. Owns the string table their tags and
// member roles index into.
struct PbfBlock {
    std::vector<std::string> string_table;
    std::vector<PbfRelation> relations;
};

// Blob location within the file
struct BlobInfo {
    size_t offset;       // file offset to start of BlobHeader length prefix
    size_t header_size;  // BlobHeader size
    size_t data_size;    // Blob data size
    std::string type;    // "OSMHeader" or "OSMData"
};

// --- PBF Reader ---

// Scan the file to find all blob offsets, on `threads` threads (0 = every core)
std::vector<BlobInfo> scan_pbf_blobs(const std::string& filename, unsigned threads = 0);

// Decode a blob's relations into an existing block (reuses vector capacity,
// avoids reallocation).
void decode_pbf_blob_into(const char* blob_data, size_t blob_size, PbfBlock& block);

// Streaming node decode — calls callback directly for each node during decode.
// Avoids creating PbfNode objects entirely. String table is passed for tag lookup.
// Callback: (id, lat, lng, tag_keys_ptr, tag_vals_ptr, ntags, string_table)
// Returns the number of way groups it skipped.
using NodeCallback = std::function<void(int64_t id, double lat, double lng,
    const uint32_t* tag_keys, const uint32_t* tag_vals, size_t ntags,
    const std::vector<std::string>& string_table)>;

size_t decode_nodes_streaming(const char* data, size_t size, const NodeCallback& callback);

// A way's node refs as its blob holds them, delta-coded: most ways are
// skipped on their tags, and decoding refs is most of decoding a way.
class WayRefs {
public:
    bool empty() const {
        for (auto packed : packed_)
            if (!packed.empty()) return false;
        return true;
    }
    // The refs, decoded on the first call into a buffer the decoding thread
    // reuses: valid until the next way.
    const std::vector<int64_t>& get() const;

    // For the decoder: starts the next way, then adds each of its REFS
    // fields, which decode one after the other, each from a ref of 0.
    void reset() {
        packed_.clear();
        decoded_ = false;
    }
    void add_packed(std::string_view packed) { packed_.push_back(packed); }

private:
    std::vector<std::string_view> packed_;
    mutable std::vector<int64_t> refs_;
    mutable bool decoded_ = false;
};

// Streaming way decode — calls callback directly for each way during decode.
// Avoids creating PbfWay objects.
// Returns the number of node groups it skipped.
using WayCallback = std::function<void(int64_t id, const WayRefs& refs,
    const uint32_t* tag_keys, const uint32_t* tag_vals, size_t ntags,
    const std::vector<std::string>& string_table)>;

size_t decode_ways_streaming(const char* data, size_t size, const WayCallback& callback);

// Read and decompress a single blob from file.
// Returns the decompressed PrimitiveBlock data.
std::string read_and_decompress_blob(int fd, const BlobInfo& info);

// Managed PBF file — opens once, provides phased parallel reading.
class PbfFile {
public:
    explicit PbfFile(const std::string& filename, unsigned num_threads = 0);
    ~PbfFile();

    // Read the relation blocks in parallel.
    // Callback is called from worker threads with (block, thread_index).
    // The callback must be thread-safe OR use the thread_index for thread-local storage.
    // Callback receives a reference — block is reused across iterations.
    // Do NOT move from the block; it will be overwritten on next decode.
    void read_blocks(std::function<void(PbfBlock&, unsigned thread_idx)> callback);

    unsigned thread_count() const { return num_threads_; }

    // Streaming readers — process entities directly during decode. Each
    // reads only its own blob class plus the relation blobs, and throws if a
    // blob of its class holds the other kind (a file not sorted by type).
    void read_nodes_streaming(const NodeCallback& callback);
    void read_ways_streaming(const WayCallback& callback);

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
