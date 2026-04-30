#include "pbf_reader.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <condition_variable>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <sys/mman.h>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unistd.h>
#include <zlib.h>

#include <protozero/pbf_reader.hpp>
#include <protozero/varint.hpp>

// --- PBF file format constants ---
// BlobHeader max size: 32 MiB (per spec), but typically <100 bytes
// Blob max size: 32 MiB (per spec)
static constexpr size_t MAX_BLOB_HEADER_SIZE = 64 * 1024;
static constexpr size_t MAX_BLOB_SIZE = 64 * 1024 * 1024;

// Protobuf field tags (from OSM PBF spec)
namespace BlobHeaderTag {
    constexpr int TYPE = 1;      // string
    constexpr int DATASIZE = 3;  // int32
}
namespace BlobTag {
    constexpr int RAW = 1;       // bytes
    constexpr int RAW_SIZE = 2;  // int32
    constexpr int ZLIB = 3;      // bytes
}
namespace PrimitiveBlockTag {
    constexpr int STRINGTABLE = 1;   // StringTable
    constexpr int PRIMITIVEGROUP = 2; // repeated PrimitiveGroup
    constexpr int GRANULARITY = 17;  // int32 (default 100)
    constexpr int LAT_OFFSET = 19;   // int64 (default 0)
    constexpr int LON_OFFSET = 20;   // int64 (default 0)
}
namespace PrimitiveGroupTag {
    constexpr int NODES = 1;
    constexpr int DENSE = 2;
    constexpr int WAYS = 3;
    constexpr int RELATIONS = 4;
}
namespace StringTableTag {
    constexpr int S = 1; // repeated bytes
}
namespace DenseNodesTag {
    constexpr int ID = 1;
    constexpr int LAT = 8;
    constexpr int LON = 9;
    constexpr int KEYS_VALS = 10;
}
namespace WayTag {
    constexpr int ID = 1;
    constexpr int KEYS = 2;
    constexpr int VALS = 3;
    constexpr int REFS = 8;
}
namespace RelationTag {
    constexpr int ID = 1;
    constexpr int KEYS = 2;
    constexpr int VALS = 3;
    constexpr int ROLES_SID = 8;
    constexpr int MEMIDS = 9;
    constexpr int TYPES = 10;
}

// --- File I/O helpers ---

static std::string read_bytes(int fd, size_t offset, size_t count) {
    std::string buf(count, '\0');
    ssize_t n = pread(fd, buf.data(), count, offset);
    if (n != (ssize_t)count) {
        throw std::runtime_error("pread failed at offset " + std::to_string(offset) +
                                 ": expected " + std::to_string(count) +
                                 " got " + std::to_string(n));
    }
    return buf;
}

static uint32_t read_u32_be(int fd, size_t offset) {
    uint8_t buf[4];
    if (pread(fd, buf, 4, offset) != 4) {
        throw std::runtime_error("failed to read 4 bytes at offset " + std::to_string(offset));
    }
    return (uint32_t(buf[0]) << 24) | (uint32_t(buf[1]) << 16) |
           (uint32_t(buf[2]) << 8) | uint32_t(buf[3]);
}

// --- Blob scanning ---

std::vector<BlobInfo> scan_pbf_blobs(const std::string& filename) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("cannot open " + filename + ": " + strerror(errno));

    // Get file size
    off_t file_size = lseek(fd, 0, SEEK_END);

    std::vector<BlobInfo> blobs;
    size_t offset = 0;

    while (offset < (size_t)file_size) {
        // Read 4-byte big-endian BlobHeader size
        uint32_t header_size = read_u32_be(fd, offset);
        if (header_size > MAX_BLOB_HEADER_SIZE) {
            throw std::runtime_error("BlobHeader too large: " + std::to_string(header_size));
        }

        // Read BlobHeader
        std::string header_data = read_bytes(fd, offset + 4, header_size);
        protozero::pbf_reader header_pbf(header_data);

        std::string type;
        int32_t data_size = 0;
        while (header_pbf.next()) {
            switch (header_pbf.tag()) {
                case BlobHeaderTag::TYPE:
                    type = header_pbf.get_string();
                    break;
                case BlobHeaderTag::DATASIZE:
                    data_size = header_pbf.get_int32();
                    break;
                default:
                    header_pbf.skip();
            }
        }

        BlobInfo info;
        info.offset = offset;
        info.header_size = header_size;
        info.data_size = data_size;
        info.type = type;
        blobs.push_back(info);

        // Advance past: 4 bytes length + header + blob data
        offset += 4 + header_size + data_size;
    }

    close(fd);
    return blobs;
}

// --- Blob decompression ---

// Reusable-buffer version: avoids allocation after warmup
static void read_and_decompress_blob_into(int fd, const BlobInfo& info,
                                            std::string& blob_buf, std::string& out) {
    size_t blob_offset = info.offset + 4 + info.header_size;
    blob_buf.resize(info.data_size);
    ssize_t n = pread(fd, blob_buf.data(), info.data_size, blob_offset);
    if (n != (ssize_t)info.data_size) {
        throw std::runtime_error("pread failed");
    }

    protozero::pbf_reader blob_pbf(blob_buf);
    protozero::data_view raw_view{}, zlib_view{};
    int32_t raw_size = 0;
    while (blob_pbf.next()) {
        switch (blob_pbf.tag()) {
            case BlobTag::RAW: raw_view = blob_pbf.get_view(); break;
            case BlobTag::RAW_SIZE: raw_size = blob_pbf.get_int32(); break;
            case BlobTag::ZLIB: zlib_view = blob_pbf.get_view(); break;
            default: blob_pbf.skip();
        }
    }
    if (raw_view.size() > 0) {
        out.assign(raw_view.data(), raw_view.size());
        return;
    }
    if (zlib_view.size() > 0) {
        out.resize(raw_size);
        thread_local z_stream strm{};
        thread_local bool strm_init = false;
        if (!strm_init) { inflateInit(&strm); strm_init = true; }
        else inflateReset(&strm);
        strm.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(zlib_view.data()));
        strm.avail_in = zlib_view.size();
        strm.next_out = reinterpret_cast<Bytef*>(out.data());
        strm.avail_out = out.size();
        inflate(&strm, Z_FINISH);
        out.resize(strm.total_out);
        return;
    }
    throw std::runtime_error("blob has no data");
}

std::string read_and_decompress_blob(int fd, const BlobInfo& info) {
    // Read the blob data (after 4-byte header length + header)
    size_t blob_offset = info.offset + 4 + info.header_size;
    std::string blob_data = read_bytes(fd, blob_offset, info.data_size);

    protozero::pbf_reader blob_pbf(blob_data);
    std::string raw_data;
    std::string zlib_data;
    int32_t raw_size = 0;

    while (blob_pbf.next()) {
        switch (blob_pbf.tag()) {
            case BlobTag::RAW: {
                auto view = blob_pbf.get_view();
                raw_data.assign(view.data(), view.size());
                break;
            }
            case BlobTag::RAW_SIZE:
                raw_size = blob_pbf.get_int32();
                break;
            case BlobTag::ZLIB: {
                auto view = blob_pbf.get_view();
                zlib_data.assign(view.data(), view.size());
                break;
            }
            default:
                blob_pbf.skip();
        }
    }

    if (!raw_data.empty()) {
        return raw_data;
    }

    if (!zlib_data.empty()) {
        std::string decompressed(raw_size, '\0');
        z_stream strm{};
        strm.next_in = reinterpret_cast<Bytef*>(zlib_data.data());
        strm.avail_in = zlib_data.size();
        strm.next_out = reinterpret_cast<Bytef*>(decompressed.data());
        strm.avail_out = decompressed.size();

        if (inflateInit(&strm) != Z_OK) {
            throw std::runtime_error("inflateInit failed");
        }
        int ret = inflate(&strm, Z_FINISH);
        inflateEnd(&strm);
        if (ret != Z_STREAM_END) {
            throw std::runtime_error("zlib inflate failed: " + std::to_string(ret));
        }
        decompressed.resize(strm.total_out);
        return decompressed;
    }

    throw std::runtime_error("blob has neither raw nor zlib data");
}

// Decompress from mmap'd file into reusable buffer (no allocation after warmup)
static void decompress_blob_from_mmap(const char* file_data, const BlobInfo& info,
                                       std::string& out) {
    const char* blob_ptr = file_data + info.offset + 4 + info.header_size;
    size_t blob_size = info.data_size;

    protozero::pbf_reader blob_pbf(blob_ptr, blob_size);
    protozero::data_view raw_view{}, zlib_view{};
    int32_t raw_size = 0;

    while (blob_pbf.next()) {
        switch (blob_pbf.tag()) {
            case BlobTag::RAW: raw_view = blob_pbf.get_view(); break;
            case BlobTag::RAW_SIZE: raw_size = blob_pbf.get_int32(); break;
            case BlobTag::ZLIB: zlib_view = blob_pbf.get_view(); break;
            default: blob_pbf.skip();
        }
    }

    if (raw_view.size() > 0) {
        out.assign(raw_view.data(), raw_view.size());
        return;
    }

    if (zlib_view.size() > 0) {
        out.resize(raw_size);
        // Reuse thread-local z_stream to avoid inflateInit/End overhead per block
        thread_local z_stream strm{};
        thread_local bool strm_init = false;
        if (!strm_init) {
            if (inflateInit(&strm) != Z_OK) throw std::runtime_error("inflateInit failed");
            strm_init = true;
        } else {
            inflateReset(&strm);
        }
        strm.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(zlib_view.data()));
        strm.avail_in = zlib_view.size();
        strm.next_out = reinterpret_cast<Bytef*>(out.data());
        strm.avail_out = out.size();
        int ret = inflate(&strm, Z_FINISH);
        if (ret != Z_STREAM_END) throw std::runtime_error("zlib inflate failed");
        out.resize(strm.total_out);
        return;
    }

    throw std::runtime_error("blob has no data");
}

// --- PrimitiveBlock decoding ---

PbfBlock decode_pbf_blob(const char* data, size_t size) {
    PbfBlock block;

    protozero::pbf_reader pb(data, size);
    auto& string_table = block.string_table;
    int32_t granularity = 100;
    int64_t lat_offset = 0;
    int64_t lon_offset = 0;

    // First pass: extract string table and granularity
    while (pb.next()) {
        switch (pb.tag()) {
            case PrimitiveBlockTag::STRINGTABLE: {
                protozero::pbf_reader st = pb.get_message();
                while (st.next()) {
                    if (st.tag() == StringTableTag::S) {
                        auto view = st.get_view();
                        string_table.emplace_back(view.data(), view.size());
                    } else {
                        st.skip();
                    }
                }
                break;
            }
            case PrimitiveBlockTag::GRANULARITY:
                granularity = pb.get_int32();
                break;
            case PrimitiveBlockTag::LAT_OFFSET:
                lat_offset = pb.get_int64();
                break;
            case PrimitiveBlockTag::LON_OFFSET:
                lon_offset = pb.get_int64();
                break;
            default:
                pb.skip();
        }
    }

    auto* st_ptr = &block.string_table; // for tag construction

    // Second pass: decode primitive groups
    protozero::pbf_reader pb2(data, size);
    while (pb2.next()) {
        if (pb2.tag() != PrimitiveBlockTag::PRIMITIVEGROUP) {
            pb2.skip();
            continue;
        }

        protozero::pbf_reader group = pb2.get_message();
        while (group.next()) {
            switch (group.tag()) {
                case PrimitiveGroupTag::DENSE: {
                    protozero::pbf_reader dense = group.get_message();
                    std::vector<int64_t> ids_vec, lats_vec, lons_vec;
                    std::vector<int32_t> kv_vec;

                    while (dense.next()) {
                        switch (dense.tag()) {
                            case DenseNodesTag::ID:
                                for (auto v : dense.get_packed_sint64()) ids_vec.push_back(v);
                                break;
                            case DenseNodesTag::LAT:
                                for (auto v : dense.get_packed_sint64()) lats_vec.push_back(v);
                                break;
                            case DenseNodesTag::LON:
                                for (auto v : dense.get_packed_sint64()) lons_vec.push_back(v);
                                break;
                            case DenseNodesTag::KEYS_VALS:
                                for (auto v : dense.get_packed_int32()) kv_vec.push_back(v);
                                break;
                            default:
                                dense.skip();
                        }
                    }

                    int64_t id = 0, lat = 0, lon = 0;
                    size_t kv_pos = 0;

                    for (size_t i = 0; i < ids_vec.size(); i++) {
                        id += ids_vec[i];
                        lat += (i < lats_vec.size()) ? lats_vec[i] : 0;
                        lon += (i < lons_vec.size()) ? lons_vec[i] : 0;

                        PbfNode node;
                        node.id = id;
                        node.lat = 0.000000001 * (lat_offset + (int64_t)granularity * lat);
                        node.lng = 0.000000001 * (lon_offset + (int64_t)granularity * lon);

                        // Only allocate tags if this node has any
                        if (kv_pos < kv_vec.size() && kv_vec[kv_pos] != 0) {
                            node.tags.string_table = st_ptr;
                            while (kv_pos < kv_vec.size()) {
                                int32_t key_idx = kv_vec[kv_pos++];
                                if (key_idx == 0) break;
                                int32_t val_idx = (kv_pos < kv_vec.size()) ? kv_vec[kv_pos++] : 0;
                                node.tags.indices.emplace_back(key_idx, val_idx);
                            }
                        } else {
                            if (kv_pos < kv_vec.size()) kv_pos++; // skip the 0 delimiter
                        }

                        block.nodes.push_back(std::move(node));
                    }
                    break;
                }

                case PrimitiveGroupTag::WAYS: {
                    protozero::pbf_reader way_msg = group.get_message();
                    PbfWay way;
                    std::vector<uint32_t> keys, vals;

                    while (way_msg.next()) {
                        switch (way_msg.tag()) {
                            case WayTag::ID:
                                way.id = way_msg.get_int64();
                                break;
                            case WayTag::KEYS: {
                                auto packed = way_msg.get_packed_uint32();
                                for (auto v : packed) keys.push_back(v);
                                break;
                            }
                            case WayTag::VALS: {
                                auto packed = way_msg.get_packed_uint32();
                                for (auto v : packed) vals.push_back(v);
                                break;
                            }
                            case WayTag::REFS: {
                                auto packed = way_msg.get_packed_sint64();
                                way.refs_offset = static_cast<uint32_t>(block.way_refs.size());
                                int64_t ref = 0;
                                for (auto delta : packed) {
                                    ref += delta;
                                    block.way_refs.push_back(ref);
                                }
                                way.refs_count = static_cast<uint32_t>(block.way_refs.size()) - way.refs_offset;
                                break;
                            }
                            default:
                                way_msg.skip();
                        }
                    }

                    way.tags.string_table = st_ptr;
                    for (size_t i = 0; i < keys.size() && i < vals.size(); i++) {
                        way.tags.indices.emplace_back(keys[i], vals[i]);
                    }
                    block.ways.push_back(std::move(way));
                    break;
                }

                case PrimitiveGroupTag::RELATIONS: {
                    protozero::pbf_reader rel_msg = group.get_message();
                    PbfRelation rel;
                    std::vector<uint32_t> keys, vals;
                    std::vector<int32_t> roles_sid;
                    std::vector<int64_t> memids_delta;
                    std::vector<int32_t> types;

                    while (rel_msg.next()) {
                        switch (rel_msg.tag()) {
                            case RelationTag::ID:
                                rel.id = rel_msg.get_int64();
                                break;
                            case RelationTag::KEYS: {
                                auto packed = rel_msg.get_packed_uint32();
                                for (auto v : packed) keys.push_back(v);
                                break;
                            }
                            case RelationTag::VALS: {
                                auto packed = rel_msg.get_packed_uint32();
                                for (auto v : packed) vals.push_back(v);
                                break;
                            }
                            case RelationTag::ROLES_SID: {
                                auto packed = rel_msg.get_packed_int32();
                                for (auto v : packed) roles_sid.push_back(v);
                                break;
                            }
                            case RelationTag::MEMIDS: {
                                auto packed = rel_msg.get_packed_sint64();
                                for (auto v : packed) memids_delta.push_back(v);
                                break;
                            }
                            case RelationTag::TYPES: {
                                auto packed = rel_msg.get_packed_int32();
                                for (auto v : packed) types.push_back(v);
                                break;
                            }
                            default:
                                rel_msg.skip();
                        }
                    }

                    rel.tags.string_table = st_ptr;
                    for (size_t i = 0; i < keys.size() && i < vals.size(); i++) {
                        rel.tags.indices.emplace_back(keys[i], vals[i]);
                    }

                    int64_t memid = 0;
                    for (size_t i = 0; i < memids_delta.size(); i++) {
                        memid += memids_delta[i];
                        char type = '?';
                        if (i < types.size()) {
                            switch (types[i]) {
                                case 0: type = 'n'; break;
                                case 1: type = 'w'; break;
                                case 2: type = 'r'; break;
                            }
                        }
                        uint32_t role_sid = (i < roles_sid.size()) ? roles_sid[i] : 0;
                        rel.members.push_back({type, memid, role_sid});
                    }
                    block.relations.push_back(std::move(rel));
                    break;
                }

                default:
                    group.skip();
            }
        }
    }

    return block;
}

void decode_nodes_streaming(const char* data, size_t size, const NodeCallback& callback) {
    // Single-pass decode: string table + dense nodes in one traversal
    std::vector<std::string> string_table;
    int32_t granularity = 100;
    int64_t lat_offset = 0, lon_offset = 0;

    // We need the string table before processing tags, but in PBF the string table
    // comes before primitive groups, so a single pass works.
    protozero::pbf_reader pb(data, size);
    while (pb.next()) {
        switch (pb.tag()) {
            case PrimitiveBlockTag::STRINGTABLE: {
                protozero::pbf_reader st = pb.get_message();
                while (st.next()) {
                    if (st.tag() == StringTableTag::S) {
                        auto view = st.get_view();
                        string_table.emplace_back(view.data(), view.size());
                    } else { st.skip(); }
                }
                break;
            }
            case PrimitiveBlockTag::GRANULARITY: granularity = pb.get_int32(); break;
            case PrimitiveBlockTag::LAT_OFFSET: lat_offset = pb.get_int64(); break;
            case PrimitiveBlockTag::LON_OFFSET: lon_offset = pb.get_int64(); break;
            case PrimitiveBlockTag::PRIMITIVEGROUP: {
                protozero::pbf_reader group = pb.get_message();
                while (group.next()) {
                    if (group.tag() == PrimitiveGroupTag::DENSE) {
                        protozero::pbf_reader dense = group.get_message();
                        protozero::data_view ids_data{}, lats_data{}, lons_data{}, kv_data{};
                        while (dense.next()) {
                            switch (dense.tag()) {
                                case DenseNodesTag::ID: ids_data = dense.get_view(); break;
                                case DenseNodesTag::LAT: lats_data = dense.get_view(); break;
                                case DenseNodesTag::LON: lons_data = dense.get_view(); break;
                                case DenseNodesTag::KEYS_VALS: kv_data = dense.get_view(); break;
                                default: dense.skip();
                            }
                        }

                        const char* id_ptr = ids_data.data();
                        const char* id_end = id_ptr + ids_data.size();
                        const char* lat_ptr = lats_data.data();
                        const char* lat_end = lat_ptr + lats_data.size();
                        const char* lon_ptr = lons_data.data();
                        const char* lon_end = lon_ptr + lons_data.size();
                        const char* kv_ptr = kv_data.data();
                        const char* kv_end = kv_ptr + kv_data.size();

                        // Reusable tag buffers (avoid allocation per node)
                        thread_local std::vector<uint32_t> tag_keys, tag_vals;

                        int64_t id = 0, lat = 0, lon = 0;
                        while (id_ptr < id_end) {
                            id += protozero::decode_zigzag64(protozero::decode_varint(&id_ptr, id_end));
                            lat += protozero::decode_zigzag64(protozero::decode_varint(&lat_ptr, lat_end));
                            lon += protozero::decode_zigzag64(protozero::decode_varint(&lon_ptr, lon_end));

                            tag_keys.clear();
                            tag_vals.clear();

                            if (kv_ptr < kv_end) {
                                uint32_t key_idx = static_cast<uint32_t>(protozero::decode_varint(&kv_ptr, kv_end));
                                if (key_idx != 0) {
                                    do {
                                        uint32_t val_idx = (kv_ptr < kv_end)
                                            ? static_cast<uint32_t>(protozero::decode_varint(&kv_ptr, kv_end)) : 0;
                                        tag_keys.push_back(key_idx);
                                        tag_vals.push_back(val_idx);
                                        if (kv_ptr >= kv_end) break;
                                        key_idx = static_cast<uint32_t>(protozero::decode_varint(&kv_ptr, kv_end));
                                    } while (key_idx != 0);
                                }
                            }

                            double dlat = 0.000000001 * (lat_offset + (int64_t)granularity * lat);
                            double dlng = 0.000000001 * (lon_offset + (int64_t)granularity * lon);
                            callback(id, dlat, dlng,
                                     tag_keys.data(), tag_vals.data(), tag_keys.size(),
                                     string_table);
                        }
                    } else {
                        group.skip();
                    }
                }
                break;
            }
            default: pb.skip();
        }
    }
}

void decode_ways_streaming(const char* data, size_t size, const WayCallback& callback) {
    std::vector<std::string> string_table;
    int32_t granularity = 100;
    int64_t lat_offset = 0, lon_offset = 0;

    // Reusable per-way buffers
    thread_local std::vector<int64_t> refs;
    thread_local std::vector<uint32_t> keys, vals;

    protozero::pbf_reader pb(data, size);
    while (pb.next()) {
        switch (pb.tag()) {
            case PrimitiveBlockTag::STRINGTABLE: {
                protozero::pbf_reader st = pb.get_message();
                while (st.next()) {
                    if (st.tag() == StringTableTag::S) {
                        auto view = st.get_view();
                        string_table.emplace_back(view.data(), view.size());
                    } else { st.skip(); }
                }
                break;
            }
            case PrimitiveBlockTag::GRANULARITY: granularity = pb.get_int32(); break;
            case PrimitiveBlockTag::LAT_OFFSET: lat_offset = pb.get_int64(); break;
            case PrimitiveBlockTag::LON_OFFSET: lon_offset = pb.get_int64(); break;
            case PrimitiveBlockTag::PRIMITIVEGROUP: {
                protozero::pbf_reader group = pb.get_message();
                while (group.next()) {
                    if (group.tag() == PrimitiveGroupTag::WAYS) {
                        protozero::pbf_reader way_msg = group.get_message();
                        int64_t way_id = 0;
                        refs.clear();
                        keys.clear();
                        vals.clear();

                        while (way_msg.next()) {
                            switch (way_msg.tag()) {
                                case WayTag::ID: way_id = way_msg.get_int64(); break;
                                case WayTag::KEYS:
                                    for (auto v : way_msg.get_packed_uint32()) keys.push_back(v);
                                    break;
                                case WayTag::VALS:
                                    for (auto v : way_msg.get_packed_uint32()) vals.push_back(v);
                                    break;
                                case WayTag::REFS: {
                                    int64_t ref = 0;
                                    for (auto delta : way_msg.get_packed_sint64()) {
                                        ref += delta;
                                        refs.push_back(ref);
                                    }
                                    break;
                                }
                                default: way_msg.skip();
                            }
                        }

                        size_t ntags = std::min(keys.size(), vals.size());
                        callback(way_id, refs.data(), refs.size(),
                                 keys.data(), vals.data(), ntags, string_table);
                    } else {
                        group.skip();
                    }
                }
                break;
            }
            default: pb.skip();
        }
    }
}

void decode_pbf_blob_into(const char* data, size_t size, PbfBlock& block) {
    // Clear vectors but keep capacity for reuse
    block.string_table.clear();
    block.nodes.clear();
    block.ways.clear();
    block.way_refs.clear();
    block.relations.clear();

    protozero::pbf_reader pb(data, size);
    auto& string_table = block.string_table;
    int32_t granularity = 100;
    int64_t lat_offset = 0;
    int64_t lon_offset = 0;

    while (pb.next()) {
        switch (pb.tag()) {
            case PrimitiveBlockTag::STRINGTABLE: {
                protozero::pbf_reader st = pb.get_message();
                while (st.next()) {
                    if (st.tag() == StringTableTag::S) {
                        auto view = st.get_view();
                        string_table.emplace_back(view.data(), view.size());
                    } else { st.skip(); }
                }
                break;
            }
            case PrimitiveBlockTag::GRANULARITY: granularity = pb.get_int32(); break;
            case PrimitiveBlockTag::LAT_OFFSET: lat_offset = pb.get_int64(); break;
            case PrimitiveBlockTag::LON_OFFSET: lon_offset = pb.get_int64(); break;
            default: pb.skip();
        }
    }

    auto* st_ptr = &block.string_table;

    protozero::pbf_reader pb2(data, size);
    while (pb2.next()) {
        if (pb2.tag() != PrimitiveBlockTag::PRIMITIVEGROUP) { pb2.skip(); continue; }
        protozero::pbf_reader group = pb2.get_message();
        while (group.next()) {
            switch (group.tag()) {
                case PrimitiveGroupTag::DENSE: {
                    protozero::pbf_reader dense = group.get_message();
                    // Save raw data views — iterate in lockstep without intermediate vectors
                    protozero::data_view ids_data, lats_data, lons_data, kv_data;
                    while (dense.next()) {
                        switch (dense.tag()) {
                            case DenseNodesTag::ID: ids_data = dense.get_view(); break;
                            case DenseNodesTag::LAT: lats_data = dense.get_view(); break;
                            case DenseNodesTag::LON: lons_data = dense.get_view(); break;
                            case DenseNodesTag::KEYS_VALS: kv_data = dense.get_view(); break;
                            default: dense.skip();
                        }
                    }

                    // Raw varint pointers for zero-copy iteration
                    const char* id_ptr = ids_data.data();
                    const char* id_end = id_ptr + ids_data.size();
                    const char* lat_ptr = lats_data.data();
                    const char* lat_end = lat_ptr + lats_data.size();
                    const char* lon_ptr = lons_data.data();
                    const char* lon_end = lon_ptr + lons_data.size();
                    const char* kv_ptr = kv_data.data();
                    const char* kv_end = kv_ptr + kv_data.size();

                    int64_t id = 0, lat = 0, lon = 0;

                    while (id_ptr < id_end) {
                        id += protozero::decode_zigzag64(protozero::decode_varint(&id_ptr, id_end));
                        lat += protozero::decode_zigzag64(protozero::decode_varint(&lat_ptr, lat_end));
                        lon += protozero::decode_zigzag64(protozero::decode_varint(&lon_ptr, lon_end));

                        PbfNode node;
                        node.id = id;
                        node.lat = 0.000000001 * (lat_offset + (int64_t)granularity * lat);
                        node.lng = 0.000000001 * (lon_offset + (int64_t)granularity * lon);

                        // Parse tags (kv pairs terminated by 0)
                        if (kv_ptr < kv_end) {
                            uint32_t key_idx = static_cast<uint32_t>(protozero::decode_varint(&kv_ptr, kv_end));
                            if (key_idx != 0) {
                                node.tags.string_table = st_ptr;
                                do {
                                    uint32_t val_idx = (kv_ptr < kv_end)
                                        ? static_cast<uint32_t>(protozero::decode_varint(&kv_ptr, kv_end)) : 0;
                                    node.tags.indices.emplace_back(key_idx, val_idx);
                                    if (kv_ptr >= kv_end) break;
                                    key_idx = static_cast<uint32_t>(protozero::decode_varint(&kv_ptr, kv_end));
                                } while (key_idx != 0);
                            }
                        }

                        block.nodes.push_back(std::move(node));
                    }
                    break;
                }
                case PrimitiveGroupTag::WAYS: {
                    protozero::pbf_reader way_msg = group.get_message();
                    PbfWay way;
                    std::vector<uint32_t> keys, vals;
                    while (way_msg.next()) {
                        switch (way_msg.tag()) {
                            case WayTag::ID: way.id = way_msg.get_int64(); break;
                            case WayTag::KEYS: for (auto v : way_msg.get_packed_uint32()) keys.push_back(v); break;
                            case WayTag::VALS: for (auto v : way_msg.get_packed_uint32()) vals.push_back(v); break;
                            case WayTag::REFS: {
                                auto packed = way_msg.get_packed_sint64();
                                way.refs_offset = static_cast<uint32_t>(block.way_refs.size());
                                int64_t ref = 0;
                                for (auto delta : packed) { ref += delta; block.way_refs.push_back(ref); }
                                way.refs_count = static_cast<uint32_t>(block.way_refs.size()) - way.refs_offset;
                                break;
                            }
                            default: way_msg.skip();
                        }
                    }
                    way.tags.string_table = st_ptr;
                    for (size_t i = 0; i < keys.size() && i < vals.size(); i++)
                        way.tags.indices.emplace_back(keys[i], vals[i]);
                    block.ways.push_back(std::move(way));
                    break;
                }
                case PrimitiveGroupTag::RELATIONS: {
                    protozero::pbf_reader rel_msg = group.get_message();
                    PbfRelation rel;
                    std::vector<uint32_t> keys, vals;
                    std::vector<int32_t> roles_sid;
                    std::vector<int64_t> memids_delta;
                    std::vector<int32_t> types;
                    while (rel_msg.next()) {
                        switch (rel_msg.tag()) {
                            case RelationTag::ID: rel.id = rel_msg.get_int64(); break;
                            case RelationTag::KEYS: for (auto v : rel_msg.get_packed_uint32()) keys.push_back(v); break;
                            case RelationTag::VALS: for (auto v : rel_msg.get_packed_uint32()) vals.push_back(v); break;
                            case RelationTag::ROLES_SID: for (auto v : rel_msg.get_packed_int32()) roles_sid.push_back(v); break;
                            case RelationTag::MEMIDS: for (auto v : rel_msg.get_packed_sint64()) memids_delta.push_back(v); break;
                            case RelationTag::TYPES: for (auto v : rel_msg.get_packed_int32()) types.push_back(v); break;
                            default: rel_msg.skip();
                        }
                    }
                    rel.tags.string_table = st_ptr;
                    for (size_t i = 0; i < keys.size() && i < vals.size(); i++)
                        rel.tags.indices.emplace_back(keys[i], vals[i]);
                    int64_t memid = 0;
                    for (size_t i = 0; i < memids_delta.size(); i++) {
                        memid += memids_delta[i];
                        char type = '?';
                        if (i < types.size()) { switch(types[i]) { case 0: type='n'; break; case 1: type='w'; break; case 2: type='r'; break; } }
                        uint32_t role_sid = (i < roles_sid.size()) ? roles_sid[i] : 0;
                        rel.members.push_back({type, memid, role_sid});
                    }
                    block.relations.push_back(std::move(rel));
                    break;
                }
                default: group.skip();
            }
        }
    }
}

// --- Parallel reader ---

void read_pbf_parallel(const std::string& filename,
                       std::function<void(PbfBlock&, size_t block_index)> callback,
                       unsigned num_threads,
                       const std::string& entity_filter) {
    if (num_threads == 0) num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 4;

    // Scan blob offsets
    auto blobs = scan_pbf_blobs(filename);

    // Filter to OSMData blobs only
    std::vector<size_t> data_blob_indices;
    for (size_t i = 0; i < blobs.size(); i++) {
        if (blobs[i].type == "OSMData") {
            data_blob_indices.push_back(i);
        }
    }

    std::cerr << "  PBF: " << blobs.size() << " blobs, "
              << data_blob_indices.size() << " data blocks, "
              << num_threads << " threads" << std::endl;

    // Open file descriptor for each thread (pread is thread-safe with separate fds)
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("cannot open " + filename);

    // Parallel decode
    std::atomic<size_t> next_block{0};
    std::mutex callback_mutex;
    std::vector<std::thread> threads;

    for (unsigned t = 0; t < num_threads; t++) {
        threads.emplace_back([&]() {
            // Each thread opens its own fd for independent pread
            int local_fd = open(filename.c_str(), O_RDONLY);
            if (local_fd < 0) return;

            while (true) {
                size_t idx = next_block.fetch_add(1);
                if (idx >= data_blob_indices.size()) break;

                size_t blob_idx = data_blob_indices[idx];
                const auto& info = blobs[blob_idx];

                std::string decompressed = read_and_decompress_blob(local_fd, info);
                PbfBlock block = decode_pbf_blob(decompressed.data(), decompressed.size());

                if (entity_filter.find('n') == std::string::npos) block.nodes.clear();
                if (entity_filter.find('w') == std::string::npos) block.ways.clear();
                if (entity_filter.find('r') == std::string::npos) block.relations.clear();

                std::lock_guard<std::mutex> lock(callback_mutex);
                callback(block, idx);
            }

            close(local_fd);
        });
    }

    for (auto& t : threads) t.join();
    close(fd);
}

// --- PbfFile implementation ---

PbfFile::PbfFile(const std::string& filename, unsigned num_threads)
    : filename_(filename), num_threads_(num_threads) {
    if (num_threads_ == 0) num_threads_ = std::thread::hardware_concurrency();
    if (num_threads_ == 0) num_threads_ = 4;

    blobs_ = scan_pbf_blobs(filename_);

    // Advise kernel to preload PBF into page cache.
    // Dramatically reduces pread latency for parallel reads (72% faster in testing).
    int fd = open(filename_.c_str(), O_RDONLY);
    if (fd >= 0) {
        off_t file_size = lseek(fd, 0, SEEK_END);
        posix_fadvise(fd, 0, file_size, POSIX_FADV_WILLNEED);
        close(fd);
        std::cerr << "  PBF: " << blobs_.size() << " blobs, "
                  << (file_size / (1024*1024)) << " MiB (preloading to page cache)" << std::endl;
    } else {
        std::cerr << "  PBF: " << blobs_.size() << " blobs" << std::endl;
    }
}

// Lightweight I/O semaphore — limits concurrent preads without queue overhead.
// All threads still do their own read+decode+callback, but only max_io threads
// can be in the pread phase simultaneously.
struct IoSemaphore {
    std::mutex mtx;
    std::condition_variable cv;
    unsigned max_concurrent;
    unsigned active = 0;
    IoSemaphore(unsigned max) : max_concurrent(max) {}
    void acquire() {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [&]{ return active < max_concurrent; });
        active++;
    }
    void release() {
        std::lock_guard<std::mutex> lock(mtx);
        active--;
        cv.notify_one();
    }
};

void PbfFile::read_nodes_streaming(const NodeCallback& callback) {
    std::vector<size_t> data_indices;
    for (size_t i = 0; i < blobs_.size(); i++)
        if (blobs_[i].type == "OSMData") data_indices.push_back(i);
    if (data_indices.empty()) return;

    // Limit concurrent I/O to avoid saturating slow storage.
    // On fast storage, all threads get through the semaphore quickly.
    // On slow storage, excess threads do decode+callback while waiting.
    unsigned max_io;
    const char* max_io_env = getenv("BUILD_MAX_IO");
    if (max_io_env) {
        max_io = static_cast<unsigned>(std::atoi(max_io_env));
        if (max_io == 0) max_io = num_threads_;
    } else {
        max_io = std::min(num_threads_, std::max(8u, num_threads_ / 2));
    }
    IoSemaphore io_sem(max_io);

    std::atomic<size_t> next_idx{0};
    std::atomic<size_t> blocks_done{0};
    std::vector<std::thread> threads;

    struct ThreadStats { double read_us = 0, decode_us = 0; size_t count = 0; };
    std::vector<ThreadStats> stats(num_threads_);

    for (unsigned t = 0; t < num_threads_; t++) {
        threads.emplace_back([&, t]() {
            int local_fd = open(filename_.c_str(), O_RDONLY);
            if (local_fd < 0) return;
            std::string decomp, blob_buf;
            auto& st = stats[t];
            while (true) {
                size_t j = next_idx.fetch_add(1);
                if (j >= data_indices.size()) break;

                io_sem.acquire();
                auto t0 = std::chrono::steady_clock::now();
                read_and_decompress_blob_into(local_fd, blobs_[data_indices[j]], blob_buf, decomp);
                auto t1 = std::chrono::steady_clock::now();
                io_sem.release();

                decode_nodes_streaming(decomp.data(), decomp.size(), callback);
                auto t2 = std::chrono::steady_clock::now();
                st.read_us += std::chrono::duration<double, std::micro>(t1 - t0).count();
                st.decode_us += std::chrono::duration<double, std::micro>(t2 - t1).count();
                st.count++;
                size_t done = blocks_done.fetch_add(1) + 1;
                if (done % 5000 == 0)
                    std::cerr << "  Processed " << done << "/" << data_indices.size() << " blocks..." << std::endl;
            }
            close(local_fd);
        });
    }
    for (auto& t : threads) t.join();

    double total_read = 0, total_decode = 0; size_t total_blocks = 0;
    for (auto& s : stats) { total_read += s.read_us; total_decode += s.decode_us; total_blocks += s.count; }
    std::cerr << "  Node streaming (" << total_blocks << " blocks, " << num_threads_ << " threads, "
              << max_io << " max_io): "
              << "read=" << (total_read/1e6) << "s decode+callback=" << (total_decode/1e6) << "s"
              << " (per-thread avg: read=" << (total_read/1e6/num_threads_)
              << "s decode+cb=" << (total_decode/1e6/num_threads_) << "s)" << std::endl;
}

void PbfFile::read_ways_streaming(const WayCallback& callback) {
    std::vector<size_t> data_indices;
    for (size_t i = 0; i < blobs_.size(); i++)
        if (blobs_[i].type == "OSMData") data_indices.push_back(i);
    if (data_indices.empty()) return;

    unsigned max_io;
    const char* max_io_env = getenv("BUILD_MAX_IO");
    if (max_io_env) {
        max_io = static_cast<unsigned>(std::atoi(max_io_env));
        if (max_io == 0) max_io = num_threads_;
    } else {
        max_io = std::min(num_threads_, std::max(8u, num_threads_ / 2));
    }
    IoSemaphore io_sem(max_io);

    std::atomic<size_t> next_idx{0};
    std::atomic<size_t> blocks_done{0};
    std::vector<std::thread> threads;

    struct ThreadStats { double read_us = 0, decode_us = 0; size_t count = 0; };
    std::vector<ThreadStats> stats(num_threads_);

    for (unsigned t = 0; t < num_threads_; t++) {
        threads.emplace_back([&, t]() {
            int local_fd = open(filename_.c_str(), O_RDONLY);
            if (local_fd < 0) return;
            std::string decomp, blob_buf;
            auto& st = stats[t];
            while (true) {
                size_t j = next_idx.fetch_add(1);
                if (j >= data_indices.size()) break;

                io_sem.acquire();
                auto t0 = std::chrono::steady_clock::now();
                read_and_decompress_blob_into(local_fd, blobs_[data_indices[j]], blob_buf, decomp);
                auto t1 = std::chrono::steady_clock::now();
                io_sem.release();

                decode_ways_streaming(decomp.data(), decomp.size(), callback);
                auto t2 = std::chrono::steady_clock::now();
                st.read_us += std::chrono::duration<double, std::micro>(t1 - t0).count();
                st.decode_us += std::chrono::duration<double, std::micro>(t2 - t1).count();
                st.count++;
                size_t done = blocks_done.fetch_add(1) + 1;
                if (done % 5000 == 0)
                    std::cerr << "  Processed " << done << "/" << data_indices.size() << " blocks..." << std::endl;
            }
            close(local_fd);
        });
    }
    for (auto& t : threads) t.join();

    double total_read = 0, total_decode = 0; size_t total_blocks = 0;
    for (auto& s : stats) { total_read += s.read_us; total_decode += s.decode_us; total_blocks += s.count; }
    std::cerr << "  Way streaming (" << total_blocks << " blocks, " << num_threads_ << " threads, "
              << max_io << " max_io): "
              << "read=" << (total_read/1e6) << "s decode+callback=" << (total_decode/1e6) << "s"
              << " (per-thread avg: read=" << (total_read/1e6/num_threads_)
              << "s decode+cb=" << (total_decode/1e6/num_threads_) << "s)" << std::endl;
}

PbfFile::~PbfFile() = default;

// Peek at a single blob to determine its content type
static int peek_blob_type(int fd, const BlobInfo& info) {
    std::string data = read_and_decompress_blob(fd, info);
    protozero::pbf_reader pb(data);
    while (pb.next()) {
        if (pb.tag() == PrimitiveBlockTag::PRIMITIVEGROUP) {
            protozero::pbf_reader group = pb.get_message();
            while (group.next()) {
                switch (group.tag()) {
                    case PrimitiveGroupTag::NODES:
                    case PrimitiveGroupTag::DENSE: return 0; // nodes
                    case PrimitiveGroupTag::WAYS: return 1;  // ways
                    case PrimitiveGroupTag::RELATIONS: return 2; // relations
                    default: group.skip();
                }
            }
        } else { pb.skip(); }
    }
    return -1;
}

void PbfFile::classify_blobs() {
    if (classified_) return;

    // PBF files are ordered: node blocks → way blocks → relation blocks.
    // Binary search for the boundaries instead of decompressing all 50K blobs.
    std::vector<size_t> data_indices;
    for (size_t i = 0; i < blobs_.size(); i++)
        if (blobs_[i].type == "OSMData") data_indices.push_back(i);

    if (data_indices.empty()) { classified_ = true; return; }

    int fd = open(filename_.c_str(), O_RDONLY);
    if (fd < 0) { classified_ = true; return; }

    // Binary search for node→way boundary
    size_t node_way_boundary = data_indices.size(); // default: all nodes
    {
        size_t lo = 0, hi = data_indices.size();
        while (lo < hi) {
            size_t mid = lo + (hi - lo) / 2;
            int type = peek_blob_type(fd, blobs_[data_indices[mid]]);
            if (type == 0) lo = mid + 1; // nodes — boundary is after this
            else hi = mid;               // ways or relations — boundary is at or before this
        }
        node_way_boundary = lo;
    }

    // Binary search for way→relation boundary
    size_t way_rel_boundary = data_indices.size(); // default: no relations
    {
        size_t lo = node_way_boundary, hi = data_indices.size();
        while (lo < hi) {
            size_t mid = lo + (hi - lo) / 2;
            int type = peek_blob_type(fd, blobs_[data_indices[mid]]);
            if (type <= 1) lo = mid + 1; // nodes or ways
            else hi = mid;               // relations
        }
        way_rel_boundary = lo;
    }

    close(fd);

    for (size_t j = 0; j < node_way_boundary; j++)
        node_blobs_.push_back(data_indices[j]);
    for (size_t j = node_way_boundary; j < way_rel_boundary; j++)
        way_blobs_.push_back(data_indices[j]);
    for (size_t j = way_rel_boundary; j < data_indices.size(); j++)
        relation_blobs_.push_back(data_indices[j]);

    classified_ = true;
    std::cerr << "  PBF classified: " << node_blobs_.size() << " node blocks, "
              << way_blobs_.size() << " way blocks, "
              << relation_blobs_.size() << " relation blocks" << std::endl;
}

void PbfFile::read_blocks(std::function<void(PbfBlock&, unsigned thread_idx)> callback,
                           const std::string& entity_filter,
                           bool ordered) {
    // For read_blocks (used for relations), classify to avoid wasting work
    // on 50K blobs when only 454 contain relations.
    classify_blobs();

    std::vector<size_t> indices;
    if (entity_filter.find('n') != std::string::npos)
        indices.insert(indices.end(), node_blobs_.begin(), node_blobs_.end());
    if (entity_filter.find('w') != std::string::npos)
        indices.insert(indices.end(), way_blobs_.begin(), way_blobs_.end());
    if (entity_filter.find('r') != std::string::npos)
        indices.insert(indices.end(), relation_blobs_.begin(), relation_blobs_.end());

    if (indices.empty()) return;

    bool want_nodes = entity_filter.find('n') != std::string::npos;
    bool want_ways = entity_filter.find('w') != std::string::npos;
    bool want_rels = entity_filter.find('r') != std::string::npos;

    if (ordered) {
        // Ordered mode: decompress + decode in parallel, callback in file order.
        // Ring buffer of decoded PbfBlocks — workers do all heavy work,
        // consumer thread just runs the lightweight callback.
        const size_t WINDOW = num_threads_ * 4;
        std::vector<PbfBlock> ring(WINDOW);
        std::vector<std::atomic<bool>> ring_ready(WINDOW);
        for (auto& r : ring_ready) r.store(false);

        std::atomic<size_t> next_decompress{0};
        std::vector<std::thread> decomp_threads;

        for (unsigned t = 0; t < num_threads_; t++) {
            decomp_threads.emplace_back([&]() {
                int local_fd = open(filename_.c_str(), O_RDONLY);
                if (local_fd < 0) return;
                while (true) {
                    size_t j = next_decompress.fetch_add(1);
                    if (j >= indices.size()) break;
                    size_t slot = j % WINDOW;
                    while (ring_ready[slot].load(std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }
                    std::string data = read_and_decompress_blob(local_fd, blobs_[indices[j]]);
                    ring[slot] = decode_pbf_blob(data.data(), data.size());
                    if (!want_nodes) ring[slot].nodes.clear();
                    if (!want_ways) ring[slot].ways.clear();
                    if (!want_rels) ring[slot].relations.clear();
                    ring_ready[slot].store(true, std::memory_order_release);
                }
                close(local_fd);
            });
        }

        // Consume in order — callback only (lightweight)
        for (size_t j = 0; j < indices.size(); j++) {
            size_t slot = j % WINDOW;
            while (!ring_ready[slot].load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            auto& block = ring[slot];

            if (!want_nodes) block.nodes.clear();
            if (!want_ways) block.ways.clear();
            if (!want_rels) block.relations.clear();

            callback(block, 0);
            ring_ready[slot].store(false, std::memory_order_release);

            if ((j + 1) % 1000 == 0) {
                std::cerr << "  Processed " << (j + 1) << "/" << indices.size() << " blocks..." << std::endl;
            }
        }

        for (auto& t : decomp_threads) t.join();
        return;
    }

    // Unordered mode: full parallel decode + callback
    // Each thread reuses a local PbfBlock to avoid repeated allocation.

    std::atomic<size_t> next_idx{0};
    std::atomic<size_t> blocks_done{0};
    std::vector<std::thread> threads;

    // Per-thread timing accumulators
    struct ThreadStats {
        double read_us = 0, decode_us = 0, callback_us = 0;
        size_t count = 0;
    };
    std::vector<ThreadStats> stats(num_threads_);

    for (unsigned t = 0; t < num_threads_; t++) {
        threads.emplace_back([&, t]() {
            int local_fd = open(filename_.c_str(), O_RDONLY);
            if (local_fd < 0) return;
            PbfBlock block;
            std::string decomp, blob_buf;
            auto& st = stats[t];

            while (true) {
                size_t j = next_idx.fetch_add(1);
                if (j >= indices.size()) break;

                auto t0 = std::chrono::steady_clock::now();
                size_t blob_idx = indices[j];
                read_and_decompress_blob_into(local_fd, blobs_[blob_idx], blob_buf, decomp);
                auto t1 = std::chrono::steady_clock::now();

                decode_pbf_blob_into(decomp.data(), decomp.size(), block);
                auto t2 = std::chrono::steady_clock::now();

                if (!want_nodes) block.nodes.clear();
                if (!want_ways) block.ways.clear();
                if (!want_rels) block.relations.clear();

                callback(block, t);
                auto t3 = std::chrono::steady_clock::now();

                st.read_us += std::chrono::duration<double, std::micro>(t1 - t0).count();
                st.decode_us += std::chrono::duration<double, std::micro>(t2 - t1).count();
                st.callback_us += std::chrono::duration<double, std::micro>(t3 - t2).count();
                st.count++;

                size_t done = blocks_done.fetch_add(1) + 1;
                if (done % 1000 == 0) {
                    std::cerr << "  Processed " << done << "/" << indices.size() << " blocks..." << std::endl;
                }
            }
            close(local_fd);
        });
    }

    for (auto& t : threads) t.join();

    // Report timing breakdown
    double total_read = 0, total_decode = 0, total_callback = 0;
    size_t total_blocks = 0;
    for (auto& s : stats) {
        total_read += s.read_us;
        total_decode += s.decode_us;
        total_callback += s.callback_us;
        total_blocks += s.count;
    }
    std::cerr << "  Block timing (" << total_blocks << " blocks, " << num_threads_ << " threads): "
              << "read=" << (total_read/1e6) << "s decode=" << (total_decode/1e6)
              << "s callback=" << (total_callback/1e6) << "s"
              << " (per-thread avg: read=" << (total_read/1e6/num_threads_)
              << "s decode=" << (total_decode/1e6/num_threads_)
              << "s callback=" << (total_callback/1e6/num_threads_) << "s)"
              << std::endl;

    // Skip the duplicate join below
    threads.clear();
    return;

    for (auto& t : threads) t.join();
}
