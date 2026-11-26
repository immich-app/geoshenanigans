// Whole zlib-format (RFC 1950) blobs and gzip files (RFC 1952) through
// libdeflate, which inflates PBF blobs 2.5x and deflates them 2.2x faster
// than zlib on the planet. Inflating gives zlib's bytes; deflating gives
// other, equally valid bytes. Streaming and partial inflates (the .osc.gz
// reader, pbf-apply's blob peeks) stay on zlib: libdeflate only takes a
// whole buffer.
#pragma once

#include <libdeflate.h>

#include <algorithm>
#include <array>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>

// Inflates the zlib data `in` into out[0, capacity) and returns the bytes it
// wrote. Throws on corrupt data and on data that inflates past capacity.
inline size_t zlib_inflate(std::string_view in, char* out, size_t capacity) {
    struct Free {
        void operator()(libdeflate_decompressor* d) const { libdeflate_free_decompressor(d); }
    };
    thread_local std::unique_ptr<libdeflate_decompressor, Free> decompressor(libdeflate_alloc_decompressor());
    if (!decompressor) throw std::bad_alloc();

    size_t size = 0;
    switch (libdeflate_zlib_decompress(decompressor.get(), in.data(), in.size(), out, capacity, &size)) {
        case LIBDEFLATE_SUCCESS: return size;
        case LIBDEFLATE_INSUFFICIENT_SPACE: throw std::runtime_error("zlib data inflates past its raw size");
        default: throw std::runtime_error("zlib data is corrupt");
    }
}

// The bytes of a whole gzip file (RFC 1952), every member's in turn. Throws
// on bytes that are no gzip members back to back, corrupt or cut short.
inline std::string gzip_inflate(std::string_view in) {
    struct Free {
        void operator()(libdeflate_decompressor* d) const { libdeflate_free_decompressor(d); }
    };
    thread_local std::unique_ptr<libdeflate_decompressor, Free> decompressor(libdeflate_alloc_decompressor());
    if (!decompressor) throw std::bad_alloc();

    std::string out;
    size_t at = 0, room = std::max<size_t>(4 * in.size(), 1 << 16);
    do {
        size_t used = 0, size = 0;
        out.resize(out.size() + room);
        char* to = out.data() + out.size() - room;
        switch (libdeflate_gzip_decompress_ex(decompressor.get(), in.data() + at, in.size() - at, to, room, &used, &size)) {
            case LIBDEFLATE_SUCCESS:
                out.resize(out.size() - room + size);
                at += used;
                break;
            case LIBDEFLATE_INSUFFICIENT_SPACE:
                out.resize(out.size() - room);
                room *= 2;
                break;
            default: throw std::runtime_error("gzip data is corrupt or cut short");
        }
    } while (at < in.size());
    return out;
}

// The zlib data of raw at a compression level of 0-12 (zlib's 0-9 and three
// slower ones).
inline std::string zlib_deflate(std::string_view raw, int level) {
    struct Free {
        void operator()(libdeflate_compressor* c) const { libdeflate_free_compressor(c); }
    };
    constexpr int kLevels = 13;
    if (level < 0 || level >= kLevels) throw std::invalid_argument("zlib level " + std::to_string(level));
    thread_local std::array<std::unique_ptr<libdeflate_compressor, Free>, kLevels> compressors;
    auto& compressor = compressors[size_t(level)];
    if (!compressor) compressor.reset(libdeflate_alloc_compressor(level));
    if (!compressor) throw std::bad_alloc();

    std::string out(libdeflate_zlib_compress_bound(compressor.get(), raw.size()), '\0');
    out.resize(libdeflate_zlib_compress(compressor.get(), raw.data(), raw.size(), out.data(), out.size()));
    if (out.empty()) throw std::runtime_error("zlib deflate failed");
    return out;
}
