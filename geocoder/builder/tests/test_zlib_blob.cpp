// Unit tests for zlib_blob.h (whole zlib blobs through libdeflate).
#include "zlib_blob.h"

#include <stdexcept>
#include <string>
#include <zlib.h>

#include "test_framework.h"

namespace {

// Text-like bytes that compress, as PBF blocks do.
std::string sample(size_t n) {
    std::string s(n, '\0');
    for (size_t i = 0; i < n; i++) s[i] = "abcdefghij0123456789 ,.:"[(i * 7 + i / 13) % 24];
    return s;
}

std::string zlib_compress(const std::string& raw, int level) {
    uLongf size = compressBound(static_cast<uLong>(raw.size()));
    std::string z(size, '\0');
    if (compress2(reinterpret_cast<Bytef*>(z.data()), &size, reinterpret_cast<const Bytef*>(raw.data()),
                  static_cast<uLong>(raw.size()), level) != Z_OK)
        throw std::runtime_error("compress2 failed");
    z.resize(size);
    return z;
}

std::string zlib_uncompress(const std::string& z, size_t raw_size) {
    std::string raw(raw_size, '\0');
    uLongf size = static_cast<uLongf>(raw_size);
    if (uncompress(reinterpret_cast<Bytef*>(raw.data()), &size, reinterpret_cast<const Bytef*>(z.data()),
                   static_cast<uLong>(z.size())) != Z_OK)
        throw std::runtime_error("uncompress failed");
    raw.resize(size);
    return raw;
}

template <class Fn>
bool throws_runtime_error(Fn fn) {
    try {
        fn();
    } catch (const std::runtime_error&) {
        return true;
    }
    return false;
}

}  // namespace

TEST(zlib_inflate_gives_the_bytes_zlib_compressed) {
    for (size_t n : {size_t(1), size_t(1000), size_t(3) << 20}) {
        const std::string raw = sample(n);
        const std::string z = zlib_compress(raw, 6);
        std::string out(n, '\0');
        CHECK_EQ(zlib_inflate(z, out.data(), out.size()), n);
        CHECK(out == raw);
    }
}

TEST(zlib_inflate_reports_fewer_bytes_than_its_room) {
    const std::string raw = sample(500);
    std::string out(800, '\0');
    CHECK_EQ(zlib_inflate(zlib_compress(raw, 6), out.data(), out.size()), size_t(500));
    CHECK(out.compare(0, 500, raw) == 0);
}

TEST(zlib_inflate_rejects_data_past_its_room_and_corrupt_data) {
    const std::string z = zlib_compress(sample(500), 6);
    std::string small(499, '\0');
    CHECK(throws_runtime_error([&] { zlib_inflate(z, small.data(), small.size()); }));

    std::string corrupt = z;
    corrupt[corrupt.size() / 2] ^= 0x5a;
    corrupt[corrupt.size() / 2 + 1] ^= 0xa5;
    std::string out(500, '\0');
    CHECK(throws_runtime_error([&] { zlib_inflate(corrupt, out.data(), out.size()); }));
    CHECK(throws_runtime_error([&] { zlib_inflate(std::string_view(), out.data(), out.size()); }));
}

TEST(zlib_deflate_output_inflates_through_zlib_at_every_level) {
    const std::string raw = sample(200000);
    for (int level : {0, 1, 6, 9, 12}) {
        const std::string z = zlib_deflate(raw, level);
        CHECK(zlib_uncompress(z, raw.size()) == raw);
    }
    CHECK(zlib_uncompress(zlib_deflate(std::string(), 6), 0).empty());
}

TEST(zlib_deflate_rejects_levels_past_twelve) {
    bool thrown = false;
    try {
        zlib_deflate("x", 13);
    } catch (const std::invalid_argument&) {
        thrown = true;
    }
    CHECK(thrown);
}
