// gen_coord_vectors: writes geocoder/testdata/coord_keys.tsv, the keys, zigzags, lengths, packed
// bit fields and varints the builder's functions (builder/src/coord_keys.h) give for a fixed set
// of inputs, which the builder's and the server's tests both check (the server's functions,
// coord_keys.rs, must give the same).
//   g++ -std=c++20 -O1 -I geocoder/builder/src geocoder/testdata/gen_coord_vectors.cpp -o gen
//   ./gen > geocoder/testdata/coord_keys.tsv
#include <cfloat>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "coord_keys.h"

using namespace coord_keys;

static std::string hex(const std::vector<uint8_t>& b) {
    if (b.empty()) return "-";
    std::string out;
    char c[3];
    for (uint8_t x : b) {
        snprintf(c, sizeof c, "%02x", x);
        out += c;
    }
    return out;
}

static void key(uint32_t bits) {
    uint32_t k;
    if (hybrid_key(bits, k)) printf("key\t%08x\t%08x,%08x\n", bits, order_key(bits), k);
    else printf("key\t%08x\t%08x,-\n", bits, order_key(bits));
}

static void hybrid(uint32_t k) {
    if (is_hybrid_key(k)) printf("hybrid\t%08x\t%08x\n", k, from_hybrid_key(k));
    else printf("hybrid\t%08x\t-\n", k);
}

static void length(uint32_t v) { printf("length\t%08x\t%u,%u\n", v, bit_length(v), byte_length(v)); }

using Fields = std::vector<std::pair<uint32_t, uint32_t>>;  // value, width

static void bits(const Fields& fields) {
    uint64_t n = 0;
    for (const auto& f : fields) n += f.second;
    // Zeroed, with the 8 bytes to spare put_bits asks for.
    std::vector<uint8_t> buf((n + 7) / 8 + 8, 0);
    std::string in;
    uint64_t bit = 0;
    for (const auto& [v, w] : fields) {
        put_bits(buf.data(), bit, v, w);
        bit += w;
        char c[24];
        snprintf(c, sizeof c, "%s%x:%u", in.empty() ? "" : ",", v, w);
        in += c;
    }
    buf.resize(buf.size() - 8);
    printf("bits\t%s\t%s\n", in.empty() ? "-" : in.c_str(), hex(buf).c_str());
}

static void varint(uint64_t v) {
    uint8_t b[VARINT_MAX_BYTES];
    const size_t n = put_varint(b, v);
    printf("varint\t%" PRIu64 "\t%s\n", v, hex(std::vector<uint8_t>(b, b + n)).c_str());
}

static void dec4(uint32_t bits) {
    int32_t k;
    if (dec4_key(bits, k)) printf("dec4\t%08x\t%d,%d\n", bits, k, dec4_round(bits));
    else printf("dec4\t%08x\t-,%d\n", bits, dec4_round(bits));
}

using Codes = std::vector<std::pair<uint32_t, uint32_t>>;  // z, k

static void rice(const Codes& codes) {
    uint64_t n = 0;
    for (const auto& [z, k] : codes) n += rice_bits(z, k);
    std::vector<uint8_t> buf((n + 7) / 8 + 8, 0);
    std::string in;
    uint64_t bit = 0;
    for (const auto& [z, k] : codes) {
        put_rice(buf.data(), bit, z, k);
        char c[24];
        snprintf(c, sizeof c, "%s%x:%u", in.empty() ? "" : ",", z, k);
        in += c;
    }
    buf.resize(buf.size() - 8);
    printf("rice\t%s\t%s\n", in.c_str(), hex(buf).c_str());
}

static void ricek(const std::vector<uint32_t>& z) {
    const uint32_t k = best_rice_k(z.data(), z.size());
    uint64_t bits = 0;
    std::string in;
    for (uint32_t v : z) {
        bits += rice_bits(v, k);
        char c[16];
        snprintf(c, sizeof c, "%s%x", in.empty() ? "" : ",", v);
        in += c;
    }
    printf("ricek\t%s\t%u,%" PRIu64 "\n", in.empty() ? "-" : in.c_str(), k, bits);
}

static void novarint(const std::vector<uint8_t>& b, size_t max_bytes) {
    size_t at = 0;
    uint64_t v;
    if (get_varint(b.data(), b.size(), at, max_bytes, v)) {
        fprintf(stderr, "%s is a varint of at most %zu bytes\n", hex(b).c_str(), max_bytes);
        exit(1);
    }
    printf("novarint\t%s\t%zu\n", hex(b).c_str(), max_bytes);
}

int main() {
    printf("# Coordinate keys and bit fields as the builder's functions give them (builder/src/coord_keys.h),\n");
    printf("# which the builder's tests and the server's (server/src/coord_keys.rs) both check. A row is a\n");
    printf("# kind, what goes in and what comes out; - stands for nothing:\n");
    printf("#   key       an f32 bit pattern (hex): its order key and its hybrid key (hex; - when it has none)\n");
    printf("#   hybrid    a u32 (hex): the pattern it is the hybrid key of (hex; - when it is no pattern's)\n");
    printf("#   zigzag32  a u32 (hex): its zigzag (hex)\n");
    printf("#   zigzag16  a u16 (hex): its zigzag (hex)\n");
    printf("#   length    a u32 (hex): its bit length and its byte length\n");
    printf("#   bits      fields, value:width each (hex:decimal): the bytes they pack to, LSB first, the\n");
    printf("#             last byte filled with zero bits (hex)\n");
    printf("#   varint    a u64 (decimal): its LEB128 bytes (hex)\n");
    printf("#   novarint  bytes (hex): a limit; no varint of at most that many bytes, in its fewest, starts them\n");
    printf("#   dec4      an f32 bit pattern (hex): its decimal key (decimal; - when it has none) and its\n");
    printf("#             decimal round (decimal)\n");
    printf("#   undec4    an i32 (decimal): the pattern it decodes to as a decimal key (hex)\n");
    printf("#   rice      codes, z:k each (hex:decimal): the bytes they pack to, LSB first, the last byte\n");
    printf("#             filled with zero bits (hex)\n");
    printf("#   ricek     values (hex; - for none): the least Rice parameter of the fewest bits, and the bits\n");
    printf("# Each also holds backwards: a key gives its pattern, a zigzag its difference, the bytes their\n");
    printf("# fields, codes and their number. Regenerate with geocoder/testdata/gen_coord_vectors.cpp.\n");
    printf("kind\tin\tout\n");

    const uint32_t sign = 0x80000000u;
    const uint32_t inf = bits_of(std::numeric_limits<float>::infinity());
    // Zeros, the smallest floats, the floats at and beside a multiple of 1e-7, 1.0 and its
    // neighbours, halves of a multiple, coordinates, infinities, NaNs and the largest patterns.
    const uint32_t tenth = bits_of(0.1f), half = bits_of(0.5f), least = from_hybrid_key(1);
    const uint32_t patterns[] = {
        0, sign, 1, sign | 1, 0x007FFFFF, bits_of(FLT_MIN), bits_of(-FLT_MIN),
        least, least + 1, least - 1, from_hybrid_key(2), least | sign, bits_of(4e-8f), bits_of(6e-8f), bits_of(-6e-8f),
        tenth, tenth + 1, tenth - 1, tenth | sign, half, half - 1, half + 1,
        bits_of(1.0f / 256), bits_of(3.0f / 256), bits_of(-1.0f / 256), bits_of(0.1234567f), bits_of(-0.1234567f),
        ONE_BITS - 2, ONE_BITS - 1, ONE_BITS, ONE_BITS + 1,
        (ONE_BITS - 2) | sign, (ONE_BITS - 1) | sign, ONE_BITS | sign, (ONE_BITS + 1) | sign,
        bits_of(52.37f), bits_of(4.89f), bits_of(-33.87f), bits_of(151.21f), bits_of(90.0f), bits_of(-90.0f), bits_of(180.0f),
        bits_of(-180.0f), bits_of(179.99999f), bits_of(FLT_MAX), bits_of(-FLT_MAX),
        inf, inf | sign, inf + 1, 0x7FC00000, 0xFFC00000, 0x7FFFFFFF, 0xFFFFFFFF,
    };
    for (uint32_t b : patterns) key(b);
    std::mt19937_64 rng(20261114);
    // One draw a statement, so the vectors don't depend on the order a compiler evaluates
    // arguments in.
    for (int i = 0; i < 40; i++) {
        const uint32_t any = uint32_t(rng());
        key(any);
    }
    for (int i = 0; i < 40; i++) {
        // A coordinate of seven decimals below 1.0, either sign, and the float after it.
        const uint32_t digits = uint32_t(rng() % TEN_MILLION);
        const uint32_t s = rng() % 2 ? sign : 0;
        key(from_hybrid_key(digits) | s);
        key((from_hybrid_key(digits) + 1) | s);
    }
    for (int i = 0; i < 40; i++) {
        const double degrees = double(int64_t(rng() % 3600000001ull) - 1800000000) / 1e7;
        key(bits_of(float(degrees)));
    }

    // Keys at the edges of the two ranges, either sign, and numbers past the largest magnitude.
    const uint32_t keys[] = {
        0, 1, 0u - 1, 2, 0u - 2, TEN_MILLION - 1, 0u - (TEN_MILLION - 1), TEN_MILLION, 0u - TEN_MILLION, TEN_MILLION + 1,
        HYBRID_KEY_MAX - 1, HYBRID_KEY_MAX, 0u - HYBRID_KEY_MAX, HYBRID_KEY_MAX + 1, 0u - (HYBRID_KEY_MAX + 1),
        0x7FFFFFFF, sign, sign + 1, 0x60000000, 0xA0000000,
    };
    for (uint32_t k : keys) hybrid(k);
    for (int i = 0; i < 20; i++) {
        const uint32_t mag = uint32_t(rng() % (uint64_t(HYBRID_KEY_MAX) + 1));
        const uint32_t shift = uint32_t(rng() % 31);
        const bool negative = rng() % 2;
        hybrid(negative ? 0u - (mag >> shift) : mag >> shift);
    }

    const uint32_t steps[] = {0, 1, 0u - 1, 2, 0u - 2, 0x7FFFFFFF, sign, sign + 1, 0x12345678, 0xFEDCBA98};
    for (uint32_t d : steps) printf("zigzag32\t%08x\t%08x\n", d, zigzag32(d));
    for (int i = 0; i < 12; i++) {
        const uint32_t shift = uint32_t(rng() % 32);
        const uint32_t d = uint32_t(rng()) >> shift;
        const bool negative = rng() % 2;
        printf("zigzag32\t%08x\t%08x\n", negative ? 0u - d : d, zigzag32(negative ? 0u - d : d));
    }
    const uint16_t short_steps[] = {0, 1, 0xFFFF, 2, 0xFFFE, 0x7FFF, 0x8000, 0x8001, 0x1234, 0xFEDC};
    for (uint16_t d : short_steps) printf("zigzag16\t%04x\t%04x\n", unsigned(d), unsigned(zigzag16(d)));
    for (int i = 0; i < 8; i++) {
        const uint32_t shift = uint32_t(rng() % 16);
        const uint16_t d = uint16_t(uint16_t(rng()) >> shift);
        const bool negative = rng() % 2;
        const uint16_t signed_d = negative ? uint16_t(0u - d) : d;
        printf("zigzag16\t%04x\t%04x\n", unsigned(signed_d), unsigned(zigzag16(signed_d)));
    }

    const uint32_t values[] = {0,      1,       2,        3,         0x7F,       0x80, 0xFF,      0x100,
                               0xFFFF, 0x10000, 0xFFFFFF, 0x1000000, 0x7FFFFFFF, sign, 0xFFFFFFFF};
    for (uint32_t v : values) length(v);
    for (int i = 0; i < 10; i++) {
        const uint32_t shift = uint32_t(rng() % 32);
        length(uint32_t(rng()) >> shift);
    }

    bits({});
    bits({{0, 0}});
    bits({{1, 1}});
    bits({{0, 1}});
    bits({{0, 0}, {1, 1}, {0, 0}});
    bits({{0xFF, 8}});
    bits({{0x1FF, 9}});
    bits({{0xFFFFFFFFu, 32}});
    bits({{0, 3}, {1, 1}, {0x1F, 5}});
    bits({{1, 1}, {0xFFFFFFFFu, 32}, {0, 3}, {0x7F, 7}});
    bits({{0, 7}, {0x80000001u, 32}, {0, 7}, {0x80000001u, 32}});
    {
        // Every width, each field with its top and bottom bit set; then 40 single bits.
        Fields every, ones;
        for (uint32_t w = 1; w <= 32; w++) every.push_back({(1u << (w - 1)) | 1, w});
        for (int i = 0; i < 40; i++) ones.push_back({uint32_t(i % 3 != 1), 1});
        bits(every);
        bits(ones);
    }
    for (int i = 0; i < 10; i++) {
        Fields fields;
        const size_t n = 1 + rng() % 24;
        for (size_t k = 0; k < n; k++) {
            const uint32_t w = uint32_t(rng() % 33);
            const uint32_t v = uint32_t(rng());
            fields.push_back({w == 32 ? v : v & ((1u << w) - 1), w});
        }
        bits(fields);
    }

    const uint64_t one = 1;
    const uint64_t numbers[] = {
        0, 1, 127, 128, 255, 16383, 16384, 65535, 65536, 2097151, 2097152, UINT32_MAX, one << 32,
        (one << 56) - 1, one << 56, (one << 63) - 1, one << 63, UINT64_MAX,
    };
    for (uint64_t v : numbers) varint(v);
    for (int i = 0; i < 8; i++) {
        const uint64_t shift = rng() % 64;
        varint(rng() >> shift);
    }
    // No bytes, a last byte that says another follows, a number spelled with a zero byte more,
    // one past 64 bits, one of eleven bytes, and two longer than their reader allows.
    novarint({}, 10);
    novarint({0x80}, 10);
    novarint({0xFF, 0xFF}, 10);
    novarint({0x80, 0x00}, 10);
    novarint({0xFF, 0x00}, 10);
    novarint({0x80, 0x80, 0x00}, 10);
    novarint({0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02}, 10);
    novarint({0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}, 10);
    novarint({0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}, 11);
    novarint({0x80, 0x80, 0x01}, 2);
    novarint({0xFF, 0xFF, 0x03}, 2);
    novarint({0x80, 0x01}, 1);

    // Coordinates of four decimals and the floats beside them, zeros, the globe's edges, 1e-4,
    // halves of 1e-4 (0.03125 and 0.09375 are 312.5 and 937.5 units), floats past 2^31 units,
    // the infinities, NaNs and the largest floats.
    const float four[] = {52.3702f, -0.1278f, 4.8952f, 151.2093f, -33.8688f, 1e-4f, -1e-4f, 180.0f, -180.0f, 90.0f, -90.0f};
    for (float v : four) {
        dec4(bits_of(v));
        dec4(bits_of(v) + 1);
        dec4(bits_of(v) - 1);
    }
    const uint32_t odd[] = {
        0, sign, 1, sign | 1, bits_of(0.00005f), bits_of(-0.00005f), bits_of(0.03125f), bits_of(-0.03125f),
        bits_of(0.09375f), bits_of(52.37025f), bits_of(214748.359375f), bits_of(214748.375f), bits_of(-214748.375f),
        bits_of(1e30f), bits_of(-1e30f), bits_of(FLT_MAX), inf, inf | sign, 0x7FC00000, 0xFFC00000, 0x7FFFFFFF,
    };
    for (uint32_t b : odd) dec4(b);
    for (int i = 0; i < 20; i++) {
        // A coordinate of four decimals up to 180 in magnitude, and the float after it.
        const int32_t units = int32_t(int64_t(rng() % 3600001) - 1800000);
        dec4(from_dec4_key(units));
        dec4(from_dec4_key(units) + 1);
    }
    for (int i = 0; i < 10; i++) {
        const uint32_t any = uint32_t(rng());
        dec4(any);
    }
    const int32_t dec_keys[] = {0, 1, -1, 1800000, -1800000, 2147483594, INT32_MAX, INT32_MIN};
    for (int32_t k : dec_keys) printf("undec4\t%d\t%08x\n", k, from_dec4_key(k));
    for (int i = 0; i < 4; i++) {
        const int32_t k = int32_t(uint32_t(rng()));
        printf("undec4\t%d\t%08x\n", k, from_dec4_key(k));
    }

    // Single codes at k 0, 5 and 31: q 0, 11 and 12 (an escape), z 0 and 2^32 - 1; then runs.
    rice({{0, 0}});
    rice({{11, 0}});
    rice({{12, 0}});
    rice({{0, 5}});
    rice({{11u << 5 | 31, 5}});
    rice({{12u << 5, 5}});
    rice({{0, 31}});
    rice({{0x7FFFFFFFu, 31}});
    rice({{0xFFFFFFFFu, 31}});
    rice({{0xFFFFFFFFu, 0}});
    rice({{0xFFFFFFFFu, 5}});
    rice({{0, 0}, {0, 0}, {0, 0}});
    rice({{2, 1}, {0xFEDCBA98u, 3}});
    rice({{12, 0}, {1, 0}, {0xFFFFFFFFu, 31}, {5, 2}});
    for (int i = 0; i < 6; i++) {
        Codes codes;
        const size_t n = 1 + rng() % 20;
        const uint32_t k = uint32_t(rng() % 32);
        for (size_t c = 0; c < n; c++) {
            const uint32_t shift = uint32_t(rng() % 32);
            const uint32_t z = uint32_t(rng()) >> shift;
            codes.push_back({z, k});
        }
        rice(codes);
    }

    ricek({});
    ricek({0});
    ricek({0, 0, 0});
    ricek({0xFFFFFFFFu});
    ricek({12});
    ricek({2, 3});
    ricek({1, 1, 1, 0x10000000u});
    for (int i = 0; i < 8; i++) {
        // Steps of a typical width, a few of them far wider.
        std::vector<uint32_t> z(1 + rng() % 63);
        const uint32_t typical = uint32_t(rng() % 25);
        for (uint32_t& v : z) {
            const bool wide = rng() % 10 == 0;
            const uint32_t w = wide ? uint32_t(rng() % 33) : typical;
            const uint32_t r = uint32_t(rng());
            v = w == 0 ? 0 : r >> (32 - w);
        }
        ricek(z);
    }
    return 0;
}
