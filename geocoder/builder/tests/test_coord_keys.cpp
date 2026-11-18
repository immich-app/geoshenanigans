// The coordinate keys and bit fields (coord_keys.h): each key comes back to
// the bit pattern it was made from, the hybrid key exists exactly where the
// format says, and the fields, zigzags and varints are the shared vectors'.
#include <cfloat>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "coord_keys.h"
#include "test_framework.h"

using namespace coord_keys;

namespace {

constexpr uint32_t SIGN = 0x80000000u;

bool has_hybrid_key(uint32_t bits) {
    uint32_t key;
    return hybrid_key(bits, key);
}

// The hybrid key of a pattern that has one.
uint32_t hybrid_key_of(uint32_t bits) {
    uint32_t key = 0;
    REQUIRE(hybrid_key(bits, key));
    return key;
}

using Fields = std::vector<std::pair<uint32_t, uint32_t>>;  // value, width

// The fields a bit at a time, LSB first, the last byte's spare bits zero.
std::vector<uint8_t> packed_bit_by_bit(const Fields& fields) {
    std::vector<uint8_t> out;
    uint64_t bit = 0;
    for (const auto& [v, w] : fields)
        for (uint32_t k = 0; k < w; k++, bit++) {
            if (bit % 8 == 0) out.push_back(0);
            out[bit / 8] |= uint8_t(((v >> k) & 1) << (bit % 8));
        }
    return out;
}

// The fields through put_bits, in a buffer with the 8 spare bytes it asks for.
std::vector<uint8_t> packed(const Fields& fields) {
    uint64_t bits = 0;
    for (const auto& f : fields) bits += f.second;
    std::vector<uint8_t> buf((bits + 7) / 8 + 8, 0);
    uint64_t bit = 0;
    for (const auto& [v, w] : fields) {
        put_bits(buf.data(), bit, v, w);
        bit += w;
    }
    for (size_t k = buf.size() - 8; k < buf.size(); k++) CHECK_EQ(buf[k], 0);
    buf.resize(buf.size() - 8);
    return buf;
}

std::string hex(uint32_t v, int digits = 8) {
    char b[16];
    std::snprintf(b, sizeof b, "%0*x", digits, v);
    return b;
}

std::string hex(const std::vector<uint8_t>& bytes) {
    if (bytes.empty()) return "-";
    std::string out;
    for (uint8_t x : bytes) out += hex(x, 2);
    return out;
}

std::vector<uint8_t> bytes_of(const std::string& s) {
    std::vector<uint8_t> out;
    if (s == "-") return out;
    for (size_t i = 0; i + 1 < s.size(); i += 2) out.push_back(uint8_t(std::stoul(s.substr(i, 2), nullptr, 16)));
    return out;
}

std::vector<std::string> split(const std::string& s, char sep) {
    std::vector<std::string> out;
    size_t a = 0;
    for (size_t b; (b = s.find(sep, a)) != std::string::npos; a = b + 1) out.push_back(s.substr(a, b - a));
    out.push_back(s.substr(a));
    return out;
}

uint32_t hex32(const std::string& s) { return uint32_t(std::stoul(s, nullptr, 16)); }

std::vector<uint8_t> varint_of(uint64_t v) {
    uint8_t b[VARINT_MAX_BYTES];
    const size_t n = put_varint(b, v);
    return std::vector<uint8_t>(b, b + n);
}

// The varint at the front of bytes, when it is one of at most max_bytes that
// ends the bytes.
bool whole_varint(const std::vector<uint8_t>& bytes, size_t max_bytes, uint64_t& v) {
    size_t at = 0;
    return get_varint(bytes.data(), bytes.size(), at, max_bytes, v) && at == bytes.size();
}

}  // namespace

TEST(order_keys_order_the_floats_and_come_back) {
    const float inf = std::numeric_limits<float>::infinity();
    const float floats[] = {-inf, -FLT_MAX, -180.0f, -1.5f, -FLT_MIN, -FLT_TRUE_MIN, -0.0f,
                            0.0f, FLT_TRUE_MIN, FLT_MIN, 1.5f, 180.0f, FLT_MAX, inf};
    for (size_t k = 0; k + 1 < std::size(floats); k++) CHECK(order_key(bits_of(floats[k])) < order_key(bits_of(floats[k + 1])));
    CHECK_EQ(order_key(bits_of(-0.0f)), 0x7FFFFFFFu);
    CHECK_EQ(order_key(bits_of(0.0f)), 0x80000000u);
    // A NaN sorts beyond its sign's infinity.
    CHECK(order_key(0x7FC00000u) > order_key(bits_of(inf)));
    CHECK(order_key(0xFFC00000u) < order_key(bits_of(-inf)));
    std::mt19937_64 rng(1);
    size_t bad = 0;
    for (int t = 0; t < 1000000; t++) {
        const uint32_t x = uint32_t(rng());
        bad += from_order_key(order_key(x)) != x || order_key(from_order_key(x)) != x || bits_of(float_of(x)) != x;
    }
    CHECK_EQ(bad, size_t(0));
}

// Below 1.0 the keys are the multiples of 1e-7: each is one float's, under
// either sign, and a larger one a larger float's.
TEST(every_hybrid_magnitude_below_ten_million_is_one_floats_key_under_either_sign) {
    size_t bad = 0;
    uint32_t last = 0;
    for (uint32_t mag = 0; mag < TEN_MILLION; mag++) {
        const uint32_t bits = from_hybrid_key(mag);
        uint32_t key = ~0u;
        bad += !(bits < ONE_BITS && (mag == 0 || bits > last) && hybrid_key(bits, key) && key == mag);
        last = bits;
        if (!mag) continue;
        const uint32_t negative = 0u - mag;
        bad += !(from_hybrid_key(negative) == (bits | SIGN) && hybrid_key(bits | SIGN, key) && key == negative);
    }
    CHECK_EQ(bad, size_t(0));
    CHECK_EQ(from_hybrid_key(0), 0u);
    CHECK_EQ(from_hybrid_key(1234567), bits_of(0.1234567f));
    CHECK_EQ(from_hybrid_key(0u - 1234567), bits_of(-0.1234567f));
    CHECK_EQ(from_hybrid_key(5000000), bits_of(0.5f));
}

TEST(from_one_up_the_hybrid_key_counts_the_floats_from_ten_million) {
    CHECK_EQ(hybrid_key_of(bits_of(1.0f)), TEN_MILLION);
    CHECK_EQ(hybrid_key_of(bits_of(1.0f) + 1), TEN_MILLION + 1);
    CHECK_EQ(hybrid_key_of(bits_of(-1.0f)), 0u - TEN_MILLION);
    CHECK_EQ(hybrid_key_of(bits_of(52.37f)), bits_of(52.37f) - ONE_BITS + TEN_MILLION);
    CHECK_EQ(hybrid_key_of(bits_of(-179.99f)), 0u - (bits_of(179.99f) - ONE_BITS + TEN_MILLION));
    // The largest float below 1.0 that has a key, and the one after it, which is nearer no multiple of 1e-7.
    CHECK_EQ(hybrid_key_of(0x3F7FFFFEu), TEN_MILLION - 1);
    CHECK(!has_hybrid_key(0x3F7FFFFFu));
    std::mt19937_64 rng(2);
    size_t bad = 0;
    for (int t = 0; t < 1000000; t++) {
        const uint32_t mag = TEN_MILLION + uint32_t(rng() % (HYBRID_KEY_MAX - TEN_MILLION + 1));
        const uint32_t key = rng() % 2 ? mag : 0u - mag;
        const uint32_t bits = from_hybrid_key(key);
        uint32_t back = ~key;
        bad += !(is_hybrid_key(key) && (bits & ~SIGN) >= ONE_BITS && (bits >> 31) == (key >> 31));
        bad += !(hybrid_key(bits, back) && back == key);
    }
    CHECK_EQ(bad, size_t(0));
}

TEST(signed_zero_nan_and_the_infinities_have_the_keys_their_bits_give) {
    CHECK_EQ(hybrid_key_of(bits_of(0.0f)), 0u);
    // -0 would share +0's key.
    CHECK(!has_hybrid_key(bits_of(-0.0f)));
    const uint32_t inf = bits_of(std::numeric_limits<float>::infinity());
    CHECK_EQ(inf, 0x7F800000u);
    CHECK_EQ(hybrid_key_of(inf), 0x40000000u + TEN_MILLION);
    CHECK_EQ(hybrid_key_of(inf | SIGN), 0u - (0x40000000u + TEN_MILLION));
    for (uint32_t nan : {0x7FC00000u, 0xFFC00000u, 0x7F800001u, 0xFFBFFFFFu}) {
        CHECK(float_of(nan) != float_of(nan));
        const uint32_t key = hybrid_key_of(nan);
        CHECK(is_hybrid_key(key));
        CHECK_EQ(from_hybrid_key(key), nan);
        CHECK_EQ(from_order_key(order_key(nan)), nan);
    }
    // The largest magnitude a pattern has.
    CHECK_EQ(HYBRID_KEY_MAX, 1092130431u);
    CHECK_EQ(hybrid_key_of(0x7FFFFFFFu), HYBRID_KEY_MAX);
    CHECK_EQ(hybrid_key_of(0xFFFFFFFFu), 0u - HYBRID_KEY_MAX);
    CHECK_EQ(from_hybrid_key(HYBRID_KEY_MAX), 0x7FFFFFFFu);
    CHECK_EQ(from_hybrid_key(0u - HYBRID_KEY_MAX), 0xFFFFFFFFu);
}

TEST(a_float_below_one_that_is_no_multiple_of_1e7_has_no_hybrid_key) {
    // 0.1f is the float nearest 1,000,000 x 1e-7; its neighbours are nearest no multiple.
    CHECK_EQ(hybrid_key_of(bits_of(0.1f)), 1000000u);
    CHECK(!has_hybrid_key(bits_of(0.1f) + 1));
    CHECK(!has_hybrid_key(bits_of(0.1f) - 1));
    CHECK(!has_hybrid_key(bits_of(-0.1f) + 1));
    // Halfway between two multiples.
    CHECK(!has_hybrid_key(bits_of(1.0f / 256)));
    CHECK(!has_hybrid_key(bits_of(3.0f / 256)));
    // Floats that round to 0 and are not 0.
    CHECK(!has_hybrid_key(1));
    CHECK(!has_hybrid_key(bits_of(FLT_MIN)));
    CHECK(!has_hybrid_key(bits_of(1e-9f)));
    CHECK(!has_hybrid_key(bits_of(-1e-9f)));
    CHECK(!has_hybrid_key(bits_of(4e-8f)));
    // Below 0.5 the floats beside one with a key have none: the multiples are more than 3 floats apart.
    std::mt19937_64 rng(3);
    size_t bad = 0;
    for (int t = 0; t < 200000; t++) {
        const uint32_t bits = from_hybrid_key(1 + uint32_t(rng() % 4999998));
        bad += !has_hybrid_key(bits) || has_hybrid_key(bits + 1) || has_hybrid_key(bits - 1) || has_hybrid_key((bits + 1) | SIGN);
    }
    CHECK_EQ(bad, size_t(0));
}

TEST(a_key_past_the_largest_magnitude_is_no_hybrid_key) {
    for (uint32_t key : {0u, 1u, 0u - 1, TEN_MILLION - 1, TEN_MILLION, 0u - TEN_MILLION, HYBRID_KEY_MAX, 0u - HYBRID_KEY_MAX})
        CHECK(is_hybrid_key(key));
    const uint32_t past = HYBRID_KEY_MAX + 1;
    for (uint32_t key : {past, 0u - past, 0x7FFFFFFFu, 0x80000000u, 0x80000001u, 0x60000000u, 0xA0000000u}) {
        CHECK(!is_hybrid_key(key));
        // Whatever pattern it decodes to has another key, or none.
        uint32_t back = key;
        CHECK(!hybrid_key(from_hybrid_key(key), back) || back != key);
    }
    // No pattern's key is past it.
    std::mt19937_64 rng(4);
    size_t bad = 0;
    for (int t = 0; t < 1000000; t++) {
        uint32_t key;
        const uint32_t bits = uint32_t(rng());
        bad += hybrid_key(bits, key) && !(is_hybrid_key(key) && from_hybrid_key(key) == bits);
    }
    CHECK_EQ(bad, size_t(0));
}

TEST(a_biased_hybrid_key_sorts_unsigned_as_the_key_does_signed) {
    const uint32_t keys[] = {0u - HYBRID_KEY_MAX, 0u - TEN_MILLION, 0u - 2, 0u - 1, 0, 1, 2, TEN_MILLION, HYBRID_KEY_MAX};
    for (size_t k = 0; k + 1 < std::size(keys); k++) CHECK(biased(keys[k]) < biased(keys[k + 1]));
    for (uint32_t key : keys) CHECK_EQ(biased(biased(key)), key);
    CHECK_EQ(biased(0), 0x80000000u);
}

TEST(a_zigzag_holds_a_small_difference_of_either_sign_in_few_bits_and_comes_back) {
    CHECK_EQ(zigzag32(0), 0u);
    CHECK_EQ(zigzag32(0u - 1), 1u);
    CHECK_EQ(zigzag32(1), 2u);
    CHECK_EQ(zigzag32(0u - 2), 3u);
    CHECK_EQ(zigzag32(0x7FFFFFFFu), 0xFFFFFFFEu);
    CHECK_EQ(zigzag32(0x80000000u), 0xFFFFFFFFu);
    std::mt19937_64 rng(5);
    size_t bad = 0;
    for (int t = 0; t < 1000000; t++) {
        // A step of d up takes one bit more than d, a step of d down one more than d - 1.
        const uint32_t shift = 1 + uint32_t(rng() % 31);
        const uint32_t d = uint32_t(rng()) >> shift;
        const uint32_t any = uint32_t(rng());
        bad += unzigzag32(zigzag32(d)) != d || unzigzag32(zigzag32(0u - d)) != 0u - d;
        bad += unzigzag32(zigzag32(any)) != any || zigzag32(unzigzag32(any)) != any;
        bad += d != 0 && (bit_length(zigzag32(d)) != bit_length(d) + 1 || bit_length(zigzag32(0u - d)) != bit_length(d - 1) + 1);
    }
    CHECK_EQ(bad, size_t(0));
    // Mod 2^16: the 32-bit zigzag of the difference as a signed 16-bit number.
    CHECK_EQ(zigzag16(0xFFFF), 1);
    CHECK_EQ(zigzag16(0x7FFF), 0xFFFE);
    CHECK_EQ(zigzag16(0x8000), 0xFFFF);
    for (uint32_t d = 0; d < 0x10000; d++) {
        const uint16_t z = zigzag16(uint16_t(d));
        bad += z != zigzag32(uint32_t(int32_t(int16_t(uint16_t(d))))) || unzigzag16(z) != d;
    }
    CHECK_EQ(bad, size_t(0));
}

TEST(bit_and_byte_lengths_are_the_fewest_that_hold_a_value) {
    CHECK_EQ(bit_length(0), 0u);
    CHECK_EQ(byte_length(0), 0u);
    for (uint32_t w = 1; w <= 32; w++) {
        const uint32_t top = 1u << (w - 1), all = top | (top - 1);
        CHECK_EQ(bit_length(top), w);
        CHECK_EQ(bit_length(all), w);
        CHECK_EQ(byte_length(top), (w + 7) / 8);
        CHECK_EQ(byte_length(all), (w + 7) / 8);
    }
}

TEST(bit_fields_pack_lsb_first_and_read_back_with_one_load) {
    CHECK(packed({{1, 1}}) == std::vector<uint8_t>{0x01});
    CHECK(packed({{0, 3}, {1, 1}, {0x1F, 5}}) == (std::vector<uint8_t>{0xF8, 0x01}));
    CHECK(packed({{0xFFFFFFFFu, 32}, {0, 7}, {1, 1}}) == (std::vector<uint8_t>{0xFF, 0xFF, 0xFF, 0xFF, 0x80}));
    CHECK(packed({}).empty());
    CHECK(packed({{0, 0}, {0, 0}}).empty());
    std::mt19937_64 rng(6);
    for (int t = 0; t < 3000; t++) {
        Fields fields;
        const size_t n = rng() % 80;
        for (size_t k = 0; k < n; k++) {
            const uint32_t w = t % 7 == 0 ? 32 : uint32_t(rng() % 33);
            const uint32_t v = uint32_t(rng());
            fields.push_back({w == 32 ? v : v & ((1u << w) - 1), w});
        }
        const std::vector<uint8_t> want = packed_bit_by_bit(fields);
        const std::vector<uint8_t> got = packed(fields);
        CHECK(got == want);
        // Read in place where 8 bytes follow each field's first, and from the bare bytes.
        std::vector<uint8_t> roomy = want;
        roomy.resize(want.size() + 8, 0xFF);
        uint64_t bit = 0;
        size_t bad = 0;
        for (const auto& [v, w] : fields) {
            bad += get_bits(roomy.data(), bit, w) != v || get_bits_bounded(want.data(), want.size(), bit, w) != v;
            bit += w;
        }
        CHECK_EQ(bad, size_t(0));
        CHECK(padding_is_zero(want.data(), bit));
        if (bit % 8) {
            std::vector<uint8_t> dirty = want;
            dirty.back() |= uint8_t(1u << (bit % 8 + rng() % (8 - bit % 8)));
            CHECK(!padding_is_zero(dirty.data(), bit));
        }
    }
    // Past the bytes a bounded read sees zero bits.
    const uint8_t two[2] = {0xFF, 0xFF};
    CHECK_EQ(get_bits_bounded(two, 2, 0, 32), 0xFFFFu);
    CHECK_EQ(get_bits_bounded(two, 2, 12, 9), 0xFu);
    CHECK_EQ(get_bits_bounded(two, 2, 16, 32), 0u);
    CHECK_EQ(get_bits_bounded(two, 2, 4000, 5), 0u);
    CHECK_EQ(get_bits_bounded(nullptr, 0, 0, 0), 0u);
}

TEST(a_varint_is_read_only_in_its_fewest_bytes) {
    CHECK(varint_of(0) == std::vector<uint8_t>{0x00});
    CHECK(varint_of(127) == std::vector<uint8_t>{0x7F});
    CHECK(varint_of(128) == (std::vector<uint8_t>{0x80, 0x01}));
    CHECK(varint_of(65535) == (std::vector<uint8_t>{0xFF, 0xFF, 0x03}));
    CHECK_EQ(varint_of(UINT64_MAX).size(), VARINT_MAX_BYTES);
    std::mt19937_64 rng(7);
    for (int t = 0; t < 200000; t++) {
        const uint64_t shift = rng() % 64;
        const uint64_t v = t < 64 ? uint64_t(1) << t : t < 128 ? (uint64_t(1) << (t - 64)) - 1 : rng() >> shift;
        std::vector<uint8_t> b = varint_of(v);
        const size_t n = b.size();
        size_t bad = n != varint_size(v);
        uint64_t got = ~v;
        bad += !whole_varint(b, VARINT_MAX_BYTES, got) || got != v;
        bad += !whole_varint(b, n, got) || got != v;
        // More bytes than the reader allows, a cut one, and one spelled with a byte of zero more.
        bad += n > 1 && whole_varint(b, n - 1, got);
        size_t at = 0;
        bad += get_varint(b.data(), n - 1, at, VARINT_MAX_BYTES, got);
        b.back() |= 0x80;
        b.push_back(0);
        bad += whole_varint(b, VARINT_MAX_BYTES + 1, got);
        // It is read from where it starts and the bytes after it are left.
        std::vector<uint8_t> among = {0xFF, 0x80};
        const std::vector<uint8_t> again = varint_of(v);
        among.insert(among.end(), again.begin(), again.end());
        among.push_back(0x80);
        at = 2;
        bad += !get_varint(among.data(), among.size(), at, VARINT_MAX_BYTES, got) || got != v || at != 2 + n;
        CHECK_EQ(bad, size_t(0));
    }
    uint64_t v;
    // 2^64 - 1 fills ten bytes; one bit more does not fit.
    CHECK(whole_varint({0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}, VARINT_MAX_BYTES, v) && v == UINT64_MAX);
    CHECK(!whole_varint({0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02}, VARINT_MAX_BYTES, v));
    CHECK(!whole_varint({0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}, 11, v));
    CHECK(!whole_varint({}, VARINT_MAX_BYTES, v));
    CHECK(!whole_varint({0x80}, VARINT_MAX_BYTES, v));
}

namespace {

bool has_dec4_key(uint32_t bits) {
    int32_t key;
    return dec4_key(bits, key);
}

int32_t dec4_key_of(uint32_t bits) {
    int32_t key = 0;
    REQUIRE(dec4_key(bits, key));
    return key;
}

using Codes = std::vector<std::pair<uint32_t, uint32_t>>;  // z, k

// The codes a bit at a time, LSB first: q zero bits, a one bit and k bits
// of z, or RICE_ESCAPE zero bits and z in 32 bits.
std::vector<uint8_t> rice_bit_by_bit(const Codes& codes) {
    Fields fields;
    for (const auto& [z, k] : codes) {
        const uint32_t q = z >> k;
        if (q >= RICE_ESCAPE) {
            for (uint32_t i = 0; i < RICE_ESCAPE; i++) fields.push_back({0, 1});
            fields.push_back({z, 32});
            continue;
        }
        for (uint32_t i = 0; i < q; i++) fields.push_back({0, 1});
        fields.push_back({1, 1});
        fields.push_back({k ? z & ((1u << k) - 1) : 0, k});
    }
    return packed_bit_by_bit(fields);
}

// The codes through put_rice, in a buffer with the 8 spare bytes it asks for.
std::vector<uint8_t> rice_packed(const Codes& codes) {
    uint64_t bits = 0;
    for (const auto& [z, k] : codes) bits += rice_bits(z, k);
    std::vector<uint8_t> buf((bits + 7) / 8 + 8, 0);
    uint64_t bit = 0;
    for (const auto& [z, k] : codes) put_rice(buf.data(), bit, z, k);
    CHECK_EQ(bit, bits);
    buf.resize(buf.size() - 8);
    return buf;
}

// The least k of the fewest bits, each k tried.
uint32_t best_rice_k_by_trial(const std::vector<uint32_t>& z) {
    uint64_t best = UINT64_MAX;
    uint32_t best_k = 0;
    for (uint32_t k = 0; k < 32; k++) {
        uint64_t total = 0;
        for (uint32_t v : z) total += rice_bits(v, k);
        if (total < best) best = total, best_k = k;
    }
    return best_k;
}

}  // namespace

// A coordinate of four decimals is its digits: each multiple of 1e-4 up to
// 180 in magnitude is one float's key, and that float's round.
TEST(every_four_decimal_coordinate_to_180_is_one_floats_decimal_key) {
    size_t bad = 0;
    uint32_t last = from_dec4_key(-1800001);
    for (int32_t key = -1800000; key <= 1800000; key++) {
        const uint32_t bits = from_dec4_key(key);
        int32_t back = ~key;
        bad += !(dec4_key(bits, back) && back == key && dec4_round(bits) == key);
        // Ascending as floats, so a step between neighbours is a small number.
        bad += !(order_key(bits) > order_key(last));
        last = bits;
    }
    CHECK_EQ(bad, size_t(0));
    CHECK_EQ(dec4_key_of(bits_of(52.3702f)), 523702);
    CHECK_EQ(dec4_key_of(bits_of(-0.1278f)), -1278);
    CHECK_EQ(dec4_key_of(bits_of(180.0f)), 1800000);
    CHECK_EQ(dec4_key_of(bits_of(-180.0f)), -1800000);
    CHECK_EQ(dec4_key_of(bits_of(1e-4f)), 1);
    CHECK_EQ(dec4_key_of(bits_of(-1e-4f)), -1);
    CHECK_EQ(dec4_key_of(bits_of(0.0f)), 0);
}

TEST(a_float_that_is_no_four_decimal_coordinate_has_no_decimal_key) {
    // A coordinate's neighbours one float off, -0 (its key would be +0's),
    // the non-finite floats and a float nearer no multiple of 1e-4.
    for (float v : {52.3702f, -0.1278f, 1e-4f, 180.0f}) {
        CHECK(!has_dec4_key(bits_of(v) + 1));
        CHECK(!has_dec4_key(bits_of(v) - 1));
    }
    CHECK(!has_dec4_key(bits_of(-0.0f)));
    CHECK_EQ(dec4_round(bits_of(-0.0f)), 0);
    CHECK(!has_dec4_key(bits_of(std::numeric_limits<float>::infinity())));
    CHECK(!has_dec4_key(bits_of(-std::numeric_limits<float>::infinity())));
    CHECK(!has_dec4_key(0x7FC00000u));
    CHECK(!has_dec4_key(bits_of(0.00005f)));
    CHECK(!has_dec4_key(bits_of(52.37025f)));
    // Halves of 1e-4 round to even: 0.03125 is 312.5 units, 0.09375 937.5.
    CHECK(!has_dec4_key(bits_of(0.03125f)));
    CHECK_EQ(dec4_round(bits_of(0.03125f)), 312);
    CHECK_EQ(dec4_round(bits_of(-0.03125f)), -312);
    CHECK_EQ(dec4_round(bits_of(0.09375f)), 938);
    // Past 2^31 units there is no key, and the round saturates; a NaN's is 0.
    CHECK(has_dec4_key(bits_of(214748.359375f)));
    CHECK_EQ(dec4_key_of(bits_of(214748.359375f)), 2147483594);
    CHECK(!has_dec4_key(bits_of(214748.375f)));
    CHECK(!has_dec4_key(bits_of(-214748.375f)));
    CHECK_EQ(dec4_round(bits_of(214748.375f)), INT32_MAX);
    CHECK_EQ(dec4_round(bits_of(-1e30f)), INT32_MIN);
    CHECK_EQ(dec4_round(bits_of(std::numeric_limits<float>::infinity())), INT32_MAX);
    CHECK_EQ(dec4_round(0x7FC00000u), 0);
    CHECK_EQ(dec4_round(0xFFC00000u), 0);
    // Wherever a pattern has a key, the key is its round.
    std::mt19937_64 rng(8);
    size_t bad = 0, keys = 0;
    for (int t = 0; t < 1000000; t++) {
        const uint32_t bits = uint32_t(rng());
        int32_t key;
        if (!dec4_key(bits, key)) continue;
        keys++;
        bad += key != dec4_round(bits) || from_dec4_key(key) != bits;
    }
    CHECK_EQ(bad, size_t(0));
    CHECK(keys > 0);
}

TEST(a_rice_code_is_q_zero_bits_a_one_and_k_bits_or_an_escape) {
    CHECK_EQ(rice_bits(0, 0), 1u);
    CHECK_EQ(rice_bits(11, 0), 12u);
    CHECK_EQ(rice_bits(12, 0), RICE_ESCAPE + 32);
    CHECK_EQ(rice_bits(11u << 5 | 31, 5), 11u + 1 + 5);
    CHECK_EQ(rice_bits(12u << 5, 5), RICE_ESCAPE + 32);
    CHECK_EQ(rice_bits(0xFFFFFFFFu, 31), 1u + 1 + 31);
    CHECK_EQ(rice_bits(0xFFFFFFFFu, 0), RICE_ESCAPE + 32);
    CHECK(rice_packed({{0, 0}}) == std::vector<uint8_t>{0x01});
    CHECK(rice_packed({{3, 0}}) == std::vector<uint8_t>{0x08});
    CHECK(rice_packed({{5, 1}}) == std::vector<uint8_t>{0x0C});
    // An escape: 12 zero bits, then z.
    CHECK(rice_packed({{12, 0}}) == (std::vector<uint8_t>{0x00, 0xC0, 0x00, 0x00, 0x00, 0x00}));
    std::mt19937_64 rng(9);
    for (int t = 0; t < 3000; t++) {
        Codes codes;
        const size_t n = rng() % 80;
        for (size_t i = 0; i < n; i++) {
            const uint32_t k = uint32_t(rng() % 32);
            const uint32_t shift = uint32_t(rng() % 33);
            const uint32_t z = shift == 32 ? 0 : uint32_t(rng()) >> shift;
            codes.push_back({z, k});
        }
        const std::vector<uint8_t> want = rice_bit_by_bit(codes);
        CHECK(rice_packed(codes) == want);
        std::vector<uint8_t> roomy = want;
        roomy.resize(want.size() + 8, 0xFF);
        uint64_t bit = 0;
        size_t bad = 0;
        for (const auto& [z, k] : codes) {
            const uint64_t at = bit;
            bad += get_rice(roomy.data(), bit, k) != z || bit - at != rice_bits(z, k);
        }
        CHECK_EQ(bad, size_t(0));
    }
}

TEST(a_rice_escape_at_the_end_of_a_stream_reads_its_32_bits_from_the_spare_bytes) {
    // Three bits of a code, then an escape whose z ends the stream: its load
    // starts at the stream's first byte and takes 47 bits.
    const Codes codes = {{2, 1}, {0xFEDCBA98u, 3}};
    std::vector<uint8_t> bytes = rice_packed(codes);
    REQUIRE(bytes.size() == 6);
    bytes.resize(bytes.size() + 8, 0);
    uint64_t bit = 0;
    CHECK_EQ(get_rice(bytes.data(), bit, 1), 2u);
    CHECK_EQ(get_rice(bytes.data(), bit, 3), 0xFEDCBA98u);
    CHECK_EQ(bit, uint64_t(3 + RICE_ESCAPE + 32));
    // An escape from bit 7 of a byte, after seven bits of something else: one
    // load at that byte holds its 44 bits.
    std::vector<uint8_t> late(7 + 8, 0);
    late[0] = 0x7F;
    uint64_t at = 7;
    put_rice(late.data(), at, 0xFFFFFFFFu, 0);
    uint64_t from = 7;
    CHECK_EQ(get_rice(late.data(), from, 0), 0xFFFFFFFFu);
    CHECK_EQ(from, at);
}

TEST(the_best_rice_parameter_is_the_least_of_the_fewest_bits) {
    CHECK_EQ(best_rice_k(nullptr, 0), 0u);
    const uint32_t zeros[] = {0, 0, 0};
    CHECK_EQ(best_rice_k(zeros, 3), 0u);
    const uint32_t big[] = {0xFFFFFFFFu};
    CHECK_EQ(best_rice_k(big, 1), 31u);
    // 12 at k = 0 is an escape (44 bits); k = 2 codes it in 6, k = 3 in 5.
    const uint32_t twelve[] = {12};
    CHECK_EQ(best_rice_k(twelve, 1), 3u);
    // {2, 3} take 6 bits at k = 1 and at k = 2: the least k.
    const uint32_t tie[] = {2, 3};
    CHECK_EQ(rice_bits(2, 1) + rice_bits(3, 1), rice_bits(2, 2) + rice_bits(3, 2));
    CHECK_EQ(best_rice_k(tie, 2), 1u);
    std::mt19937_64 rng(10);
    size_t bad = 0;
    for (int t = 0; t < 20000; t++) {
        std::vector<uint32_t> z(rng() % 64);
        const uint32_t typical = uint32_t(rng() % 33);
        for (uint32_t& v : z) {
            const uint32_t w = rng() % 8 == 0 ? uint32_t(rng() % 33) : typical;
            v = w == 0 ? 0 : uint32_t(rng()) >> (32 - w);
        }
        bad += best_rice_k(z.data(), z.size()) != best_rice_k_by_trial(z);
    }
    CHECK_EQ(bad, size_t(0));
}

TEST(coord_keys_are_the_shared_vectors_byte_for_byte) {
    // geocoder/testdata/coord_keys.tsv, which the server's functions are checked against too.
    std::ifstream in(std::string(GEOCODER_TESTDATA_DIR) + "/coord_keys.tsv");
    REQUIRE(in.good());
    std::map<std::string, size_t> rows;
    bool header = true;
    for (std::string line; std::getline(in, line);) {
        if (line.empty() || line[0] == '#') continue;
        if (header) {
            header = false;
            continue;
        }
        const std::vector<std::string> field = split(line, '\t');
        REQUIRE(field.size() == 3);
        const std::string &kind = field[0], &given = field[1], &want = field[2];
        rows[kind]++;
        if (kind == "key") {
            const uint32_t bits = hex32(given);
            uint32_t key = 0;
            const bool has = hybrid_key(bits, key);
            CHECK_EQ(hex(order_key(bits)) + "," + (has ? hex(key) : "-"), want);
            const std::vector<std::string> keys = split(want, ',');
            REQUIRE(keys.size() == 2);
            CHECK_EQ(from_order_key(hex32(keys[0])), bits);
            if (keys[1] != "-") CHECK_EQ(from_hybrid_key(hex32(keys[1])), bits);
        } else if (kind == "hybrid") {
            const uint32_t key = hex32(given);
            CHECK_EQ(is_hybrid_key(key) ? hex(from_hybrid_key(key)) : "-", want);
            if (want != "-") CHECK_EQ(hybrid_key_of(hex32(want)), key);
        } else if (kind == "zigzag32") {
            CHECK_EQ(hex(zigzag32(hex32(given))), want);
            CHECK_EQ(unzigzag32(hex32(want)), hex32(given));
        } else if (kind == "zigzag16") {
            CHECK_EQ(hex(zigzag16(uint16_t(hex32(given))), 4), want);
            CHECK_EQ(unzigzag16(uint16_t(hex32(want))), hex32(given));
        } else if (kind == "length") {
            const uint32_t v = hex32(given);
            CHECK_EQ(std::to_string(bit_length(v)) + "," + std::to_string(byte_length(v)), want);
        } else if (kind == "bits") {
            Fields fields;
            if (given != "-")
                for (const std::string& f : split(given, ',')) {
                    const std::vector<std::string> vw = split(f, ':');
                    REQUIRE(vw.size() == 2);
                    fields.push_back({hex32(vw[0]), uint32_t(std::stoul(vw[1]))});
                }
            CHECK_EQ(hex(packed(fields)), want);
            const std::vector<uint8_t> bytes = bytes_of(want);
            uint64_t bit = 0;
            for (const auto& [v, w] : fields) {
                CHECK_EQ(get_bits_bounded(bytes.data(), bytes.size(), bit, w), v);
                bit += w;
            }
            CHECK(padding_is_zero(bytes.data(), bit));
        } else if (kind == "varint") {
            const uint64_t v = std::stoull(given);
            CHECK_EQ(hex(varint_of(v)), want);
            uint64_t got = ~v;
            CHECK(whole_varint(bytes_of(want), VARINT_MAX_BYTES, got) && got == v);
        } else if (kind == "novarint") {
            // No varint of at most `want` bytes starts the bytes.
            const std::vector<uint8_t> bytes = bytes_of(given);
            size_t at = 0;
            uint64_t got;
            CHECK(!get_varint(bytes.data(), bytes.size(), at, std::stoul(want), got));
        } else if (kind == "dec4") {
            const uint32_t bits = hex32(given);
            int32_t key = 0;
            const bool has = dec4_key(bits, key);
            CHECK_EQ((has ? std::to_string(key) : "-") + "," + std::to_string(dec4_round(bits)), want);
            const std::vector<std::string> keys = split(want, ',');
            REQUIRE(keys.size() == 2);
            if (keys[0] != "-") CHECK_EQ(from_dec4_key(int32_t(std::stol(keys[0]))), bits);
        } else if (kind == "undec4") {
            // A key of any i32 decodes to some pattern.
            CHECK_EQ(hex(from_dec4_key(int32_t(std::stol(given)))), want);
        } else if (kind == "rice") {
            Codes codes;
            for (const std::string& c : split(given, ',')) {
                const std::vector<std::string> zk = split(c, ':');
                REQUIRE(zk.size() == 2);
                codes.push_back({hex32(zk[0]), uint32_t(std::stoul(zk[1]))});
            }
            CHECK_EQ(hex(rice_packed(codes)), want);
            std::vector<uint8_t> bytes = bytes_of(want);
            bytes.resize(bytes.size() + 8, 0);
            uint64_t bit = 0;
            for (const auto& [z, k] : codes) CHECK_EQ(get_rice(bytes.data(), bit, k), z);
            CHECK(padding_is_zero(bytes.data(), bit));
        } else if (kind == "ricek") {
            std::vector<uint32_t> z;
            if (given != "-")
                for (const std::string& v : split(given, ',')) z.push_back(hex32(v));
            const uint32_t k = best_rice_k(z.data(), z.size());
            uint64_t total = 0;
            for (uint32_t v : z) total += rice_bits(v, k);
            CHECK_EQ(std::to_string(k) + "," + std::to_string(total), want);
        } else {
            CHECK(kind == "a kind this test reads");
        }
    }
    CHECK(rows["key"] >= 150);
    CHECK(rows["hybrid"] >= 30);
    CHECK(rows["zigzag32"] >= 16);
    CHECK(rows["zigzag16"] >= 12);
    CHECK(rows["length"] >= 16);
    CHECK(rows["bits"] >= 16);
    CHECK(rows["varint"] >= 20);
    CHECK(rows["novarint"] >= 8);
    CHECK(rows["dec4"] >= 60);
    CHECK(rows["undec4"] >= 8);
    CHECK(rows["rice"] >= 16);
    CHECK(rows["ricek"] >= 12);
}
