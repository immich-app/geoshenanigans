// The keys an f32 coordinate is stored under, and the bit fields, zigzags and
// varints record files are packed from. The server reads with the same
// functions (server/src/coord_keys.rs), and geocoder/testdata/coord_keys.tsv
// holds values both must give.
//
// Order key: an f32 bit pattern's place in the floats' order (-0 before +0,
//   a NaN beyond its sign's infinity). A bijection on 32 bits.
// Hybrid key: a signed number, two's complement in a u32. Below 1.0 in
//   magnitude it is the float in units of 1e-7, rounded, so a coordinate of
//   seven decimals is its digits and the steps between such coordinates are
//   small numbers; from 1.0 up it counts the floats from 1.0, plus
//   10,000,000; either under the float's sign. A pattern has a hybrid key
//   only when the key decodes back to it: below 1.0 that is the one float
//   nearest each multiple of 1e-7 (the multiples are 1e-7 apart and the
//   floats there at most 6e-8, so no float is the nearest to two), and never
//   -0, whose key would be +0's. Every key of a magnitude up to
//   HYBRID_KEY_MAX is one pattern's, and no other is any pattern's.
// Decimal key: a signed number (i32), the float in units of 1e-4 rounded to
//   an integer, half to even: a coordinate of four decimals is its digits.
//   A pattern has a decimal key only when the key is below 2^31 in magnitude
//   and decodes back to it: up to 180 in magnitude that is the one float
//   nearest each multiple of 1e-4 (the floats there at most 2^-16 apart), and
//   never -0, a NaN or an infinity. A pattern's decimal round is that same
//   rounding for any pattern, saturated to i32 (0 for a NaN): a pattern's
//   decimal key, where it has one, is its round.
// zigzag: a difference mod 2^32 (or 2^16) with its sign moved to bit 0, so a
//   small step either way has few bits.
// Rice code of z (u32) with parameter k (0 to 31): q = z >> k; below
//   RICE_ESCAPE, q zero bits, a one bit, then the k low bits of z; from
//   RICE_ESCAPE up, RICE_ESCAPE zero bits, then z in 32 bits. As bit fields,
//   LSB first: a code is at most 44 bits, so one 8-byte load at its first byte
//   holds it.
// Bit fields: at most 32 bits each, LSB first, back to back; a stream's last
//   byte is filled with zero bits. A field is read with one 8-byte load at
//   its first byte and packed with one load and store there (32 bits shifted
//   by at most 7 fit 64), so a packed region is followed by 8 bytes that can
//   be read, and a buffer being packed has 8 to spare.
// Varints: LEB128, in their fewest bytes.
//
// Floats. The hybrid and decimal keys are one multiply and one rounding to an
// integer, their inverses one divide and one narrowing to f32, each rounded once, to nearest
// even (the default rounding mode, which nothing here changes): the same
// bytes on every target the asserts below let through, which is IEEE-754
// binary32 and binary64, each evaluated in its own precision, so no x87, and
// no -ffast-math. A compiler may fuse a multiply into the add or subtract it
// feeds, which rounds once where IEEE rounds twice. So no product here, and
// none in a format built on these keys, feeds a sum or a difference, and the
// bytes do not depend on -ffp-contract.
#pragma once

#include <algorithm>
#include <cfloat>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>

namespace coord_keys {

static_assert(std::numeric_limits<float>::is_iec559 && sizeof(float) == 4, "f32 must be IEEE-754 binary32");
static_assert(std::numeric_limits<double>::is_iec559 && sizeof(double) == 8, "f64 must be IEEE-754 binary64");
static_assert(std::numeric_limits<float>::round_style == std::round_to_nearest &&
                  std::numeric_limits<double>::round_style == std::round_to_nearest,
              "floats must round to nearest");
static_assert(FLT_EVAL_METHOD == 0, "floats must be evaluated in their own precision: an x87-only build rounds twice");
#if defined(__FAST_MATH__) || (defined(__FINITE_MATH_ONLY__) && __FINITE_MATH_ONLY__)
#error "coord_keys.h: -ffast-math and -ffinite-math-only change the keys"
#endif
static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__, "bit fields are loaded as little-endian words");

inline uint32_t bits_of(float f) {
    uint32_t b;
    std::memcpy(&b, &f, 4);
    return b;
}
inline float float_of(uint32_t bits) {
    float f;
    std::memcpy(&f, &bits, 4);
    return f;
}

// ------------------------------------------------------------------ keys
constexpr uint32_t order_key(uint32_t bits) { return bits & 0x80000000u ? ~bits : bits | 0x80000000u; }
constexpr uint32_t from_order_key(uint32_t key) { return key & 0x80000000u ? key & 0x7FFFFFFFu : ~key; }

// 1.0f's bits, and the hybrid key's magnitude there.
inline constexpr uint32_t ONE_BITS = 0x3F800000u, TEN_MILLION = 10000000u;
// The largest magnitude of a hybrid key: 0x7FFFFFFF's, 1,092,130,431.
inline constexpr uint32_t HYBRID_KEY_MAX = 0x7FFFFFFFu - ONE_BITS + TEN_MILLION;

// Whether some pattern has this hybrid key.
constexpr bool is_hybrid_key(uint32_t key) { return (key >> 31 ? 0u - key : key) <= HYBRID_KEY_MAX; }

// The pattern of a hybrid key. Any 32 bits may come in: what is no key
// (is_hybrid_key) gives some pattern that has another key or none.
inline uint32_t from_hybrid_key(uint32_t key) {
    const uint32_t sign = key & 0x80000000u, mag = sign ? 0u - key : key;
    if (mag >= TEN_MILLION) return (mag - TEN_MILLION + ONE_BITS) | sign;
    return bits_of(float(double(mag) / 1e7)) | sign;
}

// The hybrid key of a bit pattern, to key; false, and key not to be used,
// when it has none.
inline bool hybrid_key(uint32_t bits, uint32_t& key) {
    const uint32_t sign = bits & 0x80000000u, a = bits ^ sign;
    uint32_t mag;
    if (a >= ONE_BITS) {
        mag = a - ONE_BITS + TEN_MILLION;
    } else {
        const double n = std::nearbyint(double(float_of(a)) * 1e7);
        if (n >= double(TEN_MILLION)) return false;
        mag = uint32_t(n);
    }
    if (sign && mag == 0) return false;
    key = sign ? 0u - mag : mag;
    return from_hybrid_key(key) == bits;
}

// A hybrid key, a signed number, as the unsigned number that sorts the same
// way; and back, by the same flip.
constexpr uint32_t biased(uint32_t key) { return key ^ 0x80000000u; }

// The units of a decimal key: 1e-4.
inline constexpr double DEC4_SCALE = 1e4;

// The pattern of a decimal key. Any i32 may come in: what is no key gives
// some pattern that has another key or none.
inline uint32_t from_dec4_key(int32_t key) { return bits_of(float(double(key) / DEC4_SCALE)); }

// A pattern's decimal round: its float in units of 1e-4, rounded half to
// even, saturated to i32; 0 for a NaN.
inline int32_t dec4_round(uint32_t bits) {
    const double n = std::nearbyint(double(float_of(bits)) * DEC4_SCALE);
    if (n != n) return 0;
    if (n <= -2147483648.0) return INT32_MIN;
    if (n >= 2147483647.0) return INT32_MAX;
    return int32_t(n);
}

// The decimal key of a bit pattern, to key; false, and key not to be used,
// when it has none.
inline bool dec4_key(uint32_t bits, int32_t& key) {
    const double n = std::nearbyint(double(float_of(bits)) * DEC4_SCALE);
    if (!(std::fabs(n) < 2147483648.0)) return false;
    key = int32_t(n);
    return from_dec4_key(key) == bits;
}

// ------------------------------------------------------------------ zigzags and lengths
// A difference mod 2^32, or mod 2^16, as its zigzag, and back.
constexpr uint32_t zigzag32(uint32_t d) { return (d << 1) ^ (0u - (d >> 31)); }
constexpr uint32_t unzigzag32(uint32_t z) { return (z >> 1) ^ (0u - (z & 1)); }
constexpr uint16_t zigzag16(uint16_t d) { return uint16_t((uint32_t(d) << 1) ^ (0u - (uint32_t(d) >> 15))); }
constexpr uint16_t unzigzag16(uint16_t z) { return uint16_t((uint32_t(z) >> 1) ^ (0u - (uint32_t(z) & 1))); }

// The bits, and the bytes, of v without its leading zeros: 0 for 0.
constexpr uint32_t bit_length(uint32_t v) { return v ? 32 - uint32_t(__builtin_clz(v)) : 0; }
constexpr uint32_t byte_length(uint32_t v) { return (bit_length(v) + 7) / 8; }

// ------------------------------------------------------------------ bit fields
// Puts v, of w bits (at most 32; v below 2^w), at bit `bit` of p. The bytes
// from the field's first on hold zero bits where no field is yet, and p has 8
// bytes from that byte.
inline void put_bits(uint8_t* p, uint64_t bit, uint32_t v, uint32_t w) {
    if (!w) return;
    uint8_t* at = p + size_t(bit >> 3);
    uint64_t word;
    std::memcpy(&word, at, 8);
    word |= uint64_t(v) << (bit & 7);
    std::memcpy(at, &word, 8);
}

// The w bits (at most 32) at bit `bit` of p, which has 8 bytes from the
// field's first byte.
inline uint32_t get_bits(const uint8_t* p, uint64_t bit, uint32_t w) {
    if (!w) return 0;
    uint64_t word;
    std::memcpy(&word, p + size_t(bit >> 3), 8);
    return uint32_t((word >> (bit & 7)) & ((uint64_t(1) << w) - 1));
}

// The same of a p that holds `size` bytes and nothing after them: bits past
// them read as zero.
inline uint32_t get_bits_bounded(const uint8_t* p, size_t size, uint64_t bit, uint32_t w) {
    const uint64_t at = bit >> 3;
    if (!w || at >= size) return 0;
    uint64_t word = 0;
    std::memcpy(&word, p + size_t(at), std::min<size_t>(8, size - size_t(at)));
    return uint32_t((word >> (bit & 7)) & ((uint64_t(1) << w) - 1));
}

// Whether a stream of `bits` bits at p ends in zero bits to the byte.
inline bool padding_is_zero(const uint8_t* p, uint64_t bits) {
    return bits % 8 == 0 || (p[size_t(bits >> 3)] >> (bits % 8)) == 0;
}

// ------------------------------------------------------------------ Rice codes
inline constexpr uint32_t RICE_ESCAPE = 12, RICE_MAX_BITS = RICE_ESCAPE + 32;

// The bits of z's code with parameter k (at most 31).
constexpr uint32_t rice_bits(uint32_t z, uint32_t k) {
    const uint32_t q = z >> k;
    return q < RICE_ESCAPE ? q + 1 + k : RICE_MAX_BITS;
}

// Puts z's code with parameter k (at most 31) at bit `bit` of p, moving bit
// past it, as put_bits puts a field: zero bits from the code's first byte on,
// and 8 bytes to spare.
inline void put_rice(uint8_t* p, uint64_t& bit, uint32_t z, uint32_t k) {
    const uint32_t q = z >> k;
    if (q >= RICE_ESCAPE) {
        put_bits(p, bit + RICE_ESCAPE, z, 32);
        bit += RICE_MAX_BITS;
        return;
    }
    // The one bit, then z's k low bits: k + 1 bits, at most 32.
    put_bits(p, bit + q, uint32_t(1u | (uint64_t(z) << 1 & ((uint64_t(2) << k) - 1))), k + 1);
    bit += q + 1 + k;
}

// The code with parameter k (at most 31) at bit `bit` of p, which has 8 bytes
// from the code's first byte; bit moves past it. Bits that are no z's code
// (a q past 32 - k bits) give some z whose code is other bits.
inline uint32_t get_rice(const uint8_t* p, uint64_t& bit, uint32_t k) {
    uint64_t word;
    std::memcpy(&word, p + size_t(bit >> 3), 8);
    word >>= bit & 7;
    const uint32_t low = uint32_t(word) & ((1u << RICE_ESCAPE) - 1);
    if (!low) {
        bit += RICE_MAX_BITS;
        return uint32_t(word >> RICE_ESCAPE);
    }
    const uint32_t q = uint32_t(__builtin_ctz(low));
    bit += q + 1 + k;
    return q << k | uint32_t((word >> (q + 1)) & ((uint64_t(1) << k) - 1));
}

// The least k (0 to 31) whose codes of z[0, n) take the fewest bits, 0 for no
// z. A z of bit length w codes in 1 + k bits for k from w up, and escapes for
// k up to w - 5; only for the four k between does its code depend on more than
// its length.
inline uint32_t best_rice_k(const uint32_t* z, size_t n) {
    uint64_t length[33] = {}, between[32] = {};
    for (size_t i = 0; i < n; i++) {
        const uint32_t w = bit_length(z[i]);
        length[w]++;
        for (uint32_t k = w > 4 ? w - 4 : 0; k < w; k++) between[k] += rice_bits(z[i], k);
    }
    uint64_t upto[33], sum = 0;  // the z of bit length at most w
    for (uint32_t w = 0; w <= 32; w++) upto[w] = sum += length[w];
    uint64_t best = UINT64_MAX;
    uint32_t best_k = 0;
    for (uint32_t k = 0; k < 32; k++) {
        const uint64_t escapes = n - upto[std::min(k + 4, 32u)];
        const uint64_t bits = upto[k] * (1 + k) + between[k] + escapes * RICE_MAX_BITS;
        if (bits < best) best = bits, best_k = k;
    }
    return best_k;
}

// ------------------------------------------------------------------ varints
inline constexpr size_t VARINT_MAX_BYTES = 10;

// The bytes of v's varint.
constexpr size_t varint_size(uint64_t v) {
    size_t n = 1;
    for (; v >= 0x80; v >>= 7) n++;
    return n;
}

// v's varint at out, which has room for it (VARINT_MAX_BYTES at most); the
// bytes written.
inline size_t put_varint(uint8_t* out, uint64_t v) {
    size_t n = 0;
    for (; v >= 0x80; v >>= 7) out[n++] = uint8_t(v) | 0x80;
    out[n++] = uint8_t(v);
    return n;
}

// The varint at p[at, size) to v, moving at past it. False, and at and v not
// to be used, for one that is cut short, longer than max_bytes, past 64 bits
// or not in its fewest bytes.
inline bool get_varint(const uint8_t* p, size_t size, size_t& at, size_t max_bytes, uint64_t& v) {
    v = 0;
    for (size_t i = 0; i < max_bytes && i < VARINT_MAX_BYTES && at < size; i++) {
        const uint8_t c = p[at++];
        if (i == VARINT_MAX_BYTES - 1 && c > 1) return false;
        v |= uint64_t(c & 0x7F) << (7 * i);
        if (!(c & 0x80)) return i == 0 || c != 0;
    }
    return false;
}

}  // namespace coord_keys
