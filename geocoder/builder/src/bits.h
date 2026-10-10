// Bit counting for the rank structures (node_index.h, StringStarts).
#pragma once

#include <cstdint>

// The set bits of x. __builtin_popcountll is a libgcc call on x86-64 targets
// without POPCNT, the compiler's default; this count inlines.
inline uint32_t popcount64(uint64_t x) {
#if defined(__x86_64__) && !defined(__POPCNT__)
    x -= (x >> 1) & 0x5555555555555555ull;
    x = (x & 0x3333333333333333ull) + ((x >> 2) & 0x3333333333333333ull);
    x = (x + (x >> 4)) & 0x0F0F0F0F0F0F0F0Full;
    return static_cast<uint32_t>((x * 0x0101010101010101ull) >> 56);
#else
    return static_cast<uint32_t>(__builtin_popcountll(x));
#endif
}
