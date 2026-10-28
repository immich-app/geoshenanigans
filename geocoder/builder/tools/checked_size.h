// The byte size of a unit an input file sizes by a count (a footprint, a polygon block), which a
// malformed file can make any size: head + count * item, computed in 64 bits and checked against what
// the input has left and against the size type before anything is read or allocated. In size_t, a
// 32-bit build wraps 10 + 4 * 2^30 to 10, so a ten-byte unit would pass for a billion vertices.
// And a u32 id an input file gives as a delta from another, checked against the u32 range.
// geocoder-gc's codecs (gc_*.h) and the patcher's (gcs_*.h) share them.
#pragma once
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>

// head + count * item when that is at most left and Size holds it; empty otherwise.
template <typename Size = size_t>
inline std::optional<Size> checked_size(uint64_t head, uint64_t count, uint64_t item, uint64_t left) {
    if (item && count > (UINT64_MAX - head) / item) return std::nullopt;
    const uint64_t n = head + count * item;
    if (n > left || n > uint64_t(std::numeric_limits<Size>::max())) return std::nullopt;
    return Size(n);
}

// base + delta (a list's first id, zigzag-coded from the previous list's) when it is a u32; empty
// otherwise, so a malformed delta can neither overflow the signed sum nor narrow to a small id.
inline std::optional<uint32_t> checked_id(uint64_t base, int64_t delta) {
    if (base > uint64_t(INT64_MAX)) return std::nullopt;
    const int64_t b = int64_t(base);
    if (delta < -b || delta > int64_t(UINT32_MAX) - b) return std::nullopt;
    return uint32_t(b + delta);
}

// base + gap (an id, from the one before it) when it is a u32; empty otherwise.
inline std::optional<uint32_t> checked_id(uint64_t base, uint64_t gap) {
    if (base > UINT32_MAX || gap > UINT32_MAX - base) return std::nullopt;
    return uint32_t(base + gap);
}
