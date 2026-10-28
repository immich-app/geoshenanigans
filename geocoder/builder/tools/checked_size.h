// The byte size of a unit an input file sizes by a count (a footprint, a polygon block), which a
// malformed file can make any size: head + count * item, computed in 64 bits and checked against what
// the input has left and against the size type before anything is read or allocated. In size_t, a
// 32-bit build wraps 10 + 4 * 2^30 to 10, so a ten-byte unit would pass for a billion vertices.
// geocoder-gc's codecs (gc_*.h) and the patcher's (gcs_*.h) share it.
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
