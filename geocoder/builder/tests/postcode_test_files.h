// The postcode centroid files (postcode_files.h) of a patch test's dir.
#pragma once

#include <cstdint>
#include <vector>

#include "cell_pairs.h"
#include "postcode_files.h"
#include "tool_runs.h"

// A country code as a centroid holds it: two letters, the first high.
inline constexpr uint16_t DE = uint16_t('D' << 8 | 'E');

// A centroid at (lat, lng) of postcode string pid in country cc, in the
// level-10 cell of number cell (the patch tools read no cell from a point).
inline postcode_files::LogicalCentroid test_centroid(float lat, float lng, uint32_t pid, uint16_t cc, uint64_t cell) {
    postcode_files::LogicalCentroid c;
    c.lat = lat;
    c.lng = lng;
    c.postcode_id = pid;
    c.country_code = cc;
    c.cell_key = uint32_t(cell_pairs::cell_of(cell, 10) >> 32);
    return c;
}

// The centroid files of rows (in their logical order) into d: no rows give
// the footer alone and an empty cells file.
inline void put_centroids(gctest::BuildDir& d, const std::vector<postcode_files::LogicalCentroid>& rows) {
    std::vector<uint8_t> centroids, cells;
    auto into = [](std::vector<uint8_t>& v) { return [&v](const uint8_t* p, size_t n) { v.insert(v.end(), p, p + n); }; };
    postcode_files::Encoder enc(into(centroids), into(cells));
    for (const postcode_files::LogicalCentroid& c : rows) enc.add(c);
    enc.finish();
    for (const auto& e : enc.table()) centroids.insert(centroids.end(), e.begin(), e.end());
    const auto footer = enc.footer();
    centroids.insert(centroids.end(), footer.begin(), footer.end());
    d[postcode_files::CENTROIDS_FILE].assign(centroids.begin(), centroids.end());
    d[postcode_files::CELLS_FILE].assign(cells.begin(), cells.end());
}

// The rows' logical bytes, as geocoder-diff --decode-logical writes them.
inline std::vector<char> logical_bytes(const std::vector<postcode_files::LogicalCentroid>& rows) {
    const char* p = reinterpret_cast<const char*>(rows.data());
    return std::vector<char>(p, p + rows.size() * postcode_files::LOGICAL_STRIDE);
}
