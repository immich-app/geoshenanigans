// The place files (place_files.h) of a patch test's dir.
#pragma once

#include <cstdint>
#include <vector>

#include "place_files.h"
#include "tool_runs.h"
#include "types.h"

// A visible place node of type 1 at (lat, lng) named name.
inline PlaceNode test_place(float lat, float lng, uint32_t name) {
    PlaceNode p{};
    p.lat = lat;
    p.lng = lng;
    p.name_id = name;
    p.place_type = 1;
    return p;
}

// The place files of nodes into d: the visible ones (no flags) in their
// order, the ith of them in the cell of key i / 3 (the patch tools read no
// cell from a point), then the hidden ones in theirs.
inline void put_places(gctest::BuildDir& d, const std::vector<PlaceNode>& nodes) {
    std::vector<uint8_t> cells, blocks, hidden;
    auto into = [](std::vector<uint8_t>& v) { return [&v](const uint8_t* p, size_t n) { v.insert(v.end(), p, p + n); }; };
    place_files::Encoder enc(into(cells), into(blocks), into(hidden));
    uint32_t visible = 0;
    for (const PlaceNode& n : nodes)
        if (place_files::is_visible(n)) enc.add({n, visible++ / 3});
    for (const PlaceNode& n : nodes)
        if (!place_files::is_visible(n)) enc.add({n, NO_DATA});
    enc.finish();
    for (const auto& e : enc.table()) blocks.insert(blocks.end(), e.begin(), e.end());
    const auto footer = enc.footer();
    blocks.insert(blocks.end(), footer.begin(), footer.end());
    auto chars = [](const std::vector<uint8_t>& v) { return std::vector<char>(v.begin(), v.end()); };
    d[place_files::CELLS_FILE] = chars(cells);
    d[place_files::NODES_FILE] = chars(blocks);
    d[place_files::HIDDEN_FILE] = chars(hidden);
}
