// u32-per-way sidecars (max/streets/sidecars.py):
//   way_parents.bin   -> way_parents.u24: 3 bytes per way, NO_DATA -> 0xFFFFFF (values must be < 0xFFFFFF)
//   way_postcodes.bin / interp_postcodes.bin -> <name>.u16 (u16 index per way, 0xFFFF = NO_DATA) +
//                        <name>.dict (the distinct values, ascending u32; fewer than 0xFFFF of them)
#pragma once
#include "gc_io.h"

namespace gcset::side {

inline void encode_u24(const std::string& src, const std::string& out, Manifest& m) {
    Reader r(src + "/way_parents.bin");
    if (r.size() % 4) die("way_parents.bin size");
    const uint64_t n = r.size() / 4;
    Writer w(out + "/way_parents.u24");
    uint64_t nd = 0;
    for (uint64_t i = 0; i < n; i++) {
        uint32_t v = r.u32();
        if (v == NO) { v = 0xFFFFFF; nd++; }
        else if (v >= 0xFFFFFF) die("way_parents value %u does not fit 24 bits", v);
        const uint8_t b[3] = {uint8_t(v), uint8_t(v >> 8), uint8_t(v >> 16)};
        w.put(b, 3);
    }
    m.set("way_parents.n", n);
    fprintf(stderr, "  way_parents: %llu values, %llu NO_DATA\n", (unsigned long long)n, (unsigned long long)nd);
}

inline void decode_u24(const std::string& src, const std::string& out, const Manifest& m) {
    const uint64_t n = m.num("way_parents.n");
    Reader r(src + "/way_parents.u24");
    if (r.size() != 3 * n) die("way_parents.u24 size");
    Writer w(out + "/way_parents.bin");
    for (uint64_t i = 0; i < n; i++) {
        const uint8_t* b = r.take(3);
        uint32_t v = uint32_t(b[0]) | (uint32_t(b[1]) << 8) | (uint32_t(b[2]) << 16);
        if (v == 0xFFFFFF) v = NO;
        w.u32(v);
    }
}

// Open-addressing u32 -> u32 map (keys never NO).
struct Map32 {
    std::vector<uint32_t> keys, vals;
    size_t mask = 0, n = 0;
    explicit Map32(size_t cap_pow2 = 1 << 18) : keys(cap_pow2, NO), vals(cap_pow2, 0), mask(cap_pow2 - 1) {}
    static inline size_t h(uint32_t k) { return size_t((uint64_t(k) * 0x9E3779B97F4A7C15ull) >> 32); }
    uint32_t* find_or_add(uint32_t k, bool& added) {
        size_t i = h(k) & mask;
        while (keys[i] != NO && keys[i] != k) i = (i + 1) & mask;
        added = keys[i] == NO;
        if (added) {
            keys[i] = k;
            if (++n * 2 > keys.size()) die("Map32 full");
        }
        return &vals[i];
    }
    const uint32_t* find(uint32_t k) const {
        size_t i = h(k) & mask;
        while (keys[i] != NO && keys[i] != k) i = (i + 1) & mask;
        return keys[i] == NO ? nullptr : &vals[i];
    }
};

// Streaming: a FIFO input can be read once, so the values' first-seen indexes are kept in memory
// (2 bytes a value) and renumbered to the sorted dictionary's at the end; same files as below.
inline void encode_dict_stream(const std::string& in, const std::string& out, const char* name, Manifest& m) {
    const uint64_t n = file_size(in) / 4;
    if (file_size(in) % 4) die("%s size", in.c_str());
    Map32 seen;
    std::vector<uint32_t> dict;
    std::vector<uint16_t> first(n);
    uint64_t nd = 0;
    {
        Reader r(in);
        for (uint64_t i = 0; i < n; i++) {
            const uint32_t v = r.u32();
            if (v == NO) { first[i] = 0xFFFF; nd++; continue; }
            bool added;
            uint32_t* id = seen.find_or_add(v, added);
            if (added) {
                *id = uint32_t(dict.size());
                dict.push_back(v);
                if (dict.size() >= 0xFFFF) die("%s: too many distinct values for a u16 index", name);
            }
            first[i] = uint16_t(*id);
        }
    }
    std::vector<uint32_t> order(dict.size());
    for (size_t j = 0; j < order.size(); j++) order[j] = uint32_t(j);
    std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) { return dict[a] < dict[b]; });
    std::vector<uint16_t> final_of(dict.size());
    for (size_t j = 0; j < order.size(); j++) final_of[order[j]] = uint16_t(j);
    {
        Writer d(out + "/" + name + ".dict", 1 << 20);
        for (uint32_t j : order) d.u32(dict[j]);
    }
    Writer w(out + "/" + name + ".u16");
    for (uint64_t i = 0; i < n; i++) w.u16(first[i] == 0xFFFF ? 0xFFFF : final_of[first[i]]);
    m.set(std::string(name) + ".n", n);
    m.set(std::string(name) + ".dict", dict.size());
    fprintf(stderr, "  %s (stream): %llu values, %zu distinct, %llu NO_DATA\n", name, (unsigned long long)n, dict.size(),
            (unsigned long long)nd);
}

inline void encode_dict(const std::string& src, const std::string& out, const char* name, Manifest& m) {
    const std::string in = src + "/" + name + ".bin";
    if (is_fifo(in)) {
        encode_dict_stream(in, out, name, m);
        return;
    }
    uint64_t n = file_size(in) / 4;
    if (file_size(in) % 4) die("%s size", in.c_str());
    Map32 seen;
    std::vector<uint32_t> dict;
    {
        Reader r(in);
        for (uint64_t i = 0; i < n; i++) {
            const uint32_t v = r.u32();
            if (v == NO) continue;
            bool added;
            seen.find_or_add(v, added);
            if (added) {
                dict.push_back(v);
                if (dict.size() >= 0xFFFF) die("%s: too many distinct values for a u16 index", name);
            }
        }
    }
    std::sort(dict.begin(), dict.end());
    for (size_t j = 0; j < dict.size(); j++) {
        bool added;
        *seen.find_or_add(dict[j], added) = uint32_t(j);
    }
    {
        Writer d(out + "/" + name + ".dict", 1 << 20);
        for (uint32_t v : dict) d.u32(v);
    }
    Reader r(in);
    Writer w(out + "/" + name + ".u16");
    uint64_t nd = 0;
    for (uint64_t i = 0; i < n; i++) {
        const uint32_t v = r.u32();
        if (v == NO) { w.u16(0xFFFF); nd++; continue; }
        w.u16(uint16_t(*seen.find(v)));
    }
    m.set(std::string(name) + ".n", n);
    m.set(std::string(name) + ".dict", dict.size());
    fprintf(stderr, "  %s: %llu values, %zu distinct, %llu NO_DATA\n", name, (unsigned long long)n, dict.size(),
            (unsigned long long)nd);
}

inline void decode_dict(const std::string& src, const std::string& out, const char* name, const Manifest& m) {
    const uint64_t n = m.num(std::string(name) + ".n");
    std::vector<uint32_t> dict;
    {
        Reader d(src + "/" + name + ".dict", 1 << 20);
        while (!d.eof()) dict.push_back(d.u32());
    }
    Reader r(src + "/" + name + ".u16");
    if (r.size() != 2 * n) die("%s.u16 size", name);
    Writer w(out + "/" + name + ".bin");
    for (uint64_t i = 0; i < n; i++) {
        const uint16_t x = r.u16();
        if (x == 0xFFFF) { w.u32(NO); continue; }
        if (x >= dict.size()) die("%s: index out of range", name);
        w.u32(dict[x]);
    }
}

}  // namespace gcset::side
