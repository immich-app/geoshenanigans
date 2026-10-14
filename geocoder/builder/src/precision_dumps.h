// Measurement-only side outputs for the precision study (never shipped):
// exact 1e-7 coordinates of the records the build writes, keyed by the
// strategy-2 osm id the record's *.osm_ids sidecar carries, and the name*
// tags of places, admin boundaries and POIs. Off unless --exact-dumps or
// --name-dumps names a directory. tools/precision_align.cpp turns the keyed
// coordinate dumps into files aligned with the shipped records; the README
// written next to the dumps documents every format.
#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <cerrno>
#include <fcntl.h>
#include <unistd.h>

#include "node_index.h"

namespace precision {

// A dump file many threads append whole buffers to.
class DumpFile {
public:
    DumpFile() = default;
    DumpFile(const DumpFile&) = delete;
    DumpFile& operator=(const DumpFile&) = delete;
    ~DumpFile() { close(); }

    void open(const std::string& path) {
        fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (fd_ < 0) throw std::runtime_error("cannot create " + path);
        path_ = path;
    }
    bool is_open() const { return fd_ >= 0; }
    const std::string& path() const { return path_; }
    uint64_t bytes() const { return bytes_.load(); }
    uint64_t records() const { return records_.load(); }

    void write(const char* p, size_t n, uint64_t records) {
        std::lock_guard<std::mutex> lock(mtx_);
        while (n > 0) {
            ssize_t w = ::write(fd_, p, n);
            if (w < 0 && errno == EINTR) continue;
            if (w <= 0) throw std::runtime_error("short write to " + path_);
            p += w;
            n -= static_cast<size_t>(w);
            bytes_ += static_cast<uint64_t>(w);
        }
        records_ += records;
    }
    void close() {
        if (fd_ >= 0) ::close(fd_);
        fd_ = -1;
    }

private:
    int fd_ = -1;
    std::string path_;
    std::mutex mtx_;
    std::atomic<uint64_t> bytes_{0};
    std::atomic<uint64_t> records_{0};
};

// One thread's buffer in front of a DumpFile (none: every call is a no-op).
class DumpBuffer {
public:
    static constexpr size_t kFlushBytes = size_t(1) << 20;

    DumpBuffer() = default;
    explicit DumpBuffer(DumpFile* file) { bind(file); }
    DumpBuffer(DumpBuffer&& o) noexcept : file_(o.file_), buf_(std::move(o.buf_)), records_(o.records_) {
        o.file_ = nullptr;
        o.buf_.clear();
        o.records_ = 0;
    }
    DumpBuffer& operator=(DumpBuffer&& o) noexcept {
        if (this != &o) {
            flush();
            file_ = o.file_;
            buf_ = std::move(o.buf_);
            records_ = o.records_;
            o.file_ = nullptr;
            o.buf_.clear();
            o.records_ = 0;
        }
        return *this;
    }
    ~DumpBuffer() { flush(); }

    void bind(DumpFile* file) { file_ = file && file->is_open() ? file : nullptr; }
    bool on() const { return file_ != nullptr; }

    template <class T>
    void put(const T& v) { buf_.append(reinterpret_cast<const char*>(&v), sizeof(T)); }
    void put_bytes(const void* p, size_t n) { buf_.append(static_cast<const char*>(p), n); }
    void put_text(std::string_view s) { buf_.append(s.data(), s.size()); }
    void end_record() {
        records_++;
        if (buf_.size() >= kFlushBytes) flush();
    }
    void flush() {
        if (file_ && !buf_.empty()) file_->write(buf_.data(), buf_.size(), records_);
        buf_.clear();
        records_ = 0;
    }

private:
    DumpFile* file_ = nullptr;
    std::string buf_;
    uint64_t records_ = 0;
};

inline int32_t e7(double deg) { return static_cast<int32_t>(std::llround(deg * 1e7)); }

// Coordinate dump records (little-endian, packed):
//   line   (streets.raw, interps.raw): u64 key, u32 n, n x (i32 lat_e7, i32 lng_e7)
//   addr   (addrs.raw):  u64 key, u8 kind, u8 0, u16 0, i32 lat_e7, i32 lng_e7,
//                        u32 n, n x (i32, i32)
//   admin  (admins.raw): u64 key, u8 level, u8 source, u16 ring, i64 osm_id,
//                        u32 n_raw, u32 n_dp, n_raw x (i32, i32), n_dp x (i32, i32)
enum class AddrKind : uint8_t { Node = 0, Building = 1, PoiRelation = 2 };
enum class AdminSource : uint8_t { RelationRing = 0, ClosedWay = 1 };

inline void put_locs(DumpBuffer& b, const PackedLocation* locs, size_t n) {
    for (size_t i = 0; i < n; i++) {
        b.put(locs[i].lat_e7);
        b.put(locs[i].lon_e7);
    }
}
inline void put_pairs(DumpBuffer& b, const std::vector<std::pair<double, double>>& pts) {
    for (const auto& [lat, lng] : pts) {
        b.put(e7(lat));
        b.put(e7(lng));
    }
}

inline void put_line(DumpBuffer& b, uint64_t key, const PackedLocation* locs, size_t n) {
    if (!b.on()) return;
    b.put(key);
    b.put(static_cast<uint32_t>(n));
    put_locs(b, locs, n);
    b.end_record();
}
inline void put_line(DumpBuffer& b, uint64_t key, const std::vector<PackedLocation>& locs) {
    put_line(b, key, locs.data(), locs.size());
}

inline void put_addr(DumpBuffer& b, uint64_t key, AddrKind kind, int32_t lat_e7, int32_t lng_e7,
                     const PackedLocation* poly, size_t n) {
    if (!b.on()) return;
    b.put(key);
    b.put(static_cast<uint8_t>(kind));
    b.put(uint8_t(0));
    b.put(uint16_t(0));
    b.put(lat_e7);
    b.put(lng_e7);
    b.put(static_cast<uint32_t>(n));
    put_locs(b, poly, n);
    b.end_record();
}
inline void put_addr(DumpBuffer& b, uint64_t key, AddrKind kind, int32_t lat_e7, int32_t lng_e7,
                     const std::vector<std::pair<double, double>>& poly) {
    if (!b.on()) return;
    b.put(key);
    b.put(static_cast<uint8_t>(kind));
    b.put(uint8_t(0));
    b.put(uint16_t(0));
    b.put(lat_e7);
    b.put(lng_e7);
    b.put(static_cast<uint32_t>(poly.size()));
    put_pairs(b, poly);
    b.end_record();
}

inline void put_admin(DumpBuffer& b, uint64_t key, uint8_t level, AdminSource source, uint16_t ring,
                      int64_t osm_id, const std::vector<std::pair<double, double>>& raw,
                      const std::vector<std::pair<double, double>>& dp) {
    if (!b.on()) return;
    b.put(key);
    b.put(level);
    b.put(static_cast<uint8_t>(source));
    b.put(ring);
    b.put(osm_id);
    b.put(static_cast<uint32_t>(raw.size()));
    b.put(static_cast<uint32_t>(dp.size()));
    put_pairs(b, raw);
    put_pairs(b, dp);
    b.end_record();
}

// --- Names ---

// Name-like OSM keys: name, name:*, and *_name / *_name:* (int_name,
// alt_name, old_name, official_name, short_name, loc_name, reg_name,
// nat_name, sorting_name, alt_name:en, ...).
inline bool is_name_key(std::string_view k) {
    if (k == "name" || k.rfind("name:", 0) == 0) return true;
    for (size_t pos = k.find("_name"); pos != std::string_view::npos; pos = k.find("_name", pos + 1)) {
        size_t after = pos + 5;
        if (after == k.size() || k[after] == ':') return true;
    }
    return false;
}

inline void put_escaped(DumpBuffer& b, std::string_view s) {
    for (char c : s) {
        switch (c) {
            case '\t': b.put_text("\\t"); break;
            case '\n': b.put_text("\\n"); break;
            case '\r': b.put_text("\\r"); break;
            case '\\': b.put_text("\\\\"); break;
            default: b.put_text(std::string_view(&c, 1));
        }
    }
}

// Feature classes of the name dump.
enum class NameClass : uint8_t { Place = 0, Admin = 1, Poi = 2 };
constexpr size_t kNameClasses = 3;
inline const char* name_class_label(NameClass c) {
    switch (c) {
        case NameClass::Place: return "place";
        case NameClass::Admin: return "admin";
        default: return "poi";
    }
}

// One thread's name dump: a TSV line per feature that carries a name-like
// tag, `class type id subtype shipped (key value)*`, and per class the
// features looked at and those with a name-like tag.
struct NameDump {
    DumpBuffer buf;
    std::array<uint64_t, kNameClasses> seen{};
    std::array<uint64_t, kNameClasses> named{};

    bool on() const { return buf.on(); }

    // tags(emit) calls emit(key, value) for every tag of the feature.
    template <class Tags>
    void feature(NameClass cls, char osm_type, int64_t id, std::string_view subtype, bool shipped, Tags tags) {
        if (!on()) return;
        seen[static_cast<size_t>(cls)]++;
        bool any = false;
        tags([&](std::string_view k, std::string_view v) {
            if (!is_name_key(k)) return;
            if (!any) {
                buf.put_text(name_class_label(cls));
                buf.put_text("\t");
                buf.put_text(std::string_view(&osm_type, 1));
                buf.put_text("\t");
                buf.put_text(std::to_string(id));
                buf.put_text("\t");
                put_escaped(buf, subtype);
                buf.put_text(shipped ? "\t1" : "\t0");
                any = true;
            }
            buf.put_text("\t");
            put_escaped(buf, k);
            buf.put_text("\t");
            put_escaped(buf, v);
        });
        if (!any) return;
        buf.put_text("\n");
        named[static_cast<size_t>(cls)]++;
        buf.end_record();
    }
};

// Tag visitors for the PBF reader's callbacks and relations.
inline auto stream_tags(const uint32_t* keys, const uint32_t* vals, size_t n, const std::vector<std::string>& st) {
    return [=, &st](auto&& emit) {
        for (size_t i = 0; i < n; i++) {
            if (keys[i] >= st.size() || vals[i] >= st.size()) continue;
            emit(std::string_view(st[keys[i]]), std::string_view(st[vals[i]]));
        }
    };
}

// --- Building outline cap counters ---

// Outline vertex counts (closing node included) in buckets: <=20 is what
// the default cap leaves alone.
constexpr std::array<uint32_t, 10> kOutlineBucketMax = {8, 12, 16, 20, 32, 64, 128, 256, 1024, UINT32_MAX};

struct OutlineStats {
    uint64_t outlines = 0;          // building outlines (closed, >= 4 refs, all nodes found)
    uint64_t points_before = 0;     // their vertices before any cap
    uint64_t points_after = 0;      // vertices stored after the cap in force
    uint64_t over_default = 0;      // outlines over 20 vertices (what the default cap trims)
    uint64_t over_default_points = 0;
    uint64_t trimmed = 0;           // outlines the cap in force trimmed
    std::array<uint64_t, kOutlineBucketMax.size()> bucket_outlines{};
    std::array<uint64_t, kOutlineBucketMax.size()> bucket_points_before{};
    std::array<uint64_t, kOutlineBucketMax.size()> bucket_points_after{};
    uint64_t max_points = 0;

    void add(size_t before, size_t after) {
        outlines++;
        points_before += before;
        points_after += after;
        if (before > 20) {
            over_default++;
            over_default_points += before;
        }
        if (after < before) trimmed++;
        size_t b = 0;
        while (before > kOutlineBucketMax[b]) b++;
        bucket_outlines[b]++;
        bucket_points_before[b] += before;
        bucket_points_after[b] += after;
        max_points = std::max<uint64_t>(max_points, before);
    }
    void merge(const OutlineStats& o) {
        outlines += o.outlines;
        points_before += o.points_before;
        points_after += o.points_after;
        over_default += o.over_default;
        over_default_points += o.over_default_points;
        trimmed += o.trimmed;
        for (size_t b = 0; b < kOutlineBucketMax.size(); b++) {
            bucket_outlines[b] += o.bucket_outlines[b];
            bucket_points_before[b] += o.bucket_points_before[b];
            bucket_points_after[b] += o.bucket_points_after[b];
        }
        max_points = std::max(max_points, o.max_points);
    }
};

// The open dumps of a build; g_dumps is null when none were asked for.
struct Dumps {
    std::string exact_dir, names_dir;
    DumpFile streets, interps, addrs, admins, names, links;
    std::mutex totals_mtx;
    std::array<uint64_t, kNameClasses> name_seen{};
    std::array<uint64_t, kNameClasses> name_named{};

    void add_name_counts(const NameDump& d) {
        std::lock_guard<std::mutex> lock(totals_mtx);
        for (size_t c = 0; c < kNameClasses; c++) {
            name_seen[c] += d.seen[c];
            name_named[c] += d.named[c];
        }
    }
};
inline Dumps* g_dumps = nullptr;

// --link-dumps: one line per place node a boundary claims (the builder's
// linked_place_node_ids / step-4 hides):
//   method \t boundary type (r|w) \t boundary osm id \t node osm id \t place_type_override
// boundary_packed is an admin_osm_ids-style key: relations carry
// (relation id << 16 | ring) under type 3, closed ways the way id under type 2.
inline void link_event(const char* method, uint64_t boundary_packed, int64_t node_id, unsigned pto) {
    if (!g_dumps || !g_dumps->links.is_open()) return;
    const unsigned type = static_cast<unsigned>(boundary_packed >> 56);
    uint64_t id = boundary_packed & 0x00FFFFFFFFFFFFFFull;
    char kind = '?';
    if (type == 3) { kind = 'r'; id >>= 16; }
    if (type == 2) kind = 'w';
    char line[128];
    const int n = std::snprintf(line, sizeof line, "%s\t%c\t%llu\t%lld\t%u\n", method, kind,
                                static_cast<unsigned long long>(id), static_cast<long long>(node_id), pto);
    g_dumps->links.write(line, static_cast<size_t>(n), 1);
}

inline DumpFile* exact_file(DumpFile Dumps::*member) {
    return g_dumps && (g_dumps->*member).is_open() ? &(g_dumps->*member) : nullptr;
}

}  // namespace precision
