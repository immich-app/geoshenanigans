// precision_align: turns the builder's keyed exact-coordinate dumps
// (--exact-dumps, see precision_dumps.h) into files aligned with a cold
// planet build's shipped records, and proves the alignment record by record.
//
//   precision_align <planet output dir> <raw dump dir> <aligned out dir>
//
// <planet output dir> is <build out>/planet (full/ and quality/uncapped/ are
// read). Each shipped record is found in the dump by the key its *.osm_ids
// sidecar slot carries; among dump records with that key, the one whose
// coordinates reproduce the shipped bytes wins. Verification per kind:
//   streets, interps: float(e7 / 1e7) equals the shipped f32 node, every node;
//   addr points: node points' f32 equal; outlines re-packed from float(e7/1e7)
//     with the builder's vertex_pack.h equal the shipped addr_vertices bytes;
//   admin (quality/uncapped): the 0.1 m DP ring re-packed equals the shipped
//     admin_vertices bytes.
// Formats of what it writes: README.md beside the dumps.
#include <algorithm>
#include <atomic>
#include <cinttypes>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "types.h"
#include "vertex_pack.h"

namespace {

struct Map {
    const uint8_t* p = nullptr;
    size_t n = 0;
    explicit Map(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) throw std::runtime_error("cannot open " + path);
        struct stat st{};
        if (::fstat(fd, &st) != 0) throw std::runtime_error("cannot stat " + path);
        n = static_cast<size_t>(st.st_size);
        if (n) {
            void* m = ::mmap(nullptr, n, PROT_READ, MAP_PRIVATE, fd, 0);
            if (m == MAP_FAILED) throw std::runtime_error("cannot map " + path);
            p = static_cast<const uint8_t*>(m);
            ::madvise(m, n, MADV_WILLNEED);
        }
        ::close(fd);
    }
    ~Map() {
        if (p) ::munmap(const_cast<uint8_t*>(p), n);
    }
    Map(const Map&) = delete;
    template <class T>
    const T* as() const { return reinterpret_cast<const T*>(p); }
    template <class T>
    size_t count() const { return n / sizeof(T); }
};

template <class T>
T rd(const uint8_t* p) {
    T v;
    std::memcpy(&v, p, sizeof(T));
    return v;
}

// Sidecar keys: (object_type << 56) | stable_id per slot.
std::vector<uint64_t> sidecar_keys(const std::string& path) {
    Map m(path);
    if (m.n < 12) throw std::runtime_error(path + ": no header");
    const uint32_t count = rd<uint32_t>(m.p + 8);
    if (m.n != 12 + size_t(count) * 12) throw std::runtime_error(path + ": size does not match its count");
    std::vector<uint64_t> keys(count);
    for (size_t i = 0; i < count; i++) {
        const uint8_t* s = m.p + 12 + i * 12;
        const uint8_t type = s[0], flags = s[1];
        if (flags & 1) {
            keys[i] = UINT64_MAX;  // tombstone (none in a cold build)
            continue;
        }
        keys[i] = (uint64_t(type) << 56) | (rd<uint64_t>(s + 4) & 0x00FFFFFFFFFFFFFFull);
    }
    return keys;
}

struct Entry {
    uint64_t key;
    uint64_t off;
};

// (key, record offset) of every record of a raw dump, sorted by key; size_of
// returns a record's byte size at an offset.
template <class SizeOf>
std::vector<Entry> index_dump(const Map& m, SizeOf size_of) {
    std::vector<Entry> idx;
    for (size_t off = 0; off < m.n;) {
        idx.push_back({rd<uint64_t>(m.p + off), off});
        off += size_of(m.p + off);
    }
    std::sort(idx.begin(), idx.end(), [](const Entry& a, const Entry& b) {
        return a.key < b.key || (a.key == b.key && a.off < b.off);
    });
    return idx;
}

std::pair<const Entry*, const Entry*> find_all(const std::vector<Entry>& idx, uint64_t key) {
    auto lo = std::lower_bound(idx.begin(), idx.end(), key, [](const Entry& e, uint64_t k) { return e.key < k; });
    auto hi = lo;
    while (hi != idx.end() && hi->key == key) ++hi;
    return {idx.data() + (lo - idx.begin()), idx.data() + (hi - idx.begin())};
}

float f32_of(int32_t e7) { return static_cast<float>(e7 / 10000000.0); }

void write_file(const std::string& path, const void* p, size_t n) {
    std::ofstream f(path, std::ios::binary);
    f.write(static_cast<const char*>(p), static_cast<std::streamsize>(n));
    f.flush();
    if (!f) throw std::runtime_error("failed to write " + path);
}

struct Tally {
    std::atomic<uint64_t> records{0}, matched{0}, missing{0}, mismatched{0}, multi_key{0}, vertices{0};
};

std::ofstream summary;

void report(const char* kind, const Tally& t, const std::string& extra = "") {
    std::cout << kind << ": " << t.records << " shipped records, " << t.matched << " matched exactly, "
              << t.missing << " missing from the dump, " << t.mismatched << " mismatched, " << t.multi_key
              << " keys with several dump records, " << t.vertices << " vertices aligned" << extra << std::endl;
    summary << kind << "\t" << t.records << "\t" << t.matched << "\t" << t.missing << "\t" << t.mismatched << "\t"
            << t.multi_key << "\t" << t.vertices << "\n";
}

// Streets and interps: a sidecar, a header array with (node_offset u32 at 0,
// node_count u16 at 4), f32 node pairs; writes <name>.e7 parallel to the
// nodes file, every node, and returns the tally.
template <class Header>
void align_lines(const char* kind, const std::string& sidecar, const std::string& headers_path,
                 const std::string& nodes_path, const std::string& raw_path, const std::string& out_path) {
    const std::vector<uint64_t> keys = sidecar_keys(sidecar);
    Map headers(headers_path), nodes(nodes_path), raw(raw_path);
    const size_t n = headers.count<Header>();
    if (n != keys.size()) throw std::runtime_error(std::string(kind) + ": sidecar and records differ in count");
    const auto* h = headers.as<Header>();
    const auto* node = nodes.as<NodeCoord>();
    const size_t node_total = nodes.count<NodeCoord>();
    const std::vector<Entry> idx = index_dump(raw, [](const uint8_t* p) { return 12 + 8 * size_t(rd<uint32_t>(p + 8)); });
    std::vector<int32_t> out(2 * node_total, 0);
    Tally t;
    t.records = n;
#pragma omp parallel for schedule(dynamic, 4096)
    for (size_t i = 0; i < n; i++) {
        const uint32_t off = h[i].node_offset;
        const uint32_t cnt = h[i].node_count;
        auto [lo, hi] = find_all(idx, keys[i]);
        if (lo == hi) {
            t.missing++;
            continue;
        }
        if (hi - lo > 1) t.multi_key++;
        const uint8_t* pick = nullptr;
        for (const Entry* e = lo; e != hi && !pick; ++e) {
            const uint8_t* r = raw.p + e->off;
            if (rd<uint32_t>(r + 8) != cnt) continue;
            bool same = true;
            for (uint32_t j = 0; j < cnt && same; j++) {
                const int32_t la = rd<int32_t>(r + 12 + 8 * j), lo_ = rd<int32_t>(r + 16 + 8 * j);
                same = f32_of(la) == node[off + j].lat && f32_of(lo_) == node[off + j].lng;
            }
            if (same) pick = r;
        }
        if (!pick) {
            t.mismatched++;
            continue;
        }
        std::memcpy(&out[2 * size_t(off)], pick + 12, 8 * size_t(cnt));
        t.matched++;
        t.vertices += cnt;
    }
    write_file(out_path, out.data(), out.size() * sizeof(int32_t));
    report(kind, t);
}

// A shipped polygon's bytes at `at` in a packed vertex file: header plus
// count vertices at the tag's stride.
size_t packed_size(const uint8_t* at, uint32_t count) {
    const uint8_t tag = at[0];
    if (tag > 3) return SIZE_MAX;
    return POLY_HEADER_BYTES + size_t(count) * VERTEX_STRIDE[tag];
}

// Whether float(e7/1e7) vertices pack to exactly `shipped` (size bytes).
bool repacks_to(const uint8_t* e7_pairs, uint32_t count, const uint8_t* shipped, size_t size) {
    std::vector<NodeCoord> v(count);
    for (uint32_t j = 0; j < count; j++)
        v[j] = {f32_of(rd<int32_t>(e7_pairs + 8 * j)), f32_of(rd<int32_t>(e7_pairs + 8 * j + 4))};
    PolygonPacking plan = plan_polygon(v.data(), v.size());
    if (plan.bytes != size) return false;
    std::vector<uint8_t> bytes(plan.bytes);
    pack_polygon(bytes.data(), plan, v.data(), v.size());
    return std::memcmp(bytes.data(), shipped, size) == 0;
}

void align_addrs(const std::string& full, const std::string& raw_dir, const std::string& dst) {
    const std::vector<uint64_t> keys = sidecar_keys(full + "/addr_points.osm_ids");
    Map points(full + "/addr_points.bin"), packed(full + "/addr_vertices.bin"), raw(raw_dir + "/addrs.raw");
    const size_t n = points.count<AddrPoint>();
    if (n != keys.size()) throw std::runtime_error("addr points: sidecar and records differ in count");
    const auto* pt = points.as<AddrPoint>();
    const std::vector<Entry> idx = index_dump(raw, [](const uint8_t* p) { return 24 + 8 * size_t(rd<uint32_t>(p + 20)); });

    // Each point's shipped polygon bytes start where the previous polygons'
    // end (running offsets: a 32-bit vertex_offset wraps past 4 GiB).
    std::vector<uint64_t> byte_at(n + 1, 0);
    uint64_t wrapped = 0;
    for (size_t i = 0; i < n; i++) {
        size_t sz = 0;
        if (pt[i].vertex_count > 0 && pt[i].vertex_offset != NO_DATA) {
            if (uint32_t(byte_at[i]) != pt[i].vertex_offset) wrapped++;
            sz = byte_at[i] < packed.n ? packed_size(packed.p + byte_at[i], pt[i].vertex_count) : SIZE_MAX;
            if (sz == SIZE_MAX) throw std::runtime_error("addr_vertices.bin: bad polygon header at point " + std::to_string(i));
        }
        byte_at[i + 1] = byte_at[i] + sz;
    }
    if (byte_at[n] != packed.n) throw std::runtime_error("addr_vertices.bin: polygons do not fill the file");

    std::vector<const uint8_t*> pick(n, nullptr);
    std::vector<uint8_t> kind(n, 255);
    Tally t;
    t.records = n;
    std::atomic<uint64_t> derived_far{0}, polygons{0};
#pragma omp parallel for schedule(dynamic, 4096)
    for (size_t i = 0; i < n; i++) {
        auto [lo, hi] = find_all(idx, keys[i]);
        if (lo == hi) {
            t.missing++;
            continue;
        }
        if (hi - lo > 1) t.multi_key++;
        const AddrPoint& a = pt[i];
        const bool has_poly = a.vertex_count > 0 && a.vertex_offset != NO_DATA;
        for (const Entry* e = lo; e != hi && !pick[i]; ++e) {
            const uint8_t* r = raw.p + e->off;
            const uint8_t k = r[8];
            const int32_t la = rd<int32_t>(r + 12), lg = rd<int32_t>(r + 16);
            const uint32_t cnt = rd<uint32_t>(r + 20);
            bool ok;
            if (k == 0) {
                ok = f32_of(la) == a.lat && f32_of(lg) == a.lng;
            } else {
                // A centroid the builder derived in double: within a f32 step.
                ok = std::fabs(la / 1e7 - a.lat) < 2e-5 && std::fabs(lg / 1e7 - a.lng) < 2e-5;
            }
            if (ok && has_poly)
                ok = cnt == a.vertex_count &&
                     repacks_to(r + 24, cnt, packed.p + byte_at[i], byte_at[i + 1] - byte_at[i]);
            if (ok) {
                pick[i] = r;
                kind[i] = k;
            }
        }
        if (!pick[i]) {
            t.mismatched++;
            continue;
        }
        t.matched++;
        if (has_poly) {
            polygons++;
            t.vertices += a.vertex_count;
        }
    }

    // addr_points.e7 (i32 pair per point), addr_points.kind (u8 per point),
    // addr_vertices.e7 (i32 pairs) with addr_vertices.e7.idx (u64 CSR offsets,
    // n + 1, in vertices): a point's dump outline, empty when it ships none.
    std::vector<int32_t> pos(2 * n, 0);
    std::vector<uint64_t> vat(n + 1, 0);
    for (size_t i = 0; i < n; i++) {
        const bool has_poly = pt[i].vertex_count > 0 && pt[i].vertex_offset != NO_DATA;
        vat[i + 1] = vat[i] + (pick[i] && has_poly ? rd<uint32_t>(pick[i] + 20) : 0);
    }
    std::vector<int32_t> verts(2 * vat[n]);
#pragma omp parallel for schedule(dynamic, 4096)
    for (size_t i = 0; i < n; i++) {
        if (!pick[i]) continue;
        pos[2 * i] = rd<int32_t>(pick[i] + 12);
        pos[2 * i + 1] = rd<int32_t>(pick[i] + 16);
        if (vat[i + 1] > vat[i]) std::memcpy(&verts[2 * vat[i]], pick[i] + 24, 8 * (vat[i + 1] - vat[i]));
    }
    write_file(dst + "/addr_points.e7", pos.data(), pos.size() * sizeof(int32_t));
    write_file(dst + "/addr_points.kind", kind.data(), kind.size());
    write_file(dst + "/addr_vertices.e7", verts.data(), verts.size() * sizeof(int32_t));
    write_file(dst + "/addr_vertices.e7.idx", vat.data(), vat.size() * sizeof(uint64_t));
    uint64_t by_kind[4] = {0, 0, 0, 0};
    for (uint8_t k : kind) by_kind[k == 255 ? 3 : k]++;
    report("addr_points", t,
           ", " + std::to_string(polygons.load()) + " outlines re-packed byte-identical; kinds: node " +
               std::to_string(by_kind[0]) + ", building centroid " + std::to_string(by_kind[1]) +
               ", POI-relation centroid " + std::to_string(by_kind[2]) + ", unmatched " + std::to_string(by_kind[3]) +
               "; 32-bit vertex_offset wrapped for " + std::to_string(wrapped) + " outlines");
}

void align_admins(const std::string& uncapped, const std::string& raw_dir, const std::string& dst) {
    const std::vector<uint64_t> keys = sidecar_keys(uncapped + "/admin_polygons.osm_ids");
    Map polys(uncapped + "/admin_polygons.bin"), packed(uncapped + "/admin_vertices.bin"), raw(raw_dir + "/admins.raw");
    const size_t n = polys.count<AdminPolygon>();
    if (n != keys.size()) throw std::runtime_error("admin polygons: sidecar and records differ in count");
    const auto* ap = polys.as<AdminPolygon>();
    const std::vector<Entry> idx = index_dump(raw, [](const uint8_t* p) {
        return 28 + 8 * (size_t(rd<uint32_t>(p + 20)) + rd<uint32_t>(p + 24));
    });
    // Polygon bytes run to the next shipped polygon's offset (or the end).
    std::vector<uint64_t> end(n);
    for (size_t p = n, next = packed.n; p-- > 0;) {
        end[p] = next;
        if (ap[p].vertex_count > 0 && ap[p].vertex_offset != NO_DATA) next = ap[p].vertex_offset;
    }
    std::vector<const uint8_t*> pick(n, nullptr);
    Tally t;
    t.records = n;
    std::atomic<uint64_t> tombstones{0};
#pragma omp parallel for schedule(dynamic, 64)
    for (size_t i = 0; i < n; i++) {
        if (ap[i].vertex_count == 0 || ap[i].vertex_offset == NO_DATA) {
            tombstones++;
            continue;
        }
        auto [lo, hi] = find_all(idx, keys[i]);
        if (lo == hi) {
            t.missing++;
            continue;
        }
        if (hi - lo > 1) t.multi_key++;
        for (const Entry* e = lo; e != hi && !pick[i]; ++e) {
            const uint8_t* r = raw.p + e->off;
            const uint32_t n_raw = rd<uint32_t>(r + 20), n_dp = rd<uint32_t>(r + 24);
            if (n_dp != ap[i].vertex_count || r[8] != ap[i].admin_level) continue;
            if (repacks_to(r + 28 + 8 * size_t(n_raw), n_dp, packed.p + ap[i].vertex_offset, end[i] - ap[i].vertex_offset))
                pick[i] = r;
        }
        if (!pick[i]) {
            t.mismatched++;
            continue;
        }
        t.matched++;
        t.vertices += ap[i].vertex_count;
    }
    // admin_raw.e7 / admin_dp.e7 (i32 pairs) with u64 CSR offsets (n + 1),
    // admin_meta.tsv one row per polygon.
    std::vector<uint64_t> raw_at(n + 1, 0), dp_at(n + 1, 0);
    for (size_t i = 0; i < n; i++) {
        raw_at[i + 1] = raw_at[i] + (pick[i] ? rd<uint32_t>(pick[i] + 20) : 0);
        dp_at[i + 1] = dp_at[i] + (pick[i] ? rd<uint32_t>(pick[i] + 24) : 0);
    }
    std::vector<int32_t> raw_v(2 * raw_at[n]), dp_v(2 * dp_at[n]);
    std::ofstream meta(dst + "/admin_meta.tsv");
    meta << "polygon\tkey\tsource\tosm_id\tring\tadmin_level\tn_raw\tn_dp\tshipped_vertices\tencoding\n";
    for (size_t i = 0; i < n; i++) {
        if (!pick[i]) {
            meta << i << "\t" << keys[i] << "\t-\t-\t-\t" << int(ap[i].admin_level) << "\t0\t0\t" << ap[i].vertex_count
                 << "\t-\n";
            continue;
        }
        const uint8_t* r = pick[i];
        const uint32_t n_raw = rd<uint32_t>(r + 20), n_dp = rd<uint32_t>(r + 24);
        std::memcpy(&raw_v[2 * raw_at[i]], r + 28, 8 * size_t(n_raw));
        std::memcpy(&dp_v[2 * dp_at[i]], r + 28 + 8 * size_t(n_raw), 8 * size_t(n_dp));
        meta << i << "\t" << keys[i] << "\t" << (r[9] == 0 ? "relation" : "way") << "\t" << rd<int64_t>(r + 12) << "\t"
             << rd<uint16_t>(r + 10) << "\t" << int(r[8]) << "\t" << n_raw << "\t" << n_dp << "\t" << ap[i].vertex_count
             << "\t" << int(packed.p[ap[i].vertex_offset]) << "\n";
    }
    write_file(dst + "/admin_raw.e7", raw_v.data(), raw_v.size() * sizeof(int32_t));
    write_file(dst + "/admin_raw.e7.idx", raw_at.data(), raw_at.size() * sizeof(uint64_t));
    write_file(dst + "/admin_dp.e7", dp_v.data(), dp_v.size() * sizeof(int32_t));
    write_file(dst + "/admin_dp.e7.idx", dp_at.data(), dp_at.size() * sizeof(uint64_t));
    report("admin_polygons (quality/uncapped)", t,
           ", " + std::to_string(tombstones.load()) + " tombstones; " + std::to_string(raw_at[n]) +
               " raw ring vertices before the 0.1 m DP");
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr << "usage: precision_align <planet output dir> <raw dump dir> <aligned out dir>" << std::endl;
        return 2;
    }
    const std::string planet = argv[1], raw = argv[2], dst = argv[3];
    try {
        ::mkdir(dst.c_str(), 0755);
        summary.open(dst + "/align_summary.tsv");
        summary << "kind\tshipped_records\tmatched\tmissing\tmismatched\tkeys_with_several_dump_records\tvertices\n";
        align_lines<WayHeader>("street_ways", planet + "/full/street_ways.osm_ids", planet + "/full/street_ways.bin",
                               planet + "/full/street_nodes.bin", raw + "/streets.raw", dst + "/street_nodes.e7");
        align_lines<InterpWay>("interp_ways", planet + "/full/interp_ways.osm_ids", planet + "/full/interp_ways.bin",
                               planet + "/full/interp_nodes.bin", raw + "/interps.raw", dst + "/interp_nodes.e7");
        align_addrs(planet + "/full", raw, dst);
        align_admins(planet + "/quality/uncapped", raw, dst);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
