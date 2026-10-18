// gc: lossless compact on-disk formats for a published geocoder variant dir (today's format in,
// today's format back out, byte for byte). Single-threaded, streaming, deterministic.
//
//   gc-encode SRC_DIR OUT_DIR [--addr col|pax170] [--only fam,fam...]
//   gc-decode SRC_DIR OUT_DIR [--only fam,fam...]
//   (or: gc encode|decode ...)
//
// Families (each runs when its input files are present): streets, interps, way_parents,
// way_postcodes, interp_postcodes, gxc (geo_cells + the three entries files), addr, admin, poi.
// Every other file is hardlinked (copied across filesystems). OUT_DIR gets gc.manifest (counts and
// parameters the decoder needs). Prints one "phase" line per family: wall and CPU seconds, bytes in/out.
#include "fam_addr.h"
#include "fam_gxc.h"
#include "fam_poly.h"
#include "fam_side.h"
#include "fam_ways.h"

#include <functional>
#include <set>
#include <sys/resource.h>

struct Family {
    std::string name;
    std::vector<std::string> today;    // today's files the family replaces (all must exist to encode)
    std::vector<std::string> needs;    // other today's files it reads (kept as they are)
    std::function<std::vector<std::string>(const Manifest&)> encoded;
    std::function<void(const std::string&, const std::string&, Manifest&)> enc;
    std::function<void(const std::string&, const std::string&, const Manifest&)> dec;
};

static std::vector<Family> families(bool pax) {
    using V = std::vector<std::string>;
    std::vector<Family> f;
    f.push_back({"streets", {"street_ways.bin", "street_nodes.bin"}, {},
                 [](const Manifest&) { return V{"street.groups", "street.nodes2", "street.cnt", "street.code", "street.name"}; },
                 [](auto& s, auto& o, Manifest& m) { ways::encode(s, o, true, m); },
                 [](auto& s, auto& o, const Manifest& m) { ways::decode(s, o, true, m); }});
    f.push_back({"interps", {"interp_ways.bin", "interp_nodes.bin"}, {},
                 [](const Manifest&) {
                     return V{"interp.groups", "interp.nodes2", "interp.cntcode", "interp.street", "interp.start",
                              "interp.span", "interp.interp"};
                 },
                 [](auto& s, auto& o, Manifest& m) { ways::encode(s, o, false, m); },
                 [](auto& s, auto& o, const Manifest& m) { ways::decode(s, o, false, m); }});
    f.push_back({"way_parents", {"way_parents.bin"}, {}, [](const Manifest&) { return V{"way_parents.u24"}; },
                 [](auto& s, auto& o, Manifest& m) { side::encode_u24(s, o, m); },
                 [](auto& s, auto& o, const Manifest& m) { side::decode_u24(s, o, m); }});
    for (const char* n : {"way_postcodes", "interp_postcodes"}) {
        const std::string name = n;
        f.push_back({name, {name + ".bin"}, {}, [name](const Manifest&) { return V{name + ".u16", name + ".dict"}; },
                     [name](auto& s, auto& o, Manifest& m) { side::encode_dict(s, o, name.c_str(), m); },
                     [name](auto& s, auto& o, const Manifest& m) { side::decode_dict(s, o, name.c_str(), m); }});
    }
    f.push_back({"gxc", {"geo_cells.bin", "street_entries.bin", "addr_entries.bin", "interp_entries.bin"}, {},
                 [](const Manifest&) { return V{"gxc_top.bin", "gxc_rows.bin", "gxc_street.bin", "gxc_interp.bin"}; },
                 [](auto& s, auto& o, Manifest& m) { gxc::encode(s, o, m); },
                 [](auto& s, auto& o, const Manifest& m) { gxc::decode(s, o, m); }});
    f.push_back({"addr", {"addr_points.bin", "addr_vertices.bin"}, {},
                 [](const Manifest& m) {
                     return V{m.get("addr.layout") == "pax170" ? "addr_points.pax170" : "addr_points.col", "addr_vertices.abs2"};
                 },
                 [pax](auto& s, auto& o, Manifest& m) { addr::encode(s, o, pax, m); },
                 [](auto& s, auto& o, const Manifest& m) { addr::decode(s, o, m); }});
    f.push_back({"admin", {"admin_vertices.bin"}, {"admin_polygons.bin"}, [](const Manifest&) { return V{"admin_vertices.B"}; },
                 [](auto& s, auto& o, Manifest& m) { poly::encode_admin(s, o, m); },
                 [](auto& s, auto& o, const Manifest& m) { poly::decode_admin(s, o, m); }});
    f.push_back({"poi", {"poi_records.bin", "poi_vertices.bin"}, {},
                 [](const Manifest&) { return V{"poi_records.r3", "poi_vertices.bopen"}; },
                 [](auto& s, auto& o, Manifest& m) { poly::encode_poi(s, o, m); },
                 [](auto& s, auto& o, const Manifest& m) { poly::decode_poi(s, o, m); }});
    return f;
}

static std::set<std::string> split(const std::string& s) {
    std::set<std::string> out;
    size_t a = 0;
    while (a <= s.size()) {
        size_t b = s.find(',', a);
        if (b == std::string::npos) b = s.size();
        if (b > a) out.insert(s.substr(a, b - a));
        a = b + 1;
    }
    return out;
}

static uint64_t sum_sizes(const std::string& dir, const std::vector<std::string>& files) {
    uint64_t t = 0;
    for (auto& f : files) t += file_size(dir + "/" + f);
    return t;
}

static void usage() {
    fprintf(stderr, "usage: gc-encode SRC_DIR OUT_DIR [--addr col|pax170] [--only fam,...]\n"
                    "       gc-decode SRC_DIR OUT_DIR [--only fam,...]\n");
    exit(1);
}

int main(int argc, char** argv) {
    std::string prog = argv[0];
    prog = prog.substr(prog.find_last_of('/') + 1);
    int a = 1;
    bool encode;
    if (prog == "gc-encode") encode = true;
    else if (prog == "gc-decode") encode = false;
    else {
        if (argc < 2) usage();
        const std::string mode = argv[a++];
        if (mode != "encode" && mode != "decode") usage();
        encode = mode == "encode";
    }
    if (argc - a < 2) usage();
    const std::string src = argv[a], out = argv[a + 1];
    bool pax = false;
    std::set<std::string> only;
    for (int i = a + 2; i < argc; i++) {
        const std::string o = argv[i];
        if (o == "--addr" && i + 1 < argc) {
            const std::string v = argv[++i];
            if (v == "pax170") pax = true;
            else if (v != "col") usage();
        } else if (o == "--only" && i + 1 < argc) {
            only = split(argv[++i]);
        } else usage();
    }
    if (mkdir(out.c_str(), 0755) && errno != EEXIST) die("mkdir %s: %s", out.c_str(), strerror(errno));
    const std::vector<Family> fams = families(pax);
    const double t0 = wall_now(), c0 = cpu_now();
    std::set<std::string> consumed;  // files of SRC not to hardlink
    Manifest m;
    if (encode) {
        std::string done;
        for (const Family& f : fams) {
            if (!only.empty() && !only.count(f.name)) continue;
            bool have = true;
            for (auto& x : f.today) have &= exists(src + "/" + x);
            for (auto& x : f.needs) have &= exists(src + "/" + x);
            if (!have) continue;
            const double w = wall_now(), c = cpu_now();
            f.enc(src, out, m);
            const uint64_t in = sum_sizes(src, f.today), outb = sum_sizes(out, f.encoded(m));
            fprintf(stderr, "phase %-16s wall=%.2f cpu=%.2f in=%llu out=%llu\n", f.name.c_str(), wall_now() - w, cpu_now() - c,
                    (unsigned long long)in, (unsigned long long)outb);
            for (auto& x : f.today) consumed.insert(x);
            done += (done.empty() ? "" : ",") + f.name;
        }
        m.set("gc.version", 1);
        m.set("gc.families", done);
        consumed.insert("gc.manifest");
    } else {
        m.load(src + "/gc.manifest");
        const std::set<std::string> enc = split(m.get("gc.families"));
        for (const Family& f : fams) {
            if (!enc.count(f.name) || (!only.empty() && !only.count(f.name))) continue;
            const double w = wall_now(), c = cpu_now();
            f.dec(src, out, m);
            const uint64_t in = sum_sizes(src, f.encoded(m)), outb = sum_sizes(out, f.today);
            fprintf(stderr, "phase %-16s wall=%.2f cpu=%.2f in=%llu out=%llu\n", f.name.c_str(), wall_now() - w, cpu_now() - c,
                    (unsigned long long)in, (unsigned long long)outb);
            for (auto& x : f.encoded(m)) consumed.insert(x);
        }
        consumed.insert("gc.manifest");
    }
    const double w = wall_now(), c = cpu_now();
    uint64_t linked = 0;
    for (const std::string& f : list_files(src)) {
        if (consumed.count(f)) continue;
        link_or_copy(src + "/" + f, out + "/" + f);
        linked++;
    }
    fprintf(stderr, "phase %-16s wall=%.2f cpu=%.2f files=%llu\n", "link", wall_now() - w, cpu_now() - c, (unsigned long long)linked);
    if (encode) m.save(out + "/gc.manifest");
    rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    // gc reads and writes through pread / pwrite buffers (no mmap), so max RSS is its own memory
    fprintf(stderr, "phase %-16s wall=%.2f cpu=%.2f maxrss_kib=%ld\n", "total", wall_now() - t0, cpu_now() - c0, ru.ru_maxrss);
    return 0;
}
