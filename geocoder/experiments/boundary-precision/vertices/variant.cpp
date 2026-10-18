// Lossy variants of an admin-minimal polygon set, written in TODAY's on-disk
// format so the real reader (grid_query) can answer queries against them.
// Uses the builder's own simplify_polygon_epsilon / meters_to_degrees /
// admin_epsilon_meters / polygon_area / plan_polygon / pack_polygon headers.
//
//   variant <src_dir> <out_dir> identity
//   variant <src_dir> <out_dir> floor <metres>   re-simplify rings whose q2.5 eps < metres at that eps
//   variant <src_dir> <out_dir> scale <k>        re-simplify every ring at k x its q2.5 eps
//   variant <src_dir> <out_dir> snap <deg>       snap every vertex to a global <deg> grid
//
// Unchanged polygons keep their original bytes and area. Changed ones are
// repacked like the builder (area from the new double coordinates).
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include "geometry.h"
#include "vertex_pack.h"

static std::vector<uint8_t> slurp(const std::string& p) {
    std::ifstream f(p, std::ios::binary | std::ios::ate);
    if (!f) { std::cerr << "cannot open " << p << "\n"; std::exit(1); }
    std::vector<uint8_t> b(static_cast<size_t>(f.tellg()));
    f.seekg(0); f.read(reinterpret_cast<char*>(b.data()), b.size());
    return b;
}

int main(int argc, char** argv) {
    if (argc < 4) { std::cerr << "usage: variant src out mode [param]\n"; return 1; }
    std::string src = argv[1], out = argv[2], mode = argv[3];
    double param = argc > 4 ? std::atof(argv[4]) : 0;
    auto pb = slurp(src + "/admin_polygons.bin");
    auto vb = slurp(src + "/admin_vertices.bin");
    size_t n = pb.size() / sizeof(AdminPolygon);
    std::vector<AdminPolygon> polys(n);
    std::memcpy(polys.data(), pb.data(), pb.size());

    std::vector<uint8_t> outv; outv.reserve(vb.size());
    size_t changed = 0, v_in = 0, v_out = 0, identical_repack = 0;
    for (size_t i = 0; i < n; i++) {
        AdminPolygon& p = polys[i];
        if (p.vertex_count == 0 || p.vertex_offset == NO_DATA) continue;
        const uint8_t* h = vb.data() + p.vertex_offset;
        uint8_t enc = h[0];
        float bml, bmg; std::memcpy(&bml, h + 2, 4); std::memcpy(&bmg, h + 6, 4);
        size_t stride = VERTEX_STRIDE[enc];
        double scale = enc == 0 ? 1e-5 : enc == 1 ? 1e-4 : enc == 2 ? 1e-6 : 1e-7;
        size_t bytes = POLY_HEADER_BYTES + p.vertex_count * stride;
        std::vector<std::pair<double, double>> pts(p.vertex_count);
        for (uint32_t k = 0; k < p.vertex_count; k++) {
            const uint8_t* q = h + POLY_HEADER_BYTES + k * stride;
            double dl, dg;
            if (enc == 3) { uint32_t a, b; std::memcpy(&a, q, 4); std::memcpy(&b, q + 4, 4); dl = a; dg = b; }
            else { uint16_t a, b; std::memcpy(&a, q, 2); std::memcpy(&b, q + 2, 2); dl = a; dg = b; }
            // exactly the reader's f32 result
            pts[k] = {static_cast<float>(static_cast<double>(bml) + dl * scale),
                      static_cast<float>(static_cast<double>(bmg) + dg * scale)};
        }
        v_in += pts.size();
        double eps_today = admin_epsilon_meters(p.admin_level) * 2.5;
        bool change = false;
        if (mode == "identity") change = true;
        else if (mode == "floor" && param > eps_today) {
            pts = simplify_polygon_epsilon(pts, meters_to_degrees(param, pts[0].first)); change = true;
        } else if (mode == "scale") {
            pts = simplify_polygon_epsilon(pts, meters_to_degrees(eps_today * param, pts[0].first)); change = true;
        } else if (mode == "snap") {
            for (auto& [a, b] : pts) { a = std::round(a / param) * param; b = std::round(b / param) * param; }
            change = true;
        }
        uint32_t new_off = static_cast<uint32_t>(outv.size());
        if (!change || pts.size() < 3) {
            outv.insert(outv.end(), h, h + bytes);
            v_out += p.vertex_count;
        } else {
            PolygonPacking pk = plan_polygon(pts.data(), pts.size());
            size_t at = outv.size(); outv.resize(at + pk.bytes);
            pack_polygon(outv.data() + at, pk, pts.data(), pts.size());
            if (mode == "identity" && pk.bytes == bytes && std::memcmp(outv.data() + at, h, bytes) == 0) identical_repack++;
            else { changed++; p.area = polygon_area(pts); }
            p.vertex_count = static_cast<uint32_t>(pts.size());
            v_out += pts.size();
        }
        p.vertex_offset = new_off;
    }
    std::ofstream(out + "/admin_polygons.bin", std::ios::binary).write(reinterpret_cast<const char*>(polys.data()), n * sizeof(AdminPolygon));
    std::ofstream(out + "/admin_vertices.bin", std::ios::binary).write(reinterpret_cast<const char*>(outv.data()), outv.size());
    std::printf("%s %g: polygons changed %zu, identical repack %zu, vertices %zu -> %zu (%.1f%%), bytes %zu -> %zu\n",
                mode.c_str(), param, changed, identical_repack, v_in, v_out, 100.0 * v_out / v_in, vb.size(), outv.size());
}
