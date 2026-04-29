#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

struct AdminPolygon {
    uint32_t vertex_offset;
    uint32_t vertex_count;
    uint32_t name_id;
    uint8_t admin_level;
    char _pad1[3];
    float area;
    uint16_t country_code;
    char _pad2[2];
};
static_assert(sizeof(AdminPolygon) == 24);

struct Vert { float lat, lng; };

static double dp_dist(Vert p, Vert a, Vert b) {
    double dx = b.lat - a.lat, dy = b.lng - a.lng;
    double len_sq = dx*dx + dy*dy;
    if (len_sq == 0) {
        double px = p.lat - a.lat, py = p.lng - a.lng;
        return std::sqrt(px*px + py*py);
    }
    double t = std::max(0.0, std::min(1.0, ((p.lat-a.lat)*dx + (p.lng-a.lng)*dy) / len_sq));
    double proj_x = a.lat + t*dx - p.lat;
    double proj_y = a.lng + t*dy - p.lng;
    return std::sqrt(proj_x*proj_x + proj_y*proj_y);
}

static void dp_mark(const std::vector<Vert>& pts, size_t s, size_t e, double eps, std::vector<bool>& keep) {
    if (e <= s + 1) return;
    double max_d = 0; size_t max_i = s;
    for (size_t i = s+1; i < e; i++) {
        double d = dp_dist(pts[i], pts[s], pts[e]);
        if (d > max_d) { max_d = d; max_i = i; }
    }
    if (max_d > eps) {
        keep[max_i] = true;
        dp_mark(pts, s, max_i, eps, keep);
        dp_mark(pts, max_i, e, eps, keep);
    }
}

// Simplify to max N vertices (binary search epsilon)
static std::vector<Vert> simplify_max(const std::vector<Vert>& pts, size_t max_verts) {
    if (pts.size() <= max_verts) return pts;
    double lo = 0, hi = 1.0;
    for (int iter = 0; iter < 20; iter++) {
        double eps = (lo + hi) / 2;
        std::vector<bool> keep(pts.size(), false);
        keep[0] = keep[pts.size()-1] = true;
        dp_mark(pts, 0, pts.size()-1, eps, keep);
        size_t cnt = 0; for (bool k : keep) if (k) cnt++;
        if (cnt > max_verts) lo = eps; else hi = eps;
    }
    std::vector<bool> keep(pts.size(), false);
    keep[0] = keep[pts.size()-1] = true;
    dp_mark(pts, 0, pts.size()-1, hi, keep);
    std::vector<Vert> out;
    for (size_t i = 0; i < pts.size(); i++) if (keep[i]) out.push_back(pts[i]);
    return out;
}

// Simplify with fixed epsilon (degrees)
static std::vector<Vert> simplify_eps(const std::vector<Vert>& pts, double eps_deg) {
    if (pts.size() <= 4) return pts;
    std::vector<bool> keep(pts.size(), false);
    keep[0] = keep[pts.size()-1] = true;
    dp_mark(pts, 0, pts.size()-1, eps_deg, keep);
    std::vector<Vert> out;
    for (size_t i = 0; i < pts.size(); i++) if (keep[i]) out.push_back(pts[i]);
    return out;
}

static double meters_to_deg(double m, double lat) {
    double c = std::cos(lat * M_PI / 180.0);
    if (c < 0.01) c = 0.01;
    return m / (111320.0 * c);
}

static double admin_eps_meters(uint8_t level) {
    switch (level) {
        case 2: return 500; case 3: return 200; case 4: return 100;
        case 5: return 50; case 6: return 30; case 7: return 20;
        default: return 15;
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <index-dir> [--epsilon] [values...]\n", argv[0]);
        fprintf(stderr, "  Default: compare max-vertex modes (500 200 100 50 25)\n");
        fprintf(stderr, "  --epsilon: compare epsilon modes in meters (default per-level, 30, 50, 100)\n");
        return 1;
    }
    std::string dir = argv[1];

    // Load data
    std::vector<char> poly_buf, vert_buf, str_buf;
    auto load = [](const std::string& path, std::vector<char>& buf) {
        std::ifstream f(path, std::ios::binary | std::ios::ate);
        buf.resize(f.tellg()); f.seekg(0); f.read(buf.data(), buf.size());
    };
    load(dir + "/admin_polygons.bin", poly_buf);
    load(dir + "/admin_vertices.bin", vert_buf);
    load(dir + "/strings.bin", str_buf);

    size_t poly_count = poly_buf.size() / sizeof(AdminPolygon);
    auto* polys = reinterpret_cast<AdminPolygon*>(poly_buf.data());
    auto* verts = reinterpret_cast<Vert*>(vert_buf.data());

    std::vector<std::vector<Vert>> poly_verts(poly_count);
    for (size_t i = 0; i < poly_count; i++)
        poly_verts[i].assign(verts + polys[i].vertex_offset,
                             verts + polys[i].vertex_offset + polys[i].vertex_count);

    fprintf(stderr, "Loaded %zu polygons, %zu vertices\n", poly_count, vert_buf.size()/sizeof(Vert));

    // Determine mode
    bool epsilon_mode = false;
    for (int i = 2; i < argc; i++) {
        if (std::string(argv[i]) == "--epsilon") { epsilon_mode = true; break; }
    }

    // Parallel vertex counting helper
    auto count_parallel = [&](auto simplify_fn) -> size_t {
        std::atomic<size_t> total{0}, idx{0};
        unsigned nt = std::thread::hardware_concurrency();
        std::vector<std::thread> threads;
        for (unsigned t = 0; t < nt; t++) {
            threads.emplace_back([&]() {
                size_t local = 0;
                while (true) {
                    size_t i = idx.fetch_add(1);
                    if (i >= poly_count) break;
                    local += simplify_fn(i).size();
                }
                total += local;
            });
        }
        for (auto& t : threads) t.join();
        idx = 0;
        return total.load();
    };

    struct TestResult { std::string label; size_t total_verts; };
    std::vector<TestResult> results;

    if (!epsilon_mode) {
        // Max-vertex modes
        std::vector<size_t> levels = {500, 200, 100, 50, 25};
        printf("%12s  %12s  %10s\n", "Mode", "Total verts", "Vert MiB");
        printf("%12s  %12s  %10s\n", "----", "-----------", "--------");
        for (size_t max_v : levels) {
            auto total = count_parallel([&](size_t i) { return simplify_max(poly_verts[i], max_v); });
            printf("%8zuv max  %12zu  %10.0f\n", max_v, total, total * 8.0/1024/1024);
            char label[64]; snprintf(label, sizeof(label), "max %zu", max_v);
            results.push_back({label, total});
        }
    } else {
        // Epsilon modes
        printf("%16s  %12s  %10s\n", "Mode", "Total verts", "Vert MiB");
        printf("%16s  %12s  %10s\n", "----", "-----------", "--------");

        // Per-level epsilon
        auto total = count_parallel([&](size_t i) {
            double eps_m = admin_eps_meters(polys[i].admin_level);
            double eps_d = meters_to_deg(eps_m, poly_verts[i][0].lat);
            return simplify_eps(poly_verts[i], eps_d);
        });
        printf("%16s  %12zu  %10.0f\n", "per-level", total, total * 8.0/1024/1024);
        results.push_back({"per-level eps", total});

        // Fixed epsilons
        double fixed_eps[] = {10, 20, 30, 50, 100, 200, 500};
        for (double eps_m : fixed_eps) {
            total = count_parallel([&](size_t i) {
                double eps_d = meters_to_deg(eps_m, poly_verts[i][0].lat);
                return simplify_eps(poly_verts[i], eps_d);
            });
            char label[64]; snprintf(label, sizeof(label), "%.0fm fixed", eps_m);
            printf("%16s  %12zu  %10.0f\n", label, total, total * 8.0/1024/1024);
            results.push_back({label, total});
        }

        // Also show max-500 for comparison
        total = count_parallel([&](size_t i) { return simplify_max(poly_verts[i], 500); });
        printf("%16s  %12zu  %10.0f\n", "max 500 (orig)", total, total * 8.0/1024/1024);
    }

    // Export GeoJSON for map comparison
    auto get_name = [&](uint32_t off) -> std::string {
        if (off >= str_buf.size()) return "???";
        return std::string(str_buf.data() + off);
    };

    struct Target { const char* name; double lat, lng, max_dist; };
    Target targets[] = {
        {"Greater London", 51.5, -0.1, 2.0},
        {"Paris", 48.85, 2.35, 0.3},
        {"Berlin", 52.5, 13.4, 1.0},
        {"New York", 40.7, -74.0, 1.0},
        {"Council of the City of Sydney", -33.9, 151.2, 1.0},
        {"Roma Capitale", 41.9, 12.5, 2.0},
        {"Amsterdam", 52.3, 4.9, 0.5},
        {"Madrid", 40.4, -3.7, 0.5},
        {"München", 48.1, 11.5, 0.3},
        {"Île-de-France", 48.5, 2.5, 2.0},
    };

    struct Sample { std::string name; uint8_t level; size_t idx; };
    std::vector<Sample> samples;
    for (auto& t : targets) {
        size_t best_i = 0; size_t best_vc = 0; double best_dist = 999;
        for (size_t i = 0; i < poly_count; i++) {
            if (get_name(polys[i].name_id) != t.name) continue;
            auto& pv = poly_verts[i];
            double clat = 0, clng = 0;
            for (auto& v : pv) { clat += v.lat; clng += v.lng; }
            clat /= pv.size(); clng /= pv.size();
            double dist = std::sqrt((clat-t.lat)*(clat-t.lat) + (clng-t.lng)*(clng-t.lng));
            if (dist < t.max_dist && pv.size() > best_vc) {
                best_vc = pv.size(); best_i = i; best_dist = dist;
            }
        }
        if (best_vc > 0) samples.push_back({t.name, polys[best_i].admin_level, best_i});
    }

    FILE* gf = fopen((dir + "/comparison.geojson").c_str(), "w");
    fprintf(gf, "{\"type\":\"FeatureCollection\",\"features\":[\n");

    // Color palettes
    const char* max_colors[] = {"#e41a1c", "#0066ff", "#00cc44", "#ff00ff", "#ff8800"};
    const char* eps_colors[] = {"#e41a1c", "#0066ff", "#00cc44", "#ff00ff", "#ff8800", "#999999", "#00cccc", "#cc0066"};

    bool first = true;
    for (auto& s : samples) {
        auto& pts = poly_verts[s.idx];
        uint8_t level = s.level;

        if (!epsilon_mode) {
            size_t max_vals[] = {500, 200, 100, 50, 25};
            for (int li = 0; li < 5; li++) {
                auto sp = simplify_max(pts, max_vals[li]);
                if (!first) fprintf(gf, ",\n"); first = false;
                fprintf(gf, "{\"type\":\"Feature\",\"properties\":{\"name\":\"%s\",\"level\":%d,"
                    "\"mode\":\"max %zu\",\"actual_verts\":%zu,\"color\":\"%s\"},\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[",
                    s.name.c_str(), level, max_vals[li], sp.size(), max_colors[li]);
                for (size_t i = 0; i < sp.size(); i++) {
                    if (i) fprintf(gf, ",");
                    fprintf(gf, "[%.6f,%.6f]", sp[i].lng, sp[i].lat);
                }
                if (!sp.empty()) fprintf(gf, ",[%.6f,%.6f]", sp[0].lng, sp[0].lat);
                fprintf(gf, "]]}}");
            }
        } else {
            // Per-level epsilon
            {
                double eps_m = admin_eps_meters(level);
                double eps_d = meters_to_deg(eps_m, pts[0].lat);
                auto sp = simplify_eps(pts, eps_d);
                if (!first) fprintf(gf, ",\n"); first = false;
                char mode[64]; snprintf(mode, sizeof(mode), "per-level (%.0fm)", eps_m);
                fprintf(gf, "{\"type\":\"Feature\",\"properties\":{\"name\":\"%s\",\"level\":%d,"
                    "\"mode\":\"%s\",\"actual_verts\":%zu,\"color\":\"%s\"},\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[",
                    s.name.c_str(), level, mode, sp.size(), eps_colors[0]);
                for (size_t i = 0; i < sp.size(); i++) { if (i) fprintf(gf, ","); fprintf(gf, "[%.6f,%.6f]", sp[i].lng, sp[i].lat); }
                if (!sp.empty()) fprintf(gf, ",[%.6f,%.6f]", sp[0].lng, sp[0].lat);
                fprintf(gf, "]]}}");
            }
            // Fixed epsilons
            double fixed[] = {10, 30, 50, 100, 200};
            for (int li = 0; li < 5; li++) {
                double eps_d = meters_to_deg(fixed[li], pts[0].lat);
                auto sp = simplify_eps(pts, eps_d);
                if (!first) fprintf(gf, ",\n"); first = false;
                char mode[64]; snprintf(mode, sizeof(mode), "%.0fm fixed", fixed[li]);
                fprintf(gf, "{\"type\":\"Feature\",\"properties\":{\"name\":\"%s\",\"level\":%d,"
                    "\"mode\":\"%s\",\"actual_verts\":%zu,\"color\":\"%s\"},\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[",
                    s.name.c_str(), level, mode, sp.size(), eps_colors[li+1]);
                for (size_t i = 0; i < sp.size(); i++) { if (i) fprintf(gf, ","); fprintf(gf, "[%.6f,%.6f]", sp[i].lng, sp[i].lat); }
                if (!sp.empty()) fprintf(gf, ",[%.6f,%.6f]", sp[0].lng, sp[0].lat);
                fprintf(gf, "]]}}");
            }
            // Also max-500 baseline
            {
                auto sp = simplify_max(pts, 500);
                if (!first) fprintf(gf, ",\n"); first = false;
                fprintf(gf, "{\"type\":\"Feature\",\"properties\":{\"name\":\"%s\",\"level\":%d,"
                    "\"mode\":\"max 500 (baseline)\",\"actual_verts\":%zu,\"color\":\"%s\"},\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[",
                    s.name.c_str(), level, sp.size(), "#888888");
                for (size_t i = 0; i < sp.size(); i++) { if (i) fprintf(gf, ","); fprintf(gf, "[%.6f,%.6f]", sp[i].lng, sp[i].lat); }
                if (!sp.empty()) fprintf(gf, ",[%.6f,%.6f]", sp[0].lng, sp[0].lat);
                fprintf(gf, "]]}}");
            }
        }
    }
    fprintf(gf, "\n]}"); fclose(gf);
    fprintf(stderr, "Wrote %s/comparison.geojson (%zu cities)\n", dir.c_str(), samples.size());
    return 0;
}
