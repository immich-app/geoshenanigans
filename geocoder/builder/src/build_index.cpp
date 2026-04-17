#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <fstream>
#include <future>
#include <iostream>
#include <mutex>
#include <numeric>
#include <optional>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>

#include "pbf_reader.h"

#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>

#include "types.h"
#include "string_pool.h"
#include "geometry.h"
#include "postcode_validation.h"
#include "parsed_data.h"
#include "s2_helpers.h"
#include "ring_assembly.h"
#include "continent_boundaries.h"
#include "interpolation.h"
#include "cache.h"
#include "continent_filter.h"
#include "cell_index.h"


// --- Place type override classification ---

struct PlaceOverride {
    AdminPlaceType type;
    bool tag_present;   // true when any relevant tag (linked_place/border_type/place) was set;
                        // used to block wikidata linking for boundaries that Nominatim
                        // treats as having an authoritative place tag (even if the place
                        // value is state/country/... which we don't translate to an override).
};

static PlaceOverride classify_place_override(const char* linked_place, const char* /*border_type_unused*/, const char* place_tag) {
    // Nominatim's get_label_tag (classtypes.py) only checks
    //   extratags['place']  and  extratags['linked_place']
    // to short-circuit ADMIN_LABELS; `border_type` is kept in extratags
    // but NOT read by the labeller. So for Nominatim-parity we only honour
    // `linked_place` and a direct `place` tag on the boundary.
    const char* val = linked_place ? linked_place : place_tag;
    bool present = (val != nullptr && val[0] != '\0');
    if (!present) return {AdminPlaceType::NONE, false};
    AdminPlaceType t = AdminPlaceType::NONE;
    if (std::strcmp(val, "city") == 0) t = AdminPlaceType::CITY;
    else if (std::strcmp(val, "town") == 0) t = AdminPlaceType::TOWN;
    else if (std::strcmp(val, "village") == 0) t = AdminPlaceType::VILLAGE;
    else if (std::strcmp(val, "hamlet") == 0) t = AdminPlaceType::HAMLET;
    else if (std::strcmp(val, "borough") == 0) t = AdminPlaceType::BOROUGH;
    else if (std::strcmp(val, "suburb") == 0) t = AdminPlaceType::SUBURB;
    else if (std::strcmp(val, "neighbourhood") == 0) t = AdminPlaceType::NEIGHBOURHOOD;
    else if (std::strcmp(val, "quarter") == 0) t = AdminPlaceType::QUARTER;
    else if (std::strcmp(val, "state") == 0) t = AdminPlaceType::STATE;
    else if (std::strcmp(val, "province") == 0) t = AdminPlaceType::PROVINCE;
    else if (std::strcmp(val, "region") == 0) t = AdminPlaceType::REGION;
    else if (std::strcmp(val, "county") == 0) t = AdminPlaceType::COUNTY;
    else if (std::strcmp(val, "district") == 0) t = AdminPlaceType::DISTRICT;
    else if (std::strcmp(val, "municipality") == 0) t = AdminPlaceType::MUNICIPALITY;
    return {t, true};
}

// PlaceType (0=CITY, 1=TOWN, ...) → AdminPlaceType (0=NONE, 1=CITY, 2=TOWN, ...)
// Used when linking an admin boundary to a label-role place node. Without this
// conversion the enums disagree on zero (PlaceType::CITY=0 vs NONE=0) and any
// place=city-linked boundary silently becomes unlinked.
static uint8_t place_type_to_admin_override(uint8_t pt) {
    switch (static_cast<PlaceType>(pt)) {
        case PlaceType::CITY:          return static_cast<uint8_t>(AdminPlaceType::CITY);
        case PlaceType::TOWN:          return static_cast<uint8_t>(AdminPlaceType::TOWN);
        case PlaceType::VILLAGE:       return static_cast<uint8_t>(AdminPlaceType::VILLAGE);
        case PlaceType::SUBURB:        return static_cast<uint8_t>(AdminPlaceType::SUBURB);
        case PlaceType::HAMLET:        return static_cast<uint8_t>(AdminPlaceType::HAMLET);
        case PlaceType::NEIGHBOURHOOD: return static_cast<uint8_t>(AdminPlaceType::NEIGHBOURHOOD);
        case PlaceType::QUARTER:       return static_cast<uint8_t>(AdminPlaceType::QUARTER);
        case PlaceType::BOROUGH:       return static_cast<uint8_t>(AdminPlaceType::BOROUGH);
        // Non-settlement label-role targets. These match Nominatim's
        // get_label_tag() which returns extratags['linked_place'] directly.
        case PlaceType::STATE:         return static_cast<uint8_t>(AdminPlaceType::STATE);
        case PlaceType::PROVINCE:      return static_cast<uint8_t>(AdminPlaceType::PROVINCE);
        case PlaceType::REGION:        return static_cast<uint8_t>(AdminPlaceType::REGION);
        case PlaceType::COUNTY:        return static_cast<uint8_t>(AdminPlaceType::COUNTY);
        case PlaceType::DISTRICT:      return static_cast<uint8_t>(AdminPlaceType::DISTRICT);
        default:                        return static_cast<uint8_t>(AdminPlaceType::NONE);
    }
}

// --- TIGER address data loading ---

static void load_tiger_data(ParsedData& data, const std::string& path) {
    std::cerr << "Loading TIGER address data from " << path << "..." << std::endl;

    // Collect all CSV files
    std::vector<std::string> csv_files;
    if (path.find(".tar.gz") != std::string::npos || path.find(".tgz") != std::string::npos) {
        // Extract tar.gz to a temp directory
        std::string tmpdir = "/tmp/tiger-extract-" + std::to_string(getpid());
        std::string cmd = "mkdir -p " + tmpdir + " && tar xzf " + path + " -C " + tmpdir;
        system(cmd.c_str());
        DIR* dir = opendir(tmpdir.c_str());
        if (dir) {
            struct dirent* ent;
            while ((ent = readdir(dir)) != nullptr) {
                std::string fname = ent->d_name;
                if (fname.size() > 4 && fname.substr(fname.size()-4) == ".csv") {
                    csv_files.push_back(tmpdir + "/" + fname);
                }
            }
            closedir(dir);
        }
    } else {
        // Directory of CSVs
        DIR* dir = opendir(path.c_str());
        if (dir) {
            struct dirent* ent;
            while ((ent = readdir(dir)) != nullptr) {
                std::string fname = ent->d_name;
                if (fname.size() > 4 && fname.substr(fname.size()-4) == ".csv") {
                    csv_files.push_back(path + "/" + fname);
                }
            }
            closedir(dir);
        }
    }

    std::sort(csv_files.begin(), csv_files.end());
    std::cerr << "  Found " << csv_files.size() << " TIGER CSV files" << std::endl;

    uint64_t total_rows = 0;
    uint64_t loaded_rows = 0;

    for (const auto& csv_file : csv_files) {
        std::ifstream f(csv_file);
        if (!f) continue;

        std::string header_line;
        std::getline(f, header_line); // skip header

        std::string line;
        while (std::getline(f, line)) {
            total_rows++;

            // Parse semicolon-delimited: from;to;interpolation;street;city;state;postcode;geometry
            std::vector<std::string> fields;
            size_t pos = 0;
            while (pos < line.size()) {
                size_t next = line.find(';', pos);
                if (next == std::string::npos) next = line.size();
                fields.push_back(line.substr(pos, next - pos));
                pos = next + 1;
            }

            if (fields.size() < 8) continue;

            int from_num = std::atoi(fields[0].c_str());
            int to_num = std::atoi(fields[1].c_str());
            const std::string& interp_type = fields[2];
            const std::string& street = fields[3];
            // fields[4] = city, fields[5] = state, fields[6] = postcode
            const std::string& geometry = fields[7];

            if (from_num <= 0 || to_num <= 0 || street.empty()) continue;
            if (geometry.find("LINESTRING") == std::string::npos) continue;

            // Parse WKT LINESTRING(lng lat, lng lat, ...)
            // Note: WKT uses lng lat order — we swap to our lat/lng convention
            size_t paren_start = geometry.find('(');
            size_t paren_end = geometry.rfind(')');
            if (paren_start == std::string::npos || paren_end == std::string::npos) continue;

            std::string coords_str = geometry.substr(paren_start + 1, paren_end - paren_start - 1);
            std::vector<NodeCoord> nodes;

            size_t cpos = 0;
            while (cpos < coords_str.size()) {
                // Skip whitespace and commas
                while (cpos < coords_str.size() && (coords_str[cpos] == ' ' || coords_str[cpos] == ','))
                    cpos++;
                if (cpos >= coords_str.size()) break;

                char* end;
                double lng = std::strtod(coords_str.c_str() + cpos, &end);
                cpos = end - coords_str.c_str();
                while (cpos < coords_str.size() && coords_str[cpos] == ' ') cpos++;
                double lat = std::strtod(coords_str.c_str() + cpos, &end);
                cpos = end - coords_str.c_str();

                if (lat != 0 || lng != 0) {
                    nodes.push_back({static_cast<float>(lat), static_cast<float>(lng)});
                }
            }

            if (nodes.size() < 2 || nodes.size() > 255) continue;

            // Create InterpWay
            uint32_t node_offset = static_cast<uint32_t>(data.interp_nodes.size());
            for (const auto& n : nodes) data.interp_nodes.push_back(n);

            uint8_t itype = 0; // all
            if (interp_type == "even") itype = 1;
            else if (interp_type == "odd") itype = 2;

            InterpWay iw{};
            iw.node_offset = node_offset;
            iw.node_count = static_cast<uint8_t>(nodes.size());
            iw.street_id = data.string_pool.intern(street);
            iw.start_number = static_cast<uint32_t>(std::min(from_num, to_num));
            iw.end_number = static_cast<uint32_t>(std::max(from_num, to_num));
            iw.interpolation = itype;

            // Defer S2 computation
            uint32_t interp_id = static_cast<uint32_t>(data.interp_ways.size());
            data.interp_ways.push_back(iw);
            data.deferred_interps.push_back({interp_id, node_offset, iw.node_count});

            // Accumulate TIGER postcode centroids — TIGER has excellent
            // US zip code coverage that OSM addr:postcode mostly lacks.
            // Nominatim imports TIGER the same way and postcodes feed
            // into location_postcode.
            const std::string& postcode = fields[6];
            if (!postcode.empty() && is_valid_postcode(postcode.c_str())) {
                uint32_t pc_id = data.string_pool.intern(postcode);
                // Use the midpoint of the interpolation segment as the
                // postcode location (centroid of the segment).
                double mid_lat = 0, mid_lng = 0;
                for (const auto& n : nodes) { mid_lat += n.lat; mid_lng += n.lng; }
                mid_lat /= nodes.size(); mid_lng /= nodes.size();
                auto& acc = data.postcode_accum[pc_id];
                acc.sum_lat += mid_lat;
                acc.sum_lng += mid_lng;
                acc.count++;
            }

            loaded_rows++;
        }
    }

    std::cerr << "  TIGER: loaded " << loaded_rows << "/" << total_rows << " address ranges from "
              << csv_files.size() << " files" << std::endl;
    std::cerr << "  Interp ways now: " << data.interp_ways.size()
              << " (" << loaded_rows << " from TIGER)" << std::endl;

    // Create place nodes from TIGER city names (postal city coverage)
    // For each unique city name, compute centroid from all TIGER entries with that name
    struct CityAccum { double sum_lat = 0, sum_lng = 0; uint64_t count = 0; };
    std::unordered_map<std::string, CityAccum> city_centroids;

    // Re-read CSV files to collect city centroids (lightweight pass)
    for (const auto& csv_file : csv_files) {
        std::ifstream f(csv_file);
        if (!f) continue;
        std::string header_line;
        std::getline(f, header_line);
        std::string line;
        while (std::getline(f, line)) {
            std::vector<std::string> fields;
            size_t pos = 0;
            while (pos < line.size()) {
                size_t next = line.find(';', pos);
                if (next == std::string::npos) next = line.size();
                fields.push_back(line.substr(pos, next - pos));
                pos = next + 1;
            }
            if (fields.size() < 8) continue;
            const std::string& city = fields[4];
            const std::string& geometry = fields[7];
            if (city.empty() || geometry.find("LINESTRING") == std::string::npos) continue;

            // Get first coordinate as a rough centroid
            size_t paren = geometry.find('(');
            if (paren == std::string::npos) continue;
            char* end;
            double lng = std::strtod(geometry.c_str() + paren + 1, &end);
            double lat = std::strtod(end, &end);
            if (lat == 0 && lng == 0) continue;

            auto& acc = city_centroids[city];
            acc.sum_lat += lat;
            acc.sum_lng += lng;
            acc.count++;
        }
    }

    // Create place=town nodes for each TIGER city
    uint64_t tiger_places = 0;
    for (const auto& [city_name, acc] : city_centroids) {
        if (acc.count < 5) continue; // skip very sparse cities
        PlaceNode pn{};
        pn.lat = static_cast<float>(acc.sum_lat / acc.count);
        pn.lng = static_cast<float>(acc.sum_lng / acc.count);
        pn.name_id = data.string_pool.intern(city_name);
        pn.place_type = static_cast<uint8_t>(PlaceType::TOWN); // TIGER cities as towns
        data.place_nodes.push_back(pn);
        tiger_places++;
    }
    std::cerr << "  TIGER: created " << tiger_places << " place nodes from "
              << city_centroids.size() << " unique city names" << std::endl;
}

// --- GeoNames postcode loading ---
// Format: CSV with header "postcode,lat,lon,country_code"
// Source: GeoNames allCountries + GB_full + CA_full + NL_full
// Merged into postcode_accum as authoritative centroids that
// override OSM-derived ones (matching Nominatim's _update_from_external).
static void load_external_postcodes(ParsedData& data, const std::string& path) {
    std::cerr << "Loading external postcode centroids from " << path << "..." << std::endl;
    std::string cmd;
    std::string tmp_csv;
    if (path.find(".gz") != std::string::npos) {
        tmp_csv = "/tmp/external_postcodes_" + std::to_string(getpid()) + ".csv";
        cmd = "gunzip -c " + path + " > " + tmp_csv;
        system(cmd.c_str());
    } else {
        tmp_csv = path;
    }

    std::ifstream f(tmp_csv);
    if (!f) { std::cerr << "  Failed to open " << tmp_csv << std::endl; return; }

    std::string header;
    std::getline(f, header); // skip header

    uint64_t loaded = 0, skipped = 0;
    std::string line;
    while (std::getline(f, line)) {
        // Parse: postcode,lat,lon,country_code
        size_t p1 = line.find(',');
        if (p1 == std::string::npos) continue;
        size_t p2 = line.find(',', p1 + 1);
        if (p2 == std::string::npos) continue;
        size_t p3 = line.find(',', p2 + 1);
        if (p3 == std::string::npos) continue;

        std::string postcode = line.substr(0, p1);
        double lat = std::strtod(line.c_str() + p1 + 1, nullptr);
        double lng = std::strtod(line.c_str() + p2 + 1, nullptr);
        std::string cc_str = line.substr(p3 + 1, 2);

        if (postcode.empty() || cc_str.size() < 2) continue;
        if (lat == 0 && lng == 0) continue;

        // Validate against country pattern
        char cc_lower[3] = {
            static_cast<char>(std::tolower(cc_str[0])),
            static_cast<char>(std::tolower(cc_str[1])),
            0
        };
        if (!validate_postcode_for_country(cc_lower, postcode.c_str())) {
            skipped++;
            continue;
        }

        // GeoNames entries are authoritative — they override OSM-derived
        // centroids. Nominatim's _update_from_external adds external
        // postcodes only when NOT already present from OSM. We do the
        // same: only add if no OSM entry exists for this postcode.
        uint32_t pc_id = data.string_pool.intern(postcode);
        auto it = data.postcode_accum.find(pc_id);
        if (it == data.postcode_accum.end()) {
            auto& acc = data.postcode_accum[pc_id];
            acc.sum_lat = lat;
            acc.sum_lng = lng;
            acc.count = 1;
            loaded++;
        }
    }

    if (tmp_csv != path) std::remove(tmp_csv.c_str());
    std::cerr << "  GeoNames: loaded " << loaded << " new postcodes, "
              << skipped << " rejected by pattern, "
              << data.postcode_accum.size() << " total centroids" << std::endl;
}

// --- Main ---

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: build-index <output-dir> <input.osm.pbf> [options]" << std::endl;
        std::cerr << "       build-index <output-dir> --load-cache <path> [options]" << std::endl;
        std::cerr << std::endl;
        std::cerr << "Options:" << std::endl;
        std::cerr << "  --street-level N       S2 cell level for streets (default: 17)" << std::endl;
        std::cerr << "  --admin-level N        S2 cell level for admin (default: 10)" << std::endl;
        std::cerr << "  --max-admin-level N    Only include admin levels <= N" << std::endl;
        std::cerr << "  --save-cache <path>    Save parsed data to cache file" << std::endl;
        std::cerr << "  --load-cache <path>    Load from cache instead of PBF" << std::endl;
        std::cerr << "  --multi-output         Write full, no-addresses, admin-only indexes" << std::endl;
        std::cerr << "  --continents           Also generate per-continent indexes" << std::endl;
        std::cerr << "  --mode <mode>          Index mode: full, no-addresses, admin-only (default: full)" << std::endl;
        std::cerr << "  --admin-only           Shorthand for --mode admin-only" << std::endl;
        std::cerr << "  --no-addresses         Shorthand for --mode no-addresses" << std::endl;
        std::cerr << "  --simplify-epsilon     Use error-bounded simplification (per-level defaults)" << std::endl;
        std::cerr << "  --simplify-epsilon N   Use fixed epsilon of N meters for all levels" << std::endl;
        std::cerr << "  --epsilon-scale F      Multiply all per-level epsilons by F (e.g. 2.0 = 2x coarser)" << std::endl;
        std::cerr << "  --epsilon-levels L2,L3,...,L8  Set epsilon in meters per level (7 values)" << std::endl;
        std::cerr << "  --multi-quality [S,S,...]  Write quality variants at given scales (default: 0,0.2,0.5,1,1.5,2,2.5)" << std::endl;
        std::cerr << "  --tiger-data <path>          Load TIGER address data (tar.gz or directory of CSVs)" << std::endl;
        std::cerr << "  --external-postcodes <path>  Load GeoNames postcode centroids (CSV or .csv.gz)" << std::endl;
        std::cerr << "  --wikidata-sitelinks <path>  Load QID→sitelinks binary for POI importance" << std::endl;
        return 1;
    }

    std::string output_dir = argv[1];
    std::vector<std::string> input_files;
    std::string save_cache_path;
    std::string load_cache_path;
    std::string wikidata_sitelinks_path;
    std::string tiger_data_path;
    std::string external_postcodes_path;
    bool multi_output = false;
    bool generate_continents = false;
    IndexMode mode = IndexMode::Full;
    SimplifyMode simplify_mode = SimplifyMode::MaxVertices;
    double simplify_epsilon_override = 0; // 0 = use per-level defaults
    bool multi_quality = false;
    std::vector<double> quality_scales;

    for (int i = 2; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--street-level" && i + 1 < argc) {
            kStreetCellLevel = std::atoi(argv[++i]);
        } else if (arg == "--admin-level" && i + 1 < argc) {
            kAdminCellLevel = std::atoi(argv[++i]);
        } else if (arg == "--admin-only") {
            mode = IndexMode::AdminOnly;
        } else if (arg == "--no-addresses") {
            mode = IndexMode::NoAddresses;
        } else if (arg == "--max-admin-level" && i + 1 < argc) {
            kMaxAdminLevel = std::atoi(argv[++i]);
        } else if (arg == "--save-cache" && i + 1 < argc) {
            save_cache_path = argv[++i];
        } else if (arg == "--load-cache" && i + 1 < argc) {
            load_cache_path = argv[++i];
        } else if (arg == "--multi-output") {
            multi_output = true;
        } else if (arg == "--continents") {
            generate_continents = true;
        } else if (arg == "--simplify-epsilon") {
            simplify_mode = SimplifyMode::ErrorBounded;
            // Optional: next arg might be a number (fixed epsilon in meters)
            if (i + 1 < argc) {
                char* end;
                double val = std::strtod(argv[i+1], &end);
                if (*end == '\0' && val > 0) {
                    simplify_epsilon_override = val;
                    i++;
                }
            }
        } else if (arg == "--epsilon-scale" && i + 1 < argc) {
            kEpsilonScale = std::strtod(argv[++i], nullptr);
        } else if (arg == "--multi-quality") {
            multi_quality = true;
            // Optional: next arg might be comma-separated scales
            if (i + 1 < argc && argv[i+1][0] != '-') {
                std::string vals = argv[++i];
                size_t pos = 0;
                while (pos < vals.size()) {
                    size_t next = vals.find(',', pos);
                    if (next == std::string::npos) next = vals.size();
                    double v = std::strtod(vals.substr(pos, next - pos).c_str(), nullptr);
                    quality_scales.push_back(v);
                    pos = next + 1;
                }
            }
            if (quality_scales.empty()) {
                quality_scales = {0, 0.2, 0.5, 1.0, 1.5, 2.0, 2.5};
            }
            // Build with uncapped polygons — quality variants simplify at write time.
            // We need full vertex detail to produce all quality levels accurately.
            simplify_mode = SimplifyMode::ErrorBounded;
            simplify_epsilon_override = 0.1; // effectively uncapped
        } else if (arg == "--epsilon-levels" && i + 1 < argc) {
            // Parse comma-separated values: L2,L3,L4,L5,L6,L7,L8
            std::string vals = argv[++i];
            int level = 2;
            size_t pos = 0;
            while (pos < vals.size() && level <= 8) {
                size_t next = vals.find(',', pos);
                if (next == std::string::npos) next = vals.size();
                double v = std::strtod(vals.substr(pos, next - pos).c_str(), nullptr);
                if (v > 0) kAdminEpsilonMeters[level] = v;
                level++;
                pos = next + 1;
            }
            // Also set levels 9-11 to same as level 8
            for (int l = level; l <= 11; l++) kAdminEpsilonMeters[l] = kAdminEpsilonMeters[8];
        } else if (arg == "--mode" && i + 1 < argc) {
            std::string mode_str = argv[++i];
            if (mode_str == "full") {
                mode = IndexMode::Full;
            } else if (mode_str == "no-addresses") {
                mode = IndexMode::NoAddresses;
            } else if (mode_str == "admin-only") {
                mode = IndexMode::AdminOnly;
            } else {
                std::cerr << "Error: unknown mode '" << mode_str << "'" << std::endl;
                return 1;
            }
        } else if (arg == "--wikidata-sitelinks" && i + 1 < argc) {
            wikidata_sitelinks_path = argv[++i];
        } else if (arg == "--tiger-data" && i + 1 < argc) {
            tiger_data_path = argv[++i];
        } else if (arg == "--external-postcodes" && i + 1 < argc) {
            external_postcodes_path = argv[++i];
        } else {
            input_files.push_back(arg);
        }
    }

    // Set global simplification mode
    kSimplifyMode = simplify_mode;
    kSimplifyEpsilonOverride = simplify_epsilon_override;
    if (simplify_mode == SimplifyMode::ErrorBounded) {
        if (simplify_epsilon_override > 0) {
            std::cerr << "Simplification: error-bounded, fixed " << simplify_epsilon_override << "m epsilon" << std::endl;
        } else {
            std::cerr << "Simplification: error-bounded, scale=" << kEpsilonScale
                      << ", epsilons: L2=" << admin_epsilon_meters(2)
                      << "m L4=" << admin_epsilon_meters(4)
                      << "m L6=" << admin_epsilon_meters(6)
                      << "m L8=" << admin_epsilon_meters(8) << "m" << std::endl;
        }
    }

    ParsedData data;
    auto _pt = std::chrono::steady_clock::now();
    auto _cpu = CpuTicks::now();

    std::vector<float> poi_elevations; // build-time only, not in ParsedData
    std::vector<uint32_t> poi_qids; // build-time only: wikidata QID numbers

    // Load wikidata sitelinks data (QID → sitelinks count)
    struct QidSitelinks { uint32_t qid; uint16_t count; };
    std::vector<QidSitelinks> sitelinks_data;
    if (!wikidata_sitelinks_path.empty()) {
        std::ifstream sf(wikidata_sitelinks_path, std::ios::binary | std::ios::ate);
        if (sf) {
            size_t sz = sf.tellg();
            sf.seekg(0);
            sitelinks_data.resize(sz / sizeof(QidSitelinks));
            sf.read(reinterpret_cast<char*>(sitelinks_data.data()), sz);
            std::cerr << "Loaded " << sitelinks_data.size() << " wikidata sitelinks entries." << std::endl;
        }
    }

    // Helper to look up sitelinks count by QID (binary search since data is sorted)
    auto lookup_sitelinks = [&sitelinks_data](uint32_t qid) -> uint16_t {
        auto it = std::lower_bound(sitelinks_data.begin(), sitelinks_data.end(), qid,
            [](const QidSitelinks& entry, uint32_t q) { return entry.qid < q; });
        if (it != sitelinks_data.end() && it->qid == qid) return it->count;
        return 0;
    };

    if (!load_cache_path.empty()) {
        // Load from cache
        if (!deserialize_cache(data, load_cache_path)) {
            std::cerr << "Error: failed to load cache" << std::endl;
            return 1;
        }
    } else {
        // Parse PBF files (always collect everything)
        if (input_files.empty()) {
            std::cerr << "Error: no input files specified and no --load-cache" << std::endl;
            return 1;
        }

        // Create thread pool for concurrent admin polygon S2 covering
        unsigned int num_threads = std::max(1u, std::thread::hardware_concurrency() - 1);
        std::cerr << "Using " << num_threads << " worker threads." << std::endl;
        AdminCoverPool admin_pool(num_threads);
        // BuildHandler no longer used — parallel processing handles everything

        // Dense array node location index — lockless parallel writes
        // Planet OSM node IDs max ~12.5 billion. 8 bytes per entry = 100GB virtual.
        // MAP_NORESERVE means OS only allocates pages on write (~80GB for 10B nodes).
        static const size_t MAX_NODE_ID = MAX_NODE_ID_DEFAULT;
        struct PackedLocation {
            int32_t lat_e7;  // latitude * 10^7
            int32_t lon_e7;  // longitude * 10^7
            bool valid() const { return lat_e7 != 0 || lon_e7 != 0; }
            double lat() const { return lat_e7 / 10000000.0; }
            double lon() const { return lon_e7 / 10000000.0; }
        };
        struct DenseIndex {
            PackedLocation* data;
            size_t capacity;

            DenseIndex() : capacity(MAX_NODE_ID) {
                size_t byte_size = capacity * sizeof(PackedLocation);
                void* ptr = mmap(nullptr, byte_size,
                    PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
                    -1, 0);
                if (ptr == MAP_FAILED) {
                    std::cerr << "Error: failed to mmap dense node index ("
                              << (byte_size / (1024*1024*1024)) << "GB)" << std::endl;
                    std::exit(1);
                }
                madvise(ptr, byte_size, MADV_HUGEPAGE);
                data = static_cast<PackedLocation*>(ptr);
                std::cerr << "Allocated dense node index: " << (byte_size / (1024*1024*1024))
                          << "GB virtual address space" << std::endl;
            }

            ~DenseIndex() {
                munmap(data, capacity * sizeof(PackedLocation));
            }

            // Lockless — each node ID maps to a unique array slot
            void set(uint64_t id, double lat, double lng) {
                if (id >= capacity) return;
                data[id] = {static_cast<int32_t>(lat * 10000000.0 + (lat >= 0 ? 0.5 : -0.5)),
                            static_cast<int32_t>(lng * 10000000.0 + (lng >= 0 ? 0.5 : -0.5))};
            }

            PackedLocation get(uint64_t id) const {
                if (id >= capacity) return {0, 0};
                return data[id];
            }
        } index;

        for (const auto& input_file : input_files) {
            std::cerr << "Processing " << input_file << "..." << std::endl;

            // --- Pass 1: collect relation members for parallel admin assembly ---
            std::cerr << "  Pass 1: scanning relations..." << std::endl;
            PbfFile pbf(input_file, num_threads);

            std::vector<std::string> rel_wikidata; // parallel to data.collected_relations
            std::unordered_set<int64_t> wanted_label_nodes; // node IDs referenced as role=label
            {
                std::mutex rel_mutex;
                pbf.read_blocks([&](PbfBlock& block, unsigned) {
                    for (auto& rel : block.relations) {
                        const char* boundary = rel.tag("boundary");
                        const char* rel_type = rel.tag("type");
                        const char* rel_place = rel.tag("place");

                        // type=multipolygon + place=city/town/… relations (e.g. Moscow
                        // R2555133) are distinct placex rows in Nominatim at the
                        // place type's rank. They exist alongside any matching
                        // boundary=administrative relation at a different rank.
                        bool is_multipolygon_place =
                            !boundary && rel_type && rel_place &&
                            std::strcmp(rel_type, "multipolygon") == 0 &&
                            classify_place_override(nullptr, nullptr, rel_place).type != AdminPlaceType::NONE;

                        // boundary=census / boundary=census_district relations
                        // carrying a postal_code tag — captured for build-time
                        // postcode inheritance (mirrors Nominatim's
                        // calculated_postcode propagation). These do NOT become
                        // admin polygons; they're a transient helper discarded
                        // after the inheritance pass.
                        const char* census_postcode = nullptr;
                        bool is_census_with_postcode = false;
                        if (boundary && (std::strcmp(boundary, "census") == 0 ||
                                         std::strcmp(boundary, "census_district") == 0)) {
                            census_postcode = rel.tag("postal_code");
                            if (!census_postcode) census_postcode = rel.tag("addr:postcode");
                            if (census_postcode && *census_postcode) {
                                is_census_with_postcode = true;
                            }
                        }

                        if (is_census_with_postcode) {
                            CdpPostcodeRelation cpr;
                            cpr.id = rel.id;
                            cpr.postcode = census_postcode;
                            for (size_t mi = 0; mi < rel.members.size(); mi++) {
                                if (rel.members[mi].type == 'w') {
                                    cpr.members.emplace_back(rel.members[mi].ref, rel.member_role(mi));
                                }
                            }
                            if (!cpr.members.empty()) {
                                std::lock_guard<std::mutex> lock(rel_mutex);
                                data.cdp_postcode_relations.push_back(std::move(cpr));
                            }
                            continue;
                        }

                        if (!boundary && !is_multipolygon_place) continue;

                        bool is_admin = boundary && std::strcmp(boundary, "administrative") == 0;
                        bool is_postal = boundary && std::strcmp(boundary, "postal_code") == 0;
                        bool is_place_boundary = boundary && std::strcmp(boundary, "place") == 0;
                        if (!is_admin && !is_postal && !is_place_boundary && !is_multipolygon_place) continue;

                        // Capture label member node (Nominatim's primary place-link lookup)
                        int64_t label_node_id = -1;
                        for (size_t mi = 0; mi < rel.members.size(); mi++) {
                            if (rel.members[mi].type == 'n' && rel.member_role(mi) == "label") {
                                label_node_id = rel.members[mi].ref;
                                break;
                            }
                        }

                        // Handle boundary=place (e.g., Washington DC) and
                        // type=multipolygon+place=* (e.g., Moscow R2555133).
                        if (is_place_boundary || is_multipolygon_place) {
                            const char* place_tag = rel_place;
                            if (place_tag) {
                                AdminPlaceType pt = classify_place_override(nullptr, nullptr, place_tag).type;
                                if (pt != AdminPlaceType::NONE) {
                                    const char* name = rel.tag("name:en");
                                    if (!name) name = rel.tag("name");
                                    if (name) {
                                        CollectedRelation cr;
                                        cr.id = rel.id;
                                        cr.label_node_id = label_node_id;
                                        cr.admin_level = 15;  // special marker for boundary=place / place multipolygon
                                        cr.name = name;
                                        cr.is_postal = false;
                                        cr.fallback_place_type = static_cast<uint8_t>(pt);
                                        for (size_t mi = 0; mi < rel.members.size(); mi++) {
                                            if (rel.members[mi].type == 'w') {
                                                cr.members.emplace_back(rel.members[mi].ref, rel.member_role(mi));
                                            }
                                        }
                                        if (!cr.members.empty()) {
                                            std::lock_guard<std::mutex> lock(rel_mutex);
                                            data.collected_relations.push_back(std::move(cr));
                                            rel_wikidata.emplace_back(); // already has override, no wikidata needed
                                            if (label_node_id >= 0) wanted_label_nodes.insert(label_node_id);
                                        }
                                    }
                                }
                            }
                            continue;
                        }

                        uint8_t admin_level = 0;
                        if (is_admin) {
                            const char* level_str = rel.tag("admin_level");
                            if (!level_str) continue;
                            admin_level = static_cast<uint8_t>(std::atoi(level_str));
                            if (admin_level < 2 || admin_level > 10) continue;
                        } else {
                            admin_level = 11;
                        }
                        if (kMaxAdminLevel > 0 && admin_level > kMaxAdminLevel) continue;

                        const char* name_en = rel.tag("name:en");
                        const char* name = rel.tag("name");
                        if (!name_en) name_en = name;
                        if (!name_en && is_admin) continue;

                        std::string name_str;
                        if (is_postal) {
                            const char* postal_code = rel.tag("postal_code");
                            if (!postal_code) postal_code = name_en;
                            if (!postal_code) continue;
                            name_str = postal_code;
                        } else {
                            name_str = name_en;
                        }

                        std::string country_code;
                        if (admin_level == 2) {
                            const char* cc = rel.tag("ISO3166-1:alpha2");
                            if (cc) country_code = cc;
                        }

                        const char* linked_place = rel.tag("linked_place");
                        const char* border_type_tag = rel.tag("border_type");
                        const char* wikidata_tag = rel.tag("wikidata");
                        // place=* tag on boundary=administrative relations (e.g. Tokyo
                        // has place=province, Suginami has place=city). Nominatim's
                        // get_label_tag prefers extratags['place'] over the admin_level
                        // → rank label, so we honour it as a place_type_override.
                        const char* place_tag_on_admin = rel.tag("place");

                        auto po = classify_place_override(linked_place, border_type_tag, place_tag_on_admin);
                        CollectedRelation cr;
                        cr.id = rel.id;
                        cr.label_node_id = label_node_id;
                        cr.admin_level = admin_level;
                        cr.name = std::move(name_str);
                        cr.country_code = std::move(country_code);
                        cr.is_postal = is_postal;
                        cr.fallback_place_type = static_cast<uint8_t>(po.type);

                        for (size_t mi = 0; mi < rel.members.size(); mi++) {
                            if (rel.members[mi].type == 'w') {
                                cr.members.emplace_back(rel.members[mi].ref, rel.member_role(mi));
                            }
                        }

                        if (!cr.members.empty()) {
                            // Always store wikidata for relations. Nominatim's
                            // find_linked_place (placex_triggers.sql) goes:
                            // label role → wikidata → place type match → name
                            // match. The wikidata step runs regardless of the
                            // place=* tag on the boundary, and this is what
                            // lets Tokyo (place=province + wikidata=Q1490) link
                            // to its place=city admin_centre node and drop out
                            // of the address hierarchy after Suginami wins.
                            std::string wd_str = wikidata_tag ? wikidata_tag : "";
                            std::lock_guard<std::mutex> lock(rel_mutex);
                            data.collected_relations.push_back(std::move(cr));
                            rel_wikidata.push_back(std::move(wd_str));
                            if (label_node_id >= 0) wanted_label_nodes.insert(label_node_id);
                        }
                    }
                    // --- POI relation scanning (same pass) ---
                    for (auto& rel : block.relations) {
                        const char* r_boundary = rel.tag("boundary");
                        const char* r_leisure  = rel.tag("leisure");
                        const char* r_tourism  = rel.tag("tourism");
                        const char* r_natural  = rel.tag("natural");
                        const char* r_aeroway  = rel.tag("aeroway");
                        const char* r_place    = rel.tag("place");
                        const char* r_office   = rel.tag("office");

                        PoiCategory poi_cat = PoiCategory::UNKNOWN;
                        // boundary
                        if (r_boundary) {
                            if (std::strcmp(r_boundary, "national_park") == 0) poi_cat = PoiCategory::NATIONAL_PARK;
                            else if (std::strcmp(r_boundary, "protected_area") == 0) poi_cat = PoiCategory::PROTECTED_AREA;
                        }
                        // leisure
                        if (poi_cat == PoiCategory::UNKNOWN && r_leisure) {
                            if (std::strcmp(r_leisure, "park") == 0) poi_cat = PoiCategory::PARK;
                            else if (std::strcmp(r_leisure, "nature_reserve") == 0) poi_cat = PoiCategory::NATURE_RESERVE;
                            else if (std::strcmp(r_leisure, "stadium") == 0) poi_cat = PoiCategory::STADIUM;
                            else if (std::strcmp(r_leisure, "water_park") == 0) poi_cat = PoiCategory::WATER_PARK;
                            else if (std::strcmp(r_leisure, "golf_course") == 0) poi_cat = PoiCategory::GOLF_COURSE;
                            else if (std::strcmp(r_leisure, "garden") == 0) poi_cat = PoiCategory::GARDEN;
                            else if (std::strcmp(r_leisure, "marina") == 0) poi_cat = PoiCategory::MARINA;
                        }
                        // tourism
                        if (poi_cat == PoiCategory::UNKNOWN && r_tourism) {
                            if (std::strcmp(r_tourism, "theme_park") == 0) poi_cat = PoiCategory::THEME_PARK;
                            else if (std::strcmp(r_tourism, "zoo") == 0) poi_cat = PoiCategory::ZOO;
                            else if (std::strcmp(r_tourism, "museum") == 0) poi_cat = PoiCategory::MUSEUM;
                            else if (std::strcmp(r_tourism, "attraction") == 0) poi_cat = PoiCategory::ATTRACTION;
                            else if (std::strcmp(r_tourism, "aquarium") == 0) poi_cat = PoiCategory::AQUARIUM;
                            else if (std::strcmp(r_tourism, "resort") == 0) poi_cat = PoiCategory::RESORT;
                        }
                        // natural
                        if (poi_cat == PoiCategory::UNKNOWN && r_natural) {
                            if (std::strcmp(r_natural, "bay") == 0) poi_cat = PoiCategory::BAY;
                            else if (std::strcmp(r_natural, "island") == 0) poi_cat = PoiCategory::ISLAND;
                            else if (std::strcmp(r_natural, "beach") == 0) poi_cat = PoiCategory::BEACH;
                            else if (std::strcmp(r_natural, "glacier") == 0) poi_cat = PoiCategory::GLACIER;
                            else if (std::strcmp(r_natural, "volcano") == 0) poi_cat = PoiCategory::VOLCANO;
                        }
                        // aeroway
                        if (poi_cat == PoiCategory::UNKNOWN && r_aeroway) {
                            if (std::strcmp(r_aeroway, "aerodrome") == 0) poi_cat = PoiCategory::AERODROME;
                        }
                        // place
                        if (poi_cat == PoiCategory::UNKNOWN && r_place) {
                            if (std::strcmp(r_place, "island") == 0 || std::strcmp(r_place, "islet") == 0)
                                poi_cat = PoiCategory::ISLAND;
                        }
                        // office
                        if (poi_cat == PoiCategory::UNKNOWN && r_office) {
                            if (std::strcmp(r_office, "government") == 0) poi_cat = PoiCategory::GOVERNMENT;
                        }

                        if (poi_cat != PoiCategory::UNKNOWN) {
                            const char* poi_name = rel.tag("name:en");
                            if (!poi_name) poi_name = rel.tag("name");
                            if (poi_name) {
                                uint8_t flags = 0;
                                if (rel.tag("wikipedia")) flags |= POI_FLAG_WIKIPEDIA;
                                if (rel.tag("wikidata"))  flags |= POI_FLAG_WIKIDATA;
                                uint8_t tier = poi_get_default_tier(poi_cat);
                                if ((flags & (POI_FLAG_WIKIPEDIA | POI_FLAG_WIKIDATA)) && tier > 1) tier--;

                                CollectedPoiRelation cpr;
                                cpr.id = rel.id;
                                cpr.category = poi_cat;
                                cpr.tier = tier;
                                cpr.flags = flags;
                                cpr.qid = 0;
                                const char* r_wd = rel.tag("wikidata");
                                if (r_wd && r_wd[0] == 'Q') {
                                    cpr.qid = static_cast<uint32_t>(std::strtoul(r_wd + 1, nullptr, 10));
                                }
                                cpr.name = poi_name;
                                // Relation-level addr tags (see CollectedPoiRelation
                                // header for rationale — rank-30 rows with
                                // addr:housenumber become AddrPoints too).
                                const char* r_hn = rel.tag("addr:housenumber");
                                if (r_hn) {
                                    cpr.addr_housenumber = r_hn;
                                    const char* r_st = rel.tag("addr:street");
                                    if (r_st) cpr.addr_street = r_st;
                                    const char* r_pc = rel.tag("addr:postcode");
                                    if (r_pc) cpr.addr_postcode = r_pc;
                                }

                                for (size_t mi = 0; mi < rel.members.size(); mi++) {
                                    if (rel.members[mi].type == 'w') {
                                        cpr.members.emplace_back(rel.members[mi].ref, rel.member_role(mi));
                                    }
                                }

                                if (!cpr.members.empty()) {
                                    std::lock_guard<std::mutex> lock(rel_mutex);
                                    data.collected_poi_relations.push_back(std::move(cpr));
                                }
                            }
                        }
                    }
                }, "r");
            }
            std::cerr << "  Collected " << data.collected_relations.size()
                      << " admin/postal relations for parallel assembly." << std::endl;
            std::cerr << "  Collected " << data.collected_poi_relations.size()
                      << " POI relations for parallel assembly." << std::endl;
            log_phase("Pass 1: relation scanning", _pt, _cpu);

            // Build admin_way_ids as a bitset for O(1) lookup
            // Max way ID ~1.3B → ~162 MiB bitset (much faster than unordered_set)
            static constexpr size_t MAX_WAY_ID = 2000000000ULL; // 2B, ~250 MiB
            std::vector<uint8_t> admin_way_bits(MAX_WAY_ID / 8 + 1, 0);
            size_t admin_way_count = 0;
            for (const auto& rel : data.collected_relations) {
                for (const auto& [way_id, role] : rel.members) {
                    if (way_id > 0 && static_cast<size_t>(way_id) < MAX_WAY_ID) {
                        admin_way_bits[way_id / 8] |= (1 << (way_id % 8));
                        admin_way_count++;
                    }
                }
            }
            auto is_admin_way = [&](int64_t id) -> bool {
                if (id <= 0 || static_cast<size_t>(id) >= MAX_WAY_ID) return false;
                return admin_way_bits[id / 8] & (1 << (id % 8));
            };
            std::cerr << "  Admin assembly needs " << admin_way_count << " way geometries." << std::endl;

            // Build poi_way_bits for POI relation member ways
            std::vector<uint8_t> poi_way_bits(MAX_WAY_ID / 8 + 1, 0);
            size_t poi_way_count = 0;
            for (const auto& rel : data.collected_poi_relations) {
                for (const auto& [way_id, role] : rel.members) {
                    if (way_id > 0 && static_cast<size_t>(way_id) < MAX_WAY_ID) {
                        poi_way_bits[way_id / 8] |= (1 << (way_id % 8));
                        poi_way_count++;
                    }
                }
            }
            auto is_poi_way = [&](int64_t id) -> bool {
                if (id <= 0 || static_cast<size_t>(id) >= MAX_WAY_ID) return false;
                return poi_way_bits[id / 8] & (1 << (id % 8));
            };
            std::cerr << "  POI assembly needs " << poi_way_count << " way geometries." << std::endl;

            // --- Pass 2: Node processing (streaming — no PbfNode objects) ---
            // POI classification helper
            struct PoiClassification {
                PoiCategory category;
                uint8_t tier;
                uint8_t flags;
            };
            auto classify_poi = [](const char* t_tourism, const char* t_historic,
                                   const char* t_boundary, const char* t_amenity,
                                   const char* t_leisure, const char* t_natural,
                                   const char* t_railway, const char* t_aeroway,
                                   const char* t_man_made, const char* t_building,
                                   const char* t_craft, const char* t_power,
                                   const char* t_place, const char* t_waterway,
                                   const char* t_office,
                                   const char* t_wikipedia, const char* t_wikidata)
                -> std::optional<PoiClassification> {
                PoiCategory cat = PoiCategory::UNKNOWN;

                // tourism (highest priority)
                if (t_tourism) {
                    if (std::strcmp(t_tourism, "museum") == 0) cat = PoiCategory::MUSEUM;
                    else if (std::strcmp(t_tourism, "attraction") == 0) cat = PoiCategory::ATTRACTION;
                    else if (std::strcmp(t_tourism, "viewpoint") == 0) cat = PoiCategory::VIEWPOINT;
                    else if (std::strcmp(t_tourism, "theme_park") == 0) cat = PoiCategory::THEME_PARK;
                    else if (std::strcmp(t_tourism, "zoo") == 0) cat = PoiCategory::ZOO;
                    else if (std::strcmp(t_tourism, "gallery") == 0) cat = PoiCategory::GALLERY;
                    else if (std::strcmp(t_tourism, "artwork") == 0) cat = PoiCategory::ARTWORK;
                    else if (std::strcmp(t_tourism, "alpine_hut") == 0) cat = PoiCategory::ALPINE_HUT;
                    else if (std::strcmp(t_tourism, "aquarium") == 0) cat = PoiCategory::AQUARIUM;
                    else if (std::strcmp(t_tourism, "camp_site") == 0) cat = PoiCategory::CAMP_SITE;
                    else if (std::strcmp(t_tourism, "picnic_site") == 0) cat = PoiCategory::PICNIC_SITE;
                    else if (std::strcmp(t_tourism, "resort") == 0) cat = PoiCategory::RESORT;
                }
                // historic
                if (cat == PoiCategory::UNKNOWN && t_historic) {
                    if (std::strcmp(t_historic, "castle") == 0) cat = PoiCategory::CASTLE;
                    else if (std::strcmp(t_historic, "monument") == 0) cat = PoiCategory::MONUMENT;
                    else if (std::strcmp(t_historic, "ruins") == 0) cat = PoiCategory::RUINS;
                    else if (std::strcmp(t_historic, "archaeological_site") == 0) cat = PoiCategory::ARCHAEOLOGICAL_SITE;
                    else if (std::strcmp(t_historic, "memorial") == 0) cat = PoiCategory::MEMORIAL;
                    else if (std::strcmp(t_historic, "battlefield") == 0) cat = PoiCategory::BATTLEFIELD;
                    else if (std::strcmp(t_historic, "fort") == 0) cat = PoiCategory::FORT;
                    else if (std::strcmp(t_historic, "ship") == 0) cat = PoiCategory::SHIP;
                }
                // boundary
                if (cat == PoiCategory::UNKNOWN && t_boundary) {
                    if (std::strcmp(t_boundary, "national_park") == 0) cat = PoiCategory::NATIONAL_PARK;
                    else if (std::strcmp(t_boundary, "protected_area") == 0) cat = PoiCategory::PROTECTED_AREA;
                }
                // amenity
                if (cat == PoiCategory::UNKNOWN && t_amenity) {
                    if (std::strcmp(t_amenity, "place_of_worship") == 0) cat = PoiCategory::PLACE_OF_WORSHIP;
                    else if (std::strcmp(t_amenity, "university") == 0) cat = PoiCategory::UNIVERSITY;
                    else if (std::strcmp(t_amenity, "college") == 0) cat = PoiCategory::COLLEGE;
                    else if (std::strcmp(t_amenity, "hospital") == 0) cat = PoiCategory::HOSPITAL;
                    else if (std::strcmp(t_amenity, "theatre") == 0) cat = PoiCategory::THEATRE;
                    else if (std::strcmp(t_amenity, "cinema") == 0) cat = PoiCategory::CINEMA;
                    else if (std::strcmp(t_amenity, "library") == 0) cat = PoiCategory::LIBRARY;
                    else if (std::strcmp(t_amenity, "marketplace") == 0) cat = PoiCategory::MARKETPLACE;
                    else if (std::strcmp(t_amenity, "embassy") == 0) cat = PoiCategory::EMBASSY;
                    else if (std::strcmp(t_amenity, "fountain") == 0) cat = PoiCategory::FOUNTAIN;
                    else if (std::strcmp(t_amenity, "casino") == 0) cat = PoiCategory::CASINO;
                    else if (std::strcmp(t_amenity, "cemetery") == 0) cat = PoiCategory::CEMETERY;
                    else if (std::strcmp(t_amenity, "ferry_terminal") == 0) cat = PoiCategory::FERRY_TERMINAL;
                    else if (std::strcmp(t_amenity, "planetarium") == 0) cat = PoiCategory::PLANETARIUM;
                    else if (std::strcmp(t_amenity, "prison") == 0) cat = PoiCategory::PRISON;
                }
                // leisure
                if (cat == PoiCategory::UNKNOWN && t_leisure) {
                    if (std::strcmp(t_leisure, "park") == 0) cat = PoiCategory::PARK;
                    else if (std::strcmp(t_leisure, "nature_reserve") == 0) cat = PoiCategory::NATURE_RESERVE;
                    else if (std::strcmp(t_leisure, "stadium") == 0) cat = PoiCategory::STADIUM;
                    else if (std::strcmp(t_leisure, "garden") == 0) cat = PoiCategory::GARDEN;
                    else if (std::strcmp(t_leisure, "water_park") == 0) cat = PoiCategory::WATER_PARK;
                    else if (std::strcmp(t_leisure, "golf_course") == 0) cat = PoiCategory::GOLF_COURSE;
                    else if (std::strcmp(t_leisure, "marina") == 0) cat = PoiCategory::MARINA;
                }
                // natural
                if (cat == PoiCategory::UNKNOWN && t_natural) {
                    if (std::strcmp(t_natural, "peak") == 0) cat = PoiCategory::PEAK;
                    else if (std::strcmp(t_natural, "volcano") == 0) cat = PoiCategory::VOLCANO;
                    else if (std::strcmp(t_natural, "beach") == 0) cat = PoiCategory::BEACH;
                    else if (std::strcmp(t_natural, "cave_entrance") == 0) cat = PoiCategory::CAVE_ENTRANCE;
                    else if (std::strcmp(t_natural, "spring") == 0) cat = PoiCategory::SPRING;
                    else if (std::strcmp(t_natural, "cliff") == 0) cat = PoiCategory::CLIFF;
                    else if (std::strcmp(t_natural, "arch") == 0) cat = PoiCategory::ARCH;
                    else if (std::strcmp(t_natural, "hot_spring") == 0) cat = PoiCategory::HOT_SPRING;
                    else if (std::strcmp(t_natural, "geyser") == 0) cat = PoiCategory::GEYSER;
                    else if (std::strcmp(t_natural, "bay") == 0) cat = PoiCategory::BAY;
                    else if (std::strcmp(t_natural, "cape") == 0) cat = PoiCategory::CAPE;
                    else if (std::strcmp(t_natural, "island") == 0) cat = PoiCategory::ISLAND;
                    else if (std::strcmp(t_natural, "glacier") == 0) cat = PoiCategory::GLACIER;
                }
                // railway
                if (cat == PoiCategory::UNKNOWN && t_railway) {
                    if (std::strcmp(t_railway, "station") == 0) cat = PoiCategory::STATION;
                }
                // aeroway
                if (cat == PoiCategory::UNKNOWN && t_aeroway) {
                    if (std::strcmp(t_aeroway, "aerodrome") == 0) cat = PoiCategory::AERODROME;
                }
                // man_made
                if (cat == PoiCategory::UNKNOWN && t_man_made) {
                    if (std::strcmp(t_man_made, "tower") == 0) cat = PoiCategory::TOWER;
                    else if (std::strcmp(t_man_made, "lighthouse") == 0) cat = PoiCategory::LIGHTHOUSE;
                    else if (std::strcmp(t_man_made, "windmill") == 0) cat = PoiCategory::WINDMILL;
                    else if (std::strcmp(t_man_made, "bridge") == 0) cat = PoiCategory::BRIDGE;
                    else if (std::strcmp(t_man_made, "pier") == 0) cat = PoiCategory::PIER;
                    else if (std::strcmp(t_man_made, "dam") == 0) cat = PoiCategory::DAM;
                    else if (std::strcmp(t_man_made, "observatory") == 0) cat = PoiCategory::OBSERVATORY;
                }
                // building
                if (cat == PoiCategory::UNKNOWN && t_building) {
                    if (std::strcmp(t_building, "cathedral") == 0) cat = PoiCategory::CATHEDRAL;
                    else if (std::strcmp(t_building, "palace") == 0) cat = PoiCategory::PALACE;
                }
                // craft
                if (cat == PoiCategory::UNKNOWN && t_craft) {
                    if (std::strcmp(t_craft, "winery") == 0) cat = PoiCategory::WINERY;
                    else if (std::strcmp(t_craft, "brewery") == 0) cat = PoiCategory::BREWERY;
                }
                // power
                if (cat == PoiCategory::UNKNOWN && t_power) {
                    if (std::strcmp(t_power, "plant") == 0) cat = PoiCategory::POWER_PLANT;
                }
                // place
                if (cat == PoiCategory::UNKNOWN && t_place) {
                    if (std::strcmp(t_place, "island") == 0 || std::strcmp(t_place, "islet") == 0)
                        cat = PoiCategory::ISLAND;
                }
                // waterway
                if (cat == PoiCategory::UNKNOWN && t_waterway) {
                    if (std::strcmp(t_waterway, "waterfall") == 0) cat = PoiCategory::WATERFALL;
                }
                // office
                if (cat == PoiCategory::UNKNOWN && t_office) {
                    if (std::strcmp(t_office, "government") == 0) cat = PoiCategory::GOVERNMENT;
                }

                if (cat == PoiCategory::UNKNOWN) return std::optional<PoiClassification>{};

                uint8_t tier = poi_get_default_tier(cat);
                uint8_t flags = 0;
                if (t_wikipedia) flags |= POI_FLAG_WIKIPEDIA;
                if (t_wikidata)  flags |= POI_FLAG_WIKIDATA;
                if ((flags & (POI_FLAG_WIKIPEDIA | POI_FLAG_WIKIDATA)) && tier > 1) tier--;

                return PoiClassification{cat, tier, flags};
            };

            std::cerr << "  Pass 2: processing nodes with " << num_threads << " threads..." << std::endl;
            std::unordered_map<std::string, uint8_t> wikidata_to_place_type;
            std::unordered_map<int64_t, uint8_t> label_node_to_place_type; // node_id → PlaceType
            {
                struct NodeThreadLocal {
                    std::vector<std::pair<double,double>> addr_coords;
                    std::vector<std::pair<std::string,std::string>> addr_strings;
                    std::vector<std::string> addr_postcodes; // parallel to addr_strings
                    uint64_t count = 0;
                    std::vector<std::pair<int64_t, uint8_t>> label_hits; // (node_id, PlaceType)
                    // POI node data
                    std::vector<PoiRecord> poi_records;
                    std::vector<std::string> poi_names;
                    std::vector<float> poi_elevations;
                    std::vector<uint32_t> poi_qids;
                    uint64_t poi_count = 0;
                    // Place node data
                    std::vector<PlaceNode> place_nodes;
                    std::vector<std::string> place_names;
                    std::vector<std::pair<std::string, uint8_t>> place_wikidata; // (wikidata_id, PlaceType)
                };
                // Use thread_local for streaming callback (no thread index available)
                static thread_local NodeThreadLocal* tl_node_data = nullptr;
                std::vector<NodeThreadLocal> ntld(num_threads);
                std::atomic<unsigned> next_tl{0};

                pbf.read_nodes_streaming([&](int64_t id, double lat, double lng,
                    const uint32_t* tag_keys, const uint32_t* tag_vals, size_t ntags,
                    const std::vector<std::string>& st) {

                    // Lazy init thread-local pointer
                    if (!tl_node_data) {
                        unsigned idx = next_tl.fetch_add(1);
                        tl_node_data = &ntld[idx % ntld.size()];
                    }

                    if (id > 0) {
                        index.set(static_cast<uint64_t>(id), lat, lng);
                    }

                    // Check for address + POI tags (fast — no string copies for tagless nodes)
                    if (ntags > 0) {
                        const char* housenumber = nullptr;
                        const char* street = nullptr;
                        const char* postcode = nullptr;
                        const char* n_tourism = nullptr;
                        const char* n_historic = nullptr;
                        const char* n_amenity = nullptr;
                        const char* n_leisure = nullptr;
                        const char* n_natural = nullptr;
                        const char* n_aeroway = nullptr;
                        const char* n_railway = nullptr;
                        const char* n_man_made = nullptr;
                        const char* n_building = nullptr;
                        const char* n_craft = nullptr;
                        const char* n_power = nullptr;
                        const char* n_place = nullptr;
                        const char* n_waterway = nullptr;
                        const char* n_boundary = nullptr;
                        const char* n_office = nullptr;
                        const char* n_name = nullptr;
                        const char* n_name_en = nullptr;
                        const char* n_ref = nullptr;
                        const char* n_wikipedia = nullptr;
                        const char* n_wikidata = nullptr;
                        const char* n_ele = nullptr;
                        for (size_t i = 0; i < ntags; i++) {
                            if (tag_keys[i] >= st.size()) continue;
                            const auto& k = st[tag_keys[i]];
                            const char* v = tag_vals[i] < st.size() ? st[tag_vals[i]].c_str() : nullptr;
                            switch (k.size() > 0 ? k[0] : 0) {
                                case 'a':
                                    if (k == "addr:housenumber") housenumber = v;
                                    else if (k == "addr:street") street = v;
                                    else if (k == "addr:postcode") postcode = v;
                                    else if (k == "amenity") n_amenity = v;
                                    else if (k == "aeroway") n_aeroway = v;
                                    break;
                                case 'b':
                                    if (k == "building") n_building = v;
                                    else if (k == "boundary") n_boundary = v;
                                    break;
                                case 'c': if (k == "craft") n_craft = v; break;
                                case 'e': if (k == "ele") n_ele = v; break;
                                case 'h': if (k == "historic") n_historic = v; break;
                                case 'l': if (k == "leisure") n_leisure = v; break;
                                case 'm': if (k == "man_made") n_man_made = v; break;
                                case 'n':
                                    if (k == "name") n_name = v;
                                    else if (k == "name:en") n_name_en = v;
                                    else if (k == "natural") n_natural = v;
                                    break;
                                case 'o':
                                    if (k == "office") n_office = v;
                                    break;
                                case 'p':
                                    if (k == "place") n_place = v;
                                    else if (k == "power") n_power = v;
                                    break;
                                case 'r':
                                    if (k == "railway") n_railway = v;
                                    else if (k == "ref") n_ref = v;
                                    break;
                                case 't': if (k == "tourism") n_tourism = v; break;
                                case 'w':
                                    if (k == "waterway") n_waterway = v;
                                    else if (k == "wikipedia") n_wikipedia = v;
                                    else if (k == "wikidata") n_wikidata = v;
                                    break;
                            }
                        }
                        if (housenumber) {
                            // Nominatim's IsAddressPoint accepts any
                            // rank-30 row with a housenumber, even
                            // without addr:street — it resolves the
                            // street via parent_place_id. We index
                            // the node regardless of addr:street and
                            // backfill the street later via a
                            // nearest-named-street sweep. Empty
                            // street sentinel = "needs backfill".
                            tl_node_data->addr_coords.push_back({lat, lng});
                            tl_node_data->addr_strings.push_back({housenumber, street ? street : ""});
                            tl_node_data->addr_postcodes.push_back(postcode ? postcode : "");
                            tl_node_data->count++;
                        }

                        // Prefer name:en when present (user-facing English-only mode)
                        const char* best_name = (n_name_en && n_name_en[0]) ? n_name_en : n_name;

                        // POI node extraction
                        if (best_name) {
                            auto cls = classify_poi(n_tourism, n_historic, n_boundary,
                                n_amenity, n_leisure, n_natural, n_railway, n_aeroway,
                                n_man_made, n_building, n_craft, n_power, n_place, n_waterway,
                                n_office,
                                n_wikipedia, n_wikidata);
                            if (cls) {
                                PoiRecord pr{};
                                pr.lat = static_cast<float>(lat);
                                pr.lng = static_cast<float>(lng);
                                pr.vertex_offset = NO_DATA;
                                pr.vertex_count = 0;
                                pr.name_id = 0; // will be set during merge
                                pr.category = static_cast<uint8_t>(cls->category);
                                pr.tier = cls->tier;
                                pr.flags = cls->flags;
                                tl_node_data->poi_records.push_back(pr);
                                tl_node_data->poi_names.push_back(best_name);
                                float ele_val = 0;
                                if (n_ele) { char* end; ele_val = std::strtof(n_ele, &end); if (end == n_ele) ele_val = 0; }
                                tl_node_data->poi_elevations.push_back(ele_val);
                                uint32_t qid = 0;
                                if (n_wikidata && n_wikidata[0] == 'Q') {
                                    qid = static_cast<uint32_t>(std::strtoul(n_wikidata + 1, nullptr, 10));
                                }
                                tl_node_data->poi_qids.push_back(qid);
                                tl_node_data->poi_count++;
                            }
                        }

                        // Place nodes (settlements) + label-role higher-level place types.
                        // Nominatim uses `ref` as the display name when
                        // `name` is missing (see name_keys handling in
                        // icu_tokenizer output builders) — most commonly
                        // on numeric-ref quarter nodes like Moscow's
                        // place=quarter ref=18 node. Fall back to ref
                        // so these show up in the address walk.
                        const char* place_name = best_name;
                        if (!place_name && n_ref && n_ref[0]) place_name = n_ref;
                        if (n_place && place_name) {
                            // Settlement types that we store as searchable place_nodes
                            PlaceType settlement_pt = PlaceType::UNKNOWN;
                            if (std::strcmp(n_place, "city") == 0) settlement_pt = PlaceType::CITY;
                            else if (std::strcmp(n_place, "town") == 0) settlement_pt = PlaceType::TOWN;
                            else if (std::strcmp(n_place, "village") == 0) settlement_pt = PlaceType::VILLAGE;
                            else if (std::strcmp(n_place, "suburb") == 0) settlement_pt = PlaceType::SUBURB;
                            else if (std::strcmp(n_place, "hamlet") == 0) settlement_pt = PlaceType::HAMLET;
                            else if (std::strcmp(n_place, "neighbourhood") == 0) settlement_pt = PlaceType::NEIGHBOURHOOD;
                            else if (std::strcmp(n_place, "quarter") == 0) settlement_pt = PlaceType::QUARTER;

                            bool is_label_member = wanted_label_nodes.count(id) > 0;

                            if (settlement_pt != PlaceType::UNKNOWN) {
                                PlaceNode pn{};
                                pn.lat = static_cast<float>(lat);
                                pn.lng = static_cast<float>(lng);
                                pn.place_type = static_cast<uint8_t>(settlement_pt);
                                tl_node_data->place_nodes.push_back(pn);
                                tl_node_data->place_names.push_back(place_name);
                            }

                            // For the label-role lookup, also recognise higher-level
                            // place types. Nominatim's find_linked_place() matches
                            // class='place' (any type), and get_label_tag() then uses
                            // extratags['linked_place'] (which is the place type) as
                            // the label. So an admin L5 Region linked to a place=state
                            // label node should be labelled "state" in the address.
                            PlaceType label_pt = settlement_pt;
                            if (label_pt == PlaceType::UNKNOWN) {
                                if (std::strcmp(n_place, "state") == 0) label_pt = PlaceType::STATE;
                                else if (std::strcmp(n_place, "province") == 0) label_pt = PlaceType::PROVINCE;
                                else if (std::strcmp(n_place, "region") == 0) label_pt = PlaceType::REGION;
                                else if (std::strcmp(n_place, "county") == 0) label_pt = PlaceType::COUNTY;
                                else if (std::strcmp(n_place, "district") == 0) label_pt = PlaceType::DISTRICT;
                                else if (std::strcmp(n_place, "borough") == 0) label_pt = PlaceType::BOROUGH;
                            }
                            // Capture wikidata → place type for the
                            // admin-boundary wikidata link step in
                            // Nominatim's find_linked_place() chain.
                            // This must include *all* place types —
                            // not just settlements — because e.g.
                            // Mexico City's Cuauhtémoc borough
                            // relation links via wikidata (Q645293)
                            // to an admin_centre node tagged
                            // place=borough, and without that the
                            // boundary falls through to the default
                            // admin_level=6 → county mapping instead
                            // of picking up rank_address=18 (borough).
                            //
                            // Skip label-role members: nominatim's
                            // find_linked_place claims a place node
                            // via label role first, so wikidata
                            // linking only considers non-label nodes.
                            if (n_wikidata && !is_label_member && label_pt != PlaceType::UNKNOWN) {
                                tl_node_data->place_wikidata.push_back({n_wikidata, static_cast<uint8_t>(label_pt)});
                            }
                            if (is_label_member) {
                                tl_node_data->label_hits.push_back({id, static_cast<uint8_t>(label_pt)});
                            }
                        }
                    }
                });

                // Reset thread_local for next use
                tl_node_data = nullptr;

                // Merge address points
                uint64_t total_addrs = 0;
                for (auto& local : ntld) {
                    for (size_t j = 0; j < local.addr_coords.size(); j++) {
                        uint64_t dummy = 0;
                        const char* pc_ptr = local.addr_postcodes[j].empty() ? nullptr : local.addr_postcodes[j].c_str();
                        add_addr_point(data, local.addr_coords[j].first, local.addr_coords[j].second,
                                       local.addr_strings[j].first.c_str(),
                                       local.addr_strings[j].second.c_str(), pc_ptr, dummy);
                        // Accumulate postcode centroids (basic validation;
                        // per-country pattern validation happens later when
                        // building the centroid index).
                        const auto& pc = local.addr_postcodes[j];
                        if (!pc.empty() && is_valid_postcode(pc.c_str())) {
                            uint32_t pc_id = data.string_pool.intern(pc.c_str());
                            auto& acc = data.postcode_accum[pc_id];
                            acc.sum_lat += local.addr_coords[j].first;
                            acc.sum_lng += local.addr_coords[j].second;
                            acc.count++;
                        }
                    }
                    total_addrs += local.count;
                }
                // Merge POI node records
                uint64_t total_poi_nodes = 0;
                for (auto& local : ntld) {
                    for (size_t j = 0; j < local.poi_records.size(); j++) {
                        auto pr = local.poi_records[j];
                        pr.name_id = data.string_pool.intern(local.poi_names[j]);
                        data.poi_records.push_back(pr);
                    }
                    poi_elevations.insert(poi_elevations.end(),
                        local.poi_elevations.begin(), local.poi_elevations.end());
                    poi_qids.insert(poi_qids.end(),
                        local.poi_qids.begin(), local.poi_qids.end());
                    total_poi_nodes += local.poi_count;
                }
                // Merge place nodes
                for (auto& local : ntld) {
                    for (size_t j = 0; j < local.place_nodes.size(); j++) {
                        auto pn = local.place_nodes[j];
                        pn.name_id = data.string_pool.intern(local.place_names[j]);
                        data.place_nodes.push_back(pn);
                    }
                }
                // Build wikidata → place type map for admin boundary linking
                for (auto& local : ntld) {
                    for (auto& [wd, pt] : local.place_wikidata) {
                        wikidata_to_place_type[wd] = pt;
                    }
                    local.place_wikidata.clear();
                }
                // Build label-node → place type map (Nominatim's primary link method)
                for (auto& local : ntld) {
                    for (auto& [nid, pt] : local.label_hits) {
                        label_node_to_place_type[nid] = pt;
                    }
                    local.label_hits.clear();
                }
                if (!wikidata_to_place_type.empty()) {
                    std::cerr << "  Built wikidata→place_type map: "
                              << wikidata_to_place_type.size() << " entries." << std::endl;
                }
                if (!label_node_to_place_type.empty()) {
                    std::cerr << "  Built label-node→place_type map: "
                              << label_node_to_place_type.size() << " entries." << std::endl;
                }
                std::cerr << "  Node processing complete: " << total_addrs
                          << " address points, " << total_poi_nodes
                          << " POI nodes, " << data.place_nodes.size()
                          << " place nodes collected." << std::endl;
            }
            log_phase("Pass 2: node processing", _pt, _cpu);
            pbf.release_pages(); // free PBF mmap pages, will re-fault for way pass

            // --- Pass 2b: Way processing (fully parallel) ---
            std::cerr << "  Processing ways with " << num_threads << " threads..." << std::endl;
            {
                // Thread-local data for parallel way processing
                struct ThreadLocalData {
                    std::vector<WayHeader> ways;
                    std::vector<NodeCoord> street_nodes;
                    std::vector<DeferredWay> deferred_ways;
                    std::vector<InterpWay> interp_ways;
                    std::vector<NodeCoord> interp_nodes;
                    std::vector<DeferredInterp> deferred_interps;
                    std::vector<AddrPoint> building_addrs;
                    std::vector<std::pair<double, double>> building_addr_coords; // lat,lng for S2 cell
                    // Flat polygon storage, parallel to building_addrs via
                    // (offset, count) into building_addr_poly_verts. Stored as
                    // NodeCoord (2× float = 8 bytes/vertex) instead of
                    // pair<double,double> (16 bytes/vertex), and flat rather
                    // than vector<vector<...>>, to keep thread-local peak
                    // memory bounded during parallel way parsing.
                    std::vector<NodeCoord> building_addr_poly_verts;
                    std::vector<uint32_t> building_addr_poly_off;   // NO_DATA = no polygon
                    std::vector<uint32_t> building_addr_poly_cnt;   // 0 = no polygon
                    std::vector<std::string> way_strings;      // way name (name:en ?: name)
                    std::vector<std::string> way_orig_names;   // way original name (always name tag)
                    std::vector<std::pair<std::string,std::string>> addr_strings; // building addr {hn, street}
                    std::vector<std::string> addr_postcodes; // parallel to addr_strings
                    std::vector<std::string> interp_strings;   // interp street name strings
                    uint64_t way_count = 0;
                    uint64_t building_addr_count = 0;
                    uint64_t interp_count = 0;
                    // Way geometries for parallel admin assembly
                    struct WayGeomEntry {
                        int64_t way_id;
                        std::vector<std::pair<double,double>> coords;
                        int64_t first_node_id;
                        int64_t last_node_id;
                    };
                    std::vector<WayGeomEntry> way_geoms;
                    // Closed-way admin polygons (ways with boundary=administrative)
                    struct ClosedWayAdmin {
                        std::vector<std::pair<double,double>> vertices;
                        std::string name;
                        uint8_t admin_level;
                        std::string country_code;
                        uint8_t fallback_place_type;  // from own place tag, used last
                        std::string wikidata; // for place linking
                    };
                    std::vector<ClosedWayAdmin> closed_way_admins;
                    // POI way data
                    struct PoiWayEntry {
                        PoiRecord record;
                        std::string name;
                        std::vector<NodeCoord> vertices;
                        float elevation;
                        uint32_t qid;
                    };
                    std::vector<PoiWayEntry> poi_ways;
                    // POI way geometries for relation assembly
                    struct PoiWayGeomEntry {
                        int64_t way_id;
                        std::vector<std::pair<double,double>> coords;
                        int64_t first_node_id;
                        int64_t last_node_id;
                    };
                    std::vector<PoiWayGeomEntry> poi_way_geoms;
                };

                static thread_local ThreadLocalData* tl_way_data = nullptr;
                std::vector<ThreadLocalData> tld(num_threads);
                std::atomic<unsigned> next_tl_way{0};

                pbf.read_ways_streaming([&](int64_t way_id,
                    const int64_t* refs_data, size_t refs_size,
                    const uint32_t* tag_keys, const uint32_t* tag_vals, size_t ntags,
                    const std::vector<std::string>& st) {

                    if (!tl_way_data) {
                        unsigned idx = next_tl_way.fetch_add(1);
                        tl_way_data = &tld[idx % tld.size()];
                    }
                    auto& local = *tl_way_data;

                    // Single-pass tag extraction — avoid repeated linear scans
                    const char* t_interpolation = nullptr;
                    const char* t_housenumber = nullptr;
                    const char* t_street = nullptr;
                    const char* t_postcode = nullptr;
                    const char* t_highway = nullptr;
                    const char* t_footway = nullptr;
                    const char* t_access = nullptr;
                    const char* t_tunnel = nullptr;
                    const char* t_name = nullptr;
                    const char* t_name_en = nullptr;
                    const char* t_boundary = nullptr;
                    const char* t_admin_level = nullptr;
                    const char* t_postal_code = nullptr;
                    const char* t_iso = nullptr;
                    const char* t_linked_place = nullptr;
                    const char* t_border_type = nullptr;
                    // POI tags
                    const char* t_tourism = nullptr;
                    const char* t_historic = nullptr;
                    const char* t_amenity = nullptr;
                    const char* t_leisure = nullptr;
                    const char* t_natural = nullptr;
                    const char* t_aeroway = nullptr;
                    const char* t_railway = nullptr;
                    const char* t_man_made = nullptr;
                    const char* t_building = nullptr;
                    const char* t_craft = nullptr;
                    const char* t_power = nullptr;
                    const char* t_place = nullptr;
                    const char* t_waterway = nullptr;
                    const char* t_office = nullptr;
                    const char* t_wikipedia = nullptr;
                    const char* t_wikidata = nullptr;
                    const char* t_ele = nullptr;
                    for (size_t i = 0; i < ntags; i++) {
                        if (tag_keys[i] >= st.size()) continue;
                        const auto& k = st[tag_keys[i]];
                        const char* v = tag_vals[i] < st.size() ? st[tag_vals[i]].c_str() : nullptr;
                        // Fast dispatch by first character
                        switch (k.size() > 0 ? k[0] : 0) {
                            case 'a':
                                if (k == "addr:interpolation") t_interpolation = v;
                                else if (k == "addr:housenumber") t_housenumber = v;
                                else if (k == "addr:street") t_street = v;
                                else if (k == "addr:postcode") t_postcode = v;
                                else if (k == "admin_level") t_admin_level = v;
                                else if (k == "amenity") t_amenity = v;
                                else if (k == "aeroway") t_aeroway = v;
                                else if (k == "access") t_access = v;
                                break;
                            case 'b':
                                if (k == "boundary") t_boundary = v;
                                else if (k == "building") t_building = v;
                                else if (k == "border_type") t_border_type = v;
                                break;
                            case 'c': if (k == "craft") t_craft = v; break;
                            case 'e': if (k == "ele") t_ele = v; break;
                            case 'f': if (k == "footway") t_footway = v; break;
                            case 'h':
                                if (k == "highway") t_highway = v;
                                else if (k == "historic") t_historic = v;
                                break;
                            case 'l':
                                if (k == "leisure") t_leisure = v;
                                else if (k == "linked_place") t_linked_place = v;
                                break;
                            case 'm': if (k == "man_made") t_man_made = v; break;
                            case 'n':
                                if (k == "name") t_name = v;
                                else if (k == "name:en") t_name_en = v;
                                else if (k == "natural") t_natural = v;
                                break;
                            case 'o':
                                if (k == "office") t_office = v;
                                break;
                            case 'p':
                                if (k == "postal_code") t_postal_code = v;
                                else if (k == "place") t_place = v;
                                else if (k == "power") t_power = v;
                                break;
                            case 'r': if (k == "railway") t_railway = v; break;
                            case 't':
                                if (k == "tourism") t_tourism = v;
                                else if (k == "tunnel") t_tunnel = v;
                                break;
                            case 'I': if (k == "ISO3166-1:alpha2") t_iso = v; break;
                            case 'w':
                                if (k == "waterway") t_waterway = v;
                                else if (k == "wikipedia") t_wikipedia = v;
                                else if (k == "wikidata") t_wikidata = v;
                                break;
                        }
                    }

                    // Prefer name:en where present (English-only mode)
                    const char* t_best_name = (t_name_en && t_name_en[0]) ? t_name_en : t_name;

                    // Check if this way has POI-qualifying tags
                    bool has_poi_tags = t_tourism || t_historic || t_amenity || t_leisure ||
                        t_natural || t_aeroway || t_railway || t_man_made || t_building ||
                        t_craft || t_power || t_place || t_waterway || t_office ||
                        (t_boundary && (std::strcmp(t_boundary, "national_park") == 0 ||
                                        std::strcmp(t_boundary, "protected_area") == 0));

                    // Early exit: if no relevant tags, skip expensive node resolution
                    bool need_nodes = t_interpolation || t_housenumber ||
                        (t_highway && is_included_highway_full(t_highway, t_footway, t_access, t_tunnel) && t_name) ||
                        t_boundary ||
                        (refs_size > 0 && is_admin_way(way_id)) ||
                        (has_poi_tags && t_name) ||
                        (refs_size > 0 && is_poi_way(way_id));
                    if (!need_nodes) return;

                    // Pre-resolve all node locations
                    thread_local std::vector<PackedLocation> resolved_locs;
                    resolved_locs.clear();
                    resolved_locs.reserve(refs_size);
                    bool all_valid = true;
                    for (size_t ri = 0; ri < refs_size; ri++) {
                        auto loc = index.get(static_cast<uint64_t>(refs_data[ri]));
                        resolved_locs.push_back(loc);
                        if (!loc.valid()) all_valid = false;
                    }

                    // Address interpolation
                    if (t_interpolation) {
                        if (refs_size >= 2 && all_valid) {
                            const char* street = t_street;
                            if (street) {
                                uint32_t interp_id = static_cast<uint32_t>(local.interp_ways.size());
                                uint32_t node_offset = static_cast<uint32_t>(local.interp_nodes.size());
                                for (const auto& loc : resolved_locs)
                                    local.interp_nodes.push_back({static_cast<float>(loc.lat()), static_cast<float>(loc.lon())});
                                uint8_t interp_type = 0;
                                if (std::strcmp(t_interpolation, "even") == 0) interp_type = 1;
                                else if (std::strcmp(t_interpolation, "odd") == 0) interp_type = 2;
                                InterpWay iw{}; iw.node_offset = node_offset;
                                iw.node_count = static_cast<uint8_t>(std::min(refs_size, size_t(255)));
                                iw.interpolation = interp_type;
                                local.interp_ways.push_back(iw);
                                local.interp_strings.push_back(street);
                                local.deferred_interps.push_back({interp_id, node_offset, iw.node_count});
                                local.interp_count++;
                            }
                        }
                        return;
                    }

                    // Building addresses — index regardless of whether
                    // addr:street is present. Missing streets are
                    // backfilled via the nearest-named-street sweep
                    // after way indexing, matching Nominatim's
                    // parent_place_id resolution behaviour.
                    const char* housenumber = t_housenumber;
                    if (housenumber && refs_size > 0) {
                        const char* street = t_street;
                        double sum_lat = 0, sum_lng = 0; int valid = 0;
                        for (const auto& loc : resolved_locs) {
                            if (loc.valid()) { sum_lat += loc.lat(); sum_lng += loc.lon(); valid++; }
                        }
                        if (valid > 0) {
                            double clat = sum_lat/valid, clng = sum_lng/valid;
                            local.building_addr_coords.push_back({clat, clng});
                            local.building_addrs.push_back({static_cast<float>(clat), static_cast<float>(clng), 0, 0, 0, NO_DATA, 0});
                            local.addr_strings.push_back({housenumber, street ? street : ""});
                            local.addr_postcodes.push_back(t_postcode ? t_postcode : "");
                            // Polygon vertices for closed ways (buildings).
                            // Lets the server compute exact distance-to-
                            // polygon for Nominatim parity instead of the
                            // centroid-minus-10m approximation. Cap at 20
                            // vertices — addr-point distance only needs a
                            // coarse shape, and tight caps bound thread-
                            // local memory during parallel parse.
                            uint32_t poly_off = NO_DATA;
                            uint32_t poly_cnt = 0;
                            if (refs_size >= 4 && refs_data[0] == refs_data[refs_size-1] && all_valid) {
                                std::vector<std::pair<double,double>> poly_pts;
                                poly_pts.reserve(refs_size);
                                for (const auto& loc : resolved_locs)
                                    poly_pts.push_back({loc.lat(), loc.lon()});
                                constexpr size_t MAX_ADDR_POLY_VERTS = 20;
                                if (poly_pts.size() > MAX_ADDR_POLY_VERTS) {
                                    poly_pts = simplify_polygon(poly_pts, MAX_ADDR_POLY_VERTS);
                                }
                                poly_off = static_cast<uint32_t>(local.building_addr_poly_verts.size());
                                poly_cnt = static_cast<uint32_t>(poly_pts.size());
                                for (const auto& p : poly_pts)
                                    local.building_addr_poly_verts.push_back({static_cast<float>(p.first), static_cast<float>(p.second)});
                            }
                            local.building_addr_poly_off.push_back(poly_off);
                            local.building_addr_poly_cnt.push_back(poly_cnt);
                            local.building_addr_count++;
                        }
                    }

                    // Highway ways
                    if (t_highway && is_included_highway_full(t_highway, t_footway, t_access, t_tunnel)) {
                        if (t_best_name && refs_size >= 2 && all_valid) {
                            uint32_t wid = static_cast<uint32_t>(local.ways.size());
                            uint32_t noff = static_cast<uint32_t>(local.street_nodes.size());
                            for (const auto& loc : resolved_locs)
                                local.street_nodes.push_back({static_cast<float>(loc.lat()), static_cast<float>(loc.lon())});
                            WayHeader header{}; header.node_offset = noff;
                            header.node_count = static_cast<uint8_t>(std::min(refs_size, size_t(255)));
                            local.ways.push_back(header);
                            local.way_strings.push_back(t_best_name);
                            local.way_orig_names.push_back(t_name ? t_name : (t_best_name ? t_best_name : ""));
                            local.deferred_ways.push_back({wid, noff, header.node_count});
                            local.way_count++;
                        }
                    }

                    // Admin boundary member ways
                    if (refs_size > 0 && is_admin_way(way_id)) {
                        std::vector<std::pair<double,double>> geom;
                        for (const auto& loc : resolved_locs)
                            if (loc.valid()) geom.push_back({loc.lat(), loc.lon()});
                        if (!geom.empty())
                            local.way_geoms.push_back({way_id, std::move(geom), refs_data[0], refs_data[refs_size-1]});
                    }

                    // POI way geometries for relation assembly
                    if (refs_size > 0 && is_poi_way(way_id)) {
                        std::vector<std::pair<double,double>> geom;
                        for (const auto& loc : resolved_locs)
                            if (loc.valid()) geom.push_back({loc.lat(), loc.lon()});
                        if (!geom.empty())
                            local.poi_way_geoms.push_back({way_id, std::move(geom), refs_data[0], refs_data[refs_size-1]});
                    }

                    // Closed way POI polygons
                    if (has_poi_tags && t_best_name && refs_size >= 4 && refs_data[0] == refs_data[refs_size-1] && all_valid) {
                        auto cls = classify_poi(t_tourism, t_historic, t_boundary,
                            t_amenity, t_leisure, t_natural, t_railway, t_aeroway,
                            t_man_made, t_building, t_craft, t_power, t_place, t_waterway,
                            t_office,
                            t_wikipedia, t_wikidata);
                        if (cls) {
                            // Compute centroid
                            double sum_lat = 0, sum_lng = 0;
                            for (const auto& loc : resolved_locs) {
                                sum_lat += loc.lat(); sum_lng += loc.lon();
                            }
                            float clat = static_cast<float>(sum_lat / refs_size);
                            float clng = static_cast<float>(sum_lng / refs_size);

                            // Build polygon vertices
                            std::vector<std::pair<double,double>> poly_pts;
                            poly_pts.reserve(refs_size);
                            for (const auto& loc : resolved_locs)
                                poly_pts.push_back({loc.lat(), loc.lon()});

                            // Simplify large polygons with ~50m epsilon
                            if (poly_pts.size() > MAX_POLYGON_VERTICES) {
                                double lat0 = poly_pts.empty() ? 0.0 : poly_pts[0].first;
                                double eps_deg = meters_to_degrees(50.0, lat0);
                                poly_pts = simplify_polygon_epsilon(poly_pts, eps_deg);
                            }

                            std::vector<NodeCoord> verts;
                            verts.reserve(poly_pts.size());
                            for (const auto& [plat, plng] : poly_pts)
                                verts.push_back({static_cast<float>(plat), static_cast<float>(plng)});

                            PoiRecord pr{};
                            pr.lat = clat;
                            pr.lng = clng;
                            pr.vertex_offset = 0; // set during merge
                            pr.vertex_count = static_cast<uint32_t>(verts.size());
                            pr.name_id = 0; // set during merge
                            pr.category = static_cast<uint8_t>(cls->category);
                            pr.tier = cls->tier;
                            pr.flags = cls->flags;

                            float way_ele = 0;
                            if (t_ele) { char* end; way_ele = std::strtof(t_ele, &end); if (end == t_ele) way_ele = 0; }
                            uint32_t way_qid = 0;
                            if (t_wikidata && t_wikidata[0] == 'Q') {
                                way_qid = static_cast<uint32_t>(std::strtoul(t_wikidata + 1, nullptr, 10));
                            }
                            local.poi_ways.push_back({pr, t_best_name, std::move(verts), way_ele, way_qid});
                        }
                    }

                    // Closed way admin boundaries
                    const char* boundary = t_boundary;
                    if (boundary) {
                        bool is_admin = (std::strcmp(boundary, "administrative") == 0);
                        bool is_postal = (std::strcmp(boundary, "postal_code") == 0);
                        bool is_place_boundary = (std::strcmp(boundary, "place") == 0);
                        if ((is_admin || is_postal) && refs_size >= 4 && refs_data[0] == refs_data[refs_size-1]) {
                            uint8_t al = 0;
                            if (is_admin) { const char* ls = t_admin_level; if (ls) al = static_cast<uint8_t>(std::atoi(ls)); }
                            else al = 11;
                            int max_al = is_postal ? 11 : 10;
                            if (al >= 2 && al <= max_al && (kMaxAdminLevel == 0 || al <= kMaxAdminLevel)) {
                                const char* aname = t_best_name;
                                if (aname || !is_admin) {
                                    std::string name_str;
                                    if (is_postal) { const char* pc = t_postal_code; if (!pc) pc = aname; if (pc) name_str = pc; }
                                    else if (aname) name_str = aname;
                                    if (!name_str.empty() && all_valid && resolved_locs.size() >= 3) {
                                        std::vector<std::pair<double,double>> verts;
                                        for (const auto& loc : resolved_locs) verts.push_back({loc.lat(), loc.lon()});
                                        std::string cc; if (al == 2) { const char* iso = t_iso; if (iso) cc = iso; }
                                        auto po = classify_place_override(t_linked_place, t_border_type, t_place);
                                        uint8_t fallback = static_cast<uint8_t>(po.type);
                                        std::string wd_str = t_wikidata ? t_wikidata : "";
                                        local.closed_way_admins.push_back({std::move(verts), std::move(name_str), al, std::move(cc), fallback, std::move(wd_str)});
                                    }
                                }
                            }
                        }
                        // Closed way boundary=place (e.g., Washington DC)
                        if (is_place_boundary && refs_size >= 4 && refs_data[0] == refs_data[refs_size-1]) {
                            const char* aname = t_best_name;
                            if (aname && t_place) {
                                AdminPlaceType pt = classify_place_override(nullptr, nullptr, t_place).type;
                                if (pt != AdminPlaceType::NONE && all_valid && resolved_locs.size() >= 3) {
                                    std::vector<std::pair<double,double>> verts;
                                    for (const auto& loc : resolved_locs) verts.push_back({loc.lat(), loc.lon()});
                                    std::string wd_str = t_wikidata ? t_wikidata : "";
                                    local.closed_way_admins.push_back({std::move(verts), std::string(aname), uint8_t(15), std::string(), static_cast<uint8_t>(pt), std::move(wd_str)});
                                }
                            }
                        }
                    }

                    // Plain place=* closed ways (no boundary tag). OSM
                    // commonly tags neighbourhoods, quarters, and
                    // suburbs as closed ways with `place=*` + `name`
                    // rather than as `boundary=place` + `place=*`.
                    // nominatim indexes both forms via placex (class
                    // = 'place' at import). we miss them if we gate
                    // only on boundary=place. example: qasr al
                    // doubara in cairo is a landuse=residential way
                    // with place=neighbourhood + name:en, and nominatim
                    // returns it as neighbourhood for queries inside
                    // the polygon.
                    if (!boundary && t_place && t_best_name
                            && refs_size >= 4 && refs_data[0] == refs_data[refs_size-1]) {
                        AdminPlaceType pt = classify_place_override(nullptr, nullptr, t_place).type;
                        // Only the rank-20+ place types (neighbourhood,
                        // quarter, suburb, borough) — higher-level place
                        // types on closed ways are rare and adding them
                        // risks promoting a landuse polygon into the
                        // city / state chain.
                        bool want = (pt == AdminPlaceType::NEIGHBOURHOOD
                                  || pt == AdminPlaceType::QUARTER
                                  || pt == AdminPlaceType::SUBURB
                                  || pt == AdminPlaceType::BOROUGH);
                        if (want && all_valid && resolved_locs.size() >= 3) {
                            std::vector<std::pair<double,double>> verts;
                            for (const auto& loc : resolved_locs) verts.push_back({loc.lat(), loc.lon()});
                            std::string wd_str = t_wikidata ? t_wikidata : "";
                            local.closed_way_admins.push_back({
                                std::move(verts),
                                std::string(t_best_name),
                                uint8_t(15),
                                std::string(),
                                static_cast<uint8_t>(pt),
                                std::move(wd_str)});
                        }
                    }
                });
                tl_way_data = nullptr;
                std::cerr << "  Parallel way processing complete." << std::endl;

                // Process areas/multipolygons — sequential fallback path
                // Merge thread-local way/interp data into main ParsedData
                std::cerr << "  Merging thread-local data..." << std::endl;
                uint64_t total_ways = 0, total_building_addrs = 0, total_interps = 0;
                for (auto& local : tld) {
                    uint32_t way_base = static_cast<uint32_t>(data.ways.size());
                    uint32_t node_base = static_cast<uint32_t>(data.street_nodes.size());
                    uint32_t interp_base = static_cast<uint32_t>(data.interp_ways.size());
                    uint32_t interp_node_base = static_cast<uint32_t>(data.interp_nodes.size());

                    // Merge street ways
                    for (size_t i = 0; i < local.ways.size(); i++) {
                        auto h = local.ways[i];
                        h.node_offset += node_base;
                        h.name_id = data.string_pool.intern(local.way_strings[i]);
                        data.ways.push_back(h);
                        // Store original name for token matching
                        uint32_t orig_id = NO_DATA;
                        if (i < local.way_orig_names.size() && !local.way_orig_names[i].empty()) {
                            orig_id = data.string_pool.intern(local.way_orig_names[i]);
                        }
                        data.way_orig_name_ids.push_back(orig_id);
                    }
                    data.street_nodes.insert(data.street_nodes.end(),
                        local.street_nodes.begin(), local.street_nodes.end());

                    // Remap deferred ways
                    for (auto dw : local.deferred_ways) {
                        dw.way_id += way_base;
                        dw.node_offset += node_base;
                        data.deferred_ways.push_back(dw);
                    }

                    // Merge building addresses
                    for (size_t i = 0; i < local.building_addrs.size(); i++) {
                        uint64_t dummy = 0;
                        const char* bpc_ptr = local.addr_postcodes[i].empty() ? nullptr : local.addr_postcodes[i].c_str();
                        uint32_t poly_off = local.building_addr_poly_off[i];
                        uint32_t poly_cnt = local.building_addr_poly_cnt[i];
                        const NodeCoord* poly_verts = (poly_cnt > 0 && poly_off != NO_DATA)
                            ? &local.building_addr_poly_verts[poly_off]
                            : nullptr;
                        add_addr_point(data, local.building_addrs[i].lat, local.building_addrs[i].lng,
                                       local.addr_strings[i].first.c_str(),
                                       local.addr_strings[i].second.c_str(), bpc_ptr, dummy,
                                       poly_verts, poly_cnt);
                        // Accumulate postcode centroids (validated)
                        const auto& pc = local.addr_postcodes[i];
                        if (!pc.empty() && is_valid_postcode(pc.c_str())) {
                            uint32_t pc_id = data.string_pool.intern(pc.c_str());
                            auto& acc = data.postcode_accum[pc_id];
                            acc.sum_lat += local.building_addrs[i].lat;
                            acc.sum_lng += local.building_addrs[i].lng;
                            acc.count++;
                        }
                    }

                    // Merge interpolation ways
                    for (size_t i = 0; i < local.interp_ways.size(); i++) {
                        auto iw = local.interp_ways[i];
                        iw.node_offset += interp_node_base;
                        iw.street_id = data.string_pool.intern(local.interp_strings[i]);
                        data.interp_ways.push_back(iw);
                    }
                    data.interp_nodes.insert(data.interp_nodes.end(),
                        local.interp_nodes.begin(), local.interp_nodes.end());

                    // Remap deferred interps
                    for (auto di : local.deferred_interps) {
                        di.interp_id += interp_base;
                        di.node_offset += interp_node_base;
                        data.deferred_interps.push_back(di);
                    }

                    total_ways += local.way_count;
                    total_building_addrs += local.building_addr_count;
                    total_interps += local.interp_count;

                    // Merge way geometries for admin assembly
                    {
                        for (auto& wg : local.way_geoms) {
                            ParsedData::WayGeometry g;
                            g.coords = std::move(wg.coords);
                            g.first_node_id = wg.first_node_id;
                            g.last_node_id = wg.last_node_id;
                            data.way_geometries[wg.way_id] = std::move(g);
                        }
                        local.way_geoms.clear();
                        local.way_geoms.shrink_to_fit();
                    }

                    // Merge POI way geometries for relation assembly
                    for (auto& wg : local.poi_way_geoms) {
                        // Store in way_geometries (shared with admin — no conflict since IDs differ)
                        if (data.way_geometries.find(wg.way_id) == data.way_geometries.end()) {
                            ParsedData::WayGeometry g;
                            g.coords = std::move(wg.coords);
                            g.first_node_id = wg.first_node_id;
                            g.last_node_id = wg.last_node_id;
                            data.way_geometries[wg.way_id] = std::move(g);
                        }
                    }
                    local.poi_way_geoms.clear();
                    local.poi_way_geoms.shrink_to_fit();

                    // Merge POI ways (closed way polygons)
                    for (auto& pw : local.poi_ways) {
                        uint32_t vertex_offset = static_cast<uint32_t>(data.poi_vertices.size());
                        data.poi_vertices.insert(data.poi_vertices.end(),
                            pw.vertices.begin(), pw.vertices.end());
                        pw.record.vertex_offset = vertex_offset;
                        pw.record.name_id = data.string_pool.intern(pw.name);
                        data.poi_records.push_back(pw.record);
                        poi_elevations.push_back(pw.elevation);
                        poi_qids.push_back(pw.qid);
                    }
                    local.poi_ways.clear();
                }
                std::cerr << "  Merged: " << total_ways << " ways, "
                          << total_building_addrs << " building addrs, "
                          << total_interps << " interps from parallel processing." << std::endl;

                // --- Merge closed-way admin polygons ---
                {
                    uint64_t closed_way_admin_count = 0;
                    size_t cwa_wikidata_linked = 0;
                    for (auto& local : tld) {
                        for (auto& cwa : local.closed_way_admins) {
                            // wikidata step first (Nominatim priority), then
                            // fall back to the way's own place tag.
                            uint8_t pto = 0;
                            if (!cwa.wikidata.empty()) {
                                auto it = wikidata_to_place_type.find(cwa.wikidata);
                                if (it != wikidata_to_place_type.end()) {
                                    pto = place_type_to_admin_override(it->second);
                                    if (pto != 0) cwa_wikidata_linked++;
                                }
                            }
                            if (pto == 0) pto = cwa.fallback_place_type;
                            const char* cc = cwa.country_code.empty() ? nullptr : cwa.country_code.c_str();
                            add_admin_polygon(data, cwa.vertices, cwa.name.c_str(),
                                              cwa.admin_level, cc, &admin_pool, pto);
                            closed_way_admin_count++;
                        }
                        local.closed_way_admins.clear();
                    }
                    if (closed_way_admin_count > 0) {
                        std::cerr << "  Added " << closed_way_admin_count
                                  << " admin polygons from closed ways";
                        if (cwa_wikidata_linked > 0)
                            std::cerr << " (" << cwa_wikidata_linked << " wikidata-linked)";
                        std::cerr << "." << std::endl;
                    }
                }

                // --- Parallel admin boundary assembly ---
                {
                    log_phase("Pass 2b: way processing", _pt, _cpu);
                    pbf.unmap(); // release 86 GiB PBF mmap before admin/S2 phases
                    std::cerr << "  Assembling admin polygons in parallel ("
                              << data.collected_relations.size() << " relations, "
                              << data.way_geometries.size() << " way geometries)..." << std::endl;

                    // Thread-local admin results (to avoid locking add_admin_polygon)
                    struct AdminResult {
                        std::vector<std::pair<double,double>> vertices;
                        std::string name;
                        uint8_t admin_level;
                        std::string country_code;
                        uint8_t place_type_override;
                    };

                    std::vector<std::vector<AdminResult>> thread_admin_results(num_threads);
                    std::atomic<size_t> rel_idx{0};
                    std::atomic<uint64_t> assembled_count{0};
                    std::atomic<uint64_t> wikidata_linked_count{0};

                    if (!wikidata_to_place_type.empty()) {
                        std::cerr << "  Wikidata place linking: " << wikidata_to_place_type.size()
                                  << " place nodes available for matching." << std::endl;
                    }

                    std::vector<std::thread> admin_workers;
                    for (unsigned int t = 0; t < num_threads; t++) {
                        admin_workers.emplace_back([&, t]() {
                            auto& local_results = thread_admin_results[t];
                            while (true) {
                                size_t i = rel_idx.fetch_add(1);
                                if (i >= data.collected_relations.size()) break;

                                const auto& rel = data.collected_relations[i];

                                // Skip level 2 border-line relations — these have names like
                                // "Deutschland - Österreich" (two countries separated by
                                // " - " or " — "). They are boundary lines, not country
                                // polygons, and produce self-intersecting geometry.
                                if (rel.admin_level == 2 && rel.country_code.empty() &&
                                    (rel.name.find(" - ") != std::string::npos ||
                                     rel.name.find(" \xe2\x80\x94 ") != std::string::npos)) { // " — " (em dash)
                                    assembled_count.fetch_add(1);
                                    continue;
                                }

                                // Skip relations with missing member ways —
                                // these cross the extract boundary and would produce
                                // incorrect partial polygons
                                bool has_missing = false;
                                for (const auto& [way_id, role] : rel.members) {
                                    if (data.way_geometries.find(way_id) == data.way_geometries.end()) {
                                        has_missing = true;
                                        break;
                                    }
                                }
                                if (has_missing) {
                                    assembled_count.fetch_add(1);
                                    continue;
                                }

                                auto rings = assemble_outer_rings(rel.members, data.way_geometries);
                                // If outer-only assembly failed, retry with all ways.
                                // Osmium ignores roles during assembly (check_roles=false) —
                                // inner ways sometimes form the boundary or bridge gaps.
                                // Filter retry results: discard rings with duplicate coords
                                // (figure-8 shapes from merging holes with outer boundary).
                                if (rings.empty()) {
                                    auto retry = assemble_outer_rings(rel.members, data.way_geometries, true);
                                    for (auto& ring : retry) {
                                        if (!ring_has_duplicate_coords(ring)) {
                                            rings.push_back(std::move(ring));
                                        }
                                    }
                                }

                                // Diagnostic: log relations with 0 rings produced
                                if (rings.empty() && !rel.members.empty()) {
                                    int total_ways = 0, found = 0, missing = 0;
                                    for (const auto& [way_id, role] : rel.members) {
                                        total_ways++;
                                        auto it = data.way_geometries.find(way_id);
                                        if (it != data.way_geometries.end() && !it->second.coords.empty()) {
                                            found++;
                                        } else {
                                            missing++;
                                        }
                                    }
                                    if (missing == 0 && found > 0) {
                                        std::string detail = "  DEBUG '" + rel.name + "': ways:";
                                        for (const auto& [way_id, role] : rel.members) {
                                            auto it = data.way_geometries.find(way_id);
                                            if (it == data.way_geometries.end()) continue;
                                            detail += " [w" + std::to_string(way_id) +
                                                      " first=" + std::to_string(it->second.first_node_id) +
                                                      " last=" + std::to_string(it->second.last_node_id) +
                                                      " n=" + std::to_string(it->second.coords.size()) + "]";
                                        }
                                        std::cerr << detail << std::endl;
                                    }
                                    std::cerr << "  WARN: relation '" << rel.name
                                              << "' (level " << (int)rel.admin_level
                                              << ", " << rel.members.size() << " members): "
                                              << "0 rings from " << total_ways << " ways ("
                                              << found << " found, " << missing << " missing)" << std::endl;
                                }

                                // Place linking, matching Nominatim's find_linked_place
                                // priority order (placex_triggers.sql):
                                //   1. label role (member with role=label, class=place)
                                //   2. wikidata (place node with matching wikidata, inside
                                //      the boundary, not already linked elsewhere)
                                //   3. place type (boundary's own place=X tag with a
                                //      matching-name place node — approximated here by
                                //      the boundary's own place tag as a last resort)
                                //   4. name match (not implemented)
                                // An authoritative label-role match stops us falling
                                // through — even if the label is a non-settlement
                                // (state/province/country) Nominatim inherits its
                                // rank_address from that node rather than trying wikidata.
                                uint8_t pto = 0;
                                bool label_checked = false;
                                if (rel.label_node_id >= 0) {
                                    auto it = label_node_to_place_type.find(rel.label_node_id);
                                    if (it != label_node_to_place_type.end()) {
                                        label_checked = true;
                                        if (it->second != static_cast<uint8_t>(PlaceType::UNKNOWN)) {
                                            pto = place_type_to_admin_override(it->second);
                                            if (pto != 0) wikidata_linked_count.fetch_add(1);
                                        }
                                    }
                                }
                                if (pto == 0 && !label_checked && !rel_wikidata[i].empty()) {
                                    auto it = wikidata_to_place_type.find(rel_wikidata[i]);
                                    if (it != wikidata_to_place_type.end()) {
                                        pto = place_type_to_admin_override(it->second);
                                        if (pto != 0) wikidata_linked_count.fetch_add(1);
                                    }
                                }
                                // Fallback: place tag on the boundary itself
                                if (pto == 0 && rel.fallback_place_type != 0) {
                                    pto = rel.fallback_place_type;
                                }

                                for (auto& ring : rings) {
                                    if (ring.size() >= 3) {
                                        AdminResult ar;
                                        ar.vertices = std::move(ring);
                                        ar.name = rel.name;
                                        ar.admin_level = rel.admin_level;
                                        ar.country_code = rel.country_code;
                                        ar.place_type_override = pto;
                                        local_results.push_back(std::move(ar));
                                    }
                                }

                                uint64_t done = assembled_count.fetch_add(1) + 1;
                                if (done % 10000 == 0) {
                                    std::cerr << "  Assembled " << done / 1000
                                              << "K admin relations..." << std::endl;
                                }
                            }
                        });
                    }
                    for (auto& w : admin_workers) w.join();
                    if (wikidata_linked_count > 0) {
                        std::cerr << "  Wikidata place linking: " << wikidata_linked_count
                                  << " admin relations linked to place types." << std::endl;
                    }
                    rel_wikidata.clear();
                    rel_wikidata.shrink_to_fit();
                    log_phase("    Admin: ring assembly", _pt, _cpu);

                    // Flatten all admin results into a single vector for parallel processing
                    std::vector<AdminResult> all_admin_results;
                    for (auto& local_results : thread_admin_results) {
                        all_admin_results.insert(all_admin_results.end(),
                            std::make_move_iterator(local_results.begin()),
                            std::make_move_iterator(local_results.end()));
                        local_results.clear();
                    }
                    uint64_t total_admin_rings = all_admin_results.size();

                    // Parallel simplification: the expensive work (normalize, simplify,
                    // compute area) is done per-polygon with no shared state.
                    struct PreparedPolygon {
                        std::vector<std::pair<double,double>> simplified;
                        std::string name;
                        uint8_t admin_level;
                        std::string country_code;
                        float area;
                        uint8_t place_type_override;
                    };
                    std::vector<PreparedPolygon> prepared(total_admin_rings);
                    {
                        std::atomic<size_t> prep_idx{0};
                        std::vector<std::thread> prep_workers;
                        for (unsigned int t = 0; t < num_threads; t++) {
                            prep_workers.emplace_back([&]() {
                                while (true) {
                                    size_t i = prep_idx.fetch_add(1);
                                    if (i >= total_admin_rings) break;
                                    auto& ar = all_admin_results[i];
                                    auto& pp = prepared[i];

                                    // Normalize ring rotation
                                    auto& vertices = ar.vertices;
                                    if (vertices.size() >= 4 &&
                                        std::fabs(vertices.front().first - vertices.back().first) < 1e-7 &&
                                        std::fabs(vertices.front().second - vertices.back().second) < 1e-7) {
                                        vertices.pop_back();
                                        auto min_it = std::min_element(vertices.begin(), vertices.end());
                                        std::rotate(vertices.begin(), min_it, vertices.end());
                                        vertices.push_back(vertices.front());
                                    }

                                    pp.simplified = simplify_admin_polygon(vertices, ar.admin_level);
                                    if (pp.simplified.size() >= 3) {
                                        pp.area = polygon_area(pp.simplified);
                                    }
                                    pp.name = std::move(ar.name);
                                    pp.admin_level = ar.admin_level;
                                    pp.country_code = std::move(ar.country_code);
                                    pp.place_type_override = ar.place_type_override;
                                }
                            });
                        }
                        for (auto& w : prep_workers) w.join();
                    }
                    all_admin_results.clear();
                    log_phase("    Admin: parallel simplify", _pt, _cpu);

                    // Sequential append + submit to S2 pool (cheap: just vector push + string intern)
                    for (auto& pp : prepared) {
                        if (pp.simplified.size() < 3) continue;

                        uint32_t poly_id = static_cast<uint32_t>(data.admin_polygons.size());
                        uint32_t vertex_offset = static_cast<uint32_t>(data.admin_vertices.size());

                        for (const auto& [lat, lng] : pp.simplified) {
                            data.admin_vertices.push_back({static_cast<float>(lat), static_cast<float>(lng)});
                        }

                        AdminPolygon poly{};
                        poly.vertex_offset = vertex_offset;
                        poly.vertex_count = static_cast<uint32_t>(pp.simplified.size());
                        poly.name_id = data.string_pool.intern(pp.name);
                        poly.admin_level = pp.admin_level;
                        poly.place_type_override = pp.place_type_override;
                        poly.area = pp.area;
                        const char* cc = pp.country_code.empty() ? nullptr : pp.country_code.c_str();
                        poly.country_code = (cc && cc[0] && cc[1])
                            ? static_cast<uint16_t>((cc[0] << 8) | cc[1]) : 0;
                        data.admin_polygons.push_back(poly);

                        admin_pool.submit(poly_id, std::move(pp.simplified));
                    }

                    std::cerr << "  Parallel admin assembly complete: "
                              << total_admin_rings << " polygon rings from "
                              << data.collected_relations.size() << " relations." << std::endl;
                    log_phase("    Admin: append + submit S2", _pt, _cpu);

                    // Free collected admin data
                    data.collected_relations.clear();
                    data.collected_relations.shrink_to_fit();

                    // --- Parallel POI relation assembly ---
                    if (!data.collected_poi_relations.empty()) {
                        std::cerr << "  Assembling POI polygons in parallel ("
                                  << data.collected_poi_relations.size() << " relations)..." << std::endl;

                        struct PoiResult {
                            std::vector<std::pair<double,double>> vertices;
                            std::string name;
                            PoiCategory category;
                            uint8_t tier;
                            uint8_t flags;
                            uint32_t qid;
                            // Relation-level addr tags carried through for
                            // AddrPoint emission alongside the POI record.
                            std::string addr_housenumber;
                            std::string addr_street;
                            std::string addr_postcode;
                        };

                        std::vector<std::vector<PoiResult>> thread_poi_results(num_threads);
                        std::atomic<size_t> poi_rel_idx{0};
                        std::atomic<uint64_t> poi_assembled_count{0};

                        std::vector<std::thread> poi_workers;
                        for (unsigned int t = 0; t < num_threads; t++) {
                            poi_workers.emplace_back([&, t]() {
                                auto& local_results = thread_poi_results[t];
                                while (true) {
                                    size_t i = poi_rel_idx.fetch_add(1);
                                    if (i >= data.collected_poi_relations.size()) break;

                                    const auto& rel = data.collected_poi_relations[i];

                                    // Skip relations with missing member ways
                                    bool has_missing = false;
                                    for (const auto& [way_id, role] : rel.members) {
                                        if (data.way_geometries.find(way_id) == data.way_geometries.end()) {
                                            has_missing = true;
                                            break;
                                        }
                                    }
                                    if (has_missing) {
                                        poi_assembled_count.fetch_add(1);
                                        continue;
                                    }

                                    auto rings = assemble_outer_rings(rel.members, data.way_geometries);
                                    if (rings.empty()) {
                                        auto retry = assemble_outer_rings(rel.members, data.way_geometries, true);
                                        for (auto& ring : retry) {
                                            if (!ring_has_duplicate_coords(ring))
                                                rings.push_back(std::move(ring));
                                        }
                                    }

                                    for (auto& ring : rings) {
                                        if (ring.size() >= 3) {
                                            // Normalize ring rotation
                                            if (ring.size() >= 4 &&
                                                std::fabs(ring.front().first - ring.back().first) < 1e-7 &&
                                                std::fabs(ring.front().second - ring.back().second) < 1e-7) {
                                                ring.pop_back();
                                                auto min_it = std::min_element(ring.begin(), ring.end());
                                                std::rotate(ring.begin(), min_it, ring.end());
                                                ring.push_back(ring.front());
                                            }

                                            // Simplify with ~50m epsilon
                                            double lat0 = ring.empty() ? 0.0 : ring[0].first;
                                            double eps_deg = meters_to_degrees(50.0, lat0);
                                            ring = simplify_polygon_epsilon(ring, eps_deg);

                                            if (ring.size() >= 3) {
                                                PoiResult pr;
                                                pr.vertices = std::move(ring);
                                                pr.name = rel.name;
                                                pr.category = rel.category;
                                                pr.tier = rel.tier;
                                                pr.flags = rel.flags;
                                                pr.qid = rel.qid;
                                                pr.addr_housenumber = rel.addr_housenumber;
                                                pr.addr_street = rel.addr_street;
                                                pr.addr_postcode = rel.addr_postcode;
                                                local_results.push_back(std::move(pr));
                                            }
                                        }
                                    }

                                    poi_assembled_count.fetch_add(1);
                                }
                            });
                        }
                        for (auto& w : poi_workers) w.join();
                        log_phase("    POI: ring assembly", _pt, _cpu);

                        // Merge POI relation results
                        uint64_t total_poi_rings = 0;
                        for (auto& local_results : thread_poi_results) {
                            for (auto& pr : local_results) {
                                // Compute centroid
                                double sum_lat = 0, sum_lng = 0;
                                for (const auto& [lat, lng] : pr.vertices) {
                                    sum_lat += lat; sum_lng += lng;
                                }
                                float clat = static_cast<float>(sum_lat / pr.vertices.size());
                                float clng = static_cast<float>(sum_lng / pr.vertices.size());

                                uint32_t vertex_offset = static_cast<uint32_t>(data.poi_vertices.size());
                                for (const auto& [lat, lng] : pr.vertices)
                                    data.poi_vertices.push_back({static_cast<float>(lat), static_cast<float>(lng)});

                                PoiRecord rec{};
                                rec.lat = clat;
                                rec.lng = clng;
                                rec.vertex_offset = vertex_offset;
                                rec.vertex_count = static_cast<uint32_t>(pr.vertices.size());
                                rec.name_id = data.string_pool.intern(pr.name);
                                rec.category = static_cast<uint8_t>(pr.category);
                                rec.tier = pr.tier;
                                rec.flags = pr.flags;
                                data.poi_records.push_back(rec);
                                poi_elevations.push_back(0); // relations don't have ele
                                poi_qids.push_back(pr.qid);
                                total_poi_rings++;

                                // Relation-level AddrPoint. Mirrors Nominatim's rank-30
                                // placex row for relations carrying addr:housenumber
                                // (e.g. White House R19761182). Without this, even though
                                // the POI wins as primary the road/house_number fields
                                // would reflect the POI name, not the addr:street tag.
                                if (!pr.addr_housenumber.empty()) {
                                    uint64_t dummy = 0;
                                    const char* bpc_ptr = pr.addr_postcode.empty() ? nullptr : pr.addr_postcode.c_str();
                                    std::vector<NodeCoord> poly_verts;
                                    poly_verts.reserve(pr.vertices.size());
                                    for (const auto& [vlat, vlng] : pr.vertices) {
                                        poly_verts.push_back({static_cast<float>(vlat), static_cast<float>(vlng)});
                                    }
                                    add_addr_point(data, clat, clng,
                                                   pr.addr_housenumber.c_str(),
                                                   pr.addr_street.c_str(),
                                                   bpc_ptr, dummy,
                                                   poly_verts.data(),
                                                   static_cast<uint32_t>(poly_verts.size()));
                                    if (!pr.addr_postcode.empty() && is_valid_postcode(pr.addr_postcode.c_str())) {
                                        uint32_t pc_id = data.string_pool.intern(pr.addr_postcode.c_str());
                                        auto& acc = data.postcode_accum[pc_id];
                                        acc.sum_lat += clat;
                                        acc.sum_lng += clng;
                                        acc.count++;
                                    }
                                }
                            }
                            local_results.clear();
                        }
                        std::cerr << "  POI relation assembly complete: "
                                  << total_poi_rings << " polygon rings from "
                                  << data.collected_poi_relations.size() << " relations." << std::endl;
                        log_phase("    POI: merge results", _pt, _cpu);
                    }
                    data.collected_poi_relations.clear();
                    data.collected_poi_relations.shrink_to_fit();

                    data.way_geometries.clear();
                    std::unordered_map<int64_t, ParsedData::WayGeometry>().swap(data.way_geometries);
                }
            }
        }

        std::cerr << "Done reading:" << std::endl;
        std::cerr << "  " << data.ways.size() << " street ways" << std::endl;
        std::cerr << "  " << data.addr_points.size() << " address points" << std::endl;
        std::cerr << "  " << data.interp_ways.size() << " interpolation ways" << std::endl;
        std::cerr << "  " << data.admin_polygons.size() << " admin polygon rings" << std::endl;
        std::cerr << "  " << data.poi_records.size() << " POI records ("
                  << data.poi_vertices.size() << " polygon vertices)" << std::endl;
        std::cerr << "  " << data.place_nodes.size() << " place nodes" << std::endl;

        // Drain admin polygon thread pool (may still be processing)
        std::cerr << "Waiting for admin polygon S2 covering to complete..." << std::endl;
        auto admin_results = admin_pool.drain();
        log_phase("    Admin: S2 covering drain", _pt, _cpu);
        for (auto& [cell_id, ids] : admin_results) {
            auto& target = data.cell_to_admin[cell_id];
            target.insert(target.end(), ids.begin(), ids.end());
        }
        std::cerr << "Admin polygon S2 covering complete (" << data.cell_to_admin.size() << " cells)." << std::endl;

        // Sort admin polygons largest-first (by vertex count descending).
        // This ensures the work-stealing loops in S2 computation and quality
        // variant simplification process expensive polygons first, avoiding
        // stragglers at the end.
        {
            size_t n = data.admin_polygons.size();
            std::vector<uint32_t> order(n);
            std::iota(order.begin(), order.end(), 0u);
            std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
                return data.admin_polygons[a].vertex_count > data.admin_polygons[b].vertex_count;
            });

            // Build old→new ID mapping
            std::vector<uint32_t> old_to_new(n);
            for (uint32_t new_id = 0; new_id < n; new_id++)
                old_to_new[order[new_id]] = new_id;

            // Reorder polygons and vertices
            std::vector<AdminPolygon> new_polys(n);
            std::vector<NodeCoord> new_verts;
            new_verts.reserve(data.admin_vertices.size());
            for (uint32_t new_id = 0; new_id < n; new_id++) {
                auto p = data.admin_polygons[order[new_id]];
                uint32_t old_offset = p.vertex_offset;
                p.vertex_offset = static_cast<uint32_t>(new_verts.size());
                new_polys[new_id] = p;
                for (uint32_t v = 0; v < p.vertex_count; v++)
                    new_verts.push_back(data.admin_vertices[old_offset + v]);
            }
            data.admin_polygons = std::move(new_polys);
            data.admin_vertices = std::move(new_verts);

            // Remap poly_ids in cell_to_admin (preserving INTERIOR_FLAG)
            for (auto& [cell_id, ids] : data.cell_to_admin) {
                for (auto& id : ids) {
                    uint32_t flags = id & INTERIOR_FLAG;
                    uint32_t old_id = id & ID_MASK;
                    id = old_to_new[old_id] | flags;
                }
            }
            std::cerr << "Sorted admin polygons largest-first (" << n << " polygons)." << std::endl;
        }

        // --- Place-node addressline containment ---
        //
        // For each place node, find the smallest-area admin polygon that
        // geometrically contains its centroid, and record that polygon's
        // id on the node. At query time the server uses this to gate the
        // nearest-place fallback so adjacent-but-wrong place nodes (Paris
        // Beaubourg beside Saint-Merri, Moscow Kitay-gorod beside
        // Ilinka, etc.) no longer bleed into the response. This mirrors
        // the effect of Nominatim's insert_addresslines containment check
        // (ST_Contains at indexing time).
        //
        // We iterate place nodes in parallel, looking up candidate admin
        // polygons via the existing cell_to_admin S2 index (fast nearest-
        // candidate filter), then running point-in-polygon on each
        // candidate in ascending-area order. The first hit is the
        // smallest containing polygon.
        if (!data.place_nodes.empty() && !data.admin_polygons.empty()) {
            auto _pn_t = std::chrono::steady_clock::now();
            std::cerr << "Computing place-node parent admin polygons ("
                      << data.place_nodes.size() << " nodes)..." << std::endl;

            // Precompute candidate sets sorted by ascending area so we can
            // pick the smallest containing polygon with a single PIP pass.
            // The cell_to_admin map has (INTERIOR_FLAG | poly_id) values —
            // strip the flag before indexing.
            std::atomic<size_t> pn_idx{0};
            std::atomic<uint64_t> contained_count{0};
            std::vector<std::thread> pn_workers;
            for (unsigned int t = 0; t < num_threads; t++) {
                pn_workers.emplace_back([&]() {
                    std::vector<uint32_t> cand;
                    while (true) {
                        size_t i = pn_idx.fetch_add(1);
                        if (i >= data.place_nodes.size()) break;
                        auto& pn = data.place_nodes[i];
                        pn.parent_poly_id = 0xFFFFFFFFu;
                        // Compute the node's S2 cell at the admin level
                        // and fetch candidate polygons covering it.
                        S2CellId cell = S2CellId(
                            S2LatLng::FromDegrees(pn.lat, pn.lng))
                            .parent(kAdminCellLevel);
                        auto it = data.cell_to_admin.find(cell.id());
                        if (it == data.cell_to_admin.end()) continue;
                        cand.clear();
                        cand.reserve(it->second.size());
                        for (uint32_t id : it->second) {
                            cand.push_back(id & 0x7FFFFFFFu);
                        }
                        // Sort candidates by ascending area so the first
                        // PIP hit is the smallest containing polygon.
                        std::sort(cand.begin(), cand.end(),
                                  [&](uint32_t a, uint32_t b) {
                            return data.admin_polygons[a].area <
                                   data.admin_polygons[b].area;
                        });
                        // Pick the smallest containing admin polygon at
                        // a rank level matching this place node's type.
                        // Mirrors Nominatim's `current_boundary` cascading
                        // gate: a place=neighbourhood node must be inside
                        // the suburb-level boundary (admin_level >= 8),
                        // not just inside the country. This prevents a
                        // neighbourhood node 800m from the query (but
                        // in the same country/state) from being accepted
                        // when the query is in a different suburb.
                        //
                        // For city/town/village (pt 0-2): parent at L3+
                        // For suburb/hamlet (pt 3-4): parent at L6+
                        // For neighbourhood/quarter (pt 5-6): parent at L9+
                        uint8_t pt = pn.place_type;
                        uint8_t min_parent_al = (pt <= 2) ? 3 : (pt <= 4) ? 6 : 9;
                        for (uint32_t poly_id : cand) {
                            const auto& p = data.admin_polygons[poly_id];
                            if (p.admin_level < min_parent_al) continue;
                            if (p.admin_level > 10) continue; // skip postcode/place markers
                            const NodeCoord* verts =
                                &data.admin_vertices[p.vertex_offset];
                            if (point_in_polygon_nc(pn.lat, pn.lng,
                                                    verts, p.vertex_count)) {
                                pn.parent_poly_id = poly_id;
                                contained_count.fetch_add(1);
                                break;
                            }
                        }
                    }
                });
            }
            for (auto& w : pn_workers) w.join();

            double _pn_elapsed = std::chrono::duration<double>(
                std::chrono::steady_clock::now() - _pn_t).count();
            std::cerr << "Place-node containment: "
                      << contained_count.load() << "/"
                      << data.place_nodes.size() << " nodes linked to a "
                      << "parent admin polygon in " << _pn_elapsed << "s"
                      << std::endl;
            log_phase("Place-node containment", _pt, _cpu);
        }

        // --- find_linked_place step 4: name-based linking ---
        //
        // Nominatim's find_linked_place (placex_triggers.sql) falls back
        // to a name-match search when label-role / wikidata / own-place-tag
        // linking all fail: it picks the nearest place=X node within
        // ~50 km whose rank_search is within ±2 of the boundary's
        // rank_address and whose name overlaps the boundary's name.
        // The matched place node's type becomes the boundary's
        // place_type_override.
        //
        // This catches settlement-rank links Nominatim makes but we
        // previously missed — e.g. a borough L6 whose name=Cuauhtémoc
        // matches a nearby place=borough node when neither has a
        // wikidata tag and no label member is set.
        if (!data.admin_polygons.empty() && !data.place_nodes.empty()) {
            auto _nl_t = std::chrono::steady_clock::now();

            // Normalised-name → place-node index for exact-match lookup.
            // Nominatim's find_linked_place step 4 requires an exact name
            // overlap via hstore `&&` (same language key, same value) —
            // effectively a full-name identity match on at least one
            // name:* tag. Our data only carries a single `name`, so we
            // approximate by matching on the normalised primary name.
            // Using exact-match avoids spurious token-overlap links like
            // "1st District of Athens" → "Athens" or "Manhattan Community
            // Board 3" → any "Manhattan" place node.
            std::unordered_map<std::string, std::vector<uint32_t>> name_to_places;
            name_to_places.reserve(data.place_nodes.size());
            const auto& pool_data = data.string_pool.data();
            for (uint32_t i = 0; i < data.place_nodes.size(); i++) {
                const auto& pn = data.place_nodes[i];
                if (pn.name_id == NO_DATA) continue;
                const char* nm = pool_data.data() + pn.name_id;
                if (!nm[0]) continue;
                std::string nn = normalise_for_matching(nm);
                if (nn.empty()) continue;
                name_to_places[std::move(nn)].push_back(i);
            }

            // PlaceType → rank_search. Matches Nominatim's
            // address_levels.json defaults: place=city 16, town 18,
            // village 19, suburb/hamlet 20, neighbourhood/quarter 22.
            auto place_rank = [](uint8_t pt) -> uint8_t {
                switch (static_cast<PlaceType>(pt)) {
                    case PlaceType::CITY:          return 16;
                    case PlaceType::TOWN:          return 18;
                    case PlaceType::BOROUGH:       return 18;
                    case PlaceType::VILLAGE:       return 19;
                    case PlaceType::SUBURB:        return 20;
                    case PlaceType::HAMLET:        return 20;
                    case PlaceType::NEIGHBOURHOOD: return 22;
                    case PlaceType::QUARTER:       return 22;
                    default:                        return 255;
                }
            };

            // Nominatim gates step 4 to boundaries with
            // rank_address 16-23 (admin_level 8-11 under the default
            // rank mapping al*2). Coarser boundaries never fall through
            // to step 4 because steps 1-3 almost always catch them
            // (country/state wikidata coverage is near-total).
            std::atomic<size_t> pol_idx{0};
            std::atomic<uint64_t> linked{0};
            std::atomic<uint64_t> considered{0};
            std::vector<std::thread> nl_workers;
            const double max_dist_deg = 0.5;
            const double max_dist_sq = max_dist_deg * max_dist_deg;

            for (unsigned int t = 0; t < num_threads; t++) {
                nl_workers.emplace_back([&]() {
                    while (true) {
                        size_t i = pol_idx.fetch_add(1);
                        if (i >= data.admin_polygons.size()) break;
                        auto& poly = data.admin_polygons[i];
                        if (poly.place_type_override != 0) continue;
                        if (poly.admin_level < 8 || poly.admin_level > 11) continue;
                        if (poly.vertex_count == 0) continue;
                        if (poly.name_id == NO_DATA) continue;
                        const char* bnd_name = pool_data.data() + poly.name_id;
                        if (!bnd_name[0]) continue;
                        considered.fetch_add(1);

                        uint8_t bnd_rank = poly.admin_level * 2;

                        double sum_lat = 0.0, sum_lng = 0.0;
                        for (uint32_t v = 0; v < poly.vertex_count; v++) {
                            sum_lat += data.admin_vertices[poly.vertex_offset + v].lat;
                            sum_lng += data.admin_vertices[poly.vertex_offset + v].lng;
                        }
                        double clat = sum_lat / poly.vertex_count;
                        double clng = sum_lng / poly.vertex_count;

                        std::string nb = normalise_for_matching(bnd_name);
                        if (nb.empty()) continue;

                        uint32_t best_idx = NO_DATA;
                        double best_dist_sq = max_dist_sq;

                        auto it = name_to_places.find(nb);
                        if (it != name_to_places.end()) {
                            for (uint32_t pidx : it->second) {
                                const auto& pn = data.place_nodes[pidx];
                                uint8_t pr = place_rank(pn.place_type);
                                if (pr == 255) continue;
                                int drank = (int)pr - (int)bnd_rank;
                                if (drank < -2 || drank > 2) continue;
                                double dlat = (double)pn.lat - clat;
                                double dlng = (double)pn.lng - clng;
                                double d2 = dlat * dlat + dlng * dlng;
                                if (d2 >= best_dist_sq) continue;
                                best_dist_sq = d2;
                                best_idx = pidx;
                            }
                        }

                        if (best_idx != NO_DATA) {
                            const auto& pn = data.place_nodes[best_idx];
                            uint8_t pto = place_type_to_admin_override(pn.place_type);
                            if (pto != 0) {
                                poly.place_type_override = pto;
                                linked.fetch_add(1);
                            }
                        }
                    }
                });
            }
            for (auto& w : nl_workers) w.join();

            double _nl_elapsed = std::chrono::duration<double>(
                std::chrono::steady_clock::now() - _nl_t).count();
            std::cerr << "find_linked_place step 4 (name match): "
                      << linked.load() << "/" << considered.load()
                      << " unlinked AL8-11 boundaries linked in "
                      << _nl_elapsed << "s" << std::endl;
            log_phase("find_linked_place step 4", _pt, _cpu);
        }

        // --- Admin polygon parent chain ---
        // For each admin polygon, find the smallest containing polygon
        // with a lower admin_level. This lets the server walk up the
        // chain (way → suburb → city → state → country) without PIP.
        {
            auto _ap_t = std::chrono::steady_clock::now();
            data.admin_parent_ids.assign(data.admin_polygons.size(), NO_DATA);
            std::atomic<size_t> ap_idx{0};
            std::vector<std::thread> workers;
            for (unsigned int t = 0; t < num_threads; t++) {
                workers.emplace_back([&]() {
                    while (true) {
                        size_t i = ap_idx.fetch_add(1);
                        if (i >= data.admin_polygons.size()) break;
                        const auto& self = data.admin_polygons[i];
                        if (self.vertex_count == 0 || self.admin_level <= 2) continue;
                        // Compute centroid
                        float sum_lat = 0, sum_lng = 0;
                        for (uint32_t v = 0; v < self.vertex_count; v++) {
                            sum_lat += data.admin_vertices[self.vertex_offset + v].lat;
                            sum_lng += data.admin_vertices[self.vertex_offset + v].lng;
                        }
                        float clat = sum_lat / self.vertex_count;
                        float clng = sum_lng / self.vertex_count;
                        S2CellId cell = S2CellId(S2LatLng::FromDegrees(clat, clng)).parent(kAdminCellLevel);
                        std::vector<S2CellId> nbrs;
                        cell.AppendAllNeighbors(kAdminCellLevel, &nbrs);

                        // Pick the closest parent: highest admin_level
                        // (most immediate), with smallest area as tiebreaker.
                        // This matches Nominatim's parent_place_id = closest
                        // containing by rank_address.
                        uint8_t best_al = 0;
                        float best_area = 1e18f;
                        uint32_t best_id = NO_DATA;
                        auto check_cell = [&](uint64_t cid) {
                            auto it = data.cell_to_admin.find(cid);
                            if (it == data.cell_to_admin.end()) return;
                            for (uint32_t raw_id : it->second) {
                                uint32_t pid = raw_id & 0x7FFFFFFF;
                                if (pid == i || pid >= data.admin_polygons.size()) continue;
                                const auto& cand = data.admin_polygons[pid];
                                if (cand.admin_level >= self.admin_level) continue;
                                // Prefer highest admin_level (closest parent)
                                if (cand.admin_level < best_al) continue;
                                if (cand.admin_level == best_al && cand.area >= best_area) continue;
                                // PIP check
                                uint32_t off = cand.vertex_offset;
                                uint32_t cnt = cand.vertex_count;
                                if (off + cnt > data.admin_vertices.size()) continue;
                                const auto* verts = &data.admin_vertices[off];
                                bool inside = false;
                                for (uint32_t a = 0, b = cnt - 1; a < cnt; b = a++) {
                                    if (((verts[a].lng > clng) != (verts[b].lng > clng)) &&
                                        (clat < (verts[b].lat - verts[a].lat) * (clng - verts[a].lng) / (verts[b].lng - verts[a].lng) + verts[a].lat))
                                        inside = !inside;
                                }
                                if (inside) {
                                    best_al = cand.admin_level;
                                    best_area = cand.area;
                                    best_id = pid;
                                }
                            }
                        };
                        check_cell(cell.id());
                        for (const auto& n : nbrs) check_cell(n.id());
                        data.admin_parent_ids[i] = best_id;
                    }
                });
            }
            for (auto& w : workers) w.join();
            double el = std::chrono::duration<double>(std::chrono::steady_clock::now() - _ap_t).count();
            uint32_t linked = 0;
            for (auto id : data.admin_parent_ids) if (id != NO_DATA) linked++;
            std::cerr << "Admin parent chain: " << linked << "/" << data.admin_polygons.size()
                      << " polygons linked in " << el << "s" << std::endl;
        }

        // --- Way parent polygon ---
        // For each street way, find the smallest admin polygon containing
        // its centroid. The server uses this to walk the admin chain from
        // the primary street without PIP.
        {
            auto _wp_t = std::chrono::steady_clock::now();
            data.way_parent_ids.assign(data.ways.size(), NO_DATA);
            data.way_postcode_ids.assign(data.ways.size(), NO_DATA);

            // Note: per-way postcode is set from postal boundary PIP only.
            // The centroid fallback (get_nearest_postcode) is handled at
            // query time by the server using the per-country-validated
            // centroid index. Previously the way sweep also did centroid
            // lookup, but that used the raw (non-per-country) accum which
            // produced wrong results from cross-country contamination.

            std::atomic<size_t> way_idx{0};
            std::atomic<uint64_t> way_linked{0};
            std::vector<std::thread> workers;
            for (unsigned int t = 0; t < num_threads; t++) {
                workers.emplace_back([&]() {
                    while (true) {
                        size_t i = way_idx.fetch_add(1);
                        if (i >= data.ways.size()) break;
                        const auto& w = data.ways[i];
                        if (w.node_count < 1) continue;
                        // Compute way centroid
                        float sum_lat = 0, sum_lng = 0;
                        uint8_t cnt = w.node_count;
                        for (uint8_t k = 0; k < cnt; k++) {
                            sum_lat += data.street_nodes[w.node_offset + k].lat;
                            sum_lng += data.street_nodes[w.node_offset + k].lng;
                        }
                        float clat = sum_lat / cnt;
                        float clng = sum_lng / cnt;
                        S2CellId cell = S2CellId(S2LatLng::FromDegrees(clat, clng)).parent(kAdminCellLevel);
                        std::vector<S2CellId> nbrs;
                        cell.AppendAllNeighbors(kAdminCellLevel, &nbrs);

                        // Pick the most specific containing polygon:
                        // highest admin_level, then smallest area.
                        // Skip admin_level > 10 (postal boundaries) for parent chain.
                        uint8_t best_al = 0;
                        float best_area = 1e18f;
                        uint32_t best_id = NO_DATA;
                        // Also find containing postal boundary for postcode
                        float best_postal_area = 1e18f;
                        uint32_t best_postal_id = NO_DATA;
                        auto check_cell = [&](uint64_t cid) {
                            auto it = data.cell_to_admin.find(cid);
                            if (it == data.cell_to_admin.end()) return;
                            for (uint32_t raw_id : it->second) {
                                uint32_t pid = raw_id & 0x7FFFFFFF;
                                if (pid >= data.admin_polygons.size()) continue;
                                const auto& cand = data.admin_polygons[pid];
                                bool dominated_parent = (cand.admin_level < best_al) ||
                                    (cand.admin_level == best_al && cand.area >= best_area);
                                bool dominated_postal = (cand.admin_level != 11) ||
                                    (cand.area >= best_postal_area);
                                if (dominated_parent && dominated_postal) continue;
                                uint32_t off = cand.vertex_offset;
                                uint32_t cnt2 = cand.vertex_count;
                                if (off + cnt2 > data.admin_vertices.size()) continue;
                                const auto* verts = &data.admin_vertices[off];
                                bool inside = false;
                                for (uint32_t a = 0, b = cnt2 - 1; a < cnt2; b = a++) {
                                    if (((verts[a].lng > clng) != (verts[b].lng > clng)) &&
                                        (clat < (verts[b].lat - verts[a].lat) * (clng - verts[a].lng) / (verts[b].lng - verts[a].lng) + verts[a].lat))
                                        inside = !inside;
                                }
                                if (inside) {
                                    if (cand.admin_level <= 10) {
                                        if (cand.admin_level > best_al ||
                                            (cand.admin_level == best_al && cand.area < best_area)) {
                                            best_al = cand.admin_level;
                                            best_area = cand.area;
                                            best_id = pid;
                                        }
                                    }
                                    if (cand.admin_level == 11 && cand.area < best_postal_area) {
                                        best_postal_area = cand.area;
                                        best_postal_id = pid;
                                    }
                                }
                            }
                        };
                        check_cell(cell.id());
                        for (const auto& n : nbrs) check_cell(n.id());
                        if (best_id != NO_DATA) {
                            data.way_parent_ids[i] = best_id;
                            way_linked.fetch_add(1);
                        }
                        if (best_postal_id != NO_DATA) {
                            data.way_postcode_ids[i] = data.admin_polygons[best_postal_id].name_id;
                        }

                        // Centroid fallback removed from way sweep — now
                        // handled at query time by the server's per-country
                        // validated centroid index (resolve_postcode tier 3).
                        if (false) {
                            uint32_t best_pc_id = NO_DATA; // dead code
                            if (best_pc_id != NO_DATA) {
                                data.way_postcode_ids[i] = best_pc_id;
                            }
                        }
                    }
                });
            }
            for (auto& w : workers) w.join();
            double el = std::chrono::duration<double>(std::chrono::steady_clock::now() - _wp_t).count();
            std::cerr << "Way parent polygon: " << way_linked.load() << "/" << data.ways.size()
                      << " ways linked in " << el << "s" << std::endl;
        }

        // --- CDP postcode inheritance ---
        // Mirrors Nominatim's calculated_postcode propagation: for each
        // addr_point or way without a postcode, inherit from a containing
        // boundary=census relation that has a postal_code tag. CDPs are
        // build-time only and are NOT written to any output file.
        if (!data.cdp_postcode_relations.empty()) {
            auto _cdp_t = std::chrono::steady_clock::now();
            std::cerr << "Assembling " << data.cdp_postcode_relations.size()
                      << " boundary=census polygons..." << std::endl;

            size_t skipped_no_geom = 0;
            for (auto& cr : data.cdp_postcode_relations) {
                bool has_missing = false;
                for (const auto& [way_id, role] : cr.members) {
                    if (data.way_geometries.find(way_id) == data.way_geometries.end()) {
                        has_missing = true;
                        break;
                    }
                }
                if (has_missing) { skipped_no_geom++; continue; }

                auto rings = assemble_outer_rings(cr.members, data.way_geometries);
                if (rings.empty()) {
                    auto retry = assemble_outer_rings(cr.members, data.way_geometries, true);
                    for (auto& r : retry) rings.push_back(std::move(r));
                }
                if (rings.empty()) { skipped_no_geom++; continue; }

                for (auto& ring : rings) {
                    if (ring.size() < 3) continue;
                    CdpPostcodePoly p;
                    p.postcode = cr.postcode;
                    p.min_lat = ring[0].first;
                    p.max_lat = ring[0].first;
                    p.min_lng = ring[0].second;
                    p.max_lng = ring[0].second;
                    for (const auto& [la, ln] : ring) {
                        if (la < p.min_lat) p.min_lat = la;
                        if (la > p.max_lat) p.max_lat = la;
                        if (ln < p.min_lng) p.min_lng = ln;
                        if (ln > p.max_lng) p.max_lng = ln;
                    }
                    p.vertices = std::move(ring);
                    data.cdp_postcode_polys.push_back(std::move(p));
                }
            }
            data.cdp_postcode_relations.clear();
            data.cdp_postcode_relations.shrink_to_fit();

            double cdp_el = std::chrono::duration<double>(std::chrono::steady_clock::now() - _cdp_t).count();
            std::cerr << "CDP assembly: " << data.cdp_postcode_polys.size()
                      << " polys built in " << cdp_el << "s ("
                      << skipped_no_geom << " skipped for missing geometry)" << std::endl;
        }

        if (!data.cdp_postcode_polys.empty()) {
            auto _inh_t = std::chrono::steady_clock::now();

            // Pre-intern postcode strings (one entry per unique postcode)
            std::unordered_map<std::string, uint32_t> pc_to_id;
            std::vector<uint32_t> cdp_offsets(data.cdp_postcode_polys.size());
            for (size_t i = 0; i < data.cdp_postcode_polys.size(); i++) {
                const auto& pc = data.cdp_postcode_polys[i].postcode;
                auto it = pc_to_id.find(pc);
                if (it == pc_to_id.end()) {
                    uint32_t off = data.string_pool.intern(pc);
                    pc_to_id[pc] = off;
                    cdp_offsets[i] = off;
                } else {
                    cdp_offsets[i] = it->second;
                }
            }

            // Cell-indexed lookup: register each CDP in every admin-level
            // cell that any of its vertices fall into, plus the 8 immediate
            // neighbours of each such cell. For typical US CDPs (small
            // enough that vertex cells + neighbours cover the interior)
            // this captures every query that could be inside the CDP.
            std::unordered_map<uint64_t, std::vector<uint32_t>> cell_to_cdp;
            for (uint32_t ci = 0; ci < data.cdp_postcode_polys.size(); ci++) {
                const auto& cdp = data.cdp_postcode_polys[ci];
                std::unordered_set<uint64_t> cells;
                for (const auto& [la, ln] : cdp.vertices) {
                    S2CellId c = S2CellId(S2LatLng::FromDegrees(la, ln)).parent(kAdminCellLevel);
                    cells.insert(c.id());
                    std::vector<S2CellId> nbrs;
                    c.AppendAllNeighbors(kAdminCellLevel, &nbrs);
                    for (const auto& n : nbrs) cells.insert(n.id());
                }
                for (uint64_t cid : cells) cell_to_cdp[cid].push_back(ci);
            }
            std::cerr << "CDP cell index: " << cell_to_cdp.size() << " cells" << std::endl;

            std::atomic<size_t> inherited_addr{0};
            std::atomic<size_t> inherited_way{0};

            // Parallel scan over addr_points
            std::atomic<size_t> addr_idx{0};
            std::vector<std::thread> addr_workers;
            for (unsigned int t = 0; t < num_threads; t++) {
                addr_workers.emplace_back([&]() {
                    while (true) {
                        size_t start = addr_idx.fetch_add(4096);
                        if (start >= data.addr_points.size()) break;
                        size_t end = std::min(start + 4096, data.addr_points.size());
                        for (size_t j = start; j < end; j++) {
                            if (j >= data.addr_postcode_ids.size()) break;
                            if (data.addr_postcode_ids[j] != NO_DATA) continue;
                            const auto& ap = data.addr_points[j];
                            S2CellId cell = S2CellId(S2LatLng::FromDegrees(ap.lat, ap.lng)).parent(kAdminCellLevel);
                            auto it = cell_to_cdp.find(cell.id());
                            if (it == cell_to_cdp.end()) continue;
                            for (uint32_t ci : it->second) {
                                const auto& cdp = data.cdp_postcode_polys[ci];
                                if ((double)ap.lat < cdp.min_lat || (double)ap.lat > cdp.max_lat) continue;
                                if ((double)ap.lng < cdp.min_lng || (double)ap.lng > cdp.max_lng) continue;
                                if (point_in_polygon((double)ap.lat, (double)ap.lng, cdp.vertices)) {
                                    data.addr_postcode_ids[j] = cdp_offsets[ci];
                                    inherited_addr.fetch_add(1);
                                    break;
                                }
                            }
                        }
                    }
                });
            }
            for (auto& w : addr_workers) w.join();

            // Parallel scan over ways (use centroid as PIP point)
            std::atomic<size_t> way_pc_idx{0};
            std::vector<std::thread> way_pc_workers;
            for (unsigned int t = 0; t < num_threads; t++) {
                way_pc_workers.emplace_back([&]() {
                    while (true) {
                        size_t start = way_pc_idx.fetch_add(4096);
                        if (start >= data.ways.size()) break;
                        size_t end = std::min(start + 4096, data.ways.size());
                        for (size_t j = start; j < end; j++) {
                            if (j >= data.way_postcode_ids.size()) break;
                            if (data.way_postcode_ids[j] != NO_DATA) continue;
                            const auto& w = data.ways[j];
                            if (w.node_count == 0) continue;
                            float sum_lat = 0, sum_lng = 0;
                            for (uint8_t k = 0; k < w.node_count; k++) {
                                sum_lat += data.street_nodes[w.node_offset + k].lat;
                                sum_lng += data.street_nodes[w.node_offset + k].lng;
                            }
                            double clat = sum_lat / w.node_count;
                            double clng = sum_lng / w.node_count;
                            S2CellId cell = S2CellId(S2LatLng::FromDegrees(clat, clng)).parent(kAdminCellLevel);
                            auto it = cell_to_cdp.find(cell.id());
                            if (it == cell_to_cdp.end()) continue;
                            for (uint32_t ci : it->second) {
                                const auto& cdp = data.cdp_postcode_polys[ci];
                                if (clat < cdp.min_lat || clat > cdp.max_lat) continue;
                                if (clng < cdp.min_lng || clng > cdp.max_lng) continue;
                                if (point_in_polygon(clat, clng, cdp.vertices)) {
                                    data.way_postcode_ids[j] = cdp_offsets[ci];
                                    inherited_way.fetch_add(1);
                                    break;
                                }
                            }
                        }
                    }
                });
            }
            for (auto& w : way_pc_workers) w.join();

            double inh_el = std::chrono::duration<double>(std::chrono::steady_clock::now() - _inh_t).count();
            std::cerr << "CDP postcode inheritance: " << inherited_addr.load()
                      << " addrs + " << inherited_way.load() << " ways inherited from "
                      << data.cdp_postcode_polys.size() << " CDPs in " << inh_el << "s" << std::endl;

            // Discard CDP polys — they are build-time only
            data.cdp_postcode_polys.clear();
            data.cdp_postcode_polys.shrink_to_fit();
        }

        // Load TIGER address data
        if (!tiger_data_path.empty()) {
            load_tiger_data(data, tiger_data_path);
        }
        if (!external_postcodes_path.empty()) {
            load_external_postcodes(data, external_postcodes_path);
        }

        // --- Parallel S2 cell computation for ways and interpolation ---
        {
            log_phase("Admin assembly", _pt, _cpu);
            std::cerr << "Computing S2 cells for ways with " << num_threads << " threads..." << std::endl;

            // Flat-array approach: threads emit (cell_id, item_id) pairs into
            // thread-local vectors, then we sort globally and group by cell_id.
            // This avoids hash maps entirely — sort is cache-friendly O(n log n).

            struct CellItem { uint64_t cell_id; uint32_t item_id; };

            auto _s2t = std::chrono::steady_clock::now();
            auto _s2cpu = CpuTicks::now();

            // Process streets: emit (cell_id, way_id) pairs
            std::cerr << "  Processing " << data.deferred_ways.size() << " street ways..." << std::endl;
            std::vector<std::vector<CellItem>> way_pairs(num_threads);
            {
                std::atomic<size_t> way_idx{0};
                std::vector<std::thread> threads;
                for (unsigned int t = 0; t < num_threads; t++) {
                    threads.emplace_back([&, t]() {
                        auto& local = way_pairs[t];
                        local.reserve(data.deferred_ways.size() / num_threads * 3);
                        std::vector<S2CellId> edge_cells;
                        std::vector<uint64_t> way_cells;
                        while (true) {
                            size_t i = way_idx.fetch_add(1);
                            if (i >= data.deferred_ways.size()) break;
                            const auto& dw = data.deferred_ways[i];
                            way_cells.clear();
                            for (uint8_t j = 0; j + 1 < dw.node_count; j++) {
                                const auto& n1 = data.street_nodes[dw.node_offset + j];
                                const auto& n2 = data.street_nodes[dw.node_offset + j + 1];
                                cover_edge(n1.lat, n1.lng, n2.lat, n2.lng, edge_cells);
                                for (const auto& c : edge_cells) way_cells.push_back(c.id());
                            }
                            std::sort(way_cells.begin(), way_cells.end());
                            way_cells.erase(std::unique(way_cells.begin(), way_cells.end()), way_cells.end());
                            for (uint64_t cell_id : way_cells) {
                                local.push_back({cell_id, dw.way_id});
                            }
                        }
                    });
                }
                for (auto& t : threads) t.join();
            }
            log_phase("  S2: street ways (parallel)", _s2t, _s2cpu);

            // Process interpolations: emit (cell_id, interp_id) pairs
            std::cerr << "  Processing " << data.deferred_interps.size() << " interpolation ways..." << std::endl;
            std::vector<std::vector<CellItem>> interp_pairs(num_threads);
            {
                std::atomic<size_t> interp_idx{0};
                std::vector<std::thread> threads;
                for (unsigned int t = 0; t < num_threads; t++) {
                    threads.emplace_back([&, t]() {
                        auto& local = interp_pairs[t];
                        std::vector<S2CellId> edge_cells;
                        std::vector<uint64_t> way_cells;
                        while (true) {
                            size_t i = interp_idx.fetch_add(1);
                            if (i >= data.deferred_interps.size()) break;
                            const auto& di = data.deferred_interps[i];
                            way_cells.clear();
                            for (uint8_t j = 0; j + 1 < di.node_count; j++) {
                                const auto& n1 = data.interp_nodes[di.node_offset + j];
                                const auto& n2 = data.interp_nodes[di.node_offset + j + 1];
                                cover_edge(n1.lat, n1.lng, n2.lat, n2.lng, edge_cells);
                                for (const auto& c : edge_cells) way_cells.push_back(c.id());
                            }
                            std::sort(way_cells.begin(), way_cells.end());
                            way_cells.erase(std::unique(way_cells.begin(), way_cells.end()), way_cells.end());
                            for (uint64_t cell_id : way_cells) {
                                local.push_back({cell_id, di.interp_id});
                            }
                        }
                    });
                }
                for (auto& t : threads) t.join();
            }
            log_phase("  S2: interp ways (parallel)", _s2t, _s2cpu);

            // Merge thread-local pairs into single vectors, sort, build cell maps
            std::cerr << "  Sorting and grouping cell pairs..." << std::endl;
            // Concatenate + sort helper for flat CellItem pairs
            // Parallel sort each thread's pairs, then k-way merge directly into
            // hash map + sorted output. No intermediate merged vector needed.
            auto parallel_sort_and_build = [](
                std::vector<std::vector<CellItem>>& thread_pairs,
                std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map,
                std::vector<CellItemPair>& sorted_out
            ) {
                // Step 1: Convert + sort each thread's data in parallel
                size_t total = 0;
                for (auto& v : thread_pairs) total += v.size();

                std::vector<std::vector<CellItemPair>> chunks(thread_pairs.size());
                {
                    std::vector<std::thread> sort_threads;
                    for (size_t t = 0; t < thread_pairs.size(); t++) {
                        sort_threads.emplace_back([&, t]() {
                            auto& src = thread_pairs[t];
                            auto& dst = chunks[t];
                            dst.reserve(src.size());
                            for (auto& ci : src) dst.push_back({ci.cell_id, ci.item_id});
                            src.clear(); src.shrink_to_fit();
                            std::sort(dst.begin(), dst.end(), [](const CellItemPair& a, const CellItemPair& b) {
                                return a.cell_id < b.cell_id || (a.cell_id == b.cell_id && a.item_id < b.item_id);
                            });
                        });
                    }
                    for (auto& t : sort_threads) t.join();
                }

                // Step 2: Concatenate sorted chunks and merge using parallel tree.
                // Each chunk is already sorted. Record run boundaries, concatenate,
                // then merge adjacent runs pairwise in parallel at each level.
                auto cmp = [](const CellItemPair& a, const CellItemPair& b) {
                    return a.cell_id < b.cell_id || (a.cell_id == b.cell_id && a.item_id < b.item_id);
                };

                // Record run boundaries before concatenation
                std::vector<size_t> run_bounds = {0};
                sorted_out.clear();
                sorted_out.reserve(total);
                for (auto& chunk : chunks) {
                    sorted_out.insert(sorted_out.end(), chunk.begin(), chunk.end());
                    run_bounds.push_back(sorted_out.size());
                    chunk.clear(); chunk.shrink_to_fit();
                }

                // Parallel tree merge: at each level, merge adjacent run pairs in parallel.
                // Level 0: merge runs (0,1), (2,3), ... → N/2 runs
                // Level 1: merge runs (01,23), (45,67), ... → N/4 runs
                // ... until one sorted run remains.
                while (run_bounds.size() > 2) {
                    std::vector<size_t> new_bounds = {0};
                    std::vector<std::thread> merge_threads;
                    for (size_t i = 0; i + 2 < run_bounds.size(); i += 2) {
                        size_t left = run_bounds[i];
                        size_t mid = run_bounds[i + 1];
                        size_t right = run_bounds[i + 2];
                        merge_threads.emplace_back([&, left, mid, right] {
                            std::inplace_merge(sorted_out.begin() + left,
                                               sorted_out.begin() + mid,
                                               sorted_out.begin() + right, cmp);
                        });
                        new_bounds.push_back(right);
                    }
                    // If odd number of runs, carry the last one forward
                    if (run_bounds.size() % 2 == 0) {
                        new_bounds.push_back(run_bounds.back());
                    }
                    for (auto& t : merge_threads) t.join();
                    run_bounds = std::move(new_bounds);
                }

                // Skip building hash map — sorted_out is used directly for writing.
                // cell_map stays empty (only needed for cache/continent modes).
            };


            // Sort each thread's pairs in parallel, then k-way merge directly
            // into hash map + sorted output. No intermediate merged vector.
            auto f_ways = std::async(std::launch::async, [&] {
                parallel_sort_and_build(way_pairs, data.cell_to_ways, data.sorted_way_cells);
            });
            auto f_interps = std::async(std::launch::async, [&] {
                parallel_sort_and_build(interp_pairs, data.cell_to_interps, data.sorted_interp_cells);
            });
            f_ways.get();
            f_interps.get();
            log_phase("  S2: sort + group into cell maps", _s2t, _s2cpu);

            // --- POI parent-street precomputation ---
            //
            // For each POI, find the nearest named street and store its
            // name_id as parent_street_id. At query time the server
            // uses this to populate the `road` field when a POI wins
            // primary-feature selection, mirroring Nominatim's
            // parent_place_id chain from a rank-30 POI back to its
            // rank-26 road.
            //
            // We iterate POIs in parallel, looking up candidate ways
            // via cell_to_ways (cell + 8 neighbours, ~30km at L10),
            // then compute point-to-segment distance for each candidate
            // and keep the closest named way.
            if (!data.poi_records.empty() && !data.ways.empty()) {
                auto _ps_t = std::chrono::steady_clock::now();
                std::cerr << "Computing POI parent-street links ("
                          << data.poi_records.size() << " POIs)..." << std::endl;

                std::atomic<size_t> poi_idx{0};
                std::atomic<uint64_t> linked_count{0};
                std::vector<std::thread> workers;
                for (unsigned int t = 0; t < num_threads; t++) {
                    workers.emplace_back([&]() {
                        std::vector<uint64_t> cells_to_check;
                        while (true) {
                            size_t i = poi_idx.fetch_add(1);
                            if (i >= data.poi_records.size()) break;
                            auto& pr = data.poi_records[i];
                            pr.parent_street_id = 0xFFFFFFFFu;

                            // POI centroid: use lat/lng for points,
                            // already-stored lat/lng for polygon centroid.
                            float plat = pr.lat;
                            float plng = pr.lng;

                            // cell_to_ways is indexed at kStreetCellLevel
                            // (L17, ~300m per cell). We walk the centre
                            // plus a 5×5 ring of neighbours = ~1.5km
                            // coverage — enough for any POI to find its
                            // nearest named street but tight enough to
                            // avoid quadratic blow-up.
                            S2CellId center = S2CellId(
                                S2LatLng::FromDegrees(plat, plng))
                                .parent(kStreetCellLevel);
                            cells_to_check.clear();
                            cells_to_check.push_back(center.id());
                            std::vector<S2CellId> ring1;
                            center.AppendAllNeighbors(
                                kStreetCellLevel, &ring1);
                            for (const auto& n : ring1) {
                                cells_to_check.push_back(n.id());
                                std::vector<S2CellId> ring2;
                                n.AppendAllNeighbors(
                                    kStreetCellLevel, &ring2);
                                for (const auto& n2 : ring2) {
                                    cells_to_check.push_back(n2.id());
                                }
                            }
                            // De-duplicate expansion.
                            std::sort(cells_to_check.begin(), cells_to_check.end());
                            cells_to_check.erase(
                                std::unique(cells_to_check.begin(), cells_to_check.end()),
                                cells_to_check.end());

                            double best_d2 = 1e18;
                            uint32_t best_name = 0xFFFFFFFFu;
                            const double cos_lat = std::cos(
                                plat * M_PI / 180.0);
                            // cell_to_ways is empty at this point
                            // (parallel_sort_and_build skips building
                            // the hash map). Binary-search the sorted
                            // vector instead and walk forward while
                            // cell_id matches.
                            for (uint64_t cid : cells_to_check) {
                                CellItemPair probe{cid, 0};
                                auto lo = std::lower_bound(
                                    data.sorted_way_cells.begin(),
                                    data.sorted_way_cells.end(), probe,
                                    [](const CellItemPair& a, const CellItemPair& b) {
                                        return a.cell_id < b.cell_id ||
                                               (a.cell_id == b.cell_id && a.item_id < b.item_id);
                                    });
                                for (auto p = lo;
                                     p != data.sorted_way_cells.end() && p->cell_id == cid;
                                     ++p) {
                                    uint32_t way_id = p->item_id;
                                    if (way_id >= data.ways.size()) continue;
                                    const auto& w = data.ways[way_id];
                                    // Only named streets matter for the
                                    // parent-road field.
                                    if (w.name_id == 0xFFFFFFFFu) continue;
                                    uint32_t off = w.node_offset;
                                    uint8_t cnt = w.node_count;
                                    if (cnt < 2) continue;
                                    if (static_cast<size_t>(off) + cnt > data.street_nodes.size()) continue;
                                    // Point-to-segment distance in
                                    // local-flat approximation (good
                                    // enough for nearest-street ranking).
                                    for (uint8_t k = 0; k + 1 < cnt; k++) {
                                        const auto& a = data.street_nodes[off + k];
                                        const auto& b = data.street_nodes[off + k + 1];
                                        double ax = (a.lng - plng) * cos_lat;
                                        double ay = (a.lat - plat);
                                        double bx = (b.lng - plng) * cos_lat;
                                        double by = (b.lat - plat);
                                        double dx = bx - ax;
                                        double dy = by - ay;
                                        double seg_len2 = dx * dx + dy * dy;
                                        double d2;
                                        if (seg_len2 < 1e-18) {
                                            d2 = ax * ax + ay * ay;
                                        } else {
                                            double tt = -(ax * dx + ay * dy) / seg_len2;
                                            if (tt < 0.0) tt = 0.0;
                                            else if (tt > 1.0) tt = 1.0;
                                            double qx = ax + tt * dx;
                                            double qy = ay + tt * dy;
                                            d2 = qx * qx + qy * qy;
                                        }
                                        if (d2 < best_d2) {
                                            best_d2 = d2;
                                            best_name = w.name_id;
                                        }
                                    }
                                }
                            }
                            if (best_name != 0xFFFFFFFFu) {
                                pr.parent_street_id = best_name;
                                linked_count.fetch_add(1);
                            }
                        }
                    });
                }
                for (auto& w : workers) w.join();

                double _elapsed = std::chrono::duration<double>(
                    std::chrono::steady_clock::now() - _ps_t).count();
                std::cerr << "POI parent-street linking: "
                          << linked_count.load() << "/"
                          << data.poi_records.size()
                          << " POIs linked to a named street in "
                          << _elapsed << "s" << std::endl;
                log_phase("  POI: parent-street linking", _s2t, _s2cpu);
            }

            // --- POI parent-postcode PIP ---
            //
            // Mirrors Nominatim's placex.postcode chain for rank-30
            // POI rows that have no addr:postcode of their own: the
            // postcode is inherited via parent_place_id, which walks
            // up through the admin chain to the nearest postal
            // boundary. We approximate this by PIP'ing each POI
            // centroid into the smallest containing boundary=postal_code
            // polygon (admin_level==11 in our schema) and storing the
            // resulting postcode string offset on the POI. The server
            // uses this field as the primary-feature postcode tier
            // when the POI wins selection (e.g. Paris Hôtel de Ville
            // POI → 75004 from the 4th arrondissement postal boundary).
            if (!data.poi_records.empty() && !data.admin_polygons.empty()) {
                auto _pp_t = std::chrono::steady_clock::now();
                std::cerr << "Computing POI parent-postcode links ("
                          << data.poi_records.size() << " POIs)..."
                          << std::endl;

                std::atomic<size_t> pp_idx{0};
                std::atomic<uint64_t> pp_linked{0};
                std::vector<std::thread> pp_workers;
                for (unsigned int t = 0; t < num_threads; t++) {
                    pp_workers.emplace_back([&]() {
                        while (true) {
                            size_t i = pp_idx.fetch_add(1);
                            if (i >= data.poi_records.size()) break;
                            auto& pr = data.poi_records[i];
                            // parent_postcode_id has default NO_DATA,
                            // set explicitly here to match the
                            // parent_street_id pattern above.
                            pr.parent_postcode_id = 0xFFFFFFFFu;

                            float plat = pr.lat;
                            float plng = pr.lng;
                            S2CellId cell = S2CellId(
                                S2LatLng::FromDegrees(plat, plng))
                                .parent(kAdminCellLevel);
                            std::vector<S2CellId> nbrs;
                            cell.AppendAllNeighbors(kAdminCellLevel, &nbrs);

                            float best_area = 1e18f;
                            uint32_t best_pid = NO_DATA;
                            auto check_cell = [&](uint64_t cid) {
                                auto it = data.cell_to_admin.find(cid);
                                if (it == data.cell_to_admin.end()) return;
                                for (uint32_t raw_id : it->second) {
                                    uint32_t pid = raw_id & 0x7FFFFFFF;
                                    if (pid >= data.admin_polygons.size()) continue;
                                    const auto& cand = data.admin_polygons[pid];
                                    if (cand.admin_level != 11) continue;
                                    if (cand.area >= best_area) continue;
                                    uint32_t off = cand.vertex_offset;
                                    uint32_t cnt2 = cand.vertex_count;
                                    if (off + cnt2 > data.admin_vertices.size()) continue;
                                    const auto* verts = &data.admin_vertices[off];
                                    bool inside = false;
                                    for (uint32_t a = 0, b = cnt2 - 1; a < cnt2; b = a++) {
                                        if (((verts[a].lng > plng) != (verts[b].lng > plng)) &&
                                            (plat < (verts[b].lat - verts[a].lat) * (plng - verts[a].lng) / (verts[b].lng - verts[a].lng) + verts[a].lat))
                                            inside = !inside;
                                    }
                                    if (inside) {
                                        best_area = cand.area;
                                        best_pid = pid;
                                    }
                                }
                            };
                            check_cell(cell.id());
                            for (const auto& n : nbrs) check_cell(n.id());

                            if (best_pid != NO_DATA) {
                                pr.parent_postcode_id =
                                    data.admin_polygons[best_pid].name_id;
                                pp_linked.fetch_add(1);
                            }
                        }
                    });
                }
                for (auto& w : pp_workers) w.join();

                double _pp_el = std::chrono::duration<double>(
                    std::chrono::steady_clock::now() - _pp_t).count();
                std::cerr << "POI parent-postcode linking: "
                          << pp_linked.load() << "/"
                          << data.poi_records.size()
                          << " POIs linked to a postal boundary in "
                          << _pp_el << "s" << std::endl;
                log_phase("  POI: parent-postcode linking", _s2t, _s2cpu);
            }

            // --- Address-point parent-street backfill ---
            //
            // Nominatim indexes any rank-30 row with addr:housenumber
            // as an address point and resolves its street via
            // parent_place_id at import time. Our node/way extraction
            // now accepts housenumber without addr:street — those
            // rows are stored with street_id == NO_DATA and need to
            // be backfilled with the name_id of the nearest named
            // street. We iterate addr_points in parallel, looking up
            // candidate ways via sorted_way_cells at L17 (same as the
            // POI sweep above) and assign the closest named-way's
            // name_id.
            if (!data.addr_points.empty() && !data.ways.empty()) {
                auto _as_t = std::chrono::steady_clock::now();
                auto _as_cpu = CpuTicks::now();
                std::atomic<uint64_t> backfill_candidates{0};
                for (const auto& p : data.addr_points) {
                    if (p.street_id == NO_DATA) backfill_candidates.fetch_add(1);
                }
                std::cerr << "Backfilling addr_point parent streets ("
                          << backfill_candidates.load() << "/"
                          << data.addr_points.size() << " need it)..."
                          << std::endl;

                std::atomic<size_t> ap_idx{0};
                std::atomic<uint64_t> ap_linked{0};
                const auto& pool_data = data.string_pool.data();
                std::vector<std::thread> ap_workers;
                for (unsigned int t = 0; t < num_threads; t++) {
                    ap_workers.emplace_back([&]() {
                        std::vector<uint64_t> cells_to_check;
                        while (true) {
                            size_t i = ap_idx.fetch_add(1);
                            if (i >= data.addr_points.size()) break;
                            auto& ap = data.addr_points[i];
                            ap.parent_way_id = NO_DATA;

                            float plat = ap.lat;
                            float plng = ap.lng;
                            S2CellId center = S2CellId(
                                S2LatLng::FromDegrees(plat, plng))
                                .parent(kStreetCellLevel);
                            cells_to_check.clear();
                            cells_to_check.push_back(center.id());
                            std::vector<S2CellId> ring1;
                            center.AppendAllNeighbors(kStreetCellLevel, &ring1);
                            for (const auto& n : ring1) {
                                cells_to_check.push_back(n.id());
                                std::vector<S2CellId> ring2;
                                n.AppendAllNeighbors(kStreetCellLevel, &ring2);
                                for (const auto& n2 : ring2) {
                                    cells_to_check.push_back(n2.id());
                                }
                            }
                            std::sort(cells_to_check.begin(), cells_to_check.end());
                            cells_to_check.erase(
                                std::unique(cells_to_check.begin(), cells_to_check.end()),
                                cells_to_check.end());

                            double best_d2 = 1e18;
                            uint32_t best_name = NO_DATA;
                            uint32_t best_way_idx = NO_DATA;
                            // Token-matched resolution: track nearest
                            // way whose original name matches addr:street
                            double best_match_d2 = 1e18;
                            uint32_t best_match_name = NO_DATA;
                            uint32_t best_match_way = NO_DATA;
                            const char* addr_street_str = nullptr;
                            if (ap.street_id != NO_DATA) {
                                addr_street_str = pool_data.data() + ap.street_id;
                            }
                            const double cos_lat = std::cos(
                                plat * M_PI / 180.0);
                            for (uint64_t cid : cells_to_check) {
                                CellItemPair probe{cid, 0};
                                auto lo = std::lower_bound(
                                    data.sorted_way_cells.begin(),
                                    data.sorted_way_cells.end(), probe,
                                    [](const CellItemPair& a, const CellItemPair& b) {
                                        return a.cell_id < b.cell_id ||
                                               (a.cell_id == b.cell_id && a.item_id < b.item_id);
                                    });
                                for (auto p = lo;
                                     p != data.sorted_way_cells.end() && p->cell_id == cid;
                                     ++p) {
                                    uint32_t way_id = p->item_id;
                                    if (way_id >= data.ways.size()) continue;
                                    const auto& w = data.ways[way_id];
                                    if (w.name_id == NO_DATA) continue;
                                    uint32_t off = w.node_offset;
                                    uint8_t cnt = w.node_count;
                                    if (cnt < 2) continue;
                                    if (static_cast<size_t>(off) + cnt > data.street_nodes.size()) continue;
                                    for (uint8_t k = 0; k + 1 < cnt; k++) {
                                        const auto& a = data.street_nodes[off + k];
                                        const auto& b = data.street_nodes[off + k + 1];
                                        double ax = (a.lng - plng) * cos_lat;
                                        double ay = (a.lat - plat);
                                        double bx = (b.lng - plng) * cos_lat;
                                        double by = (b.lat - plat);
                                        double dx = bx - ax;
                                        double dy = by - ay;
                                        double seg_len2 = dx * dx + dy * dy;
                                        double d2;
                                        if (seg_len2 < 1e-18) {
                                            d2 = ax * ax + ay * ay;
                                        } else {
                                            double tt = -(ax * dx + ay * dy) / seg_len2;
                                            if (tt < 0.0) tt = 0.0;
                                            else if (tt > 1.0) tt = 1.0;
                                            double qx = ax + tt * dx;
                                            double qy = ay + tt * dy;
                                            d2 = qx * qx + qy * qy;
                                        }
                                        if (d2 < best_d2) {
                                            best_d2 = d2;
                                            best_name = w.name_id;
                                            best_way_idx = way_id;
                                        }
                                        // Token-matched: check if way's
                                        // original name matches addr:street
                                        if (d2 < best_match_d2 && addr_street_str
                                            && way_id < data.way_orig_name_ids.size()) {
                                            uint32_t orig_id = data.way_orig_name_ids[way_id];
                                            if (orig_id != NO_DATA) {
                                                const char* orig_str = pool_data.data() + orig_id;
                                                if (tokens_overlap(addr_street_str, orig_str)) {
                                                    best_match_d2 = d2;
                                                    best_match_name = w.name_id;
                                                    best_match_way = way_id;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if (best_way_idx != NO_DATA) {
                                // Prefer token-matched way (matches
                                // Nominatim's getNearestNamedRoadPlaceId
                                // which finds the closest street whose
                                // name tokens overlap with addr:street).
                                if (best_match_way != NO_DATA) {
                                    ap.parent_way_id = best_match_way;
                                    ap.street_id = best_match_name;
                                } else {
                                    ap.parent_way_id = best_way_idx;
                                    if (ap.street_id == NO_DATA && best_name != NO_DATA) {
                                        ap.street_id = best_name;
                                    }
                                }
                                ap_linked.fetch_add(1);
                            }
                        }
                    });
                }
                for (auto& w : ap_workers) w.join();
                double _ap_elapsed = std::chrono::duration<double>(
                    std::chrono::steady_clock::now() - _as_t).count();
                std::cerr << "Addr parent-street backfill: "
                          << ap_linked.load() << "/"
                          << backfill_candidates.load()
                          << " addr_points linked in "
                          << _ap_elapsed << "s" << std::endl;
                log_phase("  Addr: parent-street backfill", _as_t, _as_cpu);
            }

            // Free deferred work items
            data.deferred_ways.clear();
            data.deferred_ways.shrink_to_fit();
            data.deferred_interps.clear();
            data.deferred_interps.shrink_to_fit();

            // --- POI S2 cell computation ---
            if (!data.poi_records.empty()) {
                std::cerr << "  Computing S2 cells for " << data.poi_records.size() << " POIs..." << std::endl;
                std::vector<std::vector<CellItem>> poi_pairs(num_threads);
                {
                    std::atomic<size_t> poi_idx{0};
                    std::vector<std::thread> threads;
                    for (unsigned int t = 0; t < num_threads; t++) {
                        threads.emplace_back([&, t]() {
                            auto& local = poi_pairs[t];
                            local.reserve(data.poi_records.size() / num_threads * 2);
                            while (true) {
                                size_t i = poi_idx.fetch_add(1);
                                if (i >= data.poi_records.size()) break;
                                const auto& pr = data.poi_records[i];

                                if (pr.vertex_count == 0) {
                                    // Point POI
                                    S2CellId cell = S2CellId(S2LatLng::FromDegrees(pr.lat, pr.lng))
                                        .parent(kAdminCellLevel);
                                    local.push_back({cell.id(), static_cast<uint32_t>(i)});
                                } else {
                                    // Polygon POI — cover with S2 cells
                                    std::vector<std::pair<double,double>> verts;
                                    verts.reserve(pr.vertex_count);
                                    for (uint32_t j = 0; j < pr.vertex_count; j++) {
                                        const auto& v = data.poi_vertices[pr.vertex_offset + j];
                                        verts.emplace_back(v.lat, v.lng);
                                    }
                                    auto cells = cover_polygon(verts);
                                    for (const auto& [cell_id, is_interior] : cells) {
                                        uint32_t id = static_cast<uint32_t>(i);
                                        if (is_interior) id |= INTERIOR_FLAG;
                                        local.push_back({cell_id.id(), id});
                                    }
                                }
                            }
                        });
                    }
                    for (auto& t : threads) t.join();
                }
                log_phase("  S2: POI cells (parallel)", _s2t, _s2cpu);

                parallel_sort_and_build(poi_pairs, data.cell_to_pois, data.sorted_poi_cells);
                log_phase("  S2: POI sort + group", _s2t, _s2cpu);
            }

            // Place node S2 cells
            if (!data.place_nodes.empty()) {
                std::cerr << "  Computing S2 cells for " << data.place_nodes.size() << " place nodes..." << std::endl;
                std::vector<std::vector<CellItem>> place_pairs(num_threads);
                {
                    std::atomic<size_t> place_idx{0};
                    std::vector<std::thread> threads;
                    for (unsigned int t = 0; t < num_threads; t++) {
                        threads.emplace_back([&, t]() {
                            auto& local = place_pairs[t];
                            while (true) {
                                size_t i = place_idx.fetch_add(1);
                                if (i >= data.place_nodes.size()) break;
                                const auto& pn = data.place_nodes[i];
                                S2CellId cell = S2CellId(S2LatLng::FromDegrees(pn.lat, pn.lng))
                                    .parent(kAdminCellLevel);
                                local.push_back({cell.id(), static_cast<uint32_t>(i)});
                            }
                        });
                    }
                    for (auto& t : threads) t.join();
                }
                std::unordered_map<uint64_t, std::vector<uint32_t>> dummy_map;
                parallel_sort_and_build(place_pairs, dummy_map, data.sorted_place_cells);
                std::cerr << "  Place nodes: " << data.place_nodes.size() << " nodes, "
                          << data.sorted_place_cells.size() << " cell pairs" << std::endl;
                log_phase("  S2: place nodes", _s2t, _s2cpu);
            }

            std::cerr << "S2 cell computation complete." << std::endl;
        }

        // Resolve interpolation endpoints
        std::cerr << "Resolving interpolation endpoints..." << std::endl;
        resolve_interpolation_endpoints(data);

        // Deduplicate + convert to sorted pairs for fast writing
        log_phase("S2 cell computation", _pt, _cpu);
        std::cerr << "Deduplicating + sorting for write..." << std::endl;
        {
            // Convert addr hash map to sorted pairs.
            // Sort cell IDs only (30M unique), then build pairs in sorted order.
            // Much faster than sorting all 160M pairs.
            auto f2 = std::async(std::launch::async, [&] {
                auto _dt = std::chrono::steady_clock::now();
                auto _dc = CpuTicks::now();

                // Step 1: Extract cell IDs from hash map
                std::vector<uint64_t> sorted_cells;
                sorted_cells.reserve(data.cell_to_addrs.size());
                for (auto& [cell_id, ids] : data.cell_to_addrs)
                    sorted_cells.push_back(cell_id);
                log_phase("    Dedup: extract cell IDs", _dt, _dc);

                // Step 2: Sort cell IDs
                std::sort(sorted_cells.begin(), sorted_cells.end());
                log_phase("    Dedup: sort cell IDs", _dt, _dc);

                // Step 3: Parallel dedup within each cell
                {
                    unsigned nthreads = std::thread::hardware_concurrency();
                    size_t chunk = (sorted_cells.size() + nthreads - 1) / nthreads;
                    std::vector<std::thread> threads;
                    for (unsigned t = 0; t < nthreads; t++) {
                        size_t start = t * chunk;
                        size_t end = std::min(start + chunk, sorted_cells.size());
                        if (start >= sorted_cells.size()) break;
                        threads.emplace_back([&, start, end]() {
                            for (size_t i = start; i < end; i++) {
                                auto& ids = data.cell_to_addrs[sorted_cells[i]];
                                std::sort(ids.begin(), ids.end());
                                ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
                            }
                        });
                    }
                    for (auto& t : threads) t.join();
                }
                log_phase("    Dedup: per-cell sort+dedup", _dt, _dc);

                // Step 4: Count total pairs
                size_t total_pairs = 0;
                for (auto& [_, ids] : data.cell_to_addrs) total_pairs += ids.size();
                log_phase("    Dedup: count pairs", _dt, _dc);

                // Step 5: Build sorted pairs
                std::vector<CellItemPair> pairs;
                pairs.reserve(total_pairs);
                for (uint64_t cell_id : sorted_cells) {
                    auto& ids = data.cell_to_addrs[cell_id];
                    for (auto id : ids) pairs.push_back({cell_id, id});
                }
                data.sorted_addr_cells = std::move(pairs);
                log_phase("    Dedup: build sorted pairs", _dt, _dc);
            });
            auto f4 = std::async(std::launch::async, [&]{ deduplicate(data.cell_to_admin); });
            f2.get(); f4.get();
        }

        // Save cache if requested
        if (!save_cache_path.empty()) {
            serialize_cache(data, save_cache_path);
        }
    }

    // --- Rebuild hash maps from sorted pairs if needed for cache saving ---
    // (Continent filtering now uses sorted pairs directly, so no rebuild needed for that.)
    if (!save_cache_path.empty()) {
        // Parallel rebuild: split sorted pairs into chunks, each thread builds
        // a partial map, then merge. Sorted pairs are grouped by cell_id so we
        // can split at cell boundaries for zero-conflict parallel insertion.
        auto rebuild_map_parallel = [](const std::vector<CellItemPair>& sorted,
                                        std::unordered_map<uint64_t, std::vector<uint32_t>>& map) {
            if (sorted.empty() || !map.empty()) return;
            unsigned nthreads = std::thread::hardware_concurrency();
            if (nthreads == 0) nthreads = 4;
            size_t chunk = (sorted.size() + nthreads - 1) / nthreads;

            // Find cell boundaries for clean splits
            std::vector<size_t> boundaries = {0};
            for (unsigned t = 1; t < nthreads; t++) {
                size_t target = t * chunk;
                if (target >= sorted.size()) break;
                // Advance to next cell boundary
                while (target < sorted.size() && sorted[target].cell_id == sorted[target-1].cell_id)
                    target++;
                if (target < sorted.size()) boundaries.push_back(target);
            }
            boundaries.push_back(sorted.size());

            // Each thread builds its own sub-map
            std::vector<std::unordered_map<uint64_t, std::vector<uint32_t>>> sub_maps(boundaries.size() - 1);
            std::vector<std::thread> threads;
            for (size_t t = 0; t + 1 < boundaries.size(); t++) {
                threads.emplace_back([&, t]() {
                    auto& sub = sub_maps[t];
                    // Pre-estimate capacity
                    size_t n = boundaries[t+1] - boundaries[t];
                    sub.reserve(n / 3); // rough estimate of unique cells
                    for (size_t i = boundaries[t]; i < boundaries[t+1]; i++) {
                        sub[sorted[i].cell_id].push_back(sorted[i].item_id);
                    }
                });
            }
            for (auto& t : threads) t.join();

            // Merge sub-maps into main map (no conflicts since splits are at cell boundaries)
            size_t total_cells = 0;
            for (auto& sub : sub_maps) total_cells += sub.size();
            map.reserve(total_cells);
            for (auto& sub : sub_maps) {
                for (auto& [k, v] : sub) {
                    map[k] = std::move(v);
                }
            }
        };

        std::cerr << "Rebuilding cell maps for continent filtering..." << std::endl;
        auto f1 = std::async(std::launch::async, [&]{ rebuild_map_parallel(data.sorted_way_cells, data.cell_to_ways); });
        auto f2 = std::async(std::launch::async, [&]{ rebuild_map_parallel(data.sorted_interp_cells, data.cell_to_interps); });
        f1.get(); f2.get();
        log_phase("Rebuild cell maps", _pt, _cpu);
    }

    // --- Deterministic ordering ---
    // Sort all data by canonical keys so that the same PBF input always produces
    // the same binary output, regardless of thread scheduling. This is required
    // for incremental patching — patches between deterministic builds are small
    // because matching records land at the same file offsets.
    {
        std::cerr << "Deterministic ordering..." << std::endl;
        auto _st = std::chrono::steady_clock::now();
        auto _sc = CpuTicks::now();

        // 1. Sort string pool alphabetically, remap all string offsets
        {
            auto& pool_data = data.string_pool.mutable_data();
            if (!pool_data.empty()) {
                // Extract strings
                std::vector<std::pair<uint32_t, const char*>> strings;
                size_t pos = 0;
                while (pos < pool_data.size()) {
                    strings.emplace_back(static_cast<uint32_t>(pos), pool_data.data() + pos);
                    pos += strlen(pool_data.data() + pos) + 1;
                }
                std::sort(strings.begin(), strings.end(), [](const auto& a, const auto& b) {
                    return strcmp(a.second, b.second) < 0;
                });

                // Build remap + new pool
                std::unordered_map<uint32_t, uint32_t> remap;
                remap.reserve(strings.size());
                std::vector<char> new_data;
                new_data.reserve(pool_data.size());
                for (auto& [old_off, str] : strings) {
                    remap[old_off] = static_cast<uint32_t>(new_data.size());
                    size_t len = strlen(str);
                    new_data.insert(new_data.end(), str, str + len + 1);
                }
                pool_data = std::move(new_data);

                // Apply remap to all data records
                const auto& rm = remap;
                for (auto& w : data.ways) w.name_id = rm.at(w.name_id);
                for (auto& a : data.addr_points) {
                    a.housenumber_id = rm.at(a.housenumber_id);
                    // street_id may be NO_DATA if the parent-street
                    // backfill sweep didn't find a nearby named way.
                    if (a.street_id != NO_DATA) a.street_id = rm.at(a.street_id);
                }
                for (auto& iw : data.interp_ways) iw.street_id = rm.at(iw.street_id);
                for (auto& ap : data.admin_polygons) ap.name_id = rm.at(ap.name_id);
                for (auto& pr : data.poi_records) {
                    pr.name_id = rm.at(pr.name_id);
                    if (pr.parent_street_id != 0xFFFFFFFFu) {
                        auto psit = rm.find(pr.parent_street_id);
                        if (psit != rm.end()) {
                            pr.parent_street_id = psit->second;
                        } else {
                            pr.parent_street_id = 0xFFFFFFFFu;
                        }
                    }
                    if (pr.parent_postcode_id != 0xFFFFFFFFu) {
                        auto ppit = rm.find(pr.parent_postcode_id);
                        if (ppit != rm.end()) {
                            pr.parent_postcode_id = ppit->second;
                        } else {
                            pr.parent_postcode_id = 0xFFFFFFFFu;
                        }
                    }
                }
                for (auto& pn : data.place_nodes) pn.name_id = rm.at(pn.name_id);
                // Remap way_postcode_ids and addr_postcode_ids (string offsets)
                auto remap_pc_vec = [&](std::vector<uint32_t>& v) {
                    for (auto& pc : v) {
                        if (pc != NO_DATA) {
                            auto it = rm.find(pc);
                            if (it != rm.end()) pc = it->second;
                            else pc = NO_DATA;
                        }
                    }
                };
                remap_pc_vec(data.way_postcode_ids);
                remap_pc_vec(data.addr_postcode_ids);
                // Remap postcode_accum keys and build centroid vector
                {
                    std::unordered_map<uint32_t, ParsedData::PostcodeAccum> remapped;
                    remapped.reserve(data.postcode_accum.size());
                    for (auto& [old_id, acc] : data.postcode_accum) {
                        auto it = rm.find(old_id);
                        if (it != rm.end()) {
                            remapped[it->second] = acc;
                        }
                    }
                    data.postcode_accum = std::move(remapped);
                }

                std::cerr << "  String pool sorted: " << strings.size() << " strings" << std::endl;
            }
        }
        log_phase("  Sort strings", _st, _sc);

        // Helper: reinterpret float as uint32 for total ordering (works for IEEE 754)
        auto float_bits = [](float v) -> uint32_t {
            uint32_t bits;
            memcpy(&bits, &v, 4);
            // Handle negative floats: flip all bits if sign bit set, else flip sign bit
            return (bits & 0x80000000) ? ~bits : (bits ^ 0x80000000);
        };

        // 2. Sort addr_points by (street_id, housenumber_id, lat_bits, lng_bits)
        //    Using string offsets directly (already remapped to sorted pool) gives
        //    deterministic order. Raw float bits as final tiebreaker for total order.
        if (!data.sorted_addr_cells.empty()) {
            size_t n = data.addr_points.size();
            std::vector<uint32_t> order(n);
            std::iota(order.begin(), order.end(), 0);
            std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
                const auto& pa = data.addr_points[a];
                const auto& pb = data.addr_points[b];
                if (pa.street_id != pb.street_id) return pa.street_id < pb.street_id;
                if (pa.housenumber_id != pb.housenumber_id) return pa.housenumber_id < pb.housenumber_id;
                uint32_t la = float_bits(pa.lat), lb = float_bits(pb.lat);
                if (la != lb) return la < lb;
                uint32_t ga = float_bits(pa.lng), gb = float_bits(pb.lng);
                if (ga != gb) return ga < gb;
                return a < b; // stable tiebreaker: original index
            });
            std::vector<uint32_t> old_to_new(n);
            for (uint32_t i = 0; i < n; i++) old_to_new[order[i]] = i;
            std::vector<AddrPoint> sorted(n);
            for (uint32_t i = 0; i < n; i++) sorted[i] = data.addr_points[order[i]];
            // Reorder vertex buffer alongside addr_points. Copy each
            // record's polygon vertices into a new buffer and update
            // vertex_offset in the sorted array.
            std::vector<NodeCoord> new_addr_vertices;
            new_addr_vertices.reserve(data.addr_vertices.size());
            for (auto& a : sorted) {
                if (a.vertex_count > 0 && a.vertex_offset != NO_DATA) {
                    uint32_t old_off = a.vertex_offset;
                    a.vertex_offset = static_cast<uint32_t>(new_addr_vertices.size());
                    for (uint32_t j = 0; j < a.vertex_count; j++)
                        new_addr_vertices.push_back(data.addr_vertices[old_off + j]);
                }
            }
            data.addr_vertices = std::move(new_addr_vertices);
            // Dedup consecutive identical records (planet has ~4M duplicates).
            // Compare everything except vertex_offset — same polygon content
            // stored at different offsets should still dedup.
            auto addr_equal = [](const AddrPoint& a, const AddrPoint& b) {
                return a.lat == b.lat && a.lng == b.lng &&
                       a.housenumber_id == b.housenumber_id &&
                       a.street_id == b.street_id &&
                       a.parent_way_id == b.parent_way_id &&
                       a.vertex_count == b.vertex_count;
            };
            std::vector<uint32_t> dedup_remap(n);
            size_t write_pos = 0;
            for (size_t i = 0; i < n; i++) {
                if (write_pos == 0 || !addr_equal(sorted[i], sorted[write_pos - 1])) {
                    sorted[write_pos] = sorted[i];
                    dedup_remap[i] = static_cast<uint32_t>(write_pos);
                    write_pos++;
                } else {
                    dedup_remap[i] = static_cast<uint32_t>(write_pos - 1);
                }
            }
            sorted.resize(write_pos);
            data.addr_points = std::move(sorted);
            // Reorder + dedup addr_postcode_ids in parallel
            if (data.addr_postcode_ids.size() == n) {
                std::vector<uint32_t> sorted_pc(n);
                for (uint32_t i = 0; i < n; i++) sorted_pc[i] = data.addr_postcode_ids[order[i]];
                // Dedup: keep the first occurrence's postcode
                std::vector<uint32_t> deduped_pc(write_pos, NO_DATA);
                for (size_t i = 0; i < n; i++) {
                    uint32_t new_idx = dedup_remap[i];
                    if (deduped_pc[new_idx] == NO_DATA) deduped_pc[new_idx] = sorted_pc[i];
                }
                data.addr_postcode_ids = std::move(deduped_pc);
            }
            if (write_pos < n)
                std::cerr << "  Deduped addr_points: " << n << " → " << write_pos
                          << " (-" << (n - write_pos) << ")" << std::endl;
            // Remap IDs through both old→new and dedup
            for (auto& p : data.sorted_addr_cells)
                p.item_id = dedup_remap[old_to_new[p.item_id]];
            auto cmp = [](const CellItemPair& a, const CellItemPair& b) {
                return a.cell_id < b.cell_id || (a.cell_id == b.cell_id && a.item_id < b.item_id);
            };
            std::sort(data.sorted_addr_cells.begin(), data.sorted_addr_cells.end(), cmp);
            data.cell_to_addrs.clear();
            std::cerr << "  Addr points sorted: " << n << std::endl;
        }
        log_phase("  Sort addr_points", _st, _sc);

        // 3. Sort ways by (name, node_count, first_node) + reorder nodes
        if (!data.ways.empty()) {
            size_t n = data.ways.size();
            std::vector<uint32_t> order(n);
            std::iota(order.begin(), order.end(), 0);
            const auto& sp = data.string_pool.data();
            std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
                const auto& wa = data.ways[a];
                const auto& wb = data.ways[b];
                if (wa.name_id != wb.name_id) return wa.name_id < wb.name_id;
                if (wa.node_count != wb.node_count) return wa.node_count < wb.node_count;
                // Compare all nodes for total order
                uint8_t nc = std::min(wa.node_count, wb.node_count);
                for (uint8_t j = 0; j < nc; j++) {
                    uint32_t la = float_bits(data.street_nodes[wa.node_offset + j].lat);
                    uint32_t lb = float_bits(data.street_nodes[wb.node_offset + j].lat);
                    if (la != lb) return la < lb;
                    uint32_t ga = float_bits(data.street_nodes[wa.node_offset + j].lng);
                    uint32_t gb = float_bits(data.street_nodes[wb.node_offset + j].lng);
                    if (ga != gb) return ga < gb;
                }
                return false;
            });
            // Reorder ways + nodes by sort order, dedup identical consecutive ways
            std::vector<WayHeader> new_ways;
            std::vector<NodeCoord> new_nodes;
            new_ways.reserve(n);
            new_nodes.reserve(data.street_nodes.size());
            std::vector<uint32_t> old_to_new(n);

            for (uint32_t i = 0; i < n; i++) {
                auto w = data.ways[order[i]];
                uint32_t old_off = w.node_offset;
                uint8_t nc = w.node_count;

                // Check if this way is identical to the previous one (dedup)
                bool is_dup = false;
                if (!new_ways.empty()) {
                    auto& prev = new_ways.back();
                    if (prev.name_id == w.name_id && prev.node_count == nc) {
                        is_dup = true;
                        for (uint8_t j = 0; j < nc && is_dup; j++) {
                            auto& pn = data.street_nodes[old_off + j];
                            auto& qn = new_nodes[prev.node_offset + j];
                            if (memcmp(&pn, &qn, sizeof(NodeCoord)) != 0) is_dup = false;
                        }
                    }
                }

                if (is_dup) {
                    // Map to the previous (kept) way
                    old_to_new[order[i]] = static_cast<uint32_t>(new_ways.size() - 1);
                } else {
                    old_to_new[order[i]] = static_cast<uint32_t>(new_ways.size());
                    w.node_offset = static_cast<uint32_t>(new_nodes.size());
                    for (uint8_t j = 0; j < nc; j++)
                        new_nodes.push_back(data.street_nodes[old_off + j]);
                    new_ways.push_back(w);
                }
            }
            size_t deduped = n - new_ways.size();
            data.ways = std::move(new_ways);
            data.street_nodes = std::move(new_nodes);
            for (auto& p : data.sorted_way_cells) p.item_id = old_to_new[p.item_id];
            auto cmp = [](const CellItemPair& a, const CellItemPair& b) {
                return a.cell_id < b.cell_id || (a.cell_id == b.cell_id && a.item_id < b.item_id);
            };
            std::sort(data.sorted_way_cells.begin(), data.sorted_way_cells.end(), cmp);
            data.cell_to_ways.clear();
            // Remap way_parent_ids + way_postcode_ids: reorder + dedup
            auto remap_way_vec = [&](std::vector<uint32_t>& vec) {
                if (vec.size() != n) return;
                std::vector<uint32_t> nv(data.ways.size(), NO_DATA);
                for (uint32_t i = 0; i < n; i++) {
                    uint32_t new_idx = old_to_new[i];
                    if (new_idx < nv.size()) {
                        uint32_t val = vec[i];
                        if (nv[new_idx] == NO_DATA || val != NO_DATA)
                            nv[new_idx] = val;
                    }
                }
                vec = std::move(nv);
            };
            remap_way_vec(data.way_parent_ids);
            remap_way_vec(data.way_postcode_ids);
            // Remap addr_point parent_way_id references through way old_to_new
            for (auto& a : data.addr_points) {
                if (a.parent_way_id != NO_DATA && a.parent_way_id < n) {
                    a.parent_way_id = old_to_new[a.parent_way_id];
                }
            }
            std::cerr << "  Ways sorted: " << n << " (" << deduped << " duplicates removed)" << std::endl;
        }
        log_phase("  Sort ways", _st, _sc);

        // 4. Sort interps by (street, start, end, type) + reorder nodes
        if (!data.interp_ways.empty()) {
            size_t n = data.interp_ways.size();
            std::vector<uint32_t> order(n);
            std::iota(order.begin(), order.end(), 0);
            const auto& sp = data.string_pool.data();
            std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
                const auto& ia = data.interp_ways[a];
                const auto& ib = data.interp_ways[b];
                if (ia.street_id != ib.street_id) return ia.street_id < ib.street_id;
                if (ia.start_number != ib.start_number) return ia.start_number < ib.start_number;
                if (ia.end_number != ib.end_number) return ia.end_number < ib.end_number;
                if (ia.interpolation != ib.interpolation) return ia.interpolation < ib.interpolation;
                uint8_t nc = std::min(ia.node_count, ib.node_count);
                for (uint8_t j = 0; j < nc; j++) {
                    uint32_t la = float_bits(data.interp_nodes[ia.node_offset + j].lat);
                    uint32_t lb = float_bits(data.interp_nodes[ib.node_offset + j].lat);
                    if (la != lb) return la < lb;
                    uint32_t ga = float_bits(data.interp_nodes[ia.node_offset + j].lng);
                    uint32_t gb = float_bits(data.interp_nodes[ib.node_offset + j].lng);
                    if (ga != gb) return ga < gb;
                }
                return ia.node_count < ib.node_count;
            });
            std::vector<uint32_t> old_to_new(n);
            for (uint32_t i = 0; i < n; i++) old_to_new[order[i]] = i;
            std::vector<InterpWay> new_interps(n);
            std::vector<NodeCoord> new_nodes;
            new_nodes.reserve(data.interp_nodes.size());
            for (uint32_t i = 0; i < n; i++) {
                auto iw = data.interp_ways[order[i]];
                uint32_t old_off = iw.node_offset;
                iw.node_offset = static_cast<uint32_t>(new_nodes.size());
                for (uint8_t j = 0; j < iw.node_count; j++)
                    new_nodes.push_back(data.interp_nodes[old_off + j]);
                new_interps[i] = iw;
            }
            data.interp_ways = std::move(new_interps);
            data.interp_nodes = std::move(new_nodes);
            for (auto& p : data.sorted_interp_cells) p.item_id = old_to_new[p.item_id];
            auto cmp = [](const CellItemPair& a, const CellItemPair& b) {
                return a.cell_id < b.cell_id || (a.cell_id == b.cell_id && a.item_id < b.item_id);
            };
            std::sort(data.sorted_interp_cells.begin(), data.sorted_interp_cells.end(), cmp);
            data.cell_to_interps.clear();
            std::cerr << "  Interps sorted: " << n << std::endl;
        }
        log_phase("  Sort interps", _st, _sc);

        // 5. Sort admin polygons by (name, level, country, vertex_count) + reorder vertices
        if (!data.admin_polygons.empty()) {
            size_t n = data.admin_polygons.size();
            std::vector<uint32_t> order(n);
            std::iota(order.begin(), order.end(), 0);
            const auto& sp = data.string_pool.data();
            std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
                const auto& pa = data.admin_polygons[a];
                const auto& pb = data.admin_polygons[b];
                if (pa.name_id != pb.name_id) return pa.name_id < pb.name_id;
                if (pa.admin_level != pb.admin_level) return pa.admin_level < pb.admin_level;
                if (pa.country_code != pb.country_code) return pa.country_code < pb.country_code;
                if (pa.vertex_count != pb.vertex_count) return pa.vertex_count < pb.vertex_count;
                // Compare first few vertices for tiebreaking
                uint32_t nc = std::min(pa.vertex_count, pb.vertex_count);
                nc = std::min(nc, 20u); // limit comparison depth
                for (uint32_t j = 0; j < nc; j++) {
                    uint32_t la = float_bits(data.admin_vertices[pa.vertex_offset + j].lat);
                    uint32_t lb = float_bits(data.admin_vertices[pb.vertex_offset + j].lat);
                    if (la != lb) return la < lb;
                    uint32_t ga = float_bits(data.admin_vertices[pa.vertex_offset + j].lng);
                    uint32_t gb = float_bits(data.admin_vertices[pb.vertex_offset + j].lng);
                    if (ga != gb) return ga < gb;
                }
                return false;
            });
            std::vector<uint32_t> old_to_new(n);
            for (uint32_t i = 0; i < n; i++) old_to_new[order[i]] = i;
            std::vector<AdminPolygon> new_polys(n);
            std::vector<NodeCoord> new_verts;
            new_verts.reserve(data.admin_vertices.size());
            for (uint32_t i = 0; i < n; i++) {
                auto p = data.admin_polygons[order[i]];
                uint32_t old_off = p.vertex_offset;
                p.vertex_offset = static_cast<uint32_t>(new_verts.size());
                for (uint32_t j = 0; j < p.vertex_count; j++)
                    new_verts.push_back(data.admin_vertices[old_off + j]);
                new_polys[i] = p;
            }
            data.admin_polygons = std::move(new_polys);
            data.admin_vertices = std::move(new_verts);
            // Remap place-node parent_poly_id references
            for (auto& pn : data.place_nodes) {
                if (pn.parent_poly_id != 0xFFFFFFFFu &&
                    pn.parent_poly_id < old_to_new.size()) {
                    pn.parent_poly_id = old_to_new[pn.parent_poly_id];
                }
            }
            // Remap admin_parent_ids (both indices and values)
            if (data.admin_parent_ids.size() == n) {
                std::vector<uint32_t> new_ap(n);
                for (uint32_t i = 0; i < n; i++) {
                    uint32_t old_parent = data.admin_parent_ids[order[i]];
                    new_ap[i] = (old_parent != NO_DATA && old_parent < n)
                        ? old_to_new[old_parent] : NO_DATA;
                }
                data.admin_parent_ids = std::move(new_ap);
            }
            // Remap way_parent_ids (values only — way order hasn't changed yet)
            for (auto& pid : data.way_parent_ids) {
                if (pid != NO_DATA && pid < n) pid = old_to_new[pid];
            }
            // Remap admin cell entries
            for (auto& [cell_id, ids] : data.cell_to_admin) {
                for (auto& id : ids) {
                    uint32_t flags = id & 0x80000000u;
                    uint32_t masked = id & 0x7FFFFFFFu;
                    if (masked < old_to_new.size())
                        id = old_to_new[masked] | flags;
                }
                std::sort(ids.begin(), ids.end());
            }
            std::cerr << "  Admin polygons sorted: " << n << std::endl;
        }
        log_phase("  Sort admin", _st, _sc);

        // 6. Sort POI records by (category, tier, name_id, lat_bits, lng_bits) + reorder vertices
        if (!data.poi_records.empty()) {
            size_t n = data.poi_records.size();
            std::vector<uint32_t> order(n);
            std::iota(order.begin(), order.end(), 0);
            std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
                const auto& pa = data.poi_records[a];
                const auto& pb = data.poi_records[b];
                if (pa.category != pb.category) return pa.category < pb.category;
                if (pa.tier != pb.tier) return pa.tier < pb.tier;
                if (pa.name_id != pb.name_id) return pa.name_id < pb.name_id;
                uint32_t la = float_bits(pa.lat), lb = float_bits(pb.lat);
                if (la != lb) return la < lb;
                uint32_t ga = float_bits(pa.lng), gb = float_bits(pb.lng);
                if (ga != gb) return ga < gb;
                return a < b; // stable tiebreaker
            });

            // Build old→new mapping
            std::vector<uint32_t> old_to_new(n);
            for (uint32_t i = 0; i < n; i++) old_to_new[order[i]] = i;

            // Reorder records + vertices + elevations + qids, dedup
            bool have_elevations = (poi_elevations.size() == n);
            bool have_qids = (poi_qids.size() == n);
            std::vector<PoiRecord> new_pois;
            std::vector<NodeCoord> new_poi_verts;
            std::vector<float> new_poi_elevations;
            std::vector<uint32_t> new_poi_qids;
            new_pois.reserve(n);
            new_poi_verts.reserve(data.poi_vertices.size());
            if (have_elevations) new_poi_elevations.reserve(n);
            if (have_qids) new_poi_qids.reserve(n);
            std::vector<uint32_t> dedup_remap(n);
            size_t write_pos = 0;

            for (uint32_t i = 0; i < n; i++) {
                auto p = data.poi_records[order[i]];
                uint32_t old_voff = p.vertex_offset;
                uint32_t vc = p.vertex_count;

                // Check for duplicate vs previous
                bool is_dup = false;
                if (write_pos > 0) {
                    auto& prev = new_pois[write_pos - 1];
                    if (prev.category == p.category && prev.tier == p.tier &&
                        prev.name_id == p.name_id &&
                        prev.lat == p.lat && prev.lng == p.lng &&
                        prev.vertex_count == p.vertex_count && prev.flags == p.flags) {
                        is_dup = true;
                    }
                }

                if (is_dup) {
                    dedup_remap[i] = static_cast<uint32_t>(write_pos - 1);
                } else {
                    dedup_remap[i] = static_cast<uint32_t>(write_pos);
                    p.vertex_offset = static_cast<uint32_t>(new_poi_verts.size());
                    if (vc > 0 && old_voff != NO_DATA) {
                        for (uint32_t j = 0; j < vc; j++)
                            new_poi_verts.push_back(data.poi_vertices[old_voff + j]);
                    }
                    new_pois.push_back(p);
                    if (have_elevations) new_poi_elevations.push_back(poi_elevations[order[i]]);
                    if (have_qids) new_poi_qids.push_back(poi_qids[order[i]]);
                    write_pos++;
                }
            }

            size_t deduped = n - new_pois.size();
            data.poi_records = std::move(new_pois);
            data.poi_vertices = std::move(new_poi_verts);
            if (have_elevations) poi_elevations = std::move(new_poi_elevations);
            if (have_qids) poi_qids = std::move(new_poi_qids);

            // Remap sorted_poi_cells IDs (preserving INTERIOR_FLAG)
            for (auto& p : data.sorted_poi_cells) {
                uint32_t flags = p.item_id & INTERIOR_FLAG;
                uint32_t old_id = p.item_id & ID_MASK;
                p.item_id = dedup_remap[old_to_new[old_id]] | flags;
            }
            auto cmp = [](const CellItemPair& a, const CellItemPair& b) {
                return a.cell_id < b.cell_id || (a.cell_id == b.cell_id && a.item_id < b.item_id);
            };
            std::sort(data.sorted_poi_cells.begin(), data.sorted_poi_cells.end(), cmp);
            data.cell_to_pois.clear();
            std::cerr << "  POI records sorted: " << n << " (" << deduped << " duplicates removed)" << std::endl;

            // Compute importance
            {
                for (size_t i = 0; i < data.poi_records.size(); i++) {
                    auto& pr = data.poi_records[i];
                    PoiCategory cat = static_cast<PoiCategory>(pr.category);
                    double base = category_base_importance(cat);

                    // Wiki multiplier
                    bool has_wp = (pr.flags & POI_FLAG_WIKIPEDIA) != 0;
                    bool has_wd = (pr.flags & POI_FLAG_WIKIDATA) != 0;
                    double wiki_mult = 1.0;
                    if (!sitelinks_data.empty() && i < poi_qids.size() && poi_qids[i] > 0) {
                        uint16_t sl = lookup_sitelinks(poi_qids[i]);
                        if (sl > 0) {
                            // Smooth curve: 1 sitelink=1.2x, 10=1.8x, 50=2.9x, 200=3.6x
                            wiki_mult = 1.0 + std::log2(1.0 + sl) / 3.0;
                        } else if (has_wp && has_wd) {
                            wiki_mult = 3.0;  // fallback to binary flags
                        } else if (has_wp) {
                            wiki_mult = 2.5;
                        } else if (has_wd) {
                            wiki_mult = 1.5;
                        }
                    } else {
                        // No sitelinks data — use binary flags
                        if (has_wp && has_wd) wiki_mult = 3.0;
                        else if (has_wp) wiki_mult = 2.5;
                        else if (has_wd) wiki_mult = 1.5;
                    }

                    double raw = base * wiki_mult;

                    // Peak/volcano elevation scaling
                    if ((cat == PoiCategory::PEAK || cat == PoiCategory::VOLCANO) && i < poi_elevations.size()) {
                        float ele = poi_elevations[i];
                        if (ele > 0) raw *= std::min((double)ele / 2000.0, 3.0);
                        else raw *= 0.5;
                    }

                    // Polygon area scaling
                    if (pr.vertex_count > 0 && pr.vertex_offset != NO_DATA) {
                        // Approximate area in km² using shoelace formula
                        double area_deg2 = 0;
                        for (uint32_t j = 0; j < pr.vertex_count; j++) {
                            uint32_t k = (j + 1) % pr.vertex_count;
                            const auto& a = data.poi_vertices[pr.vertex_offset + j];
                            const auto& b = data.poi_vertices[pr.vertex_offset + k];
                            area_deg2 += (double)a.lng * b.lat - (double)b.lng * a.lat;
                        }
                        area_deg2 = std::abs(area_deg2) / 2.0;
                        double lat_mid = std::abs((double)pr.lat);
                        double deg_to_km = 111.32 * std::cos(lat_mid * M_PI / 180.0);
                        double area_km2 = area_deg2 * 111.32 * deg_to_km;
                        if (area_km2 > 0) raw *= std::min(1.0 + std::log2(1.0 + area_km2) / 4.0, 2.0);
                    }

                    pr.importance = static_cast<uint8_t>(std::max(1.0, std::min(255.0, raw)));
                }
                std::cerr << "  POI importance computed" << std::endl;
            }
        }
        log_phase("  Sort POIs", _st, _sc);

        // 7. Sort place nodes by (place_type, name_id, lat_bits, lng_bits)
        if (!data.place_nodes.empty()) {
            size_t n = data.place_nodes.size();
            std::vector<uint32_t> order(n);
            std::iota(order.begin(), order.end(), 0);
            std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
                const auto& pa = data.place_nodes[a];
                const auto& pb = data.place_nodes[b];
                if (pa.place_type != pb.place_type) return pa.place_type < pb.place_type;
                if (pa.name_id != pb.name_id) return pa.name_id < pb.name_id;
                uint32_t la = float_bits(pa.lat), lb = float_bits(pb.lat);
                if (la != lb) return la < lb;
                return float_bits(pa.lng) < float_bits(pb.lng);
            });
            std::vector<uint32_t> old_to_new(n);
            for (uint32_t i = 0; i < n; i++) old_to_new[order[i]] = i;
            std::vector<PlaceNode> sorted(n);
            for (uint32_t i = 0; i < n; i++) sorted[i] = data.place_nodes[order[i]];
            data.place_nodes = std::move(sorted);
            for (auto& p : data.sorted_place_cells) p.item_id = old_to_new[p.item_id];
            auto cmp = [](const CellItemPair& a, const CellItemPair& b) {
                return a.cell_id < b.cell_id || (a.cell_id == b.cell_id && a.item_id < b.item_id);
            };
            std::sort(data.sorted_place_cells.begin(), data.sorted_place_cells.end(), cmp);
            std::cerr << "  Place nodes sorted: " << n << std::endl;
        }
        log_phase("  Sort place nodes", _st, _sc);
    }

    // --- Write index files ---
    log_phase("Deterministic ordering", _pt, _cpu);
    std::cerr << "Writing index files to " << output_dir << "..." << std::endl;

    auto quality_dir_name = [](double scale) -> std::string {
        if (scale == 0) return "uncapped";
        char buf[32]; snprintf(buf, sizeof(buf), "q%.4g", scale);
        return buf;
    };

    // Write quality variants in parallel (bounded concurrency).
    // Each variant does parallel simplification internally, so limit
    // concurrency to avoid over-subscribing CPU.
    auto write_qualities = [&](const ParsedData& d, const std::string& admin_dir) {
        constexpr unsigned max_concurrent_qualities = 3;
        std::atomic<unsigned> active{0};
        std::mutex qmtx;
        std::condition_variable qcv;
        std::vector<std::future<void>> qfutures;

        for (double scale : quality_scales) {
            {
                std::unique_lock<std::mutex> lock(qmtx);
                qcv.wait(lock, [&]{ return active.load() < max_concurrent_qualities; });
            }
            active.fetch_add(1);

            std::string qname = quality_dir_name(scale);
            std::string qdir = admin_dir + "/" + qname;
            qfutures.push_back(std::async(std::launch::async, [&, qdir, scale]() {
                write_quality_variant(d, admin_dir, qdir, scale);
                active.fetch_sub(1);
                qcv.notify_one();
            }));
        }
        for (auto& f : qfutures) f.get();
    };

    // Write one region: modes + quality variants
    auto write_region = [&](const ParsedData& d, const std::string& base_dir) {
        ensure_dir(base_dir);

        if (multi_output) {
            // Write all 3 modes in parallel (they read shared data, write to separate dirs)
            auto wf1 = std::async(std::launch::async, [&]{ write_index(d, base_dir + "/full", IndexMode::Full); });
            auto wf2 = std::async(std::launch::async, [&]{ write_index(d, base_dir + "/no-addresses", IndexMode::NoAddresses); });
            auto wf3 = std::async(std::launch::async, [&]{ write_index(d, base_dir + "/admin", IndexMode::AdminOnly); });
            wf1.get(); wf2.get(); wf3.get();
        } else {
            write_index(d, base_dir, mode);
        }

        // Write quality variants (each gets admin_polygons + admin_vertices)
        if (multi_quality) {
            std::string quality_dir = multi_output ? base_dir + "/quality" : base_dir;
            std::cerr << "  Writing quality variants for " << base_dir << "..." << std::endl;
            write_qualities(d, quality_dir);
        }

        // Write place node files into each mode directory (so diff/patch can find them)
        if (!d.place_nodes.empty()) {
            std::unordered_map<uint64_t, std::vector<uint32_t>> place_cell_map;
            for (const auto& p : d.sorted_place_cells) {
                place_cell_map[p.cell_id].push_back(p.item_id);
            }

            auto write_place_files = [&](const std::string& dir) {
                ensure_dir(dir);
                {
                    std::ofstream f(dir + "/place_nodes.bin", std::ios::binary);
                    f.write(reinterpret_cast<const char*>(d.place_nodes.data()),
                            d.place_nodes.size() * sizeof(PlaceNode));
                }
                write_cell_index(dir + "/place_cells.bin", dir + "/place_entries.bin", place_cell_map);
            };

            if (multi_output) {
                write_place_files(base_dir + "/full");
                write_place_files(base_dir + "/no-addresses");
                write_place_files(base_dir + "/admin");
            } else {
                write_place_files(base_dir);
            }
            std::cerr << "  Place nodes: " << d.place_nodes.size() << " nodes, "
                      << place_cell_map.size() << " cells" << std::endl;
        }

        // Write POI tier variants
        if (!d.poi_records.empty()) {
            struct PoiTierVariant {
                const char* name;
                uint8_t max_tier;
            };
            PoiTierVariant poi_tiers[] = {
                {"poi/major",   1},
                {"poi/notable", 2},
                {"poi/all",     3},
            };

            for (const auto& tier_var : poi_tiers) {
                std::string poi_dir = base_dir + "/" + tier_var.name;
                ensure_dir(poi_dir);

                // Filter records by tier
                std::vector<PoiRecord> filtered_records;
                std::vector<NodeCoord> filtered_vertices;
                // Build old→new ID mapping for filtering
                std::vector<uint32_t> id_remap(d.poi_records.size(), NO_DATA);

                for (size_t i = 0; i < d.poi_records.size(); i++) {
                    if (d.poi_records[i].tier <= tier_var.max_tier) {
                        id_remap[i] = static_cast<uint32_t>(filtered_records.size());
                        auto pr = d.poi_records[i];
                        uint32_t old_voff = pr.vertex_offset;
                        uint32_t vc = pr.vertex_count;
                        pr.vertex_offset = static_cast<uint32_t>(filtered_vertices.size());
                        if (vc > 0 && old_voff != NO_DATA) {
                            for (uint32_t j = 0; j < vc; j++)
                                filtered_vertices.push_back(d.poi_vertices[old_voff + j]);
                        }
                        filtered_records.push_back(pr);
                    }
                }

                // Build filtered cell index (preserving INTERIOR_FLAG)
                std::unordered_map<uint64_t, std::vector<uint32_t>> filtered_cell_map;
                for (const auto& p : d.sorted_poi_cells) {
                    uint32_t flags = p.item_id & INTERIOR_FLAG;
                    uint32_t raw_id = p.item_id & ID_MASK;
                    if (raw_id < id_remap.size() && id_remap[raw_id] != NO_DATA) {
                        filtered_cell_map[p.cell_id].push_back(id_remap[raw_id] | flags);
                    }
                }
                // Dedup cell entries
                for (auto& [cell_id, ids] : filtered_cell_map) {
                    std::sort(ids.begin(), ids.end());
                    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
                }

                // Write files
                {
                    std::ofstream f(poi_dir + "/poi_records.bin", std::ios::binary);
                    f.write(reinterpret_cast<const char*>(filtered_records.data()),
                            filtered_records.size() * sizeof(PoiRecord));
                }
                {
                    std::ofstream f(poi_dir + "/poi_vertices.bin", std::ios::binary);
                    f.write(reinterpret_cast<const char*>(filtered_vertices.data()),
                            filtered_vertices.size() * sizeof(NodeCoord));
                }
                write_cell_index(poi_dir + "/poi_cells.bin", poi_dir + "/poi_entries.bin",
                                 filtered_cell_map);

                // Write poi_meta.json (category metadata for the server)
                {
                    std::ofstream mf(poi_dir + "/poi_meta.json");
                    mf << "{\n";
                    bool first = true;
                    // Collect categories present in this tier
                    std::set<uint8_t> cats;
                    for (const auto& r : filtered_records) cats.insert(r.category);
                    for (uint8_t cat : cats) {
                        if (!first) mf << ",\n";
                        first = false;
                        PoiCategory pc = static_cast<PoiCategory>(cat);
                        mf << "  \"" << (int)cat << "\": {\"name\": \""
                           << poi_category_label(pc) << "\", \"reference_distance\": "
                           << category_reference_distance(pc) << ", \"max_distance\": "
                           << category_max_distance(pc) << ", \"default_importance\": "
                           << (int)category_base_importance(pc) << "}";
                    }
                    mf << "\n}\n";
                }

                std::cerr << "  POI " << tier_var.name << ": "
                          << filtered_records.size() << " records, "
                          << filtered_vertices.size() << " vertices, "
                          << filtered_cell_map.size() << " cells" << std::endl;
            }
        }
    };

    // Write planet (async — overlaps with continent filtering start)
    auto planet_future = std::async(std::launch::async, [&]() {
        write_region(data, output_dir + "/planet");
    });

    // Process continents with bounded concurrency, largest first.
    if (generate_continents) {
        // Pre-compute continent membership for each unique cell in sorted pairs.
        // One parallel scan replaces 8 × cell_in_bbox per cell during filtering.
        auto _pct = std::chrono::steady_clock::now();
        auto _pcc = CpuTicks::now();

        // Load continent polygons for boundary-based filtering
        auto continent_polys = get_continent_polygons();
        std::cerr << "  Loaded " << continent_polys.size() << " continent boundary polygons" << std::endl;

        // For each sorted pair array, build a parallel array of continent bitmasks.
        // Since pairs are sorted by cell_id, consecutive entries share the same mask.
        auto precompute_masks = [&continent_polys](const std::vector<CellItemPair>& sorted) -> std::vector<uint8_t> {
            if (sorted.empty()) return {};
            std::vector<uint8_t> masks(sorted.size(), 0);

            unsigned nthreads = std::max(1u, std::thread::hardware_concurrency());
            size_t chunk = (sorted.size() + nthreads - 1) / nthreads;
            std::vector<size_t> bounds = {0};
            for (unsigned t = 1; t < nthreads; t++) {
                size_t target = t * chunk;
                if (target >= sorted.size()) break;
                while (target < sorted.size() && sorted[target].cell_id == sorted[target-1].cell_id)
                    target++;
                if (target < sorted.size()) bounds.push_back(target);
            }
            bounds.push_back(sorted.size());

            std::vector<std::thread> threads;
            for (size_t th = 0; th + 1 < bounds.size(); th++) {
                threads.emplace_back([&, th]() {
                    for (size_t i = bounds[th]; i < bounds[th+1]; ) {
                        uint64_t cell_id = sorted[i].cell_id;
                        S2CellId cell(cell_id);
                        S2LatLng center = cell.ToLatLng();
                        double lat = center.lat().degrees();
                        double lng = center.lng().degrees();
                        uint8_t mask = 0;
                        // Test against continent polygons (not bboxes)
                        for (size_t ci = 0; ci < continent_polys.size() && ci < 8; ci++) {
                            if (point_in_polygon(lat, lng, continent_polys[ci].vertices))
                                mask |= (1u << ci);
                        }
                        while (i < bounds[th+1] && sorted[i].cell_id == cell_id) {
                            masks[i] = mask;
                            i++;
                        }
                    }
                });
            }
            for (auto& t : threads) t.join();
            return masks;
        };

        // Precompute masks for every sorted-pair array we'll filter through
        // filter_by_bbox_masked: ways, addrs, interps, POIs, and place nodes.
        auto way_masks = std::async(std::launch::async, [&]{ return precompute_masks(data.sorted_way_cells); });
        auto addr_masks = std::async(std::launch::async, [&]{ return precompute_masks(data.sorted_addr_cells); });
        auto interp_masks = std::async(std::launch::async, [&]{ return precompute_masks(data.sorted_interp_cells); });
        auto poi_masks = std::async(std::launch::async, [&]{ return precompute_masks(data.sorted_poi_cells); });
        auto place_masks = std::async(std::launch::async, [&]{ return precompute_masks(data.sorted_place_cells); });
        auto way_continent_masks = way_masks.get();
        auto addr_continent_masks = addr_masks.get();
        auto interp_continent_masks = interp_masks.get();
        auto poi_continent_masks = poi_masks.get();
        auto place_continent_masks = place_masks.get();

        log_phase("Pre-compute continent masks", _pct, _pcc);

        // Sort by bbox area descending — largest continents first
        std::vector<size_t> continent_order(kContinentCount);
        std::iota(continent_order.begin(), continent_order.end(), 0u);
        std::sort(continent_order.begin(), continent_order.end(), [](size_t a, size_t b) {
            auto area = [](const ContinentBBox& c) {
                return (c.max_lat - c.min_lat) * (c.max_lng - c.min_lng);
            };
            return area(kContinents[a]) > area(kContinents[b]);
        });

        unsigned max_concurrent = std::max(1u, std::min(4u,
            std::thread::hardware_concurrency() / 8));
        std::cerr << "Processing " << kContinentCount << " continents ("
                  << max_concurrent << " concurrent, largest first)..." << std::endl;

        std::atomic<unsigned> active{0};
        std::mutex cv_mutex;
        std::condition_variable cv;
        std::vector<std::future<void>> futures;

        for (size_t i = 0; i < kContinentCount; i++) {
            {
                std::unique_lock<std::mutex> lock(cv_mutex);
                cv.wait(lock, [&]{ return active.load() < max_concurrent; });
            }
            active.fetch_add(1);

            futures.push_back(std::async(std::launch::async, [&, i]() {
                const auto& continent = kContinents[continent_order[i]];
                auto _ct = std::chrono::steady_clock::now();
                auto _cc = CpuTicks::now();
                std::cerr << "Continent: " << continent.name << " (start)..." << std::endl;
                uint8_t cbit = 1u << continent_order[i];
                const auto* poly = (continent_order[i] < continent_polys.size() && !continent_polys[continent_order[i]].vertices.empty())
                    ? &continent_polys[continent_order[i]].vertices : nullptr;
                auto subset = filter_by_bbox_masked(data, continent, cbit,
                    way_continent_masks, addr_continent_masks, interp_continent_masks,
                    poi_continent_masks, place_continent_masks, poly);
                log_phase(("  " + std::string(continent.name) + ": filter").c_str(), _ct, _cc);
                write_region(subset, output_dir + "/" + continent.name);
                log_phase(("  " + std::string(continent.name) + ": total").c_str(), _ct, _cc);

                active.fetch_sub(1);
                cv.notify_one();
            }));
        }
        for (auto& f : futures) f.get();
    }

    // Wait for planet write if not already done
    planet_future.get();
    log_phase("All index writing (total)", _pt, _cpu);

    // --- Generate manifest.json ---
    {
        auto file_size = [](const std::string& path) -> int64_t {
            struct stat st;
            if (stat(path.c_str(), &st) == 0) return st.st_size;
            return -1;
        };

        auto dir_size = [&](const std::string& dir, const std::vector<std::string>& files) -> int64_t {
            int64_t total = 0;
            for (const auto& f : files) {
                int64_t s = file_size(dir + "/" + f);
                if (s > 0) total += s;
            }
            return total;
        };

        std::vector<std::string> admin_base_files = {
            "admin_cells.bin", "admin_entries.bin", "strings.bin"
        };
        std::vector<std::string> admin_quality_files = {
            "admin_polygons.bin", "admin_vertices.bin"
        };
        std::vector<std::string> street_files = {
            "street_ways.bin", "street_nodes.bin", "street_entries.bin"
        };
        std::vector<std::string> address_files = {
            "addr_points.bin", "addr_vertices.bin", "addr_entries.bin",
            "interp_ways.bin", "interp_nodes.bin", "interp_entries.bin"
        };
        std::vector<std::string> geo_files = {"geo_cells.bin"};

        // Build list of regions
        std::vector<std::pair<std::string, std::string>> regions; // (name, path)
        regions.push_back({"planet", output_dir + "/planet"});
        if (generate_continents) {
            for (size_t ci = 0; ci < kContinentCount; ci++) {
                regions.push_back({kContinents[ci].name,
                    output_dir + "/" + kContinents[ci].name});
            }
        }

        // Quality level names
        auto quality_dir_name = [](double scale) -> std::string {
            if (scale == 0) return "uncapped";
            char buf[32]; snprintf(buf, sizeof(buf), "q%.4g", scale);
            return buf;
        };

        std::ofstream mf(output_dir + "/manifest.json");
        mf << "{\n";
        mf << "  \"build_version\": 10,\n";
        mf << "  \"patch_version\": 5,\n";
        mf << "  \"regions\": {\n";

        for (size_t ri = 0; ri < regions.size(); ri++) {
            const auto& [rname, rpath] = regions[ri];
            mf << "    \"" << rname << "\": {\n";

            // Modes (no admin_polygons/vertices — those are in quality/)
            mf << "      \"modes\": {\n";
            if (multi_output) {
                std::string full_dir = rpath + "/full";
                int64_t full_sz = dir_size(full_dir, geo_files) +
                    dir_size(full_dir, street_files) +
                    dir_size(full_dir, address_files) +
                    dir_size(full_dir, admin_base_files);
                mf << "        \"full\": {\"size\": " << full_sz << "},\n";

                std::string na_dir = rpath + "/no-addresses";
                int64_t na_sz = dir_size(na_dir, geo_files) +
                    dir_size(na_dir, street_files) +
                    dir_size(na_dir, admin_base_files);
                mf << "        \"no-addresses\": {\"size\": " << na_sz << "},\n";

                std::string admin_dir = rpath + "/admin";
                int64_t admin_sz = dir_size(admin_dir, admin_base_files);
                mf << "        \"admin\": {\"size\": " << admin_sz << "}\n";
            } else {
                int64_t sz = 0;
                for (const auto& lists : {geo_files, street_files, address_files,
                                          admin_base_files}) {
                    sz += dir_size(rpath, lists);
                }
                mf << "        \"" << (mode == IndexMode::Full ? "full" :
                    mode == IndexMode::NoAddresses ? "no-addresses" : "admin") << "\": {\"size\": " << sz << "}\n";
            }
            mf << "      },\n";

            // Quality variants (each has admin_polygons + admin_vertices)
            if (multi_quality) {
                std::string quality_dir = multi_output ? rpath + "/quality" : rpath;
                mf << "      \"qualities\": {\n";
                for (size_t qi = 0; qi < quality_scales.size(); qi++) {
                    double scale = quality_scales[qi];
                    std::string qname = quality_dir_name(scale);
                    std::string qdir = quality_dir + "/" + qname;
                    int64_t qsz = dir_size(qdir, admin_quality_files);

                    double eps_l2 = (scale == 0) ? 0 : 500.0 * scale * kEpsilonScale;
                    double eps_l8 = (scale == 0) ? 0 : 15.0 * scale * kEpsilonScale;

                    mf << "        \"" << qname << "\": {\"scale\": " << scale
                       << ", \"size\": " << qsz
                       << ", \"epsilon_l2_m\": " << eps_l2
                       << ", \"epsilon_l8_m\": " << eps_l8
                       << "}";
                    if (qi + 1 < quality_scales.size()) mf << ",";
                    mf << "\n";
                }
                mf << "      },\n";
            } else {
                mf << "      \"qualities\": {},\n";
            }

            // POI tier info
            {
                std::vector<std::string> poi_files = {
                    "poi_records.bin", "poi_vertices.bin", "poi_cells.bin", "poi_entries.bin"
                };
                mf << "      \"poi\": {\n";
                const char* poi_tier_names[] = {"major", "notable", "all"};
                for (int ti = 0; ti < 3; ti++) {
                    std::string pdir = rpath + "/poi/" + poi_tier_names[ti];
                    int64_t psz = dir_size(pdir, poi_files);
                    mf << "        \"" << poi_tier_names[ti] << "\": {\"size\": " << psz << "}";
                    if (ti < 2) mf << ",";
                    mf << "\n";
                }
                mf << "      }\n";
            }

            mf << "    }";
            if (ri + 1 < regions.size()) mf << ",";
            mf << "\n";
        }

        mf << "  }\n";
        mf << "}\n";

        std::cerr << "Wrote " << output_dir << "/manifest.json" << std::endl;
    }

    std::cerr << "Done." << std::endl;
    return 0;
}
