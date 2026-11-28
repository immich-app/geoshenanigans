// Estimate tag-bundle sizes for the builder's place nodes, admin polygons
// and shipped POI tiers. Name keys follow Nominatim's NAME_TAGS minus
// IGNORE_KEYS (lib-lua/themes/nominatim/presets.lua).
//
//   namecensus <planet.pbf> <out_dir> [threads]
//
// Scan a type-sorted PBF in relation, way and node phases so the first
// node of the first member way can locate a relation. Native-endian outputs:
//   out_dir/records.bin  per record: u8 class (0 place, 1 admin, 2 poi), u8 sub (place type, admin
//                        level, poi tier), u8 flags (1 admin-minimal keeps it, 2 its bundle is only
//                        its stored name), u8 osm type ('n','w','r'), i64 osm id, u8 loc kind ('n'
//                        node, 'w' way, 0 none), i64 loc ref, f64 lat, f64 lon (nodes), u16 stored
//                        name len + bytes, u32 bundle len + bytes
//   out_dir/way_nodes.bin   (way id i64, first node id i64) of the ways a relation's location needs
//   out_dir/node_locs.bin   (node id i64, lat f64, lon f64) of the nodes a way's location needs
//   stdout                  counts per class and key group
//
// The census counts one record per relation, without assembling its rings
// or merging linked-place names. A closed way is located at its first node.
#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <limits>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "admin_rank_config.h"
#include "paged_bits.h"
#include "pbf_reader.h"
#include "poi_classify.h"
#include "relation_import.h"
#include "types.h"

using sv = std::string_view;

namespace {

constexpr char KEY_SEP = '\x1f';
constexpr char PAIR_SEP = '\x1e';
constexpr uint8_t FLAG_ADMIN_MINIMAL = 1;
constexpr uint8_t FLAG_ONLY_STORED = 2;
constexpr size_t SUB_BUCKETS = POI_MAX_SHIPPED_TIER + 1;

enum Cls : uint8_t { PLACE = 0, ADMIN = 1, POI = 2, CLASS_COUNT = 3 };
enum Group : uint8_t {
    G_NAME, G_NAME_EN, G_NAME_LANG, G_INT, G_REG, G_LOC, G_OLD, G_ALT, G_OFFICIAL, G_PLACE_NAME, G_SHORT,
    G_REF, G_ISO, G_AFFIX, G_POI_EXTRA, GROUP_COUNT
};
const char* GROUP_NAMES[GROUP_COUNT] = {"name", "name:en", "name:<lang>", "int_name*", "reg_name*", "loc_name*",
                                        "old_name*", "alt_name*", "official_name*", "place_name*", "short_name*",
                                        "ref family", "ISO3166-2", "prefix/suffix", "brand/iata/icao/faa"};

bool starts(sv s, sv p) { return s.size() >= p.size() && s.substr(0, p.size()) == p; }
bool ends(sv s, sv p) { return s.size() >= p.size() && s.substr(s.size() - p.size()) == p; }

// IGNORE_KEYS.metatags and .name (presets.lua:388-401).
bool ignored(sv k) {
    static const sv exact[] = {"note", "source", "attribution", "comment", "fixme", "created_by", "tiger:cfcc",
                               "tiger:reviewed", "import", "ref:bygningsnr", "building:ruian:type", "type",
                               "is_in:postcode", "name:full", "name:etymology", "name:signed", "name:botanical"};
    static const sv prefix[] = {"note:", "source:", "nysgissam:", "NHD:", "nhd:", "gnis:", "geobase:", "yh:", "osak:",
                                "naptan:", "CLC:", "it:fvg:", "lacounty:", "ref:linz:", "survey:", "ref:ruian:",
                                "name:etymology:"};
    for (sv e : exact)
        if (k == e) return true;
    for (sv p : prefix)
        if (starts(k, p)) return true;
    return ends(k, "source") || ends(k, ":forward") || ends(k, ":backward");
}

// The group of a NAME_TAGS key (main, extra, and the POI additions), or -1.
int group_of(sv k, bool poi) {
    if (ignored(k)) return -1;
    if (k == "name") return G_NAME;
    if (k == "name:en") return G_NAME_EN;
    if (starts(k, "name:prefix:") || starts(k, "name:suffix:") || ends(k, ":prefix") || ends(k, ":suffix"))
        return G_AFFIX;
    if (starts(k, "name:")) return G_NAME_LANG;
    struct Base { sv key; Group g; };
    static const Base bases[] = {{"int_name", G_INT}, {"reg_name", G_REG}, {"loc_name", G_LOC}, {"old_name", G_OLD},
                                 {"alt_name", G_ALT}, {"official_name", G_OFFICIAL}, {"place_name", G_PLACE_NAME},
                                 {"short_name", G_SHORT}};
    for (const auto& b : bases) {
        if (k == b.key) return b.g;
        if (k.size() > b.key.size() && starts(k, b.key) && k[b.key.size()] == ':') return b.g;
    }
    if (starts(k, "alt_name_")) return G_ALT;
    static const sv refs[] = {"ref", "int_ref", "nat_ref", "reg_ref", "loc_ref", "old_ref"};
    for (sv r : refs)
        if (k == r) return G_REF;
    if (k == "ISO3166-2") return G_ISO;
    if (poi && (k == "brand" || k == "iata" || k == "icao" || k == "faa")) return G_POI_EXTRA;
    return -1;
}

struct Tag { sv k, v; };

struct Stats {
    uint64_t records = 0, only_stored = 0, bundle_bytes = 0, bundle_bytes_named = 0;
    uint64_t with[GROUP_COUNT] = {}, keys[GROUP_COUNT] = {}, bytes[GROUP_COUNT] = {};
    void add(const Stats& o) {
        records += o.records; only_stored += o.only_stored; bundle_bytes += o.bundle_bytes;
        bundle_bytes_named += o.bundle_bytes_named;
        for (int g = 0; g < GROUP_COUNT; g++) { with[g] += o.with[g]; keys[g] += o.keys[g]; bytes[g] += o.bytes[g]; }
    }
};

struct Out {
    std::string recs;
    std::vector<int64_t> wanted;
    std::vector<std::pair<int64_t, int64_t>> way_nodes;
    std::vector<std::tuple<int64_t, double, double>> locs;
    Stats st[CLASS_COUNT][SUB_BUCKETS];  // POI tiers have distinct buckets.
};

template <class T> void put(std::string& s, T v) { s.append(reinterpret_cast<const char*>(&v), sizeof v); }

// Appends one record: its bundle, keys sorted bytewise, and the stats.
void emit(Out& o, Cls cls, uint8_t sub, bool minimal, char otype, int64_t id, char lkind, int64_t lref, double lat,
          double lon, const char* stored, std::vector<Tag>& tags) {
    std::vector<std::pair<Tag, int>> kept;
    for (const Tag& t : tags) {
        int g = group_of(t.k, cls == POI);
        if (g >= 0 && !t.v.empty()) kept.push_back({t, g});
    }
    std::sort(kept.begin(), kept.end(), [](const auto& a, const auto& b) { return a.first.k < b.first.k; });
    std::string bundle;
    bool only_stored = true;
    Stats& st = o.st[cls][cls == POI ? sub : 0];
    bool seen[GROUP_COUNT] = {};
    for (const auto& [t, g] : kept) {
        if (!bundle.empty()) bundle += PAIR_SEP;
        bundle.append(t.k);
        bundle += KEY_SEP;
        bundle.append(t.v);
        if (!((g == G_NAME || g == G_NAME_EN) && stored && t.v == sv(stored))) only_stored = false;
        st.keys[g]++;
        st.bytes[g] += t.k.size() + t.v.size() + 2;
        seen[g] = true;
    }
    for (int g = 0; g < GROUP_COUNT; g++) st.with[g] += seen[g];
    st.records++;
    st.only_stored += only_stored;
    st.bundle_bytes += bundle.size();
    if (!only_stored) st.bundle_bytes_named += bundle.size();
    uint8_t flags = (minimal ? FLAG_ADMIN_MINIMAL : 0) | (only_stored ? FLAG_ONLY_STORED : 0);
    put(o.recs, uint8_t(cls));
    put(o.recs, sub);
    put(o.recs, flags);
    put(o.recs, uint8_t(otype));
    put(o.recs, id);
    put(o.recs, uint8_t(lkind));
    put(o.recs, lref);
    put(o.recs, lat);
    put(o.recs, lon);
    const size_t sl = stored ? std::min<size_t>(std::strlen(stored), std::numeric_limits<uint16_t>::max()) : 0;
    put(o.recs, uint16_t(sl));
    o.recs.append(stored ? stored : "", sl);
    put(o.recs, uint32_t(bundle.size()));
    o.recs += bundle;
}

const char* get(const std::vector<Tag>& tags, const char* k) {
    for (const Tag& t : tags)
        if (t.k == k) return t.v.data();
    return nullptr;
}

// Tag views into a block string table; values are NUL-terminated std::strings there.
void tags_of(const uint32_t* ks, const uint32_t* vs, size_t n, const std::vector<std::string>& st, std::vector<Tag>& out) {
    out.clear();
    for (size_t i = 0; i < n; i++)
        if (ks[i] < st.size() && vs[i] < st.size()) out.push_back({st[ks[i]], st[vs[i]]});
}

// The relation POI categories that can be inferred without assembling rings.
PoiCategory relation_poi_cat(const std::vector<Tag>& t) {
    auto is = [&](const char* k, const char* v) { const char* x = get(t, k); return x && std::strcmp(x, v) == 0; };
    if (is("boundary", "national_park")) return PoiCategory::NATIONAL_PARK;
    if (is("boundary", "protected_area")) return PoiCategory::PROTECTED_AREA;
    if (is("leisure", "park")) return PoiCategory::PARK;
    if (is("leisure", "nature_reserve")) return PoiCategory::NATURE_RESERVE;
    if (is("leisure", "stadium") || is("leisure", "sports_centre")) return PoiCategory::STADIUM;
    if (is("leisure", "water_park")) return PoiCategory::WATER_PARK;
    if (is("leisure", "golf_course")) return PoiCategory::GOLF_COURSE;
    if (is("leisure", "garden")) return PoiCategory::GARDEN;
    if (is("leisure", "marina")) return PoiCategory::MARINA;
    if (is("tourism", "theme_park")) return PoiCategory::THEME_PARK;
    if (is("tourism", "zoo")) return PoiCategory::ZOO;
    if (is("tourism", "museum")) return PoiCategory::MUSEUM;
    if (is("tourism", "attraction")) return PoiCategory::ATTRACTION;
    if (is("tourism", "aquarium")) return PoiCategory::AQUARIUM;
    if (is("tourism", "resort")) return PoiCategory::RESORT;
    if (is("natural", "bay")) return PoiCategory::BAY;
    if (is("natural", "island")) return PoiCategory::ISLAND;
    if (is("natural", "beach")) return PoiCategory::BEACH;
    if (is("natural", "glacier")) return PoiCategory::GLACIER;
    if (is("natural", "volcano")) return PoiCategory::VOLCANO;
    if (is("aeroway", "aerodrome")) return PoiCategory::AERODROME;
    if (is("place", "island") || is("place", "islet")) return PoiCategory::ISLAND;
    if (is("office", "government")) return PoiCategory::GOVERNMENT;
    return PoiCategory::UNKNOWN;
}

uint8_t poi_tier(const std::vector<Tag>& t, bool node, const char** stored) {
    const char* name_en = get(t, "name:en");
    const char* name = get(t, "name");
    const char *tourism = get(t, "tourism"), *historic = get(t, "historic"), *boundary = get(t, "boundary"),
               *amenity = get(t, "amenity"), *leisure = get(t, "leisure"), *natural = get(t, "natural"),
               *railway = get(t, "railway"), *aeroway = get(t, "aeroway"), *man_made = get(t, "man_made"),
               *building = get(t, "building"), *craft = get(t, "craft"), *power = get(t, "power"),
               *place = get(t, "place"), *waterway = get(t, "waterway"), *office = get(t, "office"),
               *wikipedia = get(t, "wikipedia"), *wikidata = get(t, "wikidata"), *highway = get(t, "highway"),
               *shop = get(t, "shop"), *landuse = get(t, "landuse"), *emergency = get(t, "emergency"),
               *footway = get(t, "footway"), *area = get(t, "area");
    std::optional<PoiClassification> cls;
    if (node) {
        const char* nm = poi_name(name_en, name);
        if (!nm) return 0;
        cls = classify_poi(tourism, historic, boundary, amenity, leisure, natural, railway, aeroway, man_made, building,
                           craft, power, place, waterway, office, wikipedia, wikidata, highway, shop, landuse, emergency,
                           nullptr, false);
        *stored = nm;
    } else {
        auto is = [](const char* a, const char* b) { return a && std::strcmp(a, b) == 0; };
        const char* best = (name_en && name_en[0]) ? name_en : name;
        bool has = tourism || historic || amenity || leisure || natural || aeroway || railway || man_made || building ||
                   craft || power || place || waterway || office ||
                   (boundary && (is(boundary, "national_park") || is(boundary, "protected_area"))) ||
                   is(emergency, "fire_water_pond") ||
                   (landuse && (is(landuse, "reservoir") || is(landuse, "basin")) &&
                    has_poi_main_tag(amenity, leisure, emergency, tourism, historic, shop, craft, office, aeroway,
                                     railway, man_made)) ||
                   (is(area, "yes") && poi_water_street(natural, landuse, highway, footway, place));
        if (!has || !best) return 0;
        cls = classify_poi(tourism, historic, boundary, amenity, leisure, natural, railway, aeroway, man_made, building,
                           craft, power, place, waterway, office, wikipedia, wikidata, highway, shop, landuse, emergency,
                           footway, true);
        *stored = best;
    }
    if (!cls || cls->tier == 0 || cls->tier > POI_MAX_SHIPPED_TIER) return 0;
    return cls->tier;
}

AdminPlaceType place_override(const char* place) { return place_label_of(place_value_of(place)); }

const char* best_name(const std::vector<Tag>& t) {
    const char* en = get(t, "name:en");
    return (en && en[0]) ? en : get(t, "name");
}

// Build the bitmap between phases so worker threads only read its pages.
PagedBits take_wanted_ids(std::vector<Out>& outs) {
    int64_t maximum = 0;
    for (const auto& o : outs)
        for (int64_t id : o.wanted) maximum = std::max(maximum, id);
    PagedBits bits(static_cast<uint64_t>(maximum) + 1);
    for (auto& o : outs) {
        for (int64_t id : o.wanted)
            if (id > 0) bits.set(id);
        o.wanted.clear();
    }
    return bits;
}

template <class T> void write_value(std::ofstream& out, const T& value) {
    out.write(reinterpret_cast<const char*>(&value), sizeof value);
}

double secs(std::chrono::steady_clock::time_point t) {
    return std::chrono::duration<double>(std::chrono::steady_clock::now() - t).count();
}

}  // namespace

int main(int argc, char** argv) try {
    unsigned threads = 0;
    if (argc == 4) {
        const sv arg = argv[3];
        const auto result = std::from_chars(arg.data(), arg.data() + arg.size(), threads);
        if (result.ec != std::errc{} || result.ptr != arg.data() + arg.size() || threads == 0) {
            std::fprintf(stderr, "thread count must be a positive integer\n");
            return 2;
        }
    } else if (argc != 3) {
        std::fprintf(stderr, "usage: namecensus <planet.pbf> <out_dir> [threads]\n");
        return 2;
    }
    const std::string out_dir = argv[2];
    const auto t0 = std::chrono::steady_clock::now();
    PbfFile pbf(argv[1], threads);
    std::vector<Out> outs(pbf.thread_count());

    auto t = std::chrono::steady_clock::now();
    pbf.read_blocks([&](PbfBlock& block, unsigned th) {
        Out& o = outs[th];
        std::vector<Tag> tags;
        for (auto& rel : block.relations) {
            tags.clear();
            for (auto [ki, vi] : rel.tags.indices)
                if (ki < block.string_table.size() && vi < block.string_table.size())
                    tags.push_back({block.string_table[ki], block.string_table[vi]});
            if (tags.empty()) continue;
            const char* rtype = get(tags, "type");
            if (!is_area_relation_type(rtype)) continue;
            int64_t first_way = 0;
            for (auto& m : rel.members)
                if (m.type == 'w') { first_way = m.ref; break; }
            if (!first_way) continue;
            const char* boundary = get(tags, "boundary");
            const char* place = get(tags, "place");
            const AdminPlaceType pt = place ? place_override(place) : AdminPlaceType::NONE;
            const RelationKind kind = area_relation_kind(boundary, is_address_place_type(pt));
            bool made = false;
            if (kind == RelationKind::PLACE) {
                const char* name = best_name(tags);
                if (pt != AdminPlaceType::NONE && name) {
                    emit(o, ADMIN, 15, admin_minimal_keeps(15, uint8_t(pt)), 'r', rel.id, 'w', first_way, NAN, NAN,
                         name, tags);
                    made = true;
                }
            } else if (kind == RelationKind::ADMIN) {
                const uint8_t level = nominatim_admin_level(get(tags, "admin_level"));
                const char* name = best_name(tags);
                if (level >= 2 && level <= kMaxBoundaryLevel && name) {
                    emit(o, ADMIN, level, admin_minimal_keeps(level, 0), 'r', rel.id, 'w', first_way, NAN, NAN, name,
                         tags);
                    made = true;
                }
            } else if (kind == RelationKind::POSTAL) {
                const char* code = postal_area_code([&](const char* k) { return get(tags, k); });
                if (code) {
                    emit(o, ADMIN, POSTAL_AREA_LEVEL, false, 'r', rel.id, 'w', first_way, NAN, NAN, code, tags);
                    made = true;
                }
            }
            // A relation of any kind may also be a POI (a park that is a boundary=protected_area).
            const char* rn = best_name(tags);
            if (rn) {
                PoiCategory c = relation_poi_cat(tags);
                if (c != PoiCategory::UNKNOWN) {
                    uint8_t tier = poi_get_default_tier(c);
                    if ((get(tags, "wikipedia") || get(tags, "wikidata")) && tier > 1) tier--;
                    if (tier >= 1 && tier <= POI_MAX_SHIPPED_TIER) {
                        emit(o, POI, tier, false, 'r', rel.id, 'w', first_way, NAN, NAN, rn, tags);
                        made = true;
                    }
                }
            }
            if (made) o.wanted.push_back(first_way);
        }
    });
    std::fprintf(stderr, "relations %.1fs\n", secs(t));

    t = std::chrono::steady_clock::now();
    {
        const auto want_ways = take_wanted_ids(outs);
        std::vector<std::vector<Tag>> tl(pbf.thread_count());
        std::atomic<unsigned> next{0};
        thread_local int my = -1;
        pbf.read_ways_streaming([&](int64_t id, const WayRefs& refs, const uint32_t* ks, const uint32_t* vs, size_t n,
                                    const std::vector<std::string>& st) {
            if (my < 0) my = int(next.fetch_add(1));
            if (size_t(my) >= outs.size()) { std::fprintf(stderr, "more stream threads than outputs\n"); std::abort(); }
            Out& o = outs[size_t(my)];
            const bool wanted = want_ways.test(id);
            if (n == 0 && !wanted) return;
            const auto& r = refs.get();
            if (r.empty()) return;
            if (wanted) {
                o.way_nodes.push_back({id, r.front()});
                o.wanted.push_back(r.front());
            }
            if (n == 0) return;
            const bool closed = r.size() >= 4 && r.front() == r.back();
            if (!closed) return;
            std::vector<Tag>& tags = tl[size_t(my)];
            tags_of(ks, vs, n, st, tags);
            bool made = false;
            const char* boundary = get(tags, "boundary");
            const char* place = get(tags, "place");
            const char* name = best_name(tags);
            if (boundary && std::strcmp(boundary, "administrative") == 0 && name) {
                const uint8_t al = nominatim_admin_level(get(tags, "admin_level"));
                const char* iso = get(tags, "ISO3166-1:alpha2");
                uint8_t level = 0;
                if (al >= kMinBoundaryWayLevel && al <= kMaxBoundaryLevel) level = al;
                else if (al == 2 && iso && iso[0]) level = 2;
                if (level) {
                    emit(o, ADMIN, level, admin_minimal_keeps(level, 0), 'w', id, 'n', r.front(), NAN, NAN, name, tags);
                    made = true;
                }
            } else if (place && name) {
                // boundary=place closed ways and plain place=* closed ways: place areas.
                const AdminPlaceType pt = place_override(place);
                if (pt != AdminPlaceType::NONE && is_address_place_type(pt)) {
                    emit(o, ADMIN, 15, admin_minimal_keeps(15, uint8_t(pt)), 'w', id, 'n', r.front(), NAN, NAN, name,
                         tags);
                    made = true;
                }
            }
            const char* stored = nullptr;
            if (uint8_t tier = poi_tier(tags, false, &stored)) {
                emit(o, POI, tier, false, 'w', id, 'n', r.front(), NAN, NAN, stored, tags);
                made = true;
            }
            if (made) o.wanted.push_back(r.front());
        });
    }
    std::fprintf(stderr, "ways %.1fs\n", secs(t));

    t = std::chrono::steady_clock::now();
    {
        const auto want_nodes = take_wanted_ids(outs);
        std::vector<std::vector<Tag>> tl(pbf.thread_count());
        std::atomic<unsigned> next{0};
        thread_local int myn = -1;
        pbf.read_nodes_streaming([&](int64_t id, double lat, double lng, const uint32_t* ks, const uint32_t* vs,
                                     size_t n, const std::vector<std::string>& st) {
            if (myn < 0) myn = int(next.fetch_add(1));
            if (size_t(myn) >= outs.size()) { std::fprintf(stderr, "more stream threads than outputs\n"); std::abort(); }
            Out& o = outs[size_t(myn)];
            if (want_nodes.test(id)) o.locs.push_back({id, lat, lng});
            if (n == 0) return;
            std::vector<Tag>& tags = tl[size_t(myn)];
            tags_of(ks, vs, n, st, tags);
            const char* place = get(tags, "place");
            if (place) {
                const char* name = best_name(tags);
                if (!name || !name[0]) name = get(tags, "ref");
                static const std::pair<const char*, PlaceType> kTypes[] = {
                    {"city", PlaceType::CITY}, {"town", PlaceType::TOWN}, {"village", PlaceType::VILLAGE},
                    {"suburb", PlaceType::SUBURB}, {"hamlet", PlaceType::HAMLET},
                    {"neighbourhood", PlaceType::NEIGHBOURHOOD}, {"quarter", PlaceType::QUARTER}};
                if (name && name[0])
                    for (const auto& [v, pt] : kTypes)
                        if (std::strcmp(place, v) == 0) {
                            emit(o, PLACE, uint8_t(pt), admin_minimal_keeps_place(uint8_t(pt)), 'n', id, 0, 0, lat,
                                 lng, name, tags);
                            break;
                        }
            }
            const char* stored = nullptr;
            if (uint8_t tier = poi_tier(tags, true, &stored))
                emit(o, POI, tier, false, 'n', id, 0, 0, lat, lng, stored, tags);
        });
    }
    std::fprintf(stderr, "nodes %.1fs\n", secs(t));

    std::ofstream records(out_dir + "/records.bin", std::ios::binary);
    std::ofstream way_nodes(out_dir + "/way_nodes.bin", std::ios::binary);
    std::ofstream node_locs(out_dir + "/node_locs.bin", std::ios::binary);
    for (auto* out : {&records, &way_nodes, &node_locs}) out->exceptions(std::ios::failbit | std::ios::badbit);
    std::vector<std::pair<int64_t, int64_t>> wn;
    std::vector<std::tuple<int64_t, double, double>> nl;
    Stats total[CLASS_COUNT][SUB_BUCKETS];
    for (Out& o : outs) {
        records.write(o.recs.data(), o.recs.size());
        wn.insert(wn.end(), o.way_nodes.begin(), o.way_nodes.end());
        nl.insert(nl.end(), o.locs.begin(), o.locs.end());
        for (int c = 0; c < CLASS_COUNT; c++)
            for (size_t s = 0; s < SUB_BUCKETS; s++) total[c][s].add(o.st[c][s]);
    }
    std::sort(wn.begin(), wn.end());
    std::sort(nl.begin(), nl.end());
    for (const auto& [way, node] : wn) {
        write_value(way_nodes, way);
        write_value(way_nodes, node);
    }
    for (const auto& [id, lat, lon] : nl) {
        write_value(node_locs, id);
        write_value(node_locs, lat);
        write_value(node_locs, lon);
    }
    records.close();
    way_nodes.close();
    node_locs.close();

    const char* cls_names[CLASS_COUNT] = {"place", "admin", "poi"};
    std::printf("class\tsub\trecords\tonly_stored_name\tbundle_bytes\tbundle_bytes_beyond_stored");
    for (int g = 0; g < GROUP_COUNT; g++) std::printf("\t%s:records\t%s:keys\t%s:bytes", GROUP_NAMES[g], GROUP_NAMES[g], GROUP_NAMES[g]);
    std::printf("\n");
    for (int c = 0; c < CLASS_COUNT; c++)
        for (size_t s = 0; s < SUB_BUCKETS; s++) {
            const Stats& x = total[c][s];
            if (!x.records) continue;
            std::printf("%s\t%zu\t%llu\t%llu\t%llu\t%llu", cls_names[c], s, (unsigned long long)x.records,
                        (unsigned long long)x.only_stored, (unsigned long long)x.bundle_bytes,
                        (unsigned long long)x.bundle_bytes_named);
            for (int g = 0; g < GROUP_COUNT; g++)
                std::printf("\t%llu\t%llu\t%llu", (unsigned long long)x.with[g], (unsigned long long)x.keys[g],
                            (unsigned long long)x.bytes[g]);
            std::printf("\n");
        }
    if (std::fflush(stdout) == EOF) throw std::runtime_error("could not write census table");
    std::fprintf(stderr, "way_nodes %zu node_locs %zu\nCENSUS_DONE %.1fs\n", wn.size(), nl.size(), secs(t0));
    return 0;
} catch (const std::exception& error) {
    std::fprintf(stderr, "namecensus: %s\n", error.what());
    return 1;
}
