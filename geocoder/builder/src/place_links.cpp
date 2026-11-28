#include "place_links.h"

#include <algorithm>
#include <atomic>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "admin_rank_config.h"
#include "country_code.h"
#include "parallel.h"
#include "place_claims.h"
#include "polygon_relations.h"

namespace {

// What find_linked_place answers for one polygon: the step and the node, in the candidates.
struct PlaceLink {
    LinkStep step = LinkStep::NONE;
    uint32_t candidate = NO_DATA;
};

// Nominatim links no polygon of this rank_search or more, and no node (:285, :331).
constexpr uint8_t kStreetSearchRank = 26;
// index_by_rank(1, 4): the runners before the boundaries' (indexer.py).
constexpr uint8_t kMaxFirstRunnerRank = 4;

// The lower case of code point c, as towlower maps the scripts lowercase_utf8 names.
char32_t lower(char32_t c) {
    auto pairs = [&](char32_t lo, char32_t hi, bool even_upper) {
        return c >= lo && c <= hi && ((c % 2 == 0) == even_upper);
    };
    if (c < 0x80) return c >= 'A' && c <= 'Z' ? c + 32 : c;
    if ((c >= 0xC0 && c <= 0xD6) || (c >= 0xD8 && c <= 0xDE)) return c + 32;
    if (c == 0x130) return 'i';
    if (c == 0x178) return 0xFF;
    if (pairs(0x100, 0x12F, true) || pairs(0x132, 0x137, true) || pairs(0x139, 0x148, false) ||
        pairs(0x14A, 0x177, true) || pairs(0x179, 0x17E, false) || pairs(0x182, 0x185, true) ||
        pairs(0x1A0, 0x1A5, true) || pairs(0x1CD, 0x1DC, false) || pairs(0x1DE, 0x1EF, true) ||
        pairs(0x1F8, 0x21F, true) || pairs(0x222, 0x233, true) || pairs(0x246, 0x24F, true))
        return c + 1;
    switch (c) {
        case 0x181: return 0x253; case 0x186: return 0x254; case 0x187: return 0x188; case 0x189: return 0x256;
        case 0x18A: return 0x257; case 0x18B: return 0x18C; case 0x18E: return 0x1DD; case 0x18F: return 0x259;
        case 0x190: return 0x25B; case 0x191: return 0x192; case 0x193: return 0x260; case 0x194: return 0x263;
        case 0x196: return 0x269; case 0x197: return 0x268; case 0x198: return 0x199; case 0x19C: return 0x26F;
        case 0x19D: return 0x272; case 0x19F: return 0x275; case 0x1A7: return 0x1A8; case 0x1A9: return 0x283;
        case 0x1AC: return 0x1AD; case 0x1AE: return 0x288; case 0x1AF: return 0x1B0; case 0x1B1: return 0x28A;
        case 0x1B2: return 0x28B; case 0x1B3: return 0x1B4; case 0x1B5: return 0x1B6; case 0x1B7: return 0x292;
        case 0x1B8: return 0x1B9; case 0x1BC: return 0x1BD; case 0x1C4: case 0x1C5: return 0x1C6;
        case 0x1C7: case 0x1C8: return 0x1C9; case 0x1CA: case 0x1CB: return 0x1CC;
        case 0x1F1: case 0x1F2: return 0x1F3; case 0x1F4: return 0x1F5; case 0x1F6: return 0x195;
        case 0x1F7: return 0x1BF; case 0x220: return 0x19E; case 0x23A: return 0x2C65; case 0x23B: return 0x23C;
        case 0x23D: return 0x19A; case 0x23E: return 0x2C66; case 0x241: return 0x242; case 0x243: return 0x180;
        case 0x244: return 0x289; case 0x245: return 0x28C;
        // Greek
        case 0x370: return 0x371; case 0x372: return 0x373; case 0x376: return 0x377; case 0x37F: return 0x3F3;
        case 0x386: return 0x3AC; case 0x38C: return 0x3CC; case 0x38E: return 0x3CD; case 0x38F: return 0x3CE;
        case 0x3CF: return 0x3D7; case 0x3F4: return 0x3B8; case 0x3F7: return 0x3F8; case 0x3F9: return 0x3F2;
        case 0x3FA: return 0x3FB; case 0x1E9E: return 0xDF; case 0x4C0: return 0x4CF;
        case 0x1FBC: return 0x1FB3; case 0x1FCC: return 0x1FC3; case 0x1FEC: return 0x1FE5; case 0x1FFC: return 0x1FF3;
        case 0x10C7: return 0x2D27; case 0x10CD: return 0x2D2D;
        default: break;
    }
    if (c >= 0x388 && c <= 0x38A) return c + 37;
    if ((c >= 0x391 && c <= 0x3A1) || (c >= 0x3A3 && c <= 0x3AB)) return c + 32;
    if (pairs(0x3D8, 0x3EF, true)) return c + 1;
    if (c >= 0x3FD && c <= 0x3FF) return c - 130;
    // Cyrillic
    if (c >= 0x400 && c <= 0x40F) return c + 80;
    if (c >= 0x410 && c <= 0x42F) return c + 32;
    if (pairs(0x460, 0x481, true) || pairs(0x48A, 0x4BF, true) || pairs(0x4C1, 0x4CE, false) ||
        pairs(0x4D0, 0x52F, true))
        return c + 1;
    // Armenian, Georgian
    if (c >= 0x531 && c <= 0x556) return c + 48;
    if (c >= 0x10A0 && c <= 0x10C5) return c + 0x1C60;
    if ((c >= 0x1C90 && c <= 0x1CBA) || (c >= 0x1CBD && c <= 0x1CBF)) return c - 0xBC0;
    // Latin Extended Additional, Greek Extended
    if (pairs(0x1E00, 0x1E95, true) || pairs(0x1EA0, 0x1EFF, true)) return c + 1;
    if ((c >= 0x1F08 && c <= 0x1F0F) || (c >= 0x1F18 && c <= 0x1F1D) || (c >= 0x1F28 && c <= 0x1F2F) ||
        (c >= 0x1F38 && c <= 0x1F3F) || (c >= 0x1F48 && c <= 0x1F4D) ||
        (c >= 0x1F59 && c <= 0x1F5F && c % 2 == 1) || (c >= 0x1F68 && c <= 0x1F6F) ||
        (c >= 0x1F88 && c <= 0x1F8F) || (c >= 0x1F98 && c <= 0x1F9F) || (c >= 0x1FA8 && c <= 0x1FAF) ||
        (c >= 0x1FB8 && c <= 0x1FB9) || (c >= 0x1FD8 && c <= 0x1FD9) || (c >= 0x1FE8 && c <= 0x1FE9))
        return c - 8;
    if (c >= 0x1FBA && c <= 0x1FBB) return c - 74;
    if (c >= 0x1FC8 && c <= 0x1FCB) return c - 86;
    if (c >= 0x1FDA && c <= 0x1FDB) return c - 100;
    if (c >= 0x1FEA && c <= 0x1FEB) return c - 112;
    if (c >= 0x1FF8 && c <= 0x1FF9) return c - 128;
    if (c >= 0x1FFA && c <= 0x1FFB) return c - 126;
    // Number forms, enclosed letters, Glagolitic, Coptic, Cyrillic and Latin extensions, fullwidth
    if (c >= 0x2160 && c <= 0x216F) return c + 16;
    if (c >= 0x24B6 && c <= 0x24CF) return c + 26;
    if (c >= 0x2C00 && c <= 0x2C2F) return c + 48;
    if (pairs(0x2C80, 0x2CE3, true) || pairs(0xA640, 0xA66D, true) || pairs(0xA680, 0xA69B, true) ||
        pairs(0xA722, 0xA72F, true) || pairs(0xA732, 0xA76F, true) || pairs(0xA77E, 0xA787, true) ||
        pairs(0xA790, 0xA793, true) || pairs(0xA796, 0xA7A9, true))
        return c + 1;
    if (c >= 0xFF21 && c <= 0xFF3A) return c + 32;
    if (c >= 0x10400 && c <= 0x10427) return c + 40;
    return c;
}

void put_utf8(char32_t c, std::string& out) {
    if (c < 0x80) {
        out.push_back(static_cast<char>(c));
    } else if (c < 0x800) {
        out.push_back(static_cast<char>(0xC0 | (c >> 6)));
        out.push_back(static_cast<char>(0x80 | (c & 0x3F)));
    } else if (c < 0x10000) {
        out.push_back(static_cast<char>(0xE0 | (c >> 12)));
        out.push_back(static_cast<char>(0x80 | ((c >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (c & 0x3F)));
    } else {
        out.push_back(static_cast<char>(0xF0 | (c >> 18)));
        out.push_back(static_cast<char>(0x80 | ((c >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((c >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (c & 0x3F)));
    }
}

// A polygon as its link step sees a node: its point inside or on the border.
class Covers {
public:
    Covers(const ParsedData& data, const AdminObjects& all, const AdminObject& o) : data_(data), all_(all), o_(o) {}

    bool operator()(PackedLocation p, size_t& tests) {
        const ExactRingBox& b = o_.box;
        if (p.lat_e7 < b.lat_lo || p.lat_e7 > b.lat_hi || p.lon_e7 < b.lng_lo || p.lon_e7 > b.lng_hi) return false;
        tests++;
        if (parts_.empty())
            for (const RingPolygon& part : object_parts(data_, all_, o_))
                parts_.push_back(std::make_unique<PolygonIndex>(part, false));
        const HalfPoint h = half_point(p);
        for (const auto& part : parts_)
            if (part->locate(h) != Where::OUTSIDE) return true;
        return false;
    }

private:
    const ParsedData& data_;
    const AdminObjects& all_;
    const AdminObject& o_;
    std::vector<std::unique_ptr<PolygonIndex>> parts_;
};

}  // namespace

bool english_only_country(uint16_t cc) { return country_language(cc) == "en"; }

std::string_view linked_english_name(bool own_name_en, uint16_t polygon_country, std::string_view node_name_en,
                                     std::string_view node_name, uint16_t node_country) {
    if (own_name_en || english_only_country(polygon_country)) return {};
    if (!node_name_en.empty()) return node_name_en;
    if (english_only_country(node_country)) return node_name;
    return {};
}

std::string lowercase_utf8(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size();) {
        const unsigned char c = static_cast<unsigned char>(s[i]);
        const size_t n = c < 0x80 ? 1 : (c >> 5) == 0x6 ? 2 : (c >> 4) == 0xE ? 3 : (c >> 3) == 0x1E ? 4 : 0;
        bool valid = n > 0 && i + n <= s.size();
        char32_t cp = n == 1 ? c : n == 2 ? c & 0x1F : n == 3 ? c & 0x0F : c & 0x07;
        for (size_t k = 1; valid && k < n; k++) {
            const unsigned char cc = static_cast<unsigned char>(s[i + k]);
            valid = (cc & 0xC0) == 0x80;
            cp = cp << 6 | (cc & 0x3F);
        }
        // UTF-8 only where the scalar needs all its bytes, and is no surrogate nor past U+10FFFF.
        static constexpr char32_t kLeast[] = {0, 0, 0x80, 0x800, 0x10000};
        if (valid && (cp < kLeast[n] || (cp >= 0xD800 && cp <= 0xDFFF) || cp > 0x10FFFF)) valid = false;
        if (!valid) {
            out.push_back(s[i]);
            i++;
            continue;
        }
        put_utf8(lower(cp), out);
        i += n;
    }
    return out;
}

LinkStats find_linked_places(ParsedData& data, const RankLookups& lookups, unsigned threads) {
    LinkStats stats;
    const AdminObjects all = admin_objects(data, lookups, threads);
    const std::vector<AdminObject>& objects = all.objects;
    LinkCandidates& candidates = data.link_candidates;
    std::vector<LinkCandidate>& nodes = candidates.nodes;
    const LinkText& text = candidates.text;
    // A build of extracts that overlap lists a node they share once an extract: of its copies the
    // first by their fields stays, the same one whatever order the extracts came in.
    auto fields = [&](const LinkCandidate& c) {
        return std::make_tuple(c.at.lat_e7, c.at.lon_e7, c.value, text.at(c.name), text.at(c.wikidata), text.at(c.name_en),
                               text.at(c.name_tag));
    };
    parallel_sort(nodes.begin(), nodes.end(), [&](const LinkCandidate& a, const LinkCandidate& b) {
        return a.node != b.node ? a.node < b.node : fields(a) < fields(b);
    }, threads);
    nodes.erase(std::unique(nodes.begin(), nodes.end(), [](const LinkCandidate& a, const LinkCandidate& b) { return a.node == b.node; }),
                nodes.end());
    stats.candidates = nodes.size();
    stats.polygons = objects.size();

    // Each node's country, and its ranks there.
    std::vector<uint16_t> node_country(nodes.size());
    std::vector<PlaceRanks> ranks(nodes.size());
    parallel_for(nodes.size(), [&](size_t begin, size_t end, unsigned) {
        for (size_t i = begin; i < end; i++) {
            node_country[i] = lookups.country_at(nodes[i].at);
            ranks[i] = place_value_ranks(node_country[i], static_cast<PlaceValue>(nodes[i].value));
        }
    }, threads);

    // The nodes by wikidata, by name and by place value and latitude, each in osm id order.
    auto by = [&](auto key, auto keep) {
        std::vector<uint32_t> out;
        for (uint32_t i = 0; i < nodes.size(); i++)
            if (keep(nodes[i])) out.push_back(i);
        std::stable_sort(out.begin(), out.end(), [&](uint32_t a, uint32_t b) { return key(a) < key(b); });
        return out;
    };
    const std::vector<uint32_t> by_wikidata = by([&](uint32_t i) { return text.at(nodes[i].wikidata); },
                                                 [](const LinkCandidate& c) { return c.wikidata != LinkText::kNone; });
    const std::vector<uint32_t> by_name = by([&](uint32_t i) { return text.at(nodes[i].name); },
                                             [](const LinkCandidate& c) { return c.name != LinkText::kNone; });
    const std::vector<uint32_t> by_value = by(
        [&](uint32_t i) { return std::make_pair(nodes[i].value, nodes[i].at.lat_e7); },
        [](const LinkCandidate& c) { return c.name != LinkText::kNone; });
    auto equal_range_of = [&](const std::vector<uint32_t>& index, auto key, auto value) {
        return std::equal_range(index.begin(), index.end(), value,
                                [&](const auto& a, const auto& b) { return key(a) < key(b); });
    };
    auto wikidata_key = [&](const auto& x) {
        if constexpr (std::is_same_v<std::decay_t<decltype(x)>, uint32_t>) return text.at(nodes[x].wikidata);
        else return std::string_view(x);
    };
    auto name_key = [&](const auto& x) {
        if constexpr (std::is_same_v<std::decay_t<decltype(x)>, uint32_t>) return text.at(nodes[x].name);
        else return std::string_view(x);
    };

    // The runners, in Nominatim's order: rank_address 1 to 4, then the boundaries by rank_search,
    // then the rest by rank_address. Polygons that link nothing are in none.
    struct Runner {
        uint8_t phase, rank;
        bool operator<(const Runner& o) const { return phase != o.phase ? phase < o.phase : rank < o.rank; }
        bool operator!=(const Runner& o) const { return phase != o.phase || rank != o.rank; }
    };
    auto runner_of = [&](const AdminObject& o) {
        if (o.initial.address <= kMaxFirstRunnerRank) return Runner{0, o.initial.address};
        if (o.boundary()) return Runner{1, o.initial.search};
        return Runner{2, o.initial.address};
    };
    std::vector<uint32_t> order;
    for (uint32_t k = 0; k < objects.size(); k++)
        if (objects[k].initial.search < kStreetSearchRank && objects[k].initial.address > 0) order.push_back(k);
    // Within a runner, boundaries by admin level, place areas the larger first, as the rank pass
    // takes them; then by osm id.
    std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
        const AdminObject &oa = objects[a], &ob = objects[b];
        const Runner ra = runner_of(oa), rb = runner_of(ob);
        if (ra != rb) return ra < rb;
        if (oa.level != ob.level) return oa.level < ob.level;
        if (!oa.boundary() && oa.area != ob.area) return oa.area > ob.area;
        return oa.id < ob.id;
    });

    auto label_of = [&](const AdminObject& o) -> uint32_t {
        // The first label member that is a place node (:297-312).
        if (!o.relation || !o.tags) return NO_DATA;
        for (const int64_t label : o.tags->labels) {
            const auto it = std::lower_bound(nodes.begin(), nodes.end(), label,
                                             [](const LinkCandidate& c, int64_t id) { return c.node < id; });
            if (it != nodes.end() && it->node == label) return static_cast<uint32_t>(it - nodes.begin());
        }
        return NO_DATA;
    };
    // The nodes a relation links as its label member: the public server's index, which takes
    // every polygon again after its import, holds each with its label relation (a label link
    // reads no other link, :291-312), so no other polygon finds one of them unlinked, whichever
    // runner it is in.
    std::vector<char> label_held(nodes.size(), 0);
    for (uint32_t k : order)
        if (const uint32_t i = label_of(objects[k]); i != NO_DATA) label_held[i] = 1;

    std::vector<uint32_t> claimant(nodes.size(), NO_DATA);  // by node, the object of the last link
    std::vector<PlaceLink> links(objects.size());
    std::atomic<size_t> covers_tests{0};
    auto unlinked = [&](uint32_t i) {
        return claimant[i] == NO_DATA && !label_held[i] && ranks[i].search < kStreetSearchRank;
    };
    auto same_or_no_wikidata = [&](uint32_t i, const AdminObject& o) {
        return nodes[i].wikidata == LinkText::kNone || !o.wikidata() || text.at(nodes[i].wikidata) == *o.wikidata();
    };
    auto link_of = [&](uint32_t k) -> PlaceLink {
        const AdminObject& o = objects[k];
        size_t tests = 0;
        Covers covers(data, all, o);
        PlaceLink found;
        auto done = [&](LinkStep step, uint32_t i) {
            found = {step, i};
            return true;
        };
        auto search = [&]() {
            const AdminTags* tags = o.tags;
            // 1. A relation's label member.
            if (const uint32_t i = label_of(o); i != NO_DATA) return done(LinkStep::LABEL, i);
            const std::string_view name = tags ? std::string_view(tags->name) : std::string_view();
            // 2. Its wikidata's nodes: unnamed ones first (a null sorts first in Nominatim's
            //    descending order), then those of its name, then the others. The order leaves
            //    nodes of one place in it unordered: of those the one of the lowest address rank
            //    in its country, one of rank 0 last (Espanola's town before its hamlet), then the
            //    lower osm id.
            if (const std::string* wikidata = o.wikidata()) {
                const auto [first, last] = equal_range_of(by_wikidata, wikidata_key, std::string_view(*wikidata));
                std::vector<uint32_t> found_nodes(first, last);
                auto order_key = [&](uint32_t i) {
                    const int named = name.empty() || nodes[i].name == LinkText::kNone ? 0 : text.at(nodes[i].name) == name ? 1 : 2;
                    return std::make_pair(named, ranks[i].address == 0 ? UINT8_MAX : ranks[i].address);
                };
                std::stable_sort(found_nodes.begin(), found_nodes.end(),
                                 [&](uint32_t a, uint32_t b) { return order_key(a) < order_key(b); });
                for (uint32_t i : found_nodes)
                    if (unlinked(i) && covers(nodes[i].at, tests)) return done(LinkStep::WIKIDATA, i);
            }
            if (name.empty()) return false;
            // 3. A node of its own place value whose name holds its name or lies in it.
            if (o.place_category && o.own != static_cast<uint8_t>(PlaceValue::OTHER)) {
                const auto first = std::lower_bound(by_value.begin(), by_value.end(), std::make_pair(o.own, o.box.lat_lo),
                                                    [&](uint32_t i, const std::pair<uint8_t, int32_t>& v) {
                                                        return std::make_pair(nodes[i].value, nodes[i].at.lat_e7) < v;
                                                    });
                std::vector<uint32_t> found_nodes;
                for (auto it = first; it != by_value.end() && nodes[*it].value == o.own && nodes[*it].at.lat_e7 <= o.box.lat_hi;
                     ++it) {
                    const uint32_t i = *it;
                    if (nodes[i].at.lon_e7 < o.box.lng_lo || nodes[i].at.lon_e7 > o.box.lng_hi) continue;
                    const std::string_view node_name = text.at(nodes[i].name);
                    if (node_name.find(name) == std::string_view::npos && name.find(node_name) == std::string_view::npos)
                        continue;
                    if (unlinked(i) && same_or_no_wikidata(i, o)) found_nodes.push_back(i);
                }
                std::sort(found_nodes.begin(), found_nodes.end());
                for (uint32_t i : found_nodes)
                    if (covers(nodes[i].at, tests)) return done(LinkStep::TYPE, i);
            }
            // 4. A node of its name and of its rank in the node's country.
            const auto [first, last] = equal_range_of(by_name, name_key, name);
            for (auto it = first; it != last; ++it) {
                const uint32_t i = *it;
                if (ranks[i].address == o.initial.address && unlinked(i) && same_or_no_wikidata(i, o) &&
                    covers(nodes[i].at, tests))
                    return done(LinkStep::NAME, i);
            }
            return false;
        };
        search();
        covers_tests += tests;
        return found;
    };

    for (size_t begin = 0; begin < order.size();) {
        size_t end = begin + 1;
        const Runner runner = runner_of(objects[order[begin]]);
        while (end < order.size() && !(runner_of(objects[order[end]]) != runner)) end++;
        stats.runners++;
        // Every row against the links as the runner starts, then each links its node in turn.
        parallel_for_dynamic(end - begin, 16, [&](size_t b, size_t e, unsigned) {
            for (size_t r = b; r < e; r++) links[order[begin + r]] = link_of(order[begin + r]);
        }, threads);
        for (size_t r = begin; r < end; r++) {
            const uint32_t k = order[r];
            const PlaceLink& link = links[k];
            if (link.step == LinkStep::NONE) continue;
            stats.by_step[static_cast<size_t>(link.step)]++;
            if (claimant[link.candidate] != NO_DATA && runner_of(objects[claimant[link.candidate]]) != runner)
                stats.taken++;
            claimant[link.candidate] = k;
        }
        begin = end;
    }
    stats.covers = covers_tests.load();

    // The labels, the names, the links, the claims and the place nodes' linking boundaries.
    std::vector<std::pair<uint64_t, uint32_t>> place_at;  // (node osm id, place node)
    place_at.reserve(data.place_osm_ids.size());
    for (uint32_t i = 0; i < data.place_osm_ids.size(); i++) place_at.push_back({data.place_osm_ids[i], i});
    std::sort(place_at.begin(), place_at.end());
    data.admin_links.clear();
    data.place_claims.clear();
    data.place_node_links.clear();
    std::vector<std::pair<uint64_t, std::string>> linked_names;
    for (size_t k = 0; k < objects.size(); k++) {
        const AdminObject& o = objects[k];
        const PlaceLink& link = links[k];
        AdminPlaceType label = AdminPlaceType::NONE;
        if (link.step != LinkStep::NONE) {
            const LinkCandidate& c = nodes[link.candidate];
            data.admin_links.push_back({o.id, c.node, c.value, ranks[link.candidate].address, link.step});
            label = place_label_of(static_cast<PlaceValue>(c.value));
            const std::string_view name = linked_english_name(!o.tags || o.tags->name_en, o.country, text.at(c.name_en),
                                                              text.at(c.name_tag), node_country[link.candidate]);
            if (!name.empty()) {
                // Its own name stays one of its names, aside (own_name_id).
                const uint32_t name_id = data.string_pool.intern(std::string(name));
                bool named = false;
                for (uint32_t m = o.first; m < o.first + o.count; m++) {
                    AdminPolygon& p = data.admin_polygons[all.members[m]];
                    if (p.name_id == name_id) continue;
                    p.own_name_id = p.name_id;
                    p.name_id = name_id;
                    named = true;
                }
                stats.named += named;
            }
            // Every copy of the node, as two overlapping extracts list it twice.
            const uint64_t node_id = pack_osm_id(gc::id_alloc::ObjectType::OSM_NODE, c.node);
            const auto& tags = *data.search_tags;
            linked_names.emplace_back(o.id, tag_bundles::merge_linked(tags.get(tag_bundles::Use::Core, o.id),
                tags.get(tag_bundles::Use::Core, node_id), country_language(node_country[link.candidate])));
            for (auto p = std::lower_bound(place_at.begin(), place_at.end(), std::make_pair(node_id, uint32_t(0)));
                 p != place_at.end() && p->first == node_id; ++p)
                data.place_node_links.push_back({p->second, o.id});
        }
        if (label == AdminPlaceType::NONE) label = place_label_of(static_cast<PlaceValue>(o.own));
        for (uint32_t m = o.first; m < o.first + o.count; m++)
            data.admin_polygons[all.members[m]].place_type_override = static_cast<uint8_t>(label);
    }
    for (uint32_t i = 0; i < nodes.size(); i++) {
        if (claimant[i] == NO_DATA) continue;
        data.place_claims.push_back({pack_osm_id(gc::id_alloc::ObjectType::OSM_NODE, nodes[i].node), objects[claimant[i]].id});
        stats.claimed++;
    }
    data.search_tags->stores[size_t(tag_bundles::Use::Core)].replace(linked_names, threads);
    sort_place_node_links(data.place_node_links);
    candidates = LinkCandidates{};
    return stats;
}
