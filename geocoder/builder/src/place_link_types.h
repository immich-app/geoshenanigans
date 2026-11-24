// What the link pass (place_links.h) reads and makes, as the parse collects
// and the rank pass reads it: the place nodes a boundary or place area may
// link, what each boundary and place area has to link by, and its link.
#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "packed_location.h"

// Text in one buffer: each string NUL-terminated at its offset.
struct LinkText {
    static constexpr uint32_t kNone = UINT32_MAX;
    std::string bytes;

    // The offset of a copy of s; kNone for an empty one.
    uint32_t add(std::string_view s) {
        if (s.empty()) return kNone;
        const uint32_t at = static_cast<uint32_t>(bytes.size());
        bytes.append(s);
        bytes.push_back('\0');
        return at;
    }
    std::string_view at(uint32_t offset) const {
        return offset == kNone ? std::string_view() : std::string_view(bytes.data() + offset);
    }
};

// A place node a boundary or place area may link (find_linked_place,
// placex_triggers.sql:271-391): one of any place value, named, of a
// wikidata or a relation's label member. Its name:en tag, else its name tag,
// as written, are the names a polygon linking it may go by
// (linked_english_name).
struct LinkCandidate {
    int64_t node = 0;                     // its osm id
    PackedLocation at{0, 0};
    uint8_t value = 0;                    // PlaceValue of its place tag
    uint32_t name = LinkText::kNone;      // its name tag lower-cased (lowercase_utf8), in the text
    uint32_t wikidata = LinkText::kNone;  // its wikidata tag, in the text
    uint32_t name_en = LinkText::kNone;   // its name:en tag, in the text
    uint32_t name_tag = LinkText::kNone;  // its name tag where it has no name:en, in the text
};

struct LinkCandidates {
    std::vector<LinkCandidate> nodes;
    LinkText text;

    void add(int64_t node, PackedLocation at, uint8_t value, std::string_view name, std::string_view wikidata,
             std::string_view name_en = {}, std::string_view name_tag = {}) {
        const uint32_t en = text.add(name_en);
        nodes.push_back({node, at, value, text.add(name), text.add(wikidata), en,
                         en == LinkText::kNone ? text.add(name_tag) : LinkText::kNone});
    }
    // Moves `other`'s candidates in after these.
    void append(LinkCandidates&& other) {
        const uint32_t base = static_cast<uint32_t>(text.bytes.size());
        auto moved = [base](uint32_t offset) { return offset == LinkText::kNone ? offset : base + offset; };
        for (LinkCandidate c : other.nodes) {
            c.name = moved(c.name);
            c.wikidata = moved(c.wikidata);
            c.name_en = moved(c.name_en);
            c.name_tag = moved(c.name_tag);
            nodes.push_back(c);
        }
        text.bytes += other.text.bytes;
        other = LinkCandidates{};
    }
};

// What find_linked_place reads of a boundary or place area beyond its rings
// and ranks: a relation's first label member, its name tag lower-cased and
// its wikidata tag; and whether a name:en tag of its own names it.
struct AdminTags {
    uint64_t boundary = 0;  // admin_boundary_id of its polygons
    int64_t label = -1;     // the node of its first label member, -1 for none
    std::string name;       // empty for none
    std::string wikidata;   // empty for none
    bool name_en = false;   // its polygons are named by a name:en tag of its own
};

// The step of find_linked_place that found a link.
enum class LinkStep : uint8_t { NONE = 0, LABEL, WIKIDATA, TYPE, NAME };

// A boundary's or place area's link: the place node it links and what that
// node ranks in its own country, which placex_update ranks a linked
// polygon by (placex_triggers.sql:1109-1133).
struct AdminLink {
    uint64_t boundary = 0;  // admin_boundary_id
    int64_t node = 0;       // its osm id
    uint8_t value = 0;      // the node's PlaceValue
    uint8_t rank = 0;       // the node's rank_address in its country
    LinkStep step = LinkStep::NONE;
};

// s with every letter of the Latin, Greek, Cyrillic, Armenian and Georgian
// scripts and the fullwidth Latin ones in lower case, as PostgreSQL's
// lower() maps them one by one; every other code point and any byte that is
// no UTF-8 kept as it is.
std::string lowercase_utf8(std::string_view s);
