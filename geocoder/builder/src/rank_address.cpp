#include "rank_address.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "admin_rank_config.h"
#include "exact_pip.h"
#include "parallel.h"
#include "place_claims.h"
#include "polygon_relations.h"
#include "polygon_rings.h"

namespace {

// The ranks an admin-nested parent has (placex_triggers.sql:891) and the
// highest the step gives a boundary (:903-904).
constexpr uint8_t kMaxParentRank = 25;
// parent_address_level where no parent is found (:878).
constexpr uint8_t kNoParentLevel = 3;
// A parent of this rank or more lifts a boundary to kMaxParentRank (:903).
constexpr uint8_t kParentCapRank = 24;
// A boundary above this rank takes the place-area step (:912).
constexpr uint8_t kMaxUncontainedBoundaryRank = 9;
// The ranks of the places that lift a boundary inside them (:920), and the
// highest place rank of an area that lifts a place area (:937, below 24).
constexpr uint8_t kMaxContainerRank = 23;
// The ranks of the place areas an area lifts (:932).
constexpr uint8_t kMinLiftedRank = 16, kMaxLiftedRank = 23;
// A link or an own place tag ranks a polygon only below the streets (:1130).
constexpr uint8_t kStreetRank = 26;
// An own place tag ranks a boundary of these ranks (:1159).
constexpr uint8_t kMinTaggedRank = 4, kMaxTaggedRank = 25;
// What a lift adds to the container's or parent's rank (:906, :928, :948,
// :970).
constexpr uint8_t kLift = 2;
// The ids of the polygons without an osm id, above every ObjectType's.
constexpr uint64_t kNoOsmId = uint64_t(0xFF) << 56;

bool lifted_rank(uint8_t rank) { return rank >= kMinLiftedRank && rank <= kMaxLiftedRank; }

// Takes the place-area step: a place area, or a way with a place tag, of a rank it lifts
// (:930-933).
bool place_area_step(const AdminObject& o) {
    return (o.level == 15 || (!o.relation && o.place_category)) && lifted_rank(o.initial.address);
}

bool meets_area_and_box(const AdminObject& outer, const AdminObject& inner) {
    return outer.area >= inner.area && box_holds(outer.box, inner.box);
}

}  // namespace

AdminObjects admin_objects(ParsedData& data, const RankLookups& lookups, unsigned threads) {
    auto& polygons = data.admin_polygons;
    const AdminExactRings& exact = data.admin_exact_rings;
    if (data.admin_osm_ids.size() != polygons.size() || exact.polygons.size() != polygons.size())
        throw std::logic_error("the rank pass needs every admin polygon's osm id and rings");
    AdminObjects out;

    // Each boundary's polygons together, in ring order. A polygon without an osm id is an
    // object of its own.
    auto boundary_of = [&](uint32_t pid) {
        const uint64_t osm = data.admin_osm_ids[pid];
        return osm ? admin_boundary_id(osm) : kNoOsmId | pid;
    };
    std::vector<uint32_t>& members = out.members;
    for (uint32_t pid = 0; pid < polygons.size(); pid++)
        if (polygons[pid].admin_level != POSTAL_AREA_LEVEL && exact.polygons[pid].ring_count > 0) members.push_back(pid);
    std::sort(members.begin(), members.end(), [&](uint32_t a, uint32_t b) {
        const uint64_t ba = boundary_of(a), bb = boundary_of(b);
        return ba != bb ? ba < bb : data.admin_osm_ids[a] < data.admin_osm_ids[b];
    });
    std::vector<AdminObject>& objects = out.objects;
    out.object_of.assign(polygons.size(), NO_DATA);
    for (uint32_t k = 0; k < members.size(); k++) {
        const uint64_t id = boundary_of(members[k]);
        if (objects.empty() || objects.back().id != id) {
            objects.emplace_back();
            objects.back().id = id;
            objects.back().first = k;
        }
        objects.back().count++;
        out.object_of[members[k]] = static_cast<uint32_t>(objects.size() - 1);
    }
    std::sort(data.admin_tags.begin(), data.admin_tags.end(),
              [](const AdminTags& a, const AdminTags& b) { return a.boundary < b.boundary; });

    // What each object is, its point on surface, country and initial ranks.
    parallel_for_dynamic(objects.size(), 256, [&](size_t begin, size_t end, unsigned) {
        for (size_t k = begin; k < end; k++) {
            AdminObject& o = objects[k];
            const AdminPolygon& first = polygons[members[o.first]];
            o.level = first.admin_level;
            o.relation = o.id >> 56 == static_cast<uint64_t>(gc::id_alloc::ObjectType::OSM_RELATION);
            o.own = first.own_place_value;
            o.place_category = o.own != static_cast<uint8_t>(PlaceValue::NONE);
            o.boundary_category = o.boundary();
            double widest = -1;
            LatLng point{0, 0};
            for (uint32_t m = o.first; m < o.first + o.count; m++) {
                const uint32_t pid = members[m];
                const AdminPolygon& p = polygons[pid];
                const auto [at, width] = interior_point_and_width(exact.vertices.data(), exact.rings_of(pid),
                                                                  exact.polygons[pid].ring_count);
                if (width > widest) {
                    widest = width;
                    point = at;
                }
                if (p.country_code && !o.country) o.country = p.country_code;
                if ((p.flags & ADMIN_FLAG_BOUNDARY) && !o.boundary()) o.boundary_category = true;
                o.area += p.area;
                const RingSpan& outer = exact.rings_of(pid)[0];
                const ExactRingBox b = exact_ring_box(exact.vertices.data() + outer.begin, outer.count);
                o.box = {std::min(o.box.lat_lo, b.lat_lo), std::max(o.box.lat_hi, b.lat_hi),
                         std::min(o.box.lng_lo, b.lng_lo), std::max(o.box.lng_hi, b.lng_hi)};
            }
            o.point = point_e7(point.first, point.second);
            if (!o.country) o.country = lookups.country_at(o.point);
            o.initial = polygon_ranks(o.country, o.level, static_cast<PlaceValue>(o.own));
            // A closed way of a level below kMinBoundaryWayLevel is no boundary of Nominatim's:
            // the builder keeps a country's (level 2, a country code) for the country alone, in no
            // address, linking nothing.
            if (!o.relation && o.boundary() && o.level < kMinBoundaryWayLevel) o.initial = {};
            const auto t = std::lower_bound(data.admin_tags.begin(), data.admin_tags.end(), o.id,
                                            [](const AdminTags& a, uint64_t id) { return a.boundary < id; });
            if (t != data.admin_tags.end() && t->boundary == o.id) o.tags = &*t;
        }
    }, threads);
    return out;
}

std::vector<RingPolygon> object_parts(const ParsedData& data, const AdminObjects& objects, const AdminObject& o) {
    const AdminExactRings& exact = data.admin_exact_rings;
    std::vector<RingPolygon> parts;
    for (uint32_t m = o.first; m < o.first + o.count; m++) {
        const uint32_t pid = objects.members[m];
        parts.push_back({exact.vertices.data(), exact.rings_of(pid), exact.polygons[pid].ring_count});
    }
    return parts;
}

RankStats compute_rank_addresses(ParsedData& data, const RankLookups& lookups, unsigned threads,
                                 std::vector<RankTrace>* trace) {
    RankStats stats;
    auto& polygons = data.admin_polygons;
    for (AdminPolygon& p : polygons) p.rank_address = 0;
    const AdminObjects all = admin_objects(data, lookups, threads);
    const std::vector<AdminObject>& objects = all.objects;
    const std::vector<uint32_t>& members = all.members;
    const std::vector<uint32_t>& object_of = all.object_of;
    auto parts_of = [&](const AdminObject& o) { return object_parts(data, all, o); };

    // Each object's link, by object.
    std::vector<AdminLink>& admin_links = data.admin_links;
    std::sort(admin_links.begin(), admin_links.end(),
              [](const AdminLink& a, const AdminLink& b) { return a.boundary < b.boundary; });
    std::vector<const AdminLink*> link_of(objects.size(), nullptr);
    for (size_t k = 0; k < objects.size(); k++) {
        const auto l = std::lower_bound(admin_links.begin(), admin_links.end(), objects[k].id,
                                        [](const AdminLink& a, uint64_t id) { return a.boundary < id; });
        if (l != admin_links.end() && l->boundary == objects[k].id) link_of[k] = &*l;
    }

    // Round each object's point: its admin-nested parents, nearest first,
    // with whether each repeats it, and the polygons that may contain it.
    std::vector<std::vector<std::pair<uint32_t, bool>>> parents(objects.size());
    std::vector<std::pair<uint32_t, uint32_t>> pairs;  // (container, object)
    std::mutex pairs_mutex;
    parallel_for_dynamic(objects.size(), 64, [&](size_t begin, size_t end, unsigned) {
        std::vector<uint32_t> near, at;
        std::vector<std::pair<uint32_t, uint32_t>> mine;
        for (size_t k = begin; k < end; k++) {
            const AdminObject& o = objects[k];
            const bool nests = o.boundary() && o.relation && o.initial.address > 0;
            const bool contains = nests || place_area_step(o);
            if (!nests && !contains) continue;
            near.clear();
            at.clear();
            lookups.polygons_near(o.point, [&](uint32_t pid) { near.push_back(pid); });
            for (uint32_t pid : near) {
                const uint32_t q = pid < object_of.size() ? object_of[pid] : NO_DATA;
                if (q == NO_DATA || q == k || std::find(at.begin(), at.end(), q) != at.end()) continue;
                if (lookups.holds(pid, o.point)) at.push_back(q);
            }
            if (nests) {
                auto& mine_parents = parents[k];
                for (uint32_t q : at) {
                    const AdminObject& p = objects[q];
                    if (p.boundary() && p.relation && p.level > 3 && p.level < o.level) mine_parents.push_back({q, false});
                }
                // The relation of the highest level; equal levels, which Nominatim takes in no set
                // order, to the smaller, then the lower osm id.
                std::sort(mine_parents.begin(), mine_parents.end(), [&](const auto& a, const auto& b) {
                    const AdminObject &pa = objects[a.first], &pb = objects[b.first];
                    if (pa.level != pb.level) return pa.level > pb.level;
                    if (pa.area != pb.area) return pa.area < pb.area;
                    return pa.id < pb.id;
                });
                for (auto& [q, same] : mine_parents) {
                    const AdminObject& p = objects[q];
                    same = o.wikidata() && p.wikidata() && *o.wikidata() == *p.wikidata() && polygons_equal(parts_of(p), parts_of(o));
                }
            }
            // A boundary below rank 10 with no parent to lift it takes no containment step.
            if (nests && o.initial.address <= kMaxUncontainedBoundaryRank && parents[k].empty()) continue;
            for (uint32_t q : at) {
                const AdminObject& c = objects[q];
                if (!meets_area_and_box(c, o)) continue;
                // A boundary's containers: place polygons whose place rank can be its rank or
                // more. A place area's: any polygon of a place rank from its own up to 23.
                const bool candidate = nests ? c.place_category && c.initial.address >= o.initial.address
                                             : c.initial.address >= o.initial.address && c.initial.address <= kMaxContainerRank;
                if (candidate) mine.push_back({q, static_cast<uint32_t>(k)});
            }
        }
        std::lock_guard<std::mutex> lock(pairs_mutex);
        pairs.insert(pairs.end(), mine.begin(), mine.end());
    }, threads);
    std::sort(pairs.begin(), pairs.end());

    // ST_Contains and not ST_Equals, each container's rings indexed once.
    const auto pairs_start = std::chrono::steady_clock::now();
    std::vector<char> held(pairs.size());
    std::vector<size_t> runs;
    for (size_t i = 0; i < pairs.size(); i++)
        if (i == 0 || pairs[i].first != pairs[i - 1].first) runs.push_back(i);
    runs.push_back(pairs.size());
    auto indexed = [&](const std::vector<RingPolygon>& parts) {
        std::vector<std::unique_ptr<PolygonIndex>> out;
        for (const RingPolygon& p : parts) out.push_back(std::make_unique<PolygonIndex>(p));
        return out;
    };
    auto pointers = [](const std::vector<std::unique_ptr<PolygonIndex>>& parts) {
        std::vector<const PolygonIndex*> out;
        for (const auto& p : parts) out.push_back(p.get());
        return out;
    };
    parallel_for_dynamic(runs.size() - 1, 1, [&](size_t begin, size_t end, unsigned) {
        for (size_t r = begin; r < end; r++) {
            const AdminObject& c = objects[pairs[runs[r]].first];
            const std::vector<RingPolygon> c_parts = parts_of(c);
            const auto c_index = indexed(c_parts);
            const auto c_pointers = pointers(c_index);
            for (size_t i = runs[r]; i < runs[r + 1]; i++) {
                const std::vector<RingPolygon> b_parts = parts_of(objects[pairs[i].second]);
                const auto b_index = indexed(b_parts);
                held[i] = multipolygon_contains(c_pointers, pointers(b_index)) && !polygons_equal(c_parts, b_parts);
            }
        }
    }, threads);
    std::vector<std::vector<uint32_t>> containers(objects.size());
    for (size_t i = 0; i < pairs.size(); i++)
        if (held[i]) containers[pairs[i].second].push_back(pairs[i].first);
    stats.pairs = pairs.size();
    stats.pair_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - pairs_start).count();
    std::vector<std::pair<uint32_t, uint32_t>>().swap(pairs);

    // In the indexer's order, every rank read as it stands then.
    std::vector<uint8_t> rank(objects.size());
    for (size_t k = 0; k < objects.size(); k++) rank[k] = objects[k].initial.address;
    std::vector<RankTrace> traced(trace ? objects.size() : 0);
    auto traced_at = [&](uint32_t k) -> RankTrace* { return trace ? &traced[k] : nullptr; };
    // The container whose place rank is highest, then of the lower osm id, of those `fits` takes.
    auto best_container = [&](uint32_t k, auto fits) {
        uint32_t best = NO_DATA;
        for (uint32_t q : containers[k]) {
            if (!fits(q)) continue;
            const AdminObject &c = objects[q];
            if (best == NO_DATA || c.initial.address > objects[best].initial.address ||
                (c.initial.address == objects[best].initial.address && c.id < objects[best].id))
                best = q;
        }
        return best;
    };
    // placex_update's place-area step (:930-950): a container of a place rank from the area's own
    // up to 23 and a rank of 1 to 25 lifts it.
    auto area_step = [&](uint32_t k) {
        const uint32_t c = best_container(k, [&](uint32_t q) {
            return objects[q].initial.address >= rank[k] && objects[q].initial.address <= kMaxContainerRank &&
                   rank[q] >= 1 && rank[q] <= kMaxParentRank;
        });
        RankTrace* t = traced_at(k);
        if (c != NO_DATA) {
            if (t) {
                t->container = objects[c].id;
                t->container_rank = rank[c];
            }
            rank[k] = static_cast<uint8_t>(rank[c] + kLift);
            stats.contained++;
        }
        // Its branch takes no link step.
        if (t) t->before_link = rank[k];
    };
    // The link, else the own place tag (:1109-1172), above parent_level: the rank of the
    // place node the polygon links in the node's country, else that of its own place value in
    // its own, either up or down.
    auto link_or_own_tag = [&](uint32_t k, uint8_t parent_level) {
        const AdminObject& o = objects[k];
        RankTrace* t = traced_at(k);
        if (t) t->before_link = rank[k];
        if (const AdminLink* link = link_of[k]) {
            if (link->rank > parent_level && link->rank < kStreetRank && link->rank != rank[k]) {
                stats.linked++;
                stats.lowered += link->rank < rank[k];
                rank[k] = link->rank;
                if (t) t->step = 'L';
            }
            return;
        }
        if (!o.place_category || rank[k] < kMinTaggedRank || rank[k] > kMaxTaggedRank) return;
        const uint8_t tagged = place_value_ranks(o.country, static_cast<PlaceValue>(o.own)).address;
        if (tagged > parent_level && tagged < kStreetRank && tagged != rank[k]) {
            stats.own_tags++;
            stats.own_lowered += tagged < rank[k];
            rank[k] = tagged;
            if (t) t->step = 'O';
        }
    };

    std::vector<uint32_t> order;
    for (uint32_t k = 0; k < objects.size(); k++)
        if (objects[k].boundary()) order.push_back(k);
    std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
        const AdminObject &oa = objects[a], &ob = objects[b];
        if (oa.initial.search != ob.initial.search) return oa.initial.search < ob.initial.search;
        if (oa.level != ob.level) return oa.level < ob.level;
        return oa.id < ob.id;
    });
    stats.boundaries = order.size();
    for (uint32_t k : order) {
        const AdminObject& o = objects[k];
        if (o.relation && rank[k] > 0) {
            uint8_t parent_level = kNoParentLevel;
            for (const auto& [q, same] : parents[k]) {
                if (rank[q] < 1 || rank[q] > kMaxParentRank) continue;
                if (RankTrace* t = traced_at(k)) {
                    t->parent = objects[q].id;
                    t->parent_rank = rank[q];
                    t->duplicate = same;
                }
                if (same) {
                    rank[k] = 0;
                    stats.duplicates++;
                } else {
                    parent_level = rank[q];
                    if (rank[q] >= rank[k]) {
                        rank[k] = rank[q] >= kParentCapRank ? kMaxParentRank : static_cast<uint8_t>(rank[q] + kLift);
                        stats.nested++;
                    }
                }
                break;
            }
            if (rank[k] > kMaxUncontainedBoundaryRank) {
                const uint32_t c = best_container(k, [&](uint32_t q) {
                    return objects[q].place_category && objects[q].initial.address >= rank[k] && rank[q] >= 1 &&
                           rank[q] <= kMaxContainerRank;
                });
                if (c != NO_DATA) {
                    if (RankTrace* t = traced_at(k)) {
                        t->container = objects[c].id;
                        t->container_rank = rank[c];
                    }
                    rank[k] = static_cast<uint8_t>(rank[c] + kLift);
                    stats.contained++;
                }
            }
            link_or_own_tag(k, parent_level);
        } else if (place_area_step(o)) {
            area_step(k);
        } else {
            link_or_own_tag(k, kNoParentLevel);
        }
    }

    order.clear();
    for (uint32_t k = 0; k < objects.size(); k++)
        if (!objects[k].boundary()) order.push_back(k);
    // Within a rank the larger first, so that an area is final before the areas it holds.
    std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
        const AdminObject &oa = objects[a], &ob = objects[b];
        if (oa.initial.address != ob.initial.address) return oa.initial.address < ob.initial.address;
        if (oa.area != ob.area) return oa.area > ob.area;
        return oa.id < ob.id;
    });
    stats.place_areas = order.size();
    for (uint32_t k : order) {
        if (place_area_step(objects[k])) area_step(k);
        else link_or_own_tag(k, kNoParentLevel);
    }

    for (size_t k = 0; k < objects.size(); k++)
        for (uint32_t m = objects[k].first; m < objects[k].first + objects[k].count; m++)
            polygons[members[m]].rank_address = rank[k];
    if (trace) {
        for (size_t k = 0; k < objects.size(); k++) {
            RankTrace& t = traced[k];
            t.id = objects[k].id;
            t.level = objects[k].level;
            t.country = objects[k].country;
            t.initial = objects[k].initial;
            t.rank = rank[k];
            if (link_of[k]) {
                t.link = link_of[k]->step;
                t.node = link_of[k]->node;
            }
        }
        *trace = std::move(traced);
    }

    // Place nodes: their type's rank, two more inside a relation of that rank (:951-971): a
    // boundary by its rank, a place polygon by its place rank. A linked node is no place of its
    // own; where a dir leaves out the boundaries standing for it, it answers as one, and none of
    // them promotes it.
    auto& nodes = data.place_nodes;
    PlaceNodeLinks& links = data.place_node_links;
    sort_place_node_links(links);
    parallel_for(nodes.size(), [&](size_t begin, size_t end, unsigned) {
        std::vector<uint32_t> near;
        for (size_t i = begin; i < end; i++) {
            PlaceNode& pn = nodes[i];
            const uint8_t base = place_node_rank(pn.place_type);
            bool promoted = false;
            if (place_node_promotable(base)) {
                const PackedLocation p = point_e7(pn.lat, pn.lng);
                const auto first = std::lower_bound(links.begin(), links.end(), std::make_pair(uint32_t(i), uint64_t(0)));
                auto links_it = [&](const AdminObject& o) {
                    for (auto l = first; l != links.end() && l->first == i; ++l)
                        if (l->second == o.id) return true;
                    return false;
                };
                near.clear();
                lookups.polygons_near(p, [&](uint32_t pid) { near.push_back(pid); });
                for (uint32_t pid : near) {
                    const uint32_t q = pid < object_of.size() ? object_of[pid] : NO_DATA;
                    if (q == NO_DATA) continue;
                    const AdminObject& o = objects[q];
                    if (!o.relation || rank[q] < 1 || rank[q] > kMaxParentRank || links_it(o)) continue;
                    if (!((o.place_category && o.initial.address == base) || (o.boundary_category && rank[q] == base)))
                        continue;
                    if (lookups.holds(pid, p)) {
                        promoted = true;
                        break;
                    }
                }
            }
            pn.rank_address = static_cast<uint8_t>(base + (promoted ? PLACE_NODE_PROMOTION : 0));
        }
    }, threads);
    stats.nodes = nodes.size();
    stats.nodes_promoted = parallel_count(nodes.size(), [&](size_t i) {
        return nodes[i].rank_address != place_node_rank(nodes[i].place_type);
    }, threads);

    std::vector<AdminTags>().swap(data.admin_tags);
    std::vector<AdminLink>().swap(data.admin_links);
    PlaceNodeLinks().swap(links);
    return stats;
}
