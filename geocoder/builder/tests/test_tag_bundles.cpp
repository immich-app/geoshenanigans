// Tag bundles (tag_bundles.h): Nominatim's name and address keys, the bundle
// string, the linked place's names, the store by object and the tiers laid
// out in record order.
#include "tag_bundles.h"

#include "test_framework.h"

#include <string>
#include <vector>

using tag_bundles::KeyClass;

namespace {

using Tags = std::vector<std::pair<std::string, std::string>>;

// Calls f(key, value) for each tag, as a scan does.
auto each_of(const Tags& tags) {
    return [&tags](auto&& f) {
        for (const auto& [k, v] : tags) f(k, v);
    };
}

std::string bundle(const Tags& tags) {
    std::vector<tag_bundles::Pair> pairs;
    for (const auto& [k, v] : tags) pairs.emplace_back(k, v);
    return tag_bundles::encode(pairs);
}

}  // namespace

TEST(a_key_is_a_name_an_address_key_or_neither_as_nominatims_full_style_reads_it) {
    struct Case {
        const char* key;
        KeyClass cls;
    };
    const Case cases[] = {
        {"name", KeyClass::NAME},
        {"name:de", KeyClass::NAME},
        {"alt_name", KeyClass::NAME},
        {"alt_name_1", KeyClass::NAME},
        {"alt_name:en", KeyClass::NAME},
        {"official_name:de", KeyClass::NAME},
        {"short_name", KeyClass::NAME},
        {"brand", KeyClass::NAME},
        {"ref", KeyClass::NAME},
        {"old_ref", KeyClass::NAME},
        {"ISO3166-2", KeyClass::NAME},
        {"iata", KeyClass::NAME},
        {"name:prefix", KeyClass::NAME},
        {"addr:street:prefix", KeyClass::OTHER},
        {"name:suffix:de", KeyClass::NAME},
        {"addr:housename", KeyClass::NAME},
        // The pre-filter's keys, deleted or sent to the extratags.
        {"name:etymology", KeyClass::OTHER},
        {"name:etymology:wikidata", KeyClass::OTHER},
        {"name:full", KeyClass::OTHER},
        {"name:forward", KeyClass::OTHER},
        {"name:source", KeyClass::OTHER},
        {"source:name", KeyClass::OTHER},
        {"note:name", KeyClass::OTHER},
        {"is_in:postcode", KeyClass::OTHER},
        {"tiger:cfcc", KeyClass::OTHER},
        {"addr:city:en", KeyClass::OTHER},
        {"addr:street:name", KeyClass::OTHER},
        // Addresses.
        {"addr:street", KeyClass::ADDRESS},
        {"addr:housenumber", KeyClass::ADDRESS},
        {"addr:conscriptionnumber", KeyClass::ADDRESS},
        {"addr:place", KeyClass::ADDRESS},
        {"addr:postcode", KeyClass::ADDRESS},
        {"postal_code", KeyClass::ADDRESS},
        {"is_in:state", KeyClass::ADDRESS},
        {"tiger:county", KeyClass::ADDRESS},
        {"ISO3166-1", KeyClass::ADDRESS},
        // Neither.
        {"nat_name", KeyClass::OTHER},
        {"Name", KeyClass::OTHER},
        {"amenity", KeyClass::OTHER},
        {"wikipedia", KeyClass::OTHER},
        {"wikidata", KeyClass::OTHER},
        {"", KeyClass::OTHER},
    };
    for (const Case& c : cases) {
        const int before = gctest::failures();
        CHECK(tag_bundles::key_class(c.key) == c.cls);
        if (gctest::failures() > before) std::printf("    %s\n", c.key);
    }
}

TEST(a_bundle_holds_the_tags_of_its_class_keys_sorted) {
    const Tags futo = {{"building", "commercial"}, {"name", "FUTO"},
                       {"addr:housenumber", "1044"}, {"addr:street", "Liberty Park Drive"},
                       {"addr:city", "Austin"}, {"name:etymology", "x"}};

    CHECK_EQ(tag_bundles::bundle_of(each_of(futo), KeyClass::NAME), std::string("name\x1F" "FUTO"));
    CHECK_EQ(tag_bundles::bundle_of(each_of(futo), KeyClass::ADDRESS),
             std::string("addr:city\x1F" "Austin\x1E" "addr:housenumber\x1F" "1044\x1E" "addr:street\x1F" "Liberty Park Drive"));
    CHECK_EQ(tag_bundles::bundle_of(each_of(futo), KeyClass::ADDRESS, "addr:housenumber"),
             std::string("addr:city\x1F" "Austin\x1E" "addr:street\x1F" "Liberty Park Drive"));
    CHECK_EQ(tag_bundles::bundle_of(each_of({{"amenity", "cafe"}}), KeyClass::NAME), std::string());
}

TEST(a_bundle_keeps_a_value_whole_and_leaves_out_a_pair_it_cannot_hold) {
    CHECK_EQ(bundle({{"name", "Foo;Bar"}, {"alt_name", ""}}), std::string("alt_name\x1F\x1Ename\x1F" "Foo;Bar"));
    CHECK_EQ(bundle({{"name", "a\x1E" "b"}, {"name:de", "c"}}), std::string("name:de\x1F" "c"));
    CHECK_EQ(bundle({{"na\x1Fme", "a"}, {"ref", std::string("x\0y", 3)}}), std::string());
    CHECK_EQ(bundle({}), std::string());
}

TEST(a_bundle_stops_at_the_longest_string_a_tier_holds) {
    Tags tags;
    for (int i = 0; i < 1000; i++) tags.push_back({"name:" + std::to_string(1000 + i), std::string(100, 'x')});

    const std::string b = bundle(tags);

    CHECK(b.size() <= string_files::MAX_STRING);
    CHECK(b.size() > string_files::MAX_STRING - 120);
    CHECK_EQ(b.substr(0, 10), std::string("name:1000\x1F"));
    CHECK_EQ(tag_bundles::pairs_of(b).back().second, std::string_view(std::string(100, 'x')));
}

TEST(only_the_name_tag_alone_says_no_more_than_a_stored_name) {
    CHECK(tag_bundles::name_alone(bundle({{"name", "Austin"}})));
    CHECK(!tag_bundles::name_alone(bundle({{"name", "Austin"}, {"name:en", "Austin"}})));
    CHECK(!tag_bundles::name_alone(bundle({{"ref", "18"}})));
    CHECK(!tag_bundles::name_alone(bundle({{"name:en", "Austin"}})));
    CHECK(!tag_bundles::name_alone(bundle({{"named", "x"}})));
}

TEST(a_boundary_takes_the_names_of_its_place_node_it_lacks_as_place_keys) {
    // München's boundary and its city node: the node's Italian name is the one
    // the boundary lacks.
    const std::string boundary = bundle({{"name", "München"}, {"name:de", "München"}, {"name:en", "Munich"}});
    const std::string node = bundle({{"name", "München"}, {"name:en", "Munich"}, {"name:it", "Monaco di Baviera"}});

    CHECK_EQ(tag_bundles::merge_linked(boundary, node, "de"),
             bundle({{"name", "München"}, {"name:de", "München"}, {"name:en", "Munich"},
                     {"_place_name:it", "Monaco di Baviera"}}));
}

TEST(a_linked_nodes_name_is_its_name_in_its_countrys_one_language) {
    const std::string boundary = bundle({{"name", "Gemeinde Testdorf"}});
    const std::string node = bundle({{"name", "Testdorf"}});

    CHECK_EQ(tag_bundles::merge_linked(boundary, node, "de"),
             bundle({{"name", "Gemeinde Testdorf"}, {"_place_name", "Testdorf"}, {"_place_name:de", "Testdorf"}}));
    // A node of a name in that language keeps it; a country of several
    // languages adds none.
    CHECK_EQ(tag_bundles::merge_linked(boundary, bundle({{"name", "Testdorf"}, {"name:de", "Testdorf-Ost"}}), "de"),
             bundle({{"name", "Gemeinde Testdorf"}, {"_place_name", "Testdorf"}, {"_place_name:de", "Testdorf-Ost"}}));
    CHECK_EQ(tag_bundles::merge_linked(boundary, node, ""),
             bundle({{"name", "Gemeinde Testdorf"}, {"_place_name", "Testdorf"}}));
}

TEST(a_boundarys_own_names_of_an_underscore_key_go_and_one_of_no_names_takes_every_node_name) {
    CHECK_EQ(tag_bundles::merge_linked(bundle({{"name", "A"}, {"_x", "B"}}), bundle({{"name", "A"}}), ""),
             bundle({{"name", "A"}}));
    CHECK_EQ(tag_bundles::merge_linked("", bundle({{"name", "A"}, {"ref", "1"}}), ""),
             bundle({{"_place_name", "A"}, {"_place_ref", "1"}}));
    CHECK_EQ(tag_bundles::merge_linked(bundle({{"name", "A"}}), "", "de"), bundle({{"name", "A"}}));
}

TEST(a_store_finds_each_objects_bundle_by_its_key) {
    std::vector<tag_bundles::Collector> nodes(2), relations(1);
    nodes[0].add(30, "c\x1Fz");
    nodes[1].add(10, "a\x1Fx");
    nodes[1].add(20, "");
    relations[0].add(tag_bundles::key_of(admin_ring_id(7, 0)), "r\x1F" "7");
    relations[0].add(tag_bundles::key_of(admin_ring_id(7, 0)), "r\x1F" "7");
    tag_bundles::Store store;
    store.absorb(nodes);
    store.absorb(relations);
    store.sort();

    CHECK_EQ(store.size(), size_t(3));
    CHECK_EQ(store.find(10), std::string_view("a\x1Fx"));
    CHECK_EQ(store.find(30), std::string_view("c\x1Fz"));
    CHECK_EQ(store.find(20), std::string_view());
    CHECK_EQ(store.find(tag_bundles::key_of(admin_ring_id(7, 3))), std::string_view("r\x1F" "7"));
    CHECK_EQ(nodes[0].size(), size_t(0));

    store.replace({{10, "a\x1Fy"}, {40, "d\x1Fw"}});

    CHECK_EQ(store.find(10), std::string_view("a\x1Fy"));
    CHECK_EQ(store.find(40), std::string_view("d\x1Fw"));
    CHECK_EQ(store.find(30), std::string_view("c\x1Fz"));
}

TEST(a_tier_laid_out_in_record_order_holds_each_bundle_once_where_its_home_groups_first_record_is) {
    // Records 0..5: bundles by record, and each record's group.
    const std::vector<std::string> of = {"b\x1F" "1", "", "a\x1F" "1", "b\x1F" "1", "c\x1F" "1", "a\x1F" "1"};
    const std::vector<size_t> home = {1, 0, 1, 0, 2, 0};

    const tag_bundles::LaidOutTier t =
        tag_bundles::lay_out(of.size(), 3, [&](size_t r) { return std::string_view(of[r]); }, [&](size_t r) { return home[r]; });

    // Group 0: b (record 3) then a (record 5); group 2: c.
    const std::string bytes(t.bytes.begin(), t.bytes.end());
    CHECK_EQ(bytes, std::string("b\x1F" "1\0a\x1F" "1\0c\x1F" "1\0", 12));
    CHECK(t.offsets == std::vector<uint32_t>({0, NO_DATA, 4, 0, 8, 4}));
    CHECK(t.group_ends == std::vector<uint32_t>({8, 8, 12}));
}

TEST(search_columns_select_the_tags_of_their_own_carrier) {
    using tag_bundles::Use;
    const Tags tags{{"name", "Station"}, {"brand", "Rail"}, {"addr:housenumber", "7"}, {"addr:street", "Markt"},
                    {"addr:interpolation", "odd"}, {"wikidata", "Q7"}, {"wikipedia:de", "Bahnhof"}};
    auto selected = [&](Use use, const Tags& want, bool road = false) {
        CHECK(tag_bundles::select(use, each_of(tags), road) == bundle(want));
    };
    selected(Use::Core, {tags[0]});
    selected(Use::Poi, {tags[0], tags[1]});
    selected(Use::Address, {tags[0], tags[2], tags[3], tags[4], {"_road_member", "yes"}}, true);
    selected(Use::PoiAddress, {tags[2], tags[3], tags[4]});
    selected(Use::Street, {tags[0]});
    selected(Use::Interpolation, {tags[3], tags[4]});
    selected(Use::Wiki, {tags[5], tags[6]});
}

TEST(address_bundles_can_be_read_after_their_scratch_block_fills) {
    tag_bundles::Collector addresses;
    const std::string payload(string_files::MAX_STRING, 'x');
    const size_t count = TAG_ARENA_BLOCK_BYTES / payload.size() + 1;
    for (size_t i = 1; i <= count; ++i) addresses.add(i, payload, true);
    tag_bundles::Store store;
    store.absorb(addresses);
    store.sort();
    CHECK(store.find(1) == payload);
    CHECK(store.find(count) == payload);
}
