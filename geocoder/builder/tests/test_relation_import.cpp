// Unit tests for relation_import.h: which relations Nominatim's flex style
// turns into admin, postal and place areas.
#include "relation_import.h"

#include "test_framework.h"

TEST(relation_area_types_are_multipolygon_and_boundary) {
    CHECK(is_area_relation_type("multipolygon"));
    CHECK(is_area_relation_type("boundary"));
}

TEST(relation_other_types_are_not_areas) {
    CHECK(!is_area_relation_type(nullptr));  // no type tag (R19573110)
    CHECK(!is_area_relation_type(""));
    CHECK(!is_area_relation_type("site"));
    CHECK(!is_area_relation_type("route"));
    CHECK(!is_area_relation_type("waterway"));  // a line, never an area
    CHECK(!is_area_relation_type("Boundary"));
}

TEST(relation_kind_from_boundary_and_place) {
    struct Case { const char* boundary; bool has_place; RelationKind kind; };
    const Case cases[] = {
        {"administrative", false, RelationKind::ADMIN},
        {"administrative", true, RelationKind::ADMIN},  // place=* only labels it
        {"postal_code", false, RelationKind::POSTAL},
        {"place", true, RelationKind::PLACE},
        {"place", false, RelationKind::NONE},  // boundary=place is dropped
        {nullptr, true, RelationKind::PLACE},
        {"census", true, RelationKind::PLACE},  // place=* is 'always'
        {"census", false, RelationKind::NONE},
        {nullptr, false, RelationKind::NONE},
    };
    for (const auto& c : cases) CHECK(area_relation_kind(c.boundary, c.has_place) == c.kind);
}
