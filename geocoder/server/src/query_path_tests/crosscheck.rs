//! The address an answering house names in its addr:city, as Nominatim's complete_address_details
//! puts it first in its rank (results.py:610-632, CrosscheckNames): of the rows the house's street
//! has at a rank, the one of that name is the rank's part, a place node's or a polygon's, the
//! names of the place nodes a polygon links (its `_place_*` names) among the polygon's.

use super::*;

/// The United States, a country (rank 4) 100 km across around the origin.
fn united_states() -> TestIndex {
    let mut index = TestIndex { origin: ORIGIN, ..TestIndex::default() };
    index.boundary("United States", 2, 0, (-50_000.0, -50_000.0, 50_000.0, 50_000.0));
    index.polygons[0].0.country_code = u16::from_be_bytes(*b"US");
    index
}

/// The answer `north_m` north and `east_m` east of the origin: its display_name and its address.
fn answer_at(index: &Index, north_m: f64, east_m: f64) -> (String, serde_json::Value) {
    let p = at(north_m, east_m);
    let answer = serde_json::to_value(index.query(p.lat as f64, p.lng as f64)).unwrap();
    (answer["display_name"].as_str().unwrap_or_default().to_owned(), answer["address"].clone())
}

/// Whalens Court, Drexel Hill, Pennsylvania (W12111978): the street's rows at rank 20 are the
/// hamlet Addingham, the nearest and so its address (isaddress), and the suburb Drexel Hill
/// (N158588499), within twice Addingham's distance; Garrettford, a village in Upper Darby's
/// boundary, ranks 18. House 2 (W1499781229) carries `addr:city`, `city` here, which a closure
/// may set; Drexel Hill is `drexel_m` north of the street's east end.
fn whalens_court(drexel_m: f64, house: impl FnOnce(&mut TestIndex)) -> Index {
    let mut index = united_states();
    index
        .boundary("Pennsylvania", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Delaware County", 6, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Upper Darby", 8, 0, (-4000.0, -4000.0, 4000.0, 4000.0))
        .place("Garrettford", 2, 600.0, 900.0)
        .place("Addingham", 4, 300.0, -200.0)
        .place("Drexel Hill", 3, drexel_m, 200.0)
        .street("Whalens Court", &[(0.0, -200.0), (0.0, 200.0)])
        .address("2", 10.0, 0.0)
        .addr_postcode("19026");
    house(&mut index);
    index.load()
}

#[test]
fn a_house_s_addr_city_takes_its_rank_from_the_place_its_street_has_there() {
    // Nominatim answers Drexel Hill at the house, Addingham on the street alone: the house's
    // addr:city puts Drexel Hill first at rank 20, and the hamlet is no part.
    let index = whalens_court(500.0, |house| {
        house.addr_city("Drexel Hill");
    });
    let (display, address) = answer_at(&index, 10.0, 0.0);
    assert_eq!(display, "2 Whalens Court, Drexel Hill, Garrettford, Upper Darby, Delaware County, Pennsylvania, 19026, United States");
    assert_eq!(address["suburb"], "Drexel Hill");
    assert_eq!(address["hamlet"], serde_json::Value::Null);
    assert_eq!(address["village"], "Garrettford");
    assert_eq!(address["city"], "Upper Darby");

    // The street wins 5 m south of it and takes its number from the house, 15 m away, whose
    // addr:city it takes too.
    let (display, address) = answer_at(&index, -5.0, 0.0);
    assert_eq!(display, "2 Whalens Court, Drexel Hill, Garrettford, Upper Darby, Delaware County, Pennsylvania, 19026, United States");
    assert_eq!(address["suburb"], "Drexel Hill");
}

#[test]
fn an_addr_city_no_row_of_its_rank_has_changes_nothing() {
    // Without the tag, with one no place has, and with Drexel Hill farther than twice Addingham's
    // distance from the street, where the street has no row of it: Addingham.
    let addingham = "2 Whalens Court, Addingham, Garrettford, Upper Darby, Delaware County, Pennsylvania, 19026, United States";
    let cases: [(&str, f64, Option<&str>); 3] =
        [("untagged", 500.0, None), ("Lansdowne", 500.0, Some("Lansdowne")), ("far", 900.0, Some("Drexel Hill"))];
    for (case, drexel_m, city) in cases {
        let index = whalens_court(drexel_m, |house| {
            if let Some(city) = city {
                house.addr_city(city);
            }
        });
        let (display, address) = answer_at(&index, 10.0, 0.0);
        assert_eq!(display, addingham, "{case}");
        assert_eq!(address["hamlet"], "Addingham", "{case}");
        assert_eq!(address["suburb"], serde_json::Value::Null, "{case}");
    }

    // The street alone, 150 m from the house, takes no house's addr:city.
    let index = whalens_court(500.0, |house| {
        house.addr_city("Drexel Hill");
    });
    let (_, address) = answer_at(&index, 0.0, 150.0);
    assert_eq!(address["house_number"], serde_json::Value::Null);
    assert_eq!(address["hamlet"], "Addingham");
}

#[test]
fn a_polygon_takes_its_rank_by_its_own_name_or_by_a_place_node_it_links() {
    // Greenburgh, a boundary of rank 16, is the city; the city area across its north-east corner
    // is a row of rank 16 too, which links the city node Yonkers: Nominatim's names of the area
    // hold the node's as `_place_name`. A house named Yonkers, or the area's own name, takes the
    // area; one named after a node it does not link, nothing.
    let index = |city: Option<&str>| {
        let mut index = united_states();
        index
            .boundary("Greenburgh", 8, 0, (-3000.0, -3000.0, 3000.0, 3000.0))
            .boundary("City of Yonkers", 15, 1, (-1000.0, -1000.0, 8000.0, 8000.0))
            .linked_place("Yonkers", 0, 4000.0, 4000.0)
            .boundary("Scarsdale", 15, 1, (20_000.0, 20_000.0, 24_000.0, 24_000.0))
            .linked_place("Edgemont", 0, 22_000.0, 22_000.0)
            .street("Riverdale Avenue", &[(0.0, -200.0), (0.0, 200.0)])
            .address("5", 10.0, 0.0);
        if let Some(city) = city {
            index.addr_city(city);
        }
        index.load()
    };
    for (city, part) in [(None, "Greenburgh"), (Some("Yonkers"), "City of Yonkers"), (Some("City of Yonkers"), "City of Yonkers"),
                         (Some("Edgemont"), "Greenburgh")] {
        let (display, address) = answer_at(&index(city), 10.0, 0.0);
        assert_eq!(display, format!("5 Riverdale Avenue, {part}, United States"), "{city:?}");
        assert_eq!(address["city"], part, "{city:?}");
    }
}

#[test]
fn a_place_node_takes_a_rank_a_polygon_holds() {
    // Bywood, a place=neighbourhood area (rank 24), holds the rank, and the neighbourhood node
    // Kirklyn in it is a row of rank 24 that is no part; the house's addr:city makes it the part.
    let index = |city: Option<&str>| {
        let mut index = united_states();
        index
            .boundary("Upper Darby", 8, 0, (-4000.0, -4000.0, 4000.0, 4000.0))
            .boundary("Bywood", 15, 5, (-600.0, -600.0, 600.0, 600.0))
            .place("Kirklyn", 5, 200.0, 100.0)
            .street("Garrett Road", &[(0.0, -200.0), (0.0, 200.0)])
            .address("7", 10.0, 0.0);
        if let Some(city) = city {
            index.addr_city(city);
        }
        index.load()
    };
    let (display, address) = answer_at(&index(None), 10.0, 0.0);
    assert_eq!(display, "7 Garrett Road, Bywood, Upper Darby, United States");
    assert_eq!(address["neighbourhood"], "Bywood");
    let (display, address) = answer_at(&index(Some("Kirklyn")), 10.0, 0.0);
    assert_eq!(display, "7 Garrett Road, Kirklyn, Upper Darby, United States");
    assert_eq!(address["neighbourhood"], "Kirklyn");
}

/// Main Street in the United States, with house 4 10 m north of it, what `places` adds round it,
/// and the house's `addr:city` where given.
fn main_street(city: Option<&str>, places: impl FnOnce(&mut TestIndex)) -> Index {
    let mut index = united_states();
    places(&mut index);
    index.street("Main Street", &[(0.0, -200.0), (0.0, 200.0)]).address("4", 10.0, 0.0);
    if let Some(city) = city {
        index.addr_city(city);
    }
    index.load()
}

#[test]
fn a_rank_takes_the_farther_of_two_nodes_of_the_value_s_name_whatever_field_the_nearer_fills() {
    // The street's rows of rank 16 are the city node Springfield 100 m north, its address, and
    // the town node Springfield 140 m south, within twice that. Both match the house's
    // addr:city, and Nominatim takes the farther first: the house's town is Springfield, and it
    // has no city.
    let places = |index: &mut TestIndex| {
        index.place("Springfield", 0, 100.0, 0.0).place("Springfield", 1, -140.0, 0.0);
    };
    let (display, address) = answer_at(&main_street(None, places), 10.0, 0.0);
    assert_eq!(display, "4 Main Street, Springfield, United States");
    assert_eq!((&address["city"], &address["town"]), (&serde_json::json!("Springfield"), &serde_json::Value::Null));

    let (display, address) = answer_at(&main_street(Some("Springfield"), places), 10.0, 0.0);
    assert_eq!(display, "4 Main Street, Springfield, United States");
    assert_eq!((&address["city"], &address["town"]), (&serde_json::Value::Null, &serde_json::json!("Springfield")));
}

#[test]
fn of_two_nodes_of_the_value_s_name_at_one_distance_a_rank_takes_the_one_of_the_higher_search_rank() {
    // A city node and a town node Springfield at one point 100 m north of the street: the walk
    // accepts the city, and both are rows of rank 16 at the same distance. Nominatim's last order
    // within a rank is rank_search, descending (results.py:622), the town's 18 before the city's
    // 16: the house's town is Springfield, and it has no city.
    let places = |index: &mut TestIndex| {
        index.place("Springfield", 0, 100.0, 0.0).place("Springfield", 1, 100.0, 0.0);
    };
    let (display, address) = answer_at(&main_street(None, places), 10.0, 0.0);
    assert_eq!(display, "4 Main Street, Springfield, United States");
    assert_eq!((&address["city"], &address["town"]), (&serde_json::json!("Springfield"), &serde_json::Value::Null));

    let (display, address) = answer_at(&main_street(Some("Springfield"), places), 10.0, 0.0);
    assert_eq!(display, "4 Main Street, Springfield, United States");
    assert_eq!((&address["city"], &address["town"]), (&serde_json::Value::Null, &serde_json::json!("Springfield")));
}

#[test]
fn a_field_the_value_s_row_leaves_goes_to_the_finest_part_of_no_field_that_fills_it() {
    // Greenburgh (rank 16) and Hartsdale, a level-9 boundary of rank 18 labelled a city, both fill
    // `city`, which the smaller takes; Greenburgh is a part of no field. The village Elmsford in
    // Hartsdale ranks 18 in Greenburgh's boundary, a row of that rank, and the house's addr:city
    // makes it the rank's part. Nominatim's first `city` label is then Greenburgh's.
    let places = |index: &mut TestIndex| {
        index
            .boundary("Greenburgh", 8, 0, (-3000.0, -3000.0, 3000.0, 3000.0))
            .boundary("Hartsdale", 9, 1, (-1000.0, -1000.0, 1000.0, 1000.0))
            .place("Elmsford", 2, 300.0, 100.0);
    };
    let (display, address) = answer_at(&main_street(None, places), 10.0, 0.0);
    assert_eq!(display, "4 Main Street, Hartsdale, Greenburgh, United States");
    assert_eq!((&address["city"], &address["village"]), (&serde_json::json!("Hartsdale"), &serde_json::Value::Null));

    let (display, address) = answer_at(&main_street(Some("Elmsford"), places), 10.0, 0.0);
    assert_eq!(display, "4 Main Street, Elmsford, Greenburgh, United States");
    assert_eq!((&address["city"], &address["village"]), (&serde_json::json!("Greenburgh"), &serde_json::json!("Elmsford")));
}

#[test]
fn a_boundary_s_every_outer_ring_has_the_names_of_the_node_it_links() {
    // The City of Yonkers is two rings, a place area of rank 16 each: the node Yonkers labels the
    // larger, 25 km north-east, and the house is in the smaller. Nominatim's boundary is one
    // relation, whose names hold Yonkers whichever ring the house is in.
    let places = |index: &mut TestIndex| {
        index
            .boundary("Greenburgh", 8, 0, (-3000.0, -3000.0, 3000.0, 3000.0))
            .boundary("City of Yonkers", 15, 1, (20_000.0, 20_000.0, 30_000.0, 30_000.0))
            .linked_place("Yonkers", 0, 25_000.0, 25_000.0)
            .boundary_ring((-1000.0, -1000.0, 1000.0, 4000.0));
    };
    for (city, part) in [(None, "Greenburgh"), (Some("Yonkers"), "City of Yonkers"), (Some("Edgemont"), "Greenburgh")] {
        let (display, address) = answer_at(&main_street(city, places), 10.0, 0.0);
        assert_eq!(display, format!("4 Main Street, {part}, United States"), "{city:?}");
        assert_eq!(address["city"], part, "{city:?}");
    }
}

#[test]
fn of_two_polygons_of_the_value_s_name_a_rank_takes_the_one_farther_from_the_street_s_middle() {
    // The City of Yonkers, a boundary of rank 16 round Main Street's middle that links the node
    // Yonkers, and Yonkers, a larger place area of rank 16 east of the middle, both hold house 4.
    // getNearFeatures' distance of an area is the street middle's to it: none for the boundary,
    // the street's city, and 100 m for the place area. Both match the house's addr:city, and
    // Nominatim takes the farther, Yonkers.
    let index = |city: Option<&str>| {
        let mut index = united_states();
        index
            .boundary("Yonkers", 15, 1, (-100.0, 100.0, 3000.0, 3000.0))
            .boundary("City of Yonkers", 8, 0, (-500.0, -500.0, 500.0, 500.0))
            .linked_place("Yonkers", 0, 300.0, -300.0)
            .street("Main Street", &[(0.0, -200.0), (0.0, 200.0)])
            .address("4", 10.0, 150.0);
        if let Some(city) = city {
            index.addr_city(city);
        }
        index.load()
    };
    for (city, part) in [(None, "City of Yonkers"), (Some("Yonkers"), "Yonkers")] {
        let (display, address) = answer_at(&index(city), 10.0, 150.0);
        assert_eq!(display, format!("4 Main Street, {part}, United States"), "{city:?}");
        assert_eq!(address["city"], part, "{city:?}");
    }
}

#[test]
fn a_larger_boundary_of_the_level_that_holds_the_house_is_a_row_of_its_rank() {
    // Greenburgh and the larger Mount Pleasant, both of level 8, hold the house: the smaller is
    // the address's city, and Nominatim's street has a row of rank 16 for each.
    let places = |index: &mut TestIndex| {
        index
            .boundary("Greenburgh", 8, 0, (-3000.0, -3000.0, 3000.0, 3000.0))
            .boundary("Mount Pleasant", 8, 0, (-6000.0, -6000.0, 6000.0, 6000.0));
    };
    for (city, part) in [(None, "Greenburgh"), (Some("Mount Pleasant"), "Mount Pleasant"), (Some("Greenburgh"), "Greenburgh")] {
        let (display, address) = answer_at(&main_street(city, places), 10.0, 0.0);
        assert_eq!(display, format!("4 Main Street, {part}, United States"), "{city:?}");
        assert_eq!(address["city"], part, "{city:?}");
    }
}

#[test]
fn a_boundary_that_repeats_its_parent_is_no_row_of_any_rank() {
    // The level 9 Town of Greenburgh repeats the level 8 Greenburgh (the same wikidata and
    // geometry, BOUNDARY_REPEATS_PARENT): is_same ranks it 0, so it is no area of getNearFeatures'
    // (placex_triggers.sql:915-932, :1273) and the street has no row of it. A house whose
    // addr:city names it keeps Greenburgh.
    let places = |index: &mut TestIndex| {
        index
            .boundary("Greenburgh", 8, 0, (-3000.0, -3000.0, 3000.0, 3000.0))
            .boundary("Town of Greenburgh", 9, BOUNDARY_REPEATS_PARENT, (-3000.0, -3000.0, 3000.0, 3000.0));
    };
    for city in [None, Some("Town of Greenburgh")] {
        let (display, address) = answer_at(&main_street(city, places), 10.0, 0.0);
        assert_eq!(display, "4 Main Street, Greenburgh, United States", "{city:?}");
        assert_eq!(address["city"], "Greenburgh", "{city:?}");
    }
}

#[test]
fn a_boundary_the_street_runs_into_is_a_row_of_its_rank() {
    // Saw Mill River Road runs east from Greenburgh into Elmsford, a boundary of the same level;
    // house 9 is in Greenburgh, 100 m from the line. Nominatim's street has a row of rank 16 for
    // each, its middle in Greenburgh: the house's addr:city takes Elmsford. A boundary neither
    // the house nor the street is in is no row.
    let index = |city: Option<&str>| {
        let mut index = united_states();
        index
            .boundary("Greenburgh", 8, 0, (-3000.0, -3000.0, 3000.0, 3000.0))
            .boundary("Elmsford", 8, 0, (-3000.0, 3000.0, 3000.0, 9000.0))
            .boundary("Ardsley", 8, 0, (3000.0, -3000.0, 9000.0, 3000.0))
            .street("Saw Mill River Road", &[(0.0, 1500.0), (0.0, 4000.0)])
            .address("9", 10.0, 2900.0);
        if let Some(city) = city {
            index.addr_city(city);
        }
        index.load()
    };
    for (city, part) in [(None, "Greenburgh"), (Some("Elmsford"), "Elmsford"), (Some("Ardsley"), "Greenburgh")] {
        let (display, address) = answer_at(&index(city), 10.0, 2900.0);
        assert_eq!(display, format!("9 Saw Mill River Road, {part}, United States"), "{city:?}");
        assert_eq!(address["city"], part, "{city:?}");
    }
}

#[test]
fn a_boundary_the_street_runs_along_is_a_row_of_its_rank() {
    // Saw Mill River Road runs along the border of Greenburgh and Elmsford, on nodes the boundary
    // shares, which the street's f32 nodes hold 20 cm off: the intersection of the street and
    // Elmsford is a stretch of the border, more than a point (ST_NPoints > 1), so Nominatim's
    // street has a row of rank 16 for each, and the house's addr:city takes Elmsford. A street
    // that ends on the border meets Elmsford at a point, and is no row of it.
    let index = |city: Option<&str>, street: &[(f64, f64)]| {
        let mut index = united_states();
        index
            .boundary("Greenburgh", 8, 0, (-3000.0, -3000.0, 3000.0, 3000.0))
            .boundary("Elmsford", 8, 0, (-3000.0, 3000.0, 3000.0, 9000.0))
            .street("Saw Mill River Road", street)
            .address("9", 0.0, 2990.0);
        if let Some(city) = city {
            index.addr_city(city);
        }
        index.load()
    };
    let along = [(-1000.0, 2999.8), (0.0, 2999.8), (1000.0, 2999.8)];
    for (city, part) in [(None, "Greenburgh"), (Some("Elmsford"), "Elmsford")] {
        let (display, address) = answer_at(&index(city, &along), 0.0, 2990.0);
        assert_eq!(display, format!("9 Saw Mill River Road, {part}, United States"), "{city:?}");
        assert_eq!(address["city"], part, "{city:?}");
    }
    let (display, address) = answer_at(&index(Some("Elmsford"), &[(0.0, 1500.0), (0.0, 2999.8)]), 0.0, 2990.0);
    assert_eq!(display, "9 Saw Mill River Road, Greenburgh, United States");
    assert_eq!(address["city"], "Greenburgh");
}

#[test]
fn a_boundary_the_street_runs_into_cells_away_from_the_house_is_a_row_of_its_rank() {
    // Saw Mill River Road runs 25 km east from Greenburgh into Elmsford, whose boundary no admin
    // cell of the house's holds; house 9 is in Greenburgh. getNearFeatures takes the street whole,
    // so Nominatim's street has a row of rank 16 for each: the house's addr:city takes Elmsford.
    let index = |city: Option<&str>| {
        let mut index = united_states();
        index
            .boundary("Greenburgh", 8, 0, (-3000.0, -3000.0, 3000.0, 3000.0))
            .boundary("Elmsford", 8, 0, (-3000.0, 25_000.0, 3000.0, 31_000.0))
            .street("Saw Mill River Road", &[(0.0, 1500.0), (0.0, 27_000.0)])
            .address("9", 10.0, 2900.0);
        if let Some(city) = city {
            index.addr_city(city);
        }
        index.load()
    };
    for (city, part) in [(None, "Greenburgh"), (Some("Elmsford"), "Elmsford")] {
        let (display, address) = answer_at(&index(city), 10.0, 2900.0);
        assert_eq!(display, format!("9 Saw Mill River Road, {part}, United States"), "{city:?}");
        assert_eq!(address["city"], part, "{city:?}");
    }
}

#[test]
fn a_polygon_holding_the_house_that_the_street_misses_is_no_row() {
    // House 4 lies in Springfield, of level 8, and Main Street, a few metres south, does not: a
    // house's address rows are its street's (results.py _get_address_lookup_id), the areas the
    // street runs through, so Springfield is none of them. The town node Springfield, in both
    // Springfield and Greenburgh round the street (closed ways, which raise no node), is a row of
    // rank 16: the house's addr:city makes it the rank's part, which leaves no city.
    let places = |index: &mut TestIndex| {
        index
            .boundary("Greenburgh", 8, 0, (-3000.0, -3000.0, 3000.0, 3000.0)).of_way()
            .boundary("Springfield", 8, 0, (5.0, -100.0, 4000.0, 100.0)).of_way()
            .place("Springfield", 1, 100.0, 50.0);
    };
    let (display, address) = answer_at(&main_street(None, places), 10.0, 0.0);
    assert_eq!(display, "4 Main Street, Springfield, United States");
    assert_eq!((&address["city"], &address["town"]), (&serde_json::json!("Springfield"), &serde_json::Value::Null));
    let (display, address) = answer_at(&main_street(Some("Springfield"), places), 10.0, 0.0);
    assert_eq!(display, "4 Main Street, Springfield, United States");
    assert_eq!((&address["city"], &address["town"]), (&serde_json::Value::Null, &serde_json::json!("Springfield")));
}

#[test]
fn a_boundary_the_street_runs_into_ranks_by_its_own_place_type() {
    // Saw Mill River Road runs east from Greenburgh, of level 8 and rank 16, into Fairview, of
    // level 8 too and tagged place=suburb: placex_update gives a boundary that links no node its
    // place tag's rank where that is above its parent's (placex_triggers.sql:1189-1199), 20, not
    // Greenburgh's. The house's addr:city makes Fairview the suburb, and Greenburgh stays.
    let index = |city: Option<&str>| {
        let mut index = united_states();
        index
            .boundary("Greenburgh", 8, 0, (-3000.0, -3000.0, 3000.0, 3000.0))
            .boundary("Fairview", 8, 4, (-3000.0, 3000.0, 3000.0, 9000.0))
            .street("Saw Mill River Road", &[(0.0, 1500.0), (0.0, 4000.0)])
            .address("9", 10.0, 2900.0);
        if let Some(city) = city {
            index.addr_city(city);
        }
        index.load()
    };
    let (display, address) = answer_at(&index(None), 10.0, 2900.0);
    assert_eq!(display, "9 Saw Mill River Road, Greenburgh, United States");
    assert_eq!(address["suburb"], serde_json::Value::Null);
    let (display, address) = answer_at(&index(Some("Fairview")), 10.0, 2900.0);
    assert_eq!(display, "9 Saw Mill River Road, Fairview, Greenburgh, United States");
    assert_eq!((&address["city"], &address["suburb"]), (&serde_json::json!("Greenburgh"), &serde_json::json!("Fairview")));
}

#[test]
fn a_boundary_the_street_runs_into_ranks_under_the_boundaries_round_it() {
    // Saw Mill River Road runs east from Fairview, of level 8 and tagged place=suburb (rank 20),
    // which holds house 9, into Elmsford, of level 9, which only Westchester County (rank 12)
    // holds. placex_update ranks a boundary under the boundary of the highest level below its own
    // round its centroid (placex_triggers.sql:881-906): Elmsford's is the county, so it keeps its
    // level's rank, 18, and Fairview, which does not hold it, does not lift it to 22. The house's
    // addr:city makes Elmsford the city_district, not the neighbourhood.
    let index = |city: Option<&str>| {
        let mut index = united_states();
        index
            .boundary("Westchester County", 6, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
            .boundary("Fairview", 8, 4, (-3000.0, -3000.0, 3000.0, 3000.0))
            .boundary("Elmsford", 9, 0, (-3000.0, 3000.0, 3000.0, 9000.0))
            .street("Saw Mill River Road", &[(0.0, 1500.0), (0.0, 4000.0)])
            .address("9", 10.0, 2900.0);
        if let Some(city) = city {
            index.addr_city(city);
        }
        index.load()
    };
    let (display, address) = answer_at(&index(None), 10.0, 2900.0);
    assert_eq!(display, "9 Saw Mill River Road, Fairview, Westchester County, United States");
    assert_eq!(address["suburb"], "Fairview");
    let (display, address) = answer_at(&index(Some("Elmsford")), 10.0, 2900.0);
    assert_eq!(display, "9 Saw Mill River Road, Fairview, Elmsford, Westchester County, United States");
    assert_eq!((&address["city_district"], &address["neighbourhood"]), (&serde_json::json!("Elmsford"), &serde_json::Value::Null));
}

#[test]
fn a_place_node_a_place_area_of_its_name_and_rank_links_is_no_row() {
    // High Wycombe, a place=town area of rank 16, holds the town node of its name, whose rank is
    // its own: find_linked_place's name match, which runs for ways as well as relations, links the
    // node to the area, and Nominatim's street has no row of it. The house's addr:city names the
    // area, the rank's part already, and the community board of rank 18 stays. Where the area
    // links another node, by its label, the town node is a row of rank 18, in the area's rank 16
    // boundary, and takes the rank; a label of the area's name and type is that one link too.
    let index = |city: Option<&str>, label: Option<&str>| {
        let mut index = united_states();
        index.boundary("High Wycombe", 15, 2, (-3000.0, -3000.0, 3000.0, 3000.0));
        if let Some(label) = label {
            index.linked_place(label, 1, 1500.0, 1500.0);
        }
        index
            .boundary("High Wycombe Community Board", 9, 0, (-2000.0, -2000.0, 2000.0, 2000.0))
            .place("High Wycombe", 1, 500.0, 800.0)
            .street("Oakridge Road", &[(0.0, -200.0), (0.0, 200.0)])
            .address("81", 10.0, 0.0);
        if let Some(city) = city {
            index.addr_city(city);
        }
        index.load()
    };
    let board = "81 Oakridge Road, High Wycombe Community Board, High Wycombe, United States";
    for city in [None, Some("High Wycombe")] {
        let (display, address) = answer_at(&index(city, None), 10.0, 0.0);
        assert_eq!(display, board, "{city:?}");
        assert_eq!(address["town"], "High Wycombe", "{city:?}");
    }
    for label in ["Wycombe", "High Wycombe"] {
        let (display, _) = answer_at(&index(None, Some(label)), 10.0, 0.0);
        assert_eq!(display, board, "{label}");
        let (display, address) = answer_at(&index(Some("High Wycombe"), Some(label)), 10.0, 0.0);
        assert_eq!(display, "81 Oakridge Road, High Wycombe, United States", "{label}");
        assert_eq!(address["town"], "High Wycombe", "{label}");
    }
}

#[test]
fn a_place_area_an_area_round_it_lifts_links_no_place_node_of_its_name() {
    // Newton Aycliffe, a place=town area, lies in the parish of Great Aycliffe (rank 20), which
    // lifts it to rank 22. find_linked_place reads the area's rank_address, no longer the town
    // node's, so the name match links no node, and the town node of its name, in Durham's rank 16
    // boundary, is a row of rank 18. The house's addr:city makes the node the rank's part, after
    // the parish: Nominatim repeats Newton Aycliffe.
    let index = |city: Option<&str>| {
        let mut index = united_states();
        index
            .boundary("Durham", 8, 0, (-10_000.0, -10_000.0, 10_000.0, 10_000.0))
            .boundary("Great Aycliffe", 10, 0, (-4000.0, -4000.0, 4000.0, 4000.0))
            .boundary("Newton Aycliffe", 15, 2, (-3000.0, -3000.0, 3000.0, 3000.0))
            .place("Newton Aycliffe", 1, 500.0, 800.0)
            .street("Stanfield Road", &[(0.0, -200.0), (0.0, 200.0)])
            .address("20", 10.0, 0.0);
        if let Some(city) = city {
            index.addr_city(city);
        }
        index.load()
    };
    let (display, _) = answer_at(&index(None), 10.0, 0.0);
    assert_eq!(display, "20 Stanfield Road, Newton Aycliffe, Great Aycliffe, Durham, United States");
    let (display, address) = answer_at(&index(Some("Newton Aycliffe")), 10.0, 0.0);
    assert_eq!(display, "20 Stanfield Road, Newton Aycliffe, Great Aycliffe, Newton Aycliffe, Durham, United States");
    assert_eq!(address["town"], "Newton Aycliffe");
}
