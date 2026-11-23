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
