//! The rows of an address as Nominatim's complete_address_details shows them (results.py:610-632):
//! the first row of each rank is a part of display_name, and the address keeps the first row of
//! each label, finest first (v1/format_json.py:29-34), a place node's or a polygon's.

use super::*;

/// `name`, a country (rank 4) 100 km across around the origin, under the country code `code`.
fn country(name: &str, code: &[u8; 2]) -> TestIndex {
    let mut index = TestIndex { origin: ORIGIN, ..TestIndex::default() };
    index.boundary(name, 2, 0, (-50_000.0, -50_000.0, 50_000.0, 50_000.0));
    index.polygons[0].0.country_code = u16::from_be_bytes(*code);
    index
}

/// The answer `north_m` north and `east_m` east of the origin: its display_name and its address.
fn answer_at(index: &Index, north_m: f64, east_m: f64) -> (String, serde_json::Value) {
    let p = at(north_m, east_m);
    let answer = serde_json::to_value(index.query(p.lat as f64, p.lng as f64)).unwrap();
    (answer["display_name"].as_str().unwrap_or_default().to_owned(), answer["address"].clone())
}

#[test]
fn a_place_node_finer_than_the_boundary_of_its_label_takes_the_label() {
    // 416 Wynstone Drive, Wexford, Pennsylvania (W165735466): the town node Wexford ranks 18
    // inside Pine Township, a boundary of rank 16 labelled town. Nominatim's town is Wexford, and
    // its display_name lists Pine Township after it.
    let mut index = country("United States", b"US");
    index
        .boundary("Pennsylvania", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Allegheny County", 6, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Pine Township", 8, 2, (-6000.0, -6000.0, 6000.0, 6000.0))
        .place("Wexford", 1, 2000.0, -1500.0)
        .street("Wynstone Drive", &[(0.0, -200.0), (0.0, 200.0)])
        .address("416", 10.0, 0.0)
        .addr_postcode("15090");
    let (display, address) = answer_at(&index.load(), 10.0, 0.0);
    assert_eq!(display, "416 Wynstone Drive, Wexford, Pine Township, Allegheny County, Pennsylvania, 15090, United States");
    assert_eq!(address["town"], "Wexford");

    // 13 Lexington Avenue, Brentwood, New York (W469252114): the town node Brentwood ranks 18 in
    // the Town of Islip, a town of rank 16, and the hamlet East Brentwood 20. Nominatim's town is
    // Brentwood; the Town of Islip is in display_name only.
    let mut index = country("United States", b"US");
    index
        .boundary("New York", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Suffolk County", 6, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Town of Islip", 8, 2, (-8000.0, -8000.0, 8000.0, 8000.0))
        .place("Brentwood", 1, 1500.0, -1000.0)
        .place("East Brentwood", 4, 500.0, 300.0)
        .street("Lexington Avenue", &[(0.0, -200.0), (0.0, 200.0)])
        .address("13", 10.0, 0.0)
        .addr_postcode("11717");
    let (display, address) = answer_at(&index.load(), 10.0, 0.0);
    assert_eq!(display,
               "13 Lexington Avenue, East Brentwood, Brentwood, Town of Islip, Suffolk County, New York, 11717, United States");
    assert_eq!((&address["hamlet"], &address["town"]), (&serde_json::json!("East Brentwood"), &serde_json::json!("Brentwood")));
}

#[test]
fn a_boundary_finer_than_a_place_node_of_its_label_takes_the_label() {
    // Hatch Lane, Vernhams Dean, Hampshire (W48862779): the parish of Vernhams Dean (R3889403,
    // admin_level 10, a linked village) ranks 20, the village node Vernham Street 18 inside Test
    // Valley (rank 16), and the hamlet Vernham Row 22 inside the parish. Nominatim's village is
    // the finer row, Vernhams Dean, and Vernham Street is a part of display_name.
    let mut index = country("United Kingdom", b"GB");
    index
        .boundary("England", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Hampshire", 6, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Test Valley", 8, 0, (-8000.0, -8000.0, 8000.0, 8000.0))
        .boundary("Vernhams Dean", 10, 3, (-3000.0, -3000.0, 3000.0, 3000.0))
        .place("Vernham Street", 2, -1200.0, 800.0)
        .place("Vernham Row", 4, 600.0, -300.0)
        .street("Hatch Lane", &[(0.0, -200.0), (0.0, 200.0)])
        .centroid("SP11 0LB", 50.0, 0.0);
    let (display, address) = answer_at(&index.load(), 0.0, 0.0);
    assert_eq!(display,
               "Hatch Lane, Vernham Row, Vernhams Dean, Vernham Street, Test Valley, Hampshire, England, SP11 0LB, United Kingdom");
    assert_eq!((&address["hamlet"], &address["village"]), (&serde_json::json!("Vernham Row"), &serde_json::json!("Vernhams Dean")));
}
