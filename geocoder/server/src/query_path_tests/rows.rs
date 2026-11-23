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

#[test]
fn of_two_areas_holding_the_point_at_a_rank_the_one_the_walk_does_not_accept_shows() {
    // 40 Mespil Road, Dublin (N6089052494): County Dublin (admin_level 6) and the postal district
    // Dublin 4, a place area of rank 12, both hold the house and the middle of Mespil Road.
    // getNearFeatures' distance (partition-functions.sql:32-35) is then a hundred-thousandth of
    // the street's to each area's centre, Dublin 4's the smaller: the address walk accepts it
    // (isaddress, score 0), and County Dublin, holding the house's centroid, scores 1 and is the
    // rank's row (results.py:611-632). Dublin 4 is no part.
    let mut index = country("Ireland", b"IE");
    index
        .boundary("Leinster", 5, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("County Dublin", 6, 0, (-10_000.0, -20_000.0, 30_000.0, 20_000.0))
        .boundary("Dublin 4", 15, 11, (-1000.0, -3000.0, 5000.0, 3000.0))
        .boundary("Dublin", 7, 1, (-5000.0, -5000.0, 5000.0, 5000.0))
        .boundary("Pembroke West C ED", 9, 0, (-500.0, -500.0, 500.0, 500.0))
        .street("Mespil Road", &[(0.0, -200.0), (0.0, 200.0)])
        .address("40", 10.0, 0.0);
    let (display, address) = answer_at(&index.load(), 10.0, 0.0);
    assert_eq!(display, "40 Mespil Road, Pembroke West C ED, Dublin, County Dublin, Leinster, Ireland");
    assert_eq!((&address["county"], &address["district"]), (&serde_json::json!("County Dublin"), &serde_json::Value::Null));

    // Donegall Square North, Belfast (W1327257813): Belfast City District (admin_level 7, linked
    // city) and the County Borough of Belfast (R14353071, boundary=historic, place=city: a place
    // area) both rank 16; the district's centre is the nearer the street, so the borough is the
    // city.
    let mut index = country("United Kingdom", b"GB");
    index
        .boundary("Northern Ireland", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("County Antrim", 15, 10, (-30_000.0, -30_000.0, 30_000.0, 30_000.0))
        .boundary("Belfast City District", 7, 1, (-3000.0, -3000.0, 3000.0, 3000.0))
        .boundary("County Borough of Belfast", 15, 1, (-2500.0, -2000.0, 4300.0, 3500.0))
        .street("Donegall Square North", &[(0.0, -100.0), (0.0, 100.0)]);
    let (display, address) = answer_at(&index.load(), 0.0, 0.0);
    assert_eq!(display, "Donegall Square North, County Borough of Belfast, County Antrim, Northern Ireland, United Kingdom");
    assert_eq!(address["city"], "County Borough of Belfast");
}

#[test]
fn the_walk_takes_the_area_nearer_the_street_first_whichever_kind_it_is() {
    // 96 Los Troncos, Montevideo (N7176348110): Conciliación, a place=suburb area, and Colón
    // (R8504934, boundary=civil, here admin_level 10) both rank 20 and hold the house; neither
    // lies within the other. Conciliación's centre is the nearer the street, so the walk accepts
    // it and Colón is the suburb, as Nominatim answers. Where Colón's centre is the nearer, the
    // walk accepts Colón and the suburb is Conciliación.
    let index = |colon: Span| {
        let mut index = country("Uruguay", b"UY");
        index
            .boundary("Montevideo", 4, 0, (-30_000.0, -30_000.0, 30_000.0, 30_000.0))
            .boundary("Montevideo", 8, 0, (-10_000.0, -10_000.0, 10_000.0, 10_000.0))
            .boundary("Colón", 10, 0, colon)
            .boundary("Conciliación", 15, 4, (-500.0, -2600.0, 1500.0, 800.0))
            .street("Los Troncos", &[(0.0, -200.0), (0.0, 200.0)])
            .address("96", 10.0, 0.0);
        index.load()
    };
    for (colon, suburb) in [((-3000.0, -2000.0, 6000.0, 3000.0), "Colón"), ((-1500.0, -1200.0, 1500.0, 2500.0), "Conciliación")] {
        let (display, address) = answer_at(&index(colon), 10.0, 0.0);
        assert_eq!(display, format!("Los Troncos 96, {suburb}, Montevideo, Uruguay"));
        assert_eq!(address["suburb"], suburb);
    }
}

#[test]
fn an_area_the_street_crosses_without_holding_the_point_is_no_part() {
    // 1037 Nordyke Road, Anderson Township, Ohio (W656273805): the road runs east into Union
    // Township and Clermont County, rows of the street at ranks 14 and 12 that hold neither the
    // house nor the street's middle. Nominatim's parts are the township and the county the house
    // is in.
    let mut index = country("United States", b"US");
    index
        .boundary("Ohio", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Hamilton County", 6, 0, (-20_000.0, -20_000.0, 20_000.0, 3000.0))
        .boundary("Clermont County", 6, 0, (-20_000.0, 3000.0, 20_000.0, 20_000.0))
        .boundary("Anderson Township", 7, 0, (-4000.0, -4000.0, 4000.0, 3000.0))
        .boundary("Union Township", 7, 0, (-4000.0, 3000.0, 4000.0, 9000.0))
        .street("Nordyke Road", &[(0.0, -200.0), (0.0, 3500.0)])
        .address("1037", 10.0, 0.0)
        .addr_postcode("45255");
    let (display, address) = answer_at(&index.load(), 10.0, 0.0);
    assert_eq!(display, "1037 Nordyke Road, Anderson Township, Hamilton County, Ohio, 45255, United States");
    assert_eq!((&address["municipality"], &address["county"]), (&serde_json::json!("Anderson Township"), &serde_json::json!("Hamilton County")));
}
