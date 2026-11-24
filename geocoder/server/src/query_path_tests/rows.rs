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

#[test]
fn a_rank_whose_place_nodes_all_fail_the_fuzzy_area_shows_the_farthest() {
    // 40 Nislevvej, Otterup (N966597217): the walk accepts the village Otterup at 16, and the
    // four hamlets of rank 20 lie outside its fuzzy area. insert_addresslines writes each a row
    // all the same (placex_triggers.sql:626-669), and at query time they all score -1 and are no
    // area, so `distance desc` puts the farthest first (results.py:611-632): Stormose. The
    // neighbourhood Nislev, in Otterup's fuzzy area, is accepted at 24.
    let mut index = country("Danmark", b"DK");
    index
        .boundary("Region Syddanmark", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Nordfyns Kommune", 7, 0, (-15_000.0, -15_000.0, 15_000.0, 15_000.0))
        .place("Otterup", 2, 1900.0, 0.0)
        .place("Bladstrup", 4, -300.0, 100.0)
        .place("Skelmade Huse", 4, -450.0, -150.0)
        .place("Bjerrumhuse", 4, -600.0, 200.0)
        .place("Stormose", 4, -900.0, 50.0)
        .place("Nislev", 5, 150.0, -100.0)
        .street("Nislevvej", &[(0.0, -200.0), (0.0, 200.0)])
        .address("40", 10.0, 0.0)
        .addr_postcode("5450");
    let (display, address) = answer_at(&index.load(), 10.0, 0.0);
    assert_eq!(display, "Nislevvej 40, Nislev, Stormose, Otterup, Nordfyns Kommune, Region Syddanmark, 5450, Danmark");
    assert_eq!((&address["hamlet"], &address["village"]), (&serde_json::json!("Stormose"), &serde_json::json!("Otterup")));

    // 141 Kaufman Road, West Deer Township (W556338845): the village Bairdford ranks 18 inside
    // the township and is accepted; the hamlet Curtisville, outside its fuzzy area, is the one
    // row of rank 20, and shows.
    let mut index = country("United States", b"US");
    index
        .boundary("Pennsylvania", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Allegheny County", 6, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("West Deer Township", 8, 2, (-6000.0, -6000.0, 6000.0, 6000.0))
        .place("Bairdford", 2, 1500.0, 0.0)
        .place("Curtisville", 4, -800.0, 0.0)
        .street("Kaufman Road", &[(0.0, -200.0), (0.0, 200.0)])
        .address("141", 10.0, 0.0)
        .addr_postcode("15044");
    let (display, address) = answer_at(&index.load(), 10.0, 0.0);
    assert_eq!(display,
               "141 Kaufman Road, Curtisville, Bairdford, West Deer Township, Allegheny County, Pennsylvania, 15044, United States");
    assert_eq!(address["hamlet"], "Curtisville");
}

#[test]
fn the_farthest_node_shows_only_at_a_rank_where_the_walk_accepts_none() {
    // 23 Ngaio Road, Titirangi (N5415799197): the walk accepts the suburb Titirangi, the nearer of
    // two at rank 20, which shows; the quarters Waimā and Woodlands Park of rank 22 lie outside
    // its fuzzy area, and the farther, Woodlands Park, shows.
    let mut index = country("New Zealand / Aotearoa", b"NZ");
    index
        .boundary("Auckland", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Waitākere Ranges", 8, 0, (-10_000.0, -10_000.0, 10_000.0, 10_000.0))
        .place("Titirangi", 3, 1500.0, 0.0)
        .place("Titirangi Beach", 3, 1800.0, 100.0)
        .place("Waimā", 6, -600.0, 0.0)
        .place("Woodlands Park", 6, -950.0, 100.0)
        .street("Ngaio Road", &[(0.0, -200.0), (0.0, 200.0)])
        .address("23", 10.0, 0.0)
        .addr_postcode("0642");
    let (display, address) = answer_at(&index.load(), 10.0, 0.0);
    assert_eq!(display, "23 Ngaio Road, Woodlands Park, Titirangi, Waitākere Ranges, Auckland, 0642, New Zealand / Aotearoa");
    assert_eq!((&address["quarter"], &address["suburb"]), (&serde_json::json!("Woodlands Park"), &serde_json::json!("Titirangi")));

    // 1016 Lindsay Road, Scott Township (N12968620169): the hamlet Bower Hill is accepted at 20;
    // the neighbourhoods Wynnecliffe Manor and Saint Clair Heights, outside its fuzzy area, are
    // rows of rank 24, Saint Clair Heights 28 times the farther in Nominatim's data, and it shows.
    let mut index = country("United States", b"US");
    index
        .boundary("Pennsylvania", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Allegheny County", 6, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Scott Township", 8, 0, (-6000.0, -6000.0, 6000.0, 6000.0))
        .place("Bower Hill", 4, 800.0, 0.0)
        .place("Wynnecliffe Manor", 5, -250.0, 0.0)
        .place("Saint Clair Heights", 5, -450.0, 100.0)
        .street("Lindsay Road", &[(0.0, -200.0), (0.0, 200.0)])
        .address("1016", 10.0, 0.0)
        .addr_postcode("15106");
    let (display, address) = answer_at(&index.load(), 10.0, 0.0);
    assert_eq!(display,
               "1016 Lindsay Road, Saint Clair Heights, Bower Hill, Scott Township, Allegheny County, Pennsylvania, 15106, United States");
    assert_eq!(address["neighbourhood"], "Saint Clair Heights");

    // 275 Linnaeusstraat, Den Haag (N2709728409): the quarter Binckhorst is accepted at 22 after
    // the suburb area Laak (20); Laakkwartier-Oost and Laakkwartier-West lie outside its fuzzy
    // area at 24, and the farther, Laakkwartier-West, shows.
    let mut index = country("Nederland", b"NL");
    index
        .boundary("Nederland", 3, 0, (-50_000.0, -50_000.0, 50_000.0, 50_000.0))
        .boundary("Zuid-Holland", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Den Haag", 8, 0, (-10_000.0, -10_000.0, 10_000.0, 10_000.0))
        .boundary("Den Haag", 10, 0, (-8000.0, -8000.0, 8000.0, 8000.0))
        .boundary("Laak", 15, 4, (-2000.0, -2000.0, 2000.0, 2000.0))
        .place("Binckhorst", 6, 900.0, 0.0)
        .place("Laakkwartier-Oost", 5, -220.0, 0.0)
        .place("Laakkwartier-West", 5, -480.0, 50.0)
        .street("Linnaeusstraat", &[(0.0, -200.0), (0.0, 200.0)])
        .address("275", 10.0, 0.0)
        .addr_postcode("2522 GN");
    let (display, address) = answer_at(&index.load(), 10.0, 0.0);
    assert_eq!(display, "Linnaeusstraat 275, Laakkwartier-West, Binckhorst, Laak, Den Haag, Zuid-Holland, Nederland, 2522 GN, Nederland");
    assert_eq!((&address["neighbourhood"], &address["quarter"]), (&serde_json::json!("Laakkwartier-West"), &serde_json::json!("Binckhorst")));
}

#[test]
fn a_house_s_addr_city_takes_its_rank_from_the_farthest_node() {
    // 9c Stamsrieder Weg, Wilsdruff (N2034299740): the village Kaufbach ranks 18 inside Wilsdruff
    // and is accepted; the suburbs Kesselsdorf, Oberhermsdorf and Braunsdorf of rank 20 lie
    // outside its fuzzy area, so the street alone shows the farthest, Braunsdorf. The house names
    // Kesselsdorf, and the crosscheck puts that row first in its rank (results.py:610-632), as
    // Nominatim answers.
    let index = |city: Option<&str>| {
        let mut index = country("Deutschland", b"DE");
        index
            .boundary("Sachsen", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
            .boundary("Sächsische Schweiz-Osterzgebirge", 6, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
            .boundary("Wilsdruff", 8, 2, (-6000.0, -6000.0, 6000.0, 6000.0))
            .place("Kaufbach", 2, 1800.0, 0.0)
            .place("Kesselsdorf", 3, -400.0, 0.0)
            .place("Oberhermsdorf", 3, -700.0, 100.0)
            .place("Braunsdorf", 3, -1300.0, -100.0)
            .street("Stamsrieder Weg", &[(0.0, -200.0), (0.0, 200.0)])
            .address("9c", 10.0, 0.0)
            .addr_postcode("01723");
        if let Some(city) = city {
            index.addr_city(city);
        }
        index.load()
    };
    for (city, suburb) in [(None, "Braunsdorf"), (Some("Kesselsdorf"), "Kesselsdorf")] {
        let (display, address) = answer_at(&index(city), 10.0, 0.0);
        assert_eq!(display,
                   format!("Stamsrieder Weg 9c, {suburb}, Kaufbach, Wilsdruff, Sächsische Schweiz-Osterzgebirge, Sachsen, 01723, Deutschland"),
                   "{city:?}");
        assert_eq!(address["suburb"], suburb, "{city:?}");
    }
}

#[test]
fn a_node_outside_its_boundary_or_its_fuzzy_area_s_reach_or_at_a_rank_an_area_holds_fills_nothing() {
    // 1600 Pennsylvania Avenue Northwest (R19761182): Nominatim's street has rows at 18 (Ward 2),
    // 16 (Washington, a place=city area) and 8 only. The West End's fuzzy area does not reach the
    // street, and Foggy Bottom's does, but it lies outside Ward 2, the boundary of rank 24's
    // nodes: neither is a row.
    let mut index = country("United States", b"US");
    index
        .boundary("District of Columbia", 4, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Washington", 15, 1, (-10_000.0, -10_000.0, 10_000.0, 10_000.0))
        .boundary("Ward 2", 9, 12, (-1000.0, -400.0, 1000.0, 1000.0))
        .place("The West End", 5, 800.0, -300.0)
        .place("Foggy Bottom", 5, 100.0, -600.0)
        .street("Pennsylvania Avenue Northwest", &[(0.0, -150.0), (0.0, 150.0)])
        .address("1600", 10.0, 0.0)
        .addr_postcode("20500");
    let (display, address) = answer_at(&index.load(), 10.0, 0.0);
    assert_eq!(display, "1600 Pennsylvania Avenue Northwest, Ward 2, Washington, District of Columbia, 20500, United States");
    assert_eq!(address["neighbourhood"], serde_json::Value::Null);

    // South Van Ness Avenue, San Francisco (N10555180060): the place=neighbourhood area South of
    // Market holds rank 24, so Hayes Valley, a neighbourhood node outside it, is no row.
    let mut index = country("United States", b"US");
    index
        .boundary("California", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("San Francisco", 6, 1, (-10_000.0, -10_000.0, 10_000.0, 10_000.0))
        .boundary("South of Market", 15, 5, (-300.0, -300.0, 2000.0, 2000.0))
        .place("Mission", 3, 1500.0, 0.0)
        .place("Civic Center", 6, -400.0, 300.0)
        .place("Hayes Valley", 5, 300.0, -400.0)
        .street("South Van Ness Avenue", &[(0.0, -200.0), (0.0, 200.0)]);
    let (display, address) = answer_at(&index.load(), 0.0, 0.0);
    assert_eq!(display, "South Van Ness Avenue, South of Market, Civic Center, Mission, San Francisco, California, United States");
    assert_eq!(address["neighbourhood"], "South of Market");

    // Kartavya Path, New Delhi (W1307222666): Nominatim answers the suburb Raisina Hill.
    let mut index = country("India", b"IN");
    index
        .boundary("Delhi", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("New Delhi", 5, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("New Delhi", 8, 0, (-8000.0, -8000.0, 8000.0, 8000.0))
        .place("Raisina Hill", 3, 400.0, 200.0)
        .street("Kartavya Path", &[(0.0, -200.0), (0.0, 200.0)]);
    let (display, address) = answer_at(&index.load(), 0.0, 0.0);
    assert_eq!(display, "Kartavya Path, Raisina Hill, New Delhi, Delhi, India");
    assert_eq!(address["suburb"], "Raisina Hill");
}

#[test]
fn a_boundary_repeating_the_outline_of_the_one_before_it_holds_no_rank() {
    // Bruxelles - Brussel (admin_level 9) repeats the outline of Brussels (8, linked city: 16), and
    // Nominatim takes it out of addresses (placex_triggers.sql:895-899), so rank 18, which its
    // level would lift it to under the city, stays free. A town node inside the city ranks 18
    // (placex_triggers.sql:951-971) and is the rank's row.
    let mut index = country("Belgium", b"BE");
    index
        .boundary("Brussels-Capital", 4, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Brussels", 8, 1, (-5000.0, -5000.0, 5000.0, 5000.0))
        .boundary("Bruxelles - Brussel", 9, 1, (-5000.0, -5000.0, 5000.0, 5000.0))
        .place("Laeken", 1, 1500.0, 0.0)
        .street("Rue Melsens - Melsensstraat", &[(0.0, -100.0), (0.0, 100.0)]);
    let (display, address) = answer_at(&index.load(), 0.0, 0.0);
    assert_eq!(display, "Rue Melsens - Melsensstraat, Laeken, Brussels, Brussels-Capital, Belgium");
    assert_eq!((&address["town"], &address["city"]), (&serde_json::json!("Laeken"), &serde_json::json!("Brussels")));
}

#[test]
fn the_walk_weighs_the_distance_of_a_rank_16_area_by_its_search_rank() {
    // Malone Road, Belfast (W10288833): Belfast City District (admin_level 7, linked city: rank 16,
    // rank_search 14) and the County Borough of Belfast (a place=city area, rank_search 16) both
    // hold the street's middle, and the district's centre is the nearer the street. The walk
    // weighs the distance of a rank-16 area of rank_search 16 by a quarter (placex_triggers.sql:
    // 597-605), so it takes the borough first and accepts it, and the district, holding the
    // house, is the rank's row.
    let mut index = country("United Kingdom", b"GB");
    index
        .boundary("Northern Ireland", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("County Antrim", 15, 10, (-30_000.0, -30_000.0, 30_000.0, 30_000.0))
        .boundary("Belfast City District", 7, 1, (-3000.0, -3000.0, 5000.0, 3000.0))
        .boundary("County Borough of Belfast", 15, 1, (-3000.0, -2000.0, 6000.0, 3500.0))
        .street("Malone Road", &[(0.0, -100.0), (0.0, 100.0)]);
    let (display, address) = answer_at(&index.load(), 0.0, 0.0);
    assert_eq!(display, "Malone Road, Belfast City District, County Antrim, Northern Ireland, United Kingdom");
    assert_eq!(address["city"], "Belfast City District");
}

#[test]
fn an_area_the_street_does_not_run_through_is_no_row_of_a_rank_it_shares() {
    // Two areas of rank 20 hold the point, 30 m off the street: the boundary Westend, which the
    // street runs through, and the suburb area Eastend, which ends 10 m short of it.
    // getNearFeatures takes an area for a street only where the two share a stretch
    // (partition-functions.sql:39-44), so Eastend is no row of the street, and Westend is the
    // rank's.
    let mut index = country("Testland", b"XX");
    index
        .boundary("Westend", 10, 0, (-1000.0, -1000.0, 1000.0, 1000.0))
        .boundary("Eastend", 15, 4, (10.0, -500.0, 1500.0, 1500.0))
        .street("Border Street", &[(0.0, -200.0), (0.0, 200.0)]);
    let (display, address) = answer_at(&index.load(), 30.0, 0.0);
    assert_eq!(display, "Border Street, Westend, Testland");
    assert_eq!(address["suburb"], "Westend");
}

#[test]
fn a_place_area_the_walk_accepts_is_the_boundary_of_finer_place_nodes() {
    // The place=city area Metro holds the street, and the walk accepts it at rank 16: it becomes
    // the current boundary (placex_triggers.sql:645-652), so the suburb node Outskirts, outside it
    // though its fuzzy area reaches the street, is no row (:606-609).
    let mut index = country("Testland", b"XX");
    index
        .boundary("Shire", 6, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Metro", 15, 1, (-1000.0, -1000.0, 1000.0, 1000.0))
        .place("Outskirts", 3, 1500.0, 0.0)
        .street("High Street", &[(0.0, -200.0), (0.0, 200.0)]);
    let (display, address) = answer_at(&index.load(), 10.0, 0.0);
    assert_eq!(display, "High Street, Metro, Shire, Testland");
    assert_eq!(address["suburb"], serde_json::Value::Null);
}

#[test]
fn an_area_the_walk_does_not_accept_leaves_the_place_nodes_fuzzy_area_in_force() {
    // Longreach (admin_level 10, rank 20) holds the point but is larger than the county Shire
    // round it, and its centre lies outside Shire, so the walk does not accept it
    // (placex_triggers.sql:629-632). It shows all the same, as an area holding the result's
    // centroid, but only an accepted area clears the last accepted node's fuzzy area (:645-652):
    // the quarters Nearfield and Farfield of rank 22, outside the fuzzy area of the town
    // Thornbury, are both turned down, and the farther shows.
    let mut index = country("Testland", b"XX");
    index
        .boundary("Wessex", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Shire", 6, 0, (-5000.0, -5000.0, 5000.0, 5000.0))
        .boundary("Longreach", 10, 0, (-2000.0, -3000.0, 2000.0, 35_000.0))
        .place("Thornbury", 1, 3500.0, 0.0)
        .place("Nearfield", 6, -600.0, 0.0)
        .place("Farfield", 6, -900.0, 50.0)
        .street("High Street", &[(0.0, -200.0), (0.0, 200.0)]);
    let (display, address) = answer_at(&index.load(), 10.0, 0.0);
    assert_eq!(display, "High Street, Farfield, Longreach, Thornbury, Shire, Wessex, Testland");
    assert_eq!((&address["quarter"], &address["suburb"]), (&serde_json::json!("Farfield"), &serde_json::json!("Longreach")));
}
