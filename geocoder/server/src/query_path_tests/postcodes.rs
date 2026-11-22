//! The postcode of an answer without one of its own: Nominatim 5.3's get_nearest_postcode on a
//! point (utils.sql:168-178), the postcode area covering it, else the nearest postcode centroid
//! whose box covers it, the box reaching the country's postcode extent from the centroid
//! (settings/country_settings.yaml, postcodes.py).

use super::*;

/// Testland around `origin` under the country code `code`.
fn country_at(origin: (f64, f64), code: &[u8; 2]) -> TestIndex {
    let mut index = TestIndex::around(origin);
    index.polygons[0].0.country_code = u16::from_be_bytes(*code);
    index
}

/// The postcode of the answer `north_m` north and `east_m` east of `origin`.
fn postcode_from(index: &Index, origin: (f64, f64), north_m: f64, east_m: f64) -> serde_json::Value {
    address_from(index, origin, north_m, east_m)["postcode"].clone()
}

/// (metres north, metres east) of the point (lat, lng) from `origin`, as at_from places them.
fn offset_m((lat0, lng0): (f64, f64), (lat, lng): (f64, f64)) -> (f64, f64) {
    ((lat - lat0) * METRES_PER_DEGREE, (lng - lng0) * METRES_PER_DEGREE * lat0.to_radians().cos())
}

#[test]
fn a_postal_area_round_the_point_names_it_before_a_nearer_centroid() {
    // Berlin-Mitte: the boundary=postal_code area 10178 holds the point, and the centroid of
    // 10179 lies nearer than 10178's own. Nominatim takes a postcode area covering the point
    // before any centroid (ORDER BY osm_id is null).
    let berlin = (52.52, 13.405);
    let index = country_at(berlin, b"DE")
        .boundary("10178", POSTCODE_AREA_LEVEL, 0, (-800.0, -800.0, 800.0, 800.0))
        .boundary("10179", POSTCODE_AREA_LEVEL, 0, (-800.0, 800.0, 800.0, 2400.0))
        .centroid("10179", 0.0, 900.0)
        .centroid("10178", 0.0, -1000.0)
        .load();
    assert_eq!(postcode_from(&index, berlin, 0.0, 300.0), "10178");
    assert_eq!(postcode_from(&index, berlin, 0.0, 1000.0), "10179");
}

#[test]
fn of_postal_areas_round_the_point_the_smallest_names_it() {
    // A commune's postal area inside a larger one of its department's code: the smaller names the
    // point. In Ille-et-Vilaine the centroid of 53540, across the departments' border, lies nearer
    // than any of 35370, and Nominatim names the area round the point, 35370.
    let rennes = (48.02, -1.18);
    let index = country_at(rennes, b"FR")
        .boundary("35000", POSTCODE_AREA_LEVEL, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("35370", POSTCODE_AREA_LEVEL, 0, (-4000.0, -4000.0, 4000.0, 4000.0))
        .centroid("53540", 0.0, 7000.0)
        .load();
    assert_eq!(postcode_from(&index, rennes, 0.0, 3000.0), "35370");
    assert_eq!(postcode_from(&index, rennes, 0.0, 6000.0), "35000");
}

#[test]
fn a_centroid_names_a_point_only_inside_its_countrys_extent() {
    // Great Britain's postcode extent is 700 m, the Netherlands' 800 m, the United States' 25 km
    // and France's (the default) 5 km: a centroid's box reaches that far north, south, east and
    // west of it (expand_by_meters).
    for (code, postcode, origin, inside_m, outside_m) in [
        (b"GB", "SW1A 2AA", (51.5, -0.12), 650.0, 750.0),
        (b"NL", "1012 AB", (52.37, 4.9), 750.0, 850.0),
        (b"US", "19073", (40.0, -75.4), 24_000.0, 26_000.0),
        (b"FR", "75004", (48.85, 2.35), 4900.0, 5100.0),
    ] {
        let index = country_at(origin, code).centroid(postcode, 0.0, 0.0).load();
        let name = String::from_utf8_lossy(code);
        assert_eq!(postcode_from(&index, origin, inside_m, 0.0), postcode, "{name} north");
        assert_eq!(postcode_from(&index, origin, 0.0, inside_m), postcode, "{name} east");
        assert_eq!(postcode_from(&index, origin, outside_m, 0.0), serde_json::Value::Null, "{name} north");
        assert_eq!(postcode_from(&index, origin, 0.0, -outside_m), serde_json::Value::Null, "{name} west");
    }
}

#[test]
fn the_nearest_centroid_is_the_nearest_in_plain_degrees() {
    // Nominatim orders the centroids covering the point by ST_Distance on lon/lat geometry, in
    // plain degrees. At 52° N a centroid 0.006° north (668 m) is nearer than one 0.009° east
    // (617 m); both boxes (Netherlands: 800 m) hold the point.
    let amsterdam = (52.0, 4.9);
    let index = country_at(amsterdam, b"NL")
        .centroid("1011 AB", 0.006 * METRES_PER_DEGREE, 0.0)
        .centroid("1012 CD", 0.0, 0.009 * METRES_PER_DEGREE * 52.0_f64.to_radians().cos())
        .load();
    assert_eq!(postcode_from(&index, amsterdam, 0.0, 0.0), "1011 AB");

    // Newtown Township, Delaware County (40.0033, -75.40011): no postal area, so the nearest of
    // the ZIP centroids within 25 km. 19008's is 5,101 m off and 19333's 5,132 m, but 19333's is
    // 0.0486° away and 19008's 0.0537°: Nominatim names 19333. (19073, the ZIP of the house
    // there, has its centroid 6.8 km off.)
    let point = (40.0033, -75.40011);
    let mut dataset = country_at(point, b"US");
    for (postcode, lat, lng) in [("19008", 39.971867, -75.35658), ("19333", 40.045578, -75.42411), ("19073", 39.971416, -75.46865)] {
        let (north, east) = offset_m(point, (lat, lng));
        dataset.centroid(postcode, north, east);
    }
    assert_eq!(postcode_from(&dataset.load(), point, 0.0, 0.0), "19333");
}

#[test]
fn a_centroid_of_another_country_names_no_point() {
    // Near the border of Germany and the Netherlands, a Dutch centroid nearer than the German
    // one: get_nearest_postcode takes the country's centroids only.
    let origin = (52.2, 7.0);
    let mut dataset = country_at(origin, b"DE");
    dataset.centroid("48599", 0.0, 3000.0).centroid("7586 AA", 0.0, -200.0);
    dataset.centroids.last_mut().unwrap().country_code = u16::from_be_bytes(*b"NL");
    assert_eq!(postcode_from(&dataset.load(), origin, 0.0, 0.0), "48599");
}
