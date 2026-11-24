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
fn of_postal_areas_round_the_point_the_one_whose_centroid_is_nearest_names_it() {
    // get_nearest_postcode orders the postal areas covering the point by the distance from their
    // centroids (utils.sql: ORDER BY osm_id is null, ST_Distance(p.centroid, geom)): round a point
    // 1,200 m east of the middle of 35000 and 400 m west of the middle of 35370, which is inside
    // it, 35370 names it, and 35000 names one 300 m east of its middle, if 35370 holds it too.
    let rennes = (48.02, -1.18);
    let index = country_at(rennes, b"FR")
        .boundary("35000", POSTCODE_AREA_LEVEL, 0, (-3000.0, -3000.0, 3000.0, 3000.0))
        .boundary("35370", POSTCODE_AREA_LEVEL, 0, (-800.0, 0.0, 800.0, 3200.0))
        .centroid("53540", 0.0, 1300.0)
        .load();
    assert_eq!(postcode_from(&index, rennes, 0.0, 1200.0), "35370");
    assert_eq!(postcode_from(&index, rennes, 0.0, 300.0), "35000");
    assert_eq!(postcode_from(&index, rennes, 0.0, -2000.0), "35000");
}

#[test]
fn a_postal_area_of_no_valid_postcode_leaves_the_point_to_another_or_a_centroid() {
    // Nominatim makes postcode areas only of postcodes its country takes (postcodes.py
    // _update_postcode_areas): a boundary=postal_code tagged 00000 round the point names
    // nothing, and the area round it or else the nearest centroid does.
    let berlin = (52.52, 13.405);
    let index = country_at(berlin, b"DE")
        .boundary("00000", POSTCODE_AREA_LEVEL, 0, (-300.0, -300.0, 300.0, 300.0))
        .centroid("10178", 0.0, 500.0)
        .load();
    assert_eq!(postcode_from(&index, berlin, 0.0, 0.0), "10178");
    let index = country_at(berlin, b"DE")
        .boundary("00000", POSTCODE_AREA_LEVEL, 0, (-300.0, -300.0, 300.0, 300.0))
        .boundary("10179", POSTCODE_AREA_LEVEL, 0, (-2000.0, -2000.0, 2000.0, 2000.0))
        .centroid("10178", 0.0, 500.0)
        .load();
    assert_eq!(postcode_from(&index, berlin, 0.0, 0.0), "10179");
    // The builder marks an area of a postcode its country's pattern rejects (POSTAL_AREA_INVALID):
    // one tagged 1234, four digits in Germany, names nothing either.
    let index = country_at(berlin, b"DE")
        .boundary("1234", POSTCODE_AREA_LEVEL, POSTAL_AREA_INVALID, (-300.0, -300.0, 300.0, 300.0))
        .centroid("10178", 0.0, 500.0)
        .load();
    assert_eq!(postcode_from(&index, berlin, 0.0, 0.0), "10178");
    let index = country_at(berlin, b"DE")
        .boundary("1234", POSTCODE_AREA_LEVEL, POSTAL_AREA_INVALID, (-300.0, -300.0, 300.0, 300.0))
        .boundary("10179", POSTCODE_AREA_LEVEL, 0, (-2000.0, -2000.0, 2000.0, 2000.0))
        .centroid("10178", 0.0, 500.0)
        .load();
    assert_eq!(postcode_from(&index, berlin, 0.0, 0.0), "10179");
}

#[test]
fn a_postal_area_of_a_postcode_its_country_takes_names_the_point_whatever_its_length() {
    // Nominatim makes a postcode area of every postcode its country's pattern takes, which the
    // builder checks (POSTAL_AREA_INVALID): Saudi Arabia's ddddd(?:-dddd)? keeps 12345-6789 whole,
    // and Iran's (ddddd)[-_ ]?(ddddd) writes 12345-67890, of 11 characters.
    for (code, postcode, origin) in [(b"SA", "12345-6789", (24.7, 46.7)), (b"IR", "12345-67890", (35.7, 51.4))] {
        let index = country_at(origin, code)
            .boundary(postcode, POSTCODE_AREA_LEVEL, 0, (-300.0, -300.0, 300.0, 300.0))
            .centroid("54321", 0.0, 500.0)
            .load();
        assert_eq!(postcode_from(&index, origin, 0.0, 0.0), postcode);
    }
}

#[test]
fn a_postal_area_of_another_country_names_no_point() {
    // Büsingen am Hochrhein, a German exclave in Switzerland: get_nearest_postcode takes the
    // postcode areas of the point's country only (p.country_code = country), and a postal area's
    // country is the one its centroid lies in. A Swiss area round the point leaves it to the
    // German centroid.
    let busingen = (47.697, 8.69);
    let mut dataset = country_at(busingen, b"DE");
    dataset.boundary("8238", POSTCODE_AREA_LEVEL, 0, (-1000.0, -1000.0, 1000.0, 1000.0))
        .centroid("78266", 0.0, 400.0);
    dataset.polygons.last_mut().unwrap().0.country_code = u16::from_be_bytes(*b"CH");
    assert_eq!(postcode_from(&dataset.load(), busingen, 0.0, 0.0), "78266");
    // One of the point's country, or of a country not known, names it.
    for code in [*b"DE", [0, 0]] {
        let mut dataset = country_at(busingen, b"DE");
        dataset.boundary("78266", POSTCODE_AREA_LEVEL, 0, (-1000.0, -1000.0, 1000.0, 1000.0))
            .centroid("79999", 0.0, 100.0);
        dataset.polygons.last_mut().unwrap().0.country_code = u16::from_be_bytes(code);
        assert_eq!(postcode_from(&dataset.load(), busingen, 0.0, 0.0), "78266");
    }
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

#[test]
fn a_house_the_street_that_won_has_near_names_its_own_postcode() {
    // Nominatim answers with the house nearest the point on the street that won, within 0.001°
    // (reverse.py _find_housenumber_for_street), and the house's postcode is its own. Here the
    // street is nearest the point, the house 30 m off has NG4 4AE and the centroid nearest the
    // point is NG4 4AF's.
    let gedling = (52.98, -1.07);
    let index = country_at(gedling, b"GB")
        .street("Yew Tree Lane", &[(0.0, -100.0), (0.0, 100.0)])
        .address("3", 30.0, 0.0)
        .addr_postcode("NG4 4AE")
        .centroid("NG4 4AF", 5.0, 0.0)
        .load();
    let address = address_from(&index, gedling, 2.0, 0.0);
    assert_eq!(address["house_number"], "3");
    assert_eq!(address["postcode"], "NG4 4AE");
}

#[test]
fn a_house_tagged_a_placeholder_postcode_takes_its_street_s() {
    // Nominatim's clean-postcodes sanitizer drops a postcode its country's pattern rejects, so a
    // house tagged 00000 has none of its own (token_get_postcode) and takes its parent street's
    // (placex_update), whether the house answers or the street that won takes its number from
    // it.
    let berlin = (52.52, 13.405);
    let index = country_at(berlin, b"DE")
        .street("Rungestraße", &[(0.0, -100.0), (0.0, 100.0)])
        .way_postcode("10179")
        .address("3", 30.0, 0.0)
        .addr_postcode("00000")
        .load();
    for north in [2.0, 30.0] {
        let address = address_from(&index, berlin, north, 0.0);
        assert_eq!(address["house_number"], "3", "{north} m north");
        assert_eq!(address["postcode"], "10179", "{north} m north");
    }
}

#[test]
fn a_house_without_a_postcode_of_its_own_takes_its_street_s() {
    // Grange View Road, Gedling: a rank-30 object takes its own addr:postcode, else that of its
    // parent street, else get_nearest_postcode at its own centroid (placex_update). House 30 on
    // Grange View Road, 50 m north of the street, has none, and Yew Tree Lane runs nearer the
    // point than Grange View Road does.
    let gedling = (52.98, -1.07);
    let index = country_at(gedling, b"GB")
        .street("Grange View Road", &[(100.0, -300.0), (100.0, 300.0)])
        .way_postcode("NG4 4BT")
        .address("30", 55.0, 0.0)
        .street("Yew Tree Lane", &[(40.0, -300.0), (40.0, 300.0)])
        .centroid("NG4 4DA", 45.0, 0.0)
        .load();
    let address = address_from(&index, gedling, 50.0, 0.0);
    assert_eq!(address["house_number"], "30");
    assert_eq!(address["postcode"], "NG4 4BT");
}

#[test]
fn a_house_whose_street_has_no_postcode_takes_the_one_nearest_the_house() {
    // The street wins and its house 20 m off gives the number: the answer is the house, rank 30.
    // Neither the house nor its street has a postcode (no centroid's 700 m box holds the street's
    // middle, 1,450 m east), so get_nearest_postcode at the house names it: NG4 4AE's centroid is
    // 50 m from the house, NG4 4AF's 55 m from it and 35 m from the point.
    let gedling = (52.98, -1.07);
    let index = country_at(gedling, b"GB")
        .street("Long Lane", &[(0.0, -100.0), (0.0, 3000.0)])
        .address("7", 20.0, 0.0)
        .centroid("NG4 4AE", 70.0, 0.0)
        .centroid("NG4 4AF", -35.0, 0.0)
        .load();
    let address = address_from(&index, gedling, 0.0, 0.0);
    assert_eq!(address["house_number"], "7");
    assert_eq!(address["postcode"], "NG4 4AE");
}

#[test]
fn a_house_takes_the_postcode_its_street_has_at_its_middle() {
    // Neither the house nor its street has a postcode stored. The street's is get_nearest_postcode's
    // at its middle, 1,450 m east, where NG4 4AF's centroid lies, and the house takes it, its
    // parent street's (placex_update), not NG4 4AE's at the house.
    let gedling = (52.98, -1.07);
    let index = country_at(gedling, b"GB")
        .street("Long Lane", &[(0.0, -100.0), (0.0, 3000.0)])
        .address("7", 20.0, 0.0)
        .centroid("NG4 4AE", 20.0, 0.0)
        .centroid("NG4 4AF", 0.0, 1450.0)
        .load();
    let address = address_from(&index, gedling, 0.0, 0.0);
    assert_eq!(address["house_number"], "7");
    assert_eq!(address["postcode"], "NG4 4AF");
}

/// The country `west` west of `origin` and `east` east of it, each 50 km across.
fn border_at(origin: (f64, f64), west: &[u8; 2], east: &[u8; 2]) -> TestIndex {
    let mut index = TestIndex { origin, ..TestIndex::default() };
    for (code, span) in [(west, (-50_000.0, -50_000.0, 50_000.0, 0.0)), (east, (-50_000.0, 0.0, 50_000.0, 50_000.0))] {
        index.boundary(&String::from_utf8_lossy(code), 2, 0, span);
        index.polygons.last_mut().unwrap().0.country_code = u16::from_be_bytes(*code);
    }
    index
}

#[test]
fn a_street_across_a_border_takes_a_postcode_of_its_middle_s_country() {
    // placex_update gives a street the country at its centroid (get_country_code) and
    // get_nearest_postcode of that country there. Rheinweg runs from Germany, where the point
    // is, 4 km east into Switzerland, its middle 1,500 m past the border: the Swiss postal area
    // 8238 round the middle names it, though the box of the German centroid 78266 by the point
    // reaches the middle too.
    let origin = (47.69, 8.6);
    let ch = u16::from_be_bytes(*b"CH");
    let mut dataset = border_at(origin, b"DE", b"CH");
    dataset.boundary("8238", POSTCODE_AREA_LEVEL, 0, (-500.0, 1000.0, 500.0, 2000.0));
    dataset.polygons.last_mut().unwrap().0.country_code = ch;
    dataset.street("Rheinweg", &[(0.0, -500.0), (0.0, 3500.0)]).centroid("78266", 0.0, -300.0);
    let address = address_from(&dataset.load(), origin, 5.0, -400.0);
    assert_eq!(address["road"], "Rheinweg");
    assert_eq!(address["postcode"], "8238");

    // A house takes its parent street's country, and where no postcode reaches the street's
    // middle, get_nearest_postcode of that country at the house. Here Rheinweg runs 16 km east,
    // and the box of the Swiss centroid 8200, 1.4 km from the house, reaches the house but not
    // the middle: it names the German house, not the German centroid beside it.
    let mut dataset = border_at(origin, b"DE", b"CH");
    dataset.street("Rheinweg", &[(0.0, -500.0), (0.0, 15_500.0)])
        .address("7", 20.0, -400.0)
        .centroid("78266", 20.0, -450.0)
        .centroid("8200", 0.0, 1000.0);
    dataset.centroids.last_mut().unwrap().country_code = ch;
    let address = address_from(&dataset.load(), origin, 5.0, -400.0);
    assert_eq!(address["house_number"], "7");
    assert_eq!(address["postcode"], "8200");

    // The postcode is of the middle's country: from a point in the United Arab Emirates, which
    // have no postcodes, a street whose middle is in Oman has Oman's 112.
    let al_ain = (24.2, 55.8);
    let mut dataset = border_at(al_ain, b"AE", b"OM");
    dataset.boundary("112", POSTCODE_AREA_LEVEL, 0, (-500.0, 1000.0, 500.0, 2000.0));
    dataset.polygons.last_mut().unwrap().0.country_code = u16::from_be_bytes(*b"OM");
    dataset.street("Border Road", &[(0.0, -500.0), (0.0, 3500.0)]);
    let address = address_from(&dataset.load(), al_ain, 5.0, -400.0);
    assert_eq!(address["road"], "Border Road");
    assert_eq!(address["postcode"], "112");
    // As is the one the builder stores for it, which it takes at the middle too.
    let mut dataset = border_at(al_ain, b"AE", b"OM");
    dataset.street("Border Road", &[(0.0, -500.0), (0.0, 3500.0)]).way_postcode("112");
    let address = address_from(&dataset.load(), al_ain, 5.0, -400.0);
    assert_eq!(address["postcode"], "112");
}

#[test]
fn a_street_s_postcode_is_the_one_nearest_its_middle() {
    // A street answers with its own postcode, which placex_update takes with get_nearest_postcode
    // at the street's centroid, the point halfway along it (get_center_point,
    // ST_LineInterpolatePoint(geometry, 0.5)). Long Lane runs 2 km east, its middle 900 m east
    // of the point: NG4 4AB's centroid 30 m from the middle names it, not NG4 4AA's 60 m from the
    // point, whose box doesn't reach the middle.
    let gedling = (52.98, -1.07);
    let index = country_at(gedling, b"GB")
        .street("Long Lane", &[(0.0, -100.0), (0.0, 1000.0), (0.0, 1900.0)])
        .centroid("NG4 4AA", 0.0, -60.0)
        .centroid("NG4 4AB", 30.0, 900.0)
        .load();
    let address = address_from(&index, gedling, 5.0, 0.0);
    assert_eq!(address["road"], "Long Lane");
    assert_eq!(address["postcode"], "NG4 4AB");
}

#[test]
fn a_street_whose_middle_no_postcode_reaches_has_none() {
    // Nominatim's answer is the street, of the street's postcode: with no centroid box round its
    // middle it has none, though a centroid's box holds the point.
    let gedling = (52.98, -1.07);
    let index = country_at(gedling, b"GB")
        .street("Long Lane", &[(0.0, -100.0), (0.0, 1900.0)])
        .centroid("NG4 4AA", 0.0, -60.0)
        .load();
    let address = address_from(&index, gedling, 5.0, 0.0);
    assert_eq!(address["road"], "Long Lane");
    assert_eq!(address["postcode"], serde_json::Value::Null);
}

#[test]
fn a_street_takes_the_postal_area_round_its_middle() {
    // get_nearest_postcode at the street's middle takes the postal area holding it before any
    // centroid: the point is in 10178 and the street's middle, 1,000 m east, in 10179.
    let berlin = (52.52, 13.405);
    let index = country_at(berlin, b"DE")
        .boundary("10178", POSTCODE_AREA_LEVEL, 0, (-800.0, -800.0, 800.0, 500.0))
        .boundary("10179", POSTCODE_AREA_LEVEL, 0, (-800.0, 500.0, 800.0, 2400.0))
        .street("Rungestraße", &[(0.0, -100.0), (0.0, 2100.0)])
        .centroid("10178", 0.0, 0.0)
        .load();
    let address = address_from(&index, berlin, 5.0, 0.0);
    assert_eq!(address["road"], "Rungestraße");
    assert_eq!(address["postcode"], "10179");
}

#[test]
fn a_centroid_s_box_is_the_envelope_of_its_buffer_on_the_ellipsoid() {
    // expand_by_meters buffers the centroid on the geography (geography_box): at 20° N a degree
    // of latitude is 110,704 m, so a point 0.2255° north of a US centroid, 24,960 m off, is
    // inside its 25 km box, though 0.2255 degrees of 111,320 m are 25,103 m.
    let origin = (20.0, -75.4);
    let index = country_at(origin, b"US").centroid("00601", 0.0, 0.0).load();
    assert_eq!(postcode_from(&index, origin, 0.2255 * METRES_PER_DEGREE, 0.0), "00601");
    assert_eq!(postcode_from(&index, origin, 0.2262 * METRES_PER_DEGREE, 0.0), serde_json::Value::Null);
}
