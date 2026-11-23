//! The postcode of an answer without one of its own: the postcode grid's cell of the point where
//! the dir has a grid of the point's country, else Nominatim 5.3's get_nearest_postcode on a
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
fn a_centroid_s_box_is_the_envelope_of_its_buffer_on_the_ellipsoid() {
    // expand_by_meters buffers the centroid on the geography (geography_box): at 20° N a degree
    // of latitude is 110,704 m, so a point 0.2255° north of a US centroid, 24,960 m off, is
    // inside its 25 km box, though 0.2255 degrees of 111,320 m are 25,103 m.
    let origin = (20.0, -75.4);
    let index = country_at(origin, b"US").centroid("00601", 0.0, 0.0).load();
    assert_eq!(postcode_from(&index, origin, 0.2255 * METRES_PER_DEGREE, 0.0), "00601");
    assert_eq!(postcode_from(&index, origin, 0.2262 * METRES_PER_DEGREE, 0.0), serde_json::Value::Null);
}

/// The level-16 cell `n` cells along the curve from the one of the point `north_m`, `east_m` from
/// `origin`, as a query takes the point.
fn grid_cell(origin: (f64, f64), north_m: f64, east_m: f64, n: i64) -> u64 {
    let p = at_from(origin, north_m, east_m);
    let cell = cell_id_at_level(p.lat as f64, p.lng as f64, 16);
    cell.wrapping_add_signed(n << 29)
}

#[test]
fn the_postcode_grid_names_a_point_before_the_nearest_centroid() {
    // Drexel Hill, Delaware County (39.93538, -75.29800), with no postal area: 19018's centroid is
    // 1,242 m off and 19026's 1,860 m, but the house 18 m off is 19026's, as Nominatim's zoom-18
    // answer is, and so is the grid's cell of the point.
    let point = (39.93538, -75.29800);
    let mut dataset = country_at(point, b"US");
    dataset.centroid("19018", 1242.0, 0.0).centroid("19026", 0.0, -1860.0);
    assert_eq!(postcode_from(&dataset.load(), point, 0.0, 0.0), "19018");
    dataset.grid_run("19026", grid_cell(point, 0.0, 0.0, -2)).grid_run("19018", grid_cell(point, 0.0, 0.0, 1));
    assert_eq!(postcode_from(&dataset.load(), point, 0.0, 0.0), "19026");

    // Newtown Township (40.0033, -75.40011): the centroid nearest is 19333's, the TIGER line's
    // ZIP there 19073, which the grid's run from the cell of the point holds.
    let point = (40.0033, -75.40011);
    let mut dataset = country_at(point, b"US");
    dataset.centroid("19333", 4700.0, -2000.0).centroid("19073", -3500.0, -5800.0);
    dataset.grid_run("19073", grid_cell(point, 0.0, 0.0, 0)).grid_run("", grid_cell(point, 0.0, 0.0, 1));
    assert_eq!(postcode_from(&dataset.load(), point, 0.0, 0.0), "19073");
}

#[test]
fn a_cell_of_no_postcode_in_the_grid_leaves_the_point_to_the_nearest_centroid() {
    // Past the reach of every house the grid has no postcode, and get_nearest_postcode names the
    // point; before the grid's first run too.
    let point = (39.93538, -75.29800);
    let mut dataset = country_at(point, b"US");
    dataset.centroid("19018", 1242.0, 0.0);
    dataset.grid_run("19026", grid_cell(point, 0.0, 0.0, -3)).grid_run("", grid_cell(point, 0.0, 0.0, 0));
    assert_eq!(postcode_from(&dataset.load(), point, 0.0, 0.0), "19018");
    let mut dataset = country_at(point, b"US");
    dataset.centroid("19018", 1242.0, 0.0).grid_run("19026", grid_cell(point, 0.0, 0.0, 1));
    assert_eq!(postcode_from(&dataset.load(), point, 0.0, 0.0), "19018");
}

#[test]
fn the_grid_names_a_point_of_its_runs_country_only() {
    // Beyond the US border the grid's US runs name no point: a Canadian one takes its own
    // country's centroid.
    let point = (45.0, -73.5);
    let mut dataset = country_at(point, b"CA");
    dataset.centroid("J0L 1A0", 500.0, 0.0).grid_run("12919", grid_cell(point, 0.0, 0.0, 0));
    dataset.grid.last_mut().unwrap().country_code = u16::from_be_bytes(*b"US");
    assert_eq!(postcode_from(&dataset.load(), point, 0.0, 0.0), "J0L 1A0");
}

#[test]
fn a_house_with_its_own_postcode_names_the_point_before_the_grid() {
    // The house the street that won has near keeps its own addr:postcode (tier 0), whatever the
    // grid's cell holds.
    let point = (39.93538, -75.29800);
    let index = country_at(point, b"US")
        .street("Whalens Court", &[(0.0, -100.0), (0.0, 100.0)])
        .address("12", 20.0, 0.0)
        .addr_postcode("19083")
        .grid_run("19026", grid_cell(point, 2.0, 0.0, -1))
        .load();
    let address = address_from(&index, point, 2.0, 0.0);
    assert_eq!(address["house_number"], "12");
    assert_eq!(address["postcode"], "19083");
}
