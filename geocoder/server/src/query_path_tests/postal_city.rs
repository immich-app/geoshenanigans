//! address.postal_city: the place name mail to an answer's postcode is addressed with
//! (postal_cities.bin), beside the postcode, where the postcode came from anything but the nearest
//! centroid and the table has a row of the answer's country and the postcode's code. Not a
//! Nominatim field: display_name and every other field stay as they are.

use super::*;

/// Testland around `origin` under the country code `code`.
fn country_at(origin: (f64, f64), code: &[u8; 2]) -> TestIndex {
    let mut index = TestIndex::around(origin);
    index.polygons[0].0.country_code = u16::from_be_bytes(*code);
    index
}

/// The level-16 cell of the point `north_m`, `east_m` from `origin`.
fn grid_cell(origin: (f64, f64), north_m: f64, east_m: f64) -> u64 {
    let p = at_from(origin, north_m, east_m);
    cell_id_at_level(p.lat as f64, p.lng as f64, 16)
}

/// A run of `postcode` over the level-16 cell of the point `north_m`, `east_m` from `origin`, and
/// none in the cell after it.
fn grid_over(dataset: &mut TestIndex, origin: (f64, f64), north_m: f64, east_m: f64, postcode: &str) {
    let cell = grid_cell(origin, north_m, east_m);
    dataset.grid_run(postcode, cell).grid_run("", cell + (1 << 29));
}

/// The rows of the Delaware County ZIPs.
fn delaware_county(dataset: &mut TestIndex) {
    dataset.postal_city("19018", "Clifton Heights").postal_city("19026", "Drexel Hill").postal_city("19073", "Newtown Square");
}

/// A house of 19026 in Drexel Hill, Delaware County, whose postal city is Drexel Hill though the
/// township is Upper Darby.
const DREXEL_HILL: (f64, f64) = (39.93538, -75.29800);
/// A point of 19073 in Newtown Township, Delaware County, whose postal city is Newtown Square.
const NEWTOWN: (f64, f64) = (40.00330, -75.40011);

#[test]
fn a_houses_own_zip_names_its_postal_city() {
    for postcode in ["19026", "19026-1234"] {
        let mut dataset = country_at(DREXEL_HILL, b"US");
        dataset.street("Whalens Court", &[(0.0, -100.0), (0.0, 100.0)]).address("2", 20.0, 0.0).addr_postcode(postcode);
        delaware_county(&mut dataset);
        let address = address_from(&dataset.load(), DREXEL_HILL, 2.0, 0.0);
        assert_eq!(address["house_number"], "2");
        assert_eq!(address["postcode"], "19026", "{postcode}");
        assert_eq!(address["postal_city"], "Drexel Hill", "{postcode}");
    }
}

#[test]
fn the_grids_zip_names_its_postal_city_where_the_nearest_centroid_names_none() {
    // At the Drexel Hill point 19018's centroid is nearer than 19026's: with no grid the answer's
    // postcode is the centroid's, of no postal city; the grid's 19026 names Drexel Hill.
    let mut dataset = country_at(DREXEL_HILL, b"US");
    dataset.centroid("19018", 1242.0, 0.0).centroid("19026", 0.0, -1860.0);
    delaware_county(&mut dataset);
    let address = address_from(&dataset.load(), DREXEL_HILL, 0.0, 0.0);
    assert_eq!(address["postcode"], "19018");
    assert!(address.get("postal_city").is_none(), "{address}");
    grid_over(&mut dataset, DREXEL_HILL, 0.0, 0.0, "19026");
    let address = address_from(&dataset.load(), DREXEL_HILL, 0.0, 0.0);
    assert_eq!(address["postcode"], "19026");
    assert_eq!(address["postal_city"], "Drexel Hill");

    // Newtown Township: the grid's 19073 is Newtown Square's.
    let mut dataset = country_at(NEWTOWN, b"US");
    dataset.centroid("19333", 4700.0, -2000.0);
    delaware_county(&mut dataset);
    grid_over(&mut dataset, NEWTOWN, 0.0, 0.0, "19073");
    let address = address_from(&dataset.load(), NEWTOWN, 0.0, 0.0);
    assert_eq!(address["postcode"], "19073");
    assert_eq!(address["postal_city"], "Newtown Square");
}

#[test]
fn a_streets_postcode_names_its_postal_city_unless_the_nearest_centroid_gave_it() {
    // The grid's ZIP at the street's middle, 900 m east, and the street's stored one name their
    // rows; the centroid nearest the middle names none.
    let mut dataset = country_at(DREXEL_HILL, b"US");
    dataset.street("Gilmore Road", &[(0.0, -100.0), (0.0, 1000.0), (0.0, 1900.0)]).centroid("19026", 40.0, 900.0);
    delaware_county(&mut dataset);
    let address = address_from(&dataset.load(), DREXEL_HILL, 5.0, 0.0);
    assert_eq!(address["road"], "Gilmore Road");
    assert_eq!(address["postcode"], "19026");
    assert!(address.get("postal_city").is_none(), "{address}");
    grid_over(&mut dataset, DREXEL_HILL, 0.0, 900.0, "19018");
    let address = address_from(&dataset.load(), DREXEL_HILL, 5.0, 0.0);
    assert_eq!(address["postcode"], "19018");
    assert_eq!(address["postal_city"], "Clifton Heights");

    let mut dataset = country_at(DREXEL_HILL, b"US");
    dataset.street("Gilmore Road", &[(0.0, -100.0), (0.0, 1900.0)]).way_postcode("19073");
    delaware_county(&mut dataset);
    let address = address_from(&dataset.load(), DREXEL_HILL, 5.0, 0.0);
    assert_eq!(address["postcode"], "19073");
    assert_eq!(address["postal_city"], "Newtown Square");
}

#[test]
fn a_postal_areas_zip_names_its_postal_city() {
    // A boundary=postal_code area of 19073 round the point, no run of the grid there, and a
    // centroid of 19333 farther off.
    let mut dataset = country_at(NEWTOWN, b"US");
    dataset.boundary("19073", POSTCODE_AREA_LEVEL, 0, (-800.0, -800.0, 800.0, 800.0)).centroid("19333", 4700.0, -2000.0);
    delaware_county(&mut dataset);
    let address = address_from(&dataset.load(), NEWTOWN, 0.0, 0.0);
    assert_eq!(address["postcode"], "19073");
    assert_eq!(address["postal_city"], "Newtown Square");
}

#[test]
fn a_tiger_lines_zip_names_its_postal_city() {
    // A street of no postcode of its own beside a TIGER line of its name carrying 19073, and no
    // house.
    let mut dataset = country_at(NEWTOWN, b"US");
    dataset
        .street("Gradyville Road", &[(0.0, -200.0), (0.0, 200.0)])
        .interpolation("Gradyville Rd", (1, 99, 0), &[(5.0, -100.0), (5.0, 100.0)])
        .interp_postcode("19073");
    delaware_county(&mut dataset);
    let address = address_from(&dataset.load(), NEWTOWN, 1.0, 0.0);
    assert_eq!(address["road"], "Gradyville Road");
    assert_eq!(address["postcode"], "19073");
    assert_eq!(address["postal_city"], "Newtown Square");
}

/// `index` with one POI, a rectangle (`s`, `w`, `n`, `e` in metres from `origin`) named `name_id`
/// of the parent street named `street_id`, whose stored postcode (parent_postcode_id, its postal
/// boundary's at build time) is `postcode_id`, strings of the index's; the admin cell of its
/// middle lists it.
fn with_postcoded_poi(mut index: Index, origin: (f64, f64), name_id: u32, street_id: u32, postcode_id: u32,
                      (s, w, n, e): Span) -> Index {
    let (low, middle) = (at_from(origin, s, w), at_from(origin, (s + n) / 2.0, (w + e) / 2.0));
    let steps = |v: f32, from: f32| ((v as f64 - from as f64) / 1e-7).round() as u32;
    let corners = [at_from(origin, s, w), at_from(origin, s, e), at_from(origin, n, e), at_from(origin, n, w)];
    let polygon = poi_files::PoiPolygon {
        encoding: 3, bmin_lat: low.lat, bmin_lng: low.lng,
        q: corners.iter().map(|c| (steps(c.lat, low.lat), steps(c.lng, low.lng))).collect(),
    };
    let record = PoiRecord {
        lat: middle.lat, lng: middle.lng, vertex_offset: 0, name_id, category: 1, tier: 0, flags: 0, importance: 0,
        parent_street_id: street_id, parent_postcode_id: postcode_id, parent_poly_id: NO_DATA,
    };
    let (records, vertices) = poi_files::encode(&[(record, Some(polygon))]);
    let cell = cell_id_at_level(middle.lat as f64, middle.lng as f64, DEFAULT_ADMIN_CELL_LEVEL);
    index.poi_index = Some(cell_pairs::from_pairs([(cell, 0)], cell_pairs::POI));
    index.poi_records = Some(PoiRecords::open(FileBytes::Owned(records)).unwrap());
    index.poi_vertices = Some(FileBytes::Owned(vertices));
    index
}

#[test]
fn a_pois_stored_zip_names_its_postal_city() {
    // A town hall round the point, of Mill Lane, where the grid's cell of the point is 19026's.
    // With its stored postcode, 19073, the answer is the POI's, of that ZIP and its postal city;
    // without one it takes the grid's, and that ZIP's postal city.
    for (stored, postcode, city) in [(true, "19073", "Newtown Square"), (false, "19026", "Drexel Hill")] {
        let mut dataset = country_at(DREXEL_HILL, b"US");
        delaware_county(&mut dataset);
        grid_over(&mut dataset, DREXEL_HILL, 0.0, 0.0, "19026");
        let (hall, lane, zip) = (dataset.string("Town Hall"), dataset.string("Mill Lane"), dataset.string("19073"));
        let index = with_postcoded_poi(dataset.load(), DREXEL_HILL, hall, lane, if stored { zip } else { NO_DATA },
                                       (-20.0, -30.0, 20.0, 30.0));
        let address = address_from(&index, DREXEL_HILL, 0.0, 0.0);
        assert_eq!(address["road"], "Mill Lane", "{address}");
        assert_eq!(address["postcode"], postcode, "{address}");
        assert_eq!(address["postal_city"], city, "{address}");
    }
}

#[test]
fn a_zip_of_no_row_and_a_postcode_of_no_zip_name_no_postal_city() {
    // 19099 has no row.
    let mut dataset = country_at(DREXEL_HILL, b"US");
    delaware_county(&mut dataset);
    grid_over(&mut dataset, DREXEL_HILL, 0.0, 0.0, "19099");
    let address = address_from(&dataset.load(), DREXEL_HILL, 0.0, 0.0);
    assert_eq!(address["postcode"], "19099");
    assert!(address.get("postal_city").is_none(), "{address}");
    // A house's postcode the country's pattern doesn't take, and one no answer carries.
    for (postcode, shown) in [("1902", serde_json::json!("1902")), ("00000", serde_json::Value::Null)] {
        let mut dataset = country_at(DREXEL_HILL, b"US");
        dataset.street("Whalens Court", &[(0.0, -100.0), (0.0, 100.0)]).address("2", 20.0, 0.0).addr_postcode(postcode);
        dataset.postal_city("1902", "Nowhere");
        delaware_county(&mut dataset);
        let address = address_from(&dataset.load(), DREXEL_HILL, 2.0, 0.0);
        assert_eq!(address["postcode"], shown, "{postcode}");
        assert!(address.get("postal_city").is_none(), "{postcode}: {address}");
    }
    // A dir of no table.
    let mut dataset = country_at(DREXEL_HILL, b"US");
    grid_over(&mut dataset, DREXEL_HILL, 0.0, 0.0, "19026");
    let address = address_from(&dataset.load(), DREXEL_HILL, 0.0, 0.0);
    assert_eq!(address["postcode"], "19026");
    assert!(address.get("postal_city").is_none(), "{address}");
}

#[test]
fn an_answer_of_another_country_names_no_postal_city() {
    // A Canadian house beside the border whose postcode is a US ZIP's digits, beside the US row.
    let point = (45.0, -73.5);
    let mut dataset = country_at(point, b"CA");
    dataset.street("Rue Principale", &[(0.0, -100.0), (0.0, 100.0)]).address("5", 20.0, 0.0).addr_postcode("12919");
    // The US row, and a row of Canada's of the same digits, which no Canadian answer reads either.
    for country in [*b"US", *b"CA"] {
        dataset.postal_cities.push(postal_cities::LogicalRow {
            country_code: u16::from_be_bytes(country),
            code: "12919".to_string(),
            name: "Champlain".to_string(),
        });
    }
    let address = address_from(&dataset.load(), point, 2.0, 0.0);
    assert_eq!(address["postcode"], "12919");
    assert!(address.get("postal_city").is_none(), "{address}");
}

#[test]
fn the_postal_city_follows_the_postcode_and_display_name_leaves_it_out() {
    let mut dataset = country_at(DREXEL_HILL, b"US");
    dataset.boundary("Upper Darby", 8, 0, (-3000.0, -3000.0, 3000.0, 3000.0));
    dataset.street("Whalens Court", &[(0.0, -100.0), (0.0, 100.0)]).address("2", 20.0, 0.0).addr_postcode("19026");
    let p = at_from(DREXEL_HILL, 2.0, 0.0);
    let without = serde_json::to_value(dataset.load().query(p.lat as f64, p.lng as f64)).unwrap();
    delaware_county(&mut dataset);
    let index = dataset.load();
    let with = serde_json::to_value(index.query(p.lat as f64, p.lng as f64)).unwrap();
    assert_eq!(with["address"]["postal_city"], "Drexel Hill");
    assert_eq!(with["display_name"], without["display_name"]);
    assert!(!with["display_name"].as_str().unwrap().contains("Drexel Hill"));
    // Every other field as it was.
    let mut others = with.clone();
    others["address"].as_object_mut().unwrap().remove("postal_city");
    assert_eq!(others, without);
    // The key sits right after the postcode.
    let json = serde_json::to_string(&index.query(p.lat as f64, p.lng as f64)).unwrap();
    assert!(json.contains("\"postcode\":\"19026\",\"postal_city\":\"Drexel Hill\",\"country\""), "{json}");
}

#[test]
fn a_garbled_table_names_no_postal_city_and_the_answer_stands() {
    let mut dataset = country_at(DREXEL_HILL, b"US");
    grid_over(&mut dataset, DREXEL_HILL, 0.0, 0.0, "19026");
    let rows = [postal_cities::LogicalRow { country_code: u16::from_be_bytes(*b"US"), code: "19026".to_string(), name: "Drexel Hill".to_string() }];
    let mut file = postal_cities::encode(&rows);
    // The row's name index, past the block's one name.
    file[6] = 7;
    let mut index = dataset.load();
    index.postal_cities = Some(postal_cities::PostalCities::open(FileBytes::Owned(file)).unwrap());
    let address = address_from(&index, DREXEL_HILL, 0.0, 0.0);
    assert_eq!(address["postcode"], "19026");
    assert!(address.get("postal_city").is_none(), "{address}");
}
