//! The rank_address of the polygons and place nodes of an address, as Nominatim's placex_update
//! gives them at index time (placex_triggers.sql), over boundaries and place areas laid out as in
//! the OSM data of the places named.

use super::*;

#[test]
fn a_place_area_keeps_its_place_rank_round_smaller_boundaries() {
    // Little Thurrock: Essex (R62162, boundary=ceremonial, place=county) is a place area of rank
    // 12 round Thurrock (admin_level 6). It takes neither the admin-nested step nor a place area's
    // (placex_triggers.sql:930-950), so it stays at 12, and lifts Thurrock to 14: rank 16 is left
    // to the town node Grays.
    let index = TestIndex::in_country(b"GB")
        .boundary("England", 4, 7, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Essex", 15, 10, (-30_000.0, -30_000.0, 30_000.0, 30_000.0))
        .boundary("Thurrock", 6, 0, (-5000.0, -5000.0, 5000.0, 5000.0))
        .place("Grays", 1, 500.0, -800.0)
        .place("Little Thurrock", 3, 200.0, 300.0)
        .load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address["county"], "Essex");
    assert_eq!(address["municipality"], "Thurrock");
    assert_eq!(address["town"], "Grays");
    assert_eq!(address["suburb"], "Little Thurrock");
}

#[test]
fn a_place_area_lifts_a_boundary_only_from_rank_23_down_and_holding_all_of_it() {
    // Prairie Township (R7238290, admin_level 7, rank 14) under Westland, a place=neighbourhood
    // area (rank 24) a little larger round it, and under Lincoln Village, a place=quarter area
    // (22) larger than it round its middle and the point, but not round its southern strip.
    // Nominatim lifts a boundary only into a place area of rank 1 to 23 that contains it whole
    // (:912-929, ST_Contains), so the township keeps 14 under either. A place=quarter area round
    // it lifts it to 24.
    let township = |area: (&str, u8, Span)| {
        TestIndex::in_country(b"US")
            .boundary("Ohio", 4, 7, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
            .boundary("Franklin County", 6, 10, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
            .boundary("Prairie Township", 7, 0, (-3000.0, -3000.0, 3000.0, 3000.0))
            .boundary(area.0, 15, area.1, area.2)
            .load()
    };
    // Round all of the township, rank 24.
    let index = township(("Westland", 5, (-3500.0, -3500.0, 3500.0, 3500.0)));
    let address = address_at(&index, 1000.0, 1000.0);
    assert_eq!(address["municipality"], "Prairie Township");
    assert_eq!(address["neighbourhood"], "Westland");
    // Rank 22, larger, round the point and the township's middle, the strip south of 2,500 m out.
    let index = township(("Lincoln Village", 6, (-2500.0, -4000.0, 3500.0, 3500.0)));
    let address = address_at(&index, 1000.0, 1000.0);
    assert_eq!(address["municipality"], "Prairie Township");
    assert_eq!(address["quarter"], "Lincoln Village");
    // Rank 22, round the township: it ranks 24, a city block.
    let index = township(("Lincoln Village", 6, (-3500.0, -3500.0, 3500.0, 3500.0)));
    let address = address_at(&index, 1000.0, 1000.0);
    assert_eq!(address.get("municipality"), None);
    assert_eq!(address["city_block"], "Prairie Township");
}

#[test]
fn a_boundary_its_level_ranks_0_stays_out_of_the_address_whatever_its_place_type() {
    // Cairns Regional (R11677109, admin_level 6, place=municipality): administrative6 is [12, 0]
    // in Australia, and a boundary of rank 0 takes no admin-nested step and no place rank
    // (placex_triggers.sql:877, :1158). Regierungsbezirk Köln (R72022, admin_level 5,
    // place=region) in Germany likewise.
    let index = TestIndex::in_country(b"AU")
        .boundary("Queensland", 4, 7, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Cairns Regional", 6, 14, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Cairns City", 9, 4, (-2000.0, -2000.0, 2000.0, 2000.0))
        .load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address.get("municipality"), None);
    assert_eq!(address["suburb"], "Cairns City");
    assert_eq!(address["state"], "Queensland");

    let index = TestIndex::in_country(b"DE")
        .boundary("North Rhine-Westphalia", 4, 7, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Regierungsbezirk Köln", 5, 9, (-30_000.0, -30_000.0, 30_000.0, 30_000.0))
        .boundary("Cologne", 6, 1, (-10_000.0, -10_000.0, 10_000.0, 10_000.0))
        .load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address.get("region"), None);
    assert_eq!(address["city"], "Cologne");
}

#[test]
fn brazil_and_slovakia_rank_their_levels_as_nominatim_does() {
    // Brazil's administrative5 to 7 are [10, 0] to [14, 0] since Nominatim 5.3.0: Greater São
    // Paulo (R2661855, admin_level 5) is in no address.
    let index = TestIndex::in_country(b"BR")
        .boundary("São Paulo", 4, 7, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Greater São Paulo", 5, 0, (-30_000.0, -30_000.0, 30_000.0, 30_000.0))
        .boundary("Ferraz de Vasconcelos", 8, 1, (-5000.0, -5000.0, 5000.0, 5000.0))
        .load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address.get("state_district"), None);
    assert_eq!(address["city"], "Ferraz de Vasconcelos");

    // Slovakia has no levels of its own any more: an okres (admin_level 6) is a county at 12, and
    // an obec (8) a city at 16 inside it.
    let index = TestIndex::in_country(b"SK")
        .boundary("Trnava Region", 4, 9, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("District of Senica", 6, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Prietrž", 8, 0, (-2000.0, -2000.0, 2000.0, 2000.0))
        .load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address["county"], "District of Senica");
    assert_eq!(address["city"], "Prietrž");
    assert_eq!(address.get("state_district"), None);
}

#[test]
fn a_country_ranks_place_types_of_its_own() {
    // place=county is [12, 0] in Canada and Germany: a county place area there is in no address.
    for code in [b"CA", b"DE"] {
        let index = TestIndex::in_country(code)
            .boundary("Province", 4, 7, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
            .boundary("Some County", 15, 10, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
            .load();
        let address = address_at(&index, 0.0, 0.0);
        assert_eq!(address.get("county"), None, "{}", String::from_utf8_lossy(code));
        assert_eq!(address["state"], "Province");
    }
}

#[test]
fn the_admin_nested_step_stops_at_rank_25_which_no_address_shows() {
    // A boundary inside one of rank 24 or more takes rank 25 (placex_triggers.sql:903-904), and an
    // area of rank 25 is in no street's or house's address (getNearFeatures, rank_address below
    // 25): the neighbourhood-linked level 10 boundary inside the level 9 one names nothing, the
    // level 9 one does.
    let index = TestIndex::new()
        .boundary("Town", 8, 0, (-10_000.0, -10_000.0, 10_000.0, 10_000.0))
        .boundary("Upper Ward", 9, 5, (-3000.0, -3000.0, 3000.0, 3000.0))
        .boundary("Lower Ward", 10, 5, (-1000.0, -1000.0, 1000.0, 1000.0))
        .load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address["neighbourhood"], "Upper Ward");
    assert_eq!(address["city"], "Town");
}

#[test]
fn a_boundary_that_repeats_its_parent_leaves_its_level_to_a_larger_one_round_the_point() {
    // is_same gives a boundary that repeats its parent rank_address 0 (placex_triggers.sql:915-932),
    // and a boundary of rank 0 is no address area (:1273): every other boundary covering the
    // point is a candidate (getNearFeatures), a larger one of the same level too, which names the
    // city round the repeat.
    let index = TestIndex::new()
        .boundary("Greater Town", 8, 0, (-10_000.0, -10_000.0, 10_000.0, 10_000.0))
        .boundary("Old Town", 8, BOUNDARY_REPEATS_PARENT, (-2000.0, -2000.0, 2000.0, 2000.0))
        .load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address["city"], "Greater Town");
}

#[test]
fn a_boundary_that_repeats_its_parent_gates_and_promotes_no_place_node() {
    // A boundary of rank 0 promotes no place node (placex_triggers.sql:994, rank_address between
    // 1 and 25) and is no address area (:1273), so it is never the boundary the address walk's
    // later candidates must lie in: the town node 3 km off, outside the repeat round the point
    // but inside the county, is the town.
    let index = TestIndex::new()
        .boundary("Some County", 6, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Old Town", 8, BOUNDARY_REPEATS_PARENT, (-2000.0, -2000.0, 2000.0, 2000.0))
        .place("Newtown", 1, 3000.0, 0.0)
        .load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address["town"], "Newtown");
    assert_eq!(address["county"], "Some County");
}

#[test]
fn every_boundary_of_a_level_round_the_smallest_holding_the_point_ranks() {
    // Brussels centre (50.85045, 4.34878): Nominatim takes every boundary of a rank above 0
    // covering the point (getNearFeatures, placex_triggers.sql:1273), one row a rank. The level 9
    // boundary repeating the city ranks 0, so Bruxelles - Brussel (admin_level 10, 18 in Belgium)
    // round Quays Quarter (10, place=neighbourhood, 24) has the city (8, linked to the city node,
    // 16) for its parent, keeps 18 and is the city_district.
    let index = TestIndex::in_country(b"BE")
        .boundary("Brussels-Capital", 4, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Brussels", 8, 1, (-5000.0, -5000.0, 5000.0, 5000.0))
        .boundary("Brussels", 9, BOUNDARY_REPEATS_PARENT, (-5000.0, -5000.0, 5000.0, 5000.0))
        .boundary("Bruxelles - Brussel", 10, 0, (-3000.0, -3000.0, 3000.0, 3000.0))
        .boundary("Quays Quarter", 10, 5, (-500.0, -500.0, 500.0, 500.0))
        .load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address["city"], "Brussels");
    assert_eq!(address["city_district"], "Bruxelles - Brussel");
    assert_eq!(address["neighbourhood"], "Quays Quarter");
}

#[test]
fn a_larger_boundary_of_the_level_beside_the_smallest_holding_the_point_takes_no_rank() {
    // Simplified neighbours overlap along their shared border: at a point in the overlap the
    // larger level 10 boundary, a suburb, holds the point but not the smaller one, a
    // neighbourhood, round which it would be a row of its own.
    let index = TestIndex::new()
        .boundary("Town", 8, 0, (-10_000.0, -10_000.0, 10_000.0, 10_000.0))
        .boundary("West End", 10, 5, (-1000.0, -2000.0, 1000.0, 20.0))
        .boundary("Eastfield", 10, 4, (-1500.0, -20.0, 1500.0, 3000.0))
        .load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address["neighbourhood"], "West End");
    assert_eq!(address.get("suburb"), None);
    assert_eq!(address["city"], "Town");
}

#[test]
fn a_place_node_ranked_above_the_boundary_of_its_label_names_the_field() {
    // Upton (52.231927, -0.936684): the parish of Upton (R1638092, admin_level 10, rank 20) is
    // labelled suburb by its rank, and the place=suburb node St Crispin's inside it ranks 22
    // (placex_triggers.sql:984-1003). Nominatim's address keeps the first row of a label, its rows
    // the finest first (format_json.py, results.py), so the node is the suburb; display_name joins
    // every row, the parish's too. At an equal rank the boundary's row comes first (isguess asc,
    // :576).
    let index = TestIndex::in_country(b"GB")
        .boundary("England", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("West Northamptonshire", 6, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Upton", 10, 0, (-2000.0, -2000.0, 2000.0, 2000.0))
        .place("St Crispin's", 3, 300.0, 200.0)
        .load();
    let answer = serde_json::to_value(index.query(at(0.0, 0.0).lat as f64, at(0.0, 0.0).lng as f64)).unwrap();
    assert_eq!(answer["address"]["suburb"], "St Crispin's");
    assert_eq!(answer["display_name"], "St Crispin's, Upton, West Northamptonshire, England, Testland");
}

#[test]
fn of_two_boundaries_of_a_level_round_the_point_at_one_rank_the_smaller_alone_is_in_the_address() {
    // Nominatim's address has a row a rank (insert_addresslines: address_havelevel), and only those
    // rows fill fields and display_name (format_json.py, results.py). A level 8 town of rank 16,
    // labelled city by its rank, inside a level 8 boundary tagged place=town, of rank 16 as well
    // (placex_triggers.sql:1192), round the point by the town's middle: the town is the city, and
    // the boundary round it names nothing.
    let index = TestIndex::new()
        .boundary("Some County", 6, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Greater Town", 8, 2, (-10_000.0, -10_000.0, 10_000.0, 10_000.0))
        .boundary("Old Town", 8, 0, (1000.0, 1000.0, 3000.0, 3000.0))
        .load();
    let answer = serde_json::to_value(index.query(at(2000.0, 2000.0).lat as f64, at(2000.0, 2000.0).lng as f64)).unwrap();
    assert_eq!(answer["address"]["city"], "Old Town");
    assert_eq!(answer["address"].get("town"), None);
    assert_eq!(answer["display_name"], "Old Town, Some County, Testland");
}
