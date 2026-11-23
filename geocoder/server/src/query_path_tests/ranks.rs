//! The rank_address of the polygons and place nodes of an address, as the builder stores them
//! (builder/src/rank_address.h, Nominatim's placex_update at index time): the server places each
//! part by it and labels a polygon without a place type by it, over boundaries and place areas
//! laid out as in the OSM data of the places named.

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
        .boundary("Thurrock", 6, 0, (-5000.0, -5000.0, 5000.0, 5000.0)).rank(14)
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
fn a_place_area_lifts_a_boundary_only_from_rank_23_down_and_round_its_surface_point() {
    // Prairie Township (R7238290, admin_level 7, rank 14) and Westland, a place=neighbourhood area
    // (rank 24) a little larger, round the point but not round the township's middle. Nominatim
    // lifts a boundary only into a place area of rank 1 to 23 that contains it (:912-929), so the
    // township keeps 14 under either. A place=quarter area (22) round it lifts it to 24.
    let township = |area: (&str, u8, Span), rank: u8| {
        TestIndex::in_country(b"US")
            .boundary("Ohio", 4, 7, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
            .boundary("Franklin County", 6, 10, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
            .boundary("Prairie Township", 7, 0, (-3000.0, -3000.0, 3000.0, 3000.0)).rank(rank)
            .boundary(area.0, 15, area.1, area.2)
            .load()
    };
    // Round the township's middle, rank 24.
    let index = township(("Westland", 5, (-3500.0, -3500.0, 3500.0, 3500.0)), 14);
    let address = address_at(&index, 1000.0, 1000.0);
    assert_eq!(address["municipality"], "Prairie Township");
    assert_eq!(address["neighbourhood"], "Westland");
    // Rank 22, larger, round the point but not round the township's middle.
    let index = township(("Lincoln Village", 6, (500.0, 500.0, 7000.0, 7000.0)), 14);
    let address = address_at(&index, 1000.0, 1000.0);
    assert_eq!(address["municipality"], "Prairie Township");
    assert_eq!(address["quarter"], "Lincoln Village");
    // Rank 22, round the township: it ranks 24, a city block.
    let index = township(("Lincoln Village", 6, (-3500.0, -3500.0, 3500.0, 3500.0)), 24);
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
        .boundary("Cairns Regional", 6, 14, (-20_000.0, -20_000.0, 20_000.0, 20_000.0)).rank(0)
        .boundary("Cairns City", 9, 4, (-2000.0, -2000.0, 2000.0, 2000.0)).rank(20)
        .load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address.get("municipality"), None);
    assert_eq!(address["suburb"], "Cairns City");
    assert_eq!(address["state"], "Queensland");

    let index = TestIndex::in_country(b"DE")
        .boundary("North Rhine-Westphalia", 4, 7, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Regierungsbezirk Köln", 5, 9, (-30_000.0, -30_000.0, 30_000.0, 30_000.0)).rank(0)
        .boundary("Cologne", 6, 1, (-10_000.0, -10_000.0, 10_000.0, 10_000.0)).rank(16)
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
        .boundary("Greater São Paulo", 5, 0, (-30_000.0, -30_000.0, 30_000.0, 30_000.0)).rank(0)
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
            .boundary("Some County", 15, 10, (-20_000.0, -20_000.0, 20_000.0, 20_000.0)).rank(0)
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
        .boundary("Upper Ward", 9, 5, (-3000.0, -3000.0, 3000.0, 3000.0)).rank(24)
        .boundary("Lower Ward", 10, 5, (-1000.0, -1000.0, 1000.0, 1000.0)).rank(25)
        .load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address["neighbourhood"], "Upper Ward");
    assert_eq!(address["city"], "Town");
}

#[test]
fn every_boundary_holding_the_point_is_a_part_of_the_address() {
    // Brussels: Bruxelles - Brussel (R2404021, admin_level 10, 18) lies round Quays Quarter
    // (admin_level 10, linked to a neighbourhood: 24); Nominatim's address has both, the larger
    // as the city district.
    let index = TestIndex::in_country(b"BE")
        .boundary("Brussels-Capital", 4, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0)).rank(6)
        .boundary("Brussels", 8, 1, (-5000.0, -5000.0, 5000.0, 5000.0))
        .boundary("Bruxelles - Brussel", 10, 0, (-3000.0, -3000.0, 3000.0, 3000.0)).rank(18)
        .boundary("Quays Quarter", 10, 5, (-300.0, -300.0, 300.0, 300.0)).rank(24)
        .boundary("Pentagon", 15, 4, (-1500.0, -1500.0, 1500.0, 1500.0))
        .street("Rue Melsens - Melsensstraat", &[(0.0, -100.0), (0.0, 100.0)])
        .load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address["neighbourhood"], "Quays Quarter");
    assert_eq!(address["city_district"], "Bruxelles - Brussel");
    assert_eq!(address["suburb"], "Pentagon");
    assert_eq!(address["city"], "Brussels");
    let p = at(0.0, 0.0);
    let answer = serde_json::to_value(index.query(p.lat as f64, p.lng as f64)).unwrap();
    assert_eq!(answer["display_name"],
               "Rue Melsens - Melsensstraat, Quays Quarter, Pentagon, Bruxelles - Brussel, Brussels, Brussels-Capital, Testland");
}

#[test]
fn a_city_labelled_boundary_holds_its_own_rank_from_place_nodes() {
    // A boundary labelled city whose rank is 18, as Sarajevo's admin_level 9 one, holds 18 and
    // not 16: rank 16 is left to the village node of Vogošća inside it.
    let index = TestIndex::in_country(b"BA")
        .boundary("Sarajevo Canton", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Sarajevo", 9, 1, (-10_000.0, -10_000.0, 10_000.0, 10_000.0))
        .place("Vogošća", 2, 300.0, 200.0)
        .load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address["city"], "Sarajevo");
    assert_eq!(address["village"], "Vogošća");
}
