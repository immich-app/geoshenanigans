//! The area that answers a point where no street, house or POI is near (reverse.py's lookup_area,
//! _lookup_area_address): the area of the highest rank_search round it, of rank_address 4 to 25,
//! which leads the answer where it is one of rank 25 no other address takes, unless a place node
//! of a higher rank_search inside it is near enough to answer instead.

use super::*;

/// Western Australia (rank 8) and Telfer, an Australian locality boundary (admin_level 9,
/// rank_search 18) linking its place=locality node: rank 25 and labelled a locality, as the
/// builder gives it (R11690317), round the origin. `street` is how far north of the origin a
/// street runs, None for a dir without streets.
fn telfer(street: Option<f64>) -> TestIndex {
    let mut index = TestIndex::in_country(b"AU");
    index.boundary("Western Australia", 4, 7, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Telfer", 9, 15, (-20_000.0, -20_000.0, 20_000.0, 20_000.0)).rank(25);
    if let Some(north_m) = street {
        index.street("Telfer Road", &[(north_m, -500.0), (north_m, 500.0)]);
    }
    index
}

fn display(index: &Index, north_m: f64, east_m: f64) -> String {
    let p = at(north_m, east_m);
    let answer = serde_json::to_value(index.query(p.lat as f64, p.lng as f64)).unwrap();
    answer["display_name"].as_str().unwrap_or_default().to_owned()
}

#[test]
fn a_locality_of_rank_25_answers_a_point_no_street_is_near() {
    // 5 km from Telfer Road Nominatim finds no street (reverse.py lookup_street_poi reaches 0.007
    // degrees), and the area of the highest rank_search round the point is Telfer itself: "Telfer,
    // Western Australia, Australia", the area labelled as its locality node. On the road the
    // road's address leaves the locality out, as getNearFeatures takes no area of rank 25 for it.
    let index = telfer(Some(5000.0)).load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address["locality"], "Telfer");
    assert_eq!(display(&index, 0.0, 0.0), "Telfer, Western Australia, Testland");
    let address = address_at(&index, 5010.0, 0.0);
    assert_eq!(address["road"], "Telfer Road");
    assert_eq!(address.get("locality"), None);
    assert_eq!(display(&index, 5010.0, 0.0), "Telfer Road, Western Australia, Testland");
}

#[test]
fn a_street_or_a_house_within_nominatims_reach_answers_instead() {
    // lookup_street_poi reaches 0.007 degrees (0.006 and 0.001 of fuzziness), planar as ST_DWithin
    // on lon/lat, much farther than the street cells round the point that our own search reads: a
    // street 500 m north (0.0045 degrees) answers, its address leaving Telfer out, and one 900 m
    // north (0.0081 degrees) is out of reach, where Telfer answers.
    for (north_m, locality) in [(500.0, None), (900.0, Some("Telfer"))] {
        let index = telfer(Some(north_m)).load();
        let address = address_at(&index, 0.0, 0.0);
        assert_eq!(address.get("locality").and_then(|v| v.as_str()), locality, "{north_m}");
    }
    // So does a house 600 m east (0.0055 degrees at 10 degrees north).
    let index = telfer(Some(5000.0)).address("12", 0.0, 600.0).load();
    assert_eq!(address_at(&index, 0.0, 0.0).get("locality"), None);
    // And a named POI whose centre is 500 m north, which no street parents: a rank-30 row of
    // placex, as near as the reach, whatever its street.
    let mut dataset = telfer(Some(5000.0));
    let name_id = dataset.string("Old Mine Museum");
    let index = with_polygon_poi(dataset.load(), name_id, NO_DATA, (450.0, -50.0, 550.0, 50.0));
    assert_eq!(address_at(&index, 0.0, 0.0).get("locality"), None);
}

#[test]
fn only_a_poi_of_rank_search_30_near_the_point_leaves_the_locality_out() {
    // The features lookup_street_poi takes are rank-30 rows of placex (_find_closest_street_or_pois:
    // rank_search 30, a class other than place and building). A named peak whose centre is 500 m
    // north, natural=peak of rank_search 18 (address-levels.json), is none of them, and Telfer
    // answers; a museum there is one, as is a named office of none of our categories
    // (GENERIC_RANK30), and Telfer stays out.
    for (category, locality) in [(80u8, Some("Telfer")), (0, None), (170, None)] {
        let mut dataset = telfer(Some(5000.0));
        let name_id = dataset.string("Mount Telfer");
        let index = with_polygon_poi_of(dataset.load(), name_id, NO_DATA, (450.0, -50.0, 550.0, 50.0), category);
        assert_eq!(address_at(&index, 0.0, 0.0).get("locality").and_then(|v| v.as_str()), locality, "{category}");
    }
}

#[test]
fn a_dir_without_streets_answers_as_on_a_street_and_leaves_the_locality_out() {
    // An admin dir has no streets to tell a point no street is near: its answers stand for a
    // street's at the point, whose address takes no area of rank 25.
    let index = telfer(None).load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address.get("locality"), None);
    assert_eq!(address["state"], "Western Australia");
}

#[test]
fn a_place_node_inside_the_locality_and_near_enough_answers_instead() {
    // Punmu, a hamlet (rank_search 20, above the boundary's 18) 1.2 km off inside Telfer: within
    // its reverse_place_diameter of 0.02 degrees, so Nominatim answers with the hamlet, whose
    // address leaves Telfer out (_place_inside_area_query). 3 km off it is too far, and Telfer
    // answers.
    let index = telfer(Some(5000.0)).place("Punmu", 4, 800.0, 900.0).load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address.get("locality"), None);
    assert_eq!(address["hamlet"], "Punmu");
    // 1.5 km due north it lies outside its fuzzy area, which reaches 1 km, but within the
    // diameter (0.0135 degrees): the hamlet answers, leading the answer.
    let index = telfer(Some(5000.0)).place("Punmu", 4, 1500.0, 0.0).load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address.get("locality"), None);
    assert_eq!(address["hamlet"], "Punmu");
    assert_eq!(display(&index, 0.0, 0.0), "Punmu, Western Australia, Testland");
    // Of two nodes in reach, the one of the higher rank_search: a neighbourhood (24) 1.5 km off over
    // a village (19) 2.5 km off, neither of whose fuzzy areas holds the point.
    let index = telfer(Some(5000.0)).place("Punmu", 2, 2500.0, 0.0).place("Camp Seven", 5, -1500.0, 0.0).load();
    assert_eq!(display(&index, 0.0, 0.0), "Camp Seven, Western Australia, Testland");
    let index = telfer(Some(5000.0)).place("Punmu", 4, 2000.0, 2200.0).load();
    assert_eq!(address_at(&index, 0.0, 0.0)["locality"], "Telfer");
    // Outside Telfer, a hamlet as near answers nothing for it.
    let index = TestIndex::in_country(b"AU")
        .boundary("Western Australia", 4, 7, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Telfer", 9, 15, (-1000.0, -1000.0, 1000.0, 1000.0)).rank(25)
        .street("Telfer Road", &[(5000.0, -500.0), (5000.0, 500.0)])
        .place("Punmu", 4, 1200.0, 1200.0)
        .load();
    assert_eq!(address_at(&index, 0.0, 0.0)["locality"], "Telfer");
}

#[test]
fn an_area_of_a_higher_rank_search_round_the_point_answers_and_the_locality_stays_out() {
    // A suburb boundary (admin_level 10, rank_search 20) inside Telfer round the point is the
    // area _lookup_area_address takes: its address, of the ranks below its own, has no Telfer.
    let index = telfer(Some(5000.0))
        .boundary("Telfer Village", 10, 0, (-2000.0, -2000.0, 2000.0, 2000.0)).rank(20)
        .load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address.get("locality"), None);
    assert_eq!(address["suburb"], "Telfer Village");
    // Outside it Telfer answers.
    assert_eq!(address_at(&index, 3000.0, 0.0)["locality"], "Telfer");
}

#[test]
fn a_rank_25_area_without_a_place_type_answers_as_its_rank_labels_it() {
    // An Indonesian desa (admin_level 10, rank_search 24) its parent lifts to 25, with no place
    // tag of its own: Nominatim labels it by ADMIN_LABELS' rank 25 // 2, a city block.
    let index = TestIndex::in_country(b"ID")
        .boundary("Jawa Barat", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Sukamaju", 10, 0, (-2000.0, -2000.0, 2000.0, 2000.0)).rank(25)
        .street("Jalan Raya", &[(5000.0, -500.0), (5000.0, 500.0)])
        .load();
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address["city_block"], "Sukamaju");
    assert_eq!(display(&index, 0.0, 0.0), "Sukamaju, Jawa Barat, Testland");
}

#[test]
fn search_ranks_follow_address_levels() {
    // compute_place_rank's rank_search: twice the level by default, a country's own levels first.
    assert_eq!(admin_search_rank("AU", 9), 18);
    assert_eq!(admin_search_rank("", 8), 16);
    assert_eq!(admin_search_rank("NL", 9), 15);
    assert_eq!(admin_search_rank("ID", 10), 24);
    assert_eq!(admin_search_rank("JP", 11), 24);
    assert_eq!(admin_search_rank("BE", 10), 18);
    assert_eq!(admin_search_rank("SE", 4), 12);
    assert_eq!(admin_search_rank("", 13), 0);
    assert_eq!(place_type_search_rank(15, ""), 25);
    assert_eq!(place_type_search_rank(4, ""), 19);
    assert_eq!(reverse_place_diameter(18), 0.08);
    assert_eq!(reverse_place_diameter(19), 0.04);
    assert_eq!(reverse_place_diameter(20), 0.02);
}
