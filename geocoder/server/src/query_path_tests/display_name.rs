//! display_name: an answer's place parts finest first by rank_address, as Nominatim's results.py
//! sorts the address rows it joins, and a name left out only where it repeats the part just before
//! it.

use super::*;

impl TestIndex {
    /// `name`, a country (rank 4) 100 km across around the origin, under the country code `code`.
    fn country(name: &str, code: &[u8; 2]) -> Self {
        let mut index = Self { origin: ORIGIN, ..Self::default() };
        index.boundary(name, 2, 0, (-50_000.0, -50_000.0, 50_000.0, 50_000.0));
        index.polygons[0].0.country_code = u16::from_be_bytes(*code);
        index
    }
}

/// The display_name of the answer `north_m` north and `east_m` east of the origin.
fn display_at(index: &Index, north_m: f64, east_m: f64) -> String {
    let p = at(north_m, east_m);
    let answer = serde_json::to_value(index.query(p.lat as f64, p.lng as f64)).unwrap();
    answer["display_name"].as_str().unwrap_or_default().to_owned()
}

#[test]
fn a_village_node_inside_a_township_of_its_rank_comes_before_the_township() {
    // Delaware County, Pennsylvania: Garrettford and Central Square are place=village nodes (rank
    // 16) inside Upper Darby and Newtown Township (admin_level 8, rank 16), so Nominatim ranks
    // them 18 (placex_triggers.sql:951-971), as the builder marks them, and lists them before
    // the township. Addingham, a hamlet (20), comes first.
    let index = TestIndex::country("United States", b"US")
        .boundary("Pennsylvania", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Delaware County", 6, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Upper Darby", 8, 0, (-4000.0, -4000.0, 4000.0, 4000.0))
        .place("Garrettford", 2, 600.0, 900.0).promoted()
        .place("Addingham", 4, 300.0, -200.0)
        .street("Whalens Court", &[(0.0, -200.0), (0.0, 200.0)])
        .address("2", 10.0, 0.0)
        .addr_postcode("19026")
        .load();
    assert_eq!(display_at(&index, 10.0, 0.0),
               "2 Whalens Court, Addingham, Garrettford, Upper Darby, Delaware County, Pennsylvania, 19026, United States");

    let index = TestIndex::country("United States", b"US")
        .boundary("Pennsylvania", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Delaware County", 6, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Newtown Township", 8, 0, (-4000.0, -4000.0, 4000.0, 4000.0))
        .place("Central Square", 2, -700.0, 400.0).promoted()
        .street("Roberts Road", &[(0.0, -200.0), (0.0, 200.0)])
        .address("13", 10.0, 0.0)
        .addr_postcode("19073")
        .load();
    assert_eq!(display_at(&index, 10.0, 0.0),
               "13 Roberts Road, Central Square, Newtown Township, Delaware County, Pennsylvania, 19073, United States");
}

#[test]
fn a_district_inside_its_city_comes_before_the_city() {
    // Bratislava, Astoria Palace (W23752623): the okres Bratislava I (R388234, admin_level 6,
    // place=district) lies inside the city (R1702499, admin_level 5, linked city: 16), so it
    // ranks 18 and comes before the city. Staré Mesto (9, place=suburb) and the place=suburb area
    // of the city's name (W293814124) both rank 20 and hold the point; Staré Mesto's centre is
    // the nearer the street, so the address walk accepts it, and the area is the rank's row, as
    // Nominatim's.
    let index = TestIndex::country("Slovakia", b"SK")
        .boundary("Region of Bratislava", 4, 9, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Bratislava", 5, 1, (-10_000.0, -10_000.0, 10_000.0, 10_000.0)).rank(16)
        .boundary("District of Bratislava I", 6, 11, (-2000.0, -2000.0, 2000.0, 2000.0)).rank(18)
        .boundary("Bratislava-Staré Mesto", 9, 4, (-2000.0, -2000.0, 2000.0, 2000.0)).rank(20)
        .boundary("Bratislava", 15, 4, (-4000.0, -4000.0, 8000.0, 8000.0))
        .place("Tivoli", 5, 100.0, 100.0)
        .street("Hodžovo námestie", &[(0.0, -200.0), (0.0, 200.0)])
        .load();
    assert_eq!(display_at(&index, 0.0, 0.0),
               "Hodžovo námestie, Tivoli, Bratislava, District of Bratislava I, Bratislava, Region of Bratislava, Slovakia");
    assert_eq!(address_at(&index, 0.0, 0.0)["suburb"], "Bratislava");
}

#[test]
fn a_city_area_of_the_country_s_name_is_listed_apart_from_the_country() {
    // Singapore: R17140517 is a place=city area with the country's name and outline (rank 16),
    // round Central Region (admin_level 5, place=borough: 18), River Valley (6, place=suburb:
    // 20) and One Tree Hill (7, place=neighbourhood: 24). The city comes after the borough and
    // the country after the postcode, so the two Singapores are not next to each other and both
    // stay.
    let index = TestIndex::country("Singapore", b"SG")
        .boundary("Singapore", 15, 1, (-50_000.0, -50_000.0, 50_000.0, 50_000.0))
        .boundary("Central Region", 5, 12, (-10_000.0, -10_000.0, 10_000.0, 10_000.0)).rank(18)
        .boundary("River Valley", 6, 4, (-2000.0, -2000.0, 2000.0, 2000.0)).rank(20)
        .boundary("One Tree Hill", 7, 5, (-500.0, -500.0, 500.0, 500.0)).rank(24)
        .street("Jalan Tupai", &[(0.0, -100.0), (0.0, 100.0)])
        .centroid("247856", 50.0, 0.0)
        .load();
    assert_eq!(display_at(&index, 0.0, 0.0),
               "Jalan Tupai, One Tree Hill, River Valley, Central Region, Singapore, 247856, Singapore");
}

#[test]
fn a_suburb_area_comes_between_the_neighbourhood_and_the_city() {
    // Brussels: the Pentagon (R13729034, boundary=place, place=suburb) ranks 20 whatever the
    // boundaries round the point, so it comes after Quays Quarter (admin_level 10 linked to a
    // neighbourhood: 24) and before Brussels (8, linked city: 16). Bruxelles - Brussel (9)
    // repeats Brussels' wikidata and outline: rank 0. Brussels-Capital, the region (4: rank 6)
    // and the arrondissement (7: rank 12), is named once: the two are next to each other.
    let index = TestIndex::country("Belgium", b"BE")
        .boundary("Brussels-Capital", 4, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0)).rank(6)
        .boundary("Brussels-Capital", 7, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0)).rank(12)
        .boundary("Brussels", 8, 1, (-5000.0, -5000.0, 5000.0, 5000.0))
        .boundary("Bruxelles - Brussel", 9, 1, (-5000.0, -5000.0, 5000.0, 5000.0)).rank(0)
        .boundary("Quays Quarter", 10, 5, (-300.0, -300.0, 300.0, 300.0)).rank(24)
        .boundary("Pentagon", 15, 4, (-1500.0, -1500.0, 1500.0, 1500.0))
        .street("Rue Melsens - Melsensstraat", &[(0.0, -100.0), (0.0, 100.0)])
        .load();
    assert_eq!(display_at(&index, 0.0, 0.0),
               "Rue Melsens - Melsensstraat, Quays Quarter, Pentagon, Brussels, Brussels-Capital, Belgium");
}

#[test]
fn a_county_area_round_a_city_comes_after_the_city() {
    // Belfast: County Antrim (R1119534, boundary=historic, place=county) is a place area of rank
    // 12 round Belfast City District (admin_level 7 linked to the city: 16), and Nominatim keeps
    // it at 12, after the city.
    let index = TestIndex::country("United Kingdom", b"GB")
        .boundary("Northern Ireland", 4, 7, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("County Antrim", 15, 10, (-30_000.0, -30_000.0, 30_000.0, 30_000.0))
        .boundary("Belfast City District", 7, 1, (-5000.0, -5000.0, 5000.0, 5000.0))
        .place("Smithfield", 5, 100.0, 0.0)
        .street("Donegall Square North", &[(0.0, -100.0), (0.0, 100.0)])
        .load();
    assert_eq!(display_at(&index, 0.0, 0.0),
               "Donegall Square North, Smithfield, Belfast City District, County Antrim, Northern Ireland, United Kingdom");
}

#[test]
fn a_name_is_left_out_only_where_it_repeats_the_part_before_it() {
    // Trabzon: the city node (place=city, 18 inside Ortahisar, a town at 16) and the province
    // (admin_level 4, place=province: 8) share the name with Ortahisar between them, and
    // Nominatim shows both (results.py display_name).
    let index = TestIndex::country("Turkey", b"TR")
        .boundary("Black Sea Region", 3, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Trabzon", 4, 8, (-30_000.0, -30_000.0, 30_000.0, 30_000.0))
        .boundary("Ortahisar", 6, 2, (-8000.0, -8000.0, 8000.0, 8000.0)).rank(16)
        .boundary("Cumhuriyet Mahallesi", 8, 4, (-300.0, -300.0, 300.0, 300.0)).rank(20)
        .place("Trabzon", 0, 1500.0, 1500.0).promoted()
        .street("Kasım Sokak", &[(0.0, -100.0), (0.0, 100.0)])
        .load();
    assert_eq!(display_at(&index, 0.0, 0.0),
               "Kasım Sokak, Cumhuriyet Mahallesi, Trabzon, Ortahisar, Trabzon, Black Sea Region, Turkey");

    // The Netherlands: the constituent country (admin_level 3, rank 6) has the kingdom's name.
    // The address names it once, as the country; display_name has both, the postcode between.
    let index = TestIndex::country("Netherlands", b"NL")
        .boundary("Netherlands", 3, 0, (-50_000.0, -50_000.0, 50_000.0, 50_000.0))
        .boundary("Drenthe", 4, 7, (-30_000.0, -30_000.0, 30_000.0, 30_000.0))
        .boundary("Hoogeveen", 8, 0, (-8000.0, -8000.0, 8000.0, 8000.0)).rank(14)
        .boundary("Alteveer", 10, 3, (-1000.0, -1000.0, 1000.0, 1000.0)).rank(16)
        .street("Hendrikus Zomerweg", &[(0.0, -100.0), (0.0, 100.0)])
        .centroid("7915 AB", 50.0, 0.0)
        .load();
    assert_eq!(display_at(&index, 0.0, 0.0),
               "Hendrikus Zomerweg, Alteveer, Hoogeveen, Drenthe, Netherlands, 7915 AB, Netherlands");
    assert_eq!(address_at(&index, 0.0, 0.0).get("region"), None);
}

#[test]
fn a_place_area_inside_a_boundary_of_its_rank_comes_before_the_boundary() {
    // Nominatim lifts a place area of rank 16 to 23 that an area of its rank up to 23 contains
    // whole to that area's rank + 2 (placex_triggers.sql:930-950): a place=village area inside a
    // city (admin_level 8, rank 16) ranks 18 and comes before the city, still the village.
    let index = TestIndex::country("France", b"FR")
        .boundary("Bretagne", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Rennes", 8, 0, (-5000.0, -5000.0, 5000.0, 5000.0))
        .boundary("Saint-Laurent", 15, 3, (500.0, 500.0, 1500.0, 1500.0)).rank(18)
        .street("Rue de Fougères", &[(1000.0, 900.0), (1000.0, 1100.0)])
        .load();
    assert_eq!(display_at(&index, 1000.0, 1000.0), "Rue de Fougères, Saint-Laurent, Rennes, Bretagne, France");
    assert_eq!(address_at(&index, 1000.0, 1000.0)["village"], "Saint-Laurent");

    // One that reaches 400 m past the city's edge is not inside it (ST_Contains), though its
    // middle is: it keeps 16, the city's rank. Its centre is the nearer the street, so the
    // address walk accepts it, and the city, holding the point too, is the rank's row.
    let index = TestIndex::country("France", b"FR")
        .boundary("Bretagne", 4, 0, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Rennes", 8, 0, (-5000.0, -5000.0, 5000.0, 5000.0))
        .boundary("Saint-Laurent", 15, 3, (500.0, 3000.0, 1500.0, 5400.0))
        .street("Rue de Fougères", &[(1000.0, 3900.0), (1000.0, 4100.0)])
        .load();
    assert_eq!(display_at(&index, 1000.0, 4000.0), "Rue de Fougères, Rennes, Bretagne, France");
    assert_eq!(address_at(&index, 1000.0, 4000.0).get("village"), None);

    // A place=county area (rank 12) is not lifted, round a smaller boundary of its rank or not.
    let index = TestIndex::country("United Kingdom", b"GB")
        .boundary("England", 4, 7, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Kent", 6, 0, (-30_000.0, -30_000.0, 30_000.0, 30_000.0))
        .boundary("Weald", 15, 10, (-5000.0, -5000.0, 5000.0, 5000.0))
        .street("High Street", &[(0.0, -100.0), (0.0, 100.0)])
        .load();
    assert_eq!(address_at(&index, 0.0, 0.0)["county"], "Kent");
    assert_eq!(display_at(&index, 0.0, 0.0), "High Street, Kent, England, United Kingdom");
}

#[test]
fn a_boundary_that_loses_its_field_to_one_of_another_rank_is_still_listed() {
    // Fredericton, New Brunswick: York County (admin_level 5, linked to a place=county node)
    // ranks 10, as place=county ranks 0 in Canada, and the City of Fredericton (6) ranks 12. Both
    // are labelled county: the address names the finer, as Nominatim's keeps the first row of a
    // label, and display_name lists every row (results.py), York County too.
    let index = TestIndex::country("Canada", b"CA")
        .boundary("New Brunswick", 4, 7, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("York County", 5, 10, (-30_000.0, -30_000.0, 30_000.0, 30_000.0))
        .boundary("City of Fredericton", 6, 0, (-8000.0, -8000.0, 8000.0, 8000.0))
        .street("Queen Street", &[(0.0, -100.0), (0.0, 100.0)])
        .load();
    assert_eq!(display_at(&index, 0.0, 0.0), "Queen Street, City of Fredericton, York County, New Brunswick, Canada");
    assert_eq!(address_at(&index, 0.0, 0.0)["county"], "City of Fredericton");

    // A boundary of the same outline as the one that takes its field is the same place tagged
    // twice, as Nominatim takes out a boundary repeating its parent's wikidata and geometry
    // (placex_triggers.sql:895-899): Bruxelles - Brussel stays out under Brussels.
    let index = TestIndex::country("Belgium", b"BE")
        .boundary("Brussels-Capital", 4, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0))
        .boundary("Brussels", 8, 1, (-5000.0, -5000.0, 5000.0, 5000.0))
        .boundary("Bruxelles - Brussel", 9, 1, (-5000.0, -5000.0, 5000.0, 5000.0)).rank(0)
        .street("Rue Melsens - Melsensstraat", &[(0.0, -100.0), (0.0, 100.0)])
        .load();
    assert_eq!(display_at(&index, 0.0, 0.0), "Rue Melsens - Melsensstraat, Brussels, Brussels-Capital, Belgium");

    // Of one area but another outline, the City of Fredericton is another place: it is listed,
    // and the address names it, the finer row of the label (format_json.py:29-34).
    let mut dataset = TestIndex::country("Canada", b"CA");
    dataset.boundary("New Brunswick", 4, 7, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("York County", 5, 10, (-8000.0, -8000.0, 8000.0, 8000.0))
        .boundary("City of Fredericton", 6, 0, (-6000.0, -6000.0, 10_000.0, 10_000.0))
        .street("Queen Street", &[(0.0, -100.0), (0.0, 100.0)]);
    let area = dataset.polygons[2].0.area;
    dataset.polygons[3].0.area = area;
    let index = dataset.load();
    assert_eq!(address_at(&index, 0.0, 0.0)["county"], "City of Fredericton");
    assert_eq!(display_at(&index, 0.0, 0.0), "Queen Street, City of Fredericton, York County, New Brunswick, Canada");
}

#[test]
fn an_answer_with_no_locality_road_or_country_still_has_a_display_name() {
    // Nominatim joins whatever address rows an answer has: a point where only a state and a
    // county hold it, of a country without a polygon here, shows them.
    let mut index = TestIndex { origin: ORIGIN, ..TestIndex::default() };
    index.boundary("Xinjiang", 4, 7, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Emin County", 6, 10, (-20_000.0, -20_000.0, 20_000.0, 20_000.0));
    let index = index.load();
    assert_eq!(address_at(&index, 0.0, 0.0)["state"], "Xinjiang");
    assert_eq!(display_at(&index, 0.0, 0.0), "Emin County, Xinjiang");
}

#[test]
fn a_place_area_labelled_by_a_linked_node_is_listed_at_its_own_rank() {
    // Weesp (R15419242, boundary=place, place=suburb) has the town node Weesp as its label member:
    // find_linked_place links it, and the link labels the area town, but placex_update keeps a
    // place area of rank 16 to 23 at its own place type's rank, the suburb's 20, which the builder
    // stores. So Weesp comes before Driemond, a village node promoted to 18 inside Amsterdam
    // (admin_level 10, linked city: 16, the ring round Driemond, which overlaps Weesp), and the
    // two Amsterdams meet, as in Nominatim's line.
    let mut dataset = TestIndex::country("Netherlands", b"NL");
    dataset.boundary("North Holland", 4, 7, (-40_000.0, -40_000.0, 40_000.0, 40_000.0))
        .boundary("Amsterdam", 8, 0, (-20_000.0, -20_000.0, 20_000.0, 20_000.0)).rank(14)
        .boundary("Amsterdam", 10, 1, (-1500.0, -3000.0, 1500.0, 1000.0)).rank(16)
        .boundary("Weesp", 15, 2, (-2000.0, -2000.0, 2000.0, 2000.0)).rank(20);
    dataset.place("Driemond", 2, -400.0, 300.0).promoted()
        .street("Maanhof", &[(0.0, -100.0), (0.0, 100.0)]);
    let index = dataset.load();
    assert_eq!(display_at(&index, 0.0, 0.0), "Maanhof, Weesp, Driemond, Amsterdam, North Holland, Netherlands");
    let address = address_at(&index, 0.0, 0.0);
    assert_eq!(address["town"], "Weesp");
    assert_eq!(address["village"], "Driemond");
    assert_eq!(address["city"], "Amsterdam");
    assert_eq!(address["municipality"], "Amsterdam");
}
