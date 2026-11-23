//! How far the street, address and interpolation lookups reach: every cell within the search
//! distance of the point, wherever the point lies in its own cell, and nothing past it.

use super::*;
use s2::latlng::LatLng;

/// A point some 60 m from the corner of S2's cube where faces 0, 1 and 2 meet (35.26° N, 45° E),
/// whose street cells lie on all three.
const CUBE_CORNER: (f64, f64) = (35.2640, 44.9995);

/// The directions looked in from each point, as bearings in degrees clockwise from north.
const BEARINGS: [f64; 8] = [0.0, 45.0, 90.0, 135.0, 180.0, 225.0, 270.0, 315.0];

/// The corners of the street cell holding `origin`, each moved 1 % of the way to its middle, and
/// its middle: the points of the cell nearest each of its sides and corners.
fn points_round_cell(origin: (f64, f64)) -> Vec<(f64, f64)> {
    let id = cell_id_at_level(origin.0, origin.1, DEFAULT_STREET_CELL_LEVEL);
    let cell = Cell::from(CellID(id));
    let middle = LatLng::from(cell.center());
    let (mlat, mlng) = (middle.lat.deg(), middle.lng.deg());
    let points: Vec<(f64, f64)> = (0..4)
        .map(|k| LatLng::from(cell.vertex(k)))
        .map(|c| (c.lat.deg() + (mlat - c.lat.deg()) * 0.01, c.lng.deg() + (mlng - c.lng.deg()) * 0.01))
        .chain(std::iter::once((mlat, mlng)))
        .collect();
    for &(lat, lng) in &points {
        assert_eq!(cell_id_at_level(lat, lng, DEFAULT_STREET_CELL_LEVEL), id, "({lat}, {lng})");
    }
    points
}

/// (metres north, metres east) `metres` from the origin along `bearing`.
fn along(metres: f64, bearing: f64) -> (f64, f64) {
    let (east, north) = bearing.to_radians().sin_cos();
    (metres * north, metres * east)
}

/// An 80 m line square to `bearing` whose point nearest the origin lies `metres` along it.
fn across(metres: f64, bearing: f64) -> [(f64, f64); 2] {
    let (foot, (east, north)) = (along(metres, bearing), bearing.to_radians().sin_cos());
    [(foot.0 - 40.0 * east, foot.1 + 40.0 * north), (foot.0 + 40.0 * east, foot.1 - 40.0 * north)]
}

/// The road of the answer at (lat, lng).
fn road_at(index: &Index, (lat, lng): (f64, f64)) -> serde_json::Value {
    serde_json::to_value(index.query(lat, lng)).unwrap()["address"]["road"].clone()
}

#[test]
fn a_street_within_the_search_distance_is_found_from_anywhere_in_a_cell_and_none_past_it() {
    // From each corner of a cell and its middle, in every direction: a street 125 m off is the
    // road, one 135 m off none, though a cell next to the point's reaches past it.
    for origin in [ORIGIN, CUBE_CORNER] {
        for from in points_round_cell(origin) {
            for bearing in BEARINGS {
                for (metres, found) in [(125.0, true), (135.0, false)] {
                    let index = TestIndex::around(from).street("Main Street", &across(metres, bearing)).load();
                    let street = &index.debug_primary(from.0, from.1)["street"];
                    let road = road_at(&index, from);
                    let at = format!("{metres} m along {bearing}° from {from:?}: {street}");
                    if found {
                        assert!(about(&street["dist_m"], metres), "{at}");
                        assert_eq!(road, "Main Street", "{at}");
                    } else {
                        assert!(street.is_null() && road.is_null(), "{at}");
                    }
                }
            }
        }
    }
}

#[test]
fn the_nearer_of_two_streets_wins_whichever_cell_lists_the_farther() {
    // A street 60 m off one way and another 110 m off another way, from each corner of a cell and
    // its middle: the nearer is the road, though the farther comes first in record order.
    for origin in [ORIGIN, CUBE_CORNER] {
        for from in points_round_cell(origin) {
            for bearing in BEARINGS {
                for turn in [90.0, 180.0, 270.0] {
                    let index = TestIndex::around(from)
                        .street("Far Street", &across(110.0, bearing))
                        .street("Near Street", &across(60.0, bearing + turn))
                        .load();
                    let street = &index.debug_primary(from.0, from.1)["street"];
                    let at = format!("110 m along {bearing}°, 60 m along {}° from {from:?}: {street}", bearing + turn);
                    assert!(street["name"] == "Near Street" && about(&street["dist_m"], 60.0), "{at}");
                    assert_eq!(road_at(&index, from), "Near Street", "{at}");
                }
            }
        }
    }
}

#[test]
fn addresses_and_house_numbers_are_found_within_their_reach_from_anywhere_in_a_cell() {
    // From each corner of a cell and its middle, in every direction: an address node 125 m off is
    // the nearest address, one 135 m off none. A street takes its house number from an address of
    // it, or an interpolation line of its name, within 100 m (Nominatim's 0.001°): one 95 m off,
    // not one 105 m off.
    for origin in [ORIGIN, CUBE_CORNER] {
        for from in points_round_cell(origin) {
            for bearing in BEARINGS {
                let at = |metres: f64| format!("{metres} m along {bearing}° from {from:?}");
                for (metres, found) in [(125.0, true), (135.0, false)] {
                    let (north, east) = along(metres, bearing);
                    let index = TestIndex::around(from).address("7", north, east).load();
                    let addr = &index.debug_primary(from.0, from.1)["addr"];
                    assert_eq!(!addr.is_null(), found, "{}: {addr}", at(metres));
                }
                for (metres, found) in [(95.0, true), (105.0, false)] {
                    let (north, east) = along(metres, bearing);
                    let index = TestIndex::around(from)
                        .street("Main Street", &across(20.0, bearing + 180.0))
                        .address("7", north, east)
                        .interpolation("Main St", (2, 100, 1), &across(metres, bearing))
                        .load();
                    let number = index.find_addr_on_way(from.0, from.1, 0, radians_sq(100.0));
                    assert_eq!(number.is_some(), found, "address {}", at(metres));
                    let interpolated = index.find_interp_on_way(from.0, from.1, "Main Street", radians_sq(100.0));
                    assert_eq!(interpolated.is_some(), found, "interpolation {}", at(metres));
                }
            }
        }
    }
}

/// The distance in radians from `p` to the cell, 0 inside it, by S2's own measure.
fn s2_distance(p: &s2::point::Point, cell: &Cell) -> f64 {
    if cell.contains_point(p) {
        return 0.0;
    }
    (0..4).map(|k| s2::edgeutil::distance_from_segment(p, &cell.vertex(k), &cell.vertex((k + 1) % 4)).rad()).fold(f64::MAX, f64::min)
}

#[test]
fn cells_within_lists_every_cell_the_reach_meets_nearest_first() {
    // Against S2's own distances over the cells round the point, out to twice the reach: on a
    // face, by an edge and a corner of the cube, by a pole, on the antimeridian.
    let points = [ORIGIN, CUBE_CORNER, (35.264_389_682_754_654, 45.0), (0.0, 45.0), (45.0, 0.0), (45.0002, -0.0003),
                  (89.9995, 10.0), (90.0, 0.0), (-33.9, 179.9999), (-33.9, -180.0)];
    for (lat, lng) in points {
        for metres in [100.0, 130.0, 660.0] {
            // A point on a cell's side is as far from the cell as rounding leaves it, up to 6 µm.
            const HAIR: f64 = 1e-12;
            let reach = metres / 6_371_000.0;
            let cells = cells_within(lat, lng, DEFAULT_STREET_CELL_LEVEL, reach);
            let at = format!("{metres} m of ({lat}, {lng})");
            assert!(cells.near.windows(2).all(|w| w[0] <= w[1]), "{at}: not nearest first");
            // The point's own cell, at no distance: on a corner or an edge, beside the others there.
            let own = cells.ids.iter().position(|&id| id == cell_id_at_level(lat, lng, DEFAULT_STREET_CELL_LEVEL));
            assert!(own.is_some_and(|i| cells.near[i] <= HAIR * HAIR), "{at}");

            // The cells round the point, each with S2's distance to it.
            let p = s2::point::Point::from(LatLng::from_degrees(lat, lng));
            let mut round = BTreeMap::new();
            let mut todo = vec![CellID(cells.ids[0])];
            while let Some(id) = todo.pop() {
                if round.contains_key(&id.0) { continue; }
                let d = s2_distance(&p, &Cell::from(id));
                round.insert(id.0, d);
                if d <= 2.0 * reach { todo.extend(id.edge_neighbors()); }
            }
            for (&id, &d) in &round {
                let listed = cells.ids.iter().position(|&c| c == id);
                if d <= reach {
                    assert!(listed.is_some(), "{at}: cell {id:x} at {} m", d * 6_371_000.0);
                }
                if let Some(i) = listed {
                    assert!(d <= reach * 1.002, "{at}: cell {id:x} at {} m", d * 6_371_000.0);
                    assert!(cells.near[i] <= (d + HAIR).powi(2) && cells.near[i] >= (d * 0.998).powi(2), "{at}: cell {id:x}");
                }
            }
            assert!(cells.ids.iter().all(|id| round.contains_key(id)), "{at}: a cell past twice the reach");
        }
    }
}
