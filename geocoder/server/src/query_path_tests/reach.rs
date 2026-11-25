//! How far the street, address and interpolation lookups reach: whatever lies within the search
//! distance of the point, wherever the point lies in its own cell and however the builder lists
//! it, and nothing past it.

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
fn an_interpolation_line_within_the_search_distance_is_found_from_anywhere_in_a_cell_and_none_past_it() {
    // From each corner of a cell and its middle, in every direction: an interpolation line 125 m
    // off is the nearest, one 135 m off none, though a cell the scan reads lists it.
    let cover = geo_cover(DEFAULT_SEARCH_DISTANCE / 6_371_000.0, DEFAULT_STREET_CELL_LEVEL);
    for from in points_round_cell(ORIGIN) {
        let scanned = cells_within(from.0, from.1, DEFAULT_STREET_CELL_LEVEL, cover).ids;
        for bearing in BEARINGS {
            for (metres, found) in [(125.0, true), (135.0, false)] {
                let mut dataset = TestIndex::around(from);
                dataset.interpolation("Main Street", (1, 99, 0), &across(metres, bearing));
                let at = format!("{metres} m along {bearing}° from {from:?}");
                assert!(scanned.iter().any(|cell| dataset.geo_cells.get(cell).is_some_and(|lists| lists[geo_index::INTERPOLATIONS].contains(&0))), "{at}");
                let index = dataset.load();
                let interp = &index.debug_primary(from.0, from.1)["interp"];
                if found {
                    assert!(about(&interp["dist_m"], metres), "{at}: {interp}");
                } else {
                    assert!(interp.is_null(), "{at}: {interp}");
                }
            }
        }
    }
}

#[test]
fn an_interpolation_line_several_cells_list_is_read_once_a_scan() {
    // A line 300 m long through the point, listed in many of the cells the scan reads: the house
    // number lookup reads its name once, not once a cell.
    let mut dataset = TestIndex::new();
    dataset.interpolation("Main Street", (1, 99, 0), &[(0.0, -150.0), (0.0, 150.0)]);
    let cover = geo_cover(DEFAULT_SEARCH_DISTANCE / 6_371_000.0, DEFAULT_STREET_CELL_LEVEL);
    let listing = cells_within(ORIGIN.0, ORIGIN.1, DEFAULT_STREET_CELL_LEVEL, cover).ids.iter()
        .filter(|cell| dataset.geo_cells.get(cell).is_some_and(|lists| lists[geo_index::INTERPOLATIONS].contains(&0)))
        .count();
    assert!(listing > 3, "{listing}");
    let index = dataset.load();
    let before = STRING_READS.with(|n| n.get());
    assert_eq!(index.find_interp_on_way(ORIGIN.0, ORIGIN.1, "Main Street", radians_sq(100.0)), Some(50));
    assert_eq!(STRING_READS.with(|n| n.get()) - before, 1);
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

                    // The answer: the street the road, with the number of its address; or, in the
                    // US, without an address, the number of its interpolation line.
                    let address = TestIndex::around(from)
                        .street("Main Street", &across(20.0, bearing + 180.0))
                        .address("7", north, east)
                        .load();
                    let mut us = TestIndex::around(from);
                    us.polygons[0].0.country_code = u16::from_be_bytes(*b"US");
                    let line = us.street("Main Street", &across(20.0, bearing + 180.0))
                        .interpolation("Main St", (2, 100, 1), &across(metres, bearing))
                        .load();
                    for (index, what) in [(&address, "address"), (&line, "interpolation")] {
                        let answer = serde_json::to_value(index.query(from.0, from.1)).unwrap()["address"].clone();
                        assert_eq!(answer["road"], "Main Street", "{what} {}: {answer}", at(metres));
                        assert_eq!(answer["house_number"].is_string(), found, "{what} {}: {answer}", at(metres));
                    }
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

/// Checks cells_within(lat, lng, level, reach) against S2's own distances over the cells round
/// the point, out to twice the reach: every cell within the reach listed, nearest first, each at
/// its distance, and none past twice the reach.
fn assert_cells_within_as_s2_has_them(lat: f64, lng: f64, level: u64, reach: f64) {
    // A point on a cell's side is as far from the cell as rounding leaves it, up to 6 µm.
    const HAIR: f64 = 1e-12;
    let cells = cells_within(lat, lng, level, reach);
    let at = format!("{} m of ({lat}, {lng}) at level {level}", reach * 6_371_000.0);
    assert!(cells.near.windows(2).all(|w| w[0] <= w[1]), "{at}: not nearest first");
    // The point's own cell, at no distance: on a corner or an edge, beside the others there.
    let own = cells.ids.iter().position(|&id| id == cell_id_at_level(lat, lng, level));
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

/// Points on a face, by an edge and a corner of the cube, by a pole, on the antimeridian.
const SCAN_POINTS: [(f64, f64); 10] = [ORIGIN, CUBE_CORNER, (35.264_389_682_754_654, 45.0), (0.0, 45.0), (45.0, 0.0),
                                       (45.0002, -0.0003), (89.9995, 10.0), (90.0, 0.0), (-33.9, 179.9999), (-33.9, -180.0)];

#[test]
fn cells_within_lists_every_cell_the_reach_meets_nearest_first() {
    for (lat, lng) in SCAN_POINTS {
        for metres in [100.0, 130.0, 660.0] {
            assert_cells_within_as_s2_has_them(lat, lng, DEFAULT_STREET_CELL_LEVEL, metres / 6_371_000.0);
        }
    }
}

#[test]
fn a_street_level_too_coarse_for_the_scan_of_a_point_s_cells_is_refused() {
    // The scan of the cells round a point holds within 5° of it, as far as the cells next to the
    // point's own come at street level 5 and finer, where it lists the cells S2 has round the
    // point. Coarser, from (35°, 44°) by the corner of three faces at level 1, it would miss the
    // point's own cell, and the server refuses to load.
    for level in 5..=8 {
        assert_eq!(geo_reach_refused(DEFAULT_SEARCH_DISTANCE, level), None, "level {level}");
        for (lat, lng) in SCAN_POINTS.into_iter().chain([(35.0, 44.0)]) {
            assert_cells_within_as_s2_has_them(lat, lng, level, geo_cover(DEFAULT_SEARCH_DISTANCE / 6_371_000.0, level));
        }
    }
    let dir = ScratchDir::new();
    TestIndex::new().street("Main Street", &across(20.0, 0.0)).write(dir.path());
    let path = dir.path().to_str().unwrap();
    for level in 0..=4 {
        let refused = geo_reach_refused(DEFAULT_SEARCH_DISTANCE, level).unwrap_or_default();
        assert!(refused.contains(&format!("level {level}")), "{level}: {refused}");
        let refused = Index::load(path, level, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).err().unwrap_or_default();
        assert!(refused.contains("search distance") && refused.contains(&format!("level {level}")), "{level}: {refused}");
        let refused = MultiIndex::load(path, level, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).err().unwrap_or_default();
        assert!(refused.contains("search distance") && refused.contains(&format!("level {level}")), "{level}: {refused}");
    }
}

/// A point of Seattle's Blaine Street, whose edge there the builder lists only in the cells of its
/// two ends, though it crosses the cell of this point.
const BLAINE_STREET: (f64, f64) = (47.6347, -122.348584);

/// A case by corner `k` of the street cell holding `origin`: a point 13 m into the cell along the
/// corner's bisector; an edge square to the bisector 10 m from the corner, which crosses the cell
/// there, from 0.5 m past one side to 10 m more past the other than it runs inside the cell, so
/// that its middle lies in the cell of its far end; and an 8 m line square to the bisector 5 m past
/// the point. The point as (lat, lng), and the nodes of the edge and of the line.
fn corner_case(origin: (f64, f64), k: usize) -> ((f64, f64), [NodeCoord; 2], [NodeCoord; 2]) {
    let cell = Cell::from(CellID(cell_id_at_level(origin.0, origin.1, DEFAULT_STREET_CELL_LEVEL)));
    let vertex = |k: usize| {
        let v = LatLng::from(cell.vertex(k % 4));
        (v.lat.deg(), v.lng.deg())
    };
    let corner = vertex(k);
    // (north, east) in metres from the corner, as at_from lays points out.
    let metres = |(lat, lng): (f64, f64)| {
        ((lat - corner.0) * METRES_PER_DEGREE, (lng - corner.1) * METRES_PER_DEGREE * corner.0.to_radians().cos())
    };
    let unit = |(n, e): (f64, f64)| (n / n.hypot(e), e / n.hypot(e));
    let (u, w) = (unit(metres(vertex(k + 1))), unit(metres(vertex(k + 3))));
    let bisector = unit((u.0 + w.0, u.1 + w.1));
    let cross = |a: (f64, f64), b: (f64, f64)| a.0 * b.1 - a.1 * b.0;
    // Square to the bisector, from w's side towards u's.
    let mut square = (-bisector.1, bisector.0);
    if square.0 * u.0 + square.1 * u.1 < 0.0 {
        square = (-square.0, -square.1);
    }
    // The point `out` along the bisector and `along` square to it.
    let point = |out: f64, along: f64| at_from(corner, out * bisector.0 + along * square.0, out * bisector.1 + along * square.1);
    // How far along the edge's line, 10 m out, it meets the side through the corner along `side`.
    let meets = |side: (f64, f64)| -10.0 * cross(bisector, side) / cross(square, side);
    let (near, far) = (meets(w), meets(u));
    let q = point(13.0, 0.0);
    ((q.lat as f64, q.lng as f64), [point(10.0, near - 0.5), point(10.0, 2.0 * far - near + 10.0)], [point(18.0, -4.0), point(18.0, 4.0)])
}

#[test]
fn a_way_listed_only_in_the_cells_of_its_ends_is_found_from_a_cell_it_crosses() {
    // The builder lists an edge whose middle lies in the cell of one of its ends only in the cells
    // of its two ends (cover_edge), though it may cross a third by their corner, as Blaine Street
    // crosses the cell of BLAINE_STREET. From a point of that cell, by each of its corners: the
    // edge 3 m off is the street, though the point's cell lists a line 5 m off and the cells of
    // the edge's ends lie farther than that; and an interpolation line along the edge gives the
    // house number, as the nearest line and as the line of the street's name.
    for origin in [ORIGIN, CUBE_CORNER, BLAINE_STREET] {
        let own = cell_id_at_level(origin.0, origin.1, DEFAULT_STREET_CELL_LEVEL);
        for k in 0..4 {
            let (q, edge, line) = corner_case(origin, k);
            let at = format!("corner {k} of the cell of {origin:?}");
            // The case: the point in the cell, the edge's ends listed alone and both farther from
            // the point than the line, and its point nearest the point in the cell.
            assert_eq!(cell_id_at_level(q.0, q.1, DEFAULT_STREET_CELL_LEVEL), own, "{at}");
            let ends = edge_cells(edge[0], edge[1]);
            assert!(ends.len() == 2 && !ends.contains(&own), "{at}: {ends:x?}");
            let cos_lat = q.0.to_radians().cos();
            let metres = |line: &[NodeCoord; 2]| {
                let (a, b) = (line[0], line[1]);
                point_to_segment_distance(q.0, q.1, a.lat as f64, a.lng as f64, b.lat as f64, b.lng as f64, cos_lat).sqrt() * 6_371_000.0
            };
            let p = s2::point::Point::from(LatLng::from_degrees(q.0, q.1));
            for &end in &ends {
                let off = s2_distance(&p, &Cell::from(CellID(end))) * 6_371_000.0;
                assert!(off > metres(&line) + 0.5, "{at}: cell {end:x} {off} m off");
            }
            let (a, b) = ((edge[0].lat as f64, edge[0].lng as f64), (edge[1].lat as f64, edge[1].lng as f64));
            let (d, t) = point_to_segment_with_t(q.0, q.1 * cos_lat, a.0, a.1 * cos_lat, b.0, b.1 * cos_lat, PLANAR);
            let foot = (a.0 + t * (b.0 - a.0), a.1 + t * (b.1 - a.1));
            assert_eq!(cell_id_at_level(foot.0, foot.1, DEFAULT_STREET_CELL_LEVEL), own, "{at}");
            assert!(about(&serde_json::json!(d.sqrt() * 6_371_000.0), 3.0), "{at}");

            let streets = TestIndex::around(q)
                .street_through("Inner Street", line.to_vec())
                .street_through("Corner Street", edge.to_vec())
                .load();
            let street = &streets.debug_primary(q.0, q.1)["street"];
            assert!(street["name"] == "Corner Street" && about(&street["dist_m"], 3.0), "{at}: {street}");

            let lines = TestIndex::around(q)
                .interpolation_through("Main Street", (101, 199, 2), line.to_vec())
                .interpolation_through("Main Street", (2, 100, 1), edge.to_vec())
                .load();
            let interp = &lines.debug_primary(q.0, q.1)["interp"];
            assert!(interp["number"].as_u64().is_some_and(|n| n % 2 == 0) && about(&interp["dist_m"], 3.0), "{at}: {interp}");
            let number = lines.find_interp_on_way(q.0, q.1, "Main Street", radians_sq(100.0));
            assert!(number.is_some_and(|n| n % 2 == 0), "{at}: {number:?}");
        }
    }
}

#[test]
fn the_street_found_is_the_nearest_of_every_way_within_the_search_distance() {
    // Short ways listed as the builder lists them, round points by a corner of their cell all over
    // the globe, against every way measured: the street query_geo finds is the nearest within the
    // search distance, and none past it.
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = move || {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        (z ^ (z >> 31)) as f64 / u64::MAX as f64
    };
    for trial in 0..300 {
        let origin = (next() * 120.0 - 60.0, next() * 340.0 - 170.0);
        let cell = Cell::from(CellID(cell_id_at_level(origin.0, origin.1, DEFAULT_STREET_CELL_LEVEL)));
        let corner = LatLng::from(cell.vertex((next() * 4.0) as usize % 4));
        let corner = (corner.lat.deg(), corner.lng.deg());
        let q = at_from(corner, next() * 30.0 - 15.0, next() * 30.0 - 15.0);
        let q = (q.lat as f64, q.lng as f64);
        // Every other case close by the corner, where a way's edges join cells across it.
        let (spread, longest) = if trial % 2 == 0 { (40.0, 60.0) } else { (170.0, 150.0) };
        let mut dataset = TestIndex::around(q);
        let mut ways = Vec::new();
        for w in 0..1 + (next() * 6.0) as usize {
            let (north, east) = (next() * 2.0 * spread - spread, next() * 2.0 * spread - spread);
            let (length, bearing) = (2.0 + next() * longest, next() * 360.0);
            let (dn, de) = along(length, bearing);
            let nodes = vec![at_from(q, north, east), at_from(q, north + dn, east + de)];
            dataset.street_through(&format!("Street {w}"), nodes.clone());
            ways.push(nodes);
        }
        let index = dataset.load();
        let cos_lat = q.0.to_radians().cos();
        let nearest = ways.iter()
            .map(|n| point_to_segment_distance(q.0, q.1, n[0].lat as f64, n[0].lng as f64, n[1].lat as f64, n[1].lng as f64, cos_lat))
            .enumerate()
            .filter(|&(_, d)| d < radians_sq(DEFAULT_SEARCH_DISTANCE))
            .min_by(|a, b| a.1.total_cmp(&b.1));
        let street = &index.debug_primary(q.0, q.1)["street"];
        let at = format!("trial {trial} at {q:?}: {street}");
        match nearest {
            Some((id, _)) => assert_eq!(street["id"], id, "{at}"),
            None => assert!(street.is_null(), "{at}"),
        }
    }
}

#[test]
fn a_long_edge_is_found_where_the_builder_lists_its_great_circle() {
    // The builder lists a long edge in the cells its great circle crosses, but the scans measure to
    // its line in degrees: at 60° N, a way along the parallel from 0.55° W to 0.55° E bows 127 m
    // north of it in the cells. On the parallel it is the road at no distance, though a street 1 m
    // off is listed in the point's own cell and the long way is not.
    let origin = (60.0, 0.0);
    let mut dataset = TestIndex::around(origin);
    dataset.street_through("Long Road", vec![NodeCoord { lat: 60.0, lng: -0.55 }, NodeCoord { lat: 60.0, lng: 0.55 }])
        .street("Short Street", &across(1.0, 0.0));
    let own = cell_id_at_level(origin.0, origin.1, DEFAULT_STREET_CELL_LEVEL);
    assert!(!dataset.geo_cells[&own][geo_index::STREETS].contains(&0));
    let index = dataset.load();
    let street = &index.debug_primary(origin.0, origin.1)["street"];
    assert!(street["name"] == "Long Road" && about(&street["dist_m"], 0.0), "{street}");
}

#[test]
fn streets_addresses_and_house_numbers_are_found_across_the_antimeridian() {
    // On the equator, 8 steps of a node's longitude (2^-16°, 1.7 m) west of the antimeridian: a
    // street along the meridian 5 steps east of it, an address node 12 steps east and a building
    // from 2 to 8 steps east, each as near as it is across it; and an interpolation line along the
    // street gives the street its house number.
    let step = (-16f64).exp2();
    let metres = |steps: f64| (steps * step).to_radians() * 6_371_000.0;
    let q = (0.0, 180.0 - 8.0 * step);
    let node = |lat: f64, steps_east: f64| NodeCoord { lat: lat as f32, lng: (-180.0 + steps_east * step) as f32 };
    let street = vec![node(-0.0005, 5.0), node(0.0005, 5.0)];
    let index = TestIndex::new()
        .street_through("Date Line Road", street.clone())
        .address_point("3", node(0.0, 12.0), None)
        .interpolation_through("Date Line Road", (1, 99, 0), street)
        .load();
    let found = index.debug_primary(q.0, q.1);
    assert!(found["street"]["name"] == "Date Line Road" && about(&found["street"]["dist_m"], metres(13.0)), "{found}");
    assert!(found["addr"]["house_number"] == "3" && about(&found["addr"]["dist_m"], metres(20.0)), "{found}");
    assert!(found["interp"]["number"] == 50 && about(&found["interp"]["dist_m"], metres(13.0)), "{found}");
    assert_eq!(index.find_addr_on_way(q.0, q.1, 0, radians_sq(100.0)).map(|(id, _)| id), Some(0));
    assert_eq!(index.find_interp_on_way(q.0, q.1, "Date Line Road", radians_sq(100.0)), Some(50));

    let ring = [node(-0.00005, 2.0), node(-0.00005, 8.0), node(0.00005, 8.0), node(0.00005, 2.0), node(-0.00005, 2.0)];
    let index = TestIndex::new().address_point("5", node(0.0, 5.0), Some(Footprint::of(&ring))).load();
    let addr = &index.debug_primary(q.0, q.1)["addr"];
    assert!(addr["house_number"] == "5" && about(&addr["dist_m"], metres(10.0)), "{addr}");

    // A street and an interpolation line 16 steps north of the equator, from 24 steps west of the
    // antimeridian to 24 steps east of it: 16 steps off, the point a third of the way along, so
    // number 34 of 1 to 99.
    let lat = 16.0 * step;
    let seam = vec![NodeCoord { lat: lat as f32, lng: (180.0 - 24.0 * step) as f32 }, node(lat, 24.0)];
    let index = TestIndex::new()
        .street_through("Seam Road", seam.clone())
        .interpolation_through("Seam Road", (1, 99, 0), seam)
        .load();
    let found = index.debug_primary(q.0, q.1);
    assert!(found["street"]["name"] == "Seam Road" && about(&found["street"]["dist_m"], metres(16.0)), "{found}");
    assert!(found["interp"]["number"] == 34 && about(&found["interp"]["dist_m"], metres(16.0)), "{found}");
    assert_eq!(index.find_interp_on_way(q.0, q.1, "Seam Road", radians_sq(100.0)), Some(34));
}

#[test]
fn an_address_and_a_street_over_a_pole_are_as_far_as_they_are() {
    // 64 steps of a node's latitude (2^-17°, 0.85 m) from the North Pole: an address node as far
    // past it on the other side is 108.6 m off, where a degree of longitude of the point's would
    // put it 171 m off; and a street from 90° E to 90° W across the pole passes 54.3 m off, though
    // in degrees it runs through the point.
    let lat = 90.0 - 64.0 * (-17f64).exp2();
    let metres = |degrees: f64| degrees.to_radians() * 6_371_000.0;
    let node = |lng: f32| NodeCoord { lat: lat as f32, lng };
    let index = TestIndex::new()
        .address_point("1", node(180.0), None)
        .street_through("Pole Road", vec![node(90.0), node(-90.0)])
        .load();
    let found = index.debug_primary(lat, 0.0);
    assert!(found["addr"]["house_number"] == "1" && about(&found["addr"]["dist_m"], metres(2.0 * (90.0 - lat))), "{found}");
    assert!(found["street"]["name"] == "Pole Road" && about(&found["street"]["dist_m"], metres(90.0 - lat)), "{found}");
}

#[test]
fn an_interpolation_number_by_a_pole_is_along_its_great_circle() {
    // By the pole, where the scans measure on the gnomonic projection about the point: a line
    // numbered 1 to 3001 along the meridian from 84° N to the pole, at 89° N, five sixths of the
    // way along it, is number 2501; though five sixths of the way along its projection,
    // tan 5° / (tan 5° + tan 1°), would be 2502.
    let q = (89.0, 0.0);
    let line = vec![NodeCoord { lat: 84.0, lng: 0.0 }, NodeCoord { lat: 90.0, lng: 0.0 }];
    let index = TestIndex::around(q).interpolation_through("Pole Road", (1, 3001, 0), line).load();
    assert_eq!(index.find_interp_on_way(q.0, q.1, "Pole Road", radians_sq(100.0)), Some(2501));
    let interp = &index.debug_primary(q.0, q.1)["interp"];
    assert!(interp["number"] == 2501 && about(&interp["dist_m"], 0.0), "{interp}");
}

#[test]
fn an_interpolation_line_is_as_far_as_its_point_nearest_in_metres() {
    // At 60° N, a line running north-east whose nearest point lies 95 m south-east of the point,
    // numbered 1 to 301 along its 300 m: a street of its name takes its number from it, within
    // 100 m, though the line's point nearest in degrees lies 57 m along it and 111 m off. The
    // number is the one at that point, 94, where Nominatim's ST_LineLocatePoint on the lon/lat
    // line takes it. At 120 m the line is the nearest within the search distance, though 140 m
    // off at its point nearest in degrees, which gives number 79.
    let origin = (60.0, 10.0);
    for (metres, within, expected) in [(95.0, 100.0, 94), (120.0, DEFAULT_SEARCH_DISTANCE, 79)] {
        let foot = along(metres, 135.0);
        let (dn, de) = along(150.0, 45.0);
        let index = TestIndex::around(origin)
            .interpolation("Main Street", (1, 301, 0), &[(foot.0 - dn, foot.1 - de), (foot.0 + dn, foot.1 + de)])
            .load();
        let number = index.find_interp_on_way(origin.0, origin.1, "Main Street", radians_sq(within));
        assert_eq!(number, Some(expected), "{metres} m");
        let interp = &index.debug_primary(origin.0, origin.1)["interp"];
        let off = metres * 6_371_000.0_f64.to_radians() / METRES_PER_DEGREE;
        assert!(interp["number"] == expected && about(&interp["dist_m"], off), "{metres} m: {interp}");
    }
}

#[test]
fn an_interpolation_number_is_where_its_line_is_nearest_in_degrees() {
    // At 60° N, a line numbered 1 to 301 running 0.0018° east along 60.0009° N from 0.0006° W,
    // then 0.0018° south. Nominatim's ST_LineLocatePoint on the lon/lat line takes the point
    // nearest in degrees, 0.0009° north on the first leg (the second passes 0.0012° east), a
    // sixth of the way along the line in degrees, so number 51; though in metres the second leg
    // is the nearer, 67 m east against 100 m north, and the first leg a third as long as the
    // second.
    let origin = (60.0, 0.0);
    let node = |lat: f64, lng: f64| NodeCoord { lat: lat as f32, lng: lng as f32 };
    let line = vec![node(60.0009, -0.0006), node(60.0009, 0.0012), node(59.9991, 0.0012)];
    let index = TestIndex::around(origin).interpolation_through("Main Street", (1, 301, 0), line).load();
    assert_eq!(index.find_interp_on_way(origin.0, origin.1, "Main Street", radians_sq(100.0)), Some(51));
    let interp = &index.debug_primary(origin.0, origin.1)["interp"];
    assert!(interp["number"] == 51 && about(&interp["dist_m"], 0.0006_f64.to_radians() * 6_371_000.0), "{interp}");
}

#[test]
fn a_building_round_the_point_is_found_at_any_search_distance() {
    // With a 30 m search distance, a building the point lies in whose own point lies 80 m east, in
    // a cell past 30 m of the point: the address at no distance, as the cells round the point's
    // own are read at any search distance.
    let q = points_round_cell(ORIGIN)[4];
    let mut dataset = TestIndex::around(q);
    dataset.building("9", (-10.0, -10.0, 10.0, 170.0));
    let index = dataset.load_reaching(30.0);
    let middle = at_from(q, 0.0, 80.0);
    let cells = cells_within(q.0, q.1, DEFAULT_STREET_CELL_LEVEL, 30.0 / 6_371_000.0);
    assert!(!cells.ids.contains(&cell_id_at_level(middle.lat as f64, middle.lng as f64, DEFAULT_STREET_CELL_LEVEL)));
    let addr = &index.debug_primary(q.0, q.1)["addr"];
    assert!(addr["house_number"] == "9" && about(&addr["dist_m"], 0.0), "{addr}");
}

#[test]
fn a_search_distance_past_what_a_query_may_read_is_refused() {
    // At the street cells' level, a query reads every cell within the search distance: 660 m
    // (Nominatim's 0.006°) reads a few hundred, 100 km millions, which the server refuses to load
    // with, as it does a distance that is no distance: a flat layout, and one of continents,
    // whose continents that can't load it would otherwise leave empty.
    let dir = ScratchDir::new();
    TestIndex::new().street("Main Street", &across(20.0, 0.0)).write(dir.path());
    let path = dir.path().to_str().unwrap();
    let load = |metres: f64| Index::load(path, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, metres);
    let continents = two_continents();
    let root = continents.path().to_str().unwrap();
    let load_all = |metres: f64| MultiIndex::load(root, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, metres);
    assert!(load(660.0).is_ok());
    assert_eq!(load_all(660.0).map(|all| all.loaded_count()), Ok(2));
    for metres in [100_000.0, f64::INFINITY, f64::NAN, 0.0, -5.0] {
        let refused = load(metres).err().unwrap_or_default();
        assert!(refused.contains("search distance"), "{metres}: {refused}");
        let refused = MultiIndex::load(path, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, metres).err().unwrap_or_default();
        assert!(refused.contains("search distance"), "{metres}: {refused}");
        let refused = load_all(metres).err().unwrap_or_default();
        assert!(refused.contains("search distance"), "continents, {metres}: {refused}");
    }
}

#[test]
fn no_two_points_of_a_cell_lie_farther_apart_than_s2s_longest_diagonal() {
    // The listing gap rests on S2's kMaxDiag: over cells on a face, by an edge and a corner of the
    // cube and by a pole, at the street level and others, no two corners of a cell (between which
    // its points lie farthest apart) are farther apart than S2_MAX_DIAG times 2^-level.
    for (lat, lng) in [ORIGIN, CUBE_CORNER, (0.0, 45.0), (45.0, 0.0), (89.9995, 10.0), (-35.26, -135.0), (0.0, 0.0), (10.0, 44.99)] {
        for level in [10, 14, 17, 20] {
            let id = CellID(cell_id_at_level(lat, lng, level));
            for cell in std::iter::once(id).chain(id.edge_neighbors()).map(Cell::from) {
                let corners: Vec<_> = (0..4).map(|k| cell.vertex(k)).collect();
                for a in &corners {
                    for b in &corners {
                        let apart = a.distance(b).rad();
                        assert!(apart <= S2_MAX_DIAG * (-(level as f64)).exp2(), "({lat}, {lng}) level {level}: {apart}");
                    }
                }
            }
        }
    }
}
