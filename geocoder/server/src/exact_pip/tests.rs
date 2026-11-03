use super::*;

/// The rows of a table in geocoder/testdata, which the builder's tests read too: tab-separated
/// fields, with '#' comment lines and the header line left out.
pub(crate) fn testdata_rows(name: &str) -> Vec<Vec<String>> {
    let path = format!("{}/../testdata/{name}", env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    text.lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .skip(1)
        .map(|l| l.split('\t').map(str::to_string).collect())
        .collect()
}

fn e7(field: &str) -> i64 {
    field.parse().unwrap_or_else(|e| panic!("{field}: {e}"))
}

#[test]
fn edge_crosses_as_the_shared_vectors_say_either_way_round() {
    let rows = testdata_rows("pip_edges.tsv");
    assert!(rows.len() > 500);
    let mut crossing = 0;
    for r in &rows {
        assert_eq!(r.len(), 7, "{r:?}");
        let a = PointE7 { lat: e7(&r[0]), lng: e7(&r[1]) };
        let b = PointE7 { lat: e7(&r[2]), lng: e7(&r[3]) };
        let p = PointE7 { lat: e7(&r[4]), lng: e7(&r[5]) };
        let want = r[6] == "1";
        crossing += want as usize;
        assert_eq!(edge_crosses(a, b, p), want, "{r:?}");
        assert_eq!(edge_crosses(b, a, p), want, "{r:?} reversed");
    }
    assert!(crossing > 100 && crossing < rows.len() - 100);
}

#[test]
fn to_e7_rounds_as_the_shared_vectors_say() {
    let rows = testdata_rows("point_e7.tsv");
    assert!(rows.len() > 60);
    for r in &rows {
        assert_eq!(r.len(), 3, "{r:?}");
        let deg = match r[1].as_str() {
            "f64" => r[0].parse::<f64>().unwrap(),
            "f32" => r[0].parse::<f32>().unwrap() as f64,
            kind => panic!("kind {kind}"),
        };
        assert_eq!(to_e7(deg), e7(&r[2]), "{r:?}");
    }
}

#[test]
fn a_point_is_on_an_edge_only_where_the_edge_runs() {
    let (a, b) = (PointE7 { lat: 0, lng: 0 }, PointE7 { lat: 30, lng: 10 });
    for k in 0..=10 {
        let p = PointE7 { lat: 3 * k, lng: k };
        assert!(on_edge(a, b, p) && on_edge(b, a, p), "{p:?}");
        for off in [PointE7 { lat: p.lat + 1, ..p }, PointE7 { lng: p.lng - 1, ..p }] {
            assert!(!on_edge(a, b, off), "{off:?}");
        }
    }
    // On the edge's line, beyond its ends.
    for p in [PointE7 { lat: -3, lng: -1 }, PointE7 { lat: 33, lng: 11 }] {
        assert!(!on_edge(a, b, p), "{p:?}");
    }
    // An edge of no length holds its one point.
    assert!(on_edge(a, a, a) && !on_edge(a, a, b));
}

#[test]
fn far_off_points_cross_nothing_they_should_not() {
    // Products past i64 still compare right: an edge across the globe and points far north and
    // south of it, at its longitudes.
    let (a, b) = (PointE7 { lat: -900_000_000, lng: -1_800_000_000 }, PointE7 { lat: 900_000_000, lng: 1_800_000_000 });
    for lat in [i64::MAX / 4, 1 << 40] {
        assert!(!edge_crosses(a, b, PointE7 { lat, lng: 0 }));
        assert!(edge_crosses(a, b, PointE7 { lat: -lat, lng: 0 }));
    }
}
