// Scratch probe (not committed): for each "lat lng" line on stdin print one JSON line with the
// answer, the place-node debug and the admin debug of the flat dir.
//   parity_probe <flat-dir> < coords
use query_server::{Index, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL};
use std::io::BufRead;

fn main() {
    let dir = std::env::args().nth(1).expect("usage: parity_probe <dir>");
    let idx = Index::load(&dir, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE)
        .expect("load");
    for line in std::io::stdin().lock().lines() {
        let line = line.unwrap();
        let mut it = line.split_whitespace();
        let (Some(a), Some(b)) = (it.next(), it.next()) else { continue };
        let (lat, lng): (f64, f64) = (a.parse().unwrap(), b.parse().unwrap());
        let ans = serde_json::to_value(idx.query(lat, lng)).unwrap();
        println!("{}", serde_json::json!({
            "lat": lat, "lng": lng,
            "answer": ans["address"],
            "places": idx.debug_places(lat, lng),
            "admin": idx.debug_admin(lat, lng)["picked_per_level"],
            "primary": idx.debug_primary(lat, lng),
        }));
    }
}
