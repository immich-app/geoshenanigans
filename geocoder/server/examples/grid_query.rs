// Differential query harness for validating builds end-to-end through
// the real read path. Loads a flat single-region build directory and
// runs query_json() for each "lat lng" line on stdin, printing
// "lat lng <json>" per line. Diff the output of two builds (e.g. a
// chained build vs its fresh ground-truth rebuild) to catch any
// divergence in the resolved admin / road / postcode / POI fields —
// the read-path coverage that byte-level patch-verify cannot give.
//
//   cargo build --release --example grid_query
//   ./target/release/examples/grid_query <build>/planet/full < coords.txt > out.jsonl
//
// coords.txt: whitespace-separated "lat lng" per line.
use query_server::{
    MultiIndex, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL,
};
use std::io::{BufRead, BufWriter, Write};

fn main() {
    let dir = std::env::args()
        .nth(1)
        .expect("usage: grid_query <build-dir> < coords.txt");
    let idx = MultiIndex::load(
        &dir,
        DEFAULT_STREET_CELL_LEVEL,
        DEFAULT_ADMIN_CELL_LEVEL,
        DEFAULT_SEARCH_DISTANCE,
    )
    .expect("failed to load index");

    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for line in stdin.lock().lines() {
        let line = line.unwrap();
        let mut it = line.split_whitespace();
        let (Some(a), Some(b)) = (it.next(), it.next()) else { continue };
        let (lat, lng): (f64, f64) = match (a.parse(), b.parse()) {
            (Ok(la), Ok(ln)) => (la, ln),
            _ => continue,
        };
        // Print coords with fixed precision so two runs key identically.
        writeln!(out, "{:.6} {:.6} {}", lat, lng, idx.query_json(lat, lng)).unwrap();
    }
}
