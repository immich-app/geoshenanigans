// Admin read-path work per reverse lookup, on a reader patched by instrument.py. Reads "lat lng"
// lines on stdin; runs find_admin over all points, then query_json; prints one JSON line of
// per-point means: cell index lookups (all cell indexes), admin polygon records read, admin rings
// whose vertices were read, and those vertices.
//
//   rg_count <dir>
use query_server::{Index, MultiIndex, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL};
use query_server::{RG_LOOKUPS, RG_RECORDS, RG_RINGS, RG_VERTS};
use std::io::BufRead;
use std::sync::atomic::Ordering::Relaxed;

fn take() -> [u64; 4] {
    [RG_LOOKUPS.swap(0, Relaxed), RG_RECORDS.swap(0, Relaxed), RG_RINGS.swap(0, Relaxed), RG_VERTS.swap(0, Relaxed)]
}

fn main() {
    let dir = std::env::args().nth(1).expect("usage: rg_count <dir>");
    let pts: Vec<(f64, f64)> = std::io::stdin()
        .lock()
        .lines()
        .filter_map(|l| {
            let l = l.ok()?;
            let mut it = l.split_whitespace();
            Some((it.next()?.parse().ok()?, it.next()?.parse().ok()?))
        })
        .collect();
    let multi = MultiIndex::load(&dir, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).expect("load");
    let single = Index::load(&dir, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).expect("load");
    let n = pts.len() as f64;
    take();
    for &(lat, lng) in &pts {
        std::hint::black_box(single.find_admin(lat, lng));
    }
    let a = take();
    for &(lat, lng) in &pts {
        std::hint::black_box(multi.query_json(lat, lng));
    }
    let q = take();
    let f = |c: [u64; 4]| format!("{{\"lookups\":{:.2},\"records\":{:.1},\"rings\":{:.2},\"vertices\":{:.0}}}", c[0] as f64 / n, c[1] as f64 / n, c[2] as f64 / n, c[3] as f64 / n);
    println!("{{\"points\":{},\"find_admin\":{},\"query_json\":{}}}", pts.len(), f(a), f(q));
}
