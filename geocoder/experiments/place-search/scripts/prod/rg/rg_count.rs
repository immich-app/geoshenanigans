// Admin ring work per query, on a reader patched by instrument.py (counters in
// Index::admin_verts and Index::admin_pip). Reads "lat lng" lines on stdin, runs find_admin and
// query_json once per point, prints one JSON line of per-point means.
//
//   rg_count <dir>
use query_server::{Index, MultiIndex, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL};
use query_server::{RG_RING_CALLS, RG_RING_VERTS};
use std::io::BufRead;
use std::sync::atomic::Ordering::Relaxed;

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
    for &(lat, lng) in &pts {
        std::hint::black_box(single.find_admin(lat, lng));
    }
    let (ac, av) = (RG_RING_CALLS.swap(0, Relaxed), RG_RING_VERTS.swap(0, Relaxed));
    for &(lat, lng) in &pts {
        std::hint::black_box(multi.query_json(lat, lng));
    }
    let (qc, qv) = (RG_RING_CALLS.swap(0, Relaxed), RG_RING_VERTS.swap(0, Relaxed));
    println!(
        "{{\"points\":{},\"find_admin_rings\":{:.2},\"find_admin_vertices\":{:.1},\"query_rings\":{:.2},\"query_vertices\":{:.1}}}",
        pts.len(),
        ac as f64 / n,
        av as f64 / n,
        qc as f64 / n,
        qv as f64 / n
    );
}
