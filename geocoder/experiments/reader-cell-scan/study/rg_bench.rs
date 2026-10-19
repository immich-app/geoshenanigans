// Reverse-geocode latency for one flat set dir. Reads "lat lng" lines on stdin.
//
//   rg_bench <dir> <query|admin> <passes>
//
// passes >= 1 (warm): one untimed pass, then `passes` timed passes; each point keeps its fastest
// time. passes == 0 (cold): one timed pass with no warm-up, each point timed once (the caller
// evicts the set's files from the page cache first).
// query = MultiIndex::query_json (the server's answer); admin = Index::find_admin alone.
// Prints one JSON line: mean / p50 / p90 / p99 / max of the per-point times, in µs.
use query_server::{Index, MultiIndex, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL};
use std::io::BufRead;
use std::time::Instant;

fn pct(sorted: &[u64], q: f64) -> f64 {
    sorted[((sorted.len() - 1) as f64 * q).round() as usize] as f64 / 1e3
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("usage: rg_bench <dir> <query|admin> <passes>");
    let mode = args.next().expect("mode");
    let passes: usize = args.next().expect("passes").parse().unwrap();
    let pts: Vec<(f64, f64)> = std::io::stdin()
        .lock()
        .lines()
        .filter_map(|l| {
            let l = l.ok()?;
            let mut it = l.split_whitespace();
            Some((it.next()?.parse().ok()?, it.next()?.parse().ok()?))
        })
        .collect();
    let admin = mode == "admin";
    let multi = (!admin).then(|| MultiIndex::load(&dir, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).expect("load"));
    let single = admin.then(|| Index::load(&dir, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).expect("load"));
    let run = |lat: f64, lng: f64| match (&single, &multi) {
        (Some(s), _) => {
            std::hint::black_box(s.find_admin(lat, lng));
        }
        (_, Some(m)) => {
            std::hint::black_box(m.query_json(lat, lng));
        }
        _ => unreachable!(),
    };
    let timed_passes = passes.max(1);
    if passes > 0 {
        for &(lat, lng) in &pts {
            run(lat, lng);
        }
    }
    let mut best = vec![u64::MAX; pts.len()];
    for _ in 0..timed_passes {
        for (i, &(lat, lng)) in pts.iter().enumerate() {
            let t = Instant::now();
            run(lat, lng);
            best[i] = best[i].min(t.elapsed().as_nanos() as u64);
        }
    }
    let mut s = best;
    s.sort_unstable();
    let mean = s.iter().sum::<u64>() as f64 / s.len() as f64 / 1e3;
    println!(
        "{{\"mode\":\"{mode}\",\"passes\":{passes},\"points\":{},\"mean_us\":{mean:.2},\"p50_us\":{:.2},\"p90_us\":{:.2},\"p99_us\":{:.2},\"max_us\":{:.1}}}",
        s.len(),
        pct(&s, 0.5),
        pct(&s, 0.9),
        pct(&s, 0.99),
        *s.last().unwrap() as f64 / 1e3
    );
}
