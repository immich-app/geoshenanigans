// Reverse-geocode latency bench for one flat build dir. Reads "lat lng" lines on stdin, runs
// one untimed pass (page cache warm), then `passes` timed passes; each point keeps its fastest
// time. Prints one JSON line with the percentiles of those per-point times (µs).
//
//   rg_bench <dir> [query|admin] [passes]
//
// query = MultiIndex::query_json (what the server answers); admin = Index::find_admin only.
use query_server::{Index, MultiIndex, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL};
use std::io::BufRead;
use std::time::Instant;

fn pct(sorted: &[u64], q: f64) -> f64 {
    sorted[((sorted.len() - 1) as f64 * q).round() as usize] as f64 / 1e3
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("usage: rg_bench <dir> [query|admin] [passes]");
    let mode = args.next().unwrap_or_else(|| "query".into());
    let passes: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(3);
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
    let admin = mode == "admin";
    let run = |lat: f64, lng: f64| {
        if admin {
            std::hint::black_box(single.find_admin(lat, lng));
        } else {
            std::hint::black_box(multi.query_json(lat, lng));
        }
    };
    for &(lat, lng) in &pts {
        run(lat, lng);
    }
    let mut best = vec![u64::MAX; pts.len()];
    let mut pass_s = Vec::new();
    for _ in 0..passes {
        let t0 = Instant::now();
        for (i, &(lat, lng)) in pts.iter().enumerate() {
            let t = Instant::now();
            run(lat, lng);
            best[i] = best[i].min(t.elapsed().as_nanos() as u64);
        }
        pass_s.push(t0.elapsed().as_secs_f64());
    }
    let mut s = best.clone();
    s.sort_unstable();
    let mean = s.iter().sum::<u64>() as f64 / s.len() as f64 / 1e3;
    let fastest = pass_s.iter().cloned().fold(f64::MAX, f64::min);
    println!(
        "{{\"mode\":\"{mode}\",\"points\":{},\"mean_us\":{mean:.2},\"p50_us\":{:.2},\"p90_us\":{:.2},\"p99_us\":{:.2},\"max_us\":{:.1},\"fastest_pass_us_per_point\":{:.2}}}",
        s.len(),
        pct(&s, 0.5),
        pct(&s, 0.9),
        pct(&s, 0.99),
        *s.last().unwrap() as f64 / 1e3,
        fastest * 1e6 / s.len() as f64
    );
}
