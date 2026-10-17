//! place-search <corpus.tsv> <context_aliases.tsv> [--bench <repeats>] [--limit <n>] [--dump <dir>]
//!
//! Builds the index from the corpus, prints build stats (stderr), then reads one query per
//! stdin line. Default: one JSON line per query with the results and the query's latency.
//! --bench: runs the whole query list <repeats> times and prints latency percentiles.
//! --dump: writes the index parts (fst, postings, entity table, names) to <dir> for sizing.
use place_search::index::Index;
use place_search::query::Searcher;
use serde_json::json;
use std::io::{BufRead, Write};
use std::time::Instant;

fn rss_kb(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with(field))
        .and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse().ok()))
        .unwrap_or(0)
}

fn dump(idx: &Index, dir: &str) {
    std::fs::create_dir_all(dir).unwrap();
    let w = |name: &str, bytes: &[u8]| std::fs::write(format!("{dir}/{name}"), bytes).unwrap();
    let as_bytes = |p: *const u8, n: usize| unsafe { std::slice::from_raw_parts(p, n) };
    w("keys.fst", idx.map.as_fst().as_bytes());
    w("postings.bin", as_bytes(idx.postings.as_ptr() as *const u8, std::mem::size_of_val(&idx.postings[..])));
    w("post_off.bin", as_bytes(idx.post_off.as_ptr() as *const u8, idx.post_off.len() * 4));
    w("entities.bin", as_bytes(idx.entities.as_ptr() as *const u8, std::mem::size_of_val(&idx.entities[..])));
    w("names.txt", idx.name_text.as_bytes());
    w("name_off.bin", as_bytes(idx.name_off.as_ptr() as *const u8, idx.name_off.len() * 4));
    w("names_start.bin", as_bytes(idx.names_start.as_ptr() as *const u8, idx.names_start.len() * 4));
    w("name_class.bin", &idx.name_class);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let corpus = &args[1];
    let aliases = &args[2];
    let opt = |name: &str| args.iter().position(|a| a == name).map(|i| args[i + 1].clone());
    let limit: usize = opt("--limit").map(|v| v.parse().unwrap()).unwrap_or(place_search::constants::DEFAULT_LIMIT);
    let bench: Option<usize> = opt("--bench").map(|v| v.parse().unwrap());

    let rss0 = rss_kb("VmRSS:");
    let idx = Index::build(corpus, aliases);
    extern "C" {
        fn malloc_trim(pad: usize) -> i32;
    }
    // Hand the build's freed scratch back so VmRSS shows what the index holds.
    unsafe { malloc_trim(0) };
    for (k, v) in &idx.stats {
        eprintln!("{k}: {v}");
    }
    eprintln!("rss after build: {} MB (peak {} MB, before {} MB)", rss_kb("VmRSS:") / 1024, rss_kb("VmHWM:") / 1024, rss0 / 1024);
    if let Some(dir) = opt("--dump") {
        dump(&idx, &dir);
    }
    let t = Instant::now();
    let s = Searcher::new(&idx);
    eprintln!("levenshtein builders: {:.1} ms", t.elapsed().as_secs_f64() * 1e3);

    let queries: Vec<String> = std::io::stdin().lock().lines().map_while(Result::ok).collect();
    if let Some(rep) = bench {
        for q in &queries {
            s.search(q, limit);
        }
        let mut lat: Vec<f64> = Vec::with_capacity(queries.len() * rep);
        for _ in 0..rep {
            for q in &queries {
                let t = Instant::now();
                std::hint::black_box(s.search(q, limit));
                lat.push(t.elapsed().as_secs_f64() * 1e6);
            }
        }
        lat.sort_by(|a, b| a.total_cmp(b));
        let p = |q: f64| lat[((lat.len() as f64 - 1.0) * q).round() as usize];
        let mean = lat.iter().sum::<f64>() / lat.len() as f64;
        println!(
            "{}",
            json!({"queries": queries.len(), "runs": lat.len(), "mean_us": mean, "p50_us": p(0.5),
                   "p90_us": p(0.9), "p99_us": p(0.99), "max_us": p(1.0)})
        );
        eprintln!("rss after bench: {} MB", rss_kb("VmRSS:") / 1024);
        return;
    }
    let out = std::io::stdout();
    let mut out = out.lock();
    for q in &queries {
        let t = Instant::now();
        let found = s.search(q, limit);
        let us = t.elapsed().as_secs_f64() * 1e6;
        let results: Vec<_> = found
            .iter()
            .map(|f| {
                let h = &f.hit;
                let e = &idx.entities[h.entity as usize];
                let osm_t = (e.osm >> 56) as u8 as char;
                json!({
                    "name": f.name,
                    "latitude": e.lat,
                    "longitude": e.lon,
                    "admin1name": idx.strings[e.state as usize],
                    "admin2name": idx.strings[e.county as usize],
                    "country": idx.strings[e.country as usize],
                    "city": idx.strings[e.city as usize],
                    "kind": (e.kind as char).to_string(),
                    "type": idx.types[e.type_id as usize],
                    "osm": format!("{}{}", osm_t, e.osm & ((1u64 << 56) - 1)),
                    "score": h.score,
                    "text": h.text,
                    "importance": e.imp,
                    "context": h.context,
                    "matched": idx.name(h.entity, h.ord).0,
                    "matched_class": idx.name(h.entity, h.ord).1,
                })
            })
            .collect();
        writeln!(out, "{}", json!({"q": q, "us": us, "results": results})).unwrap();
    }
}
