//! place-search build <set dir> <index dir> [--sub places|poi|all] [--threads N] [--mem-mb M]
//!                    [--tmp DIR] [--no-inline] [--extras extra_names.bin] [--imp DIR] [--area-proxy]
//!                    [--poi-max-tier 1|2|3]
//! place-search pack-extras <extra_names.tsv> <extra_names.bin>
//! place-search query <set dir> <index dir> [--extras F] [--iso F] [--limit N]
//!                    [--bench REPEATS [--no-warmup]]
//!   one query per stdin line; default one JSON line per query, --bench latency percentiles
//! place-search ctxbench <set dir> < "lat lng" lines   (reverse-lookup cost per point)
use place_search::build::{build, BuildOpts};
use place_search::reader::SearchIndex;
use place_search::query::Searcher;
use query_server::{Index, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL};
use serde_json::json;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

fn status_kb(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with(field))
        .and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse().ok()))
        .unwrap_or(0)
}

fn mem_json() -> serde_json::Value {
    json!({"RssAnon_kB": status_kb("RssAnon:"), "RssFile_kB": status_kb("RssFile:"), "VmRSS_kB": status_kb("VmRSS:"),
           "VmHWM_kB": status_kb("VmHWM:")})
}

fn load_geo(dir: &str) -> Index {
    Index::load(dir, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).expect("geocoder index")
}

fn opt(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).map(|i| args[i + 1].clone())
}

fn flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

fn cmd_build(args: &[String]) {
    let (set, out) = (&args[2], PathBuf::from(&args[3]));
    let geo = load_geo(set);
    let sub = opt(args, "--sub").unwrap_or_else(|| "all".into());
    let opts = BuildOpts {
        threads: opt(args, "--threads").map(|v| v.parse().unwrap()).unwrap_or(1),
        mem_mb: opt(args, "--mem-mb").map(|v| v.parse().unwrap()).unwrap_or(place_search::constants::DEFAULT_RUN_BUDGET_MB),
        tmp: opt(args, "--tmp").map(PathBuf::from).unwrap_or_else(|| out.join("tmp")),
        inline: !flag(args, "--no-inline"),
        extras: opt(args, "--extras"),
        imp_dir: opt(args, "--imp"),
        area_proxy: flag(args, "--area-proxy"),
        poi_max_tier: opt(args, "--poi-max-tier").map(|v| v.parse().unwrap()).unwrap_or(3),
    };
    let mut subs = vec![];
    if sub == "places" || sub == "all" {
        subs.push("places");
    }
    if (sub == "poi" || sub == "all") && geo.poi_records.is_some() {
        subs.push("poi");
    }
    let mut replacing = subs.clone();
    if geo.poi_records.is_none() { replacing.push("poi"); }
    place_search::build::check_retained_names(&out, &replacing, opts.extras.is_some())
        .unwrap_or_else(|e| panic!("{e}"));
    let meta_path = out.join("search_meta.json");
    let mut meta: serde_json::Value = std::fs::read_to_string(&meta_path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_else(|| json!({}));
    let n_place_nodes = geo.place_nodes.as_ref().map(|m| m.len() / 20).unwrap_or(0);
    meta["n_place_nodes"] = json!(n_place_nodes);
    meta["extras"] = json!(opts.extras.is_some());
    meta["inline"] = json!(opts.inline);
    meta["imp"] = json!(opts.imp_dir.is_some());
    meta["area_proxy"] = json!(opts.area_proxy);
    meta["poi_max_tier"] = json!(opts.poi_max_tier);
    for s in subs {
        let st = build(&geo, s, &out, &opts).unwrap_or_else(|e| panic!("build {s}: {e}"));
        let mut o = serde_json::Map::new();
        for (k, v) in &st.lines {
            eprintln!("{s}: {k}: {v}");
            o.insert(k.clone(), json!(v));
        }
        meta[s] = serde_json::Value::Object(o);
    }
    let _ = std::fs::remove_dir(&opts.tmp);
    std::fs::write(&meta_path, serde_json::to_string_pretty(&meta).unwrap()).unwrap();
    eprintln!("build memory: {}", mem_json());
}

fn latency_summary(lat: &mut [f64]) -> Option<serde_json::Value> {
    if lat.is_empty() { return None; }
    lat.sort_by(f64::total_cmp);
    let p = |q: f64| lat[((lat.len() - 1) as f64 * q).round() as usize];
    Some(json!({"mean_us": lat.iter().sum::<f64>() / lat.len() as f64,
                "p50_us": p(0.5), "p90_us": p(0.9), "p99_us": p(0.99), "max_us": p(1.0)}))
}

fn answer_lines(input: impl BufRead, out: &mut impl Write,
                mut answer: impl FnMut(&str) -> serde_json::Value) -> std::io::Result<()> {
    for line in input.lines() {
        let q = line?;
        if q.is_empty() { continue; }
        writeln!(out, "{}", answer(&q))?;
        out.flush()?;
    }
    Ok(())
}

fn cmd_query(args: &[String]) {
    let (set, dir) = (&args[2], Path::new(&args[3]));
    let limit: usize = opt(args, "--limit").map(|v| v.parse().unwrap()).unwrap_or(place_search::constants::DEFAULT_LIMIT);
    let t = Instant::now();
    let geo = load_geo(set);
    let extras = opt(args, "--extras");
    let iso = opt(args, "--iso");
    let idx = SearchIndex::open(&geo, dir, extras.as_deref(), iso.as_deref()).expect("search index");
    let s = Searcher::new(&idx);
    eprintln!("load: {:.1} ms, memory {}", t.elapsed().as_secs_f64() * 1e3, mem_json());
    if let Some(rep) = opt(args, "--bench").map(|v| v.parse::<usize>().unwrap()) {
        if rep == 0 { eprintln!("--bench needs at least 1 repeat"); std::process::exit(2); }
        let queries: Vec<String> = std::io::stdin().lock().lines().collect::<Result<Vec<_>, _>>()
            .expect("read queries").into_iter().filter(|l| !l.is_empty()).collect();
        if queries.is_empty() { eprintln!("no queries on stdin"); std::process::exit(2); }
        if !flag(args, "--no-warmup") {
            for q in &queries {
                std::hint::black_box(s.search(q, limit));
            }
        }
        let mut lat: Vec<f64> = Vec::with_capacity(queries.len() * rep);
        let mut lookups = 0usize;
        let t_all = Instant::now();
        for _ in 0..rep {
            for q in &queries {
                let t = Instant::now();
                let (r, st) = s.search(q, limit);
                std::hint::black_box(r);
                lat.push(t.elapsed().as_secs_f64() * 1e6);
                lookups += st.reverse_lookups;
            }
        }
        let wall = t_all.elapsed().as_secs_f64();
        if let Some(path) = opt(args, "--lat-out") {
            let text: Vec<String> = lat.iter().map(|v| format!("{v:.1}")).collect();
            std::fs::write(path, text.join("\n") + "\n").unwrap();
        }
        let mut summary = latency_summary(&mut lat).unwrap();
        let obj = summary.as_object_mut().unwrap();
        obj.insert("queries".into(), json!(queries.len()));
        obj.insert("runs".into(), json!(lat.len()));
        obj.insert("wall_s".into(), json!(wall));
        obj.insert("reverse_lookups_per_query".into(), json!(lookups as f64 / lat.len() as f64));
        obj.insert("memory".into(), mem_json());
        println!("{summary}");
        return;
    }
    let out = std::io::stdout();
    let mut out = out.lock();
    answer_lines(std::io::stdin().lock(), &mut out, |q| {
        let t = Instant::now();
        let (found, st) = s.search(q, limit);
        let us = t.elapsed().as_secs_f64() * 1e6;
        let results: Vec<_> = found
            .iter()
            .map(|f| {
                let c = &f.ctx;
                json!({
                    "name": f.name,
                    "latitude": f.lat,
                    "longitude": f.lng,
                    "admin1name": c.state,
                    "admin2name": if !c.county.is_empty() { c.county } else { c.district },
                    "country": c.country,
                    "country_code": std::str::from_utf8(&c.cc).unwrap_or(""),
                    "kind": f.kind.letter().to_string(),
                    "type": f.typ,
                    "score": f.hit.score,
                    "text": f.hit.text,
                    "context": f.hit.context,
                    "class": f.hit.class,
                    "entity": f.hit.entity,
                })
            })
            .collect();
        json!({"q": q, "us": us, "reverse_lookups": st.reverse_lookups, "context_checks": st.context_checks,
               "keys": st.keys_matched, "postings": st.postings_read, "results": results})
    }).expect("query input/output");
    eprintln!("after queries: memory {}", mem_json());
}

fn cmd_ctxbench(args: &[String]) {
    let geo = load_geo(&args[2]);
    let dir = Path::new(&args[3]);
    let idx = SearchIndex::open(&geo, dir, None, None).expect("search index");
    let s = Searcher::new(&idx);
    let pts: Vec<(f32, f32)> = std::io::stdin()
        .lock()
        .lines()
        .map_while(Result::ok)
        .filter_map(|l| {
            let mut it = l.split_whitespace();
            Some((it.next()?.parse().ok()?, it.next()?.parse().ok()?))
        })
        .collect();
    let mut lat = Vec::with_capacity(pts.len());
    for &(a, b) in &pts {
        let t = Instant::now();
        std::hint::black_box(s.context_at(a, b));
        lat.push(t.elapsed().as_secs_f64() * 1e6);
    }
    let Some(mut summary) = latency_summary(&mut lat) else {
        eprintln!("no 'lat lng' points parsed from stdin"); std::process::exit(2);
    };
    summary["points"] = json!(pts.len());
    summary["memory"] = mem_json();
    println!("{summary}");
}

/// Key-level churn between two builds of one sub-index: keys only in A, only in B, in both with
/// the same / a different posting list.
fn cmd_diff(args: &[String]) {
    use fst::{IntoStreamer, Streamer};
    let (a, b, name) = (Path::new(&args[2]), Path::new(&args[3]), &args[4]);
    let sa = place_search::reader::SubIndex::open(a, name).expect("A");
    let sb = place_search::reader::SubIndex::open(b, name).expect("B");
    let mut ia = sa.map.stream();
    let mut ib = sb.map.into_stream();
    let (mut only_a, mut only_b, mut same, mut changed) = (0u64, 0u64, 0u64, 0u64);
    let (mut pa, mut pb) = (Vec::new(), Vec::new());
    let mut na = ia.next().map(|(k, v)| (k.to_vec(), v));
    let mut nb = ib.next().map(|(k, v)| (k.to_vec(), v));
    loop {
        match (&na, &nb) {
            (None, None) => break,
            (Some(_), None) => { only_a += 1; na = ia.next().map(|(k, v)| (k.to_vec(), v)); }
            (None, Some(_)) => { only_b += 1; nb = ib.next().map(|(k, v)| (k.to_vec(), v)); }
            (Some((ka, va)), Some((kb, vb))) => match ka.cmp(kb) {
                std::cmp::Ordering::Less => { only_a += 1; na = ia.next().map(|(k, v)| (k.to_vec(), v)); }
                std::cmp::Ordering::Greater => { only_b += 1; nb = ib.next().map(|(k, v)| (k.to_vec(), v)); }
                std::cmp::Ordering::Equal => {
                    sa.postings(*va, &mut pa);
                    sb.postings(*vb, &mut pb);
                    if pa == pb { same += 1 } else { changed += 1 }
                    na = ia.next().map(|(k, v)| (k.to_vec(), v));
                    nb = ib.next().map(|(k, v)| (k.to_vec(), v));
                }
            },
        }
    }
    println!("{}", json!({"sub": name, "keys_only_a": only_a, "keys_only_b": only_b, "keys_same": same, "keys_changed_postings": changed}));
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("build") => cmd_build(&args),
        Some("pack-extras") => {
            let (n, k) = place_search::extras::pack(&args[2], &args[3]).expect("pack");
            eprintln!("records {n}, names {k}");
        }
        Some("query") => cmd_query(&args),
        Some("ctxbench") => cmd_ctxbench(&args),
        Some("diff") => cmd_diff(&args),
        _ => eprintln!("usage: see the header of main.rs"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn empty_latency_samples_have_no_summary() {
        assert!(latency_summary(&mut []).is_none());
        let summary = latency_summary(&mut [5.0]).unwrap();
        for key in ["mean_us", "p50_us", "p90_us", "p99_us", "max_us"] {
            assert_eq!(summary[key], json!(5.0));
        }
    }

    #[test]
    fn queries_are_answered_and_flushed_before_reading_the_next_line() {
        struct Input { first: bool, out: Rc<RefCell<Vec<u8>>> }
        impl std::io::Read for Input {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if self.first { self.first = false; buf[..2].copy_from_slice(b"a\n"); Ok(2) }
                else { assert_eq!(&*self.out.borrow(), b"{\"q\":\"a\"}\n"); Ok(0) }
            }
        }
        struct Output(Rc<RefCell<Vec<u8>>>, usize);
        impl Write for Output {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.borrow_mut().extend_from_slice(buf); Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> { self.1 += 1; Ok(()) }
        }
        let bytes = Rc::new(RefCell::new(Vec::new()));
        let input = std::io::BufReader::new(Input { first: true, out: bytes.clone() });
        let mut out = Output(bytes, 0);
        answer_lines(input, &mut out, |q| json!({"q": q})).unwrap();
        assert_eq!(out.1, 1);
    }

    #[test]
    fn query_input_errors_are_reported() {
        struct Broken;
        impl std::io::Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("input failed"))
            }
        }
        assert!(answer_lines(std::io::BufReader::new(Broken), &mut Vec::new(), |_| json!(null)).is_err());
    }
}
