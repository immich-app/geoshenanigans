// Times the server's answer with water (MultiIndex::query_json on a dataset whose index holds a
// water dir of the builder's water layer: the address, the water lookup and the answer's JSON
// with its water field) against the same point's answer without water, and the water lookup
// alone (WaterLayer::at: one cell read, then the ring tests of its entries in answering order,
// up to the first that holds the point), at the cells whose non-interior entries carry the most
// rings: the lakes and bays with thousands of islands, whose holes a ring test visits one by
// one, and the cells of many small polygons.
//
//   cargo build --release --example water_bench
//   ./target/release/examples/water_bench <dataset dir> <water dir> [cells] [rounds] > report.txt
//
// <dataset dir> is a flat dir grid_query loads (a mode's files beside a boundary tier), without
// a water dir; <water dir> is one the builder writes: {region}/water/major/simplified,
// {region}/water/all/simplified or {region}/water/all/exact, or {region}/water of a single-output
// build. Both are read at the default cell levels (DEFAULT_ADMIN_CELL_LEVEL, and
// DEFAULT_STREET_CELL_LEVEL for the dataset's streets), those of a build without --admin-level or
// --street-level: a dir of other levels fails to load. The dataset loads twice, as the server
// loads a flat dir (one world slot): without water, and with the water dir's layer as its index's
// water. The cells: of the polygons with the largest ring blocks (200), the L10 cells they reach
// where their entry is not interior, scored by the rings of every non-interior entry there; the
// `cells` best (default 40), each sampled at 5 x 5 points spread over it as blends of its
// corners. Each point's answer with water is the one without but for its water field, which
// names the lookup's water there (name, class and type), and is absent where it finds none. Each
// point is answered once by each of the three untimed, then `rounds` times (default 200), a round
// timing the lookup, the answer without water and the one with it, and the medians kept. Prints
// each cell, then p50 and p99 over the points of the lookup and of both answers, and of the
// answer with water against the one without, point by point and as quantiles. Missing or bad
// arguments, a dataset dir of no dataset and cells of no point to time exit with a message and
// status 2.
use query_server::{
    cell_id_at_level, cell_pairs, mmap_file, AdminGeometry, AdminPolygon, CellPairs, ContinentSlot, Index,
    MultiIndex, WaterLayer, ADMIN_POLYGON_RECORD_BYTES, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE,
    DEFAULT_STREET_CELL_LEVEL, ID_MASK, INTERIOR_FLAG,
};
use s2::cell::Cell;
use s2::cellid::CellID;
use s2::latlng::LatLng;
use s2::point::Point;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

const USAGE: &str = "usage: water_bench <dataset dir> <water dir> [cells] [rounds]";

/// What a run is given: the two dirs, how many cells it samples and how many rounds it answers
/// each point.
#[derive(Debug, PartialEq)]
struct Args {
    dataset: String,
    water: String,
    cells: usize,
    rounds: usize,
}

impl Args {
    /// The command line, the program's name first; an error, the usage line in it, for a dir
    /// left out, an argument past the four, or a count that is no number above 0.
    fn parse(args: &[String]) -> Result<Args, String> {
        if !(3..=5).contains(&args.len()) {
            return Err(USAGE.to_string());
        }
        let count = |at: usize, name: &str, default: usize| match args.get(at) {
            None => Ok(default),
            Some(a) => match a.parse::<usize>() {
                Ok(n) if n > 0 => Ok(n),
                _ => Err(format!("{name} is no number above 0: {a}\n{USAGE}")),
            },
        };
        Ok(Args {
            dataset: args[1].clone(),
            water: args[2].clone(),
            cells: count(3, "cells", 40)?,
            rounds: count(4, "rounds", 200)?,
        })
    }
}

/// The flat dataset dir `dir` as the server loads it (MultiIndex::load: one world slot), `water`
/// its index's water layer; an error for a dir that loads no dataset, whose answers would all be
/// empty.
fn world(dir: &str, water: Option<WaterLayer>) -> Result<MultiIndex, String> {
    if !std::path::Path::new(&format!("{dir}/admin_cells.bin")).exists() {
        return Err(format!("{dir} holds no dataset grid_query loads"));
    }
    let mut index = Index::load(dir, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE)?;
    index.water = water;
    Ok(MultiIndex {
        continents: vec![ContinentSlot {
            name: "world",
            continent: None,
            index: arc_swap::ArcSwapOption::from(Some(Arc::new(index))),
            loaded_stamp: None,
        }],
        root_dir: dir.to_string(),
        street_cell_level: DEFAULT_STREET_CELL_LEVEL,
        admin_cell_level: DEFAULT_ADMIN_CELL_LEVEL,
        search_distance: DEFAULT_SEARCH_DISTANCE,
    })
}

/// An answer's JSON, parsed.
fn parsed(json: &str) -> serde_json::Value {
    serde_json::from_str(json).expect("an answer's JSON")
}

/// The water dir's records, cells and rings, which pick the cells timed.
struct Water {
    records: Vec<AdminPolygon>,
    index: CellPairs,
    geometry: AdminGeometry,
}

impl Water {
    fn load(dir: &str) -> Water {
        let polygons = mmap_file(&format!("{dir}/admin_polygons.bin")).expect("admin_polygons.bin");
        let geometry = AdminGeometry::load(dir).expect("water geometry");
        let bytes = polygons.as_slice();
        let n = bytes.len() / ADMIN_POLYGON_RECORD_BYTES as usize;
        let records = (0..n)
            .map(|i| {
                let at = i * ADMIN_POLYGON_RECORD_BYTES as usize;
                let end = if i + 1 < n {
                    u64::from(u32::from_le_bytes(bytes[at + 16..at + 20].try_into().unwrap()))
                } else {
                    geometry.rings_len()
                };
                AdminPolygon::from_record(&bytes[at..at + 16], end)
            })
            .collect();
        let index = CellPairs::open(
            mmap_file(&format!("{dir}/admin_cells.bin")).expect("admin_cells.bin"),
            mmap_file(&format!("{dir}/admin_entries.bin")).expect("admin_entries.bin"),
            cell_pairs::ADMIN,
            DEFAULT_ADMIN_CELL_LEVEL,
        )
        .expect("water cell index");
        Water { records, index, geometry }
    }
}

/// The 5 x 5 points a cell is sampled at, lat and lng: blends of the cell's corners a fifth of
/// the way apart each way, the first a tenth in from its lower left corner, so each is in the
/// cell wherever it lies, along the antimeridian and at a pole too.
fn cell_points(cell: u64) -> Vec<(f64, f64)> {
    let c = Cell::from(CellID(cell));
    let corners: Vec<_> = (0..4).map(|k| c.vertex(k).0).collect();
    let mut points = Vec::with_capacity(25);
    for i in 0..5 {
        for j in 0..5 {
            let (s, t) = (0.1 + 0.2 * j as f64, 0.1 + 0.2 * i as f64);
            // Lower left, lower right, upper right, upper left, as Cell::vertex gives them.
            let blend = corners[0] * ((1.0 - s) * (1.0 - t))
                + corners[1] * (s * (1.0 - t))
                + corners[2] * (s * t)
                + corners[3] * ((1.0 - s) * t);
            let at = LatLng::from(Point(blend.normalize()));
            points.push((at.lat.deg(), at.lng.deg()));
        }
    }
    points
}

/// A point's medians over its rounds, in microseconds: of the water lookup, and of the answers
/// without water and with it.
#[derive(Debug, Clone, Copy)]
struct Timing {
    lookup: f64,
    without: f64,
    with: f64,
}

/// Each path called once untimed, warm, then `rounds` rounds of `lookup`, `without` and then
/// `with`, each timed by `clock` (microseconds since some fixed instant; `since` in a run).
fn time_point(
    rounds: usize,
    clock: impl Fn() -> f64,
    mut lookup: impl FnMut(),
    mut without: impl FnMut(),
    mut with: impl FnMut(),
) -> Timing {
    lookup();
    without();
    with();
    let (mut l, mut d, mut w) = (Vec::with_capacity(rounds), Vec::with_capacity(rounds), Vec::with_capacity(rounds));
    for _ in 0..rounds {
        let t = clock();
        lookup();
        l.push(clock() - t);
        let t = clock();
        without();
        d.push(clock() - t);
        let t = clock();
        with();
        w.push(clock() - t);
    }
    Timing { lookup: median(l), without: median(d), with: median(w) }
}

/// A clock of microseconds since `start`, for time_point.
fn since(start: Instant) -> impl Fn() -> f64 {
    move || start.elapsed().as_secs_f64() * 1e6
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn quantile(v: &[f64], q: f64) -> f64 {
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[((s.len() as f64 - 1.0) * q).round() as usize]
}

/// The summary line of the points' timings; an error for no point.
fn summary(points: &[Timing]) -> Result<String, String> {
    if points.is_empty() {
        return Err("no point to time: no cell of the water dir lists a polygon not interior to it".to_string());
    }
    let of = |f: fn(&Timing) -> f64| points.iter().map(f).collect::<Vec<f64>>();
    let (lookup, without, with, ratio) = (of(|t| t.lookup), of(|t| t.without), of(|t| t.with), of(|t| t.with / t.without));
    Ok(format!(
        "WATER_BENCH points={} lookup_p50_us={:.1} lookup_p99_us={:.1} dry_p50_us={:.1} dry_p99_us={:.1} wet_p50_us={:.1} wet_p99_us={:.1} wet_p99_over_dry_p99={:.2} pointwise_wet_over_dry_p50={:.2} pointwise_wet_over_dry_p99={:.2}",
        points.len(),
        quantile(&lookup, 0.5),
        quantile(&lookup, 0.99),
        quantile(&without, 0.5),
        quantile(&without, 0.99),
        quantile(&with, 0.5),
        quantile(&with, 0.99),
        quantile(&with, 0.99) / quantile(&without, 0.99),
        quantile(&ratio, 0.5),
        quantile(&ratio, 0.99)
    ))
}

/// Its message on stderr, and status 2.
fn fail(why: &str) -> ! {
    eprintln!("water_bench: {why}");
    std::process::exit(2)
}

fn main() {
    let args = Args::parse(&std::env::args().collect::<Vec<_>>()).unwrap_or_else(|why| fail(&why));
    let water = Water::load(&args.water);
    let open =
        || WaterLayer::open(&args.water, DEFAULT_ADMIN_CELL_LEVEL).expect("water dir").expect("a water dir's files");
    let layer = open();
    let dry = world(&args.dataset, None).unwrap_or_else(|why| fail(&why));
    let wet = world(&args.dataset, Some(open())).unwrap_or_else(|why| fail(&why));
    let start = Instant::now();

    // The polygons with the largest ring blocks, by their rings.
    let mut by_bytes: Vec<u32> = (0..water.records.len() as u32).collect();
    by_bytes.sort_by_key(|&p| std::cmp::Reverse(water.records[p as usize].ring_bytes));
    let mut ring_count: BTreeMap<u32, usize> = BTreeMap::new();
    let mut rings_of = |p: u32, water: &Water| -> usize {
        *ring_count.entry(p).or_insert_with(|| water.geometry.rings(&water.records[p as usize]).len())
    };
    let heavy: Vec<u32> = by_bytes.iter().copied().take(200).collect();
    // Their cells: a grid of points over each one's box, a fifth of a cell apart.
    let mut scored: BTreeMap<u64, usize> = BTreeMap::new();
    for &p in &heavy {
        let rings = water.geometry.rings(&water.records[p as usize]);
        let (mut lo_lat, mut hi_lat, mut lo_lng, mut hi_lng) = (90.0f64, -90.0f64, 180.0f64, -180.0f64);
        for v in rings.iter().flatten() {
            lo_lng = lo_lng.min(v[0]);
            hi_lng = hi_lng.max(v[0]);
            lo_lat = lo_lat.min(v[1]);
            hi_lat = hi_lat.max(v[1]);
        }
        let step = 0.02;
        let mut seen = BTreeSet::new();
        let mut lat = lo_lat;
        while lat <= hi_lat {
            let mut lng = lo_lng;
            while lng <= hi_lng {
                seen.insert(cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL));
                lng += step;
            }
            lat += step;
        }
        for cell in seen {
            if scored.contains_key(&cell) {
                continue;
            }
            let ids = water.index.ids(cell);
            if !ids.iter().any(|&id| id & ID_MASK == p && id & INTERIOR_FLAG == 0) {
                continue;
            }
            let score: usize =
                ids.iter().filter(|&&id| id & INTERIOR_FLAG == 0).map(|&id| rings_of(id & ID_MASK, &water)).sum();
            scored.insert(cell, score);
        }
    }
    let mut best: Vec<(u64, usize)> = scored.into_iter().collect();
    best.sort_by_key(|&(cell, score)| (std::cmp::Reverse(score), cell));
    best.truncate(args.cells);

    let mut points = Vec::new();
    for &(cell, score) in &best {
        let c = Cell::from(CellID(cell));
        let ids = water.index.ids(cell);
        let entries = ids.iter().filter(|&&id| id & INTERIOR_FLAG == 0).count();
        let mut timed = Vec::new();
        let mut held = 0;
        for (lat, lng) in cell_points(cell) {
            if cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL) != cell {
                continue;
            }
            // The answer with water is the one without, but for its water field: the lookup's
            // name, class and type, absent where it finds none.
            let water = serde_json::to_value(layer.at(lat, lng)).unwrap();
            held += usize::from(!water.is_null());
            let dry_answer = parsed(&dry.query_json(lat, lng));
            let mut wet_answer = parsed(&wet.query_json(lat, lng));
            let wet_water = wet_answer.as_object_mut().expect("an answer's object").remove("water");
            assert_eq!(dry_answer.get("water"), None, "the water the answer without water at ({lat}, {lng}) names");
            assert_eq!(wet_answer, dry_answer, "the answers with and without water at ({lat}, {lng}), but for the water");
            assert_eq!(wet_water.unwrap_or_default(), water, "the water the answer with water at ({lat}, {lng}) names");
            timed.push(time_point(
                args.rounds,
                since(start),
                || {
                    std::hint::black_box(layer.at(lat, lng));
                },
                || {
                    std::hint::black_box(dry.query_json(lat, lng));
                },
                || {
                    std::hint::black_box(wet.query_json(lat, lng));
                },
            ));
        }
        if timed.is_empty() {
            continue;
        }
        let (lat, lng) = (LatLng::from(c.center()).lat.deg(), LatLng::from(c.center()).lng.deg());
        println!(
            "cell {cell:016x} at {lat:.4} {lng:.4}: {} entries, {entries} not interior with {score} rings; {held} of {} points in water; lookup {:.1} us, answer without water {:.1} us, with water {:.1} us (medians)",
            ids.len(),
            timed.len(),
            median(timed.iter().map(|t| t.lookup).collect()),
            median(timed.iter().map(|t| t.without).collect()),
            median(timed.iter().map(|t| t.with).collect())
        );
        points.extend(timed);
    }
    println!("{}", summary(&points).unwrap_or_else(|why| fail(&why)));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Result<Args, String> {
        Args::parse(&std::iter::once("water_bench").chain(list.iter().copied()).map(String::from).collect::<Vec<_>>())
    }

    #[test]
    fn a_run_takes_both_dirs_and_counts_above_0() {
        let given = |dataset: &str, water: &str, cells, rounds| Args { dataset: dataset.into(), water: water.into(), cells, rounds };
        assert_eq!(args(&["d", "w"]), Ok(given("d", "w", 40, 200)));
        assert_eq!(args(&["d", "w", "3"]), Ok(given("d", "w", 3, 200)));
        assert_eq!(args(&["d", "w", "3", "1"]), Ok(given("d", "w", 3, 1)));
        for list in [&[][..], &["d"], &["d", "w", "3", "1", "x"]] {
            assert_eq!(args(list), Err(USAGE.to_string()), "{list:?}");
        }
        for (list, bad) in [(&["d", "w", "0"][..], "cells is no number above 0: 0"), (&["d", "w", "x"], "cells is no number above 0: x"),
            (&["d", "w", "3", "0"], "rounds is no number above 0: 0"), (&["d", "w", "3", "-1"], "rounds is no number above 0: -1")]
        {
            assert_eq!(args(list), Err(format!("{bad}\n{USAGE}")), "{list:?}");
        }
    }

    #[test]
    fn a_dataset_dir_that_loads_no_dataset_is_no_baseline() {
        // The water fixture's days, none a dataset or a continent's dir, and a dir that isn't.
        for dir in [concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/water_dir"), concat!(env!("CARGO_MANIFEST_DIR"), "/no such dir")] {
            assert_eq!(world(dir, None).err(), Some(format!("{dir} holds no dataset grid_query loads")));
        }
    }

    #[test]
    fn every_point_of_a_cell_is_in_it_wherever_it_lies() {
        // Ordinary cells, both sides of the antimeridian (the Bering Sea, Fiji), and at the poles.
        let at = [(52.0, 5.0), (-33.9, 18.4), (60.0, 179.999), (60.0, -179.999), (-17.0, 179.999), (-17.0, -179.999),
            (0.0, 180.0), (89.99, 45.0), (-89.99, -100.0)];
        // Of each cell, its distinct points in it.
        let held: Vec<((f64, f64), usize)> = at
            .iter()
            .map(|&(lat, lng)| {
                let cell = cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL);
                let points = cell_points(cell);
                let distinct: BTreeSet<(u64, u64)> = points
                    .iter()
                    .filter(|&&(a, b)| cell_id_at_level(a, b, DEFAULT_ADMIN_CELL_LEVEL) == cell)
                    .map(|&(a, b)| (a.to_bits(), b.to_bits()))
                    .collect();
                assert!(distinct.len() == points.len(), "{lat} {lng}: {points:?}");
                ((lat, lng), distinct.len())
            })
            .collect();
        assert_eq!(held, at.iter().map(|&p| (p, 25)).collect::<Vec<_>>());
    }

    // A path on the clock `now` (microseconds) whose calls `slow_at` lists (0 the first) move it
    // on by `slow`, the others by `fast`.
    fn timed_call<'a>(now: &'a std::cell::Cell<f64>, slow_at: &'static [usize], slow: f64, fast: f64) -> impl FnMut() + 'a {
        let mut n = 0;
        move || {
            now.set(now.get() + if slow_at.contains(&n) { slow } else { fast });
            n += 1;
        }
    }

    #[test]
    fn a_points_first_answer_of_each_path_is_untimed() {
        let now = std::cell::Cell::new(0.0);
        let slow_first = || timed_call(&now, &[0], 50_000.0, 0.0);
        let t = time_point(1, || now.get(), slow_first(), slow_first(), slow_first());
        assert_eq!((t.lookup, t.without, t.with), (0.0, 0.0, 0.0));
    }

    #[test]
    fn each_path_is_timed_on_its_own() {
        let now = std::cell::Cell::new(0.0);
        let t = time_point(3, || now.get(), timed_call(&now, &[], 0.0, 10.0), timed_call(&now, &[], 0.0, 20.0), timed_call(&now, &[], 0.0, 30.0));
        assert_eq!((t.lookup, t.without, t.with), (10.0, 20.0, 30.0));
    }

    #[test]
    fn a_run_of_no_point_to_time_has_no_summary() {
        assert_eq!(summary(&[]), Err("no point to time: no cell of the water dir lists a polygon not interior to it".to_string()));
        let line = summary(&[Timing { lookup: 2.0, without: 4.0, with: 5.0 }]).unwrap();
        assert!(line.starts_with("WATER_BENCH points=1 lookup_p50_us=2.0 lookup_p99_us=2.0 dry_p50_us=4.0 "), "{line}");
        assert!(line.contains(" wet_p50_us=5.0 ") && line.ends_with(" pointwise_wet_over_dry_p50=1.25 pointwise_wet_over_dry_p99=1.25"), "{line}");
    }
}
