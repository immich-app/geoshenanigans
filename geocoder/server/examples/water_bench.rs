// Times a water lookup (a water dir of the builder's water layer, read with the admin readers:
// one cell read, then the ring test of each entry not flagged interior) against the same point's
// answer without water, at the cells whose non-interior entries carry the most rings: the lakes
// and bays with thousands of islands, whose holes a ring test visits one by one.
//
//   cargo build --release --example water_bench
//   ./target/release/examples/water_bench <dataset dir> <water dir> [cells] [rounds] > report.txt
//
// <dataset dir> is a flat dir grid_query loads (a mode's files beside a boundary tier); <water
// dir> is one the builder writes: {region}/water/major/simplified, {region}/water/all/simplified
// or {region}/water/all/exact, or {region}/water of a single-output build. Both are read at the
// default cell levels (DEFAULT_ADMIN_CELL_LEVEL, and DEFAULT_STREET_CELL_LEVEL for the dataset's
// streets), those of a build without --admin-level or --street-level: a dir of other levels
// fails to load. The cells: of the polygons with the largest ring blocks (200), the L10 cells
// they reach where their entry is not interior, scored by the rings of every non-interior entry
// there; the `cells` best (default 40), each sampled at 5 x 5 points spread over it as blends of
// its corners. Each point is answered once by each path untimed, then `rounds` times
// (default 200), a round timing the water lookup and then the answer, and the medians kept: of
// each path and of the two together round by round. Prints each cell, then p50 and p99 over the
// points of both paths, of the two together, and of the two together against the answer alone,
// point by point and as quantiles. Missing or bad arguments, a dataset dir of no dataset and
// cells of no point to time exit with a message and status 2.
use query_server::{
    cell_id_at_level, cell_pairs, mmap_file, AdminGeometry, AdminPoint, AdminPolygon, CellPairs, MultiIndex, PipMemo,
    ADMIN_POLYGON_RECORD_BYTES, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL, ID_MASK,
    INTERIOR_FLAG,
};
use s2::cell::Cell;
use s2::cellid::CellID;
use s2::latlng::LatLng;
use s2::point::Point;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
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

/// The dataset the answers without water come from; an error for a dir that loads none, whose
/// answers would all be empty.
fn baseline(dir: &str) -> Result<MultiIndex, String> {
    let index = MultiIndex::load(dir, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE)?;
    if index.loaded_count() == 0 {
        return Err(format!("{dir} holds no dataset grid_query loads"));
    }
    Ok(index)
}

/// The rank of each type code water_meta.json lists, by code; an error for text that isn't such
/// a list.
fn ranks(meta: &str) -> Result<HashMap<u16, u8>, String> {
    #[derive(serde::Deserialize)]
    struct Kind {
        code: u16,
        rank: u8,
    }
    #[derive(serde::Deserialize)]
    struct Meta {
        types: Vec<Kind>,
    }
    let meta: Meta = serde_json::from_str(meta).map_err(|e| e.to_string())?;
    Ok(meta.types.into_iter().map(|k| (k.code, k.rank)).collect())
}

/// A candidate's place in the order in which water answers: the smaller area, then the higher
/// rank, then the lower type code, as the layer's answer orders them; then the lower polygon id.
#[derive(Debug, Clone, Copy)]
struct Precedence {
    area: f32,
    rank: u8,
    code: u16,
    id: u32,
}

impl Precedence {
    fn order(&self, other: &Precedence) -> Ordering {
        self.area
            .total_cmp(&other.area)
            .then(other.rank.cmp(&self.rank))
            .then(self.code.cmp(&other.code))
            .then(self.id.cmp(&other.id))
    }
}

struct Water {
    records: Vec<AdminPolygon>,
    /// Each type code's rank (water_meta.json), which breaks ties between areas of one size.
    ranks: HashMap<u16, u8>,
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
        let meta = std::fs::read_to_string(format!("{dir}/water_meta.json")).expect("water_meta.json");
        let ranks = ranks(&meta).expect("water_meta.json");
        let index = CellPairs::open(
            mmap_file(&format!("{dir}/admin_cells.bin")).expect("admin_cells.bin"),
            mmap_file(&format!("{dir}/admin_entries.bin")).expect("admin_entries.bin"),
            cell_pairs::ADMIN,
            DEFAULT_ADMIN_CELL_LEVEL,
        )
        .expect("water cell index");
        Water { records, ranks, index, geometry }
    }

    /// The water holding the point, by Precedence, as the layer's answer picks it; the polygon
    /// id. A polygon of a type code water_meta.json lists no rank for answers nothing.
    fn at(&self, lat: f64, lng: f64) -> Option<u32> {
        let point = AdminPoint::new(lat, lng);
        let (lat, lng) = point.degrees();
        let mut memo = PipMemo::new();
        let mut best: Option<Precedence> = None;
        self.index.with_ids(cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL), |ids| {
            for &id in ids {
                let poly = &self.records[(id & ID_MASK) as usize];
                if poly.ring_bytes == 0 {
                    continue;
                }
                let Some(&rank) = self.ranks.get(&poly.country_code) else {
                    continue;
                };
                let key = Precedence { area: poly.area, rank, code: poly.country_code, id: id & ID_MASK };
                if best.is_some_and(|b| b.order(&key) != Ordering::Greater) {
                    continue;
                }
                if id & INTERIOR_FLAG != 0 || self.geometry.contains(poly, point, &mut memo) {
                    best = Some(key);
                }
            }
        });
        best.map(|b| b.id)
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

/// A point's medians over its rounds, in microseconds: of the water lookup, of the answer, and
/// of the two together, round by round.
#[derive(Debug, Clone, Copy)]
struct Timing {
    water: f64,
    answer: f64,
    together: f64,
}

/// Each path called once untimed, warm, then `rounds` rounds of `water` and then `answer`, each
/// timed by `clock` (microseconds since some fixed instant; `since` in a run).
fn time_point(rounds: usize, clock: impl Fn() -> f64, mut water: impl FnMut(), mut answer: impl FnMut()) -> Timing {
    water();
    answer();
    let (mut w, mut a, mut both) = (Vec::with_capacity(rounds), Vec::with_capacity(rounds), Vec::with_capacity(rounds));
    for _ in 0..rounds {
        let t = clock();
        water();
        let tw = clock() - t;
        let t = clock();
        answer();
        let ta = clock() - t;
        w.push(tw);
        a.push(ta);
        both.push(tw + ta);
    }
    Timing { water: median(w), answer: median(a), together: median(both) }
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
    let (with, without, both, ratio) = (of(|t| t.water), of(|t| t.answer), of(|t| t.together), of(|t| t.together / t.answer));
    Ok(format!(
        "WATER_BENCH points={} water_p50_us={:.1} water_p99_us={:.1} answer_p50_us={:.1} answer_p99_us={:.1} together_p50_us={:.1} together_p99_us={:.1} together_p99_over_answer_p99={:.2} pointwise_over_answer_p50={:.2} pointwise_over_answer_p99={:.2}",
        points.len(),
        quantile(&with, 0.5),
        quantile(&with, 0.99),
        quantile(&without, 0.5),
        quantile(&without, 0.99),
        quantile(&both, 0.5),
        quantile(&both, 0.99),
        quantile(&both, 0.99) / quantile(&without, 0.99),
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
    let index = baseline(&args.dataset).unwrap_or_else(|why| fail(&why));
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
            held += usize::from(water.at(lat, lng).is_some());
            timed.push(time_point(
                args.rounds,
                since(start),
                || {
                    std::hint::black_box(water.at(lat, lng));
                },
                || {
                    std::hint::black_box(index.query_json(lat, lng));
                },
            ));
        }
        if timed.is_empty() {
            continue;
        }
        let (lat, lng) = (LatLng::from(c.center()).lat.deg(), LatLng::from(c.center()).lng.deg());
        println!(
            "cell {cell:016x} at {lat:.4} {lng:.4}: {} entries, {entries} not interior with {score} rings; {held} of {} points in water; water {:.1} us, answer {:.1} us (medians)",
            ids.len(),
            timed.len(),
            median(timed.iter().map(|t| t.water).collect()),
            median(timed.iter().map(|t| t.answer).collect())
        );
        points.extend(timed);
    }
    println!("{}", summary(&points).unwrap_or_else(|why| fail(&why)));
}

#[cfg(test)]
mod tests {
    use super::*;

    const WATER_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/water_dir/day3");

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
            assert_eq!(baseline(dir).err(), Some(format!("{dir} holds no dataset grid_query loads")));
        }
    }

    #[test]
    fn of_two_areas_of_one_size_the_higher_rank_answers() {
        let key = |area: f32, rank: u8, code: u16, id: u32| Precedence { area, rank, code, id };
        // A pond (code 4, rank 24) before a lake (code 2, rank 20) of its area; the smaller area
        // first, whatever its rank.
        assert_eq!(key(1.0, 24, 4, 9).order(&key(1.0, 20, 2, 1)), Ordering::Less);
        assert_eq!(key(1.0, 20, 2, 1).order(&key(2.0, 24, 4, 9)), Ordering::Less);
        // Of one rank, the lower code; of one code, the lower id.
        assert_eq!(key(1.0, 22, 7, 1).order(&key(1.0, 22, 8, 0)), Ordering::Less);
        assert_eq!(key(1.0, 22, 7, 1).order(&key(1.0, 22, 7, 2)), Ordering::Less);
        assert_eq!(key(1.0, 22, 7, 1).order(&key(1.0, 22, 7, 1)), Ordering::Equal);
    }

    // The water fixture's lake given the type code `lake`, and a twin of it of the code `twin`
    // (the lake's rings, so of its area, a polygon after the fixture's polygons), both listed not
    // interior in the cell of a point of the lake off its island: the water, the point and the
    // two ids.
    fn twins(lake: u16, twin: u16) -> (Water, (f64, f64), u32, u32) {
        let mut water = Water::load(WATER_DIR);
        let at = water.records.iter().position(|p| p.country_code == 2 && p.ring_bytes > 0).expect("the fixture's lake");
        water.records[at].country_code = lake;
        water.records.push(AdminPolygon { country_code: twin, ..water.records[at] });
        let (lake_id, twin_id) = (at as u32, water.records.len() as u32 - 1);
        let point = (47.002, 8.002);
        let cell = cell_id_at_level(point.0, point.1, DEFAULT_ADMIN_CELL_LEVEL);
        water.index = cell_pairs::from_pairs([(cell, lake_id), (cell, twin_id)], cell_pairs::ADMIN);
        (water, point, lake_id, twin_id)
    }

    #[test]
    fn at_a_point_of_two_areas_of_one_size_the_higher_rank_answers() {
        // Of a lake (2, rank 20) and a pond (4, rank 24) the pond, whichever polygon is which,
        // though its code is the higher; of a lake and a river (29, rank 19) the lake; of two of
        // rank 22 (50492, 64461) the lower code; of two of one code the lower id.
        for (lake, twin, twin_answers) in [(2, 4, true), (4, 2, false), (2, 29, false), (64461, 50492, true), (2, 2, false)] {
            let (water, (lat, lng), lake_id, twin_id) = twins(lake, twin);
            assert_eq!(water.at(lat, lng), Some(if twin_answers { twin_id } else { lake_id }), "{lake} {twin}");
        }
        // A code water_meta.json lists no rank for answers nothing: of two such, neither.
        let (water, (lat, lng), _, _) = twins(7, 8);
        assert_eq!(water.at(lat, lng), None);
    }

    #[test]
    fn a_water_dir_gives_each_type_code_its_rank() {
        let water = Water::load(WATER_DIR);
        let listed: BTreeMap<u16, u8> = water.ranks.iter().map(|(&c, &r)| (c, r)).collect();
        assert_eq!(listed, BTreeMap::from([(2, 20), (4, 24), (25, 2), (29, 19), (50492, 22), (64461, 22)]));
        assert!(ranks("{\"types\": 1}").is_err());
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
        let t = time_point(1, || now.get(), timed_call(&now, &[0], 50_000.0, 0.0), timed_call(&now, &[0], 50_000.0, 0.0));
        assert_eq!((t.water, t.answer, t.together), (0.0, 0.0, 0.0));
    }

    #[test]
    fn each_path_is_timed_on_its_own() {
        let now = std::cell::Cell::new(0.0);
        let t = time_point(3, || now.get(), timed_call(&now, &[], 0.0, 10.0), timed_call(&now, &[], 0.0, 20.0));
        assert_eq!((t.water, t.answer, t.together), (10.0, 20.0, 30.0));
    }

    #[test]
    fn the_two_together_are_the_median_of_each_rounds_sum() {
        // The first round's water lookup (30 ms) and the second's answer (50 ms) are slow: each
        // path's median is 0, and of the rounds' sums (30 ms, 50 ms and 0) the median is 30 ms.
        let now = std::cell::Cell::new(0.0);
        let t = time_point(3, || now.get(), timed_call(&now, &[1], 30_000.0, 0.0), timed_call(&now, &[2], 50_000.0, 0.0));
        assert_eq!((t.water, t.answer, t.together), (0.0, 0.0, 30_000.0));
    }

    #[test]
    fn a_run_of_no_point_to_time_has_no_summary() {
        assert_eq!(summary(&[]), Err("no point to time: no cell of the water dir lists a polygon not interior to it".to_string()));
        let line = summary(&[Timing { water: 2.0, answer: 4.0, together: 5.0 }]).unwrap();
        assert!(line.starts_with("WATER_BENCH points=1 water_p50_us=2.0 water_p99_us=2.0 answer_p50_us=4.0 "), "{line}");
        assert!(line.contains(" together_p50_us=5.0 ") && line.ends_with(" pointwise_over_answer_p50=1.25 pointwise_over_answer_p99=1.25"), "{line}");
    }
}
