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
// a water dir; <water dir> is one of {region}/water/{all,major}/{simplified,exact}. The dataset
// loads twice, as the server loads a flat dir (one world slot): without water, and with the
// water dir's layer as its index's water. The cells: of the polygons with the largest ring
// blocks (200), the L10 cells they reach where their entry is not interior, scored by the rings
// of every non-interior entry there; the `cells` best (default 40), each sampled at 5 x 5
// points. Each point's answer with water names the water the lookup finds there, and the one
// without names none. Each point is timed `rounds` times (default 200) warm, by each of the
// three, and its medians kept. Prints each cell, then p50 and p99 over the points of the lookup
// and of both answers, and of the answer with water against the one without, point by point and
// as quantiles.
use query_server::{
    cell_id_at_level, cell_pairs, mmap_file, AdminGeometry, AdminPolygon, CellPairs, ContinentSlot, Index,
    MultiIndex, WaterLayer, ADMIN_POLYGON_RECORD_BYTES, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE,
    DEFAULT_STREET_CELL_LEVEL, ID_MASK, INTERIOR_FLAG,
};
use s2::cell::Cell;
use s2::cellid::CellID;
use s2::latlng::LatLng;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

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

/// The flat dataset dir `dir` as the server loads it (MultiIndex::load: one world slot), `water`
/// its index's water layer.
fn world(dir: &str, water: Option<WaterLayer>) -> MultiIndex {
    let index = Index::load(dir, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE);
    let mut index = index.expect("dataset");
    index.water = water;
    MultiIndex {
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
    }
}

/// The name of the water an answer's JSON holds, if it holds one.
fn named_water(json: &str) -> Option<String> {
    let answer: serde_json::Value = serde_json::from_str(json).expect("an answer's JSON");
    answer.get("water").map(|w| w["name"].as_str().expect("a water name").to_string())
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

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (dataset, water_dir) = (&args[1], &args[2]);
    let cells: usize = args.get(3).map_or(40, |a| a.parse().unwrap());
    let rounds: usize = args.get(4).map_or(200, |a| a.parse().unwrap());
    let water = Water::load(water_dir);
    let open =
        || WaterLayer::open(water_dir, DEFAULT_ADMIN_CELL_LEVEL).expect("water dir").expect("a water dir's files");
    let layer = open();
    let (dry, wet) = (world(dataset, None), world(dataset, Some(open())));

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
    best.truncate(cells);

    let (mut lookup, mut without, mut with, mut ratio) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for &(cell, score) in &best {
        let c = Cell::from(CellID(cell));
        let (mut lo_lat, mut hi_lat, mut lo_lng, mut hi_lng) = (90.0f64, -90.0f64, 180.0f64, -180.0f64);
        for k in 0..4 {
            let v = LatLng::from(c.vertex(k));
            lo_lat = lo_lat.min(v.lat.deg());
            hi_lat = hi_lat.max(v.lat.deg());
            lo_lng = lo_lng.min(v.lng.deg());
            hi_lng = hi_lng.max(v.lng.deg());
        }
        let ids = water.index.ids(cell);
        let entries = ids.iter().filter(|&&id| id & INTERIOR_FLAG == 0).count();
        let (mut cell_lookup, mut cell_without, mut cell_with) = (Vec::new(), Vec::new(), Vec::new());
        let mut held = 0;
        for i in 0..5 {
            for j in 0..5 {
                let lat = lo_lat + (hi_lat - lo_lat) * (0.1 + 0.2 * i as f64);
                let lng = lo_lng + (hi_lng - lo_lng) * (0.1 + 0.2 * j as f64);
                if cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL) != cell {
                    continue;
                }
                let water = layer.at(lat, lng).map(|w| w.name.into_owned());
                held += usize::from(water.is_some());
                let dry_water = named_water(&dry.query_json(lat, lng));
                let wet_water = named_water(&wet.query_json(lat, lng));
                assert_eq!(dry_water, None, "the water the answer without water at ({lat}, {lng}) names");
                assert_eq!(wet_water, water, "the water the answer with water at ({lat}, {lng}) names");
                let (mut l, mut d, mut w) = (Vec::new(), Vec::new(), Vec::new());
                for _ in 0..rounds {
                    let t = Instant::now();
                    std::hint::black_box(layer.at(lat, lng));
                    l.push(t.elapsed().as_secs_f64() * 1e6);
                    let t = Instant::now();
                    std::hint::black_box(dry.query_json(lat, lng));
                    d.push(t.elapsed().as_secs_f64() * 1e6);
                    let t = Instant::now();
                    std::hint::black_box(wet.query_json(lat, lng));
                    w.push(t.elapsed().as_secs_f64() * 1e6);
                }
                let (ml, md, mw) = (median(l), median(d), median(w));
                cell_lookup.push(ml);
                cell_without.push(md);
                cell_with.push(mw);
                lookup.push(ml);
                without.push(md);
                with.push(mw);
                ratio.push(mw / md);
            }
        }
        if cell_lookup.is_empty() {
            continue;
        }
        let (lat, lng) = (LatLng::from(c.center()).lat.deg(), LatLng::from(c.center()).lng.deg());
        println!(
            "cell {cell:016x} at {lat:.4} {lng:.4}: {} entries, {entries} not interior with {score} rings; {held} of {} points in water; lookup {:.1} us, answer without water {:.1} us, with water {:.1} us (medians)",
            ids.len(),
            cell_lookup.len(),
            median(cell_lookup),
            median(cell_without),
            median(cell_with)
        );
    }
    println!(
        "WATER_BENCH points={} lookup_p50_us={:.1} lookup_p99_us={:.1} dry_p50_us={:.1} dry_p99_us={:.1} wet_p50_us={:.1} wet_p99_us={:.1} wet_p99_over_dry_p99={:.2} pointwise_wet_over_dry_p50={:.2} pointwise_wet_over_dry_p99={:.2}",
        lookup.len(),
        quantile(&lookup, 0.5),
        quantile(&lookup, 0.99),
        quantile(&without, 0.5),
        quantile(&without, 0.99),
        quantile(&with, 0.5),
        quantile(&with, 0.99),
        quantile(&with, 0.99) / quantile(&without, 0.99),
        quantile(&ratio, 0.5),
        quantile(&ratio, 0.99)
    );
}
