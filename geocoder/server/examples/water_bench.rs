// Times a water lookup (a water dir of the builder's water layer, read with the admin readers:
// one cell read, then the ring test of each entry not flagged interior) against the same point's
// answer without water, at the cells whose non-interior entries carry the most rings: the lakes
// and bays with thousands of islands, whose holes a ring test visits one by one.
//
//   cargo build --release --example water_bench
//   ./target/release/examples/water_bench <dataset dir> <water dir> [cells] [rounds] > report.txt
//
// <dataset dir> is a flat dir grid_query loads (a mode's files beside a boundary tier); <water
// dir> is one of {region}/water/{all,major}/{simplified,exact}. The cells: of the polygons with
// the largest ring blocks (200), the L10 cells they reach where their entry is not interior, scored
// by the rings of every non-interior entry there; the `cells` best (default 40), each sampled at
// 5 x 5 points. Each point is answered `rounds` times (default 200) warm, by each path, and its
// median kept. Prints each cell, then p50 and p99 over the points of both paths, of the two
// together, and of the two together against the answer alone, point by point and as quantiles.
use query_server::{
    cell_id_at_level, cell_pairs, mmap_file, AdminGeometry, AdminPoint, AdminPolygon, CellPairs, MultiIndex, PipMemo,
    ADMIN_POLYGON_RECORD_BYTES, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL, ID_MASK,
    INTERIOR_FLAG,
};
use s2::cell::Cell;
use s2::cellid::CellID;
use s2::latlng::LatLng;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

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

    /// The water holding the point: the smallest area, then the lower type code (the record
    /// order's last word), as the layer's answer picks it; the polygon id.
    fn at(&self, lat: f64, lng: f64) -> Option<u32> {
        let point = AdminPoint::new(lat, lng);
        let (lat, lng) = point.degrees();
        let mut memo = PipMemo::new();
        let mut best: Option<(f32, u16, u32)> = None;
        self.index.with_ids(cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL), |ids| {
            for &id in ids {
                let poly = &self.records[(id & ID_MASK) as usize];
                if poly.ring_bytes == 0 {
                    continue;
                }
                let key = (poly.area, poly.country_code, id & ID_MASK);
                if best.is_some_and(|b| (b.0, b.1, b.2) <= key) {
                    continue;
                }
                if id & INTERIOR_FLAG != 0 || self.geometry.contains(poly, point, &mut memo) {
                    best = Some(key);
                }
            }
        });
        best.map(|b| b.2)
    }
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
    let index = MultiIndex::load(dataset, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE)
        .expect("dataset");

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

    let (mut with, mut without, mut ratio, mut both) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
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
        let (mut cell_with, mut cell_without) = (Vec::new(), Vec::new());
        let mut held = 0;
        for i in 0..5 {
            for j in 0..5 {
                let lat = lo_lat + (hi_lat - lo_lat) * (0.1 + 0.2 * i as f64);
                let lng = lo_lng + (hi_lng - lo_lng) * (0.1 + 0.2 * j as f64);
                if cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL) != cell {
                    continue;
                }
                held += usize::from(water.at(lat, lng).is_some());
                let mut w = Vec::new();
                let mut a = Vec::new();
                for _ in 0..rounds {
                    let t = Instant::now();
                    std::hint::black_box(water.at(lat, lng));
                    w.push(t.elapsed().as_secs_f64() * 1e6);
                    let t = Instant::now();
                    std::hint::black_box(index.query_json(lat, lng));
                    a.push(t.elapsed().as_secs_f64() * 1e6);
                }
                let (mw, ma) = (median(w), median(a));
                cell_with.push(mw);
                cell_without.push(ma);
                with.push(mw);
                without.push(ma);
                both.push(mw + ma);
                ratio.push((mw + ma) / ma);
            }
        }
        if cell_with.is_empty() {
            continue;
        }
        let (lat, lng) = (LatLng::from(c.center()).lat.deg(), LatLng::from(c.center()).lng.deg());
        println!(
            "cell {cell:016x} at {lat:.4} {lng:.4}: {} entries, {entries} not interior with {score} rings; {held} of {} points in water; water {:.1} us, answer {:.1} us (medians)",
            ids.len(),
            cell_with.len(),
            median(cell_with),
            median(cell_without)
        );
    }
    println!(
        "WATER_BENCH points={} water_p50_us={:.1} water_p99_us={:.1} answer_p50_us={:.1} answer_p99_us={:.1} together_p50_us={:.1} together_p99_us={:.1} together_p99_over_answer_p99={:.2} pointwise_over_answer_p50={:.2} pointwise_over_answer_p99={:.2}",
        with.len(),
        quantile(&with, 0.5),
        quantile(&with, 0.99),
        quantile(&without, 0.5),
        quantile(&without, 0.99),
        quantile(&both, 0.5),
        quantile(&both, 0.99),
        quantile(&both, 0.99) / quantile(&without, 0.99),
        quantile(&ratio, 0.5),
        quantile(&ratio, 0.99)
    );
}
