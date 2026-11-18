use super::*;
use crate::test_rng::Rng;
use crate::{cell_id_at_level, cell_neighbors_at_level, DEFAULT_ADMIN_CELL_LEVEL};

const LEVEL: u64 = DEFAULT_ADMIN_CELL_LEVEL;
const GB: u16 = (b'G' as u16) << 8 | b'B' as u16;
const IE: u16 = (b'I' as u16) << 8 | b'E' as u16;

fn open(files: &(Vec<u8>, Vec<u8>)) -> Result<PostcodeFiles, String> {
    PostcodeFiles::open(FileBytes::Owned(files.0.clone()), FileBytes::Owned(files.1.clone()), LEVEL)
}

/// Centroids around a few towns (many to a cell, a thousand to some), half of them at four
/// decimals, a few far from their town, one in 30 of a second country, each its own postcode.
fn random_points(rng: &mut Rng, n: usize) -> Vec<Centroid> {
    let towns = [(51.5f64, -0.12f64), (52.37, 4.89), (0.0, 0.0), (-33.87, 151.21), (64.1, -21.9), (10.0, 179.99)];
    (0..n)
        .map(|i| {
            let (lat, lng) = towns[(rng.next() % towns.len() as u64) as usize];
            let spread = if rng.next() % 50 == 0 { 3.0 } else { 0.08 };
            let (mut a, mut g) = (lat + rng.range(-spread, spread), lng + rng.range(-spread, spread));
            if rng.next() % 2 == 0 {
                a = (a * 1e4).round() / 1e4;
                g = (g * 1e4).round() / 1e4;
            }
            Centroid {
                lat: a.clamp(-89.9, 89.9) as f32,
                lng: g.clamp(-180.0, 180.0) as f32,
                postcode_id: i as u32 * 13 + (rng.next() % 13) as u32,
                country_code: if rng.next() % 30 == 0 { IE } else { GB },
            }
        })
        .collect()
}

/// The rows `encode_points` writes for the points: each with its cell's key, in cell order.
fn logical(points: &[Centroid]) -> Vec<LogicalCentroid> {
    let mut rows: Vec<LogicalCentroid> = points
        .iter()
        .map(|c| LogicalCentroid {
            lat: c.lat,
            lng: c.lng,
            postcode_id: c.postcode_id,
            country_code: c.country_code,
            cell_key: (cell_id_at_level(c.lat as f64, c.lng as f64, LEVEL) >> 32) as u32,
        })
        .collect();
    rows.sort_by_key(|r| (r.cell_key, r.postcode_id, r.country_code));
    rows
}

fn same(a: &Centroid, b: &LogicalCentroid) -> bool {
    a.lat.to_bits() == b.lat.to_bits() && a.lng.to_bits() == b.lng.to_bits() && a.postcode_id == b.postcode_id
        && a.country_code == b.country_code
}

fn every_row(f: &PostcodeFiles) -> Vec<(u32, Centroid)> {
    let mut out = Vec::new();
    f.for_rows(0, f.rows(), |r, c| out.push((r, c)));
    out
}

#[test]
fn every_centroid_reads_back_at_its_row_and_in_its_cell() {
    let mut rng = Rng::new(20261117);
    for n in [0usize, 1, 2, 31, 32, 33, 64, 65, 3000] {
        let points = random_points(&mut rng, n);
        let rows = logical(&points);
        let files = encode(&rows);
        assert_eq!(files, encode_points(&points, LEVEL));
        let f = open(&files).unwrap();
        assert_eq!(f.rows() as usize, rows.len());
        let got = every_row(&f);
        assert_eq!(got.len(), rows.len(), "{n}");
        for (k, (r, c)) in got.iter().enumerate() {
            assert_eq!(*r as usize, k);
            assert!(same(c, &rows[k]), "row {k} of {n}");
        }
        // A cell's rows are those of its key.
        for (k, row) in rows.iter().enumerate() {
            let (first, end) = f.cell_rows((row.cell_key as u64) << 32);
            assert!(first as usize <= k && k < end as usize, "row {k}'s cell");
            assert!(rows[first as usize..end as usize].iter().all(|r| r.cell_key == row.cell_key));
        }
    }
}

#[test]
fn the_files_of_no_centroids_are_the_footer_and_an_empty_cells_file() {
    let files = encode(&[]);
    assert_eq!(files, (b"\0\0\0\0PCC1".to_vec(), Vec::new()));
    let f = open(&files).unwrap();
    assert_eq!(f.rows(), 0);
    let mut near = vec![NearBlock { block: 9, rows: 1, d: 0.0 }];
    f.blocks_near(&[cell_id_at_level(51.5, -0.12, LEVEL)], 51.5, -0.12, 1.0, &mut near);
    assert!(near.is_empty());
    assert_eq!(f.cell_rows(cell_id_at_level(51.5, -0.12, LEVEL)), (0, 0));
    // A cells file beside no rows is refused.
    let one = encode(&logical(&random_points(&mut Rng::new(3), 1)));
    assert!(open(&(files.0.clone(), one.1)).is_err());
}

#[test]
fn postcode_files_read_as_the_shared_vectors_say_and_the_test_encoder_writes_them() {
    let rows = crate::exact_pip::tests::testdata_rows("postcode_files.tsv");
    let hex = |s: &str| (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap()).collect::<Vec<u8>>();
    let bytes = |s: &str| if s == "-" { Vec::new() } else { hex(s) };
    let mut zero = false;
    for r in &rows {
        assert_eq!(r.len(), 4, "{r:?}");
        let logical: Vec<LogicalCentroid> = r[1]
            .split(';')
            .filter(|s| !s.is_empty() && *s != "-")
            .map(|s| {
                let f: Vec<&str> = s.split(',').collect();
                let h = |k: usize| u32::from_str_radix(f[k], 16).unwrap();
                LogicalCentroid {
                    lat: f32::from_bits(h(0)),
                    lng: f32::from_bits(h(1)),
                    postcode_id: f[2].parse().unwrap(),
                    country_code: h(3) as u16,
                    cell_key: h(4),
                }
            })
            .collect();
        let want = (bytes(&r[2]), bytes(&r[3]));
        assert_eq!(encode(&logical), want, "{}", r[0]);
        let f = open(&want).unwrap_or_else(|e| panic!("{}: {e}", r[0]));
        let got = every_row(&f);
        assert_eq!(got.len(), logical.len(), "{}", r[0]);
        for (k, (_, c)) in got.iter().enumerate() {
            assert!(same(c, &logical[k]), "{} row {k}", r[0]);
            let (first, end) = f.cell_rows((logical[k].cell_key as u64) << 32);
            assert!(first as usize <= k && k < end as usize, "{} row {k}'s cell", r[0]);
        }
        zero |= logical.is_empty();
    }
    assert!(rows.len() >= 14 && zero);
}

#[test]
fn files_no_builder_writes_are_refused_at_open() {
    let files = encode(&logical(&random_points(&mut Rng::new(7), 100)));
    let (c, k) = (&files.0, &files.1);
    let open2 = |c: &[u8], k: &[u8], level: u64| PostcodeFiles::open(FileBytes::Owned(c.to_vec()), FileBytes::Owned(k.to_vec()), level);
    assert!(open2(c, k, LEVEL).is_ok());
    assert!(open2(&c[..c.len() - 1], k, LEVEL).is_err());
    assert!(open2(&c[..5], k, LEVEL).is_err());
    assert!(open2(&[], &[], LEVEL).is_err());
    // A footer counting rows whose table the file can't hold.
    let mut more = c.clone();
    let at = more.len() - 8;
    more[at..at + 4].copy_from_slice(&1_000_000u32.to_le_bytes());
    assert!(open2(&more, k, LEVEL).err().unwrap().contains("shorter than its block table"));
    // Cells of another row count, level or kind.
    let mut fewer = c.clone();
    fewer[at..at + 4].copy_from_slice(&99u32.to_le_bytes());
    assert!(open2(&fewer, k, LEVEL).is_err());
    assert!(open2(c, k, LEVEL + 1).err().unwrap().contains("read at level 11"));
    let (lists, _) = crate::cell_pairs::write(&[(cell_id_at_level(51.5, -0.12, LEVEL), vec![1])], crate::cell_pairs::POI);
    assert!(open2(c, &lists, LEVEL).err().unwrap().contains("another kind of index"));
}

/// Every byte of small files changed and every cut either opens or is refused; what opens reads
/// without a panic, every row it gives whole.
#[test]
fn damaged_files_read_without_a_panic() {
    let mut rng = Rng::new(99);
    let points = random_points(&mut rng, 70);
    let files = encode(&logical(&points));
    let cells: Vec<u64> = points.iter().map(|c| cell_id_at_level(c.lat as f64, c.lng as f64, LEVEL)).collect();
    let read = |f: &PostcodeFiles| {
        let _ = every_row(f);
        for b in 0..f.rows().div_ceil(BLOCK_ROWS as u32) + 2 {
            f.rows_of(b, u32::MAX, |_, _| {});
        }
        let mut near = Vec::new();
        for c in points.iter().take(5) {
            f.blocks_near(&cells, c.lat as f64, c.lng as f64, 1.0, &mut near);
            for n in &near {
                f.rows_of(n.block, n.rows, |_, _| {});
            }
        }
        for &c in &cells {
            let (first, end) = f.cell_rows(c);
            assert!(first <= end && end <= f.rows());
        }
    };
    let (mut opened, mut refused) = (0, 0);
    for which in 0..2 {
        let len = if which == 0 { files.0.len() } else { files.1.len() };
        for at in 0..len {
            for bit in [0, 3, 7] {
                let mut g = files.clone();
                let file = if which == 0 { &mut g.0 } else { &mut g.1 };
                file[at] ^= 1 << bit;
                match open(&g) {
                    Ok(f) => {
                        opened += 1;
                        read(&f);
                    }
                    Err(_) => refused += 1,
                }
            }
        }
        for cut in 0..len {
            let mut g = files.clone();
            if which == 0 { g.0.truncate(cut) } else { g.1.truncate(cut) }
            if let Ok(f) = open(&g) {
                read(&f);
            }
        }
    }
    assert!(opened > 0 && refused > 0);
}

/// Each block's box, as the reader computes it, holds its rows' coordinates as f64s.
#[test]
fn a_blocks_box_holds_its_rows() {
    let mut rng = Rng::new(5);
    let points = random_points(&mut rng, 5000);
    let rows = logical(&points);
    let f = open(&encode(&rows)).unwrap();
    let mut open_sides = 0;
    for b in 0..f.rows().div_ceil(BLOCK_ROWS as u32) {
        let [lat_lo, lat_hi, lng_lo, lng_hi] = f.block_box(b).unwrap();
        open_sides += [lat_lo, lng_lo].iter().filter(|v| **v == f64::NEG_INFINITY).count()
            + [lat_hi, lng_hi].iter().filter(|v| **v == f64::INFINITY).count();
        f.rows_of(b, u32::MAX, |_, c| {
            assert!(lat_lo <= c.lat as f64 && c.lat as f64 <= lat_hi, "block {b}");
            assert!(lng_lo <= c.lng as f64 && c.lng as f64 <= lng_hi, "block {b}");
        });
    }
    assert!(open_sides > 0);
}

/// The nearest-box-first walk over the nine cells finds what a look at each of their rows finds,
/// reading fewer blocks where the cells are dense: queries at the towns, on rows (a box's edge),
/// beside the antimeridian and in empty sea.
#[test]
fn the_walk_finds_the_nearest_centroid_every_row_gives() {
    let mut rng = Rng::new(42);
    let mut points = random_points(&mut rng, 20_000);
    // A dense cell: a thousand centroids around one point.
    points.extend((0..1000).map(|i| Centroid {
        lat: (51.5072 + rng.range(-0.02, 0.02)) as f32,
        lng: (-0.1276 + rng.range(-0.02, 0.02)) as f32,
        postcode_id: 5_000_000 + i,
        country_code: GB,
    }));
    let rows = logical(&points);
    let f = open(&encode(&rows)).unwrap();
    let max_d = 0.05f64.to_radians().powi(2);
    let mut queries: Vec<(f64, f64)> = (0..3000).map(|k| {
        let p = &points[k * 7 % points.len()];
        (p.lat as f64 + rng.range(-0.03, 0.03), p.lng as f64 + rng.range(-0.03, 0.03))
    }).collect();
    queries.extend(points.iter().step_by(97).map(|p| (p.lat as f64, p.lng as f64)));
    queries.extend([(10.0, 179.999), (10.0, -179.999), (51.5072, -0.1276), (30.0, -40.0)]);
    let (mut decoded, mut held, mut nearest_third_or_later) = (0usize, 0usize, 0usize);
    let mut near = Vec::new();
    for (lat, lng) in queries {
        let cell = cell_id_at_level(lat, lng, LEVEL);
        let nine: Vec<u64> = std::iter::once(cell).chain(cell_neighbors_at_level(cell, LEVEL)).collect();
        let cos_lat = lat.to_radians().cos();
        let better = |best: Option<(f64, u32)>, d: f64, pid: u32| d < max_d && best.is_none_or(|b| (d, pid) < b);
        // Every row of the nine cells.
        let mut want: Option<(f64, u32)> = None;
        for &c in &nine {
            let (first, end) = f.cell_rows(c);
            if first < end {
                held += ((end - 1) / BLOCK_ROWS as u32 - first / BLOCK_ROWS as u32 + 1) as usize;
            }
            f.for_rows(first, end, |_, r| {
                let d = distance(lat, lng, cos_lat, r.lat as f64, r.lng as f64);
                if better(want, d, r.postcode_id) {
                    want = Some((d, r.postcode_id));
                }
            });
        }
        // The walk.
        let mut got: Option<(f64, u32)> = None;
        f.blocks_near(&nine, lat, lng, max_d, &mut near);
        assert!(near.iter().all(|n| near[0].d <= n.d), "the nearest first");
        assert!(near.iter().skip(1).is_sorted_by_key(|n| n.block), "the others in block order");
        nearest_third_or_later += (near.iter().filter(|n| n.block < near[0].block).count() >= 2) as usize;
        for n in &near {
            assert!(n.d < max_d && n.rows != 0);
            if got.is_some_and(|b| n.d > b.0) {
                continue;
            }
            decoded += 1;
            f.rows_of(n.block, n.rows, |_, r| {
                let d = distance(lat, lng, cos_lat, r.lat as f64, r.lng as f64);
                assert!(d >= n.d, "a row nearer than its box");
                if better(got, d, r.postcode_id) {
                    got = Some((d, r.postcode_id));
                }
            });
        }
        assert_eq!(got, want, "({lat}, {lng})");
    }
    assert!(decoded * 2 < held, "{decoded} blocks decoded of {held}");
    assert!(nearest_third_or_later > 0);
}

/// The nearest candidate goes first and the others keep their block order, wherever it was among
/// them: one cell's 128 rows in four blocks, each block's rows around a point of its own, the
/// third block's at the query.
#[test]
fn the_nearest_block_goes_first_and_the_others_keep_their_order() {
    let (lat, lng) = (52.37f64, 4.89f64);
    let spots = [(0.002, 0.0), (0.0, 0.002), (0.0, 0.0), (-0.002, 0.0)];
    let points: Vec<Centroid> = (0..128u32)
        .map(|i| {
            let (dlat, dlng) = spots[i as usize / BLOCK_ROWS];
            Centroid { lat: (lat + dlat + (i % 32) as f64 * 1e-5) as f32, lng: (lng + dlng) as f32, postcode_id: i, country_code: GB }
        })
        .collect();
    let cell = cell_id_at_level(lat, lng, LEVEL);
    assert!(points.iter().all(|p| cell_id_at_level(p.lat as f64, p.lng as f64, LEVEL) == cell));
    let f = open(&encode_points(&points, LEVEL)).unwrap();
    let mut near = Vec::new();
    f.blocks_near(&[cell], lat, lng, 1.0, &mut near);
    assert_eq!(near.iter().map(|n| n.block).collect::<Vec<_>>(), [2, 0, 1, 3]);
}

/// A level past 14 would cut its cells' ids short of their keys.
#[test]
#[should_panic(expected = "postcode centroid cells of level 15")]
fn the_test_encoder_takes_no_level_past_14() {
    encode_points(&random_points(&mut Rng::new(1), 3), 15);
}

#[test]
fn a_mask_gives_its_rows_of_the_block_and_no_others() {
    let rows = logical(&random_points(&mut Rng::new(11), 200));
    let f = open(&encode(&rows)).unwrap();
    let all = every_row(&f);
    for b in 0..f.rows().div_ceil(BLOCK_ROWS as u32) {
        for mask in [0u32, 1, 2, 1 << 31, 0x8000_0001, 0x0F0F_0F0F, u32::MAX] {
            let mut got = Vec::new();
            f.rows_of(b, mask, |r, c| got.push((r, c)));
            let want: Vec<(u32, Centroid)> =
                all.iter().filter(|(r, _)| r / BLOCK_ROWS as u32 == b && mask >> (r % BLOCK_ROWS as u32) & 1 != 0).copied().collect();
            assert_eq!(got, want, "block {b} mask {mask:#x}");
        }
    }
    let mut none = 0;
    f.rows_of(f.rows().div_ceil(BLOCK_ROWS as u32), u32::MAX, |_, _| none += 1);
    assert_eq!(none, 0);
}

/// Two cells whose rows share a block are one candidate holding both cells' rows; a cell listed
/// twice is one.
#[test]
fn a_block_two_cells_share_is_one_candidate() {
    let a = (52.37f32, 4.89f32);
    let b = (52.37f32, 5.2f32);
    let (ca, cb) = (cell_id_at_level(a.0 as f64, a.1 as f64, LEVEL), cell_id_at_level(b.0 as f64, b.1 as f64, LEVEL));
    assert_ne!(ca, cb);
    let points: Vec<Centroid> = (0..10)
        .map(|i| {
            let (lat, lng) = if i < 5 { a } else { b };
            Centroid { lat: lat + i as f32 * 1e-4, lng, postcode_id: i, country_code: GB }
        })
        .collect();
    let f = open(&encode_points(&points, LEVEL)).unwrap();
    let mut near = Vec::new();
    f.blocks_near(&[ca, cb, ca], 52.37, 5.0, 1.0, &mut near);
    assert_eq!(near.len(), 1);
    assert_eq!(near[0].rows, 0x3FF);
    f.blocks_near(&[cb], 52.37, 5.0, 1.0, &mut near);
    let (first, end) = f.cell_rows(cb);
    assert_eq!((near.len(), near[0].rows), (1, ((1u32 << end) - 1) & !((1u32 << first) - 1)));
}
