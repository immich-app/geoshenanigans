use super::*;
use crate::coord_keys::ONE_BITS;
use crate::test_rng::Rng;
use crate::NO_DATA;

type Ways = Vec<(WayRow, Vec<NodeCoord>)>;

const KINDS: [Kind; 2] = [Kind::Streets, Kind::Interps];

fn open(kind: Kind, ways: &[u8], nodes: &[u8]) -> Result<WayFiles, String> {
    WayFiles::open(kind, FileBytes::Owned(ways.to_vec()), FileBytes::Owned(nodes.to_vec()))
}

fn files(kind: Kind, b: &WayFileBytes) -> WayFiles {
    open(kind, &b.ways, &b.nodes).unwrap_or_else(|e| panic!("{e}"))
}

fn node(lat: u32, lng: u32) -> NodeCoord {
    NodeCoord { lat: f32::from_bits(lat), lng: f32::from_bits(lng) }
}

fn bits(nodes: &[NodeCoord]) -> Vec<(u32, u32)> {
    nodes.iter().map(|c| (c.lat.to_bits(), c.lng.to_bits())).collect()
}

/// The bits of a coordinate of seven decimals, which has a hybrid key.
fn e7(v: i64) -> u32 {
    ((v as f64 / 1e7) as f32).to_bits()
}

#[derive(Clone, Copy)]
enum Shape {
    SevenDecimals,
    NearZero,
    AnyBits,
    OneNodeOver,
}

/// The nodes of a way of `n` nodes: a walk over coordinates of seven decimals (each has a hybrid
/// key), floats near the equator and the prime meridian (few have one), any bit patterns, or one
/// node over and over.
fn random_nodes(rng: &mut Rng, n: usize, shape: Shape) -> Vec<NodeCoord> {
    let mut lat = (rng.next() % 1_700_000_001) as i64 - 850_000_000;
    let mut lng = (rng.next() % 3_600_000_001) as i64 - 1_800_000_000;
    let mut step_lat = (rng.next() % 4001) as i64 - 2000;
    let mut step_lng = (rng.next() % 4001) as i64 - 2000;
    let zero_lat = 0.3f32.to_bits() + (rng.next() % 100_000) as u32;
    let zero_lng = (0.6f32.to_bits() + (rng.next() % 100_000) as u32) | 0x8000_0000;
    (0..n)
        .map(|j| match shape {
            Shape::SevenDecimals => {
                let c = node(e7(lat), e7(lng));
                step_lat += (rng.next() % 41) as i64 - 20;
                step_lng += (rng.next() % 41) as i64 - 20;
                lat += step_lat;
                lng += step_lng;
                c
            }
            Shape::NearZero => {
                let (dl, dg) = ((rng.next() % 5000) as u32, (rng.next() % 5000) as u32);
                node(zero_lat + j as u32 * 3000 + dl, zero_lng - j as u32 * 3000 - dg)
            }
            Shape::AnyBits => node(rng.next() as u32, rng.next() as u32),
            Shape::OneNodeOver => node(e7(lat), e7(lng)),
        })
        .collect()
}

/// `n` ways of the kind: every eighth without nodes, some of one node and of two, names of every
/// width, an interpolation's numbers either way round.
fn random_ways(rng: &mut Rng, kind: Kind, n: usize) -> Ways {
    (0..n)
        .map(|_| {
            let count = match rng.next() % 16 {
                0 | 1 => 0,
                2 => 1,
                3 => 2,
                _ => 3 + (rng.next() % 40) as usize,
            };
            let shape = match rng.next() % 8 {
                0..=4 => Shape::SevenDecimals,
                5 => Shape::NearZero,
                6 => Shape::AnyBits,
                _ => Shape::OneNodeOver,
            };
            let nodes = random_nodes(rng, count, shape);
            let mut row = WayRow { node_count: count as u16, name: NO_DATA, start_number: 0, end_number: 0, interpolation: 0 };
            if count > 0 {
                row.name = (rng.next() >> (32 + rng.next() % 32)) as u32;
            }
            if kind == Kind::Interps {
                row.start_number = (rng.next() % 3000) as u32;
                row.end_number = (rng.next() % 3000) as u32;
                row.interpolation = (rng.next() % 4) as u8;
            }
            (row, nodes)
        })
        .collect()
}

/// Every way of the files as the reader gives it, and that each read path agrees on it.
fn read_all(f: &WayFiles) -> Ways {
    // A buffer that holds another way's nodes.
    let mut buf = vec![node(1, 2); 3];
    (0..f.count())
        .map(|id| {
            let row = f.row(id).unwrap_or_else(|| panic!("way {id} has no row"));
            assert!(f.way(id, &mut buf), "way {id}");
            assert_eq!(buf.len(), row.node_count as usize, "way {id}");
            assert_eq!(f.first_node(id).map(|c| (c.lat.to_bits(), c.lng.to_bits())), bits(&buf).first().copied(), "way {id}");
            (row, buf.clone())
        })
        .collect()
}

fn same(a: &Ways, b: &Ways) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.0 == y.0 && bits(&x.1) == bits(&y.1))
}

/// A reader that takes the files a bit at a time, in id order, as the format is laid out:
/// every way's row fields and nodes.
fn read_bit_by_bit(kind: Kind, rows: &[u8], nodes: &[u8]) -> Ways {
    let le = |bytes: &[u8], at: usize, n: usize| (0..n).fold(0u64, |v, k| v | (bytes[at + k] as u64) << (8 * k));
    let field = |at: usize, w: usize| (0..w).fold(0u32, |v, k| v | ((nodes[(at + k) / 8] >> ((at + k) % 8) & 1) as u32) << k);
    let count = le(rows, rows.len() - 8, 4) as usize;
    let per_way = if kind == Kind::Interps { 17 } else { 8 };
    let mut out = Vec::new();
    let mut row_at = 0;
    for g in 0..count.div_ceil(16) {
        let n = 16.min(count - 16 * g);
        let mut block_at = le(rows, row_at, 4) as usize;
        let (anchor_lat, anchor_lng) = (le(rows, row_at + 4, 4) as u32, le(rows, row_at + 8, 4) as u32);
        let numbers_at = row_at + 12 + 4 * n;
        let names_at = numbers_at + if kind == Kind::Interps { 9 * n } else { 0 };
        for j in 0..n {
            let cnt = le(rows, row_at + 12 + 4 * j, 2) as usize;
            let code = le(rows, row_at + 14 + 4 * j, 2) as usize;
            let (wl, wg, hybrid, wf) = (code & 63, code >> 6 & 63, code >> 12 & 1 == 1, code >> 13);
            let mut way = Vec::new();
            if cnt > 0 {
                let (zl, zg) = (le(nodes, block_at, wf) as u32, le(nodes, block_at + wf, wf) as u32);
                let lat = from_order_key(order_key(anchor_lat).wrapping_add(unzigzag32(zl)));
                let lng = from_order_key(order_key(anchor_lng).wrapping_add(unzigzag32(zg)));
                way.push(node(lat, lng));
                let key = |b: u32| if hybrid { hybrid_key(b).unwrap() } else { order_key(b) };
                let pattern = |k: u32| if hybrid { from_hybrid_key(k) } else { from_order_key(k) };
                let (mut key_lat, mut key_lng, mut step_lat, mut step_lng) = (key(lat), key(lng), 0u32, 0u32);
                let values_at = 8 * (block_at + 2 * wf);
                for k in 0..cnt - 1 {
                    step_lat = step_lat.wrapping_add(unzigzag32(field(values_at + k * wl, wl)));
                    step_lng = step_lng.wrapping_add(unzigzag32(field(values_at + (cnt - 1) * wl + k * wg, wg)));
                    key_lat = key_lat.wrapping_add(step_lat);
                    key_lng = key_lng.wrapping_add(step_lng);
                    way.push(node(pattern(key_lat), pattern(key_lng)));
                }
                block_at += 2 * wf + ((cnt - 1) * (wl + wg)).div_ceil(8);
            }
            let mut row = WayRow { node_count: cnt as u16, name: le(rows, names_at + 4 * j, 4) as u32, start_number: 0, end_number: 0, interpolation: 0 };
            if kind == Kind::Interps {
                row.start_number = le(rows, numbers_at + 9 * j, 4) as u32;
                row.end_number = row.start_number.wrapping_add(le(rows, numbers_at + 9 * j + 4, 4) as u32);
                row.interpolation = rows[numbers_at + 9 * j + 8];
            }
            out.push((row, way));
        }
        row_at += 12 + per_way * n;
    }
    assert_eq!(row_at + 8, rows.len());
    out
}

#[test]
fn every_way_reads_back_at_its_id_whatever_its_place_in_a_group() {
    let mut rng = Rng::new(20261114);
    for kind in KINDS {
        for n in [0usize, 1, 15, 16, 17, 3000] {
            let ways = random_ways(&mut rng, kind, n);
            let bytes = encode(kind, &ways);
            // A row a group of 16, the last shorter, and the two footers.
            assert_eq!(bytes.ways.len(), 12 * n.div_ceil(16) + kind.row_way_bytes() * n + 8);
            assert!(bytes.nodes.len() >= 16);
            let f = files(kind, &bytes);
            assert_eq!(f.count() as usize, n);
            assert!(same(&read_all(&f), &ways), "{n} {kind:?}");
            // Past the last way there is none, and a buffer comes back empty.
            let mut buf = vec![node(1, 2)];
            for id in [n as u32, n as u32 + 1, n as u32 + 16, NO_DATA] {
                assert_eq!(f.row(id), None);
                assert!(!f.way(id, &mut buf) && buf.is_empty());
                assert!(f.first_node(id).is_none());
            }
        }
    }
}

#[test]
fn ways_of_no_one_two_and_the_most_nodes_read_back() {
    let mut rng = Rng::new(3);
    for kind in KINDS {
        let shapes = [Shape::SevenDecimals, Shape::SevenDecimals, Shape::NearZero, Shape::SevenDecimals, Shape::AnyBits, Shape::OneNodeOver];
        let counts = [0usize, 1, 2, 65535, 65535, 65535];
        let ways: Ways = (0..counts.len())
            .map(|i| {
                let row = WayRow { node_count: counts[i] as u16, name: i as u32, start_number: 0, end_number: 0, interpolation: 0 };
                (row, random_nodes(&mut rng, counts[i], shapes[i]))
            })
            .collect();
        let bytes = encode(kind, &ways);
        let f = files(kind, &bytes);
        assert!(same(&read_all(&f), &ways));
        // A way without nodes reads, as no nodes; it has no first node.
        let mut buf = vec![node(1, 2)];
        assert!(f.way(0, &mut buf) && buf.is_empty());
        assert!(f.first_node(0).is_none());
        // Any bit patterns fill the widths: the largest block a way has.
        let code = u16_at(&bytes.ways, 12 + 4 * 4 + 2);
        assert_eq!((code & 63, code >> 6 & 63, code >> 12 & 1), (32, 32, 0));
        assert_eq!(block_bytes(65535, code), 2 * u64::from(code >> 13) + 65534 * 8);
    }
}

#[test]
fn an_interpolations_numbers_and_a_names_edges_read_back() {
    let two = vec![node(52.5f32.to_bits(), 4.5f32.to_bits()), node(52.6f32.to_bits(), 4.6f32.to_bits())];
    let ways: Ways = vec![
        // The end below the start: the span wraps.
        (WayRow { node_count: 2, name: 0, start_number: 10, end_number: 2, interpolation: 255 }, two.clone()),
        (WayRow { node_count: 0, name: NO_DATA, start_number: NO_DATA, end_number: 0, interpolation: 0 }, vec![]),
        (WayRow { node_count: 1, name: NO_DATA - 1, start_number: 0, end_number: NO_DATA, interpolation: 1 }, vec![node(0x8000_0000, 0)]),
    ];
    let f = files(Kind::Interps, &encode(Kind::Interps, &ways));
    assert!(same(&read_all(&f), &ways));
    // A street has no numbers: its row gives 0.
    let streets = files(Kind::Streets, &encode(Kind::Streets, &ways));
    let read = read_all(&streets);
    for (id, (row, nodes)) in ways.iter().enumerate() {
        assert_eq!(read[id].0, WayRow { start_number: 0, end_number: 0, interpolation: 0, ..*row });
        assert_eq!(bits(&read[id].1), bits(nodes));
    }
}

#[test]
fn files_no_builder_writes_are_refused_at_open_by_name() {
    let said = |kind: Kind, ways: &[u8], nodes: &[u8]| open(kind, ways, nodes).err().unwrap_or_default();
    let streets = encode(Kind::Streets, &random_ways(&mut Rng::new(7), Kind::Streets, 40));
    let interps = encode(Kind::Interps, &random_ways(&mut Rng::new(8), Kind::Interps, 40));
    let (w, n) = (&streets.ways, &streets.nodes);
    assert_eq!(said(Kind::Streets, w, n), "");
    assert_eq!(said(Kind::Interps, &interps.ways, &interps.nodes), "");

    // A footer cut, or with another magic.
    assert_eq!(said(Kind::Streets, &w[..w.len() - 1], n), "street_ways.bin has no SWY1 footer");
    assert_eq!(said(Kind::Streets, &w[..7], n), "street_ways.bin has no SWY1 footer");
    assert_eq!(said(Kind::Streets, &[], n), "street_ways.bin has no SWY1 footer");
    assert_eq!(said(Kind::Streets, w, &n[..n.len() - 1]), "street_nodes.bin has no SWN1 footer");
    assert_eq!(said(Kind::Streets, w, &n[..15]), "street_nodes.bin has no SWN1 footer");
    assert_eq!(said(Kind::Streets, w, &[]), "street_nodes.bin has no SWN1 footer");
    // Each kind's files are its own.
    assert_eq!(said(Kind::Interps, w, n), "interp_ways.bin has no IWY1 footer");
    assert_eq!(said(Kind::Interps, &interps.ways, n), "interp_nodes.bin has no IWN1 footer");
    assert_eq!(said(Kind::Streets, &interps.ways, n), "street_ways.bin has no SWY1 footer");
    assert_eq!(said(Kind::Streets, w, &interps.nodes), "street_nodes.bin has no SWN1 footer");
    // Open reads the footers alone: a nodes file cut to its footer opens.
    assert_eq!(said(Kind::Streets, w, &n[n.len() - 16..]), "");

    // Footers that count different ways.
    let counting = |bytes: &[u8], count: u32| {
        let mut b = bytes.to_vec();
        let at = b.len() - 8;
        b[at..at + 4].copy_from_slice(&count.to_le_bytes());
        b
    };
    assert_eq!(said(Kind::Streets, &counting(w, 41), n), "street_ways.bin and street_nodes.bin count different ways");
    assert_eq!(said(Kind::Streets, w, &counting(n, 39)), "street_ways.bin and street_nodes.bin count different ways");
    // A rows file that is not the rows of the ways it counts: a way more or fewer, a byte more
    // or fewer, a count no file holds.
    assert_eq!(said(Kind::Streets, &counting(w, 41), &counting(n, 41)), "street_ways.bin is not the rows of its 41 ways");
    assert_eq!(said(Kind::Streets, &counting(w, 39), &counting(n, 39)), "street_ways.bin is not the rows of its 39 ways");
    assert_eq!(said(Kind::Streets, &counting(w, 0), &counting(n, 0)), "street_ways.bin is not the rows of its 0 ways");
    assert_eq!(said(Kind::Streets, &counting(w, u32::MAX), &counting(n, u32::MAX)), "street_ways.bin is not the rows of its 4294967295 ways");
    assert_eq!(said(Kind::Streets, &w[1..], n), "street_ways.bin is not the rows of its 40 ways");
    let mut longer = vec![0u8];
    longer.extend_from_slice(w);
    assert_eq!(said(Kind::Streets, &longer, n), "street_ways.bin is not the rows of its 40 ways");
    assert_eq!(said(Kind::Interps, &interps.ways[1..], &interps.nodes), "interp_ways.bin is not the rows of its 40 ways");
}

#[test]
fn a_way_whose_block_no_builder_writes_reads_as_none_and_its_row_still_reads() {
    // One group of three streets: two nodes, none, two nodes. All four coordinates are above 1.0,
    // so both ways are under the hybrid key.
    let near = |d: u32| vec![node(52.0f32.to_bits() + d, 4.0f32.to_bits() + d), node(52.0f32.to_bits() + d + 9, 4.0f32.to_bits())];
    let row = |name: u32, node_count: u16| WayRow { node_count, name, start_number: 0, end_number: 0, interpolation: 0 };
    let ways: Ways = vec![(row(1, 2), near(0)), (row(2, 0), vec![]), (row(3, 2), near(300))];
    let bytes = encode(Kind::Streets, &ways);
    assert!(same(&read_all(&files(Kind::Streets, &bytes)), &ways));
    let with = |edit: &dyn Fn(&mut Vec<u8>, &mut Vec<u8>)| {
        let (mut rows, mut nodes) = (bytes.ways.clone(), bytes.nodes.clone());
        edit(&mut rows, &mut nodes);
        open(Kind::Streets, &rows, &nodes).unwrap_or_else(|e| panic!("{e}"))
    };
    let mut buf = Vec::new();
    let code_at = |j: usize| 12 + 4 * j + 2;
    let set_code = |rows: &mut Vec<u8>, j: usize, code: u16| rows[code_at(j)..code_at(j) + 2].copy_from_slice(&code.to_le_bytes());
    let code = |j: usize| u16_at(&bytes.ways, code_at(j));
    assert_eq!((code(0) >> 12 & 1, code(2) >> 12 & 1, code(2) >> 13), (1, 1, 2));

    // A width past 32 bits, or a first node in more than 4 bytes: the way has no nodes to give,
    // and the ways beside it read.
    for bad in [33u16, 33 << 6, 5 << 13] {
        let f = with(&|rows, _| set_code(rows, 2, bad));
        assert!(!f.way(2, &mut buf) && buf.is_empty());
        assert!(f.first_node(2).is_none());
        assert_eq!(f.row(2), Some(ways[2].0));
        assert!(f.way(0, &mut buf) && bits(&buf) == bits(&ways[0].1));
    }
    // A block that runs past the blocks' end, by its own count or by the group's off.
    let f = with(&|rows, _| rows[12 + 4 * 2..14 + 4 * 2].copy_from_slice(&1000u16.to_le_bytes()));
    assert!(!f.way(2, &mut buf) && buf.is_empty() && f.first_node(2).is_none());
    assert_eq!(f.row(2).map(|r| r.node_count), Some(1000));
    let f = with(&|rows, _| rows[..4].copy_from_slice(&u32::MAX.to_le_bytes()));
    for id in 0..3 {
        // The way without nodes has none to read.
        assert!(f.way(id, &mut buf) == (id == 1) && buf.is_empty() && f.first_node(id).is_none());
        assert_eq!(f.row(id), Some(ways[id as usize].0));
    }
    // A nodes file cut to its footer has no block to give.
    let f = with(&|_, nodes| {
        nodes.drain(..nodes.len() - 16);
    });
    assert!(!f.way(0, &mut buf) && f.first_node(0).is_none() && f.way(1, &mut buf));
    // A way under the hybrid key whose first node has none: the anchor moved to a float between
    // two decimals. Its first node still reads.
    let between = 0.1f32.to_bits() + 1;
    assert!(between < ONE_BITS && hybrid_key(between).is_none());
    let f = with(&|rows, _| rows[4..8].copy_from_slice(&between.to_le_bytes()));
    assert!(!f.way(0, &mut buf) && buf.is_empty());
    assert_eq!(f.first_node(0).map(|c| c.lat.to_bits()), Some(between));
}

#[test]
fn damaged_rows_and_blocks_read_without_a_panic() {
    let mut rng = Rng::new(99);
    for kind in KINDS {
        let bytes = encode(kind, &random_ways(&mut rng, kind, 300));
        let mut buf = Vec::new();
        let (mut read, mut unread) = (0usize, 0usize);
        for round in 0..2000 {
            let (mut rows, mut nodes) = (bytes.ways.clone(), bytes.nodes.clone());
            for _ in 0..1 + rng.next() % 4 {
                // Keep the footers, so the files open.
                let at = (rng.next() % (rows.len() as u64 - 8)) as usize;
                rows[at] = rng.next() as u8;
                let at = (rng.next() % (nodes.len() as u64 - 16)) as usize;
                nodes[at] = rng.next() as u8;
            }
            if round % 10 == 0 {
                // And a nodes file that lost its first blocks.
                let cut = (rng.next() % (nodes.len() as u64 - 16)) as usize;
                nodes.drain(..cut);
            }
            let f = open(kind, &rows, &nodes).unwrap_or_else(|e| panic!("{e}"));
            for id in 0..f.count() + 2 {
                let row = f.row(id);
                assert_eq!(row.is_some(), id < f.count());
                let whole = f.way(id, &mut buf);
                let first = f.first_node(id);
                if whole {
                    // The nodes the row counts, the first of them the first node.
                    assert_eq!(buf.len(), row.unwrap().node_count as usize);
                    assert_eq!(first.map(|c| (c.lat.to_bits(), c.lng.to_bits())), bits(&buf).first().copied());
                    read += 1;
                } else {
                    assert!(buf.is_empty());
                    unread += 1;
                }
            }
        }
        assert!(read > 100_000 && unread > 10_000, "{read} {unread}");
    }
}

#[test]
fn the_reader_gives_what_a_bit_at_a_time_reader_does() {
    let mut rng = Rng::new(5);
    for kind in KINDS {
        for n in [1usize, 16, 33, 500] {
            let mut ways = random_ways(&mut rng, kind, n);
            // Long ways of every shape among them, the widest values a block holds.
            for shape in [Shape::SevenDecimals, Shape::NearZero, Shape::AnyBits, Shape::OneNodeOver] {
                let count = 2000 + (rng.next() % 3000) as usize;
                let at = (rng.next() % ways.len() as u64) as usize;
                ways[at] = (WayRow { node_count: count as u16, ..ways[at].0 }, random_nodes(&mut rng, count, shape));
            }
            let bytes = encode(kind, &ways);
            let slow = read_bit_by_bit(kind, &bytes.ways, &bytes.nodes);
            assert!(same(&slow, &ways), "{n} {kind:?}");
            assert!(same(&read_all(&files(kind, &bytes)), &slow), "{n} {kind:?}");
        }
    }
}

#[test]
fn way_files_read_as_the_shared_vectors_say_and_the_test_encoder_writes_them() {
    let rows = crate::exact_pip::tests::testdata_rows("way_files.tsv");
    let hex = |s: &str| (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap()).collect::<Vec<u8>>();
    let hex32 = |s: &str| u32::from_str_radix(s, 16).unwrap_or_else(|e| panic!("{s}: {e}"));
    let (mut streets, mut interps, mut hybrid, mut order) = (0, 0, 0, 0);
    for r in &rows {
        assert_eq!(r.len(), 5, "{r:?}");
        let kind = match r[1].as_str() {
            "street" => Kind::Streets,
            "interp" => Kind::Interps,
            other => panic!("a kind this test does not read: {other}"),
        };
        let ways: Ways = r[2]
            .split(';')
            .filter(|w| *w != "-")
            .map(|w| {
                let f: Vec<&str> = w.split(',').collect();
                assert_eq!(f.len(), 5, "{w}");
                let nodes: Vec<NodeCoord> = f[4]
                    .split('|')
                    .filter(|n| *n != "-")
                    .map(|n| {
                        let (lat, lng) = n.split_once(':').unwrap();
                        node(hex32(lat), hex32(lng))
                    })
                    .collect();
                let number = |k: usize| f[k].parse::<u32>().unwrap();
                let row =
                    WayRow { node_count: nodes.len() as u16, name: number(0), start_number: number(1), end_number: number(2), interpolation: number(3) as u8 };
                (row, nodes)
            })
            .collect();
        let (rows_file, nodes_file) = (hex(&r[3]), hex(&r[4]));
        let ours = encode(kind, &ways);
        assert_eq!((&ours.ways, &ours.nodes), (&rows_file, &nodes_file), "{}", r[0]);
        let f = open(kind, &rows_file, &nodes_file).unwrap_or_else(|e| panic!("{}: {e}", r[0]));
        assert_eq!(f.count() as usize, ways.len(), "{}", r[0]);
        assert!(same(&read_all(&f), &ways), "{}", r[0]);
        assert!(same(&read_bit_by_bit(kind, &rows_file, &nodes_file), &ways), "{}", r[0]);
        match kind {
            Kind::Streets => streets += 1,
            Kind::Interps => interps += 1,
        }
        for g in 0..ways.len().div_ceil(GROUP_WAYS) {
            let row_at = g * (12 + GROUP_WAYS * kind.row_way_bytes());
            for j in 0..GROUP_WAYS.min(ways.len() - g * GROUP_WAYS) {
                if u16_at(&rows_file, row_at + 12 + 4 * j) != 0 {
                    let is_hybrid = u16_at(&rows_file, row_at + 14 + 4 * j) >> 12 & 1 == 1;
                    hybrid += is_hybrid as usize;
                    order += !is_hybrid as usize;
                }
            }
        }
    }
    assert!(streets >= 10 && interps >= 6, "{streets} {interps}");
    assert!(hybrid >= 50 && order >= 20, "{hybrid} {order}");
}
