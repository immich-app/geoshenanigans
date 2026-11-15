use super::*;
use crate::coord_keys::{zigzag32, HYBRID_KEY_MAX, ONE_BITS};
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

/// The hybrid flag of a code.
const HYBRID: u16 = 1 << 12;

/// One group of streets written by hand: where its blocks start, its anchor, each way's node
/// count and code (its name is its place), and the nodes file's bytes before its footer.
fn hand_streets(off: u32, anchor: (u32, u32), ways: &[(u16, u16)], blocks: &[u8]) -> WayFileBytes {
    let mut rows = Vec::new();
    for field in [off, anchor.0, anchor.1] {
        rows.extend(field.to_le_bytes());
    }
    for &(cnt, code) in ways {
        rows.extend(cnt.to_le_bytes());
        rows.extend(code.to_le_bytes());
    }
    for name in 0..ways.len() as u32 {
        rows.extend(name.to_le_bytes());
    }
    rows.extend((ways.len() as u32).to_le_bytes());
    rows.extend(b"SWY1");
    let mut nodes = blocks.to_vec();
    nodes.extend(ways.iter().map(|way| u64::from(way.0)).sum::<u64>().to_le_bytes());
    nodes.extend((ways.len() as u32).to_le_bytes());
    nodes.extend(b"SWN1");
    WayFileBytes { ways: rows, nodes }
}

/// The streets of the nodes (as bits), each named by its place, through the test encoder: what
/// `hand_streets` writes by hand where the builder writes it.
fn encoded_streets(ways: &[&[(u32, u32)]]) -> WayFileBytes {
    let ways: Ways = ways
        .iter()
        .enumerate()
        .map(|(name, nodes)| {
            let row = WayRow { node_count: nodes.len() as u16, name: name as u32, start_number: 0, end_number: 0, interpolation: 0 };
            (row, nodes.iter().map(|&(lat, lng)| node(lat, lng)).collect())
        })
        .collect();
    encode(Kind::Streets, &ways)
}

fn same_bytes(a: &WayFileBytes, b: &WayFileBytes) -> bool {
    a.ways == b.ways && a.nodes == b.nodes
}

/// Way `id`'s nodes, as bits; None where they do not read, the buffer then left empty.
fn nodes_of(f: &WayFiles, id: u32) -> Option<Vec<(u32, u32)>> {
    // A buffer that holds another way's nodes.
    let mut buf = vec![node(1, 2); 3];
    if !f.way(id, &mut buf) {
        assert!(buf.is_empty());
        return None;
    }
    Some(bits(&buf))
}

/// Way `id`'s first node, as bits, read alone.
fn first_of(f: &WayFiles, id: u32) -> Option<(u32, u32)> {
    f.first_node(id).map(|c| (c.lat.to_bits(), c.lng.to_bits()))
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
        assert_eq!(block_bytes(65535, code), Some(2 * u64::from(code >> 13) + 65534 * 8));
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
    assert_eq!(said(Kind::Streets, &counting(w, u32::MAX - 1), &counting(n, u32::MAX - 1)), "street_ways.bin is not the rows of its 4294967294 ways");
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
    // two decimals. It has no nodes to give, and no first node.
    let between = 0.1f32.to_bits() + 1;
    assert!(between < ONE_BITS && hybrid_key(between).is_none());
    let f = with(&|rows, _| rows[4..8].copy_from_slice(&between.to_le_bytes()));
    assert!(!f.way(0, &mut buf) && buf.is_empty() && f.first_node(0).is_none());
    // The same of a way of that one node; under the order key it reads.
    let alone = |code: u16| files(Kind::Streets, &hand_streets(0, (between, 0), &[(1, code)], &[]));
    assert_eq!((nodes_of(&alone(HYBRID), 0), first_of(&alone(HYBRID), 0)), (None, None));
    assert_eq!((nodes_of(&alone(0), 0), first_of(&alone(0), 0)), (Some(vec![(between, 0)]), Some((between, 0))));
    assert!(same_bytes(&hand_streets(0, (between, 0), &[(1, 0)], &[]), &encoded_streets(&[&[(between, 0)]])));
}

#[test]
fn a_way_under_the_hybrid_key_reads_only_where_each_of_its_keys_is_a_coordinates() {
    // Two nodes from 0, 0, the second's lat value 32 bits of ones: a step to the key 0x80000000,
    // which no coordinate has.
    let f = files(Kind::Streets, &hand_streets(0, (0, 0), &[(2, 32 | HYBRID)], &[0xFF; 4]));
    assert_eq!(nodes_of(&f, 0), None);
    // On either axis the largest key either side of zero is a coordinate's, in the bytes the
    // encoder writes for it, and no key past it is one's.
    let most = HYBRID_KEY_MAX;
    let keys = [
        (most, Some(0x7FFF_FFFF)),
        (most + 1, None),
        (0x7FFF_FFFF, None),
        (most.wrapping_neg(), Some(u32::MAX)),
        ((most + 1).wrapping_neg(), None),
        (0x8000_0000, None),
    ];
    for (key, pattern) in keys {
        for lat in [true, false] {
            let code = if lat { 32 | HYBRID } else { 32 << 6 | HYBRID };
            let hand = hand_streets(0, (0, 0), &[(2, code)], &zigzag32(key).to_le_bytes());
            let nodes = pattern.map(|p| vec![(0, 0), if lat { (p, 0) } else { (0, p) }]);
            assert_eq!(nodes_of(&files(Kind::Streets, &hand), 0), nodes, "the key {key:#x}, lat {lat}");
            if let Some(nodes) = nodes {
                assert!(same_bytes(&hand, &encoded_streets(&[&nodes])), "the key {key:#x}, lat {lat}");
            }
        }
    }
}

#[test]
fn a_ways_first_node_reads_only_in_the_bytes_its_steps_from_the_anchor_need() {
    let anchor = (52.0f32.to_bits(), 4.0f32.to_bits());
    // The node `zl` and `zg` from the anchor: zigzags of steps in the floats' order.
    let from_anchor = |zl: u32, zg: u32| {
        let step = |bits: u32, z: u32| from_order_key(order_key(bits).wrapping_add(unzigzag32(z)));
        (step(anchor.0, zl), step(anchor.1, zg))
    };
    // Two streets of one node, the first at the anchor: the second's nodes, its first node
    // written in `wf` bytes an axis, and the files.
    let read = |wf: u16, zl: u32, zg: u32| {
        let block = [&zl.to_le_bytes()[..wf as usize], &zg.to_le_bytes()[..wf as usize]].concat();
        let hand = hand_streets(0, anchor, &[(1, HYBRID), (1, wf << 13 | HYBRID)], &block);
        let f = files(Kind::Streets, &hand);
        let nodes = nodes_of(&f, 1);
        assert_eq!(first_of(&f, 1), nodes.as_ref().map(|nodes| nodes[0]));
        (nodes, hand)
    };
    assert_eq!(read(0, 0, 0).0, Some(vec![anchor]));
    for wf in 1..=4u16 {
        // The least zigzag of `wf` bytes on either axis is the encoder's; the largest of a
        // byte fewer on both is none's.
        let least = 1u32 << (8 * (wf - 1));
        for (zl, zg) in [(least, 0), (0, least), (least, least)] {
            let (nodes, hand) = read(wf, zl, zg);
            assert_eq!(nodes, Some(vec![from_anchor(zl, zg)]), "{wf} bytes");
            assert!(same_bytes(&hand, &encoded_streets(&[&[anchor], &[from_anchor(zl, zg)]])), "{wf} bytes");
        }
        assert_eq!(read(wf, least - 1, least - 1).0, None, "{wf} bytes");
    }
}

#[test]
fn a_way_reads_only_where_its_groups_first_way_with_nodes_is_at_the_anchor() {
    let anchor = (52.0f32.to_bits(), 4.0f32.to_bits());
    // A node one float north of the anchor, in one byte an axis.
    let (beside, one_byte) = ((anchor.0 + 1, anchor.1), 1 << 13 | HYBRID);
    let read = |ways: &[(u16, u16)], blocks: &[u8], id: u32| {
        let f = files(Kind::Streets, &hand_streets(0, anchor, ways, blocks));
        let nodes = nodes_of(&f, id);
        assert_eq!(first_of(&f, id), nodes.as_ref().map(|nodes| nodes[0]));
        nodes
    };
    // After a way with nodes it reads; as the group's first way with nodes, alone or after
    // ways without nodes, it does not.
    assert_eq!(read(&[(1, HYBRID), (1, one_byte)], &[2, 0], 1), Some(vec![beside]));
    assert_eq!(read(&[(1, one_byte)], &[2, 0], 0), None);
    assert_eq!(read(&[(0, 0), (1, one_byte)], &[2, 0], 1), None);
    assert_eq!(read(&[(0, 0), (0, 0), (1, one_byte)], &[2, 0], 2), None);
    // Nor does a way after it, at the anchor or beside it: the anchor is no way's first node.
    assert_eq!(read(&[(1, one_byte), (1, HYBRID)], &[2, 0], 1), None);
    assert_eq!(read(&[(0, 0), (1, one_byte), (0, 0), (1, one_byte)], &[2, 0, 2, 0], 3), None);
    assert_eq!(read(&[(0, 0), (1, HYBRID), (0, 0), (1, one_byte)], &[2, 0], 3), Some(vec![beside]));
}

#[test]
fn a_way_reads_only_where_it_and_the_ways_before_it_in_its_group_have_codes_they_can_have() {
    let anchor = (52.0f32.to_bits(), 4.0f32.to_bits());
    let mut buf = vec![node(1, 2)];
    // A way without nodes has the code 0: with another it has no nodes to give, not even
    // none, and neither has the way after it. Its row reads.
    for code in [0u16, 1, 1 << 6, HYBRID, 1 << 13, u16::MAX] {
        let f = files(Kind::Streets, &hand_streets(0, anchor, &[(0, code), (1, HYBRID)], &[]));
        assert_eq!(f.way(0, &mut buf), code == 0, "{code:#x}");
        assert!(buf.is_empty() && f.first_node(0).is_none(), "{code:#x}");
        assert_eq!(f.row(0).map(|row| (row.node_count, row.name)), Some((0, 0)), "{code:#x}");
        assert_eq!((nodes_of(&f, 1), first_of(&f, 1)), ((code == 0).then(|| vec![anchor]), (code == 0).then_some(anchor)), "{code:#x}");
    }
    // A way with nodes has widths of up to 32 bits and a first node of up to 4 bytes an axis:
    // after a way with a width of 33 bits, or a first node of 5 bytes, a way is none.
    for (code, fits) in [(32, true), (33, false), (32 << 6, true), (33 << 6, false), (5 << 13, false)] {
        let f = files(Kind::Streets, &hand_streets(0, anchor, &[(1, code | HYBRID), (1, HYBRID)], &[]));
        assert_eq!((nodes_of(&f, 1), first_of(&f, 1)), (fits.then(|| vec![anchor]), fits.then_some(anchor)), "{code:#x}");
        assert_eq!(nodes_of(&f, 0), fits.then(|| vec![anchor]), "{code:#x}");
        assert_eq!(f.row(1).map(|row| (row.node_count, row.name)), Some((1, 1)), "{code:#x}");
    }
}

#[test]
fn a_way_with_bits_set_past_its_blocks_last_value_reads_as_none() {
    let read = |cnt: u16, code: u16, block: &[u8]| {
        let f = files(Kind::Streets, &hand_streets(0, (0, 0), &[(cnt, code)], block));
        (nodes_of(&f, 0), first_of(&f, 0))
    };
    // Two nodes from 0, 0 under the hybrid key, the second a step of 1e-7 south: one value of
    // one bit, and seven bits to the byte's end.
    let south = e7(-1);
    assert_eq!(read(2, 1 | HYBRID, &[1]), (Some(vec![(0, 0), (south, 0)]), Some((0, 0))));
    assert!(same_bytes(&hand_streets(0, (0, 0), &[(2, 1 | HYBRID)], &[1]), &encoded_streets(&[&[(0, 0), (south, 0)]])));
    for byte in [0b11, 0b101, 0x81] {
        assert_eq!(read(2, 1 | HYBRID, &[byte]), (None, None), "{byte:#x}");
    }
    // A lat and a lng value of one bit each, and six bits to the byte's end.
    assert_eq!(read(2, 1 | 1 << 6 | HYBRID, &[0b11]).0, Some(vec![(0, 0), (south, south)]));
    for byte in [0b111, 0x83] {
        assert_eq!(read(2, 1 | 1 << 6 | HYBRID, &[byte]), (None, None), "{byte:#x}");
    }
    // Eight values of one bit fill their byte: each step a unit further south than the last.
    let steps: Vec<(u32, u32)> = [0, 1, 3, 6, 10, 15, 21, 28, 36].iter().map(|&n| (e7(-n), 0)).collect();
    assert_eq!(read(9, 1 | HYBRID, &[0xFF]).0, Some(steps));
}

#[test]
fn the_first_groups_blocks_start_at_the_nodes_files_first_byte() {
    // A street of two nodes whose block is one byte.
    let south = e7(-1);
    let f = files(Kind::Streets, &hand_streets(0, (0, 0), &[(2, 1 | HYBRID)], &[1]));
    assert_eq!(nodes_of(&f, 0), Some(vec![(0, 0), (south, 0)]));
    // A byte before it, and the group's off counting from it.
    let f = files(Kind::Streets, &hand_streets(1, (0, 0), &[(2, 1 | HYBRID)], &[0xAA, 1]));
    assert_eq!((nodes_of(&f, 0), first_of(&f, 0)), (None, None));
}

#[test]
fn footers_that_count_what_no_ways_hold_are_refused_at_open() {
    let said = |ways: &[u8], nodes: &[u8]| open(Kind::Streets, ways, nodes).err().unwrap_or_default();
    let streets = random_ways(&mut Rng::new(7), Kind::Streets, 40);
    let bytes = encode(Kind::Streets, &streets);
    let (w, n) = (&bytes.ways, &bytes.nodes);
    // The nodes file with a footer that counts `nodes` nodes.
    let counting = |nodes: u64| {
        let mut b = n.clone();
        let at = b.len() - 16;
        b[at..at + 8].copy_from_slice(&nodes.to_le_bytes());
        b
    };
    assert_eq!(counting(streets.iter().map(|way| way.1.len() as u64).sum()), *n);
    // As many nodes as its 40 ways can have, 65,535 each, and one more.
    let more = "street_nodes.bin counts more nodes than its ways can have";
    assert_eq!(said(w, &counting(40 * 65_535)), "");
    assert_eq!(said(w, &counting(40 * 65_535 + 1)), more);
    assert_eq!(said(w, &counting(u64::MAX)), more);
    // Blocks of 8 bytes a node at the most: the fewest nodes that fill them, and one fewer.
    let blocks = (n.len() - 16) as u64;
    assert!(blocks > 8);
    let longer = "street_nodes.bin is longer than its nodes can fill";
    assert_eq!(said(w, &counting(blocks.div_ceil(8))), "");
    assert_eq!(said(w, &counting(blocks.div_ceil(8) - 1)), longer);
    assert_eq!(said(w, &counting(0)), longer);
    // 2^32 - 1 ways, whatever the files' sizes.
    let ways_counting = |bytes: &[u8], count: u32| {
        let mut b = bytes.to_vec();
        let at = b.len() - 8;
        b[at..at + 4].copy_from_slice(&count.to_le_bytes());
        b
    };
    assert_eq!(
        said(&ways_counting(w, u32::MAX), &ways_counting(n, u32::MAX)),
        "street_ways.bin holds 2^32 - 1 ways or more: NO_DATA is no way's id"
    );
}

#[cfg(all(unix, target_pointer_width = "64"))]
#[test]
fn a_nodes_file_of_4_gib_or_more_is_refused_at_open() {
    use std::os::unix::fs::FileExt;
    // The fewest ways whose nodes fill 4 GiB, 8 bytes a node: rows of zeros, and a footer that
    // counts 2^29 nodes.
    let (ways, nodes) = (8193u32, 1u64 << 29);
    assert!(u64::from(ways - 1) * 65_535 < nodes && nodes <= u64::from(ways) * 65_535);
    let mut rows = vec![0u8; (ways.div_ceil(16) * 12 + ways * 8) as usize];
    rows.extend(ways.to_le_bytes());
    rows.extend(b"SWY1");
    let footer = [&nodes.to_le_bytes()[..], &ways.to_le_bytes()[..], &b"SWN1"[..]].concat();
    // A nodes file of `size` bytes, zeros but for its footer: a sparse file, mapped.
    let dir = crate::scratch_dir::ScratchDir::new();
    let said = |size: u64| {
        let path = dir.path().join(format!("{size}.bin"));
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(size).unwrap();
        file.write_all_at(&footer, size - 16).unwrap();
        let nodes = crate::mmap_file(path.to_str().unwrap()).unwrap();
        WayFiles::open(Kind::Streets, FileBytes::Owned(rows.clone()), nodes).err().unwrap_or_default()
    };
    assert_eq!(said(u32::MAX as u64), "");
    assert_eq!(said(1 << 32), "street_nodes.bin is 4 GiB or more");
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
