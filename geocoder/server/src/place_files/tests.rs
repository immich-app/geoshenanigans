use super::*;
use crate::test_rng::Rng;
use crate::{cell_id_at_level, DEFAULT_ADMIN_CELL_LEVEL};

fn files(f: &PlaceFileBytes) -> PlaceFiles {
    PlaceFiles::open(FileBytes::Owned(f.cells.clone()), FileBytes::Owned(f.nodes.clone()), FileBytes::Owned(f.hidden.clone())).unwrap()
}

fn node(lat: f32, lng: f32, name_id: u32, place_type: u8) -> PlaceNode {
    PlaceNode { lat, lng, name_id, place_type, _pad1: 0, _pad2: 0, parent_poly_id: NO_DATA }
}

fn same(a: &PlaceNode, b: &PlaceNode) -> bool {
    a.lat.to_bits() == b.lat.to_bits() && a.lng.to_bits() == b.lng.to_bits() && a.name_id == b.name_id
        && a.place_type == b.place_type && a._pad1 == b._pad1 && a._pad2 == b._pad2 && a.parent_poly_id == b.parent_poly_id
}

/// Nodes around a few towns (many to a cell, more than a block to some), a few far away, labels
/// and other hidden nodes among them, names up to 2^27 and both zeros.
fn random_nodes(rng: &mut Rng, n: usize) -> Vec<PlaceNode> {
    let towns = [(52.37f64, 4.89f64), (-33.87, 151.21), (0.0, 0.0), (64.1, -21.9), (-0.0, -179.99)];
    (0..n)
        .map(|i| {
            let (lat, lng) = towns[(rng.next() % towns.len() as u64) as usize];
            let spread = if rng.next() % 8 == 0 { 20.0 } else { 0.3 };
            let mut pn = node(
                (lat + rng.range(-spread, spread)).clamp(-89.9, 89.9) as f32,
                (lng + rng.range(-spread, spread)).clamp(-179.99, 179.99) as f32,
                (rng.next() % (1 << 27)) as u32 >> (rng.next() % 27),
                (rng.next() % 7) as u8,
            );
            if i % 11 == 3 {
                pn.lat = if i % 2 == 0 { 0.0 } else { -0.0 };
            }
            match rng.next() % 10 {
                0 => {
                    pn._pad1 = PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF;
                    pn.parent_poly_id = (rng.next() % 5000) as u32;
                }
                1 => pn._pad1 = PLACE_FLAG_HIDDEN,
                _ => {}
            }
            pn
        })
        .collect()
}

#[test]
fn every_node_reads_back_at_its_id_and_every_visible_one_in_its_cell() {
    let mut rng = Rng::new(20261113);
    for n in [0usize, 1, 63, 64, 65, 200, 3000] {
        let nodes = random_nodes(&mut rng, n);
        let (bytes, ids) = encode(&nodes, DEFAULT_ADMIN_CELL_LEVEL);
        let f = files(&bytes);
        assert_eq!(f.count() as usize, n);
        let visible = nodes.iter().filter(|pn| pn._pad1 == 0).count();
        assert_eq!(f.visible() as usize, visible);
        for (i, pn) in nodes.iter().enumerate() {
            assert_eq!((ids[i] < f.visible()), pn._pad1 == 0, "node {i}");
            assert!(same(&f.node(ids[i]).unwrap(), pn), "node {i} of {n}");
            if pn._pad1 == 0 {
                let (first, end) = f.cell_rows(cell_id_at_level(pn.lat as f64, pn.lng as f64, DEFAULT_ADMIN_CELL_LEVEL));
                assert!(first <= ids[i] && ids[i] < end, "node {i}'s cell holds it");
            }
        }
        assert!(f.node(f.count()).is_none());
        // A cell's rows, all of them in it, read in one pass.
        let mut seen = 0;
        for (i, pn) in nodes.iter().enumerate().filter(|(_, pn)| pn._pad1 == 0) {
            let cell = cell_id_at_level(pn.lat as f64, pn.lng as f64, DEFAULT_ADMIN_CELL_LEVEL);
            let (first, end) = f.cell_rows(cell);
            let mut rows = Vec::new();
            f.for_rows(first, end, |row, got| {
                assert_eq!(cell_id_at_level(got.lat as f64, got.lng as f64, DEFAULT_ADMIN_CELL_LEVEL), cell);
                rows.push(row);
            });
            assert_eq!(rows, (first..end).collect::<Vec<_>>());
            assert!(rows.contains(&ids[i]));
            seen += 1;
        }
        assert_eq!(seen, visible);
    }
}

#[test]
fn a_cell_without_nodes_has_no_rows() {
    let nodes = [node(52.37, 4.89, 1, 0), node(52.38, 4.90, 2, 1)];
    let (bytes, _) = encode(&nodes, DEFAULT_ADMIN_CELL_LEVEL);
    let f = files(&bytes);
    assert_eq!(f.cell_rows(cell_id_at_level(-33.87, 151.21, DEFAULT_ADMIN_CELL_LEVEL)), (0, 0));
    // An id that is no level-10 cell's.
    assert_eq!(f.cell_rows(cell_id_at_level(52.37, 4.89, 20)), (0, 0));
}

#[test]
fn files_no_builder_writes_are_refused_at_open() {
    let (bytes, _) = encode(&random_nodes(&mut Rng::new(7), 100), DEFAULT_ADMIN_CELL_LEVEL);
    let open = |cells: &[u8], nodes: &[u8], hidden: &[u8]| {
        PlaceFiles::open(FileBytes::Owned(cells.to_vec()), FileBytes::Owned(nodes.to_vec()), FileBytes::Owned(hidden.to_vec()))
    };
    assert!(open(&bytes.cells, &bytes.nodes, &bytes.hidden).is_ok());
    assert!(open(&bytes.cells, &bytes.nodes[..bytes.nodes.len() - 1], &bytes.hidden).is_err());
    assert!(open(&bytes.cells, &bytes.nodes[..4], &bytes.hidden).is_err());
    assert!(open(&bytes.cells[..bytes.cells.len() - 3], &bytes.nodes, &bytes.hidden).is_err());
    assert!(open(&bytes.cells, &bytes.nodes, &bytes.hidden[..bytes.hidden.len() - 1]).is_err());
    // A footer counting more rows than the table holds entries for.
    let mut more = bytes.nodes.clone();
    let at = more.len() - 8;
    more[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(open(&bytes.cells, &more, &bytes.hidden).is_err());
}

#[test]
fn damaged_blocks_and_cells_read_without_a_panic() {
    let mut rng = Rng::new(99);
    let (bytes, _) = encode(&random_nodes(&mut rng, 500), DEFAULT_ADMIN_CELL_LEVEL);
    for _ in 0..2000 {
        let (mut cells, mut nodes) = (bytes.cells.clone(), bytes.nodes.clone());
        for _ in 0..1 + rng.next() % 4 {
            // Keep the footer, so the files open.
            let at = (rng.next() % (nodes.len() as u64 - 8)) as usize;
            nodes[at] = rng.next() as u8;
            if !cells.is_empty() {
                let c = (rng.next() % cells.len() as u64) as usize;
                cells[c] = rng.next() as u8;
            }
        }
        let f = PlaceFiles::open(FileBytes::Owned(cells), FileBytes::Owned(nodes), FileBytes::Owned(bytes.hidden.clone())).unwrap();
        f.for_rows(0, f.visible(), |_, _| {});
        for id in 0..f.count() + 2 {
            let _ = f.node(id);
        }
        for pn in [node(52.37, 4.89, 0, 0), node(0.0, 0.0, 0, 0)] {
            let (first, end) = f.cell_rows(cell_id_at_level(pn.lat as f64, pn.lng as f64, DEFAULT_ADMIN_CELL_LEVEL));
            assert!(first <= end && end <= f.visible());
            f.for_rows(first, end, |_, _| {});
        }
    }
}

#[test]
fn order_keys_order_the_floats_and_come_back() {
    let floats = [f32::NEG_INFINITY, -180.0, -1.5, -f32::MIN_POSITIVE, -0.0, 0.0, f32::MIN_POSITIVE, 1.5, 180.0, f32::INFINITY];
    for w in floats.windows(2) {
        assert!(order_key(w[0].to_bits()) < order_key(w[1].to_bits()), "{} < {}", w[0], w[1]);
    }
    for f in floats {
        assert_eq!(from_order_key(order_key(f.to_bits())), f.to_bits());
    }
}

#[test]
fn place_files_read_as_the_shared_vectors_say_and_the_test_encoder_writes_them() {
    let rows = crate::exact_pip::tests::testdata_rows("place_files.tsv");
    let hex = |s: &str| (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap()).collect::<Vec<u8>>();
    for r in &rows {
        assert_eq!(r.len(), 5, "{r:?}");
        let logical: Vec<(PlaceNode, u32)> = r[1]
            .split(';')
            .filter(|s| !s.is_empty())
            .map(|s| {
                let f: Vec<&str> = s.split(',').collect();
                let n = |k: usize| f[k].parse::<u32>().unwrap();
                let pn = PlaceNode {
                    lat: f32::from_bits(u32::from_str_radix(f[0], 16).unwrap()),
                    lng: f32::from_bits(u32::from_str_radix(f[1], 16).unwrap()),
                    name_id: n(2),
                    place_type: n(3) as u8,
                    _pad1: n(4) as u8,
                    _pad2: 0,
                    parent_poly_id: n(5),
                };
                (pn, n(6))
            })
            .collect();
        let (cells, nodes, hidden) = (hex(&r[2]), hex(&r[3]), hex(&r[4]));
        let ours = encode_logical(&logical);
        assert_eq!((&ours.cells, &ours.nodes, &ours.hidden), (&cells, &nodes, &hidden), "{}", r[0]);
        let f = PlaceFiles::open(FileBytes::Owned(cells), FileBytes::Owned(nodes), FileBytes::Owned(hidden)).unwrap();
        assert_eq!(f.count() as usize, logical.len(), "{}", r[0]);
        for (id, (pn, key)) in logical.iter().enumerate() {
            assert!(same(&f.node(id as u32).unwrap(), pn), "{} node {id}", r[0]);
            if *key != NO_DATA {
                let (first, end) = f.cell_rows((*key as u64) << 32);
                assert!(first <= id as u32 && (id as u32) < end, "{} node {id}'s cell", r[0]);
            }
        }
    }
    assert!(rows.len() >= 9);
}

#[test]
fn the_last_block_of_the_most_rows_ends_at_the_read_end() {
    // 2^32 - 1 rows: the last block, 2^26 - 1, would end at 2^32, past u32.
    let last = (u32::MAX as usize) / BLOCK_ROWS;
    assert_eq!(block_end(last, u32::MAX), u32::MAX);
    assert_eq!(block_end(last - 1, u32::MAX), (last * BLOCK_ROWS) as u32);
    assert_eq!(block_end(0, 10), 10);
    assert_eq!(block_end(0, 64), 64);
    assert_eq!(block_end(1, 200), 128);
}
