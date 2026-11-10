use super::*;
use crate::test_rng::Rng;
use crate::DEFAULT_STREET_CELL_LEVEL;

type Rows = Vec<(u64, [Vec<u32>; TABLES])>;

/// The level-L cell of number n (face * 4^L + its position).
fn cell_at(n: u64, level: u64) -> u64 {
    n << (61 - 2 * level) | 1 << (60 - 2 * level)
}
fn cell(n: u64) -> u64 {
    cell_at(n, DEFAULT_STREET_CELL_LEVEL)
}

fn open(files: (Vec<u8>, Vec<u8>), level: u64) -> Result<GeoIndex, String> {
    GeoIndex::open(FileBytes::Owned(files.0), FileBytes::Owned(files.1), level)
}

/// A list of one of many shapes: one id, runs, sparse ids with gaps of 1 to 5 bytes, repeated
/// ids, ids at 0 and the top of the u32 range, the table's list of the row before (SAME) and
/// ones it has but for ids dropped or added (EDIT), now and then 0xFFFF ids.
fn shape(rng: &mut Rng, prev: &[u32]) -> Vec<u32> {
    let mut l: Vec<u32> = match rng.next() % 13 {
        0 => vec![(rng.next() % 1000) as u32],
        1 => vec![0],
        2 => vec![u32::MAX],
        3 => (100..140).collect(),
        4 => (0..30).map(|_| (rng.next() >> (7 * (rng.next() % 5) + 32)) as u32).collect(),
        5 => (0..12).map(|_| (rng.next() % 50) as u32).collect(), // repeats
        6 | 7 => prev.to_vec(),
        8 => {
            let mut l = prev.to_vec();
            if l.len() > 1 {
                l.remove((rng.next() % l.len() as u64) as usize);
            }
            l
        }
        9 => prev.iter().copied().chain([(rng.next() % 6000) as u32]).collect(),
        10 => prev.iter().copied().chain((0..3).map(|_| rng.next() as u32)).collect(),
        11 if rng.next() % 40 == 0 => (0..LIST_MAX as u32).map(|k| k * 3).collect(),
        11 => vec![(rng.next() % 7) as u32, 0xFFFF_FFF0],
        _ => (0..4).map(|k| [0, 127, 128, 16383, 16384, 2097151, 2097152, 268435455, 268435456][(rng.next() % 9) as usize] + k).collect(),
    };
    if l.is_empty() {
        l.push(1);
    }
    l.sort_unstable();
    l.truncate(LIST_MAX as usize);
    l
}

/// `n` rows at `level`: steps of 1 to 3 and escaped ones, from at or near the level's first cell,
/// each with one to three tables' lists of every shape, addresses most often fresh ranges as the
/// builder numbers them.
fn random_rows(rng: &mut Rng, n: usize, level: u64) -> Rows {
    let cells = 6u64 << (2 * level);
    let mut number = if rng.next() % 2 == 0 { 0 } else { rng.next() % 1000 };
    let mut prev: [Vec<u32>; TABLES] = [vec![1], vec![1], vec![1]];
    let mut next_addr = 0u32;
    let mut out = Rows::new();
    for _ in 0..n {
        let mut lists: [Vec<u32>; TABLES] = Default::default();
        let mask = 1 + rng.next() % 7;
        for t in 0..TABLES {
            if mask >> t & 1 == 0 {
                continue;
            }
            lists[t] = if t == ADDRESSES && rng.next() % 3 != 0 {
                let k = 1 + (rng.next() % 20) as u32;
                next_addr += k;
                (next_addr - k..next_addr).collect()
            } else {
                shape(rng, &prev[t])
            };
            prev[t] = lists[t].clone();
        }
        out.push((cell_at(number, level), lists));
        let kind = rng.next() % 10;
        let step = if kind < 4 { 1 + kind % 3 } else if kind < 8 { 4 + rng.next() % 200 } else if kind < 9 { 1 + (rng.next() >> (rng.next() % 40 + 24)) } else { 3 };
        if step >= cells - number {
            break;
        }
        number += step;
    }
    out
}

/// Cells around each row: it, its neighbours by number, ones far off and of other levels.
fn probes(rng: &mut Rng, rows: &Rows, level: u64) -> Vec<u64> {
    let mut out = Vec::new();
    for (c, _) in rows {
        let n = c >> (61 - 2 * level);
        out.extend([*c, cell_at(n + 1, level)]);
        if n > 0 {
            out.push(cell_at(n - 1, level));
        }
        if rng.next() % 5 == 0 {
            out.push(cell_at(rng.next() % (6u64 << (2 * level)), level));
            out.push(*c | 1); // another level
        }
    }
    out.push(cell_at(0, level));
    out.push(cell_at((6u64 << (2 * level)) - 1, level));
    out
}

/// The lists of a cell of `rows` (empty past them), restricted to `tables`.
fn want(rows: &Rows, c: u64, tables: u8) -> [Vec<u32>; TABLES] {
    let mut out: [Vec<u32>; TABLES] = Default::default();
    if let Ok(i) = rows.binary_search_by_key(&c, |r| r.0) {
        for t in 0..TABLES {
            if tables >> t & 1 != 0 {
                out[t] = rows[i].1[t].clone();
            }
        }
    }
    out
}

fn got(lists: &GeoLists, i: usize) -> [Vec<u32>; TABLES] {
    std::array::from_fn(|t| lists.list(i, t).to_vec())
}

#[test]
fn every_row_reads_back_one_cell_at_a_time() {
    let mut rng = Rng::new(20261109);
    let mut lists = GeoLists::default();
    for (n, level) in [(1, 17), (63, 17), (64, 17), (65, 17), (4095, 17), (4096, 17), (4097, 17), (9000, 17), (3000, 1), (3000, 30), (3000, 10)] {
        let rows = random_rows(&mut rng, n, level);
        let index = open(write(&rows), level).unwrap();
        for c in probes(&mut rng, &rows, level) {
            let tables = [ALL_TABLES, 1 << STREETS, 1 << ADDRESSES, 1 << INTERPOLATIONS, 0b101][(rng.next() % 5) as usize];
            index.lookup(c, tables, &mut lists);
            assert_eq!(got(&lists, 0), want(&rows, c, tables), "cell {c:#x} tables {tables:#b} of {n} rows at level {level}");
        }
    }
}

/// Nine cells at a time, in any order, as a query asks for its cell and its neighbours: some in
/// one block, some in blocks apart, some twice, some without rows. Each gets what a lookup of it
/// alone gets.
#[test]
fn the_batched_lookup_gets_what_one_cell_at_a_time_gets() {
    let mut rng = Rng::new(7);
    let (mut many, mut one) = (GeoLists::default(), GeoLists::default());
    for (n, level) in [(65, 17), (4097, 17), (20000, 17), (3000, 10)] {
        let rows = random_rows(&mut rng, n, level);
        let index = open(write(&rows), level).unwrap();
        let all = probes(&mut rng, &rows, level);
        for _ in 0..3000 {
            let start = (rng.next() % all.len() as u64) as usize;
            let mut cells: Vec<u64> = (0..9).map(|k| all[(start + k * (1 + (rng.next() % 3) as usize)) % all.len()]).collect();
            if rng.next() % 4 == 0 {
                cells[3] = cells[0];
            }
            if rng.next() % 4 == 0 {
                cells.reverse();
            }
            let tables = [ALL_TABLES, 1 << ADDRESSES, 1 << INTERPOLATIONS][(rng.next() % 3) as usize];
            index.lookup_many(&cells, tables, &mut many);
            for (i, &c) in cells.iter().enumerate() {
                index.lookup(c, tables, &mut one);
                assert_eq!(got(&many, i), got(&one, 0), "cell {c:#x}, {i} of {cells:x?}");
                assert_eq!(got(&many, i), want(&rows, c, tables));
            }
        }
    }
}

/// The builder's geo_index::Writer bytes (builder/tests/test_geo_index.cpp's golden indexes):
/// this writer gives them and the reader reads them.
#[test]
fn the_golden_indexes_read_back_and_this_writer_writes_them() {
    let golden: Rows = vec![
        (cell(5), [vec![3, 9], vec![100], vec![]]),
        (cell(6), [vec![3, 9], vec![101, 101], vec![7]]),
        (cell(9), [vec![3, 9, 12], vec![], vec![7]]),
        (cell(100), [vec![200], vec![], vec![]]),
    ];
    let top = vec![b'G', b'X', b'T', b'2', 17, 64, 64, 0, 0, 0, 0, 0x2C, 0, 0, 0, 0, 22, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0];
    let blocks = vec![5, 4, 6, 5, 2, 0xC8, 0x01, 22, 5, 2, 0, 2, 14, 163, 0, 1, 18, 193, 87, 2, 0x8A, 0x03,
                      0, 0, 0, 0x2C, 0, 0, 0, 0, 0, 0, 0, 0];
    assert_eq!(write(&golden), (top.clone(), blocks.clone()));
    let index = open((top, blocks), DEFAULT_STREET_CELL_LEVEL).unwrap();
    let mut lists = GeoLists::default();
    for (c, l) in &golden {
        index.lookup(*c, ALL_TABLES, &mut lists);
        assert_eq!(&got(&lists, 0), l);
    }
    // The long one: 4,100 rows in 65 blocks of 2 groups, by length and FNV-1a 64.
    fn fnv1a(b: &[u8]) -> u64 {
        b.iter().fold(0xcbf29ce484222325, |h, &x| (h ^ x as u64).wrapping_mul(0x100000001b3))
    }
    let mut number = 1;
    let long: Rows = (0..4100u32)
        .map(|k| {
            let r = (cell(number), [vec![k / 3, k / 3 + 2], if k % 2 == 0 { vec![k] } else { vec![] }, if k % 7 == 0 { vec![k % 7, 9] } else { vec![] }]);
            number += k as u64 % 5 + 1;
            r
        })
        .collect();
    let (t, b) = write(&long);
    assert_eq!((t.len(), b.len(), fnv1a(&t), fnv1a(&b)), (40, 15142, 0x825ad00c323593c7, 0xeccb53b69187e3e3));
    let index = open((t, b), DEFAULT_STREET_CELL_LEVEL).unwrap();
    for (c, l) in &long {
        index.lookup(*c, ALL_TABLES, &mut lists);
        assert_eq!(&got(&lists, 0), l);
    }
}

#[test]
fn an_index_of_no_rows_is_two_empty_files() {
    assert_eq!(write(&[]), (vec![], vec![]));
    let index = open((vec![], vec![]), DEFAULT_STREET_CELL_LEVEL).unwrap();
    assert!(index.is_empty());
    let mut lists = GeoLists::default();
    index.lookup_many(&[cell(1), cell(2)], ALL_TABLES, &mut lists);
    assert!(got(&lists, 0).iter().chain(got(&lists, 1).iter()).all(Vec::is_empty));
    assert!(GeoIndex::none().is_empty());
}

#[test]
fn open_refuses_files_that_arent_an_index_at_its_level() {
    let one: Rows = vec![(cell(5), [vec![3, 9], vec![], vec![]]), (cell(6), [vec![3, 9], vec![], vec![]])];
    let (top, blocks) = write(&one);
    assert!(open((top.clone(), blocks.clone()), DEFAULT_STREET_CELL_LEVEL).is_ok());
    let with = |at: usize, v: u8| {
        let mut t = top.clone();
        t[at] = v;
        t
    };
    let cases: Vec<(Vec<u8>, Vec<u8>, &str)> = vec![
        (vec![], blocks.clone(), "blocks without a top file"),
        (top[..20].to_vec(), blocks.clone(), "not a geo index (no GXT2 header)"),
        (with(3, b'1'), blocks.clone(), "not a geo index (no GXT2 header)"),
        (with(5, 32), blocks.clone(), "not a geo index (another layout of blocks)"),
        (with(6, 128), blocks.clone(), "not a geo index (another layout of blocks)"),
        (with(7, 1), blocks.clone(), "not a geo index (another layout of blocks)"),
        (with(4, 0), blocks.clone(), "not a geo index (a level past the levels it takes)"),
        (with(4, 31), blocks.clone(), "not a geo index (a level past the levels it takes)"),
        (with(4, 16), blocks.clone(), "an index of level 16 cells, read at level 17"),
        (with(20, 0), blocks.clone(), "not a geo index (a footer that isn't its groups')"),
        (with(27, 1), blocks.clone(), "not a geo index (a footer that isn't its groups')"),
        ([&top[..], &[0; 12]].concat(), blocks.clone(), "not a geo index (a footer that isn't its groups')"),
        (with(16, 4), blocks.clone(), "not a geo index (a last footer that doesn't end the blocks file)"),
        (top.clone(), [&blocks[..], &[0]].concat(), "not a geo index (a last footer that doesn't end the blocks file)"),
    ];
    for (t, b, why) in cases {
        assert_eq!(open((t, b), DEFAULT_STREET_CELL_LEVEL).err().as_deref(), Some(why));
    }
}

/// Blocks malformed past what open checks: lookups give what they can and never panic, nor read
/// past the files.
#[test]
fn lookups_in_malformed_blocks_stop_without_panicking() {
    let mut rng = Rng::new(3);
    let rows = random_rows(&mut rng, 5000, 17);
    let (top, blocks) = write(&rows);
    let mut lists = GeoLists::default();
    let all = probes(&mut rng, &rows, 17);
    for round in 0..300 {
        let (mut t, mut b) = (top.clone(), blocks.clone());
        // Bytes changed in the blocks and their footers, or in the groups' entries.
        for _ in 0..1 + round % 5 {
            if rng.next() % 4 == 0 {
                let at = HEADER_BYTES + (rng.next() as usize % (t.len() - HEADER_BYTES - FOOTER_BYTES));
                t[at] = rng.next() as u8;
            } else {
                let at = rng.next() as usize % b.len();
                b[at] = rng.next() as u8;
            }
        }
        let Ok(index) = open((t, b), 17) else { continue };
        for _ in 0..50 {
            let start = (rng.next() % all.len() as u64) as usize;
            let cells: Vec<u64> = (0..9).map(|k| all[(start + k) % all.len()]).collect();
            index.lookup_many(&cells, ALL_TABLES, &mut lists);
        }
    }
}

/// A row whose edit's ops run past its block's end (the block cut short after the first of its two
/// ops bytes): a lookup of a cell past it stops at the block's end, and one before it still reads.
#[test]
fn a_lookup_stepping_over_an_edit_cut_short_stops_at_the_blocks_end() {
    // Row 0: streets {0..15} PLAIN (16 << 1, zigzag 0, 15 gaps of 0); row 1: an EDIT, one ops byte.
    let mut blocks = vec![0x01, 0x20, 0x00];
    blocks.extend([0u8; 15]);
    blocks.extend([0x03, 0x00]);
    let footer = blocks.len() as u32;
    blocks.extend(cell(5).to_le_bytes());
    blocks.extend(0u32.to_le_bytes());
    let mut top = vec![b'G', b'X', b'T', b'2', 17, 64, 64, 0];
    top.extend(cell(5).to_le_bytes());
    top.extend(footer.to_le_bytes());
    top.extend(2u64.to_le_bytes());
    let index = open((top, blocks), DEFAULT_STREET_CELL_LEVEL).unwrap();
    let mut lists = GeoLists::default();
    index.lookup_many(&[cell(5), cell(7)], ALL_TABLES, &mut lists);
    assert_eq!(got(&lists, 0), [(0..16).collect::<Vec<u32>>(), vec![], vec![]]);
    assert!(got(&lists, 1).iter().all(Vec::is_empty));
}

/// A lookup of the cells the last one asked for takes their lists as they are when it decoded the
/// tables asked for, in the same index: a subset of them, then all of them again, then the same
/// cells in another index (another day's rows), which has lists of its own.
#[test]
fn a_lookup_of_the_cells_just_looked_up_reads_the_same_lists() {
    let mut rng = Rng::new(11);
    let rows = random_rows(&mut rng, 5000, 17);
    let mut other = rows.clone();
    for r in other.iter_mut() {
        r.1[STREETS] = vec![7];
    }
    let (index, other_index) = (open(write(&rows), 17).unwrap(), open(write(&other), 17).unwrap());
    let all = probes(&mut rng, &rows, 17);
    let mut lists = GeoLists::default();
    for _ in 0..500 {
        let start = (rng.next() % all.len() as u64) as usize;
        let cells: Vec<u64> = (0..9).map(|k| all[(start + k) % all.len()]).collect();
        for (ix, rs, tables) in [(&index, &rows, ALL_TABLES), (&index, &rows, 1 << ADDRESSES), (&index, &rows, 1 << STREETS),
                                 (&index, &rows, ALL_TABLES), (&other_index, &other, 1 << STREETS), (&index, &rows, 1 << STREETS),
                                 (&index, &rows, 1 << INTERPOLATIONS), (&index, &rows, 0b011)] {
            ix.lookup_many(&cells, tables, &mut lists);
            for (i, &c) in cells.iter().enumerate() {
                assert_eq!(got(&lists, i), want(rs, c, tables), "cell {c:#x} tables {tables:#b}");
            }
        }
    }
}
