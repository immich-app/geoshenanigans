use super::*;
use crate::test_rng::Rng;
use crate::DEFAULT_ADMIN_CELL_LEVEL;

const FORMATS: [Format; 2] = [ADMIN, POI];

/// The level-10 cell of number n (face * 4^10 + its position).
fn cell(n: u64) -> u64 {
    n << 41 | 1 << 40
}

fn open(files: (Vec<u8>, Vec<u8>), format: Format) -> Result<CellPairs, String> {
    CellPairs::open(FileBytes::Owned(files.0), FileBytes::Owned(files.1), format, DEFAULT_ADMIN_CELL_LEVEL)
}

/// Lists of every shape, over parents at the bitmap's word ends and the last face's last parent,
/// each holding its first and last children, runs of neighbours and lone cells: empty lists, one
/// id, runs of ids, sparse ids with gaps of 1 to 4 bytes, repeated ids, interior flags, ids at
/// the top of the bare and the u32 range, a list the row before has (SAME, a shared offset) and
/// ones it has but for an id dropped, added or its flag toggled (EDIT), and one of 0xFFFF ids.
fn shapes(rng: &mut Rng) -> Vec<(u64, Vec<u32>)> {
    let mut out: Vec<(u64, Vec<u32>)> = Vec::new();
    let mut prev: Vec<u32> = vec![3, 9];
    for parent in [0u64, 1, 63, 64, 65, 1000, 4095, 6143] {
        let mut children: Vec<u64> = vec![0, 1, 2, 63, 64, 511, 1022, 1023];
        children.extend((0..40).map(|_| rng.next() % 1024));
        children.sort_unstable();
        children.dedup();
        for child in children {
            let shape = rng.next() % 14;
            let mut l: Vec<u32> = match shape {
                0 => vec![],
                1 => vec![(rng.next() % 1000) as u32],
                2 => vec![BARE],
                3 => vec![u32::MAX],
                4 => (100..140).collect(),
                5 => (0..30).map(|_| (rng.next() >> (8 * (rng.next() % 4) + 33)) as u32).collect(),
                6 => (0..12).map(|_| (rng.next() % 50) as u32).collect(), // repeats
                7 => (0..10).map(|_| (rng.next() % 5000) as u32 | if rng.next() % 3 == 0 { FLAG } else { 0 }).collect(),
                8 | 9 => prev.clone(),
                10 => { let mut l = prev.clone(); if !l.is_empty() { l.remove(rng.next() as usize % l.len()); } l }
                11 => { let mut l = prev.clone(); l.push((rng.next() % 6000) as u32); l }
                12 => { let mut l = prev.clone(); if let Some(v) = l.first_mut() { *v ^= FLAG; } l }
                _ => (0..4).map(|k| [0u32, 255, 256, 65_535, 65_536, 16_777_215, 16_777_216, BARE][(rng.next() % 8) as usize] + k).collect(),
            };
            l.sort_unstable();
            prev = l.clone();
            out.push((cell(parent << 10 | child), l));
        }
    }
    // The longest list a cell holds, at the last cell of the last parent.
    let last = out.len() - 1;
    out[last].1 = (0..0xFFFF).map(|k| k * 7).collect();
    out
}

#[test]
fn every_list_shape_reads_back_from_each_kind() {
    let mut rng = Rng::new(20261109);
    for format in FORMATS {
        let lists = shapes(&mut rng);
        let pairs = open(write(&lists, format), format).unwrap();
        for (c, l) in &lists {
            assert_eq!(&pairs.ids(*c), l, "{format:?} cell {c:#x}");
            assert_eq!(&pairs.with_ids(*c, |ids| ids.to_vec()), l, "{format:?} cell {c:#x}");
        }
        // Cells it doesn't hold: absent children of held parents, absent parents, past the last
        // bitmap word, at other levels and not cells at all.
        let held: std::collections::HashSet<u64> = lists.iter().map(|x| x.0).collect();
        let mut absent: Vec<u64> = (0..2000).map(|_| cell(rng.next() % (6 << 20))).filter(|c| !held.contains(c)).collect();
        absent.extend([cell(3 << 10), cell(66 << 10 | 5), lists[0].0 << 2, lists[0].0 | 2, 0, u64::MAX]);
        for c in absent {
            assert!(pairs.ids(c).is_empty(), "{format:?} cell {c:#x}");
        }
    }
}

/// Every level an index can be at (5 to 13): lists in its first and last parents and those at the
/// bitmap's word ends among them read back at that level. Below level 8 a level's parents (6, 24
/// and 96) end inside a word.
#[test]
fn every_level_reads_back() {
    let mut rng = Rng::new(5);
    for level in MIN_LEVEL..=MAX_LEVEL {
        let parents = 6u64 << (2 * (level - LEAF_LEVELS));
        let mut at = vec![0, 1, parents - 2, parents - 1];
        at.extend([63, 64, 65, 127, 128].into_iter().filter(|&p| p < parents));
        at.sort_unstable();
        at.dedup();
        let mut lists: Vec<(u64, Vec<u32>)> = Vec::new();
        for &parent in &at {
            for child in [0u64, 1, 512, 1023] {
                let mut l: Vec<u32> = (0..1 + rng.next() % 5)
                    .map(|_| (rng.next() % 1000) as u32 | if rng.next() % 4 == 0 { FLAG } else { 0 })
                    .collect();
                l.sort_unstable();
                let n = parent << (2 * LEAF_LEVELS) | child;
                lists.push((n << (61 - 2 * level) | 1 << (60 - 2 * level), l));
            }
        }
        for format in FORMATS {
            let (cells, entries) = write(&lists, format);
            let pairs = CellPairs::open(FileBytes::Owned(cells), FileBytes::Owned(entries), format, level)
                .unwrap_or_else(|e| panic!("{format:?} at level {level}: {e}"));
            for (c, l) in &lists {
                assert_eq!(&pairs.ids(*c), l, "{format:?} at level {level}, cell {c:#x}");
            }
        }
    }
}

#[test]
fn an_index_of_no_cells_is_two_empty_files() {
    for format in FORMATS {
        let files = write(&[], format);
        assert_eq!(files, (Vec::new(), Vec::new()));
        let pairs = open(files, format).unwrap();
        assert!(pairs.ids(cell(5)).is_empty());
        assert!(CellPairs::none().ids(cell(5)).is_empty());
    }
}

/// A small region's index holds its bitmap up to the word of its last parent, so a region of one
/// cell has a few hundred bytes of cells file wherever it is: one at parent 0 holds one bitmap
/// word, one at the last parent of the last face 96 (6 * 4^5 parents over 64-bit words), never
/// the 6 * 4^10 bits of a bitmap of its cells.
#[test]
fn a_small_regions_bitmap_is_its_parents_words() {
    for (n, words) in [(5u64, 1usize), (64 << 10, 2), ((6 << 20) - 1, 96)] {
        let (cells, entries) = write(&[(cell(n), vec![7])], ADMIN);
        let record = 4 + LEAF_BYTES + 4;
        assert_eq!(cells.len(), HEADER_BYTES + record + 4 + words * 12 + 4 + FOOTER_BYTES, "cell {n}");
        assert_eq!(entries.len(), 2 + 3);
        assert_eq!(open((cells, entries), ADMIN).unwrap().ids(cell(n)), [7]);
    }
}

/// The index of two parents, as `write` gives it, and where its tail starts.
fn two_parents(format: Format) -> ((Vec<u8>, Vec<u8>), usize) {
    let lists = vec![(cell(3), vec![1, 2]), (cell(4), vec![1, 2]), (cell(70 << 10), vec![5 | FLAG])];
    let files = write(&lists, format);
    let words = 2;
    let tail = files.0.len() - (words * 12 + 2 * 4 + FOOTER_BYTES);
    (files, tail)
}

#[test]
fn open_refuses_each_malformed_cells_file() {
    let refusal = |f: &dyn Fn(&mut Vec<u8>, &mut Vec<u8>), format: Format| {
        let ((mut c, mut e), _) = two_parents(format);
        f(&mut c, &mut e);
        open((c, e), format).err().unwrap_or_default()
    };
    let ((good, _), tail) = two_parents(ADMIN);
    assert!(open(two_parents(ADMIN).0, ADMIN).is_ok());
    let cases: Vec<(&str, Box<dyn Fn(&mut Vec<u8>, &mut Vec<u8>)>, &str)> = vec![
        ("entries without cells", Box::new(|c, _| c.clear()), "entries without cells"),
        ("magic", Box::new(|c, _| c[0] = b'X'), "no GCP1 header"),
        ("too short", Box::new(|c, _| c.truncate(15)), "no GCP1 header"),
        ("kind", Box::new(|c, _| c[4] = 2), "another kind of index"),
        ("flags", Box::new(|c, _| c[5] = 3), "another kind of index"),
        ("pad", Box::new(|c, _| c[7] = 1), "another kind of index"),
        ("level", Box::new(|c, _| c[6] = 4), "a level past the levels it takes"),
        ("no records", Box::new(|c, _| { let n = c.len(); c[n - 8..n - 4].copy_from_slice(&0u32.to_le_bytes()) }), "a footer past its index"),
        ("too many words", Box::new(|c, _| { let n = c.len(); c[n - 4..].copy_from_slice(&97u32.to_le_bytes()) }), "a footer past its index"),
        ("ranks", Box::new(move |c, _| c[tail + 16 + 4] ^= 1), "ranks that aren't the bitmap's"),
        ("bitmap", Box::new(move |c, _| c[tail + 8] ^= 2), "a bitmap that isn't its records'"),
        ("directory", Box::new(move |c, _| c[tail + 24 + 4] ^= 4), "a record that isn't the directory's"),
        ("parent number", Box::new(|c, _| c[8] = 1), "a record that isn't the directory's"),
        ("end of records", Box::new(move |c, _| c[tail - 1] = 0), "no end of the records before the tail"),
        ("an extra offset", Box::new(move |c, _| { c.splice(tail - 4..tail - 4, [0u8; 4]); }), "bytes between the records and the tail"),
    ];
    for (name, f, want) in &cases {
        let got = refusal(f.as_ref(), ADMIN);
        assert!(got.contains(want), "{name}: {got}");
    }
    assert_eq!(
        CellPairs::open(FileBytes::Owned(good), FileBytes::Owned(Vec::new()), ADMIN, 11).err().unwrap(),
        "an index of level 10 cells, read at level 11"
    );
}

/// Entries a lookup can't decode give the ids before the fault and no more, never a panic.
#[test]
fn a_lookup_past_its_entries_stops_there() {
    for format in FORMATS {
        let lists = vec![(cell(3), (0..40).map(|k| k * 1000).collect::<Vec<u32>>()), (cell(4), vec![9])];
        let (c, e) = write(&lists, format);
        for cut in 0..e.len() {
            let pairs = open((c.clone(), e[..cut].to_vec()), format).unwrap();
            for (cell, l) in &lists {
                let got = pairs.ids(*cell);
                assert!(l.starts_with(&got), "{format:?} cut {cut}");
            }
        }
    }
}

/// FNV-1a 64 of bytes: the long cells files of the golden indexes, compared by hash.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, &b| (h ^ b as u64).wrapping_mul(0x100_0000_01b3))
}

/// The builder's bytes for small indexes of each kind (builder/tests/test_cell_pairs.cpp,
/// cell_pairs_writer_bytes_are_fixed): `write` gives them, and they read back.
#[test]
fn the_builders_bytes_read_back_and_write_gives_them() {
    let admin = vec![(cell(5), vec![3, 9]), (cell(6), vec![3, 9]), (cell(7), vec![3, 12, 9 | FLAG]), (cell(8), vec![7, 7]), (cell(1030), vec![])];
    let poi = vec![(cell(5), vec![1, 300, 70000, 20000000, 5 | FLAG]), (cell(6), vec![1, 300, 70000, 20000000, 5 | FLAG]), (cell(2048), vec![42])];
    let cases: [(&Vec<(u64, Vec<u32>)>, Format, usize, u64, &[u8]); 2] = [
        (&admin, ADMIN, 312, 0x736b_d71a_2827_593b, &[36, 0, 4, 0, 6, 5, 8, 1, 24, 5, 0, 8, 0, 0, 0, 0, 0]),
        (&poi, POI, 316, 0xa981_50bc_ae96_40a2, &[8, 1, 1, 57, 42, 1, 67, 16, 1, 143, 27, 48, 1, 5, 2, 0, 42]),
    ];
    for (lists, format, cells_bytes, cells_hash, entries) in cases {
        let (c, e) = write(lists, format);
        assert_eq!((c.len(), fnv1a(&c), &e[..]), (cells_bytes, cells_hash, entries), "{format:?}");
        let pairs = open((c, e), format).unwrap();
        for (cell, l) in lists {
            assert_eq!(&pairs.ids(*cell), l, "{format:?}");
        }
    }
}

/// Each cell's first row and the next one's, in a file of rows in cell order holding `rows` rows
/// a cell.
fn tiled(ranges: &[(u64, u32)]) -> Vec<(u64, u32, u32)> {
    let mut row = 0;
    ranges
        .iter()
        .map(|&(c, n)| {
            row += n;
            (c, row - n, row)
        })
        .collect()
}

fn total(ranges: &[(u64, u32)]) -> u32 {
    ranges.iter().map(|r| r.1).sum()
}

/// Cells over parents at the bitmap's word ends and the last face's last parent, their first and
/// last children, runs of neighbours and lone cells, holding one row, a few, a block's worth
/// either side of 32, and thousands.
fn range_shapes(rng: &mut Rng) -> Vec<(u64, u32)> {
    let mut out = Vec::new();
    for parent in [0u64, 1, 63, 64, 65, 1000, 4095, 6143] {
        let mut children: Vec<u64> = vec![0, 1, 2, 63, 64, 511, 1022, 1023];
        children.extend((0..40).map(|_| rng.next() % 1024));
        children.sort_unstable();
        children.dedup();
        for child in children {
            let rows = [1u32, 1, 2, 5, 31, 32, 33, 64, 200, 22690][(rng.next() % 10) as usize];
            out.push((cell(parent << 10 | child), rows));
        }
    }
    out
}

fn open_ranges(cells: Vec<u8>, rows: u32) -> Result<CellPairs, String> {
    CellPairs::open_ranges(FileBytes::Owned(cells), DEFAULT_ADMIN_CELL_LEVEL, rows)
}

#[test]
fn every_range_reads_back_and_a_cell_it_doesnt_hold_has_none() {
    let mut rng = Rng::new(20261117);
    let ranges = range_shapes(&mut rng);
    let pairs = open_ranges(write_ranges(&ranges), total(&ranges)).unwrap();
    for (c, first, end) in tiled(&ranges) {
        assert_eq!(pairs.range(c), (first, end), "cell {c:#x}");
        assert!(pairs.ids(c).is_empty());
    }
    // The walk gives every cell in order with its range.
    let mut walked = Vec::new();
    pairs.for_each_range(|c, first, end| walked.push((c, first, end)));
    assert_eq!(walked, tiled(&ranges));
    let held: std::collections::HashSet<u64> = ranges.iter().map(|x| x.0).collect();
    let mut absent: Vec<u64> = (0..2000).map(|_| cell(rng.next() % (6 << 20))).filter(|c| !held.contains(c)).collect();
    absent.extend([cell(3 << 10), cell(66 << 10 | 5), ranges[0].0 << 2, ranges[0].0 | 2, 0, u64::MAX]);
    for c in absent {
        assert_eq!(pairs.range(c), (0, 0), "cell {c:#x}");
    }
}

#[test]
fn ranges_at_every_level_read_back() {
    let mut rng = Rng::new(6);
    for level in MIN_LEVEL..=MAX_LEVEL {
        let parents = 6u64 << (2 * (level - LEAF_LEVELS));
        let mut at = vec![0, 1, parents - 2, parents - 1];
        at.extend([63, 64, 65, 127, 128].into_iter().filter(|&p| p < parents));
        at.sort_unstable();
        at.dedup();
        let mut ranges = Vec::new();
        for &parent in &at {
            for child in [0u64, 1, 512, 1023] {
                let n = parent << (2 * LEAF_LEVELS) | child;
                ranges.push((n << (61 - 2 * level) | 1 << (60 - 2 * level), 1 + (rng.next() % 50) as u32));
            }
        }
        let pairs = CellPairs::open_ranges(FileBytes::Owned(write_ranges(&ranges)), level, total(&ranges))
            .unwrap_or_else(|e| panic!("level {level}: {e}"));
        for (c, first, end) in tiled(&ranges) {
            assert_eq!(pairs.range(c), (first, end), "level {level}, cell {c:#x}");
        }
        let mut walked = Vec::new();
        pairs.for_each_range(|c, first, end| walked.push((c, first, end)));
        assert_eq!(walked, tiled(&ranges), "level {level}");
    }
}

/// The builder's bytes for a small ranges index (builder/tests/test_cell_pairs.cpp,
/// cell_pairs_ranges_writer_bytes_are_fixed): `write_ranges` gives them, and they read back.
#[test]
fn the_builders_ranges_bytes_read_back_and_write_ranges_gives_them() {
    let ranges = [(cell(5), 3), (cell(6), 1), (cell(7), 40), (cell(2048), 2)];
    let c = write_ranges(&ranges);
    assert_eq!((c.len(), fnv1a(&c)), (328, 0x9266_4348_4cc7_2de1));
    assert_eq!(&c[..14], &[b'G', b'C', b'P', b'1', 3, 0, 10, 0, 0, 0, 0, 0, 0xE0, 0]);
    let pairs = open_ranges(c, 46).unwrap();
    assert_eq!([cell(5), cell(6), cell(7), cell(2048)].map(|c| pairs.range(c)), [(0, 3), (3, 4), (4, 44), (44, 46)]);
}

#[test]
fn a_ranges_index_of_no_cells_is_an_empty_file_of_no_rows() {
    assert!(write_ranges(&[]).is_empty());
    let pairs = open_ranges(Vec::new(), 0).unwrap();
    assert_eq!(pairs.range(cell(5)), (0, 0));
    pairs.for_each_range(|c, _, _| panic!("cell {c:#x}"));
    assert!(open_ranges(Vec::new(), 1).is_err());
    // An index of lists has no ranges to walk.
    let (cells, entries) = write(&[(cell(5), vec![1, 2])], POI);
    open((cells, entries), POI).unwrap().for_each_range(|c, _, _| panic!("cell {c:#x}"));
}

#[test]
fn open_ranges_refuses_ranges_that_dont_tile_the_rows() {
    // Two cells of 3 and 2 rows in parent 0, then one of 4 in parent 1: the first record's payload
    // (0, 3, 5) right after its leaf, the second's (5, 9) after its own.
    let ranges = [(cell(5), 3), (cell(6), 2), (cell(1 << 10 | 7), 4)];
    let good = write_ranges(&ranges);
    let first = HEADER_BYTES + 4 + LEAF_BYTES;
    let second = first + 12 + 4 + LEAF_BYTES;
    assert!(open_ranges(good.clone(), 9).is_ok());
    let set = |at: usize, v: u32| {
        let mut c = good.clone();
        c[at..at + 4].copy_from_slice(&v.to_le_bytes());
        c
    };
    let cases: Vec<(&str, Vec<u8>, u32, &str)> = vec![
        ("not from row 0", set(first, 1), 9, "a record that doesn't start where the one before ends"),
        ("a gap between records", set(second, 6), 9, "a record that doesn't start where the one before ends"),
        ("records overlapping", set(second, 4), 9, "a record that doesn't start where the one before ends"),
        ("an empty cell", set(first + 4, 0), 9, "a cell without rows"),
        ("an empty last cell", set(second + 4, 5), 9, "a cell without rows"),
        ("rows going back", set(first + 4, 6), 9, "rows out of order"),
        ("ending short of the rows", good.clone(), 10, "ranges that don't end at the file's last row"),
        ("ending past the rows", good.clone(), 8, "ranges that don't end at the file's last row"),
    ];
    for (name, cells, rows, want) in cases {
        let got = open_ranges(cells, rows).err().unwrap_or_default();
        assert!(got.contains(want), "{name}: {got}");
    }
    // Another kind of index, the lists format asked of a ranges file, and another level.
    let (admin, _) = write(&[(cell(5), vec![1])], ADMIN);
    assert!(open_ranges(admin, 1).err().unwrap().contains("another kind of index"));
    assert!(open((good.clone(), Vec::new()), POI).err().unwrap().contains("another kind of index"));
    assert!(open((good.clone(), Vec::new()), POSTCODE_CELLS).is_err());
    assert_eq!(
        CellPairs::open_ranges(FileBytes::Owned(good), 11, 9).err().unwrap(),
        "an index of level 10 cells, read at level 11"
    );
}

/// A ranges file with bytes changed opens or is refused, never panics, and every range it gives
/// lies in the rows.
#[test]
fn damaged_ranges_never_panic_and_stay_in_the_rows() {
    let mut rng = Rng::new(31);
    let ranges = range_shapes(&mut rng);
    let rows = total(&ranges);
    let good = write_ranges(&ranges);
    let probes: Vec<u64> = ranges.iter().map(|r| r.0).step_by(7).chain([cell(3), 0, u64::MAX]).collect();
    let (mut opened, mut refused) = (0, 0);
    for t in 0..3000 {
        let mut c = good.clone();
        if t % 100 == 0 {
            c.truncate((rng.next() % c.len() as u64) as usize);
        } else {
            for _ in 0..1 + rng.next() % 3 {
                let at = (rng.next() % c.len() as u64) as usize;
                c[at] ^= 1 << (rng.next() % 8);
            }
        }
        let Ok(pairs) = open_ranges(c, rows) else {
            refused += 1;
            continue;
        };
        opened += 1;
        for &p in &probes {
            let (first, end) = pairs.range(p);
            assert!(first <= end && end <= rows);
        }
    }
    assert!(opened > 0 && refused > 0);
}
