use super::*;
use crate::test_rng::Rng;
use crate::DEFAULT_ADMIN_CELL_LEVEL;

const FORMATS: [Format; 3] = [ADMIN, POI, POSTCODE_CENTROID];

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
                assert!(l.starts_with(&got) || format.kind == Kind::Blocks, "{format:?} cut {cut}");
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
    let postcode = vec![(cell(5), vec![10, 11, 400]), (cell(6), vec![10, 11, 400]), (cell(7), vec![0xFFFF_FFF0])];
    let cases: [(&Vec<(u64, Vec<u32>)>, Format, usize, u64, &[u8]); 3] = [
        (&admin, ADMIN, 312, 0x736b_d71a_2827_593b, &[36, 0, 4, 0, 6, 5, 8, 1, 24, 5, 0, 8, 0, 0, 0, 0, 0]),
        (&poi, POI, 316, 0xa981_50bc_ae96_40a2, &[8, 1, 1, 57, 42, 1, 67, 16, 1, 143, 27, 48, 1, 5, 2, 0, 42]),
        (&postcode, POSTCODE_CENTROID, 180, 0x1b32_dba5_fd24_c3cc, &[6, 10, 0, 132, 3, 2, 240, 255, 255, 255, 15]),
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
