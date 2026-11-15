use super::*;
use crate::coord_keys::BitWriter;
use crate::test_rng::Rng;
use crate::NO_DATA;

const NAME: &str = "way_postcodes.bin";

fn open(bytes: &[u8]) -> Result<SideBlocks, String> {
    SideBlocks::open(FileBytes::Owned(bytes.to_vec()), NAME)
}

/// `n` values of one of four kinds: a postcode column (runs of one id, NO_DATA among them),
/// parents (ids that climb, a few of them NO_DATA), any 32 bits, and values within a range of
/// some width.
fn random_values(rng: &mut Rng, n: usize, kind: u32) -> Vec<u32> {
    let mut run = (rng.next() % 100_000) as u32;
    let width = (rng.next() % 33) as u32;
    let low = rng.next() as u32;
    (0..n)
        .map(|_| {
            let r = rng.next();
            match kind {
                0 => {
                    if r % 9 == 0 {
                        run = (rng.next() % 100_000) as u32;
                    }
                    if (r >> 8) % 6 == 0 { NO_DATA } else { run }
                }
                1 => {
                    run += (r % 5) as u32;
                    if (r >> 8) % 40 == 0 { NO_DATA } else { run - ((r >> 16) % 3) as u32 }
                }
                2 => r as u32,
                _ => {
                    let d = if width == 32 { r as u32 } else { r as u32 & ((1 << width) - 1) };
                    low.checked_add(d).unwrap_or(d)
                }
            }
        })
        .collect()
}

fn fields(values: &[u32], w: u32) -> Vec<u8> {
    let mut bits = BitWriter::default();
    for &v in values {
        bits.put(v, w);
    }
    bits.finish()
}

/// A FOR block written by hand: the base, the width, and each value's difference from the base.
fn for_block(base: u32, w: u8, diffs: &[u32]) -> Vec<u8> {
    let mut out = vec![0];
    out.extend(base.to_le_bytes());
    out.push(w);
    out.extend(fields(diffs, w as u32));
    out
}

/// A dictionary block written by hand: the entries, the width, and each value's index.
fn dictionary_block(entries: &[u32], w: u8, indexes: &[u32]) -> Vec<u8> {
    let mut out = vec![1, entries.len() as u8];
    for e in entries {
        out.extend(e.to_le_bytes());
    }
    out.push(w);
    out.extend(fields(indexes, w as u32));
    out
}

/// The blocks with the table of their offsets and a footer of `values` values.
fn file_of(blocks: &[Vec<u8>], values: u32) -> Vec<u8> {
    let (mut out, mut table) = (Vec::new(), Vec::new());
    for b in blocks {
        table.extend((out.len() as u32).to_le_bytes());
        out.extend(b);
    }
    out.extend(table);
    out.extend(values.to_le_bytes());
    out.extend(b"SDB1");
    out
}

/// The values of ids [0, n) of a file that opens.
fn read(bytes: &[u8], n: u32) -> Vec<Option<u32>> {
    let f = open(bytes).unwrap();
    (0..n).map(|id| f.get(id)).collect()
}

#[test]
fn every_value_reads_back_at_its_id() {
    let mut rng = Rng::new(20261114);
    for n in [0usize, 1, 2, 63, 64, 65, 127, 128, 129, 3000] {
        for kind in 0..4 {
            let values = random_values(&mut rng, n, kind);
            let f = open(&encode(&values)).unwrap();
            assert_eq!(f.count() as usize, n);
            for (id, v) in values.iter().enumerate() {
                assert_eq!(f.get(id as u32), Some(*v), "value {id} of {n}, kind {kind}");
            }
            // Past the last value, in its block and past the blocks.
            for id in [n as u32, n as u32 + 1, n as u32 + 64, n as u32 + 6400, u32::MAX] {
                assert_eq!(f.get(id), None, "id {id} of {n}");
            }
        }
    }
}

#[test]
fn files_no_builder_writes_are_refused_at_open_by_name() {
    // Four values far apart: a dictionary of 20 bytes, its offset and the footer.
    let good = encode(&[5, 6, NO_DATA, 9]);
    assert_eq!(good.len(), 20 + 4 + 8);
    assert!(open(&good).is_ok());
    let refusal = |bytes: &[u8]| open(bytes).err().unwrap_or_default();
    let counting = |values: u32| {
        let mut f = good.clone();
        let at = f.len() - 8;
        f[at..at + 4].copy_from_slice(&values.to_le_bytes());
        f
    };
    // No footer, a cut one, and another file's.
    assert_eq!(refusal(&[]), "way_postcodes.bin has no SDB1 footer");
    assert_eq!(refusal(&good[..7]), "way_postcodes.bin has no SDB1 footer");
    assert_eq!(refusal(&good[..good.len() - 1]), "way_postcodes.bin has no SDB1 footer");
    let mut other = good.clone();
    *other.last_mut().unwrap() = b'2';
    assert_eq!(refusal(&other), "way_postcodes.bin has no SDB1 footer");
    // A footer counting more values than the file holds table entries for.
    assert_eq!(refusal(&counting(u32::MAX)), "way_postcodes.bin is shorter than its block table");
    assert_eq!(refusal(&counting(64 * 6 + 1)), "way_postcodes.bin is shorter than its block table");
    // Blocks of fewer bytes than the least their values take, 6 a block, or of more than the
    // most, 262 a block; and blocks where there are no values.
    let size = "way_postcodes.bin has blocks of a size its values cannot take";
    assert!(open(&counting(64)).is_ok());
    assert!(open(&counting(128)).is_ok());
    assert_eq!(refusal(&counting(129)), size);
    assert_eq!(refusal(&counting(0)), size);
    assert!(open(&file_of(&[vec![0; 262]], 1)).is_ok());
    assert_eq!(refusal(&file_of(&[vec![0; 263]], 1)), size);
    assert!(open(&file_of(&[vec![0; 5], vec![0; 7]], 65)).is_ok());
    assert_eq!(refusal(&file_of(&[vec![0; 5], vec![0; 6]], 65)), size);
    assert!(open(&file_of(&[], 0)).is_ok());
}

#[test]
fn damaged_blocks_and_tables_read_without_a_panic() {
    let mut rng = Rng::new(99);
    for kind in 0..4 {
        let values = random_values(&mut rng, 1000, kind);
        let good = encode(&values);
        let blocks = values.len().div_ceil(BLOCK_VALUES);
        let table_at = good.len() - 8 - 4 * blocks;
        let entry = |b: usize| u32::from_le_bytes(good[table_at + 4 * b..][..4].try_into().unwrap()) as usize;
        let starts: Vec<usize> = (0..blocks).map(entry).collect();
        for _ in 0..500 {
            let mut bytes = good.clone();
            let mut hit = vec![false; blocks];
            for _ in 0..1 + rng.next() % 4 {
                // Keep the footer, so the file opens.
                let at = (rng.next() % (bytes.len() as u64 - 8)) as usize;
                bytes[at] = rng.next() as u8;
                // The block the byte is in, or whose table entry it is in; and the block before
                // that one, which ends where the entry says.
                let b = if at >= table_at { (at - table_at) / 4 } else { starts.partition_point(|&s| s <= at) - 1 };
                hit[b] = true;
                if at >= table_at && b > 0 {
                    hit[b - 1] = true;
                }
            }
            let f = open(&bytes).unwrap();
            for id in 0..f.count() + 2 {
                let got = f.get(id);
                // A block no byte changed in or under still reads.
                if (id as usize) < values.len() && !hit[id as usize / BLOCK_VALUES] {
                    assert_eq!(got, Some(values[id as usize]), "value {id}");
                }
            }
        }
    }
}

#[test]
fn a_block_no_builder_writes_reads_as_no_value_where_it_has_none() {
    let far = 1 << 28;
    // The blocks the cases below are cut from read whole.
    assert_eq!(read(&file_of(&[for_block(5, 8, &[0, 1, 2, 3])], 4), 5), [Some(5), Some(6), Some(7), Some(8), None]);
    let dictionary = file_of(&[dictionary_block(&[0, far, 5], 2, &[0, 1, 2, 1])], 4);
    assert_eq!(read(&dictionary, 4), [Some(0), Some(far), Some(5), Some(far)]);
    // A base and a difference that pass 32 bits.
    assert_eq!(read(&file_of(&[for_block(u32::MAX, 1, &[0, 1])], 2), 2), [Some(u32::MAX), None]);
    // A width past 32 bits, in either mode, with the bytes its fields take.
    let mut wide = for_block(5, 33, &[]);
    wide.extend([0; 9]);
    assert_eq!(read(&file_of(&[wide], 2), 2), [None, None]);
    let mut wide = dictionary_block(&[0, far], 1, &[0, 1]);
    wide[10] = 40;
    wide.extend([0; 9]);
    assert_eq!(read(&file_of(&[wide], 2), 2), [None, None]);
    // An index past the dictionary's entries.
    assert_eq!(read(&file_of(&[dictionary_block(&[0, far, 5], 2, &[0, 1, 2, 3])], 4), 4), [Some(0), Some(far), Some(5), None]);
    // A dictionary whose entries run past the blocks.
    assert_eq!(read(&file_of(&[vec![1, 200, 0, 0, 0, 0, 0]], 2), 2), [None, None]);
    // A mode that is neither.
    let mut neither = for_block(5, 8, &[0, 1, 2, 3]);
    neither[0] = 2;
    assert_eq!(read(&file_of(&[neither], 4), 4), [None; 4]);
    // More values than the block has fields for: it is not the bytes its values take, and none
    // of them reads.
    assert_eq!(read(&file_of(&[for_block(5, 8, &[0, 1, 2, 3])], 9), 9), [None; 9]);
    assert_eq!(read(&file_of(&[for_block(5, 12, &[0, 1, 2, 3])], 5), 5), [None; 5]);
    // A table entry at the blocks' end and past it; the next block still reads.
    let two = file_of(&[for_block(9, 0, &[]), for_block(5, 8, &[0, 1, 2, 3])], 68);
    let table_at = 6 + 10;
    for entry in [table_at as u32, table_at as u32 + 1, u32::MAX] {
        let mut moved = two.clone();
        moved[table_at..table_at + 4].copy_from_slice(&entry.to_le_bytes());
        let got = read(&moved, 68);
        assert!(got[..64].iter().all(Option::is_none), "{entry}");
        assert_eq!(got[64..], [Some(5), Some(6), Some(7), Some(8)], "{entry}");
    }
    assert_eq!(read(&two, 68)[..64], [Some(9); 64]);
}

#[test]
fn a_block_is_the_bytes_up_to_the_next_blocks_start_and_no_value_is_read_from_past_them() {
    let some = |values: &[u32]| values.iter().map(|&v| Some(v)).collect::<Vec<_>>();
    // 64 values of 9 and one of 5: two FOR blocks of no bits a value, 6 bytes each.
    let values = [vec![9; 64], vec![5]].concat();
    let good = file_of(&[for_block(9, 0, &[]), for_block(5, 0, &[])], 65);
    assert_eq!(encode(&values), good);
    assert_eq!(read(&good, 65), some(&values));
    // The first block's width set to 1: its 64 fields would take 8 bytes, and the next block
    // starts where they would. None is read from that block's bytes, and it still reads.
    let mut wide = good.clone();
    wide[5] = 1;
    assert_eq!(open(&wide).unwrap().get(8), None);
    assert_eq!(read(&wide, 65), [vec![None; 64], vec![Some(5)]].concat());

    // A block of a byte more than its values take, or of a byte fewer, is no block, whether the
    // next block's start ends it or the table's; the block beside it reads.
    let ones = for_block(9, 1, &[1; 64]);
    let four = for_block(5, 8, &[0, 1, 2, 3]);
    assert_eq!((ones.len(), four.len()), (14, 10));
    let (tens, from_five) = (vec![Some(10); 64], some(&[5, 6, 7, 8]));
    assert_eq!(read(&file_of(&[ones.clone(), four.clone()], 68), 68), [tens.clone(), from_five.clone()].concat());
    let longer = |block: &[u8]| [block, &[0u8][..]].concat();
    let shorter = |block: &[u8]| block[..block.len() - 1].to_vec();
    for first in [longer(&ones), shorter(&ones)] {
        assert_eq!(read(&file_of(&[first, four.clone()], 68), 68), [vec![None; 64], from_five.clone()].concat());
    }
    for last in [longer(&four), shorter(&four)] {
        assert_eq!(read(&file_of(&[ones.clone(), last], 68), 68), [tens.clone(), vec![None; 4]].concat());
    }
    // The same of a dictionary, its entries and its indexes.
    let far = 1 << 28;
    let dictionary = dictionary_block(&[7, far], 1, &[0, 1, 1, 0]);
    assert_eq!(read(&file_of(&[dictionary.clone()], 4), 4), some(&[7, far, far, 7]));
    for block in [longer(&dictionary), shorter(&dictionary)] {
        assert_eq!(read(&file_of(&[block], 4), 4), [None; 4]);
    }
}

#[test]
fn a_dictionary_of_more_entries_than_values_or_of_indexes_of_another_width_gives_no_value() {
    let far = 1 << 28;
    // As many entries as the block has values, and one more.
    assert_eq!(read(&file_of(&[dictionary_block(&[7, far], 1, &[0, 1])], 2), 2), [Some(7), Some(far)]);
    assert_eq!(read(&file_of(&[dictionary_block(&[7, far], 1, &[1])], 1), 1), [None]);
    // Two entries take indexes of 1 bit and three of 2. Four indexes of a bit more or fewer
    // fill the same byte, so the block is the bytes its values take.
    assert_eq!(read(&file_of(&[dictionary_block(&[7, far, 5], 2, &[0, 1, 2, 0])], 4), 4), [Some(7), Some(far), Some(5), Some(7)]);
    assert_eq!(read(&file_of(&[dictionary_block(&[7, far], 2, &[0, 1, 1, 0])], 4), 4), [None; 4]);
    assert_eq!(read(&file_of(&[dictionary_block(&[7, far, 5], 1, &[0, 1, 1, 0])], 4), 4), [None; 4]);
    assert_eq!(read(&file_of(&[dictionary_block(&[7, far, 5], 3, &[0, 1, 2, 0])], 4), 4), [None; 4]);
}

#[test]
fn the_first_block_starts_at_the_files_first_byte() {
    // A byte before the blocks, and the table counting from it: the first block is none, and
    // the one after it reads.
    let mut shifted = vec![0xAA];
    shifted.extend(for_block(9, 0, &[]));
    shifted.extend(for_block(5, 8, &[0, 1, 2, 3]));
    for entry in [1u32, 7] {
        shifted.extend(entry.to_le_bytes());
    }
    shifted.extend(68u32.to_le_bytes());
    shifted.extend(b"SDB1");
    let got = read(&shifted, 68);
    assert!(got[..64].iter().all(Option::is_none), "{got:?}");
    assert_eq!(got[64..], [Some(5), Some(6), Some(7), Some(8)]);
}

#[test]
fn side_files_read_as_the_shared_vectors_say_and_the_test_encoder_writes_them() {
    let rows = crate::exact_pip::tests::testdata_rows("side_blocks.tsv");
    let hex = |s: &str| (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap()).collect::<Vec<u8>>();
    for r in &rows {
        assert_eq!(r.len(), 3, "{r:?}");
        let values: Vec<u32> = if r[1] == "-" { Vec::new() } else { r[1].split(',').map(|v| v.parse().unwrap()).collect() };
        let file = hex(&r[2]);
        assert_eq!(encode(&values), file, "{}", r[0]);
        let f = open(&file).unwrap();
        assert_eq!(f.count() as usize, values.len(), "{}", r[0]);
        for (id, v) in values.iter().enumerate() {
            assert_eq!(f.get(id as u32), Some(*v), "{} value {id}", r[0]);
        }
        assert_eq!(f.get(values.len() as u32), None, "{}", r[0]);
    }
    assert!(rows.len() >= 75);
}
