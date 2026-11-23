//! The postcode grid as the builder writes it (builder/src/postcode_grid.h, which lays the format
//! out; builder/src/postcode_grid_build.h says what each cell holds): postcode_grid.bin, in the
//! countries it covers the postcode of each S2 cell at the grid's level, as runs of cells in cell
//! order, in blocks of BLOCK_ROWS rows with a table of each block's first cell, and a footer. A
//! cell's postcode is its run's: the last row whose cell is at or before it, none before the first
//! row or in a row of no postcode.
//!
//! The reader checks the footer and the table's size at open, and reads a block without trusting
//! it: a malformed block reads as no postcode or a wrong one, never a panic.

use crate::FileBytes;

pub const FILE: &str = "postcode_grid.bin";
const MAGIC: &[u8; 4] = b"PGR1";
pub const BLOCK_ROWS: usize = 128;
const ENTRY_BYTES: usize = 12;
const FOOTER_BYTES: usize = 12;
/// The longest code, and the finest level a grid can be at.
pub const CODE_BYTES: usize = 14;
pub const MAX_LEVEL: u64 = 20;
const RICE_ESCAPE: u32 = 12;

/// A run as the patch tools diff it (postcode_grid.h's LogicalRow): its first cell, its country
/// (two upper-case letters, 0 for none) and its code (empty for none).
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalRow {
    pub cell: u64,
    pub country_code: u16,
    pub code: String,
}

#[inline(always)]
fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}

#[inline(always)]
fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

/// The bits a cell id's position at `level` starts past.
fn cell_shift(level: u64) -> u64 {
    2 * (30 - level) + 1
}

/// The fewest bits that hold an index below n.
fn index_bits(n: usize) -> u32 {
    if n <= 1 { 0 } else { 64 - ((n - 1) as u64).leading_zeros() }
}

/// Bits LSB first from a block's bytes; None once a read passes the end.
struct Bits<'a> {
    b: &'a [u8],
    bit: u64,
}

impl Bits<'_> {
    fn get(&mut self, w: u32) -> Option<u64> {
        let mut v = 0u64;
        for i in 0..w {
            let byte = *self.b.get((self.bit / 8) as usize)?;
            v |= ((byte >> (self.bit % 8)) as u64 & 1) << i;
            self.bit += 1;
        }
        Some(v)
    }

    fn rice(&mut self, k: u32) -> Option<u64> {
        let mut q = 0u64;
        while self.get(1)? == 0 {
            q += 1;
            if q == RICE_ESCAPE as u64 {
                return self.get(64);
            }
        }
        let low = self.get(k)?;
        Some(q.checked_shl(k).unwrap_or(0) | low)
    }
}

/// A block's entries as they sit: (country, code) each.
struct Entries<'a> {
    list: [(u16, &'a str); BLOCK_ROWS],
    n: usize,
}

/// The file in place.
pub struct PostcodeGrid {
    file: FileBytes,
    rows: u32,
    level: u64,
    table_at: usize,
}

impl PostcodeGrid {
    /// The file of a dir, mapped; an error for one no builder writes (a bad footer, a level past
    /// the grid's, a table past the file), which a block's bytes are not checked for.
    pub fn open(file: FileBytes) -> Result<PostcodeGrid, String> {
        let b = file.as_slice();
        if b.len() < FOOTER_BYTES || &b[b.len() - 4..] != MAGIC {
            return Err(format!("{FILE} has no PGR1 footer"));
        }
        let f = &b[b.len() - FOOTER_BYTES..];
        let rows = u32_at(f, 0);
        let level = f[4] as u64;
        if f[5..8] != [0, 0, 0] || rows == u32::MAX || (rows > 0 && level > MAX_LEVEL) || (rows == 0 && level != 0) {
            return Err(format!("{FILE}'s footer is not one the builder writes"));
        }
        let blocks = (rows as usize).div_ceil(BLOCK_ROWS);
        let table_at = (b.len() - FOOTER_BYTES)
            .checked_sub(blocks * ENTRY_BYTES)
            .ok_or_else(|| format!("{FILE} is shorter than its block table"))?;
        Ok(PostcodeGrid { file, rows, level, table_at })
    }

    /// The runs: rows below this.
    pub fn rows(&self) -> u32 {
        self.rows
    }

    /// The level of the grid's cells (0 for a grid of no rows).
    pub fn level(&self) -> u64 {
        self.level
    }

    /// The blocks: rows below BLOCK_ROWS times this.
    pub fn blocks(&self) -> u32 {
        self.rows.div_ceil(BLOCK_ROWS as u32)
    }

    /// Block b's first cell and its bytes.
    fn block(&self, b: u32) -> Option<(u64, &[u8])> {
        if b >= self.blocks() {
            return None;
        }
        let f = self.file.as_slice();
        let at = self.table_at + b as usize * ENTRY_BYTES;
        let start = u32_at(f, at + 8) as usize;
        let end = if b + 1 < self.blocks() { u32_at(f, at + ENTRY_BYTES + 8) as usize } else { self.table_at };
        Some((u64_at(f, at), f.get(start..end.min(self.table_at))?))
    }

    /// Block b's entries, its parameter and its bits.
    fn parse(bytes: &[u8]) -> Option<(Entries<'_>, u32, Bits<'_>)> {
        let mut at = 0usize;
        let (mut n, mut shift) = (0u64, 0u32);
        loop {
            let c = *bytes.get(at)?;
            at += 1;
            n |= ((c & 0x7F) as u64).checked_shl(shift)?;
            if c & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        if n == 0 || n as usize > BLOCK_ROWS {
            return None;
        }
        let mut entries = Entries { list: [(0, ""); BLOCK_ROWS], n: n as usize };
        for e in entries.list.iter_mut().take(n as usize) {
            let country = u16::from_le_bytes(bytes.get(at..at + 2)?.try_into().ok()?);
            at += 2;
            let len = bytes.get(at..)?.iter().position(|&c| c == 0)?;
            let code = std::str::from_utf8(&bytes[at..at + len]).ok()?;
            at += len + 1;
            *e = (country, code);
        }
        let k = *bytes.get(at)? as u32;
        Some((entries, k, Bits { b: &bytes[at + 1..], bit: 0 }))
    }

    /// Calls `f` with each row of block b in turn and its entry, until it returns false or the
    /// block ends; false where the block reads as malformed before that.
    fn walk<'a>(&'a self, b: u32, mut f: impl FnMut(u64, (u16, &'a str)) -> bool) -> bool {
        let Some((first, bytes)) = self.block(b) else { return false };
        let Some((entries, k, mut bits)) = Self::parse(bytes) else { return false };
        let rows = (self.rows as usize - b as usize * BLOCK_ROWS).min(BLOCK_ROWS);
        let (entry_bits, rank_bits) = (index_bits(entries.n), index_bits(entries.n - 1));
        let Some(mut index) = bits.get(entry_bits).map(|i| i as usize).filter(|&i| i < entries.n) else { return false };
        let mut cell = first;
        let shift = cell_shift(self.level);
        for i in 0..rows {
            if !f(cell, entries.list[index]) {
                return true;
            }
            if i + 1 == rows {
                break;
            }
            let (Some(step), Some(rank)) = (bits.rice(k), bits.get(rank_bits)) else { return false };
            let rank = rank as usize;
            if rank + 1 >= entries.n {
                return false;
            }
            index = if rank < index { rank } else { rank + 1 };
            let Some(next) = step.checked_add(1).and_then(|s| s.checked_mul(1u64 << shift)).and_then(|d| cell.checked_add(d)) else {
                return false;
            };
            cell = next;
        }
        true
    }

    /// The postcode of `cell`, an S2 cell id at the grid's level: its run's country and code, None
    /// in a run of no postcode or before the first.
    pub fn postcode_at(&self, cell: u64) -> Option<([u8; 2], &str)> {
        let blocks = self.blocks();
        if blocks == 0 {
            return None;
        }
        let f = self.file.as_slice();
        let first_of = |b: u32| u64_at(f, self.table_at + b as usize * ENTRY_BYTES);
        // The last block whose first cell is at or before the cell.
        let (mut lo, mut hi) = (0u32, blocks);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if first_of(mid) <= cell { lo = mid + 1 } else { hi = mid }
        }
        let b = lo.checked_sub(1)?;
        let mut found = None;
        self.walk(b, |c, entry| {
            if c > cell {
                return false;
            }
            found = Some(entry);
            true
        });
        let (country, code) = found?;
        (country != 0 && !code.is_empty()).then(|| (country.to_be_bytes(), code))
    }

    /// Calls `f` with each row in order, a block read whole; false where a block reads as
    /// malformed (its rows before that given).
    pub fn for_rows(&self, mut f: impl FnMut(LogicalRow)) -> bool {
        (0..self.blocks()).all(|b| {
            self.walk(b, |cell, (country, code)| {
                f(LogicalRow { cell, country_code: country, code: code.to_string() });
                true
            })
        })
    }
}

/// The file the builder's encoder writes for `rows` (postcode_grid.h), for the fixtures.
#[cfg(any(test, feature = "fixtures"))]
pub fn encode(rows: &[LogicalRow]) -> Vec<u8> {
    fn rice_bits(z: u64, k: u32) -> u64 {
        if (z >> k) < RICE_ESCAPE as u64 { (z >> k) + 1 + k as u64 } else { RICE_ESCAPE as u64 + 64 }
    }
    struct Out {
        b: Vec<u8>,
        bit: u64,
    }
    impl Out {
        fn put(&mut self, v: u64, w: u32) {
            for i in 0..w {
                if self.bit / 8 >= self.b.len() as u64 {
                    self.b.push(0);
                }
                self.b[(self.bit / 8) as usize] |= ((v >> i & 1) as u8) << (self.bit % 8);
                self.bit += 1;
            }
        }
    }
    let level = rows.first().map_or(0, |r| 30 - r.cell.trailing_zeros() as u64 / 2);
    let shift = cell_shift(level);
    let (mut out, mut table) = (Vec::new(), Vec::new());
    for block in rows.chunks(BLOCK_ROWS) {
        table.extend_from_slice(&block[0].cell.to_le_bytes());
        table.extend_from_slice(&(out.len() as u32).to_le_bytes());
        let mut entries: Vec<(u16, &str)> = block.iter().map(|r| (r.country_code, r.code.as_str())).collect();
        entries.sort();
        entries.dedup();
        let mut n = entries.len() as u64;
        while n >= 0x80 {
            out.push(n as u8 | 0x80);
            n >>= 7;
        }
        out.push(n as u8);
        for (country, code) in &entries {
            out.extend_from_slice(&country.to_le_bytes());
            out.extend_from_slice(code.as_bytes());
            out.push(0);
        }
        let steps: Vec<u64> = block.windows(2).map(|w| ((w[1].cell - w[0].cell) >> shift) - 1).collect();
        let k = (0..=63u32).min_by_key(|&k| (steps.iter().map(|&z| rice_bits(z, k)).sum::<u64>(), k)).filter(|_| !steps.is_empty()).unwrap_or(0);
        out.push(k as u8);
        let index = |r: &LogicalRow| entries.binary_search(&(r.country_code, r.code.as_str())).unwrap();
        let mut bits = Out { b: Vec::new(), bit: 0 };
        bits.put(index(&block[0]) as u64, index_bits(entries.len()));
        for (i, step) in steps.iter().enumerate() {
            let q = step >> k;
            if q >= RICE_ESCAPE as u64 {
                bits.put(0, RICE_ESCAPE);
                bits.put(*step, 64);
            } else {
                bits.put(0, q as u32);
                bits.put(1, 1);
                bits.put(*step, k);
            }
            let (prev, at) = (index(&block[i]), index(&block[i + 1]));
            bits.put(if at < prev { at } else { at - 1 } as u64, index_bits(entries.len() - 1));
        }
        out.extend_from_slice(&bits.b);
    }
    out.extend_from_slice(&table);
    out.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    out.extend_from_slice(&[level as u8, 0, 0, 0]);
    out.extend_from_slice(MAGIC);
    out
}

#[cfg(test)]
mod tests;
