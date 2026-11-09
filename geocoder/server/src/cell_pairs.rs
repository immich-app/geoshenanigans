//! The cell pair indexes, the admin, POI and postcode centroid indexes: each cell's sorted list of
//! ids, in the cells file and the entries file the builder writes (builder/src/cell_pairs.h, which
//! lays the format out). A lookup finds the cell's parent record through the parent bitmap, its
//! rank among the parent's rows through the leaf, and decodes its list: a direct index's own list,
//! or a block index's block up to the cell's row.

use crate::FileBytes;
use std::cell::Cell;

const MAGIC: &[u8; 4] = b"GCP1";
const HEADER_BYTES: usize = 8;
const FOOTER_BYTES: usize = 8;
const LEAF_LEVELS: u64 = 5;
const LEAF_WORDS: usize = 16;
const LEAF_BYTES: usize = LEAF_WORDS * 8;
const BLOCK_ROWS: usize = 8;
const LIST_MAX: u64 = 0xFFFF;
const FLAG: u32 = 0x8000_0000;
const BARE: u32 = 0x7FFF_FFFF;
const END_OF_RECORDS: u32 = 0xFFFF_FFFF;
const MIN_LEVEL: u64 = 5;
const MAX_LEVEL: u64 = 13;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    /// Lists in blocks of 8 rows, each coded against the row before.
    Blocks = 1,
    /// One list per row.
    Direct = 2,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Gaps {
    Varint,
    /// Quads of gaps, each in as few bytes as it takes, after a byte of their lengths.
    GroupVarint,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Format {
    pub kind: Kind,
    /// The ids flagged INTERIOR_FLAG coded as a group of their own.
    pub groups: bool,
    pub gaps: Gaps,
}

impl Format {
    fn flags(self) -> u8 {
        self.groups as u8 | if self.gaps == Gaps::GroupVarint { 2 } else { 0 }
    }
}

pub const ADMIN: Format = Format { kind: Kind::Blocks, groups: true, gaps: Gaps::Varint };
pub const POI: Format = Format { kind: Kind::Direct, groups: true, gaps: Gaps::GroupVarint };
pub const POSTCODE_CENTROID: Format = Format { kind: Kind::Direct, groups: false, gaps: Gaps::Varint };

/// A cell pair index's two files, checked at open.
pub struct CellPairs {
    cells: FileBytes,
    entries: FileBytes,
    /// None: an index of no cells (both files empty).
    layout: Option<Layout>,
}

#[derive(Clone, Copy)]
struct Layout {
    format: Format,
    /// A cell's number at the index's level is the cell id shifted right this far.
    shift: u32,
    pbits_at: usize,
    pwords: usize,
    ranks_at: usize,
    dir_at: usize,
}

thread_local! {
    /// A lookup's two lists, the row's and the one before (taken while it decodes).
    static SCRATCH: Cell<(Vec<u32>, Vec<u32>)> = const { Cell::new((Vec::new(), Vec::new())) };
}

/// The words of a bitmap of every parent (cell 5 levels up) at `level`: past the last whole one
/// below level 8 (6, 24 and 96 parents), at least one.
fn parent_words_at(level: u64) -> usize {
    (6u64 << (2 * (level - LEAF_LEVELS))).div_ceil(64) as usize
}

#[inline(always)]
fn rd32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}
#[inline(always)]
fn rd64(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

/// The varint at `*p`, past it; None past the end of `k` or 64 bits.
#[inline(always)]
fn varint(k: &[u8], p: &mut usize) -> Option<u64> {
    let b = *k.get(*p)?;
    *p += 1;
    if b < 0x80 {
        return Some(b as u64);
    }
    let mut v = (b & 0x7F) as u64;
    let mut shift = 7;
    loop {
        let b = *k.get(*p)?;
        *p += 1;
        v |= ((b & 0x7F) as u64) << shift;
        if b < 0x80 {
            return Some(v);
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
}

#[inline(always)]
fn unzigzag(v: u64) -> i64 {
    if v & 1 != 0 { -((v >> 1) as i64) - 1 } else { (v >> 1) as i64 }
}

const GAP_MASK: [u32; 4] = [0xFF, 0xFFFF, 0xFF_FFFF, 0xFFFF_FFFF];

/// n - 1 group varint gaps from `*p`, each added (with `inc`) to `v` and passed to `f` with `flag`.
#[inline(always)]
fn group_gaps(k: &[u8], p: &mut usize, n: u64, mut v: u32, inc: u32, flag: u32, f: &mut impl FnMut(u32)) -> Option<()> {
    let mut left = n - 1;
    while left >= 4 && *p + 17 <= k.len() {
        let lengths = k[*p] as usize;
        *p += 1;
        for q in 0..4 {
            let len = (lengths >> (2 * q)) & 3;
            let gap = rd32(k, *p) & GAP_MASK[len];
            *p += len + 1;
            v = v.wrapping_add(gap).wrapping_add(inc);
            f(v | flag);
        }
        left -= 4;
    }
    while left > 0 {
        let lengths = *k.get(*p)? as usize;
        *p += 1;
        for q in 0..left.min(4) as usize {
            let len = ((lengths >> (2 * q)) & 3) + 1;
            let bytes = k.get(*p..*p + len)?;
            *p += len;
            let gap = bytes.iter().rev().fold(0u32, |g, &b| g << 8 | b as u32);
            v = v.wrapping_add(gap).wrapping_add(inc);
            f(v | flag);
        }
        left -= left.min(4);
    }
    Some(())
}

impl CellPairs {
    /// The index of `format` at `level` in these files; an error names what is wrong with them.
    pub fn open(cells: FileBytes, entries: FileBytes, format: Format, level: u64) -> Result<CellPairs, String> {
        let layout = Self::check(cells.as_slice(), entries.as_slice(), format, level)?;
        Ok(CellPairs { cells, entries, layout })
    }

    /// An index of no cells.
    pub fn none() -> CellPairs {
        CellPairs { cells: FileBytes::Owned(Vec::new()), entries: FileBytes::Owned(Vec::new()), layout: None }
    }

    fn check(c: &[u8], e: &[u8], format: Format, level: u64) -> Result<Option<Layout>, String> {
        if c.is_empty() {
            return if e.is_empty() { Ok(None) } else { Err("entries without cells".into()) };
        }
        let bad = |what: &str| Err(format!("not a cell pair index ({what})"));
        if c.len() < HEADER_BYTES + 4 + FOOTER_BYTES || &c[0..4] != MAGIC {
            return bad("no GCP1 header");
        }
        if c[4] != format.kind as u8 || c[5] != format.flags() || c[7] != 0 {
            return bad("another kind of index");
        }
        let file_level = c[6] as u64;
        if !(MIN_LEVEL..=MAX_LEVEL).contains(&file_level) {
            return bad("a level past the levels it takes");
        }
        if file_level != level {
            return Err(format!("an index of level {file_level} cells, read at level {level}"));
        }
        let n = c.len();
        let records = rd32(c, n - 8) as usize;
        let pwords = rd32(c, n - 4) as usize;
        let max_words = parent_words_at(level);
        if pwords == 0 || pwords > max_words || records == 0 {
            return bad("a footer past its index");
        }
        let tail = pwords * 12 + records * 4 + FOOTER_BYTES;
        if tail + HEADER_BYTES + 4 > n {
            return bad("a tail past the file");
        }
        let pbits_at = n - tail;
        let ranks_at = pbits_at + 8 * pwords;
        let dir_at = ranks_at + 4 * pwords;
        if rd32(c, pbits_at - 4) != END_OF_RECORDS {
            return bad("no end of the records before the tail");
        }
        let mut before = 0usize;
        for w in 0..pwords {
            if rd32(c, ranks_at + 4 * w) as usize != before {
                return bad("ranks that aren't the bitmap's");
            }
            before += rd64(c, pbits_at + 8 * w).count_ones() as usize;
        }
        if before != records || rd64(c, pbits_at + 8 * (pwords - 1)) == 0 {
            return bad("a bitmap that isn't its records'");
        }
        // Each record where the directory says, the parent the bitmap says, its size its leaf's rows'.
        let mut at = HEADER_BYTES;
        let mut r = 0usize;
        for w in 0..pwords {
            let mut bits = rd64(c, pbits_at + 8 * w);
            while bits != 0 {
                let parent = (w * 64 + bits.trailing_zeros() as usize) as u32;
                bits &= bits - 1;
                if rd32(c, dir_at + 4 * r) as usize != at || at + 4 + LEAF_BYTES > pbits_at - 4 || rd32(c, at) != parent {
                    return bad("a record that isn't the directory's");
                }
                let rows: usize = (0..LEAF_WORDS).map(|i| rd64(c, at + 4 + 8 * i).count_ones() as usize).sum();
                let offsets = if format.kind == Kind::Blocks { rows.div_ceil(BLOCK_ROWS) } else { rows };
                at += 4 + LEAF_BYTES + 4 * offsets;
                if rows == 0 || at > pbits_at - 4 {
                    return bad("a record past its rows");
                }
                r += 1;
            }
        }
        if at != pbits_at - 4 {
            return bad("bytes between the records and the tail");
        }
        Ok(Some(Layout { format, shift: 61 - 2 * level as u32, pbits_at, pwords, ranks_at, dir_at }))
    }

    /// Where the cell's row is: its parent's record and its rank among the record's rows.
    #[inline(always)]
    fn locate(&self, l: &Layout, cell: u64) -> Option<(usize, usize)> {
        if cell & ((1u64 << l.shift) - 1) != 1u64 << (l.shift - 1) {
            return None;
        }
        let number = (cell >> l.shift) as usize;
        let parent = number >> (2 * LEAF_LEVELS);
        let w = parent / 64;
        if w >= l.pwords {
            return None;
        }
        let c = self.cells.as_slice();
        let pw = rd64(c, l.pbits_at + 8 * w);
        if (pw >> (parent % 64)) & 1 == 0 {
            return None;
        }
        let record = rd32(c, l.ranks_at + 4 * w) as usize + (pw & ((1u64 << (parent % 64)) - 1)).count_ones() as usize;
        let at = rd32(c, l.dir_at + 4 * record) as usize;
        let child = number & ((1 << (2 * LEAF_LEVELS)) - 1);
        let word = rd64(c, at + 4 + 8 * (child / 64));
        if (word >> (child % 64)) & 1 == 0 {
            return None;
        }
        let mut rank = (word & ((1u64 << (child % 64)) - 1)).count_ones() as usize;
        for i in 0..child / 64 {
            rank += rd64(c, at + 4 + 8 * i).count_ones() as usize;
        }
        Some((at, rank))
    }

    /// Calls `f` with each id the cell lists, ascending: none for a cell the index doesn't hold
    /// (and the ids before the fault of a list the entries file cuts short). The list is decoded
    /// whole first (`with_ids`), so `f` runs from one loop: the POI readers' closures are large,
    /// and inlined into the decoder's loops they took 1.04x (find_pois) and 1.13x
    /// (find_nearest_poi_with_parent) the time over planet poi/all.
    #[inline]
    pub fn for_each(&self, cell: u64, mut f: impl FnMut(u32)) {
        self.with_ids(cell, |ids| ids.iter().for_each(|&id| f(id)))
    }

    /// `f` of the ids the cell lists, ascending (as `for_each` gives them), held for it.
    pub fn with_ids<R>(&self, cell: u64, f: impl FnOnce(&[u32]) -> R) -> R {
        let (mut cur, mut prev) = SCRATCH.take();
        cur.clear();
        if let Some(l) = &self.layout {
            if let Some((at, rank)) = self.locate(l, cell) {
                let c = self.cells.as_slice();
                match l.format.kind {
                    Kind::Direct => {
                        let off = rd32(c, at + 4 + LEAF_BYTES + 4 * rank) as usize;
                        let _ = self.direct_list(l.format, off, &mut |id| cur.push(id));
                    }
                    Kind::Blocks => {
                        let off = rd32(c, at + 4 + LEAF_BYTES + 4 * (rank / BLOCK_ROWS)) as usize;
                        if self.block_row(l.format, off, rank % BLOCK_ROWS, &mut cur, &mut prev).is_none() {
                            cur.clear();
                        }
                    }
                }
            }
        }
        let r = f(&cur);
        cur.clear();
        SCRATCH.set((cur, prev));
        r
    }

    /// The ids the cell lists, ascending.
    pub fn ids(&self, cell: u64) -> Vec<u32> {
        let mut out = Vec::new();
        self.for_each(cell, |id| out.push(id));
        out
    }

    /// A direct row's list at `off`, through `f`.
    #[inline(always)]
    fn direct_list(&self, format: Format, off: usize, f: &mut impl FnMut(u32)) -> Option<()> {
        let k = self.entries.as_slice();
        let mut p = off;
        let h = varint(k, &mut p)?;
        let inc = 1 - (h & 1) as u32;
        let n0 = h >> 1;
        let n1 = if format.groups { varint(k, &mut p)? } else { 0 };
        if n0 > LIST_MAX || n1 > LIST_MAX - n0 {
            return None;
        }
        for (n, flag) in [(n0, 0), (n1, FLAG)] {
            if n == 0 {
                continue;
            }
            let mut v = varint(k, &mut p)? as u32;
            f(v | flag);
            match format.gaps {
                Gaps::GroupVarint => group_gaps(k, &mut p, n, v, inc, flag, f)?,
                Gaps::Varint => {
                    for _ in 1..n {
                        v = v.wrapping_add(varint(k, &mut p)? as u32).wrapping_add(inc);
                        f(v | flag);
                    }
                }
            }
        }
        Some(())
    }

    /// Row `target` of the block at `off` into `cur` (`prev` is scratch).
    fn block_row(&self, format: Format, off: usize, target: usize, cur: &mut Vec<u32>, prev: &mut Vec<u32>) -> Option<()> {
        let k = self.entries.as_slice();
        let codes = u16::from_le_bytes([*k.get(off)?, *k.get(off + 1)?]);
        let mut p = off + 2;
        let mut lf = [0u32; 2];
        cur.clear();
        for row in 0..=target {
            let code = (codes >> (2 * row)) & 3;
            if code == 1 {
                continue; // SAME: the row's list is the one before, in cur
            }
            std::mem::swap(cur, prev);
            cur.clear();
            match code {
                0 => Self::plain(k, &mut p, format, &lf, cur)?,
                2 => Self::edit(k, &mut p, format.groups, prev, cur)?,
                _ => return None,
            }
            // The firsts later rows code theirs against.
            let n0 = if format.groups { cur.partition_point(|&id| id & FLAG == 0) } else { cur.len() };
            if n0 > 0 {
                lf[0] = cur[0] & if format.groups { BARE } else { u32::MAX };
            }
            if n0 < cur.len() {
                lf[1] = cur[n0] & BARE;
            }
        }
        Some(())
    }

    /// A block's PLAIN list, firsts against `lf`, into `out`.
    fn plain(k: &[u8], p: &mut usize, format: Format, lf: &[u32; 2], out: &mut Vec<u32>) -> Option<()> {
        let h = varint(k, p)?;
        let inc = 1 - (h & 1) as u32;
        let n0 = h >> 1;
        let n1 = if format.groups { varint(k, p)? } else { 0 };
        if n0 > LIST_MAX || n1 > LIST_MAX - n0 {
            return None;
        }
        for (g, (n, flag)) in [(n0, 0), (n1, FLAG)].into_iter().enumerate() {
            if n == 0 {
                continue;
            }
            let mut v = (lf[g] as i64).wrapping_add(unzigzag(varint(k, p)?)) as u32;
            out.push(v | flag);
            for _ in 1..n {
                v = v.wrapping_add(varint(k, p)? as u32).wrapping_add(inc);
                out.push(v | flag);
            }
        }
        Some(())
    }

    /// The EDIT of `prev` into `out`.
    fn edit(k: &[u8], p: &mut usize, groups: bool, prev: &[u32], out: &mut Vec<u32>) -> Option<()> {
        let bits = if groups { 2 } else { 1 };
        let ops = k.get(*p..*p + (prev.len() * bits).div_ceil(8))?;
        *p += ops.len();
        let mut moved = false;
        for (i, &id) in prev.iter().enumerate() {
            match (ops[i * bits / 8] >> (i * bits % 8)) & ((1 << bits) - 1) {
                0 => out.push(id),
                2 => {
                    out.push(id ^ FLAG);
                    moved = true;
                }
                _ => {}
            }
        }
        let adds = varint(k, p)?;
        if out.len() as u64 > LIST_MAX || adds > LIST_MAX - out.len() as u64 {
            return None;
        }
        let mut t = 0u64;
        for j in 0..adds {
            let gap = varint(k, p)?;
            t = if j == 0 { gap } else { t.wrapping_add(gap).wrapping_add(1) };
            let t = t as u32;
            out.push(if groups { t >> 1 | t << 31 } else { t });
        }
        if adds > 0 || moved {
            out.sort_unstable();
        }
        Some(())
    }
}

/// The index of (cell, id) pairs at the default admin cell level, as the builder writes it.
#[cfg(any(test, feature = "fixtures"))]
pub fn from_pairs(pairs: impl IntoIterator<Item = (u64, u32)>, format: Format) -> CellPairs {
    let (cells, entries) = write_pairs(pairs, format);
    CellPairs::open(FileBytes::Owned(cells), FileBytes::Owned(entries), format, crate::DEFAULT_ADMIN_CELL_LEVEL).unwrap()
}

/// The files the builder writes for (cell, id) pairs: each cell's ids, sorted.
#[cfg(any(test, feature = "fixtures"))]
pub fn write_pairs(pairs: impl IntoIterator<Item = (u64, u32)>, format: Format) -> (Vec<u8>, Vec<u8>) {
    let mut lists: std::collections::BTreeMap<u64, Vec<u32>> = Default::default();
    for (cell, id) in pairs {
        lists.entry(cell).or_default().push(id);
    }
    write(&lists.into_iter().collect::<Vec<_>>(), format)
}

/// The files the builder writes for `lists`, each list sorted first (the builder sorts them):
/// the canonical encoding builder/src/cell_pairs.h's Writer gives, for the tests of this crate
/// and others. The cells ascend, at one level.
#[cfg(any(test, feature = "fixtures"))]
pub fn write(lists: &[(u64, Vec<u32>)], format: Format) -> (Vec<u8>, Vec<u8>) {
    let sorted: Vec<(u64, Vec<u32>)> = lists
        .iter()
        .map(|(cell, ids)| {
            let mut ids = ids.clone();
            ids.sort_unstable();
            (*cell, ids)
        })
        .collect();
    assert!(sorted.windows(2).all(|w| w[0].0 < w[1].0), "cells out of order");
    let lists = &sorted[..];
    fn put_varint(o: &mut Vec<u8>, mut v: u64) {
        while v >= 0x80 {
            o.push(v as u8 | 0x80);
            v >>= 7;
        }
        o.push(v as u8);
    }
    fn zigzag(v: i64) -> u64 {
        if v >= 0 { (v as u64) << 1 } else { ((-(v + 1)) as u64) << 1 | 1 }
    }
    fn group0(l: &[u32], groups: bool) -> usize {
        if groups { l.partition_point(|&id| id & FLAG == 0) } else { l.len() }
    }
    fn repeats(l: &[u32]) -> bool {
        l.windows(2).any(|w| w[0] == w[1])
    }
    fn plain(o: &mut Vec<u8>, l: &[u32], format: Format, lf: Option<[u32; 2]>) {
        let rep = repeats(l);
        let n0 = group0(l, format.groups);
        put_varint(o, (n0 as u64) << 1 | rep as u64);
        if format.groups {
            put_varint(o, (l.len() - n0) as u64);
        }
        for (g, group) in [&l[..n0], &l[n0..]].into_iter().enumerate() {
            let Some(&first) = group.first() else { continue };
            let first = if format.groups { first & BARE } else { first };
            match lf {
                Some(lf) => put_varint(o, zigzag(first as i64 - lf[g] as i64)),
                None => put_varint(o, first as u64),
            }
            let gaps: Vec<u32> = group.windows(2).map(|w| w[1] - w[0] - !rep as u32).collect();
            match format.gaps {
                Gaps::Varint => gaps.iter().for_each(|&gap| put_varint(o, gap as u64)),
                Gaps::GroupVarint => {
                    for quad in gaps.chunks(4) {
                        let at = o.len();
                        o.push(0);
                        for (q, &gap) in quad.iter().enumerate() {
                            let bytes = 1 + (gap >= 1 << 8) as usize + (gap >= 1 << 16) as usize + (gap >= 1 << 24) as usize;
                            o[at] |= ((bytes - 1) << (2 * q)) as u8;
                            o.extend_from_slice(&gap.to_le_bytes()[..bytes]);
                        }
                    }
                }
            }
        }
    }
    fn edit(o: &mut Vec<u8>, prev: &[u32], l: &[u32], groups: bool) {
        let bits = if groups { 2 } else { 1 };
        let at = o.len();
        o.resize(at + (prev.len() * bits).div_ceil(8), 0);
        let mut used = vec![false; l.len()];
        for (i, &id) in prev.iter().enumerate() {
            let op = if let Ok(j) = l.binary_search(&id) {
                used[j] = true;
                0
            } else if let (true, Ok(j)) = (groups, l.binary_search(&(id ^ FLAG))) {
                if prev.binary_search(&(id ^ FLAG)).is_err() {
                    used[j] = true;
                    2
                } else {
                    1
                }
            } else {
                1
            };
            o[at + i * bits / 8] |= (op << (i * bits % 8)) as u8;
        }
        let mut adds: Vec<u32> = l
            .iter()
            .zip(&used)
            .filter(|(_, &u)| !u)
            .map(|(&id, _)| if groups { (id & BARE) << 1 | id >> 31 } else { id })
            .collect();
        adds.sort_unstable();
        put_varint(o, adds.len() as u64);
        for (j, &a) in adds.iter().enumerate() {
            put_varint(o, if j == 0 { a as u64 } else { (a - adds[j - 1] - 1) as u64 });
        }
    }
    fn firsts(l: &[u32], groups: bool, lf: &mut [u32; 2]) {
        let n0 = group0(l, groups);
        if n0 > 0 {
            lf[0] = if groups { l[0] & BARE } else { l[0] };
        }
        if n0 < l.len() {
            lf[1] = l[n0] & BARE;
        }
    }

    let (mut cells, mut entries) = (Vec::new(), Vec::new());
    let Some(&(first, _)) = lists.first() else { return (cells, entries) };
    let level = 30 - first.trailing_zeros() as u64 / 2;
    let shift = 61 - 2 * level as u32;
    cells.extend_from_slice(MAGIC);
    cells.extend_from_slice(&[format.kind as u8, format.flags(), level as u8, 0]);
    let mut pbits = vec![0u64; parent_words_at(level)];
    let mut dir = Vec::new();
    let mut prev: Option<(&Vec<u32>, u32)> = None; // direct: the row before and its offset
    let mut i = 0;
    while i < lists.len() {
        let parent = (lists[i].0 >> shift) >> (2 * LEAF_LEVELS);
        let end = i + lists[i..].iter().take_while(|(c, _)| (c >> shift) >> (2 * LEAF_LEVELS) == parent).count();
        pbits[parent as usize / 64] |= 1 << (parent % 64);
        dir.push(cells.len() as u32);
        cells.extend((parent as u32).to_le_bytes());
        let mut leaf = [0u64; LEAF_WORDS];
        for (c, _) in &lists[i..end] {
            let child = ((c >> shift) & ((1 << (2 * LEAF_LEVELS)) - 1)) as usize;
            leaf[child / 64] |= 1 << (child % 64);
        }
        leaf.iter().for_each(|w| cells.extend(w.to_le_bytes()));
        match format.kind {
            Kind::Blocks => {
                for block in lists[i..end].chunks(BLOCK_ROWS) {
                    cells.extend((entries.len() as u32).to_le_bytes());
                    let mut out = vec![0u8; 2];
                    let mut codes = 0u16;
                    let mut lf = [0u32; 2];
                    for (k, (_, l)) in block.iter().enumerate() {
                        let code = if k > 0 && *l == block[k - 1].1 {
                            1
                        } else {
                            let mut a = Vec::new();
                            plain(&mut a, l, format, Some(lf));
                            let mut code = 0;
                            if k > 0 && !repeats(l) && !repeats(&block[k - 1].1) {
                                let mut e = Vec::new();
                                edit(&mut e, &block[k - 1].1, l, format.groups);
                                if e.len() < a.len() {
                                    a = e;
                                    code = 2;
                                }
                            }
                            out.extend(a);
                            code
                        };
                        codes |= code << (2 * k);
                        firsts(l, format.groups, &mut lf);
                    }
                    out[..2].copy_from_slice(&codes.to_le_bytes());
                    entries.extend(out);
                }
            }
            Kind::Direct => {
                for (_, l) in &lists[i..end] {
                    if let Some((p, off)) = prev.filter(|(p, _)| *p == l) {
                        let _ = p;
                        cells.extend(off.to_le_bytes());
                        continue;
                    }
                    let off = entries.len() as u32;
                    cells.extend(off.to_le_bytes());
                    plain(&mut entries, l, format, None);
                    prev = Some((l, off));
                }
            }
        }
        i = end;
    }
    cells.extend(END_OF_RECORDS.to_le_bytes());
    let words = pbits.iter().rposition(|&w| w != 0).unwrap() + 1;
    let mut before = 0u32;
    let ranks: Vec<u32> = pbits[..words]
        .iter()
        .map(|w| {
            let r = before;
            before += w.count_ones();
            r
        })
        .collect();
    pbits[..words].iter().for_each(|w| cells.extend(w.to_le_bytes()));
    ranks.iter().for_each(|r| cells.extend(r.to_le_bytes()));
    dir.iter().for_each(|d| cells.extend(d.to_le_bytes()));
    cells.extend((dir.len() as u32).to_le_bytes());
    cells.extend((words as u32).to_le_bytes());
    (cells, entries)
}

#[cfg(test)]
mod tests;
