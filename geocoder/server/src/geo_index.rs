//! The geo index ("GXT2"): each street-level cell's lists of street ways, address points and
//! interpolation ways, in the top file (geo_cells.bin) and the blocks file (geo_entries.bin) the
//! builder writes (builder/src/geo_index.h, which lays the format out). A lookup finds a cell's
//! group by a binary search of the top file, its block by one of the group's footer, and walks
//! the block's rows forward to the cell, stepping over the lists before it without decoding
//! them. A query asks for its cell and the cells around it at once: `lookup_many` sorts them and
//! walks each block they share once.

use crate::FileBytes;
use std::sync::atomic::{AtomicU64, Ordering};

const MAGIC: &[u8; 4] = b"GXT2";
const HEADER_BYTES: usize = 8;
const FOOTER_BYTES: usize = 8;
const ENTRY_BYTES: usize = 12;
const BLOCK_ROWS: u64 = 64;
const GROUP_BLOCKS: u64 = 64;
const LIST_MAX: u64 = 0xFFFF;
const MIN_LEVEL: u64 = 1;
const MAX_LEVEL: u64 = 30;
const PLAIN: u8 = 1;
const SAME: u8 = 2;
const EDIT: u8 = 3;

/// The tables, by their bit in a lookup's `tables` mask and their index in its lists.
pub const STREETS: usize = 0;
pub const ADDRESSES: usize = 1;
pub const INTERPOLATIONS: usize = 2;
pub const TABLES: usize = 3;
pub const ALL_TABLES: u8 = 0b111;

/// A geo index's two files, checked at open.
pub struct GeoIndex {
    top: FileBytes,
    blocks: FileBytes,
    /// None: an index of no rows (both files empty).
    layout: Option<Layout>,
    /// This index among those the process opened: a GeoLists names the index it holds the lists of.
    id: u64,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy)]
struct Layout {
    /// A cell's number at the index's level is the cell id shifted right this far.
    shift: u32,
    rows: u64,
    groups: usize,
    blocks: usize,
}

/// A lookup's lists: for each cell asked, in the order asked, its lists of the tables asked
/// (an empty one: none, or the cell has no row). A lookup of the cells the last one asked for,
/// in the same index, for tables it decoded, takes their lists as they are: the server's refines
/// after query_geo ask for its cells again.
#[derive(Default)]
pub struct GeoLists {
    lists: Vec<[Vec<u32>; TABLES]>,
    /// The tables the last lookup asked for, and the index, cells and tables its lists are of.
    asked: u8,
    held: Option<(u64, u8)>,
    held_cells: Vec<u64>,
    /// The walk's state of each table.
    state: [TableState; TABLES],
    /// A cell's position in the sorted order, and two scratch lists.
    order: Vec<usize>,
    tmp: Vec<u32>,
    prev: Vec<u32>,
}

impl GeoLists {
    /// Cell i's list of table t (empty for a table the lookup didn't ask for).
    pub fn list(&self, i: usize, t: usize) -> &[u32] {
        if self.asked >> t & 1 == 0 { &[] } else { &self.lists[i][t] }
    }
}

/// Where a table's list in effect stands in a block: its last PLAIN list (at `base`, coded
/// against `base_lf`) and the EDIT rows since, the last first and the list's length.
#[derive(Default, Clone)]
struct TableState {
    have: bool,
    base: usize,
    base_lf: u32,
    edits: Vec<usize>,
    len: u64,
    lf: u32,
}

#[inline(always)]
fn rd32(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?))
}
#[inline(always)]
fn rd64(b: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?))
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

/// Past `n` varints from `*p`.
#[inline(always)]
fn skip_varints(k: &[u8], p: &mut usize, mut n: u64) -> Option<()> {
    while n > 0 {
        let rest = k.get(*p..)?;
        // Eight bytes at a time: each varint ends at a byte below 0x80.
        if rest.len() >= 8 && n >= 8 {
            let ends = (!u64::from_le_bytes(rest[..8].try_into().unwrap()) & 0x8080_8080_8080_8080).count_ones() as u64;
            if ends <= n {
                n -= ends;
                *p += 8;
                continue;
            }
        }
        let b = *rest.first()?;
        *p += 1;
        if b < 0x80 {
            n -= 1;
        }
    }
    Some(())
}

#[inline(always)]
fn unzigzag(v: u64) -> i64 {
    if v & 1 != 0 { -((v >> 1) as i64) - 1 } else { (v >> 1) as i64 }
}

/// `lf` moved by a coded first (zigzag of the gap from it), None past the u32 range.
#[inline(always)]
fn first_from(lf: u32, coded: u64) -> Option<u32> {
    if coded >> 34 != 0 {
        return None;
    }
    u32::try_from(lf as i64 + unzigzag(coded)).ok()
}

/// The PLAIN list at `*p`, coded against the last first `lf`, into `out`.
fn plain(k: &[u8], p: &mut usize, lf: u32, out: &mut Vec<u32>) -> Option<()> {
    let h = varint(k, p)?;
    let n = h >> 1;
    if n == 0 || n > LIST_MAX {
        return None;
    }
    let inc = 1 - (h & 1);
    let mut v = first_from(lf, varint(k, p)?)? as u64;
    out.clear();
    out.reserve(n as usize);
    out.push(v as u32);
    for _ in 1..n {
        v = v.checked_add(varint(k, p)?)?.checked_add(inc)?;
        if v > u32::MAX as u64 {
            return None;
        }
        out.push(v as u32);
    }
    Some(())
}

/// The EDIT at `*p` of `prev` (coded against the last first `lf`) into `out`.
fn edit(k: &[u8], p: &mut usize, lf: u32, prev: &[u32], out: &mut Vec<u32>) -> Option<()> {
    let ops = k.get(*p..*p + prev.len().div_ceil(8))?;
    *p += ops.len();
    out.clear();
    for (i, &id) in prev.iter().enumerate() {
        if (ops[i / 8] >> (i % 8)) & 1 == 0 {
            out.push(id);
        }
    }
    let kept = out.len();
    let adds = varint(k, p)?;
    if adds > LIST_MAX - kept as u64 {
        return None;
    }
    let mut v = 0u64;
    for j in 0..adds {
        let coded = varint(k, p)?;
        v = if j == 0 { first_from(lf, coded)? as u64 } else { v.checked_add(coded)?.checked_add(1)? };
        if v > u32::MAX as u64 {
            return None;
        }
        out.push(v as u32);
    }
    if kept > 0 && adds > 0 && out[kept - 1] > out[kept] {
        out.sort_unstable();
    }
    Some(())
}

/// How a list's payload stepped over at `*p` moves its table: a PLAIN list's length and coded
/// first, or an EDIT's drops and adds.
enum Step {
    None,
    Plain(u64, u64),
    Edit(u64, u64),
}

/// Steps over a list of `code` at `*p` without decoding its ids; `len` is the table's list
/// length before the row (an EDIT's ops are a bit per id of it).
#[inline(always)]
fn step_over(k: &[u8], p: &mut usize, code: u8, len: u64) -> Option<Step> {
    match code {
        PLAIN => {
            let n = varint(k, p)? >> 1;
            if n == 0 {
                return None;
            }
            let first = varint(k, p)?;
            skip_varints(k, p, n - 1)?;
            Some(Step::Plain(n, first))
        }
        EDIT => {
            let ops = k.get(*p..)?.get(..usize::try_from(len.div_ceil(8)).ok()?)?;
            *p += ops.len();
            let drops: u64 = ops.iter().map(|b| b.count_ones() as u64).sum();
            let adds = varint(k, p)?;
            skip_varints(k, p, adds)?;
            Some(Step::Edit(drops, adds))
        }
        _ => Some(Step::None),
    }
}

impl TableState {
    fn reset(&mut self) {
        self.have = false;
        self.edits.clear();
        self.len = 0;
        self.lf = 0;
    }

    /// Past the table's list of `code` at `*p` in the next row.
    #[inline(always)]
    fn step(&mut self, k: &[u8], p: &mut usize, code: u8) -> Option<()> {
        if code == 0 {
            return Some(());
        }
        if code != PLAIN && !self.have {
            return None;
        }
        let at = *p;
        match step_over(k, p, code, self.len)? {
            Step::Plain(n, first) => {
                self.base = at;
                self.base_lf = self.lf;
                self.lf = first_from(self.lf, first)?;
                self.edits.clear();
                self.len = n;
            }
            Step::Edit(drops, adds) => {
                self.edits.push(at);
                self.len = self.len.saturating_sub(drops).saturating_add(adds);
            }
            Step::None => {}
        }
        self.have = true;
        Some(())
    }

    /// The table's list in effect (its last PLAIN list and the EDITs since) into `out`.
    fn current(&self, k: &[u8], out: &mut Vec<u32>, tmp: &mut Vec<u32>) -> Option<()> {
        let mut p = self.base;
        plain(k, &mut p, self.base_lf, out)?;
        for &at in &self.edits {
            let mut p = at;
            std::mem::swap(out, tmp);
            edit(k, &mut p, self.lf, tmp, out)?;
        }
        Some(())
    }
}

impl GeoIndex {
    /// The index at `level` in these files; an error names what is wrong with them.
    pub fn open(top: FileBytes, blocks: FileBytes, level: u64) -> Result<GeoIndex, String> {
        let layout = Self::check(top.as_slice(), blocks.as_slice(), level)?;
        Ok(GeoIndex { top, blocks, layout, id: NEXT_ID.fetch_add(1, Ordering::Relaxed) })
    }

    /// An index of no rows.
    pub fn none() -> GeoIndex {
        GeoIndex { top: FileBytes::Owned(Vec::new()), blocks: FileBytes::Owned(Vec::new()), layout: None,
                   id: NEXT_ID.fetch_add(1, Ordering::Relaxed) }
    }

    fn check(t: &[u8], b: &[u8], level: u64) -> Result<Option<Layout>, String> {
        if t.is_empty() {
            return if b.is_empty() { Ok(None) } else { Err("blocks without a top file".into()) };
        }
        let bad = |what: &str| Err(format!("not a geo index ({what})"));
        if t.len() < HEADER_BYTES + ENTRY_BYTES + FOOTER_BYTES || &t[0..4] != MAGIC {
            return bad("no GXT2 header");
        }
        if t[5] as u64 != BLOCK_ROWS || t[6] as u64 != GROUP_BLOCKS || t[7] != 0 {
            return bad("another layout of blocks");
        }
        let file_level = t[4] as u64;
        if !(MIN_LEVEL..=MAX_LEVEL).contains(&file_level) {
            return bad("a level past the levels it takes");
        }
        if file_level != level {
            return Err(format!("an index of level {file_level} cells, read at level {level}"));
        }
        let rows = rd64(t, t.len() - FOOTER_BYTES).unwrap();
        let blocks = rows.div_ceil(BLOCK_ROWS);
        let groups = blocks.div_ceil(GROUP_BLOCKS);
        if rows == 0 || (t.len() - HEADER_BYTES - FOOTER_BYTES) as u64 != groups.saturating_mul(ENTRY_BYTES as u64) {
            return bad("a footer that isn't its groups'");
        }
        let last_at = rd32(t, HEADER_BYTES + (groups as usize - 1) * ENTRY_BYTES + 8).unwrap() as usize;
        let last_blocks = (blocks - (groups - 1) * GROUP_BLOCKS) as usize;
        if last_at + last_blocks * ENTRY_BYTES != b.len() {
            return bad("a last footer that doesn't end the blocks file");
        }
        Ok(Some(Layout { shift: 61 - 2 * level as u32, rows, groups: groups as usize, blocks: blocks as usize }))
    }

    /// Whether the index has no rows.
    pub fn is_empty(&self) -> bool {
        self.layout.is_none()
    }

    /// Group g's first cell and where its footer starts.
    #[inline(always)]
    fn group(&self, g: usize) -> Option<(u64, usize)> {
        let t = self.top.as_slice();
        let at = HEADER_BYTES + g * ENTRY_BYTES;
        Some((rd64(t, at)?, rd32(t, at + 8)? as usize))
    }

    /// The block holding `cell` if any does: its first cell number, bytes [begin, end) and rows,
    /// and the first cell number of the block after it (past any cell after the last block).
    fn block_of(&self, l: &Layout, cell: u64) -> Option<BlockAt> {
        let (mut lo, mut hi) = (0, l.groups);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.group(mid)?.0 <= cell { lo = mid + 1 } else { hi = mid }
        }
        let g = lo.checked_sub(1)?;
        let (_, footer) = self.group(g)?;
        let first = g * GROUP_BLOCKS as usize;
        let n = (l.blocks - first).min(GROUP_BLOCKS as usize);
        let k = self.blocks.as_slice();
        let entry = |j: usize| -> Option<(u64, usize)> {
            let at = footer + j * ENTRY_BYTES;
            Some((rd64(k, at)?, rd32(k, at + 8)? as usize))
        };
        let (mut a, mut z) = (0, n);
        while a < z {
            let mid = (a + z) / 2;
            if entry(mid)?.0 <= cell { a = mid + 1 } else { z = mid }
        }
        let j = a.checked_sub(1)?;
        let (first_cell, begin) = entry(j)?;
        let (end, next) = if j + 1 < n {
            let (c, at) = entry(j + 1)?;
            (at, c)
        } else if g + 1 < l.groups {
            (footer, self.group(g + 1)?.0)
        } else {
            (footer, u64::MAX)
        };
        if begin >= end || end > k.len() {
            return None;
        }
        let b = first + j;
        let rows = if b + 1 < l.blocks { BLOCK_ROWS } else { l.rows - (l.blocks as u64 - 1) * BLOCK_ROWS };
        Some(BlockAt { first: first_cell >> l.shift, begin, end, rows, next: next >> l.shift })
    }

    /// The lists of `tables` (a mask of 1 << STREETS, 1 << ADDRESSES and 1 << INTERPOLATIONS) of
    /// each of `cells` into `out`, in their order. Cells of another level, or without a row, have
    /// none. A block two of them share is walked once, front to back.
    pub fn lookup_many(&self, cells: &[u64], tables: u8, out: &mut GeoLists) {
        out.asked = tables;
        if matches!(out.held, Some((id, held)) if id == self.id && tables & !held == 0) && out.held_cells == cells {
            return;
        }
        out.held = Some((self.id, tables));
        out.held_cells.clear();
        out.held_cells.extend_from_slice(cells);
        if out.lists.len() < cells.len() {
            out.lists.resize_with(cells.len(), Default::default);
        }
        for l in &mut out.lists[..cells.len()] {
            l.iter_mut().for_each(Vec::clear);
        }
        let Some(l) = self.layout else { return };
        out.order.clear();
        out.order.extend(0..cells.len());
        out.order.sort_unstable_by_key(|&i| cells[i]);
        let k = self.blocks.as_slice();
        let mut walk: Option<Walk> = None;
        for oi in 0..cells.len() {
            let i = out.order[oi];
            let cell = cells[i];
            if cell & ((1u64 << l.shift) - 1) != 1u64 << (l.shift - 1) {
                continue;
            }
            let target = cell >> l.shift;
            match &walk {
                // In the block the walk is in: past the cell before it (cells come sorted), and no row
                // lies between that cell and the walk's.
                Some(w) if target >= w.block.first && target < w.block.next => {
                    if target < w.number {
                        continue;
                    }
                }
                _ => {
                    let Some(block) = self.block_of(&l, cell) else {
                        walk = None;
                        continue;
                    };
                    out.state.iter_mut().for_each(TableState::reset);
                    walk = Some(Walk { block, p: block.begin, row: 0, number: block.first, header: None });
                }
            }
            let w = walk.as_mut().unwrap();
            if Self::walk_to(k, w, target, &mut out.state).is_none() {
                walk = None;
                continue;
            }
            if let (true, Some(header)) = (w.number == target, w.header) {
                let GeoLists { lists, state, tmp, prev, .. } = &mut *out;
                if Self::take(k, w.p, header, state, tables, &mut lists[i], tmp, prev).is_none() {
                    lists[i].iter_mut().for_each(Vec::clear);
                }
            }
        }
    }

    /// One cell's lists of `tables` into `out` (its list(0, t)).
    pub fn lookup(&self, cell: u64, tables: u8, out: &mut GeoLists) {
        self.lookup_many(&[cell], tables, out)
    }

    /// Walks forward to the first row at or past `target`, its header read and its lists not:
    /// None when the block's bytes run out or are malformed. A row past the block's last leaves
    /// the walk with no header.
    #[inline(always)]
    fn walk_to(k: &[u8], w: &mut Walk, target: u64, state: &mut [TableState; TABLES]) -> Option<()> {
        let k = &k[..w.block.end];
        loop {
            if w.header.is_none() {
                if w.row == w.block.rows {
                    return Some(());
                }
                let h = *k.get(w.p)?;
                w.p += 1;
                if w.row > 0 {
                    let d = (h >> 6) as u64;
                    let step = if d < 3 { d + 1 } else { varint(k, &mut w.p)?.checked_add(4)? };
                    w.number = w.number.checked_add(step)?;
                } else if h >> 6 != 0 {
                    return None;
                }
                w.header = Some(h);
            }
            if w.number >= target {
                return Some(());
            }
            let h = w.header.take().unwrap();
            for (t, s) in state.iter_mut().enumerate() {
                s.step(k, &mut w.p, (h >> (2 * t)) & 3)?;
            }
            w.row += 1;
        }
    }

    /// The row's lists of `tables` (its header read, its lists at p) into `lists`.
    #[allow(clippy::too_many_arguments)]
    fn take(k: &[u8], mut p: usize, header: u8, state: &[TableState; TABLES], tables: u8, lists: &mut [Vec<u32>; TABLES],
            tmp: &mut Vec<u32>, prev: &mut Vec<u32>) -> Option<()> {
        for t in 0..TABLES {
            let code = (header >> (2 * t)) & 3;
            let s = &state[t];
            if code != 0 && code != PLAIN && !s.have {
                return None;
            }
            if tables >> t & 1 == 0 || code == 0 {
                step_over(k, &mut p, code, s.len)?;
                continue;
            }
            let out = &mut lists[t];
            match code {
                PLAIN => plain(k, &mut p, s.lf, out)?,
                SAME => s.current(k, out, tmp)?,
                _ => {
                    s.current(k, prev, tmp)?;
                    edit(k, &mut p, s.lf, prev, out)?;
                }
            }
        }
        Some(())
    }
}

#[derive(Clone, Copy)]
struct BlockAt {
    first: u64,
    begin: usize,
    end: usize,
    rows: u64,
    next: u64,
}

/// A walk through a block: where it is (the row's lists, or its header when `header` is
/// None), the row and its cell number.
struct Walk {
    block: BlockAt,
    p: usize,
    row: u64,
    number: u64,
    header: Option<u8>,
}

/// The files of an index of `rows` (cells ascending at one level, each a list per table, an
/// empty one none, at least one not), as the builder's geo_index::Writer writes them: the
/// server's tests and those of the crates that read a geo index.
#[cfg(any(test, feature = "fixtures"))]
pub fn write(rows: &[(u64, [Vec<u32>; TABLES])]) -> (Vec<u8>, Vec<u8>) {
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
    fn repeats(l: &[u32]) -> bool {
        l.windows(2).any(|w| w[0] == w[1])
    }
    fn plain(o: &mut Vec<u8>, l: &[u32], lf: u32) {
        let rep = repeats(l);
        put_varint(o, (l.len() as u64) << 1 | rep as u64);
        put_varint(o, zigzag(l[0] as i64 - lf as i64));
        for w in l.windows(2) {
            put_varint(o, (w[1] - w[0]) as u64 - (!rep) as u64);
        }
    }
    fn edit(o: &mut Vec<u8>, prev: &[u32], l: &[u32], lf: u32) {
        let mut ops = vec![0u8; prev.len().div_ceil(8)];
        for (i, id) in prev.iter().enumerate() {
            if l.binary_search(id).is_err() {
                ops[i / 8] |= 1 << (i % 8);
            }
        }
        o.extend_from_slice(&ops);
        let adds: Vec<u32> = l.iter().copied().filter(|id| prev.binary_search(id).is_err()).collect();
        put_varint(o, adds.len() as u64);
        for (j, &a) in adds.iter().enumerate() {
            put_varint(o, if j == 0 { zigzag(a as i64 - lf as i64) } else { (a - adds[j - 1] - 1) as u64 });
        }
    }
    let (mut top, mut blocks) = (Vec::new(), Vec::new());
    if rows.is_empty() {
        return (top, blocks);
    }
    let level = 30 - rows[0].0.trailing_zeros() as u64 / 2;
    let shift = 61 - 2 * level;
    top.extend_from_slice(&[b'G', b'X', b'T', b'2', level as u8, BLOCK_ROWS as u8, GROUP_BLOCKS as u8, 0]);
    let mut footer: Vec<u8> = Vec::new();
    let mut group_first = 0u64;
    let end_group = |top: &mut Vec<u8>, blocks: &mut Vec<u8>, footer: &mut Vec<u8>, group_first: u64| {
        top.extend_from_slice(&group_first.to_le_bytes());
        top.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
        blocks.append(footer);
    };
    let (mut prev, mut have, mut lf) = (<[Vec<u32>; TABLES]>::default(), [false; TABLES], [0u32; TABLES]);
    let mut last = 0u64;
    for (r, (cell, lists)) in rows.iter().enumerate() {
        let number = cell >> shift;
        if r as u64 % BLOCK_ROWS == 0 {
            let b = r as u64 / BLOCK_ROWS;
            if b > 0 && b % GROUP_BLOCKS == 0 {
                end_group(&mut top, &mut blocks, &mut footer, group_first);
            }
            if b % GROUP_BLOCKS == 0 {
                group_first = *cell;
            }
            footer.extend_from_slice(&cell.to_le_bytes());
            footer.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
            have = [false; TABLES];
            lf = [0; TABLES];
        }
        let at = blocks.len();
        blocks.push(0);
        let mut header = 0u8;
        if r as u64 % BLOCK_ROWS != 0 {
            let step = number - last;
            let d = if step <= 3 { step - 1 } else { 3 };
            if d == 3 {
                put_varint(&mut blocks, step - 4);
            }
            header |= (d as u8) << 6;
        }
        for t in 0..TABLES {
            let l = &lists[t];
            if l.is_empty() {
                continue;
            }
            let prior = have[t] && t != ADDRESSES;
            let code = if prior && prev[t] == *l {
                SAME
            } else {
                let mut p = Vec::new();
                plain(&mut p, l, lf[t]);
                let mut e = Vec::new();
                if prior && !repeats(l) && !repeats(&prev[t]) {
                    edit(&mut e, &prev[t], l, lf[t]);
                }
                if !e.is_empty() && e.len() < p.len() {
                    blocks.extend_from_slice(&e);
                    EDIT
                } else {
                    blocks.extend_from_slice(&p);
                    lf[t] = l[0];
                    PLAIN
                }
            };
            header |= code << (2 * t);
            prev[t] = l.clone();
            have[t] = true;
        }
        blocks[at] = header;
        last = number;
    }
    end_group(&mut top, &mut blocks, &mut footer, group_first);
    top.extend_from_slice(&(rows.len() as u64).to_le_bytes());
    (top, blocks)
}

#[cfg(test)]
mod tests;
