//! The blocks of the arc files, as the builder writes them (builder/src/arc_blocks.h).
//! Integers are little-endian, varints LEB128, and signed values go through zigzag.
//!
//! `arc_vertices.bin` holds one arc block per arc slot, at the u32 offset `arc_index.bin` gives
//! the slot; a block runs to the next slot's offset, the last to the end of the file, and a dead
//! slot's is empty. An arc block is a flags byte (bits 0-1 the grid exponent k: coordinates are
//! in units of 10^k e7; bit 7 a box follows), the first vertex (i32 latitude, i32 longitude), the
//! box of its vertices (i32 min longitude, max longitude, max latitude) when bit 7 says so, then
//! each later vertex as varint zz(Δlat), varint zz(Δlng).
//!
//! `admin_rings.bin` holds one ring block per polygon slot: its rings, the outer ring first, each
//! one or more chunks of refs to arc slots. A chunk is a head varint (refs << 2 | has_box << 1 |
//! last_in_ring); with a box, the i32 min longitude, max longitude and max latitude in e7 units
//! of its arcs' raw vertices and a varint of the bytes of refs that follow; then each ref as
//! varint(zz(slot - the slot before it in the chunk, 0 for the first) << 1 | reversed).
//!
//! Every read here is bounds-checked: a truncated or overlong varint, an offset past the end of
//! a file, a malformed head or a coordinate past i32 in e7 units (which the builder's decoder
//! rejects too) reads as None, never a panic.

use crate::exact_pip::{edge_crosses, PointE7};

/// How far, per axis in e7 units, a vertex an arc block stores may lie from the raw vertex a
/// chunk's box spans: half the coarsest grid a tier may store (builder kChunkBoxSlackE7).
pub(crate) const CHUNK_BOX_SLACK_E7: i64 = 500;
/// The most refs a chunk holds (builder kRefChunkMax).
pub(crate) const REF_CHUNK_MAX: u64 = 16;
/// An arc block's flags: the grid exponent k, and that a box follows the first vertex.
pub(crate) const GRID_MASK: u8 = 0x03;
pub(crate) const HAS_BOX: u8 = 0x80;
/// 10^k for each grid exponent k.
const GRID_STEPS: [i64; 4] = [1, 10, 100, 1000];

/// The LEB128 varint at `*at`, which it moves past it; None for one running past the end of
/// `bytes` or past 64 bits.
#[inline]
pub(crate) fn varint(bytes: &[u8], at: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(*at)?;
        *at += 1;
        if shift == 63 && byte > 1 {
            return None;
        }
        v |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(v);
        }
    }
    None
}

#[inline]
pub(crate) fn unzigzag(z: u64) -> i64 {
    (z >> 1) as i64 ^ -((z & 1) as i64)
}

#[inline]
fn i32_at(bytes: &[u8], at: &mut usize) -> Option<i32> {
    let b = bytes.get(*at..at.checked_add(4)?)?;
    *at += 4;
    Some(i32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

/// A chunk's box: the extent in e7 units of its arcs' raw vertices along the three sides a north
/// ray's skip reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ChunkBox {
    pub min_lng: i32,
    pub max_lng: i32,
    pub max_lat: i32,
}

impl ChunkBox {
    /// Whether no edge of the chunk's arcs can cross the ray from `p` towards +lat: every stored
    /// vertex lies within CHUNK_BOX_SLACK_E7 of the box, so past the box and its slack in
    /// longitude no edge straddles p, and north of it none passes north of p.
    #[inline]
    pub fn excludes(&self, p: PointE7) -> bool {
        p.lng < self.min_lng as i64 - CHUNK_BOX_SLACK_E7
            || p.lng >= self.max_lng as i64 + CHUNK_BOX_SLACK_E7
            || p.lat >= self.max_lat as i64 + CHUNK_BOX_SLACK_E7
    }

    /// Whether, besides `excludes`, `p` is on no edge of the chunk's arcs.
    #[inline]
    pub fn excludes_on_edge(&self, p: PointE7) -> bool {
        p.lng < self.min_lng as i64 - CHUNK_BOX_SLACK_E7
            || p.lng > self.max_lng as i64 + CHUNK_BOX_SLACK_E7
            || p.lat > self.max_lat as i64 + CHUNK_BOX_SLACK_E7
    }
}

/// A ring's ref to an arc slot; the ring walks the arc backwards when `reversed`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ArcRef {
    pub slot: u32,
    pub reversed: bool,
}

/// What `walk_rings` does with a ring block's chunks and refs.
pub(crate) trait RingVisitor {
    /// Whether to read the refs of a chunk whose arcs `bbox` spans; a chunk without a box is
    /// always read.
    fn wants_chunk(&mut self, bbox: &ChunkBox) -> bool;
    /// The next ref, in ring order; None stops the walk as malformed.
    fn arc(&mut self, r: ArcRef) -> Option<()>;
    /// The ring of the refs since the last ring end ends here.
    fn ring_end(&mut self) -> Option<()> {
        Some(())
    }
}

/// Walks a ring block's chunks in order, handing `v` the refs of each chunk it wants. None for a
/// malformed block: a truncated or overlong varint, a chunk of no refs or of more than
/// REF_CHUNK_MAX, a box whose byte count runs past the block or disagrees with its refs, a slot
/// outside u32, or a last chunk that ends no ring.
pub(crate) fn walk_rings(block: &[u8], v: &mut impl RingVisitor) -> Option<()> {
    let mut at = 0;
    let mut ring_open = false;
    while at < block.len() {
        let head = varint(block, &mut at)?;
        let refs = head >> 2;
        if refs == 0 || refs > REF_CHUNK_MAX {
            return None;
        }
        ring_open = true;
        if head & 2 != 0 {
            let bbox = ChunkBox { min_lng: i32_at(block, &mut at)?, max_lng: i32_at(block, &mut at)?, max_lat: i32_at(block, &mut at)? };
            let body = usize::try_from(varint(block, &mut at)?).ok()?;
            let end = at.checked_add(body).filter(|&end| end <= block.len())?;
            if v.wants_chunk(&bbox) {
                read_refs(&block[..end], &mut at, refs, v)?;
                if at != end {
                    return None;
                }
            }
            at = end;
        } else {
            read_refs(block, &mut at, refs, v)?;
        }
        if head & 1 != 0 {
            v.ring_end()?;
            ring_open = false;
        }
    }
    (!ring_open).then_some(())
}

fn read_refs(bytes: &[u8], at: &mut usize, refs: u64, v: &mut impl RingVisitor) -> Option<()> {
    let mut prev = 0i64;
    for _ in 0..refs {
        let z = varint(bytes, at)?;
        // |delta| < 2^62 and prev < 2^32: the sum can't overflow.
        let slot = u32::try_from(prev + unzigzag(z >> 1)).ok()?;
        prev = slot as i64;
        v.arc(ArcRef { slot, reversed: z & 1 != 0 })?;
    }
    Some(())
}

/// The arc files a set of ring blocks refers to.
#[derive(Clone, Copy)]
pub(crate) struct ArcFiles<'a> {
    /// arc_index.bin: a u32 offset into `vertices` per arc slot.
    pub index: &'a [u8],
    /// arc_vertices.bin: the arc blocks.
    pub vertices: &'a [u8],
}

impl<'a> ArcFiles<'a> {
    pub fn slots(&self) -> usize {
        self.index.len() / 4
    }

    fn offset(&self, slot: usize) -> Option<usize> {
        let at = slot.checked_mul(4)?;
        let b = self.index.get(at..at.checked_add(4)?)?;
        usize::try_from(u32::from_le_bytes([b[0], b[1], b[2], b[3]])).ok()
    }

    /// Arc slot `slot`'s block, from its offset to the next slot's (to the end of the file for
    /// the last slot); None for a slot past the index, a dead slot (an empty block) and a block
    /// past the end of the file.
    #[inline]
    pub fn block(&self, slot: u32) -> Option<&'a [u8]> {
        let slot = usize::try_from(slot).ok()?;
        let start = self.offset(slot)?;
        let end = if slot + 1 < self.slots() { self.offset(slot + 1)? } else { self.vertices.len() };
        self.vertices.get(start..end).filter(|block| !block.is_empty())
    }
}

/// An arc block's head: its grid step, its first vertex and its box (min lng, max lng, max lat)
/// in grid units, where its deltas start, and the grid units whose e7 value fits i32.
struct ArcHead {
    step: i64,
    lat: i64,
    lng: i64,
    bbox: Option<[i64; 3]>,
    deltas: usize,
    units: std::ops::RangeInclusive<i64>,
}

fn arc_head(block: &[u8]) -> Option<ArcHead> {
    let flags = *block.first()?;
    if flags & !(GRID_MASK | HAS_BOX) != 0 {
        return None;
    }
    let step = GRID_STEPS[(flags & GRID_MASK) as usize];
    let units = i32::MIN as i64 / step..=i32::MAX as i64 / step;
    let mut at = 1;
    let mut coordinate = || i32_at(block, &mut at).map(i64::from).filter(|c| units.contains(c));
    let (lat, lng) = (coordinate()?, coordinate()?);
    let bbox = if flags & HAS_BOX != 0 { Some([coordinate()?, coordinate()?, coordinate()?]) } else { None };
    Some(ArcHead { step, lat, lng, bbox, deltas: at, units })
}

/// The next vertex of an arc block in grid units, from the deltas at `*at`; None past i32 e7.
#[inline]
fn next_vertex(block: &[u8], at: &mut usize, head: &ArcHead, lat: i64, lng: i64) -> Option<(i64, i64)> {
    let lat = lat.checked_add(unzigzag(varint(block, at)?))?;
    let lng = lng.checked_add(unzigzag(varint(block, at)?))?;
    (head.units.contains(&lat) && head.units.contains(&lng)).then_some((lat, lng))
}

/// Calls `f` with each vertex of an arc block in e7 units, in the order it stores them; their
/// count, or None for a malformed block.
pub(crate) fn arc_vertices(block: &[u8], mut f: impl FnMut(PointE7)) -> Option<u32> {
    let head = arc_head(block)?;
    let e7 = |lat: i64, lng: i64| PointE7 { lat: lat * head.step, lng: lng * head.step };
    let (mut lat, mut lng) = (head.lat, head.lng);
    f(e7(lat, lng));
    let (mut at, mut n) = (head.deltas, 1u32);
    while at < block.len() {
        (lat, lng) = next_vertex(block, &mut at, &head, lat, lng)?;
        f(e7(lat, lng));
        n = n.wrapping_add(1);
    }
    Some(n)
}

/// What the ray from a point towards +lat does at one arc.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ArcCrossings {
    /// The arc's box shows the ray crosses none of its edges; no vertex was read.
    Boxed,
    /// The arc's vertices were read: whether the ray crosses its edges an odd number of times,
    /// and how many vertices it has.
    Read { odd: bool, vertices: u32 },
}

/// How the ray from `p` towards +lat crosses the edges of an arc block; None for a malformed
/// block. The straddle test compares grid units against p's longitude in grid units, so only
/// the edges that straddle p are taken to e7 for `edge_crosses`.
#[inline]
pub(crate) fn arc_crossings(block: &[u8], p: PointE7) -> Option<ArcCrossings> {
    let head = arc_head(block)?;
    let step = head.step;
    if let Some([min_lng, max_lng, max_lat]) = head.bbox {
        if p.lng < min_lng * step || p.lng >= max_lng * step || p.lat >= max_lat * step {
            return Some(ArcCrossings::Boxed);
        }
    }
    // A vertex is east of p exactly when its grid longitude is east of p's, floored to the grid.
    let p_lng = p.lng.div_euclid(step);
    let e7 = |lat: i64, lng: i64| PointE7 { lat: lat * step, lng: lng * step };
    let (mut lat, mut lng) = (head.lat, head.lng);
    let mut east = lng > p_lng;
    let (mut odd, mut vertices) = (false, 1u32);
    let mut at = head.deltas;
    while at < block.len() {
        let (next_lat, next_lng) = next_vertex(block, &mut at, &head, lat, lng)?;
        let next_east = next_lng > p_lng;
        if next_east != east && edge_crosses(e7(lat, lng), e7(next_lat, next_lng), p) {
            odd = !odd;
        }
        (lat, lng, east) = (next_lat, next_lng, next_east);
        vertices = vertices.wrapping_add(1);
    }
    Some(ArcCrossings::Read { odd, vertices })
}

/// The crossing parities of the arcs read at one point, so an arc several polygons share (a
/// border between levels) is read once per point: up to ARC_MEMO_CAPACITY arcs in a fixed table
/// of twice as many entries, open-addressed by slot, so that it is never more than half full and
/// a probe for an arc it doesn't hold ends at a free entry within a few steps. Once it holds
/// ARC_MEMO_CAPACITY arcs it remembers nothing more and later arcs are read again. It never
/// allocates.
pub struct ArcMemo {
    /// An arc slot per entry, FREE where none.
    slots: [u32; ARC_MEMO_ENTRIES],
    /// Bit i: the parity of the arc in slots[i].
    odd: u128,
    len: u32,
    arcs_read: u32,
    vertices_read: u64,
}

pub(crate) const ARC_MEMO_CAPACITY: usize = 64;
const ARC_MEMO_ENTRIES: usize = 2 * ARC_MEMO_CAPACITY;

impl Default for ArcMemo {
    fn default() -> Self {
        ArcMemo { slots: [ArcMemo::FREE; ARC_MEMO_ENTRIES], odd: 0, len: 0, arcs_read: 0, vertices_read: 0 }
    }
}

impl ArcMemo {
    /// Marks a free entry; no ring's ref names it (it would need a 16 GiB arc_index.bin).
    const FREE: u32 = u32::MAX;

    /// How many arcs and vertices were read at this point.
    pub fn arcs_read(&self) -> u32 {
        self.arcs_read
    }

    pub fn vertices_read(&self) -> u64 {
        self.vertices_read
    }

    fn home(slot: u32) -> usize {
        // Fibonacci hashing; the top bits of the product mix every bit of the slot.
        (slot.wrapping_mul(0x9E37_79B9) >> (32 - ARC_MEMO_ENTRIES.trailing_zeros())) as usize
    }

    /// The parity remembered for `slot`.
    #[inline]
    pub(crate) fn get(&self, slot: u32) -> Option<bool> {
        if slot == Self::FREE || self.len == 0 {
            return None;
        }
        // At most half the entries are taken: the probe meets a free one.
        let mut i = Self::home(slot);
        loop {
            match self.slots[i] {
                s if s == slot => return Some(self.odd >> i & 1 != 0),
                Self::FREE => return None,
                _ => i = (i + 1) % ARC_MEMO_ENTRIES,
            }
        }
    }

    /// Counts an arc of `vertices` read at this point, and remembers its parity while there is
    /// room. `slot` must not be remembered already.
    pub(crate) fn arc_read(&mut self, slot: u32, odd: bool, vertices: u32) {
        self.arcs_read += 1;
        self.vertices_read += vertices as u64;
        if self.len as usize == ARC_MEMO_CAPACITY || slot == Self::FREE {
            return;
        }
        let mut i = Self::home(slot);
        while self.slots[i] != Self::FREE {
            i = (i + 1) % ARC_MEMO_ENTRIES;
        }
        self.slots[i] = slot;
        self.odd |= (odd as u128) << i;
        self.len += 1;
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len as usize
    }
}

/// The ray from one point towards +lat, crossing the arcs of the chunks it reaches.
struct Crossings<'a, 'm> {
    arcs: ArcFiles<'a>,
    p: PointE7,
    memo: Option<&'m mut ArcMemo>,
    odd: bool,
}

impl RingVisitor for Crossings<'_, '_> {
    fn wants_chunk(&mut self, bbox: &ChunkBox) -> bool {
        !bbox.excludes(self.p)
    }

    #[inline]
    fn arc(&mut self, r: ArcRef) -> Option<()> {
        let remembered = self.memo.as_deref().and_then(|memo| memo.get(r.slot));
        let odd = match remembered {
            Some(odd) => odd,
            None => match arc_crossings(self.arcs.block(r.slot)?, self.p)? {
                ArcCrossings::Boxed => false,
                ArcCrossings::Read { odd, vertices } => {
                    if let Some(memo) = self.memo.as_deref_mut() {
                        memo.arc_read(r.slot, odd, vertices);
                    }
                    odd
                }
            },
        };
        self.odd ^= odd;
        Some(())
    }
}

/// Whether `p` is inside the polygon whose ring block is `block`: the ray from it towards +lat
/// crosses its rings' edges an odd number of times, so its holes are left out. The parity of
/// each arc is that of its stored edges, which a ring walking it backwards shares. None for a
/// malformed block or a ref to no arc. `memo`, when given, must only ever see `p` and `arcs`.
pub(crate) fn ring_block_contains(block: &[u8], arcs: ArcFiles<'_>, p: PointE7, memo: Option<&mut ArcMemo>) -> Option<bool> {
    let mut ray = Crossings { arcs, p, memo, odd: false };
    walk_rings(block, &mut ray)?;
    Some(ray.odd)
}

#[cfg(test)]
pub(crate) mod tests;
