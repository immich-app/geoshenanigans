//! The arcs unit: arc_vertices.bin's arc blocks unpacked, a slot a unit, and arc_index.bin
//! computed (arc_blocks.rs lays the files out). Decode rebuilds each arc's vertices from its first
//! vertex and its deltas, as an arc block's reader does, to write its box (the extent of those
//! vertices) when its flags say it has one, and packs each group of deltas at the widths its
//! header byte gives. A slot's offset is where the blocks before it end.

use super::container::{bits_at, col, invalid, read_tail, unzigzag, zigzag, Chunk, ColSpec, Input, Out, Pack, Packed, Section, Source, B, MAX_CHUNK_BYTES, R};
use crate::arc_blocks::{ARC_GROUP, BASE_WIDTH_SHIFT, HAS_BOX, SECOND_ORDER};
use std::io::{self, Read, Write};

pub(super) const MEMBERS: [&str; 2] = ["arc_index.bin", "arc_vertices.bin"];

/// A slot's vertex count; an arc's flags and first vertex; the arcs' group header bytes; their
/// deltas.
const SLOTS: [ColSpec; 7] = [col(4, B), col(1, R), col(8, B), col(8, B), col(1, R), col(8, B), col(8, B)];
const COUNT: usize = 0;
const FLAGS: usize = 1;
const FIRST_LAT: usize = 2;
const FIRST_LNG: usize = 3;
const GROUPS: usize = 4;
const ZL: usize = 5;
const ZG: usize = 6;
const SLOT_COLS: usize = 4;
const ARC_COLS: usize = 1 + 8 + 8;
const DELTA_COLS: usize = 8 + 8;

fn base_width(flags: u8) -> u32 {
    2 * u32::from(flags >> BASE_WIDTH_SHIFT & 15)
}

/// An arc's box (min lng, max lng, max lat) and last vertex, in grid units, from its first vertex
/// and its m zz deltas (first or second order as its flags say), as the readers sum them: mod 2^64,
/// which no block a reader accepts reaches.
fn walk(flags: u8, first: (i64, i64), deltas: impl Iterator<Item = (u64, u64)>) -> ([i64; 3], (i64, i64)) {
    let second = flags & SECOND_ORDER != 0;
    let ((mut lat, mut lng), mut step) = (first, (0i64, 0i64));
    let mut bx = [lng, lng, lat];
    for (zl, zg) in deltas {
        let (dl, dg) = (unzigzag(zl), unzigzag(zg));
        step = if second { (step.0.wrapping_add(dl), step.1.wrapping_add(dg)) } else { (dl, dg) };
        lat = lat.wrapping_add(step.0);
        lng = lng.wrapping_add(step.1);
        bx = [bx[0].min(lng), bx[1].max(lng), bx[2].max(lat)];
    }
    (bx, (lat, lng))
}

/// An arc block: its flags, first vertex, its box when the flags say so (from `walk`, over
/// `deltas`), then its m deltas (`deltas_again`, the same) in groups of ARC_GROUP, each at the
/// widths its header byte (from `headers`) gives. Its last vertex.
fn pack_arc(
    p: &mut impl Pack,
    flags: u8,
    first: (i32, i32),
    m: u64,
    deltas: impl Iterator<Item = (u64, u64)>,
    mut deltas_again: impl Iterator<Item = (u64, u64)>,
    mut headers: impl Iterator<Item = u8>,
) -> io::Result<(i64, i64)> {
    p.byte(flags)?;
    p.bytes(&first.0.to_le_bytes())?;
    p.bytes(&first.1.to_le_bytes())?;
    let (bx, last) = walk(flags, (i64::from(first.0), i64::from(first.1)), deltas.take(m as usize));
    if flags & HAS_BOX != 0 {
        for c in bx {
            let c = i32::try_from(c).map_err(|_| invalid("an arc's box past 32 bits"))?;
            p.bytes(&c.to_le_bytes())?;
        }
    }
    let b = base_width(flags);
    let (mut left, mut group) = (m, [(0u64, 0u64); ARC_GROUP]);
    while left > 0 {
        let c = left.min(ARC_GROUP as u64) as usize;
        let h = headers.next().unwrap_or(0);
        p.byte(h)?;
        let (wl, wg) = (b + u32::from(h >> 4), b + u32::from(h & 15));
        for d in group.iter_mut().take(c) {
            *d = deltas_again.next().unwrap_or((0, 0));
        }
        for d in &group[..c] {
            p.bits(d.0, wl)?;
        }
        for d in &group[..c] {
            p.bits(d.1, wg)?;
        }
        p.pad()?;
        left -= c as u64;
    }
    Ok(last)
}

/// An arc block's flags, first vertex, group header bytes and zz deltas, read as the readers read
/// it (arc_blocks.rs arc_head and bit_group); what is wrong with a block they refuse, or with one
/// of more deltas than a chunk holds.
struct Arc {
    flags: u8,
    first: (i32, i32),
    headers: Vec<u8>,
    deltas: Vec<(u64, u64)>,
}

fn unpack(block: &[u8], arc: &mut Arc) -> Result<(), &'static str> {
    let short = "shorter than its head";
    let flags = *block.first().ok_or(short)?;
    let i32_at = |at: usize| block.get(at..at + 4).map(|b| i32::from_le_bytes([b[0], b[1], b[2], b[3]])).ok_or(short);
    arc.flags = flags;
    arc.first = (i32_at(1)?, i32_at(5)?);
    let mut at = if flags & HAS_BOX != 0 { 21 } else { 9 };
    if at > block.len() {
        return Err(short);
    }
    arc.headers.clear();
    arc.deltas.clear();
    let b = base_width(flags);
    while at < block.len() {
        let h = block[at];
        at += 1;
        let (wl, wg) = (b + u32::from(h >> 4), b + u32::from(h & 15));
        let w = (wl + wg) as usize;
        let rem = block.len() - at;
        let c = if rem >= 2 * w {
            ARC_GROUP
        } else if w < 8 {
            return Err("a short group of fewer than 8 bits a delta");
        } else {
            rem * 8 / w
        };
        let used = (c * w).div_ceil(8);
        if c == 0 || (c < ARC_GROUP && used != rem) {
            return Err("a group its bytes don't hold");
        }
        // Bounded as they are unpacked: deltas of no bits take no bytes of the file.
        if (arc.deltas.len() + c) * DELTA_COLS > MAX_CHUNK_BYTES - SLOT_COLS - ARC_COLS - arc.headers.len() - 1 {
            return Err("more deltas than a chunk holds");
        }
        let g = &block[at..at + used];
        arc.headers.push(h);
        let lng0 = c * wl as usize;
        for i in 0..c {
            arc.deltas.push((bits_at(g, i * wl as usize, wl), bits_at(g, lng0 + i * wg as usize, wg)));
        }
        at += used;
    }
    Ok(())
}

pub(super) fn encode(w: &mut impl Write, index: &Source, vertices: &Source) -> io::Result<()> {
    let (idx, v) = (index.bytes(), vertices.bytes());
    if idx.len() % 4 != 0 {
        return Err(index.refuse("not whole slots"));
    }
    let slots = idx.len() / 4;
    if slots == 0 && !v.is_empty() {
        return Err(vertices.refuse("blocks where arc_index.bin has no slots"));
    }
    let offset = |s: usize| u32::from_le_bytes([idx[4 * s], idx[4 * s + 1], idx[4 * s + 2], idx[4 * s + 3]]) as usize;
    let mut sec = Section::new(&SLOTS);
    let (mut at, mut last, mut packed) = (0usize, (0i64, 0i64), Packed::default());
    let mut arc = Arc { flags: 0, first: (0, 0), headers: Vec::new(), deltas: Vec::new() };
    for s in 0..slots {
        if offset(s) != at {
            return Err(index.refuse(format!("slot {s}'s offset is not where the blocks before it end")));
        }
        let end = if s + 1 < slots { offset(s + 1) } else { v.len() };
        if end < at || end > v.len() {
            return Err(index.refuse(format!("slot {s}'s block ends before it starts or past arc_vertices.bin")));
        }
        let block = &v[at..end];
        if block.is_empty() {
            if sec.unit(w, SLOT_COLS)? {
                last = (0, 0);
            }
            sec.push(COUNT, 0);
            continue;
        }
        unpack(block, &mut arc).map_err(|what| vertices.refuse(format!("slot {s}'s block: {what}")))?;
        let m = arc.deltas.len() as u64;
        packed.clear();
        let end_vertex = pack_arc(&mut packed, arc.flags, arc.first, m, arc.deltas.iter().copied(), arc.deltas.iter().copied(), arc.headers.iter().copied())?;
        if packed.bytes != block {
            return Err(vertices.refuse(format!("slot {s}'s block is not as decode writes it back (a box that is not its vertices', or bits set past its deltas)")));
        }
        if sec.unit(w, SLOT_COLS + ARC_COLS + arc.headers.len() + arc.deltas.len() * DELTA_COLS)? {
            last = (0, 0);
        }
        sec.push(COUNT, m + 1);
        sec.push(FLAGS, u64::from(arc.flags));
        sec.push(FIRST_LAT, zigzag(i64::from(arc.first.0).wrapping_sub(last.0)));
        sec.push(FIRST_LNG, zigzag(i64::from(arc.first.1).wrapping_sub(last.1)));
        for &h in &arc.headers {
            sec.push(GROUPS, u64::from(h));
        }
        for &(zl, zg) in &arc.deltas {
            sec.push(ZL, zl);
            sec.push(ZG, zg);
        }
        last = end_vertex;
        at = end;
    }
    sec.finish(w, &[])
}

pub(super) fn decode<R: Read, W: Write>(r: &mut Input<R>, chunk: &mut Chunk, outs: &mut [Out<W>]) -> io::Result<()> {
    let [index, vertices] = outs else { return Err(invalid("an arcs unit without its two members")) };
    while let Some(units) = chunk.read(r, &SLOTS)? {
        let slots = u64::from(units);
        chunk.expect(COUNT, slots, "an arc slot's vertex count")?;
        let buf = &chunk.buf;
        let (mut counts, mut arcs, mut groups, mut deltas) = (chunk.col(COUNT), 0u64, 0u64, 0u64);
        for _ in 0..slots {
            let n = counts.next(buf);
            if n > 0 {
                arcs += 1;
                groups += (n - 1).div_ceil(ARC_GROUP as u64);
                deltas += n - 1;
            }
        }
        for (c, n, what) in [
            (FLAGS, arcs, "an arc's flags"),
            (FIRST_LAT, arcs, "an arc's first vertex"),
            (FIRST_LNG, arcs, "an arc's first vertex"),
            (GROUPS, groups, "the arcs' groups"),
            (ZL, deltas, "the arcs' deltas"),
            (ZG, deltas, "the arcs' deltas"),
        ] {
            chunk.expect(c, n, what)?;
        }
        let (mut counts, mut flags, mut first_lat, mut first_lng) = (chunk.col(COUNT), chunk.col(FLAGS), chunk.col(FIRST_LAT), chunk.col(FIRST_LNG));
        let (mut headers, mut zl, mut zg) = (chunk.col(GROUPS), chunk.col(ZL), chunk.col(ZG));
        let mut last = (0i64, 0i64);
        for _ in 0..slots {
            let offset = u32::try_from(vertices.at()).map_err(|_| invalid("arc_vertices.bin past 4 GiB"))?;
            index.bytes(&offset.to_le_bytes())?;
            let n = counts.next(buf);
            if n == 0 {
                continue;
            }
            let f = flags.next(buf) as u8;
            let first = (last.0.wrapping_add(unzigzag(first_lat.next(buf))), last.1.wrapping_add(unzigzag(first_lng.next(buf))));
            let first = match (i32::try_from(first.0), i32::try_from(first.1)) {
                (Ok(lat), Ok(lng)) => (lat, lng),
                _ => return Err(invalid("an arc's first vertex past 32 bits")),
            };
            let (mut zl1, mut zg1) = (zl, zg);
            let ahead = std::iter::from_fn(|| Some((zl1.next(buf), zg1.next(buf))));
            let again = std::iter::from_fn(|| Some((zl.next(buf), zg.next(buf))));
            let hs = std::iter::from_fn(|| Some(headers.next(buf) as u8));
            last = pack_arc(vertices, f, first, n - 1, ahead, again, hs)?;
        }
    }
    read_tail(r, vertices, &mut chunk.buf)
}
