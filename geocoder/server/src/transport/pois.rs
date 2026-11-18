//! The pois unit: poi_records.bin's R2 rows, its dictionaries and footer as the tail, and
//! poi_vertices.bin's V2 blocks unpacked (poi_files.rs; builder/src/poi_files.h lays the files
//! out). Decode writes each block's header from its values and packs its stored vertices at its
//! widths.

use super::cols::{decode_rows, encode_rows, Layout};
use super::container::{bits_at, col, invalid, read_tail, unit_col, Chunk, ColSpec, Input, Out, Pack, Packed, Section, Source, B, D, MAX_CHUNK_BYTES, P, R, RAW_P, RAW_S, S};
use crate::poi_files::{FOOTER_BYTES, MAGIC, RECORDS_FILE, ROW_BYTES, VERTEX_PAD, VERTICES_FILE};
use std::io::{self, Read, Write};

pub(super) const MEMBERS: [&str; 2] = [RECORDS_FILE, VERTICES_FILE];

/// R2's row: lat, lng, voff, name, street, the three bytes of poly, postcode, combo.
const RECORDS: [ColSpec; 10] = [col(4, D), col(4, B), col(4, S), col(4, S), col(4, R), col(1, D), col(1, R), col(1, R), col(2, R), col(2, B)];
/// A block's header (tag, m, the bbox varints' values, wl, wg), then its stored vertices: q_lat
/// and q_lng as u16 (encodings 0 to 2) or as u32 (encoding 3).
const VERTICES: [ColSpec; 10] = [
    col(1, R),
    col(4, B),
    col(8, B),
    col(8, B),
    col(1, R),
    col(1, R),
    unit_col(2, P),
    unit_col(2, P),
    unit_col(4, RAW_P),
    unit_col(4, RAW_S),
];
const TAG: usize = 0;
const M: usize = 1;
const BL: usize = 2;
const BG: usize = 3;
const WL: usize = 4;
const WG: usize = 5;
const LAT16: usize = 6;
const LNG16: usize = 7;
const LAT32: usize = 8;
const LNG32: usize = 9;
const HEADER_COLS: usize = 1 + 4 + 8 + 8 + 1 + 1;

/// Encoding 3's vertices are u32, the others' u16.
fn wide(tag: u64) -> bool {
    tag & 3 == 3
}

/// A block's header from its values, then its m stored vertices, `vertex` giving each one's q_lat
/// and q_lng, at widths wl and wg.
fn pack_block(p: &mut impl Pack, [tag, m, bl, bg, wl, wg]: [u64; 6], mut vertex: impl FnMut() -> (u64, u64)) -> io::Result<()> {
    p.byte(tag as u8)?;
    p.varint(m)?;
    p.varint(bl)?;
    p.varint(bg)?;
    p.bytes(&[wl as u8, wg as u8])?;
    for _ in 0..m {
        let (ql, qg) = vertex();
        p.bits(ql, wl as u32)?;
        p.bits(qg, wg as u32)?;
    }
    p.pad()
}

/// The LEB128 varint at `*at` of `b`, which it moves past it; None past b's end or 64 bits.
fn varint(b: &[u8], at: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *b.get(*at)?;
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

pub(super) fn encode(w: &mut impl Write, records: &Source, vertices: &Source) -> io::Result<()> {
    let r = records.bytes();
    if r.len() < FOOTER_BYTES || &r[r.len() - 4..] != MAGIC {
        return Err(records.refuse("no POR2 footer"));
    }
    let rows = u32::from_le_bytes(r[r.len() - FOOTER_BYTES..r.len() - FOOTER_BYTES + 4].try_into().unwrap()) as usize;
    if rows > (r.len() - FOOTER_BYTES) / ROW_BYTES {
        return Err(records.refuse(format!("{rows} rows past its end")));
    }
    encode_rows(w, r, rows, &Layout::of(&RECORDS), &r[rows * ROW_BYTES..])?;
    encode_vertices(w, vertices)
}

fn encode_vertices(w: &mut impl Write, vertices: &Source) -> io::Result<()> {
    let v = vertices.bytes();
    let end = v.len().checked_sub(VERTEX_PAD).ok_or_else(|| vertices.refuse("shorter than its pad"))?;
    let mut sec = Section::new(&VERTICES);
    let (mut at, mut packed, mut q) = (0usize, Packed::default(), Vec::new());
    while at < end {
        let block = &v[at..end];
        let bad = |what: &str| vertices.refuse(format!("the block at {at}: {what}"));
        let mut h = 1;
        let tag = u64::from(block[0]);
        let (Some(m), Some(bl), Some(bg)) = (varint(block, &mut h), varint(block, &mut h), varint(block, &mut h)) else {
            return Err(bad("a header varint past the pad"));
        };
        let (Some(&wl), Some(&wg)) = (block.get(h), block.get(h + 1)) else { return Err(bad("a header past the pad")) };
        h += 2;
        let (wl, wg, max) = (u32::from(wl), u32::from(wg), if wide(tag) { 32 } else { 16 });
        if wl > max || wg > max {
            return Err(bad("a vertex width past its encoding's"));
        }
        // Bounded before they are unpacked: vertices of no bits take no bytes of the file.
        let vertex_cols = if wide(tag) { 8 } else { 4 };
        if m > (MAX_CHUNK_BYTES - HEADER_COLS) as u64 / vertex_cols {
            return Err(bad("more vertices than a chunk holds"));
        }
        let payload = (m as usize).checked_mul((wl + wg) as usize).map(|b| b.div_ceil(8)).filter(|&p| p <= block.len() - h);
        let Some(payload) = payload else { return Err(bad("vertices past the pad")) };
        q.clear();
        let mut bit = h * 8;
        for _ in 0..m {
            let ql = bits_at(block, bit, wl);
            let qg = bits_at(block, bit + wl as usize, wg);
            bit += (wl + wg) as usize;
            q.push((ql, qg));
        }
        let header = [tag, m, bl, bg, u64::from(wl), u64::from(wg)];
        packed.clear();
        let mut next = q.iter();
        pack_block(&mut packed, header, || *next.next().unwrap_or(&(0, 0)))?;
        if packed.bytes != block[..h + payload] {
            return Err(bad("not as decode writes it back (a varint in more bytes than it needs, or bits set past its vertices)"));
        }
        sec.unit(w, HEADER_COLS + q.len() * vertex_cols as usize)?;
        for (c, x) in [TAG, M, BL, BG, WL, WG].into_iter().zip(header) {
            sec.push(c, x);
        }
        let (lat, lng) = if wide(tag) { (LAT32, LNG32) } else { (LAT16, LNG16) };
        for &(ql, qg) in &q {
            sec.push(lat, ql);
            sec.push(lng, qg);
        }
        at += h + payload;
    }
    sec.finish(w, &v[end..])
}

pub(super) fn decode<R: Read, W: Write>(r: &mut Input<R>, chunk: &mut Chunk, outs: &mut [Out<W>]) -> io::Result<()> {
    let [records, vertices] = outs else { return Err(invalid("a pois unit without its two members")) };
    decode_rows(r, chunk, records, &Layout::of(&RECORDS))?;
    while let Some(units) = chunk.read(r, &VERTICES)? {
        let polygons = u64::from(units);
        for c in [TAG, M, BL, BG, WL, WG] {
            chunk.expect(c, polygons, "a polygon's header")?;
        }
        let buf = &chunk.buf;
        let (mut tags, mut ms, mut narrow, mut wide_n) = (chunk.col(TAG), chunk.col(M), 0u64, 0u64);
        for _ in 0..polygons {
            let (t, m) = (tags.next(buf), ms.next(buf));
            *if wide(t) { &mut wide_n } else { &mut narrow } += m;
        }
        for (c, n) in [(LAT16, narrow), (LNG16, narrow), (LAT32, wide_n), (LNG32, wide_n)] {
            chunk.expect(c, n, "a polygon's vertices")?;
        }
        let mut c: [_; 10] = std::array::from_fn(|i| chunk.col(i));
        for _ in 0..polygons {
            let header = [TAG, M, BL, BG, WL, WG].map(|i| c[i].next(buf));
            let (tag, wl, wg) = (header[0], header[4], header[5]);
            let max = if wide(tag) { 32 } else { 16 };
            if wl > max || wg > max {
                return Err(invalid(format!("a polygon's vertex width past its encoding's {max} bits")));
            }
            let (lat, lng) = if wide(tag) { (LAT32, LNG32) } else { (LAT16, LNG16) };
            let [ql, qg] = c.get_disjoint_mut([lat, lng]).map_err(|_| invalid("vertex columns"))?;
            ql.unit_start();
            qg.unit_start();
            pack_block(vertices, header, || (ql.next(buf), qg.next(buf)))?;
        }
    }
    read_tail(r, vertices, &mut chunk.buf)
}
