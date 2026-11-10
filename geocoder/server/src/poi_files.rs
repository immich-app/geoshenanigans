//! The POI files as the builder writes them (builder/src/poi_files.h, which lays the format out):
//! poi_records.bin as R2, a 27-byte row per record whose combo (category, tier, flags, importance,
//! polygon) and postcode are indexes into two dictionaries after the rows, and poi_vertices.bin as
//! V2, each polygon's block of bit-packed vertices. A record reads from its row and the
//! dictionaries, which load holds; a polygon decodes from its block alone.

use crate::{vertex_scale, FileBytes, NodeCoord, PoiRecord, NO_DATA};

const MAGIC: &[u8; 4] = b"POR2";
const ROW_BYTES: usize = 27;
const COMBO_BYTES: usize = 5;
const POSTCODE_BYTES: usize = 4;
const FOOTER_BYTES: usize = 24;
const DICT_MAX: usize = 0xFFFF;
const NO_POLY: u32 = 0xFF_FFFF;
const NO_POSTCODE: usize = 0xFFFF;
/// The zero bytes after the last block, for the decoder's 8-byte loads.
const VERTEX_PAD: usize = 8;

#[inline(always)]
fn u16_at(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([b[at], b[at + 1]])
}
#[inline(always)]
fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}

/// poi_records.bin: its rows in place, its dictionaries held.
pub struct PoiRecords {
    file: FileBytes,
    rows: u32,
    combos: Vec<[u8; COMBO_BYTES]>,
    postcodes: Vec<u32>,
}

impl PoiRecords {
    pub fn open(file: FileBytes) -> Result<PoiRecords, String> {
        let b = file.as_slice();
        if b.len() < FOOTER_BYTES {
            return Err("not R2 (shorter than its footer)".into());
        }
        let f = &b[b.len() - FOOTER_BYTES..];
        if &f[20..24] != MAGIC {
            return Err("not R2".into());
        }
        let (rows, combos, postcodes) = (u32_at(f, 0), u32_at(f, 4) as usize, u32_at(f, 8) as usize);
        if combos > DICT_MAX || postcodes > DICT_MAX {
            return Err("a dictionary of more than 65535 entries".into());
        }
        let rows_end = rows as usize * ROW_BYTES;
        let size = rows_end + combos * COMBO_BYTES + postcodes * POSTCODE_BYTES + FOOTER_BYTES;
        if size != b.len() {
            return Err(format!("{} bytes, its footer gives {size}", b.len()));
        }
        let c = &b[rows_end..rows_end + combos * COMBO_BYTES];
        let p = &b[rows_end + combos * COMBO_BYTES..b.len() - FOOTER_BYTES];
        let combos = c.chunks_exact(COMBO_BYTES).map(|e| e.try_into().unwrap()).collect();
        let postcodes = p.chunks_exact(POSTCODE_BYTES).map(|e| u32::from_le_bytes(e.try_into().unwrap())).collect();
        Ok(PoiRecords { file, rows, combos, postcodes })
    }

    /// The records, tombstones included.
    pub fn len(&self) -> u32 {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Record idx, a tombstone included (name_id NO_DATA); None past the last, or where its row
    /// names a dictionary entry the file lacks.
    #[inline]
    pub fn record(&self, idx: u32) -> Option<PoiRecord> {
        if idx >= self.rows {
            return None;
        }
        let at = idx as usize * ROW_BYTES;
        let b: &[u8; ROW_BYTES] = self.file.as_slice()[at..at + ROW_BYTES].try_into().unwrap();
        let c = self.combos.get(u16_at(b, 25) as usize)?;
        let postcode = u16_at(b, 23) as usize;
        let parent_postcode_id = if postcode == NO_POSTCODE { NO_DATA } else { *self.postcodes.get(postcode)? };
        let poly = b[20] as u32 | (b[21] as u32) << 8 | (b[22] as u32) << 16;
        Some(PoiRecord {
            lat: f32::from_le_bytes(b[0..4].try_into().unwrap()),
            lng: f32::from_le_bytes(b[4..8].try_into().unwrap()),
            vertex_offset: if c[4] != 0 { u32_at(b, 8) } else { NO_DATA },
            name_id: u32_at(b, 12),
            category: c[0],
            tier: c[1],
            flags: c[2],
            importance: c[3],
            parent_street_id: u32_at(b, 16),
            parent_postcode_id,
            parent_poly_id: if poly == NO_POLY { NO_DATA } else { poly },
        })
    }
}

/// A V2 block's header.
struct Block {
    encoding: u8,
    closed: bool,
    m: usize,
    bmin_lat: f64,
    bmin_lng: f64,
    wl: u32,
    wg: u32,
    /// Where its vertices start.
    at: usize,
}

fn varint(v: &[u8], at: &mut usize) -> Option<u64> {
    let mut x = 0u64;
    for i in 0..5 {
        let c = *v.get(*at)?;
        *at += 1;
        x |= ((c & 0x7f) as u64) << (7 * i);
        if c & 0x80 == 0 {
            return Some(x);
        }
    }
    None
}

fn unzigzag(z: u64) -> i64 {
    (z >> 1) as i64 ^ -((z & 1) as i64)
}

/// The block of polygon record rec, or None: a point, a block whose vertices run past the file's
/// pad, or one of more vertices than the file has bits (the builder's blocks never have, and a
/// corrupt count of zero-width vertices must not size the decode).
fn block(v: &[u8], rec: &PoiRecord) -> Option<Block> {
    if !rec.is_polygon() {
        return None;
    }
    let mut at = rec.vertex_offset as usize;
    let tag = *v.get(at)?;
    at += 1;
    let m = varint(v, &mut at)? as usize;
    let bits = |rec_bits: u32, d: u64| -> Option<f64> {
        let b = i32::try_from(rec_bits as i32 as i64 + unzigzag(d)).ok()?;
        Some(f32::from_bits(b as u32) as f64)
    };
    let bmin_lat = bits(rec.lat.to_bits(), varint(v, &mut at)?)?;
    let bmin_lng = bits(rec.lng.to_bits(), varint(v, &mut at)?)?;
    let (wl, wg) = (*v.get(at)? as u32, *v.get(at + 1)? as u32);
    at += 2;
    let encoding = tag & 3;
    let max = if encoding == 3 { 32 } else { 16 };
    if tag > 7 || m == 0 || wl > max || wg > max || m as u64 > 8 * v.len() as u64 {
        return None;
    }
    let payload = (m as u64 * (wl + wg) as u64).div_ceil(8);
    if at as u64 + payload + VERTEX_PAD as u64 > v.len() as u64 {
        return None;
    }
    Some(Block { encoding, closed: tag & 4 != 0, m, bmin_lat, bmin_lng, wl, wg, at })
}

/// The polygon of record rec, decoded from its block in poi_vertices.bin: None for a point or a
/// block past the file's end.
pub fn polygon(vertices: &FileBytes, rec: &PoiRecord) -> Option<Vec<NodeCoord>> {
    let v = vertices.as_slice();
    let b = block(v, rec)?;
    let scale = vertex_scale(b.encoding);
    let w = (b.wl + b.wg) as usize;
    let (ml, mg) = (mask(b.wl), mask(b.wg));
    let payload = &v[b.at..];
    // The 8 bytes from the one holding bit `bit` of the vertices, shifted to it. Every bit a vertex
    // has is in the block's payload, and block() checked that 8 bytes past the payload's end stay in
    // the file (its pad), so no load leaves `payload`.
    let load = |bit: usize| {
        debug_assert!((bit >> 3) + 8 <= payload.len());
        // SAFETY: in bounds, as above; read_unaligned takes any alignment.
        u64::from_le(unsafe { std::ptr::read_unaligned(payload.as_ptr().add(bit >> 3) as *const u64) }) >> (bit & 7)
    };
    let mut out = Vec::with_capacity(b.m + b.closed as usize);
    for k in 0..b.m {
        let pos = k * w;
        let (ql, qg) = if w <= 57 {
            let x = load(pos);
            (x & ml, (x >> b.wl) & mg)
        } else {
            (load(pos) & ml, load(pos + b.wl as usize) & mg)
        };
        out.push(NodeCoord {
            lat: (b.bmin_lat + ql as f64 * scale) as f32,
            lng: (b.bmin_lng + qg as f64 * scale) as f32,
        });
    }
    if b.closed {
        out.push(out[0]);
    }
    Some(out)
}

fn mask(w: u32) -> u64 {
    (1u64 << w) - 1
}

/// The vertex count of record rec's polygon: 0 for a point, None for a block past the file's end.
pub fn vertex_count(vertices: &FileBytes, rec: &PoiRecord) -> Option<u32> {
    if !rec.is_polygon() {
        return Some(0);
    }
    let b = block(vertices.as_slice(), rec)?;
    Some((b.m + b.closed as usize) as u32)
}

/// A polygon as the builder holds it: its encoding (0 to 2: u16 vertices, 3: u32), its bbox
/// minimum and its vertices' quantized offsets from it.
#[cfg(any(test, feature = "fixtures"))]
#[derive(Clone, Debug)]
pub struct PoiPolygon {
    pub encoding: u8,
    pub bmin_lat: f32,
    pub bmin_lng: f32,
    pub q: Vec<(u32, u32)>,
}

/// The POI files of pois, each record with its polygon (its vertex_offset not read), as the
/// builder writes them: (poi_records.bin, poi_vertices.bin).
#[cfg(any(test, feature = "fixtures"))]
pub fn encode(pois: &[(PoiRecord, Option<PoiPolygon>)]) -> (Vec<u8>, Vec<u8>) {
    let (mut rows, mut verts) = (Vec::new(), Vec::new());
    let (mut combos, mut postcodes): (Vec<[u8; COMBO_BYTES]>, Vec<u32>) = (Vec::new(), Vec::new());
    let mut logical = 0u64;
    fn index_of<T: PartialEq + Copy>(dict: &mut Vec<T>, v: T) -> u16 {
        let i = dict.iter().position(|&e| e == v).unwrap_or_else(|| {
            dict.push(v);
            dict.len() - 1
        });
        assert!(i < DICT_MAX, "more than 65535 distinct dictionary values");
        i as u16
    }
    fn varint_into(o: &mut Vec<u8>, mut v: u64) {
        while v >= 0x80 {
            o.push(v as u8 | 0x80);
            v >>= 7;
        }
        o.push(v as u8);
    }
    for (r, poly) in pois {
        let combo = [r.category, r.tier, r.flags, r.importance, poly.is_some() as u8];
        let ci = index_of(&mut combos, combo);
        let pi = if r.parent_postcode_id == NO_DATA { NO_POSTCODE as u16 } else { index_of(&mut postcodes, r.parent_postcode_id) };
        let poly_id = if r.parent_poly_id == NO_DATA { NO_POLY } else { r.parent_poly_id };
        assert!(poly_id <= NO_POLY && (poly_id < NO_POLY || r.parent_poly_id == NO_DATA), "parent_poly_id past 24 bits");
        rows.extend_from_slice(&r.lat.to_le_bytes());
        rows.extend_from_slice(&r.lng.to_le_bytes());
        rows.extend_from_slice(&(verts.len() as u32).to_le_bytes());
        rows.extend_from_slice(&r.name_id.to_le_bytes());
        rows.extend_from_slice(&r.parent_street_id.to_le_bytes());
        rows.extend_from_slice(&poly_id.to_le_bytes()[..3]);
        rows.extend_from_slice(&pi.to_le_bytes());
        rows.extend_from_slice(&ci.to_le_bytes());
        let Some(p) = poly else { continue };
        assert!(p.encoding <= 3 && !p.q.is_empty());
        let n = p.q.len();
        let closed = n >= 2 && p.q[0] == p.q[n - 1];
        let m = n - closed as usize;
        let width = |f: fn(&(u32, u32)) -> u32| 32 - p.q[..m].iter().map(f).max().unwrap().leading_zeros();
        let (wl, wg) = (width(|q| q.0), width(|q| q.1));
        verts.push(p.encoding | (closed as u8) << 2);
        varint_into(&mut verts, m as u64);
        for (bmin, at) in [(p.bmin_lat, r.lat), (p.bmin_lng, r.lng)] {
            let d = bmin.to_bits() as i32 as i64 - at.to_bits() as i32 as i64;
            varint_into(&mut verts, ((d << 1) ^ (d >> 63)) as u64);
        }
        verts.extend_from_slice(&[wl as u8, wg as u8]);
        let (mut acc, mut held) = (0u128, 0u32);
        for &(ql, qg) in &p.q[..m] {
            acc |= ((ql as u128) | (qg as u128) << wl) << held;
            held += wl + wg;
            while held >= 8 {
                verts.push(acc as u8);
                acc >>= 8;
                held -= 8;
            }
        }
        if held > 0 {
            verts.push(acc as u8);
        }
        logical += 10 + n as u64 * if p.encoding == 3 { 8 } else { 4 };
    }
    let mut records = rows;
    combos.iter().for_each(|c| records.extend_from_slice(c));
    postcodes.iter().for_each(|p| records.extend_from_slice(&p.to_le_bytes()));
    for v in [pois.len() as u32, combos.len() as u32, postcodes.len() as u32] {
        records.extend_from_slice(&v.to_le_bytes());
    }
    records.extend_from_slice(&logical.to_le_bytes());
    records.extend_from_slice(MAGIC);
    verts.extend_from_slice(&[0; VERTEX_PAD]);
    (records, verts)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(h: &str) -> Vec<u8> {
        let h: String = h.chars().filter(|c| !c.is_whitespace()).collect();
        (0..h.len()).step_by(2).map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap()).collect()
    }

    // The small set of builder/tests/test_poi_files.cpp (a point and a closed triangle) and its
    // files as the builder writes them.
    const SMALL_R2: &str = "0000c03f 000010c0 00000000 64000000 ffffffff ffffff 0000 0000
        0000803f 00000040 00000000 65000000 2c010000 563412 0000 0100
        07 01 02 c8 00  07 01 02 c8 01
        f4010000
        02000000 02000000 01000000 1a00000000000000 504f5232";
    const SMALL_V2: &str = "04 03 00 ffffff03 02 03 7474  0000000000000000";

    fn small_set() -> Vec<(PoiRecord, Option<PoiPolygon>)> {
        let a = PoiRecord {
            lat: 1.5, lng: -2.25, vertex_offset: NO_DATA, name_id: 100, category: 7, tier: 1, flags: 2, importance: 200,
            parent_street_id: NO_DATA, parent_postcode_id: 500, parent_poly_id: NO_DATA,
        };
        let b = PoiRecord { lat: 1.0, lng: 2.0, vertex_offset: 0, name_id: 101, parent_street_id: 300, parent_poly_id: 0x123456, ..a };
        let tri = PoiPolygon { encoding: 0, bmin_lat: 1.0, bmin_lng: 1.5, q: vec![(0, 5), (3, 0), (1, 7), (0, 5)] };
        vec![(a, None), (b, Some(tri))]
    }

    #[test]
    fn reads_a_small_set_as_the_builder_writes_it() {
        let (r, v) = encode(&small_set());
        assert_eq!(r, hex(SMALL_R2));
        assert_eq!(v, hex(SMALL_V2));
        let records = PoiRecords::open(FileBytes::Owned(r)).unwrap();
        let vertices = FileBytes::Owned(v);
        assert_eq!(records.len(), 2);
        let (a, b) = (records.record(0).unwrap(), records.record(1).unwrap());
        assert!(records.record(2).is_none());
        let want = small_set();
        for (got, (w, _)) in [a, b].iter().zip(&want) {
            assert_eq!(
                (got.lat, got.lng, got.name_id, got.category, got.tier, got.flags, got.importance),
                (w.lat, w.lng, w.name_id, w.category, w.tier, w.flags, w.importance)
            );
            assert_eq!((got.parent_street_id, got.parent_postcode_id, got.parent_poly_id), (w.parent_street_id, w.parent_postcode_id, w.parent_poly_id));
        }
        assert!(!a.is_polygon() && b.is_polygon() && b.vertex_offset == 0);
        assert!(polygon(&vertices, &a).is_none());
        assert_eq!(vertex_count(&vertices, &a), Some(0));
        assert_eq!(vertex_count(&vertices, &b), Some(4));
        let got: Vec<(f32, f32)> = polygon(&vertices, &b).unwrap().iter().map(|c| (c.lat, c.lng)).collect();
        let want: Vec<(f32, f32)> = [(0, 5), (3, 0), (1, 7), (0, 5)]
            .iter()
            .map(|&(ql, qg)| ((1.0f64 + ql as f64 * 1e-5) as f32, (1.5f32 as f64 + qg as f64 * 1e-5) as f32))
            .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn decodes_vertices_at_every_width() {
        // u16 and u32 vertices from no bits to 64 a vertex (two loads past 57), open and closed.
        let rec = PoiRecord {
            lat: 47.0, lng: 8.0, vertex_offset: 0, name_id: 1, category: 1, tier: 1, flags: 0, importance: 0,
            parent_street_id: NO_DATA, parent_postcode_id: NO_DATA, parent_poly_id: NO_DATA,
        };
        let polys = [
            (0u8, vec![(0u32, 0u32), (0, 0)]),
            (1, vec![(65535, 3), (7, 65535), (12, 400), (65535, 3)]),
            (3, vec![(u32::MAX, u32::MAX), (0, 1), (0x8000_0000, 0x7FFF_FFFF)]),
            (3, vec![(u32::MAX, 0x7FFF_FFFF), (1, 2), (3, 4), (5, 0x7FFF_FFFF), (u32::MAX, 0x7FFF_FFFF)]),
        ];
        for (encoding, q) in polys {
            let p = PoiPolygon { encoding, bmin_lat: 46.5, bmin_lng: 7.5, q: q.clone() };
            let (r, v) = encode(&[(rec, Some(p))]);
            let records = PoiRecords::open(FileBytes::Owned(r)).unwrap();
            let got = polygon(&FileBytes::Owned(v), &records.record(0).unwrap()).unwrap();
            let scale = vertex_scale(encoding);
            let want: Vec<(f32, f32)> =
                q.iter().map(|&(a, b)| ((46.5f64 + a as f64 * scale) as f32, (7.5f64 + b as f64 * scale) as f32)).collect();
            assert_eq!(got.iter().map(|c| (c.lat, c.lng)).collect::<Vec<_>>(), want, "encoding {encoding}");
        }
    }

    #[test]
    fn refuses_files_that_are_not_r2_and_blocks_past_the_end() {
        let r = hex(SMALL_R2);
        for (bad, err) in [
            (r[..23].to_vec(), "not R2 (shorter than its footer)"),
            ([&r[..r.len() - 1], b"3"].concat(), "not R2"),
            ([&r[..27], &r[..]].concat(), "119 bytes, its footer gives 92"),
        ] {
            assert_eq!(PoiRecords::open(FileBytes::Owned(bad)).err().as_deref(), Some(err));
        }
        let records = PoiRecords::open(FileBytes::Owned(r)).unwrap();
        let b = records.record(1).unwrap();
        // The block's last vertex byte without the pad after it.
        let v = hex(SMALL_V2);
        assert!(polygon(&FileBytes::Owned(v[..11].to_vec()), &b).is_none());
        assert!(polygon(&FileBytes::Owned(v), &b).is_some());
        // 2^32 - 1 vertices of no bits each in a block of 7 bytes.
        let zero_width = FileBytes::Owned(hex("00 ffffffff0f 00 ffffff03 00 00  0000000000000000"));
        assert_eq!(vertex_count(&zero_width, &b), None);
        assert!(polygon(&zero_width, &b).is_none());
    }
}
