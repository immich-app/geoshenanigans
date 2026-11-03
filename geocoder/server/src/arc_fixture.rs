//! Arc files as the builder writes them (builder/src/arc_blocks.h), for the tests here and, with
//! the `fixtures` feature, for the crates that read them: arcs given as e7 vertices, admin polygon
//! records whose rings are refs to those arcs, and the four files they make.

use crate::exact_pip::{to_e7, PointE7};
use crate::{AdminPolygon, NO_DATA};

/// An arc block of this many vertices or more carries its box (builder kArcBboxMinVertices).
pub const ARC_BBOX_MIN_VERTICES: usize = 16;
/// A ring of at most this many refs is one chunk without a box (builder kChunkBoxMinRefs).
pub const CHUNK_BOX_MIN_REFS: usize = 8;
/// A longer ring is cut after a ref whose slot's mix64 has REF_CHUNK_CUT_MASK's bits all set once
/// the chunk holds REF_CHUNK_MIN refs, after REF_CHUNK_MAX, and at its end (builder
/// kRefChunkMin, kRefChunkMax, kRefChunkCutMask).
pub const REF_CHUNK_MIN: usize = 4;
pub const REF_CHUNK_MAX: usize = 16;
pub const REF_CHUNK_CUT_MASK: u64 = 7;

/// splitmix64's finalizer, which the builder's chunk cuts hash a slot with.
pub fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    x
}

pub fn zigzag(v: i64) -> u64 {
    ((v as u64) << 1) ^ ((v >> 63) as u64)
}

pub fn put_varint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// The point (lat, lng) in degrees on the e7 grid.
pub fn e7(lat: f64, lng: f64) -> PointE7 {
    PointE7 { lat: to_e7(lat), lng: to_e7(lng) }
}

/// A ring's ref to an arc slot, the arc walked backwards when `reversed`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ref {
    pub slot: u32,
    pub reversed: bool,
}

impl Ref {
    pub fn fwd(slot: u32) -> Self {
        Ref { slot, reversed: false }
    }

    pub fn rev(slot: u32) -> Self {
        Ref { slot, reversed: true }
    }
}

/// The extent in e7 units of an arc's raw vertices, along the three sides a chunk's box keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RawBox {
    pub min_lng: i32,
    pub max_lng: i32,
    pub max_lat: i32,
}

impl RawBox {
    pub const EMPTY: RawBox = RawBox { min_lng: i32::MAX, max_lng: i32::MIN, max_lat: i32::MIN };

    pub fn of(vertices: &[PointE7]) -> RawBox {
        let mut b = RawBox::EMPTY;
        for v in vertices {
            b.add(RawBox { min_lng: v.lng as i32, max_lng: v.lng as i32, max_lat: v.lat as i32 });
        }
        b
    }

    pub fn add(&mut self, o: RawBox) {
        self.min_lng = self.min_lng.min(o.min_lng);
        self.max_lng = self.max_lng.max(o.max_lng);
        self.max_lat = self.max_lat.max(o.max_lat);
    }
}

/// The arc block of `vertices` (e7 units, each a multiple of 10^k, at least one), with the box of
/// its vertices when `boxed`.
pub fn encode_arc(k: u8, vertices: &[PointE7], boxed: bool) -> Vec<u8> {
    assert!(k <= 3 && !vertices.is_empty());
    let step = 10i64.pow(k as u32);
    let unit = |c: i64| {
        assert_eq!(c % step, 0, "a vertex off the 10^{k} grid");
        i32::try_from(c / step).expect("a coordinate past i32 grid units")
    };
    let mut out = vec![k | if boxed { 0x80 } else { 0 }];
    out.extend(unit(vertices[0].lat).to_le_bytes());
    out.extend(unit(vertices[0].lng).to_le_bytes());
    if boxed {
        let units: Vec<PointE7> = vertices.iter().map(|v| PointE7 { lat: unit(v.lat) as i64, lng: unit(v.lng) as i64 }).collect();
        let b = RawBox::of(&units);
        for c in [b.min_lng, b.max_lng, b.max_lat] {
            out.extend(c.to_le_bytes());
        }
    }
    for e in vertices.windows(2) {
        put_varint(zigzag(unit(e[1].lat) as i64 - unit(e[0].lat) as i64), &mut out);
        put_varint(zigzag(unit(e[1].lng) as i64 - unit(e[0].lng) as i64), &mut out);
    }
    out
}

/// One ring's chunks as the builder cuts them, a boxed chunk spanning `box_of` each of its arcs.
pub fn encode_ring(refs: &[Ref], box_of: impl Fn(u32) -> RawBox) -> Vec<u8> {
    let mut out = Vec::new();
    let mut chunk = |refs: &[Ref], boxed: bool, last: bool| {
        put_varint((refs.len() as u64) << 2 | (boxed as u64) << 1 | last as u64, &mut out);
        let mut body = Vec::new();
        let mut prev = 0i64;
        for r in refs {
            put_varint(zigzag(r.slot as i64 - prev) << 1 | r.reversed as u64, &mut body);
            prev = r.slot as i64;
        }
        if boxed {
            let mut b = RawBox::EMPTY;
            refs.iter().for_each(|r| b.add(box_of(r.slot)));
            for c in [b.min_lng, b.max_lng, b.max_lat] {
                out.extend(c.to_le_bytes());
            }
            put_varint(body.len() as u64, &mut out);
        }
        out.extend(body);
    };
    if refs.len() <= CHUNK_BOX_MIN_REFS {
        chunk(refs, false, true);
        return out;
    }
    let mut first = 0;
    for i in 0..refs.len() {
        let count = i - first + 1;
        let cut = count == REF_CHUNK_MAX || (count >= REF_CHUNK_MIN && mix64(refs[i].slot as u64) & REF_CHUNK_CUT_MASK == REF_CHUNK_CUT_MASK);
        if i + 1 == refs.len() || cut {
            chunk(&refs[first..=i], true, i + 1 == refs.len());
            first = i + 1;
        }
    }
    out
}

/// A record's 24 bytes as admin_polygons.bin stores it.
pub fn record_bytes(p: &AdminPolygon) -> Vec<u8> {
    let b = [
        &p.ring_offset.to_le_bytes()[..], &p.ring_bytes.to_le_bytes(), &p.name_id.to_le_bytes(),
        &[p.admin_level, p.place_type_override, p._pad2, p._pad3], &p.area.to_le_bytes(),
        &p.country_code.to_le_bytes(), &p.hole_count.to_le_bytes(),
    ]
    .concat();
    assert_eq!(b.len(), std::mem::size_of::<AdminPolygon>());
    b
}

/// A record with every field but the name, level and area at 0, for `ArcFixture::polygon` to
/// give its rings.
pub fn record(name_id: u32, admin_level: u8, area: f32) -> AdminPolygon {
    AdminPolygon {
        ring_offset: 0, ring_bytes: 0, name_id, admin_level, place_type_override: 0, _pad2: 0, _pad3: 0, area,
        country_code: 0, hole_count: 0,
    }
}

/// The area the builder stores for a polygon's rings (the outer, then its holes): each ring's
/// exact shoelace, the holes taken off (none below 0), in square degrees.
pub fn exact_area(rings: &[Vec<PointE7>]) -> f32 {
    let twice = |r: &Vec<PointE7>| -> i128 {
        let n = r.len();
        (0..n).map(|a| {
            let b = (a + n - 1) % n;
            r[b].lat as i128 * r[a].lng as i128 - r[a].lat as i128 * r[b].lng as i128
        }).sum::<i128>().abs()
    };
    let mut area2 = rings.first().map_or(0, twice);
    for r in rings.iter().skip(1) {
        area2 -= twice(r);
    }
    (area2.max(0) as f64 * 0.5e-14) as f32
}

/// Admin polygons and the arcs their rings are made of, in slot order.
#[derive(Default)]
pub struct ArcFixture {
    arcs: Vec<(Vec<u8>, RawBox)>,
    polygons: Vec<(AdminPolygon, Vec<Vec<Ref>>)>,
}

impl ArcFixture {
    pub fn new() -> Self {
        Self::default()
    }

    /// An arc of `vertices`, boxed from ARC_BBOX_MIN_VERTICES on as the builder boxes one, its
    /// raw vertices the stored ones; its slot.
    pub fn arc(&mut self, k: u8, vertices: &[PointE7]) -> u32 {
        self.arc_block(encode_arc(k, vertices, vertices.len() >= ARC_BBOX_MIN_VERTICES), RawBox::of(vertices))
    }

    /// An arc slot holding `block` as given, whose raw vertices `raw` spans; its slot.
    pub fn arc_block(&mut self, block: Vec<u8>, raw: RawBox) -> u32 {
        self.arcs.push((block, raw));
        self.arcs.len() as u32 - 1
    }

    /// A dead arc slot: an empty block.
    pub fn dead_arc(&mut self) -> u32 {
        self.arc_block(Vec::new(), RawBox::EMPTY)
    }

    /// A polygon whose rings, outer first, are the refs given (none: a polygon without rings).
    /// The record's ring_offset, ring_bytes and hole_count are set from them; its slot.
    pub fn polygon(&mut self, record: AdminPolygon, rings: &[Vec<Ref>]) -> u32 {
        self.polygons.push((record, rings.to_vec()));
        self.polygons.len() as u32 - 1
    }

    /// A polygon whose closed rings (outer first) are each one arc at grid k; its slot.
    pub fn ring_polygon(&mut self, record: AdminPolygon, k: u8, rings: &[Vec<PointE7>]) -> u32 {
        let refs: Vec<Vec<Ref>> = rings.iter().map(|r| vec![Ref::fwd(self.arc(k, r))]).collect();
        self.polygon(record, &refs)
    }

    /// A polygon slot's tombstone: no rings and no name.
    pub fn tombstone(&mut self) -> u32 {
        self.polygon(AdminPolygon { ring_offset: NO_DATA, name_id: NO_DATA, ..record(0, 0, 0.0) }, &[])
    }

    pub fn arc_index(&self) -> Vec<u8> {
        let mut offset = 0u32;
        let mut index = Vec::new();
        for (block, _) in &self.arcs {
            index.extend(offset.to_le_bytes());
            offset += block.len() as u32;
        }
        index
    }

    pub fn arc_vertices(&self) -> Vec<u8> {
        self.arcs.iter().flat_map(|(block, _)| block.iter().copied()).collect()
    }

    /// The ring block of each polygon, in slot order.
    pub fn ring_blocks(&self) -> Vec<Vec<u8>> {
        let box_of = |slot: u32| self.arcs.get(slot as usize).map_or(RawBox::EMPTY, |a| a.1);
        self.polygons.iter().map(|(_, rings)| rings.iter().flat_map(|r| encode_ring(r, box_of)).collect()).collect()
    }

    /// Each polygon's record as written: its ring block's offset and length in admin_rings.bin
    /// and its hole count (a tombstone keeps NO_DATA).
    pub fn records(&self) -> Vec<AdminPolygon> {
        let mut offset = 0u32;
        self.polygons.iter().zip(self.ring_blocks()).map(|((record, rings), block)| {
            let written = if record.ring_offset == NO_DATA {
                *record
            } else {
                AdminPolygon {
                    ring_offset: offset, ring_bytes: block.len() as u32,
                    hole_count: rings.len().saturating_sub(1).min(u16::MAX as usize) as u16, ..*record
                }
            };
            offset += block.len() as u32;
            written
        }).collect()
    }

    /// admin_polygons.bin, admin_rings.bin, arc_index.bin and arc_vertices.bin, by name.
    pub fn files(&self) -> [(&'static str, Vec<u8>); 4] {
        [
            ("admin_polygons.bin", self.records().iter().flat_map(record_bytes).collect()),
            ("admin_rings.bin", self.ring_blocks().concat()),
            ("arc_index.bin", self.arc_index()),
            ("arc_vertices.bin", self.arc_vertices()),
        ]
    }
}
