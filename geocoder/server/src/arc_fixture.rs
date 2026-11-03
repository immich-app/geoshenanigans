//! Arc files as the builder writes them (builder/src/arc_blocks.h), for the tests here and, with
//! the `fixtures` feature, for the crates that read them: arcs given as e7 vertices, and the ring
//! blocks of refs to them.

use crate::exact_pip::PointE7;

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

/// Arcs, in slot order.
#[derive(Default)]
pub struct ArcFixture {
    arcs: Vec<(Vec<u8>, RawBox)>,
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
}
