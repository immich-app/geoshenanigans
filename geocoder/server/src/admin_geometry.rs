//! The admin polygons' geometry. Every admin point-in-polygon test and ring decode goes through
//! `AdminGeometry`, so how a region stores its rings is this module's business alone.

use crate::{
    decode_polygon_verts, encoded_polygon, geojson_rings, point_in_encoded_polygon, point_on_surface, point_on_surface_in,
    AdminPolygon, FileBytes, NodeCoord, NO_DATA,
};
use std::borrow::Cow;

/// A point `AdminGeometry::contains` tests, at the precision rings are compared at.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AdminPoint {
    lat: f32,
    lng: f32,
}

impl AdminPoint {
    pub fn new(lat: f64, lng: f64) -> Self {
        AdminPoint { lat: lat as f32, lng: lng as f32 }
    }
}

/// `AdminGeometry::contains` answers for one point in one geometry, by ring block (open
/// addressing, linear probing). Holds nothing until its first answer.
#[derive(Default)]
pub struct PipMemo {
    slots: Vec<(u64, bool)>,
    len: usize,
}

impl PipMemo {
    /// Marks a free slot; no ring block's key is all ones (`AdminGeometry::block_key`).
    const FREE: u64 = u64::MAX;
    const START_SLOTS: usize = 64;

    pub fn new() -> Self {
        Self::default()
    }

    /// The answer stored under `key`, else `answer()`, stored.
    fn get_or_insert_with(&mut self, key: u64, answer: impl FnOnce() -> bool) -> bool {
        if self.slots.is_empty() {
            self.slots = vec![(Self::FREE, false); Self::START_SLOTS];
        }

        let i = self.slot(key);
        if self.slots[i].0 == key {
            return self.slots[i].1;
        }
        let inside = answer();
        self.slots[i] = (key, inside);
        self.len += 1;
        if self.len * 2 > self.slots.len() {
            self.grow();
        }
        inside
    }

    fn slot(&self, key: u64) -> usize {
        let mask = self.slots.len() - 1;
        // Fibonacci hashing; the high bits of the product mix every bit of the key.
        let mut i = (key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize & mask;
        while self.slots[i].0 != Self::FREE && self.slots[i].0 != key {
            i = (i + 1) & mask;
        }
        i
    }

    fn grow(&mut self) {
        let bigger = vec![(Self::FREE, false); self.slots.len() * 2];
        let old = std::mem::replace(&mut self.slots, bigger);
        for (key, inside) in old {
            if key == Self::FREE { continue; }
            let i = self.slot(key);
            self.slots[i] = (key, inside);
        }
    }
}

/// A region's admin polygon geometry: each polygon's rings as one vertex range in
/// `admin_vertices.bin` (or `postal_vertices.bin`), holes keyhole-encoded (`AdminPolygon::hole_count`).
pub struct AdminGeometry {
    vertices: FileBytes,
}

impl AdminGeometry {
    pub fn new(vertices: FileBytes) -> Self {
        AdminGeometry { vertices }
    }

    /// Whether `poly` has rings: false for a polygon a tier simplified away, or whose ring block
    /// lies past the end of the file.
    pub fn has_rings(&self, poly: &AdminPolygon) -> bool {
        encoded_polygon(&self.vertices, poly.vertex_offset, poly.vertex_count).is_some()
    }

    /// Whether `poly` holds `p`: inside its outer ring and outside its holes, false without rings.
    /// `memo` keeps the answers for `p`, so it must only ever see `p` and this geometry.
    pub fn contains(&self, poly: &AdminPolygon, p: AdminPoint, memo: &mut PipMemo) -> bool {
        if poly.vertex_count == 0 || poly.vertex_offset == NO_DATA {
            return false;
        }

        memo.get_or_insert_with(Self::block_key(poly), || self.contains_once(poly, p))
    }

    /// `contains` without a memo, for callers that test no ring block twice at `p`.
    pub fn contains_once(&self, poly: &AdminPolygon, p: AdminPoint) -> bool {
        point_in_encoded_polygon(&self.vertices, poly.vertex_offset, poly.vertex_count, p.lat, p.lng).unwrap_or(false)
    }

    /// `poly`'s rings for GeoJSON: the outer ring, then its holes, each closed, as [lng, lat]
    /// degrees; none without rings.
    pub fn rings(&self, poly: &AdminPolygon) -> Vec<Vec<[f64; 2]>> {
        self.vertex_range(poly).map(|v| geojson_rings(&v, poly.hole_count)).unwrap_or_default()
    }

    /// `poly`'s point on surface (`point_on_surface`, the builder's interior_point) as (lat, lng);
    /// none without rings.
    pub fn point_on_surface(&self, poly: &AdminPolygon) -> Option<(f32, f32)> {
        self.vertex_range(poly).and_then(|v| point_on_surface(&v, poly.hole_count))
    }

    /// Whether `poly`'s point on surface lies inside `other`, not on its edges
    /// (`point_on_surface_in`); false when either has no rings.
    pub fn point_on_surface_in(&self, poly: &AdminPolygon, other: &AdminPolygon) -> bool {
        match (self.vertex_range(poly), self.vertex_range(other)) {
            (Some(verts), Some(other_verts)) => point_on_surface_in(&verts, poly.hole_count, &other_verts, other.hole_count),
            _ => false,
        }
    }

    /// `poly`'s stored vertex range: the outer ring, then each hole as a keyhole excursion
    /// (builder polygon_rings.h).
    fn vertex_range(&self, poly: &AdminPolygon) -> Option<Cow<'_, [NodeCoord]>> {
        decode_polygon_verts(&self.vertices, poly.vertex_offset, poly.vertex_count)
    }

    /// Hints that `poly`'s rings are read soon. Never changes an answer.
    pub fn prefetch(&self, poly: &AdminPolygon) {
        if poly.vertex_offset != NO_DATA {
            self.vertices.prefetch(poly.vertex_offset as u64);
        }
    }

    /// The memo key of `poly`'s ring block, whose answer depends on nothing else. Never all ones:
    /// `contains` keys no block at offset NO_DATA.
    fn block_key(poly: &AdminPolygon) -> u64 {
        (poly.vertex_offset as u64) << 32 | poly.vertex_count as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoded_pip_tests::{pack_ring, Rng};

    /// A polygon record whose ring block is `count` vertices at byte `offset`.
    fn record(offset: u32, count: u32, hole_count: u16) -> AdminPolygon {
        AdminPolygon {
            vertex_offset: offset, vertex_count: count, name_id: 0, admin_level: 8, place_type_override: 0,
            _pad2: 0, _pad3: 0, area: 1.0, country_code: 0, hole_count,
        }
    }

    /// The vertex ranges packed back to back at encoding `enc`, each with its hole count, with
    /// their records.
    fn packed(enc: u8, ranges: &[(Vec<(f64, f64)>, u16)]) -> (AdminGeometry, Vec<AdminPolygon>) {
        let mut bytes = Vec::new();
        let mut polys = Vec::new();
        for (range, holes) in ranges {
            polys.push(record(bytes.len() as u32, range.len() as u32, *holes));
            bytes.extend(pack_ring(enc, range));
        }
        (AdminGeometry::new(FileBytes::Owned(bytes)), polys)
    }

    /// The rings packed back to back at encoding 3, with their records.
    fn geometry(rings: &[Vec<(f64, f64)>]) -> (AdminGeometry, Vec<AdminPolygon>) {
        packed(3, &rings.iter().map(|r| (r.clone(), 0)).collect::<Vec<_>>())
    }

    fn square(lat0: f64, lng0: f64, side: f64) -> Vec<(f64, f64)> {
        vec![(lat0, lng0), (lat0, lng0 + side), (lat0 + side, lng0 + side), (lat0 + side, lng0), (lat0, lng0)]
    }

    /// `outer`'s keyhole range with `hole`, reached from the outer ring's first vertex through
    /// `connector` (polygon_rings.h).
    fn keyhole(outer: &[(f64, f64)], connector: (f64, f64), hole: &[(f64, f64)]) -> Vec<(f64, f64)> {
        outer.iter().copied().chain([outer[0], connector]).chain(hole.iter().copied()).chain([connector]).collect()
    }

    #[test]
    fn contains_is_the_encoded_ray_cast() {
        let rings: Vec<_> = (0..20).map(|i| square(i as f64, 2.0 * i as f64, 1.5)).collect();
        let (geom, polys) = geometry(&rings);
        let mut rng = Rng::new(20261027);
        for _ in 0..500 {
            let (lat, lng) = (rng.range(-1.0, 22.0), rng.range(-1.0, 42.0));
            let (p, mut memo) = (AdminPoint::new(lat, lng), PipMemo::new());
            for poly in &polys {
                let want = point_in_encoded_polygon(&geom.vertices, poly.vertex_offset, poly.vertex_count, lat as f32, lng as f32);
                assert_eq!(geom.contains(poly, p, &mut memo), want.unwrap(), "{lat} {lng}");
                assert_eq!(geom.contains_once(poly, p), want.unwrap(), "{lat} {lng}");
            }
        }
    }

    #[test]
    fn contains_is_false_without_rings() {
        let (geom, polys) = geometry(&[square(0.0, 0.0, 1.0)]);
        let p = AdminPoint::new(0.5, 0.5);
        let mut memo = PipMemo::new();
        assert!(geom.contains(&polys[0], p, &mut memo));
        for vertexless in [record(0, 0, 0), record(NO_DATA, 5, 0), record(NO_DATA, 0, 0), record(1000, 5, 0)] {
            assert!(!geom.contains(&vertexless, p, &mut memo));
            assert!(!geom.contains_once(&vertexless, p));
            assert!(!geom.has_rings(&vertexless));
            assert!(geom.rings(&vertexless).is_empty());
            assert!(geom.vertex_range(&vertexless).is_none());
        }
        assert!(geom.has_rings(&polys[0]));
    }

    #[test]
    fn memo_answers_each_ring_block_once() {
        let mut memo = PipMemo::new();
        assert!(memo.slots.is_empty());
        assert!(memo.get_or_insert_with(7, || true));
        // Asked again, the first answer stands: nothing is tested.
        assert!(memo.get_or_insert_with(7, || panic!("tested twice")));
        assert!(!memo.get_or_insert_with(8, || false));
        assert_eq!(memo.len, 2);
    }

    #[test]
    fn memo_keeps_its_answers_as_it_grows() {
        // 300 rings, every third holding the point: the memo grows from 64 slots past 512.
        let rings: Vec<_> = (0..300).map(|i| square(if i % 3 == 0 { -0.5 } else { 0.5 } + i as f64 * 1e-4, -0.5, 1.0)).collect();
        let (geom, polys) = geometry(&rings);
        let p = AdminPoint::new(0.25, 0.0);
        let mut memo = PipMemo::new();
        for pass in 0..2 {
            for (i, poly) in polys.iter().enumerate() {
                assert_eq!(geom.contains(poly, p, &mut memo), i % 3 == 0, "pass {pass} ring {i}");
            }
        }
        assert_eq!(memo.len, 300);
        assert!(memo.slots.len() >= 600);
    }

    #[test]
    fn rings_split_the_keyhole_range() {
        // A 10° square with a hole.
        let outer = square(0.0, 0.0, 10.0);
        let hole = vec![(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0), (4.0, 4.0)];
        let range = keyhole(&outer, (0.0, 4.0), &hole);
        let geom = AdminGeometry::new(FileBytes::Owned(pack_ring(4, &range)));
        let poly = record(0, range.len() as u32, 1);
        let lng_lat = |r: &[(f64, f64)]| r.iter().map(|&(lat, lng)| [lng, lat]).collect::<Vec<_>>();
        assert_eq!(geom.rings(&poly), vec![lng_lat(&outer), lng_lat(&hole)]);
        assert_eq!(geom.vertex_range(&poly).unwrap().len(), range.len());
        let mut memo = PipMemo::new();
        assert!(geom.contains(&poly, AdminPoint::new(2.0, 2.0), &mut memo));
        assert!(!geom.contains(&poly, AdminPoint::new(5.0, 5.0), &mut PipMemo::new()));
    }

    #[test]
    fn point_on_surface_reads_the_stored_rings() {
        // A 10° square with a hole from 4° to 6°, a square in the hole, one in the body and one
        // round them all, raw (encoding 4) so the stored vertices are the given ones.
        let holed = keyhole(&square(0.0, 0.0, 10.0), (0.0, 4.0), &square(4.0, 4.0, 2.0));
        let ranges = [(holed, 1), (square(4.5, 4.5, 1.0), 0), (square(1.0, 1.0, 1.0), 0), (square(-5.0, -5.0, 20.0), 0)];
        let (geom, polys) = packed(4, &ranges);
        let coords = |r: &[(f64, f64)]| r.iter().map(|&(lat, lng)| NodeCoord { lat: lat as f32, lng: lng as f32 }).collect::<Vec<_>>();

        for (poly, (range, holes)) in polys.iter().zip(&ranges) {
            assert_eq!(geom.point_on_surface(poly), point_on_surface(&coords(range), *holes));
            for (other, (other_range, other_holes)) in polys.iter().zip(&ranges) {
                let want = point_on_surface_in(&coords(range), *holes, &coords(other_range), *other_holes);
                assert_eq!(geom.point_on_surface_in(poly, other), want);
            }
        }
        // At lat 5 the runs either side of the hole are as wide; the first is taken.
        assert_eq!(geom.point_on_surface(&polys[0]), Some((5.0, 2.0)));
        // The squares in the hole and round it both have their point on surface in the hole.
        let within = |a: usize, b: usize| geom.point_on_surface_in(&polys[a], &polys[b]);
        assert_eq!([within(1, 0), within(2, 0), within(0, 3), within(3, 0)], [false, true, true, false]);
    }

    #[test]
    fn point_on_surface_is_none_without_rings() {
        let (geom, polys) = geometry(&[square(0.0, 0.0, 1.0)]);
        assert!(geom.point_on_surface_in(&polys[0], &polys[0]));
        for vertexless in [record(0, 0, 0), record(NO_DATA, 5, 0), record(1000, 5, 0)] {
            assert_eq!(geom.point_on_surface(&vertexless), None);
            assert!(!geom.point_on_surface_in(&vertexless, &polys[0]));
            assert!(!geom.point_on_surface_in(&polys[0], &vertexless));
        }
    }

    #[test]
    fn prefetch_accepts_any_record() {
        let (geom, polys) = geometry(&[square(0.0, 0.0, 1.0)]);
        for poly in [polys[0], record(NO_DATA, 0, 0), record(u32::MAX - 1, 5, 0)] {
            geom.prefetch(&poly);
        }
        assert!(geom.contains(&polys[0], AdminPoint::new(0.5, 0.5), &mut PipMemo::new()));
    }
}
