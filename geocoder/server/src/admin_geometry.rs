//! The admin polygons' geometry. Every admin point-in-polygon test and ring decode goes through
//! `AdminGeometry`, so how a region stores its rings is this module's business alone: each
//! polygon's ring block in admin_rings.bin, a list of refs to the arcs in arc_vertices.bin that
//! the neighbours along a border share (arc_blocks.rs).

use crate::arc_blocks::{
    arc_box_max_lat, arc_vertices, ring_block_contains, walk_rings, ArcFiles, ArcMemo, ArcRef, ChunkBox, RingVisitor,
    CHUNK_BOX_SLACK_E7,
};
use crate::exact_pip::{edge_crosses, on_edge, to_e7, PointE7};
use crate::{mmap_file, AdminPolygon, FileBytes, NO_DATA};
use std::sync::Mutex;

/// A point as every admin test takes it: rounded once to the e7 grid, as the builder rounds the
/// points it links (`to_e7`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdminPoint(PointE7);

impl AdminPoint {
    /// Beyond every vertex (|lng| <= 1.8e9 e7): a coordinate off the globe, or NaN, rounds to
    /// here, a point in no ring whose products with any vertex stay far inside i128.
    const LIMIT_E7: i64 = 1 << 32;

    pub fn new(lat: f64, lng: f64) -> Self {
        let e7 = |deg: f64| if deg.is_nan() { Self::LIMIT_E7 } else { to_e7(deg).clamp(-Self::LIMIT_E7, Self::LIMIT_E7) };
        AdminPoint(PointE7 { lat: e7(lat), lng: e7(lng) })
    }

    pub fn e7(self) -> PointE7 {
        self.0
    }

    /// (lat, lng) in degrees.
    pub fn degrees(self) -> (f64, f64) {
        self.0.degrees()
    }
}

/// `AdminGeometry::contains` answers for one point in one geometry: by ring block, and by arc
/// for the arcs read there.
#[derive(Default)]
pub struct PipMemo {
    blocks: BlockMemo,
    arcs: ArcMemo,
}

impl PipMemo {
    pub fn new() -> Self {
        Self::default()
    }

    /// How many arcs, and how many of their vertices, were read at this point.
    pub fn arcs_read(&self) -> u32 {
        self.arcs.arcs_read()
    }

    pub fn vertices_read(&self) -> u64 {
        self.arcs.vertices_read()
    }
}

/// Answers by ring block (open addressing, linear probing). Holds nothing until its first answer.
#[derive(Default)]
struct BlockMemo {
    slots: Vec<(u64, bool)>,
    len: usize,
}

impl BlockMemo {
    /// Marks a free slot; no ring block's key is all ones (`AdminGeometry::block_key`).
    const FREE: u64 = u64::MAX;
    const START_SLOTS: usize = 64;

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

/// A region's admin polygon geometry: each polygon's rings as a ring block in admin_rings.bin
/// (`AdminPolygon::ring_offset`, `ring_bytes`) of refs to arc slots, each slot's vertices a block
/// of arc_vertices.bin at the offset arc_index.bin gives it.
pub struct AdminGeometry {
    rings: FileBytes,
    arc_index: FileBytes,
    arc_vertices: FileBytes,
    surfaces: Mutex<SurfaceMemo>,
}

/// The points on surface `point_on_surface_in` found, by ring block: a direct-mapped table of
/// SLOTS entries (a block takes over its slot from the last one there), empty until its first
/// answer. A point on surface is two passes over every arc of a polygon, and the address walk
/// tests the same few large boundaries at point after point (a level-3 region as large as its
/// country, say). The answers depend on nothing but the files, which the geometry owns.
#[derive(Default)]
struct SurfaceMemo {
    slots: Vec<(u64, Option<(f64, f64)>)>,
}

impl SurfaceMemo {
    const SLOTS: usize = 256;
    /// Marks a free slot; no ring block's key is all ones (`AdminGeometry::block_key`).
    const FREE: u64 = u64::MAX;

    fn slot(key: u64) -> usize {
        // Fibonacci hashing; the top bits of the product mix every bit of the key.
        (key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - Self::SLOTS.trailing_zeros())) as usize
    }

    fn get(&self, key: u64) -> Option<Option<(f64, f64)>> {
        self.slots.get(Self::slot(key)).filter(|(k, _)| *k == key).map(|&(_, point)| point)
    }

    fn insert(&mut self, key: u64, point: Option<(f64, f64)>) {
        if self.slots.is_empty() {
            self.slots = vec![(Self::FREE, None); Self::SLOTS];
        }
        self.slots[Self::slot(key)] = (key, point);
    }
}

/// The files `AdminGeometry::load` maps, by name.
pub const ADMIN_GEOMETRY_FILES: [&str; 3] = ["admin_rings.bin", "arc_index.bin", "arc_vertices.bin"];

impl AdminGeometry {
    pub fn new(rings: FileBytes, arc_index: FileBytes, arc_vertices: FileBytes) -> Self {
        AdminGeometry { rings, arc_index, arc_vertices, surfaces: Mutex::default() }
    }

    /// The geometry files of the index in `dir`; the error names a file that is missing.
    pub fn load(dir: &str) -> Result<Self, String> {
        let [rings, arc_index, arc_vertices] = ADMIN_GEOMETRY_FILES.map(|name| mmap_file(&format!("{dir}/{name}")));
        Ok(AdminGeometry::new(rings?, arc_index?, arc_vertices?))
    }

    fn arcs(&self) -> ArcFiles<'_> {
        ArcFiles { index: self.arc_index.as_slice(), vertices: self.arc_vertices.as_slice() }
    }

    /// `poly`'s ring block; none for a tombstone, a polygon a tier left without rings, and a
    /// block past the end of the file.
    fn ring_block(&self, poly: &AdminPolygon) -> Option<&[u8]> {
        if poly.ring_offset == NO_DATA || poly.ring_bytes == 0 {
            return None;
        }
        let start = usize::try_from(poly.ring_offset).ok()?;
        self.rings.as_slice().get(start..start.checked_add(usize::try_from(poly.ring_bytes).ok()?)?)
    }

    /// Whether `poly` has rings: false for a tombstone, a polygon a tier left without rings, and
    /// one whose ring block lies past the end of the file.
    pub fn has_rings(&self, poly: &AdminPolygon) -> bool {
        self.ring_block(poly).is_some()
    }

    /// Whether `poly` holds `p`: inside its outer ring and outside its holes, false without rings
    /// or with a malformed ring block. `memo` keeps the answers for `p`, so it must only ever see
    /// `p` and this geometry.
    pub fn contains(&self, poly: &AdminPolygon, p: AdminPoint, memo: &mut PipMemo) -> bool {
        let Some(block) = self.ring_block(poly) else { return false };
        let PipMemo { blocks, arcs } = memo;
        blocks.get_or_insert_with(Self::block_key(poly), || {
            ring_block_contains(block, self.arcs(), p.e7(), Some(arcs)).unwrap_or(false)
        })
    }

    /// `contains` without a memo, for callers that test no ring block twice at `p`.
    pub fn contains_once(&self, poly: &AdminPolygon, p: AdminPoint) -> bool {
        self.ring_block(poly).and_then(|block| ring_block_contains(block, self.arcs(), p.e7(), None)).unwrap_or(false)
    }

    /// `poly`'s rings for GeoJSON: the outer ring, then its holes, each closed, as [lng, lat]
    /// degrees; none without rings or with a malformed ring block.
    pub fn rings(&self, poly: &AdminPolygon) -> Vec<Vec<[f64; 2]>> {
        let Some(block) = self.ring_block(poly) else { return Vec::new() };
        let mut joined = RingJoiner { arcs: self.arcs(), rings: Vec::new(), ring: Vec::new(), arc: Vec::new() };
        if walk_rings(block, &mut joined).is_none() {
            return Vec::new();
        }
        let lng_lat = |v: &PointE7| {
            let (lat, lng) = v.degrees();
            [lng, lat]
        };
        joined.rings.iter().map(|r| r.iter().map(lng_lat).collect()).collect()
    }

    /// `poly`'s point on surface (the builder's interior_point) as (lat, lng); none without rings.
    pub fn point_on_surface(&self, poly: &AdminPolygon) -> Option<(f32, f32)> {
        self.point_on_surface_with(poly, &mut Vec::new())
    }

    /// `point_on_surface`, its scan line's crossings gathered in `crossings`, which a caller
    /// finding many polygons' points reuses.
    pub fn point_on_surface_with(&self, poly: &AdminPolygon, crossings: &mut Vec<f64>) -> Option<(f32, f32)> {
        self.surface_point(self.ring_block(poly)?, crossings).map(|(lat, lng)| (lat as f32, lng as f32))
    }

    /// Whether `poly`'s point on surface lies inside `other`, not on its edges (ST_Contains):
    /// Nominatim's test of a boundary's centroid against the current boundary. False when either
    /// has no rings.
    pub fn point_on_surface_in(&self, poly: &AdminPolygon, other: &AdminPolygon) -> bool {
        let (Some(block), Some(other_block)) = (self.ring_block(poly), self.ring_block(other)) else { return false };
        let Some((lat, lng)) = self.memo_surface_point(poly, block) else { return false };
        let mut test = InsideOffEdges { arcs: self.arcs(), p: AdminPoint::new(lat, lng).e7(), odd: false, on_edge: false };
        walk_rings(other_block, &mut test).is_some() && test.odd && !test.on_edge
    }

    /// `surface_point` of `poly`'s ring block `block`, from the memo when it holds it.
    fn memo_surface_point(&self, poly: &AdminPolygon, block: &[u8]) -> Option<(f64, f64)> {
        let key = Self::block_key(poly);
        // A poisoned lock is the memo of a thread that panicked: its answers are still sound.
        let memo = || self.surfaces.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(point) = memo().get(key) {
            return point;
        }
        let point = self.surface_point(block, &mut Vec::new());
        memo().insert(key, point);
        point
    }

    /// Hints that `poly`'s rings are read soon. Never changes an answer.
    pub fn prefetch(&self, poly: &AdminPolygon) {
        if poly.ring_offset != NO_DATA {
            self.rings.prefetch(poly.ring_offset as u64);
        }
    }

    /// The memo key of `poly`'s ring block, whose answer depends on nothing else. Never all ones:
    /// `contains` keys no block at offset NO_DATA.
    fn block_key(poly: &AdminPolygon) -> u64 {
        (poly.ring_offset as u64) << 32 | poly.ring_bytes as u64
    }

    /// GEOS's InteriorPointArea (PostGIS ST_PointOnSurface, which Nominatim's get_center_point
    /// uses for areas), as the builder's interior_point finds it over the exact rings
    /// (polygon_rings.h): the midpoint of the widest inside stretch of the line of latitude
    /// half-way between the vertex latitudes of all rings nearest the middle of the outer ring's
    /// latitude range; the first vertex where no stretch has width. (lat, lng) in degrees.
    ///
    /// No bound in the files holds a ring's lowest latitude, and a whole exact country ring is
    /// too large to hold, so it streams the arcs twice: the outer ring's latitude range, then the
    /// latitudes nearest its middle together with the edges that can cross the scan line.
    fn surface_point(&self, block: &[u8], crossings: &mut Vec<f64>) -> Option<(f64, f64)> {
        let arcs = self.arcs();

        let mut outer = OuterRange { arcs, lo: i64::MAX, hi: i64::MIN, first: None, done: false };
        walk_rings(block, &mut outer)?;
        let first = outer.first?;
        let (lo, hi) = (deg(outer.lo), deg(outer.hi));
        let centre = (lo + hi) / 2.0;

        // The scan line lies strictly between lo, at or below the centre, and hi, above it, and
        // no vertex lies between those: an edge with both ends at or below the centre (so at or
        // below lo) or both above it (at or above hi) doesn't reach the scan line. The edges that
        // cross the centre's line are kept, as (y0, x0, y1, x1) in their ring's direction.
        crossings.clear();
        let mut near = NearCentre { arcs, centre, lo, hi, edges: crossings };
        walk_rings(block, &mut near)?;
        let scan = (near.lo + near.hi) / 2.0;

        // Each kept edge's crossing of the scan line, if any, in place of the edges.
        let mut found = 0;
        for k in 0..crossings.len() / 4 {
            let (y0, x0, y1, x1) = (crossings[4 * k], crossings[4 * k + 1], crossings[4 * k + 2], crossings[4 * k + 3]);
            if (y0 > scan && y1 > scan) || (y0 < scan && y1 < scan) || y0 == y1 { continue; }
            if y0 == scan && y1 < scan { continue; }  // a downward edge leaves out its start
            if y1 == scan && y0 < scan { continue; }  // an upward edge leaves out its end
            crossings[found] = if x0 == x1 { x0 } else { x0 + (scan - y0) / ((y1 - y0) / (x1 - x0)) };
            found += 1;
        }
        crossings.truncate(found);
        crossings.sort_by(f64::total_cmp);

        let mut best = first.degrees();
        let mut best_width = 0.0;
        for run in crossings.chunks_exact(2) {
            if run[1] - run[0] > best_width {
                best_width = run[1] - run[0];
                best = (scan, (run[0] + run[1]) / 2.0);
            }
        }
        Some(best)
    }
}

/// Joins each ring's arcs, reversed ones flipped, each junction vertex once.
struct RingJoiner<'a> {
    arcs: ArcFiles<'a>,
    rings: Vec<Vec<PointE7>>,
    ring: Vec<PointE7>,
    arc: Vec<PointE7>,
}

impl RingVisitor for RingJoiner<'_> {
    fn wants_chunk(&mut self, _: &ChunkBox) -> bool {
        true
    }

    fn arc(&mut self, r: ArcRef) -> Option<()> {
        let arc = &mut self.arc;
        arc.clear();
        arc_vertices(self.arcs.block(r.slot)?, |v| arc.push(v))?;
        if r.reversed {
            arc.reverse();
        }
        let joined = usize::from(self.ring.last().is_some() && self.ring.last() == arc.first());
        self.ring.extend_from_slice(&arc[joined..]);
        Some(())
    }

    fn ring_end(&mut self) -> Option<()> {
        if self.ring.first() != self.ring.last() {
            self.ring.push(self.ring[0]);
        }
        self.rings.push(std::mem::take(&mut self.ring));
        Some(())
    }
}

/// The latitude range of a ring block's first ring, the outer one, and its first vertex.
struct OuterRange<'a> {
    arcs: ArcFiles<'a>,
    lo: i64,
    hi: i64,
    first: Option<PointE7>,
    done: bool,
}

impl RingVisitor for OuterRange<'_> {
    fn wants_chunk(&mut self, _: &ChunkBox) -> bool {
        !self.done
    }

    fn arc(&mut self, r: ArcRef) -> Option<()> {
        if self.done {
            return Some(());
        }
        let (mut lo, mut hi, mut first, mut last) = (self.lo, self.hi, None, None);
        arc_vertices(self.arcs.block(r.slot)?, |v| {
            lo = lo.min(v.lat);
            hi = hi.max(v.lat);
            first.get_or_insert(v);
            last = Some(v);
        })?;
        (self.lo, self.hi) = (lo, hi);
        if self.first.is_none() {
            self.first = if r.reversed { last } else { first };
        }
        Some(())
    }

    fn ring_end(&mut self) -> Option<()> {
        self.done = true;
        Some(())
    }
}

/// An e7 coordinate in degrees, as the builder reads a PackedLocation.
fn deg(e7: i64) -> f64 {
    e7 as f64 / 1e7
}

/// `surface_point`'s second pass over every ring: the vertex latitudes nearest `centre`, at or
/// below it (`lo`, raised from the outer ring's lowest) and above it (`hi`, lowered from its
/// highest), and the edges crossing the centre's line of latitude, as (y0, x0, y1, x1) in their
/// ring's direction (every vertex starts an edge of its ring: the closing one ends where the ring
/// began). A chunk or an arc whose vertices all lie at or below `lo` can neither move `lo` or `hi`
/// nor cross the centre's line, so it isn't read.
struct NearCentre<'a, 'e> {
    arcs: ArcFiles<'a>,
    centre: f64,
    lo: f64,
    hi: f64,
    edges: &'e mut Vec<f64>,
}

impl RingVisitor for NearCentre<'_, '_> {
    fn wants_chunk(&mut self, bbox: &ChunkBox) -> bool {
        deg(bbox.max_lat as i64 + CHUNK_BOX_SLACK_E7) > self.lo
    }

    fn arc(&mut self, r: ArcRef) -> Option<()> {
        let block = self.arcs.block(r.slot)?;
        if arc_box_max_lat(block).is_some_and(|max| deg(max) <= self.lo) {
            return Some(());
        }
        let NearCentre { centre, lo, hi, edges, .. } = self;
        let mut prev: Option<PointE7> = None;
        arc_vertices(block, |v| {
            if let Some(p) = prev {
                let (a, b) = if r.reversed { (v, p) } else { (p, v) };
                let y = deg(a.lat);
                if y <= *centre {
                    if y > *lo { *lo = y; }
                } else if y < *hi {
                    *hi = y;
                }
                if (y <= *centre) != (deg(b.lat) <= *centre) {
                    edges.extend([y, deg(a.lng), deg(b.lat), deg(b.lng)]);
                }
            }
            prev = Some(v);
        })?;
        Some(())
    }
}

/// Whether the ray from `p` crosses a ring block's edges an odd number of times, and whether `p`
/// lies on one of them.
struct InsideOffEdges<'a> {
    arcs: ArcFiles<'a>,
    p: PointE7,
    odd: bool,
    on_edge: bool,
}

impl RingVisitor for InsideOffEdges<'_> {
    fn wants_chunk(&mut self, bbox: &ChunkBox) -> bool {
        !self.on_edge && !bbox.excludes_on_edge(self.p)
    }

    fn arc(&mut self, r: ArcRef) -> Option<()> {
        let (p, mut prev) = (self.p, None::<PointE7>);
        let (mut odd, mut touches) = (false, false);
        arc_vertices(self.arcs.block(r.slot)?, |v| {
            if let Some(a) = prev {
                odd ^= edge_crosses(a, v, p);
                touches |= on_edge(a, v, p);
            }
            prev = Some(v);
        })?;
        self.odd ^= odd;
        self.on_edge |= touches;
        Some(())
    }
}

#[cfg(test)]
mod tests;
