//! The admin polygons' geometry. Every admin point-in-polygon test and ring decode goes through
//! `AdminGeometry`, so how a region stores its rings is this module's business alone: each
//! polygon's ring block in admin_rings.bin, a list of refs to the arcs in arc_vertices.bin that
//! the neighbours along a border share (arc_blocks.rs).

use crate::arc_blocks::{
    arc_box, arc_vertices, ring_block_contains, walk_rings, ArcFiles, ArcMemo, ArcRef, ChunkBox, RingVisitor,
    CHUNK_BOX_SLACK_E7,
};
use crate::exact_pip::{edge_crosses, on_edge, to_e7, PointE7};
use crate::{mmap_file, AdminPolygon, FileBytes};
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
    insides: Mutex<InsideMemo>,
    withins: Mutex<InsideMemo>,
}

/// The answers `point_on_surface_in` gave, by the two ring blocks: a direct-mapped table of
/// SLOTS entries like SurfaceMemo's. The address walk asks the same few pairs again and again,
/// and each answer is a ray cast over the other polygon's arcs besides the point on surface.
#[derive(Default)]
struct InsideMemo {
    slots: Vec<(u64, u64, bool)>,
}

impl InsideMemo {
    const SLOTS: usize = 256;

    fn slot(poly: u64, other: u64) -> usize {
        ((poly ^ other.rotate_left(32)).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - Self::SLOTS.trailing_zeros())) as usize
    }

    fn get(&self, poly: u64, other: u64) -> Option<bool> {
        self.slots.get(Self::slot(poly, other)).filter(|&&(p, o, _)| (p, o) == (poly, other)).map(|&(_, _, inside)| inside)
    }

    fn insert(&mut self, poly: u64, other: u64, inside: bool) {
        if self.slots.is_empty() {
            self.slots = vec![(SurfaceMemo::FREE, SurfaceMemo::FREE, false); Self::SLOTS];
        }
        self.slots[Self::slot(poly, other)] = (poly, other, inside);
    }
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

/// How far apart, in degrees, the centroids of one outline can lie (`AdminGeometry::same_outline`):
/// a tenth of a millimetre.
const OUTLINE_SLACK_DEG: f64 = 1e-9;

/// The files `AdminGeometry::load` maps, by name.
pub const ADMIN_GEOMETRY_FILES: [&str; 3] = ["admin_rings.bin", "arc_index.bin", "arc_vertices.bin"];

impl AdminGeometry {
    pub fn new(rings: FileBytes, arc_index: FileBytes, arc_vertices: FileBytes) -> Self {
        AdminGeometry {
            rings, arc_index, arc_vertices, surfaces: Mutex::default(), insides: Mutex::default(), withins: Mutex::default(),
        }
    }

    /// The geometry files of the index in `dir`; the error names a file that is missing.
    pub fn load(dir: &str) -> Result<Self, String> {
        let [rings, arc_index, arc_vertices] = ADMIN_GEOMETRY_FILES.map(|name| mmap_file(&format!("{dir}/{name}")));
        Ok(AdminGeometry::new(rings?, arc_index?, arc_vertices?))
    }

    /// admin_rings.bin's length, where the last polygon's ring block ends.
    pub fn rings_len(&self) -> u64 {
        self.rings.len()
    }

    fn arcs(&self) -> ArcFiles<'_> {
        ArcFiles { index: self.arc_index.as_slice(), vertices: self.arc_vertices.as_slice() }
    }

    /// `poly`'s ring block; none for a tombstone, a polygon a tier left without rings, and a
    /// block past the end of the file.
    fn ring_block(&self, poly: &AdminPolygon) -> Option<&[u8]> {
        if poly.ring_bytes == 0 {
            return None;
        }
        // The format's u32 offsets address 4 GiB of rings: no block ends past that.
        let end = poly.ring_offset.checked_add(poly.ring_bytes)?;
        self.rings.as_slice().get(usize::try_from(poly.ring_offset).ok()?..usize::try_from(end).ok()?)
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

    /// `poly`'s centroid as (lat, lng) degrees: ST_Centroid of its rings on lon/lat, the outer
    /// ring's centre weighted by its area less its holes'. None without rings, with a malformed
    /// ring block or of no area.
    pub fn centroid(&self, poly: &AdminPolygon) -> Option<(f64, f64)> {
        let block = self.ring_block(poly)?;
        let mut c = Centroid { arcs: self.arcs(), origin: None, ring: RingSums::default(), ends: None, rings: 0, sums: RingSums::default() };
        walk_rings(block, &mut c)?;
        let (lng0, lat0) = c.origin?;
        (c.sums.area > 0.0).then(|| (lat0 + c.sums.y / c.sums.area, lng0 + c.sums.x / c.sums.area))
    }

    /// Whether `a` and `b` have one outline: the same area, and the same ring block or centroids
    /// within OUTLINE_SLACK_DEG of each other, the most the sums' rounding moves one when its
    /// rings start at another vertex. False when either has no rings.
    pub fn same_outline(&self, a: &AdminPolygon, b: &AdminPolygon) -> bool {
        if a.area != b.area {
            return false;
        }
        let (Some(block_a), Some(block_b)) = (self.ring_block(a), self.ring_block(b)) else { return false };
        if block_a == block_b {
            return true;
        }
        let near = |p: f64, q: f64| (p - q).abs() <= OUTLINE_SLACK_DEG;
        matches!((self.centroid(a), self.centroid(b)), (Some(p), Some(q)) if near(p.0, q.0) && near(p.1, q.1))
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
        let (poly_key, other_key) = (Self::block_key(poly), Self::block_key(other));
        // A poisoned lock is the memo of a thread that panicked: its answers are still sound.
        let memo = || self.insides.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(inside) = memo().get(poly_key, other_key) {
            return inside;
        }
        let inside = self.memo_surface_point(poly, block).is_some_and(|(lat, lng)| {
            let mut test = InsideOffEdges { arcs: self.arcs(), p: AdminPoint::new(lat, lng).e7(), odd: false, on_edge: false };
            walk_rings(other_block, &mut test).is_some() && test.odd && !test.on_edge
        });
        memo().insert(poly_key, other_key, inside);
        inside
    }

    /// Whether `poly` lies within `other`, as Nominatim's ST_Contains(other, poly) of a place
    /// area or a boundary: every vertex of its rings inside `other` or on its edges, none of its
    /// edges crossing one of `other`'s, no point of `other`'s edges inside it off its edges (as
    /// of a hole of `other` that it lies round, or of a notch in `other` that it spans), and its
    /// point on surface inside `other` off its edges (not so for one filling a hole of `other`).
    /// Areas nested along shared ways meet on edges alone. `poly`'s rings are read once and
    /// held, and `other`'s once, but for the chunks and arcs south of `poly` or beside it. False
    /// when either has no rings.
    pub fn polygon_within(&self, poly: &AdminPolygon, other: &AdminPolygon) -> bool {
        let (Some(block), Some(other_block)) = (self.ring_block(poly), self.ring_block(other)) else { return false };
        let (poly_key, other_key) = (Self::block_key(poly), Self::block_key(other));
        // A poisoned lock is the memo of a thread that panicked: its answers are still sound.
        let memo = || self.withins.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(within) = memo().get(poly_key, other_key) {
            return within;
        }
        let within = self.memo_surface_point(poly, block)
            .and_then(|(lat, lng)| HeldRings::of(block, self.arcs(), AdminPoint::new(lat, lng).e7()))
            .is_some_and(|held| held.within(self.arcs(), other_block));
        memo().insert(poly_key, other_key, within);
        within
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
        if poly.ring_bytes != 0 {
            self.rings.prefetch(poly.ring_offset as u64);
        }
    }

    /// The memo key of `poly`'s ring block, whose answer depends on nothing else. Never all ones:
    /// a block ends within the 4 GiB its u32 offset addresses (`ring_block`), so one at offset
    /// u32::MAX is empty, and an empty block has no key.
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
/// A ring's shoelace sums over its edges, in degrees from the block's first vertex: twice its
/// signed area (the sum of each edge's cross product) and the sums of (x_i + x_i+1) and
/// (y_i + y_i+1) times each cross product, which over 3 times the first sum give its centre.
#[derive(Clone, Copy, Default)]
struct RingSums {
    area: f64,
    x: f64,
    y: f64,
}

impl RingSums {
    fn edge(&mut self, a: (f64, f64), b: (f64, f64)) {
        let cross = a.0 * b.1 - b.0 * a.1;
        self.area += cross;
        self.x += (a.0 + b.0) * cross;
        self.y += (a.1 + b.1) * cross;
    }
}

/// AdminGeometry::centroid's walk: each ring's sums, and over all rings each ring's centre times
/// twice its area, added for the outer ring and taken off for a hole, and twice the area left.
struct Centroid<'a> {
    arcs: ArcFiles<'a>,
    /// The first vertex, as (lng, lat) degrees, which the sums are taken from.
    origin: Option<(f64, f64)>,
    ring: RingSums,
    /// The ring's first and last vertex so far, in ring order.
    ends: Option<((f64, f64), (f64, f64))>,
    rings: usize,
    sums: RingSums,
}

impl RingVisitor for Centroid<'_> {
    fn wants_chunk(&mut self, _: &ChunkBox) -> bool {
        true
    }

    fn arc(&mut self, r: ArcRef) -> Option<()> {
        // An arc walked backwards crosses each of its edges the other way: its sums change sign.
        let (mut sums, mut first, mut prev) = (RingSums::default(), None, None::<(f64, f64)>);
        let origin = &mut self.origin;
        arc_vertices(self.arcs.block(r.slot)?, |v| {
            let (lat, lng) = v.degrees();
            let (lng0, lat0) = *origin.get_or_insert((lng, lat));
            let p = (lng - lng0, lat - lat0);
            if let Some(a) = prev {
                sums.edge(a, p);
            }
            first.get_or_insert(p);
            prev = Some(p);
        })?;
        let (Some(first), Some(last)) = (first, prev) else { return Some(()) };
        let (sign, from, to) = if r.reversed { (-1.0, last, first) } else { (1.0, first, last) };
        self.ring.area += sign * sums.area;
        self.ring.x += sign * sums.x;
        self.ring.y += sign * sums.y;
        self.ends = Some((self.ends.map_or(from, |(start, _)| start), to));
        Some(())
    }

    fn ring_end(&mut self) -> Option<()> {
        let mut ring = std::mem::take(&mut self.ring);
        if let Some((start, end)) = self.ends.take() {
            ring.edge(end, start);
        }
        if ring.area != 0.0 {
            let (weight, sign) = (ring.area.abs(), if self.rings == 0 { 1.0 } else { -1.0 });
            self.sums.area += sign * weight;
            self.sums.x += sign * weight * ring.x / (3.0 * ring.area);
            self.sums.y += sign * weight * ring.y / (3.0 * ring.area);
        }
        self.rings += 1;
        Some(())
    }
}

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
        if arc_box(block).is_some_and(|[_, _, max_lat]| deg(max_lat) <= self.lo) {
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

/// What the edges of another polygon read so far do at a point: whether the ray from it towards
/// +lat crosses an odd number of them, and whether it lies on one.
#[derive(Clone, Copy)]
struct Probe {
    p: PointE7,
    odd: bool,
    on_edge: bool,
}

impl Probe {
    fn at(p: PointE7) -> Self {
        Probe { p, odd: false, on_edge: false }
    }

    /// Reads the edge a-b; whether the point lies on it.
    fn read(&mut self, a: PointE7, b: PointE7) -> bool {
        let on = on_edge(a, b, self.p);
        self.odd ^= edge_crosses(a, b, self.p);
        self.on_edge |= on;
        on
    }
}

/// Whether the edges a-b and c-d cross at a point inside both: each one's ends lie on either
/// side of the other's line, off it.
fn cross_inside(a: PointE7, b: PointE7, c: PointE7, d: PointE7) -> bool {
    // The ends are within i32, as the arc decoder gives them: a difference takes 33 bits.
    let side = |p: PointE7, q: PointE7, r: PointE7| {
        ((q.lng - p.lng) as i128 * (r.lat - p.lat) as i128 - (q.lat - p.lat) as i128 * (r.lng - p.lng) as i128).signum()
    };
    side(a, b, c) * side(a, b, d) < 0 && side(c, d, a) * side(c, d, b) < 0
}

/// Twice the coordinates of `p`: there the middle of two e7 points is on the grid.
fn twice(p: PointE7) -> PointE7 {
    PointE7 { lat: 2 * p.lat, lng: 2 * p.lng }
}

/// A polygon's rings held whole for AdminGeometry::polygon_within: its vertices, each once and by
/// longitude, and its point on surface, as probes of the other polygon's edges; and its edges,
/// each listed in every strip of longitude its own longitudes meet, so that a point or an edge
/// of the other polygon is tested against the edges of its strips alone.
struct HeldRings {
    probes: Vec<Probe>,
    /// The probes on the edge read last but for its ends, which cut it into pieces.
    cuts: Vec<PointE7>,
    surface: Probe,
    edges: Vec<(PointE7, PointE7)>,
    /// The edges meeting strip s are edges[listed[k]] for k in starts[s]..starts[s + 1].
    starts: Vec<u32>,
    listed: Vec<u32>,
    /// The corners of the box of the vertices and the point on surface.
    lo: PointE7,
    hi: PointE7,
    strip_width: i64,
}

impl HeldRings {
    /// The strips: one for about EDGES_PER_STRIP edges, at most MAX_STRIPS, and fewer where the
    /// lists would hold more than LISTED_PER_EDGE entries an edge, as long edges make them.
    const EDGES_PER_STRIP: usize = 4;
    const MAX_STRIPS: usize = 1 << 14;
    const LISTED_PER_EDGE: usize = 8;

    /// The rings of ring block `block`, with `surface` its point on surface; none for a malformed
    /// block or one without vertices.
    fn of(block: &[u8], arcs: ArcFiles<'_>, surface: PointE7) -> Option<HeldRings> {
        let mut read = EdgeReader { arcs, edges: Vec::new(), vertices: Vec::new() };
        walk_rings(block, &mut read)?;
        let EdgeReader { edges, mut vertices, .. } = read;
        vertices.sort_unstable_by_key(|v| (v.lng, v.lat));
        vertices.dedup();
        let (mut lo, mut hi) = (surface, surface);
        for v in &vertices {
            (lo.lat, lo.lng, hi.lat, hi.lng) = (lo.lat.min(v.lat), lo.lng.min(v.lng), hi.lat.max(v.lat), hi.lng.max(v.lng));
        }
        if vertices.is_empty() {
            return None;
        }

        let mut strips = (edges.len() / Self::EDGES_PER_STRIP).clamp(1, Self::MAX_STRIPS);
        let (strip_width, spans) = loop {
            let width = (hi.lng - lo.lng) / strips as i64 + 1;
            let strip = |lng: i64| ((lng - lo.lng) / width) as usize;
            let spans: Vec<(usize, usize)> = edges.iter().map(|(a, b)| (strip(a.lng.min(b.lng)), strip(a.lng.max(b.lng)))).collect();
            let listed: usize = spans.iter().map(|&(s0, s1)| s1 - s0 + 1).sum();
            if strips == 1 || listed <= Self::LISTED_PER_EDGE * edges.len() {
                break (width, spans);
            }
            strips /= 2;
        };
        let mut starts = vec![0u32; strips + 1];
        for &(s0, s1) in &spans {
            for s in s0..=s1 {
                starts[s + 1] += 1;
            }
        }
        for s in 0..strips {
            starts[s + 1] += starts[s];
        }
        let mut next = starts.clone();
        let mut listed = vec![0u32; starts[strips] as usize];
        for (k, &(s0, s1)) in spans.iter().enumerate() {
            for s in s0..=s1 {
                listed[next[s] as usize] = k as u32;
                next[s] += 1;
            }
        }
        let probes = vertices.into_iter().map(Probe::at).collect();
        Some(HeldRings { probes, cuts: Vec::new(), surface: Probe::at(surface), edges, starts, listed, lo, hi, strip_width })
    }

    /// The edges listed in the strips from the one of longitude `lng0` to the one of `lng1`
    /// (lng0 <= lng1), an edge once a strip.
    fn edges_between(&self, lng0: i64, lng1: i64) -> impl Iterator<Item = (PointE7, PointE7)> + '_ {
        let strip = |lng: i64| ((lng.clamp(self.lo.lng, self.hi.lng) - self.lo.lng) / self.strip_width) as usize;
        let (first, end) = (self.starts[strip(lng0)] as usize, self.starts[strip(lng1) + 1] as usize);
        self.listed[first..end].iter().map(|&k| self.edges[k as usize])
    }

    /// Whether every point of a box from longitude `min_lng` to `max_lng` and up to latitude
    /// `max_lat` lies south of the held probes or east or west of them: none of its edges
    /// crosses the ray from a probe, holds one, or meets an edge of the rings.
    fn keeps_off(&self, min_lng: i64, max_lng: i64, max_lat: i64) -> bool {
        max_lat < self.lo.lat || min_lng > self.hi.lng || max_lng < self.lo.lng
    }

    /// Whether the point of twice the coordinates `p2` lies inside the rings, off their edges.
    /// (The ray cast's products stay far inside i128 at twice the coordinates.)
    fn holds_off_edges(&self, p2: PointE7) -> bool {
        let (lo, hi) = (twice(self.lo), twice(self.hi));
        if !(lo.lat < p2.lat && p2.lat < hi.lat && lo.lng < p2.lng && p2.lng < hi.lng) {
            return false;
        }
        // An edge spanning a longitude halfway between two of the grid spans the lower one.
        let lng = p2.lng.div_euclid(2);
        let mut odd = false;
        for (a, b) in self.edges_between(lng, lng) {
            let (a, b) = (twice(a), twice(b));
            if on_edge(a, b, p2) {
                return false;
            }
            odd ^= edge_crosses(a, b, p2);
        }
        odd
    }

    /// Reads the other polygon's edge a-b into every probe whose longitude it spans; whether it
    /// leaves some of the rings outside: it crosses an edge of theirs at a point inside both, or
    /// a piece of it runs inside them off their edges. Cut at the vertices of the rings on it, a
    /// piece meets their edges at its ends alone, so it lies inside them where its middle does.
    fn read_edge(&mut self, a: PointE7, b: PointE7) -> bool {
        let (lng0, lng1) = (a.lng.min(b.lng), a.lng.max(b.lng));
        let first = self.probes.partition_point(|q| q.p.lng < lng0);
        let mut cuts = std::mem::take(&mut self.cuts);
        cuts.clear();
        for q in self.probes[first..].iter_mut().take_while(|q| q.p.lng <= lng1) {
            if q.read(a, b) && q.p != a && q.p != b {
                cuts.push(q.p);
            }
        }
        self.surface.read(a, b);
        let outside = !(a.lat.max(b.lat) <= self.lo.lat || a.lat.min(b.lat) >= self.hi.lat || lng1 <= self.lo.lng || lng0 >= self.hi.lng)
            && (self.edges_between(lng0, lng1).any(|(c, d)| cross_inside(a, b, c, d)) || {
                let along = |v: &PointE7| (v.lat - a.lat) as i128 * (b.lat - a.lat) as i128 + (v.lng - a.lng) as i128 * (b.lng - a.lng) as i128;
                cuts.sort_unstable_by_key(along);
                let mut from = a;
                cuts.iter().chain([&b]).any(|&to| {
                    let middle = PointE7 { lat: from.lat + to.lat, lng: from.lng + to.lng };
                    from = to;
                    self.holds_off_edges(middle)
                })
            });
        self.cuts = cuts;
        outside
    }

    /// Whether the rings lie within the polygon whose ring block is `other`, read through `arcs`.
    fn within(self, arcs: ArcFiles<'_>, other: &[u8]) -> bool {
        let mut walk = WithinWalk { arcs, held: self, outside: false };
        if walk_rings(other, &mut walk).is_none() || walk.outside {
            return false;
        }
        let HeldRings { probes, surface, .. } = &walk.held;
        probes.iter().all(|q| q.odd || q.on_edge) && surface.odd && !surface.on_edge
    }
}

/// A ring block's edges, each arc's in the order it stores them, and their vertices.
struct EdgeReader<'a> {
    arcs: ArcFiles<'a>,
    edges: Vec<(PointE7, PointE7)>,
    vertices: Vec<PointE7>,
}

impl RingVisitor for EdgeReader<'_> {
    fn wants_chunk(&mut self, _: &ChunkBox) -> bool {
        true
    }

    fn arc(&mut self, r: ArcRef) -> Option<()> {
        let EdgeReader { arcs, edges, vertices } = self;
        let mut prev: Option<PointE7> = None;
        arc_vertices(arcs.block(r.slot)?, |v| {
            if let Some(a) = prev.filter(|&a| a != v) {
                edges.push((a, v));
            }
            vertices.push(v);
            prev = Some(v);
        })?;
        Some(())
    }
}

/// AdminGeometry::polygon_within's walk of the other polygon's rings: each edge read into the
/// held rings, the walk stopped at the first that leaves some of them outside.
struct WithinWalk<'a> {
    arcs: ArcFiles<'a>,
    held: HeldRings,
    outside: bool,
}

impl RingVisitor for WithinWalk<'_> {
    fn wants_chunk(&mut self, bbox: &ChunkBox) -> bool {
        let slack = CHUNK_BOX_SLACK_E7;
        !self.held.keeps_off(bbox.min_lng as i64 - slack, bbox.max_lng as i64 + slack, bbox.max_lat as i64 + slack)
    }

    fn arc(&mut self, r: ArcRef) -> Option<()> {
        let block = self.arcs.block(r.slot)?;
        if arc_box(block).is_some_and(|[min_lng, max_lng, max_lat]| self.held.keeps_off(min_lng, max_lng, max_lat)) {
            return Some(());
        }
        let held = &mut self.held;
        let (mut prev, mut outside) = (None::<PointE7>, false);
        arc_vertices(block, |v| {
            if outside { return; }
            outside = prev.is_some_and(|a| held.read_edge(a, v));
            prev = Some(v);
        })?;
        self.outside |= outside;
        // An edge that leaves some of the rings outside ends the walk, which then reads as
        // malformed.
        (!outside).then_some(())
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
