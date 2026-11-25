//! Reverse-geocoder library, shared by the HTTP server and the napi
//! binding (geocoder/node). The bin crate (main.rs) wires up axum +
//! auth on top of this.

use s2::cellid::CellID;
use s2::latlng::LatLng;
use s2::region::Region;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

pub mod addr_files;
mod admin_geometry;
pub use admin_geometry::{AdminGeometry, AdminPoint, PipMemo, ADMIN_GEOMETRY_FILES};
mod arc_blocks;
#[cfg(any(test, feature = "fixtures"))]
pub mod arc_fixture;
pub mod cell_pairs;
pub mod coord_keys;
pub mod geo_index;
pub use cell_pairs::CellPairs;
pub use geo_index::{GeoIndex, GeoLists};
mod exact_pip;
pub use exact_pip::{edge_crosses, on_edge, to_e7, PointE7};
mod file_stamp;
pub use file_stamp::FileStamp;
mod hex;
pub use hex::lower_hex;
pub mod place_files;
pub mod poi_files;
pub mod geography_box;
pub mod postcode_files;
pub mod postcode_grid;
pub mod side_blocks;
pub mod strings;
pub mod transport;
mod water;
pub use water::{WaterDetail, WaterKind, WaterLayer, WaterMeta, WATER_DIR, WATER_FILES};
pub mod way_files;
pub use addr_files::{AddrFiles, AddrRecord};
pub use place_files::PlaceFiles;
pub use poi_files::PoiRecords;
pub use postcode_files::PostcodeFiles;
pub use side_blocks::SideBlocks;
pub use way_files::{WayFiles, WayRow};
#[cfg(test)]
mod scratch_dir;
#[cfg(test)]
mod test_heap;
// The example that dumps a dir's logical files, as a module of the tests: its own tests run
// with them, and `query_path_tests` dumps its datasets' dirs with it. It names this crate
// query_server, and its main is not run here.
#[cfg(test)]
extern crate self as query_server;
#[cfg(test)]
#[allow(dead_code)]
#[path = "../examples/dump_logical.rs"]
mod dump_logical;
// The water layer's benchmark, as a module of the tests: its own tests run with them, and its
// main is not run here.
#[cfg(test)]
#[allow(dead_code)]
#[path = "../examples/water_bench.rs"]
mod water_bench;

// Byte-source abstraction.  Index files are memory-mapped; readers go
// through `read_chunk(off, len)` and `read_at::<T>(idx)`.

use memmap2::Mmap;
use std::fs::File;

/// Backing storage for an indexed file.  Both variants implement
/// `read_chunk(off, len)` for zero-copy windowed access.
pub enum FileBytes {
    /// Owned bytes resident in process memory.  Used for test data.
    Owned(Vec<u8>),
    /// Memory-mapped file.  Zero-copy lazy page-faulting from the OS
    /// page cache; works for files of any size.
    Mmap(Mmap),
}

impl FileBytes {
    /// Total file length in bytes.
    pub fn len(&self) -> u64 {
        match self {
            Self::Owned(v) => v.len() as u64,
            Self::Mmap(m) => m.len() as u64,
        }
    }

    pub fn is_empty(&self) -> bool { self.len() == 0 }

    /// The whole file.
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(v) => v,
            Self::Mmap(m) => m,
        }
    }

    /// Read `len` bytes starting at `off`.  `len` stays usize because
    /// individual reads are always tiny (max a few KiB of vertices).
    /// Returns `Cow::Borrowed` (zero-copy slice).
    pub fn read_chunk(&self, off: u64, len: usize) -> std::borrow::Cow<'_, [u8]> {
        match self {
            Self::Owned(v) => {
                let off = off as usize;
                std::borrow::Cow::Borrowed(&v[off..off + len])
            }
            Self::Mmap(m) => {
                let off = off as usize;
                std::borrow::Cow::Borrowed(&m[off..off + len])
            }
        }
    }

    /// Hints how a mapped file is read from here on: in order, where the kernel reads ahead of
    /// each fault, or at random (as mmap_file maps it), where it reads only the page a fault
    /// needs. Never changes what a read returns; a no-op for owned bytes.
    pub fn advise_in_order(&self, in_order: bool) {
        if let Self::Mmap(m) = self {
            let _ = m.advise(if in_order { memmap2::Advice::Sequential } else { memmap2::Advice::Random });
        }
    }

    /// Hints that the byte at `off` is read soon, so its cache miss can overlap other work. Never
    /// changes what a read returns; a no-op past the end and on targets other than x86_64.
    #[inline]
    pub fn prefetch(&self, off: u64) {
        let bytes: &[u8] = match self {
            Self::Owned(v) => v,
            Self::Mmap(m) => m,
        };
        let Some(byte) = bytes.get(off as usize) else { return };
        #[cfg(target_arch = "x86_64")]
        // SAFETY: a prefetch reads nothing back and never faults.
        unsafe {
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>((byte as *const u8).cast())
        };
        #[cfg(not(target_arch = "x86_64"))]
        let _ = byte;
    }

    /// Read a `Copy` record at index `idx` (byte offset = idx * sizeof T)
    /// via a pointer cast through read_unaligned.
    pub fn read_at<T: Copy>(&self, idx: u64) -> T {
        let size = std::mem::size_of::<T>();
        let off = idx * size as u64;
        let bytes = self.read_chunk(off, size);
        // SAFETY: read_chunk returns at least `size` bytes; T is
        // `repr(C)` + `Copy`. read_unaligned is safe regardless of
        // source slice alignment.
        unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const T) }
    }
}

// --- S2 helpers ---

pub const DEFAULT_STREET_CELL_LEVEL: u64 = 17;
pub const DEFAULT_ADMIN_CELL_LEVEL: u64 = 10;
/// How far the street, address and interpolation lookups reach from the point, in metres: the
/// nearest street within it, measured to the way's segments, wherever the point lies in its
/// cell, and none past it. Nominatim's _find_closest_street_or_poi reaches 0.006° (about
/// 660 m), but a road that far off is no longer the road the point is on (NOMINATIM_REFERENCE.md).
pub const DEFAULT_SEARCH_DISTANCE: f64 = 130.0;

// Matches POI_FLAG_HIGHWAY in builder/src/types.h — feature is a
// highway-class polygon (highway=pedestrian/footway/etc.). Nominatim
// ranks these as rank_search=26 (road) rather than the POI default
// rank_search=30, so their own name becomes the `road` field rather
// than falling through to `parent_street`.
pub const POI_FLAG_HIGHWAY: u8 = 0x04;

/// Whether (lat, lng) is a point on the globe: lat within ±90 and lng within ±180, so neither is NaN.
pub fn coords_in_range(lat: f64, lng: f64) -> bool {
    (-90.0..=90.0).contains(&lat) && (-180.0..=180.0).contains(&lng)
}

pub fn cell_id_at_level(lat: f64, lng: f64, level: u64) -> u64 {
    let ll = LatLng::from_degrees(lat, lng);
    CellID::from(ll).parent(level).0
}

pub fn cell_neighbors_at_level(cell_id: u64, level: u64) -> Vec<u64> {
    let cell = CellID(cell_id);
    cell.all_neighbors(level).into_iter().map(|c| c.0).collect()
}

/// How far the scans' distances (GeoOrigin) may stray from the sphere's round the point,
/// relatively: planar ones by no more than half of it, as GeoOrigin measures on the gnomonic
/// projection where they would stray more. The cells round a point are read that much past the
/// reach, and each counts that much nearer.
const CELL_REACH_SLACK: f64 = 1e-3;

/// S2's kMaxDiag for its quadratic projection (s2metrics.cc): no cell of level k has a diagonal
/// longer than this times 2^-k radians, nor, being convex, two points farther apart.
const S2_MAX_DIAG: f64 = 2.438654594434021;

/// S2's kMinArea for its quadratic projection (s2metrics.cc), 8√2/9: no cell of level k is
/// smaller than this times 4^-k steradians.
const S2_MIN_AREA: f64 = 1.257_078_722_109_418;

/// The most street cells the geo lookups may read round a point (geo_cover): what bounds a
/// query's work, and the lists GEO_LISTS keeps, whatever the search distance.
const MAX_GEO_CELLS: f64 = 4096.0;

/// The widest cover, in radians, cells_within reads the cells of: every point of a face of S2's
/// cube lies within 54.74° of its middle, so the faces it reads, those whose middle the point lies
/// within 60° of, hold every point within 5.26° of it.
const MAX_GEO_COVER: f64 = 5.0 * std::f64::consts::PI / 180.0;

/// How far across a street cell of `level` is at the most, in radians.
fn cell_diameter(level: u64) -> f64 {
    S2_MAX_DIAG * (-(level as f64)).exp2()
}

/// How far the point of an edge nearest a point can lie from every cell the builder lists the
/// edge in, at the most, in radians at `level`: half a cell's diameter. The builder (cover_edge,
/// builder/src/s2_helpers.cpp) lists an edge whose middle lies in the cell of one of its ends in
/// the cells of its two ends alone, though the edge may cross a third by their corner. Its half
/// from that end to its middle lies in that cell; the other half, as long and so no longer than
/// the cell is across, lies within half of that of its middle or its other end.
fn listing_gap(level: u64) -> f64 {
    cell_diameter(level) / 2.0
}

/// How far round a point the geo lookups read the street cells, in radians, for a search
/// distance of `reach` radians: every cell a way within the reach is listed in comes that near.
/// The point of the way nearest the point lies in a cell the way is listed in, or on an edge square
/// to the line to it, within the listing gap of a point of the edge in such a cell, so within
/// √(reach² + gap²), on the scans' plane. And at the least the cells next to the point's own
/// (address_cover).
fn geo_cover(reach: f64, level: u64) -> f64 {
    (reach.hypot(listing_gap(level)) * (1.0 + CELL_REACH_SLACK)).max(address_cover(reach, level))
}

/// How far round a point the address lookups read the cells' addresses, in radians: as far as
/// the search distance, an address being listed in the cell of its point, but at the least the
/// cells next to the point's own, which come within a cell's diameter of it, where a building
/// round the point lists its point.
fn address_cover(reach: f64, level: u64) -> f64 {
    reach.max(cell_diameter(level))
}

/// Why the geo lookups can't reach `search_distance` metres at street level `level`: no
/// distance, a cover wider than MAX_GEO_COVER, or more street cells round a point than
/// MAX_GEO_CELLS; none where they can.
fn geo_reach_refused(search_distance: f64, level: u64) -> Option<String> {
    if !(search_distance > 0.0 && search_distance.is_finite()) {
        return Some(format!("search distance {search_distance} m is no distance"));
    }
    let cover = geo_cover(search_distance / 6_371_000.0, level);
    if cover > MAX_GEO_COVER {
        return Some(format!(
            "search distance {search_distance} m reads street cells {:.2}° round a point at level {level}, \
             past the {:.0}° a query's scan of them holds within", cover.to_degrees(), MAX_GEO_COVER.to_degrees()));
    }
    // The cells within the cover lie within a cell's diameter past it, none smaller than kMinArea.
    let disc = (cover + cell_diameter(level)).min(std::f64::consts::PI);
    let cells = 4.0 * std::f64::consts::PI * (disc / 2.0).sin().powi(2) / (S2_MIN_AREA * (-2.0 * level as f64).exp2());
    (cells > MAX_GEO_CELLS).then(|| format!(
        "search distance {search_distance} m reads up to {cells:.0} street cells round a point at level {level}, \
         more than the {MAX_GEO_CELLS} a query may read"))
}

/// A point of the sphere as a unit vector.
fn unit_vector(lat: f64, lng: f64) -> [f64; 3] {
    let (sin_lat, cos_lat) = lat.to_radians().sin_cos();
    let (sin_lng, cos_lng) = lng.to_radians().sin_cos();
    [cos_lat * cos_lng, cos_lat * sin_lng, sin_lat]
}

/// Where a geo lookup measures from, and on what plane, squared distances in radians² as
/// `max_distance_sq` is: the plane where a degree of longitude is cos(lat) of a degree of latitude
/// at the point (point_to_segment_distance), each longitude taken the short way round from the
/// point's, so across the antimeridian too. Near a pole, where that plane would stray from the
/// sphere by more than half CELL_REACH_SLACK within the cover round the point, the gnomonic
/// projection about the point instead, where a point is as far as it is and an edge is its great
/// circle, as the builder lists it (cover_edge). Wherever the point lies, an edge whose line in
/// degrees can stray from its great circle by more than half CELL_REACH_SLACK of the cover is
/// measured on that projection too, unless an end lies past where the projection holds.
#[derive(Clone, Copy)]
struct GeoOrigin {
    lat: f64,
    lng: f64,
    cos_lat: f64,
    /// Near a pole, the point and its directions east and north, as unit vectors.
    polar: Option<[[f64; 3]; 3]>,
    /// The point and its directions east and north, as unit vectors.
    basis: [[f64; 3]; 3],
    /// How far an edge measured on the plane may stray from its great circle, in radians.
    stray: f64,
}

impl GeoOrigin {
    /// The point (lat, lng), whose lookups read the cells within `cover` radians of it.
    fn new(lat: f64, lng: f64, cover: f64) -> Self {
        let (sin_lat, cos_lat) = lat.to_radians().sin_cos();
        let (sin_lng, cos_lng) = lng.to_radians().sin_cos();
        let basis = [unit_vector(lat, lng), [-sin_lng, cos_lng, 0.0], [-sin_lat * cos_lng, -sin_lat * sin_lng, cos_lat]];
        // The plane's scale strays by up to the cover times tan(lat) within the cover.
        let polar = (cover * lat.abs().to_radians().tan() > CELL_REACH_SLACK / 2.0).then_some(basis);
        GeoOrigin { lat, lng, cos_lat: lat.to_radians().cos(), polar, basis, stray: cover * CELL_REACH_SLACK / 2.0 }
    }

    /// Whether the line in degrees of the edge from `a` to `b` can stray from its great circle by
    /// more than `stray`: by up to Δλ (Δλ + 2 |Δφ|) / 8 radians, to second order, at any latitude.
    fn strays(&self, a: NodeCoord, b: NodeCoord) -> bool {
        let dlng = (self.near_lng(b.lng as f64) - self.near_lng(a.lng as f64)).to_radians().abs();
        let dlat = (b.lat as f64 - a.lat as f64).to_radians().abs();
        dlng * (dlng + 2.0 * dlat) > 8.0 * self.stray
    }

    /// `lng` the short way round from the point's: within 180° of it.
    fn near_lng(&self, lng: f64) -> f64 {
        let east = lng - self.lng;
        if east > 180.0 { lng - 360.0 } else if east < -180.0 { lng + 360.0 } else { lng }
    }

    /// A node on the gnomonic projection about the point: None for one 84° or more away, where the
    /// projection fails.
    fn project(basis: &[[f64; 3]; 3], node: NodeCoord) -> Option<(f64, f64)> {
        let v = unit_vector(node.lat as f64, node.lng as f64);
        let dot = |a: &[f64; 3]| a[0] * v[0] + a[1] * v[1] + a[2] * v[2];
        let toward = dot(&basis[0]);
        (toward > 0.1).then(|| (dot(&basis[1]) / toward, dot(&basis[2]) / toward))
    }

    /// The squared distance to the point (lat, lng).
    fn point_sq(&self, lat: f64, lng: f64) -> f64 {
        match &self.polar {
            None => dist_sq((lat - self.lat).to_radians(), (self.near_lng(lng) - self.lng).to_radians(), self.cos_lat),
            Some(basis) => {
                let v = unit_vector(lat, lng);
                let (p, along) = (basis[0], v[0] * basis[0][0] + v[1] * basis[0][1] + v[2] * basis[0][2]);
                let across = [p[1] * v[2] - p[2] * v[1], p[2] * v[0] - p[0] * v[2], p[0] * v[1] - p[1] * v[0]];
                (across[0] * across[0] + across[1] * across[1] + across[2] * across[2]).sqrt().atan2(along).powi(2)
            }
        }
    }

    /// The squared distance to the edge from `a` to `b`, and the fraction along it of its point
    /// nearest the point.
    fn segment(&self, a: NodeCoord, b: NodeCoord) -> (f64, f64) {
        if self.polar.is_some() || self.strays(a, b) {
            match (Self::project(&self.basis, a), Self::project(&self.basis, b)) {
                (Some(a), Some(b)) => {
                    let (r, t) = nearest_to_origin(a, b);
                    return (r.atan().powi(2), t);
                }
                // An end the projection fails for: by a pole, an edge out of reach; else on the plane.
                _ if self.polar.is_some() => return (f64::MAX, 0.0),
                _ => {}
            }
        }
        let c = self.cos_lat;
        point_to_segment_with_t(self.lat, self.lng * c, a.lat as f64, self.near_lng(a.lng as f64) * c,
                                b.lat as f64, self.near_lng(b.lng as f64) * c, PLANAR)
    }

    /// The squared distance in plain degrees (as radians², as PostGIS measures a lon/lat line) to
    /// the point of the edge from `a` to `b` nearest in degrees, and the fraction along the edge
    /// of that point: at any latitude, as Nominatim locates a house number on its lon/lat line.
    fn segment_in_degrees(&self, a: NodeCoord, b: NodeCoord) -> (f64, f64) {
        point_to_segment_with_t(self.lat, self.lng, a.lat as f64, self.near_lng(a.lng as f64),
                                b.lat as f64, self.near_lng(b.lng as f64), PLANAR)
    }

    /// The length of the edge from `a` to `b` in plain degrees (as radians), as PostGIS measures
    /// a lon/lat line.
    fn length(&self, a: NodeCoord, b: NodeCoord) -> f64 {
        let dlat = (b.lat as f64 - a.lat as f64).to_radians();
        let dlng = (self.near_lng(b.lng as f64) - self.near_lng(a.lng as f64)).to_radians();
        dist_sq(dlat, dlng, PLANAR).sqrt()
    }

    /// The squared distance to the polygon through `vertices`: 0 for the point inside it, as
    /// polygon_distance_sq measures one.
    fn polygon_sq(&self, vertices: &[NodeCoord]) -> f64 {
        let Some(basis) = &self.polar else {
            let wraps = |v: &NodeCoord| self.near_lng(v.lng as f64) != v.lng as f64;
            if !vertices.iter().any(wraps) {
                return polygon_distance_sq(self.lat, self.lng, vertices, self.cos_lat);
            }
            // A vertex across the antimeridian, near the point's side of it: past 128° in f32 a
            // turn of 360° is exact.
            let near: Vec<NodeCoord> = vertices.iter()
                .map(|v| NodeCoord { lat: v.lat, lng: self.near_lng(v.lng as f64) as f32 })
                .collect();
            return polygon_distance_sq(self.lat, self.lng, &near, self.cos_lat);
        };
        let Some(ring) = vertices.iter().map(|&v| Self::project(basis, v)).collect::<Option<Vec<_>>>() else {
            return f64::MAX;
        };
        if ring.is_empty() {
            return f64::MAX;
        }
        if origin_in_ring(&ring) {
            return 0.0;
        }
        (0..ring.len()).map(|i| nearest_to_origin(ring[i], ring[(i + 1) % ring.len()]).0).fold(f64::MAX, f64::min).atan().powi(2)
    }
}

/// How far the origin of a plane is from the segment from `a` to `b`, and the fraction along it
/// of the segment's point nearest the origin.
fn nearest_to_origin(a: (f64, f64), b: (f64, f64)) -> (f64, f64) {
    let (dx, dy) = (b.0 - a.0, b.1 - a.1);
    let length_sq = dx * dx + dy * dy;
    let t = if length_sq > 0.0 { (-(a.0 * dx + a.1 * dy) / length_sq).clamp(0.0, 1.0) } else { 0.0 };
    ((a.0 + t * dx).hypot(a.1 + t * dy), t)
}

/// Whether the origin of a plane lies inside the ring through `points`, by ray casting as
/// point_in_polygon does.
fn origin_in_ring(points: &[(f64, f64)]) -> bool {
    let mut inside = false;
    let mut j = points.len() - 1;
    for (i, &(xi, yi)) in points.iter().enumerate() {
        let (xj, yj) = points[j];
        if (yi > 0.0) != (yj > 0.0) && 0.0 < (xj - xi) * (0.0 - yi) / (yj - yi) + xi {
            inside = !inside;
        }
        j = i;
    }
    inside
}

/// The interpolation ways in a lookup's lists of `cells`, in the order read, each once a scan: a
/// line several cells list is measured once.
fn interps_once<'a>(lists: &'a GeoLists, cells: &NearCells) -> impl Iterator<Item = u32> + 'a {
    let mut seen = std::collections::HashSet::new();
    (0..cells.ids.len())
        .flat_map(move |i| lists.list(i, geo_index::INTERPOLATIONS).iter().copied())
        .filter(move |&id| seen.insert(id))
}

/// The cells a lookup round a point reads (cells_within), nearest first: their ids, and the
/// squared distance (radians², as the scans measure one) that nothing in each comes nearer than.
struct NearCells {
    ids: Vec<u64>,
    near: Vec<f64>,
}

impl NearCells {
    /// How many of the cells, nearest first, come within `reach` radians of the point, as
    /// cells_within takes a cell in.
    fn within(&self, reach: f64) -> usize {
        let near_reach = reach * (1.0 + CELL_REACH_SLACK) * (1.0 - CELL_REACH_SLACK);
        self.near.partition_point(|&near| near <= near_reach * near_reach)
    }
}

/// The cells of `level` that come within `reach` radians of (lat, lng), nearest first, equally
/// near ones by id. On each face of S2's cube the reach meets, the cells of the box of (u, v)
/// holding every point within reach are measured on the gnomonic projection about the point,
/// where a cell is the quadrilateral of its corners and distances from the point keep their
/// order.
fn cells_within(lat: f64, lng: f64, level: u64, reach: f64) -> NearCells {
    let (sin_lat, cos_lat) = lat.to_radians().sin_cos();
    let (sin_lng, cos_lng) = lng.to_radians().sin_cos();
    let p = [cos_lat * cos_lng, cos_lat * sin_lng, sin_lat];
    let (east, north) = ([-sin_lng, cos_lng, 0.0], [-sin_lat * cos_lng, -sin_lat * sin_lng, cos_lat]);
    let dot = |a: [f64; 3], b: [f64; 3]| a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
    // A point of the sphere (any length) on the projection: the tangent of its distance from p,
    // split east and north.
    let project = |q: [f64; 3]| {
        let t = dot(q, p);
        (dot(q, east) / t, dot(q, north) / t)
    };
    let cover = reach * (1.0 + CELL_REACH_SLACK);
    let (sin_cover, tan_cover) = (cover.sin(), cover.tan());
    let (size, shift) = (1u64 << level, 30 - level);
    let mut found: Vec<(f64, u64)> = Vec::new();
    let (mut us, mut vs, mut corners) = (Vec::new(), Vec::new(), Vec::new());
    for face in 0..6u8 {
        // Every point of a face lies within 55° of its middle, so p lies within 60° of the middle
        // of a face that holds a point within a reach of MAX_GEO_COVER.
        let toward = p[usize::from(face % 3)] * if face < 3 { 1.0 } else { -1.0 };
        if toward <= 0.5 {
            continue;
        }
        let (u, v) = face_xyz_to_uv(face, p);
        let (u_lo, u_hi) = (expand_uv(u, v.abs(), -sin_cover), expand_uv(u, v.abs(), sin_cover));
        let (v_lo, v_hi) = (expand_uv(v, u.abs(), -sin_cover), expand_uv(v, u.abs(), sin_cover));
        if u_hi < -1.0 || u_lo > 1.0 || v_hi < -1.0 || v_lo > 1.0 {
            continue;
        }
        let index = |uv: f64| ((uv_to_st(uv.clamp(-1.0, 1.0)) * size as f64) as u64).min(size - 1);
        let (i_lo, i_hi, j_lo, j_hi) = (index(u_lo), index(u_hi), index(v_lo), index(v_hi));
        let side = |k: u64| st_to_uv(k as f64 / size as f64);
        us.clear();
        us.extend((i_lo..=i_hi + 1).map(side));
        vs.clear();
        vs.extend((j_lo..=j_hi + 1).map(side));
        corners.clear();
        for &v in &vs {
            corners.extend(us.iter().map(|&u| project(face_uv_to_xyz(face, u, v))));
        }
        let row = us.len();
        for (b, j) in (j_lo..=j_hi).enumerate() {
            for (a, i) in (i_lo..=i_hi).enumerate() {
                let at = b * row + a;
                let tan_sq = origin_to_quad_sq([corners[at], corners[at + 1], corners[at + row + 1], corners[at + row]]);
                if tan_sq <= tan_cover * tan_cover {
                    let id = CellID::from_face_ij(face, (i << shift) as i32, (j << shift) as i32).parent(level);
                    found.push((tan_sq.sqrt().atan(), id.0));
                }
            }
        }
    }
    found.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    let near = |angle: f64| (angle * (1.0 - CELL_REACH_SLACK)).powi(2);
    NearCells { ids: found.iter().map(|&(_, id)| id).collect(), near: found.iter().map(|&(angle, _)| near(angle)).collect() }
}

/// The point (of any length) at (u, v) on the plane of face `face` of S2's cube: S2's
/// FaceUVtoXYZ.
fn face_uv_to_xyz(face: u8, u: f64, v: f64) -> [f64; 3] {
    match face {
        0 => [1.0, u, v],
        1 => [-u, 1.0, v],
        2 => [-u, -v, 1.0],
        3 => [-1.0, -v, -u],
        4 => [v, -1.0, -u],
        _ => [v, u, -1.0],
    }
}

/// (u, v) of a point in the half of the sphere round face `face`, on the face's plane: S2's
/// ValidFaceXYZtoUV.
fn face_xyz_to_uv(face: u8, [x, y, z]: [f64; 3]) -> (f64, f64) {
    match face {
        0 => (y / x, z / x),
        1 => (-x / y, z / y),
        2 => (-x / z, -y / z),
        3 => (z / x, y / x),
        4 => (z / y, -x / y),
        _ => (-y / z, -x / z),
    }
}

/// S2's quadratic projection of s (or t) in [0, 1] on u (or v) in [-1, 1], and back: STtoUV
/// and UVtoST.
fn st_to_uv(s: f64) -> f64 {
    if s >= 0.5 { (4.0 * s * s - 1.0) / 3.0 } else { (1.0 - 4.0 * (1.0 - s) * (1.0 - s)) / 3.0 }
}

fn uv_to_st(u: f64) -> f64 {
    if u >= 0.0 { 0.5 * (1.0 + 3.0 * u).sqrt() } else { 1.0 - 0.5 * (1.0 - 3.0 * u).sqrt() }
}

/// Where the side at `u` of a box of (u, v) whose largest |v| is `max_v` moves to for the box to
/// hold every point within the distance whose sine is `sin_dist` of it (negative for the low
/// side): S2's ExpandEndpoint (s2cell_id.cc), tan(atan(u) + the side's turn).
fn expand_uv(u: f64, max_v: f64, sin_dist: f64) -> f64 {
    let sin_turn = sin_dist * ((1.0 + u * u + max_v * max_v) / (1.0 + u * u)).sqrt();
    let cos_turn = (1.0 - sin_turn * sin_turn).sqrt();
    (cos_turn * u + sin_turn) / (cos_turn - sin_turn * u)
}

/// The squared distance from the origin to the convex quadrilateral through `corners` in order:
/// 0 for the origin inside it or on a side.
fn origin_to_quad_sq(corners: [(f64, f64); 4]) -> f64 {
    let (mut turns, mut nearest) = ([0.0; 4], f64::MAX);
    for k in 0..4 {
        let ((ax, ay), (bx, by)) = (corners[k], corners[(k + 1) % 4]);
        turns[k] = ax * by - ay * bx;
        let (dx, dy) = (bx - ax, by - ay);
        let length_sq = dx * dx + dy * dy;
        let t = if length_sq > 0.0 { (-(ax * dx + ay * dy) / length_sq).clamp(0.0, 1.0) } else { 0.0 };
        let (x, y) = (ax + t * dx, ay + t * dy);
        nearest = nearest.min(x * x + y * y);
    }
    let inside = turns.iter().all(|&turn| turn >= 0.0) || turns.iter().all(|&turn| turn <= 0.0);
    if inside { 0.0 } else { nearest }
}

// --- Binary format structs (must match C++ build pipeline) ---

#[inline]
pub fn vertex_scale(enc: u8) -> f64 {
    match enc {
        0 => 1e-5, // 1.1 m
        1 => 1e-4, // 11 m
        2 => 1e-6, // 11 cm
        3 => 1e-7, // 1.1 cm
        _ => 0.0,  // F32: not used
    }
}

/// The admin level of a postcode area's record (boundary=postal_code).
pub const POSTCODE_AREA_LEVEL: u8 = 11;

/// The place type the builder gives a postal area whose postcode its country's pattern rejects:
/// Nominatim makes no postcode area of it (postcodes.py _update_postcode_areas).
pub const POSTAL_AREA_INVALID: u8 = 15;

/// The place type the builder gives a boundary (admin_level 4 to 10) that repeats its parent, the
/// same wikidata and geometry, and links no place node: Nominatim takes it out of addresses
/// (placex_update's is_same, placex_triggers.sql:909-931). One that links a place node keeps the
/// node's type and has this as its own_place_type: it takes the node's rank back (:1161).
pub const BOUNDARY_REPEATS_PARENT: u8 = 15;

/// The top bit of an admin_polygons.bin record's last byte: the polygon is a closed way's
/// (AdminPolygon::of_way); the bits below it are its own_place_type.
pub const ADMIN_OF_WAY: u8 = 0x80;

/// The admin level `/polygons` gives a postcode area, the one the test portal colours as postal.
pub const POLYGONS_POSTAL_LEVEL: u8 = 99;

/// The bytes of an admin_polygons.bin record (builder AdminPolygonRecord): the u32 offset of its
/// ring block in admin_rings.bin, every record's the running one, a tombstone's included (whose
/// block, like that of a polygon a tier left without rings, is empty); its u32 name_id, NO_DATA
/// for a tombstone; its f32 area; its u16 country code; admin_level << 4 | place_type_override;
/// own_place_type, ADMIN_OF_WAY set for a closed way's. A ring block runs to the next record's
/// offset, the last to the end of admin_rings.bin.
pub const ADMIN_POLYGON_RECORD_BYTES: u64 = 16;

/// An admin polygon: its admin_polygons.bin record, with its ring block's length.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AdminPolygon {
    /// Byte offset of the polygon's ring block in admin_rings.bin (admin_geometry.rs).
    pub ring_offset: u32,
    /// The ring block's length: 0 for a tombstone and where a tier left the polygon without
    /// rings.
    pub ring_bytes: u32,
    pub name_id: u32,
    pub admin_level: u8,
    pub place_type_override: u8,  // 0=none, 1=city, 2=town, 3=village, 4=suburb, 5=neighbourhood, 6=quarter
    /// A place area's own place type where it links a place node of another type, whose type is
    /// its place_type_override and labels it (find_linked_place); 0 where its place type is its
    /// own.
    pub own_place_type: u8,
    /// The exact shoelace of the polygon's rings, holes taken off, in square degrees: the same in
    /// every tier.
    pub area: f32,
    pub country_code: u16,
    /// A closed way's polygon, not a ring of a relation (the record's last byte's top bit): it
    /// raises no place node of its rank to a subpart, which placex_update does inside a relation
    /// only (osm_type = 'R', placex_triggers.sql:984-1004).
    pub of_way: bool,
}

impl AdminPolygon {
    /// The record at `bytes` (ADMIN_POLYGON_RECORD_BYTES of them) whose ring block ends at
    /// `ring_end`: the next record's offset, or admin_rings.bin's length after the last one. A
    /// block ending before it starts reads as empty.
    pub fn from_record(bytes: &[u8], ring_end: u64) -> AdminPolygon {
        let u32_at = |at: usize| u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]]);
        let ring_offset = u32_at(0);
        let ring_bytes = u32::try_from(ring_end.saturating_sub(u64::from(ring_offset))).unwrap_or(0);
        AdminPolygon {
            ring_offset,
            ring_bytes,
            name_id: u32_at(4),
            admin_level: bytes[14] >> 4,
            place_type_override: bytes[14] & 15,
            own_place_type: bytes[15] & !ADMIN_OF_WAY,
            area: f32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            country_code: u16::from_le_bytes([bytes[12], bytes[13]]),
            of_way: bytes[15] & ADMIN_OF_WAY != 0,
        }
    }

    /// The record's bytes, as the builder writes them.
    pub fn record_bytes(&self) -> [u8; ADMIN_POLYGON_RECORD_BYTES as usize] {
        let mut b = [0u8; ADMIN_POLYGON_RECORD_BYTES as usize];
        b[0..4].copy_from_slice(&self.ring_offset.to_le_bytes());
        b[4..8].copy_from_slice(&self.name_id.to_le_bytes());
        b[8..12].copy_from_slice(&self.area.to_le_bytes());
        b[12..14].copy_from_slice(&self.country_code.to_le_bytes());
        b[14] = self.admin_level << 4 | self.place_type_override;
        b[15] = self.own_place_type | if self.of_way { ADMIN_OF_WAY } else { 0 };
        b
    }

    /// The place type of a place area's own place tag: own_place_type where it links a node of
    /// another type, else place_type_override.
    pub fn own_place_tag(&self) -> u8 {
        if self.own_place_type != 0 { self.own_place_type } else { self.place_type_override }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct NodeCoord {
    pub lat: f32,
    pub lng: f32,
}

/// A POI as poi_records.bin's row and dictionaries give it (poi_files.rs).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PoiRecord {
    pub lat: f32,
    pub lng: f32,
    /// Where the POI's polygon block starts in poi_vertices.bin; NO_DATA for a point.
    pub vertex_offset: u32,
    pub name_id: u32,
    pub category: u8,
    pub tier: u8,
    pub flags: u8,
    pub importance: u8,
    /// Name offset of the nearest named street to this POI, pre-
    /// computed at build time. Used to populate the `road` field
    /// when this POI wins query_geo primary-feature selection.
    /// Mirrors Nominatim's parent_place_id chain from a rank-30
    /// POI back to its rank-26 road. `NO_DATA` if unset.
    pub parent_street_id: u32,
    /// String offset of the POI's calculated postcode, inherited
    /// from the smallest containing boundary=postal_code polygon
    /// at build time. Mirrors Nominatim's placex.postcode chain
    /// for rank-30 POI rows whose postcode comes via their
    /// parent_place_id. `NO_DATA` if no postal boundary contains
    /// this POI.
    pub parent_postcode_id: u32,
    /// Smallest admin polygon (admin_level 2..10) containing the
    /// POI, computed at build time via PIP. Used by the server as
    /// the starting point of a chain-containment check: when the
    /// primary is a POI whose admin ancestry doesn't include the
    /// query's containing city/state, the POI is demoted in favour
    /// of a more locally-correct candidate. `NO_DATA` if no admin
    /// boundary contains the POI (e.g. ocean nodes, cross-border
    /// features lost during continent-filter remap).
    pub parent_poly_id: u32,
}

impl PoiRecord {
    /// Whether the POI is a polygon (has a block in poi_vertices.bin), not a point.
    pub fn is_polygon(&self) -> bool {
        self.vertex_offset != NO_DATA
    }
}

/// A place node as place_files.rs reads it from its row.
#[derive(Clone, Copy, Debug)]
pub struct PlaceNode {
    pub lat: f32,
    pub lng: f32,
    pub name_id: u32,
    pub place_type: u8,
    pub _pad1: u8,
    pub _pad2: u16,
    /// With PLACE_FLAG_LABEL_OF in `_pad1`: the polygon of the boundary
    /// that claims the node, which place search labels with it. Else
    /// `NO_DATA`. Reverse geocoding doesn't read it.
    pub parent_poly_id: u32,
}

/// PlaceNode._pad1 bit: a node a boundary claims. It keeps its name, the boundary's label for
/// place search, but reverse geocoding skips it: place_hidden.bin holds it.
pub const PLACE_FLAG_HIDDEN: u8 = 0x02;
/// PlaceNode._pad1 bit, on a hidden node: parent_poly_id is the boundary that claims it.
pub const PLACE_FLAG_LABEL_OF: u8 = 0x01;

impl PlaceNode {
    /// Whether reverse geocoding reads the node: it is not hidden.
    pub fn is_visible(&self) -> bool {
        self._pad1 & PLACE_FLAG_HIDDEN == 0
    }
}

// POI category metadata — loaded from poi_meta.json (written by builder)
pub struct PoiMeta {
    pub categories: std::collections::HashMap<u8, PoiCategoryMeta>,
}

#[derive(Deserialize)]
pub struct PoiCategoryMeta {
    pub name: String,
    #[serde(default = "default_ref_dist")]
    pub reference_distance: f64,
    #[serde(default)]
    pub max_distance: f64,
    #[serde(default = "default_importance")]
    pub default_importance: f64,
}

pub fn default_ref_dist() -> f64 { 100.0 }
pub fn default_importance() -> f64 { 5.0 }

impl PoiMeta {
    /// Parse poi_meta.json content. `load(dir)` reads the file and
    /// calls this.
    pub fn from_json(s: &str) -> Self {
        if let Ok(raw) = serde_json::from_str::<std::collections::HashMap<String, PoiCategoryMeta>>(s) {
            let categories = raw.into_iter()
                .filter_map(|(k, v)| k.parse::<u8>().ok().map(|id| (id, v)))
                .collect();
            return PoiMeta { categories };
        }
        PoiMeta { categories: std::collections::HashMap::new() }
    }

    pub fn load(dir: &str) -> Self {
        let path = format!("{}/poi_meta.json", dir);
        if let Ok(data) = std::fs::read_to_string(&path) {
            return Self::from_json(&data);
        }
        PoiMeta { categories: std::collections::HashMap::new() }
    }

    pub fn category_name(&self, id: u8) -> &str {
        self.categories.get(&id).map(|c| c.name.as_str()).unwrap_or("unknown")
    }

    pub fn reference_distance(&self, id: u8) -> f64 {
        self.categories.get(&id).map(|c| c.reference_distance).unwrap_or(100.0)
    }

    pub fn max_distance(&self, id: u8) -> f64 {
        self.categories.get(&id).map(|c| c.max_distance).unwrap_or(500.0)
    }

    pub fn default_importance(&self, id: u8) -> f64 {
        self.categories.get(&id).map(|c| c.default_importance).unwrap_or(5.0)
    }
}

// --- Admin level → address rank mapping (Nominatim approach) ---

pub struct AdminLevelConfig {
    pub default: HashMap<u8, u8>,
    pub countries: HashMap<String, HashMap<u8, u8>>,
}

impl AdminLevelConfig {
    pub fn load() -> Self {
        let json: serde_json::Value = serde_json::from_str(
            include_str!("../admin_levels.json")
        ).expect("Failed to parse admin_levels.json");

        let mut default = HashMap::new();
        let mut countries = HashMap::new();

        if let Some(obj) = json.as_object() {
            for (key, val) in obj {
                let map: HashMap<u8, u8> = val.as_object().unwrap_or(&serde_json::Map::new())
                    .iter()
                    .filter_map(|(k, v)| {
                        Some((k.parse::<u8>().ok()?, v.as_u64()? as u8))
                    })
                    .collect();
                if key == "default" {
                    default = map;
                } else {
                    countries.insert(key.to_uppercase(), map);
                }
            }
        }
        AdminLevelConfig { default, countries }
    }

    pub fn to_rank(&self, country_code: &str, admin_level: u8) -> u8 {
        if let Some(overrides) = self.countries.get(country_code) {
            if let Some(&rank) = overrides.get(&admin_level) {
                return rank;
            }
        }
        self.default.get(&admin_level).copied().unwrap_or(0)
    }
}

// AdminPlaceType → output field name.
// Must stay in sync with builder/src/types.h AdminPlaceType enum.
// These mirror Nominatim's get_label_tag() fallthrough to
// extratags['place'] / extratags['linked_place']: the key in the JSON
// address dict is the place type verbatim.
pub fn place_type_to_field(pt: u8) -> Option<&'static str> {
    match pt {
        1 => Some("city"),
        2 => Some("town"),
        3 => Some("village"),
        4 => Some("suburb"),
        5 => Some("neighbourhood"),
        6 => Some("quarter"),
        7 => Some("state"),
        8 => Some("province"),
        9 => Some("region"),
        10 => Some("county"),
        11 => Some("district"),
        12 => Some("borough"),
        13 => Some("hamlet"),
        14 => Some("municipality"),
        _ => None,
    }
}

// Matches Nominatim's ADMIN_LABELS (rank_address → label).
// ADMIN_LABELS in classtypes.py keys on rank//2:
//   1 Continent, 2 Country, 3 Region, 4 State, 5 State District,
//   6 County, 7 Municipality, 8 City, 9 City District, 10 Suburb,
//   11 Neighbourhood, 12 City Block.
// Map a PoiCategory numeric value to the OSM top-level class whose
// name the server should use as the landmark field in the address
// response. Mirrors Nominatim's convention where the feature's
// `class` column becomes the address-dict key (Statue of Liberty
// → tourism, Notre-Dame → historic, Disneyland → tourism).
// Returns None for categories that shouldn't surface as a landmark
// — either admin-level area labels (ISLAND, BAY, CAPE, NATIONAL_PARK
// etc. are typically too large to be meaningful here) or generic
// rank-30 filler.
pub fn poi_category_to_osm_class(cat: u8) -> Option<&'static str> {
    match cat {
        // tourism: museum, attraction, viewpoint, theme_park, zoo,
        // gallery, artwork, alpine_hut, aquarium, camp_site,
        // picnic_site, resort, hotel, information, motel,
        // guest_house, hostel, apartment, chalet, caravan_site
        0..=19 => Some("tourism"),
        // historic: castle, monument, ruins, archaeological_site,
        // memorial, battlefield, fort, ship, wayside_cross,
        // wayside_shrine, city_gate, citywalls, boundary_stone,
        // milestone, mine, aircraft, locomotive, cannon, tomb, manor
        20..=39 => Some("historic"),
        // amenity landmarks (40-54)
        40..=54 => Some("amenity"),
        // amenity civic (55-59, 67-69) — keep `amenity` class
        55..=59 | 67..=69 => Some("amenity"),
        // leisure (60-66, 70-72, 77-79)
        60..=66 | 70..=72 | 77..=79 => Some("leisure"),
        // transport / commerce root (73 bus_stop, 74 parking, 75 fuel, 76 shop).
        // amenity=parking is a rank-30 named POI in Nominatim (like its
        // bicycle/motorcycle-parking siblings 228/229), so it must map to a
        // class to surface as a landmark — 74 was previously an unintended gap.
        73 | 74 | 75 | 76 => Some("amenity"),
        // natural — skip large area-label values (bay=91, cape=92,
        // island=93). Everything else in 80..=99 is small enough.
        80..=90 | 94..=99 => Some("natural"),
        // aeroway, railway
        100 => Some("aeroway"),
        105 => Some("railway"),
        // man_made (110..=119)
        110..=119 => Some("man_made"),
        // building (cathedral, palace)
        120..=121 => Some("building"),
        // boundary — national_park, protected_area can legitimately
        // be named landmarks (Sherwood Forest NNR, Yosemite). Our
        // 'contained polygon, smallest area wins' sort makes sure a
        // huge NP doesn't outrank a specific feature anyway.
        130..=131 => Some("boundary"),
        // Extra natural (valley, ridge, saddle, gorge, bare_rock)
        132..=136 => Some("natural"),
        // craft (winery, brewery) — also named landmarks
        140..=141 => Some("craft"),
        // power=plant (150) is NOT a Nominatim main tag — it never becomes a
        // placex row or a reverse landmark, so exclude it explicitly. This arm
        // must precede the 142..=152 band that would otherwise catch it.
        150 => None,
        // landuse (meadow, orchard, vineyard, farmland, allotments,
        // quarry, reservoir, recreation_ground, military, religious)
        142..=152 => Some("landuse"),
        // office=government
        160 => Some("office"),
        // leisure=swimming_area, emergency=fire_water_pond
        161 => Some("leisure"),
        162 => Some("emergency"),
        // Small-amenity sub-categories (171..=179) — toilets, clock…
        171..=179 => Some("amenity"),
        // Food / civic / health / finance / mail / transport extras
        // (180..=234) all stay in `amenity`
        180..=234 => Some("amenity"),
        // Leisure extras (235..=240) stay in `leisure`
        235..=240 => Some("leisure"),
        // Shop sub-categories (241..=253)
        241..=253 => Some("shop"),
        // GENERIC_RANK30=170 never surfaces (by definition generic).
        // (POWER_PLANT=150 is excluded explicitly above.)
        _ => None,
    }
}

pub fn rank_to_field(rank: u8) -> Option<&'static str> {
    match rank {
        4..=5 => Some("country"),
        6..=7 => Some("region"),
        8..=9 => Some("state"),
        10..=11 => Some("state_district"),
        12..=13 => Some("county"),
        14..=15 => Some("municipality"),
        16..=17 => Some("city"),
        18..=19 => Some("city_district"),
        20..=21 => Some("suburb"),
        22..=23 => Some("neighbourhood"),
        24..=25 => Some("city_block"),
        _ => None,
    }
}

/// The address fields a polygon fills (AdminResult's), each in a slot of its own.
pub const ADMIN_FIELDS: [&str; 18] = [
    "country", "state", "province", "region", "state_district", "county", "district", "municipality", "city", "town",
    "village", "hamlet", "city_district", "borough", "suburb", "quarter", "neighbourhood", "city_block",
];

/// The slot of an address field in ADMIN_FIELDS.
pub fn admin_field_index(field: &str) -> Option<usize> {
    ADMIN_FIELDS.iter().position(|f| *f == field)
}

/// The address fields a place node fills, by PlaceNode type.
pub const PLACE_NODE_FIELDS: [&str; 7] = ["city", "town", "village", "suburb", "hamlet", "neighbourhood", "quarter"];

/// The rank_address of the polygon or the place node behind each field of an
/// address (ADMIN_FIELDS), 0 where none is known: display_name lists the
/// place parts by it, as Nominatim lists its address rows.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct FieldRanks([u8; ADMIN_FIELDS.len()]);

impl FieldRanks {
    pub fn get(&self, field: &str) -> u8 {
        admin_field_index(field).map_or(0, |i| self.0[i])
    }

    pub fn set(&mut self, field: &str, rank: u8) {
        if let Some(i) = admin_field_index(field) {
            self.0[i] = rank;
        }
    }
}

/// Whether a place area (class=place polygon) only repeats an address entry
/// already found: a polygon of the same name and rank_address, no larger.
/// Nominatim keeps one entry per rank (address_havelevel); a namesake of
/// another rank is an entry of its own (Singapore's place=city area has the
/// country's name and outline, and Nominatim names it as the city).
pub fn place_area_repeats<S: AsRef<str>>(found: impl IntoIterator<Item = (S, u8, f32)>, name: &str, rank: u8, area: f32) -> bool {
    found.into_iter().any(|(n, r, a)| n.as_ref() == name && r == rank && a <= area)
}

// Map an AdminPlaceType override to its Nominatim rank_address equivalent in
// `country` (the upper-case ISO code, "" where unknown). Drives the
// nested-boundary rank adjustment so linked / place=tagged boundaries sit in
// the same rank hierarchy as admin_level-based ones, and ranks place areas.
// Ranks come from settings/address-levels.json: a country's `place` entries,
// then the default section.
pub fn place_type_to_rank(pt: u8, country: &str) -> u8 {
    // A country's own place ranks (address-levels.json: ca, de, es, id, ru,
    // sa; its civil_parish is no type of ours).
    match (country, pt) {
        ("CA", 10) => return 0,              // county [12, 0]
        ("DE", 9) | ("DE", 10) => return 0,  // region [10, 0], county [12, 0]
        ("ES", 8) => return 10,              // province 10
        ("ID", 14) | ("RU", 14) => return 18, // municipality 18
        ("SA", 8) => return 12,              // province 12
        ("SA", 14) => return 18,             // municipality 18
        _ => {}
    }
    // Values come from Nominatim's settings/address-levels.json —
    // specifically the rank_address (second element of [rank_search,
    // rank_address] pairs). place=state / province / region / country are
    // explicitly 0 (not in address chain) — applying them as "place area
    // parents" would otherwise promote valid countries to suburb rank
    // via the second-check containment loop (see "Mashriq" place=region
    // multipolygon covering Egypt in our test data).
    match pt {
        1 => 16,  // city  [16]
        2 => 16,  // town  [18, 16]
        3 => 16,  // village [19, 16]
        4 => 20,  // suburb [19, 20]
        5 => 24,  // neighbourhood [24]
        6 => 22,  // quarter [20, 22]
        7 => 0,   // state  [8, 0] → not in address chain
        8 => 0,   // province [8, 0]
        9 => 0,   // region [18, 0]
        10 => 12, // county
        11 => 12, // district
        12 => 18, // borough
        13 => 20, // hamlet
        14 => 14, // municipality
        _ => 0,
    }
}

/// A place area's (admin_level 15) compute_place_rank, its own place tag's rank, and its
/// rank_address before an area containing it lifts it, in `country`: placex_update keeps a place
/// area of rank 16 to 23 at its own rank whether it links a place node or not (that branch leaves
/// parent_address_level unset, placex_triggers.sql:930-950, :1129), and gives any other the rank
/// of the node it links where that is above 3 and below 26 (:1109-1133). One of rank 0 links
/// nothing (find_linked_place, :285).
pub fn place_area_ranks(poly: &AdminPolygon, country: &str) -> (u8, u8) {
    let own = place_type_to_rank(poly.own_place_tag(), country);
    if poly.own_place_type == 0 || own == 0 || PLACE_AREA_LIFTED_RANKS.contains(&own) {
        return (own, own);
    }
    let linked = place_type_to_rank(poly.place_type_override, country);
    (own, if (4..26).contains(&linked) { linked } else { own })
}

/// A polygon holding a query point with its rank_address in the point's
/// address.
#[derive(Clone, Copy)]
pub struct RankedPoly {
    pub poly: AdminPolygon,
    pub rank_address: u8,
}

/// Nominatim's rank_address of the polygons holding a point (placex_update,
/// placex_triggers.sql:876-950 and :1109-1170), each given at its initial
/// rank_address: its level's in the country for a boundary, the one before
/// any lift (place_area_ranks) for a place area (admin_level 15). Nominatim
/// indexes the coarser first; here they are sorted by admin_level ascending,
/// ties (same admin_level) by area descending (the larger polygon is assumed
/// to be the parent), and each boundary of a rank other than 0 is taken
/// through three steps in that order, every polygon before it final:
///
///   1. Admin nested (:876-910): the parent is the latest earlier polygon
///      with admin_level in (3, self.admin_level) and a rank of 1 to 25,
///      standing in for Nominatim's, the boundary of the highest admin_level
///      round self's centroid. A parent of rank >= self lifts self to
///      parent_rank + 2, or to 25 under a parent of 24 or more.
///   2. Place area containment, from rank 10 (:912-929), for a boundary
///      without a place type: the place area (class='place', e.g. R9185096
///      "Nairobi" with place=city) of the highest rank from self's up to 23
///      round self lifts it to place_area_rank + 2: one larger than self that
///      self lies within (`within(self, place_area)`, ST_Contains).
///   3. Linked place (:1109-1170): the rank of the place type, from the
///      linked place node or the boundary's own place tag, replaces self's
///      rank where it is higher than both self's and the parent's. Nominatim
///      takes it where it is higher than the parent's alone, so it can lower
///      a boundary too, but only from a link, or from the own tag where no
///      node links: the place type here may be either, and only raises.
///
/// A boundary that repeats its parent and links a place node (own_place_type
/// BOUNDARY_REPEATS_PARENT) takes none of them: it ranks as its place node,
/// above 3 and below 26, or 0 (is_same leaves parent_address_level 3).
///
/// A place area (admin_level 15, after every boundary) takes none of them:
/// one of rank 16 to 23 is lifted to an area's rank + 2 where that area, a
/// boundary or a place area whose initial rank is the place area's or more
/// up to 23, contains it (:930-950), the one of the highest initial rank:
/// one larger than the place area that it lies within, as in step 2. `place_areas` are
/// the class='place' polygons holding the point, any with a place type;
/// `country` is the upper-case ISO code of the point's country, "" where
/// unknown.
///
/// A Paris L6 boundary with linked_place=city stays at rank 16 even though
/// it's nested inside a larger polygon with a place type: it has a place
/// type of its own, so it takes no place-area step.
pub fn promote_nested_ranks<'a>(
    ranked: &mut [RankedPoly],
    place_areas: impl Iterator<Item = &'a AdminPolygon> + Clone,
    country: &str,
    within: impl Fn(&AdminPolygon, &AdminPolygon) -> bool,
) {
    ranked.sort_by(|a, b| {
        a.poly.admin_level.cmp(&b.poly.admin_level)
            .then(b.poly.area.partial_cmp(&a.poly.area).unwrap_or(std::cmp::Ordering::Equal))
    });
    // compute_place_rank's address_rank of each, which the place-area steps compare: a linked
    // place area's own place type's.
    let initial: Vec<u8> = ranked.iter()
        .map(|r| if r.poly.admin_level == 15 && r.poly.own_place_type != 0 { place_area_ranks(&r.poly, country).0 } else { r.rank_address })
        .collect();

    for i in 0..ranked.len() {
        let RankedPoly { poly, mut rank_address } = ranked[i];
        if poly.admin_level == 15 {
            // A place area inside an area of its rank: every polygon before it is a boundary,
            // or a larger place area, at its final rank.
            if PLACE_AREA_LIFTED_RANKS.contains(&initial[i]) {
                let lift = ranked[..i].iter().zip(&initial)
                    .filter(|&(area, &prank)| {
                        (initial[i]..=*PLACE_AREA_LIFT_RANKS.end()).contains(&prank)
                            && ADMIN_PARENT_RANKS.contains(&area.rank_address) && area.poly.area > poly.area
                    })
                    .filter(|(area, _)| within(&poly, &area.poly))
                    .max_by_key(|&(_, &prank)| prank)
                    .map(|(area, _)| area.rank_address);
                if let Some(area_rank) = lift {
                    ranked[i].rank_address = area_rank + 2;
                }
            }
            continue;
        }
        if rank_address == 0 { continue; }
        // One that repeats its parent and links a place node ranks 0 there, parent_address_level
        // staying 3, and takes the node's rank back where that is above 3 and below 26 (:1161).
        if poly.own_place_type == BOUNDARY_REPEATS_PARENT {
            let linked = place_type_to_rank(poly.place_type_override, country);
            ranked[i].rank_address = if (4..26).contains(&linked) { linked } else { 0 };
            continue;
        }

        // Step 1: admin nested.
        let parent_rank = ranked[..i].iter().rev()
            .find(|p| p.poly.admin_level > 3 && p.poly.admin_level < poly.admin_level && ADMIN_PARENT_RANKS.contains(&p.rank_address))
            .map(|p| p.rank_address);
        if let Some(parent) = parent_rank {
            if parent >= rank_address {
                rank_address = if parent >= 24 { MAX_NESTED_RANK } else { parent + 2 };
            }
        }

        // Step 2: place area containment, over the genuine class='place'
        // polygons only: admin boundaries with linked_place tags (Nairobi
        // L3, BA L8, Paris L6) are NOT used here.
        if poly.place_type_override == 0 && rank_address >= PLACE_AREA_MIN_RANK {
            let lift = place_areas.clone()
                .filter(|pa| pa.place_type_override != 0 && pa.area > poly.area)
                .map(|pa| (place_area_ranks(pa, country), pa))
                .filter(|&((prank, pa_rank), _)| PLACE_AREA_LIFT_RANKS.contains(&pa_rank) && prank >= rank_address)
                .filter(|(_, pa)| within(&poly, pa))
                .map(|(ranks, _)| ranks)
                .max();
            if let Some((_, pa_rank)) = lift {
                rank_address = pa_rank + 2;
            }
        }

        // Step 3: linked place. Nominatim's parent_address_level is 3 where
        // no parent was found.
        if poly.place_type_override > 0 {
            let linked_rank = place_type_to_rank(poly.place_type_override, country);
            if linked_rank > parent_rank.unwrap_or(3) && linked_rank > rank_address && linked_rank < 26 {
                rank_address = linked_rank;
            }
        }
        ranked[i].rank_address = rank_address;
    }
}

/// The lowest rank_address the place-area step promotes a boundary from
/// (placex_triggers.sql:912).
pub const PLACE_AREA_MIN_RANK: u8 = 10;

/// The ranks of the place areas that lift a boundary inside them
/// (placex_triggers.sql:920: rank_address between 1 and 23), and the highest
/// initial rank of an area that lifts a place area (:937, below 24).
pub const PLACE_AREA_LIFT_RANKS: std::ops::RangeInclusive<u8> = 1..=23;

/// The ranks of the place areas an area of their rank or more lifts when it
/// contains them (placex_triggers.sql:932).
pub const PLACE_AREA_LIFTED_RANKS: std::ops::RangeInclusive<u8> = 16..=23;

/// The ranks a boundary's admin-nested parent can have (placex_triggers.sql:891).
pub const ADMIN_PARENT_RANKS: std::ops::RangeInclusive<u8> = 1..=25;

/// The rank_address the admin-nested step lifts a boundary to under a parent
/// of rank 24 or more (placex_triggers.sql:903-904), the highest any boundary
/// takes.
pub const MAX_NESTED_RANK: u8 = 25;

/// The ranks of the areas in a street's or a house's address: getNearFeatures
/// takes only areas of rank_address below maxrank, 25 for those
/// (placex_triggers.sql:1218-1226, partition-functions.sql:46). An area of
/// rank 25 names nothing, nor does one of rank 0.
pub const ADDRESS_AREA_RANKS: std::ops::Range<u8> = 1..25;

/// The margin round a street of the admin cells the crosscheck reads areas from
/// (Index::crosscheck_rows): the street runs through an area only in a cell under it, and
/// cells_under_line cuts the street into boxes twice this across.
pub const STREET_CELLS_MARGIN_M: f64 = 50.0;

// --- String pool (tiered) ---
//
// Strings are split into 5 files: core/street/addr/postcode/poi.  Record
// name_ids are offsets into a global virtual pool spanning all tiers,
// contiguously in [bases[t], bases[t+1]).  Clients download the subset
// of tier files they need; any offset referencing an unloaded tier
// resolves to "" (graceful degradation).  strings_core.bin is required.
// Each file is FCC2, front-coded buckets (strings.rs); an offset into a
// tier is still an offset into its decoded pool.

pub const STR_TIER_COUNT: usize = 5;
pub const STR_TIER_FILENAMES: [&str; STR_TIER_COUNT] = [
    "strings_core.bin", "strings_street.bin", "strings_addr.bin",
    "strings_postcode.bin", "strings_poi.bin",
];

pub struct StringPool {
    pub tiers: [Option<strings::Tier>; STR_TIER_COUNT],
    pub bases: [u32; STR_TIER_COUNT + 1],
}

#[cfg(test)]
thread_local! {
    /// The strings `StringPool::get` has read on this thread, for tests that count a lookup's reads.
    static STRING_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl StringPool {
    /// Construct directly from opened tiers + parsed bases.  Used by
    /// the loader (which mmaps then opens).
    pub fn from_parts(tiers: [Option<strings::Tier>; STR_TIER_COUNT], bases: [u32; STR_TIER_COUNT + 1]) -> Self {
        StringPool {
            tiers,
            bases,
        }
    }

    pub fn load(dir: &str) -> Result<Self, String> {
        Self::load_tiers(dir, STR_TIER_COUNT)
    }

    /// `load`, opening only the first `count` tiers' files: the strings of a later tier read as
    /// "", whether its file is there or not.
    pub fn load_tiers(dir: &str, count: usize) -> Result<Self, String> {
        let layout_path = format!("{}/strings_layout.json", dir);
        let layout_src = std::fs::read_to_string(&layout_path)
            .map_err(|e| format!("Failed to read {}: {}", layout_path, e))?;
        let v: serde_json::Value = serde_json::from_str(&layout_src)
            .map_err(|e| format!("Failed to parse {}: {}", layout_path, e))?;
        let entries = v.get("tiers").and_then(|t| t.as_array())
            .ok_or_else(|| format!("{}: missing 'tiers' array", layout_path))?;
        if entries.len() != STR_TIER_COUNT {
            return Err(format!("{}: expected {} tier entries, got {}",
                layout_path, STR_TIER_COUNT, entries.len()));
        }
        let mut bases = [0u32; STR_TIER_COUNT + 1];
        let mut tiers: [Option<strings::Tier>; STR_TIER_COUNT] = Default::default();
        for (t, entry) in entries.iter().enumerate() {
            let start = entry.get("start").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
            let end   = entry.get("end").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
            bases[t] = start;
            bases[t + 1] = end;
            if t >= count {
                continue;
            }
            let path = format!("{}/{}", dir, STR_TIER_FILENAMES[t]);
            tiers[t] = mmap_file_optional(&path)
                .map(strings::Tier::open)
                .transpose()
                .map_err(|e| format!("{}: {}", path, e))?;
        }
        // Core tier is mandatory — admin record names resolve from it.
        if tiers[0].is_none() {
            return Err(format!("Required {}/{} missing", dir, STR_TIER_FILENAMES[0]));
        }
        Ok(StringPool::from_parts(tiers, bases))
    }

    /// The string at a global offset: borrowed from its tier's file when the
    /// file holds it whole, else built (strings.rs).
    pub fn get(&self, offset: u32) -> Cow<'_, str> {
        #[cfg(test)]
        STRING_READS.with(|n| n.set(n.get() + 1));
        if offset == NO_DATA { return Cow::Borrowed(""); }
        for t in 0..STR_TIER_COUNT {
            if offset < self.bases[t + 1] {
                let Some(tier) = &self.tiers[t] else { return Cow::Borrowed(""); };
                if offset < self.bases[t] { return Cow::Borrowed(""); }
                return tier.get(offset - self.bases[t]);
            }
        }
        Cow::Borrowed("")
    }
}

// --- Continent layout ---
//
// One Index per OSM continent. The builder writes per-continent
// directories (`<root>/<name>/`) with the same file shape as planet and
// assigns every record and admin cell to the continents whose Geofabrik
// boundary (`builder/data/*.poly`, embedded in build-index) contains the
// S2 cell centre. The server embeds the same files and parses them the
// same way, so a query is routed to exactly the continents that can
// hold its data.

const CONTINENT_POLYS: [(&str, &str); 8] = [
    ("africa", include_str!("../../builder/data/africa.poly")),
    ("asia", include_str!("../../builder/data/asia.poly")),
    ("europe", include_str!("../../builder/data/europe.poly")),
    ("north-america", include_str!("../../builder/data/north-america.poly")),
    ("south-america", include_str!("../../builder/data/south-america.poly")),
    ("oceania", include_str!("../../builder/data/australia-oceania.poly")),
    ("central-america", include_str!("../../builder/data/central-america.poly")),
    ("antarctica", include_str!("../../builder/data/antarctica.poly")),
];

pub struct Continent {
    pub name: &'static str,
    /// (lat, lng) of every ring concatenated and closed, as
    /// continent_boundaries.cpp builds it. Even-odd ray casting over the
    /// concatenation equals the union of the rings.
    vertices: Vec<(f64, f64)>,
    min_lat: f64,
    max_lat: f64,
    min_lng: f64,
    max_lng: f64,
}

impl Continent {
    fn parse(name: &'static str, poly: &str) -> Self {
        // .poly vertex lines are "lng lat"; ring names and END lines don't parse.
        let mut vertices: Vec<(f64, f64)> = poly.lines()
            .filter_map(|line| {
                let mut it = line.split_whitespace();
                let lng: f64 = it.next()?.parse().ok()?;
                let lat: f64 = it.next()?.parse().ok()?;
                Some((lat, lng))
            })
            .collect();
        if let (Some(&first), Some(&last)) = (vertices.first(), vertices.last()) {
            if first != last {
                vertices.push(first);
            }
        }

        let (mut min_lat, mut max_lat, mut min_lng, mut max_lng) = (f64::MAX, f64::MIN, f64::MAX, f64::MIN);
        for &(lat, lng) in &vertices {
            min_lat = min_lat.min(lat);
            max_lat = max_lat.max(lat);
            min_lng = min_lng.min(lng);
            max_lng = max_lng.max(lng);
        }
        Continent { name, vertices, min_lat, max_lat, min_lng, max_lng }
    }

    pub fn contains(&self, lat: f64, lng: f64) -> bool {
        if lat < self.min_lat || lat > self.max_lat || lng < self.min_lng || lng > self.max_lng {
            return false;
        }

        let mut inside = false;
        let mut j = self.vertices.len() - 1;
        for (i, &(yi, xi)) in self.vertices.iter().enumerate() {
            let (yj, xj) = self.vertices[j];
            if (yi > lat) != (yj > lat) && lng < (xj - xi) * (lat - yi) / (yj - yi) + xi {
                inside = !inside;
            }
            j = i;
        }
        inside
    }

    /// The builder assigns cells to continents by cell centre and a query's
    /// place and street lookups read its cell plus the 8 neighbours, so this
    /// continent can hold data for the query when it contains any of those 9
    /// centres (its admin cells reach one cell past it, for the admin scan's
    /// own cell).
    pub fn covers_query(&self, lat: f64, lng: f64, admin_cell_level: u64) -> bool {
        let cell = cell_id_at_level(lat, lng, admin_cell_level);
        std::iter::once(cell)
            .chain(cell_neighbors_at_level(cell, admin_cell_level))
            .any(|c| {
                let centre = LatLng::from(CellID(c));
                self.contains(centre.lat.deg(), centre.lng.deg())
            })
    }
}

pub fn continents() -> &'static [Continent] {
    static CONTINENTS: std::sync::OnceLock<Vec<Continent>> = std::sync::OnceLock::new();
    CONTINENTS.get_or_init(|| {
        CONTINENT_POLYS.iter().map(|&(name, poly)| Continent::parse(name, poly)).collect()
    })
}

// --- Admin cell scan ---

/// The admin rings one reverse lookup can return: the entry list of the point's level-10 cell, in
/// the order every admin scan visits them, and the point-in-polygon answers computed so far. The
/// builder lists a ring in every cell that holds a point the ring's straight lat/lng edges contain
/// (builder admin_cover.h), so no neighbour cell can add one.
pub struct AdminScan {
    /// The entries of the point's own cell, each with its polygon record (entries whose record
    /// `Index::admin_polygon` rejects are left out).
    rings: Vec<(u32, AdminPolygon)>,
    pip: ScanPip,
}

impl AdminScan {
    /// The scan of the point (`lat`, `lng`), rounded once to the e7 grid: its cell is that of the
    /// point every ring is tested at. A point off the globe, or NaN, is in no ring and its scan
    /// lists none: S2 would wrap it onto a cell of the globe ((10, 380) onto the cell of (10, 20)),
    /// whose interior entries hold that cell's points, not this one.
    pub fn new(index: &Index, lat: f64, lng: f64) -> Self {
        let point = AdminPoint::new(lat, lng);
        if !coords_in_range(lat, lng) {
            return AdminScan { rings: Vec::new(), pip: ScanPip { point, memo: PipMemo::new() } };
        }
        let (lat, lng) = point.degrees();
        // Each record and most ring headers are read below wherever the point is, so they are
        // requested before the first one is needed: their cache misses overlap.
        let record_bytes = ADMIN_POLYGON_RECORD_BYTES;
        let mut rings = Vec::new();
        index.admin_index.with_ids(cell_id_at_level(lat, lng, index.admin_cell_level), |ids| {
            ids.iter().for_each(|&id| index.admin_polygons.prefetch((id & ID_MASK) as u64 * record_bytes));
            for &id in ids {
                if let Some(poly) = index.admin_polygon(id & ID_MASK) { rings.push((id, poly)); }
            }
        });
        for (_, poly) in &rings {
            index.admin_geometry.prefetch(poly);
        }
        AdminScan { rings, pip: ScanPip { point, memo: PipMemo::new() } }
    }

    /// Calls `f(pip, entry, polygon)` for every ring, in visiting order.
    fn for_each_entry(&mut self, mut f: impl FnMut(&mut ScanPip, u32, AdminPolygon)) {
        for &(id, poly) in &self.rings {
            f(&mut self.pip, id, poly);
        }
    }
}

/// An admin scan's point and the `AdminGeometry::contains` answers computed there so far.
struct ScanPip {
    point: AdminPoint,
    memo: PipMemo,
}

impl ScanPip {
    fn contains(&mut self, index: &Index, poly: &AdminPolygon) -> bool {
        index.admin_geometry.contains(poly, self.point, &mut self.memo)
    }

    /// Whether the polygon of the scan's entry `id` holds the point. An entry flagged interior
    /// does without a test: the builder flags one only where the rings of every tier it covers
    /// hold the whole cell, at the reach the ray cast needs (admin_cover.h, cell_index.cpp
    /// union_of_tiers), and the scan's cell is the rounded point's, which is on the globe
    /// (`AdminScan::new`). Any other entry is tested.
    fn holds(&mut self, index: &Index, id: u32, poly: &AdminPolygon) -> bool {
        (id & INTERIOR_FLAG != 0 && index.admin_geometry.has_rings(poly)) || self.contains(index, poly)
    }
}

// --- Index data ---

pub struct Index {
    /// The cells' lists of street ways, address points and interpolation ways (geo_index.rs);
    /// none in a dir without streets.
    pub geo_index: GeoIndex,
    /// The street ways (way_files.rs), street_ways.bin and street_nodes.bin, which come with
    /// the geo index.
    pub streets: Option<WayFiles>,
    /// The address points (addr_files.rs), addr_points.bin, addr_vertices.bin and
    /// addr_index.bin; none in a dir without them.
    pub addrs: Option<AddrFiles>,
    /// The interpolation ways, interp_ways.bin and interp_nodes.bin; none in a dir without them.
    pub interps: Option<WayFiles>,
    // TIGER per-segment ZIP side file: a postcode string offset per interp
    // way (NO_DATA for OSM interpolations). Absent on builds without TIGER
    // data.
    pub interp_postcodes: Option<SideBlocks>,
    pub admin_index: CellPairs,
    pub admin_polygons: FileBytes,
    pub admin_geometry: AdminGeometry,
    pub poi_index: Option<CellPairs>,
    pub poi_records: Option<PoiRecords>,
    pub poi_vertices: Option<FileBytes>,
    pub poi_meta: PoiMeta,
    /// The place nodes: place_cells.bin, place_nodes.bin and place_hidden.bin (place_files.rs).
    pub places: Option<PlaceFiles>,
    // Parent chain files (Nominatim-style address walk)
    pub way_parents: Option<SideBlocks>,    // per way → smallest containing admin poly
    pub admin_parents: Option<FileBytes>,   // u32 per poly → next-larger containing admin poly
    pub addr_postcodes: Option<SideBlocks>, // per addr_point → postcode string (optional)
    /// Per address point: its addr:city, a core string where a place or a boundary has that name,
    /// addr_cities.bin; none in a dir without it.
    pub addr_cities: Option<SideBlocks>,
    pub way_postcodes: Option<SideBlocks>,  // per way → postcode string from postal boundary
    /// The postcode centroids: postcode_centroids.bin and postcode_centroid_cells.bin
    /// (postcode_files.rs).
    pub postcode_centroids: Option<PostcodeFiles>,
    /// The postcode grid (postcode_grid.bin): each cell's postcode in the countries it covers
    pub postcode_grid: Option<postcode_grid::PostcodeGrid>,
    pub admin_config: AdminLevelConfig,
    pub strings: StringPool,
    /// The water layer of the dataset's water dir (water.rs); none without one.
    pub water: Option<WaterLayer>,
    pub street_cell_level: u64,
    pub admin_cell_level: u64,
    pub max_distance_sq: f64,
    /// (boundary_key, name_id) of the place nodes each boundary links, ascending: Nominatim
    /// merges a linked node's names into its boundary's as `_place_*` (placex_indexing_prepare),
    /// which the crosscheck reads, and a boundary links one node at most (find_linked_place). A
    /// node labels one polygon of its boundary, the largest outer ring, and the key holds for
    /// every ring. From place_hidden.bin's labels, on first use.
    pub linked_names: std::sync::OnceLock<Vec<(u64, u32)>>,
    /// The credit of the data in the dir as it loaded (dir_credits): a download may replace the
    /// dir's files later, while this index answers from those it mapped.
    pub credits: Vec<serde_json::Value>,
}

/// Normalised street-name token overlap, mirroring the builder's
/// TokenMatcher (geometry.h) / Nominatim's token_matches_street: lowercase,
/// non-alphanumerics become separators, then test whether any token of >= 2
/// chars appears in both names. (The builder also folds Latin-1 accents; the
/// TIGER context here is US ASCII names, where that folding is a no-op.)
fn normalise_street_tokens(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_space = true;
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_space = false;
        } else if !last_space {
            out.push(' ');
            last_space = true;
        }
    }
    while out.ends_with(' ') {
        out.pop();
    }
    out
}

fn street_tokens_overlap(a: &str, b: &str) -> bool {
    let na = normalise_street_tokens(a);
    let nb = normalise_street_tokens(b);
    if na.is_empty() || nb.is_empty() {
        return false;
    }
    let bt: std::collections::HashSet<&str> = nb.split(' ').filter(|t| t.len() >= 2).collect();
    na.split(' ').filter(|t| t.len() >= 2).any(|t| bt.contains(t))
}

/// House number at fraction `t` along an interpolation way. Rounds to the
/// even/odd grid relative to start_number and clamps into [min, max] of the
/// range — the old open-coded version computed the offset in unsigned space,
/// so descending ranges (start > end) saturated the negative offset to 0.
/// Squared distance (radians²) from `origin` to an interpolation line, as far
/// as its point nearest in metres, as a street is (GeoOrigin::segment); and
/// the fraction along it, by its length in degrees, of its point nearest in
/// degrees, where Nominatim takes the house number (_locate_interpolation in
/// reverse.py: ST_LineLocatePoint on the lon/lat line, its first segment of
/// the least distance in degrees). None for a line without length.
fn interp_position(nodes: &[NodeCoord], origin: &GeoOrigin) -> Option<(f64, f64)> {
    if nodes.len() < 2 { return None; }
    // Lengths, not their squares: accumulating squares skews the along-way
    // fraction toward long segments.
    let seg_len = |i: usize| origin.length(nodes[i], nodes[i + 1]);
    let total_len: f64 = (0..nodes.len() - 1).map(seg_len).sum();
    if total_len == 0.0 { return None; }

    let mut best_dist = f64::MAX;
    // The segment of the point nearest in degrees: the one least far at its own such point.
    let (mut best_at, mut best_t) = (f64::MAX, 0.0);
    let mut accumulated = 0.0;
    for i in 0..nodes.len() - 1 {
        let len = seg_len(i);
        best_dist = best_dist.min(origin.segment(nodes[i], nodes[i + 1]).0);
        let (at, seg_t) = origin.segment_in_degrees(nodes[i], nodes[i + 1]);
        if at < best_at {
            best_at = at;
            best_t = (accumulated + seg_t * len) / total_len;
        }
        accumulated += len;
    }
    Some((best_dist, best_t))
}

fn interpolate_house_number(start: u32, end: u32, interpolation: u8, t: f64) -> u32 {
    let raw = start as f64 + t * (end as f64 - start as f64);
    let step = match interpolation { 1 | 2 => 2.0, _ => 1.0 };
    let lo = start.min(end) as f64;
    let hi = start.max(end) as f64;
    let snapped = if step == 2.0 {
        start as f64 + ((raw - start as f64) / 2.0).round() * 2.0
    } else {
        raw.round()
    };
    snapped.max(lo).min(hi) as u32
}

pub const NO_DATA: u32 = 0xFFFFFFFF;

thread_local! {
    /// The lists of a query's geo lookups (geo_index.rs), taken while the lookup's caller reads
    /// them and put back after.
    static GEO_LISTS: std::cell::Cell<GeoLists> = Default::default();

    /// The buffer a read decodes a way's nodes into (way_files.rs), taken while the read uses
    /// it and put back after, with room for WAY_NODES_KEPT nodes.
    static WAY_NODES: std::cell::Cell<Vec<NodeCoord>> = Default::default();

    /// The buffer a scan decodes an address's footprint into (addr_files.rs), taken and put
    /// back as the way buffer is, with room for FOOTPRINT_KEPT vertices.
    static FOOTPRINT: std::cell::Cell<Vec<NodeCoord>> = Default::default();
}

/// The nodes a thread's way buffer has room for between reads, 32 KiB: an OSM way has at most
/// 2,000 nodes, a way file's up to 65,535.
const WAY_NODES_KEPT: usize = 4096;

/// The vertices a thread's footprint buffer has room for between reads, 32 KiB: a building's
/// footprint has few, and nothing bounds how many one has.
const FOOTPRINT_KEPT: usize = 4096;

/// Runs `read` with the buffer this thread keeps in `kept`, which has room for `room`
/// coordinates between reads. A read of more grows it, and a buffer that grew is freed when
/// `read` returns.
fn with_buffer<R>(
    kept: &'static std::thread::LocalKey<std::cell::Cell<Vec<NodeCoord>>>,
    room: usize,
    read: impl FnOnce(&mut Vec<NodeCoord>) -> R,
) -> R {
    let mut buffer = kept.with(std::cell::Cell::take);
    if buffer.capacity() < room {
        buffer = Vec::with_capacity(room);
    }
    let room = buffer.capacity();
    let result = read(&mut buffer);
    if buffer.capacity() == room {
        kept.with(|kept| kept.set(buffer));
    }
    result
}

/// Runs `read` with this thread's way buffer to decode ways into.
fn with_way_nodes<R>(read: impl FnOnce(&mut Vec<NodeCoord>) -> R) -> R {
    with_buffer(&WAY_NODES, WAY_NODES_KEPT, read)
}

/// Runs `read` with this thread's footprint buffer to decode footprints into.
fn with_footprint<R>(read: impl FnOnce(&mut Vec<NodeCoord>) -> R) -> R {
    with_buffer(&FOOTPRINT, FOOTPRINT_KEPT, read)
}

// Entry-ID encoding for admin/POI cell entries: the high bit flags an
// "interior" entry (cell fully inside the polygon, a fast path for PIP),
// and the low 31 bits carry the polygon/POI index.
pub const INTERIOR_FLAG: u32 = 0x80000000;
pub const ID_MASK: u32 = 0x7FFFFFFF;

/// Record `idx` of the admin_polygons.bin `polygons`, a tombstone's too, beside an
/// admin_rings.bin of `rings_len` bytes; None past the end.
pub(crate) fn polygon_record(polygons: &FileBytes, rings_len: u64, idx: u32) -> Option<AdminPolygon> {
    let at = u64::from(idx) * ADMIN_POLYGON_RECORD_BYTES;
    if at + ADMIN_POLYGON_RECORD_BYTES > polygons.len() { return None; }
    // The record and the next one's ring offset, where its block ends.
    let last = at + 2 * ADMIN_POLYGON_RECORD_BYTES > polygons.len();
    let bytes = polygons.read_chunk(at, if last { 16 } else { 20 });
    let ring_end = if last {
        rings_len
    } else {
        u64::from(u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]))
    };
    Some(AdminPolygon::from_record(&bytes, ring_end))
}

pub fn mmap_file(path: &str) -> Result<FileBytes, String> {
    let file = File::open(path).map_err(|e| format!("Failed to open {}: {}", path, e))?;
    mmap_opened(&file, path)
}

/// `file`, opened at `path`, mapped as `mmap_file` maps one.
fn mmap_opened(file: &File, path: &str) -> Result<FileBytes, String> {
    let mmap = unsafe { Mmap::map(file).map_err(|e| format!("Failed to mmap {}: {}", path, e))? };
    // MADV_RANDOM disables kernel readahead.  All our access patterns are
    // random index lookups (cell binary search, polygon vertex slices,
    // string offsets) — readahead pulls in 32 pages of neighbours per
    // touched page, inflating RSS several × without helping the next
    // access.  Random-mode keeps RSS proportional to actually-touched
    // bytes.
    let _ = mmap.advise(memmap2::Advice::Random);
    Ok(FileBytes::Mmap(mmap))
}

pub fn mmap_file_optional(path: &str) -> Option<FileBytes> {
    let file = File::open(path).ok()?;
    let mmap = unsafe { Mmap::map(&file).ok() }?;
    let _ = mmap.advise(memmap2::Advice::Random);
    Some(FileBytes::Mmap(mmap))
}

/// Open a group of files that are only valid together: either the whole
/// group is present (returned as all-Some) or the whole group is absent
/// (all-None, a legitimate index variant — e.g. admin-minimal ships no POI
/// files). A half-present group means a corrupt or partially-synced index
/// dir; loading it would silently disable the feature (or worse), so it is
/// a hard load error instead.
fn mmap_group(dir: &str, names: &[&str]) -> Result<Vec<Option<FileBytes>>, String> {
    let maps: Vec<Option<FileBytes>> = names
        .iter()
        .map(|n| mmap_file_optional(&format!("{}/{}", dir, n)))
        .collect();
    let present = maps.iter().filter(|m| m.is_some()).count();
    if present != 0 && present != names.len() {
        let missing: Vec<&str> = names
            .iter()
            .zip(&maps)
            .filter(|(_, m)| m.is_none())
            .map(|(n, _)| *n)
            .collect();
        return Err(format!(
            "Partial index group [{}]: missing [{}]",
            names.join(", "),
            missing.join(", ")
        ));
    }
    Ok(maps)
}

/// The place files of the index in `dir` (place_files.rs), mapped; none without them.
fn load_places(dir: &str) -> Result<Option<PlaceFiles>, String> {
    let [cells, nodes, hidden]: [Option<FileBytes>; 3] =
        mmap_group(dir, &[place_files::CELLS_FILE, place_files::NODES_FILE, place_files::HIDDEN_FILE])?
            .try_into().map_err(|_| "place group arity")?;
    match (cells, nodes, hidden) {
        (Some(cells), Some(nodes), Some(hidden)) => PlaceFiles::open(cells, nodes, hidden).map(Some).map_err(|e| format!("{dir}: {e}")),
        _ => Ok(None),
    }
}

/// The way files of `kind` in `dir` (way_files.rs), opened; none where it has neither. An error
/// for one file without the other, and for files no builder writes.
fn open_ways(dir: &str, kind: way_files::Kind, ways: Option<FileBytes>, nodes: Option<FileBytes>) -> Result<Option<WayFiles>, String> {
    match (ways, nodes) {
        (Some(ways), Some(nodes)) => WayFiles::open(kind, ways, nodes).map(Some).map_err(|e| format!("{dir}: {e}")),
        (None, None) => Ok(None),
        (Some(_), None) => Err(format!("{dir}: {} without {}", kind.ways_file(), kind.nodes_file())),
        (None, Some(_)) => Err(format!("{dir}: {} without {}", kind.nodes_file(), kind.ways_file())),
    }
}

/// The side file `name` of the index in `dir` (side_blocks.rs), mapped; none without it. Its
/// values are read by the ids of the records of `parent`, the file it sits beside: an error
/// where it holds another count than `records`, the parent's, or the dir has no such parent.
fn load_side(dir: &str, name: &str, parent: &str, records: Option<u64>) -> Result<Option<SideBlocks>, String> {
    let Some(bytes) = mmap_file_optional(&format!("{dir}/{name}")) else { return Ok(None) };
    let side = SideBlocks::open(bytes, name).map_err(|e| format!("{dir}: {e}"))?;
    match records {
        Some(records) if records == u64::from(side.count()) => Ok(Some(side)),
        Some(records) => Err(format!("{dir}: {name} holds {} values for the {records} records of {parent}", side.count())),
        None => Err(format!("{dir}: {name} without {parent}")),
    }
}

thread_local! {
    /// The blocks of a postcode centroid lookup (PostcodeFiles::blocks_near), kept for the next.
    static NEAR_BLOCKS: std::cell::RefCell<Vec<postcode_files::NearBlock>> = const { std::cell::RefCell::new(Vec::new()) };
}

impl Index {
    pub fn load(dir: &str, street_cell_level: u64, admin_cell_level: u64, search_distance: f64) -> Result<Self, String> {
        if let Some(why) = geo_reach_refused(search_distance, street_cell_level) {
            return Err(why);
        }
        // The reach of the geo lookups and the primary-feature selection.
        // The internal distances are stored in radians² (via `to_radians()`
        // before squaring), so convert metres → radians → radians².
        // Earth radius is 6 371 000 m.
        let radians = search_distance / 6_371_000.0;
        let max_distance_sq = radians * radians;

        let [geo_cells, geo_entries]: [Option<FileBytes>; 2] =
            mmap_group(dir, &["geo_cells.bin", "geo_entries.bin"])?.try_into().map_err(|_| "geo group arity")?;
        let has_geo = geo_cells.is_some();
        let geo_index = match (geo_cells, geo_entries) {
            (Some(top), Some(blocks)) => GeoIndex::open(top, blocks, street_cell_level)
                .map_err(|e| format!("{dir}/geo_cells.bin: {e}"))?,
            _ => GeoIndex::none(),
        };

        let required_geo = |name: &str| -> Result<Option<FileBytes>, String> {
            let path = format!("{}/{}", dir, name);
            let mmap = mmap_file_optional(&path);
            if has_geo && mmap.is_none() {
                return Err(format!("Partial geo index: {} is missing", name));
            }
            Ok(mmap)
        };

        // Files that are only meaningful together load as all-or-nothing
        // groups (whole-group-absent = a thinner index variant; half-present
        // = corruption).
        let file = |name: &str| mmap_file_optional(&format!("{dir}/{name}"));
        let addrs = AddrFiles::open(file(addr_files::POINTS_FILE), file(addr_files::VERTICES_FILE), file(addr_files::INDEX_FILE))
            .map_err(|e| format!("{dir}: {e}"))?;
        let [interp_ways, interp_nodes]: [Option<FileBytes>; 2] =
            mmap_group(dir, &["interp_ways.bin", "interp_nodes.bin"])?
                .try_into().map_err(|_| "interp group arity")?;
        let interps = open_ways(dir, way_files::Kind::Interps, interp_ways, interp_nodes)?;
        let [poi_cells, poi_entries, poi_records, poi_vertices]: [Option<FileBytes>; 4] =
            mmap_group(dir, &["poi_cells.bin", "poi_entries.bin", "poi_records.bin", "poi_vertices.bin"])?
                .try_into().map_err(|_| "poi group arity")?;
        let places = load_places(dir)?;
        let [postcode_centroids, postcode_centroid_cells]: [Option<FileBytes>; 2] =
            mmap_group(dir, &[postcode_files::CENTROIDS_FILE, postcode_files::CELLS_FILE])?
                .try_into().map_err(|_| "centroid group arity")?;
        let postcode_centroids = match (postcode_centroids, postcode_centroid_cells) {
            (Some(centroids), Some(cells)) => {
                Some(PostcodeFiles::open(centroids, cells, admin_cell_level).map_err(|e| format!("{dir}: {e}"))?)
            }
            _ => None,
        };
        let postcode_grid = file(postcode_grid::FILE)
            .map(|f| postcode_grid::PostcodeGrid::open(f).map_err(|e| format!("{dir}: {e}")))
            .transpose()?;
        // A cell pair index (cell_pairs.rs) of the dir, an error naming its cells file.
        let pairs = |prefix: &str, cells: FileBytes, entries: FileBytes, format: cell_pairs::Format| {
            CellPairs::open(cells, entries, format, admin_cell_level).map_err(|e| format!("{dir}/{prefix}_cells.bin: {e}"))
        };
        let poi_records =
            poi_records.map(|f| PoiRecords::open(f).map_err(|e| format!("{dir}/poi_records.bin: {e}"))).transpose()?;
        let poi_index = match (poi_cells, poi_entries) {
            (Some(c), Some(e)) => Some(pairs("poi", c, e, cell_pairs::POI)?),
            _ => None,
        };
        let admin_index = pairs("admin", mmap_file(&format!("{}/admin_cells.bin", dir))?,
                                mmap_file(&format!("{}/admin_entries.bin", dir))?, cell_pairs::ADMIN)?;
        let streets =
            open_ways(dir, way_files::Kind::Streets, required_geo("street_ways.bin")?, required_geo("street_nodes.bin")?)?;
        // Each side file is optional, and holds a value for each record of the file it sits
        // beside.
        let ways = |files: &Option<WayFiles>| files.as_ref().map(|files| u64::from(files.count()));
        let way_parents = load_side(dir, "way_parents.bin", "street_ways.bin", ways(&streets))?;
        let way_postcodes = load_side(dir, "way_postcodes.bin", "street_ways.bin", ways(&streets))?;
        let addr_records = addrs.as_ref().map(|addrs| u64::from(addrs.count()));
        let addr_postcodes = load_side(dir, "addr_postcodes.bin", addr_files::POINTS_FILE, addr_records)?;
        let addr_cities = load_side(dir, "addr_cities.bin", addr_files::POINTS_FILE, addr_records)?;
        let interp_postcodes = load_side(dir, "interp_postcodes.bin", "interp_ways.bin", ways(&interps))?;

        Ok(Index {
            geo_index,
            streets,
            addrs,
            interps,
            interp_postcodes,
            admin_index,
            admin_polygons: mmap_file(&format!("{}/admin_polygons.bin", dir))?,
            admin_geometry: AdminGeometry::load(dir)?,
            poi_index,
            poi_records,
            poi_vertices,
            poi_meta: PoiMeta::load(dir),
            places,
            way_parents,
            admin_parents: mmap_file_optional(&format!("{}/admin_parents.bin", dir)),
            addr_postcodes,
            addr_cities,
            way_postcodes,
            postcode_centroids,
            postcode_grid,
            admin_config: AdminLevelConfig::load(),
            strings: StringPool::load(dir)?,
            water: WaterLayer::open(&format!("{dir}/{WATER_DIR}"), admin_cell_level)?,
            street_cell_level,
            admin_cell_level,
            max_distance_sq,
            linked_names: Default::default(),
            credits: dir_credits(dir),
        })
    }

    pub fn get_string(&self, offset: u32) -> Cow<'_, str> {
        self.strings.get(offset)
    }

    /// A POI polygon's vertices, decoded from its block in `poi_vertices` (optional file): None
    /// for a point.
    pub fn poi_verts(&self, poly: &PoiRecord) -> Option<std::borrow::Cow<'_, [NodeCoord]>> {
        self.poi_records.as_ref()?.polygon(self.poi_vertices.as_ref()?, poly).map(std::borrow::Cow::Owned)
    }

    /// A POI's vertex count: 0 for a point, None where its polygon can't be read.
    pub fn poi_vertex_count(&self, poi: &PoiRecord) -> Option<u32> {
        if !poi.is_polygon() {
            return Some(0);
        }
        self.poi_records.as_ref()?.vertex_count(self.poi_vertices.as_ref()?, poi)
    }

    /// The place nodes, visible and hidden: their ids run below this.
    pub fn place_node_count(&self) -> u32 {
        self.places.as_ref().map_or(0, PlaceFiles::count)
    }

    /// Place node `id`, visible or hidden; None past the last.
    pub fn place_node_record(&self, id: u32) -> Option<PlaceNode> {
        self.places.as_ref()?.node(id)
    }

    /// Calls `f` with each place node of ids [first, end) and its id, in id order.
    pub fn for_each_place_node(&self, first: u32, end: u32, f: impl FnMut(u32, PlaceNode)) {
        if let Some(places) = &self.places {
            places.for_nodes(first, end, f);
        }
    }

    /// Calls `f` with each visible place node in the admin cells `cells`, in id order, each once.
    fn for_each_place_in(&self, cells: impl IntoIterator<Item = u64>, mut f: impl FnMut(PlaceNode)) {
        let Some(places) = &self.places else { return };
        let mut ranges: Vec<(u32, u32)> = cells.into_iter().map(|c| places.cell_rows(c)).filter(|&(a, b)| a < b).collect();
        ranges.sort_unstable();
        ranges.dedup();
        for (first, end) in ranges {
            places.for_rows(first, end, |_, pn| f(pn));
        }
    }

    #[inline]
    pub fn poi_record(&self, idx: u32) -> Option<PoiRecord> {
        let pr = self.poi_records.as_ref()?.record(idx)?;
        // The builder ships no unnamed POI, so a nameless record is a tombstone.
        if pr.name_id == NO_DATA { return None; }
        Some(pr)
    }

    /// The records admin_polygons.bin holds, tombstones included.
    pub fn admin_polygon_count(&self) -> u32 {
        u32::try_from(self.admin_polygons.len() / ADMIN_POLYGON_RECORD_BYTES).unwrap_or(u32::MAX)
    }

    /// Record `idx` of admin_polygons.bin, a tombstone's too; None past the end.
    pub fn admin_polygon_record(&self, idx: u32) -> Option<AdminPolygon> {
        polygon_record(&self.admin_polygons, self.admin_geometry.rings_len(), idx)
    }

    pub fn admin_polygon(&self, idx: u32) -> Option<AdminPolygon> {
        self.admin_polygon_record(idx).filter(|ap| ap.name_id != NO_DATA)
    }

    // --- Geo lookup (streets, addresses, interpolation from merged index) ---

    /// The street-level cells every geo lookup reads round the point, nearest first, whose lists
    /// GEO_LISTS keeps for the lookups after the first: those of the cover (geo_cover), which
    /// every way within the search distance is listed in some of.
    fn geo_cells_around(&self, lat: f64, lng: f64) -> NearCells {
        cells_within(lat, lng, self.street_cell_level, geo_cover(self.max_distance_sq.sqrt(), self.street_cell_level))
    }

    /// How many of a lookup's cells (geo_cells_around) the address scans read: those within the
    /// address cover, which lists every address point within the search distance.
    fn address_cells(&self, cells: &NearCells) -> usize {
        cells.within(address_cover(self.max_distance_sq.sqrt(), self.street_cell_level))
    }

    /// Where the geo lookups at (lat, lng) measure from.
    fn geo_origin(&self, lat: f64, lng: f64) -> GeoOrigin {
        GeoOrigin::new(lat, lng, geo_cover(self.max_distance_sq.sqrt(), self.street_cell_level))
    }

    /// What decides between two address points at one exact distance (see
    /// `beats`): the distance of the point itself (a building's distance is
    /// its outline's), then its house number and street as text.
    fn addr_tie_key(&self, a: &AddrRecord, origin: &GeoOrigin) -> (f64, Cow<'_, str>, Cow<'_, str>) {
        let d = origin.point_sq(a.lat as f64, a.lng as f64);
        (d, self.get_string(a.housenumber_id), self.get_string(a.street_id))
    }

    /// What decides between two streets at one exact distance: the name as
    /// text, then the way's first node. Ways that share both fall to the
    /// order the cells list them in, which is record order.
    fn street_tie_key(&self, streets: &WayFiles, id: u32) -> (Cow<'_, str>, f32, f32) {
        let name = streets.row(id).map_or(Cow::Borrowed(""), |way| self.get_string(way.name));
        let first = streets.first_node(id).unwrap_or(NodeCoord { lat: 0.0, lng: 0.0 });
        (name, first.lat, first.lng)
    }

    /// What decides between two interpolation lines at one exact distance
    /// (lines that meet at the point): the street as text, the range, the
    /// type, then the line's nodes. Lines equal in all of them are one line,
    /// which the builder folds, so record order never decides.
    fn interp_tie_key(&self, interps: &WayFiles, id: u32, iw: &WayRow) -> (Cow<'_, str>, u32, u32, u8, Vec<(f32, f32)>) {
        let mut nodes = Vec::new();
        interps.way(id, &mut nodes);
        let nodes = nodes.iter().map(|node| (node.lat, node.lng)).collect();
        (self.get_string(iw.name), iw.start_number, iw.end_number, iw.interpolation, nodes)
    }

    /// The address point, the interpolation and the street way nearest (lat, lng) within the
    /// search distance, each with its squared distance and its id: the address's record, the
    /// interpolation's street and the house number at the point, the street's row.
    pub fn query_geo(&self, lat: f64, lng: f64)
        -> (Option<(f64, AddrRecord, u32)>, Option<(f64, Cow<'_, str>, u32, u32)>, Option<(f64, WayRow, u32)>)
    {
        if self.geo_index.is_empty() {
            return (None, None, None);
        }
        // Street files always accompany the geo index in a well-formed dir
        // (required_geo enforces all-or-nothing at load). Guard rather than
        // unwrap so a partial/admin-only index degrades gracefully instead
        // of panicking.
        let Some(streets) = self.streets.as_ref() else {
            return (None, None, None);
        };
        let cells = self.geo_cells_around(lat, lng);
        let mut lists = GEO_LISTS.with(std::cell::Cell::take);
        self.geo_index.lookup_many(&cells.ids, geo_index::ALL_TABLES, &mut lists);

        let origin = self.geo_origin(lat, lng);
        let addr_result = self.nearest_addr(&lists, &cells, &origin);
        let street_result = self.nearest_street(streets, &lists, &cells, &origin);
        let interp_result = self.nearest_interp(&lists, &cells, &origin);

        GEO_LISTS.with(|g| g.set(lists));

        (addr_result, interp_result, street_result)
    }

    /// The address point nearest `origin` within the search distance in `lists`, a lookup's
    /// lists of `cells`: its squared distance, its record and its id. An exact tie goes by
    /// `addr_tie_key`, then to the point read first: cells are read in the order the lookup
    /// asked for them, a cell's ids in its list's.
    fn nearest_addr(&self, lists: &GeoLists, cells: &NearCells, origin: &GeoOrigin)
        -> Option<(f64, AddrRecord, u32)>
    {
        let addrs = self.addrs.as_ref()?;
        let addr_key = |a: &AddrRecord| self.addr_tie_key(a, origin);

        let mut best_addr_dist = self.max_distance_sq;
        let mut best_addr: Option<AddrRecord> = None;
        let mut best_addr_id: u32 = NO_DATA;
        // One run over the lists of every cell of the address cover: a building is listed in the
        // cell of its point but is as far as its outline, which can come nearer than that cell. A
        // corrupt cell entry reads as no record: an id past the last address, a record whose
        // block can't be read.
        let ids = (0..self.address_cells(cells)).flat_map(|i| lists.list(i, geo_index::ADDRESSES).iter().copied());
        with_footprint(|footprint| {
            addrs.for_ids(ids, |id, point| {
                let mut dist = origin.point_sq(point.lat as f64, point.lng as f64);
                // A building is as far as its outline, matching ST_Distance behaviour; one
                // whose footprint can't be read, as its point.
                if point.is_building() && addrs.footprint(&point, footprint) {
                    dist = origin.polygon_sq(footprint);
                }
                if beats(dist, best_addr_dist, || best_addr.as_ref().filter(|_| best_addr_id != id).map(|b| (addr_key(&point), addr_key(b)))) {
                    best_addr_dist = dist;
                    best_addr = Some(point);
                    best_addr_id = id;
                }
            })
        });
        best_addr.map(|p| (best_addr_dist, p, best_addr_id))
    }

    /// The street way nearest `origin` within the search distance in `lists`, read in
    /// `nearest_addr`'s order, from every cell: its squared distance, its row and its id. An exact
    /// tie goes by `street_tie_key`, then to the way read first.
    fn nearest_street(&self, streets: &WayFiles, lists: &GeoLists, cells: &NearCells, origin: &GeoOrigin)
        -> Option<(f64, WayRow, u32)>
    {
        let mut best_street_dist = self.max_distance_sq;
        let mut best_street: Option<u32> = None;

        // Fixed-size hash set on stack to skip duplicate street IDs across cells
        let mut seen_streets: [u32; 64] = [u32::MAX; 64];

        with_way_nodes(|nodes| {
            // Every cell of the cover: the builder lists a way in some of them, though not
            // always in the cells it crosses (listing_gap).
            for i in 0..cells.ids.len() {
                for &id in lists.list(i, geo_index::STREETS) {
                    let slot = (id as usize) & 0x3F;
                    if seen_streets[slot] == id { continue; }
                    seen_streets[slot] = id;

                    // A corrupt cell entry reads as no way: an id past the last way, a way
                    // whose block can't be read.
                    if !streets.way(id, nodes) || nodes.len() < 2 { continue; }
                    for i in 0..nodes.len() - 1 {
                        let (dist, _) = origin.segment(nodes[i], nodes[i + 1]);
                        // A tie with the way's own earlier segment (the node two segments
                        // share is the nearest point) needs no key.
                        let keys = || best_street.filter(|&best| best != id).map(|best| (self.street_tie_key(streets, id), self.street_tie_key(streets, best)));
                        if beats(dist, best_street_dist, keys) {
                            best_street_dist = dist;
                            best_street = Some(id);
                        }
                    }
                }
            }
        });
        let id = best_street?;
        Some((best_street_dist, streets.row(id)?, id))
    }

    /// The interpolation way nearest `origin` within the search distance in `lists`, read in
    /// `nearest_addr`'s order, from every cell: its squared distance, its street, the house
    /// number at the point of its line nearest `origin`, and its id. Equally near ways go to the
    /// lesser `interp_tie_key`.
    fn nearest_interp(&self, lists: &GeoLists, cells: &NearCells, origin: &GeoOrigin)
        -> Option<(f64, Cow<'_, str>, u32, u32)>
    {
        let interps = self.interps.as_ref()?;

        let mut best_interp_dist = self.max_distance_sq;
        let mut best_interp: Option<(u32, WayRow)> = None;
        let mut best_interp_t: f64 = 0.0;
        // Every cell of the cover, as for a street.
        for id in interps_once(lists, cells) {
            // A corrupt cell entry reads as no way. The row comes first: a way without
            // numbers costs no node.
            let Some(iw) = interps.row(id) else { continue };
            if iw.start_number == 0 || iw.end_number == 0 { continue; }
            let Some((dist, t)) = self.interp_way_position(interps, id, origin) else { continue };
            // A line several cells list ties with itself, which needs no key.
            let keys = || best_interp.as_ref().filter(|(best, _)| *best != id).map(|(best, b)| (self.interp_tie_key(interps, id, &iw), self.interp_tie_key(interps, *best, b)));
            if beats(dist, best_interp_dist, keys) {
                best_interp_dist = dist;
                best_interp = Some((id, iw));
                best_interp_t = t;
            }
        }
        best_interp.map(|(id, iw)| {
            let number = interpolate_house_number(
                iw.start_number, iw.end_number, iw.interpolation, best_interp_t);
            (best_interp_dist, self.get_string(iw.name), number, id)
        })
    }

    // Nominatim's `_find_housenumber_for_street` (reverse.py:231):
    // given a street that has just won primary selection, look up the
    // nearest address whose parent_way_id matches that street's
    // index, gated by `ST_DWithin(query, 0.001)` (~100m). Returns the
    // address whose housenumber should refine the result, with its record id.
    // Like every geo lookup it reaches no farther than the search distance,
    // in the cells query_geo read.
    pub fn find_addr_on_way(&self, lat: f64, lng: f64, parent_way_idx: u32, max_dist_sq: f64)
        -> Option<(u32, AddrRecord)>
    {
        let addrs = self.addrs.as_ref()?;

        let cells = self.geo_cells_around(lat, lng);
        let mut lists = GEO_LISTS.with(std::cell::Cell::take);
        self.geo_index.lookup_many(&cells.ids, 1 << geo_index::ADDRESSES, &mut lists);
        let origin = self.geo_origin(lat, lng);
        let addr_key = |a: &AddrRecord| self.addr_tie_key(a, &origin);

        let mut best_dist = max_dist_sq.min(self.max_distance_sq);
        let mut best: Option<(u32, AddrRecord)> = None;
        // The lists nearest_addr reads. Of an address under another way no more than its street
        // and its parent is read.
        let ids = (0..self.address_cells(&cells)).flat_map(|i| lists.list(i, geo_index::ADDRESSES).iter().copied());
        with_footprint(|footprint| {
            addrs.for_ids_on_way(ids, parent_way_idx, |id, point| {
                let mut dist = origin.point_sq(point.lat as f64, point.lng as f64);
                if point.is_building() && addrs.footprint(&point, footprint) {
                    dist = origin.polygon_sq(footprint);
                }
                if beats(dist, best_dist, || best.as_ref().map(|(_, b)| (addr_key(&point), addr_key(b)))) {
                    best_dist = dist;
                    best = Some((id, point));
                }
            })
        });
        GEO_LISTS.with(|g| g.set(lists));
        best
    }

    /// interp_position over interpolation way `id`'s nodes; None for a way
    /// whose nodes can't be read.
    fn interp_way_position(&self, interps: &WayFiles, id: u32, origin: &GeoOrigin) -> Option<(f64, f64)> {
        with_way_nodes(|nodes| if interps.way(id, nodes) { interp_position(nodes, origin) } else { None })
    }

    /// Nominatim's `_find_tiger_number_for_street` (reverse.py): when a US
    /// street wins without a matching address point, the nearest
    /// interpolation line of that street within `max_dist_sq` gives the
    /// house number. TIGER lines carry no parent street, so "of that
    /// street" is the name-token gate the TIGER postcode tier uses.
    /// Equally near lines (TIGER's odd and even sides share the street
    /// centreline) go to the lower start number and type, then to the lesser
    /// interp_tie_key, never to record order. Like every geo lookup it reaches
    /// no farther than the search distance.
    pub fn find_interp_on_way(&self, lat: f64, lng: f64, street_name: &str, max_dist_sq: f64) -> Option<u32> {
        let interps = self.interps.as_ref()?;

        let cells = self.geo_cells_around(lat, lng);
        let mut lists = GEO_LISTS.with(std::cell::Cell::take);
        self.geo_index.lookup_many(&cells.ids, 1 << geo_index::INTERPOLATIONS, &mut lists);
        let origin = self.geo_origin(lat, lng);
        let max_dist_sq = max_dist_sq.min(self.max_distance_sq);

        let mut best: Option<(f64, u32, WayRow, f64)> = None;
        // Every cell of the cover, as in nearest_interp.
        for id in interps_once(&lists, &cells) {
            // The row comes first: only a line of the street's name costs its nodes.
            let Some(iw) = interps.row(id) else { continue };
            if iw.start_number == 0 || iw.end_number == 0 { continue; }
            if !street_tokens_overlap(&self.get_string(iw.name), street_name) { continue; }
            let Some((dist, t)) = self.interp_way_position(interps, id, &origin) else { continue };
            if dist >= max_dist_sq { continue; }
            let range = |w: &WayRow| (w.start_number, w.interpolation);
            let better = match &best {
                None => true,
                Some((bd, best_id, bw, _)) => dist < *bd
                    || (dist == *bd
                        && (range(&iw) < range(bw)
                            || (range(&iw) == range(bw)
                                && *best_id != id
                                && self.interp_tie_key(interps, id, &iw) < self.interp_tie_key(interps, *best_id, bw)))),
            };
            if better { best = Some((dist, id, iw, t)); }
        }
        GEO_LISTS.with(|g| g.set(lists));
        best.map(|(_, _, iw, t)| interpolate_house_number(iw.start_number, iw.end_number, iw.interpolation, t))
    }

    // Debug helper — returns primary feature distances + names. Each of
    // the address, street and interpolation winners also comes with what
    // tells two builds' answers apart: its record id, its distance exactly
    // (`dist_bits`, the bits of the squared distance the scan compared) and
    // the key that breaks an exact tie (`tie`, floats as their bits, -0 as
    // +0 since the scan compares them equal). Two winners with equal
    // `dist_bits` and `tie` were told apart by record order alone; an
    // interpolation's key is its whole content, so two such are one line.
    pub fn debug_primary(&self, lat: f64, lng: f64) -> serde_json::Value {
        let (addr, interp, street) = self.query_geo(lat, lng);
        let poi_primary = self.find_nearest_poi_with_parent(lat, lng);
        let to_m = |d2: f64| (d2.sqrt() * 6_371_000.0) as f64;
        let bits = |d: f64| format!("{:016x}", d.to_bits());
        let float_bits = |v: f32| format!("{:08x}", if v == 0.0 { 0 } else { v.to_bits() });
        let origin = self.geo_origin(lat, lng);
        serde_json::json!({
            "addr": addr.map(|(d, p, id)| {
                let (point_dist, house_number, street) = self.addr_tie_key(&p, &origin);
                serde_json::json!({
                    "dist_m": to_m(d),
                    "house_number": if p.housenumber_id != NO_DATA { self.get_string(p.housenumber_id) } else { Cow::Borrowed("") },
                    "street": if p.street_id != NO_DATA { self.get_string(p.street_id) } else { Cow::Borrowed("") },
                    "lat": p.lat, "lng": p.lng,
                    "vertex_count": self.addrs.as_ref().map_or(0, |addrs| addrs.vertex_count(&p)),
                    "id": id,
                    "dist_bits": bits(d),
                    "tie": [serde_json::json!(bits(point_dist)), serde_json::json!(house_number), serde_json::json!(street)],
                })
            }),
            "street": street.map(|(d, w, idx)| serde_json::json!({
                "dist_m": to_m(d),
                "name": self.get_string(w.name),
                "id": idx,
                "dist_bits": bits(d),
                "tie": self.streets.as_ref().map(|streets| {
                    let (name, first_lat, first_lng) = self.street_tie_key(streets, idx);
                    serde_json::json!([name, float_bits(first_lat), float_bits(first_lng)])
                }),
            })),
            "interp": interp.map(|(d, s, n, id)| serde_json::json!({
                "dist_m": to_m(d),
                "street": s,
                "number": n,
                "id": id,
                "dist_bits": bits(d),
                "tie": self.interps.as_ref().and_then(|interps| {
                    let (street, start, end, kind, nodes) = self.interp_tie_key(interps, id, &interps.row(id)?);
                    let nodes: String = nodes.iter().map(|&(lat, lng)| float_bits(lat) + &float_bits(lng)).collect();
                    Some(serde_json::json!([street, start, end, kind, nodes]))
                }),
            })),
            "poi": poi_primary.map(|(d, p)| serde_json::json!({
                "dist_m": to_m(d),
                "name": self.get_string(p.name_id),
                "parent_street": if p.parent_street_id != NO_DATA { self.get_string(p.parent_street_id) } else { Cow::Borrowed("") },
                "vertex_count": self.poi_vertex_count(&p),
                "category": p.category,
                "category_name": self.poi_meta.category_name(p.category),
            })),
        })
    }

    pub fn debug_places(&self, lat: f64, lng: f64) -> serde_json::Value {
        let mut admin_scan = AdminScan::new(self, lat, lng);
        let admin = self.find_admin_in(&mut admin_scan);
        let current_boundaries = self.find_current_boundaries(&mut admin_scan, admin.country_code);
        let place = self.find_places(PlaceFeature::Point(lat, lng), &current_boundaries, 0, None);

        if self.places.is_none() { return serde_json::json!({"error": "no place nodes"}); }
        let to_m = |d2: f64| (d2.sqrt() * 6_371_000.0) as f64;
        let cos_lat = lat.to_radians().cos();
        let type_names = ["city", "town", "village", "suburb", "hamlet", "neighbourhood", "quarter"];

        // The cells find_places looks in around the point.
        let mut nearby: Vec<serde_json::Value> = Vec::new();
        self.for_each_place_in(cells_near_point(lat, lng, MAX_PLACE_FUZZY_HALF_SIDE_M, self.admin_cell_level), |pn| {
                let pt = pn.place_type as usize;
                if pt >= 7 { return; }
                let dlat = (lat - pn.lat as f64).to_radians();
                let dlng = (lng - pn.lng as f64).to_radians();
                let dist_sq = dlat * dlat + dlng * dlng * cos_lat * cos_lat;
                let dist_m = to_m(dist_sq);
                if dist_m > DEBUG_PLACES_NEARBY_M { return; }
                nearby.push(serde_json::json!({
                    "type": type_names[pt],
                    "name": self.get_string(pn.name_id),
                    "dist_m": (dist_m * 10.0).round() / 10.0,
                    "lat": pn.lat, "lng": pn.lng,
                }));
        });
        nearby.sort_by(|a, b| {
            a["dist_m"].as_f64().unwrap_or(f64::MAX)
                .partial_cmp(&b["dist_m"].as_f64().unwrap_or(f64::MAX))
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        serde_json::json!({
            "result": {
                "city": place.city, "town": place.town, "village": place.village,
                "suburb": place.suburb, "hamlet": place.hamlet,
                "neighbourhood": place.neighbourhood, "quarter": place.quarter,
            },
            "current_boundary": CurrentBoundaries::RANKS.iter().zip(current_boundaries.gates.iter())
                .map(|(rank, p)| (rank.to_string(), p.as_ref().map(|p| self.get_string(p.name_id)).into()))
                .collect::<serde_json::Map<String, serde_json::Value>>(),
            "nearby_nodes": nearby,
        })
    }

    // --- Admin boundary lookup (point-in-polygon) ---

    pub fn debug_admin(&self, lat: f64, lng: f64) -> serde_json::Value {
        let (point, mut memo) = (AdminPoint::new(lat, lng), PipMemo::new());
        let (lat, lng) = point.degrees();
        let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.admin_cell_level);

        let mut best_by_level: [Option<(f32, AdminPolygon, bool)>; 12] = [None; 12];
        let mut seen_in_level: [Vec<serde_json::Value>; 12] = Default::default();

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            self.admin_index.for_each(c, |id| {
                // The interior flag on a (cell, polygon) entry proves
                // containment only for queries INSIDE that cell. Neighbor
                // cells are iterated for near-miss distance work; honoring
                // their interior flags claimed containment for points up to
                // a full cell away (a Dutch query 'inside' a German
                // naturpark). A polygon truly containing the query always
                // has the query's own cell in its covering, so this loses
                // no true positives.
                let is_interior = c == cell && (id & INTERIOR_FLAG) != 0;
                let poly_id = id & ID_MASK;
                let Some(poly) = self.admin_polygon(poly_id) else { return; };
                let level = poly.admin_level as usize;
                if level >= 12 { return; }
                // Level-2 candidates without an ISO code can never name a
                // country. Mirrors Nominatim, where the response's country
                // comes from the country_code -> country-names table, so a
                // stray admin_level=2 polygon (e.g. Taiwan's 12-nautical-
                // mile "maritime boundary" closed way, boundary=
                // administrative + maritime=yes but no ISO tag) is invisible
                // to addressing. Ours tied with the real country polygon on
                // area and could win by entry order.
                if level == 2 && poly.country_code == 0 { return; }
                if poly.area <= 0.0 { return; }

                if !self.admin_geometry.has_rings(&poly) { return; }
                let pip = self.admin_geometry.contains(&poly, point, &mut memo);

                seen_in_level[level].push(serde_json::json!({
                    "name": self.get_string(poly.name_id),
                    "area": poly.area,
                    "pip": pip,
                    "is_interior": is_interior,
                    "override": poly.place_type_override,
                    "poly_id": poly_id,
                }));

                if pip {
                    if let Some((best_area, _, _)) = best_by_level[level] {
                        if poly.area < best_area {
                            best_by_level[level] = Some((poly.area, poly, true));
                        }
                    } else {
                        best_by_level[level] = Some((poly.area, poly, true));
                    }
                }
            });
        }

        let mut picked: Vec<serde_json::Value> = Vec::new();
        for level in 0..12 {
            if let Some((_, poly, _)) = best_by_level[level] {
                picked.push(serde_json::json!({
                    "admin_level": level,
                    "name": self.get_string(poly.name_id),
                    "area": poly.area,
                    "override": poly.place_type_override,
                }));
            }
        }

        serde_json::json!({
            "picked_per_level": picked,
            "seen_per_level": seen_in_level,
        })
    }

    /// The polygons holding (lat, lng) as `/polygons` lists them, GeoJSON features: each polygon
    /// the admin cells round the point list, once, a postcode area as kind "postal" at
    /// POLYGONS_POSTAL_LEVEL and any other as kind "admin" at its own level.
    pub fn polygon_features(&self, lat: f64, lng: f64) -> Vec<serde_json::Value> {
        let point = AdminPoint::new(lat, lng);
        let (lat, lng) = point.degrees();
        let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
        let (mut seen, mut hits) = (std::collections::HashSet::new(), Vec::new());
        for c in std::iter::once(cell).chain(cell_neighbors_at_level(cell, self.admin_cell_level)) {
            self.admin_index.for_each(c, |id| {
                if seen.insert(id & ID_MASK) { hits.push(id & ID_MASK); }
            });
        }
        hits.into_iter()
            .filter_map(|id| self.admin_polygon(id))
            .filter(|poly| self.admin_geometry.contains_once(poly, point))
            .map(|poly| {
                let (kind, admin_level) = if poly.admin_level == POSTCODE_AREA_LEVEL {
                    ("postal", POLYGONS_POSTAL_LEVEL)
                } else {
                    ("admin", poly.admin_level)
                };
                serde_json::json!({
                    "type": "Feature",
                    "geometry": { "type": "Polygon", "coordinates": self.admin_geometry.rings(&poly) },
                    "properties": {
                        "name": self.get_string(poly.name_id),
                        "admin_level": admin_level,
                        "area": poly.area,
                        "kind": kind,
                    }
                })
            })
            .collect()
    }

    pub fn find_admin(&self, lat: f64, lng: f64) -> AdminResult<'_> {
        self.find_admin_in(&mut AdminScan::new(self, lat, lng))
    }

    /// `find_admin` over a scan the caller may share with the other admin lookups of the point.
    pub fn find_admin_in(&self, scan: &mut AdminScan) -> AdminResult<'_> {
        // For each admin level, the smallest-area polygon holding the point, the first in
        // visiting order among equals (`ScanPip::holds`: an entry flagged interior holds it
        // without a test). Owned AdminPolygon values so the backing admin_polygons file can be a
        // chunked source. Nominatim ranks every boundary of a rank above 0 covering the point
        // (getNearFeatures, placex_triggers.sql:1273): of levels 3 to 10 every larger one holding
        // the point that the smallest lies within ranks too (`round_smallest`), Bruxelles -
        // Brussel round Quays Quarter. One beside the smallest is left out, as the simplified
        // presets' neighbours overlap along their shared borders, and of level 2 the smallest
        // alone names the country.
        let mut best_by_level: [Option<AdminPolygon>; 12] = [None; 12];
        let mut round_smallest: Vec<AdminPolygon> = Vec::new();
        // Class='place' polygons (admin_level=15 marker) that cover the query point. These
        // drive the second-nested-check rule (placex_triggers.sql lines 944-963) — Nominatim
        // uses only these, not admin boundaries with linked_place tags. Kept apart from
        // best_by_level[]. Stored as (poly_id, AdminPolygon) so we can dedup by id.
        let mut place_area_polygons: Vec<(u32, AdminPolygon)> = Vec::new();
        // The boundaries to test, as (level, area, place in the list) in one integer: an area
        // here is positive, so its bits order as it does, and the place orders equals.
        let mut boundaries: Vec<u64> = Vec::with_capacity(scan.rings.len());
        let mut place = 0u64;

        scan.for_each_entry(|pip, id, poly| {
            let poly_id = id & ID_MASK;
            place += 1;
            if poly.admin_level == 15 {
                if poly.area <= 0.0 { return; }
                if !pip.holds(self, id, &poly) { return; }
                if !place_area_polygons.iter().any(|(pid, _)| *pid == poly_id) {
                    place_area_polygons.push((poly_id, poly));
                }
                return;
            }
            let level = poly.admin_level as usize;
            if level >= 12 { return; }
            // See debug_admin: ISO-less level-2 polygons (maritime
            // boundary closed ways) can never name a country.
            if level == 2 && poly.country_code == 0 { return; }
            if poly.area <= 0.0 { return; }
            // One that repeats its parent ranks 0 and is no address area (placex_triggers.sql
            // :915-932, :1273), so the next larger boundary of its level is tried in its place.
            if poly.place_type_override == BOUNDARY_REPEATS_PARENT && level != POSTCODE_AREA_LEVEL as usize { return; }
            boundaries.push((level as u64) << 56 | (poly.area.to_bits() as u64) << 24 | (place - 1));
        });
        // Each level's polygons from the smallest up: the first to hold the point is the level's
        // best. The larger ones of levels 3 to 10 are tested too, as find_current_boundaries
        // tests them on the same scan; those of level 2 are never read. Every postal area
        // holding the point is a candidate for its postcode.
        boundaries.sort_unstable();
        let within = |poly: &AdminPolygon, other: &AdminPolygon| self.admin_geometry.polygon_within(poly, other);
        let mut postal_areas: Vec<AdminPolygon> = Vec::new();
        for key in boundaries {
            let level = (key >> 56) as usize;
            let postal = level == POSTCODE_AREA_LEVEL as usize;
            let round = (3..=10).contains(&level);
            if best_by_level[level].is_some() && !postal && !round { continue; }
            let (id, poly) = scan.rings[(key & 0xff_ffff) as usize];
            if !scan.pip.holds(self, id, &poly) { continue; }
            match best_by_level[level] {
                Some(smallest) if round => {
                    if within(&smallest, &poly) { round_smallest.push(poly); }
                }
                _ => {
                    best_by_level[level].get_or_insert(poly);
                    if postal { postal_areas.push(poly); }
                }
            }
        }

        let mut result = AdminResult::default();

        // Get country code from level 2 polygon
        let country_code_str: String = best_by_level[2]
            .and_then(|poly| {
                if poly.country_code != 0 {
                    Some(format!("{}{}",
                        (poly.country_code >> 8) as u8 as char,
                        (poly.country_code & 0xFF) as u8 as char
                    ).to_uppercase())
                } else {
                    None
                }
            })
            .unwrap_or_default();
        result.postcode_area = self.postal_area_postcode(&postal_areas, &country_code_str, scan.pip.point.degrees());

        // Map admin levels to address fields using country-specific config
        // Track best (smallest area) polygon per output field
        struct FieldCandidate<'b> {
            area: f32,
            name: Cow<'b, str>,
            country_code: u16,
            rank: u8,
            /// The polygon's place in `ranked`.
            index: usize,
        }
        // A slot per field of ADMIN_FIELDS.
        let mut best_by_field: [Option<FieldCandidate<'_>>; ADMIN_FIELDS.len()] = Default::default();

        // Collect polygons at this point with their initial rank_address;
        // promote_nested_ranks then gives them their effective one.
        let mut ranked: Vec<RankedPoly> = Vec::new();
        for &poly in best_by_level.iter().flatten().chain(&round_smallest) {
            if poly.admin_level == POSTCODE_AREA_LEVEL { continue; } // handled separately

            // A boundary its level ranks 0 is in no address, whatever its place type: it takes
            // neither the admin-nested step nor a place rank (placex_triggers.sql:877, :1158).
            let initial_rank = self.admin_config.to_rank(&country_code_str, poly.admin_level);
            if initial_rank == 0 { continue; }

            ranked.push(RankedPoly { poly, rank_address: initial_rank });
        }

        // Add class='place' polygons (our admin_level=15 marker) to
        // the ranked list. These come from closed ways / relations
        // tagged `place=neighbourhood / quarter / suburb / borough`
        // (the closed-way extraction path in the builder) — Nominatim
        // indexes them as rank_address 20-22 and they fill the
        // neighbourhood / quarter / suburb fields. Fixes cairo
        // qasr al doubara (place=neighbourhood closed way).
        //
        // Pick the smallest polygon per name and rank to avoid duplicates
        // when overlapping place areas share a name.
        for (_pid, poly) in &place_area_polygons {
            if poly.place_type_override == 0 { continue; }
            let (_, r) = place_area_ranks(poly, &country_code_str);
            if r == 0 { continue; }
            let name = self.get_string(poly.name_id);
            if place_area_repeats(ranked.iter().map(|p| (self.get_string(p.poly.name_id), p.rank_address, p.poly.area)), &name, r, poly.area) {
                continue;
            }
            ranked.push(RankedPoly { poly: *poly, rank_address: r });
        }

        promote_nested_ranks(&mut ranked, place_area_polygons.iter().map(|(_, pa)| pa), &country_code_str, within);
        ranked.retain(|r| ADDRESS_AREA_RANKS.contains(&r.rank_address));
        one_boundary_a_rank(&mut ranked);
        result.boundary_ranks = rank_bits(&ranked);

        // Now assign each polygon to its field slot. Each rank yields one
        // field label (via place_type_override → fixed field, or
        // rank_to_field). Smaller polygon (later in list after sort) wins
        // Two-pass field assignment: admin boundaries (al < 15) have
        // priority over place-area polygons (al=15). This prevents a
        // small place=suburb closed way (e.g. Le Marais, Paris) from
        // overriding the admin boundary (4th Arrondissement) that
        // Nominatim uses for the same field. Nominatim's addressline
        // walk prioritizes admin boundaries via `fromarea DESC` in the
        // ORDER BY clause.
        // Field assignment helper: determines which address field (if any)
        // a polygon should claim. For sub-city ranks (>= 20), only polygons
        // with an explicit place_type_override (i.e. tagged place=suburb /
        // neighbourhood / quarter in OSM) get a named field. Pure admin
        // boundaries at admin_level 9-11 (e.g. NYC community boards,
        // Paris arrondissements-level sub-divisions) are administrative
        // containers — Nominatim puts them in display_name as anonymous
        // address rows but doesn't let them claim "neighbourhood" or
        // "suburb" in the address dict, so they don't suppress the actual
        // place=neighbourhood nodes found by find_places.
        let field_for = polygon_field;
        let candidate = |r: &RankedPoly, index: usize| FieldCandidate {
            area: r.poly.area, name: self.get_string(r.poly.name_id), country_code: r.poly.country_code, rank: r.rank_address,
            index,
        };
        let slots: Vec<Option<usize>> = ranked.iter().map(|r| field_for(r).and_then(admin_field_index)).collect();

        for (k, r) in ranked.iter().enumerate() {
            if r.poly.admin_level == 15 { continue; } // skip place-area in first pass
            if let Some(idx) = slots[k] {
                if let Some(ref existing) = best_by_field[idx] {
                    if r.poly.area >= existing.area { continue; }
                }
                best_by_field[idx] = Some(candidate(r, k));
            }
        }
        // Second pass: place-area polygons fill only empty fields.
        for (k, r) in ranked.iter().enumerate() {
            if r.poly.admin_level != 15 { continue; }
            if let Some(idx) = slots[k] {
                if best_by_field[idx].is_none() {
                    best_by_field[idx] = Some(candidate(r, k));
                }
            }
        }

        // Fill result
        for (field, c) in ADMIN_FIELDS.iter().zip(&best_by_field) {
            if let Some(c) = c { result.ranks.set(field, c.rank); }
        }
        let winners = best_by_field.each_ref().map(|c| c.as_ref().map(|c| c.index));
        let same_outline = |a: &AdminPolygon, b: &AdminPolygon| self.admin_geometry.same_outline(a, b);
        result.unlabelled = unlabelled_rows(&ranked, &slots, &winners, same_outline).into_iter()
            .map(|k| (ranked[k].rank_address, self.get_string(ranked[k].poly.name_id)))
            .collect();
        if let Some(c) = best_by_field[0].take() {
            result.country = Some(c.name);
            if c.country_code != 0 {
                result.country_code = Some([
                    (c.country_code >> 8) as u8,
                    (c.country_code & 0xFF) as u8,
                ]);
            }
        }
        result.state          = best_by_field[1].take().map(|c| c.name);
        result.province       = best_by_field[2].take().map(|c| c.name);
        result.region         = best_by_field[3].take().map(|c| c.name);
        result.state_district = best_by_field[4].take().map(|c| c.name);
        result.county         = best_by_field[5].take().map(|c| c.name);
        result.district       = best_by_field[6].take().map(|c| c.name);
        result.municipality   = best_by_field[7].take().map(|c| c.name);
        result.city           = best_by_field[8].take().map(|c| c.name);
        result.town           = best_by_field[9].take().map(|c| c.name);
        result.village        = best_by_field[10].take().map(|c| c.name);
        result.hamlet         = best_by_field[11].take().map(|c| c.name);
        result.city_district  = best_by_field[12].take().map(|c| c.name);
        result.borough        = best_by_field[13].take().map(|c| c.name);
        result.suburb         = best_by_field[14].take().map(|c| c.name);
        result.quarter        = best_by_field[15].take().map(|c| c.name);
        result.neighbourhood  = best_by_field[16].take().map(|c| c.name);
        result.city_block     = best_by_field[17].take().map(|c| c.name);
        result.rows = ranked;

        result
    }

    // --- Place node lookup (nearest city/town/village/suburb) ---

    // Nominatim's `current_boundary` (insert_addresslines in
    // placex_triggers.sql): the address walk takes candidates by
    // rank_address, boundaries before place nodes within a rank, and each
    // boundary it accepts (rank_address other than 5 and 11) becomes the
    // geometry later place nodes must sit in. A place node of rank R is so
    // gated by the last boundary accepted before it, whatever its
    // admin_level: on a Vatican point that is the Vatican itself, which keeps
    // Rome's neighbourhood nodes out. The walk accepts one boundary per rank,
    // and only one whose centroid lies in the boundary accepted before it
    // (CurrentBoundaries::walk); the smallest containing polygon of a rank is
    // tried first, and a polygon's point on surface stands in for its
    // centroid. The rank is placex's rank_address, after the nested-boundary
    // promotions (CurrentBoundaries::from_ranked). They are found at the
    // point of `scan`, which keeps the polygons find_admin tested there from
    // being tested again.
    pub fn find_current_boundaries(&self, scan: &mut AdminScan, country_code: Option<[u8; 2]>) -> CurrentBoundaries {
        let cc: String = country_code
            .map(|c| c.iter().map(|b| b.to_ascii_uppercase() as char).collect())
            .unwrap_or_default();

        // (area, entry, polygon) of every boundary and place area near the point.
        let mut boundaries: Vec<(f32, u32, AdminPolygon)> = Vec::new();
        let mut place_areas: Vec<(f32, u32, AdminPolygon)> = Vec::new();
        scan.for_each_entry(|_, id, poly| {
            if poly.area <= 0.0 { return; }
            match poly.admin_level {
                // ISO-less level-2 polygons are maritime lines, not countries.
                2 if poly.country_code == 0 => {}
                // One that repeats its parent ranks 0: it promotes no place node, gates none and
                // is no boundary's parent (placex_triggers.sql:924, :994, :1273).
                2..=10 if poly.place_type_override == BOUNDARY_REPEATS_PARENT => {}
                2..=10 => boundaries.push((poly.area, id, poly)),
                15 if poly.place_type_override != 0 => place_areas.push((poly.area, id, poly)),
                _ => {}
            }
        });
        let mut holding = |mut polys: Vec<(f32, u32, AdminPolygon)>| -> Vec<AdminPolygon> {
            polys.sort_by(|a, b| a.0.total_cmp(&b.0).then((a.1 & ID_MASK).cmp(&(b.1 & ID_MASK))));
            polys.dedup_by_key(|p| p.1 & ID_MASK);
            polys.into_iter().filter(|(_, id, poly)| scan.pip.holds(self, *id, poly)).map(|(_, _, poly)| poly).collect()
        };
        let ranked: Vec<RankedPoly> = holding(boundaries).into_iter()
            .map(|poly| RankedPoly { poly, rank_address: self.admin_config.to_rank(&cc, poly.admin_level) })
            .filter(|r| r.rank_address != 0)
            .collect();
        let inside = |poly: &AdminPolygon, current: &AdminPolygon| self.admin_geometry.point_on_surface_in(poly, current);
        let within = |poly: &AdminPolygon, other: &AdminPolygon| self.admin_geometry.polygon_within(poly, other);
        CurrentBoundaries::from_ranked(ranked, holding(place_areas).iter(), &cc, inside, within)
    }

    /// The place nodes of an address along `feature` (Nominatim's address walk, place_node_walk),
    /// and the address rows of those named `named` (a string id).
    pub fn find_places(&self, feature: PlaceFeature<'_>, current_boundaries: &CurrentBoundaries, held_ranks: u32,
                       named: Option<u32>) -> PlaceResult<'_> {
        if self.places.is_none() { return PlaceResult::default(); }

        // Every node whose fuzzy area covers the feature, with its squared
        // distance, in id order.
        let mut candidates: Vec<(PlaceNode, f64)> = Vec::new();
        // A node is listed in the admin cell (L10, ~10km) of its centroid
        // only: look in every cell a node whose area covers the feature can
        // sit in. Around a point that is the point's box grown by the largest
        // fuzzy half side, a city's; along a street, a strip along it, so the
        // street's labels don't depend on where along it the query falls.
        let cells = match feature {
            PlaceFeature::Point(lat, lng) => cells_near_point(lat, lng, MAX_PLACE_FUZZY_HALF_SIDE_M, self.admin_cell_level),
            PlaceFeature::Line(line) => cells_under_line(line, MAX_PLACE_FUZZY_HALF_SIDE_M, self.admin_cell_level),
        };
        self.for_each_place_in(cells, |pn| {
            if let Some(dist_sq) = place_node_covering(&pn, feature) {
                candidates.push((pn, dist_sq));
            }
        });

        let mut result = PlaceResult::default();

        // Nominatim's cascading containment gate (placex_triggers.sql
        // lines 597-600): place nodes (isguess=true) are rejected if
        // their centroid is NOT inside `current_boundary` — the
        // geometry of the last accepted non-guess admin boundary.
        //
        // We implement this by PIP-testing the PLACE NODE's lat/lng
        // against the current boundary for its rank_address
        // (find_current_boundaries). This is the correct Nominatim
        // check: the NODE must be inside the boundary, not the query.
        let geometry = &self.admin_geometry;
        let holds_node = |poly: &AdminPolygon, pn: &PlaceNode| {
            geometry.contains_once(poly, AdminPoint::new(pn.lat as f64, pn.lng as f64))
        };
        let node_inside_boundary = |pn: &PlaceNode, rank: u8| -> bool {
            match current_boundaries.for_rank(rank) {
                None => true, // no admin boundary found — accept all
                // A boundary without rings accepts all too.
                Some(poly) => !geometry.has_rings(poly) || holds_node(poly, pn),
            }
        };

        let nodes: Vec<(usize, u8, f64, f32, f32)> = candidates.iter()
            .map(|(pn, dist_sq)| {
                let rank = place_node_rank(pn, &current_boundaries.promoting, |poly| holds_node(poly, pn));
                (pn.place_type as usize, rank, *dist_sq, pn.lat, pn.lng)
            })
            .collect();
        // The walk tests every node's boundary for its rows, so it lists them only where a node
        // has the name asked for.
        let rows = named.is_some_and(|name| candidates.iter().any(|(pn, _)| pn.name_id == name));
        let (accepted, written) = place_node_walk(&nodes, held_ranks, |k| node_inside_boundary(&candidates[k].0, nodes[k].1), rows);
        result.named_rows = written.into_iter()
            .filter(|&k| Some(candidates[k].0.name_id) == named)
            .map(|k| NamedRow { rank: nodes[k].1, field: PLACE_NODE_FIELDS[nodes[k].0], distance: nodes[k].2.sqrt(), node: candidates[k].0 })
            .collect();
        for (pt, slot) in [&mut result.city, &mut result.town, &mut result.village, &mut result.suburb,
                           &mut result.hamlet, &mut result.neighbourhood, &mut result.quarter].into_iter().enumerate() {
            if let Some(k) = accepted[pt] {
                *slot = Some(self.get_string(candidates[k].0.name_id));
                result.ranks.set(PLACE_NODE_FIELDS[pt], nodes[k].1);
            }
        }

        result
    }

    // Categories that are km-scale area labels rather than
    // addressable building-level features. Matches Nominatim's
    // exclusion of class_ IN ('place', 'building') from the
    // rank-30 POI branch in _find_closest_street_or_pois.
    //
    // See builder/src/types.h for the numeric category enum.
    pub fn is_area_label_category(cat: u8) -> bool {
        matches!(cat,
            // natural: large polygons
            80 | 81 | 82 | 92 | 93 | // peak, volcano, beach, cape, island
            // boundary/leisure: parks, reserves
            60 | 61 | 62 | 63 | 64 | 65 | // park, nature_reserve, stadium, garden, water_park, golf_course
            130 | 131 | // national_park, protected_area
            // amenity: campus-scale
            41 | 42 | 43 | 47 | 51 | 54 | // university, college, hospital, marketplace, cemetery, prison
            // tourism: resorts / theme parks
            3 | 4 | 9 | 11 | // theme_park, zoo, camp_site, resort
            // historic: battlefields
            25 | // battlefield
            // aeroway / transport / industrial
            100 | // aerodrome
            150   // power_plant
        )
    }

    // Water kinds, which never take the primary slot, so never set `road`
    // or `postcode`. Nominatim's POI lane takes rank_search 30 alone
    // (reverse.py _find_closest_street_or_pois), and it ranks
    // natural=water, wetland, bay and glacier 22 ("natural": {"": [22, 0]})
    // and landuse=reservoir|basin on an area 24 ("landuse": {"": [24, 0]},
    // settings/address-levels.json), while a landuse node ranks 30
    // (lib-sql/functions/ranking.sql compute_place_rank): a point RESERVOIR
    // stays in the lane. A water object that also carries a POI main tag is
    // a rank-30 POI there, and the builder classifies it by that tag
    // (poi_classify.h), so it stays too.
    pub fn is_below_poi_rank(cat: u8, polygon: bool) -> bool {
        match cat {
            86 | 91 | 97 | 99 => true, // glacier, bay, wetland, natural_water
            148 => polygon,            // reservoir
            _ => false,
        }
    }

    // Whether `poi` is a water street: a water area of the highway flag, which
    // a highway kind of rank 26 or 27 ranks (the builder flags it only there,
    // poi_classify.h poi_water_street). Nominatim imports it as one placex row
    // of class highway, its polygon the geometry and rank_address 26 or 27
    // (init.lua process_tags, ranking.sql compute_place_rank), which its
    // street lane, of address ranks 26 to 29, finds by the distance to that
    // polygon, 0 inside (reverse.py _find_closest_street_or_pois), and which
    // answers by its own name as a street does, of no parent street
    // (placex_update parents rank_search > 27 alone): it competes as one,
    // wherever it wins, linked to a parent street or not.
    pub fn is_water_street(poi: &PoiRecord) -> bool {
        poi.is_polygon() && poi.flags & POI_FLAG_HIGHWAY != 0 && Self::is_below_poi_rank(poi.category, true)
    }

    // Whether `poi` stays out of the primary slot: a water kind below the
    // POI rank, but a water street (is_water_street).
    pub fn is_out_of_the_primary_lanes(poi: &PoiRecord) -> bool {
        Self::is_below_poi_rank(poi.category, poi.is_polygon()) && !Self::is_water_street(poi)
    }

    // Chain-containment check for a POI primary candidate.
    // The POI's parent_poly_id is the smallest admin polygon (level
    // 2..10) that contained the POI at build time. At query time we
    // verify that polygon actually contains the query point — if it
    // does, the POI is in the same admin jurisdiction as the query
    // and is a valid primary candidate. If not, the POI centroid
    // sits in a different admin (common for river-boundary landmarks
    // and cross-border features) and we reject it.
    //
    // POIs built before parent_poly_id was added have NO_DATA and
    // always pass. POIs whose parent was dropped during continent-
    // filter remap also have NO_DATA and pass.
    pub fn poi_chain_contains_query(&self, poi: &PoiRecord, lat: f64, lng: f64) -> bool {
        if poi.parent_poly_id == NO_DATA { return true; }
        let Some(poly) = self.admin_polygon(poi.parent_poly_id) else { return true; };
        if !self.admin_geometry.has_rings(&poly) { return true; }
        self.admin_geometry.contains_once(&poly, AdminPoint::new(lat, lng))
    }

    // --- POI primary-feature lookup ---
    //
    // Returns the nearest POI with a valid parent_street_id, or water
    // street (is_water_street), to the query point. Used by query() as an
    // additional primary-feature
    // candidate alongside the raw address / interpolation / street
    // lookups — mirrors Nominatim's _find_closest_street_or_pois
    // which returns the closest rank-26+ feature regardless of
    // whether it's a road or an addressable POI.
    //
    // For polygon POIs, returns distance 0 if the query is inside
    // the polygon, otherwise the distance to the nearest edge. None for
    // a point off the globe (see find_pois).
    pub fn find_nearest_poi_with_parent(&self, lat: f64, lng: f64) -> Option<(f64, PoiRecord)> {
        self.nearest_linked_poi(lat, lng, false)
    }

    /// The POI find_nearest_poi_with_parent would find if the primary lanes took the water they
    /// leave out (is_out_of_the_primary_lanes) too. A glacier or bay labels an area there, as an
    /// island does: it wins only where no specific POI is found, however far.
    pub fn find_nearest_poi_or_water_with_parent(&self, lat: f64, lng: f64) -> Option<(f64, PoiRecord)> {
        self.nearest_linked_poi(lat, lng, true)
    }

    fn nearest_linked_poi(&self, lat: f64, lng: f64, water: bool) -> Option<(f64, PoiRecord)> {
        let poi_index = self.poi_index.as_ref()?;
        if self.poi_records.is_none() || !coords_in_range(lat, lng) { return None; }

        let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.admin_cell_level);

        let cos_lat = lat.to_radians().cos();
        let mut best_dist_sq = f64::MAX;
        let mut best_idx: Option<u32> = None;
        // Fallback tier: the closest area-label POI (national park,
        // island, cape, campus). Only wins primary if no specific
        // POI qualifies — so "middle of Yosemite with no roads" can
        // surface the park, while urban queries never get "Manhattan
        // Island" as the primary feature.
        let mut fallback_dist_sq = f64::MAX;
        let mut fallback_idx: Option<u32> = None;

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            poi_index.for_each(c, |id| {
                // The interior flag on a (cell, polygon) entry proves
                // containment only for queries INSIDE that cell. Neighbor
                // cells are iterated for near-miss distance work; honoring
                // their interior flags claimed containment for points up to
                // a full cell away (a Dutch query 'inside' a German
                // naturpark). A polygon truly containing the query always
                // has the query's own cell in its covering, so this loses
                // no true positives.
                let is_interior = c == cell && (id & INTERIOR_FLAG) != 0;
                let poi_id = (id & ID_MASK) as u32;
                let Some(poi) = self.poi_record(poi_id) else { return; };
                // Need a linked parent street (for the `road` field)
                // when this POI wins — without it we have nothing to
                // surface as the road name; a water street's own name is
                // its road.
                if (poi.parent_street_id == NO_DATA && !Self::is_water_street(&poi))
                    || (!water && Self::is_out_of_the_primary_lanes(&poi)) {
                    return;
                }

                // Compute distance²: inside the polygon → 0;
                // otherwise min distance to any edge; for point POIs,
                // direct point-to-point distance.
                let dist_sq_val;
                if poi.is_polygon() {
                    let Some(verts) = self.poi_verts(&poi) else { return; };
                    let count = verts.len();
                    if is_interior || point_in_polygon(lat as f32, lng as f32, &verts) {
                        dist_sq_val = 0.0;
                    } else {
                        let mut mind = f64::MAX;
                        for i in 0..count {
                            let j = if i + 1 < count { i + 1 } else { 0 };
                            let d = point_to_segment_distance(
                                lat, lng,
                                verts[i].lat as f64, verts[i].lng as f64,
                                verts[j].lat as f64, verts[j].lng as f64,
                                cos_lat,
                            );
                            if d < mind { mind = d; }
                        }
                        dist_sq_val = mind;
                    }
                } else {
                    let dlat = (lat - poi.lat as f64).to_radians();
                    let dlng = (lng - poi.lng as f64).to_radians();
                    dist_sq_val = dlat * dlat + dlng * dlng * cos_lat * cos_lat;
                }

                // Glaciers and bays (86, 91) label an area too where
                // the lanes take water.
                if Self::is_area_label_category(poi.category) || (water && matches!(poi.category, 86 | 91)) {
                    // Split lane: area labels (islands, parks, capes,
                    // campuses) only win primary when nothing more
                    // specific competes. Track them separately so a
                    // "middle-of-nowhere in Yosemite" query still has
                    // a useful primary, without letting Manhattan
                    // Island claim every NYC query.
                    if dist_sq_val < fallback_dist_sq {
                        fallback_dist_sq = dist_sq_val;
                        fallback_idx = Some(poi_id);
                    }
                    return;
                }

                if dist_sq_val < best_dist_sq {
                    best_dist_sq = dist_sq_val;
                    best_idx = Some(poi_id);
                }
            });
        }

        // Only fall back to the area-label candidate when no specific
        // POI was found at all within the search radius.
        if let Some(i) = best_idx {
            self.poi_record(i).map(|p| (best_dist_sq, p))
        } else {
            fallback_idx.and_then(|i| self.poi_record(i).map(|p| (fallback_dist_sq, p)))
        }
    }

    // --- POI lookup ---

    /// The POIs near (lat, lng); none for a point off the globe, or NaN: S2 would wrap it onto a
    /// cell of the globe, whose interior entries hold that cell's points, not this one.
    pub fn find_pois(&self, lat: f64, lng: f64) -> Vec<PoiMatch<'_>> {
        let Some(poi_index) = &self.poi_index else { return vec![] };
        if self.poi_records.is_none() || !coords_in_range(lat, lng) { return vec![]; }

        let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.admin_cell_level);

        let mut results: Vec<PoiMatch<'_>> = Vec::new();

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            poi_index.for_each(c, |id| {
                // The interior flag on a (cell, polygon) entry proves
                // containment only for queries INSIDE that cell. Neighbor
                // cells are iterated for near-miss distance work; honoring
                // their interior flags claimed containment for points up to
                // a full cell away (a Dutch query 'inside' a German
                // naturpark). A polygon truly containing the query always
                // has the query's own cell in its covering, so this loses
                // no true positives.
                let is_interior = c == cell && (id & INTERIOR_FLAG) != 0;
                let poi_id = (id & ID_MASK) as u32;
                let Some(poi) = self.poi_record(poi_id) else { return; };

                let ref_dist = self.poi_meta.reference_distance(poi.category);
                let importance = if poi.importance > 0 {
                    poi.importance as f64
                } else {
                    self.poi_meta.default_importance(poi.category)
                };
                let base_max = self.poi_meta.max_distance(poi.category);
                let cos_lat = lat.to_radians().cos();

                let dist_m;
                let contained;
                // Polygon area via the shoelace formula, in squared-
                // degree units. Only computed for contained polygons
                // and used solely as a tie-break to prefer the more
                // specific polygon when multiple contain the query
                // (Eiffel Tower's ~0.0000002 over Field of Mars's
                // ~0.00001 over Banks of the Seine's much larger
                // area).
                let mut area_sq_deg = 0.0_f64;

                if poi.is_polygon() {
                    let Some(verts) = self.poi_verts(&poi) else { return; };
                    let count = verts.len();
                    contained = is_interior || point_in_polygon(lat as f32, lng as f32, &verts);

                    if contained {
                        dist_m = 0.0;
                        let mut a = 0.0_f64;
                        for i in 0..count {
                            let j = if i + 1 < count { i + 1 } else { 0 };
                            a += verts[i].lng as f64 * verts[j].lat as f64
                               - verts[j].lng as f64 * verts[i].lat as f64;
                        }
                        area_sq_deg = (a / 2.0).abs();
                    } else {
                        let mut min_dist_sq = f64::MAX;
                        for i in 0..count {
                            let j = if i + 1 < count { i + 1 } else { 0 };
                            let d = point_to_segment_distance(
                                lat, lng,
                                verts[i].lat as f64, verts[i].lng as f64,
                                verts[j].lat as f64, verts[j].lng as f64,
                                cos_lat,
                            );
                            if d < min_dist_sq { min_dist_sq = d; }
                        }
                        dist_m = min_dist_sq.sqrt() * 6_371_000.0;
                    }
                } else {
                    contained = false;
                    let dlat = (lat - poi.lat as f64).to_radians();
                    let dlng = (lng - poi.lng as f64).to_radians();
                    dist_m = (dlat * dlat + dlng * dlng * cos_lat * cos_lat).sqrt() * 6_371_000.0;
                }

                // Hard max distance cutoff (importance-scaled)
                if !contained {
                    if base_max == 0.0 { return; }
                    let effective_max = base_max * (1.0 + importance / 50.0);
                    if dist_m > effective_max { return; }
                }

                // Islands (PoiCategory::ISLAND=93) are admin-level
                // area labels (Manhattan Island, Staten Island,
                // Manhattan-level stuff) — they duplicate what
                // address.borough / address.suburb already convey and
                // crowd out true landmarks. Drop from places[] so a
                // query at Strawberry Fields doesn't lead with
                // "Manhattan Island, Central Park, 72nd Street".
                if poi.category == 93 { return; }
                // Score with squared distance decay. Contained-polygon
                // POIs (Disneyland / London Zoo / Yellowstone / a
                // university campus around the query) get an explicit
                // 3× boost so they surface ahead of the cluster of
                // point-attractions inside them. Without the boost,
                // clicking Walt's Plaza returns "Astro Orbitor /
                // Buzz Lightyear / ..." with Disneyland buried at rank
                // 10+ — the user almost certainly wants "Disneyland"
                // as the primary landmark label and the rides as
                // supporting context.
                let proximity_weight = if contained {
                    3.0
                } else {
                    let r = dist_m / ref_dist;
                    1.0 / (1.0 + r * r)
                };
                let score = importance * proximity_weight;
                if score < 0.5 { return; }
                // Read the name only now: none of the filters above needs it, and most
                // candidates fail one of them.
                let name = self.get_string(poi.name_id);
                if name.is_empty() { return; }

                results.push(PoiMatch {
                    name,
                    category: self.poi_meta.category_name(poi.category),
                    category_id: poi.category,
                    distance_m: if contained { 0.0 } else { dist_m },
                    contained,
                    is_point: !poi.is_polygon(),
                    score,
                    area: area_sq_deg,
                });
            });
        }

        results.sort_by(poi_order);
        // Dedup rules:
        //   - always drop duplicate NAMES regardless of containment
        //     (a user clicking once shouldn't see two "Starbucks" even
        //     if they're in different polygons)
        //   - for non-contained CENTROID-ONLY point POIs, also drop
        //     duplicate categories so the nearest cafe / restaurant
        //     / bar wins and others of the same kind don't flood the
        //     list. Polygon POIs are exempt — two nearby polygons of
        //     the same category are distinct landmarks (Big Ben +
        //     Palace of Westminster, both attractions, are both
        //     worth keeping).
        let mut seen_names = std::collections::HashSet::new();
        let mut seen_point_cats = std::collections::HashSet::new();
        results.retain(|r| {
            if !seen_names.insert(r.name.clone()) { return false; }
            if !r.contained && r.is_point && !seen_point_cats.insert(r.category) { return false; }
            true
        });
        results.truncate(5);
        results
    }

    // --- Chain-based admin walk ---
    //
    // Given a starting polygon ID (the smallest admin polygon
    // containing the primary street's centroid), walk up the
    // admin_parents chain collecting every polygon into the
    // ranked list. Then apply the same rank adjustment cascade
    // as find_admin(). This mirrors Nominatim's addressline
    // parent_place_id walk — only polygons in the chain appear
    // in the result, avoiding the false-positive EXTRAs from
    // the pure PIP approach.
    pub fn find_admin_from_chain(&self, start_poly_id: u32, lat: f64, lng: f64) -> AdminResult<'_> {
        self.find_admin_from_chain_in(start_poly_id, &mut AdminScan::new(self, lat, lng))
    }

    pub fn find_admin_from_chain_in(&self, start_poly_id: u32, scan: &mut AdminScan) -> AdminResult<'_> {
        let admin_parents = match &self.admin_parents {
            Some(m) => m,
            None => return self.find_admin_in(scan),
        };
        let parents_count = admin_parents.len() / 4;
        let total_polys = u64::from(self.admin_polygon_count());

        // Collect chain: walk start → parent → parent → ...
        let mut chain_ids: Vec<u32> = Vec::with_capacity(12);
        let mut current = start_poly_id;
        for _ in 0..15 { // safety bound
            if current == NO_DATA || (current as u64) >= total_polys { break; }
            chain_ids.push(current);
            if (current as u64) >= parents_count { break; }
            current = admin_parents.read_at::<u32>(current as u64);
        }

        // Also collect class='place' polygons (admin_level=15) via
        // PIP from the query point — these aren't in the parent chain
        // but provide place-area rank promotions.
        let mut place_area_polygons: Vec<AdminPolygon> = Vec::new();
        scan.for_each_entry(|pip, id, poly| {
            if poly.admin_level != 15 { return; }
            if poly.area <= 0.0 { return; }
            if !pip.holds(self, id, &poly) { return; }
            place_area_polygons.push(poly);
        });

        // Now build the same ranked structure as find_admin
        let mut result = AdminResult::default();

        let country_code_str: String = chain_ids.iter()
            .find_map(|&pid| {
                let poly = self.admin_polygon(pid)?;
                if poly.admin_level == 2 && poly.country_code != 0 {
                    Some(format!("{}{}",
                        (poly.country_code >> 8) as u8 as char,
                        (poly.country_code & 0xFF) as u8 as char
                    ).to_uppercase())
                } else { None }
            })
            .unwrap_or_default();

        let mut ranked: Vec<RankedPoly> = Vec::new();
        for &pid in &chain_ids {
            let Some(poly) = self.admin_polygon(pid) else { continue; };
            if poly.admin_level == POSTCODE_AREA_LEVEL { continue; }
            // ISO-less level-2 polygons (maritime boundary closed ways)
            // can never name a country — see find_admin.
            if poly.admin_level == 2 && poly.country_code == 0 { continue; }
            let initial_rank = self.admin_config.to_rank(&country_code_str, poly.admin_level);
            if initial_rank == 0 { continue; }
            if poly.place_type_override == BOUNDARY_REPEATS_PARENT { continue; }
            ranked.push(RankedPoly { poly, rank_address: initial_rank });
        }
        // Add place-area polygons
        for poly in &place_area_polygons {
            if poly.place_type_override == 0 { continue; }
            let (_, r) = place_area_ranks(poly, &country_code_str);
            if r == 0 { continue; }
            ranked.push(RankedPoly { poly: *poly, rank_address: r });
        }

        let within = |poly: &AdminPolygon, other: &AdminPolygon| self.admin_geometry.polygon_within(poly, other);
        promote_nested_ranks(&mut ranked, place_area_polygons.iter(), &country_code_str, within);
        ranked.retain(|r| ADDRESS_AREA_RANKS.contains(&r.rank_address));
        result.boundary_ranks = rank_bits(&ranked);

        // Map to fields, a slot per field of ADMIN_FIELDS.
        struct FC<'b> { area: f32, name: Cow<'b, str>, country_code: u16, rank: u8, index: usize }
        let mut best_by_field: [Option<FC<'_>>; ADMIN_FIELDS.len()] = Default::default();
        let slots: Vec<Option<usize>> = ranked.iter()
            .map(|r| if r.poly.place_type_override > 0 {
                place_type_to_field(r.poly.place_type_override)
            } else {
                rank_to_field(r.rank_address)
            })
            .map(|field| field.and_then(admin_field_index))
            .collect();

        for (k, r) in ranked.iter().enumerate() {
            if let Some(idx) = slots[k] {
                if let Some(ref existing) = best_by_field[idx] {
                    if r.poly.area >= existing.area { continue; }
                }
                best_by_field[idx] = Some(FC { area: r.poly.area, name: self.get_string(r.poly.name_id), country_code: r.poly.country_code, rank: r.rank_address, index: k });
            }
        }

        for (field, c) in ADMIN_FIELDS.iter().zip(&best_by_field) {
            if let Some(c) = c { result.ranks.set(field, c.rank); }
        }
        let winners = best_by_field.each_ref().map(|c| c.as_ref().map(|c| c.index));
        let same_outline = |a: &AdminPolygon, b: &AdminPolygon| self.admin_geometry.same_outline(a, b);
        result.unlabelled = unlabelled_rows(&ranked, &slots, &winners, same_outline).into_iter()
            .map(|k| (ranked[k].rank_address, self.get_string(ranked[k].poly.name_id)))
            .collect();
        if let Some(c) = best_by_field[0].take() {
            result.country = Some(c.name);
            if c.country_code != 0 {
                result.country_code = Some([
                    (c.country_code >> 8) as u8,
                    (c.country_code & 0xFF) as u8,
                ]);
            }
        }
        result.state          = best_by_field[1].take().map(|c| c.name);
        result.province       = best_by_field[2].take().map(|c| c.name);
        result.region         = best_by_field[3].take().map(|c| c.name);
        result.state_district = best_by_field[4].take().map(|c| c.name);
        result.county         = best_by_field[5].take().map(|c| c.name);
        result.district       = best_by_field[6].take().map(|c| c.name);
        result.municipality   = best_by_field[7].take().map(|c| c.name);
        result.city           = best_by_field[8].take().map(|c| c.name);
        result.town           = best_by_field[9].take().map(|c| c.name);
        result.village        = best_by_field[10].take().map(|c| c.name);
        result.hamlet         = best_by_field[11].take().map(|c| c.name);
        result.city_district  = best_by_field[12].take().map(|c| c.name);
        result.borough        = best_by_field[13].take().map(|c| c.name);
        result.suburb         = best_by_field[14].take().map(|c| c.name);
        result.quarter        = best_by_field[15].take().map(|c| c.name);
        result.neighbourhood  = best_by_field[16].take().map(|c| c.name);
        result.city_block     = best_by_field[17].take().map(|c| c.name);

        result
    }

    // --- Postcode resolution ---
    //
    // Nominatim answers with the winning feature's stored `placex.postcode`,
    // which placex_update (placex_triggers.sql) resolves at index time. Tiers
    // 0 and 1 read postcodes the builder stored; tier 2 searches around the
    // feature:
    //   0. A house's (rank 30) addr:postcode (token_get_postcode)
    //   1. The postcode of the street: a house's parent street, else the
    //      street that answers (way_postcodes.bin, the postal area the builder
    //      finds at the street's middle); a POI's parent_postcode_id
    //      (in-PoiRecord field); then, for a US street, the ZIP of a TIGER
    //      line of that street within ~100 m of the query
    //      (interp_postcodes.bin)
    //   2. get_nearest_postcode's rule for a point (utils.sql:168-178,
    //      Nominatim 5.3), which Nominatim runs on the feature at index
    //      time: the postcode area covering the point (a boundary=postal_code
    //      polygon, the level-11 admin polygons), else the postcode centroid
    //      of the country nearest it in plain degrees among those whose box
    //      holds it. A centroid's box reaches its country's postcode extent
    //      north, south, east and west of it (postcode_extent_m,
    //      expand_by_meters). The point is the street's centroid, halfway
    //      along it (get_center_point), for the street and a house of it,
    //      of the country there (get_country_code), which a house takes from
    //      its street: placex_update takes a house's postcode from its own
    //      tag, then its parent street's, then get_nearest_postcode at the
    //      house. A street none reaches has no postcode, nor has its answer.
    //      Without a street the point is the house's, else the query point
    //      (postcode_area, the postal area there).
    //
    // The postal areas feed tier 1 at BUILD time too (way_postcodes.bin,
    // PoiRecord.parent_postcode_id inheritance).
    /// The postcode get_nearest_postcode takes from the postal areas holding the point (lat, lng)
    /// of the country `country` (upper-case ISO, "" where unknown), the smallest first: of those of the
    /// country, any where it is unknown, whose postcode the country has (Nominatim makes no
    /// postcode area of another, nor of one of no country, postcodes.py _update_postcode_areas),
    /// the one whose centroid is nearest the point in plain degrees (utils.sql: ORDER BY
    /// ST_Distance(p.centroid, geom)), equals to the smaller. A postal area's country is the one
    /// its centroid lies in, as get_country_code gives place_postcode's.
    fn postal_area_postcode<'a>(&'a self, areas: &[AdminPolygon], country: &str, (lat, lng): (f64, f64)) -> Option<Cow<'a, str>> {
        let cc: Option<[u8; 2]> = country.as_bytes().try_into().ok().map(|c: [u8; 2]| c.map(|b| b.to_ascii_lowercase()));
        let eligible = |poly: &AdminPolygon| -> Option<Cow<'a, str>> {
            if poly.place_type_override == POSTAL_AREA_INVALID { return None; }
            let of_country = poly.country_code.to_be_bytes().map(|b| b.to_ascii_lowercase());
            if let Some(cc) = cc {
                if of_country != cc || country_has_no_postcode(&cc) { return None; }
            }
            // The builder checked the postcode against its country's pattern, whatever its
            // length; past that, one value and no placeholder.
            let postcode = self.get_string(poly.name_id);
            let valid = !postcode.contains(';') && postcode_looks_valid(&cc.unwrap_or(of_country), &postcode);
            valid.then_some(postcode)
        };
        let mut found = areas.iter().filter_map(|poly| eligible(poly).map(|postcode| (poly, postcode)));
        let first = found.next()?;
        let rest: Vec<_> = found.collect();
        if rest.is_empty() {
            return Some(first.1);
        }
        let distance = |poly: &AdminPolygon| {
            self.admin_geometry.centroid(poly).map_or(f64::INFINITY, |(c_lat, c_lng)| postcode_files::distance(lat, lng, c_lat, c_lng))
        };
        std::iter::once(first).chain(rest)
            .map(|(poly, postcode)| (distance(poly), poly.area, postcode))
            .min_by(|a, b| a.0.total_cmp(&b.0).then(a.1.total_cmp(&b.1)).then_with(|| a.2.cmp(&b.2)))
            .map(|(_, _, postcode)| postcode)
    }

    /// Record `idx`'s value in a side file (way_parents / addr_postcodes /
    /// way_postcodes / interp_postcodes: a u32 per record, NO_DATA = absent).
    fn sidecar_u32(f: &SideBlocks, idx: u64) -> Option<u32> {
        f.get(u32::try_from(idx).ok()?).filter(|&v| v != NO_DATA)
    }

    /// The postcode get_nearest_postcode takes from the postal areas holding the point (lat, lng)
    /// in the country `country_code` (postal_area_postcode), scanned there.
    fn postal_area_at<'a>(&'a self, lat: f64, lng: f64, country_code: Option<[u8; 2]>) -> Option<Cow<'a, str>> {
        let (areas, point, _) = self.postal_areas_at(lat, lng, true);
        self.postal_area_postcode(&areas, &upper_country(country_code), point)
    }

    /// The postal areas holding the point (lat, lng), the smallest first (none unless `postal`),
    /// the point as the scan rounds it, and the country there (get_country_code): that of the
    /// smallest country polygon holding it, the lower code of two as large; none where none
    /// holds it.
    fn postal_areas_at(&self, lat: f64, lng: f64, postal: bool) -> (Vec<AdminPolygon>, (f64, f64), Option<[u8; 2]>) {
        let mut scan = AdminScan::new(self, lat, lng);
        let mut areas: Vec<AdminPolygon> = Vec::new();
        let mut country: Option<(f32, u16)> = None;
        scan.for_each_entry(|pip, id, poly| {
            let is_country = poly.admin_level == 2 && poly.country_code != 0;
            if !(is_country || postal && poly.admin_level == POSTCODE_AREA_LEVEL && poly.area > 0.0) || !pip.holds(self, id, &poly) {
                return;
            }
            if !is_country {
                areas.push(poly);
            } else if country.is_none_or(|(area, code)| poly.area < area || poly.area == area && poly.country_code < code) {
                country = Some((poly.area, poly.country_code));
            }
        });
        areas.sort_by(|a, b| a.area.total_cmp(&b.area));
        (areas, scan.pip.point.degrees(), country.map(|(_, code)| code.to_be_bytes()))
    }

    /// The point halfway along street way `id`, Nominatim's centroid of a street (line_middle);
    /// None for a way whose nodes can't be read.
    fn street_middle(&self, id: u32) -> Option<(f64, f64)> {
        let streets = self.streets.as_ref()?;
        with_way_nodes(|nodes| if streets.way(id, nodes) { line_middle(nodes) } else { None })
    }

    /// The postcode of the answer at (lat, lng), as Nominatim stores it on the feature that
    /// answers (the tiers above): `addr` the house that answers, `street` the street the
    /// postcode comes from (a house's parent street, else the street that answers), `poi` the
    /// POI that answers, `interp` the TIGER line near a street that answers, `postcode_area` the
    /// postal area's postcode at the query point and `country_code` the answer's country.
    pub fn resolve_postcode<'a>(&'a self, lat: f64, lng: f64, street: Option<(usize, &WayRow)>, addr: Option<(usize, &AddrRecord)>, poi: Option<&PoiRecord>, interp: Option<(usize, f64, &str)>, postcode_area: Option<Cow<'a, str>>, country_code: Option<[u8; 2]>) -> Option<Cow<'a, str>> {
        self.resolve_postcode_in(lat, lng, street, addr, poi, interp, postcode_area, country_code).map(|(postcode, _)| postcode)
    }

    /// `resolve_postcode`'s postcode, with the country it is of: the one get_nearest_postcode
    /// takes it in at a street's middle, and a house of the street, else `country_code`.
    fn resolve_postcode_in<'a>(&'a self, lat: f64, lng: f64, street: Option<(usize, &WayRow)>, addr: Option<(usize, &AddrRecord)>, poi: Option<&PoiRecord>, interp: Option<(usize, f64, &str)>, postcode_area: Option<Cow<'a, str>>, country_code: Option<[u8; 2]>) -> Option<(Cow<'a, str>, Option<[u8; 2]>)> {
        // 0. The house's own postcode (from addr_postcodes.bin)
        // Matches Nominatim's token_get_postcode — the feature's own
        // addr:postcode tag is the highest-priority source. The
        // clean-postcodes sanitizer drops one in a country without
        // postcodes or one its country's pattern rejects, and the house
        // then takes its street's: the builder stores none that the
        // pattern of the house's country rejects, and one failing the
        // answer's checks below (country_has_no_postcode,
        // postcode_looks_valid) is passed over.
        if let (Some(apc), Some((addr_id, _))) = (self.addr_postcodes.as_ref(), addr) {
            if let Some(pid) = Self::sidecar_u32(apc, addr_id as u64) {
                let own = self.get_string(pid);
                if country_code.is_none_or(|cc| !country_has_no_postcode(&cc) && postcode_looks_valid(&cc, &own)) {
                    return Some((own, country_code));
                }
            }
        }

        // 1. way_postcodes[street.way_id] — street's computed postcode,
        // which the builder takes at its middle, of the country there.
        if let (Some(wpc), Some((way_id, _))) = (self.way_postcodes.as_ref(), street) {
            if let Some(pid) = Self::sidecar_u32(wpc, way_id as u64) {
                let middle = u32::try_from(way_id).ok().and_then(|id| self.street_middle(id));
                let country = middle.and_then(|(m_lat, m_lng)| self.postal_areas_at(m_lat, m_lng, false).2);
                return Some((self.get_string(pid), country.or(country_code)));
            }
        }

        // 1b. POI's calculated_postcode — build-time PIP into the smallest
        // containing boundary=postal_code polygon. Stored on PoiRecord
        // itself as `parent_postcode_id` so it rides the POI record's
        // memory locality.
        if let Some(p) = poi {
            if p.parent_postcode_id != NO_DATA {
                return Some((self.get_string(p.parent_postcode_id), country_code));
            }
        }

        // 1c. TIGER per-segment ZIP (interp_postcodes.bin): when the primary
        // is a road-ish feature (street passed in) and an interpolation
        // segment lies within ~100m of the query, use that segment's ZIP.
        // Mirrors Nominatim's _find_tiger_number_for_street, which returns
        // the location_property_tiger row (postcode included) for queries
        // within 0.001 deg of a TIGER segment parented to the winning
        // street. Ranked below way_postcodes: where an OSM postal boundary
        // exists it is authoritative (and agrees with TIGER); this tier
        // exists for rural US, where no postal boundary is mapped and the
        // nearest-centroid fallback often misses or crosses ZIP borders.
        if let (Some(ipc), Some((iw_idx, interp_dist, seg_street))) = (self.interp_postcodes.as_ref(), interp) {
            // Nominatim joins location_property_tiger on parent_place_id =
            // the winning street; our segments carry the TIGER street NAME,
            // so require a token overlap between the segment's street and
            // the winning way's name (the same normalised-token test the
            // builder uses to parent addr points, mirroring Nominatim's
            // token_matches_street). An unnamed winner can't be a TIGER
            // parent — skip.
            if let Some((_, way)) = street {
                if way.name != NO_DATA {
                    let refine = 100.0_f64 / 6_371_000.0;
                    if interp_dist <= refine * refine
                        && street_tokens_overlap(seg_street, &self.get_string(way.name))
                    {
                        if let Some(pid) = Self::sidecar_u32(ipc, iw_idx as u64) {
                            return Some((self.get_string(pid), country_code));
                        }
                    }
                }
            }
        }

        // 2. get_nearest_postcode at the street's centroid, of the country
        // there (get_country_code), for the street and a house of it, which
        // takes its street's country; a street none reaches has no postcode.
        // Then at the house; else at the query point, of its postcode area.
        // At each, the postal areas first (ORDER BY osm_id is null), then the
        // postcode grid in place of the nearest centroid, where the dir has
        // one of the point's country, then the centroid.
        let mut country_code = country_code;
        if let Some((way_id, _)) = street {
            if let Some((m_lat, m_lng)) = u32::try_from(way_id).ok().and_then(|id| self.street_middle(id)) {
                let (areas, point, country_there) = self.postal_areas_at(m_lat, m_lng, true);
                country_code = country_there.or(country_code);
                let found = self.postal_area_postcode(&areas, &upper_country(country_code), point)
                    .or_else(|| self.grid_postcode(m_lat, m_lng, country_code))
                    .or_else(|| self.centroid_postcode(m_lat, m_lng, country_code));
                if found.is_some() || addr.is_none() {
                    return found.map(|postcode| (postcode, country_code));
                }
            }
        }
        if let Some((_, house)) = addr {
            let (h_lat, h_lng) = (house.lat as f64, house.lng as f64);
            return self.postal_area_at(h_lat, h_lng, country_code)
                .or_else(|| self.grid_postcode(h_lat, h_lng, country_code))
                .or_else(|| self.centroid_postcode(h_lat, h_lng, country_code))
                .map(|postcode| (postcode, country_code));
        }
        // The postcode area covering the point: get_nearest_postcode takes
        // one before any centroid (ORDER BY osm_id is null).
        postcode_area
            .or_else(|| self.grid_postcode(lat, lng, country_code))
            .or_else(|| self.centroid_postcode(lat, lng, country_code))
            .map(|postcode| (postcode, country_code))
    }

    /// The postcode grid's at the point (lat, lng), where the cell's run is of the country
    /// `country_code`: the postcode most of the houses round the point carry
    /// (postcode_grid_build.h). It stands in for get_nearest_postcode's nearest centroid at each
    /// point that rule runs at, after the postal areas, which the rule takes before any centroid
    /// (ORDER BY osm_id is null): a US ZIP is an area OSM barely maps, and the centroid of a large
    /// one lies farther from its edge than a neighbour's, where the houses round the point carry
    /// their own. A cell of no postcode, past the reach of every house, has none, nor has one of a
    /// postcode the answer refuses (postcode_looks_valid), which then leaves the point to the
    /// centroids, as such a postal area's does.
    fn grid_postcode<'a>(&'a self, lat: f64, lng: f64, country_code: Option<[u8; 2]>) -> Option<Cow<'a, str>> {
        let (grid, cc) = (self.postcode_grid.as_ref()?, country_code?);
        let (country, code) = grid.postcode_at(cell_id_at_level(lat, lng, grid.level()))?;
        (country == cc && postcode_looks_valid(&cc, code)).then_some(Cow::Borrowed(code))
    }

    /// The postcode of the postcode centroid nearest the point (lat, lng) whose box holds it,
    /// get_nearest_postcode's after the postal areas. Like get_nearest_postcode, of the country
    /// `country_code` only: without it, a French "90012" can win over a US "90012" near the
    /// Maine/Quebec border or vice-versa — and the same digits often collide across countries.
    /// Where no country is known (ocean, Antarctica), of any, so the point still has one.
    fn centroid_postcode<'a>(&'a self, lat: f64, lng: f64, country_code: Option<[u8; 2]>) -> Option<Cow<'a, str>> {
        let country_gate: Option<u16> = country_code.map(|cc| {
            (cc[0] as u16) << 8 | (cc[1] as u16)
        });
        if let Some(files) = &self.postcode_centroids {
            // No centroid farther than its country's extent along either axis holds the point,
            // the most expand_by_meters' box reaches, but for a box across the antimeridian (read
            // below).
            let reach_m = country_code.map_or(MAX_POSTCODE_EXTENT_M, |cc| postcode_extent_m(cc, "")) * geography_box::BOX_REACH_FACTOR;
            let dlat = reach_m / METRES_PER_DEGREE;
            let (max_dist_sq, dlng) = {
                let metres_per_lng = METRES_PER_DEGREE * (lat.abs() + dlat).min(90.0).to_radians().cos();
                let dlng = if metres_per_lng > reach_m / 360.0 { reach_m / metres_per_lng } else { 360.0 };
                (postcode_files::distance(0.0, 0.0, dlat, dlng) * (1.0 + 1e-9), dlng)
            };
            let mut best: Option<(f64, Cow<'a, str>)> = None;
            let consider = |pc: postcode_files::Centroid, best: &mut Option<(f64, Cow<'a, str>)>| {
                if let Some(cc) = country_gate {
                    if pc.country_code != 0 && pc.country_code != cc { return; }
                }
                let d = postcode_files::distance(lat, lng, pc.lat as f64, pc.lng as f64);
                // A centroid farther than the best so far can't win (nearer_postcode), so its
                // postcode isn't read.
                if best.as_ref().is_some_and(|&(best_d, _)| d > best_d) { return; }
                // The box of expand_by_meters. A centroid of no country stands for one of the
                // point's.
                let pc_country = match pc.country_code {
                    0 => country_code.unwrap_or([0, 0]),
                    cc => cc.to_be_bytes(),
                };
                let (c_lat, c_lng) = (pc.lat as f64, pc.lng as f64);
                if !geography_box::box_covers(c_lat, c_lng, postcode_extent_m(pc_country, ""), lat, lng) { return; }
                let s = self.get_string(pc.postcode_id);
                // A country's other postcode patterns may reach less far.
                let extent = postcode_extent_m(pc_country, &s);
                if extent != postcode_extent_m(pc_country, "") && !geography_box::box_covers(c_lat, c_lng, extent, lat, lng) { return; }
                if centroid_postcode_ok(&s) && nearer_postcode(d, &s, best.as_ref().map(|(bd, bs)| (*bd, &**bs))) {
                    *best = Some((d, s));
                }
            };
            // The blocks of the centroids of `cells` nearer than `max_d`, the nearest box first:
            // no centroid is nearer than its block's box, so a block whose box is farther than
            // the best so far holds none that can win, and is left undecoded.
            let read = |cells: &[u64], max_d: f64, best: &mut Option<(f64, Cow<'a, str>)>| NEAR_BLOCKS.with_borrow_mut(|near| {
                files.blocks_near(cells, lat, lng, max_d, near);
                for n in near.iter() {
                    if best.as_ref().is_some_and(|&(best_d, _)| n.d > best_d) { continue; }
                    files.rows_of(n.block, n.rows, |_, pc| consider(pc, best));
                }
            });
            // The cells a centroid holding the point can be in.
            read(&cells_near_point(lat, lng, reach_m, self.admin_cell_level), max_dist_sq, &mut best);
            // A box across the antimeridian spans every longitude between its corners the long
            // way round (expand_by_meters), so a centroid within reach of the antimeridian at
            // the point's latitude can hold the point however far off it lies, past the cells
            // within reach. Those are read, at any distance, unless no centroid of the
            // antimeridian's cells can be as near as the best so far: none lies closer to the
            // point's longitude than `dlng` and a cell's diagonal (S2's kMaxDiag, 2.438 / 2^level
            // radians) short of the antimeridian. Past 70° the polar projection's boxes and the
            // cells reach farther round the pole than that, and the cells are always read.
            let near_pole = lat.abs() + dlat > geography_box::POLAR_LAT;
            let diagonal = (2.438 / (1u64 << self.admin_cell_level) as f64).to_degrees();
            let cell_lng = diagonal / (lat.abs() + dlat + diagonal).min(90.0).to_radians().cos();
            let gap = (180.0 - dlng - cell_lng - lng.abs()).max(0.0);
            if near_pole || best.as_ref().is_none_or(|&(best_d, _)| postcode_files::distance(0.0, 0.0, 0.0, gap) <= best_d) {
                let cells: Vec<u64> = [180.0, -180.0].into_iter()
                    .flat_map(|edge| cells_near_point(lat, edge, reach_m, self.admin_cell_level))
                    .collect();
                read(&cells, f64::INFINITY, &mut best);
            }
            return best.map(|(_, s)| s);
        }

        None
    }

    // --- Combined query ---

    fn resolve_admin(&self, scan: &mut AdminScan, lat: f64, lng: f64, street: Option<&(f64, WayRow, u32)>) -> AdminResult<'_> {
        // Hybrid admin resolution: when the primary is a street with a
        // pre-computed parent polygon, walk its admin_parents chain for
        // coarse ranks (country → city) and use PIP for finer ranks
        // (city_district / suburb / quarter / neighbourhood / block).
        // Chain walk mirrors Nominatim's parent_place_id chain and
        // filters out PIP false positives at coarse levels (e.g. a
        // query inside an overlapping province that isn't in the
        // street's addressline). Fine ranks stay on PIP because the
        // street's centroid often sits in a different suburb polygon
        // than the query point — chain walk would then pick the
        // centroid-containing suburb rather than the query's suburb.
        let admin_pip = self.find_admin_in(scan);
        let admin_chain = (|| -> Option<AdminResult<'_>> {
            let wp = self.way_parents.as_ref()?;
            let (_, _, way_idx) = street?;
            let start = Self::sidecar_u32(wp, u64::from(*way_idx))?;
            Some(self.find_admin_from_chain_in(start, scan))
        })();
        // Nominatim's answer puts, at each rank, a boundary holding the
        // result's centroid (for a street its ST_ClosestPoint to the query)
        // before the street's own address line (complete_address_details).
        // Where the parent chain and the query point name another state,
        // region or province, the boundaries at that point decide: a path
        // along the Bern-Valais ridge is Kandersteg's in Nominatim, but
        // its point nearest Leukerbad answers Valais.
        let admin_chain = admin_chain.map(|chain| {
            let mut street_ranks = None;
            let [state, region, province] = coarse_fields(
                [chain.state, chain.region, chain.province],
                [admin_pip.state.clone(), admin_pip.region.clone(), admin_pip.province.clone()],
                || {
                    let (_, _, way_idx) = street?;
                    let (plat, plng) = self.closest_point_on_way(*way_idx, lat, lng)?;
                    let at = self.find_admin(plat, plng);
                    street_ranks = Some(at.ranks);
                    Some([at.state, at.region, at.province])
                },
            );
            // A field the street's point names has the rank it has there.
            let mut ranks = chain.ranks;
            if let Some(at) = street_ranks {
                for field in ["state", "region", "province"] {
                    if at.get(field) != 0 { ranks.set(field, at.get(field)); }
                }
            }
            AdminResult { state, region, province, ranks, ..chain }
        });
        match admin_chain {
            Some(chain) => AdminResult {
                // Each field's rank is that of the polygon it came from.
                ranks: {
                    let mut ranks = admin_pip.ranks;
                    for (field, named) in [("country", chain.country.is_some()), ("state", chain.state.is_some()),
                                           ("province", chain.province.is_some()), ("region", chain.region.is_some())] {
                        if named { ranks.set(field, chain.ranks.get(field)); }
                    }
                    ranks
                },
                // Only country/state/region/province replace PIP from
                // chain — at those ranks PIP false-positives are rare
                // and chain walk rarely drifts vs query. For finer
                // ranks (state_district / county / municipality / city
                // and below) chain walk picks the centroid-containing
                // admin, which regresses when the street's centroid
                // sits in a different fine-grain polygon than the
                // query (Buenos Aires Comuna 14 vs Comuna 1, Chicago
                // 'West Chicago Township' leaking as municipality).
                country: chain.country.or(admin_pip.country),
                country_code: chain.country_code.or(admin_pip.country_code),
                state: chain.state.or(admin_pip.state),
                province: chain.province.or(admin_pip.province),
                region: chain.region.or(admin_pip.region),
                // Everything else: PIP is authoritative.
                state_district: admin_pip.state_district,
                county: admin_pip.county,
                district: admin_pip.district,
                municipality: admin_pip.municipality,
                city: admin_pip.city,
                town: admin_pip.town,
                village: admin_pip.village,
                hamlet: admin_pip.hamlet,
                city_district: admin_pip.city_district,
                borough: admin_pip.borough,
                suburb: admin_pip.suburb,
                quarter: admin_pip.quarter,
                neighbourhood: admin_pip.neighbourhood,
                city_block: admin_pip.city_block,
                // The place-node ranks (16 and up) are PIP's.
                boundary_ranks: admin_pip.boundary_ranks,
                postcode_area: admin_pip.postcode_area,
                unlabelled: admin_pip.unlabelled,
                rows: admin_pip.rows,
            },
            None => admin_pip,
        }
    }

    /// Street way `id`'s nodes, owned; None for NO_DATA, an id past the last way and a way
    /// without a segment.
    fn way_nodes(&self, id: u32) -> Option<Vec<NodeCoord>> {
        let mut nodes = Vec::new();
        (self.streets.as_ref()?.way(id, &mut nodes) && nodes.len() >= 2).then_some(nodes)
    }

    /// The point of street way `id` nearest (lat, lng), planar in degrees like
    /// PostGIS ST_ClosestPoint on placex geometry.
    fn closest_point_on_way(&self, id: u32, lat: f64, lng: f64) -> Option<(f64, f64)> {
        let streets = self.streets.as_ref()?;
        with_way_nodes(|nodes| if streets.way(id, nodes) { closest_point_on_line(nodes, lat, lng) } else { None })
    }

    // Collect the most-specific surfaceable landmark name plus the
    // name-deduped list of nearby POIs for the response.
    fn collect_landmark_and_places(&self, lat: f64, lng: f64) -> (Option<Cow<'_, str>>, Vec<PoiDetail>) {
        let pois = self.find_pois(lat, lng);
        // Landmark: name of the smallest contained polygon POI whose
        // category is a surfaceable landmark class (filters out
        // admin-level area labels, GENERIC_RANK30 and very
        // large natural / boundary features). find_pois already
        // sorts contained polygons first by area ascending, so the
        // first qualifying hit is the most specific enclosing
        // landmark.
        let landmark_name: Option<Cow<'_, str>> = pois.iter().find_map(|p| {
            if !p.contained { return None; }
            if poi_category_to_osm_class(p.category_id).is_some() {
                Some(p.name.clone())
            } else {
                None
            }
        });
        // De-dupe by name, keep nearest instance. Chain outlets in
        // dense cities (Starbucks / McDonald's / 7-Eleven) often
        // cluster several branches inside a single query radius — a
        // user browsing nearby landmarks wants a diverse list, not
        // three copies of the same brand. Pure-name dedup is safe in
        // practice because distinct venues with the same name are
        // rare at rank-30; where they legitimately exist (e.g. two
        // "Central Park" instances) the nearest one is still the one
        // the user most likely means.
        let mut places: Vec<PoiDetail> = Vec::with_capacity(pois.len());
        let mut seen_names: std::collections::HashSet<String> = std::collections::HashSet::with_capacity(pois.len());
        for p in pois.into_iter() {
            if seen_names.insert(p.name.to_string()) {
                places.push(PoiDetail {
                    name: p.name.to_string(),
                    category: p.category.to_string(),
                    distance_m: (p.distance_m * 10.0).round() / 10.0,
                });
            }
        }
        (landmark_name, places)
    }

    /// The address of (lat, lng); none for a point off the globe, or NaN: S2 would wrap it onto
    /// cells of the globe, whose streets, POIs and boundaries are near another point.
    pub fn query(&self, lat: f64, lng: f64) -> Address<'_> {
        if !coords_in_range(lat, lng) {
            return Address::default();
        }
        let max_dist = self.max_distance_sq;
        // The water holding the point, which nothing else in the answer depends on: a point no
        // address anchors (open sea) keeps it.
        let water = self.water.as_ref().and_then(|w| w.at(lat, lng));

        // Run geo lookups first so we know the primary street
        let (addr, interp, street) = self.query_geo(lat, lng);

        // One admin cell scan serves every admin lookup at the point.
        let mut admin_scan = AdminScan::new(self, lat, lng);
        let admin = self.resolve_admin(&mut admin_scan, lat, lng, street.as_ref());
        // The boundaries place nodes must sit inside to be accepted in the
        // address walk (Nominatim's `current_boundary`).
        let current_boundaries = self.find_current_boundaries(&mut admin_scan, admin.country_code);
        // address_havelevel: the ranks the address's boundaries hold take no
        // place node. A boundary-derived city/town/village covers rank 16
        // whatever its rank, so place nodes must not fill (or fuzzy-anchor
        // on) a competing rank-16 candidate. Nominatim gives a boundary its
        // linked place's rank even below its own (placex_triggers.sql:1129):
        // the parish of Winsford, linked to a town, ranks 16 where its
        // admin_level gives 20. The place type here need not be the one
        // Nominatim links, so promote_nested_ranks only raises, and the hold
        // stands in for that rank.
        let rank16 = admin.city.is_some() || admin.town.is_some() || admin.village.is_some();
        let held_ranks = admin.boundary_ranks | if rank16 { 1 << CurrentBoundaries::CITY } else { 0 };

        // Primary-feature selection: match Nominatim's reverse flow —
        // pick the closest rank-26+ feature (road OR addressable POI),
        // then refine with a house number if one sits on the selected
        // road within a small radius.
        //
        // Nominatim runs one query (`_find_closest_street_or_pois`) that
        // returns the closest named feature at rank 26+. If that feature
        // is a road (`rank_address <= 27`), it does a second query for
        // house numbers on the same road within ~100m and uses that as
        // the final match. We approximate this by comparing distances
        // between the nearest address point, interpolation segment, and
        // raw street way — whichever is closest becomes the primary.
        //
        // Prior behaviour (addr always wins) was too strong: it picked
        // `120 South Main Street` even when the Nominatim-equivalent
        // closest feature was a nearby pedestrian plaza with no
        // addressed point. This left us with a wrong road name + a
        // spurious house number on test points that sit on non-
        // residential ways.
        let mut house_number: Option<Cow<'_, str>> = None;
        // The house a street that won takes its number from: Nominatim's answer is then the
        // house, of the house's postcode.
        let mut street_house: Option<(u32, AddrRecord)> = None;
        let mut road: Option<Cow<'_, str>> = None;

        // POI primary-feature candidate: Nominatim's
        // _find_closest_street_or_pois returns the closest rank-26+
        // feature, which may be an addressable POI (amenity / shop /
        // tourism / historic / building / craft) in addition to roads.
        // We look up the nearest POI with a pre-computed parent_street_id
        // and treat it as a 4th candidate alongside address / interp /
        // street.
        //
        // Gated to ~100m: we don't index unnamed residential ways, so
        // without a radius cap the nearest named POI wins on queries
        // that fall on an unnamed road (Tokyo suburban residentials),
        // injecting a wrong road name into results where Nominatim
        // returns none. Nominatim's `_find_closest_street_or_pois`
        // uses 0.006 deg (~660m) but its set also contains unnamed
        // streets (rank_search=26, name=''), which win at 0m and
        // suppress far POIs — we approximate that by capping the
        // POI contribution to 100m.
        let poi_primary_raw = self.find_nearest_poi_with_parent(lat, lng);
        let poi_max_sq = {
            let r = 100.0_f64 / 6_371_000.0;
            r * r
        };
        let poi_primary = poi_primary_raw
            .filter(|(d, _)| *d <= poi_max_sq)
            // Chain-containment check: reject POIs whose parent admin
            // polygon isn't in the query's containing admin chain.
            // A river-boundary landmark whose centroid sits on the
            // other side of the state line would otherwise win as
            // primary for a query on the "correct" side; this filter
            // confirms the POI's pre-computed parent_poly_id actually
            // contains the query point before letting it compete.
            // POIs built with pre-#11 builders have parent_poly_id =
            // NO_DATA and bypass this check so old data still works.
            .filter(|(_, p)| self.poi_chain_contains_query(p, lat, lng));

        // addr_dist already reflects real distance-to-polygon for
        // building-sourced addr_points (computed in the cell sweep via
        // polygon_distance_sq). For node-sourced addr_points, it's
        // centroid distance, matching Nominatim's ST_Distance to a
        // point geometry.
        let addr_dist = addr.as_ref().map(|(d, _, _)| *d).unwrap_or(f64::MAX);
        // Nominatim's `_find_closest_street_or_pois` queries the placex
        // table (streets + rank-30 POIs / addr points). Interpolation
        // rows live in separate tables and are looked up only AFTER a
        // street wins (reverse.py `_find_tiger_number_for_street`), so
        // interpolation is not a primary candidate: that picked
        // abbreviated TIGER names ("W 1st St") over the actual OSM
        // street ("West 1st Street") whenever the TIGER segment sat
        // closer than the raw way. It only refines the street below.
        let street_dist = street.as_ref().map(|(d, _, _)| *d).unwrap_or(f64::MAX);
        let poi_dist = poi_primary.as_ref().map(|(d, _)| *d).unwrap_or(f64::MAX);

        // Nominatim's reverse.py lines 219-235 and 355-360: when the
        // closest result is an area (osm_type != 'N', rank_search > 27)
        // at distance 0 (query inside polygon), Nominatim prefers any
        // rank-30 NODE whose geometry is inside that polygon
        // (ST_CoveredBy on _best_geometry). This is how Nominatim
        // returns "Sushi teria, 350, 5th Avenue" for Empire State or
        // "6, City Hall Plaza" for Paris Hôtel de Ville instead of the
        // containing building/POI.
        //
        // We implement the real containment check: if the nearest
        // containing POI has a polygon and the nearest addr_point is a
        // node (no footprint) that point_in_polygon reports as
        // inside, demote the POI so the node wins. A water street
        // (is_water_street), of rank_search 26 or 27, takes no such node
        // in its place: a street that wins refines to a house number it is
        // the parent of alone (reverse.py lookup_street_poi).
        let mut effective_poi_dist = poi_dist;
        if poi_dist == 0.0 {
            if let (Some((_, addr_pt, _)), Some((_, poi))) = (addr.as_ref(), poi_primary.as_ref()) {
                // Case 1: addr polygon also contains the query (v9 building
                // addr_points with footprint geometry). Both records describe
                // the same underlying OSM way — Nominatim has a single placex
                // row carrying name + addr:* tags, so the addr-side fields
                // (housenumber, street) drive the result.
                if addr_dist == 0.0 {
                    effective_poi_dist = f64::MAX;
                } else if !addr_pt.is_building() && poi.is_polygon() && !Self::is_water_street(poi) {
                    let mut inside = false;
                    if let Some(verts) = self.poi_verts(&poi) {
                        inside = point_in_polygon(addr_pt.lat, addr_pt.lng, &verts);
                    }
                    // Fallback proximity gate: POI polygons go through the
                    // continent-filter simplifier (reduced vertex count),
                    // so an addr_point that is geographically inside the
                    // real OSM footprint may fall outside the simplified
                    // polygon used for point-in-polygon tests. If the
                    // addr_point is close enough to the query to be
                    // plausibly part of the same building (< 50 m AND
                    // closer than the nearest street), treat it as
                    // inside. Paris Hôtel de Ville: the simplified 5-
                    // vertex polygon rejects the addr-node at 17 m, yet
                    // the node carries housenumber=6 and street="Place
                    // de l'Hôtel de Ville" — exactly the primary
                    // Nominatim surfaces.
                    if !inside
                        && addr_dist.sqrt() * 6_371_000.0 < 50.0
                        && addr_dist < street_dist {
                        inside = true;
                    }
                    if inside {
                        effective_poi_dist = f64::MAX;
                    }
                }
            }
        }

        // Tier-1 POI boost: a major, well-known landmark (wikidata-
        // annotated attraction, castle, monument, national park, top-
        // category building etc.) within a reasonable radius should
        // win the primary slot over a slightly-closer nameless street.
        // Without this boost, clicking near the Eiffel Tower returns
        // "Rue de la Fédération" instead of "Eiffel Tower". The boost
        // is applied only to the comparison distance — the hard
        // max_dist cutoff below still uses the real distance, so we
        // don't drag in a landmark from kilometres away.
        //
        // Boost: 50 m subtracted from the comparison distance for
        // tier-1 POLYGON POIs (vertex_count > 0). Keeps the boost
        // tight enough that random side-street queries don't flip to
        // a nearby landmark (LA Civic Center, Cairo El Tahrir), while
        // still letting a landmark-inside-polygon query win over a
        // slightly-closer side road. Point-POIs (tier-1 attractions
        // like "Kilometre zero" in Red Square) are excluded because
        // they typically lack parent_postcode and would drop the
        // address' postcode when they win — Moscow 109012 regressed
        // to '—' when the tier-1 boost applied to a point attraction.
        // A water street is a street, which competes by its distance
        // alone.
        let tier1_bonus_sq: f64 = {
            let r = 50.0_f64 / 6_371_000.0_f64;
            r * r
        };
        let compare_poi_dist = match poi_primary.as_ref() {
            Some((_, poi)) if poi.tier == 1
                && poi.is_polygon()
                && !Self::is_water_street(poi)
                && effective_poi_dist < max_dist => {
                if effective_poi_dist > tier1_bonus_sq {
                    effective_poi_dist - tier1_bonus_sq
                } else {
                    0.0
                }
            }
            _ => effective_poi_dist,
        };
        let closest_feature_dist = addr_dist.min(street_dist).min(compare_poi_dist);
        let mut addr_won_primary = false;
        let mut poi_won_primary = false;
        if closest_feature_dist < max_dist {
            // Whichever feature class has the smallest distance wins.
            if compare_poi_dist <= addr_dist && compare_poi_dist <= street_dist {
                // Record the win here so postcode routing below follows the
                // SAME decision (incl. the tier-1 boost). It previously
                // recomputed the winner with the un-boosted distance, so a
                // POI that won primary only via the boost surfaced the POI's
                // road but the street's postcode. Nominatim routes the whole
                // address (postcode included) through whichever feature wins
                // the reverse lookup, so the flag must match the road pick.
                poi_won_primary = true;
                // POI is closest. Nominatim splits "POI with polygon
                // at dist=0" into two cases depending on rank_search:
                //  - rank 26 (highway=pedestrian/footway/etc. polygons
                //    like Mexico City's "Constitution Square") — the
                //    feature IS a road, so its own name is the `road`.
                //  - rank 30 (historic=monument, tourism=attraction,
                //    amenity=* polygons like Buenos Aires's Obelisco
                //    or the White House) — the feature is NOT a road,
                //    so `road` comes from its parent_place_id chain
                //    (parent_street in our indexer).
                // POI_FLAG_HIGHWAY is set by the builder for highway-
                // class polygons; without the flag we default to
                // parent_street even when the polygon contains the
                // query, matching nominatim's rank-30 chain walk. A
                // water street is a street wherever it wins.
                let (_, poi) = poi_primary.unwrap();
                let is_highway_poi = (poi.flags & POI_FLAG_HIGHWAY) != 0;
                let use_poi_name = Self::is_water_street(&poi) || (is_highway_poi && poi.is_polygon() && poi_dist == 0.0);
                if use_poi_name {
                    road = Some(self.get_string(poi.name_id));
                } else if poi.parent_street_id != NO_DATA {
                    road = Some(self.get_string(poi.parent_street_id));
                }
            } else if addr_dist <= street_dist && addr_dist <= effective_poi_dist {
                // Address is closest — use it with its housenumber.
                addr_won_primary = true;
                let (_, point, _) = addr.unwrap();
                house_number = Some(self.get_string(point.housenumber_id));
                // street_id == NO_DATA means the builder's parent-
                // street backfill didn't find a named way nearby; in
                // that case fall back to the closest-street result
                // instead of the missing addr:street.
                if point.street_id != NO_DATA {
                    road = Some(self.get_string(point.street_id));
                } else if let Some((_, way, _)) = street {
                    road = Some(self.get_string(way.name));
                }
            } else {
                // Street is closest — use its name, then refine with a
                // housenumber if an address whose parent_way_id matches
                // this street sits within ~100m of the query. Mirrors
                // Nominatim's `_find_housenumber_for_street` (reverse.py
                // :231): after the closest street wins, a second query
                // looks for the nearest rank-30 address point whose
                // parent_place_id matches the street's place_id, gated
                // by ST_DWithin(query, 0.001) (~100m).
                let (_, way, way_idx) = street.unwrap();
                road = if way.name != NO_DATA {
                    Some(self.get_string(way.name))
                } else {
                    None
                };
                let refine_max_sq = {
                    let r = 100.0_f64 / 6_371_000.0;
                    r * r
                };
                if let Some((id, addr_pt)) = self.find_addr_on_way(lat, lng, way_idx, refine_max_sq) {
                    if addr_pt.housenumber_id != NO_DATA {
                        house_number = Some(self.get_string(addr_pt.housenumber_id));
                        street_house = Some((id, addr_pt));
                    }
                }
                // No address point: Nominatim falls back to TIGER in the US.
                if house_number.is_none() && admin.country_code == Some(*b"US") {
                    if let Some(number) = road.as_deref().and_then(|name| self.find_interp_on_way(lat, lng, name, refine_max_sq)) {
                        house_number = Some(Cow::Owned(number.to_string()));
                    }
                }
            }
        }

        // Place nodes are matched against the street the address hangs from:
        // Nominatim takes a rank-30 answer's address from its parent street
        // (results.py _get_address_lookup_id). That is the street that won,
        // the address point's parent way, or for a POI the nearest street
        // when it carries the POI's parent street name (POIs keep only the
        // name). Without one, the query point stands in, as it does for a
        // water street, whose address is its own (rank_search <= 27).
        let address_street: Option<u32> = if closest_feature_dist >= max_dist {
            None
        } else if poi_won_primary {
            let (_, poi) = poi_primary.unwrap();
            street.as_ref().filter(|(_, w, _)| {
                !Self::is_water_street(&poi) && poi.parent_street_id != NO_DATA && w.name != NO_DATA
                    && self.get_string(w.name) == self.get_string(poi.parent_street_id)
            }).map(|(_, _, id)| *id)
        } else if addr_won_primary {
            addr.map(|(_, point, _)| point.parent_way_id)
        } else {
            street.as_ref().map(|(_, _, id)| *id)
        };
        let street_line = address_street.and_then(|id| self.way_nodes(id));
        let feature = match street_line.as_deref() {
            Some(line) => PlaceFeature::Line(line),
            None => PlaceFeature::Point(lat, lng),
        };
        // The answering house's addr:city: the address point that won, or the house a winning
        // street takes its number from, which is then Nominatim's result (a POI keeps no address,
        // and a street alone or a TIGER number has none).
        let addr_city: Option<u32> = self.addr_cities.as_ref().and_then(|cities| {
            let house = if addr_won_primary { addr.as_ref().map(|(_, _, id)| *id) } else { street_house.as_ref().map(|(id, _)| *id) };
            Self::sidecar_u32(cities, u64::from(house?))
        });
        let place = self.find_places(feature, &current_boundaries, held_ranks, addr_city);

        // Resolve postcode: mirror Nominatim's stored placex.postcode chain
        // (Tier 0 the house's own / Tier 1 its street's or the primary
        // feature's calculated_postcode / Tier 2 get_nearest_postcode at the
        // street's centroid, the house or the point: the postal area round
        // it, else the postcode grid's cell of it, else the nearest
        // centroid). Build-time postal-boundary
        // inheritance feeds Tier 1 (way_postcodes.bin for streets,
        // parent_postcode_id for POIs); at the query point Tier 2 takes the
        // postal area find_admin found there. Tier 0 and Tier 1 are
        // primary-feature-driven — only the winning feature's inherited
        // postcode is consulted (poi_won_primary is set by the SAME cascade
        // branch that picked the road above), so a POI win no longer leaks
        // the street's inherited ZIP (Mexico City Zócalo 06060 leak).
        // The street the postcode comes from: a house's parent street (placex_update takes the
        // parent's postcode, find_parent_for_poi's street, else the nearest), else the street that
        // won.
        let postcode_street: Option<(u32, WayRow)> = if poi_won_primary {
            None
        } else if addr_won_primary {
            addr.as_ref()
                .and_then(|(_, point, _)| (point.parent_way_id != NO_DATA).then_some(point.parent_way_id))
                .and_then(|id| Some((id, self.streets.as_ref()?.row(id)?)))
                .or_else(|| street.as_ref().map(|(_, w, idx)| (*idx, *w)))
        } else {
            street.as_ref().map(|(_, w, idx)| (*idx, *w))
        };
        let resolved_postcode = self.resolve_postcode_in(
            lat, lng,
            postcode_street.as_ref().map(|(idx, w)| (*idx as usize, w)),
            if addr_won_primary {
                addr.as_ref().map(|(_, a, id)| (*id as usize, a))
            } else {
                street_house.as_ref().map(|(id, a)| (*id as usize, a))
            },
            if poi_won_primary { poi_primary.as_ref().map(|(_, p)| p) } else { None },
            // TIGER tier: only when the street itself won the primary.
            // Nominatim consults location_property_tiger only after the
            // street wins and no rank-30 housenumber row was found; a POI
            // or addr-point win routes the postcode through that feature.
            if poi_won_primary || addr_won_primary || street_house.is_some() { None } else {
                interp.as_ref().map(|(d, s, _, idx)| (*idx as usize, *d, &**s))
            },
            admin.postcode_area.clone(),
            admin.country_code,
        );

        // Where no boundary holds the point, its place nodes are the whole
        // address: Nominatim has no country in Antarctica (no `aq` in
        // settings/country_settings.yaml) and answers a station with its
        // place nodes alone. Boundaries without a country, city or state
        // still give no answer: they are mostly a coast or border our
        // country polygon misses.
        let anchored = road.is_some() || admin.country.is_some() || admin.city.is_some() || admin.state.is_some();
        let place_nodes_only = admin.is_empty() && !place.is_empty();
        // Water the primary lanes leave out gives no road nor postcode, but a
        // point where it would win the slot keeps its landmark and `places`
        // where nothing else anchors the address (a glacier on a coast no
        // boundary holds): where it is the POI the lanes would find if they
        // took it too, within reach of the slot as poi_primary is. A glacier
        // or bay is an area label there: a specific POI found near takes the
        // lane from it, and one too far for the slot leaves the point nothing.
        let water_holds = || {
            self.find_nearest_poi_or_water_with_parent(lat, lng)
                .filter(|(d, p)| {
                    Self::is_out_of_the_primary_lanes(p) && *d <= poi_max_sq && self.poi_chain_contains_query(p, lat, lng)
                })
                .is_some()
        };
        if !anchored && !place_nodes_only && !water_holds() {
            return Address { water, ..Address::default() };
        }
        // Nor does the nearest postcode centroid give one where water alone holds the point.
        let resolved_postcode = resolved_postcode.filter(|_| anchored || place_nodes_only);

        let (landmark_name, places) = self.collect_landmark_and_places(lat, lng);

        // Note on name-suffix normalisation (previously considered as
        // Option 6): Nominatim does not normalise the localname it
        // surfaces in the reverse response — `classtypes.get_label_tag`
        // (v1/classtypes.py:18) emits the raw name verbatim and
        // `icu_tokenizer.yaml` only touches the search-side token
        // pipeline, not the display label. Cases we previously flagged
        // as "suffix variants" ("Izumi 2-chome"↔"Izumi 2", "Colonia
        // Centro"↔"Centro", "The Loop"↔"Loop", "Tverskoy"↔"Tverskoy
        // District") were in fact pick-divergences between the admin
        // relation and the place node — both carry distinct OSM names
        // that each become the label verbatim. The admin-first priority
        // flip covers every suffix case we had in the 25-city test,
        // matching Nominatim's isguess-asc ordering. Adding a suffix
        // stripper on our side would diverge from Nominatim without
        // improving parity, so Option 6 is a no-op by design.
        //
        // Merge admin + place results (admin authoritative, place as fallback).
        // No post-hoc dedup: Nominatim's rank-based address-row build lets
        // the same name appear in multiple fields (e.g. Paris city + Paris
        // city_district), and the find_admin rank pipeline already enforces
        // first-label-wins for same-rank collisions.
        //
        // Nominatim's reverse response does NOT inject place-node neighbourhoods
        // when the admin chain already covers the sub-city range densely.
        // Specifically, `complete_address_details` walks `addressline` which
        // comes from `placex_triggers.sql` — the chain links each feature to
        // parents by parent_place_id, not by spatial containment. If the
        // admin chain jumps from suburb (rank 20) straight to city_block
        // (rank 24) the intermediate rank 22 stays empty; Nominatim does NOT
        // fill it from a place=neighbourhood node that happens to contain
        // the query point. Approximate this: when admin has BOTH a suburb
        // and a city_block (so the chain is dense at the sub-city range),
        // suppress the place-node neighbourhood/quarter overrides. Paris
        // 4th Arrondissement has suburb=4th Arrondissement + city_block=
        // Quartier Saint-Merri and Nominatim emits no neighbourhood —
        // before this rule we'd inject `neighbourhood=Beaubourg` from a
        // nearby place node.
        let admin_chain_dense = admin.suburb.is_some() && admin.city_block.is_some();
        let allow_place_quarter = !admin_chain_dense;
        let allow_place_neighbourhood = !admin_chain_dense;
        // A field a boundary and a place node both fill names the finer
        // (label_field): Nominatim's address keeps the first row of a
        // label, its rows the finest first, and its insert_addresslines
        // orders candidates by `rank_address, isguess asc, distance`
        // (placex_triggers.sql:576), so at one rank the boundary's row
        // (isguess=false) comes first: "Tverskoy District" (AL8
        // boundary) over "Tverskoy" (place=suburb node), and Beijing's
        // "Donghuamen Subdistrict" over "Donghuamen". A suburb node of
        // rank 22 inside a parish of rank 20 is the suburb, and the
        // parish's row stays in display_name. Each field's rank_address
        // is that of the polygon or the place node it came from.
        let mut ranks = admin.ranks;
        let mut unlabelled = admin.unlabelled;
        let mut label = |field: &str, boundary, node| {
            let (named, lost) = label_field(boundary, admin.ranks.get(field), node, place.ranks.get(field));
            if let Some((_, rank)) = &named { ranks.set(field, *rank); }
            unlabelled.extend(lost);
            named.map(|(name, _)| name)
        };
        let city = label("city", admin.city, place.city);
        let town = label("town", admin.town, place.town);
        let village = label("village", admin.village, place.village);
        let hamlet = label("hamlet", admin.hamlet, place.hamlet);
        let suburb = label("suburb", admin.suburb, place.suburb);
        let quarter = label("quarter", admin.quarter, place.quarter.filter(|_| allow_place_quarter));
        let neighbourhood = label("neighbourhood", admin.neighbourhood, place.neighbourhood.filter(|_| allow_place_neighbourhood));
        // Same-name suppression, observed Nominatim behaviour: the Kingdom
        // of the Netherlands (AL2) and the constituent country (AL3) are both
        // named "Nederland"; Nominatim's address keeps one row per label and
        // drops the duplicate, which would be region=Netherlands here. Its
        // display_name joins every row, the region's too.
        let (region, country_region) = match (admin.region, &admin.country) {
            (Some(r), Some(c)) if r == *c => (None, Some((ranks.get("region"), r))),
            (r, _) => (r, None),
        };
        let (area_rows, node_rows) = (admin.rows, place.named_rows);
        let mut address = AddressDetails {
            ranks,
            unlabelled: unlabelled.into_iter().chain(country_region).collect(),
            landmark: landmark_name,
            // A string in a tier this mode doesn't ship (admin mode has no
            // street or addr strings) resolves to "" — omit the field.
            house_number: house_number.filter(|s| !s.is_empty()),
            road: road.filter(|s| !s.is_empty()),
            city,
            town,
            village,
            hamlet,
            municipality: admin.municipality,
            state: admin.state,
            province: admin.province,
            region,
            state_district: admin.state_district,
            county: admin.county,
            district: admin.district,
            borough: admin.borough,
            suburb,
            quarter,
            city_district: admin.city_district,
            neighbourhood,
            city_block: admin.city_block,
            // Postcode from resolve_postcode(): 3-tier chain
            // (own → inherited → postal area, grid or nearest centroid).
            // Reject if its country has no postal-code system or if
            // the string doesn't match the country's pattern, then
            // normalise whitespace/separators per country.
            postcode: match resolved_postcode {
                Some((pc, postcode_country)) => match postcode_country {
                    Some(cc) => {
                        if country_has_no_postcode(&cc) || !postcode_looks_valid(&cc, &pc) {
                            None
                        } else {
                            // The postcode as read unless formatting changes it.
                            let formatted = match format_postcode(&cc, &pc) {
                                Cow::Owned(s) => Some(s),
                                Cow::Borrowed(_) => None,
                            };
                            Some(formatted.map_or(pc, Cow::Owned))
                        }
                    }
                    None => Some(pc),
                },
                None => None,
            },
            country: admin.country,
            country_code: admin.country_code.map(|c| String::from_utf8_lossy(&c).into_owned()),
        };
        if let Some(addr_city) = addr_city {
            let country = address.country_code.as_deref().unwrap_or_default().to_ascii_uppercase();
            let rows = self.crosscheck_rows(addr_city, &area_rows, &mut admin_scan, &node_rows, feature, &country);
            let areas: Vec<CrosscheckRow> = area_rows.iter().map(|r| self.area_row(r)).collect();
            address.crosscheck(&rows, &areas);
        }
        let display_name = format_address(&address);
        Address { licence: Licence, display_name, address, places, water }
    }

    /// The rows of an address that are named `name` (a string id), in Nominatim's order within a
    /// rank (results.py:617-622: fromarea, then distance, descending, then rank_search): the
    /// polygons named so, by their own name or by a place node's they link, the farthest from
    /// `feature` first (area_row_distance), then the place nodes of `nodes`
    /// (PlaceResult::named_rows), the farthest first, equals by rank_search, but those a place
    /// area links (links_by_name). The polygons are those that
    /// are rows of `feature` as getNearFeatures takes them (partition-functions.sql:39-44): of a
    /// point, the polygons holding it, of a street (a house's rows are its street's), those the
    /// street runs through. They are those of `areas`, the address's ranked polygons (found by
    /// `scan`), and the others: of the scan where the feature is a point, such as a larger
    /// boundary of a level `areas` holds it by, else of any admin cell under the street, each at
    /// area_row_rank's rank, a boundary the point is outside at own_area_rank's; `country` is the
    /// point's, upper case.
    fn crosscheck_rows(&self, name: u32, areas: &[RankedPoly], scan: &mut AdminScan, nodes: &[NamedRow],
                       feature: PlaceFeature<'_>, country: &str) -> Vec<CrosscheckRow<'_>> {
        let named = |poly: &AdminPolygon| poly.name_id == name || self.links_name(poly, name);
        let same = |a: &AdminPolygon, b: &AdminPolygon| a.ring_offset == b.ring_offset && a.ring_bytes == b.ring_bytes;
        let geometry = &self.admin_geometry;
        let runs_through = |line: &[NodeCoord], poly: &AdminPolygon| geometry.has_rings(poly)
            && line_runs_through(line, &geometry.rings(poly), |lat, lng| geometry.contains_once(poly, AdminPoint::new(lat, lng)));
        // A polygon holding the point is a row of a street only where the street runs through it.
        let mut polygons: Vec<RankedPoly> = areas.iter()
            .filter(|r| named(&r.poly) && match feature {
                PlaceFeature::Point(..) => true,
                PlaceFeature::Line(line) => runs_through(line, &r.poly),
            })
            .copied().collect();
        // The scan lists the point's admin cell alone, and a street runs through the areas of
        // every cell under it.
        let mut candidates = scan.rings.clone();
        if let PlaceFeature::Line(line) = feature {
            let (lat, lng) = scan.pip.point.degrees();
            let here = cell_id_at_level(lat, lng, self.admin_cell_level);
            let mut listed: std::collections::HashSet<u32> = candidates.iter().map(|&(id, _)| id & ID_MASK).collect();
            for cell in cells_under_line(line, STREET_CELLS_MARGIN_M, self.admin_cell_level) {
                if cell == here { continue; }
                self.admin_index.with_ids(cell, |ids| {
                    for &id in ids {
                        if !listed.insert(id & ID_MASK) { continue; }
                        if let Some(poly) = self.admin_polygon(id & ID_MASK) { candidates.push((id & ID_MASK, poly)); }
                    }
                });
            }
        }
        let mut seen: Vec<u32> = Vec::new();
        let scanned = scan.rings.len();
        for (k, &(id, poly)) in candidates.iter().enumerate() {
            if !is_address_area(&poly) || !named(&poly) || seen.contains(&(id & ID_MASK)) { continue; }
            seen.push(id & ID_MASK);
            if areas.iter().any(|r| same(&r.poly, &poly)) { continue; }
            let (row, at_point) = match feature {
                PlaceFeature::Point(..) => (scan.pip.holds(self, id, &poly), true),
                PlaceFeature::Line(line) => (runs_through(line, &poly), k < scanned && scan.pip.holds(self, id, &poly)),
            };
            if !row { continue; }
            // A boundary the point is outside ranks under the polygons round it, not the point's.
            let rank = if at_point || poly.admin_level == 15 { self.area_row_rank(&poly, areas, country) } else { self.own_area_rank(&poly) };
            if ADDRESS_AREA_RANKS.contains(&rank) {
                polygons.push(RankedPoly { poly, rank_address: rank });
            }
        }
        // Most ranks have one polygon of the name, which needs no distance.
        if polygons.iter().enumerate().any(|(i, a)| polygons[..i].iter().any(|b| b.rank_address == a.rank_address)) {
            let mut far: Vec<(f64, RankedPoly)> = polygons.iter().map(|r| (self.area_row_distance(&r.poly, feature), *r)).collect();
            far.sort_by(|a, b| b.0.total_cmp(&a.0));
            polygons = far.into_iter().map(|(_, r)| r).collect();
        }
        let place_area = |poly: &AdminPolygon| poly.admin_level == 15 && poly.name_id == name;
        let place_areas: Vec<RankedPoly> = areas.iter().filter(|r| place_area(&r.poly)).copied()
            .chain(candidates.iter().filter(|(_, poly)| place_area(poly) && !areas.iter().any(|r| same(&r.poly, poly)))
                .map(|&(_, poly)| RankedPoly { poly, rank_address: self.area_row_rank(&poly, areas, country) }))
            .collect();
        let mut nodes: Vec<NamedRow> = nodes.iter().filter(|row| !place_areas.iter().any(|r| self.links_by_name(r, &row.node))).copied().collect();
        // Equals by rank_search, the higher first (results.py:622).
        let search_rank = |row: &NamedRow| PLACE_NODE_SEARCH_RANK.get(usize::from(row.node.place_type)).copied().unwrap_or(0);
        nodes.sort_by(|a, b| b.distance.total_cmp(&a.distance).then_with(|| search_rank(b).cmp(&search_rank(a))));
        let mut rows: Vec<CrosscheckRow> = polygons.iter()
            .map(|r| self.area_row(r))
            .chain(nodes.iter().map(|row| CrosscheckRow { rank: row.rank, name: self.get_string(name), field: Some(row.field) }))
            .collect();
        // Stable: within a rank the polygons stay ahead of the nodes.
        rows.sort_by_key(|row| row.rank);
        rows
    }

    /// The address row of a ranked polygon: its rank, its own name and its field.
    fn area_row(&self, r: &RankedPoly) -> CrosscheckRow<'_> {
        CrosscheckRow { rank: r.rank_address, name: self.get_string(r.poly.name_id), field: polygon_field(r) }
    }

    /// The rank_address of `poly`, a row of an address that is none of the address's ranked
    /// polygons `areas`: that of the polygon of `areas` of its level and place types, else for a
    /// place area its own before any lift (place_area_ranks), and for a boundary the one
    /// promote_nested_ranks gives it from its level's rank in `country` under the coarser
    /// polygons of `areas`: a place tag's rank where that is above its parent's
    /// (placex_triggers.sql:1189-1199). A boundary that repeats its parent and links a place node
    /// ranks as promote_nested_ranks ranks it: its node's rank, above 3 and below 26, else 0.
    fn area_row_rank(&self, poly: &AdminPolygon, areas: &[RankedPoly], country: &str) -> u8 {
        if poly.admin_level != 15 && poly.own_place_type == BOUNDARY_REPEATS_PARENT {
            let linked = place_type_to_rank(poly.place_type_override, country);
            return if (4..26).contains(&linked) { linked } else { 0 };
        }
        let kin = |r: &&RankedPoly| r.poly.admin_level == poly.admin_level
            && (r.poly.place_type_override, r.poly.own_place_type) == (poly.place_type_override, poly.own_place_type);
        if let Some(r) = areas.iter().find(kin) {
            return r.rank_address;
        }
        if poly.admin_level == 15 {
            return place_area_ranks(poly, country).1;
        }
        let initial = self.admin_config.to_rank(country, poly.admin_level);
        if initial == 0 {
            return 0;
        }
        let mut ranked: Vec<RankedPoly> = areas.iter().filter(|r| r.poly.admin_level < poly.admin_level).copied().collect();
        ranked.push(RankedPoly { poly: *poly, rank_address: initial });
        let place_areas: Vec<AdminPolygon> = areas.iter().filter(|r| r.poly.admin_level == 15).map(|r| r.poly).collect();
        promote_nested_ranks(&mut ranked, place_areas.iter(), country, |a, b| self.admin_geometry.polygon_within(a, b));
        let same = |r: &&RankedPoly| r.poly.ring_offset == poly.ring_offset && r.poly.ring_bytes == poly.ring_bytes;
        ranked.iter().find(same).map_or(initial, |r| r.rank_address)
    }

    /// The rank_address of the boundary `poly`, a row of an address whose point it does not hold:
    /// the one find_admin gives it at its own point on surface, as placex_update ranks a boundary
    /// under the boundary of the highest level below its own round its centroid
    /// (placex_triggers.sql:881-906); where find_admin there ranks it in no row, area_row_rank's
    /// under the polygons of the other levels round that point, in their country. 0 without rings.
    fn own_area_rank(&self, poly: &AdminPolygon) -> u8 {
        let Some((lat, lng)) = self.admin_geometry.point_on_surface(poly) else { return 0 };
        let there = self.find_admin(f64::from(lat), f64::from(lng));
        let same = |r: &&RankedPoly| r.poly.ring_offset == poly.ring_offset && r.poly.ring_bytes == poly.ring_bytes;
        if let Some(r) = there.rows.iter().find(same) {
            return r.rank_address;
        }
        let others: Vec<RankedPoly> = there.rows.iter().filter(|r| r.poly.admin_level != poly.admin_level).copied().collect();
        self.area_row_rank(poly, &others, &upper_country(there.country_code))
    }

    /// getNearFeatures' distance of the area `poly` from `feature` (partition-functions.sql:32-36),
    /// planar in degrees as ST_Distance is on lon/lat: the distance from the feature's centre to
    /// the area, none inside it, and a hundred-thousandth of the feature's to the area's centre as
    /// a tie breaker. A street's centre is its middle (line_middle), an area's its point on
    /// surface (get_center_point).
    fn area_row_distance(&self, poly: &AdminPolygon, feature: PlaceFeature<'_>) -> f64 {
        let geometry = &self.admin_geometry;
        let centre = match feature {
            PlaceFeature::Point(lat, lng) => Some((lat, lng)),
            PlaceFeature::Line(line) => line_middle(line),
        };
        let to_area = centre.map_or(0.0, |(lat, lng)| {
            if geometry.contains_once(poly, AdminPoint::new(lat, lng)) { 0.0 } else { rings_distance(&geometry.rings(poly), lat, lng) }
        });
        let to_centre = geometry.point_on_surface(poly).map_or(0.0, |(lat, lng)| feature_distance(feature, lat as f64, lng as f64));
        to_area + 0.00001 * to_centre
    }

    /// Whether the boundary of polygon `poly` links a place node named `name` (linked_names).
    fn links_name(&self, poly: &AdminPolygon, name: u32) -> bool {
        self.linked_names.get_or_init(|| self.read_linked_names()).binary_search(&(boundary_key(poly), name)).is_ok()
    }

    /// Whether the ranked place area `area` (admin_level 15) links the place node `node` by
    /// find_linked_place's name match, which runs for ways as well as relations and which the
    /// builder runs for boundaries only (placex_triggers.sql:359-385): the area has the node's
    /// name, its rank_address is the one the node's type has (compute_place_rank), which an area
    /// lifted by one round it no longer has, and it holds the node, where it links no node by its
    /// label or wikidata: find_linked_place links one node, which may have the area's name and
    /// type too.
    fn links_by_name(&self, area: &RankedPoly, node: &PlaceNode) -> bool {
        let (poly, geometry) = (&area.poly, &self.admin_geometry);
        poly.admin_level == 15 && poly.name_id == node.name_id && poly.own_place_type == 0
            && PLACE_NODE_RANK.get(usize::from(node.place_type)) == Some(&area.rank_address)
            && geometry.has_rings(poly) && geometry.contains_once(poly, AdminPoint::new(f64::from(node.lat), f64::from(node.lng)))
            && !self.links_any(poly)
    }

    /// Whether the boundary of polygon `poly` links a place node (linked_names), of its own name
    /// or another.
    fn links_any(&self, poly: &AdminPolygon) -> bool {
        let key = boundary_key(poly);
        let linked = self.linked_names.get_or_init(|| self.read_linked_names());
        linked.get(linked.partition_point(|&(k, _)| k < key)).is_some_and(|&(k, _)| k == key)
    }

    /// linked_names from place_hidden.bin: each label's boundary and name.
    fn read_linked_names(&self) -> Vec<(u64, u32)> {
        let mut linked = Vec::new();
        if let Some(places) = &self.places {
            places.for_each_label(|polygon, name| {
                if let Some(poly) = self.admin_polygon(polygon) {
                    linked.push((boundary_key(&poly), name));
                }
            });
        }
        linked.sort_unstable();
        linked.dedup();
        linked.shrink_to_fit();
        linked
    }
}

/// What the polygons of one boundary share: their record but for the ring and the area (name,
/// level, place types, country). The builder writes each outer ring of a relation as a polygon of
/// its own, and a node links the boundary by its largest (place_claims.h boundary_polygons).
pub fn boundary_key(poly: &AdminPolygon) -> u64 {
    u64::from(poly.name_id) << 32 | u64::from(poly.country_code) << 16 | u64::from(poly.admin_level & 15) << 12
        | u64::from(poly.place_type_override & 15) << 8 | u64::from(poly.own_place_type)
}

/// Whether `poly` is a polygon find_admin_in ranks: a boundary of a level below 12 but a postal
/// area's, of a country at level 2, or a place area with a place type; of an area. A boundary
/// that repeats its parent and links no place node is none (BOUNDARY_REPEATS_PARENT).
pub fn is_address_area(poly: &AdminPolygon) -> bool {
    poly.area > 0.0 && match poly.admin_level {
        15 => poly.place_type_override != 0,
        _ if poly.place_type_override == BOUNDARY_REPEATS_PARENT => false,
        2 => poly.country_code != 0,
        level => level < 12 && level != POSTCODE_AREA_LEVEL,
    }
}

/// The side of the line p-q ([lng, lat] each) that r lies on: 1 to the left, -1 to the right, 0
/// on it.
fn side(p: [f64; 2], q: [f64; 2], r: [f64; 2]) -> i8 {
    let cross = (q[0] - p[0]) * (r[1] - p[1]) - (q[1] - p[1]) * (r[0] - p[0]);
    i8::from(cross > 0.0) - i8::from(cross < 0.0)
}

/// Where `p`, on the line of the segment a-b, lies along it: 0 at a, 1 at b, measured along the
/// axis the segment runs further on. None for a segment of no length.
fn along(a: [f64; 2], b: [f64; 2], p: [f64; 2]) -> Option<f64> {
    let axis = usize::from((b[1] - a[1]).abs() > (b[0] - a[0]).abs());
    (a[axis] != b[axis]).then(|| (p[axis] - a[axis]) / (b[axis] - a[axis]))
}

/// Whether `p` lies on the segment c-d, an end included.
fn on_segment(p: [f64; 2], c: [f64; 2], d: [f64; 2]) -> bool {
    side(c, d, p) == 0 && (c[0].min(d[0])..=c[0].max(d[0])).contains(&p[0]) && (c[1].min(d[1])..=c[1].max(d[1])).contains(&p[1])
}

/// Whether `line` meets the polygon of `rings` (AdminGeometry::rings: [lng, lat] each, the outer
/// ring then its holes) in more than a point, as getNearFeatures takes an area for a line
/// (ST_NPoints(ST_Intersection) > 1, partition-functions.sql:44): a node of the line inside it
/// (`inside(lat, lng)`), a segment crossing an edge, running along one or passing inside between
/// the vertices it meets, or two points where the line touches a ring. A node the boundary
/// shares is the vertex its f32 coordinates round from, and takes the vertex's own: the line
/// touches the boundary there, and runs along it between two such.
pub fn line_runs_through(line: &[NodeCoord], rings: &[Vec<[f64; 2]>], mut inside: impl FnMut(f64, f64) -> bool) -> bool {
    let vertex = |n: &NodeCoord| rings.iter().flatten().find(|v| v[0] as f32 == n.lng && v[1] as f32 == n.lat).copied();
    let points: Vec<[f64; 2]> = line.iter().map(|n| vertex(n).unwrap_or([f64::from(n.lng), f64::from(n.lat)])).collect();
    let edges = || rings.iter().flat_map(|ring| ring.windows(2)).map(|e| (e[0], e[1]));
    let on_ring = |p: [f64; 2]| edges().any(|(c, d)| on_segment(p, c, d));
    let mut touches: Vec<[f64; 2]> = Vec::new();
    for &p in &points {
        if on_ring(p) {
            touches.push(p);
        } else if inside(p[1], p[0]) {
            return true;
        }
    }
    for w in points.windows(2) {
        let (a, b) = (w[0], w[1]);
        // Where the segment meets a vertex between its ends: between two such, or such a vertex
        // and an end, it lies wholly inside or wholly outside.
        let mut cuts = vec![0.0, 1.0];
        for (c, d) in edges() {
            let (abc, abd) = (side(a, b, c), side(a, b, d));
            if abc * abd < 0 && side(c, d, a) * side(c, d, b) < 0 {
                return true;
            }
            if abc == 0 && abd == 0 {
                if let (Some(tc), Some(td)) = (along(a, b, c), along(a, b, d)) {
                    if tc.max(td).min(1.0) > tc.min(td).max(0.0) {
                        return true;
                    }
                }
            }
            for (v, s) in [(c, abc), (d, abd)] {
                if let Some(t) = along(a, b, v).filter(|&t| s == 0 && t > 0.0 && t < 1.0) {
                    touches.push(v);
                    cuts.push(t);
                }
            }
        }
        if cuts.len() > 2 || on_ring(a) || on_ring(b) {
            cuts.sort_by(f64::total_cmp);
            for t in cuts.windows(2).filter(|t| t[1] > t[0]) {
                let m = (t[0] + t[1]) / 2.0;
                let p = [a[0] + (b[0] - a[0]) * m, a[1] + (b[1] - a[1]) * m];
                if !on_ring(p) && inside(p[1], p[0]) {
                    return true;
                }
            }
        }
    }
    touches.sort_by(|p, q| p[0].total_cmp(&q[0]).then(p[1].total_cmp(&q[1])));
    touches.dedup();
    touches.len() > 1
}

/// The planar distance in degrees from (lat, lng) to the nearest edge of `rings` ([lng, lat]
/// each); infinite without an edge.
pub fn rings_distance(rings: &[Vec<[f64; 2]>], lat: f64, lng: f64) -> f64 {
    rings.iter()
        .flat_map(|ring| ring.windows(2))
        .map(|e| planar_segment_distance((lat, lng), (e[0][1], e[0][0]), (e[1][1], e[1][0])))
        .fold(f64::INFINITY, f64::min)
}

/// The planar distance in degrees from (lat, lng) to `feature`.
pub fn feature_distance(feature: PlaceFeature<'_>, lat: f64, lng: f64) -> f64 {
    match feature {
        PlaceFeature::Point(plat, plng) => (lat - plat).hypot(lng - plng),
        PlaceFeature::Line(line) => line.windows(2)
            .map(|w| planar_segment_distance((lat, lng), (w[0].lat as f64, w[0].lng as f64), (w[1].lat as f64, w[1].lng as f64)))
            .fold(f64::INFINITY, f64::min),
    }
}

/// The planar distance in degrees from p to the segment a-b, each (lat, lng).
fn planar_segment_distance(p: (f64, f64), a: (f64, f64), b: (f64, f64)) -> f64 {
    let (dx, dy) = (b.0 - a.0, b.1 - a.1);
    let len_sq = dx * dx + dy * dy;
    let t = if len_sq == 0.0 { 0.0 } else { (((p.0 - a.0) * dx + (p.1 - a.1) * dy) / len_sq).clamp(0.0, 1.0) };
    (p.0 - (a.0 + t * dx)).hypot(p.1 - (a.1 + t * dy))
}

// --- Multi-continent dispatcher ---
//
// Holds one optionally-loaded `Index` per continent. Each query is
// routed to the continents that cover it (`Continent::covers_query`);
// for those with an index loaded the query runs and the most-specific
// result wins. Unloaded matches are surfaced via `unloaded_matches()` so
// the on-demand downloader can fetch them.
//
// Backward-compat: if `<root>/admin_cells.bin` exists at construction
// time, the old flat single-region layout is wrapped in a single
// "world" slot that covers every query. New deployments use per-
// continent subdirs.

/// The stamp of a continent dir the server's downloader fills: the date of
/// the build it holds a complete selection of, with the bucket and the
/// selection (the downloader's stamp_text), and no other file the
/// downloader put there nor of a name a file of the build has. The
/// downloader removes it before it writes or removes anything else in the
/// dir but FILE_LEDGER, and puts it back once the last of the selection is
/// in and the files of others are gone, a new file renamed into place: a
/// load that takes no lock tells by it that a download ran (DirMarks).
pub const BUILD_STAMP: &str = ".build_date";

/// The ledger the server's downloader keeps in a continent dir of what each
/// file there holds: lines `<name> <sha256>`, or `<name> -` where it is
/// unknown, the last line of a name the one that counts. It names every
/// file a download put there until one removes the file. A download makes
/// it, where the dir has none, before it removes BUILD_STAMP or any file:
/// a dir holding it but no BUILD_STAMP is one whose download did not
/// finish, and may mix two builds' or selections' files; one holding
/// neither was laid out by other means. A download appends to it or renames
/// a new one over it, and never writes it in place.
pub const FILE_LEDGER: &str = ".file_sha256";

/// The file in a continent dir the server's downloader fills whose flock(2) lock (on unix, where
/// the file system has locks) keeps a download into the dir and the loads of the dir apart,
/// those of other processes too (servers sharing a data dir). A download holds an exclusive lock
/// on it from start to end; a load (a server starting, or a hot-add once its download is in)
/// holds a shared one from before it checks the dir's BUILD_STAMP until it has loaded the dir.
/// Each makes it where it isn't there, and never truncates it, so that a load and the first
/// download into a dir lock the same file. Downloads go one at a time, and no server loads a dir
/// while a download changes its files (one that can't make the file loads without a lock, and
/// drops what it loaded where a download ran meanwhile: LockFor::Load). Not the dir itself,
/// which its transport decodes lock (the install lock).
pub const DOWNLOAD_LOCK: &str = ".download.lock";

/// What takes a continent dir's DOWNLOAD_LOCK.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockFor {
    /// A download, which writes the dir: an exclusive lock, on the file open to write or, where
    /// this process can't write it, open to read (take_download_lock).
    Download,
    /// A load, which reads the dir: a shared lock. Where the file isn't there and can't be made,
    /// as the dir is read-only or not writable to this process, the dir loads without a lock: no
    /// download of this process can write there either. A download of another process that can
    /// may run meanwhile, which the load tells by the dir's stamp and ledger (DirMarks).
    Load,
}

/// Takes the download lock of the continent dir `dir` (DOWNLOAD_LOCK) for `what`, the file made
/// where it isn't there, waiting while another holds it where they can't share it. None where
/// no lock is taken: on a file system without locks, on platforms other than unix, and for a load
/// where the file isn't there and can't be made (cant_write). A download opens the file to read
/// and write; where it can't write it (a lock file a load of another user's server made, of mode
/// 0644 under umask 022, or one on a read-only mount), it opens it to read and takes the lock on
/// that, which a local file system allows. One whose file system takes an exclusive lock only on
/// a file open to write (NFS) then fails, saying so. The lock is held until the file returned is
/// dropped.
#[cfg(unix)]
pub fn take_download_lock(dir: &std::path::Path, what: LockFor) -> std::io::Result<Option<File>> {
    let path = dir.join(DOWNLOAD_LOCK);
    let (file, read_only) = match what {
        LockFor::Download => match File::options().read(true).write(true).create(true).truncate(false).open(&path) {
            Ok(file) => (file, false),
            Err(e) if cant_write(&e) => match File::open(&path) {
                Ok(file) => (file, true),
                Err(n) if n.kind() == std::io::ErrorKind::NotFound => return Err(e),
                Err(n) => return Err(n),
            },
            Err(e) => return Err(e),
        },
        LockFor::Load => match open_or_make(&path) {
            Ok(file) => (file, true),
            // Another process may have made it meanwhile.
            Err(e) if cant_write(&e) => match File::open(&path) {
                Ok(file) => (file, true),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e),
            },
            Err(e) => return Err(e),
        },
    };
    loop {
        let taken = match what {
            LockFor::Download => lock_exclusive(&file),
            LockFor::Load => file.lock_shared(),
        };
        match taken {
            Ok(()) => return Ok(Some(file)),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) if transport::no_locks_here(&e) => return Ok(None),
            // Where flock(2) is a lock of a byte range (NFS), an exclusive one needs the file
            // open to write.
            Err(e) if what == LockFor::Download && read_only && e.raw_os_error() == Some(libc::EBADF) => {
                return Err(std::io::Error::new(e.kind(), format!(
                    "the lock file isn't writable to this server, and this file system takes an exclusive lock only on a file open to write ({}): make it writable to every server that downloads into the dir",
                    e)));
            }
            Err(e) => return Err(e),
        }
    }
}

/// Takes an exclusive flock on `file`, waiting while another holds a lock on it.
#[cfg(unix)]
fn lock_exclusive(file: &File) -> std::io::Result<()> {
    #[cfg(test)]
    if let Some(errno) = DOWNLOAD_LOCK_FAULT.take() {
        return Err(std::io::Error::from_raw_os_error(errno));
    }
    file.lock()
}

#[cfg(not(unix))]
pub fn take_download_lock(_dir: &std::path::Path, _what: LockFor) -> std::io::Result<Option<File>> {
    Ok(None)
}

/// The file at `path`, opened to read, made empty where it isn't there: open(2) with O_CREAT and
/// without O_TRUNC. Not opened to write, which a lock doesn't need: a lock file another user's
/// server made, that this process can't write, still opens.
#[cfg(unix)]
fn open_or_make(path: &std::path::Path) -> std::io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    #[cfg(test)]
    if let Some(errno) = LOCK_FILE_UNMADE.get() {
        if !path.exists() {
            return Err(std::io::Error::from_raw_os_error(errno));
        }
    }
    File::options().read(true).custom_flags(libc::O_CREAT).open(path)
}

/// Whether `e`, an error making a file or opening one to write, says the file system is
/// read-only (EROFS) or the dir or the file isn't writable to this process (EACCES, EPERM).
#[cfg(unix)]
fn cant_write(e: &std::io::Error) -> bool {
    matches!(e.raw_os_error(), Some(libc::EROFS | libc::EACCES | libc::EPERM))
}

/// A file's device and FileStamp (inode, size, modification time): which
/// file is at a path, one renamed into place over it another.
pub type FileMark = (u64, FileStamp);

/// The mark of the file at `path`, None where none is there.
pub fn file_mark(path: &std::path::Path) -> std::io::Result<Option<FileMark>> {
    match std::fs::metadata(path) {
        #[cfg(unix)]
        Ok(m) => Ok(Some((std::os::unix::fs::MetadataExt::dev(&m), FileStamp::of(&m)))),
        #[cfg(not(unix))]
        Ok(m) => Ok(Some((0, FileStamp::of(&m)))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// A continent dir's BUILD_STAMP and FILE_LEDGER as a load finds them: each
/// None where it isn't there, else its FileMark. A download that changes a
/// file in the dir changes one of them first: it makes the ledger where the
/// dir has none, and removes the stamp. It appends to the ledger or renames
/// a new one over it, and writes the stamp only under a temp name renamed
/// into place, a new inode. So a load that finds them the same before it
/// checks the dir and once it loaded it knows no download changed the dir
/// meanwhile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DirMarks {
    stamp: Option<FileMark>,
    ledger: Option<FileMark>,
}

impl DirMarks {
    /// Those of the continent dir `dir`, or why either can't be checked.
    fn of(dir: &str) -> Result<DirMarks, String> {
        let mark = |name: &str| {
            let path = format!("{}/{}", dir, name);
            file_mark(std::path::Path::new(&path)).map_err(|e| format!("can't check {}: {}", path, e))
        };
        Ok(DirMarks { stamp: mark(BUILD_STAMP)?, ledger: mark(FILE_LEDGER)? })
    }

    /// Why a dir of these is left unloaded, if it is: it holds FILE_LEDGER
    /// but not BUILD_STAMP, a download that did not finish. A dir holding
    /// neither was laid out by other means (by hand, by the test portal) and
    /// loads.
    fn unfinished_download(&self) -> Option<String> {
        match (self.stamp, self.ledger) {
            (None, Some(_)) => Some(format!("it holds {} but no {}: a download that did not finish", FILE_LEDGER, BUILD_STAMP)),
            _ => None,
        }
    }
}

#[cfg(test)]
thread_local! {
    /// Set by a test: told each continent dir load_continent on this thread
    /// loads, once its stamp is checked, before Index::load.
    static BEFORE_INDEX_LOAD: std::cell::RefCell<Option<Box<dyn FnMut(&str)>>> = const { std::cell::RefCell::new(None) };
    /// Set by a test: the errno with which a load on this thread fails to
    /// make a dir's DOWNLOAD_LOCK where it isn't there, as it would in a dir
    /// that is read-only or not writable to it.
    static LOCK_FILE_UNMADE: std::cell::Cell<Option<i32>> = const { std::cell::Cell::new(None) };
    /// Set by a test: the errno with which the next exclusive lock a download
    /// on this thread takes on a dir's DOWNLOAD_LOCK fails, once.
    static DOWNLOAD_LOCK_FAULT: std::cell::Cell<Option<i32>> = const { std::cell::Cell::new(None) };
}

/// The index in the continent dir `dir`, for a server starting
/// (MultiIndex::load) and for a hot-add once a download is in
/// (MultiIndex::install_region). A download into the dir (another
/// server's, on the same data dir) may be removing its stamp and replacing
/// its files for another selection: the dir is checked and loaded under a
/// shared download lock (LockFor::Load), taken before anything in it is
/// read and held until Index::load returned, which waits for that download
/// to end and keeps the next from starting until the load returned. The
/// lock file is made where it isn't there, so that the first download into
/// a dir waits for a load too. Where no lock is taken (the lock file can't
/// be made, the file system has no locks) such a download may run all the
/// same: the dir's stamp and ledger (DirMarks) are found before it is
/// checked and again once Index::load returned, and a load that finds them
/// changed is dropped. That holds where the lock is taken too: a download
/// on a file system whose locks don't reach other machines may not see it,
/// and the check costs a few stats. The files stay mapped after. With the
/// index, the text of the stamp the dir held as it loaded, which names the
/// bucket and the selection of the files it loaded (None for a dir laid out
/// by other means).
/// None for a dir that isn't there or holds no admin_cells.bin. An error,
/// why the dir doesn't load, for a lock that can't be taken, a download that
/// broke off (unfinished_download): it can leave files of a build or
/// selection beside those of the one before, which Index::load can't tell
/// apart; for a dir Index::load refuses; and for one a download changed
/// while it loaded.
fn load_continent(dir: &str, street_cell_level: u64, admin_cell_level: u64, search_distance: f64) -> Result<Option<(Index, Option<String>)>, String> {
    // A dir that isn't there holds nothing to load, and gets no lock file.
    if !std::path::Path::new(dir).is_dir() {
        return Ok(None);
    }
    let _lock = take_download_lock(std::path::Path::new(dir), LockFor::Load)
        .map_err(|e| format!("can't lock {}/{}: {}", dir, DOWNLOAD_LOCK, e))?;
    if !std::path::Path::new(&format!("{}/admin_cells.bin", dir)).exists() {
        return Ok(None);
    }
    let marks = DirMarks::of(dir)?;
    if let Some(why) = marks.unfinished_download() {
        return Err(why);
    }
    // Read before the dir is, under its lock: a stamp a download put in
    // meanwhile changes the marks, which the check below finds.
    let stamp_path = format!("{}/{}", dir, BUILD_STAMP);
    let stamp = match std::fs::read_to_string(&stamp_path) {
        Ok(text) => Some(text),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(format!("can't read {}: {}", stamp_path, e)),
    };
    #[cfg(test)]
    BEFORE_INDEX_LOAD.with_borrow_mut(|told| {
        if let Some(tell) = told {
            tell(dir);
        }
    });
    let index = Index::load(dir, street_cell_level, admin_cell_level, search_distance)?;
    // Dropped here, unmapped, where a download changed the dir meanwhile:
    // its files may be of two selections.
    if DirMarks::of(dir)? != marks {
        return Err(format!("a download into it ran while it loaded ({} or {} changed): it may mix two selections' files", BUILD_STAMP, FILE_LEDGER));
    }
    Ok(Some((index, stamp)))
}

pub struct ContinentSlot {
    pub name: &'static str,
    /// Boundary that routes queries here; `None` for the flat "world" slot.
    pub continent: Option<&'static Continent>,
    /// Atomically-swappable index slot. `None` until the continent's
    /// files are loaded (or when load is asked but the dir is empty).
    /// Hot-add via `MultiIndex::install_region` swaps a fresh Arc in
    /// without locking out concurrent queries.
    pub index: arc_swap::ArcSwapOption<Index>,
    /// The text of the stamp (BUILD_STAMP) its dir held as MultiIndex::load
    /// loaded it there, which names the bucket and the selection of its
    /// files: None where it loaded none, and for a dir laid out by other
    /// means. An install later does not change it.
    pub loaded_stamp: Option<String>,
}

pub struct MultiIndex {
    pub continents: Vec<ContinentSlot>,
    /// Captured at construction so `install_region` knows where to
    /// look for newly-downloaded files.
    pub root_dir: String,
    pub street_cell_level: u64,
    pub admin_cell_level: u64,
    pub search_distance: f64,
}

impl MultiIndex {
    /// Try per-continent subdirs first (`<root>/africa/`, `<root>/asia/`, …).
    /// Fall back to a single-region "world" Index if `<root>` itself looks
    /// like a flat layout. Either way, returns a MultiIndex whose query()
    /// behaves the same — single-region setups just see one slot.
    pub fn load(root: &str, street_cell_level: u64, admin_cell_level: u64, search_distance: f64) -> Result<Self, String> {
        // Refused before any continent, each of which Index::load would refuse.
        if let Some(why) = geo_reach_refused(search_distance, street_cell_level) {
            return Err(why);
        }
        // Flat-layout fast path: keep existing single-region deployments
        // working without changes. The "world" slot covers the globe.
        if std::path::Path::new(&format!("{}/admin_cells.bin", root)).exists() {
            let idx = Index::load(root, street_cell_level, admin_cell_level, search_distance)?;
            return Ok(MultiIndex {
                continents: vec![ContinentSlot {
                    name: "world",
                    continent: None,
                    index: arc_swap::ArcSwapOption::from(Some(Arc::new(idx))),
                    loaded_stamp: None,
                }],
                root_dir: root.to_string(),
                street_cell_level, admin_cell_level, search_distance,
            });
        }

        // Per-continent layout: walk the canonical list, load whichever
        // dirs exist. Missing continents stay as None — they'll either
        // remain empty (caller doesn't care) or get downloaded later.
        let mut slots = Vec::with_capacity(continents().len());
        for continent in continents() {
            let dir = format!("{}/{}", root, continent.name);
            // A dir a download broke off in, or that can't load, must not
            // take down the continents that did load; the slot stays empty
            // so the downloader can complete it later.
            let (arc_opt, loaded_stamp) = match load_continent(&dir, street_cell_level, admin_cell_level, search_distance) {
                Ok(Some((idx, stamp))) => (Some(Arc::new(idx)), stamp),
                Ok(None) => (None, None),
                Err(why) => {
                    eprintln!("skipping continent {}: {}", continent.name, why);
                    (None, None)
                }
            };
            slots.push(ContinentSlot {
                name: continent.name,
                continent: Some(continent),
                index: arc_swap::ArcSwapOption::from(arc_opt),
                loaded_stamp,
            });
        }
        Ok(MultiIndex {
            continents: slots,
            root_dir: root.to_string(),
            street_cell_level, admin_cell_level, search_distance,
        })
    }

    pub fn loaded_count(&self) -> usize {
        self.continents.iter().filter(|c| c.index.load().is_some()).count()
    }

    /// The credit of the data the loaded datasets hold (Index::credits, as each loaded), each
    /// source once, in the order the datasets list them, of every notice any of them gives it
    /// (a dataset of GB centroids owes GeoNames a notice others don't); none where none is
    /// loaded.
    pub fn credits(&self) -> Vec<serde_json::Value> {
        let mut out: Vec<serde_json::Value> = Vec::new();
        for index in self.continents.iter().filter_map(|c| c.index.load_full()) {
            for entry in &index.credits {
                let Some(seen) = out.iter_mut().find(|e| e["id"] == entry["id"]) else {
                    out.push(entry.clone());
                    continue;
                };
                let Some(have) = seen.get_mut("notices").and_then(serde_json::Value::as_array_mut) else { continue };
                for notice in entry["notices"].as_array().into_iter().flatten() {
                    if !have.contains(notice) {
                        have.push(notice.clone());
                    }
                }
            }
        }
        out
    }

    /// A point off the globe is in no slot: S2 would wrap it onto a cell of the globe.
    fn covers(&self, slot: &ContinentSlot, lat: f64, lng: f64) -> bool {
        coords_in_range(lat, lng) && slot.continent.map_or(true, |c| c.covers_query(lat, lng, self.admin_cell_level))
    }

    /// Snapshot of loaded indices that cover the point. Returns Arc
    /// clones so callers can hold them across an await without keeping
    /// the MultiIndex borrowed. Most points hit one continent; where
    /// boundaries meet or overlap (Sinai, the Urals) two can.
    pub fn loaded_matches(&self, lat: f64, lng: f64) -> Vec<Arc<Index>> {
        self.continents.iter()
            .filter(|c| self.covers(c, lat, lng))
            .filter_map(|c| c.index.load_full())
            .collect()
    }

    /// Names of covering continents that aren't loaded. The downloader
    /// uses this to know what to fetch when a request lands on an
    /// unloaded region.
    pub fn unloaded_matches(&self, lat: f64, lng: f64) -> Vec<&'static str> {
        self.continents.iter()
            .filter(|c| c.index.load().is_none() && self.covers(c, lat, lng))
            .map(|c| c.name)
            .collect()
    }

    /// Hot-add a freshly-downloaded continent. Atomically swaps in a
    /// new Arc<Index>; concurrent readers either still see the old
    /// slot or the new one — never a torn state.
    ///
    /// The dir loads as it does when a server starts (load_continent): a
    /// download into it of another server on the data dir may have begun
    /// once this server's download let go of the dir, and the dir is read
    /// once that one ended, and loaded only where it finished. Else, or
    /// where the dir holds no admin_cells.bin, an error, the slot as it was.
    /// It blocks as long as that download runs: an async caller runs it on a
    /// thread of its own (spawn_blocking), not on one of the runtime's.
    /// Returns the text of the stamp the dir held as it loaded, which names
    /// the bucket and the selection of the files it loaded.
    pub fn install_region(&self, name: &str) -> Result<Option<String>, String> {
        let slot = self.continents.iter().find(|c| c.name == name)
            .ok_or_else(|| format!("unknown continent: {}", name))?;
        let dir = format!("{}/{}", self.root_dir, name);
        let (idx, stamp) = load_continent(&dir, self.street_cell_level, self.admin_cell_level, self.search_distance)?
            .ok_or_else(|| format!("{}/admin_cells.bin isn't there", dir))?;
        slot.index.store(Some(Arc::new(idx)));
        Ok(stamp)
    }

    /// Reverse-geocode dispatch. Picks the best result among matching
    /// loaded continents — "best" = the one whose Address chain has the
    /// most populated fields (deeper admin chain wins), the first of
    /// those that tie. On the typical non-boundary case there's only one
    /// match and this is just a pass-through.
    ///
    /// The water doesn't count towards the pick, so it never moves an
    /// address to another continent. It is the one of all the matching
    /// continents' that answers first in a water dir's order (water.rs),
    /// the picked answer's where they tie: a continent's water dirs hold
    /// only the water its region takes in, and continents downloaded on
    /// other days, or of other water tiers, can hold other water at the
    /// point.
    ///
    /// The answers borrow their strings from the continents' indexes,
    /// whose Arcs are held here until the chosen one is serialized.
    pub fn query_json(&self, lat: f64, lng: f64) -> String {
        let arcs = self.loaded_matches(lat, lng);
        let mut answers: Vec<Address<'_>> = arcs.iter().map(|arc| arc.query(lat, lng)).collect();
        let mut best: Option<(i32, usize)> = None;
        for (i, r) in answers.iter().enumerate() {
            let s = address_score(r);
            if best.map_or(true, |(prev, _)| s > prev) {
                best = Some((s, i));
            }
        }
        let Some((_, chosen)) = best else { return "{}".to_string() };
        let mut answer = answers.remove(chosen);
        answer.water = std::iter::once(answer.water.take())
            .chain(answers.into_iter().map(|r| r.water))
            .flatten()
            .min_by(WaterDetail::order);
        serde_json::to_string(&answer).unwrap_or_default()
    }
}

#[cfg(test)]
mod multi_index_tests {
    use super::*;
    use crate::scratch_dir::ScratchDir;

    #[test]
    fn interpolate_house_number_ranges() {
        // ascending even range, midpoint
        assert_eq!(interpolate_house_number(2, 10, 1, 0.5), 6);
        // descending range: negative offsets must not saturate to start
        assert_eq!(interpolate_house_number(10, 2, 1, 0.5), 6);
        assert_eq!(interpolate_house_number(10, 2, 1, 1.0), 2);
        // odd step keeps parity of start
        assert_eq!(interpolate_house_number(1, 9, 2, 0.55), 5);
        // clamped to range ends
        assert_eq!(interpolate_house_number(2, 4, 1, 1.4), 4);
    }

    #[test]
    fn interp_position_measures_along_the_line() {
        let line = [NodeCoord { lat: 0.0, lng: 0.0 }, NodeCoord { lat: 0.0, lng: 0.002 }];
        let origin = |lat: f64, lng: f64| GeoOrigin::new(lat, lng, geo_cover(DEFAULT_SEARCH_DISTANCE / 6_371_000.0, DEFAULT_STREET_CELL_LEVEL));
        let (d, t) = interp_position(&line, &origin(0.0001, 0.0005)).unwrap();
        assert!((t - 0.25).abs() < 1e-6, "t={}", t);
        assert!((d.sqrt().to_degrees() - 0.0001).abs() < 1e-9);

        // Past the end clamps to the last node.
        let (_, t_end) = interp_position(&line, &origin(0.0, 0.01)).unwrap();
        assert!((t_end - 1.0).abs() < 1e-9);

        // Degenerate lines have no position.
        assert!(interp_position(&line[..1], &origin(0.0, 0.0)).is_none());
        assert!(interp_position(&[line[0], line[0]], &origin(0.0, 0.0)).is_none());
    }

    #[test]
    fn street_tokens_overlap_matches_tiger_names() {
        // TIGER abbreviated vs OSM full name: shared significant token.
        assert!(street_tokens_overlap("W 1st St", "West 1st Street"));
        assert!(street_tokens_overlap("Boyt Road", "Boyt Road"));
        assert!(street_tokens_overlap("RICE ROAD", "Rice Rd"));
        // Different streets: no shared token of >= 2 chars.
        assert!(!street_tokens_overlap("Main Street", "Oak Avenue"));
        // Unnamed / empty never matches.
        assert!(!street_tokens_overlap("", "Main Street"));
        // Punctuation-only separators normalise away.
        assert!(street_tokens_overlap("St. Mary's Road", "Saint Marys Road"));
    }

    fn routed(lat: f64, lng: f64) -> Vec<&'static str> {
        continents().iter()
            .filter(|c| c.covers_query(lat, lng, DEFAULT_ADMIN_CELL_LEVEL))
            .map(|c| c.name)
            .collect()
    }

    // Expected continents follow the builder's .poly membership, which
    // decides which continent directory holds the data.
    #[test]
    fn interior_points_route_to_one_continent() {
        for (lat, lng, want) in [
            (48.85, 2.35, "europe"),
            (6.45, 3.40, "africa"),
            (-33.86, 151.21, "oceania"),
            (40.71, -74.0, "north-america"),
            (-89.0, 0.0, "antarctica"),
        ] {
            assert_eq!(routed(lat, lng), vec![want], "({}, {})", lat, lng);
        }
    }

    #[test]
    fn island_territories_route_to_their_continent() {
        // All outside the old hand-written bboxes, so they got `{}`.
        for (lat, lng, want) in [
            (37.74, -25.67, "europe"),          // Azores
            (78.22, 15.65, "europe"),           // Svalbard
            (-17.54, -149.57, "oceania"),       // Tahiti
            (-13.83, -171.76, "oceania"),       // Samoa
            (-21.14, -175.2, "oceania"),        // Tonga
            (51.88, -176.66, "north-america"),  // Adak
            (-0.74, -90.31, "south-america"),   // Galápagos
            (-21.1, 55.5, "africa"),            // Réunion
            (-20.16, 57.5, "africa"),           // Mauritius
        ] {
            assert_eq!(routed(lat, lng), vec![want], "({}, {})", lat, lng);
        }
    }

    #[test]
    fn overlapping_polygons_route_to_both() {
        // The africa and asia polygons both cover the Sinai.
        let got = routed(28.5, 33.6);
        assert!(got.contains(&"africa") && got.contains(&"asia"), "{:?}", got);
    }

    #[test]
    fn multi_ring_polygons_cover_both_sides_of_the_antimeridian() {
        // asia and oceania are split into two rings at ±180.
        assert!(routed(64.73, 177.5).contains(&"asia"));   // Anadyr
        assert!(routed(-43.95, -176.56).contains(&"oceania")); // Chatham Is.
    }

    #[test]
    fn load_skips_unloadable_continent() {
        let root = ScratchDir::new();
        let europe = root.path().join("europe");
        std::fs::create_dir_all(&europe).unwrap();
        std::fs::write(europe.join("admin_cells.bin"), b"").unwrap();

        let mi = MultiIndex::load(root.path().to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE);

        let mi = mi.expect("a partial continent dir must not fail the whole load");
        assert_eq!(mi.loaded_count(), 0);
        assert_eq!(mi.unloaded_matches(48.85, 2.35), vec!["europe"]);
    }

    #[test]
    fn address_score_prefers_populated() {
        let empty = Address::default();
        let mut populated = Address::default();
        populated.address.country = Some("France".into());
        populated.address.city = Some("Paris".into());
        assert!(address_score(&populated) > address_score(&empty));
    }
}

#[cfg(test)]
mod query_tests {
    use super::*;

    /// (lat, lng, name, place type: 1 town, 2 village, 4 hamlet).
    type Node<'a> = (f32, f32, &'a str, u8);
    /// (name, admin_level, [south, west, north, east]), no country code.
    type Boundary<'a> = (&'a str, u8, [f32; 4]);

    // N515065315 place=town and N560153667 place=hamlet.
    const MCMURDO: Node<'static> = (-77.8463, 166.668, "McMurdo Station", 1);
    const SCOTT_BASE: Node<'static> = (-77.8494, 166.768, "Scott Base", 4);
    // R12823139, admin_level 5, outside every country, and a village inside.
    const SHOALS: Boundary<'static> = ("Beting Patinggi Ali Shoals", 5, [5.0, 112.6, 5.1, 112.7]);
    const SHOALS_VILLAGE: Node<'static> = (5.05, 112.65, "Shoals Village", 2);

    /// The place files of `nodes` as the builder writes them, at the default admin cell level.
    fn place_fixture(nodes: &[PlaceNode]) -> PlaceFiles {
        let (f, _) = place_files::encode(nodes, DEFAULT_ADMIN_CELL_LEVEL);
        PlaceFiles::open(FileBytes::Owned(f.cells), FileBytes::Owned(f.nodes), FileBytes::Owned(f.hidden)).unwrap()
    }


    /// An index of boundaries (in the admin cell of their centre) and place
    /// nodes only, without streets, POIs or countries, like Antarctica's.
    fn index(boundaries: &[Boundary<'_>], nodes: &[Node<'_>]) -> Index {
        let cell = |lat: f32, lng: f32| cell_id_at_level(lat as f64, lng as f64, DEFAULT_ADMIN_CELL_LEVEL);
        let mut strings = Vec::new();
        let mut name = |s: &str| {
            let id = strings.len() as u32;
            strings.extend_from_slice(s.as_bytes());
            strings.push(0);
            id
        };
        let (mut arcs, mut admin_pairs) = (arc_fixture::ArcFixture::new(), Vec::new());
        for (id, &(boundary, admin_level, [s, w, n, e])) in boundaries.iter().enumerate() {
            let at = |lat: f32, lng: f32| arc_fixture::e7(lat as f64, lng as f64);
            let ring = [at(s, w), at(s, e), at(n, e), at(n, w), at(s, w)];
            arcs.ring_polygon(arc_fixture::record(name(boundary), admin_level, (n - s) * (e - w)), 0, &[ring.to_vec()]);
            admin_pairs.push((cell((s + n) / 2.0, (w + e) / 2.0), id as u32));
        }
        let [(_, polygons), (_, rings), (_, arc_index), (_, arc_vertices)] = arcs.files();
        let records: Vec<PlaceNode> = nodes.iter()
            .map(|&(lat, lng, node, place_type)| PlaceNode { lat, lng, name_id: name(node), place_type, _pad1: 0, _pad2: 0, parent_poly_id: NO_DATA })
            .collect();
        let admin_index = cell_pairs::from_pairs(admin_pairs, cell_pairs::ADMIN);
        let end = strings.len() as u32;
        Index {
            geo_index: GeoIndex::none(),
            streets: None,
            addrs: None,
            interps: None,
            interp_postcodes: None,
            admin_index,
            admin_polygons: FileBytes::Owned(polygons),
            admin_geometry: AdminGeometry::new(FileBytes::Owned(rings), FileBytes::Owned(arc_index), FileBytes::Owned(arc_vertices)),
            poi_index: None,
            poi_records: None,
            poi_vertices: None,
            poi_meta: PoiMeta::from_json("{}"),
            places: Some(place_fixture(&records)),
            way_parents: None,
            admin_parents: None,
            addr_postcodes: None,
            addr_cities: None,
            way_postcodes: None,
            postcode_centroids: None,
            postcode_grid: None,
            admin_config: AdminLevelConfig::load(),
            strings: StringPool::from_parts([Some(strings::Tier::of_pool(&strings)), None, None, None, None], [0, end, end, end, end, end]),
            water: None,
            street_cell_level: DEFAULT_STREET_CELL_LEVEL,
            admin_cell_level: DEFAULT_ADMIN_CELL_LEVEL,
            max_distance_sq: (DEFAULT_SEARCH_DISTANCE / 6_371_000.0).powi(2),
            linked_names: Default::default(),
            credits: Vec::new(),
        }
    }

    fn answer(idx: &Index, lat: f64, lng: f64) -> serde_json::Value {
        serde_json::to_value(idx.query(lat, lng)).unwrap()
    }

    #[test]
    fn place_nodes_answer_where_no_boundary_holds_the_point() {
        // Nominatim at Scott Base: hamlet=Scott Base, town=McMurdo Station.
        let idx = index(&[], &[MCMURDO, SCOTT_BASE]);

        let got = answer(&idx, -77.8494, 166.768);

        assert_eq!(got["address"], serde_json::json!({"town": "McMurdo Station", "hamlet": "Scott Base"}));
    }

    #[test]
    fn a_hidden_place_node_answers_like_none() {
        // A node a boundary claims keeps its name for search, in place_hidden.bin, which reverse
        // geocoding doesn't read.
        let mut idx = index(&[], &[MCMURDO, SCOTT_BASE]);
        let mut nodes: Vec<PlaceNode> = (0..2).map(|id| idx.place_node_record(id).unwrap()).collect();
        let mcmurdo = nodes.iter().position(|pn| pn.place_type == MCMURDO.3).unwrap();
        nodes[mcmurdo]._pad1 = PLACE_FLAG_HIDDEN;
        let hidden = { idx.places = Some(place_fixture(&nodes)); idx };

        let got = answer(&hidden, -77.8494, 166.768);

        assert_eq!(got["address"], serde_json::json!({"hamlet": "Scott Base"}));
        assert_eq!(got, answer(&index(&[], &[SCOTT_BASE]), -77.8494, 166.768));
    }

    #[test]
    fn a_boundary_without_a_country_city_or_state_has_no_answer() {
        let idx = index(&[SHOALS], &[SHOALS_VILLAGE]);

        assert_eq!(answer(&idx, 5.05, 112.65), serde_json::json!({"licence": OSM_LICENCE, "address": {}}));
    }

    #[test]
    fn a_point_nothing_holds_has_no_answer() {
        let idx = index(&[SHOALS], &[MCMURDO]);

        for (lat, lng) in [(-82.0, 60.0), (5.2, 112.65)] {
            assert_eq!(answer(&idx, lat, lng), serde_json::json!({"licence": OSM_LICENCE, "address": {}}), "({}, {})", lat, lng);
        }
    }

    #[test]
    fn a_place_node_gate_without_rings_accepts_every_node() {
        let idx = index(&[SHOALS, ("Elsewhere", 8, [0.0, 0.0, 0.1, 0.1])], &[SHOALS_VILLAGE]);
        let elsewhere = idx.admin_polygon(1).unwrap();
        let village = |gate: AdminPolygon| {
            let gates = CurrentBoundaries::walk([(CurrentBoundaries::CITY, gate)], |_, _| true);
            idx.find_places(PlaceFeature::Point(5.05, 112.65), &gates, 0, None).village
        };

        assert_eq!(village(elsewhere), None);
        assert_eq!(village(AdminPolygon { ring_bytes: 0, ..elsewhere }).as_deref(), Some("Shoals Village"));
    }

    // The water field is an extension of the answer: last, and absent where no water holds the
    // point, so that the answer there is the one of a dataset without water, byte for byte; where
    // water holds it, the rest of the answer is still that one.
    #[test]
    fn water_comes_last_and_changes_no_other_field() {
        use crate::water::fixture::{WaterFixture, LAKE};
        // A state (the fixture's boundaries have no country code), listed in the cell of its
        // centre, where the point is.
        let (testland, (lat, lng)) = (("Testland", 4, [0.0, 0.0, 2.0, 2.0]), (1.0, 1.0));
        let with = |water: Option<WaterLayer>| {
            let mut idx = index(&[testland], &[]);
            idx.water = water;
            serde_json::to_string(&idx.query(lat, lng)).unwrap()
        };
        let lake = |span| Some(WaterFixture::new().area("Testsee", LAKE, span, &[]).layer());

        let dry = with(None);
        assert_eq!(serde_json::from_str::<serde_json::Value>(&dry).unwrap()["address"], serde_json::json!({"state": "Testland"}));
        assert_eq!(with(lake((0.5, 0.5, 0.9, 0.9))), dry);
        let wet = format!("{},\"water\":{{\"name\":\"Testsee\",\"class\":\"water\",\"type\":\"lake\"}}}}", dry.strip_suffix('}').unwrap());
        assert_eq!(with(lake((0.9, 0.9, 1.1, 1.1))), wet);
    }

    // A point no boundary, street or place node anchors (open sea) has an empty address, and
    // keeps its water; off the globe there is none.
    #[test]
    fn an_open_sea_point_keeps_its_water() {
        use crate::water::fixture::{WaterFixture, SEA};
        let mut idx = index(&[SHOALS], &[]);
        idx.water = Some(WaterFixture::new().area("South China Sea", SEA, (4.0, 111.0, 6.0, 114.0), &[]).layer());

        let sea = serde_json::json!({"licence": OSM_LICENCE, "address": {}, "water": {"name": "South China Sea", "class": "place", "type": "sea"}});
        assert_eq!(answer(&idx, 5.5, 113.5), sea);
        assert_eq!(answer(&idx, 5.05, 112.65), sea);
        assert_eq!(answer(&idx, 7.0, 113.5), serde_json::json!({"licence": OSM_LICENCE, "address": {}}));
        assert_eq!(answer(&idx, 5.5, 473.5), serde_json::json!({"licence": OSM_LICENCE, "address": {}}));
    }

    /// A dispatcher over the continents named (of continents()), each with the index given, in
    /// the order given.
    fn multi(slots: Vec<(&'static str, Index)>) -> MultiIndex {
        let continents = slots.into_iter()
            .map(|(name, index)| ContinentSlot {
                name,
                continent: continents().iter().find(|c| c.name == name),
                index: arc_swap::ArcSwapOption::from(Some(Arc::new(index))),
                loaded_stamp: None,
            })
            .collect();
        MultiIndex {
            continents, root_dir: String::new(),
            street_cell_level: DEFAULT_STREET_CELL_LEVEL, admin_cell_level: DEFAULT_ADMIN_CELL_LEVEL, search_distance: DEFAULT_SEARCH_DISTANCE,
        }
    }

    // Where continents overlap (the Sinai: africa and asia), the answer is the continent's with the
    // fuller address, as without water, the first of those that tie; its water is the one of all
    // theirs that answers first in a water dir's order (water.rs), the address's continent's or
    // another's: continents downloaded on other days, or of other water tiers, can hold other
    // water at the point.
    #[test]
    fn the_water_of_overlapping_continents_answers_in_a_water_dirs_order() {
        use crate::water::fixture::{WaterFixture, BAY, SEA};
        // South Sinai, a state, is listed in the cell of its centre, where the point is; the Gulf
        // of Suez lies in the Red Sea around it.
        let (lat, lng) = (28.5, 33.6);
        let egypt = ("South Sinai", 4, [27.5, 32.6, 29.5, 34.6]);
        let (gulf, sea) = ((28.0, 33.0, 29.0, 34.0), (27.0, 32.0, 30.0, 35.0));
        let water = |name: &str, code: u16, span| Some(WaterFixture::new().area(name, code, span, &[]).layer());
        let with = |mut idx: Index, w: Option<WaterLayer>| {
            idx.water = w;
            idx
        };
        let json = |slots: Vec<(&'static str, Index)>| serde_json::from_str::<serde_json::Value>(&multi(slots).query_json(lat, lng)).unwrap();
        let named = |name: &str, class: &str, r#type: &str| serde_json::json!({"name": name, "class": class, "type": r#type});
        let both_orders = |africa: &dyn Fn() -> Index, asia: &dyn Fn() -> Index| {
            [json(vec![("africa", africa()), ("asia", asia())]), json(vec![("asia", asia()), ("africa", africa())])]
        };

        // Africa's answer, Asia's water, whichever comes first.
        let mut want = serde_json::to_value(index(&[egypt], &[]).query(lat, lng)).unwrap();
        assert_eq!(want["address"], serde_json::json!({"state": "South Sinai"}));
        want["water"] = named("Gulf of Suez", "natural", "bay");
        assert_eq!(both_orders(&|| index(&[egypt], &[]), &|| with(index(&[], &[]), water("Gulf of Suez", BAY, gulf))), [want.clone(), want.clone()]);
        // The smaller body, Africa's or Asia's, beside Africa's address.
        let gulf_in_asia = both_orders(&|| with(index(&[egypt], &[]), water("Red Sea", SEA, sea)), &|| with(index(&[], &[]), water("Gulf of Suez", BAY, gulf)));
        assert_eq!(gulf_in_asia, [want.clone(), want.clone()]);
        let gulf_in_africa = both_orders(&|| with(index(&[egypt], &[]), water("Gulf of Suez", BAY, gulf)), &|| with(index(&[], &[]), water("Red Sea", SEA, sea)));
        assert_eq!(gulf_in_africa, [want.clone(), want.clone()]);
        // Bodies of one area, rank and type code: the smaller name, Africa's or Asia's.
        for (africa, asia) in [("Gulf of Suez", "Gulf of Aqaba"), ("Gulf of Aqaba", "Gulf of Suez")] {
            let got = both_orders(&|| with(index(&[egypt], &[]), water(africa, BAY, gulf)), &|| with(index(&[], &[]), water(asia, BAY, gulf)));
            assert_eq!(got.map(|g| g["water"].clone()), [named("Gulf of Aqaba", "natural", "bay"), named("Gulf of Aqaba", "natural", "bay")]);
        }
        // Water counts for nothing in the pick: of two empty addresses the first answers, with
        // the water of the second, and so does the first of two that tie, a state each.
        let got = json(vec![("africa", index(&[], &[])), ("asia", with(index(&[], &[]), water("Gulf of Suez", BAY, gulf)))]);
        assert_eq!(got, serde_json::json!({"licence": OSM_LICENCE, "address": {}, "water": named("Gulf of Suez", "natural", "bay")}));
        assert_eq!(json(vec![("africa", index(&[], &[])), ("asia", index(&[], &[]))]), serde_json::json!({"licence": OSM_LICENCE, "address": {}}));
        let north = ("North Sinai", 4, [27.5, 32.6, 29.5, 34.6]);
        for (first, second) in [(egypt, north), (north, egypt)] {
            let mut want = serde_json::to_value(index(&[first], &[]).query(lat, lng)).unwrap();
            want["water"] = named("Gulf of Suez", "natural", "bay");
            let got = json(vec![("africa", index(&[first], &[])), ("asia", with(index(&[second], &[]), water("Gulf of Suez", BAY, gulf)))]);
            assert_eq!(got, want, "{}", first.0);
        }
    }

    /// (metres north of `origin()`, metres east, name or None for a tombstone, category,
    /// importance) of a point POI.
    type Poi<'a> = (f64, f64, Option<&'a str>, u8, u8);
    /// (metres north of `origin()`, metres east, postcode, country code or [0, 0]) of a postcode
    /// centroid.
    type Centroid<'a> = (f64, f64, &'a str, [u8; 2]);

    /// The centre of an admin cell in Paris.
    fn origin() -> (f64, f64) {
        let centre = LatLng::from(CellID(cell_id_at_level(48.8566, 2.3522, DEFAULT_ADMIN_CELL_LEVEL)));
        (centre.lat.deg(), centre.lng.deg())
    }

    /// The point `north` and `east` metres from `origin()`.
    fn at(north: f64, east: f64) -> (f64, f64) {
        let (lat, lng) = origin();
        let per_degree = 6_371_000.0_f64.to_radians();
        (lat + north / per_degree, lng + east / (per_degree * lat.to_radians().cos()))
    }

    /// The POI categories of `pois_and_centroids`: 100 m reference distance, 500 m max distance,
    /// importance 5.
    const POI_META: &str = r#"{"0": {"name": "cafe", "max_distance": 500}, "1": {"name": "museum", "max_distance": 500},
        "2": {"name": "fountain", "max_distance": 500}, "60": {"name": "park", "max_distance": 500},
        "93": {"name": "island", "max_distance": 500}}"#;

    /// The postcode centroid files of these centroids (lat, lng, postcode string id, country),
    /// as the builder writes them, opened.
    fn centroid_files(centroids: &[(f32, f32, u32, [u8; 2])]) -> PostcodeFiles {
        let points: Vec<postcode_files::Centroid> = centroids.iter()
            .map(|&(lat, lng, postcode_id, country)| postcode_files::Centroid { lat, lng, postcode_id, country_code: u16::from_be_bytes(country) })
            .collect();
        let (centroids, cells) = postcode_files::encode_points(&points, DEFAULT_ADMIN_CELL_LEVEL);
        PostcodeFiles::open(FileBytes::Owned(centroids), FileBytes::Owned(cells), DEFAULT_ADMIN_CELL_LEVEL).unwrap()
    }

    /// An index of postcode centroids anywhere (lat, lng, postcode, country code or [0, 0]), its
    /// strings their postcodes, one each.
    fn centroids_at(centroids: &[(f32, f32, &str, [u8; 2])]) -> Index {
        let mut strings = Vec::new();
        let mut rows = Vec::new();
        for &(lat, lng, postcode, country) in centroids {
            rows.push((lat, lng, strings.len() as u32, country));
            strings.extend_from_slice(postcode.as_bytes());
            strings.push(0);
        }
        let end = strings.len() as u32;
        Index {
            postcode_centroids: Some(centroid_files(&rows)),
            strings: StringPool::from_parts([Some(strings::Tier::of_pool(&strings)), None, None, None, None], [0, end, end, end, end, end]),
            ..index(&[], &[])
        }
    }

    /// The postcode the centroid tier answers at (lat, lng) when reading every centroid's:
    /// the nearest valid one in plain degrees of the country (or of none) whose box holds the
    /// point, ties to the smaller postcode.
    fn nearest_centroid<'a>(centroids: &[(f32, f32, &'a str, [u8; 2])], lat: f64, lng: f64, country: Option<[u8; 2]>) -> Option<&'a str> {
        centroids.iter()
            .filter(|c| country.is_none_or(|cc| c.3 == [0, 0] || c.3 == cc))
            .filter_map(|&(c_lat, c_lng, postcode, c_country)| {
                let extent_m = postcode_extent_m(if c_country == [0, 0] { country.unwrap_or([0, 0]) } else { c_country }, postcode);
                let d = postcode_files::distance(lat, lng, c_lat as f64, c_lng as f64);
                (geography_box::box_covers(c_lat as f64, c_lng as f64, extent_m, lat, lng) && centroid_postcode_ok(postcode)).then_some((d, postcode))
            })
            .min_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(b.1)))
            .map(|(_, postcode)| postcode)
    }

    /// An index of point POIs (of POI_META's categories) and postcode centroids only, all in
    /// `origin()`'s cell: its POI cell list holds the POIs in record order, and its centroids
    /// are in the order given.
    fn pois_and_centroids(pois: &[Poi<'_>], centroids: &[Centroid<'_>]) -> Index {
        let own = cell_id_at_level(origin().0, origin().1, DEFAULT_ADMIN_CELL_LEVEL);
        let cell = |(lat, lng): (f64, f64)| {
            assert_eq!(cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL), own, "({lat}, {lng})");
            own
        };
        let mut strings = Vec::new();
        let mut string = |s: &str| {
            let id = strings.len() as u32;
            strings.extend_from_slice(s.as_bytes());
            strings.push(0);
            id
        };
        let (mut records, mut poi_pairs) = (Vec::new(), Vec::new());
        for (id, &(north, east, name, category, importance)) in pois.iter().enumerate() {
            let (lat, lng) = at(north, east);
            let record = PoiRecord {
                lat: lat as f32, lng: lng as f32, vertex_offset: NO_DATA, name_id: name.map_or(NO_DATA, &mut string), category,
                tier: 0, flags: 0, importance, parent_street_id: NO_DATA, parent_postcode_id: NO_DATA, parent_poly_id: NO_DATA,
            };
            records.push((record, None));
            poi_pairs.push((cell((lat, lng)), id as u32));
        }
        let (records, vertices) = poi_files::encode(&records);
        // Each centroid's postcode a string of its own: in the cell, in postcode_id order, the
        // centroids are in the order given.
        let rows: Vec<(f32, f32, u32, [u8; 2])> = centroids.iter()
            .map(|&(north, east, postcode, country)| {
                let (lat, lng) = at(north, east);
                cell((lat, lng));
                (lat as f32, lng as f32, string(postcode), country)
            })
            .collect();
        let end = strings.len() as u32;
        Index {
            poi_index: Some(cell_pairs::from_pairs(poi_pairs, cell_pairs::POI)),
            poi_records: Some(PoiRecords::open(FileBytes::Owned(records)).unwrap()),
            poi_vertices: Some(FileBytes::Owned(vertices)),
            poi_meta: PoiMeta::from_json(POI_META),
            postcode_centroids: Some(centroid_files(&rows)),
            strings: StringPool::from_parts([Some(strings::Tier::of_pool(&strings)), None, None, None, None], [0, end, end, end, end, end]),
            ..index(&[], &[])
        }
    }

    /// `f`'s result and the number of strings it read.
    fn reads<T>(f: impl FnOnce() -> T) -> (T, usize) {
        let before = STRING_READS.with(|n| n.get());
        let got = f();
        (got, STRING_READS.with(|n| n.get()) - before)
    }

    /// An index of polygon POIs of POI_META's categories in `origin()`'s cell, each (its corners
    /// as metres north of `origin()` and east, its name, category, importance), its vertices to
    /// 1e-7°, with a parent street of its own name.
    fn polygon_pois(pois: &[(&[(f64, f64)], &str, u8, u8)]) -> Index {
        let own = cell_id_at_level(origin().0, origin().1, DEFAULT_ADMIN_CELL_LEVEL);
        let mut strings = Vec::new();
        let (mut records, mut pairs) = (Vec::new(), Vec::new());
        for (id, &(corners, name, category, importance)) in pois.iter().enumerate() {
            let name_id = strings.len() as u32;
            strings.extend_from_slice(name.as_bytes());
            strings.push(0);
            let corners: Vec<(f64, f64)> = corners.iter().map(|&(north, east)| at(north, east)).collect();
            for &(lat, lng) in &corners {
                assert_eq!(cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL), own, "({lat}, {lng})");
            }
            let (bmin_lat, bmin_lng) = corners.iter().fold((f32::MAX, f32::MAX), |(a, b), &(lat, lng)| (a.min(lat as f32), b.min(lng as f32)));
            let q = corners.iter()
                .map(|&(lat, lng)| (((lat - bmin_lat as f64) / 1e-7).round() as u32, ((lng - bmin_lng as f64) / 1e-7).round() as u32))
                .collect();
            let record = PoiRecord {
                lat: corners[0].0 as f32, lng: corners[0].1 as f32, vertex_offset: 0, name_id, category, tier: 2, flags: 0, importance,
                parent_street_id: name_id, parent_postcode_id: NO_DATA, parent_poly_id: NO_DATA,
            };
            records.push((record, Some(poi_files::PoiPolygon { encoding: 3, bmin_lat, bmin_lng, q })));
            pairs.push((own, id as u32));
        }
        let (records, vertices) = poi_files::encode(&records);
        let end = strings.len() as u32;
        Index {
            poi_index: Some(cell_pairs::from_pairs(pairs, cell_pairs::POI)),
            poi_records: Some(PoiRecords::open(FileBytes::Owned(records)).unwrap()),
            poi_vertices: Some(FileBytes::Owned(vertices)),
            poi_meta: PoiMeta::from_json(POI_META),
            strings: StringPool::from_parts([Some(strings::Tier::of_pool(&strings)), None, None, None, None], [0, end, end, end, end, end]),
            ..index(&[], &[])
        }
    }

    /// A triangle whose side nearest `origin()` runs north-east, `metres` south-east of it at its
    /// point nearest in metres, and 400 m long.
    fn north_east_side(metres: f64) -> [(f64, f64); 3] {
        let (foot, half) = ((-metres / 2f64.sqrt(), metres / 2f64.sqrt()), 200.0 / 2f64.sqrt());
        [(foot.0 - half, foot.1 - half), (foot.0 + half, foot.1 + half), (foot.0 - 40.0, foot.1 + 40.0)]
    }

    #[test]
    fn a_poi_polygon_is_as_far_as_its_point_nearest_in_metres() {
        // In Paris, a polygon whose side nearest the point runs north-east is as far as that
        // side's point nearest in metres, not its point nearest in degrees, 7.5 % farther: one
        // 290 m off scores 5 / (1 + 2.9²), over the 0.5 a cafe (importance 5) is listed at, where
        // 312 m would not; and one 100 m off is nearer than a square 104 m off, so it is the first
        // listed and the nearest POI, where 107.5 m would put the square first.
        let idx = polygon_pois(&[(&north_east_side(290.0), "Far Cafe", 0, 0)]);
        let listed = idx.find_pois(origin().0, origin().1);
        assert!(listed.len() == 1 && (listed[0].distance_m - 290.0).abs() < 0.1, "{:?}", listed.iter().map(|p| p.distance_m).collect::<Vec<_>>());

        let square = [(104.0, -50.0), (104.0, 50.0), (154.0, 50.0), (154.0, -50.0)];
        let idx = polygon_pois(&[(&square, "Square", 1, 0), (&north_east_side(100.0), "Diagonal", 1, 0)]);
        let listed: Vec<_> = idx.find_pois(origin().0, origin().1).iter().map(|p| (p.name.to_string(), p.distance_m)).collect();
        assert!(listed.len() == 2 && listed[0].0 == "Diagonal" && (listed[0].1 - 100.0).abs() < 0.1 && listed[1].0 == "Square", "{listed:?}");
        let (d, nearest) = idx.find_nearest_poi_with_parent(origin().0, origin().1).unwrap();
        assert_eq!(idx.get_string(nearest.name_id), "Diagonal");
        assert!((d.sqrt() * 6_371_000.0 - 100.0).abs() < 0.1, "{}", d.sqrt() * 6_371_000.0);
    }

    #[test]
    fn a_poi_name_is_read_only_once_the_poi_passes_every_filter() {
        // Cafes (category 0) and a museum (1), with POI_META's distances and importance.
        let idx = pois_and_centroids(&[
            (50.0, 0.0, Some("Kept"), 0, 0),
            (2000.0, 0.0, Some("Too far"), 0, 0),  // past 550 m, the max at importance 5
            (50.0, 0.0, Some("Island"), 93, 0),    // islands never list
            (400.0, 0.0, Some("Faint"), 0, 1),     // scores 1 / (1 + 4²), under 0.5
            (60.0, 0.0, Some(""), 0, 0),           // passes every filter, but has no name
            (120.0, 0.0, Some("Also kept"), 1, 0),
        ], &[]);

        let (names, read) = reads(|| idx.find_pois(origin().0, origin().1).iter().map(|p| p.name.clone()).collect::<Vec<_>>());

        assert_eq!(names, ["Kept", "Also kept"]);
        assert_eq!(read, 3);
    }

    /// 2,000 centroids of France 60 m apart in `origin()`'s cell, 32 a row from west to east and
    /// the rows from south to north, so each block's rows are one row of them: the one at the
    /// origin is the 1,009th.
    fn centroid_grid(names: &[String]) -> Vec<Centroid<'_>> {
        (0..2000).map(|k| ((k / 32) as f64 * 60.0 - 1860.0, (k % 32) as f64 * 60.0 - 960.0, names[k].as_str(), *b"fr")).collect()
    }

    #[test]
    fn a_query_beside_one_of_many_centroids_decodes_only_the_blocks_as_near() {
        let names: Vec<String> = (0..2000).map(|k| format!("{}", 10_000 + k)).collect();
        let idx = pois_and_centroids(&[], &centroid_grid(&names));
        let files = idx.postcode_centroids.as_ref().unwrap();
        assert_eq!(files.rows(), 2000);
        assert_eq!(files.cell_rows(cell_id_at_level(origin().0, origin().1, DEFAULT_ADMIN_CELL_LEVEL)), (0, 2000));

        // 6 m east of the origin's centroid: its block's box holds the point, and every other
        // block's box is 50 m or more away, farther than the centroid. Read alone, the block's
        // centroids are read from the west: its postcode is the 17th read at most.
        let (lat, lng) = at(0.0, 6.0);
        let blocks_before = postcode_files::BLOCK_READS.with(|n| n.get());
        let (postcode, read) = reads(|| idx.resolve_postcode(lat, lng, None, None, None, None, None, Some(*b"fr")));
        let blocks = postcode_files::BLOCK_READS.with(|n| n.get()) - blocks_before;

        assert_eq!(postcode.as_deref(), Some("11008"));
        assert_eq!(blocks, 1);
        assert!(read <= 17, "{read}");
    }

    #[test]
    fn a_query_on_the_edge_of_a_blocks_box_answers_as_reading_every_centroid() {
        // Each block's box: its corners and the middles of its sides, and the points a metre
        // either side of them, nearest to a row of the block or of the next row of centroids.
        let names: Vec<String> = (0..2000).map(|k| format!("{}", 10_000 + k)).collect();
        let grid = centroid_grid(&names);
        let idx = pois_and_centroids(&[], &grid);
        let files = idx.postcode_centroids.as_ref().unwrap();
        let all: Vec<(f32, f32, &str, [u8; 2])> = grid.iter().map(|&(north, east, postcode, country)| {
            let (lat, lng) = at(north, east);
            (lat as f32, lng as f32, postcode, country)
        }).collect();
        let metre = 1.0 / 6_371_000.0_f64.to_radians();
        let mut queries = 0;
        for b in 0..files.blocks() {
            let [lat_lo, lat_hi, lng_lo, lng_hi] = files.block_box(b).unwrap();
            assert!([lat_lo, lat_hi, lng_lo, lng_hi].iter().all(|side| side.is_finite()), "block {b}");
            for lat in [lat_lo, (lat_lo + lat_hi) / 2.0, lat_hi] {
                for lng in [lng_lo, (lng_lo + lng_hi) / 2.0, lng_hi] {
                    for (dlat, dlng) in [(0.0, 0.0), (-metre, 0.0), (metre, 0.0), (0.0, -metre), (0.0, metre)] {
                        let (lat, lng) = (lat + dlat, lng + dlng);
                        let want = nearest_centroid(&all, lat, lng, Some(*b"fr"));
                        assert_eq!(idx.resolve_postcode(lat, lng, None, None, None, None, None, Some(*b"fr")).as_deref(), want, "({lat}, {lng}) block {b}");
                        queries += 1;
                    }
                }
            }
        }
        assert_eq!(queries, 63 * 45);
    }

    #[test]
    fn a_query_beside_the_antimeridian_answers_as_reading_every_centroid() {
        // Centroids a few hundred metres either side of the antimeridian, in the cells on both
        // sides, which are each other's neighbours; resolve_postcode measures across it the long
        // way round, so none answers from the other side. Each box reaches across it, and
        // ST_Envelope makes it of the longitudes between its corners the long way round, which
        // leaves out the meridian: no centroid names a point on it.
        let centroids: Vec<(f32, f32, &str, [u8; 2])> = vec![
            (-16.75, 179.997, "F1", [0, 0]), (-16.751, 179.9995, "F2", [0, 0]), (-16.749, -179.998, "W1", [0, 0]),
            (-16.75, -179.9995, "W2", [0, 0]), (-16.76, 179.96, "F3", [0, 0]), (-16.74, -179.97, "W3", [0, 0]),
        ];
        let cells: std::collections::BTreeSet<u64> = centroids.iter()
            .map(|c| cell_id_at_level(c.0 as f64, c.1 as f64, DEFAULT_ADMIN_CELL_LEVEL))
            .collect();
        assert!(cells.len() >= 2);
        let idx = centroids_at(&centroids);
        let mut answered = 0;
        for k in 0..=40 {
            let lng = 179.98 + 0.001 * k as f64;
            let lng = if lng > 180.0 { lng - 360.0 } else { lng };
            for lat in [-16.76, -16.75, -16.7495, -16.74] {
                let want = nearest_centroid(&centroids, lat, lng, None);
                assert_eq!(idx.resolve_postcode(lat, lng, None, None, None, None, None, None).as_deref(), want, "({lat}, {lng})");
                answered += usize::from(want.is_some());
            }
        }
        assert_eq!(answered, 40 * 4);
        assert_eq!(nearest_centroid(&centroids, -16.75, 180.0, None), None);
    }

    #[test]
    fn a_box_across_the_antimeridian_holds_points_far_from_its_centroid() {
        // expand_by_meters' 25 km box of a US centroid at (10, 179.999) reaches across the
        // antimeridian, and ST_Envelope makes it of every longitude between its corners the long
        // way round (geography_box): it holds points of its latitudes 110 km west, across the
        // antimeridian and half a world away where no nearer centroid's box does, as
        // get_nearest_postcode finds them, and none of other latitudes or another country.
        let centroids = [(10.0, 179.999, "96799", *b"us"), (10.0, 0.05, "96800", *b"us")];
        let idx = centroids_at(&centroids);
        for (lat, lng, country, want) in [
            (10.0, 179.0, b"us", Some("96799")), (10.1, -170.0, b"us", Some("96799")), (10.2, 90.0, b"us", Some("96799")),
            (10.0, 0.0, b"us", Some("96800")), (10.3, 90.0, b"us", None), (10.0, 179.0, b"fr", None),
        ] {
            assert_eq!(nearest_centroid(&centroids, lat, lng, Some(*country)), want, "({lat}, {lng})");
            assert_eq!(idx.resolve_postcode(lat, lng, None, None, None, None, None, Some(*country)).as_deref(), want, "({lat}, {lng})");
        }
        let idx = centroids_at(&centroids[..1]);
        assert_eq!(idx.resolve_postcode(10.0, 0.0, None, None, None, None, None, Some(*b"us")).as_deref(), Some("96799"));
        assert_eq!(idx.resolve_postcode(10.0, 0.0, None, None, None, None, None, None).as_deref(), Some("96799"));

        // One can be nearer than a centroid within reach that holds the point: at (0, 179.69),
        // 96801's box 0.22° south-west holds it and 96802's across the antimeridian, 0.30° east,
        // whose cell is past the cells within reach.
        let centroids = [(0.22, 179.47, "96801", *b"us"), (0.0, 179.99, "96802", *b"us")];
        let reach = MAX_POSTCODE_EXTENT_M * geography_box::BOX_REACH_FACTOR;
        let wrapping = cell_id_at_level(centroids[1].0 as f64, centroids[1].1 as f64, DEFAULT_ADMIN_CELL_LEVEL);
        assert!(!cells_near_point(0.0, 179.69, reach, DEFAULT_ADMIN_CELL_LEVEL).contains(&wrapping));
        assert_eq!(nearest_centroid(&centroids[..1], 0.0, 179.69, Some(*b"us")), Some("96801"));
        assert_eq!(nearest_centroid(&centroids, 0.0, 179.69, Some(*b"us")), Some("96802"));
        assert_eq!(centroids_at(&centroids).resolve_postcode(0.0, 179.69, None, None, None, None, None, Some(*b"us")).as_deref(), Some("96802"));

        // Near a pole the cells read reach far past the reach in longitude, and the boxes of the
        // polar projection farther still: at (-89.614, 4.095) two boxes hold the point, and the
        // nearer centroid's cell is one of the antimeridian's.
        let centroids = [(-89.803123, 91.400845, "AQ1", *b"us"), (-89.780914, -84.83687, "AQ2", *b"us")];
        assert_eq!(nearest_centroid(&centroids[1..], -89.614027, 4.095063, None), Some("AQ2"));
        assert_eq!(nearest_centroid(&centroids, -89.614027, 4.095063, None), Some("AQ1"));
        assert_eq!(centroids_at(&centroids).resolve_postcode(-89.614027, 4.095063, None, None, None, None, None, None).as_deref(), Some("AQ1"));
    }

    #[test]
    fn a_centroid_farther_than_the_best_so_far_leaves_its_postcode_unread() {
        // One block, read in the order given (each postcode its own string, in postcode_id
        // order).
        let idx = pois_and_centroids(&[], &[
            (1000.0, 0.0, "75001", *b"fr"),
            (2000.0, 0.0, "75002", *b"fr"),  // farther: not read
            (1000.0, 0.0, "75003", *b"fr"),  // as near, a larger postcode
            (500.0, 0.0, "75;04", *b"fr"),   // nearer, not a postcode
            (300.0, 0.0, "10115", *b"de"),   // another country's: not read
            (1000.0, 0.0, "75000", *b"fr"),  // as near, a smaller postcode: the answer
            (1500.0, 0.0, "75005", *b"fr"),  // farther: not read
        ]);

        let (postcode, read) = reads(|| idx.resolve_postcode(origin().0, origin().1, None, None, None, None, None, Some(*b"fr")));

        assert_eq!(postcode.as_deref(), Some("75000"));
        assert_eq!(read, 4);
    }

    /// A name of the jumble's pool with its NUL: "Lieu00" to "Lieu29".
    const JUMBLE_NAME_BYTES: u32 = 7;

    /// 150 POIs of POI_META's categories on a 300 m grid around origin(), named from 30 names:
    /// points and polygons (squares and triangles 40 to 400 m across, of every vertex encoding,
    /// open and closed rings), one in fifteen a tombstone, two in three with a parent street.
    fn poi_jumble() -> Vec<(PoiRecord, Option<poi_files::PoiPolygon>)> {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut pick = move |n: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % n as u64) as usize
        };
        let grid = |k: usize| (k as f64 - 5.0) * 300.0;
        (0..150)
            .map(|k| {
                let (north, east) = (grid(pick(11)), grid(pick(11)));
                let (lat, lng) = at(north, east);
                let polygon = (pick(2) == 0).then(|| {
                    let encoding = pick(4) as u8;
                    let half = [20.0, 80.0, 200.0][pick(3)];
                    let mut corners = vec![at(north - half, east - half), at(north - half, east + half), at(north + half, east + half)];
                    if pick(2) == 0 {
                        corners.push(at(north + half, east - half));
                    }
                    let (bmin_lat, bmin_lng) = (corners[0].0 as f32, corners[0].1 as f32);
                    let scale = vertex_scale(encoding);
                    let mut q: Vec<(u32, u32)> = corners
                        .iter()
                        .map(|&(a, b)| (((a - bmin_lat as f64) / scale).round() as u32, ((b - bmin_lng as f64) / scale).round() as u32))
                        .collect();
                    if pick(2) == 0 {
                        q.push(q[0]);
                    }
                    poi_files::PoiPolygon { encoding, bmin_lat, bmin_lng, q }
                });
                let record = PoiRecord {
                    lat: lat as f32, lng: lng as f32, vertex_offset: 0,
                    name_id: if pick(15) == 0 { NO_DATA } else { JUMBLE_NAME_BYTES * pick(30) as u32 },
                    category: [0, 1, 2, 60, 93][pick(5)], tier: 1 + pick(3) as u8, flags: 0, importance: [0, 1, 20, 100][pick(4)],
                    parent_street_id: if pick(3) == 0 { NO_DATA } else { JUMBLE_NAME_BYTES * pick(30) as u32 },
                    parent_postcode_id: NO_DATA, parent_poly_id: NO_DATA,
                };
                let _ = k;
                (record, polygon)
            })
            .collect()
    }

    /// The POI lookups of an index of the jumble, on a 250 m grid and at each POI: every
    /// find_pois match and find_nearest_poi_with_parent's answer, as text.
    fn poi_jumble_answers() -> (usize, usize, u64) {
        let pois = poi_jumble();
        let own = cell_id_at_level(origin().0, origin().1, DEFAULT_ADMIN_CELL_LEVEL);
        let pairs = pois.iter().enumerate().filter(|(_, (r, _))| r.name_id != NO_DATA).map(|(i, _)| (own, i as u32));
        let strings: Vec<u8> = (0..30).flat_map(|k| format!("Lieu{k:02}").into_bytes().into_iter().chain([0])).collect();
        assert_eq!(strings.len() as u32, 30 * JUMBLE_NAME_BYTES);
        let end = strings.len() as u32;
        let (records, vertices) = poi_files::encode(&pois);
        let idx = Index {
            poi_index: Some(cell_pairs::from_pairs(pairs.collect::<Vec<_>>(), cell_pairs::POI)),
            poi_records: Some(PoiRecords::open(FileBytes::Owned(records)).unwrap()),
            poi_vertices: Some(FileBytes::Owned(vertices)),
            poi_meta: PoiMeta::from_json(POI_META),
            strings: StringPool::from_parts([Some(strings::Tier::of_pool(&strings)), None, None, None, None], [0, end, end, end, end, end]),
            ..index(&[], &[])
        };
        let points = (0..13 * 13).map(|k| at((k / 13) as f64 * 250.0 - 1500.0, (k % 13) as f64 * 250.0 - 1500.0))
            .chain(pois.iter().map(|(r, _)| (r.lat as f64, r.lng as f64)));
        let (mut matches, mut contained, mut hash) = (0, 0, 0xcbf2_9ce4_8422_2325_u64);
        for (lat, lng) in points {
            assert_eq!(cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL), own, "({lat}, {lng})");
            let listed: Vec<_> = idx.find_pois(lat, lng).iter()
                .map(|p| (p.name.clone(), p.category_id, p.distance_m.to_bits(), p.contained, p.is_point, p.score.to_bits(), p.area.to_bits()))
                .collect();
            matches += listed.len();
            contained += listed.iter().filter(|p| p.3).count();
            let nearest = idx.find_nearest_poi_with_parent(lat, lng).map(|(d, p)| (d.to_bits(), p.name_id, p.category, p.lat.to_bits()));
            for b in format!("{listed:?} {nearest:?}").bytes() {
                hash = (hash ^ b as u64).wrapping_mul(0x0100_0000_01b3);
            }
        }
        (matches, contained, hash)
    }

    #[test]
    fn poi_lookups_answer_as_the_old_reader_did() {
        // The answers today's 36-byte records and vertex stream gave for the jumble (the reader
        // before R2 and V2, at 447a8a3), match for match and bit for bit, with a polygon as far
        // as its edges' points nearest in metres (point_to_segment_distance).
        assert_eq!(poi_jumble_answers(), (1593, 122, 375_056_972_952_063_247));
    }

    #[test]
    fn every_poi_name_and_postcode_answers_as_reading_them_all_would() {
        // A jumble on a 300 m grid, so centroids coincide and tie: POIs of every filter's kind,
        // one in five without a name, and centroids of three countries (one none), some with no
        // valid postcode.
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let mut pick = move |n: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % n as u64) as usize
        };
        let grid = |k: usize| (k as f64 - 5.0) * 300.0;
        let names: Vec<String> = (0..40).map(|k| format!("Lieu {k}")).collect();
        let pois: Vec<Poi<'_>> = (0..300).map(|_| {
            let (north, east) = (grid(pick(11)), grid(pick(11)));
            let name = if pick(5) == 0 { "" } else { names[pick(names.len())].as_str() };
            (north, east, Some(name), [0, 1, 2, 60, 93][pick(5)], [0, 1, 20, 100][pick(4)])
        }).collect();
        let postcodes = ["75001", "75002", "75003", "75004", "75005", "", "75;06", "75001-2345", "10115"];
        let centroids: Vec<Centroid<'_>> = (0..300).map(|_| {
            let (north, east) = (grid(pick(11)), grid(pick(11)));
            (north, east, postcodes[pick(postcodes.len())], [*b"fr", *b"de", [0, 0]][pick(3)])
        }).collect();
        // Reading every name first drops a nameless POI before any filter, as a tombstone is.
        let tombstoned: Vec<Poi<'_>> = pois.iter().map(|&(n, e, name, c, i)| (n, e, name.filter(|s| !s.is_empty()), c, i)).collect();
        let (idx, eager) = (pois_and_centroids(&pois, &centroids), pois_and_centroids(&tombstoned, &centroids));
        fn listed(idx: &Index, lat: f64, lng: f64) -> Vec<(Cow<'_, str>, u8, u64, bool, bool, u64, u64)> {
            idx.find_pois(lat, lng).iter()
                .map(|p| (p.name.clone(), p.category_id, p.distance_m.to_bits(), p.contained, p.is_point, p.score.to_bits(), p.area.to_bits()))
                .collect()
        }
        // Reading every centroid's postcode: the nearest valid one in plain degrees of the
        // country, or of none, whose box holds the point, ties to the smaller postcode; and
        // whether another postcode is as near.
        let nearest = |lat: f64, lng: f64, country: Option<[u8; 2]>| {
            let mut found: Vec<(f64, &str)> = centroids.iter()
                .filter(|c| country.is_none_or(|cc| c.3 == [0, 0] || c.3 == cc))
                .filter_map(|&(north, east, postcode, c_country)| {
                    let (c_lat, c_lng) = at(north, east);
                    let (c_lat, c_lng) = (c_lat as f32, c_lng as f32);
                    let extent_m = postcode_extent_m(if c_country == [0, 0] { country.unwrap_or([0, 0]) } else { c_country }, postcode);
                    let d = postcode_files::distance(lat, lng, c_lat as f64, c_lng as f64);
                    (geography_box::box_covers(c_lat as f64, c_lng as f64, extent_m, lat, lng) && centroid_postcode_ok(postcode)).then_some((d, postcode))
                })
                .collect();
            found.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(b.1)));
            let tied = found.len() > 1 && found[0].0 == found[1].0 && found[0].1 != found[1].1;
            (found.first().map(|&(_, s)| s), tied)
        };

        // The nameless POIs that pass every filter, and the answers another postcode ties.
        let (mut nameless, mut tied) = (0, 0);
        for (north, east) in pois.iter().map(|p| (p.0, p.1)).chain(centroids.iter().map(|c| (c.0, c.1))) {
            let (lat, lng) = at(north, east);
            let ((got, read), (want, read_named)) = (reads(|| listed(&idx, lat, lng)), reads(|| listed(&eager, lat, lng)));
            assert_eq!(got, want, "({lat}, {lng})");
            nameless += read - read_named;
            for country in [None, Some(*b"fr"), Some(*b"de")] {
                let (want, tie) = nearest(lat, lng, country);
                assert_eq!(idx.resolve_postcode(lat, lng, None, None, None, None, None, country).as_deref(), want, "({lat}, {lng}) {country:?}");
                tied += usize::from(tie);
            }
        }
        assert!(nameless > 1000 && tied > 500, "{nameless} {tied}");
    }
}

/// Crude "how specific is this result" score for picking among
/// boundary-overlap candidates. Counts populated address fields plus a
/// nudge for places resolved.
fn address_score(a: &Address<'_>) -> i32 {
    let d = &a.address;
    let mut score = 0;
    if d.country.is_some() { score += 1; }
    if d.state.is_some() { score += 2; }
    if d.region.is_some() { score += 2; }
    if d.county.is_some() { score += 3; }
    if d.city.is_some() { score += 4; }
    if d.town.is_some() { score += 4; }
    if d.village.is_some() { score += 4; }
    if d.suburb.is_some() { score += 3; }
    if d.neighbourhood.is_some() { score += 3; }
    if d.road.is_some() { score += 5; }
    if d.house_number.is_some() { score += 6; }
    if !a.places.is_empty() { score += 1; }
    score
}

// --- Geometry helpers ---

// Countries with `postcode: no` in nominatim's country_settings.yaml
// (UAE, Qatar, most of Sub-Saharan Africa, etc.). Nominatim rejects
// any postcode for these countries during import via the
// clean_postcodes sanitizer. We mirror that here at query time —
// any OSM addr:postcode tag on a Dubai building (typically '000000'
// or similar placeholder) gets filtered out.
/// How far from a postcode centroid of `country` (its ISO code) the box it
/// covers reaches north, south, east and west, in metres: the `extent` of the
/// country's postcode pattern that `postcode` matches in Nominatim's
/// settings/country_settings.yaml, 5000 where the pattern gives none
/// (postcode_format.py). Nominatim's postcodes.py makes each artificial
/// postcode's geometry expand_by_meters(centroid, extent). Nigeria's second
/// pattern, of letters, has an extent of 1; a postcode of digits only (or "")
/// has the largest extent of its country's.
pub fn postcode_extent_m(country: [u8; 2], postcode: &str) -> f64 {
    match &country.map(|b| b.to_ascii_uppercase()) {
        b"CH" | b"EE" | b"JP" => 3000.0,
        b"GB" | b"IM" => 700.0,
        b"GG" | b"LU" | b"PT" => 1000.0,
        b"IE" => 50.0,
        b"LI" => 4000.0,
        b"NL" => 800.0,
        b"RO" => 2500.0,
        b"US" => MAX_POSTCODE_EXTENT_M,
        b"NG" if postcode.bytes().any(|b| b.is_ascii_alphabetic()) => 1.0,
        _ => DEFAULT_POSTCODE_EXTENT_M,
    }
}

/// The extent of a postcode whose country's pattern gives none
/// (postcode_format.py: config.get('extent', 5000)).
pub const DEFAULT_POSTCODE_EXTENT_M: f64 = 5000.0;

/// The largest extent of any country's postcodes, the United States'.
pub const MAX_POSTCODE_EXTENT_M: f64 = 25_000.0;

pub fn country_has_no_postcode(country_code: &[u8; 2]) -> bool {
    let cc = std::str::from_utf8(country_code).unwrap_or("").to_ascii_lowercase();
    matches!(cc.as_str(),
        "ae" | "ag" | "ao" | "bf" | "bi" | "bj" | "bo" | "bs" | "bw" | "bz" |
        "cd" | "cf" | "cg" | "ci" | "ck" | "cm" | "dj" | "dm" | "er" | "fj" |
        "ga" | "gd" | "gm" | "gq" | "gy" | "jm" | "ki" | "km" | "kp" | "ly" |
        "ml" | "mr" | "mw" | "nr" | "nu" | "qa" | "rw" | "sb" | "sc" | "sl" |
        "sr" | "ss" | "st" | "sy" | "td" | "tg" | "tk" | "tl" | "to" | "tv" |
        "ug" | "vu" | "ye" | "zw"
    )
}

// Cheap postcode validity check: rejects strings of all-zero / all-
// same digits (e.g. '000000', '0000') which show up in OSM when a
// mapper leaves the addr:postcode tag as a placeholder. Mirrors
// the spirit of nominatim's CountryPostcodeMatcher.match() rejecting
// values that don't conform to the country's pattern — we don't
// ship the full regex table here but this catches the common
// garbage cases the test3 set exposes.
pub fn postcode_looks_valid(_country_code: &[u8; 2], postcode: &str) -> bool {
    let trimmed: String = postcode.chars().filter(|c| !c.is_whitespace() && *c != '-').collect();
    if trimmed.is_empty() { return false; }
    // All same character (e.g. '00000', '-----').
    let first = trimmed.chars().next().unwrap();
    if trimmed.chars().all(|c| c == first) {
        // 'X' alone might still be valid in rare cases, but '00000'
        // or 'XXXXX' is always a placeholder.
        return false;
    }
    true
}

/// Whether a centroid at squared distance `d` beats the current best.
/// Equally near centroids (GeoNames puts many PO-box ZIPs at one post
/// office) are ordered by postcode, never by record order, which differs
/// between continent and planet builds and between fresh and chained ones.
pub fn nearer_postcode(d: f64, s: &str, best: Option<(f64, &str)>) -> bool {
    match best {
        None => true,
        Some((best_d, best_s)) => d < best_d || (d == best_d && s < best_s),
    }
}

/// Whether a postcode centroid's postcode can name a point: not a list of values. The builder
/// keeps a centroid only of the postcode its country's pattern makes (postcode_validation.h), of
/// any length or shape, Iran's ddddd-ddddd among them; postcode_looks_valid drops placeholders
/// (all zeros or dashes). There is no PO/Box filter: Nominatim has none, and the Portsmouth
/// area's postcodes (PO1 1BA) are real.
pub fn centroid_postcode_ok(s: &str) -> bool {
    !s.contains(';')
}

// Per-country postcode formatter. Mirrors Nominatim's
// CountryPostcodeMatcher (settings/country_settings.yaml) for the
// countries where OSM frequently stores postcodes in an unspaced
// form but Nominatim emits the canonical spaced form — Sweden
// (ddd dd), Greece (ddd dd), Netherlands (dddd AA), UK (partial),
// Canada (ldl dld), Brazil (ddddd-ddd), Japan (ddd-dddd).
//
// Strategy: strip existing spaces / dashes, match the country's
// digit-pattern, reinsert the canonical separator. On no match
// returns the original string borrowed.
pub fn format_postcode<'a>(country_code: &[u8; 2], postcode: &'a str) -> Cow<'a, str> {
    let cc = std::str::from_utf8(country_code).unwrap_or("").to_ascii_lowercase();
    // Upper-case buffer with spaces/dashes stripped.
    let bare: String = postcode
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .flat_map(|c| c.to_uppercase())
        .collect();

    // Non-ASCII postcodes (possible in raw addr:postcode tags) are returned
    // verbatim: every split arm below indexes `bare` by byte offset, which
    // panics on a non-char-boundary. All real formats are ASCII.
    if !bare.is_ascii() {
        return Cow::Borrowed(postcode);
    }

    // Helper — split at index `at`, joined by `sep`.
    let split_at = |at: usize, sep: char| -> Option<String> {
        if bare.len() < at + 1 { return None; }
        let (a, b) = bare.split_at(at);
        Some(format!("{}{}{}", a, sep, b))
    };

    let matches_digits = |len: usize| -> bool {
        bare.len() == len && bare.chars().all(|c| c.is_ascii_digit())
    };

    let result = match cc.as_str() {
        // ddd dd — Sweden, Greece, Norway (partial), Slovakia, Czechia
        "se" | "gr" | "sk" | "cz" if matches_digits(5) => split_at(3, ' '),
        // dddd AA — Netherlands, Argentina (optional)
        "nl" if bare.len() == 6
            && bare[..4].chars().all(|c| c.is_ascii_digit())
            && bare[4..].chars().all(|c| c.is_ascii_alphabetic()) => {
            split_at(4, ' ')
        }
        // ldl dld — Canada
        "ca" if bare.len() == 6 => {
            let c: Vec<char> = bare.chars().collect();
            if c[0].is_ascii_alphabetic() && c[1].is_ascii_digit() && c[2].is_ascii_alphabetic()
                && c[3].is_ascii_digit() && c[4].is_ascii_alphabetic() && c[5].is_ascii_digit() {
                split_at(3, ' ')
            } else { None }
        }
        // ddd-dddd — Japan
        "jp" if matches_digits(7) => split_at(3, '-'),
        // ddddd-ddd — Brazil
        "br" if matches_digits(8) => split_at(5, '-'),
        // ddddd-dddd — US ZIP+4: Nominatim's country pattern is
        // "(ddddd)(?:-dddd)?" with output \1, i.e. it stores only the 5-digit
        // base. Match the exact ZIP+4 shape on the ORIGINAL string (not `bare`,
        // since a dashless 9-digit string would not fullmatch Nominatim's
        // pattern) and emit the 5-digit base. Output-path only: a stored
        // "12345-6789" (a US addr's own addr:postcode) becomes "12345"; plain
        // 5-digit ZIPs and postcode centroids are unaffected.
        "us" if postcode.len() == 10
            && postcode.as_bytes()[5] == b'-'
            && postcode.as_bytes()[..5].iter().all(u8::is_ascii_digit)
            && postcode.as_bytes()[6..].iter().all(u8::is_ascii_digit) => {
            Some(postcode[..5].to_string())
        }
        _ => None,
    };

    match result {
        Some(s) => Cow::Owned(s),
        None => Cow::Borrowed(postcode),
    }
}

/// Whether a candidate at `dist` beats the current best at `best_dist`.
/// Nominatim orders address points and streets by distance alone and leaves
/// exact ties (a point inside two buildings, the junction node two streets
/// share) to database row order. Here a tie goes to the smaller content key,
/// so the answer depends on neither record order (which differs between a
/// fresh build and a chained one) nor string offsets (which move when a string
/// changes tier). `keys` gives (candidate key, best key), None without a best.
pub fn beats<K: PartialOrd>(dist: f64, best_dist: f64, keys: impl FnOnce() -> Option<(K, K)>) -> bool {
    if dist != best_dist {
        return dist < best_dist;
    }
    keys().is_some_and(|(k, best)| k < best)
}

/// The `cos_lat` of a distance planar in degrees, as PostGIS measures lon/lat
/// geometry: no cos(lat) scaling of longitude.
const PLANAR: f64 = 1.0;

pub fn dist_sq(dlat: f64, dlng: f64, cos_lat: f64) -> f64 {
    dlat * dlat + dlng * dlng * cos_lat * cos_lat
}

pub fn point_to_segment_with_t(
    px: f64, py: f64,
    ax: f64, ay: f64,
    bx: f64, by: f64,
    cos_lat: f64,
) -> (f64, f64) {
    let dx = bx - ax;
    let dy = by - ay;
    let len_sq = dx * dx + dy * dy;

    let t = if len_sq == 0.0 {
        0.0
    } else {
        (((px - ax) * dx + (py - ay) * dy) / len_sq).clamp(0.0, 1.0)
    };

    let proj_x = ax + t * dx;
    let proj_y = ay + t * dy;
    let dlat = (px - proj_x).to_radians();
    let dlng = (py - proj_y).to_radians();
    (dist_sq(dlat, dlng, cos_lat), t)
}

/// The point of a line nearest (lat, lng), as (lat, lng): planar in degrees,
/// as PostGIS ST_ClosestPoint is on placex's lon/lat geometry (reverse.py:46),
/// so a longitude degree counts as much as a latitude degree at any latitude.
pub fn closest_point_on_line(nodes: &[NodeCoord], lat: f64, lng: f64) -> Option<(f64, f64)> {
    let mut best: Option<(f64, f64, f64)> = None;
    for w in nodes.windows(2) {
        let (a, b) = ((w[0].lat as f64, w[0].lng as f64), (w[1].lat as f64, w[1].lng as f64));
        let (d, t) = point_to_segment_with_t(lat, lng, a.0, a.1, b.0, b.1, PLANAR);
        if best.map_or(true, |(bd, _, _)| d < bd) {
            best = Some((d, a.0 + t * (b.0 - a.0), a.1 + t * (b.1 - a.1)));
        }
    }
    best.map(|(_, plat, plng)| (plat, plng))
}

/// The point halfway along a line, as (lat, lng): Nominatim's centroid of a line
/// (get_center_point, utils.sql), ST_LineInterpolatePoint(geometry, 0.5) on placex's lon/lat
/// geometry, planar in degrees, and ST_ReducePrecision to 1e-7 degrees. A line of no length is
/// its first point; None for one without nodes.
pub fn line_middle(nodes: &[NodeCoord]) -> Option<(f64, f64)> {
    let first = nodes.first()?;
    let point = |n: &NodeCoord| (n.lat as f64, n.lng as f64);
    let length = |a: (f64, f64), b: (f64, f64)| (b.0 - a.0).hypot(b.1 - a.1);
    let total: f64 = nodes.windows(2).map(|w| length(point(&w[0]), point(&w[1]))).sum();
    let mut left = total / 2.0;
    let mut middle = point(first);
    for w in nodes.windows(2) {
        let (a, b) = (point(&w[0]), point(&w[1]));
        let step = length(a, b);
        if step > 0.0 && left <= step {
            let t = left / step;
            middle = (a.0 + t * (b.0 - a.0), a.1 + t * (b.1 - a.1));
            break;
        }
        left -= step;
        middle = b;
    }
    let reduce = |v: f64| (v * 1e7).round() / 1e7;
    Some((reduce(middle.0), reduce(middle.1)))
}

/// Squared distance (radians²) from the point (px, py) to the segment from
/// (ax, ay) to (bx, by), latitudes and longitudes in degrees, on the plane
/// where a degree of longitude is `cos_lat` of a degree of latitude: to the
/// segment's point nearest in metres, at any latitude.
pub fn point_to_segment_distance(
    px: f64, py: f64,
    ax: f64, ay: f64,
    bx: f64, by: f64,
    cos_lat: f64,
) -> f64 {
    point_to_segment_with_t(px, py * cos_lat, ax, ay * cos_lat, bx, by * cos_lat, PLANAR).0
}

// Squared distance (radians²) from query point to nearest polygon edge,
// or 0 if the point is inside. Matches Nominatim's ST_Distance(query,
// polygon_geom) = 0 when the query lies inside the footprint.
pub fn polygon_distance_sq(lat: f64, lng: f64, vertices: &[NodeCoord], cos_lat: f64) -> f64 {
    if vertices.is_empty() { return f64::MAX; }
    if point_in_polygon(lat as f32, lng as f32, vertices) { return 0.0; }
    let mut best = f64::MAX;
    for i in 0..vertices.len() {
        let j = (i + 1) % vertices.len();
        let d = point_to_segment_distance(
            lat, lng,
            vertices[i].lat as f64, vertices[i].lng as f64,
            vertices[j].lat as f64, vertices[j].lng as f64,
            cos_lat,
        );
        if d < best { best = d; }
    }
    best
}

// Ray casting point-in-polygon test
pub fn point_in_polygon(lat: f32, lng: f32, vertices: &[NodeCoord]) -> bool {
    let mut inside = false;
    let n = vertices.len();
    let mut j = n - 1;

    for i in 0..n {
        let vi = &vertices[i];
        let vj = &vertices[j];

        if ((vi.lng > lng) != (vj.lng > lng))
            && (lat < (vj.lat - vi.lat) * (lng - vi.lng) / (vj.lng - vi.lng) + vi.lat)
        {
            inside = !inside;
        }
        j = i;
    }

    inside
}

// --- API types ---

pub struct PoiMatch<'a> {
    pub name: Cow<'a, str>,
    pub category: &'a str,
    pub category_id: u8,
    pub distance_m: f64,
    pub contained: bool,
    // Point POIs (no polygon geometry) are centroid-only hits —
    // multiple of them in the same category look like chain-outlet
    // clutter (three "cafe" within 500 m). Polygon POIs have
    // distinct footprints and two nearby polygons of the same
    // category are usually genuinely distinct landmarks (Big Ben
    // vs Palace of Westminster, both attractions).
    pub is_point: bool,
    pub score: f64,
    // Polygon area in (squared-degree-ish shoelace units). Used to
    // tie-break between POIs that all contain the query — the
    // smallest polygon is the most specific ("Eiffel Tower" rather
    // than "Field of Mars"). 0.0 for point POIs or uncontained.
    pub area: f64,
}

/// Ordering of POI matches (and so the landmark, the first contained one):
///   1. Contained polygons first — the query is physically inside them,
///      so they beat any nearby point POI regardless of score.
///   2. Within contained, polygon area ascending — smallest = most
///      specific (Eiffel Tower over Field of Mars over "Paris, Banks of
///      the Seine"). Area beats importance: a wikidata-boosted attraction
///      that happens to be the bigger containing polygon should not
///      outrank the tiny, specific landmark the user clicked on.
///   3. Non-contained POIs: score descending (importance × proximity).
///   4. Ties by name, then category. Never by record order, which
///      differs between a fresh and a chained build of the same data.
pub fn poi_order(a: &PoiMatch<'_>, b: &PoiMatch<'_>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a.contained, b.contained) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (true, true) => a.area.partial_cmp(&b.area).unwrap_or(Ordering::Equal),
        (false, false) => b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal),
    }
    .then_with(|| a.name.cmp(&b.name))
    .then_with(|| a.category_id.cmp(&b.category_id))
}

/// Half-side in metres of a place node's `place_node_fuzzy_area`
/// (lib-sql/functions/utils.sql) by PlaceNode type, from its rank_search:
///   rank_search ≤ 16 (city):   15000 m
///   rank_search ≤ 18 (town):    4000 m
///   rank_search ≤ 19 (village): 2000 m  (place=suburb default too)
///   rank_search ≤ 20 (hamlet):  1000 m  (place=quarter too)
///   default:                     500 m  (place=neighbourhood, rank_search 24)
/// The area is expand_by_meters: the envelope of ST_Buffer(geography, radius,
/// 1), a square whose corners lie on the axes at the radius, so the half-side
/// is the radius. (Lambton is 726 m east of the quarter Paekākā in Wellington
/// and Nominatim's answer has both.)
pub fn place_fuzzy_half_side_m(pt: usize) -> f64 {
    match pt {
        0 => MAX_PLACE_FUZZY_HALF_SIDE_M, // city
        1 => 4000.0,               // town
        2 | 3 => 2000.0,           // village / suburb (rank_search ≤ 19)
        4 | 6 => 1000.0,           // hamlet / quarter (rank_search 20)
        _ => 500.0,                // neighbourhood
    }
}

/// The largest half side a place node's fuzzy area has, a city's (rank_search
/// <= 16, utils.sql:344-345): no node farther than this from a feature along
/// either axis covers it.
pub const MAX_PLACE_FUZZY_HALF_SIDE_M: f64 = 15000.0;

/// How far from the point debug_places lists the place nodes in the cells
/// find_places looks in.
const DEBUG_PLACES_NEARBY_M: f64 = 20000.0;

/// Metres per degree of latitude (and of longitude at the equator) in the
/// fuzzy-area squares.
pub const METRES_PER_DEGREE: f64 = 111320.0;

/// The cells of `level` a node within `margin_m` of `line` along each axis
/// can sit in, ascending: the cells under the union over stretches of the line
/// of each stretch's lat/lng box grown by `margin_m` (grown_rect). A node's
/// fuzzy area crosses the line only where it crosses a segment, so the node
/// lies in that segment's grown box. Consecutive segments share a box while it
/// spans at most twice `margin_m` either way, and a longer segment is cut into
/// pieces that don't: a compact street costs one box, a long or diagonal one
/// a strip of boxes along it instead of its whole bounding box.
pub fn cells_under_line(line: &[NodeCoord], margin_m: f64, level: u64) -> Vec<u64> {
    let span_m = 2.0 * margin_m;
    let points = cut_to_span(line, span_m);
    let mut boxes = Vec::new();
    let mut start = 0;
    while start < points.len() {
        // At least one segment, then more while the box stays small.
        let mut end = (start + 2).min(points.len());
        let mut bounds = points[start..end].iter().fold(LatLngBox::at(points[start]), |b, &p| b.union(LatLngBox::at(p)));
        while end < points.len() {
            let grown = bounds.union(LatLngBox::at(points[end]));
            if grown.extent_m() > span_m { break; }
            bounds = grown;
            end += 1;
        }
        boxes.push(grown_rect(bounds, margin_m));
        if end == points.len() { break; }
        start = end - 1;
    }
    cells_under_rects(boxes, level)
}

/// The cells of `level` a node within `margin_m` of the point along each axis
/// can sit in, ascending: the cells under the point grown by `margin_m`
/// (grown_rect).
pub fn cells_near_point(lat: f64, lng: f64, margin_m: f64, level: u64) -> Vec<u64> {
    cells_under_rects(vec![grown_rect(LatLngBox::at((lat, lng)), margin_m)], level)
}

/// The line's points as (lat, lng) degrees, with points added along every
/// segment whose box spans more than `span_m` either way, evenly in degrees:
/// the fuzzy-area tests map degrees linearly to metres, so the pieces cover
/// the segment exactly.
fn cut_to_span(line: &[NodeCoord], span_m: f64) -> Vec<(f64, f64)> {
    let mut points: Vec<(f64, f64)> = Vec::with_capacity(line.len());
    for p in line {
        let b = (p.lat as f64, p.lng as f64);
        if let Some(&a) = points.last() {
            let pieces = (LatLngBox::at(a).union(LatLngBox::at(b)).extent_m() / span_m).ceil().max(1.0) as usize;
            for k in 1..pieces {
                let t = k as f64 / pieces as f64;
                points.push((a.0 + (b.0 - a.0) * t, a.1 + (b.1 - a.1) * t));
            }
        }
        points.push(b);
    }
    points
}

/// A lat/lng box in degrees: (lat_lo, lng_lo, lat_hi, lng_hi).
#[derive(Clone, Copy)]
struct LatLngBox(f64, f64, f64, f64);

impl LatLngBox {
    fn at((lat, lng): (f64, f64)) -> LatLngBox {
        LatLngBox(lat, lng, lat, lng)
    }

    fn union(self, o: LatLngBox) -> LatLngBox {
        LatLngBox(self.0.min(o.0), self.1.min(o.1), self.2.max(o.2), self.3.max(o.3))
    }

    /// The larger of the box's height and width in metres, the width at its
    /// middle latitude.
    fn extent_m(self) -> f64 {
        let mid_lat = (self.0 + self.2) / 2.0;
        ((self.2 - self.0) * METRES_PER_DEGREE).max((self.3 - self.1) * METRES_PER_DEGREE * mid_lat.to_radians().cos())
    }
}

/// `bounds` grown by `margin_m` along each axis, longitude metres taken at the
/// latitude in the grown box farthest from the equator, as fuzzy_area_holds
/// measures them from a node. The box stops at ±90° and ±180° without
/// wrapping: the fuzzy-area tests compare plain degrees, so no node across the
/// antimeridian covers the line.
fn grown_rect(bounds: LatLngBox, margin_m: f64) -> s2::rect::Rect {
    let LatLngBox(lat_lo, lng_lo, lat_hi, lng_hi) = bounds;
    let dlat = margin_m / METRES_PER_DEGREE;
    let (lat_lo, lat_hi) = ((lat_lo - dlat).max(-90.0), (lat_hi + dlat).min(90.0));
    let metres_per_lng = METRES_PER_DEGREE * lat_lo.abs().max(lat_hi.abs()).to_radians().cos();
    let dlng = if metres_per_lng > margin_m / 360.0 { margin_m / metres_per_lng } else { 360.0 };
    s2::rect::Rect::from_degrees(lat_lo, (lng_lo - dlng).max(-180.0), lat_hi, (lng_hi + dlng).min(180.0))
}

/// The cells of `level` meeting any of `rects`, ascending: one covering of
/// their union, so a cell under several of them is tested once.
fn cells_under_rects(rects: Vec<s2::rect::Rect>, level: u64) -> Vec<u64> {
    if rects.is_empty() {
        return Vec::new();
    }

    let coverer = s2::region::RegionCoverer { min_level: level as u8, max_level: level as u8, level_mod: 1, max_cells: usize::MAX };
    coverer.covering(&RectUnion::new(rects)).0.into_iter().map(|c| c.0).collect()
}

/// A union of lat/lng rects as an S2 region.
struct RectUnion {
    rects: Vec<s2::rect::Rect>,
    bound: s2::rect::Rect,
}

impl RectUnion {
    fn new(rects: Vec<s2::rect::Rect>) -> Self {
        let bound = rects.iter().fold(s2::rect::Rect::empty(), |b, r| b.union(r));
        RectUnion { rects, bound }
    }
}

impl Region for RectUnion {
    fn cap_bound(&self) -> s2::cap::Cap {
        self.bound.cap_bound()
    }

    fn rect_bound(&self) -> s2::rect::Rect {
        self.bound.clone()
    }

    fn contains_cell(&self, cell: &s2::cell::Cell) -> bool {
        self.rects.iter().any(|r| r.contains_cell(cell))
    }

    /// Whether the cell's lat/lng bounds meet one of the rects: true for every
    /// cell that meets one, and for a few cells just outside them, whose
    /// candidates the fuzzy-area test then leaves out.
    fn intersects_cell(&self, cell: &s2::cell::Cell) -> bool {
        let bound = cell.rect_bound();
        self.rects.iter().any(|r| r.intersects(&bound))
    }
}

/// Whether the place_node_fuzzy_area of a node at (centre_lat, centre_lng)
/// with this half side in metres holds the point.
pub fn fuzzy_area_holds(centre_lat: f32, centre_lng: f32, half_m: f64, lat: f64, lng: f64) -> bool {
    let (lat0, lng0) = (centre_lat as f64, centre_lng as f64);
    let dlat_m = ((lat - lat0) * METRES_PER_DEGREE).abs();
    let dlng_m = ((lng - lng0) * METRES_PER_DEGREE * lat0.to_radians().cos()).abs();
    dlat_m <= half_m && dlng_m <= half_m
}

/// A street answer's [state, region, province]: the parent chain's, unless
/// the query point names others; then the boundaries at the street's point
/// nearest the query (`at_street`), the chain's where those have none.
pub fn coarse_fields<T: PartialEq>(chain: [Option<T>; 3], query: [Option<T>; 3],
                                   at_street: impl FnOnce() -> Option<[Option<T>; 3]>) -> [Option<T>; 3] {
    if chain == query {
        return chain;
    }
    let Some([a0, a1, a2]) = at_street() else { return chain; };
    let [c0, c1, c2] = chain;
    [a0.or(c0), a1.or(c1), a2.or(c2)]
}

/// rank_address of each PlaceNode type (0 city, 1 town, 2 village, 3 suburb,
/// 4 hamlet, 5 neighbourhood, 6 quarter), from address-levels.json: quarter
/// (22) and neighbourhood (24) are ranks of their own.
pub const PLACE_NODE_RANK: [u8; 7] = [16, 16, 16, 20, 20, 24, 22];

/// rank_search of each PlaceNode type, from address-levels.json (no country there sets one of
/// these types): a town 18, a village and a suburb 19, a quarter 20.
pub const PLACE_NODE_SEARCH_RANK: [u8; 7] = [16, 18, 19, 19, 20, 24, 20];

/// insert_addresslines' weight on a place node's distance by its
/// rank_search (placex_triggers.sql:597-605): city (16) 0.25, town (18) 0.5,
/// all others 1. (A capital, rank_search 15, weighs 0.2 there; place nodes
/// here carry no capital flag.)
pub const PLACE_NODE_DISTANCE_WEIGHT: [f64; 7] = [0.25, 0.5, 1.0, 1.0, 1.0, 1.0, 1.0];

/// What place nodes are matched against (getNearFeatures' `feature`): the
/// street an answer's address hangs from, Nominatim computing a street's
/// address lines on its whole geometry (placex_triggers.sql:1216-1220), or
/// the query point where the answer has no street.
#[derive(Clone, Copy)]
pub enum PlaceFeature<'a> {
    Point(f64, f64),
    Line(&'a [NodeCoord]),
}

/// Whether a line crosses the place_node_fuzzy_area of a node at
/// (centre_lat, centre_lng) along a stretch of positive length: getNearFeatures
/// takes an area for a line feature when ST_NPoints(ST_Intersection) > 1
/// (partition-functions.sql:44), so a line only touching it doesn't count.
pub fn line_crosses_fuzzy_area(line: &[NodeCoord], centre_lat: f32, centre_lng: f32, half_m: f64) -> bool {
    let (lat0, lng0) = (centre_lat as f64, centre_lng as f64);
    let m_per_lng = METRES_PER_DEGREE * lat0.to_radians().cos();
    let to_xy = |p: &NodeCoord| ((p.lng as f64 - lng0) * m_per_lng, (p.lat as f64 - lat0) * METRES_PER_DEGREE);
    line.windows(2).any(|w| {
        let ((x0, y0), (x1, y1)) = (to_xy(&w[0]), to_xy(&w[1]));
        let (dx, dy) = (x1 - x0, y1 - y0);
        // Liang-Barsky: clip the segment's parameter range to each side.
        let (mut t0, mut t1) = (0.0_f64, 1.0_f64);
        for (p, q) in [(-dx, x0 + half_m), (dx, half_m - x0), (-dy, y0 + half_m), (dy, half_m - y0)] {
            if p == 0.0 {
                if q < 0.0 { return false; }
            } else if p < 0.0 {
                t0 = t0.max(q / p);
            } else {
                t1 = t1.min(q / p);
            }
        }
        t1 > t0
    })
}

/// The squared distance of a place node whose place_node_fuzzy_area covers
/// the feature, None when it doesn't: Nominatim stores the node with that
/// area in location_area_large, and getNearFeatures takes only the rows
/// covering the feature (partition-functions.sql:39-44), each at its
/// distance from the feature (partition-functions.sql:32). That distance is
/// ST_Distance on lon/lat geometry, planar in degrees, so a longitude degree
/// counts as much as a latitude degree at any latitude; only the fuzzy area
/// is measured in metres.
pub fn place_node_covering(pn: &PlaceNode, feature: PlaceFeature<'_>) -> Option<f64> {
    let pt = pn.place_type as usize;
    if pt >= PLACE_NODE_RANK.len() {
        return None;
    }

    let half_m = place_fuzzy_half_side_m(pt);
    match feature {
        PlaceFeature::Point(lat, lng) => fuzzy_area_holds(pn.lat, pn.lng, half_m, lat, lng)
            .then(|| dist_sq((lat - pn.lat as f64).to_radians(), (lng - pn.lng as f64).to_radians(), PLANAR)),
        PlaceFeature::Line(line) => line_crosses_fuzzy_area(line, pn.lat, pn.lng, half_m).then(|| {
            line.windows(2)
                .map(|w| point_to_segment_distance(pn.lat as f64, pn.lng as f64, w[0].lat as f64, w[0].lng as f64,
                                                   w[1].lat as f64, w[1].lng as f64, PLANAR))
                .fold(f64::MAX, f64::min)
        }),
    }
}

/// Nominatim's address walk over place nodes (insert_addresslines,
/// placex_triggers.sql:596-670). `nodes` holds every place node covering the
/// point as (PlaceNode type, rank_address, squared distance, lat, lng). The
/// walk takes them by rank_address, then by distance times
/// PLACE_NODE_DISTANCE_WEIGHT at rank 16, and passes over a node outside the
/// current boundary of its rank (`in_boundary(index)`) or outside the last
/// accepted node's place_node_fuzzy_area; the first that passes is its
/// rank's one entry (address_havelevel). Without the fuzzy-area cascade,
/// spurious sub-city place nodes (White House "The West End", SF "Hayes
/// Valley") leak in where Nominatim's walk rejects them. Boundaries come
/// first within a rank (isguess asc, placex_triggers.sql:599): a rank in
/// `held_ranks` (a bit per rank) takes no node, and a boundary the walk
/// passes after the last accepted node clears its fuzzy area
/// (current_node_area := NULL, :651). Returns the accepted node's index per
/// type, of the highest rank when a type has entries at two: the address
/// keeps a label's first row, finest first (v1/format_json.py:31-34).
pub fn accept_place_nodes(nodes: &[(usize, u8, f64, f32, f32)], held_ranks: u32, in_boundary: impl FnMut(usize) -> bool) -> [Option<usize>; 7] {
    place_node_walk(nodes, held_ranks, in_boundary, false).0
}

/// accept_place_nodes' walk, and with `rows` the nodes it writes an address
/// row for (placex_triggers.sql:666-669), in walk order: each node inside its
/// rank's current boundary until one of the rank is accepted, those its
/// fuzzy area cascade turns down among them, and after that, or in a rank a
/// boundary holds, the ones within twice the nearest the rank had (:614-622,
/// place_min_distance, from 1 degree). The nodes accepted are the same
/// either way; without `rows` the walk tests no boundary of a node it can't
/// accept.
pub fn place_node_walk(nodes: &[(usize, u8, f64, f32, f32)], held_ranks: u32, mut in_boundary: impl FnMut(usize) -> bool, rows: bool)
    -> ([Option<usize>; 7], Vec<usize>) {
    let held = |rank: u8| held_ranks & (1 << rank) != 0;
    let rank = |k: usize| nodes[k].1;
    let weighed = |k: usize| {
        let (pt, rank, dist_sq, _, _) = nodes[k];
        dist_sq.sqrt() * if rank == CurrentBoundaries::CITY { PLACE_NODE_DISTANCE_WEIGHT[pt] } else { 1.0 }
    };
    let mut order: Vec<usize> = (0..nodes.len()).filter(|&k| rows || !held(rank(k))).collect();
    // Ties: type, then position, so the walk doesn't depend on record order.
    order.sort_by(|&a, &b| {
        rank(a).cmp(&rank(b))
            .then(weighed(a).total_cmp(&weighed(b)))
            .then(nodes[a].0.cmp(&nodes[b].0))
            .then(nodes[a].3.total_cmp(&nodes[b].3))
            .then(nodes[a].4.total_cmp(&nodes[b].4))
    });

    let mut accepted = [None; 7];
    let mut written = Vec::new();
    // The ranks that have their entry (address_havelevel), and the nearest
    // node each rank had (place_min_distance), in degrees.
    let mut had = held_ranks;
    let mut nearest = [1.0f64; u32::BITS as usize];
    let mut last_rank = 0;
    // The last accepted node's fuzzy area: (lat, lng, half side in metres).
    let mut fuzzy: Option<(f32, f32, f64)> = None;
    for k in order {
        let (pt, r, dist_sq, lat, lng) = nodes[k];
        let first = had & (1 << r) == 0;
        if !first && !rows { continue; }
        if (last_rank + 1..=r).any(held) {
            fuzzy = None;
        }
        let in_fuzzy = || fuzzy.map_or(true, |(lat0, lng0, half_m)| fuzzy_area_holds(lat0, lng0, half_m, lat as f64, lng as f64));
        if rows {
            if !in_boundary(k) { continue; }
            let distance = dist_sq.sqrt();
            let slot = &mut nearest[usize::from(r)];
            if !first && distance > 2.0 * *slot { continue; }
            *slot = slot.min(distance);
            written.push(k);
            if !first || !in_fuzzy() { continue; }
        } else if !in_fuzzy() || !in_boundary(k) {
            continue;
        }

        accepted[pt] = Some(k);
        had |= 1 << r;
        last_rank = r;
        fuzzy = Some((lat, lng, place_fuzzy_half_side_m(pt)));
    }
    (accepted, written)
}

/// The rank_address range of place nodes that a boundary of their own rank
/// promotes (placex_triggers.sql:961-962: rank_address between 16 and 23).
pub const PROMOTABLE_RANKS: std::ops::RangeInclusive<u8> = 16..=23;
/// What the promotion adds to the node's rank_address (placex_triggers.sql:980).
pub const PLACE_NODE_PROMOTION: u8 = 2;

/// A place node's rank_address: its type's, two more when a polygon of
/// `promoting`, (rank, polygon) holding the query point, has that rank
/// and `holds` the node (placex_triggers.sql:961-981): a village in a city
/// boundary ranks 18, a hamlet in a district linked to a suburb 22.
pub fn place_node_rank(pn: &PlaceNode, promoting: &[(u8, AdminPolygon)], mut holds: impl FnMut(&AdminPolygon) -> bool) -> u8 {
    let rank = PLACE_NODE_RANK[pn.place_type as usize];
    let promoted = PROMOTABLE_RANKS.contains(&rank)
        && promoting.iter().any(|(r, poly)| *r == rank && holds(poly));
    if promoted { rank + PLACE_NODE_PROMOTION } else { rank }
}

/// Nominatim's `current_boundary` (find_current_boundaries) for the place
/// nodes find_places gates, one per rank_address in RANKS, and the polygons
/// holding the point that promote place nodes.
#[derive(Clone, Default)]
pub struct CurrentBoundaries {
    pub gates: [Option<AdminPolygon>; 5],
    /// (rank_address, polygon) of the boundaries, and (place rank, polygon)
    /// of the place areas, holding the point at a PROMOTABLE_RANKS rank, of
    /// relations: a closed way's raises no place node.
    pub promoting: Vec<(u8, AdminPolygon)>,
}

impl CurrentBoundaries {
    /// rank_address of place=city, town and village (address-levels.json)
    pub const CITY: u8 = 16;
    /// rank_address of a city, town or village node promoted in a city
    pub const CITY_PART: u8 = Self::CITY + PLACE_NODE_PROMOTION;
    /// rank_address of place=suburb and place=hamlet
    pub const SUBURB: u8 = 20;
    pub const QUARTER: u8 = 22;
    pub const NEIGHBOURHOOD: u8 = 24;
    pub const RANKS: [u8; 5] = [Self::CITY, Self::CITY_PART, Self::SUBURB, Self::QUARTER, Self::NEIGHBOURHOOD];

    pub fn for_rank(&self, rank: u8) -> Option<&AdminPolygon> {
        Self::RANKS.iter().position(|&r| r == rank).and_then(|g| self.gates[g].as_ref())
    }

    /// Each rank's boundary from the boundaries holding the point at their
    /// initial rank_address and the place areas holding it, in the point's
    /// `country`: the nested-boundary promotions give each its rank_address
    /// (an admin_level 7 boundary in a level 6 one linked to a suburb is
    /// ranked 22, not 14), then `walk` takes them as Nominatim's address walk
    /// does, over the ranks of address areas (ADDRESS_AREA_RANKS). A place
    /// area is a row of that walk as a boundary is: the walk makes every row
    /// it takes that is no place node the current_boundary (insert_addresslines,
    /// placex_triggers.sql:622-632), so a place=suburb way in the address
    /// holds the place nodes of finer ranks to itself. `inside(poly, other)`
    /// tells whether poly's point on surface lies in other, `within(poly,
    /// other)` whether poly lies within other.
    pub fn from_ranked<'a>(
        mut ranked: Vec<RankedPoly>,
        place_areas: impl Iterator<Item = &'a AdminPolygon> + Clone,
        country: &str,
        inside: impl Fn(&AdminPolygon, &AdminPolygon) -> bool,
        within: impl Fn(&AdminPolygon, &AdminPolygon) -> bool,
    ) -> Self {
        // The place areas are rows of the walk at their rank_address, as find_admin_in ranks them.
        ranked.extend(place_areas.clone().map(|p| RankedPoly { poly: *p, rank_address: place_area_ranks(p, country).1 }));
        promote_nested_ranks(&mut ranked, place_areas.clone(), country, within);
        ranked.sort_by(|a, b| a.poly.area.total_cmp(&b.poly.area));
        // A place node is raised inside a relation of its rank alone, a boundary of its
        // rank_address or a place area of its own place rank (placex_triggers.sql:984-1004).
        let promoting = ranked.iter().filter(|r| r.poly.admin_level != 15).map(|r| (r.rank_address, r.poly))
            .chain(place_areas.map(|p| (place_area_ranks(p, country).0, *p)))
            .filter(|(rank, poly)| PROMOTABLE_RANKS.contains(rank) && !poly.of_way)
            .collect();
        let gating = ranked.into_iter()
            .filter(|r| ADDRESS_AREA_RANKS.contains(&r.rank_address) && !matches!(r.rank_address, 5 | 11));
        Self { promoting, ..Self::walk(gating.map(|r| (r.rank_address, r.poly)), inside) }
    }

    /// Each rank's boundary as Nominatim's address walk (insert_addresslines)
    /// leaves it, from (rank_address, polygon) candidates holding the point:
    /// taken by rank, the smallest first within a rank, a boundary is accepted
    /// when none is yet at its rank and its centroid lies in the boundary
    /// accepted before it. A rank's boundary is the last accepted at or below
    /// it. A boundary smaller than the current one is taken to lie in it, as
    /// nested boundaries do; one as large or larger is tested (`inside(
    /// boundary, current)`), so it gates only when it is round the current
    /// one (a suburb-linked boundary round a city-linked one), not beside it.
    /// Simplified polygons overlap at shared borders, and testing a smaller
    /// boundary there would reject the one the point is really in.
    pub fn walk(
        candidates: impl IntoIterator<Item = (u8, AdminPolygon)>,
        inside: impl Fn(&AdminPolygon, &AdminPolygon) -> bool,
    ) -> Self {
        let mut candidates: Vec<(u8, AdminPolygon)> = candidates.into_iter().collect();
        candidates.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.area.total_cmp(&b.1.area)));
        let mut found = Self::default();
        let mut current: Option<(u8, AdminPolygon)> = None;
        for (rank, poly) in candidates {
            if let Some((current_rank, current_poly)) = &current {
                if *current_rank == rank || (poly.area >= current_poly.area && !inside(&poly, current_poly)) { continue; }
            }
            current = Some((rank, poly));
            for (slot, gate) in found.gates.iter_mut().zip(Self::RANKS) {
                if rank <= gate { *slot = Some(poly); }
            }
        }
        found
    }
}

#[derive(Default)]
pub struct PlaceResult<'a> {
    pub city: Option<Cow<'a, str>>,
    pub town: Option<Cow<'a, str>>,
    pub village: Option<Cow<'a, str>>,
    pub hamlet: Option<Cow<'a, str>>,
    pub suburb: Option<Cow<'a, str>>,
    pub quarter: Option<Cow<'a, str>>,
    pub neighbourhood: Option<Cow<'a, str>>,
    /// The rank_address of the node behind each field.
    pub ranks: FieldRanks,
    /// The address rows of the nodes of the name find_places was asked for
    /// (place_node_walk).
    pub named_rows: Vec<NamedRow>,
}

/// A place node's address row: its rank_address, its field, its distance from
/// the feature in degrees, and the node.
#[derive(Clone, Copy)]
pub struct NamedRow {
    pub rank: u8,
    pub field: &'static str,
    pub distance: f64,
    pub node: PlaceNode,
}

impl PlaceResult<'_> {
    /// Whether no place node fills a field.
    pub fn is_empty(&self) -> bool {
        [&self.city, &self.town, &self.village, &self.hamlet, &self.suburb, &self.quarter, &self.neighbourhood]
            .iter().all(|f| f.is_none())
    }
}

#[derive(Default)]
pub struct AdminResult<'a> {
    pub country: Option<Cow<'a, str>>,
    pub country_code: Option<[u8; 2]>,
    pub state: Option<Cow<'a, str>>,
    pub province: Option<Cow<'a, str>>,
    pub region: Option<Cow<'a, str>>,
    pub state_district: Option<Cow<'a, str>>,
    pub county: Option<Cow<'a, str>>,
    pub district: Option<Cow<'a, str>>,
    pub municipality: Option<Cow<'a, str>>,
    pub city: Option<Cow<'a, str>>,
    pub town: Option<Cow<'a, str>>,
    pub village: Option<Cow<'a, str>>,
    pub hamlet: Option<Cow<'a, str>>,
    pub city_district: Option<Cow<'a, str>>,
    pub borough: Option<Cow<'a, str>>,
    pub suburb: Option<Cow<'a, str>>,
    pub quarter: Option<Cow<'a, str>>,
    pub neighbourhood: Option<Cow<'a, str>>,
    pub city_block: Option<Cow<'a, str>>,
    /// rank_address of the boundaries in the address, a bit per rank: the
    /// ranks they take before any place node (address_havelevel).
    pub boundary_ranks: u32,
    /// The rank_address of the polygon behind each field.
    pub ranks: FieldRanks,
    /// The postcode of the eligible postal area (boundary=postal_code) holding the point
    /// whose centroid is nearest, with ties going to the smaller area.
    pub postcode_area: Option<Cow<'a, str>>,
    /// The polygons in the address that fill no field (unlabelled_rows), as
    /// (rank_address, name), the finest first: display_name lists them.
    pub unlabelled: Vec<(u8, Cow<'a, str>)>,
    /// The polygons of the address's rows with their rank_address
    /// (find_admin_in's ranked polygons), those that fill no field or part of
    /// display_name among them.
    pub rows: Vec<RankedPoly>,
}

impl AdminResult<'_> {
    /// Whether no polygon fills a field.
    pub fn is_empty(&self) -> bool {
        [&self.country, &self.state, &self.province, &self.region, &self.state_district, &self.county,
         &self.district, &self.municipality, &self.city, &self.town, &self.village, &self.hamlet,
         &self.city_district, &self.borough, &self.suburb, &self.quarter, &self.neighbourhood, &self.city_block]
            .iter().all(|f| f.is_none())
    }
}

/// The field a polygon of an address fills: its place type's, else its
/// rank's (rank_to_field).
pub fn polygon_field(r: &RankedPoly) -> Option<&'static str> {
    if r.poly.place_type_override > 0 {
        place_type_to_field(r.poly.place_type_override)
    } else {
        rank_to_field(r.rank_address)
    }
}

/// A country code as postal_area_postcode takes it: upper case, "" where unknown.
fn upper_country(country_code: Option<[u8; 2]>) -> String {
    country_code.map_or_else(String::new, |c| c.iter().map(|b| b.to_ascii_uppercase() as char).collect())
}

/// The rank_address bits of the polygons in an address (AdminResult::boundary_ranks).
pub fn rank_bits(ranked: &[RankedPoly]) -> u32 {
    ranked.iter()
        .filter(|r| (1..u32::BITS as u8).contains(&r.rank_address))
        .fold(0, |bits, r| bits | 1 << r.rank_address)
}

/// Of the boundaries of `ranked` (admin_level below 15) of one rank_address, the smallest, the
/// first of equals, the one find_current_boundaries' walk takes at that rank: Nominatim's address
/// has a row a rank (insert_addresslines: address_havelevel), and only its rows fill a field or
/// join display_name (format_json.py, results.py). The others go; the place areas stay.
pub fn one_boundary_a_rank(ranked: &mut Vec<RankedPoly>) {
    let mut kept: [Option<usize>; u32::BITS as usize] = [None; u32::BITS as usize];
    for (k, r) in ranked.iter().enumerate() {
        let rank = r.rank_address as usize;
        if r.poly.admin_level == 15 || rank >= kept.len() { continue; }
        if kept[rank].is_none_or(|b| r.poly.area < ranked[b].poly.area) {
            kept[rank] = Some(k);
        }
    }
    let mut k = 0;
    ranked.retain(|r| {
        let rank = r.rank_address as usize;
        let keep = r.poly.admin_level == 15 || rank >= kept.len() || kept[rank] == Some(k);
        k += 1;
        keep
    });
}

/// The polygons of `ranked` that fill no field but are rows of the address all the same:
/// Nominatim's address has a row a rank and keeps the first row of each label, and its
/// display_name joins every row (format_json.py, results.py). `slots[k]` is the field ranked[k]
/// would fill and `winners[f]` the polygon filling field f. Of each rank no field's polygon has,
/// the smallest polygon that lost its field, unless it has the outline of the one that took it
/// (`same_outline`): that is one place tagged twice, as a boundary repeating its parent's
/// wikidata and geometry, which Nominatim takes out of addresses (placex_triggers.sql:884-899);
/// the records hold no wikidata. Indices into `ranked`, the finest rank first.
pub fn unlabelled_rows(ranked: &[RankedPoly], slots: &[Option<usize>], winners: &[Option<usize>], same_outline: impl Fn(&AdminPolygon, &AdminPolygon) -> bool) -> Vec<usize> {
    let held = winners.iter().flatten().fold(0u32, |bits, &w| bits | 1u32.checked_shl(ranked[w].rank_address as u32).unwrap_or(0));
    let mut by_rank: [Option<usize>; u32::BITS as usize] = [None; u32::BITS as usize];
    for (k, (r, slot)) in ranked.iter().zip(slots).enumerate() {
        let Some(winner) = slot.and_then(|f| winners[f]) else { continue };
        let rank = r.rank_address as usize;
        if winner == k || rank >= by_rank.len() || held & 1 << rank != 0 || same_outline(&ranked[winner].poly, &r.poly) { continue; }
        if by_rank[rank].is_none_or(|b| r.poly.area < ranked[b].poly.area) {
            by_rank[rank] = Some(k);
        }
    }
    by_rank.into_iter().rev().flatten().collect()
}

/// A field of the address a boundary of rank `boundary_rank` and a place node of rank `node_rank`
/// may both fill: Nominatim's address keeps the first row of a label, its rows the finest first
/// (format_json.py, results.py), and of rows of one rank the boundary's (isguess asc,
/// placex_triggers.sql:576). The name filling it with its rank, and the boundary's row, as
/// (rank, name), where the node takes the field: display_name still lists it.
pub fn label_field<'a>(boundary: Option<Cow<'a, str>>, boundary_rank: u8, node: Option<Cow<'a, str>>, node_rank: u8)
    -> (Option<(Cow<'a, str>, u8)>, Option<(u8, Cow<'a, str>)>) {
    match (boundary, node) {
        (Some(b), Some(n)) if node_rank > boundary_rank => (Some((n, node_rank)), Some((boundary_rank, b))),
        (Some(b), _) => (Some((b, boundary_rank)), None),
        (None, n) => (n.map(|n| (n, node_rank)), None),
    }
}

#[derive(Serialize, Default)]
pub struct AddressDetails<'a> {
    /// The rank_address of the polygon or the place node behind each place
    /// field, which display_name lists the parts by.
    #[serde(skip)]
    pub ranks: FieldRanks,
    /// Parts of display_name that fill no field, with their rank_address: a
    /// region of the country's own name (Nominatim's address keeps one row
    /// per label, its display_name every row).
    #[serde(skip)]
    pub unlabelled: Vec<(u8, Cow<'a, str>)>,
    // Landmark field: set to the name of the smallest-area
    // polygon POI that contains the query point. Category details
    // (specific type, distance, …) live in places[], so a single
    // field here is enough to let display_name lead with the
    // landmark name while clients still know the "what" via
    // places[0].category.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub landmark: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub house_number: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub road: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub city: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub town: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub village: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hamlet: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub municipality: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub province: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_district: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub county: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub district: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub borough: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suburb: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quarter: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub city_district: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub neighbourhood: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub city_block: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postcode: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country_code: Option<String>,
}

/// The place parts of display_name, each field with the rank_address of its
/// label in Nominatim's settings/address-levels.json, which a part without a
/// known rank takes (format_address), finest first.
const PLACE_PARTS: [(&str, u8); 17] = [
    ("city_block", 25), ("neighbourhood", 24), ("quarter", 22), ("hamlet", 20), ("suburb", 20), ("borough", 18),
    ("city", 16), ("city_district", 16), ("town", 16), ("village", 16), ("municipality", 14), ("district", 12),
    ("county", 12), ("state", 8), ("state_district", 6), ("region", 6), ("province", 6),
];

impl<'a> AddressDetails<'a> {
    /// The place part `field` of PLACE_PARTS; None for another field.
    fn place_part(&mut self, field: &str) -> Option<&mut Option<Cow<'a, str>>> {
        Some(match field {
            "city_block" => &mut self.city_block,
            "neighbourhood" => &mut self.neighbourhood,
            "quarter" => &mut self.quarter,
            "hamlet" => &mut self.hamlet,
            "suburb" => &mut self.suburb,
            "borough" => &mut self.borough,
            "city" => &mut self.city,
            "city_district" => &mut self.city_district,
            "town" => &mut self.town,
            "village" => &mut self.village,
            "municipality" => &mut self.municipality,
            "district" => &mut self.district,
            "county" => &mut self.county,
            "state" => &mut self.state,
            "state_district" => &mut self.state_district,
            "region" => &mut self.region,
            "province" => &mut self.province,
            _ => return None,
        })
    }

    /// The rank of place part `field` as display_name lists it: its polygon's
    /// or its node's, else its label's.
    fn part_rank(&self, field: &str) -> u8 {
        match self.ranks.get(field) {
            0 => PLACE_PARTS.iter().find(|(f, _)| *f == field).map_or(0, |&(_, rank)| rank),
            rank => rank,
        }
    }

    /// The place parts that fill a field, as (rank, name), in PLACE_PARTS' order.
    fn place_parts(&self) -> impl Iterator<Item = (u8, &str)> + '_ {
        self.labelled_parts().map(|(_, rank, name)| (rank, name))
    }

    /// The place parts that fill a field, as (field, rank, name), in PLACE_PARTS' order.
    fn labelled_parts(&self) -> impl Iterator<Item = (&'static str, u8, &str)> + '_ {
        let value = |field: &str| match field {
            "city_block" => &self.city_block,
            "neighbourhood" => &self.neighbourhood,
            "quarter" => &self.quarter,
            "hamlet" => &self.hamlet,
            "suburb" => &self.suburb,
            "borough" => &self.borough,
            "city" => &self.city,
            "city_district" => &self.city_district,
            "town" => &self.town,
            "village" => &self.village,
            "municipality" => &self.municipality,
            "district" => &self.district,
            "county" => &self.county,
            "state" => &self.state,
            "state_district" => &self.state_district,
            "region" => &self.region,
            _ => &self.province,
        };
        PLACE_PARTS.iter().filter_map(move |&(field, _)| value(field).as_deref().map(|v| (field, self.part_rank(field), v)))
    }

    /// Nominatim's crosscheck of an answer's address values (results.py:610-632):
    /// CrosscheckNames puts a row named by one of them first in its rank, and
    /// only a rank's first row is a part. The first of each rank's rows of
    /// `rows`, the rows named by a value in Nominatim's order within a rank,
    /// takes the place of the rank's parts unless it is one of them (a part
    /// of its rank, name and field, or of no field): in its field (rank and
    /// all) where that is free or holds a coarser part, which then names no
    /// field, else as a part of no field. A field the rank's parts leave goes
    /// to the finest part of no field that fills it, one of `areas` (the
    /// address's polygons as rows) of a rank no field's part has, as a label's
    /// first row is its field (v1/format_json.py:31-34). A row of another
    /// field (the country) is left out.
    pub fn crosscheck(&mut self, rows: &[CrosscheckRow<'a>], areas: &[CrosscheckRow<'a>]) {
        let mut done = 0u32;
        for row in rows {
            if row.field.is_some_and(|field| !PLACE_PARTS.iter().any(|(part, _)| *part == field)) { continue; }
            let bit = 1u32.checked_shl(u32::from(row.rank)).unwrap_or(0);
            if done & bit != 0 { continue; }
            done |= bit;
            let shown = self.labelled_parts().any(|(field, rank, name)| rank == row.rank && name == row.name && row.field == Some(field))
                || self.unlabelled.iter().any(|(rank, name)| *rank == row.rank && *name == row.name);
            if shown { continue; }
            let mut freed = Vec::new();
            for (field, _) in PLACE_PARTS {
                if self.part_rank(field) == row.rank {
                    if let Some(part) = self.place_part(field) {
                        if part.take().is_some() { freed.push(field); }
                    }
                    self.ranks.set(field, 0);
                }
            }
            self.unlabelled.retain(|(rank, _)| *rank != row.rank);
            let name = row.name.clone();
            match row.field {
                Some(field) => {
                    let rank = self.part_rank(field);
                    let Some(part) = self.place_part(field) else { continue };
                    if part.is_none() || rank < row.rank {
                        let coarser = part.replace(name);
                        self.ranks.set(field, row.rank);
                        if let Some(coarser) = coarser { self.unlabelled.push((rank, coarser)); }
                    } else {
                        self.unlabelled.push((row.rank, name));
                    }
                }
                None => self.unlabelled.push((row.rank, name)),
            }
            for field in freed {
                if self.place_part(field).is_some_and(|part| part.is_some()) { continue; }
                let labelled: Vec<u8> = self.labelled_parts().map(|(_, rank, _)| rank).collect();
                let finest = self.unlabelled.iter().enumerate()
                    .filter(|(_, (rank, name))| !labelled.contains(rank)
                        && areas.iter().any(|a| a.rank == *rank && a.name == *name && a.field == Some(field)))
                    .max_by_key(|(_, (rank, _))| *rank)
                    .map(|(k, _)| k);
                if let Some(k) = finest {
                    let (rank, name) = self.unlabelled.remove(k);
                    if let Some(part) = self.place_part(field) { *part = Some(name); }
                    self.ranks.set(field, rank);
                }
            }
        }
    }
}

/// A row of an address that a crosscheck may make its rank's part (AddressDetails::crosscheck):
/// its rank_address, its name (a polygon's own, where a node it links has the value's) and the
/// field it fills, none for a polygon that fills none.
pub struct CrosscheckRow<'a> {
    pub rank: u8,
    pub name: Cow<'a, str>,
    pub field: Option<&'static str>,
}

#[derive(Serialize)]
pub struct PoiDetail {
    pub name: String,
    pub category: String,
    pub distance_m: f64,
}

/// The licence of every answer, Nominatim's (src/nominatim_api/v1/
/// classtypes.py OSM_ATTRIBUTION): its json and jsonv2 results carry it
/// first after their place_id (v1/format_json.py format_base_json), and
/// these answers have no place_id.
pub const OSM_LICENCE: &str = "Data © OpenStreetMap contributors, ODbL 1.0. http://osm.org/copyright";

/// An answer's `licence`, OSM_LICENCE.
#[derive(Clone, Copy, Default)]
pub struct Licence;

impl Serialize for Licence {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(OSM_LICENCE)
    }
}

/// OpenStreetMap's entry of the builder's credits (builder/src/attribution.h kAttribution), the
/// credit of a dataset whose dir holds no attribution.json.
pub fn osm_credit() -> serde_json::Value {
    serde_json::json!({
        "id": "osm",
        "text": "© OpenStreetMap contributors",
        "url": "https://www.openstreetmap.org/copyright",
        "licence": "ODbL 1.0",
        "licence_url": "https://opendatacommons.org/licenses/odbl/1-0/",
        "notices": ["This dataset is a derivative database of OpenStreetMap, available under the ODbL 1.0."],
    })
}

/// The credit of the data in dataset dir `dir`: the entries of its attribution.json and
/// poi_attribution.json, as the builder writes them; OpenStreetMap's alone in place of an
/// attribution.json the dir holds none of, or one that is no list.
pub fn dir_credits(dir: &str) -> Vec<serde_json::Value> {
    let read = |name: &str| -> Option<Vec<serde_json::Value>> {
        serde_json::from_str(&std::fs::read_to_string(format!("{dir}/{name}")).ok()?).ok()
    };
    let mut credits = read("attribution.json").unwrap_or_else(|| vec![osm_credit()]);
    credits.extend(read("poi_attribution.json").unwrap_or_default());
    credits
}

/// `credits` as query-server's /test/credits gives them (main.rs): a disclosure whose summary
/// names each source, and a paragraph of each, its text linked to its URL and its licence to the
/// licence's, then its notices, all as text. A URL of no http(s) scheme links nothing. No
/// credits, no disclosure.
pub fn credits_html(credits: &[serde_json::Value]) -> String {
    fn escape(s: &str) -> String {
        s.chars().fold(String::with_capacity(s.len()), |mut out, c| {
            match c {
                '&' => out.push_str("&amp;"),
                '<' => out.push_str("&lt;"),
                '>' => out.push_str("&gt;"),
                '"' => out.push_str("&quot;"),
                '\'' => out.push_str("&#39;"),
                _ => out.push(c),
            }
            out
        })
    }
    fn link(url: &str, label: &str) -> String {
        if url.starts_with("https://") || url.starts_with("http://") {
            format!(r#"<a href="{}" target="_blank" rel="noopener">{}</a>"#, escape(url), escape(label))
        } else {
            escape(label)
        }
    }
    if credits.is_empty() {
        return String::new();
    }
    let field = |e: &serde_json::Value, key: &str| e[key].as_str().unwrap_or("").to_owned();
    let names: Vec<String> = credits.iter().map(|e| escape(&field(e, "text"))).collect();
    let mut out = format!("<details><summary>Data: {}</summary>\n", names.join(", "));
    for e in credits {
        out += "<p>";
        out += &link(&field(e, "url"), &field(e, "text"));
        let licence = field(e, "licence");
        if !licence.is_empty() {
            out += ", ";
            out += &link(&field(e, "licence_url"), &licence);
        }
        out += ".";
        for notice in e["notices"].as_array().into_iter().flatten().filter_map(|n| n.as_str()) {
            out += " ";
            out += &escape(notice);
        }
        out += "</p>\n";
    }
    out + "</details>\n"
}

#[derive(Serialize, Default)]
pub struct Address<'a> {
    pub licence: Licence,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub address: AddressDetails<'a>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub places: Vec<PoiDetail>,
    /// The body of water holding the point, from the dataset's water dir (water.rs): an
    /// extension of Nominatim's answer, which no other field depends on. Last, and absent where
    /// no water holds the point, so an answer without water is the answer of a dataset without
    /// a water dir.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub water: Option<WaterDetail<'a>>,
}

// Address formatting patterns by country code
// Returns (number_after_street, postcode_before_city, include_state)
pub fn format_rules(country_code: Option<&str>) -> (bool, bool, bool) {
    match country_code {
        // Number before street, postcode after city, include state
        Some("US") | Some("CA") | Some("AU") | Some("NZ")
        | Some("GB") | Some("IE") | Some("ZA") | Some("IN")
        | Some("NG") | Some("KE") | Some("GH") | Some("PK")
        | Some("PH") | Some("TH") | Some("MY") => (false, false, true),

        // Number before street, postcode before city, include state
        Some("JP") | Some("KR") | Some("CN") | Some("TW") => (false, true, true),

        // Number after street, postcode before city, no state (most of Europe, etc.)
        _ => (true, true, false),
    }
}

// Build display_name matching Nominatim's `BaseResult.display_name`
// (`src/nominatim_api/results.py`): the address rows sorted by rank_address
// descending (fine → coarse), joined, and a row left out only where its name
// repeats the part just before it. The landmark (the result's own name)
// leads, then the house number and road; the postcode (rank 5) and the
// country (rank 4) close it, as _finalize_entry adds them.
//
// A place part's rank is the rank_address of the polygon or the place node
// behind its field (AddressDetails::ranks): a village node inside a township
// of its own rank is promoted to 18 and comes before the township's 16. A
// field without a known rank takes that of its label in Nominatim's
// settings/address-levels.json, and parts of one rank keep this order:
//
//   25 = city_block
//   24 = neighbourhood
//   22 = quarter
//   20 = hamlet, suburb
//   18 = borough
//   16 = city, city_district, town, village
//   14 = municipality
//   12 = district, county
//    8 = state
//    6 = state_district, region, province
//
// Notes:
// - place=state/province/region all have rank_address=0 in Nominatim
//   (excluded from the address chain at the place-tag level). State
//   comes from boundary=administrative4, which is rank 8.
// - Our AddressDetails fields `region`, `province`, `state_district`
//   are populated by our rank_to_field fallback for admin levels that
//   don't map to a canonical Nominatim field; without a rank they sit
//   between state and country, as Nominatim's admin_level=3 (rank 6).
// - Every answer with a part has a display_name, as every one of
//   Nominatim's with an address row: a state and a county without a
//   locality, a road or a country too.
pub fn format_address(addr: &AddressDetails<'_>) -> Option<String> {
    let (number_after, _, _) = format_rules(addr.country_code.as_deref());

    // The house number and the road, one part.
    let road = addr.road.as_ref().map(|road| match &addr.house_number {
        Some(hn) if number_after => format!("{} {}", road, hn),
        Some(hn) => format!("{} {}", hn, road),
        None => road.to_string(),
    });

    // The place parts, finest first: a stable sort keeps PLACE_PARTS' order
    // (the one above) among parts of one rank.
    let mut places: Vec<(u8, &str)> = addr.place_parts().collect();
    // A part without a field is a row of a rank no field's part has: an address has a row a rank.
    let labelled = places.len();
    for (rank, v) in &addr.unlabelled {
        if !places[..labelled].iter().any(|&(r, _)| r == *rank) {
            places.push((*rank, v));
        }
    }
    places.sort_by(|a, b| b.0.cmp(&a.0));

    let mut parts: Vec<&str> = Vec::new();
    let line = addr.landmark.as_deref().into_iter()
        .chain(road.as_deref())
        .chain(places.iter().map(|&(_, v)| v))
        .chain(addr.postcode.as_deref())
        .chain(addr.country.as_deref());
    for part in line {
        if part.is_empty() || parts.last() == Some(&part) { continue; }
        parts.push(part);
    }

    if parts.is_empty() { None } else { Some(parts.join(", ")) }
}

// --- Pure-helper regression tests -------------------------------------------
//
// These lock in the *current* output of the pure helper functions so a
// refactor can't silently change Nominatim-parity results.  They are a
// regression net, NOT a spec: where current behaviour looks surprising
// it's still asserted as-is with a `// NOTE:` explaining why.
#[cfg(test)]
mod pure_helper_tests {
    use super::*;

    #[test]
    fn coords_in_range_accepts_the_globe_only() {
        for (lat, lng) in [(0.0, 0.0), (90.0, 180.0), (-90.0, -180.0), (48.85, 2.35)] {
            assert!(coords_in_range(lat, lng), "({}, {})", lat, lng);
        }
        for (lat, lng) in [(91.0, 0.0), (-90.5, 0.0), (0.0, 180.1), (0.0, -181.0), (f64::NAN, 0.0), (0.0, f64::INFINITY)] {
            assert!(!coords_in_range(lat, lng), "({}, {})", lat, lng);
        }
    }

    // -- nearest postcode ------------------------------------------------------

    #[test]
    fn nearer_postcode_orders_ties_by_postcode() {
        assert!(nearer_postcode(1.0, "60680", None));
        assert!(nearer_postcode(1.0, "60680", Some((2.0, "60454"))));
        assert!(!nearer_postcode(3.0, "60454", Some((2.0, "60680"))));
        // GeoNames puts many PO-box ZIPs at one post office: equal distance.
        assert!(nearer_postcode(2.0, "60454", Some((2.0, "60680"))));
        assert!(!nearer_postcode(2.0, "60680", Some((2.0, "60454"))));
    }

    // -- place areas ----------------------------------------------------------

    #[test]
    fn place_area_repeats_only_a_namesake_of_its_rank_no_larger() {
        // found (name, rank, area); the place area's name, rank, area; repeats
        let cases = [
            (("Singapore", 4, 0.1155), "Singapore", 16, 0.1155, false), // the country around the city area
            (("Mitte", 20, 0.5), "Mitte", 20, 0.7, true),
            (("Mitte", 20, 0.7), "Mitte", 20, 0.7, true),
            (("Mitte", 20, 0.9), "Mitte", 20, 0.7, false), // a larger namesake doesn't hide it
            (("Wedding", 20, 0.5), "Mitte", 20, 0.7, false),
        ];
        for (found, name, rank, area, repeats) in cases {
            assert_eq!(place_area_repeats([found], name, rank, area), repeats, "{found:?} {name} {rank}");
        }
    }

    // -- POI ordering ---------------------------------------------------------

    fn poi<'a>(name: &'a str, category_id: u8, contained: bool, score: f64, area: f64) -> PoiMatch<'a> {
        PoiMatch { name: Cow::Borrowed(name), category: "", category_id, distance_m: 0.0, contained, is_point: !contained, score, area }
    }

    fn sorted_names(mut pois: Vec<PoiMatch<'_>>) -> Vec<String> {
        pois.sort_by(poi_order);
        pois.iter().map(|p| p.name.to_string()).collect()
    }

    // Record order differs between a fresh and a chained build of the
    // same data, so ties must not fall back to it (DC: the landmark
    // flipped between National Mall and Henry Park).
    #[test]
    fn poi_order_breaks_ties_independent_of_record_order() {
        let a = || vec![poi("National Mall", 10, true, 1.0, 2.5), poi("Henry Park", 11, true, 1.0, 2.5)];
        let b = || vec![poi("Henry Park", 11, true, 1.0, 2.5), poi("National Mall", 10, true, 1.0, 2.5)];
        assert_eq!(sorted_names(a()), sorted_names(b()));

        let c = || vec![poi("소프소프", 4, false, 3.0, 0.0), poi("소프청담(SOP)", 4, false, 3.0, 0.0)];
        let d = || vec![poi("소프청담(SOP)", 4, false, 3.0, 0.0), poi("소프소프", 4, false, 3.0, 0.0)];
        assert_eq!(sorted_names(c()), sorted_names(d()));
    }

    #[test]
    fn poi_order_keeps_primary_keys() {
        let got = sorted_names(vec![
            poi("far", 1, false, 1.0, 0.0),
            poi("big", 1, true, 1.0, 9.0),
            poi("near", 1, false, 5.0, 0.0),
            poi("small", 1, true, 1.0, 1.0),
        ]);
        assert_eq!(got, vec!["small", "big", "near", "far"]);
    }

    // -- postcode formatting / validation ------------------------------------

    fn cc(s: &str) -> [u8; 2] {
        let b = s.as_bytes();
        [b[0], b[1]]
    }

    #[test]
    fn format_postcode_se_gr_inserts_space_at_3() {
        // ddd dd — 5 digits split after the third.
        assert_eq!(format_postcode(&cc("se"), "11422"), "114 22");
        assert_eq!(format_postcode(&cc("gr"), "10431"), "104 31");
        // sk / cz share the same ddd dd rule.
        assert_eq!(format_postcode(&cc("sk"), "81101"), "811 01");
        assert_eq!(format_postcode(&cc("cz"), "11000"), "110 00");
        // Already-spaced input is stripped then reformatted identically.
        assert_eq!(format_postcode(&cc("se"), "114 22"), "114 22");
        // Wrong length -> no match -> original returned verbatim.
        assert_eq!(format_postcode(&cc("se"), "1142"), "1142");
        assert_eq!(format_postcode(&cc("se"), "1142A"), "1142A");
    }

    #[test]
    fn format_postcode_nl_dddd_aa() {
        // dddd AA — uppercased, space inserted after 4th char.
        assert_eq!(format_postcode(&cc("nl"), "1011ab"), "1011 AB");
        assert_eq!(format_postcode(&cc("nl"), "1011 AB"), "1011 AB");
        // Letters where digits expected -> no match.
        assert_eq!(format_postcode(&cc("nl"), "10A1AB"), "10A1AB");
        // Wrong length -> no match.
        assert_eq!(format_postcode(&cc("nl"), "1011A"), "1011A");
    }

    #[test]
    fn format_postcode_ca_ldl_dld() {
        // ldl dld — letter/digit alternating, space after 3rd char.
        assert_eq!(format_postcode(&cc("ca"), "K1A0B1"), "K1A 0B1");
        // Lowercase is uppercased first.
        assert_eq!(format_postcode(&cc("ca"), "k1a0b1"), "K1A 0B1");
        // Pattern mismatch (all digits) -> no match.
        assert_eq!(format_postcode(&cc("ca"), "123456"), "123456");
    }

    #[test]
    fn format_postcode_jp_and_br() {
        // JP: ddd-dddd (7 digits, dash after 3rd).
        assert_eq!(format_postcode(&cc("jp"), "1000001"), "100-0001");
        // BR: ddddd-ddd (8 digits, dash after 5th).
        assert_eq!(format_postcode(&cc("br"), "01310100"), "01310-100");
        // Wrong digit count -> original.
        assert_eq!(format_postcode(&cc("jp"), "100001"), "100001");
    }

    #[test]
    fn format_postcode_us_zip4_truncates_to_base() {
        // US ZIP+4 -> 5-digit base (Nominatim country pattern output \1).
        assert_eq!(format_postcode(&cc("us"), "12345-6789"), "12345");
        assert_eq!(format_postcode(&cc("us"), "10001-2062"), "10001");
        // Plain 5-digit ZIP unchanged.
        assert_eq!(format_postcode(&cc("us"), "12345"), "12345");
        // A dashless 9-digit string does NOT fullmatch Nominatim's pattern, so
        // it is left untouched (not silently truncated).
        assert_eq!(format_postcode(&cc("us"), "123456789"), "123456789");
        // Non-US ddddd-dddd is not a US ZIP+4 — left to its own (or no) rule.
        assert_eq!(format_postcode(&cc("gb"), "12345-6789"), "12345-6789");
    }

    #[test]
    fn format_postcode_uppercases_cc_and_unknown_passthrough() {
        // Country code is lowercased internally, so an uppercase CC still matches.
        assert_eq!(format_postcode(&cc("SE"), "11422"), "114 22");
        // A country with no rule returns the input borrowed, unchanged.
        assert_eq!(format_postcode(&cc("us"), "10001"), "10001");
        assert_eq!(format_postcode(&cc("gb"), "SW1A 1AA"), "SW1A 1AA");
    }

    #[test]
    fn postcode_looks_valid_rejects_placeholders() {
        // All-same digits / chars are placeholders.
        assert!(!postcode_looks_valid(&cc("us"), "000000"));
        assert!(!postcode_looks_valid(&cc("us"), "0000"));
        assert!(!postcode_looks_valid(&cc("us"), "-----"));
        // Empty after stripping whitespace and dashes.
        assert!(!postcode_looks_valid(&cc("us"), ""));
        assert!(!postcode_looks_valid(&cc("us"), "   "));
        assert!(!postcode_looks_valid(&cc("us"), "-"));
        // NOTE: a single repeated char counts as "all same" and is rejected,
        // so a lone "5" is invalid even though it could be a real value.
        assert!(!postcode_looks_valid(&cc("us"), "5"));
        // Normal values pass.
        assert!(postcode_looks_valid(&cc("us"), "10001"));
        assert!(postcode_looks_valid(&cc("se"), "114 22"));
    }

    #[test]
    fn centroid_postcode_ok_rules() {
        assert!(centroid_postcode_ok("10001"));
        assert!(centroid_postcode_ok("SW1A 1AA"));
        // semicolon (multiple values) rejected.
        assert!(!centroid_postcode_ok("10001;10002"));
        // No PO/Box substring filter: legitimate UK Portsmouth-area postcodes
        // (the "PO" postcode area) must NOT be dropped — Nominatim has no such
        // filter. This was a real divergence (Portsmouth returned no postcode).
        assert!(centroid_postcode_ok("PO1 1BA"));
        assert!(centroid_postcode_ok("PO16 7GZ"));
        // The builder keeps a centroid only of the postcode its country's pattern makes, of any
        // length or shape: Iran's ddddd-ddddd and Saudi Arabia's ddddd-dddd.
        assert!(centroid_postcode_ok("12345-67890"));
        assert!(centroid_postcode_ok("12345-6789"));
    }

    #[test]
    fn country_has_no_postcode_membership() {
        // In the no-postcode set.
        assert!(country_has_no_postcode(&cc("ae")));
        assert!(country_has_no_postcode(&cc("qa")));
        assert!(country_has_no_postcode(&cc("zw")));
        // Case-insensitive (uppercased input still matches).
        assert!(country_has_no_postcode(&cc("AE")));
        // Not in the set.
        assert!(!country_has_no_postcode(&cc("us")));
        assert!(!country_has_no_postcode(&cc("gb")));
        assert!(!country_has_no_postcode(&cc("de")));
    }

    // -- enum -> field/rank/class snapshots ----------------------------------

    #[test]
    fn place_type_to_field_snapshot() {
        assert_eq!(place_type_to_field(1), Some("city"));
        assert_eq!(place_type_to_field(2), Some("town"));
        assert_eq!(place_type_to_field(3), Some("village"));
        assert_eq!(place_type_to_field(4), Some("suburb"));
        assert_eq!(place_type_to_field(5), Some("neighbourhood"));
        assert_eq!(place_type_to_field(6), Some("quarter"));
        assert_eq!(place_type_to_field(7), Some("state"));
        assert_eq!(place_type_to_field(8), Some("province"));
        assert_eq!(place_type_to_field(9), Some("region"));
        assert_eq!(place_type_to_field(10), Some("county"));
        assert_eq!(place_type_to_field(11), Some("district"));
        assert_eq!(place_type_to_field(12), Some("borough"));
        assert_eq!(place_type_to_field(13), Some("hamlet"));
        assert_eq!(place_type_to_field(14), Some("municipality"));
        assert_eq!(place_type_to_field(0), None);
        assert_eq!(place_type_to_field(15), None);
        assert_eq!(place_type_to_field(255), None);
    }

    #[test]
    fn label_field_names_the_finer_row_and_the_boundary_at_one_rank() {
        let (parish, suburb) = (|| Some(Cow::Borrowed("Upton")), || Some(Cow::Borrowed("St Crispin's")));
        assert_eq!(label_field(parish(), 20, suburb(), 22), (Some((Cow::Borrowed("St Crispin's"), 22)), Some((20, Cow::Borrowed("Upton")))));
        assert_eq!(label_field(parish(), 22, suburb(), 22), (Some((Cow::Borrowed("Upton"), 22)), None));
        assert_eq!(label_field(parish(), 22, suburb(), 20), (Some((Cow::Borrowed("Upton"), 22)), None));
        assert_eq!(label_field(None, 0, suburb(), 20), (Some((Cow::Borrowed("St Crispin's"), 20)), None));
        assert_eq!(label_field(parish(), 20, None, 0), (Some((Cow::Borrowed("Upton"), 20)), None));
    }

    #[test]
    fn rank_to_field_snapshot() {
        // Below 4 -> None.
        assert_eq!(rank_to_field(0), None);
        assert_eq!(rank_to_field(3), None);
        assert_eq!(rank_to_field(4), Some("country"));
        assert_eq!(rank_to_field(5), Some("country"));
        assert_eq!(rank_to_field(6), Some("region"));
        assert_eq!(rank_to_field(7), Some("region"));
        assert_eq!(rank_to_field(8), Some("state"));
        assert_eq!(rank_to_field(9), Some("state"));
        assert_eq!(rank_to_field(10), Some("state_district"));
        assert_eq!(rank_to_field(11), Some("state_district"));
        assert_eq!(rank_to_field(12), Some("county"));
        assert_eq!(rank_to_field(13), Some("county"));
        assert_eq!(rank_to_field(14), Some("municipality"));
        assert_eq!(rank_to_field(15), Some("municipality"));
        assert_eq!(rank_to_field(16), Some("city"));
        assert_eq!(rank_to_field(17), Some("city"));
        assert_eq!(rank_to_field(18), Some("city_district"));
        assert_eq!(rank_to_field(19), Some("city_district"));
        assert_eq!(rank_to_field(20), Some("suburb"));
        assert_eq!(rank_to_field(21), Some("suburb"));
        assert_eq!(rank_to_field(22), Some("neighbourhood"));
        assert_eq!(rank_to_field(23), Some("neighbourhood"));
        assert_eq!(rank_to_field(24), Some("city_block"));
        assert_eq!(rank_to_field(25), Some("city_block"));
        // 26+ -> None.
        assert_eq!(rank_to_field(26), None);
        assert_eq!(rank_to_field(30), None);
    }

    #[test]
    fn place_type_to_rank_snapshot() {
        assert_eq!(place_type_to_rank(1, ""), 16);
        assert_eq!(place_type_to_rank(2, ""), 16);
        assert_eq!(place_type_to_rank(3, ""), 16);
        assert_eq!(place_type_to_rank(4, ""), 20);
        assert_eq!(place_type_to_rank(5, ""), 24);
        assert_eq!(place_type_to_rank(6, ""), 22);
        // NOTE: state/province/region map to 0 (deliberately not in address chain).
        assert_eq!(place_type_to_rank(7, ""), 0);
        assert_eq!(place_type_to_rank(8, ""), 0);
        assert_eq!(place_type_to_rank(9, ""), 0);
        assert_eq!(place_type_to_rank(10, ""), 12);
        assert_eq!(place_type_to_rank(11, ""), 12);
        assert_eq!(place_type_to_rank(12, ""), 18);
        assert_eq!(place_type_to_rank(13, ""), 20);
        assert_eq!(place_type_to_rank(14, ""), 14);
        assert_eq!(place_type_to_rank(0, ""), 0);
        assert_eq!(place_type_to_rank(255, ""), 0);
        // A country's own place ranks come first.
        assert_eq!(place_type_to_rank(10, "CA"), 0);
        assert_eq!(place_type_to_rank(10, "DE"), 0);
        assert_eq!(place_type_to_rank(9, "DE"), 0);
        assert_eq!(place_type_to_rank(8, "ES"), 10);
        assert_eq!(place_type_to_rank(14, "ID"), 18);
        assert_eq!(place_type_to_rank(14, "RU"), 18);
        assert_eq!(place_type_to_rank(8, "SA"), 12);
        assert_eq!(place_type_to_rank(14, "SA"), 18);
        assert_eq!(place_type_to_rank(10, "GB"), 12);
    }

    #[test]
    fn a_linked_place_area_ranks_by_its_own_tag_from_16_to_23_else_by_the_node() {
        // (own place type, the linked node's, country) → (compute_place_rank, rank_address).
        let ranks = |own: u8, linked: u8, country: &str| {
            let poly = AdminPolygon { place_type_override: linked, own_place_type: own, ..arc_fixture::record(0, 15, 1.0) };
            place_area_ranks(&poly, country)
        };
        assert_eq!(ranks(4, 2, ""), (20, 20));    // suburb linked to a town (Weesp)
        assert_eq!(ranks(3, 13, ""), (16, 16));   // village linked to a hamlet
        assert_eq!(ranks(14, 2, "RU"), (18, 18)); // municipality, 18 in Russia, linked to a town
        assert_eq!(ranks(5, 6, ""), (24, 22));    // neighbourhood linked to a quarter
        assert_eq!(ranks(14, 1, ""), (14, 16));   // municipality linked to a city
        assert_eq!(ranks(5, 7, ""), (24, 24));    // neighbourhood linked to a state, rank 0
        assert_eq!(ranks(10, 2, "CA"), (0, 0));   // county, 0 in Canada, links nothing
        assert_eq!(ranks(0, 4, ""), (20, 20));    // a suburb of its own
    }

    #[test]
    fn poi_category_to_osm_class_snapshot() {
        // Range boundaries for each class.
        assert_eq!(poi_category_to_osm_class(0), Some("tourism"));
        assert_eq!(poi_category_to_osm_class(19), Some("tourism"));
        assert_eq!(poi_category_to_osm_class(20), Some("historic"));
        assert_eq!(poi_category_to_osm_class(39), Some("historic"));
        assert_eq!(poi_category_to_osm_class(40), Some("amenity"));
        assert_eq!(poi_category_to_osm_class(54), Some("amenity"));
        assert_eq!(poi_category_to_osm_class(55), Some("amenity"));
        assert_eq!(poi_category_to_osm_class(59), Some("amenity"));
        assert_eq!(poi_category_to_osm_class(60), Some("leisure"));
        assert_eq!(poi_category_to_osm_class(66), Some("leisure"));
        assert_eq!(poi_category_to_osm_class(67), Some("amenity"));
        assert_eq!(poi_category_to_osm_class(69), Some("amenity"));
        assert_eq!(poi_category_to_osm_class(70), Some("leisure"));
        assert_eq!(poi_category_to_osm_class(72), Some("leisure"));
        assert_eq!(poi_category_to_osm_class(73), Some("amenity"));
        // 74 (PARKING) maps to amenity like its 73/75/76 neighbours and the
        // bicycle/motorcycle-parking sibs (228/229); Nominatim surfaces
        // amenity=parking as a rank-30 named POI.
        assert_eq!(poi_category_to_osm_class(74), Some("amenity"));
        assert_eq!(poi_category_to_osm_class(75), Some("amenity"));
        assert_eq!(poi_category_to_osm_class(76), Some("amenity"));
        assert_eq!(poi_category_to_osm_class(77), Some("leisure"));
        assert_eq!(poi_category_to_osm_class(79), Some("leisure"));
        assert_eq!(poi_category_to_osm_class(80), Some("natural"));
        assert_eq!(poi_category_to_osm_class(90), Some("natural"));
        // NOTE: 91/92/93 (bay/cape/island) are area labels -> None.
        assert_eq!(poi_category_to_osm_class(91), None);
        assert_eq!(poi_category_to_osm_class(92), None);
        assert_eq!(poi_category_to_osm_class(93), None);
        assert_eq!(poi_category_to_osm_class(94), Some("natural"));
        assert_eq!(poi_category_to_osm_class(99), Some("natural"));
        assert_eq!(poi_category_to_osm_class(100), Some("aeroway"));
        assert_eq!(poi_category_to_osm_class(105), Some("railway"));
        assert_eq!(poi_category_to_osm_class(110), Some("man_made"));
        assert_eq!(poi_category_to_osm_class(119), Some("man_made"));
        assert_eq!(poi_category_to_osm_class(120), Some("building"));
        assert_eq!(poi_category_to_osm_class(121), Some("building"));
        assert_eq!(poi_category_to_osm_class(130), Some("boundary"));
        assert_eq!(poi_category_to_osm_class(131), Some("boundary"));
        assert_eq!(poi_category_to_osm_class(132), Some("natural"));
        assert_eq!(poi_category_to_osm_class(136), Some("natural"));
        assert_eq!(poi_category_to_osm_class(140), Some("craft"));
        assert_eq!(poi_category_to_osm_class(141), Some("craft"));
        assert_eq!(poi_category_to_osm_class(142), Some("landuse"));
        assert_eq!(poi_category_to_osm_class(152), Some("landuse"));
        // POWER_PLANT=150 is excluded (None): power is not a Nominatim main tag
        // and never surfaces as a reverse landmark. The explicit `150 => None`
        // arm precedes the 142..=152 landuse band.
        assert_eq!(poi_category_to_osm_class(150), None);
        assert_eq!(poi_category_to_osm_class(160), Some("office"));
        assert_eq!(poi_category_to_osm_class(161), Some("leisure"));
        assert_eq!(poi_category_to_osm_class(162), Some("emergency"));
        // NOTE: GENERIC_RANK30=170 -> None (generic filler).
        assert_eq!(poi_category_to_osm_class(170), None);
        assert_eq!(poi_category_to_osm_class(171), Some("amenity"));
        assert_eq!(poi_category_to_osm_class(179), Some("amenity"));
        assert_eq!(poi_category_to_osm_class(180), Some("amenity"));
        assert_eq!(poi_category_to_osm_class(234), Some("amenity"));
        assert_eq!(poi_category_to_osm_class(235), Some("leisure"));
        assert_eq!(poi_category_to_osm_class(240), Some("leisure"));
        assert_eq!(poi_category_to_osm_class(241), Some("shop"));
        assert_eq!(poi_category_to_osm_class(253), Some("shop"));
        assert_eq!(poi_category_to_osm_class(254), None);
        assert_eq!(poi_category_to_osm_class(255), None);
    }

    #[test]
    fn is_area_label_category_snapshot() {
        // A representative sample of the matched set.
        for cat in [80u8, 81, 82, 92, 93, 60, 61, 62, 63, 64, 65,
                    130, 131, 41, 42, 43, 47, 51, 54, 3, 4, 9, 11, 25, 100, 150] {
            assert!(Index::is_area_label_category(cat), "expected {cat} to be an area label");
        }
        // A representative sample of non-matches. Bays (91) and glaciers (86) never take the
        // primary slot at all.
        for cat in [0u8, 1, 2, 5, 40, 44, 50, 66, 70, 86, 90, 91, 120, 170, 200, 255] {
            assert!(!Index::is_area_label_category(cat), "expected {cat} to NOT be an area label");
        }
    }

    // -- address_score -------------------------------------------------------

    #[test]
    fn address_score_field_weights() {
        // Empty address scores 0.
        let empty = Address::default();
        assert_eq!(address_score(&empty), 0);

        // Each field contributes its documented weight, additively.
        let mut a = Address::default();
        a.address.country = Some("X".into());       // +1
        a.address.state = Some("X".into());         // +2
        a.address.county = Some("X".into());        // +3
        a.address.city = Some("X".into());          // +4
        a.address.road = Some("X".into());          // +5
        a.address.house_number = Some(Cow::Borrowed("1")); // +6
        assert_eq!(address_score(&a), 1 + 2 + 3 + 4 + 5 + 6);

        // A populated result outscores an emptier one (monotonic refinement).
        let mut coarse = Address::default();
        coarse.address.country = Some("France".into());
        let mut fine = Address::default();
        fine.address.country = Some("France".into());
        fine.address.city = Some("Paris".into());
        fine.address.road = Some("Rue de Rivoli".into());
        assert!(address_score(&fine) > address_score(&coarse));

        // A non-empty places vec adds 1.
        let mut with_place = Address::default();
        with_place.places.push(PoiDetail {
            name: "Eiffel Tower".into(),
            category: "tourism".into(),
            distance_m: 0.0,
        });
        assert_eq!(address_score(&with_place), 1);
    }

    // -- geometry: point_in_polygon / distances ------------------------------

    fn nc(lat: f32, lng: f32) -> NodeCoord { NodeCoord { lat, lng } }

    fn unit_square() -> Vec<NodeCoord> {
        // CCW unit square [0,1]x[0,1] (lat, lng).
        vec![nc(0.0, 0.0), nc(0.0, 1.0), nc(1.0, 1.0), nc(1.0, 0.0)]
    }

    #[test]
    fn point_in_polygon_square() {
        let sq = unit_square();
        // Interior.
        assert!(point_in_polygon(0.5, 0.5, &sq));
        // Clearly outside on every side.
        assert!(!point_in_polygon(0.5, 1.5, &sq));
        assert!(!point_in_polygon(0.5, -0.5, &sq));
        assert!(!point_in_polygon(1.5, 0.5, &sq));
        assert!(!point_in_polygon(-0.5, 0.5, &sq));
    }

    fn boundary(name_id: u32, admin_level: u8) -> AdminPolygon {
        AdminPolygon {
            ring_offset: 0, ring_bytes: 0, name_id, admin_level, place_type_override: 0, own_place_type: 0, area: 1.0,
            country_code: 0, of_way: false,
        }
    }

    fn names(found: &CurrentBoundaries) -> [Option<u32>; 5] {
        found.gates.map(|p| p.map(|p| p.name_id))
    }

    fn ranked(admin_level: u8, rank_address: u8, area: f32, place_type_override: u8) -> RankedPoly {
        RankedPoly { poly: AdminPolygon { area, place_type_override, ..boundary(0, admin_level) }, rank_address }
    }

    fn levels_and_ranks(polys: &[RankedPoly]) -> Vec<(u8, u8)> {
        polys.iter().map(|p| (p.poly.admin_level, p.rank_address)).collect()
    }

    #[test]
    fn nested_ranks_promote_the_children_of_promoted_boundaries() {
        // Starehe (admin_level 6, rank 12) lies in the place=city area of
        // Nairobi and becomes 18; CBD division (7, rank 14) inside it 20.
        let nairobi = AdminPolygon { area: 100.0, place_type_override: 1, ..boundary(0, 15) };
        let mut polys = [ranked(7, 14, 1.0, 0), ranked(6, 12, 10.0, 0), ranked(2, 4, 1000.0, 0)];
        promote_nested_ranks(&mut polys, [nairobi].iter(), "", |_, _| true);
        assert_eq!(levels_and_ranks(&polys), [(2, 4), (6, 18), (7, 20)]);
    }

    #[test]
    fn nested_ranks_keep_a_linked_place_rank_inside_a_place_area() {
        // Paris (admin_level 6, linked place=city) takes rank 16 and keeps it
        // inside a larger place=city area; its arrondissement (9) goes below.
        let area = AdminPolygon { area: 100.0, place_type_override: 1, ..boundary(0, 15) };
        let mut polys = [ranked(9, 18, 1.0, 0), ranked(6, 12, 10.0, 1)];
        promote_nested_ranks(&mut polys, [area].iter(), "", |_, _| true);
        assert_eq!(levels_and_ranks(&polys), [(6, 16), (9, 18)]);
    }

    #[test]
    fn nested_ranks_keep_boundaries_below_rank_10_out_of_place_areas() {
        // A state (level 4, rank 8) inside a larger suburb area keeps its
        // rank; a county (level 6, rank 12) in it ranks 22.
        let area = AdminPolygon { area: 100.0, place_type_override: 4, ..boundary(0, 15) };
        let mut polys = [ranked(6, 12, 1.0, 0), ranked(4, 8, 10.0, 0)];
        promote_nested_ranks(&mut polys, [area].iter(), "", |_, _| true);
        assert_eq!(levels_and_ranks(&polys), [(4, 8), (6, 22)]);
    }

    #[test]
    fn nested_ranks_stop_at_25_under_a_parent_of_24() {
        // Upper Ward (admin_level 9, rank 18) takes its linked neighbourhood's 24, and Lower Ward
        // (10, rank 20, a neighbourhood too) inside it 25, the most the admin-nested step gives
        // under a parent of 24 or more (placex_triggers.sql:903-904): a boundary of 25 is still
        // an admin-nested parent (ADMIN_PARENT_RANKS), one of 26 would not be.
        let mut polys = [ranked(10, 20, 1.0, 5), ranked(9, 18, 10.0, 5)];
        promote_nested_ranks(&mut polys, std::iter::empty::<&AdminPolygon>(), "", |_, _| true);
        assert_eq!(levels_and_ranks(&polys), [(9, 24), (10, 25)]);
    }

    #[test]
    fn nested_ranks_give_a_boundary_repeating_its_parent_its_place_nodes_rank() {
        // is_same takes a boundary that repeats its parent out of addresses and leaves
        // parent_address_level at 3, so the city node it links gives it 16 back (:1161) under a
        // parent of 24, where it would rank 25, and a boundary inside it ranks under 16. One
        // linking a place=state node, of rank 0, stays out.
        let repeating = |admin_level: u8, rank_address: u8, place_type_override: u8| RankedPoly {
            poly: AdminPolygon { own_place_type: BOUNDARY_REPEATS_PARENT, ..ranked(admin_level, rank_address, 10.0, place_type_override).poly },
            rank_address,
        };
        let mut polys = [ranked(11, 22, 1.0, 0), repeating(10, 20, 1), ranked(8, 16, 10.0, 5)];
        promote_nested_ranks(&mut polys, std::iter::empty::<&AdminPolygon>(), "", |_, _| true);
        assert_eq!(levels_and_ranks(&polys), [(8, 24), (10, 16), (11, 22)]);
        let mut polys = [repeating(10, 20, 7), ranked(8, 16, 10.0, 0)];
        promote_nested_ranks(&mut polys, std::iter::empty::<&AdminPolygon>(), "", |_, _| true);
        assert_eq!(levels_and_ranks(&polys), [(8, 16), (10, 0)]);
    }

    #[test]
    fn current_boundaries_fall_back_to_the_country() {
        // A Vatican point: Rome's boundaries are near but leave it out; the
        // country holds it and gates every place rank.
        let found = CurrentBoundaries::walk([(4, boundary(3, 2))], |_, _| true);
        assert_eq!(names(&found), [Some(3); 5]);
    }

    #[test]
    fn current_boundaries_skip_boundaries_ranked_after_the_place() {
        // Spain's admin_level 10 ranks 22: it gates quarters and
        // neighbourhoods, while suburbs see the municipality around it.
        let candidates = [(22, boundary(1, 10)), (16, boundary(2, 8)), (4, boundary(3, 2))];
        let found = CurrentBoundaries::walk(candidates, |_, _| true);
        assert_eq!(names(&found), [Some(2), Some(2), Some(2), Some(1), Some(1)]);
    }

    #[test]
    fn current_boundaries_rank_boundaries_by_their_promoted_rank() {
        // An admin_level 6 boundary linked to a suburb has rank 20, so its
        // level 7 child is promoted from 14 to 22: suburb nodes are gated by
        // the level 6 boundary, quarters and neighbourhoods by the child.
        let suburb = AdminPolygon { area: 10.0, place_type_override: 4, ..boundary(1, 6) };
        let child = AdminPolygon { area: 1.0, ..boundary(2, 7) };
        let country = AdminPolygon { area: 1000.0, ..boundary(3, 2) };
        let ranked = vec![
            RankedPoly { poly: child, rank_address: 14 },
            RankedPoly { poly: suburb, rank_address: 12 },
            RankedPoly { poly: country, rank_address: 4 },
        ];
        let found = CurrentBoundaries::from_ranked(ranked, [].iter(), "", |_, _| true, |_, _| true);
        assert_eq!(names(&found), [Some(3), Some(3), Some(1), Some(2), Some(2)]);
    }

    #[test]
    fn current_boundaries_rank_boundaries_inside_a_place_area() {
        // Starehe (admin_level 6, rank 12) inside Nairobi's place=city area
        // ranks 18, after the city's 16: it gates suburbs, quarters and
        // neighbourhoods, and the city's area city, town and village nodes.
        let starehe = AdminPolygon { area: 10.0, ..boundary(1, 6) };
        let country = AdminPolygon { area: 1000.0, ..boundary(2, 2) };
        let nairobi = AdminPolygon { area: 100.0, place_type_override: 1, ..boundary(3, 15) };
        let ranked = vec![RankedPoly { poly: starehe, rank_address: 12 }, RankedPoly { poly: country, rank_address: 4 }];
        let found = CurrentBoundaries::from_ranked(ranked, [nairobi].iter(), "", |_, _| true, |_, _| true);
        assert_eq!(names(&found), [Some(3), Some(1), Some(1), Some(1), Some(1)]);
    }

    #[test]
    fn current_boundaries_gate_suburbs_by_the_promoted_rank_20_boundary() {
        // An admin_level 6 boundary linked to a city ranks 16, so levels 7
        // to 10 below it rank 18, 20, 22 and 24 (default ranks 14 to 20):
        // suburb nodes are gated by level 8, not by level 10 at its default
        // rank 20; quarters by level 9, neighbourhoods by level 10.
        let level = |admin_level: u8, rank_address: u8, area: f32, place_type_override: u8| RankedPoly {
            poly: AdminPolygon { area, place_type_override, ..boundary(admin_level as u32, admin_level) },
            rank_address,
        };
        let ranked = vec![
            level(10, 20, 1.0, 0), level(9, 18, 2.0, 0), level(8, 16, 4.0, 0),
            level(7, 14, 8.0, 0), level(6, 12, 16.0, 1), level(2, 4, 1000.0, 0),
        ];
        let found = CurrentBoundaries::from_ranked(ranked, [].iter(), "", |_, _| true, |_, _| true);
        let gate = |rank| found.for_rank(rank).map(|p| p.admin_level);
        assert_eq!(gate(CurrentBoundaries::SUBURB), Some(8));
        assert_eq!(gate(CurrentBoundaries::QUARTER), Some(9));
        assert_eq!(gate(CurrentBoundaries::NEIGHBOURHOOD), Some(10));
    }

    #[test]
    fn current_boundaries_are_the_last_the_walk_accepts() {
        // Admin_level 6 boundaries round the point, the larger linked to a
        // suburb (rank 20), the smaller to a city (rank 16). Nominatim's walk
        // accepts the city at rank 16, then the suburb at rank 20 when its
        // centroid lies in the city (concentric): the suburb gates suburbs,
        // quarters and neighbourhoods. Otherwise (overlapping) the city does.
        // The city gates city and city-part nodes either way.
        let suburb = AdminPolygon { area: 10.0, place_type_override: 4, ..boundary(1, 6) };
        let city = AdminPolygon { area: 1.0, place_type_override: 1, ..boundary(2, 6) };
        let country = AdminPolygon { area: 1000.0, ..boundary(3, 2) };
        let ranked = || vec![
            RankedPoly { poly: city, rank_address: 12 },
            RankedPoly { poly: suburb, rank_address: 12 },
            RankedPoly { poly: country, rank_address: 4 },
        ];
        let concentric = CurrentBoundaries::from_ranked(ranked(), [].iter(), "", |_, _| true, |_, _| true);
        assert_eq!(names(&concentric), [Some(2), Some(2), Some(1), Some(1), Some(1)]);
        let overlapping = CurrentBoundaries::from_ranked(ranked(), [].iter(), "", |poly, _| poly.name_id != 1, |_, _| true);
        assert_eq!(names(&overlapping), [Some(2); 5]);
    }

    #[test]
    fn current_boundaries_take_smaller_boundaries_as_inside() {
        // A county, then a town, each smaller than the boundary before: both
        // are accepted without testing their centroids.
        let candidates = [(4, boundary(3, 2)), (12, AdminPolygon { area: 0.5, ..boundary(5, 6) }), (16, AdminPolygon { area: 0.1, ..boundary(6, 8) })];
        let found = CurrentBoundaries::walk(candidates, |_, _| false);
        assert_eq!(names(&found), [Some(6); 5]);
    }

    #[test]
    fn current_boundaries_try_the_smallest_of_a_rank_first() {
        // Two rank 20 boundaries: the smaller is accepted and the larger
        // skipped; where the smaller lies outside the current boundary, the
        // larger is tried next.
        let wider = AdminPolygon { area: 20.0, ..boundary(4, 8) };
        let suburb = AdminPolygon { area: 10.0, ..boundary(1, 8) };
        let city = AdminPolygon { area: 1.0, ..boundary(2, 6) };
        let candidates = [(20, wider), (20, suburb), (16, city)];
        let found = CurrentBoundaries::walk(candidates, |_, _| true);
        assert_eq!(names(&found), [Some(2), Some(2), Some(1), Some(1), Some(1)]);
        let found = CurrentBoundaries::walk(candidates, |poly, _| poly.name_id != 1);
        assert_eq!(names(&found), [Some(2), Some(2), Some(4), Some(4), Some(4)]);
    }

    #[test]
    fn current_boundaries_gate_rank_16_by_the_boundary_below_it() {
        // Dhaka: no rank-16 boundary, so Dhaka Metropolitan (rank 12) gates
        // the city, town and village nodes; Keraniganj across the river,
        // outside it, is no candidate.
        let candidates = [(12, boundary(1, 6)), (10, boundary(2, 5)), (8, boundary(3, 4)), (4, boundary(4, 2))];
        let found = CurrentBoundaries::walk(candidates, |_, _| true);
        assert_eq!(names(&found), [Some(1); 5]);
        assert_eq!(found.for_rank(CurrentBoundaries::CITY).map(|p| p.admin_level), Some(6));
    }

    // A node of PlaceNode type `pt` `east_m` metres east of (0, 0),
    // `dist_sq` from the query.
    fn node_at(pt: usize, east_m: f64, dist_sq: f64) -> (usize, u8, f64, f32, f32) {
        (pt, PLACE_NODE_RANK[pt], dist_sq, 0.0, (east_m / METRES_PER_DEGREE) as f32)
    }

    // The same, promoted two ranks.
    fn promoted_node_at(pt: usize, east_m: f64, dist_sq: f64) -> (usize, u8, f64, f32, f32) {
        let (pt, rank, dist_sq, lat, lng) = node_at(pt, east_m, dist_sq);
        (pt, rank + PLACE_NODE_PROMOTION, dist_sq, lat, lng)
    }

    fn place_node(place_type: u8, lat_m: f64, lng_m: f64) -> PlaceNode {
        PlaceNode {
            lat: (lat_m / 111320.0) as f32, lng: (lng_m / 111320.0) as f32, name_id: 0, place_type,
            _pad1: 0, _pad2: 0, parent_poly_id: NO_DATA,
        }
    }

    #[test]
    fn coarse_fields_follow_the_street_point_when_chain_and_query_differ() {
        // Leukerbad: the chain says Bern, the query and the street point Valais.
        let chain = [Some("Bern"), None, None];
        let query = [Some("Valais"), None, None];
        assert_eq!(coarse_fields(chain, query, || Some([Some("Valais"), None, None])), query);
        // The street point outside every state keeps the chain's.
        assert_eq!(coarse_fields(chain, query, || Some([None, None, None])), chain);
        assert_eq!(coarse_fields(chain, query, || None), chain);
        // Agreement never looks at the street.
        assert_eq!(coarse_fields(chain, chain, || panic!("looked")), chain);
    }

    #[test]
    fn closest_point_on_line_is_planar_in_degrees() {
        // At 60°N a segment 0.015° east is 0.0075° away in metres-true terms,
        // nearer than one 0.01° north; in plain degrees, as ST_ClosestPoint
        // measures, the northern one is nearer.
        let line = [nc(59.99, 0.015), nc(60.01, 0.015), nc(60.01, -0.02)];
        let (lat, lng) = closest_point_on_line(&line, 60.0, 0.0).unwrap();
        assert!((lat - 60.01f32 as f64).abs() < 1e-9 && lng.abs() < 1e-6, "({lat}, {lng})");
        assert_eq!(closest_point_on_line(&line[..1], 60.0, 0.0), None);
    }

    #[test]
    fn fuzzy_area_is_a_square_of_the_half_side() {
        let east = |m: f64| m / 111320.0;
        // A neighbourhood reaches 500 m along each axis, corners included.
        let neighbourhood = place_fuzzy_half_side_m(5);
        assert!(fuzzy_area_holds(0.0, 0.0, neighbourhood, 0.0, east(490.0)));
        assert!(fuzzy_area_holds(0.0, 0.0, neighbourhood, east(490.0), east(490.0)));
        assert!(!fuzzy_area_holds(0.0, 0.0, neighbourhood, 0.0, east(510.0)));
        // A quarter reaches 1000 m.
        assert!(fuzzy_area_holds(0.0, 0.0, place_fuzzy_half_side_m(6), east(990.0), 0.0));
        assert!(!fuzzy_area_holds(0.0, 0.0, place_fuzzy_half_side_m(6), east(1010.0), 0.0));
        // East-west metres shrink with the cosine of the node's latitude.
        assert!(fuzzy_area_holds(60.0, 0.0, 500.0, 60.0, 2.0 * east(490.0)));
        assert!(!fuzzy_area_holds(60.0, 0.0, 500.0, 60.0, 2.0 * east(510.0)));
    }

    #[test]
    fn max_place_fuzzy_half_side_is_the_largest_of_any_type() {
        let largest = (0..PLACE_NODE_RANK.len()).map(place_fuzzy_half_side_m).fold(0.0, f64::max);
        assert_eq!(largest, MAX_PLACE_FUZZY_HALF_SIDE_M);
    }

    #[test]
    fn cells_under_line_hold_a_city_beyond_each_corner_of_the_street() {
        // A street at 60°N; a city just inside reach of each end, diagonally
        // out, covers that end and sits in one of the cells.
        let line = [nc(60.0, 10.0), nc(60.1, 10.0)];
        let cells = cells_under_line(&line, MAX_PLACE_FUZZY_HALF_SIDE_M, DEFAULT_ADMIN_CELL_LEVEL);
        let reach = MAX_PLACE_FUZZY_HALF_SIDE_M - 100.0;
        for (end_lat, north) in [(60.1, 1.0), (60.0, -1.0)] {
            let lat = end_lat + north * reach / METRES_PER_DEGREE;
            for east in [1.0, -1.0] {
                let lng = 10.0 + east * reach / (METRES_PER_DEGREE * lat.to_radians().cos());
                assert!(fuzzy_area_holds(lat as f32, lng as f32, MAX_PLACE_FUZZY_HALF_SIDE_M, end_lat, 10.0));
                assert!(cells.contains(&cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL)), "({lat}, {lng})");
            }
        }
        assert!(cells_under_line(&[], MAX_PLACE_FUZZY_HALF_SIDE_M, DEFAULT_ADMIN_CELL_LEVEL).is_empty());
    }

    #[test]
    fn cells_under_line_follow_a_diagonal_street() {
        // A 270 km street running north-east from 45°N, drawn with a point
        // every 1.4 km and as one segment: a city within reach of any point of
        // it, diagonally out, sits in one of the cells, and the cells are a
        // strip along the street, far fewer than under its bounding box grown
        // by the same reach.
        let points: Vec<NodeCoord> = (0..=200).map(|k| nc(45.0 + k as f32 * 0.01, 10.0 + k as f32 * 0.01)).collect();
        let margin = MAX_PLACE_FUZZY_HALF_SIDE_M;
        let corners = LatLngBox::at((45.0, 10.0)).union(LatLngBox::at((47.0, 12.0)));
        let bbox = cells_under_rects(vec![grown_rect(corners, margin)], DEFAULT_ADMIN_CELL_LEVEL);
        let reach = margin - 100.0;
        for line in [points.clone(), vec![points[0], points[200]]] {
            let cells = cells_under_line(&line, margin, DEFAULT_ADMIN_CELL_LEVEL);
            for p in points.iter().step_by(7) {
                let (lat0, lng0) = (p.lat as f64, p.lng as f64);
                for (north, east) in [(1.0, -1.0), (-1.0, 1.0), (1.0, 1.0), (-1.0, -1.0)] {
                    let lat = lat0 + north * reach / METRES_PER_DEGREE;
                    let lng = lng0 + east * reach / (METRES_PER_DEGREE * lat.to_radians().cos());
                    assert!(fuzzy_area_holds(lat as f32, lng as f32, margin, lat0, lng0));
                    assert!(cells.contains(&cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL)), "{} points: ({lat}, {lng})", line.len());
                }
            }
            assert!(cells.len() * 2 < bbox.len(), "{} points: {} cells, {} under the bounding box", line.len(), cells.len(), bbox.len());
        }
    }

    #[test]
    fn cells_near_point_hold_a_city_at_each_corner_of_its_reach() {
        // A city just inside reach diagonally out from the point, at 10°N and
        // at 60°N, covers the point and sits in one of the cells.
        for (lat0, lng0) in [(10.0, 20.0), (60.0, 10.0)] {
            let cells = cells_near_point(lat0, lng0, MAX_PLACE_FUZZY_HALF_SIDE_M, DEFAULT_ADMIN_CELL_LEVEL);
            let reach = MAX_PLACE_FUZZY_HALF_SIDE_M - 100.0;
            for (north, east) in [(1.0, 1.0), (1.0, -1.0), (-1.0, 1.0), (-1.0, -1.0)] {
                let lat = lat0 + north * reach / METRES_PER_DEGREE;
                let lng = lng0 + east * reach / (METRES_PER_DEGREE * lat.to_radians().cos());
                assert!(fuzzy_area_holds(lat as f32, lng as f32, MAX_PLACE_FUZZY_HALF_SIDE_M, lat0, lng0));
                assert!(cells.contains(&cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL)), "({lat}, {lng})");
            }
        }
    }

    #[test]
    fn place_nodes_fill_quarter_and_neighbourhood_both() {
        // Taguig: Nominatim's answer has a quarter and a neighbourhood.
        let nodes = [node_at(6, 0.0, 2.0), node_at(5, 500.0, 1.0)];
        let accepted = accept_place_nodes(&nodes, 0, |_| true);
        assert_eq!(accepted, [None, None, None, None, None, Some(1), Some(0)]);
    }

    #[test]
    fn place_nodes_neighbourhood_must_sit_in_the_quarter_fuzzy_area() {
        // 1000 m is the quarter's half side: 1100 m east is outside.
        let mut nodes = [node_at(6, 0.0, 1.0), node_at(5, 1100.0, 2.0)];
        assert_eq!(accept_place_nodes(&nodes, 0, |_| true), [None, None, None, None, None, None, Some(0)]);
        // Without an admissible quarter nothing holds the neighbourhood.
        assert_eq!(accept_place_nodes(&nodes, 0, |k| k != 0), [None, None, None, None, None, Some(1), None]);
        // 900 m east is inside: the half side is the radius, not radius / √2.
        nodes[1] = node_at(5, 900.0, 2.0);
        assert_eq!(accept_place_nodes(&nodes, 0, |_| true), [None, None, None, None, None, Some(1), Some(0)]);
    }

    #[test]
    fn place_nodes_one_entry_at_rank_16_and_20() {
        let mut nodes = [
            node_at(0, 0.0, 9.0), node_at(1, 100.0, 2.0), node_at(2, 150.0, 1.0),
            node_at(3, 200.0, 2.0), node_at(4, 300.0, 1.0),
        ];
        // A city 3 away weighs 0.75, a town 1.41 away 0.71, a village 1 away
        // 1: the town; the nearer of suburb and hamlet.
        assert_eq!(accept_place_nodes(&nodes, 0, |_| true), [None, Some(1), None, None, Some(4), None, None]);
        // A city 2 away (0.5) beats the town.
        nodes[0] = node_at(0, 0.0, 4.0);
        assert_eq!(accept_place_nodes(&nodes, 0, |_| true), [Some(0), None, None, None, Some(4), None, None]);
        // A boundary at rank 16 leaves city, town and village out.
        assert_eq!(accept_place_nodes(&nodes, 1 << CurrentBoundaries::CITY, |_| true), [None, None, None, None, Some(4), None, None]);
    }

    #[test]
    fn place_nodes_rank_16_node_holds_the_suburb() {
        // A village's fuzzy half side is 2000 m: a suburb 2100 m away stays
        // out, one 1500 m away comes in.
        let mut nodes = [node_at(2, 0.0, 1.0), node_at(3, 2100.0, 1.0)];
        assert_eq!(accept_place_nodes(&nodes, 0, |_| true), [None, None, Some(0), None, None, None, None]);
        nodes[1] = node_at(3, 1500.0, 1.0);
        assert_eq!(accept_place_nodes(&nodes, 0, |_| true), [None, None, Some(0), Some(1), None, None, None]);
    }

    #[test]
    fn place_nodes_cover_the_point_with_their_own_fuzzy_area() {
        // A neighbourhood 510 m east is nearer than one 490 m north and 490 m
        // east, but only the latter's 500 m square covers the point.
        let east = place_node(5, 0.0, 510.0);
        let north_east = place_node(5, 490.0, 490.0);
        let query = PlaceFeature::Point(0.0, 0.0);
        assert_eq!(place_node_covering(&east, query), None);
        assert!(place_node_covering(&north_east, query).is_some());
    }

    // A line through points given in metres (north, east) from (0, 0).
    fn line_m(points: &[(f64, f64)]) -> Vec<NodeCoord> {
        points.iter().map(|&(n, e)| nc((n / METRES_PER_DEGREE) as f32, (e / METRES_PER_DEGREE) as f32)).collect()
    }

    #[test]
    fn line_crosses_fuzzy_area_along_a_stretch() {
        // A 500 m square around (0, 0).
        let crosses = |pts: &[(f64, f64)]| line_crosses_fuzzy_area(&line_m(pts), 0.0, 0.0, 500.0);
        assert!(crosses(&[(-2000.0, 300.0), (2000.0, 300.0)]));
        assert!(!crosses(&[(-2000.0, 600.0), (2000.0, 600.0)]));
        // Ending inside the square, or just short of it.
        assert!(crosses(&[(0.0, 2000.0), (0.0, 400.0)]));
        assert!(!crosses(&[(0.0, 2000.0), (0.0, 600.0)]));
        // Its second segment crosses.
        assert!(crosses(&[(-2000.0, 900.0), (0.0, 900.0), (0.0, -900.0)]));
    }

    #[test]
    fn place_nodes_cover_and_measure_from_the_street() {
        // The street runs north along longitude 0 from 1 km south to 5 km
        // north of the query. A hamlet 3 km north and 200 m east is no
        // candidate at the query point, but its 1000 m square crosses the
        // street, 200 m away; one 900 m east of the query is 900 m away. The
        // street's walk takes the northern one (Bégard: Nominatim's hamlet
        // Justice is 40 m from the street and far from the query).
        let street = line_m(&[(-1000.0, 0.0), (5000.0, 0.0)]);
        let north = place_node(4, 3000.0, 200.0);
        let east = place_node(4, 0.0, 900.0);
        let (point, line) = (PlaceFeature::Point(0.0, 0.0), PlaceFeature::Line(&street));
        assert_eq!(place_node_covering(&north, point), None);
        let metres = |pn: &PlaceNode| place_node_covering(pn, line).unwrap().sqrt() * 6_371_000.0;
        assert!((metres(&north) - 200.0).abs() < 1.0, "{}", metres(&north));
        assert!((metres(&east) - 900.0).abs() < 2.0, "{}", metres(&east));
        let walk = |feature| {
            let nodes: Vec<(usize, u8, f64, f32, f32)> = [north, east].iter()
                .filter_map(|pn| place_node_covering(pn, feature).map(|d| (4, CurrentBoundaries::SUBURB, d, pn.lat, pn.lng)))
                .collect();
            let accepted = accept_place_nodes(&nodes, 0, |_| true)[4];
            accepted.map(|k| nodes[k].4)
        };
        assert_eq!(walk(line), Some(north.lng));
        assert_eq!(walk(point), Some(east.lng));
    }

    #[test]
    fn place_nodes_measure_in_plain_degrees() {
        // At 60°N, 0.0075° east of the point and of a street ending there is
        // farther than 0.005° north, though nearer in metres.
        let hamlet = |lat: f32, lng: f32| PlaceNode { lat, lng, ..place_node(4, 0.0, 0.0) };
        let (east, north) = (hamlet(60.0, 10.0075), hamlet(60.005, 10.0));
        let street = [nc(60.0, 9.997), nc(60.0, 10.0)];
        for feature in [PlaceFeature::Point(60.0, 10.0), PlaceFeature::Line(&street)] {
            let degrees = |pn: &PlaceNode| place_node_covering(pn, feature).unwrap().sqrt().to_degrees();
            assert!((degrees(&east) - 0.0075).abs() < 1e-5, "{}", degrees(&east));
            assert!((degrees(&north) - 0.005).abs() < 1e-5, "{}", degrees(&north));
        }
    }

    #[test]
    fn place_nodes_rank_two_up_inside_a_polygon_of_their_rank() {
        let square = unit_square();
        let in_square = |pn: &PlaceNode, promoting: &[(u8, AdminPolygon)]| {
            place_node_rank(pn, promoting, |_| point_in_polygon(pn.lat, pn.lng, &square))
        };
        let city = [(CurrentBoundaries::CITY, boundary(1, 8))];
        let village = PlaceNode { lat: 0.5, lng: 0.5, ..place_node(2, 0.0, 0.0) };
        assert_eq!(in_square(&village, &city), CurrentBoundaries::CITY_PART);
        assert_eq!(in_square(&PlaceNode { lat: 1.5, ..village }, &city), CurrentBoundaries::CITY);
        // Only a polygon of the node's own rank promotes it; no other is tested.
        assert_eq!(place_node_rank(&PlaceNode { place_type: 4, ..village }, &city, |_| panic!("tested")), CurrentBoundaries::SUBURB);
        // A neighbourhood (24) is past the promotable ranks.
        let neighbourhoods = [(CurrentBoundaries::NEIGHBOURHOOD, boundary(1, 10))];
        assert_eq!(place_node_rank(&PlaceNode { place_type: 5, ..village }, &neighbourhoods, |_| panic!("tested")), CurrentBoundaries::NEIGHBOURHOOD);
    }

    #[test]
    fn place_nodes_promoted_walk_at_their_new_rank() {
        // Taipei: the city is a rank-16 boundary and the village node inside
        // it ranks 18, so the boundary leaves it in; Nominatim's answer has
        // city=Taipei and village=Xingya.
        let nodes = [node_at(1, 0.0, 1.0), promoted_node_at(2, 100.0, 4.0)];
        assert_eq!(accept_place_nodes(&nodes, 1 << CurrentBoundaries::CITY, |_| true), [None, None, Some(1), None, None, None, None]);
        assert_eq!(accept_place_nodes(&nodes, 0, |_| true), [None, Some(0), Some(1), None, None, None, None]);
        // The rank_search weights apply at rank 16 only: at 18 a city 2 away
        // loses to a village 1 away.
        let nodes = [promoted_node_at(0, 0.0, 4.0), promoted_node_at(2, 100.0, 1.0)];
        assert_eq!(accept_place_nodes(&nodes, 0, |_| true), [None, None, Some(1), None, None, None, None]);
        // A hamlet at 20 and a promoted one at 22: the finer one names it.
        let nodes = [node_at(4, 0.0, 1.0), promoted_node_at(4, 100.0, 2.0)];
        assert_eq!(accept_place_nodes(&nodes, 0, |_| true)[4], Some(1));
    }

    #[test]
    fn place_nodes_skip_the_ranks_boundaries_hold() {
        // Taipei: the district boundary (linked to a suburb) holds rank 20,
        // so a hamlet node there names nothing; Nominatim ranks the hamlet
        // inside it 22, behind the rank-22 village boundary as well.
        let nodes = [node_at(4, 0.0, 1.0), node_at(3, 100.0, 2.0), node_at(6, 200.0, 3.0)];
        let held = |ranks: &[u8]| ranks.iter().fold(0, |bits, r| bits | 1 << r);
        assert_eq!(accept_place_nodes(&nodes, held(&[20]), |_| true), [None, None, None, None, None, None, Some(2)]);
        assert_eq!(accept_place_nodes(&nodes, held(&[20, 22]), |_| true), [None; 7]);
        let hamlet_in_district = [promoted_node_at(4, 0.0, 1.0)];
        assert_eq!(accept_place_nodes(&hamlet_in_district, held(&[20, 22]), |_| true), [None; 7]);
        assert_eq!(accept_place_nodes(&hamlet_in_district, held(&[20]), |_| true)[4], Some(0));
    }

    #[test]
    fn place_node_rows_are_a_rank_s_nodes_until_one_is_accepted_and_then_those_within_twice_the_nearest() {
        // insert_addresslines (placex_triggers.sql:606-669): Whalens Court's rank 20 has Addingham
        // at 300 m, accepted, and Drexel Hill at 500 m and Kellyville at 590 m as rows; a node at
        // 700 m is more than twice the nearest away and no row.
        let d = |m: f64| (m / METRES_PER_DEGREE).powi(2);
        let nodes = [node_at(4, 0.0, d(300.0)), node_at(3, 50.0, d(500.0)), node_at(4, 100.0, d(590.0)), node_at(3, 150.0, d(700.0))];
        let (accepted, rows) = place_node_walk(&nodes, 0, |_| true, true);
        assert_eq!(accepted, accept_place_nodes(&nodes, 0, |_| true));
        assert_eq!(accepted[4], Some(0));
        assert_eq!(rows, [0, 1, 2]);
        // A node outside its rank's boundary is no row and no nearest: Drexel Hill is accepted,
        // and the rest are within twice its distance.
        let (accepted, rows) = place_node_walk(&nodes, 0, |k| k != 0, true);
        assert_eq!(accepted[3], Some(1));
        assert_eq!(rows, [1, 2, 3]);
        // A rank a boundary holds takes no node, and its first node is the nearest.
        let (accepted, rows) = place_node_walk(&nodes, 1 << 20, |_| true, true);
        assert_eq!(accepted, [None; 7]);
        assert_eq!(rows, [0, 1, 2]);
        // Without rows the walk accepts the same nodes, Addingham alone, and lists none.
        assert_eq!(place_node_walk(&nodes, 0, |_| true, false), ([None, None, None, None, Some(0), None, None], Vec::new()));
    }

    #[test]
    fn place_node_rows_of_a_rank_none_accepts_all_count() {
        // A village at 16, whose fuzzy area reaches 2000 m east: the hamlets 2500 m and 2600 m
        // east are outside it, so each is a row and none is accepted, however far; the one 1000 m
        // east is accepted, and one past twice the nearest (300 m) after it is no row.
        let d = |m: f64| (m / METRES_PER_DEGREE).powi(2);
        let nodes = [node_at(2, 0.0, d(100.0)), node_at(4, 2500.0, d(300.0)), node_at(4, 2600.0, d(900.0)),
                     node_at(4, 1000.0, d(1000.0)), node_at(4, 1100.0, d(1500.0))];
        let (accepted, rows) = place_node_walk(&nodes, 0, |_| true, true);
        assert_eq!(accepted, [None, None, Some(0), None, Some(3), None, None]);
        assert_eq!(accepted, accept_place_nodes(&nodes, 0, |_| true));
        assert_eq!(rows, [0, 1, 2, 3]);
    }

    #[test]
    fn place_nodes_after_a_boundary_leave_the_fuzzy_area_behind() {
        // A village's fuzzy area reaches 2000 m: a suburb 3000 m away stays
        // out, unless a boundary between them (rank 18) took over.
        let nodes = [node_at(2, 0.0, 1.0), node_at(3, 3000.0, 1.0)];
        assert_eq!(accept_place_nodes(&nodes, 0, |_| true), [None, None, Some(0), None, None, None, None]);
        assert_eq!(accept_place_nodes(&nodes, 1 << 18, |_| true), [None, None, Some(0), Some(1), None, None, None]);
    }

    #[test]
    fn current_boundaries_keep_the_polygons_that_promote() {
        // A rank-16 city and a rank-20 suburb area promote; the country, a
        // rank-24 neighbourhood area and a rank-12 county don't.
        let city = AdminPolygon { area: 10.0, ..boundary(1, 8) };
        let county = AdminPolygon { area: 100.0, ..boundary(2, 6) };
        let country = AdminPolygon { area: 1000.0, ..boundary(3, 2) };
        let suburb = AdminPolygon { area: 1.0, place_type_override: 4, ..boundary(4, 15) };
        let neighbourhood = AdminPolygon { area: 0.5, place_type_override: 5, ..boundary(5, 15) };
        let ranked = vec![
            RankedPoly { poly: city, rank_address: 16 },
            RankedPoly { poly: county, rank_address: 12 },
            RankedPoly { poly: country, rank_address: 4 },
        ];
        let found = CurrentBoundaries::from_ranked(ranked, [suburb, neighbourhood].iter(), "", |_, _| true, |_, _| true);
        let promoting: Vec<(u8, u32)> = found.promoting.iter().map(|(r, p)| (*r, p.name_id)).collect();
        assert_eq!(promoting, [(16, 1), (20, 4)]);
    }

    #[test]
    fn place_nodes_take_the_first_of_their_rank_that_passes() {
        // The nearer neighbourhood lies outside the current boundary: the
        // farther one is the entry.
        let nodes = [node_at(5, 100.0, 1.0), node_at(5, 200.0, 2.0)];
        assert_eq!(accept_place_nodes(&nodes, 0, |k| k != 0)[5], Some(1));
        // The nearer one outside the quarter's fuzzy area: the farther one
        // inside it is the entry.
        let nodes = [node_at(6, 0.0, 1.0), node_at(5, 1100.0, 2.0), node_at(5, 900.0, 3.0)];
        assert_eq!(accept_place_nodes(&nodes, 0, |_| true), [None, None, None, None, None, Some(2), Some(0)]);
        // A town passed over for its gate leaves rank 16 to the village.
        let nodes = [node_at(1, 0.0, 1.0), node_at(2, 100.0, 4.0)];
        assert_eq!(accept_place_nodes(&nodes, 0, |k| k != 0), [None, None, Some(1), None, None, None, None]);
    }

    #[test]
    fn current_boundaries_are_empty_without_a_containing_boundary() {
        let found = CurrentBoundaries::walk([], |_, _| true);
        assert_eq!(names(&found), [None; 5]);
        assert!(found.for_rank(CurrentBoundaries::SUBURB).is_none());
    }

    #[test]
    fn point_in_polygon_triangle() {
        // Triangle with vertices (0,0), (0,4), (4,0) — right triangle.
        let tri = vec![nc(0.0, 0.0), nc(0.0, 4.0), nc(4.0, 0.0)];
        // Inside (below the hypotenuse lat+lng < 4).
        assert!(point_in_polygon(1.0, 1.0, &tri));
        // Outside, beyond the hypotenuse.
        assert!(!point_in_polygon(3.0, 3.0, &tri));
        // Outside entirely.
        assert!(!point_in_polygon(-1.0, -1.0, &tri));
    }

    #[test]
    fn beats_breaks_exact_ties_by_content() {
        // Two buildings the query sits inside (both 0 m): the nearer address
        // point wins, then the house number / street text that sorts first,
        // whatever the string offsets; record order never decides.
        let at = |lat: f32, lng: f32, hn: u32, st: u32| AddrRecord {
            lat, lng, housenumber_id: hn, street_id: st, parent_way_id: 0,
            blob_at: 0, blob_len: 0,
        };
        let text = ["2", "1/2", "Angell Street", "Bell Road"];
        let (lat, lng) = (52.0f64, 13.0f64);
        let cos_lat = lat.to_radians().cos();
        let key = |a: &AddrRecord| {
            let d = dist_sq((a.lat as f64 - lat).to_radians(), (a.lng as f64 - lng).to_radians(), cos_lat);
            (d, text[a.housenumber_id as usize], text[a.street_id as usize])
        };
        let wins = |dist: f64, p: &AddrRecord, best_dist: f64, best: Option<&AddrRecord>| {
            beats(dist, best_dist, || best.map(|b| (key(p), key(b))))
        };
        let near = at(52.0001, 13.0, 0, 3);
        let far = at(52.0005, 13.0, 1, 2);
        assert!(wins(0.0, &near, 0.0, Some(&far)));
        assert!(!wins(0.0, &far, 0.0, Some(&near)));
        // "1/2" sorts before "2" although its offset is larger.
        let half = at(52.0001, 13.0, 1, 3);
        assert!(wins(0.0, &half, 0.0, Some(&near)));
        assert!(!wins(0.0, &near, 0.0, Some(&half)));
        let half_angell = at(52.0001, 13.0, 1, 2);
        assert!(wins(0.0, &half_angell, 0.0, Some(&half)));
        // The same candidate seen again (another cell) never replaces itself.
        assert!(!wins(0.0, &half, 0.0, Some(&half)));
        // Distance still comes first, and an equal first candidate never
        // replaces the empty best.
        assert!(wins(1.0, &far, 2.0, Some(&near)));
        assert!(!wins(2.0, &near, 2.0, None));
    }

    #[test]
    fn dist_sq_basic() {
        // cos_lat = 1: plain euclidean squared distance.
        assert!((dist_sq(3.0, 4.0, 1.0) - 25.0).abs() < 1e-12);
        // cos_lat scales the lng component.
        assert!((dist_sq(0.0, 2.0, 0.5) - (2.0_f64 * 2.0 * 0.5 * 0.5)).abs() < 1e-12);
        assert_eq!(dist_sq(0.0, 0.0, 1.0), 0.0);
    }

    #[test]
    fn point_to_segment_with_t_clamping() {
        // Segment from (0,0) to (0,10) along lng axis, cos_lat=1.
        // Projection of (0,5) lands at the midpoint, t=0.5, dist 0.
        let (d, t) = point_to_segment_with_t(0.0, 5.0, 0.0, 0.0, 0.0, 10.0, 1.0);
        assert!((t - 0.5).abs() < 1e-12);
        assert!(d < 1e-12);

        // Point before the start clamps t to 0.
        let (_, t0) = point_to_segment_with_t(0.0, -5.0, 0.0, 0.0, 0.0, 10.0, 1.0);
        assert_eq!(t0, 0.0);

        // Point past the end clamps t to 1.
        let (_, t1) = point_to_segment_with_t(0.0, 50.0, 0.0, 0.0, 0.0, 10.0, 1.0);
        assert_eq!(t1, 1.0);
    }

    #[test]
    fn a_segment_is_as_far_as_its_point_nearest_in_metres_at_any_latitude() {
        // A line running north-east whose nearest point lies 100 m south-east of the point. A
        // degree of longitude is cos(lat) of a degree of latitude long, so away from the
        // equator the line's point nearest in degrees is not the one nearest in metres: at 60°
        // north it is 117 m away.
        let metres_per_degree = 6_371_000.0_f64.to_radians();
        let (half, foot) = (300.0 / 2f64.sqrt(), 100.0 / 2f64.sqrt());
        for lat in [0.0_f64, 30.0, 52.0, 60.0, 75.0] {
            let cos_lat = lat.to_radians().cos();
            let at = |north: f64, east: f64| (lat + north / metres_per_degree, 10.0 + east / (metres_per_degree * cos_lat));
            let (a, b) = (at(-foot - half, foot - half), at(-foot + half, foot + half));
            let metres = point_to_segment_distance(lat, 10.0, a.0, a.1, b.0, b.1, cos_lat).sqrt() * 6_371_000.0;
            assert!((metres - 100.0).abs() < 1e-6, "{lat}°: {metres} m");
        }
    }

    #[test]
    fn point_to_segment_with_t_zero_length() {
        // Degenerate (zero-length) segment: t forced to 0, distance is the
        // radian-space distance to the single point.
        let (d, t) = point_to_segment_with_t(0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0);
        assert_eq!(t, 0.0);
        // dlng = 1 degree in radians, dlat = 0.
        let expect = dist_sq(0.0_f64.to_radians(), 1.0_f64.to_radians(), 1.0);
        assert!((d - expect).abs() < 1e-18);
    }

    #[test]
    fn polygon_distance_sq_inside_is_zero() {
        let sq = unit_square();
        // Inside -> exactly 0.0.
        assert_eq!(polygon_distance_sq(0.5, 0.5, &sq, 1.0), 0.0);
        // Empty polygon -> f64::MAX (guarded before the ray cast).
        assert_eq!(polygon_distance_sq(0.5, 0.5, &[], 1.0), f64::MAX);
    }

    #[test]
    fn polygon_distance_sq_outside_matches_nearest_edge() {
        let sq = unit_square();
        // Point at (0.5, 2.0): nearest edge is lng=1 at lat 0.5, so the
        // perpendicular distance is 1 degree of lng (in radians, cos_lat=1).
        let got = polygon_distance_sq(0.5, 2.0, &sq, 1.0);
        let expect = dist_sq(0.0, 1.0_f64.to_radians(), 1.0);
        assert!((got - expect).abs() < 1e-12, "got {got} expect {expect}");
    }

    #[test]
    fn vertex_scale_snapshot() {
        assert_eq!(vertex_scale(0), 1e-5);
        assert_eq!(vertex_scale(1), 1e-4);
        assert_eq!(vertex_scale(2), 1e-6);
        assert_eq!(vertex_scale(3), 1e-7);
        // F32 (and unknown) report scale 0.0 (unused for the f32 path).
        assert_eq!(vertex_scale(4), 0.0);
        assert_eq!(vertex_scale(99), 0.0);
    }
}

/// Query-path tests: a small index in the builder's file layout, written by
/// TestIndex, loaded by Index::load and asked through Index::query. They pin
/// how find_admin, find_current_boundaries and find_places are wired together,
/// which the helper tests cannot see, and what a query reads from the street,
/// address and interpolation files and the side files beside them.
#[cfg(test)]
mod query_path_tests {
    use super::*;
    use s2::cell::Cell;
    use s2::rect::Rect;
    use s2::region::RegionCoverer;
    use crate::scratch_dir::ScratchDir;
    use std::collections::BTreeMap;

    mod crosscheck;
    mod display_name;
    mod postcodes;
    mod ranks;
    mod reach;

    /// The fixtures' origin unless one is given: everything sits metres
    /// north and east of it.
    const ORIGIN: (f64, f64) = (10.0, 20.0);
    const COUNTRY_CODE: u16 = (b'x' as u16) << 8 | b'x' as u16;

    /// A rectangle in metres from the origin: (south, west, north, east).
    type Span = (f64, f64, f64, f64);

    fn at(north_m: f64, east_m: f64) -> NodeCoord {
        at_from(ORIGIN, north_m, east_m)
    }

    /// The point `north_m` north and `east_m` east of `origin`.
    fn at_from((lat0, lng0): (f64, f64), north_m: f64, east_m: f64) -> NodeCoord {
        let metres_per_lng = METRES_PER_DEGREE * lat0.to_radians().cos();
        NodeCoord {
            lat: (lat0 + north_m / METRES_PER_DEGREE) as f32,
            lng: (lng0 + east_m / metres_per_lng) as f32,
        }
    }

    /// (lat_lo, lng_lo, lat_hi, lng_hi) of the points, in degrees.
    fn bounds(points: &[NodeCoord]) -> (f64, f64, f64, f64) {
        points.iter().fold((f64::MAX, f64::MAX, f64::MIN, f64::MIN), |(a, b, c, d), p| {
            (a.min(p.lat as f64), b.min(p.lng as f64), c.max(p.lat as f64), d.max(p.lng as f64))
        })
    }

    /// The cells of `level` meeting the box of `points`, widened by a hair so
    /// a line along a meridian still has width.
    fn cells_meeting(points: &[NodeCoord], level: u64) -> Vec<u64> {
        const HAIR: f64 = 1e-6;
        let (lat_lo, lng_lo, lat_hi, lng_hi) = bounds(points);
        let rect = Rect::from_degrees(lat_lo - HAIR, lng_lo - HAIR, lat_hi + HAIR, lng_hi + HAIR);
        let coverer = RegionCoverer { min_level: level as u8, max_level: level as u8, level_mod: 1, max_cells: 1 << 16 };
        coverer.covering(&rect).0.iter().map(|c| c.0).collect()
    }

    /// A street or an interpolation way as TestIndex holds one. A tombstone has no nodes.
    struct Way {
        /// A street's name_id, an interpolation's street_id.
        name_id: u32,
        nodes: Vec<NodeCoord>,
        /// An interpolation's start_number, end_number and interpolation; zeros for a street.
        numbers: (u32, u32, u8),
    }

    /// An address point as TestIndex holds one.
    struct Addr {
        point: NodeCoord,
        housenumber_id: u32,
        street_id: u32,
        parent_way_id: u32,
        /// A building's outline; none for a node.
        footprint: Option<Footprint>,
    }

    /// A building's outline as its logical blob holds one at vertex encoding 2 (the builder's
    /// U16_011M, which a building's span takes): each vertex in steps of 1e-6 degrees north and
    /// east of `min`.
    struct Footprint {
        min: NodeCoord,
        steps: Vec<(u16, u16)>,
    }

    impl Footprint {
        const ENCODING: u8 = 2;

        /// The outline through `ring`, as the builder's pack_polygon rounds it.
        fn of(ring: &[NodeCoord]) -> Footprint {
            let (lat_lo, lng_lo, _, _) = bounds(ring);
            let step = |v: f32, lo: f64| ((v as f64 - lo) / vertex_scale(Self::ENCODING)).round() as u16;
            Footprint {
                min: NodeCoord { lat: lat_lo as f32, lng: lng_lo as f32 },
                steps: ring.iter().map(|v| (step(v.lat, lat_lo), step(v.lng, lng_lo))).collect(),
            }
        }
    }

    /// The two files of `ways` as the builder writes them (way_files.rs): street_ways.bin and
    /// street_nodes.bin, or for interpolation ways interp_ways.bin and interp_nodes.bin.
    fn way_pair(ways: &[Way], kind: way_files::Kind) -> [(&'static str, Vec<u8>); 2] {
        let rows: Vec<(way_files::WayRow, Vec<NodeCoord>)> = ways.iter()
            .map(|way| {
                let (start_number, end_number, interpolation) = way.numbers;
                let node_count = u16::try_from(way.nodes.len()).unwrap();
                (way_files::WayRow { node_count, name: way.name_id, start_number, end_number, interpolation }, way.nodes.clone())
            })
            .collect();
        let files = way_files::encode(kind, &rows);
        [(kind.ways_file(), files.ways), (kind.nodes_file(), files.nodes)]
    }

    /// The logical pair of `addrs`, addr_points.bin and addr_vertices.bin: a record of seven
    /// u32s an address, and each footprint's logical blob (its 10-byte header, then its steps as
    /// u16 pairs) at the record's vertex_offset.
    fn addr_logical(addrs: &[Addr]) -> (Vec<u8>, Vec<u8>) {
        let (mut points, mut vertices) = (Vec::new(), Vec::new());
        for addr in addrs {
            let (vertex_offset, vertex_count) = match &addr.footprint {
                Some(footprint) => (addr_files::vertex_offset_of(vertices.len() as u64).unwrap(), footprint.steps.len() as u32),
                None => (NO_DATA, 0),
            };
            let fields = [addr.point.lat.to_bits(), addr.point.lng.to_bits(), addr.housenumber_id, addr.street_id,
                          addr.parent_way_id, vertex_offset, vertex_count];
            points.extend(fields.iter().flat_map(|field| field.to_le_bytes()));
            if let Some(Footprint { min, steps }) = &addr.footprint {
                vertices.extend_from_slice(&[Footprint::ENCODING, 0]);
                vertices.extend_from_slice(&min.lat.to_le_bytes());
                vertices.extend_from_slice(&min.lng.to_le_bytes());
                for (north, east) in steps {
                    vertices.extend_from_slice(&north.to_le_bytes());
                    vertices.extend_from_slice(&east.to_le_bytes());
                }
            }
        }
        (points, vertices)
    }

    /// The three files of `addrs` as the builder writes them (addr_files.rs): addr_points.bin,
    /// addr_vertices.bin and addr_index.bin, from their logical pair.
    fn addr_triple(addrs: &[Addr]) -> [(&'static str, Vec<u8>); 3] {
        let (points, vertices) = addr_logical(addrs);
        let files = addr_files::encode(&points, &vertices);
        [(addr_files::POINTS_FILE, files.points), (addr_files::VERTICES_FILE, files.vertices), (addr_files::INDEX_FILE, files.index)]
    }

    #[derive(Default)]
    struct TestIndex {
        origin: (f64, f64),
        strings: Vec<u8>,
        polygons: Vec<(AdminPolygon, Vec<NodeCoord>)>,
        place_nodes: Vec<PlaceNode>,
        streets: Vec<Way>,
        addrs: Vec<Addr>,
        interps: Vec<Way>,
        /// The street cells' lists of streets, addresses and interpolations.
        geo_cells: BTreeMap<u64, [Vec<u32>; geo_index::TABLES]>,
        /// What each side file holds other than NO_DATA, as (record, value): a street's parent
        /// polygon, its postcode, an address's postcode and addr:city, and an interpolation's
        /// postcode.
        way_parents: Vec<(u32, u32)>,
        way_postcodes: Vec<(u32, u32)>,
        addr_postcodes: Vec<(u32, u32)>,
        addr_cities: Vec<(u32, u32)>,
        interp_postcodes: Vec<(u32, u32)>,
        /// The postcode centroids, each one's point, postcode and country.
        centroids: Vec<postcode_files::Centroid>,
        /// The postcode grid's runs, in cell order.
        grid: Vec<postcode_grid::LogicalRow>,
    }

    impl TestIndex {
        /// Testland, a country (rank 4) 100 km across around the origin.
        fn new() -> Self {
            Self::around(ORIGIN)
        }

        /// Testland around `origin`, which everything is placed from.
        fn around(origin: (f64, f64)) -> Self {
            let mut index = Self { origin, ..Self::default() };
            index.boundary("Testland", 2, 0, (-50_000.0, -50_000.0, 50_000.0, 50_000.0));
            index
        }

        /// Testland under the country code `code`.
        fn in_country(code: &[u8; 2]) -> Self {
            let mut index = Self::new();
            index.polygons[0].0.country_code = u16::from_be_bytes(*code);
            index
        }

        fn string(&mut self, s: &str) -> u32 {
            let offset = self.strings.len() as u32;
            self.strings.extend_from_slice(s.as_bytes());
            self.strings.push(0);
            offset
        }

        /// The offset of the first string `s` already in the pool, as the builder's pool holds
        /// each string once; a new string where there is none.
        fn interned(&mut self, s: &str) -> u32 {
            let mut at = 0;
            for held in self.strings.split(|&b| b == 0) {
                if at < self.strings.len() && held == s.as_bytes() {
                    return at as u32;
                }
                at += held.len() + 1;
            }
            self.string(s)
        }

        /// A rectangular boundary; admin_level 15 makes it a place area.
        fn boundary(&mut self, name: &str, admin_level: u8, place_type_override: u8, (s, w, n, e): Span) -> &mut Self {
            let at = |north_m, east_m| at_from(self.origin, north_m, east_m);
            let ring = vec![at(s, w), at(s, e), at(n, e), at(n, w), at(s, w)];
            let (lat_lo, lng_lo, lat_hi, lng_hi) = bounds(&ring);
            // The builder gives a country to countries, and to postal areas the one their centroid
            // lies in: here the first country's, unless a test sets another.
            let country_code = match admin_level {
                2 => COUNTRY_CODE,
                POSTCODE_AREA_LEVEL => self.polygons.first().map_or(0, |(country, _)| country.country_code),
                _ => 0,
            };
            let poly = AdminPolygon {
                place_type_override, country_code,
                ..arc_fixture::record(self.string(name), admin_level, ((lat_hi - lat_lo) * (lng_hi - lng_lo)) as f32)
            };
            self.polygons.push((poly, ring));
            self
        }

        /// Another outer ring of the boundary added last, a polygon of its own with the
        /// boundary's record (name, level, place types, country), as the builder writes each
        /// outer ring of a relation.
        fn boundary_ring(&mut self, (s, w, n, e): Span) -> &mut Self {
            let at = |north_m, east_m| at_from(self.origin, north_m, east_m);
            let ring = vec![at(s, w), at(s, e), at(n, e), at(n, w), at(s, w)];
            let (lat_lo, lng_lo, lat_hi, lng_hi) = bounds(&ring);
            let poly = AdminPolygon { area: ((lat_hi - lat_lo) * (lng_hi - lng_lo)) as f32, ..self.polygons.last().unwrap().0 };
            self.polygons.push((poly, ring));
            self
        }

        /// A boundary through `corners_m`, (north, east) metres each, closed: a rectangle with
        /// vertices along its edges too, which a street's nodes may share, its area its box's.
        fn boundary_through(&mut self, name: &str, admin_level: u8, place_type_override: u8, corners_m: &[(f64, f64)]) -> &mut Self {
            let span = corners_m.iter().fold((f64::MAX, f64::MAX, f64::MIN, f64::MIN), |(s, w, n, e), &(north, east)| {
                (s.min(north), w.min(east), n.max(north), e.max(east))
            });
            self.boundary(name, admin_level, place_type_override, span);
            let origin = self.origin;
            let mut ring: Vec<NodeCoord> = corners_m.iter().map(|&(north, east)| at_from(origin, north, east)).collect();
            ring.push(ring[0]);
            self.polygons.last_mut().unwrap().1 = ring;
            self
        }

        /// Makes the polygon added last a closed way's, not a relation's.
        fn of_way(&mut self) -> &mut Self {
            self.polygons.last_mut().unwrap().0.of_way = true;
            self
        }

        /// A place node of PlaceNode type `place_type`, its name a string of the pool's, as the
        /// builder's pool holds each string once.
        fn place(&mut self, name: &str, place_type: u8, north_m: f64, east_m: f64) -> &mut Self {
            let NodeCoord { lat, lng } = at_from(self.origin, north_m, east_m);
            let name_id = self.interned(name);
            self.place_nodes.push(PlaceNode { lat, lng, name_id, place_type, _pad1: 0, _pad2: 0, parent_poly_id: NO_DATA });
            self
        }

        /// A place node a boundary claims, which keeps its name for place search: hidden.
        fn claimed_place(&mut self, name: &str, place_type: u8, north_m: f64, east_m: f64) -> &mut Self {
            self.place(name, place_type, north_m, east_m);
            self.place_nodes.last_mut().unwrap()._pad1 = PLACE_FLAG_HIDDEN;
            self
        }

        /// A place node the polygon added last links (find_linked_place): hidden, the polygon's
        /// label.
        fn linked_place(&mut self, name: &str, place_type: u8, north_m: f64, east_m: f64) -> &mut Self {
            self.place(name, place_type, north_m, east_m);
            let polygon = self.polygons.len() as u32 - 1;
            let node = self.place_nodes.last_mut().unwrap();
            node._pad1 = PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF;
            node.parent_poly_id = polygon;
            self
        }

        /// Lists record `id` of geo table `table` in the street cell `cell`.
        fn list(&mut self, table: usize, id: u32, cell: u64) {
            let ids = &mut self.geo_cells.entry(cell).or_default()[table];
            if !ids.contains(&id) { ids.push(id); }
        }

        /// The nodes of a way through points given as (north, east) metres.
        fn nodes_at(&self, points_m: &[(f64, f64)]) -> Vec<NodeCoord> {
            points_m.iter().map(|&(n, e)| at_from(self.origin, n, e)).collect()
        }

        /// Lists record `id` of geo table `table`, a way through `nodes`, in the street cells
        /// the builder lists each of its edges in (edge_cells).
        fn list_way(&mut self, table: usize, id: usize, nodes: &[NodeCoord]) {
            for segment in nodes.windows(2) {
                for cell in edge_cells(segment[0], segment[1]) {
                    self.list(table, id as u32, cell);
                }
            }
        }

        /// A named street through points given as (north, east) metres.
        fn street(&mut self, name: &str, points_m: &[(f64, f64)]) -> &mut Self {
            let nodes = self.nodes_at(points_m);
            self.street_through(name, nodes)
        }

        /// A named street through `nodes`.
        fn street_through(&mut self, name: &str, nodes: Vec<NodeCoord>) -> &mut Self {
            let name_id = self.string(name);
            self.list_way(geo_index::STREETS, self.streets.len(), &nodes);
            self.streets.push(Way { name_id, nodes, numbers: (0, 0, 0) });
            self
        }

        /// An interpolation way of the street named `street`, from house number `start` at its
        /// first point to `end` at its last; `interpolation` is 0 for every number, 1 for the
        /// even ones, 2 for the odd ones.
        fn interpolation(&mut self, street: &str, numbers: (u32, u32, u8), points_m: &[(f64, f64)]) -> &mut Self {
            let nodes = self.nodes_at(points_m);
            self.interpolation_through(street, numbers, nodes)
        }

        /// An interpolation way of the street named `street` through `nodes`, numbered as
        /// `interpolation` numbers one.
        fn interpolation_through(&mut self, street: &str, numbers: (u32, u32, u8), nodes: Vec<NodeCoord>) -> &mut Self {
            let name_id = self.string(street);
            self.list_way(geo_index::INTERPOLATIONS, self.interps.len(), &nodes);
            self.interps.push(Way { name_id, nodes, numbers });
            self
        }

        /// An address at `point`, listed in its street cell, on the street added last: its
        /// street by name and its parent way. Before any street it has neither.
        fn address_point(&mut self, house_number: &str, point: NodeCoord, footprint: Option<Footprint>) -> &mut Self {
            let housenumber_id = self.string(house_number);
            let (street_id, parent_way_id) = match self.streets.last() {
                Some(street) => (street.name_id, self.streets.len() as u32 - 1),
                None => (NO_DATA, NO_DATA),
            };
            let cell = cell_id_at_level(point.lat as f64, point.lng as f64, DEFAULT_STREET_CELL_LEVEL);
            self.list(geo_index::ADDRESSES, self.addrs.len() as u32, cell);
            self.addrs.push(Addr { point, housenumber_id, street_id, parent_way_id, footprint });
            self
        }

        /// An address node with a house number, on the street added last.
        fn address(&mut self, house_number: &str, north_m: f64, east_m: f64) -> &mut Self {
            self.address_point(house_number, at_from(self.origin, north_m, east_m), None)
        }

        /// A building with a house number on the street added last: its outline the
        /// rectangle, its point the rectangle's middle.
        fn building(&mut self, house_number: &str, (s, w, n, e): Span) -> &mut Self {
            let at = |north_m, east_m| at_from(self.origin, north_m, east_m);
            let ring = [at(s, w), at(s, e), at(n, e), at(n, w), at(s, w)];
            let middle = at((s + n) / 2.0, (w + e) / 2.0);
            self.address_point(house_number, middle, Some(Footprint::of(&ring)))
        }

        /// The tombstone a chained build leaves in street_ways.bin where a street was (builder
        /// cell_index.cpp). No cell lists it.
        fn dead_street(&mut self) -> &mut Self {
            self.streets.push(Way { name_id: NO_DATA, nodes: Vec::new(), numbers: (0, 0, 0) });
            self
        }

        /// The tombstone of an address point, which no cell lists.
        fn dead_address(&mut self) -> &mut Self {
            let nowhere = NodeCoord { lat: 0.0, lng: 0.0 };
            self.addrs.push(Addr { point: nowhere, housenumber_id: 0, street_id: 0, parent_way_id: NO_DATA, footprint: None });
            self
        }

        /// The tombstone of an interpolation way, which no cell lists.
        fn dead_interpolation(&mut self) -> &mut Self {
            self.interps.push(Way { name_id: NO_DATA, nodes: Vec::new(), numbers: (0, 0, 0) });
            self
        }

        /// Lists record `id` of geo table `table` in the street cell at the point as well, as
        /// no builder lists a tombstone.
        fn listed(&mut self, table: usize, id: u32, north_m: f64, east_m: f64) -> &mut Self {
            let p = at_from(self.origin, north_m, east_m);
            self.list(table, id, cell_id_at_level(p.lat as f64, p.lng as f64, DEFAULT_STREET_CELL_LEVEL));
            self
        }

        /// Admin polygon `polygon` as the parent of the street added last.
        fn way_parent(&mut self, polygon: u32) -> &mut Self {
            self.way_parents.push((self.streets.len() as u32 - 1, polygon));
            self
        }

        /// The postcode of the street added last.
        fn way_postcode(&mut self, postcode: &str) -> &mut Self {
            let postcode_id = self.string(postcode);
            self.way_postcodes.push((self.streets.len() as u32 - 1, postcode_id));
            self
        }

        /// The postcode of the address added last.
        fn addr_postcode(&mut self, postcode: &str) -> &mut Self {
            let postcode_id = self.string(postcode);
            self.addr_postcodes.push((self.addrs.len() as u32 - 1, postcode_id));
            self
        }

        /// The addr:city of the address added last, a string of the pool's.
        fn addr_city(&mut self, city: &str) -> &mut Self {
            let city_id = self.interned(city);
            self.addr_cities.push((self.addrs.len() as u32 - 1, city_id));
            self
        }

        /// The postcode of the interpolation way added last.
        fn interp_postcode(&mut self, postcode: &str) -> &mut Self {
            let postcode_id = self.string(postcode);
            self.interp_postcodes.push((self.interps.len() as u32 - 1, postcode_id));
            self
        }

        /// A postcode centroid of Testland's country.
        fn centroid(&mut self, postcode: &str, north_m: f64, east_m: f64) -> &mut Self {
            let NodeCoord { lat, lng } = at_from(self.origin, north_m, east_m);
            let postcode_id = self.string(postcode);
            self.centroids.push(postcode_files::Centroid { lat, lng, postcode_id, country_code: self.polygons[0].0.country_code });
            self
        }

        /// A run of the postcode grid from `cell`, a level-16 cell, of Testland's country and
        /// `postcode`, or of no postcode for "": runs go in cell order.
        fn grid_run(&mut self, postcode: &str, cell: u64) -> &mut Self {
            let country_code = if postcode.is_empty() { 0 } else { self.polygons[0].0.country_code };
            self.grid.push(postcode_grid::LogicalRow { cell, country_code, code: postcode.to_string() });
            self
        }

        /// The side files with a value set, each with its values: NO_DATA for every other record
        /// of the file it sits beside.
        fn side_values(&self) -> Vec<(&'static str, Vec<u32>)> {
            let side_files = [
                ("way_parents.bin", &self.way_parents, self.streets.len()),
                ("way_postcodes.bin", &self.way_postcodes, self.streets.len()),
                ("addr_postcodes.bin", &self.addr_postcodes, self.addrs.len()),
                ("addr_cities.bin", &self.addr_cities, self.addrs.len()),
                ("interp_postcodes.bin", &self.interp_postcodes, self.interps.len()),
            ];
            side_files.into_iter()
                .filter(|(_, set, _)| !set.is_empty())
                .map(|(name, set, records)| {
                    let mut values = vec![NO_DATA; records];
                    for &(record, value) in set {
                        values[record as usize] = value;
                    }
                    (name, values)
                })
                .collect()
        }

        /// The files as the builder writes them, by name. A boundary is
        /// listed in every admin cell meeting its box, flagged interior where
        /// the cell lies inside it; a place node in its own cell; a street or
        /// an interpolation way in the street cells along it; an address in
        /// the street cell of its point. The street files come with the geo
        /// index, the interpolation and address files where there are such
        /// records, a side file (side_blocks.rs) where one of its values is
        /// set, with NO_DATA for every other record.
        fn files(&self) -> Vec<(&'static str, Vec<u8>)> {
            let mut arcs = arc_fixture::ArcFixture::new();
            let mut admin_cells: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
            for (id, (poly, ring)) in self.polygons.iter().enumerate() {
                let (lat_lo, lng_lo, lat_hi, lng_hi) = bounds(ring);
                let e7_ring: Vec<PointE7> = ring.iter().map(|v| arc_fixture::e7(v.lat as f64, v.lng as f64)).collect();
                arcs.ring_polygon(*poly, 0, &[e7_ring]);
                for cell in cells_meeting(ring, DEFAULT_ADMIN_CELL_LEVEL) {
                    let b = Cell::from(CellID(cell)).rect_bound();
                    let interior = b.lat_lo().deg() > lat_lo && b.lat_hi().deg() < lat_hi
                        && b.lng_lo().deg() > lng_lo && b.lng_hi().deg() < lng_hi;
                    admin_cells.entry(cell).or_default().push(id as u32 | if interior { INTERIOR_FLAG } else { 0 });
                }
            }
            let (cells, entries) = cell_pairs::write(&admin_cells.into_iter().collect::<Vec<_>>(), cell_pairs::ADMIN);
            let mut files: Vec<(&'static str, Vec<u8>)> = arcs.files().into();
            files.extend([("admin_cells.bin", cells), ("admin_entries.bin", entries)]);

            if !self.place_nodes.is_empty() {
                let (places, _) = place_files::encode(&self.place_nodes, DEFAULT_ADMIN_CELL_LEVEL);
                files.extend([(place_files::CELLS_FILE, places.cells), (place_files::NODES_FILE, places.nodes),
                              (place_files::HIDDEN_FILE, places.hidden)]);
            }

            if !self.centroids.is_empty() {
                let (centroids, cells) = postcode_files::encode_points(&self.centroids, DEFAULT_ADMIN_CELL_LEVEL);
                files.extend([(postcode_files::CENTROIDS_FILE, centroids), (postcode_files::CELLS_FILE, cells)]);
            }

            if !self.grid.is_empty() {
                files.push((postcode_grid::FILE, postcode_grid::encode(&self.grid)));
            }

            if !(self.streets.is_empty() && self.addrs.is_empty() && self.interps.is_empty()) {
                let rows: Vec<(u64, [Vec<u32>; geo_index::TABLES])> = self.geo_cells.iter()
                    .map(|(&cell, lists)| (cell, lists.clone().map(|mut ids| { ids.sort_unstable(); ids })))
                    .collect();
                let (top, blocks) = geo_index::write(&rows);
                files.extend([("geo_cells.bin", top), ("geo_entries.bin", blocks)]);
                files.extend(way_pair(&self.streets, way_files::Kind::Streets));
                if !self.interps.is_empty() {
                    files.extend(way_pair(&self.interps, way_files::Kind::Interps));
                }
                if !self.addrs.is_empty() {
                    files.extend(addr_triple(&self.addrs));
                }
                files.extend(self.side_values().into_iter().map(|(name, values)| (name, side_blocks::encode(&values))));
            }

            let end = self.strings.len();
            let tiers: Vec<String> = (0..STR_TIER_COUNT)
                .map(|t| format!("{{\"start\": {}, \"end\": {}}}", if t == 0 { 0 } else { end }, end))
                .collect();
            files.push(("strings_layout.json", format!("{{\"tiers\": [{}]}}", tiers.join(", ")).into_bytes()));
            files.push(("strings_core.bin", strings::encode(&self.strings)));
            files
        }

        /// Writes the files into `dir` the way a patch replaces one: a new file renamed over it.
        fn write(&self, dir: &std::path::Path) {
            for (name, bytes) in self.files() {
                let new = dir.join(format!("{name}.new"));
                std::fs::write(&new, bytes).unwrap();
                std::fs::rename(&new, dir.join(name)).unwrap();
            }
        }

        /// Writes the files into a fresh directory and loads them. The
        /// directory is removed once loaded; the maps keep the bytes.
        fn load(&self) -> Index {
            self.load_reaching(DEFAULT_SEARCH_DISTANCE)
        }

        /// As load, the geo lookups reaching `search_distance` metres.
        fn load_reaching(&self, search_distance: f64) -> Index {
            let dir = ScratchDir::new();
            self.write(dir.path());
            Index::load(dir.path().to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, search_distance).unwrap()
        }
    }

    /// The street cells the builder lists an edge from `a` to `b` in (cover_edge,
    /// builder/src/s2_helpers.cpp): the cell of both ends where they share one; the cells of its
    /// two ends alone where its middle in degrees lies in one of them, though it may cross a third
    /// by their corner; else the cells its great circle crosses. Those are here the cells of
    /// points of its great circle 25 cm apart, which may leave out a cell it clips by less, never
    /// list one it misses.
    fn edge_cells(a: NodeCoord, b: NodeCoord) -> Vec<u64> {
        let cell = |lat: f64, lng: f64| cell_id_at_level(lat, lng, DEFAULT_STREET_CELL_LEVEL);
        let (alat, alng, blat, blng) = (a.lat as f64, a.lng as f64, b.lat as f64, b.lng as f64);
        let (first, last) = (cell(alat, alng), cell(blat, blng));
        if first == last {
            return vec![first];
        }
        let middle = cell((alat + blat) / 2.0, (alng + blng) / 2.0);
        if middle == first || middle == last {
            return vec![first, last];
        }
        let (pa, pb) = (unit_vector(alat, alng), unit_vector(blat, blng));
        let cross = [pa[1] * pb[2] - pa[2] * pb[1], pa[2] * pb[0] - pa[0] * pb[2], pa[0] * pb[1] - pa[1] * pb[0]];
        let angle = (cross[0] * cross[0] + cross[1] * cross[1] + cross[2] * cross[2]).sqrt()
            .atan2(pa[0] * pb[0] + pa[1] * pb[1] + pa[2] * pb[2]);
        let steps = (angle * 6_371_000.0 / 0.25).ceil() as usize;
        let mut cells: Vec<u64> = (0..=steps)
            .map(|k| {
                let t = k as f64 / steps as f64;
                let (wa, wb) = (((1.0 - t) * angle).sin(), (t * angle).sin());
                let p = [wa * pa[0] + wb * pb[0], wa * pa[1] + wb * pb[1], wa * pa[2] + wb * pb[2]];
                cell(p[2].atan2(p[0].hypot(p[1])).to_degrees(), p[1].atan2(p[0]).to_degrees())
            })
            .collect();
        cells.sort_unstable();
        cells.dedup();
        cells
    }

    fn load_dir(dir: &std::path::Path) -> Index {
        Index::load(dir.to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).unwrap()
    }

    #[test]
    fn a_dir_holds_its_postcode_centroids_as_both_files_or_neither() {
        // Two centroids in one admin cell and one 20 km south in another.
        let mut dataset = TestIndex::new();
        dataset.centroid("10115", 100.0, 0.0).centroid("10117", 2_000.0, 0.0).centroid("12049", -20_000.0, 0.0);
        let dir = ScratchDir::new();
        dataset.write(dir.path());
        let idx = load_dir(dir.path());
        assert_eq!(idx.postcode_centroids.as_ref().map(PostcodeFiles::rows), Some(3));
        for (north, postcode) in [(0.0, "10115"), (2_100.0, "10117"), (-19_000.0, "12049")] {
            assert_eq!(address_at(&idx, north, 0.0)["postcode"], postcode, "{north} m north");
        }
        // No centroid within 5 km of the point along either axis, the extent of its postcodes.
        assert_eq!(address_at(&idx, 10_000.0, 0.0)["postcode"], serde_json::Value::Null);

        // Without either file the dir has no centroids; one without the other is refused.
        let path = |name: &str| dir.path().join(name);
        let (centroids, cells) = (std::fs::read(path(postcode_files::CENTROIDS_FILE)).unwrap(), std::fs::read(path(postcode_files::CELLS_FILE)).unwrap());
        let load = || Index::load(dir.path().to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE);
        std::fs::remove_file(path(postcode_files::CELLS_FILE)).unwrap();
        assert!(load().err().unwrap().ends_with(&format!("missing [{}]", postcode_files::CELLS_FILE)));
        std::fs::remove_file(path(postcode_files::CENTROIDS_FILE)).unwrap();
        let without = load().unwrap();
        assert!(without.postcode_centroids.is_none());
        assert_eq!(address_at(&without, 0.0, 0.0)["postcode"], serde_json::Value::Null);
        std::fs::write(path(postcode_files::CELLS_FILE), &cells).unwrap();
        assert!(load().err().unwrap().ends_with(&format!("missing [{}]", postcode_files::CENTROIDS_FILE)));

        // The files of no centroid: the footer alone and an empty cells file, which answer none.
        std::fs::write(path(postcode_files::CENTROIDS_FILE), [0, 0, 0, 0, b'P', b'C', b'C', b'1']).unwrap();
        std::fs::write(path(postcode_files::CELLS_FILE), []).unwrap();
        let none = load().unwrap();
        assert_eq!(none.postcode_centroids.as_ref().map(PostcodeFiles::rows), Some(0));
        assert_eq!(address_at(&none, 0.0, 0.0)["postcode"], serde_json::Value::Null);

        // Files no builder writes: 16-byte records without a footer, and the centroids' cells
        // file beside another dir's centroids.
        std::fs::write(path(postcode_files::CENTROIDS_FILE), [0u8; 32]).unwrap();
        std::fs::write(path(postcode_files::CELLS_FILE), &cells).unwrap();
        assert!(load().err().unwrap().contains("postcode_centroids.bin has no PCC1 footer"));
        let (other, _) = postcode_files::encode_points(&dataset.centroids[..2], DEFAULT_ADMIN_CELL_LEVEL);
        std::fs::write(path(postcode_files::CENTROIDS_FILE), other).unwrap();
        assert!(load().err().unwrap().contains(postcode_files::CELLS_FILE));
        std::fs::write(path(postcode_files::CENTROIDS_FILE), &centroids).unwrap();
        assert_eq!(load().unwrap().postcode_centroids.as_ref().map(PostcodeFiles::rows), Some(3));
    }

    /// Testland with Oldtown, a village 1 km north and east of the origin, and Lowfield, a hamlet
    /// 20 km south of it in another admin cell.
    fn oldtown() -> TestIndex {
        let mut index = TestIndex::new();
        index.place("Oldtown", 2, 1_000.0, 1_000.0).place("Lowfield", 4, -20_000.0, 0.0);
        index
    }

    #[test]
    fn a_dataset_finds_its_places_in_their_cells() {
        let index = oldtown().load();

        assert_eq!(address_at(&index, 1_000.0, 1_100.0)["village"], "Oldtown");
        assert_eq!(address_at(&index, -20_000.0, 100.0)["hamlet"], "Lowfield");
    }

    /// A lake 1 to 3 km north and east of `origin`, with an island 1.8 to 2.2 km north and east.
    fn lake_near(origin: (f64, f64)) -> crate::water::fixture::WaterFixture {
        let span = |s: f64, w: f64, n: f64, e: f64| {
            let (sw, ne) = (at_from(origin, s, w), at_from(origin, n, e));
            (sw.lat as f64, sw.lng as f64, ne.lat as f64, ne.lng as f64)
        };
        let mut water = crate::water::fixture::WaterFixture::new();
        water.area("Testsee", crate::water::fixture::LAKE, span(1_000.0, 1_000.0, 3_000.0, 3_000.0), &[span(1_800.0, 1_800.0, 2_200.0, 2_200.0)]);
        water
    }

    // A dataset dir's water/ holds its water layer: all of its files, or none of them (a dataset
    // without water, the dir there or not); some without the rest fail the load, naming them.
    #[test]
    fn a_dataset_loads_the_water_layer_of_its_water_dir() {
        let dir = ScratchDir::new();
        oldtown().write(dir.path());
        let water = |idx: &Index, north: f64, east: f64| {
            let p = at(north, east);
            serde_json::to_value(idx.query(p.lat as f64, p.lng as f64)).unwrap()["water"].clone()
        };
        let load = || Index::load(dir.path().to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE);

        assert!(load().unwrap().water.is_none());
        std::fs::create_dir(dir.path().join(WATER_DIR)).unwrap();
        assert!(load().unwrap().water.is_none());
        lake_near(ORIGIN).write(&dir.path().join(WATER_DIR));
        let idx = load().unwrap();
        assert_eq!(water(&idx, 1_500.0, 1_500.0), serde_json::json!({"name": "Testsee", "class": "water", "type": "lake"}));
        assert_eq!((water(&idx, 2_000.0, 2_000.0), water(&idx, 500.0, 500.0)), (serde_json::Value::Null, serde_json::Value::Null));
        assert_eq!(address_at(&idx, 1_500.0, 1_500.0), address_at(&oldtown().load(), 1_500.0, 1_500.0));
        std::fs::remove_file(dir.path().join(WATER_DIR).join("admin_rings.bin")).unwrap();
        assert_eq!(load().err(), Some(format!("Partial water dir {}/{WATER_DIR}: missing [admin_rings.bin]", dir.path().display())));
    }

    // A continent dir's water/ loads with it, when a server starts and when a download is in.
    #[test]
    fn a_continent_loads_the_water_layer_of_its_water_dir() {
        let paris = (48.85, 2.35);
        let root = ScratchDir::new();
        let europe = root.path().join("europe");
        std::fs::create_dir(&europe).unwrap();
        TestIndex::around(paris).write(&europe);
        lake_near(paris).write(&europe.join(WATER_DIR));
        let p = at_from(paris, 1_500.0, 1_500.0);
        let water = |mi: &MultiIndex| serde_json::from_str::<serde_json::Value>(&mi.query_json(p.lat as f64, p.lng as f64)).unwrap()["water"]["name"].clone();
        let load = || MultiIndex::load(root.path().to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).unwrap();

        assert_eq!(water(&load()), "Testsee");
        let late = load();
        late.continents.iter().for_each(|c| c.index.store(None));
        assert_eq!(late.loaded_count(), 0);
        late.install_region("europe").unwrap();
        assert_eq!(water(&late), "Testsee");
    }

    #[test]
    fn a_claimed_place_node_is_no_answer() {
        let mut dataset = oldtown();
        dataset.claimed_place("Claimed", 2, -20_000.0, 0.0);

        let index = dataset.load();

        assert_eq!(index.place_node_count(), 3);
        assert_eq!(address_at(&index, -20_000.0, 100.0).get("village"), None);
        assert_eq!(address_at(&index, -20_000.0, 100.0)["hamlet"], "Lowfield");
    }

    #[test]
    fn replaced_place_files_load_their_places() {
        let dir = ScratchDir::new();
        oldtown().write(dir.path());
        load_dir(dir.path());
        let mut moved = TestIndex::new();
        moved.place("Newtown", 2, -20_000.0, 0.0);

        moved.write(dir.path());
        let index = load_dir(dir.path());

        assert_eq!(address_at(&index, -20_000.0, 100.0)["village"], "Newtown");
        assert_eq!(address_at(&index, 1_000.0, 1_100.0).get("village"), None);
    }

    #[test]
    fn a_dir_with_some_of_the_place_files_does_not_load() {
        for name in [place_files::CELLS_FILE, place_files::NODES_FILE, place_files::HIDDEN_FILE] {
            let dir = ScratchDir::new();
            oldtown().write(dir.path());
            std::fs::remove_file(dir.path().join(name)).unwrap();

            let refused = Index::load(dir.path().to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE);

            assert!(refused.err().unwrap().contains(name), "without {name}");
        }
    }

    /// The address dict of the answer at `north_m`, `east_m` from the origin.
    fn address_at(index: &Index, north_m: f64, east_m: f64) -> serde_json::Value {
        address_from(index, ORIGIN, north_m, east_m)
    }

    /// The address dict of the answer at `north_m`, `east_m` from `origin`.
    fn address_from(index: &Index, origin: (f64, f64), north_m: f64, east_m: f64) -> serde_json::Value {
        let p = at_from(origin, north_m, east_m);
        serde_json::to_value(index.query(p.lat as f64, p.lng as f64)).unwrap()["address"].clone()
    }

    /// The point `north_m`, `east_m` from the origin, as a query takes one.
    fn query_point(north_m: f64, east_m: f64) -> (f64, f64) {
        let p = at(north_m, east_m);
        (p.lat as f64, p.lng as f64)
    }

    /// What query_geo finds nearest the point `north_m`, `east_m` from the origin, as
    /// debug_primary lists it: its `addr`, `street` and `interp`.
    fn primary_at(index: &Index, north_m: f64, east_m: f64) -> serde_json::Value {
        let (lat, lng) = query_point(north_m, east_m);
        index.debug_primary(lat, lng)
    }

    /// Whether a distance debug_primary gives is within a metre of `metres`.
    fn about(dist_m: &serde_json::Value, metres: f64) -> bool {
        dist_m.as_f64().is_some_and(|d| (d - metres).abs() < 1.0)
    }

    /// `metres` as the scans compare distances: in radians, squared.
    fn radians_sq(metres: f64) -> f64 {
        let radians = metres / 6_371_000.0;
        radians * radians
    }

    /// The house number of the address find_addr_on_way gives for street way `way` within
    /// `max_m` of the point.
    fn house_number_on_way(index: &Index, north_m: f64, east_m: f64, way: u32, max_m: f64) -> Option<String> {
        let (lat, lng) = query_point(north_m, east_m);
        index.find_addr_on_way(lat, lng, way, radians_sq(max_m)).map(|(_, a)| index.get_string(a.housenumber_id).into_owned())
    }

    /// The house number find_interp_on_way gives at the point for a street named `street`,
    /// from the interpolation ways within `max_m`.
    fn number_on_street(index: &Index, north_m: f64, east_m: f64, street: &str, max_m: f64) -> Option<u32> {
        let (lat, lng) = query_point(north_m, east_m);
        index.find_interp_on_way(lat, lng, street, radians_sq(max_m))
    }

    /// A line along the parallel `north_m` north of the origin, from `half_m` west of it to
    /// `half_m` east.
    fn parallel(north_m: f64, half_m: f64) -> [(f64, f64); 2] {
        [(north_m, -half_m), (north_m, half_m)]
    }

    /// Two datasets `add` fills, told `true` for the one that takes its two records in the
    /// other order.
    fn in_both_orders(add: impl Fn(&mut TestIndex, bool)) -> [Index; 2] {
        [false, true].map(|swapped| {
            let mut dataset = TestIndex::new();
            add(&mut dataset, swapped);
            dataset.load()
        })
    }

    /// A root with oldtown() in the dirs of asia and europe, as the test
    /// portal lays them out: no stamp of a build, no ledger of a download.
    fn two_continents() -> ScratchDir {
        let root = ScratchDir::new();
        for name in ["asia", "europe"] {
            let dir = root.path().join(name);
            std::fs::create_dir(&dir).unwrap();
            oldtown().write(&dir);
        }
        root
    }

    /// The names of the continents MultiIndex::load loads under `root`.
    fn loaded_continents(root: &std::path::Path) -> Vec<&'static str> {
        let mi = MultiIndex::load(root.to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).unwrap();
        mi.continents.iter().filter(|c| c.index.load().is_some()).map(|c| c.name).collect()
    }

    /// The template's credits (configurations.template.json `attribution`), each without its
    /// `applies_to`: what the builder writes for each source.
    fn template_credits() -> Vec<serde_json::Value> {
        let template: serde_json::Value = serde_json::from_str(include_str!("../../builder/configurations.template.json")).unwrap();
        template["attribution"].as_array().unwrap().iter()
            .map(|e| {
                let mut e = e.clone();
                e.as_object_mut().unwrap().remove("applies_to");
                e
            })
            .collect()
    }

    /// The ids of `credits`, in order.
    fn ids(credits: &[serde_json::Value]) -> Vec<&str> {
        credits.iter().map(|e| e["id"].as_str().unwrap()).collect()
    }

    /// The template's entry `id` (template_credits).
    fn template_entry(id: &str) -> serde_json::Value {
        template_credits().into_iter().find(|e| e["id"] == id).unwrap()
    }

    /// Writes `name` in `dir`: the template's entries of `ids`, as the builder writes a credit file.
    fn write_credits(dir: &std::path::Path, name: &str, ids: &[&str]) {
        let entries: Vec<serde_json::Value> = ids.iter().map(|&id| template_entry(id)).collect();
        std::fs::write(dir.join(name), serde_json::to_string(&entries).unwrap()).unwrap();
    }

    // A server credits the data of the datasets it loaded, as their dirs credit it: each
    // source once. Europe's dataset holds OpenStreetMap's data and GeoNames's, and Wikidata
    // ranked its POIs; Asia's of a build before attribution.json, OpenStreetMap's alone. A
    // dataset whose credit names no GeoNames gives none; one of a minimal selection, of
    // OpenStreetMap alone, OpenStreetMap's.
    #[test]
    fn a_server_credits_the_sources_of_the_datasets_it_loaded() {
        let load = |root: &std::path::Path| {
            MultiIndex::load(root.to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).unwrap()
        };

        let root = two_continents();
        write_credits(&root.path().join("europe"), "attribution.json", &["osm", "geonames", "codepoint-open"]);
        write_credits(&root.path().join("europe"), "poi_attribution.json", &["wikidata"]);
        let credits = load(root.path()).credits();
        assert_eq!(ids(&credits), ["osm", "geonames", "codepoint-open", "wikidata"]);
        // Asia's, first, of no file.
        assert_eq!(credits[0], osm_credit());
        assert_eq!(credits[1], template_entry("geonames"));

        for (written, want) in [(vec!["osm", "tiger"], vec!["osm", "tiger"]), (vec!["osm"], vec!["osm"]), (vec![], vec!["osm"])] {
            let flat = ScratchDir::new();
            oldtown().write(flat.path());
            if !written.is_empty() {
                write_credits(flat.path(), "attribution.json", &written);
            }
            let credits = load(flat.path()).credits();
            assert_eq!(ids(&credits), want, "{written:?}");
            assert_eq!(credits[0], template_entry("osm"));
        }
    }

    // A source two datasets credit is credited once, of the notices of both: Asia's GeoNames
    // centroids, of no GB postcode, owe no GB_full notice, which Europe's, of GB's, do.
    #[test]
    fn a_server_credits_a_source_of_two_datasets_once_of_both_their_notices() {
        let root = two_continents();
        let geonames = template_entry("geonames");
        let notices = geonames["notices"].as_array().unwrap().clone();
        assert!(notices[0].as_str().unwrap().starts_with("UK (GB_full.csv.zip)"));
        let mut without_gb = geonames.clone();
        without_gb["notices"] = serde_json::Value::Array(notices[1..].to_vec());
        let asia = serde_json::json!([template_entry("osm"), without_gb]);
        std::fs::write(root.path().join("asia").join("attribution.json"), asia.to_string()).unwrap();
        write_credits(&root.path().join("europe"), "attribution.json", &["osm", "geonames", "codepoint-open"]);

        let mi = MultiIndex::load(root.path().to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).unwrap();
        let credits = mi.credits();
        assert_eq!(ids(&credits), ["osm", "geonames", "codepoint-open"]);
        let mut merged = credits[1].clone();
        let mut given: Vec<String> = merged["notices"].as_array().unwrap().iter().map(|n| n.as_str().unwrap().to_owned()).collect();
        let mut owed: Vec<String> = notices.iter().map(|n| n.as_str().unwrap().to_owned()).collect();
        given.sort();
        owed.sort();
        assert_eq!(given, owed, "{merged}");
        merged["notices"] = geonames["notices"].clone();
        assert_eq!(merged, geonames);
        assert_eq!(credits[2], template_entry("codepoint-open"));
    }

    // A server credits the datasets it loaded, not those their dirs hold now: a server sharing
    // the data dir may have replaced Europe's selection with a minimal one since, while this one
    // still answers from the files it mapped. Once it loads Europe's dir again it credits what
    // that holds, in a flat dir as in a continent's.
    #[test]
    fn a_server_credits_the_datasets_it_loaded_until_it_loads_their_dirs_again() {
        let root = two_continents();
        let europe = root.path().join("europe");
        write_credits(&europe, "attribution.json", &["osm", "geonames"]);
        write_credits(&europe, "poi_attribution.json", &["wikidata"]);
        let mi = MultiIndex::load(root.path().to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).unwrap();
        write_credits(&europe, "attribution.json", &["osm"]);
        std::fs::remove_file(europe.join("poi_attribution.json")).unwrap();

        assert_eq!(ids(&mi.credits()), ["osm", "geonames", "wikidata"]);
        mi.install_region("europe").unwrap();
        assert_eq!(ids(&mi.credits()), ["osm"]);

        let flat = ScratchDir::new();
        oldtown().write(flat.path());
        write_credits(flat.path(), "attribution.json", &["osm", "tiger"]);
        let mi = MultiIndex::load(flat.path().to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).unwrap();
        std::fs::remove_file(flat.path().join("attribution.json")).unwrap();
        assert_eq!(ids(&mi.credits()), ["osm", "tiger"]);
    }

    // A dataset of no attribution.json is OpenStreetMap's data: its credit is the builder's
    // OpenStreetMap entry, the template's.
    #[test]
    fn the_credit_of_a_dataset_of_no_attribution_file_is_the_templates_osm_entry() {
        assert_eq!(osm_credit(), template_credits()[0]);
    }

    /// Each paragraph of `html`: the hrefs of its links, and its text, tags left out and
    /// entities decoded.
    fn paragraphs(html: &str) -> Vec<(Vec<String>, String)> {
        let decode = |s: &str| s.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&#39;", "'").replace("&amp;", "&");
        html.split("<p>").skip(1).map(|p| {
            let p = &p[..p.find("</p>").unwrap()];
            let hrefs = p.split("href=\"").skip(1).map(|h| decode(&h[..h.find('"').unwrap()])).collect();
            let mut text = String::new();
            let mut in_tag = false;
            for c in p.chars() {
                match c {
                    '<' => in_tag = true,
                    '>' => in_tag = false,
                    _ if !in_tag => text.push(c),
                    _ => {}
                }
            }
            (hrefs, decode(&text))
        }).collect()
    }

    // The test page's credits: a paragraph a source, its URL and its licence's the hrefs of
    // its links, its text, licence and notices its text. Text is text, whatever it holds, and
    // a URL of another scheme than http(s) links nothing.
    #[test]
    fn the_test_pages_credits_link_each_source_and_its_licence_and_give_its_notices() {
        let credits = template_credits();
        let html = credits_html(&credits);
        let names: Vec<&str> = credits.iter().map(|e| e["text"].as_str().unwrap()).collect();
        assert!(html.starts_with(&format!("<details><summary>Data: {}</summary>", names.join(", ").replace('&', "&amp;"))), "{html}");
        let found = paragraphs(&html);
        assert_eq!(found.len(), credits.len());
        for (entry, (hrefs, text)) in credits.iter().zip(&found) {
            let mut links = vec![entry["url"].as_str().unwrap()];
            links.extend(entry["licence_url"].as_str());
            assert_eq!(hrefs, &links, "{}", entry["id"]);
            for field in ["text", "licence"] {
                assert!(text.contains(entry[field].as_str().unwrap()), "{} {field}: {text}", entry["id"]);
            }
            for notice in entry["notices"].as_array().unwrap() {
                assert!(text.contains(notice.as_str().unwrap()), "{}: {text}", entry["id"]);
            }
        }

        let hostile = serde_json::json!([{"id": "x", "text": "<b>X</b> & Co", "url": "javascript:alert(1)",
                                          "licence": "L", "licence_url": "https://l.example/?a=1&b=\"2\"",
                                          "notices": ["<script>alert(1)</script> 'q'"]}]);
        let html = credits_html(hostile.as_array().unwrap());
        assert!(!html.contains("<script>") && !html.contains("<b>") && !html.contains("javascript:"), "{html}");
        assert_eq!(paragraphs(&html), [(vec![r#"https://l.example/?a=1&b="2""#.to_owned()],
                                         "<b>X</b> & Co, L. <script>alert(1)</script> 'q'".to_owned())]);
        assert_eq!(credits_html(&[]), "");
    }

    // A dir holding the downloader's ledger but no stamp is one whose
    // download did not finish, and it may mix two builds' or selections'
    // files that load as one. Its slot stays empty, for the downloader to
    // finish it; the other continents load.
    #[test]
    fn a_continent_dir_holding_the_ledger_but_no_stamp_is_skipped() {
        let root = two_continents();
        std::fs::write(root.path().join("europe").join(FILE_LEDGER), "admin_cells.bin -\n").unwrap();

        assert_eq!(loaded_continents(root.path()), ["asia"]);
    }

    // The stamp goes in once all of a download is: a dir holding it loads,
    // the ledger beside it.
    #[test]
    fn a_continent_dir_holding_the_stamp_and_the_ledger_loads() {
        let root = two_continents();
        std::fs::write(root.path().join("europe").join(FILE_LEDGER), "admin_cells.bin -\n").unwrap();
        std::fs::write(root.path().join("europe").join(BUILD_STAMP), "2026-01-01").unwrap();

        assert_eq!(loaded_continents(root.path()), ["asia", "europe"]);
    }

    #[test]
    fn a_continent_dir_holding_neither_the_ledger_nor_a_stamp_loads() {
        let root = two_continents();

        assert_eq!(loaded_continents(root.path()), ["asia", "europe"]);
    }

    // A stamp or ledger that can't be checked leaves its continent
    // unloaded: here a link to itself (too many levels of links), which
    // stops root too, whom a dir's modes don't.
    #[cfg(unix)]
    #[test]
    fn a_continent_dir_whose_stamp_or_ledger_cant_be_checked_is_skipped() {
        for name in [BUILD_STAMP, FILE_LEDGER] {
            let root = two_continents();
            std::os::unix::fs::symlink(name, root.path().join("europe").join(name)).unwrap();

            assert_eq!(loaded_continents(root.path()), ["asia"], "{name}");
        }
    }

    /// The names of the continents MultiIndex::load loads under `root`, on a
    /// thread of its own: none while it is still loading after `wait`, else
    /// those it loaded; then those it loads once `meanwhile` has run.
    fn loaded_continents_meanwhile(root: &std::path::Path, wait: std::time::Duration, meanwhile: impl FnOnce()) -> (Option<Vec<&'static str>>, Option<Vec<&'static str>>) {
        let (done, loaded) = std::sync::mpsc::channel();
        let root = root.to_path_buf();
        let starting = std::thread::spawn(move || done.send(loaded_continents(&root)).unwrap());
        let early = loaded.recv_timeout(wait).ok();
        meanwhile();
        let late = loaded.recv().ok();
        starting.join().unwrap();
        (early, late)
    }

    // A download into a continent dir (another server's, on the same data
    // dir) holds the dir's download lock while it removes the stamp and
    // replaces the files for another selection. A server starting meanwhile
    // reads the dir only once the download let go of the lock, and finds
    // what it left: here, the download stopped, its stamp gone, and the
    // continent stays unloaded.
    #[cfg(unix)]
    #[test]
    fn a_server_starting_reads_a_continent_dir_only_once_a_download_into_it_let_go_of_its_lock() {
        let root = two_continents();
        let europe = root.path().join("europe");
        std::fs::write(europe.join(FILE_LEDGER), "admin_cells.bin -\n").unwrap();
        std::fs::write(europe.join(BUILD_STAMP), "2026-01-01").unwrap();
        let download = File::create(europe.join(DOWNLOAD_LOCK)).unwrap();
        download.lock().unwrap();

        let (early, late) = loaded_continents_meanwhile(root.path(), std::time::Duration::from_millis(500), || {
            std::fs::remove_file(europe.join(BUILD_STAMP)).unwrap();
            drop(download);
        });

        assert_eq!(early, None, "loaded while the download held the lock");
        assert_eq!(late, Some(vec!["asia"]));
    }

    // A server starting holds the lock until it has loaded the dir, and
    // lets go of it then: a try at it from another open fails while the dir
    // loads, and takes it after. A dir without the lock file (asia here)
    // gets it from the load.
    #[cfg(unix)]
    #[test]
    fn a_server_starting_holds_a_continent_dirs_download_lock_until_it_loaded_the_dir() {
        let root = two_continents();
        let europe = root.path().join("europe");
        std::fs::write(europe.join(FILE_LEDGER), "admin_cells.bin -\n").unwrap();
        std::fs::write(europe.join(BUILD_STAMP), "2026-01-01").unwrap();
        File::create(europe.join(DOWNLOAD_LOCK)).unwrap();
        let free = |dir: &std::path::Path| File::open(dir.join(DOWNLOAD_LOCK)).unwrap().try_lock().is_ok();
        let told = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let telling = told.clone();
        BEFORE_INDEX_LOAD.set(Some(Box::new(move |dir: &str| {
            let dir = std::path::Path::new(dir);
            telling.borrow_mut().push((dir.file_name().unwrap().to_str().unwrap().to_string(), dir.join(DOWNLOAD_LOCK).exists().then(|| free(dir))));
        })));

        let loaded = loaded_continents(root.path());

        BEFORE_INDEX_LOAD.set(None);
        assert_eq!(loaded, ["asia", "europe"]);
        assert_eq!(*told.borrow(), [("asia".to_string(), Some(false)), ("europe".to_string(), Some(false))]);
        assert!(free(&europe));
    }

    // A dir without the lock file (laid out by hand, or never downloaded
    // into by a server) gets it from a server starting, as from a download,
    // and the load holds a shared lock on it. The first download into the
    // dir (another server's, on the same data dir) that comes while the dir
    // loads waits for the load to end, and takes the lock after: else it
    // would make the file and take the lock at once, and replace the files
    // the load maps.
    #[cfg(unix)]
    #[test]
    fn a_first_download_into_a_continent_dir_without_the_lock_file_waits_for_a_server_loading_it() {
        let root = two_continents();
        let europe = root.path().join("europe");
        assert!(!europe.join(DOWNLOAD_LOCK).exists());
        let (taken, took) = std::sync::mpsc::channel();
        let (took, waiting) = (std::rc::Rc::new(took), std::rc::Rc::new(std::cell::RefCell::new(None)));
        let (taking, early) = (took.clone(), waiting.clone());
        // The download comes once the dir's stamp is checked, before it is
        // mapped, and runs on a thread of its own, as another process's would.
        BEFORE_INDEX_LOAD.set(Some(Box::new(move |dir: &str| {
            if !dir.ends_with("/europe") {
                return;
            }
            let (dir, taken) = (std::path::PathBuf::from(dir), taken.clone());
            let download = std::thread::spawn(move || taken.send(take_download_lock(&dir, LockFor::Download).unwrap().is_some()).unwrap());
            *early.borrow_mut() = Some((taking.recv_timeout(std::time::Duration::from_millis(500)).ok(), download));
        })));

        let loaded = loaded_continents(root.path());

        BEFORE_INDEX_LOAD.set(None);
        let (early, download) = waiting.borrow_mut().take().expect("europe loads");
        let late = took.recv_timeout(std::time::Duration::from_secs(10)).ok();
        download.join().unwrap();
        assert_eq!((early, late), (None, Some(true)), "the download took the lock while the dir loaded, or never");
        assert_eq!(loaded, ["asia", "europe"]);
        assert!(europe.join(DOWNLOAD_LOCK).exists() && root.path().join("asia").join(DOWNLOAD_LOCK).exists());
    }

    // A continent dir that isn't there loads nothing, and is no error: a
    // server starting passes it by without a word, and makes no lock file.
    #[test]
    fn a_continent_dir_that_isnt_there_loads_nothing_and_is_no_error() {
        let root = two_continents();
        let africa = root.path().join("africa");

        let got = load_continent(africa.to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE);

        assert_eq!(got.map(|idx| idx.is_some()), Ok(false));
        assert!(!africa.exists());
    }

    // A dir the lock file isn't in and can't be made in, as it is read-only
    // (a read-only mount) or not writable to this process (a dir of another
    // user), loads without a lock, as no download of this process can write
    // there. Any other error making it leaves the dir unloaded. The errors
    // are the seam's (LOCK_FILE_UNMADE), as root's writes aren't stopped by
    // a dir's modes; where this process isn't root, a dir of mode 0555 is
    // one it can't write too.
    #[cfg(unix)]
    #[test]
    fn a_continent_dir_the_lock_file_cant_be_made_in_loads_without_a_lock_only_where_it_cant_be_written() {
        use std::os::unix::fs::PermissionsExt;
        let mut got = Vec::new();
        for (errno, what) in [(libc::EROFS, "EROFS"), (libc::EACCES, "EACCES"), (libc::EPERM, "EPERM"), (libc::EIO, "EIO")] {
            let root = two_continents();
            LOCK_FILE_UNMADE.set(Some(errno));
            let loaded = loaded_continents(root.path());
            LOCK_FILE_UNMADE.set(None);
            got.push((what, loaded, root.path().join("europe").join(DOWNLOAD_LOCK).exists()));
        }
        let both = vec!["asia", "europe"];
        assert_eq!(got, [("EROFS", both.clone(), false), ("EACCES", both.clone(), false), ("EPERM", both.clone(), false), ("EIO", vec![], false)]);
        if unsafe { libc::geteuid() } != 0 {
            let root = two_continents();
            let europe = root.path().join("europe");
            std::fs::set_permissions(&europe, std::fs::Permissions::from_mode(0o555)).unwrap();
            let loaded = loaded_continents(root.path());
            std::fs::set_permissions(&europe, std::fs::Permissions::from_mode(0o755)).unwrap();
            assert_eq!((loaded, europe.join(DOWNLOAD_LOCK).exists()), (both, false));
        }
    }

    /// Whether load_continent loads europe of two_continents(), stamped with
    /// its ledger beside as a finished download leaves it (or holding neither,
    /// laid out by hand, where `by_hand`), or why not, when `meanwhile` runs
    /// on the dir once its stamp and ledger were checked, before Index::load:
    /// as a download into it of another server on the data dir would, that
    /// the load doesn't keep out. Where `lock_free` the dir's lock file isn't
    /// there and can't be made (EROFS, the seam's: LOCK_FILE_UNMADE), and the
    /// load takes no lock; else it holds the lock, and `meanwhile` is a
    /// download that doesn't see it.
    #[cfg(unix)]
    fn europe_loads_while(by_hand: bool, lock_free: bool, meanwhile: impl FnOnce(&std::path::Path) + 'static) -> Result<bool, String> {
        let root = two_continents();
        let europe = root.path().join("europe");
        if !by_hand {
            std::fs::write(europe.join(FILE_LEDGER), "admin_cells.bin -\n").unwrap();
            std::fs::write(europe.join(BUILD_STAMP), "2026-01-01").unwrap();
        }
        let mut meanwhile = Some(meanwhile);
        BEFORE_INDEX_LOAD.set(Some(Box::new(move |dir: &str| {
            if let Some(run) = meanwhile.take() {
                run(std::path::Path::new(dir));
            }
        })));
        LOCK_FILE_UNMADE.set(lock_free.then_some(libc::EROFS));
        let got = load_continent(europe.to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).map(|idx| idx.is_some());
        LOCK_FILE_UNMADE.set(None);
        BEFORE_INDEX_LOAD.set(None);
        assert_eq!(europe.join(DOWNLOAD_LOCK).exists(), !lock_free);
        got
    }

    /// Writes the files of another selection into `dir` as a download puts
    /// them in place: each a new file renamed over the one before.
    #[cfg(unix)]
    fn another_selection(dir: &std::path::Path) {
        let mut moved = TestIndex::new();
        moved.place("Newtown", 2, -20_000.0, 0.0);
        moved.write(dir);
    }

    /// Why load_continent leaves a dir unloaded that a download changed while
    /// it loaded.
    #[cfg(unix)]
    fn changed_while_it_loaded() -> String {
        format!("a download into it ran while it loaded ({BUILD_STAMP} or {FILE_LEDGER} changed): it may mix two selections' files")
    }

    // A server that can't make a continent dir's lock file (a read-only
    // dir, or one not writable to it) loads the dir without a lock, and a
    // download into it of another server on the data dir, which can write
    // there, may make the lock file, remove the stamp and replace the files
    // while it maps them. Such a download makes the ledger where the dir has
    // none, and removes the stamp, before it changes a file: a load that
    // finds either changed once Index::load returned drops what it loaded,
    // and the continent stays unloaded, for a later load. One that finds
    // both as they were keeps it.
    #[cfg(unix)]
    #[test]
    fn a_continent_dir_a_download_changed_while_it_loaded_without_a_lock_is_left_unloaded() {
        let got = [
            ("nothing changed", europe_loads_while(false, true, |_| {})),
            ("nothing changed, laid out by hand", europe_loads_while(true, true, |_| {})),
            ("the stamp removed, the files replaced", europe_loads_while(false, true, |dir| {
                std::fs::remove_file(dir.join(BUILD_STAMP)).unwrap();
                another_selection(dir);
            })),
            ("the ledger made, laid out by hand", europe_loads_while(true, true, |dir| {
                std::fs::write(dir.join(FILE_LEDGER), "").unwrap();
                another_selection(dir);
            })),
        ];

        let changed = changed_while_it_loaded();
        assert_eq!(got, [
            ("nothing changed", Ok(true)),
            ("nothing changed, laid out by hand", Ok(true)),
            ("the stamp removed, the files replaced", Err(changed.clone())),
            ("the ledger made, laid out by hand", Err(changed)),
        ]);
    }

    // A server starting tells the stamp it found from one a download wrote
    // since by more than its being there: a download that ran all through
    // while the dir loaded removed it and wrote it again, by rename, a new
    // inode, though of the same bytes. Here of the same modification time
    // too.
    #[cfg(unix)]
    #[test]
    fn a_continent_dir_whose_stamp_was_written_again_while_it_loaded_is_left_unloaded() {
        use std::os::unix::fs::MetadataExt;
        let got = europe_loads_while(false, true, |dir| {
            let stamp = dir.join(BUILD_STAMP);
            let was = std::fs::metadata(&stamp).unwrap();
            // Made while the old one is there: another inode.
            let tmp = dir.join(format!("{BUILD_STAMP}.tmp"));
            std::fs::write(&tmp, std::fs::read(&stamp).unwrap()).unwrap();
            std::fs::remove_file(&stamp).unwrap();
            another_selection(dir);
            File::options().write(true).open(&tmp).unwrap().set_modified(was.modified().unwrap()).unwrap();
            std::fs::rename(&tmp, &stamp).unwrap();
            let now = std::fs::metadata(&stamp).unwrap();
            assert!(now.ino() != was.ino() && (now.dev(), now.len(), now.modified().unwrap()) == (was.dev(), was.len(), was.modified().unwrap()));
        });

        assert_eq!(got, Err(changed_while_it_loaded()));
    }

    // A load that holds the lock checks the stamp and ledger again too: a
    // download that doesn't see its lock (on a file system whose locks don't
    // reach other machines, or one without locks, where none is taken) may
    // run all the same.
    #[cfg(unix)]
    #[test]
    fn a_continent_dir_a_download_changed_while_it_loaded_under_its_lock_is_left_unloaded() {
        let got = europe_loads_while(false, false, |dir| {
            std::fs::remove_file(dir.join(BUILD_STAMP)).unwrap();
            another_selection(dir);
        });

        assert_eq!(got, Err(changed_while_it_loaded()));
    }

    // Servers starting on one data dir load a continent dir at once: the
    // lock a load holds is shared.
    #[cfg(unix)]
    #[test]
    fn servers_starting_load_a_continent_dir_at_once() {
        let root = two_continents();
        let europe = root.path().join("europe");
        std::fs::write(europe.join(FILE_LEDGER), "admin_cells.bin -\n").unwrap();
        std::fs::write(europe.join(BUILD_STAMP), "2026-01-01").unwrap();
        let other = File::create(europe.join(DOWNLOAD_LOCK)).unwrap();
        other.lock_shared().unwrap();

        let (early, _) = loaded_continents_meanwhile(root.path(), std::time::Duration::from_secs(10), || drop(other));

        assert_eq!(early, Some(vec!["asia", "europe"]));
    }

    // A load makes the lock file where it isn't there, of mode 0644 under
    // umask 022, and a server of another user can't open it to write,
    // though the dir is writable to both (here through its group). Its
    // download opens the file to read and takes the exclusive lock on that,
    // which a local file system allows: a try at the lock from another open
    // fails while the download holds it, and takes it after; the file stays
    // as it was. Where the file system takes an exclusive lock only on a
    // file open to write (NFS), the download fails, saying the lock file is
    // to be writable to every server that downloads into the dir: that
    // error is the seam's (DOWNLOAD_LOCK_FAULT). Run as root, the dir and
    // the lock file are another user's and the lock is taken on a thread
    // whose file system uid is nobody's and gid the dir's group, which
    // takes root's power over modes away; else the lock file is this
    // user's, of mode 0444.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_download_takes_its_lock_on_a_lock_file_it_cant_write() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        const OTHER: u32 = 65533;
        const NOBODY: u32 = 65534;
        const NOGROUP: u32 = 65534;
        let dir = ScratchDir::new();
        let lock = dir.path().join(DOWNLOAD_LOCK);
        File::create(&lock).unwrap();
        let as_root = unsafe { libc::geteuid() } == 0;
        if as_root {
            for (path, mode) in [(&lock, 0o644), (&dir.path().to_path_buf(), 0o775)] {
                std::os::unix::fs::chown(path, Some(OTHER), Some(NOGROUP)).unwrap();
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
            }
        } else {
            std::fs::set_permissions(&lock, std::fs::Permissions::from_mode(0o444)).unwrap();
        }
        let was = std::fs::metadata(&lock).unwrap();
        let free = || File::open(&lock).unwrap().try_lock_shared().is_ok();

        let (can, took, free_while, nfs) = std::thread::scope(|s| {
            s.spawn(|| {
                if as_root {
                    // Each returns the id it had: the second call tells the first took.
                    unsafe { libc::syscall(libc::SYS_setfsgid, NOGROUP) };
                    assert_eq!(unsafe { libc::syscall(libc::SYS_setfsgid, NOGROUP) }, NOGROUP as libc::c_long);
                    unsafe { libc::syscall(libc::SYS_setfsuid, NOBODY) };
                    assert_eq!(unsafe { libc::syscall(libc::SYS_setfsuid, NOBODY) }, NOBODY as libc::c_long);
                }
                // It makes files in the dir, and can't write the lock file.
                let can = (File::create(dir.path().join("made")).is_ok(), File::options().write(true).open(&lock).is_ok());
                let taken = take_download_lock(dir.path(), LockFor::Download);
                let free_while = free();
                let took = taken.map(|file| file.is_some()).map_err(|e| e.to_string());
                DOWNLOAD_LOCK_FAULT.set(Some(libc::EBADF));
                let nfs = take_download_lock(dir.path(), LockFor::Download).map(|file| file.is_some()).map_err(|e| e.to_string());
                DOWNLOAD_LOCK_FAULT.set(None);
                (can, took, free_while, nfs)
            })
            .join()
            .unwrap()
        });

        assert_eq!((can, took, free_while), ((true, false), Ok(true), false));
        assert!(free());
        let now = std::fs::metadata(&lock).unwrap();
        assert_eq!((now.ino(), now.uid(), now.mode(), now.len()), (was.ino(), was.uid(), was.mode(), 0));
        assert_eq!(nfs, Err("the lock file isn't writable to this server, and this file system takes an exclusive lock only on a file open to write (Bad file descriptor (os error 9)): make it writable to every server that downloads into the dir".to_string()));
    }

    /// A MultiIndex of two_continents() whose europe isn't loaded, as a
    /// server's before a download into europe is in, and the dir, stamped as
    /// a download that finished leaves it, the empty lock file beside.
    fn europe_to_hot_add() -> (ScratchDir, MultiIndex, std::path::PathBuf) {
        let root = two_continents();
        let europe = root.path().join("europe");
        let mi = MultiIndex::load(root.path().to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).unwrap();
        mi.continents.iter().find(|c| c.name == "europe").unwrap().index.store(None);
        std::fs::write(europe.join(FILE_LEDGER), "admin_cells.bin -\n").unwrap();
        std::fs::write(europe.join(BUILD_STAMP), "2026-01-01").unwrap();
        File::create(europe.join(DOWNLOAD_LOCK)).unwrap();
        (root, mi, europe)
    }

    // A hot-add (install_region, once a server's download is in) loads the
    // dir as a server starting does. A download into it of another server on
    // the data dir may have taken its lock as this server's let go of it, and
    // be replacing its files for another selection: the hot-add reads the dir
    // only once that download let go of the lock, and loads it only where it
    // finished. Here it stopped, its stamp gone, and the hot-add fails, the
    // slot left empty; or it finished, and the hot-add loads the dir.
    #[cfg(unix)]
    #[test]
    fn a_hot_add_reads_a_continent_dir_only_once_a_download_into_it_let_go_of_its_lock() {
        let mut got = Vec::new();
        for stopped in [true, false] {
            let (_root, mi, europe) = europe_to_hot_add();
            let download = File::open(europe.join(DOWNLOAD_LOCK)).unwrap();
            download.lock().unwrap();

            let (early, late) = std::thread::scope(|s| {
                let (done, added) = std::sync::mpsc::channel();
                let mi = &mi;
                s.spawn(move || done.send(mi.install_region("europe").map(drop)).unwrap());
                let early = added.recv_timeout(std::time::Duration::from_millis(500)).ok();
                if stopped {
                    std::fs::remove_file(europe.join(BUILD_STAMP)).unwrap();
                }
                drop(download);
                (early, added.recv().ok())
            });

            got.push((stopped, early, late, mi.loaded_count()));
        }
        let unfinished = format!("it holds {FILE_LEDGER} but no {BUILD_STAMP}: a download that did not finish");
        assert_eq!(got, [(true, None, Some(Err(unfinished)), 1), (false, None, Some(Ok(())), 2)]);
    }

    // A hot-add holds the lock until it has loaded the dir, as a server
    // starting does: a try at it from another open fails while the dir loads,
    // and takes it after.
    #[cfg(unix)]
    #[test]
    fn a_hot_add_holds_a_continent_dirs_download_lock_until_it_loaded_the_dir() {
        let (_root, mi, europe) = europe_to_hot_add();
        let free = |dir: &std::path::Path| File::open(dir.join(DOWNLOAD_LOCK)).unwrap().try_lock().is_ok();
        let told = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let telling = told.clone();
        BEFORE_INDEX_LOAD.set(Some(Box::new(move |dir: &str| telling.borrow_mut().push(free(std::path::Path::new(dir))))));

        let added = mi.install_region("europe").map(drop);

        BEFORE_INDEX_LOAD.set(None);
        assert_eq!((added, mi.loaded_count()), (Ok(()), 2));
        assert_eq!(*told.borrow(), [false]);
        assert!(free(&europe));
    }

    #[test]
    fn query_json_finds_nothing_off_the_globe() {
        let world = MultiIndex {
            continents: vec![ContinentSlot {
                name: "world",
                continent: None,
                index: arc_swap::ArcSwapOption::from(Some(Arc::new(TestIndex::new().load()))),
                loaded_stamp: None,
            }],
            root_dir: String::new(),
            street_cell_level: DEFAULT_STREET_CELL_LEVEL,
            admin_cell_level: DEFAULT_ADMIN_CELL_LEVEL,
            search_distance: DEFAULT_SEARCH_DISTANCE,
        };
        let (lat, lng) = ORIGIN;
        assert!(world.query_json(lat, lng).contains("Testland"));
        assert!(world.query_json(lat, lng).starts_with(r#"{"licence":"Data © OpenStreetMap contributors, ODbL 1.0. http://osm.org/copyright","#));
        // The first two are the origin's own point once S2 wraps them onto the globe.
        for (lat, lng) in [(lat, lng + 360.0), (180.0 - lat, lng - 180.0), (f64::NAN, lng), (lat, f64::INFINITY), (90.5, lng), (lat, -180.5)] {
            assert_eq!(world.query_json(lat, lng), "{}", "({}, {})", lat, lng);
        }
    }

    /// What a lookup that finds nothing answers: Nominatim's licence, and no address.
    const NOTHING_FOUND: &str = r#"{"licence":"Data © OpenStreetMap contributors, ODbL 1.0. http://osm.org/copyright","address":{}}"#;

    #[test]
    fn every_answer_carries_nominatims_licence_at_its_head() {
        // Nominatim's json and jsonv2 write "licence" first in every result but for its
        // place_id, which these answers have none of (v1/format_json.py format_base_json),
        // worded as its v1/classtypes.py OSM_ATTRIBUTION. An answer of nothing found carries
        // it too.
        let index = TestIndex::new().street("Main Street", &parallel(0.0, 200.0)).load();
        let (lat, lng) = query_point(1.0, 0.0);
        let answer = serde_json::to_string(&index.query(lat, lng)).unwrap();
        assert!(answer.starts_with(r#"{"licence":"Data © OpenStreetMap contributors, ODbL 1.0. http://osm.org/copyright","display_name":"Main Street"#), "{answer}");
        // 60 km north: outside Testland.
        let (lat, lng) = query_point(60_000.0, 0.0);
        assert_eq!(serde_json::to_string(&index.query(lat, lng)).unwrap(), NOTHING_FOUND);
    }

    #[test]
    fn query_finds_nothing_off_the_globe_though_its_wrapped_cells_list_a_street_with_a_parent() {
        // S2 wraps a point off the globe onto cells of the globe, and where those cells list a
        // street the query's admin walks that street's parent chain, however far the street is:
        // Main Street's chain names Testland.
        // Main Street (way 0) in Testland (polygon 0), the top of its chain.
        let mut index = TestIndex::new().street("Main Street", &[(0.0, -500.0), (0.0, 500.0)]).way_parent(0).load();
        index.admin_parents = Some(FileBytes::Owned(NO_DATA.to_le_bytes().to_vec()));
        let on = address_at(&index, 10.0, 0.0);
        assert_eq!((on["road"].as_str(), on["country"].as_str()), (Some("Main Street"), Some("Testland")));
        let p = at(10.0, 0.0);
        let (lat, lng) = (p.lat as f64, p.lng as f64);
        let cell = |lat: f64, lng: f64| cell_id_at_level(lat, lng, DEFAULT_STREET_CELL_LEVEL);
        // Each is p once S2 wraps it onto the globe.
        for (wlat, wlng) in [(lat, lng + 360.0), (lat, lng - 360.0), (180.0 - lat, lng - 180.0), (-180.0 - lat, lng + 180.0)] {
            assert_eq!(cell(wlat, wlng), cell(lat, lng), "({wlat}, {wlng})");
            assert_eq!(serde_json::to_string(&index.query(wlat, wlng)).unwrap(), NOTHING_FOUND, "({wlat}, {wlng})");
        }
        for (wlat, wlng) in [(f64::NAN, lng), (lat, f64::NAN), (lat, f64::INFINITY), (f64::NEG_INFINITY, lng)] {
            assert_eq!(serde_json::to_string(&index.query(wlat, wlng)).unwrap(), NOTHING_FOUND, "({wlat}, {wlng})");
        }
    }

    #[test]
    fn polygons_list_a_postcode_area_once_as_postal_at_level_99() {
        let index = TestIndex::new()
            .boundary("1010", POSTCODE_AREA_LEVEL, 0, (-15_000.0, -15_000.0, 15_000.0, 15_000.0))
            .boundary("Town", 8, 0, (-2000.0, -2000.0, 2000.0, 2000.0))
            .load();
        // Testland is polygon 0, the postcode area the next.
        let postcode_area = 1;
        let p = at(0.0, 0.0);
        let (lat, lng) = (p.lat as f64, p.lng as f64);
        let cell = cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL);
        let cells_listing_it = std::iter::once(cell)
            .chain(cell_neighbors_at_level(cell, DEFAULT_ADMIN_CELL_LEVEL))
            .filter(|&c| {
                let mut listed = false;
                index.admin_index.for_each(c, |id| {
                    listed |= (id & ID_MASK) == postcode_area
                });
                listed
            })
            .count();
        assert!(cells_listing_it > 1, "{cells_listing_it}");

        let features = index.polygon_features(lat, lng);

        let mut listed: Vec<(&str, &str, u64)> = features
            .iter()
            .map(|f| {
                let p = &f["properties"];
                (p["name"].as_str().unwrap(), p["kind"].as_str().unwrap(), p["admin_level"].as_u64().unwrap())
            })
            .collect();
        listed.sort();
        assert_eq!(listed, [("1010", "postal", 99), ("Testland", "admin", 2), ("Town", "admin", 8)]);
        for f in &features {
            let rings = f["geometry"]["coordinates"].as_array().unwrap();
            assert_eq!((rings.len(), rings[0].as_array().unwrap().len()), (1, 5), "{}", f["properties"]["name"]);
        }
    }

    #[test]
    fn query_gates_place_nodes_by_the_promoted_ranks_of_boundaries() {
        // A level 6 boundary linked to a suburb ranks 20, so its level 7
        // child ranks 22: the country, not the child, gates the village node
        // east of the child.
        let index = TestIndex::new()
            .boundary("Suburbia", 6, 4, (-5000.0, -5000.0, 5000.0, 5000.0))
            .boundary("Inner", 7, 0, (-1000.0, -1000.0, 1000.0, 1000.0))
            .place("Outer Village", 2, 0.0, 1500.0)
            .load();
        let address = address_at(&index, 0.0, 0.0);
        assert_eq!(address["suburb"], "Suburbia");
        assert_eq!(address["village"], "Outer Village");
    }

    #[test]
    fn query_takes_the_nearest_place_node_that_passes_its_boundary() {
        // Both neighbourhoods' squares cover the point; the nearer lies
        // outside the town, so the farther one inside it names the point.
        let index = TestIndex::new()
            .boundary("Town", 8, 0, (-2000.0, -2000.0, 2000.0, 300.0))
            .place("Across", 5, 0.0, 350.0)
            .place("Within", 5, 0.0, -450.0)
            .load();
        let address = address_at(&index, 0.0, 0.0);
        assert_eq!(address["city"], "Town");
        assert_eq!(address["neighbourhood"], "Within");
    }

    #[test]
    fn debug_primary_tells_an_exact_tie_from_a_nearer_street() {
        // Two ways of one name from one junction, the point on the junction:
        // both at distance 0 with one tie key, so the first listed wins,
        // whichever way that is. A third street 30 m off is no tie.
        let junction = |first_north: bool| {
            let (north, east) = (&[(0.0, 0.0), (200.0, 0.0)], &[(0.0, 0.0), (0.0, 200.0)]);
            let (a, b): (&[(f64, f64)], &[(f64, f64)]) = if first_north { (north, east) } else { (east, north) };
            TestIndex::new().street("Fork Road", a).street("Fork Road", b).street("Far Road", &[(30.0, -200.0), (30.0, -100.0)]).load()
        };
        let (one, other) = (junction(true), junction(false));
        let (lat, lng) = (at(0.0, 0.0).lat as f64, at(0.0, 0.0).lng as f64);
        let (a, b) = (one.debug_primary(lat, lng), other.debug_primary(lat, lng));
        for street in [&a["street"], &b["street"]] {
            assert_eq!(street["name"], "Fork Road");
            assert_eq!(street["id"], 0);
            assert_eq!(street["dist_bits"], format!("{:016x}", 0f64.to_bits()));
        }
        // The winners are different ways (north in one, east in the other),
        // which only their order told apart.
        assert_eq!(a["street"]["tie"], b["street"]["tie"]);
        assert_eq!(a["street"]["tie"][0], "Fork Road");
        assert_eq!(a["street"]["tie"][1], format!("{:08x}", at(0.0, 0.0).lat.to_bits()));

        // On the far street, 30 m north of the east way's line: it wins
        // alone, with a key of its own.
        let far = one.debug_primary(at(30.0, -150.0).lat as f64, at(30.0, -150.0).lng as f64);
        assert_eq!(far["street"]["name"], "Far Road");
        assert_eq!(far["street"]["id"], 2);
        assert_ne!(far["street"]["tie"], a["street"]["tie"]);
    }

    #[test]
    fn equally_near_interpolation_lines_go_by_content_whatever_their_record_order() {
        type Line<'a> = (&'a str, (u32, u32), &'a [(f64, f64)]);
        let (north, east): (&[(f64, f64)], &[(f64, f64)]) = (&[(0.0, 0.0), (200.0, 0.0)], &[(0.0, 0.0), (0.0, 200.0)]);
        // Through the corner, which is its middle node, half way along.
        let through: &[(f64, f64)] = &[(-200.0, 0.0), (0.0, 0.0), (0.0, 200.0)];
        let (lat, lng) = (at(0.0, 0.0).lat as f64, at(0.0, 0.0).lng as f64);
        // Two lines that touch the corner, both at distance 0 from it, as
        // records 0 and 1 or the other way round.
        let pair = |a: Line, b: Line, swap: bool| {
            let (first, second) = if swap { (b, a) } else { (a, b) };
            TestIndex::new()
                .street("Far Road", &[(900.0, 900.0), (900.0, 1000.0)])
                .interpolation(first.0, (first.1.0, first.1.1, 0), first.2)
                .interpolation(second.0, (second.1.0, second.1.1, 0), second.2)
                .load()
        };
        for swap in [false, true] {
            let second = if swap { 0 } else { 1 };
            // Two streets: the one that sorts first as text.
            let index = pair(("Pine Hill Road", (1, 99), north), ("Clemmons Street", (2, 98), east), swap);
            let (dist, street, number, id) = index.query_geo(lat, lng).1.unwrap();
            assert_eq!((dist, &*street, number, id), (0.0, "Clemmons Street", 2, second));
            let primary = index.debug_primary(lat, lng);
            assert_eq!(primary["interp"]["dist_bits"], format!("{:016x}", 0f64.to_bits()));
            assert_eq!(primary["interp"]["tie"][0], "Clemmons Street");
            assert_eq!(primary["interp"]["tie"][1], 2);

            // One street: the lower start, then the lower end.
            let index = pair(("Fork Road", (3, 9), north), ("Fork Road", (1, 9), east), swap);
            assert_eq!(index.query_geo(lat, lng).1.unwrap().3, second);
            let index = pair(("Fork Road", (1, 9), north), ("Fork Road", (1, 5), east), swap);
            assert_eq!(index.query_geo(lat, lng).1.unwrap().3, second);
            assert_eq!(index.find_interp_on_way(lat, lng, "Fork Road", f64::MAX), Some(1));

            // One street and range: the line whose nodes sort first, which
            // starts south of the corner and is half way to 9 there.
            let index = pair(("Fork Road", (1, 9), north), ("Fork Road", (1, 9), through), swap);
            assert_eq!(index.query_geo(lat, lng).1.unwrap().2, 5);
            assert_eq!(index.find_interp_on_way(lat, lng, "Fork Road", f64::MAX), Some(5));
        }
    }

    #[test]
    fn query_matches_place_nodes_against_the_address_street() {
        // The hamlet's 1000 m square crosses the street 3 km north of the
        // point but leaves the point out.
        let index = TestIndex::new()
            .street("Long Road", &[(-1000.0, 0.0), (5000.0, 0.0)])
            .place("Justice", 4, 3000.0, 200.0)
            .load();
        let address = address_at(&index, 0.0, 20.0);
        assert_eq!(address["road"], "Long Road");
        assert_eq!(address["hamlet"], "Justice");
    }

    #[test]
    fn query_finds_place_nodes_along_the_whole_street() {
        // The hamlet's square crosses the street 38 km north of the point,
        // cells away from the point's own: it names the street wherever along
        // it the query falls.
        let index = TestIndex::new()
            .street("Long Road", &[(-1000.0, 0.0), (40_000.0, 0.0)])
            .place("Far End", 4, 38_000.0, 200.0)
            .load();
        assert_eq!(address_at(&index, 37_500.0, 20.0)["hamlet"], "Far End");
        assert_eq!(address_at(&index, 0.0, 20.0)["hamlet"], "Far End");
    }

    #[test]
    fn query_finds_a_city_covering_the_point_from_cells_away() {
        // The city's 15 km square covers the streetless point, but its node,
        // 14.5 km north, sits two cells up, past the point's cell and that
        // cell's neighbours: it names the point all the same.
        let (point, city) = (at(8000.0, 0.0), at(22_500.0, 0.0));
        let cell = cell_id_at_level(point.lat as f64, point.lng as f64, DEFAULT_ADMIN_CELL_LEVEL);
        let city_cell = cell_id_at_level(city.lat as f64, city.lng as f64, DEFAULT_ADMIN_CELL_LEVEL);
        assert!(city_cell != cell && !cell_neighbors_at_level(cell, DEFAULT_ADMIN_CELL_LEVEL).contains(&city_cell));
        let index = TestIndex::new().place("Metropolis", 0, 22_500.0, 0.0).load();
        assert_eq!(address_at(&index, 8000.0, 0.0)["city"], "Metropolis");
    }

    /// At 60°N: a hamlet 0.0075° east is 417 m away, one 0.005° north 557 m.
    const HIGH_LATITUDE: (f64, f64) = (60.0, 10.0);
    const EAST_M: f64 = 0.0075 * METRES_PER_DEGREE * 0.5;
    const NORTH_M: f64 = 0.005 * METRES_PER_DEGREE;

    #[test]
    fn query_orders_place_nodes_in_plain_degrees() {
        // The eastern hamlet is nearer in metres, and both squares cover the
        // point; Nominatim orders them by ST_Distance on lon/lat geometry, in
        // plain degrees, so the northern one names the point.
        let index = TestIndex::around(HIGH_LATITUDE)
            .place("East", 4, 0.0, EAST_M)
            .place("North", 4, NORTH_M, 0.0)
            .load();
        assert_eq!(address_from(&index, HIGH_LATITUDE, 0.0, 0.0)["hamlet"], "North");
    }

    #[test]
    fn query_orders_place_nodes_along_the_street_in_plain_degrees() {
        // The same measured from a street ending at (0, 0): the northern
        // hamlet is 0.005° from it, the eastern one 0.0075°.
        let index = TestIndex::around(HIGH_LATITUDE)
            .street("West Road", &[(0.0, -300.0), (0.0, 0.0)])
            .place("East", 4, 0.0, EAST_M)
            .place("North", 4, NORTH_M, -150.0)
            .load();
        let address = address_from(&index, HIGH_LATITUDE, 0.0, -150.0);
        assert_eq!(address["road"], "West Road");
        assert_eq!(address["hamlet"], "North");
    }

    #[test]
    fn current_boundaries_reuse_what_find_admin_tested() {
        // The boundaries at the point are the same over find_admin's scan
        // as over a fresh one.
        let index = TestIndex::new()
            .boundary("Suburbia", 6, 4, (-5000.0, -5000.0, 5000.0, 5000.0))
            .boundary("Inner", 7, 0, (-1000.0, -1000.0, 1000.0, 1000.0))
            .boundary("Quarter", 15, 6, (-3000.0, -3000.0, 500.0, 500.0))
            .load();
        let names = |found: CurrentBoundaries| found.gates.map(|g| g.map(|p| index.get_string(p.name_id)));
        for (north, east) in [(0.0, 0.0), (2000.0, 0.0), (0.0, 4000.0), (-2000.0, -2000.0)] {
            let p = at(north, east);
            let mut scan = AdminScan::new(&index, p.lat as f64, p.lng as f64);
            let admin = index.find_admin_in(&mut scan);
            let shared = index.find_current_boundaries(&mut scan, admin.country_code);
            let fresh = index.find_current_boundaries(&mut AdminScan::new(&index, p.lat as f64, p.lng as f64), admin.country_code);
            assert_eq!(names(shared), names(fresh), "({north}, {east})");
        }
    }

    #[test]
    fn query_leaves_the_ranks_its_boundaries_hold_to_them() {
        // The district, linked to a suburb, ranks 20 and promotes the hamlet
        // inside it to 22, the rank its level 7 ward holds: the ward takes the
        // rank and the hamlet names nothing (Zhongpo in Taipei).
        let index = TestIndex::new()
            .boundary("Daan", 6, 4, (-3000.0, -3000.0, 3000.0, 3000.0))
            .boundary("Ward", 7, 0, (-1500.0, -1500.0, 1500.0, 1500.0))
            .place("Zhongpo", 4, 0.0, 400.0)
            .load();
        let address = address_at(&index, 0.0, 0.0);
        assert_eq!(address["suburb"], "Daan");
        assert_eq!(address["neighbourhood"], "Ward");
        assert_eq!(address.get("hamlet"), None);
    }

    #[test]
    fn the_nearest_address_is_as_far_as_a_nodes_point_or_a_buildings_outline() {
        // House 1 is a node. House 3 is a building 30 m square, its point in the middle.
        let index = TestIndex::new()
            .street("Main Street", &parallel(-40.0, 200.0))
            .address("1", 20.0, -30.0)
            .building("3", (10.0, 20.0, 40.0, 50.0))
            .load();
        let nearest = |north_m, east_m| primary_at(&index, north_m, east_m)["addr"].clone();
        let record = |addr: &serde_json::Value| {
            (addr["house_number"].as_str().unwrap().to_owned(), addr["id"].as_u64().unwrap(), addr["vertex_count"].as_u64().unwrap())
        };

        // The node is 30 m west and the building's point 35 m off, but its west wall is 20 m east.
        let addr = nearest(20.0, 0.0);
        assert_eq!(record(&addr), ("3".to_owned(), 1, 5));
        assert!(about(&addr["dist_m"], 20.0), "{addr}");
        let address = address_at(&index, 20.0, 0.0);
        assert_eq!((address["house_number"].as_str(), address["road"].as_str()), (Some("3"), Some("Main Street")));

        // 8 m further west the node is 22 m off and the wall 28 m.
        let addr = nearest(20.0, -8.0);
        assert_eq!(record(&addr), ("1".to_owned(), 0, 0));
        assert!(about(&addr["dist_m"], 22.0), "{addr}");
        assert_eq!(address_at(&index, 20.0, -8.0)["house_number"], "1");

        // Inside the building its distance is 0, wherever its point is.
        let addr = nearest(15.0, 45.0);
        assert_eq!(record(&addr), ("3".to_owned(), 1, 5));
        assert_eq!(addr["dist_bits"], format!("{:016x}", 0f64.to_bits()));
    }

    #[test]
    fn an_exact_address_tie_goes_to_the_smaller_key_whatever_the_record_order() {
        let house_number_at = |index: &Index, north_m, east_m| {
            primary_at(index, north_m, east_m)["addr"]["house_number"].as_str().unwrap().to_owned()
        };

        // Two flats on one node: the house number decides, as text.
        for index in in_both_orders(|dataset, swapped| {
            let (first, second) = if swapped { ("9", "10") } else { ("10", "9") };
            dataset.street("Main Street", &parallel(-40.0, 200.0)).address(first, 20.0, 0.0).address(second, 20.0, 0.0);
        }) {
            assert_eq!(house_number_at(&index, 15.0, 5.0), "10");
        }

        // One house number on one node for two streets: the street decides.
        for index in in_both_orders(|dataset, swapped| {
            let (first, second) = if swapped { ("Mill Lane", "Abbey Road") } else { ("Abbey Road", "Mill Lane") };
            dataset.street(first, &parallel(-40.0, 200.0)).address("7", 20.0, 0.0);
            dataset.street(second, &parallel(-80.0, 200.0)).address("7", 20.0, 0.0);
        }) {
            assert_eq!(primary_at(&index, 15.0, 5.0)["addr"]["street"], "Abbey Road");
        }

        // A point inside two buildings is at distance 0 from both: the nearer point decides,
        // before the house number.
        for index in in_both_orders(|dataset, swapped| {
            let (near, far): ((&str, Span), (&str, Span)) = (("8", (0.0, 0.0, 20.0, 20.0)), ("2", (10.0, 10.0, 40.0, 40.0)));
            let (first, second) = if swapped { (far, near) } else { (near, far) };
            dataset.street("Main Street", &parallel(-40.0, 200.0)).building(first.0, first.1).building(second.0, second.1);
        }) {
            let addr = primary_at(&index, 12.0, 12.0)["addr"].clone();
            assert_eq!(addr["dist_bits"], format!("{:016x}", 0f64.to_bits()));
            assert_eq!(addr["house_number"], "8");
        }

        // With one key the first record read stays.
        let twins = TestIndex::new().street("Main Street", &parallel(-40.0, 200.0)).address("7", 20.0, 0.0).address("7", 20.0, 0.0).load();
        assert_eq!(primary_at(&twins, 15.0, 5.0)["addr"]["id"], 0);
    }

    #[test]
    fn a_street_that_wins_takes_the_house_number_of_its_own_nearest_address() {
        // From 2 m north of Main Street: house 5 is the nearest address, 14 m off, but hangs
        // from Side Street. Of Main Street's own, the node of house 9 is 39 m off and the
        // building of house 11 has its point at 46 m and a corner at 34 m.
        let index = TestIndex::new()
            .street("Main Street", &parallel(0.0, 200.0))
            .address("9", 40.0, 10.0)
            .building("11", (30.0, -40.0, 45.0, -20.0))
            .street("Side Street", &parallel(60.0, 200.0))
            .address("5", 15.0, 5.0)
            .load();
        assert_eq!(primary_at(&index, 2.0, 0.0)["addr"]["house_number"], "5");

        let address = address_at(&index, 2.0, 0.0);
        assert_eq!((address["road"].as_str(), address["house_number"].as_str()), (Some("Main Street"), Some("11")));

        assert_eq!(house_number_on_way(&index, 2.0, 0.0, 0, 100.0).as_deref(), Some("11"));
        assert_eq!(house_number_on_way(&index, 2.0, 0.0, 1, 100.0).as_deref(), Some("5"));
        // Past the limit, and a way no address hangs from.
        assert_eq!(house_number_on_way(&index, 2.0, 0.0, 0, 30.0), None);
        assert_eq!(house_number_on_way(&index, 2.0, 0.0, 2, 100.0), None);

        // Two flats of the street on one node: the house number decides, not the record order.
        for index in in_both_orders(|dataset, swapped| {
            let (first, second) = if swapped { ("2B", "2A") } else { ("2A", "2B") };
            dataset.street("Main Street", &parallel(0.0, 200.0)).address(first, 30.0, 0.0).address(second, 30.0, 0.0);
        }) {
            assert_eq!(house_number_on_way(&index, 2.0, 0.0, 0, 100.0).as_deref(), Some("2A"));
            assert_eq!(address_at(&index, 2.0, 0.0)["house_number"], "2A");
        }
    }

    /// How far east of the origin the interpolation tests ask from: 0.3 of the way along a line
    /// of `parallel(_, 100.0)`. There 2 to 100 gives 31 and its even numbers 32, 1 to 99 odd
    /// gives 31, 2 to 200 even 62.
    const ALONG_EAST_M: f64 = -40.0;

    #[test]
    fn find_interp_on_way_takes_the_nearest_line_of_the_street_by_name() {
        let index = TestIndex::new()
            .street("Main Street", &parallel(0.0, 200.0))
            .interpolation("Oak Avenue", (500, 600, 0), &parallel(3.0, 100.0))
            .interpolation("Main St", (2, 100, 1), &parallel(8.0, 100.0))
            .interpolation("Main St", (1001, 1099, 2), &parallel(-20.0, 100.0))
            .load();
        let number = |street, max_m| number_on_street(&index, 3.0, ALONG_EAST_M, street, max_m);

        // The line at the point itself is Oak Avenue's: query_geo's nearest, but not Main Street's.
        let interp = primary_at(&index, 3.0, ALONG_EAST_M)["interp"].clone();
        assert_eq!((interp["id"].as_u64(), interp["street"].as_str(), interp["number"].as_u64()), (Some(0), Some("Oak Avenue"), Some(530)));
        assert!(about(&interp["dist_m"], 0.0), "{interp}");

        assert_eq!(number("Main Street", 100.0), Some(32));
        assert_eq!(number("Oak Avenue", 100.0), Some(530));
        assert_eq!(number("Elm Road", 100.0), None);
        // Main Street's nearest line is 5 m off.
        assert_eq!(number("Main Street", 4.0), None);
    }

    #[test]
    fn equally_near_interpolations_go_to_the_lower_start_then_the_lower_interpolation() {
        let pair = |first: (u32, u32, u8), second: (u32, u32, u8)| {
            in_both_orders(move |dataset, swapped| {
                let (a, b) = if swapped { (second, first) } else { (first, second) };
                dataset.street("Main Street", &parallel(0.0, 200.0));
                dataset.interpolation("Main St", a, &parallel(5.0, 100.0)).interpolation("Main St", b, &parallel(5.0, 100.0));
            })
        };
        let number = |index: &Index| number_on_street(index, 1.0, ALONG_EAST_M, "Main Street", 100.0);
        let nearest = |index: &Index| {
            let interp = primary_at(index, 1.0, ALONG_EAST_M)["interp"].clone();
            (interp["id"].as_u64().unwrap(), interp["number"].as_u64().unwrap())
        };

        // The odd and the even side of one centreline: the lower start number, in either order.
        let [odd_first, even_first] = pair((1, 99, 2), (2, 100, 1));
        assert_eq!((number(&odd_first), number(&even_first)), (Some(31), Some(31)));
        // query_geo's nearest goes by the street, then the range: the odd line, whichever record it is.
        assert_eq!((nearest(&odd_first), nearest(&even_first)), ((0, 31), (1, 31)));

        // One start number: the lower interpolation.
        let [all_first, even_first] = pair((2, 100, 0), (2, 100, 1));
        assert_eq!((number(&all_first), number(&even_first)), (Some(31), Some(31)));

        // One start number and interpolation: the lower end, in either order.
        let [short_first, long_first] = pair((2, 100, 1), (2, 200, 1));
        assert_eq!((number(&short_first), number(&long_first)), (Some(32), Some(32)));
    }

    #[test]
    fn a_us_street_without_an_address_point_takes_its_house_number_from_an_interpolation() {
        let with_line = |mut dataset: TestIndex| {
            dataset.street("Main Street", &parallel(0.0, 200.0)).interpolation("Main St", (2, 100, 1), &parallel(8.0, 100.0));
            dataset.load()
        };

        let address = address_at(&with_line(TestIndex::in_country(b"US")), 1.0, ALONG_EAST_M);
        assert_eq!((address["road"].as_str(), address["house_number"].as_str()), (Some("Main Street"), Some("32")));

        let address = address_at(&with_line(TestIndex::new()), 1.0, ALONG_EAST_M);
        assert_eq!((address["road"].as_str(), address.get("house_number")), (Some("Main Street"), None));
    }

    #[test]
    fn an_interpolation_with_a_zero_start_or_end_is_skipped() {
        let zeros = |dataset: &mut TestIndex| {
            dataset.street("Main Street", &parallel(0.0, 200.0))
                .interpolation("Main St", (10, 0, 0), &parallel(2.0, 100.0))
                .interpolation("Main St", (0, 10, 0), &parallel(3.0, 100.0));
        };

        // Both lie nearer than the line from 100 to 200.
        let mut dataset = TestIndex::new();
        zeros(&mut dataset);
        let index = dataset.interpolation("Main St", (100, 200, 0), &parallel(20.0, 100.0)).load();
        let interp = primary_at(&index, 1.0, ALONG_EAST_M)["interp"].clone();
        assert_eq!((interp["id"].as_u64(), interp["number"].as_u64()), (Some(2), Some(130)));
        assert!(about(&interp["dist_m"], 19.0), "{interp}");
        assert_eq!(number_on_street(&index, 1.0, ALONG_EAST_M, "Main Street", 100.0), Some(130));

        // Alone they are no interpolation at all.
        let mut dataset = TestIndex::new();
        zeros(&mut dataset);
        let index = dataset.load();
        assert!(primary_at(&index, 1.0, ALONG_EAST_M)["interp"].is_null());
        assert_eq!(number_on_street(&index, 1.0, ALONG_EAST_M, "Main Street", 100.0), None);
    }

    #[test]
    fn query_reads_the_postcode_of_the_address_that_won_then_of_its_street() {
        let index = TestIndex::new()
            .street("Mill Lane", &parallel(-1000.0, 200.0)).way_postcode("2000")
            .street("Main Street", &parallel(0.0, 200.0)).way_postcode("2020")
            .address("5", 30.0, -40.0)
            .address("7", 30.0, 0.0).addr_postcode("1010")
            .address("9", 60.0, 80.0)
            .street("Side Street", &parallel(70.0, 200.0)).way_postcode("2121")
            .load();
        let answer = |north_m, east_m| {
            let address = address_at(&index, north_m, east_m);
            let field = |name| address.get(name).map(|v| v.as_str().unwrap().to_owned());
            (field("road"), field("house_number"), field("postcode"))
        };
        let some = |s: &str| Some(s.to_owned());

        // On house 7: its own postcode.
        assert_eq!(answer(29.0, 0.0), (some("Main Street"), some("7"), some("1010")));
        // On house 5, which has none: its street's.
        assert_eq!(answer(29.0, -40.0), (some("Main Street"), some("5"), some("2020")));
        // House 9 has none either: its street's, though Side Street runs nearer it.
        assert_eq!(answer(59.0, 80.0), (some("Main Street"), some("9"), some("2020")));
        // On the street: house 7's, which gives the house number, as Nominatim answers with the
        // house (reverse.py lookup_street_poi); on house 5's side, the street's.
        assert_eq!(answer(1.0, 0.0), (some("Main Street"), some("7"), some("1010")));
        assert_eq!(answer(1.0, -40.0), (some("Main Street"), some("5"), some("2020")));
        assert_eq!(answer(-999.0, 0.0), (some("Mill Lane"), None, some("2000")));
    }

    #[test]
    fn query_reads_an_interpolations_postcode_for_a_street_without_one() {
        let index = TestIndex::new()
            .street("Main Street", &parallel(0.0, 200.0)).way_postcode("2020")
            .interpolation("Main St", (1, 99, 0), &parallel(5.0, 100.0)).interp_postcode("3030")
            .street("High Street", &parallel(1000.0, 200.0))
            .interpolation("High St", (1, 99, 0), &parallel(1005.0, 100.0)).interp_postcode("3131")
            .street("Oak Avenue", &parallel(2000.0, 200.0))
            .interpolation("Elm Road", (1, 99, 0), &parallel(2005.0, 100.0)).interp_postcode("3232")
            .interpolation("Oak Ave", (1, 99, 0), &parallel(2030.0, 100.0)).interp_postcode("3434")
            .street("Park Lane", &parallel(3015.0, 200.0))
            .interpolation("Park Ln", (1, 99, 0), &parallel(3121.0, 100.0)).interp_postcode("3333")
            .load();
        let postcode = |north_m| address_at(&index, north_m, 0.0).get("postcode").map(|v| v.as_str().unwrap().to_owned());

        // The street's own postcode comes first.
        assert_eq!(postcode(1.0).as_deref(), Some("2020"));
        assert_eq!(postcode(1001.0).as_deref(), Some("3131"));
        // The nearest line is another street's. The tier asks no further, though a line of Oak
        // Avenue's own lies 29 m off.
        assert_eq!(primary_at(&index, 2001.0, 0.0)["interp"]["id"], 2);
        assert_eq!(number_on_street(&index, 2001.0, 0.0, "Oak Avenue", 100.0), Some(50));
        assert_eq!(postcode(2001.0), None);
        // A line of the street's name more than 100 m from the point, which sits at the south
        // edge of its street cell: the cell north of it lists the line.
        let interp = primary_at(&index, 3016.0, 0.0)["interp"].clone();
        assert_eq!(interp["id"], 4);
        assert!(about(&interp["dist_m"], 105.0), "{interp}");
        assert_eq!(postcode(3016.0), None);
    }

    #[test]
    fn query_walks_the_parent_chain_of_the_nearest_street() {
        // Neither Farland nor Otherland holds a street, but way_parents.bin names them.
        let mut dataset = TestIndex::new();
        dataset
            .boundary("Farland", 2, 0, (60_000.0, 60_000.0, 70_000.0, 70_000.0))
            .boundary("Otherland", 2, 0, (-70_000.0, -70_000.0, -60_000.0, -60_000.0))
            .street("Main Street", &parallel(0.0, 200.0)).way_parent(2)
            .street("High Street", &parallel(1000.0, 200.0)).way_parent(1)
            .street("Mill Lane", &parallel(2000.0, 200.0));
        let mut index = dataset.load();
        // No polygon has a parent: a chain is the street's parent alone.
        index.admin_parents = Some(FileBytes::Owned([NO_DATA; 3].iter().flat_map(|parent| parent.to_le_bytes()).collect()));

        assert_eq!(address_at(&index, 10.0, 0.0)["country"], "Otherland");
        assert_eq!(address_at(&index, 1010.0, 0.0)["country"], "Farland");
        // Mill Lane has no parent: the boundaries at the point answer.
        assert_eq!(address_at(&index, 2010.0, 0.0)["country"], "Testland");
    }

    #[test]
    fn query_takes_a_streets_state_at_its_point_nearest_the_query_where_its_chain_and_the_query_disagree() {
        // Border Road runs in Northshire, 10 m from Southshire, and its parent is Farshire.
        let mut dataset = TestIndex::new();
        dataset
            .boundary("Northshire", 4, 0, (-10.0, -5000.0, 5000.0, 5000.0))
            .boundary("Southshire", 4, 0, (-5000.0, -5000.0, -10.0, 5000.0))
            .boundary("Farshire", 4, 0, (60_000.0, 60_000.0, 70_000.0, 70_000.0))
            .street("Border Road", &parallel(0.0, 200.0)).way_parent(3);
        let mut index = dataset.load();
        // Farshire's parent is Testland, the top of the chain.
        index.admin_parents = Some(FileBytes::Owned([NO_DATA, NO_DATA, NO_DATA, 0].iter().flat_map(|parent| parent.to_le_bytes()).collect()));
        let answer = |north_m| {
            let address = address_at(&index, north_m, ALONG_EAST_M);
            (address["road"].as_str().map(str::to_owned), address["state"].as_str().map(str::to_owned))
        };
        let some = |s: &str| Some(s.to_owned());

        // From Southshire, 20 m off the road: the road's nearest point lies in Northshire.
        assert_eq!(answer(-20.0), (some("Border Road"), some("Northshire")));
        // From Northshire, which the chain does not name either.
        assert_eq!(answer(5.0), (some("Border Road"), some("Northshire")));
    }

    /// Two streets, two addresses and two interpolation ways, the first of each with a value in
    /// each of its side files.
    fn two_of_each() -> TestIndex {
        let mut dataset = TestIndex::new();
        dataset
            .street("Main Street", &parallel(0.0, 200.0)).way_parent(0).way_postcode("2020")
            .address("7", 30.0, 0.0).addr_postcode("1010").addr_city("Springfield")
            .interpolation("Main St", (1, 99, 0), &parallel(5.0, 100.0)).interp_postcode("3030")
            .street("High Street", &parallel(1000.0, 200.0))
            .address("9", 1030.0, 0.0)
            .interpolation("High St", (1, 99, 0), &parallel(1005.0, 100.0));
        dataset
    }

    /// Each side file and the file whose records its values are for.
    const SIDE_FILES: [(&str, &str); 5] = [
        ("way_parents.bin", "street_ways.bin"), ("way_postcodes.bin", "street_ways.bin"),
        ("addr_postcodes.bin", "addr_points.bin"), ("addr_cities.bin", "addr_points.bin"),
        ("interp_postcodes.bin", "interp_ways.bin"),
    ];

    /// What Index::load refuses the files of `dataset` with once `edit` has changed them in
    /// their dir; none where it loads them.
    fn load_refusal(dataset: &TestIndex, edit: impl FnOnce(&std::path::Path)) -> Option<String> {
        let dir = ScratchDir::new();
        dataset.write(dir.path());
        edit(dir.path());
        Index::load(dir.path().to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).err()
    }

    #[test]
    fn load_refuses_a_side_file_of_another_count_than_its_parent() {
        let dataset = two_of_each();
        assert_eq!(load_refusal(&dataset, |_| ()), None);
        for (name, parent) in SIDE_FILES {
            let holding = |values: &[u32]| {
                load_refusal(&dataset, |dir| std::fs::write(dir.join(name), side_blocks::encode(values)).unwrap())
            };
            assert_eq!(holding(&[0, 0]), None, "{name}");
            let others: [&[u32]; 3] = [&[0], &[0, 0, 0], &[]];
            for values in others {
                let said = holding(values).unwrap_or_default();
                let want = format!(": {name} holds {} values for the 2 records of {parent}", values.len());
                assert!(said.ends_with(&want), "{name}: {said}");
            }
        }
    }

    #[test]
    fn load_refuses_a_side_file_whose_parent_is_absent() {
        let dataset = two_of_each();
        // The street files come with the geo index. Without them the other side file of the
        // streets goes too, so that the one left is the one refused.
        let streets = ["geo_cells.bin", "geo_entries.bin", "street_ways.bin", "street_nodes.bin"];
        let addresses = ["addr_points.bin", "addr_vertices.bin", "addr_index.bin"];
        let cases: [(&str, &str, Vec<&str>); 5] = [
            ("way_parents.bin", "street_ways.bin", [&streets[..], &["way_postcodes.bin"][..]].concat()),
            ("way_postcodes.bin", "street_ways.bin", [&streets[..], &["way_parents.bin"][..]].concat()),
            ("addr_postcodes.bin", "addr_points.bin", addresses.to_vec()),
            ("addr_cities.bin", "addr_points.bin", [&addresses[..], &["addr_postcodes.bin"][..]].concat()),
            ("interp_postcodes.bin", "interp_ways.bin", vec!["interp_ways.bin", "interp_nodes.bin"]),
        ];
        for (name, parent, without) in cases {
            let said = load_refusal(&dataset, |dir| {
                for file in &without {
                    std::fs::remove_file(dir.join(file)).unwrap();
                }
            });
            let said = said.unwrap_or_default();
            assert!(said.ends_with(&format!(": {name} without {parent}")), "{name}: {said}");
        }
    }

    #[test]
    fn load_refuses_a_side_file_that_is_not_side_blocks() {
        let dataset = two_of_each();
        for (name, _) in SIDE_FILES {
            // A u32 a record, and no footer.
            let said = load_refusal(&dataset, |dir| std::fs::write(dir.join(name), [0u8; 8]).unwrap()).unwrap_or_default();
            assert!(said.ends_with(&format!(": {name} has no SDB1 footer")), "{name}: {said}");
        }
    }

    /// The two files of each kind of way.
    const WAY_FILES: [(&str, &str); 2] = [("street_ways.bin", "street_nodes.bin"), ("interp_ways.bin", "interp_nodes.bin")];

    #[test]
    fn load_refuses_way_files_whose_footers_count_different_ways() {
        let dataset = two_of_each();
        for (ways, nodes) in WAY_FILES {
            // A nodes file ends in its way count and its magic.
            let said = load_refusal(&dataset, |dir| {
                let mut bytes = std::fs::read(dir.join(nodes)).unwrap();
                let count = bytes.len() - 8;
                assert_eq!(bytes[count..count + 4], 2u32.to_le_bytes());
                bytes[count..count + 4].copy_from_slice(&3u32.to_le_bytes());
                std::fs::write(dir.join(nodes), bytes).unwrap();
            });
            let said = said.unwrap_or_default();
            assert!(said.ends_with(&format!(": {ways} and {nodes} count different ways")), "{nodes}: {said}");
        }
    }

    #[test]
    fn load_refuses_a_way_file_without_its_footer_and_rows_that_are_not_its_ways() {
        let dataset = two_of_each();
        let refusal = |file: &str, edit: &dyn Fn(Vec<u8>) -> Vec<u8>| {
            let said = load_refusal(&dataset, |dir| std::fs::write(dir.join(file), edit(std::fs::read(dir.join(file)).unwrap())).unwrap());
            let said = said.unwrap_or_default();
            // The dir, then what the files' reader says.
            said.split_once(": ").map(|(_, said)| said.to_owned()).unwrap_or(said)
        };
        // Two fixed-width records, a file cut short, none of its bytes.
        assert_eq!(refusal("street_ways.bin", &|_| vec![0; 24]), "street_ways.bin has no SWY1 footer");
        assert_eq!(refusal("street_nodes.bin", &|mut bytes| { bytes.pop(); bytes }), "street_nodes.bin has no SWN1 footer");
        assert_eq!(refusal("interp_ways.bin", &|_| Vec::new()), "interp_ways.bin has no IWY1 footer");
        assert_eq!(refusal("interp_nodes.bin", &|bytes| bytes[..8].to_vec()), "interp_nodes.bin has no IWN1 footer");
        // The footer after a byte more or fewer than the ways' rows.
        assert_eq!(refusal("street_ways.bin", &|mut bytes| { bytes.insert(0, 0); bytes }), "street_ways.bin is not the rows of its 2 ways");
        assert_eq!(refusal("interp_ways.bin", &|bytes| bytes[1..].to_vec()), "interp_ways.bin is not the rows of its 2 ways");
    }

    #[test]
    fn load_refuses_one_file_of_a_way_pair_without_the_other() {
        let dataset = two_of_each();
        let without = |files: &[&str]| {
            let said = load_refusal(&dataset, |dir| {
                for file in files {
                    std::fs::remove_file(dir.join(file)).unwrap();
                }
            });
            said.unwrap_or_default()
        };
        // The street files come with the geo index, the interpolation files with each other.
        assert_eq!(without(&["street_ways.bin"]), "Partial geo index: street_ways.bin is missing");
        assert_eq!(without(&["street_nodes.bin"]), "Partial geo index: street_nodes.bin is missing");
        assert_eq!(without(&["interp_ways.bin"]), "Partial index group [interp_ways.bin, interp_nodes.bin]: missing [interp_ways.bin]");
        assert_eq!(without(&["interp_nodes.bin"]), "Partial index group [interp_ways.bin, interp_nodes.bin]: missing [interp_nodes.bin]");
        // A dir without a geo index needs no street file, and loads neither without the other.
        let no_geo = ["geo_cells.bin", "geo_entries.bin", "way_parents.bin", "way_postcodes.bin"];
        for (kept, gone) in [("street_ways.bin", "street_nodes.bin"), ("street_nodes.bin", "street_ways.bin")] {
            let said = without(&[&no_geo[..], &[gone][..]].concat());
            assert!(said.ends_with(&format!(": {kept} without {gone}")), "{kept}: {said}");
        }
        assert_eq!(without(&[&no_geo[..], &["street_ways.bin", "street_nodes.bin"][..]].concat()), "");
    }

    #[test]
    fn load_refuses_part_of_the_address_files() {
        let dataset = two_of_each();
        let (points, vertices, index) = (addr_files::POINTS_FILE, addr_files::VERTICES_FILE, addr_files::INDEX_FILE);
        // Without the addresses' postcodes and cities too, which no dir holds without
        // addr_points.bin.
        let without = |files: &[&str]| {
            let said = load_refusal(&dataset, |dir| {
                for file in files.iter().chain(&["addr_postcodes.bin", "addr_cities.bin"]) {
                    std::fs::remove_file(dir.join(file)).unwrap();
                }
            });
            said.unwrap_or_default()
        };
        // The dir, then the files it holds and the ones it lacks.
        let cases: [(&[&str], &str); 6] = [
            (&[points], "addr_vertices.bin and addr_index.bin without addr_points.bin"),
            (&[vertices], "addr_points.bin and addr_index.bin without addr_vertices.bin"),
            (&[index], "addr_points.bin and addr_vertices.bin without addr_index.bin"),
            (&[points, vertices], "addr_index.bin without addr_points.bin and addr_vertices.bin"),
            (&[points, index], "addr_vertices.bin without addr_points.bin and addr_index.bin"),
            (&[vertices, index], "addr_points.bin without addr_vertices.bin and addr_index.bin"),
        ];
        for (files, want) in cases {
            let said = without(files);
            assert!(said.ends_with(&format!(": {want}")), "{files:?}: {said}");
        }
        // A dir with none of the three has no addresses, and loads.
        assert_eq!(without(&[points, vertices, index]), "");
    }

    #[test]
    fn load_refuses_address_files_no_builder_writes() {
        let dataset = two_of_each();
        // What load says of the dir with `file` edited: the dir, then what the files' reader says.
        let refused = |file: &str, edit: &dyn Fn(Vec<u8>) -> Vec<u8>, want: &str| {
            let said = load_refusal(&dataset, |dir| std::fs::write(dir.join(file), edit(std::fs::read(dir.join(file)).unwrap())).unwrap());
            let said = said.unwrap_or_default();
            assert!(said.ends_with(&format!(": {want}")), "{file}: {said}");
        };
        // The count a footer holds before its magic, set to 3: the records of addr_points.bin
        // and of addr_index.bin, which are 2, and the blobs of addr_vertices.bin, of which it
        // has none.
        let counting_three = |mut bytes: Vec<u8>| {
            let count = bytes.len() - 8;
            bytes[count..count + 4].copy_from_slice(&3u32.to_le_bytes());
            bytes
        };
        // Two fixed-width records, a file cut short, none of its bytes.
        refused("addr_points.bin", &|_| vec![0; 56], "addr_points.bin has no ADB1 footer");
        refused("addr_vertices.bin", &|mut bytes| { bytes.pop(); bytes }, "addr_vertices.bin has no ADF1 footer");
        refused("addr_index.bin", &|_| Vec::new(), "addr_index.bin has no ADX1 footer");
        // Footers that count different records, an index a byte longer than its records' rows,
        // and more blobs than records.
        refused("addr_points.bin", &counting_three, "addr_points.bin and addr_index.bin count different records");
        refused("addr_index.bin", &counting_three, "addr_points.bin and addr_index.bin count different records");
        refused("addr_index.bin", &|mut bytes| { bytes.insert(0, 0); bytes }, "addr_index.bin is not the rows of its 2 records");
        refused("addr_vertices.bin", &counting_three, "addr_vertices.bin counts more blobs than addr_points.bin has records");
    }

    #[test]
    fn address_files_of_no_records_load_and_give_no_address() {
        // What a full dir holds for a region without addresses: the three files' footers.
        let mut dataset = TestIndex::new();
        dataset.street("Main Street", &parallel(0.0, 200.0));
        let dir = ScratchDir::new();
        dataset.write(dir.path());
        let none = addr_files::encode(&[], &[]);
        assert_eq!((none.points.len(), none.vertices.len(), none.index.len()), (8, 16, 8));
        std::fs::write(dir.path().join(addr_files::POINTS_FILE), none.points).unwrap();
        std::fs::write(dir.path().join(addr_files::VERTICES_FILE), none.vertices).unwrap();
        std::fs::write(dir.path().join(addr_files::INDEX_FILE), none.index).unwrap();

        let index = load_dir(dir.path());

        assert_eq!(index.addrs.as_ref().map(AddrFiles::count), Some(0));
        let primary = primary_at(&index, 1.0, ALONG_EAST_M);
        assert_eq!(primary["street"]["name"], "Main Street");
        assert!(primary["addr"].is_null(), "{primary}");
        assert_eq!(house_number_on_way(&index, 1.0, ALONG_EAST_M, 0, 100.0), None);
        let address = address_at(&index, 1.0, ALONG_EAST_M);
        assert_eq!((address["road"].as_str(), address.get("house_number")), (Some("Main Street"), None));
    }

    #[test]
    fn an_address_a_cell_lists_past_the_last_record_is_skipped() {
        // No builder lists an id its files do not hold. Where a cell does, one just past the
        // last record and one blocks past it, the scans read the records there are.
        let index = TestIndex::new()
            .street("Main Street", &parallel(0.0, 200.0))
            .address("7", 30.0, 0.0)
            .listed(geo_index::ADDRESSES, 1, 30.0, 0.0)
            .listed(geo_index::ADDRESSES, 4000, 30.0, 0.0)
            .load();

        let addr = primary_at(&index, 29.0, 0.0)["addr"].clone();
        assert_eq!((addr["id"].as_u64(), addr["house_number"].as_str()), (Some(0), Some("7")));
        assert_eq!(house_number_on_way(&index, 29.0, 0.0, 0, 100.0).as_deref(), Some("7"));
        assert_eq!(address_at(&index, 29.0, 0.0)["house_number"], "7");
    }

    #[test]
    fn a_building_whose_blob_does_not_read_is_as_far_as_its_point() {
        // House 3 is a building 30 m square. Its blob starts with a tag no blob has.
        let mut dataset = TestIndex::new();
        dataset.street("Main Street", &parallel(-40.0, 200.0)).building("3", (10.0, 20.0, 40.0, 50.0));
        let dir = ScratchDir::new();
        dataset.write(dir.path());
        let blobs = dir.path().join(addr_files::VERTICES_FILE);
        let mut bytes = std::fs::read(&blobs).unwrap();
        bytes[0] = 0x40;
        std::fs::write(&blobs, bytes).unwrap();
        let read = |index: &Index, north_m, east_m| {
            let addr = primary_at(index, north_m, east_m)["addr"].clone();
            (addr["house_number"].as_str().map(str::to_owned), addr["vertex_count"].as_u64(), addr["dist_m"].clone())
        };

        // Inside its outline, 12 m west of its point, for both scans.
        let damaged = load_dir(dir.path());
        let (house_number, vertex_count, dist_m) = read(&damaged, 25.0, 23.0);
        assert_eq!((house_number.as_deref(), vertex_count), (Some("3"), Some(0)));
        assert!(about(&dist_m, 12.0), "{dist_m}");
        assert_eq!(house_number_on_way(&damaged, 25.0, 23.0, 0, 100.0).as_deref(), Some("3"));
        assert_eq!(house_number_on_way(&damaged, 25.0, 23.0, 0, 10.0), None);

        // With the blob as the builder writes it, the point is in the building.
        let whole = dataset.load();
        let (house_number, vertex_count, dist_m) = read(&whole, 25.0, 23.0);
        assert_eq!((house_number.as_deref(), vertex_count), (Some("3"), Some(5)));
        assert!(about(&dist_m, 0.0), "{dist_m}");
        assert_eq!(house_number_on_way(&whole, 25.0, 23.0, 0, 10.0).as_deref(), Some("3"));
    }

    /// `index` with one polygon POI, the rectangle: `name_id` its name and `parent_street_id`
    /// its parent street's, strings of the index's. The admin cell of its middle lists it.
    fn with_polygon_poi(index: Index, name_id: u32, parent_street_id: u32, span: Span) -> Index {
        with_poi(index, (1, 0), name_id, (parent_street_id, NO_DATA), span, true)
    }

    /// `index` with one POI of `category` and `flags`: the rectangle, or a point at its middle
    /// where `polygon` is false. `name_id` its name and `parents` its parent street's and
    /// postcode's, strings of the index's. The admin cell of its middle lists it.
    fn with_poi(index: Index, kind: (u8, u8), name_id: u32, parents: (u32, u32), span: Span, polygon: bool) -> Index {
        with_pois(index, &[(kind, name_id, parents, span, polygon)])
    }

    /// `index` with `pois`, each as with_poi takes one, record i the i-th.
    fn with_pois(index: Index, pois: &[((u8, u8), u32, (u32, u32), Span, bool)]) -> Index {
        with_pois_of_tier(index, pois, 0)
    }

    /// with_pois, every record of `tier`.
    fn with_pois_of_tier(mut index: Index, pois: &[((u8, u8), u32, (u32, u32), Span, bool)], tier: u8) -> Index {
        let mut records = Vec::new();
        let mut cells = Vec::new();
        for (i, &((category, flags), name_id, (parent_street_id, parent_postcode_id), (s, w, n, e), polygon)) in pois.iter().enumerate() {
            let (low, middle) = (at(s, w), at((s + n) / 2.0, (w + e) / 2.0));
            // Its corners at 1e-7 degrees (encoding 3) from its low corner.
            let steps = |v: f32, from: f32| ((v as f64 - from as f64) / 1e-7).round() as u32;
            let outline = polygon.then(|| poi_files::PoiPolygon {
                encoding: 3, bmin_lat: low.lat, bmin_lng: low.lng,
                q: [at(s, w), at(s, e), at(n, e), at(n, w)].iter().map(|c| (steps(c.lat, low.lat), steps(c.lng, low.lng))).collect(),
            });
            let record = PoiRecord {
                lat: middle.lat, lng: middle.lng, vertex_offset: if polygon { 0 } else { NO_DATA }, name_id, category, tier, flags,
                importance: 0, parent_street_id, parent_postcode_id, parent_poly_id: NO_DATA,
            };
            records.push((record, outline));
            cells.push((cell_id_at_level(middle.lat as f64, middle.lng as f64, DEFAULT_ADMIN_CELL_LEVEL), i as u32));
        }
        let (records, vertices) = poi_files::encode(&records);
        index.poi_index = Some(cell_pairs::from_pairs(cells, cell_pairs::POI));
        index.poi_records = Some(PoiRecords::open(FileBytes::Owned(records)).unwrap());
        index.poi_vertices = Some(FileBytes::Owned(vertices));
        index
    }

    #[test]
    fn a_poi_around_the_query_gives_way_to_an_address_node_inside_it_and_not_to_a_building() {
        // The town hall's outline, 40 m by 60 m, holds the query and, 28 m from it, the point of
        // house 1: a node, or the middle of a building 10 m square. Main Street runs 70 m south
        // of the query; the town hall's parent street is Mill Lane.
        let answer = |building: bool| {
            let mut dataset = TestIndex::new();
            dataset.street("Main Street", &parallel(-40.0, 200.0));
            if building {
                dataset.building("1", (5.0, 25.0, 15.0, 35.0));
            } else {
                dataset.address("1", 10.0, 30.0);
            }
            let (hall, lane) = (dataset.string("Town Hall"), dataset.string("Mill Lane"));
            let index = with_polygon_poi(dataset.load(), hall, lane, (0.0, 0.0, 40.0, 60.0));
            assert_eq!(primary_at(&index, 30.0, 10.0)["addr"]["vertex_count"], if building { 5 } else { 0 });
            let address = address_at(&index, 30.0, 10.0);
            (address["house_number"].as_str().map(str::to_owned), address["road"].as_str().map(str::to_owned))
        };

        // The node answers, on its street.
        assert_eq!(answer(false), (Some("1".to_owned()), Some("Main Street".to_owned())));
        // The building's nearest wall is 21 m from the query: the town hall answers, on its
        // parent street.
        assert_eq!(answer(true), (None, Some("Mill Lane".to_owned())));
    }

    #[test]
    fn a_water_poi_never_takes_the_primary_slot_nor_gives_its_road_or_postcode() {
        // A POI 40 m by 60 m holds the query, its parent street Mill Lane and its postcode 4510;
        // Main Street, of postcode 2020, runs 70 m south of the query. Nominatim's POI lane
        // takes rank 30 alone (reverse.py _find_closest_street_or_pois), and it ranks
        // natural=water, wetland, bay and glacier 22, and landuse=reservoir|basin on an area 24
        // (address-levels.json): the street answers there, with its postcode. A landuse node
        // ranks 30 (ranking.sql compute_place_rank), and so do a swimming area, a fire pond
        // and a fountain: the POI answers, on its parent street with its postcode.
        let answer = |category: u8, polygon: bool| {
            let mut dataset = TestIndex::new();
            dataset.street("Main Street", &parallel(-40.0, 200.0)).way_postcode("2020");
            let (name, lane, postcode) = (dataset.string("Water"), dataset.string("Mill Lane"), dataset.string("4510"));
            let index = with_poi(dataset.load(), (category, 0), name, (lane, postcode), (0.0, 0.0, 40.0, 60.0), polygon);
            let primary = primary_at(&index, 30.0, 10.0);
            let address = address_at(&index, 30.0, 10.0);
            (primary["poi"]["category"].as_u64(), address["road"].as_str().map(str::to_owned), address["postcode"].as_str().map(str::to_owned))
        };
        let street = (None, Some("Main Street".to_owned()), Some("2020".to_owned()));
        let poi = |category: u8| (Some(category as u64), Some("Mill Lane".to_owned()), Some("4510".to_owned()));

        // NATURAL_WATER, WETLAND, BAY, GLACIER and RESERVOIR, areas.
        for category in [99, 97, 91, 86, 148] {
            assert_eq!(answer(category, true), street, "category {category}");
        }
        // NATURAL_WATER, WETLAND, BAY and GLACIER nodes rank 22 too.
        for category in [99, 97, 91, 86] {
            assert_eq!(answer(category, false), street, "category {category}, a point");
        }
        // A RESERVOIR node, SWIMMING_AREA, FIRE_WATER_POND and FOUNTAIN.
        assert_eq!(answer(148, false), poi(148));
        for category in [161, 162, 49] {
            assert_eq!(answer(category, true), poi(category), "category {category}");
        }
    }

    #[test]
    fn a_water_area_of_the_highway_flag_answers_its_own_name_as_a_street_does() {
        // A water area a highway kind of rank 26 or 27 ranks (area=yes, highway=pedestrian,
        // natural=water, tourism=attraction: the builder's NATURAL_WATER of the highway flag)
        // is a street in Nominatim, whose street lane takes address ranks 26 to 29 (reverse.py
        // _find_closest_street_or_pois): inside it, its own name answers, though Main Street,
        // 5 m south of the query, is nearer than its outline, 10 m west, and its parent street
        // is Mill Lane. Of no flag, the water stays out of both lanes and Main Street answers;
        // a node of the flag stays out too.
        let answer = |flags: u8, polygon: bool| {
            let mut dataset = TestIndex::new();
            dataset.street("Main Street", &parallel(25.0, 200.0));
            let (name, lane) = (dataset.string("Seeplatz"), dataset.string("Mill Lane"));
            let index = with_poi(dataset.load(), (99, flags), name, (lane, NO_DATA), (0.0, 0.0, 40.0, 60.0), polygon);
            address_at(&index, 30.0, 10.0)["road"].as_str().map(str::to_owned)
        };
        assert_eq!(answer(POI_FLAG_HIGHWAY, true).as_deref(), Some("Seeplatz"));
        assert_eq!(answer(0, true).as_deref(), Some("Main Street"));
        assert_eq!(answer(POI_FLAG_HIGHWAY, false).as_deref(), Some("Main Street"));
        for category in [86, 91, 97, 148] {
            let record = PoiRecord {
                lat: 0.0, lng: 0.0, vertex_offset: 0, name_id: 0, category, tier: 0, flags: POI_FLAG_HIGHWAY,
                importance: 0, parent_street_id: 0, parent_postcode_id: NO_DATA, parent_poly_id: NO_DATA,
            };
            assert!(!Index::is_out_of_the_primary_lanes(&record), "category {category}");
            assert!(Index::is_out_of_the_primary_lanes(&PoiRecord { flags: 0, ..record }), "category {category}");
        }
    }

    #[test]
    fn a_water_street_answers_its_own_name_wherever_it_wins_and_needs_no_parent_street() {
        // A water street, a water area of the highway flag, is one placex row of class highway
        // in Nominatim, its polygon the geometry and rank_address 26 or 27 (init.lua
        // process_tags, ranking.sql compute_place_rank): the street lane finds it by its
        // distance to the polygon, 0 inside, and answers its own name (reverse.py
        // lookup_street_poi), and a street has no parent street (placex_update parents
        // rank_search > 27 alone). Main Street runs 10 m south of a point 3 m west of the
        // outline, 40 m by 60 m, and of one inside it 10 m from the outline: the water street
        // wins at both, and answers its own name, not that of its parent street, Mill Lane, nor
        // Main Street where it links none. A polygon of the flag that is no water keeps its
        // parent street outside it, and takes no slot without one.
        let answer = |category: u8, linked: bool, (north, east): (f64, f64)| {
            let mut dataset = TestIndex::new();
            dataset.street("Main Street", &parallel(20.0, 200.0));
            let (name, lane) = (dataset.string("Seeplatz"), dataset.string("Mill Lane"));
            let parents = (if linked { lane } else { NO_DATA }, NO_DATA);
            let index = with_poi(dataset.load(), (category, POI_FLAG_HIGHWAY), name, parents, (0.0, 0.0, 40.0, 60.0), true);
            address_at(&index, north, east)["road"].as_str().map(str::to_owned)
        };
        let (outside, inside) = ((30.0, -3.0), (30.0, 10.0));
        // NATURAL_WATER, WETLAND, BAY, GLACIER and RESERVOIR.
        for category in [99, 97, 91, 86, 148] {
            for linked in [true, false] {
                for point in [outside, inside] {
                    assert_eq!(answer(category, linked, point).as_deref(), Some("Seeplatz"), "category {category}, linked {linked}, {point:?}");
                }
            }
        }
        // An ATTRACTION.
        assert_eq!(answer(1, true, inside).as_deref(), Some("Seeplatz"));
        assert_eq!(answer(1, true, outside).as_deref(), Some("Mill Lane"));
        assert_eq!(answer(1, false, inside).as_deref(), Some("Main Street"));
    }

    #[test]
    fn a_water_street_competes_by_the_rules_of_a_street_not_those_of_a_poi() {
        // A water street is a street in Nominatim, of rank_address 26 or 27: a street that wins
        // its street lane refines to a house number it is the parent of alone, and a rank-30 node
        // inside the closest feature takes its place where that is an area of rank_search above
        // 27 alone (reverse.py lookup_street_poi), nor does a street take a head start. The lake,
        // 40 m by 60 m, of the highway flag, its parent street Mill Lane, holds the point asked
        // and an address node on Main Street 28 m off, Main Street 70 m south: the lake answers,
        // with no house number. Of tier 1, which gives a landmark polygon its head start of 50 m,
        // and 30 m east of the point asked, with Main Street 10 m north, it leaves the answer to
        // Main Street. An ATTRACTION of the flag keeps the POIs' rules: the node answers inside
        // it, on its street, and of tier 1 it wins beside it, on its parent street.
        let answer = |category: u8, tier: u8, address: bool, street_north: f64, (north, east): (f64, f64)| {
            let mut dataset = TestIndex::new();
            dataset.street("Main Street", &parallel(street_north, 200.0));
            if address {
                dataset.address("1", 10.0, 30.0);
            }
            let (name, lane) = (dataset.string("Seeplatz"), dataset.string("Mill Lane"));
            let pois = [((category, POI_FLAG_HIGHWAY), name, (lane, NO_DATA), (0.0, 0.0, 40.0, 60.0), true)];
            let address = address_at(&with_pois_of_tier(dataset.load(), &pois, tier), north, east);
            (address["road"].as_str().map(str::to_owned), address["house_number"].as_str().map(str::to_owned))
        };
        let road = |name: &str, house_number: Option<&str>| (Some(name.to_owned()), house_number.map(str::to_owned));
        let (inside, beside) = ((30.0, 10.0), (20.0, -30.0));
        // NATURAL_WATER, WETLAND, BAY, GLACIER and RESERVOIR.
        for category in [99, 97, 91, 86, 148] {
            assert_eq!(answer(category, 0, true, -40.0, inside), road("Seeplatz", None), "category {category}");
            assert_eq!(answer(category, 1, false, 30.0, beside), road("Main Street", None), "category {category}");
        }
        // An ATTRACTION.
        assert_eq!(answer(1, 0, true, -40.0, inside), road("Main Street", Some("1")));
        assert_eq!(answer(1, 1, false, 30.0, beside), road("Mill Lane", None));
    }

    #[test]
    fn a_water_street_matches_place_nodes_against_the_point_not_its_parent_street() {
        // Nominatim takes a street's address from the street itself, a rank-30 answer's from its
        // parent street (results.py _get_address_lookup_id). The lake, 40 m by 60 m, of the
        // highway flag, holds the point asked, 40 m east of Long Road, its parent street and the
        // nearest street, whose line the hamlet's 1000 m square crosses 3 km north of the point,
        // which it leaves out: the lake answers, and no hamlet. An ATTRACTION there answers on
        // Long Road, in the hamlet.
        let answer = |(category, flags): (u8, u8)| {
            let mut dataset = TestIndex::new();
            dataset.street("Long Road", &[(-1000.0, 0.0), (5000.0, 0.0)]).place("Justice", 4, 3000.0, 200.0);
            let (name, lane) = (dataset.string("Seeplatz"), dataset.string("Long Road"));
            let index = with_poi(dataset.load(), (category, flags), name, (lane, NO_DATA), (-20.0, 20.0, 20.0, 80.0), true);
            let address = address_at(&index, 0.0, 40.0);
            (address["road"].as_str().map(str::to_owned), address["hamlet"].as_str().map(str::to_owned))
        };
        // NATURAL_WATER, WETLAND, BAY, GLACIER and RESERVOIR.
        for category in [99, 97, 91, 86, 148] {
            assert_eq!(answer((category, POI_FLAG_HIGHWAY)), (Some("Seeplatz".to_owned()), None), "category {category}");
        }
        assert_eq!(answer((1, 0)), (Some("Long Road".to_owned()), Some("Justice".to_owned())));
    }

    #[test]
    fn water_no_address_anchors_keeps_its_landmark_and_places_but_gives_no_road_nor_postcode() {
        // A glacier 60 km north, outside Testland, holds the query, its parent street Coast
        // Road and its postcode 4510; no boundary holds the point and no street is near, and a
        // postcode centroid, 4520, is 10 m off. Water never takes the primary slot, so its road
        // and postcode don't answer, nor the centroid's, but where it would have taken the slot
        // its name answers as the landmark and in `places`. One of no linked parent street
        // never could, and the point finds nothing, as one in no POI.
        let answer = |linked: bool| {
            let mut dataset = TestIndex::new();
            dataset.centroid("4520", 60_030.0, 20.0);
            let (name, road, postcode) = (dataset.string("Coast Glacier"), dataset.string("Coast Road"), dataset.string("4510"));
            let parents = if linked { (road, postcode) } else { (NO_DATA, NO_DATA) };
            let index = with_poi(dataset.load(), (86, 0), name, parents, (60_000.0, 0.0, 60_040.0, 60.0), true);
            let (lat, lng) = query_point(60_030.0, 10.0);
            serde_json::to_string(&index.query(lat, lng)).unwrap()
        };
        let linked: serde_json::Value = serde_json::from_str(&answer(true)).unwrap();
        assert_eq!(linked["address"], serde_json::json!({"landmark": "Coast Glacier"}), "{linked}");
        assert_eq!(linked["places"][0]["name"], "Coast Glacier", "{linked}");
        assert_eq!(linked["licence"], OSM_LICENCE);
        assert_eq!(answer(false), NOTHING_FOUND);
    }

    #[test]
    fn a_bay_or_glacier_no_address_anchors_answers_nothing_where_a_specific_poi_is_found() {
        // A bay or glacier 60 km north, outside Testland, holds the query, its parent street
        // Coast Road; no boundary holds the point and no street is near. 150 m east of the
        // query, past the 100 m a POI takes the primary slot within, is a POI of Hill Road.
        // Were water in the lanes, a bay or glacier would be an area label there, as an island
        // is, and win the slot only where no specific POI is found: a school or a lake takes
        // the lane from it, too far to take the slot, and the point finds nothing, as one in
        // no water. An area label (a protected area) or a school of no linked parent street
        // leaves it the lane, and a lake, itself specific, keeps the lane from the farther
        // school: the water's name answers as the landmark.
        let answer = |water: u8, (category, linked): (u8, bool)| {
            let mut dataset = TestIndex::new();
            let (name, road) = (dataset.string("Coast Water"), dataset.string("Coast Road"));
            let (other, hill_road) = (dataset.string("Hilltop"), dataset.string("Hill Road"));
            let pois = [
                ((water, 0), name, (road, NO_DATA), (60_000.0, 0.0, 60_040.0, 60.0), true),
                ((category, 0), other, (if linked { hill_road } else { NO_DATA }, NO_DATA), (60_030.0, 160.0, 60_030.0, 160.0), false),
            ];
            let index = with_pois(dataset.load(), &pois);
            let (lat, lng) = query_point(60_030.0, 10.0);
            serde_json::to_value(index.query(lat, lng)).unwrap()
        };
        let nothing: serde_json::Value = serde_json::from_str(NOTHING_FOUND).unwrap();
        let (bay, glacier, lake, school, protected_area) = (91, 86, 99, 56, 131);
        for water in [bay, glacier] {
            assert_eq!(answer(water, (school, true)), nothing, "category {water}, a school");
            assert_eq!(answer(water, (lake, true)), nothing, "category {water}, a lake");
        }
        for (water, other) in [(glacier, (protected_area, true)), (glacier, (school, false)), (lake, (school, true))] {
            let held = answer(water, other);
            assert_eq!(held["address"], serde_json::json!({"landmark": "Coast Water"}), "category {water}, {other:?}: {held}");
        }
    }

    #[test]
    fn interpolation_files_of_no_ways_load_and_give_no_interpolation() {
        // What a full dir holds for a region without interpolations: the two files' footers.
        let mut dataset = TestIndex::new();
        dataset.street("Main Street", &parallel(0.0, 200.0));
        let dir = ScratchDir::new();
        dataset.write(dir.path());
        let none = way_files::encode(way_files::Kind::Interps, &[]);
        assert_eq!((none.ways.len(), none.nodes.len()), (8, 16));
        std::fs::write(dir.path().join("interp_ways.bin"), none.ways).unwrap();
        std::fs::write(dir.path().join("interp_nodes.bin"), none.nodes).unwrap();

        let index = load_dir(dir.path());

        assert_eq!(index.interps.as_ref().map(WayFiles::count), Some(0));
        let primary = primary_at(&index, 1.0, ALONG_EAST_M);
        assert_eq!(primary["street"]["name"], "Main Street");
        assert!(primary["interp"].is_null(), "{primary}");
        assert_eq!(number_on_street(&index, 1.0, ALONG_EAST_M, "Main Street", 100.0), None);
        assert_eq!(address_at(&index, 1.0, ALONG_EAST_M)["road"], "Main Street");
    }

    #[test]
    fn a_tombstone_of_each_kind_is_never_an_answer() {
        // Record 0 of each file is a tombstone, which no cell lists; the records after it
        // answer under their own ids, with their own side values.
        let index = TestIndex::new()
            .dead_street().dead_address().dead_interpolation()
            .street("Main Street", &parallel(0.0, 200.0)).way_postcode("2020")
            .address("7", 30.0, 0.0).addr_postcode("1010")
            .interpolation("Main St", (2, 100, 1), &parallel(8.0, 100.0))
            .load();

        let primary = primary_at(&index, 29.0, ALONG_EAST_M);
        assert_eq!((primary["addr"]["id"].as_u64(), primary["street"]["id"].as_u64(), primary["interp"]["id"].as_u64()), (Some(1), Some(1), Some(1)));
        assert_eq!((primary["addr"]["house_number"].as_str(), primary["street"]["name"].as_str(), primary["interp"]["number"].as_u64()),
                   (Some("7"), Some("Main Street"), Some(32)));

        let on_house = address_at(&index, 29.0, 0.0);
        assert_eq!((on_house["house_number"].as_str(), on_house["road"].as_str(), on_house["postcode"].as_str()),
                   (Some("7"), Some("Main Street"), Some("1010")));
        let on_street = address_at(&index, 1.0, 0.0);
        assert_eq!((on_street["house_number"].as_str(), on_street["road"].as_str(), on_street["postcode"].as_str()),
                   (Some("7"), Some("Main Street"), Some("1010")));
    }

    #[test]
    fn a_street_or_interpolation_tombstone_a_cell_lists_is_skipped() {
        let index = TestIndex::new()
            .dead_street().listed(geo_index::STREETS, 0, 1.0, ALONG_EAST_M)
            .dead_interpolation().listed(geo_index::INTERPOLATIONS, 0, 1.0, ALONG_EAST_M)
            .street("Main Street", &parallel(0.0, 200.0))
            .interpolation("Main St", (2, 100, 1), &parallel(8.0, 100.0))
            .load();

        let primary = primary_at(&index, 1.0, ALONG_EAST_M);
        assert_eq!((primary["street"]["id"].as_u64(), primary["street"]["name"].as_str()), (Some(1), Some("Main Street")));
        assert_eq!((primary["interp"]["id"].as_u64(), primary["interp"]["number"].as_u64()), (Some(1), Some(32)));
        assert_eq!(number_on_street(&index, 1.0, ALONG_EAST_M, "Main Street", 100.0), Some(32));
        assert_eq!(address_at(&index, 1.0, ALONG_EAST_M)["road"], "Main Street");
    }

    #[test]
    fn an_address_tombstone_a_cell_lists_is_read_as_a_node_at_zero() {
        // Nothing tells an address tombstone from a node at (0, 0) whose house number and
        // street are string 0. No builder lists one in a cell; where a cell at (0, 0) does, the
        // scan reads that node.
        const NULL_ISLAND: (f64, f64) = (0.0, 0.0);
        let mut dataset = TestIndex::around(NULL_ISLAND);
        dataset.dead_address().listed(geo_index::ADDRESSES, 0, 0.0, 0.0).address("7", 30.0, 0.0);
        let index = dataset.load();
        let primary_from = |north_m, east_m| {
            let p = at_from(NULL_ISLAND, north_m, east_m);
            index.debug_primary(p.lat as f64, p.lng as f64)["addr"].clone()
        };

        let addr = primary_from(5.0, 5.0);
        assert_eq!((addr["id"].as_u64(), addr["lat"].as_f64(), addr["lng"].as_f64(), addr["vertex_count"].as_u64()),
                   (Some(0), Some(0.0), Some(0.0), Some(0)));
        assert!(about(&addr["dist_m"], 7.0), "{addr}");
        // String 0 is Testland's name.
        assert_eq!(address_from(&index, NULL_ISLAND, 5.0, 5.0)["house_number"], "Testland");
        // The living node, where it is the nearer.
        assert_eq!(primary_from(25.0, 0.0)["house_number"], "7");
    }

    /// The room this thread's way buffer has between reads.
    fn way_buffer_room() -> usize {
        WAY_NODES.with(|kept| {
            let nodes = kept.take();
            let room = nodes.capacity();
            kept.set(nodes);
            room
        })
    }

    /// The room this thread's footprint buffer has between reads.
    fn footprint_buffer_room() -> usize {
        FOOTPRINT.with(|kept| {
            let vertices = kept.take();
            let room = vertices.capacity();
            kept.set(vertices);
            room
        })
    }

    #[test]
    fn a_footprint_of_more_vertices_than_the_footprint_buffer_keeps_room_for_leaves_it_no_larger() {
        // House 3 is a building 30 m square. House 5, 5 km north of it, is one 400 m square
        // whose outline has more vertices than the buffer keeps room for: half as many as that
        // along each wall, and the first again.
        const WALL: usize = FOOTPRINT_KEPT / 2;
        const VERTICES: usize = 4 * WALL + 1;
        let corners = [(5000.0, 0.0), (5000.0, 400.0), (5400.0, 400.0), (5400.0, 0.0), (5000.0, 0.0)];
        let mut ring: Vec<NodeCoord> = corners
            .windows(2)
            .flat_map(|wall| {
                let ((north, east), (to_north, to_east)) = (wall[0], wall[1]);
                (0..WALL).map(move |i| {
                    let along = i as f64 / WALL as f64;
                    at(north + (to_north - north) * along, east + (to_east - east) * along)
                })
            })
            .collect();
        ring.push(ring[0]);
        assert_eq!(ring.len(), VERTICES);
        let mut dataset = TestIndex::new();
        dataset.street("Main Street", &parallel(-40.0, 200.0)).building("3", (10.0, 20.0, 40.0, 50.0));
        dataset.street("Long Row", &parallel(4960.0, 200.0)).address_point("5", at(5200.0, 200.0), Some(Footprint::of(&ring)));
        let index = dataset.load();
        assert_eq!(footprint_buffer_room(), 0);
        let nearest = |north_m, east_m| {
            let addr = primary_at(&index, north_m, east_m)["addr"].clone();
            (addr["house_number"].as_str().map(str::to_owned), addr["vertex_count"].as_u64(), addr["dist_bits"].as_str().map(str::to_owned))
        };
        let inside = Some(format!("{:016x}", 0f64.to_bits()));

        // A query of the small building leaves the buffer with the room it keeps.
        assert_eq!(nearest(15.0, 45.0), (Some("3".to_owned()), Some(5), inside.clone()));
        assert_eq!(footprint_buffer_room(), FOOTPRINT_KEPT);

        // Both scans of the large one, from inside it, 14 m from its point.
        assert_eq!(nearest(5190.0, 190.0), (Some("5".to_owned()), Some(VERTICES as u64), inside.clone()));
        assert_eq!(house_number_on_way(&index, 5190.0, 190.0, 1, 10.0).as_deref(), Some("5"));
        assert!(footprint_buffer_room() <= FOOTPRINT_KEPT, "{}", footprint_buffer_room());

        // The small building answers as before, from a buffer of that room again.
        assert_eq!(nearest(15.0, 45.0), (Some("3".to_owned()), Some(5), inside));
        assert_eq!(footprint_buffer_room(), FOOTPRINT_KEPT);
    }

    #[test]
    fn a_way_of_more_nodes_than_the_way_buffer_keeps_room_for_leaves_it_no_larger() {
        // A street and an interpolation line of as many nodes as a way's count holds, a metre
        // apart, and a short street with a short line 5 km north of them. The long street runs
        // through Northshire and its parent is Farshire.
        const NODES: usize = u16::MAX as usize;
        let half_m = (NODES / 2) as f64;
        let line = |north_m: f64| (0..NODES).map(|i| (north_m, i as f64 - half_m)).collect::<Vec<(f64, f64)>>();
        let mut dataset = TestIndex::in_country(b"US");
        dataset
            .boundary("Northshire", 4, 0, (-100.0, -40_000.0, 100.0, 40_000.0))
            .boundary("Farshire", 4, 0, (60_000.0, 60_000.0, 70_000.0, 70_000.0))
            .street("Long Road", &line(0.0)).way_parent(2)
            .interpolation("Long Rd", (1, 65535, 0), &line(5.0))
            .street("Short Street", &parallel(5000.0, 200.0))
            .interpolation("Short St", (2, 100, 1), &parallel(5008.0, 100.0));
        let mut index = dataset.load();
        // Farshire's parent is Testland, the top of the chain.
        index.admin_parents = Some(FileBytes::Owned([NO_DATA, NO_DATA, 0].iter().flat_map(|parent| parent.to_le_bytes()).collect()));
        assert_eq!(way_buffer_room(), 0);

        // A query of the short ways leaves the buffer with the room it keeps.
        let short = address_at(&index, 5001.0, ALONG_EAST_M);
        assert_eq!((short["road"].as_str(), short["house_number"].as_str()), (Some("Short Street"), Some("32")));
        assert_eq!(way_buffer_room(), WAY_NODES_KEPT);

        // Every read of the long ways: the two scans, the street's line for its place nodes,
        // the house number along the interpolation line, and the street's point nearest the
        // query, where its state is read: the query's is not its parent's.
        let long = address_at(&index, 1.0, 0.0);
        assert_eq!((long["road"].as_str(), long["house_number"].as_str()), (Some("Long Road"), Some("32768")));
        assert_eq!(long["state"], "Northshire");
        let primary = primary_at(&index, 1.0, 0.0);
        assert_eq!((primary["street"]["name"].as_str(), primary["interp"]["street"].as_str()), (Some("Long Road"), Some("Long Rd")));
        assert!(way_buffer_room() <= WAY_NODES_KEPT, "{}", way_buffer_room());

        // The short ways answer as before, from a buffer of that room again.
        assert_eq!(address_at(&index, 5001.0, ALONG_EAST_M), short);
        assert_eq!(way_buffer_room(), WAY_NODES_KEPT);
    }

    #[test]
    fn a_street_of_65535_nodes_answers_along_its_whole_length() {
        // As many nodes as a way's count holds, a metre apart along the origin's parallel.
        const NODES: usize = u16::MAX as usize;
        let half_m = (NODES / 2) as f64;
        let line: Vec<(f64, f64)> = (0..NODES).map(|i| (0.0, i as f64 - half_m)).collect();
        let index = TestIndex::new().street("Long Road", &line).place("West End", 4, 200.0, 500.0 - half_m).load();

        for east_m in [-half_m, 0.0, half_m] {
            let street = primary_at(&index, 10.0, east_m)["street"].clone();
            assert_eq!(street["name"], "Long Road", "{east_m}");
            assert!(about(&street["dist_m"], 10.0), "{east_m}: {street}");
        }
        // The hamlet's square crosses the street's first kilometre, 65 km from its last node.
        let address = address_at(&index, 10.0, half_m);
        assert_eq!((address["road"].as_str(), address["hamlet"].as_str()), (Some("Long Road"), Some("West End")));
    }

    /// The logical files of `ways` as the builder's types.h lays their records out: a WayHeader
    /// a street or an InterpWay an interpolation way, its node_offset the nodes of the ways
    /// before it (0 of a way without nodes), and the ways' nodes back to back.
    fn logical_ways(ways: &[Way], kind: way_files::Kind) -> [(&'static str, Vec<u8>); 2] {
        let (mut records, mut nodes) = (Vec::new(), Vec::new());
        let mut nodes_before = 0u32;
        for way in ways {
            let (start_number, end_number, interpolation) = way.numbers;
            records.extend((if way.nodes.is_empty() { 0 } else { nodes_before }).to_le_bytes());
            records.extend((way.nodes.len() as u16).to_le_bytes());
            records.extend([0; 2]);
            records.extend(way.name_id.to_le_bytes());
            if kind == way_files::Kind::Interps {
                records.extend(start_number.to_le_bytes());
                records.extend(end_number.to_le_bytes());
                records.extend([interpolation, 0, 0, 0]);
            }
            for node in &way.nodes {
                nodes.extend(node.lat.to_le_bytes());
                nodes.extend(node.lng.to_le_bytes());
            }
            nodes_before += way.nodes.len() as u32;
        }
        [(kind.ways_file(), records), (kind.nodes_file(), nodes)]
    }

    /// The dataset's centroids as logical rows (builder postcode_files.h's LogicalCentroid), in
    /// cell order, and the cells they are in.
    fn logical_centroids(dataset: &TestIndex) -> (Vec<u8>, usize) {
        let mut rows: Vec<(u32, u32, u16, f32, f32)> = dataset.centroids.iter()
            .map(|c| ((cell_id_at_level(c.lat as f64, c.lng as f64, DEFAULT_ADMIN_CELL_LEVEL) >> 32) as u32, c.postcode_id, c.country_code, c.lat, c.lng))
            .collect();
        rows.sort_by_key(|r| (r.0, r.1, r.2));
        let cells = rows.iter().map(|r| r.0).collect::<std::collections::BTreeSet<_>>().len();
        let mut bytes = Vec::new();
        for (cell_key, postcode_id, country_code, lat, lng) in rows {
            bytes.extend(lat.to_le_bytes());
            bytes.extend(lng.to_le_bytes());
            bytes.extend(postcode_id.to_le_bytes());
            bytes.extend(country_code.to_le_bytes());
            bytes.extend([0, 0]);
            bytes.extend(cell_key.to_le_bytes());
        }
        (bytes, cells)
    }

    /// The logical files of the dataset's street, interpolation, address, side and postcode
    /// centroid files, by name: a family's where `TestIndex::files` writes the family's.
    fn logical_files(dataset: &TestIndex) -> BTreeMap<String, Vec<u8>> {
        let mut files: Vec<(&'static str, Vec<u8>)> = Vec::new();
        if !(dataset.streets.is_empty() && dataset.addrs.is_empty() && dataset.interps.is_empty()) {
            files.extend(logical_ways(&dataset.streets, way_files::Kind::Streets));
            if !dataset.interps.is_empty() {
                files.extend(logical_ways(&dataset.interps, way_files::Kind::Interps));
            }
            if !dataset.addrs.is_empty() {
                let (points, vertices) = addr_logical(&dataset.addrs);
                files.extend([(addr_files::POINTS_FILE, points), (addr_files::VERTICES_FILE, vertices)]);
            }
            let bytes = |values: Vec<u32>| values.iter().flat_map(|value| value.to_le_bytes()).collect();
            files.extend(dataset.side_values().into_iter().map(|(name, values)| (name, bytes(values))));
        }
        if !dataset.centroids.is_empty() {
            files.push((postcode_files::CENTROIDS_FILE, logical_centroids(dataset).0));
        }
        files.into_iter().map(|(name, bytes)| (name.to_owned(), bytes)).collect()
    }

    /// What a dump of a dir gave (examples/dump_logical.rs): its exit status, its report, what
    /// it said of an error, and the files it wrote, by name.
    struct Dump {
        status: u8,
        report: String,
        errors: String,
        files: BTreeMap<String, Vec<u8>>,
    }

    /// The dump of `dir` into `out`.
    fn dump_into(dir: &std::path::Path, out: &std::path::Path) -> Dump {
        let args = [dir, out].map(|path| path.to_str().unwrap().to_owned());
        let (mut report, mut errors) = (Vec::new(), Vec::new());
        let status = crate::dump_logical::run(&args, &mut report, &mut errors);
        let files = std::fs::read_dir(out).unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (entry.file_name().into_string().unwrap(), std::fs::read(entry.path()).unwrap())
            })
            .collect();
        Dump { status, report: String::from_utf8(report).unwrap(), errors: String::from_utf8(errors).unwrap(), files }
    }

    /// The dump of `dir` into a fresh dir.
    fn dump_of(dir: &std::path::Path) -> Dump {
        dump_into(dir, ScratchDir::new().path())
    }

    /// The dump of the dataset's dir once `edit` has changed it.
    fn dump_after(dataset: &TestIndex, edit: impl FnOnce(&std::path::Path)) -> Dump {
        let dir = ScratchDir::new();
        dataset.write(dir.path());
        edit(dir.path());
        dump_of(dir.path())
    }

    /// The families a dump reports, each by its first file.
    const DUMP_FAMILIES: [&str; 9] = [
        "street_ways.bin", "interp_ways.bin", "addr_points.bin", "way_parents.bin", "way_postcodes.bin",
        "addr_postcodes.bin", "addr_cities.bin", "interp_postcodes.bin", "postcode_centroids.bin",
    ];

    /// The report of a dump in which no read disagreed with another: for each family what it
    /// read and how many of its reads gave nothing, and none of a family the dir is without.
    fn dump_report(families: [Option<(String, u64)>; 9]) -> String {
        let (mut report, mut nothing) = (String::new(), 0);
        for (file, family) in DUMP_FAMILIES.iter().zip(families) {
            report += &match family {
                Some((read, gave_nothing)) => {
                    nothing += gave_nothing;
                    format!("{file}: {read}, {gave_nothing} reads gave nothing, 0 disagreed\n")
                }
                None => format!("{file}: none\n"),
            };
        }
        report + &format!("{nothing} reads gave nothing, 0 disagreed\n")
    }

    /// What a dump reports of the dataset's families, every read giving its record.
    fn whole_dump_report(dataset: &TestIndex) -> [Option<(String, u64)>; 9] {
        let ways = |ways: &[Way]| {
            let nodes: usize = ways.iter().map(|way| way.nodes.len()).sum();
            (format!("{} ways, {nodes} nodes", ways.len()), 0)
        };
        let footprints = dataset.addrs.iter().filter(|addr| addr.footprint.is_some()).count();
        let has = !(dataset.streets.is_empty() && dataset.addrs.is_empty() && dataset.interps.is_empty());
        let sides = dataset.side_values();
        let side = |name: &str| sides.iter().find(|side| side.0 == name).map(|side| (format!("{} values", side.1.len()), 0));
        [
            has.then(|| ways(&dataset.streets)),
            (!dataset.interps.is_empty()).then(|| ways(&dataset.interps)),
            (!dataset.addrs.is_empty()).then(|| (format!("{} records, {footprints} footprints", dataset.addrs.len()), 0)),
            side("way_parents.bin"), side("way_postcodes.bin"), side("addr_postcodes.bin"), side("addr_cities.bin"),
            side("interp_postcodes.bin"),
            (!dataset.centroids.is_empty())
                .then(|| (format!("{} centroids in {} cells", dataset.centroids.len(), logical_centroids(dataset).1), 0)),
        ]
    }

    /// Asserts that the dump wrote the files of `want`, each with its bytes there but the ones
    /// `other` names, which have the bytes it gives.
    fn assert_dumped(dump: &Dump, want: &BTreeMap<String, Vec<u8>>, other: &[(&str, Vec<u8>)]) {
        assert_eq!(dump.files.keys().collect::<Vec<_>>(), want.keys().collect::<Vec<_>>());
        for (name, bytes) in want {
            let bytes = other.iter().find(|(other, _)| other == name).map_or(bytes, |(_, bytes)| bytes);
            assert!(dump.files[name] == *bytes, "{name}: {} bytes for {}", dump.files[name].len(), bytes.len());
        }
    }

    /// Testland with every kind of record the dumped files hold, across the groups, blocks and
    /// superblocks their files keep them in: tombstones of each kind, the first record and
    /// among the others; streets of one node, of two and of as many as a way's count holds;
    /// interpolations of every type, one counting down; addresses that are nodes, closed and
    /// open outlines, a single vertex, a building kilometres from its point and an outline of
    /// more vertices than a blob holds; one before any street; each side file with values for
    /// some of its records (addr:city among them); and postcode centroids, a cell of one, cells of a block's worth either
    /// side of 32 and one of hundreds, of two countries, a postcode of both.
    fn every_kind_of_record() -> TestIndex {
        let mut dataset = TestIndex::new();
        dataset.address("1", -500.0, 0.0).dead_street().dead_interpolation();
        for i in 0..40u32 {
            let north = 20.0 * f64::from(i);
            dataset.street(&format!("Street {i}"), &parallel(north, 200.0));
            if i % 3 == 0 {
                dataset.way_parent(0);
            }
            if i % 4 == 1 {
                dataset.way_postcode(&format!("{}", 1000 + i));
            }
            for j in 0..30u32 {
                let (south, west) = (north + 3.0, 10.0 * f64::from(j) - 150.0);
                let house_number = format!("{}", 2 * j + 1);
                match j % 3 {
                    0 => dataset.address(&house_number, south, west),
                    1 => dataset.building(&house_number, (south, west, south + 6.0, west + 7.0)),
                    _ => {
                        let outline = [at(south, west), at(south + 5.0, west), at(south + 5.0, west + 4.0)];
                        dataset.address_point(&house_number, at(south + 2.0, west + 2.0), Some(Footprint::of(&outline)))
                    }
                };
                if j % 7 == 2 {
                    dataset.addr_postcode(&format!("{}", 2000 + j));
                }
                if j % 5 == 1 {
                    dataset.addr_city(&format!("Town {}", j % 3));
                }
            }
            dataset.interpolation(&format!("Street {i}"), (1 + 2 * i, 199 - i, (i % 3) as u8), &parallel(north + 1.0, 100.0));
            if i % 5 == 3 {
                dataset.interp_postcode(&format!("{}", 3000 + i));
            }
            if i == 17 {
                dataset.dead_street().dead_interpolation();
            }
        }
        dataset.interpolation("Street 0", (99, 1, 0), &parallel(-5.0, 100.0));
        dataset.street("Stub", &[(900.0, 0.0)]).address_point("S", at(905.0, 0.0), Some(Footprint::of(&[at(906.0, 1.0)])));
        let far: Vec<NodeCoord> = [(3000.0, 0.0), (3000.0, 9.0), (3009.0, 9.0), (3000.0, 0.0)].iter().map(|&(n, e)| at(n, e)).collect();
        dataset.address_point("F", at(905.0, 10.0), Some(Footprint::of(&far)));
        let many: Vec<NodeCoord> = (0..65_536).map(|i| at(4000.0 + f64::from(i % 256) * 0.5, f64::from(i / 256) * 0.5)).collect();
        dataset.address_point("M", at(4064.0, 64.0), Some(Footprint::of(&many))).dead_address();
        // No cell lists the long street: the dump reads it by its id.
        let name_id = dataset.string("Long Road");
        dataset.streets.push(Way { name_id, nodes: (0..65_535).map(|i| at(2000.0, f64::from(i))).collect(), numbers: (0, 0, 0) });
        for (cell, (count, north)) in [(1, 0.0), (31, 15_000.0), (33, 30_000.0), (300, -15_000.0)].into_iter().enumerate() {
            for k in 0..count {
                dataset.centroid(&format!("{}", 10_000 * (cell + 1) + k), north + f64::from(k as u32 % 20) * 30.0, f64::from(k as u32 / 20) * 30.0);
            }
        }
        let shared = dataset.centroids[5];
        dataset.centroids.push(postcode_files::Centroid { lat: shared.lat + 0.0001, country_code: u16::from_be_bytes(*b"yy"), ..shared });
        dataset
    }

    #[test]
    fn a_dump_of_a_dir_is_the_logical_files_of_the_records_it_was_built_from() {
        let dataset = every_kind_of_record();
        let nodes = |ways: &[Way]| ways.iter().map(|way| way.nodes.len()).collect::<Vec<usize>>();
        assert!([0, 1, 2, 65_535].iter().all(|count| nodes(&dataset.streets).contains(count)) && dataset.streets.len() == 44);
        // A way without nodes after ways with nodes, of each kind.
        assert_eq!((nodes(&dataset.streets)[19], nodes(&dataset.interps)[19], dataset.interps.len()), (0, 0, 43));
        let vertices = |addr: &Addr| addr.footprint.as_ref().map_or(0, |footprint| footprint.steps.len());
        let outlines: Vec<usize> = dataset.addrs.iter().map(vertices).collect();
        assert!([0, 1, 3, 4, 5, 65_536].iter().all(|count| outlines.contains(count)) && dataset.addrs.len() == 1205);
        assert_eq!(dataset.side_values().len(), 5);

        let dir = ScratchDir::new();
        dataset.write(dir.path());
        let dump = dump_of(dir.path());
        assert_eq!((dump.status, dump.errors.as_str()), (0, ""));
        let want = logical_files(&dataset);
        assert_eq!(want.len(), 12);
        assert_dumped(&dump, &want, &[]);
        assert_eq!(dump.report, dump_report(whole_dump_report(&dataset)));
        let lines: Vec<&str> = dump.report.lines().collect();
        assert_eq!(lines.len(), 10);
        assert_eq!(lines[0], "street_ways.bin: 44 ways, 65616 nodes, 0 reads gave nothing, 0 disagreed");
        assert_eq!(lines[1], "interp_ways.bin: 43 ways, 82 nodes, 0 reads gave nothing, 0 disagreed");
        assert_eq!(lines[2], "addr_points.bin: 1205 records, 803 footprints, 0 reads gave nothing, 0 disagreed");
        assert_eq!(lines[6], "addr_cities.bin: 1205 values, 0 reads gave nothing, 0 disagreed");
        assert_eq!(lines[7], "interp_postcodes.bin: 43 values, 0 reads gave nothing, 0 disagreed");
        let cells = logical_centroids(&dataset).1;
        assert!(cells >= 4);
        assert_eq!(lines[8], format!("postcode_centroids.bin: 366 centroids in {cells} cells, 0 reads gave nothing, 0 disagreed"));
        assert_eq!(lines[9], "0 reads gave nothing, 0 disagreed");

        // The dir is one the server answers from.
        let address = address_at(&load_dir(dir.path()), 4.0, -148.0);
        assert_eq!((address["road"].as_str(), address["house_number"].as_str()), (Some("Street 0"), Some("1")));
    }

    #[test]
    fn a_dump_holds_a_footprint_of_each_encoding_as_its_logical_blob() {
        let here = NodeCoord { lat: 52.37, lng: 4.89 };
        // A corner, a wall along each axis, the far corner and the first again; the same in
        // steps of 32 bits.
        let ring = [(0u32, 0u32), (0, 310), (120, 310), (120, 0), (0, 0)];
        let wide = ring.map(|(north, east)| (north << 14, east << 14));
        // The bbox minimum where the point puts it, and a degree south-west of that.
        let near = |scale: f64, north: u32, east: u32| NodeCoord {
            lat: (f64::from(here.lat) - f64::from(north) * scale) as f32,
            lng: (f64::from(here.lng) - f64::from(east) * scale) as f32,
        };
        let far = NodeCoord { lat: here.lat - 1.0, lng: here.lng - 1.0 };
        let footprints: [(u8, NodeCoord, &[(u32, u32)]); 9] = [
            (0, near(1e-5, 48, 124), &ring), (0, far, &ring),
            (1, near(1e-4, 48, 124), &ring), (1, far, &ring[..2]),
            (2, near(1e-6, 48, 124), &ring), (2, far, &ring[..1]),
            (3, near(1e-7, 48 << 14, 124 << 14), &wide), (3, far, &wide[..4]), (3, far, &wide[..1]),
        ];
        let (mut points, mut vertices) = (Vec::new(), Vec::new());
        let mut record = |footprint: Option<&(u8, NodeCoord, &[(u32, u32)])>| {
            let (vertex_offset, vertex_count) = footprint.map_or((NO_DATA, 0), |footprint| {
                (addr_files::vertex_offset_of(vertices.len() as u64).unwrap(), footprint.2.len() as u32)
            });
            for field in [here.lat.to_bits(), here.lng.to_bits(), 1, 2, 3, vertex_offset, vertex_count] {
                points.extend(field.to_le_bytes());
            }
            let Some(&(encoding, min, steps)) = footprint else { return };
            vertices.extend([encoding, 0]);
            vertices.extend(min.lat.to_le_bytes());
            vertices.extend(min.lng.to_le_bytes());
            for &(north, east) in steps {
                if encoding == 3 {
                    vertices.extend(north.to_le_bytes());
                    vertices.extend(east.to_le_bytes());
                } else {
                    vertices.extend((north as u16).to_le_bytes());
                    vertices.extend((east as u16).to_le_bytes());
                }
            }
        };
        record(None);
        footprints.iter().for_each(|footprint| record(Some(footprint)));
        record(None);
        let files = addr_files::encode(&points, &vertices);
        // Each blob's tag: its encoding, 4 of a closed ring, 8 of a bbox minimum in two residuals;
        // and escapes.
        let tags: Vec<u8> = {
            let addrs = AddrFiles::open(Some(FileBytes::Owned(files.points.clone())), Some(FileBytes::Owned(files.vertices.clone())),
                                        Some(FileBytes::Owned(files.index.clone()))).unwrap().unwrap();
            (1..10).map(|id| files.vertices[addrs.record(id).unwrap().blob_at as usize]).collect()
        };
        assert_eq!(tags, [4 | 8, 4, 1 | 4 | 8, 1, 2 | 4 | 8, 2, 0x80, 0x80, 0x80]);

        // A dir of the three files alone.
        let dir = ScratchDir::new();
        let three = [(addr_files::POINTS_FILE, &files.points), (addr_files::VERTICES_FILE, &files.vertices), (addr_files::INDEX_FILE, &files.index)];
        for (name, bytes) in three {
            std::fs::write(dir.path().join(name), bytes).unwrap();
        }
        let dump = dump_of(dir.path());
        assert_eq!((dump.status, dump.errors.as_str()), (0, ""));
        let want = BTreeMap::from([(addr_files::POINTS_FILE.to_owned(), points), (addr_files::VERTICES_FILE.to_owned(), vertices)]);
        assert_dumped(&dump, &want, &[]);
        let read = Some(("11 records, 9 footprints".to_owned(), 0));
        assert_eq!(dump.report, dump_report([None, None, read, None, None, None, None, None, None]));
    }

    /// `two_of_each` with a building, the third address, and 40 postcode centroids in a cell.
    fn two_of_each_and_a_building() -> TestIndex {
        let mut dataset = two_of_each();
        dataset.building("11", (1040.0, 10.0, 1050.0, 20.0));
        for k in 0..40 {
            dataset.centroid(&format!("{}", 1000 + k), 2000.0 + f64::from(k) * 20.0, 0.0);
        }
        dataset
    }

    #[test]
    fn a_dump_counts_the_reads_that_give_nothing_and_fails() {
        let dataset = two_of_each_and_a_building();
        let want = logical_files(&dataset);
        let whole = dump_after(&dataset, |_| ());
        assert_eq!((whole.status, whole.errors.as_str(), whole.report.as_str()), (0, "", dump_report(whole_dump_report(&dataset)).as_str()));
        assert_dumped(&whole, &want, &[]);

        // The dump of the dir with a change to one byte of one file, and what it reports with
        // `read` in place of the family's line.
        let damaged = |file: &'static str, at: usize, change: fn(u8) -> u8, family: usize, read: (&str, u64)| {
            let dump = dump_after(&dataset, |dir| {
                let mut bytes = std::fs::read(dir.join(file)).unwrap();
                bytes[at] = change(bytes[at]);
                std::fs::write(dir.join(file), bytes).unwrap();
            });
            let mut families = whole_dump_report(&dataset);
            families[family] = Some((read.0.to_owned(), read.1));
            assert_eq!((dump.status, dump.errors.as_str(), dump.report.as_str()), (1, "", dump_report(families).as_str()), "{file}");
            dump
        };
        // The second way of a pair, its block of a width no block has (byte 18 of its row is
        // its code's): its row reads, its nodes do not, and the way before it is whole.
        for (ways, nodes, family) in [("street_ways.bin", "street_nodes.bin", 0), ("interp_ways.bin", "interp_nodes.bin", 1)] {
            let dump = damaged(ways, 18, |_| 33, family, ("2 ways, 2 nodes", 1));
            assert_dumped(&dump, &want, &[(nodes, want[nodes][..16].to_vec())]);
        }
        // A block of addresses with a flag no block has: none of its three records reads, in
        // the run or alone, and each is written as zeros.
        let dump = damaged(addr_files::POINTS_FILE, 0, |flags| flags | 8, 2, ("3 records, 0 footprints", 6));
        assert_dumped(&dump, &want, &[(addr_files::POINTS_FILE, vec![0; 3 * 28]), (addr_files::VERTICES_FILE, Vec::new())]);
        // The building's blob with a tag no blob has: no vertices, no logical blob and no
        // count, and its record is written as a point's.
        let dump = damaged(addr_files::VERTICES_FILE, 0, |tag| tag | 0x20, 2, ("3 records, 0 footprints", 3));
        let mut as_a_point = want[addr_files::POINTS_FILE][..3 * 28 - 8].to_vec();
        as_a_point.extend([NO_DATA, 0].iter().flat_map(|field| field.to_le_bytes()));
        assert_dumped(&dump, &want, &[(addr_files::POINTS_FILE, as_a_point), (addr_files::VERTICES_FILE, Vec::new())]);
        // A side file's block in a mode no block has: neither of its values reads.
        for (family, (name, _)) in SIDE_FILES.into_iter().enumerate() {
            let values = want[name].len() / 4;
            let dump = damaged(name, 0, |_| 7, 3 + family, (&format!("{values} values"), values as u64));
            assert_dumped(&dump, &want, &[(name, vec![0; 4 * values])]);
        }
        // The first block of centroids with its bits past the file (the top byte of its entry's
        // offset): none of its 32 rows reads, in the run or whole, and each is written as zeros.
        let centroids = postcode_files::CENTROIDS_FILE;
        let size = dataset.files().into_iter().find(|(name, _)| *name == centroids).unwrap().1.len();
        // Two blocks, their entries of 29 bytes before the footer's 8.
        let read = format!("40 centroids in {} cells", logical_centroids(&dataset).1);
        let dump = damaged(centroids, size - 8 - 2 * 29 + 15, |_| 0xFF, 8, (&read, 64));
        let mut first_zeros = vec![0; 32 * 20];
        first_zeros.extend(&want[centroids][32 * 20..]);
        assert_dumped(&dump, &want, &[(centroids, first_zeros)]);

        // The reads of every family that gave nothing are counted together.
        let dump = dump_after(&dataset, |dir| {
            for (file, at, byte) in [("street_ways.bin", 18, 33), ("addr_points.bin", 0, 8), ("way_postcodes.bin", 0, 7)] {
                let mut bytes = std::fs::read(dir.join(file)).unwrap();
                bytes[at] = byte;
                std::fs::write(dir.join(file), bytes).unwrap();
            }
        });
        assert_eq!(dump.status, 1);
        assert!(dump.report.ends_with("\n9 reads gave nothing, 0 disagreed\n"), "{}", dump.report);
    }

    #[test]
    fn a_dump_refuses_a_dir_with_part_of_a_family_and_writes_nothing() {
        let dataset = two_of_each_and_a_building();
        // What the dump says of the dir after the edit, after the dir's name.
        let refusal = |edit: &dyn Fn(&std::path::Path)| {
            let dump = dump_after(&dataset, |dir| edit(dir));
            assert_eq!((dump.status, dump.report.as_str(), dump.files.len()), (2, "", 0), "{}", dump.errors);
            let said = dump.errors.strip_prefix("dump_logical: ").and_then(|said| said.strip_suffix('\n')).expect("a line").to_owned();
            said.split_once(": ").map(|(_, said)| said.to_owned()).unwrap_or(said)
        };
        let without = |files: &[&str]| refusal(&|dir| files.iter().for_each(|file| std::fs::remove_file(dir.join(file)).unwrap()));
        // One file of a pair of way files.
        for (ways, nodes) in WAY_FILES {
            assert_eq!(without(&[ways]), format!("{nodes} without {ways}"));
            assert_eq!(without(&[nodes]), format!("{ways} without {nodes}"));
        }
        // Some of the three address files.
        let (points, vertices, index) = (addr_files::POINTS_FILE, addr_files::VERTICES_FILE, addr_files::INDEX_FILE);
        assert_eq!(without(&[index]), format!("{points} and {vertices} without {index}"));
        assert_eq!(without(&[vertices]), format!("{points} and {index} without {vertices}"));
        assert_eq!(without(&[points, vertices]), format!("{index} without {points} and {vertices}"));
        // A side file without the file its values' records are in, or of another count.
        assert_eq!(without(&["street_ways.bin", "street_nodes.bin"]), "way_parents.bin without street_ways.bin");
        assert_eq!(without(&["street_ways.bin", "street_nodes.bin", "way_parents.bin"]), "way_postcodes.bin without street_ways.bin");
        assert_eq!(without(&[points, vertices, index]), format!("addr_postcodes.bin without {points}"));
        assert_eq!(without(&[points, vertices, index, "addr_postcodes.bin"]), format!("addr_cities.bin without {points}"));
        assert_eq!(without(&["interp_ways.bin", "interp_nodes.bin"]), "interp_postcodes.bin without interp_ways.bin");
        // One file of the postcode centroids' pair.
        let (centroids, cells) = (postcode_files::CENTROIDS_FILE, postcode_files::CELLS_FILE);
        assert_eq!(without(&[centroids]), format!("{cells} without {centroids}"));
        assert_eq!(without(&[cells]), format!("{centroids} without {cells}"));
        for (name, parent) in SIDE_FILES {
            let records = if parent == points { 3 } else { 2 };
            let said = refusal(&|dir| std::fs::write(dir.join(name), side_blocks::encode(&[5; 4])).unwrap());
            assert_eq!(said, format!("{name} holds 4 values for the {records} records of {parent}"));
        }
        // Files no builder writes: fixed-width records, without a footer.
        let fixed_width = |file: &'static str| refusal(&|dir| std::fs::write(dir.join(file), [0u8; 24]).unwrap());
        assert_eq!(fixed_width("street_ways.bin"), "street_ways.bin has no SWY1 footer");
        assert_eq!(fixed_width("interp_nodes.bin"), "interp_nodes.bin has no IWN1 footer");
        assert_eq!(fixed_width(points), "addr_points.bin has no ADB1 footer");
        assert_eq!(fixed_width("way_parents.bin"), "way_parents.bin has no SDB1 footer");
        assert_eq!(fixed_width(postcode_files::CENTROIDS_FILE), "postcode_centroids.bin has no PCC1 footer");
        // A dir that is not there.
        assert!(refusal(&|dir| std::fs::remove_dir_all(dir).unwrap()).ends_with(" is not a dir"));
    }

    #[test]
    fn a_dump_writes_into_a_dir_it_makes_or_an_empty_one_and_takes_two_dirs() {
        let dataset = two_of_each_and_a_building();
        let want = logical_files(&dataset);
        let dir = ScratchDir::new();
        dataset.write(dir.path());
        let out = ScratchDir::new();
        // A dir that is not there yet, under one that is not either.
        let deep = out.path().join("day-b").join("planet-full");
        let dump = dump_into(dir.path(), &deep);
        assert_eq!((dump.status, dump.errors.as_str()), (0, ""));
        assert_dumped(&dump, &want, &[]);
        // A dir that holds a file, which is left as it is, beside nothing else.
        let again = dump_into(dir.path(), &deep);
        assert_eq!((again.status, again.report.as_str()), (2, ""));
        assert_eq!(again.errors, format!("dump_logical: {} is not empty\n", deep.display()));
        assert_dumped(&again, &want, &[]);
        let held = out.path().join("held");
        std::fs::create_dir(&held).unwrap();
        std::fs::write(held.join("notes"), b"kept").unwrap();
        let dump = dump_into(dir.path(), &held);
        assert_eq!((dump.status, dump.files), (2, BTreeMap::from([("notes".to_owned(), b"kept".to_vec())])));

        // Anything but the two dirs is told how to call it.
        let usage = "usage: dump_logical <build-dir> <out-dir>\n";
        let arg = |path: &std::path::Path| path.to_str().unwrap().to_owned();
        let fresh = out.path().join("fresh");
        for args in [vec![], vec![arg(dir.path())], vec![arg(dir.path()), arg(&fresh), arg(&fresh)]] {
            let (mut report, mut errors) = (Vec::new(), Vec::new());
            assert_eq!(crate::dump_logical::run(&args, &mut report, &mut errors), 2);
            assert_eq!((String::from_utf8(report).unwrap().as_str(), String::from_utf8(errors).unwrap().as_str()), ("", usage));
        }
        assert!(!fresh.exists());
    }

    #[test]
    fn a_dump_writes_no_file_of_a_family_the_dir_is_without_and_empty_ones_of_a_family_without_records() {
        // A dir of boundaries alone: no family, no file.
        let admin = TestIndex::new();
        let dump = dump_after(&admin, |_| ());
        assert_eq!((dump.status, dump.errors.as_str(), dump.files.len()), (0, "", 0));
        assert_eq!(dump.report, dump_report([None, None, None, None, None, None, None, None, None]));
        assert!(dump.report.starts_with("street_ways.bin: none\ninterp_ways.bin: none\naddr_points.bin: none\nway_parents.bin: none\n"));

        // Streets alone, one with a postcode.
        let mut streets = TestIndex::new();
        streets.street("Main Street", &parallel(0.0, 200.0)).way_postcode("2020").street("High Street", &parallel(1000.0, 200.0));
        let want = logical_files(&streets);
        assert_eq!(want.keys().collect::<Vec<_>>(), ["street_nodes.bin", "street_ways.bin", "way_postcodes.bin"]);
        let dump = dump_after(&streets, |_| ());
        assert_eq!((dump.status, dump.errors.as_str(), dump.report.as_str()), (0, "", dump_report(whole_dump_report(&streets)).as_str()));
        assert_dumped(&dump, &want, &[]);

        // The same beside interpolation and address files of no records, the footers a full dir
        // holds without them: a family of no records has its files, of no bytes.
        let dump = dump_after(&streets, |dir| {
            let empty = way_pair(&[], way_files::Kind::Interps).into_iter().chain(addr_triple(&[]));
            for (name, bytes) in empty {
                std::fs::write(dir.join(name), bytes).unwrap();
            }
        });
        let mut families = whole_dump_report(&streets);
        (families[1], families[2]) = (Some(("0 ways, 0 nodes".to_owned(), 0)), Some(("0 records, 0 footprints".to_owned(), 0)));
        assert_eq!((dump.status, dump.errors.as_str(), dump.report.as_str()), (0, "", dump_report(families).as_str()));
        let mut want = want;
        want.extend(["interp_ways.bin", "interp_nodes.bin", "addr_points.bin", "addr_vertices.bin"].map(|name| (name.to_owned(), Vec::new())));
        assert_dumped(&dump, &want, &[]);
    }
}

#[cfg(test)]
mod test_rng {
    /// xorshift64*: deterministic test values without a dependency.
    pub(crate) struct Rng(u64);
    impl Rng {
        pub(crate) fn new(seed: u64) -> Self { Rng(seed.max(1)) }
        pub(crate) fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        /// Uniform in [lo, hi).
        pub(crate) fn range(&mut self, lo: f64, hi: f64) -> f64 {
            lo + (self.next() >> 11) as f64 / (1u64 << 53) as f64 * (hi - lo)
        }
    }
}

#[cfg(test)]
mod admin_scan_tests {
    use super::*;

    // A query point on Egypt's side of its 22°N border with Sudan. An S2 covering of the great
    // circle between the border's end points, which bows 3.3 km north of the parallel, leaves Egypt
    // out of the point's own level-10 cell; the builder covers the straight edge instead.
    const Q: (f64, f64) = (22.002, 27.57);

    struct Ring {
        name: &'static str,
        level: u8,
        country: &'static [u8; 2],
        place_type: u8,
        area: f32,
        verts: Vec<(f64, f64)>,
        /// (index into `cells()`, interior flag) of every cell the ring is listed in.
        cells: Vec<(usize, bool)>,
    }

    fn ring(name: &'static str, level: u8, area: f32, (lat0, lng0, lat1, lng1): (f64, f64, f64, f64), cells: &[(usize, bool)]) -> Ring {
        let verts = vec![(lat0, lng0), (lat0, lng1), (lat1, lng1), (lat1, lng0)];
        Ring { name, level, country: b"eg", place_type: 0, area, verts, cells: cells.to_vec() }
    }

    /// The query's cell, then its neighbours in the order find_admin visits them.
    fn cells() -> Vec<u64> {
        let own = cell_id_at_level(Q.0, Q.1, DEFAULT_ADMIN_CELL_LEVEL);
        std::iter::once(own).chain(cell_neighbors_at_level(own, DEFAULT_ADMIN_CELL_LEVEL)).collect()
    }

    /// An Index holding only the admin files, laid out as the builder writes them.
    fn admin_index(rings: &[Ring]) -> Index {
        admin_index_in(rings, &cells())
    }

    /// `admin_index` with each ring's cells indices into `cell_ids`.
    fn admin_index_in(rings: &[Ring], cell_ids: &[u64]) -> Index {
        let mut strings = vec![0u8];
        let mut arcs = arc_fixture::ArcFixture::new();
        let mut by_cell: std::collections::BTreeMap<u64, Vec<u32>> = Default::default();
        for (id, r) in rings.iter().enumerate() {
            let name_id = strings.len() as u32;
            strings.extend_from_slice(r.name.as_bytes());
            strings.push(0);
            let record = AdminPolygon {
                place_type_override: r.place_type, country_code: u16::from_be_bytes(*r.country),
                ..arc_fixture::record(name_id, r.level, r.area)
            };
            if r.verts.is_empty() {
                arcs.polygon(record, &[]);
            } else {
                let ring: Vec<PointE7> = r.verts.iter().chain(&r.verts[..1]).map(|&(lat, lng)| arc_fixture::e7(lat, lng)).collect();
                arcs.ring_polygon(record, 0, &[ring]);
            }
            for &(k, interior) in &r.cells {
                by_cell.entry(cell_ids[k]).or_default().push(id as u32 | if interior { INTERIOR_FLAG } else { 0 });
            }
        }
        let [(_, polygons), (_, ring_blocks), (_, arc_index), (_, arc_vertices)] = arcs.files();
        // The builder sorts each cell's entries by value (cell_pairs::write does).
        let (cells, entries) = cell_pairs::write(&by_cell.into_iter().collect::<Vec<_>>(), cell_pairs::ADMIN);
        let len = strings.len() as u32;
        Index {
            geo_index: GeoIndex::none(), streets: None,
            addrs: None,
            interps: None, interp_postcodes: None,
            admin_index: CellPairs::open(FileBytes::Owned(cells), FileBytes::Owned(entries), cell_pairs::ADMIN, DEFAULT_ADMIN_CELL_LEVEL).unwrap(),
            admin_polygons: FileBytes::Owned(polygons),
            admin_geometry: AdminGeometry::new(FileBytes::Owned(ring_blocks), FileBytes::Owned(arc_index), FileBytes::Owned(arc_vertices)),
            poi_index: None, poi_records: None, poi_vertices: None,
            poi_meta: PoiMeta { categories: HashMap::new() },
            places: None,
            way_parents: None, admin_parents: None, addr_postcodes: None, addr_cities: None, way_postcodes: None,
            postcode_centroids: None,
            postcode_grid: None,
            admin_config: AdminLevelConfig::load(),
            strings: StringPool::from_parts([Some(strings::Tier::of_pool(&strings)), None, None, None, None], [0, len, len, len, len, len]),
            water: None,
            street_cell_level: DEFAULT_STREET_CELL_LEVEL,
            admin_cell_level: DEFAULT_ADMIN_CELL_LEVEL,
            max_distance_sq: 1.0,
            linked_names: Default::default(),
            credits: Vec::new(),
        }
    }

    // Egypt north of 22°N; Sudan south of it.
    const EGYPT: (f64, f64, f64, f64) = (22.0, 25.0, 31.6, 36.9);
    const SUDAN: (f64, f64, f64, f64) = (8.7, 21.8, 22.0, 38.6);

    #[test]
    fn find_admin_reads_only_the_points_own_cell() {
        // Rings listed only in neighbour cells: the builder lists a ring in the cell of every point
        // it contains, so these contain no point of Q's cell. Sudan's edge on 22°N lies within
        // reach of Q's cell, which lists it, not interior.
        let idx = admin_index(&[
            Ring { country: b"sd", ..ring("Sudan", 2, 190.0, SUDAN, &[(0, false), (3, true)]) },
            ring("Egypt", 2, 97.0, EGYPT, &[(3, false), (5, false)]),
            ring("New Valley", 4, 38.8, (22.0, 25.0, 27.7, 32.7), &[(5, false)]),
        ]);
        let a = idx.find_admin(Q.0, Q.1);
        assert_eq!((a.country, a.state), (None, None));
        let idx = admin_index(&[
            Ring { country: b"sd", ..ring("Sudan", 2, 190.0, SUDAN, &[(0, false), (3, true)]) },
            ring("Egypt", 2, 97.0, EGYPT, &[(0, false), (5, false)]),
            ring("New Valley", 4, 38.8, (22.0, 25.0, 27.7, 32.7), &[(0, false)]),
        ]);
        let a = idx.find_admin(Q.0, Q.1);
        assert_eq!((a.country.as_deref(), a.state.as_deref()), (Some("Egypt"), Some("New Valley")));
    }

    #[test]
    fn a_longitude_whole_turns_away_is_in_no_ring() {
        // S2 would wrap the longitude back into Q's cell, but the point is rounded to the e7 grid
        // first, 9.7e11 degrees east of every ring (past i64 in e7 units): beyond them all.
        let far = Q.1 + 360.0 * 2.7e9;
        assert_eq!(cell_id_at_level(Q.0, far, DEFAULT_ADMIN_CELL_LEVEL), cell_id_at_level(Q.0, Q.1, DEFAULT_ADMIN_CELL_LEVEL));
        let idx = admin_index(&[
            ring("Egypt", 2, 97.0, EGYPT, &[(0, false)]),
            ring("New Valley", 4, 38.8, (22.0, 25.0, 27.7, 32.7), &[(0, false)]),
        ]);
        assert_eq!(idx.find_admin(Q.0, Q.1).country.as_deref(), Some("Egypt"));
        let a = idx.find_admin(Q.0, far);
        assert_eq!((a.country, a.state), (None, None));
        let egypt = idx.admin_polygon(0).unwrap();
        assert!(!idx.admin_geometry.contains_once(&egypt, AdminPoint::new(Q.0, far)));
    }

    #[test]
    fn a_point_off_the_globe_is_in_no_ring_though_its_wrapped_cell_lists_them_interior() {
        // Each point is Q once S2 wraps it onto the globe, so it would look up Q's cell, where every
        // ring is listed interior: an interior entry holds the points of its cell, not this one.
        let idx = admin_index(&[
            ring("Egypt", 2, 97.0, EGYPT, &[(0, true)]),
            ring("New Valley", 4, 38.8, (22.0, 25.0, 27.7, 32.7), &[(0, true)]),
            ring("Paris", 8, 2.0, (21.9, 27.4, 22.1, 27.7), &[(0, true)]),
            Ring { place_type: 5, ..ring("Bir Tarfawi", 15, 0.01, (21.99, 27.5, 22.05, 27.6), &[(0, true)]) },
        ]);
        let a = idx.find_admin(Q.0, Q.1);
        assert_eq!((a.country.as_deref(), a.state.as_deref(), a.neighbourhood.as_deref()), (Some("Egypt"), Some("New Valley"), Some("Bir Tarfawi")));
        for (lat, lng) in [(Q.0, Q.1 + 360.0), (Q.0, Q.1 - 360.0), (180.0 - Q.0, Q.1 - 180.0), (-180.0 - Q.0, Q.1 + 180.0)] {
            let (rlat, rlng) = AdminPoint::new(lat, lng).degrees();
            assert_eq!(cell_id_at_level(rlat, rlng, DEFAULT_ADMIN_CELL_LEVEL), cells()[0], "({lat}, {lng})");
            let a = idx.find_admin(lat, lng);
            assert_eq!((a.country, a.state, a.neighbourhood), (None, None, None), "({lat}, {lng})");
            let mut scan = AdminScan::new(&idx, lat, lng);
            assert!(scan.rings.is_empty(), "({lat}, {lng})");
            let gates = idx.find_current_boundaries(&mut scan, Some(*b"eg"));
            assert!(gates.gates.iter().all(Option::is_none) && gates.promoting.is_empty(), "({lat}, {lng})");
            assert_eq!(idx.find_admin_from_chain(0, lat, lng).neighbourhood, None, "({lat}, {lng})");
        }
        for (lat, lng) in [(f64::NAN, Q.1), (Q.0, f64::NAN), (Q.0, f64::INFINITY), (f64::NEG_INFINITY, Q.1)] {
            assert!(AdminScan::new(&idx, lat, lng).rings.is_empty(), "({lat}, {lng})");
            assert_eq!(idx.find_admin(lat, lng).country, None, "({lat}, {lng})");
        }
    }

    #[test]
    fn the_globes_edges_are_on_it_and_a_hair_past_them_is_not() {
        // A ring listed interior both in the cell of a point on the edge of the globe and in the
        // cell S2 wraps a point a hair past it onto.
        for ((lat, lng), past, around) in [
            ((90.0, 45.0), (90.0000001, 45.0), (89.0, 44.0, 90.0, 46.0)),
            ((-90.0, 45.0), (-90.0000001, 45.0), (-90.0, 44.0, -89.0, 46.0)),
            ((10.0, 180.0), (10.0, 180.0000001), (9.0, 179.0, 11.0, 180.0)),
            ((10.0, -180.0), (10.0, -180.0000001), (9.0, -180.0, 11.0, -179.0)),
        ] {
            let cell = |(lat, lng): (f64, f64)| {
                let (lat, lng) = AdminPoint::new(lat, lng).degrees();
                cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL)
            };
            let idx = admin_index_in(&[ring("Edge", 2, 1.0, around, &[(0, true), (1, true)])], &[cell((lat, lng)), cell(past)]);
            assert_eq!(idx.find_admin(lat, lng).country.as_deref(), Some("Edge"), "({lat}, {lng})");
            assert_eq!(idx.find_admin(past.0, past.1).country, None, "{past:?}");
        }
    }

    #[test]
    fn a_neighbour_cells_interior_flag_does_not_prove_containment() {
        let idx = admin_index(&[
            ring("Egypt", 2, 97.0, EGYPT, &[(0, false)]),
            Ring { country: b"sd", ..ring("Sudan", 2, 9.0, SUDAN, &[(1, true), (2, true)]) },
        ]);
        assert_eq!(idx.find_admin(Q.0, Q.1).country.as_deref(), Some("Egypt"));
    }

    #[test]
    fn an_interior_entry_holds_the_point_without_reading_an_arc() {
        // The builder flags an entry interior only where the rings hold the whole cell, so the scan
        // takes it as holding the point: find_admin, the current boundaries and the chain lookup
        // read no arc of an interior entry, and test only the others.
        let mut idx = admin_index(&[
            ring("Egypt", 2, 97.0, EGYPT, &[(0, true)]),
            ring("New Valley", 4, 38.8, (22.0, 25.0, 27.7, 32.7), &[(0, true)]),
            ring("Paris", 8, 2.0, (21.9, 27.4, 22.1, 27.7), &[(0, true)]),
            Ring { place_type: 5, ..ring("Bir Tarfawi", 15, 0.01, (21.99, 27.5, 22.05, 27.6), &[(0, true)]) },
            // A smaller state holding the point, which Paris's chain doesn't take.
            ring("Kharga", 4, 20.0, (22.0, 27.0, 26.0, 31.0), &[(0, true)]),
        ]);
        // Each polygon's next larger one: Paris in New Valley in Egypt.
        let parents = [NO_DATA, 0, 1, NO_DATA, 0];
        idx.admin_parents = Some(FileBytes::Owned(parents.iter().flat_map(|p| p.to_le_bytes()).collect()));
        let mut scan = AdminScan::new(&idx, Q.0, Q.1);
        let a = idx.find_admin_in(&mut scan);
        assert_eq!((a.country.as_deref(), a.state.as_deref(), a.city.as_deref(), a.neighbourhood.as_deref()), (Some("Egypt"), Some("Kharga"), Some("Paris"), Some("Bir Tarfawi")));
        let gates = idx.find_current_boundaries(&mut scan, Some(*b"eg"));
        assert_eq!(gates.for_rank(CurrentBoundaries::CITY).map(|p| idx.get_string(p.name_id)).as_deref(), Some("Paris"));
        assert_eq!(scan.pip.memo.arcs_read(), 0);
        // The chain lookup from Paris over a scan of its own: its boundaries are the chain's (New
        // Valley, not the scan's Kharga, so this is no fallback to find_admin), its place area the
        // scan's interior entry.
        let mut scan = AdminScan::new(&idx, Q.0, Q.1);
        let c = idx.find_admin_from_chain_in(2, &mut scan);
        assert_eq!((c.country.as_deref(), c.state.as_deref(), c.city.as_deref(), c.neighbourhood.as_deref()), (Some("Egypt"), Some("New Valley"), Some("Paris"), Some("Bir Tarfawi")));
        assert_eq!(scan.pip.memo.arcs_read(), 0);
        // The same rings listed not interior are tested.
        let idx = admin_index(&[
            ring("Egypt", 2, 97.0, EGYPT, &[(0, false)]),
            ring("New Valley", 4, 38.8, (22.0, 25.0, 27.7, 32.7), &[(0, false)]),
        ]);
        let mut scan = AdminScan::new(&idx, Q.0, Q.1);
        assert_eq!(idx.find_admin_in(&mut scan).state.as_deref(), Some("New Valley"));
        assert!(scan.pip.memo.arcs_read() > 0);
    }

    #[test]
    fn a_place_area_fills_its_field() {
        let idx = admin_index(&[
            ring("Egypt", 2, 97.0, EGYPT, &[(0, true)]),
            Ring { place_type: 4, ..ring("Bir Tarfawi", 15, 0.01, (21.99, 27.5, 22.05, 27.6), &[(0, false), (7, false)]) },
        ]);
        assert_eq!(idx.find_admin(Q.0, Q.1).suburb.as_deref(), Some("Bir Tarfawi"));
    }

    #[test]
    fn equal_area_rings_resolve_in_entry_order() {
        let around_q = (21.99, 27.5, 22.05, 27.6);
        // By polygon id.
        let idx = admin_index(&[
            ring("Egypt", 2, 97.0, EGYPT, &[(0, true)]),
            ring("First", 4, 1.0, around_q, &[(0, false)]),
            ring("Second", 4, 1.0, around_q, &[(0, false)]),
        ]);
        assert_eq!(idx.find_admin(Q.0, Q.1).state.as_deref(), Some("First"));
        // The interior flag is the entry's high bit.
        let idx = admin_index(&[
            ring("Egypt", 2, 97.0, EGYPT, &[(0, true)]),
            ring("First, interior", 4, 1.0, around_q, &[(0, true)]),
            ring("Second", 4, 1.0, around_q, &[(0, false)]),
        ]);
        assert_eq!(idx.find_admin(Q.0, Q.1).state.as_deref(), Some("Second"));
    }

    #[test]
    fn a_countrys_larger_polygons_are_not_read_once_a_smaller_one_holds_the_point() {
        let around_q = (21.99, 27.5, 22.05, 27.6);
        let wider = (21.9, 27.4, 22.1, 27.7);
        // The country and the state found, and the arcs read.
        let arcs_read = |rings: &[Ring]| {
            let idx = admin_index(rings);
            let mut scan = AdminScan::new(&idx, Q.0, Q.1);
            let found = idx.find_admin_in(&mut scan);
            ((found.country.map(Cow::into_owned), found.state.map(Cow::into_owned)), scan.pip.memo.arcs_read())
        };
        // Listed first, so visited first in a walk down the list.
        let (found, read) = arcs_read(&[
            ring("Large", 2, 5.0, wider, &[(0, false)]),
            ring("Small", 2, 1.0, around_q, &[(0, false)]),
        ]);
        assert_eq!(found.0.as_deref(), Some("Small"));
        assert_eq!((found, read), arcs_read(&[ring("Small", 2, 1.0, around_q, &[(0, false)])]));
        // A smaller one that misses the point leaves the larger one to be read.
        let elsewhere = (30.0, 27.5, 30.05, 27.6);
        let (found, read) = arcs_read(&[
            ring("Large", 2, 5.0, wider, &[(0, false)]),
            ring("Small", 2, 1.0, elsewhere, &[(0, false)]),
        ]);
        assert_eq!(found.0.as_deref(), Some("Large"));
        assert!(read > 0);
        // A larger state is read, as one round the smaller one is in the address too; the
        // smaller one names the state.
        let (found, read) = arcs_read(&[
            ring("Large", 4, 5.0, wider, &[(0, false)]),
            ring("Small", 4, 1.0, around_q, &[(0, false)]),
        ]);
        assert_eq!(found.1.as_deref(), Some("Small"));
        assert!(read > arcs_read(&[ring("Small", 4, 1.0, around_q, &[(0, false)])]).1);
    }

    #[test]
    fn a_levels_best_is_the_smallest_holding_the_point_the_first_listed_among_equals() {
        // Every list of the point's boundaries, whatever order it visits them in: what a walk down
        // the list keeping the smaller one finds is the answer.
        const NAMES: [&str; 6] = ["A", "B", "C", "D", "E", "F"];
        const LEVELS: [u8; 3] = [4, 6, 8];
        let holds_q = (21.99, 27.5, 22.05, 27.6);
        let misses_q = (30.0, 27.5, 30.05, 27.6);
        let mut rng = crate::test_rng::Rng::new(20261107);
        for case in 0..300 {
            let rings: Vec<Ring> = (0..NAMES.len()).map(|i| {
                let level = LEVELS[(rng.next() % 3) as usize];
                let area = [0.5f32, 0.5, 1.0, 2.0, 3.0][(rng.next() % 5) as usize];
                let (holds, interior) = match rng.next() % 4 { 0 => (false, false), 1 => (true, true), _ => (true, false) };
                ring(NAMES[i], level, area, if holds { holds_q } else { misses_q }, &[(0, interior)])
            }).collect();
            let want = |level: u8| {
                // The builder sorts each cell's entries by value: the interior flag's high bit
                // puts those last.
                let mut listed: Vec<&Ring> = rings.iter().collect();
                listed.sort_by_key(|r| r.cells[0].1);
                let mut best: Option<&Ring> = None;
                for r in listed {
                    if r.level == level && r.verts[0].0 == holds_q.0 && best.is_none_or(|b| r.area < b.area) {
                        best = Some(r);
                    }
                }
                best.map(|r| r.name)
            };
            let idx = admin_index(&rings);
            let a = idx.find_admin(Q.0, Q.1);
            assert_eq!(a.state.as_deref(), want(4), "case {case} state");
            assert_eq!(a.county.as_deref(), want(6), "case {case} county");
            assert_eq!(a.city.as_deref(), want(8), "case {case} city");
        }
    }

    #[test]
    fn smaller_rings_that_miss_the_point_do_not_displace_the_containing_one() {
        let idx = admin_index(&[
            ring("Egypt", 2, 97.0, EGYPT, &[(0, true)]),
            ring("New Valley", 4, 38.8, (22.0, 25.0, 27.7, 32.7), &[(0, true), (1, true)]),
            ring("Aswan", 4, 6.0, (22.0, 31.0, 24.5, 33.6), &[(1, false), (0, false)]),
            ring("Red Sea", 4, 12.0, (22.0, 33.6, 28.0, 36.9), &[(2, false)]),
        ]);
        assert_eq!(idx.find_admin(Q.0, Q.1).state.as_deref(), Some("New Valley"));
    }

    #[test]
    fn the_smallest_containing_boundaries_gate_place_nodes() {
        let idx = admin_index(&[
            ring("Egypt", 2, 97.0, EGYPT, &[(0, true)]),
            ring("Paris", 8, 2.0, (21.9, 27.4, 22.1, 27.7), &[(0, false)]),
            ring("Balat", 8, 0.5, (21.9, 27.4, 21.95, 27.7), &[(0, false)]),
            ring("Mut", 10, 0.3, (21.99, 27.5, 22.05, 27.6), &[(0, false)]),
        ]);
        let gates = idx.find_current_boundaries(&mut AdminScan::new(&idx, Q.0, Q.1), Some(*b"eg"));
        let name = |rank| gates.for_rank(rank).map(|p| idx.get_string(p.name_id));
        assert_eq!(name(CurrentBoundaries::CITY).as_deref(), Some("Paris"));
        assert_eq!(name(CurrentBoundaries::SUBURB).as_deref(), Some("Mut"));
    }

    #[test]
    fn one_scan_serves_every_admin_lookup_of_the_point() {
        let idx = admin_index(&[
            ring("Egypt", 2, 97.0, EGYPT, &[(0, false)]),
            ring("New Valley", 4, 38.8, (22.0, 25.0, 27.7, 32.7), &[(0, false), (5, false)]),
            ring("Paris", 8, 2.0, (21.9, 27.4, 22.1, 27.7), &[(0, false)]),
            ring("Mut", 10, 0.3, (21.99, 27.5, 22.05, 27.6), &[(0, false)]),
            Ring { place_type: 5, ..ring("Bir Tarfawi", 15, 0.01, (21.99, 27.5, 22.05, 27.6), &[(0, false)]) },
        ]);
        let mut scan = AdminScan::new(&idx, Q.0, Q.1);
        let gates = idx.find_current_boundaries(&mut scan, Some(*b"eg"));
        let name = |rank| gates.for_rank(rank).map(|p| idx.get_string(p.name_id));
        assert_eq!(name(CurrentBoundaries::CITY).as_deref(), Some("Paris"));
        assert_eq!(name(CurrentBoundaries::SUBURB).as_deref(), Some("Mut"));
        let a = idx.find_admin_in(&mut scan);
        assert_eq!((a.country.as_deref(), a.state.as_deref(), a.neighbourhood.as_deref()), (Some("Egypt"), Some("New Valley"), Some("Bir Tarfawi")));
        // No admin_parents: the chain lookup falls back to find_admin over the same scan.
        let c = idx.find_admin_from_chain_in(0, &mut scan);
        assert_eq!((c.country.as_deref(), c.state.as_deref(), c.neighbourhood.as_deref()), (Some("Egypt"), Some("New Valley"), Some("Bir Tarfawi")));
    }

    #[test]
    fn admin_scan_reads_the_own_cells_entries_in_value_order() {
        let new_valley = (22.0, 25.0, 27.7, 32.7);
        let scan = AdminScan::new(&admin_index(&[ring("Egypt", 2, 97.0, EGYPT, &[(0, true)])]), Q.0, Q.1);
        assert_eq!(scan.rings.len(), 1);
        // The own cell absent from the index.
        let scan = AdminScan::new(&admin_index(&[ring("Egypt", 2, 97.0, EGYPT, &[(3, true), (5, false)])]), Q.0, Q.1);
        assert!(scan.rings.is_empty());
        let idx = admin_index(&[
            ring("Egypt", 2, 97.0, EGYPT, &[(5, false), (0, true)]),
            ring("New Valley", 4, 38.8, new_valley, &[(8, false), (0, false)]),
        ]);
        let scan = AdminScan::new(&idx, Q.0, Q.1);
        let names: Vec<_> = scan.rings.iter().map(|(_, p)| idx.get_string(p.name_id)).collect();
        // The interior flag is the high bit.
        assert_eq!(names, ["New Valley", "Egypt"]);
    }

    #[test]
    fn prefetch_reads_nothing_and_accepts_any_offset() {
        for src in [FileBytes::Owned(Vec::new()), FileBytes::Owned(vec![1, 2, 3])] {
            for off in [0, 2, 3, u64::MAX] { src.prefetch(off); }
        }
        let src = FileBytes::Owned(vec![1, 2, 3]);
        src.prefetch(1);
        assert_eq!(&src.read_chunk(0, 3)[..], &[1, 2, 3]);
    }

    /// A ring a tier simplified away: its record keeps its name, its ring block is empty.
    fn ringless(name: &'static str, level: u8, cells: &[(usize, bool)]) -> Ring {
        Ring { verts: Vec::new(), ..ring(name, level, 1.0, EGYPT, cells) }
    }

    /// `idx` with one polygon POI, the box `around` named as admin polygon `named_after` is, its
    /// own parent street, listed in Q's cell (interior or not).
    fn with_polygon_poi(mut idx: Index, named_after: u32, (lat0, lng0, lat1, lng1): (f64, f64, f64, f64), interior: bool) -> Index {
        let name_id = idx.admin_polygon(named_after).unwrap().name_id;
        // The box at 1e-7 degrees (encoding 3) from its low corner.
        let q = |v: f64, from: f64| ((v - (from as f32) as f64) / 1e-7).round() as u32;
        let polygon = poi_files::PoiPolygon {
            encoding: 3, bmin_lat: lat0 as f32, bmin_lng: lng0 as f32,
            q: [(lat0, lng0), (lat0, lng1), (lat1, lng1), (lat1, lng0)].iter().map(|&(a, b)| (q(a, lat0), q(b, lng0))).collect(),
        };
        let record = PoiRecord {
            lat: ((lat0 + lat1) / 2.0) as f32, lng: ((lng0 + lng1) / 2.0) as f32, vertex_offset: 0, name_id, category: 1, tier: 0,
            flags: 0, importance: 0, parent_street_id: name_id, parent_postcode_id: NO_DATA, parent_poly_id: NO_DATA,
        };
        let (records, vertices) = poi_files::encode(&[(record, Some(polygon))]);
        let entry = if interior { INTERIOR_FLAG } else { 0 };
        idx.poi_index = Some(cell_pairs::from_pairs([(cells()[0], entry)], cell_pairs::POI));
        idx.poi_records = Some(PoiRecords::open(FileBytes::Owned(records)).unwrap());
        idx.poi_vertices = Some(FileBytes::Owned(vertices));
        idx
    }

    /// What a lookup that finds nothing answers: Nominatim's licence, and no address.
    const NOTHING_FOUND: &str = r#"{"licence":"Data © OpenStreetMap contributors, ODbL 1.0. http://osm.org/copyright","address":{}}"#;

    #[test]
    fn a_point_off_the_globe_is_in_no_poi_though_its_wrapped_cell_lists_one_interior() {
        let around_q = (21.99, 27.5, 22.05, 27.6);
        let rings = || [ring("Egypt", 2, 97.0, EGYPT, &[(0, true)]), Ring { place_type: 5, ..ring("Bir Tarfawi", 15, 0.01, around_q, &[(0, true)]) }];
        for interior in [false, true] {
            let idx = with_polygon_poi(admin_index(&rings()), 1, around_q, interior);
            let pois = |lat, lng| idx.find_pois(lat, lng).iter().map(|p| (p.name.to_string(), p.contained)).collect::<Vec<_>>();
            assert_eq!(pois(Q.0, Q.1), [("Bir Tarfawi".to_string(), true)], "interior {interior}");
            assert_eq!(idx.find_nearest_poi_with_parent(Q.0, Q.1).map(|(d, p)| (d, p.name_id)), Some((0.0, idx.admin_polygon(1).unwrap().name_id)));
            let answer = serde_json::to_string(&idx.query(Q.0, Q.1)).unwrap();
            assert!(answer.contains("Egypt") && answer.contains("Bir Tarfawi"), "{answer}");
            for (lat, lng) in [(Q.0, Q.1 + 360.0), (Q.0, Q.1 - 360.0), (180.0 - Q.0, Q.1 - 180.0), (-180.0 - Q.0, Q.1 + 180.0)] {
                assert_eq!(cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL), cells()[0], "({lat}, {lng})");
                assert_eq!(pois(lat, lng), [], "({lat}, {lng}) interior {interior}");
                assert!(idx.find_nearest_poi_with_parent(lat, lng).is_none(), "({lat}, {lng}) interior {interior}");
                assert_eq!(serde_json::to_string(&idx.query(lat, lng)).unwrap(), NOTHING_FOUND, "({lat}, {lng}) interior {interior}");
            }
            for (lat, lng) in [(f64::NAN, Q.1), (Q.0, f64::NAN), (Q.0, f64::INFINITY), (f64::NEG_INFINITY, Q.1)] {
                assert_eq!(pois(lat, lng), [], "({lat}, {lng})");
                assert!(idx.find_nearest_poi_with_parent(lat, lng).is_none(), "({lat}, {lng})");
            }
        }
    }

    #[test]
    fn a_poi_whose_parent_has_no_rings_passes_the_chain_check() {
        let idx = admin_index(&[ring("Sudan", 2, 97.0, SUDAN, &[]), ringless("Ringless", 8, &[])]);
        let poi = |parent_poly_id: u32| PoiRecord {
            lat: 0.0, lng: 0.0, vertex_offset: NO_DATA, name_id: 0, category: 0, tier: 0, flags: 0,
            importance: 0, parent_street_id: NO_DATA, parent_postcode_id: NO_DATA, parent_poly_id,
        };

        assert!(!idx.poi_chain_contains_query(&poi(0), Q.0, Q.1));
        assert!(idx.poi_chain_contains_query(&poi(1), Q.0, Q.1));
    }

    #[test]
    fn a_nameless_poi_record_is_a_tombstone_point_or_polygon() {
        let mut idx = admin_index(&[]);
        let poi = |name_id: u32, polygon: bool| {
            let record = PoiRecord {
                lat: 0.0, lng: 0.0, vertex_offset: 0, name_id, category: 0, tier: 3, flags: 0, importance: 1,
                parent_street_id: 0, parent_postcode_id: NO_DATA, parent_poly_id: NO_DATA,
            };
            let square = poi_files::PoiPolygon { encoding: 0, bmin_lat: 0.0, bmin_lng: 0.0, q: vec![(0, 0), (0, 1), (1, 1), (1, 0)] };
            (record, polygon.then_some(square))
        };
        let (records, vertices) = poi_files::encode(&[poi(0, false), poi(0, true), poi(NO_DATA, false), poi(NO_DATA, true)]);
        idx.poi_records = Some(PoiRecords::open(FileBytes::Owned(records)).unwrap());
        idx.poi_vertices = Some(FileBytes::Owned(vertices));

        let live: Vec<bool> = (0..4).map(|i| idx.poi_record(i).is_some()).collect();

        assert_eq!(live, [true, true, false, false]);
    }

    #[test]
    fn debug_admin_leaves_out_polygons_without_rings() {
        let idx = admin_index(&[ring("Egypt", 2, 97.0, EGYPT, &[(0, false)]), ringless("Ringless", 8, &[(0, false)])]);

        let seen = &idx.debug_admin(Q.0, Q.1)["seen_per_level"];

        assert_eq!(seen[2][0]["name"], "Egypt");
        assert_eq!(seen[2][0]["pip"], true);
        assert_eq!(seen[8], serde_json::json!([]));
    }
}
