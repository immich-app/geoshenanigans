//! Point-in-polygon on OSM's own e7 grid, in integers: the one rounding of a coordinate to that
//! grid (`to_e7`) and the one ray cast (`edge_crosses`) every admin answer takes, the same as the
//! builder's links (builder/src/exact_pip.h). Neighbouring polygons share their edges vertex for
//! vertex, and a point on a shared edge crosses nothing there, so it is inside exactly one of
//! them. geocoder/testdata/pip_edges.tsv and point_e7.tsv hold the cases both must agree on.

/// A coordinate pair in 1e-7 degree units.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct PointE7 {
    pub lat: i64,
    pub lng: i64,
}

impl PointE7 {
    /// (lat, lng) in degrees, as the builder reads a PackedLocation: e7 / 1e7.
    pub fn degrees(self) -> (f64, f64) {
        (self.lat as f64 / 1e7, self.lng as f64 / 1e7)
    }
}

/// A coordinate in degrees on the e7 grid, half away from zero: OSM's own e7 value for a
/// coordinate read from it, and the builder's std::llround of the same f64 product. A record's
/// f32 coordinate is widened to f64 first.
pub fn to_e7(deg: f64) -> i64 {
    (deg * 1e7).round() as i64
}

/// `x - y`, exact for any two i64: each is widened to i128 first.
#[inline(always)]
fn diff(x: i64, y: i64) -> i128 {
    x as i128 - y as i128
}

/// Whether the edge a-b, a straight line in (lat, lng), crosses the ray from `p` towards +lat: it
/// straddles p's longitude (one end east of it, the other at or west of it) and passes strictly
/// north of p there. A point on the edge crosses nothing. Either direction of the edge gives the
/// same answer.
///
/// The edge's ends are within i32, as every vertex the arc decoder gives is (an arc block's vertex
/// past i32 e7 makes it malformed); `p` is any point, however far off the globe. Each difference is
/// taken in i128: one with p's coordinate takes at most 65 bits and one between the ends at most
/// 33, so each product fits in 98.
#[inline]
pub fn edge_crosses(a: PointE7, b: PointE7, p: PointE7) -> bool {
    if (a.lng > p.lng) == (b.lng > p.lng) {
        return false;
    }
    let lhs = diff(p.lat, a.lat) * diff(b.lng, a.lng);
    let rhs = diff(b.lat, a.lat) * diff(p.lng, a.lng);
    lhs != rhs && ((lhs < rhs) != (b.lng < a.lng))
}

/// Whether `p` lies on the edge a-b, its ends included; the ends within i32, as for `edge_crosses`.
#[inline]
pub fn on_edge(a: PointE7, b: PointE7, p: PointE7) -> bool {
    (a.lat.min(b.lat)..=a.lat.max(b.lat)).contains(&p.lat)
        && (a.lng.min(b.lng)..=a.lng.max(b.lng)).contains(&p.lng)
        && diff(b.lat, a.lat) * diff(p.lng, a.lng) == diff(p.lat, a.lat) * diff(b.lng, a.lng)
}

#[cfg(test)]
pub(crate) mod tests;
