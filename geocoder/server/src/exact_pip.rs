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

/// Whether the edge a-b, a straight line in (lat, lng), crosses the ray from `p` towards +lat: it
/// straddles p's longitude (one end east of it, the other at or west of it) and passes strictly
/// north of p there. A point on the edge crosses nothing. Either direction of the edge gives the
/// same answer.
///
/// On the globe each product is at most 1.8e9 * 3.6e9, within i64; they are taken in i128 so
/// that no input, however far off the globe, can overflow them.
#[inline]
pub fn edge_crosses(a: PointE7, b: PointE7, p: PointE7) -> bool {
    if (a.lng > p.lng) == (b.lng > p.lng) {
        return false;
    }
    let lhs = p.lat.wrapping_sub(a.lat) as i128 * b.lng.wrapping_sub(a.lng) as i128;
    let rhs = b.lat.wrapping_sub(a.lat) as i128 * p.lng.wrapping_sub(a.lng) as i128;
    lhs != rhs && ((lhs < rhs) != (b.lng < a.lng))
}

/// Whether `p` lies on the edge a-b, its ends included.
#[inline]
pub fn on_edge(a: PointE7, b: PointE7, p: PointE7) -> bool {
    (a.lat.min(b.lat)..=a.lat.max(b.lat)).contains(&p.lat)
        && (a.lng.min(b.lng)..=a.lng.max(b.lng)).contains(&p.lng)
        && b.lat.wrapping_sub(a.lat) as i128 * p.lng.wrapping_sub(a.lng) as i128
            == p.lat.wrapping_sub(a.lat) as i128 * b.lng.wrapping_sub(a.lng) as i128
}

#[cfg(test)]
pub(crate) mod tests;
