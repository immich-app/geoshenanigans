//! Nominatim's expand_by_meters (lib-sql/functions/utils.sql), which makes the box a postcode
//! centroid covers: `ST_Envelope(ST_Buffer(geom::geography, meters, 1)::geometry)`. PostGIS buffers
//! a geography in the planar projection _ST_BestSRID picks for it (for a point, Lambert azimuthal
//! equal-area about the pole above 70° N or below 70° S, else the UTM zone of its longitude), there
//! a diamond: a quarter circle in one segment, the vertices `meters` east, south, west and north of
//! the point. The envelope is the lon/lat box of the four vertices taken back. The projections are
//! PROJ's on WGS 84: the transverse Mercator as Krüger's series to the sixth order in n (Karney
//! 2011), as PROJ's exact algorithm, and the polar Lambert azimuthal equal-area of laea.cpp.

/// WGS 84's semi-major axis in metres and flattening.
const A: f64 = 6_378_137.0;
const F: f64 = 1.0 / 298.257_223_563;

/// UTM's scale on its central meridian.
const UTM_K0: f64 = 0.9996;

/// The latitude beyond which _ST_BestSRID takes the polar projection.
const POLAR_LAT: f64 = 70.0;

/// The longitude of origin of the north polar projection (PostGIS SRID_NORTH_LAMBERT,
/// `+proj=laea +lat_0=90 +lon_0=-40`); the south polar one's is 0.
const NORTH_POLE_LON0: f64 = -40.0;

/// The box (lat_lo, lng_lo, lat_hi, lng_hi) in degrees that expand_by_meters makes of the point
/// (lat, lng) and `metres`. A diamond across the antimeridian comes back with longitudes of both
/// signs, and its box spans every longitude between them, as ST_Envelope makes it.
pub fn expand_by_meters(lat: f64, lng: f64, metres: f64) -> (f64, f64, f64, f64) {
    let projection = Projection::best_for(lat, lng);
    let (x, y) = projection.forward(lat, lng);
    let mut bounds = (f64::INFINITY, f64::INFINITY, f64::NEG_INFINITY, f64::NEG_INFINITY);
    for (dx, dy) in [(metres, 0.0), (0.0, -metres), (-metres, 0.0), (0.0, metres)] {
        let (vlat, vlng) = projection.inverse(x + dx, y + dy);
        bounds = (bounds.0.min(vlat), bounds.1.min(vlng), bounds.2.max(vlat), bounds.3.max(vlng));
    }
    bounds
}

/// Whether the box expand_by_meters makes of (centre_lat, centre_lng) and `metres` covers the
/// point (lat, lng), its edges included (ST_Covers). A point well inside or outside the band the
/// box's edges can lie in takes no projection (FAST_INSIDE, FAST_OUTSIDE).
pub fn box_covers(centre_lat: f64, centre_lng: f64, metres: f64, lat: f64, lng: f64) -> bool {
    let metres_per_lng = crate::METRES_PER_DEGREE * centre_lat.to_radians().cos();
    if centre_lat.abs() <= POLAR_LAT && centre_lng.abs() + metres * FAST_OUTSIDE / metres_per_lng < 180.0 {
        let (north_m, east_m) = ((lat - centre_lat).abs() * crate::METRES_PER_DEGREE, (lng - centre_lng).abs() * metres_per_lng);
        if north_m > metres * FAST_OUTSIDE || east_m > metres * FAST_OUTSIDE {
            return false;
        }
        if north_m < metres * FAST_INSIDE && east_m < metres * FAST_INSIDE {
            return true;
        }
    }
    exact_box_covers(centre_lat, centre_lng, metres, lat, lng)
}

/// box_covers from the box itself.
fn exact_box_covers(centre_lat: f64, centre_lng: f64, metres: f64, lat: f64, lng: f64) -> bool {
    let (lat_lo, lng_lo, lat_hi, lng_hi) = expand_by_meters(centre_lat, centre_lng, metres);
    (lat_lo..=lat_hi).contains(&lat) && (lng_lo..=lng_hi).contains(&lng)
}

/// Shares of the metres a degree takes at 111,320 m (of latitude, and of longitude times the
/// cosine of the centroid's latitude) inside and outside of which a box from 70° S to 70° N that
/// keeps off the antimeridian holds, and does not hold, a point along an axis: there a box reaches
/// 0.9955 to 1.0072 times as far, a meridian degree being 110,574 to 111,694 m, a degree of the
/// prime vertical 111,319 m and more, and UTM's scale 0.9996 to 1.001.
const FAST_INSIDE: f64 = 0.99;
const FAST_OUTSIDE: f64 = 1.01;

/// The most that expand_by_meters' box reaches from its point, as a share of the metres a degree
/// of latitude (METRES_PER_DEGREE) and of longitude take there: a meridian degree is 110,574 m at
/// the equator, UTM shrinks by up to its k0 and the polar projection by cos(10°) along a meridian
/// at 70°. Searches for the boxes holding a point look this much farther. It bounds a box that
/// keeps off the antimeridian: one across it reaches this far in latitude, but spans every
/// longitude between its corners the long way round.
pub const BOX_REACH_FACTOR: f64 = 1.03;

/// _ST_BestSRID's projection of a point.
enum Projection {
    /// The UTM zone of this central meridian, in degrees.
    Utm { lon0: f64 },
    /// Lambert azimuthal equal-area about the north (true) or south pole, of this longitude of
    /// origin in degrees.
    Polar { north: bool, lon0: f64 },
}

impl Projection {
    fn best_for(lat: f64, lng: f64) -> Self {
        if lat > POLAR_LAT {
            return Projection::Polar { north: true, lon0: NORTH_POLE_LON0 };
        }
        if lat < -POLAR_LAT {
            return Projection::Polar { north: false, lon0: 0.0 };
        }
        let zone = (((lng + 180.0) / 6.0).floor() as i32).clamp(0, 59);
        Projection::Utm { lon0: -177.0 + 6.0 * zone as f64 }
    }

    /// (x, y) in metres of (lat, lng) degrees, without false easting or northing.
    fn forward(&self, lat: f64, lng: f64) -> (f64, f64) {
        match *self {
            Projection::Utm { lon0 } => tm_forward(lat.to_radians(), wrap_pi((lng - lon0).to_radians())),
            Projection::Polar { north, lon0 } => polar_forward(north, lat.to_radians(), wrap_pi((lng - lon0).to_radians())),
        }
    }

    /// (lat, lng) degrees of (x, y), the longitude in [-180, 180].
    fn inverse(&self, x: f64, y: f64) -> (f64, f64) {
        let (phi, lam, lon0) = match *self {
            Projection::Utm { lon0 } => {
                let (phi, lam) = tm_inverse(x, y);
                (phi, lam, lon0)
            }
            Projection::Polar { north, lon0 } => {
                let (phi, lam) = polar_inverse(north, x, y);
                (phi, lam, lon0)
            }
        };
        (phi.to_degrees(), wrap_pi(lam + lon0.to_radians()).to_degrees())
    }
}

/// An angle in radians brought into [-π, π] (PROJ's adjlon).
fn wrap_pi(a: f64) -> f64 {
    if a.abs() <= std::f64::consts::PI {
        return a;
    }
    let tau = std::f64::consts::TAU;
    a - tau * ((a + std::f64::consts::PI) / tau).floor()
}

fn eccentricity() -> f64 {
    (F * (2.0 - F)).sqrt()
}

/// The third flattening n, and the rectifying radius times UTM's k0.
fn tm_constants() -> (f64, f64) {
    let n = F / (2.0 - F);
    let n2 = n * n;
    (n, UTM_K0 * A / (1.0 + n) * (1.0 + n2 / 4.0 + n2 * n2 / 64.0 + n2 * n2 * n2 / 256.0))
}

/// Krüger's α (forward) series coefficients to n^6.
fn tm_alpha(n: f64) -> [f64; 6] {
    let (n2, n3) = (n * n, n * n * n);
    let (n4, n5, n6) = (n2 * n2, n2 * n3, n3 * n3);
    [
        n / 2.0 - 2.0 * n2 / 3.0 + 5.0 * n3 / 16.0 + 41.0 * n4 / 180.0 - 127.0 * n5 / 288.0 + 7891.0 * n6 / 37800.0,
        13.0 * n2 / 48.0 - 3.0 * n3 / 5.0 + 557.0 * n4 / 1440.0 + 281.0 * n5 / 630.0 - 1983433.0 * n6 / 1935360.0,
        61.0 * n3 / 240.0 - 103.0 * n4 / 140.0 + 15061.0 * n5 / 26880.0 + 167603.0 * n6 / 181440.0,
        49561.0 * n4 / 161280.0 - 179.0 * n5 / 168.0 + 6601661.0 * n6 / 7257600.0,
        34729.0 * n5 / 80640.0 - 3418889.0 * n6 / 1995840.0,
        212378941.0 * n6 / 319334400.0,
    ]
}

/// Krüger's β (inverse) series coefficients to n^6.
fn tm_beta(n: f64) -> [f64; 6] {
    let (n2, n3) = (n * n, n * n * n);
    let (n4, n5, n6) = (n2 * n2, n2 * n3, n3 * n3);
    [
        n / 2.0 - 2.0 * n2 / 3.0 + 37.0 * n3 / 96.0 - n4 / 360.0 - 81.0 * n5 / 512.0 + 96199.0 * n6 / 604800.0,
        n2 / 48.0 + n3 / 15.0 - 437.0 * n4 / 1440.0 + 46.0 * n5 / 105.0 - 1118711.0 * n6 / 3870720.0,
        17.0 * n3 / 480.0 - 37.0 * n4 / 840.0 - 209.0 * n5 / 4480.0 + 5569.0 * n6 / 90720.0,
        4397.0 * n4 / 161280.0 - 11.0 * n5 / 504.0 - 830251.0 * n6 / 7257600.0,
        4583.0 * n5 / 161280.0 - 108847.0 * n6 / 3991680.0,
        20648693.0 * n6 / 638668800.0,
    ]
}

/// The transverse Mercator (scale UTM_K0) of latitude phi and longitude lam from the central
/// meridian, in radians.
fn tm_forward(phi: f64, lam: f64) -> (f64, f64) {
    let e = eccentricity();
    let (n, ak0) = tm_constants();
    // The conformal latitude's tangent.
    let t = (phi.sin().atanh() - e * (e * phi.sin()).atanh()).sinh();
    let xi1 = t.atan2(lam.cos());
    let eta1 = (lam.sin() / (1.0 + t * t).sqrt()).atanh();
    let (mut xi, mut eta) = (xi1, eta1);
    for (j, a) in tm_alpha(n).iter().enumerate() {
        let k = 2.0 * (j + 1) as f64;
        xi += a * (k * xi1).sin() * (k * eta1).cosh();
        eta += a * (k * xi1).cos() * (k * eta1).sinh();
    }
    (ak0 * eta, ak0 * xi)
}

/// tm_forward's inverse: (latitude, longitude from the central meridian) in radians.
fn tm_inverse(x: f64, y: f64) -> (f64, f64) {
    let e = eccentricity();
    let (n, ak0) = tm_constants();
    let (xi, eta) = (y / ak0, x / ak0);
    let (mut xi1, mut eta1) = (xi, eta);
    for (j, b) in tm_beta(n).iter().enumerate() {
        let k = 2.0 * (j + 1) as f64;
        xi1 -= b * (k * xi).sin() * (k * eta).cosh();
        eta1 -= b * (k * xi).cos() * (k * eta).sinh();
    }
    let lam = eta1.sinh().atan2(xi1.cos());
    // The conformal latitude's tangent, then the latitude's by Newton's method (Karney's tauf).
    let tau1 = xi1.sin() / (eta1.sinh().powi(2) + xi1.cos().powi(2)).sqrt();
    let one_es = 1.0 - e * e;
    let conformal = |tau: f64| {
        let sigma = (e * (e * tau / (1.0 + tau * tau).sqrt()).atanh()).sinh();
        tau * (1.0 + sigma * sigma).sqrt() - sigma * (1.0 + tau * tau).sqrt()
    };
    let mut tau = tau1;
    for _ in 0..8 {
        let tau_i = conformal(tau);
        let step = (tau1 - tau_i) / (1.0 + tau_i * tau_i).sqrt() * (1.0 + one_es * tau * tau)
            / (one_es * (1.0 + tau * tau).sqrt());
        tau += step;
        if step.abs() < 1e-15 * tau.abs().max(1.0) {
            break;
        }
    }
    (tau.atan(), lam)
}

/// PROJ's pj_qsfn: the authalic function q of sin(latitude).
fn qsfn(sinphi: f64) -> f64 {
    let e = eccentricity();
    let one_es = 1.0 - e * e;
    let con = e * sinphi;
    one_es * (sinphi / (1.0 - con * con) - 0.5 / e * ((1.0 - con) / (1.0 + con)).ln())
}

/// The polar Lambert azimuthal equal-area (laea.cpp, N_POLE and S_POLE) of latitude phi and
/// longitude lam from the longitude of origin, in radians.
fn polar_forward(north: bool, phi: f64, lam: f64) -> (f64, f64) {
    let qp = qsfn(1.0);
    let q = qsfn(phi.sin());
    let q = if north { qp - q } else { qp + q };
    if q < 0.0 {
        return (0.0, 0.0);
    }
    let b = q.sqrt();
    (A * b * lam.sin(), A * lam.cos() * if north { -b } else { b })
}

/// polar_forward's inverse: (latitude, longitude from the longitude of origin) in radians, the
/// latitude from the authalic one by pj_authlat's series.
fn polar_inverse(north: bool, x: f64, y: f64) -> (f64, f64) {
    let (x, y) = (x / A, if north { -y / A } else { y / A });
    let q = x * x + y * y;
    if q == 0.0 {
        return (if north { 90.0_f64 } else { -90.0_f64 }.to_radians(), 0.0);
    }
    let qp = qsfn(1.0);
    let ab = 1.0 - q / qp;
    let ab = if north { ab } else { -ab };
    let es = eccentricity().powi(2);
    let apa = [
        es / 3.0 + es * es * 31.0 / 180.0 + es * es * es * 517.0 / 5040.0,
        es * es * 23.0 / 360.0 + es * es * es * 251.0 / 3780.0,
        es * es * es * 761.0 / 45360.0,
    ];
    let beta = ab.clamp(-1.0, 1.0).asin();
    let phi = beta + apa[0] * (2.0 * beta).sin() + apa[1] * (4.0 * beta).sin() + apa[2] * (6.0 * beta).sin();
    (phi, x.atan2(y))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_box_is_postgis_s_to_the_millimetre() {
        // ST_Envelope(ST_Buffer(ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography, m, 1)::geometry)
        // in PostGIS 3.4.3 on PROJ 7.2.1: (lat, lng, m) and the box (lat_lo, lng_lo, lat_hi, lng_hi).
        let cases = [
            ((20.0, -75.4, 25000.0), (19.77408544775242, -75.63898325483862, 20.22590878272961, -75.16100893512991)),
            ((0.0, 0.0, 25000.0), (-0.22587076280383872, -0.22433495899056488, 0.22587076280383872, 0.22438131237024397)),
            ((71.29, -156.78, 25000.0), (71.08697149774355, -157.39151425766457, 71.49241085342288, -156.16182627247076)),
            ((22.448067204908757, -1.6180971882660344, 2500.0), (22.42548859699002, -1.64238720940025, 22.470645747587383, -1.5938074529826327)),
            ((-35.71052985479539, -119.8622734692647, 25000.0), (-35.935652294608545, -120.13825952041604, -35.485397398289656, -119.58619808332921)),
            // A diamond across the antimeridian: the box spans the longitudes between.
            ((10.0, 179.999, 25000.0), (9.774195551541496, -179.99890296642235, 10.225800966458193, 179.99695214790427)),
        ];
        // 1e-8 degrees is a millimetre.
        for ((lat, lng, m), want) in cases {
            let got = expand_by_meters(lat, lng, m);
            for (g, w) in [(got.0, want.0), (got.1, want.1), (got.2, want.2), (got.3, want.3)] {
                assert!((g - w).abs() < 1e-8, "({lat}, {lng}, {m}): {got:?} against {want:?}");
            }
        }
    }

    #[test]
    fn a_box_holds_the_points_its_projected_corners_hold_whichever_way_it_is_told() {
        // Points from 0.97 to 1.03 of a box's reach along each axis and diagonal, round centroids
        // from 75° S to 75° N across every zone and up to the antimeridian, of each extent of
        // country_settings.yaml: box_covers' answer is the projected box's.
        let mut seed: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed >> 11) as f64 / (1u64 << 53) as f64
        };
        let mut tested = 0;
        for _ in 0..1500 {
            let (lat, lng) = (next() * 150.0 - 75.0, next() * 360.0 - 180.0);
            for metres in [1.0, 50.0, 700.0, 800.0, 1000.0, 2500.0, 3000.0, 4000.0, 5000.0, 25_000.0] {
                let (north, east) = (metres / crate::METRES_PER_DEGREE, metres / (crate::METRES_PER_DEGREE * lat.to_radians().cos()));
                for share in [0.97, 0.985, 0.99, 0.995, 1.0, 1.005, 1.01, 1.015, 1.03] {
                    for (dn, de) in [(1.0, 0.0), (-1.0, 0.0), (0.0, 1.0), (0.0, -1.0), (1.0, 1.0), (-1.0, -0.5)] {
                        let (p_lat, p_lng) = (lat + dn * north * share, lng + de * east * share);
                        if !(-180.0..=180.0).contains(&p_lng) { continue; }
                        assert_eq!(box_covers(lat, lng, metres, p_lat, p_lng), exact_box_covers(lat, lng, metres, p_lat, p_lng),
                                   "({lat}, {lng}) {metres} m: ({p_lat}, {p_lng})");
                        tested += 1;
                    }
                }
            }
        }
        assert!(tested > 750_000);
    }

    #[test]
    fn a_us_centroid_s_box_reaches_farther_north_than_a_degree_of_111_320_m_gives() {
        // At 20° N a degree of latitude is 110,704 m: a point 0.2255° north of a US centroid,
        // 24,960 m off, is inside its 25 km box, and one 0.2262° north is not.
        assert!(box_covers(20.0, -75.4, 25_000.0, 20.2255, -75.4));
        assert!(!box_covers(20.0, -75.4, 25_000.0, 20.2262, -75.4));
    }
}
