// The test portal's column of what Immich stores for a photo today: its
// reverse geocoding (server/src/repositories/map.repository.ts of the Immich
// release the portal's image copies its geodata from, v3.4.2 when this was
// written) over the same files, GeoNames' cities500.txt, admin1CodesASCII.txt,
// admin2Codes.txt and countryInfo.txt and Natural Earth's
// ne_10m_admin_0_countries.geojson (v5.1.2), as its import reads them and its
// queries answer. Immich asks Postgres:
//
//   SELECT * FROM geodata_places
//   WHERE earth_box(ll_to_earth_public($lat, $lon), 25000) @> ll_to_earth_public(latitude, longitude)
//   ORDER BY earth_distance(ll_to_earth_public($lat, $lon), ll_to_earth_public(latitude, longitude))
//   LIMIT 1
//
// and, where that finds no place, the first Natural Earth polygon (its outer
// ring alone) that holds the point:
//
//   SELECT * FROM naturalearth_countries WHERE coordinates @> point($lon, $lat) LIMIT 1
//
// Both are done here in the same arithmetic: earthdistance's earth() of
// 6378168 m, ll_to_earth_public's cube of x, y and z, earth_box's cube of
// gc_to_sec(25000) about it, which holds the points of its corners too (up
// to ~43 km away: there is no second test of the distance), the order by
// earth_distance, and Postgres' polygon @> point (geo_ops.c point_inside,
// whose edges are fuzzed by 1e-6 degrees). The place it finds names the
// city (its name), the state (its admin1 name) and the country (the
// countryInfo.txt name of its country code); a Natural Earth polygon the
// country alone, by its ADM0_A3 in countryInfo.txt, which some codes lack.
// Of places at one distance Postgres keeps the first its scan reads; here,
// the first in cities500.txt.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Deserialize;

/// The column's dataset id, as /datasets lists it and /reverse takes it.
pub const ID: &str = "immich-cities500";

pub const LABEL: &str = "Immich today (GeoNames cities500)";

/// Where the portal's image keeps the geodata of Immich's release image
/// (its /build/geodata), and the release's version beside it (RELEASE).
pub const DIR: &str = "/usr/local/share/immich-geodata";

/// The file the image writes beside the geodata: version=<tag> and
/// commit=<sha> of the Immich release it copied them from.
pub const RELEASE: &str = "immich-release.txt";

/// The file Immich's image dates its geodata by (geodata-date.txt).
pub const DATE_FILE: &str = "geodata-date.txt";

/// reverseGeocodeMaxDistance (server/src/constants.ts): the great-circle
/// metres earth_box makes the half side of its cube from.
pub const MAX_DISTANCE: f64 = 25_000.0;

/// earthdistance's earth(): the radius, in metres, of its sphere.
pub const EARTH: f64 = 6_378_168.0;

/// Postgres' RADIANS_PER_DEGREE (radians()).
const RADIANS_PER_DEGREE: f64 = 0.0174532925199432957692;

/// Postgres' EPSILON of its geometric types (geo_decls.h).
const EPSILON: f64 = 1.0e-6;

/// The geodata the portal answers the column from, as its image holds it:
/// its dir, its date and the Immich release it came with.
#[derive(Clone, Debug)]
pub struct Source {
    pub dir: PathBuf,
    /// The release's tag (v3.4.2) and commit, where RELEASE names them.
    pub version: Option<String>,
    pub commit: Option<String>,
    /// geodata-date.txt: when Immich's image downloaded the files.
    pub geodata_date: String,
    /// countryInfo.txt's names by ISO alpha-2 code, which Immich stores as
    /// the country of a place's code (none where the file doesn't read:
    /// the column's geodata then doesn't load either).
    pub country_names: Arc<BTreeMap<String, String>>,
}

impl Source {
    /// The geodata in `dir`: none where it holds no geodata-date.txt.
    pub fn at(dir: &Path) -> Option<Source> {
        let geodata_date = std::fs::read_to_string(dir.join(DATE_FILE)).ok()?.trim().to_string();
        let release = std::fs::read_to_string(dir.join(RELEASE)).unwrap_or_default();
        let value = |name: &str| {
            release.lines().find_map(|l| l.strip_prefix(name)?.strip_prefix('=')).map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
        };
        let names = read(dir, "countryInfo.txt").map(|text| country_names(&text).0.into_iter().collect()).unwrap_or_default();
        Some(Source {
            dir: dir.to_path_buf(),
            version: value("version"),
            commit: value("commit"),
            geodata_date,
            country_names: Arc::new(names),
        })
    }

    /// The geodata of PORTAL_IMMICH_GEODATA (DIR by default), if it holds
    /// any.
    pub fn from_env() -> Option<Source> {
        Source::at(Path::new(&std::env::var("PORTAL_IMMICH_GEODATA").unwrap_or_else(|_| DIR.to_string())))
    }

    /// The release as the page names it.
    fn release(&self) -> String {
        self.version.clone().unwrap_or_else(|| "release".to_string())
    }

    /// What /datasets lists of the column (among its "others"): no
    /// geocoder dataset, so of kind "immich", with no search; and the
    /// country names Immich stores by ISO alpha-2 code, with which the page
    /// names the country our Immich integration would store from another
    /// column's answer.
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": ID,
            "kind": "immich",
            "label": LABEL,
            "description": format!(
                "What Immich {} stores for a photo here: the nearest GeoNames cities500 place in a box of {} km about the point, \
                 else the Natural Earth country, else nothing (geodata of {}). Points only: no search, no polygons.",
                self.release(), MAX_DISTANCE / 1000.0, self.geodata_date
            ),
            "immich": self.about(),
            "country_names": self.country_names.as_ref(),
            "search": null,
        })
    }

    fn about(&self) -> serde_json::Value {
        serde_json::json!({
            "release": self.version,
            "commit": self.commit,
            "geodata_date": self.geodata_date,
            "max_distance_m": MAX_DISTANCE,
        })
    }
}

/// A row of geodata_places.
struct Place {
    /// Its line's place in cities500.txt: of places at one distance, the
    /// first wins.
    line: u32,
    id: i64,
    name: Box<str>,
    lat: f64,
    lon: f64,
    at: [f64; 3],
    country: Box<str>,
    admin1: Box<str>,
    admin2: Box<str>,
    feature: Box<str>,
}

/// A row of naturalearth_countries: a polygon's outer ring.
struct Country {
    admin: String,
    adm0_a3: String,
    kind: String,
    /// Its feature's place in the file and its polygon's in the feature.
    feature: usize,
    polygon: usize,
    /// (x, y) = (lon, lat), as the polygon's text gives them.
    ring: Vec<(f64, f64)>,
    /// The ring's extent, widened beyond Postgres' fuzz: no point outside
    /// it is in the ring.
    bbox: [f64; 4],
}

/// The geodata as Immich's import keeps it.
pub struct Geodata {
    /// Sorted by z (earth_box's cube is a range of z among the others).
    places: Vec<Place>,
    countries: Vec<Country>,
    admin1: HashMap<String, Option<String>>,
    admin2: HashMap<String, Option<String>>,
    by_alpha2: HashMap<String, String>,
    by_alpha3: HashMap<String, String>,
    /// The lines of cities500.txt the import leaves out (PPLX outside
    /// Australia, PPLH).
    pub skipped: usize,
}

/// The lines of `text` as Node's readline gives them: ended by \n, \r\n or
/// \r, the last one too where it has no end.
fn lines(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let (bytes, mut start, mut i) = (text.as_bytes(), 0, 0);
    while i < bytes.len() {
        match bytes[i] {
            b'\n' => {
                out.push(&text[start..i]);
                start = i + 1;
            }
            b'\r' => {
                out.push(&text[start..i]);
                if bytes.get(i + 1) == Some(&b'\n') {
                    i += 1;
                }
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    if start < bytes.len() {
        out.push(&text[start..]);
    }
    out
}

/// The countries' names of countryInfo.txt's `text` by their ISO alpha-2
/// and alpha-3 codes, as Immich's readCountryNames reads them: its first,
/// second and fifth columns, its # lines and those of no name left out.
fn country_names(text: &str) -> (HashMap<String, String>, HashMap<String, String>) {
    let (mut by_alpha2, mut by_alpha3) = (HashMap::new(), HashMap::new());
    for line in lines(text) {
        if line.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.split('\t').take(5).collect();
        let Some(name) = fields.get(4).map(|n| js_trim(n)).filter(|n| !n.is_empty()) else { continue };
        if !fields[0].is_empty() {
            by_alpha2.insert(fields[0].to_string(), name.to_string());
        }
        if !fields[1].is_empty() {
            by_alpha3.insert(fields[1].to_string(), name.to_string());
        }
    }
    (by_alpha2, by_alpha3)
}

/// JavaScript's String.prototype.trim.
fn js_trim(s: &str) -> &str {
    s.trim_matches(|c: char| (c.is_whitespace() && c != '\u{85}') || c == '\u{feff}')
}

/// JavaScript's Number() of a field: none for one that isn't a number.
fn js_number(s: &str) -> Option<f64> {
    let s = js_trim(s);
    if s.is_empty() {
        return Some(0.0);
    }
    s.parse::<f64>().ok().filter(|v| v.is_finite())
}

fn read(dir: &Path, name: &str) -> Result<String, String> {
    let bytes = std::fs::read(dir.join(name)).map_err(|e| format!("Geodata file {} not found: {e}", dir.join(name).display()))?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// ll_to_earth_public(lat, lon): the point's cube, x, y and z.
pub fn ll_to_earth(lat: f64, lon: f64) -> [f64; 3] {
    let (lat, lon) = (lat * RADIANS_PER_DEGREE, lon * RADIANS_PER_DEGREE);
    [EARTH * lat.cos() * lon.cos(), EARTH * lat.cos() * lon.sin(), EARTH * lat.sin()]
}

/// gc_to_sec(metres): the chord of an arc of the sphere.
fn gc_to_sec(m: f64) -> f64 {
    if m < 0.0 {
        0.0
    } else if m / EARTH > std::f64::consts::PI {
        2.0 * EARTH
    } else {
        2.0 * EARTH * (m / (2.0 * EARTH)).sin()
    }
}

/// sec_to_gc(chord): the arc of a chord of the sphere, in metres.
fn sec_to_gc(c: f64) -> f64 {
    if c < 0.0 {
        0.0
    } else if c / (2.0 * EARTH) > 1.0 {
        std::f64::consts::PI * EARTH
    } else {
        2.0 * EARTH * (c / (2.0 * EARTH)).asin()
    }
}

/// earth_distance(a, b): sec_to_gc of cube_distance, which sums the squares
/// of each axis's difference, larger minus smaller (distance_1D).
pub fn earth_distance(a: [f64; 3], b: [f64; 3]) -> f64 {
    let mut sum = 0.0;
    for i in 0..3 {
        let d = if a[i] <= b[i] { b[i] - a[i] } else { a[i] - b[i] };
        sum += d * d;
    }
    sec_to_gc(sum.sqrt())
}

/// earth_box(center, metres): cube_enlarge of the point by the chord, on
/// each axis [c - r, c + r].
fn earth_box(center: [f64; 3], m: f64) -> [(f64, f64); 3] {
    let r = gc_to_sec(m);
    center.map(|c| (c - r, c + r))
}

const ON_POLYGON: i32 = i32::MAX;

fn fp_zero(a: f64) -> bool {
    a.abs() <= EPSILON
}
fn fp_lt(a: f64, b: f64) -> bool {
    a + EPSILON < b
}
fn fp_le(a: f64, b: f64) -> bool {
    a <= b + EPSILON
}
fn fp_gt(a: f64, b: f64) -> bool {
    a > b + EPSILON
}
fn fp_ge(a: f64, b: f64) -> bool {
    a + EPSILON >= b
}

/// Postgres' lseg_crossing (geo_ops.c): how the segment from (prev_x,
/// prev_y) to (x, y), relative to the point, crosses the positive X axis.
fn lseg_crossing(x: f64, y: f64, prev_x: f64, prev_y: f64) -> i32 {
    if fp_zero(y) {
        if fp_zero(x) {
            ON_POLYGON
        } else if fp_gt(x, 0.0) {
            if fp_zero(prev_y) {
                return if fp_gt(prev_x, 0.0) { 0 } else { ON_POLYGON };
            }
            if fp_lt(prev_y, 0.0) { 1 } else { -1 }
        } else {
            if fp_zero(prev_y) {
                return if fp_lt(prev_x, 0.0) { 0 } else { ON_POLYGON };
            }
            0
        }
    } else {
        let y_sign = if fp_gt(y, 0.0) { 1 } else { -1 };
        if fp_zero(prev_y) {
            if fp_lt(prev_x, 0.0) { 0 } else { y_sign }
        } else if (y_sign < 0 && fp_lt(prev_y, 0.0)) || (y_sign > 0 && fp_gt(prev_y, 0.0)) {
            0
        } else {
            if fp_ge(x, 0.0) && fp_gt(prev_x, 0.0) {
                return 2 * y_sign;
            }
            if fp_lt(x, 0.0) && fp_le(prev_x, 0.0) {
                return 0;
            }
            let z = (x - prev_x) * y - (y - prev_y) * x;
            if fp_zero(z) {
                return ON_POLYGON;
            }
            if (y_sign < 0 && fp_lt(z, 0.0)) || (y_sign > 0 && fp_gt(z, 0.0)) {
                return 0;
            }
            2 * y_sign
        }
    }
}

/// Postgres' point_inside (geo_ops.c), which polygon @> point takes for
/// true where it isn't 0: 1 inside, 2 on its edge (within EPSILON).
pub fn point_inside(px: f64, py: f64, ring: &[(f64, f64)]) -> i32 {
    let Some(&(fx, fy)) = ring.first() else { return 0 };
    let (x0, y0) = (fx - px, fy - py);
    let (mut prev_x, mut prev_y) = (x0, y0);
    let mut total = 0;
    for &(rx, ry) in &ring[1..] {
        let (x, y) = (rx - px, ry - py);
        let cross = lseg_crossing(x, y, prev_x, prev_y);
        if cross == ON_POLYGON {
            return 2;
        }
        total += cross;
        (prev_x, prev_y) = (x, y);
    }
    let cross = lseg_crossing(x0, y0, prev_x, prev_y);
    if cross == ON_POLYGON {
        return 2;
    }
    total += cross;
    if total != 0 { 1 } else { 0 }
}

#[derive(Deserialize)]
struct Collection {
    #[serde(rename = "type")]
    kind: String,
    features: Vec<Feature>,
}

#[derive(Deserialize)]
struct Feature {
    properties: Properties,
    geometry: Geometry,
}

#[derive(Deserialize)]
struct Properties {
    #[serde(rename = "ADMIN")]
    admin: String,
    #[serde(rename = "ADM0_A3")]
    adm0_a3: String,
    #[serde(rename = "TYPE")]
    kind: String,
}

#[derive(Deserialize)]
struct Geometry {
    #[serde(rename = "type")]
    kind: String,
    coordinates: Vec<serde_json::Value>,
}

/// A ring of the GeoJSON, its points as (x, y).
fn ring(v: &serde_json::Value) -> Result<Vec<(f64, f64)>, String> {
    let points = v.as_array().ok_or("a ring is no array")?;
    points
        .iter()
        .map(|p| match (p.get(0).and_then(|x| x.as_f64()), p.get(1).and_then(|y| y.as_f64())) {
            (Some(x), Some(y)) => Ok((x, y)),
            _ => Err(format!("a point of a ring is no [x, y]: {p}")),
        })
        .collect()
}

impl Geodata {
    /// The geodata of `dir` as Immich's import reads it: cities500.txt but
    /// its PPLX outside Australia and its PPLH, each place's admin1 and
    /// admin2 names by "<country>.<admin1>" and "<country>.<admin1>.<admin2>"
    /// (the line of a code last in its file), the countries' names by their
    /// ISO alpha-2 and alpha-3 codes (countryInfo.txt's first, second and
    /// fifth columns, its # lines and those of no name left out), and the
    /// outer ring of each polygon of each Natural Earth country, in the
    /// file's order. An error where a file is missing or can't be read as
    /// Immich reads it.
    pub fn load(dir: &Path) -> Result<Geodata, String> {
        let admin = |name: &str| -> Result<HashMap<String, Option<String>>, String> {
            let text = read(dir, name)?;
            let mut map = HashMap::new();
            for line in lines(&text) {
                let mut fields = line.split('\t');
                let key = fields.next().unwrap_or_default().to_string();
                map.insert(key, fields.next().map(str::to_string));
            }
            Ok(map)
        };
        let (admin1, admin2) = (admin("admin1CodesASCII.txt")?, admin("admin2Codes.txt")?);

        let (by_alpha2, by_alpha3) = country_names(&read(dir, "countryInfo.txt")?);

        let text = read(dir, "cities500.txt")?;
        let (mut places, mut skipped) = (Vec::new(), 0);
        for (n, line) in lines(&text).into_iter().enumerate() {
            let f: Vec<&str> = line.split('\t').collect();
            if f.len() < 12 {
                return Err(format!("cities500.txt line {} has {} fields", n + 1, f.len()));
            }
            if (f[7] == "PPLX" && f[8] != "AU") || f[7] == "PPLH" {
                skipped += 1;
                continue;
            }
            let (Ok(id), Some(lat), Some(lon)) = (f[0].trim().parse::<i64>(), js_number(f[4]), js_number(f[5])) else {
                return Err(format!("cities500.txt line {} has no id or coordinates: {line}", n + 1));
            };
            places.push(Place {
                line: n as u32,
                id,
                name: f[1].into(),
                lat,
                lon,
                at: ll_to_earth(lat, lon),
                country: f[8].into(),
                admin1: f[10].into(),
                admin2: f[11].into(),
                feature: f[7].into(),
            });
        }
        places.sort_by(|a, b| a.at[2].total_cmp(&b.at[2]));

        let geojson: Collection = serde_json::from_str(&read(dir, "ne_10m_admin_0_countries.geojson")?)
            .map_err(|e| format!("ne_10m_admin_0_countries.geojson: {e}"))?;
        if geojson.kind != "FeatureCollection" {
            return Err("ne_10m_admin_0_countries.geojson is no FeatureCollection".to_string());
        }
        let mut countries = Vec::new();
        for (feature, f) in geojson.features.into_iter().enumerate() {
            let polygons: Vec<&serde_json::Value> = match f.geometry.kind.as_str() {
                // A MultiPolygon's polygons, each by its outer ring.
                "MultiPolygon" => f.geometry.coordinates.iter().map(|p| p.get(0).ok_or("a polygon has no ring")).collect::<Result<_, _>>()?,
                // A Polygon by its outer ring.
                "Polygon" => f.geometry.coordinates.first().into_iter().collect(),
                other => return Err(format!("feature {feature} ({}) is a {other}, which Immich's import can't read", f.properties.admin)),
            };
            for (polygon, r) in polygons.into_iter().enumerate() {
                let ring = ring(r).map_err(|e| format!("feature {feature} ({}): {e}", f.properties.admin))?;
                let margin = 10.0 * EPSILON;
                let bbox = ring.iter().fold([f64::INFINITY, f64::INFINITY, f64::NEG_INFINITY, f64::NEG_INFINITY], |b, &(x, y)| {
                    [b[0].min(x - margin), b[1].min(y - margin), b[2].max(x + margin), b[3].max(y + margin)]
                });
                countries.push(Country {
                    admin: f.properties.admin.clone(),
                    adm0_a3: f.properties.adm0_a3.clone(),
                    kind: f.properties.kind.clone(),
                    feature,
                    polygon,
                    ring,
                    bbox,
                });
            }
        }
        Ok(Geodata { places, countries, admin1, admin2, by_alpha2, by_alpha3, skipped })
    }

    /// The places Immich imports.
    pub fn places(&self) -> usize {
        self.places.len()
    }

    /// The Natural Earth rows: an outer ring of a polygon each.
    pub fn rings(&self) -> usize {
        self.countries.len()
    }

    /// What Immich's reverseGeocode answers at (lat, lon), and how; at 0, 0
    /// what its metadata extraction stores, which takes that for no GPS.
    pub fn reverse(&self, lat: f64, lon: f64) -> Answer<'_> {
        if lat == 0.0 && lon == 0.0 {
            return Answer { lat, lon, how: How::NoGps };
        }
        let at = ll_to_earth(lat, lon);
        let cube = earth_box(at, MAX_DISTANCE);
        let (lo, hi) = cube[2];
        let from = self.places.partition_point(|p| p.at[2] < lo);
        let to = self.places.partition_point(|p| p.at[2] <= hi);
        let mut best: Option<(f64, &Place)> = None;
        let mut in_box = 0;
        for p in &self.places[from..to] {
            if !(0..3).all(|i| cube[i].0 <= p.at[i] && cube[i].1 >= p.at[i]) {
                continue;
            }
            in_box += 1;
            let d = earth_distance(at, p.at);
            if best.is_none_or(|(bd, bp)| d < bd || (d == bd && p.line < bp.line)) {
                best = Some((d, p));
            }
        }
        if let Some((distance, place)) = best {
            return Answer { lat, lon, how: How::Place { place, distance, in_box, geodata: self } };
        }
        let country = self.countries.iter().find(|c| {
            lon >= c.bbox[0] && lat >= c.bbox[1] && lon <= c.bbox[2] && lat <= c.bbox[3] && point_inside(lon, lat, &c.ring) != 0
        });
        match country {
            Some(country) => Answer { lat, lon, how: How::Country { country, geodata: self } },
            None => Answer { lat, lon, how: How::Nothing },
        }
    }
}

/// What reverseGeocode found.
pub struct Answer<'a> {
    pub lat: f64,
    pub lon: f64,
    how: How<'a>,
}

enum How<'a> {
    Place { place: &'a Place, distance: f64, in_box: usize, geodata: &'a Geodata },
    Country { country: &'a Country, geodata: &'a Geodata },
    Nothing,
    NoGps,
}

impl Answer<'_> {
    /// The city, the state and the country Immich stores.
    pub fn stored(&self) -> (Option<&str>, Option<&str>, Option<&str>) {
        match &self.how {
            How::Place { place, geodata, .. } => (
                Some(&*place.name),
                geodata.admin1.get(&format!("{}.{}", place.country, place.admin1)).and_then(|n| n.as_deref()),
                geodata.by_alpha2.get(&*place.country).map(String::as_str),
            ),
            How::Country { country, geodata } => (None, None, geodata.by_alpha3.get(&country.adm0_a3).map(String::as_str)),
            How::Nothing | How::NoGps => (None, None, None),
        }
    }

    /// The answer of /reverse?dataset=immich-cities500: what Immich stores
    /// (immich.stored: city, state, country, each null where it stores
    /// none), how it found it (immich.how: "cities500", "natural_earth",
    /// "nothing", or "no_gps" at 0, 0), the place with its distance and the
    /// places of the box, or the Natural Earth polygon, and the release and
    /// its geodata; for the page's comparisons, the stored values as an
    /// address and a display name too.
    pub fn json(&self, source: &Source) -> serde_json::Value {
        let (city, state, country) = self.stored();
        let mut address = serde_json::Map::new();
        for (field, value) in [("city", city), ("state", state), ("country", country)] {
            if let Some(value) = value {
                address.insert(field.to_string(), value.into());
            }
        }
        let shown: Vec<&str> = [city, state, country].into_iter().flatten().filter(|v| !v.is_empty()).collect();
        let mut immich = source.about();
        immich["stored"] = serde_json::json!({ "city": city, "state": state, "country": country });
        immich["place"] = serde_json::Value::Null;
        immich["natural_earth"] = serde_json::Value::Null;
        immich["how"] = match &self.how {
            How::Place { place, distance, in_box, geodata } => {
                let admin1 = format!("{}.{}", place.country, place.admin1);
                let admin2 = format!("{admin1}.{}", place.admin2);
                immich["place"] = serde_json::json!({
                    "geonameid": place.id,
                    "name": place.name,
                    "latitude": place.lat,
                    "longitude": place.lon,
                    "feature_code": place.feature,
                    "country_code": place.country,
                    "admin1_code": place.admin1,
                    "admin1": geodata.admin1.get(&admin1).and_then(|n| n.as_deref()),
                    "admin2_code": place.admin2,
                    "admin2": geodata.admin2.get(&admin2).and_then(|n| n.as_deref()),
                    "distance_m": (distance * 100.0).round() / 100.0,
                    "beyond_max_distance": *distance > MAX_DISTANCE,
                });
                immich["in_box"] = (*in_box).into();
                "cities500".into()
            }
            How::Country { country, .. } => {
                immich["natural_earth"] = serde_json::json!({
                    "admin": country.admin,
                    "adm0_a3": country.adm0_a3,
                    "type": country.kind,
                    "feature": country.feature,
                    "polygon": country.polygon,
                });
                "natural_earth".into()
            }
            How::Nothing => "nothing".into(),
            How::NoGps => "no_gps".into(),
        };
        serde_json::json!({
            "lat": self.lat,
            "lon": self.lon,
            "display_name": if shown.is_empty() { serde_json::Value::Null } else { shown.join(", ").into() },
            "address": address,
            "immich": immich,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lines_end_as_readlines_do() {
        assert_eq!(lines("a\nb\r\nc\rd"), ["a", "b", "c", "d"]);
        assert_eq!(lines("a\n\nb\n"), ["a", "", "b"]);
        assert!(lines("").is_empty());
    }

    #[test]
    fn a_point_on_a_polygons_edge_is_in_it_as_postgres_has_it() {
        let square = [(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)];
        assert_eq!(point_inside(0.5, 0.5, &square), 1);
        assert_eq!(point_inside(1.0, 0.5, &square), 2);
        assert_eq!(point_inside(1.0 + 0.5e-6, 0.5, &square), 2);
        assert_eq!(point_inside(1.0 + 2e-6, 0.5, &square), 0);
        assert_eq!(point_inside(-0.5, 0.5, &square), 0);
        assert_eq!(point_inside(0.5, 1.5, &square), 0);
    }

    #[test]
    fn the_box_is_the_chord_of_25_km_about_the_point() {
        let r = gc_to_sec(MAX_DISTANCE);
        assert!((r - 24_999.984).abs() < 0.001, "{r}");
        let at = ll_to_earth(0.0, 0.0);
        assert_eq!(at, [EARTH, 0.0, 0.0]);
        assert!((earth_distance(at, ll_to_earth(1.0, 0.0)) - EARTH * RADIANS_PER_DEGREE).abs() < 1e-6);
    }
}
