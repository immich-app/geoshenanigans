// WASM port of the geocoder server's reverse-geocode logic.  Subset
// matching the current TS port (admin + places + nearest-centroid
// postcode), so the three implementations can be benchmarked on the
// same workload.
//
// Source of truth for the algorithms: geocoder/server/src/main.rs.
// Reproduced here (rather than path-imported) because the existing
// crate isn't yet split into lib + bin and depends on axum/tokio
// which don't compile to wasm32.

use std::collections::HashMap;
use wasm_bindgen::prelude::*;

use s2::cellid::CellID;
use s2::latlng::LatLng;

// --- Binary struct layouts (must match the C++ build pipeline) ---

#[repr(C)]
#[derive(Clone, Copy)]
struct AdminPolygon {
    vertex_offset: u32,
    vertex_count: u32,
    name_id: u32,
    admin_level: u8,
    place_type_override: u8,
    _pad2: u8,
    _pad3: u8,
    area: f32,
    country_code: u16,
    _pad4: u16,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct NodeCoord {
    lat: f32,
    lng: f32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct PlaceNode {
    lat: f32,
    lng: f32,
    name_id: u32,
    place_type: u8,
    _pad1: u8,
    _pad2: u16,
    parent_poly_id: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct PostcodeCentroid {
    lat: f32,
    lng: f32,
    postcode_id: u32,
    country_code: u16,
    _pad: u16,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct PoiRecord {
    lat: f32,
    lng: f32,
    vertex_offset: u32,
    vertex_count: u32,
    name_id: u32,
    category: u8,
    tier: u8,
    flags: u8,
    importance: u8,
    parent_street_id: u32,
    parent_postcode_id: u32,
    parent_poly_id: u32,
}

#[derive(serde::Deserialize, Default)]
struct PoiCategoryMeta {
    #[serde(default)]
    name: String,
    #[serde(default = "default_ref")]
    reference_distance: f64,
    #[serde(default)]
    max_distance: f64,
    #[serde(default = "default_imp")]
    default_importance: f64,
}
fn default_ref() -> f64 { 100.0 }
fn default_imp() -> f64 { 5.0 }

struct PoiMeta {
    categories: HashMap<u8, PoiCategoryMeta>,
}
impl PoiMeta {
    fn from_json(s: &str) -> Self {
        let mut categories = HashMap::new();
        if let Ok(raw) = serde_json::from_str::<HashMap<String, PoiCategoryMeta>>(s) {
            for (k, v) in raw {
                if let Ok(id) = k.parse::<u8>() { categories.insert(id, v); }
            }
        }
        PoiMeta { categories }
    }
    fn category_name(&self, id: u8) -> &str {
        self.categories.get(&id).map(|c| c.name.as_str()).unwrap_or("unknown")
    }
    fn reference_distance(&self, id: u8) -> f64 {
        self.categories.get(&id).map(|c| c.reference_distance).unwrap_or(100.0)
    }
    fn max_distance(&self, id: u8) -> f64 {
        self.categories.get(&id).map(|c| c.max_distance).unwrap_or(500.0)
    }
    fn default_importance(&self, id: u8) -> f64 {
        self.categories.get(&id).map(|c| c.default_importance).unwrap_or(5.0)
    }
}

const NO_DATA: u32 = 0xFFFFFFFF;
const INTERIOR_FLAG: u32 = 0x80000000;
const ID_MASK: u32 = 0x7FFFFFFF;

const STR_TIER_COUNT: usize = 5;
const ADMIN_CELL_LEVEL: u64 = 10;

// --- String pool (tiered) ---

struct StringPool {
    tiers: Vec<Option<Vec<u8>>>,
    bases: [u32; STR_TIER_COUNT + 1],
}

impl StringPool {
    fn from_layout(layout_json: &str, tiers: Vec<Option<Vec<u8>>>) -> Result<Self, String> {
        let v: serde_json::Value = serde_json::from_str(layout_json)
            .map_err(|e| format!("layout parse: {}", e))?;
        let entries = v.get("tiers").and_then(|t| t.as_array())
            .ok_or("missing tiers array")?;
        if entries.len() != STR_TIER_COUNT {
            return Err(format!("expected {} tiers, got {}", STR_TIER_COUNT, entries.len()));
        }
        let mut bases = [0u32; STR_TIER_COUNT + 1];
        for (t, e) in entries.iter().enumerate() {
            bases[t] = e.get("start").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
            bases[t + 1] = e.get("end").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
        }
        if tiers.len() != STR_TIER_COUNT || tiers[0].is_none() {
            return Err("strings_core (tier 0) is required".into());
        }
        Ok(StringPool { tiers, bases })
    }

    fn get(&self, off: u32) -> &str {
        if off == NO_DATA { return ""; }
        for t in 0..STR_TIER_COUNT {
            if off >= self.bases[t + 1] { continue; }
            let buf = match &self.tiers[t] {
                Some(b) => b,
                None => return "",
            };
            if off < self.bases[t] { return ""; }
            let local = (off - self.bases[t]) as usize;
            if local >= buf.len() { return ""; }
            let bytes = &buf[local..];
            let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
            return std::str::from_utf8(&bytes[..end]).unwrap_or("");
        }
        ""
    }
}

// --- Cell index readers ---

const ADMIN_CELL_RECORD_SIZE: usize = 12; // u64 cell_id + u32 entries_offset

fn lookup_admin_cell(cells: &[u8], target: u64) -> u32 {
    let n = cells.len() / ADMIN_CELL_RECORD_SIZE;
    let mut lo = 0;
    let mut hi = n;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let off = mid * ADMIN_CELL_RECORD_SIZE;
        let id = u64::from_le_bytes(cells[off..off + 8].try_into().unwrap());
        if id < target { lo = mid + 1; }
        else if id > target { hi = mid; }
        else {
            return u32::from_le_bytes(cells[off + 8..off + 12].try_into().unwrap());
        }
    }
    NO_DATA
}

fn for_each_entry(entries: &[u8], offset: u32, mut cb: impl FnMut(u32)) {
    if offset == NO_DATA { return; }
    let off = offset as usize;
    if off + 2 > entries.len() { return; }
    let count = u16::from_le_bytes([entries[off], entries[off + 1]]) as usize;
    let mut p = off + 2;
    for _ in 0..count {
        if p + 4 > entries.len() { return; }
        let id = u32::from_le_bytes(entries[p..p + 4].try_into().unwrap());
        cb(id);
        p += 4;
    }
}

// --- Geometry ---

fn point_in_polygon(lat: f32, lng: f32, vertices: &[u8], vert_off_bytes: usize, vert_count: usize) -> bool {
    if vert_count < 3 { return false; }
    let read = |i: usize| -> (f32, f32) {
        let off = vert_off_bytes + i * 8;
        let lat = f32::from_le_bytes(vertices[off..off + 4].try_into().unwrap());
        let lng = f32::from_le_bytes(vertices[off + 4..off + 8].try_into().unwrap());
        (lat, lng)
    };
    let mut inside = false;
    let mut j = vert_count - 1;
    for i in 0..vert_count {
        let (lat_i, lng_i) = read(i);
        let (lat_j, lng_j) = read(j);
        let intersects = (lng_i > lng) != (lng_j > lng) &&
            lat < (lat_j - lat_i) * (lng - lng_i) / (lng_j - lng_i) + lat_i;
        if intersects { inside = !inside; }
        j = i;
    }
    inside
}

fn read_admin_poly(buf: &[u8], poly_id: usize) -> Option<AdminPolygon> {
    let off = poly_id * std::mem::size_of::<AdminPolygon>();
    if off + std::mem::size_of::<AdminPolygon>() > buf.len() { return None; }
    Some(AdminPolygon {
        vertex_offset: u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()),
        vertex_count: u32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap()),
        name_id: u32::from_le_bytes(buf[off + 8..off + 12].try_into().unwrap()),
        admin_level: buf[off + 12],
        place_type_override: buf[off + 13],
        _pad2: buf[off + 14],
        _pad3: buf[off + 15],
        area: f32::from_le_bytes(buf[off + 16..off + 20].try_into().unwrap()),
        country_code: u16::from_le_bytes(buf[off + 20..off + 22].try_into().unwrap()),
        _pad4: u16::from_le_bytes(buf[off + 22..off + 24].try_into().unwrap()),
    })
}

// --- Index struct (owns the byte buffers) ---

struct Index {
    admin_cells: Vec<u8>,
    admin_entries: Vec<u8>,
    admin_polygons: Vec<u8>,
    admin_vertices: Vec<u8>,
    place_cells: Option<Vec<u8>>,
    place_entries: Option<Vec<u8>>,
    place_nodes: Option<Vec<u8>>,
    postcode_centroids: Option<Vec<u8>>,
    postcode_cells: Option<Vec<u8>>,
    postcode_entries: Option<Vec<u8>>,
    poi_records: Option<Vec<u8>>,
    poi_vertices: Option<Vec<u8>>,
    poi_cells: Option<Vec<u8>>,
    poi_entries: Option<Vec<u8>>,
    poi_meta: PoiMeta,
    strings: StringPool,
}

const ADMIN_LEVEL_COUNT: usize = 16;

#[derive(Default, serde::Serialize)]
struct AddressDetails {
    #[serde(skip_serializing_if = "Option::is_none")] landmark: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] city: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] town: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] village: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] suburb: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] hamlet: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] quarter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] neighbourhood: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] borough: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] municipality: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] county: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] country_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] postcode: Option<String>,
}

#[derive(serde::Serialize)]
struct PoiPlace {
    name: String,
    category: String,
    distance_m: f64,
}

#[derive(Default, serde::Serialize)]
struct Address {
    #[serde(skip_serializing_if = "Option::is_none")] display_name: Option<String>,
    address: AddressDetails,
    #[serde(skip_serializing_if = "Option::is_none")] places: Option<Vec<PoiPlace>>,
}

fn admin_level_to_field(lvl: usize) -> Option<&'static str> {
    match lvl {
        2 => Some("country"),
        4 => Some("state"),
        5 => Some("region"),
        6 => Some("county"),
        7 => Some("municipality"),
        8 => Some("city"),
        9 => Some("borough"),
        10 => Some("suburb"),
        _ => None,
    }
}

fn place_type_to_field(pt: u8) -> Option<&'static str> {
    match pt {
        1 => Some("city"),
        2 => Some("town"),
        3 => Some("village"),
        4 => Some("suburb"),
        5 => Some("neighbourhood"),
        6 => Some("quarter"),
        _ => None,
    }
}

fn cell_id_at_level(lat: f64, lng: f64, level: u64) -> u64 {
    let ll = LatLng::from_degrees(lat, lng);
    CellID::from(ll).parent(level).0
}

fn cell_neighbors_at_level(cell_id: u64, level: u64) -> Vec<u64> {
    let cell = CellID(cell_id);
    cell.all_neighbors(level).into_iter().map(|c| c.0).collect()
}

fn set_field(addr: &mut AddressDetails, field: &str, value: String) {
    match field {
        "city" => if addr.city.is_none() { addr.city = Some(value); },
        "town" => if addr.town.is_none() { addr.town = Some(value); },
        "village" => if addr.village.is_none() { addr.village = Some(value); },
        "suburb" => if addr.suburb.is_none() { addr.suburb = Some(value); },
        "hamlet" => if addr.hamlet.is_none() { addr.hamlet = Some(value); },
        "quarter" => if addr.quarter.is_none() { addr.quarter = Some(value); },
        "neighbourhood" => if addr.neighbourhood.is_none() { addr.neighbourhood = Some(value); },
        "borough" => if addr.borough.is_none() { addr.borough = Some(value); },
        "municipality" => if addr.municipality.is_none() { addr.municipality = Some(value); },
        "county" => if addr.county.is_none() { addr.county = Some(value); },
        "state" => if addr.state.is_none() { addr.state = Some(value); },
        "region" => if addr.region.is_none() { addr.region = Some(value); },
        "country" => if addr.country.is_none() { addr.country = Some(value); },
        _ => {}
    }
}

fn find_admin(idx: &Index, lat: f64, lng: f64) -> ([Option<AdminPolygon>; ADMIN_LEVEL_COUNT], Option<String>) {
    let cell = cell_id_at_level(lat, lng, ADMIN_CELL_LEVEL);
    let neighbors = cell_neighbors_at_level(cell, ADMIN_CELL_LEVEL);
    let mut best: [Option<(f32, AdminPolygon)>; ADMIN_LEVEL_COUNT] = [None; ADMIN_LEVEL_COUNT];
    let lat_f = lat as f32;
    let lng_f = lng as f32;

    for c in std::iter::once(cell).chain(neighbors.into_iter()) {
        for_each_entry(&idx.admin_entries, lookup_admin_cell(&idx.admin_cells, c), |raw_id| {
            let _is_interior = (raw_id & INTERIOR_FLAG) != 0;
            let poly_id = (raw_id & ID_MASK) as usize;
            let poly = match read_admin_poly(&idx.admin_polygons, poly_id) {
                Some(p) => p,
                None => return,
            };
            let level = poly.admin_level as usize;
            if level >= ADMIN_LEVEL_COUNT { return; }
            if poly.area <= 0.0 { return; }
            if let Some((cur_area, _)) = best[level] {
                if poly.area >= cur_area { return; }
            }
            // Always PIP (matches TS port post-fix).
            let off = poly.vertex_offset as usize * 8;
            let cnt = poly.vertex_count as usize;
            if off + cnt * 8 > idx.admin_vertices.len() { return; }
            if !point_in_polygon(lat_f, lng_f, &idx.admin_vertices, off, cnt) { return; }
            best[level] = Some((poly.area, poly));
        });
    }

    let cc = best[2].as_ref().and_then(|(_, p)| {
        if p.country_code != 0 {
            let bytes = [(p.country_code >> 8) as u8, (p.country_code & 0xff) as u8];
            std::str::from_utf8(&bytes).ok().map(|s| s.to_string())
        } else { None }
    });

    let mut by_level: [Option<AdminPolygon>; ADMIN_LEVEL_COUNT] = [None; ADMIN_LEVEL_COUNT];
    for i in 0..ADMIN_LEVEL_COUNT {
        by_level[i] = best[i].map(|(_, p)| p);
    }
    (by_level, cc)
}

fn smallest_in_range(by_level: &[Option<AdminPolygon>; ADMIN_LEVEL_COUNT], lo: usize, hi: usize) -> Option<AdminPolygon> {
    let mut best: Option<AdminPolygon> = None;
    for lvl in lo..=hi.min(ADMIN_LEVEL_COUNT - 1) {
        if let Some(p) = by_level[lvl] {
            if best.map(|b| p.area < b.area).unwrap_or(true) {
                best = Some(p);
            }
        }
    }
    best
}

fn read_place_node(buf: &[u8], idx: usize) -> Option<PlaceNode> {
    let off = idx * 20;
    if off + 20 > buf.len() { return None; }
    Some(PlaceNode {
        lat: f32::from_le_bytes(buf[off..off + 4].try_into().unwrap()),
        lng: f32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap()),
        name_id: u32::from_le_bytes(buf[off + 8..off + 12].try_into().unwrap()),
        place_type: buf[off + 12],
        _pad1: buf[off + 13],
        _pad2: u16::from_le_bytes(buf[off + 14..off + 16].try_into().unwrap()),
        parent_poly_id: u32::from_le_bytes(buf[off + 16..off + 20].try_into().unwrap()),
    })
}

fn find_places(
    idx: &Index,
    lat: f64,
    lng: f64,
    current_boundary: Option<&AdminPolygon>,
    municipality_boundary: Option<&AdminPolygon>,
    addr: &mut AddressDetails,
) {
    let (place_cells, place_entries, place_nodes) = match (
        idx.place_cells.as_ref(), idx.place_entries.as_ref(), idx.place_nodes.as_ref(),
    ) {
        (Some(c), Some(e), Some(n)) => (c, e, n),
        _ => return,
    };

    let cell = cell_id_at_level(lat, lng, ADMIN_CELL_LEVEL);
    let neighbors = cell_neighbors_at_level(cell, ADMIN_CELL_LEVEL);

    const DEG_TO_RAD: f64 = std::f64::consts::PI / 180.0;
    let cos_lat = (lat * DEG_TO_RAD).cos();
    let max_city = (0.16 * DEG_TO_RAD).powi(2);
    let max_town = (0.08 * DEG_TO_RAD).powi(2);
    let max_village = (0.04 * DEG_TO_RAD).powi(2);
    let max_rank20 = (0.02 * DEG_TO_RAD).powi(2);

    // best[pt] = (dist_sq, place_node)
    let mut best: [Option<(f64, PlaceNode)>; 7] = [None; 7];

    for c in std::iter::once(cell).chain(neighbors.into_iter()) {
        for_each_entry(place_entries, lookup_admin_cell(place_cells, c), |id| {
            let pn = match read_place_node(place_nodes, id as usize) {
                Some(p) => p,
                None => return,
            };
            let pt = pn.place_type as usize;
            if pt >= 7 { return; }
            let dlat = (lat - pn.lat as f64) * DEG_TO_RAD;
            let dlng = (lng - pn.lng as f64) * DEG_TO_RAD;
            let d = dlat * dlat + dlng * dlng * cos_lat * cos_lat;
            if let Some((bd, _)) = best[pt] {
                if d >= bd { return; }
            }
            best[pt] = Some((d, pn));
        });
    }

    let node_inside = |pn: &PlaceNode, b: Option<&AdminPolygon>| -> bool {
        match b {
            None => true,
            Some(poly) => {
                let off = poly.vertex_offset as usize * 8;
                let cnt = poly.vertex_count as usize;
                if off + cnt * 8 > idx.admin_vertices.len() { return true; }
                point_in_polygon(pn.lat, pn.lng, &idx.admin_vertices, off, cnt)
            }
        }
    };

    fn fuzzy_half_m(pt: u8) -> f64 {
        let r = match pt { 0 => 15000.0, 1 => 4000.0, 2 | 3 => 2000.0, 4 | 5 | 6 => 1000.0, _ => 500.0 };
        r / std::f64::consts::SQRT_2
    }
    let mut fuzzy: Option<(f64, f64, f64)> = None;
    let inside_fuzzy = |pn: &PlaceNode, fc: Option<(f64, f64, f64)>| -> bool {
        match fc {
            None => true,
            Some((lat0, lng0, half_m)) => {
                let dlat_m = ((pn.lat as f64 - lat0) * 111320.0).abs();
                let cl = (lat0 * DEG_TO_RAD).cos();
                let dlng_m = ((pn.lng as f64 - lng0) * 111320.0 * cl).abs();
                dlat_m <= half_m && dlng_m <= half_m
            }
        }
    };

    // city/town/village (rank 16-19): single slot, priority order
    for pt in [0, 1, 2usize] {
        if let Some((d, pn)) = best[pt] {
            let threshold = match pt { 0 => max_city, 1 => max_town, _ => max_village };
            if d > threshold { continue; }
            if !node_inside(&pn, municipality_boundary) { continue; }
            let name = idx.strings.get(pn.name_id);
            if name.is_empty() { continue; }
            let field = match pt { 0 => "city", 1 => "town", _ => "village" };
            set_field(addr, field, name.to_string());
            fuzzy = Some((pn.lat as f64, pn.lng as f64, fuzzy_half_m(pt as u8)));
            break;
        }
    }

    // rank 20: suburb vs hamlet
    let suburb = best[3];
    let hamlet = best[4];
    let s_valid = suburb.filter(|(d, pn)| *d <= max_rank20 && node_inside(pn, current_boundary) && inside_fuzzy(pn, fuzzy));
    let h_valid = hamlet.filter(|(d, pn)| *d <= max_rank20 && node_inside(pn, current_boundary) && inside_fuzzy(pn, fuzzy));
    match (s_valid, h_valid) {
        (Some((sd, sp)), Some((hd, hp))) => {
            if sd <= hd {
                set_field(addr, "suburb", idx.strings.get(sp.name_id).to_string());
                fuzzy = Some((sp.lat as f64, sp.lng as f64, fuzzy_half_m(3)));
            } else {
                set_field(addr, "hamlet", idx.strings.get(hp.name_id).to_string());
                fuzzy = Some((hp.lat as f64, hp.lng as f64, fuzzy_half_m(4)));
            }
        }
        (Some((_, p)), None) => {
            set_field(addr, "suburb", idx.strings.get(p.name_id).to_string());
            fuzzy = Some((p.lat as f64, p.lng as f64, fuzzy_half_m(3)));
        }
        (None, Some((_, p))) => {
            set_field(addr, "hamlet", idx.strings.get(p.name_id).to_string());
            fuzzy = Some((p.lat as f64, p.lng as f64, fuzzy_half_m(4)));
        }
        (None, None) => {}
    }

    // rank 22: quarter vs neighbourhood
    let q = best[6];
    let n = best[5];
    let q_v = q.filter(|(d, pn)| *d <= max_rank20 && node_inside(pn, current_boundary) && inside_fuzzy(pn, fuzzy));
    let n_v = n.filter(|(d, pn)| *d <= max_rank20 && node_inside(pn, current_boundary) && inside_fuzzy(pn, fuzzy));
    match (q_v, n_v) {
        (Some((qd, qp)), Some((nd, np))) => {
            if qd <= nd {
                set_field(addr, "quarter", idx.strings.get(qp.name_id).to_string());
            } else {
                set_field(addr, "neighbourhood", idx.strings.get(np.name_id).to_string());
            }
        }
        (Some((_, p)), None) => set_field(addr, "quarter", idx.strings.get(p.name_id).to_string()),
        (None, Some((_, p))) => set_field(addr, "neighbourhood", idx.strings.get(p.name_id).to_string()),
        (None, None) => {}
    }
}

// --- POIs ---

const POI_RECORD_SIZE: usize = 36;

fn read_poi(buf: &[u8], idx: usize) -> Option<PoiRecord> {
    let off = idx * POI_RECORD_SIZE;
    if off + POI_RECORD_SIZE > buf.len() { return None; }
    Some(PoiRecord {
        lat: f32::from_le_bytes(buf[off..off + 4].try_into().unwrap()),
        lng: f32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap()),
        vertex_offset: u32::from_le_bytes(buf[off + 8..off + 12].try_into().unwrap()),
        vertex_count: u32::from_le_bytes(buf[off + 12..off + 16].try_into().unwrap()),
        name_id: u32::from_le_bytes(buf[off + 16..off + 20].try_into().unwrap()),
        category: buf[off + 20],
        tier: buf[off + 21],
        flags: buf[off + 22],
        importance: buf[off + 23],
        parent_street_id: u32::from_le_bytes(buf[off + 24..off + 28].try_into().unwrap()),
        parent_postcode_id: u32::from_le_bytes(buf[off + 28..off + 32].try_into().unwrap()),
        parent_poly_id: u32::from_le_bytes(buf[off + 32..off + 36].try_into().unwrap()),
    })
}

struct PoiHit {
    name: String,
    category: String,
    distance_m: f64,
    contained: bool,
    is_point: bool,
    score: f64,
    area: f64,
}

fn point_to_segment_dist_sq(
    px: f64, py: f64, ax: f64, ay: f64, bx: f64, by: f64, cos_lat: f64,
) -> f64 {
    let dx = bx - ax;
    let dy = by - ay;
    let len_sq = dx * dx + dy * dy;
    let t = if len_sq == 0.0 { 0.0 }
        else { (((px - ax) * dx + (py - ay) * dy) / len_sq).clamp(0.0, 1.0) };
    let proj_x = ax + t * dx;
    let proj_y = ay + t * dy;
    let dlat = (px - proj_x) * std::f64::consts::PI / 180.0;
    let dlng = (py - proj_y) * std::f64::consts::PI / 180.0;
    dlat * dlat + dlng * dlng * cos_lat * cos_lat
}

fn find_pois(idx: &Index, lat: f64, lng: f64) -> Vec<PoiHit> {
    let (records, vertices, cells, entries) = match (
        idx.poi_records.as_ref(), idx.poi_vertices.as_ref(),
        idx.poi_cells.as_ref(), idx.poi_entries.as_ref(),
    ) {
        (Some(r), Some(v), Some(c), Some(e)) => (r, v, c, e),
        _ => return vec![],
    };
    const DEG: f64 = std::f64::consts::PI / 180.0;
    let cos_lat = (lat * DEG).cos();
    let lat_f = lat as f32;
    let lng_f = lng as f32;
    let total_pois = records.len() / POI_RECORD_SIZE;

    let cell = cell_id_at_level(lat, lng, ADMIN_CELL_LEVEL);
    let neighbors = cell_neighbors_at_level(cell, ADMIN_CELL_LEVEL);

    let mut results: Vec<PoiHit> = Vec::new();
    for c in std::iter::once(cell).chain(neighbors.into_iter()) {
        for_each_entry(entries, lookup_admin_cell(cells, c), |raw_id| {
            let is_interior = (raw_id & INTERIOR_FLAG) != 0;
            let pid = (raw_id & ID_MASK) as usize;
            if pid >= total_pois { return; }
            let poi = match read_poi(records, pid) { Some(p) => p, None => return };
            if poi.name_id == NO_DATA { return; }
            let name = idx.strings.get(poi.name_id);
            if name.is_empty() { return; }

            let ref_dist = idx.poi_meta.reference_distance(poi.category);
            let importance = if poi.importance > 0 { poi.importance as f64 }
                else { idx.poi_meta.default_importance(poi.category) };
            let base_max = idx.poi_meta.max_distance(poi.category);

            let dist_m;
            let contained;
            let mut area_sq_deg = 0.0;

            if poi.vertex_count > 0 {
                let vert_off = poi.vertex_offset as usize;
                let vert_count = poi.vertex_count as usize;
                let vert_off_bytes = vert_off * 8;
                if vert_off_bytes + vert_count * 8 > vertices.len() { return; }
                contained = is_interior || point_in_polygon(lat_f, lng_f, vertices, vert_off_bytes, vert_count);
                if contained {
                    dist_m = 0.0;
                    let mut a = 0.0;
                    for i in 0..vert_count {
                        let j = if i + 1 < vert_count { i + 1 } else { 0 };
                        let i_off = vert_off_bytes + i * 8;
                        let j_off = vert_off_bytes + j * 8;
                        let i_lat = f32::from_le_bytes(vertices[i_off..i_off + 4].try_into().unwrap()) as f64;
                        let i_lng = f32::from_le_bytes(vertices[i_off + 4..i_off + 8].try_into().unwrap()) as f64;
                        let j_lat = f32::from_le_bytes(vertices[j_off..j_off + 4].try_into().unwrap()) as f64;
                        let j_lng = f32::from_le_bytes(vertices[j_off + 4..j_off + 8].try_into().unwrap()) as f64;
                        a += i_lng * j_lat - j_lng * i_lat;
                    }
                    area_sq_deg = (a / 2.0).abs();
                } else {
                    let mut min_d = f64::MAX;
                    for i in 0..vert_count {
                        let j = if i + 1 < vert_count { i + 1 } else { 0 };
                        let i_off = vert_off_bytes + i * 8;
                        let j_off = vert_off_bytes + j * 8;
                        let i_lat = f32::from_le_bytes(vertices[i_off..i_off + 4].try_into().unwrap()) as f64;
                        let i_lng = f32::from_le_bytes(vertices[i_off + 4..i_off + 8].try_into().unwrap()) as f64;
                        let j_lat = f32::from_le_bytes(vertices[j_off..j_off + 4].try_into().unwrap()) as f64;
                        let j_lng = f32::from_le_bytes(vertices[j_off + 4..j_off + 8].try_into().unwrap()) as f64;
                        let d = point_to_segment_dist_sq(lat, lng, i_lat, i_lng, j_lat, j_lng, cos_lat);
                        if d < min_d { min_d = d; }
                    }
                    dist_m = min_d.sqrt() * 6_371_000.0;
                }
            } else {
                contained = false;
                let dlat = (lat - poi.lat as f64) * DEG;
                let dlng = (lng - poi.lng as f64) * DEG;
                dist_m = (dlat * dlat + dlng * dlng * cos_lat * cos_lat).sqrt() * 6_371_000.0;
            }

            if !contained {
                if base_max == 0.0 { return; }
                let effective_max = base_max * (1.0 + importance / 50.0);
                if dist_m > effective_max { return; }
            }
            if poi.category == 93 { return; } // ISLAND

            let proximity_weight = if contained { 3.0 }
                else { 1.0 / (1.0 + (dist_m / ref_dist).powi(2)) };
            let score = importance * proximity_weight;
            if score < 0.5 { return; }

            results.push(PoiHit {
                name: name.to_string(),
                category: idx.poi_meta.category_name(poi.category).to_string(),
                distance_m: if contained { 0.0 } else { dist_m },
                contained,
                is_point: poi.vertex_count == 0,
                score,
                area: area_sq_deg,
            });
        });
    }

    results.sort_by(|a, b| {
        use std::cmp::Ordering;
        match (a.contained, b.contained) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (true, true) => a.area.partial_cmp(&b.area).unwrap_or(Ordering::Equal),
            (false, false) => b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal),
        }
    });
    let mut seen_names = std::collections::HashSet::new();
    let mut seen_point_cats = std::collections::HashSet::new();
    let mut out: Vec<PoiHit> = Vec::new();
    for r in results.into_iter() {
        if !seen_names.insert(r.name.clone()) { continue; }
        if !r.contained && r.is_point && !seen_point_cats.insert(r.category.clone()) { continue; }
        out.push(r);
        if out.len() >= 5 { break; }
    }
    out
}

fn read_postcode_centroid(buf: &[u8], idx: usize) -> Option<PostcodeCentroid> {
    let off = idx * 16;
    if off + 16 > buf.len() { return None; }
    Some(PostcodeCentroid {
        lat: f32::from_le_bytes(buf[off..off + 4].try_into().unwrap()),
        lng: f32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap()),
        postcode_id: u32::from_le_bytes(buf[off + 8..off + 12].try_into().unwrap()),
        country_code: u16::from_le_bytes(buf[off + 12..off + 14].try_into().unwrap()),
        _pad: u16::from_le_bytes(buf[off + 14..off + 16].try_into().unwrap()),
    })
}

fn resolve_postcode(idx: &Index, lat: f64, lng: f64, country: Option<&str>) -> Option<String> {
    let centroids = idx.postcode_centroids.as_ref()?;
    let cgate: Option<u16> = country.and_then(|c| {
        let bytes = c.as_bytes();
        if bytes.len() == 2 { Some(((bytes[0] as u16) << 8) | bytes[1] as u16) } else { None }
    });
    const DEG_TO_RAD: f64 = std::f64::consts::PI / 180.0;
    let cos_lat = (lat * DEG_TO_RAD).cos();
    let max_dist = (0.05 * DEG_TO_RAD).powi(2);
    let mut best_dist = f64::MAX;
    let mut best: Option<String> = None;

    let mut consider = |i: usize| {
        let pc = match read_postcode_centroid(centroids, i) {
            Some(p) => p,
            None => return,
        };
        if let Some(cc) = cgate {
            if pc.country_code != 0 && pc.country_code != cc { return; }
        }
        let dlat = (lat - pc.lat as f64) * DEG_TO_RAD;
        let dlng = (lng - pc.lng as f64) * DEG_TO_RAD;
        let d = dlat * dlat + dlng * dlng * cos_lat * cos_lat;
        if d >= best_dist || d >= max_dist { return; }
        let s = idx.strings.get(pc.postcode_id);
        if s.is_empty() || s.len() > 20 { return; }
        best_dist = d;
        best = Some(s.to_string());
    };

    if let (Some(cells), Some(entries)) = (idx.postcode_cells.as_ref(), idx.postcode_entries.as_ref()) {
        let cell = cell_id_at_level(lat, lng, ADMIN_CELL_LEVEL);
        let neighbors = cell_neighbors_at_level(cell, ADMIN_CELL_LEVEL);
        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            for_each_entry(entries, lookup_admin_cell(cells, c), |id| consider(id as usize));
        }
    } else {
        let n = centroids.len() / 16;
        for i in 0..n { consider(i); }
    }
    best
}

fn build_display(addr: &AddressDetails) -> String {
    let order: [Option<&String>; 14] = [
        addr.neighbourhood.as_ref(), addr.quarter.as_ref(),
        addr.suburb.as_ref(), addr.hamlet.as_ref(),
        addr.village.as_ref(), addr.town.as_ref(), addr.city.as_ref(),
        addr.borough.as_ref(), addr.municipality.as_ref(),
        addr.county.as_ref(), addr.region.as_ref(), addr.state.as_ref(),
        addr.country.as_ref(), addr.postcode.as_ref(),
    ];
    let mut seen: HashMap<String, ()> = HashMap::new();
    let mut parts: Vec<String> = Vec::new();
    for o in order {
        if let Some(s) = o {
            if !s.is_empty() && !seen.contains_key(s) {
                parts.push(s.clone());
                seen.insert(s.clone(), ());
            }
        }
    }
    parts.join(", ")
}

fn query_inner(idx: &Index, lat: f64, lng: f64) -> Address {
    let mut addr = AddressDetails::default();
    let (by_level, cc) = find_admin(idx, lat, lng);
    for lvl in 0..ADMIN_LEVEL_COUNT {
        if let Some(p) = by_level[lvl] {
            let name = idx.strings.get(p.name_id);
            if name.is_empty() { continue; }
            let field = place_type_to_field(p.place_type_override)
                .or_else(|| admin_level_to_field(lvl));
            if let Some(f) = field {
                set_field(&mut addr, f, name.to_string());
            }
        }
    }
    addr.country_code = cc.clone();

    let cur = smallest_in_range(&by_level, 8, 10);
    let muni = smallest_in_range(&by_level, 7, 8);
    find_places(idx, lat, lng, cur.as_ref(), muni.as_ref(), &mut addr);

    let pois = find_pois(idx, lat, lng);
    if !pois.is_empty() && pois[0].contained {
        addr.landmark = Some(pois[0].name.clone());
    }

    if let Some(pc) = resolve_postcode(idx, lat, lng, cc.as_deref()) {
        addr.postcode = Some(pc);
    }
    let display = build_display(&addr);
    let places = if pois.is_empty() {
        None
    } else {
        Some(pois.into_iter().map(|p| PoiPlace {
            name: p.name,
            category: p.category,
            distance_m: (p.distance_m * 10.0).round() / 10.0,
        }).collect())
    };
    Address { display_name: Some(display), address: addr, places }
}

// --- WASM exports ---

#[wasm_bindgen]
pub struct Geocoder {
    index: Index,
}

#[wasm_bindgen]
impl Geocoder {
    /// Construct from a JS object whose values are `Uint8Array | null`
    /// for each binary file, plus a `strings_layout: string` JSON.
    /// Only `admin_cells`, `admin_entries`, `admin_polygons`,
    /// `admin_vertices`, `strings_core`, and `strings_layout` are
    /// strictly required; the rest are optional.
    #[wasm_bindgen(constructor)]
    pub fn new(buffers: JsValue) -> Result<Geocoder, JsValue> {
        let obj = js_sys::Object::from(buffers);

        let take_required = |k: &str| -> Result<Vec<u8>, JsValue> {
            let v = js_sys::Reflect::get(&obj, &JsValue::from_str(k))?;
            if v.is_undefined() || v.is_null() {
                return Err(JsValue::from_str(&format!("missing required buffer: {}", k)));
            }
            let arr = js_sys::Uint8Array::from(v);
            Ok(arr.to_vec())
        };
        let take_optional = |k: &str| -> Option<Vec<u8>> {
            let v = match js_sys::Reflect::get(&obj, &JsValue::from_str(k)) {
                Ok(v) => v,
                Err(_) => return None,
            };
            if v.is_undefined() || v.is_null() { return None; }
            Some(js_sys::Uint8Array::from(v).to_vec())
        };
        let take_string = |k: &str| -> Result<String, JsValue> {
            let v = js_sys::Reflect::get(&obj, &JsValue::from_str(k))?;
            v.as_string().ok_or_else(|| JsValue::from_str(&format!("expected string: {}", k)))
        };

        let admin_cells = take_required("admin_cells")?;
        let admin_entries = take_required("admin_entries")?;
        let admin_polygons = take_required("admin_polygons")?;
        let admin_vertices = take_required("admin_vertices")?;
        let strings_layout = take_string("strings_layout")?;

        let strings_core = take_optional("strings_core");
        let strings_street = take_optional("strings_street");
        let strings_addr = take_optional("strings_addr");
        let strings_postcode = take_optional("strings_postcode");
        let strings_poi = take_optional("strings_poi");

        let strings = StringPool::from_layout(
            &strings_layout,
            vec![strings_core, strings_street, strings_addr, strings_postcode, strings_poi],
        ).map_err(|e| JsValue::from_str(&e))?;

        let poi_meta_json = match js_sys::Reflect::get(&obj, &JsValue::from_str("poi_meta")) {
            Ok(v) if v.is_string() => v.as_string().unwrap_or_default(),
            _ => String::new(),
        };

        let index = Index {
            admin_cells, admin_entries, admin_polygons, admin_vertices,
            place_cells: take_optional("place_cells"),
            place_entries: take_optional("place_entries"),
            place_nodes: take_optional("place_nodes"),
            postcode_centroids: take_optional("postcode_centroids"),
            postcode_cells: take_optional("postcode_centroid_cells"),
            postcode_entries: take_optional("postcode_centroid_entries"),
            poi_records: take_optional("poi_records"),
            poi_vertices: take_optional("poi_vertices"),
            poi_cells: take_optional("poi_cells"),
            poi_entries: take_optional("poi_entries"),
            poi_meta: PoiMeta::from_json(&poi_meta_json),
            strings,
        };

        Ok(Geocoder { index })
    }

    /// Reverse-geocode a single coordinate. Returns a JS object with
    /// `display_name` and `address` keys, mirroring the Rust HTTP
    /// server's response shape.
    #[wasm_bindgen]
    pub fn reverse(&self, lat: f64, lng: f64) -> Result<JsValue, JsValue> {
        let r = query_inner(&self.index, lat, lng);
        serde_wasm_bindgen::to_value(&r).map_err(|e| JsValue::from_str(&e.to_string()))
    }
}
