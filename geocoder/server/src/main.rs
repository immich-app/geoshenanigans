mod auth;

use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use memmap2::Mmap;
use s2::cellid::CellID;
use s2::latlng::LatLng;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::sync::{Arc, RwLock};

// --- S2 helpers ---

const DEFAULT_STREET_CELL_LEVEL: u64 = 17;
const DEFAULT_ADMIN_CELL_LEVEL: u64 = 10;
// Default primary-feature search radius. Matches Nominatim's
// lookup_street_poi default of 0.006 degrees ≈ 660m. This is the
// radius inside which we'll accept a street/addr/interp/POI as the
// primary feature for the reverse result.
const DEFAULT_SEARCH_DISTANCE: f64 = 660.0;

fn cell_id_at_level(lat: f64, lng: f64, level: u64) -> u64 {
    let ll = LatLng::from_degrees(lat, lng);
    CellID::from(ll).parent(level).0
}

fn cell_neighbors_at_level(cell_id: u64, level: u64) -> Vec<u64> {
    let cell = CellID(cell_id);
    cell.all_neighbors(level).into_iter().map(|c| c.0).collect()
}

// --- Binary format structs (must match C++ build pipeline) ---

#[repr(C)]
#[derive(Clone, Copy)]
struct WayHeader {
    node_offset: u32,
    node_count: u8,
    name_id: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct AddrPoint {
    lat: f32,
    lng: f32,
    housenumber_id: u32,
    street_id: u32,
    parent_way_id: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct InterpWay {
    node_offset: u32,
    node_count: u8,
    street_id: u32,
    start_number: u32,
    end_number: u32,
    interpolation: u8,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct AdminPolygon {
    vertex_offset: u32,
    vertex_count: u32,
    name_id: u32,
    admin_level: u8,
    place_type_override: u8,  // 0=none, 1=city, 2=town, 3=village, 4=suburb, 5=neighbourhood, 6=quarter
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
    /// Name offset of the nearest named street to this POI, pre-
    /// computed at build time. Used to populate the `road` field
    /// when this POI wins query_geo primary-feature selection.
    /// Mirrors Nominatim's parent_place_id chain from a rank-30
    /// POI back to its rank-26 road. `NO_DATA` if unset.
    parent_street_id: u32,
}

const POI_FLAG_WIKIPEDIA: u8 = 0x01;
const POI_FLAG_WIKIDATA: u8 = 0x02;

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
struct PlaceNode {
    lat: f32,
    lng: f32,
    name_id: u32,
    place_type: u8,
    _pad1: u8,
    _pad2: u16,
    /// Smallest admin polygon (by area) that contains this place node
    /// centroid, computed at build time. Used to gate nearest-place
    /// fallback: a quarter/neighbourhood candidate is only returned if
    /// the query point is inside the polygon referenced here. Mirrors
    /// Nominatim's insert_addresslines containment check at indexing.
    /// 0xFFFFFFFF means "no containing admin polygon found" — fall back
    /// to radius-only matching.
    parent_poly_id: u32,
}

// POI category metadata — loaded from poi_meta.json (written by builder)
struct PoiMeta {
    categories: std::collections::HashMap<u8, PoiCategoryMeta>,
}

#[derive(Deserialize)]
struct PoiCategoryMeta {
    name: String,
    #[serde(default = "default_ref_dist")]
    reference_distance: f64,
    #[serde(default)]
    max_distance: f64,
    #[serde(default = "default_importance")]
    default_importance: f64,
    // legacy field, ignored
    #[serde(default)]
    proximity: f64,
}

fn default_ref_dist() -> f64 { 100.0 }
fn default_importance() -> f64 { 5.0 }

impl PoiMeta {
    fn load(dir: &str) -> Self {
        let path = format!("{}/poi_meta.json", dir);
        if let Ok(data) = std::fs::read_to_string(&path) {
            if let Ok(raw) = serde_json::from_str::<std::collections::HashMap<String, PoiCategoryMeta>>(&data) {
                let categories = raw.into_iter()
                    .filter_map(|(k, v)| k.parse::<u8>().ok().map(|id| (id, v)))
                    .collect();
                return PoiMeta { categories };
            }
        }
        PoiMeta { categories: std::collections::HashMap::new() }
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

// --- Admin level → address rank mapping (Nominatim approach) ---

struct AdminLevelConfig {
    default: HashMap<u8, u8>,
    countries: HashMap<String, HashMap<u8, u8>>,
}

impl AdminLevelConfig {
    fn load() -> Self {
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

    fn to_rank(&self, country_code: &str, admin_level: u8) -> u8 {
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
fn place_type_to_field(pt: u8) -> Option<&'static str> {
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
fn rank_to_field(rank: u8) -> Option<&'static str> {
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

// Map an AdminPlaceType override to its Nominatim rank_address equivalent.
// Drives the nested-boundary rank adjustment so linked / place=tagged
// boundaries sit in the same rank hierarchy as admin_level-based ones.
// Ranks come from settings/address-levels.json default section.
fn place_type_to_rank(pt: u8) -> u8 {
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

// --- Index data ---

struct Index {
    geo_cells: Option<Mmap>,
    street_entries: Option<Mmap>,
    street_ways: Option<Mmap>,
    street_nodes: Option<Mmap>,
    addr_entries: Option<Mmap>,
    addr_points: Option<Mmap>,
    interp_entries: Option<Mmap>,
    interp_ways: Option<Mmap>,
    interp_nodes: Option<Mmap>,
    admin_cells: Mmap,
    admin_entries: Mmap,
    admin_polygons: Mmap,
    admin_vertices: Mmap,
    poi_cells: Option<Mmap>,
    poi_entries: Option<Mmap>,
    poi_records: Option<Mmap>,
    poi_vertices: Option<Mmap>,
    poi_meta: PoiMeta,
    place_nodes: Option<Mmap>,
    place_cells: Option<Mmap>,
    place_entries: Option<Mmap>,
    // Parent chain files (Nominatim-style address walk)
    way_parents: Option<Mmap>,    // u32 per way → smallest containing admin poly
    admin_parents: Option<Mmap>,  // u32 per poly → next-larger containing admin poly
    addr_postcodes: Option<Mmap>, // u32 per addr_point → postcode string (optional)
    way_postcodes: Option<Mmap>,  // u32 per way → postcode string from postal boundary
    // Separate postcode files (optional — postcodes omitted when absent)
    postal_polygons: Option<Mmap>,
    postal_vertices: Option<Mmap>,
    postcode_centroids: Option<Mmap>,
    postcode_centroid_cells: Option<Mmap>,
    postcode_centroid_entries: Option<Mmap>,
    admin_config: AdminLevelConfig,
    strings: Mmap,
    street_cell_level: u64,
    admin_cell_level: u64,
    max_distance_sq: f64,
}

const NO_DATA: u32 = 0xFFFFFFFF;

struct GeoCellOffsets {
    street: u32,
    addr: u32,
    interp: u32,
}

fn mmap_file(path: &str) -> Result<Mmap, String> {
    let file = File::open(path).map_err(|e| format!("Failed to open {}: {}", path, e))?;
    unsafe { Mmap::map(&file).map_err(|e| format!("Failed to mmap {}: {}", path, e)) }
}

fn mmap_file_optional(path: &str) -> Option<Mmap> {
    let file = File::open(path).ok()?;
    unsafe { Mmap::map(&file).ok() }
}

impl Index {
    fn load(dir: &str, street_cell_level: u64, admin_cell_level: u64, search_distance: f64) -> Result<Self, String> {
        // Distance budget used by the primary-feature selection. The
        // internal distances are stored in radians² (via `to_radians()`
        // before squaring), so convert metres → radians → radians².
        // Earth radius is 6 371 000 m.
        let radians = search_distance / 6_371_000.0;
        let max_distance_sq = radians * radians;

        let geo_cells = mmap_file_optional(&format!("{}/geo_cells.bin", dir));
        let has_geo = geo_cells.is_some();

        let required_geo = |name: &str| -> Result<Option<Mmap>, String> {
            let path = format!("{}/{}", dir, name);
            let mmap = mmap_file_optional(&path);
            if has_geo && mmap.is_none() {
                return Err(format!("Partial geo index: {} is missing", name));
            }
            Ok(mmap)
        };

        Ok(Index {
            geo_cells,
            street_entries: required_geo("street_entries.bin")?,
            street_ways: required_geo("street_ways.bin")?,
            street_nodes: required_geo("street_nodes.bin")?,
            addr_entries: mmap_file_optional(&format!("{}/addr_entries.bin", dir)),
            addr_points: mmap_file_optional(&format!("{}/addr_points.bin", dir)),
            interp_entries: mmap_file_optional(&format!("{}/interp_entries.bin", dir)),
            interp_ways: mmap_file_optional(&format!("{}/interp_ways.bin", dir)),
            interp_nodes: mmap_file_optional(&format!("{}/interp_nodes.bin", dir)),
            admin_cells: mmap_file(&format!("{}/admin_cells.bin", dir))?,
            admin_entries: mmap_file(&format!("{}/admin_entries.bin", dir))?,
            admin_polygons: mmap_file(&format!("{}/admin_polygons.bin", dir))?,
            admin_vertices: mmap_file(&format!("{}/admin_vertices.bin", dir))?,
            poi_cells: mmap_file_optional(&format!("{}/poi_cells.bin", dir)),
            poi_entries: mmap_file_optional(&format!("{}/poi_entries.bin", dir)),
            poi_records: mmap_file_optional(&format!("{}/poi_records.bin", dir)),
            poi_vertices: mmap_file_optional(&format!("{}/poi_vertices.bin", dir)),
            poi_meta: PoiMeta::load(dir),
            place_nodes: mmap_file_optional(&format!("{}/place_nodes.bin", dir)),
            place_cells: mmap_file_optional(&format!("{}/place_cells.bin", dir)),
            place_entries: mmap_file_optional(&format!("{}/place_entries.bin", dir)),
            way_parents: mmap_file_optional(&format!("{}/way_parents.bin", dir)),
            admin_parents: mmap_file_optional(&format!("{}/admin_parents.bin", dir)),
            addr_postcodes: mmap_file_optional(&format!("{}/addr_postcodes.bin", dir)),
            way_postcodes: mmap_file_optional(&format!("{}/way_postcodes.bin", dir)),
            postal_polygons: mmap_file_optional(&format!("{}/postal_polygons.bin", dir)),
            postal_vertices: mmap_file_optional(&format!("{}/postal_vertices.bin", dir)),
            postcode_centroids: mmap_file_optional(&format!("{}/postcode_centroids.bin", dir)),
            postcode_centroid_cells: mmap_file_optional(&format!("{}/postcode_centroid_cells.bin", dir)),
            postcode_centroid_entries: mmap_file_optional(&format!("{}/postcode_centroid_entries.bin", dir)),
            admin_config: AdminLevelConfig::load(),
            strings: mmap_file(&format!("{}/strings.bin", dir))?,
            street_cell_level,
            admin_cell_level,
            max_distance_sq,
        })
    }

    fn get_string(&self, offset: u32) -> &str {
        let bytes = &self.strings[offset as usize..];
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        std::str::from_utf8(&bytes[..end]).unwrap_or("")
    }

    fn read_u16(data: &[u8], offset: usize) -> u16 {
        u16::from_le_bytes([data[offset], data[offset + 1]])
    }

    fn read_u32(data: &[u8], offset: usize) -> u32 {
        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
    }

    fn read_u64(data: &[u8], offset: usize) -> u64 {
        u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
    }

    // Iterate entry IDs inline from entries file at given offset
    fn for_each_entry(entries: &[u8], offset: u32, mut f: impl FnMut(u32)) {
        if offset == NO_DATA { return; }
        let offset = offset as usize;
        if offset + 2 > entries.len() { return; }

        let id_count = Self::read_u16(entries, offset) as usize;
        let data_start = offset + 2;
        if data_start + id_count * 4 > entries.len() { return; }

        for i in 0..id_count {
            f(Self::read_u32(entries, data_start + i * 4));
        }
    }

    // Binary search geo cell index: 20 bytes per entry (u64 cell_id + u32 street + u32 addr + u32 interp)
    fn lookup_geo_cell(cells: &[u8], cell_id: u64) -> GeoCellOffsets {
        let entry_size: usize = 20;
        let count = cells.len() / entry_size;
        let empty = GeoCellOffsets { street: NO_DATA, addr: NO_DATA, interp: NO_DATA };
        if count == 0 { return empty; }

        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_id = Self::read_u64(cells, mid * entry_size);
            if mid_id == cell_id {
                return GeoCellOffsets {
                    street: Self::read_u32(cells, mid * entry_size + 8),
                    addr: Self::read_u32(cells, mid * entry_size + 12),
                    interp: Self::read_u32(cells, mid * entry_size + 16),
                };
            } else if mid_id < cell_id {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        empty
    }

    // Binary search admin cell index: 12 bytes per entry (u64 cell_id + u32 offset)
    fn lookup_admin_cell(cells: &[u8], cell_id: u64) -> u32 {
        let entry_size: usize = 12;
        let count = cells.len() / entry_size;
        if count == 0 { return NO_DATA; }

        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_id = Self::read_u64(cells, mid * entry_size);
            if mid_id == cell_id {
                return Self::read_u32(cells, mid * entry_size + 8);
            } else if mid_id < cell_id {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        NO_DATA
    }

    // --- Geo lookup (streets, addresses, interpolation from merged index) ---

    fn query_geo(&self, lat: f64, lng: f64)
        -> (Option<(f64, &AddrPoint)>, Option<(f64, &str, u32)>, Option<(f64, &WayHeader)>)
    {
        let geo_cells = match &self.geo_cells {
            Some(gc) => gc,
            None => return (None, None, None),
        };
        let street_entries = self.street_entries.as_ref().unwrap();
        let street_ways_mmap = self.street_ways.as_ref().unwrap();
        let street_nodes_mmap = self.street_nodes.as_ref().unwrap();
        let cell = cell_id_at_level(lat, lng, self.street_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.street_cell_level);

        let empty_addr: &[AddrPoint] = &[];
        let all_points: &[AddrPoint] = if let Some(ref m) = self.addr_points {
            unsafe {
                std::slice::from_raw_parts(
                    m.as_ptr() as *const AddrPoint,
                    m.len() / std::mem::size_of::<AddrPoint>(),
                )
            }
        } else {
            empty_addr
        };
        let all_ways: &[WayHeader] = unsafe {
            std::slice::from_raw_parts(
                street_ways_mmap.as_ptr() as *const WayHeader,
                street_ways_mmap.len() / std::mem::size_of::<WayHeader>(),
            )
        };
        let all_street_nodes: &[NodeCoord] = unsafe {
            std::slice::from_raw_parts(
                street_nodes_mmap.as_ptr() as *const NodeCoord,
                street_nodes_mmap.len() / std::mem::size_of::<NodeCoord>(),
            )
        };
        let empty_interp: &[InterpWay] = &[];
        let all_interps: &[InterpWay] = if let Some(ref m) = self.interp_ways {
            unsafe {
                std::slice::from_raw_parts(
                    m.as_ptr() as *const InterpWay,
                    m.len() / std::mem::size_of::<InterpWay>(),
                )
            }
        } else {
            empty_interp
        };
        let empty_interp_nodes: &[NodeCoord] = &[];
        let all_interp_nodes: &[NodeCoord] = if let Some(ref m) = self.interp_nodes {
            unsafe {
                std::slice::from_raw_parts(
                    m.as_ptr() as *const NodeCoord,
                    m.len() / std::mem::size_of::<NodeCoord>(),
                )
            }
        } else {
            empty_interp_nodes
        };

        let cos_lat = lat.to_radians().cos();

        let mut best_addr_dist = f64::MAX;
        let mut best_addr: Option<&AddrPoint> = None;
        let mut best_street_dist = f64::MAX;
        let mut best_street: Option<&WayHeader> = None;
        let mut best_interp_dist = f64::MAX;
        let mut best_interp: Option<&InterpWay> = None;
        let mut best_interp_t: f64 = 0.0;

        // Fixed-size hash set on stack to skip duplicate street IDs across cells
        let mut seen_streets: [u32; 64] = [u32::MAX; 64];

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            let offsets = Self::lookup_geo_cell(geo_cells, c);

            // Addresses
            if let Some(ref addr_entries) = self.addr_entries {
                Self::for_each_entry(addr_entries, offsets.addr, |id| {
                    let idx = id as usize;
                    if idx >= all_points.len() { return; }
                    let point = &all_points[idx];
                    let dlat = (point.lat as f64 - lat).to_radians();
                    let dlng = (point.lng as f64 - lng).to_radians();
                    let dist = dist_sq(dlat, dlng, cos_lat);
                    if dist < best_addr_dist {
                        best_addr_dist = dist;
                        best_addr = Some(point);
                    }
                });
            }

            // Streets
            Self::for_each_entry(street_entries, offsets.street, |id| {
                let slot = (id as usize) & 0x3F;
                if seen_streets[slot] == id { return; }
                seen_streets[slot] = id;

                let way = &all_ways[id as usize];
                let offset = way.node_offset as usize;
                let count = way.node_count as usize;
                let nodes = &all_street_nodes[offset..offset + count];

                for i in 0..nodes.len() - 1 {
                    let dist = point_to_segment_distance(
                        lat, lng,
                        nodes[i].lat as f64, nodes[i].lng as f64,
                        nodes[i + 1].lat as f64, nodes[i + 1].lng as f64,
                        cos_lat,
                    );
                    if dist < best_street_dist {
                        best_street_dist = dist;
                        best_street = Some(way);
                    }
                }
            });

            // Interpolation
            if let Some(ref interp_entries) = self.interp_entries {
                Self::for_each_entry(interp_entries, offsets.interp, |id| {
                    let iw = &all_interps[id as usize];
                    if iw.start_number == 0 || iw.end_number == 0 { return; }

                    let offset = iw.node_offset as usize;
                    let count = iw.node_count as usize;
                    let nodes = &all_interp_nodes[offset..offset + count];

                    let mut total_len: f64 = 0.0;
                    for i in 0..nodes.len() - 1 {
                        let dlat = (nodes[i + 1].lat as f64 - nodes[i].lat as f64).to_radians();
                        let dlng = (nodes[i + 1].lng as f64 - nodes[i].lng as f64).to_radians();
                        total_len += dist_sq(dlat, dlng, cos_lat);
                    }
                    if total_len == 0.0 { return; }

                    let mut best_seg_dist = f64::MAX;
                    let mut best_seg_t: f64 = 0.0;
                    let mut prev_accumulated: f64 = 0.0;

                    for i in 0..nodes.len() - 1 {
                        let dlat = (nodes[i + 1].lat as f64 - nodes[i].lat as f64).to_radians();
                        let dlng = (nodes[i + 1].lng as f64 - nodes[i].lng as f64).to_radians();
                        let seg_len = dist_sq(dlat, dlng, cos_lat);
                        let (dist, seg_t) = point_to_segment_with_t(
                            lat, lng,
                            nodes[i].lat as f64, nodes[i].lng as f64,
                            nodes[i + 1].lat as f64, nodes[i + 1].lng as f64,
                            cos_lat,
                        );
                        if dist < best_seg_dist {
                            best_seg_dist = dist;
                            best_seg_t = (prev_accumulated + seg_t * seg_len) / total_len;
                        }
                        prev_accumulated += seg_len;
                    }

                    if best_seg_dist < best_interp_dist {
                        best_interp_dist = best_seg_dist;
                        best_interp = Some(iw);
                        best_interp_t = best_seg_t;
                    }
                });
            }
        }

        let addr_result = best_addr.map(|p| (best_addr_dist, p));
        let street_result = best_street.map(|w| (best_street_dist, w));
        let interp_result = best_interp.map(|iw| {
            let start = iw.start_number as f64;
            let end = iw.end_number as f64;
            let raw = start + best_interp_t * (end - start);

            let step: u32 = match iw.interpolation {
                1 | 2 => 2,
                _ => 1,
            };

            let number = if step == 2 {
                let base = iw.start_number;
                let offset = ((raw - base as f64) / step as f64).round() as u32 * step;
                base + offset
            } else {
                raw.round() as u32
            };

            (best_interp_dist, self.get_string(iw.street_id), number)
        });

        (addr_result, interp_result, street_result)
    }

    // Debug helper — returns primary feature distances + names.
    fn debug_primary(&self, lat: f64, lng: f64) -> serde_json::Value {
        let (addr, interp, street) = self.query_geo(lat, lng);
        let poi_primary = self.find_nearest_poi_with_parent(lat, lng);
        let to_m = |d2: f64| (d2.sqrt() * 6_371_000.0) as f64;
        serde_json::json!({
            "addr": addr.map(|(d, p)| serde_json::json!({
                "dist_m": to_m(d),
                "house_number": self.get_string(p.housenumber_id),
                "street": self.get_string(p.street_id),
                "lat": p.lat, "lng": p.lng,
            })),
            "street": street.map(|(d, w)| serde_json::json!({
                "dist_m": to_m(d),
                "name": self.get_string(w.name_id),
            })),
            "interp": interp.map(|(d, s, n)| serde_json::json!({
                "dist_m": to_m(d),
                "street": s,
                "number": n,
            })),
            "poi": poi_primary.map(|(d, p)| serde_json::json!({
                "dist_m": to_m(d),
                "name": self.get_string(p.name_id),
                "parent_street": if p.parent_street_id != NO_DATA { self.get_string(p.parent_street_id) } else { "" },
            })),
        })
    }

    // --- Admin boundary lookup (point-in-polygon) ---

    fn debug_admin(&self, lat: f64, lng: f64) -> serde_json::Value {
        let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.admin_cell_level);

        let all_polygons: &[AdminPolygon] = unsafe {
            std::slice::from_raw_parts(
                self.admin_polygons.as_ptr() as *const AdminPolygon,
                self.admin_polygons.len() / std::mem::size_of::<AdminPolygon>(),
            )
        };
        let all_vertices: &[NodeCoord] = unsafe {
            std::slice::from_raw_parts(
                self.admin_vertices.as_ptr() as *const NodeCoord,
                self.admin_vertices.len() / std::mem::size_of::<NodeCoord>(),
            )
        };

        const INTERIOR_FLAG: u32 = 0x80000000;
        const ID_MASK: u32 = 0x7FFFFFFF;

        // Mirror find_admin's best_by_level logic with full logging.
        let mut best_by_level: [Option<(f32, &AdminPolygon, bool)>; 12] = [None; 12];
        let mut seen_in_level: [Vec<serde_json::Value>; 12] = Default::default();

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            Self::for_each_entry(&self.admin_entries, Self::lookup_admin_cell(&self.admin_cells, c), |id| {
                let is_interior = (id & INTERIOR_FLAG) != 0;
                let poly_id = (id & ID_MASK) as usize;
                if poly_id >= all_polygons.len() { return; }
                let poly = &all_polygons[poly_id];
                let level = poly.admin_level as usize;
                if level >= 12 { return; }
                if poly.area <= 0.0 { return; }

                let offset = poly.vertex_offset as usize;
                let count = poly.vertex_count as usize;
                if offset + count > all_vertices.len() { return; }
                let verts = &all_vertices[offset..offset + count];
                let pip = point_in_polygon(lat as f32, lng as f32, verts);

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

    fn find_admin(&self, lat: f64, lng: f64) -> AdminResult<'_> {
        let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.admin_cell_level);

        let all_polygons: &[AdminPolygon] = unsafe {
            std::slice::from_raw_parts(
                self.admin_polygons.as_ptr() as *const AdminPolygon,
                self.admin_polygons.len() / std::mem::size_of::<AdminPolygon>(),
            )
        };
        let all_vertices: &[NodeCoord] = unsafe {
            std::slice::from_raw_parts(
                self.admin_vertices.as_ptr() as *const NodeCoord,
                self.admin_vertices.len() / std::mem::size_of::<NodeCoord>(),
            )
        };

        // For each admin level, find the smallest-area polygon actually containing
        // the point. Interior flags are used as a fast path for uncontested cells,
        // but PIP is always run when a polygon would win the smallest-area contest
        // (interior flags can be wrong at polygon borders, e.g. NJ/NY).
        const INTERIOR_FLAG: u32 = 0x80000000;
        const ID_MASK: u32 = 0x7FFFFFFF;

        let mut best_by_level: [Option<(f32, &AdminPolygon, bool)>; 12] = [None; 12]; // (area, poly, pip_verified)
        // Class='place' polygons (stored by our builder at admin_level=15 as a
        // marker). These are boundary=place / multipolygon+place=* entries —
        // Nominatim's second-check nested adjustment (placex_triggers.sql
        // lines 944-963) uses only these, NOT admin boundaries with
        // linked_place tags. Tracked separately so we can apply rule #3
        // only for genuine class='place' polygons — see Buenos Aires
        // Comuna 1 where L8 Buenos Aires (boundary + linked_place=city)
        // would otherwise incorrectly promote L5 Comuna 1 to rank 18.
        let mut place_area_polygons: Vec<&AdminPolygon> = Vec::new();

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            Self::for_each_entry(&self.admin_entries, Self::lookup_admin_cell(&self.admin_cells, c), |id| {
                let is_interior = (id & INTERIOR_FLAG) != 0;
                let poly_id = (id & ID_MASK) as usize;
                if poly_id >= all_polygons.len() { return; }
                let poly = &all_polygons[poly_id];
                let level = poly.admin_level as usize;
                if level >= 12 { return; }
                if poly.area <= 0.0 { return; }

                let offset = poly.vertex_offset as usize;
                let count = poly.vertex_count as usize;
                if offset + count > all_vertices.len() { return; }
                let verts = &all_vertices[offset..offset + count];

                if let Some((best_area, _, best_verified)) = best_by_level[level] {
                    if poly.area >= best_area {
                        // Larger than current best — skip unless current best is unverified
                        // and this one can be verified
                        if !best_verified {
                            // Current best only passed via interior flag — verify it now
                            let (_, best_poly, _) = best_by_level[level].unwrap();
                            let boff = best_poly.vertex_offset as usize;
                            let bcnt = best_poly.vertex_count as usize;
                            if point_in_polygon(lat as f32, lng as f32, &all_vertices[boff..boff + bcnt]) {
                                best_by_level[level] = Some((best_area, best_poly, true));
                            } else {
                                // Best was wrong — try this polygon instead
                                let pip = point_in_polygon(lat as f32, lng as f32, verts);
                                if is_interior || pip {
                                    best_by_level[level] = Some((poly.area, poly, pip));
                                } else {
                                    best_by_level[level] = None;
                                }
                            }
                        }
                        return;
                    }
                    // This polygon is smaller — must verify with PIP
                    let pip = point_in_polygon(lat as f32, lng as f32, verts);
                    if pip {
                        best_by_level[level] = Some((poly.area, poly, true));
                    }
                    // If PIP fails, keep existing best
                } else {
                    // No existing candidate — accept with interior flag, verify later if contested
                    if is_interior {
                        best_by_level[level] = Some((poly.area, poly, false));
                    } else if point_in_polygon(lat as f32, lng as f32, verts) {
                        best_by_level[level] = Some((poly.area, poly, true));
                    }
                }
            });
        }

        // Verify any uncontested winners that only passed via interior flag
        for level in 0..12 {
            if let Some((area, poly, false)) = best_by_level[level] {
                let offset = poly.vertex_offset as usize;
                let count = poly.vertex_count as usize;
                if point_in_polygon(lat as f32, lng as f32, &all_vertices[offset..offset + count]) {
                    best_by_level[level] = Some((area, poly, true));
                } else {
                    best_by_level[level] = None;
                }
            }
        }

        // Second pass: collect class='place' polygons (admin_level=15 in our
        // builder's schema) that cover the query point. These drive the
        // second-nested-check rule (placex_triggers.sql lines 944-963) —
        // Nominatim uses only these, not admin boundaries with linked_place
        // tags. Kept in a separate vec so we don't disturb best_by_level[].
        let neighbors2 = cell_neighbors_at_level(cell, self.admin_cell_level);
        for c in std::iter::once(cell).chain(neighbors2.into_iter()) {
            Self::for_each_entry(&self.admin_entries, Self::lookup_admin_cell(&self.admin_cells, c), |id| {
                let poly_id = (id & ID_MASK) as usize;
                if poly_id >= all_polygons.len() { return; }
                let poly = &all_polygons[poly_id];
                if poly.admin_level != 15 { return; }
                if poly.area <= 0.0 { return; }
                let offset = poly.vertex_offset as usize;
                let count = poly.vertex_count as usize;
                if offset + count > all_vertices.len() { return; }
                let verts = &all_vertices[offset..offset + count];
                if !point_in_polygon(lat as f32, lng as f32, verts) { return; }
                if !place_area_polygons.iter().any(|p| std::ptr::eq(*p, poly)) {
                    place_area_polygons.push(poly);
                }
            });
        }

        let mut result = AdminResult::default();

        // Get country code from level 2 polygon
        let country_code_str: String = best_by_level[2]
            .and_then(|(_, poly, _)| {
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

        // Map admin levels to address fields using country-specific config
        // Track best (smallest area) polygon per output field
        struct FieldCandidate<'b> {
            area: f32,
            name: &'b str,
            country_code: u16,
        }
        let mut best_by_field: [Option<FieldCandidate<'_>>; 19] = Default::default();
        // 0 country 1 state 2 province 3 region 4 state_district
        // 5 county 6 district 7 municipality
        // 8 city 9 town 10 village 11 hamlet
        // 12 city_district 13 borough 14 suburb 15 quarter 16 neighbourhood
        // 17 city_block

        let field_index = |field: &str| -> Option<usize> {
            match field {
                "country" => Some(0),
                "state" => Some(1),
                "province" => Some(2),
                "region" => Some(3),
                "state_district" => Some(4),
                "county" => Some(5),
                "district" => Some(6),
                "municipality" => Some(7),
                "city" => Some(8),
                "town" => Some(9),
                "village" => Some(10),
                "hamlet" => Some(11),
                "city_district" => Some(12),
                "borough" => Some(13),
                "suburb" => Some(14),
                "quarter" => Some(15),
                "neighbourhood" => Some(16),
                "city_block" => Some(17),
                _ => None,
            }
        };

        // Collect polygons at this point with their effective rank_address.
        // Process in admin_level order (ascending) so that each polygon's
        // parent is the previously-processed polygon with the highest
        // admin_level strictly less than self — matching Nominatim's
        // placex_triggers.sql nested-boundary rank adjustment.
        struct RankedPoly<'b> {
            admin_level: u8,
            rank_address: u8,
            place_type_override: u8,
            name: &'b str,
            area: f32,
            country_code: u16,
        }
        let mut ranked: Vec<RankedPoly<'_>> = Vec::new();
        for level in 0..12 {
            if let Some((_, poly, _)) = best_by_level[level] {
                if poly.admin_level == 11 { continue; } // postal code, handled separately

                let initial_rank = self.admin_config.to_rank(&country_code_str, poly.admin_level);
                if initial_rank == 0 && poly.place_type_override == 0 { continue; }

                ranked.push(RankedPoly {
                    admin_level: poly.admin_level,
                    rank_address: initial_rank,
                    place_type_override: poly.place_type_override,
                    name: self.get_string(poly.name_id),
                    area: poly.area,
                    country_code: poly.country_code,
                });
            }
        }

        // Add class='place' polygons (our admin_level=15 marker) to
        // the ranked list. These come from closed ways / relations
        // tagged `place=neighbourhood / quarter / suburb / borough`
        // (the closed-way extraction path in the builder) — Nominatim
        // indexes them as rank_address 20-22 and they fill the
        // neighbourhood / quarter / suburb fields. Fixes cairo
        // qasr al doubara (place=neighbourhood closed way).
        //
        // Pick the smallest polygon per name to avoid duplicates
        // when multiple overlapping place areas share a name.
        for poly in &place_area_polygons {
            if poly.place_type_override == 0 { continue; }
            let r = place_type_to_rank(poly.place_type_override);
            if r == 0 { continue; }
            // Skip if a smaller polygon with the same name already exists.
            let name = self.get_string(poly.name_id);
            if ranked.iter().any(|p| p.name == name && p.area <= poly.area) {
                continue;
            }
            ranked.push(RankedPoly {
                admin_level: 15,
                rank_address: r,
                place_type_override: poly.place_type_override,
                name,
                area: poly.area,
                country_code: poly.country_code,
            });
        }

        // Sort by admin_level ascending. Ties (same admin_level) broken by
        // area descending (the larger polygon is assumed to be the parent).
        ranked.sort_by(|a, b| {
            a.admin_level.cmp(&b.admin_level)
                .then(b.area.partial_cmp(&a.area).unwrap_or(std::cmp::Ordering::Equal))
        });

        // Nominatim's nested-boundary rank adjustment (placex_triggers.sql
        // lines 910-1175) applies two independent promotions in order:
        //
        //   1. Admin nested: if a parent admin boundary (admin_level > 3
        //      AND admin_level < self) has rank_address >= self, push
        //      self to parent_rank + 2.
        //   2. Linked place override: if the linked place's rank >
        //      parent_address_level (the same "parent" computed above),
        //      replace self rank with the linked place rank.
        //   3. Place area containment: if a class='place' area (e.g.
        //      R9185096 "Nairobi" with place=city) with rank >= self
        //      contains self, push self to place_area_rank + 2.
        //
        // We approximate class='place' polygons as any polygon with a
        // non-zero place_type_override — that's the set the builder
        // populates from place=X and linked_place=X tags. Because every
        // polygon in best_by_level contains the query point, we use
        // (area > self.area) as a proxy for "contains self".
        //
        // Order matters: admin nested must run before the place-area
        // promotion for the ranks to stabilise. We iterate twice to let
        // rank changes cascade (Starehe L6 getting bumped to 18 then
        // promoting CBD division L7 from 14 to 20).
        // Combined rank adjustment. Applies Nominatim's three rules in
        // sequence on each polygon, iterating 3 times so the cascade can
        // propagate through admin and place parents alike:
        //   1. Admin nested: push to parent_rank + 2 if parent admin's
        //      rank >= self (parent = latest earlier polygon with
        //      admin_level in (3, self.admin_level)).
        //   2. Linked place override: if self has a place_type tag, use
        //      its rank_address (only if > admin_parent_rank and > self).
        //   3. Place area containment: push to place_rank + 2 if any
        //      larger polygon with a place_type_override (a "place area"
        //      class='place' in Nominatim terms) has rank >= self.
        //
        // Only polygons without their own place_type_override are eligible
        // for place-area promotion (rule 3) — a Paris L6 boundary with
        // linked_place=city should stay at rank 16 even though it's
        // nested inside a larger polygon with an override.
        for _ in 0..3 {
            for i in 0..ranked.len() {
                let self_al = ranked[i].admin_level;
                let self_area = ranked[i].area;
                let self_override = ranked[i].place_type_override;

                // Step 1: admin nested.
                let mut admin_parent_rank: u8 = 0;
                if self_al > 4 {
                    for j in (0..i).rev() {
                        let al = ranked[j].admin_level;
                        if al > 3 && al < self_al {
                            admin_parent_rank = ranked[j].rank_address;
                            break;
                        }
                    }
                }
                if ranked[i].rank_address <= admin_parent_rank {
                    ranked[i].rank_address = admin_parent_rank + 2;
                }

                // Step 2: linked place override.
                if self_override > 0 {
                    let linked_rank = place_type_to_rank(self_override);
                    if linked_rank > admin_parent_rank && linked_rank > ranked[i].rank_address {
                        ranked[i].rank_address = linked_rank;
                    }
                }

                // Step 3: place area containment — only for polygons
                // without their own place_type_override. Uses the genuine
                // class='place' polygons collected above. Admin boundaries
                // with linked_place tags (Nairobi L3, BA L8, Paris L6)
                // are NOT used here — Nominatim's SQL explicitly
                // restricts this loop to class='place'.
                if self_override == 0 {
                    let mut place_parent_rank: u8 = 0;
                    for pa in &place_area_polygons {
                        if pa.place_type_override == 0 { continue; }
                        if pa.area <= self_area { continue; }
                        let pa_rank = place_type_to_rank(pa.place_type_override);
                        if pa_rank < ranked[i].rank_address { continue; }
                        if pa_rank > place_parent_rank {
                            place_parent_rank = pa_rank;
                        }
                    }
                    if place_parent_rank > 0 && place_parent_rank >= ranked[i].rank_address {
                        ranked[i].rank_address = place_parent_rank + 2;
                    }
                }
            }
        }

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
        for r in &ranked {
            if r.admin_level == 15 { continue; } // skip place-area in first pass
            let field = if r.place_type_override > 0 {
                place_type_to_field(r.place_type_override)
            } else {
                rank_to_field(r.rank_address)
            };
            if let Some(field) = field {
                if let Some(idx) = field_index(field) {
                    if let Some(ref existing) = best_by_field[idx] {
                        if r.area >= existing.area { continue; }
                    }
                    best_by_field[idx] = Some(FieldCandidate {
                        area: r.area, name: r.name, country_code: r.country_code
                    });
                }
            }
        }
        // Second pass: place-area polygons fill only empty fields.
        for r in &ranked {
            if r.admin_level != 15 { continue; }
            let field = if r.place_type_override > 0 {
                place_type_to_field(r.place_type_override)
            } else {
                rank_to_field(r.rank_address)
            };
            if let Some(field) = field {
                if let Some(idx) = field_index(field) {
                    if best_by_field[idx].is_none() {
                        best_by_field[idx] = Some(FieldCandidate {
                            area: r.area, name: r.name, country_code: r.country_code
                        });
                    }
                }
            }
        }

        // Fill result
        if let Some(ref c) = best_by_field[0] {
            result.country = Some(c.name);
            if c.country_code != 0 {
                result.country_code = Some([
                    (c.country_code >> 8) as u8,
                    (c.country_code & 0xFF) as u8,
                ]);
            }
        }
        result.state          = best_by_field[1].as_ref().map(|c| c.name);
        result.province       = best_by_field[2].as_ref().map(|c| c.name);
        result.region         = best_by_field[3].as_ref().map(|c| c.name);
        result.state_district = best_by_field[4].as_ref().map(|c| c.name);
        result.county         = best_by_field[5].as_ref().map(|c| c.name);
        result.district       = best_by_field[6].as_ref().map(|c| c.name);
        result.municipality   = best_by_field[7].as_ref().map(|c| c.name);
        result.city           = best_by_field[8].as_ref().map(|c| c.name);
        result.town           = best_by_field[9].as_ref().map(|c| c.name);
        result.village        = best_by_field[10].as_ref().map(|c| c.name);
        result.hamlet         = best_by_field[11].as_ref().map(|c| c.name);
        result.city_district  = best_by_field[12].as_ref().map(|c| c.name);
        result.borough        = best_by_field[13].as_ref().map(|c| c.name);
        result.suburb         = best_by_field[14].as_ref().map(|c| c.name);
        result.quarter        = best_by_field[15].as_ref().map(|c| c.name);
        result.neighbourhood  = best_by_field[16].as_ref().map(|c| c.name);
        result.city_block     = best_by_field[17].as_ref().map(|c| c.name);

        result
    }

    // --- Place node lookup (nearest city/town/village/suburb) ---

    // Find the smallest admin polygon at admin_level >= 8 (city/suburb
    // level) that contains the query point. This is Nominatim's
    // `current_boundary` used in the cascading containment gate for
    // place nodes (placex_triggers.sql lines 597-600, 627-635).
    fn find_current_boundary(&self, lat: f64, lng: f64) -> Option<&AdminPolygon> {
        let all_polygons: &[AdminPolygon] = unsafe {
            std::slice::from_raw_parts(
                self.admin_polygons.as_ptr() as *const AdminPolygon,
                self.admin_polygons.len() / std::mem::size_of::<AdminPolygon>(),
            )
        };
        let all_vertices: &[NodeCoord] = unsafe {
            std::slice::from_raw_parts(
                self.admin_vertices.as_ptr() as *const NodeCoord,
                self.admin_vertices.len() / std::mem::size_of::<NodeCoord>(),
            )
        };
        let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.admin_cell_level);

        const ID_MASK: u32 = 0x7FFFFFFF;
        let mut best: Option<&AdminPolygon> = None;
        let mut best_area: f32 = f32::MAX;

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            Self::for_each_entry(&self.admin_entries, Self::lookup_admin_cell(&self.admin_cells, c), |id| {
                let poly_id = (id & ID_MASK) as usize;
                if poly_id >= all_polygons.len() { return; }
                let poly = &all_polygons[poly_id];
                // Only city/suburb level and deeper (admin_level >= 8)
                // Matches Nominatim's current_boundary which gets
                // updated for rank_address != 11 AND rank_address != 5
                if poly.admin_level < 8 || poly.admin_level > 10 { return; }
                if poly.area >= best_area { return; }
                let offset = poly.vertex_offset as usize;
                let count = poly.vertex_count as usize;
                if offset + count > all_vertices.len() { return; }
                let verts = &all_vertices[offset..offset + count];
                if point_in_polygon(lat as f32, lng as f32, verts) {
                    best_area = poly.area;
                    best = Some(poly);
                }
            });
        }
        best
    }

    fn find_places(&self, lat: f64, lng: f64, current_boundary: Option<&AdminPolygon>) -> PlaceResult<'_> {
        let place_cells = match &self.place_cells {
            Some(c) => c,
            None => return PlaceResult::default(),
        };
        let place_entries = match &self.place_entries {
            Some(e) => e,
            None => return PlaceResult::default(),
        };
        let place_nodes_mmap = match &self.place_nodes {
            Some(n) => n,
            None => return PlaceResult::default(),
        };

        let all_places: &[PlaceNode] = unsafe {
            std::slice::from_raw_parts(
                place_nodes_mmap.as_ptr() as *const PlaceNode,
                place_nodes_mmap.len() / std::mem::size_of::<PlaceNode>(),
            )
        };

        // Place nodes are indexed at admin cell level (L10, ~10km cells).
        // Cell + 8 neighbors covers ~30km — sufficient for all place types.
        let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.admin_cell_level);

        // For each place type, find the nearest node
        // 0=city, 1=town, 2=village, 3=suburb, 4=hamlet, 5=neighbourhood, 6=quarter
        let mut best: [Option<(f64, &PlaceNode)>; 7] = [None; 7];
        let cos_lat = lat.to_radians().cos();

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            Self::for_each_entry(place_entries, Self::lookup_admin_cell(place_cells, c), |id| {
                let place_id = id as usize;
                if place_id >= all_places.len() { return; }
                let pn = &all_places[place_id];
                let pt = pn.place_type as usize;
                if pt >= 7 { return; }

                let dlat = (lat - pn.lat as f64).to_radians();
                let dlng = (lng - pn.lng as f64).to_radians();
                let dist_sq = dlat * dlat + dlng * dlng * cos_lat * cos_lat;

                if let Some((best_dist, _)) = best[pt] {
                    if dist_sq >= best_dist { return; }
                }
                best[pt] = Some((dist_sq, pn));
            });
        }

        // Convert to result — find best city-like place (city > town > village)
        let mut result = PlaceResult::default();

        // Max search radii. Nominatim's `reverse_place_diameter`
        // (lib-sql/functions/ranking.sql) is an import-time bound
        // for the addressline trigger. At query time it filters by
        // parent_place_id chain membership, which is much stricter
        // than the bare radius. We can't emulate that so we use
        // tighter radii — wide enough for the correct quarter/
        // neighbourhood to win, tight enough to not inject extras.
        // Per-rank search radii matching Nominatim's
        // reverse_place_diameter (lib-sql/functions/ranking.sql):
        //   rank_search ≤ 17 (city):  0.16 deg
        //   rank_search = 18 (town):  0.08 deg
        //   rank_search = 19 (village): 0.04 deg
        //   rank_search ≥ 20 (suburb/hamlet/quarter/neighbourhood): 0.02 deg
        let max_city = (0.16_f64).to_radians().powi(2);
        let max_town = (0.08_f64).to_radians().powi(2);
        let max_village = (0.04_f64).to_radians().powi(2);
        let max_rank20 = (0.02_f64).to_radians().powi(2);

        // Nominatim's cascading containment gate (placex_triggers.sql
        // lines 597-600): place nodes (isguess=true) are rejected if
        // their centroid is NOT inside `current_boundary` — the
        // geometry of the last accepted non-guess admin boundary.
        //
        // We implement this by PIP-testing the PLACE NODE's lat/lng
        // against `current_boundary` (the smallest accepted admin
        // polygon from find_admin). This is the correct Nominatim
        // check: the NODE must be inside the boundary, not the query.
        let all_vertices: &[NodeCoord] = unsafe {
            std::slice::from_raw_parts(
                self.admin_vertices.as_ptr() as *const NodeCoord,
                self.admin_vertices.len() / std::mem::size_of::<NodeCoord>(),
            )
        };
        let node_inside_boundary = |pn: &PlaceNode| -> bool {
            match current_boundary {
                None => true, // no admin boundary found — accept all
                Some(poly) => {
                    let off = poly.vertex_offset as usize;
                    let cnt = poly.vertex_count as usize;
                    if off + cnt > all_vertices.len() { return true; }
                    let verts = &all_vertices[off..off + cnt];
                    // PIP test the PLACE NODE's centroid, not the query point
                    point_in_polygon(pn.lat, pn.lng, verts)
                }
            }
        };

        // City / town / village: no containment gate — the query may
        // sit inside the city's area while the place=city node itself
        // is at the city centre, potentially outside any L9/L10 polygon
        // our builder would pick as a parent.
        for pt in [0, 1, 2] {
            if let Some((dist_sq, pn)) = best[pt] {
                let threshold = match pt {
                    0 => max_city,    // city: 0.16 deg
                    1 => max_town,    // town: 0.08 deg
                    _ => max_village, // village: 0.04 deg
                };
                if dist_sq <= threshold {
                    let name = self.get_string(pn.name_id);
                    match pt {
                        0 => result.city = Some(name),
                        1 => result.town = Some(name),
                        _ => result.village = Some(name),
                    }
                    break;
                }
            }
        }

        // Nominatim's address_havelevel dedup: only ONE entry per
        // rank_address. suburb(rank 20) and hamlet(rank 20) share the
        // same rank — pick the closer one. Same for quarter(rank 22)
        // and neighbourhood(rank 22). This matches insert_addresslines
        // line 603: `location_isaddress := not address_havelevel[rank]`.
        let suburb_valid = best[3].filter(|(d, pn)| *d <= max_rank20 && node_inside_boundary(pn));
        let hamlet_valid = best[4].filter(|(d, pn)| *d <= max_rank20 && node_inside_boundary(pn));
        // rank 20: closest of suburb/hamlet wins
        match (suburb_valid, hamlet_valid) {
            (Some((sd, sp)), Some((hd, hp))) => {
                if sd <= hd {
                    result.suburb = Some(self.get_string(sp.name_id));
                } else {
                    result.hamlet = Some(self.get_string(hp.name_id));
                }
            }
            (Some((_, pn)), None) => result.suburb = Some(self.get_string(pn.name_id)),
            (None, Some((_, pn))) => result.hamlet = Some(self.get_string(pn.name_id)),
            (None, None) => {}
        }

        let quarter_valid = best[6].filter(|(d, pn)| *d <= max_rank20 && node_inside_boundary(pn));
        let neigh_valid = best[5].filter(|(d, pn)| *d <= max_rank20 && node_inside_boundary(pn));
        // rank 22: closest of quarter/neighbourhood wins
        match (quarter_valid, neigh_valid) {
            (Some((qd, qp)), Some((nd, np))) => {
                if qd <= nd {
                    result.quarter = Some(self.get_string(qp.name_id));
                } else {
                    result.neighbourhood = Some(self.get_string(np.name_id));
                }
            }
            (Some((_, pn)), None) => result.quarter = Some(self.get_string(pn.name_id)),
            (None, Some((_, pn))) => result.neighbourhood = Some(self.get_string(pn.name_id)),
            (None, None) => {}
        }

        result
    }

    // Categories that are km-scale area labels rather than
    // addressable building-level features. Matches Nominatim's
    // exclusion of class_ IN ('place', 'building') from the
    // rank-30 POI branch in _find_closest_street_or_pois.
    //
    // See builder/src/types.h for the numeric category enum.
    fn is_area_label_category(cat: u8) -> bool {
        matches!(cat,
            // natural: large polygons
            80 | 81 | 82 | 86 | 91 | 92 | 93 | // peak, volcano, beach, glacier, bay, cape, island
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

    // --- POI primary-feature lookup ---
    //
    // Returns the nearest POI with a valid parent_street_id to the
    // query point. Used by query() as an additional primary-feature
    // candidate alongside the raw address / interpolation / street
    // lookups — mirrors Nominatim's _find_closest_street_or_pois
    // which returns the closest rank-26+ feature regardless of
    // whether it's a road or an addressable POI.
    //
    // For polygon POIs, returns distance 0 if the query is inside
    // the polygon, otherwise the distance to the nearest edge.
    fn find_nearest_poi_with_parent(&self, lat: f64, lng: f64) -> Option<(f64, &PoiRecord)> {
        let poi_cells = self.poi_cells.as_ref()?;
        let poi_entries = self.poi_entries.as_ref()?;
        let poi_records_mmap = self.poi_records.as_ref()?;

        let all_pois: &[PoiRecord] = unsafe {
            std::slice::from_raw_parts(
                poi_records_mmap.as_ptr() as *const PoiRecord,
                poi_records_mmap.len() / std::mem::size_of::<PoiRecord>(),
            )
        };
        let all_poi_vertices: &[NodeCoord] = match &self.poi_vertices {
            Some(v) => unsafe {
                std::slice::from_raw_parts(
                    v.as_ptr() as *const NodeCoord,
                    v.len() / std::mem::size_of::<NodeCoord>(),
                )
            },
            None => &[],
        };

        let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.admin_cell_level);

        const INTERIOR_FLAG: u32 = 0x80000000;
        const ID_MASK: u32 = 0x7FFFFFFF;

        let cos_lat = lat.to_radians().cos();
        let mut best_dist_sq = f64::MAX;
        let mut best_idx: Option<usize> = None;

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            Self::for_each_entry(poi_entries, Self::lookup_admin_cell(poi_cells, c), |id| {
                let is_interior = (id & INTERIOR_FLAG) != 0;
                let poi_id = (id & ID_MASK) as usize;
                if poi_id >= all_pois.len() { return; }
                let poi = &all_pois[poi_id];
                // Must have a name (for display) and a linked parent
                // street (for the `road` field). Without parent, we
                // can't populate a road name if this POI wins.
                if poi.name_id == NO_DATA || poi.parent_street_id == NO_DATA {
                    return;
                }
                // Nominatim's _find_closest_street_or_pois filter
                // excludes `class_ IN ('place', 'building')` from the
                // POI rank-30 branch. Our large "containment only"
                // categories (islands, bays, capes, airports, parks,
                // universities, cemeteries, etc.) would otherwise win
                // at distance 0 every time the query is inside one,
                // e.g. "Manhattan Island" covering all of Manhattan.
                // Skip categories that are typically km-scale and act
                // as area labels rather than addressable features.
                if Self::is_area_label_category(poi.category) {
                    return;
                }

                // Compute distance²: inside the polygon → 0;
                // otherwise min distance to any edge; for point POIs,
                // direct point-to-point distance.
                let dist_sq_val;
                if poi.vertex_count > 0 && (poi.vertex_offset as usize) < all_poi_vertices.len() {
                    let offset = poi.vertex_offset as usize;
                    let count = poi.vertex_count as usize;
                    if offset + count > all_poi_vertices.len() { return; }
                    let verts = &all_poi_vertices[offset..offset + count];
                    if is_interior || point_in_polygon(lat as f32, lng as f32, verts) {
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

                if dist_sq_val < best_dist_sq {
                    best_dist_sq = dist_sq_val;
                    best_idx = Some(poi_id);
                }
            });
        }

        best_idx.map(|i| (best_dist_sq, &all_pois[i]))
    }

    // --- POI lookup ---

    fn find_pois(&self, lat: f64, lng: f64) -> Vec<PoiMatch<'_>> {
        let poi_cells = match &self.poi_cells {
            Some(c) => c,
            None => return vec![],
        };
        let poi_entries = match &self.poi_entries {
            Some(e) => e,
            None => return vec![],
        };
        let poi_records_mmap = match &self.poi_records {
            Some(r) => r,
            None => return vec![],
        };

        let all_pois: &[PoiRecord] = unsafe {
            std::slice::from_raw_parts(
                poi_records_mmap.as_ptr() as *const PoiRecord,
                poi_records_mmap.len() / std::mem::size_of::<PoiRecord>(),
            )
        };
        let all_poi_vertices: &[NodeCoord] = match &self.poi_vertices {
            Some(v) => unsafe {
                std::slice::from_raw_parts(
                    v.as_ptr() as *const NodeCoord,
                    v.len() / std::mem::size_of::<NodeCoord>(),
                )
            },
            None => &[],
        };

        let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.admin_cell_level);

        const INTERIOR_FLAG: u32 = 0x80000000;
        const ID_MASK: u32 = 0x7FFFFFFF;

        let mut results: Vec<PoiMatch<'_>> = Vec::new();

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            Self::for_each_entry(poi_entries, Self::lookup_admin_cell(poi_cells, c), |id| {
                let is_interior = (id & INTERIOR_FLAG) != 0;
                let poi_id = (id & ID_MASK) as usize;
                if poi_id >= all_pois.len() { return; }
                let poi = &all_pois[poi_id];
                let name = self.get_string(poi.name_id);
                if name.is_empty() { return; }

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

                if poi.vertex_count > 0 && (poi.vertex_offset as usize) < all_poi_vertices.len() {
                    let offset = poi.vertex_offset as usize;
                    let count = poi.vertex_count as usize;
                    let verts = &all_poi_vertices[offset..offset + count];
                    contained = is_interior || point_in_polygon(lat as f32, lng as f32, verts);

                    if contained {
                        dist_m = 0.0;
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

                // Score with squared distance decay
                let proximity_weight = if contained {
                    1.0
                } else {
                    let r = dist_m / ref_dist;
                    1.0 / (1.0 + r * r)
                };
                let score = importance * proximity_weight;
                if score < 0.5 { return; }

                results.push(PoiMatch {
                    name,
                    category: self.poi_meta.category_name(poi.category),
                    distance_m: if contained { 0.0 } else { dist_m },
                    contained,
                    score,
                });
            });
        }

        // Sort by score descending
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        let mut seen = std::collections::HashSet::new();
        results.retain(|r| seen.insert(r.name));
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
    fn find_admin_from_chain(&self, start_poly_id: u32, lat: f64, lng: f64) -> AdminResult<'_> {
        let admin_parents = match &self.admin_parents {
            Some(m) => unsafe {
                std::slice::from_raw_parts(
                    m.as_ptr() as *const u32,
                    m.len() / 4,
                )
            },
            None => return self.find_admin(lat, lng),
        };
        let all_polygons: &[AdminPolygon] = unsafe {
            std::slice::from_raw_parts(
                self.admin_polygons.as_ptr() as *const AdminPolygon,
                self.admin_polygons.len() / std::mem::size_of::<AdminPolygon>(),
            )
        };

        // Collect chain: walk start → parent → parent → ...
        let mut chain_ids: Vec<u32> = Vec::with_capacity(12);
        let mut current = start_poly_id;
        for _ in 0..15 { // safety bound
            if current == NO_DATA || current as usize >= all_polygons.len() { break; }
            chain_ids.push(current);
            if current as usize >= admin_parents.len() { break; }
            current = admin_parents[current as usize];
        }

        // Also collect class='place' polygons (admin_level=15) via
        // PIP from the query point — these aren't in the parent chain
        // but provide place-area rank promotions.
        let all_vertices: &[NodeCoord] = unsafe {
            std::slice::from_raw_parts(
                self.admin_vertices.as_ptr() as *const NodeCoord,
                self.admin_vertices.len() / std::mem::size_of::<NodeCoord>(),
            )
        };
        let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.admin_cell_level);
        let mut place_area_polygons: Vec<&AdminPolygon> = Vec::new();
        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            Self::for_each_entry(&self.admin_entries, Self::lookup_admin_cell(&self.admin_cells, c), |id| {
                let poly_id = (id & 0x7FFFFFFF) as usize;
                if poly_id >= all_polygons.len() { return; }
                let poly = &all_polygons[poly_id];
                if poly.admin_level != 15 { return; }
                if poly.area <= 0.0 { return; }
                let offset = poly.vertex_offset as usize;
                let count = poly.vertex_count as usize;
                if offset + count > all_vertices.len() { return; }
                let verts = &all_vertices[offset..offset + count];
                if !point_in_polygon(lat as f32, lng as f32, verts) { return; }
                place_area_polygons.push(poly);
            });
        }

        // Now build the same ranked structure as find_admin
        let mut result = AdminResult::default();

        let country_code_str: String = chain_ids.iter()
            .find_map(|&pid| {
                let poly = &all_polygons[pid as usize];
                if poly.admin_level == 2 && poly.country_code != 0 {
                    Some(format!("{}{}",
                        (poly.country_code >> 8) as u8 as char,
                        (poly.country_code & 0xFF) as u8 as char
                    ).to_uppercase())
                } else { None }
            })
            .unwrap_or_default();

        struct RankedPoly2<'b> {
            admin_level: u8,
            rank_address: u8,
            place_type_override: u8,
            name: &'b str,
            area: f32,
            country_code: u16,
        }
        let mut ranked: Vec<RankedPoly2<'_>> = Vec::new();
        for &pid in &chain_ids {
            let poly = &all_polygons[pid as usize];
            if poly.admin_level == 11 { continue; } // postal
            let initial_rank = self.admin_config.to_rank(&country_code_str, poly.admin_level);
            if initial_rank == 0 && poly.place_type_override == 0 { continue; }
            ranked.push(RankedPoly2 {
                admin_level: poly.admin_level,
                rank_address: initial_rank,
                place_type_override: poly.place_type_override,
                name: self.get_string(poly.name_id),
                area: poly.area,
                country_code: poly.country_code,
            });
        }
        // Add place-area polygons
        for poly in &place_area_polygons {
            if poly.place_type_override == 0 { continue; }
            let r = place_type_to_rank(poly.place_type_override);
            if r == 0 { continue; }
            ranked.push(RankedPoly2 {
                admin_level: 15,
                rank_address: r,
                place_type_override: poly.place_type_override,
                name: self.get_string(poly.name_id),
                area: poly.area,
                country_code: poly.country_code,
            });
        }

        ranked.sort_by(|a, b| {
            a.admin_level.cmp(&b.admin_level)
                .then(b.area.partial_cmp(&a.area).unwrap_or(std::cmp::Ordering::Equal))
        });

        // Rank adjustment cascade (same as find_admin)
        for _ in 0..3 {
            for i in 0..ranked.len() {
                let self_al = ranked[i].admin_level;
                let self_override = ranked[i].place_type_override;
                let self_area = ranked[i].area;

                let mut admin_parent_rank: u8 = 0;
                if self_al > 4 {
                    for j in (0..i).rev() {
                        let al = ranked[j].admin_level;
                        if al > 3 && al < self_al {
                            admin_parent_rank = ranked[j].rank_address;
                            break;
                        }
                    }
                }
                if ranked[i].rank_address <= admin_parent_rank {
                    ranked[i].rank_address = admin_parent_rank + 2;
                }
                if self_override > 0 {
                    let linked_rank = place_type_to_rank(self_override);
                    if linked_rank > admin_parent_rank && linked_rank > ranked[i].rank_address {
                        ranked[i].rank_address = linked_rank;
                    }
                }
                if self_override == 0 {
                    for pa in &place_area_polygons {
                        if pa.place_type_override == 0 { continue; }
                        if pa.area <= self_area { continue; }
                        let pa_rank = place_type_to_rank(pa.place_type_override);
                        if pa_rank >= ranked[i].rank_address {
                            ranked[i].rank_address = pa_rank + 2;
                        }
                    }
                }
            }
        }

        // Map to fields
        struct FC<'b> { area: f32, name: &'b str, country_code: u16 }
        let mut best_by_field: [Option<FC<'_>>; 19] = Default::default();
        let field_index = |field: &str| -> Option<usize> {
            match field {
                "country" => Some(0), "state" => Some(1), "province" => Some(2),
                "region" => Some(3), "state_district" => Some(4), "county" => Some(5),
                "district" => Some(6), "municipality" => Some(7), "city" => Some(8),
                "town" => Some(9), "village" => Some(10), "hamlet" => Some(11),
                "city_district" => Some(12), "borough" => Some(13), "suburb" => Some(14),
                "quarter" => Some(15), "neighbourhood" => Some(16), "city_block" => Some(17),
                _ => None,
            }
        };

        for r in &ranked {
            if let Some(field) = if r.place_type_override > 0 {
                place_type_to_field(r.place_type_override)
            } else {
                rank_to_field(r.rank_address)
            } {
                if let Some(idx) = field_index(field) {
                    if let Some(ref existing) = best_by_field[idx] {
                        if r.area >= existing.area { continue; }
                    }
                    best_by_field[idx] = Some(FC { area: r.area, name: r.name, country_code: r.country_code });
                }
            }
        }

        if let Some(ref c) = best_by_field[0] {
            result.country = Some(c.name);
            if c.country_code != 0 {
                result.country_code = Some([
                    (c.country_code >> 8) as u8,
                    (c.country_code & 0xFF) as u8,
                ]);
            }
        }
        result.state          = best_by_field[1].as_ref().map(|c| c.name);
        result.province       = best_by_field[2].as_ref().map(|c| c.name);
        result.region         = best_by_field[3].as_ref().map(|c| c.name);
        result.state_district = best_by_field[4].as_ref().map(|c| c.name);
        result.county         = best_by_field[5].as_ref().map(|c| c.name);
        result.district       = best_by_field[6].as_ref().map(|c| c.name);
        result.municipality   = best_by_field[7].as_ref().map(|c| c.name);
        result.city           = best_by_field[8].as_ref().map(|c| c.name);
        result.town           = best_by_field[9].as_ref().map(|c| c.name);
        result.village        = best_by_field[10].as_ref().map(|c| c.name);
        result.hamlet         = best_by_field[11].as_ref().map(|c| c.name);
        result.city_district  = best_by_field[12].as_ref().map(|c| c.name);
        result.borough        = best_by_field[13].as_ref().map(|c| c.name);
        result.suburb         = best_by_field[14].as_ref().map(|c| c.name);
        result.quarter        = best_by_field[15].as_ref().map(|c| c.name);
        result.neighbourhood  = best_by_field[16].as_ref().map(|c| c.name);
        result.city_block     = best_by_field[17].as_ref().map(|c| c.name);

        result
    }

    // --- Postcode resolution ---
    //
    // Three-tier postcode lookup matching Nominatim's chain:
    //   1. way_postcodes[street.way_id] — street's computed postcode
    //   2. Postal boundary PIP (postal_polygons + postal_vertices)
    //   3. Nearest postcode centroid (Nominatim's get_nearest_postcode)
    fn resolve_postcode(&self, lat: f64, lng: f64, street: Option<&WayHeader>, addr: Option<&AddrPoint>) -> Option<&str> {
        // 0. Nearest addr_point's own postcode (from addr_postcodes.bin)
        // Matches Nominatim's token_get_postcode — the feature's own
        // addr:postcode tag is the highest-priority source.
        if let (Some(ref apc), Some(ap)) = (&self.addr_postcodes, addr) {
            let all_points: &[AddrPoint] = unsafe {
                std::slice::from_raw_parts(
                    self.addr_points.as_ref().unwrap().as_ptr() as *const AddrPoint,
                    self.addr_points.as_ref().unwrap().len() / std::mem::size_of::<AddrPoint>(),
                )
            };
            let addr_id = unsafe {
                (ap as *const AddrPoint).offset_from(all_points.as_ptr()) as usize
            };
            let all_addr_pc: &[u32] = unsafe {
                std::slice::from_raw_parts(apc.as_ptr() as *const u32, apc.len() / 4)
            };
            if addr_id < all_addr_pc.len() && all_addr_pc[addr_id] != NO_DATA {
                return Some(self.get_string(all_addr_pc[addr_id]));
            }
        }

        // 1. way_postcodes[street.way_id] — street's computed postcode
        if let (Some(ref wpc), Some(way)) = (&self.way_postcodes, street) {
            let way_id = unsafe {
                let ways_base = self.street_ways.as_ref().unwrap().as_ptr() as *const WayHeader;
                (way as *const WayHeader).offset_from(ways_base) as usize
            };
            let all_postcodes: &[u32] = unsafe {
                std::slice::from_raw_parts(wpc.as_ptr() as *const u32, wpc.len() / 4)
            };
            if way_id < all_postcodes.len() && all_postcodes[way_id] != NO_DATA {
                return Some(self.get_string(all_postcodes[way_id]));
            }
        }

        // 2. Postal boundary PIP (postal_polygons + postal_vertices)
        if let (Some(ref pp), Some(ref pv)) = (&self.postal_polygons, &self.postal_vertices) {
            let all_postal: &[AdminPolygon] = unsafe {
                std::slice::from_raw_parts(pp.as_ptr() as *const AdminPolygon, pp.len() / std::mem::size_of::<AdminPolygon>())
            };
            let all_pverts: &[NodeCoord] = unsafe {
                std::slice::from_raw_parts(pv.as_ptr() as *const NodeCoord, pv.len() / std::mem::size_of::<NodeCoord>())
            };
            let mut best_area = f32::MAX;
            let mut best_postcode: Option<&str> = None;
            for poly in all_postal {
                if poly.area >= best_area { continue; }
                let off = poly.vertex_offset as usize;
                let cnt = poly.vertex_count as usize;
                if off + cnt > all_pverts.len() { continue; }
                let verts = &all_pverts[off..off+cnt];
                if point_in_polygon(lat as f32, lng as f32, verts) {
                    best_area = poly.area;
                    best_postcode = Some(self.get_string(poly.name_id));
                }
            }
            if best_postcode.is_some() { return best_postcode; }
        }

        // 3. Nearest postcode centroid (Nominatim's get_nearest_postcode).
        // The builder validates postcodes via is_valid_postcode()
        // before accumulating centroids (matching Nominatim's
        // clean_postcodes sanitizer), so query-time filtering is
        // applied as a safety net only.
        if let Some(ref pc_mmap) = self.postcode_centroids {
            let all_centroids: &[PostcodeCentroid] = unsafe {
                std::slice::from_raw_parts(pc_mmap.as_ptr() as *const PostcodeCentroid, pc_mmap.len() / std::mem::size_of::<PostcodeCentroid>())
            };
            // 0.05 deg ~ 5.5km, matches Nominatim's get_nearest_postcode
            let max_dist_sq = (0.05_f64).to_radians().powi(2);
            let cos_lat = lat.to_radians().cos();
            let mut best_dist = f64::MAX;
            let mut best_pc: Option<&str> = None;

            if let (Some(ref cells), Some(ref entries)) = (&self.postcode_centroid_cells, &self.postcode_centroid_entries) {
                let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
                let neighbors = cell_neighbors_at_level(cell, self.admin_cell_level);
                for c in std::iter::once(cell).chain(neighbors.into_iter()) {
                    Self::for_each_entry(entries, Self::lookup_admin_cell(cells, c), |id| {
                        let idx = id as usize;
                        if idx >= all_centroids.len() { return; }
                        let pc = &all_centroids[idx];
                        let dlat = (lat - pc.lat as f64).to_radians();
                        let dlng = (lng - pc.lng as f64).to_radians();
                        let d = dlat * dlat + dlng * dlng * cos_lat * cos_lat;
                        if d < best_dist && d < max_dist_sq {
                            let s = self.get_string(pc.postcode_id);
                            if centroid_postcode_ok(s) {
                                best_dist = d;
                                best_pc = Some(s);
                            }
                        }
                    });
                }
            } else {
                for pc in all_centroids {
                    let dlat = (lat - pc.lat as f64).to_radians();
                    let dlng = (lng - pc.lng as f64).to_radians();
                    let d = dlat * dlat + dlng * dlng * cos_lat * cos_lat;
                    if d < best_dist && d < max_dist_sq {
                        let s = self.get_string(pc.postcode_id);
                        if centroid_postcode_ok(s) {
                            best_dist = d;
                            best_pc = Some(s);
                        }
                    }
                }
            }
            return best_pc;
        }

        None
    }

    // --- Combined query ---

    fn query(&self, lat: f64, lng: f64) -> Address<'_> {
        let max_dist = self.max_distance_sq;

        // Run geo lookups first so we know the primary street
        // Find the smallest admin polygon containing the query at
        // admin_level >= 8 (city/suburb level). This is Nominatim's
        // `current_boundary` — the geometry that place nodes must
        // sit inside to be accepted in the address walk.
        let current_boundary = self.find_current_boundary(lat, lng);
        let place = self.find_places(lat, lng, current_boundary);
        let (addr, interp, street) = self.query_geo(lat, lng);

        // Use the standard PIP-based admin walk. The chain-based walk
        // (way_parents.bin + admin_parents.bin) is available but causes
        // regressions for fine-grained levels where a street's centroid
        // drifts across suburb/neighbourhood boundaries. A future
        // improvement could use the chain for coarse levels (country →
        // city) while keeping PIP for sub-city levels.
        let admin = self.find_admin(lat, lng);

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
        let mut road: Option<&str> = None;

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
        let poi_primary = poi_primary_raw.filter(|(d, _)| *d <= poi_max_sq);

        let addr_dist_raw = addr.as_ref().map(|(d, _)| *d).unwrap_or(f64::MAX);
        // Nominatim includes addr_points (IsAddressPoint, rank_search=30)
        // in the SAME primary query as streets and computes distance to
        // the building's POLYGON geometry — 0 when the query is inside
        // the footprint. We store centroids only, so our distance is
        // always centroid-to-query (~5-15m even when inside the building).
        //
        // Approximate Nominatim's polygon distance by subtracting an
        // estimated building radius (~10m) from the centroid distance,
        // floored at 0. This lets a building at centroid distance 10m
        // compete with a street at 1.6m, matching how Nominatim's
        // polygon distance = 0 beats the street.
        let building_radius_sq = {
            let r = 10.0_f64 / 6_371_000.0;
            r * r
        };
        let addr_dist = if addr_dist_raw <= building_radius_sq {
            0.0
        } else {
            let raw_m = addr_dist_raw.sqrt() * 6_371_000.0;
            let adj_m = (raw_m - 10.0).max(0.0);
            let adj_rad = adj_m / 6_371_000.0;
            adj_rad * adj_rad
        };
        // Nominatim's `_find_closest_street_or_pois` queries the placex
        // table (streets + rank-30 POIs / addr points). Interpolation
        // rows live in the separate `osmline` table and are looked up
        // only AFTER a street wins, via `_find_interpolation_for_street`
        // with a parent_place_id filter (reverse.py:253). Treating
        // interpolation as a primary candidate in its own right picks
        // abbreviated TIGER names ("W 1st St") over the actual OSM
        // street ("West 1st Street") when the TIGER segment happens
        // to sit closer to the query than the raw way — a non-
        // Nominatim behaviour. We keep interp only for housenumber
        // refinement below.
        let interp_dist = f64::MAX;
        let _ = interp.as_ref();
        let street_dist = street.as_ref().map(|(d, _)| *d).unwrap_or(f64::MAX);
        let poi_dist = poi_primary.as_ref().map(|(d, _)| *d).unwrap_or(f64::MAX);

        // Nominatim's reverse.py lines 218-226: when the closest feature
        // is an area at distance 0 (query is inside the polygon) AND
        // there is an addressable rank-30 node nearby, prefer the node.
        // Nominatim's exact threshold is `distance < 0.0001` deg (~11m)
        // for the second-row fallback, but Nominatim also *excludes*
        // building-class polygons from the candidate set entirely,
        // which we don't (Hôtel de Ville, Empire State Building etc.
        // are in our POI index). As an approximation, widen the
        // addr-point preference radius to ~30m so that a nearby
        // addressed rank-30 node beats an enclosing building POI.
        //
        // This is how we pick "Sushi teria, 350, 5th Avenue" for NYC
        // Empire State and "6, City Hall Plaza" for Paris Hôtel de
        // Ville instead of the containing building.
        let inside_node_threshold_sq = (0.00027_f64).to_radians().powi(2);
        let mut effective_poi_dist = poi_dist;
        if poi_dist == 0.0 && addr_dist < inside_node_threshold_sq {
            // Demote POI so addr wins the comparison below.
            effective_poi_dist = f64::MAX;
        }

        let closest_feature_dist = addr_dist.min(interp_dist).min(street_dist).min(effective_poi_dist);
        if closest_feature_dist < max_dist {
            // Whichever feature class has the smallest distance wins.
            if effective_poi_dist <= addr_dist && effective_poi_dist <= interp_dist && effective_poi_dist <= street_dist {
                // POI is closest. For polygon POIs (vertex_count > 0)
                // that the query is inside, use the POI's own name
                // as the road: nominatim picks highway=pedestrian
                // relations like "Constitution Square" (Mexico City
                // Zócalo) directly via its rank-26 street query, and
                // we don't index those as streets so they land in
                // the POI index with the correct name:en. For point
                // POIs (statues, plaques, shrines) that happen to sit
                // on top of the query, stick with parent_street —
                // their own name is a landmark label, not a road.
                let (_, poi) = poi_primary.unwrap();
                let use_poi_name = poi.vertex_count > 0 && poi_dist == 0.0;
                if use_poi_name && poi.name_id != NO_DATA {
                    road = Some(self.get_string(poi.name_id));
                } else if poi.parent_street_id != NO_DATA {
                    road = Some(self.get_string(poi.parent_street_id));
                }
            } else if addr_dist <= interp_dist && addr_dist <= street_dist && addr_dist <= effective_poi_dist {
                // Address is closest — use it with its housenumber.
                let (_, point) = addr.unwrap();
                house_number = Some(Cow::Borrowed(self.get_string(point.housenumber_id)));
                // street_id == NO_DATA means the builder's parent-
                // street backfill didn't find a named way nearby; in
                // that case fall back to the closest-street result
                // instead of the missing addr:street.
                if point.street_id != NO_DATA {
                    road = Some(self.get_string(point.street_id));
                } else if let Some((_, way)) = street {
                    road = Some(self.get_string(way.name_id));
                }
            } else if interp_dist <= addr_dist && interp_dist <= street_dist && interp_dist <= effective_poi_dist {
                // Interpolation segment is closest.
                let (_, street_name, number) = interp.unwrap();
                house_number = Some(Cow::Owned(number.to_string()));
                road = Some(street_name);
            } else {
                // Street is closest. Match Nominatim's
                // `_find_housenumber_for_street` (reverse.py:231):
                // find an IsAddressPoint (addr_point with housenumber)
                // whose parent_way_id matches the winning street,
                // within 0.001 deg (~100m).
                let (_, way) = street.unwrap();
                road = if way.name_id != NO_DATA {
                    Some(self.get_string(way.name_id))
                } else {
                    None
                };

                // Compute way_id for parent_way_id matching
                let way_id = unsafe {
                    let ways_base = self.street_ways.as_ref().unwrap().as_ptr() as *const WayHeader;
                    (way as *const WayHeader).offset_from(ways_base) as u32
                };

                // Housenumber refinement: Nominatim includes addr_points
                // (IsAddressPoint, rank_search=30) in the SAME primary
                // query as streets, so a building polygon at distance 0
                // beats the street at distance > 0. We can't replicate
                // this because we store addr_points as centroids (not
                // polygons), so buildings always have dist > 0 even when
                // the query is inside the footprint. parent_way_id
                // matching gives correct numbers when our street matches
                // Nominatim's but wrong numbers otherwise (net neutral).
                // Disabled until we store building footprints or add
                // addr_points to the primary-feature query.
            }
        }

        // Resolve postcode via the 4-tier chain matching Nominatim:
        // 0. Nearest addr_point's own addr:postcode (from addr_postcodes.bin)
        // 1. way_postcodes[street.way_id] — street's computed postcode
        // 2. Postal boundary PIP (postal_polygons + postal_vertices)
        // 3. Nearest postcode centroid (Nominatim's get_nearest_postcode)
        let resolved_postcode = self.resolve_postcode(
            lat, lng,
            street.as_ref().map(|(_, w)| *w),
            addr.as_ref().map(|(_, a)| *a),
        );

        if road.is_none() && admin.country.is_none() && admin.city.is_none() && admin.state.is_none() {
            return Address::default();
        }

        let pois = self.find_pois(lat, lng);
        let places: Vec<PoiDetail> = pois.into_iter().map(|p| PoiDetail {
            name: p.name.to_string(),
            category: p.category.to_string(),
            distance_m: (p.distance_m * 10.0).round() / 10.0,
        }).collect();

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
        let address = AddressDetails {
            house_number,
            road,
            city: admin.city.or(place.city),
            town: admin.town.or(place.town),
            village: admin.village.or(place.village),
            hamlet: admin.hamlet.or(place.hamlet),
            municipality: admin.municipality,
            state: admin.state,
            province: admin.province,
            region: admin.region,
            state_district: admin.state_district,
            county: admin.county,
            district: admin.district,
            borough: admin.borough,
            suburb: admin.suburb.or(place.suburb),
            quarter: admin.quarter.or(if allow_place_quarter { place.quarter } else { None }),
            city_district: admin.city_district,
            neighbourhood: admin.neighbourhood.or(if allow_place_neighbourhood { place.neighbourhood } else { None }),
            city_block: admin.city_block,
            // Postcode from resolve_postcode(): 3-tier chain
            // (way_postcodes → postal boundary PIP → nearest centroid).
            // Reject if the country has no postal-code system or if
            // the string doesn't match the country's pattern, then
            // normalise whitespace/separators per country.
            postcode: match resolved_postcode {
                Some(pc) => match admin.country_code {
                    Some(cc) => {
                        if country_has_no_postcode(&cc) || !postcode_looks_valid(&cc, pc) {
                            None
                        } else {
                            Some(format_postcode(&cc, pc))
                        }
                    }
                    None => Some(Cow::Borrowed(pc)),
                },
                None => None,
            },
            country: admin.country,
            country_code: admin.country_code.map(|c| String::from_utf8_lossy(&c).into_owned()),
        };
        let display_name = format_address(&address);
        Address { display_name, address, places }
    }
}

// --- Geometry helpers ---

// Countries with `postcode: no` in nominatim's country_settings.yaml
// (UAE, Qatar, most of Sub-Saharan Africa, etc.). Nominatim rejects
// any postcode for these countries during import via the
// clean_postcodes sanitizer. We mirror that here at query time —
// any OSM addr:postcode tag on a Dubai building (typically '000000'
// or similar placeholder) gets filtered out.
fn country_has_no_postcode(country_code: &[u8; 2]) -> bool {
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
fn postcode_looks_valid(_country_code: &[u8; 2], postcode: &str) -> bool {
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

// Quick validation for postcode centroid candidates. Rejects
// strings that are clearly not standalone postcodes — matches
// the spirit of Nominatim's clean_postcodes sanitizer which
// validates against per-country regex patterns at import.
fn centroid_postcode_ok(s: &str) -> bool {
    if s.len() > 10 { return false; }  // postcodes are short
    if s.contains(';') { return false; } // multiple values
    if s.contains("PO") || s.contains("Box") { return false; }
    // Reject US zip+4 (5+4 digit format like "10001-2062")
    if s.len() == 10 && s.as_bytes().get(5) == Some(&b'-') {
        return false;
    }
    true
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
fn format_postcode<'a>(country_code: &[u8; 2], postcode: &'a str) -> Cow<'a, str> {
    let cc = std::str::from_utf8(country_code).unwrap_or("").to_ascii_lowercase();
    // Upper-case buffer with spaces/dashes stripped.
    let bare: String = postcode
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .flat_map(|c| c.to_uppercase())
        .collect();

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
        _ => None,
    };

    match result {
        Some(s) => Cow::Owned(s),
        None => Cow::Borrowed(postcode),
    }
}

fn dist_sq(dlat: f64, dlng: f64, cos_lat: f64) -> f64 {
    dlat * dlat + dlng * dlng * cos_lat * cos_lat
}

fn point_to_segment_with_t(
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

fn point_to_segment_distance(
    px: f64, py: f64,
    ax: f64, ay: f64,
    bx: f64, by: f64,
    cos_lat: f64,
) -> f64 {
    point_to_segment_with_t(px, py, ax, ay, bx, by, cos_lat).0
}

// Ray casting point-in-polygon test
fn point_in_polygon(lat: f32, lng: f32, vertices: &[NodeCoord]) -> bool {
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

struct PoiMatch<'a> {
    name: &'a str,
    category: &'a str,
    distance_m: f64,
    contained: bool,
    score: f64,
}

#[derive(Default)]
struct PlaceResult<'a> {
    city: Option<&'a str>,
    town: Option<&'a str>,
    village: Option<&'a str>,
    hamlet: Option<&'a str>,
    suburb: Option<&'a str>,
    quarter: Option<&'a str>,
    neighbourhood: Option<&'a str>,
}

#[derive(Default)]
struct AdminResult<'a> {
    country: Option<&'a str>,
    country_code: Option<[u8; 2]>,
    state: Option<&'a str>,
    province: Option<&'a str>,
    region: Option<&'a str>,
    state_district: Option<&'a str>,
    county: Option<&'a str>,
    district: Option<&'a str>,
    municipality: Option<&'a str>,
    city: Option<&'a str>,
    town: Option<&'a str>,
    village: Option<&'a str>,
    hamlet: Option<&'a str>,
    city_district: Option<&'a str>,
    borough: Option<&'a str>,
    suburb: Option<&'a str>,
    quarter: Option<&'a str>,
    neighbourhood: Option<&'a str>,
    city_block: Option<&'a str>,
}

#[derive(Serialize, Default)]
struct AddressDetails<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    house_number: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    road: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    city: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    town: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    village: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hamlet: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    municipality: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    province: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state_district: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    county: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    district: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    borough: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    suburb: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    quarter: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    city_district: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    neighbourhood: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    city_block: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    postcode: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country_code: Option<String>,
}

#[derive(Serialize)]
struct PoiDetail {
    name: String,
    category: String,
    distance_m: f64,
}

#[derive(Serialize, Default)]
struct Address<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    address: AddressDetails<'a>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    places: Vec<PoiDetail>,
}

// Address formatting patterns by country code
// Returns (number_after_street, postcode_before_city, include_state)
fn format_rules(country_code: Option<&str>) -> (bool, bool, bool) {
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

fn format_address(addr: &AddressDetails<'_>) -> Option<String> {
    if addr.road.is_none() && addr.city.is_none() && addr.country.is_none() {
        return None;
    }

    let (number_after, postcode_before_city, include_state) = format_rules(addr.country_code.as_deref());
    let mut parts: Vec<String> = Vec::new();

    // Street + house number
    if let Some(road) = addr.road {
        if let Some(ref hn) = addr.house_number {
            if number_after {
                parts.push(format!("{} {}", road, hn));
            } else {
                parts.push(format!("{} {}", hn, road));
            }
        } else {
            parts.push(road.to_string());
        }
    }

    // City + postcode + state
    if postcode_before_city {
        let mut city_part = String::new();
        if let Some(ref pc) = addr.postcode {
            city_part.push_str(pc);
            city_part.push(' ');
        }
        if let Some(city) = addr.city {
            city_part.push_str(city);
        }
        if include_state {
            if let Some(state) = addr.state {
                if !city_part.is_empty() { city_part.push_str(", "); }
                city_part.push_str(state);
            }
        }
        if !city_part.is_empty() {
            parts.push(city_part.trim().to_string());
        }
    } else {
        let mut city_part = String::new();
        if let Some(city) = addr.city {
            city_part.push_str(city);
        }
        if include_state {
            if let Some(state) = addr.state {
                if !city_part.is_empty() { city_part.push_str(", "); }
                city_part.push_str(state);
            }
        }
        if let Some(ref pc) = addr.postcode {
            if !city_part.is_empty() { city_part.push(' '); }
            city_part.push_str(pc);
        }
        if !city_part.is_empty() {
            parts.push(city_part);
        }
    }

    // Country
    if let Some(country) = addr.country {
        parts.push(country.to_string());
    }

    if parts.is_empty() { None } else { Some(parts.join(", ")) }
}

#[derive(Deserialize)]
struct QueryParams {
    lat: f64,
    lon: f64,
    key: Option<String>,
    debug: Option<String>,
}

async fn reverse_geocode(
    Query(params): Query<QueryParams>,
    state: axum::extract::State<Arc<RwLock<auth::Db>>>,
    index: axum::extract::Extension<Arc<Index>>,
    limiter: axum::extract::Extension<Arc<auth::RateLimiter>>,
    connect_info: axum::extract::ConnectInfo<std::net::SocketAddr>,
) -> Response {
    let key = match params.key {
        Some(k) => k,
        None => return (StatusCode::UNAUTHORIZED, "Missing API key").into_response(),
    };

    let (login, rps, rpd, by_ip) = match state.read().unwrap().validate_token(&key) {
        Some(info) => info,
        None => return (StatusCode::UNAUTHORIZED, "Invalid API key").into_response(),
    };

    let rate_key = if by_ip {
        format!("{}:{}", login, connect_info.0.ip())
    } else {
        login
    };

    if let Err(msg) = auth::check_rate(&limiter, &rate_key, rps, rpd) {
        return (StatusCode::TOO_MANY_REQUESTS, msg).into_response();
    }

    if let Some(ref mode) = params.debug {
        let debug = if mode == "primary" {
            index.debug_primary(params.lat, params.lon)
        } else {
            index.debug_admin(params.lat, params.lon)
        };
        let json = serde_json::to_string_pretty(&debug).unwrap_or_default();
        return ([(axum::http::header::CONTENT_TYPE, "application/json")], json).into_response();
    }

    let address = index.query(params.lat, params.lon);
    let json = serde_json::to_string(&address).unwrap_or_default();
    ([(axum::http::header::CONTENT_TYPE, "application/json")], json).into_response()
}

async fn test_portal() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("../../test-portal/index.html"),
    )
}

async fn polygons_geojson(
    Query(params): Query<QueryParams>,
    state: axum::extract::State<Arc<RwLock<auth::Db>>>,
    index: axum::extract::Extension<Arc<Index>>,
) -> Response {
    let key = match params.key {
        Some(k) => k,
        None => return (StatusCode::UNAUTHORIZED, "Missing API key").into_response(),
    };
    if state.read().unwrap().validate_token(&key).is_none() {
        return (StatusCode::UNAUTHORIZED, "Invalid API key").into_response();
    }

    let lat = params.lat;
    let lng = params.lon;
    let idx = &**index;

    let all_polygons: &[AdminPolygon] = unsafe {
        std::slice::from_raw_parts(
            idx.admin_polygons.as_ptr() as *const AdminPolygon,
            idx.admin_polygons.len() / std::mem::size_of::<AdminPolygon>(),
        )
    };
    let all_vertices: &[NodeCoord] = unsafe {
        std::slice::from_raw_parts(
            idx.admin_vertices.as_ptr() as *const NodeCoord,
            idx.admin_vertices.len() / std::mem::size_of::<NodeCoord>(),
        )
    };

    const ID_MASK: u32 = 0x7FFFFFFF;

    let mut features: Vec<serde_json::Value> = Vec::new();
    let mut seen: std::collections::HashSet<u32> = std::collections::HashSet::new();

    let cell = cell_id_at_level(lat, lng, idx.admin_cell_level);
    let neighbors = cell_neighbors_at_level(cell, idx.admin_cell_level);

    for c in std::iter::once(cell).chain(neighbors.into_iter()) {
        Index::for_each_entry(&idx.admin_entries, Index::lookup_admin_cell(&idx.admin_cells, c), |id| {
            let poly_id = id & ID_MASK;
            if !seen.insert(poly_id) { return; }
            let pid = poly_id as usize;
            if pid >= all_polygons.len() { return; }
            let poly = &all_polygons[pid];
            let offset = poly.vertex_offset as usize;
            let count = poly.vertex_count as usize;
            if offset + count > all_vertices.len() { return; }
            let verts = &all_vertices[offset..offset + count];
            if !point_in_polygon(lat as f32, lng as f32, verts) { return; }
            let coords: Vec<[f64; 2]> = verts.iter().map(|v| [v.lng as f64, v.lat as f64]).collect();
            features.push(serde_json::json!({
                "type": "Feature",
                "geometry": { "type": "Polygon", "coordinates": [coords] },
                "properties": {
                    "name": idx.get_string(poly.name_id),
                    "admin_level": poly.admin_level,
                    "area": poly.area,
                    "kind": "admin",
                }
            }));
        });
    }

    if let (Some(ref pp), Some(ref pv)) = (&idx.postal_polygons, &idx.postal_vertices) {
        let all_postal: &[AdminPolygon] = unsafe {
            std::slice::from_raw_parts(pp.as_ptr() as *const AdminPolygon, pp.len() / std::mem::size_of::<AdminPolygon>())
        };
        let all_pverts: &[NodeCoord] = unsafe {
            std::slice::from_raw_parts(pv.as_ptr() as *const NodeCoord, pv.len() / std::mem::size_of::<NodeCoord>())
        };
        for poly in all_postal {
            let off = poly.vertex_offset as usize;
            let cnt = poly.vertex_count as usize;
            if off + cnt > all_pverts.len() { continue; }
            let verts = &all_pverts[off..off + cnt];
            if !point_in_polygon(lat as f32, lng as f32, verts) { continue; }
            let coords: Vec<[f64; 2]> = verts.iter().map(|v| [v.lng as f64, v.lat as f64]).collect();
            features.push(serde_json::json!({
                "type": "Feature",
                "geometry": { "type": "Polygon", "coordinates": [coords] },
                "properties": {
                    "name": idx.get_string(poly.name_id),
                    "admin_level": 99,
                    "area": poly.area,
                    "kind": "postal",
                }
            }));
        }
    }

    let fc = serde_json::json!({
        "type": "FeatureCollection",
        "features": features,
    });
    let body = serde_json::to_string(&fc).unwrap_or_default();
    ([(axum::http::header::CONTENT_TYPE, "application/json")], body).into_response()
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_dir = args.get(1).map(|s| s.as_str()).unwrap_or(".");

    let arg_value = |flag: &str| -> Option<&String> {
        args.iter().position(|a| a == flag).and_then(|p| args.get(p + 1))
    };
    let street_cell_level = arg_value("--street-level").and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_STREET_CELL_LEVEL);
    let admin_cell_level = arg_value("--admin-level").and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_ADMIN_CELL_LEVEL);
    let search_distance = arg_value("--search-distance").and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_SEARCH_DISTANCE);

    let db_path = format!("{}/geocoder.json", data_dir);
    let db = auth::Db::load(&db_path);

    eprintln!("Loading index from {}...", data_dir);
    let index = match Index::load(data_dir, street_cell_level, admin_cell_level, search_distance) {
        Ok(idx) => {
            let place_status = if idx.place_nodes.is_some() {
                let count = idx.place_nodes.as_ref().unwrap().len() / std::mem::size_of::<PlaceNode>();
                format!(" + {} place nodes", count)
            } else {
                String::new()
            };
            let poi_status = if idx.poi_records.is_some() {
                let count = idx.poi_records.as_ref().unwrap().len() / std::mem::size_of::<PoiRecord>();
                format!(" + {} POIs ({} categories)", count, idx.poi_meta.categories.len())
            } else {
                String::new()
            };
            if idx.geo_cells.is_some() {
                if idx.addr_points.is_some() {
                    eprintln!("Loaded full index (admin + geo + addresses{}{})", place_status, poi_status);
                } else {
                    eprintln!("Loaded streets index (admin + geo, no addresses{})", poi_status);
                }
            } else {
                eprintln!("Loaded admin-only index (geo files not found{})", poi_status);
            }
            Arc::new(idx)
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    let db = Arc::new(RwLock::new(db));
    let limiter = Arc::new(auth::RateLimiter::default()); // RwLock<HashMap> with atomic counters

    let app = Router::new()
        .route("/reverse", get(reverse_geocode))
        .route("/polygons", get(polygons_geojson))
        .route("/test", get(test_portal))
        .merge(auth::router())
        .layer(axum::Extension(index))
        .layer(axum::Extension(limiter))
        .with_state(db);

    // ACME mode: --domain <domain> [--cache <dir>]
    let domain_pos = args.iter().position(|a| a == "--domain");
    if let Some(pos) = domain_pos {
        let domain = args.get(pos + 1).expect("--domain requires a value").clone();
        let cache_dir = args.iter().position(|a| a == "--cache")
            .and_then(|p| args.get(p + 1).cloned())
            .unwrap_or_else(|| "acme-cache".to_string());

        use rustls_acme::caches::DirCache;
        use rustls_acme::AcmeConfig;
        use tokio_stream::StreamExt;

        let mut state = AcmeConfig::new([domain.clone()])
            .cache(DirCache::new(cache_dir.clone()))
            .directory_lets_encrypt(true)
            .state();
        let acceptor = state.axum_acceptor(state.default_rustls_config());

        tokio::spawn(async move {
            loop {
                match state.next().await {
                    Some(Ok(ok)) => eprintln!("ACME event: {:?}", ok),
                    Some(Err(err)) => eprintln!("ACME error: {:?}", err),
                    None => break,
                }
            }
        });

        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 443));
        eprintln!("Starting HTTPS server on :443 for {}...", domain);
        axum_server::bind(addr)
            .acceptor(acceptor)
            .serve(app.into_make_service_with_connect_info::<std::net::SocketAddr>())
            .await
            .unwrap();
    } else {
        let bind_addr = args.get(2).map(|s| s.as_str()).unwrap_or("0.0.0.0:3000");
        eprintln!("Starting HTTP server on {}...", bind_addr);
        let listener = tokio::net::TcpListener::bind(bind_addr).await.unwrap();
        axum::serve(listener, app.into_make_service_with_connect_info::<std::net::SocketAddr>()).await.unwrap();
    }
}
