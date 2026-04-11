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
const DEFAULT_SEARCH_DISTANCE: f64 = 75.0;

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
    postcode_id: u32,
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
        let meters_to_rad = search_distance / 111_320.0;
        let max_distance_sq = meters_to_rad * meters_to_rad;

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
        -> (Option<(f64, &AddrPoint)>, Option<(f64, &str, u32)>, Option<(f64, &WayHeader)>, Option<&AddrPoint>)
    {
        let geo_cells = match &self.geo_cells {
            Some(gc) => gc,
            None => return (None, None, None, None),
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
        // Track the nearest address that has a postcode, used as a fallback
        // when the chosen address point lacks addr:postcode itself. Matches
        // Nominatim's `get_postcode_matching_boundary` behaviour of walking
        // neighbouring address rows to find one with a postcode when the
        // immediate candidate has none.
        let mut best_addr_with_postcode_dist = f64::MAX;
        let mut best_addr_with_postcode: Option<&AddrPoint> = None;
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
                    if point.postcode_id != NO_DATA && dist < best_addr_with_postcode_dist {
                        best_addr_with_postcode_dist = dist;
                        best_addr_with_postcode = Some(point);
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
        let addr_with_postcode = best_addr_with_postcode;
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

        (addr_result, interp_result, street_result, addr_with_postcode)
    }

    // Debug helper — returns primary feature distances + names.
    fn debug_primary(&self, lat: f64, lng: f64) -> serde_json::Value {
        let (addr, interp, street, _) = self.query_geo(lat, lng);
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
        // ties, matching the deepest-ancestor-per-rank behaviour.
        for r in &ranked {
            let field = if r.place_type_override > 0 {
                // For settlement/quarter/etc., the label is the place type
                // verbatim (mirrors Nominatim's get_label_tag fallthrough).
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

        // Also handle postcode (level 11 postal boundaries kept as before)
        for level in 0..12 {
            if let Some((_, poly, _)) = best_by_level[level] {
                if poly.admin_level == 11 {
                    result.postcode = Some(self.get_string(poly.name_id));
                    break;
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

    fn find_places(&self, lat: f64, lng: f64) -> PlaceResult<'_> {
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

        // Max search radii: Nominatim's reverse_place_diameter values
        // halved for city/town/village to match reality, halved again
        // for quarter / neighbourhood because those rely on the
        // containment gate below for precision and the radius acts as a
        // fast-reject bound only.
        let max_rank17 = (0.08_f64).to_radians().powi(2); // city/town/village
        let max_rank19 = (0.02_f64).to_radians().powi(2); // suburb / hamlet
        let max_rank20 = (0.005_f64).to_radians().powi(2); // quarter / neighbourhood

        // Containment gate: for suburb and deeper (pt ≥ 3), only accept a
        // place-node candidate if it's inside its pre-computed parent
        // admin polygon AND the query point is also inside that same
        // polygon. Mirrors Nominatim's insert_addresslines ST_Contains
        // behaviour — the node is attached to the address chain only if
        // both the road and the node sit in the same containing polygon.
        //
        // The builder stores parent_poly_id per place node (smallest
        // admin polygon by area containing the node centroid). Fall back
        // to radius-only when parent_poly_id is unset (legacy data or
        // unmatched node).
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
        let parent_contains_query = |pn: &PlaceNode| -> bool {
            if pn.parent_poly_id == u32::MAX {
                return true; // no parent info, fall through to radius-only
            }
            let pid = pn.parent_poly_id as usize;
            if pid >= all_polygons.len() { return true; }
            let poly = &all_polygons[pid];
            let off = poly.vertex_offset as usize;
            let cnt = poly.vertex_count as usize;
            if off + cnt > all_vertices.len() { return true; }
            let verts = &all_vertices[off..off + cnt];
            point_in_polygon(lat as f32, lng as f32, verts)
        };

        // City / town / village: no containment gate — the query may
        // sit inside the city's area while the place=city node itself
        // is at the city centre, potentially outside any L9/L10 polygon
        // our builder would pick as a parent.
        for pt in [0, 1, 2] {
            if let Some((dist_sq, pn)) = best[pt] {
                if dist_sq <= max_rank17 {
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

        if let Some((dist_sq, pn)) = best[3] {
            if dist_sq <= max_rank19 && parent_contains_query(pn) {
                result.suburb = Some(self.get_string(pn.name_id));
            }
        }
        if let Some((dist_sq, pn)) = best[4] {
            if dist_sq <= max_rank19 && parent_contains_query(pn) {
                result.hamlet = Some(self.get_string(pn.name_id));
            }
        }
        if let Some((dist_sq, pn)) = best[6] {
            if dist_sq <= max_rank20 && parent_contains_query(pn) {
                result.quarter = Some(self.get_string(pn.name_id));
            }
        }
        if let Some((dist_sq, pn)) = best[5] {
            if dist_sq <= max_rank20 && parent_contains_query(pn) {
                result.neighbourhood = Some(self.get_string(pn.name_id));
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

    // --- Combined query ---

    fn query(&self, lat: f64, lng: f64) -> Address<'_> {
        let max_dist = self.max_distance_sq;

        let admin = self.find_admin(lat, lng);
        let place = self.find_places(lat, lng);
        let (addr, interp, street, nearest_with_postcode) = self.query_geo(lat, lng);

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
        let mut addr_postcode: Option<&str> = None;

        // POI primary-feature candidate: Nominatim's
        // _find_closest_street_or_pois returns the closest rank-26+
        // feature, which may be an addressable POI (amenity / shop /
        // tourism / historic / building / craft) in addition to roads.
        // We look up the nearest POI with a pre-computed parent_street_id
        // and treat it as a 4th candidate alongside address / interp /
        // street.
        let poi_primary = self.find_nearest_poi_with_parent(lat, lng);

        let addr_dist = addr.as_ref().map(|(d, _)| *d).unwrap_or(f64::MAX);
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

        // Nominatim's housenumber refinement radius is 0.001 deg ≈ 100m
        // (hnr_distance tolerance in `_find_closest_street_or_pois`).
        let hnr_tolerance_sq = (0.001_f64).to_radians().powi(2);

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
                // POI is closest — use its pre-computed parent street
                // as the road. No housenumber (POIs represent whole
                // buildings / monuments / squares without a specific
                // addr:housenumber of their own).
                let (_, poi) = poi_primary.unwrap();
                if poi.parent_street_id != NO_DATA {
                    road = Some(self.get_string(poi.parent_street_id));
                }
                // Inherit postcode from a nearby addressed point if
                // there's one within the refinement radius.
                if let Some((ad, ap)) = addr {
                    if ad - poi_dist < hnr_tolerance_sq && ap.postcode_id != NO_DATA {
                        addr_postcode = Some(self.get_string(ap.postcode_id));
                    }
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
                if point.postcode_id != NO_DATA {
                    addr_postcode = Some(self.get_string(point.postcode_id));
                }
            } else if interp_dist <= addr_dist && interp_dist <= street_dist && interp_dist <= effective_poi_dist {
                // Interpolation segment is closest.
                let (_, street_name, number) = interp.unwrap();
                house_number = Some(Cow::Owned(number.to_string()));
                road = Some(street_name);
                if let Some((ad, ap)) = addr {
                    if ad - interp_dist < hnr_tolerance_sq && ap.postcode_id != NO_DATA {
                        addr_postcode = Some(self.get_string(ap.postcode_id));
                    }
                }
            } else {
                // Street is closest — use the street name with no
                // housenumber. Nominatim runs a second lookup
                // (`_find_housenumber_for_street`) that joins by the
                // street's place_id, but our name-based matching
                // produces too many false positives due to OSM name
                // inconsistencies ("Canal St" vs "Canal Street") and
                // cross-neighbourhood duplicate street names, so we
                // leave housenumber off when street is the primary.
                let (_, way) = street.unwrap();
                road = Some(self.get_string(way.name_id));
                if let Some((ad, ap)) = addr {
                    if ad - street_dist < hnr_tolerance_sq && ap.postcode_id != NO_DATA {
                        addr_postcode = Some(self.get_string(ap.postcode_id));
                    }
                }
            }
        }
        // Nominatim fallback: if no postcode yet, walk to the nearest
        // address point that has one.
        if addr_postcode.is_none() {
            if let Some(p) = nearest_with_postcode {
                if p.postcode_id != NO_DATA {
                    addr_postcode = Some(self.get_string(p.postcode_id));
                }
            }
        }

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
            quarter: admin.quarter.or(place.quarter),
            city_district: admin.city_district,
            neighbourhood: admin.neighbourhood.or(place.neighbourhood),
            city_block: admin.city_block,
            postcode: admin.postcode.or(addr_postcode),
            country: admin.country,
            country_code: admin.country_code.map(|c| String::from_utf8_lossy(&c).into_owned()),
        };
        let display_name = format_address(&address);
        Address { display_name, address, places }
    }
}

// --- Geometry helpers ---

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
    postcode: Option<&'a str>,
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
    postcode: Option<&'a str>,
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
        if let Some(pc) = addr.postcode {
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
        if let Some(pc) = addr.postcode {
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
