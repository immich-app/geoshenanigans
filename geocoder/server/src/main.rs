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
    area: f32,
    country_code: u16,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct NodeCoord {
    lat: f32,
    lng: f32,
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

    fn query_geo(&self, lat: f64, lng: f64) -> (Option<(f64, &AddrPoint)>, Option<(f64, &str, u32)>, Option<(f64, &WayHeader)>) {
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
                    let point = &all_points[id as usize];
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

    // --- Admin boundary lookup (point-in-polygon) ---

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

        // For each admin level, find the smallest-area polygon containing the point
        let mut best_by_level: [Option<(f32, &AdminPolygon)>; 12] = [None; 12];

        const INTERIOR_FLAG: u32 = 0x80000000;
        const ID_MASK: u32 = 0x7FFFFFFF;

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            Self::for_each_entry(&self.admin_entries, Self::lookup_admin_cell(&self.admin_cells, c), |id| {
                let is_interior = (id & INTERIOR_FLAG) != 0;
                let poly_id = (id & ID_MASK) as usize;
                let poly = &all_polygons[poly_id];
                let level = poly.admin_level as usize;
                if level >= 12 { return; }

                // Skip if we already have a smaller polygon at this level
                if let Some((best_area, _)) = best_by_level[level] {
                    if poly.area >= best_area { return; }
                }

                // Interior cells skip point-in-polygon test
                if is_interior || point_in_polygon(lat as f32, lng as f32, {
                    let offset = poly.vertex_offset as usize;
                    let count = poly.vertex_count as usize;
                    &all_vertices[offset..offset + count]
                }) {
                    best_by_level[level] = Some((poly.area, poly));
                }
            });
        }

        let mut result = AdminResult::default();

        for level in 0..12 {
            if let Some((_, poly)) = best_by_level[level] {
                let name = self.get_string(poly.name_id);
                match poly.admin_level {
                    2 => {
                        result.country = Some(name);
                        if poly.country_code != 0 {
                            result.country_code = Some([
                                (poly.country_code >> 8) as u8,
                                (poly.country_code & 0xFF) as u8,
                            ]);
                        }
                    }
                    4 => result.state = Some(name),
                    6 => result.county = Some(name),
                    8 => result.city = Some(name),
                    11 => result.postcode = Some(name),
                    _ => {}
                }
            }
        }

        result
    }

    // --- Combined query ---

    fn query(&self, lat: f64, lng: f64) -> Address<'_> {
        let max_dist = self.max_distance_sq;

        let admin = self.find_admin(lat, lng);
        let (addr, interp, street) = self.query_geo(lat, lng);

        // Determine house_number and road from best geo match (priority: address > interpolation > street)
        let mut house_number: Option<Cow<'_, str>> = None;
        let mut road: Option<&str> = None;

        if let Some((dist, point)) = addr {
            if dist < max_dist {
                house_number = Some(Cow::Borrowed(self.get_string(point.housenumber_id)));
                road = Some(self.get_string(point.street_id));
            }
        }
        if road.is_none() {
            if let Some((dist, street_name, number)) = interp {
                if dist < max_dist {
                    house_number = Some(Cow::Owned(number.to_string()));
                    road = Some(street_name);
                }
            }
        }
        if road.is_none() {
            if let Some((dist, way)) = street {
                if dist < max_dist {
                    road = Some(self.get_string(way.name_id));
                }
            }
        }

        if road.is_none() && admin.country.is_none() && admin.city.is_none() && admin.state.is_none() {
            return Address::default();
        }

        let address = AddressDetails {
            house_number,
            road,
            city: admin.city,
            state: admin.state,
            county: admin.county,
            postcode: admin.postcode,
            country: admin.country,
            country_code: admin.country_code.map(|c| String::from_utf8_lossy(&c).into_owned()),
        };
        let display_name = format_address(&address);
        Address { display_name, address }
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

#[derive(Default)]
struct AdminResult<'a> {
    country: Option<&'a str>,
    country_code: Option<[u8; 2]>,
    state: Option<&'a str>,
    county: Option<&'a str>,
    city: Option<&'a str>,
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
    state: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    county: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    postcode: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country_code: Option<String>,
}

#[derive(Serialize, Default)]
struct Address<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    address: AddressDetails<'a>,
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

    let address = index.query(params.lat, params.lon);
    let json = serde_json::to_string(&address).unwrap_or_default();
    ([(axum::http::header::CONTENT_TYPE, "application/json")], json).into_response()
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
            if idx.geo_cells.is_some() {
                if idx.addr_points.is_some() {
                    eprintln!("Loaded full index (admin + geo + addresses)");
                } else {
                    eprintln!("Loaded streets index (admin + geo, no addresses)");
                }
            } else {
                eprintln!("Loaded admin-only index (geo files not found)");
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
