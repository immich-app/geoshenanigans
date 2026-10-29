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

mod admin_geometry;
pub use admin_geometry::{AdminGeometry, AdminPoint, PipMemo};
#[cfg(test)]
mod scratch_dir;

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

/// A polygon's encoded vertices as `decode_polygon_verts` reads them: the inline 10-byte header
/// (encoding tag, bbox minimum) at `vertex_offset` and where its `vertex_count` vertices start.
pub(crate) struct EncodedPolygon {
    encoding: u8,
    bmin_lat: f32,
    bmin_lng: f32,
    verts_off: u64,
}

/// The polygon at `vertex_offset`, or None where it has none: no vertices, or the header or
/// vertices past the end of `src`.
pub(crate) fn encoded_polygon(src: &FileBytes, vertex_offset: u32, vertex_count: u32) -> Option<EncodedPolygon> {
    if vertex_count == 0 || vertex_offset == NO_DATA { return None; }
    let off = vertex_offset as u64;
    if off + POLY_HEADER_BYTES > src.len() { return None; }
    let header = src.read_chunk(off, POLY_HEADER_BYTES as usize);
    let encoding = header[0];
    let bmin_lat = f32::from_le_bytes(header[2..6].try_into().unwrap());
    let bmin_lng = f32::from_le_bytes(header[6..10].try_into().unwrap());
    let verts_off = off + POLY_HEADER_BYTES;
    if verts_off + vertex_count as u64 * vertex_stride(encoding) as u64 > src.len() { return None; }
    Some(EncodedPolygon { encoding, bmin_lat, bmin_lng, verts_off })
}

/// Decode a polygon's quantized vertices into NodeCoord values.
/// Reads the inline 10-byte polygon header (encoding tag + bbox_min)
/// at vertex_offset, then `vertex_count` vertices that follow.  Used
/// by AdminGeometry, Index::poi_verts, Index::addr_polygon_verts
/// — keeping the encoding logic in one place.
pub fn decode_polygon_verts<'a>(
    src: &'a FileBytes,
    vertex_offset: u32,
    vertex_count: u32,
) -> Option<std::borrow::Cow<'a, [NodeCoord]>> {
    let EncodedPolygon { encoding, bmin_lat: bbox_min_lat, bmin_lng: bbox_min_lng, verts_off } =
        encoded_polygon(src, vertex_offset, vertex_count)?;
    let total_bytes = (vertex_count as u64) * vertex_stride(encoding) as u64;
    let bytes = src.read_chunk(verts_off, total_bytes as usize);

    if encoding == 4 {
        // Legacy raw F32 lat/lng (8 bytes/vertex). Vertices start at
        // POLY_HEADER_BYTES (byte 10), which is only 2-aligned, so a direct
        // `bytes.as_ptr() as *const NodeCoord` cast is undefined behaviour
        // (NodeCoord needs 4-byte alignment). Decode each field with an
        // unaligned little-endian read instead: this yields bit-identical
        // f32 values to the cast on little-endian targets without the
        // alignment hazard.
        let count = vertex_count as usize;
        let mut out = Vec::<NodeCoord>::with_capacity(count);
        for i in 0..count {
            let lat = f32::from_le_bytes(bytes[i*8..i*8+4].try_into().unwrap());
            let lng = f32::from_le_bytes(bytes[i*8+4..i*8+8].try_into().unwrap());
            out.push(NodeCoord { lat, lng });
        }
        Some(std::borrow::Cow::Owned(out))
    } else {
        // Delta-encoded.  Decode each vertex into a fresh NodeCoord.
        let scale = vertex_scale(encoding);
        let bmin_lat = bbox_min_lat as f64;
        let bmin_lng = bbox_min_lng as f64;
        let count = vertex_count as usize;
        let mut out = Vec::<NodeCoord>::with_capacity(count);
        if encoding == 3 {
            // u32 deltas (8 bytes/vertex)
            for i in 0..count {
                let dlat = u32::from_le_bytes(bytes[i*8..i*8+4].try_into().unwrap()) as f64;
                let dlng = u32::from_le_bytes(bytes[i*8+4..i*8+8].try_into().unwrap()) as f64;
                out.push(NodeCoord {
                    lat: (bmin_lat + dlat * scale) as f32,
                    lng: (bmin_lng + dlng * scale) as f32,
                });
            }
        } else {
            // u16 deltas (4 bytes/vertex) — encodings 0, 1, 2
            for i in 0..count {
                let dlat = u16::from_le_bytes(bytes[i*4..i*4+2].try_into().unwrap()) as f64;
                let dlng = u16::from_le_bytes(bytes[i*4+2..i*4+4].try_into().unwrap()) as f64;
                out.push(NodeCoord {
                    lat: (bmin_lat + dlat * scale) as f32,
                    lng: (bmin_lng + dlng * scale) as f32,
                });
            }
        }
        Some(std::borrow::Cow::Owned(out))
    }
}

// --- S2 helpers ---

pub const DEFAULT_STREET_CELL_LEVEL: u64 = 17;
pub const DEFAULT_ADMIN_CELL_LEVEL: u64 = 10;
// Default primary-feature search radius. Matches Nominatim's
// lookup_street_poi default of 0.006 degrees ≈ 660m. This is the
// radius inside which we'll accept a street/addr/interp/POI as the
// primary feature for the reverse result.
pub const DEFAULT_SEARCH_DISTANCE: f64 = 660.0;

// Matches POI_FLAG_HIGHWAY in builder/src/types.h — feature is a
// highway-class polygon (highway=pedestrian/footway/etc.). Nominatim
// ranks these as rank_search=26 (road) rather than the POI default
// rank_search=30, so their own name becomes the `road` field rather
// than falling through to `parent_street`.
pub const POI_FLAG_HIGHWAY: u8 = 0x04;

pub fn cell_id_at_level(lat: f64, lng: f64, level: u64) -> u64 {
    let ll = LatLng::from_degrees(lat, lng);
    CellID::from(ll).parent(level).0
}

pub fn cell_neighbors_at_level(cell_id: u64, level: u64) -> Vec<u64> {
    let cell = CellID(cell_id);
    cell.all_neighbors(level).into_iter().map(|c| c.0).collect()
}

// --- Binary format structs (must match C++ build pipeline) ---

#[repr(C)]
#[derive(Clone, Copy)]
pub struct WayHeader {
    pub node_offset: u32,
    pub node_count: u16,
    pub name_id: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct AddrPoint {
    pub lat: f32,
    pub lng: f32,
    pub housenumber_id: u32,
    pub street_id: u32,
    pub parent_way_id: u32,
    pub vertex_offset: u32,  // NO_DATA for node-sourced points
    pub vertex_count: u32,   // 0 for nodes, >0 for building polygons
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct InterpWay {
    pub node_offset: u32,
    pub node_count: u16,
    pub street_id: u32,
    pub start_number: u32,
    pub end_number: u32,
    pub interpolation: u8,
}

#[inline]
pub fn vertex_stride(enc: u8) -> usize {
    match enc {
        0 | 1 | 2 => 4, // u16 dlat + u16 dlng
        3 | 4     => 8, // u32 dlat + u32 dlng OR f32 + f32
        _ => 8,
    }
}

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

/// Per-polygon header at the start of each polygon's vertex slice in
/// admin_vertices.bin / poi_vertices.bin / addr_vertices.bin:
///   byte 0     : vertex encoding tag (VertexEncoding in builder/src/types.h)
///   byte 1     : padding
///   bytes 2..6 : bbox_min_lat (f32)
///   bytes 6..10: bbox_min_lng (f32)
/// Vertices follow at byte 10, packed at the encoding's stride.
/// Header is omitted for vertex_count == 0 (point-only records,
/// vertex_offset == NO_DATA).
pub const POLY_HEADER_BYTES: u64 = 10;

/// Largest vertex delta (in scale steps) the u16 encodings 0-2 can store.
const U16_VERTEX_MAX_STEPS: f64 = u16::MAX as f64;
/// Margin of the latitude bounds test in `ring_bounds_exclude`. `point_in_polygon` computes an
/// edge's crossing latitude in f32; while the edge's longitude span is a normal f32 it lands
/// within about 7e-5 degrees of the edge's latitude span (five roundings relative to a span of at
/// most 180 degrees, plus the final addition), so a point 1e-3 degrees beyond every vertex is
/// beyond every crossing.
const PIP_LAT_BOUND_SLACK_DEG: f32 = 1e-3;
/// 2^-100 (f32 biased exponent 27). At or above this |lng| the longitudes of an edge straddling
/// the point differ by at least 2^-124, a normal f32; closer to 0 the latitude test is skipped.
const PIP_LAT_BOUND_MIN_ABS_LNG: f32 = f32::from_bits(27 << 23);

#[repr(C)]
#[derive(Clone, Copy)]
pub struct AdminPolygon {
    pub vertex_offset: u32,       // BYTE offset into admin_vertices.bin (to header)
    pub vertex_count: u32,
    pub name_id: u32,
    pub admin_level: u8,
    pub place_type_override: u8,  // 0=none, 1=city, 2=town, 3=village, 4=suburb, 5=neighbourhood, 6=quarter
    pub _pad2: u8,
    pub _pad3: u8,
    pub area: f32,                // shoelace, holes subtracted
    pub country_code: u16,
    /// Holes appended to the outer ring in the vertex range. They are
    /// keyhole-encoded (builder/src/polygon_rings.h): each is reached from
    /// the outer ring's first vertex by axis-aligned connector edges walked
    /// both ways, which cancel in `point_in_polygon`, so the plain crossing
    /// test over all `vertex_count` vertices already leaves holes out.
    pub hole_count: u16,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct NodeCoord {
    pub lat: f32,
    pub lng: f32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PoiRecord {
    pub lat: f32,
    pub lng: f32,
    pub vertex_offset: u32,       // BYTE offset into poi_vertices.bin (to header), NO_DATA for point POIs
    pub vertex_count: u32,
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

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PostcodeCentroid {
    pub lat: f32,
    pub lng: f32,
    pub postcode_id: u32,
    pub country_code: u16,
    pub _pad: u16,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PlaceNode {
    pub lat: f32,
    pub lng: f32,
    pub name_id: u32,
    pub place_type: u8,
    pub _pad1: u8,
    pub _pad2: u16,
    /// Smallest admin polygon (by area) that contains this place node
    /// centroid, computed at build time. Used to gate nearest-place
    /// fallback: a quarter/neighbourhood candidate is only returned if
    /// the query point is inside the polygon referenced here. Mirrors
    /// Nominatim's insert_addresslines containment check at indexing.
    /// 0xFFFFFFFF means "no containing admin polygon found" — fall back
    /// to radius-only matching.
    pub parent_poly_id: u32,
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
        // Small-amenity sub-categories (171..=179) — toilets, clock…
        171..=179 => Some("amenity"),
        // Food / civic / health / finance / mail / transport extras
        // (180..=234) all stay in `amenity`
        180..=234 => Some("amenity"),
        // Leisure extras (235..=240) stay in `leisure`
        235..=240 => Some("leisure"),
        // Shop sub-categories (241..=253)
        241..=253 => Some("shop"),
        // UNNAMED_RANK30=170 never surfaces (by definition generic).
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

/// Whether a place area (class=place polygon) only repeats an address entry
/// already found: a polygon of the same name and rank_address, no larger.
/// Nominatim keeps one entry per rank (address_havelevel); a namesake of
/// another rank is an entry of its own (Singapore's place=city area has the
/// country's name and outline, and Nominatim names it as the city).
pub fn place_area_repeats<'a>(found: impl IntoIterator<Item = (&'a str, u8, f32)>, name: &str, rank: u8, area: f32) -> bool {
    found.into_iter().any(|(n, r, a)| n == name && r == rank && a <= area)
}

// Map an AdminPlaceType override to its Nominatim rank_address equivalent.
// Drives the nested-boundary rank adjustment so linked / place=tagged
// boundaries sit in the same rank hierarchy as admin_level-based ones.
// Ranks come from settings/address-levels.json default section.
pub fn place_type_to_rank(pt: u8) -> u8 {
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

/// A polygon holding a query point with its rank_address in the point's
/// address.
#[derive(Clone, Copy)]
pub struct RankedPoly {
    pub poly: AdminPolygon,
    pub rank_address: u8,
}

/// Nominatim's nested-boundary rank adjustment (placex_triggers.sql
/// lines 910-1175) over the polygons holding a point, each at its initial
/// rank_address: sorts them by admin_level ascending, ties (same
/// admin_level) by area descending (the larger polygon is assumed to be
/// the parent), then applies three promotions in order:
///
///   1. Admin nested: if a parent admin boundary (admin_level > 3
///      AND admin_level < self) has rank_address >= self, push
///      self to parent_rank + 2.
///   2. Linked place override: if the linked place's rank >
///      parent_address_level (the same "parent" computed above),
///      replace self rank with the linked place rank.
///   3. Place area containment, from rank 10 (:922): if a class='place'
///      area (e.g. R9185096 "Nairobi" with place=city) with rank >= self
///      contains self, push self to place_area_rank + 2.
///
/// `place_areas` are the class='place' polygons (admin_level 15) holding
/// the point. We approximate class='place' polygons as any polygon with a
/// non-zero place_type_override — that's the set the builder
/// populates from place=X and linked_place=X tags. Because every
/// polygon here contains the query point, we use
/// (area > self.area) as a proxy for "contains self".
///
/// Order matters: admin nested must run before the place-area
/// promotion for the ranks to stabilise. We iterate 3 times so the
/// cascade can propagate through admin and place parents alike
/// (Starehe L6 getting bumped to 18 then promoting CBD division L7
/// from 14 to 20). The parent of rule 1 is the latest earlier polygon
/// with admin_level in (3, self.admin_level).
///
/// Only polygons without their own place_type_override are eligible
/// for place-area promotion (rule 3) — a Paris L6 boundary with
/// linked_place=city should stay at rank 16 even though it's
/// nested inside a larger polygon with an override.
pub fn promote_nested_ranks<'a>(ranked: &mut [RankedPoly], place_areas: impl Iterator<Item = &'a AdminPolygon> + Clone) {
    ranked.sort_by(|a, b| {
        a.poly.admin_level.cmp(&b.poly.admin_level)
            .then(b.poly.area.partial_cmp(&a.poly.area).unwrap_or(std::cmp::Ordering::Equal))
    });

    for _ in 0..3 {
        for i in 0..ranked.len() {
            let self_al = ranked[i].poly.admin_level;
            let self_area = ranked[i].poly.area;
            let self_override = ranked[i].poly.place_type_override;

            // Step 1: admin nested.
            let mut admin_parent_rank: u8 = 0;
            if self_al > 4 {
                for j in (0..i).rev() {
                    let al = ranked[j].poly.admin_level;
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
            // class='place' polygons only. Admin boundaries
            // with linked_place tags (Nairobi L3, BA L8, Paris L6)
            // are NOT used here — Nominatim's SQL explicitly
            // restricts this loop to class='place'.
            if self_override == 0 && ranked[i].rank_address >= PLACE_AREA_MIN_RANK {
                let mut place_parent_rank: u8 = 0;
                for pa in place_areas.clone() {
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
}

/// The lowest rank_address the place-area step promotes a boundary from
/// (placex_triggers.sql:922).
pub const PLACE_AREA_MIN_RANK: u8 = 10;

// --- String pool (tiered) ---
//
// Strings are split into 5 files: core/street/addr/postcode/poi.  Record
// name_ids are offsets into a global virtual pool spanning all tiers,
// contiguously in [bases[t], bases[t+1]).  Clients download the subset
// of tier files they need; any offset referencing an unloaded tier
// resolves to "" (graceful degradation).  strings_core.bin is required.

pub const STR_TIER_COUNT: usize = 5;
pub const STR_TIER_FILENAMES: [&str; STR_TIER_COUNT] = [
    "strings_core.bin", "strings_street.bin", "strings_addr.bin",
    "strings_postcode.bin", "strings_poi.bin",
];

pub struct StringPool {
    pub tiers: [Option<FileBytes>; STR_TIER_COUNT],
    pub bases: [u32; STR_TIER_COUNT + 1],
}

impl StringPool {
    /// Construct directly from tier buffers + parsed bases.  Used by
    /// the loader (which mmaps then wraps).
    pub fn from_parts(tiers: [Option<FileBytes>; STR_TIER_COUNT], bases: [u32; STR_TIER_COUNT + 1]) -> Self {
        StringPool {
            tiers,
            bases,
        }
    }

    pub fn load(dir: &str) -> Result<Self, String> {
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
        let mut tiers: [Option<FileBytes>; STR_TIER_COUNT] = Default::default();
        for (t, entry) in entries.iter().enumerate() {
            let start = entry.get("start").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
            let end   = entry.get("end").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
            bases[t] = start;
            bases[t + 1] = end;
            tiers[t] = mmap_file_optional(&format!("{}/{}", dir, STR_TIER_FILENAMES[t]));
        }
        // Core tier is mandatory — admin record names resolve from it.
        if tiers[0].is_none() {
            return Err(format!("Required {}/{} missing", dir, STR_TIER_FILENAMES[0]));
        }
        Ok(StringPool::from_parts(tiers, bases))
    }

    pub fn get(&self, offset: u32) -> &str {
        if offset == NO_DATA { return ""; }
        for t in 0..STR_TIER_COUNT {
            if offset < self.bases[t + 1] {
                let Some(m) = &self.tiers[t] else { return ""; };
                if offset < self.bases[t] { return ""; }
                let local = (offset - self.bases[t]) as u64;
                if local >= m.len() { return ""; }
                return self.read_string(m, local);
            }
        }
        ""
    }

    /// Resolve a single string from a tier as an in-place borrow.
    fn read_string<'a>(&'a self, m: &'a FileBytes, local: u64) -> &'a str {
        match m {
            FileBytes::Owned(v) => {
                let bytes = &v[local as usize..];
                let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
                std::str::from_utf8(&bytes[..end]).unwrap_or("")
            }
            FileBytes::Mmap(mm) => {
                let bytes = &mm[local as usize..];
                let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
                std::str::from_utf8(&bytes[..end]).unwrap_or("")
            }
        }
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
    pub fn new(index: &Index, lat: f64, lng: f64) -> Self {
        let list = Index::lookup_admin_cell_fb(&index.admin_cells, cell_id_at_level(lat, lng, index.admin_cell_level));
        // The list, each record and most ring headers are read below wherever the point is, so
        // they are requested before the first one is needed: their cache misses overlap.
        if list != NO_DATA { index.admin_entries.prefetch(list as u64); }
        let record_bytes = std::mem::size_of::<AdminPolygon>() as u64;
        Index::for_each_entry_fb(&index.admin_entries, list, |id| index.admin_polygons.prefetch((id & ID_MASK) as u64 * record_bytes));
        let mut rings = Vec::new();
        Index::for_each_entry_fb(&index.admin_entries, list, |id| {
            if let Some(poly) = index.admin_polygon(id & ID_MASK) { rings.push((id, poly)); }
        });
        for (_, poly) in &rings {
            index.admin_geometry.prefetch(poly);
        }
        AdminScan { rings, pip: ScanPip { point: AdminPoint::new(lat, lng), memo: PipMemo::new() } }
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
}

// --- Index data ---

pub struct Index {
    pub geo_cells: Option<FileBytes>,
    pub street_entries: Option<FileBytes>,
    pub street_ways: Option<FileBytes>,
    pub street_nodes: Option<FileBytes>,
    pub addr_entries: Option<FileBytes>,
    pub addr_points: Option<FileBytes>,
    pub addr_vertices: Option<FileBytes>,
    pub interp_entries: Option<FileBytes>,
    pub interp_ways: Option<FileBytes>,
    pub interp_nodes: Option<FileBytes>,
    // TIGER per-segment ZIP sidecar: u32 postcode string offset per interp
    // way (NO_DATA for OSM interpolations). Absent on builds without TIGER
    // data and on pre-sidecar indexes — individually optional.
    pub interp_postcodes: Option<FileBytes>,
    pub admin_cells: FileBytes,
    pub admin_entries: FileBytes,
    pub admin_polygons: FileBytes,
    pub admin_geometry: AdminGeometry,
    pub poi_cells: Option<FileBytes>,
    pub poi_entries: Option<FileBytes>,
    pub poi_records: Option<FileBytes>,
    pub poi_vertices: Option<FileBytes>,
    pub poi_meta: PoiMeta,
    pub place_nodes: Option<FileBytes>,
    pub place_cells: Option<FileBytes>,
    pub place_entries: Option<FileBytes>,
    // Parent chain files (Nominatim-style address walk)
    pub way_parents: Option<FileBytes>,    // u32 per way → smallest containing admin poly
    pub admin_parents: Option<FileBytes>,  // u32 per poly → next-larger containing admin poly
    pub addr_postcodes: Option<FileBytes>, // u32 per addr_point → postcode string (optional)
    pub way_postcodes: Option<FileBytes>,  // u32 per way → postcode string from postal boundary
    // Separate postcode files (optional — postcodes omitted when absent)
    pub postal_polygons: Option<FileBytes>,
    pub postal_geometry: Option<AdminGeometry>,
    pub postcode_centroids: Option<FileBytes>,
    pub postcode_centroid_cells: Option<FileBytes>,
    pub postcode_centroid_entries: Option<FileBytes>,
    pub admin_config: AdminLevelConfig,
    pub strings: StringPool,
    pub street_cell_level: u64,
    pub admin_cell_level: u64,
    pub max_distance_sq: f64,
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
/// Squared distance (radians²) from the point to an interpolation line and
/// the fraction along it, by length, of the closest point. None for a line
/// without length.
fn interp_position(nodes: &[NodeCoord], lat: f64, lng: f64, cos_lat: f64) -> Option<(f64, f64)> {
    if nodes.len() < 2 { return None; }
    let seg_len = |i: usize| {
        let dlat = (nodes[i + 1].lat as f64 - nodes[i].lat as f64).to_radians();
        let dlng = (nodes[i + 1].lng as f64 - nodes[i].lng as f64).to_radians();
        // sqrt: dist_sq is squared length; accumulating squares
        // skews the along-way fraction toward long segments.
        dist_sq(dlat, dlng, cos_lat).sqrt()
    };
    let total_len: f64 = (0..nodes.len() - 1).map(seg_len).sum();
    if total_len == 0.0 { return None; }

    let mut best_dist = f64::MAX;
    let mut best_t = 0.0;
    let mut accumulated = 0.0;
    for i in 0..nodes.len() - 1 {
        let len = seg_len(i);
        let (dist, seg_t) = point_to_segment_with_t(
            lat, lng,
            nodes[i].lat as f64, nodes[i].lng as f64,
            nodes[i + 1].lat as f64, nodes[i + 1].lng as f64,
            cos_lat,
        );
        if dist < best_dist {
            best_dist = dist;
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

// Entry-ID encoding for admin/POI cell entries: the high bit flags an
// "interior" entry (cell fully inside the polygon, a fast path for PIP),
// and the low 31 bits carry the polygon/POI index.
pub const INTERIOR_FLAG: u32 = 0x80000000;
pub const ID_MASK: u32 = 0x7FFFFFFF;

pub struct GeoCellOffsets {
    pub street: u32,
    pub addr: u32,
    pub interp: u32,
}

pub fn mmap_file(path: &str) -> Result<FileBytes, String> {
    let file = File::open(path).map_err(|e| format!("Failed to open {}: {}", path, e))?;
    let mmap = unsafe { Mmap::map(&file).map_err(|e| format!("Failed to mmap {}: {}", path, e))? };
    // MADV_RANDOM disables kernel readahead.  All our access patterns are
    // random index lookups (cell binary search, polygon vertex slices,
    // string offsets) — readahead pulls in 32 pages of neighbours per
    // touched page, inflating RSS several × without helping the next
    // access.  Random-mode keeps RSS proportional to actually-touched
    // bytes.  Cuts admin_vertices RSS from ~150 MB → ~35 MB on planet.
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

impl Index {
    pub fn load(dir: &str, street_cell_level: u64, admin_cell_level: u64, search_distance: f64) -> Result<Self, String> {
        // Distance budget used by the primary-feature selection. The
        // internal distances are stored in radians² (via `to_radians()`
        // before squaring), so convert metres → radians → radians².
        // Earth radius is 6 371 000 m.
        let radians = search_distance / 6_371_000.0;
        let max_distance_sq = radians * radians;

        let geo_cells = mmap_file_optional(&format!("{}/geo_cells.bin", dir));
        let has_geo = geo_cells.is_some();

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
        // = corruption). addr_vertices is NOT part of the addr group — it
        // was added in v9 (building footprints) and older indexes
        // legitimately lack it. The way_/admin_parents and *_postcodes
        // sidecars are individually optional for the same version reason.
        let [addr_entries, addr_points]: [Option<FileBytes>; 2] =
            mmap_group(dir, &["addr_entries.bin", "addr_points.bin"])?
                .try_into().map_err(|_| "addr group arity")?;
        let [interp_entries, interp_ways, interp_nodes]: [Option<FileBytes>; 3] =
            mmap_group(dir, &["interp_entries.bin", "interp_ways.bin", "interp_nodes.bin"])?
                .try_into().map_err(|_| "interp group arity")?;
        let [poi_cells, poi_entries, poi_records, poi_vertices]: [Option<FileBytes>; 4] =
            mmap_group(dir, &["poi_cells.bin", "poi_entries.bin", "poi_records.bin", "poi_vertices.bin"])?
                .try_into().map_err(|_| "poi group arity")?;
        let [place_nodes, place_cells, place_entries]: [Option<FileBytes>; 3] =
            mmap_group(dir, &["place_nodes.bin", "place_cells.bin", "place_entries.bin"])?
                .try_into().map_err(|_| "place group arity")?;
        let [postal_polygons, postal_vertices]: [Option<FileBytes>; 2] =
            mmap_group(dir, &["postal_polygons.bin", "postal_vertices.bin"])?
                .try_into().map_err(|_| "postal group arity")?;
        let [postcode_centroids, postcode_centroid_cells, postcode_centroid_entries]: [Option<FileBytes>; 3] =
            mmap_group(dir, &["postcode_centroids.bin", "postcode_centroid_cells.bin", "postcode_centroid_entries.bin"])?
                .try_into().map_err(|_| "centroid group arity")?;

        Ok(Index {
            geo_cells,
            street_entries: required_geo("street_entries.bin")?,
            street_ways: required_geo("street_ways.bin")?,
            street_nodes: required_geo("street_nodes.bin")?,
            addr_entries,
            addr_points,
            addr_vertices: mmap_file_optional(&format!("{}/addr_vertices.bin", dir)),
            interp_entries,
            interp_ways,
            interp_nodes,
            interp_postcodes: mmap_file_optional(&format!("{}/interp_postcodes.bin", dir)),
            admin_cells: mmap_file(&format!("{}/admin_cells.bin", dir))?,
            admin_entries: mmap_file(&format!("{}/admin_entries.bin", dir))?,
            admin_polygons: mmap_file(&format!("{}/admin_polygons.bin", dir))?,
            admin_geometry: AdminGeometry::new(mmap_file(&format!("{}/admin_vertices.bin", dir))?),
            poi_cells,
            poi_entries,
            poi_records,
            poi_vertices,
            poi_meta: PoiMeta::load(dir),
            place_nodes,
            place_cells,
            place_entries,
            way_parents: mmap_file_optional(&format!("{}/way_parents.bin", dir)),
            admin_parents: mmap_file_optional(&format!("{}/admin_parents.bin", dir)),
            addr_postcodes: mmap_file_optional(&format!("{}/addr_postcodes.bin", dir)),
            way_postcodes: mmap_file_optional(&format!("{}/way_postcodes.bin", dir)),
            postal_polygons,
            postal_geometry: postal_vertices.map(AdminGeometry::new),
            postcode_centroids,
            postcode_centroid_cells,
            postcode_centroid_entries,
            admin_config: AdminLevelConfig::load(),
            strings: StringPool::load(dir)?,
            street_cell_level,
            admin_cell_level,
            max_distance_sq,
        })
    }

    pub fn get_string(&self, offset: u32) -> &str {
        self.strings.get(offset)
    }

    /// Decode a POI polygon's quantized vertex slice from `poi_vertices` (optional file): the
    /// inline 10-byte polygon header (encoding + bbox_min) at `poly.vertex_offset`, then the
    /// vertices that follow.
    pub fn poi_verts(&self, poly: &PoiRecord) -> Option<std::borrow::Cow<'_, [NodeCoord]>> {
        let v = self.poi_vertices.as_ref()?;
        decode_polygon_verts(v, poly.vertex_offset, poly.vertex_count)
    }

    /// Decode an addr_point's polygon footprint vertex slice.  Same
    /// inline-header layout as admin/POI vertices.
    pub fn addr_polygon_verts(&self, point: &AddrPoint) -> Option<std::borrow::Cow<'_, [NodeCoord]>> {
        let v = self.addr_vertices.as_ref()?;
        decode_polygon_verts(v, point.vertex_offset, point.vertex_count)
    }

    /// Read a single record from a chunkable file by index.  Returns
    /// None if the file is absent or `idx` would overrun.  Used by
    /// hot paths that need an owned record copy (place/poi/postcode
    /// iteration) — Box::new pattern would force allocation; this
    /// returns a stack-resident value via FileBytes::read_at.
    ///
    /// Strategy-2 tombstones: when the previous build had a record
    /// at idx K that no longer exists in this build, the slot is
    /// emitted as a zeroed record with name_id = NO_DATA (or
    /// equivalent sentinel). These accessors detect tombstones and
    /// return None so callers iterating slot ranges naturally skip
    /// them. Cell-driven access only sees live IDs (cell entries are
    /// rebuilt against the live record set), so tombstones are only
    /// reached via direct iteration paths.
    pub fn place_node(&self, idx: u32) -> Option<PlaceNode> {
        let m = self.place_nodes.as_ref()?;
        let total = m.len() / std::mem::size_of::<PlaceNode>() as u64;
        if (idx as u64) >= total { return None; }
        let pn = m.read_at::<PlaceNode>(idx as u64);
        // _pad1 bit 1: a node a boundary claims (hidden, keeps its name for search)
        if pn.name_id == NO_DATA || pn._pad1 & 0x02 != 0 { return None; }
        Some(pn)
    }

    pub fn poi_record(&self, idx: u32) -> Option<PoiRecord> {
        let m = self.poi_records.as_ref()?;
        let total = m.len() / std::mem::size_of::<PoiRecord>() as u64;
        if (idx as u64) >= total { return None; }
        let pr = m.read_at::<PoiRecord>(idx as u64);
        if pr.name_id == NO_DATA && pr.vertex_offset == NO_DATA { return None; }
        Some(pr)
    }

    pub fn postcode_centroid(&self, idx: u32) -> Option<PostcodeCentroid> {
        let m = self.postcode_centroids.as_ref()?;
        let total = m.len() / std::mem::size_of::<PostcodeCentroid>() as u64;
        if (idx as u64) >= total { return None; }
        Some(m.read_at::<PostcodeCentroid>(idx as u64))
    }

    pub fn admin_polygon(&self, idx: u32) -> Option<AdminPolygon> {
        let m = &self.admin_polygons;
        let total = m.len() / std::mem::size_of::<AdminPolygon>() as u64;
        if (idx as u64) >= total { return None; }
        let ap = m.read_at::<AdminPolygon>(idx as u64);
        if ap.name_id == NO_DATA { return None; }
        Some(ap)
    }

    /// Call `f` for each ID in the cell-entry list at `offset`.  Wire
    /// format: u16 count, then count u32 IDs.  Bulk-reads the entries into a Cow once so the inner
    /// loop doesn't pay per-id chunk-fetch overhead.
    pub fn for_each_entry_fb(entries: &FileBytes, offset: u32, mut f: impl FnMut(u32)) {
        if offset == NO_DATA { return; }
        let off = offset as u64;
        if off + 2 > entries.len() { return; }
        let header = entries.read_chunk(off, 2);
        let count = u16::from_le_bytes([header[0], header[1]]) as usize;
        if count == 0 { return; }
        let total = count * 4;
        if off + 2 + total as u64 > entries.len() { return; }
        let blob = entries.read_chunk(off + 2, total);
        for i in 0..count {
            f(u32::from_le_bytes(blob[i*4..i*4+4].try_into().unwrap()));
        }
    }

    /// Chunked-aware admin-cell lookup: 12-byte stride sorted index
    /// (u64 cell_id + u32 entries_offset) over a FileBytes source.
    pub fn lookup_admin_cell_fb(cells: &FileBytes, cell_id: u64) -> u32 {
        const STRIDE: u64 = 12;
        let count = cells.len() / STRIDE;
        if count == 0 { return NO_DATA; }
        let mut lo: u64 = 0;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let bytes = cells.read_chunk(mid * STRIDE, STRIDE as usize);
            let mid_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
            if mid_id == cell_id {
                return u32::from_le_bytes(bytes[8..12].try_into().unwrap());
            } else if mid_id < cell_id { lo = mid + 1; } else { hi = mid; }
        }
        NO_DATA
    }

    // geo_cells is a 20-byte-stride sorted index — binary search it
    // with read_chunk.
    pub fn lookup_geo_cell_fb(geo_cells: &FileBytes, target: u64) -> GeoCellOffsets {
        let entry_size: u64 = 20;
        let count = geo_cells.len() / entry_size;
        let empty = GeoCellOffsets { street: NO_DATA, addr: NO_DATA, interp: NO_DATA };
        if count == 0 { return empty; }
        let mut lo: u64 = 0;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let bytes = geo_cells.read_chunk(mid * entry_size, entry_size as usize);
            let mid_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
            if mid_id == target {
                return GeoCellOffsets {
                    street: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
                    addr:   u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
                    interp: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
                };
            } else if mid_id < target { lo = mid + 1; } else { hi = mid; }
        }
        empty
    }

    // Read entries blob as Cow so it works on chunked sources.
    // Same wire format as `for_each_entry_fb` (u16 count, then count
    // u32 IDs), but materialises the IDs into a Vec for callers that
    // need to iterate them more than once.
    pub fn read_entries_fb(source: &FileBytes, offset: u32) -> Vec<u32> {
        if offset == NO_DATA { return Vec::new(); }
        let off = offset as u64;
        if off + 2 > source.len() { return Vec::new(); }
        let header = source.read_chunk(off, 2);
        let count = u16::from_le_bytes([header[0], header[1]]) as usize;
        if count == 0 { return Vec::new(); }
        // Bound the payload: a corrupt count must not read past EOF.
        if off + 2 + (count as u64) * 4 > source.len() { return Vec::new(); }
        let blob = source.read_chunk(off + 2, count * 4);
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            out.push(u32::from_le_bytes(blob[i*4..i*4+4].try_into().unwrap()));
        }
        out
    }


    // --- Geo lookup (streets, addresses, interpolation from merged index) ---

    pub fn query_geo(&self, lat: f64, lng: f64)
        -> (Option<(f64, AddrPoint, u32)>, Option<(f64, &str, u32, u32)>, Option<(f64, WayHeader, u32)>)
    {
        let geo_cells = match &self.geo_cells {
            Some(gc) => gc,
            None => return (None, None, None),
        };
        // Street files always accompany geo_cells in a well-formed index
        // (required_geo enforces all-or-nothing at load). Guard rather than
        // unwrap so a partial/admin-only index degrades gracefully instead
        // of panicking.
        let (Some(street_entries), Some(street_ways), Some(street_nodes)) =
            (self.street_entries.as_ref(), self.street_ways.as_ref(), self.street_nodes.as_ref())
        else {
            return (None, None, None);
        };
        let cell = cell_id_at_level(lat, lng, self.street_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.street_cell_level);

        let cos_lat = lat.to_radians().cos();
        let addr_key = |a: &AddrPoint| {
            let d = dist_sq((a.lat as f64 - lat).to_radians(), (a.lng as f64 - lng).to_radians(), cos_lat);
            (d, self.get_string(a.housenumber_id), self.get_string(a.street_id))
        };
        let street_key = |w: &WayHeader| {
            let first: NodeCoord = street_nodes.read_at(w.node_offset as u64);
            (self.get_string(w.name_id), first.lat, first.lng)
        };

        let mut best_addr_dist = f64::MAX;
        let mut best_addr: Option<AddrPoint> = None;
        let mut best_addr_id: u32 = NO_DATA;
        let mut best_street_dist = f64::MAX;
        let mut best_street: Option<WayHeader> = None;
        let mut best_street_idx: u32 = NO_DATA;
        let mut best_interp_dist = f64::MAX;
        let mut best_interp: Option<InterpWay> = None;
        let mut best_interp_t: f64 = 0.0;
        let mut best_interp_id: u32 = NO_DATA;

        // Fixed-size hash set on stack to skip duplicate street IDs across cells
        let mut seen_streets: [u32; 64] = [u32::MAX; 64];

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            let offsets = Self::lookup_geo_cell_fb(geo_cells, c);

            // Addresses. Building-sourced points (vertex_count > 0)
            // use polygon distance, matching ST_Distance behaviour.
            if let (Some(addr_entries), Some(addr_points)) = (self.addr_entries.as_ref(), self.addr_points.as_ref()) {
                let total_addr = addr_points.len() / std::mem::size_of::<AddrPoint>() as u64;
                for id in Self::read_entries_fb(addr_entries, offsets.addr) {
                    let idx = id as u64;
                    if idx >= total_addr { continue; }
                    let point: AddrPoint = addr_points.read_at(idx);
                    let dlat = (point.lat as f64 - lat).to_radians();
                    let dlng = (point.lng as f64 - lng).to_radians();
                    let mut dist = dist_sq(dlat, dlng, cos_lat);
                    if let Some(verts) = self.addr_polygon_verts(&point) {
                        dist = polygon_distance_sq(lat, lng, &verts, cos_lat);
                    }
                    if beats(dist, best_addr_dist, || best_addr.as_ref().map(|b| (addr_key(&point), addr_key(b)))) {
                        best_addr_dist = dist;
                        best_addr = Some(point);
                        best_addr_id = id;
                    }
                }
            }

            // Streets
            let total_ways = street_ways.len() / std::mem::size_of::<WayHeader>() as u64;
            let total_street_nodes = street_nodes.len() / std::mem::size_of::<NodeCoord>() as u64;
            for id in Self::read_entries_fb(street_entries, offsets.street) {
                let slot = (id as usize) & 0x3F;
                if seen_streets[slot] == id { continue; }
                seen_streets[slot] = id;

                // Corrupt cell entries must not read past the record files.
                if (id as u64) >= total_ways { continue; }
                let way: WayHeader = street_ways.read_at(id as u64);
                let off = way.node_offset as u64;
                let cnt = way.node_count as usize;
                if cnt < 2 || off + cnt as u64 > total_street_nodes { continue; }
                let nbytes = street_nodes.read_chunk(off * std::mem::size_of::<NodeCoord>() as u64,
                                                     cnt * std::mem::size_of::<NodeCoord>());
                let nodes: &[NodeCoord] = unsafe {
                    std::slice::from_raw_parts(nbytes.as_ptr() as *const NodeCoord, cnt)
                };
                for i in 0..nodes.len() - 1 {
                    let dist = point_to_segment_distance(
                        lat, lng,
                        nodes[i].lat as f64, nodes[i].lng as f64,
                        nodes[i + 1].lat as f64, nodes[i + 1].lng as f64,
                        cos_lat,
                    );
                    if beats(dist, best_street_dist, || best_street.as_ref().map(|b| (street_key(&way), street_key(b)))) {
                        best_street_dist = dist;
                        best_street = Some(way);
                        best_street_idx = id;
                    }
                }
            }

            // Interpolation
            if let (Some(interp_entries), Some(interp_ways)) =
                (self.interp_entries.as_ref(), self.interp_ways.as_ref())
            {
                let total_interps = interp_ways.len() / std::mem::size_of::<InterpWay>() as u64;
                for id in Self::read_entries_fb(interp_entries, offsets.interp) {
                    // Corrupt cell entries must not read past the record files.
                    if (id as u64) >= total_interps { continue; }
                    let iw: InterpWay = interp_ways.read_at(id as u64);
                    if iw.start_number == 0 || iw.end_number == 0 { continue; }
                    let Some((dist, t)) = self.interp_way_position(&iw, lat, lng, cos_lat) else { continue };
                    if dist < best_interp_dist {
                        best_interp_dist = dist;
                        best_interp = Some(iw);
                        best_interp_t = t;
                        best_interp_id = id;
                    }
                }
            }
        }

        let addr_result = best_addr.map(|p| (best_addr_dist, p, best_addr_id));
        let street_result = best_street.map(|w| (best_street_dist, w, best_street_idx));
        let interp_result = best_interp.map(|iw| {
            let number = interpolate_house_number(
                iw.start_number, iw.end_number, iw.interpolation, best_interp_t);
            (best_interp_dist, self.get_string(iw.street_id), number, best_interp_id)
        });

        (addr_result, interp_result, street_result)
    }

    // Nominatim's `_find_housenumber_for_street` (reverse.py:231):
    // given a street that has just won primary selection, look up the
    // nearest AddrPoint whose parent_way_id matches that street's
    // index, gated by `ST_DWithin(query, 0.001)` (~100m). Returns the
    // addr_point whose housenumber should refine the result.
    pub fn find_addr_on_way(&self, lat: f64, lng: f64, parent_way_idx: u32, max_dist_sq: f64)
        -> Option<AddrPoint>
    {
        let geo_cells = self.geo_cells.as_ref()?;
        let addr_entries = self.addr_entries.as_ref()?;
        let addr_points = self.addr_points.as_ref()?;
        let total_addr = addr_points.len() / std::mem::size_of::<AddrPoint>() as u64;

        let cell = cell_id_at_level(lat, lng, self.street_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.street_cell_level);
        let cos_lat = lat.to_radians().cos();
        let addr_key = |a: &AddrPoint| {
            let d = dist_sq((a.lat as f64 - lat).to_radians(), (a.lng as f64 - lng).to_radians(), cos_lat);
            (d, self.get_string(a.housenumber_id), self.get_string(a.street_id))
        };

        let mut best_dist = max_dist_sq;
        let mut best: Option<AddrPoint> = None;
        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            let offsets = Self::lookup_geo_cell_fb(geo_cells, c);
            for id in Self::read_entries_fb(addr_entries, offsets.addr) {
                let idx = id as u64;
                if idx >= total_addr { continue; }
                let point: AddrPoint = addr_points.read_at(idx);
                if point.parent_way_id != parent_way_idx { continue; }
                let dlat = (point.lat as f64 - lat).to_radians();
                let dlng = (point.lng as f64 - lng).to_radians();
                let mut dist = dist_sq(dlat, dlng, cos_lat);
                if let Some(verts) = self.addr_polygon_verts(&point) {
                    dist = polygon_distance_sq(lat, lng, &verts, cos_lat);
                }
                if beats(dist, best_dist, || best.as_ref().map(|b| (addr_key(&point), addr_key(b)))) {
                    best_dist = dist;
                    best = Some(point);
                }
            }
        }
        best
    }

    /// interp_position over an interpolation way's nodes; None when the
    /// node range lies outside interp_nodes.bin.
    fn interp_way_position(&self, iw: &InterpWay, lat: f64, lng: f64, cos_lat: f64) -> Option<(f64, f64)> {
        let interp_nodes = self.interp_nodes.as_ref()?;
        let total_nodes = interp_nodes.len() / std::mem::size_of::<NodeCoord>() as u64;
        let off = iw.node_offset as u64;
        let cnt = iw.node_count as usize;
        if cnt < 2 || off + cnt as u64 > total_nodes { return None; }
        let bytes = interp_nodes.read_chunk(off * std::mem::size_of::<NodeCoord>() as u64,
                                            cnt * std::mem::size_of::<NodeCoord>());
        let nodes: &[NodeCoord] = unsafe {
            std::slice::from_raw_parts(bytes.as_ptr() as *const NodeCoord, cnt)
        };
        interp_position(nodes, lat, lng, cos_lat)
    }

    /// Nominatim's `_find_tiger_number_for_street` (reverse.py): when a US
    /// street wins without a matching address point, the nearest
    /// interpolation line of that street within `max_dist_sq` gives the
    /// house number. TIGER lines carry no parent street, so "of that
    /// street" is the name-token gate the TIGER postcode tier uses.
    /// Equally near lines (TIGER's odd and even sides share the street
    /// centreline) go to the lower start number, not to record order.
    pub fn find_interp_on_way(&self, lat: f64, lng: f64, street_name: &str, max_dist_sq: f64) -> Option<u32> {
        let geo_cells = self.geo_cells.as_ref()?;
        let interp_entries = self.interp_entries.as_ref()?;
        let interp_ways = self.interp_ways.as_ref()?;
        let total_interps = interp_ways.len() / std::mem::size_of::<InterpWay>() as u64;

        let cell = cell_id_at_level(lat, lng, self.street_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.street_cell_level);
        let cos_lat = lat.to_radians().cos();

        let mut best: Option<(f64, InterpWay, f64)> = None;
        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            let offsets = Self::lookup_geo_cell_fb(geo_cells, c);
            for id in Self::read_entries_fb(interp_entries, offsets.interp) {
                if (id as u64) >= total_interps { continue; }
                let iw: InterpWay = interp_ways.read_at(id as u64);
                if iw.start_number == 0 || iw.end_number == 0 { continue; }
                if !street_tokens_overlap(self.get_string(iw.street_id), street_name) { continue; }
                let Some((dist, t)) = self.interp_way_position(&iw, lat, lng, cos_lat) else { continue };
                if dist >= max_dist_sq { continue; }
                let better = match best {
                    None => true,
                    Some((bd, bw, _)) => dist < bd
                        || (dist == bd && (iw.start_number, iw.interpolation) < (bw.start_number, bw.interpolation)),
                };
                if better { best = Some((dist, iw, t)); }
            }
        }
        best.map(|(_, iw, t)| interpolate_house_number(iw.start_number, iw.end_number, iw.interpolation, t))
    }

    // Debug helper — returns primary feature distances + names.
    pub fn debug_primary(&self, lat: f64, lng: f64) -> serde_json::Value {
        let (addr, interp, street) = self.query_geo(lat, lng);
        let poi_primary = self.find_nearest_poi_with_parent(lat, lng);
        let to_m = |d2: f64| (d2.sqrt() * 6_371_000.0) as f64;
        serde_json::json!({
            "addr": addr.map(|(d, p, _id)| serde_json::json!({
                "dist_m": to_m(d),
                "house_number": if p.housenumber_id != NO_DATA { self.get_string(p.housenumber_id) } else { "" },
                "street": if p.street_id != NO_DATA { self.get_string(p.street_id) } else { "" },
                "lat": p.lat, "lng": p.lng,
                "vertex_count": p.vertex_count,
            })),
            "street": street.map(|(d, w, _idx)| serde_json::json!({
                "dist_m": to_m(d),
                "name": self.get_string(w.name_id),
            })),
            "interp": interp.map(|(d, s, n, _)| serde_json::json!({
                "dist_m": to_m(d),
                "street": s,
                "number": n,
            })),
            "poi": poi_primary.map(|(d, p)| serde_json::json!({
                "dist_m": to_m(d),
                "name": if p.name_id != NO_DATA { self.get_string(p.name_id) } else { "" },
                "parent_street": if p.parent_street_id != NO_DATA { self.get_string(p.parent_street_id) } else { "" },
                "vertex_count": p.vertex_count,
                "category": p.category,
                "category_name": self.poi_meta.category_name(p.category),
            })),
        })
    }

    pub fn debug_places(&self, lat: f64, lng: f64) -> serde_json::Value {
        let mut admin_scan = AdminScan::new(self, lat, lng);
        let admin = self.find_admin_in(&mut admin_scan);
        let current_boundaries = self.find_current_boundaries(&mut admin_scan, admin.country_code);
        let place = self.find_places(PlaceFeature::Point(lat, lng), &current_boundaries, 0);

        let place_cells = match &self.place_cells {
            Some(c) => c,
            None => return serde_json::json!({"error": "no place index"}),
        };
        let place_entries = match &self.place_entries {
            Some(e) => e,
            None => return serde_json::json!({"error": "no place index"}),
        };
        if self.place_nodes.is_none() { return serde_json::json!({"error": "no place index"}); }
        let to_m = |d2: f64| (d2.sqrt() * 6_371_000.0) as f64;
        let cos_lat = lat.to_radians().cos();
        let type_names = ["city", "town", "village", "suburb", "hamlet", "neighbourhood", "quarter"];

        // The cells find_places looks in around the point.
        let mut nearby: Vec<serde_json::Value> = Vec::new();
        for c in cells_near_point(lat, lng, MAX_PLACE_FUZZY_HALF_SIDE_M, self.admin_cell_level) {
            Self::for_each_entry_fb(place_entries, Self::lookup_admin_cell_fb(place_cells, c), |id| {
                let Some(pn) = self.place_node(id) else { return; };
                let pt = pn.place_type as usize;
                if pt >= 7 { return; }
                let dlat = (lat - pn.lat as f64).to_radians();
                let dlng = (lng - pn.lng as f64).to_radians();
                let dist_sq = dlat * dlat + dlng * dlng * cos_lat * cos_lat;
                let dist_m = to_m(dist_sq);
                if dist_m > 20000.0 { return; }
                nearby.push(serde_json::json!({
                    "type": type_names[pt],
                    "name": self.get_string(pn.name_id),
                    "dist_m": (dist_m * 10.0).round() / 10.0,
                    "lat": pn.lat, "lng": pn.lng,
                }));
            });
        }
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
        let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.admin_cell_level);

        let mut best_by_level: [Option<(f32, AdminPolygon, bool)>; 12] = [None; 12];
        let mut seen_in_level: [Vec<serde_json::Value>; 12] = Default::default();
        let (point, mut memo) = (AdminPoint::new(lat, lng), PipMemo::new());

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            Self::for_each_entry_fb(&self.admin_entries, Self::lookup_admin_cell_fb(&self.admin_cells, c), |id| {
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

    pub fn find_admin(&self, lat: f64, lng: f64) -> AdminResult<'_> {
        self.find_admin_in(&mut AdminScan::new(self, lat, lng))
    }

    /// `find_admin` over a scan the caller may share with the other admin lookups of the point.
    pub fn find_admin_in(&self, scan: &mut AdminScan) -> AdminResult<'_> {
        // For each admin level, find the smallest-area polygon actually containing
        // the point. Interior flags are used as a fast path for uncontested cells,
        // but PIP is always run when a polygon would win the smallest-area contest
        // (interior flags can be wrong at polygon borders, e.g. NJ/NY).
        // (area, id, poly, pip_verified) — owned AdminPolygon values so the
        // backing admin_polygons file can be a chunked source.
        let mut best_by_level: [Option<(f32, u32, AdminPolygon, bool)>; 12] = [None; 12];
        // Class='place' polygons (admin_level=15 marker) that cover the query point. These
        // drive the second-nested-check rule (placex_triggers.sql lines 944-963) — Nominatim
        // uses only these, not admin boundaries with linked_place tags. Kept apart from
        // best_by_level[]. Stored as (poly_id, AdminPolygon) so we can dedup by id.
        let mut place_area_polygons: Vec<(u32, AdminPolygon)> = Vec::new();

        scan.for_each_entry(|pip, id, poly| {
            // The interior flag on a (cell, polygon) entry proves
            // containment for queries inside that cell, the scan's only one.
            let is_interior = (id & INTERIOR_FLAG) != 0;
            let poly_id = id & ID_MASK;
            if poly.admin_level == 15 {
                if poly.area <= 0.0 { return; }
                if !pip.contains(self, &poly) { return; }
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

            if let Some((best_area, best_id, best_poly, best_verified)) = best_by_level[level] {
                if poly.area >= best_area {
                    // Larger than current best — skip unless current best is unverified
                    // and this one can be verified
                    if !best_verified {
                        // Current best only passed via interior flag — verify it now
                        if pip.contains(self, &best_poly) {
                            best_by_level[level] = Some((best_area, best_id, best_poly, true));
                        } else {
                            // Best was wrong — try this polygon instead
                            let inside = pip.contains(self, &poly);
                            if is_interior || inside {
                                best_by_level[level] = Some((poly.area, poly_id, poly, inside));
                            } else {
                                best_by_level[level] = None;
                            }
                        }
                    }
                    return;
                }
                // This polygon is smaller — must verify with PIP
                if pip.contains(self, &poly) {
                    best_by_level[level] = Some((poly.area, poly_id, poly, true));
                }
                // If PIP fails, keep existing best
            } else {
                // No existing candidate — accept with interior flag, verify later if contested
                if is_interior {
                    best_by_level[level] = Some((poly.area, poly_id, poly, false));
                } else if pip.contains(self, &poly) {
                    best_by_level[level] = Some((poly.area, poly_id, poly, true));
                }
            }
        });

        // Verify any uncontested winners that only passed via interior flag
        for level in 0..12 {
            if let Some((area, id, poly, false)) = best_by_level[level] {
                best_by_level[level] = scan.pip.contains(self, &poly).then_some((area, id, poly, true));
            }
        }

        let mut result = AdminResult::default();

        // Get country code from level 2 polygon
        let country_code_str: String = best_by_level[2]
            .and_then(|(_, _, poly, _)| {
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

        // Collect polygons at this point with their initial rank_address;
        // promote_nested_ranks then gives them their effective one.
        let mut ranked: Vec<RankedPoly> = Vec::new();
        for level in 0..12 {
            if let Some((_, _, poly, _)) = best_by_level[level] {
                if poly.admin_level == 11 { continue; } // postal code, handled separately

                let initial_rank = self.admin_config.to_rank(&country_code_str, poly.admin_level);
                if initial_rank == 0 && poly.place_type_override == 0 { continue; }

                ranked.push(RankedPoly { poly, rank_address: initial_rank });
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
        // Pick the smallest polygon per name and rank to avoid duplicates
        // when overlapping place areas share a name.
        for (_pid, poly) in &place_area_polygons {
            if poly.place_type_override == 0 { continue; }
            let r = place_type_to_rank(poly.place_type_override);
            if r == 0 { continue; }
            let name = self.get_string(poly.name_id);
            if place_area_repeats(ranked.iter().map(|p| (self.get_string(p.poly.name_id), p.rank_address, p.poly.area)), name, r, poly.area) {
                continue;
            }
            ranked.push(RankedPoly { poly: *poly, rank_address: r });
        }

        promote_nested_ranks(&mut ranked, place_area_polygons.iter().map(|(_, pa)| pa));
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
        let field_for = |r: &RankedPoly| -> Option<&'static str> {
            if r.poly.place_type_override > 0 {
                place_type_to_field(r.poly.place_type_override)
            } else {
                rank_to_field(r.rank_address)
            }
        };
        let candidate = |r: &RankedPoly| FieldCandidate {
            area: r.poly.area, name: self.get_string(r.poly.name_id), country_code: r.poly.country_code
        };

        for r in &ranked {
            if r.poly.admin_level == 15 { continue; } // skip place-area in first pass
            if let Some(field) = field_for(r) {
                if let Some(idx) = field_index(field) {
                    if let Some(ref existing) = best_by_field[idx] {
                        if r.poly.area >= existing.area { continue; }
                    }
                    best_by_field[idx] = Some(candidate(r));
                }
            }
        }
        // Second pass: place-area polygons fill only empty fields.
        for r in &ranked {
            if r.poly.admin_level != 15 { continue; }
            if let Some(field) = field_for(r) {
                if let Some(idx) = field_index(field) {
                    if best_by_field[idx].is_none() {
                        best_by_field[idx] = Some(candidate(r));
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

        // (area, id, polygon) of every boundary and place area near the point.
        let mut boundaries: Vec<(f32, u32, AdminPolygon)> = Vec::new();
        let mut place_areas: Vec<(f32, u32, AdminPolygon)> = Vec::new();
        scan.for_each_entry(|_, id, poly| {
            let poly_id = id & ID_MASK;
            if poly.area <= 0.0 { return; }
            match poly.admin_level {
                // ISO-less level-2 polygons are maritime lines, not countries.
                2 if poly.country_code == 0 => {}
                2..=10 => boundaries.push((poly.area, poly_id, poly)),
                15 if poly.place_type_override != 0 => place_areas.push((poly.area, poly_id, poly)),
                _ => {}
            }
        });
        let mut holding = |mut polys: Vec<(f32, u32, AdminPolygon)>| -> Vec<AdminPolygon> {
            polys.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            polys.dedup_by_key(|p| p.1);
            polys.into_iter().filter(|(_, _, poly)| scan.pip.contains(self, poly)).map(|(_, _, poly)| poly).collect()
        };
        let ranked: Vec<RankedPoly> = holding(boundaries).into_iter()
            .map(|poly| RankedPoly { poly, rank_address: self.admin_config.to_rank(&cc, poly.admin_level) })
            .filter(|r| r.rank_address != 0 || r.poly.place_type_override != 0)
            .collect();
        let inside = |poly: &AdminPolygon, current: &AdminPolygon| self.admin_geometry.point_on_surface_in(poly, current);
        CurrentBoundaries::from_ranked(ranked, holding(place_areas).iter(), inside)
    }

    pub fn find_places(&self, feature: PlaceFeature<'_>, current_boundaries: &CurrentBoundaries, held_ranks: u32) -> PlaceResult<'_> {
        let place_cells = match &self.place_cells {
            Some(c) => c,
            None => return PlaceResult::default(),
        };
        let place_entries = match &self.place_entries {
            Some(e) => e,
            None => return PlaceResult::default(),
        };
        if self.place_nodes.is_none() { return PlaceResult::default(); }

        // Every node whose fuzzy area covers the feature, with its squared
        // distance (owned copies so the place_nodes file can be a chunked
        // source).
        let mut candidates: Vec<(PlaceNode, f64)> = Vec::new();
        let consider = |id: u32| {
            let Some(pn) = self.place_node(id) else { return; };
            if let Some(dist_sq) = place_node_covering(&pn, feature) {
                candidates.push((pn, dist_sq));
            }
        };
        // A node is listed in the admin cell (L10, ~10km) of its centroid
        // only: look in every cell a node whose area covers the feature can
        // sit in. Around a point that is the point's box grown by the largest
        // fuzzy half side, a city's; along a street, a strip along it, so the
        // street's labels don't depend on where along it the query falls.
        let cells = match feature {
            PlaceFeature::Point(lat, lng) => cells_near_point(lat, lng, MAX_PLACE_FUZZY_HALF_SIDE_M, self.admin_cell_level),
            PlaceFeature::Line(line) => cells_under_line(line, MAX_PLACE_FUZZY_HALF_SIDE_M, self.admin_cell_level),
        };
        let mut ids: Vec<u32> = Vec::new();
        for c in cells {
            Self::for_each_entry_fb(place_entries, Self::lookup_admin_cell_fb(place_cells, c), |id| ids.push(id));
        }
        ids.sort_unstable();
        ids.dedup();
        ids.into_iter().for_each(consider);

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
        let accepted = accept_place_nodes(&nodes, held_ranks, |k| node_inside_boundary(&candidates[k].0, nodes[k].1));
        for (pt, slot) in [&mut result.city, &mut result.town, &mut result.village, &mut result.suburb,
                           &mut result.hamlet, &mut result.neighbourhood, &mut result.quarter].into_iter().enumerate() {
            if let Some(k) = accepted[pt] {
                *slot = Some(self.get_string(candidates[k].0.name_id));
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
    // Returns the nearest POI with a valid parent_street_id to the
    // query point. Used by query() as an additional primary-feature
    // candidate alongside the raw address / interpolation / street
    // lookups — mirrors Nominatim's _find_closest_street_or_pois
    // which returns the closest rank-26+ feature regardless of
    // whether it's a road or an addressable POI.
    //
    // For polygon POIs, returns distance 0 if the query is inside
    // the polygon, otherwise the distance to the nearest edge.
    pub fn find_nearest_poi_with_parent(&self, lat: f64, lng: f64) -> Option<(f64, PoiRecord)> {
        let poi_cells = self.poi_cells.as_ref()?;
        let poi_entries = self.poi_entries.as_ref()?;
        if self.poi_records.is_none() { return None; }

        let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.admin_cell_level);

        let cos_lat = lat.to_radians().cos();
        let mut best_dist_sq = f64::MAX;
        let mut best_idx: Option<u32> = None;
        // Fallback tier: the closest area-label POI (national park,
        // island, bay, campus). Only wins primary if no specific
        // POI qualifies — so "middle of Yosemite with no roads" can
        // surface the park, while urban queries never get "Manhattan
        // Island" as the primary feature.
        let mut fallback_dist_sq = f64::MAX;
        let mut fallback_idx: Option<u32> = None;

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            Self::for_each_entry_fb(poi_entries, Self::lookup_admin_cell_fb(poi_cells, c), |id| {
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
                // surface as the road name.
                if poi.parent_street_id == NO_DATA {
                    return;
                }
                // Always require a name on the winning POI. Unnamed
                // POIs (UNNAMED_RANK30 and name-less specific-category
                // rows) would only surface their parent_street as the
                // road — which drifts the result away from the user's
                // actual nearest street. SF query (37.7749,-122.4194):
                // a waste_basket at dist=0 currently shifts `road`
                // from Market Street (the real nearest) to South Van
                // Ness via the bin's parent_street. Dropping unnamed
                // candidates keeps the primary selection aligned with
                // human-recognisable features. (We still index them
                // for the `places[]` layer because they can provide
                // landmark context in rural areas.)
                if poi.name_id == NO_DATA {
                    return;
                }

                // Compute distance²: inside the polygon → 0;
                // otherwise min distance to any edge; for point POIs,
                // direct point-to-point distance.
                let dist_sq_val;
                if poi.vertex_count > 0 && poi.vertex_offset != NO_DATA {
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

                if Self::is_area_label_category(poi.category) {
                    // Split lane: area labels (islands, parks, bays,
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

    pub fn find_pois(&self, lat: f64, lng: f64) -> Vec<PoiMatch<'_>> {
        let poi_cells = match &self.poi_cells {
            Some(c) => c,
            None => return vec![],
        };
        let poi_entries = match &self.poi_entries {
            Some(e) => e,
            None => return vec![],
        };
        if self.poi_records.is_none() { return vec![]; }

        let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.admin_cell_level);

        let mut results: Vec<PoiMatch<'_>> = Vec::new();

        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            Self::for_each_entry_fb(poi_entries, Self::lookup_admin_cell_fb(poi_cells, c), |id| {
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
                if poi.name_id == NO_DATA { return; }
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
                // Polygon area via the shoelace formula, in squared-
                // degree units. Only computed for contained polygons
                // and used solely as a tie-break to prefer the more
                // specific polygon when multiple contain the query
                // (Eiffel Tower's ~0.0000002 over Field of Mars's
                // ~0.00001 over Banks of the Seine's much larger
                // area).
                let mut area_sq_deg = 0.0_f64;

                if poi.vertex_count > 0 && poi.vertex_offset != NO_DATA {
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

                results.push(PoiMatch {
                    name,
                    category: self.poi_meta.category_name(poi.category),
                    category_id: poi.category,
                    distance_m: if contained { 0.0 } else { dist_m },
                    contained,
                    is_point: poi.vertex_count == 0,
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
            if !seen_names.insert(r.name) { return false; }
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
        let total_polys = self.admin_polygons.len() / std::mem::size_of::<AdminPolygon>() as u64;

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
        scan.for_each_entry(|pip, _, poly| {
            if poly.admin_level != 15 { return; }
            if poly.area <= 0.0 { return; }
            if !pip.contains(self, &poly) { return; }
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
            if poly.admin_level == 11 { continue; } // postal
            // ISO-less level-2 polygons (maritime boundary closed ways)
            // can never name a country — see find_admin.
            if poly.admin_level == 2 && poly.country_code == 0 { continue; }
            let initial_rank = self.admin_config.to_rank(&country_code_str, poly.admin_level);
            if initial_rank == 0 && poly.place_type_override == 0 { continue; }
            ranked.push(RankedPoly { poly, rank_address: initial_rank });
        }
        // Add place-area polygons
        for poly in &place_area_polygons {
            if poly.place_type_override == 0 { continue; }
            let r = place_type_to_rank(poly.place_type_override);
            if r == 0 { continue; }
            ranked.push(RankedPoly { poly: *poly, rank_address: r });
        }

        promote_nested_ranks(&mut ranked, place_area_polygons.iter());
        result.boundary_ranks = rank_bits(&ranked);

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
            if let Some(field) = if r.poly.place_type_override > 0 {
                place_type_to_field(r.poly.place_type_override)
            } else {
                rank_to_field(r.rank_address)
            } {
                if let Some(idx) = field_index(field) {
                    if let Some(ref existing) = best_by_field[idx] {
                        if r.poly.area >= existing.area { continue; }
                    }
                    best_by_field[idx] = Some(FC { area: r.poly.area, name: self.get_string(r.poly.name_id), country_code: r.poly.country_code });
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
    // Mirrors Nominatim's stored `placex.postcode` chain, all of which is
    // resolved at build time by placex_triggers.sql:
    //   0. addr:postcode on the primary feature itself (token_get_postcode)
    //   1. Primary feature's calculated_postcode (inherited from containing
    //      boundary=postal_code at build time):
    //        - street → way_postcodes.bin
    //        - POI    → poi.parent_postcode_id (in-PoiRecord field)
    //   2. Nearest postcode centroid (get_nearest_postcode, 0.05°, country
    //      scoped). Mirrors the `get_nearest_postcode(...)` fallback in
    //      placex_triggers.sql rows 1052 + 1256.
    //
    // No query-time PIP on postal boundaries — Nominatim's postcode field
    // comes straight from placex.postcode, which is populated via triggers
    // at index time. `postal_polygons.bin` is consumed at BUILD time only
    // (for way_postcodes.bin / PoiRecord.parent_postcode_id inheritance);
    // it is not consulted here.
    /// Read one u32 from a per-record sidecar file (addr_postcodes /
    /// way_postcodes / interp_postcodes: u32 per record, NO_DATA = absent).
    fn sidecar_u32(f: &FileBytes, idx: u64) -> Option<u32> {
        let off = idx * 4;
        if off + 4 > f.len() {
            return None;
        }
        let bytes = f.read_chunk(off, 4);
        let v = u32::from_le_bytes(bytes[..].try_into().unwrap());
        if v == NO_DATA { None } else { Some(v) }
    }

    pub fn resolve_postcode(&self, lat: f64, lng: f64, street: Option<(usize, &WayHeader)>, addr: Option<(usize, &AddrPoint)>, poi: Option<&PoiRecord>, interp: Option<(usize, f64, &str)>, country_code: Option<[u8; 2]>) -> Option<&str> {
        // 0. Nearest addr_point's own postcode (from addr_postcodes.bin)
        // Matches Nominatim's token_get_postcode — the feature's own
        // addr:postcode tag is the highest-priority source.
        if let (Some(apc), Some((addr_id, _))) = (self.addr_postcodes.as_ref(), addr) {
            if let Some(pid) = Self::sidecar_u32(apc, addr_id as u64) {
                return Some(self.get_string(pid));
            }
        }

        // 1. way_postcodes[street.way_id] — street's computed postcode
        if let (Some(wpc), Some((way_id, _))) = (self.way_postcodes.as_ref(), street) {
            if let Some(pid) = Self::sidecar_u32(wpc, way_id as u64) {
                return Some(self.get_string(pid));
            }
        }

        // 1b. POI's calculated_postcode — build-time PIP into the smallest
        // containing boundary=postal_code polygon. Stored on PoiRecord
        // itself as `parent_postcode_id` so it rides the POI record's
        // memory locality.
        if let Some(p) = poi {
            if p.parent_postcode_id != NO_DATA {
                return Some(self.get_string(p.parent_postcode_id));
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
                if way.name_id != NO_DATA {
                    let refine = 100.0_f64 / 6_371_000.0;
                    if interp_dist <= refine * refine
                        && street_tokens_overlap(seg_street, self.get_string(way.name_id))
                    {
                        if let Some(pid) = Self::sidecar_u32(ipc, iw_idx as u64) {
                            return Some(self.get_string(pid));
                        }
                    }
                }
            }
        }

        // 2. Nearest postcode centroid (Nominatim's get_nearest_postcode).
        // Nominatim's get_nearest_postcode filters by country_code
        // (reverse.py `_find_nearest_postcode` joins location_postcode
        // on the query's resolved country). Without this filter, a
        // French "90012" can win over a US "90012" near the Maine/
        // Quebec border or vice-versa — and the same digits often
        // collide across countries. We use the country resolved by
        // find_admin as the gate; if no country is known (ocean,
        // Antarctica), fall back to unfiltered lookup so we still
        // return something.
        let country_gate: Option<u16> = country_code.map(|cc| {
            (cc[0] as u16) << 8 | (cc[1] as u16)
        });
        if let Some(ref pc_mmap) = self.postcode_centroids {
            let total_centroids = pc_mmap.len() / std::mem::size_of::<PostcodeCentroid>() as u64;
            // 0.05 deg ~ 5.5km, matches Nominatim's get_nearest_postcode
            let max_dist_sq = (0.05_f64).to_radians().powi(2);
            let cos_lat = lat.to_radians().cos();
            let mut best: Option<(f64, &str)> = None;
            let mut consider = |pc: PostcodeCentroid| {
                if let Some(cc) = country_gate {
                    if pc.country_code != 0 && pc.country_code != cc { return; }
                }
                let dlat = (lat - pc.lat as f64).to_radians();
                let dlng = (lng - pc.lng as f64).to_radians();
                let d = dlat * dlat + dlng * dlng * cos_lat * cos_lat;
                if d >= max_dist_sq { return; }
                let s = self.get_string(pc.postcode_id);
                if centroid_postcode_ok(s) && nearer_postcode(d, s, best) {
                    best = Some((d, s));
                }
            };

            if let (Some(ref cells), Some(ref entries)) = (&self.postcode_centroid_cells, &self.postcode_centroid_entries) {
                let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
                let neighbors = cell_neighbors_at_level(cell, self.admin_cell_level);
                for c in std::iter::once(cell).chain(neighbors.into_iter()) {
                    Self::for_each_entry_fb(entries, Self::lookup_admin_cell_fb(cells, c), |id| {
                        if let Some(pc) = self.postcode_centroid(id) { consider(pc); }
                    });
                }
            } else {
                // Full-scan fallback when the cell index isn't loaded.
                // O(N) but only triggers in admin-only deployments
                // where N is small.
                for i in 0..total_centroids as u32 {
                    if let Some(pc) = self.postcode_centroid(i) { consider(pc); }
                }
            }
            return best.map(|(_, s)| s);
        }

        None
    }

    // --- Combined query ---

    fn resolve_admin(&self, scan: &mut AdminScan, lat: f64, lng: f64, street: Option<&(f64, WayHeader, u32)>) -> AdminResult<'_> {
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
            // way_parents is u32 per way.
            let way_id = *way_idx as u64;
            let off = way_id * 4;
            if off + 4 > wp.len() { return None; }
            let pbytes = wp.read_chunk(off, 4);
            let start = u32::from_le_bytes(pbytes[..].try_into().unwrap());
            if start == NO_DATA { return None; }
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
            let [state, region, province] = coarse_fields(
                [chain.state, chain.region, chain.province],
                [admin_pip.state, admin_pip.region, admin_pip.province],
                || {
                    let (_, way, _) = street?;
                    let (plat, plng) = self.closest_point_on_way(way, lat, lng)?;
                    let at = self.find_admin(plat, plng);
                    Some([at.state, at.region, at.province])
                },
            );
            AdminResult { state, region, province, ..chain }
        });
        match admin_chain {
            Some(chain) => AdminResult {
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
            },
            None => admin_pip,
        }
    }

    /// The street way at `idx` in street_ways.bin, None for NO_DATA or out of range.
    fn street_way(&self, idx: u32) -> Option<WayHeader> {
        let ways = self.street_ways.as_ref()?;
        let total = ways.len() / std::mem::size_of::<WayHeader>() as u64;
        (idx != NO_DATA && (idx as u64) < total).then(|| ways.read_at::<WayHeader>(idx as u64))
    }

    /// A street way's nodes (owned, so street_nodes can be a chunked source).
    fn way_nodes(&self, way: &WayHeader) -> Option<Vec<NodeCoord>> {
        let street_nodes = self.street_nodes.as_ref()?;
        let total = street_nodes.len() / std::mem::size_of::<NodeCoord>() as u64;
        let (off, cnt) = (way.node_offset as u64, way.node_count as u64);
        if cnt < 2 || off + cnt > total { return None; }
        Some((off..off + cnt).map(|i| street_nodes.read_at::<NodeCoord>(i)).collect())
    }

    /// The point of a street way nearest (lat, lng), planar in degrees like
    /// PostGIS ST_ClosestPoint on placex geometry.
    fn closest_point_on_way(&self, way: &WayHeader, lat: f64, lng: f64) -> Option<(f64, f64)> {
        let street_nodes = self.street_nodes.as_ref()?;
        let total = street_nodes.len() / std::mem::size_of::<NodeCoord>() as u64;
        let (off, cnt) = (way.node_offset as u64, way.node_count as usize);
        if cnt < 2 || off + cnt as u64 > total { return None; }
        let bytes = street_nodes.read_chunk(off * std::mem::size_of::<NodeCoord>() as u64, cnt * std::mem::size_of::<NodeCoord>());
        let nodes: &[NodeCoord] = unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const NodeCoord, cnt) };
        closest_point_on_line(nodes, lat, lng)
    }

    // Collect the most-specific surfaceable landmark name plus the
    // name-deduped list of nearby POIs for the response.
    fn collect_landmark_and_places(&self, lat: f64, lng: f64) -> (Option<&str>, Vec<PoiDetail>) {
        let pois = self.find_pois(lat, lng);
        // Landmark: name of the smallest contained polygon POI whose
        // category is a surfaceable landmark class (filters out
        // admin-level area labels, generic UNNAMED_RANK30 and very
        // large natural / boundary features). find_pois already
        // sorts contained polygons first by area ascending, so the
        // first qualifying hit is the most specific enclosing
        // landmark.
        let landmark_name: Option<&str> = pois.iter().find_map(|p| {
            if !p.contained { return None; }
            if poi_category_to_osm_class(p.category_id).is_some() {
                Some(p.name)
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

    pub fn query(&self, lat: f64, lng: f64) -> Address<'_> {
        let max_dist = self.max_distance_sq;

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
        // on) a competing rank-16 candidate.
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
        // node (vertex_count == 0) that point_in_polygon reports as
        // inside, demote the POI so the node wins.
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
                } else if addr_pt.vertex_count == 0 && poi.vertex_count > 0
                    && poi.vertex_offset != NO_DATA {
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
        let tier1_bonus_sq: f64 = {
            let r = 50.0_f64 / 6_371_000.0_f64;
            r * r
        };
        let compare_poi_dist = match poi_primary.as_ref() {
            Some((_, poi)) if poi.tier == 1
                && poi.vertex_count > 0
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
                // query, matching nominatim's rank-30 chain walk.
                let (_, poi) = poi_primary.unwrap();
                let is_highway_poi = (poi.flags & POI_FLAG_HIGHWAY) != 0;
                let use_poi_name = is_highway_poi && poi.vertex_count > 0 && poi_dist == 0.0;
                if use_poi_name && poi.name_id != NO_DATA {
                    road = Some(self.get_string(poi.name_id));
                } else if poi.parent_street_id != NO_DATA {
                    road = Some(self.get_string(poi.parent_street_id));
                }
            } else if addr_dist <= street_dist && addr_dist <= effective_poi_dist {
                // Address is closest — use it with its housenumber.
                addr_won_primary = true;
                let (_, point, _) = addr.unwrap();
                house_number = Some(Cow::Borrowed(self.get_string(point.housenumber_id)));
                // street_id == NO_DATA means the builder's parent-
                // street backfill didn't find a named way nearby; in
                // that case fall back to the closest-street result
                // instead of the missing addr:street.
                if point.street_id != NO_DATA {
                    road = Some(self.get_string(point.street_id));
                } else if let Some((_, way, _)) = street {
                    road = Some(self.get_string(way.name_id));
                }
            } else {
                // Street is closest — use its name, then refine with a
                // housenumber if an AddrPoint whose parent_way_id matches
                // this street sits within ~100m of the query. Mirrors
                // Nominatim's `_find_housenumber_for_street` (reverse.py
                // :231): after the closest street wins, a second query
                // looks for the nearest rank-30 address point whose
                // parent_place_id matches the street's place_id, gated
                // by ST_DWithin(query, 0.001) (~100m).
                let (_, way, way_idx) = street.unwrap();
                road = if way.name_id != NO_DATA {
                    Some(self.get_string(way.name_id))
                } else {
                    None
                };
                let refine_max_sq = {
                    let r = 100.0_f64 / 6_371_000.0;
                    r * r
                };
                if let Some(addr_pt) = self.find_addr_on_way(lat, lng, way_idx, refine_max_sq) {
                    if addr_pt.housenumber_id != NO_DATA {
                        house_number = Some(Cow::Borrowed(self.get_string(addr_pt.housenumber_id)));
                    }
                }
                // No address point: Nominatim falls back to TIGER in the US.
                if house_number.is_none() && admin.country_code == Some(*b"US") {
                    if let Some(number) = road.and_then(|name| self.find_interp_on_way(lat, lng, name, refine_max_sq)) {
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
        // name). Without one, the query point stands in.
        let address_street: Option<WayHeader> = if closest_feature_dist >= max_dist {
            None
        } else if poi_won_primary {
            let (_, poi) = poi_primary.unwrap();
            street.as_ref().map(|(_, w, _)| *w).filter(|w| {
                poi.parent_street_id != NO_DATA && w.name_id != NO_DATA
                    && self.get_string(w.name_id) == self.get_string(poi.parent_street_id)
            })
        } else if addr_won_primary {
            addr.and_then(|(_, point, _)| self.street_way(point.parent_way_id))
        } else {
            street.as_ref().map(|(_, w, _)| *w)
        };
        let street_line = address_street.and_then(|w| self.way_nodes(&w));
        let feature = match street_line.as_deref() {
            Some(line) => PlaceFeature::Line(line),
            None => PlaceFeature::Point(lat, lng),
        };
        let place = self.find_places(feature, &current_boundaries, held_ranks);

        // Resolve postcode: mirror Nominatim's stored placex.postcode chain
        // (Tier 0 addr / Tier 1 primary feature's calculated_postcode /
        // Tier 2 nearest centroid). Build-time postal-boundary inheritance
        // feeds Tier 1 (way_postcodes.bin for streets, parent_postcode_id
        // for POIs); no query-time postal-boundary PIP. Tier 0 and Tier 1
        // are primary-feature-driven — only the winning feature's
        // inherited postcode is consulted (poi_won_primary is set by the
        // SAME cascade branch that picked the road above), so a POI win no
        // longer leaks the street's inherited ZIP (Mexico City Zócalo
        // 06060 leak).
        let resolved_postcode = self.resolve_postcode(
            lat, lng,
            if poi_won_primary { None } else { street.as_ref().map(|(_, w, idx)| (*idx as usize, w)) },
            if addr_won_primary { addr.as_ref().map(|(_, a, id)| (*id as usize, a)) } else { None },
            if poi_won_primary { poi_primary.as_ref().map(|(_, p)| p) } else { None },
            // TIGER tier: only when the street itself won the primary.
            // Nominatim consults location_property_tiger only after the
            // street wins and no rank-30 housenumber row was found; a POI
            // or addr-point win routes the postcode through that feature.
            if poi_won_primary || addr_won_primary { None } else {
                interp.as_ref().map(|(d, s, _, idx)| (*idx as usize, *d, *s))
            },
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
        if !anchored && !place_nodes_only {
            return Address::default();
        }

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
        // Nominatim's insert_addresslines orders candidates by
        // `rank_address, isguess asc, distance` (placex_triggers.sql
        // :590). `isguess asc` puts admin boundaries (isguess=false)
        // ahead of place nodes (isguess=true) at the same rank, so
        // admin wins the address slot within a rank tie. Mirror that
        // here for suburb / quarter / neighbourhood — earlier we did
        // place-first, which surfaced "Tverskoy" (place=suburb node)
        // instead of "Tverskoy District" (AL8 boundary), and similar
        // for Beijing "Donghuamen" vs "Donghuamen Subdistrict".
        // NYC still resolves as before because admin.suburb=Manhattan
        // and place.suburb=Manhattan carry the same name.
        let address = AddressDetails {
            landmark: landmark_name,
            // A string in a tier this mode doesn't ship (admin mode has no
            // street or addr strings) resolves to "" — omit the field.
            house_number: house_number.filter(|s| !s.is_empty()),
            road: road.filter(|s| !s.is_empty()),
            city: admin.city.or(place.city),
            town: admin.town.or(place.town),
            village: admin.village.or(place.village),
            hamlet: admin.hamlet.or(place.hamlet),
            municipality: admin.municipality,
            state: admin.state,
            province: admin.province,
            // Same-name suppression, observed Nominatim behaviour: the
            // Kingdom of the Netherlands (AL2) and the constituent country
            // (AL3) are both named "Nederland"; Nominatim's addressline
            // drops the duplicate, we surfaced it as region=Netherlands.
            region: match (admin.region, admin.country) {
                (Some(r), Some(c)) if r == c => None,
                (r, _) => r,
            },
            state_district: admin.state_district,
            county: admin.county,
            district: admin.district,
            borough: admin.borough,
            suburb: admin.suburb.or(place.suburb),
            quarter: if allow_place_quarter { admin.quarter.or(place.quarter) } else { admin.quarter },
            city_district: admin.city_district,
            neighbourhood: if allow_place_neighbourhood { admin.neighbourhood.or(place.neighbourhood) } else { admin.neighbourhood },
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

pub struct ContinentSlot {
    pub name: &'static str,
    /// Boundary that routes queries here; `None` for the flat "world" slot.
    pub continent: Option<&'static Continent>,
    /// Atomically-swappable index slot. `None` until the continent's
    /// files are loaded (or when load is asked but the dir is empty).
    /// Hot-add via `MultiIndex::install_region` swaps a fresh Arc in
    /// without locking out concurrent queries.
    pub index: arc_swap::ArcSwapOption<Index>,
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
        // Flat-layout fast path: keep existing single-region deployments
        // working without changes. The "world" slot covers the globe.
        if std::path::Path::new(&format!("{}/admin_cells.bin", root)).exists() {
            let idx = Index::load(root, street_cell_level, admin_cell_level, search_distance)?;
            return Ok(MultiIndex {
                continents: vec![ContinentSlot {
                    name: "world",
                    continent: None,
                    index: arc_swap::ArcSwapOption::from(Some(Arc::new(idx))),
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
            let arc_opt = if std::path::Path::new(&format!("{}/admin_cells.bin", dir)).exists() {
                // An interrupted or failed download leaves a dir that has
                // admin_cells.bin but can't load. It must not take down the
                // continents that did load; the slot stays empty so the
                // downloader can complete it later.
                match Index::load(&dir, street_cell_level, admin_cell_level, search_distance) {
                    Ok(idx) => Some(Arc::new(idx)),
                    Err(e) => {
                        eprintln!("skipping continent {}: {}", continent.name, e);
                        None
                    }
                }
            } else {
                None
            };
            slots.push(ContinentSlot {
                name: continent.name,
                continent: Some(continent),
                index: arc_swap::ArcSwapOption::from(arc_opt),
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

    fn covers(&self, slot: &ContinentSlot, lat: f64, lng: f64) -> bool {
        slot.continent.map_or(true, |c| c.covers_query(lat, lng, self.admin_cell_level))
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
    pub fn install_region(&self, name: &str) -> Result<(), String> {
        let slot = self.continents.iter().find(|c| c.name == name)
            .ok_or_else(|| format!("unknown continent: {}", name))?;
        let dir = format!("{}/{}", self.root_dir, name);
        let idx = Index::load(&dir,
            self.street_cell_level, self.admin_cell_level, self.search_distance)?;
        slot.index.store(Some(Arc::new(idx)));
        Ok(())
    }

    /// Reverse-geocode dispatch. Picks the best result among matching
    /// loaded continents — "best" = the one whose Address chain has the
    /// most populated fields (deeper admin chain wins). On the typical
    /// non-boundary case there's only one match and this is just a
    /// pass-through.
    ///
    /// Returns (Vec<Arc<Index>>, JSON-serialized response). The Arcs
    /// are returned alongside so the caller can keep them alive while
    /// the borrowed-string Address gets serialized — borrowing across
    /// an Arc deref is sound only while the Arc is held.
    pub fn query_json(&self, lat: f64, lng: f64) -> String {
        let arcs = self.loaded_matches(lat, lng);
        let mut best_json: Option<(i32, String)> = None;
        for arc in &arcs {
            let r = arc.query(lat, lng);
            let s = address_score(&r);
            if best_json.as_ref().map_or(true, |(prev, _)| s > *prev) {
                best_json = Some((s, serde_json::to_string(&r).unwrap_or_default()));
            }
        }
        best_json.map(|(_, j)| j).unwrap_or_else(|| "{}".to_string())
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
        let (d, t) = interp_position(&line, 0.0001, 0.0005, 1.0).unwrap();
        assert!((t - 0.25).abs() < 1e-6, "t={}", t);
        assert!((d.sqrt().to_degrees() - 0.0001).abs() < 1e-9);

        // Past the end clamps to the last node.
        let (_, t_end) = interp_position(&line, 0.0, 0.01, 1.0).unwrap();
        assert!((t_end - 1.0).abs() < 1e-9);

        // Degenerate lines have no position.
        assert!(interp_position(&line[..1], 0.0, 0.0, 1.0).is_none());
        assert!(interp_position(&[line[0], line[0]], 0.0, 0.0, 1.0).is_none());
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
        populated.address.country = Some("France");
        populated.address.city = Some("Paris");
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

    fn u32s(out: &mut Vec<u8>, values: &[u32]) {
        for v in values {
            out.extend_from_slice(&v.to_le_bytes());
        }
    }

    /// The `*_cells.bin` and `*_entries.bin` of (cell, id) pairs.
    fn cell_index(mut pairs: Vec<(u64, u32)>) -> (FileBytes, FileBytes) {
        pairs.sort();
        let (mut cells, mut entries) = (Vec::new(), Vec::new());
        for group in pairs.chunk_by(|a, b| a.0 == b.0) {
            cells.extend_from_slice(&group[0].0.to_le_bytes());
            u32s(&mut cells, &[entries.len() as u32]);
            entries.extend_from_slice(&(group.len() as u16).to_le_bytes());
            u32s(&mut entries, &group.iter().map(|p| p.1).collect::<Vec<_>>());
        }
        (FileBytes::Owned(cells), FileBytes::Owned(entries))
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
        let (mut polygons, mut vertices, mut admin_pairs) = (Vec::new(), Vec::new(), Vec::new());
        for (id, &(boundary, admin_level, [s, w, n, e])) in boundaries.iter().enumerate() {
            let ring = [s, w, s, e, n, e, n, w, s, w];
            u32s(&mut polygons, &[vertices.len() as u32, ring.len() as u32 / 2, name(boundary)]);
            polygons.extend_from_slice(&[admin_level, 0, 0, 0]);
            polygons.extend_from_slice(&((n - s) * (e - w)).to_le_bytes());
            polygons.extend_from_slice(&[0; 4]); // country_code, hole_count
            vertices.extend_from_slice(&[4, 0]); // raw f32 vertices, then the bbox minimum
            for v in [s, w].iter().chain(&ring) {
                vertices.extend_from_slice(&v.to_le_bytes());
            }
            admin_pairs.push((cell((s + n) / 2.0, (w + e) / 2.0), id as u32));
        }
        let (mut records, mut place_pairs) = (Vec::new(), Vec::new());
        for (id, &(lat, lng, node, place_type)) in nodes.iter().enumerate() {
            records.extend_from_slice(&lat.to_le_bytes());
            records.extend_from_slice(&lng.to_le_bytes());
            u32s(&mut records, &[name(node)]);
            records.extend_from_slice(&[place_type, 0, 0, 0]);
            u32s(&mut records, &[NO_DATA]);
            place_pairs.push((cell(lat, lng), id as u32));
        }
        assert_eq!(polygons.len(), boundaries.len() * std::mem::size_of::<AdminPolygon>());
        assert_eq!(records.len(), nodes.len() * std::mem::size_of::<PlaceNode>());
        let (admin_cells, admin_entries) = cell_index(admin_pairs);
        let (place_cells, place_entries) = cell_index(place_pairs);
        let end = strings.len() as u32;
        Index {
            geo_cells: None,
            street_entries: None,
            street_ways: None,
            street_nodes: None,
            addr_entries: None,
            addr_points: None,
            addr_vertices: None,
            interp_entries: None,
            interp_ways: None,
            interp_nodes: None,
            interp_postcodes: None,
            admin_cells,
            admin_entries,
            admin_polygons: FileBytes::Owned(polygons),
            admin_geometry: AdminGeometry::new(FileBytes::Owned(vertices)),
            poi_cells: None,
            poi_entries: None,
            poi_records: None,
            poi_vertices: None,
            poi_meta: PoiMeta::from_json("{}"),
            place_nodes: Some(FileBytes::Owned(records)),
            place_cells: Some(place_cells),
            place_entries: Some(place_entries),
            way_parents: None,
            admin_parents: None,
            addr_postcodes: None,
            way_postcodes: None,
            postal_polygons: None,
            postal_geometry: None,
            postcode_centroids: None,
            postcode_centroid_cells: None,
            postcode_centroid_entries: None,
            admin_config: AdminLevelConfig::load(),
            strings: StringPool::from_parts([Some(FileBytes::Owned(strings)), None, None, None, None], [0, end, end, end, end, end]),
            street_cell_level: DEFAULT_STREET_CELL_LEVEL,
            admin_cell_level: DEFAULT_ADMIN_CELL_LEVEL,
            max_distance_sq: (DEFAULT_SEARCH_DISTANCE / 6_371_000.0).powi(2),
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
    fn a_hidden_place_node_answers_like_a_nameless_one() {
        // A node a boundary claims keeps its name for search and is flagged
        // hidden (PlaceNode._pad1 bit 1) where the builder used to clear it.
        let with = |offset: usize, bytes: &[u8]| {
            let mut idx = index(&[], &[MCMURDO, SCOTT_BASE]);
            let Some(FileBytes::Owned(records)) = idx.place_nodes.as_mut() else { unreachable!() };
            records[offset..offset + bytes.len()].copy_from_slice(bytes);
            idx
        };
        let (hidden, nameless) = (with(13, &[0x02]), with(8, &NO_DATA.to_le_bytes()));

        let got = answer(&hidden, -77.8494, 166.768);

        assert_eq!(got["address"], serde_json::json!({"hamlet": "Scott Base"}));
        assert_eq!(got, answer(&nameless, -77.8494, 166.768));
    }

    #[test]
    fn a_boundary_without_a_country_city_or_state_has_no_answer() {
        let idx = index(&[SHOALS], &[SHOALS_VILLAGE]);

        assert_eq!(answer(&idx, 5.05, 112.65), serde_json::json!({"address": {}}));
    }

    #[test]
    fn a_point_nothing_holds_has_no_answer() {
        let idx = index(&[SHOALS], &[MCMURDO]);

        for (lat, lng) in [(-82.0, 60.0), (5.2, 112.65)] {
            assert_eq!(answer(&idx, lat, lng), serde_json::json!({"address": {}}), "({}, {})", lat, lng);
        }
    }

    #[test]
    fn a_place_node_gate_without_rings_accepts_every_node() {
        let idx = index(&[SHOALS, ("Elsewhere", 8, [0.0, 0.0, 0.1, 0.1])], &[SHOALS_VILLAGE]);
        let elsewhere = idx.admin_polygon(1).unwrap();
        let village = |gate: AdminPolygon| {
            let gates = CurrentBoundaries::walk([(CurrentBoundaries::CITY, gate)], |_, _| true);
            idx.find_places(PlaceFeature::Point(5.05, 112.65), &gates, 0).village
        };

        assert_eq!(village(elsewhere), None);
        assert_eq!(village(AdminPolygon { vertex_count: 0, ..elsewhere }), Some("Shoals Village"));
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

// Quick validation for postcode centroid candidates. Rejects
// strings that are clearly not standalone postcodes — matches
// the spirit of Nominatim's clean_postcodes sanitizer which
// validates against per-country regex patterns at import.
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

pub fn centroid_postcode_ok(s: &str) -> bool {
    if s.len() > 10 { return false; }  // postcodes are short
    if s.contains(';') { return false; } // multiple values
    // NOTE: no PO/Box substring filter — Nominatim has none. It wrongly dropped
    // every legitimate UK Portsmouth-area postcode (PO1 1BA, etc.). The
    // per-country pattern is enforced at build time (postcode_validation.h) and
    // placeholders (all-zeros/dashes) by postcode_looks_valid.
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
    const PLANAR: f64 = 1.0; // no cos(lat) scaling of longitude
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

pub fn point_to_segment_distance(
    px: f64, py: f64,
    ax: f64, ay: f64,
    bx: f64, by: f64,
    cos_lat: f64,
) -> f64 {
    point_to_segment_with_t(px, py, ax, ay, bx, by, cos_lat).0
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

/// A ring inside a polygon's keyhole vertex range: `count` vertices from
/// `begin`, the last repeating the first (builder/src/polygon_rings.h).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingSpan {
    pub begin: usize,
    pub count: usize,
}

/// Vertices of one keyhole excursion beyond its hole ring: v0, c before it
/// and c after (polygon_rings.h kExcursionConnectors).
const EXCURSION_CONNECTORS: usize = 3;
/// A closed ring's fewest vertices: a triangle and its closing vertex.
const MIN_CLOSED_RING: usize = 4;

/// The outer ring (first) and the holes of a keyhole vertex range with
/// `holes` excursions, as polygon_rings.h split_rings finds them: each
/// excursion is v0, c, h0 ... h0, c, peeled off the end, the hole ring
/// starting at the last h0 that follows v0 and c. A range that doesn't parse
/// as that is one ring.
pub fn split_rings(v: &[NodeCoord], holes: u16) -> Vec<RingSpan> {
    let same = |a: &NodeCoord, b: &NodeCoord| a.lat == b.lat && a.lng == b.lng;
    let whole = vec![RingSpan { begin: 0, count: v.len() }];
    let mut spans = Vec::with_capacity(holes as usize + 1);
    let mut end = v.len();
    for _ in 0..holes {
        if end < MIN_CLOSED_RING + EXCURSION_CONNECTORS + MIN_CLOSED_RING {
            return whole;
        }
        let (c, h0) = (&v[end - 1], &v[end - 2]);
        let Some(j) = (MIN_CLOSED_RING + 2..=end - 1 - MIN_CLOSED_RING).rev()
            .find(|&j| same(&v[j], h0) && same(&v[j - 1], c) && same(&v[j - 2], &v[0]))
        else {
            return whole;
        };
        spans.push(RingSpan { begin: j, count: end - 1 - j });
        end = j - 2;
    }
    if end < MIN_CLOSED_RING || !same(&v[end - 1], &v[0]) {
        return whole;
    }

    spans.push(RingSpan { begin: 0, count: end });
    spans.reverse();
    spans
}

/// A polygon's rings for GeoJSON: outer ring first, then its holes, each a
/// closed ring of [lng, lat] positions.
pub fn geojson_rings(verts: &[NodeCoord], holes: u16) -> Vec<Vec<[f64; 2]>> {
    split_rings(verts, holes).iter()
        .map(|r| verts[r.begin..r.begin + r.count].iter().map(|p| [p.lng as f64, p.lat as f64]).collect())
        .collect()
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

/// `point_in_polygon` over a polygon's encoded vertices (the layout `decode_polygon_verts`
/// reads), decoding one vertex at a time to the same f32 values: no allocation, one pass. None
/// exactly where `decode_polygon_verts` is None. A point the header's bounds already exclude is
/// answered without reading the vertices.
///
/// Never inlined, so the per-vertex loops get registers of their own: inlined into
/// `AdminGeometry::contains`, whose memo values stay live across the test, the u32 grid loop
/// reloaded its vertex pointer from the stack, 15 instructions a vertex instead of 13.
#[inline(never)]
pub fn point_in_encoded_polygon(src: &FileBytes, vertex_offset: u32, vertex_count: u32, lat: f32, lng: f32) -> Option<bool> {
    let EncodedPolygon { encoding, bmin_lat: bmin_lat32, bmin_lng: bmin_lng32, verts_off } =
        encoded_polygon(src, vertex_offset, vertex_count)?;
    let stride = vertex_stride(encoding);
    let n = vertex_count as usize;
    if ring_bounds_exclude(encoding, bmin_lat32, bmin_lng32, lat, lng) { return Some(false); }
    let (bmin_lat, bmin_lng) = (bmin_lat32 as f64, bmin_lng32 as f64);
    let bytes = src.read_chunk(verts_off, n * stride);
    let scale = vertex_scale(encoding);
    let u16_at = |b: &[u8], i: usize| u16::from_le_bytes([b[i], b[i + 1]]) as i64;
    let u32_at = |b: &[u8], i: usize| u32::from_le_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]]);
    Some(match encoding {
        4 => pip_vertices(&bytes, 8, lat, lng, |b| (f32::from_bits(u32_at(b, 0)), f32::from_bits(u32_at(b, 4)))),
        3 => pip_grid(&bytes, 8, lat, lng, bmin_lat, bmin_lng, scale, u32::MAX as i64, |b| (u32_at(b, 0) as i64, u32_at(b, 4) as i64)),
        _ => pip_grid(&bytes, 4, lat, lng, bmin_lat, bmin_lng, scale, u16::MAX as i64, |b| (u16_at(b, 0), u16_at(b, 2))),
    })
}

/// Largest k in -1..=k_max with `(base + k * scale) as f32 <= x`; -1 when even k = 0 decodes
/// above x. The decoded value is monotone in k (every step rounds monotonically), so a vertex
/// whose k is in 0..=k_max decodes above x exactly when its k is above this. Comparisons with NaN
/// are false: a NaN x or base gives k_max, no vertex above x. The search gallops out from the
/// estimate, so any x (±inf, NaN, far off the grid) takes a few dozen steps at most.
#[inline]
pub fn grid_threshold(x: f32, base: f64, scale: f64, k_max: i64) -> i64 {
    let above = |k: i64| (base + k as f64 * scale) as f32 > x;
    let estimate = ((x as f64 - base) / scale).floor();
    // `as` saturates to the i64 range, and would turn NaN into 0.
    let start = if estimate.is_nan() { k_max } else { (estimate as i64).clamp(-1, k_max) };
    // Bracket the answer: lo is -1 or not above x, hi is above x or k_max + 1.
    let (mut lo, mut hi) = (start, start + 1);
    let mut step = 1;
    while lo >= 0 && above(lo) {
        hi = lo;
        lo = (lo - step).max(-1);
        step *= 2;
    }
    step = 1;
    while hi <= k_max && !above(hi) {
        lo = hi;
        hi = (hi + step).min(k_max + 1);
        step *= 2;
    }
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if above(mid) { hi = mid } else { lo = mid }
    }
    lo
}

/// `pip_vertices` over quantized vertices (k_lat, k_lng) in 0..=k_max that decode to `(bmin + k *
/// scale) as f32`: the straddle test compares integers against `grid_threshold` of the point's
/// longitude, and only the straddling edges decode, to the same values, so the answer is the same.
#[inline]
#[allow(clippy::too_many_arguments)]
fn pip_grid(bytes: &[u8], stride: usize, lat: f32, lng: f32, bmin_lat: f64, bmin_lng: f64, scale: f64, k_max: i64,
            at: impl Fn(&[u8]) -> (i64, i64)) -> bool {
    let t = grid_threshold(lng, bmin_lng, scale, k_max);
    let decode = |k_lat: i64, k_lng: i64| ((bmin_lat + k_lat as f64 * scale) as f32, (bmin_lng + k_lng as f64 * scale) as f32);
    let n = bytes.len() / stride;
    let mut inside = false;
    let (mut kj_lat, mut kj_lng) = at(&bytes[(n - 1) * stride..]);
    for c in bytes.chunks_exact(stride) {
        let (ki_lat, ki_lng) = at(c);
        if (ki_lng > t) != (kj_lng > t) {
            let (vi_lat, vi_lng) = decode(ki_lat, ki_lng);
            let (vj_lat, vj_lng) = decode(kj_lat, kj_lng);
            if lat < (vj_lat - vi_lat) * (lng - vi_lng) / (vj_lng - vi_lng) + vi_lat {
                inside = !inside;
            }
        }
        kj_lat = ki_lat;
        kj_lng = ki_lng;
    }
    inside
}

/// True when `point_in_polygon` on a ring of encoding `enc` with header bbox minimum
/// (`bmin_lat`, `bmin_lng`) is false at (`lat`, `lng`) whatever the vertices. Encodings 0-3 decode
/// every vertex to `(bmin + k * scale) as f32` with k >= 0, so no vertex is below the minimum; the
/// u16 encodings 0-2 also bound k by 65535. Outside the bounds in longitude, no edge straddles the
/// point's meridian. Outside them in latitude, every straddling edge crosses north of the point
/// (or none does), and a closed ring has an even number of straddling edges.
fn ring_bounds_exclude(enc: u8, bmin_lat: f32, bmin_lng: f32, lat: f32, lng: f32) -> bool {
    if enc > 3 { return false; }
    if lng < bmin_lng { return true; }
    let latitude_test = lng.abs() >= PIP_LAT_BOUND_MIN_ABS_LNG;
    if latitude_test && lat < bmin_lat - PIP_LAT_BOUND_SLACK_DEG { return true; }
    if enc == 3 { return false; }
    let span = U16_VERTEX_MAX_STEPS * vertex_scale(enc);
    let (bmax_lat, bmax_lng) = ((bmin_lat as f64 + span) as f32, (bmin_lng as f64 + span) as f32);
    lng > bmax_lng || (latitude_test && lat > bmax_lat + PIP_LAT_BOUND_SLACK_DEG)
}

/// The `point_in_polygon` ray cast over packed vertices of one encoding (`at` decodes one).
#[inline]
fn pip_vertices(bytes: &[u8], stride: usize, lat: f32, lng: f32, at: impl Fn(&[u8]) -> (f32, f32)) -> bool {
    let n = bytes.len() / stride;
    let mut inside = false;
    let (mut vj_lat, mut vj_lng) = at(&bytes[(n - 1) * stride..]);
    for c in bytes.chunks_exact(stride) {
        let (vi_lat, vi_lng) = at(c);
        if ((vi_lng > lng) != (vj_lng > lng))
            && (lat < (vj_lat - vi_lat) * (lng - vi_lng) / (vj_lng - vi_lng) + vi_lat)
        {
            inside = !inside;
        }
        vj_lat = vi_lat;
        vj_lng = vi_lng;
    }
    inside
}

/// A point inside a polygon's keyhole vertex range as GEOS's
/// InteriorPointArea finds it (PostGIS ST_PointOnSurface, which Nominatim's
/// get_center_point uses for areas), the same as the builder's
/// interior_point (polygon_rings.h): the midpoint of the widest inside
/// stretch of the line of latitude half-way between the vertex latitudes of
/// all rings nearest the middle of the outer ring's latitude range. The
/// first vertex where no stretch has width; None for no vertices.
/// (lat, lng).
pub fn point_on_surface(verts: &[NodeCoord], holes: u16) -> Option<(f32, f32)> {
    let first = verts.first()?;
    let rings: Vec<&[NodeCoord]> = split_rings(verts, holes).iter().map(|r| &verts[r.begin..r.begin + r.count]).collect();
    let (mut lo, mut hi) = rings[0].iter().fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), p| (lo.min(p.lat as f64), hi.max(p.lat as f64)));
    let centre = (lo + hi) / 2.0;
    for p in rings.iter().flat_map(|r| r.iter()) {
        let y = p.lat as f64;
        if y <= centre {
            if y > lo { lo = y; }
        } else if y < hi {
            hi = y;
        }
    }
    let scan = (lo + hi) / 2.0;
    let mut crossings: Vec<f64> = Vec::new();
    for ring in &rings {
        for edge in ring.windows(2) {
            let (y0, y1) = (edge[0].lat as f64, edge[1].lat as f64);
            let (x0, x1) = (edge[0].lng as f64, edge[1].lng as f64);
            if (y0 > scan && y1 > scan) || (y0 < scan && y1 < scan) || y0 == y1 { continue; }
            if y0 == scan && y1 < scan { continue; }  // a downward edge leaves out its start
            if y1 == scan && y0 < scan { continue; }  // an upward edge leaves out its end
            crossings.push(if x0 == x1 { x0 } else { x0 + (scan - y0) / ((y1 - y0) / (x1 - x0)) });
        }
    }
    crossings.sort_by(|a, b| a.total_cmp(b));
    let mut best = (first.lat, first.lng);
    let mut best_width = 0.0;
    for run in crossings.chunks_exact(2) {
        if run[1] - run[0] > best_width {
            best_width = run[1] - run[0];
            best = (scan as f32, ((run[0] + run[1]) / 2.0) as f32);
        }
    }
    Some(best)
}

/// Whether a polygon's point on surface lies inside another polygon, not on
/// its edges (ST_Contains): Nominatim's test of a boundary's centroid
/// against the current boundary.
pub fn point_on_surface_in(verts: &[NodeCoord], holes: u16, other: &[NodeCoord], other_holes: u16) -> bool {
    !other.is_empty()
        && point_on_surface(verts, holes)
            .is_some_and(|(lat, lng)| point_in_polygon(lat, lng, other) && !on_ring_edge(lat, lng, other, other_holes))
}

/// Whether a point lies on an edge of a polygon's outer ring or holes (not
/// on a keyhole connector).
fn on_ring_edge(lat: f32, lng: f32, verts: &[NodeCoord], holes: u16) -> bool {
    let (y, x) = (lat as f64, lng as f64);
    split_rings(verts, holes).iter().any(|r| {
        verts[r.begin..r.begin + r.count].windows(2).any(|e| {
            let (y0, x0, y1, x1) = (e[0].lat as f64, e[0].lng as f64, e[1].lat as f64, e[1].lng as f64);
            (x1 - x0) * (y - y0) == (y1 - y0) * (x - x0)
                && (x0.min(x1)..=x0.max(x1)).contains(&x)
                && (y0.min(y1)..=y0.max(y1)).contains(&y)
        })
    })
}

// --- API types ---

pub struct PoiMatch<'a> {
    pub name: &'a str,
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
    .then_with(|| a.name.cmp(b.name))
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
pub fn coarse_fields<'a>(chain: [Option<&'a str>; 3], query: [Option<&'a str>; 3],
                         at_street: impl FnOnce() -> Option<[Option<&'a str>; 3]>) -> [Option<&'a str>; 3] {
    if chain == query {
        return chain;
    }
    let Some(at) = at_street() else { return chain; };

    std::array::from_fn(|k| at[k].or(chain[k]))
}

/// rank_address of each PlaceNode type (0 city, 1 town, 2 village, 3 suburb,
/// 4 hamlet, 5 neighbourhood, 6 quarter), from address-levels.json: quarter
/// (22) and neighbourhood (24) are ranks of their own.
pub const PLACE_NODE_RANK: [u8; 7] = [16, 16, 16, 20, 20, 24, 22];

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
    const PLANAR: f64 = 1.0; // no cos(lat) scaling of longitude
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
pub fn accept_place_nodes(nodes: &[(usize, u8, f64, f32, f32)], held_ranks: u32, mut in_boundary: impl FnMut(usize) -> bool) -> [Option<usize>; 7] {
    let held = |rank: u8| held_ranks & (1 << rank) != 0;
    let rank = |k: usize| nodes[k].1;
    let weighed = |k: usize| {
        let (pt, rank, dist_sq, _, _) = nodes[k];
        dist_sq.sqrt() * if rank == CurrentBoundaries::CITY { PLACE_NODE_DISTANCE_WEIGHT[pt] } else { 1.0 }
    };
    let mut order: Vec<usize> = (0..nodes.len()).filter(|&k| !held(rank(k))).collect();
    // Ties: type, then position, so the walk doesn't depend on record order.
    order.sort_by(|&a, &b| {
        rank(a).cmp(&rank(b))
            .then(weighed(a).total_cmp(&weighed(b)))
            .then(nodes[a].0.cmp(&nodes[b].0))
            .then(nodes[a].3.total_cmp(&nodes[b].3))
            .then(nodes[a].4.total_cmp(&nodes[b].4))
    });

    let mut accepted = [None; 7];
    let mut last_rank = 0;
    // The last accepted node's fuzzy area: (lat, lng, half side in metres).
    let mut fuzzy: Option<(f32, f32, f64)> = None;
    for k in order {
        let (pt, _, _, lat, lng) = nodes[k];
        if rank(k) == last_rank { continue; }
        if (last_rank + 1..=rank(k)).any(held) {
            fuzzy = None;
        }
        let in_fuzzy = fuzzy.map_or(true, |(lat0, lng0, half_m)| fuzzy_area_holds(lat0, lng0, half_m, lat as f64, lng as f64));
        if !in_fuzzy || !in_boundary(k) { continue; }

        accepted[pt] = Some(k);
        last_rank = rank(k);
        fuzzy = Some((lat, lng, place_fuzzy_half_side_m(pt)));
    }
    accepted
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
    /// of the place areas, holding the point at a PROMOTABLE_RANKS rank.
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
    /// initial rank_address and the place areas holding it: the nested-
    /// boundary promotions give each its rank_address (an admin_level 7
    /// boundary in a level 6 one linked to a suburb is ranked 22, not 14),
    /// then `walk` takes them as Nominatim's address walk does.
    pub fn from_ranked<'a>(
        mut ranked: Vec<RankedPoly>,
        place_areas: impl Iterator<Item = &'a AdminPolygon> + Clone,
        inside: impl Fn(&AdminPolygon, &AdminPolygon) -> bool,
    ) -> Self {
        promote_nested_ranks(&mut ranked, place_areas.clone());
        ranked.sort_by(|a, b| a.poly.area.total_cmp(&b.poly.area));
        let promoting = ranked.iter().map(|r| (r.rank_address, r.poly))
            .chain(place_areas.map(|p| (place_type_to_rank(p.place_type_override), *p)))
            .filter(|(rank, _)| PROMOTABLE_RANKS.contains(rank))
            .collect();
        let gating = ranked.into_iter().filter(|r| !matches!(r.rank_address, 0 | 5 | 11));
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
    pub city: Option<&'a str>,
    pub town: Option<&'a str>,
    pub village: Option<&'a str>,
    pub hamlet: Option<&'a str>,
    pub suburb: Option<&'a str>,
    pub quarter: Option<&'a str>,
    pub neighbourhood: Option<&'a str>,
}

impl PlaceResult<'_> {
    /// Whether no place node fills a field.
    pub fn is_empty(&self) -> bool {
        [self.city, self.town, self.village, self.hamlet, self.suburb, self.quarter, self.neighbourhood]
            .iter().all(Option::is_none)
    }
}

#[derive(Default)]
pub struct AdminResult<'a> {
    pub country: Option<&'a str>,
    pub country_code: Option<[u8; 2]>,
    pub state: Option<&'a str>,
    pub province: Option<&'a str>,
    pub region: Option<&'a str>,
    pub state_district: Option<&'a str>,
    pub county: Option<&'a str>,
    pub district: Option<&'a str>,
    pub municipality: Option<&'a str>,
    pub city: Option<&'a str>,
    pub town: Option<&'a str>,
    pub village: Option<&'a str>,
    pub hamlet: Option<&'a str>,
    pub city_district: Option<&'a str>,
    pub borough: Option<&'a str>,
    pub suburb: Option<&'a str>,
    pub quarter: Option<&'a str>,
    pub neighbourhood: Option<&'a str>,
    pub city_block: Option<&'a str>,
    /// rank_address of the boundaries in the address, a bit per rank: the
    /// ranks they take before any place node (address_havelevel).
    pub boundary_ranks: u32,
}

impl AdminResult<'_> {
    /// Whether no polygon fills a field.
    pub fn is_empty(&self) -> bool {
        [self.country, self.state, self.province, self.region, self.state_district, self.county,
         self.district, self.municipality, self.city, self.town, self.village, self.hamlet,
         self.city_district, self.borough, self.suburb, self.quarter, self.neighbourhood, self.city_block]
            .iter().all(Option::is_none)
    }
}

/// The rank_address bits of the polygons in an address (AdminResult::boundary_ranks).
pub fn rank_bits(ranked: &[RankedPoly]) -> u32 {
    ranked.iter()
        .filter(|r| (1..u32::BITS as u8).contains(&r.rank_address))
        .fold(0, |bits, r| bits | 1 << r.rank_address)
}

#[derive(Serialize, Default)]
pub struct AddressDetails<'a> {
    // Landmark field: set to the name of the smallest-area
    // polygon POI that contains the query point. Category details
    // (specific type, distance, …) live in places[], so a single
    // field here is enough to let display_name lead with the
    // landmark name while clients still know the "what" via
    // places[0].category.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub landmark: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub house_number: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub road: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub city: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub town: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub village: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hamlet: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub municipality: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub province: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_district: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub county: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub district: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub borough: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suburb: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quarter: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub city_district: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub neighbourhood: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub city_block: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postcode: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country_code: Option<String>,
}

#[derive(Serialize)]
pub struct PoiDetail {
    pub name: String,
    pub category: String,
    pub distance_m: f64,
}

#[derive(Serialize, Default)]
pub struct Address<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub address: AddressDetails<'a>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub places: Vec<PoiDetail>,
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
// (`src/nominatim_api/results.py`): iterate address_rows sorted by
// rank_address descending (fine → coarse), filtering by isaddress flag,
// and dedup *consecutively* — only skip a row whose display_name matches
// the previous pushed entry, not globally. Country and postcode are
// appended at the end as pseudo-entries in Nominatim's _finalize_entry.
//
// Iteration order below uses rank_address values read verbatim from
// Nominatim's `settings/address-levels.json` — any future divergence
// should only happen if Nominatim's upstream file changes.
//
//   30 = house_number, road
//   25 = city_block
//   24 = neighbourhood
//   22 = quarter
//   20 = hamlet, suburb
//   18 = borough
//   16 = city, town, village
//   14 = municipality
//   12 = county, district
//    8 = state  (from boundary=administrative4, not place=state)
//    5 = postcode
//    4 = country
//
// Notes:
// - place=state/province/region all have rank_address=0 in Nominatim
//   (excluded from the address chain at the place-tag level). State
//   comes from boundary=administrative4, which is rank 8.
// - Our AddressDetails fields `region`, `province`, `state_district`
//   are populated by our rank_to_field fallback for admin levels that
//   don't map to a canonical Nominatim field. We place them between
//   state and country — Nominatim's equivalent is admin_level=3 at
//   rank 6, so between state (8) and country (4).
// - `city_district` is not a Nominatim place-tag. We emit it at rank 16
//   (next to city) as a practical fallback for our builder's output.
pub fn format_address(addr: &AddressDetails<'_>) -> Option<String> {
    let has_locality = addr.city.is_some() || addr.town.is_some()
        || addr.village.is_some() || addr.municipality.is_some()
        || addr.hamlet.is_some() || addr.suburb.is_some()
        || addr.borough.is_some();
    if addr.road.is_none() && !has_locality && addr.country.is_none() {
        return None;
    }

    let (number_after, _, _) = format_rules(addr.country_code.as_deref());

    // Collect (rank_address, value) tuples for every populated field.
    // We emit in rank-descending order (fine → coarse) but dedupe by
    // value, keeping only the COARSEST-rank occurrence of each string.
    // This matches cases like NYC where city="New York" and state="New
    // York" share a value — Nominatim's output retains the state slot
    // and drops the duplicate city.
    let mut entries: Vec<(u8, String)> = Vec::new();

    // Rank 32: landmark name. Higher than road so the landmark
    // leads the display line — "Eiffel Tower, 5 Avenue Gustave
    // Eiffel, Paris, France" reads better than the road alone.
    // The category / OSM type is carried on places[0] for clients
    // that need it, so a single field here is enough.
    if let Some(s) = addr.landmark { entries.push((32, s.to_string())); }

    // Rank 30: house_number + road
    if let Some(road) = addr.road {
        let s = if let Some(ref hn) = addr.house_number {
            if number_after {
                format!("{} {}", road, hn)
            } else {
                format!("{} {}", hn, road)
            }
        } else {
            road.to_string()
        };
        entries.push((30, s));
    }

    // Rank 25: city_block
    if let Some(s) = addr.city_block { entries.push((25, s.to_string())); }
    // Rank 24: neighbourhood
    if let Some(s) = addr.neighbourhood { entries.push((24, s.to_string())); }
    // Rank 22: quarter
    if let Some(s) = addr.quarter { entries.push((22, s.to_string())); }
    // Rank 20: hamlet, suburb
    if let Some(s) = addr.hamlet { entries.push((20, s.to_string())); }
    if let Some(s) = addr.suburb { entries.push((20, s.to_string())); }
    // Rank 18: borough
    if let Some(s) = addr.borough { entries.push((18, s.to_string())); }
    // Rank 16: city, town, village, city_district
    if let Some(s) = addr.city { entries.push((16, s.to_string())); }
    if let Some(s) = addr.city_district { entries.push((16, s.to_string())); }
    if let Some(s) = addr.town { entries.push((16, s.to_string())); }
    if let Some(s) = addr.village { entries.push((16, s.to_string())); }
    // Rank 14: municipality
    if let Some(s) = addr.municipality { entries.push((14, s.to_string())); }
    // Rank 12: county, district
    if let Some(s) = addr.district { entries.push((12, s.to_string())); }
    if let Some(s) = addr.county { entries.push((12, s.to_string())); }
    // Rank 8: state + fallbacks (state_district, region, province come
    // from admin polygons via rank_to_field — grouped with state here,
    // but iterated in a coarser-than-state order below)
    if let Some(s) = addr.state { entries.push((8, s.to_string())); }
    // state_district / region / province slot in between state and
    // country at rank 6 (corresponds to boundary=administrative3 in
    // Nominatim, which is rank_address=6).
    if let Some(s) = addr.state_district { entries.push((6, s.to_string())); }
    if let Some(s) = addr.region { entries.push((6, s.to_string())); }
    if let Some(s) = addr.province { entries.push((6, s.to_string())); }
    // Rank 5: postcode
    if let Some(ref pc) = addr.postcode { entries.push((5, pc.to_string())); }
    // Rank 4: country
    if let Some(s) = addr.country { entries.push((4, s.to_string())); }

    // Global value dedup: for each unique value, find the COARSEST
    // (lowest rank) occurrence. Drop finer-rank duplicates. This mirrors
    // Nominatim's behaviour of preferring the state/county "authority"
    // over a finer duplicate like city=New York vs state=New York.
    let mut coarsest: std::collections::HashMap<String, u8> =
        std::collections::HashMap::new();
    for (rank, val) in &entries {
        coarsest
            .entry(val.clone())
            .and_modify(|r| if *rank < *r { *r = *rank })
            .or_insert(*rank);
    }

    // Emit in rank-descending (fine → coarse) order, keeping only
    // entries whose rank matches the coarsest rank for their value.
    // Consecutive dedup is implicit — if two different entries at the
    // same rank have different values, both are kept.
    let mut parts: Vec<String> = Vec::new();
    let mut emitted: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    for (rank, val) in &entries {
        if val.is_empty() { continue; }
        if coarsest[val] != *rank { continue; }
        if !emitted.insert(val.clone()) { continue; }
        parts.push(val.clone());
        let _ = rank; // silence unused-binding warning
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
        PoiMatch { name, category: "", category_id, distance_m: 0.0, contained, is_point: !contained, score, area }
    }

    fn sorted_names(mut pois: Vec<PoiMatch<'_>>) -> Vec<&str> {
        pois.sort_by(poi_order);
        pois.iter().map(|p| p.name).collect()
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
        // > 10 chars rejected.
        assert!(!centroid_postcode_ok("12345678901"));
        // semicolon (multiple values) rejected.
        assert!(!centroid_postcode_ok("10001;10002"));
        // No PO/Box substring filter: legitimate UK Portsmouth-area postcodes
        // (the "PO" postcode area) must NOT be dropped — Nominatim has no such
        // filter. This was a real divergence (Portsmouth returned no postcode).
        assert!(centroid_postcode_ok("PO1 1BA"));
        assert!(centroid_postcode_ok("PO16 7GZ"));
        // US zip+4 (len 10, dash at index 5) rejected.
        assert!(!centroid_postcode_ok("10001-2062"));
        // A 9-char dashed value is NOT caught by the len==10 zip+4 rule.
        assert!(centroid_postcode_ok("1001-2062"));
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
        assert_eq!(place_type_to_rank(1), 16);
        assert_eq!(place_type_to_rank(2), 16);
        assert_eq!(place_type_to_rank(3), 16);
        assert_eq!(place_type_to_rank(4), 20);
        assert_eq!(place_type_to_rank(5), 24);
        assert_eq!(place_type_to_rank(6), 22);
        // NOTE: state/province/region map to 0 (deliberately not in address chain).
        assert_eq!(place_type_to_rank(7), 0);
        assert_eq!(place_type_to_rank(8), 0);
        assert_eq!(place_type_to_rank(9), 0);
        assert_eq!(place_type_to_rank(10), 12);
        assert_eq!(place_type_to_rank(11), 12);
        assert_eq!(place_type_to_rank(12), 18);
        assert_eq!(place_type_to_rank(13), 20);
        assert_eq!(place_type_to_rank(14), 14);
        assert_eq!(place_type_to_rank(0), 0);
        assert_eq!(place_type_to_rank(255), 0);
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
        // NOTE: UNNAMED_RANK30=170 -> None (generic filler).
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
        for cat in [80u8, 81, 82, 86, 91, 92, 93, 60, 61, 62, 63, 64, 65,
                    130, 131, 41, 42, 43, 47, 51, 54, 3, 4, 9, 11, 25, 100, 150] {
            assert!(Index::is_area_label_category(cat), "expected {cat} to be an area label");
        }
        // A representative sample of non-matches.
        for cat in [0u8, 1, 2, 5, 40, 44, 50, 66, 70, 90, 120, 170, 200, 255] {
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
        a.address.country = Some("X");       // +1
        a.address.state = Some("X");         // +2
        a.address.county = Some("X");        // +3
        a.address.city = Some("X");          // +4
        a.address.road = Some("X");          // +5
        a.address.house_number = Some(Cow::Borrowed("1")); // +6
        assert_eq!(address_score(&a), 1 + 2 + 3 + 4 + 5 + 6);

        // A populated result outscores an emptier one (monotonic refinement).
        let mut coarse = Address::default();
        coarse.address.country = Some("France");
        let mut fine = Address::default();
        fine.address.country = Some("France");
        fine.address.city = Some("Paris");
        fine.address.road = Some("Rue de Rivoli");
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

    // The keyhole range of a closed outer ring and closed holes, laid out as
    // polygon_rings.h encode_rings writes it: the outer ring, then per hole
    // v0, c = (v0.lat, h0.lng), the hole ring, c.
    fn keyhole(outer: &[NodeCoord], holes: &[Vec<NodeCoord>]) -> Vec<NodeCoord> {
        let mut v = outer.to_vec();
        for h in holes {
            let c = nc(outer[0].lat, h[0].lng);
            v.extend([outer[0], c]);
            v.extend_from_slice(h);
            v.push(c);
        }
        v
    }

    // Outer square 0..10 with holes 2..4 and (6..7, 5..8), all closed.
    fn square_with_two_holes() -> (Vec<NodeCoord>, Vec<Vec<NodeCoord>>) {
        let outer = vec![nc(0.0, 0.0), nc(0.0, 10.0), nc(10.0, 10.0), nc(10.0, 0.0), nc(0.0, 0.0)];
        let holes = [(2.0, 2.0, 4.0, 4.0), (6.0, 5.0, 7.0, 8.0)].map(|(lat0, lng0, lat1, lng1)| {
            vec![nc(lat0, lng0), nc(lat1, lng0), nc(lat1, lng1), nc(lat0, lng1), nc(lat0, lng0)]
        });
        (outer, holes.to_vec())
    }

    #[test]
    fn split_rings_recovers_the_keyhole_rings() {
        let (outer, holes) = square_with_two_holes();
        let v = keyhole(&outer, &holes);
        let span = |begin, count| RingSpan { begin, count };
        assert_eq!(split_rings(&v, 2), [span(0, 5), span(7, 5), span(15, 5)]);
        // GeoJSON gets the outer ring and each hole as a closed ring of its
        // own, without the connector edges.
        let lnglat = |ring: &[NodeCoord]| ring.iter().map(|p| [p.lng as f64, p.lat as f64]).collect::<Vec<_>>();
        let want: Vec<Vec<[f64; 2]>> = std::iter::once(&outer).chain(&holes).map(|r| lnglat(r)).collect();
        assert_eq!(geojson_rings(&v, 2), want);
        // Without holes, or a range that doesn't parse: the whole range.
        assert_eq!(split_rings(&outer, 0), [span(0, 5)]);
        assert_eq!(split_rings(&outer, 1), [span(0, 5)]);
        assert_eq!(split_rings(&v[..v.len() - 1], 2), [span(0, v.len() - 1)]);
        assert_eq!(split_rings(&v, 3), [span(0, v.len())]);
        assert_eq!(geojson_rings(&outer, 0), [lnglat(&outer)]);
    }

    #[test]
    fn point_in_polygon_leaves_out_keyhole_holes() {
        let (outer, holes) = square_with_two_holes();
        let v = keyhole(&outer, &holes);
        assert!(point_in_polygon(1.0, 1.0, &v));
        assert!(point_in_polygon(5.0, 9.0, &v));
        assert!(!point_in_polygon(3.0, 3.0, &v));
        assert!(!point_in_polygon(6.5, 6.0, &v));
        assert!(!point_in_polygon(11.0, 5.0, &v));
        // On the connector lines (v0's latitude, the holes' first longitudes).
        assert!(point_in_polygon(0.5, 2.0, &v));
        assert!(point_in_polygon(5.0, 2.0, &v));
        assert!(point_in_polygon(1.0, 5.0, &v));
    }

    fn boundary(name_id: u32, admin_level: u8) -> AdminPolygon {
        AdminPolygon {
            vertex_offset: 0, vertex_count: 0, name_id, admin_level, place_type_override: 0,
            _pad2: 0, _pad3: 0, area: 1.0, country_code: 0, hole_count: 0,
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
        promote_nested_ranks(&mut polys, [nairobi].iter());
        assert_eq!(levels_and_ranks(&polys), [(2, 4), (6, 18), (7, 20)]);
    }

    #[test]
    fn nested_ranks_keep_a_linked_place_rank_inside_a_place_area() {
        // Paris (admin_level 6, linked place=city) takes rank 16 and keeps it
        // inside a larger place=city area; its arrondissement (9) goes below.
        let area = AdminPolygon { area: 100.0, place_type_override: 1, ..boundary(0, 15) };
        let mut polys = [ranked(9, 18, 1.0, 0), ranked(6, 12, 10.0, 1)];
        promote_nested_ranks(&mut polys, [area].iter());
        assert_eq!(levels_and_ranks(&polys), [(6, 16), (9, 18)]);
    }

    #[test]
    fn nested_ranks_keep_boundaries_below_rank_10_out_of_place_areas() {
        // A state (level 4, rank 8) inside a larger suburb area keeps its
        // rank; a county (level 6, rank 12) in it ranks 22.
        let area = AdminPolygon { area: 100.0, place_type_override: 4, ..boundary(0, 15) };
        let mut polys = [ranked(6, 12, 1.0, 0), ranked(4, 8, 10.0, 0)];
        promote_nested_ranks(&mut polys, [area].iter());
        assert_eq!(levels_and_ranks(&polys), [(4, 8), (6, 22)]);
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
        let found = CurrentBoundaries::from_ranked(ranked, [].iter(), |_, _| true);
        assert_eq!(names(&found), [Some(3), Some(3), Some(1), Some(2), Some(2)]);
    }

    #[test]
    fn current_boundaries_rank_boundaries_inside_a_place_area() {
        // Starehe (admin_level 6, rank 12) inside Nairobi's place=city area
        // ranks 18, after the city's 16: it gates suburbs, quarters and
        // neighbourhoods, no longer city, town and village nodes.
        let starehe = AdminPolygon { area: 10.0, ..boundary(1, 6) };
        let country = AdminPolygon { area: 1000.0, ..boundary(2, 2) };
        let nairobi = AdminPolygon { area: 100.0, place_type_override: 1, ..boundary(3, 15) };
        let ranked = vec![RankedPoly { poly: starehe, rank_address: 12 }, RankedPoly { poly: country, rank_address: 4 }];
        let found = CurrentBoundaries::from_ranked(ranked, [nairobi].iter(), |_, _| true);
        assert_eq!(names(&found), [Some(2), Some(1), Some(1), Some(1), Some(1)]);
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
        let found = CurrentBoundaries::from_ranked(ranked, [].iter(), |_, _| true);
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
        let concentric = CurrentBoundaries::from_ranked(ranked(), [].iter(), |_, _| true);
        assert_eq!(names(&concentric), [Some(2), Some(2), Some(1), Some(1), Some(1)]);
        let overlapping = CurrentBoundaries::from_ranked(ranked(), [].iter(), |poly, _| poly.name_id != 1);
        assert_eq!(names(&overlapping), [Some(2); 5]);
    }

    #[test]
    fn current_boundaries_test_the_point_on_surface_of_larger_and_equal_boundaries() {
        // A city square (rank 16) and a rank 20 boundary holding a point in
        // the city: round the city, beside it, beside it with the same area,
        // and with its point on surface on the city's edge. Only the one round
        // it has its point on surface inside the city.
        let square = |lat0: f32, lng0: f32, lat1: f32, lng1: f32| {
            vec![nc(lat0, lng0), nc(lat0, lng1), nc(lat1, lng1), nc(lat1, lng0), nc(lat0, lng0)]
        };
        let verts = [
            (2, square(0.0, 0.0, 2.0, 2.0)),
            (11, square(-1.0, -1.0, 3.0, 3.0)),
            (12, square(1.5, 1.5, 3.5, 9.5)),
            (13, square(1.5, 1.5, 3.5, 3.5)),
            // Its point on surface (0, 1) is on the city's edge.
            (14, square(-1.0, -1.0, 1.0, 3.0)),
        ];
        let of = |poly: &AdminPolygon| &verts.iter().find(|(id, _)| *id == poly.name_id).unwrap().1;
        let inside = |poly: &AdminPolygon, current: &AdminPolygon| point_on_surface_in(of(poly), 0, of(current), 0);
        let city = AdminPolygon { area: 4.0, ..boundary(2, 8) };
        for (id, area, gate) in [(11, 16.0, 11), (12, 16.0, 2), (13, 4.0, 2), (14, 8.0, 2)] {
            let other = AdminPolygon { area, ..boundary(id, 9) };
            let found = CurrentBoundaries::walk([(16, city), (20, other)], inside);
            assert_eq!(names(&found), [Some(2), Some(2), Some(gate), Some(gate), Some(gate)], "rank 20 boundary {id}");
        }
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
    fn point_on_surface_is_in_the_widest_run_inside() {
        // A square: its middle. An L whose bounding box's middle is outside
        // it: on the line between the latitudes round the middle (1 and 4,
        // so 2.5), the middle of the run inside (lng 0 to 2).
        let square = [nc(0.0, 0.0), nc(0.0, 2.0), nc(2.0, 2.0), nc(2.0, 0.0), nc(0.0, 0.0)];
        assert_eq!(point_on_surface(&square, 0), Some((1.0, 1.0)));
        let l = [nc(0.0, 0.0), nc(0.0, 8.0), nc(1.0, 8.0), nc(1.0, 2.0), nc(4.0, 2.0), nc(4.0, 0.0), nc(0.0, 0.0)];
        assert_eq!(point_on_surface(&l, 0), Some((2.5, 1.0)));
        // Holes split the runs, keyhole connectors don't: the line at lat 5
        // runs inside from 0 to 10 past the second hole's connector, and from
        // 6 to 10 right of a hole from lng 1 to 6.
        let (outer, holes) = square_with_two_holes();
        assert_eq!(point_on_surface(&keyhole(&outer, &holes), 2), Some((5.0, 5.0)));
        let wide_hole = vec![nc(2.0, 1.0), nc(8.0, 1.0), nc(8.0, 6.0), nc(2.0, 6.0), nc(2.0, 1.0)];
        let v = keyhole(&outer, &[wide_hole]);
        assert_eq!(point_on_surface(&v, 1), Some((5.0, 8.0)));
        assert!(point_in_polygon(5.0, 8.0, &v));
        // Hole latitudes choose the line too: a hole from lat 4 to 8 moves it
        // from 5 to half-way between 4 and 8.
        let high_hole = vec![nc(4.0, 1.0), nc(8.0, 1.0), nc(8.0, 6.0), nc(4.0, 6.0), nc(4.0, 1.0)];
        assert_eq!(point_on_surface(&keyhole(&outer, &[high_hole]), 1), Some((6.0, 8.0)));
        // The square's point on surface (1, 1) is inside the 0..10 square,
        // but not when it is on the top edge of a hole there (which ray
        // casting counts as inside).
        assert!(point_on_surface_in(&square, 0, &outer, 0));
        let low_hole = vec![nc(0.5, 0.5), nc(1.0, 0.5), nc(1.0, 4.0), nc(0.5, 4.0), nc(0.5, 0.5)];
        let holed = keyhole(&outer, &[low_hole]);
        assert!(point_in_polygon(1.0, 1.0, &holed));
        assert!(!point_on_surface_in(&square, 0, &holed, 1));
        // A ring with no width gives its first vertex; no vertices, none.
        assert_eq!(point_on_surface(&[nc(1.0, 0.0), nc(1.0, 2.0), nc(1.0, 1.0), nc(1.0, 0.0)], 0), Some((1.0, 0.0)));
        assert_eq!(point_on_surface(&[], 0), None);
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
        let found = CurrentBoundaries::from_ranked(ranked, [suburb, neighbourhood].iter(), |_, _| true);
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
        let at = |lat: f32, lng: f32, hn: u32, st: u32| AddrPoint {
            lat, lng, housenumber_id: hn, street_id: st, parent_way_id: 0,
            vertex_offset: NO_DATA, vertex_count: 0,
        };
        let text = ["2", "1/2", "Angell Street", "Bell Road"];
        let (lat, lng) = (52.0f64, 13.0f64);
        let cos_lat = lat.to_radians().cos();
        let key = |a: &AddrPoint| {
            let d = dist_sq((a.lat as f64 - lat).to_radians(), (a.lng as f64 - lng).to_radians(), cos_lat);
            (d, text[a.housenumber_id as usize], text[a.street_id as usize])
        };
        let wins = |dist: f64, p: &AddrPoint, best_dist: f64, best: Option<&AddrPoint>| {
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

    // -- vertex encoding round-trips -----------------------------------------

    #[test]
    fn vertex_stride_and_scale_snapshot() {
        assert_eq!(vertex_stride(0), 4);
        assert_eq!(vertex_stride(1), 4);
        assert_eq!(vertex_stride(2), 4);
        assert_eq!(vertex_stride(3), 8);
        assert_eq!(vertex_stride(4), 8);
        // NOTE: unknown encodings fall through to 8.
        assert_eq!(vertex_stride(99), 8);

        assert_eq!(vertex_scale(0), 1e-5);
        assert_eq!(vertex_scale(1), 1e-4);
        assert_eq!(vertex_scale(2), 1e-6);
        assert_eq!(vertex_scale(3), 1e-7);
        // F32 (and unknown) report scale 0.0 (unused for the f32 path).
        assert_eq!(vertex_scale(4), 0.0);
        assert_eq!(vertex_scale(99), 0.0);
    }

    // Build a polygon buffer: 10-byte header + packed vertices.
    fn build_poly_buf(enc: u8, bmin_lat: f32, bmin_lng: f32, verts: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(enc);
        buf.push(0); // padding
        buf.extend_from_slice(&bmin_lat.to_le_bytes());
        buf.extend_from_slice(&bmin_lng.to_le_bytes());
        buf.extend_from_slice(verts);
        buf
    }

    #[test]
    fn decode_polygon_verts_u16_encodings() {
        // Encodings 0,1,2 all use u16 deltas (4 bytes/vertex) with differing
        // scales.  One vertex with dlat=10, dlng=20.
        let mut vbytes = Vec::new();
        vbytes.extend_from_slice(&10u16.to_le_bytes());
        vbytes.extend_from_slice(&20u16.to_le_bytes());

        for enc in [0u8, 1, 2] {
            let buf = build_poly_buf(enc, 50.0, 5.0, &vbytes);
            let src = FileBytes::Owned(buf);
            let got = decode_polygon_verts(&src, 0, 1).expect("decode");
            assert_eq!(got.len(), 1);
            let scale = vertex_scale(enc);
            let expect_lat = (50.0_f64 + 10.0 * scale) as f32;
            let expect_lng = (5.0_f64 + 20.0 * scale) as f32;
            assert_eq!(got[0].lat, expect_lat, "enc {enc} lat");
            assert_eq!(got[0].lng, expect_lng, "enc {enc} lng");
        }
    }

    #[test]
    fn decode_polygon_verts_u32_encoding3() {
        // Encoding 3: u32 deltas (8 bytes/vertex), scale 1e-7.
        let mut vbytes = Vec::new();
        vbytes.extend_from_slice(&1_000_000u32.to_le_bytes()); // dlat
        vbytes.extend_from_slice(&2_000_000u32.to_le_bytes()); // dlng
        let buf = build_poly_buf(3, -10.0, 100.0, &vbytes);
        let src = FileBytes::Owned(buf);
        let got = decode_polygon_verts(&src, 0, 1).expect("decode");
        assert_eq!(got.len(), 1);
        let expect_lat = (-10.0_f64 + 1_000_000.0 * 1e-7) as f32;
        let expect_lng = (100.0_f64 + 2_000_000.0 * 1e-7) as f32;
        assert_eq!(got[0].lat, expect_lat);
        assert_eq!(got[0].lng, expect_lng);
    }

    #[test]
    fn decode_polygon_verts_f32_encoding4() {
        // Encoding 4: raw f32 lat/lng (8 bytes/vertex), no scale/bbox.
        // Vertices begin at POLY_HEADER_BYTES (byte 10), which is only
        // 2-aligned, so this exercises the unaligned-read decode path
        // (a `*const NodeCoord` cast here would be UB). Two vertices.
        let mut vbytes = Vec::new();
        vbytes.extend_from_slice(&51.5074_f32.to_le_bytes());
        vbytes.extend_from_slice(&(-0.1278_f32).to_le_bytes());
        vbytes.extend_from_slice(&48.8566_f32.to_le_bytes());
        vbytes.extend_from_slice(&2.3522_f32.to_le_bytes());
        let buf = build_poly_buf(4, 0.0, 0.0, &vbytes);
        let src = FileBytes::Owned(buf);
        let got = decode_polygon_verts(&src, 0, 2).expect("decode");
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].lat, 51.5074_f32);
        assert_eq!(got[0].lng, -0.1278_f32);
        assert_eq!(got[1].lat, 48.8566_f32);
        assert_eq!(got[1].lng, 2.3522_f32);
    }

    #[test]
    fn decode_polygon_verts_guards() {
        let buf = build_poly_buf(0, 0.0, 0.0, &[0, 0, 0, 0]);
        let src = FileBytes::Owned(buf);
        // vertex_count == 0 -> None.
        assert!(decode_polygon_verts(&src, 0, 0).is_none());
        // NO_DATA offset -> None.
        assert!(decode_polygon_verts(&src, NO_DATA, 1).is_none());
        // Offset+header past EOF -> None (offset way beyond buffer).
        assert!(decode_polygon_verts(&src, 1000, 1).is_none());
        // Declared more vertices than the buffer holds -> None.
        assert!(decode_polygon_verts(&src, 0, 5).is_none());
    }

    // A polygon buffer of `verts` in encoding `enc`, the bbox minimum as the delta base.
    fn encode_poly(enc: u8, verts: &[NodeCoord]) -> Vec<u8> {
        let bmin_lat = verts.iter().map(|v| v.lat).fold(f32::MAX, f32::min);
        let bmin_lng = verts.iter().map(|v| v.lng).fold(f32::MAX, f32::min);
        let delta = |x: f32, min: f32| ((x - min) as f64 / vertex_scale(enc)).round() as u64;
        let mut packed = Vec::new();
        for v in verts {
            match enc {
                4 => [v.lat.to_le_bytes(), v.lng.to_le_bytes()].iter().for_each(|b| packed.extend_from_slice(b)),
                3 => [delta(v.lat, bmin_lat), delta(v.lng, bmin_lng)]
                    .iter()
                    .for_each(|&d| packed.extend_from_slice(&u32::try_from(d).unwrap().to_le_bytes())),
                _ => [delta(v.lat, bmin_lat), delta(v.lng, bmin_lng)]
                    .iter()
                    .for_each(|&d| packed.extend_from_slice(&u16::try_from(d).unwrap().to_le_bytes())),
            }
        }
        build_poly_buf(enc, bmin_lat, bmin_lng, &packed)
    }

    #[test]
    fn point_in_encoded_polygon_matches_the_decoded_ray_cast() {
        // The keyhole square with two holes, scaled into each encoding's delta range.
        let (outer, holes) = square_with_two_holes();
        let unit = keyhole(&outer, &holes);
        // Inside, in each hole, outside on three sides, on the keyhole connectors.
        let probes = [
            ((1.0, 1.0), true), ((5.0, 9.0), true), ((3.0, 3.0), false), ((6.5, 6.0), false), ((11.0, 5.0), false),
            ((5.0, -1.0), false), ((-1.0, 5.0), false), ((0.5, 2.0), true), ((5.0, 2.0), true), ((1.0, 5.0), true),
        ];
        for (enc, extent) in [(0u8, 0.5f32), (1, 5.0), (2, 0.05), (3, 10.0), (4, 10.0)] {
            let k = extent / 10.0;
            let at = |lat: f32, lng: f32| nc(40.0 + lat * k, -3.0 + lng * k);
            let verts: Vec<NodeCoord> = unit.iter().map(|p| at(p.lat, p.lng)).collect();
            // At an odd byte offset, behind another record's bytes.
            let mut buf = vec![0xAB; 7];
            let off = buf.len() as u32;
            buf.extend(encode_poly(enc, &verts));
            let src = FileBytes::Owned(buf);
            let n = verts.len() as u32;
            let decoded = decode_polygon_verts(&src, off, n).unwrap();
            let mut points: Vec<(NodeCoord, Option<bool>)> =
                probes.iter().map(|&((lat, lng), inside)| (at(lat, lng), Some(inside))).collect();
            // On the boundary: every decoded vertex and edge midpoint.
            for (i, v) in decoded.iter().enumerate() {
                let w = decoded[(i + 1) % decoded.len()];
                points.extend([(*v, None), (nc((v.lat + w.lat) / 2.0, (v.lng + w.lng) / 2.0), None)]);
            }

            for (p, inside) in points {
                let got = point_in_encoded_polygon(&src, off, n, p.lat, p.lng);

                assert_eq!(got, Some(point_in_polygon(p.lat, p.lng, &decoded)), "enc {enc} at ({}, {})", p.lat, p.lng);
                if let Some(inside) = inside {
                    assert_eq!(got, Some(inside), "enc {enc} at ({}, {})", p.lat, p.lng);
                }
            }
        }
    }

    #[test]
    fn point_in_encoded_polygon_is_none_where_the_decoder_is() {
        let square = unit_square();
        let src = FileBytes::Owned(encode_poly(1, &square));
        let len = src.len() as u32;
        assert_eq!(point_in_encoded_polygon(&src, 0, 4, 0.5, 0.5), Some(true));
        // No vertices, no record, a header past the end, more vertices than the buffer holds.
        for (off, n) in [(0, 0), (NO_DATA, 1), (len - 5, 1), (0, 5)] {
            assert_eq!(point_in_encoded_polygon(&src, off, n, 0.5, 0.5), None, "offset {off} count {n}");
            assert!(decode_polygon_verts(&src, off, n).is_none(), "offset {off} count {n}");
        }
    }
}

/// Query-path tests: a small index in the builder's file layout, written by
/// TestIndex, loaded by Index::load and asked through Index::query. They pin
/// how find_admin, find_current_boundaries and find_places are wired together,
/// which the helper tests cannot see.
#[cfg(test)]
mod query_path_tests {
    use super::*;
    use s2::cell::Cell;
    use s2::rect::Rect;
    use s2::region::RegionCoverer;
    use crate::scratch_dir::ScratchDir;
    use std::collections::BTreeMap;

    /// The fixtures' origin unless one is given: everything sits metres
    /// north and east of it.
    const ORIGIN: (f64, f64) = (10.0, 20.0);
    const COUNTRY_CODE: u16 = (b'x' as u16) << 8 | b'x' as u16;
    /// Raw f32 vertices (VertexEncoding in builder/src/types.h).
    const RAW_F32_VERTICES: u8 = 4;

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

    /// A cell file (u64 cell id, u32 entry offset; sorted) and its entry
    /// file (u16 count, then the u32 ids) for `cells`.
    fn cell_files(cells: &BTreeMap<u64, Vec<u32>>) -> (Vec<u8>, Vec<u8>) {
        let (mut index, mut entries) = (Vec::new(), Vec::new());
        for (cell, ids) in cells {
            index.extend_from_slice(&cell.to_le_bytes());
            index.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            entries.extend_from_slice(&(ids.len() as u16).to_le_bytes());
            ids.iter().for_each(|id| entries.extend_from_slice(&id.to_le_bytes()));
        }
        (index, entries)
    }

    #[derive(Default)]
    struct TestIndex {
        origin: (f64, f64),
        strings: Vec<u8>,
        polygons: Vec<(AdminPolygon, Vec<NodeCoord>)>,
        place_nodes: Vec<PlaceNode>,
        streets: Vec<(u32, Vec<NodeCoord>)>,
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

        fn string(&mut self, s: &str) -> u32 {
            let offset = self.strings.len() as u32;
            self.strings.extend_from_slice(s.as_bytes());
            self.strings.push(0);
            offset
        }

        /// A rectangular boundary; admin_level 15 makes it a place area.
        fn boundary(&mut self, name: &str, admin_level: u8, place_type_override: u8, (s, w, n, e): Span) -> &mut Self {
            let at = |north_m, east_m| at_from(self.origin, north_m, east_m);
            let ring = vec![at(s, w), at(s, e), at(n, e), at(n, w), at(s, w)];
            let (lat_lo, lng_lo, lat_hi, lng_hi) = bounds(&ring);
            let poly = AdminPolygon {
                vertex_offset: 0, vertex_count: ring.len() as u32, name_id: self.string(name), admin_level,
                place_type_override, _pad2: 0, _pad3: 0, area: ((lat_hi - lat_lo) * (lng_hi - lng_lo)) as f32,
                country_code: COUNTRY_CODE, hole_count: 0,
            };
            self.polygons.push((poly, ring));
            self
        }

        /// A place node of PlaceNode type `place_type`.
        fn place(&mut self, name: &str, place_type: u8, north_m: f64, east_m: f64) -> &mut Self {
            let NodeCoord { lat, lng } = at_from(self.origin, north_m, east_m);
            let name_id = self.string(name);
            self.place_nodes.push(PlaceNode { lat, lng, name_id, place_type, _pad1: 0, _pad2: 0, parent_poly_id: NO_DATA });
            self
        }

        /// A named street through points given as (north, east) metres.
        fn street(&mut self, name: &str, points_m: &[(f64, f64)]) -> &mut Self {
            let name_id = self.string(name);
            let line = points_m.iter().map(|&(n, e)| at_from(self.origin, n, e)).collect();
            self.streets.push((name_id, line));
            self
        }

        /// The files as the builder writes them, by name. A boundary is
        /// listed in every admin cell meeting its box, flagged interior where
        /// the cell lies inside it; a place node in its own cell; a street in
        /// the street cells along it.
        fn files(&self) -> Vec<(&'static str, Vec<u8>)> {
            let (mut polygons, mut vertices) = (Vec::new(), Vec::new());
            let mut admin_cells: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
            for (id, (poly, ring)) in self.polygons.iter().enumerate() {
                let (lat_lo, lng_lo, lat_hi, lng_hi) = bounds(ring);
                polygons.extend_from_slice(&(vertices.len() as u32).to_le_bytes());
                polygons.extend_from_slice(&poly.vertex_count.to_le_bytes());
                polygons.extend_from_slice(&poly.name_id.to_le_bytes());
                polygons.extend_from_slice(&[poly.admin_level, poly.place_type_override, 0, 0]);
                polygons.extend_from_slice(&poly.area.to_le_bytes());
                polygons.extend_from_slice(&poly.country_code.to_le_bytes());
                polygons.extend_from_slice(&poly.hole_count.to_le_bytes());
                vertices.extend_from_slice(&[RAW_F32_VERTICES, 0]);
                vertices.extend_from_slice(&(lat_lo as f32).to_le_bytes());
                vertices.extend_from_slice(&(lng_lo as f32).to_le_bytes());
                for v in ring {
                    vertices.extend_from_slice(&v.lat.to_le_bytes());
                    vertices.extend_from_slice(&v.lng.to_le_bytes());
                }
                for cell in cells_meeting(ring, DEFAULT_ADMIN_CELL_LEVEL) {
                    let b = Cell::from(CellID(cell)).rect_bound();
                    let interior = b.lat_lo().deg() > lat_lo && b.lat_hi().deg() < lat_hi
                        && b.lng_lo().deg() > lng_lo && b.lng_hi().deg() < lng_hi;
                    admin_cells.entry(cell).or_default().push(id as u32 | if interior { INTERIOR_FLAG } else { 0 });
                }
            }
            assert_eq!(polygons.len(), self.polygons.len() * std::mem::size_of::<AdminPolygon>());
            let (cells, entries) = cell_files(&admin_cells);
            let mut files = vec![
                ("admin_polygons.bin", polygons), ("admin_vertices.bin", vertices),
                ("admin_cells.bin", cells), ("admin_entries.bin", entries),
            ];

            if !self.place_nodes.is_empty() {
                let mut nodes = Vec::new();
                let mut place_cells: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
                for (id, pn) in self.place_nodes.iter().enumerate() {
                    nodes.extend_from_slice(&pn.lat.to_le_bytes());
                    nodes.extend_from_slice(&pn.lng.to_le_bytes());
                    nodes.extend_from_slice(&pn.name_id.to_le_bytes());
                    nodes.extend_from_slice(&[pn.place_type, 0, 0, 0]);
                    nodes.extend_from_slice(&pn.parent_poly_id.to_le_bytes());
                    let cell = cell_id_at_level(pn.lat as f64, pn.lng as f64, DEFAULT_ADMIN_CELL_LEVEL);
                    place_cells.entry(cell).or_default().push(id as u32);
                }
                assert_eq!(nodes.len(), self.place_nodes.len() * std::mem::size_of::<PlaceNode>());
                let (cells, entries) = cell_files(&place_cells);
                files.extend([("place_nodes.bin", nodes), ("place_cells.bin", cells), ("place_entries.bin", entries)]);
            }

            if !self.streets.is_empty() {
                let (mut ways, mut nodes) = (Vec::new(), Vec::new());
                let mut street_cells: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
                for (id, (name_id, line)) in self.streets.iter().enumerate() {
                    ways.extend_from_slice(&((nodes.len() / std::mem::size_of::<NodeCoord>()) as u32).to_le_bytes());
                    ways.extend_from_slice(&(line.len() as u16).to_le_bytes());
                    ways.extend_from_slice(&[0, 0]);
                    ways.extend_from_slice(&name_id.to_le_bytes());
                    for v in line {
                        nodes.extend_from_slice(&v.lat.to_le_bytes());
                        nodes.extend_from_slice(&v.lng.to_le_bytes());
                    }
                    for segment in line.windows(2) {
                        for cell in cells_meeting(segment, DEFAULT_STREET_CELL_LEVEL) {
                            let ids = street_cells.entry(cell).or_default();
                            if !ids.contains(&(id as u32)) { ids.push(id as u32); }
                        }
                    }
                }
                assert_eq!(ways.len(), self.streets.len() * std::mem::size_of::<WayHeader>());
                let (mut geo_cells, mut entries) = (Vec::new(), Vec::new());
                for (cell, ids) in &street_cells {
                    geo_cells.extend_from_slice(&cell.to_le_bytes());
                    geo_cells.extend_from_slice(&(entries.len() as u32).to_le_bytes());
                    geo_cells.extend_from_slice(&NO_DATA.to_le_bytes());
                    geo_cells.extend_from_slice(&NO_DATA.to_le_bytes());
                    entries.extend_from_slice(&(ids.len() as u16).to_le_bytes());
                    ids.iter().for_each(|id| entries.extend_from_slice(&id.to_le_bytes()));
                }
                files.extend([("geo_cells.bin", geo_cells), ("street_entries.bin", entries),
                              ("street_ways.bin", ways), ("street_nodes.bin", nodes)]);
            }

            let end = self.strings.len();
            let tiers: Vec<String> = (0..STR_TIER_COUNT)
                .map(|t| format!("{{\"start\": {}, \"end\": {}}}", if t == 0 { 0 } else { end }, end))
                .collect();
            files.push(("strings_layout.json", format!("{{\"tiers\": [{}]}}", tiers.join(", ")).into_bytes()));
            files.push(("strings_core.bin", self.strings.clone()));
            files
        }

        /// Writes the files into a fresh directory and loads them. The
        /// directory is removed once loaded; the maps keep the bytes.
        fn load(&self) -> Index {
            let dir = ScratchDir::new();
            for (name, bytes) in self.files() {
                std::fs::write(dir.path().join(name), bytes).unwrap();
            }
            Index::load(dir.path().to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL,
                        DEFAULT_SEARCH_DISTANCE).unwrap()
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
}

#[cfg(test)]
mod encoded_pip_tests {
    use super::*;

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

    /// One ring packed as the builder lays it out in admin_vertices.bin: the 10-byte header
    /// (encoding, pad, bbox minimum as f32) and the vertices at the encoding's stride.
    pub(crate) fn pack_ring(enc: u8, ring: &[(f64, f64)]) -> Vec<u8> {
        let scale = vertex_scale(enc);
        let min = |f: fn(&(f64, f64)) -> f64| ring.iter().map(f).fold(f64::MAX, f64::min);
        // One step below the minimum keeps every delta non-negative after f32 rounding.
        let (bmin_lat, bmin_lng) = if enc == 4 { (0.0, 0.0) } else { ((min(|v| v.0) - scale) as f32, (min(|v| v.1) - scale) as f32) };
        let mut buf = vec![enc, 0];
        buf.extend_from_slice(&bmin_lat.to_le_bytes());
        buf.extend_from_slice(&bmin_lng.to_le_bytes());
        for &(lat, lng) in ring {
            let step = |v: f64, b: f32| ((v - b as f64) / scale).round();
            match enc {
                4 => {
                    buf.extend_from_slice(&(lat as f32).to_le_bytes());
                    buf.extend_from_slice(&(lng as f32).to_le_bytes());
                }
                3 => {
                    buf.extend_from_slice(&(step(lat, bmin_lat) as u32).to_le_bytes());
                    buf.extend_from_slice(&(step(lng, bmin_lng) as u32).to_le_bytes());
                }
                _ => {
                    assert!(step(lat, bmin_lat).max(step(lng, bmin_lng)) <= u16::MAX as f64, "ring too wide for encoding {enc}");
                    buf.extend_from_slice(&(step(lat, bmin_lat) as u16).to_le_bytes());
                    buf.extend_from_slice(&(step(lng, bmin_lng) as u16).to_le_bytes());
                }
            }
        }
        buf
    }

    /// A random simple ring: vertices at increasing angles around a centre, random radii.
    fn star_ring(rng: &mut Rng, lat: f64, lng: f64, radius: f64) -> Vec<(f64, f64)> {
        let n = 3 + (rng.next() % 30) as usize;
        let mut angles: Vec<f64> = (0..n).map(|_| rng.range(0.0, std::f64::consts::TAU)).collect();
        angles.sort_by(f64::total_cmp);
        angles.iter().map(|a| (lat + radius * rng.range(0.2, 1.0) * a.sin(), lng + radius * rng.range(0.2, 1.0) * a.cos())).collect()
    }

    // Ring widths (degrees) each encoding can hold: u16 deltas cap the encodings 0-2.
    const WIDTH_BY_ENCODING: [f64; 5] = [0.3, 3.0, 0.03, 20.0, 20.0];

    #[test]
    fn point_in_encoded_polygon_matches_the_decoded_ray_cast() {
        let mut rng = Rng::new(20261019);
        for enc in 0u8..=4 {
            let radius = WIDTH_BY_ENCODING[enc as usize] / 2.0;
            let mut inside = 0;
            for _ in 0..300 {
                let (clat, clng) = (rng.range(-80.0, 80.0), rng.range(-170.0, 170.0));
                let ring = star_ring(&mut rng, clat, clng, radius);
                let src = FileBytes::Owned(pack_ring(enc, &ring));
                let n = ring.len() as u32;
                let verts = decode_polygon_verts(&src, 0, n).unwrap();
                for k in 0..40 {
                    // Mostly points around the ring; some on a vertex's latitude or longitude.
                    let mut lat = rng.range(clat - 1.5 * radius, clat + 1.5 * radius);
                    let mut lng = rng.range(clng - 1.5 * radius, clng + 1.5 * radius);
                    let v = verts[k % verts.len()];
                    if k % 5 == 1 { lat = v.lat as f64; }
                    if k % 5 == 2 { lng = v.lng as f64; }
                    let want = point_in_polygon(lat as f32, lng as f32, &verts);
                    assert_eq!(point_in_encoded_polygon(&src, 0, n, lat as f32, lng as f32), Some(want), "enc {enc} ring {ring:?} point {lat} {lng}");
                    inside += want as usize;
                }
            }
            // Both answers are exercised.
            assert!(inside > 1000 && inside < 11000, "enc {enc}: {inside} of 12000 inside");
        }
    }

    #[test]
    fn ring_bounds_exclude_only_points_the_ray_cast_puts_outside() {
        let mut rng = Rng::new(7);
        let mut excluded = [0usize; 4];
        for enc in 0u8..=3 {
            let scale = vertex_scale(enc);
            // Nearly every u16 step (the f32 bbox minimum sits up to a few steps below the ring),
            // so the extent bound is exercised at its limit.
            let width = if enc == 3 { 5.0 } else { (u16::MAX - 20) as f64 * scale };
            for _ in 0..200 {
                let (lat0, lng0) = (rng.range(-80.0, 75.0), rng.range(-175.0, 170.0));
                // A square spanning the full width, each side's midpoint pushed in a little.
                let (lat1, lng1) = (lat0 + width, lng0 + width);
                let (mid_lat, mid_lng) = (lat0 + width / 2.0, lng0 + width / 2.0);
                let mut dent = || rng.range(0.0, 0.1) * width;
                let ring = vec![
                    (lat0, lng0), (lat0 + dent(), mid_lng), (lat0, lng1), (mid_lat, lng1 - dent()),
                    (lat1, lng1), (lat1 - dent(), mid_lng), (lat1, lng0), (mid_lat, lng0 + dent()),
                ];
                let src = FileBytes::Owned(pack_ring(enc, &ring));
                let verts = decode_polygon_verts(&src, 0, ring.len() as u32).unwrap();
                let h = src.read_chunk(0, POLY_HEADER_BYTES as usize);
                let (bmin_lat, bmin_lng) = (f32::from_le_bytes(h[2..6].try_into().unwrap()), f32::from_le_bytes(h[6..10].try_into().unwrap()));
                for _ in 0..50 {
                    // Points within 0.01 degrees of a bbox side, in both directions.
                    let near = |rng: &mut Rng, lo: f64, hi: f64| if rng.next() % 2 == 0 { lo + rng.range(-0.01, 0.01) } else { hi + rng.range(-0.01, 0.01) };
                    let (lat, lng) = match rng.next() % 3 {
                        0 => (near(&mut rng, lat0, lat0 + width), rng.range(lng0, lng0 + width)),
                        1 => (rng.range(lat0, lat0 + width), near(&mut rng, lng0, lng0 + width)),
                        _ => (near(&mut rng, lat0, lat0 + width), near(&mut rng, lng0, lng0 + width)),
                    };
                    let (lat, lng) = (lat as f32, lng as f32);
                    if ring_bounds_exclude(enc, bmin_lat, bmin_lng, lat, lng) {
                        excluded[enc as usize] += 1;
                        assert!(!point_in_polygon(lat, lng, &verts), "enc {enc} excluded a point inside: {lat} {lng}");
                        assert_eq!(point_in_encoded_polygon(&src, 0, ring.len() as u32, lat, lng), Some(false));
                    }
                }
            }
        }
        assert!(excluded.iter().all(|&n| n > 1000), "{excluded:?}");
        // The legacy f32 encoding carries no usable bounds.
        assert!(!ring_bounds_exclude(4, 10.0, 10.0, 0.0, 0.0));
    }

    #[test]
    fn ring_bounds_exclude_skips_the_latitude_test_at_the_zero_meridian() {
        assert!(ring_bounds_exclude(3, 10.0, -1.0, 9.0, 0.5));
        assert!(!ring_bounds_exclude(3, 10.0, -1.0, 9.0, 0.0));
        assert!(!ring_bounds_exclude(3, 10.0, -1.0, 9.0, 1e-35));
        // The longitude test holds everywhere.
        assert!(ring_bounds_exclude(3, 10.0, 1e-30, 9.0, 0.0));
    }

    #[test]
    fn grid_threshold_splits_the_decoded_values_at_the_point() {
        let mut rng = Rng::new(31);
        for enc in 0u8..=3 {
            let (scale, k_max) = (vertex_scale(enc), k_max(enc));
            for _ in 0..5000 {
                // Near 180 degrees an f32 step spans many grid steps (enc 3: about 150).
                let base = rng.range(-180.0, 179.9) as f32 as f64;
                let decode = |k: i64| (base + k as f64 * scale) as f32;
                let k = (rng.next() % 70_000) as i64 - 1_000;
                let x = match rng.next() % 4 {
                    0 => decode(k),
                    1 => (decode(k) as f64 - scale / 3.0) as f32,
                    2 => (decode(k) as f64 + scale / 3.0) as f32,
                    _ => (base + rng.range(-0.1, 70_000.0 * scale)) as f32,
                };
                let t = grid_threshold(x, base, scale, k_max);
                assert!((-1..=k_max).contains(&t), "enc {enc} base {base} x {x} t {t}");
                assert!((t < 0 || decode(t) <= x) && (t == k_max || decode(t + 1) > x), "enc {enc} base {base} x {x} t {t}");
            }
        }
    }

    /// The largest delta of an encoding: u32 for 3, u16 for 0-2 and the tags `pip_grid` reads as u16.
    fn k_max(enc: u8) -> i64 {
        if enc == 3 { u32::MAX as i64 } else { u16::MAX as i64 }
    }

    /// f32 coordinates a caller can hand `query_json` (it narrows f64 input with `as f32`): NaN,
    /// the infinities, the largest finite values, values past the enc 3 grid's i64 range (beyond
    /// 9.22e11 degrees) or on its widest f32 plateaus, the edges of the valid range, signed zeros
    /// and subnormals.
    const EXTREME: [f32; 26] = [
        f32::NAN, f32::INFINITY, f32::NEG_INFINITY, f32::MAX, -f32::MAX, 1_099_511_627_776.0, -1_099_511_627_776.0,
        1e12, -1e12, 9.3e11, 1e9, -1e9, 1e6, -1e6, 360.0, -360.0, 180.0, -180.0, 90.0, -90.0, 0.0, -0.0,
        f32::MIN_POSITIVE, -f32::MIN_POSITIVE, 1e-45, -1e-45,
    ];

    #[test]
    fn grid_threshold_splits_the_decoded_values_for_any_input() {
        let mut rng = Rng::new(5);
        // Scale 0.0 is what `vertex_scale` gives a tag other than 0-4; the bases include ones only
        // a damaged header holds.
        let scales = [vertex_scale(0), vertex_scale(1), vertex_scale(2), vertex_scale(3), 0.0];
        let bases = [-180.0, -0.5, 0.0, 25.0, 179.9, f32::NAN, f32::INFINITY, f32::NEG_INFINITY, f32::MAX, 1e30];
        for (s, &scale) in scales.iter().enumerate() {
            let k_max = k_max(s as u8);
            for &base in &bases {
                let base = base as f64;
                for &x in EXTREME.iter().chain(&[base as f32, (base + 1e-3) as f32, (base + 0.25) as f32]) {
                    let t = grid_threshold(x, base, scale, k_max);
                    assert!((-1..=k_max).contains(&t), "scale {scale} base {base} x {x} t {t}");
                    let probes = [0, 1, k_max - 1, k_max, t, t + 1, (rng.next() % (k_max as u64 + 1)) as i64];
                    for k in probes.into_iter().filter(|k| (0..=k_max).contains(k)) {
                        let decoded = (base + k as f64 * scale) as f32;
                        assert_eq!(decoded > x, k > t, "scale {scale} base {base} x {x} t {t} k {k}");
                    }
                }
            }
        }
    }

    #[test]
    fn point_in_encoded_polygon_matches_the_decoded_ray_cast_at_extreme_coordinates() {
        let mut rng = Rng::new(20261020);
        let mut rings: Vec<(u8, Vec<u8>, u32)> = Vec::new();
        for enc in 0u8..=4 {
            let w = WIDTH_BY_ENCODING[enc as usize].min(1.0);
            let mut shapes = vec![
                // The unit square at the origin (as wide as the encoding holds).
                vec![(0.0, 0.0), (0.0, w), (w, w), (w, 0.0)],
                // At the antimeridian, from either side, and at the poles.
                vec![(-w / 2.0, 180.0 - w), (w / 2.0, 180.0 - w), (0.0, 180.0)],
                vec![(-w / 2.0, -180.0), (0.0, -180.0 + w), (w / 2.0, -180.0)],
                vec![(90.0 - w, 180.0 - w), (90.0, 180.0 - w), (90.0, 180.0)],
                vec![(-90.0, -180.0), (-90.0, -180.0 + w), (-90.0 + w, -180.0 + w)],
            ];
            for _ in 0..20 {
                let (lat, lng) = (rng.range(-80.0, 80.0), rng.range(-170.0, 170.0));
                shapes.push(star_ring(&mut rng, lat, lng, w / 2.0));
            }
            rings.extend(shapes.iter().map(|s| (enc, pack_ring(enc, s), s.len() as u32)));
        }
        // A tag the reader does not know (vertex_scale 0.0): every vertex decodes to the minimum.
        let mut unknown = vec![7, 0];
        unknown.extend_from_slice(&0.5f32.to_le_bytes());
        unknown.extend_from_slice(&0.5f32.to_le_bytes());
        unknown.extend((0..4 * 8).map(|_| rng.next() as u8));
        rings.push((7, unknown, 4));

        let mut inside = [0usize; 8];
        for (enc, ring, n) in &rings {
            let src = FileBytes::Owned(ring.clone());
            let verts = decode_polygon_verts(&src, 0, *n).unwrap();
            let shape: Vec<(f32, f32)> = verts.iter().map(|v| (v.lat, v.lng)).collect();
            // A vertex and the vertex centroid of the ring, next to every extreme value.
            let centre = |f: fn(&NodeCoord) -> f32| verts.iter().map(f).sum::<f32>() / verts.len() as f32;
            let lats: Vec<f32> = EXTREME.iter().copied().chain([verts[0].lat, centre(|v| v.lat)]).collect();
            let lngs: Vec<f32> = EXTREME.iter().copied().chain([verts[0].lng, centre(|v| v.lng)]).collect();
            for &lat in &lats {
                for &lng in &lngs {
                    let want = point_in_polygon(lat, lng, &verts);
                    assert_eq!(point_in_encoded_polygon(&src, 0, *n, lat, lng), Some(want), "enc {enc} ring {shape:?} point {lat} {lng}");
                    inside[*enc as usize] += want as usize;
                }
            }
        }
        // Each known encoding has points inside some ring (the centroids of the convex ones).
        assert!(inside[..5].iter().all(|&n| n > 0), "{inside:?}");
    }

    #[test]
    fn point_in_encoded_polygon_is_none_where_decoding_is() {
        let ring = [(10.0, 10.0), (10.0, 10.1), (10.1, 10.05)];
        let src = FileBytes::Owned(pack_ring(0, &ring));
        assert_eq!(point_in_encoded_polygon(&src, 0, 0, 10.05, 10.05), None);
        assert_eq!(point_in_encoded_polygon(&src, NO_DATA, 3, 10.05, 10.05), None);
        assert_eq!(point_in_encoded_polygon(&src, 1000, 3, 10.05, 10.05), None);
        assert_eq!(point_in_encoded_polygon(&src, 0, 4, 10.05, 10.05), None);
        assert_eq!(point_in_encoded_polygon(&src, 0, 3, 10.03, 10.05), Some(true));
    }

    #[test]
    fn point_in_encoded_polygon_reads_the_ring_at_its_offset() {
        let a = pack_ring(3, &[(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)]);
        let b = pack_ring(2, &[(5.0, 5.0), (5.0, 5.01), (5.01, 5.01), (5.01, 5.0)]);
        let src = FileBytes::Owned([a.clone(), b].concat());
        assert_eq!(point_in_encoded_polygon(&src, 0, 4, 0.5, 0.5), Some(true));
        assert_eq!(point_in_encoded_polygon(&src, a.len() as u32, 4, 0.5, 0.5), Some(false));
        assert_eq!(point_in_encoded_polygon(&src, a.len() as u32, 4, 5.005, 5.005), Some(true));
    }
}

#[cfg(test)]
mod admin_scan_tests {
    use super::encoded_pip_tests::pack_ring;
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
        let cell_ids = cells();
        let mut strings = vec![0u8];
        let mut vertices = Vec::new();
        let mut polygons = Vec::new();
        let mut by_cell: std::collections::BTreeMap<u64, Vec<u32>> = Default::default();
        for (id, r) in rings.iter().enumerate() {
            let name_id = strings.len() as u32;
            strings.extend_from_slice(r.name.as_bytes());
            strings.push(0);
            let vertex_offset = vertices.len() as u32;
            vertices.extend(pack_ring(3, &r.verts));
            polygons.extend_from_slice(&vertex_offset.to_le_bytes());
            polygons.extend_from_slice(&(r.verts.len() as u32).to_le_bytes());
            polygons.extend_from_slice(&name_id.to_le_bytes());
            polygons.extend_from_slice(&[r.level, r.place_type, 0, 0]);
            polygons.extend_from_slice(&r.area.to_le_bytes());
            polygons.extend_from_slice(&u16::from_be_bytes(*r.country).to_le_bytes());
            polygons.extend_from_slice(&[0, 0]);
            for &(k, interior) in &r.cells {
                by_cell.entry(cell_ids[k]).or_default().push(id as u32 | if interior { INTERIOR_FLAG } else { 0 });
            }
        }
        assert_eq!(polygons.len(), rings.len() * std::mem::size_of::<AdminPolygon>());
        let (mut cells, mut entries) = (Vec::new(), Vec::new());
        for (cell, mut ids) in by_cell {
            ids.sort_unstable(); // the builder sorts each cell's entries by value
            cells.extend_from_slice(&cell.to_le_bytes());
            cells.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            entries.extend_from_slice(&(ids.len() as u16).to_le_bytes());
            for id in ids { entries.extend_from_slice(&id.to_le_bytes()); }
        }
        let len = strings.len() as u32;
        Index {
            geo_cells: None, street_entries: None, street_ways: None, street_nodes: None,
            addr_entries: None, addr_points: None, addr_vertices: None,
            interp_entries: None, interp_ways: None, interp_nodes: None, interp_postcodes: None,
            admin_cells: FileBytes::Owned(cells),
            admin_entries: FileBytes::Owned(entries),
            admin_polygons: FileBytes::Owned(polygons),
            admin_geometry: AdminGeometry::new(FileBytes::Owned(vertices)),
            poi_cells: None, poi_entries: None, poi_records: None, poi_vertices: None,
            poi_meta: PoiMeta { categories: HashMap::new() },
            place_nodes: None, place_cells: None, place_entries: None,
            way_parents: None, admin_parents: None, addr_postcodes: None, way_postcodes: None,
            postal_polygons: None, postal_geometry: None,
            postcode_centroids: None, postcode_centroid_cells: None, postcode_centroid_entries: None,
            admin_config: AdminLevelConfig::load(),
            strings: StringPool::from_parts([Some(FileBytes::Owned(strings)), None, None, None, None], [0, len, len, len, len, len]),
            street_cell_level: DEFAULT_STREET_CELL_LEVEL,
            admin_cell_level: DEFAULT_ADMIN_CELL_LEVEL,
            max_distance_sq: 1.0,
        }
    }

    // Egypt north of 22°N; Sudan south of it.
    const EGYPT: (f64, f64, f64, f64) = (22.0, 25.0, 31.6, 36.9);
    const SUDAN: (f64, f64, f64, f64) = (8.7, 21.8, 22.0, 38.6);

    #[test]
    fn find_admin_reads_only_the_points_own_cell() {
        // Rings listed only in neighbour cells: the builder lists a ring in the cell of every point
        // it contains, so these contain no point of Q's cell.
        let idx = admin_index(&[
            Ring { country: b"sd", ..ring("Sudan", 2, 190.0, SUDAN, &[(0, true), (3, true)]) },
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
        assert_eq!((a.country, a.state), (Some("Egypt"), Some("New Valley")));
    }

    #[test]
    fn a_longitude_whole_turns_away_is_in_no_ring() {
        // S2 wraps the longitude back into Q's cell; point-in-polygon tests the f32 longitude
        // itself, 9.7e11 degrees east of every ring (past the i64 range of the enc 3 grid).
        let far = Q.1 + 360.0 * 2.7e9;
        assert_eq!(cell_id_at_level(Q.0, far, DEFAULT_ADMIN_CELL_LEVEL), cell_id_at_level(Q.0, Q.1, DEFAULT_ADMIN_CELL_LEVEL));
        let idx = admin_index(&[
            ring("Egypt", 2, 97.0, EGYPT, &[(0, false)]),
            ring("New Valley", 4, 38.8, (22.0, 25.0, 27.7, 32.7), &[(0, false)]),
        ]);
        assert_eq!(idx.find_admin(Q.0, Q.1).country, Some("Egypt"));
        let a = idx.find_admin(Q.0, far);
        assert_eq!((a.country, a.state), (None, None));
    }

    #[test]
    fn a_neighbour_cells_interior_flag_does_not_prove_containment() {
        let idx = admin_index(&[
            ring("Egypt", 2, 97.0, EGYPT, &[(0, false)]),
            Ring { country: b"sd", ..ring("Sudan", 2, 9.0, SUDAN, &[(1, true), (2, true)]) },
        ]);
        assert_eq!(idx.find_admin(Q.0, Q.1).country, Some("Egypt"));
    }

    #[test]
    fn a_place_area_fills_its_field() {
        let idx = admin_index(&[
            ring("Egypt", 2, 97.0, EGYPT, &[(0, true)]),
            Ring { place_type: 4, ..ring("Bir Tarfawi", 15, 0.01, (21.99, 27.5, 22.05, 27.6), &[(0, false), (7, false)]) },
        ]);
        assert_eq!(idx.find_admin(Q.0, Q.1).suburb, Some("Bir Tarfawi"));
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
        assert_eq!(idx.find_admin(Q.0, Q.1).state, Some("First"));
        // The interior flag is the entry's high bit.
        let idx = admin_index(&[
            ring("Egypt", 2, 97.0, EGYPT, &[(0, true)]),
            ring("First, interior", 4, 1.0, around_q, &[(0, true)]),
            ring("Second", 4, 1.0, around_q, &[(0, false)]),
        ]);
        assert_eq!(idx.find_admin(Q.0, Q.1).state, Some("Second"));
    }

    #[test]
    fn smaller_rings_that_miss_the_point_do_not_displace_the_containing_one() {
        let idx = admin_index(&[
            ring("Egypt", 2, 97.0, EGYPT, &[(0, true)]),
            ring("New Valley", 4, 38.8, (22.0, 25.0, 27.7, 32.7), &[(0, true), (1, true)]),
            ring("Aswan", 4, 6.0, (22.0, 31.0, 24.5, 33.6), &[(1, false), (0, false)]),
            ring("Red Sea", 4, 12.0, (22.0, 33.6, 28.0, 36.9), &[(2, false)]),
        ]);
        assert_eq!(idx.find_admin(Q.0, Q.1).state, Some("New Valley"));
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
        assert_eq!(name(CurrentBoundaries::CITY), Some("Paris"));
        assert_eq!(name(CurrentBoundaries::SUBURB), Some("Mut"));
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
        assert_eq!(name(CurrentBoundaries::CITY), Some("Paris"));
        assert_eq!(name(CurrentBoundaries::SUBURB), Some("Mut"));
        let a = idx.find_admin_in(&mut scan);
        assert_eq!((a.country, a.state, a.neighbourhood), (Some("Egypt"), Some("New Valley"), Some("Bir Tarfawi")));
        // No admin_parents: the chain lookup falls back to find_admin over the same scan.
        let c = idx.find_admin_from_chain_in(0, &mut scan);
        assert_eq!((c.country, c.state, c.neighbourhood), (Some("Egypt"), Some("New Valley"), Some("Bir Tarfawi")));
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
        let names: Vec<&str> = scan.rings.iter().map(|(_, p)| idx.get_string(p.name_id)).collect();
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

    /// A ring a tier simplified away: its record keeps its name, its vertex range is empty.
    fn ringless(name: &'static str, level: u8, cells: &[(usize, bool)]) -> Ring {
        Ring { verts: Vec::new(), ..ring(name, level, 1.0, EGYPT, cells) }
    }

    #[test]
    fn a_poi_whose_parent_has_no_rings_passes_the_chain_check() {
        let idx = admin_index(&[ring("Sudan", 2, 97.0, SUDAN, &[]), ringless("Ringless", 8, &[])]);
        let poi = |parent_poly_id: u32| PoiRecord {
            lat: 0.0, lng: 0.0, vertex_offset: NO_DATA, vertex_count: 0, name_id: 0, category: 0, tier: 0, flags: 0,
            importance: 0, parent_street_id: NO_DATA, parent_postcode_id: NO_DATA, parent_poly_id,
        };

        assert!(!idx.poi_chain_contains_query(&poi(0), Q.0, Q.1));
        assert!(idx.poi_chain_contains_query(&poi(1), Q.0, Q.1));
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
