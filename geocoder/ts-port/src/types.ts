// Binary struct sizes — must match the Rust server's #[repr(C)] layouts
// in geocoder/server/src/main.rs. Any change there requires a parallel
// change here. See the Rust source for the authoritative byte layouts.

export const NO_DATA = 0xFFFFFFFF;

export const ADMIN_POLYGON_SIZE = 24; // u32 vert_off + u32 vert_count + u32 name_id + u8 lvl + u8 override + 2 pad + f32 area + u16 cc + u16 pad
export const NODE_COORD_SIZE = 8;     // f32 lat + f32 lng
export const PLACE_NODE_SIZE = 20;    // f32 lat + f32 lng + u32 name + u8 type + 1 pad + u16 pad + u32 parent_poly
export const POI_RECORD_SIZE = 36;    // f32 lat + f32 lng + u32 vert_off + u32 vert_count + u32 name + 4 bytes (cat,tier,flags,imp) + u32 parent_street + u32 parent_postcode + u32 parent_poly
// AddrPoint: f32 lat + f32 lng + u32 housenumber + u32 street + u32 parent_way + u32 vert_off + u32 vert_count = 28 bytes
export const ADDR_POINT_SIZE = 28;
// WayHeader: u32 node_offset + u8 node_count + u32 name_id with #[repr(C)]
// alignment is 12 bytes (3 bytes padding after node_count). Detected
// from file size at load time (planet builds = 12, older = 9).
export const WAY_HEADER_SIZE_DEFAULT = 12;
// InterpWay: u32 node_offset + u8 node_count + u32 street_id + u32 start_number + u32 end_number + u8 interpolation
// = 24 bytes with #[repr(C)] padding. Older build_versions used 18 / 20.
export const INTERP_WAY_SIZE_DEFAULT = 24;
// PostcodeCentroid: f32 lat + f32 lng + u32 postcode_id + u16 cc + u16 pad
export const POSTCODE_CENTROID_SIZE = 16;

export const DEFAULT_STREET_CELL_LEVEL = 17;
export const DEFAULT_ADMIN_CELL_LEVEL = 10;

// Bit flags packed into admin_entries cell IDs
export const INTERIOR_FLAG = 0x80000000;
export const ID_MASK = 0x7FFFFFFF;

// String tier names + filenames (must match builder/src/parsed_data.h)
export const STR_TIER_FILENAMES = [
  "strings_core.bin",
  "strings_street.bin",
  "strings_addr.bin",
  "strings_postcode.bin",
  "strings_poi.bin",
] as const;

export interface AdminPolygon {
  index: number;          // index into the admin_polygons buffer (acts as polygon ID)
  vertex_offset: number;
  vertex_count: number;
  name_id: number;
  admin_level: number;
  place_type_override: number;
  area: number;
  country_code: number;
}

export interface NodeCoord {
  lat: number;
  lng: number;
}

export interface PlaceNode {
  index: number;
  lat: number;
  lng: number;
  name_id: number;
  place_type: number;
  parent_poly_id: number;
}

// Reverse-geocode response shape — mirrors the Rust server's Address.
// Kept loose with optional fields so the TS port can grow toward parity
// without churning the type as new fields land.
export interface AddressDetails {
  house_number?: string;
  road?: string;
  landmark?: string;
  neighbourhood?: string;
  city_block?: string;
  quarter?: string;
  suburb?: string;
  city_district?: string;
  city?: string;
  town?: string;
  village?: string;
  hamlet?: string;
  borough?: string;
  county?: string;
  municipality?: string;
  state_district?: string;
  state?: string;
  region?: string;
  province?: string;
  postcode?: string;
  country?: string;
  country_code?: string;
}

export interface PoiPlace {
  name: string;
  category: string;
  distance_m: number;
}

export interface Address {
  display_name?: string;
  address: AddressDetails;
  places?: PoiPlace[];
}
