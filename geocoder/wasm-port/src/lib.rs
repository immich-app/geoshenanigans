//! WASM bindings around the query_server lib.  All geocoding logic
//! lives in the server crate (../server/src/lib.rs); this file is just
//! the JS↔Rust glue: take JS Uint8Array buffers, build the Index, and
//! expose a reverse(lat, lng) → JsValue method.
//!
//! Note: wasm32 has a 4 GiB linear-memory cap. The constructor copies
//! each Uint8Array into Vec<u8> (which IS query_server::FileBytes on
//! wasm), so the sum of all file sizes must fit. Continent extracts
//! work; planet's geo_cells (7.7 GiB) and addr_vertices (5 GiB) don't.
//! The caller is responsible for not passing oversized buffers.

use wasm_bindgen::prelude::*;

use query_server::{
    Index, PoiMeta, StringPool, FileBytes,
    DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE,
    STR_TIER_COUNT,
};

#[wasm_bindgen]
pub struct Geocoder {
    index: Index,
}

fn take_required(obj: &js_sys::Object, key: &str) -> Result<Vec<u8>, JsValue> {
    let v = js_sys::Reflect::get(obj, &JsValue::from_str(key))?;
    if v.is_undefined() || v.is_null() {
        return Err(JsValue::from_str(&format!("missing required buffer: {}", key)));
    }
    Ok(js_sys::Uint8Array::from(v).to_vec())
}

fn take_optional(obj: &js_sys::Object, key: &str) -> Option<Vec<u8>> {
    let v = js_sys::Reflect::get(obj, &JsValue::from_str(key)).ok()?;
    if v.is_undefined() || v.is_null() {
        return None;
    }
    Some(js_sys::Uint8Array::from(v).to_vec())
}

fn take_string(obj: &js_sys::Object, key: &str) -> Result<String, JsValue> {
    let v = js_sys::Reflect::get(obj, &JsValue::from_str(key))?;
    v.as_string()
        .ok_or_else(|| JsValue::from_str(&format!("expected string: {}", key)))
}

fn parse_strings_layout(json: &str, tiers: [Option<Vec<u8>>; STR_TIER_COUNT]) -> Result<StringPool, String> {
    let v: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| format!("strings_layout parse: {}", e))?;
    let entries = v.get("tiers").and_then(|t| t.as_array())
        .ok_or("strings_layout: missing 'tiers' array")?;
    if entries.len() != STR_TIER_COUNT {
        return Err(format!("strings_layout: expected {} tiers, got {}", STR_TIER_COUNT, entries.len()));
    }
    let mut bases = [0u32; STR_TIER_COUNT + 1];
    for (t, e) in entries.iter().enumerate() {
        bases[t] = e.get("start").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
        bases[t + 1] = e.get("end").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
    }
    if tiers[0].is_none() {
        return Err("strings_core (tier 0) is required".into());
    }
    // FileBytes IS Vec<u8> on wasm32, so we can pass tiers through.
    let tiers_typed: [Option<FileBytes>; STR_TIER_COUNT] = tiers;
    Ok(StringPool::from_parts(tiers_typed, bases))
}

#[wasm_bindgen]
impl Geocoder {
    /// Construct from a JS object whose values are `Uint8Array | null`
    /// for each binary file, plus `strings_layout: string` (the JSON
    /// from the build pipeline).  Required buffers: admin_cells,
    /// admin_entries, admin_polygons, admin_vertices, strings_core.
    /// Everything else is optional and degrades gracefully.
    #[wasm_bindgen(constructor)]
    pub fn new(buffers: JsValue) -> Result<Geocoder, JsValue> {
        let obj = js_sys::Object::from(buffers);

        let admin_cells = take_required(&obj, "admin_cells")?;
        let admin_entries = take_required(&obj, "admin_entries")?;
        let admin_polygons = take_required(&obj, "admin_polygons")?;
        let admin_vertices = take_required(&obj, "admin_vertices")?;
        let strings_layout = take_string(&obj, "strings_layout")?;

        let str_tiers: [Option<Vec<u8>>; STR_TIER_COUNT] = [
            take_optional(&obj, "strings_core"),
            take_optional(&obj, "strings_street"),
            take_optional(&obj, "strings_addr"),
            take_optional(&obj, "strings_postcode"),
            take_optional(&obj, "strings_poi"),
        ];
        let strings = parse_strings_layout(&strings_layout, str_tiers)
            .map_err(|e| JsValue::from_str(&e))?;

        let poi_meta_json = match js_sys::Reflect::get(&obj, &JsValue::from_str("poi_meta")) {
            Ok(v) if v.is_string() => v.as_string().unwrap_or_default(),
            _ => String::new(),
        };
        let poi_meta = PoiMeta::from_json(&poi_meta_json);

        let index = Index::from_buffers(
            admin_cells, admin_entries, admin_polygons, admin_vertices,
            strings,
            take_optional(&obj, "place_nodes"),
            take_optional(&obj, "place_cells"),
            take_optional(&obj, "place_entries"),
            take_optional(&obj, "postcode_centroids"),
            take_optional(&obj, "postcode_centroid_cells"),
            take_optional(&obj, "postcode_centroid_entries"),
            take_optional(&obj, "poi_records"),
            take_optional(&obj, "poi_vertices"),
            take_optional(&obj, "poi_cells"),
            take_optional(&obj, "poi_entries"),
            poi_meta,
            take_optional(&obj, "geo_cells"),
            take_optional(&obj, "street_ways"),
            take_optional(&obj, "street_nodes"),
            take_optional(&obj, "street_entries"),
            take_optional(&obj, "addr_points"),
            take_optional(&obj, "addr_vertices"),
            take_optional(&obj, "addr_entries"),
            take_optional(&obj, "interp_ways"),
            take_optional(&obj, "interp_nodes"),
            take_optional(&obj, "interp_entries"),
            take_optional(&obj, "way_postcodes"),
            take_optional(&obj, "addr_postcodes"),
            take_optional(&obj, "admin_parents"),
            take_optional(&obj, "way_parents"),
            take_optional(&obj, "postal_polygons"),
            take_optional(&obj, "postal_vertices"),
            DEFAULT_STREET_CELL_LEVEL,
            DEFAULT_ADMIN_CELL_LEVEL,
            DEFAULT_SEARCH_DISTANCE,
        );

        Ok(Geocoder { index })
    }

    /// Reverse-geocode (lat, lng). Returns a JS object with shape
    /// `{ display_name, address, places? }` matching the Rust HTTP
    /// server's /reverse?lat=&lon= response.
    #[wasm_bindgen]
    pub fn reverse(&self, lat: f64, lng: f64) -> Result<JsValue, JsValue> {
        let r = self.index.query(lat, lng);
        serde_wasm_bindgen::to_value(&r).map_err(|e| JsValue::from_str(&e.to_string()))
    }
}
