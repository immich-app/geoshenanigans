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

fn take_required(obj: &js_sys::Object, key: &str) -> Result<FileBytes, JsValue> {
    let v = js_sys::Reflect::get(obj, &JsValue::from_str(key))?;
    if v.is_undefined() || v.is_null() {
        return Err(JsValue::from_str(&format!("missing required buffer: {}", key)));
    }
    Ok(FileBytes::Owned(js_sys::Uint8Array::from(v).to_vec()))
}

fn take_optional(obj: &js_sys::Object, key: &str) -> Option<FileBytes> {
    let v = js_sys::Reflect::get(obj, &JsValue::from_str(key)).ok()?;
    if v.is_undefined() || v.is_null() {
        return None;
    }
    Some(FileBytes::Owned(js_sys::Uint8Array::from(v).to_vec()))
}

/// Pull a chunked-byte-source descriptor for a given key.  The JS
/// caller provides `<key>_chunked: { handle: number, len: number }`
/// when the file is too large for in-linear-memory loading.  Returns
/// None if the key is absent.  Requires a global `js_read` callback
/// previously installed via `set_js_read`.
fn take_chunked(obj: &js_sys::Object, key: &str) -> Option<FileBytes> {
    let chunked_key = format!("{}_chunked", key);
    let v = js_sys::Reflect::get(obj, &JsValue::from_str(&chunked_key)).ok()?;
    if v.is_undefined() || v.is_null() { return None; }
    let desc = js_sys::Object::from(v);
    let handle = js_sys::Reflect::get(&desc, &JsValue::from_str("handle")).ok()?
        .as_f64()?;
    let len = js_sys::Reflect::get(&desc, &JsValue::from_str("len")).ok()?
        .as_f64()? as usize;
    let js_read = JS_READ.with(|r| r.borrow().clone())
        .expect("set_js_read must be called before constructing a chunked-backed Geocoder");
    Some(FileBytes::JsChunked(query_server::wasm_chunked::JsChunked::new(js_read, handle, len)))
}

/// Either an Owned buffer or a JsChunked source — caller may pass
/// whichever fits.  Owned is preferred when the file is small enough
/// to copy into wasm linear memory; JsChunked is required for files
/// larger than ~2-3 GiB total.
fn take_either(obj: &js_sys::Object, key: &str) -> Option<FileBytes> {
    take_optional(obj, key).or_else(|| take_chunked(obj, key))
}

thread_local! {
    static JS_READ: std::cell::RefCell<Option<js_sys::Function>> = std::cell::RefCell::new(None);
}

/// Install the JS read callback used by JsChunked-backed sources.
/// Signature: `(handle: number, off: number, len: number) -> Uint8Array`.
/// Must be called before constructing a Geocoder that uses any
/// `*_chunked` buffer descriptors.
#[wasm_bindgen]
pub fn set_js_read(f: js_sys::Function) {
    JS_READ.with(|r| *r.borrow_mut() = Some(f));
}

fn take_string(obj: &js_sys::Object, key: &str) -> Result<String, JsValue> {
    let v = js_sys::Reflect::get(obj, &JsValue::from_str(key))?;
    v.as_string()
        .ok_or_else(|| JsValue::from_str(&format!("expected string: {}", key)))
}

fn parse_strings_layout(json: &str, tiers: [Option<FileBytes>; STR_TIER_COUNT]) -> Result<StringPool, String> {
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
    Ok(StringPool::from_parts(tiers, bases))
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

        let str_tiers: [Option<FileBytes>; STR_TIER_COUNT] = [
            take_either(&obj, "strings_core"),
            take_either(&obj, "strings_street"),
            take_either(&obj, "strings_addr"),
            take_either(&obj, "strings_postcode"),
            take_either(&obj, "strings_poi"),
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
            take_either(&obj, "place_nodes"),
            take_either(&obj, "place_cells"),
            take_either(&obj, "place_entries"),
            take_either(&obj, "postcode_centroids"),
            take_either(&obj, "postcode_centroid_cells"),
            take_either(&obj, "postcode_centroid_entries"),
            take_either(&obj, "poi_records"),
            take_either(&obj, "poi_vertices"),
            take_either(&obj, "poi_cells"),
            take_either(&obj, "poi_entries"),
            poi_meta,
            take_either(&obj, "geo_cells"),
            take_either(&obj, "street_ways"),
            take_either(&obj, "street_nodes"),
            take_either(&obj, "street_entries"),
            take_either(&obj, "addr_points"),
            take_either(&obj, "addr_vertices"),
            take_either(&obj, "addr_entries"),
            take_either(&obj, "interp_ways"),
            take_either(&obj, "interp_nodes"),
            take_either(&obj, "interp_entries"),
            take_either(&obj, "way_postcodes"),
            take_either(&obj, "addr_postcodes"),
            take_either(&obj, "admin_parents"),
            take_either(&obj, "way_parents"),
            take_either(&obj, "postal_polygons"),
            take_either(&obj, "postal_vertices"),
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
