// The query routes' work that query-server (main.rs) and query-portal
// (portal_main.rs) share, so that a dataset of the test portal answers
// byte for byte what a query-server answers from the same files: the key,
// coordinate and rate checks, and the answers of /reverse and /polygons
// from a MultiIndex. Each binary keeps its own routes around them.

use std::sync::RwLock;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::auth;
use query_server::{coords_in_range, MultiIndex};

// Shared key-validation preamble for authenticated endpoints. Returns the
// validated token tuple `(login, rps, rpd, by_ip)` on success, or the
// appropriate error `Response` (401 "Missing API key" / 401 "Invalid API
// key") on failure. Callers decide whether to use the returned rate-limit
// fields: `/reverse` rate-limits with them; `/polygons` discards them,
// preserving its existing validate-only (no rate-limit) behaviour.
pub fn require_valid_key(
    db: &RwLock<auth::Db>,
    key: Option<&str>,
) -> Result<(String, u32, u32, bool), Response> {
    // Sidecar mode: when the server runs on a private container network
    // (e.g. as an Immich companion service) there is no key to manage.
    // GEOCODER_ALLOW_ANONYMOUS=true accepts keyless requests with rate
    // limiting effectively disabled. Never set this on an internet-facing
    // deployment.
    if anonymous_allowed() && key.is_none() {
        return Ok(("anonymous".to_string(), u32::MAX, u32::MAX, false));
    }

    let key = match key {
        Some(k) => k,
        None => return Err((StatusCode::UNAUTHORIZED, "Missing API key").into_response()),
    };

    match db.read().unwrap().validate_token(key) {
        Some(info) => Ok(info),
        None => Err((StatusCode::UNAUTHORIZED, "Invalid API key").into_response()),
    }
}

fn require_coords(lat: f64, lon: f64) -> Result<(), Response> {
    if coords_in_range(lat, lon) {
        Ok(())
    } else {
        Err((StatusCode::BAD_REQUEST, "lat must be within ±90 and lon within ±180").into_response())
    }
}

pub fn anonymous_allowed() -> bool {
    static ALLOWED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ALLOWED.get_or_init(|| {
        std::env::var("GEOCODER_ALLOW_ANONYMOUS")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

/// What a /reverse goes through before it is answered: the key (401), the
/// point (400) and the key's rate (429), counted per client IP `ip` for a
/// login rated by IP.
pub fn check_reverse(
    db: &RwLock<auth::Db>,
    limiter: &auth::RateLimiter,
    key: Option<&str>,
    lat: f64,
    lon: f64,
    ip: std::net::IpAddr,
) -> Result<(), Response> {
    let (login, rps, rpd, by_ip) = require_valid_key(db, key)?;
    require_coords(lat, lon)?;

    let rate_key = if by_ip {
        format!("{}:{}", login, ip)
    } else {
        login
    };

    if let Err(msg) = auth::check_rate(limiter, &rate_key, rps, rpd) {
        return Err((StatusCode::TOO_MANY_REQUESTS, msg).into_response());
    }
    Ok(())
}

/// The answer of /reverse at (lat, lon) from `index`: the address, or with
/// `debug` the internals it names. It reads the files: an async caller that
/// must not hold up a runtime thread on a cold page runs it on a blocking
/// thread.
pub fn reverse_answer(index: &MultiIndex, lat: f64, lon: f64, debug: Option<&str>) -> Response {
    if let Some(mode) = debug {
        // Debug endpoints route to the first loaded continent that
        // bbox-matches the point. In single-region (legacy flat) deploys
        // that's the world Index; in per-continent setups it's whichever
        // continent the query lands in. No cross-continent merging on
        // debug paths — they're for inspecting a single Index's internals.
        let arcs = index.loaded_matches(lat, lon);
        let Some(idx) = arcs.first() else {
            return ([(axum::http::header::CONTENT_TYPE, "application/json")], "{}").into_response();
        };
        if mode == "all" {
            let address = idx.query(lat, lon);
            let admin_debug = idx.debug_admin(lat, lon);
            let primary_debug = idx.debug_primary(lat, lon);
            let places_debug = idx.debug_places(lat, lon);
            let combined = serde_json::json!({
                "result": serde_json::to_value(&address).unwrap_or_default(),
                "admin_polygons": admin_debug,
                "primary_features": primary_debug,
                "place_nodes": places_debug,
            });
            let json = serde_json::to_string_pretty(&combined).unwrap_or_default();
            return ([(axum::http::header::CONTENT_TYPE, "application/json")], json).into_response();
        }
        let debug = if mode == "primary" {
            idx.debug_primary(lat, lon)
        } else if mode == "places" {
            idx.debug_places(lat, lon)
        } else {
            idx.debug_admin(lat, lon)
        };
        let json = serde_json::to_string_pretty(&debug).unwrap_or_default();
        return ([(axum::http::header::CONTENT_TYPE, "application/json")], json).into_response();
    }

    let json = index.query_json(lat, lon);
    ([(axum::http::header::CONTENT_TYPE, "application/json")], json).into_response()
}

/// What a /polygons goes through before it is answered: the key (401) and
/// the point (400); no rate.
pub fn check_polygons(db: &RwLock<auth::Db>, key: Option<&str>, lat: f64, lon: f64) -> Result<(), Response> {
    require_valid_key(db, key)?;
    require_coords(lat, lon)
}

/// The answer of /polygons at (lat, lon) from `index`: the admin polygons
/// around the point as GeoJSON. It reads the files, as reverse_answer does.
pub fn polygons_answer(index: &MultiIndex, lat: f64, lon: f64) -> Response {
    // Every loaded continent whose bbox covers the query: at a border
    // (Russia/Türkiye/Egypt) two may, each listing its own polygons.
    let features: Vec<serde_json::Value> = index
        .loaded_matches(lat, lon)
        .iter()
        .flat_map(|idx| idx.polygon_features(lat, lon))
        .collect();

    let fc = serde_json::json!({
        "type": "FeatureCollection",
        "features": features,
    });
    let body = serde_json::to_string(&fc).unwrap_or_default();
    ([(axum::http::header::CONTENT_TYPE, "application/json")], body).into_response()
}

pub async fn test_portal() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("../../test-portal/index.html"),
    )
}

// Permissive CORS, so that a page of another origin (the test page as
// another server serves it, say) can query this one. Browsers treat that
// as cross-origin so we tag every response with `*`.
pub async fn cors_middleware(
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Response {
    if req.method() == axum::http::Method::OPTIONS {
        let mut resp = Response::default();
        let h = resp.headers_mut();
        h.insert("access-control-allow-origin", "*".parse().unwrap());
        h.insert("access-control-allow-methods", "GET, OPTIONS".parse().unwrap());
        h.insert("access-control-allow-headers", "*".parse().unwrap());
        return resp;
    }
    let mut resp = next.run(req).await;
    resp.headers_mut().insert("access-control-allow-origin", "*".parse().unwrap());
    resp
}
