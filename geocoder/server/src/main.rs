mod auth;
mod downloader;
mod region_state;

use region_state::{Configuration, RegionState};

use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use serde::Deserialize;
use std::sync::{Arc, RwLock};

// Library: all geocoding logic now lives in query_server (src/lib.rs).
use query_server::*;

#[derive(Deserialize)]
struct QueryParams {
    lat: f64,
    lon: f64,
    key: Option<String>,
    debug: Option<String>,
}

// Shared key-validation preamble for authenticated endpoints. Returns the
// validated token tuple `(login, rps, rpd, by_ip)` on success, or the
// appropriate error `Response` (401 "Missing API key" / 401 "Invalid API
// key") on failure. Callers decide whether to use the returned rate-limit
// fields: `/reverse` rate-limits with them; `/polygons` discards them,
// preserving its existing validate-only (no rate-limit) behaviour.
fn require_valid_key(
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

fn anonymous_allowed() -> bool {
    static ALLOWED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ALLOWED.get_or_init(|| {
        std::env::var("GEOCODER_ALLOW_ANONYMOUS")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

async fn reverse_geocode(
    Query(params): Query<QueryParams>,
    state: axum::extract::State<Arc<RwLock<auth::Db>>>,
    index: axum::extract::Extension<Arc<MultiIndex>>,
    region_state: axum::extract::Extension<Arc<RegionState>>,
    limiter: axum::extract::Extension<Arc<auth::RateLimiter>>,
    connect_info: axum::extract::ConnectInfo<std::net::SocketAddr>,
) -> Response {
    let (login, rps, rpd, by_ip) = match require_valid_key(&state, params.key.as_deref()) {
        Ok(info) => info,
        Err(resp) => return resp,
    };

    let rate_key = if by_ip {
        format!("{}:{}", login, connect_info.0.ip())
    } else {
        login
    };

    if let Err(msg) = auth::check_rate(&limiter, &rate_key, rps, rpd) {
        return (StatusCode::TOO_MANY_REQUESTS, msg).into_response();
    }

    // On-demand region download: if the point bbox-matches one or
    // more continents we don't have loaded, trigger a synchronous
    // download (single-flighted) and block this request until done.
    // Concurrent requests for the same unloaded continent share one
    // download via the per-region mutex in RegionState.
    let unloaded = index.unloaded_matches(params.lat, params.lon);
    for region in &unloaded {
        if let Err(e) = region_state.ensure_loaded(region).await {
            eprintln!("on-demand load of {}: {}", region, e);
            // Fall through — the query against still-loaded continents
            // (if any) returns whatever it can. Failure to download
            // one continent shouldn't block answers from others.
        }
    }

    if let Some(ref mode) = params.debug {
        // Debug endpoints route to the first loaded continent that
        // bbox-matches the point. In single-region (legacy flat) deploys
        // that's the world Index; in per-continent setups it's whichever
        // continent the query lands in. No cross-continent merging on
        // debug paths — they're for inspecting a single Index's internals.
        let arcs = index.loaded_matches(params.lat, params.lon);
        let Some(idx) = arcs.first() else {
            return ([(axum::http::header::CONTENT_TYPE, "application/json")], "{}").into_response();
        };
        if mode == "all" {
            let address = idx.query(params.lat, params.lon);
            let admin_debug = idx.debug_admin(params.lat, params.lon);
            let primary_debug = idx.debug_primary(params.lat, params.lon);
            let places_debug = idx.debug_places(params.lat, params.lon);
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
            idx.debug_primary(params.lat, params.lon)
        } else if mode == "places" {
            idx.debug_places(params.lat, params.lon)
        } else {
            idx.debug_admin(params.lat, params.lon)
        };
        let json = serde_json::to_string_pretty(&debug).unwrap_or_default();
        return ([(axum::http::header::CONTENT_TYPE, "application/json")], json).into_response();
    }

    let json = index.query_json(params.lat, params.lon);
    ([(axum::http::header::CONTENT_TYPE, "application/json")], json).into_response()
}

async fn health(
    index: axum::extract::Extension<Arc<MultiIndex>>,
) -> impl IntoResponse {
    // Liveness + readiness in one: 200 once the index is loaded and
    // serving. Container orchestrators (Immich compose health checks)
    // poll this; keep it allocation-light and unauthenticated.
    let body = format!("{{\"status\":\"ok\",\"regions\":{}}}", index.loaded_count());
    ([(axum::http::header::CONTENT_TYPE, "application/json")], body)
}

async fn test_portal() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("../../test-portal/index.html"),
    )
}

async fn get_configuration(
    state: axum::extract::Extension<Arc<RegionState>>,
) -> Response {
    let cfg = state.get_configuration().await;
    let json = serde_json::to_string_pretty(&cfg).unwrap_or_default();
    ([(axum::http::header::CONTENT_TYPE, "application/json")], json).into_response()
}

async fn put_configuration(
    state: axum::extract::Extension<Arc<RegionState>>,
    axum::Json(new_cfg): axum::Json<Configuration>,
) -> Response {
    if let Err(e) = state.set_configuration(new_cfg).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("persist failed: {}", e)).into_response();
    }
    (StatusCode::ACCEPTED, "configuration updated, downloads scheduled").into_response()
}

// Permissive CORS for the test portal. The full-dataset server embeds
// the test page; the page also fetches from a second minimal-dataset
// server on a different port for side-by-side comparison. Browsers
// treat that as cross-origin so we tag every response with `*`.
async fn cors_middleware(
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

async fn polygons_geojson(
    Query(params): Query<QueryParams>,
    state: axum::extract::State<Arc<RwLock<auth::Db>>>,
    index: axum::extract::Extension<Arc<MultiIndex>>,
) -> Response {
    if let Err(resp) = require_valid_key(&state, params.key.as_deref()) {
        return resp;
    }

    let lat = params.lat;
    let lng = params.lon;

    const ID_MASK: u32 = 0x7FFFFFFF;

    let mut features: Vec<serde_json::Value> = Vec::new();

    // Iterate every loaded continent whose bbox covers the query.
    // For boundary points (Russia/Türkiye/Egypt) two continents may
    // contribute — feature IDs are local to each Index, so we dedupe
    // per-Index using `seen`.
    for idx in index.loaded_matches(lat, lng) {
        let mut seen: std::collections::HashSet<u32> = std::collections::HashSet::new();

        let cell = cell_id_at_level(lat, lng, idx.admin_cell_level);
        let neighbors = cell_neighbors_at_level(cell, idx.admin_cell_level);

        let mut hits: Vec<u32> = Vec::new();
        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            Index::for_each_entry_fb(&idx.admin_entries, Index::lookup_admin_cell_fb(&idx.admin_cells, c), |id| {
                let poly_id = id & ID_MASK;
                if seen.insert(poly_id) { hits.push(poly_id); }
            });
        }
        for poly_id in hits {
            let Some(poly) = idx.admin_polygon(poly_id) else { continue; };
            // v15 vertices are per-polygon delta-encoded with a 10-byte
            // inline header — must decode through admin_verts(), not a
            // raw [NodeCoord] cast (which produces nonsense floats).
            let Some(verts) = idx.admin_verts(&poly) else { continue; };
            if !point_in_polygon(lat as f32, lng as f32, &verts) { continue; }
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
        }

        if let Some(ref pp) = &idx.postal_polygons {
            let total = pp.len() / std::mem::size_of::<AdminPolygon>() as u64;
            for i in 0..total {
                let poly: AdminPolygon = pp.read_at::<AdminPolygon>(i);
                let Some(verts) = decode_polygon_verts(
                    idx.postal_vertices.as_ref().unwrap(),
                    poly.vertex_offset, poly.vertex_count
                ) else { continue; };
                if !point_in_polygon(lat as f32, lng as f32, &verts) { continue; }
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
    }

    let fc = serde_json::json!({
        "type": "FeatureCollection",
        "features": features,
    });
    let body = serde_json::to_string(&fc).unwrap_or_default();
    ([(axum::http::header::CONTENT_TYPE, "application/json")], body).into_response()
}

// Worker pool sized for a lightweight sidecar: reverse geocoding is
// short-lived CPU + page-cache work, and a handful of workers saturates
// it. On big shared hosts the tokio default (one worker per core, 64+
// threads on servers) is pure overhead for this service. Override with
// GEOCODER_WORKER_THREADS when needed.
fn worker_threads() -> usize {
    std::env::var("GEOCODER_WORKER_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4).min(8))
}

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads())
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async_main());
}

async fn async_main() {
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
    let index = match MultiIndex::load(data_dir, street_cell_level, admin_cell_level, search_distance) {
        Ok(mi) => {
            let loaded: Vec<_> = mi.continents.iter()
                .filter(|c| c.index.load().is_some())
                .map(|c| c.bbox.name)
                .collect();
            if loaded.len() == 1 && loaded[0] == "world" {
                eprintln!("Loaded single-region index from {}", data_dir);
            } else {
                eprintln!("Loaded {} continent(s): {}", loaded.len(), loaded.join(", "));
            }
            Arc::new(mi)
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    let db = Arc::new(RwLock::new(db));
    let limiter = Arc::new(auth::RateLimiter::default()); // RwLock<HashMap> with atomic counters

    let region_state = Arc::new(RegionState::new(index.clone(), std::path::PathBuf::from(data_dir)));

    // On startup, kick off any preload downloads from the persisted
    // configuration. Skipped silently if there's no persisted file —
    // server starts empty and waits for an Immich PUT.
    {
        let s = region_state.clone();
        let cfg = s.get_configuration().await;
        for region in cfg.preload {
            let s = s.clone();
            tokio::spawn(async move {
                if let Err(e) = s.ensure_loaded(&region).await {
                    eprintln!("startup preload {}: {}", region, e);
                }
            });
        }
    }

    let app = Router::new()
        .route("/health", get(health))
        .route("/reverse", get(reverse_geocode))
        .route("/polygons", get(polygons_geojson))
        .route("/test", get(test_portal))
        .route("/admin/configuration", get(get_configuration).put(put_configuration))
        .merge(auth::router())
        .layer(axum::Extension(index))
        .layer(axum::Extension(region_state))
        .layer(axum::Extension(limiter))
        .layer(axum::middleware::from_fn(cors_middleware))
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
