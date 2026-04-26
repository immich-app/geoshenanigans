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

// Library: all geocoding logic now lives in query_server (src/lib.rs).
use query_server::*;

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
        if mode == "all" {
            let address = index.query(params.lat, params.lon);
            let admin_debug = index.debug_admin(params.lat, params.lon);
            let primary_debug = index.debug_primary(params.lat, params.lon);
            let places_debug = index.debug_places(params.lat, params.lon);
            let combined = serde_json::json!({
                "result": serde_json::from_str::<serde_json::Value>(
                    &serde_json::to_string(&address).unwrap_or_default()
                ).unwrap_or_default(),
                "admin_polygons": admin_debug,
                "primary_features": primary_debug,
                "place_nodes": places_debug,
            });
            let json = serde_json::to_string_pretty(&combined).unwrap_or_default();
            return ([(axum::http::header::CONTENT_TYPE, "application/json")], json).into_response();
        }
        let debug = if mode == "primary" {
            index.debug_primary(params.lat, params.lon)
        } else if mode == "places" {
            index.debug_places(params.lat, params.lon)
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
            idx.admin_polygons.len() as usize / std::mem::size_of::<AdminPolygon>(),
        )
    };
    let all_vertices: &[NodeCoord] = unsafe {
        std::slice::from_raw_parts(
            idx.admin_vertices.as_ptr() as *const NodeCoord,
            idx.admin_vertices.len() as usize / std::mem::size_of::<NodeCoord>(),
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
            std::slice::from_raw_parts(pp.as_ptr() as *const AdminPolygon, pp.len() as usize / std::mem::size_of::<AdminPolygon>())
        };
        let all_pverts: &[NodeCoord] = unsafe {
            std::slice::from_raw_parts(pv.as_ptr() as *const NodeCoord, pv.len() as usize / std::mem::size_of::<NodeCoord>())
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
                let count = idx.place_nodes.as_ref().unwrap().len() as usize / std::mem::size_of::<PlaceNode>();
                format!(" + {} place nodes", count)
            } else {
                String::new()
            };
            let poi_status = if idx.poi_records.is_some() {
                let count = idx.poi_records.as_ref().unwrap().len() as usize / std::mem::size_of::<PoiRecord>();
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
