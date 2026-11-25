mod auth;
mod downloader;
mod handlers;
// The test portal (the query-portal binary, portal_main.rs), whose tests run
// here beside the routes its answers are compared with: on Unix alone, as
// the portal does.
#[cfg(all(test, unix))]
#[allow(dead_code)]
mod portal;
#[cfg(all(test, unix))]
#[path = "portal/tests.rs"]
mod portal_tests;
mod region_state;
#[cfg(test)]
mod scratch_dir;
#[cfg(test)]
mod test_bucket;

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

// Configuration changes repoint where datasets are downloaded from, so
// they need an admin (dashboard session or an admin's API key). Sidecar
// mode trusts the private network for this as it does for queries.
fn require_admin(
    db: &RwLock<auth::Db>,
    headers: &axum::http::HeaderMap,
    key: Option<&str>,
) -> Result<(), Response> {
    let session_id = auth::get_session_cookie(headers);
    if handlers::anonymous_allowed() && key.is_none() && session_id.is_none() {
        return Ok(());
    }

    let db = db.read().unwrap_or_else(|e| e.into_inner());
    if db.is_admin(session_id.as_deref(), key) {
        Ok(())
    } else {
        Err((StatusCode::UNAUTHORIZED, "Admin access required").into_response())
    }
}

#[derive(Deserialize)]
struct KeyParam {
    key: Option<String>,
}

async fn reverse_geocode(
    Query(params): Query<QueryParams>,
    state: axum::extract::State<Arc<RwLock<auth::Db>>>,
    index: axum::extract::Extension<Arc<MultiIndex>>,
    region_state: axum::extract::Extension<Arc<RegionState>>,
    limiter: axum::extract::Extension<Arc<auth::RateLimiter>>,
    connect_info: axum::extract::ConnectInfo<std::net::SocketAddr>,
) -> Response {
    if let Err(resp) = handlers::check_reverse(&state, &limiter, params.key.as_deref(), params.lat, params.lon, connect_info.0.ip()) {
        return resp;
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

    handlers::reverse_answer(&index, params.lat, params.lon, params.debug.as_deref())
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

// The credit of the data this server answers from, as HTML: the loaded
// datasets' attribution.json and poi_attribution.json (credits_html),
// read-only and unauthenticated, as /test is.
async fn test_credits(index: axum::extract::Extension<Arc<MultiIndex>>) -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        credits_html(&index.credits()),
    )
}

async fn get_configuration(
    Query(params): Query<KeyParam>,
    headers: axum::http::HeaderMap,
    db: axum::extract::State<Arc<RwLock<auth::Db>>>,
    state: axum::extract::Extension<Arc<RegionState>>,
) -> Response {
    if let Err(resp) = require_admin(&db, &headers, params.key.as_deref()) {
        return resp;
    }

    let cfg = state.get_configuration().await;
    let json = serde_json::to_string_pretty(&cfg).unwrap_or_default();
    ([(axum::http::header::CONTENT_TYPE, "application/json")], json).into_response()
}

async fn put_configuration(
    Query(params): Query<KeyParam>,
    headers: axum::http::HeaderMap,
    db: axum::extract::State<Arc<RwLock<auth::Db>>>,
    state: axum::extract::Extension<Arc<RegionState>>,
    axum::Json(new_cfg): axum::Json<Configuration>,
) -> Response {
    if let Err(resp) = require_admin(&db, &headers, params.key.as_deref()) {
        return resp;
    }

    // A selection the build at hand doesn't offer is refused, naming the
    // values it does.
    if let Err(e) = state.set_configuration(new_cfg).await {
        return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
    }
    (StatusCode::ACCEPTED, "configuration updated, downloads scheduled").into_response()
}

async fn polygons_geojson(
    Query(params): Query<QueryParams>,
    state: axum::extract::State<Arc<RwLock<auth::Db>>>,
    index: axum::extract::Extension<Arc<MultiIndex>>,
) -> Response {
    if let Err(resp) = handlers::check_polygons(&state, params.key.as_deref(), params.lat, params.lon) {
        return resp;
    }
    handlers::polygons_answer(&index, params.lat, params.lon)
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
    let db = match auth::Db::load(&db_path) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Error: {} (fix or remove it; an empty account database lets the next login claim admin)", e);
            std::process::exit(1);
        }
    };

    eprintln!("Loading index from {}...", data_dir);
    let index = match MultiIndex::load(data_dir, street_cell_level, admin_cell_level, search_distance) {
        Ok(mi) => {
            let loaded: Vec<_> = mi.continents.iter()
                .filter(|c| c.index.load().is_some())
                .map(|c| c.name)
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
    // server starts empty and waits for a PUT /admin/configuration.
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
        .route("/test", get(handlers::test_portal))
        .route("/test/credits", get(test_credits))
        .route("/admin/configuration", get(get_configuration).put(put_configuration))
        .merge(auth::router())
        .layer(axum::Extension(index))
        .layer(axum::Extension(region_state))
        .layer(axum::Extension(limiter))
        .layer(axum::middleware::from_fn(handlers::cors_middleware))
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
