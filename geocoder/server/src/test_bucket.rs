//! A bucket of one build for the download tests, served on localhost:
//! builds/latest.json, builds/<DATE>/configurations.json and every file's
//! .zst. Each file is empty; the manifest's files table lists every path its
//! components name, in each region of the template.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use sha2::{Digest, Sha256};

use crate::downloader::Configurations;

pub const DATE: &str = "2026-01-01";

const TEMPLATE: &str = include_str!("../../builder/configurations.template.json");

pub struct TestBucket {
    /// The base_url a configuration names the bucket by.
    pub url: String,
    /// What it serves as builds/<DATE>/configurations.json.
    pub manifest: Configurations,
    file_requests: Arc<AtomicUsize>,
}

impl TestBucket {
    /// How many files (.zst) it has served.
    pub fn files_served(&self) -> usize {
        self.file_requests.load(Ordering::SeqCst)
    }
}

/// The template's components.
pub fn template_components() -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(TEMPLATE).unwrap()["components"].clone()
}

struct Served {
    manifest: String,
    body: Vec<u8>,
    file_requests: Arc<AtomicUsize>,
}

async fn get(State(s): State<Arc<Served>>, Path(path): Path<String>) -> Response {
    if path == "latest.json" {
        return format!("{{\"latest\":\"{DATE}\"}}").into_response();
    }
    if path == format!("{DATE}/configurations.json") {
        return s.manifest.clone().into_response();
    }
    if path.starts_with(&format!("{DATE}/")) && path.ends_with(".zst") {
        s.file_requests.fetch_add(1, Ordering::SeqCst);
        return s.body.clone().into_response();
    }
    StatusCode::NOT_FOUND.into_response()
}

/// Serves a build whose manifest has `components`.
pub async fn serve(components: serde_json::Value) -> TestBucket {
    let template: serde_json::Value = serde_json::from_str(TEMPLATE).unwrap();
    let regions: Vec<&String> = template["axes"]["region"]["values"].as_object().unwrap().keys().collect();
    let body = zstd::encode_all(&[][..], 3).unwrap();
    let entry = serde_json::json!({ "size_zst": body.len(), "size_raw": 0, "sha256": format!("{:x}", Sha256::digest(b"")) });
    let mut files = serde_json::Map::new();
    for c in components.as_object().unwrap().values() {
        for f in ["files", "optional_files"].iter().filter_map(|k| c[*k].as_array()).flatten() {
            for region in &regions {
                files.insert(f.as_str().unwrap().replace("{region}", region), entry.clone());
            }
        }
    }
    let manifest = serde_json::json!({ "build": { "date": DATE }, "components": components, "files": files });

    let file_requests = Arc::new(AtomicUsize::new(0));
    let served = Arc::new(Served { manifest: manifest.to_string(), body, file_requests: file_requests.clone() });
    let app = axum::Router::new().route("/builds/{*path}", axum::routing::get(get)).with_state(served);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    TestBucket { url: format!("http://{addr}"), manifest: serde_json::from_value(manifest).unwrap(), file_requests }
}
