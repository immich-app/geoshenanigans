//! A bucket of one build for the download tests, served on localhost:
//! builds/latest.json, builds/<DATE>/configurations.json and every file as
//! compress_index.py publishes it. The manifest's files table lists every
//! path its components name, in each region of the template, each of bytes
//! of its own: a dir's place, POI and arc files are a small dataset of that
//! dir (the library's fixture encoders), the others made up, a region's JSON
//! files alike as a build's are. A dir holding all the files of one of the
//! template's transport units publishes the unit, the only form of them;
//! every other file goes as its .zst.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use query_server::arc_fixture::ArcFixture;
use query_server::poi_files::PoiPolygon;
use query_server::transport::{self, Kind};
use query_server::{place_files, poi_files, PlaceNode, PoiRecord, PointE7, DEFAULT_ADMIN_CELL_LEVEL, NO_DATA, PLACE_FLAG_HIDDEN};
use sha2::{Digest, Sha256};

use crate::downloader::Configurations;
use crate::scratch_dir::ScratchDir;

pub const DATE: &str = "2026-01-01";

const TEMPLATE: &str = include_str!("../../builder/configurations.template.json");

pub struct TestBucket {
    /// The base_url a configuration names the bucket by.
    pub url: String,
    /// What it serves as builds/<DATE>/configurations.json.
    pub manifest: Configurations,
    file_requests: Arc<AtomicUsize>,
    files: BTreeMap<String, Vec<u8>>,
}

impl TestBucket {
    /// How many objects (a file's .zst or a unit) it has served.
    pub fn files_served(&self) -> usize {
        self.file_requests.load(Ordering::SeqCst)
    }

    /// The bytes of the file at `path`, as its manifest entry gives them.
    pub fn file(&self, path: &str) -> &[u8] {
        &self.files[path]
    }
}

/// The template's components.
pub fn template_components() -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(TEMPLATE).unwrap()["components"].clone()
}

struct Served {
    manifest: String,
    objects: HashMap<String, Vec<u8>>,
    file_requests: Arc<AtomicUsize>,
}

async fn get(State(s): State<Arc<Served>>, Path(path): Path<String>) -> Response {
    if path == "latest.json" {
        return format!("{{\"latest\":\"{DATE}\"}}").into_response();
    }
    if path == format!("{DATE}/configurations.json") {
        return s.manifest.clone().into_response();
    }
    if let Some(body) = path.strip_prefix(&format!("{DATE}/")).and_then(|p| s.objects.get(p)) {
        s.file_requests.fetch_add(1, Ordering::SeqCst);
        return body.clone().into_response();
    }
    StatusCode::NOT_FOUND.into_response()
}

/// A splitmix64 stream, seeded by a name.
struct Rng(u64);

impl Rng {
    fn of(name: &str) -> Rng {
        Rng(name.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| (h ^ b as u64).wrapping_mul(0x100_0000_01b3)))
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn range(&mut self, lo: f32, hi: f32) -> f32 {
        lo + (hi - lo) * (self.below(1 << 24) as f32 / (1 << 24) as f32)
    }
}

/// Made-up bytes of a file at `path`; its region's JSON files alike. A
/// length that is no whole number of rows, so a cols file's unit has a tail.
fn made_up(path: &str) -> Vec<u8> {
    let (region, name) = (path.split('/').next().unwrap(), path.rsplit('/').next().unwrap());
    let mut rng = Rng::of(&if name.ends_with(".json") { format!("{region}/{name}") } else { path.to_string() });
    let n = 64 * (2 + rng.below(30)) + 7;
    (0..n).map(|_| rng.next() as u8).collect()
}

/// The place, POI and arc files of a dir: a small dataset of its own.
fn dataset(dir: &str) -> BTreeMap<&'static str, Vec<u8>> {
    let mut rng = Rng::of(dir);
    let nodes: Vec<PlaceNode> = (0..100u32)
        .map(|i| PlaceNode {
            lat: rng.range(-60.0, 60.0),
            lng: rng.range(-170.0, 170.0),
            name_id: rng.below(1 << 20) as u32,
            place_type: rng.below(7) as u8,
            _pad1: if i % 9 == 4 { PLACE_FLAG_HIDDEN } else { 0 },
            _pad2: 0,
            parent_poly_id: NO_DATA,
        })
        .collect();
    let (places, _) = place_files::encode(&nodes, DEFAULT_ADMIN_CELL_LEVEL);
    let pois: Vec<(PoiRecord, Option<PoiPolygon>)> = (0..80u32)
        .map(|i| {
            let record = PoiRecord {
                lat: rng.range(-60.0, 60.0),
                lng: rng.range(-170.0, 170.0),
                vertex_offset: NO_DATA,
                name_id: i * 3,
                category: rng.below(40) as u8,
                tier: 1,
                flags: 0,
                importance: rng.below(256) as u8,
                parent_street_id: NO_DATA,
                parent_postcode_id: NO_DATA,
                parent_poly_id: i,
            };
            let polygon = (i % 3 == 1).then(|| PoiPolygon {
                encoding: (i % 4) as u8,
                bmin_lat: record.lat,
                bmin_lng: record.lng,
                q: (0..3 + rng.below(20)).map(|_| (rng.below(1000) as u32, rng.below(1000) as u32)).collect(),
            });
            (record, polygon)
        })
        .collect();
    let (records, vertices) = poi_files::encode(&pois);
    let mut arcs = ArcFixture::new();
    for i in 0..30 {
        if i % 11 == 5 {
            arcs.dead_arc();
            continue;
        }
        let (mut lat, mut lng) = (rng.below(1_000_000_000) as i64 - 500_000_000, rng.below(3_000_000_000) as i64 - 1_500_000_000);
        let line: Vec<PointE7> = (0..1 + rng.below(40))
            .map(|_| {
                lat += rng.below(20_001) as i64 - 10_000;
                lng += rng.below(20_001) as i64 - 10_000;
                PointE7 { lat, lng }
            })
            .collect();
        arcs.arc(0, &line);
    }
    BTreeMap::from([
        ("place_cells.bin", places.cells),
        ("place_nodes.bin", places.nodes),
        ("place_hidden.bin", places.hidden),
        ("poi_records.bin", records),
        ("poi_vertices.bin", vertices),
        ("arc_index.bin", arcs.arc_index()),
        ("arc_vertices.bin", arcs.arc_vertices()),
    ])
}

fn sha256(b: &[u8]) -> String {
    format!("{:x}", Sha256::digest(b))
}

/// Serves a build whose manifest has `components`.
pub async fn serve(components: serde_json::Value) -> TestBucket {
    serve_tampered(components, |_, _| {}).await
}

/// Serves a build whose manifest has `components`, each object's bytes
/// before zstd (a unit's stream, a file's bytes) given to `tamper` by its
/// path, to change, the manifest still the true files'.
pub async fn serve_tampered(components: serde_json::Value, tamper: impl Fn(&str, &mut Vec<u8>)) -> TestBucket {
    let template: serde_json::Value = serde_json::from_str(TEMPLATE).unwrap();
    let regions: Vec<&String> = template["axes"]["region"]["values"].as_object().unwrap().keys().collect();
    let mut paths = BTreeSet::new();
    for c in components.as_object().unwrap().values() {
        for f in ["files", "optional_files"].iter().filter_map(|k| c[*k].as_array()).flatten() {
            for region in &regions {
                paths.insert(f.as_str().unwrap().replace("{region}", region));
            }
        }
    }
    let mut by_dir: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for p in &paths {
        let (dir, name) = p.rsplit_once('/').unwrap();
        by_dir.entry(dir.to_string()).or_default().insert(name.to_string());
    }

    let (mut files, mut objects) = (BTreeMap::new(), HashMap::new());
    let (mut entries, mut units) = (serde_json::Map::new(), serde_json::Map::new());
    for (dir, names) in &by_dir {
        let fixture = dataset(dir);
        let bytes: BTreeMap<&str, Vec<u8>> =
            names.iter().map(|n| (n.as_str(), fixture.get(n.as_str()).cloned().unwrap_or_else(|| made_up(&format!("{dir}/{n}"))))).collect();
        let mut in_unit = BTreeSet::new();
        for u in template["transport"]["units"].as_array().unwrap() {
            let held: Vec<&str> = u["files"].as_array().unwrap().iter().map(|f| f.as_str().unwrap()).collect();
            if !held.iter().all(|f| bytes.contains_key(f)) {
                continue;
            }
            let scratch = ScratchDir::new();
            for f in &held {
                std::fs::write(scratch.path().join(f), &bytes[f]).unwrap();
            }
            let kind = Kind::from_name(u["kind"].as_str().unwrap()).unwrap();
            let mut stream = Vec::new();
            transport::encode(kind, scratch.path(), (kind == Kind::Cols).then_some(held[0]), &mut stream).unwrap();
            let path = format!("{dir}/{}", u["unit"].as_str().unwrap());
            tamper(&path, &mut stream);
            let body = zstd::encode_all(&stream[..], 3).unwrap();
            for f in &held {
                entries.insert(format!("{dir}/{f}"), serde_json::json!({ "size_raw": bytes[f].len(), "sha256": sha256(&bytes[f]), "unit": path }));
                in_unit.insert(format!("{dir}/{f}"));
            }
            let held: Vec<String> = held.iter().map(|f| format!("{dir}/{f}")).collect();
            units.insert(path.clone(), serde_json::json!({ "kind": kind.name(), "transport_version": template["transport"]["version"],
                                                           "size": body.len(), "files": held }));
            objects.insert(path, body);
        }
        for (name, b) in &bytes {
            let path = format!("{dir}/{name}");
            if !in_unit.contains(&path) {
                let mut sent = b.clone();
                tamper(&format!("{path}.zst"), &mut sent);
                let body = zstd::encode_all(&sent[..], 3).unwrap();
                entries.insert(path.clone(), serde_json::json!({ "size_zst": body.len(), "size_raw": b.len(), "sha256": sha256(b) }));
                objects.insert(format!("{path}.zst"), body);
            }
            files.insert(path, b.clone());
        }
    }
    let manifest = serde_json::json!({ "build": { "date": DATE }, "components": components, "files": entries, "units": units });

    let file_requests = Arc::new(AtomicUsize::new(0));
    let served = Arc::new(Served { manifest: manifest.to_string(), objects, file_requests: file_requests.clone() });
    let app = axum::Router::new().route("/builds/{*path}", axum::routing::get(get)).with_state(served);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    TestBucket { url: format!("http://{addr}"), manifest: serde_json::from_value(manifest).unwrap(), file_requests, files }
}
