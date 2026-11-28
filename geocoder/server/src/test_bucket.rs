//! A bucket of one build for the download tests, served on localhost:
//! builds/latest.json, builds/<DATE>/configurations.json and every file as
//! compress_index.py publishes it. A test can add builds of other dates and
//! versions, of the same files (TestBucket::add_build), publish one again
//! with its manifest changed (TestBucket::edit_build), and move latest.json
//! to one (TestBucket::set_latest). The manifest's files table lists every
//! path its components name, in each region of the template, each of bytes
//! of its own: a dir's place, POI and arc files are a small dataset of that
//! dir (the library's fixture encoders), the others made up, a region's JSON
//! files alike as a build's are; dirs a test names alike (serve_alike) hold
//! the first one's bytes under each name. A dir holding all the files of one
//! of the template's transport units publishes the unit, the only form of them;
//! every other file goes as its .zst. A test can have it hold an object's
//! body back half way (TestBucket::hold), or a manifest's, or fail the next
//! fetches of one (TestBucket::fail). A case of a host without CA
//! certificates runs in a process of its own (without_ca_store).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use query_server::arc_fixture::ArcFixture;
use query_server::poi_files::PoiPolygon;
use query_server::transport::{self, Kind};
use query_server::{lower_hex, place_files, poi_files, PlaceNode, PoiRecord, PointE7, DEFAULT_ADMIN_CELL_LEVEL, NO_DATA, PLACE_FLAG_HIDDEN};
use sha2::{Digest, Sha256};

use crate::downloader::Configurations;
use crate::scratch_dir::ScratchDir;

pub const DATE: &str = "2026-01-01";

const TEMPLATE: &str = include_str!("../../builder/configurations.template.json");

pub struct TestBucket {
    /// The base_url a configuration names the bucket by.
    pub url: String,
    /// The manifest of DATE as it first serves it, at
    /// builds/<DATE>/configurations.json: a publication of DATE again
    /// (edit_build) changes what it serves, not this.
    pub manifest: Configurations,
    file_requests: Arc<AtomicUsize>,
    files: BTreeMap<String, Vec<u8>>,
    hold: Arc<Mutex<Option<Hold>>>,
    served: Arc<Served>,
}

/// An object whose body the bucket holds back half way (TestBucket::hold).
struct Hold {
    path: String,
    reached: Arc<tokio::sync::Notify>,
    go: tokio::sync::oneshot::Receiver<()>,
}

/// Held by a test while the bucket holds an object's body back: the rest
/// goes once it is dropped.
pub struct Holding {
    path: String,
    reached: Arc<tokio::sync::Notify>,
    _go: tokio::sync::oneshot::Sender<()>,
}

impl Holding {
    /// Returns once the bucket sent the first half of the body; panics,
    /// naming the object, once it has waited a minute for it.
    pub async fn reached(&self) {
        if tokio::time::timeout(std::time::Duration::from_secs(60), self.reached.notified()).await.is_err() {
            panic!("waited a minute for the bucket to send half of {}", self.path);
        }
    }
}

impl TestBucket {
    /// How many fetches of an object (a file's .zst or a unit) it has had,
    /// those it failed (fail) among them.
    pub fn files_served(&self) -> usize {
        self.file_requests.load(Ordering::SeqCst)
    }

    /// Holds back the body of the object at `path` (a unit, a file's .zst,
    /// or `configurations.json`, a build's manifest) the next time it is
    /// fetched: it sends the first half, and the rest once the Holding
    /// returned is dropped.
    pub fn hold(&self, path: &str) -> Holding {
        let (reached, (go_tx, go)) = (Arc::new(tokio::sync::Notify::new()), tokio::sync::oneshot::channel());
        *self.hold.lock().unwrap() = Some(Hold { path: path.to_string(), reached: reached.clone(), go });
        Holding { path: path.to_string(), reached, _go: go_tx }
    }

    /// The bytes of the file at `path`, as its manifest entry gives them.
    pub fn file(&self, path: &str) -> &[u8] {
        &self.files[path]
    }

    /// Has it serve each object as the manifest gives it from now on, one a
    /// serve_tampered changed of its true bytes, as a bucket that recovered.
    pub fn mend(&self) {
        self.served.mended.store(true, Ordering::SeqCst);
    }

    /// Serves a build of `date` too, of build version `version` and the
    /// same files as DATE's.
    pub fn add_build(&self, date: &str, version: u32) {
        let mut manifest: serde_json::Value = serde_json::from_str(&self.served.builds.lock().unwrap()[DATE]).unwrap();
        manifest["build"] = serde_json::json!({ "date": date, "version": version });
        self.served.builds.lock().unwrap().insert(date.to_string(), manifest.to_string());
    }

    /// Publishes the build of `date` again, its manifest as `edit` changes
    /// it (a rerun of the nightly over a date's build).
    pub fn edit_build(&self, date: &str, edit: impl FnOnce(&mut serde_json::Value)) {
        let mut builds = self.served.builds.lock().unwrap();
        let mut manifest: serde_json::Value = serde_json::from_str(&builds[date]).unwrap();
        edit(&mut manifest);
        builds.insert(date.to_string(), manifest.to_string());
    }

    /// Has latest.json name the build of `date`, of build version
    /// `version` (none: a latest.json without one).
    pub fn set_latest(&self, date: &str, version: Option<u32>) {
        *self.served.latest.lock().unwrap() = latest_json(date, version);
    }

    /// Each fetch of an object (a file's .zst or a unit) it has had so far,
    /// in order, those it failed (fail) among them: `<date>/<path>`.
    pub fn requests(&self) -> Vec<String> {
        self.served.requests.lock().unwrap().clone()
    }

    /// Fails the next `times` fetches of the object at `path` (a unit, or a
    /// file's .zst), of any build, with a 503.
    pub fn fail(&self, path: &str, times: usize) {
        self.served.failing.lock().unwrap().insert(path.to_string(), times);
    }
}

/// The build version of the template, which a build of the bucket has
/// unless added as another (TestBucket::add_build).
pub fn template_version() -> u32 {
    serde_json::from_str::<serde_json::Value>(TEMPLATE).unwrap()["build"]["version"].as_u64().unwrap() as u32
}

fn latest_json(date: &str, version: Option<u32>) -> String {
    match version {
        Some(v) => format!("{{\"build_version\":{v},\"latest\":\"{date}\"}}"),
        None => format!("{{\"latest\":\"{date}\"}}"),
    }
}

/// The template's components.
pub fn template_components() -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(TEMPLATE).unwrap()["components"].clone()
}

struct Served {
    /// What it serves as latest.json.
    latest: Mutex<String>,
    /// Each build's configurations.json, by date.
    builds: Mutex<BTreeMap<String, String>>,
    objects: HashMap<String, Vec<u8>>,
    /// The true bodies of the objects tampered with, served once mended.
    true_objects: HashMap<String, Vec<u8>>,
    mended: AtomicBool,
    file_requests: Arc<AtomicUsize>,
    requests: Mutex<Vec<String>>,
    failing: Mutex<HashMap<String, usize>>,
    hold: Arc<Mutex<Option<Hold>>>,
}

async fn get(State(s): State<Arc<Served>>, Path(path): Path<String>) -> Response {
    if path == "latest.json" {
        return s.latest.lock().unwrap().clone().into_response();
    }
    let Some((date, rest)) = path.split_once('/') else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(manifest) = s.builds.lock().unwrap().get(date).cloned() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if rest == "configurations.json" {
        return send(&s, rest, manifest.as_bytes());
    }
    if let Some((object, body)) = s.objects.get_key_value(rest) {
        s.file_requests.fetch_add(1, Ordering::SeqCst);
        s.requests.lock().unwrap().push(path.clone());
        if let Some(left) = s.failing.lock().unwrap().get_mut(object).filter(|left| **left > 0) {
            *left -= 1;
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        let body = if s.mended.load(Ordering::SeqCst) { s.true_objects.get(object).unwrap_or(body) } else { body };
        return send(&s, object, body);
    }
    StatusCode::NOT_FOUND.into_response()
}

/// The response of `body`, the object at `object`: its first half, and the
/// rest once the test lets go, where it holds the object back (hold).
fn send(s: &Served, object: &str, body: &[u8]) -> Response {
    let held = s.hold.lock().unwrap().take_if(|h| h.path == object);
    if let Some(Hold { reached, go, .. }) = held {
        use futures_util::StreamExt;
        let (first, rest) = body.split_at(body.len() / 2);
        let (first, rest) = (bytes::Bytes::copy_from_slice(first), bytes::Bytes::copy_from_slice(rest));
        let rest = futures_util::stream::once(async move {
            reached.notify_one();
            let _ = go.await;
            Ok::<_, std::io::Error>(rest)
        });
        return axum::body::Body::from_stream(futures_util::stream::iter([Ok(first)]).chain(rest)).into_response();
    }
    body.to_vec().into_response()
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
        .map(|i| {
            let (lat, lng, name_id) = (rng.range(-60.0, 60.0), rng.range(-170.0, 170.0), rng.below(1 << 20) as u32);
            let place_type = rng.below(7) as u8;
            let hidden = i % 9 == 4;
            PlaceNode {
                lat,
                lng,
                name_id,
                place_type,
                _pad1: if hidden { PLACE_FLAG_HIDDEN } else { 0 },
                rank_address: if hidden { 0 } else { crate::PLACE_NODE_RANK[place_type as usize] },
                parent_poly_id: NO_DATA,
            }
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
    lower_hex(&Sha256::digest(b))
}

/// Serves a build whose manifest has `components`.
pub async fn serve(components: serde_json::Value) -> TestBucket {
    serve_tampered(components, |_, _| {}).await
}

/// Serves a build whose manifest has `components`, the dirs `alike` holding
/// under each name the bytes the first of them holds, as the place files
/// and core strings of the planet build's modes full, no-addresses and
/// admin are.
pub async fn serve_alike(components: serde_json::Value, alike: &[&str]) -> TestBucket {
    serve_with(components, |_, _| {}, alike).await
}

/// Serves a build whose manifest has `components`, each object's bytes
/// before zstd (a unit's stream, a file's bytes) given to `tamper` by its
/// path, to change, the manifest still the true files' (and the true bytes
/// served once it is mended).
pub async fn serve_tampered(components: serde_json::Value, tamper: impl Fn(&str, &mut Vec<u8>)) -> TestBucket {
    serve_with(components, tamper, &[]).await
}

async fn serve_with(components: serde_json::Value, tamper: impl Fn(&str, &mut Vec<u8>), alike: &[&str]) -> TestBucket {
    let template: serde_json::Value = serde_json::from_str(TEMPLATE).unwrap();
    let regions: Vec<&String> = template["axes"]["region"]["values"].as_object().unwrap().keys().collect();
    let mut paths = BTreeSet::new();
    for c in components.as_object().unwrap().values() {
        for f in ["files", "optional_files"].iter().filter_map(|k| c[*k].as_array()).flatten() {
            for region in &regions {
                // The injected geographic datasets have their own rows. This
                // bucket exercises a build without the optional search columns.
                let path = f.as_str().unwrap();
                let name = path.rsplit('/').next().unwrap();
                if name.starts_with("strings_tags_") || name.ends_with("_tags.bin") || name.ends_with("_imp.bin") {
                    continue;
                }
                paths.insert(path.replace("{region}", region));
            }
        }
    }
    let mut by_dir: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for p in &paths {
        let (dir, name) = p.rsplit_once('/').unwrap();
        by_dir.entry(dir.to_string()).or_default().insert(name.to_string());
    }

    let (mut files, mut objects, mut true_objects) = (BTreeMap::new(), HashMap::new(), HashMap::new());
    let (mut entries, mut units) = (serde_json::Map::new(), serde_json::Map::new());
    for (dir, names) in &by_dir {
        let of = if alike.contains(&dir.as_str()) { alike[0] } else { dir.as_str() };
        let fixture = dataset(of);
        let bytes: BTreeMap<&str, Vec<u8>> =
            names.iter().map(|n| (n.as_str(), fixture.get(n.as_str()).cloned().unwrap_or_else(|| made_up(&format!("{of}/{n}"))))).collect();
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
            let true_stream = stream.clone();
            tamper(&path, &mut stream);
            if stream != true_stream {
                true_objects.insert(path.clone(), zstd::encode_all(&true_stream[..], 3).unwrap());
            }
            let body = zstd::encode_all(&stream[..], 3).unwrap();
            // The manifest's size is the true unit's, served once mended.
            let size = true_objects.get(&path).map_or(body.len(), Vec::len);
            for f in &held {
                entries.insert(format!("{dir}/{f}"), serde_json::json!({ "size_raw": bytes[f].len(), "sha256": sha256(&bytes[f]), "unit": path }));
                in_unit.insert(format!("{dir}/{f}"));
            }
            let held: Vec<String> = held.iter().map(|f| format!("{dir}/{f}")).collect();
            units.insert(path.clone(), serde_json::json!({ "kind": kind.name(), "transport_version": template["transport"]["version"],
                                                           "size": size, "files": held }));
            objects.insert(path, body);
        }
        for (name, b) in &bytes {
            let path = format!("{dir}/{name}");
            if !in_unit.contains(&path) {
                let mut sent = b.clone();
                tamper(&format!("{path}.zst"), &mut sent);
                if sent != *b {
                    true_objects.insert(format!("{path}.zst"), zstd::encode_all(&b[..], 3).unwrap());
                }
                let body = zstd::encode_all(&sent[..], 3).unwrap();
                let size = true_objects.get(&format!("{path}.zst")).map_or(body.len(), Vec::len);
                entries.insert(path.clone(), serde_json::json!({ "size_zst": size, "size_raw": b.len(), "sha256": sha256(b) }));
                objects.insert(format!("{path}.zst"), body);
            }
            files.insert(path, b.clone());
        }
    }
    // The template's own sections besides, as a build's manifest has them.
    let mut manifest = template.clone();
    manifest["build"] = serde_json::json!({ "date": DATE, "version": template["build"]["version"] });
    manifest["components"] = components;
    manifest["files"] = entries.into();
    manifest["units"] = units.into();

    let (file_requests, hold) = (Arc::new(AtomicUsize::new(0)), Arc::new(Mutex::new(None)));
    let served = Arc::new(Served {
        latest: Mutex::new(latest_json(DATE, Some(template_version()))),
        builds: Mutex::new(BTreeMap::from([(DATE.to_string(), manifest.to_string())])),
        objects,
        true_objects,
        mended: AtomicBool::new(false),
        file_requests: file_requests.clone(),
        requests: Mutex::new(Vec::new()),
        failing: Mutex::new(HashMap::new()),
        hold: hold.clone(),
    });
    let app = axum::Router::new().route("/builds/{*path}", axum::routing::get(get)).with_state(served.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    TestBucket { url: format!("http://{addr}"), manifest: serde_json::from_value(manifest).unwrap(), file_requests, files, hold, served }
}

/// Set for a process `without_ca_store` starts.
#[cfg(all(unix, not(target_vendor = "apple"), not(target_os = "android")))]
pub const NO_CA_STORE: &str = "GEOCODER_TEST_NO_CA_STORE";

/// Runs the ignored test `name` of this binary alone, in a process of its
/// own whose CA store is empty: SSL_CERT_FILE and SSL_CERT_DIR, which name
/// the store in place of the system's where it is read from files (Unix but
/// Apple's and Android's: elsewhere the verifier asks the system, which they
/// don't change), name none, and NO_CA_STORE is set. The two are the
/// process's, so it runs apart. Panics with its output unless it passed.
#[cfg(all(unix, not(target_vendor = "apple"), not(target_os = "android")))]
pub fn without_ca_store(name: &str) {
    let empty = ScratchDir::new();
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([name, "--exact", "--ignored", "--test-threads=1"])
        .env("SSL_CERT_FILE", empty.path().join("none.pem"))
        .env("SSL_CERT_DIR", empty.path())
        .env(NO_CA_STORE, "1")
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success() && stdout.contains(" 1 passed;"), "{stdout}{}", String::from_utf8_lossy(&out.stderr));
}

/// A CA certificate of no one's, self-signed, its key thrown away: a store
/// the client builds with, and the bucket, of plain HTTP, never asks for it.
#[cfg(all(unix, not(target_vendor = "apple"), not(target_os = "android")))]
const TEST_CA: &str = "-----BEGIN CERTIFICATE-----
MIIBfDCCASKgAwIBAgIUALXgKemIbz/MhEkVWAJ5pfbUXyIwCgYIKoZIzj0EAwIw
GzEZMBcGA1UEAwwQZ2VvY29kZXIgdGVzdCBDQTAgFw0yNjExMjMyMDE3MDdaGA8y
MTI2MTAzMDIwMTcwN1owGzEZMBcGA1UEAwwQZ2VvY29kZXIgdGVzdCBDQTBZMBMG
ByqGSM49AgEGCCqGSM49AwEHA0IABBsXpEgb5tuTIbszY0eKXGlLFQBAcJxwthjM
37dCc0ANoKmnGL97NqFrNeuw1crik6vrHRZYbUavNONOB1t5+jmjQjBAMB0GA1Ud
DgQWBBTaVqM64tlUtORGKJxgEMAF9tAjVzAPBgNVHRMBAf8EBTADAQH/MA4GA1Ud
DwEB/wQEAwICBDAKBggqhkjOPQQDAgNIADBFAiEA0fpbsRDb6LEPJeV6GybFv76L
pis9+k8eFkw/rp4JIuACIFgTRdGzU46T24xNttrCePT+/UVqgJKMkPn9UJa8fdOl
-----END CERTIFICATE-----
";

/// Gives a process of `without_ca_store` its certificates: SSL_CERT_FILE
/// names TEST_CA, written into `dir`, not the host's store, which may be
/// none; SSL_CERT_DIR still names an empty dir. The process runs its case
/// alone, on a runtime of one thread, so the change is safe there.
#[cfg(all(unix, not(target_vendor = "apple"), not(target_os = "android")))]
pub fn with_test_ca(dir: &std::path::Path) {
    let file = dir.join("test-ca.pem");
    std::fs::write(&file, TEST_CA).unwrap();
    std::env::set_var("SSL_CERT_FILE", file);
}
