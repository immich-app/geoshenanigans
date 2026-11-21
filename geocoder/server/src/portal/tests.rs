// The test portal's tests (portal.rs), in query-server's binary beside the
// routes a dataset's answers are compared with. The portal syncs from a
// bucket on localhost (test_bucket.rs), whose template build has real
// transport units. Its datasets load through an injected loader: a small
// dataset of each one's own (write_dataset), its country named by the
// dataset's id; the real load of a build's 28 is the box gate's.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use axum::routing::get;
use axum::Router;
use query_server::{
    arc_fixture, cell_id_at_level, cell_pairs, place_files, strings, AdminPolygon, MultiIndex, PlaceNode, PointE7,
    DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL, NO_DATA, STR_TIER_COUNT,
};

use crate::portal::catalog::{self, Object};
use crate::portal::sync::{self, BuildDir, BUILDS, COMPLETE, MANIFEST, REFUSED, SETS};
use crate::portal::{load_dataset, router, Config, Loader, Portal, BUILD_VERSION, LOOKUPS_AT_ONCE};
use crate::downloader::{basename, resolve_files, Configurations, RegionSelection};
use crate::scratch_dir::ScratchDir;
use crate::test_bucket::{self, TestBucket, DATE};
use crate::{auth, handlers};

/// Where the datasets of these tests answer: in their country, at their
/// town.
const POINT: (f64, f64) = (10.5, 20.5);

/// The dates of builds after the bucket's.
const NEXT: &str = "2026-01-02";
const THIRD: &str = "2026-01-03";
const FOURTH: &str = "2026-01-04";

fn string(pool: &mut Vec<u8>, s: &str) -> u32 {
    let at = pool.len() as u32;
    pool.extend_from_slice(s.as_bytes());
    pool.push(0);
    at
}

/// Writes into `dir` a dataset Index::load reads: a country named
/// `country`, the box of lat 10-11 and lng 20-21, listed in the admin cell
/// of POINT, and the town Testville at POINT.
fn write_dataset(dir: &Path, country: &str) {
    let mut pool = Vec::new();
    let (name, town) = (string(&mut pool, country), string(&mut pool, "Testville"));
    let ring: Vec<PointE7> = [(10.0, 20.0), (10.0, 21.0), (11.0, 21.0), (11.0, 20.0), (10.0, 20.0)]
        .into_iter()
        .map(|(lat, lng)| arc_fixture::e7(lat, lng))
        .collect();
    let mut arcs = arc_fixture::ArcFixture::new();
    arcs.ring_polygon(AdminPolygon { country_code: u16::from_le_bytes(*b"tl"), ..arc_fixture::record(name, 2, 1.0) }, 0, &[ring]);
    let cell = cell_id_at_level(POINT.0, POINT.1, DEFAULT_ADMIN_CELL_LEVEL);
    let (cells, entries) = cell_pairs::write(&[(cell, vec![0])], cell_pairs::ADMIN);
    let node = PlaceNode {
        lat: POINT.0 as f32, lng: POINT.1 as f32, name_id: town, place_type: 1, _pad1: 0, _pad2: 0, parent_poly_id: NO_DATA,
    };
    let (places, _) = place_files::encode(&[node], DEFAULT_ADMIN_CELL_LEVEL);
    let tiers: Vec<String> = (0..STR_TIER_COUNT)
        .map(|t| format!("{{\"start\": {}, \"end\": {}}}", if t == 0 { 0 } else { pool.len() }, pool.len()))
        .collect();
    let mut files: Vec<(&str, Vec<u8>)> = arcs.files().into();
    files.extend([
        ("admin_cells.bin", cells),
        ("admin_entries.bin", entries),
        (place_files::CELLS_FILE, places.cells),
        (place_files::NODES_FILE, places.nodes),
        (place_files::HIDDEN_FILE, places.hidden),
        ("strings_layout.json", format!("{{\"tiers\": [{}]}}", tiers.join(", ")).into_bytes()),
        ("strings_core.bin", strings::encode(&pool)),
    ]);
    std::fs::create_dir_all(dir).unwrap();
    for (name, bytes) in files {
        std::fs::write(dir.join(name), bytes).unwrap();
    }
}

/// The datasets an injected loader loads (fixtures).
struct Fixtures {
    dir: ScratchDir,
    /// The dates of the builds whose datasets fail to load.
    failing: Mutex<BTreeSet<String>>,
}

/// Loads each dataset dir (<build>/sets/<id>) as a dataset of its own, its
/// country named by its id; fails for those of the builds `fixtures` names.
fn fixtures() -> (Arc<Fixtures>, Loader) {
    let fixtures = Arc::new(Fixtures { dir: ScratchDir::new(), failing: Mutex::new(BTreeSet::new()) });
    let loaded = fixtures.clone();
    let load: Loader = Arc::new(move |set: &Path| {
        let id = set.file_name().unwrap().to_str().unwrap().to_string();
        let build = set.parent().unwrap().parent().unwrap().file_name().unwrap().to_str().unwrap();
        let date = BuildDir::parse(build).unwrap().date;
        let failing = loaded.failing.lock().unwrap();
        if failing.contains(&date) {
            return Err(format!("it holds no admin_cells.bin of build {date}"));
        }
        let dir = loaded.dir.path().join(&id);
        if !dir.exists() {
            write_dataset(&dir, &id);
        }
        load_dataset(&dir)
    });
    (fixtures, load)
}

fn config(bucket: &TestBucket, load: Loader) -> Config {
    Config {
        base_url: bucket.url.clone(),
        interval: Duration::from_millis(20),
        retry_after: Duration::from_millis(20),
        retry_delays: Vec::new(),
        fetches_at_once: 8,
        lookups_at_once: LOOKUPS_AT_ONCE,
        load,
        free_space: |_| Ok(u64::MAX),
    }
}

fn portal(data: &Path, config: Config) -> Arc<Portal> {
    Arc::new(Portal::new(data.to_path_buf(), config))
}

/// An account database with the key `test`, of no rate limit.
fn db() -> Arc<RwLock<auth::Db>> {
    let db = serde_json::json!({
        "users": { "test": { "password_hash": "", "admin": false, "rate_per_second": 0, "rate_per_day": 0, "rate_by_ip": false } },
        "tokens": { "test": "test" },
    });
    Arc::new(RwLock::new(serde_json::from_value(db).unwrap()))
}

/// Serves `app` on localhost; its base URL.
async fn serve(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap() });
    format!("http://{addr}")
}

struct Got {
    status: u16,
    content_type: Option<String>,
    headers: reqwest::header::HeaderMap,
    body: String,
}

impl Got {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.body).unwrap_or_else(|e| panic!("{e}: {}", self.body))
    }
}

async fn fetch(url: &str) -> Got {
    let resp = reqwest::get(url).await.unwrap();
    let (status, headers) = (resp.status().as_u16(), resp.headers().clone());
    let content_type = headers.get("content-type").map(|v| v.to_str().unwrap().to_string());
    Got { status, content_type, headers, body: resp.text().await.unwrap() }
}

/// Waits (at most a minute) until `done`.
async fn until(what: &str, done: impl Fn() -> bool) {
    for _ in 0..6000 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("waited a minute for {what}");
}

/// The bucket's build's manifest, as the sync reads it.
async fn manifest_of(bucket: &TestBucket) -> (Configurations, catalog::Manifest) {
    let bytes = reqwest::get(format!("{}/builds/{DATE}/{MANIFEST}", bucket.url)).await.unwrap().bytes().await.unwrap();
    sync::parse_manifest(&bytes).unwrap()
}

/// What a sync of the bucket's build fetches: the objects but those it
/// links.
async fn fetched(bucket: &TestBucket) -> Vec<Object> {
    let (cfg, manifest) = manifest_of(bucket).await;
    catalog::plan(&cfg, &manifest).unwrap().objects.into_iter().filter(|o| o.link.is_none()).collect()
}

/// The path the bucket serves `object` at, under its build's date.
fn object_url(object: &Object) -> String {
    if object.unit {
        object.path.clone()
    } else {
        format!("{}.zst", object.path)
    }
}

/// The build dirs under `data`.
fn build_dirs(data: &Path) -> Vec<String> {
    let mut dirs: Vec<String> = std::fs::read_dir(data.join(BUILDS)).unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    dirs.sort();
    dirs
}

fn inode(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).unwrap_or_else(|e| panic!("{}: {e}", path.display())).ino()
}

fn template() -> serde_json::Value {
    serde_json::from_str(include_str!("../../../builder/configurations.template.json")).unwrap()
}

/// The values of an axis of the template, in their order.
fn template_values(axis: &str) -> Vec<String> {
    let t = template();
    let mut values: Vec<(i64, String)> = t["axes"][axis]["values"].as_object().unwrap().iter()
        .map(|(id, v)| (v["order"].as_i64().unwrap(), id.clone()))
        .collect();
    values.sort();
    values.into_iter().map(|(_, id)| id).collect()
}

/// Every planet selection of the template's axes, in their values' order,
/// with whether its constraints allow it.
fn template_selections() -> Vec<(RegionSelection, bool)> {
    let t = template();
    let mut all = Vec::new();
    for mode in template_values("mode") {
        for quality in template_values("quality") {
            for poi_tier in template_values("poi_tier") {
                let s = RegionSelection { region: "planet".into(), mode: mode.clone(), quality: quality.clone(), poi_tier };
                let allowed = t["constraints"].as_array().unwrap().iter().all(|c| {
                    let has = |key: &str| c[key].as_object().unwrap().iter().all(|(axis, value)| s.axis(axis) == value.as_str());
                    !has("when") || has("requires")
                });
                all.push((s, allowed));
            }
        }
    }
    all
}

/// Links every file of the build dir `from` into `to` and gives its
/// manifest the build `date` of build version `version`.
fn copy_build(from: &Path, to: &Path, date: &str, version: u32) {
    fn link_tree(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                link_tree(&entry.path(), &to.join(entry.file_name()));
            } else {
                std::fs::hard_link(entry.path(), to.join(entry.file_name())).unwrap();
            }
        }
    }
    link_tree(from, to);
    let mut manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(to.join(MANIFEST)).unwrap()).unwrap();
    manifest["build"]["date"] = date.into();
    manifest["build"]["version"] = version.into();
    std::fs::remove_file(to.join(MANIFEST)).unwrap();
    std::fs::write(to.join(MANIFEST), manifest.to_string()).unwrap();
}

// The image reads builds of the template's version, and syncs only those.
#[test]
fn the_build_version_it_serves_is_the_templates() {
    assert_eq!(u64::from(BUILD_VERSION), template()["build"]["version"].as_u64().unwrap());
}

// A dataset's answers are those of a query-server serving its files: the
// same status, content type and bytes from /reverse (each debug= among
// them) and /polygons, for points in and out of the dataset, off the globe,
// and without a key or a good one, with dataset= or without.
#[tokio::test]
async fn a_dataset_answers_byte_for_byte_what_query_server_answers_from_its_files() {
    let files = ScratchDir::new();
    let dir = files.path().join("dataset");
    write_dataset(&dir, "Testland");
    let index = Arc::new(MultiIndex::load(dir.to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).unwrap());
    let region_state = Arc::new(crate::region_state::RegionState::new(index.clone(), dir.clone()));
    let query_server = Router::new()
        .route("/reverse", get(crate::reverse_geocode))
        .route("/polygons", get(crate::polygons_geojson))
        .layer(axum::Extension(index))
        .layer(axum::Extension(region_state))
        .layer(axum::Extension(Arc::new(auth::RateLimiter::default())))
        .layer(axum::middleware::from_fn(handlers::cors_middleware))
        .with_state(db());
    // A portal whose every dataset is that dir.
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let load: Loader = Arc::new(move |_: &Path| load_dataset(&dir));
    let p = portal(data.path(), config(&bucket, load));
    p.round(true).await.unwrap();
    let (ours, theirs) = (serve(router(p, db())).await, serve(query_server).await);

    let mut queries = Vec::new();
    for at in ["lat=10.5&lon=20.5", "lat=10.501&lon=20.499", "lat=10.95&lon=20.95", "lat=40&lon=40"] {
        for debug in ["", "&debug=all", "&debug=primary", "&debug=places", "&debug=admin", "&debug=other"] {
            queries.push(format!("{at}&key=test{debug}"));
        }
    }
    queries.extend(["lat=95&lon=0&key=test", "lat=10.5&lon=20.5", "lat=10.5&lon=20.5&key=nope", "lon=20.5&key=test", "lat=north&lon=20.5&key=test"].map(String::from));
    let mut found = 0;
    for route in ["reverse", "polygons"] {
        for q in &queries {
            let want = fetch(&format!("{theirs}/{route}?{q}")).await;
            found += usize::from(want.body.contains("Testland"));
            for dataset in ["", "&dataset=with-addresses", "&dataset=full.simplified.all"] {
                let got = fetch(&format!("{ours}/{route}?{q}{dataset}")).await;
                assert_eq!((got.status, &got.content_type, &got.body), (want.status, &want.content_type, &want.body), "/{route}?{q}{dataset}");
            }
        }
    }
    assert!(found >= 6, "{found} answers name the country");
}

// The objects of a build are each one the selections the portal serves take,
// once: every file of every planet selection the constraints allow comes
// through one, each transport unit once. The sync fetches each object once,
// and those whose every file the build holds byte for byte elsewhere not at
// all: their files are hard links of those. Each dataset's dir links its
// files by name.
#[tokio::test]
async fn a_builds_objects_cover_every_selection_it_serves_and_its_sync_fetches_each_once() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let (cfg, manifest) = manifest_of(&bucket).await;
    let plan = catalog::plan(&cfg, &manifest).unwrap();
    let held: BTreeMap<&str, &Object> = plan.objects.iter().flat_map(|o| o.files.iter().map(move |f| (f.as_str(), o))).collect();
    let (mut units, mut taken) = (BTreeSet::new(), BTreeSet::new());
    let allowed: Vec<RegionSelection> = template_selections().into_iter().filter(|(_, allowed)| *allowed).map(|(s, _)| s).collect();
    assert_eq!(allowed.len(), 28);
    for s in &allowed {
        let resolved = resolve_files(&cfg, s).unwrap();
        let files: Vec<&String> = resolved.paths.iter().filter(|p| cfg.files.contains_key(*p)).collect();
        for f in &files {
            assert!(held.contains_key(f.as_str()), "{s:?}: {f}");
            units.extend(cfg.files[*f].unit.clone());
            taken.insert(f.to_string());
        }
        let dataset = plan.datasets.iter().find(|d| d.id == catalog::dataset_id(s)).unwrap_or_else(|| panic!("{s:?}"));
        assert_eq!(dataset.files.iter().collect::<Vec<_>>(), files, "{s:?}");
    }
    assert_eq!(plan.datasets.len(), allowed.len());
    assert_eq!(held.keys().map(|f| f.to_string()).collect::<BTreeSet<_>>(), taken);
    let unit_objects: Vec<&String> = plan.objects.iter().filter(|o| o.unit).map(|o| &o.path).collect();
    assert_eq!(unit_objects, units.iter().collect::<Vec<_>>());
    assert!(!units.is_empty() && plan.objects.iter().any(|o| o.link.is_some()));

    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    portal(data.path(), config(&bucket, load)).round(true).await.unwrap();

    let mut requests = bucket.requests();
    requests.sort();
    let mut want: Vec<String> = plan.objects.iter().filter(|o| o.link.is_none()).map(|o| format!("{DATE}/{}", object_url(o))).collect();
    want.sort();
    assert_eq!(requests, want);
    let root = data.path().join(BUILDS).join(format!("{DATE}-1"));
    for o in &plan.objects {
        for (i, f) in o.files.iter().enumerate() {
            assert!(std::fs::read(root.join(f)).unwrap() == bucket.file(f), "{f}");
            if let Some(from) = &o.link {
                assert_eq!(inode(&root.join(f)), inode(&root.join(&from[i])), "{f}");
            }
        }
    }
    for d in &plan.datasets {
        let links = d.links();
        let mut names: Vec<String> = std::fs::read_dir(root.join(SETS).join(&d.id)).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect();
        names.sort();
        assert_eq!(names, links.keys().cloned().collect::<Vec<_>>(), "{}", d.id);
        for (name, path) in links {
            assert_eq!(inode(&root.join(SETS).join(&d.id).join(&name)), inode(&root.join(&path)), "{} {name}", d.id);
        }
    }
}

// A build of another build version is refused, whether latest.json says so
// or only its manifest does: nothing of it is fetched, the build served
// stays, and the refusal is on record, with why, as long as latest.json
// names that build.
#[tokio::test]
async fn a_build_of_another_version_is_never_switched_to_and_is_recorded() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let p = portal(data.path(), config(&bucket, load));
    p.round(true).await.unwrap();
    let served = bucket.requests().len();
    let refused = || -> Vec<(String, u32, String)> {
        let records: Vec<sync::Refusal> = serde_json::from_slice(&std::fs::read(data.path().join(REFUSED)).unwrap()).unwrap();
        records.into_iter().map(|r| (r.date, r.version, r.reason)).collect()
    };
    let why = format!("it is of build version {}; this server reads build version {BUILD_VERSION}", BUILD_VERSION + 1);

    bucket.add_build(NEXT, BUILD_VERSION + 1);
    bucket.set_latest(NEXT, Some(BUILD_VERSION + 1));
    assert_eq!(p.round(false).await, Ok(format!("refused build {NEXT}: {why}")));
    assert_eq!(refused(), [(NEXT.to_string(), BUILD_VERSION, why.clone())]);

    bucket.add_build(THIRD, BUILD_VERSION + 1);
    bucket.set_latest(THIRD, Some(BUILD_VERSION));
    assert_eq!(p.round(false).await, Ok(format!("refused build {THIRD}: {why}")));
    assert_eq!(refused(), [(THIRD.to_string(), BUILD_VERSION, why.clone())]);
    assert_eq!(p.round(false).await, Ok(format!("build {THIRD} was refused: {why}")));

    assert_eq!(p.served().unwrap().dir, format!("{DATE}-1"));
    assert_eq!(p.status().refused.iter().map(|r| r.date.as_str()).collect::<Vec<_>>(), [THIRD]);
    assert_eq!(bucket.requests().len(), served);
    assert_eq!(build_dirs(data.path()), [format!("{DATE}-1")]);
}

// A build whose datasets don't all load is refused: the build served stays,
// the refused one's dir goes, and the refusal is on record, so that neither
// this server nor one started afresh on the data dir fetches it again.
#[tokio::test]
async fn a_build_that_doesnt_load_is_never_switched_to_is_recorded_and_not_fetched_again() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (fixtures, load) = fixtures();
    fixtures.failing.lock().unwrap().insert(NEXT.to_string());
    let p = portal(data.path(), config(&bucket, load.clone()));
    p.round(true).await.unwrap();

    bucket.add_build(NEXT, BUILD_VERSION);
    bucket.set_latest(NEXT, Some(BUILD_VERSION));
    let done = p.round(false).await.unwrap();
    let why = format!("it doesn't load: dataset {}: it holds no admin_cells.bin of build {NEXT}", template_ids()[0]);
    assert_eq!(done, format!("refused build {NEXT}: {why}"));
    assert_eq!(p.served().unwrap().dir, format!("{DATE}-1"));
    assert_eq!(build_dirs(data.path()), [format!("{DATE}-1")]);
    let fetched = bucket.requests().len();

    assert_eq!(p.round(false).await, Ok(format!("build {NEXT} was refused: {why}")));
    let again = portal(data.path(), config(&bucket, load));
    assert_eq!(again.round(true).await, Ok(format!("build {NEXT} was refused: {why}")));
    assert_eq!(again.served().unwrap().dir, format!("{DATE}-1"));
    assert_eq!(bucket.requests().len(), fetched);
}

/// The ids of the template's planet datasets, in order.
fn template_ids() -> Vec<String> {
    template_selections().into_iter().filter(|(_, allowed)| *allowed).map(|(s, _)| catalog::dataset_id(&s)).collect()
}

/// Gives the manifest `m` the file `path`, as its own .zst, which mode
/// full's component names as `listed`.
fn add_file(m: &mut serde_json::Value, listed: &str, path: &str) {
    m["components"]["mode.full"]["files"].as_array_mut().unwrap().push(listed.into());
    m["files"][path] = serde_json::json!({ "size_zst": 10, "size_raw": 10, "sha256": "00" });
}

/// The plan of the bucket's build with its manifest as `edit` changes it.
async fn plan_edited(bucket: &TestBucket, edit: impl FnOnce(&mut serde_json::Value)) -> Result<catalog::Plan, String> {
    let bytes = reqwest::get(format!("{}/builds/{DATE}/{MANIFEST}", bucket.url)).await.unwrap().bytes().await.unwrap();
    let mut m: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    edit(&mut m);
    let (cfg, manifest) = sync::parse_manifest(m.to_string().as_bytes()).unwrap();
    catalog::plan(&cfg, &manifest)
}

// The sync writes only in a build dir, at names of the build's own: a
// manifest whose datasets take a file, a unit or a member whose path is not
// the region's dir and then plain names (none absolute, empty, '.', '..',
// hidden as the sync's and the decoder's own names are, a temporary file's
// or of other characters), or whose axis value or dataset id is not one
// plain name, can't be planned, and says why.
#[tokio::test]
async fn a_manifest_naming_a_path_or_a_name_the_sync_cant_keep_in_its_build_dir_cant_be_planned() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let plan = plan_edited(&bucket, |_| {}).await.unwrap();
    let full = template_ids().into_iter().find(|id| id.starts_with("full.")).unwrap();
    let plain = "which is not a plain name (letters, digits, '.', '-' and '_', starting with none of '.' and not ending in .tmp)";

    for (listed, path, why) in [
        ("{region}/../escape.bin", "planet/../escape.bin", format!("has the part \"..\", {plain}")),
        ("{region}/./escape.bin", "planet/./escape.bin", format!("has the part \".\", {plain}")),
        ("{region}//escape.bin", "planet//escape.bin", format!("has the part \"\", {plain}")),
        ("/{region}/escape.bin", "/planet/escape.bin", "is not under planet/".to_string()),
        ("europe/escape.bin", "europe/escape.bin", "is not under planet/".to_string()),
        ("configurations.json", "configurations.json", "is not under planet/".to_string()),
        ("{region}/full/.fetched", "planet/full/.fetched", format!("has the part \".fetched\", {plain}")),
        ("{region}/full/.transport.1.1", "planet/full/.transport.1.1", format!("has the part \".transport.1.1\", {plain}")),
        ("{region}/full/escape.bin.tmp", "planet/full/escape.bin.tmp", format!("has the part \"escape.bin.tmp\", {plain}")),
        ("{region}/full/line\nbreak.bin", "planet/full/line\nbreak.bin", format!("has the part \"line\\nbreak.bin\", {plain}")),
    ] {
        let got = plan_edited(&bucket, |m| add_file(m, listed, path)).await.map(|_| ());
        assert_eq!(got, Err(format!("{full}: the path {path:?} {why}")), "{path:?}");
    }

    // A unit of the first dataset, under a hidden name.
    let first = &plan.datasets[0];
    let unit = plan.objects.iter().find(|o| o.unit && first.files.contains(&o.files[0])).unwrap();
    let hidden = unit.path.rsplit_once('/').map(|(dir, name)| format!("{dir}/.{name}")).unwrap();
    let got = plan_edited(&bucket, |m| {
        let entry = m["units"].as_object_mut().unwrap().remove(&unit.path).unwrap();
        for f in &unit.files {
            m["files"][f]["unit"] = hidden.clone().into();
        }
        m["units"][&hidden] = entry;
    })
    .await
    .map(|_| ());
    let part = hidden.rsplit('/').next().unwrap();
    assert_eq!(got, Err(format!("{}: the path {hidden:?} has the part {part:?}, {plain}", first.id)));

    for (axis, value) in [("mode", "../up"), ("mode", "a/b"), ("quality", "a.b"), ("poi_tier", "")] {
        let got = plan_edited(&bucket, |m| m["axes"][axis]["values"][value] = serde_json::json!({ "label": "x", "order": 99 })).await.map(|_| ());
        assert_eq!(got, Err(format!("the manifest's {axis} value {value:?} is not a plain name (letters, digits, '-' and '_')")), "{axis} {value:?}");
    }
    let got = plan_edited(&bucket, |m| m["axes"]["poi_tier"]["values"]["tmp"] = serde_json::json!({ "label": "x", "order": 99 })).await.map(|_| ());
    let id = format!("{}.{}.tmp", template_values("mode")[0], template_values("quality")[0]);
    assert_eq!(got, Err(format!("the dataset id {id:?} is not a plain name")));
}

// A build whose manifest names a path out of its build dir is refused,
// with why: nothing of it is fetched or written, and the build served
// stays.
#[tokio::test]
async fn a_build_naming_a_path_out_of_its_dir_is_refused_and_nothing_of_it_is_written() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let p = portal(data.path(), config(&bucket, load));
    p.round(true).await.unwrap();
    let fetched = bucket.requests().len();
    let path = "planet/../../../escape.bin";
    bucket.add_build(NEXT, BUILD_VERSION);
    bucket.edit_build(NEXT, |m| add_file(m, "{region}/../../../escape.bin", path));
    bucket.set_latest(NEXT, Some(BUILD_VERSION));

    let done = p.round(false).await.unwrap();

    let full = template_ids().into_iter().find(|id| id.starts_with("full.")).unwrap();
    let why = format!("{full}: the path {path:?} has the part \"..\", which is not a plain name");
    assert!(done.starts_with(&format!("refused build {NEXT}: {why}")), "{done}");
    assert_eq!(p.status().refused.len(), 1);
    assert!(p.status().refused[0].reason.starts_with(&why), "{:?}", p.status().refused);
    assert_eq!(p.served().unwrap().dir, format!("{DATE}-1"));
    assert_eq!(bucket.requests().len(), fetched);
    assert_eq!(build_dirs(data.path()), [format!("{DATE}-1")]);
    assert!(!data.path().join("escape.bin").exists());
}

// A portal with no build it can serve starts all the same: /health,
// /datasets, /reverse and /polygons answer 503 with the reason, while its
// rounds go on; once the bucket has a build of its version, it serves it.
#[tokio::test]
async fn a_start_without_a_usable_build_answers_503_with_the_reason_and_keeps_syncing() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    bucket.add_build(NEXT, BUILD_VERSION + 1);
    bucket.set_latest(NEXT, Some(BUILD_VERSION + 1));
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let p = portal(data.path(), config(&bucket, load));
    tokio::spawn(p.clone().run());
    let url = serve(router(p.clone(), db())).await;
    until("a round", || p.status().last_result.is_some()).await;

    let health = fetch(&format!("{url}/health")).await;
    assert_eq!(health.status, 503);
    let reason = health.json()["reason"].as_str().unwrap().to_string();
    let waiting = format!("no build to serve: waiting for a build of version {BUILD_VERSION}");
    assert!(reason.starts_with(&waiting), "{reason}");
    assert!(reason.contains(&format!("refused build {NEXT}: it is of build version {}", BUILD_VERSION + 1)), "{reason}");
    assert_eq!(health.json()["sync"]["latest"]["date"], NEXT);
    for route in ["datasets", "reverse?lat=10.5&lon=20.5&key=test", "polygons?lat=10.5&lon=20.5&key=test"] {
        let got = fetch(&format!("{url}/{route}")).await;
        assert_eq!(got.status, 503, "{route}");
        assert!(got.body.starts_with(&waiting), "{route}: {}", got.body);
    }

    bucket.set_latest(DATE, Some(BUILD_VERSION));
    until("the build served", || p.served().is_some()).await;
    let health = fetch(&format!("{url}/health")).await;
    assert_eq!(health.status, 200);
    assert_eq!(health.json()["build"]["dir"], format!("{DATE}-1"));
    assert_eq!(health.json()["datasets"], 28);
    assert_eq!(fetch(&format!("{url}/reverse?lat=10.5&lon=20.5&key=test")).await.status, 200);
}

// A portal started on a volume whose only build it can't serve says which
// build it skipped and why in the reason it answers 503 with.
#[tokio::test]
async fn a_start_on_a_build_it_cant_serve_says_why_in_its_503() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let made = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    portal(made.path(), config(&bucket, load.clone())).round(true).await.unwrap();
    let data = ScratchDir::new();
    let older = BUILD_VERSION - 1;
    copy_build(&made.path().join(BUILDS).join(format!("{DATE}-1")), &data.path().join(BUILDS).join(format!("{DATE}-1")), DATE, older);
    bucket.add_build(NEXT, BUILD_VERSION + 1);
    bucket.set_latest(NEXT, Some(BUILD_VERSION + 1));
    let p = portal(data.path(), config(&bucket, load));
    tokio::spawn(p.clone().run());
    let url = serve(router(p.clone(), db())).await;
    until("a round", || p.status().last_result.is_some()).await;

    let health = fetch(&format!("{url}/health")).await;
    assert_eq!(health.status, 503);
    let reason = health.json()["reason"].as_str().unwrap().to_string();
    let skipped = format!("build {DATE}-1 here can't be served: it is of build version {older}; this server reads build version {BUILD_VERSION}");
    assert!(reason.contains(&skipped), "{reason}");
    assert!(fetch(&format!("{url}/datasets")).await.body.contains(&skipped));
}

// At start a build's files are checked before it is served: one whose
// files don't hold their manifest's bytes is never loaded, so never served,
// and the next older one is, once its own check out; with none that does,
// none is served, and the 503 says why.
#[tokio::test]
async fn a_start_serves_no_build_before_its_files_check_out() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    portal(data.path(), config(&bucket, load.clone())).round(true).await.unwrap();
    let builds = data.path().join(BUILDS);
    copy_build(&builds.join(format!("{DATE}-1")), &builds.join(format!("{NEXT}-1")), NEXT, BUILD_VERSION);
    let damaged = "planet/full/admin_cells.bin";
    // A byte of the file flipped, in a copy of its own (the build's dirs
    // share it by hard links).
    let damage = |dir: &str| {
        let at = builds.join(dir).join(damaged);
        let mut bytes = std::fs::read(&at).unwrap();
        bytes[0] ^= 1;
        std::fs::remove_file(&at).unwrap();
        std::fs::write(&at, &bytes).unwrap();
    };
    damage(&format!("{NEXT}-1"));
    // No round gets past the start: the latest build stays unknown.
    std::fs::write(data.path().join(sync::LATEST_OVERRIDE), "tomorrow\n").unwrap();
    let loaded = Arc::new(Mutex::new(BTreeSet::new()));
    let recording: Loader = {
        let (loaded, load) = (loaded.clone(), load.clone());
        Arc::new(move |set: &Path| {
            loaded.lock().unwrap().insert(set.parent().unwrap().parent().unwrap().file_name().unwrap().to_str().unwrap().to_string());
            load(set)
        })
    };

    let p = portal(data.path(), config(&bucket, recording.clone()));
    assert!(p.round(true).await.is_err());

    assert_eq!(p.served().unwrap().dir, format!("{DATE}-1"));
    assert_eq!(*loaded.lock().unwrap(), BTreeSet::from([format!("{DATE}-1")]));
    assert_eq!(build_dirs(data.path()), [format!("{DATE}-1")]);

    damage(&format!("{DATE}-1"));
    let none = portal(data.path(), config(&bucket, recording));
    assert!(none.round(true).await.is_err());
    assert!(none.served().is_none());
    let skipped = format!("build {DATE}-1 here can't be served: {damaged} holds bytes of sha256 ");
    assert!(none.unavailable().contains(&skipped), "{}", none.unavailable());
}

// A sync killed part way leaves its dir incomplete, its ledger naming the
// objects in place; the next, of a server started afresh, resumes in that
// dir and fetches only the others.
#[tokio::test]
async fn a_sync_killed_part_way_resumes_in_its_dir_fetching_only_what_had_not_come_down() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let mut one_at_a_time = config(&bucket, load.clone());
    one_at_a_time.fetches_at_once = 1;
    let p = portal(data.path(), one_at_a_time);
    let mut order = fetched(&bucket).await;
    order.sort_by(|a, b| b.size.cmp(&a.size).then_with(|| a.path.cmp(&b.path)));
    let holding = bucket.hold(&object_url(&order[3]));

    let round = tokio::spawn({
        let p = p.clone();
        async move { p.round(true).await }
    });
    holding.reached().await;
    round.abort();
    assert!(round.await.unwrap_err().is_cancelled());
    // The decode the kill left on its thread sees its stream end.
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(holding);
    let root = data.path().join(BUILDS).join(format!("{DATE}-1"));
    assert!(!root.join(COMPLETE).exists());
    assert_eq!(sync::ledger(&root), order[..3].iter().map(|o| o.path.clone()).collect::<BTreeSet<_>>());
    let before = bucket.requests().len();

    let again = portal(data.path(), config(&bucket, load));
    assert_eq!(again.round(true).await, Ok(format!("build {DATE} is served ({DATE}-1)")));

    let mut resumed = bucket.requests()[before..].to_vec();
    resumed.sort();
    let mut want: Vec<String> = order[3..].iter().map(|o| format!("{DATE}/{}", object_url(o))).collect();
    want.sort();
    assert_eq!(resumed, want);
    let (cfg, _) = manifest_of(&bucket).await;
    for f in cfg.files.keys().filter(|f| f.starts_with("planet/")) {
        assert!(std::fs::read(root.join(f)).unwrap() == bucket.file(f), "{f}");
    }
}

// A sync killed part way, then a start that can't learn the latest build
// (the bucket out of reach, or a latest.override of no date): its round
// fails before it knows which build to resume, and keeps the dir the kill
// left, which the next round resumes, fetching only what had not come down.
#[tokio::test]
async fn a_round_that_cant_learn_the_latest_build_keeps_the_sync_left_part_way() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let mut one_at_a_time = config(&bucket, load.clone());
    one_at_a_time.fetches_at_once = 1;
    let p = portal(data.path(), one_at_a_time);
    let mut order = fetched(&bucket).await;
    order.sort_by(|a, b| b.size.cmp(&a.size).then_with(|| a.path.cmp(&b.path)));
    let holding = bucket.hold(&object_url(&order[3]));
    let round = tokio::spawn({
        let p = p.clone();
        async move { p.round(true).await }
    });
    holding.reached().await;
    round.abort();
    assert!(round.await.unwrap_err().is_cancelled());
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(holding);
    let root = data.path().join(BUILDS).join(format!("{DATE}-1"));
    let kept = sync::ledger(&root);
    assert_eq!(kept.len(), 3);

    std::fs::write(data.path().join(sync::LATEST_OVERRIDE), "tomorrow\n").unwrap();
    let failed = portal(data.path(), config(&bucket, load.clone())).round(true).await.unwrap_err();
    assert_eq!(failed, format!("{} holds \"tomorrow\", no date", sync::LATEST_OVERRIDE));
    assert_eq!(build_dirs(data.path()), [format!("{DATE}-1")]);
    assert_eq!(sync::ledger(&root), kept);

    std::fs::remove_file(data.path().join(sync::LATEST_OVERRIDE)).unwrap();
    let before = bucket.requests().len();
    assert_eq!(portal(data.path(), config(&bucket, load)).round(true).await, Ok(format!("build {DATE} is served ({DATE}-1)")));
    assert_eq!(bucket.requests().len() - before, order.len() - 3);
}

/// What fetches left in the build dir `root`: a download's temporary files
/// and decodes' dirs, by path.
fn leftovers(root: &Path) -> Vec<String> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            if name.ends_with(".tmp") || name.starts_with(".transport.") {
                out.push(entry.path().strip_prefix(root).unwrap().display().to_string());
            } else if entry.file_type().unwrap().is_dir() {
                walk(root, &entry.path(), out);
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out
}

// A round whose ledger can't take an object's line starts no other fetch,
// and ends only once the fetches it started have: none goes on writing in
// its dir once the round, and its lock, are over.
#[tokio::test]
async fn a_round_whose_ledger_fails_ends_only_once_the_fetches_it_started_have() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let mut two_at_a_time = config(&bucket, load);
    two_at_a_time.fetches_at_once = 2;
    let p = portal(data.path(), two_at_a_time);
    let mut order = fetched(&bucket).await;
    order.sort_by(|a, b| b.size.cmp(&a.size).then_with(|| a.path.cmp(&b.path)));
    // A dir of the build to resume whose ledger is a dir: no line goes in.
    let root = data.path().join(BUILDS).join(format!("{DATE}-1"));
    std::fs::create_dir_all(root.join(sync::FETCHED)).unwrap();
    let manifest = reqwest::get(format!("{}/builds/{DATE}/{MANIFEST}", bucket.url)).await.unwrap().bytes().await.unwrap();
    std::fs::write(root.join(MANIFEST), &manifest).unwrap();
    let holding = bucket.hold(&object_url(&order[0]));

    let round = tokio::spawn({
        let p = p.clone();
        async move { p.round(true).await }
    });
    holding.reached().await;
    let second = format!("{DATE}/{}", object_url(&order[1]));
    until("the second object's fetch", || bucket.requests().contains(&second)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!round.is_finished());
    drop(holding);

    let failed = round.await.unwrap().unwrap_err();
    assert!(failed.starts_with(&format!("write {}: ", root.join(sync::FETCHED).display())), "{failed}");
    let mut requests = bucket.requests();
    requests.sort();
    let mut want: Vec<String> = order[..2].iter().map(|o| format!("{DATE}/{}", object_url(o))).collect();
    want.sort();
    assert_eq!(requests, want);
    for f in order[..2].iter().flat_map(|o| &o.files) {
        assert!(std::fs::read(root.join(f)).unwrap() == bucket.file(f), "{f}");
    }
    assert_eq!(leftovers(&root), Vec::<String>::new());
}

// Two tries at one file at once (one a round left running, one of the
// next) each write a file of their own, and each puts it in place.
#[tokio::test]
async fn two_tries_at_one_file_at_once_each_write_a_file_of_their_own() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let root = ScratchDir::new();
    let object = fetched(&bucket).await.into_iter().find(|o| !o.unit).unwrap();
    let dir = root.path().join(object.path.rsplit_once('/').unwrap().0);
    let client = reqwest::Client::new();
    let temporary = || std::fs::read_dir(&dir).map_or(0, |d| d.filter(|e| e.as_ref().unwrap().file_name().to_str().unwrap().ends_with(".tmp")).count());
    let holding = bucket.hold(&object_url(&object));

    let first = tokio::spawn({
        let (client, url, cfg, object, root) = (client.clone(), bucket.url.clone(), bucket.manifest.clone(), object.clone(), root.path().to_path_buf());
        async move { sync::try_object(&client, &url, DATE, &cfg, &object, &root).await }
    });
    holding.reached().await;
    until("the first try's file", || temporary() == 1).await;
    sync::try_object(&client, &bucket.url, DATE, &bucket.manifest, &object, root.path()).await.unwrap();
    drop(holding);

    first.await.unwrap().unwrap();
    assert!(std::fs::read(root.path().join(&object.path)).unwrap() == bucket.file(&object.path));
    assert_eq!(temporary(), 0);
}

// A build the volume holds whose files no longer hold their manifest's
// bytes is not served at start: it is fetched again, into a new dir, and
// served from there; the damaged one goes.
#[tokio::test]
async fn a_damaged_build_is_fetched_again_into_a_new_dir() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    portal(data.path(), config(&bucket, load.clone())).round(true).await.unwrap();
    let damaged = "planet/full/admin_cells.bin";
    let at = data.path().join(BUILDS).join(format!("{DATE}-1")).join(damaged);
    let mut bytes = std::fs::read(&at).unwrap();
    bytes[0] ^= 1;
    {
        use std::io::Write;
        std::fs::OpenOptions::new().write(true).open(&at).unwrap().write_all(&bytes).unwrap();
    }
    let before = bucket.requests().len();

    let p = portal(data.path(), config(&bucket, load));
    assert_eq!(p.round(true).await, Ok(format!("build {DATE} is served ({DATE}-2)")));

    assert_eq!(build_dirs(data.path()), [format!("{DATE}-2")]);
    assert_eq!(bucket.requests().len() - before, fetched(&bucket).await.len());
    assert!(std::fs::read(data.path().join(BUILDS).join(format!("{DATE}-2")).join(damaged)).unwrap() == bucket.file(damaged));
}

// The prune keeps the build served and the complete one before it, never a
// newer one of another build version it can't serve; after the next day's
// build is in, the build served before is the one before.
#[tokio::test]
async fn the_prune_keeps_the_build_served_and_the_one_before_it() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    portal(data.path(), config(&bucket, load.clone())).round(true).await.unwrap();
    let builds = data.path().join(BUILDS);
    let first = builds.join(format!("{DATE}-1"));
    for (date, version) in [("2025-12-31", BUILD_VERSION), (NEXT, BUILD_VERSION + 1), (THIRD, BUILD_VERSION + 1)] {
        copy_build(&first, &builds.join(format!("{date}-1")), date, version);
    }

    let p = portal(data.path(), config(&bucket, load));
    assert_eq!(p.round(true).await, Ok(format!("build {DATE} is served ({DATE}-1)")));
    assert_eq!(build_dirs(data.path()), ["2025-12-31-1".to_string(), format!("{DATE}-1")]);

    bucket.add_build(FOURTH, BUILD_VERSION);
    bucket.set_latest(FOURTH, Some(BUILD_VERSION));
    assert_eq!(p.round(false).await, Ok(format!("build {FOURTH} is served ({FOURTH}-1)")));
    assert_eq!(build_dirs(data.path()), [format!("{DATE}-1"), format!("{FOURTH}-1")]);
}

// A build the volume holds complete is served again from its dir once
// latest.json names it again (moved back to it), nothing fetched; the prune
// keeps the build served before it, of another date, and never a second
// dir of one build.
#[tokio::test]
async fn a_build_the_volume_holds_is_served_again_without_a_fetch() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let p = portal(data.path(), config(&bucket, load));
    p.round(true).await.unwrap();
    bucket.add_build(NEXT, BUILD_VERSION);
    bucket.set_latest(NEXT, Some(BUILD_VERSION));
    assert_eq!(p.round(false).await, Ok(format!("build {NEXT} is served ({NEXT}-1)")));
    let fetched = bucket.requests().len();

    bucket.set_latest(DATE, Some(BUILD_VERSION));
    assert_eq!(p.round(false).await, Ok(format!("build {DATE} is served ({DATE}-1)")));
    assert_eq!(build_dirs(data.path()), [format!("{DATE}-1"), format!("{NEXT}-1")]);
    bucket.set_latest(NEXT, Some(BUILD_VERSION));
    assert_eq!(p.round(false).await, Ok(format!("build {NEXT} is served ({NEXT}-1)")));
    assert_eq!(build_dirs(data.path()), [format!("{DATE}-1"), format!("{NEXT}-1")]);
    assert_eq!(bucket.requests().len(), fetched);
}

// A build published again under its date (a rerun of the nightly) is
// another build: the sync fetches it into a new dir and serves it, though
// the build served is of that date, and the dir of the build it replaces
// goes.
#[tokio::test]
async fn a_build_published_again_under_its_date_is_fetched_into_a_new_dir_and_served() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let p = portal(data.path(), config(&bucket, load));
    p.round(true).await.unwrap();
    let before = bucket.requests().len();

    bucket.edit_build(DATE, |m| m["presets"][2]["label"] = "With street addresses, corrected".into());

    assert_eq!(p.round(false).await, Ok(format!("build {DATE} is served ({DATE}-2)")));
    assert_eq!(p.served().unwrap().plan.datasets[p.served().unwrap().plan.default].label, "With street addresses, corrected");
    assert_eq!(bucket.requests().len() - before, fetched(&bucket).await.len());
    assert_eq!(build_dirs(data.path()), [format!("{DATE}-2")]);
    assert_eq!(p.round(false).await, Ok(format!("build {DATE} is served ({DATE}-2)")));
    assert_eq!(bucket.requests().len() - before, fetched(&bucket).await.len());
}

// A refusal is of the build as it was published: once the build of its
// date is published again, corrected, the sync fetches and serves it, and
// so does a portal started afresh on the volume.
#[tokio::test]
async fn a_refusal_doesnt_hold_against_the_build_published_again_under_its_date() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let p = portal(data.path(), config(&bucket, load.clone()));
    p.round(true).await.unwrap();
    bucket.add_build(NEXT, BUILD_VERSION + 1);
    bucket.set_latest(NEXT, Some(BUILD_VERSION));
    let why = format!("it is of build version {}; this server reads build version {BUILD_VERSION}", BUILD_VERSION + 1);
    assert_eq!(p.round(false).await, Ok(format!("refused build {NEXT}: {why}")));
    assert_eq!(p.round(false).await, Ok(format!("build {NEXT} was refused: {why}")));

    bucket.add_build(NEXT, BUILD_VERSION);

    assert_eq!(p.round(false).await, Ok(format!("build {NEXT} is served ({NEXT}-1)")));
    assert!(p.status().refused.is_empty(), "{:?}", p.status().refused);
    let again = portal(data.path(), config(&bucket, load));
    assert_eq!(again.round(true).await, Ok(format!("build {NEXT} is served ({NEXT}-1)")));
}

// A sync left part way of a build since published again under its date is
// not resumed: the build as published now goes into a new dir, and the dir
// left part way goes.
#[tokio::test]
async fn a_sync_left_part_way_of_a_build_published_again_since_is_not_resumed() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let mut one_at_a_time = config(&bucket, load.clone());
    one_at_a_time.fetches_at_once = 1;
    let p = portal(data.path(), one_at_a_time);
    let mut order = fetched(&bucket).await;
    order.sort_by(|a, b| b.size.cmp(&a.size).then_with(|| a.path.cmp(&b.path)));
    let holding = bucket.hold(&object_url(&order[3]));
    let round = tokio::spawn({
        let p = p.clone();
        async move { p.round(true).await }
    });
    holding.reached().await;
    round.abort();
    assert!(round.await.unwrap_err().is_cancelled());
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(holding);

    bucket.edit_build(DATE, |m| m["presets"][2]["label"] = "With street addresses, corrected".into());
    let before = bucket.requests().len();

    assert_eq!(portal(data.path(), config(&bucket, load)).round(true).await, Ok(format!("build {DATE} is served ({DATE}-2)")));
    assert_eq!(bucket.requests().len() - before, order.len());
    assert_eq!(build_dirs(data.path()), [format!("{DATE}-2")]);
}

// A build the volume holds whose files no longer hold their manifest's
// bytes is not served again: it is fetched anew into a new dir, and the
// damaged one goes.
#[tokio::test]
async fn a_build_the_volume_holds_damaged_is_fetched_again_before_it_is_served_again() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let p = portal(data.path(), config(&bucket, load));
    p.round(true).await.unwrap();
    bucket.add_build(NEXT, BUILD_VERSION);
    bucket.set_latest(NEXT, Some(BUILD_VERSION));
    p.round(false).await.unwrap();
    let at = data.path().join(BUILDS).join(format!("{DATE}-1")).join("planet/full/admin_cells.bin");
    let mut bytes = std::fs::read(&at).unwrap();
    bytes[0] ^= 1;
    {
        use std::io::Write;
        std::fs::OpenOptions::new().write(true).open(&at).unwrap().write_all(&bytes).unwrap();
    }
    let before = bucket.requests().len();

    bucket.set_latest(DATE, Some(BUILD_VERSION));
    assert_eq!(p.round(false).await, Ok(format!("build {DATE} is served ({DATE}-2)")));
    assert_eq!(bucket.requests().len() - before, fetched(&bucket).await.len());
    assert_eq!(build_dirs(data.path()), [format!("{DATE}-2"), format!("{NEXT}-1")]);
}

// /datasets lists the 28 planet selections the constraints allow, in the
// axes' order, each with its label, description, preset, values and sizes
// (the bytes of its dir, which holds one file of each name as a client's
// download does, and the manifest's sum of what a client downloads), the
// presets with their labels and descriptions, the axes and their values
// with theirs, and the 4 combinations the constraint excludes, each with
// the constraint's message and what it requires.
#[tokio::test]
async fn datasets_lists_the_28_offered_and_the_combinations_the_constraints_exclude() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let p = portal(data.path(), config(&bucket, load));
    p.round(true).await.unwrap();
    let got = fetch(&format!("{}/datasets", serve(router(p, db())).await)).await;
    assert_eq!((got.status, got.content_type.as_deref()), (200, Some("application/json")));
    let json = got.json();
    let t = template();
    let (cfg, _) = manifest_of(&bucket).await;

    assert_eq!(json["build"], serde_json::json!({ "date": DATE, "version": BUILD_VERSION, "dir": format!("{DATE}-1") }));
    assert_eq!(json["default"], "full.simplified.all");
    let datasets = json["datasets"].as_array().unwrap();
    assert_eq!(datasets.iter().map(|d| d["id"].as_str().unwrap()).collect::<Vec<_>>(), template_ids());
    assert_eq!(datasets.len(), 28);
    let presets: Vec<(&str, &str)> = json["presets"].as_array().unwrap().iter().map(|p| (p["id"].as_str().unwrap(), p["dataset"].as_str().unwrap())).collect();
    assert_eq!(presets, [("smallest", "admin-minimal.simplified.none"), ("immich-recommended", "admin.simplified.major"),
                         ("with-addresses", "full.simplified.all"), ("max-quality", "full.exact.all")]);
    for (got, want) in json["presets"].as_array().unwrap().iter().zip(t["presets"].as_array().unwrap()) {
        assert_eq!((&got["label"], &got["description"]), (&want["label"], &want["description"]), "preset {}", want["id"]);
        assert!(want["label"].is_string() && want["description"].is_string(), "preset {}", want["id"]);
    }
    let dataset = |id: &str| datasets.iter().find(|d| d["id"] == id).unwrap().clone();
    let with_addresses = &t["presets"].as_array().unwrap()[2];
    assert_eq!(dataset("full.simplified.all")["label"], with_addresses["label"]);
    assert_eq!(dataset("full.simplified.all")["description"], with_addresses["description"]);
    assert_eq!(dataset("full.simplified.all")["preset"], "with-addresses");
    assert_eq!(dataset("admin.exact.notable")["label"], "Admin only · Exact · Notable (~3.8M POIs)");
    assert_eq!(dataset("admin.exact.notable")["preset"], serde_json::Value::Null);
    assert_eq!(dataset("admin.exact.notable")["selection"],
               serde_json::json!({ "region": "planet", "mode": "admin", "quality": "exact", "poi_tier": "notable" }));
    let sets = data.path().join(BUILDS).join(format!("{DATE}-1")).join(SETS);
    let mut sharing_a_name = 0;
    for (s, _) in template_selections().into_iter().filter(|(_, allowed)| *allowed) {
        let files: Vec<String> = resolve_files(&cfg, &s).unwrap().paths.into_iter().filter(|p| cfg.files.contains_key(p)).collect();
        if files.iter().map(|f| basename(f)).collect::<BTreeSet<_>>().len() < files.len() {
            sharing_a_name += 1;
        }
        let units: BTreeSet<&String> = files.iter().filter_map(|f| cfg.files[f].unit.as_ref()).collect();
        let id = catalog::dataset_id(&s);
        let disk: u64 = std::fs::read_dir(sets.join(&id)).unwrap().map(|e| std::fs::metadata(e.unwrap().path()).unwrap().len()).sum();
        let download: u64 = files.iter().filter_map(|f| cfg.files[f].size_zst).sum::<u64>() + units.iter().map(|u| cfg.units[*u].size).sum::<u64>();
        let d = dataset(&id);
        assert_eq!((d["size_disk"].as_u64(), d["size_download"].as_u64()), (Some(disk), Some(download)), "{s:?}");
    }
    assert!(sharing_a_name > 0, "no selection has two files of one name");

    let constraint = &t["constraints"][0];
    let excluded: Vec<serde_json::Value> = template_selections().into_iter().filter(|(_, allowed)| !*allowed)
        .map(|(s, _)| serde_json::json!({
            "id": catalog::dataset_id(&s),
            "selection": { "region": s.region, "mode": s.mode, "quality": s.quality, "poi_tier": s.poi_tier },
            "constraint": constraint["id"],
            "message": constraint["message"],
            "requires": constraint["requires"],
        }))
        .collect();
    assert_eq!(excluded.len(), 4);
    assert_eq!(json["excluded"].as_array().unwrap(), &excluded);

    let axes: Vec<(&str, Vec<&str>)> = json["axes"].as_array().unwrap().iter()
        .map(|a| (a["id"].as_str().unwrap(), a["values"].as_array().unwrap().iter().map(|v| v["id"].as_str().unwrap()).collect()))
        .collect();
    let values = |axis: &str| template_values(axis);
    assert_eq!(axes, [("region", vec!["planet"]), ("mode", values("mode").iter().map(String::as_str).collect()),
                      ("quality", values("quality").iter().map(String::as_str).collect()),
                      ("poi_tier", values("poi_tier").iter().map(String::as_str).collect())]);
    for axis in json["axes"].as_array().unwrap() {
        let want = &t["axes"][axis["id"].as_str().unwrap()];
        assert_eq!((&axis["label"], &axis["description"]), (&want["label"], &want["description"]), "axis {}", axis["id"]);
        assert!(want["label"].is_string() && want["description"].is_string(), "axis {}", axis["id"]);
        for value in axis["values"].as_array().unwrap() {
            let of = &want["values"][value["id"].as_str().unwrap()];
            assert_eq!((&value["label"], &value["description"]), (&of["label"], &of["description"]), "{} {}", axis["id"], value["id"]);
            assert!(of["label"].is_string(), "{} {}", axis["id"], value["id"]);
        }
    }
    assert!(t["axes"]["mode"]["values"]["full"]["description"].is_string());
}

// The lookups run a few at once over every client, each on a blocking
// thread: one waits its turn while the others hold every place, /health
// answers meanwhile, and it is answered once a place is free.
#[tokio::test]
async fn a_lookup_waits_its_turn_while_the_lookups_at_once_run() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let mut two = config(&bucket, load);
    two.lookups_at_once = 2;
    let p = portal(data.path(), two);
    p.round(true).await.unwrap();
    let url = serve(router(p.clone(), db())).await;
    assert_eq!(p.lookups.available_permits(), 2);

    let taken = p.lookups.clone().try_acquire_many_owned(2).unwrap();
    let waiting: Vec<_> = ["reverse", "polygons"].into_iter()
        .map(|route| {
            let at = format!("{url}/{route}?lat=10.5&lon=20.5&key=test");
            tokio::spawn(async move { fetch(&at).await.status })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(waiting.iter().all(|w| !w.is_finished()));
    assert_eq!(fetch(&format!("{url}/health")).await.status, 200);

    drop(taken);
    for w in waiting {
        assert_eq!(w.await.unwrap(), 200);
    }
    assert_eq!(p.lookups.available_permits(), 2);
    assert_eq!(Config::from_env().lookups_at_once, LOOKUPS_AT_ONCE);
}

// A /reverse or /polygons without dataset= answers from with-addresses, as
// :3002 did; one names its dataset by a preset's id or its own, and the
// headers name the dataset and build that answered. A combination the
// constraints exclude is a 400 with the constraint's message, an unknown
// one a 400 naming what the build offers.
#[tokio::test]
async fn a_request_without_a_dataset_answers_with_addresses() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let p = portal(data.path(), config(&bucket, load));
    p.round(true).await.unwrap();
    let url = serve(router(p, db())).await;

    for route in ["reverse", "polygons"] {
        for (dataset, id) in [("", "full.simplified.all"), ("&dataset=with-addresses", "full.simplified.all"),
                              ("&dataset=smallest", "admin-minimal.simplified.none"), ("&dataset=admin.simplified.major", "admin.simplified.major")] {
            let got = fetch(&format!("{url}/{route}?lat=10.5&lon=20.5&key=test{dataset}")).await;
            assert_eq!(got.status, 200, "{route} {dataset}");
            assert!(got.body.contains(&format!("\"{id}\"")), "{route} {dataset}: {}", got.body);
            assert_eq!((got.header("x-geocoder-dataset"), got.header("x-geocoder-build")), (Some(id), Some(DATE)), "{route} {dataset}");
        }
    }
    let excluded = fetch(&format!("{url}/reverse?lat=10.5&lon=20.5&key=test&dataset=admin-minimal.exact.none")).await;
    assert_eq!((excluded.status, excluded.body.as_str()),
               (400, format!("dataset admin-minimal.exact.none is not offered: {}", template()["constraints"][0]["message"].as_str().unwrap()).as_str()));
    let unknown = fetch(&format!("{url}/reverse?lat=10.5&lon=20.5&key=test&dataset=everything")).await;
    assert_eq!(unknown.status, 400);
    assert!(unknown.body.starts_with("unknown dataset \"everything\": this build offers the presets smallest, immich-recommended, with-addresses, max-quality and "), "{}", unknown.body);
}

// An object whose fetch fails is fetched again after each pause; one that
// fails each time fails the round, which leaves its dir to the next to
// resume.
#[tokio::test]
async fn an_object_whose_fetch_fails_is_tried_again_after_a_pause() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let objects = fetched(&bucket).await;
    let (flaky, failing) = (object_url(&objects[0]), object_url(&objects[1]));
    let mut retrying = config(&bucket, load.clone());
    retrying.retry_delays = vec![Duration::from_millis(10); 2];
    bucket.fail(&flaky, 2);
    bucket.fail(&failing, 3);
    let p = portal(data.path(), retrying);

    let failed = p.round(true).await.unwrap_err();
    assert!(failed.starts_with(&format!("1 of build {DATE}'s objects did not come down: {}: HTTP error: ", objects[1].path)), "{failed}");
    let count = |path: &str| bucket.requests().iter().filter(|r| **r == format!("{DATE}/{path}")).count();
    assert_eq!((count(&flaky), count(&failing)), (3, 3));
    let before = bucket.requests().len();

    assert_eq!(p.round(false).await, Ok(format!("build {DATE} is served ({DATE}-1)")));
    assert_eq!(bucket.requests()[before..], [format!("{DATE}/{failing}")]);
}

// A sync that hasn't the room for a build's files and a margin beside them
// fetches nothing and says how much it needs.
#[tokio::test]
async fn a_sync_without_room_for_the_build_fetches_nothing_and_says_what_it_needs() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let mut cramped = config(&bucket, load);
    cramped.free_space = |_| Ok(1 << 30);
    let p = portal(data.path(), cramped);

    let failed = p.round(true).await.unwrap_err();

    assert!(failed.starts_with(&format!("1.00 GiB free in {}, 2.00 GiB needed to fetch build {DATE}: its files' 0.00 GiB and a margin of 2.00 GiB", data.path().display())), "{failed}");
    assert_eq!(bucket.requests().len(), 0);
    assert!(!data.path().join(BUILDS).join(format!("{DATE}-1")).exists());
}

// Rounds go one at a time, those of other processes on the data dir too: one
// waits while another holds the lock, fetching nothing.
#[tokio::test]
async fn a_round_waits_while_another_holds_the_sync_lock() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let lock = std::fs::File::create(data.path().join(sync::LOCK)).unwrap();
    lock.lock().unwrap();
    let p = portal(data.path(), config(&bucket, load));

    let round = tokio::spawn({
        let p = p.clone();
        async move { p.round(true).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!round.is_finished());
    assert_eq!(bucket.requests().len(), 0);
    drop(lock);

    assert_eq!(round.await.unwrap(), Ok(format!("build {DATE} is served ({DATE}-1)")));
}

// One query-portal a volume: while one holds the data dir's lock, another
// on that dir fails to start, saying why; once the first is gone, one
// starts again.
#[test]
fn a_second_portal_on_a_volume_fails_to_start_while_the_first_runs() {
    let data = ScratchDir::new();
    let first = sync::hold_volume(data.path()).unwrap();
    assert!(data.path().join(sync::VOLUME_LOCK).is_file());

    let second = sync::hold_volume(data.path()).unwrap_err();

    assert_eq!(second, format!(
        "another query-portal runs on {}: it holds {}, and two on one volume would prune each other's builds",
        data.path().display(), data.path().join(sync::VOLUME_LOCK).display()
    ));
    drop(first);
    sync::hold_volume(data.path()).unwrap();
}

// A date in the data dir's LATEST_OVERRIDE stands for latest.json's, so
// that a gate can have the portal fetch the builds it names.
#[tokio::test]
async fn a_latest_override_names_the_build_to_fetch() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    bucket.add_build(NEXT, BUILD_VERSION);
    let data = ScratchDir::new();
    std::fs::write(data.path().join(sync::LATEST_OVERRIDE), format!("{NEXT}\n")).unwrap();
    let (_fixtures, load) = fixtures();

    let p = portal(data.path(), config(&bucket, load));

    assert_eq!(p.round(true).await, Ok(format!("build {NEXT} is served ({NEXT}-1)")));
}

// A latest.json naming no date fails the round, writing nothing.
#[tokio::test]
async fn a_latest_build_of_no_date_fails_the_round() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    bucket.set_latest("../2026-01-01", Some(BUILD_VERSION));
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();

    let failed = portal(data.path(), config(&bucket, load)).round(true).await.unwrap_err();

    assert_eq!(failed, format!("{}/builds/latest.json names \"../2026-01-01\", no date", bucket.url));
    assert!(!data.path().join(BUILDS).exists());
}

// A ledger line a kill cut short names no object.
#[test]
fn a_ledgers_last_line_cut_short_names_nothing() {
    let dir = ScratchDir::new();
    std::fs::write(dir.path().join(sync::FETCHED), "planet/a.bin\nplanet/b/c.tt.zst\nplanet/d.b").unwrap();

    assert_eq!(sync::ledger(dir.path()), BTreeSet::from(["planet/a.bin".to_string(), "planet/b/c.tt.zst".to_string()]));
}

#[test]
fn times_are_iso_8601_in_utc() {
    use std::time::{Duration, UNIX_EPOCH};
    assert_eq!(sync::iso(UNIX_EPOCH), "1970-01-01T00:00:00Z");
    assert_eq!(sync::iso(UNIX_EPOCH + Duration::from_secs(1_763_626_896)), "2025-11-20T08:21:36Z");
    assert_eq!(sync::iso(UNIX_EPOCH + Duration::from_secs(951_782_400)), "2000-02-29T00:00:00Z");
}
