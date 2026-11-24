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
use crate::portal::search;
use crate::portal::sync::{self, BuildDir, BUILDS, COMPLETE, MANIFEST, REFUSED, SETS};
use crate::portal::{load_dataset, lookup, router, Config, Loader, Portal, BUILD_VERSION, LOOKUPS_AT_ONCE, SEARCHES_AT_ONCE};
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
    /// The datasets that fail to load, each by its build's date and its id.
    failing: Mutex<BTreeSet<(String, String)>>,
}

/// Loads each dataset dir (<build>/sets/<id>) as a dataset of its own, its
/// country named by its id; fails for those `fixtures` names.
fn fixtures() -> (Arc<Fixtures>, Loader) {
    let fixtures = Arc::new(Fixtures { dir: ScratchDir::new(), failing: Mutex::new(BTreeSet::new()) });
    let loaded = fixtures.clone();
    let load: Loader = Arc::new(move |set: &Path| {
        let id = set.file_name().unwrap().to_str().unwrap().to_string();
        let build = set.parent().unwrap().parent().unwrap().file_name().unwrap().to_str().unwrap();
        let date = BuildDir::parse(build).unwrap().date;
        let failing = loaded.failing.lock().unwrap();
        if failing.contains(&(date.clone(), id.clone())) {
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
    config_at(&bucket.url, load)
}

/// The portal's config of these tests, of the bucket at `url`.
fn config_at(url: &str, load: Loader) -> Config {
    Config {
        base_url: url.to_string(),
        interval: Duration::from_millis(20),
        retry_after: Duration::from_millis(20),
        retry_delays: Vec::new(),
        fetches_at_once: 8,
        lookups_at_once: LOOKUPS_AT_ONCE,
        searches_at_once: SEARCHES_AT_ONCE,
        load,
        open_search: Arc::new(search::open_search),
        search_jobs: 4,
        free_space: |_| Ok(u64::MAX),
        immich: None,
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

/// Flips a bit of the first byte of the file at `path`, in place.
fn flip(path: &Path) {
    use std::io::Write;
    let mut bytes = std::fs::read(path).unwrap();
    bytes[0] ^= 1;
    std::fs::OpenOptions::new().write(true).open(path).unwrap().write_all(&bytes).unwrap();
}

/// `load`, once each file of the dataset dir it loads holds the bytes of
/// the bucket's file it links: else an error naming the first that
/// doesn't, as a real load of damaged files may fail.
async fn checking(bucket: &TestBucket, load: Loader) -> Loader {
    let (cfg, manifest) = manifest_of(bucket).await;
    let links: BTreeMap<String, BTreeMap<String, String>> =
        catalog::plan(&cfg, &manifest).unwrap().datasets.into_iter().map(|d| (d.id.clone(), d.links())).collect();
    let bytes: BTreeMap<String, Vec<u8>> = cfg.files.keys().map(|f| (f.clone(), bucket.file(f).to_vec())).collect();
    Arc::new(move |set: &Path| {
        let id = set.file_name().unwrap().to_str().unwrap();
        for (name, path) in &links[id] {
            if std::fs::read(set.join(name)).ok().as_ref() != Some(&bytes[path]) {
                return Err(format!("{name} doesn't hold the bytes of {path}"));
            }
        }
        load(set)
    })
}

/// A sync of the bucket's build into `data`, an object at a time, killed
/// while it fetches the fourth largest: its objects, largest first, and the
/// build dir it left part way, its ledger naming the first three.
async fn killed_part_way(bucket: &TestBucket, data: &Path, load: Loader) -> (Vec<Object>, std::path::PathBuf) {
    let mut one_at_a_time = config(bucket, load);
    one_at_a_time.fetches_at_once = 1;
    let p = portal(data, one_at_a_time);
    let mut order = fetched(bucket).await;
    order.sort_by(|a, b| b.size.cmp(&a.size).then_with(|| a.path.cmp(&b.path)));
    let holding = bucket.hold(&object_url(&order[3]));
    let round = tokio::spawn(async move { p.round(true).await });
    holding.reached().await;
    round.abort();
    assert!(round.await.unwrap_err().is_cancelled());
    // The decode the kill left on its thread sees its stream end.
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(holding);
    let root = data.join(BUILDS).join(format!("{DATE}-1"));
    assert_eq!(sync::ledger(&root), order[..3].iter().map(|o| o.path.clone()).collect::<BTreeSet<_>>());
    (order, root)
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
    let mut found = BTreeMap::new();
    for route in ["reverse", "polygons"] {
        for q in &queries {
            let want = fetch(&format!("{theirs}/{route}?{q}")).await;
            *found.entry(route).or_insert(0) += usize::from(want.status == 200 && want.body.contains("Testland"));
            for dataset in ["", "&dataset=with-addresses", "&dataset=full.simplified.all"] {
                let got = fetch(&format!("{ours}/{route}?{q}{dataset}")).await;
                assert_eq!((got.status, &got.content_type, &got.body), (want.status, &want.content_type, &want.body), "/{route}?{q}{dataset}");
            }
        }
    }
    // Each route answers with the dataset's country at the points in it,
    // not only with what both would answer of none.
    for route in ["reverse", "polygons"] {
        assert!(found.get(route).is_some_and(|&n| n >= 3), "/{route}: {found:?} answers name the country");
    }
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
    let mut held: BTreeMap<&str, &Object> = BTreeMap::new();
    for o in &plan.objects {
        for f in &o.files {
            if let Some(other) = held.insert(f.as_str(), o) {
                panic!("{f} is a file of {} and of {}", other.path, o.path);
            }
        }
    }
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

// The portal serves each selection without water (catalog::WATER): no
// dataset takes a file of the build's water dirs, and no sync fetches one;
// a preset of a selection with water names none of its datasets, as a
// preset of another region names none.
#[tokio::test]
async fn the_portal_serves_the_selections_without_water() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let (cfg, _) = manifest_of(&bucket).await;
    let plan = plan_edited(&bucket, |_| {}).await.unwrap();
    let water: BTreeSet<&String> = cfg.files.keys().filter(|f| f.starts_with("planet/water/")).collect();
    assert!(!water.is_empty());
    for d in &plan.datasets {
        assert!(d.files.iter().all(|f| !water.contains(f)), "{}", d.id);
    }
    assert!(plan.objects.iter().flat_map(|o| &o.files).all(|f| !water.contains(f)));

    let wet = plan_edited(&bucket, |m| {
        let mut wet = m["presets"][0].clone();
        wet["id"] = "wet".into();
        wet["selections"]["water"] = "all".into();
        m["presets"].as_array_mut().unwrap().push(wet);
    })
    .await
    .unwrap();
    let presets = |plan: &catalog::Plan| plan.presets.iter().map(|(p, at)| (p.id.clone(), *at)).collect::<Vec<_>>();
    assert_eq!(presets(&wet), presets(&plan));
}

// A build of another build version is refused once its manifest says so,
// latest.json giving that version or none: nothing of it is fetched, the
// build served stays, and the refusal is on record, with why, as long as
// latest.json names that build. While latest.json gives another version
// than the manifest, the round fails and refuses nothing.
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
    let failed = p.round(false).await.unwrap_err();
    assert!(failed.starts_with(&format!("latest.json gives build {THIRD} build version {BUILD_VERSION}, ")), "{failed}");
    assert!(refused().is_empty(), "{:?}", refused());
    bucket.set_latest(THIRD, None);
    assert_eq!(p.round(false).await, Ok(format!("refused build {THIRD}: {why}")));
    assert_eq!(refused(), [(THIRD.to_string(), BUILD_VERSION, why.clone())]);
    assert_eq!(p.round(false).await, Ok(format!("build {THIRD} was refused: {why}")));

    assert_eq!(p.served().unwrap().dir, format!("{DATE}-1"));
    assert_eq!(p.status().refused.iter().map(|r| r.date.as_str()).collect::<Vec<_>>(), [THIRD]);
    assert_eq!(bucket.requests().len(), served);
    assert_eq!(build_dirs(data.path()), [format!("{DATE}-1")]);
}

// A build whose datasets don't all load is refused, though all but its
// last do: the build served stays, the refused one's dir goes, and the
// refusal is on record, so that neither this server nor one started afresh
// on the data dir fetches it again.
#[tokio::test]
async fn a_build_that_doesnt_load_is_never_switched_to_is_recorded_and_not_fetched_again() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (fixtures, load) = fixtures();
    let last = template_ids().pop().unwrap();
    fixtures.failing.lock().unwrap().insert((NEXT.to_string(), last.clone()));
    let p = portal(data.path(), config(&bucket, load.clone()));
    p.round(true).await.unwrap();

    bucket.add_build(NEXT, BUILD_VERSION);
    bucket.set_latest(NEXT, Some(BUILD_VERSION));
    let done = p.round(false).await.unwrap();
    let why = format!("it doesn't load: dataset {last}: it holds no admin_cells.bin of build {NEXT}");
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

// A manifest whose constraint names an axis or a value its axes don't
// have (which would rule out nothing, or everything it is meant to
// allow), or whose preset has another preset's id or a selection's (which
// dataset= would take for another dataset than it names), can't be
// planned, and says why.
#[tokio::test]
async fn a_manifest_whose_constraints_or_presets_name_what_it_lacks_or_shares_cant_be_planned() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let constraint = template()["constraints"][0]["id"].as_str().unwrap().to_string();
    for (key, axis, value, why) in [
        ("when", "mode", "admin-minimum", "the mode value \"admin-minimum\", which the manifest's mode axis doesn't have"),
        ("when", "modes", "admin-minimal", "the axis \"modes\", which the manifest doesn't have"),
        ("requires", "quality", "simple", "the quality value \"simple\", which the manifest's quality axis doesn't have"),
        ("requires", "qualities", "simplified", "the axis \"qualities\", which the manifest doesn't have"),
    ] {
        let got = plan_edited(&bucket, |m| {
            let mut values = serde_json::Map::new();
            values.insert(axis.to_string(), value.into());
            m["constraints"][0][key] = values.into();
        })
        .await
        .map(|_| ());
        assert_eq!(got, Err(format!("constraint {constraint} names {why}")), "{key} {axis} {value}");
    }

    let twice = plan_edited(&bucket, |m| {
        let mut again = m["presets"][0].clone();
        again["selections"]["mode"] = "full".into();
        m["presets"].as_array_mut().unwrap().push(again);
    })
    .await
    .map(|_| ());
    assert_eq!(twice, Err("two presets have the id smallest".to_string()));
    for id in ["full.exact.none", "admin.simplified.major", "admin-minimal.exact.none"] {
        let got = plan_edited(&bucket, |m| m["presets"][1]["id"] = id.into()).await.map(|_| ());
        assert_eq!(got, Err(format!("preset {id} has the id of a selection, which dataset= names as such")), "{id}");
    }
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
    let refused = health.json()["sync"]["refused"].clone();
    let why = format!("it is of build version {}; this server reads build version {BUILD_VERSION}", BUILD_VERSION + 1);
    assert_eq!(refused.as_array().map(Vec::len), Some(1), "{refused}");
    assert_eq!((refused[0]["date"].as_str(), refused[0]["reason"].as_str()), (Some(NEXT), Some(why.as_str())), "{refused}");
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

// A host without CA certificates (an image without a CA bundle) builds no
// HTTP client. A portal there still starts: /health answers 503 naming why
// its rounds fail, /test answers, and nothing is fetched; the first round
// once the certificates are there builds the client and serves the build.
// Started again without them, it serves the build its volume holds and its
// round fails naming why. The case runs in a process of its own
// (test_bucket::without_ca_store).
#[cfg(all(unix, not(target_vendor = "apple"), not(target_os = "android")))]
#[test]
fn a_portal_without_ca_certificates_starts_serves_its_volumes_build_and_its_rounds_fail_naming_why() {
    test_bucket::without_ca_store("portal_tests::without_ca_certificates");
}

#[cfg(all(unix, not(target_vendor = "apple"), not(target_os = "android")))]
#[tokio::test]
#[ignore = "run by a_portal_without_ca_certificates_starts_serves_its_volumes_build_and_its_rounds_fail_naming_why"]
async fn without_ca_certificates() {
    if std::env::var_os(test_bucket::NO_CA_STORE).is_none() {
        return;
    }
    // The case's own fetches, of plain HTTP, with a store of no certificate:
    // one that loads no system's.
    let client = reqwest::Client::builder().tls_certs_only(Vec::new()).build().unwrap();
    let get = |url: String| {
        let client = client.clone();
        async move {
            let resp = client.get(url).send().await.unwrap();
            (resp.status().as_u16(), resp.text().await.unwrap())
        }
    };
    let why = "HTTP error: building the client: builder error: unexpected error: No CA certificates were loaded from the system";
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let p = portal(data.path(), config(&bucket, load.clone()));
    let running = tokio::spawn(p.clone().run());
    let url = serve(router(p.clone(), db())).await;
    until("a round", || p.status().last_result.is_some()).await;

    let (status, health) = get(format!("{url}/health")).await;
    assert_eq!(status, 503, "{health}");
    let health: serde_json::Value = serde_json::from_str(&health).unwrap();
    let reason = health["reason"].as_str().unwrap();
    assert!(reason.ends_with(&format!("; the last sync: failed: {why}")), "{reason}");
    assert!(health["sync"]["latest"].is_null(), "{health}");
    let (status, page) = get(format!("{url}/test")).await;
    assert_eq!((status, page.as_str()), (200, include_str!("../../../test-portal/index.html")));
    assert_eq!(bucket.requests(), Vec::<String>::new());
    assert!(!data.path().join(BUILDS).exists());

    let none = std::env::var_os("SSL_CERT_FILE").unwrap();
    let certificates = ScratchDir::new();
    test_bucket::with_test_ca(certificates.path());
    until("the build served", || p.served().is_some()).await;
    let (status, health) = get(format!("{url}/health")).await;
    assert_eq!(status, 200, "{health}");
    running.abort();

    // Safe to change: the process runs this case alone, on a runtime of one thread.
    std::env::set_var("SSL_CERT_FILE", none);
    let fetched = bucket.requests().len();
    let again = portal(data.path(), config(&bucket, load));
    assert_eq!(again.round(true).await, Err(why.to_string()));
    assert_eq!(again.served().map(|s| s.dir.clone()), Some(format!("{DATE}-1")));
    let url = serve(router(again, db())).await;
    let (status, health) = get(format!("{url}/health")).await;
    assert_eq!(status, 200, "{health}");
    let (status, _) = get(format!("{url}/reverse?lat=10.5&lon=20.5&key=test")).await;
    assert_eq!(status, 200);
    assert_eq!(bucket.requests().len(), fetched);
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
    let source = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    portal(source.path(), config(&bucket, load.clone())).round(true).await.unwrap();
    let made = source.path().join(BUILDS).join(format!("{DATE}-1"));
    let (cfg, manifest) = manifest_of(&bucket).await;
    let plan = catalog::plan(&cfg, &manifest).unwrap();
    let damaged = "planet/full/admin_cells.bin";
    // A copy of the build as NEXT's, a byte of the file flipped in a copy
    // of its own (the build's dirs share it by hard links), which its
    // datasets' dirs link: it is in place, and its bytes alone are amiss.
    let damaged_next = |builds: &Path| {
        let root = builds.join(format!("{NEXT}-1"));
        copy_build(&made, &root, NEXT, BUILD_VERSION);
        let at = root.join(damaged);
        let mut bytes = std::fs::read(&at).unwrap();
        bytes[0] ^= 1;
        std::fs::remove_file(&at).unwrap();
        std::fs::write(&at, &bytes).unwrap();
        for d in &plan.datasets {
            for (name, _) in d.links().into_iter().filter(|(_, path)| path == damaged) {
                let link = root.join(SETS).join(&d.id).join(name);
                std::fs::remove_file(&link).unwrap();
                std::fs::hard_link(&at, &link).unwrap();
            }
        }
    };
    let skipped = |dir: &str| format!("build {dir} here can't be served: {damaged} holds bytes of sha256 ");

    // Alone on a volume, the copy is not served, for its file's bytes.
    let alone = ScratchDir::new();
    damaged_next(&alone.path().join(BUILDS));
    // No round gets past the start: the latest build stays unknown.
    std::fs::write(alone.path().join(sync::LATEST_OVERRIDE), "tomorrow\n").unwrap();
    let none = portal(alone.path(), config(&bucket, load.clone()));
    assert!(none.round(true).await.is_err());
    assert!(none.served().is_none());
    assert!(none.unavailable().contains(&skipped(&format!("{NEXT}-1"))), "{}", none.unavailable());

    // Beside the build it is a copy of, that one is served.
    let data = ScratchDir::new();
    let builds = data.path().join(BUILDS);
    copy_build(&made, &builds.join(format!("{DATE}-1")), DATE, BUILD_VERSION);
    damaged_next(&builds);
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

    // The build alone holds its files now: a byte flipped in place, which
    // its datasets' links see too.
    flip(&builds.join(format!("{DATE}-1")).join(damaged));
    let none = portal(data.path(), config(&bucket, recording));
    assert!(none.round(true).await.is_err());
    assert!(none.served().is_none());
    assert!(none.unavailable().contains(&skipped(&format!("{DATE}-1"))), "{}", none.unavailable());
}

// A dataset loads from its dir of links, so a build is served only where
// each such dir holds its dataset's files and nothing else, each the
// build's file itself (a hard link of it), whose bytes the start reads: not
// a copy of its own, damaged or not, nor a symlink, nor a file besides.
#[tokio::test]
async fn a_start_serves_no_build_whose_dataset_dirs_hold_other_than_its_files() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let made = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    portal(made.path(), config(&bucket, load.clone())).round(true).await.unwrap();
    let (cfg, manifest) = manifest_of(&bucket).await;
    let dataset = catalog::plan(&cfg, &manifest).unwrap().datasets.remove(0);
    let (name, path) = dataset.links().into_iter().next().unwrap();
    let entry = format!("{SETS}/{}/{name}", dataset.id);
    let stray = format!("{SETS}/{}/stray.bin", dataset.id);
    let not_it = format!("{entry} is no hard link of {path}");
    let besides = format!("{stray} is none of its dataset's files");

    for (case, why) in [("a damaged copy", &not_it), ("a copy", &not_it), ("a symlink", &not_it), ("a file besides", &besides)] {
        let data = ScratchDir::new();
        let root = data.path().join(BUILDS).join(format!("{DATE}-1"));
        copy_build(&made.path().join(BUILDS).join(format!("{DATE}-1")), &root, DATE, BUILD_VERSION);
        let at = root.join(&entry);
        match case {
            "a damaged copy" | "a copy" => {
                let mut bytes = std::fs::read(&at).unwrap();
                if case == "a damaged copy" {
                    bytes[0] ^= 1;
                }
                std::fs::remove_file(&at).unwrap();
                std::fs::write(&at, bytes).unwrap();
            }
            "a symlink" => {
                std::fs::remove_file(&at).unwrap();
                std::os::unix::fs::symlink(root.join(&path), &at).unwrap();
            }
            _ => std::fs::write(root.join(&stray), b"stray").unwrap(),
        }
        // No round gets past the start.
        std::fs::write(data.path().join(sync::LATEST_OVERRIDE), "tomorrow\n").unwrap();

        let p = portal(data.path(), config(&bucket, load.clone()));
        assert!(p.round(true).await.is_err(), "{case}");

        assert!(p.served().is_none(), "{case}: build {} served", p.served().unwrap().dir);
        let skipped = format!("build {DATE}-1 here can't be served: {why}");
        assert!(p.unavailable().contains(&skipped), "{case}: {}", p.unavailable());
    }
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
    let (cfg, manifest) = manifest_of(&bucket).await;
    for f in catalog::plan(&cfg, &manifest).unwrap().objects.iter().flat_map(|o| &o.files) {
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
    // The second object's files in place: adding its line, which fails, is
    // what the round does next, while the first's fetch is held.
    let in_place = |o: &Object| o.files.iter().all(|f| std::fs::read(root.join(f)).is_ok_and(|b| b == bucket.file(f)));
    until("the second object's files", || in_place(&order[1])).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!round.is_finished());
    assert_eq!(bucket.requests().len(), 2);
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

/// The bytes of a volume `free_on_volume` tells the room of.
static VOLUME: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The bytes of the regular files under `dir`.
fn bytes_under(dir: &Path) -> u64 {
    std::fs::read_dir(dir).map_or(0, |entries| {
        entries.map(|e| e.unwrap()).map(|e| {
            let kind = e.file_type().unwrap();
            if kind.is_dir() { bytes_under(&e.path()) } else if kind.is_file() { e.metadata().unwrap().len() } else { 0 }
        }).sum()
    })
}

/// The room on a volume of VOLUME bytes that holds the data dir `data` and
/// nothing else.
fn free_on_volume(data: &Path) -> std::io::Result<u64> {
    Ok(VOLUME.load(std::sync::atomic::Ordering::SeqCst).saturating_sub(bytes_under(data)))
}

/// The bytes free that `room` tells of, whatever the volume holds.
static ROOM: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn room(_: &Path) -> std::io::Result<u64> {
    Ok(ROOM.load(std::sync::atomic::Ordering::SeqCst))
}

// A sync killed part way leaves files its ledger doesn't name: an object in
// place whose line hadn't gone in, a try's temporary file. The next, on a
// volume with room for the whole build and the margin besides, removes them
// before it weighs the room it needs, counts only what is still to fetch,
// and resumes.
#[tokio::test]
async fn a_resumed_sync_removes_what_its_ledger_doesnt_name_before_it_weighs_the_room() {
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
    for f in &order[4].files {
        std::fs::create_dir_all(root.join(f).parent().unwrap()).unwrap();
        std::fs::write(root.join(f), bucket.file(f)).unwrap();
    }
    let abandoned = root.join(format!("{}.4242.7.tmp", order[4].files[0]));
    std::fs::File::create(&abandoned).unwrap().set_len(1 << 20).unwrap();
    let build: u64 = order.iter().flat_map(|o| &o.files).map(|f| bucket.manifest.files[f].size_raw).sum();
    let manifest = std::fs::metadata(root.join(MANIFEST)).unwrap().len();
    VOLUME.store(sync::FREE_MARGIN + sync::SEARCH_ROOM + build + manifest + (64 << 10), std::sync::atomic::Ordering::SeqCst);
    let mut roomy = config(&bucket, load);
    roomy.free_space = free_on_volume;
    let before = bucket.requests().len();

    assert_eq!(portal(data.path(), roomy).round(true).await, Ok(format!("build {DATE} is served ({DATE}-1)")));

    let mut resumed = bucket.requests()[before..].to_vec();
    resumed.sort();
    let mut want: Vec<String> = order[3..].iter().map(|o| format!("{DATE}/{}", object_url(o))).collect();
    want.sort();
    assert_eq!(resumed, want);
    assert!(!abandoned.exists());
    assert_eq!(leftovers(&root), Vec::<String>::new());
}

// A resumed sync reads the files of the objects its ledger names against
// their sha256 before it takes those objects as in place: one whose bytes
// changed at their size goes, counts in the room the sync wants, and is
// fetched again, and the build is served, not refused.
#[tokio::test]
async fn a_resumed_sync_fetches_again_an_object_its_ledger_names_whose_bytes_changed() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let (order, root) = killed_part_way(&bucket, data.path(), load.clone()).await;
    let changed = &order[0];
    flip(&root.join(&changed.files[0]));
    let size = |objects: &[Object]| -> u64 { objects.iter().flat_map(|o| &o.files).map(|f| bucket.manifest.files[f].size_raw).sum() };
    let load = checking(&bucket, load).await;
    let mut tight = config(&bucket, load.clone());
    tight.free_space = room;
    // Room for the objects the ledger doesn't name, but not for the one
    // whose bytes changed besides.
    ROOM.store(sync::FREE_MARGIN + sync::SEARCH_ROOM + size(&order[3..]), std::sync::atomic::Ordering::SeqCst);

    let failed = portal(data.path(), tight).round(true).await.unwrap_err();

    assert!(failed.contains(&format!("needed to fetch build {DATE}: ")), "{failed}");
    ROOM.store(sync::FREE_MARGIN + sync::SEARCH_ROOM + size(&order[3..]) + size(std::slice::from_ref(changed)), std::sync::atomic::Ordering::SeqCst);
    let mut roomy = config(&bucket, load);
    roomy.free_space = room;
    let before = bucket.requests().len();
    assert_eq!(portal(data.path(), roomy).round(true).await, Ok(format!("build {DATE} is served ({DATE}-1)")));
    let mut again = bucket.requests()[before..].to_vec();
    again.sort();
    let mut want: Vec<String> = std::iter::once(changed).chain(&order[3..]).map(|o| format!("{DATE}/{}", object_url(o))).collect();
    want.sort();
    assert_eq!(again, want);
    for f in &changed.files {
        assert!(std::fs::read(root.join(f)).unwrap() == bucket.file(f), "{f}");
    }
}

// A round reads every file of the build against its sha256 before it
// switches to it, not only as each comes down: where one's bytes changed in
// its dir since, the round fails and its dir goes, the build not refused,
// and the next round fetches the build again, into a dir made anew.
#[tokio::test]
async fn a_round_reads_the_builds_files_before_it_switches_and_fetches_again_one_whose_bytes_changed() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let mut one_at_a_time = config(&bucket, checking(&bucket, load).await);
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
    let changed = data.path().join(BUILDS).join(format!("{DATE}-1")).join(&order[0].files[0]);
    flip(&changed);
    drop(holding);

    let failed = round.await.unwrap().unwrap_err();

    assert!(failed.starts_with(&format!("build {DATE}-1's files don't check out: ")), "{failed}");
    assert!(failed.contains(" holds bytes of sha256 "), "{failed}");
    assert!(p.served().is_none());
    assert!(p.status().refused.is_empty(), "{:?}", p.status().refused);
    assert!(!data.path().join(REFUSED).exists());
    assert_eq!(build_dirs(data.path()), Vec::<String>::new());
    let before = bucket.requests().len();
    assert_eq!(p.round(false).await, Ok(format!("build {DATE} is served ({DATE}-1)")));
    assert_eq!(bucket.requests().len() - before, order.len());
    assert!(std::fs::read(&changed).unwrap() == bucket.file(&order[0].files[0]));

    // The dir made anew under the name of the one that went is a build's
    // like any other: once the next build is served, the prune keeps it as
    // the one served before.
    bucket.add_build(NEXT, BUILD_VERSION);
    bucket.set_latest(NEXT, Some(BUILD_VERSION));
    assert_eq!(p.round(false).await, Ok(format!("build {NEXT} is served ({NEXT}-1)")));
    assert_eq!(build_dirs(data.path()), [format!("{DATE}-1"), format!("{NEXT}-1")]);
}

// A ledger whose last line a kill cut short is mended before the resumed
// sync adds a line: each object it fetches is a line of its own, none
// joined to what the kill left.
#[tokio::test]
async fn a_resumed_sync_mends_a_ledger_line_a_kill_cut_short_before_it_adds_one() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let (order, root) = killed_part_way(&bucket, data.path(), load.clone()).await;
    {
        use std::io::Write;
        let mut ledger = std::fs::OpenOptions::new().append(true).open(root.join(sync::FETCHED)).unwrap();
        ledger.write_all(&order[3].path.as_bytes()[..8]).unwrap();
    }

    assert_eq!(portal(data.path(), config(&bucket, load)).round(true).await, Ok(format!("build {DATE} is served ({DATE}-1)")));

    let text = std::fs::read_to_string(root.join(sync::FETCHED)).unwrap();
    assert!(text.ends_with('\n'), "{text:?}");
    let mut lines: Vec<&str> = text.lines().collect();
    lines.sort();
    let mut want: Vec<&str> = order.iter().map(|o| o.path.as_str()).collect();
    want.sort();
    assert_eq!(lines, want);
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
    bucket.set_latest(NEXT, None);
    let why = format!("it is of build version {}; this server reads build version {BUILD_VERSION}", BUILD_VERSION + 1);
    assert_eq!(p.round(false).await, Ok(format!("refused build {NEXT}: {why}")));
    assert_eq!(p.round(false).await, Ok(format!("build {NEXT} was refused: {why}")));

    bucket.add_build(NEXT, BUILD_VERSION);

    assert_eq!(p.round(false).await, Ok(format!("build {NEXT} is served ({NEXT}-1)")));
    assert!(p.status().refused.is_empty(), "{:?}", p.status().refused);
    let again = portal(data.path(), config(&bucket, load));
    assert_eq!(again.round(true).await, Ok(format!("build {NEXT} is served ({NEXT}-1)")));
}

// A build version latest.json gives that its build's manifest doesn't (the
// nightly published a date's build again, and latest.json is not yet, or
// no longer, of it) refuses nothing: the round fails, to be tried again,
// saying why, and once the two agree the build is fetched and served.
#[tokio::test]
async fn a_latest_json_version_other_than_its_manifests_fails_the_round_and_refuses_nothing() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let p = portal(data.path(), config(&bucket, load));
    p.round(true).await.unwrap();
    bucket.edit_build(DATE, |m| m["presets"][2]["label"] = "With street addresses, corrected".into());
    bucket.set_latest(DATE, Some(BUILD_VERSION + 1));

    let failed = p.round(false).await.unwrap_err();

    assert_eq!(failed, format!(
        "latest.json gives build {DATE} build version {}, its {MANIFEST} build version {BUILD_VERSION}: nothing is refused while they disagree",
        BUILD_VERSION + 1
    ));
    assert!(p.status().refused.is_empty(), "{:?}", p.status().refused);
    assert!(!data.path().join(REFUSED).exists());
    assert_eq!(p.served().unwrap().dir, format!("{DATE}-1"));
    bucket.set_latest(DATE, Some(BUILD_VERSION));
    assert_eq!(p.round(false).await, Ok(format!("build {DATE} is served ({DATE}-2)")));
}

// A build version latest.json gives that the manifest of the build served,
// or of the one refused, doesn't fails the round as well, to be tried again
// soon: the manifest fetched may be of the publication before the one
// latest.json names, as the nightly publishes a date's build again.
#[tokio::test]
async fn a_latest_json_version_other_than_the_served_or_refused_manifests_fails_the_round() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let p = portal(data.path(), config(&bucket, load));
    p.round(true).await.unwrap();
    let disagree = |date: &str, latest: u32, manifest: u32| format!(
        "latest.json gives build {date} build version {latest}, its {MANIFEST} build version {manifest}: nothing is refused while they disagree"
    );

    bucket.set_latest(DATE, Some(BUILD_VERSION + 1));
    assert_eq!(p.round(false).await, Err(disagree(DATE, BUILD_VERSION + 1, BUILD_VERSION)));
    assert_eq!(p.served().unwrap().dir, format!("{DATE}-1"));

    bucket.add_build(NEXT, BUILD_VERSION + 1);
    bucket.set_latest(NEXT, None);
    let why = format!("it is of build version {}; this server reads build version {BUILD_VERSION}", BUILD_VERSION + 1);
    assert_eq!(p.round(false).await, Ok(format!("refused build {NEXT}: {why}")));
    bucket.set_latest(NEXT, Some(BUILD_VERSION));
    assert_eq!(p.round(false).await, Err(disagree(NEXT, BUILD_VERSION, BUILD_VERSION + 1)));
    bucket.set_latest(NEXT, Some(BUILD_VERSION + 1));
    assert_eq!(p.round(false).await, Ok(format!("build {NEXT} was refused: {why}")));
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

    assert_eq!(json["build"], serde_json::json!({
        "date": DATE, "version": BUILD_VERSION, "id": publication_of(&bucket, DATE).await, "dir": format!("{DATE}-1"),
    }));
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
    // The build's credit, as its manifest has it (the template's): null where
    // it has none.
    assert_eq!(json["attribution"], t["attribution"]);
}

// /datasets passes on the credit of the build's data, its manifest's
// attribution, as it is, for the page to show: null where the manifest has
// none.
#[tokio::test]
async fn datasets_passes_on_the_builds_attribution_as_it_is() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let credit = serde_json::json!([{
        "id": "osm", "text": "© OpenStreetMap contributors", "url": "https://www.openstreetmap.org/copyright",
        "licence": "ODbL 1.0", "licence_url": "https://opendatacommons.org/licenses/odbl/1-0/",
        "notices": ["This dataset is a derivative database of OpenStreetMap, available under the ODbL 1.0."],
        "applies_to": ["mode.*"],
    }]);
    for (given, listed) in [(credit.clone(), credit), ("no list".into(), "no list".into())] {
        let plan = plan_edited(&bucket, |m| m["attribution"] = given).await.unwrap();
        assert_eq!(plan.json("id", "dir")["attribution"], listed);
    }
    let plan = plan_edited(&bucket, |m| {
        m.as_object_mut().unwrap().remove("attribution");
    }).await.unwrap();
    assert_eq!(plan.json("id", "dir")["attribution"], serde_json::Value::Null);
}

// /datasets lists the ids of the build's components, sorted, which
// the shell patterns of its attribution's applies_to name: the page tells a
// credit of the water alone by the components its patterns match.
#[tokio::test]
async fn datasets_lists_the_ids_of_the_builds_components() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let mut ids: Vec<String> = template()["components"].as_object().unwrap().keys().cloned().collect();
    ids.sort();
    assert!(ids.contains(&"water.major".to_string()) && ids.contains(&"dir.admin-minimal".to_string()), "{ids:?}");
    let plan = plan_edited(&bucket, |_| {}).await.unwrap();
    assert_eq!(plan.json("id", "dir")["components"], serde_json::json!(ids));
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

// A lookup holds its place until its answer's body is sent or dropped, not
// only until the answer is made: the answer's bytes are in memory until
// then. The next lookup waits while a body is held, and goes once it is
// sent; a body dropped unsent frees its place too.
#[tokio::test]
async fn a_lookup_holds_its_place_until_its_answers_body_is_released() {
    use axum::body::Body;
    use axum::response::IntoResponse;
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let mut one = config(&bucket, load);
    one.lookups_at_once = 1;
    let p = portal(data.path(), one);
    p.round(true).await.unwrap();
    let served = p.served().unwrap();
    // An answer whose body comes once `send` sends it.
    let held = || {
        let (send, body) = tokio::sync::oneshot::channel::<&'static str>();
        let stream = futures_util::stream::once(async move { body.await.map(bytes::Bytes::from).map_err(std::io::Error::other) });
        (send, move |_: &query_server::MultiIndex| Body::from_stream(stream).into_response())
    };

    let (send, answer) = held();
    let first = lookup(&p, served.clone(), 0, answer).await;
    assert_eq!(p.lookups.available_permits(), 0);
    let next = tokio::spawn({
        let (p, served) = (p.clone(), served.clone());
        async move { lookup(&p, served, 0, |_| "next".into_response()).await.status() }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!next.is_finished());
    send.send("first").unwrap();
    assert_eq!(axum::body::to_bytes(first.into_body(), usize::MAX).await.unwrap(), "first");
    assert_eq!(next.await.unwrap(), 200);
    assert_eq!(p.lookups.available_permits(), 1);

    let (_send, answer) = held();
    let unsent = lookup(&p, served, 0, answer).await;
    assert_eq!(p.lookups.available_permits(), 0);
    drop(unsent);
    assert_eq!(p.lookups.available_permits(), 1);
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

// A /reverse the portal can't answer (no build served yet, a dataset it
// doesn't offer) takes nothing of its key's daily allowance: a key allowed
// one lookup a day has it answered after those.
#[tokio::test]
async fn a_reverse_the_portal_cant_answer_takes_nothing_of_the_keys_allowance() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let p = portal(data.path(), config(&bucket, load));
    let db = serde_json::json!({
        "users": { "daily": { "password_hash": "", "admin": false, "rate_per_second": 0, "rate_per_day": 1, "rate_by_ip": false } },
        "tokens": { "daily": "daily" },
    });
    let url = serve(router(p.clone(), Arc::new(RwLock::new(serde_json::from_value(db).unwrap())))).await;
    let reverse = |dataset: &str| format!("{url}/reverse?lat=10.5&lon=20.5&key=daily{dataset}");

    assert_eq!(fetch(&reverse("")).await.status, 503);
    p.round(true).await.unwrap();
    for dataset in ["&dataset=everything", "&dataset=admin-minimal.exact.none"] {
        assert_eq!(fetch(&reverse(dataset)).await.status, 400, "{dataset}");
    }

    assert_eq!(fetch(&reverse("")).await.status, 200);
    assert_eq!(fetch(&reverse("")).await.status, 429);
}

/// The publication ID of the build the bucket publishes now under `date`:
/// the date and the first 12 hex digits of its manifest's sha256.
async fn publication_of(bucket: &TestBucket, date: &str) -> String {
    let manifest = reqwest::get(format!("{}/builds/{date}/{MANIFEST}", bucket.url)).await.unwrap().bytes().await.unwrap();
    format!("{date}-{}", &sync::manifest_sha256(&manifest)[..12])
}

/// Whether the lookups' headers, /health and /datasets of the portal at
/// `url` name the build served by the publication ID `id` and the dir `dir`.
async fn names_the_build(url: &str, id: &str, dir: &str) {
    for route in ["reverse", "polygons"] {
        let got = fetch(&format!("{url}/{route}?lat=10.5&lon=20.5&key=test")).await;
        assert_eq!((got.header("x-geocoder-build"), got.header("x-geocoder-build-id"), got.header("x-geocoder-build-dir")),
                   (Some(DATE), Some(id), Some(dir)), "{route}");
        let exposed: Vec<String> = got.header("access-control-expose-headers").unwrap_or_default().split(", ").map(str::to_string).collect();
        for header in ["x-geocoder-build", "x-geocoder-build-id", "x-geocoder-build-dir", "x-geocoder-dataset"] {
            assert!(exposed.iter().any(|h| h == header), "{route}: {exposed:?}");
        }
    }
    let health = fetch(&format!("{url}/health")).await.json();
    assert_eq!((&health["build"]["id"], &health["build"]["dir"], &health["sync"]["served"]),
               (&serde_json::Value::from(id), &serde_json::Value::from(dir), &serde_json::Value::from(dir)));
    let datasets = fetch(&format!("{url}/datasets")).await.json();
    assert_eq!((&datasets["build"]["id"], &datasets["build"]["dir"]), (&serde_json::Value::from(id), &serde_json::Value::from(dir)));
}

// A lookup's headers name the build that answered by its date, its
// publication ID and its dir, which /health (its build and its sync's) and
// /datasets name too: a build published again under its date is another,
// in a dir of its own, and they name that once it is served.
#[tokio::test]
async fn the_headers_health_and_datasets_name_the_build_served_by_its_publication_and_its_dir() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let p = portal(data.path(), config(&bucket, load));
    p.round(true).await.unwrap();
    let url = serve(router(p.clone(), db())).await;
    let first = publication_of(&bucket, DATE).await;
    names_the_build(&url, &first, &format!("{DATE}-1")).await;

    bucket.edit_build(DATE, |m| m["presets"][2]["label"] = "With street addresses, corrected".into());
    assert_eq!(p.round(false).await, Ok(format!("build {DATE} is served ({DATE}-2)")));

    let again = publication_of(&bucket, DATE).await;
    assert_ne!(again, first);
    names_the_build(&url, &again, &format!("{DATE}-2")).await;
}

// The publication ID names a build as it was published, the dir only where
// a volume holds it: two publications of a date, each fetched into a volume
// of its own, both fill <date>-1, and each is named by an ID of its own.
// So does a portal started afresh on the volume, serving the build it holds.
#[tokio::test]
async fn two_publications_of_a_date_in_one_dir_of_two_volumes_have_ids_of_their_own() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let (before, after) = (ScratchDir::new(), ScratchDir::new());
    let (_fixtures, load) = fixtures();
    let first = portal(before.path(), config(&bucket, load.clone()));
    first.round(true).await.unwrap();
    let first_id = publication_of(&bucket, DATE).await;
    bucket.edit_build(DATE, |m| m["presets"][2]["label"] = "With street addresses, corrected".into());
    let second = portal(after.path(), config(&bucket, load.clone()));
    assert_eq!(second.round(true).await, Ok(format!("build {DATE} is served ({DATE}-1)")));
    let second_id = publication_of(&bucket, DATE).await;

    assert_ne!(first_id, second_id);
    assert_eq!((first.served().unwrap().dir.clone(), second.served().unwrap().dir.clone()), (format!("{DATE}-1"), format!("{DATE}-1")));
    names_the_build(&serve(router(first, db())).await, &first_id, &format!("{DATE}-1")).await;
    names_the_build(&serve(router(second, db())).await, &second_id, &format!("{DATE}-1")).await;

    // The first volume's build, served from its dir at start, the bucket's
    // publication now another.
    std::fs::write(before.path().join(sync::LATEST_OVERRIDE), "tomorrow\n").unwrap();
    let restarted = portal(before.path(), config(&bucket, load));
    assert!(restarted.round(true).await.is_err());
    names_the_build(&serve(router(restarted, db())).await, &first_id, &format!("{DATE}-1")).await;
}

// An object whose fetch fails is fetched again after each pause, each try
// once its pause is over; one that fails each time fails the round, which
// leaves its dir to the next to resume.
#[tokio::test]
async fn an_object_whose_fetch_fails_is_tried_again_after_a_pause() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let objects = fetched(&bucket).await;
    let (flaky, failing) = (object_url(&objects[0]), object_url(&objects[1]));
    let mut retrying = config(&bucket, load.clone());
    retrying.retry_delays = vec![Duration::from_millis(100); 2];
    bucket.fail(&flaky, 2);
    bucket.fail(&failing, 3);
    let p = portal(data.path(), retrying);
    let started = std::time::Instant::now();

    let failed = p.round(true).await.unwrap_err();

    assert!(started.elapsed() >= Duration::from_millis(200), "{:?}", started.elapsed());
    assert!(failed.starts_with(&format!("1 of build {DATE}'s objects did not come down: {}: HTTP error: ", objects[1].path)), "{failed}");
    let count = |path: &str| bucket.requests().iter().filter(|r| **r == format!("{DATE}/{path}")).count();
    assert_eq!((count(&flaky), count(&failing)), (3, 3));
    let before = bucket.requests().len();

    assert_eq!(p.round(false).await, Ok(format!("build {DATE} is served ({DATE}-1)")));
    assert_eq!(bucket.requests()[before..], [format!("{DATE}/{failing}")]);
}

/// A dir of the bucket's build for a sync to resume, `<DATE>-1`, holding its
/// manifest alone: its root.
async fn to_resume(bucket: &TestBucket, data: &Path) -> std::path::PathBuf {
    let root = data.join(BUILDS).join(format!("{DATE}-1"));
    std::fs::create_dir_all(&root).unwrap();
    let manifest = reqwest::get(format!("{}/builds/{DATE}/{MANIFEST}", bucket.url)).await.unwrap().bytes().await.unwrap();
    std::fs::write(root.join(MANIFEST), &manifest).unwrap();
    root
}

// A file whose try can't put it in place (a dir at its path) is tried again
// after each pause, and each try removes its temporary file: the tries
// leave none, where each would leave a whole file's bytes.
#[tokio::test]
async fn a_file_that_cant_go_into_place_is_tried_again_and_its_tries_leave_no_temporary_file() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let object = fetched(&bucket).await.into_iter().find(|o| !o.unit).unwrap();
    let root = to_resume(&bucket, data.path()).await;
    std::fs::create_dir_all(root.join(&object.path)).unwrap();
    let mut retrying = config(&bucket, load);
    retrying.retry_delays = vec![Duration::ZERO; 2];

    let failed = portal(data.path(), retrying).round(true).await.unwrap_err();

    assert!(failed.starts_with(&format!("1 of build {DATE}'s objects did not come down: {}: I/O error: rename ", object.path)), "{failed}");
    let count = bucket.requests().iter().filter(|r| **r == format!("{DATE}/{}", object_url(&object))).count();
    assert_eq!(count, 3);
    assert_eq!(leftovers(&root), Vec::<String>::new());
}

// A try that can't remove its temporary file once the file failed to go
// into place is not made again: what it left stays on the disk, and each try
// again would leave a file of its own beside it.
#[tokio::test]
async fn a_try_that_leaves_a_temporary_file_it_cant_remove_is_not_made_again() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let object = fetched(&bucket).await.into_iter().find(|o| !o.unit).unwrap();
    let root = to_resume(&bucket, data.path()).await;
    // A dir at the file's path that no dir renames over: it holds one.
    let dest = root.join(&object.path);
    std::fs::create_dir_all(dest.join("held")).unwrap();
    let (dir, name) = (dest.parent().unwrap().to_path_buf(), dest.file_name().unwrap().to_str().unwrap().to_string());
    let temporary = move || std::fs::read_dir(&dir).unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.file_name().unwrap().to_str().unwrap().starts_with(&format!("{name}.")) && p.extension().is_some_and(|x| x == "tmp"));
    let mut retrying = config(&bucket, load);
    retrying.retry_delays = vec![Duration::ZERO; 2];
    let p = portal(data.path(), retrying);
    let holding = bucket.hold(&object_url(&object));

    let round = tokio::spawn(async move { p.round(true).await });
    holding.reached().await;
    until("the try's temporary file", || temporary().is_some()).await;
    // Its temporary file, a dir by the time it goes into place: a dir it
    // can't rename over its file's, nor remove as a file.
    let tmp = temporary().unwrap();
    std::fs::remove_file(&tmp).unwrap();
    std::fs::create_dir(&tmp).unwrap();
    drop(holding);

    let failed = round.await.unwrap().unwrap_err();
    assert!(failed.contains(&format!("and remove {}: ", tmp.display())), "{failed}");
    let count = bucket.requests().iter().filter(|r| **r == format!("{DATE}/{}", object_url(&object))).count();
    assert_eq!(count, 1);
}

// A sync that hasn't the room for a build's files, its place search
// indexes and a margin beside them fetches nothing and says how much it
// needs.
#[tokio::test]
async fn a_sync_without_room_for_the_build_fetches_nothing_and_says_what_it_needs() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let mut cramped = config(&bucket, load);
    cramped.free_space = |_| Ok(1 << 30);
    let p = portal(data.path(), cramped);

    let failed = p.round(true).await.unwrap_err();

    assert!(failed.starts_with(&format!(
        "1.00 GiB free in {}, 9.00 GiB needed to fetch build {DATE}: its files' 0.00 GiB, its place search indexes' 7.00 GiB and a margin of 2.00 GiB",
        data.path().display()
    )), "{failed}");
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

// A first round that can't take its lock has not started: the next one
// serves the volume's build before it asks the bucket anything, and its
// prune keeps that build though the latest build stays unknown.
#[tokio::test]
async fn a_first_round_that_cant_take_its_lock_leaves_the_start_to_the_next() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    portal(data.path(), config(&bucket, load.clone())).round(true).await.unwrap();
    // A dir where the lock goes: it can't be opened.
    let lock = data.path().join(sync::LOCK);
    std::fs::remove_file(&lock).unwrap();
    std::fs::create_dir(&lock).unwrap();
    // No round gets past the start.
    std::fs::write(data.path().join(sync::LATEST_OVERRIDE), "tomorrow\n").unwrap();
    let p = portal(data.path(), config(&bucket, load));
    tokio::spawn(p.clone().run());
    let result = || p.status().last_result.unwrap_or_default();
    until("a round that can't take its lock", || result().starts_with("failed: lock ")).await;

    std::fs::remove_dir(&lock).unwrap();
    until("a round past the lock", || result().contains(sync::LATEST_OVERRIDE)).await;

    assert_eq!(p.served().map(|s| s.dir.clone()), Some(format!("{DATE}-1")));
    assert_eq!(build_dirs(data.path()), [format!("{DATE}-1")]);
}

// A first round that can't list the volume's builds fails, asking the bucket
// nothing and pruning nothing, and leaves the start to the next: once the
// builds can be listed, that one serves the volume's build though the bucket
// is out of reach, and its prune keeps it.
#[tokio::test]
async fn a_first_round_that_cant_list_the_builds_leaves_the_start_to_the_next() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    portal(data.path(), config(&bucket, load.clone())).round(true).await.unwrap();
    // The builds' dir, a link to a file until the builds are linked back.
    let (builds, kept, file) = (data.path().join(BUILDS), data.path().join("builds.kept"), data.path().join("no-dir"));
    std::fs::rename(&builds, &kept).unwrap();
    std::fs::write(&file, "").unwrap();
    std::os::unix::fs::symlink(&file, &builds).unwrap();
    // No round gets past the latest build.
    std::fs::write(data.path().join(sync::LATEST_OVERRIDE), "tomorrow\n").unwrap();
    let p = portal(data.path(), config(&bucket, load));
    let before = bucket.requests().len();

    let failed = p.round(true).await.unwrap_err();

    assert!(failed.starts_with(&format!("read {}: ", builds.display())), "{failed}");
    assert_eq!(bucket.requests().len(), before);
    tokio::spawn(p.clone().run());
    let result = || p.status().last_result.unwrap_or_default();
    until("a round that can't list the builds", || result().starts_with("failed: read ")).await;
    // The builds linked back at once, by a link renamed over the other.
    std::os::unix::fs::symlink(&kept, data.path().join("builds.back")).unwrap();
    std::fs::rename(data.path().join("builds.back"), &builds).unwrap();
    let complete = kept.join(format!("{DATE}-1")).join(COMPLETE);
    until("the volume's build served, or removed", || p.served().is_some() || !complete.exists()).await;
    until("a round past the start", || result().contains(sync::LATEST_OVERRIDE)).await;

    assert_eq!(p.served().map(|s| s.dir.clone()), Some(format!("{DATE}-1")));
    assert_eq!(build_dirs(data.path()), [format!("{DATE}-1")]);
}

// An interval longer than the clock can count past now leaves the sync
// running: the time of the next round goes untold, and the sync waits.
#[tokio::test]
async fn an_interval_past_the_clocks_reach_leaves_the_sync_running() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let (_fixtures, load) = fixtures();
    let mut forever = config(&bucket, load);
    forever.interval = Duration::MAX;
    let p = portal(data.path(), forever);

    let running = tokio::spawn(p.clone().run());
    until("a round", || p.status().last_result.is_some()).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(!running.is_finished());
    assert_eq!(p.status().last_result, Some(format!("ok: build {DATE} is served ({DATE}-1)")));
    assert_eq!(p.status().next_round, None);
}

// A latest.override the round can't read fails the round, saying why: it
// is not taken for none, with latest.json's build fetched instead.
#[tokio::test]
async fn a_latest_override_that_cant_be_read_fails_the_round() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let data = ScratchDir::new();
    let path = data.path().join(sync::LATEST_OVERRIDE);
    std::fs::write(&path, b"\xff2026-01-02\n").unwrap();
    let (_fixtures, load) = fixtures();

    let failed = portal(data.path(), config(&bucket, load)).round(true).await.unwrap_err();

    assert!(failed.starts_with(&format!("read {}: ", path.display())), "{failed}");
    assert_eq!(bucket.requests().len(), 0);
    assert!(!data.path().join(BUILDS).exists());
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

// The place search's tests, beside these, whose helpers they take.
#[path = "search_tests.rs"]
mod search_tests;

// The feedback's tests, as the place search's.
#[path = "feedback_tests.rs"]
mod feedback_tests;

// The Immich column's tests, as the place search's.
#[path = "immich_tests.rs"]
mod immich_tests;
