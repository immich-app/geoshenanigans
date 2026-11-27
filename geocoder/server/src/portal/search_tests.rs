// The portal's place search (search.rs): its groups, the opening of their
// indexes on each path that serves a build, and /search, whose answers are
// those the node binding and place-search give over the same index. The
// indexes are built for real, of the datasets the tests' loaders load; an
// opener wraps the portal's to hold, fail or count opens.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Condvar;

use super::*;
use place_search::constants::META_FILE;
use place_search::dataset::{Origin, PlaceSearch, Stamps, DATASET_FILES};
use place_search::profile::{Profile, SearchRequest};
use place_search::query::{debug_line, results_json};

/// A gate the opens of a test wait at (Gate::waiting) until the test opens
/// it. It opens when it is dropped too, so that a test that fails before
/// it opens it lets go of the opens it holds, and ends at once; it never
/// opens by itself, however long a busy runner takes to reach what the
/// test checks while it holds them.
struct Gate(Arc<(Mutex<bool>, Condvar)>);

/// What the opens of a gate wait at.
#[derive(Clone)]
struct Waiting(Arc<(Mutex<bool>, Condvar)>);

impl Gate {
    fn new() -> Gate {
        Gate(Arc::new((Mutex::new(false), Condvar::new())))
    }

    fn waiting(&self) -> Waiting {
        Waiting(self.0.clone())
    }

    fn open(&self) {
        let (open, opened) = &*self.0;
        *open.lock().unwrap_or_else(|e| e.into_inner()) = true;
        opened.notify_all();
    }
}

impl Drop for Gate {
    fn drop(&mut self) {
        self.open();
    }
}

impl Waiting {
    fn wait(&self) {
        let (open, opened) = &*self.0;
        drop(opened.wait_while(open.lock().unwrap(), |open| !*open).unwrap());
    }
}

/// The portal's opener, each open of which first calls `hook` with its
/// index's name (its group's) and its dataset's dir: an error the hook
/// returns is the open's.
fn opener(hook: impl Fn(&str, &Path) -> Result<(), String> + Send + Sync + 'static) -> search::Opener {
    Arc::new(move |geo: Arc<query_server::Index>, stamps: &Stamps, dir: &Path, index: &Path| {
        hook(index.file_name().unwrap().to_str().unwrap(), dir)?;
        search::open_search(geo, stamps, dir, index)
    })
}

/// A loader of one dataset for every dataset dir: the country Testland and
/// its town Testville (write_dataset), in a dir of `files`.
fn testland(files: &ScratchDir) -> Loader {
    let dir = files.path().join("testland");
    write_dataset(&dir, "Testland");
    Arc::new(move |_: &Path| load_dataset(&dir))
}

/// The dir of the index of the group `group` of the build dir `build`.
fn index_dir(data: &Path, build: &str, group: &str) -> PathBuf {
    data.join(BUILDS).join(build).join(sync::SEARCH).join(group)
}

/// /health's search of the portal at `url`.
async fn health_search(url: &str) -> serde_json::Value {
    fetch(&format!("{url}/health")).await.json()["search"].clone()
}

/// /health's state of the group `group`, as `search` (health_search) tells it.
fn state_of<'a>(search: &'a serde_json::Value, group: &str) -> &'a serde_json::Value {
    search["groups"].as_array().unwrap().iter().find(|g| g["index"] == group).unwrap_or_else(|| panic!("no group {group}: {search}"))
}

/// The bucket of the template's build whose dirs of the modes full,
/// no-addresses and admin hold alike bytes under each name, as the planet
/// build's place files and core strings are: their datasets of a quality
/// and a POI tier are one group, as the planet build's are.
async fn sharing() -> TestBucket {
    test_bucket::serve_alike(test_bucket::template_components(), &["planet/full", "planet/no-addresses", "planet/admin"]).await
}

/// The groups of the plan of the bucket's build: their names.
async fn group_names(bucket: &TestBucket) -> Vec<String> {
    let (cfg, manifest) = manifest_of(bucket).await;
    catalog::plan(&cfg, &manifest).unwrap().groups.into_iter().map(|g| g.name).collect()
}

// The datasets whose dirs hold the files an index is made of alike are one
// group, named by its first dataset: two are of one group exactly where
// each of those files is of one sha256 in both, or lacking in both. Where
// the modes full, no-addresses and admin hold the same place files and core
// strings, as the planet build's do, the 28 make 12: a quality and a POI
// tier of those three share one, and each of admin-minimal's is its own.
#[tokio::test]
async fn datasets_whose_index_files_are_alike_are_one_group() {
    for bucket in [test_bucket::serve(test_bucket::template_components()).await, sharing().await] {
        let (cfg, manifest) = manifest_of(&bucket).await;
        let plan = catalog::plan(&cfg, &manifest).unwrap();
        let files = |d: &catalog::Dataset| -> Vec<Option<String>> {
            let links = d.links();
            DATASET_FILES.iter().map(|name| links.get(*name).map(|path| cfg.files[path].sha256.clone())).collect()
        };
        for (g, group) in plan.groups.iter().enumerate() {
            assert_eq!(group.name, plan.datasets[group.datasets[0]].id);
            assert!(group.datasets.iter().all(|&d| plan.datasets[d].group == g), "{}", group.name);
        }
        for a in &plan.datasets {
            for b in &plan.datasets {
                assert_eq!(a.group == b.group, files(a) == files(b), "{} {}", a.id, b.id);
            }
        }
    }

    let (cfg, manifest) = manifest_of(&sharing().await).await;
    let plan = catalog::plan(&cfg, &manifest).unwrap();
    let got: Vec<(String, Vec<String>)> = plan.groups.iter()
        .map(|g| (g.name.clone(), g.datasets.iter().map(|&d| plan.datasets[d].id.clone()).collect()))
        .collect();
    let mut want = Vec::new();
    for quality in template_values("quality") {
        for tier in template_values("poi_tier") {
            let ids = ["full", "no-addresses", "admin"].map(|mode| format!("{mode}.{quality}.{tier}")).to_vec();
            want.push((ids[0].clone(), ids));
        }
    }
    for tier in template_values("poi_tier") {
        let id = format!("admin-minimal.simplified.{tier}");
        want.push((id.clone(), vec![id]));
    }
    assert_eq!(got, want);
}

// A group's cost, by which its index is built before or after others', is
// the bytes of the files of its records and names, then of its boundaries'
// geometry.
#[tokio::test]
async fn a_groups_cost_is_the_bytes_of_its_records_then_of_its_geometry() {
    let bucket = sharing().await;
    let (cfg, manifest) = manifest_of(&bucket).await;
    let plan = catalog::plan(&cfg, &manifest).unwrap();
    let geometry = ["admin_rings.bin", "arc_index.bin", "arc_vertices.bin"];

    for group in &plan.groups {
        let links = plan.datasets[group.datasets[0]].links();
        let bytes = |of_geometry: bool| -> u64 {
            DATASET_FILES.iter().filter(|name| geometry.contains(*name) == of_geometry)
                .filter_map(|name| links.get(*name)).map(|path| cfg.files[path].size_raw).sum()
        };
        assert_eq!(group.cost, (bytes(false), bytes(true)), "{}", group.name);
        assert!(group.cost.0 > 0 && group.cost.1 > 0, "{}", group.name);
    }
}

// The group of a selection whose index the build publishes (search_index's
// shipped) knows the index's dir and its files but search_meta.json, each
// with the sha256 the manifest gives it, none where the manifest lacks the
// file: a build published without its index (the nightly's search step
// failed) is served all the same. A manifest whose search_index doesn't
// read as one publishes none, and is served too.
#[tokio::test]
async fn the_groups_of_the_shipped_selections_know_the_published_index() {
    let bucket = sharing().await;
    let t = template();
    let names: Vec<String> = t["search_index"]["files"].as_array().unwrap().iter()
        .chain(t["search_index"]["optional_files"].as_array().unwrap())
        .map(|n| n.as_str().unwrap().to_string())
        .filter(|n| n != META_FILE)
        .collect();
    let published = "planet/search/full/simplified/all/places.fst";

    let plan = plan_edited(&bucket, |m| {
        m["files"][published] = serde_json::json!({ "size_zst": 10, "size_raw": 10, "sha256": "ab" });
    }).await.unwrap();

    let shipped: Vec<(&str, &catalog::Shipped)> = plan.groups.iter().filter_map(|g| Some((g.name.as_str(), g.shipped.as_ref()?))).collect();
    let selections: Vec<String> = t["search_index"]["shipped"].as_array().unwrap().iter()
        .map(|s| format!("full.{}.{}", s["quality"].as_str().unwrap(), s["poi_tier"].as_str().unwrap()))
        .collect();
    let mut groups: Vec<String> = shipped.iter().map(|(g, _)| g.to_string()).collect();
    groups.sort();
    let mut selections = selections;
    selections.sort();
    assert_eq!(groups, selections);
    let (_, simplified) = shipped.iter().find(|(g, _)| *g == "full.simplified.all").unwrap();
    assert_eq!(simplified.dir, "planet/search/full/simplified/all");
    assert_eq!(simplified.files.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>(), names);
    for (name, sha256) in &simplified.files {
        assert_eq!(sha256.as_deref(), (name == "places.fst").then_some("ab"), "{name}");
    }
    let unread = plan_edited(&bucket, |m| m["search_index"] = "everything".into()).await.unwrap();
    assert!(unread.groups.iter().all(|g| g.shipped.is_none()));
}

// The comparison of an index with the one a build publishes: a file of
// other bytes than the published one's differs, as does one the index
// lacks; one the build doesn't publish is unpublished; one neither has is
// none of these.
#[test]
fn an_index_is_compared_with_the_published_one_by_each_files_sha256() {
    let dir = ScratchDir::new();
    for (name, bytes) in [("same", "a"), ("other", "b"), ("unpublished", "c")] {
        std::fs::write(dir.path().join(name), bytes).unwrap();
    }
    let sha256 = |s: &str| query_server::lower_hex(&<sha2::Sha256 as sha2::Digest>::digest(s.as_bytes()));
    let shipped = catalog::Shipped {
        dir: "planet/search/full/simplified/all".into(),
        files: vec![
            ("same".into(), Some(sha256("a"))),
            ("other".into(), Some(sha256("a"))),
            ("lacking".into(), Some(sha256("a"))),
            ("unpublished".into(), None),
            ("neither".into(), None),
        ],
    };

    let check = search::check_shipped(dir.path(), &shipped);

    assert_eq!(check, search::ShippedCheck {
        dir: shipped.dir.clone(),
        differ: vec!["other".into(), "lacking".into()],
        unpublished: vec!["unpublished".into()],
    });
    assert!(!check.same());
    let same = catalog::Shipped { dir: shipped.dir, files: vec![("same".into(), Some(sha256("a"))), ("neither".into(), None)] };
    assert!(search::check_shipped(dir.path(), &same).same());
}

// A /search answers what the node binding's searchPlaces answers from the
// same dataset and index, byte for byte (query::results_json), and with
// debug=1 the line place-search query prints (query::debug_line), but the
// time it took: for queries of a place, a country, a typo, a prefix, a
// context after a comma, none found, spaces around, and each limit, without
// one the binding's default. The index is the one the sync built in the
// group's dir, which another opener finds built from these files.
#[tokio::test]
async fn a_search_answers_byte_for_byte_what_the_binding_answers_from_its_index() {
    let bucket = sharing().await;
    let (data, files) = (ScratchDir::new(), ScratchDir::new());
    let load = testland(&files);
    let p = portal(data.path(), config(&bucket, load.clone()));
    p.round(true).await.unwrap();
    let url = serve(router(p.clone(), db())).await;
    let build = format!("{DATE}-1");

    let mut found = 0;
    for (dataset, group) in [("with-addresses", "full.simplified.all"), ("admin.exact.major", "full.exact.major"), ("smallest", "admin-minimal.simplified.none")] {
        let id = p.served().unwrap().plan.datasets[p.served().unwrap().plan.find(dataset).unwrap()].id.clone();
        let set = data.path().join(BUILDS).join(&build).join(SETS).join(&id);
        let geo = load(&set).unwrap().continents[0].index.load_full().unwrap();
        let (theirs, origin) = PlaceSearch::open(geo, &Stamps::of(&set).unwrap(), &set, &index_dir(data.path(), &build, group), &Default::default()).unwrap();
        assert_eq!(origin, Origin::Current, "{dataset}");
        for q in ["testville", "Testville", "testvile", "testland", "testv", "testville, testland", "nowhere at all", "  testville "] {
            for limit in [None, Some(1), Some(5), Some(50)] {
                let wanted = limit.unwrap_or(Profile::Immich.policy().default_limit);
                let (results, stats) = theirs.search(&SearchRequest { limit: wanted, ..SearchRequest::new(q, Profile::Immich) }).unwrap();
                let encoded: String = q.bytes().map(|b| format!("%{b:02X}")).collect();
                let at = format!("{url}/search?q={encoded}&dataset={dataset}&key=test{}", limit.map(|l| format!("&limit={l}")).unwrap_or_default());

                let got = fetch(&at).await;
                assert_eq!((got.status, got.content_type.as_deref()), (200, Some("application/json")), "{at}: {}", got.body);
                assert_eq!(got.body, results_json(&results), "{at}");
                found += results.len();
                assert_eq!((got.header("x-geocoder-dataset"), got.header("x-geocoder-search-index"), got.header("x-geocoder-build-dir")),
                           (Some(id.as_str()), Some(group), Some(build.as_str())), "{at}");
                let exposed = got.header("access-control-expose-headers").unwrap_or_default().to_string();
                assert!(exposed.split(", ").any(|h| h == "x-geocoder-search-index"), "{exposed}");

                let debug = fetch(&format!("{at}&debug=1")).await;
                assert_eq!(debug.status, 200, "{at}");
                assert!(debug.json()["us"].as_f64().is_some_and(|us| us >= 0.0), "{}", debug.body);
                // The line but its time, its last key.
                let timeless = |line: &str| line[..line.rfind(",\"us\":").unwrap()].to_string();
                assert_eq!(timeless(&debug.body), timeless(&debug_line(q, 0.0, &results, &stats).to_string()), "{at}");
            }
        }
    }
    assert!(found > 0, "no query found anything");
}

// A /search without dataset= searches with-addresses, as /reverse answers
// from it; one of a dataset the build doesn't offer is a 400 naming what it
// does, of one the constraints exclude a 400 with the constraint's message,
// and with no build served a 503 with the reason.
#[tokio::test]
async fn a_search_names_its_dataset_as_a_lookup_does() {
    let bucket = sharing().await;
    let (data, files) = (ScratchDir::new(), ScratchDir::new());
    let p = portal(data.path(), config(&bucket, testland(&files)));
    let url = serve(router(p.clone(), db())).await;
    let search = |dataset: &str| format!("{url}/search?q=testville&key=test{dataset}");

    let none = fetch(&search("")).await;
    assert_eq!(none.status, 503);
    assert!(none.body.starts_with("no build to serve"), "{}", none.body);
    p.round(true).await.unwrap();

    let got = fetch(&search("")).await;
    assert_eq!((got.status, got.header("x-geocoder-dataset")), (200, Some("full.simplified.all")));
    assert_eq!(got.body, fetch(&search("&dataset=with-addresses")).await.body);
    let unknown = fetch(&search("&dataset=everything")).await;
    assert_eq!(unknown.status, 400);
    assert!(unknown.body.starts_with("unknown dataset \"everything\": this build offers the presets "), "{}", unknown.body);
    let excluded = fetch(&search("&dataset=admin-minimal.exact.none")).await;
    assert_eq!((excluded.status, excluded.body.as_str()),
               (400, format!("dataset admin-minimal.exact.none is not offered: {}", template()["constraints"][0]["message"].as_str().unwrap()).as_str()));
}

// A /search takes a key (401 without one or with a bad one), q of 1 to 256
// bytes once the spaces at its ends are left out (400 else), limit 1 to 50
// (400 else) and debug=1 alone (400 else). It counts against the key's rate
// as a /reverse does, in the same allowance; a search the portal can't
// answer (no build served, a dataset it doesn't offer, an index not open)
// takes nothing of it.
#[tokio::test]
async fn a_search_checks_its_key_query_limit_and_rate() {
    let bucket = sharing().await;
    let (data, files) = (ScratchDir::new(), ScratchDir::new());
    let gate = Gate::new();
    let waiting = gate.waiting();
    let mut cfg = config(&bucket, testland(&files));
    cfg.open_search = opener(move |group, _| {
        if group == "full.exact.none" {
            waiting.wait();
        }
        Ok(())
    });
    let p = portal(data.path(), cfg);
    let db = serde_json::json!({
        "users": {
            "test": { "password_hash": "", "admin": false, "rate_per_second": 0, "rate_per_day": 0, "rate_by_ip": false },
            "daily": { "password_hash": "", "admin": false, "rate_per_second": 0, "rate_per_day": 2, "rate_by_ip": false },
        },
        "tokens": { "test": "test", "daily": "daily" },
    });
    let url = serve(router(p.clone(), Arc::new(RwLock::new(serde_json::from_value(db).unwrap())))).await;
    assert_eq!(fetch(&format!("{url}/search?q=testville&key=daily")).await.status, 503);
    // The volume's build, served at start: its groups open after, one of
    // them held.
    portal(data.path(), config(&bucket, testland(&files))).round(true).await.unwrap();
    std::fs::remove_dir_all(index_dir(data.path(), &format!("{DATE}-1"), "full.exact.none")).unwrap();
    let round = tokio::spawn({
        let p = p.clone();
        async move { p.round(true).await }
    });
    until("the build served", || p.served().is_some()).await;
    // A search is answered only once its group is open (503 before, ahead
    // of the key and the query), and the cases search the default dataset:
    // they wait for its group to open as well as for the held one to be
    // building. Both open on threads of the lowest priority, which a busy
    // runner keeps waiting while the test goes on.
    until("the default dataset's group open and the held one building", || {
        let search = p.served().unwrap().search.json();
        state_of(&search, "full.simplified.all")["state"] == "ready" && state_of(&search, "full.exact.none")["state"] == "building"
    }).await;

    let cases = [
        ("q=testville", 401, "Missing API key"),
        ("q=testville&key=nope", 401, "Invalid API key"),
        ("key=test", 400, "q must name what to search for"),
        ("q=&key=test", 400, "q must name what to search for"),
        ("q=%20%20&key=test", 400, "q must name what to search for"),
        ("q=testville&key=test&limit=0", 400, "limit must be 1 to 50"),
        ("q=testville&key=test&limit=51", 400, "limit must be 1 to 50"),
        ("q=testville&key=test&debug=2", 400, "debug must be 1"),
    ];
    for (query, status, body) in cases {
        let got = fetch(&format!("{url}/search?{query}")).await;
        assert_eq!((got.status, got.body.as_str()), (status, body), "{query}");
    }
    assert_eq!(fetch(&format!("{url}/search?q=testville&key=test&limit=many")).await.status, 400);
    let longest = "a".repeat(256);
    assert_eq!(fetch(&format!("{url}/search?q=%20{longest}%20&key=test")).await.status, 200);
    let over = fetch(&format!("{url}/search?q={longest}a&key=test")).await;
    assert_eq!((over.status, over.body.as_str()), (400, "q must be at most 256 bytes"));
    assert_eq!(fetch(&format!("{url}/search?q=testville&key=test&limit=50")).await.status, 200);

    for dataset in ["everything", "admin-minimal.exact.none", "full.exact.none"] {
        assert_ne!(fetch(&format!("{url}/search?q=testville&key=daily&dataset={dataset}")).await.status, 200, "{dataset}");
    }
    assert_eq!(fetch(&format!("{url}/search?q=testville&key=daily")).await.status, 200);
    assert_eq!(fetch(&format!("{url}/reverse?lat=10.5&lon=20.5&key=daily")).await.status, 200);
    let spent = fetch(&format!("{url}/search?q=testville&key=daily")).await;
    assert_eq!(spent.status, 429);
    gate.open();
    round.await.unwrap().unwrap();
}

// At start the build the volume holds is served at once, and its place
// search opens after: meanwhile /reverse answers, and /search of a dataset
// whose group isn't open answers 503 building, with when that began and a
// Retry-After; /health tells each group's state. A group whose open fails
// answers 503 failed with why. The others answer as soon as theirs is
// open.
#[tokio::test]
async fn a_start_serves_at_once_and_its_place_search_answers_as_each_group_opens() {
    let bucket = sharing().await;
    let (data, files) = (ScratchDir::new(), ScratchDir::new());
    portal(data.path(), config(&bucket, testland(&files))).round(true).await.unwrap();
    std::fs::remove_dir_all(data.path().join(BUILDS).join(format!("{DATE}-1")).join(sync::SEARCH)).unwrap();
    let gate = Gate::new();
    let waiting = gate.waiting();
    let mut cfg = config(&bucket, testland(&files));
    cfg.open_search = opener(move |group, _| match group {
        "full.simplified.all" => {
            waiting.wait();
            Ok(())
        }
        "full.exact.all" => Err("no room for its sort runs".to_string()),
        _ => Ok(()),
    });
    let p = portal(data.path(), cfg);
    let url = serve(router(p.clone(), db())).await;

    let round = tokio::spawn({
        let p = p.clone();
        async move { p.round(true).await }
    });
    until("the build served", || p.served().is_some()).await;
    assert_eq!(fetch(&format!("{url}/reverse?lat=10.5&lon=20.5&key=test")).await.status, 200);
    let failed = || p.served().unwrap().search.json()["groups"].as_array().unwrap().iter().any(|g| g["state"] == "failed");
    until("the failing group", failed).await;

    let building = fetch(&format!("{url}/search?q=testville&key=test")).await;
    assert_eq!((building.status, building.header("retry-after"), building.content_type.as_deref()), (503, Some("5"), Some("application/json")));
    let body = building.json();
    assert_eq!((&body["status"], &body["dataset"], &body["index"]), (&"building".into(), &"full.simplified.all".into(), &"full.simplified.all".into()));
    assert!(body["since"].as_str().is_some_and(|s| s.ends_with('Z')), "{body}");
    let broken = fetch(&format!("{url}/search?q=testville&key=test&dataset=max-quality")).await;
    assert_eq!((broken.status, broken.header("retry-after")), (503, None));
    let body = broken.json();
    assert_eq!((&body["status"], &body["dataset"], &body["index"]), (&"failed".into(), &"full.exact.all".into(), &"full.exact.all".into()));
    assert_eq!(body["reason"], "dataset full.exact.all: no room for its sort runs");
    let health = health_search(&url).await;
    assert_eq!(state_of(&health, "full.simplified.all")["state"], "building");
    assert_eq!(state_of(&health, "full.exact.all")["state"], "failed");
    assert_eq!(state_of(&health, "full.exact.all")["reason"], "dataset full.exact.all: no room for its sort runs");
    assert_eq!(health["datasets"], 28);
    until("a group of another index open", || p.served().unwrap().search.json()["groups"].as_array().unwrap()
        .iter().any(|g| g["index"] == "admin-minimal.simplified.none" && g["state"] == "ready")).await;
    assert_eq!(fetch(&format!("{url}/search?q=testville&key=test&dataset=smallest")).await.status, 200);

    gate.open();
    assert_eq!(round.await.unwrap(), Ok(format!("build {DATE} is served ({DATE}-1)")));
    assert_eq!(fetch(&format!("{url}/search?q=testville&key=test")).await.status, 200);
    let health = health_search(&url).await;
    assert_eq!(health["ready"], 25);
    assert_eq!(state_of(&health, "full.simplified.all")["origin"], "built");
}

// A sync builds and opens the place search of the build it fetched before
// it switches to it: meanwhile searches answer from the build served, and
// the sync's status says what it does. Once it switches, they answer from
// the new build.
#[tokio::test]
async fn a_sync_opens_the_place_search_of_its_build_before_it_switches_to_it() {
    let bucket = sharing().await;
    let (data, files) = (ScratchDir::new(), ScratchDir::new());
    let gate = Gate::new();
    let waiting = gate.waiting();
    let mut cfg = config(&bucket, testland(&files));
    cfg.open_search = opener(move |_, dir| {
        if dir.to_str().unwrap().contains(NEXT) {
            waiting.wait();
        }
        Ok(())
    });
    let p = portal(data.path(), cfg);
    p.round(true).await.unwrap();
    let url = serve(router(p.clone(), db())).await;
    bucket.add_build(NEXT, BUILD_VERSION);
    bucket.set_latest(NEXT, Some(BUILD_VERSION));

    let round = tokio::spawn({
        let p = p.clone();
        async move { p.round(false).await }
    });
    until("the new build's place search opening", || {
        p.status().running.is_some_and(|r| r == format!("opening the place search of build {NEXT}-1"))
    }).await;
    let meanwhile = fetch(&format!("{url}/search?q=testville&key=test")).await;
    assert_eq!((meanwhile.status, meanwhile.header("x-geocoder-build")), (200, Some(DATE)));
    assert_eq!(p.served().unwrap().dir, format!("{DATE}-1"));

    gate.open();
    assert_eq!(round.await.unwrap(), Ok(format!("build {NEXT} is served ({NEXT}-1)")));
    let after = fetch(&format!("{url}/search?q=testville&key=test")).await;
    assert_eq!((after.status, after.header("x-geocoder-build")), (200, Some(NEXT)));
    assert_eq!(health_search(&url).await["ready"], 28);
}

// A group whose index fails to open refuses nothing: the build is switched
// to all the same, its other groups answer, and the next round tries the
// group again (the round that switched to the build doesn't, having just
// tried it), which then answers. A group whose open panics fails alone.
#[tokio::test]
async fn a_group_that_fails_doesnt_refuse_its_build_and_the_next_round_tries_it_again() {
    let bucket = sharing().await;
    let (data, files) = (ScratchDir::new(), ScratchDir::new());
    let (tries, panicked) = (Arc::new(AtomicUsize::new(0)), Arc::new(std::sync::atomic::AtomicBool::new(false)));
    let (counted, once) = (tries.clone(), panicked.clone());
    let mut cfg = config(&bucket, testland(&files));
    cfg.open_search = opener(move |group, dir| {
        if group == "full.exact.notable" && dir.ends_with("full.exact.notable") && counted.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err("the disk is full".to_string());
        }
        if group == "full.simplified.none" && !once.swap(true, Ordering::SeqCst) {
            panic!("a build that panics");
        }
        Ok(())
    });
    let p = portal(data.path(), cfg);
    let url = serve(router(p.clone(), db())).await;

    assert_eq!(p.round(true).await, Ok(format!("build {DATE} is served ({DATE}-1)")));

    assert_eq!(tries.load(Ordering::SeqCst), 1);
    assert!(!data.path().join(REFUSED).exists() || !std::fs::read_to_string(data.path().join(REFUSED)).unwrap().contains(DATE));
    let failed = fetch(&format!("{url}/search?q=testville&key=test&dataset=admin.exact.notable")).await;
    assert_eq!((failed.status, failed.json()["reason"].as_str()), (503, Some("dataset full.exact.notable: the disk is full")));
    let panicked = fetch(&format!("{url}/search?q=testville&key=test&dataset=full.simplified.none")).await;
    assert_eq!(panicked.status, 503);
    assert!(panicked.json()["reason"].as_str().unwrap().contains("a build that panics"), "{}", panicked.body);
    assert_eq!(fetch(&format!("{url}/search?q=testville&key=test")).await.status, 200);
    assert_eq!(fetch(&format!("{url}/reverse?lat=10.5&lon=20.5&key=test&dataset=admin.exact.notable")).await.status, 200);

    assert_eq!(p.round(false).await, Ok(format!("build {DATE} is served ({DATE}-1)")));

    assert_eq!(tries.load(Ordering::SeqCst), 2);
    for dataset in ["admin.exact.notable", "full.simplified.none"] {
        assert_eq!(fetch(&format!("{url}/search?q=testville&key=test&dataset={dataset}")).await.status, 200, "{dataset}");
    }
    assert_eq!(health_search(&url).await["ready"], 28);
}

// A build served again (latest.json moved back to it) opens its place
// search over the indexes its dir holds before it is switched to: each is
// found built from its files, none built again. A build pruned takes its
// indexes along.
#[tokio::test]
async fn a_build_served_again_opens_the_indexes_it_holds_and_a_pruned_one_takes_them_along() {
    let bucket = sharing().await;
    let (data, files) = (ScratchDir::new(), ScratchDir::new());
    let p = portal(data.path(), config(&bucket, testland(&files)));
    p.round(true).await.unwrap();
    let url = serve(router(p.clone(), db())).await;
    bucket.add_build(NEXT, BUILD_VERSION);
    bucket.set_latest(NEXT, Some(BUILD_VERSION));
    assert_eq!(p.round(false).await, Ok(format!("build {NEXT} is served ({NEXT}-1)")));

    bucket.set_latest(DATE, Some(BUILD_VERSION));
    assert_eq!(p.round(false).await, Ok(format!("build {DATE} is served ({DATE}-1)")));

    let health = health_search(&url).await;
    assert_eq!(health["ready"], 28);
    for group in health["groups"].as_array().unwrap() {
        assert_eq!(group["origin"], "current", "{group}");
    }
    let again = fetch(&format!("{url}/search?q=testville&key=test")).await;
    assert_eq!((again.status, again.header("x-geocoder-build")), (200, Some(DATE)));
    // Once THIRD is served, the build served before it is NEXT, the newer.
    let first = data.path().join(BUILDS).join(format!("{DATE}-1")).join(sync::SEARCH);
    assert!(first.is_dir());
    bucket.add_build(THIRD, BUILD_VERSION);
    bucket.set_latest(THIRD, Some(BUILD_VERSION));
    assert_eq!(p.round(false).await, Ok(format!("build {THIRD} is served ({THIRD}-1)")));
    assert!(!first.exists());
    assert!(index_dir(data.path(), &format!("{THIRD}-1"), "full.simplified.all").is_dir());
}

// The index builds run at most search_jobs at once, each on a thread at the
// lowest priority, the costliest group first.
#[tokio::test]
async fn index_builds_run_a_few_at_once_at_the_lowest_priority_the_costliest_first() {
    let bucket = sharing().await;
    let (files_of, manifest) = manifest_of(&bucket).await;
    let plan = catalog::plan(&files_of, &manifest).unwrap();
    let mut costliest: Vec<&catalog::Group> = plan.groups.iter().collect();
    costliest.sort_by(|a, b| b.cost.cmp(&a.cost).then_with(|| a.name.cmp(&b.name)));
    let costliest: Vec<String> = costliest.into_iter().map(|g| g.name.clone()).collect();
    for jobs in [1, 3] {
        let (data, files) = (ScratchDir::new(), ScratchDir::new());
        let (order, running, most) = (Arc::new(Mutex::new(Vec::new())), Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let (o, r, m) = (order.clone(), running.clone(), most.clone());
        let mut cfg = config(&bucket, testland(&files));
        cfg.search_jobs = jobs;
        let inner = cfg.open_search.clone();
        cfg.open_search = Arc::new(move |geo: Arc<query_server::Index>, stamps: &Stamps, dir: &Path, index: &Path| {
            let group = index.file_name().unwrap().to_str().unwrap().to_string();
            let mut seen = o.lock().unwrap();
            if !seen.iter().any(|(g, _)| *g == group) {
                #[cfg(target_os = "linux")]
                // SAFETY: getpriority reads nothing of this process's memory.
                let nice = unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) };
                #[cfg(not(target_os = "linux"))]
                let nice = 19;
                seen.push((group, nice));
            }
            drop(seen);
            m.fetch_max(r.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(20));
            let opened = inner(geo, stamps, dir, index);
            r.fetch_sub(1, Ordering::SeqCst);
            opened
        });
        let p = portal(data.path(), cfg);

        p.round(true).await.unwrap();

        let order = order.lock().unwrap().clone();
        assert!(order.iter().all(|(_, nice)| *nice == 19), "{order:?}");
        if jobs == 1 {
            assert_eq!(order.into_iter().map(|(g, _)| g).collect::<Vec<_>>(), costliest);
        }
        assert_eq!(most.load(Ordering::SeqCst), jobs, "jobs {jobs}");
    }
}

// The index builds at once are PORTAL_SEARCH_JOBS's count, 4 without one
// (or with one that is no count above 0), and never more than the CPUs the
// portal may run on.
#[test]
fn the_index_builds_at_once_are_bounded_by_the_cpus() {
    assert_eq!(crate::portal::search_jobs(None, 64), crate::portal::SEARCH_JOBS);
    assert_eq!(crate::portal::SEARCH_JOBS, 4);
    assert_eq!(crate::portal::search_jobs(Some("6"), 64), 6);
    assert_eq!(crate::portal::search_jobs(Some(" 2 "), 64), 2);
    for wrong in ["0", "-1", "many", ""] {
        assert_eq!(crate::portal::search_jobs(Some(wrong), 64), 4, "{wrong:?}");
    }
    assert_eq!(crate::portal::search_jobs(Some("6"), 2), 2);
    assert_eq!(crate::portal::search_jobs(None, 1), 1);
    assert_eq!(crate::portal::search_jobs(None, 0), 1);
}

/// The index files of every group of the build dir `build`: by group and
/// name, each file's inode and bytes.
fn index_files(data: &Path, build: &str) -> BTreeMap<(String, String), (u64, Vec<u8>)> {
    let mut out = BTreeMap::new();
    for group in std::fs::read_dir(data.join(BUILDS).join(build).join(sync::SEARCH)).unwrap() {
        let group = group.unwrap();
        for file in std::fs::read_dir(group.path()).unwrap() {
            let file = file.unwrap();
            let name = file.file_name().into_string().unwrap();
            if file.file_type().unwrap().is_file() && ![META_FILE, "dataset_hashes.json"].contains(&name.as_str()) {
                out.insert((group.file_name().into_string().unwrap(), name), (inode(&file.path()), std::fs::read(file.path()).unwrap()));
            }
        }
    }
    out
}

// Once the indexes are built, the files of one name that hold the same
// bytes in two groups' dirs are hard links of one, and those of other bytes
// are not: for groups of one dataset everywhere every file of a name is
// one, and for datasets of their own (their countries named by their ids)
// only those alike are. No temporary file stays.
#[tokio::test]
async fn the_index_files_alike_in_two_groups_are_hard_links_of_one() {
    let bucket = sharing().await;
    let build = format!("{DATE}-1");
    let (alike, files) = (ScratchDir::new(), ScratchDir::new());
    portal(alike.path(), config(&bucket, testland(&files))).round(true).await.unwrap();
    let data = ScratchDir::new();
    let (_fixtures, own) = fixtures();
    portal(data.path(), config(&bucket, own)).round(true).await.unwrap();

    let linked = index_files(alike.path(), &build);
    assert_eq!(linked.keys().map(|(g, _)| g.clone()).collect::<BTreeSet<_>>().len(), 12);
    let mut by_name: BTreeMap<&str, BTreeSet<u64>> = BTreeMap::new();
    for ((_, name), (ino, _)) in &linked {
        by_name.entry(name.as_str()).or_default().insert(*ino);
    }
    assert!(by_name.contains_key("places.fst") && by_name.contains_key("places.bnd"), "{:?}", by_name.keys());
    assert!(by_name.values().all(|inodes| inodes.len() == 1), "{by_name:?}");
    let own = index_files(data.path(), &build);
    for ((ga, name), (ia, ba)) in &own {
        for ((gb, other), (ib, bb)) in &own {
            if name == other {
                assert_eq!(ia == ib, ba == bb, "{ga}/{name} and {gb}/{other}");
            }
        }
    }
    assert!(own.values().map(|(ino, _)| ino).collect::<BTreeSet<_>>().len() < own.len());
    assert_eq!(leftovers(&data.path().join(BUILDS).join(&build)), Vec::<String>::new());
}

// /datasets names each dataset's index (its group) and whether the build
// publishes it; /health tells, of each group, its datasets, its state and
// since when, whether it was built or found built from these files, and
// for a group whose index the build publishes, how it compares with it: a
// build that publishes none of its files (the bucket's) has them all
// unpublished. Opened again at a start, every index is found built.
#[tokio::test]
async fn datasets_and_health_tell_each_datasets_index() {
    let bucket = sharing().await;
    let (data, files) = (ScratchDir::new(), ScratchDir::new());
    let p = portal(data.path(), config(&bucket, testland(&files)));
    p.round(true).await.unwrap();
    let url = serve(router(p.clone(), db())).await;
    let names = group_names(&bucket).await;

    let datasets = fetch(&format!("{url}/datasets")).await.json();
    for d in datasets["datasets"].as_array().unwrap() {
        let shipped = ["full.simplified.all", "full.exact.all"].contains(&d["search"]["index"].as_str().unwrap());
        assert_eq!(d["search"]["shipped"], shipped, "{d}");
        assert!(names.iter().any(|n| d["search"]["index"] == n.as_str()), "{d}");
    }
    assert_eq!(datasets["datasets"][0]["search"], serde_json::json!({ "index": "full.exact.none", "shipped": false }));
    let health = health_search(&url).await;
    assert_eq!((&health["ready"], &health["datasets"]), (&28.into(), &28.into()));
    assert_eq!(health["groups"].as_array().unwrap().iter().map(|g| g["index"].as_str().unwrap().to_string()).collect::<Vec<_>>(), names);
    let group = state_of(&health, "full.simplified.major");
    assert_eq!((&group["state"], &group["origin"], &group["shipped"]), (&"ready".into(), &"built".into(), &serde_json::Value::Null));
    assert_eq!(group["datasets"], serde_json::json!(["full.simplified.major", "no-addresses.simplified.major", "admin.simplified.major"]));
    assert!(group["since"].as_str().is_some() && group["seconds"].as_f64().is_some(), "{group}");
    let t = template();
    let published: Vec<serde_json::Value> = t["search_index"]["files"].as_array().unwrap().iter()
        .chain(t["search_index"]["optional_files"].as_array().unwrap())
        .filter(|n| *n != META_FILE)
        .filter(|n| index_dir(data.path(), &format!("{DATE}-1"), "full.simplified.all").join(n.as_str().unwrap()).exists())
        .cloned()
        .collect();
    assert!(!published.is_empty());
    assert_eq!(state_of(&health, "full.simplified.all")["shipped"], serde_json::json!({
        "dir": "planet/search/full/simplified/all", "same": false, "differ": [], "unpublished": published,
    }));

    let again = portal(data.path(), config(&bucket, testland(&files)));
    again.round(true).await.unwrap();
    let url = serve(router(again, db())).await;
    for group in health_search(&url).await["groups"].as_array().unwrap() {
        assert_eq!((&group["state"], &group["origin"]), (&"ready".into(), &"current".into()), "{group}");
    }
}

// A build that publishes the index of a shipped selection is told the same
// as it where the index built here holds the published bytes, and how it
// differs where it doesn't.
#[tokio::test]
async fn a_shipped_index_is_compared_with_the_one_built_here() {
    let bucket = sharing().await;
    let (data, files) = (ScratchDir::new(), ScratchDir::new());
    let p = portal(data.path(), config(&bucket, testland(&files)));
    p.round(true).await.unwrap();
    let url = serve(router(p.clone(), db())).await;
    let built = |group: &str| -> Vec<(String, String, u64)> {
        let dir = index_dir(data.path(), &format!("{DATE}-1"), group);
        let mut out: Vec<(String, String, u64)> = std::fs::read_dir(&dir).unwrap()
            .map(|e| e.unwrap())
            .filter(|e| e.file_type().unwrap().is_file())
            .map(|e| e.file_name().into_string().unwrap())
            .filter(|n| ![META_FILE, "dataset_hashes.json"].contains(&n.as_str()))
            .map(|n| {
                let bytes = std::fs::read(dir.join(&n)).unwrap();
                (n, query_server::lower_hex(&<sha2::Sha256 as sha2::Digest>::digest(&bytes)), bytes.len() as u64)
            })
            .collect();
        out.sort();
        out
    };
    let (simplified, exact) = (built("full.simplified.all"), built("full.exact.all"));
    bucket.edit_build(DATE, |m| {
        for (group, files) in [("simplified", &simplified), ("exact", &exact)] {
            for (name, sha256, size) in files.iter() {
                let sha256 = if group == "exact" && name == "places.bnd" { "00".to_string() } else { sha256.clone() };
                m["files"][format!("planet/search/full/{group}/all/{name}")] = serde_json::json!({ "size_zst": size, "size_raw": size, "sha256": sha256 });
            }
        }
    });

    assert_eq!(p.round(false).await, Ok(format!("build {DATE} is served ({DATE}-2)")));

    let health = health_search(&url).await;
    assert_eq!(state_of(&health, "full.simplified.all")["shipped"],
               serde_json::json!({ "dir": "planet/search/full/simplified/all", "same": true, "differ": [], "unpublished": [] }));
    assert_eq!(state_of(&health, "full.exact.all")["shipped"],
               serde_json::json!({ "dir": "planet/search/full/exact/all", "same": false, "differ": ["places.bnd"], "unpublished": [] }));
}

// A dataset's place search opens over the files its load stamped: one
// replaced in its dir since it loaded fails its group (it would build an
// index of other files than those loaded), and the build is served all the
// same.
#[tokio::test]
async fn a_dataset_whose_files_changed_since_its_load_fails_its_group() {
    let bucket = sharing().await;
    let (data, files) = (ScratchDir::new(), ScratchDir::new());
    let mut cfg = config(&bucket, testland(&files));
    cfg.open_search = opener(|group, dir| {
        if group == "full.simplified.notable" && dir.ends_with("no-addresses.simplified.notable") {
            let file = dir.join("place_nodes.bin");
            let copy = dir.join("place_nodes.bin.copy");
            std::fs::copy(&file, &copy).unwrap();
            std::fs::rename(&copy, &file).unwrap();
        }
        Ok(())
    });
    let p = portal(data.path(), cfg);

    assert_eq!(p.round(true).await, Ok(format!("build {DATE} is served ({DATE}-1)")));

    let health = p.served().unwrap().search.json();
    let group = state_of(&health, "full.simplified.notable");
    assert_eq!(group["state"], "failed");
    assert!(group["reason"].as_str().unwrap().starts_with("dataset no-addresses.simplified.notable: The dataset in "), "{group}");
    assert!(group["reason"].as_str().unwrap().ends_with("changed since it was loaded: load it again"), "{group}");
}

// Searches run on places of their own, beside the lookups': while every
// lookup's place is held (a few /polygons of exact boundaries), a search is
// answered; while every search's place is held, a search waits its turn and
// /reverse is answered, and the search once a place is free.
#[tokio::test]
async fn searches_have_places_of_their_own() {
    let bucket = sharing().await;
    let (data, files) = (ScratchDir::new(), ScratchDir::new());
    let mut two = config(&bucket, testland(&files));
    two.searches_at_once = 2;
    let p = portal(data.path(), two);
    p.round(true).await.unwrap();
    let url = serve(router(p.clone(), db())).await;
    assert_eq!((p.searches.available_permits(), Config::from_env().searches_at_once), (2, SEARCHES_AT_ONCE));

    let lookups = p.lookups.clone().try_acquire_many_owned(LOOKUPS_AT_ONCE as u32).unwrap();
    assert_eq!(fetch(&format!("{url}/search?q=testville&key=test")).await.status, 200);
    drop(lookups);
    let searches = p.searches.clone().try_acquire_many_owned(2).unwrap();
    let waiting = tokio::spawn({
        let at = format!("{url}/search?q=testville&key=test");
        async move { fetch(&at).await.status }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!waiting.is_finished());
    assert_eq!(fetch(&format!("{url}/reverse?lat=10.5&lon=20.5&key=test")).await.status, 200);

    drop(searches);
    assert_eq!(waiting.await.unwrap(), 200);
    assert_eq!(p.searches.available_permits(), 2);
}
