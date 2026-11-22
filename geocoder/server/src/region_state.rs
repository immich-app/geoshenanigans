// Region state coordinator: holds the current admin-supplied
// configuration, the cached configurations.json from the build, and
// the per-region download mutexes. Triggers downloads, installs
// completed regions into the live MultiIndex via arc-swap.
//
// Two flows feed this:
//
// 1. Configuration push (PUT /admin/configuration): full snapshot of what
//    regions the user wants, plus mode/quality/poi_tier/water. Server preloads
//    regions in the snapshot that aren't already loaded, and where the
//    selection changed downloads each loaded region again for it.
//
// 2. Lazy on-demand: a /reverse request lands on a continent we don't
//    have, the request handler calls `ensure_loaded` which blocks
//    until the download finishes.
//
// Per-region mutex serialises concurrent ensure_loaded calls so each
// region is downloaded at most once even under request fan-in. A download
// runs in a task of its own, which holds the mutex to its end: a request
// that gives up leaves it to run on.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::downloader::{
    check_selection, download_region, Configurations, DownloadError, DownloadProgress, RegionSelection, COMPONENT_AXES,
};
use query_server::MultiIndex;

/// How long a fetched configurations.json is trusted before latest.json
/// is re-resolved. CI keeps full builds for 3 days, so a server that
/// never re-resolved would 404 on every new download from day 4.
const CONFIGURATIONS_TTL: Duration = Duration::from_secs(3600);

#[cfg(test)]
thread_local! {
    /// Set by a test: run by the next download of a region on this thread
    /// once it is in, before it loads the region dir, which it is given.
    static ONCE_DOWNLOADED: std::cell::Cell<Option<Box<dyn FnOnce(&std::path::Path)>>> = const { std::cell::Cell::new(None) };
}

/// The selection a server downloads. A mode, quality, poi_tier or water
/// value its build has no component for fails each download, naming the
/// values the build offers; a PUT naming one is refused when the build's
/// manifest is at hand.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Configuration {
    /// Base URL for the geocoder build artifacts. Latest.json + each
    /// build's configurations.json + its files' .zst and transport units
    /// all hang off this.
    pub base_url: String,
    /// Address detail level — must match a `mode.*` component.
    pub mode: String,
    /// Boundary tier ("simplified" or "exact"). Must match a
    /// `quality.*` component.
    pub quality: String,
    /// POI tier ("none", "major", "notable", "all"). Must match a
    /// `poi_tier.*` component.
    pub poi_tier: String,
    /// Bodies of water ("none", "major", "all", "all-exact"), in each
    /// region's water/ dir. Must match a `water.*` component; "none", the
    /// axis's default, where the configuration names none.
    #[serde(default = "water_none")]
    pub water: String,
    /// Regions to preload on configuration push. Anything not listed
    /// here can still be downloaded lazily on first request.
    #[serde(default)]
    pub preload: Vec<String>,
}

impl Default for Configuration {
    fn default() -> Self {
        Configuration {
            base_url: "https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder"
                .to_string(),
            mode: "admin".to_string(),
            quality: "simplified".to_string(),
            poi_tier: "none".to_string(),
            water: water_none(),
            preload: Vec::new(),
        }
    }
}

/// The water axis's default: no water.
fn water_none() -> String {
    "none".to_string()
}

impl Configuration {
    pub fn selection(&self, region: &str) -> RegionSelection {
        RegionSelection {
            region: region.to_string(),
            mode: self.mode.clone(),
            quality: self.quality.clone(),
            poi_tier: self.poi_tier.clone(),
            water: self.water.clone(),
        }
    }
}

pub struct RegionState {
    pub multi_index: Arc<MultiIndex>,
    pub root_dir: PathBuf,
    pub config_path: PathBuf,
    pub config: Mutex<Configuration>,
    /// Cached configurations.json for the latest build under
    /// `config.base_url`, with when it was fetched. Loaded lazily on first
    /// download, dropped when the configuration is updated (a config
    /// change might point at a different bucket) and re-resolved once it
    /// is older than `configurations_ttl`.
    pub configurations: Mutex<Option<(Instant, Configurations)>>,
    pub configurations_ttl: Duration,
    /// Per-region serialisation. Concurrent requests for the same region
    /// share one inner Mutex; only the leader actually downloads, the
    /// rest wait on the lock and find the region already loaded.
    pub region_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    pub http: reqwest::Client,
}

impl RegionState {
    pub fn new(multi_index: Arc<MultiIndex>, root_dir: PathBuf) -> Self {
        let config_path = root_dir.join("configuration.json");
        // Load persisted configuration if present; new deployments
        // start from defaults until one is PUT to /admin/configuration.
        let config = match std::fs::read_to_string(&config_path) {
            Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
            Err(_) => Configuration::default(),
        };
        RegionState {
            multi_index,
            root_dir,
            config_path,
            config: Mutex::new(config),
            configurations: Mutex::new(None),
            configurations_ttl: CONFIGURATIONS_TTL,
            region_locks: Mutex::new(HashMap::new()),
            http: reqwest::Client::builder()
                .user_agent("geocoder-server/0.1")
                .build()
                .expect("failed to build reqwest client"),
        }
    }

    /// Replace the active configuration. Persists to disk and kicks
    /// off background downloads: of each preload region not loaded yet,
    /// and, where the selection (mode, quality, poi_tier or water) changed,
    /// of each loaded region again, for the new selection's files, which
    /// load in place of the index it had once they are in. Till then, and
    /// where the download or the load fails, the region answers as before.
    /// Returns immediately — downloads run as tokio tasks.
    ///
    /// Refused, changing nothing, when the manifest at hand has no
    /// component for its mode, quality, poi_tier or water.
    pub async fn set_configuration(self: &Arc<Self>, new_cfg: Configuration) -> Result<(), DownloadError> {
        self.configure(new_cfg).await.map(drop)
    }

    /// set_configuration, returning the tasks it started.
    async fn configure(self: &Arc<Self>, new_cfg: Configuration) -> Result<Vec<tokio::task::JoinHandle<()>>, DownloadError> {
        // The manifest at hand: the cached one, when it is of this base_url
        // and within its TTL. Without one the first download reports a
        // value the build lacks. The check reads no region.
        let before = self.config.lock().await.clone();
        let same_bucket = before.base_url == new_cfg.base_url;
        if let Some((fetched_at, manifest)) = self.configurations.lock().await.as_ref() {
            if same_bucket && fetched_at.elapsed() < self.configurations_ttl {
                check_selection(manifest, &new_cfg.selection(""))?;
            }
        }

        // Persist + swap in atomically. Disk write is best-effort —
        // we still apply the new config in memory if disk is full.
        let json = serde_json::to_string_pretty(&new_cfg).unwrap_or_default();
        let _ = tokio::fs::write(&self.config_path, json).await;

        *self.config.lock().await = new_cfg.clone();
        // Force re-fetch of configurations.json on next download (the
        // referenced build may have changed).
        *self.configurations.lock().await = None;

        // Spawn preload downloads, and where the selection changed one of
        // each other region, which downloads it again if it is loaded. Each
        // task decides under the region's mutex (download_if), so a download
        // of the selection before that holds it ends first, and its region
        // is then downloaded again; even if a /reverse request beats the
        // preload to the punch, only one download runs.
        let changed = COMPONENT_AXES.iter()
            .any(|axis| before.selection("").axis(axis) != new_cfg.selection("").axis(axis));
        let mut regions: Vec<(String, bool)> = Vec::new();
        for region in &new_cfg.preload {
            if !regions.iter().any(|(r, _)| r == region) {
                regions.push((region.clone(), true));
            }
        }
        if changed {
            for c in &self.multi_index.continents {
                if !regions.iter().any(|(r, _)| r == c.name) {
                    regions.push((c.name.to_string(), false));
                }
            }
        }
        let mut tasks = Vec::new();
        for (region, preload) in regions {
            let s = self.clone();
            tasks.push(tokio::spawn(async move {
                if let Err(e) = s.download_if(&region, preload, changed).await {
                    eprintln!("{} {}: {}", if preload { "preload" } else { "download" }, region, e);
                }
            }));
        }
        Ok(tasks)
    }

    pub async fn get_configuration(&self) -> Configuration {
        self.config.lock().await.clone()
    }

    /// Fetch (or return cached) configurations.json for the current
    /// configuration's base_url + latest build date.
    async fn ensure_configurations(&self) -> Result<Configurations, DownloadError> {
        if let Some((fetched_at, c)) = self.configurations.lock().await.as_ref() {
            if fetched_at.elapsed() < self.configurations_ttl {
                return Ok(c.clone());
            }
        }

        let cfg = self.config.lock().await.clone();
        // Resolve latest build date via latest.json.
        let latest_url = format!("{}/builds/latest.json", cfg.base_url.trim_end_matches('/'));
        let latest: serde_json::Value = self.http.get(&latest_url).send().await
            .map_err(|e| DownloadError::Http(format!("{}: {}", latest_url, e)))?
            .error_for_status().map_err(|e| DownloadError::Http(format!("{}: {}", latest_url, e)))?
            .json().await
            .map_err(|e| DownloadError::Http(format!("{}: parse latest.json: {}", latest_url, e)))?;
        let date = latest.get("latest").and_then(|v| v.as_str())
            .ok_or_else(|| DownloadError::Http(format!("{}: missing 'latest' field", latest_url)))?;

        let cfg_url = format!("{}/builds/{}/configurations.json", cfg.base_url.trim_end_matches('/'), date);
        let configurations: Configurations = self.http.get(&cfg_url).send().await
            .map_err(|e| DownloadError::Http(format!("{}: {}", cfg_url, e)))?
            .error_for_status().map_err(|e| DownloadError::Http(format!("{}: {}", cfg_url, e)))?
            .json().await
            .map_err(|e| DownloadError::Http(format!("{}: parse: {}", cfg_url, e)))?;

        *self.configurations.lock().await = Some((Instant::now(), configurations.clone()));
        Ok(configurations)
    }

    /// Block until `region` is loaded into the MultiIndex. If it
    /// isn't already, downloads its files (single-flighted), then
    /// installs the new Index. Subsequent calls for the same region
    /// after a successful download return immediately.
    ///
    /// The download and the install run in a task of their own, which
    /// holds the region's mutex until they end: a caller that gives up (a
    /// request whose client went, its future dropped) leaves them to run
    /// on. Dropped part way, a download would let go of the dir's lock
    /// while what it started on threads of their own (a unit's decode, a
    /// file's writer) went on writing the dir.
    pub async fn ensure_loaded(self: &Arc<Self>, region: &str) -> Result<(), DownloadError> {
        self.download_if(region, true, false).await
    }

    /// Downloads `region` and installs it, as ensure_loaded does, where it
    /// isn't loaded and `unloaded`, or is and `loaded`; else returns. A
    /// region the flat layout's one slot holds is never downloaded again:
    /// its dir was laid out by hand.
    async fn download_if(self: &Arc<Self>, region: &str, unloaded: bool, loaded: bool) -> Result<(), DownloadError> {
        // Per-region serialisation. Acquire the inner mutex so only
        // one concurrent request runs the download below; the rest
        // wait, find the region already installed, and return.
        let inner = {
            let mut g = self.region_locks.lock().await;
            g.entry(region.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let guard = inner.lock_owned().await;

        // Bail early if `region` isn't a known continent.
        let Some(slot) = self.multi_index.continents.iter().find(|c| c.name == region) else {
            return Err(DownloadError::UnknownRegion(region.to_string()));
        };
        // Checked after acquiring the lock: an earlier waiter may have
        // just finished the download.
        let wanted = if slot.index.load().is_some() { loaded && slot.continent.is_some() } else { unloaded };
        if !wanted {
            return Ok(());
        }

        let (state, region) = (self.clone(), region.to_string());
        tokio::spawn(async move {
            let _guard = guard;
            state.download_and_install(&region).await
        })
        .await
        .map_err(|e| DownloadError::Io(format!("download task: {}", e)))?
    }

    /// Downloads `region` and installs it: the task of download_if. A region
    /// loaded already answers from the files its index mapped until the new
    /// index is installed, and goes on doing so where the download or the
    /// load fails: Index::load maps each file it reads and opens none by its
    /// path later, and a download writes no file in place, but renames a new
    /// one over it or removes it, which leaves a mapped file as it was.
    async fn download_and_install(&self, region: &str) -> Result<(), DownloadError> {
        let configurations = self.ensure_configurations().await?;
        let cfg = self.config.lock().await.clone();
        let sel = cfg.selection(region);

        let region_owned = region.to_string();
        download_region(
            &self.http,
            &cfg.base_url,
            &configurations,
            &sel,
            &self.root_dir,
            move |p: DownloadProgress| {
                eprintln!(
                    "[{}] {}/{} files, {}/{} bytes",
                    region_owned, p.files_done, p.files_total, p.bytes_done, p.bytes_total
                );
            },
        ).await?;

        #[cfg(test)]
        if let Some(meanwhile) = ONCE_DOWNLOADED.take() {
            meanwhile(&self.root_dir.join(region));
        }
        // Another server's download into the dir may begin once this one let
        // go of it: the dir loads once that one ended, where it finished. The
        // load waits for it on a thread of its own, not on one of the
        // runtime's, which answer the queries meanwhile.
        let (multi_index, name) = (self.multi_index.clone(), region.to_string());
        tokio::task::spawn_blocking(move || multi_index.install_region(&name))
            .await
            .map_err(|e| DownloadError::Io(format!("load task: {}", e)))?
            .map_err(DownloadError::UnknownRegion)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scratch_dir::ScratchDir;
    use crate::test_bucket::{self, TestBucket};
    use query_server::{BUILD_STAMP, DOWNLOAD_LOCK, FILE_LEDGER};
    use std::cell::RefCell;
    use std::collections::BTreeSet;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FakeBucket {
        latest: std::sync::Mutex<String>,
        fetches: AtomicUsize,
    }

    // Serves builds/latest.json and builds/<date>/configurations.json.
    async fn serve(bucket: Arc<FakeBucket>) -> String {
        use axum::extract::{Path, State};
        use axum::routing::get;
        let app = axum::Router::new()
            .route("/builds/latest.json", get(|State(b): State<Arc<FakeBucket>>| async move {
                format!("{{\"latest\":\"{}\"}}", b.latest.lock().unwrap())
            }))
            .route("/builds/{date}/configurations.json", get(|State(b): State<Arc<FakeBucket>>, Path(date): Path<String>| async move {
                b.fetches.fetch_add(1, Ordering::SeqCst);
                format!("{{\"build\":{{\"date\":\"{}\"}},\"components\":{{}},\"files\":{{}}}}", date)
            }))
            .with_state(bucket);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{}", addr)
    }

    /// The state lives in the returned dir, which must outlive it.
    async fn state_for(base_url: String, ttl: std::time::Duration) -> (ScratchDir, RegionState) {
        let root = ScratchDir::new();
        let mi = MultiIndex::load(root.path().to_str().unwrap(), 17, 10, 660.0).unwrap();
        let mut state = RegionState::new(Arc::new(mi), root.path().to_path_buf());
        state.config.get_mut().base_url = base_url;
        state.configurations_ttl = ttl;
        (root, state)
    }

    #[tokio::test]
    async fn configurations_are_cached_within_ttl() {
        let bucket = Arc::new(FakeBucket { latest: "2026-01-01".to_string().into(), fetches: AtomicUsize::new(0) });
        let (_root, state) = state_for(serve(bucket.clone()).await, std::time::Duration::from_secs(3600)).await;

        state.ensure_configurations().await.unwrap();
        *bucket.latest.lock().unwrap() = "2026-01-02".to_string();
        let second = state.ensure_configurations().await.unwrap();

        assert_eq!(bucket.fetches.load(Ordering::SeqCst), 1);
        assert_eq!(second.build.date, "2026-01-01");
    }

    #[tokio::test]
    async fn configurations_follow_latest_after_ttl() {
        let bucket = Arc::new(FakeBucket { latest: "2026-01-01".to_string().into(), fetches: AtomicUsize::new(0) });
        let (_root, state) = state_for(serve(bucket.clone()).await, std::time::Duration::ZERO).await;

        state.ensure_configurations().await.unwrap();
        *bucket.latest.lock().unwrap() = "2026-01-02".to_string();
        let second = state.ensure_configurations().await.unwrap();

        assert_eq!(bucket.fetches.load(Ordering::SeqCst), 2);
        assert_eq!(second.build.date, "2026-01-02");
    }

    // A configuration.json kept from an earlier build, naming a value this
    // one lacks, loads as it is; its downloads fail naming the value and the
    // build's, fetching nothing into the region dir.
    #[tokio::test]
    async fn a_persisted_value_the_build_lacks_fails_its_download_naming_it() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let root = ScratchDir::new();
        let persisted = Configuration { base_url: bucket.url.clone(), quality: "q2.5".into(), ..Configuration::default() };
        std::fs::write(root.path().join("configuration.json"), serde_json::to_string(&persisted).unwrap()).unwrap();
        let mi = MultiIndex::load(root.path().to_str().unwrap(), 17, 10, 660.0).unwrap();
        let state = Arc::new(RegionState::new(Arc::new(mi), root.path().to_path_buf()));

        let got = state.ensure_loaded("europe").await.map_err(|e| e.to_string());

        assert_eq!(got, Err("this build has no quality \"q2.5\"; it offers quality: exact, simplified".to_string()));
        assert_eq!(bucket.files_served(), 0);
        assert!(!root.path().join("europe").exists());
    }

    // With the build's manifest at hand, a configuration naming a value it
    // lacks is refused before it replaces or persists anything.
    #[tokio::test]
    async fn a_configuration_naming_a_value_the_manifest_at_hand_lacks_is_refused() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (root, state) = state_for(bucket.url.clone(), std::time::Duration::from_secs(3600)).await;
        let state = Arc::new(state);
        state.ensure_configurations().await.unwrap();
        let before = state.get_configuration().await;

        for (cfg, error) in [
            (Configuration { quality: "q2.5".into(), ..before.clone() },
             "this build has no quality \"q2.5\"; it offers quality: exact, simplified"),
            (Configuration { mode: "ful".into(), ..before.clone() },
             "this build has no mode \"ful\"; it offers mode: admin, admin-minimal, full, no-addresses"),
            (Configuration { water: "lakes".into(), ..before.clone() },
             "this build has no water \"lakes\"; it offers water: all, all-exact, major, none"),
        ] {
            let got = state.set_configuration(cfg).await.map_err(|e| e.to_string());

            assert_eq!(got, Err(error.to_string()));
        }
        let json = |c: &Configuration| serde_json::to_value(c).unwrap();
        assert_eq!(json(&state.get_configuration().await), json(&before));
        assert!(!root.path().join("configuration.json").exists());

        state.set_configuration(Configuration { quality: "exact".into(), ..before.clone() }).await.unwrap();
        assert_eq!(state.get_configuration().await.quality, "exact");
        state.set_configuration(Configuration { water: "all-exact".into(), ..before.clone() }).await.unwrap();
        assert_eq!(state.get_configuration().await.selection("europe").water, "all-exact");
    }

    // A configuration's water is a value of the water axis, "none" where it
    // names none: in a PUT's body as in a configuration.json kept on disk.
    #[test]
    fn a_configuration_naming_no_water_takes_none() {
        let body = r#"{"base_url": "http://bucket", "mode": "full", "quality": "exact", "poi_tier": "all"}"#;
        let named = r#"{"base_url": "http://bucket", "mode": "full", "quality": "exact", "poi_tier": "all", "water": "major"}"#;

        let water = |json: &str| serde_json::from_str::<Configuration>(json).unwrap().selection("europe").water;

        assert_eq!((water(body), water(named)), ("none".to_string(), "major".to_string()));
        assert_eq!(Configuration::default().water, "none");
        assert_eq!(serde_json::to_value(Configuration::default()).unwrap()["water"], "none");
    }

    // The manifest at hand is the cached one of the configuration's own
    // bucket, within its TTL. Another bucket's build, or a newer build than
    // a manifest past its TTL, may offer a value that manifest lacks: a PUT
    // naming one is accepted, for its downloads to fetch the build they use.
    #[tokio::test]
    async fn a_configuration_is_checked_only_against_a_fresh_manifest_of_its_bucket() {
        let mut components = test_bucket::template_components();
        components["quality.fine"] = components["quality.simplified"].clone();
        let offering = test_bucket::serve(components).await;
        let lacking = test_bucket::serve(test_bucket::template_components()).await;

        // A PUT moving to another bucket, with this one's manifest at hand.
        let (_root, state) = state_for(lacking.url.clone(), Duration::from_secs(3600)).await;
        let state = Arc::new(state);
        state.ensure_configurations().await.unwrap();
        let moved = Configuration { base_url: offering.url.clone(), quality: "fine".into(), ..state.get_configuration().await };
        state.set_configuration(moved).await.unwrap();
        assert_eq!(state.get_configuration().await.quality, "fine");

        // A PUT once the manifest at hand is past its TTL.
        let (_root, state) = state_for(offering.url.clone(), Duration::ZERO).await;
        let state = Arc::new(state);
        *state.configurations.lock().await = Some((Instant::now(), lacking.manifest.clone()));
        let fine = Configuration { quality: "fine".into(), ..state.get_configuration().await };
        state.set_configuration(fine).await.unwrap();
        assert_eq!(state.get_configuration().await.quality, "fine");
    }

    // Once its download is in, a server loads the region dir under a shared
    // lock on its DOWNLOAD_LOCK, which waits while a download of another
    // server on the data dir holds the lock. It waits on a thread of its
    // own, and the runtime's threads go on with other tasks meanwhile. Here
    // another open takes the lock as the download let go of it and keeps it
    // a second, on a runtime of one thread: a task due in 100 ms runs while
    // the lock is held, and the load returns once it was let go of.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_load_waiting_for_another_servers_download_leaves_the_runtime_free() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (_root, state) = state_for(bucket.url.clone(), Duration::from_secs(3600)).await;
        let state = Arc::new(state);
        let meanwhile = Arc::new(std::sync::Mutex::new(None));
        let started = meanwhile.clone();
        ONCE_DOWNLOADED.set(Some(Box::new(move |dir: &std::path::Path| {
            let other = std::fs::File::open(dir.join(DOWNLOAD_LOCK)).unwrap();
            other.lock().unwrap();
            let let_go = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_secs(1));
                let at = Instant::now();
                drop(other);
                at
            });
            let ticked = tokio::spawn(async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Instant::now()
            });
            *started.lock().unwrap() = Some((let_go, ticked));
        })));

        let got = state.ensure_loaded("europe").await.map_err(|e| e.to_string());
        let returned = Instant::now();

        let (let_go, ticked) = meanwhile.lock().unwrap().take().expect("the download is in");
        let (let_go, ticked) = (let_go.join().unwrap(), ticked.await.unwrap());
        assert_eq!((ticked < let_go, let_go < returned), (true, true), "{got:?}");
    }

    /// The names in a dir and their bytes.
    fn tree(dir: &std::path::Path) -> std::collections::BTreeMap<String, Vec<u8>> {
        std::fs::read_dir(dir).unwrap()
            .map(|e| e.unwrap())
            .map(|e| (e.file_name().into_string().unwrap(), std::fs::read(e.path()).unwrap()))
            .collect()
    }

    // A request that gives up, its future dropped (as the server drops a
    // handler's once its client went), leaves the download it started to
    // run to its end. Dropped part way, a download lets go of the dir's
    // lock and the region's mutex while what it started on threads of their
    // own (a unit's decode, a file's writer) goes on writing the dir, and a
    // download after it may mix their files with its own. Here the bucket
    // holds a unit's body back half way and the request gives up meanwhile:
    // the lock and the mutex stay held; once the bucket sends the rest, the
    // download ends, the dir as a download of the selection leaves it, each
    // object fetched once.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_request_that_gives_up_leaves_its_download_to_run_to_its_end() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (root, state) = state_for(bucket.url.clone(), Duration::from_secs(3600)).await;
        let state = Arc::new(state);
        let sel = state.get_configuration().await.selection("europe");
        let whole = ScratchDir::new();
        download_region(&state.http, &bucket.url, &bucket.manifest, &sel, whole.path(), |_| {}).await.unwrap();
        let (want, all) = (tree(&whole.path().join("europe")), bucket.files_served());
        let unit = crate::downloader::resolve_files(&bucket.manifest, &sel).unwrap().paths.iter()
            .find_map(|p| bucket.manifest.files.get(p)?.unit.clone())
            .expect("the selection takes a unit");
        let holding = bucket.hold(&unit);

        tokio::select! {
            got = state.ensure_loaded("europe") => panic!("done while the bucket held {unit} back: {got:?}"),
            () = holding.reached() => {}
        }
        let dir = root.path().join("europe");
        let lock_free = std::fs::File::open(dir.join(DOWNLOAD_LOCK)).unwrap().try_lock().is_ok();
        let mutex = state.region_locks.lock().await["europe"].clone();
        let mutex_free = mutex.try_lock().is_ok();
        drop(holding);
        let ended = tokio::time::timeout(Duration::from_secs(60), mutex.lock()).await.is_ok();

        assert_eq!((lock_free, mutex_free, ended), (false, false, true));
        assert!(tree(&dir) == want, "{:?}", tree(&dir).keys().collect::<Vec<_>>());
        assert_eq!(bucket.files_served(), 2 * all);
    }

    /// The files in a dir and in its sub-dirs, by their places in it.
    fn names(dir: &std::path::Path) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for e in std::fs::read_dir(dir).unwrap().map(|e| e.unwrap()) {
            let name = e.file_name().into_string().unwrap();
            if e.file_type().unwrap().is_dir() {
                out.extend(names(&e.path()).into_iter().map(|n| format!("{name}/{n}")));
            } else {
                out.insert(name);
            }
        }
        out
    }

    /// What a download of `sel` leaves in its region dir, by place: its
    /// files, the ledger, the stamp and, on unix, the lock file.
    fn names_of(bucket: &TestBucket, sel: &RegionSelection) -> BTreeSet<String> {
        let resolved = crate::downloader::resolve_files(&bucket.manifest, sel).unwrap();
        let mut out: BTreeSet<String> = resolved.paths.iter()
            .filter(|p| bucket.manifest.files.contains_key(*p))
            .map(|p| resolved.dest(p).to_string())
            .collect();
        out.extend([BUILD_STAMP, FILE_LEDGER].map(String::from));
        if cfg!(unix) {
            out.insert(DOWNLOAD_LOCK.to_string());
        }
        out
    }

    /// Replaces the files of the continent dir `dir`, and of its water dir
    /// where that holds any, with those of an index of nothing, which loads
    /// (the bucket's are made-up bytes, most of them); the stamp, the ledger
    /// and the lock file stay.
    fn make_loadable(dir: &std::path::Path) {
        use query_server::{arc_fixture::ArcFixture, cell_pairs, strings, STR_TIER_COUNT, WATER_DIR};
        let (cells, entries) = cell_pairs::write_pairs(std::iter::empty(), cell_pairs::ADMIN);
        let tiers = vec![r#"{"start": 0, "end": 0}"#; STR_TIER_COUNT].join(", ");
        let mut files: Vec<(&str, Vec<u8>)> = ArcFixture::new().files().into();
        files.extend([
            ("admin_cells.bin", cells),
            ("admin_entries.bin", entries),
            ("strings_core.bin", strings::encode(b"")),
            ("strings_layout.json", format!(r#"{{"tiers": [{tiers}]}}"#).into_bytes()),
        ]);
        let water = dir.join(WATER_DIR);
        let wet = water.is_dir() && std::fs::read_dir(&water).unwrap().next().is_some();
        let meta = ("water_meta.json", br#"{"types": []}"#.to_vec());
        for (d, extra) in [(dir.to_path_buf(), None), (water, Some(meta))] {
            if extra.is_some() && !wet {
                continue;
            }
            for e in std::fs::read_dir(&d).unwrap().map(|e| e.unwrap()) {
                let name = e.file_name().into_string().unwrap();
                if e.file_type().unwrap().is_file() && ![BUILD_STAMP, FILE_LEDGER, DOWNLOAD_LOCK].contains(&name.as_str()) {
                    std::fs::remove_file(e.path()).unwrap();
                }
            }
            for (name, bytes) in files.iter().cloned().chain(extra) {
                std::fs::write(d.join(name), bytes).unwrap();
            }
        }
    }

    /// Has each download on this thread from now on put the places of its
    /// region dir's files in `seen` once it is in, then make the dir one
    /// that loads (make_loadable).
    fn on_each_download(seen: Rc<RefCell<Vec<BTreeSet<String>>>>) {
        ONCE_DOWNLOADED.set(Some(Box::new(move |dir: &std::path::Path| {
            seen.borrow_mut().push(names(dir));
            make_loadable(dir);
            on_each_download(seen);
        })));
    }

    // A configuration whose selection differs from the one before downloads
    // each loaded region again, for the new one's files, and loads it once
    // they are in, in place of the index it had, whether preload names it or
    // not: here europe, loaded, from no water to all and back, its files the
    // selection's each time and its index with a water dir, then without.
    // Asia, neither loaded nor named, is left as it was, and a configuration
    // of the same selection downloads no region it has loaded.
    #[tokio::test]
    async fn a_new_selection_is_downloaded_into_each_loaded_region_and_loaded() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (root, state) = state_for(bucket.url.clone(), Duration::from_secs(3600)).await;
        let state = Arc::new(state);
        let seen = Rc::new(RefCell::new(Vec::new()));
        on_each_download(seen.clone());
        state.ensure_loaded("europe").await.unwrap();
        let index = |name: &str| state.multi_index.continents.iter().find(|c| c.name == name).unwrap().index.load_full();
        let mut cfg = state.get_configuration().await;

        for water in ["all", "none"] {
            let was = index("europe").unwrap();
            cfg.water = water.into();
            for task in state.configure(cfg.clone()).await.unwrap() {
                task.await.unwrap();
            }

            let now = index("europe").unwrap();
            assert_eq!((Arc::ptr_eq(&was, &now), now.water.is_some()), (false, water != "none"), "water {water}");
            assert_eq!(seen.borrow().last(), Some(&names_of(&bucket, &cfg.selection("europe"))), "water {water}");
        }
        let (was, served) = (index("europe").unwrap(), bucket.files_served());
        cfg.preload = vec!["europe".into()];
        for task in state.configure(cfg.clone()).await.unwrap() {
            task.await.unwrap();
        }

        assert!(Arc::ptr_eq(&was, &index("europe").unwrap()));
        assert_eq!((seen.borrow().len(), bucket.files_served()), (3, served));
        assert!(index("asia").is_none() && !root.path().join("asia").exists());
    }

    // A loaded region whose download of a new selection fails answers from
    // the index it had: the new files load only once all are in. Here a
    // water file comes down with other bytes than its sha256, and the dir is
    // left without its stamp, as a download that did not finish.
    #[tokio::test]
    async fn a_loaded_region_whose_new_selection_fails_to_download_keeps_its_index() {
        let bucket = test_bucket::serve_tampered(test_bucket::template_components(), |path, bytes| {
            if path == "europe/water/all/simplified/admin_cells.bin.zst" {
                bytes[0] ^= 1;
            }
        }).await;
        let (root, state) = state_for(bucket.url.clone(), Duration::from_secs(3600)).await;
        let state = Arc::new(state);
        let seen = Rc::new(RefCell::new(Vec::new()));
        on_each_download(seen.clone());
        state.ensure_loaded("europe").await.unwrap();
        let slot = state.multi_index.continents.iter().find(|c| c.name == "europe").unwrap();
        let was = slot.index.load_full().unwrap();

        let cfg = Configuration { water: "all".into(), ..state.get_configuration().await };
        for task in state.configure(cfg).await.unwrap() {
            task.await.unwrap();
        }

        assert!(Arc::ptr_eq(&was, &slot.index.load_full().unwrap()));
        assert_eq!(seen.borrow().len(), 1);
        assert!(!root.path().join("europe").join(BUILD_STAMP).exists());
    }

    // A region whose download of the selection before still runs as the
    // configuration changes is downloaded again once that one ends: the new
    // selection's task waits for the region's mutex and finds it loaded.
    // Here the first download is held back half way through a unit while
    // the water changes to all.
    #[tokio::test]
    async fn a_region_downloading_as_the_selection_changes_is_downloaded_again_once_that_ends() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (_root, state) = state_for(bucket.url.clone(), Duration::from_secs(3600)).await;
        let state = Arc::new(state);
        let seen = Rc::new(RefCell::new(Vec::new()));
        on_each_download(seen.clone());
        let before = state.get_configuration().await;
        let holding = bucket.hold("europe/admin/places.tt.zst");
        let first = {
            let state = state.clone();
            tokio::spawn(async move { state.ensure_loaded("europe").await.map_err(|e| e.to_string()) })
        };
        holding.reached().await;

        let after = Configuration { water: "all".into(), ..before.clone() };
        let tasks = state.configure(after.clone()).await.unwrap();
        drop(holding);
        assert_eq!(first.await.unwrap(), Ok(()));
        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(*seen.borrow(), [names_of(&bucket, &before.selection("europe")), names_of(&bucket, &after.selection("europe"))]);
        let europe = state.multi_index.continents.iter().find(|c| c.name == "europe").unwrap();
        assert!(europe.index.load_full().unwrap().water.is_some());
    }
}
