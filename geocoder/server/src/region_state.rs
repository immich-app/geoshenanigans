// Region state coordinator: holds the current admin-supplied
// configuration, the cached configurations.json from the build, and
// the per-region download mutexes. Triggers downloads, installs
// completed regions into the live MultiIndex via arc-swap.
//
// Two flows feed this:
//
// 1. Configuration push (PUT /admin/configuration): full snapshot of what
//    regions the user wants, plus mode/quality/poi_tier. Server preloads
//    regions in the snapshot that aren't already on disk.
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
    check_selection, download_region, Configurations, DownloadError, DownloadProgress, RegionSelection,
};
use query_server::MultiIndex;

/// How long a fetched configurations.json is trusted before latest.json
/// is re-resolved. CI keeps full builds for 3 days, so a server that
/// never re-resolved would 404 on every new download from day 4.
const CONFIGURATIONS_TTL: Duration = Duration::from_secs(3600);

#[cfg(test)]
thread_local! {
    /// Set by a test: run by the next ensure_loaded on this thread once its
    /// download is in, before it loads the region dir, which it is given.
    static ONCE_DOWNLOADED: std::cell::Cell<Option<Box<dyn FnOnce(&std::path::Path)>>> = const { std::cell::Cell::new(None) };
}

/// The selection a server downloads. A mode, quality or poi_tier its build
/// has no component for fails each download, naming the values the build
/// offers; a PUT naming one is refused when the build's manifest is at hand.
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
            preload: Vec::new(),
        }
    }
}

impl Configuration {
    pub fn selection(&self, region: &str) -> RegionSelection {
        RegionSelection {
            region: region.to_string(),
            mode: self.mode.clone(),
            quality: self.quality.clone(),
            poi_tier: self.poi_tier.clone(),
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
    /// The client of downloads, built by the first (`http`).
    http: tokio::sync::OnceCell<reqwest::Client>,
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
            http: tokio::sync::OnceCell::new(),
        }
    }

    /// The client of downloads. Building one loads the system's CA
    /// certificates, which a host may lack, so the first download builds
    /// it and a server without them still serves the regions on its disk.
    /// A client that can't be built fails the download, naming why, and
    /// the next download tries again.
    async fn http(&self) -> Result<&reqwest::Client, DownloadError> {
        self.http
            .get_or_try_init(|| async {
                reqwest::Client::builder().user_agent("geocoder-server/0.1").build().map_err(|e| {
                    let mut why = e.to_string();
                    let mut source = std::error::Error::source(&e);
                    while let Some(s) = source {
                        why += &format!(": {s}");
                        source = s.source();
                    }
                    DownloadError::Http(format!("building the client: {why}"))
                })
            })
            .await
    }

    /// Replace the active configuration. Persists to disk and kicks
    /// off background downloads for any preload region not yet on
    /// disk. Returns immediately — downloads run as tokio tasks.
    ///
    /// Refused, changing nothing, when the manifest at hand has no
    /// component for its mode, quality or poi_tier.
    pub async fn set_configuration(self: &Arc<Self>, new_cfg: Configuration) -> Result<(), DownloadError> {
        // The manifest at hand: the cached one, when it is of this base_url
        // and within its TTL. Without one the first download reports a
        // value the build lacks. The check reads no region.
        let same_bucket = self.config.lock().await.base_url == new_cfg.base_url;
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

        // Spawn preload downloads. Each goes through ensure_loaded
        // which is single-flighted, so even if a /reverse request
        // beats the preload to the punch, only one download runs.
        for region in &new_cfg.preload {
            let s = self.clone();
            let r = region.clone();
            tokio::spawn(async move {
                if let Err(e) = s.ensure_loaded(&r).await {
                    eprintln!("preload {}: {}", r, e);
                }
            });
        }
        Ok(())
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
        let http = self.http().await?;
        // Resolve latest build date via latest.json.
        let latest_url = format!("{}/builds/latest.json", cfg.base_url.trim_end_matches('/'));
        let latest: serde_json::Value = http.get(&latest_url).send().await
            .map_err(|e| DownloadError::Http(format!("{}: {}", latest_url, e)))?
            .error_for_status().map_err(|e| DownloadError::Http(format!("{}: {}", latest_url, e)))?
            .json().await
            .map_err(|e| DownloadError::Http(format!("{}: parse latest.json: {}", latest_url, e)))?;
        let date = latest.get("latest").and_then(|v| v.as_str())
            .ok_or_else(|| DownloadError::Http(format!("{}: missing 'latest' field", latest_url)))?;

        let cfg_url = format!("{}/builds/{}/configurations.json", cfg.base_url.trim_end_matches('/'), date);
        let configurations: Configurations = http.get(&cfg_url).send().await
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

        // Re-check after acquiring the lock: an earlier waiter may
        // have just finished the download.
        if self.multi_index.continents.iter()
            .any(|c| c.name == region && c.index.load().is_some())
        {
            return Ok(());
        }
        // Bail early if `region` isn't a known continent.
        if !self.multi_index.continents.iter().any(|c| c.name == region) {
            return Err(DownloadError::UnknownRegion(region.to_string()));
        }

        let (state, region) = (self.clone(), region.to_string());
        tokio::spawn(async move {
            let _guard = guard;
            state.download_and_install(&region).await
        })
        .await
        .map_err(|e| DownloadError::Io(format!("download task: {}", e)))?
    }

    /// Downloads `region` and installs it: the task of ensure_loaded.
    async fn download_and_install(&self, region: &str) -> Result<(), DownloadError> {
        let configurations = self.ensure_configurations().await?;
        let cfg = self.config.lock().await.clone();
        let sel = cfg.selection(region);

        let region_owned = region.to_string();
        download_region(
            self.http().await?,
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
    use crate::test_bucket;
    use query_server::DOWNLOAD_LOCK;
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

    /// Set for the process `a_host_without_ca_certificates_starts_and_its_downloads_fail_naming_why`
    /// starts, which runs `without_ca_certificates` with no CA store.
    const NO_CA_STORE: &str = "GEOCODER_TEST_NO_CA_STORE";

    // A host without CA certificates (an image without a CA bundle) builds
    // no HTTP client. A server there still starts and serves the regions on
    // its disk; each download fails naming why, fetching nothing, and the
    // first once the certificates are there builds it. The case runs in a
    // process of its own, for SSL_CERT_FILE and SSL_CERT_DIR, which name the
    // CA store in place of the system's, are the process's.
    #[test]
    fn a_host_without_ca_certificates_starts_and_its_downloads_fail_naming_why() {
        let empty = ScratchDir::new();
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["region_state::tests::without_ca_certificates", "--exact", "--ignored", "--test-threads=1"])
            .env("SSL_CERT_FILE", empty.path().join("none.pem"))
            .env("SSL_CERT_DIR", empty.path())
            .env(NO_CA_STORE, "1")
            .output()
            .unwrap();

        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(out.status.success() && stdout.contains(" 1 passed;"), "{stdout}{}", String::from_utf8_lossy(&out.stderr));
    }

    #[tokio::test]
    #[ignore = "run by a_host_without_ca_certificates_starts_and_its_downloads_fail_naming_why"]
    async fn without_ca_certificates() {
        if std::env::var_os(NO_CA_STORE).is_none() {
            return;
        }
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (root, state) = state_for(bucket.url.clone(), Duration::from_secs(3600)).await;
        let state = Arc::new(state);

        let got = state.ensure_loaded("europe").await.map_err(|e| e.to_string());

        assert_eq!(
            got,
            Err("HTTP error: building the client: builder error: unexpected error: No CA certificates were loaded from the system".to_string())
        );
        assert_eq!(bucket.files_served(), 0);
        assert!(!root.path().join("europe").exists());

        // Safe to change: the process runs this case alone, on a runtime of one thread.
        std::env::remove_var("SSL_CERT_FILE");
        std::env::remove_var("SSL_CERT_DIR");
        state.ensure_configurations().await.unwrap();
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
        ] {
            let got = state.set_configuration(cfg).await.map_err(|e| e.to_string());

            assert_eq!(got, Err(error.to_string()));
        }
        let json = |c: &Configuration| serde_json::to_value(c).unwrap();
        assert_eq!(json(&state.get_configuration().await), json(&before));
        assert!(!root.path().join("configuration.json").exists());

        state.set_configuration(Configuration { quality: "exact".into(), ..before.clone() }).await.unwrap();
        assert_eq!(state.get_configuration().await.quality, "exact");
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
        download_region(state.http().await.unwrap(), &bucket.url, &bucket.manifest, &sel, whole.path(), |_| {}).await.unwrap();
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
}
