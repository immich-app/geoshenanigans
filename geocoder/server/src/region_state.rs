// Region state coordinator: holds the current admin-supplied
// configuration, the cached configurations.json from the build, and
// the per-region download mutexes. Triggers downloads, installs
// completed regions into the live MultiIndex via arc-swap.
//
// Two flows feed this:
//
// 1. Configuration push (PUT /admin/configuration): full snapshot of what
//    regions the user wants, plus mode/quality/poi_tier/water. Server preloads
//    regions in the snapshot that aren't already loaded, and downloads each
//    loaded region again whose files are of another bucket or selection. A
//    server starting does the same for the configuration it kept (resume).
//
// 2. Lazy on-demand: a /reverse request lands on a continent we don't
//    have, the request handler calls `ensure_loaded` which blocks
//    until the download finishes.
//
// Per-region mutex serialises concurrent ensure_loaded calls so each
// region is downloaded at most once even under request fan-in, and guards
// the bucket and selection of the region's files. A download runs in a
// task of its own, which holds the mutex to its end: a request that gives
// up leaves it to run on.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::downloader::{
    check_selection, client_error, download_region, stamped_source, Configurations, DownloadError, DownloadProgress,
    RegionSelection,
};
use query_server::{MultiIndex, BUILD_STAMP, FILE_LEDGER};

/// Where a region's files came from: the bucket (a configuration's
/// `base_url`) and the selection, as the stamp of its dir names them
/// (downloader::stamp_text).
type Source = (String, RegionSelection);

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
    /// Cached configurations.json for the latest build under a base_url,
    /// with when it was fetched and that base_url: a download takes it only
    /// for that bucket's. Loaded lazily on first download, kept only while
    /// the configuration still names that bucket, dropped when the
    /// configuration is updated (the bucket's latest build may have changed)
    /// and re-resolved once it is older than `configurations_ttl`.
    pub configurations: Mutex<Option<(Instant, String, Configurations)>>,
    pub configurations_ttl: Duration,
    /// Per-region serialisation. Concurrent requests for the same region
    /// share one inner Mutex; only the leader actually downloads, the
    /// rest wait on the lock and find the region already loaded. It holds
    /// the bucket and the selection of the files the region's index was
    /// loaded from, as the stamp the dir held as it loaded them names them:
    /// as the server started (ContinentSlot::loaded_stamp), or once a
    /// download and its install went through, the install finding the stamp
    /// that download left or that of another server's download into the dir
    /// that ran between the two. None for a region it loaded from a dir
    /// holding no such stamp, and from when a download begins till it and
    /// the install went through, as one that fails may leave the dir without
    /// its stamp and with files of no selection.
    pub region_locks: Mutex<HashMap<String, Arc<Mutex<Option<Source>>>>>,
    /// The client of downloads, built by the first (`http`).
    http: tokio::sync::OnceCell<reqwest::Client>,
    /// Whether the configuration is one read from `config_path` as the
    /// server started, not the defaults.
    kept: bool,
    /// Bumped as each configuration is swapped in, under the
    /// configuration's lock: a manifest fetched under an earlier one is not
    /// cached (ensure_configurations).
    generation: AtomicU64,
}

impl RegionState {
    pub fn new(multi_index: Arc<MultiIndex>, root_dir: PathBuf) -> Self {
        let config_path = root_dir.join("configuration.json");
        // Load persisted configuration if present; new deployments
        // start from defaults until one is PUT to /admin/configuration.
        let kept: Option<Configuration> = std::fs::read_to_string(&config_path).ok().and_then(|s| serde_json::from_str(&s).ok());
        // The files of each region loaded as the server started are of the
        // bucket and selection the stamp its dir held then names.
        let region_locks = multi_index.continents.iter()
            .filter_map(|slot| Some((slot.name.to_string(), stamped_source(slot.loaded_stamp.as_deref()?, slot.name)?)))
            .map(|(name, source)| (name, Arc::new(Mutex::new(Some(source)))))
            .collect();
        RegionState {
            multi_index,
            root_dir,
            config_path,
            kept: kept.is_some(),
            config: Mutex::new(kept.unwrap_or_default()),
            configurations: Mutex::new(None),
            configurations_ttl: CONFIGURATIONS_TTL,
            region_locks: Mutex::new(region_locks),
            http: tokio::sync::OnceCell::new(),
            generation: AtomicU64::new(0),
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
                reqwest::Client::builder().user_agent("geocoder-server/0.1").build().map_err(client_error)
            })
            .await
    }

    /// Replace the active configuration. Persists to disk and kicks
    /// off background downloads: of each preload region not loaded yet,
    /// and of each loaded region again whose dir the downloader filled,
    /// unless the stamp its dir held as its files loaded names the new
    /// configuration's bucket (base_url) and selection (mode, quality,
    /// poi_tier and water), for the new selection's files from the latest
    /// build of that bucket, which load in place of the index it had once
    /// they are in. So a region whose download failed is downloaded again on
    /// each configuration, of whichever selection, till one goes through.
    /// Till then, and where the download or the load fails, the region
    /// answers as before. Returns immediately — downloads run as tokio
    /// tasks.
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
        if let Some((fetched_at, base_url, manifest)) = self.configurations.lock().await.as_ref() {
            if *base_url == new_cfg.base_url && fetched_at.elapsed() < self.configurations_ttl {
                check_selection(manifest, &new_cfg.selection(""))?;
            }
        }

        // Persist + swap in under the configuration's lock, so that PUTs
        // land on disk in the order they apply and a server restarted after
        // them resumes the last (resume); the file is replaced by a rename,
        // never left half written. Disk write is best-effort — we still
        // apply the new config in memory if disk is full.
        {
            let mut cfg = self.config.lock().await;
            let json = serde_json::to_string_pretty(&new_cfg).unwrap_or_default();
            let mut tmp = self.config_path.clone().into_os_string();
            tmp.push(".tmp");
            if tokio::fs::write(&tmp, json).await.is_ok() {
                let _ = tokio::fs::rename(&tmp, &self.config_path).await;
            }
            *cfg = new_cfg.clone();
            self.generation.fetch_add(1, Ordering::SeqCst);
        }
        // Force re-fetch of configurations.json on next download (the
        // referenced build may have changed).
        *self.configurations.lock().await = None;

        Ok(self.start_downloads(&new_cfg, ""))
    }

    /// Starts, as a server starts, the downloads the configuration it kept
    /// (configuration.json) asks for, as a PUT of it would
    /// (set_configuration): of each region it preloads that isn't loaded,
    /// and of each loaded region the downloader filled whose stamp names
    /// another bucket or selection. A PUT keeps its configuration before its
    /// downloads run, and a server stopped before they went through would
    /// else serve the files before it till the next PUT. None without a
    /// kept configuration: the defaults then stand in for none, and a
    /// region's files stay as they are till a PUT. Returns the tasks it
    /// started.
    pub async fn resume(self: &Arc<Self>) -> Vec<tokio::task::JoinHandle<()>> {
        if !self.kept {
            return Vec::new();
        }
        let cfg = self.get_configuration().await;
        self.start_downloads(&cfg, "startup ")
    }

    /// Spawns, for the configuration `cfg`, preload downloads, and one of
    /// each other region, which downloads it again if it is loaded and of
    /// another bucket or selection; each logs its error after `when`. Each
    /// task decides under the region's mutex (download_if), so a download of
    /// the selection before that holds it ends first, and its region is then
    /// downloaded again; even if a /reverse request beats the preload to the
    /// punch, only one download runs.
    fn start_downloads(self: &Arc<Self>, cfg: &Configuration, when: &'static str) -> Vec<tokio::task::JoinHandle<()>> {
        let mut regions: Vec<(String, bool)> = Vec::new();
        for region in &cfg.preload {
            if !regions.iter().any(|(r, _)| r == region) {
                regions.push((region.clone(), true));
            }
        }
        for c in &self.multi_index.continents {
            if !regions.iter().any(|(r, _)| r == c.name) {
                regions.push((c.name.to_string(), false));
            }
        }
        let mut tasks = Vec::new();
        for (region, preload) in regions {
            let s = self.clone();
            tasks.push(tokio::spawn(async move {
                if let Err(e) = s.download_if(&region, preload, true).await {
                    eprintln!("{}{} {}: {}", when, if preload { "preload" } else { "download" }, region, e);
                }
            }));
        }
        tasks
    }

    pub async fn get_configuration(&self) -> Configuration {
        self.config.lock().await.clone()
    }

    /// Fetch (or return cached) configurations.json for the latest build
    /// of the bucket at `base_url`. The one fetched is cached only where no
    /// configuration was swapped in while it was fetched and the
    /// configuration names that bucket: one fetched before a PUT, whose
    /// bucket's latest build may since be another, is this download's
    /// alone.
    async fn ensure_configurations(&self, base_url: &str) -> Result<Configurations, DownloadError> {
        if let Some((fetched_at, of, c)) = self.configurations.lock().await.as_ref() {
            if of == base_url && fetched_at.elapsed() < self.configurations_ttl {
                return Ok(c.clone());
            }
        }
        let generation = self.generation.load(Ordering::SeqCst);

        let http = self.http().await?;
        // Resolve latest build date via latest.json.
        let latest_url = format!("{}/builds/latest.json", base_url.trim_end_matches('/'));
        let latest: serde_json::Value = http.get(&latest_url).send().await
            .map_err(|e| DownloadError::Http(format!("{}: {}", latest_url, e)))?
            .error_for_status().map_err(|e| DownloadError::Http(format!("{}: {}", latest_url, e)))?
            .json().await
            .map_err(|e| DownloadError::Http(format!("{}: parse latest.json: {}", latest_url, e)))?;
        let date = latest.get("latest").and_then(|v| v.as_str())
            .ok_or_else(|| DownloadError::Http(format!("{}: missing 'latest' field", latest_url)))?;

        let cfg_url = format!("{}/builds/{}/configurations.json", base_url.trim_end_matches('/'), date);
        let configurations: Configurations = http.get(&cfg_url).send().await
            .map_err(|e| DownloadError::Http(format!("{}: {}", cfg_url, e)))?
            .error_for_status().map_err(|e| DownloadError::Http(format!("{}: {}", cfg_url, e)))?
            .json().await
            .map_err(|e| DownloadError::Http(format!("{}: parse: {}", cfg_url, e)))?;

        // Checked under the configuration's lock, which a PUT holds as it
        // swaps its configuration in, before it drops the cache.
        let cfg = self.config.lock().await;
        if cfg.base_url == base_url && self.generation.load(Ordering::SeqCst) == generation {
            *self.configurations.lock().await = Some((Instant::now(), base_url.to_string(), configurations.clone()));
        }
        drop(cfg);
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
    /// loaded region is downloaded again only where its files (by the stamp
    /// its dir held as they loaded) aren't of the configuration's bucket and
    /// selection, and its continent dir is the downloader's, holding its
    /// ledger or its stamp: one holding neither was laid out by other means
    /// (by hand, by the test portal), as the flat layout's one slot was, and
    /// is never downloaded again. The bucket and selection are forgotten as
    /// a download begins, and noted once the download and the install went
    /// through, as the stamp the install found names them: a download of
    /// another server on the data dir may run between the two.
    async fn download_if(self: &Arc<Self>, region: &str, unloaded: bool, loaded: bool) -> Result<(), DownloadError> {
        // Per-region serialisation. Acquire the inner mutex so only
        // one concurrent request runs the download below; the rest
        // wait, find the region already installed, and return.
        let inner = {
            let mut g = self.region_locks.lock().await;
            g.entry(region.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(None)))
                .clone()
        };
        let mut installed = inner.lock_owned().await;

        // Bail early if `region` isn't a known continent.
        let Some(slot) = self.multi_index.continents.iter().find(|c| c.name == region) else {
            return Err(DownloadError::UnknownRegion(region.to_string()));
        };
        // Checked after acquiring the lock: an earlier waiter may have
        // just finished the download.
        let wanted = if slot.index.load().is_some() {
            let cfg = self.config.lock().await.clone();
            let source = (cfg.base_url.clone(), cfg.selection(region));
            loaded && slot.continent.is_some() && installed.as_ref() != Some(&source) && self.downloaded_into(region).await
        } else {
            unloaded
        };
        if !wanted {
            return Ok(());
        }

        let (state, region) = (self.clone(), region.to_string());
        tokio::spawn(async move {
            // Unknown till the download and the install went through: one
            // that fails may have removed the stamp and files of the
            // selection before, and the region's dir is then downloaded again
            // for whichever selection comes next.
            *installed = None;
            *installed = state.download_and_install(&region).await?;
            Ok(())
        })
        .await
        .map_err(|e| DownloadError::Io(format!("download task: {}", e)))?
    }

    /// Whether the continent dir of `region` is one the downloader filled:
    /// it holds the ledger or the stamp (FILE_LEDGER, BUILD_STAMP), which no
    /// other writer of a continent dir makes.
    async fn downloaded_into(&self, region: &str) -> bool {
        let dir = self.root_dir.join(region);
        for name in [FILE_LEDGER, BUILD_STAMP] {
            if tokio::fs::symlink_metadata(dir.join(name)).await.is_ok() {
                return true;
            }
        }
        false
    }

    /// Downloads `region` and installs it, for the configuration as it is
    /// as the download begins, its manifest that of its bucket: returns the
    /// bucket and the selection the stamp the dir held as it loaded names
    /// (that of this download, or of another server's that ran after it),
    /// None for a stamp of no such text: the task of download_if. A region
    /// loaded already answers from the files its index mapped until the new
    /// index is installed, and goes on doing so where the download or the
    /// load fails: Index::load maps each file it reads and opens none by its
    /// path later, and a download writes no file in place, but renames a new
    /// one over it or removes it, which leaves a mapped file as it was.
    async fn download_and_install(&self, region: &str) -> Result<Option<Source>, DownloadError> {
        let cfg = self.config.lock().await.clone();
        let configurations = self.ensure_configurations(&cfg.base_url).await?;
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
        // runtime's, which answer the queries meanwhile. The stamp it found
        // names the bucket and the selection of the files it loaded, this
        // download's or that one's.
        let (multi_index, name) = (self.multi_index.clone(), region.to_string());
        let stamp = tokio::task::spawn_blocking(move || multi_index.install_region(&name))
            .await
            .map_err(|e| DownloadError::Io(format!("load task: {}", e)))?
            .map_err(DownloadError::UnknownRegion)?;
        Ok(stamp.as_deref().and_then(|text| stamped_source(text, region)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scratch_dir::ScratchDir;
    use crate::test_bucket::{self, TestBucket};
    use query_server::DOWNLOAD_LOCK;
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

        state.ensure_configurations(&state.get_configuration().await.base_url).await.unwrap();
        *bucket.latest.lock().unwrap() = "2026-01-02".to_string();
        let second = state.ensure_configurations(&state.get_configuration().await.base_url).await.unwrap();

        assert_eq!(bucket.fetches.load(Ordering::SeqCst), 1);
        assert_eq!(second.build.date, "2026-01-01");
    }

    #[tokio::test]
    async fn configurations_follow_latest_after_ttl() {
        let bucket = Arc::new(FakeBucket { latest: "2026-01-01".to_string().into(), fetches: AtomicUsize::new(0) });
        let (_root, state) = state_for(serve(bucket.clone()).await, std::time::Duration::ZERO).await;

        state.ensure_configurations(&state.get_configuration().await.base_url).await.unwrap();
        *bucket.latest.lock().unwrap() = "2026-01-02".to_string();
        let second = state.ensure_configurations(&state.get_configuration().await.base_url).await.unwrap();

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

    // A host without CA certificates (an image without a CA bundle) builds
    // no HTTP client. A server there still starts and serves the regions on
    // its disk; each download fails naming why, fetching nothing, and the
    // first once the certificates are there builds it. The case runs in a
    // process of its own (test_bucket::without_ca_store).
    #[cfg(all(unix, not(target_vendor = "apple"), not(target_os = "android")))]
    #[test]
    fn a_host_without_ca_certificates_starts_and_its_downloads_fail_naming_why() {
        test_bucket::without_ca_store("region_state::tests::without_ca_certificates");
    }

    #[cfg(all(unix, not(target_vendor = "apple"), not(target_os = "android")))]
    #[tokio::test]
    #[ignore = "run by a_host_without_ca_certificates_starts_and_its_downloads_fail_naming_why"]
    async fn without_ca_certificates() {
        if std::env::var_os(test_bucket::NO_CA_STORE).is_none() {
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

        let certificates = ScratchDir::new();
        test_bucket::with_test_ca(certificates.path());
        state.ensure_configurations(&state.get_configuration().await.base_url).await.unwrap();
    }

    // With the build's manifest at hand, a configuration naming a value it
    // lacks is refused before it replaces or persists anything.
    #[tokio::test]
    async fn a_configuration_naming_a_value_the_manifest_at_hand_lacks_is_refused() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (root, state) = state_for(bucket.url.clone(), std::time::Duration::from_secs(3600)).await;
        let state = Arc::new(state);
        state.ensure_configurations(&state.get_configuration().await.base_url).await.unwrap();
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
        state.ensure_configurations(&state.get_configuration().await.base_url).await.unwrap();
        let moved = Configuration { base_url: offering.url.clone(), quality: "fine".into(), ..state.get_configuration().await };
        state.set_configuration(moved).await.unwrap();
        assert_eq!(state.get_configuration().await.quality, "fine");

        // A PUT once the manifest at hand is past its TTL.
        let (_root, state) = state_for(offering.url.clone(), Duration::ZERO).await;
        let state = Arc::new(state);
        *state.configurations.lock().await = Some((Instant::now(), offering.url.clone(), lacking.manifest.clone()));
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

    // A continent dir holding neither the ledger nor the stamp was laid out
    // by other means (by hand, by the test portal), and a server loads it as
    // it is: a configuration of another selection, naming it in preload or
    // not, downloads nothing into it and leaves its index. Here europe laid
    // out as an index of nothing, loaded as a server starts, and a switch to
    // water all, then to the poi tier major as well with europe preloaded.
    #[tokio::test]
    async fn a_continent_laid_out_by_other_means_is_never_downloaded_again() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let root = ScratchDir::new();
        let dir = root.path().join("europe");
        std::fs::create_dir(&dir).unwrap();
        make_loadable(&dir);
        let mi = MultiIndex::load(root.path().to_str().unwrap(), 17, 10, 660.0).unwrap();
        let mut state = RegionState::new(Arc::new(mi), root.path().to_path_buf());
        state.config.get_mut().base_url = bucket.url.clone();
        let state = Arc::new(state);
        let slot = state.multi_index.continents.iter().find(|c| c.name == "europe").unwrap();
        let (was, files) = (slot.index.load_full().expect("the dir laid out loads"), tree(&dir));
        let cfg = state.get_configuration().await;

        for cfg in [
            Configuration { water: "all".into(), ..cfg.clone() },
            Configuration { water: "all".into(), poi_tier: "major".into(), preload: vec!["europe".into()], ..cfg.clone() },
        ] {
            for task in state.configure(cfg.clone()).await.unwrap() {
                task.await.unwrap();
            }

            assert_eq!(bucket.files_served(), 0, "{cfg:?}");
            assert!(Arc::ptr_eq(&was, &slot.index.load_full().unwrap()), "{cfg:?}");
            assert!(tree(&dir) == files, "{cfg:?}: {:?}", tree(&dir).keys().collect::<Vec<_>>());
        }
    }

    // A loaded region whose download of a new selection fails answers from
    // the index it had: the new files load only once all are in. Here a
    // water file comes down with other bytes than its sha256, and the dir is
    // left without its stamp, as a download that did not finish, which a
    // server starting leaves unloaded. The selection of the region's files
    // is then unknown till a download goes through: a configuration of the
    // same selection as the one whose download failed downloads it again,
    // and so does one of the selection before, which puts the stamp back
    // and loads the region without water; once the bucket recovers, one of
    // water all loads it with water.
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
        let stamp = root.path().join("europe").join(BUILD_STAMP);
        let none = state.get_configuration().await;
        let all = Configuration { water: "all".into(), ..none.clone() };

        for _ in 0..2 {
            let served = bucket.files_served();
            for task in state.configure(all.clone()).await.unwrap() {
                task.await.unwrap();
            }

            assert!(bucket.files_served() > served);
            assert!(Arc::ptr_eq(&was, &slot.index.load_full().unwrap()));
            assert_eq!(seen.borrow().len(), 1);
            assert!(!stamp.exists());
        }

        for task in state.configure(none.clone()).await.unwrap() {
            task.await.unwrap();
        }

        let now = slot.index.load_full().unwrap();
        assert_eq!((stamp.exists(), Arc::ptr_eq(&was, &now), now.water.is_some()), (true, false, false));
        assert_eq!(seen.borrow().last(), Some(&names_of(&bucket, &none.selection("europe"))));

        bucket.mend();
        for task in state.configure(all.clone()).await.unwrap() {
            task.await.unwrap();
        }

        assert!(slot.index.load_full().unwrap().water.is_some());
        assert_eq!(seen.borrow().last(), Some(&names_of(&bucket, &all.selection("europe"))));
    }

    /// Downloads `sel` into the data dir `root` as another server on it
    /// would, from a bucket of its own on a runtime of its own: it runs to
    /// its end while this thread waits for it, in a hook a download on this
    /// thread runs (ONCE_DOWNLOADED) too.
    fn another_servers_download(root: &std::path::Path, sel: &RegionSelection) {
        let (root, sel) = (root.to_path_buf(), sel.clone());
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(async {
                let bucket = test_bucket::serve(test_bucket::template_components()).await;
                download_region(&reqwest::Client::new(), &bucket.url, &bucket.manifest, &sel, &root, |_| {}).await.unwrap();
            });
        }).join().unwrap();
    }

    // Servers on one data dir may download other selections into a region
    // dir: another server's download may run once this one's let go of the
    // dir and before this one loads it, which then loads that download's
    // files. A server knows the selection of the files it loaded only where
    // the dir holds the stamp its own download left. Here another server
    // downloads water all into europe as this one's download of no water is
    // in: europe loads with water, and the next configuration of no water
    // downloads it again and loads it without.
    #[tokio::test]
    async fn a_region_another_server_downloaded_into_before_it_loaded_is_downloaded_again() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (root, state) = state_for(bucket.url.clone(), Duration::from_secs(3600)).await;
        let state = Arc::new(state);
        let none = state.get_configuration().await;
        let (data_dir, theirs) = (root.path().to_path_buf(), Configuration { water: "all".into(), ..none.clone() }.selection("europe"));
        let seen = Rc::new(RefCell::new(Vec::new()));
        let after = seen.clone();
        ONCE_DOWNLOADED.set(Some(Box::new(move |dir: &std::path::Path| {
            another_servers_download(&data_dir, &theirs);
            make_loadable(dir);
            on_each_download(after);
        })));
        state.ensure_loaded("europe").await.unwrap();
        let slot = state.multi_index.continents.iter().find(|c| c.name == "europe").unwrap();
        let wet = slot.index.load_full().unwrap().water.is_some();

        for task in state.configure(none.clone()).await.unwrap() {
            task.await.unwrap();
        }

        assert_eq!((wet, slot.index.load_full().unwrap().water.is_some()), (true, false));
        assert_eq!(*seen.borrow(), [names_of(&bucket, &none.selection("europe"))]);
    }

    /// A data dir as a server finds it as it starts: europe downloaded for
    /// `europe`, asia for `asia`, both made loadable, and `persisted` the
    /// configuration it keeps.
    async fn restarted(bucket: &TestBucket, europe: &Configuration, asia: &Configuration, persisted: &Configuration) -> (ScratchDir, Arc<RegionState>) {
        let root = ScratchDir::new();
        for (region, cfg) in [("europe", europe), ("asia", asia)] {
            download_region(&reqwest::Client::new(), &cfg.base_url, &bucket.manifest, &cfg.selection(region), root.path(), |_| {}).await.unwrap();
            make_loadable(&root.path().join(region));
        }
        std::fs::write(root.path().join("configuration.json"), serde_json::to_string(persisted).unwrap()).unwrap();
        let mi = MultiIndex::load(root.path().to_str().unwrap(), 17, 10, 660.0).unwrap();
        let state = Arc::new(RegionState::new(Arc::new(mi), root.path().to_path_buf()));
        (root, state)
    }

    // A server knows the bucket and selection of the files of a region it
    // loaded as it started by the stamp its dir held: a configuration
    // downloads such a region again only where those are another bucket or
    // selection than its own, and loads it once that is in; a configuration
    // of that selection after it downloads it no more. Here europe stamped
    // with no water, asia with water all, and two configurations of water
    // all.
    #[tokio::test]
    async fn a_region_loaded_as_the_server_started_is_downloaded_again_only_for_another_selection() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let none = Configuration { base_url: bucket.url.clone(), ..Configuration::default() };
        let all = Configuration { water: "all".into(), ..none.clone() };
        let (_root, state) = restarted(&bucket, &none, &all, &none).await;
        let slot = |name: &str| state.multi_index.continents.iter().find(|c| c.name == name).unwrap();
        let (europe, asia) = (slot("europe").index.load_full().unwrap(), slot("asia").index.load_full().unwrap());
        let seen = Rc::new(RefCell::new(Vec::new()));
        on_each_download(seen.clone());

        for _ in 0..2 {
            for task in state.configure(all.clone()).await.unwrap() {
                task.await.unwrap();
            }
        }

        let now = slot("europe").index.load_full().unwrap();
        assert_eq!((Arc::ptr_eq(&europe, &now), now.water.is_some()), (false, true));
        assert!(Arc::ptr_eq(&asia, &slot("asia").index.load_full().unwrap()));
        assert_eq!(*seen.borrow(), [names_of(&bucket, &all.selection("europe"))]);
    }

    // A configuration is kept on disk before its downloads run: a server
    // restarted before they went through runs them as it starts (resume),
    // for each loaded region whose stamp names another bucket or selection
    // than the kept configuration, and for each region it preloads that is
    // not loaded. Here the configuration of water all, preloading africa,
    // kept as europe's download of it had not begun; asia's had gone
    // through.
    #[tokio::test]
    async fn a_server_restarted_before_a_configurations_downloads_went_through_runs_them_as_it_starts() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let none = Configuration { base_url: bucket.url.clone(), ..Configuration::default() };
        let all = Configuration { water: "all".into(), preload: vec!["africa".into()], ..none.clone() };
        let (_root, state) = restarted(&bucket, &none, &all, &all).await;
        let slot = |name: &str| state.multi_index.continents.iter().find(|c| c.name == name).unwrap();
        let asia = slot("asia").index.load_full().unwrap();
        let seen = Rc::new(RefCell::new(Vec::new()));
        on_each_download(seen.clone());

        for task in state.resume().await {
            task.await.unwrap();
        }

        assert!(slot("europe").index.load_full().unwrap().water.is_some());
        assert!(slot("africa").index.load_full().is_some());
        assert!(Arc::ptr_eq(&asia, &slot("asia").index.load_full().unwrap()));
        let want = BTreeSet::from([names_of(&bucket, &all.selection("africa")), names_of(&bucket, &all.selection("europe"))]);
        assert_eq!((seen.borrow().len(), seen.borrow().iter().cloned().collect::<BTreeSet<_>>()), (2, want));
        for task in state.configure(all.clone()).await.unwrap() {
            task.await.unwrap();
        }
        assert_eq!(seen.borrow().len(), 2);
    }

    // A configuration naming another bucket, of the same selection, downloads
    // each loaded region again from that bucket's latest build, and one
    // naming it again downloads it no more. Here europe from bucket a, then
    // from b, whose latest build is of another date.
    #[tokio::test]
    async fn a_configuration_naming_another_bucket_downloads_each_loaded_region_from_it() {
        let (a, b) = (test_bucket::serve(test_bucket::template_components()).await, test_bucket::serve(test_bucket::template_components()).await);
        b.add_build("2026-01-02", test_bucket::template_version());
        b.set_latest("2026-01-02", Some(test_bucket::template_version()));
        let (_root, state) = state_for(a.url.clone(), Duration::from_secs(3600)).await;
        let state = Arc::new(state);
        let seen = Rc::new(RefCell::new(Vec::new()));
        on_each_download(seen.clone());
        state.ensure_loaded("europe").await.unwrap();
        let moved = Configuration { base_url: b.url.clone(), ..state.get_configuration().await };

        for _ in 0..2 {
            for task in state.configure(moved.clone()).await.unwrap() {
                task.await.unwrap();
            }
        }

        assert_eq!(seen.borrow().len(), 2);
        let fetched = b.requests();
        assert!(!fetched.is_empty() && fetched.iter().all(|r| r.starts_with("2026-01-02/europe/")), "{fetched:?}");
    }

    // A manifest is cached only for the configuration it was fetched under:
    // one a PUT replaced while it was fetched, of the same bucket, serves the
    // download that fetched it alone, and a newer build's, fetched since,
    // stays at hand. Here europe's download waits on the manifest of the
    // build of DATE while a PUT of water all comes in and the bucket's latest
    // moves on; asia's download then caches the newer build's, and europe's
    // download for the PUT takes that one.
    #[tokio::test]
    async fn a_manifest_fetched_before_a_configuration_is_not_cached_after_it() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (root, state) = state_for(bucket.url.clone(), Duration::from_secs(3600)).await;
        let state = Arc::new(state);
        on_each_download(Rc::new(RefCell::new(Vec::new())));
        let holding = bucket.hold("configurations.json");
        let first = {
            let state = state.clone();
            tokio::spawn(async move { state.ensure_loaded("europe").await.map_err(|e| e.to_string()) })
        };
        holding.reached().await;

        bucket.add_build("2026-01-02", test_bucket::template_version());
        bucket.set_latest("2026-01-02", Some(test_bucket::template_version()));
        let all = Configuration { water: "all".into(), ..state.get_configuration().await };
        let tasks = state.configure(all.clone()).await.unwrap();
        state.ensure_loaded("asia").await.unwrap();
        drop(holding);
        assert_eq!(first.await.unwrap(), Ok(()));
        for task in tasks {
            task.await.unwrap();
        }

        let stamp = std::fs::read_to_string(root.path().join("europe").join(BUILD_STAMP)).unwrap();
        assert_eq!(stamp, crate::downloader::stamp_text("2026-01-02", &bucket.url, &all.selection("europe")));
    }

    // A download fetches the files its manifest names from the bucket the
    // manifest came from, and a manifest is at hand only for the bucket it
    // came from: here europe's download waits on bucket a's manifest while
    // a configuration moves to bucket b, whose latest build is of another
    // date. Europe comes down from a, then again from b; asia, after them,
    // from b.
    #[tokio::test]
    async fn a_download_fetches_its_files_from_the_bucket_its_manifest_came_from() {
        let (a, b) = (test_bucket::serve(test_bucket::template_components()).await, test_bucket::serve(test_bucket::template_components()).await);
        b.add_build("2026-01-02", test_bucket::template_version());
        b.set_latest("2026-01-02", Some(test_bucket::template_version()));
        let (_root, state) = state_for(a.url.clone(), Duration::from_secs(3600)).await;
        let state = Arc::new(state);
        on_each_download(Rc::new(RefCell::new(Vec::new())));
        let holding = a.hold("configurations.json");
        let first = {
            let state = state.clone();
            tokio::spawn(async move { state.ensure_loaded("europe").await.map_err(|e| e.to_string()) })
        };
        holding.reached().await;

        let moved = Configuration { base_url: b.url.clone(), ..state.get_configuration().await };
        let tasks = state.configure(moved).await.unwrap();
        drop(holding);
        assert_eq!(first.await.unwrap(), Ok(()));
        for task in tasks {
            task.await.unwrap();
        }
        state.ensure_loaded("asia").await.unwrap();

        let (from_a, from_b) = (a.requests(), b.requests());
        assert!(!from_a.is_empty() && from_a.iter().all(|r| r.starts_with(&format!("{}/europe/", test_bucket::DATE))), "{from_a:?}");
        for region in ["europe", "asia"] {
            assert!(from_b.iter().any(|r| r.starts_with(&format!("2026-01-02/{region}/"))), "{region}: {from_b:?}");
        }
        assert!(from_b.iter().all(|r| r.starts_with("2026-01-02/")), "{from_b:?}");
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
