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
// region is downloaded at most once even under request fan-in.

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

/// The selection a server downloads. A mode, quality or poi_tier its build
/// has no component for fails each download, naming the values the build
/// offers; a PUT naming one is refused when the build's manifest is at hand.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Configuration {
    /// Base URL for the geocoder build artifacts. Latest.json + each
    /// build's configurations.json + .bin.zst files all hang off this.
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
    pub async fn ensure_loaded(&self, region: &str) -> Result<(), DownloadError> {
        // Per-region serialisation. Acquire the inner mutex so only
        // one concurrent request runs the download below; the rest
        // wait, find the region already installed, and return.
        let inner = {
            let mut g = self.region_locks.lock().await;
            g.entry(region.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _guard = inner.lock().await;

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

        self.multi_index.install_region(region)
            .map_err(DownloadError::UnknownRegion)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scratch_dir::ScratchDir;
    use crate::test_bucket;
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
        let state = RegionState::new(Arc::new(mi), root.path().to_path_buf());

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
}
