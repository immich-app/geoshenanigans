// Region state coordinator: holds the current Immich-supplied
// configuration, the cached configurations.json from the build, and
// the per-region download mutexes. Triggers downloads, installs
// completed regions into the live MultiIndex via arc-swap.
//
// Two flows feed this:
//
// 1. Configuration push (from Immich): full snapshot of what regions
//    the user wants, plus mode/quality/poi_tier. Server preloads
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

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::downloader::{
    download_region, Configurations, DownloadError, DownloadProgress, RegionSelection,
};
use query_server::MultiIndex;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Configuration {
    /// Base URL for the geocoder build artifacts. Latest.json + each
    /// build's configurations.json + .bin.zst files all hang off this.
    pub base_url: String,
    /// Address detail level — must match a `mode.*` component.
    pub mode: String,
    /// Boundary precision quality variant (e.g. "q2.5"). Must match
    /// a `quality.*` component.
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
            quality: "q2.5".to_string(),
            poi_tier: "none".to_string(),
            preload: Vec::new(),
        }
    }
}

pub struct RegionState {
    pub multi_index: Arc<MultiIndex>,
    pub root_dir: PathBuf,
    pub config_path: PathBuf,
    pub config: Mutex<Configuration>,
    /// Cached configurations.json for the build referenced by `config.base_url`.
    /// Loaded lazily on first download, refreshed when the configuration
    /// is updated (a config change might point at a different build).
    pub configurations: Mutex<Option<Configurations>>,
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
        // start from defaults until Immich pushes one.
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
    pub async fn set_configuration(self: &Arc<Self>, new_cfg: Configuration) -> std::io::Result<()> {
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
        if let Some(c) = self.configurations.lock().await.as_ref() {
            return Ok(c.clone());
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

        *self.configurations.lock().await = Some(configurations.clone());
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
            .any(|c| c.bbox.name == region && c.index.load().is_some())
        {
            return Ok(());
        }
        // Bail early if `region` isn't a known continent.
        if !self.multi_index.continents.iter().any(|c| c.bbox.name == region) {
            return Err(DownloadError::UnknownRegion(region.to_string()));
        }

        let configurations = self.ensure_configurations().await?;
        let cfg = self.config.lock().await.clone();
        let sel = RegionSelection {
            region: region.to_string(),
            mode: cfg.mode.clone(),
            quality: cfg.quality.clone(),
            poi_tier: cfg.poi_tier.clone(),
        };

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
