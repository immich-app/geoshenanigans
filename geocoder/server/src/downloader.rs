// Region downloader: resolves the file list for a configuration via
// configurations.json, fetches each .zst file from Tigris, verifies
// sha256 against the manifest, decompresses, and writes atomically.
//
// Used by the on-demand download path: when a request lands on a
// continent we don't have, the request handler triggers this and
// awaits completion before serving the geocode result.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;

use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Debug)]
pub enum DownloadError {
    Http(String),
    Io(String),
    HashMismatch { path: String, expected: String, got: String },
    NotInManifest(String),
    Decompress(String),
    UnknownRegion(String),
}

impl std::fmt::Display for DownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DownloadError::Http(s) => write!(f, "HTTP error: {}", s),
            DownloadError::Io(s) => write!(f, "I/O error: {}", s),
            DownloadError::HashMismatch { path, expected, got } => {
                write!(f, "sha256 mismatch on {}: expected {}, got {}", path, expected, got)
            }
            DownloadError::NotInManifest(p) => write!(f, "file not in manifest: {}", p),
            DownloadError::Decompress(s) => write!(f, "zstd decompress: {}", s),
            DownloadError::UnknownRegion(r) => write!(f, "unknown region: {}", r),
        }
    }
}

impl std::error::Error for DownloadError {}

// --- configurations.json shape (subset we care about) ------------------------

#[derive(Deserialize, Debug, Clone)]
pub struct Configurations {
    pub build: BuildMeta,
    pub components: BTreeMap<String, Component>,
    pub files: BTreeMap<String, FileEntry>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BuildMeta {
    pub date: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Component {
    #[serde(default)]
    pub files: Vec<String>,
    #[serde(default)]
    pub replaces: Vec<String>,
    #[serde(default, rename = "extends")]
    pub extends: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct FileEntry {
    pub size_zst: u64,
    pub size_raw: u64,
    pub sha256: String,
}

/// Per-region selection passed in by the configuration endpoint. The
/// quality/poi_tier strings have to match component IDs in
/// configurations.components (e.g. "q2.5", "major").
#[derive(Debug, Clone)]
pub struct RegionSelection {
    pub region: String,
    pub mode: String,
    pub quality: String,
    pub poi_tier: String,
}

/// Resolved file set for a single region after walking components +
/// applying `replaces`. Each entry is a path relative to the build root
/// (e.g. "europe/full/admin_cells.bin"); look up sizes/hashes in
/// configurations.files.
pub fn resolve_files(cfg: &Configurations, sel: &RegionSelection) -> Vec<String> {
    let mut files = Vec::new();
    let mut replaced = HashSet::new();

    let component_ids = [
        format!("mode.{}", sel.mode),
        format!("quality.{}", sel.quality),
        format!("poi_tier.{}", sel.poi_tier),
    ];

    fn substitute(path: &str, sel: &RegionSelection) -> String {
        path.replace("{region}", &sel.region)
            .replace("{mode}", &sel.mode)
            .replace("{quality}", &sel.quality)
            .replace("{poi_tier}", &sel.poi_tier)
    }

    fn visit(
        cfg: &Configurations, sel: &RegionSelection, id: &str,
        seen: &mut HashSet<String>, files: &mut Vec<String>,
        replaced: &mut HashSet<String>,
    ) {
        if !seen.insert(id.to_string()) { return; }
        let Some(c) = cfg.components.get(id) else { return; };
        if let Some(parent) = c.extends.as_deref() {
            visit(cfg, sel, parent, seen, files, replaced);
        }
        for f in &c.files { files.push(substitute(f, sel)); }
        for r in &c.replaces { replaced.insert(substitute(r, sel)); }
    }

    for cid in &component_ids {
        visit(cfg, sel, cid, &mut HashSet::new(), &mut files, &mut replaced);
    }

    files.into_iter()
        .filter(|f| !replaced.contains(f))
        .collect::<BTreeSet<_>>() // dedup + stable order
        .into_iter()
        .collect()
}

// --- Download a single file -------------------------------------------------

/// Fetch `{base_url}/builds/{date}/{path}.zst`, decompress, verify
/// sha256 against `expected.sha256`, write atomically to `dest`.
async fn download_file(
    client: &reqwest::Client,
    base_url: &str,
    date: &str,
    path: &str,
    expected: &FileEntry,
    dest: &Path,
) -> Result<(), DownloadError> {
    let url = format!("{}/builds/{}/{}.zst", base_url.trim_end_matches('/'), date, path);
    let resp = client.get(&url).send().await
        .map_err(|e| DownloadError::Http(format!("{}: {}", url, e)))?;
    if !resp.status().is_success() {
        return Err(DownloadError::Http(format!("{}: HTTP {}", url, resp.status())));
    }
    let zst_bytes = resp.bytes().await
        .map_err(|e| DownloadError::Http(format!("{}: read body: {}", url, e)))?;

    // Decompress in memory. Files top out around 1.5 GiB raw on planet
    // (full mode addr_*); fine for a server with the configured memory
    // budget. Streaming decode + hash would let us cap RAM at ~chunk
    // size, worth doing if multi-GB peaks become a problem.
    let raw = zstd::decode_all(zst_bytes.as_ref())
        .map_err(|e| DownloadError::Decompress(format!("{}: {}", path, e)))?;

    let mut hasher = Sha256::new();
    hasher.update(&raw);
    let got = format!("{:x}", hasher.finalize());
    if got != expected.sha256 {
        return Err(DownloadError::HashMismatch {
            path: path.to_string(),
            expected: expected.sha256.clone(),
            got,
        });
    }

    // Atomic write: tmp file + rename. dest's parent must exist; caller
    // ensures that.
    let tmp = dest.with_extension(format!(
        "{}.tmp", dest.extension().and_then(|s| s.to_str()).unwrap_or("")
    ));
    tokio::fs::write(&tmp, &raw).await
        .map_err(|e| DownloadError::Io(format!("write {}: {}", tmp.display(), e)))?;
    tokio::fs::rename(&tmp, dest).await
        .map_err(|e| DownloadError::Io(format!("rename {}→{}: {}", tmp.display(), dest.display(), e)))?;
    Ok(())
}

// --- Region download ---------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct DownloadProgress {
    pub region: String,
    pub files_done: usize,
    pub files_total: usize,
    pub bytes_done: u64,
    pub bytes_total: u64,
    pub current_file: Option<String>,
}

/// Resolve and download every file for a single region. Files that
/// already exist on disk with the right size are skipped (full sha256
/// re-verification on every restart would be expensive on planet-
/// sized data — size match is good enough as a fast path; the
/// complete sha256 check happens on first download).
///
/// `progress` fires after each file completes with the running total;
/// the request handler uses it to surface "downloading 3/35" responses.
pub async fn download_region(
    client: &reqwest::Client,
    base_url: &str,
    cfg: &Configurations,
    sel: &RegionSelection,
    root: &Path,
    progress: impl Fn(DownloadProgress) + Send + Sync,
) -> Result<(), DownloadError> {
    let paths = resolve_files(cfg, sel);
    if paths.is_empty() {
        return Err(DownloadError::UnknownRegion(sel.region.clone()));
    }

    let bytes_total: u64 = paths.iter()
        .filter_map(|p| cfg.files.get(p).map(|f| f.size_zst))
        .sum();
    let mut bytes_done: u64 = 0;
    let files_total = paths.len();

    progress(DownloadProgress {
        region: sel.region.clone(),
        files_total,
        bytes_total,
        ..Default::default()
    });

    for (i, path) in paths.iter().enumerate() {
        let entry = cfg.files.get(path)
            .ok_or_else(|| DownloadError::NotInManifest(path.clone()))?;

        let dest = root.join(path);
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await
                .map_err(|e| DownloadError::Io(format!("mkdir {}: {}", parent.display(), e)))?;
        }

        let already_correct = match tokio::fs::metadata(&dest).await {
            Ok(m) => m.len() == entry.size_raw,
            Err(_) => false,
        };

        if !already_correct {
            download_file(client, base_url, &cfg.build.date, path, entry, &dest).await?;
        }

        bytes_done = bytes_done.saturating_add(entry.size_zst);
        progress(DownloadProgress {
            region: sel.region.clone(),
            files_done: i + 1,
            files_total,
            bytes_done,
            bytes_total,
            current_file: Some(path.clone()),
        });
    }

    Ok(())
}
