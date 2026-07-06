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

    // Files land FLAT under <root>/<region>/ — Index::load (via
    // install_region) reads a flat dir, not the manifest's nested
    // {region}/{mode}/... layout. Later paths overwrite earlier basenames
    // (quality-variant admin polygons over the mode set), mirroring how
    // server dirs are assembled from build output.
    let region_dir = root.join(&sel.region);
    tokio::fs::create_dir_all(&region_dir).await
        .map_err(|e| DownloadError::Io(format!("mkdir {}: {}", region_dir.display(), e)))?;

    // Key the dir by build date: size-only resume checks across different
    // builds would otherwise mix files from two builds into one index.
    let stamp = region_dir.join(".build_date");
    let existing = tokio::fs::read_to_string(&stamp).await.unwrap_or_default();
    let fresh_build = existing.trim() != cfg.build.date;
    if fresh_build {
        tokio::fs::write(&stamp, &cfg.build.date).await
            .map_err(|e| DownloadError::Io(format!("write {}: {}", stamp.display(), e)))?;
    }

    for (i, path) in paths.iter().enumerate() {
        let entry = cfg.files.get(path)
            .ok_or_else(|| DownloadError::NotInManifest(path.clone()))?;

        let fname = Path::new(path).file_name()
            .ok_or_else(|| DownloadError::Io(format!("bad manifest path: {}", path)))?;
        let dest = region_dir.join(fname);

        let already_correct = !fresh_build && match tokio::fs::metadata(&dest).await {
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

// --- Pure-helper regression tests -------------------------------------------
//
// Lock in current behaviour of resolve_files (component graph walking) and
// the DownloadError Display strings.  No network/filesystem.
#[cfg(test)]
mod tests {
    use super::*;

    fn component(files: &[&str], replaces: &[&str], extends: Option<&str>) -> Component {
        Component {
            files: files.iter().map(|s| s.to_string()).collect(),
            replaces: replaces.iter().map(|s| s.to_string()).collect(),
            extends: extends.map(|s| s.to_string()),
        }
    }

    fn cfg(components: Vec<(&str, Component)>) -> Configurations {
        Configurations {
            build: BuildMeta { date: "2026-01-01".into() },
            components: components.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
            files: BTreeMap::new(),
        }
    }

    fn sel(region: &str, mode: &str, quality: &str, poi_tier: &str) -> RegionSelection {
        RegionSelection {
            region: region.to_string(),
            mode: mode.to_string(),
            quality: quality.to_string(),
            poi_tier: poi_tier.to_string(),
        }
    }

    #[test]
    fn resolve_files_substitutes_placeholders() {
        let c = cfg(vec![
            ("mode.full", component(
                &["{region}/{mode}/admin.bin", "{region}/{quality}/q.bin"],
                &[], None)),
            ("quality.q2.5", component(&["{region}/{quality}/cells.bin"], &[], None)),
            ("poi_tier.major", component(&["{region}/poi/{poi_tier}.bin"], &[], None)),
        ]);
        let s = sel("europe", "full", "q2.5", "major");
        let files = resolve_files(&c, &s);
        // BTreeSet => sorted, deduped. {region}->europe, {mode}->full,
        // {quality}->q2.5, {poi_tier}->major.
        assert_eq!(files, vec![
            "europe/full/admin.bin".to_string(),
            "europe/poi/major.bin".to_string(),
            "europe/q2.5/cells.bin".to_string(),
            "europe/q2.5/q.bin".to_string(),
        ]);
    }

    #[test]
    fn resolve_files_extends_chain_collects_parent_files() {
        // quality.q2.5 extends quality.base; both sets of files appear.
        let c = cfg(vec![
            ("mode.full", component(&["m.bin"], &[], None)),
            ("quality.base", component(&["base_a.bin", "base_b.bin"], &[], None)),
            ("quality.q2.5", component(&["q.bin"], &[], Some("quality.base"))),
            ("poi_tier.none", component(&[], &[], None)),
        ]);
        let s = sel("eu", "full", "q2.5", "none");
        let files = resolve_files(&c, &s);
        assert_eq!(files, vec![
            "base_a.bin".to_string(),
            "base_b.bin".to_string(),
            "m.bin".to_string(),
            "q.bin".to_string(),
        ]);
    }

    #[test]
    fn resolve_files_replaces_removes_matching_path() {
        // quality.q2.5 replaces the base admin file with a higher-quality one.
        let c = cfg(vec![
            ("mode.full", component(&["admin_lo.bin", "cells.bin"], &[], None)),
            ("quality.q2.5", component(&["admin_hi.bin"], &["admin_lo.bin"], None)),
            ("poi_tier.none", component(&[], &[], None)),
        ]);
        let s = sel("eu", "full", "q2.5", "none");
        let files = resolve_files(&c, &s);
        assert_eq!(files, vec![
            "admin_hi.bin".to_string(),
            "cells.bin".to_string(),
        ]);
        // admin_lo.bin was replaced out.
        assert!(!files.contains(&"admin_lo.bin".to_string()));
    }

    #[test]
    fn resolve_files_replaces_uses_substitution() {
        // A replaces entry containing {region} is substituted before matching.
        let c = cfg(vec![
            ("mode.full", component(&["{region}/admin_lo.bin"], &[], None)),
            ("quality.q2.5", component(&["{region}/admin_hi.bin"], &["{region}/admin_lo.bin"], None)),
            ("poi_tier.none", component(&[], &[], None)),
        ]);
        let s = sel("asia", "full", "q2.5", "none");
        let files = resolve_files(&c, &s);
        assert_eq!(files, vec!["asia/admin_hi.bin".to_string()]);
    }

    #[test]
    fn resolve_files_dedups_identical_paths() {
        // Same path emitted by two components collapses to one entry.
        let c = cfg(vec![
            ("mode.full", component(&["shared.bin"], &[], None)),
            ("quality.q2.5", component(&["shared.bin", "extra.bin"], &[], None)),
            ("poi_tier.none", component(&[], &[], None)),
        ]);
        let s = sel("eu", "full", "q2.5", "none");
        let files = resolve_files(&c, &s);
        assert_eq!(files, vec!["extra.bin".to_string(), "shared.bin".to_string()]);
    }

    #[test]
    fn resolve_files_missing_component_ids_yield_empty() {
        // No matching component ids at all -> empty result.
        let c = cfg(vec![("something.else", component(&["x.bin"], &[], None))]);
        let s = sel("eu", "full", "q2.5", "none");
        assert!(resolve_files(&c, &s).is_empty());
    }

    #[test]
    fn download_error_display_strings() {
        assert_eq!(DownloadError::Http("boom".into()).to_string(), "HTTP error: boom");
        assert_eq!(DownloadError::Io("nope".into()).to_string(), "I/O error: nope");
        assert_eq!(
            DownloadError::HashMismatch {
                path: "a/b.bin".into(),
                expected: "deadbeef".into(),
                got: "cafef00d".into(),
            }.to_string(),
            "sha256 mismatch on a/b.bin: expected deadbeef, got cafef00d"
        );
        assert_eq!(
            DownloadError::NotInManifest("x.bin".into()).to_string(),
            "file not in manifest: x.bin"
        );
        assert_eq!(
            DownloadError::Decompress("bad frame".into()).to_string(),
            "zstd decompress: bad frame"
        );
        assert_eq!(
            DownloadError::UnknownRegion("atlantis".into()).to_string(),
            "unknown region: atlantis"
        );
    }
}
