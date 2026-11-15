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
    /// The selection names a value on `axis` (mode, quality or poi_tier)
    /// that the build has no component for; `offered` lists those it has.
    NotOffered { axis: &'static str, value: String, offered: Vec<String> },
    /// The manifest names a component (as a parent or a variant) that it
    /// doesn't define.
    MissingComponent(String),
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
            DownloadError::NotOffered { axis, value, offered } => {
                let offered = if offered.is_empty() { "(none)".to_string() } else { offered.join(", ") };
                write!(f, "this build has no {axis} \"{value}\"; it offers {axis}: {offered}")
            }
            DownloadError::MissingComponent(id) => {
                write!(f, "the manifest names component \"{id}\" but does not define it")
            }
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
    /// Files the builder writes only when the data has them (TIGER
    /// sidecars, postcode centroids). Absent from the manifest = skip.
    #[serde(default)]
    pub optional_files: Vec<String>,
    /// Components of which the selection takes one more: the first whose
    /// `when` it matches.
    #[serde(default)]
    pub variants: Vec<Variant>,
}

/// A component a selection takes when it has every axis value `when` names
/// (an empty `when`: any selection).
#[derive(Deserialize, Debug, Clone)]
pub struct Variant {
    #[serde(default)]
    pub when: BTreeMap<String, String>,
    pub component: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct FileEntry {
    pub size_zst: u64,
    pub size_raw: u64,
    pub sha256: String,
}

/// The axes a selection takes one component of, `<axis>.<value>`.
pub const COMPONENT_AXES: [&str; 3] = ["mode", "quality", "poi_tier"];

impl Configurations {
    /// The values of `axis` the build has a component for, in order.
    pub fn offered(&self, axis: &str) -> Vec<String> {
        let prefix = format!("{axis}.");
        self.components.keys().filter_map(|id| id.strip_prefix(&prefix)).map(str::to_string).collect()
    }
}

/// Fails, naming the value and those the build offers, unless the build has
/// a component for each of `sel`'s mode, quality and poi_tier. The region
/// isn't checked: the manifest's files table holds it.
pub fn check_selection(cfg: &Configurations, sel: &RegionSelection) -> Result<(), DownloadError> {
    for axis in COMPONENT_AXES {
        let value = sel.axis(axis).unwrap_or_default();
        if !cfg.components.contains_key(&format!("{axis}.{value}")) {
            return Err(DownloadError::NotOffered { axis, value: value.to_string(), offered: cfg.offered(axis) });
        }
    }
    Ok(())
}

/// Per-region selection passed in by the configuration endpoint. The
/// mode/quality/poi_tier strings have to match component IDs in
/// configurations.components (e.g. "simplified", "major").
#[derive(Debug, Clone)]
pub struct RegionSelection {
    pub region: String,
    pub mode: String,
    pub quality: String,
    pub poi_tier: String,
}

impl RegionSelection {
    /// The selection's value on an axis of configurations.axes.
    pub fn axis(&self, name: &str) -> Option<&str> {
        match name {
            "region" => Some(&self.region),
            "mode" => Some(&self.mode),
            "quality" => Some(&self.quality),
            "poi_tier" => Some(&self.poi_tier),
            _ => None,
        }
    }

    fn matches(&self, when: &BTreeMap<String, String>) -> bool {
        when.iter().all(|(axis, value)| self.axis(axis) == Some(value.as_str()))
    }
}

/// Resolved file set for a single region after walking components (and
/// the variant of each the selection matches) + applying `replaces`. Each
/// path is relative to the build root (e.g.
/// "europe/full/admin_cells.bin"); look up sizes/hashes in
/// configurations.files.
pub struct ResolvedFiles {
    /// Every path to fetch, sorted and deduped.
    pub paths: Vec<String>,
    /// Paths that may legitimately be missing from the manifest.
    pub optional: HashSet<String>,
}

/// The files of `sel`, or an error when it names a value the build has no
/// component for or the manifest names a component it lacks: skipping one
/// would leave a set that can never load.
pub fn resolve_files(cfg: &Configurations, sel: &RegionSelection) -> Result<ResolvedFiles, DownloadError> {
    check_selection(cfg, sel)?;
    let mut files = Vec::new();
    let mut optional = HashSet::new();
    let mut replaced = HashSet::new();

    fn substitute(path: &str, sel: &RegionSelection) -> String {
        path.replace("{region}", &sel.region)
            .replace("{mode}", &sel.mode)
            .replace("{quality}", &sel.quality)
            .replace("{poi_tier}", &sel.poi_tier)
    }

    fn visit(
        cfg: &Configurations, sel: &RegionSelection, id: &str,
        seen: &mut HashSet<String>, files: &mut Vec<String>,
        optional: &mut HashSet<String>, replaced: &mut HashSet<String>,
    ) -> Result<(), DownloadError> {
        if !seen.insert(id.to_string()) { return Ok(()); }
        let c = cfg.components.get(id).ok_or_else(|| DownloadError::MissingComponent(id.to_string()))?;
        if let Some(parent) = c.extends.as_deref() {
            visit(cfg, sel, parent, seen, files, optional, replaced)?;
        }
        for f in &c.files { files.push(substitute(f, sel)); }
        for f in &c.optional_files {
            let path = substitute(f, sel);
            optional.insert(path.clone());
            files.push(path);
        }
        for r in &c.replaces { replaced.insert(substitute(r, sel)); }
        if let Some(v) = c.variants.iter().find(|v| sel.matches(&v.when)) {
            visit(cfg, sel, &v.component, seen, files, optional, replaced)?;
        }
        Ok(())
    }

    for axis in COMPONENT_AXES {
        let id = format!("{axis}.{}", sel.axis(axis).unwrap_or_default());
        visit(cfg, sel, &id, &mut HashSet::new(), &mut files, &mut optional, &mut replaced)?;
    }

    let paths: Vec<String> = files.into_iter()
        .filter(|f| !replaced.contains(f))
        .collect::<BTreeSet<_>>() // dedup + stable order
        .into_iter()
        .collect();
    optional.retain(|p| paths.contains(p));
    Ok(ResolvedFiles { paths, optional })
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
    // Stream: HTTP chunks → zstd streaming decoder → sha256 + tmp file.
    // The old path buffered the whole compressed body AND the whole
    // decompressed output in RAM (multi-GiB peaks on planet addr_* files),
    // which OOMs a memory-constrained deployment. Peak RAM is now a few
    // network chunks plus the zstd window, regardless of file size.
    // Decode + hash + write run on a blocking thread fed through a
    // bounded channel so the async executor never blocks on disk I/O.
    let tmp = dest.with_extension(format!(
        "{}.tmp", dest.extension().and_then(|s| s.to_str()).unwrap_or("")
    ));

    let (tx, rx) = std::sync::mpsc::sync_channel::<bytes::Bytes>(8);
    let tmp_for_writer = tmp.clone();
    let path_owned = path.to_string();
    let writer = tokio::task::spawn_blocking(move || {
        decode_hash_write(rx, &tmp_for_writer, &path_owned)
    });

    let mut stream = resp.bytes_stream();
    let mut stream_err: Option<DownloadError> = None;
    use futures_util::StreamExt;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(b) => {
                if tx.send(b).is_err() {
                    break; // writer died — its error surfaces below
                }
            }
            Err(e) => {
                stream_err = Some(DownloadError::Http(format!("{}: read body: {}", url, e)));
                break;
            }
        }
    }
    drop(tx);
    let writer_result = writer.await
        .map_err(|e| DownloadError::Io(format!("writer task: {}", e)));
    let got = match (stream_err, writer_result) {
        (Some(e), _) | (None, Err(e)) | (None, Ok(Err(e))) => {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        (None, Ok(Ok(hash))) => hash,
    };

    if got != expected.sha256 {
        let _ = std::fs::remove_file(&tmp);
        return Err(DownloadError::HashMismatch {
            path: path.to_string(),
            expected: expected.sha256.clone(),
            got,
        });
    }

    tokio::fs::rename(&tmp, dest).await
        .map_err(|e| DownloadError::Io(format!("rename {}→{}: {}", tmp.display(), dest.display(), e)))?;
    Ok(())
}

/// Blocking half of the streaming download: consume compressed chunks
/// from `rx`, zstd-decode them, write the decompressed bytes to `tmp`
/// and hash them incrementally. Returns the sha256 hex of the
/// decompressed content. RAM use is bounded by the channel depth plus
/// the zstd window regardless of file size.
fn decode_hash_write(
    rx: std::sync::mpsc::Receiver<bytes::Bytes>,
    tmp: &Path,
    label: &str,
) -> Result<String, DownloadError> {
    use std::io::Write;
    struct HashWriter {
        file: std::io::BufWriter<std::fs::File>,
        hasher: Sha256,
    }
    impl std::io::Write for HashWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.hasher.update(buf);
            self.file.write_all(buf)?;
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.file.flush()
        }
    }
    let file = std::fs::File::create(tmp)
        .map_err(|e| DownloadError::Io(format!("create {}: {}", tmp.display(), e)))?;
    let hw = HashWriter { file: std::io::BufWriter::new(file), hasher: Sha256::new() };
    let mut dec = zstd::stream::write::Decoder::new(hw)
        .map_err(|e| DownloadError::Decompress(format!("{}: {}", label, e)))?;
    while let Ok(chunk) = rx.recv() {
        dec.write_all(&chunk)
            .map_err(|e| DownloadError::Decompress(format!("{}: {}", label, e)))?;
    }
    dec.flush()
        .map_err(|e| DownloadError::Decompress(format!("{}: {}", label, e)))?;
    let mut hw = dec.into_inner();
    hw.flush()
        .map_err(|e| DownloadError::Io(format!("flush {}: {}", tmp.display(), e)))?;
    Ok(format!("{:x}", hw.hasher.finalize()))
}

// --- Region download ---------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct DownloadProgress {
    pub files_done: usize,
    pub files_total: usize,
    pub bytes_done: u64,
    pub bytes_total: u64,
}

/// Resolve and download every file for a single region. Files that
/// already exist on disk with the right size are skipped (full sha256
/// re-verification on every restart would be expensive on planet-
/// sized data — size match is good enough as a fast path; the
/// complete sha256 check happens on first download).
///
/// `progress` fires after each file completes with the running total;
/// the caller logs it.
pub async fn download_region(
    client: &reqwest::Client,
    base_url: &str,
    cfg: &Configurations,
    sel: &RegionSelection,
    root: &Path,
    progress: impl Fn(DownloadProgress) + Send + Sync,
) -> Result<(), DownloadError> {
    let resolved = resolve_files(cfg, sel)?;
    if resolved.paths.is_empty() {
        return Err(DownloadError::UnknownRegion(sel.region.clone()));
    }
    let paths: Vec<&String> = resolved.paths.iter()
        .filter(|p| cfg.files.contains_key(*p) || !resolved.optional.contains(*p))
        .collect();

    let bytes_total: u64 = paths.iter()
        .filter_map(|p| cfg.files.get(*p).map(|f| f.size_zst))
        .sum();
    let mut bytes_done: u64 = 0;
    let files_total = paths.len();

    progress(DownloadProgress {
        files_total,
        bytes_total,
        ..Default::default()
    });

    // Files land FLAT under <root>/<region>/ — Index::load (via
    // install_region) reads a flat dir, not the manifest's nested
    // {region}/{mode}/... layout. A later path overwrites an earlier
    // basename: no two .bin files of a template selection share one (its
    // tests check it), and the strings_layout.json each dir holds are alike.
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
        let entry = cfg.files.get(*path)
            .ok_or_else(|| DownloadError::NotInManifest((*path).clone()))?;

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
            files_done: i + 1,
            files_total,
            bytes_done,
            bytes_total,
        });
    }

    Ok(())
}

// --- Regression tests ---------------------------------------------------------
//
// Lock in current behaviour of resolve_files (component graph walking) and
// the DownloadError Display strings; the download tests fetch from a bucket
// on localhost (test_bucket.rs) into scratch dirs.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::scratch_dir::ScratchDir;
    use crate::test_bucket::{self, TestBucket};

    #[test]
    fn streaming_decode_hash_roundtrip() {
        // 3 MiB of patterned data, compressed, fed through the channel in
        // small chunks — hash and file content must match the original.
        let raw: Vec<u8> = (0..3 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
        let compressed = zstd::encode_all(&raw[..], 3).unwrap();

        let dir = ScratchDir::new();
        let tmp = dir.path().join("out.bin.tmp");

        let (tx, rx) = std::sync::mpsc::sync_channel::<bytes::Bytes>(8);
        let feeder = std::thread::spawn(move || {
            for chunk in compressed.chunks(64 * 1024) {
                tx.send(bytes::Bytes::copy_from_slice(chunk)).unwrap();
            }
        });
        let got = decode_hash_write(rx, &tmp, "test").unwrap();
        feeder.join().unwrap();

        let mut hasher = Sha256::new();
        hasher.update(&raw);
        assert_eq!(got, format!("{:x}", hasher.finalize()));
        assert_eq!(std::fs::read(&tmp).unwrap(), raw);
    }

    fn component(files: &[&str], replaces: &[&str], extends: Option<&str>) -> Component {
        Component {
            files: files.iter().map(|s| s.to_string()).collect(),
            replaces: replaces.iter().map(|s| s.to_string()).collect(),
            extends: extends.map(|s| s.to_string()),
            optional_files: Vec::new(),
            variants: Vec::new(),
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
            ("quality.simplified", component(&["{region}/{quality}/cells.bin"], &[], None)),
            ("poi_tier.major", component(&["{region}/poi/{poi_tier}.bin"], &[], None)),
        ]);
        let s = sel("europe", "full", "simplified", "major");
        let files = resolve_files(&c, &s).unwrap().paths;
        // BTreeSet => sorted, deduped. {region}->europe, {mode}->full,
        // {quality}->simplified, {poi_tier}->major.
        assert_eq!(files, vec![
            "europe/full/admin.bin".to_string(),
            "europe/poi/major.bin".to_string(),
            "europe/simplified/cells.bin".to_string(),
            "europe/simplified/q.bin".to_string(),
        ]);
    }

    #[test]
    fn resolve_files_extends_chain_collects_parent_files() {
        // quality.simplified extends quality.base; both sets of files appear.
        let c = cfg(vec![
            ("mode.full", component(&["m.bin"], &[], None)),
            ("quality.base", component(&["base_a.bin", "base_b.bin"], &[], None)),
            ("quality.simplified", component(&["q.bin"], &[], Some("quality.base"))),
            ("poi_tier.none", component(&[], &[], None)),
        ]);
        let s = sel("eu", "full", "simplified", "none");
        let files = resolve_files(&c, &s).unwrap().paths;
        assert_eq!(files, vec![
            "base_a.bin".to_string(),
            "base_b.bin".to_string(),
            "m.bin".to_string(),
            "q.bin".to_string(),
        ]);
    }

    #[test]
    fn resolve_files_replaces_removes_matching_path() {
        // quality.simplified replaces the base admin file with a higher-quality one.
        let c = cfg(vec![
            ("mode.full", component(&["admin_lo.bin", "cells.bin"], &[], None)),
            ("quality.simplified", component(&["admin_hi.bin"], &["admin_lo.bin"], None)),
            ("poi_tier.none", component(&[], &[], None)),
        ]);
        let s = sel("eu", "full", "simplified", "none");
        let files = resolve_files(&c, &s).unwrap().paths;
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
            ("quality.simplified", component(&["{region}/admin_hi.bin"], &["{region}/admin_lo.bin"], None)),
            ("poi_tier.none", component(&[], &[], None)),
        ]);
        let s = sel("asia", "full", "simplified", "none");
        let files = resolve_files(&c, &s).unwrap().paths;
        assert_eq!(files, vec!["asia/admin_hi.bin".to_string()]);
    }

    #[test]
    fn resolve_files_dedups_identical_paths() {
        // Same path emitted by two components collapses to one entry.
        let c = cfg(vec![
            ("mode.full", component(&["shared.bin"], &[], None)),
            ("quality.simplified", component(&["shared.bin", "extra.bin"], &[], None)),
            ("poi_tier.none", component(&[], &[], None)),
        ]);
        let s = sel("eu", "full", "simplified", "none");
        let files = resolve_files(&c, &s).unwrap().paths;
        assert_eq!(files, vec!["extra.bin".to_string(), "shared.bin".to_string()]);
    }

    // A value the build has no component for would otherwise be skipped,
    // leaving a set without that component's files that can never load.
    #[test]
    fn resolve_files_refuses_a_value_the_build_has_no_component_for() {
        let (_, c) = template();
        let not_offered = |s: RegionSelection| match resolve_files(&c, &s) {
            Err(DownloadError::NotOffered { axis, value, offered }) => (axis, value, offered),
            Err(e) => panic!("{s:?}: {e}"),
            Ok(r) => panic!("{s:?} resolved to {:?}", r.paths),
        };
        let strings = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();

        assert_eq!(not_offered(sel("europe", "admin", "q2.5", "none")),
                   ("quality", "q2.5".to_string(), strings(&["exact", "simplified"])));
        assert_eq!(not_offered(sel("europe", "ful", "simplified", "major")),
                   ("mode", "ful".to_string(), strings(&["admin", "admin-minimal", "full", "no-addresses"])));
        assert_eq!(not_offered(sel("europe", "full", "simplified", "majr")),
                   ("poi_tier", "majr".to_string(), strings(&["all", "major", "none", "notable"])));
    }

    #[test]
    fn resolve_files_refuses_a_component_the_manifest_names_but_lacks() {
        let missing = |components: serde_json::Value| {
            let c = Configurations {
                build: BuildMeta { date: "2026-01-01".into() },
                components: serde_json::from_value(components).unwrap(),
                files: BTreeMap::new(),
            };
            match resolve_files(&c, &sel("eu", "full", "simplified", "none")) {
                Err(DownloadError::MissingComponent(id)) => id,
                Err(e) => panic!("{e}"),
                Ok(r) => panic!("resolved to {:?}", r.paths),
            }
        };

        assert_eq!(missing(serde_json::json!({
            "mode.full": { "files": ["{region}/full/a.bin"], "extends": "full/base" },
            "quality.simplified": {}, "poi_tier.none": {}
        })), "full/base");
        assert_eq!(missing(serde_json::json!({
            "mode.full": { "files": ["{region}/full/a.bin"], "variants": [{ "component": "full/alone" }] },
            "quality.simplified": {}, "poi_tier.none": {}
        })), "full/alone");
    }

    #[test]
    fn resolve_files_takes_the_first_variant_the_selection_matches() {
        let c = Configurations {
            build: BuildMeta { date: "2026-01-01".into() },
            components: serde_json::from_value(serde_json::json!({
                "mode.small": {
                    "files": ["{region}/small/a.bin"],
                    "variants": [
                        { "when": { "mode": "small", "poi_tier": "none" }, "component": "small/alone" },
                        { "component": "small/beside" },
                        { "component": "small/never" }
                    ]
                },
                "small/alone": { "files": ["{region}/small/b.bin"] },
                "small/beside": {
                    "files": ["{region}/small-poi/b.bin"],
                    "replaces": ["{region}/small/a.bin"]
                },
                "small/never": { "files": ["{region}/never.bin"] },
                "quality.simplified": {},
                "poi_tier.none": {},
                "poi_tier.major": { "files": ["{region}/poi/{poi_tier}/p.bin"] }
            })).unwrap(),
            files: BTreeMap::new(),
        };

        assert_eq!(resolve_files(&c, &sel("eu", "small", "simplified", "none")).unwrap().paths,
                   vec!["eu/small/a.bin".to_string(), "eu/small/b.bin".to_string()]);
        assert_eq!(resolve_files(&c, &sel("eu", "small", "simplified", "major")).unwrap().paths,
                   vec!["eu/poi/major/p.bin".to_string(), "eu/small-poi/b.bin".to_string()]);
    }

    #[test]
    fn resolve_files_marks_optional_files() {
        let mut full = component(&["{region}/full/admin_cells.bin"], &[], None);
        full.optional_files = vec!["{region}/full/interp_postcodes.bin".to_string()];
        let c = cfg(vec![
            ("mode.full", full),
            ("quality.simplified", component(&[], &[], None)),
            ("poi_tier.none", component(&[], &[], None)),
        ]);
        let s = sel("europe", "full", "simplified", "none");

        let resolved = resolve_files(&c, &s).unwrap();

        assert_eq!(resolved.paths, vec![
            "europe/full/admin_cells.bin".to_string(),
            "europe/full/interp_postcodes.bin".to_string(),
        ]);
        assert!(resolved.optional.contains("europe/full/interp_postcodes.bin"));
        assert!(!resolved.optional.contains("europe/full/admin_cells.bin"));
    }

    /// The template, as JSON and as the downloader reads it.
    fn template() -> (serde_json::Value, Configurations) {
        let v: serde_json::Value = serde_json::from_str(include_str!("../../builder/configurations.template.json")).unwrap();
        let c = Configurations {
            build: BuildMeta { date: "2026-01-01".into() },
            components: serde_json::from_value(v["components"].clone()).unwrap(),
            files: BTreeMap::new(),
        };
        (v, c)
    }

    fn axis_values(v: &serde_json::Value, axis: &str) -> Vec<String> {
        v["axes"][axis]["values"].as_object().unwrap().keys().cloned().collect()
    }

    /// Whether `s` keeps the template's constraints: a selection with every
    /// value a constraint's `when` names has every value its `requires` names.
    fn allowed(v: &serde_json::Value, s: &RegionSelection) -> bool {
        v["constraints"].as_array().unwrap().iter().all(|c| {
            let has = |key: &str| c[key].as_object().unwrap().iter().all(|(axis, value)| s.axis(axis) == value.as_str());
            !has("when") || has("requires")
        })
    }

    /// Every mode × quality × poi_tier the template's constraints allow, in
    /// europe, and its presets' selections.
    fn template_selections(v: &serde_json::Value) -> Vec<RegionSelection> {
        let mut all = Vec::new();
        for mode in axis_values(v, "mode") {
            for quality in axis_values(v, "quality") {
                for poi_tier in axis_values(v, "poi_tier") {
                    all.push(sel("europe", &mode, &quality, &poi_tier));
                }
            }
        }
        all.retain(|s| allowed(v, s));
        for preset in v["presets"].as_array().unwrap() {
            let axis = |name: &str| preset["selections"][name].as_str().unwrap().to_string();
            all.push(sel(&axis("region"), &axis("mode"), &axis("quality"), &axis("poi_tier")));
        }
        all
    }

    fn basename(path: &str) -> &str {
        path.rsplit('/').next().unwrap()
    }

    // Index::load needs strings_layout.json in every region dir and
    // PoiMeta::load reads poi_meta.json; a selection that resolves without
    // them downloads GiBs into a dir that can never load.
    #[test]
    fn template_selections_include_load_sidecars() {
        let (v, c) = template();
        for s in template_selections(&v) {
            let paths = resolve_files(&c, &s).unwrap().paths;
            let has = |name: &str| paths.iter().any(|p| basename(p) == name);
            assert!(has("strings_layout.json"), "{:?} lacks strings_layout.json", s);
            assert!(has("admin_cells.bin"), "{:?} lacks admin_cells.bin", s);
            assert_eq!(has("poi_meta.json"), s.poi_tier != "none", "{:?} poi_meta.json", s);
        }
    }

    // admin-minimal/ holds strings of its own, which a POI tier's names and
    // parent polygons don't point into: beside a POI tier the mode takes
    // admin-minimal-poi/, on the shared slots and strings.
    #[test]
    fn template_admin_minimal_takes_its_poi_dir_beside_a_poi_tier() {
        let (v, c) = template();
        let files = ["admin_cells.bin", "admin_entries.bin", "admin_polygons.bin", "admin_rings.bin",
                     "arc_index.bin", "arc_vertices.bin", "place_cells.bin", "place_hidden.bin", "place_nodes.bin", "strings_core.bin",
                     "strings_layout.json"];
        let minimal: Vec<RegionSelection> = template_selections(&v).into_iter()
            .filter(|s| s.mode == "admin-minimal" && s.region == "europe")
            .collect();
        assert_eq!(minimal.len(), axis_values(&v, "poi_tier").len());
        for s in minimal {
            let paths = resolve_files(&c, &s).unwrap().paths;
            let (dir, other) = if s.poi_tier == "none" {
                ("europe/admin-minimal/", "europe/admin-minimal-poi/")
            } else {
                ("europe/admin-minimal-poi/", "europe/admin-minimal/")
            };
            let in_dir: Vec<&str> = paths.iter().filter_map(|p| p.strip_prefix(dir)).collect();
            assert_eq!(in_dir, files, "{:?}", s);
            let stray: Vec<&String> = paths.iter()
                .filter(|p| p.starts_with(other) || p.starts_with("europe/quality/"))
                .collect();
            assert!(stray.is_empty(), "{:?}: {:?}", s, stray);
        }
    }

    // Downloads land flat by basename, so a selection taking two copies of an
    // admin geometry file, or a .bin name twice, would mix two sets of arcs.
    #[test]
    fn template_selections_take_each_admin_geometry_file_once_from_their_dir() {
        let (v, c) = template();
        for s in template_selections(&v) {
            let paths = resolve_files(&c, &s).unwrap().paths;
            let dir = match (s.mode.as_str(), s.poi_tier.as_str()) {
                ("admin-minimal", "none") => format!("{}/admin-minimal/", s.region),
                ("admin-minimal", _) => format!("{}/admin-minimal-poi/", s.region),
                _ => format!("{}/quality/{}/", s.region, s.quality),
            };
            for name in std::iter::once("admin_polygons.bin").chain(query_server::ADMIN_GEOMETRY_FILES) {
                let copies: Vec<&String> = paths.iter().filter(|p| basename(p) == name).collect();
                assert_eq!(copies, [&format!("{dir}{name}")], "{:?}", s);
            }
            let mut bins: Vec<&str> = paths.iter().map(|p| basename(p)).filter(|n| n.ends_with(".bin")).collect();
            let count = bins.len();
            bins.sort_unstable();
            bins.dedup();
            assert_eq!(bins.len(), count, "{:?}: {:?}", s, paths);
            let stale: Vec<&String> = paths.iter()
                .filter(|p| basename(p) == "admin_vertices.bin" || basename(p).starts_with("postal_"))
                .collect();
            assert!(stale.is_empty(), "{:?}: {:?}", s, stale);
        }
    }

    #[test]
    fn the_default_configuration_is_a_selection_the_template_allows() {
        let (v, c) = template();
        let d = crate::region_state::Configuration::default();
        let s = sel("europe", &d.mode, &d.quality, &d.poi_tier);

        for (axis, value) in [("mode", &d.mode), ("quality", &d.quality), ("poi_tier", &d.poi_tier)] {
            assert!(axis_values(&v, axis).contains(value), "{axis} {value}");
        }
        assert!(allowed(&v, &s));
        assert!(resolve_files(&c, &s).unwrap().paths.iter().any(|p| p == &format!("europe/quality/{}/arc_vertices.bin", d.quality)));
    }

    async fn download(bucket: &TestBucket, s: &RegionSelection, root: &Path) -> Result<(), DownloadError> {
        download_region(&reqwest::Client::new(), &bucket.url, &bucket.manifest, s, root, |_| {}).await
    }

    // Skipping a value the build lacks fetched the rest of the selection and
    // succeeded: a q2.5 selection got no admin geometry, and install failed.
    #[tokio::test]
    async fn a_download_of_a_value_the_build_lacks_fails_naming_it_before_fetching_a_file() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        for (s, error) in [
            (sel("europe", "admin", "q2.5", "none"),
             "this build has no quality \"q2.5\"; it offers quality: exact, simplified"),
            (sel("europe", "ful", "simplified", "major"),
             "this build has no mode \"ful\"; it offers mode: admin, admin-minimal, full, no-addresses"),
        ] {
            let root = ScratchDir::new();

            let got = download(&bucket, &s, root.path()).await.map_err(|e| e.to_string());

            assert_eq!(got, Err(error.to_string()), "{s:?}");
            assert!(!root.path().join(&s.region).exists(), "{s:?}");
        }
        assert_eq!(bucket.files_served(), 0);
    }

    #[tokio::test]
    async fn a_download_naming_a_component_the_manifest_lacks_fails_before_fetching_a_file() {
        let bucket = test_bucket::serve(serde_json::json!({
            "mode.full": { "files": ["{region}/full/a.bin"], "variants": [{ "component": "full/alone" }] },
            "quality.simplified": {},
            "poi_tier.none": {}
        })).await;
        let root = ScratchDir::new();

        let got = download(&bucket, &sel("europe", "full", "simplified", "none"), root.path()).await.map_err(|e| e.to_string());

        assert_eq!(got, Err("the manifest names component \"full/alone\" but does not define it".to_string()));
        assert_eq!(bucket.files_served(), 0);
    }

    // The checks refuse no selection the template allows, nor a preset's.
    #[tokio::test]
    async fn every_template_selection_downloads_its_admin_geometry() {
        let (v, _) = template();
        let bucket = test_bucket::serve(v["components"].clone()).await;
        for s in template_selections(&v) {
            let root = ScratchDir::new();

            download(&bucket, &s, root.path()).await.unwrap_or_else(|e| panic!("{s:?}: {e}"));

            for name in ["admin_cells.bin", "strings_layout.json", "admin_polygons.bin"].into_iter().chain(query_server::ADMIN_GEOMETRY_FILES) {
                assert!(root.path().join(&s.region).join(name).exists(), "{s:?} {name}");
            }
        }
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
        assert_eq!(
            DownloadError::NotOffered { axis: "quality", value: "q2.5".into(), offered: vec!["exact".into(), "simplified".into()] }.to_string(),
            "this build has no quality \"q2.5\"; it offers quality: exact, simplified"
        );
        assert_eq!(
            DownloadError::NotOffered { axis: "mode", value: "full".into(), offered: Vec::new() }.to_string(),
            "this build has no mode \"full\"; it offers mode: (none)"
        );
        assert_eq!(
            DownloadError::MissingComponent("dir.admin-minimal".into()).to_string(),
            "the manifest names component \"dir.admin-minimal\" but does not define it"
        );
    }
}
