// Region downloader: resolves the file list for a configuration via
// configurations.json, fetches each file from Tigris as published (its .zst,
// or the transport unit that holds it, once for all its files), decompresses
// or decodes it, verifies each file's sha256 against the manifest, and
// writes atomically.
//
// Used by the on-demand download path: when a request lands on a
// continent we don't have, the request handler triggers this and
// awaits completion before serving the geocode result.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;

use query_server::lower_hex;
use query_server::transport::{self, ChunkReader, Expected, TRANSPORT_VERSION};
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
    /// A transport unit that can't be taken or failed to decode into its
    /// files: the unit's path and why.
    Transport(String),
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
            DownloadError::Transport(s) => write!(f, "transport unit {}", s),
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
    /// The build's transport units, by path.
    #[serde(default)]
    pub units: BTreeMap<String, UnitEntry>,
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
    /// The bytes of the file's .zst; none for a file a transport unit holds.
    #[serde(default)]
    pub size_zst: Option<u64>,
    pub size_raw: u64,
    pub sha256: String,
    /// The transport unit holding the file, its only published form.
    #[serde(default)]
    pub unit: Option<String>,
}

/// A transport unit (query_server::transport): the download-only form of
/// the files it holds, all in one dir.
#[derive(Deserialize, Debug, Clone)]
pub struct UnitEntry {
    pub transport_version: u32,
    /// Its bytes on the wire.
    pub size: u64,
    /// The paths of its files.
    pub files: Vec<String>,
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

/// Feeds the body of `resp` to a blocking decoder through `tx`, chunk by
/// chunk; stops early when the decoder has gone (its error tells why). The
/// body's read error, if any.
async fn send_body(resp: reqwest::Response, tx: &std::sync::mpsc::SyncSender<bytes::Bytes>, url: &str) -> Option<DownloadError> {
    use futures_util::StreamExt;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(b) => {
                if tx.send(b).is_err() {
                    return None; // writer died — its error surfaces below
                }
            }
            Err(e) => return Some(DownloadError::Http(format!("{}: read body: {}", url, e))),
        }
    }
    None
}

async fn get(client: &reqwest::Client, url: &str) -> Result<reqwest::Response, DownloadError> {
    let resp = client.get(url).send().await
        .map_err(|e| DownloadError::Http(format!("{}: {}", url, e)))?;
    if !resp.status().is_success() {
        return Err(DownloadError::Http(format!("{}: HTTP {}", url, resp.status())));
    }
    Ok(resp)
}

/// Fetch `{base_url}/builds/{date}/{path}.zst`, a file published as its own
/// .zst, decompress, verify sha256 against `expected.sha256`, write
/// atomically to `dest`.
async fn download_file(
    client: &reqwest::Client,
    base_url: &str,
    date: &str,
    path: &str,
    expected: &FileEntry,
    dest: &Path,
) -> Result<(), DownloadError> {
    let url = format!("{}/builds/{}/{}.zst", base_url.trim_end_matches('/'), date, path);
    let resp = get(client, &url).await?;
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

    let stream_err = send_body(resp, &tx, &url).await;
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
    Ok(lower_hex(&hw.hasher.finalize()))
}

// --- Download a transport unit -----------------------------------------------

/// Fetch the transport unit `{base_url}/builds/{date}/{unit}` and decode it
/// into `dir` as it streams in (transport::decode_into_dir, on a blocking
/// thread fed through a bounded channel): its files are written and hashed
/// as they come, must be `files` with their sizes and sha256, and are
/// renamed into place together once all are; on an error none is. RAM use
/// is the channel, the zstd window and one chunk of the unit's columns.
async fn download_unit(
    client: &reqwest::Client,
    base_url: &str,
    date: &str,
    unit: &str,
    files: &[(&str, &FileEntry)],
    dir: &Path,
) -> Result<(), DownloadError> {
    let url = format!("{}/builds/{}/{}", base_url.trim_end_matches('/'), date, unit);
    let resp = get(client, &url).await?;
    let expect: Vec<Expected> = files.iter()
        .map(|(path, e)| Expected { name: basename(path).to_string(), sha256: e.sha256.clone(), size: Some(e.size_raw) })
        .collect();
    let (tx, rx) = std::sync::mpsc::sync_channel::<bytes::Bytes>(8);
    let dir = dir.to_path_buf();
    let decoder = tokio::task::spawn_blocking(move || {
        transport::decode_into_dir(ChunkReader::new(rx), &dir, Some(&expect)).map(|_| ())
    });
    let stream_err = send_body(resp, &tx, &url).await;
    drop(tx);
    let decoded = decoder.await
        .map_err(|e| DownloadError::Io(format!("decoder task: {}", e)))?;
    if let Some(e) = stream_err {
        return Err(e);
    }
    decoded.map_err(|e| DownloadError::Transport(format!("{}: {}", unit, e)))
}

fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

// --- Region download ---------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct DownloadProgress {
    pub files_done: usize,
    pub files_total: usize,
    pub bytes_done: u64,
    pub bytes_total: u64,
}

/// One object a region's download fetches.
enum Fetch<'a> {
    /// A file, through its own .zst of `size` bytes.
    File { path: &'a str, entry: &'a FileEntry, size: u64 },
    /// A transport unit of `size` bytes, for its files.
    Unit { path: &'a str, size: u64, files: Vec<(&'a str, &'a FileEntry)> },
}

impl Fetch<'_> {
    fn size(&self) -> u64 {
        match self {
            Fetch::File { size, .. } | Fetch::Unit { size, .. } => *size,
        }
    }

    fn files(&self) -> usize {
        match self {
            Fetch::File { .. } => 1,
            Fetch::Unit { files, .. } => files.len(),
        }
    }
}

/// What a download of `paths` fetches, in path order: each file's .zst, or
/// its transport unit, once for all the files it holds. A unit must be of
/// this build's transport version, and the selection must take all of its
/// files: decoding it writes every one.
fn fetches<'a>(cfg: &'a Configurations, paths: &[&'a String]) -> Result<Vec<Fetch<'a>>, DownloadError> {
    let taken: HashSet<&str> = paths.iter().map(|p| p.as_str()).collect();
    let mut units = HashSet::new();
    let mut out = Vec::new();
    for path in paths {
        let entry = cfg.files.get(*path)
            .ok_or_else(|| DownloadError::NotInManifest((*path).clone()))?;
        let Some(u) = entry.unit.as_deref() else {
            let size = entry.size_zst.ok_or_else(|| DownloadError::NotInManifest(format!("{}.zst", path)))?;
            out.push(Fetch::File { path: path.as_str(), entry, size });
            continue;
        };
        if !units.insert(u) {
            continue;
        }
        let unit = cfg.units.get(u)
            .ok_or_else(|| DownloadError::Transport(format!("{}: not in the manifest's units", u)))?;
        if unit.transport_version != TRANSPORT_VERSION {
            return Err(DownloadError::Transport(format!(
                "{}: a unit of transport version {}: this build reads version {} only",
                u, unit.transport_version, TRANSPORT_VERSION
            )));
        }
        if !unit.files.iter().any(|f| f == *path) {
            return Err(DownloadError::Transport(format!("{}: doesn't hold {}, which names it", u, path)));
        }
        let mut files = Vec::with_capacity(unit.files.len());
        for f in &unit.files {
            if !taken.contains(f.as_str()) {
                return Err(DownloadError::Transport(format!("{}: holds {}, which the selection doesn't take", u, f)));
            }
            files.push((f.as_str(), cfg.files.get(f).ok_or_else(|| DownloadError::NotInManifest(f.clone()))?));
        }
        out.push(Fetch::Unit { path: u, size: unit.size, files });
    }
    Ok(out)
}

/// Whether the file at `path` is in `dir` at its size.
async fn on_disk(dir: &Path, path: &str, entry: &FileEntry) -> bool {
    matches!(tokio::fs::metadata(dir.join(basename(path))).await, Ok(m) if m.len() == entry.size_raw)
}

/// Resolve and download every file for a single region. Files that
/// already exist on disk with the right size are skipped, and a transport
/// unit when all its files are (full sha256 re-verification on every
/// restart would be expensive on planet-sized data — size match is good
/// enough as a fast path; the complete sha256 check happens on first
/// download). A unit's files land together, so a download needs the
/// largest unit's files' size free beside the region.
///
/// `progress` fires after each file or unit completes with the running
/// total; the caller logs it.
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
    let fetches = fetches(cfg, &paths)?;

    let bytes_total: u64 = fetches.iter().map(Fetch::size).sum();
    let mut bytes_done: u64 = 0;
    let files_total = paths.len();
    let mut files_done = 0;

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
    for fetch in &fetches {
        match fetch {
            Fetch::File { path, entry, .. } => {
                if fresh_build || !on_disk(&region_dir, path, entry).await {
                    download_file(client, base_url, &cfg.build.date, path, entry, &region_dir.join(basename(path))).await?;
                }
            }
            Fetch::Unit { path, files, .. } => {
                let mut all_on_disk = !fresh_build;
                for (f, entry) in files {
                    all_on_disk = all_on_disk && on_disk(&region_dir, f, entry).await;
                }
                if !all_on_disk {
                    download_unit(client, base_url, &cfg.build.date, path, files, &region_dir).await?;
                }
            }
        }

        files_done += fetch.files();
        bytes_done = bytes_done.saturating_add(fetch.size());
        progress(DownloadProgress {
            files_done,
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
        assert_eq!(got, lower_hex(&hasher.finalize()));
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
            units: BTreeMap::new(),
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
                units: BTreeMap::new(),
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
            units: BTreeMap::new(),
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
            units: BTreeMap::new(),
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

    // Index::load refuses a dir with some of the three address files and not all of them.
    #[test]
    fn template_selections_take_the_three_address_files_together() {
        use query_server::addr_files::{INDEX_FILE, POINTS_FILE, VERTICES_FILE};
        let (v, c) = template();
        for s in template_selections(&v) {
            let paths = resolve_files(&c, &s).unwrap().paths;
            let held = [POINTS_FILE, VERTICES_FILE, INDEX_FILE].map(|name| paths.iter().any(|p| basename(p) == name));
            assert_eq!(held, [s.mode == "full"; 3], "{:?}", s);
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

    /// The names in a region dir and their bytes.
    fn held(dir: &Path) -> BTreeMap<String, Vec<u8>> {
        std::fs::read_dir(dir).unwrap()
            .map(|e| e.unwrap())
            .map(|e| (e.file_name().into_string().unwrap(), std::fs::read(e.path()).unwrap()))
            .collect()
    }

    /// What a download of `s` should leave in its region dir: each file the
    /// bucket has, by its name, and the build's stamp.
    fn files_of(bucket: &TestBucket, s: &RegionSelection) -> BTreeMap<String, Vec<u8>> {
        let mut want: BTreeMap<String, Vec<u8>> = resolve_files(&bucket.manifest, s).unwrap().paths.iter()
            .filter(|p| bucket.manifest.files.contains_key(*p))
            .map(|p| (basename(p).to_string(), bucket.file(p).to_vec()))
            .collect();
        want.insert(".build_date".into(), test_bucket::DATE.as_bytes().to_vec());
        want
    }

    // The checks refuse no selection the template allows, nor a preset's, and
    // each gets the bucket's files byte for byte, its transport units' among
    // them.
    #[tokio::test]
    async fn every_template_selection_downloads_its_files_byte_for_byte() {
        let (v, _) = template();
        let bucket = test_bucket::serve(v["components"].clone()).await;
        let mut units = 0;
        for s in template_selections(&v) {
            let root = ScratchDir::new();

            download(&bucket, &s, root.path()).await.unwrap_or_else(|e| panic!("{s:?}: {e}"));

            let got = held(&root.path().join(&s.region));
            for name in ["admin_cells.bin", "strings_layout.json", "admin_polygons.bin"].into_iter().chain(query_server::ADMIN_GEOMETRY_FILES) {
                assert!(got.contains_key(name), "{s:?} {name}");
            }
            assert!(got == files_of(&bucket, &s), "{s:?}: {:?}", got.keys().collect::<Vec<_>>());
            units += resolve_files(&bucket.manifest, &s).unwrap().paths.iter().filter(|p| bucket.manifest.files[*p].unit.is_some()).count();
        }
        assert!(units > 0);
    }

    // A unit's files come down together, each to its sha256: the unit once
    // for all of them, counted once in the bytes.
    #[tokio::test]
    async fn a_unit_is_fetched_once_for_all_its_files() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let s = sel("europe", "admin", "simplified", "major");
        let root = ScratchDir::new();
        let seen = std::sync::Mutex::new(Vec::new());

        download_region(&reqwest::Client::new(), &bucket.url, &bucket.manifest, &s, root.path(), |p| seen.lock().unwrap().push(p)).await.unwrap();

        let paths = resolve_files(&bucket.manifest, &s).unwrap().paths;
        let units: BTreeSet<&str> = paths.iter().filter_map(|p| bucket.manifest.files[p].unit.as_deref()).collect();
        assert_eq!(units, BTreeSet::from(["europe/admin/places.tt.zst", "europe/poi/major/pois.tt.zst",
                                          "europe/quality/simplified/admin_polygons.tt.zst", "europe/quality/simplified/arcs.tt.zst"]));
        let plain = paths.iter().filter(|p| bucket.manifest.files[*p].unit.is_none()).count();
        assert_eq!(bucket.files_served(), plain + units.len());
        let bytes: u64 = paths.iter().filter_map(|p| bucket.manifest.files[p].size_zst).sum::<u64>()
            + units.iter().map(|u| bucket.manifest.units[*u].size).sum::<u64>();
        let last = seen.lock().unwrap().last().cloned().unwrap();
        assert_eq!((last.files_done, last.files_total, last.bytes_done, last.bytes_total), (paths.len(), paths.len(), bytes, bytes));
        assert!(held(&root.path().join("europe")) == files_of(&bucket, &s));
    }

    // Resume: a unit whose files are all there at their size is not fetched
    // again; one that lost a file is, for all of them.
    #[tokio::test]
    async fn a_unit_is_fetched_again_only_when_one_of_its_files_is_missing() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let s = sel("europe", "admin-minimal", "simplified", "none");
        let root = ScratchDir::new();
        download(&bucket, &s, root.path()).await.unwrap();
        let served = bucket.files_served();

        download(&bucket, &s, root.path()).await.unwrap();
        assert_eq!(bucket.files_served(), served);

        std::fs::remove_file(root.path().join("europe/place_nodes.bin")).unwrap();
        download(&bucket, &s, root.path()).await.unwrap();
        assert_eq!(bucket.files_served(), served + 1);
        assert!(held(&root.path().join("europe")) == files_of(&bucket, &s));
    }

    // A unit with one bad byte, in a file's bytes (decoded, of another
    // sha256) or in its stream (refused), fails its download, leaving none of
    // its files and no .tmp.
    #[tokio::test]
    async fn a_unit_with_one_bad_byte_fails_its_download_and_leaves_none_of_its_files() {
        let s = sel("europe", "admin", "simplified", "none");
        for (unit, at_end, why) in [
            ("europe/quality/simplified/admin_polygons.tt.zst", true, "admin_polygons.bin's sha256 is "),
            ("europe/admin/places.tt.zst", false, "a unit of transport version "),
        ] {
            let bucket = test_bucket::serve_tampered(test_bucket::template_components(), |path, bytes| {
                if path == unit {
                    let at = if at_end { bytes.len() - 1 } else { 5 };
                    bytes[at] ^= 0x10;
                }
            }).await;
            let root = ScratchDir::new();

            let got = download(&bucket, &s, root.path()).await.map_err(|e| e.to_string());

            let error = got.unwrap_err();
            assert!(error.starts_with(&format!("transport unit {unit}: transport: {why}")), "{error}");
            let left = held(&root.path().join("europe"));
            for f in &bucket.manifest.units[unit].files {
                assert!(!left.contains_key(basename(f)), "{unit}: {f} left");
            }
            assert!(left.keys().all(|n| !n.ends_with(".tmp")), "{unit}: {:?}", left.keys().collect::<Vec<_>>());
        }
    }

    // Before a byte is fetched: a unit of another transport version, or one
    // holding a file the selection doesn't take, which its decode would
    // write beside the selection's.
    #[tokio::test]
    async fn a_unit_this_build_cant_take_is_refused_before_fetching_a_file() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let s = sel("europe", "admin", "simplified", "none");
        let unit = "europe/admin/places.tt.zst";
        for (change, error) in [
            (Box::new(|u: &mut UnitEntry| u.transport_version = 1) as Box<dyn Fn(&mut UnitEntry)>,
             format!("transport unit {unit}: a unit of transport version 1: this build reads version {TRANSPORT_VERSION} only")),
            (Box::new(|u: &mut UnitEntry| u.files.push("europe/full/street_ways.bin".into())),
             format!("transport unit {unit}: holds europe/full/street_ways.bin, which the selection doesn't take")),
        ] {
            let mut manifest = bucket.manifest.clone();
            change(manifest.units.get_mut(unit).unwrap());
            let root = ScratchDir::new();

            let got = download_region(&reqwest::Client::new(), &bucket.url, &manifest, &s, root.path(), |_| {}).await.map_err(|e| e.to_string());

            assert_eq!(got, Err(error));
        }
        assert_eq!(bucket.files_served(), 0);
    }

    // compress_index.py publishes a unit for each dir holding all its files,
    // and a download decodes all of them: every selection takes all of a
    // unit's files in a dir or none.
    #[test]
    fn template_selections_take_all_of_a_units_files_or_none() {
        let (v, c) = template();
        for s in template_selections(&v) {
            let paths: BTreeSet<String> = resolve_files(&c, &s).unwrap().paths.into_iter().collect();
            let dirs: BTreeSet<&str> = paths.iter().map(|p| p.rsplit_once('/').unwrap().0).collect();
            for dir in dirs {
                for u in v["transport"]["units"].as_array().unwrap() {
                    let files: Vec<String> = u["files"].as_array().unwrap().iter().map(|f| format!("{dir}/{}", f.as_str().unwrap())).collect();
                    let taken = files.iter().filter(|f| paths.contains(*f)).count();
                    assert!(taken == 0 || taken == files.len(), "{s:?}: {taken} of {files:?}");
                }
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
        assert_eq!(
            DownloadError::Transport("europe/admin/places.tt.zst: transport: the unit ends early, past byte 12".into()).to_string(),
            "transport unit europe/admin/places.tt.zst: transport: the unit ends early, past byte 12"
        );
    }
}
