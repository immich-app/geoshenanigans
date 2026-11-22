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

use query_server::{lower_hex, take_download_lock, LockFor, BUILD_STAMP, DOWNLOAD_LOCK, FILE_LEDGER};
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

impl<'a> Fetch<'a> {
    fn path(&self) -> &'a str {
        match self {
            Fetch::File { path, .. } | Fetch::Unit { path, .. } => path,
        }
    }

    /// The files it puts in place, by path, with their entries.
    fn files(&self) -> Vec<(&'a str, &'a FileEntry)> {
        match self {
            Fetch::File { path, entry, .. } => vec![(*path, *entry)],
            Fetch::Unit { files, .. } => files.clone(),
        }
    }

    fn size(&self) -> u64 {
        match self {
            Fetch::File { size, .. } | Fetch::Unit { size, .. } => *size,
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

/// Whether there is anything at `path`.
async fn there(path: &Path) -> Result<bool, DownloadError> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(DownloadError::Io(format!("stat {}: {}", path.display(), e))),
    }
}

/// Whether the file at `path` is in `dir` at its size.
async fn on_disk(dir: &Path, path: &str, entry: &FileEntry) -> bool {
    matches!(tokio::fs::metadata(dir.join(basename(path))).await, Ok(m) if m.len() == entry.size_raw)
}

/// What each file in a region dir holds, as the dir's ledger (FILE_LEDGER)
/// tells it: by name, the sha256 of its bytes, or none where they are
/// unknown. A name it has no line for is unknown too.
struct Ledger {
    path: std::path::PathBuf,
    held: BTreeMap<String, Option<String>>,
    /// The name on a last line a kill cut short (no newline after it).
    torn: Option<String>,
}

impl Ledger {
    /// The ledger in `dir`, empty when there is none.
    async fn read(dir: &Path) -> Result<Ledger, DownloadError> {
        let path = dir.join(FILE_LEDGER);
        let text = match tokio::fs::read(&path).await {
            Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => return Err(DownloadError::Io(format!("read {}: {}", path.display(), e))),
        };
        let (lines, cut) = text.rsplit_once('\n').unwrap_or(("", text.as_str()));
        let mut held = BTreeMap::new();
        for line in lines.split('\n').filter(|l| !l.is_empty()) {
            let (name, sha256) = line.split_once(' ').unwrap_or((line, "-"));
            held.insert(name.to_string(), (sha256 != "-").then(|| sha256.to_string()));
        }
        // A line cut short says nothing of its file's bytes, whatever it holds.
        let torn = (!cut.is_empty()).then(|| cut.split_once(' ').map_or(cut, |(name, _)| name).to_string());
        if let Some(name) = &torn {
            held.insert(name.clone(), None);
        }
        Ok(Ledger { path, held, torn })
    }

    /// Whether it says the file at `path` holds the bytes of `entry`.
    fn holds(&self, path: &str, entry: &FileEntry) -> bool {
        matches!(self.held.get(basename(path)), Some(Some(sha256)) if *sha256 == entry.sha256)
    }

    /// Adds a line for each of `files`: its sha256 once it is `in_place`,
    /// else unknown. One write: a kill cuts short at most its last line. A
    /// line cut short before goes on a line of its own, and an unknown line
    /// for its name after it.
    async fn note(&mut self, files: &[(&str, &FileEntry)], in_place: bool) -> Result<(), DownloadError> {
        use tokio::io::AsyncWriteExt;
        let mut lines = self.torn.as_ref().map_or(String::new(), |name| format!("\n{} -\n", name));
        for (path, entry) in files {
            lines.push_str(&format!("{} {}\n", basename(path), if in_place { entry.sha256.as_str() } else { "-" }));
        }
        let err = |e: std::io::Error| DownloadError::Io(format!("write {}: {}", self.path.display(), e));
        let mut f = tokio::fs::OpenOptions::new().create(true).append(true).open(&self.path).await.map_err(err)?;
        f.write_all(lines.as_bytes()).await.map_err(err)?;
        f.flush().await.map_err(err)?;
        self.torn = None;
        for (path, entry) in files {
            self.held.insert(basename(path).to_string(), in_place.then(|| entry.sha256.clone()));
        }
        Ok(())
    }

    /// Makes its file where it isn't there, empty; one that is there stays
    /// as it is. A dir holding it but no stamp is one a server leaves
    /// unloaded (a download that did not finish), and one holding neither
    /// one it loads (laid out by hand).
    async fn make(&self) -> Result<(), DownloadError> {
        tokio::fs::OpenOptions::new().create(true).append(true).open(&self.path).await
            .map(drop)
            .map_err(|e| DownloadError::Io(format!("write {}: {}", self.path.display(), e)))
    }

    /// Rewrites it with only the last line of each name.
    async fn compact(&self) -> Result<(), DownloadError> {
        let lines: String = self.held.iter().map(|(name, sha256)| format!("{} {}\n", name, sha256.as_deref().unwrap_or("-"))).collect();
        write_whole(&self.path, lines.as_bytes()).await
    }
}

/// Takes the download lock of the region dir `dir` for a download
/// (DOWNLOAD_LOCK, exclusive), waiting while another download holds it, or
/// a server loads the dir: downloads into the dir go one at a time, those of
/// other processes too, as they share the dir's temp names, ledger and
/// stamp. None on a file system without locks.
async fn download_lock(dir: &Path) -> Result<Option<std::fs::File>, DownloadError> {
    let at = dir.join(DOWNLOAD_LOCK);
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || take_download_lock(&dir, LockFor::Download))
        .await
        .map_err(|e| DownloadError::Io(format!("lock task: {}", e)))?
        .map_err(|e| DownloadError::Io(format!("lock {}: {}", at.display(), e)))
}

/// Finishes what decodes into the region dir `dir` killed part way left
/// (transport::finish_leftovers_in): the dir of each, holding the files its
/// unit's files replaced, which go back unless all of the unit's were in.
/// Run before a download reads the dir: finished later, by the download's
/// first decode, such a dir would put back a file once the download had
/// listed the files to remove, or over one the download fetched. One that
/// can't be finished, and would put back a file, fails the download.
async fn finish_leftovers(dir: &Path) -> Result<(), DownloadError> {
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || transport::finish_leftovers_in(&dir))
        .await
        .map_err(|e| DownloadError::Io(format!("leftovers task: {}", e)))?
        .map_err(|e| DownloadError::Io(e.to_string()))
}

/// Writes `bytes` to `path` as one: under a temp name beside it, then
/// renamed over it.
async fn write_whole(path: &Path, bytes: &[u8]) -> Result<(), DownloadError> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    tokio::fs::write(&tmp, bytes).await
        .map_err(|e| DownloadError::Io(format!("write {}: {}", Path::new(&tmp).display(), e)))?;
    tokio::fs::rename(&tmp, path).await
        .map_err(|e| DownloadError::Io(format!("rename {}→{}: {}", Path::new(&tmp).display(), path.display(), e)))
}

#[cfg(test)]
thread_local! {
    /// Set by a test: the fetch, by its path, at which the next download on
    /// this thread stops once its files are in place, before the ledger
    /// names them, as one killed there would.
    static STOP_ONCE_FETCHED: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
    /// Set by a test: the next download on this thread stops once it removed
    /// the files its selection doesn't take, before its first fetch.
    static STOP_ONCE_REMOVED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Resolve and download every file for a single region. A fetch (a file's
/// .zst, or a transport unit for all its files) is skipped only when each
/// of its files is on disk at its size and the dir's ledger (FILE_LEDGER)
/// says it holds the bytes of the manifest's sha256 for its path; size
/// alone trusts no file, as one of the size another selection or build
/// left, or a fetch a kill cut short, may hold other bytes. Before a fetch
/// can replace a file the ledger marks it unknown, and once the fetch is
/// in place (its bytes checked against the manifest's sha256) names its
/// sha256. A file the selection doesn't take (an earlier selection's or
/// build's, which Index::load would read beside this one's) is removed
/// before the first fetch: each the ledger names, and each of a name a file
/// of the build has in any selection, the ledger naming it or not (a dir
/// laid out by hand, or whose ledger was lost); a file of another name
/// stays. Their lines go when the ledger is rewritten. The dir's stamp
/// (BUILD_STAMP) says it holds a complete selection of the build it names
/// and none of those files: a download that fetches or removes anything
/// makes the ledger where the dir has none, then removes the stamp, before
/// it writes anything else (a server leaves a dir holding the ledger but no
/// stamp unloaded, and loads one holding neither), and puts the stamp in
/// once the last fetch is in, the ledger rewritten with a line a file of
/// the selection; one that has nothing to fetch or remove in a dir stamped
/// with its build writes nothing. Downloads into the dir go one at a time,
/// across processes too, and a server loads the dir only while none runs
/// (DOWNLOAD_LOCK). Before it reads the dir a download finishes what a
/// decode killed part way left there (finish_leftovers), or fails, writing
/// nothing. A unit's files land together, so a download needs the largest
/// unit's files' size free beside the region. It is to run to its end
/// (ensure_loaded runs it in a task of its own): dropped part way, it lets
/// go of the lock while a decode or a file's writer it started goes on
/// writing the dir on a thread of its own.
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
    // Held until the download returns.
    let _lock = download_lock(&region_dir).await?;
    // The only change to the dir's files before the ledger is made and the
    // stamp removed: the leftovers are those of decodes a download ran once
    // it had done both, in a dir that holds the ledger and no stamp since,
    // which no server loads.
    finish_leftovers(&region_dir).await?;

    let stamp = region_dir.join(BUILD_STAMP);
    let mut ledger = Ledger::read(&region_dir).await?;
    let mut in_place = Vec::with_capacity(fetches.len());
    for fetch in &fetches {
        let mut held = true;
        for (path, entry) in fetch.files() {
            held = held && ledger.holds(path, entry) && on_disk(&region_dir, path, entry).await;
        }
        in_place.push(held);
    }
    let to_fetch = in_place.contains(&false);
    let taken: HashSet<&str> = paths.iter().map(|p| basename(p)).collect();
    // The files the selection doesn't take: each the ledger names, and each
    // in the dir of a name a file of the build has in any selection, the
    // ledger naming it or not. Other names stay.
    let mut others: BTreeSet<String> = ledger.held.keys().filter(|name| !taken.contains(name.as_str())).cloned().collect();
    let build_names: BTreeSet<&str> = cfg.files.keys().map(|p| basename(p)).filter(|name| !taken.contains(name)).collect();
    for name in build_names {
        if !others.contains(name) && there(&region_dir.join(name)).await? {
            others.insert(name.to_string());
        }
    }
    let changes = to_fetch || !others.is_empty();
    let stamped = tokio::fs::read_to_string(&stamp).await.is_ok_and(|s| s.trim() == cfg.build.date);
    if changes {
        // The ledger first, where the dir has none (the first download into
        // a dir laid out by hand): killed once the stamp or a file went, the
        // download leaves a dir a server doesn't load, not one it takes for
        // laid out by hand. Both before any file changes: a server that
        // loads the dir without its lock (one that can't make the lock file)
        // tells by the ledger made or the stamp gone that a download ran
        // while it loaded (DirMarks in lib.rs).
        ledger.make().await?;
        match tokio::fs::remove_file(&stamp).await {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err(DownloadError::Io(format!("remove {}: {}", stamp.display(), e)));
            }
            _ => {}
        }
    }
    // Before the fetches, so that their room is free. Their lines go only
    // when the ledger is rewritten: a download killed before then leaves
    // them, and the next finds their files gone.
    for name in &others {
        let path = region_dir.join(name);
        match tokio::fs::remove_file(&path).await {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err(DownloadError::Io(format!("remove {}: {}", path.display(), e)));
            }
            _ => {}
        }
        ledger.held.remove(name);
    }
    #[cfg(test)]
    if STOP_ONCE_REMOVED.replace(false) {
        return Err(DownloadError::Io("stopped once the files the selection doesn't take were removed".to_string()));
    }
    for (fetch, &held) in fetches.iter().zip(&in_place) {
        if !held {
            let files = fetch.files();
            ledger.note(&files, false).await?;
            match fetch {
                Fetch::File { path, entry, .. } => {
                    download_file(client, base_url, &cfg.build.date, path, entry, &region_dir.join(basename(path))).await?;
                }
                Fetch::Unit { path, files, .. } => {
                    download_unit(client, base_url, &cfg.build.date, path, files, &region_dir).await?;
                }
            }
            #[cfg(test)]
            if STOP_ONCE_FETCHED.get() == Some(fetch.path()) {
                STOP_ONCE_FETCHED.set(None);
                return Err(DownloadError::Io(format!("stopped once {} was in place", fetch.path())));
            }
            ledger.note(&files, true).await?;
        }

        files_done += fetch.files().len();
        bytes_done = bytes_done.saturating_add(fetch.size());
        progress(DownloadProgress {
            files_done,
            files_total,
            bytes_done,
            bytes_total,
        });
    }

    if changes || !stamped {
        ledger.compact().await?;
        // A new file renamed into place, never the one there written again:
        // a server loading the dir without its lock tells it from the stamp
        // it found by its inode, should the download run all through while
        // it loaded.
        write_whole(&stamp, cfg.build.date.as_bytes()).await?;
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
    // them downloads GiBs into a dir that can never load. Every selection
    // takes one mode dir, and with it that dir's attribution.json, the credit
    // of the data the dataset holds; one with a POI tier takes the tier dir's
    // poi_attribution.json too where the build wrote one (an optional file),
    // the credit of the sitelinks that rank its POIs, by a name of its own,
    // as downloads land flat by name.
    #[test]
    fn template_selections_include_load_sidecars() {
        let (v, c) = template();
        for s in template_selections(&v) {
            let resolved = resolve_files(&c, &s).unwrap();
            let paths = resolved.paths;
            let has = |name: &str| paths.iter().any(|p| basename(p) == name);
            assert!(has("strings_layout.json"), "{:?} lacks strings_layout.json", s);
            assert!(has("admin_cells.bin"), "{:?} lacks admin_cells.bin", s);
            assert_eq!(has("poi_meta.json"), s.poi_tier != "none", "{:?} poi_meta.json", s);
            let credits: Vec<&String> = paths.iter().filter(|p| basename(p) == "attribution.json").collect();
            assert_eq!(credits.len(), 1, "{:?} attribution.json", s);
            assert!(!resolved.optional.contains(credits[0].as_str()), "{:?}", s);
            let poi_credits: Vec<&String> = paths.iter().filter(|p| basename(p) == "poi_attribution.json").collect();
            let poi_credit = format!("{}/poi/{}/poi_attribution.json", s.region, s.poi_tier);
            assert_eq!(poi_credits, if s.poi_tier == "none" { vec![] } else { vec![&poi_credit] }, "{:?}", s);
            assert!(poi_credits.iter().all(|p| resolved.optional.contains(p.as_str())), "{:?}", s);
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
                     "arc_index.bin", "arc_vertices.bin", "attribution.json", "place_cells.bin", "place_hidden.bin", "place_nodes.bin",
                     "strings_core.bin", "strings_layout.json"];
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
    /// bucket has, by its name, the ledger naming each one's sha256, the
    /// build's stamp and, on unix, the empty download lock file.
    fn files_of(bucket: &TestBucket, s: &RegionSelection) -> BTreeMap<String, Vec<u8>> {
        let mut want: BTreeMap<String, Vec<u8>> = resolve_files(&bucket.manifest, s).unwrap().paths.iter()
            .filter(|p| bucket.manifest.files.contains_key(*p))
            .map(|p| (basename(p).to_string(), bucket.file(p).to_vec()))
            .collect();
        let ledger: String = want.iter().map(|(name, bytes)| format!("{name} {}\n", sha256_of(bytes))).collect();
        want.insert(FILE_LEDGER.into(), ledger.into_bytes());
        want.insert(BUILD_STAMP.into(), test_bucket::DATE.as_bytes().to_vec());
        if cfg!(unix) {
            want.insert(DOWNLOAD_LOCK.into(), Vec::new());
        }
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

    /// The names whose bytes differ between two region dirs' files, or that
    /// one lacks.
    fn differing(got: &BTreeMap<String, Vec<u8>>, want: &BTreeMap<String, Vec<u8>>) -> Vec<String> {
        got.keys().chain(want.keys()).collect::<BTreeSet<_>>().into_iter().filter(|n| got.get(*n) != want.get(*n)).cloned().collect()
    }

    fn sha256_of(bytes: &[u8]) -> String {
        lower_hex(&Sha256::digest(bytes))
    }

    /// The region dir of `s` under `root` as a download of an older build
    /// left it: each file of the size the bucket's has, of other bytes, the
    /// ledger naming those, and that build's stamp.
    fn older_build(bucket: &TestBucket, s: &RegionSelection, root: &Path) {
        let dir = root.join(&s.region);
        std::fs::create_dir_all(&dir).unwrap();
        let mut ledger = String::new();
        for (name, bytes) in files_of(bucket, s) {
            match name.as_str() {
                BUILD_STAMP => std::fs::write(dir.join(name), "2025-12-01").unwrap(),
                FILE_LEDGER | DOWNLOAD_LOCK => {}
                _ => {
                    let older: Vec<u8> = bytes.iter().map(|b| b ^ 0x5a).collect();
                    ledger.push_str(&format!("{name} {}\n", sha256_of(&older)));
                    std::fs::write(dir.join(name), older).unwrap();
                }
            }
        }
        std::fs::write(dir.join(FILE_LEDGER), ledger).unwrap();
    }

    /// The last line the ledger in `dir` holds for each of `names`.
    fn ledger_lines(dir: &Path, names: &[&str]) -> Vec<String> {
        let text = std::fs::read_to_string(dir.join(FILE_LEDGER)).unwrap();
        names.iter().map(|n| text.lines().filter(|l| l.split(' ').next() == Some(*n)).last().unwrap_or("").to_string()).collect()
    }

    /// The bucket's build, the object at `object` (a unit, or a file's
    /// .zst) of one bad byte: its download fails, those of the objects
    /// before it in path order come down.
    async fn failing_at(object: &'static str) -> TestBucket {
        test_bucket::serve_tampered(test_bucket::template_components(), move |path, bytes| {
            if path == object {
                let at = bytes.len() - 1;
                bytes[at] ^= 0x10;
            }
        }).await
    }

    /// A unit of the selection the tests below download, in the middle of
    /// its fetches in path order.
    const FAILING_UNIT: &str = "europe/poi/major/pois.tt.zst";

    // A download of a fresh build over the dir of an older one that fails
    // part-way leaves the dir stamped with neither; retried, it leaves none
    // of the older build's files, not even those of the size the fresh
    // build's have.
    #[tokio::test]
    async fn a_fresh_builds_download_that_failed_part_way_leaves_no_older_file_once_retried() {
        let s = sel("europe", "admin", "simplified", "major");
        let (failing, bucket) = (failing_at(FAILING_UNIT).await, test_bucket::serve(test_bucket::template_components()).await);
        let root = ScratchDir::new();
        older_build(&bucket, &s, root.path());

        let failed = download(&failing, &s, root.path()).await.map_err(|e| e.to_string());
        let stamped = std::fs::read_to_string(root.path().join("europe").join(BUILD_STAMP)).ok();
        download(&bucket, &s, root.path()).await.unwrap();

        assert!(failed.as_ref().is_err_and(|e| e.starts_with(&format!("transport unit {FAILING_UNIT}: "))), "{failed:?}");
        assert_eq!(differing(&held(&root.path().join("europe")), &files_of(&bucket, &s)), Vec::<String>::new());
        assert_eq!(stamped, None);
    }

    // The stamp goes before the first fetch can replace a file, and the
    // ledger marks a fetch's files unknown before it starts: a download that
    // fails on its first fetch leaves no stamp, and that file unknown.
    #[tokio::test]
    async fn a_download_that_failed_on_its_first_fetch_leaves_no_stamp_and_its_file_unknown() {
        let s = sel("europe", "admin", "simplified", "major");
        let failing = failing_at("europe/admin/admin_cells.bin.zst").await;
        let root = ScratchDir::new();
        older_build(&failing, &s, root.path());
        let dir = root.path().join("europe");

        let failed = download(&failing, &s, root.path()).await.map_err(|e| e.to_string());

        assert!(failed.as_ref().is_err_and(|e| e.starts_with("sha256 mismatch on europe/admin/admin_cells.bin: ")), "{failed:?}");
        assert_eq!(failing.files_served(), 1);
        assert_eq!(ledger_lines(&dir, &["admin_cells.bin"]), ["admin_cells.bin -"]);
        assert!(!dir.join(BUILD_STAMP).exists());
    }

    // The ledger is made before the stamp or any file goes: a download that
    // can't make its ledger (a link in its place to a dir that isn't there)
    // fails having fetched nothing, the dir stamped with its build still and
    // holding the files its selection doesn't take. A switch from poi tier
    // major to none, over an older build's files.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_download_that_cant_make_its_ledger_fails_before_it_removes_the_stamp_or_a_file() {
        let (before, after) = (sel("europe", "admin", "simplified", "major"), sel("europe", "admin", "simplified", "none"));
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (theirs, _) = not_taken(&bucket, &before, &after);
        let root = ScratchDir::new();
        older_build(&bucket, &before, root.path());
        let dir = root.path().join("europe");
        let ledger = dir.join(FILE_LEDGER);
        std::fs::remove_file(&ledger).unwrap();
        std::os::unix::fs::symlink("missing/ledger", &ledger).unwrap();

        let failed = download(&bucket, &after, root.path()).await.map_err(|e| e.to_string());

        assert!(failed.as_ref().is_err_and(|e| e.starts_with(&format!("I/O error: write {}: ", ledger.display()))), "{failed:?}");
        assert_eq!(bucket.files_served(), 0);
        let left: Vec<&String> = theirs.iter().filter(|n| dir.join(n).exists()).collect();
        let stamp = std::fs::read_to_string(dir.join(BUILD_STAMP)).ok();
        assert_eq!((stamp.as_deref(), left), (Some("2025-12-01"), theirs.iter().collect()));
    }

    // A download into a dir that holds all of the selection, stamped with
    // the build, fetches nothing and writes nothing: not the stamp, nor the
    // ledger, though it holds more than a line a file. A file written under
    // a temp name and renamed into place, of the same bytes, is another
    // inode.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_download_with_nothing_to_fetch_writes_nothing() {
        use std::io::Write;
        use std::os::unix::fs::MetadataExt;
        let s = sel("europe", "admin", "simplified", "major");
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let root = ScratchDir::new();
        download(&bucket, &s, root.path()).await.unwrap();
        let dir = root.path().join("europe");
        let sha256 = &bucket.manifest.files["europe/admin/admin_cells.bin"].sha256;
        let mut ledger = std::fs::OpenOptions::new().append(true).open(dir.join(FILE_LEDGER)).unwrap();
        write!(ledger, "admin_cells.bin -\nadmin_cells.bin {sha256}\n").unwrap();
        let entries = || -> Vec<(String, u64, std::time::SystemTime, Vec<u8>)> {
            let mut v: Vec<_> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap())
                .map(|e| {
                    let meta = e.metadata().unwrap();
                    (e.file_name().into_string().unwrap(), meta.ino(), meta.modified().unwrap(), std::fs::read(e.path()).unwrap())
                })
                .collect();
            v.sort();
            v
        };
        let (before, served) = (entries(), bucket.files_served());

        download(&bucket, &s, root.path()).await.unwrap();

        assert_eq!(bucket.files_served(), served);
        assert!(entries() == before);
    }

    // A download with nothing to fetch into a dir whose stamp doesn't name
    // the build (one killed once its last fetch was in, before the stamp, or
    // stamped with an older build) fetches nothing, rewrites the ledger with
    // a line a file and stamps the dir.
    #[tokio::test]
    async fn a_download_with_nothing_to_fetch_into_a_dir_not_stamped_with_its_build_stamps_it() {
        use std::io::Write;
        let s = sel("europe", "admin", "simplified", "major");
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        for stamp in [None, Some("2025-12-01")] {
            let root = ScratchDir::new();
            download(&bucket, &s, root.path()).await.unwrap();
            let dir = root.path().join("europe");
            match stamp {
                None => std::fs::remove_file(dir.join(BUILD_STAMP)).unwrap(),
                Some(date) => std::fs::write(dir.join(BUILD_STAMP), date).unwrap(),
            }
            let sha256 = &bucket.manifest.files["europe/admin/admin_cells.bin"].sha256;
            let mut ledger = std::fs::OpenOptions::new().append(true).open(dir.join(FILE_LEDGER)).unwrap();
            write!(ledger, "admin_cells.bin -\nadmin_cells.bin {sha256}\n").unwrap();
            let served = bucket.files_served();

            download(&bucket, &s, root.path()).await.unwrap();

            assert_eq!(bucket.files_served(), served, "{stamp:?}");
            assert!(held(&dir) == files_of(&bucket, &s), "{stamp:?}");
        }
    }

    /// How many fetches of `s` a retry makes once a download that stopped at
    /// the fetch at `at` put those before it in place: that one and those
    /// after it, but those whose files all came down, under their names,
    /// before it (a region's JSON files, alike in each of its dirs).
    fn fetched_on_retry(bucket: &TestBucket, s: &RegionSelection, at: &str) -> usize {
        let resolved = resolve_files(&bucket.manifest, s).unwrap();
        let paths: Vec<&String> = resolved.paths.iter().filter(|p| bucket.manifest.files.contains_key(*p)).collect();
        let all = fetches(&bucket.manifest, &paths).unwrap();
        let i = all.iter().position(|f| f.path() == at).unwrap();
        let before: BTreeSet<&str> = all[..i].iter().flat_map(|f| f.files()).map(|(p, _)| basename(p)).collect();
        all[i..].iter().filter(|f| !f.files().iter().all(|(p, _)| before.contains(basename(p)))).count()
    }

    // The retry fetches again only what had not come down: the object that
    // failed and those after it.
    #[tokio::test]
    async fn a_retry_of_a_fresh_builds_download_fetches_only_what_had_not_come_down() {
        let s = sel("europe", "admin", "simplified", "major");
        let (failing, bucket) = (failing_at(FAILING_UNIT).await, test_bucket::serve(test_bucket::template_components()).await);
        let (root, whole) = (ScratchDir::new(), ScratchDir::new());
        download(&bucket, &s, whole.path()).await.unwrap();
        let all = bucket.files_served();
        older_build(&bucket, &s, root.path());
        assert!(download(&failing, &s, root.path()).await.is_err());
        let came_down = failing.files_served() - 1;

        download(&bucket, &s, root.path()).await.unwrap();

        let retried = fetched_on_retry(&bucket, &s, FAILING_UNIT);
        assert!(came_down > 0 && retried > 1 && came_down + retried <= all, "{came_down} and {retried} of {all}");
        assert_eq!(bucket.files_served() - all, retried);
        assert!(held(&root.path().join("europe")) == files_of(&bucket, &s));
    }

    // A download stopped as a kill would once a fetch is in place, before
    // the ledger names its files, leaves them unknown and no stamp; the
    // retry fetches that fetch again and those after it, not those before.
    #[tokio::test]
    async fn a_download_stopped_once_a_fetch_was_in_fetches_it_again_on_retry() {
        let s = sel("europe", "admin", "simplified", "major");
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (root, whole) = (ScratchDir::new(), ScratchDir::new());
        download(&bucket, &s, whole.path()).await.unwrap();
        let all = bucket.files_served();
        older_build(&bucket, &s, root.path());
        let dir = root.path().join("europe");
        STOP_ONCE_FETCHED.set(Some(FAILING_UNIT));

        let stopped = download(&bucket, &s, root.path()).await.map_err(|e| e.to_string());
        let came_down = bucket.files_served() - all;
        let (lines, stamped) = (ledger_lines(&dir, &["poi_records.bin", "poi_vertices.bin"]), dir.join(BUILD_STAMP).exists());
        download(&bucket, &s, root.path()).await.unwrap();

        assert_eq!(stopped, Err(format!("I/O error: stopped once {FAILING_UNIT} was in place")));
        assert_eq!(lines, ["poi_records.bin -", "poi_vertices.bin -"]);
        assert!(!stamped);
        let retried = fetched_on_retry(&bucket, &s, FAILING_UNIT);
        assert!(came_down > 1 && retried > 1 && came_down + retried <= all + 1, "{came_down} and {retried} of {all}");
        assert_eq!(bucket.files_served() - all - came_down, retried);
        assert!(held(&dir) == files_of(&bucket, &s));
    }

    // Only the ledger tells a file holds the build's bytes: a dir stamped
    // with the build whose files are of their sizes, but whose ledger names
    // other bytes (another selection's, say), gets every fetch again.
    #[tokio::test]
    async fn a_file_of_its_size_the_ledger_names_other_bytes_of_is_fetched_again() {
        let s = sel("europe", "admin", "simplified", "major");
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (root, whole) = (ScratchDir::new(), ScratchDir::new());
        download(&bucket, &s, whole.path()).await.unwrap();
        let all = bucket.files_served();
        older_build(&bucket, &s, root.path());
        std::fs::write(root.path().join("europe").join(BUILD_STAMP), test_bucket::DATE).unwrap();

        download(&bucket, &s, root.path()).await.unwrap();

        assert_eq!(bucket.files_served() - all, all);
        assert_eq!(differing(&held(&root.path().join("europe")), &files_of(&bucket, &s)), Vec::<String>::new());
    }

    /// Components of a build whose poi tiers major and all each hold one
    /// file, arc_index.bin: of one size in both (the fixture's arc index, an
    /// entry for each of its 30 arc slots), of other bytes.
    fn two_poi_tiers() -> serde_json::Value {
        serde_json::json!({
            "mode.admin": { "files": ["{region}/admin/admin_cells.bin"] },
            "quality.simplified": {},
            "poi_tier.major": { "files": ["{region}/poi/major/arc_index.bin"] },
            "poi_tier.all": { "files": ["{region}/poi/all/arc_index.bin"] }
        })
    }

    // A file of the selection that a switch to another replaced with one of
    // its name and size is fetched again on the switch back: poi tier major,
    // then all, whose arc_index.bin goes in (and the download stops there, as
    // a kill would, before the ledger names it, or not), then major again.
    #[tokio::test]
    async fn a_switch_back_fetches_again_a_file_the_other_selection_replaced_with_one_of_its_size() {
        let bucket = test_bucket::serve(two_poi_tiers()).await;
        let (major, all) = (sel("europe", "admin", "simplified", "major"), sel("europe", "admin", "simplified", "all"));
        let (ours, theirs) = (bucket.file("europe/poi/major/arc_index.bin"), bucket.file("europe/poi/all/arc_index.bin"));
        assert!(ours.len() == theirs.len() && ours != theirs);
        for stopped in [false, true] {
            let root = ScratchDir::new();
            download(&bucket, &major, root.path()).await.unwrap();
            let before = bucket.files_served();
            if stopped {
                STOP_ONCE_FETCHED.set(Some("europe/poi/all/arc_index.bin"));
            }

            let switched = download(&bucket, &all, root.path()).await.map_err(|e| e.to_string());
            let (fetched, replaced) = (bucket.files_served() - before, std::fs::read(root.path().join("europe/arc_index.bin")).unwrap());
            let stamp = std::fs::read_to_string(root.path().join("europe").join(BUILD_STAMP)).ok();
            download(&bucket, &major, root.path()).await.unwrap();

            assert_eq!(switched.is_err(), stopped, "{switched:?}");
            assert_eq!((fetched, replaced == theirs), (1, true), "stopped: {stopped}");
            // The dir was stamped with the build before the switch: one that
            // stopped part way leaves it stamped with none.
            assert_eq!(stamp.as_deref(), (!stopped).then_some(test_bucket::DATE), "stopped: {stopped}");
            assert_eq!(bucket.files_served() - before, 2, "stopped: {stopped}");
            assert!(held(&root.path().join("europe")) == files_of(&bucket, &major), "stopped: {stopped}");
        }
    }

    // A switch to a selection that doesn't take some of the files of the one
    // before leaves the dir holding the new selection only, its ledger a line
    // a file and its stamp. Admin-minimal beside poi tier major, then
    // without POIs: its dir has strings and polygon ids of its own, which the
    // POI files' names and parents don't point into. Admin beside major, then
    // without POIs: once the first download is complete the dir holds all of
    // the new selection, and nothing is fetched. The first download stops
    // once the pois unit is in (before the boundary files, which come after
    // it), or completes.
    #[tokio::test]
    async fn a_switch_leaves_none_of_the_files_of_the_selection_before_that_it_doesnt_take() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        for (mode, stopped, fetches) in [("admin-minimal", true, true), ("admin-minimal", false, true), ("admin", true, true), ("admin", false, false)] {
            let (before, after) = (sel("europe", mode, "simplified", "major"), sel("europe", mode, "simplified", "none"));
            let root = ScratchDir::new();
            if stopped {
                STOP_ONCE_FETCHED.set(Some("europe/poi/major/pois.tt.zst"));
            }
            let first = download(&bucket, &before, root.path()).await;
            let served = bucket.files_served();

            download(&bucket, &after, root.path()).await.unwrap();

            let case = format!("{mode}, stopped: {stopped}");
            assert_eq!(first.is_err(), stopped, "{case}: {first:?}");
            assert_eq!(bucket.files_served() > served, fetches, "{case}");
            assert_eq!(differing(&held(&root.path().join("europe")), &files_of(&bucket, &after)), Vec::<String>::new(), "{case}");
        }
    }

    // A download removes the stamp, then the files the ledger names that its
    // selection doesn't take, before it fetches anything: stopped there, as a
    // kill would, it leaves the dir with no stamp, none of those files and
    // nothing fetched, and the ledger as it was (made only where there is
    // none, it is never truncated); retried, it finishes. A switch from poi
    // tier major to none, in a mode whose dir the switch fetches, and in one
    // whose it doesn't.
    #[tokio::test]
    async fn a_download_removes_the_stamp_then_the_files_its_selection_doesnt_take_before_it_fetches() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        for mode in ["admin-minimal", "admin"] {
            let (before, after) = (sel("europe", mode, "simplified", "major"), sel("europe", mode, "simplified", "none"));
            let root = ScratchDir::new();
            let dir = root.path().join("europe");
            download(&bucket, &before, root.path()).await.unwrap();
            let theirs: Vec<String> = files_of(&bucket, &before).into_keys().filter(|n| !files_of(&bucket, &after).contains_key(n)).collect();
            let (served, ledger) = (bucket.files_served(), std::fs::read_to_string(dir.join(FILE_LEDGER)).unwrap());
            STOP_ONCE_REMOVED.set(true);

            let stopped = download(&bucket, &after, root.path()).await.map_err(|e| e.to_string());
            let left: Vec<&String> = theirs.iter().filter(|n| dir.join(n).exists()).collect();
            let (stamped, fetched) = (dir.join(BUILD_STAMP).exists(), bucket.files_served() - served);
            let kept = std::fs::read_to_string(dir.join(FILE_LEDGER)).unwrap() == ledger;
            download(&bucket, &after, root.path()).await.unwrap();

            assert!(theirs.len() > 1 && theirs.iter().all(|n| n.starts_with("poi_") || n.starts_with("strings_poi")), "{mode}: {theirs:?}");
            assert_eq!(stopped, Err("I/O error: stopped once the files the selection doesn't take were removed".to_string()), "{mode}");
            assert_eq!((left, stamped, fetched, kept), (Vec::<&String>::new(), false, 0, true), "{mode}");
            assert_eq!(differing(&held(&dir), &files_of(&bucket, &after)), Vec::<String>::new(), "{mode}");
        }
    }

    /// The names of the files of `before` that `after` doesn't take, and the
    /// files of names no file of the build has, hidden or not.
    fn not_taken(bucket: &TestBucket, before: &RegionSelection, after: &RegionSelection) -> (Vec<String>, BTreeMap<String, Vec<u8>>) {
        let theirs: Vec<String> = files_of(bucket, before).into_keys().filter(|n| !files_of(bucket, after).contains_key(n)).collect();
        assert!(theirs.len() > 1 && theirs.iter().all(|n| n.starts_with("poi_") || n.starts_with("strings_poi")), "{theirs:?}");
        let unknown = [("notes.txt", "not the build's"), (".notes", "nor the build's")];
        (theirs, unknown.into_iter().map(|(n, b)| (n.to_string(), b.as_bytes().to_vec())).collect())
    }

    // A file of a name a file of the build has, that the selection doesn't
    // take, goes though the ledger doesn't name it: Index::load would read it
    // beside the selection's. A dir with no ledger (lost, or laid out by
    // hand), stamped with the build or not, holding poi tier major's files,
    // switches to poi tier none: the POI files go, and files of other names
    // stay.
    #[tokio::test]
    async fn a_switch_removes_the_files_of_the_build_it_doesnt_take_that_no_ledger_names() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (before, after) = (sel("europe", "admin", "simplified", "major"), sel("europe", "admin", "simplified", "none"));
        let (_, unknown) = not_taken(&bucket, &before, &after);
        for stamped in [true, false] {
            let root = ScratchDir::new();
            let dir = root.path().join("europe");
            download(&bucket, &before, root.path()).await.unwrap();
            std::fs::remove_file(dir.join(FILE_LEDGER)).unwrap();
            if !stamped {
                std::fs::remove_file(dir.join(BUILD_STAMP)).unwrap();
            }
            for (name, bytes) in &unknown {
                std::fs::write(dir.join(name), bytes).unwrap();
            }

            download(&bucket, &after, root.path()).await.unwrap();

            let mut want = files_of(&bucket, &after);
            want.extend(unknown.clone());
            assert_eq!(differing(&held(&dir), &want), Vec::<String>::new(), "stamped: {stamped}");
        }
    }

    // Removing such a file is a write: the stamp goes first. A dir holding
    // the whole selection, stamped with the build, and POI files of the
    // build that its ledger doesn't name (put there by hand): the download
    // has nothing to fetch, and removes the stamp, then those files; stopped
    // there, as a kill would, it leaves no stamp and none of them; retried,
    // it stamps the dir again, fetching nothing. Files of other names stay.
    #[tokio::test]
    async fn a_download_removes_the_stamp_then_the_files_of_the_build_it_doesnt_take_that_no_ledger_names() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (before, after) = (sel("europe", "admin", "simplified", "major"), sel("europe", "admin", "simplified", "none"));
        let (theirs, unknown) = not_taken(&bucket, &before, &after);
        let (root, other) = (ScratchDir::new(), ScratchDir::new());
        download(&bucket, &before, other.path()).await.unwrap();
        download(&bucket, &after, root.path()).await.unwrap();
        let dir = root.path().join("europe");
        for name in &theirs {
            std::fs::copy(other.path().join("europe").join(name), dir.join(name)).unwrap();
        }
        for (name, bytes) in &unknown {
            std::fs::write(dir.join(name), bytes).unwrap();
        }
        let served = bucket.files_served();
        STOP_ONCE_REMOVED.set(true);

        let stopped = download(&bucket, &after, root.path()).await.map_err(|e| e.to_string());
        let left: Vec<&String> = theirs.iter().filter(|n| dir.join(n).exists()).collect();
        let stamped = dir.join(BUILD_STAMP).exists();
        download(&bucket, &after, root.path()).await.unwrap();

        assert_eq!(stopped, Err("I/O error: stopped once the files the selection doesn't take were removed".to_string()));
        assert_eq!((left, stamped), (Vec::<&String>::new(), false));
        assert_eq!(bucket.files_served(), served);
        let mut want = files_of(&bucket, &after);
        want.extend(unknown);
        assert_eq!(differing(&held(&dir), &want), Vec::<String>::new());
    }

    /// Why a server leaves the continent dir of `region` under the data dir
    /// `root` unloaded, if it does: the error of a hot-add of it, which checks
    /// and loads the dir as a server starting does (load_continent).
    fn left_unloaded(root: &Path, region: &str) -> Option<String> {
        let mi = query_server::MultiIndex::load(root.to_str().unwrap(), query_server::DEFAULT_STREET_CELL_LEVEL, query_server::DEFAULT_ADMIN_CELL_LEVEL, query_server::DEFAULT_SEARCH_DISTANCE).unwrap();
        mi.install_region(region).err()
    }

    // The first download into a dir without a ledger (laid out by hand, as
    // the test portal lays dirs out: no stamp nor ledger, and a server loads
    // it) makes the ledger before it removes the stamp or any file. Stopped
    // once it removed the files its selection doesn't take, as a kill would,
    // it leaves the ledger and no stamp, and a server leaves the continent
    // unloaded as one whose download did not finish, before it maps a file,
    // rather than load the dir half switched (here without its POI files, a
    // group Index::load takes as absent). Retried, it finishes.
    #[tokio::test]
    async fn a_first_download_into_a_dir_without_a_ledger_makes_it_before_it_removes_a_file() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (before, after) = (sel("europe", "admin", "simplified", "major"), sel("europe", "admin", "simplified", "none"));
        let (theirs, _) = not_taken(&bucket, &before, &after);
        let root = ScratchDir::new();
        let dir = root.path().join("europe");
        download(&bucket, &before, root.path()).await.unwrap();
        std::fs::remove_file(dir.join(FILE_LEDGER)).unwrap();
        std::fs::remove_file(dir.join(BUILD_STAMP)).unwrap();
        STOP_ONCE_REMOVED.set(true);

        let stopped = download(&bucket, &after, root.path()).await.map_err(|e| e.to_string());
        let left: Vec<&String> = theirs.iter().filter(|n| dir.join(n).exists()).collect();
        let (ledger, stamped) = (dir.join(FILE_LEDGER).exists(), dir.join(BUILD_STAMP).exists());
        let why = left_unloaded(root.path(), "europe");
        download(&bucket, &after, root.path()).await.unwrap();

        let unfinished = format!("it holds {FILE_LEDGER} but no {BUILD_STAMP}: a download that did not finish");
        assert_eq!(stopped, Err("I/O error: stopped once the files the selection doesn't take were removed".to_string()));
        assert_eq!((left, ledger, stamped, why), (Vec::<&String>::new(), true, false, Some(unfinished)), "a server takes the dir for one laid out by hand");
        assert_eq!(differing(&held(&dir), &files_of(&bucket, &after)), Vec::<String>::new());
    }

    /// The dir of a decode into a region dir (transport::decode_into_dir) of
    /// a process that is gone, killed while it put the unit's files in place.
    const KILLED_DECODES_DIR: &str = ".transport.9.0";

    // A download killed while the decode of a unit put the unit's files in
    // place leaves the decode's dir, holding the files they replaced, which
    // the next decode into the dir puts back. The next download finishes it
    // before it reads the dir: a file put back once the download listed the
    // files to remove would stay, under the stamp. Poi tier all, then a
    // download of major killed once its poi_records.bin went in over all's;
    // then a switch to no POIs in admin-minimal, which decodes units, and in
    // admin, which fetches nothing. Then the switch to admin once more, the
    // kill come before major's file went in and the ledger lost since: no
    // poi_records.bin is in the dir but the one the decode's dir holds, and
    // the download lists only the files in the dir under the build's names.
    // Each leaves its selection only.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_download_finishes_what_a_killed_decode_left_before_it_lists_the_files_to_remove() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (all, major) = (sel("europe", "admin", "simplified", "all"), sel("europe", "admin", "simplified", "major"));
        let ours = files_of(&bucket, &major)["poi_records.bin"].clone();
        assert_ne!(files_of(&bucket, &all)["poi_records.bin"], ours);
        let mut got = Vec::new();
        for (mode, lost) in [("admin-minimal", false), ("admin", false), ("admin", true)] {
            let after = sel("europe", mode, "simplified", "none");
            let root = ScratchDir::new();
            let dir = root.path().join("europe");
            download(&bucket, &all, root.path()).await.unwrap();
            // What the download of major left: the ledger naming the pois
            // unit's files unknown, no stamp, and the decode's dir holding
            // all's poi_records.bin, major's in its place; or none in its
            // place, the ledger lost.
            let ledger = std::fs::read_to_string(dir.join(FILE_LEDGER)).unwrap();
            std::fs::write(dir.join(FILE_LEDGER), format!("{ledger}poi_records.bin -\npoi_vertices.bin -\n")).unwrap();
            std::fs::remove_file(dir.join(BUILD_STAMP)).unwrap();
            std::fs::create_dir(dir.join(KILLED_DECODES_DIR)).unwrap();
            std::fs::rename(dir.join("poi_records.bin"), dir.join(KILLED_DECODES_DIR).join(".earlier.poi_records.bin")).unwrap();
            if lost {
                std::fs::remove_file(dir.join(FILE_LEDGER)).unwrap();
            } else {
                std::fs::write(dir.join("poi_records.bin"), &ours).unwrap();
            }
            let served = bucket.files_served();

            download(&bucket, &after, root.path()).await.unwrap();

            let left = dir.join(KILLED_DECODES_DIR).exists();
            let differ = if left { Vec::new() } else { differing(&held(&dir), &files_of(&bucket, &after)) };
            got.push((mode, lost, bucket.files_served() > served, left, differ));
        }
        assert_eq!(got, [("admin-minimal", false, true, false, Vec::new()), ("admin", false, false, false, Vec::new()), ("admin", true, true, false, Vec::new())]);
    }

    // A killed decode's dir that can't be finished, which would put a file
    // back once finished, fails the download before it writes anything: the
    // dir stays as it was, stamped. Here a directory is where the file goes.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_download_fails_before_it_writes_while_a_killed_decodes_dir_cant_be_finished() {
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (before, after) = (sel("europe", "admin", "simplified", "major"), sel("europe", "admin-minimal", "simplified", "none"));
        let root = ScratchDir::new();
        let dir = root.path().join("europe");
        download(&bucket, &before, root.path()).await.unwrap();
        let was = held(&dir);
        std::fs::create_dir_all(dir.join(KILLED_DECODES_DIR)).unwrap();
        std::fs::write(dir.join(KILLED_DECODES_DIR).join(".earlier.notes.txt"), "a file a member replaced").unwrap();
        std::fs::create_dir(dir.join("notes.txt")).unwrap();
        let served = bucket.files_served();

        let got = download(&bucket, &after, root.path()).await.map_err(|e| e.to_string());

        assert_eq!(got, Err(format!("I/O error: {} can't be finished (Is a directory (os error 21)): it would put back notes.txt", dir.join(KILLED_DECODES_DIR).display())));
        assert_eq!(bucket.files_served(), served);
        std::fs::remove_dir(dir.join("notes.txt")).unwrap();
        std::fs::remove_dir_all(dir.join(KILLED_DECODES_DIR)).unwrap();
        assert_eq!(differing(&held(&dir), &was), Vec::<String>::new());
    }

    // A last line a kill cut short says nothing of its file, though it names
    // the file's sha256: the file is fetched again, and the next lines go
    // after an unknown line for it, not onto it.
    #[tokio::test]
    async fn a_ledgers_last_line_cut_short_leaves_its_file_unknown() {
        use std::io::Write;
        let s = sel("europe", "admin", "simplified", "major");
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let root = ScratchDir::new();
        download(&bucket, &s, root.path()).await.unwrap();
        let dir = root.path().join("europe");
        // A download killed as it wrote poi_cells.bin's line, its stamp gone;
        // admin_cells.bin lost since.
        std::fs::remove_file(dir.join(BUILD_STAMP)).unwrap();
        let sha256 = &bucket.manifest.files["europe/poi/major/poi_cells.bin"].sha256;
        write!(std::fs::OpenOptions::new().append(true).open(dir.join(FILE_LEDGER)).unwrap(), "poi_cells.bin {sha256}").unwrap();
        std::fs::remove_file(dir.join("admin_cells.bin")).unwrap();
        let served = bucket.files_served();
        STOP_ONCE_FETCHED.set(Some("europe/admin/admin_cells.bin"));

        let stopped = download(&bucket, &s, root.path()).await;
        let lines = ledger_lines(&dir, &["admin_cells.bin", "poi_cells.bin"]);
        download(&bucket, &s, root.path()).await.unwrap();

        assert!(stopped.is_err());
        assert_eq!(lines, ["admin_cells.bin -", "poi_cells.bin -"]);
        assert_eq!(bucket.files_served() - served, 3);
        assert!(held(&dir) == files_of(&bucket, &s));
    }

    // A complete download leaves the ledger a line a file, each naming the
    // build's sha256, however many lines it held, and the stamp.
    #[tokio::test]
    async fn a_complete_download_leaves_the_ledger_a_line_a_file_and_the_stamp() {
        let s = sel("europe", "admin", "simplified", "major");
        let (failing, bucket) = (failing_at(FAILING_UNIT).await, test_bucket::serve(test_bucket::template_components()).await);
        let root = ScratchDir::new();
        older_build(&bucket, &s, root.path());
        assert!(download(&failing, &s, root.path()).await.is_err());
        let dir = root.path().join("europe");
        let lines = std::fs::read_to_string(dir.join(FILE_LEDGER)).unwrap().lines().count();

        download(&bucket, &s, root.path()).await.unwrap();

        let want = files_of(&bucket, &s);
        assert!(lines > want.len(), "{lines}");
        assert_eq!(std::fs::read_to_string(dir.join(FILE_LEDGER)).unwrap(), String::from_utf8(want[FILE_LEDGER].clone()).unwrap());
        assert_eq!(std::fs::read_to_string(dir.join(BUILD_STAMP)).unwrap(), test_bucket::DATE);
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

    // Downloads into a region dir go one at a time, those of other processes
    // too (a second server on the data dir, say): they share the dir's temp
    // names, ledger and stamp. One waits while another holds the dir's
    // download lock, fetching and writing nothing; reads the dir only once it
    // holds the lock, so that it finds what the other put there meanwhile
    // (here the whole selection, and it fetches nothing); and holds the lock
    // to its end: a try at it from another open fails each time the download
    // tells its progress.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_download_waits_while_another_holds_the_dirs_download_lock_and_then_holds_it() {
        let s = sel("europe", "admin", "simplified", "major");
        let bucket = test_bucket::serve(test_bucket::template_components()).await;
        let (root, whole) = (ScratchDir::new(), ScratchDir::new());
        download(&bucket, &s, whole.path()).await.unwrap();
        let all = bucket.files_served();
        let dir = root.path().join("europe");
        std::fs::create_dir_all(&dir).unwrap();
        // Another open of the file: its lock is another's, as another process's would be.
        let other = std::fs::File::create(dir.join(DOWNLOAD_LOCK)).unwrap();
        other.lock().unwrap();
        let (client, free) = (reqwest::Client::new(), std::sync::Mutex::new(Vec::new()));
        let waiting = download_region(&client, &bucket.url, &bucket.manifest, &s, root.path(), |_| {
            free.lock().unwrap().push(std::fs::File::open(dir.join(DOWNLOAD_LOCK)).unwrap().try_lock().is_ok());
        });
        tokio::pin!(waiting);

        let early = tokio::time::timeout(std::time::Duration::from_millis(500), &mut waiting).await.ok().map(|r| r.map_err(|e| e.to_string()));
        let (served, names) = (bucket.files_served(), held(&dir).into_keys().collect::<Vec<_>>());
        for (name, bytes) in held(&whole.path().join("europe")) {
            if name != DOWNLOAD_LOCK {
                std::fs::write(dir.join(name), bytes).unwrap();
            }
        }
        drop(other);
        let late = match early {
            Some(_) => None,
            None => Some(waiting.await.map_err(|e| e.to_string())),
        };

        assert_eq!(early, None, "done while the lock was held");
        assert_eq!((served, names), (all, vec![DOWNLOAD_LOCK.to_string()]));
        assert_eq!(late, Some(Ok(())));
        assert_eq!(bucket.files_served(), all);
        assert!(held(&dir) == files_of(&bucket, &s));
        let free = free.lock().unwrap();
        assert!(free.len() > 2 && !free.contains(&true), "{free:?}");
    }

    /// Runs `f` as a server of the user of uid `uid` and group `gid`, of the
    /// group `shared` too, would under `umask`: on a thread whose file
    /// system ids, groups and umask are those (its own, unshared), given a
    /// runtime whose threads, started by it, take them too. As root only.
    #[cfg(target_os = "linux")]
    fn as_user<T: Send>(uid: u32, gid: u32, shared: u32, umask: libc::mode_t, f: impl FnOnce(&tokio::runtime::Runtime) -> T + Send) -> T {
        let become_user = move || unsafe {
            let groups = [shared];
            assert_eq!(libc::syscall(libc::SYS_setgroups, 1, groups.as_ptr()), 0);
            libc::syscall(libc::SYS_setfsgid, gid);
            libc::syscall(libc::SYS_setfsuid, uid);
            // Each returns the id it had: the second call tells the first took.
            assert_eq!((libc::syscall(libc::SYS_setfsgid, gid), libc::syscall(libc::SYS_setfsuid, uid)), (gid as libc::c_long, uid as libc::c_long));
        };
        std::thread::scope(|s| {
            s.spawn(|| {
                assert_eq!(unsafe { libc::unshare(libc::CLONE_FS) }, 0);
                unsafe { libc::umask(umask) };
                become_user();
                let rt = tokio::runtime::Builder::new_current_thread().enable_all().on_thread_start(become_user).build().unwrap();
                f(&rt)
            })
            .join()
            .unwrap()
        })
    }

    // Servers of several users can each download into one data dir where
    // each can write every file and dir the others' downloads make in it
    // (README). Two users, each of a group of their own and both of a third,
    // take turns in a data dir of that group with the setgid bit: A
    // downloads a selection, B switches the dir to another, A back to the
    // first, and B to the other again once kills of A's downloads left a
    // decode's dir and the temp files of the ledger and the stamp. Each has
    // a umask of 002: each download goes through, and the dir ends as B's
    // selection leaves it. Under a umask of 022, the region's dir made
    // group-writable by hand, B can't append to the ledger A made, nor
    // finish A's decode, and its downloads fail, naming the file and the
    // dir. Run as root, the downloads run as the users (as_user); else the
    // test can't be another user, and ends.
    #[cfg(target_os = "linux")]
    #[test]
    fn downloads_of_two_users_into_one_dir_go_through_where_each_has_umask_002() {
        use std::os::unix::fs::PermissionsExt;
        const SHARED: u32 = 65530;
        const A: (u32, u32) = (65531, 65531);
        const B: (u32, u32) = (65532, 65532);
        if unsafe { libc::geteuid() } != 0 {
            return;
        }
        let rt = tokio::runtime::Runtime::new().unwrap();
        let bucket = rt.block_on(test_bucket::serve(test_bucket::template_components()));
        let (major, none) = (sel("europe", "admin", "simplified", "major"), sel("europe", "admin", "simplified", "none"));
        let mut got = Vec::new();
        for umask in [0o022, 0o002] {
            let root = ScratchDir::new();
            let dir = root.path().join("europe");
            std::os::unix::fs::chown(root.path(), None, Some(SHARED)).unwrap();
            std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o2775)).unwrap();
            if umask == 0o022 {
                std::fs::create_dir(&dir).unwrap();
                std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o2775)).unwrap();
            }
            let run = |(uid, gid): (u32, u32), s: &RegionSelection| {
                let got = as_user(uid, gid, SHARED, umask, |rt| rt.block_on(download(&bucket, s, root.path())));
                got.map_err(|e| e.to_string().replace(&dir.display().to_string(), "<dir>"))
            };
            let mut turns = vec![run(A, &major), run(B, &none), run(A, &major)];
            // A's download of major killed once its pois decode put
            // poi_records.bin in (a_download_finishes_what_a_killed_decode_left_before_it_lists_the_files_to_remove),
            // and kills of others as they wrote the ledger and the stamp.
            as_user(A.0, A.1, SHARED, umask, |_| {
                let mut ledger = std::fs::OpenOptions::new().append(true).open(dir.join(FILE_LEDGER)).unwrap();
                std::io::Write::write_all(&mut ledger, b"poi_records.bin -\npoi_vertices.bin -\n").unwrap();
                std::fs::remove_file(dir.join(BUILD_STAMP)).unwrap();
                std::fs::create_dir(dir.join(KILLED_DECODES_DIR)).unwrap();
                std::fs::rename(dir.join("poi_records.bin"), dir.join(KILLED_DECODES_DIR).join(".earlier.poi_records.bin")).unwrap();
                std::fs::write(dir.join("poi_records.bin"), b"a member").unwrap();
                for name in [FILE_LEDGER, BUILD_STAMP] {
                    std::fs::write(dir.join(format!("{name}.tmp")), b"cut short").unwrap();
                }
            });
            turns.push(run(B, &none));
            let differ = if turns.iter().all(Result::is_ok) { differing(&held(&dir), &files_of(&bucket, &none)) } else { Vec::new() };
            got.push((umask, turns, differ));
        }
        let refused = vec![
            Ok(()),
            Err("I/O error: write <dir>/.file_sha256: Permission denied (os error 13)".to_string()),
            Ok(()),
            Err("I/O error: <dir>/.transport.9.0 can't be finished (Permission denied (os error 13)): it would put back poi_records.bin".to_string()),
        ];
        assert_eq!(got, [(0o022, refused, Vec::new()), (0o002, vec![Ok(()), Ok(()), Ok(()), Ok(())], Vec::new())]);
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
            (Box::new(|u: &mut UnitEntry| u.transport_version = 2) as Box<dyn Fn(&mut UnitEntry)>,
             format!("transport unit {unit}: a unit of transport version 2: this build reads version {TRANSPORT_VERSION} only")),
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
