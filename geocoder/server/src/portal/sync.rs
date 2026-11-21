// The portal's sync: it keeps the latest build the bucket names in
// <data>/builds/<date>-<n>, fetched with the downloader's own fetches of a
// file or a transport unit (each checked against the manifest's sha256 as
// it comes), and switches to it only once every dataset of it loads.
//
// A build dir holds configurations.json, the build's files as it lays them
// out (<region>/<dir>/<name>), a dir of links per dataset (sets/<id>/, each
// file by its name, as a client's flat download holds them), the ledger of
// the objects in place (FETCHED) and, once all are, COMPLETE. Each sync of a
// build goes into a dir of its own, named anew, which no server maps until
// it is complete: a sync never writes into the dir of the build served,
// and a build found damaged is fetched again into a new dir. A sync killed
// part way resumes in its dir, fetching only the objects its ledger
// doesn't name. Files of the same bytes within a build are fetched once and
// hard-linked (catalog::Object::link).
//
// A build is the one of a date as its configurations.json was published: a
// rerun of the nightly that publishes a date's build again makes another,
// which the sync fetches into a new dir though it serves the one before.
// A build of another build version, or one whose datasets don't all load,
// is refused: never switched to, and recorded in REFUSED with why and its
// manifest's sha256, so that it is not fetched again while latest.json
// names it and the bucket holds it as it was refused. A query-portal holds
// the data dir's lock (VOLUME_LOCK) for as long as it runs, so that no
// other process prunes its builds, and every round runs under one lock
// (LOCK); the first of a process serves the newest local build whose files
// hold their manifest's bytes (each read against its sha256) and that
// loads, before it asks the bucket anything: none is served before its
// files check out. A build the data dir holds complete is served again
// from its dir (latest.json moved back to it) once its files check out,
// not fetched. The prune keeps the build served, the one served before it
// and the dir of the latest build a sync left part way.

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::catalog::{self, Plan};
use super::{Latest, Portal, Progress, Served, BUILD_VERSION};
use crate::downloader::{self, Configurations, DownloadError, FileEntry};
use query_server::lower_hex;

/// The dir of the builds, in the data dir.
pub const BUILDS: &str = "builds";
/// A build's manifest, in its dir.
pub const MANIFEST: &str = "configurations.json";
/// The dir of a build's datasets, in its dir.
pub const SETS: &str = "sets";
/// The ledger of a build dir: the path of each object in place, one a
/// line, appended once its files are.
pub const FETCHED: &str = ".fetched";
/// Put in a build dir once every file of the build and every dataset's
/// links are: the build's date.
pub const COMPLETE: &str = ".complete";
/// The lock every round holds (flock), in the data dir.
pub const LOCK: &str = "sync.lock";
/// The lock a query-portal holds for as long as it runs (flock), in the
/// data dir: one process a volume.
pub const VOLUME_LOCK: &str = "portal.lock";
/// The record of the builds refused, in the data dir.
pub const REFUSED: &str = "refused.json";
/// A date in this file in the data dir stands for latest.json's: the sync
/// fetches that build instead.
pub const LATEST_OVERRIDE: &str = "latest.override";
/// The room a sync wants free beyond its files'.
pub const FREE_MARGIN: u64 = 2 << 30;

/// A build refused by a server of build version `version`: the build of
/// `date` as it was published, its configurations.json of sha256
/// `manifest_sha256` (a build published again under its date is another).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Refusal {
    pub date: String,
    pub manifest_sha256: String,
    pub version: u32,
    pub reason: String,
    pub at: String,
}

/// A build dir's name: `<date>-<n>`, ordered by date, then n.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BuildDir {
    pub date: String,
    pub n: u32,
}

impl BuildDir {
    pub fn parse(name: &str) -> Option<BuildDir> {
        let (date, n) = name.rsplit_once('-')?;
        if !is_date(date) || n.is_empty() || !n.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        Some(BuildDir { date: date.to_string(), n: n.parse().ok()? })
    }

    pub fn name(&self) -> String {
        format!("{}-{}", self.date, self.n)
    }
}

/// Whether `s` is a date as builds are named: YYYY-MM-DD.
pub fn is_date(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 10 && b.iter().enumerate().all(|(i, c)| if i == 4 || i == 7 { *c == b'-' } else { c.is_ascii_digit() })
}

/// The time `t` as ISO 8601 in UTC, to the second.
pub fn iso(t: SystemTime) -> String {
    let secs = t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let (days, rem) = ((secs / 86_400) as i64, secs % 86_400);
    // The civil date of a day count (days_from_civil's inverse, H. Hinnant).
    let z = days + 719_468;
    let (era, doe) = (z.div_euclid(146_097), z.rem_euclid(146_097));
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", rem / 3_600, rem % 3_600 / 60, rem % 60)
}

fn gib(bytes: u64) -> String {
    format!("{:.2} GiB", bytes as f64 / (1u64 << 30) as f64)
}

/// The sha256 of a configurations.json's bytes, which tells a build from
/// the one published before it under its date.
pub fn manifest_sha256(bytes: &[u8]) -> String {
    lower_hex(&Sha256::digest(bytes))
}

/// The downloader's and the portal's reading of a configurations.json.
pub fn parse_manifest(bytes: &[u8]) -> Result<(Configurations, catalog::Manifest), String> {
    let cfg = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let manifest = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    Ok((cfg, manifest))
}

/// The objects the ledger of the build dir `root` names; a last line a
/// kill cut short names none.
pub fn ledger(root: &Path) -> BTreeSet<String> {
    let text = std::fs::read_to_string(root.join(FETCHED)).unwrap_or_default();
    let whole = text.rfind('\n').map_or("", |end| &text[..end]);
    whole.lines().filter(|l| !l.is_empty()).map(str::to_string).collect()
}

/// Whether each of `files` is at its path under `root` at its size.
fn sized(root: &Path, cfg: &Configurations, files: &[String]) -> bool {
    files.iter().all(|f| std::fs::metadata(root.join(f)).is_ok_and(|m| m.is_file() && m.len() == cfg.files[f].size_raw))
}

/// Writes `bytes` to `path` as one: under a temp name beside it, then
/// renamed over it.
fn write_whole(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)
}

/// Puts a hard link of `from` at `to`, over what is there.
fn relink(from: &Path, to: &Path) -> std::io::Result<()> {
    if let Some(dir) = to.parent() {
        std::fs::create_dir_all(dir)?;
    }
    match std::fs::remove_file(to) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
        _ => {}
    }
    std::fs::hard_link(from, to)
}

/// The files the build holds elsewhere byte for byte, as hard links of
/// those (catalog::Object::link); then each dataset's dir of links, made
/// anew.
fn link(root: &Path, plan: &Plan) -> Result<(), String> {
    let err = |path: &Path, e: std::io::Error| format!("link {}: {e}", path.display());
    for object in &plan.objects {
        for (to, from) in object.files.iter().zip(object.link.iter().flatten()) {
            relink(&root.join(from), &root.join(to)).map_err(|e| err(&root.join(to), e))?;
        }
    }
    for dataset in &plan.datasets {
        let dir = root.join(SETS).join(&dataset.id);
        match std::fs::remove_dir_all(&dir) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(err(&dir, e)),
            _ => {}
        }
        for (name, path) in dataset.links() {
            relink(&root.join(&path), &dir.join(&name)).map_err(|e| err(&dir.join(&name), e))?;
        }
    }
    Ok(())
}

/// Whether every file of the complete build dir `root`, and each dataset's
/// link to one, is there at its size; else the first that isn't.
fn in_place(root: &Path, cfg: &Configurations, plan: &Plan) -> Result<(), String> {
    for f in plan.objects.iter().flat_map(|o| &o.files) {
        if !sized(root, cfg, std::slice::from_ref(f)) {
            return Err(format!("{f} is not there at its size"));
        }
    }
    for dataset in &plan.datasets {
        for (name, path) in dataset.links() {
            let at = root.join(SETS).join(&dataset.id).join(&name);
            if !std::fs::metadata(&at).is_ok_and(|m| m.len() == cfg.files[&path].size_raw) {
                return Err(format!("{} is not there at its size", at.display()));
            }
        }
    }
    Ok(())
}

/// Whether every file of the build dir `root` holds the bytes of its
/// manifest sha256 (each inode read once, a few at a time); else the first
/// found that doesn't.
fn verify(root: &Path, cfg: &Configurations, plan: &Plan) -> Result<(), String> {
    use std::os::unix::fs::MetadataExt;
    let mut seen = HashSet::new();
    let mut files = Vec::new();
    for f in plan.objects.iter().flat_map(|o| &o.files) {
        let m = std::fs::metadata(root.join(f)).map_err(|e| format!("{f}: {e}"))?;
        if seen.insert((m.dev(), m.ino())) {
            files.push(f);
        }
    }
    let next = std::sync::atomic::AtomicUsize::new(0);
    let failed = std::sync::Mutex::new(None);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| loop {
                let at = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(f) = files.get(at) else { break };
                let got = (|| {
                    use std::io::Read;
                    let (mut file, mut hasher, mut buf) = (std::fs::File::open(root.join(f))?, Sha256::new(), vec![0u8; 1 << 20]);
                    loop {
                        match file.read(&mut buf)? {
                            0 => return Ok::<_, std::io::Error>(lower_hex(&hasher.finalize())),
                            n => hasher.update(&buf[..n]),
                        }
                    }
                })();
                let why = match got {
                    Ok(sha256) if sha256 == cfg.files[*f].sha256 => continue,
                    Ok(sha256) => format!("{f} holds bytes of sha256 {sha256}, not the manifest's {}", cfg.files[*f].sha256),
                    Err(e) => format!("{f}: {e}"),
                };
                failed.lock().unwrap().get_or_insert(why);
                break;
            });
        }
    });
    failed.into_inner().unwrap().map_or(Ok(()), Err)
}

/// The lock of the data dir `data` (VOLUME_LOCK), held until the file
/// returned is dropped: a query-portal takes it at start and holds it for
/// good. While another process holds it, an error saying so at once: the
/// second process exits rather than waits, so that two services on one
/// volume (two rounds on it would prune each other's builds) show as one
/// that fails to start, with why, never as one that hangs before it serves.
pub fn hold_volume(data: &Path) -> Result<std::fs::File, String> {
    let path = data.join(VOLUME_LOCK);
    std::fs::create_dir_all(data).map_err(|e| format!("mkdir {}: {e}", data.display()))?;
    let file = std::fs::File::options().read(true).write(true).create(true).truncate(false).open(&path)
        .map_err(|e| format!("lock {}: {e}", path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(format!(
            "another query-portal runs on {}: it holds {}, and two on one volume would prune each other's builds",
            data.display(), path.display()
        )),
        Err(std::fs::TryLockError::Error(e)) => Err(format!("lock {}: {e}", path.display())),
    }
}

/// How many tries at a file this process has made, for the names of their
/// temporary files.
static TRIES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// One try at `object` of the build of `date` at `base_url`
/// (downloader::download_file_through or download_unit), its files'
/// entries those of `cfg`, into the build dir `root`. A file's try writes
/// a temporary file of its own beside it, `<file>.<pid>.<n>.tmp`, as a
/// unit's decode writes a dir of its own: a try whose round has gone may
/// still be writing its file while the next try writes another.
pub async fn try_object(http: &reqwest::Client, base_url: &str, date: &str, cfg: &Configurations, object: &catalog::Object, root: &Path) -> Result<(), DownloadError> {
    let files: Vec<(&str, &FileEntry)> = object.files.iter().map(|f| (f.as_str(), &cfg.files[f])).collect();
    let dir = root.join(Path::new(&object.path).parent().unwrap_or(Path::new("")));
    tokio::fs::create_dir_all(&dir).await.map_err(|e| DownloadError::Io(format!("mkdir {}: {e}", dir.display())))?;
    if object.unit {
        return downloader::download_unit(http, base_url, date, &object.path, &files, &dir).await;
    }
    let dest = root.join(&object.path);
    let mut tmp = dest.as_os_str().to_owned();
    tmp.push(format!(".{}.{}.tmp", std::process::id(), TRIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed)));
    downloader::download_file_through(http, base_url, date, &object.path, files[0].1, &dest, Path::new(&tmp)).await
}

#[derive(Deserialize)]
struct LatestJson {
    latest: String,
    #[serde(default)]
    build_version: Option<u32>,
}

impl Portal {
    fn builds(&self) -> PathBuf {
        self.data.join(BUILDS)
    }

    /// The build dirs, oldest first, each with whether it is complete.
    fn list(&self) -> Result<Vec<(BuildDir, bool)>, String> {
        let entries = match std::fs::read_dir(self.builds()) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(format!("read {}: {e}", self.builds().display())),
        };
        let mut dirs: Vec<(BuildDir, bool)> = entries
            .filter_map(|e| e.ok())
            .filter_map(|e| BuildDir::parse(&e.file_name().to_string_lossy()).map(|d| (d, e.path().join(COMPLETE).exists())))
            .collect();
        dirs.sort();
        Ok(dirs)
    }

    fn unusable(&self, dir: &str) -> bool {
        self.unusable.lock().unwrap_or_else(|e| e.into_inner()).contains(dir)
    }

    fn set_unusable(&self, dir: &str) {
        self.unusable.lock().unwrap_or_else(|e| e.into_inner()).insert(dir.to_string());
    }

    fn running(&self, what: String) {
        eprintln!("portal sync: {what}");
        self.set_status(|s| s.running = Some(what));
    }

    /// Runs the rounds for good, a round every interval (retry_after once
    /// one failed). A round that panics is a round that failed.
    pub async fn run(self: Arc<Self>) {
        let mut first = true;
        loop {
            self.set_status(|s| {
                s.running = Some("starting".to_string());
                s.last_started = Some(iso(SystemTime::now()));
                s.next_round = None;
            });
            let me = self.clone();
            let outcome = tokio::spawn(async move { me.round(first).await }).await;
            let (result, wait) = match outcome {
                Ok(Ok(done)) => (format!("ok: {done}"), self.config.interval),
                Ok(Err(why)) => (format!("failed: {why}"), self.config.retry_after),
                Err(e) => (format!("failed: the round panicked: {e}"), self.config.retry_after),
            };
            eprintln!("portal sync: {result}");
            self.set_status(|s| {
                s.running = None;
                s.progress = None;
                s.last_finished = Some(iso(SystemTime::now()));
                s.last_result = Some(result);
                s.next_round = Some(iso(SystemTime::now() + wait));
            });
            first = false;
            tokio::time::sleep(wait).await;
        }
    }

    /// One round, under the lock: the start, on the `first` of a process
    /// (start), then the sync, then the prune. What it did, or why it
    /// failed.
    pub async fn round(&self, first: bool) -> Result<String, String> {
        let _lock = self.lock().await?;
        if first {
            self.start().await;
        }
        let synced = self.sync().await;
        self.prune().await;
        synced
    }

    /// Takes the lock every round holds, waiting while another holds it
    /// (a round of another process on the data dir).
    async fn lock(&self) -> Result<std::fs::File, String> {
        let path = self.data.join(LOCK);
        let at = path.clone();
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(path.parent().unwrap())?;
            let file = std::fs::File::options().read(true).write(true).create(true).truncate(false).open(&path)?;
            file.lock()?;
            Ok::<_, std::io::Error>(file)
        })
        .await
        .map_err(|e| format!("lock task: {e}"))?
        .map_err(|e| format!("lock {}: {e}", at.display()))
    }

    /// Serves the newest complete build dir that opens (open: of
    /// BUILD_VERSION, not refused, every file of it there and holding the
    /// bytes of its manifest's sha256, and every dataset loading), if any,
    /// trying each from the newest down: none is served before its files
    /// check out. A dir that can't be served, a damaged one among them, is
    /// left to the prune, and its build fetched again (sync).
    async fn start(&self) {
        let dirs = match self.list() {
            Ok(dirs) => dirs,
            Err(why) => return eprintln!("portal start: {why}"),
        };
        for (dir, _) in dirs.iter().rev().filter(|(_, complete)| *complete) {
            let name = dir.name();
            match self.open(&name).await {
                Ok(served) => {
                    self.serve(served);
                    break;
                }
                Err(why) => {
                    eprintln!("portal start: build {name} can't be served: {why}");
                    self.set_unusable(&name);
                    self.set_status(|s| s.not_served.push(format!("{name} here can't be served: {why}")));
                }
            }
        }
    }

    /// The complete build dir `name`, checked and loaded; or why it can't
    /// be served. Its files are each read against their manifest's sha256
    /// (verify), which also brings them into the page cache, before any
    /// dataset of it loads.
    async fn open(&self, name: &str) -> Result<Served, String> {
        let root = self.builds().join(name);
        let bytes = std::fs::read(root.join(MANIFEST)).map_err(|e| format!("{MANIFEST}: {e}"))?;
        let (cfg, manifest) = parse_manifest(&bytes).map_err(|e| format!("{MANIFEST}: {e}"))?;
        if manifest.build.version != BUILD_VERSION {
            return Err(format!("it is of build version {}; this server reads build version {BUILD_VERSION}", manifest.build.version));
        }
        let sha256 = manifest_sha256(&bytes);
        if let Some(refused) = self.refusal(&manifest.build.date, &sha256) {
            return Err(format!("it was refused: {}", refused.reason));
        }
        let plan = catalog::plan(&cfg, &manifest)?;
        self.running(format!("checking the files of build {name}"));
        let (at, checked) = (root.clone(), plan.clone());
        tokio::task::spawn_blocking(move || in_place(&at, &cfg, &checked).and_then(|()| verify(&at, &cfg, &checked)))
            .await
            .map_err(|e| format!("check task: {e}"))??;
        self.running(format!("loading build {name}"));
        self.load(name, plan, sha256).await
    }

    /// Every dataset of the build dir `name`, whose configurations.json is of
    /// sha256 `manifest_sha256`, loaded; or the first that doesn't load, and
    /// why.
    async fn load(&self, name: &str, plan: Plan, manifest_sha256: String) -> Result<Served, String> {
        let (sets, load, dir) = (self.builds().join(name).join(SETS), self.config.load.clone(), name.to_string());
        tokio::task::spawn_blocking(move || {
            let mut indexes = Vec::with_capacity(plan.datasets.len());
            for dataset in &plan.datasets {
                let index = load(&sets.join(&dataset.id)).map_err(|e| format!("dataset {}: {e}", dataset.id))?;
                indexes.push(Arc::new(index));
            }
            let datasets_json = plan.json(&dir).to_string();
            Ok(Served { dir, manifest_sha256, plan, indexes, datasets_json })
        })
        .await
        .map_err(|e| format!("load task: {e}"))?
    }

    /// Serves `served` from now on; the build served before, if any.
    fn serve(&self, served: Served) -> Option<Arc<Served>> {
        let dir = served.dir.clone();
        eprintln!("portal: serving build {dir}");
        let before = self.served.swap(Some(Arc::new(served)));
        self.set_status(|s| {
            s.served = Some(dir);
            s.not_served.clear();
        });
        before
    }

    /// The refusals on record, of any server's build version.
    fn refusals(&self) -> Vec<Refusal> {
        match std::fs::read(self.data.join(REFUSED)) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| {
                eprintln!("portal: {REFUSED} doesn't parse ({e}); taking it for empty");
                Vec::new()
            }),
            Err(_) => Vec::new(),
        }
    }

    fn keep_refusals(&self, refusals: Vec<Refusal>) -> Result<(), String> {
        let path = self.data.join(REFUSED);
        write_whole(&path, serde_json::to_string_pretty(&refusals).unwrap().as_bytes())
            .map_err(|e| format!("write {}: {e}", path.display()))?;
        let ours: Vec<Refusal> = refusals.into_iter().filter(|r| r.version == BUILD_VERSION).collect();
        self.set_status(|s| s.refused = ours);
        Ok(())
    }

    /// This server's refusal of the build of `date` whose configurations.json
    /// is of sha256 `sha256`, if it refused it.
    fn refusal(&self, date: &str, sha256: &str) -> Option<Refusal> {
        self.refusals().into_iter().find(|r| r.date == date && r.manifest_sha256 == sha256 && r.version == BUILD_VERSION)
    }

    /// Records that this server refuses the build of `date` whose
    /// configurations.json is of sha256 `sha256`, and why: what the round
    /// did.
    fn refuse(&self, date: &str, sha256: &str, why: &str) -> Result<String, String> {
        eprintln!("portal sync: refusing build {date}: {why}");
        let mut refusals = self.refusals();
        refusals.retain(|r| !(r.date == date && r.manifest_sha256 == sha256 && r.version == BUILD_VERSION));
        refusals.push(Refusal {
            date: date.to_string(),
            manifest_sha256: sha256.to_string(),
            version: BUILD_VERSION,
            reason: why.to_string(),
            at: iso(SystemTime::now()),
        });
        self.keep_refusals(refusals)?;
        Ok(format!("refused build {date}: {why}"))
    }

    /// The bytes at `url`, tried again after each of the retry delays while
    /// it fails.
    async fn fetch_bytes(&self, url: &str) -> Result<Vec<u8>, String> {
        let mut delays = self.config.retry_delays.iter();
        loop {
            let got = async {
                let resp = downloader::get(&self.http, url).await?;
                resp.bytes().await.map_err(|e| DownloadError::Http(format!("{url}: read body: {e}")))
            }
            .await;
            match (got, delays.next()) {
                (Ok(bytes), _) => return Ok(bytes.to_vec()),
                (Err(e), Some(delay)) => {
                    eprintln!("portal sync: {e}; trying again in {delay:?}");
                    tokio::time::sleep(*delay).await;
                }
                (Err(e), None) => return Err(e.to_string()),
            }
        }
    }

    /// The latest build: the date in LATEST_OVERRIDE, where the data dir
    /// holds one, else the one latest.json names.
    async fn latest(&self) -> Result<Latest, String> {
        if let Ok(text) = std::fs::read_to_string(self.data.join(LATEST_OVERRIDE)) {
            let date = text.trim();
            if !date.is_empty() {
                if !is_date(date) {
                    return Err(format!("{LATEST_OVERRIDE} holds {date:?}, no date"));
                }
                return Ok(Latest { date: date.to_string(), build_version: None, manifest_sha256: None });
            }
        }
        let url = format!("{}/builds/latest.json", self.config.base_url.trim_end_matches('/'));
        let bytes = self.fetch_bytes(&url).await?;
        let latest: LatestJson = serde_json::from_slice(&bytes).map_err(|e| format!("{url}: {e}"))?;
        if !is_date(&latest.latest) {
            return Err(format!("{url} names {:?}, no date", latest.latest));
        }
        Ok(Latest { date: latest.latest, build_version: latest.build_version, manifest_sha256: None })
    }

    /// Fetches the latest build into a dir of its own and serves it once
    /// all its datasets load; what it did, or why it failed. The latest
    /// build is the one of the date latest.json names as its
    /// configurations.json is now: one published again under its date (a
    /// rerun of the nightly) is another build. A build of another build
    /// version, or that can't be served, is refused; one refused is not
    /// fetched again while it is the latest. With the latest served it does
    /// nothing; with it complete in a dir, it serves it from there.
    async fn sync(&self) -> Result<String, String> {
        self.running("reading the latest build".to_string());
        let mut latest = self.latest().await?;
        let date = latest.date.clone();
        self.set_status(|s| s.latest = Some(latest.clone()));
        self.running(format!("fetching build {date}'s {MANIFEST}"));
        let base = self.config.base_url.trim_end_matches('/').to_string();
        let bytes = self.fetch_bytes(&format!("{base}/builds/{date}/{MANIFEST}")).await?;
        let sha256 = manifest_sha256(&bytes);
        latest.manifest_sha256 = Some(sha256.clone());
        self.set_status(|s| s.latest = Some(latest.clone()));
        // A refusal counts while latest.json names its build, as published
        // then.
        let refusals = self.refusals();
        let current: Vec<Refusal> = refusals.iter().filter(|r| r.date == date && r.manifest_sha256 == sha256).cloned().collect();
        if current.len() != refusals.len() {
            self.keep_refusals(current)?;
        } else {
            self.set_status(|s| s.refused = current.into_iter().filter(|r| r.version == BUILD_VERSION).collect());
        }
        if let Some(version) = latest.build_version.filter(|&v| v != BUILD_VERSION) {
            return self.refuse(&date, &sha256, &format!("it is of build version {version}; this server reads build version {BUILD_VERSION}"));
        }
        if let Some(refused) = self.refusal(&date, &sha256) {
            return Ok(format!("build {date} was refused: {}", refused.reason));
        }
        let served = self.served();
        if let Some(served) = served.as_ref().filter(|s| s.plan.date == date && s.manifest_sha256 == sha256) {
            return Ok(format!("build {date} is served ({})", served.dir));
        }

        let (cfg, manifest) = match parse_manifest(&bytes) {
            Ok(parsed) => parsed,
            Err(why) => return self.refuse(&date, &sha256, &format!("its {MANIFEST} doesn't parse: {why}")),
        };
        if manifest.build.date != date {
            return Err(format!("build {date}'s {MANIFEST} is build {}'s", manifest.build.date));
        }
        if manifest.build.version != BUILD_VERSION {
            return self.refuse(&date, &sha256, &format!("it is of build version {}; this server reads build version {BUILD_VERSION}", manifest.build.version));
        }
        let plan = match catalog::plan(&cfg, &manifest) {
            Ok(plan) => plan,
            Err(why) => return self.refuse(&date, &sha256, &why),
        };

        // A complete dir of this build, of its manifest, which this process
        // didn't find unusable (the build served before, latest.json moved
        // back to it): served again once its files hold their manifest's
        // bytes and it loads (open), nothing fetched.
        let dirs = self.list()?;
        for (d, _) in dirs.iter().rev().filter(|(d, complete)| *complete && d.date == date) {
            let name = d.name();
            if self.unusable(&name) || !std::fs::read(self.builds().join(&name).join(MANIFEST)).is_ok_and(|m| m == bytes) {
                continue;
            }
            match self.open(&name).await {
                Ok(served) => {
                    if let Some(before) = self.serve(served) {
                        eprintln!("portal sync: build {name} is served again in place of {}", before.dir);
                    }
                    return Ok(format!("build {date} is served ({name})"));
                }
                Err(why) => {
                    eprintln!("portal sync: build {name} can't be served again: {why}");
                    self.set_unusable(&name);
                }
            }
        }

        // A dir of this build a sync left incomplete, to resume, or a new
        // one.
        let resume = dirs.iter().rev()
            .find(|(d, complete)| d.date == date && !complete)
            .map(|(d, _)| d.name())
            .filter(|name| !self.unusable(name) && std::fs::read(self.builds().join(name).join(MANIFEST)).is_ok_and(|m| m == bytes));
        let name = resume.clone().unwrap_or_else(|| {
            let n = dirs.iter().filter(|(d, _)| d.date == date).map(|(d, _)| d.n).max().unwrap_or(0) + 1;
            BuildDir { date: date.clone(), n }.name()
        });
        let root = self.builds().join(&name);
        if resume.is_some() {
            // What decodes a kill cut short left, finished before the
            // ledger's objects are checked.
            let dirs: BTreeSet<PathBuf> = plan.objects.iter().filter(|o| o.unit)
                .filter_map(|o| Path::new(&o.path).parent().map(|d| root.join(d)))
                .filter(|d| d.is_dir())
                .collect();
            tokio::task::spawn_blocking(move || dirs.iter().try_for_each(|d| query_server::transport::finish_leftovers_in(d)))
                .await
                .map_err(|e| format!("leftovers task: {e}"))?
                .map_err(|e| format!("build {name}: {e}"))?;
        }
        let done = ledger(&root);
        let todo: Vec<&catalog::Object> = plan.objects.iter()
            .filter(|o| o.link.is_none() && !(done.contains(&o.path) && sized(&root, &cfg, &o.files)))
            .collect();

        let need: u64 = todo.iter().flat_map(|o| &o.files).map(|f| cfg.files[f].size_raw).sum();
        let free = (self.config.free_space)(&self.data).map_err(|e| format!("free space in {}: {e}", self.data.display()))?;
        if free < need + FREE_MARGIN {
            return Err(format!(
                "{} free in {}, {} needed to fetch build {date}: its files' {} and a margin of {}",
                gib(free), self.data.display(), gib(need + FREE_MARGIN), gib(need), gib(FREE_MARGIN)
            ));
        }
        if resume.is_none() {
            std::fs::create_dir_all(&root).map_err(|e| format!("mkdir {}: {e}", root.display()))?;
            write_whole(&root.join(MANIFEST), &bytes).map_err(|e| format!("write {}: {e}", root.join(MANIFEST).display()))?;
        }

        self.fetch(&root, &cfg, &date, todo).await?;
        self.running(format!("linking build {name}"));
        let (at, linked) = (root.clone(), plan.clone());
        tokio::task::spawn_blocking(move || {
            link(&at, &linked)?;
            write_whole(&at.join(COMPLETE), linked.date.as_bytes()).map_err(|e| format!("write {}: {e}", at.join(COMPLETE).display()))
        })
        .await
        .map_err(|e| format!("link task: {e}"))??;

        self.running(format!("loading build {name}"));
        match self.load(&name, plan, sha256.clone()).await {
            Ok(loaded) => {
                if let Some(before) = self.serve(loaded) {
                    eprintln!("portal sync: build {name} is served in place of {}", before.dir);
                }
                Ok(format!("build {date} is served ({name})"))
            }
            Err(why) => {
                self.set_unusable(&name);
                self.refuse(&date, &sha256, &format!("it doesn't load: {why}"))
            }
        }
    }

    /// Fetches `todo` into the build dir `root`, the largest first, a few
    /// at once: each object into the dir its path names, its line added to
    /// the ledger once its files are in. Every object is tried; an error
    /// names those that failed. A line that can't go into the ledger stops
    /// it: it starts no other fetch, and returns that error once every
    /// fetch it started has ended (a fetch's decode, on a blocking thread,
    /// writes into `root` until its fetch ends), so that nothing of the
    /// round writes in the dir once the round, and its lock, are over.
    async fn fetch(&self, root: &Path, cfg: &Configurations, date: &str, mut todo: Vec<&catalog::Object>) -> Result<(), String> {
        todo.sort_by(|a, b| b.size.cmp(&a.size).then_with(|| a.path.cmp(&b.path)));
        let mut progress = Progress {
            objects_total: todo.len(),
            bytes_total: todo.iter().map(|o| o.size).sum(),
            ..Default::default()
        };
        self.running(format!("fetching build {date}: {} objects, {}", progress.objects_total, gib(progress.bytes_total)));
        self.set_status(|s| s.progress = Some(progress.clone()));
        let (mut todo, mut fetching) = (todo.into_iter(), futures_util::stream::FuturesUnordered::new());
        let (mut failed, mut stopped) = (Vec::new(), None);
        loop {
            while stopped.is_none() && fetching.len() < self.config.fetches_at_once.max(1) {
                let Some(object) = todo.next() else { break };
                fetching.push(self.fetch_object(root, cfg, date, object));
            }
            let Some((object, got)) = fetching.next().await else { break };
            if let Err(e) = got {
                failed.push(format!("{}: {e}", object.path));
                continue;
            }
            if stopped.is_some() {
                continue;
            }
            use std::io::Write;
            let line = format!("{}\n", object.path);
            let noted = std::fs::OpenOptions::new().create(true).append(true).open(root.join(FETCHED))
                .and_then(|mut f| f.write_all(line.as_bytes()));
            if let Err(e) = noted {
                stopped = Some(format!("write {}: {e}", root.join(FETCHED).display()));
                continue;
            }
            progress.objects_done += 1;
            progress.bytes_done += object.size;
            self.set_status(|s| s.progress = Some(progress.clone()));
        }
        if let Some(why) = stopped {
            return Err(why);
        }
        if failed.is_empty() {
            Ok(())
        } else {
            Err(format!("{} of build {date}'s objects did not come down: {}", failed.len(), failed.join("; ")))
        }
    }

    /// Fetches `object` (downloader::download_file or download_unit) into
    /// the build dir `root`, tried again after each of the retry delays
    /// while it fails; the object, and how it went.
    async fn fetch_object<'a>(&self, root: &Path, cfg: &Configurations, date: &str, object: &'a catalog::Object) -> (&'a catalog::Object, Result<(), DownloadError>) {
        (object, self.fetch_attempts(root, cfg, date, object).await)
    }

    /// fetch_object's tries.
    async fn fetch_attempts(&self, root: &Path, cfg: &Configurations, date: &str, object: &catalog::Object) -> Result<(), DownloadError> {
        let mut delays = self.config.retry_delays.iter();
        loop {
            let got = try_object(&self.http, &self.config.base_url, date, cfg, object, root).await;
            match (got, delays.next()) {
                (Ok(()), _) => return Ok(()),
                (Err(e), Some(delay)) => {
                    eprintln!("portal sync: {}: {e}; trying again in {delay:?}", object.path);
                    tokio::time::sleep(*delay).await;
                }
                (Err(e), None) => return Err(e),
            }
        }
    }

    /// Removes every build dir but the one served; the newest complete one
    /// of another date that this process didn't find unusable (the one
    /// served before it: the one before it, or after latest.json moved back
    /// to an older build, the newer one it served); and the newest
    /// incomplete one of the latest build (to resume): of its date and, once
    /// the round fetched its configurations.json, of that manifest, not one
    /// of the build published before it under its date; or of any while the
    /// latest build is not known (a first round that couldn't ask the
    /// bucket).
    async fn prune(&self) {
        let dirs = match self.list() {
            Ok(dirs) => dirs,
            Err(why) => return eprintln!("portal prune: {why}"),
        };
        let mut keep = BTreeSet::new();
        if let Some(served) = self.served() {
            keep.insert(served.dir.clone());
            let date = BuildDir::parse(&served.dir).map(|d| d.date);
            let before = dirs.iter().rev()
                .find(|(d, complete)| *complete && Some(&d.date) != date.as_ref() && !self.unusable(&d.name()));
            if let Some((d, _)) = before {
                keep.insert(d.name());
            }
        }
        let latest = self.status().latest;
        let of_latest = |d: &BuildDir| match &latest {
            None => true,
            Some(l) => l.date == d.date && l.manifest_sha256.as_ref().is_none_or(|sha256| {
                std::fs::read(self.builds().join(d.name()).join(MANIFEST)).is_ok_and(|m| manifest_sha256(&m) == *sha256)
            }),
        };
        let staging = dirs.iter().rev()
            .find(|(d, complete)| !complete && of_latest(d) && !self.unusable(&d.name()));
        if let Some((d, _)) = staging {
            keep.insert(d.name());
        }
        for (d, _) in dirs {
            let name = d.name();
            if keep.contains(&name) {
                continue;
            }
            let path = self.builds().join(&name);
            eprintln!("portal prune: removing build {name}");
            match tokio::task::spawn_blocking(move || std::fs::remove_dir_all(path)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => eprintln!("portal prune: {name}: {e}"),
                Err(e) => eprintln!("portal prune: {name}: {e}"),
            }
        }
    }
}
