//! The dataset an index belongs to. Postings point into the geocoder files by record index and
//! string offset, so an index answers right only beside the exact files it was built from. Its
//! META_FILE records the sha256 of each of them, and `open` uses an index only while the files
//! beside it still hash the same: a shipped index for another day, region or tier, or one left
//! from before a patch, is built again from the files instead of being read. The files hashed
//! must be the ones the geocoder index was loaded from (`load`): one replaced under a loaded
//! index would have the index built from the old files but recorded as built from the new.
use crate::build::{self, build_held, has_extra_names, has_sub_index, has_unfinished_publish, held_subs, sub_index_files, BuildOpts, Contended, HeldIndexDir};
use crate::constants::*;
use crate::reader::SearchIndex;
use query_server::Index;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{ErrorKind, Read};
use std::path::Path;

/// The geocoder files an index is derived from: the records its postings point at, the string
/// tiers of their names and the rings its boundary points come from.
pub const DATASET_FILES: [&str; 7] = [
    "admin_polygons.bin",
    "admin_vertices.bin",
    "place_nodes.bin",
    "poi_records.bin",
    "strings_core.bin",
    "strings_layout.json",
    "strings_poi.bin",
];

/// The sha256 (hex) of each dataset file present, by name.
pub type Fingerprint = BTreeMap<String, String>;

/// A file's stat when it was hashed: its hash is reused while the file keeps it. Patches write
/// new files (a new inode and mtime), so a patched file is always hashed again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Stamp {
    size: u64,
    mtime_ns: u128,
    ino: u64,
}

impl Stamp {
    fn of(m: &std::fs::Metadata) -> Stamp {
        let mtime_ns = m.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).map_or(0, |d| d.as_nanos());
        #[cfg(unix)]
        let ino = std::os::unix::fs::MetadataExt::ino(m);
        #[cfg(not(unix))]
        let ino = 0;
        Stamp { size: m.len(), mtime_ns, ino }
    }
}

/// The stat of each dataset file present, by name.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Stamps(BTreeMap<String, Stamp>);

impl Stamps {
    /// The stamps of the dataset files in `dir`.
    pub fn of(dir: &Path) -> std::io::Result<Stamps> {
        let mut out = BTreeMap::new();
        for name in DATASET_FILES {
            match std::fs::metadata(dir.join(name)) {
                Ok(m) => {
                    out.insert(name.to_string(), Stamp::of(&m));
                }
                Err(e) if e.kind() == ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(Stamps(out))
    }
}

/// Runs `load` (which loads the geocoder index of the dataset in `dir`) and returns what it
/// loaded with the stamps of the files it read: the same before and after the load, else the
/// files were being replaced and it fails.
pub fn load<T>(dir: &Path, load: impl FnOnce() -> Result<T, String>) -> Result<(T, Stamps), String> {
    let stamps = || Stamps::of(dir).map_err(|e| format!("{}: {e}", dir.display()));
    let before = stamps()?;
    let loaded = load()?;
    if stamps()? != before {
        return Err(format!("The dataset in {} changed while it was loading", dir.display()));
    }
    Ok((loaded, before))
}

/// Hashes of dataset files, each with the stat it was taken at; kept beside an index
/// (HASH_CACHE_FILE) so opening it does not read the dataset again.
#[derive(Default)]
pub struct HashCache(BTreeMap<String, (Stamp, String)>);

impl HashCache {
    /// The cache at `path`; empty when it is missing or unreadable.
    pub fn load(path: &Path) -> HashCache {
        let Ok(text) = std::fs::read_to_string(path) else { return HashCache::default() };
        let Ok(Value::Object(files)) = serde_json::from_str::<Value>(&text) else { return HashCache::default() };
        let entry = |v: &Value| -> Option<(Stamp, String)> {
            let stamp = Stamp { size: v["size"].as_u64()?, mtime_ns: v["mtime_ns"].as_str()?.parse().ok()?, ino: v["ino"].as_u64()? };
            Some((stamp, v["sha256"].as_str()?.to_string()))
        };
        HashCache(files.iter().filter_map(|(name, v)| Some((name.clone(), entry(v)?))).collect())
    }

    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let files: serde_json::Map<String, Value> = self
            .0
            .iter()
            .map(|(name, (s, sha))| (name.clone(), json!({"size": s.size, "mtime_ns": s.mtime_ns.to_string(), "ino": s.ino, "sha256": sha})))
            .collect();
        std::fs::write(path, serde_json::to_string_pretty(&Value::Object(files)).unwrap())
    }

    /// The fingerprint of the dataset in `dir`, its files stamped `stamps`: only those whose
    /// stamp the cache has not seen are hashed.
    pub fn fingerprint(&mut self, dir: &Path, stamps: &Stamps) -> std::io::Result<Fingerprint> {
        self.0.retain(|name, _| stamps.0.contains_key(name));
        let mut out = Fingerprint::new();
        for (name, stamp) in &stamps.0 {
            let sha = match self.0.get(name) {
                Some((seen, sha)) if seen == stamp => sha.clone(),
                _ => {
                    let sha = sha256_file(&dir.join(name))?;
                    self.0.insert(name.clone(), (*stamp, sha.clone()));
                    sha
                }
            };
            out.insert(name.clone(), sha);
        }
        Ok(out)
    }
}

fn sha256_file(path: &Path) -> std::io::Result<String> {
    let mut f = File::open(path)?;
    let mut sha = Sha256::new();
    let mut buf = vec![0u8; HASH_READ_BUFFER];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        sha.update(&buf[..n]);
    }
    Ok(format!("{:x}", sha.finalize()))
}

/// The fingerprint of the dataset in `dir` as loaded, its files stamped `loaded` (`load`), the
/// hashes `hashes` holds reused: refused when they no longer have those stamps, before the
/// hashing or after it, as the files hashed would not be the ones loaded.
fn fingerprint_loaded(dir: &Path, loaded: &Stamps, hashes: &mut HashCache) -> Result<Fingerprint, String> {
    let io = |e: std::io::Error| format!("{}: {e}", dir.display());
    let unchanged = || match Stamps::of(dir).map_err(io)? {
        now if now == *loaded => Ok(()),
        _ => Err(format!("The dataset in {} changed since it was loaded: load it again", dir.display())),
    };
    unchanged()?;
    let have = hashes.fingerprint(dir, loaded).map_err(io)?;
    unchanged()?;
    Ok(have)
}

/// The META_FILE of the index in `index` (None without a readable one).
fn meta_of(index: &Path) -> Option<Value> {
    serde_json::from_str(&std::fs::read_to_string(index.join(META_FILE)).ok()?).ok()
}

/// The fingerprint of the dataset the index in `index` was built from (None without one).
pub fn built_from(index: &Path) -> Option<Fingerprint> {
    let meta = meta_of(index)?;
    let files = meta[META_DATASET_KEY].as_object()?;
    files.iter().map(|(name, sha)| Some((name.clone(), sha.as_str()?.to_string()))).collect()
}

/// The META_FILE fields of an index built with `opts` from the dataset whose files hash
/// `dataset`, loaded as `geo`: `build::index_meta` and the fingerprint `built_from` reads.
pub fn index_meta(geo: &Index, opts: &BuildOpts, dataset: &Fingerprint) -> serde_json::Map<String, Value> {
    let mut meta = build::index_meta(geo, opts);
    meta.insert(META_DATASET_KEY.to_string(), json!(dataset));
    meta
}

/// The META_FILE of a `place-search build` of the sub-indexes `subs` into `out` from the dataset
/// in `dir`, loaded as `geo` from files stamped `loaded` (`load`): merged into the one there, so
/// sub-indexes can be built one at a time. Refused when the files are no longer the ones loaded
/// (`fingerprint_loaded`), or when a sub-index it keeps was built from other files, as the
/// fingerprint it records is that of every sub-index there.
pub fn merged_meta(geo: &Index, loaded: &Stamps, opts: &BuildOpts, dir: &Path, out: &Path, subs: &[&str]) -> Result<Value, String> {
    // An index never reads the POIs of a dataset without any, so those are not kept.
    let mut replacing = subs.to_vec();
    if geo.poi_records.is_none() {
        replacing.push("poi");
    }
    build::check_retained_names(out, &replacing, opts.extras.is_some())?;
    let dataset = fingerprint_loaded(dir, loaded, &mut HashCache::default())?;
    let kept = SUB_INDEXES.iter().find(|s| !replacing.contains(s) && has_sub_index(out, s));
    if let Some(kept) = kept.filter(|_| built_from(out).as_ref() != Some(&dataset)) {
        return Err(format!("retained {kept} sub-index was built from other dataset files; rebuild with --sub all"));
    }
    let mut meta: Value = std::fs::read_to_string(out.join(META_FILE))
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_else(|| json!({}));
    for (k, v) in index_meta(geo, opts, &dataset) {
        meta[k] = v;
    }
    Ok(meta)
}

/// Where the index `open` opened came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    /// The index there was built from these files: shipped with the dataset, or built here before.
    Current,
    /// There was none, or it was built from other files: it was built from these.
    Built,
}

/// How `open` builds an index.
pub struct BuildOnDevice {
    pub threads: usize,
    /// Sort-run memory (MiB): the build's peak is about this plus 8 MiB.
    pub mem_mb: usize,
}

impl Default for BuildOnDevice {
    fn default() -> Self {
        BuildOnDevice { threads: 1, mem_mb: DEFAULT_RUN_BUDGET_MB }
    }
}

/// Whether `index` holds every file `SearchIndex::open` maps for `geo`: a `place-search build
/// --sub` into an empty directory records the whole dataset, but holds only what it built.
fn is_complete(geo: &Index, index: &Path) -> bool {
    let sub_files = held_subs(geo, SUB_INDEXES).into_iter().flat_map(sub_index_files);
    sub_files.chain([BOUNDARY_FILE.to_string()]).all(|f| index.join(f).is_file())
}

/// Opens the place search index in `index` of the dataset in `dir` (loaded as `geo` from files
/// stamped `loaded`, by `load`): the one there when it is whole (`is_complete`), no publish into
/// it was left unfinished and it was built from these files without extra names (it has no
/// extra-names file to read them with), else one built from them first by `build_held` with the
/// default options (staged in `index`, every file moved in only once all are complete). Fails when
/// the files are no longer the ones loaded. The open holds `index` (`HeldIndexDir`, which every
/// build takes) until the index is mapped, waiting while another open or a build has it: a
/// second open finds the first's index current, and no build moves files in while one is read,
/// or has its stage cleared.
pub fn open<'g>(geo: &'g Index, loaded: &Stamps, dir: &Path, index: &Path, on_device: &BuildOnDevice) -> Result<(SearchIndex<'g>, Origin), String> {
    let held = HeldIndexDir::take(index, Contended::Wait)?;
    let cache = index.join(HASH_CACHE_FILE);
    let mut hashes = HashCache::load(&cache);
    let have = fingerprint_loaded(dir, loaded, &mut hashes)?;

    // Held, so an unfinished publish is a dead build's: some files there may be its own and some
    // the index's before it, whose META_FILE matches whenever both were built from these files.
    let reusable = !has_unfinished_publish(index)
        && is_complete(geo, index)
        && meta_of(index).is_some_and(|m| !has_extra_names(&m))
        && built_from(index).as_ref() == Some(&have);
    let origin = if reusable {
        Origin::Current
    } else {
        // Held, so a stage here is a dead build's, with any files a publish it did not finish
        // replaced still aside (which a build refuses to start over). The META_FILE goes first:
        // until a build over them completes, the files there are no index of these files, even
        // once the stage that showed a publish unfinished is gone.
        let meta_file = index.join(META_FILE);
        match std::fs::remove_file(&meta_file) {
            Err(e) if e.kind() != ErrorKind::NotFound => return Err(format!("{}: {e}", meta_file.display())),
            _ => {}
        }
        let _ = std::fs::remove_dir_all(index.join(BUILD_STAGE_DIR));
        let opts = BuildOpts { threads: on_device.threads, mem_mb: on_device.mem_mb, ..BuildOpts::new(index.join("tmp")) };
        let meta = index_meta(geo, &opts, &have);
        build_held(geo, &held_subs(geo, SUB_INDEXES), &held, &opts, || Ok(meta.into()))?;
        Origin::Built
    };
    // Only saves a later open the hashing; a read-only index dir is fine.
    let _ = hashes.save(&cache);
    // Mapped while held: files a later build moves in are new ones, these stay as mapped.
    let opened = SearchIndex::open(geo, index, None, None)?;
    drop(held);
    Ok((opened, origin))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::build_all;
    use crate::fixture::{Geo, TempDir};
    use crate::query::Searcher;
    use query_server::{DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL};
    use std::path::PathBuf;

    /// The fingerprint of the dataset in `dir`, every file hashed.
    fn fingerprint(dir: &Path) -> Result<Fingerprint, String> {
        HashCache::default().fingerprint(dir, &Stamps::of(dir).map_err(|e| e.to_string())?).map_err(|e| e.to_string())
    }

    /// A country and two places named `names`.
    fn dataset(names: [&str; 2]) -> Geo {
        let mut g = Geo::default();
        g.boundary("Testland", 2, [10.0, 20.0, 11.0, 21.0]);
        for (i, name) in names.into_iter().enumerate() {
            g.place(name, 1, 10.2 + i as f32 * 0.5, 20.5);
        }
        g
    }

    /// The dataset in `dir` loaded, with its stamps, as the binding loads it.
    fn load_dataset(dir: &Path) -> (Index, Stamps) {
        load(dir, || Index::load(dir.to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE)).unwrap()
    }

    /// The index of the dataset in `ci` built into `out` as `place-search build` builds one to ship.
    fn ship(ci: &Path, out: &Path) {
        let (geo, _) = load_dataset(ci);
        let opts = BuildOpts::new(ci.join("tmp"));
        let meta = index_meta(&geo, &opts, &fingerprint(ci).unwrap());
        build_all(&geo, &held_subs(&geo, SUB_INDEXES), out, &opts, || Ok(meta.into())).unwrap();
    }

    /// `dataset(names)` with one POI named `poi`.
    fn with_poi(names: [&str; 2], poi: &str) -> Geo {
        let mut g = dataset(names);
        g.poi(poi, 10.4, 20.6, 10);
        g
    }

    /// `place-search build --sub` of the sub-indexes `subs` of the dataset in `dir` into `index`.
    fn build_subs(dir: &Path, index: &Path, subs: &[&str]) -> Result<(), String> {
        let (geo, stamps) = load_dataset(dir);
        let opts = BuildOpts::new(dir.join("tmp"));
        build_all(&geo, subs, index, &opts, || merged_meta(&geo, &stamps, &opts, dir, index, subs)).map(drop)
    }

    /// The names `idx` finds for `q`.
    fn names(idx: &SearchIndex, q: &str) -> Vec<String> {
        Searcher::new(idx).search(q, 5).0.into_iter().map(|f| f.name).collect()
    }

    /// The index files in `dir` (not its META_FILE, which holds build times, nor the hash cache).
    fn files_of(dir: &Path) -> BTreeMap<String, Vec<u8>> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.is_file() && ![META_FILE, HASH_CACHE_FILE].contains(&p.file_name().unwrap().to_str().unwrap()))
            .map(|p| (p.file_name().unwrap().to_string_lossy().into_owned(), std::fs::read(&p).unwrap()))
            .collect()
    }

    /// Copies the files of `from` into `to`, as a client downloads a shipped index.
    fn download(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for e in std::fs::read_dir(from).unwrap() {
            let p = e.unwrap().path();
            std::fs::copy(&p, to.join(p.file_name().unwrap())).unwrap();
        }
    }

    #[test]
    fn an_absent_index_is_built_from_the_dataset() {
        let ds = TempDir::new("absent");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let (geo, stamps) = load_dataset(&ds.0);
        let index = ds.0.join("search");

        let (idx, origin) = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap();

        assert_eq!(origin, Origin::Built);
        assert_eq!(built_from(&index), Some(fingerprint(&ds.0).unwrap()));
        assert_eq!(names(&idx, "testville"), vec!["Testville"]);
        let left: Vec<PathBuf> = std::fs::read_dir(&index).unwrap().map(|e| e.unwrap().path()).filter(|p| p.is_dir()).collect();
        assert!(left.is_empty(), "{left:?}");
    }

    #[test]
    fn a_device_build_is_the_shipped_index() {
        let (ci, ds) = (TempDir::new("ci-same"), TempDir::new("device"));
        dataset(["Testville", "Otherton"]).write(&ci.0);
        dataset(["Testville", "Otherton"]).write(&ds.0);
        ship(&ci.0, &ci.0.join("out"));
        let (geo, stamps) = load_dataset(&ds.0);

        open(&geo, &stamps, &ds.0, &ds.0.join("search"), &BuildOnDevice::default()).unwrap();

        assert_eq!(files_of(&ds.0.join("search")), files_of(&ci.0.join("out")));
        assert_eq!(built_from(&ds.0.join("search")), built_from(&ci.0.join("out")));
    }

    #[test]
    fn an_index_built_from_these_files_is_kept() {
        let ds = TempDir::new("current");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let (geo, stamps) = load_dataset(&ds.0);
        let index = ds.0.join("search");
        drop(open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap());
        let built = std::fs::metadata(index.join("places.fst")).unwrap();

        let (_, origin) = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap();

        assert_eq!(origin, Origin::Current);
        assert_eq!(Stamp::of(&std::fs::metadata(index.join("places.fst")).unwrap()), Stamp::of(&built));
    }

    #[test]
    fn a_shipped_index_of_the_same_files_is_kept() {
        let (ci, ds) = (TempDir::new("ci"), TempDir::new("shipped"));
        dataset(["Testville", "Otherton"]).write(&ci.0);
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let shipped = ci.0.join("out");
        ship(&ci.0, &shipped);
        let index = ds.0.join("search");
        download(&shipped, &index);
        let (geo, stamps) = load_dataset(&ds.0);

        let (idx, origin) = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap();

        assert_eq!(origin, Origin::Current);
        assert_eq!(std::fs::read(index.join(META_FILE)).unwrap(), std::fs::read(shipped.join(META_FILE)).unwrap());
        assert_eq!(names(&idx, "otherton"), vec!["Otherton"]);
    }

    #[test]
    fn an_index_of_other_files_is_rebuilt() {
        let (ci, ds) = (TempDir::new("ci-stale"), TempDir::new("stale"));
        dataset(["Testville", "Otherton"]).write(&ci.0);
        let shipped = ci.0.join("out");
        ship(&ci.0, &shipped);
        // The next day's dataset: one place renamed, every file the same size.
        dataset(["Nestville", "Otherton"]).write(&ds.0);
        let index = ds.0.join("search");
        download(&shipped, &index);
        let (geo, stamps) = load_dataset(&ds.0);

        let (idx, origin) = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap();

        assert_eq!(origin, Origin::Built);
        assert_eq!(built_from(&index), Some(fingerprint(&ds.0).unwrap()));
        assert_eq!(names(&idx, "nestville"), vec!["Nestville"]);
        assert!(names(&idx, "testville").is_empty());
        assert!(!index.join(BUILD_STAGE_DIR).exists());
    }

    #[test]
    fn a_build_cut_off_while_moving_its_files_in_is_built_again() {
        let (ci, ds) = (TempDir::new("ci-cut"), TempDir::new("cut"));
        dataset(["Testville", "Otherton"]).write(&ci.0);
        let shipped = ci.0.join("out");
        ship(&ci.0, &shipped);
        dataset(["Nestville", "Otherton"]).write(&ds.0);
        let index = ds.0.join("search");
        download(&shipped, &index);
        // Killed while publishing: a file it replaced still aside in its stage.
        let aside = index.join(BUILD_STAGE_DIR).join(BUILD_ASIDE_DIR);
        std::fs::create_dir_all(&aside).unwrap();
        std::fs::copy(index.join("places.fst"), aside.join("places.fst")).unwrap();
        let (geo, stamps) = load_dataset(&ds.0);

        let (idx, origin) = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap();

        assert_eq!(origin, Origin::Built);
        assert_eq!(names(&idx, "nestville"), vec!["Nestville"]);
        assert!(!index.join(BUILD_STAGE_DIR).exists());
    }

    /// Over the index `open` built in `index` of the dataset in `dir` (loaded as `geo`): a
    /// `place-search build --no-inline` of the same files, killed between moving in its
    /// places.fst and its places.post. The old places.post and META_FILE are still in place.
    fn cut_off_publish(geo: &Index, dir: &Path, index: &Path) {
        let stage = index.join(BUILD_STAGE_DIR);
        let opts = BuildOpts { inline: false, ..BuildOpts::new(dir.join("tmp")) };
        let meta = index_meta(geo, &opts, &fingerprint(dir).unwrap());
        build_all(geo, &held_subs(geo, SUB_INDEXES), &stage, &opts, || Ok(meta.into())).unwrap();
        let aside = stage.join(BUILD_ASIDE_DIR);
        std::fs::create_dir(&aside).unwrap();
        for f in ["places.bnd", "places.fst"] {
            std::fs::rename(index.join(f), aside.join(f)).unwrap();
            std::fs::rename(stage.join(f), index.join(f)).unwrap();
        }
    }

    #[test]
    fn a_publish_cut_off_over_an_index_of_the_same_files_is_built_again() {
        let ds = TempDir::new("cut-same");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let (geo, stamps) = load_dataset(&ds.0);
        let index = ds.0.join("search");
        drop(open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap());
        cut_off_publish(&geo, &ds.0, &index);

        let (idx, origin) = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap();

        assert_eq!(origin, Origin::Built);
        assert_eq!(names(&idx, "testville"), vec!["Testville"]);
        assert!(!index.join(BUILD_STAGE_DIR).exists());
    }

    #[test]
    fn a_publish_cut_off_stays_unread_after_a_failed_build_over_it() {
        let ds = TempDir::new("cut-failed");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let (geo, stamps) = load_dataset(&ds.0);
        let index = ds.0.join("search");
        drop(open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap());
        cut_off_publish(&geo, &ds.0, &index);
        // A sort budget no build can run with: the build over the cut-off publish fails.
        let failing = BuildOnDevice { threads: 1, mem_mb: 0 };
        assert!(open(&geo, &stamps, &ds.0, &index, &failing).is_err());

        let (idx, origin) = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap();

        assert_eq!(origin, Origin::Built);
        assert_eq!(names(&idx, "testville"), vec!["Testville"]);
    }

    #[test]
    fn a_patch_of_its_own_dataset_rebuilds_the_index() {
        let ds = TempDir::new("patched");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let index = ds.0.join("search");
        let (geo, stamps) = load_dataset(&ds.0);
        drop(open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap());
        dataset(["Nestville", "Otherton"]).write(&ds.0);
        let (geo, stamps) = load_dataset(&ds.0);

        let (idx, origin) = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap();

        assert_eq!(origin, Origin::Built);
        assert_eq!(names(&idx, "nestville"), vec!["Nestville"]);
    }

    #[test]
    fn files_replaced_since_the_load_are_refused() {
        let ds = TempDir::new("replaced");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let index = ds.0.join("search");
        let (geo, stamps) = load_dataset(&ds.0);
        drop(open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap());
        // Patched under the loaded index: an index built now would hold its old names.
        dataset(["Nestville", "Otherton"]).write(&ds.0);

        let refused = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default());

        assert!(refused.err().unwrap().contains("changed since it was loaded"));
        let (geo, stamps) = load_dataset(&ds.0);
        let (idx, origin) = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap();
        assert_eq!(origin, Origin::Built);
        assert_eq!(names(&idx, "nestville"), vec!["Nestville"]);
    }

    #[test]
    fn a_build_of_files_replaced_since_the_load_is_refused() {
        let ds = TempDir::new("replaced-build");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let index = ds.0.join("search");
        let (geo, stamps) = load_dataset(&ds.0);
        // Patched between the load and the META_FILE: the index would hold the old names.
        dataset(["Nestville", "Otherton"]).write(&ds.0);
        let (opts, subs) = (BuildOpts::new(ds.0.join("tmp")), held_subs(&geo, SUB_INDEXES));

        let refused = build_all(&geo, &subs, &index, &opts, || merged_meta(&geo, &stamps, &opts, &ds.0, &index, &subs));

        assert!(refused.unwrap_err().contains("changed since it was loaded"));
        assert!(!index.join(META_FILE).exists());
    }

    #[test]
    fn opens_of_one_index_take_turns() {
        let ds = TempDir::new("turns");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let (geo, stamps) = load_dataset(&ds.0);
        let index = ds.0.join("search");

        let mut origins: Vec<Origin> = std::thread::scope(|s| {
            let opens: Vec<_> = (0..2).map(|_| s.spawn(|| open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).map(|(_, o)| o))).collect();
            opens.into_iter().map(|h| h.join().unwrap().unwrap()).collect()
        });

        origins.sort_by_key(|o| *o == Origin::Current);
        assert_eq!(origins, [Origin::Built, Origin::Current]);
    }

    #[test]
    fn an_open_waits_for_a_build_of_its_index() {
        // A stale index and one built from these files: neither is read while a build holds it.
        let cases = [("stale", ["Testville", "Otherton"], Origin::Built), ("current", ["Nestville", "Otherton"], Origin::Current)];
        for (case, shipped_names, origin) in cases {
            let (ci, ds) = (TempDir::new("ci-held"), TempDir::new("held"));
            dataset(shipped_names).write(&ci.0);
            ship(&ci.0, &ci.0.join("out"));
            dataset(["Nestville", "Otherton"]).write(&ds.0);
            let index = ds.0.join("search");
            download(&ci.0.join("out"), &index);
            // `place-search build` into the index: it holds the directory and has staged a file.
            let build = File::open(&index).unwrap();
            build.try_lock().unwrap();
            let staged = index.join(BUILD_STAGE_DIR).join("places.fst");
            std::fs::create_dir_all(staged.parent().unwrap()).unwrap();
            std::fs::write(&staged, "staged by the build").unwrap();
            let (geo, stamps) = load_dataset(&ds.0);

            std::thread::scope(|s| {
                let opening = s.spawn(|| open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).map(|(_, o)| o));
                std::thread::sleep(std::time::Duration::from_millis(300));

                assert!(!opening.is_finished(), "{case}");
                assert_eq!(std::fs::read_to_string(&staged).ok().as_deref(), Some("staged by the build"), "{case}");
                drop(build);
                assert_eq!(opening.join().unwrap(), Ok(origin), "{case}");
            });
        }
    }

    #[test]
    fn a_partial_build_over_an_index_of_other_files_is_refused() {
        for &kept in SUB_INDEXES {
            let ds = TempDir::new("partial");
            let index = ds.0.join("search");
            with_poi(["Testville", "Otherton"], "Old Mill").write(&ds.0);
            build_subs(&ds.0, &index, SUB_INDEXES).unwrap();
            with_poi(["Nestville", "Otherton"], "New Mill").write(&ds.0);
            let building: Vec<&str> = SUB_INDEXES.iter().copied().filter(|&s| s != kept).collect();

            let refused = build_subs(&ds.0, &index, &building);

            assert!(refused.as_ref().is_err_and(|e| e.contains(&format!("retained {kept} sub-index"))), "{kept}: {refused:?}");
            let (geo, stamps) = load_dataset(&ds.0);
            let (idx, origin) = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap();
            assert_eq!(origin, Origin::Built, "{kept}");
            assert_eq!((names(&idx, "nestville"), names(&idx, "new mill")), (vec!["Nestville".to_string()], vec!["New Mill".to_string()]), "{kept}");
        }
    }

    #[test]
    fn a_partial_build_over_an_index_of_the_same_files_keeps_the_rest() {
        let ds = TempDir::new("partial-same");
        let index = ds.0.join("search");
        with_poi(["Testville", "Otherton"], "Old Mill").write(&ds.0);
        build_subs(&ds.0, &index, SUB_INDEXES).unwrap();
        let poi = Stamp::of(&std::fs::metadata(index.join("poi.fst")).unwrap());

        build_subs(&ds.0, &index, &["places"]).unwrap();

        let (geo, stamps) = load_dataset(&ds.0);
        let (idx, origin) = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap();
        assert_eq!(origin, Origin::Current);
        assert_eq!(Stamp::of(&std::fs::metadata(index.join("poi.fst")).unwrap()), poi);
        assert_eq!((names(&idx, "testville"), names(&idx, "old mill")), (vec!["Testville".to_string()], vec!["Old Mill".to_string()]));
    }

    #[test]
    fn an_index_of_some_sub_indexes_is_built_whole() {
        // `place-search build --sub` into an empty directory records the whole dataset.
        for &sub in SUB_INDEXES {
            let ds = TempDir::new("some-subs");
            let index = ds.0.join("search");
            with_poi(["Testville", "Otherton"], "Old Mill").write(&ds.0);
            build_subs(&ds.0, &index, &[sub]).unwrap();
            let (geo, stamps) = load_dataset(&ds.0);

            let opened = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default());

            let (idx, origin) = opened.unwrap_or_else(|e| panic!("{sub}: {e}"));
            assert_eq!(origin, Origin::Built, "{sub}");
            assert_eq!((names(&idx, "testville"), names(&idx, "old mill")), (vec!["Testville".to_string()], vec!["Old Mill".to_string()]), "{sub}");
        }
    }

    #[test]
    fn an_index_missing_a_file_is_built_again() {
        for file in SUB_INDEXES.iter().copied().flat_map(sub_index_files).chain([BOUNDARY_FILE.to_string()]) {
            let ds = TempDir::new("missing-file");
            let index = ds.0.join("search");
            with_poi(["Testville", "Otherton"], "Old Mill").write(&ds.0);
            build_subs(&ds.0, &index, SUB_INDEXES).unwrap();
            // A download cut short, or a file removed by hand.
            std::fs::remove_file(index.join(&file)).unwrap();
            let (geo, stamps) = load_dataset(&ds.0);

            let opened = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default());

            let (idx, origin) = opened.unwrap_or_else(|e| panic!("{file}: {e}"));
            assert_eq!(origin, Origin::Built, "{file}");
            assert_eq!((names(&idx, "testville"), names(&idx, "old mill")), (vec!["Testville".to_string()], vec!["Old Mill".to_string()]), "{file}");
        }
    }

    #[test]
    fn an_index_built_with_extra_names_is_built_again_without_them() {
        let ds = TempDir::new("extras");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let index = ds.0.join("search");
        // `place-search build --extras` of these files: read only with an extra-names file.
        let (tsv, bin) = (ds.0.join("extra_names.tsv"), ds.0.join("extra_names.bin"));
        std::fs::write(&tsv, "p\t0\t2\tname:de\tPrüfdorf\n").unwrap();
        crate::extras::pack(tsv.to_str().unwrap(), bin.to_str().unwrap()).unwrap();
        let (geo, stamps) = load_dataset(&ds.0);
        let opts = BuildOpts { extras: Some(bin.to_str().unwrap().into()), ..BuildOpts::new(ds.0.join("tmp")) };
        let meta = index_meta(&geo, &opts, &fingerprint(&ds.0).unwrap());
        build_all(&geo, &held_subs(&geo, SUB_INDEXES), &index, &opts, || Ok(meta.into())).unwrap();

        let (idx, origin) = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap();

        assert_eq!(origin, Origin::Built);
        assert_eq!((names(&idx, "testville"), names(&idx, "prüfdorf")), (vec!["Testville".to_string()], vec![]));
        drop(idx);
        assert_eq!(open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap().1, Origin::Current);
    }

    #[test]
    fn load_refuses_files_replaced_while_loading() {
        let ds = TempDir::new("loading");
        dataset(["Testville", "Otherton"]).write(&ds.0);

        let refused = load(&ds.0, || {
            dataset(["Nestville", "Otherton"]).write(&ds.0);
            Ok(())
        });

        assert!(refused.unwrap_err().contains("changed while it was loading"));
    }

    #[test]
    fn fingerprint_hashes_only_files_whose_stat_changed() {
        let ds = TempDir::new("cache");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let mut sut = HashCache::default();
        let first = sut.fingerprint(&ds.0, &Stamps::of(&ds.0).unwrap()).unwrap();
        // A hash the cache must reuse, not recompute, while place_nodes.bin keeps its stat.
        sut.0.get_mut("place_nodes.bin").unwrap().1 = "kept".into();
        let path = ds.0.join(HASH_CACHE_FILE);
        sut.save(&path).unwrap();
        let mut sut = HashCache::load(&path);

        let kept = sut.fingerprint(&ds.0, &Stamps::of(&ds.0).unwrap()).unwrap();
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let rehashed = sut.fingerprint(&ds.0, &Stamps::of(&ds.0).unwrap()).unwrap();

        assert_eq!(kept["place_nodes.bin"], "kept");
        assert_eq!(rehashed, first);
    }

    #[test]
    fn fingerprint_names_the_files_present() {
        let ds = TempDir::new("present");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let mut sut = HashCache::default();
        let without = sut.fingerprint(&ds.0, &Stamps::of(&ds.0).unwrap()).unwrap();
        std::fs::write(ds.0.join("strings_poi.bin"), b"\0").unwrap();

        let with = sut.fingerprint(&ds.0, &Stamps::of(&ds.0).unwrap()).unwrap();

        assert!(!without.contains_key("strings_poi.bin"));
        assert_eq!(with.len(), without.len() + 1);
        assert_eq!(with["strings_poi.bin"], format!("{:x}", Sha256::digest(b"\0")));
    }
}
