//! The dataset an index belongs to. Postings point into the geocoder files by record index and
//! string offset, so an index answers right only beside the exact files it was built from. Its
//! META_FILE records the sha256 of each of them, and `open` uses an index only while the files
//! beside it still hash the same: a shipped index for another day, region or tier, or one left
//! from before a patch, is built again from the files instead of being read. The files hashed
//! must be the ones the geocoder index was loaded from (`load`): one replaced under a loaded
//! index would have the index built from the old files but recorded as built from the new.
use crate::build::{
    self, build_held, has_extra_names, has_sub_index, has_unfinished_publish, held_subs, is_current_format, sub_index_files, BuildOpts,
    Contended, HeldIndexDir,
};
use crate::constants::*;
use crate::profile::{MatchMode, Profile, RequestError, SearchRequest};
use crate::query::{Found, QueryStats, SearchCandidate, Searcher};
use crate::reader::SearchIndex;
use query_server::{lower_hex, stamps_of, FileStamp, Index, WATER_DIR, WATER_FILES};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{ErrorKind, Read};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

pub use query_server::{Stamps, DATASET_FILES};

pub(crate) fn core_stamps(dir: &Path) -> std::io::Result<Stamps> {
    let mut stamps = Stamps::of(dir)?;
    stamps.0.extend(stamps_of(dir, &CHAIN_INPUTS)?);
    Ok(stamps)
}

/// Search-only files. Opening reverse data never reads a bundle or importance column.
#[derive(Default)]
pub struct SearchFiles {
    tiers: [Option<(u32, query_server::strings::Tier)>; 2],
    tags: [Option<query_server::side_blocks::SideBlocks>; 3],
    wiki: [Option<query_server::side_blocks::SideBlocks>; 3],
}

impl SearchFiles {
    pub fn open(dir: &Path, geo: &Index) -> Result<Self, String> {
        Self::with_access(dir, geo, false)
    }

    pub(crate) fn with_access(dir: &Path, geo: &Index, sequential: bool) -> Result<Self, String> {
        let mut files = Self::default();
        let counts =
            [geo.place_node_count(), geo.admin_polygon_count(), geo.poi_records.as_ref().map_or(0, |p| p.len())];
        let optional = |name: &str| {
            let path = dir.join(name);
            if !path.try_exists().map_err(|e| e.to_string())? {
                return Ok(None);
            }
            let bytes = query_server::mmap_file(path.to_str().ok_or("invalid dataset path")?)?;
            if sequential {
                bytes.advise_in_order(true);
            }
            Ok::<_, String>(Some(bytes))
        };
        for (i, name) in [PLACE_TAGS_FILE, ADMIN_TAGS_FILE, POI_TAGS_FILE].into_iter().enumerate() {
            for (file, col) in [(name, &mut files.tags[i]), (IMPORTANCE_FILES[i], &mut files.wiki[i])] {
                if let Some(bytes) = optional(file)? {
                    let side = query_server::side_blocks::SideBlocks::open(bytes, file)?;
                    if side.count() != counts[i] { return Err(format!("{file} has {} rows, expected {}", side.count(), counts[i])); }
                    *col = Some(side);
                }
            }
        }
        for (i, name) in TAG_TIERS.into_iter().enumerate() {
            if let Some(bytes) = optional(name)? {
                let layout: Value = serde_json::from_reader(File::open(dir.join("strings_layout.json")).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
                let entry = layout["tiers"].as_array().and_then(|ts| ts.iter().find(|t| t["file"] == name))
                    .ok_or_else(|| format!("missing layout for {name}"))?;
                let bounds = |field: &str| entry[field].as_u64().and_then(|u| u32::try_from(u).ok()).ok_or_else(|| format!("bad {field} for {name}"));
                let tier = query_server::strings::Tier::open(bytes)?;
                let base = bounds("start")?;
                if tier.len() > bounds("end")?.checked_sub(base).ok_or("reversed tier bounds")? {
                    return Err(format!("{name} exceeds its layout"));
                }
                files.tiers[i] = Some((base, tier));
            }
        }
        Ok(files)
    }

    /// Place rows, admin slots and POI rows respectively.
    pub fn bundle(&self, kind: crate::model::Kind, row: u32) -> Option<crate::tags::TagBundle<'_>> {
        let k = Self::column(kind);
        let offset = self.bundle_id(kind, row)?;
        let (base, tier) = self.tiers[usize::from(k == 2)].as_ref()?;
        crate::tags::TagBundle::parse(tier.get(offset.checked_sub(*base)?))
    }

    pub(crate) fn bundle_id(&self, kind: crate::model::Kind, row: u32) -> Option<u32> {
        self.tags[Self::column(kind)].as_ref()?.get(row).filter(|&v| v != query_server::NO_DATA)
    }

    pub fn wiki_importance(&self, kind: crate::model::Kind, row: u32) -> Option<u32> {
        self.wiki[Self::column(kind)].as_ref()?.get(row).filter(|&v| v != 0)
    }

    fn column(kind: crate::model::Kind) -> usize {
        use crate::model::Kind;
        match kind { Kind::Place | Kind::Minor => 0, Kind::Area | Kind::Linked => 1, Kind::Poi => 2 }
    }
}

/// Opens a host column only for the full profile that consumes it.
pub fn host_tags(path: &Path) -> Result<crate::tags::TagBlob, String> {
    crate::tags::TagBlob::open(query_server::mmap_file(path.to_str().ok_or("invalid host file path")?)?)
}

/// The sha256 (hex) of each dataset file present, by name.
pub type Fingerprint = BTreeMap<String, String>;

/// Runs `load` (which loads the geocoder index of the dataset in `dir`) and returns what it
/// loaded with the stamps of the dataset files it read: those and the files of the dataset's
/// water dir (WATER_FILES, which the geocoder index loads and an index of place search derives
/// nothing from) the same before and after the load, else the files were being replaced and it
/// fails. A dir of continent dirs (one holding no admin_cells.bin, which MultiIndex::load
/// loads continent by continent) has each continent dir's dataset and water files checked so
/// too, a continent whose files can't be looked up taken as such (MultiIndex::load skips it,
/// and loads the others); the stamps returned are still those of `dir`'s own.
pub fn load<T>(dir: &Path, load: impl FnOnce() -> Result<T, String>) -> Result<(T, Stamps), String> {
    let continents: Vec<std::path::PathBuf> = if dir.join("admin_cells.bin").exists() {
        Vec::new()
    } else {
        query_server::continents().iter().map(|c| dir.join(c.name)).filter(|d| d.is_dir()).collect()
    };
    let of = |d: &Path| Ok::<_, std::io::Error>((core_stamps(d)?, stamps_of(&d.join(WATER_DIR), &WATER_FILES)?));
    let stamps = || {
        let own = of(dir).map_err(|e| e.to_string())?;
        Ok::<_, String>((own, continents.iter().map(|d| of(d).ok()).collect::<Vec<_>>()))
    };
    let before = stamps()?;
    let loaded = load()?;
    if stamps()? != before {
        return Err(format!("The dataset in {} changed while it was loading", dir.display()));
    }
    let ((own, _), _) = before;
    Ok((loaded, own))
}

/// Hashes of dataset files, each with the stat it was taken at; kept beside an index
/// (HASH_CACHE_FILE) so opening it does not read the dataset again.
#[derive(Default)]
pub struct HashCache(BTreeMap<String, (FileStamp, String)>);

impl HashCache {
    /// The cache at `path`; empty when it is missing or unreadable.
    pub fn load(path: &Path) -> HashCache {
        let Ok(text) = std::fs::read_to_string(path) else { return HashCache::default() };
        let Ok(Value::Object(files)) = serde_json::from_str::<Value>(&text) else { return HashCache::default() };
        let entry = |v: &Value| Some((FileStamp::from_json(v)?, v["sha256"].as_str()?.to_string()));
        HashCache(files.iter().filter_map(|(name, v)| Some((name.clone(), entry(v)?))).collect())
    }

    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let files: serde_json::Map<String, Value> = self
            .0
            .iter()
            .map(|(name, (stamp, sha))| {
                let mut entry = stamp.to_json();
                entry["sha256"] = json!(sha);
                (name.clone(), entry)
            })
            .collect();
        std::fs::write(path, serde_json::to_string_pretty(&Value::Object(files)).unwrap())
    }

    /// The fingerprint of the dataset in `dir`, its files stamped `stamps`: only those whose
    /// stamp the cache has not seen are hashed.
    pub fn fingerprint(&mut self, dir: &Path, stamps: &Stamps) -> std::io::Result<Fingerprint> {
        let mut inputs = stamps.clone();
        inputs.0.retain(|name, _| DATASET_FILES.contains(&name.as_str()));
        self.fingerprint_inputs(dir, &inputs)
    }

    fn fingerprint_inputs(&mut self, dir: &Path, stamps: &Stamps) -> std::io::Result<Fingerprint> {
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
    Ok(lower_hex(&sha.finalize()))
}

/// The fingerprint of the dataset in `dir` as loaded, its files stamped `loaded` (`load`), the
/// hashes `hashes` holds reused: refused when they no longer have those stamps, before the
/// hashing or after it, as the files hashed would not be the ones loaded.
fn fingerprint_loaded(dir: &Path, loaded: &Stamps, hashes: &mut HashCache) -> Result<Fingerprint, String> {
    let mut base = loaded.clone();
    base.0.retain(|name, _| DATASET_FILES.contains(&name.as_str()));
    checked_fingerprint(dir, &base, Stamps::of, || hashes.fingerprint_inputs(dir, &base))
}

pub(crate) fn core_fingerprint(dir: &Path, loaded: &Stamps, hashes: &mut HashCache) -> Result<Fingerprint, String> {
    checked_fingerprint(dir, loaded, core_stamps, || hashes.fingerprint_inputs(dir, loaded))
}

fn checked_fingerprint(
    dir: &Path,
    loaded: &Stamps,
    stamps: fn(&Path) -> std::io::Result<Stamps>,
    fingerprint: impl FnOnce() -> std::io::Result<Fingerprint>,
) -> Result<Fingerprint, String> {
    let io = |e: std::io::Error| format!("{}: {e}", dir.display());
    let unchanged = || match stamps(dir).map_err(|e| e.to_string())? {
        now if now == *loaded => Ok(()),
        _ => Err(format!("The dataset in {} changed since it was loaded: load it again", dir.display())),
    };
    unchanged()?;
    let have = fingerprint().map_err(io)?;
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
/// (`fingerprint_loaded`), or when a sub-index it keeps was built from other files or in another
/// format, as the fingerprint and the format it records are those of every sub-index there.
pub fn merged_meta(geo: &Index, loaded: &Stamps, opts: &BuildOpts, dir: &Path, out: &Path, subs: &[&str]) -> Result<Value, String> {
    // An index never reads the POIs of a dataset without any, so those are not kept.
    let mut replacing = subs.to_vec();
    if geo.poi_records.is_none() {
        replacing.push("poi");
    }
    build::check_retained_names(out, &replacing, opts.extras.is_some())?;
    let dataset = fingerprint_loaded(dir, loaded, &mut HashCache::default())?;
    let kept = PLACES_SUB_INDEXES.iter().find(|s| !replacing.contains(s) && has_sub_index(out, s));
    if let Some(kept) = kept.filter(|_| built_from(out).as_ref() != Some(&dataset)) {
        return Err(format!("retained {kept} sub-index was built from other dataset files; rebuild with --sub all"));
    }
    if let Some(kept) = kept.filter(|_| !meta_of(out).is_some_and(|m| is_current_format(&m))) {
        return Err(format!("retained {kept} sub-index was built in another format; rebuild with --sub all"));
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
    let sub_files = held_subs(geo, PLACES_SUB_INDEXES).into_iter().flat_map(sub_index_files);
    sub_files.chain([BOUNDARY_FILE.to_string()]).all(|f| index.join(f).is_file())
}

/// Opens the place search index in `index` of the dataset in `dir` (loaded as `geo` from files
/// stamped `loaded`, by `load`): the one there when it is whole (`is_complete`), no publish into
/// it was left unfinished and it was built from these files in this build's format
/// (PLACES_INDEX_FORMAT) without extra names (it has no extra-names file to read them with), else one
/// built from them first by `build_held` with the default options (staged in `index`, every file
/// moved in only once all are complete). Fails when the files are no longer the ones loaded. The
/// open holds `index` (`HeldIndexDir`, which every build takes) until the index is mapped,
/// waiting while another open or a build has it: a second open finds the first's index current,
/// and no build moves files in while one is read, or has its stage cleared.
pub fn open<'g>(geo: &'g Index, loaded: &Stamps, dir: &Path, index: &Path, on_device: &BuildOnDevice) -> Result<(SearchIndex<'g>, Origin), String> {
    let held = HeldIndexDir::take(index, Contended::Wait)?;
    let cache = index.join(HASH_CACHE_FILE);
    let mut hashes = HashCache::load(&cache);
    let search_files = SearchFiles::open(dir, geo)?;
    let have = fingerprint_loaded(dir, loaded, &mut hashes)?;

    // Held, so an unfinished publish is a dead build's: some files there may be its own and some
    // the index's before it, whose META_FILE matches whenever both were built from these files.
    let reusable = !has_unfinished_publish(index)
        && is_complete(geo, index)
        && meta_of(index).is_some_and(|m| !has_extra_names(&m) && is_current_format(&m))
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
        build_held(geo, &held_subs(geo, PLACES_SUB_INDEXES), &held, &opts, || Ok(meta.into()))?;
        Origin::Built
    };
    // Only saves a later open the hashing; a read-only index dir is fine.
    let _ = hashes.save(&cache);
    // Mapped while held: files a later build moves in are new ones, these stay as mapped.
    let mut opened = SearchIndex::open(geo, index, None, None)?;
    opened.search_files = search_files;
    drop(held);
    Ok((opened, origin))
}

/// Checks a published core without rebuilding it. A query can only reuse the profile
/// and dataset whose entity ids the postings address.
pub fn read_core(dir: &Path, index: &Path, profile: Profile) -> Result<crate::index::CoreIndex, String> {
    let held = HeldIndexDir::take(index, Contended::Wait)?;
    let loaded = core_stamps(dir).map_err(|e| e.to_string())?;
    let cache = index.join(HASH_CACHE_FILE);
    let mut hashes = HashCache::load(&cache);
    let have = core_fingerprint(dir, &loaded, &mut hashes)?;
    if !meta_of(index).is_some_and(|m| m["profile"] == profile.as_str() && m["core"][META_DATASET_KEY] == json!(have)) {
        return Err(format!("{} does not match the requested dataset and profile", index.display()));
    }
    let core = crate::index::CoreIndex::open_held(index, &held).map_err(|e| e.to_string())?;
    let _ = hashes.save(&cache);
    Ok(core)
}

/// Opens or builds the requested core, keeping the dataset fingerprint, generation and
/// mappings together under the publish lock.
pub fn open_core(
    geo: &Index,
    loaded: &Stamps,
    dir: &Path,
    index: &Path,
    profile: Profile,
    on_device: &BuildOnDevice,
) -> Result<(crate::index::CoreIndex, Origin), String> {
    use crate::index::CoreIndex;
    let held = HeldIndexDir::take(index, Contended::Wait)?;
    let cache = index.join(HASH_CACHE_FILE);
    let mut hashes = HashCache::load(&cache);
    let have = core_fingerprint(dir, loaded, &mut hashes)?;
    let current = meta_of(index).is_some_and(|m| {
        m["profile"] == profile.as_str()
            && m["poi_max_tier"] == MAX_CORE_POI_TIER
            && m["core"][META_DATASET_KEY] == json!(have)
    });
    if current {
        if let Ok(core) = CoreIndex::open_held(index, &held) {
            let _ = hashes.save(&cache);
            return Ok((core, Origin::Current));
        }
    }
    let opts = BuildOpts { threads: on_device.threads, mem_mb: on_device.mem_mb, ..BuildOpts::new(index.join("tmp")) };
    build::build_core_held(geo, loaded, dir, &held, profile, &opts, &mut hashes)?;
    let core = CoreIndex::open_held(index, &held).map_err(|e| e.to_string())?;
    let _ = hashes.save(&cache);
    Ok((core, Origin::Built))
}

/// A place search `open` opened, held with the geocoder index its postings point into, so that it
/// can be kept and shared across threads: the node binding's and the test portal's.
pub struct PlaceSearch {
    // Fields drop in order: the searcher borrows the index, which borrows the geocoder index.
    searcher: Searcher<'static, 'static>,
    _index: Arc<SearchIndex<'static>>,
    _geo: Arc<Index>,
}

impl PlaceSearch {
    /// `open` of the index in `index` for `geo`, the dataset in `dir` loaded from files stamped
    /// `loaded`.
    pub fn open(geo: Arc<Index>, loaded: &Stamps, dir: &Path, index: &Path, on_device: &BuildOnDevice) -> Result<(PlaceSearch, Origin), String> {
        // SAFETY: the borrows point into the two Arcs' shared allocations, which never move and
        // are held by this struct until after the searcher and the index that borrow them drop.
        let geo_ref: &'static Index = unsafe { &*Arc::as_ptr(&geo) };
        let (opened, origin) = open(geo_ref, loaded, dir, index, on_device)?;
        let opened = Arc::new(opened);
        let index_ref: &'static SearchIndex<'static> = unsafe { &*Arc::as_ptr(&opened) };
        Ok((PlaceSearch { searcher: Searcher::new(index_ref), _index: opened, _geo: geo }, origin))
    }

    /// `Searcher::search` of the request's query and limit: the results, best first, and the work
    /// they took. They borrow this search, which they can't outlive. A request it can't honour
    /// (`require_supported`), or of a limit its profile refuses, is refused.
    pub fn search(&self, req: &SearchRequest) -> Result<(Vec<Found<'_>>, QueryStats), RequestError> {
        require_supported(req)?;
        req.require_limit()?;
        Ok(self.searcher.search(&req.q, req.limit))
    }

    pub(crate) fn candidates(&self, req: &SearchRequest) -> (Vec<SearchCandidate>, QueryStats) {
        self.searcher.candidates(&req.q, req.limit)
    }

    pub(crate) fn describe_candidate(&self, candidate: SearchCandidate, stats: &mut QueryStats) -> Found<'_> {
        let (lat, lng) = candidate.point;
        stats.reverse_lookups += 1;
        candidate.with_context(Rc::new(self.searcher.context_at(lat, lng)))
    }
}

/// Fails for a request this index can't honour: it answers the immich profile in photon mode,
/// deduplicated, from the query and the limit alone.
pub(crate) fn require_supported(req: &SearchRequest) -> Result<(), RequestError> {
    if req.profile != Profile::Immich {
        return Err(RequestError::ProfileNotBuilt(req.profile));
    }

    let options = [
        ("mode", req.mode != MatchMode::Photon),
        ("langs", !req.langs.is_empty()),
        ("bias", req.bias.is_some()),
        ("viewbox", req.viewbox.is_some()),
        ("countries", !req.countries.is_empty()),
        ("layers", req.layers.is_some()),
        ("categories", !req.categories.is_empty()),
        ("dedupe", !req.dedupe),
    ];
    match options.into_iter().find(|&(_, set)| set) {
        Some((option, _)) => Err(RequestError::UnsupportedOption(option)),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::build_all;
    use crate::fixture::{new_request, Geo, TempDir};
    use crate::index::entities::{BBox, Category};
    use crate::profile::{Bias, Layer, Viewbox};
    use query_server::{AdminPoint, ADMIN_GEOMETRY_FILES, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL};
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

    /// Hookcounty, a county whose ring is `outer`, (lat, lng) in tenths of a degree from (10, 20).
    fn county(outer: &[(f32, f32)]) -> Geo {
        let ring: Vec<(f32, f32)> = outer.iter().map(|&(lat, lng)| (10.0 + lat / 10.0, 20.0 + lng / 10.0)).collect();
        let mut g = Geo::default();
        g.shaped_boundary("Hookcounty", 6, &ring, &[]);
        g
    }

    /// Where the index `idx` has Hookcounty's label point.
    fn label_point(idx: &SearchIndex) -> AdminPoint {
        let found = Searcher::new(idx).search("Hookcounty", 5).0;
        assert_eq!(found.len(), 1);
        AdminPoint::new(found[0].lat as f64, found[0].lng as f64)
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
        build_all(&geo, &held_subs(&geo, PLACES_SUB_INDEXES), out, &opts, || Ok(meta.into())).unwrap();
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
        assert_eq!(FileStamp::of(&std::fs::metadata(index.join("places.fst")).unwrap()), FileStamp::of(&built));
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

    /// Sets the META_FILE field `key` of the index in `index` to `value` (removes it for None).
    fn set_meta(index: &Path, key: &str, value: Option<Value>) {
        let mut meta = meta_of(index).unwrap();
        match value {
            Some(v) => meta[key] = v,
            None => drop(meta.as_object_mut().unwrap().remove(key)),
        }
        std::fs::write(index.join(META_FILE), meta.to_string()).unwrap();
    }

    #[test]
    fn a_shipped_index_of_another_format_is_built_again() {
        for (case, format) in [("format 1", Some(json!(1))), ("no format", None)] {
            let (ci, ds) = (TempDir::new("ci-format"), TempDir::new("format"));
            dataset(["Testville", "Otherton"]).write(&ci.0);
            dataset(["Testville", "Otherton"]).write(&ds.0);
            let shipped = ci.0.join("out");
            ship(&ci.0, &shipped);
            let index = ds.0.join("search");
            download(&shipped, &index);
            set_meta(&index, "format", format);
            let (geo, stamps) = load_dataset(&ds.0);

            let (idx, origin) = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap();

            assert_eq!(origin, Origin::Built, "{case}");
            assert_eq!(meta_of(&index).unwrap()["format"], meta_of(&shipped).unwrap()["format"], "{case}");
            assert_eq!(names(&idx, "otherton"), vec!["Otherton"], "{case}");
        }
    }

    #[test]
    fn a_partial_build_over_an_index_of_another_format_is_refused() {
        for &kept in PLACES_SUB_INDEXES {
            let ds = TempDir::new("partial-format");
            let index = ds.0.join("search");
            with_poi(["Testville", "Otherton"], "Old Mill").write(&ds.0);
            build_subs(&ds.0, &index, PLACES_SUB_INDEXES).unwrap();
            set_meta(&index, "format", Some(json!(1)));
            let building: Vec<&str> = PLACES_SUB_INDEXES.iter().copied().filter(|&s| s != kept).collect();

            let refused = build_subs(&ds.0, &index, &building);

            assert!(refused.as_ref().is_err_and(|e| e.contains(&format!("retained {kept} sub-index"))), "{kept}: {refused:?}");
            build_subs(&ds.0, &index, PLACES_SUB_INDEXES).unwrap();
            let (geo, stamps) = load_dataset(&ds.0);
            assert_eq!(open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap().1, Origin::Current, "{kept}");
        }
    }

    #[test]
    fn an_index_shipped_beside_one_boundary_tier_is_rebuilt_beside_the_other() {
        // One county in two tiers, which share its record and ring and differ in their arcs: the
        // simplified one is a square, whose label point is its middle, where the exact one is open
        // to the north.
        let square = [(0.0, 0.0), (0.0, 3.0), (3.0, 3.0), (3.0, 0.0), (0.0, 0.0)];
        let hook = [(0.0, 0.0), (0.0, 3.0), (3.0, 3.0), (3.0, 2.0), (1.0, 2.0), (1.0, 1.0), (3.0, 1.0), (3.0, 0.0), (0.0, 0.0)];
        let (ci, ds, arcs) = (TempDir::new("ci-tier"), TempDir::new("other-tier"), TempDir::new("tier-arcs"));
        county(&square).write(&ci.0);
        county(&square).write(&ds.0);
        county(&hook).write(&arcs.0);
        std::fs::copy(arcs.0.join("arc_vertices.bin"), ds.0.join("arc_vertices.bin")).unwrap();
        let shipped = ci.0.join("out");
        ship(&ci.0, &shipped);
        let index = ds.0.join("search");
        download(&shipped, &index);
        let (geo, stamps) = load_dataset(&ds.0);
        let ci_geo = load_dataset(&ci.0).0;
        let hook_county = geo.admin_polygon(0).unwrap();
        let shipped_label = label_point(&SearchIndex::open(&ci_geo, &shipped, None, None).unwrap());
        assert!(!geo.admin_geometry.contains_once(&hook_county, shipped_label));

        let (idx, origin) = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap();

        assert_eq!(origin, Origin::Built);
        assert!(geo.admin_geometry.contains_once(&hook_county, label_point(&idx)));
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
    /// `place-search build --area-proxy` of the same files, killed between moving in its
    /// places.fst and its places.post. The old places.post and META_FILE are still in place.
    fn cut_off_publish(geo: &Index, dir: &Path, index: &Path) {
        let stage = index.join(BUILD_STAGE_DIR);
        let opts = BuildOpts { area_proxy: true, ..BuildOpts::new(dir.join("tmp")) };
        let meta = index_meta(geo, &opts, &fingerprint(dir).unwrap());
        build_all(geo, &held_subs(geo, PLACES_SUB_INDEXES), &stage, &opts, || Ok(meta.into())).unwrap();
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
        let (opts, subs) = (BuildOpts::new(ds.0.join("tmp")), held_subs(&geo, PLACES_SUB_INDEXES));

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
        for &kept in PLACES_SUB_INDEXES {
            let ds = TempDir::new("partial");
            let index = ds.0.join("search");
            with_poi(["Testville", "Otherton"], "Old Mill").write(&ds.0);
            build_subs(&ds.0, &index, PLACES_SUB_INDEXES).unwrap();
            with_poi(["Nestville", "Otherton"], "New Mill").write(&ds.0);
            let building: Vec<&str> = PLACES_SUB_INDEXES.iter().copied().filter(|&s| s != kept).collect();

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
        build_subs(&ds.0, &index, PLACES_SUB_INDEXES).unwrap();
        let poi = FileStamp::of(&std::fs::metadata(index.join("poi.fst")).unwrap());

        build_subs(&ds.0, &index, &["places"]).unwrap();

        let (geo, stamps) = load_dataset(&ds.0);
        let (idx, origin) = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap();
        assert_eq!(origin, Origin::Current);
        assert_eq!(FileStamp::of(&std::fs::metadata(index.join("poi.fst")).unwrap()), poi);
        assert_eq!((names(&idx, "testville"), names(&idx, "old mill")), (vec!["Testville".to_string()], vec!["Old Mill".to_string()]));
    }

    #[test]
    fn an_index_of_some_sub_indexes_is_built_whole() {
        // `place-search build --sub` into an empty directory records the whole dataset.
        for &sub in PLACES_SUB_INDEXES {
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
        for file in PLACES_SUB_INDEXES.iter().copied().flat_map(sub_index_files).chain([BOUNDARY_FILE.to_string()]) {
            let ds = TempDir::new("missing-file");
            let index = ds.0.join("search");
            with_poi(["Testville", "Otherton"], "Old Mill").write(&ds.0);
            build_subs(&ds.0, &index, PLACES_SUB_INDEXES).unwrap();
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
        build_all(&geo, &held_subs(&geo, PLACES_SUB_INDEXES), &index, &opts, || Ok(meta.into())).unwrap();

        let (idx, origin) = open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap();

        assert_eq!(origin, Origin::Built);
        assert_eq!((names(&idx, "testville"), names(&idx, "prüfdorf")), (vec!["Testville".to_string()], vec![]));
        drop(idx);
        assert_eq!(open(&geo, &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap().1, Origin::Current);
    }

    #[test]
    fn a_place_search_answers_as_a_searcher_over_the_index_it_opened() {
        let ds = TempDir::new("place-search");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let (geo, stamps) = load_dataset(&ds.0);
        let index = ds.0.join("search");

        let (search, origin) = PlaceSearch::open(Arc::new(geo), &stamps, &ds.0, &index, &BuildOnDevice::default()).unwrap();

        // Kept by another thread than the one that opened it.
        let search = std::thread::spawn(move || search).join().unwrap();
        assert_eq!(origin, Origin::Built);
        let geo = load_dataset(&ds.0).0;
        let idx = SearchIndex::open(&geo, &index, None, None).unwrap();
        let (found, _) = search.search(&new_request("otherton", 5)).unwrap();
        assert_eq!(crate::query::results_json(&found), crate::query::results_json(&Searcher::new(&idx).search("otherton", 5).0));
        assert_eq!(found.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(), ["Otherton"]);
    }

    #[test]
    fn a_place_search_refuses_a_profile_its_index_holds_no_data_of() {
        let ds = TempDir::new("place-search-full");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let (geo, stamps) = load_dataset(&ds.0);
        let (search, _) = PlaceSearch::open(Arc::new(geo), &stamps, &ds.0, &ds.0.join("search"), &BuildOnDevice::default()).unwrap();

        let refused = search.search(&SearchRequest::new("otherton", Profile::Full));

        assert_eq!(refused.map(drop), Err(RequestError::ProfileNotBuilt(Profile::Full)));
    }

    #[test]
    fn a_place_search_refuses_an_option_its_index_cannot_honour() {
        let ds = TempDir::new("place-search-options");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let (geo, stamps) = load_dataset(&ds.0);
        let (search, _) = PlaceSearch::open(Arc::new(geo), &stamps, &ds.0, &ds.0.join("search"), &BuildOnDevice::default()).unwrap();
        let immich = || new_request("otherton", 5);
        let bbox = BBox { min_lat: 10.0, min_lon: 20.0, max_lat: 11.0, max_lon: 21.0 };

        for (req, option) in [
            (SearchRequest { mode: MatchMode::Nominatim, ..immich() }, "mode"),
            (SearchRequest { langs: vec!["de".into()], ..immich() }, "langs"),
            (SearchRequest { bias: Some(Bias::at(10.5, 20.5)), ..immich() }, "bias"),
            (SearchRequest { viewbox: Some(Viewbox::Bounded(bbox)), ..immich() }, "viewbox"),
            (SearchRequest { countries: [*b"de"].into(), ..immich() }, "countries"),
            (SearchRequest { layers: Some([Layer::Address].into()), ..immich() }, "layers"),
            (SearchRequest { categories: [Category { class: "place".into(), typ: "city".into() }].into(), ..immich() }, "categories"),
            (SearchRequest { dedupe: false, ..immich() }, "dedupe"),
        ] {
            assert_eq!(search.search(&req).map(drop), Err(RequestError::UnsupportedOption(option)), "{option}");
        }
        assert_eq!(search.search(&immich()).map(|(found, _)| found.len()), Ok(1));
    }

    #[test]
    fn a_place_search_refuses_a_limit_outside_its_profiles_range() {
        let ds = TempDir::new("place-search-limit");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let (geo, stamps) = load_dataset(&ds.0);
        let (search, _) = PlaceSearch::open(Arc::new(geo), &stamps, &ds.0, &ds.0.join("search"), &BuildOnDevice::default()).unwrap();

        for limit in [0, 51, usize::MAX] {
            let refused = search.search(&new_request("otherton", limit));

            assert_eq!(refused.map(drop), Err(RequestError::LimitOutOfRange { limit, max: 50 }), "{limit}");
        }
        assert_eq!(search.search(&new_request("otherton", 50)).map(|(found, _)| found.len()), Ok(1));
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

    // The geocoder index loads the dataset's water dir too: a water file replaced, added or
    // removed during the load fails it. The stamps kept are of the files place search reads,
    // which a water dir leaves as they are.
    #[test]
    fn load_refuses_a_water_dir_replaced_while_loading() {
        let ds = TempDir::new("water");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let water = ds.0.join(WATER_DIR);
        std::fs::create_dir(&water).unwrap();
        std::fs::write(water.join("admin_polygons.bin"), b"day 1").unwrap();
        std::fs::write(water.join("water_meta.json"), b"day 1").unwrap();
        let replace = |name: &str| {
            let new = water.join(format!("{name}.new"));
            std::fs::write(&new, b"day 2").unwrap();
            std::fs::rename(&new, water.join(name)).unwrap();
        };

        let kept = load(&ds.0, || Ok(()));
        let replaced = load(&ds.0, || {
            replace("admin_polygons.bin");
            Ok(())
        });
        let added = load(&ds.0, || {
            replace("arc_index.bin");
            Ok(())
        });
        let removed = load(&ds.0, || std::fs::remove_file(water.join("water_meta.json")).map_err(|e| e.to_string()));

        for (what, got) in [("replaced", replaced), ("added", added), ("removed", removed)] {
            assert!(got.unwrap_err().contains("changed while it was loading"), "{what}");
        }
        assert_eq!(kept.unwrap().1, core_stamps(&ds.0).unwrap());
    }

    // A dir of continent dirs (no admin_cells.bin of its own) loads each continent's dataset
    // and water dir, as MultiIndex::load does: a file of either replaced during the load fails
    // it too. The stamps kept are still those of the dir's own dataset files, of which it holds
    // none.
    #[test]
    fn load_refuses_a_continents_files_replaced_while_loading() {
        let ds = TempDir::new("continents");
        let europe = ds.0.join("europe");
        std::fs::create_dir(&europe).unwrap();
        dataset(["Testville", "Otherton"]).write(&europe);
        std::fs::create_dir(europe.join(WATER_DIR)).unwrap();
        std::fs::write(europe.join(WATER_DIR).join("admin_polygons.bin"), b"day 1").unwrap();
        let replace = |path: PathBuf| {
            let new = path.with_extension("new");
            std::fs::write(&new, b"day 2").unwrap();
            std::fs::rename(&new, path).unwrap();
        };

        let kept = load(&ds.0, || Ok(()));
        let water = load(&ds.0, || {
            replace(europe.join(WATER_DIR).join("admin_polygons.bin"));
            Ok(())
        });
        let dataset_file = load(&ds.0, || {
            replace(europe.join("place_nodes.bin"));
            Ok(())
        });

        for (what, got) in [("water", water), ("dataset", dataset_file)] {
            assert!(got.is_err_and(|e| e.contains("changed while it was loading")), "{what}");
        }
        assert_eq!(kept.unwrap().1, Stamps::default());
    }

    // A continent whose files can't be looked up (here under a water dir that is a file) is one
    // MultiIndex::load skips, loading the others: it fails no load of the dir of continents.
    #[test]
    fn a_continent_whose_files_cant_be_looked_up_fails_no_regional_load() {
        let ds = TempDir::new("unsearchable-continent");
        for name in ["europe", "asia"] {
            std::fs::create_dir(ds.0.join(name)).unwrap();
            dataset(["Testville", "Otherton"]).write(&ds.0.join(name));
        }
        std::fs::write(ds.0.join("asia").join(WATER_DIR), b"not a dir").unwrap();

        let got = load(&ds.0, || Ok(()));

        assert_eq!(got.map(|(_, stamps)| stamps), Ok(Stamps::default()));
    }

    // A file that can't be looked up (here under a water dir that is a file, as under one that
    // can't be searched) fails the load before the geocoder index opens it, naming it.
    #[test]
    fn load_names_a_file_it_cant_look_up() {
        let ds = TempDir::new("unsearchable");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let water = ds.0.join(WATER_DIR);
        std::fs::write(&water, b"not a dir").unwrap();
        let first = water.join(WATER_FILES[0]);

        let got = load(&ds.0, || Ok(()));

        assert!(got.as_ref().is_err_and(|e| e.starts_with(&format!("{}: ", first.display()))), "{got:?}");
        assert_eq!(stamps_of(&water, &WATER_FILES).unwrap_err().kind(), ErrorKind::NotADirectory);
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
    fn the_fingerprint_holds_each_file_the_boundaries_are_read_from() {
        let ds = TempDir::new("boundaries");
        dataset(["Testville", "Otherton"]).write(&ds.0);
        let before = fingerprint(&ds.0).unwrap();
        let mut vertices = std::fs::read(ds.0.join("arc_vertices.bin")).unwrap();
        *vertices.last_mut().unwrap() ^= 1;
        std::fs::write(ds.0.join("arc_vertices.bin"), vertices).unwrap();

        let after = fingerprint(&ds.0).unwrap();

        for name in ["admin_polygons.bin", query_server::ADMIN_OWN_NAMES_FILE].into_iter().chain(ADMIN_GEOMETRY_FILES) {
            assert!(before.contains_key(name), "{name}");
        }
        assert_ne!(after, before);
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
        assert_eq!(with["strings_poi.bin"], lower_hex(&Sha256::digest(b"\0")));
    }
}
