// The datasets of a build the portal serves, from its configurations.json:
// each selection of a region it serves (REGIONS) that the manifest's
// constraints allow, the files of each as a client downloads them
// (downloader::resolve_files), the objects one sync fetches for all of them,
// the groups of datasets whose place search is one index (Group), the water
// dirs served beside them (Water), and what /datasets says of them. The
// constraints are read here only: /datasets lists the combinations they
// exclude, with each one's message and what it requires, for the page to
// grey out.
//
// Water is no axis of a dataset here, as it is of a client's selection: the
// datasets are the selections of water none, and each water value of the
// build with files (major, all, all-exact) is one water dir of its own,
// whose layer the portal loads once and answers beside any dataset's
// address, as a dataset with that water dir would (water=).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::Deserialize;

use crate::downloader::{self, basename, Configurations, RegionSelection};
use query_server::WATER_DIR;
use place_search::dataset::DATASET_FILES;
use query_server::ADMIN_GEOMETRY_FILES;

/// The regions whose selections the portal serves.
pub const REGIONS: [&str; 1] = ["planet"];

/// The preset a /reverse or /polygons without dataset= answers: the
/// dataset :3002 served before the portal had more than one.
pub const DEFAULT_PRESET: &str = "with-addresses";

/// The water axis of a client's selection, which the portal serves as water
/// dirs beside its datasets (Water), not as an axis of theirs.
pub const WATER_AXIS: &str = "water";

/// The water value of no water: the datasets' own, which takes no water
/// dir.
pub const NO_WATER: &str = "none";

/// The water a /reverse without water= answers with, where the build has
/// it; else none.
pub const DEFAULT_WATER: &str = "all";

/// What the portal reads of configurations.json besides what the
/// downloader does (downloader::Configurations): the build, the axes, the
/// constraints and the presets.
#[derive(Deserialize, Debug, Clone)]
pub struct Manifest {
    pub build: Build,
    pub axes: BTreeMap<String, Axis>,
    #[serde(default)]
    pub constraints: Vec<Constraint>,
    #[serde(default)]
    pub presets: Vec<Preset>,
    /// The place search indexes the build publishes (SearchIndex), as it
    /// is: one that doesn't read as such is taken for none, as the portal
    /// builds every index itself and only compares those with these.
    #[serde(default)]
    pub search_index: Option<serde_json::Value>,
    /// The credit of the build's data (its sources, their licences and
    /// notices), as it is: /datasets passes it on for the page to show,
    /// which reads what it can of it.
    #[serde(default)]
    pub attribution: Option<serde_json::Value>,
}

/// The manifest's search_index: the dir of a selection's index (its axes'
/// values in braces), its files, and the selections whose index it
/// publishes there.
#[derive(Deserialize, Debug, Clone)]
pub struct SearchIndex {
    pub dir: String,
    pub files: Vec<String>,
    #[serde(default)]
    pub optional_files: Vec<String>,
    #[serde(default)]
    pub shipped: Vec<BTreeMap<String, String>>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Build {
    pub date: String,
    pub version: u32,
}

/// An axis of a selection (region, mode, quality, poi_tier).
#[derive(Deserialize, Debug, Clone)]
pub struct Axis {
    pub label: String,
    #[serde(default)]
    pub description: Option<String>,
    pub values: BTreeMap<String, AxisValue>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AxisValue {
    pub label: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub order: i64,
}

/// A selection with every value `when` names must have every value
/// `requires` names.
#[derive(Deserialize, Debug, Clone)]
pub struct Constraint {
    pub id: String,
    #[serde(default)]
    pub when: BTreeMap<String, String>,
    #[serde(default)]
    pub requires: BTreeMap<String, String>,
    pub message: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Preset {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub description: Option<String>,
    pub selections: BTreeMap<String, String>,
}

/// The axes of a selection, in the order a dataset's id names them after
/// its region.
const AXES: [&str; 3] = ["mode", "quality", "poi_tier"];

/// A selection's dataset id: `<mode>.<quality>.<poi_tier>`, after
/// `<region>/` where the region isn't planet.
pub fn dataset_id(sel: &RegionSelection) -> String {
    let id = format!("{}.{}.{}", sel.mode, sel.quality, sel.poi_tier);
    if sel.region == "planet" {
        id
    } else {
        format!("{}/{}", sel.region, id)
    }
}

/// A selection the portal serves.
#[derive(Debug, Clone)]
pub struct Dataset {
    pub id: String,
    pub selection: RegionSelection,
    /// The preset's label for a preset's selection; the labels of its values
    /// for any other.
    pub label: String,
    /// The preset's description; none for a selection no preset names.
    pub description: Option<String>,
    /// The id of the preset that names it.
    pub preset: Option<String>,
    /// Its files as the build lays them out (`<region>/<dir>/<name>`), those
    /// the manifest has, in path order: of two of one name, the later is the
    /// one it reads, as a client's flat download keeps it.
    pub files: Vec<String>,
    /// The bytes of the files its dir holds (links): one of each name, as a
    /// client's flat download keeps them.
    pub size_disk: u64,
    /// The bytes a client downloads for it: its files' .zst and transport
    /// units, each unit once.
    pub size_download: u64,
    /// Its group (Plan::groups): the datasets its place search shares an
    /// index with.
    pub group: usize,
}

/// The datasets whose place search is one index: those whose dirs hold the
/// files an index is made of (place_search::dataset::DATASET_FILES) of the
/// same bytes each, by the manifest's sha256, and lack the same of them. An
/// index answers right only beside the files it was built from, so each
/// dataset of a group searches its index with its own geocoder index, whose
/// admin files give the context.
#[derive(Debug, Clone)]
pub struct Group {
    /// Its index's dir in the build's search dir: the id of its first
    /// dataset.
    pub name: String,
    /// Its datasets, in the plan's order.
    pub datasets: Vec<usize>,
    /// What building its index costs, to build the longest first: the bytes
    /// of the files of its records and their names, then of its boundaries'
    /// geometry (query_server::ADMIN_GEOMETRY_FILES), which costs less.
    pub cost: (u64, u64),
    /// The index the build publishes of a selection of it (search_index's
    /// shipped), if any: the first of them.
    pub shipped: Option<Shipped>,
}

/// An index a build publishes: its dir in the build, and each of its files
/// but its search_meta.json (build times) by name, with the sha256 the
/// manifest gives it, none for a file the manifest lacks.
#[derive(Debug, Clone, PartialEq)]
pub struct Shipped {
    pub dir: String,
    pub files: Vec<(String, Option<String>)>,
}

/// A selection the manifest's constraints rule out.
#[derive(Debug, Clone)]
pub struct Excluded {
    pub id: String,
    pub selection: RegionSelection,
    /// The constraint's id, message and the values it requires.
    pub constraint: String,
    pub message: String,
    pub requires: BTreeMap<String, String>,
}

/// A water dir the portal serves beside its datasets: the files a client
/// selection of `id` (major, all, all-exact) puts in the dataset's water/
/// dir, which the sync links into a dir of its own (Water::set) and the
/// portal loads once, for every dataset of its region.
#[derive(Debug, Clone)]
pub struct Water {
    pub region: String,
    /// The water axis value.
    pub id: String,
    pub label: String,
    pub description: Option<String>,
    /// Its files as the build lays them out, in path order, as a dataset's.
    pub files: Vec<String>,
    /// The bytes of the files its dir holds.
    pub size_disk: u64,
    /// The bytes a client downloads for it: its files' .zst and transport
    /// units, each unit once.
    pub size_download: u64,
}

/// An object of the build the sync puts in place: a file published as its
/// own .zst, or a transport unit, for all its files.
#[derive(Debug, Clone)]
pub struct Object {
    /// Its path in the build: a unit's, or the file's for a file published
    /// as its .zst (fetched from `<path>.zst`).
    pub path: String,
    pub unit: bool,
    /// Its bytes on the wire.
    pub size: u64,
    /// The files it puts in place, by path.
    pub files: Vec<String>,
    /// Where an object whose every file the build holds byte for byte in an
    /// object fetched before it in path order takes its files from instead
    /// of the bucket: by file, the path of those bytes. Such an object is
    /// not fetched; its files are hard links.
    pub link: Option<Vec<String>>,
}

/// The datasets of a build and the objects that make them.
#[derive(Debug, Clone)]
pub struct Plan {
    pub date: String,
    pub version: u32,
    /// The selections the portal serves, by region then each axis in its
    /// values' order.
    pub datasets: Vec<Dataset>,
    /// The selections the constraints rule out, in the same order.
    pub excluded: Vec<Excluded>,
    /// Every object any dataset takes, each once, in path order.
    pub objects: Vec<Object>,
    /// The axes, as /datasets lists them: of the region axis, only the
    /// regions served.
    pub axes: Vec<(String, Axis)>,
    /// The presets of the regions served, in the manifest's order, each with
    /// its dataset.
    pub presets: Vec<(Preset, usize)>,
    /// The dataset a request without dataset= answers (DEFAULT_PRESET's).
    pub default: usize,
    /// The groups of datasets whose place search is one index, in the order
    /// of their first datasets.
    pub groups: Vec<Group>,
    /// The manifest's attribution, as it is.
    pub attribution: Option<serde_json::Value>,
    /// The ids of the build's components, sorted: the shell patterns of an attribution
    /// entry's applies_to name them.
    pub components: Vec<String>,
    /// The water axis as the manifest gives it, where it has one with water
    /// dirs.
    pub water_axis: Option<Axis>,
    /// The water dirs served, by region then in the water axis's order: each
    /// value of it with files, in each region served.
    pub water: Vec<Water>,
}

/// The values of an axis of `manifest`, in their order (by key where two
/// share one).
fn values<'a>(manifest: &'a Manifest, axis: &str) -> Result<Vec<(&'a String, &'a AxisValue)>, String> {
    let axis_of = manifest.axes.get(axis).ok_or_else(|| format!("the manifest has no axis {axis}"))?;
    let mut values: Vec<(&String, &AxisValue)> = axis_of.values.iter().collect();
    values.sort_by(|a, b| (a.1.order, a.0).cmp(&(b.1.order, b.0)));
    Ok(values)
}

/// Whether `name` is a plain name: ASCII letters, digits, '.', '-' and
/// '_', starting with none of '.' and not ending in ".tmp". A part of a
/// path that is one is no '.' or '..', none of the hidden names the sync
/// and the decoder keep of their own in a build dir (.fetched, .complete,
/// .transport.<pid>.<n>), and no temporary file's (a download's, or
/// write_whole's).
pub fn plain_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && !name.ends_with(".tmp")
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
}

/// Whether `value` can be an axis value of a dataset's id, whose values the
/// id joins with '.': ASCII letters, digits, '-' and '_'.
fn plain_value(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
}

/// Whether the sync can keep a file, or decode a unit, at `path` in a build
/// dir for a dataset of `region`: the region's dir, then one or more plain
/// names. Such a path stays in the build dir, under the region's dir, so
/// that it meets none of the build dir's own names (its manifest, sets/,
/// the ledger); else why not.
fn check_path(path: &str, region: &str) -> Result<(), String> {
    let Some(rest) = path.strip_prefix(region).and_then(|r| r.strip_prefix('/')) else {
        return Err(format!("the path {path:?} is not under {region}/"));
    };
    match rest.split('/').find(|part| !plain_name(part)) {
        Some(part) => Err(format!(
            "the path {path:?} has the part {part:?}, which is not a plain name (letters, digits, '.', '-' and '_', starting with none of '.' and not ending in .tmp)"
        )),
        None => Ok(()),
    }
}

/// The first constraint of `manifest` that `sel` breaks: it has every value
/// the constraint's `when` names and lacks one its `requires` names.
fn broken<'a>(manifest: &'a Manifest, sel: &RegionSelection) -> Option<&'a Constraint> {
    let has = |values: &BTreeMap<String, String>| values.iter().all(|(axis, value)| sel.axis(axis) == Some(value.as_str()));
    manifest.constraints.iter().find(|c| has(&c.when) && !has(&c.requires))
}

/// The plan of the build `cfg` and `manifest` describe (one
/// configurations.json): every selection of REGIONS, each axis's values in
/// their order, the constraints rule out or the portal serves. An error,
/// why, where an axis value or a dataset's id is no plain name (the id names
/// the dataset's dir), one it serves doesn't resolve
/// (downloader::resolve_files), lacks a file the manifest must have or
/// takes a file, a unit or a unit's member at a path the sync can't keep in
/// a build dir (check_path), a constraint names an axis or a value the
/// manifest's axes don't have, a preset of a region served names a
/// selection it doesn't serve or has another such preset's id or a
/// selection's, or no preset is DEFAULT_PRESET: the build can't be served
/// as the portal serves one.
pub fn plan(cfg: &Configurations, manifest: &Manifest) -> Result<Plan, String> {
    let mut axes = Vec::new();
    for name in std::iter::once("region").chain(AXES) {
        let mut axis = manifest.axes.get(name).ok_or_else(|| format!("the manifest has no axis {name}"))?.clone();
        if name == "region" {
            for region in REGIONS {
                if !axis.values.contains_key(region) {
                    return Err(format!("the manifest's region axis has no {region}"));
                }
            }
            axis.values.retain(|v, _| REGIONS.contains(&v.as_str()));
        }
        axes.push((name.to_string(), axis));
    }

    let (modes, qualities, tiers) = (values(manifest, "mode")?, values(manifest, "quality")?, values(manifest, "poi_tier")?);
    for (axis, values) in AXES.iter().zip([&modes, &qualities, &tiers]) {
        if let Some((value, _)) = values.iter().find(|(value, _)| !plain_value(value)) {
            return Err(format!("the manifest's {axis} value {value:?} is not a plain name (letters, digits, '-' and '_')"));
        }
    }
    // A constraint names the manifest's axes and their values alone: one
    // of another name would never match, and rule out what it shouldn't,
    // or nothing.
    for c in &manifest.constraints {
        for (axis, value) in c.when.iter().chain(&c.requires) {
            let Some(of) = manifest.axes.get(axis) else {
                return Err(format!("constraint {} names the axis {axis:?}, which the manifest doesn't have", c.id));
            };
            if !of.values.contains_key(value) {
                return Err(format!("constraint {} names the {axis} value {value:?}, which the manifest's {axis} axis doesn't have", c.id));
            }
        }
    }
    let (mut datasets, mut excluded) = (Vec::new(), Vec::new());
    let mut objects: BTreeMap<String, Object> = BTreeMap::new();
    for region in REGIONS {
        for (mode, mode_value) in &modes {
            for (quality, quality_value) in &qualities {
                for (tier, tier_value) in &tiers {
                    let sel = RegionSelection {
                        region: region.to_string(),
                        mode: mode.to_string(),
                        quality: quality.to_string(),
                        poi_tier: tier.to_string(),
                        water: NO_WATER.to_string(),
                    };
                    let id = dataset_id(&sel);
                    if !plain_name(&id) {
                        return Err(format!("the dataset id {id:?} is not a plain name"));
                    }
                    if let Some(c) = broken(manifest, &sel) {
                        excluded.push(Excluded {
                            id, selection: sel, constraint: c.id.clone(), message: c.message.clone(), requires: c.requires.clone(),
                        });
                        continue;
                    }
                    let resolved = downloader::resolve_files(cfg, &sel).map_err(|e| format!("{id}: {e}"))?;
                    let (files, fetches) = taken(cfg, &resolved, &resolved.paths, region, &mut objects).map_err(|why| format!("{id}: {why}"))?;
                    let mut dataset = Dataset {
                        id,
                        selection: sel,
                        label: [mode_value, quality_value, tier_value].map(|v| v.label.as_str()).join(" · "),
                        description: None,
                        preset: None,
                        size_disk: 0,
                        size_download: fetches,
                        files,
                        group: 0,
                    };
                    dataset.size_disk = dataset.links().values().map(|p| cfg.files[p].size_raw).sum();
                    datasets.push(dataset);
                }
            }
        }
    }

    let (water_axis, water) = water(cfg, manifest, &datasets, &mut objects)?;

    let mut presets = Vec::new();
    for preset in &manifest.presets {
        if !preset.selections.get("region").is_some_and(|r| REGIONS.contains(&r.as_str())) {
            continue;
        }
        if preset.selections.get("water").is_some_and(|w| w != NO_WATER) {
            continue;
        }
        let at = datasets.iter().position(|d| {
            std::iter::once("region").chain(AXES).all(|axis| preset.selections.get(axis).map(String::as_str) == d.selection.axis(axis))
        });
        let Some(at) = at else {
            return Err(format!("preset {} names a selection this build doesn't serve: {:?}", preset.id, preset.selections));
        };
        let d = &mut datasets[at];
        if d.preset.is_none() {
            d.preset = Some(preset.id.clone());
            d.label = preset.label.clone();
            d.description = preset.description.clone();
        }
        presets.push((preset.clone(), at));
    }
    // A preset's id names it alone (Plan::find takes a preset's before a
    // selection's): no other preset's, and no selection's.
    let mut ids = BTreeSet::new();
    for (preset, _) in &presets {
        if !ids.insert(preset.id.as_str()) {
            return Err(format!("two presets have the id {}", preset.id));
        }
        if datasets.iter().any(|d| d.id == preset.id) || excluded.iter().any(|x| x.id == preset.id) {
            return Err(format!("preset {} has the id of a selection, which dataset= names as such", preset.id));
        }
    }
    let default = presets.iter().find(|(p, _)| p.id == DEFAULT_PRESET).map(|(_, at)| *at)
        .ok_or_else(|| format!("the manifest has no preset {DEFAULT_PRESET}, which the portal answers without dataset="))?;

    let groups = groups(cfg, manifest, &mut datasets);

    // An object whose every file some object before it holds byte for byte
    // is linked to those, not fetched (the build's dirs share files: the
    // place files of each mode, its core strings, ...).
    let mut held: HashMap<&str, String> = HashMap::new();
    let mut objects: Vec<Object> = objects.into_values().collect();
    for object in &mut objects {
        let sha256 = |path: &String| cfg.files[path].sha256.as_str();
        let sources: Option<Vec<String>> = object.files.iter().map(|f| held.get(sha256(f)).cloned()).collect();
        match sources {
            Some(sources) => object.link = Some(sources),
            None => {
                for f in &object.files {
                    held.entry(sha256(f)).or_insert_with(|| f.clone());
                }
            }
        }
    }

    Ok(Plan {
        date: manifest.build.date.clone(),
        version: manifest.build.version,
        datasets,
        excluded,
        objects,
        axes,
        presets,
        default,
        groups,
        attribution: manifest.attribution.clone(),
        components: cfg.components.keys().cloned().collect(),
        water_axis,
        water,
    })
}

/// The files of `paths` (of `resolved`) the manifest has, each at a path
/// the sync can keep (check_path) under `region`, and the bytes their
/// download takes; each object that fetches them added to `objects`. An
/// error, why, where a path that must be there isn't, or a fetch is no
/// object the sync can keep.
fn taken(
    cfg: &Configurations,
    resolved: &downloader::ResolvedFiles,
    paths: &[String],
    region: &str,
    objects: &mut BTreeMap<String, Object>,
) -> Result<(Vec<String>, u64), String> {
    let mut files = Vec::new();
    for path in paths {
        if cfg.files.contains_key(path) {
            check_path(path, region)?;
            files.push(path);
        } else if !resolved.optional.contains(path) {
            return Err(format!("{path} is not in the manifest"));
        }
    }
    let fetches = downloader::fetches(cfg, resolved, &files).map_err(|e| e.to_string())?;
    for fetch in &fetches {
        for path in std::iter::once(fetch.path()).chain(fetch.files().into_iter().map(|f| f.path)) {
            check_path(path, region)?;
        }
        // A unit decodes into its own dir.
        let dir = |path: &str| path.rsplit_once('/').map(|(dir, _)| dir.to_string());
        if fetch.files().iter().any(|f| dir(f.path) != dir(fetch.path())) {
            return Err(format!("{} holds files of another dir", fetch.path()));
        }
        objects.entry(fetch.path().to_string()).or_insert_with(|| Object {
            path: fetch.path().to_string(),
            unit: matches!(fetch, downloader::Fetch::Unit { .. }),
            size: fetch.size(),
            files: fetch.files().iter().map(|f| f.path.to_string()).collect(),
            link: None,
        });
    }
    Ok((files.into_iter().cloned().collect(), fetches.iter().map(|f| f.size()).sum()))
}

/// The water axis of `manifest` and the water dirs the portal serves of
/// it: of each region served, each value of the axis but NO_WATER whose
/// files (those a selection of it puts in the dataset's water/ dir: the
/// region's first dataset's with that water, downloader::resolve_files) the
/// manifest has, their objects added to `objects`. A value whose files the
/// manifest has none of is not served (a build without that water dir); an
/// error where it has some of them and lacks others, or where a value is no
/// plain name.
fn water(
    cfg: &Configurations,
    manifest: &Manifest,
    datasets: &[Dataset],
    objects: &mut BTreeMap<String, Object>,
) -> Result<(Option<Axis>, Vec<Water>), String> {
    let Some(axis) = manifest.axes.get(WATER_AXIS) else { return Ok((None, Vec::new())) };
    let mut served = Vec::new();
    for region in REGIONS {
        let Some(base) = datasets.iter().find(|d| d.selection.region == region) else { continue };
        for (value, v) in values(manifest, WATER_AXIS)? {
            if value == NO_WATER {
                continue;
            }
            if !plain_value(value) {
                return Err(format!("the manifest's {WATER_AXIS} value {value:?} is not a plain name (letters, digits, '-' and '_')"));
            }
            let sel = RegionSelection { water: value.clone(), ..base.selection.clone() };
            let id = format!("{WATER_AXIS} {value}");
            let resolved = downloader::resolve_files(cfg, &sel).map_err(|e| format!("{id}: {e}"))?;
            let in_dir = |path: &&String| resolved.dest(path).starts_with(&format!("{WATER_DIR}/"));
            let paths: Vec<String> = resolved.paths.iter().filter(in_dir).cloned().collect();
            let listed = paths.iter().filter(|p| cfg.files.contains_key(*p)).count();
            if listed == 0 {
                continue;
            }
            let (files, size_download) = taken(cfg, &resolved, &paths, region, objects).map_err(|why| format!("{id}: {why}"))?;
            let mut water = Water {
                region: region.to_string(),
                id: value.clone(),
                label: v.label.clone(),
                description: v.description.clone(),
                files,
                size_disk: 0,
                size_download,
            };
            water.size_disk = water.links().values().map(|p| cfg.files[p].size_raw).sum();
            served.push(water);
        }
    }
    Ok((if served.is_empty() { None } else { Some(axis.clone()) }, served))
}

/// The groups of `datasets` (Group), each dataset given its own; the
/// index `manifest`'s search_index publishes of a selection given to its
/// group.
fn groups(cfg: &Configurations, manifest: &Manifest, datasets: &mut [Dataset]) -> Vec<Group> {
    let mut groups: Vec<Group> = Vec::new();
    let mut by_files: HashMap<Vec<Option<String>>, usize> = HashMap::new();
    for (at, dataset) in datasets.iter_mut().enumerate() {
        let links = dataset.links();
        let file = |name: &str| links.get(name).map(|path| &cfg.files[path]);
        let sha256: Vec<Option<String>> = DATASET_FILES.iter().map(|name| file(name).map(|f| f.sha256.clone())).collect();
        let size = |geometry: bool| -> u64 {
            DATASET_FILES.iter().filter(|name| ADMIN_GEOMETRY_FILES.contains(*name) == geometry).filter_map(|name| file(name)).map(|f| f.size_raw).sum()
        };
        dataset.group = *by_files.entry(sha256).or_insert_with(|| {
            groups.push(Group { name: dataset.id.clone(), datasets: Vec::new(), cost: (size(false), size(true)), shipped: None });
            groups.len() - 1
        });
        groups[dataset.group].datasets.push(at);
    }
    let Some(spec) = manifest.search_index.clone().and_then(|v| serde_json::from_value::<SearchIndex>(v).ok()) else {
        return groups;
    };
    for selection in &spec.shipped {
        let of = |d: &&Dataset| selection.iter().all(|(axis, value)| d.selection.axis(axis) == Some(value.as_str()));
        for dataset in datasets.iter().filter(of) {
            let mut dir = spec.dir.clone();
            for axis in std::iter::once("region").chain(AXES) {
                dir = dir.replace(&format!("{{{axis}}}"), dataset.selection.axis(axis).unwrap_or_default());
            }
            let files = spec.files.iter().chain(&spec.optional_files)
                .filter(|name| name.as_str() != place_search::constants::META_FILE)
                .map(|name| (name.clone(), cfg.files.get(&format!("{dir}/{name}")).map(|f| f.sha256.clone())))
                .collect();
            groups[dataset.group].shipped.get_or_insert(Shipped { dir, files });
        }
    }
    groups
}

impl Plan {
    /// The water dir a lookup of the dataset `at` answers with, by what
    /// `wanted` names (water=): none for NO_WATER, else the water dir of
    /// that value in the dataset's region; without water=, DEFAULT_WATER's
    /// where the build has it, else none. Else why not, for a 400: what the
    /// build offers.
    pub fn water_of(&self, at: usize, wanted: Option<&str>) -> Result<Option<usize>, String> {
        let region = &self.datasets[at].selection.region;
        let find = |value: &str| self.water.iter().position(|w| &w.region == region && w.id == value);
        match wanted {
            None => Ok(find(DEFAULT_WATER)),
            Some(NO_WATER) => Ok(None),
            Some(value) => find(value).map(Some).ok_or_else(|| {
                let offered: Vec<&str> = std::iter::once(NO_WATER)
                    .chain(self.water.iter().filter(|w| &w.region == region).map(|w| w.id.as_str()))
                    .collect();
                format!("unknown water \"{value}\": this build offers water {}", offered.join(", "))
            }),
        }
    }

    /// The dataset `wanted` names: a preset's id, or a dataset's. Else why
    /// not, for a 400: the constraint's message for a selection it rules
    /// out, or what the build offers.
    pub fn find(&self, wanted: &str) -> Result<usize, String> {
        if let Some((_, at)) = self.presets.iter().find(|(p, _)| p.id == wanted) {
            return Ok(*at);
        }
        if let Some(at) = self.datasets.iter().position(|d| d.id == wanted) {
            return Ok(at);
        }
        if let Some(x) = self.excluded.iter().find(|x| x.id == wanted) {
            return Err(format!("dataset {wanted} is not offered: {}", x.message));
        }
        let presets: Vec<&str> = self.presets.iter().map(|(p, _)| p.id.as_str()).collect();
        let axes: Vec<String> = self.axes.iter().filter(|(name, _)| name != "region")
            .map(|(name, axis)| {
                let mut values: Vec<(&String, &AxisValue)> = axis.values.iter().collect();
                values.sort_by(|a, b| (a.1.order, a.0).cmp(&(b.1.order, b.0)));
                format!("{name} {}", values.iter().map(|(v, _)| v.as_str()).collect::<Vec<_>>().join(", "))
            })
            .collect();
        Err(format!(
            "unknown dataset \"{wanted}\": this build offers the presets {} and <mode>.<quality>.<poi_tier> of {}, except the combinations /datasets lists as excluded",
            presets.join(", "), axes.join("; ")
        ))
    }

    /// What /datasets answers while the build of the publication ID `id`
    /// (sync::publication_id) is served from its dir `dir`: its attribution
    /// as the manifest has it, null where it has none, and the ids of its
    /// components, which the attribution's applies_to names.
    pub fn json(&self, id: &str, dir: &str) -> serde_json::Value {
        let selection = |s: &RegionSelection| {
            serde_json::json!({ "region": s.region, "mode": s.mode, "quality": s.quality, "poi_tier": s.poi_tier })
        };
        let axes: Vec<serde_json::Value> = self.axes.iter().map(|(name, axis)| {
            let mut values: Vec<(&String, &AxisValue)> = axis.values.iter().collect();
            values.sort_by(|a, b| (a.1.order, a.0).cmp(&(b.1.order, b.0)));
            serde_json::json!({
                "id": name,
                "label": axis.label,
                "description": axis.description,
                "values": values.iter().map(|(id, v)| serde_json::json!({ "id": id, "label": v.label, "description": v.description })).collect::<Vec<_>>(),
            })
        }).collect();
        // The water dirs served, as the water axis's values (NO_WATER first),
        // each with its sizes; none for a build without one.
        let water = self.water_axis.as_ref().map(|axis| {
            let mut values: Vec<serde_json::Value> = Vec::new();
            if let Some(none) = axis.values.get(NO_WATER) {
                values.push(serde_json::json!({ "id": NO_WATER, "label": none.label, "description": none.description }));
            }
            for w in &self.water {
                values.push(serde_json::json!({
                    "id": w.id, "label": w.label, "description": w.description, "region": w.region,
                    "size_disk": w.size_disk, "size_download": w.size_download,
                }));
            }
            let default = if self.water.iter().any(|w| w.id == DEFAULT_WATER) { DEFAULT_WATER } else { NO_WATER };
            serde_json::json!({ "label": axis.label, "description": axis.description, "default": default, "values": values })
        });
        serde_json::json!({
            "build": { "date": self.date, "version": self.version, "id": id, "dir": dir },
            "water": water,
            "default": self.datasets[self.default].id,
            "axes": axes,
            "presets": self.presets.iter().map(|(p, at)| serde_json::json!({
                "id": p.id, "label": p.label, "description": p.description, "dataset": self.datasets[*at].id,
            })).collect::<Vec<_>>(),
            "datasets": self.datasets.iter().map(|d| serde_json::json!({
                "id": d.id,
                "label": d.label,
                "description": d.description,
                "preset": d.preset,
                "selection": selection(&d.selection),
                "size_disk": d.size_disk,
                "size_download": d.size_download,
                "search": { "index": self.groups[d.group].name, "shipped": self.groups[d.group].shipped.is_some() },
            })).collect::<Vec<_>>(),
            "excluded": self.excluded.iter().map(|x| serde_json::json!({
                "id": x.id,
                "selection": selection(&x.selection),
                "constraint": x.constraint,
                "message": x.message,
                "requires": x.requires,
            })).collect::<Vec<_>>(),
            "attribution": self.attribution,
            "components": self.components,
        })
    }
}

impl Dataset {
    /// The path of each file its dir holds, by its name: of two of one name,
    /// the later.
    pub fn links(&self) -> BTreeMap<String, String> {
        links(&self.files)
    }
}

impl Water {
    /// Its dir's name in a build's sets dir: `water/<id>`, after
    /// `<region>/` where the region isn't planet (as dataset_id names a
    /// dataset's).
    pub fn set(&self) -> String {
        let set = format!("{WATER_DIR}/{}", self.id);
        if self.region == "planet" {
            set
        } else {
            format!("{}/{set}", self.region)
        }
    }

    /// The path of each file its dir holds, by its name, as a dataset's.
    pub fn links(&self) -> BTreeMap<String, String> {
        links(&self.files)
    }
}

/// The path of each of `files` by its name: of two of one name, the later.
fn links(files: &[String]) -> BTreeMap<String, String> {
    files.iter().map(|path| (basename(path).to_string(), path.clone())).collect()
}
