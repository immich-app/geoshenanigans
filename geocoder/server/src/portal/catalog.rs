// The datasets of a build the portal serves, from its configurations.json:
// each selection of a region it serves (REGIONS) that the manifest's
// constraints allow, the files of each as a client downloads them
// (downloader::resolve_files), the objects one sync fetches for all of them,
// and what /datasets says of them. The constraints are read here only:
// /datasets lists the combinations they exclude, with each one's message and
// what it requires, for the page to grey out.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::Deserialize;

use crate::downloader::{self, basename, Configurations, RegionSelection};

/// The regions whose selections the portal serves.
pub const REGIONS: [&str; 1] = ["planet"];

/// The preset a /reverse or /polygons without dataset= answers: the
/// dataset :3002 served before the portal had more than one.
pub const DEFAULT_PRESET: &str = "with-addresses";

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
                    let mut files = Vec::new();
                    for path in &resolved.paths {
                        if cfg.files.contains_key(path) {
                            check_path(path, region).map_err(|why| format!("{id}: {why}"))?;
                            files.push(path);
                        } else if !resolved.optional.contains(path) {
                            return Err(format!("{id}: {path} is not in the manifest"));
                        }
                    }
                    let fetches = downloader::fetches(cfg, &files).map_err(|e| format!("{id}: {e}"))?;
                    for fetch in &fetches {
                        for path in std::iter::once(fetch.path()).chain(fetch.files().into_iter().map(|(path, _)| path)) {
                            check_path(path, region).map_err(|why| format!("{id}: {why}"))?;
                        }
                        // A unit decodes into its own dir.
                        let dir = |path: &str| path.rsplit_once('/').map(|(dir, _)| dir.to_string());
                        if fetch.files().iter().any(|(path, _)| dir(path) != dir(fetch.path())) {
                            return Err(format!("{id}: {} holds files of another dir", fetch.path()));
                        }
                        objects.entry(fetch.path().to_string()).or_insert_with(|| Object {
                            path: fetch.path().to_string(),
                            unit: matches!(fetch, downloader::Fetch::Unit { .. }),
                            size: fetch.size(),
                            files: fetch.files().iter().map(|(path, _)| path.to_string()).collect(),
                            link: None,
                        });
                    }
                    let mut dataset = Dataset {
                        id,
                        selection: sel,
                        label: [mode_value, quality_value, tier_value].map(|v| v.label.as_str()).join(" · "),
                        description: None,
                        preset: None,
                        size_disk: 0,
                        size_download: fetches.iter().map(|f| f.size()).sum(),
                        files: files.into_iter().cloned().collect(),
                    };
                    dataset.size_disk = dataset.links().values().map(|p| cfg.files[p].size_raw).sum();
                    datasets.push(dataset);
                }
            }
        }
    }

    let mut presets = Vec::new();
    for preset in &manifest.presets {
        if !preset.selections.get("region").is_some_and(|r| REGIONS.contains(&r.as_str())) {
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
    })
}

impl Plan {
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

    /// What /datasets answers while the build is served from its dir
    /// `dir`.
    pub fn json(&self, dir: &str) -> serde_json::Value {
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
        serde_json::json!({
            "build": { "date": self.date, "version": self.version, "dir": dir },
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
            })).collect::<Vec<_>>(),
            "excluded": self.excluded.iter().map(|x| serde_json::json!({
                "id": x.id,
                "selection": selection(&x.selection),
                "constraint": x.constraint,
                "message": x.message,
                "requires": x.requires,
            })).collect::<Vec<_>>(),
        })
    }
}

impl Dataset {
    /// The path of each file its dir holds, by its name: of two of one name,
    /// the later.
    pub fn links(&self) -> BTreeMap<String, String> {
        self.files.iter().map(|path| (basename(path).to_string(), path.clone())).collect()
    }
}
