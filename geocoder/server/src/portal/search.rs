// The portal's place search. Each dataset of a build searches the index of
// its group (catalog::Group): the datasets whose dirs hold the files an
// index is made of alike share one. A build's indexes are in its dir, a dir
// a group under search/, and go with it when it is pruned. The sync opens
// them (Searches::step) once the build loads, before it switches to it; a
// build served at start opens them once it is served. A group's index is
// built from its first dataset's files where its dir doesn't hold one
// built from them (place_search::dataset::open, as a device builds its
// own), a few groups at once, the costliest first, each on a thread of its
// own at the lowest priority; each other dataset of the group then finds
// the index built from its own files. The files of the indexes that hold
// the same bytes under one name are then made hard links of one. A group
// that fails is a test result, not the build's: the build is switched to
// all the same, its datasets answer /search with 503 and why, and a later
// round tries it again. The index of a group the build publishes
// (search_index's shipped selections) is compared with the published
// files' sha256 once it is open, for /health to tell.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime};

use place_search::build::sub_index_files;
use place_search::constants::{BOUNDARY_FILE, SUB_INDEXES};
use place_search::dataset::{Origin, PlaceSearch, Stamps};
use place_search::multi::MultiSearch;
use query_server::{Index, MultiIndex};

use super::catalog::{self, Plan, Shipped};
use super::sync::{iso, sha256_of};

/// How a dataset's place search opens over the index in a dir: its
/// geocoder index, the stamps of the files that was loaded from, its dir
/// and the index's dir (open_search).
pub type Opener = Arc<dyn Fn(Arc<Index>, &Stamps, &Path, &Path) -> Result<(PlaceSearch, Origin), String> + Send + Sync>;

/// place_search::dataset::open, as a device opens its place search: the
/// index there if it was built from these files, else one built there from
/// them first, on one thread, with the sort memory of a device's build.
pub fn open_search(geo: Arc<Index>, loaded: &Stamps, dir: &Path, index: &Path) -> Result<(PlaceSearch, Origin), String> {
    PlaceSearch::open(geo, loaded, dir, index, &Default::default())
}

/// A dataset of a build as it loaded, which its place search opens over.
pub struct Loaded {
    pub id: String,
    pub index: Arc<MultiIndex>,
}

/// Where a group's index is.
enum State {
    /// To be opened, since then.
    Queued { since: String },
    /// Being opened, and built where it must be, since then.
    Building { since: String },
    /// Open: a search for each dataset of the group, in its order.
    Ready { searches: Vec<Arc<MultiSearch>>, origin: Origin, seconds: f64, since: String, shipped: Option<ShippedCheck> },
    /// Its open failed, then, and why.
    Failed { since: String, reason: String },
}

/// How the index of a group compares with the one the build publishes of
/// it (catalog::Shipped).
#[derive(Debug, Clone, PartialEq)]
pub struct ShippedCheck {
    pub dir: String,
    /// The files of other bytes than the published ones', those the index
    /// lacks among them.
    pub differ: Vec<String>,
    /// The files of the index that the build doesn't publish.
    pub unpublished: Vec<String>,
}

impl ShippedCheck {
    pub fn same(&self) -> bool {
        self.differ.is_empty() && self.unpublished.is_empty()
    }
}

/// The files of the index in `dir` against those of `shipped`, by sha256.
pub fn check_shipped(dir: &Path, shipped: &Shipped) -> ShippedCheck {
    let (mut differ, mut unpublished) = (Vec::new(), Vec::new());
    for (name, published) in &shipped.files {
        let path = dir.join(name);
        match (published, path.is_file()) {
            (Some(sha256), true) => match sha256_of(&path) {
                Ok(ours) if ours == *sha256 => {}
                Ok(_) => differ.push(name.clone()),
                Err(e) => differ.push(format!("{name} ({e})")),
            },
            (Some(_), false) => differ.push(name.clone()),
            (None, true) => unpublished.push(name.clone()),
            (None, false) => {}
        }
    }
    ShippedCheck { dir: shipped.dir.clone(), differ, unpublished }
}

/// Why a dataset's search can't answer: its group's index isn't open.
pub enum Unready {
    /// It is to be opened, or being opened, since then.
    Building { since: String },
    /// Its open failed, then, and why.
    Failed { since: String, reason: String },
}

struct Group {
    group: catalog::Group,
    state: Mutex<State>,
}

/// The place search of a build's datasets.
pub struct Searches {
    /// The build's search dir.
    root: PathBuf,
    groups: Vec<Group>,
    datasets: Vec<Loaded>,
    /// Of each dataset: its group, and its place among the group's datasets.
    of: Vec<(usize, usize)>,
    /// Held by the step that opens the groups: one at a time.
    step: Mutex<()>,
}

/// Lowers the calling thread's priority to the lowest, on Linux, where a
/// thread has one of its own: an index build then takes only the CPU the
/// lookups and the searches leave.
fn lower_priority() {
    // SAFETY: setpriority reads nothing of this process's memory.
    #[cfg(target_os = "linux")]
    unsafe {
        libc::setpriority(libc::PRIO_PROCESS, 0, 19);
    }
}

fn now() -> String {
    iso(SystemTime::now())
}

impl Searches {
    /// The place search of the build of `plan`, in its search dir `root`:
    /// each of `datasets` (the plan's, in its order), every group queued.
    pub fn new(root: PathBuf, plan: &Plan, datasets: Vec<Loaded>) -> Searches {
        let mut of = vec![(0, 0); plan.datasets.len()];
        for (g, group) in plan.groups.iter().enumerate() {
            for (i, &d) in group.datasets.iter().enumerate() {
                of[d] = (g, i);
            }
        }
        let since = now();
        let groups = plan.groups.iter()
            .map(|group| Group { group: group.clone(), state: Mutex::new(State::Queued { since: since.clone() }) })
            .collect();
        Searches { root, groups, datasets, of, step: Mutex::new(()) }
    }

    fn state(&self, g: usize) -> std::sync::MutexGuard<'_, State> {
        self.groups[g].state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn set(&self, g: usize, state: State) {
        *self.state(g) = state;
    }

    /// Whether every group's index is open.
    pub fn all_ready(&self) -> bool {
        (0..self.groups.len()).all(|g| matches!(*self.state(g), State::Ready { .. }))
    }

    /// The name of the index of the dataset `at` (its group's).
    pub fn index_of(&self, at: usize) -> &str {
        &self.groups[self.of[at].0].group.name
    }

    /// The search of the dataset `at`, or why there is none yet.
    pub fn search(&self, at: usize) -> Result<Arc<MultiSearch>, Unready> {
        let (g, i) = self.of[at];
        match &*self.state(g) {
            State::Ready { searches, .. } => Ok(searches[i].clone()),
            State::Queued { since } | State::Building { since } => Err(Unready::Building { since: since.clone() }),
            State::Failed { since, reason } => Err(Unready::Failed { since: since.clone(), reason: reason.clone() }),
        }
    }

    /// Opens every group's index not open, with `open`, `jobs` at once, the
    /// costliest first (catalog::Group::cost), each on a thread of its own
    /// at the lowest priority; then makes the index files of the same bytes
    /// hard links of one (link_identical). It blocks until all are done:
    /// on a blocking thread. A step that finds another running does
    /// nothing. What it did.
    pub fn step(&self, open: &Opener, jobs: usize) -> String {
        let Ok(_one) = self.step.try_lock() else { return "another step opens its groups".to_string() };
        let mut todo: Vec<usize> = (0..self.groups.len()).filter(|&g| !matches!(*self.state(g), State::Ready { .. })).collect();
        if todo.is_empty() {
            return format!("its {} groups are open", self.groups.len());
        }
        todo.sort_by(|&a, &b| {
            let (a, b) = (&self.groups[a].group, &self.groups[b].group);
            b.cost.cmp(&a.cost).then_with(|| a.name.cmp(&b.name))
        });
        let since = now();
        for &g in &todo {
            self.set(g, State::Queued { since: since.clone() });
        }
        let next = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..jobs.clamp(1, todo.len()) {
                let work = || {
                    lower_priority();
                    while let Some(&g) = todo.get(next.fetch_add(1, Ordering::Relaxed)) {
                        self.open_group(g, open);
                    }
                };
                std::thread::Builder::new().name("portal-search".to_string()).spawn_scoped(scope, work).expect("search thread");
            }
        });
        let linked = self.link_identical();
        let failed: Vec<&str> = (0..self.groups.len())
            .filter(|&g| matches!(*self.state(g), State::Failed { .. }))
            .map(|g| self.groups[g].group.name.as_str())
            .collect();
        let open = self.groups.len() - failed.len();
        let mut done = format!("{open} of its {} groups open, {} opened now; {linked} files linked", self.groups.len(), todo.len());
        if !failed.is_empty() {
            done.push_str(&format!("; failed: {}", failed.join(", ")));
        }
        done
    }

    /// Opens the index of the group `g` for each of its datasets, the first
    /// building it where it must; a panic of it is a failure of the group.
    fn open_group(&self, g: usize, open: &Opener) {
        let group = &self.groups[g].group;
        let dir = self.root.join(&group.name);
        self.set(g, State::Building { since: now() });
        let started = Instant::now();
        let opened = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<(Vec<Arc<MultiSearch>>, Origin), String> {
            std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir {}: {e}", dir.display()))?;
            let mut searches = Vec::new();
            let mut origin = Origin::Current;
            for &d in &group.datasets {
                let dataset = &self.datasets[d];
                let (search, opened) = MultiSearch::open_with(&dataset.index, Some(&dir), &**open)
                    .map_err(|e| format!("dataset {}: {e}", dataset.id))?;
                if opened == Origin::Built {
                    origin = Origin::Built;
                }
                searches.push(Arc::new(search));
            }
            Ok((searches, origin))
        }));
        let seconds = started.elapsed().as_secs_f64();
        let state = match opened {
            Ok(Ok((searches, origin))) => {
                let shipped = group.shipped.as_ref().map(|s| check_shipped(&dir, s));
                eprintln!("portal search: {} open ({}) in {seconds:.1} s", group.name, if origin == Origin::Built { "built" } else { "current" });
                State::Ready { searches, origin, seconds, since: now(), shipped }
            }
            Ok(Err(reason)) => State::Failed { since: now(), reason },
            Err(panic) => {
                let why = panic.downcast_ref::<String>().cloned().or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()));
                State::Failed { since: now(), reason: format!("its open panicked: {}", why.unwrap_or_default()) }
            }
        };
        if let State::Failed { reason, .. } = &state {
            eprintln!("portal search: {} failed: {reason}", group.name);
        }
        self.set(g, state);
    }

    /// Makes each index file of a group's dir that holds the bytes of the
    /// file of its name in a group's dir before it a hard link of that one,
    /// put in its place by a rename; files of one inode are read no more.
    /// The searches that map a file replaced keep its bytes. How many it
    /// linked; a file it can't link stays as it is.
    fn link_identical(&self) -> usize {
        use std::os::unix::fs::MetadataExt;
        let names = SUB_INDEXES.iter().flat_map(|s| sub_index_files(s)).chain([BOUNDARY_FILE.to_string()]);
        let mut linked = 0;
        for name in names {
            // The files kept so far: the inode, size and sha256 of each, and its path.
            let mut kept: Vec<((u64, u64), u64, Option<String>, PathBuf)> = Vec::new();
            for group in &self.groups {
                let path = self.root.join(&group.group.name).join(&name);
                let Ok(meta) = std::fs::symlink_metadata(&path) else { continue };
                let inode = (meta.dev(), meta.ino());
                if !meta.is_file() || kept.iter().any(|k| k.0 == inode) {
                    continue;
                }
                let mut sha256 = None;
                let mut same = None;
                for k in kept.iter_mut().filter(|k| k.1 == meta.len()) {
                    if k.2.is_none() {
                        k.2 = sha256_of(&k.3).ok();
                    }
                    if sha256.is_none() {
                        sha256 = sha256_of(&path).ok();
                    }
                    if k.2.is_some() && k.2 == sha256 {
                        same = Some(k.3.clone());
                        break;
                    }
                }
                let Some(from) = same else {
                    kept.push((inode, meta.len(), sha256, path));
                    continue;
                };
                let mut tmp = path.as_os_str().to_owned();
                tmp.push(".link.tmp");
                let tmp = PathBuf::from(tmp);
                let _ = std::fs::remove_file(&tmp);
                match std::fs::hard_link(&from, &tmp).and_then(|()| std::fs::rename(&tmp, &path)) {
                    Ok(()) => linked += 1,
                    Err(e) => {
                        let _ = std::fs::remove_file(&tmp);
                        eprintln!("portal search: link {} to {}: {e}", path.display(), from.display());
                    }
                }
            }
        }
        linked
    }

    /// What /health tells of them: how many datasets search, and each
    /// group's state.
    pub fn json(&self) -> serde_json::Value {
        let mut ready = 0;
        let groups: Vec<serde_json::Value> = self.groups.iter().enumerate().map(|(g, group)| {
            let datasets: Vec<&str> = group.group.datasets.iter().map(|&d| self.datasets[d].id.as_str()).collect();
            let mut out = serde_json::json!({ "index": group.group.name, "datasets": datasets });
            match &*self.state(g) {
                State::Queued { since } => {
                    out["state"] = "queued".into();
                    out["since"] = since.as_str().into();
                }
                State::Building { since } => {
                    out["state"] = "building".into();
                    out["since"] = since.as_str().into();
                }
                State::Ready { origin, seconds, since, shipped, .. } => {
                    ready += datasets.len();
                    out["state"] = "ready".into();
                    out["since"] = since.as_str().into();
                    out["origin"] = if *origin == Origin::Built { "built" } else { "current" }.into();
                    out["seconds"] = serde_json::json!((seconds * 10.0).round() / 10.0);
                    if let Some(s) = shipped {
                        out["shipped"] = serde_json::json!({ "dir": s.dir, "same": s.same(), "differ": s.differ, "unpublished": s.unpublished });
                    }
                }
                State::Failed { since, reason } => {
                    out["state"] = "failed".into();
                    out["since"] = since.as_str().into();
                    out["reason"] = reason.as_str().into();
                }
            }
            if out.get("shipped").is_none() {
                out["shipped"] = group.group.shipped.as_ref().map(|s| serde_json::json!({ "dir": s.dir })).into();
            }
            out
        }).collect();
        serde_json::json!({ "ready": ready, "datasets": self.datasets.len(), "groups": groups })
    }
}
