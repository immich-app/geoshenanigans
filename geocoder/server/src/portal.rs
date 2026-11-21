// The test portal's server (the query-portal binary, portal_main.rs). It
// keeps the whole planet build in <data>/builds, synced from the bucket by
// itself (sync.rs), and serves each selection of it the manifest's
// constraints allow (catalog.rs) as a dataset: /reverse and /polygons with
// dataset=<id> answer what a query-server answers from that selection's
// files (handlers.rs), /datasets lists the datasets and the combinations
// the constraints exclude, /health tells the build served and how the sync
// goes, and /test is the page. Without a build to serve it starts all the
// same: those routes answer 503 with the reason while it syncs.

pub mod catalog;
pub mod sync;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use axum::extract::{ConnectInfo, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use serde::{Deserialize, Serialize};

use crate::{auth, handlers};
use query_server::{MultiIndex, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL};

/// The build version of the files this server reads, the build.version of
/// configurations.template.json (a test here and
/// builder/tools/test_build_search_indexes.py hold it to that): it syncs and
/// serves builds of this version only.
pub const BUILD_VERSION: u32 = 24;

/// Where the builds are published: latest.json and each build's files
/// under builds/.
pub const BASE_URL: &str = "https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder";

/// How a dataset's dir loads: into a MultiIndex of one slot.
pub type Loader = Arc<dyn Fn(&Path) -> Result<MultiIndex, String> + Send + Sync>;

/// How the portal syncs and loads.
pub struct Config {
    /// The bucket (BASE_URL).
    pub base_url: String,
    /// The time between two sync rounds.
    pub interval: Duration,
    /// The time after a round that failed before the next.
    pub retry_after: Duration,
    /// The pauses before each new try of an object (or a JSON file) whose
    /// fetch failed: as many tries more as it has pauses.
    pub retry_delays: Vec<Duration>,
    /// How many objects a sync fetches at once.
    pub fetches_at_once: usize,
    /// How a dataset's dir loads (load_dataset).
    pub load: Loader,
    /// The bytes free to this process in the file system of a dir.
    pub free_space: fn(&Path) -> std::io::Result<u64>,
}

impl Config {
    /// The portal's: the bucket of PORTAL_BASE_URL (BASE_URL by default), a
    /// round every PORTAL_SYNC_SECONDS (3600 by default).
    pub fn from_env() -> Config {
        let seconds = std::env::var("PORTAL_SYNC_SECONDS").ok().and_then(|v| v.parse::<u64>().ok()).filter(|&s| s > 0).unwrap_or(3600);
        Config {
            base_url: std::env::var("PORTAL_BASE_URL").unwrap_or_else(|_| BASE_URL.to_string()),
            interval: Duration::from_secs(seconds),
            retry_after: Duration::from_secs(seconds.min(600)),
            retry_delays: [5, 30, 120].map(Duration::from_secs).to_vec(),
            fetches_at_once: 8,
            load: Arc::new(load_dataset),
            free_space,
        }
    }
}

/// The dataset in the dir `dir` (the links to one selection's files), as a
/// query-server serving that dir loads it: a MultiIndex of one "world"
/// slot. An error where it doesn't load, or holds no admin_cells.bin.
pub fn load_dataset(dir: &Path) -> Result<MultiIndex, String> {
    let at = dir.to_str().ok_or_else(|| format!("{} is no UTF-8 path", dir.display()))?;
    let index = MultiIndex::load(at, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE)?;
    if index.continents.len() != 1 || index.loaded_count() != 1 {
        return Err(format!("{at} holds no admin_cells.bin"));
    }
    Ok(index)
}

/// The bytes free to this process in the file system of `dir` (statvfs).
pub fn free_space(dir: &Path) -> std::io::Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(dir.as_os_str().as_bytes())?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(path.as_ptr(), &mut stat) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(stat.f_bavail as u64 * stat.f_frsize as u64)
}

/// A build the portal serves: its datasets, loaded.
pub struct Served {
    /// Its dir under builds/ (`<date>-<n>`).
    pub dir: String,
    pub plan: catalog::Plan,
    /// Each dataset's index, as plan.datasets orders them.
    pub indexes: Vec<Arc<MultiIndex>>,
    /// What /datasets answers.
    pub datasets_json: String,
}

/// What the sync rounds have done, for /health and the page.
#[derive(Serialize, Clone, Default, Debug)]
pub struct Status {
    /// What the round running now does; none between rounds.
    pub running: Option<String>,
    /// The objects the running round's fetch has put in place, of all.
    pub progress: Option<Progress>,
    pub last_started: Option<String>,
    pub last_finished: Option<String>,
    /// How the last round ended: "ok: ..." or "failed: ...".
    pub last_result: Option<String>,
    pub next_round: Option<String>,
    /// The latest build the bucket names, as the last round found it.
    pub latest: Option<Latest>,
    /// The dir of the build served.
    pub served: Option<String>,
    /// The builds refused (sync::Refusal), not fetched again while latest.json
    /// names them.
    pub refused: Vec<sync::Refusal>,
    /// Why the files of the build served are not those of its manifest: the
    /// sync fetches it again into a new dir.
    pub damaged: Option<String>,
}

#[derive(Serialize, Clone, Default, Debug)]
pub struct Progress {
    pub objects_done: usize,
    pub objects_total: usize,
    pub bytes_done: u64,
    pub bytes_total: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Latest {
    pub date: String,
    /// Its build version, where latest.json names one.
    pub build_version: Option<u32>,
}

pub struct Portal {
    /// The data dir: geocoder.json, builds/, the sync's lock and records.
    pub data: PathBuf,
    pub config: Config,
    served: arc_swap::ArcSwapOption<Served>,
    status: Mutex<Status>,
    /// The build dirs this process found it can't serve (another build
    /// version, damaged, not loading): the prune keeps none of them.
    unusable: Mutex<std::collections::BTreeSet<String>>,
    http: reqwest::Client,
}

impl Portal {
    pub fn new(data: PathBuf, config: Config) -> Portal {
        Portal {
            data,
            config,
            served: arc_swap::ArcSwapOption::empty(),
            status: Mutex::new(Status::default()),
            unusable: Mutex::new(Default::default()),
            http: reqwest::Client::builder()
                .user_agent("geocoder-portal/0.1")
                .connect_timeout(Duration::from_secs(30))
                .read_timeout(Duration::from_secs(120))
                .build()
                .expect("failed to build reqwest client"),
        }
    }

    /// The build served, if any.
    pub fn served(&self) -> Option<Arc<Served>> {
        self.served.load_full()
    }

    pub fn status(&self) -> Status {
        self.status.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn set_status(&self, f: impl FnOnce(&mut Status)) {
        f(&mut self.status.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// Why the routes answer 503: no build to serve, and what the sync does
    /// about it.
    pub fn unavailable(&self) -> String {
        let status = self.status();
        let mut why = format!("no build to serve: waiting for a build of version {BUILD_VERSION}");
        if let Some(running) = &status.running {
            why.push_str(&format!("; the sync is {running}"));
        }
        if let Some(result) = &status.last_result {
            why.push_str(&format!("; the last sync: {result}"));
        }
        why
    }

    /// The build served and the dataset `wanted` names (none: DEFAULT_PRESET's), or the answer
    /// why not: 503 without a build, 400 for a dataset it doesn't serve.
    fn dataset(&self, wanted: Option<&str>) -> Result<(Arc<Served>, usize), Response> {
        let Some(served) = self.served() else {
            return Err((StatusCode::SERVICE_UNAVAILABLE, self.unavailable()).into_response());
        };
        let at = match wanted {
            None => served.plan.default,
            Some(id) => served.plan.find(id).map_err(|why| (StatusCode::BAD_REQUEST, why).into_response())?,
        };
        Ok((served, at))
    }
}

struct App {
    portal: Arc<Portal>,
    db: Arc<RwLock<auth::Db>>,
    limiter: auth::RateLimiter,
}

/// The portal's routes: /health, /datasets, /reverse, /polygons and /test,
/// keys checked against `db`.
pub fn router(portal: Arc<Portal>, db: Arc<RwLock<auth::Db>>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/datasets", get(datasets))
        .route("/reverse", get(reverse))
        .route("/polygons", get(polygons))
        .route("/test", get(handlers::test_portal))
        .layer(axum::middleware::from_fn(handlers::cors_middleware))
        .with_state(Arc::new(App { portal, db, limiter: auth::RateLimiter::default() }))
}

fn json(status: StatusCode, body: String) -> Response {
    (status, [(axum::http::header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// 200 with the build served, its dataset count and the sync's status; 503
/// with the reason and the sync's status while there is none.
async fn health(State(app): State<Arc<App>>) -> Response {
    let sync = app.portal.status();
    match app.portal.served() {
        Some(served) => json(StatusCode::OK, serde_json::json!({
            "status": "ok",
            "build": { "date": served.plan.date, "version": served.plan.version, "dir": served.dir },
            "datasets": served.plan.datasets.len(),
            "sync": sync,
        }).to_string()),
        None => json(StatusCode::SERVICE_UNAVAILABLE, serde_json::json!({
            "status": "unavailable",
            "reason": app.portal.unavailable(),
            "sync": sync,
        }).to_string()),
    }
}

/// The served build's datasets (catalog::Plan::json); 503 while there is
/// none.
async fn datasets(State(app): State<Arc<App>>) -> Response {
    match app.portal.served() {
        Some(served) => json(StatusCode::OK, served.datasets_json.clone()),
        None => (StatusCode::SERVICE_UNAVAILABLE, app.portal.unavailable()).into_response(),
    }
}

#[derive(Deserialize)]
struct Params {
    lat: f64,
    lon: f64,
    key: Option<String>,
    debug: Option<String>,
    dataset: Option<String>,
}

/// An answer of the dataset `at` of `served`, which a blocking thread gave
/// (`answered`), named by the headers X-Geocoder-Build and
/// X-Geocoder-Dataset.
fn tagged(served: &Served, at: usize, answered: Result<Response, tokio::task::JoinError>) -> Response {
    let mut resp = match answered {
        Ok(resp) => resp,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("the lookup failed: {e}")).into_response(),
    };
    let h = resp.headers_mut();
    if let Ok(build) = axum::http::HeaderValue::from_str(&served.plan.date) {
        h.insert("x-geocoder-build", build);
    }
    if let Ok(dataset) = axum::http::HeaderValue::from_str(&served.plan.datasets[at].id) {
        h.insert("x-geocoder-dataset", dataset);
    }
    h.insert("access-control-expose-headers", "x-geocoder-build, x-geocoder-dataset".parse().unwrap());
    resp
}

/// /reverse of a dataset, checked as query-server checks it, answered on a
/// blocking thread: a lookup that waits on cold pages holds up no other.
async fn reverse(State(app): State<Arc<App>>, ConnectInfo(peer): ConnectInfo<SocketAddr>, Query(p): Query<Params>) -> Response {
    if let Err(resp) = handlers::check_reverse(&app.db, &app.limiter, p.key.as_deref(), p.lat, p.lon, peer.ip()) {
        return resp;
    }
    let (served, at) = match app.portal.dataset(p.dataset.as_deref()) {
        Ok(found) => found,
        Err(resp) => return resp,
    };
    let index = served.indexes[at].clone();
    let answered = tokio::task::spawn_blocking(move || handlers::reverse_answer(&index, p.lat, p.lon, p.debug.as_deref())).await;
    tagged(&served, at, answered)
}

/// /polygons of a dataset, as reverse.
async fn polygons(State(app): State<Arc<App>>, Query(p): Query<Params>) -> Response {
    if let Err(resp) = handlers::check_polygons(&app.db, p.key.as_deref(), p.lat, p.lon) {
        return resp;
    }
    let (served, at) = match app.portal.dataset(p.dataset.as_deref()) {
        Ok(found) => found,
        Err(resp) => return resp,
    };
    let index = served.indexes[at].clone();
    let answered = tokio::task::spawn_blocking(move || handlers::polygons_answer(&index, p.lat, p.lon)).await;
    tagged(&served, at, answered)
}

/// query-portal <data dir> [<bind address>]: serves at once (0.0.0.0:3000
/// by default), the keys of <data dir>/geocoder.json, and syncs in the
/// background for good.
pub async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data = PathBuf::from(args.get(1).map(String::as_str).unwrap_or("/data"));
    let bind = args.get(2).map(String::as_str).unwrap_or("0.0.0.0:3000");

    let db_path = data.join("geocoder.json");
    let db = match auth::Db::load(&db_path.to_string_lossy()) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };
    let portal = Arc::new(Portal::new(data, Config::from_env()));
    // The sync runs on a runtime of its own: a fetch hands its body to a
    // blocking decoder through a bounded channel, and a decoder held up by
    // the disk holds up the thread that feeds it, which must not be one
    // that answers requests.
    let syncing = portal.clone();
    std::thread::Builder::new()
        .name("portal-sync".to_string())
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("portal-sync")
                .enable_all()
                .build()
                .expect("sync runtime")
                .block_on(syncing.run())
        })
        .expect("sync thread");

    let app = router(portal, Arc::new(RwLock::new(db)));
    eprintln!("Starting HTTP server on {}...", bind);
    let listener = tokio::net::TcpListener::bind(bind).await.unwrap();
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
}
