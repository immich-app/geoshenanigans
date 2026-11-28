// The test portal's server (the query-portal binary, a bin of place-search:
// search/src/portal_main.rs). It keeps the whole planet build in
// <data>/builds, synced from the bucket by itself (sync.rs), and serves each
// selection of it the manifest's constraints allow (catalog.rs) as a
// dataset: /reverse and /polygons with dataset=<id> answer what a
// query-server answers from that selection's files (handlers.rs), /search
// with dataset=<id> what the node binding's searchPlaces answers from them
// (search.rs), /datasets lists the datasets and the combinations the
// constraints exclude, /health tells the build served (by its publication
// ID, sync::publication_id), how the sync goes and how each place search
// index does, /reverse with water=<value> answers a dataset's address with
// the water of the build's water dir of that value (catalog::Water: one
// loaded once for every dataset, as a dataset with that water dir answers
// it; all by default, where the build has it), POST /feedback keeps the reports of those who disagree with
// an answer (feedback.rs), and /test is the page. Beside the build's
// datasets, /datasets lists the column of what Immich stores today
// (immich.rs: its reverse geocoding over the geodata of its release image,
// which the portal's image copies), which /reverse answers with
// dataset=immich-cities500, build or none. Without a build to serve it
// starts all the same: those routes answer 503 with the reason while it
// syncs; /feedback takes reports all the same. So it does on a host without
// CA certificates, which the sync's fetches check the bucket's against: it
// serves the build its volume holds, and each round fails, naming why, until
// they are there.

// By path: query-portal, a bin of place-search, takes this file in by
// #[path], from which a plain `mod` would look beside it.
#[path = "portal/catalog.rs"]
pub mod catalog;
#[path = "portal/feedback.rs"]
pub mod feedback;
#[path = "portal/immich.rs"]
pub mod immich;
#[path = "portal/search.rs"]
pub mod search;
#[path = "portal/search_params.rs"]
mod search_params;
#[path = "portal/sync.rs"]
pub mod sync;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};

use crate::{auth, handlers};
use self::search_params::SearchParams;
use query_server::{MultiIndex, WaterLayer, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL};

/// The build version of the files this server reads, the build.version of
/// configurations.template.json (a test here and
/// builder/tools/test_build_search_indexes.py hold it to that): it syncs and
/// serves builds of this version only.
pub const BUILD_VERSION: u32 = 25;

/// Where the builds are published: latest.json and each build's files
/// under builds/.
pub const BASE_URL: &str = "https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder";

/// The lookups the portal runs at once (Config::lookups_at_once): as many
/// as a query-server on a host of 8 cores or more answers at once, one a
/// worker. A /polygons of a dataset of exact boundaries builds the JSON of
/// every vertex of the rings around its point, hundreds of MB in a big
/// country, so the lookups are capped, not only the page's requests.
pub const LOOKUPS_AT_ONCE: usize = 8;

/// The searches the portal runs at once (Config::searches_at_once), beside
/// the lookups: a few /polygons of exact boundaries, seconds each, can hold
/// every lookup's place, and a search as one types must not wait on them.
/// A search takes 1-35 ms warm.
pub const SEARCHES_AT_ONCE: usize = 4;

/// The place search indexes a sync builds at once (PORTAL_SEARCH_JOBS), one thread per build.
pub const SEARCH_JOBS: usize = 4;

/// How a dataset's dir loads into a MultiIndex.
pub type Loader = Arc<dyn Fn(&Path) -> Result<MultiIndex, String> + Send + Sync>;

/// How a water dir's dir of links loads: into its water layer.
pub type WaterLoader = Arc<dyn Fn(&Path) -> Result<WaterLayer, String> + Send + Sync>;

/// How the portal syncs and loads.
pub struct Config {
    /// The bucket (BASE_URL).
    pub base_url: String,
    /// The regions of each published build to serve.
    pub regions: Vec<String>,
    /// The time between two sync rounds.
    pub interval: Duration,
    /// The time after a round that failed before the next.
    pub retry_after: Duration,
    /// The pauses before each new try of an object (or a JSON file) whose
    /// fetch failed: as many tries more as it has pauses.
    pub retry_delays: Vec<Duration>,
    /// How many objects a sync fetches at once.
    pub fetches_at_once: usize,
    /// How many lookups (/reverse, /polygons) run at once, over every
    /// client: the others wait their turn.
    pub lookups_at_once: usize,
    /// How many searches (/search) run at once, over every client, beside
    /// the lookups.
    pub searches_at_once: usize,
    /// How a dataset's dir loads (load_dataset).
    pub load: Loader,
    /// How a water dir loads (load_water).
    pub load_water: WaterLoader,
    /// How a dataset's place search opens over the index of its group
    /// (search::open_search).
    pub open_search: search::Opener,
    /// How many place search indexes a sync builds at once.
    pub search_jobs: usize,
    /// The bytes free to this process in the file system of a dir.
    pub free_space: fn(&Path) -> std::io::Result<u64>,
    /// The geodata of the Immich column (immich.rs), if the portal has it.
    pub immich: Option<immich::Source>,
}

impl Config {
    /// The portal's: the bucket of PORTAL_BASE_URL (BASE_URL by default), a
    /// round every PORTAL_SYNC_SECONDS (3600 by default), PORTAL_SEARCH_JOBS
    /// index builds at once (search_jobs).
    pub fn from_env() -> Config {
        let seconds = std::env::var("PORTAL_SYNC_SECONDS").ok().and_then(|v| v.parse::<u64>().ok()).filter(|&s| s > 0).unwrap_or(3600);
        let cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
        Config {
            base_url: std::env::var("PORTAL_BASE_URL").unwrap_or_else(|_| BASE_URL.to_string()),
            regions: std::env::var("PORTAL_REGIONS").ok().map_or_else(catalog::default_regions, |value| {
                value.split(',').map(str::trim).filter(|name| !name.is_empty()).map(str::to_string).collect()
            }),
            interval: Duration::from_secs(seconds),
            retry_after: Duration::from_secs(seconds.min(600)),
            retry_delays: [5, 30, 120].map(Duration::from_secs).to_vec(),
            fetches_at_once: 8,
            lookups_at_once: LOOKUPS_AT_ONCE,
            searches_at_once: SEARCHES_AT_ONCE,
            load: Arc::new(load_dataset),
            load_water: Arc::new(load_water),
            open_search: Arc::new(search::open_search),
            search_jobs: search_jobs(std::env::var("PORTAL_SEARCH_JOBS").ok().as_deref(), cpus),
            free_space,
            immich: immich::Source::from_env(),
        }
    }
}

/// The index builds a sync runs at once: `wanted` (PORTAL_SEARCH_JOBS),
/// SEARCH_JOBS where it is none or no count above 0, and no more than the
/// `cpus` this process may run on (its CPU affinity and its cgroup's
/// quota, std::thread::available_parallelism).
pub fn search_jobs(wanted: Option<&str>, cpus: usize) -> usize {
    let wanted = wanted.and_then(|v| v.trim().parse::<usize>().ok()).filter(|&n| n > 0).unwrap_or(SEARCH_JOBS);
    wanted.min(cpus.max(1))
}

/// The dataset in the dir `dir` (the links to one selection's files), as a
/// query-server serving that dir loads it. An error where it has no
/// loaded regions.
pub fn load_dataset(dir: &Path) -> Result<MultiIndex, String> {
    let at = dir.to_str().ok_or_else(|| format!("{} is no UTF-8 path", dir.display()))?;
    let index = MultiIndex::load(at, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE)?;
    if index.loaded_count() == 0 {
        return Err(format!("{at} holds no admin_cells.bin"));
    }
    Ok(index)
}

/// The water layer in the dir `dir` (the links to one water dir's files), as
/// a dataset's water/ dir holding them loads (query_server::WaterLayer::open,
/// its cells at the datasets' admin cell level). An error where it doesn't
/// load, or holds no water files.
pub fn load_water(dir: &Path) -> Result<WaterLayer, String> {
    let at = dir.to_str().ok_or_else(|| format!("{} is no UTF-8 path", dir.display()))?;
    WaterLayer::open(at, DEFAULT_ADMIN_CELL_LEVEL)?.ok_or_else(|| format!("{at} holds no water files"))
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
    /// Its publication ID (sync::publication_id): of the build as it was
    /// published, the same on every volume that holds it.
    pub id: String,
    /// Its dir under builds/ (`<date>-<n>`), the volume's: on another volume
    /// that dir may hold another publication of its date.
    pub dir: String,
    /// The sha256 of its configurations.json (sync::manifest_sha256).
    pub manifest_sha256: String,
    pub plan: catalog::Plan,
    /// Each dataset's index, as plan.datasets orders them.
    pub indexes: Vec<Arc<MultiIndex>>,
    /// Each water dir's layer, as plan.water orders them, which every
    /// dataset of its region answers with.
    pub water: Vec<Arc<WaterLayer>>,
    /// What /datasets answers.
    pub datasets_json: String,
    /// Each dataset's place search.
    pub search: Arc<search::Searches>,
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
    /// The builds the volume held at start that can't be served, each with
    /// why ("<dir> here can't be served: <why>"), one whose files don't hold
    /// their manifest's bytes among them; none once one is served.
    pub not_served: Vec<String>,
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
    /// The sha256 of its configurations.json, once the round fetched it:
    /// a build published again under its date is another.
    pub manifest_sha256: Option<String>,
}

pub struct Portal {
    /// The data dir: geocoder.json, builds/, the locks and the sync's
    /// records.
    pub data: PathBuf,
    pub config: Config,
    served: arc_swap::ArcSwapOption<Served>,
    status: Mutex<Status>,
    /// The build dirs this process found it can't serve (another build
    /// version, damaged, not loading): the prune keeps none of them, and
    /// forgets each it removes.
    unusable: Mutex<std::collections::BTreeSet<String>>,
    /// Whether a round of this process has run the start (sync.rs): one
    /// that failed on its lock, or on the list of the builds at its start,
    /// has not.
    started: std::sync::atomic::AtomicBool,
    /// A place for each lookup that may run (Config::lookups_at_once),
    /// held until its answer's body is sent or dropped (lookup).
    pub lookups: Arc<tokio::sync::Semaphore>,
    /// A place for each search that may run (Config::searches_at_once),
    /// held as a lookup's is.
    pub searches: Arc<tokio::sync::Semaphore>,
    /// The Immich column's geodata, loaded at its first use (immich()).
    immich: std::sync::OnceLock<Result<Arc<immich::Geodata>, String>>,
    /// The client of the sync's fetches, built by the first (http()).
    http: tokio::sync::OnceCell<reqwest::Client>,
}

impl Portal {
    pub fn new(data: PathBuf, config: Config) -> Portal {
        Portal {
            data,
            lookups: Arc::new(tokio::sync::Semaphore::new(config.lookups_at_once.max(1))),
            searches: Arc::new(tokio::sync::Semaphore::new(config.searches_at_once.max(1))),
            config,
            served: arc_swap::ArcSwapOption::empty(),
            status: Mutex::new(Status::default()),
            unusable: Mutex::new(Default::default()),
            started: Default::default(),
            immich: Default::default(),
            http: tokio::sync::OnceCell::new(),
        }
    }

    /// The client of the sync's fetches. Building one loads the system's CA
    /// certificates, which a host may lack, so the first fetch builds it,
    /// after the start has served the build the volume holds, and a portal
    /// without them still starts and serves. A client that can't be built
    /// fails the round, naming why, and the next round tries again.
    async fn http(&self) -> Result<&reqwest::Client, crate::downloader::DownloadError> {
        self.http
            .get_or_try_init(|| async {
                reqwest::Client::builder()
                    .user_agent("geocoder-portal/0.1")
                    .connect_timeout(Duration::from_secs(30))
                    .read_timeout(Duration::from_secs(120))
                    .build()
                    .map_err(crate::downloader::client_error)
            })
            .await
    }

    /// The build served, if any.
    pub fn served(&self) -> Option<Arc<Served>> {
        self.served.load_full()
    }

    /// The Immich column's geodata, loaded at the first call (seconds: a
    /// caller that may wait is on a blocking thread), or why it doesn't
    /// load; none where the portal has none.
    pub fn immich(&self) -> Option<Result<Arc<immich::Geodata>, String>> {
        let source = self.config.immich.as_ref()?;
        Some(self.immich.get_or_init(|| immich::Geodata::load(&source.dir).map(Arc::new)).clone())
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
        for skipped in &status.not_served {
            why.push_str(&format!("; build {skipped}"));
        }
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
    /// The reports of <data>/feedback.
    feedback: Arc<feedback::Feedback>,
}

/// The portal's routes: /health, /datasets, /reverse, /polygons, /search,
/// /feedback and /test, keys checked against `db`.
pub fn router(portal: Arc<Portal>, db: Arc<RwLock<auth::Db>>) -> Router {
    let feedback = Arc::new(feedback::Feedback::new(portal.data.join(feedback::DIR)));
    Router::new()
        .route("/health", get(health))
        .route("/datasets", get(datasets))
        .route("/reverse", get(reverse))
        .route("/polygons", get(polygons))
        .route("/search", get(search_places))
        .route("/feedback", post(feedback::submit))
        .route("/feedback/count", get(feedback::count))
        .route("/test", get(handlers::test_portal))
        .layer(axum::middleware::from_fn(handlers::cors_middleware))
        .with_state(Arc::new(App { portal, db, limiter: auth::RateLimiter::default(), feedback }))
}

fn json(status: StatusCode, body: String) -> Response {
    (status, [(axum::http::header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// 200 with the build served, its dataset count, how its place search
/// does (search::Searches::json) and the sync's status; 503 with the reason
/// and the sync's status while there is none. The status's build served is
/// the one this answer names, though a switch came between the two reads.
async fn health(State(app): State<Arc<App>>) -> Response {
    let (mut sync, served) = (app.portal.status(), app.portal.served());
    sync.served = served.as_ref().map(|s| s.dir.clone());
    match served {
        Some(served) => json(StatusCode::OK, serde_json::json!({
            "status": "ok",
            "build": { "date": served.plan.date, "version": served.plan.version, "id": served.id, "dir": served.dir },
            "datasets": served.plan.datasets.len(),
            "water": served.plan.water.iter().map(|w| w.set()).collect::<Vec<_>>(),
            "search": served.search.json(),
            "sync": sync,
        }).to_string()),
        None => json(StatusCode::SERVICE_UNAVAILABLE, serde_json::json!({
            "status": "unavailable",
            "reason": app.portal.unavailable(),
            "sync": sync,
        }).to_string()),
    }
}

/// The served build's datasets (catalog::Plan::json), and under "others"
/// the columns of no geocoder dataset: the Immich column where the portal
/// has its geodata (immich::Source::json); 503 while there is none.
async fn datasets(State(app): State<Arc<App>>) -> Response {
    match app.portal.served() {
        Some(served) => {
            let others: Vec<serde_json::Value> = app.portal.config.immich.iter().map(immich::Source::json).collect();
            match serde_json::from_str::<serde_json::Value>(&served.datasets_json) {
                Ok(mut listed) => {
                    listed["others"] = others.into();
                    json(StatusCode::OK, listed.to_string())
                }
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("the datasets' JSON doesn't read: {e}")).into_response(),
            }
        }
        None => (StatusCode::SERVICE_UNAVAILABLE, app.portal.unavailable()).into_response(),
    }
}

/// Whether `dataset` names the Immich column of a portal that has its
/// geodata.
fn is_immich(portal: &Portal, dataset: Option<&str>) -> bool {
    dataset == Some(immich::ID) && portal.config.immich.is_some()
}

/// /reverse of the Immich column: what Immich stores for a photo at the
/// point and how it found it (immich::Answer::json), checked as any
/// /reverse is, a lookup's place held while it is looked up, named by the
/// header X-Geocoder-Dataset alone (it is of no build); 503 where its
/// geodata doesn't load, with why.
async fn immich_reverse(app: &App, peer: SocketAddr, p: Params) -> Response {
    if let Err(resp) = handlers::check_reverse(&app.db, &app.limiter, p.key.as_deref(), p.lat, p.lon, peer.ip()) {
        return resp;
    }
    let Ok(place) = app.portal.lookups.clone().acquire_owned().await else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "the lookups are closed").into_response();
    };
    let portal = app.portal.clone();
    let answered = tokio::task::spawn_blocking(move || {
        let _place = place;
        let source = portal.config.immich.clone()?;
        Some(portal.immich()?.map(|geodata| geodata.reverse(p.lat, p.lon).json(&source).to_string()))
    })
    .await;
    let mut resp = match answered {
        Ok(Some(Ok(body))) => json(StatusCode::OK, body),
        Ok(Some(Err(why))) => (StatusCode::SERVICE_UNAVAILABLE, format!("the Immich geodata doesn't load: {why}")).into_response(),
        Ok(None) => (StatusCode::BAD_REQUEST, format!("this portal has no Immich geodata: no {}", immich::ID)).into_response(),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("the lookup failed: {e}")).into_response(),
    };
    let h = resp.headers_mut();
    h.insert("x-geocoder-dataset", axum::http::HeaderValue::from_static(immich::ID));
    h.insert("access-control-expose-headers", "x-geocoder-dataset".parse().unwrap());
    resp
}

/// What a route the Immich column has none of answers it: 400, why.
fn immich_lacks(what: &str) -> Response {
    (StatusCode::BAD_REQUEST, format!("{} ({}) has no {what}: it looks up points alone", immich::LABEL, immich::ID)).into_response()
}

#[derive(Deserialize)]
struct Params {
    lat: f64,
    lon: f64,
    key: Option<String>,
    debug: Option<String>,
    dataset: Option<String>,
    /// The water a /reverse answers with (Plan::water_of).
    water: Option<String>,
}

/// A lookup's answer's body, which holds the lookup's place (Portal::lookups)
/// until it is sent or dropped: the answer is in memory until then, the
/// JSON of a /polygons of exact boundaries hundreds of MB.
struct Holding {
    body: axum::body::Body,
    place: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl axum::body::HttpBody for Holding {
    type Data = axum::body::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let polled = axum::body::HttpBody::poll_frame(std::pin::Pin::new(&mut this.body), cx);
        if let std::task::Poll::Ready(None | Some(Err(_))) = polled {
            this.place = None;
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        axum::body::HttpBody::is_end_stream(&self.body)
    }

    fn size_hint(&self) -> http_body::SizeHint {
        axum::body::HttpBody::size_hint(&self.body)
    }
}

/// The answer of `answer` from the dataset `at` of `served`, on a blocking
/// thread once a place of the portal's lookups is free (Portal::lookups):
/// a lookup that waits on cold pages holds up no other request, and at most
/// Config::lookups_at_once run. A lookup holds its place until its answer's
/// body is sent or dropped (Holding), and until the lookup ends where its
/// client has gone before. Named by the headers X-Geocoder-Build (the
/// build's date), X-Geocoder-Build-Id (its publication ID, as /health and
/// /datasets name it: a build published again under its date is another,
/// whatever dir it is in), X-Geocoder-Build-Dir (its dir on this volume)
/// and X-Geocoder-Dataset.
pub async fn lookup(portal: &Portal, served: Arc<Served>, at: usize, answer: impl FnOnce(&MultiIndex) -> Response + Send + 'static) -> Response {
    let index = served.indexes[at].clone();
    held(&portal.lookups, &served, at, move || answer(&index)).await
}

/// The answer `answer` makes of the dataset `at` of `served`, as lookup
/// makes one, once a place of `places` is free.
async fn held(places: &Arc<tokio::sync::Semaphore>, served: &Served, at: usize, answer: impl FnOnce() -> Response + Send + 'static) -> Response {
    let Ok(place) = places.clone().acquire_owned().await else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "the lookups are closed").into_response();
    };
    let answered = tokio::task::spawn_blocking(move || (answer(), place)).await;
    let mut resp = match answered {
        Ok((resp, place)) => {
            let (parts, body) = resp.into_parts();
            Response::from_parts(parts, axum::body::Body::new(Holding { body, place: Some(place) }))
        }
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("the lookup failed: {e}")).into_response(),
    };
    let h = resp.headers_mut();
    if let Ok(build) = axum::http::HeaderValue::from_str(&served.plan.date) {
        h.insert("x-geocoder-build", build);
    }
    if let Ok(id) = axum::http::HeaderValue::from_str(&served.id) {
        h.insert("x-geocoder-build-id", id);
    }
    if let Ok(dir) = axum::http::HeaderValue::from_str(&served.dir) {
        h.insert("x-geocoder-build-dir", dir);
    }
    if let Ok(dataset) = axum::http::HeaderValue::from_str(&served.plan.datasets[at].id) {
        h.insert("x-geocoder-dataset", dataset);
    }
    h.insert("access-control-expose-headers", "x-geocoder-build, x-geocoder-build-id, x-geocoder-build-dir, x-geocoder-dataset".parse().unwrap());
    resp
}

/// The answer of /reverse at (lat, lon) from `index` with the water of
/// `water`, as a dataset whose water/ dir holds those files answers it
/// (query_server::Address's water: last, absent where no water holds the
/// point, and never in an answer of no continent): query-server's
/// (handlers::reverse_answer), its water field that of `water`, its landmark
/// decided as the server decides it (MultiIndex::query_json_with_water),
/// debug=all's result's too. The other debug answers tell nothing of water.
pub fn reverse_with_water(index: &MultiIndex, water: Option<&WaterLayer>, lat: f64, lon: f64, debug: Option<&str>) -> Response {
    let Some(water) = water.filter(|_| matches!(debug, None | Some("all"))) else {
        return handlers::reverse_answer(index, lat, lon, debug);
    };
    let json_answer = |json: String| ([(axum::http::header::CONTENT_TYPE, "application/json")], json).into_response();
    if debug.is_some() {
        // As reverse_answer's debug=all: of the first continent that
        // bbox-matches the point.
        let arcs = index.loaded_matches(lat, lon);
        let Some(idx) = arcs.first() else { return handlers::reverse_answer(index, lat, lon, debug) };
        let mut address = idx.query(lat, lon);
        if address.water.is_none() {
            address.water = water.at(lat, lon);
        }
        let combined = serde_json::json!({
            "result": serde_json::to_value(&address).unwrap_or_default(),
            "admin_polygons": idx.debug_admin(lat, lon),
            "primary_features": idx.debug_primary(lat, lon),
            "place_nodes": idx.debug_places(lat, lon),
        });
        return json_answer(serde_json::to_string_pretty(&combined).unwrap_or_default());
    }
    json_answer(index.query_json_with_water(lat, lon, Some(water)))
}

/// /reverse of a dataset, checked as query-server checks it, answered as a
/// lookup with the water water= names (Plan::water_of: all by default, where
/// the build has it), named by the header X-Geocoder-Water (none, or the
/// water dir's value). The dataset and the water are found first: a request
/// the portal can't answer (no build served, a dataset or water it doesn't
/// offer) takes nothing of its key's rate, and tells nothing /datasets
/// doesn't.
async fn reverse(State(app): State<Arc<App>>, ConnectInfo(peer): ConnectInfo<SocketAddr>, Query(p): Query<Params>) -> Response {
    if is_immich(&app.portal, p.dataset.as_deref()) {
        return immich_reverse(&app, peer, p).await;
    }
    let (served, at) = match app.portal.dataset(p.dataset.as_deref()) {
        Ok(found) => found,
        Err(resp) => return resp,
    };
    let water = match served.plan.water_of(at, p.water.as_deref()) {
        Ok(water) => water,
        Err(why) => return (StatusCode::BAD_REQUEST, why).into_response(),
    };
    if let Err(resp) = handlers::check_reverse(&app.db, &app.limiter, p.key.as_deref(), p.lat, p.lon, peer.ip()) {
        return resp;
    }
    let named = water.map_or(catalog::NO_WATER.to_string(), |w| served.plan.water[w].id.clone());
    let layer = water.map(|w| served.water[w].clone());
    let mut resp = lookup(&app.portal, served, at, move |index| reverse_with_water(index, layer.as_deref(), p.lat, p.lon, p.debug.as_deref())).await;
    let h = resp.headers_mut();
    if let Ok(named) = axum::http::HeaderValue::from_str(&named) {
        h.insert("x-geocoder-water", named);
    }
    h.insert(
        "access-control-expose-headers",
        "x-geocoder-build, x-geocoder-build-id, x-geocoder-build-dir, x-geocoder-dataset, x-geocoder-water".parse().unwrap(),
    );
    resp
}

/// /polygons of a dataset, as reverse.
async fn polygons(State(app): State<Arc<App>>, Query(p): Query<Params>) -> Response {
    if is_immich(&app.portal, p.dataset.as_deref()) {
        return immich_lacks("polygons");
    }
    let (served, at) = match app.portal.dataset(p.dataset.as_deref()) {
        Ok(found) => found,
        Err(resp) => return resp,
    };
    if let Err(resp) = handlers::check_polygons(&app.db, p.key.as_deref(), p.lat, p.lon) {
        return resp;
    }
    lookup(&app.portal, served, at, move |index| handlers::polygons_answer(index, p.lat, p.lon)).await
}

/// Searches an open dataset on a blocking worker after validation and rate checking.
async fn search_places(
    State(app): State<Arc<App>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(p): Query<SearchParams>,
) -> Response {
    if is_immich(&app.portal, p.dataset.as_deref()) {
        return immich_lacks("place search");
    }
    let (served, at) = match app.portal.dataset(p.dataset.as_deref()) {
        Ok(found) => found,
        Err(resp) => return resp,
    };
    let (id, index) = (served.plan.datasets[at].id.clone(), served.search.index_of(at).to_string());
    let searcher = match served.search.search(at) {
        Ok(searcher) => searcher,
        Err(unready) => {
            let (status, since, reason) = match unready {
                search::Unready::Building { since } => ("building", since, None),
                search::Unready::Failed { since, reason } => ("failed", since, Some(reason)),
            };
            let mut body = serde_json::json!({ "status": status, "dataset": id, "index": index, "since": since });
            if let Some(reason) = reason {
                body["reason"] = reason.into();
            }
            let mut resp = json(StatusCode::SERVICE_UNAVAILABLE, body.to_string());
            if status == "building" {
                resp.headers_mut().insert(axum::http::header::RETRY_AFTER, axum::http::HeaderValue::from_static("5"));
            }
            return resp;
        }
    };
    let (login, rps, rpd, by_ip) = match handlers::require_valid_key(&app.db, p.key.as_deref()) {
        Ok(key) => key,
        Err(resp) => return resp,
    };
    let (req, debug) = match p.into_request(&headers) {
        Ok(parsed) => parsed,
        Err(why) => return (StatusCode::BAD_REQUEST, why).into_response(),
    };
    if let Err(why) = searcher.require_supported(&req) {
        return (StatusCode::BAD_REQUEST, why.to_string()).into_response();
    }
    let rate_key = if by_ip { format!("{login}:{}", peer.ip()) } else { login };
    if let Err(msg) = auth::check_rate(&app.limiter, &rate_key, rps, rpd) {
        return (StatusCode::TOO_MANY_REQUESTS, msg).into_response();
    }
    let mut resp = held(&app.portal.searches, &served, at, move || {
        let started = std::time::Instant::now();
        let (found, stats) = match searcher.search(&req) {
            Ok(answer) => answer,
            Err(refused) => return (StatusCode::BAD_REQUEST, refused.to_string()).into_response(),
        };
        let body = if debug {
            place_search::query::debug_line(&req.q, started.elapsed().as_secs_f64() * 1e6, &found, &stats).to_string()
        } else {
            place_search::query::results_json(&found)
        };
        json(StatusCode::OK, body)
    })
    .await;
    let h = resp.headers_mut();
    if let Ok(index) = axum::http::HeaderValue::from_str(&index) {
        h.insert("x-geocoder-search-index", index);
    }
    h.insert(
        "access-control-expose-headers",
        "x-geocoder-build, x-geocoder-build-id, x-geocoder-build-dir, x-geocoder-dataset, x-geocoder-search-index".parse().unwrap(),
    );
    resp
}

/// query-portal <data dir> [<bind address>]: serves at once (0.0.0.0:3000
/// by default), the keys of <data dir>/geocoder.json, and syncs in the
/// background for good. It holds the data dir's lock (sync::hold_volume)
/// for as long as it runs, and exits at start while another holds it.
pub async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data = PathBuf::from(args.get(1).map(String::as_str).unwrap_or("/data"));
    let bind = args.get(2).map(String::as_str).unwrap_or("0.0.0.0:3000");

    let _volume = match sync::hold_volume(&data) {
        Ok(lock) => lock,
        Err(why) => {
            eprintln!("Error: {why}");
            std::process::exit(1);
        }
    };

    let db_path = data.join("geocoder.json");
    let db = match auth::Db::load(&db_path.to_string_lossy()) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };
    let config = Config::from_env();
    if let Err(why) = catalog::validate_regions(&config.regions) {
        eprintln!("Error: PORTAL_REGIONS: {why}");
        std::process::exit(1);
    }
    let portal = Arc::new(Portal::new(data, config));
    // The Immich column's geodata loads at start, beside the build's, so
    // that its first lookup doesn't wait for it.
    let loading = portal.clone();
    std::thread::Builder::new()
        .name("portal-immich".to_string())
        .spawn(move || match loading.immich() {
            Some(Ok(geodata)) => eprintln!(
                "Immich geodata: {} places ({} lines left out), {} Natural Earth rings",
                geodata.places(),
                geodata.skipped,
                geodata.rings()
            ),
            Some(Err(why)) => eprintln!("Immich geodata doesn't load: {why}"),
            None => eprintln!("no Immich geodata: no {} column", immich::ID),
        })
        .expect("immich thread");
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
