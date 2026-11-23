// The test portal's feedback (portal.rs): POST /feedback takes the report of
// someone who disagrees with an answer the page showed (a reverse lookup's,
// a search's, or none: general) and what they expected, and appends it as
// one JSON line to <data>/feedback/<YYYY-MM-DD>.jsonl, the UTC day it came;
// GET /feedback/count tells how many came that day and in all, for the page
// to thank them. A report keeps only its own fields and when it came:
// nothing of who sent it, no IP and no email.

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use axum::extract::{ConnectInfo, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use super::{json, sync, App};
use crate::handlers;

/// The dir of the data dir the reports go into, a file a day.
pub const DIR: &str = "feedback";

/// The most bytes of a report's body: 413 past it.
pub const BODY_MAX: usize = 16 << 10;

/// The reports a client IP sends in RATE_WINDOW: 429 past them.
pub const RATE_PER_WINDOW: u32 = 20;
pub const RATE_WINDOW: Duration = Duration::from_secs(60);

/// The most characters of the contact a report may carry.
pub const CONTACT_MAX: usize = 200;

/// The IPs the rate keeps before it forgets those whose window is over.
const RATE_KEPT: usize = 10_000;

/// What a report is of.
#[derive(Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum Kind {
    /// The answer of a reverse lookup at the report's point.
    Reverse,
    /// The results of the search q, or the one of them at result_index.
    Search,
    /// No answer: the page, or the portal.
    General,
}

/// What the person expected: in their words, by the fields of an address,
/// or that the answer should be empty.
#[derive(Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
struct Expected {
    text: Option<String>,
    name: Option<String>,
    road: Option<String>,
    house_number: Option<String>,
    city: Option<String>,
    postcode: Option<String>,
    country: Option<String>,
    #[serde(default)]
    empty: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Point {
    lat: f64,
    lon: f64,
}

/// The page's link as it was: its columns (d=), its point (at=) and the
/// search box's text (q=).
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Page {
    #[serde(default)]
    columns: Vec<String>,
    point: Option<Point>,
    q: Option<String>,
}

/// A report as the page sends it, and as its line keeps it: when it came
/// (`received`, which the page can't send) first.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Report {
    #[serde(skip_deserializing)]
    received: String,
    kind: Kind,
    lat: Option<f64>,
    lon: Option<f64>,
    q: Option<String>,
    /// The result of the search's answer the report is of, from 0; none for
    /// the results as a whole.
    result_index: Option<u32>,
    /// The dataset that answered, and the build (its publication ID).
    dataset: Option<String>,
    build: Option<String>,
    /// The JSON the page showed, or the start of it as a string where it
    /// was too long to send (answer_trimmed).
    #[serde(default)]
    answer: serde_json::Value,
    #[serde(default)]
    answer_trimmed: bool,
    #[serde(default)]
    expected: Expected,
    comment: Option<String>,
    /// How to reach the person who sent it, if they gave a name or contact
    /// (the page remembers it in their browser): to ask them more later.
    contact: Option<String>,
    page: Option<Page>,
}

fn said(s: &Option<String>) -> bool {
    s.as_deref().is_some_and(|s| !s.trim().is_empty())
}

/// Why `r` is no report: a point of one coordinate or off the globe, a
/// reverse's without its point, a search's without its query, a result of
/// another kind, or nothing of what was expected nor a comment.
fn check(r: &Report) -> Result<(), String> {
    match (r.lat, r.lon) {
        (Some(lat), Some(lon)) if !query_server::coords_in_range(lat, lon) => {
            return Err("lat must be within ±90 and lon within ±180".to_string())
        }
        (Some(_), None) | (None, Some(_)) => return Err("a report names its point by lat and lon together".to_string()),
        _ => {}
    }
    if r.kind == Kind::Reverse && r.lat.is_none() {
        return Err("a reverse report names its point (lat, lon)".to_string());
    }
    if r.kind == Kind::Search && !said(&r.q) {
        return Err("a search report names its query (q)".to_string());
    }
    if r.kind != Kind::Search && r.result_index.is_some() {
        return Err("result_index is a search report's".to_string());
    }
    if r.contact.as_deref().is_some_and(|c| c.chars().count() > CONTACT_MAX) {
        return Err(format!("contact must be at most {CONTACT_MAX} characters"));
    }
    let e = &r.expected;
    if !(e.empty || [&e.text, &e.name, &e.road, &e.house_number, &e.city, &e.postcode, &e.country, &r.comment].into_iter().any(said)) {
        return Err("a report says what was expected or comments".to_string());
    }
    Ok(())
}

/// A rate of `per_window` a window of each client IP.
pub struct PerIp {
    per_window: u32,
    window: Duration,
    /// Each IP's window, when it began, and its count in it.
    seen: Mutex<HashMap<IpAddr, (Instant, u32)>>,
}

impl PerIp {
    pub fn new(per_window: u32, window: Duration) -> PerIp {
        PerIp { per_window, window, seen: Mutex::new(HashMap::new()) }
    }

    /// Whether `ip` may send one more at `now`, counted if so.
    pub fn allow(&self, ip: IpAddr, now: Instant) -> bool {
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        if seen.len() >= RATE_KEPT {
            seen.retain(|_, (began, _)| now.saturating_duration_since(*began) < self.window);
        }
        let (began, count) = seen.entry(ip).or_insert((now, 0));
        if now.saturating_duration_since(*began) >= self.window {
            (*began, *count) = (now, 0);
        }
        if *count >= self.per_window {
            return false;
        }
        *count += 1;
        true
    }
}

/// The reports of a data dir.
pub struct Feedback {
    /// <data>/feedback.
    dir: PathBuf,
    /// The reports of each day, by its date, once count has read the files:
    /// a report is written while it is held, so that two lines never
    /// interleave, and counted where they have been read.
    days: Mutex<Option<BTreeMap<String, u64>>>,
    pub rate: PerIp,
}

impl Feedback {
    pub fn new(dir: PathBuf) -> Feedback {
        Feedback { dir, days: Mutex::new(None), rate: PerIp::new(RATE_PER_WINDOW, RATE_WINDOW) }
    }

    /// Appends `report` to the file of its day, as one line in one write,
    /// the dir made where there is none.
    fn append(&self, report: &Report) -> Result<(), String> {
        let mut line = serde_json::to_vec(report).map_err(|e| format!("the report as JSON: {e}"))?;
        line.push(b'\n');
        let date = &report.received[..10];
        let mut days = self.days.lock().unwrap_or_else(|e| e.into_inner());
        std::fs::create_dir_all(&self.dir).map_err(|e| format!("create {}: {e}", self.dir.display()))?;
        let path = self.dir.join(format!("{date}.jsonl"));
        let mut file = std::fs::OpenOptions::new().create(true).append(true).open(&path).map_err(|e| format!("open {}: {e}", path.display()))?;
        file.write_all(&line).map_err(|e| format!("write {}: {e}", path.display()))?;
        if let Some(days) = days.as_mut() {
            *days.entry(date.to_string()).or_default() += 1;
        }
        Ok(())
    }

    /// The reports of the UTC day of `now`, and of every day: the lines of
    /// the files, read at the first count.
    fn count(&self, now: SystemTime) -> Result<(u64, u64), String> {
        let mut days = self.days.lock().unwrap_or_else(|e| e.into_inner());
        if days.is_none() {
            *days = Some(self.read()?);
        }
        let days = days.as_ref().expect("read");
        let today = days.get(&sync::iso(now)[..10]).copied().unwrap_or(0);
        Ok((today, days.values().sum()))
    }

    /// The lines of each day's file.
    fn read(&self) -> Result<BTreeMap<String, u64>, String> {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
            Err(e) => return Err(format!("read {}: {e}", self.dir.display())),
        };
        let mut days = BTreeMap::new();
        for entry in entries {
            let entry = entry.map_err(|e| format!("read {}: {e}", self.dir.display()))?;
            let name = entry.file_name();
            let Some(date) = name.to_str().and_then(|n| n.strip_suffix(".jsonl")) else { continue };
            let bytes = std::fs::read(entry.path()).map_err(|e| format!("read {}: {e}", entry.path().display()))?;
            days.insert(date.to_string(), bytes.iter().filter(|&&b| b == b'\n').count() as u64);
        }
        Ok(days)
    }
}

#[derive(Deserialize)]
pub(super) struct KeyParam {
    key: Option<String>,
}

/// POST /feedback?key=: the key, as the other routes take it (401), the
/// client IP's rate (429), the body's size (413) and the report (400), then
/// 201 once its line is written.
pub(super) async fn submit(State(app): State<Arc<App>>, ConnectInfo(peer): ConnectInfo<SocketAddr>, Query(p): Query<KeyParam>, body: axum::body::Body) -> Response {
    if let Err(resp) = handlers::require_valid_key(&app.db, p.key.as_deref()) {
        return resp;
    }
    if !app.feedback.rate.allow(peer.ip(), Instant::now()) {
        return (StatusCode::TOO_MANY_REQUESTS, format!("at most {RATE_PER_WINDOW} reports a minute: try again in a minute")).into_response();
    }
    let Ok(bytes) = axum::body::to_bytes(body, BODY_MAX).await else {
        return (StatusCode::PAYLOAD_TOO_LARGE, format!("a report is at most {BODY_MAX} bytes")).into_response();
    };
    let mut report: Report = match serde_json::from_slice(&bytes) {
        Ok(report) => report,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("not a report: {e}")).into_response(),
    };
    if let Err(why) = check(&report) {
        return (StatusCode::BAD_REQUEST, why).into_response();
    }
    report.received = sync::iso(SystemTime::now());
    let feedback = app.feedback.clone();
    match tokio::task::spawn_blocking(move || feedback.append(&report)).await {
        Ok(Ok(())) => json(StatusCode::CREATED, r#"{"status":"received"}"#.to_string()),
        Ok(Err(why)) => (StatusCode::INTERNAL_SERVER_ERROR, why).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("the write failed: {e}")).into_response(),
    }
}

/// GET /feedback/count, of no key: {"today": <the reports of the UTC day>,
/// "total": <of every day>}.
pub(super) async fn count(State(app): State<Arc<App>>) -> Response {
    let feedback = app.feedback.clone();
    match tokio::task::spawn_blocking(move || feedback.count(SystemTime::now())).await {
        Ok(Ok((today, total))) => json(StatusCode::OK, serde_json::json!({ "today": today, "total": total }).to_string()),
        Ok(Err(why)) => (StatusCode::INTERNAL_SERVER_ERROR, why).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("the count failed: {e}")).into_response(),
    }
}
