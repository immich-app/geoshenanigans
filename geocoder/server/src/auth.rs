use axum::extract::{Form, Path, State};
use axum::http::header::SET_COOKIE;
use axum::http::HeaderMap;
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Write;
use std::fs;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

// --- Data model ---

#[derive(Serialize, Deserialize, Clone)]
pub struct User {
    password_hash: String,
    admin: bool,
    rate_per_second: u32,
    rate_per_day: u32,
    #[serde(default)]
    rate_by_ip: bool,
}

pub struct RateState {
    second_count: AtomicU32,
    second_ts: AtomicU64,
    day_count: AtomicU32,
    day_ts: AtomicU32,
}

impl Default for RateState {
    fn default() -> Self {
        Self {
            second_count: AtomicU32::new(0),
            second_ts: AtomicU64::new(0),
            day_count: AtomicU32::new(0),
            day_ts: AtomicU32::new(0),
        }
    }
}

pub type RateLimiter = RwLock<HashMap<String, Arc<RateState>>>;

#[derive(Serialize, Deserialize, Default)]
pub struct Db {
    users: HashMap<String, User>,
    tokens: HashMap<String, String>, // token -> login
    #[serde(skip)]
    sessions: HashMap<String, String>, // session_id -> login (in-memory only)
    #[serde(skip)]
    path: String,
}

impl Db {
    pub fn load(path: &str) -> Self {
        let mut db = match fs::read_to_string(path) {
            Ok(data) => serde_json::from_str(&data).unwrap_or_default(),
            Err(_) => Db::default(),
        };
        db.path = path.to_string();
        db
    }

    fn save(&self) {
        let data = serde_json::to_string_pretty(self).unwrap();
        fs::write(&self.path, data).expect("Failed to save database");
    }

    fn create_user(&mut self, login: &str, password: &str, admin: bool, rate_per_second: u32, rate_per_day: u32, rate_by_ip: bool) {
        let hash = bcrypt::hash(password, bcrypt::DEFAULT_COST).expect("bcrypt hash failed");
        self.users.insert(login.to_string(), User {
            password_hash: hash,
            admin,
            rate_per_second,
            rate_per_day,
            rate_by_ip,
        });
        self.save();
    }

    pub fn validate_token(&self, key: &str) -> Option<(String, u32, u32, bool)> {
        let login = self.tokens.get(key)?;
        let user = self.users.get(login)?;
        Some((login.clone(), user.rate_per_second, user.rate_per_day, user.rate_by_ip))
    }
}

pub fn check_rate(limiter: &RateLimiter, login: &str, rate_per_second: u32, rate_per_day: u32) -> Result<(), &'static str> {
    let state = {
        let map = limiter.read().unwrap();
        if let Some(s) = map.get(login) {
            Arc::clone(s)
        } else {
            drop(map);
            let mut map = limiter.write().unwrap();
            Arc::clone(map.entry(login.to_string()).or_default())
        }
    };

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let now_secs = now.as_secs();
    let now_day = (now_secs / 86400) as u32;

    if state.second_ts.load(Relaxed) != now_secs {
        state.second_ts.store(now_secs, Relaxed);
        state.second_count.store(0, Relaxed);
    }
    if state.day_ts.load(Relaxed) != now_day {
        state.day_ts.store(now_day, Relaxed);
        state.day_count.store(0, Relaxed);
    }

    if rate_per_second > 0 && state.second_count.fetch_add(1, Relaxed) >= rate_per_second {
        return Err("Rate limit exceeded (per second)");
    }
    if rate_per_day > 0 && state.day_count.fetch_add(1, Relaxed) >= rate_per_day {
        return Err("Rate limit exceeded (per day)");
    }

    Ok(())
}

fn random_hex(len: usize) -> String {
    let mut bytes = vec![0u8; len];
    getrandom::getrandom(&mut bytes).expect("getrandom failed");
    let mut s = String::with_capacity(len * 2);
    for b in &bytes {
        write!(s, "{:02x}", b).unwrap();
    }
    s
}

fn get_session_cookie(headers: &HeaderMap) -> Option<String> {
    headers
        .get_all("cookie")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(';'))
        .map(|s| s.trim())
        .find_map(|s| s.strip_prefix("session=").map(|v| v.to_string()))
}

fn set_session_cookie(value: &str) -> ([(axum::http::header::HeaderName, String); 1],) {
    ([(SET_COOKIE, format!("session={}; Path=/; HttpOnly", value))],)
}

fn clear_session_cookie() -> ([(axum::http::header::HeaderName, String); 1],) {
    ([(SET_COOKIE, "session=; Path=/; HttpOnly; Max-Age=0".to_string())],)
}

// --- Router ---

pub fn router() -> Router<Arc<RwLock<Db>>> {
    Router::new()
        .route("/", get(dashboard))
        .route("/login", get(login_page).post(login_submit))
        .route("/logout", get(logout))
        .route("/tokens", post(create_token))
        .route("/tokens/{token}/delete", get(delete_token))
        .route("/users", post(create_user_handler))
        .route("/users/{login}/delete", get(delete_user))
}

// --- Handlers ---

#[derive(Deserialize)]
struct LoginForm {
    login: String,
    password: String,
}

async fn login_page(headers: HeaderMap, state: State<Arc<RwLock<Db>>>) -> Response {
    if let Some(session_id) = get_session_cookie(&headers) {
        let db = state.read().unwrap();
        if db.sessions.contains_key(&session_id) {
            return Redirect::to("/").into_response();
        }
    }
    Html(LOGIN_HTML).into_response()
}

async fn login_submit(state: State<Arc<RwLock<Db>>>, Form(form): Form<LoginForm>) -> Response {
    let mut db = state.write().unwrap();

    // First login ever — create admin account
    if db.users.is_empty() {
        db.create_user(&form.login, &form.password, true, 0, 0, false);
    }

    if let Some(user) = db.users.get(&form.login) {
        if bcrypt::verify(&form.password, &user.password_hash).unwrap_or(false) {
            let session_id = random_hex(32);
            db.sessions.insert(session_id.clone(), form.login);
            let (cookie,) = set_session_cookie(&session_id);
            return (cookie, Redirect::to("/")).into_response();
        }
    }
    Html(LOGIN_HTML.replace("</form>", "<small><ins>Invalid credentials</ins></small></form>"))
        .into_response()
}

async fn logout(headers: HeaderMap, state: State<Arc<RwLock<Db>>>) -> Response {
    if let Some(session_id) = get_session_cookie(&headers) {
        let mut db = state.write().unwrap();
        db.sessions.remove(&session_id);
    }
    let (cookie,) = clear_session_cookie();
    (cookie, Redirect::to("/login")).into_response()
}

async fn dashboard(
    headers: HeaderMap,
    state: State<Arc<RwLock<Db>>>,
    limiter: axum::extract::Extension<Arc<RateLimiter>>,
) -> Response {
    let session_id = match get_session_cookie(&headers) {
        Some(s) => s,
        None => return Redirect::to("/login").into_response(),
    };

    let db = state.read().unwrap();
    let login = match db.sessions.get(&session_id) {
        Some(l) => l.clone(),
        None => return Redirect::to("/login").into_response(),
    };

    let is_admin = db.users.get(&login).map(|u| u.admin).unwrap_or(false);

    let mut rows = String::new();
    for (token, owner) in &db.tokens {
        if owner == &login {
            rows.push_str(&format!(
                "<tr><td><code>{}</code></td><td><a href=\"/tokens/{}/delete\">Delete</a></td></tr>",
                html_escape(token),
                html_escape(token),
            ));
        }
    }

    let mut user_rows = String::new();
    if is_admin {
        for (ulogin, user) in &db.users {
            let role = if user.admin { "Admin" } else { "User" };
            let delete = if ulogin != &login {
                format!("<a href=\"/users/{}/delete\">Delete</a>", html_escape(ulogin))
            } else {
                String::new()
            };
            let by_ip = if user.rate_by_ip { "Yes" } else { "" };
            user_rows.push_str(&format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                html_escape(ulogin), role, user.rate_per_second, user.rate_per_day, by_ip, delete
            ));
        }
    }

    let now_day = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() / 86400) as u32;
    let daily_usage = {
        let map = limiter.read().unwrap();
        map.get(&login).map(|s| {
            if s.day_ts.load(Relaxed) == now_day { s.day_count.load(Relaxed) } else { 0 }
        }).unwrap_or(0)
    };

    let html = DASHBOARD_HTML
        .replace("{user}", &html_escape(&login))
        .replace("{rows}", &rows)
        .replace("{user_rows}", &user_rows)
        .replace("{daily_usage}", &daily_usage.to_string())
        .replace("{users_display}", if is_admin { "" } else { "display:none" });
    Html(html).into_response()
}

async fn create_token(headers: HeaderMap, state: State<Arc<RwLock<Db>>>) -> Response {
    let session_id = match get_session_cookie(&headers) {
        Some(s) => s,
        None => return Redirect::to("/login").into_response(),
    };

    let mut db = state.write().unwrap();
    if let Some(login) = db.sessions.get(&session_id).cloned() {
        let token = random_hex(16);
        db.tokens.insert(token, login);
        db.save();
    }
    Redirect::to("/").into_response()
}

async fn delete_token(
    headers: HeaderMap,
    state: State<Arc<RwLock<Db>>>,
    Path(token): Path<String>,
) -> Response {
    let session_id = match get_session_cookie(&headers) {
        Some(s) => s,
        None => return Redirect::to("/login").into_response(),
    };

    let mut db = state.write().unwrap();
    if let Some(login) = db.sessions.get(&session_id) {
        if db.tokens.get(&token).map(|o| o == login).unwrap_or(false) {
            db.tokens.remove(&token);
            db.save();
        }
    }
    Redirect::to("/").into_response()
}

#[derive(Deserialize)]
struct CreateUserForm {
    login: String,
    password: String,
    rate_per_second: u32,
    rate_per_day: u32,
    #[serde(default)]
    rate_by_ip: Option<String>,
}

async fn create_user_handler(headers: HeaderMap, state: State<Arc<RwLock<Db>>>, Form(form): Form<CreateUserForm>) -> Response {
    let session_id = match get_session_cookie(&headers) {
        Some(s) => s,
        None => return Redirect::to("/login").into_response(),
    };

    let mut db = state.write().unwrap();
    if let Some(login) = db.sessions.get(&session_id).cloned() {
        let is_admin = db.users.get(&login).map(|u| u.admin).unwrap_or(false);
        if is_admin && !form.login.is_empty() && !form.password.is_empty() {
            db.create_user(&form.login, &form.password, false, form.rate_per_second, form.rate_per_day, form.rate_by_ip.as_deref() == Some("on"));
        }
    }
    Redirect::to("/").into_response()
}

async fn delete_user(
    headers: HeaderMap,
    state: State<Arc<RwLock<Db>>>,
    Path(target): Path<String>,
) -> Response {
    let session_id = match get_session_cookie(&headers) {
        Some(s) => s,
        None => return Redirect::to("/login").into_response(),
    };

    let mut db = state.write().unwrap();
    if let Some(login) = db.sessions.get(&session_id).cloned() {
        let is_admin = db.users.get(&login).map(|u| u.admin).unwrap_or(false);
        if is_admin && target != login {
            db.users.remove(&target);
            db.tokens.retain(|_, owner| owner != &target);
            db.save();
        }
    }
    Redirect::to("/").into_response()
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

const LOGIN_HTML: &str = include_str!("web/login.html");
const DASHBOARD_HTML: &str = include_str!("web/dashboard.html");
