// The portal's feedback (feedback.rs): POST /feedback's reports, each a
// line of the file of the UTC day it came under <data>/feedback, and GET
// /feedback/count. Feedback needs no build: the portals of these tests serve
// none, and never ask their bucket.

use std::net::IpAddr;
use std::time::{Instant, SystemTime};

use super::*;
use crate::portal::feedback::{PerIp, BODY_MAX, CONTACT_MAX, DIR, RATE_PER_WINDOW, RATE_WINDOW};

/// A portal of the data dir `data`, serving no build, at its base URL.
async fn serving(data: &Path) -> String {
    serve(router(portal(data, config_at("http://127.0.0.1:9", Arc::new(load_dataset))), db())).await
}

/// A report of a reverse answer, as the page sends it.
fn report() -> serde_json::Value {
    serde_json::json!({
        "kind": "reverse",
        "lat": 48.8584,
        "lon": 2.2945,
        "dataset": "full.simplified.all",
        "build": "2026-11-20-9bc04e7d1a25",
        "answer": { "display_name": "Avenue Anatole France, Paris", "address": { "road": "Avenue Anatole France", "city": "Paris" } },
        "answer_trimmed": false,
        "expected": { "text": "the tower itself", "name": "Tour Eiffel", "house_number": "5" },
        "comment": "the tower is right there",
        "page": { "columns": ["with-addresses", "smallest"], "point": { "lat": 48.8584, "lon": 2.2945 }, "q": "eiffel" },
    })
}

/// POSTs `body` to /feedback of the portal at `url` with `key` (none: no
/// key=).
async fn post(url: &str, key: Option<&str>, body: impl Into<reqwest::Body>) -> Got {
    let at = match key {
        Some(key) => format!("{url}/feedback?key={key}"),
        None => format!("{url}/feedback"),
    };
    let resp = reqwest::Client::new().post(at).header("content-type", "application/json").body(body).send().await.unwrap();
    let (status, headers) = (resp.status().as_u16(), resp.headers().clone());
    let content_type = headers.get("content-type").map(|v| v.to_str().unwrap().to_string());
    Got { status, content_type, headers, body: resp.text().await.unwrap() }
}

/// The UTC date now.
fn today() -> String {
    sync::iso(SystemTime::now())[..10].to_string()
}

/// The lines of the feedback files of the data dir `data`, by file name.
fn lines(data: &Path) -> BTreeMap<String, Vec<String>> {
    let Ok(dir) = std::fs::read_dir(data.join(DIR)) else { return BTreeMap::new() };
    dir.map(|e| {
        let e = e.unwrap();
        let text = std::fs::read_to_string(e.path()).unwrap();
        assert!(text.ends_with('\n'), "{text}");
        (e.file_name().into_string().unwrap(), text.lines().map(str::to_string).collect())
    })
    .collect()
}

// A report is one JSON line of the file of the UTC day it came: when it
// came, then what the page sent, every field named (null where it sent
// none), and nothing of who sent it.
#[tokio::test]
async fn a_report_is_a_line_of_its_days_file_with_when_it_came_and_nothing_of_who_sent_it() {
    let data = ScratchDir::new();
    let url = serving(data.path()).await;

    let before = today();
    let got = post(&url, Some("test"), report().to_string()).await;
    let after = today();

    assert_eq!(got.status, 201, "{}", got.body);
    let files = lines(data.path());
    assert_eq!(files.len(), 1, "{files:?}");
    let (name, lines) = files.into_iter().next().unwrap();
    assert!(name == format!("{before}.jsonl") || name == format!("{after}.jsonl"), "{name}");
    assert_eq!(lines.len(), 1);
    assert!(!lines[0].contains("127.0.0.1"), "{}", lines[0]);
    let mut line: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
    let received = line["received"].take();
    let received = received.as_str().unwrap();
    assert!(received.starts_with(&name[..10]) && received.ends_with('Z') && received.len() == 20, "{received}");
    assert_eq!(line, serde_json::json!({
        "received": null,
        "kind": "reverse",
        "lat": 48.8584,
        "lon": 2.2945,
        "q": null,
        "result_index": null,
        "dataset": "full.simplified.all",
        "build": "2026-11-20-9bc04e7d1a25",
        "answer": { "display_name": "Avenue Anatole France, Paris", "address": { "road": "Avenue Anatole France", "city": "Paris" } },
        "answer_trimmed": false,
        "expected": { "text": "the tower itself", "name": "Tour Eiffel", "road": null, "house_number": "5", "city": null, "postcode": null,
                      "country": null, "empty": false },
        "comment": "the tower is right there",
        "contact": null,
        "page": { "columns": ["with-addresses", "smallest"], "point": { "lat": 48.8584, "lon": 2.2945 }, "q": "eiffel" },
    }));
    assert_eq!(lines[0].split("\",\"").next().unwrap(), format!("{{\"received\":\"{received}"), "received comes first");
}

// A report keeps the name or contact its sender gave, to ask them more
// later; one past CONTACT_MAX characters is 400 and writes nothing.
#[tokio::test]
async fn a_report_keeps_the_contact_its_sender_gave() {
    let data = ScratchDir::new();
    let url = serving(data.path()).await;
    let mut with = report();
    with["contact"] = serde_json::json!("ana@example.org");
    let mut long = report();
    long["contact"] = serde_json::json!("x".repeat(CONTACT_MAX + 1));

    assert_eq!(post(&url, Some("test"), long.to_string()).await.status, 400);
    assert!(lines(data.path()).is_empty());
    let got = post(&url, Some("test"), with.to_string()).await;

    assert_eq!(got.status, 201, "{}", got.body);
    let line: serde_json::Value = serde_json::from_str(&lines(data.path()).into_values().next().unwrap()[0]).unwrap();
    assert_eq!(line["contact"], "ana@example.org");
}

// Reports sent at once are each a whole line of their own.
#[tokio::test]
async fn reports_sent_at_once_are_each_a_whole_line() {
    let data = ScratchDir::new();
    let url = serving(data.path()).await;
    let sent: Vec<_> = (0..RATE_PER_WINDOW)
        .map(|n| {
            let mut r = report();
            r["comment"] = format!("report {n} {}", "x".repeat(4000)).into();
            let url = url.clone();
            tokio::spawn(async move { post(&url, Some("test"), r.to_string()).await.status })
        })
        .collect();
    for s in sent {
        assert_eq!(s.await.unwrap(), 201);
    }

    let comments: BTreeSet<String> = lines(data.path())
        .into_values()
        .flatten()
        .map(|l| serde_json::from_str::<serde_json::Value>(&l).unwrap()["comment"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(comments, (0..RATE_PER_WINDOW).map(|n| format!("report {n} {}", "x".repeat(4000))).collect());
}

// /feedback/count, of no key, tells the reports of the UTC day and those of
// every day, the volume's files counted at its first ask and each report
// after it added.
#[tokio::test]
async fn the_count_tells_todays_reports_and_every_days() {
    let empty = ScratchDir::new();
    let got = fetch(&format!("{}/feedback/count", serving(empty.path()).await)).await;
    assert_eq!((got.status, got.content_type.as_deref()), (200, Some("application/json")));
    assert_eq!(got.json(), serde_json::json!({ "today": 0, "total": 0 }));

    let data = ScratchDir::new();
    std::fs::create_dir_all(data.path().join(DIR)).unwrap();
    std::fs::write(data.path().join(DIR).join("2026-01-01.jsonl"), "{}\n{}\n").unwrap();
    let url = serving(data.path()).await;
    let count = || async { fetch(&format!("{url}/feedback/count")).await.json() };
    assert_eq!(count().await, serde_json::json!({ "today": 0, "total": 2 }));

    assert_eq!(post(&url, Some("test"), report().to_string()).await.status, 201);

    assert_eq!(count().await, serde_json::json!({ "today": 1, "total": 3 }));
}

// A body that isn't a report is a 400 that says why, and writes nothing: no
// JSON, another kind, a field a report hasn't (nothing but its own is
// kept), a reverse without its point, a point off the globe, a search
// without its query, a result of another kind than a search's, a report
// that says nothing of what was expected, a field of another type. A
// general one that only comments is one, as is a search's of a result.
#[tokio::test]
async fn a_body_that_isnt_a_report_is_400_and_writes_nothing() {
    let data = ScratchDir::new();
    let url = serving(data.path()).await;
    fn edited(edit: impl FnOnce(&mut serde_json::Value)) -> String {
        let mut r = report();
        edit(&mut r);
        r.to_string()
    }
    let bad = [
        ("{".to_string(), "not a report: "),
        (edited(|r| r["kind"] = "other".into()), "not a report: unknown variant `other`"),
        (edited(|r| r["ip"] = "203.0.113.9".into()), "not a report: unknown field `ip`"),
        (edited(|r| r["expected"]["email"] = "a@b.c".into()), "not a report: unknown field `email`"),
        (edited(|r| r["lat"] = serde_json::Value::Null), "a report names its point by lat and lon together"),
        (edited(|r| { r["lat"] = serde_json::Value::Null; r["lon"] = serde_json::Value::Null; }), "a reverse report names its point"),
        (edited(|r| r["lat"] = serde_json::json!(91)), "lat must be within ±90 and lon within ±180"),
        (edited(|r| r["kind"] = "search".into()), "a search report names its query (q)"),
        (edited(|r| r["result_index"] = serde_json::json!(0)), "result_index is a search report's"),
        (edited(|r| { r["expected"] = serde_json::json!({ "text": " " }); r["comment"] = "".into(); }),
         "a report says what was expected or comments"),
        (edited(|r| r["comment"] = serde_json::json!(5)), "not a report: invalid type"),
    ];
    for (body, why) in bad {
        let got = post(&url, Some("test"), body.clone()).await;
        assert_eq!(got.status, 400, "{body}: {}", got.body);
        assert!(got.body.starts_with(why), "{body}: {}", got.body);
    }
    assert_eq!(lines(data.path()), BTreeMap::new());

    let general = serde_json::json!({ "kind": "general", "comment": "the search is fast" });
    let search = serde_json::json!({ "kind": "search", "q": "paris", "result_index": 2, "lat": 33.66, "lon": -95.55,
                                     "answer": [{ "name": "Paris" }], "expected": { "empty": true } });
    for body in [general, search] {
        assert_eq!(post(&url, Some("test"), body.to_string()).await.status, 201, "{body}");
    }
    assert_eq!(lines(data.path()).into_values().flatten().count(), 2);
}

// A body of BODY_MAX bytes is taken; one byte more is a 413, which writes
// nothing.
#[tokio::test]
async fn a_report_of_16_kib_is_taken_and_a_byte_more_is_413() {
    let data = ScratchDir::new();
    let url = serving(data.path()).await;
    let of = |bytes: usize| {
        let mut r = report();
        r["comment"] = "".into();
        let pad = bytes - r.to_string().len();
        r["comment"] = "x".repeat(pad).into();
        let body = r.to_string();
        assert_eq!(body.len(), bytes);
        body
    };
    assert_eq!(BODY_MAX, 16 << 10);

    let over = post(&url, Some("test"), of(BODY_MAX + 1)).await;
    assert_eq!((over.status, over.body.as_str()), (413, "a report is at most 16384 bytes"));
    assert_eq!(lines(data.path()), BTreeMap::new());
    assert_eq!(post(&url, Some("test"), of(BODY_MAX)).await.status, 201);
}

// A report takes the key the other routes do, and a client IP sends
// RATE_PER_WINDOW a minute, those of no key or a wrong one not counted:
// the next is a 429, which writes nothing.
#[tokio::test]
async fn a_report_takes_the_key_and_a_client_sends_20_a_minute() {
    let data = ScratchDir::new();
    let url = serving(data.path()).await;
    assert_eq!(post(&url, None, report().to_string()).await.status, 401);
    assert_eq!(post(&url, Some("wrong"), report().to_string()).await.status, 401);
    assert_eq!(RATE_PER_WINDOW, 20);

    for n in 0..RATE_PER_WINDOW {
        assert_eq!(post(&url, Some("test"), report().to_string()).await.status, 201, "report {n}");
    }
    let limited = post(&url, Some("test"), report().to_string()).await;

    assert_eq!((limited.status, limited.body.as_str()), (429, "at most 20 reports a minute: try again in a minute"));
    assert_eq!(lines(data.path()).into_values().flatten().count(), RATE_PER_WINDOW as usize);
}

// Each IP has a rate of its own, which comes back once its minute is over.
#[test]
fn each_ip_has_a_rate_of_its_own_back_after_its_minute() {
    let limiter = PerIp::new(RATE_PER_WINDOW, RATE_WINDOW);
    let (a, b): (IpAddr, IpAddr) = ("203.0.113.1".parse().unwrap(), "2001:db8::1".parse().unwrap());
    let start = Instant::now();

    for n in 0..RATE_PER_WINDOW {
        assert!(limiter.allow(a, start + Duration::from_secs(n as u64)), "{n}");
    }

    assert!(!limiter.allow(a, start + Duration::from_secs(59)));
    assert!(limiter.allow(b, start + Duration::from_secs(59)));
    assert!(limiter.allow(a, start + RATE_WINDOW));
}
