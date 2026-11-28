//! Compare built searches to Nominatim before execution or the debug renderer's truncation.
//! Only the fixture knows database word ids. The planner sees ordinary terms and counts.
use super::plan::*;
use crate::index::entities::Category;
use crate::index::terms::{MemTerms, TermInfo, TermKind};
use crate::profile::{MatchMode, Profile, SearchRequest};
use serde_json::{json, Value};
use std::collections::HashMap;

fn millis(value: f64) -> i64 {
    (value * 1000.0).round() as i64
}

fn array(v: &Value) -> &[Value] {
    v.as_array().unwrap()
}

fn string(v: &Value) -> &str {
    v.as_str().unwrap()
}

type WordIds = HashMap<(TermKind, String, String, TermInfo), u64>;

fn terms(rows: &Value) -> (MemTerms, WordIds) {
    let mut terms = MemTerms::default();
    let mut ids = HashMap::new();
    for row in array(rows) {
        let kind = match string(&row[0]) {
            "w" => TermKind::Partial,
            "W" => TermKind::Word,
            "C" => TermKind::Country,
            "H" => TermKind::HouseNumber,
            other => panic!("unexpected dictionary term {other}"),
        };
        let (token, word, lookup) = (string(&row[1]), string(&row[2]), string(&row[3]));
        let info = row[6].as_str().map_or(TermInfo::None, |cc| TermInfo::Country(cc.as_bytes().try_into().unwrap()));
        let counts = (row[4].as_u64().unwrap() as u32, row[5].as_u64().unwrap() as u32);
        terms.insert(kind, token, (word, lookup), counts, info.clone());
        ids.insert((kind, token.into(), word.into(), info), row[7].as_u64().unwrap_or_default());
    }
    (terms, ids)
}

fn weighted<T>(out: &mut Value, key: &str, items: &Weighted<T>, render: impl Fn(&T) -> Value) {
    if !items.0.is_empty() {
        let mut pairs: Vec<_> = items.0.iter().map(|(v, p)| json!([render(v), millis(*p)])).collect();
        pairs.sort_by_key(Value::to_string);
        out[key] = Value::Array(pairs);
    }
}

fn column(f: Field) -> &'static str {
    match f {
        Field::Name => "name_vector",
        Field::Address | Field::NameAndAddress => "nameaddress_vector",
    }
}

fn describe(s: &Search, ids: &[u64]) -> Value {
    let kind = match s {
        Search::Country { .. } => "Country",
        Search::Place { .. } => "Place",
        Search::Address { .. } => "Address",
        Search::Postcode { .. } => "Postcode",
        Search::Near { .. } => "Near",
        Search::Poi { .. } => "Poi",
    };
    let mut out = json!({"kind": kind, "penalty": millis(s.penalty())});
    let country = |cc: &[u8; 2]| json!(std::str::from_utf8(cc).unwrap());
    let category = |c: &Category| json!([c.class, c.typ]);
    let data = match s {
        Search::Near { categories, base, .. } => {
            weighted(&mut out, "categories", categories, category);
            out["base"] = describe(base, ids);
            return out;
        }
        Search::Country { countries, .. } => {
            weighted(&mut out, "countries", countries, country);
            return out;
        }
        Search::Poi { qualifiers, countries, .. } => {
            weighted(&mut out, "countries", countries, country);
            weighted(&mut out, "qualifiers", qualifiers, category);
            return out;
        }
        Search::Place { expected_count, has_address_terms, data, .. } | Search::Address { expected_count, has_address_terms, data, .. } => {
            out["count"] = json!(expected_count);
            out["address"] = json!(has_address_terms);
            data
        }
        Search::Postcode { data, .. } => data,
    };
    weighted(&mut out, "postcodes", &data.postcodes, |v| json!(v));
    weighted(&mut out, "housenumbers", &data.housenumbers, |v| json!(v));
    weighted(&mut out, "countries", &data.countries, country);
    weighted(&mut out, "qualifiers", &data.qualifiers, category);
    let words = |terms: &[TermId]| terms.iter().map(|t| ids[t.0 as usize]).collect::<Vec<_>>();
    if !data.lookups.is_empty() {
        out["lookups"] = data
            .lookups
            .iter()
            .map(|l| {
                let op = match l.kind {
                    LookupKind::Restrict => "Restrict",
                    LookupKind::All => "LookupAll",
                    LookupKind::Any => "LookupAny",
                };
                json!([op, column(l.field), words(&l.terms)])
            })
            .collect();
    }
    if !data.rankings.is_empty() {
        out["rankings"] = data
            .rankings
            .iter()
            .map(|r| {
                let variants: Vec<_> = r.rankings.iter().map(|v| json!([millis(v.penalty), words(&v.terms)])).collect();
                json!([column(r.field), millis(r.default), variants])
            })
            .collect();
    }
    out
}

fn replay(r: &Value) -> Value {
    let (terms, ids) = terms(&r["terms"]);
    let mut req = SearchRequest::new(string(&r["q"]), Profile::Full);
    req.mode = MatchMode::Nominatim;
    req.countries = array(&r["countries"]).iter().map(|cc| string(cc).as_bytes().try_into().unwrap()).collect();
    match read(&req, &terms, Pass::First) {
        Err(_) => json!("too long"),
        Ok(plan) if plan.reverse => json!("reverse"),
        Ok(plan) => {
            let ids: Vec<_> = plan
                .terms
                .iter()
                .map(|t| ids.get(&(t.kind, t.token.clone(), t.word.clone(), t.info.clone())).copied().unwrap_or(0))
                .collect();
            plan.searches.iter().map(|s| describe(s, &ids)).collect()
        }
    }
}

// Every alternative of a given penalty scores the same. SQL evaluates lookup
// operands as a set. Keep the order of the searches, fields and distinct penalties.
fn canonical(searches: &mut Value) {
    fn one(s: &mut Value) {
        if let Some(lookups) = s.get_mut("lookups").and_then(Value::as_array_mut) {
            for lookup in lookups {
                lookup[2].as_array_mut().unwrap().sort_by_key(|v| v.as_u64().unwrap());
            }
        }
        if let Some(rankings) = s.get_mut("rankings").and_then(Value::as_array_mut) {
            for ranking in rankings {
                let variants = ranking[2].as_array_mut().unwrap();
                for tie in variants.chunk_by_mut(|a, b| a[0] == b[0]) {
                    tie.sort_by_key(Value::to_string);
                }
            }
        }
        if let Some(base) = s.get_mut("base") {
            one(base);
        }
    }
    if let Some(list) = searches.as_array_mut() {
        for s in list {
            one(s);
        }
    }
}

fn expected(r: &Value) -> Value {
    if !r["error"].is_null() {
        return r["error"].clone();
    }
    if r["reverse"] == true {
        return json!("reverse");
    }
    r["searches"].clone()
}

#[test]
fn builds_the_oracles_searches_in_order() {
    for line in include_str!("../../testdata/searches.jsonl").lines() {
        let r: Value = serde_json::from_str(line).unwrap();
        let mut actual = replay(&r);
        let mut want = expected(&r);
        canonical(&mut actual);
        canonical(&mut want);
        assert_eq!(actual, want, "{}", r["q"]);
    }
}

#[test]
#[ignore = "reads the external oracle sample"]
fn compare_oracle_sample() {
    use std::io::{BufRead, BufReader, Write};
    let corpus = std::fs::File::open(std::env::var("SEARCH_TRACE_SAMPLE").unwrap()).unwrap();
    let mut differences = std::fs::File::create(std::env::var("SEARCH_TRACE_DIFFERENCES").unwrap()).unwrap();
    let mut total = 0;
    let mut exact_lookups = 0;
    let mut full = 0;
    fn lookup_list(searches: &Value) -> Value {
        fn fields(s: &Value) -> Value {
            json!([s["kind"], s["penalty"], s["lookups"], s.get("base").map(fields)])
        }
        searches.as_array().map_or_else(|| searches.clone(), |v| v.iter().map(fields).collect())
    }
    for line in BufReader::new(corpus).lines() {
        let r: Value = serde_json::from_str(&line.unwrap()).unwrap();
        total += 1;
        let actual = replay(&r);
        let want = expected(&r);
        if lookup_list(&actual) == lookup_list(&want) {
            exact_lookups += 1;
        }
        let mut canonical_actual = actual.clone();
        let mut canonical_want = want.clone();
        canonical(&mut canonical_actual);
        canonical(&mut canonical_want);
        if canonical_actual == canonical_want {
            full += 1;
        }
        if actual != want {
            writeln!(differences, "{}", json!({"id":r["id"],"q":r["q"],"actual":actual})).unwrap();
        }
    }
    println!("{exact_lookups}/{total} ordered lookup lists, {full}/{total} including rankings, filters and expected counts");
    assert!(total > 0 && exact_lookups * 100 >= total * 95);
}

#[test]
fn public_term_frequencies_reproduce_the_printed_searches() {
    use super::readings::reading;
    for name in ["futo-public", "liberty-public"] {
        let r = reading(name);
        let mut req = SearchRequest::new(&r.q, Profile::Full);
        req.mode = MatchMode::Nominatim;
        let plan = read(&req, &r.terms(), Pass::First).unwrap();
        let normalized_lookup = |lookup: &str| {
            let (start, terms) = lookup.split_once("[[").unwrap();
            let mut words: Vec<_> = terms.strip_suffix("])").unwrap().split(",[").collect();
            words.sort_unstable();
            format!("{start}[[{}])", words.join(",["))
        };
        let labels = |terms: &[TermId]| {
            let words: Vec<_> = terms
                .iter()
                .map(|id| {
                    let t = &plan.terms[id.0 as usize];
                    let k = if t.kind == TermKind::Partial { 'P' } else { t.kind.letter() };
                    format!("[{k}]{}", t.lookup_word)
                })
                .collect();
            words.join(",")
        };
        let mut expected: Vec<_> = r
            .searches
            .iter()
            .map(|rows| {
                let values: Vec<Vec<_>> = rows.iter().map(|row| row.split(" | ").collect()).collect();
                let lookups = values.iter().map(|row| row[1].to_owned()).filter(|v| !v.is_empty()).collect::<Vec<_>>();
                let kind = if values.iter().any(|row| !row[2].is_empty()) {
                    "Address"
                } else if values.iter().any(|row| !row[3].is_empty()) && lookups.iter().all(|l| l.starts_with("Restrict")) {
                    "Postcode"
                } else {
                    "Place"
                };
                (values[0][0].parse::<f64>().unwrap(), kind, lookups)
            })
            .collect();
        // The assignment tables precede the stable (penalty, priority) sort.
        expected.sort_by(|a, b| a.0.total_cmp(&b.0).then((a.1 == "Postcode").cmp(&(b.1 == "Postcode"))));
        assert_eq!(plan.searches.len(), expected.len());
        for (search, (printed, kind, lookups)) in plan.searches.iter().zip(expected) {
            // The page rounds to three significant digits.
            let tolerance = 0.5 * 10.0f64.powi(printed.log10().floor() as i32 - 2);
            assert!((search.penalty() - printed).abs() <= tolerance + 1e-12);
            let (actual, data) = match search {
                Search::Address { data, .. } => ("Address", data),
                Search::Postcode { data, .. } => ("Postcode", data),
                Search::Place { data, .. } => ("Place", data),
                _ => panic!("unexpected search for {}", r.q),
            };
            assert_eq!(actual, kind);
            let actual: Vec<_> = data
                .lookups
                .iter()
                .map(|l| {
                    let op = match l.kind {
                        LookupKind::All => "LookupAll",
                        LookupKind::Any => "LookupAny",
                        LookupKind::Restrict => "Restrict",
                    };
                    format!("{op}({}[{}])", column(l.field), labels(&l.terms))
                })
                .collect();
            assert_eq!(
                actual.iter().map(|v| normalized_lookup(v)).collect::<Vec<_>>(),
                lookups.iter().map(|v| normalized_lookup(v)).collect::<Vec<_>>(),
                "{} at {printed}",
                r.q
            );
        }
    }
}
