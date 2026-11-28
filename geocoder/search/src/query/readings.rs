//! Nominatim's own readings of a few queries (testdata/readings.txt): their word tokens, which
//! seed an in-memory term source, the direction penalty and every search each reading built.
use crate::index::terms::{MemTerms, TermInfo, TermKind};

const READINGS: &str = include_str!("../../testdata/readings.txt");

/// One word token of a reading, as Nominatim's debug page lists it.
pub struct TraceToken {
    pub ttype: char,
    pub start: usize,
    pub end: usize,
    pub token: String,
    pub lookup_word: String,
    pub penalty: f64,
    pub count: u32,
    pub addr_count: u32,
    pub info: String,
}

pub struct Reading {
    pub q: String,
    pub label: String,
    pub tokens: Vec<TraceToken>,
    pub direction: f64,
    /// Every search, each its table rows, cells joined by " | " and trimmed.
    pub searches: Vec<Vec<String>>,
}

impl Reading {
    /// The terms its tokens were looked up as, with their counts. A row whose info names a lookup
    /// word was made from a name of another spelling, so its word is set apart; postcodes and
    /// special phrases are the reading's own, so they are none.
    pub fn terms(&self) -> MemTerms {
        let mut terms = MemTerms::default();
        for (i, t) in self.tokens.iter().enumerate() {
            let kind = match t.ttype {
                'w' => TermKind::Partial,
                'W' => TermKind::Word,
                'H' => TermKind::HouseNumber,
                'C' => TermKind::Country,
                _ => continue,
            };
            if t.ttype == 'w' && t.penalty >= crate::constants::PARTIAL_MISSING_PENALTY {
                continue;
            }
            let info = match self.info_value(t, "cc") {
                Some(cc) if kind == TermKind::Country => TermInfo::Country(cc.as_bytes().try_into().expect("a country code")),
                _ => TermInfo::None,
            };
            let word = match self.info_value(t, "lookup") {
                Some(_) => format!("{}@{i}", t.lookup_word),
                None if kind == TermKind::Partial => String::new(),
                None => t.lookup_word.clone(),
            };
            let lookup = if kind == TermKind::Partial { String::new() } else { t.lookup_word.clone() };
            terms.insert(kind, &t.token, (&word, &lookup), (t.count, t.addr_count), info);
        }
        terms
    }

    fn info_value(&self, t: &TraceToken, key: &str) -> Option<String> {
        let at = t.info.find(&format!("'{key}': '"))? + key.len() + 5;
        Some(t.info[at..].split('\'').next()?.to_string())
    }
}

/// The readings, in file order.
pub fn readings() -> Vec<Reading> {
    let mut out: Vec<Reading> = Vec::new();
    for line in READINGS.lines().filter(|l| !l.starts_with('#')) {
        let f: Vec<&str> = line.split('\t').collect();
        match f[0] {
            "query" => out.push(Reading { q: f[1].into(), label: f[2].into(), tokens: Vec::new(), direction: 0.0, searches: Vec::new() }),
            "token" => {
                let r = out.last_mut().expect("a query first");
                r.tokens.push(TraceToken {
                    ttype: f[1].chars().next().expect("a type"),
                    start: f[2].parse().expect("a start"),
                    end: f[3].parse().expect("an end"),
                    token: f[4].into(),
                    lookup_word: f[5].into(),
                    penalty: f[6].parse().expect("a penalty"),
                    count: f[7].parse().expect("a count"),
                    addr_count: f[8].parse().expect("an addr_count"),
                    info: f.get(9).copied().unwrap_or("").into(),
                });
            }
            "direction" => out.last_mut().expect("a query first").direction = f[1].parse().expect("a direction"),
            "search" => {
                let r = out.last_mut().expect("a query first");
                let row = f[1].split(" | ").map(str::trim).collect::<Vec<_>>().join(" | ");
                if row.split(" | ").next() == Some("") {
                    r.searches.last_mut().expect("a search first").push(row);
                } else {
                    r.searches.push(vec![row]);
                }
            }
            _ => panic!("a line of no kind: {line}"),
        }
    }
    out
}

/// The reading labelled `label`.
pub fn reading(label: &str) -> Reading {
    readings().into_iter().find(|r| r.label == label).unwrap_or_else(|| panic!("no reading {label}"))
}
