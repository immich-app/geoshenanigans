//! Query: the prototype's semantics (typo and prefix lookups on an fst, trailing query words as
//! context, importance ranking, de-duplication, settlements before same-named areas) over the
//! mapped index. Context is not stored: a candidate's country / state / county / city come from
//! reverse geocoding its point with the geocoder itself, only for the candidates that need it.
use crate::build::prefix_text;
use crate::constants::*;
use crate::model::*;
use crate::normalize::{is_ideographic, join, normalize, tokens, Token};
use crate::entities::{cc_of, polygon_raw, Entities};
use crate::reader::{SearchIndex, SubIndex};
use query_server::{cell_id_at_level, AdminPoint, AdminPolygon, Index, ID_MASK, INTERIOR_FLAG, NO_DATA};
use fst::{Automaton, Streamer};
use levenshtein_automata::{Distance, LevenshteinAutomatonBuilder, DFA, SINK_STATE};
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

pub struct Searcher<'a, 'g> {
    pub idx: &'a SearchIndex<'g>,
    lev: [LevenshteinAutomatonBuilder; 2],
    limits: Limits,
}

/// How much a search holds and does: the constants', but for tests that make them small.
#[derive(Clone, Copy, Debug)]
struct Limits {
    /// CANDIDATE_POOL
    pool: usize,
    /// MAX_CONTEXT_CHECKS
    context_checks: usize,
    /// MAX_PENDING_CANDIDATES
    pending: usize,
    /// AREA_CHECK_TOP
    area_top: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits { pool: CANDIDATE_POOL, context_checks: MAX_CONTEXT_CHECKS, pending: MAX_PENDING_CANDIDATES, area_top: AREA_CHECK_TOP }
    }
}

#[derive(Clone, Debug)]
pub struct Hit {
    pub entity: u32,
    pub ord: u8,
    pub class: u8,
    pub score: f32,
    pub text: f32,
    pub context: bool,
    /// The query split that produced the hit (index into the search's phrases).
    pub split: u8,
}

/// A result's context for display: the geocoder's admin lookup at its point.
pub struct Ctx<'g> {
    pub cc: [u8; 2],
    pub country: Cow<'g, str>,
    pub state: Cow<'g, str>,
    pub county: Cow<'g, str>,
    pub district: Cow<'g, str>,
}

pub struct Found<'g> {
    pub hit: Hit,
    pub name: String,
    pub lat: f32,
    pub lng: f32,
    pub kind: Kind,
    pub typ: String,
    pub ctx: Rc<Ctx<'g>>,
}

impl Found<'_> {
    /// The result as callers get it: the name that matched, the point, the state as admin1name,
    /// the county (else the district) as admin2name, the country and the type.
    pub fn to_json(&self) -> serde_json::Value {
        let c = &self.ctx;
        serde_json::json!({
            "name": self.name,
            "latitude": self.lat,
            "longitude": self.lng,
            "admin1name": c.state,
            "admin2name": if !c.county.is_empty() { &c.county } else { &c.district },
            "country": c.country,
            "country_code": std::str::from_utf8(&c.cc).unwrap_or(""),
            "type": self.typ,
        })
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct QueryStats {
    pub reverse_lookups: usize,
    pub context_checks: usize,
    pub keys_matched: usize,
    pub postings_read: usize,
}

impl std::ops::AddAssign for QueryStats {
    fn add_assign(&mut self, o: QueryStats) {
        self.reverse_lookups += o.reverse_lookups;
        self.context_checks += o.context_checks;
        self.keys_matched += o.keys_matched;
        self.postings_read += o.postings_read;
    }
}

/// A Levenshtein DFA whose first characters must match exactly (Elasticsearch's prefix_length).
struct Anchored<'a> {
    first: &'a [u8],
    dfa: &'a DFA,
}

impl Automaton for Anchored<'_> {
    type State = (u32, u32);
    fn start(&self) -> (u32, u32) {
        (0, self.dfa.initial_state())
    }
    fn is_match(&self, s: &(u32, u32)) -> bool {
        s.0 as usize == self.first.len() && matches!(self.dfa.distance(s.1), Distance::Exact(_))
    }
    fn can_match(&self, s: &(u32, u32)) -> bool {
        s.1 != SINK_STATE
    }
    fn accept(&self, s: &(u32, u32), b: u8) -> (u32, u32) {
        let at = s.0 as usize;
        if at < self.first.len() && b != self.first[at] {
            return (s.0, SINK_STATE);
        }
        ((at + 1).min(self.first.len()) as u32, self.dfa.transition(s.1, b))
    }
}

#[derive(Clone, Copy)]
struct Ord32(f32);
impl PartialEq for Ord32 {
    fn eq(&self, other: &Self) -> bool { self.cmp(other).is_eq() }
}
impl PartialOrd for Ord32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}
impl Eq for Ord32 {}
impl Ord for Ord32 {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&o.0)
    }
}

fn context_mask_key(name_id: u32, country_code: u16, admin_level: u8) -> (u32, u16, bool) {
    (name_id, country_code, admin_level == 2)
}

/// Best text score per matched fst value.
type KeyMatches = HashMap<u64, f32>;

fn allowed_edits(chars: usize, no_typo_max: usize, one_typo_max: usize, ideographic: bool) -> u8 {
    if ideographic || chars <= no_typo_max {
        0
    } else if chars <= one_typo_max {
        1
    } else {
        2
    }
}

fn typo_text(d: u8) -> f32 {
    match d {
        0 => 1.0,
        1 => TYPO1_TEXT,
        _ => TYPO2_TEXT,
    }
}

pub fn char_len(b: &[u8]) -> usize {
    b.iter().filter(|x| (**x & 0xC0) != 0x80).count()
}

fn damerau_le1(a: &str, b: &str) -> bool {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.len().abs_diff(b.len()) > 1 {
        return false;
    }
    let mut i = 0;
    while i < a.len() && i < b.len() && a[i] == b[i] {
        i += 1;
    }
    if i == a.len() && i == b.len() {
        return true;
    }
    let rest = |x: &[char], y: &[char]| x == y;
    if a.len() == b.len() {
        rest(&a[i + 1..], &b[i + 1..])
            || (i + 1 < a.len() && a[i] == b[i + 1] && a[i + 1] == b[i] && rest(&a[i + 2..], &b[i + 2..]))
    } else if a.len() > b.len() {
        rest(&a[i + 1..], &b[i..])
    } else {
        rest(&a[i..], &b[i + 1..])
    }
}

fn context_word_matches(alias: &str, w: &str, w_chars: usize) -> bool {
    alias == w
        || (w_chars >= CONTEXT_PREFIX_MIN_CHARS && alias.starts_with(w))
        || (w_chars >= CONTEXT_WORD_MIN_CHARS && alias.contains(' ') && alias.split(' ').any(|t| t == w))
        || (w_chars >= CONTEXT_TYPO_MIN_CHARS
            && alias.len().abs_diff(w.len()) <= 4
            && alias.chars().next() == w.chars().next()
            && damerau_le1(alias, w))
}

pub fn haversine_km(a: (f32, f32), b: (f32, f32)) -> f64 {
    let (la1, lo1, la2, lo2) =
        ((a.0 as f64).to_radians(), (a.1 as f64).to_radians(), (b.0 as f64).to_radians(), (b.1 as f64).to_radians());
    let h = ((la2 - la1) / 2.0).sin().powi(2) + la1.cos() * la2.cos() * ((lo2 - lo1) / 2.0).sin().powi(2);
    2.0 * 6371.0 * h.sqrt().asin()
}

const PHRASE_BIT: u64 = 1 << 63;

/// Context words of one query split; bit i of a mask is context word i, bit 63 the whole phrase.
struct Context<'q> {
    words: &'q [String],
    phrase: String,
}

impl Context<'_> {
    fn mask_of(&self, alias: &str) -> u64 {
        if alias.is_empty() {
            return 0;
        }
        let mut m = 0;
        if context_word_matches(alias, &self.phrase, self.phrase.chars().count()) {
            m |= PHRASE_BIT;
        }
        for (i, w) in self.words.iter().enumerate() {
            if context_word_matches(alias, w, w.chars().count()) {
                m |= 1 << i;
            }
        }
        m
    }

    fn codes_mask(&self, codes: &[String]) -> u64 {
        let mut m = 0;
        for code in codes {
            // codes match whole words only ("tx" is not the start of "texas")
            if *code == self.phrase {
                m |= PHRASE_BIT;
            }
            for (i, w) in self.words.iter().enumerate() {
                if code == w {
                    m |= 1 << i;
                }
            }
        }
        m
    }

    fn satisfied(&self, m: u64) -> bool {
        let all = (1u64 << self.words.len()) - 1;
        m & PHRASE_BIT != 0 || m & all == all
    }
}

struct Pool {
    best: HashMap<u32, Hit>,
    /// (current score, entity) of the best `size` distinct entities offered: an entity's
    /// improved score replaces its old one, so each counts once in both bounds
    scores: BTreeSet<(Ord32, u32)>,
    /// CANDIDATE_POOL
    size: usize,
    /// results the context cut-off keeps reachable (limit * CONTEXT_CUTOFF_FACTOR)
    top_k: usize,
    /// Entities taken in unread, held or evicted since, until the pool reads them
    /// (`read_held`).
    unread: Vec<u32>,
    /// Of those, the ones it had evicted when it read the ones it held, unread still, and its
    /// threshold then (`read_held`).
    set_aside: Vec<u32>,
    set_aside_at: f32,
}

impl Pool {
    fn new(size: usize, top_k: usize) -> Self {
        Pool { best: HashMap::new(), scores: BTreeSet::new(), size, top_k, unread: Vec::new(), set_aside: Vec::new(), set_aside_at: 0.0 }
    }

    fn threshold(&self) -> f32 {
        if self.scores.len() < self.size {
            0.0
        } else {
            self.scores.first().map(|s| s.0 .0).unwrap_or(0.0)
        }
    }

    /// Takes `h` in if it scores above the threshold and above its entity's hit, if any. An
    /// entity the pool does not hold enters only if `reads` says it is somewhere (one that is
    /// nowhere is left out), or, with `LATER`, unread (`read_held`).
    fn offer<const LATER: bool>(&mut self, h: Hit, reads: impl FnOnce(u32) -> bool) {
        if h.score <= self.threshold() {
            return;
        }
        match self.best.get(&h.entity) {
            Some(old) if old.score >= h.score => return,
            Some(old) => {
                self.scores.remove(&(Ord32(old.score), h.entity));
            }
            None if LATER => self.unread.push(h.entity),
            None if !reads(h.entity) => return,
            None => {}
        }
        self.scores.insert((Ord32(h.score), h.entity));
        if self.scores.len() > self.size {
            if let Some((_, evicted)) = self.scores.pop_first() {
                self.best.remove(&evicted);
            }
        }
        self.best.insert(h.entity, h);
    }

    /// Reads the entities it took in unread that it holds: false when one is nowhere
    /// (`Searcher::search`). Until then such an entity decides nothing while the pool has room,
    /// as nothing is turned away or evicted, and once it is full only which hits at or below its
    /// threshold the pool holds: above it, the pool holds the hits of one that reads first, and
    /// those entities. Once those it holds read, its scores are those of a pool that reads
    /// first, and so are its hits but for which ones take the places at its threshold: one it
    /// evicted, if nowhere, could have turned away a hit that ties there and then been evicted
    /// in the place of another. It sets those aside unread, with that threshold, until a hit at
    /// or below it is taken (`Searcher::search_once`). Meanwhile the scores alone decide which
    /// candidates a split with context words weighs and checks, and its hits keep the
    /// difference to those places: one that takes such a place from its entity's hit in one
    /// pool evicts another hit there in the other.
    fn read_held(&mut self, mut reads: impl FnMut(u32) -> bool) -> bool {
        let at = self.threshold();
        for e in std::mem::take(&mut self.unread) {
            if !self.best.contains_key(&e) {
                if self.set_aside.is_empty() {
                    self.set_aside_at = at;
                }
                self.set_aside.push(e);
            } else if !reads(e) {
                return false;
            }
        }
        true
    }

    /// No candidate scoring at most this can reach the results: the `top_k`-th best distinct
    /// entity's score.
    fn cutoff(&self) -> f32 {
        if self.top_k == 0 { return f32::INFINITY; }
        match self.scores.iter().nth_back(self.top_k - 1) {
            Some(s) => s.0 .0.max(self.threshold()),
            None => self.threshold(),
        }
    }
}

/// Per-search caches: one reverse lookup per entity at most.
struct State<'g> {
    ctx: HashMap<u32, Rc<Ctx<'g>>>,
    stats: QueryStats,
}

/// A candidate of a split with context words, before its (costly) context check.
#[derive(Clone, Copy)]
struct Cand {
    entity: u32,
    ord: u8,
    class: u8,
    text: f32,
    potential: f32,
}

#[cfg(test)]
thread_local! {
    /// The pending candidates the cut-backs of the thread have looked at (`Searcher::cut_back`).
    static CUT_BACK_LOOKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl<'a, 'g> Searcher<'a, 'g> {
    pub fn new(idx: &'a SearchIndex<'g>) -> Self {
        Searcher { idx, lev: [LevenshteinAutomatonBuilder::new(1, true), LevenshteinAutomatonBuilder::new(2, true)], limits: Limits::default() }
    }

    /// Display context of a point: the geocoder's admin lookup (country, state, county).
    pub fn context_at(&self, lat: f32, lng: f32) -> Ctx<'g> {
        let admin = self.idx.geo.find_admin(lat as f64, lng as f64);
        let field = |label: &str| admin.row(label).map(|row| row.name.clone());
        Ctx {
            cc: admin.country_code.map(|c| [c[0].to_ascii_uppercase(), c[1].to_ascii_uppercase()]).unwrap_or([0, 0]),
            country: field("country").unwrap_or_default(),
            state: field("state").or_else(|| field("province")).unwrap_or_default(),
            county: field("county").unwrap_or_default(),
            district: field("state_district").or_else(|| field("district")).unwrap_or_default(),
        }
    }

    /// Display context of entity `e`, which is at (`lat`, `lng`).
    fn ctx_of(&self, st: &mut State<'g>, e: u32, (lat, lng): (f32, f32)) -> Rc<Ctx<'g>> {
        if let Some(c) = st.ctx.get(&e) {
            return c.clone();
        }
        st.stats.reverse_lookups += 1;
        let c = Rc::new(self.context_at(lat, lng));
        st.ctx.insert(e, c.clone());
        c
    }

    /// Boundaries covering a point's admin cell: (polygon, record, interior flag).
    fn cell_polygons(&self, lat: f32, lng: f32) -> Vec<(u32, AdminPolygon, bool)> {
        let geo: &Index = self.idx.geo;
        let cell = cell_id_at_level(lat as f64, lng as f64, geo.admin_cell_level);
        geo.admin_index
            .ids(cell)
            .into_iter()
            .filter_map(|id| Some((id & ID_MASK, polygon_raw(geo, id & ID_MASK)?, id & INTERIOR_FLAG != 0)))
            .filter(|(_, ap, _)| ap.name_id != NO_DATA)
            .collect()
    }

    /// Context words a candidate's point satisfies: the boundaries covering its admin cell whose
    /// name (or ISO code, or country code) matches a word, and that contain the point (the
    /// cell's interior flag, else point-in-polygon only for those boundaries). A one-cell slice
    /// of the reverse lookup, without its field mapping. The candidate is at (`lat`, `lng`).
    fn context_mask(&self, ctx: &Context, names: &mut HashMap<(u32, u16, bool), u64>, (lat, lng): (f32, f32)) -> u64 {
        let geo: &Index = self.idx.geo;
        let point = AdminPoint::new(lat as f64, lng as f64);
        let mut m = 0u64;
        for (_, ap, interior) in self.cell_polygons(lat, lng) {
            let nm = *names.entry(context_mask_key(ap.name_id, ap.country_code, ap.admin_level)).or_insert_with(|| {
                let folded = normalize(&geo.get_string(ap.name_id));
                let cc = cc_of(ap.country_code);
                let mut nm = ctx.mask_of(&folded);
                if ap.admin_level == 2 && ap.country_code != 0 {
                    let upper = [cc[0].to_ascii_uppercase(), cc[1].to_ascii_uppercase()];
                    let mut codes = vec![String::from_utf8_lossy(&[cc[0].to_ascii_lowercase(), cc[1].to_ascii_lowercase()]).into_owned()];
                    if let Some(c) = self.idx.iso.country.get(&upper) {
                        codes.extend(c.iter().cloned());
                    }
                    nm |= ctx.codes_mask(&codes);
                } else if let Some(c) = self.idx.iso.state.get(&(cc, folded)) {
                    nm |= ctx.codes_mask(c);
                }
                nm
            });
            if nm == 0 || m & nm == nm {
                continue;
            }
            if interior || geo.admin_geometry.contains_once(&ap, point) {
                m |= nm;
            }
        }
        m
    }

    /// Whether entity `e`'s point lies in the boundary whose primary ring is `area` (any of its
    /// rings: same name, level and country).
    fn contained_in(&self, e: u32, area: &AdminPolygon) -> bool {
        let Some((lat, lng)) = self.idx.ents.point(e) else { return false };
        let point = AdminPoint::new(lat as f64, lng as f64);
        for (_, ap, interior) in self.cell_polygons(lat, lng) {
            if ap.name_id != area.name_id || ap.admin_level != area.admin_level || ap.country_code != area.country_code {
                continue;
            }
            if interior || self.idx.geo.admin_geometry.contains_once(&ap, point) {
                return true;
            }
        }
        false
    }

    fn stream_dfa(&self, sub: &SubIndex, phrase: &str, edits: u8, dfa: &DFA, typed: usize, prefix: bool, out: &mut KeyMatches) {
        let anchor_chars = if edits >= 2 { ANCHOR_CHARS_TWO_EDITS } else { ANCHOR_CHARS_ONE_EDIT };
        let first: usize = phrase.chars().take(anchor_chars).map(|c| c.len_utf8()).sum();
        let aut = Anchored { first: &phrase.as_bytes()[..first], dfa };
        let mut s = sub.map.search_with_state(aut);
        let mut n = 0;
        while let Some((k, v, st)) = s.next() {
            let d = match dfa.distance(st.1) {
                Distance::Exact(d) => d,
                Distance::AtLeast(_) => continue,
            };
            let klen = char_len(k);
            let text = if prefix && klen > typed { prefix_text(typed, klen, d) } else { typo_text(d) };
            let e = out.entry(v).or_insert(0.0);
            *e = e.max(text);
            n += 1;
            if n >= MAX_KEYS_STREAMED {
                break;
            }
        }
    }

    /// Keys matching `phrase` as a whole name (with typos by length) and, when the user may
    /// still be typing it, as a prefix.
    fn lookup(&self, sub: &SubIndex, phrase: &str, prefix: bool, out: &mut KeyMatches) {
        let chars = phrase.chars().count();
        let ideo = is_ideographic(phrase);
        match allowed_edits(chars, EXACT_NO_TYPO_MAX_CHARS, EXACT_ONE_TYPO_MAX_CHARS, ideo) {
            0 => {
                if let Some(v) = sub.map.get(phrase) {
                    out.insert(v, 1.0);
                }
            }
            d => {
                let dfa = self.lev[d as usize - 1].build_dfa(phrase);
                self.stream_dfa(sub, phrase, d, &dfa, chars, false, out)
            }
        }
        if !prefix {
            return;
        }
        match allowed_edits(chars, PREFIX_NO_TYPO_MAX_CHARS, PREFIX_ONE_TYPO_MAX_CHARS, ideo) {
            0 => {
                let aut = fst::automaton::Str::new(phrase).starts_with();
                let mut s = sub.map.search(aut);
                let mut n = 0;
                while let Some((k, v)) = s.next() {
                    let klen = char_len(k);
                    let text = if klen > chars { prefix_text(chars, klen, 0) } else { 1.0 };
                    let e = out.entry(v).or_insert(0.0);
                    *e = e.max(text);
                    n += 1;
                    if n >= MAX_KEYS_STREAMED {
                        break;
                    }
                }
            }
            d => {
                let dfa = self.lev[d as usize - 1].build_prefix_dfa(phrase);
                self.stream_dfa(sub, phrase, d, &dfa, chars, true, out)
            }
        }
    }

    fn score(text: f32, imp: f32, class: u8, ctx_ok: bool) -> f32 {
        let ctx = if ctx_ok { 1.0 } else { CONTEXT_MISS_MULT };
        text * text * (IMPORTANCE_FLOOR + imp) * CLASS_MULT[class.min(5) as usize] * ctx
    }

    /// Scores one query split into `pool`. An entity whose point does not read is left out
    /// where the split reads it: as the pool takes it in, at its context check, and by a
    /// cut-back of the pending candidates that weighs it; with `READ_FIRST` also as it is
    /// queued, so that none is pending. Without, a split without context words (a search's
    /// first) has the pool take its hits in unread, and a split with them first has the pool
    /// read those it holds: false when one is nowhere (`search`).
    fn score_split<const READ_FIRST: bool>(&self, st: &mut State<'g>, split: u8, name: &[Token], ctx_words: &[String], prefix: bool, pool: &mut Pool) -> bool {
        let phrase = join(name);
        let ctx = Context { words: ctx_words, phrase: ctx_words.join(" ") };
        let has_ctx = !ctx_words.is_empty();
        let ents = &self.idx.ents;
        if phrase.is_empty() {
            return true;
        }
        // The split weighs its candidates against all the pool's scores, and its hits take
        // places from the pool's entities.
        if has_ctx && !pool.read_held(|e| ents.point(e).is_some()) {
            return false;
        }
        let chars = phrase.chars().count();
        let folded_last = name.last().is_some_and(|t| t.raw != t.folded);
        let mut cands: Vec<Cand> = Vec::new();
        // How many of the pending candidates, from the first, are known to read (`cut_back`).
        let mut read = 0;
        // Without context words a candidate goes straight to the pool; with them it waits for
        // the context check, made best-first and only while it can still reach the results.
        let mut emit = |pool: &mut Pool, entity: u32, ord: u8, class: u8, text: f32, imp: f32| {
            let potential = Self::score(text, imp, class, true);
            if potential <= pool.threshold() {
                return;
            }
            if !has_ctx {
                let hit = Hit { entity, ord, class, score: potential, text, context: false, split };
                if READ_FIRST {
                    pool.offer::<false>(hit, |e| ents.point(e).is_some());
                } else {
                    pool.offer::<true>(hit, |_| true);
                }
                return;
            }
            // Read first, an entity is read as it is queued (one the pool holds is somewhere).
            if READ_FIRST && !pool.best.contains_key(&entity) && ents.point(entity).is_none() {
                return;
            }
            cands.push(Cand { entity, ord, class, text, potential });
            if cands.len() > self.limits.pending {
                Self::cut_back(ents, &mut cands, &mut read, self.limits.pending);
            }
        };
        for (_, sub) in self.idx.subs() {
            if prefix && chars <= PREFIX_TABLE_MAX_CHARS && !folded_last {
                match sub.prefix_hits(&phrase) {
                    Some(hits) => {
                        for h in hits {
                            emit(pool, h.entity, h.ord, h.class, h.text, dequantize(h.imp_q));
                        }
                        continue;
                    }
                    // Short prefixes are all in the table: no entry means no key starts with it.
                    None if chars <= PREFIX_TABLE_ALL_CHARS => continue,
                    None => {}
                }
            }
            let mut keys = KeyMatches::new();
            self.lookup(sub, &phrase, prefix, &mut keys);
            // The user may still be typing a word the folding changed ("fort" -> "ft").
            if prefix {
                if let Some(last) = name.last() {
                    if last.raw != last.folded {
                        let mut raw = name.to_vec();
                        raw.last_mut().unwrap().folded = last.raw.clone();
                        self.lookup(sub, &join(&raw), true, &mut keys);
                    }
                }
            }
            st.stats.keys_matched += keys.len();
            let mut order: Vec<(u64, f32, f32)> = keys
                .into_iter()
                .map(|(v, t)| (v, t, t * t * (IMPORTANCE_FLOOR + dequantize(sub.first_imp(v)))))
                .collect();
            // Best bound first; keys that tie in key order (a value is its list's offset, and
            // the lists are in key order).
            order.sort_by(|a, b| b.2.total_cmp(&a.2).then(a.0.cmp(&b.0)));
            for (v, t, bound) in order {
                if bound <= pool.threshold() {
                    break;
                }
                for p in sub.list(v) {
                    st.stats.postings_read += 1;
                    let text = if p.partial { t * PARTIAL_TEXT } else { t };
                    let imp = dequantize(p.imp_q);
                    if text * text * (IMPORTANCE_FLOOR + imp) <= pool.threshold() {
                        if !p.partial {
                            break;
                        }
                        continue;
                    }
                    emit(pool, p.entity, p.ord, p.class, text, imp);
                }
            }
        }
        if !has_ctx {
            return true;
        }
        cands.sort_by(|a, b| b.potential.total_cmp(&a.potential).then(a.entity.cmp(&b.entity)));
        let mut names: HashMap<(u32, u16, bool), u64> = HashMap::new();
        let mut checked: HashMap<u32, bool> = HashMap::new();
        for c in cands {
            if c.potential <= pool.cutoff() {
                break;
            }
            let ok = match checked.get(&c.entity) {
                Some(&ok) => ok,
                // A miss, read as the pool would take it in.
                None if st.stats.context_checks >= self.limits.context_checks => false,
                None => {
                    let Some(at) = ents.point(c.entity) else { continue };
                    st.stats.context_checks += 1;
                    let ok = ctx.satisfied(self.context_mask(&ctx, &mut names, at));
                    checked.insert(c.entity, ok);
                    ok
                }
            };
            let score = if ok { c.potential } else { c.potential * CONTEXT_MISS_MULT };
            let hit = Hit { entity: c.entity, ord: c.ord, class: c.class, score, text: c.text, context: ok, split };
            pool.offer::<false>(hit, |e| checked.contains_key(&e) || ents.point(e).is_some());
        }
        true
    }

    /// Cuts the pending candidates, more than `max` of them, back to the half of `max` that
    /// could score best; the first `read` of them are known to read. It first reads the
    /// others and leaves out those that are nowhere, then cuts back only if more than `max` are
    /// left: kept, such a one could push out a candidate that, checked, would score above it
    /// (a cut-back ranks by the score a candidate could reach, not the one it gets); counted,
    /// it brings the cut-back on before a search without it cuts back, which keeps what this
    /// one drops. It looks at the others alone: once the candidates are full, each one queued
    /// that does not read brings on a cut-back that cuts nothing back and keeps all before it.
    /// Out of line, so that the closure that queues the candidates stays inlined in the loop
    /// over the postings.
    #[cold]
    #[inline(never)]
    fn cut_back(ents: &Entities, cands: &mut Vec<Cand>, read: &mut usize, max: usize) {
        // Those that read move up over those that are nowhere, in their order.
        let mut kept = *read;
        for at in *read..cands.len() {
            let c = cands[at];
            #[cfg(test)]
            CUT_BACK_LOOKS.set(CUT_BACK_LOOKS.get() + 1);
            if ents.point(c.entity).is_some() {
                cands[kept] = c;
                kept += 1;
            }
        }
        cands.truncate(kept);
        if cands.len() > max {
            cands.sort_by(|a, b| b.potential.total_cmp(&a.potential).then(a.entity.cmp(&b.entity)));
            cands.truncate(max / 2);
        }
        *read = cands.len();
    }

    pub fn search(&self, q: &str, limit: usize) -> (Vec<Found<'g>>, QueryStats) {
        if limit == 0 { return (Vec::new(), QueryStats::default()); }
        let mut st = State { ctx: HashMap::new(), stats: QueryStats::default() };
        // An entity can be nowhere: its record or its slot does not read (a damaged file). A
        // search answers as the search of the index without such entities, one that reads each
        // candidate's point as it is queued or offered and leaves it out if it is nowhere
        // (`READ_FIRST`), but reads fewer. What a search decides comes from the pool (its
        // threshold and cut-off, by ties too, and its hits), from the context checks made (a
        // budget its splits share) and from a split's pending candidates.
        //
        // A pending candidate is read before its context check, before a cut-back that weighs
        // it, and before the pool takes it in as a miss, and each of those reads leaves out one
        // that is nowhere, where a search that reads first has already left it out; one that
        // gets to none of them decides nothing. A split without context words, most of whose
        // hits never reach the results, has the pool take them in unread: before a split with
        // context words weighs candidates against its scores, the pool reads those it holds and
        // sets aside those it has evicted, which can have decided only which entities' hits take
        // the places at its threshold (`Pool::read_held`); the area order and the results read
        // the hits they take, and a full pool reads the entities still unread before either
        // takes a hit at or below its threshold (the one it had when it set them aside, if it
        // did). A search that meets one of those entities nowhere runs again, reading first,
        // with its own budget of context checks.
        let found = match self.search_once::<false>(&mut st, q, limit) {
            Some(found) => found,
            None => {
                let first = std::mem::take(&mut st.stats);
                let found = self.search_once::<true>(&mut st, q, limit).unwrap_or_default();
                st.stats += first;
                found
            }
        };
        (found, st.stats)
    }

    /// `search`'s results; with `READ_FIRST`, those of the search it answers as. None, without,
    /// for a search that met an entity it took in unread nowhere (`score_split`).
    fn search_once<const READ_FIRST: bool>(&self, st: &mut State<'g>, q: &str, limit: usize) -> Option<Vec<Found<'g>>> {
        let mut pool = Pool::new(self.limits.pool, limit * CONTEXT_CUTOFF_FACTOR);
        let mut phrases: Vec<String> = Vec::new();
        if let Some((name, rest)) = q.split_once(',') {
            let name = tokens(name);
            let ctx: Vec<String> = tokens(rest).into_iter().map(|t| t.folded).take(63).collect();
            phrases.push(join(&name));
            if !self.score_split::<READ_FIRST>(st, 0, &name, &ctx, ctx.is_empty(), &mut pool) {
                return None;
            }
        } else {
            let t = tokens(q);
            let n = t.len();
            for k in (1..=n).rev() {
                if n - k > MAX_CONTEXT_WORDS {
                    break;
                }
                let ctx: Vec<String> = t[k..].iter().map(|x| x.folded.clone()).collect();
                phrases.push(join(&t[..k]));
                if !self.score_split::<READ_FIRST>(st, (phrases.len() - 1) as u8, &t[..k], &ctx, k == n, &mut pool) {
                    return None;
                }
            }
        }
        // The pool holds unread entities only after a lone split without context words, and then
        // only those: the hits taken are read, and a full pool, which has turned hits away for
        // them, reads them all before a hit at or below its threshold is taken. Else every hit
        // is somewhere, and the hits are those of a pool that reads first but for which take the
        // places at the threshold at which the pool set entities aside, if it did; it reads
        // those before a hit at or below that threshold is taken (`Pool::read_held`).
        let ents = &self.idx.ents;
        let unread = std::mem::take(&mut pool.unread);
        let lazy = !unread.is_empty();
        let (level, later) = if lazy { (pool.threshold(), unread) } else { (pool.set_aside_at, std::mem::take(&mut pool.set_aside)) };
        let check = !later.is_empty();
        let mut later = Some(later).filter(|u| !u.is_empty() && pool.scores.len() >= pool.size);
        let mut ready = |h: &Hit| h.score > level || later.take().is_none_or(|u| u.into_iter().all(|e| ents.point(e).is_some()));
        let mut hits: Vec<Hit> = pool.best.into_values().collect();
        hits.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.entity.cmp(&b.entity)));
        if !self.place_before_its_area(&mut hits, check.then_some(&mut ready)) {
            return None;
        }
        let mut out: Vec<Found<'g>> = Vec::new();
        let mut kept: Vec<(String, (f32, f32))> = Vec::new();
        for h in hits {
            let Some(at) = ents.point(h.entity).filter(|_| ready(&h)) else {
                if check {
                    return None;
                }
                continue;
            };
            let name = self.display_name(&h, &phrases[h.split as usize]);
            let label = normalize(&name);
            if kept.iter().any(|(l, p)| *l == label && haversine_km(*p, at) < DEDUPE_KM) {
                continue;
            }
            kept.push((label, at));
            let (kind, typ) = self.idx.ents.kind_and_type(h.entity);
            let ctx = self.ctx_of(st, h.entity, at);
            out.push(Found { name, lat: at.0, lng: at.1, kind, typ: typ.into_owned(), ctx, hit: h });
            if out.len() >= limit {
                break;
            }
        }
        Some(out)
    }

    /// "Dubai" the city before "Dubai" the emirate: an area (other than a country) drops just
    /// below a settlement of the same name that lies inside it. With `ready`, of a pool that
    /// took entities in unread: false as soon as a hit it looks at is of an entity that is
    /// nowhere, or `ready` is false for it (`search_once`).
    fn place_before_its_area(&self, hits: &mut [Hit], mut ready: Option<&mut impl FnMut(&Hit) -> bool>) -> bool {
        let ents = &self.idx.ents;
        let label = |h: &Hit| normalize(&ents.base_name(h.entity));
        let mut changed = false;
        let area_top = self.limits.area_top;
        let top = hits.len().min(area_top);
        for i in 0..top {
            let e = hits[i].entity;
            if ready.as_mut().is_some_and(|r| !r(&hits[i])) {
                return false;
            }
            // Only a boundary moves: of the others, whether they are somewhere.
            if Entities::is_poi(e) || e < ents.n_place_nodes {
                if ready.is_some() && ents.point(e).is_none() {
                    return false;
                }
                continue;
            }
            let Some((k, t)) = ents.kind_and_type_of(e) else {
                if ready.is_some() {
                    return false;
                }
                continue;
            };
            if !Entities::is_area(k, &t) {
                continue;
            }
            let Some(area) = ents.slot(e - ents.n_place_nodes).and_then(|s| polygon_raw(self.idx.geo, s.poly)) else { continue };
            if area.admin_level == 2 {
                continue;
            }
            let area_label = label(&hits[i]);
            let mut found = None;
            for (j, h) in hits.iter().enumerate().take(i + 1 + area_top).skip(i + 1) {
                if ready.as_mut().is_some_and(|r| !r(h)) {
                    return false;
                }
                // A POI is no settlement.
                if Entities::is_poi(h.entity) {
                    if ready.is_some() && ents.point(h.entity).is_none() {
                        return false;
                    }
                    continue;
                }
                let Some((k, t)) = ents.kind_and_type_of(h.entity) else {
                    if ready.is_some() {
                        return false;
                    }
                    continue;
                };
                if Entities::is_settlement(k, &t) && label(h) == area_label && self.contained_in(h.entity, &area) {
                    found = Some(j);
                    break;
                }
            }
            if let Some(j) = found {
                hits[i].score = hits[j].score * 0.999;
                changed = true;
            }
        }
        if changed {
            hits.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.entity.cmp(&b.entity)));
        }
        true
    }

    /// The matched name when it starts with what was typed (a language the user chose:
    /// "Moskau"), else the record's own name.
    pub fn display_name(&self, h: &Hit, phrase: &str) -> String {
        let base = self.idx.ents.base_name(h.entity);
        if h.ord == 0 {
            return base.to_string();
        }
        let matched = self.idx.ents.name(h.entity, h.ord);
        if !normalize(&base).starts_with(phrase) && normalize(&matched).starts_with(phrase) {
            return matched.into_owned();
        }
        base.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn damerau_one_edit() {
        assert!(damerau_le1("texas", "texsa"));
        assert!(damerau_le1("texas", "texa"));
        assert!(damerau_le1("texas", "texass"));
        assert!(!damerau_le1("texas", "tesax"));
    }

    #[test]
    fn edits_by_length() {
        assert_eq!(allowed_edits(3, 3, 7, false), 0);
        assert_eq!(allowed_edits(5, 3, 7, false), 1);
        assert_eq!(allowed_edits(9, 3, 7, false), 2);
        assert_eq!(allowed_edits(9, 3, 7, true), 0);
    }

    #[test]
    fn korean_queries_allow_no_edits() {
        // 서울특별시 is 13 Jamo once normalized: long enough for two edits in a Latin script.
        let q = normalize("서울특별시");
        assert_eq!(q.chars().count(), 13);
        for (no_typo, one_typo) in [(EXACT_NO_TYPO_MAX_CHARS, EXACT_ONE_TYPO_MAX_CHARS), (PREFIX_NO_TYPO_MAX_CHARS, PREFIX_ONE_TYPO_MAX_CHARS)] {
            assert_eq!(allowed_edits(q.chars().count(), no_typo, one_typo, is_ideographic(&q)), 0);
        }
    }

    #[test]
    fn codes_match_whole_words_only() {
        let words = vec!["tx".to_string()];
        let c = Context { words: &words, phrase: "tx".into() };
        assert_eq!(c.mask_of("texas") & 1, 0);
    }
    fn hit(entity: u32, score: f32) -> Hit {
        Hit { entity, ord: 0, class: 0, score, text: 1.0, context: false, split: 0 }
    }

    #[test]
    fn zero_limit_has_no_reachable_candidate() {
        let mut pool = Pool::new(CANDIDATE_POOL, 0);
        assert_eq!(pool.cutoff(), f32::INFINITY);
        pool.offer::<false>(hit(1, 1.0), |_| true);
        assert_eq!(pool.cutoff(), f32::INFINITY);
    }

    #[test]
    fn pool_counts_each_entity_once() {
        // One entity improving 0.8 -> 0.9 is one candidate: with top_k 2 a context candidate at
        // 0.7 can still reach the results until a second entity is in.
        let mut pool = Pool::new(CANDIDATE_POOL, 2);
        pool.offer::<false>(hit(1, 0.8), |_| true);
        pool.offer::<false>(hit(1, 0.9), |_| true);
        assert_eq!(pool.cutoff(), 0.0);
        pool.offer::<false>(hit(2, 0.7), |_| true);
        assert_eq!(pool.cutoff(), 0.7);
        assert_eq!(pool.best.len(), 2);
        // CANDIDATE_POOL improvements of one entity leave the pool open to everyone else.
        let mut pool = Pool::new(CANDIDATE_POOL, 2);
        for k in 0..CANDIDATE_POOL {
            pool.offer::<false>(hit(1, 0.1 + k as f32 / 1000.0), |_| true);
        }
        assert_eq!(pool.threshold(), 0.0);
        for e in 2..CANDIDATE_POOL as u32 + 1 {
            pool.offer::<false>(hit(e, 0.05), |_| true);
        }
        assert_eq!(pool.threshold(), 0.05);
        pool.offer::<false>(hit(1000, 0.04), |_| true);
        assert!(!pool.best.contains_key(&1000));
    }

    #[test]
    fn pool_keeps_only_the_best_candidates() {
        let mut pool = Pool::new(CANDIDATE_POOL, 2);
        let n = CANDIDATE_POOL as u32 + 50;
        for e in 0..n {
            pool.offer::<false>(hit(e, 0.1 + e as f32 / 1000.0), |_| true);
        }
        assert_eq!(pool.scores.len(), CANDIDATE_POOL);
        let mut kept: Vec<u32> = pool.best.keys().copied().collect();
        kept.sort();
        assert_eq!(kept, (50..n).collect::<Vec<_>>());
    }

    #[test]
    fn only_a_settlement_inside_an_area_ranks_above_it() {
        use crate::fixture::{build_search, Geo, TempDir};
        let mut g = Geo::default();
        // Two level-6 areas a degree across; same-named features inside them, 60 km from their label points.
        let village = g.place("Barham", 2, 10.1, 30.1);
        let foxton = g.boundary("Foxton", 6, [10.0, 20.0, 11.0, 21.0]);
        let barham = g.boundary("Barham", 6, [10.0, 30.0, 11.0, 31.0]);
        let poi = g.poi("Foxton", 10.1, 20.1, 0) | POI_BIT;
        let dir = TempDir::new("area-order");
        let geo = g.load(&dir.0.join("geo"));
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        let idx = SearchIndex::open(&geo, &out, None, None).unwrap();
        let s = Searcher::new(&idx);
        let area = |poly: u32| 1 + poly;

        for (q, want) in [("Foxton", [area(foxton), poi]), ("Barham", [village, area(barham)])] {
            let (found, _) = s.search(q, 5);

            assert_eq!(found.iter().map(|f| f.hit.entity).collect::<Vec<_>>(), want, "{q}");
        }
    }

    #[test]
    fn keys_that_tie_are_read_in_key_order() {
        use crate::fixture::{build_search, Geo, TempDir};
        let mut g = Geo::default();
        // "abcd" starts both keys equally well and their postings have the same importance, so the
        // keys tie, and once CANDIDATE_POOL postings of that score are in, no more enter: the key
        // read first is found whole. Read in key order, "abcdx" (one POI) comes before "abcdy".
        g.poi("Elsewhere", 0.0, 0.0, 9);
        let first = g.poi("Abcdx", -30.0, -60.0, 9) | POI_BIT;
        let mut second = Vec::new();
        for k in 0..CANDIDATE_POOL as u32 {
            second.push(g.poi("Abcdy", (k / 20) as f32 * 5.0 - 25.0, (k % 20) as f32 * 5.0 - 50.0, 9) | POI_BIT);
        }
        let dir = TempDir::new("ties");
        let geo = g.load(&dir.0.join("geo"));
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        let idx = SearchIndex::open(&geo, &out, None, None).unwrap();

        let (found, _) = Searcher::new(&idx).search("abcd", 3);

        let got: Vec<(u32, &str)> = found.iter().map(|f| (f.hit.entity, f.name.as_str())).collect();
        assert_eq!(got, [(first, "Abcdx"), (second[0], "Abcdy"), (second[1], "Abcdy")]);
    }

    /// The index of the set at `set` once the rows of its POIs `rows` do not read: their combo
    /// past the dictionary (27 bytes a row), in a new file as a patch writes one.
    fn with_unreadable_pois(set: &std::path::Path, rows: &[u32]) -> Index {
        use query_server::{DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL};
        let mut records = std::fs::read(set.join("poi_records.bin")).unwrap();
        for &i in rows {
            let at = i as usize * 27 + 25;
            records[at..at + 2].copy_from_slice(&0xFFFEu16.to_le_bytes());
        }
        std::fs::write(set.join("poi_records.bin.new"), records).unwrap();
        std::fs::rename(set.join("poi_records.bin.new"), set.join("poi_records.bin")).unwrap();
        let damaged = Index::load(set.to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).unwrap();
        assert!(rows.iter().all(|&i| damaged.poi_records.as_ref().unwrap().record(i).is_none()));
        damaged
    }

    #[test]
    fn hits_of_records_that_do_not_read_take_no_place_from_those_that_do() {
        use crate::fixture::{build_search, Geo, TempDir};
        let mut g = Geo::default();
        // A neighbourhood "A" in a boundary "B"; one "Old Mill" of the least importance, then more
        // than the pool holds of the most; three "A Bay" of the most (the prefix table's hits of
        // "a b"); and as many "Market Hall" of the most as the pool holds, above one of the
        // least. Once the index is built, the rows of those "Old Mill" and "A Bay" and of that
        // last "Market Hall" are made not to read.
        g.boundary("B", 8, [10.0, 20.0, 10.2, 20.2]);
        let a = g.place("A", 5, 10.1, 20.1);
        let valid = g.poi("Old Mill", -30.0, -60.0, 0) | POI_BIT;
        let mut unreadable = Vec::new();
        for k in 0..=CANDIDATE_POOL as u32 {
            unreadable.push(g.poi("Old Mill", (k / 20) as f32 * 5.0 - 25.0, (k % 20) as f32 * 5.0 - 50.0, 255));
        }
        for k in 0..3 {
            unreadable.push(g.poi("A Bay", 40.0 + k as f32, 40.0, 255));
        }
        let halls: Vec<u32> = (0..CANDIDATE_POOL as u32)
            .map(|k| g.poi("Market Hall", 50.0 + (k / 20) as f32 * 2.0, 100.0 + (k % 20) as f32 * 2.0, 255) | POI_BIT)
            .collect();
        let last_hall = g.poi("Market Hall", -50.0, 100.0, 0);
        let dir = TempDir::new("unreadable-hits");
        let set = dir.0.join("geo");
        let geo = g.load(&set);
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        let queries = [("old mill", 5), ("old mill, nowhere", 5), ("a b", 1), ("market hall", 5)];
        // Each query's results, and the postings it read.
        let found = |geo: &Index| {
            let idx = SearchIndex::open(geo, &out, None, None).unwrap();
            let s = Searcher::new(&idx);
            queries.map(|(q, limit)| {
                let (found, st) = s.search(q, limit);
                (found.iter().map(|f| f.hit.entity).collect::<Vec<_>>(), st.postings_read)
            })
        };
        // Each of the first three queries' best hits is of the most importance while its row reads.
        let readable = found(&geo);
        for ((got, _), (q, _)) in readable.iter().zip(queries).take(3) {
            assert!(got.first().is_some_and(|e| unreadable.contains(&(e & !POI_BIT))), "{q}: {got:?}");
        }
        unreadable.push(last_hall);
        let damaged = with_unreadable_pois(&set, &unreadable);

        let got = found(&damaged);

        let entities: Vec<&Vec<u32>> = got.iter().map(|(e, _)| e).collect();
        assert_eq!(entities, [&vec![valid], &vec![valid], &vec![a], &halls[..5].to_vec()]);
        // The pool turns the last "Market Hall" away unread: that search reads the index once.
        assert_eq!(got[3], readable[3]);
    }

    /// The results of "old mill, b" (two of them) over `g`, whose POIs `three_named` are also
    /// "Old Mil" and "Old Mills" (each of those names is one edit from "old mill"): as built;
    /// then, once the rows of its POIs `unreadable` do not read, those of the search and of one
    /// that reads every entity as it is offered.
    fn old_mill_in_b(g: &crate::fixture::Geo, three_named: &[u32], unreadable: &[u32]) -> [Vec<u32>; 3] {
        use crate::fixture::{build_search, TempDir};
        let dir = TempDir::new("crowded-split");
        let (tsv, bin) = (dir.0.join("extra_names.tsv"), dir.0.join("extra_names.bin"));
        let rows: String = three_named.iter().map(|e| format!("o\t{e}\t2\tname:de\tOld Mil\no\t{e}\t3\talt_name\tOld Mills\n")).collect();
        std::fs::write(&tsv, rows).unwrap();
        crate::extras::pack(tsv.to_str().unwrap(), bin.to_str().unwrap()).unwrap();
        let set = dir.0.join("geo");
        let geo = g.load(&set);
        let out = dir.0.join("idx");
        build_search(&geo, &out, Some(&bin));
        let found = |geo: &Index, read_first: bool| {
            let idx = SearchIndex::open(geo, &out, Some(bin.to_str().unwrap()), None).unwrap();
            let s = Searcher::new(&idx);
            let found = if read_first {
                let mut st = State { ctx: HashMap::new(), stats: QueryStats::default() };
                s.search_once::<true>(&mut st, "old mill, b", 2).unwrap()
            } else {
                s.search("old mill, b", 2).0
            };
            found.iter().map(|f| f.hit.entity).collect::<Vec<_>>()
        };
        let built = found(&geo, false);
        let damaged = with_unreadable_pois(&set, unreadable);
        [built, found(&damaged, false), found(&damaged, true)]
    }

    #[test]
    fn records_that_do_not_read_crowd_no_candidate_out_of_a_context_split() {
        let mut g = crate::fixture::Geo::default();
        // Under "olde mill", best first: one POI of the most importance, as many more as fill the
        // pending candidates, then one in the boundary "B" of the least, which the cut-back of
        // the pending candidates it brings on drops. Then under "old mil", "old milk" and "old
        // mills" fewer POIs than the context checks allow, three names each, all of which score
        // above those many: the next cut-back drops every one of the many. The rows of the many
        // are made not to read.
        g.boundary("B", 8, [10.0, 20.0, 10.2, 20.2]);
        let lead = g.poi("Olde Mill", -30.0, -60.0, 255) | POI_BIT;
        let many: Vec<u32> = (0..MAX_PENDING_CANDIDATES as u32 - 1)
            .map(|k| g.poi("Olde Mill", -1.0 - (k / 100) as f32, -170.0 + (k % 100) as f32 * 3.4, 15))
            .collect();
        let inside = g.poi("Olde Mill", 10.1, 20.1, 3) | POI_BIT;
        // 3/4 of the context checks: three names each are more than the next cut-back keeps.
        let three_named: Vec<u32> = (0..MAX_CONTEXT_CHECKS as u32 * 3 / 4)
            .map(|k| g.poi("Old Milk", -1.5 - (k / 50) as f32 * 2.0, -175.0 + (k % 50) as f32 * 7.0, 127))
            .collect();

        let [built, damaged, read_first] = old_mill_in_b(&g, &three_named, &many);

        // While the many read, they keep the one in "B" from its context check: every result
        // misses the context.
        assert_eq!(built, [lead, three_named[0] | POI_BIT]);
        assert_eq!(damaged, [inside, lead]);
        assert_eq!(damaged, read_first);
    }

    #[test]
    fn records_that_do_not_read_bring_on_no_cut_back_of_a_context_split() {
        let mut g = crate::fixture::Geo::default();
        // Under "old mil", "old milk" and "old mills", best first: fewer POIs than the context
        // checks allow, the first of the most importance, three names each, as many names as
        // half the pending candidates and two more. Then under "olde mill" POIs of less
        // importance, as many as fill the pending candidates, and one in the boundary "B" of the
        // least: the cut-back it brings on keeps only POIs of three names, and drops it with the
        // many. The rows of the many are made not to read.
        g.boundary("B", 8, [10.0, 20.0, 10.2, 20.2]);
        let three_named: Vec<u32> = (0..MAX_PENDING_CANDIDATES as u32 / 6 + 1)
            .map(|k| g.poi("Old Milk", -1.5 - (k / 50) as f32 * 2.0, -175.0 + (k % 50) as f32 * 7.0, if k == 0 { 255 } else { 127 }))
            .collect();
        assert!(three_named.len() * 3 > MAX_PENDING_CANDIDATES / 2 && three_named.len() < MAX_CONTEXT_CHECKS);
        let many: Vec<u32> = (0..(MAX_PENDING_CANDIDATES - three_named.len() * 3) as u32)
            .map(|k| g.poi("Olde Mill", -1.0 - (k / 100) as f32, -170.0 + (k % 100) as f32 * 3.4, 15))
            .collect();
        let inside = g.poi("Olde Mill", 10.1, 20.1, 3) | POI_BIT;

        let [built, damaged, read_first] = old_mill_in_b(&g, &three_named, &many);

        // While the many read, the one in "B" is pushed out with them, so it gets no context
        // check: every result misses the context. Without them there is no cut-back.
        assert_eq!(built, [three_named[0] | POI_BIT, three_named[1] | POI_BIT]);
        assert_eq!(damaged, [inside, three_named[0] | POI_BIT]);
        assert_eq!(damaged, read_first);
    }

    #[test]
    fn records_that_do_not_read_kept_by_a_cut_back_hide_no_context_match() {
        let mut g = crate::fixture::Geo::default();
        // Under "olde mill", best first: one POI of the most importance and "the next", as many
        // as the context checks with it; then the rest of the pending candidates and one more,
        // "the first" of more importance than "the last". The cut-back the last of them brings
        // on keeps the one, the next and some of the first, and drops the last. Then under "old
        // mil", "old milk" and "old mills" POIs of three names each, every name of which scores
        // above the next, as many names as half the pending candidates less one: no cut-back
        // follows. Last, under "old mila", one in the boundary "B" of the least importance. The
        // rows of the first are made not to read.
        g.boundary("B", 8, [10.0, 20.0, 10.2, 20.2]);
        let lead = g.poi("Olde Mill", -30.0, -60.0, 255) | POI_BIT;
        let at = |k: usize| (-1.0 - (k / 100) as f32, -170.0 + (k % 100) as f32 * 3.4);
        let (n_next, n_first) = (MAX_CONTEXT_CHECKS - 1, MAX_PENDING_CANDIDATES * 7 / 16);
        let next: Vec<u32> = (0..n_next).map(|k| g.poi("Olde Mill", at(k).0, at(k).1, 31)).collect();
        let first: Vec<u32> = (n_next..n_next + n_first).map(|k| g.poi("Olde Mill", at(k).0, at(k).1, 15)).collect();
        for k in n_next + n_first..MAX_PENDING_CANDIDATES {
            g.poi("Olde Mill", at(k).0, at(k).1, 7);
        }
        let three_named: Vec<u32> = (0..(MAX_PENDING_CANDIDATES as u32 / 2 - 1) / 3)
            .map(|k| g.poi("Old Milk", -1.5 - (k / 50) as f32 * 2.0, -175.0 + (k % 50) as f32 * 7.0, 127))
            .collect();
        let inside = g.poi("Old Mila", 10.1, 20.1, 3) | POI_BIT;
        let names = three_named.len() * 3;
        assert!(1 + next.len() == MAX_CONTEXT_CHECKS && MAX_PENDING_CANDIDATES / 2 + names + 1 <= MAX_PENDING_CANDIDATES);
        // A search that leaves the first out cuts back once it has read as many names of the
        // three-named as the first are, and keeps fewer of the next than the checks have left.
        assert!(n_first < names && 1 + three_named.len() + (MAX_PENDING_CANDIDATES / 2 - 1 - n_first) < MAX_CONTEXT_CHECKS);

        let [built, damaged, read_first] = old_mill_in_b(&g, &three_named, &first);

        // While the first read, the context checks run out among the next: every result misses
        // the context.
        assert_eq!(built, [lead, three_named[0] | POI_BIT]);
        assert_eq!(damaged, [inside, lead]);
        assert_eq!(damaged, read_first);
    }

    #[test]
    fn cut_backs_look_at_each_pending_candidate_once_at_most() {
        use crate::fixture::{build_search, Geo, TempDir};
        let mut g = Geo::default();
        // Under "olde mill", best first: as many POIs as fill the pending candidates; then many
        // of less importance, made not to read, each of which a search that does not read first
        // queues, bringing on a cut-back that leaves it out and cuts nothing back; then as many
        // of the least, the first of which brings on a cut-back that cuts back, reading first or
        // not. All far apart.
        let many = MAX_PENDING_CANDIDATES / 4;
        let n = MAX_PENDING_CANDIDATES + 2 * many;
        let at = |k: usize| (-78.0 + (k / 100) as f32 * 1.3, -170.0 + (k % 100) as f32 * 3.4);
        let mut unreadable = Vec::new();
        for k in 0..n {
            let imp = if k < MAX_PENDING_CANDIDATES {
                15
            } else if k < MAX_PENDING_CANDIDATES + many {
                7
            } else {
                3
            };
            let row = g.poi("Olde Mill", at(k).0, at(k).1, imp);
            if imp == 7 {
                unreadable.push(row);
            }
        }
        assert_eq!(unreadable.len(), many);
        let dir = TempDir::new("cut-back-looks");
        let set = dir.0.join("geo");
        let geo = g.load(&set);
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        let damaged = with_unreadable_pois(&set, &unreadable);
        let idx = SearchIndex::open(&damaged, &out, None, None).unwrap();
        let s = Searcher::new(&idx);
        // The answer of a search that runs once (false) or reads first (true), and how many
        // pending candidates its cut-backs looked at.
        let looked = |read_first: bool| {
            CUT_BACK_LOOKS.set(0);
            let mut st = State { ctx: HashMap::new(), stats: QueryStats::default() };
            let found = if read_first {
                s.search_once::<true>(&mut st, "olde mill, b", DEFAULT_LIMIT)
            } else {
                s.search_once::<false>(&mut st, "olde mill, b", DEFAULT_LIMIT)
            };
            (answer(&found.unwrap()), CUT_BACK_LOOKS.get())
        };

        let [(got, once), (read_first, first)] = [false, true].map(looked);

        // Each candidate queued is looked at by one cut-back at most, the first that weighs it:
        // reading first, none that does not read is queued.
        assert!(once > MAX_PENDING_CANDIDATES && once <= n, "{once} looks at {n} candidates");
        assert!(first > MAX_PENDING_CANDIDATES && first <= n - many, "{first} looks at {} candidates", n - many);
        assert_eq!(got, read_first);
    }

    /// A result as the tests compare it: the hit, the name and the point.
    type Answer = Vec<(u32, u8, u32, bool, u8, String, u32, u32)>;

    fn answer(found: &[Found]) -> Answer {
        found.iter().map(|f| (f.hit.entity, f.hit.ord, f.hit.score.to_bits(), f.hit.context, f.hit.split, f.name.clone(), f.lat.to_bits(), f.lng.to_bits())).collect()
    }

    /// The results of `q` as a search answers it and as one that reads every entity first does.
    fn both_answers(s: &Searcher, q: &str, limit: usize) -> [Answer; 2] {
        let mut st = State { ctx: HashMap::new(), stats: QueryStats::default() };
        let read_first = s.search_once::<true>(&mut st, q, limit).unwrap();
        [answer(&s.search(q, limit).0), answer(&read_first)]
    }

    #[test]
    fn a_record_that_does_not_read_turns_away_no_hit_that_ties_with_it() {
        use crate::fixture::{build_search, Geo, TempDir};
        let mut g = Geo::default();
        // An "Old Mill" of the most importance; as many of less as the pool holds, all of one
        // importance, the first of them made not to read; then an "Old Mills" that scores
        // between them, which the prefix "old mill" reads after the "Old Mill" and which evicts
        // the first. All far apart.
        let at = |k: u32| (-60.0 + (k / 20) as f32 * 6.0, -170.0 + (k % 20) as f32 * 17.0);
        let top = g.poi("Old Mill", at(0).0, at(0).1, 255) | POI_BIT;
        let tied: Vec<u32> = (1..=CANDIDATE_POOL as u32).map(|k| g.poi("Old Mill", at(k).0, at(k).1, 3) | POI_BIT).collect();
        let mills = g.poi("Old Mills", 70.0, 0.0, 255) | POI_BIT;
        let dir = TempDir::new("tied-hits");
        let set = dir.0.join("geo");
        let geo = g.load(&set);
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        let damaged = with_unreadable_pois(&set, &[tied[0] & !POI_BIT]);
        let idx = SearchIndex::open(&damaged, &out, None, None).unwrap();
        let s = Searcher::new(&idx);

        // "old mill" as a search's only split, and as its first, before a split with the
        // context word "mill".
        let answers = ["old mill,", "old mill"].map(|q| both_answers(&s, q, DEFAULT_LIMIT));

        // Without the first, the pool has room for the last of them, and the "Old Mills" evicts
        // the second: the results go on from the third.
        let want: Vec<u32> = [top, mills].into_iter().chain(tied[2..DEFAULT_LIMIT].iter().copied()).collect();
        for [got, read_first] in answers {
            assert_eq!(got.iter().map(|r| r.0).collect::<Vec<_>>(), want);
            assert_eq!(got, read_first);
        }
    }

    #[test]
    fn a_record_that_does_not_read_saves_no_context_check_for_a_later_split() {
        use crate::fixture::{build_search, Geo, TempDir};
        let mut g = Geo::default();
        // As many "A B C" of some importance as the cut-off of a search of the default limit
        // keeps reachable, the first made not to read; more "A B" than the context checks, of
        // less, outside the boundary "C"; and as many "A" as "A B C", of the most, inside the
        // boundaries "B" and "C". All far apart.
        g.boundary("B", 8, [10.0, 20.0, 14.0, 24.0]);
        g.boundary("C", 8, [10.0, 20.0, 14.0, 24.0]);
        let reach = DEFAULT_LIMIT * CONTEXT_CUTOFF_FACTOR;
        let whole: Vec<u32> = (0..reach as u32).map(|k| g.poi("A B C", -60.0 + k as f32, -60.0, 127) | POI_BIT).collect();
        for k in 0..MAX_CONTEXT_CHECKS as u32 + 10 {
            g.poi("A B", -1.0 - (k / 100) as f32, -170.0 + (k % 100) as f32 * 3.4, 15);
        }
        for k in 0..reach as u32 {
            g.poi("A", 10.2 + (k / 7) as f32 * 0.5, 20.2 + (k % 7) as f32 * 0.5, 255);
        }
        let dir = TempDir::new("cross-split");
        let set = dir.0.join("geo");
        let geo = g.load(&set);
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        let damaged = with_unreadable_pois(&set, &[whole[0] & !POI_BIT]);
        let idx = SearchIndex::open(&damaged, &out, None, None).unwrap();

        let [got, read_first] = both_answers(&Searcher::new(&idx), "a b c", DEFAULT_LIMIT);

        // Without the first "A B C" the "A B" are above the cut-off and take every context
        // check, the "A" miss the context, and the other "A B C" are the results. With it in the
        // pool, the "A B" would be below the cut-off, and the "A" would take the checks and rank
        // above every "A B C".
        assert_eq!(got.iter().map(|r| r.0).collect::<Vec<_>>(), whole[1..=DEFAULT_LIMIT]);
        assert_eq!(got, read_first);
    }

    #[test]
    fn a_record_that_does_not_read_keeps_no_settlement_out_of_the_area_order() {
        use crate::fixture::{build_search, Geo, TempDir};
        let mut g = Geo::default();
        // A level-6 area "Foxton"; below it as many POIs "Foxton" as the area order looks past
        // it, of one importance, the first made not to read; below them a village "Foxton"
        // inside the area.
        g.place("Foxton", 2, 10.5, 20.5);
        // The area is entity 1 (one place node) + its polygon.
        let area = 1 + g.boundary("Foxton", 6, [10.0, 20.0, 11.0, 21.0]);
        let pois: Vec<u32> = (0..AREA_CHECK_TOP as u32)
            .map(|k| g.poi("Foxton", -60.0 + (k / 10) as f32 * 5.0, -170.0 + (k % 10) as f32 * 30.0, 3) | POI_BIT)
            .collect();
        let dir = TempDir::new("area-order-reads");
        let set = dir.0.join("geo");
        let geo = g.load(&set);
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        let found = |geo: &Index| {
            let idx = SearchIndex::open(geo, &out, None, None).unwrap();
            both_answers(&Searcher::new(&idx), "foxton", 1)
        };
        let [built, _] = found(&geo);
        let damaged = with_unreadable_pois(&set, &[pois[0] & !POI_BIT]);

        let [got, read_first] = found(&damaged);

        // Without the first POI the village is the last hit the area order looks at, and the
        // area drops below it.
        assert_eq!(built.iter().map(|r| r.0).collect::<Vec<_>>(), [area]);
        assert_eq!(got.iter().map(|r| r.0).collect::<Vec<_>>(), [pois[1]]);
        assert_eq!(got, read_first);
    }

    /// A xorshift64* generator: the cases of the search against one that reads first, from a seed.
    struct Rng(u64);

    impl Rng {
        fn new(seed: u64) -> Self {
            Rng(seed.wrapping_add(1).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
        }

        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        /// Below `n`, which is at least 1.
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }

        fn one_in(&mut self, n: usize) -> bool {
            self.below(n) == 0
        }

        fn pick<'x, T>(&mut self, xs: &'x [T]) -> &'x T {
            &xs[self.below(xs.len())]
        }

        /// In [lo, hi).
        fn at(&mut self, lo: f32, hi: f32) -> f32 {
            lo + (hi - lo) * self.below(4096) as f32 / 4096.0
        }
    }

    /// The words of the random names: one, two and three letters (no edit, the prefix table),
    /// longer ones a typo or two apart, and the trailing words of longer names.
    const WORDS: [&str; 12] = ["a", "b", "c", "ab", "bay", "old", "mill", "olde", "mills", "north", "norths", "market"];

    /// A random index, small enough that its names crowd a search's small limits. Its names are
    /// those of a few phrases of a few of the words, and each phrase's leading words: a query of
    /// a phrase then has hits in each of its splits. Over two degrees: rectangular boundaries
    /// named by the trailing words of the phrases (a split's context), overlapping, some
    /// labelled by a node; place nodes, some inside a boundary and named as it is; POIs in
    /// groups of one name, of one importance or a few (hits that tie), close together (a
    /// result's duplicates) or apart, inside a boundary or not; extra names for some; and now
    /// and then a crowd (`crowd`). Returns it with the extra-names rows, the phrases, the
    /// boundaries' names and the crowd's rows.
    fn random_geo(rng: &mut Rng) -> (crate::fixture::Geo, String, Vec<String>, Vec<String>, Vec<u32>) {
        let mut g = crate::fixture::Geo::default();
        let mut words: Vec<&str> = WORDS.to_vec();
        for k in (1..words.len()).rev() {
            words.swap(k, rng.below(k + 1));
        }
        words.truncate(3 + rng.below(4));
        let name = |rng: &mut Rng, n: usize| (0..n).map(|_| *rng.pick(&words)).collect::<Vec<_>>().join(" ");
        let phrases: Vec<String> = (0..1 + rng.below(3)).map(|_| {
            let n = 1 + rng.below(3);
            name(rng, n)
        }).collect();
        let (mut areas, mut rects, mut extras) = (Vec::new(), Vec::new(), String::new());
        for _ in 0..1 + rng.below(5) {
            let n = if rng.one_in(4) {
                let n = 1 + rng.below(2);
                name(rng, n)
            } else {
                let w: Vec<&str> = rng.pick(&phrases).split(' ').collect();
                w[w.len() - 1 - rng.below(w.len().min(2))..].join(" ")
            };
            let rect = match rects.last() {
                Some(&r) if rng.one_in(2) => r,
                _ => {
                    let (s, w) = (rng.at(10.0, 11.6), rng.at(20.0, 21.6));
                    let (h, x) = (rng.at(0.1, 0.8), rng.at(0.1, 0.8));
                    [s, w, s + h, w + x]
                }
            };
            let poly = g.boundary(&n, *rng.pick(&[2, 4, 6, 8, 8]), rect);
            if rng.one_in(3) {
                let node = g.place(if rng.one_in(2) { n.as_str() } else { *rng.pick(&words) }, rng.below(7) as u8, (rect[0] + rect[2]) / 2.0, (rect[1] + rect[3]) / 2.0);
                g.claim(poly, node);
            }
            if rng.one_in(4) {
                extras += &format!("b\t{poly}\t0\talt_name\t{}\n", name(rng, 1));
            }
            areas.push(n);
            rects.push(rect);
        }
        let inside = |rng: &mut Rng| {
            let [s, w, n, e] = *rng.pick(&rects);
            (rng.at(s, n), rng.at(w, e))
        };
        let anywhere = |rng: &mut Rng| (rng.at(9.8, 12.2), rng.at(19.8, 22.2));
        // Each phrase's leading words, and now and then another name.
        let mut names: Vec<String> = Vec::new();
        for p in &phrases {
            let w: Vec<&str> = p.split(' ').collect();
            names.extend((1..=w.len()).map(|k| w[..k].join(" ")));
        }
        for _ in 0..rng.below(3) {
            let n = 1 + rng.below(2);
            names.push(name(rng, n));
        }
        for _ in 0..rng.below(8) {
            let n = if rng.one_in(2) { rng.pick(&areas).clone() } else { rng.pick(&names).clone() };
            let (lat, lng) = if rng.one_in(2) { inside(rng) } else { anywhere(rng) };
            let node = g.place(&n, rng.below(7) as u8, lat, lng);
            if rng.one_in(5) {
                extras += &format!("p\t{node}\t0\tname:de\t{}\n", name(rng, 1));
            }
        }
        let imps = [0u8, 1, 3, 15, 63, 127, 255];
        let mut pois = 0;
        for _ in 0..2 + rng.below(8) {
            let n = rng.pick(&names).clone();
            let imp = *rng.pick(&imps);
            let (near, spread) = (rng.one_in(2), *rng.pick(&[0.01, 0.1, 1.0]));
            let (lat, lng) = if near { inside(rng) } else { anywhere(rng) };
            for _ in 0..1 + rng.below(12) {
                let (lat, lng) = if near || rng.one_in(2) { (lat + rng.at(-spread, spread), lng + rng.at(-spread, spread)) } else { anywhere(rng) };
                g.poi(&n, lat, lng, if rng.one_in(3) { *rng.pick(&imps) } else { imp });
                if rng.one_in(8) {
                    extras += &format!("o\t{pois}\t0\t{}\t{}\n", rng.pick(&["name:de", "alt_name", "old_name"]), name(rng, 1));
                }
                pois += 1;
            }
        }
        let mut phrases = phrases;
        let crowd = if rng.one_in(2) { crowd(rng, &mut g, &words, &mut phrases) } else { Vec::new() };
        (g, extras, phrases, areas, crowd)
    }

    /// A crowd: a name's POIs, more than a small pool holds, all of one low importance (hits
    /// that tie at its threshold), above them one of the most, and one of the most named by the
    /// name and a letter more, which the name as a prefix reads after them. The name is a word
    /// or, now and then, two: searched as they are, a split without context words then comes
    /// before one with them. Returns the rows of the many; the name is a phrase.
    fn crowd(rng: &mut Rng, g: &mut crate::fixture::Geo, words: &[&str], phrases: &mut Vec<String>) -> Vec<u32> {
        let w = words.iter().copied().filter(|w| w.len() >= 3).max_by_key(|_| rng.next()).unwrap_or("market");
        let w = if rng.one_in(3) { format!("{w} {}", rng.pick(words)) } else { w.to_string() };
        let w = w.as_str();
        let at = |rng: &mut Rng| (rng.at(9.8, 12.2), rng.at(19.8, 22.2));
        let (lat, lng) = at(rng);
        g.poi(w, lat, lng, 255);
        let imp = *rng.pick(&[0u8, 1, 3]);
        let many = (0..3 + rng.below(12)).map(|_| {
            let (lat, lng) = at(rng);
            g.poi(w, lat, lng, imp)
        }).collect();
        let (lat, lng) = at(rng);
        g.poi(&format!("{w}s"), lat, lng, 255);
        phrases.push(w.to_string());
        many
    }

    /// A random query of one of the `phrases` and the boundaries' names `areas`: a phrase as it
    /// is, or maybe cut short or with a letter changed, maybe without its first or last words,
    /// then maybe context words, after a comma or not.
    fn random_query(rng: &mut Rng, phrases: &[String], areas: &[String]) -> String {
        let mut words: Vec<String> = rng.pick(phrases).split(' ').map(str::to_string).collect();
        if rng.one_in(3) {
            return words.join(" ");
        }
        if words.len() > 1 && rng.one_in(5) {
            words.remove(if rng.one_in(2) { 0 } else { words.len() - 1 });
        }
        let last = words.len() - 1;
        if words[last].len() > 2 && rng.one_in(4) {
            let keep = 1 + rng.below(words[last].len() - 1);
            words[last].truncate(keep);
        }
        let k = rng.below(words.len());
        if words[k].len() >= 4 && rng.one_in(5) {
            let at = 1 + rng.below(words[k].len() - 1);
            words[k].replace_range(at..at + 1, if rng.one_in(2) { "x" } else { "" });
        }
        let ctx: Vec<String> = (0..rng.below(3)).map(|_| rng.pick(areas).split(' ').next().unwrap().to_string()).collect();
        match rng.below(3) {
            0 if !ctx.is_empty() => format!("{}, {}", words.join(" "), ctx.join(" ")),
            1 if rng.one_in(4) => format!("{},", words.join(" ")),
            _ => words.into_iter().chain(ctx).collect::<Vec<_>>().join(" "),
        }
    }

    /// `bnd` (a places.bnd) with the slots `damage` picks made to read as no slot: a labelled
    /// one labelled by no node, an unlabelled one's point off the globe.
    fn with_unreadable_slots(bnd: &[u8], mut damage: impl FnMut() -> bool) -> (Vec<u8>, usize) {
        use crate::entities::{BOUNDARY_FOOTER_BYTES, LABELLED_SLOT_BYTES, UNLABELLED_SLOT_BYTES};
        let mut out = bnd.to_vec();
        let slots = out.len() - BOUNDARY_FOOTER_BYTES;
        let labelled = u32::from_le_bytes(out[slots..].try_into().unwrap()) as usize;
        let mut n = 0;
        for k in 0..labelled {
            if damage() {
                let at = k * LABELLED_SLOT_BYTES + 4;
                out[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
                n += 1;
            }
        }
        for at in (labelled * LABELLED_SLOT_BYTES..slots).step_by(UNLABELLED_SLOT_BYTES) {
            if damage() {
                out[at + 4..at + 8].copy_from_slice(&f32::NAN.to_le_bytes());
                n += 1;
            }
        }
        (out, n)
    }

    #[test]
    fn a_search_answers_as_one_that_reads_every_entity_first() {
        use crate::fixture::{build_search, TempDir};
        use query_server::{DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL};
        // Random indexes, each searched with random limits of the pool, the context checks, the
        // pending candidates and the area order, small enough that its hits fill them, once a
        // random part of its POI rows and boundary slots does not read: the answers as a search
        // that reads first has them, and, by a search that runs once, the context checks made
        // (the postings read can be fewer, where the pool turns hits away for an entity that is
        // nowhere, which it holds unread and below every hit taken). PLACE_SEARCH_SEEDS indexes
        // (from PLACE_SEARCH_FIRST_SEED) for a longer run.
        let env = |k: &str, d: u64| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
        let (first, seeds) = (env("PLACE_SEARCH_FIRST_SEED", 0), env("PLACE_SEARCH_SEEDS", 40));
        let (mut cases, mut damaged, mut again, mut failed, mut counted) = (0usize, 0usize, 0usize, Vec::new(), Vec::new());
        for seed in first..first + seeds {
            let mut rng = Rng::new(seed);
            let (g, extras, phrases, areas, crowd) = random_geo(&mut rng);
            let dir = TempDir::new("same-answers");
            let (tsv, bin) = (dir.0.join("extra_names.tsv"), dir.0.join("extra_names.bin"));
            std::fs::write(&tsv, extras + "o\t0\t0\tname:de\tolde\n").unwrap();
            crate::extras::pack(tsv.to_str().unwrap(), bin.to_str().unwrap()).unwrap();
            let set = dir.0.join("geo");
            let geo = g.load(&set);
            let out = dir.0.join("idx");
            build_search(&geo, &out, Some(&bin));
            let rows = geo.poi_records.as_ref().unwrap().len() as usize;
            // Mapped before the damage, which writes new files.
            let built = SearchIndex::open(&geo, &out, Some(bin.to_str().unwrap()), None).unwrap();
            let records = std::fs::read(set.join("poi_records.bin")).unwrap();
            let bnd = std::fs::read(out.join(BOUNDARY_FILE)).unwrap();
            for damage in 0..4 {
                // One POI row, of the crowd as often as not; a boundary slot now and then; a
                // tenth of the rows and a quarter of the slots; up to a third of the rows and most
                // slots.
                let share = [0, 0, 1, 1 + rng.below(6)][damage];
                let one = if !crowd.is_empty() && rng.one_in(2) { *rng.pick(&crowd) as usize } else { rng.below(rows) };
                let mut recs = records.clone();
                let mut n = 0;
                for (i, at) in (0..rows).map(|i| (i, i * 27 + 25)) {
                    if (damage == 0 && i == one) || (damage >= 2 && rng.below(20) <= share) {
                        recs[at..at + 2].copy_from_slice(&0xFFFEu16.to_le_bytes());
                        n += 1;
                    }
                }
                let (slots, m) = with_unreadable_slots(&bnd, || damage >= 1 && rng.below(4 * (4 - damage)) <= share);
                if n + m == 0 {
                    continue;
                }
                for (name, bytes, at) in [("poi_records.bin", &recs, &set), (BOUNDARY_FILE, &slots, &out)] {
                    std::fs::write(at.join(format!("{name}.new")), bytes).unwrap();
                    std::fs::rename(at.join(format!("{name}.new")), at.join(name)).unwrap();
                }
                let geo = Index::load(set.to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).unwrap();
                let idx = SearchIndex::open(&geo, &out, Some(bin.to_str().unwrap()), None).unwrap();
                for _ in 0..4 {
                    let limits = if rng.one_in(8) {
                        Limits::default()
                    } else {
                        Limits { pool: 1 + rng.below(10), context_checks: rng.below(10), pending: 2 + rng.below(14), area_top: 1 + rng.below(5) }
                    };
                    let (s, as_built) = (Searcher { limits, ..Searcher::new(&idx) }, Searcher { limits, ..Searcher::new(&built) });
                    for _ in 0..16 {
                        let q = random_query(&mut rng, &phrases, &areas);
                        for limit in [1 + rng.below(2), 3 + rng.below(6)] {
                            let got = answer(&s.search(&q, limit).0);
                            let mut st = State { ctx: HashMap::new(), stats: QueryStats::default() };
                            let read_first = answer(&s.search_once::<true>(&mut st, &q, limit).unwrap());
                            let mut once = State { ctx: HashMap::new(), stats: QueryStats::default() };
                            let ran_once = s.search_once::<false>(&mut once, &q, limit).is_some();
                            cases += 1;
                            again += !ran_once as usize;
                            damaged += (got != answer(&as_built.search(&q, limit).0)) as usize;
                            let case = format!("seed {seed} damage {damage} ({n} rows, {m} slots) {limits:?} {q:?} limit {limit}");
                            if got != read_first {
                                failed.push(format!("{case}:\n  {got:?}\n  {read_first:?}"));
                            } else if ran_once && once.stats.context_checks != st.stats.context_checks {
                                counted.push(format!("{case}: {:?}, {:?}", once.stats, st.stats));
                            }
                        }
                    }
                }
            }
        }
        eprintln!("{cases} cases, {damaged} answered otherwise than before the damage, {again} searched again");
        assert!(failed.is_empty(), "{} of {cases} cases answered otherwise; the first:\n{}", failed.len(), failed[..failed.len().min(5)].join("\n"));
        assert!(counted.is_empty(), "{} of {cases} cases made other context checks; the first:\n{}", counted.len(), counted[..counted.len().min(5)].join("\n"));
        assert!(cases >= seeds as usize * 50 && damaged >= cases / 20);
    }

    #[test]
    fn context_masks_distinguish_country_and_state_codes() {
        let mut names = HashMap::new();
        let country = context_mask_key(10, u16::from_le_bytes(*b"GE"), 2);
        let state = context_mask_key(10, 0, 4);
        names.insert(country, 1);
        assert_eq!(*names.entry(state).or_insert(2), 2);
        assert_ne!(context_mask_key(10, 0, 2), context_mask_key(10, 0, 4));
        assert_ne!(context_mask_key(10, 1, 4), context_mask_key(10, 2, 4));
    }

    #[test]
    fn float_order_is_consistent_for_nan_and_signed_zero() {
        for value in [f32::NAN, -0.0, 0.0, 1.0] {
            let a = Ord32(value);
            assert!(a == a);
            assert_eq!(a.partial_cmp(&a), Some(a.cmp(&a)));
        }
        assert!(Ord32(-0.0) != Ord32(0.0));
    }

}
