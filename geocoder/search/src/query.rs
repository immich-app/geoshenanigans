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
use query_server::{cell_id_at_level, AdminPolygon, Index, ID_MASK, INTERIOR_FLAG, NO_DATA};
use fst::{Automaton, IntoStreamer, Streamer};
use levenshtein_automata::{Distance, LevenshteinAutomatonBuilder, DFA, SINK_STATE};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::rc::Rc;

pub struct Searcher<'a, 'g> {
    pub idx: &'a SearchIndex<'g>,
    lev: [LevenshteinAutomatonBuilder; 2],
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
    pub country: &'g str,
    pub state: &'g str,
    pub county: &'g str,
    pub district: &'g str,
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

#[derive(Default, Debug, Clone, Copy)]
pub struct QueryStats {
    pub reverse_lookups: usize,
    pub context_checks: usize,
    pub keys_matched: usize,
    pub postings_read: usize,
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
    heap: BinaryHeap<Reverse<Ord32>>,
    /// best `top_k` scores offered, descending (the cut-off for costly context checks)
    top: Vec<f32>,
    top_k: usize,
}

impl Pool {
    fn threshold(&self) -> f32 {
        if self.heap.len() < CANDIDATE_POOL {
            0.0
        } else {
            self.heap.peek().map(|r| r.0 .0).unwrap_or(0.0)
        }
    }

    fn offer(&mut self, h: Hit) {
        if h.score <= self.threshold() {
            return;
        }
        if let Some(old) = self.best.get(&h.entity) {
            if old.score >= h.score {
                return;
            }
        }
        self.heap.push(Reverse(Ord32(h.score)));
        if self.heap.len() > CANDIDATE_POOL {
            self.heap.pop();
        }
        let at = self.top.partition_point(|&x| x >= h.score);
        if at < self.top_k {
            self.top.insert(at, h.score);
            self.top.truncate(self.top_k);
        }
        self.best.insert(h.entity, h);
    }

    /// No candidate scoring at most this can reach the results.
    fn cutoff(&self) -> f32 {
        if self.top_k == 0 { return f32::INFINITY; }
        if self.top.len() < self.top_k {
            self.threshold()
        } else {
            self.top[self.top_k - 1].max(self.threshold())
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

impl<'a, 'g> Searcher<'a, 'g> {
    pub fn new(idx: &'a SearchIndex<'g>) -> Self {
        Searcher { idx, lev: [LevenshteinAutomatonBuilder::new(1, true), LevenshteinAutomatonBuilder::new(2, true)] }
    }

    /// Display context of a point: the geocoder's admin lookup (country, state, county).
    pub fn context_at(&self, lat: f32, lng: f32) -> Ctx<'g> {
        let admin = self.idx.geo.find_admin(lat as f64, lng as f64);
        Ctx {
            cc: admin.country_code.map(|c| [c[0].to_ascii_uppercase(), c[1].to_ascii_uppercase()]).unwrap_or([0, 0]),
            country: admin.country.unwrap_or(""),
            state: admin.state.or(admin.province).unwrap_or(""),
            county: admin.county.unwrap_or(""),
            district: admin.state_district.or(admin.district).unwrap_or(""),
        }
    }

    fn ctx_of(&self, st: &mut State<'g>, e: u32) -> Rc<Ctx<'g>> {
        if let Some(c) = st.ctx.get(&e) {
            return c.clone();
        }
        let (lat, lng) = self.idx.ents.point(e);
        st.stats.reverse_lookups += 1;
        let c = Rc::new(self.context_at(lat, lng));
        st.ctx.insert(e, c.clone());
        c
    }

    fn inside(&self, ap: &AdminPolygon, lat: f32, lng: f32) -> bool {
        self.idx.geo.admin_pip(ap, lat as f64, lng as f64)
    }

    /// Boundaries covering a point's admin cell: (polygon, record, interior flag).
    fn cell_polygons(&self, lat: f32, lng: f32) -> Vec<(u32, AdminPolygon, bool)> {
        let geo: &Index = self.idx.geo;
        let cell = cell_id_at_level(lat as f64, lng as f64, geo.admin_cell_level);
        let off = Index::lookup_admin_cell_fb(&geo.admin_cells, cell);
        Index::read_entries_fb(&geo.admin_entries, off)
            .into_iter()
            .map(|id| (id & ID_MASK, polygon_raw(geo, id & ID_MASK), id & INTERIOR_FLAG != 0))
            .filter(|(_, ap, _)| ap.name_id != NO_DATA)
            .collect()
    }

    /// Context words a candidate's point satisfies: the boundaries covering its admin cell whose
    /// name (or ISO code, or country code) matches a word, and that contain the point (the
    /// cell's interior flag, else point-in-polygon only for those boundaries). A one-cell slice
    /// of the reverse lookup, without its field mapping.
    fn context_mask(&self, ctx: &Context, names: &mut HashMap<(u32, u16, bool), u64>, e: u32) -> u64 {
        let geo: &Index = self.idx.geo;
        let (lat, lng) = self.idx.ents.point(e);
        let mut m = 0u64;
        for (_, ap, interior) in self.cell_polygons(lat, lng) {
            let nm = *names.entry(context_mask_key(ap.name_id, ap.country_code, ap.admin_level)).or_insert_with(|| {
                let folded = normalize(geo.get_string(ap.name_id));
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
            if interior || self.inside(&ap, lat, lng) {
                m |= nm;
            }
        }
        m
    }

    /// Whether entity `e`'s point lies in the boundary whose primary ring is `area` (any of its
    /// rings: same name, level and country).
    fn contained_in(&self, e: u32, area: &AdminPolygon) -> bool {
        let (lat, lng) = self.idx.ents.point(e);
        for (_, ap, interior) in self.cell_polygons(lat, lng) {
            if ap.name_id != area.name_id || ap.admin_level != area.admin_level || ap.country_code != area.country_code {
                continue;
            }
            if interior || self.inside(&ap, lat, lng) {
                return true;
            }
        }
        false
    }

    fn stream_dfa(&self, sub: &SubIndex, phrase: &str, edits: u8, dfa: &DFA, typed: usize, prefix: bool, out: &mut KeyMatches) {
        let anchor_chars = if edits >= 2 { ANCHOR_CHARS_TWO_EDITS } else { ANCHOR_CHARS_ONE_EDIT };
        let first: usize = phrase.chars().take(anchor_chars).map(|c| c.len_utf8()).sum();
        let aut = Anchored { first: &phrase.as_bytes()[..first], dfa };
        let mut s = sub.map.search_with_state(aut).into_stream();
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
                let mut s = sub.map.search(aut).into_stream();
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

    fn score_split(&self, st: &mut State<'g>, split: u8, name: &[Token], ctx_words: &[String], prefix: bool, pool: &mut Pool) {
        let phrase = join(name);
        if phrase.is_empty() {
            return;
        }
        let ctx = Context { words: ctx_words, phrase: ctx_words.join(" ") };
        let has_ctx = !ctx_words.is_empty();
        let chars = phrase.chars().count();
        let folded_last = name.last().is_some_and(|t| t.raw != t.folded);
        let mut cands: Vec<Cand> = Vec::new();
        // Without context words a candidate goes straight to the pool; with them it waits for
        // the context check, made best-first and only while it can still reach the results.
        let mut emit = |pool: &mut Pool, entity: u32, ord: u8, class: u8, text: f32, imp: f32| {
            let potential = Self::score(text, imp, class, true);
            if potential <= pool.threshold() {
                return;
            }
            if has_ctx {
                cands.push(Cand { entity, ord, class, text, potential });
                if cands.len() > MAX_PENDING_CANDIDATES {
                    cands.sort_by(|a, b| b.potential.total_cmp(&a.potential).then(a.entity.cmp(&b.entity)));
                    cands.truncate(MAX_PENDING_CANDIDATES / 2);
                }
            } else {
                pool.offer(Hit { entity, ord, class, score: potential, text, context: false, split });
            }
        };
        let mut postings = Vec::new();
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
            order.sort_by(|a, b| b.2.total_cmp(&a.2).then(a.0.cmp(&b.0)));
            for (v, t, bound) in order {
                if bound <= pool.threshold() {
                    break;
                }
                sub.postings(v, &mut postings);
                for p in &postings {
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
            return;
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
                None if st.stats.context_checks >= MAX_CONTEXT_CHECKS => false,
                None => {
                    st.stats.context_checks += 1;
                    let ok = ctx.satisfied(self.context_mask(&ctx, &mut names, c.entity));
                    checked.insert(c.entity, ok);
                    ok
                }
            };
            let score = if ok { c.potential } else { c.potential * CONTEXT_MISS_MULT };
            pool.offer(Hit { entity: c.entity, ord: c.ord, class: c.class, score, text: c.text, context: ok, split });
        }
    }

    pub fn search(&self, q: &str, limit: usize) -> (Vec<Found<'g>>, QueryStats) {
        if limit == 0 { return (Vec::new(), QueryStats::default()); }
        let mut st = State { ctx: HashMap::new(), stats: QueryStats::default() };
        let mut pool = Pool { best: HashMap::new(), heap: BinaryHeap::new(), top: Vec::new(), top_k: limit * CONTEXT_CUTOFF_FACTOR };
        let mut phrases: Vec<String> = Vec::new();
        if let Some((name, rest)) = q.split_once(',') {
            let name = tokens(name);
            let ctx: Vec<String> = tokens(rest).into_iter().map(|t| t.folded).take(63).collect();
            phrases.push(join(&name));
            self.score_split(&mut st, 0, &name, &ctx, ctx.is_empty(), &mut pool);
        } else {
            let t = tokens(q);
            let n = t.len();
            for k in (1..=n).rev() {
                if n - k > MAX_CONTEXT_WORDS {
                    break;
                }
                let ctx: Vec<String> = t[k..].iter().map(|x| x.folded.clone()).collect();
                phrases.push(join(&t[..k]));
                self.score_split(&mut st, (phrases.len() - 1) as u8, &t[..k], &ctx, k == n, &mut pool);
            }
        }
        let mut hits: Vec<Hit> = pool.best.into_values().collect();
        hits.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.entity.cmp(&b.entity)));
        self.place_before_its_area(&mut hits);
        let mut out: Vec<Found<'g>> = Vec::new();
        let mut kept: Vec<(String, (f32, f32))> = Vec::new();
        for h in hits {
            let at = self.idx.ents.point(h.entity);
            let name = self.display_name(&h, &phrases[h.split as usize]);
            let label = normalize(&name);
            if kept.iter().any(|(l, p)| *l == label && haversine_km(*p, at) < DEDUPE_KM) {
                continue;
            }
            kept.push((label, at));
            let (kind, typ) = self.idx.ents.kind_and_type(h.entity);
            let ctx = self.ctx_of(&mut st, h.entity);
            out.push(Found { name, lat: at.0, lng: at.1, kind, typ: typ.into_owned(), ctx, hit: h });
            if out.len() >= limit {
                break;
            }
        }
        (out, st.stats)
    }

    /// "Dubai" the city before "Dubai" the emirate: an area (other than a country) drops just
    /// below a settlement of the same name that lies inside it.
    fn place_before_its_area(&self, hits: &mut [Hit]) {
        let ents = &self.idx.ents;
        let label = |h: &Hit| normalize(ents.base_name(h.entity));
        let mut changed = false;
        let top = hits.len().min(AREA_CHECK_TOP);
        for i in 0..top {
            let e = hits[i].entity;
            if !ents.is_area(e) || Entities::is_poi(e) || e < ents.n_place_nodes {
                continue;
            }
            let area = polygon_raw(self.idx.geo, ents.slot(e - ents.n_place_nodes).poly);
            if area.admin_level == 2 {
                continue;
            }
            let area_label = label(&hits[i]);
            let found = (i + 1..hits.len().min(i + 1 + AREA_CHECK_TOP))
                .filter(|&j| !ents.is_area(hits[j].entity) && label(&hits[j]) == area_label)
                .find(|&j| self.contained_in(hits[j].entity, &area));
            if let Some(j) = found {
                hits[i].score = hits[j].score * 0.999;
                changed = true;
            }
        }
        if changed {
            hits.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.entity.cmp(&b.entity)));
        }
    }

    /// The matched name when it starts with what was typed (a language the user chose:
    /// "Moskau"), else the record's own name.
    pub fn display_name(&self, h: &Hit, phrase: &str) -> String {
        let base = self.idx.ents.base_name(h.entity);
        if h.ord == 0 {
            return base.to_string();
        }
        let matched = self.idx.ents.name(h.entity, h.ord);
        if !normalize(base).starts_with(phrase) && normalize(&matched).starts_with(phrase) {
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
    fn codes_match_whole_words_only() {
        let words = vec!["tx".to_string()];
        let c = Context { words: &words, phrase: "tx".into() };
        assert_eq!(c.mask_of("texas") & 1, 0);
    }
    #[test]
    fn zero_limit_has_no_reachable_candidate() {
        let mut pool = Pool { best: HashMap::new(), heap: BinaryHeap::new(), top: Vec::new(), top_k: 0 };
        assert_eq!(pool.cutoff(), f32::INFINITY);
        pool.top.push(1.0);
        assert_eq!(pool.cutoff(), f32::INFINITY);
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
