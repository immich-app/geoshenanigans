use crate::constants::*;
use crate::index::{char_len, dequantize, prefix_text, Entity, Index, NONE};
use crate::normalize::{is_ideographic, join, tokens, Token};
use fst::{Automaton, IntoStreamer, Streamer};
use levenshtein_automata::{Distance, LevenshteinAutomatonBuilder, DFA, SINK_STATE};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

pub struct Searcher<'a> {
    pub idx: &'a Index,
    lev: [LevenshteinAutomatonBuilder; 2],
}

#[derive(Clone, Debug)]
pub struct Hit {
    pub entity: u32,
    pub ord: u16,
    pub score: f32,
    pub text: f32,
    pub context: bool,
    /// The query split that produced the hit (index into the search's phrases).
    pub split: u8,
}

pub struct Found {
    pub hit: Hit,
    pub name: String,
}

/// PS_TRACE=1 prints each lookup's key count and time to stderr.
fn trace() -> bool {
    static T: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *T.get_or_init(|| std::env::var_os("PS_TRACE").is_some())
}

/// A Levenshtein DFA whose first characters must match exactly (Elasticsearch's
/// prefix_length): an edit allowed near the start would make the fst walk every branch near
/// its root.
struct Anchored<'a> {
    first: &'a [u8],
    dfa: &'a DFA,
}

impl Automaton for Anchored<'_> {
    /// (bytes of the first character consumed, DFA state)
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

/// Score as f32 ordered for the heap.
#[derive(PartialEq, PartialOrd, Clone, Copy)]
struct Ord32(f32);
impl Eq for Ord32 {}
impl Ord for Ord32 {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&o.0)
    }
}

/// Best text score per matched key, from whole-name and prefix lookups.
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
        // a word of a longer area name: "crimea" in "autonomous republic of crimea"
        || (w_chars >= CONTEXT_WORD_MIN_CHARS && alias.contains(' ') && alias.split(' ').any(|t| t == w))
        || (w_chars >= CONTEXT_TYPO_MIN_CHARS
            // cheap rejects first: one edit changes the length by at most one char (4 bytes)
            // and, as in the name lookups, the first character is not an edit
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

/// Context words of one query split; bit i of a mask is context word i, bit 63 the whole phrase.
struct Context<'q> {
    words: &'q [String],
    phrase: String,
    cache: HashMap<(u8, u32), u64>,
}

const PHRASE_BIT: u64 = 1 << 63;

impl<'q> Context<'q> {
    fn mask_of(&self, alias: &str) -> u64 {
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

    fn group_mask(&mut self, idx: &Index, g: u32) -> u64 {
        if g == NONE {
            return 0;
        }
        if let Some(&m) = self.cache.get(&(0, g)) {
            return m;
        }
        let m = idx.groups[g as usize].sorted.iter().fold(0, |m, a| m | self.mask_of(a));
        self.cache.insert((0, g), m);
        m
    }

    fn string_mask(&mut self, idx: &Index, s: u32) -> u64 {
        if s == 0 {
            return 0;
        }
        if let Some(&m) = self.cache.get(&(1, s)) {
            return m;
        }
        let m = self.mask_of(&idx.strings_norm[s as usize]);
        self.cache.insert((1, s), m);
        m
    }

    fn matches(&mut self, idx: &Index, e: &Entity) -> bool {
        let cc = [e.cc[0].to_ascii_lowercase(), e.cc[1].to_ascii_lowercase()];
        let cc = std::str::from_utf8(&cc).unwrap_or("");
        let m = self.group_mask(idx, e.country_grp)
            | self.group_mask(idx, e.state_grp)
            | self.string_mask(idx, e.country)
            | self.string_mask(idx, e.state)
            | self.string_mask(idx, e.county)
            | self.string_mask(idx, e.city)
            | self.string_mask(idx, e.region)
            | self.string_mask(idx, e.district)
            | self.mask_of(cc);
        let all = (1u64 << self.words.len()) - 1;
        m & PHRASE_BIT != 0 || m & all == all
    }
}

struct Pool {
    best: HashMap<u32, Hit>,
    heap: BinaryHeap<Reverse<Ord32>>,
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
        match self.best.get(&h.entity) {
            Some(old) if old.score >= h.score => return,
            _ => {}
        }
        self.heap.push(Reverse(Ord32(h.score)));
        if self.heap.len() > CANDIDATE_POOL {
            self.heap.pop();
        }
        self.best.insert(h.entity, h);
    }
}

impl<'a> Searcher<'a> {
    pub fn new(idx: &'a Index) -> Self {
        Searcher { idx, lev: [LevenshteinAutomatonBuilder::new(1, true), LevenshteinAutomatonBuilder::new(2, true)] }
    }

    fn stream_dfa(&self, phrase: &str, edits: u8, dfa: &DFA, typed: usize, prefix: bool, out: &mut KeyMatches) {
        let anchor_chars = if edits >= 2 { ANCHOR_CHARS_TWO_EDITS } else { ANCHOR_CHARS_ONE_EDIT };
        let first: usize = phrase.chars().take(anchor_chars).map(|c| c.len_utf8()).sum();
        let aut = Anchored { first: &phrase.as_bytes()[..first], dfa };
        let t0 = std::time::Instant::now();
        let before = out.len();
        let mut s = self.idx.map.search_with_state(aut).into_stream();
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
        if trace() {
            eprintln!("  dfa {phrase:?} edits={edits} prefix={prefix} keys={n} new={} {:.0}us", out.len() - before, t0.elapsed().as_secs_f64() * 1e6);
        }
    }

    /// Keys matching `phrase` as a whole name (with typos by length) and, when the user may
    /// still be typing it, as a prefix.
    fn lookup(&self, phrase: &str, prefix: bool, out: &mut KeyMatches) {
        let chars = phrase.chars().count();
        let ideo = is_ideographic(phrase);
        match allowed_edits(chars, EXACT_NO_TYPO_MAX_CHARS, EXACT_ONE_TYPO_MAX_CHARS, ideo) {
            0 => {
                if let Some(v) = self.idx.map.get(phrase) {
                    out.insert(v, 1.0);
                }
            }
            d => {
                let t = std::time::Instant::now();
                let dfa = self.lev[d as usize - 1].build_dfa(phrase);
                if trace() {
                    eprintln!("  build dfa {phrase:?} edits={d} {:.0}us", t.elapsed().as_secs_f64() * 1e6);
                }
                self.stream_dfa(phrase, d, &dfa, chars, false, out)
            }
        }
        if !prefix {
            return;
        }
        match allowed_edits(chars, PREFIX_NO_TYPO_MAX_CHARS, PREFIX_ONE_TYPO_MAX_CHARS, ideo) {
            0 => {
                let aut = fst::automaton::Str::new(phrase).starts_with();
                let mut s = self.idx.map.search(aut).into_stream();
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
                let t = std::time::Instant::now();
                let dfa = self.lev[d as usize - 1].build_prefix_dfa(phrase);
                if trace() {
                    eprintln!("  build prefix dfa {phrase:?} edits={d} {:.0}us", t.elapsed().as_secs_f64() * 1e6);
                }
                self.stream_dfa(phrase, d, &dfa, chars, true, out)
            }
        }
    }

    fn score(&self, text: f32, imp: f32, class: u8, ctx_ok: bool) -> f32 {
        let ctx = if ctx_ok { 1.0 } else { CONTEXT_MISS_MULT };
        text * text * (IMPORTANCE_FLOOR + imp) * CLASS_MULT[class as usize] * ctx
    }

    fn score_split(&self, split: u8, name: &[Token], ctx_words: &[String], prefix: bool, pool: &mut Pool) {
        let idx = self.idx;
        let phrase = join(name);
        if phrase.is_empty() {
            return;
        }
        let mut ctx = Context { words: ctx_words, phrase: ctx_words.join(" "), cache: HashMap::new() };
        let has_ctx = !ctx_words.is_empty();
        let offer = |pool: &mut Pool, entity: u32, ord: u16, text: f32, ctx: &mut Context| {
            let e = &idx.entities[entity as usize];
            let (_, class) = idx.name(entity, ord);
            // The context check is the costly part: skip it when even a match would not make
            // the pool.
            if self.score(text, e.imp, class, true) <= pool.threshold() {
                return;
            }
            let ok = !has_ctx || ctx.matches(idx, e);
            pool.offer(Hit { entity, ord, score: self.score(text, e.imp, class, ok), text, context: has_ctx && ok, split });
        };

        let chars = phrase.chars().count();
        let folded_last = name.last().is_some_and(|t| t.raw != t.folded);
        if prefix && chars <= PREFIX_TABLE_MAX_CHARS && !folded_last {
            match idx.prefix_table.get(&phrase) {
                Some(hits) => {
                    for h in hits {
                        offer(pool, h.entity, h.ord, h.text, &mut ctx);
                    }
                    return;
                }
                // Short prefixes are all in the table: no entry means no key starts with it.
                None if chars <= PREFIX_TABLE_ALL_CHARS => return,
                None => {}
            }
        }
        let mut keys = KeyMatches::new();
        self.lookup(&phrase, prefix, &mut keys);
        // The user may still be typing a word the folding changed ("fort" -> "ft").
        if prefix {
            if let Some(last) = name.last() {
                if last.raw != last.folded {
                    let mut raw = name.to_vec();
                    raw.last_mut().unwrap().folded = last.raw.clone();
                    self.lookup(&join(&raw), true, &mut keys);
                }
            }
        }
        let max_ctx = 1.0;
        let mut order: Vec<(u64, f32, f32)> = keys
            .into_iter()
            .map(|(k, t)| {
                let top = idx.postings_of(k).first().map(|p| dequantize(p.imp_q)).unwrap_or(0.0);
                (k, t, t * t * (IMPORTANCE_FLOOR + top) * max_ctx)
            })
            .collect();
        order.sort_by(|a, b| b.2.total_cmp(&a.2));
        for (k, t, bound) in order {
            if bound <= pool.threshold() {
                break;
            }
            for p in idx.postings_of(k) {
                let text = if p.partial > 0 { t * PARTIAL_TEXT } else { t };
                if text * text * (IMPORTANCE_FLOOR + dequantize(p.imp_q)) * max_ctx <= pool.threshold() {
                    if p.partial == 0 {
                        break;
                    }
                    continue;
                }
                offer(pool, p.entity, p.ord, text, &mut ctx);
            }
        }
    }

    pub fn search(&self, q: &str, limit: usize) -> Vec<Found> {
        let t0 = std::time::Instant::now();
        let mut pool = Pool { best: HashMap::new(), heap: BinaryHeap::new() };
        let mut phrases: Vec<String> = Vec::new();
        if let Some((name, rest)) = q.split_once(',') {
            let name = tokens(name);
            let ctx: Vec<String> = tokens(rest).into_iter().map(|t| t.folded).take(63).collect();
            phrases.push(join(&name));
            self.score_split(0, &name, &ctx, ctx.is_empty(), &mut pool);
        } else {
            let t = tokens(q);
            let n = t.len();
            for k in (1..=n).rev() {
                if n - k > MAX_CONTEXT_WORDS {
                    break;
                }
                let ctx: Vec<String> = t[k..].iter().map(|x| x.folded.clone()).collect();
                phrases.push(join(&t[..k]));
                self.score_split((phrases.len() - 1) as u8, &t[..k], &ctx, k == n, &mut pool);
            }
        }
        if trace() {
            eprintln!("  scored {} candidates {:.0}us", pool.best.len(), t0.elapsed().as_secs_f64() * 1e6);
        }
        let mut hits: Vec<Hit> = pool.best.into_values().collect();
        hits.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.entity.cmp(&b.entity)));
        self.place_before_its_area(&mut hits);
        let mut out: Vec<Found> = Vec::new();
        let mut kept: Vec<(String, (f32, f32))> = Vec::new();
        for h in hits {
            let e = &self.idx.entities[h.entity as usize];
            let name = self.display_name(&h, &phrases[h.split as usize]).to_string();
            let label = crate::normalize::normalize(&name);
            let at = (e.lat, e.lon);
            if kept.iter().any(|(l, p)| *l == label && haversine_km(*p, at) < DEDUPE_KM) {
                continue;
            }
            kept.push((label, at));
            out.push(Found { hit: h, name });
            if out.len() >= limit {
                break;
            }
        }
        if trace() {
            eprintln!("  done {:.0}us", t0.elapsed().as_secs_f64() * 1e6);
        }
        out
    }

    /// "Dubai" the city before "Dubai" the emirate: an area (state, county, country, unlinked
    /// boundary) drops just below a settlement of the same name in the same state, the city a
    /// searcher means and a better point for a photo than the area's label point.
    fn place_before_its_area(&self, hits: &mut [Hit]) {
        let idx = self.idx;
        let is_area = |h: &Hit| {
            let e = &idx.entities[h.entity as usize];
            e.kind == b'A' || AREA_TYPES.contains(&idx.types[e.type_id as usize].as_str())
        };
        let label = |h: &Hit| crate::normalize::normalize(idx.primary_name(h.entity));
        let mut changed = false;
        for i in 0..hits.len().min(AREA_CHECK_TOP) {
            if !is_area(&hits[i]) {
                continue;
            }
            let (area_label, area_state) = (label(&hits[i]), idx.entities[hits[i].entity as usize].state);
            let place = hits[i + 1..].iter().take(AREA_CHECK_TOP).find(|s| {
                !is_area(s) && idx.entities[s.entity as usize].state == area_state && label(s) == area_label
            });
            if let Some(s) = place {
                hits[i].score = s.score * 0.999;
                changed = true;
            }
        }
        if changed {
            hits.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.entity.cmp(&b.entity)));
        }
    }

    /// The first of `name`, `name:en` and the matched name that starts with what was typed (the
    /// matched one is a language the user chose: "Moskau"); after a typo `name:en`, then `name`.
    pub fn display_name(&self, h: &Hit, phrase: &str) -> &str {
        let idx = self.idx;
        let a = idx.names_start[h.entity as usize];
        let b = idx.names_start[h.entity as usize + 1];
        let of_class =
            |c: u8| (a..b).find(|g| idx.name_class[*g as usize] == c).map(|g| idx.name(h.entity, (g - a) as u16).0);
        let (primary, en) = (of_class(0), of_class(1));
        let (matched, _) = idx.name(h.entity, h.ord);
        for n in [primary, en, Some(matched)].into_iter().flatten() {
            if crate::normalize::normalize(n).starts_with(phrase) {
                return n;
            }
        }
        en.or(primary).unwrap_or(matched)
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
        assert!(damerau_le1("texas", "texas"));
        assert!(!damerau_le1("texas", "tesax"));
    }

    #[test]
    fn edits_by_length() {
        assert_eq!(allowed_edits(3, 3, 7, false), 0);
        assert_eq!(allowed_edits(5, 3, 7, false), 1);
        assert_eq!(allowed_edits(9, 3, 7, false), 2);
        assert_eq!(allowed_edits(9, 3, 7, true), 0);
    }
}
