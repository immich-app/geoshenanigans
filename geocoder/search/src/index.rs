use crate::constants::*;
use crate::normalize::{normalize, tokens, transliterate};

/// Tag key of the synthetic transliterated name (class 3, an alternate).
pub const TRANSLITERATION_KEY: &str = "translit";
#[cfg(test)]
mod tests {
    use super::class_of;

    #[test]
    fn classes_by_key_and_language() {
        assert_eq!(class_of("name"), 0);
        assert_eq!(class_of("name:en"), 1);
        assert_eq!(class_of("name:de"), 2);
        assert_eq!(class_of("name:zh-Hans"), 2);
        assert_eq!(class_of("alt_name"), 3);
        assert_eq!(class_of("alt_name:fr"), 3);
        assert_eq!(class_of("old_name"), 4);
        assert_eq!(class_of("name:tg"), 5);
        assert_eq!(class_of("alt_name:vi-Latn"), 3);
        assert_eq!(class_of("official_name:pdc"), 5);
    }
}
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

pub const NONE: u32 = u32::MAX;

#[derive(Clone, Copy, Debug)]
pub struct Entity {
    pub lat: f32,
    pub lon: f32,
    pub imp: f32,
    pub kind: u8,
    pub cc: [u8; 2],
    pub type_id: u32,
    /// String ids (0 is the empty string).
    pub country: u32,
    pub state: u32,
    pub county: u32,
    pub city: u32,
    /// Context alias groups (NONE when the printed name matched no ISO3166 relation).
    pub country_grp: u32,
    pub state_grp: u32,
    /// osm type letter << 56 | id
    pub osm: u64,
}

/// One entity under one key. `ord` is the name's position in the entity's name list.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Posting {
    pub entity: u32,
    pub ord: u16,
    pub imp_q: u8,
    pub partial: u8,
}

#[derive(Clone, Copy, Debug)]
pub struct PrefixHit {
    pub entity: u32,
    pub ord: u16,
    pub partial: u8,
    pub text: f32,
}

pub struct AliasGroup {
    pub set: HashSet<String>,
    pub sorted: Vec<String>,
}

pub struct Index {
    pub entities: Vec<Entity>,
    /// Names of entity e: names_start[e]..names_start[e + 1] into name_off / name_class.
    pub names_start: Vec<u32>,
    pub name_off: Vec<u32>,
    pub name_text: String,
    pub name_class: Vec<u8>,
    pub strings: Vec<String>,
    pub strings_norm: Vec<String>,
    pub types: Vec<String>,
    pub groups: Vec<AliasGroup>,
    pub map: fst::Map<Vec<u8>>,
    /// Postings of key k: postings[post_off[k]..post_off[k + 1]], best importance first.
    pub post_off: Vec<u32>,
    pub postings: Vec<Posting>,
    pub prefix_table: HashMap<String, Vec<PrefixHit>>,
    pub stats: Vec<(String, String)>,
}

pub fn class_of(key: &str) -> u8 {
    let (base, suf) = key.split_once(':').unwrap_or((key, ""));
    let lang = suf.split(['-', '_']).next().unwrap_or("");
    if !suf.is_empty() && suf != "en" && !MAJOR_LANGUAGES.contains(&lang) {
        return 5;
    }
    match base {
        "name" if suf.is_empty() => 0,
        "name" if suf == "en" => 1,
        "name" => 2,
        "old_name" => 4,
        _ => 3,
    }
}

pub fn quantize(imp: f32) -> u8 {
    (imp.clamp(0.0, 1.27) * 200.0).round() as u8
}

pub fn dequantize(q: u8) -> f32 {
    q as f32 / 200.0
}

fn log_score(v: f64, full: f32) -> f32 {
    ((v.max(0.0) + 1.0).log10() as f32 / full).clamp(0.0, 1.0)
}

fn type_base(kind: u8, typ: &str, level: u32) -> f32 {
    if kind == b'A' {
        return match level {
            2 => 0.90,
            3 | 4 => 0.65,
            5 => 0.50,
            6 => 0.45,
            7 => 0.35,
            _ => 0.25,
        };
    }
    match typ {
        "country" => 0.90,
        "state" | "province" => 0.70,
        "region" => 0.60,
        "city" => 0.55,
        "county" => 0.45,
        "town" => 0.40,
        "municipality" | "district" | "archipelago" => 0.35,
        "island" | "borough" => 0.30,
        "village" => 0.25,
        "suburb" => 0.22,
        "quarter" => 0.16,
        "hamlet" => 0.12,
        "neighbourhood" | "locality" | "islet" => 0.10,
        _ => 0.25,
    }
}

/// Row columns: kind osm type lat lon population sitelinks capital admin_level poi_importance
/// poi_flags names cc country state county city (see corpus.py / ctx.py).
fn importance(c: &[&str]) -> f32 {
    let kind = c[0].as_bytes()[0];
    if kind == b'O' {
        let raw: f64 = c[9].parse().unwrap_or(0.0);
        let sl: f64 = c[6].parse().unwrap_or(0.0);
        return POI_IMPORTANCE_FLOOR
            + W_POI_BUILDER * ((1.0 + raw).log2() / 8.0).clamp(0.0, 1.0) as f32
            + W_POI_SITELINKS * log_score(sl, SITELINKS_LOG_FULL);
    }
    let level: u32 = c[8].parse().unwrap_or(0);
    let pop: f64 = c[5].parse().unwrap_or(0.0);
    let sl: f64 = c[6].parse().unwrap_or(0.0);
    let cap = match c[7] {
        "yes" | "2" => CAPITAL_NATIONAL_BONUS,
        "3" | "4" => CAPITAL_REGIONAL_BONUS,
        _ => 0.0,
    };
    let area = kind == b'A' || matches!(c[2], "country" | "state" | "province" | "region" | "county");
    let pop_weight = if area { W_POPULATION * AREA_POPULATION_FACTOR } else { W_POPULATION };
    W_TYPE * type_base(kind, c[2], level)
        + pop_weight * log_score(pop, POPULATION_LOG_FULL)
        + W_SITELINKS * log_score(sl, SITELINKS_LOG_FULL)
        + cap
}

struct Parsed {
    imp: f32,
    lat: f32,
    lon: f32,
    /// (tag key, value)
    names: Vec<(String, String)>,
    /// (normalized key, name ord, partial)
    keys: Vec<(String, u16, u8)>,
}

/// PS_MAJOR_LANGUAGES_ONLY=1 drops names in languages outside MAJOR_LANGUAGES (class 5): the
/// size/recall trade-off measurement.
fn major_languages_only() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| std::env::var_os("PS_MAJOR_LANGUAGES_ONLY").is_some())
}

fn parse_row(line: &str) -> Option<Parsed> {
    let c: Vec<&str> = line.split('\t').collect();
    if c.len() < 17 {
        return None;
    }
    let mut names = Vec::new();
    let major_only = major_languages_only();
    for kv in c[11].split('\x1f') {
        let Some((k, v)) = kv.split_once('=') else { continue };
        if names.len() >= u16::MAX as usize - 1 {
            break;
        }
        if major_only && class_of(k) == 5 {
            continue;
        }
        names.push((k.to_string(), v.to_string()));
    }
    // Places named only in Cyrillic or Greek get their Latin spelling as an alternate name.
    if let Some(latin) = names.iter().find(|(k, _)| k == "name").and_then(|(_, v)| transliterate(v, c[12])) {
        names.push((TRANSLITERATION_KEY.to_string(), latin));
    }
    // A key belongs to the entity's best claim on it: a whole name before a trailing-word
    // match, then the better name class ("new york" is NYC's name, not the tail of an alt_name).
    let mut by_class: Vec<(u8, u16, Vec<crate::normalize::Token>)> = names
        .iter()
        .enumerate()
        .map(|(ord, (k, v))| (class_of(k), ord as u16, tokens(v)))
        .filter(|x| !x.2.is_empty())
        .collect();
    by_class.sort_by_key(|x| (x.0, x.1));
    let mut keys = Vec::new();
    let mut seen = HashSet::new();
    for (_, ord, toks) in &by_class {
        let full = crate::normalize::join(toks);
        if seen.insert(full.clone()) {
            keys.push((full, *ord, 0));
        }
    }
    for (_, ord, toks) in &by_class {
        if toks.len() > PARTIAL_MAX_WORDS {
            continue;
        }
        for j in 1..toks.len() {
            let suffix = crate::normalize::join(&toks[j..]);
            if suffix.chars().count() >= PARTIAL_MIN_CHARS && seen.insert(suffix.clone()) {
                keys.push((suffix, *ord, 1));
            }
        }
    }
    Some(Parsed { imp: importance(&c), lat: c[3].parse().ok()?, lon: c[4].parse().ok()?, names, keys })
}

struct Interner {
    map: HashMap<String, u32>,
    list: Vec<String>,
}

impl Interner {
    fn new() -> Self {
        let mut s = Interner { map: HashMap::new(), list: Vec::new() };
        s.id("");
        s
    }
    fn id(&mut self, v: &str) -> u32 {
        if let Some(&i) = self.map.get(v) {
            return i;
        }
        let i = self.list.len() as u32;
        self.map.insert(v.to_string(), i);
        self.list.push(v.to_string());
        i
    }
}

fn osm_key(s: &str) -> u64 {
    let (t, id) = s.split_at(1);
    ((t.as_bytes()[0] as u64) << 56) | id.parse::<u64>().unwrap_or(0)
}

fn load_groups(path: &str) -> (Vec<AliasGroup>, HashMap<(String, String, String), u32>) {
    let mut groups = Vec::new();
    let mut lookup = HashMap::new();
    for line in std::fs::read_to_string(path).expect("aliases").lines() {
        let c: Vec<&str> = line.split('\t').collect();
        if c.len() < 3 {
            continue;
        }
        let gid = groups.len() as u32;
        let mut set = HashSet::new();
        for a in c[2].split('\x1f') {
            let n = normalize(a);
            if !n.is_empty() {
                set.insert(n);
            }
        }
        for n in &set {
            lookup.entry((c[0].to_string(), c[1].to_string(), n.clone())).or_insert(gid);
        }
        let mut sorted: Vec<String> = set.iter().cloned().collect();
        sorted.sort();
        groups.push(AliasGroup { set, sorted });
    }
    (groups, lookup)
}

pub fn prefix_text(typed: usize, key_len: usize, edits: u8) -> f32 {
    let r = (typed as f32 / key_len.max(1) as f32).min(1.0);
    (PREFIX_BASE + PREFIX_COMPLETION * r) * PREFIX_TYPO.powi(edits as i32)
}

pub fn char_len(b: &[u8]) -> usize {
    b.iter().filter(|x| (**x & 0xC0) != 0x80).count()
}

impl Index {
    pub fn build(corpus: &str, aliases: &str) -> Index {
        let mut stats = Vec::new();
        let t0 = Instant::now();
        let text = std::fs::read_to_string(corpus).expect("corpus");
        let lines: Vec<&str> = text.lines().collect();
        let t_read = t0.elapsed();
        let parsed: Vec<Option<Parsed>> = lines.par_iter().map(|l| parse_row(l)).collect();
        let t_parse = t0.elapsed();
        let (groups, group_lookup) = load_groups(aliases);

        let mut strings = Interner::new();
        let mut types = Interner::new();
        let mut entities = Vec::with_capacity(lines.len());
        let mut names_start = vec![0u32];
        let mut name_off = vec![0u32];
        let mut name_text = String::new();
        let mut name_class = Vec::new();
        let mut key_arena: Vec<u8> = Vec::new();
        // (arena offset, len, posting)
        let mut entries: Vec<(u64, u32, Posting)> = Vec::new();
        for (line, p) in lines.iter().zip(parsed) {
            let Some(p) = p else { continue };
            if p.keys.is_empty() {
                continue;
            }
            let c: Vec<&str> = line.split('\t').collect();
            let e = entities.len() as u32;
            let cc = c[12].as_bytes();
            let grp = |level: &str, printed: &str| {
                group_lookup
                    .get(&(c[12].to_string(), level.to_string(), normalize(printed)))
                    .copied()
                    .unwrap_or(NONE)
            };
            entities.push(Entity {
                lat: p.lat,
                lon: p.lon,
                imp: p.imp,
                kind: c[0].as_bytes()[0],
                cc: [cc.first().copied().unwrap_or(b' '), cc.get(1).copied().unwrap_or(b' ')],
                type_id: types.id(c[2]),
                country: strings.id(c[13]),
                state: strings.id(c[14]),
                county: strings.id(c[15]),
                city: strings.id(c[16]),
                country_grp: grp("country", c[13]),
                state_grp: grp("state", c[14]),
                osm: osm_key(c[1]),
            });
            for (k, v) in &p.names {
                name_text.push_str(v);
                name_off.push(name_text.len() as u32);
                name_class.push(class_of(k));
            }
            names_start.push(name_off.len() as u32 - 1);
            let imp_q = quantize(p.imp);
            for (key, ord, partial) in p.keys {
                entries.push((key_arena.len() as u64, key.len() as u32, Posting { entity: e, ord, imp_q, partial }));
                key_arena.extend_from_slice(key.as_bytes());
            }
        }
        drop(text);
        let t_tables = t0.elapsed();

        let arena = &key_arena;
        let key_of = |x: &(u64, u32, Posting)| &arena[x.0 as usize..x.0 as usize + x.1 as usize];
        entries.par_sort_unstable_by(|a, b| key_of(a).cmp(key_of(b)));
        let t_sort = t0.elapsed();

        let mut builder = fst::MapBuilder::memory();
        let mut post_off = vec![0u32];
        let mut postings: Vec<Posting> = Vec::with_capacity(entries.len());
        let mut i = 0;
        let mut nkeys = 0u64;
        while i < entries.len() {
            let k = key_of(&entries[i]);
            let mut j = i;
            while j < entries.len() && key_of(&entries[j]) == k {
                j += 1;
            }
            let mut group: Vec<Posting> = entries[i..j].iter().map(|x| x.2).collect();
            // One posting per entity: whole-name match over trailing words, then name class.
            let rank = |p: &Posting| (p.partial, name_class[(names_start[p.entity as usize] + p.ord as u32) as usize]);
            group.sort_by(|a, b| a.entity.cmp(&b.entity).then(rank(a).cmp(&rank(b))));
            group.dedup_by_key(|p| p.entity);
            group.sort_by(|a, b| b.imp_q.cmp(&a.imp_q).then(a.partial.cmp(&b.partial)));
            let mut partial_kept = 0;
            for p in group {
                if p.partial > 0 {
                    if partial_kept >= PARTIAL_CAP {
                        continue;
                    }
                    partial_kept += 1;
                }
                postings.push(p);
            }
            builder.insert(k, nkeys).expect("sorted keys");
            post_off.push(postings.len() as u32);
            nkeys += 1;
            i = j;
        }
        let n_entries = entries.len();
        drop(entries);
        let map = fst::Map::new(builder.into_inner().expect("fst")).expect("fst");
        let t_fst = t0.elapsed();

        let mut idx = Index {
            entities,
            names_start,
            name_off,
            name_text,
            name_class,
            strings_norm: strings.list.iter().map(|s| normalize(s)).collect(),
            strings: strings.list,
            types: types.list,
            groups,
            map,
            post_off,
            postings,
            prefix_table: HashMap::new(),
            stats: Vec::new(),
        };
        drop(key_arena);
        idx.prefix_table = idx.build_prefix_table();
        let t_prefix = t0.elapsed();

        let mb = |b: usize| format!("{:.1} MB", b as f64 / 1e6);
        stats.push(("entities".into(), idx.entities.len().to_string()));
        stats.push(("names".into(), idx.name_class.len().to_string()));
        stats.push(("key entries before dedupe".into(), n_entries.to_string()));
        stats.push(("keys".into(), nkeys.to_string()));
        stats.push(("postings".into(), idx.postings.len().to_string()));
        stats.push(("prefix table prefixes".into(), idx.prefix_table.len().to_string()));
        stats.push(("fst bytes".into(), mb(idx.map.as_fst().as_bytes().len())));
        stats.push(("postings bytes".into(), mb(idx.postings.len() * std::mem::size_of::<Posting>() + idx.post_off.len() * 4)));
        stats.push(("entity table bytes".into(), mb(idx.entities.len() * std::mem::size_of::<Entity>())));
        stats.push(("name arena bytes".into(), mb(idx.name_text.len() + idx.name_off.len() * 4 + idx.name_class.len() + idx.names_start.len() * 4)));
        let secs = |d: std::time::Duration| format!("{:.2} s", d.as_secs_f64());
        stats.push(("time read".into(), secs(t_read)));
        stats.push(("time parse+normalize".into(), secs(t_parse - t_read)));
        stats.push(("time tables".into(), secs(t_tables - t_parse)));
        stats.push(("time sort".into(), secs(t_sort - t_tables)));
        stats.push(("time fst+postings".into(), secs(t_fst - t_sort)));
        stats.push(("time prefix table".into(), secs(t_prefix - t_fst)));
        stats.push(("time total build".into(), secs(t_prefix)));
        idx.stats = stats;
        idx
    }

    pub fn postings_of(&self, k: u64) -> &[Posting] {
        &self.postings[self.post_off[k as usize] as usize..self.post_off[k as usize + 1] as usize]
    }

    pub fn name(&self, entity: u32, ord: u16) -> (&str, u8) {
        let g = (self.names_start[entity as usize] + ord as u32) as usize;
        (&self.name_text[self.name_off[g] as usize..self.name_off[g + 1] as usize], self.name_class[g])
    }

    pub fn primary_name(&self, entity: u32) -> &str {
        let a = self.names_start[entity as usize];
        let b = self.names_start[entity as usize + 1];
        for g in a..b {
            if self.name_class[g as usize] == 0 {
                return self.name(entity, (g - a) as u16).0;
            }
        }
        self.name(entity, 0).0
    }

    fn build_prefix_table(&self) -> HashMap<String, Vec<PrefixHit>> {
        struct Slot {
            prefix: String,
            chars: usize,
            keys: usize,
            hits: Vec<(f32, PrefixHit)>,
        }
        let mut table: HashMap<String, Vec<PrefixHit>> = HashMap::new();
        let mut open: Vec<Slot> = (1..=PREFIX_TABLE_MAX_CHARS)
            .map(|chars| Slot { prefix: String::new(), chars, keys: 0, hits: Vec::new() })
            .collect();
        let flush = |table: &mut HashMap<String, Vec<PrefixHit>>, slot: &mut Slot| {
            let mut v = std::mem::take(&mut slot.hits);
            let keys = std::mem::take(&mut slot.keys);
            if slot.prefix.is_empty() || (slot.chars > PREFIX_TABLE_ALL_CHARS && keys < HEAVY_PREFIX_MIN_KEYS) {
                return;
            }
            v.sort_by(|a, b| b.0.total_cmp(&a.0));
            let mut seen = HashSet::new();
            let hits: Vec<PrefixHit> =
                v.into_iter().filter(|(_, h)| seen.insert(h.entity)).take(PREFIX_TABLE_TOP).map(|x| x.1).collect();
            table.insert(std::mem::take(&mut slot.prefix), hits);
        };
        let mut stream = self.map.stream();
        use fst::Streamer;
        while let Some((k, v)) = stream.next() {
            let key = std::str::from_utf8(k).unwrap_or("");
            let klen = key.chars().count();
            for n in 1..=PREFIX_TABLE_MAX_CHARS.min(klen) {
                let pfx: String = key.chars().take(n).collect();
                let slot = &mut open[n - 1];
                if slot.prefix != pfx {
                    flush(&mut table, slot);
                    slot.prefix = pfx;
                }
                slot.keys += 1;
                let text = if n == klen { 1.0 } else { prefix_text(n, klen, 0) };
                for p in self.postings_of(v).iter().take(PREFIX_TABLE_TOP) {
                    let t = if p.partial > 0 { text * PARTIAL_TEXT } else { text };
                    let class = self.name(p.entity, p.ord).1;
                    let s = t * t * (IMPORTANCE_FLOOR + dequantize(p.imp_q)) * CLASS_MULT[class as usize];
                    slot.hits.push((s, PrefixHit { entity: p.entity, ord: p.ord, partial: p.partial, text: t }));
                }
                if slot.hits.len() > PREFIX_TABLE_TOP * 64 {
                    slot.hits.sort_by(|a, b| b.0.total_cmp(&a.0));
                    slot.hits.truncate(PREFIX_TABLE_TOP * 4);
                }
            }
        }
        for slot in open.iter_mut() {
            flush(&mut table, slot);
        }
        table
    }
}
