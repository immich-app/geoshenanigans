//! Index build from the geocoder's own files. Entities stream out of the mapped records, their
//! names become normalized keys, keys are sorted in bounded-memory runs (spilled to disk) and
//! merged into an fst (written streaming) with postings and the short-prefix table.
//!
//! Output per sub-index `<name>` (places / poi): `<name>.fst` keys -> value (bit 0 set: the
//! key's one posting inline, else the byte offset of its list in `<name>.post`),
//! `<name>.post`, `<name>.pfx.fst` + `<name>.pfx` (prefix table), and for places `places.bnd`
//! (boundary slots). Nothing else is stored: names, points and types stay in the geocoder files.
use crate::constants::*;
use crate::entities::*;
use crate::extras::{record_ref, Extras, KIND_PLACE_NODE, KIND_POI, KIND_POLYGON};
use crate::model::*;
use crate::normalize::{join, tokens, transliterate};
use memmap2::Mmap;
use query_server::{AdminPolygon, Index, NodeCoord, PlaceNode, PoiRecord, NO_DATA};
use std::collections::{BinaryHeap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

pub struct BuildOpts {
    pub threads: usize,
    pub mem_mb: usize,
    pub tmp: PathBuf,
    /// Single-posting keys carry their posting in the fst value.
    pub inline: bool,
    pub extras: Option<String>,
    /// Directory with imp_places.u8 / imp_polys.u8 / imp_poi.u8 (the shipped importance bytes).
    pub imp_dir: Option<String>,
    pub area_proxy: bool,
    /// POIs of tiers above this are left out (3 = all).
    pub poi_max_tier: u8,
}

#[derive(Default, Debug)]
pub struct Stats {
    pub lines: Vec<(String, String)>,
}

impl Stats {
    fn add(&mut self, k: &str, v: impl ToString) {
        self.lines.push((k.to_string(), v.to_string()));
    }
}

fn map_opt(path: Option<String>) -> Option<Mmap> {
    let f = File::open(path?).ok()?;
    let m = unsafe { Mmap::map(&f) }.ok()?;
    Some(m)
}

fn byte_at(m: &Option<Mmap>, i: u32) -> u8 {
    m.as_ref().and_then(|m| m.get(i as usize).copied()).unwrap_or(0)
}

fn ring_centroid(v: &[NodeCoord]) -> (f32, f32) {
    let n = v.len();
    let mean = || {
        let (mut a, mut b) = (0f64, 0f64);
        for p in v {
            a += p.lat as f64;
            b += p.lng as f64;
        }
        ((a / n as f64) as f32, (b / n as f64) as f32)
    };
    if n < 3 {
        return mean();
    }
    let (mut area, mut cx, mut cy) = (0f64, 0f64, 0f64);
    for i in 0..n {
        let (x0, y0) = (v[i].lng as f64, v[i].lat as f64);
        let (x1, y1) = (v[(i + 1) % n].lng as f64, v[(i + 1) % n].lat as f64);
        let cr = x0 * y1 - x1 * y0;
        area += cr;
        cx += (x0 + x1) * cr;
        cy += (y0 + y1) * cr;
    }
    area /= 2.0;
    if area.abs() < 1e-12 {
        return mean();
    }
    ((cy / (6.0 * area)) as f32, (cx / (6.0 * area)) as f32)
}

// ------------------------------------------------------------------ entity sources

/// One entity's index input.
#[derive(Default)]
struct EntityBuf {
    id: u32,
    imp: f32,
    cc: [u8; 2],
    /// (class, ord, transliterate?, name)
    names: Vec<(u8, u8, bool, String)>,
}

pub struct PlacesSource<'a> {
    geo: &'a Index,
    n_place_nodes: u32,
    slots: Vec<BoundarySlot>,
    link: Vec<u32>,
    /// label nodes that name a boundary slot (indexed through the boundary, not on their own)
    label_used: Vec<bool>,
    imp_places: Option<Mmap>,
    imp_polys: Option<Mmap>,
    extras: Option<&'a Extras>,
    area_proxy: bool,
}

impl<'a> PlacesSource<'a> {
    pub fn new(geo: &'a Index, opts: &BuildOpts, extras: Option<&'a Extras>) -> Self {
        let n_place_nodes = record_count::<PlaceNode>(geo.place_nodes.as_ref().expect("place_nodes"));
        let n_poly = record_count::<AdminPolygon>(&geo.admin_polygons);
        // A hidden node that names its claiming boundary is that boundary's label point.
        let mut link = vec![NO_DATA; n_poly as usize];
        for i in 0..n_place_nodes {
            let pn = place_node_raw(geo, i);
            if is_hidden(&pn) && pn._pad1 & PLACE_FLAG_LABEL_OF != 0 && pn.parent_poly_id < n_poly {
                let l = &mut link[pn.parent_poly_id as usize];
                if *l == NO_DATA {
                    *l = i;
                }
            }
        }
        let mut slots = Vec::new();
        let mut label_used = vec![false; n_place_nodes as usize];
        // Sets built without the search flags: every named ring is its own boundary entity.
        let flagged = (0..n_poly).any(|p| polygon_raw(geo, p)._pad2 & POLY_FLAG_PRIMARY_RING != 0);
        for p in 0..n_poly {
            let ap = polygon_raw(geo, p);
            if ap.name_id == NO_DATA || (flagged && ap._pad2 & POLY_FLAG_PRIMARY_RING == 0) {
                continue;
            }
            if link[p as usize] != NO_DATA {
                let pn = place_node_raw(geo, link[p as usize]);
                label_used[link[p as usize] as usize] = true;
                slots.push(BoundarySlot { poly: p, node: link[p as usize], lat: pn.lat, lng: pn.lng });
            } else if (BOUNDARY_MIN_LEVEL..=BOUNDARY_MAX_LEVEL).contains(&ap.admin_level) && ap.area > 0.0 {
                let Some(v) = geo.admin_verts(&ap) else { continue };
                let (lat, lng) = ring_centroid(&v);
                slots.push(BoundarySlot { poly: p, node: NO_DATA, lat, lng });
            }
        }
        let dir = opts.imp_dir.clone();
        PlacesSource {
            geo,
            n_place_nodes,
            slots,
            link,
            label_used,
            imp_places: map_opt(dir.as_ref().map(|d| format!("{d}/imp_places.u8"))),
            imp_polys: map_opt(dir.as_ref().map(|d| format!("{d}/imp_polys.u8"))),
            extras,
            area_proxy: opts.area_proxy,
        }
    }

    pub fn write_slots(&self, path: &Path) -> std::io::Result<()> {
        let mut w = BufWriter::new(File::create(path)?);
        for s in &self.slots {
            w.write_all(&s.to_bytes())?;
        }
        w.flush()
    }

    fn len(&self) -> u32 {
        self.slots.len() as u32 + self.n_place_nodes
    }

    fn fill(&self, pos: u32, e: &mut EntityBuf) -> bool {
        e.names.clear();
        let geo = self.geo;
        let (rref, base_id) = if pos < self.slots.len() as u32 {
            let s = self.slots[pos as usize];
            let ap = polygon_raw(geo, s.poly);
            let node = (self.link[s.poly as usize] != NO_DATA).then(|| place_node_raw(geo, self.link[s.poly as usize]));
            let kind = if s.linked() { Kind::Linked } else { Kind::Area };
            let typ = boundary_type(&ap, node.as_ref());
            e.id = self.n_place_nodes + s.poly;
            e.imp = W_TYPE * type_base(kind, &typ, ap.admin_level) + extra_importance(byte_at(&self.imp_polys, s.poly));
            if self.area_proxy {
                e.imp += W_AREA_PROXY * area_proxy(ap.area, s.lat);
            }
            e.cc = cc_of(ap.country_code);
            let (primary, own) = slot_name_ids(geo, &s);
            if let Some(own) = own {
                let n = geo.get_string(own);
                if !n.is_empty() {
                    e.names.push((0, ORD_BOUNDARY_NAME, true, n.to_string()));
                }
            }
            (record_ref(KIND_POLYGON, s.poly), primary)
        } else {
            let i = pos - self.slots.len() as u32;
            let pn = place_node_raw(geo, i);
            // A hidden node is searchable on its own only when no boundary of this set claims it.
            if pn.name_id == NO_DATA || self.label_used[i as usize] {
                return false;
            }
            let kind = place_node_kind(pn.place_type);
            let typ = PLACE_NODE_TYPES.get(pn.place_type as usize).copied().unwrap_or("place");
            e.id = i;
            e.imp = W_TYPE * type_base(kind, typ, 0) + extra_importance(byte_at(&self.imp_places, i));
            e.cc = parent_cc(geo, pn.parent_poly_id);
            (record_ref(KIND_PLACE_NODE, i), pn.name_id)
        };
        fill_names(geo, self.extras, rref, base_id, e)
    }
}

pub struct PoiSource<'a> {
    geo: &'a Index,
    n: u32,
    max_tier: u8,
    imp_poi: Option<Mmap>,
    extras: Option<&'a Extras>,
}

impl<'a> PoiSource<'a> {
    pub fn new(geo: &'a Index, opts: &BuildOpts, extras: Option<&'a Extras>) -> Self {
        let n = record_count::<PoiRecord>(geo.poi_records.as_ref().expect("poi_records"));
        PoiSource { geo, n, max_tier: opts.poi_max_tier, imp_poi: map_opt(opts.imp_dir.as_ref().map(|d| format!("{d}/imp_poi.u8"))), extras }
    }

    fn fill(&self, pos: u32, e: &mut EntityBuf) -> bool {
        e.names.clear();
        let r = poi_raw(self.geo, pos);
        if r.name_id == NO_DATA || r.flags & POI_FLAG_SECONDARY_RING != 0 || r.tier > self.max_tier {
            return false;
        }
        e.id = pos;
        e.imp = poi_importance(r.importance, byte_at(&self.imp_poi, pos));
        e.cc = parent_cc(self.geo, r.parent_poly_id);
        fill_names(self.geo, self.extras, record_ref(KIND_POI, pos), r.name_id, e)
    }
}

fn fill_names(geo: &Index, extras: Option<&Extras>, rref: u32, base_id: u32, e: &mut EntityBuf) -> bool {
    let base = geo.get_string(base_id);
    if !base.is_empty() {
        e.names.insert(0, (0, 0, true, base.to_string()));
    }
    if let Some(x) = extras {
        if let Some(row) = x.find(rref) {
            for (k, n) in x.names(row).into_iter().enumerate() {
                let ord = if k < MAX_EXTRA_ORD as usize { k as u8 + 1 } else { MAX_NAME_ORD };
                e.names.push((n.class, ord, n.key == "name", n.name.to_string()));
            }
        }
    }
    !e.names.is_empty()
}

enum Source<'a> {
    Places(PlacesSource<'a>),
    Poi(PoiSource<'a>),
}

impl Source<'_> {
    fn len(&self) -> u32 {
        match self {
            Source::Places(s) => s.len(),
            Source::Poi(s) => s.n,
        }
    }
    fn fill(&self, pos: u32, e: &mut EntityBuf) -> bool {
        match self {
            Source::Places(s) => s.fill(pos, e),
            Source::Poi(s) => s.fill(pos, e),
        }
    }
}

/// Keys of one entity: each name whole (best class first), then its trailing words
/// ("york" of "new york"); a key belongs to the entity's best claim on it.
fn keys_of(e: &EntityBuf, out: &mut Vec<(String, u8, bool, u8)>) {
    out.clear();
    let cc = cc_str(&e.cc).to_string();
    let mut by_class: Vec<(u8, u8, Vec<crate::normalize::Token>)> = Vec::with_capacity(e.names.len() + 1);
    for (class, ord, tr, name) in &e.names {
        let t = tokens(name);
        if !t.is_empty() {
            by_class.push((*class, *ord, t));
        }
        if *tr {
            if let Some(latin) = transliterate(name, &cc) {
                let t = tokens(&latin);
                if !t.is_empty() {
                    by_class.push((TRANSLITERATION_CLASS, *ord, t));
                }
            }
        }
    }
    by_class.sort_by_key(|x| (x.0, x.1));
    let mut seen: HashSet<String> = HashSet::new();
    for (class, ord, toks) in &by_class {
        let full = join(toks);
        if seen.insert(full.clone()) {
            out.push((full, *ord, false, *class));
        }
    }
    for (class, ord, toks) in &by_class {
        if toks.len() > PARTIAL_MAX_WORDS {
            continue;
        }
        for j in 1..toks.len() {
            let suffix = join(&toks[j..]);
            if suffix.chars().count() >= PARTIAL_MIN_CHARS && seen.insert(suffix.clone()) {
                out.push((suffix, *ord, true, *class));
            }
        }
    }
}

// ------------------------------------------------------------------ sort runs

#[derive(Clone, Copy)]
struct Item {
    off: u32,
    len: u16,
    flags: u8,
    ord: u8,
    entity: u32,
    imp: u8,
}

struct Runs {
    arena: Vec<u8>,
    items: Vec<Item>,
    files: Vec<PathBuf>,
    dir: PathBuf,
    tag: String,
    spilled_bytes: u64,
}

impl Runs {
    fn new(budget: usize, dir: &Path, tag: String) -> Self {
        // Fixed capacities: a run spills when either fills, so memory never exceeds the budget.
        let arena_cap = budget * 6 / 10;
        let items_cap = (budget - arena_cap) / std::mem::size_of::<Item>();
        Runs {
            arena: Vec::with_capacity(arena_cap),
            items: Vec::with_capacity(items_cap.max(1024)),
            files: Vec::new(),
            dir: dir.to_path_buf(),
            tag,
            spilled_bytes: 0,
        }
    }

    fn push(&mut self, key: &[u8], p: Posting) -> std::io::Result<()> {
        if self.arena.len() + key.len() > self.arena.capacity() || self.items.len() == self.items.capacity() {
            self.spill()?;
        }
        let off = self.arena.len() as u32;
        self.arena.extend_from_slice(key);
        self.items.push(Item {
            off,
            len: key.len().min(u16::MAX as usize) as u16,
            flags: (p.partial as u8) | (p.class << 1),
            ord: p.ord,
            entity: p.entity,
            imp: p.imp_q,
        });
        Ok(())
    }

    fn spill(&mut self) -> std::io::Result<()> {
        if self.items.is_empty() {
            return Ok(());
        }
        let arena = &self.arena;
        let key = |it: &Item| &arena[it.off as usize..it.off as usize + it.len as usize];
        self.items.sort_unstable_by(|a, b| key(a).cmp(key(b)).then(a.entity.cmp(&b.entity)));
        let path = self.dir.join(format!("{}-{}.run", self.tag, self.files.len()));
        let mut w = BufWriter::with_capacity(RUN_READ_BUFFER, File::create(&path)?);
        for it in &self.items {
            let k = key(it);
            w.write_all(&it.len.to_le_bytes())?;
            w.write_all(k)?;
            w.write_all(&it.entity.to_le_bytes())?;
            w.write_all(&[it.imp, it.flags, it.ord])?;
            self.spilled_bytes += 2 + k.len() as u64 + 7;
        }
        w.flush()?;
        self.files.push(path);
        self.items.clear();
        self.arena.clear();
        Ok(())
    }
}

struct Head {
    key: Vec<u8>,
    p: Posting,
    run: usize,
}

impl PartialEq for Head {
    fn eq(&self, o: &Self) -> bool {
        self.key == o.key && self.p.entity == o.p.entity
    }
}
impl Eq for Head {}
impl PartialOrd for Head {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Head {
    // reversed: BinaryHeap pops the smallest key first
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        o.key.cmp(&self.key).then(o.p.entity.cmp(&self.p.entity)).then(o.run.cmp(&self.run))
    }
}

struct RunReader {
    r: BufReader<File>,
}

impl RunReader {
    fn read_into(&mut self, h: &mut Head) -> bool {
        let mut len = [0u8; 2];
        if self.r.read_exact(&mut len).is_err() {
            return false;
        }
        let n = u16::from_le_bytes(len) as usize;
        h.key.resize(n, 0);
        self.r.read_exact(&mut h.key).expect("run key");
        let mut rest = [0u8; 7];
        self.r.read_exact(&mut rest).expect("run posting");
        h.p = Posting {
            entity: u32::from_le_bytes(rest[0..4].try_into().unwrap()),
            imp_q: rest[4],
            partial: rest[5] & 1 == 1,
            class: rest[5] >> 1,
            ord: rest[6],
        };
        true
    }
}

// ------------------------------------------------------------------ writer

struct PrefixSlot {
    prefix: String,
    chars: usize,
    keys: usize,
    hits: Vec<(f32, PrefixHit)>,
    /// flushed prefixes of this length, in key order: u16 length, prefix, u32 length, hit blob
    out: BufWriter<File>,
    path: PathBuf,
    flushed: u64,
}

pub fn prefix_text(typed: usize, key_len: usize, edits: u8) -> f32 {
    let r = (typed as f32 / key_len.max(1) as f32).min(1.0);
    (PREFIX_BASE + PREFIX_COMPLETION * r) * PREFIX_TYPO.powi(edits as i32)
}

struct Writer {
    fst: fst::MapBuilder<BufWriter<File>>,
    post: BufWriter<File>,
    post_len: u64,
    scratch: Vec<u8>,
    inline: bool,
    with_names: bool,
    slots: Vec<PrefixSlot>,
    keys: u64,
    postings: u64,
    inline_keys: u64,
    longest: usize,
}

impl Writer {
    fn add(&mut self, key: &[u8], group: &mut Vec<Posting>) -> Result<(), String> {
        // One posting per entity: whole-name match over trailing words, then name class.
        group.sort_unstable_by(|a, b| a.entity.cmp(&b.entity).then((a.partial, a.class).cmp(&(b.partial, b.class))));
        group.dedup_by_key(|p| p.entity);
        group.sort_unstable_by(|a, b| b.imp_q.cmp(&a.imp_q).then(a.partial.cmp(&b.partial)).then(a.entity.cmp(&b.entity)));
        let mut partial_kept = 0;
        group.retain(|p| {
            if !p.partial {
                return true;
            }
            partial_kept += 1;
            partial_kept <= PARTIAL_CAP
        });
        let value = if self.inline && group.len() == 1 {
            self.inline_keys += 1;
            inline_value(&group[0])
        } else {
            self.scratch.clear();
            encode_list(&mut self.scratch, group, self.with_names);
            let v = list_value(self.post_len, group[0].imp_q);
            self.post.write_all(&self.scratch).map_err(|e| e.to_string())?;
            self.post_len += self.scratch.len() as u64;
            v
        };
        self.fst.insert(key, value).map_err(|e| e.to_string())?;
        self.keys += 1;
        self.postings += group.len() as u64;
        self.longest = self.longest.max(group.len());
        self.prefix_add(key, group);
        Ok(())
    }

    fn prefix_add(&mut self, key: &[u8], list: &[Posting]) {
        let Ok(key) = std::str::from_utf8(key) else { return };
        let klen = key.chars().count();
        let mut pfx = String::new();
        for (n, c) in key.chars().take(PREFIX_TABLE_MAX_CHARS).enumerate() {
            pfx.push(c);
            let n = n + 1;
            if self.slots[n - 1].prefix != pfx {
                let slot = &mut self.slots[n - 1];
                flush_slot(slot, self.with_names).expect("prefix temp file");
                slot.prefix = pfx.clone();
            }
            let slot = &mut self.slots[n - 1];
            slot.keys += 1;
            let text = if n == klen { 1.0 } else { prefix_text(n, klen, 0) };
            for p in list.iter().take(PREFIX_TABLE_TOP) {
                let t = if p.partial { text * PARTIAL_TEXT } else { text };
                let s = t * t * (IMPORTANCE_FLOOR + dequantize(p.imp_q)) * CLASS_MULT[p.class.min(5) as usize];
                slot.hits.push((s, PrefixHit { entity: p.entity, partial: p.partial, text: t, imp_q: p.imp_q, class: p.class, ord: p.ord }));
            }
            if slot.hits.len() > PREFIX_TABLE_TOP * 64 {
                best_entities(&mut slot.hits, PREFIX_TABLE_TOP * 4);
            }
        }
    }
}

fn best_entities(hits: &mut Vec<(f32, PrefixHit)>, n: usize) {
    hits.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.entity.cmp(&b.1.entity)));
    let mut seen = HashSet::new();
    hits.retain(|(_, h)| seen.insert(h.entity));
    hits.truncate(n);
}

fn flush_slot(slot: &mut PrefixSlot, with_names: bool) -> std::io::Result<()> {
    let mut v = std::mem::take(&mut slot.hits);
    let keys = std::mem::take(&mut slot.keys);
    if slot.prefix.is_empty() || (slot.chars > PREFIX_TABLE_ALL_CHARS && keys < HEAVY_PREFIX_MIN_KEYS) {
        return Ok(());
    }
    best_entities(&mut v, PREFIX_TABLE_TOP);
    let hits: Vec<PrefixHit> = v.into_iter().map(|x| x.1).collect();
    let mut blob = Vec::new();
    encode_prefix_hits(&mut blob, &hits, with_names);
    slot.out.write_all(&(slot.prefix.len() as u16).to_le_bytes())?;
    slot.out.write_all(slot.prefix.as_bytes())?;
    slot.out.write_all(&(blob.len() as u32).to_le_bytes())?;
    slot.out.write_all(&blob)?;
    slot.flushed += 1;
    Ok(())
}

/// Next (prefix, blob) of one per-length prefix file.
fn read_prefix(r: &mut BufReader<File>) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut l2 = [0u8; 2];
    r.read_exact(&mut l2).ok()?;
    let mut k = vec![0u8; u16::from_le_bytes(l2) as usize];
    r.read_exact(&mut k).ok()?;
    let mut l4 = [0u8; 4];
    r.read_exact(&mut l4).ok()?;
    let mut b = vec![0u8; u32::from_le_bytes(l4) as usize];
    r.read_exact(&mut b).ok()?;
    Some((k, b))
}

fn vm_hwm_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with("VmHWM:"))
        .and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse().ok()))
        .unwrap_or(0)
}

/// Build one sub-index ("places" or "poi") of the flat set `geo` into `out`.
pub fn build(geo: &Index, name: &str, out: &Path, opts: &BuildOpts) -> Result<Stats, String> {
    let t0 = Instant::now();
    let mut st = Stats::default();
    std::fs::create_dir_all(out).map_err(|e| e.to_string())?;
    std::fs::create_dir_all(&opts.tmp).map_err(|e| e.to_string())?;
    let extras = match &opts.extras {
        Some(p) => Some(Extras::open(p)?),
        None => None,
    };
    let src = if name == "poi" {
        Source::Poi(PoiSource::new(geo, opts, extras.as_ref()))
    } else {
        let s = PlacesSource::new(geo, opts, extras.as_ref());
        s.write_slots(&out.join("places.bnd")).map_err(|e| e.to_string())?;
        st.add("place nodes", s.n_place_nodes);
        st.add("boundary slots", s.slots.len());
        st.add("boundary slots linked", s.slots.iter().filter(|x| x.linked()).count());
        Source::Places(s)
    };
    let t_src = t0.elapsed();

    // ---- keys -> sorted runs, one worker per contiguous range of entities
    let threads = opts.threads.max(1);
    let budget = (opts.mem_mb << 20) / threads;
    let n = src.len();
    let results: Vec<Result<(Runs, u64, u64), String>> = std::thread::scope(|sc| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let src = &src;
                let tmp = &opts.tmp;
                let tag = format!("{name}-{t}");
                sc.spawn(move || -> Result<(Runs, u64, u64), String> {
                    let lo = (n as u64 * t as u64 / threads as u64) as u32;
                    let hi = (n as u64 * (t as u64 + 1) / threads as u64) as u32;
                    let mut runs = Runs::new(budget, tmp, tag);
                    let mut e = EntityBuf::default();
                    let mut keys = Vec::new();
                    let (mut ents, mut entries) = (0u64, 0u64);
                    for pos in lo..hi {
                        if !src.fill(pos, &mut e) {
                            continue;
                        }
                        keys_of(&e, &mut keys);
                        if keys.is_empty() {
                            continue;
                        }
                        ents += 1;
                        let imp_q = quantize(e.imp);
                        for (k, ord, partial, class) in &keys {
                            runs.push(k.as_bytes(), Posting { entity: e.id, imp_q, partial: *partial, class: *class, ord: *ord })
                                .map_err(|x| x.to_string())?;
                            entries += 1;
                        }
                    }
                    runs.spill().map_err(|x| x.to_string())?;
                    // Hand the run memory back before the merge.
                    runs.arena = Vec::new();
                    runs.items = Vec::new();
                    Ok((runs, ents, entries))
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut files = Vec::new();
    let (mut ents, mut entries, mut spilled) = (0u64, 0u64, 0u64);
    for r in results {
        let (runs, e, k) = r?;
        ents += e;
        entries += k;
        spilled += runs.spilled_bytes;
        files.extend(runs.files);
    }
    let t_runs = t0.elapsed();

    // ---- merge -> fst + postings + prefix table
    let with_names = extras.is_some();
    let mk = |ext: &str| File::create(out.join(format!("{name}.{ext}"))).map_err(|e| e.to_string());
    let mut w = Writer {
        fst: fst::MapBuilder::new(BufWriter::with_capacity(1 << 20, mk("fst")?)).map_err(|e| e.to_string())?,
        post: BufWriter::with_capacity(1 << 20, mk("post")?),
        post_len: 0,
        scratch: Vec::new(),
        inline: opts.inline,
        with_names,
        slots: (1..=PREFIX_TABLE_MAX_CHARS)
            .map(|chars| -> Result<PrefixSlot, String> {
                let path = opts.tmp.join(format!("{name}-pfx{chars}.tmp"));
                let out = BufWriter::new(File::create(&path).map_err(|e| e.to_string())?);
                Ok(PrefixSlot { prefix: String::new(), chars, keys: 0, hits: Vec::new(), out, path, flushed: 0 })
            })
            .collect::<Result<Vec<_>, String>>()?,
        keys: 0,
        postings: 0,
        inline_keys: 0,
        longest: 0,
    };
    let mut readers: Vec<RunReader> = Vec::with_capacity(files.len());
    for f in &files {
        readers.push(RunReader { r: BufReader::with_capacity(RUN_READ_BUFFER, File::open(f).map_err(|e| e.to_string())?) });
    }
    let mut heap = BinaryHeap::with_capacity(readers.len());
    for (i, r) in readers.iter_mut().enumerate() {
        let mut h = Head { key: Vec::new(), p: Posting { entity: 0, imp_q: 0, partial: false, class: 0, ord: 0 }, run: i };
        if r.read_into(&mut h) {
            heap.push(h);
        }
    }
    let mut cur: Vec<u8> = Vec::new();
    let mut group: Vec<Posting> = Vec::new();
    while let Some(mut h) = heap.pop() {
        if h.key != cur {
            if !group.is_empty() {
                w.add(&cur, &mut group)?;
                group.clear();
            }
            cur.clear();
            cur.extend_from_slice(&h.key);
        }
        group.push(h.p);
        if readers[h.run].read_into(&mut h) {
            heap.push(h);
        }
    }
    if !group.is_empty() {
        w.add(&cur, &mut group)?;
    }
    drop(readers);
    for f in &files {
        let _ = std::fs::remove_file(f);
    }
    let Writer { fst, mut post, mut slots, keys, postings, inline_keys, longest, with_names, .. } = w;
    fst.finish().map_err(|e| e.to_string())?;
    post.flush().map_err(|e| e.to_string())?;
    let mut n_prefixes = 0u64;
    let mut pfx_readers = Vec::new();
    for sl in slots.iter_mut() {
        flush_slot(sl, with_names).map_err(|e| e.to_string())?;
        sl.out.flush().map_err(|e| e.to_string())?;
        n_prefixes += sl.flushed;
        pfx_readers.push(BufReader::new(File::open(&sl.path).map_err(|e| e.to_string())?));
    }
    // Merge the per-length files: each is sorted, and a prefix occurs in one length only.
    let mut heads: Vec<Option<(Vec<u8>, Vec<u8>)>> = pfx_readers.iter_mut().map(read_prefix).collect();
    let mut pf = fst::MapBuilder::new(BufWriter::new(mk("pfx.fst")?)).map_err(|e| e.to_string())?;
    let mut pb = BufWriter::new(mk("pfx")?);
    let mut off = 0u64;
    loop {
        let Some(i) = (0..heads.len()).filter(|&i| heads[i].is_some()).min_by(|&a, &b| heads[a].as_ref().unwrap().0.cmp(&heads[b].as_ref().unwrap().0)) else {
            break;
        };
        let (k, blob) = heads[i].take().unwrap();
        pf.insert(&k, off).map_err(|e| e.to_string())?;
        pb.write_all(&blob).map_err(|e| e.to_string())?;
        off += blob.len() as u64;
        heads[i] = read_prefix(&mut pfx_readers[i]);
    }
    for sl in &slots {
        let _ = std::fs::remove_file(&sl.path);
    }
    pf.finish().map_err(|e| e.to_string())?;
    pb.flush().map_err(|e| e.to_string())?;
    let t_merge = t0.elapsed();

    let size = |ext: &str| std::fs::metadata(out.join(format!("{name}.{ext}"))).map(|m| m.len()).unwrap_or(0);
    st.add("entities indexed", ents);
    st.add("key entries", entries);
    st.add("keys", keys);
    st.add("postings", postings);
    st.add("inline keys", inline_keys);
    st.add("longest list", longest);
    st.add("prefix table prefixes", n_prefixes);
    st.add("sort runs", files.len());
    st.add("run bytes spilled", spilled);
    st.add("bytes fst", size("fst"));
    st.add("bytes post", size("post"));
    st.add("bytes pfx", size("pfx.fst") + size("pfx"));
    if name != "poi" {
        st.add("bytes bnd", std::fs::metadata(out.join("places.bnd")).map(|m| m.len()).unwrap_or(0));
    }
    st.add("time sources s", format!("{:.2}", t_src.as_secs_f64()));
    st.add("time keys+runs s", format!("{:.2}", (t_runs - t_src).as_secs_f64()));
    st.add("time merge+write s", format!("{:.2}", (t_merge - t_runs).as_secs_f64()));
    st.add("time total s", format!("{:.2}", t_merge.as_secs_f64()));
    st.add("VmHWM MiB", format!("{:.1}", vm_hwm_kb() as f64 / 1024.0));
    Ok(st)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn writer(dir: &Path) -> Writer {
        std::fs::create_dir_all(dir).unwrap();
        let mk = |n: &str| BufWriter::new(File::create(dir.join(n)).unwrap());
        Writer {
            fst: fst::MapBuilder::new(mk("fst")).unwrap(),
            post: mk("post"),
            post_len: 0,
            scratch: Vec::new(),
            inline: false,
            with_names: false,
            slots: (1..=PREFIX_TABLE_MAX_CHARS)
                .map(|chars| PrefixSlot { prefix: String::new(), chars, keys: 0, hits: Vec::new(), out: mk(&format!("pfx{chars}")), path: dir.join(format!("pfx{chars}")), flushed: 0 })
                .collect(),
            keys: 0,
            postings: 0,
            inline_keys: 0,
            longest: 0,
        }
    }

    #[test]
    fn prefix_table_keeps_distinct_entities() {
        // 30 minor entities under "a", then 10 major ones with many names each: pruning hits rather
        // than entities leaves only the 10 major ones in the table.
        let dir = std::env::temp_dir().join(format!("pfx-distinct-{}", std::process::id()));
        let mut w = writer(&dir);
        let post = |entity, imp_q| Posting { entity, imp_q, partial: false, class: 0, ord: 0 };
        for e in 0..30u32 {
            w.prefix_add(format!("aa{e:03}").as_bytes(), &[post(1000 + e, 10)]);
        }
        let major: Vec<Posting> = (0..10).map(|e| post(e, 250)).collect();
        for k in 0..300 {
            w.prefix_add(format!("ab{k:03}").as_bytes(), &major);
        }
        let slot = &mut w.slots[0];
        flush_slot(slot, false).unwrap();
        slot.out.flush().unwrap();
        let (k, blob) = read_prefix(&mut BufReader::new(File::open(&slot.path).unwrap())).unwrap();
        let mut hits = Vec::new();
        decode_prefix_hits(&blob, 0, false, &mut hits);
        std::fs::remove_dir_all(&dir).ok();
        assert_eq!(k, b"a");
        assert_eq!(hits.len(), PREFIX_TABLE_TOP);
    }
}
