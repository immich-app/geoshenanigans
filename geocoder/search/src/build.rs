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
use crate::extras::{record_ref, Extras, KIND_PLACE_NODE, KIND_POI};
use crate::model::*;
use crate::normalize::{join, tokens, transliterate};
use memmap2::Mmap;
use query_server::{point_on_surface, AdminPolygon, Index, PlaceNode, PoiRecord, NO_DATA};
use std::collections::{BinaryHeap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

pub struct BuildOpts {
    pub threads: usize,
    pub mem_mb: usize,
    pub tmp: PathBuf,
    /// Single-posting keys carry their posting in the fst value.
    pub inline: bool,
    pub extras: Option<String>,
    /// Directory with imp_places.u8 / imp_polys.u8 / imp_poi.u8 (the shipped importance bytes, one
    /// per record); a file left out leaves its records without them.
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

/// One importance file of `dir` for `records` records: None without a directory or when the
/// directory leaves the file out; an error for a missing directory, a file that fails to open or
/// map, or one that does not hold one byte per record.
fn map_importance(dir: Option<&str>, file: &str, records: u32) -> std::io::Result<Option<Mmap>> {
    let Some(dir) = dir else { return Ok(None) };
    let path = Path::new(dir).join(file);
    let with_path = |e: std::io::Error| std::io::Error::new(e.kind(), format!("{}: {e}", path.display()));
    let f = match File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && Path::new(dir).is_dir() => return Ok(None),
        Err(e) => return Err(with_path(e)),
    };
    let m = unsafe { Mmap::map(&f) }.map_err(with_path)?;
    if m.len() != records as usize {
        let e = format!("{}: {} bytes for {records} records", path.display(), m.len());
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e));
    }
    Ok(Some(m))
}

fn byte_at(m: &Option<Mmap>, i: u32) -> u8 {
    m.as_ref().and_then(|m| m.get(i as usize).copied()).unwrap_or(0)
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
    pub fn new(geo: &'a Index, opts: &BuildOpts, extras: Option<&'a Extras>) -> std::io::Result<Self> {
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
                // The builder's interior_point (ST_PointOnSurface, Nominatim's centre of an area):
                // an area centroid can fall outside a concave ring or in a hole.
                let Some((lat, lng)) = geo.admin_verts(&ap).and_then(|v| point_on_surface(&v, ap.hole_count)) else { continue };
                slots.push(BoundarySlot { poly: p, node: NO_DATA, lat, lng });
            }
        }
        let dir = opts.imp_dir.as_deref();
        Ok(PlacesSource {
            geo,
            n_place_nodes,
            slots,
            link,
            label_used,
            imp_places: map_importance(dir, "imp_places.u8", n_place_nodes)?,
            imp_polys: map_importance(dir, "imp_polys.u8", n_poly)?,
            extras,
            area_proxy: opts.area_proxy,
        })
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
        let (rrefs, base_id) = if pos < self.slots.len() as u32 {
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
            (s.record_refs(), primary)
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
            (vec![record_ref(KIND_PLACE_NODE, i)], pn.name_id)
        };
        fill_names(geo, self.extras, &rrefs, base_id, e)
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
    pub fn new(geo: &'a Index, opts: &BuildOpts, extras: Option<&'a Extras>) -> std::io::Result<Self> {
        let n = record_count::<PoiRecord>(geo.poi_records.as_ref().expect("poi_records"));
        let imp_poi = map_importance(opts.imp_dir.as_deref(), "imp_poi.u8", n)?;
        Ok(PoiSource { geo, n, max_tier: opts.poi_max_tier, imp_poi, extras })
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
        fill_names(self.geo, self.extras, &[record_ref(KIND_POI, pos)], r.name_id, e)
    }
}

/// An entity's names: its base name, then the extra names of its records (`rrefs`, in the order
/// `Entities::record_refs` gives them, which numbers the ordinals).
fn fill_names(geo: &Index, extras: Option<&Extras>, rrefs: &[u32], base_id: u32, e: &mut EntityBuf) -> bool {
    let base = geo.get_string(base_id);
    if !base.is_empty() {
        e.names.insert(0, (0, 0, true, base.to_string()));
    }
    if let Some(x) = extras {
        for (k, n) in x.names_of(rrefs).into_iter().enumerate() {
            let ord = if k < MAX_EXTRA_ORD as usize { k as u8 + 1 } else { MAX_NAME_ORD };
            e.names.push((n.class, ord, n.key == "name", n.name.to_string()));
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

    /// Adds one key entry; an error for a key longer than a run entry's u16 length holds.
    fn push(&mut self, key: &[u8], p: Posting) -> std::io::Result<()> {
        let Ok(len) = u16::try_from(key.len()) else {
            let e = format!("entity {}: a key of {} bytes, over the {} bytes a sort run entry holds", p.entity, key.len(), u16::MAX);
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, e));
        };
        if self.arena.len() + key.len() > self.arena.capacity() || self.items.len() == self.items.capacity() {
            self.spill()?;
        }
        let off = self.arena.len() as u32;
        self.arena.extend_from_slice(key);
        self.items.push(Item {
            off,
            len,
            flags: entry_flags(&p),
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
            self.spilled_bytes += write_entry(&mut w, key(it), it.entity, it.imp, it.flags, it.ord)?;
        }
        w.flush()?;
        self.files.push(path);
        self.items.clear();
        self.arena.clear();
        Ok(())
    }
}

/// One run entry: u16 key length, key, u32 entity, importance, partial | class << 1, ordinal.
/// Returns the bytes written.
fn write_entry(w: &mut impl Write, key: &[u8], entity: u32, imp: u8, flags: u8, ord: u8) -> std::io::Result<u64> {
    w.write_all(&(key.len() as u16).to_le_bytes())?;
    w.write_all(key)?;
    w.write_all(&entity.to_le_bytes())?;
    w.write_all(&[imp, flags, ord])?;
    Ok(2 + key.len() as u64 + 7)
}

fn entry_flags(p: &Posting) -> u8 {
    (p.partial as u8) | (p.class << 1)
}

/// The entries of several sorted runs in (key, entity, run) order.
struct Merge {
    files: Vec<PathBuf>,
    readers: Vec<RunReader>,
    heap: BinaryHeap<Head>,
}

impl Merge {
    fn open(files: &[PathBuf]) -> std::io::Result<Self> {
        let mut readers = Vec::with_capacity(files.len());
        for f in files {
            readers.push(RunReader { r: BufReader::with_capacity(RUN_READ_BUFFER, File::open(f).map_err(|e| run_error(f, e))?) });
        }
        let mut heap = BinaryHeap::with_capacity(readers.len());
        for (i, r) in readers.iter_mut().enumerate() {
            let mut h = Head { key: Vec::new(), p: Posting { entity: 0, imp_q: 0, partial: false, class: 0, ord: 0 }, run: i };
            if r.read_into(&mut h).map_err(|e| run_error(&files[i], e))? {
                heap.push(h);
            }
        }
        Ok(Merge { files: files.to_vec(), readers, heap })
    }

    /// Copies the next entry into `key` and `p`; false after the last, an error for a run that
    /// fails to read or is cut short.
    fn next(&mut self, key: &mut Vec<u8>, p: &mut Posting) -> std::io::Result<bool> {
        let Some(mut top) = self.heap.peek_mut() else { return Ok(false) };
        key.clear();
        key.extend_from_slice(&top.key);
        *p = top.p;
        let run = top.run;
        if !self.readers[run].read_into(&mut top).map_err(|e| run_error(&self.files[run], e))? {
            std::collections::binary_heap::PeekMut::pop(top);
        }
        Ok(true)
    }
}

/// A sort run's open or read error, naming the run.
fn run_error(path: &Path, e: std::io::Error) -> std::io::Error {
    std::io::Error::new(e.kind(), format!("sort run {}: {e}", path.display()))
}

/// Merges groups of at most MERGE_FAN_IN consecutive runs into longer runs until at most
/// MERGE_FAN_IN are left (consecutive, so entries that compare equal keep their run order).
/// Each pass merges every run while more than MERGE_FAN_IN² are left, then only as many of the
/// last runs as it takes to leave MERGE_FAN_IN. Returns the runs left and the bytes rewritten.
fn reduce_runs(mut files: Vec<PathBuf>, dir: &Path, tag: &str) -> std::io::Result<(Vec<PathBuf>, u64)> {
    let mut rewritten = 0u64;
    let mut pass = 0;
    while files.len() > MERGE_FAN_IN {
        let n = files.len();
        let excess = n - MERGE_FAN_IN;
        let merged = if n > MERGE_FAN_IN * MERGE_FAN_IN { n } else { excess + excess.div_ceil(MERGE_FAN_IN - 1) };
        let mut next = files[..n - merged].to_vec();
        for (i, group) in files[n - merged..].chunks(MERGE_FAN_IN).enumerate() {
            if group.len() == 1 {
                next.push(group[0].clone());
                continue;
            }
            let path = dir.join(format!("{tag}-m{pass}-{i}.run"));
            let mut w = BufWriter::with_capacity(RUN_READ_BUFFER, File::create(&path)?);
            let mut m = Merge::open(group)?;
            let mut key = Vec::new();
            let mut p = Posting { entity: 0, imp_q: 0, partial: false, class: 0, ord: 0 };
            while m.next(&mut key, &mut p)? {
                rewritten += write_entry(&mut w, &key, p.entity, p.imp_q, entry_flags(&p), p.ord)?;
            }
            w.flush()?;
            drop(m);
            for f in group {
                let _ = std::fs::remove_file(f);
            }
            next.push(path);
        }
        files = next;
        pass += 1;
    }
    Ok((files, rewritten))
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
    /// The run's next record into `h`: false at the run's end (nothing left before a record), an
    /// error for a failed read or a record cut short.
    fn read_into(&mut self, h: &mut Head) -> std::io::Result<bool> {
        if self.r.fill_buf()?.is_empty() {
            return Ok(false);
        }
        let mut len = [0u8; 2];
        self.r.read_exact(&mut len)?;
        let n = u16::from_le_bytes(len) as usize;
        h.key.resize(n, 0);
        self.r.read_exact(&mut h.key)?;
        let mut rest = [0u8; 7];
        self.r.read_exact(&mut rest)?;
        h.p = Posting {
            entity: u32::from_le_bytes(rest[0..4].try_into().unwrap()),
            imp_q: rest[4],
            partial: rest[5] & 1 == 1,
            class: rest[5] >> 1,
            ord: rest[6],
        };
        Ok(true)
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
            encode_list(&mut self.scratch, group);
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
        let top = best_postings(list, PREFIX_TABLE_TOP);
        let mut pfx = String::new();
        for (n, c) in key.chars().take(PREFIX_TABLE_MAX_CHARS).enumerate() {
            pfx.push(c);
            let n = n + 1;
            if self.slots[n - 1].prefix != pfx {
                let slot = &mut self.slots[n - 1];
                flush_slot(slot).expect("prefix temp file");
                slot.prefix = pfx.clone();
            }
            let slot = &mut self.slots[n - 1];
            slot.keys += 1;
            let text = if n == klen { 1.0 } else { prefix_text(n, klen, 0) };
            for p in &top {
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

/// A key's `n` best postings by prefix score, best first. Every posting of a key has the key's
/// text score, so they rank by trailing-word match, importance and name class.
fn best_postings(list: &[Posting], n: usize) -> Vec<Posting> {
    let rank = |p: &Posting| {
        let t = if p.partial { PARTIAL_TEXT } else { 1.0 };
        (t * t * (IMPORTANCE_FLOOR + dequantize(p.imp_q)) * CLASS_MULT[p.class.min(5) as usize], p.entity)
    };
    let before = |a: &(f32, u32), b: &(f32, u32)| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)).is_lt();
    let mut top: Vec<((f32, u32), Posting)> = Vec::with_capacity(n + 1);
    for p in list {
        let r = rank(p);
        let at = top.partition_point(|(t, _)| before(t, &r));
        if at < n {
            top.insert(at, (r, *p));
            top.truncate(n);
        }
    }
    top.into_iter().map(|(_, p)| p).collect()
}

fn best_entities(hits: &mut Vec<(f32, PrefixHit)>, n: usize) {
    hits.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.entity.cmp(&b.1.entity)));
    let mut seen = HashSet::new();
    hits.retain(|(_, h)| seen.insert(h.entity));
    hits.truncate(n);
}

fn flush_slot(slot: &mut PrefixSlot) -> std::io::Result<()> {
    let mut v = std::mem::take(&mut slot.hits);
    let keys = std::mem::take(&mut slot.keys);
    if slot.prefix.is_empty() || (slot.chars > PREFIX_TABLE_ALL_CHARS && keys < HEAVY_PREFIX_MIN_KEYS) {
        return Ok(());
    }
    best_entities(&mut v, PREFIX_TABLE_TOP);
    let hits: Vec<PrefixHit> = v.into_iter().map(|x| x.1).collect();
    let mut blob = Vec::new();
    encode_prefix_hits(&mut blob, &hits);
    slot.out.write_all(&(slot.prefix.len() as u16).to_le_bytes())?;
    slot.out.write_all(slot.prefix.as_bytes())?;
    slot.out.write_all(&(blob.len() as u32).to_le_bytes())?;
    slot.out.write_all(&blob)?;
    slot.flushed += 1;
    Ok(())
}

/// Next (prefix, blob) of one per-length prefix file: None at the file's end (nothing left before
/// an entry), an error for a failed read or an entry cut short.
fn read_prefix(r: &mut BufReader<File>) -> std::io::Result<Option<(Vec<u8>, Vec<u8>)>> {
    if r.fill_buf()?.is_empty() {
        return Ok(None);
    }
    let mut l2 = [0u8; 2];
    r.read_exact(&mut l2)?;
    let mut k = vec![0u8; u16::from_le_bytes(l2) as usize];
    r.read_exact(&mut k)?;
    let mut l4 = [0u8; 4];
    r.read_exact(&mut l4)?;
    let mut b = vec![0u8; u32::from_le_bytes(l4) as usize];
    r.read_exact(&mut b)?;
    Ok(Some((k, b)))
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
        Source::Poi(PoiSource::new(geo, opts, extras.as_ref()).map_err(|e| e.to_string())?)
    } else {
        let s = PlacesSource::new(geo, opts, extras.as_ref()).map_err(|e| e.to_string())?;
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
    let n_runs = files.len();
    let (files, rewritten) = reduce_runs(files, &opts.tmp, name).map_err(|e| e.to_string())?;
    let mk = |ext: &str| File::create(out.join(format!("{name}.{ext}"))).map_err(|e| e.to_string());
    let mut w = Writer {
        fst: fst::MapBuilder::new(BufWriter::with_capacity(1 << 20, mk("fst")?)).map_err(|e| e.to_string())?,
        post: BufWriter::with_capacity(1 << 20, mk("post")?),
        post_len: 0,
        scratch: Vec::new(),
        inline: opts.inline,
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
    let mut m = Merge::open(&files).map_err(|e| e.to_string())?;
    let mut cur: Vec<u8> = Vec::new();
    let mut key: Vec<u8> = Vec::new();
    let mut p = Posting { entity: 0, imp_q: 0, partial: false, class: 0, ord: 0 };
    let mut group: Vec<Posting> = Vec::new();
    while m.next(&mut key, &mut p).map_err(|e| e.to_string())? {
        if key != cur {
            if !group.is_empty() {
                w.add(&cur, &mut group)?;
                group.clear();
            }
            std::mem::swap(&mut cur, &mut key);
        }
        group.push(p);
    }
    if !group.is_empty() {
        w.add(&cur, &mut group)?;
    }
    drop(m);
    for f in &files {
        let _ = std::fs::remove_file(f);
    }
    let Writer { fst, mut post, mut slots, keys, postings, inline_keys, longest, .. } = w;
    fst.finish().map_err(|e| e.to_string())?;
    post.flush().map_err(|e| e.to_string())?;
    let mut n_prefixes = 0u64;
    let mut pfx_readers = Vec::new();
    for sl in slots.iter_mut() {
        flush_slot(sl).map_err(|e| e.to_string())?;
        sl.out.flush().map_err(|e| e.to_string())?;
        n_prefixes += sl.flushed;
        pfx_readers.push(BufReader::new(File::open(&sl.path).map_err(|e| e.to_string())?));
    }
    // Merge the per-length files: each is sorted, and a prefix occurs in one length only.
    let prefix_error = |i: usize, e: std::io::Error| format!("prefix temp file {}: {e}", slots[i].path.display());
    let mut heads: Vec<Option<(Vec<u8>, Vec<u8>)>> =
        pfx_readers.iter_mut().enumerate().map(|(i, r)| read_prefix(r).map_err(|e| prefix_error(i, e))).collect::<Result<_, _>>()?;
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
        heads[i] = read_prefix(&mut pfx_readers[i]).map_err(|e| prefix_error(i, e))?;
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
    st.add("sort runs", n_runs);
    st.add("run bytes spilled", spilled);
    st.add("run bytes rewritten", rewritten);
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

/// A partial rebuild must keep the extra-name source required by retained sub-indexes.
pub fn check_retained_names(out: &Path, building: &[&str], with_extras: bool) -> Result<(), String> {
    let text = match std::fs::read_to_string(out.join("search_meta.json")) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    let meta: serde_json::Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    for name in ["places", "poi"] {
        if !building.contains(&name) && out.join(format!("{name}.fst")).exists()
            && meta["extras"].as_bool().unwrap_or(false) != with_extras {
            return Err(format!("retained {name} sub-index uses a different extra-name setting; rebuild with --sub all or the same --extras setting"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// xorshift: deterministic test values without a dependency.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    fn posting(entity: u32, imp_q: u8, partial: bool, class: u8) -> Posting {
        Posting { entity, imp_q, partial, class, ord: (entity % 16) as u8 }
    }

    fn tmp_dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("place-search-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Writes one sorted run of `entries` and returns its path.
    fn write_run(dir: &Path, name: &str, entries: &mut [(Vec<u8>, Posting)]) -> PathBuf {
        entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.entity.cmp(&b.1.entity)));
        let path = dir.join(name);
        let mut w = BufWriter::new(File::create(&path).unwrap());
        for (k, p) in entries.iter() {
            write_entry(&mut w, k, p.entity, p.imp_q, entry_flags(p), p.ord).unwrap();
        }
        w.flush().unwrap();
        path
    }

    fn read_all(files: &[PathBuf]) -> Vec<(Vec<u8>, Posting)> {
        let mut m = Merge::open(files).unwrap();
        let (mut key, mut p) = (Vec::new(), posting(0, 0, false, 0));
        let mut out = Vec::new();
        while m.next(&mut key, &mut p).unwrap() {
            out.push((key.clone(), p));
        }
        out
    }

    /// `n` runs of random entries; returns the run files and every entry in merge order.
    fn random_runs(dir: &Path, n: usize, rng: &mut Rng) -> (Vec<PathBuf>, Vec<(Vec<u8>, Posting)>) {
        let mut all = Vec::new();
        let mut files = Vec::new();
        let mut entity = 0u32;
        for r in 0..n {
            let mut run: Vec<(Vec<u8>, Posting)> = (0..(1 + rng.next() % 40))
                .map(|_| {
                    entity += 1;
                    let key = format!("k{}", rng.next() % 50).into_bytes();
                    (key, posting(entity, (rng.next() % 200) as u8, rng.next().is_multiple_of(2), (rng.next() % 6) as u8))
                })
                .collect();
            files.push(write_run(dir, &format!("t-{r}.run"), &mut run));
            all.extend(run);
        }
        all.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.entity.cmp(&b.1.entity)));
        (files, all)
    }

    #[test]
    fn merge_streams_runs_in_key_then_entity_order() {
        let dir = tmp_dir("merge");
        let (files, all) = random_runs(&dir, 5, &mut Rng(7));
        assert_eq!(read_all(&files), all);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reduce_runs_merges_only_the_runs_over_the_fan_in() {
        let dir = tmp_dir("reduce-excess");
        let (files, all) = random_runs(&dir, MERGE_FAN_IN + 1, &mut Rng(11));
        let last_two: u64 = files[MERGE_FAN_IN - 1..].iter().map(|f| std::fs::metadata(f).unwrap().len()).sum();
        let (left, rewritten) = reduce_runs(files.clone(), &dir, "t").unwrap();
        assert_eq!(left.len(), MERGE_FAN_IN);
        assert_eq!(left[..MERGE_FAN_IN - 1], files[..MERGE_FAN_IN - 1]);
        assert_eq!(rewritten, last_two);
        assert!(!files[MERGE_FAN_IN].exists());
        assert_eq!(read_all(&left), all);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reduce_runs_takes_passes_until_the_fan_in_is_left() {
        for n in [MERGE_FAN_IN, 3 * MERGE_FAN_IN + 5, MERGE_FAN_IN * MERGE_FAN_IN + 3] {
            let dir = tmp_dir(&format!("reduce-{n}"));
            let (files, all) = random_runs(&dir, n, &mut Rng(n as u64));
            let (left, rewritten) = reduce_runs(files, &dir, "t").unwrap();
            assert!(left.len() <= MERGE_FAN_IN);
            assert_eq!(rewritten == 0, n <= MERGE_FAN_IN);
            assert_eq!(read_all(&left), all);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    #[test]
    fn runs_refuse_keys_over_the_length_limit() {
        let dir = tmp_dir("long-key");
        let mut runs = Runs::new(1 << 20, &dir, "t".into());
        let p = posting(1, 9, false, 0);
        let longest = vec![b'a'; u16::MAX as usize];
        assert!(runs.push(&longest, p).is_ok());

        // One byte over: ASCII, and a two-byte character across the limit.
        for key in [vec![b'a'; u16::MAX as usize + 1], [&longest[1..], "ж".as_bytes()].concat()] {
            assert_eq!(runs.push(&key, p).map_err(|e| e.kind()), Err(std::io::ErrorKind::InvalidInput), "{} bytes", key.len());
        }
        runs.spill().unwrap();
        assert_eq!(read_all(&runs.files), [(longest, p)]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_build_fails_on_a_name_over_the_key_limit() {
        use crate::fixture::{Geo, TempDir};
        let mut g = Geo::default();
        g.place("Testville", 1, 10.1, 20.1);
        let dir = TempDir::new("long-name");
        let (tsv, bin) = (dir.0.join("extra_names.tsv"), dir.0.join("extra_names.bin"));
        std::fs::write(&tsv, format!("p\t0\t2\tname:ru\t{}\n", "ж".repeat(u16::MAX as usize / 2 + 1))).unwrap();
        crate::extras::pack(tsv.to_str().unwrap(), bin.to_str().unwrap()).unwrap();
        let geo = g.load(&dir.0.join("geo"));
        let opts = BuildOpts {
            threads: 1, mem_mb: DEFAULT_RUN_BUDGET_MB, tmp: dir.0.join("tmp"), inline: true,
            extras: Some(bin.to_str().unwrap().into()), imp_dir: None, area_proxy: false, poi_max_tier: 3,
        };

        let err = build(&geo, "places", &dir.0.join("idx"), &opts).err();

        assert!(err.as_deref().is_some_and(|e| e.contains("65536 bytes")), "{err:?}");
    }

    fn writer(dir: &Path) -> Writer {
        std::fs::create_dir_all(dir).unwrap();
        let mk = |n: &str| BufWriter::new(File::create(dir.join(n)).unwrap());
        Writer {
            fst: fst::MapBuilder::new(mk("fst")).unwrap(),
            post: mk("post"),
            post_len: 0,
            scratch: Vec::new(),
            inline: false,
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
        flush_slot(slot).unwrap();
        slot.out.flush().unwrap();
        let (k, blob) = read_prefix(&mut BufReader::new(File::open(&slot.path).unwrap())).unwrap().unwrap();
        let mut hits = Vec::new();
        decode_prefix_hits(&blob, 0, &mut hits);
        std::fs::remove_dir_all(&dir).ok();
        assert_eq!(k, b"a");
        assert_eq!(hits.len(), PREFIX_TABLE_TOP);
    }
    #[test]
    fn run_reader_tells_the_end_from_a_cut_record() {
        // u16 key length, key, u32 entity, importance, flags, ordinal (Runs::spill).
        let record = [&2u16.to_le_bytes()[..], b"ab", &7u32.to_le_bytes(), &[9, 3, 1]].concat();
        let path = std::env::temp_dir().join(format!("run-cut-{}.run", std::process::id()));
        let read_all = |bytes: &[u8]| -> std::io::Result<Vec<u32>> {
            std::fs::write(&path, bytes).unwrap();
            let mut r = RunReader { r: BufReader::new(File::open(&path).unwrap()) };
            let mut h = Head { key: Vec::new(), p: Posting { entity: 0, imp_q: 0, partial: false, class: 0, ord: 0 }, run: 0 };
            let mut entities = Vec::new();
            while r.read_into(&mut h)? {
                entities.push(h.p.entity);
            }
            Ok(entities)
        };
        assert!(read_all(&[]).unwrap().is_empty());
        assert_eq!(read_all(&record).unwrap(), vec![7u32]);
        // The next record cut in its length, its key and its posting.
        for cut in [1, 3, record.len() - 1] {
            let bytes = [&record[..], &record[..cut]].concat();
            assert_eq!(read_all(&bytes).map_err(|e| e.kind()), Err(std::io::ErrorKind::UnexpectedEof), "cut at {cut}");
        }
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn prefix_reader_tells_the_end_from_a_cut_entry() {
        // u16 prefix length, prefix, u32 blob length, blob (flush_slot).
        let entry = [&2u16.to_le_bytes()[..], b"ab", &3u32.to_le_bytes(), &[1, 2, 3]].concat();
        let path = std::env::temp_dir().join(format!("pfx-cut-{}.tmp", std::process::id()));
        let read_all = |bytes: &[u8]| -> std::io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
            std::fs::write(&path, bytes).unwrap();
            let mut r = BufReader::new(File::open(&path).unwrap());
            let mut entries = Vec::new();
            while let Some(e) = read_prefix(&mut r)? {
                entries.push(e);
            }
            Ok(entries)
        };
        assert!(read_all(&[]).unwrap().is_empty());
        assert_eq!(read_all(&entry).unwrap(), vec![(b"ab".to_vec(), vec![1, 2, 3])]);
        // The next entry cut in its prefix length, its prefix, its blob length and its blob.
        for cut in [1, 3, 5, entry.len() - 1] {
            let bytes = [&entry[..], &entry[..cut]].concat();
            assert_eq!(read_all(&bytes).map_err(|e| e.kind()), Err(std::io::ErrorKind::UnexpectedEof), "cut at {cut}");
        }
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn prefix_table_takes_a_keys_best_scores() {
        // Forty class-5 postings come first by importance; the class-0 one after them scores best.
        let dir = std::env::temp_dir().join(format!("pfx-score-{}", std::process::id()));
        let mut w = writer(&dir);
        let mut list: Vec<Posting> = (0..PREFIX_TABLE_TOP as u32)
            .map(|entity| Posting { entity, imp_q: 200, partial: false, class: 5, ord: 1 })
            .collect();
        list.push(Posting { entity: 99, imp_q: 190, partial: false, class: 0, ord: 0 });
        w.prefix_add(b"abc", &list);
        let slot = &mut w.slots[0];
        flush_slot(slot).unwrap();
        slot.out.flush().unwrap();
        let (k, blob) = read_prefix(&mut BufReader::new(File::open(&slot.path).unwrap())).unwrap().unwrap();
        let mut hits = Vec::new();
        decode_prefix_hits(&blob, 0, &mut hits);
        std::fs::remove_dir_all(&dir).ok();
        assert_eq!(k, b"a");
        assert_eq!(hits.len(), PREFIX_TABLE_TOP);
        assert_eq!(hits[0].entity, 99);
    }

    #[test]
    fn a_claimed_label_nodes_extra_names_find_its_boundary() {
        use crate::fixture::{build_search, Geo, TempDir};
        use crate::query::Searcher;
        use crate::reader::SearchIndex;
        let mut g = Geo::default();
        let poly = g.boundary("Testville", 8, [10.0, 20.0, 10.2, 20.2]);
        let node = g.place("Testville", 1, 10.1, 20.1);
        g.claim(poly, node);
        let dir = TempDir::new("label-extras");
        let (tsv, bin) = (dir.0.join("extra_names.tsv"), dir.0.join("extra_names.bin"));
        // The polygon's French name is the boundary's extra name 1, its label node's German one 2.
        std::fs::write(&tsv, "b\t0\t2\tname:fr\tVille-Essai\np\t0\t2\tname:de\tPrüfstadt\n").unwrap();
        crate::extras::pack(tsv.to_str().unwrap(), bin.to_str().unwrap()).unwrap();
        let geo = g.load(&dir.0.join("geo"));
        let out = dir.0.join("idx");
        build_search(&geo, &out, Some(&bin));
        let idx = SearchIndex::open(&geo, &out, Some(bin.to_str().unwrap()), None).unwrap();
        let s = Searcher::new(&idx);

        for q in ["Prüfstadt", "Ville-Essai"] {
            let (found, _) = s.search(q, 5);

            // The boundary is entity n_place_nodes (1) + its polygon (0); its node is not its own.
            let got: Vec<(u32, &str)> = found.iter().map(|f| (f.hit.entity, f.name.as_str())).collect();
            assert_eq!(got, [(1, q)], "{q}");
        }
    }

    #[test]
    fn an_unlinked_boundary_is_found_at_a_point_inside_it() {
        use crate::fixture::{build_search, Geo, TempDir};
        use crate::query::Searcher;
        use crate::reader::SearchIndex;
        let at = |lat: f32, lng: f32| (10.0 + lat / 10.0, 20.0 + lng / 10.0);
        let rect = |s: f32, w: f32, n: f32, e: f32| [at(s, w).0, at(s, w).1, at(n, e).0, at(n, e).1];
        let mut g = Geo::default();
        // A county open to the north: its area centroid falls in its notch, where another county lies.
        let u = [(0.0, 0.0), (0.0, 3.0), (3.0, 3.0), (3.0, 2.0), (1.0, 2.0), (1.0, 1.0), (3.0, 1.0), (3.0, 0.0), (0.0, 0.0)];
        let hook = g.shaped_boundary("Hookcounty", 6, &u.map(|(a, b)| at(a, b)), &[]);
        g.boundary("Notchcounty", 6, rect(1.0, 1.0, 3.0, 2.0));
        // A county around a hole another county fills: its area centroid falls in the hole.
        let outer = [(0.0, 4.0), (0.0, 7.0), (3.0, 7.0), (3.0, 4.0), (0.0, 4.0)].map(|(a, b)| at(a, b));
        let hole = [(1.0, 5.0), (2.0, 5.0), (2.0, 6.0), (1.0, 6.0), (1.0, 5.0)].map(|(a, b)| at(a, b));
        let ring = g.shaped_boundary("Ringcounty", 6, &outer, &[&hole]);
        g.boundary("Holecounty", 6, rect(1.0, 5.0, 2.0, 6.0));
        let dir = TempDir::new("label-point");
        let geo = g.load(&dir.0.join("geo"));
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        let idx = SearchIndex::open(&geo, &out, None, None).unwrap();
        let s = Searcher::new(&idx);

        for (q, poly) in [("Hookcounty", hook), ("Ringcounty", ring)] {
            let (found, _) = s.search(q, 5);

            // No place nodes: the boundary is entity 0 + its polygon.
            let f = &found[0];
            assert_eq!(f.hit.entity, poly, "{q}");
            assert!(geo.admin_pip(&polygon_raw(&geo, poly), f.lat as f64, f.lng as f64), "{q} at {} {}", f.lat, f.lng);
            assert_eq!(f.ctx.county, q);
        }
    }

    #[test]
    fn partial_build_retains_extra_name_requirements() {
        let dir = std::env::temp_dir().join(format!("retained-names-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("places.fst"), []).unwrap();
        std::fs::write(dir.join("poi.fst"), []).unwrap();
        for old in [false, true] {
            std::fs::write(dir.join("search_meta.json"), format!("{{\"extras\":{old}}}")).unwrap();
            assert!(check_retained_names(&dir, &["poi"], !old).is_err());
            assert!(check_retained_names(&dir, &["places"], !old).is_err());
            assert!(check_retained_names(&dir, &["poi"], old).is_ok());
            assert!(check_retained_names(&dir, &["places", "poi"], !old).is_ok());
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn importance_files_hold_one_byte_per_record() {
        use std::io::ErrorKind::{InvalidData, NotFound};
        let dir = tmp_dir("imp");
        std::fs::write(dir.join("imp_places.u8"), [7, 8, 9]).unwrap();
        std::fs::create_dir(dir.join("imp_polys.u8")).unwrap();
        let (d, missing) = (dir.to_str().unwrap(), dir.join("missing"));
        let got = |dir: Option<&str>, file: &str, records: u32| {
            map_importance(dir, file, records).map(|m| m.map(|m| m.to_vec())).map_err(|e| e.kind())
        };
        assert_eq!(got(None, "imp_places.u8", 3), Ok(None));
        assert_eq!(got(Some(d), "imp_places.u8", 3), Ok(Some(vec![7, 8, 9])));
        // The directory leaves imp_poi.u8 out.
        assert_eq!(got(Some(d), "imp_poi.u8", 3), Ok(None));
        assert_eq!(got(Some(missing.to_str().unwrap()), "imp_places.u8", 3), Err(NotFound));
        for records in [2, 4] {
            assert_eq!(got(Some(d), "imp_places.u8", records), Err(InvalidData), "{records} records");
        }
        // A directory opens but does not map.
        assert!(got(Some(d), "imp_polys.u8", 0).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_build_refuses_importance_files_of_another_set() {
        use crate::fixture::{Geo, TempDir};
        let mut g = Geo::default();
        g.boundary("Testville", 8, [10.0, 20.0, 10.2, 20.2]);
        g.place("Testville", 1, 10.1, 20.1);
        g.place("Testburg", 2, 10.15, 20.15);
        g.poi("Test Hall", 10.05, 20.05, 0);
        let dir = TempDir::new("imp-build");
        let geo = g.load(&dir.0.join("geo"));
        let (imp, out) = (dir.0.join("imp"), dir.0.join("idx"));
        std::fs::create_dir_all(&imp).unwrap();
        let opts = BuildOpts {
            threads: 1, mem_mb: DEFAULT_RUN_BUDGET_MB, tmp: dir.0.join("tmp"), inline: true, extras: None,
            imp_dir: Some(imp.to_str().unwrap().into()), area_proxy: false, poi_max_tier: 3,
        };
        // One byte per place node, polygon and POI record.
        for (file, n) in [("imp_places.u8", 2), ("imp_polys.u8", 1), ("imp_poi.u8", 1)] {
            std::fs::write(imp.join(file), vec![0u8; n]).unwrap();
        }
        assert!(build(&geo, "places", &out, &opts).is_ok());
        assert!(build(&geo, "poi", &out, &opts).is_ok());

        for (file, n, sub) in [("imp_places.u8", 2, "places"), ("imp_polys.u8", 1, "places"), ("imp_poi.u8", 1, "poi")] {
            std::fs::write(imp.join(file), vec![0u8; n + 1]).unwrap();
            let err = build(&geo, sub, &out, &opts).unwrap_err();
            assert!(err.contains(file), "{err}");
            std::fs::write(imp.join(file), vec![0u8; n]).unwrap();
        }
    }
}
