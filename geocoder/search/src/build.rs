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
use std::time::{Duration, Instant};

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
    /// Polygon that holds the entity's country code (read only to transliterate a name: country
    /// codes are random reads into admin_polygons).
    cc_poly: u32,
    /// (class, ord, transliterate?, name)
    names: Vec<(u8, u8, bool, String)>,
}

/// One bit per record.
struct Bits(Vec<u64>);

impl Bits {
    fn new(n: usize) -> Self {
        Bits(vec![0; n.div_ceil(64)])
    }

    fn set(&mut self, i: u32) {
        self.0[i as usize / 64] |= 1 << (i % 64);
    }

    fn get(&self, i: u32) -> bool {
        self.0[i as usize / 64] >> (i % 64) & 1 == 1
    }
}

pub struct PlacesSource<'a> {
    geo: &'a Index,
    n_place_nodes: u32,
    /// The boundary slots, read back from the `places.bnd` just written (paged, not held).
    bnd: Mmap,
    n_slots: u32,
    n_linked: u32,
    /// label nodes that name a boundary slot (indexed through the boundary, not on their own)
    label_used: Bits,
    imp_places: Option<Mmap>,
    imp_polys: Option<Mmap>,
    extras: Option<&'a Extras>,
    area_proxy: bool,
}

impl<'a> PlacesSource<'a> {
    /// Derives the boundary slots and writes them to `bnd_path` (sorted by polygon).
    pub fn new(geo: &'a Index, opts: &BuildOpts, extras: Option<&'a Extras>, bnd_path: &Path) -> std::io::Result<Self> {
        let n_place_nodes = record_count::<PlaceNode>(geo.place_nodes.as_ref().expect("place_nodes"));
        let n_poly = record_count::<AdminPolygon>(&geo.admin_polygons);
        // A hidden node that names its claiming boundary is that boundary's label point.
        let mut link_buf = MapVec::with_capacity(n_poly as usize)?;
        for _ in 0..n_poly {
            link_buf.push(NO_DATA);
        }
        let link = link_buf.as_mut_slice();
        for i in 0..n_place_nodes {
            let pn = place_node_raw(geo, i);
            if is_hidden(&pn) && pn._pad1 & PLACE_FLAG_LABEL_OF != 0 && pn.parent_poly_id < n_poly {
                let l = &mut link[pn.parent_poly_id as usize];
                if *l == NO_DATA {
                    *l = i;
                }
            }
        }
        let mut label_used = Bits::new(n_place_nodes as usize);
        let mut w = BufWriter::with_capacity(WRITE_BUFFER, File::create(bnd_path)?);
        let (mut n_slots, mut n_linked) = (0u32, 0u32);
        // Sets built without the search flags: every named ring is its own boundary entity.
        let flagged = (0..n_poly).any(|p| polygon_raw(geo, p)._pad2 & POLY_FLAG_PRIMARY_RING != 0);
        for p in 0..n_poly {
            let ap = polygon_raw(geo, p);
            if ap.name_id == NO_DATA || (flagged && ap._pad2 & POLY_FLAG_PRIMARY_RING == 0) {
                continue;
            }
            let node = link[p as usize];
            let slot = if node != NO_DATA {
                let pn = place_node_raw(geo, node);
                label_used.set(node);
                BoundarySlot { poly: p, node, lat: pn.lat, lng: pn.lng }
            } else if (BOUNDARY_MIN_LEVEL..=BOUNDARY_MAX_LEVEL).contains(&ap.admin_level) && ap.area > 0.0 {
                let Some(v) = geo.admin_verts(&ap) else { continue };
                let (lat, lng) = ring_centroid(&v);
                BoundarySlot { poly: p, node: NO_DATA, lat, lng }
            } else {
                continue;
            };
            w.write_all(&slot.to_bytes())?;
            n_slots += 1;
            n_linked += slot.linked() as u32;
        }
        w.into_inner().map_err(|e| e.into_error())?;
        drop(link_buf);
        let bnd = unsafe { Mmap::map(&File::open(bnd_path)?) }?;
        let dir = opts.imp_dir.clone();
        Ok(PlacesSource {
            geo,
            n_place_nodes,
            bnd,
            n_slots,
            n_linked,
            label_used,
            imp_places: map_opt(dir.as_ref().map(|d| format!("{d}/imp_places.u8"))),
            imp_polys: map_opt(dir.as_ref().map(|d| format!("{d}/imp_polys.u8"))),
            extras,
            area_proxy: opts.area_proxy,
        })
    }

    fn slot(&self, pos: u32) -> BoundarySlot {
        BoundarySlot::from_bytes(&self.bnd[pos as usize * SLOT_BYTES..][..SLOT_BYTES])
    }

    fn len(&self) -> u32 {
        self.n_slots + self.n_place_nodes
    }

    fn fill(&self, pos: u32, e: &mut EntityBuf) -> bool {
        e.names.clear();
        let geo = self.geo;
        let (rref, base_id) = if pos < self.n_slots {
            let s = self.slot(pos);
            let ap = polygon_raw(geo, s.poly);
            let node = s.linked().then(|| place_node_raw(geo, s.node));
            let kind = if s.linked() { Kind::Linked } else { Kind::Area };
            let typ = boundary_type(&ap, node.as_ref());
            e.id = self.n_place_nodes + s.poly;
            e.imp = W_TYPE * type_base(kind, &typ, ap.admin_level) + extra_importance(byte_at(&self.imp_polys, s.poly));
            if self.area_proxy {
                e.imp += W_AREA_PROXY * area_proxy(ap.area, s.lat);
            }
            e.cc_poly = s.poly;
            let (primary, own) = slot_name_ids(geo, &s);
            if let Some(own) = own {
                let n = geo.get_string(own);
                if !n.is_empty() {
                    e.names.push((0, ORD_BOUNDARY_NAME, true, n.to_string()));
                }
            }
            (record_ref(KIND_POLYGON, s.poly), primary)
        } else {
            let i = pos - self.n_slots;
            let pn = place_node_raw(geo, i);
            // A hidden node is searchable on its own only when no boundary of this set claims it.
            if pn.name_id == NO_DATA || self.label_used.get(i) {
                return false;
            }
            let kind = place_node_kind(pn.place_type);
            let typ = PLACE_NODE_TYPES.get(pn.place_type as usize).copied().unwrap_or("place");
            e.id = i;
            e.imp = W_TYPE * type_base(kind, typ, 0) + extra_importance(byte_at(&self.imp_places, i));
            e.cc_poly = pn.parent_poly_id;
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
        e.cc_poly = r.parent_poly_id;
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
fn keys_of(geo: &Index, e: &EntityBuf, out: &mut Vec<(String, u8, bool, u8)>) {
    out.clear();
    let mut cc: Option<String> = None;
    let mut by_class: Vec<(u8, u8, Vec<crate::normalize::Token>)> = Vec::with_capacity(e.names.len() + 1);
    for (class, ord, tr, name) in &e.names {
        let t = tokens(name);
        if !t.is_empty() {
            by_class.push((*class, *ord, t));
        }
        if *tr {
            if let Some(latin) = transliterate(name, || cc.get_or_insert_with(|| cc_str(&parent_cc(geo, e.cc_poly)).to_string()).clone()) {
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

/// A fixed-capacity array in its own anonymous mapping. Dropping it unmaps it, so a phase's
/// buffers go back to the system before the next phase: glibc keeps freed heap memory for reuse
/// once a block this large has been freed, and the next phase's buffers came on top of it.
struct MapVec<T: Copy> {
    map: memmap2::MmapMut,
    len: usize,
    cap: usize,
    _t: std::marker::PhantomData<T>,
}

impl<T: Copy> MapVec<T> {
    fn with_capacity(cap: usize) -> std::io::Result<Self> {
        let map = memmap2::MmapOptions::new().len((cap * std::mem::size_of::<T>()).max(1)).map_anon()?;
        Ok(MapVec { map, len: 0, cap, _t: std::marker::PhantomData })
    }

    fn capacity(&self) -> usize {
        self.cap
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn clear(&mut self) {
        self.len = 0;
    }

    fn push(&mut self, v: T) {
        assert!(self.len < self.cap, "MapVec full");
        // SAFETY: in bounds of the mapping, which is page-aligned (so aligned for T).
        unsafe { (self.map.as_mut_ptr() as *mut T).add(self.len).write(v) };
        self.len += 1;
    }

    fn extend_from_slice(&mut self, s: &[T]) {
        assert!(self.len + s.len() <= self.cap, "MapVec full");
        // SAFETY: in bounds (checked above); a fresh mapping never overlaps `s`.
        unsafe { std::ptr::copy_nonoverlapping(s.as_ptr(), (self.map.as_mut_ptr() as *mut T).add(self.len), s.len()) };
        self.len += s.len();
    }

    fn as_slice(&self) -> &[T] {
        // SAFETY: the first `len` elements were written by push.
        unsafe { std::slice::from_raw_parts(self.map.as_ptr() as *const T, self.len) }
    }

    fn as_mut_slice(&mut self) -> &mut [T] {
        // SAFETY: as in as_slice.
        unsafe { std::slice::from_raw_parts_mut(self.map.as_mut_ptr() as *mut T, self.len) }
    }
}

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
    arena: MapVec<u8>,
    items: MapVec<Item>,
    files: Vec<PathBuf>,
    dir: PathBuf,
    tag: String,
    spilled_bytes: u64,
}

impl Runs {
    fn new(budget: usize, dir: &Path, tag: String) -> std::io::Result<Self> {
        Self::with_arena_limit(budget, MAX_RUN_ARENA_BYTES, dir, tag)
    }

    /// `new` with at most `arena_limit` key bytes per run.
    fn with_arena_limit(budget: usize, arena_limit: usize, dir: &Path, tag: String) -> std::io::Result<Self> {
        // Fixed capacities: a run spills when either fills, so memory never exceeds the budget.
        let (arena_cap, items_cap) = run_capacities(budget, arena_limit);
        Ok(Runs {
            arena: MapVec::with_capacity(arena_cap)?,
            items: MapVec::with_capacity(items_cap.max(1024))?,
            files: Vec::new(),
            dir: dir.to_path_buf(),
            tag,
            spilled_bytes: 0,
        })
    }

    fn push(&mut self, key: &[u8], p: Posting) -> std::io::Result<()> {
        let fits = MAX_KEY_BYTES.min(self.arena.capacity());
        if key.len() > fits {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Key too long: {} bytes, a run holds {fits}", key.len())));
        }

        if self.arena.len() + key.len() > self.arena.capacity() || self.items.len() == self.items.capacity() {
            self.spill()?;
        }
        let off = self.arena.len() as u32;
        self.arena.extend_from_slice(key);
        self.items.push(Item { off, len: key.len() as u16, flags: entry_flags(&p), ord: p.ord, entity: p.entity, imp: p.imp_q });
        Ok(())
    }

    /// Spills the last run and frees the buffers: the run files and the bytes spilled.
    fn finish(mut self) -> std::io::Result<(Vec<PathBuf>, u64)> {
        self.spill()?;
        Ok((self.files, self.spilled_bytes))
    }

    fn spill(&mut self) -> std::io::Result<()> {
        if self.items.is_empty() {
            return Ok(());
        }
        let arena = self.arena.as_slice();
        let key = |it: &Item| &arena[it.off as usize..it.off as usize + it.len as usize];
        self.items.as_mut_slice().sort_unstable_by(|a, b| key(a).cmp(key(b)).then(a.entity.cmp(&b.entity)));
        let path = self.dir.join(format!("{}-{}.run", self.tag, self.files.len()));
        let mut w = BufWriter::with_capacity(WRITE_BUFFER, File::create(&path)?);
        for it in self.items.as_slice() {
            self.spilled_bytes += write_entry(&mut w, key(it), it.entity, it.imp, it.flags, it.ord)?;
        }
        w.flush()?;
        self.files.push(path);
        self.items.clear();
        self.arena.clear();
        Ok(())
    }
}

/// (key bytes, entries) one sort run of `budget` bytes holds: RUN_ARENA_PERCENT of the budget for
/// keys, at most `arena_limit` of it, and the rest for entries.
fn run_capacities(budget: usize, arena_limit: usize) -> (usize, usize) {
    let arena_share = budget * RUN_ARENA_PERCENT / 100;
    (arena_share.min(arena_limit), (budget - arena_share) / std::mem::size_of::<Item>())
}

/// Sort-run memory of each of `threads` workers sharing `mem_mb`; an error below
/// MIN_RUN_BUDGET_BYTES, where the longest key would not fit a run.
fn run_budget(mem_mb: usize, threads: usize) -> Result<usize, String> {
    let budget = (mem_mb << 20) / threads;
    if budget < MIN_RUN_BUDGET_BYTES {
        return Err(format!("Sort budget too small: {budget} bytes per thread, at least {MIN_RUN_BUDGET_BYTES}"));
    }

    Ok(budget)
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
    readers: Vec<RunReader>,
    heap: BinaryHeap<Head>,
}

impl Merge {
    fn open(files: &[PathBuf]) -> std::io::Result<Self> {
        let mut readers = Vec::with_capacity(files.len());
        for f in files {
            readers.push(RunReader { r: BufReader::with_capacity(RUN_READ_BUFFER, File::open(f)?) });
        }
        let mut heap = BinaryHeap::with_capacity(readers.len());
        for (i, r) in readers.iter_mut().enumerate() {
            let mut h = Head { key: Vec::new(), p: Posting { entity: 0, imp_q: 0, partial: false, class: 0, ord: 0 }, run: i };
            if r.read_into(&mut h) {
                heap.push(h);
            }
        }
        Ok(Merge { readers, heap })
    }

    /// Copies the next entry into `key` and `p`; false after the last.
    fn next(&mut self, key: &mut Vec<u8>, p: &mut Posting) -> bool {
        let Some(mut top) = self.heap.peek_mut() else { return false };
        key.clear();
        key.extend_from_slice(&top.key);
        *p = top.p;
        let run = top.run;
        if !self.readers[run].read_into(&mut top) {
            std::collections::binary_heap::PeekMut::pop(top);
        }
        true
    }
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
            let mut w = BufWriter::with_capacity(WRITE_BUFFER, File::create(&path)?);
            let mut m = Merge::open(group)?;
            let mut key = Vec::new();
            let mut p = Posting { entity: 0, imp_q: 0, partial: false, class: 0, ord: 0 };
            while m.next(&mut key, &mut p) {
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

/// The postings of one key as the merge streams them (entity order), cut down while they arrive
/// to what the index keeps: one posting per entity (a whole-name match over a trailing-word one,
/// then the better name class), every whole-name posting, and the PARTIAL_CAP best trailing-word
/// postings (importance, then entity). A common trailing word ("church") never piles up.
#[derive(Default)]
struct Group {
    whole: Vec<Posting>,
    partial: BinaryHeap<WorstFirst>,
    last: Option<Posting>,
}

/// Trailing-word posting ordered worst first: lower importance, then higher entity.
struct WorstFirst(Posting);

impl PartialEq for WorstFirst {
    fn eq(&self, o: &Self) -> bool {
        self.cmp(o) == std::cmp::Ordering::Equal
    }
}
impl Eq for WorstFirst {}
impl PartialOrd for WorstFirst {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for WorstFirst {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        o.0.imp_q.cmp(&self.0.imp_q).then(self.0.entity.cmp(&o.0.entity))
    }
}

impl Group {
    fn is_empty(&self) -> bool {
        self.last.is_none()
    }

    fn push(&mut self, p: Posting) {
        match self.last {
            Some(l) if l.entity == p.entity => {
                if (p.partial, p.class) < (l.partial, l.class) {
                    self.last = Some(p);
                }
            }
            Some(l) => {
                self.keep(l);
                self.last = Some(p);
            }
            None => self.last = Some(p),
        }
    }

    fn keep(&mut self, p: Posting) {
        if !p.partial {
            self.whole.push(p);
            return;
        }
        self.partial.push(WorstFirst(p));
        if self.partial.len() > PARTIAL_CAP {
            self.partial.pop();
        }
    }

    /// The kept postings into `out`, best importance first (then whole-name, then entity);
    /// leaves the group empty.
    fn take(&mut self, out: &mut Vec<Posting>) {
        if let Some(l) = self.last.take() {
            self.keep(l);
        }
        out.clear();
        out.append(&mut self.whole);
        out.extend(self.partial.drain().map(|w| w.0));
        out.sort_unstable_by(|a, b| b.imp_q.cmp(&a.imp_q).then(a.partial.cmp(&b.partial)).then(a.entity.cmp(&b.entity)));
    }
}

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
    /// Writes one key with its kept postings (`Group::take`).
    fn add(&mut self, key: &[u8], group: &[Posting]) -> Result<(), String> {
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
                slot.hits.sort_by(|a, b| b.0.total_cmp(&a.0));
                slot.hits.truncate(PREFIX_TABLE_TOP * 4);
            }
        }
    }
}

fn flush_slot(slot: &mut PrefixSlot, with_names: bool) -> std::io::Result<()> {
    let mut v = std::mem::take(&mut slot.hits);
    let keys = std::mem::take(&mut slot.keys);
    if slot.prefix.is_empty() || (slot.chars > PREFIX_TABLE_ALL_CHARS && keys < HEAVY_PREFIX_MIN_KEYS) {
        return Ok(());
    }
    v.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.entity.cmp(&b.1.entity)));
    let mut seen = HashSet::new();
    let hits: Vec<PrefixHit> = v.into_iter().filter(|(_, h)| seen.insert(h.entity)).take(PREFIX_TABLE_TOP).map(|x| x.1).collect();
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

/// Builds the sub-indexes `names` ("places", "poi") of the flat set `geo` into `out`: every key
/// phase first, then every merge, so a merge's heap (the fst builder) is never held beside a key
/// phase's sort buffers.
pub fn build(geo: &Index, names: &[&str], out: &Path, opts: &BuildOpts) -> Result<Vec<(String, Stats)>, String> {
    let mut runs = Vec::with_capacity(names.len());
    for name in names {
        runs.push(sort_keys(geo, name, out, opts)?);
    }
    let mut stats = Vec::with_capacity(runs.len());
    for r in runs {
        let name = r.name.clone();
        stats.push((name, merge(r, out, opts)?));
    }
    Ok(stats)
}

/// One sub-index between its two phases: its sorted runs on disk.
pub struct KeyRuns {
    name: String,
    files: Vec<PathBuf>,
    with_names: bool,
    st: Stats,
    ents: u64,
    entries: u64,
    spilled: u64,
    t_src: Duration,
    t_keys: Duration,
}

/// Key phase of one sub-index: its entities' normalized keys, sorted in bounded runs spilled to
/// `opts.tmp` (places also writes `places.bnd` to `out`).
pub fn sort_keys(geo: &Index, name: &str, out: &Path, opts: &BuildOpts) -> Result<KeyRuns, String> {
    let threads = opts.threads.max(1);
    let budget = run_budget(opts.mem_mb, threads)?;

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
        let s = PlacesSource::new(geo, opts, extras.as_ref(), &out.join("places.bnd")).map_err(|e| e.to_string())?;
        st.add("place nodes", s.n_place_nodes);
        st.add("boundary slots", s.n_slots);
        st.add("boundary slots linked", s.n_linked);
        Source::Places(s)
    };
    let t_src = t0.elapsed();

    // ---- keys -> sorted runs, one worker per contiguous range of entities
    let n = src.len();
    type RunsOut = (Vec<PathBuf>, u64, u64, u64);
    let results: Vec<Result<RunsOut, String>> = std::thread::scope(|sc| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let src = &src;
                let tmp = &opts.tmp;
                let tag = format!("{name}-{t}");
                sc.spawn(move || -> Result<RunsOut, String> {
                    let lo = (n as u64 * t as u64 / threads as u64) as u32;
                    let hi = (n as u64 * (t as u64 + 1) / threads as u64) as u32;
                    let mut runs = Runs::new(budget, tmp, tag).map_err(|x| x.to_string())?;
                    let mut e = EntityBuf::default();
                    let mut keys = Vec::new();
                    let (mut ents, mut entries) = (0u64, 0u64);
                    for pos in lo..hi {
                        if !src.fill(pos, &mut e) {
                            continue;
                        }
                        keys_of(geo, &e, &mut keys);
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
                    // Hands the run memory back before the merge.
                    let (files, spilled) = runs.finish().map_err(|x| x.to_string())?;
                    Ok((files, spilled, ents, entries))
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut files = Vec::new();
    let (mut ents, mut entries, mut spilled) = (0u64, 0u64, 0u64);
    for r in results {
        let (f, s, e, k) = r?;
        ents += e;
        entries += k;
        spilled += s;
        files.extend(f);
    }
    drop(src);
    let t_keys = t0.elapsed() - t_src;
    Ok(KeyRuns { name: name.to_string(), files, with_names: extras.is_some(), st, ents, entries, spilled, t_src, t_keys })
}

/// Merge phase of one sub-index: its runs -> `<name>.fst`, `.post`, `.pfx.fst` and `.pfx` in `out`.
pub fn merge(runs: KeyRuns, out: &Path, opts: &BuildOpts) -> Result<Stats, String> {
    let t0 = Instant::now();
    let KeyRuns { name, files, with_names, mut st, ents, entries, spilled, t_src, t_keys } = runs;
    let name = name.as_str();
    let n_runs = files.len();
    let (files, rewritten) = reduce_runs(files, &opts.tmp, name).map_err(|e| e.to_string())?;
    let mk = |ext: &str| File::create(out.join(format!("{name}.{ext}"))).map_err(|e| e.to_string());
    let mut w = Writer {
        fst: fst::MapBuilder::new(BufWriter::with_capacity(WRITE_BUFFER, mk("fst")?)).map_err(|e| e.to_string())?,
        post: BufWriter::with_capacity(WRITE_BUFFER, mk("post")?),
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
    let mut m = Merge::open(&files).map_err(|e| e.to_string())?;
    let mut cur: Vec<u8> = Vec::new();
    let mut key: Vec<u8> = Vec::new();
    let mut p = Posting { entity: 0, imp_q: 0, partial: false, class: 0, ord: 0 };
    let mut group = Group::default();
    let mut list: Vec<Posting> = Vec::new();
    while m.next(&mut key, &mut p) {
        if key != cur {
            if !group.is_empty() {
                group.take(&mut list);
                w.add(&cur, &list)?;
            }
            std::mem::swap(&mut cur, &mut key);
        }
        group.push(p);
    }
    if !group.is_empty() {
        group.take(&mut list);
        w.add(&cur, &list)?;
    }
    drop(m);
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
    st.add("time keys+runs s", format!("{:.2}", t_keys.as_secs_f64()));
    st.add("time merge+write s", format!("{:.2}", t_merge.as_secs_f64()));
    st.add("time total s", format!("{:.2}", (t_src + t_keys + t_merge).as_secs_f64()));
    st.add("VmHWM MiB", format!("{:.1}", vm_hwm_kb() as f64 / 1024.0));
    Ok(st)
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
        while m.next(&mut key, &mut p) {
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
    fn bits_set_and_get_across_words() {
        let mut b = Bits::new(130);
        for i in [0, 63, 64, 129] {
            b.set(i);
        }
        let set: Vec<u32> = (0..130).filter(|&i| b.get(i)).collect();
        assert_eq!(set, vec![0, 63, 64, 129]);
    }

    #[test]
    fn map_vec_holds_what_was_pushed() {
        let mut v: MapVec<Item> = MapVec::with_capacity(4).unwrap();
        let item = |entity| Item { off: 0, len: 0, flags: 0, ord: 0, entity, imp: 0 };
        v.push(item(3));
        v.extend_from_slice(&[item(1), item(2)]);
        v.as_mut_slice().sort_unstable_by_key(|it| it.entity);
        assert_eq!(v.as_slice().iter().map(|it| it.entity).collect::<Vec<_>>(), vec![1, 2, 3]);
        v.clear();
        assert!(v.is_empty());
        assert_eq!(v.capacity(), 4);
    }

    #[test]
    #[should_panic(expected = "MapVec full")]
    fn map_vec_refuses_to_grow() {
        let mut v: MapVec<u8> = MapVec::with_capacity(2).unwrap();
        v.extend_from_slice(b"abc");
    }

    #[test]
    fn run_budget_refuses_less_than_the_longest_key_needs() {
        for (mem_mb, threads, want) in
            [(0, 1, None), (1, 16, None), (1, 9, Some((1 << 20) / 9)), (DEFAULT_RUN_BUDGET_MB, 1, Some(DEFAULT_RUN_BUDGET_MB << 20))]
        {
            assert_eq!(run_budget(mem_mb, threads).ok(), want, "{mem_mb} MiB, {threads} threads");
        }
        assert_eq!(run_budget(0, 1), Err(format!("Sort budget too small: 0 bytes per thread, at least {MIN_RUN_BUDGET_BYTES}")));
    }

    #[test]
    fn runs_hold_the_longest_key_at_the_least_budget() {
        let dir = tmp_dir("runs-longest");
        let longest = vec![b'z'; MAX_KEY_BYTES];
        let mut sut = Runs::new(MIN_RUN_BUDGET_BYTES, &dir, "t".into()).unwrap();
        sut.push(b"a", posting(1, 0, false, 0)).unwrap();
        sut.push(&longest, posting(2, 0, false, 0)).unwrap();
        sut.push(b"b", posting(3, 0, false, 0)).unwrap();
        let (files, _) = sut.finish().unwrap();
        let keys: Vec<Vec<u8>> = read_all(&files).into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), longest]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_keys_stay_addressable_at_any_budget() {
        // An entry addresses its key by a u32 offset into the run's arena.
        for mem_mb in [DEFAULT_RUN_BUDGET_MB, 6_826, 6_827, 6_828, 1 << 20] {
            let budget = run_budget(mem_mb, 1).unwrap();
            let (arena, items) = run_capacities(budget, MAX_RUN_ARENA_BYTES);
            assert!(arena <= u32::MAX as usize, "{mem_mb} MiB: {arena} key bytes");
            assert_eq!(items, (budget - budget * RUN_ARENA_PERCENT / 100) / std::mem::size_of::<Item>());
        }
    }

    #[test]
    fn runs_spill_at_the_arena_limit() {
        let dir = tmp_dir("runs-arena-limit");
        let mut sut = Runs::with_arena_limit(1 << 20, 64, &dir, "t".into()).unwrap();
        let keys: Vec<Vec<u8>> = (0..40).rev().map(|k: u32| format!("key{k:03}").into_bytes()).collect();
        for (entity, key) in keys.iter().enumerate() {
            sut.push(key, posting(entity as u32, 0, false, 0)).unwrap();
        }
        let (files, _) = sut.finish().unwrap();
        // 6-byte keys, 10 to a 64-byte arena.
        assert_eq!(files.len(), 4);
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(read_all(&files).into_iter().map(|(k, _)| k).collect::<Vec<_>>(), sorted);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn runs_refuse_a_key_they_cannot_hold() {
        for (budget, key_len) in [(0, 1), (MIN_RUN_BUDGET_BYTES - 1, MAX_KEY_BYTES), (2 * MIN_RUN_BUDGET_BYTES, MAX_KEY_BYTES + 1)] {
            let dir = tmp_dir(&format!("runs-refuse-{budget}"));
            let mut sut = Runs::new(budget, &dir, "t".into()).unwrap();
            let err = sut.push(&vec![b'z'; key_len], posting(1, 0, false, 0)).unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "budget {budget}, key {key_len}");
            let (files, _) = sut.finish().unwrap();
            assert!(files.is_empty());
            let _ = std::fs::remove_dir_all(&dir);
        }
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

    /// What the index kept before postings were capped while streaming: the whole group sorted,
    /// one posting per entity, every whole-name posting and the PARTIAL_CAP best trailing-word ones.
    fn kept_by_sorting(mut group: Vec<Posting>) -> Vec<Posting> {
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
        group
    }

    #[test]
    fn group_keeps_what_sorting_the_whole_group_kept() {
        let mut rng = Rng(3);
        for n in [1, 5, PARTIAL_CAP, PARTIAL_CAP + 1, 3 * PARTIAL_CAP + 17] {
            let postings: Vec<Posting> =
                (0..n as u32).map(|e| posting(e * 3, (rng.next() % 40) as u8, !rng.next().is_multiple_of(4), (rng.next() % 6) as u8)).collect();
            let mut sut = Group::default();
            for p in &postings {
                sut.push(*p);
            }
            let mut got = Vec::new();
            sut.take(&mut got);
            assert_eq!(got, kept_by_sorting(postings));
            assert!(sut.is_empty());
        }
    }

    #[test]
    fn group_keeps_an_entitys_whole_name_claim_over_a_trailing_word() {
        let mut sut = Group::default();
        sut.push(posting(9, 10, true, 0));
        sut.push(posting(9, 10, false, 3));
        sut.push(posting(12, 50, true, 1));
        let mut got = Vec::new();
        sut.take(&mut got);
        assert_eq!(got, vec![posting(12, 50, true, 1), posting(9, 10, false, 3)]);
    }
}
