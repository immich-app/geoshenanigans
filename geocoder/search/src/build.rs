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
use query_server::{AdminPolygon, Index, PlaceNode, PoiRecord, NO_DATA};
use serde_json::json;
use std::collections::{BinaryHeap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

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

impl BuildOpts {
    /// The index as it ships: one thread, the default sort budget, inline postings, every POI tier,
    /// nothing beyond the geocoder files.
    pub fn new(tmp: PathBuf) -> Self {
        BuildOpts {
            threads: 1,
            mem_mb: DEFAULT_RUN_BUDGET_MB,
            tmp,
            inline: true,
            extras: None,
            imp_dir: None,
            area_proxy: false,
            poi_max_tier: 3,
        }
    }
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
                // The builder's interior_point (ST_PointOnSurface, Nominatim's centre of an area):
                // an area centroid can fall outside a concave ring or in a hole.
                let Some((lat, lng)) = geo.admin_geometry.point_on_surface(&ap) else { continue };
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
        let dir = opts.imp_dir.as_deref();
        Ok(PlacesSource {
            geo,
            n_place_nodes,
            bnd,
            n_slots,
            n_linked,
            label_used,
            imp_places: map_importance(dir, "imp_places.u8", n_place_nodes)?,
            imp_polys: map_importance(dir, "imp_polys.u8", n_poly)?,
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
        let (rrefs, base_id) = if pos < self.n_slots {
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
            (s.record_refs(), primary)
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
        e.cc_poly = r.parent_poly_id;
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

    /// Adds one key entry; an error for a key longer than a run holds: MAX_KEY_BYTES (a run entry's
    /// u16 length), or the run's key arena when that is smaller.
    fn push(&mut self, key: &[u8], p: Posting) -> std::io::Result<()> {
        let fits = MAX_KEY_BYTES.min(self.arena.capacity());
        if key.len() > fits {
            let e = format!("entity {}: a key of {} bytes, over the {fits} bytes a sort run holds", p.entity, key.len());
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, e));
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
            let mut w = BufWriter::with_capacity(WRITE_BUFFER, File::create(&path)?);
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

    /// Hands the kept postings to `write`, best importance first (then whole-name, then entity),
    /// sorted in the group's own buffer so a key's postings are held once; leaves the group empty.
    fn take<R>(&mut self, write: impl FnOnce(&[Posting]) -> R) -> R {
        if let Some(l) = self.last.take() {
            self.keep(l);
        }
        self.whole.extend(self.partial.drain().map(|w| w.0));
        self.whole.sort_unstable_by(|a, b| b.imp_q.cmp(&a.imp_q).then(a.partial.cmp(&b.partial)).then(a.entity.cmp(&b.entity)));
        let r = write(&self.whole);
        self.whole.clear();
        r
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

impl PrefixSlot {
    /// An error of this length's prefix temp file, naming it.
    fn error(&self, e: std::io::Error) -> String {
        format!("prefix temp file {}: {e}", self.path.display())
    }
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
    /// Writes one key with its kept postings (`Group::take`).
    fn add(&mut self, key: &[u8], group: &[Posting]) -> Result<(), String> {
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
        self.prefix_add(key, group)
    }

    fn prefix_add(&mut self, key: &[u8], list: &[Posting]) -> Result<(), String> {
        let Ok(key) = std::str::from_utf8(key) else { return Ok(()) };
        let klen = key.chars().count();
        let top = best_postings(list, PREFIX_TABLE_TOP);
        let mut pfx = String::new();
        for (n, c) in key.chars().take(PREFIX_TABLE_MAX_CHARS).enumerate() {
            pfx.push(c);
            let n = n + 1;
            if self.slots[n - 1].prefix != pfx {
                let slot = &mut self.slots[n - 1];
                flush_slot(slot).map_err(|e| slot.error(e))?;
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
        Ok(())
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

/// Builds the sub-indexes `names` ("places", "poi") of the flat set `geo` into `out`: every key
/// phase first, then every merge, so a merge's heap (the fst builder) is never held beside a key
/// phase's sort buffers.
pub fn build(geo: &Index, names: &[&str], out: &Path, opts: &BuildOpts) -> Result<Vec<(String, Stats)>, String> {
    let mut runs = Vec::with_capacity(names.len());
    for name in names {
        runs.push(sort_keys(geo, name, out, opts).map_err(|e| format!("build {name}: {e}"))?);
    }
    let mut stats = Vec::with_capacity(runs.len());
    for r in runs {
        let name = r.name.clone();
        let st = merge(r, out, opts).map_err(|e| format!("build {name}: {e}"))?;
        stats.push((name, st));
    }
    Ok(stats)
}

/// One sub-index between its two phases: its sorted runs on disk.
pub struct KeyRuns {
    name: String,
    files: Vec<PathBuf>,
    st: Stats,
    ents: u64,
    entries: u64,
    spilled: u64,
    t_src: Duration,
    t_keys: Duration,
}

impl Drop for KeyRuns {
    /// Removes the runs no merge took: a failed build leaves none of a sub-index it never merged.
    fn drop(&mut self) {
        for f in &self.files {
            let _ = std::fs::remove_file(f);
        }
    }
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
        Source::Poi(PoiSource::new(geo, opts, extras.as_ref()).map_err(|e| e.to_string())?)
    } else {
        let s = PlacesSource::new(geo, opts, extras.as_ref(), &out.join(BOUNDARY_FILE)).map_err(|e| e.to_string())?;
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
    Ok(KeyRuns { name: name.to_string(), files, st, ents, entries, spilled, t_src, t_keys })
}

/// Merge phase of one sub-index: its runs -> `<name>.fst`, `.post`, `.pfx.fst` and `.pfx` in `out`.
pub fn merge(mut runs: KeyRuns, out: &Path, opts: &BuildOpts) -> Result<Stats, String> {
    let t0 = Instant::now();
    let (files, mut st) = (std::mem::take(&mut runs.files), std::mem::take(&mut runs.st));
    let (ents, entries, spilled, t_src, t_keys) = (runs.ents, runs.entries, runs.spilled, runs.t_src, runs.t_keys);
    let name = runs.name.as_str();
    let n_runs = files.len();
    let (files, rewritten) = reduce_runs(files, &opts.tmp, name).map_err(|e| e.to_string())?;
    let [keys_path, post_path, pfx_keys_path, pfx_path] = sub_index_files(name).map(|f| out.join(f));
    let mk = |path: &Path| File::create(path).map_err(|e| e.to_string());
    let mut w = Writer {
        fst: fst::MapBuilder::new(BufWriter::with_capacity(WRITE_BUFFER, mk(&keys_path)?)).map_err(|e| e.to_string())?,
        post: BufWriter::with_capacity(WRITE_BUFFER, mk(&post_path)?),
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
    let mut group = Group::default();
    while m.next(&mut key, &mut p).map_err(|e| e.to_string())? {
        if key != cur {
            if !group.is_empty() {
                group.take(|list| w.add(&cur, list))?;
            }
            std::mem::swap(&mut cur, &mut key);
        }
        group.push(p);
    }
    if !group.is_empty() {
        group.take(|list| w.add(&cur, list))?;
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
        flush_slot(sl).map_err(|e| sl.error(e))?;
        sl.out.flush().map_err(|e| sl.error(e))?;
        n_prefixes += sl.flushed;
        pfx_readers.push(BufReader::new(File::open(&sl.path).map_err(|e| sl.error(e))?));
    }
    // Merge the per-length files: each is sorted, and a prefix occurs in one length only.
    let mut heads: Vec<Option<(Vec<u8>, Vec<u8>)>> =
        pfx_readers.iter_mut().enumerate().map(|(i, r)| read_prefix(r).map_err(|e| slots[i].error(e))).collect::<Result<_, _>>()?;
    let mut pf = fst::MapBuilder::new(BufWriter::new(mk(&pfx_keys_path)?)).map_err(|e| e.to_string())?;
    let mut pb = BufWriter::new(mk(&pfx_path)?);
    let mut off = 0u64;
    loop {
        let Some(i) = (0..heads.len()).filter(|&i| heads[i].is_some()).min_by(|&a, &b| heads[a].as_ref().unwrap().0.cmp(&heads[b].as_ref().unwrap().0)) else {
            break;
        };
        let (k, blob) = heads[i].take().unwrap();
        pf.insert(&k, off).map_err(|e| e.to_string())?;
        pb.write_all(&blob).map_err(|e| e.to_string())?;
        off += blob.len() as u64;
        heads[i] = read_prefix(&mut pfx_readers[i]).map_err(|e| slots[i].error(e))?;
    }
    for sl in &slots {
        let _ = std::fs::remove_file(&sl.path);
    }
    pf.finish().map_err(|e| e.to_string())?;
    pb.flush().map_err(|e| e.to_string())?;
    let t_merge = t0.elapsed();

    let size = |path: &Path| std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
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
    st.add("bytes fst", size(&keys_path));
    st.add("bytes post", size(&post_path));
    st.add("bytes pfx", size(&pfx_keys_path) + size(&pfx_path));
    if name != "poi" {
        st.add("bytes bnd", size(&out.join(BOUNDARY_FILE)));
    }
    st.add("time sources s", format!("{:.2}", t_src.as_secs_f64()));
    st.add("time keys+runs s", format!("{:.2}", t_keys.as_secs_f64()));
    st.add("time merge+write s", format!("{:.2}", t_merge.as_secs_f64()));
    st.add("time total s", format!("{:.2}", (t_src + t_keys + t_merge).as_secs_f64()));
    st.add("VmHWM MiB", format!("{:.1}", vm_hwm_kb() as f64 / 1024.0));
    Ok(st)
}

/// The sub-indexes of `selected` that `geo` has records for: POIs only where it has them.
pub fn held_subs(geo: &Index, selected: &[&'static str]) -> Vec<&'static str> {
    selected.iter().copied().filter(|&s| s != "poi" || geo.poi_records.is_some()).collect()
}

/// The files of sub-index `name`, one per SUB_INDEX_EXTS.
pub fn sub_index_files(name: &str) -> [String; 4] {
    SUB_INDEX_EXTS.map(|ext| format!("{name}.{ext}"))
}

/// Whether `dir` holds sub-index `name`: its keys file is there.
pub fn has_sub_index(dir: &Path, name: &str) -> bool {
    let [keys, ..] = sub_index_files(name);
    dir.join(keys).exists()
}

/// The META_FILE fields of an index of `geo` built with `opts`: what `SearchIndex::open` reads
/// and the options it was built with.
pub fn index_meta(geo: &Index, opts: &BuildOpts) -> serde_json::Map<String, serde_json::Value> {
    let n_place_nodes = geo.place_nodes.as_ref().map_or(0, record_count::<PlaceNode>);
    [
        ("n_place_nodes", json!(n_place_nodes)),
        ("extras", json!(opts.extras.is_some())),
        ("inline", json!(opts.inline)),
        ("imp", json!(opts.imp_dir.is_some())),
        ("area_proxy", json!(opts.area_proxy)),
        ("poi_max_tier", json!(opts.poi_max_tier)),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect()
}

/// Whether the index whose META_FILE is `meta` was built with extra names (`index_meta`): one
/// `SearchIndex::open` reads only with their file.
pub fn has_extra_names(meta: &serde_json::Value) -> bool {
    meta["extras"].as_bool().unwrap_or(false)
}

/// Builds the sub-indexes `subs` of `geo` for the index directory `out`, the value `meta` gives
/// with each one's stats becoming its search_meta.json. Every file is written into a staging
/// directory and moved into `out` only once all of them are complete, search_meta.json last
/// (`publish`): a failed build or move leaves `out` as it was. The build holds `out` from before
/// `meta` runs until it has cleaned up (`HeldIndexDir`). Returns the stats; an error, before
/// anything is built, while another build or an open holds `out`, when `meta` fails, or while
/// files a failed publish could not put back are still aside.
pub fn build_all(geo: &Index, subs: &[&str], out: &Path, opts: &BuildOpts,
                 meta: impl FnOnce() -> Result<serde_json::Value, String>) -> Result<Vec<(String, Stats)>, String> {
    build_held(geo, subs, &HeldIndexDir::take(out, Contended::Fail)?, opts, meta)
}

/// `build_all` into an index directory already held.
pub fn build_held(geo: &Index, subs: &[&str], out: &HeldIndexDir, opts: &BuildOpts,
                  meta: impl FnOnce() -> Result<serde_json::Value, String>) -> Result<Vec<(String, Stats)>, String> {
    let out = out.path.as_path();
    let meta = meta()?;
    let stage = out.join(BUILD_STAGE_DIR);
    if holds_replaced(&stage) {
        let aside = stage.join(BUILD_ASIDE_DIR);
        return Err(format!("{}: files a failed publish replaced; move them back into {} or remove them", aside.display(), out.display()));
    }
    let _ = std::fs::remove_dir_all(&stage);
    let built = build_staged(geo, subs, &stage, opts, meta)
        .and_then(|stats| publish(&stage, out, |from, to| std::fs::rename(from, to)).map(|_| stats).map_err(|e| format!("publish {}: {e}", out.display())));
    clear_stage(&stage);
    // While `out` is still held: the temp directory is by default inside it, and removing it
    // after would race the next build's.
    let _ = std::fs::remove_dir(&opts.tmp);
    built
}

/// What taking an index directory does while another holder has it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Contended {
    /// Fails at once.
    Fail,
    /// Waits until it is released.
    Wait,
}

/// An index directory held: an exclusive lock on the directory itself (no lock file is left in
/// the index), released when dropped or when the process exits. Every build takes it, and
/// `dataset::open` through its open, so builds into one index directory never share its
/// search_meta.json, staging or temp directories, and no open reads or clears files a build
/// is writing.
pub struct HeldIndexDir {
    path: PathBuf,
    _lock: File,
}

impl HeldIndexDir {
    pub fn take(path: &Path, contended: Contended) -> Result<HeldIndexDir, String> {
        std::fs::create_dir_all(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let dir = File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
        match contended {
            Contended::Fail => dir.try_lock().map_err(|e| match e {
                std::fs::TryLockError::WouldBlock => format!("{}: another build or open is using this index directory", path.display()),
                std::fs::TryLockError::Error(e) => format!("lock {}: {e}", path.display()),
            }),
            Contended::Wait => dir.lock().map_err(|e| format!("lock {}: {e}", path.display())),
        }?;
        Ok(HeldIndexDir { path: path.to_path_buf(), _lock: dir })
    }
}

/// Whether a publish into `out` did not finish: one cut off while it moved its files in, or that
/// failed to put back the files it replaced, leaves its aside dir (made before its first move,
/// removed after its last). `out` may then hold some of its files beside some of the index it
/// replaced, under the META_FILE of either.
pub fn has_unfinished_publish(out: &Path) -> bool {
    out.join(BUILD_STAGE_DIR).join(BUILD_ASIDE_DIR).exists()
}

/// Whether a publish that failed to put files back left the files they replaced in `stage`.
fn holds_replaced(stage: &Path) -> bool {
    std::fs::read_dir(stage.join(BUILD_ASIDE_DIR)).is_ok_and(|mut d| d.next().is_some())
}

/// Removes the staging directory, unless it holds files a failed publish replaced.
fn clear_stage(stage: &Path) {
    if !holds_replaced(stage) {
        let _ = std::fs::remove_dir_all(stage);
    }
}

fn build_staged(geo: &Index, subs: &[&str], stage: &Path, opts: &BuildOpts, mut meta: serde_json::Value) -> Result<Vec<(String, Stats)>, String> {
    std::fs::create_dir_all(stage).map_err(|e| format!("{}: {e}", stage.display()))?;
    let stats = build(geo, subs, stage, opts)?;
    for (s, st) in &stats {
        meta[s.as_str()] = st.lines.iter().map(|(k, v)| (k.clone(), serde_json::json!(v))).collect::<serde_json::Map<_, _>>().into();
    }
    let text = serde_json::to_string_pretty(&meta).map_err(|e| e.to_string())?;
    std::fs::write(stage.join(META_FILE), text).map_err(|e| e.to_string())?;
    Ok(stats)
}

/// Moves every file of `stage` into `out` with `rename`, search_meta.json last. Each file it
/// replaces is moved aside first; when a move fails, the files moved so far are put back, and
/// a file that cannot be put back stays aside (the error names the directory).
fn publish(stage: &Path, out: &Path, rename: impl Fn(&Path, &Path) -> std::io::Result<()>) -> std::io::Result<()> {
    let mut names = std::fs::read_dir(stage)?.map(|e| e.map(|e| e.file_name())).collect::<std::io::Result<Vec<_>>>()?;
    names.sort_by_key(|n| (n == META_FILE, n.clone()));
    let aside = stage.join(BUILD_ASIDE_DIR);
    std::fs::create_dir(&aside)?;
    // (name, whether it replaced a file of `out`)
    let mut moved = Vec::new();
    for n in &names {
        let (to, old) = (out.join(n), aside.join(n));
        let replaced = match rename(&to, &old) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        };
        let step = replaced.and_then(|r| {
            moved.push((n, r));
            rename(&stage.join(n), &to)
        });
        if let Err(e) = step {
            let mut stuck = Vec::new();
            for (n, replaced) in moved.into_iter().rev() {
                let back = if replaced { rename(&aside.join(n), &out.join(n)) } else { std::fs::remove_file(out.join(n)) };
                if back.is_err_and(|e| e.kind() != std::io::ErrorKind::NotFound) {
                    stuck.push(n.to_string_lossy());
                }
            }
            if stuck.is_empty() {
                return Err(e);
            }
            let e = format!("{e}; could not put back {}: the files they replaced are in {}", stuck.join(", "), aside.display());
            return Err(std::io::Error::other(e));
        }
    }
    let _ = std::fs::remove_dir_all(&aside);
    Ok(())
}

/// A partial rebuild must keep the extra-name source required by retained sub-indexes.
pub fn check_retained_names(out: &Path, building: &[&str], with_extras: bool) -> Result<(), String> {
    let text = match std::fs::read_to_string(out.join(META_FILE)) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    let meta: serde_json::Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    for &name in SUB_INDEXES {
        if !building.contains(&name) && has_sub_index(out, name)
            && has_extra_names(&meta) != with_extras {
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
            let got = sut.take(|list| list.to_vec());
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
        let got = sut.take(|list| list.to_vec());
        assert_eq!(got, vec![posting(12, 50, true, 1), posting(9, 10, false, 3)]);
    }

    #[test]
    fn group_holds_only_the_next_keys_postings_after_a_take() {
        let mut sut = Group::default();
        sut.push(posting(3, 20, false, 0));
        sut.push(posting(5, 30, true, 0));
        sut.take(|_| ());
        sut.push(posting(7, 10, false, 1));
        let got = sut.take(|list| list.to_vec());
        assert_eq!(got, vec![posting(7, 10, false, 1)]);
    }

    /// Heap the calling thread holds, counted by the test binary's allocator (per thread, so tests
    /// running beside one another do not count).
    mod heap {
        use std::alloc::{GlobalAlloc, Layout, System};
        use std::cell::Cell;

        thread_local! {
            static LIVE: Cell<isize> = const { Cell::new(0) };
            static PEAK: Cell<isize> = const { Cell::new(0) };
        }

        fn note(delta: isize) {
            let live = LIVE.get() + delta;
            LIVE.set(live);
            PEAK.set(PEAK.get().max(live));
        }

        struct Counting;

        unsafe impl GlobalAlloc for Counting {
            unsafe fn alloc(&self, l: Layout) -> *mut u8 {
                let p = unsafe { System.alloc(l) };
                if !p.is_null() {
                    note(l.size() as isize);
                }
                p
            }

            unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
                let p = unsafe { System.alloc_zeroed(l) };
                if !p.is_null() {
                    note(l.size() as isize);
                }
                p
            }

            unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
                unsafe { System.dealloc(p, l) };
                note(-(l.size() as isize));
            }

            unsafe fn realloc(&self, p: *mut u8, l: Layout, size: usize) -> *mut u8 {
                let q = unsafe { System.realloc(p, l, size) };
                if !q.is_null() {
                    note(size as isize - l.size() as isize);
                }
                q
            }
        }

        #[global_allocator]
        static COUNTING: Counting = Counting;

        /// `f`'s result and the most heap this thread held above its level at the call while `f` ran.
        pub fn peak_during<R>(f: impl FnOnce() -> R) -> (R, usize) {
            let base = LIVE.get();
            PEAK.set(base);
            let r = f();
            (r, (PEAK.get() - base) as usize)
        }
    }

    /// Merges one key named in full by `n` entities: the merge's peak heap and the key's encoded
    /// list bytes (`.post`).
    fn merge_one_key(n: u32) -> (usize, usize) {
        let dir = tmp_dir(&format!("merge-heap-{n}"));
        let mut run: Vec<(Vec<u8>, Posting)> =
            (0..n).map(|entity| (b"church".to_vec(), Posting { entity, imp_q: 100, partial: false, class: 0, ord: 0 })).collect();
        let files = vec![write_run(&dir, "k.run", &mut run)];
        let runs = KeyRuns {
            name: "poi".into(),
            files,
            st: Stats::default(),
            ents: 0,
            entries: 0,
            spilled: 0,
            t_src: Duration::ZERO,
            t_keys: Duration::ZERO,
        };
        let opts = BuildOpts {
            threads: 1,
            mem_mb: DEFAULT_RUN_BUDGET_MB,
            tmp: dir.clone(),
            inline: false,
            extras: None,
            imp_dir: None,
            area_proxy: false,
            poi_max_tier: 3,
        };
        let (r, peak) = heap::peak_during(|| merge(runs, &dir, &opts));
        r.unwrap();
        let post = std::fs::metadata(dir.join("poi.post")).unwrap().len() as usize;
        let _ = std::fs::remove_dir_all(&dir);
        (peak, post)
    }

    #[test]
    fn merge_holds_a_keys_whole_name_postings_once() {
        // Over a one-posting key, the heap grows by one buffer of the postings and the list encoded
        // from them; a second copy of the postings takes it to two copies plus the list.
        let n = 1 << 16;
        let (base, _) = merge_one_key(1);
        let (peak, post) = merge_one_key(n);
        let copy = n as usize * std::mem::size_of::<Posting>();
        let growth = peak - base;
        assert!(growth < 2 * copy + post, "heap grew {growth} bytes: postings {copy} bytes, encoded list {post} bytes");
    }

    #[test]
    fn runs_refuse_keys_over_the_length_limit() {
        let dir = tmp_dir("long-key");
        let mut runs = Runs::new(1 << 20, &dir, "t".into()).unwrap();
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

        let err = build(&geo, &["places"], &dir.0.join("idx"), &opts).err();

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

    #[cfg(target_os = "linux")]
    #[test]
    fn a_failed_prefix_spill_fails_the_add() {
        let dir = tmp_dir("pfx-full");
        let mut w = writer(&dir);
        // Unbuffered onto a full device: flushing prefix "a" when "b" starts fails to write.
        let full = PathBuf::from("/dev/full");
        w.slots[0].out = BufWriter::with_capacity(0, File::options().write(true).open(&full).unwrap());
        w.slots[0].path = full;
        w.add(b"a", &[posting(1, 9, false, 0)]).unwrap();

        let err = w.add(b"b", &[posting(2, 9, false, 0)]).err();

        assert!(err.as_deref().is_some_and(|e| e.contains("/dev/full")), "{err:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prefix_table_keeps_distinct_entities() {
        // 30 minor entities under "a", then 10 major ones with many names each: pruning hits rather
        // than entities leaves only the 10 major ones in the table.
        let dir = std::env::temp_dir().join(format!("pfx-distinct-{}", std::process::id()));
        let mut w = writer(&dir);
        let post = |entity, imp_q| Posting { entity, imp_q, partial: false, class: 0, ord: 0 };
        for e in 0..30u32 {
            w.prefix_add(format!("aa{e:03}").as_bytes(), &[post(1000 + e, 10)]).unwrap();
        }
        let major: Vec<Posting> = (0..10).map(|e| post(e, 250)).collect();
        for k in 0..300 {
            w.prefix_add(format!("ab{k:03}").as_bytes(), &major).unwrap();
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
        w.prefix_add(b"abc", &list).unwrap();
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
        use query_server::AdminPoint;
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
            let label = AdminPoint::new(f.lat as f64, f.lng as f64);
            assert!(geo.admin_geometry.contains_once(&polygon_raw(&geo, poly), label), "{q} at {} {}", f.lat, f.lng);
            assert_eq!(f.ctx.county, q);
        }
    }

    #[test]
    fn a_failed_rebuild_leaves_the_index_as_it_was() {
        use crate::fixture::{build_search, Geo, TempDir};
        use crate::query::Searcher;
        use crate::reader::SearchIndex;
        let mut g = Geo::default();
        g.boundary("Testville", 8, [10.0, 20.0, 10.2, 20.2]);
        g.place("Testburg", 1, 10.1, 20.1);
        g.poi("Test Hall", 10.05, 20.05, 0);
        let dir = TempDir::new("rebuild");
        let geo = g.load(&dir.0.join("geo"));
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        let entries = |d: &Path| {
            let mut v: Vec<_> = std::fs::read_dir(d).unwrap().map(|e| e.unwrap().path()).map(|p| (std::fs::read(&p).ok(), p)).collect();
            v.sort();
            v
        };
        let before = entries(&out);
        // Importance bytes change the places postings; the POI build after them fails on an
        // importance file of the wrong size.
        let imp = dir.0.join("imp");
        std::fs::create_dir_all(&imp).unwrap();
        for (file, n) in [("imp_places.u8", 1), ("imp_polys.u8", 1), ("imp_poi.u8", 2)] {
            std::fs::write(imp.join(file), vec![200u8; n]).unwrap();
        }
        let opts = BuildOpts {
            threads: 1, mem_mb: DEFAULT_RUN_BUDGET_MB, tmp: dir.0.join("tmp"), inline: true, extras: None,
            imp_dir: Some(imp.to_str().unwrap().into()), area_proxy: false, poi_max_tier: 3,
        };

        let err = build_all(&geo, &["places", "poi"], &out, &opts, || Ok(serde_json::json!({}))).err();

        assert!(err.as_deref().is_some_and(|e| e.contains("imp_poi.u8")), "{err:?}");
        let after = entries(&out);
        let changed: Vec<_> = before.iter().filter(|e| !after.contains(e)).chain(after.iter().filter(|e| !before.contains(e))).map(|e| &e.1).collect();
        assert!(changed.is_empty(), "changed: {changed:?}");
        let idx = SearchIndex::open(&geo, &out, None, None).unwrap();
        let (found, _) = Searcher::new(&idx).search("Testburg", 5);
        assert_eq!(found.iter().map(|f| f.hit.entity).collect::<Vec<_>>(), [0]);
    }

    #[test]
    fn a_failed_build_leaves_no_sort_runs_behind() {
        use crate::fixture::{Geo, TempDir};
        let mut g = Geo::default();
        g.boundary("Testville", 8, [10.0, 20.0, 10.2, 20.2]);
        g.place("Testburg", 1, 10.1, 20.1);
        g.poi("Test Hall", 10.05, 20.05, 0);
        let dir = TempDir::new("failed-runs");
        let geo = g.load(&dir.0.join("geo"));
        let out = dir.0.join("idx");
        // The places keys are sorted into runs before the POI build fails on an importance file
        // of the wrong size.
        let imp = dir.0.join("imp");
        std::fs::create_dir_all(&imp).unwrap();
        for (file, n) in [("imp_places.u8", 1), ("imp_polys.u8", 1), ("imp_poi.u8", 2)] {
            std::fs::write(imp.join(file), vec![200u8; n]).unwrap();
        }
        let opts = BuildOpts {
            threads: 1, mem_mb: DEFAULT_RUN_BUDGET_MB, tmp: out.join("tmp"), inline: true, extras: None,
            imp_dir: Some(imp.to_str().unwrap().into()), area_proxy: false, poi_max_tier: 3,
        };

        let err = build_all(&geo, &["places", "poi"], &out, &opts, || Ok(serde_json::json!({}))).err();

        assert!(err.as_deref().is_some_and(|e| e.contains("imp_poi.u8")), "{err:?}");
        let left: Vec<_> = std::fs::read_dir(&opts.tmp).into_iter().flatten().map(|e| e.unwrap().file_name()).collect();
        assert!(left.is_empty(), "left in the temp directory: {left:?}");
        assert!(!opts.tmp.exists());
    }

    #[test]
    fn a_failed_publish_puts_back_what_it_replaced() {
        let dir = tmp_dir("publish");
        let (stage, out) = (dir.join("stage"), dir.join("out"));
        for (d, file, text) in [(&stage, "a", "new a"), (&stage, "b", "new b"), (&stage, META_FILE, "new meta"),
                                (&out, "a", "old a"), (&out, META_FILE, "old meta")] {
            std::fs::create_dir_all(d).unwrap();
            std::fs::write(d.join(file), text).unwrap();
        }
        let contents = |d: &Path| {
            let mut v: Vec<_> = std::fs::read_dir(d).unwrap().map(|e| e.unwrap().path()).map(|p| (std::fs::read_to_string(&p).ok(), p)).collect();
            v.sort();
            v
        };
        let before = contents(&out);
        // Moving b in fails once a has moved.
        let rename = |from: &Path, to: &Path| {
            if from.ends_with("stage/b") {
                return Err(std::io::Error::other("injected"));
            }
            std::fs::rename(from, to)
        };

        let result = publish(&stage, &out, rename);
        clear_stage(&stage);

        assert!(result.is_err());
        assert_eq!(contents(&out), before);
        assert!(!stage.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_publish_is_unfinished_from_its_first_move_to_its_last() {
        let out = tmp_dir("unfinished");
        let stage = out.join(BUILD_STAGE_DIR);
        for (d, file) in [(&stage, "a"), (&stage, "b"), (&stage, META_FILE), (&out, "a"), (&out, META_FILE)] {
            std::fs::create_dir_all(d).unwrap();
            std::fs::write(d.join(file), file).unwrap();
        }
        // Whether the publish reads as unfinished at each of its moves.
        let seen = std::cell::RefCell::new(Vec::new());
        let rename = |from: &Path, to: &Path| {
            seen.borrow_mut().push(has_unfinished_publish(&out));
            std::fs::rename(from, to)
        };

        publish(&stage, &out, rename).unwrap();

        assert_eq!(*seen.borrow(), [true; 6]);
        assert!(!has_unfinished_publish(&out));
        let _ = std::fs::remove_dir_all(&out);
    }

    #[test]
    fn a_failed_put_back_keeps_the_replaced_files() {
        let dir = tmp_dir("put-back");
        let (stage, out) = (dir.join("stage"), dir.join("out"));
        for (d, file, text) in [(&stage, "a", "new a"), (&stage, "b", "new b"), (&out, "a", "old a")] {
            std::fs::create_dir_all(d).unwrap();
            std::fs::write(d.join(file), text).unwrap();
        }
        // Moving b in fails once a has moved, and so does putting the old a back.
        let rename = |from: &Path, to: &Path| {
            if from.ends_with("stage/b") || from.ends_with(Path::new(BUILD_ASIDE_DIR).join("a")) {
                return Err(std::io::Error::other("injected"));
            }
            std::fs::rename(from, to)
        };

        let err = publish(&stage, &out, rename).unwrap_err();
        clear_stage(&stage);

        let aside = stage.join(BUILD_ASIDE_DIR);
        assert!(err.to_string().contains(aside.to_str().unwrap()), "{err}");
        assert_eq!(std::fs::read_to_string(aside.join("a")).ok().as_deref(), Some("old a"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_build_keeps_the_files_a_failed_publish_could_not_put_back() {
        use crate::fixture::{build_search, Geo, TempDir};
        let mut g = Geo::default();
        g.place("Testburg", 1, 10.1, 20.1);
        let dir = TempDir::new("kept-replaced");
        let geo = g.load(&dir.0.join("geo"));
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        let kept = out.join(BUILD_STAGE_DIR).join(BUILD_ASIDE_DIR).join("places.fst");
        std::fs::create_dir_all(kept.parent().unwrap()).unwrap();
        std::fs::write(&kept, "old places.fst").unwrap();
        let opts = BuildOpts {
            threads: 1, mem_mb: DEFAULT_RUN_BUDGET_MB, tmp: dir.0.join("tmp"), inline: true, extras: None,
            imp_dir: None, area_proxy: false, poi_max_tier: 3,
        };

        let err = build_all(&geo, &["places"], &out, &opts, || Ok(serde_json::json!({}))).err();

        assert!(err.as_deref().is_some_and(|e| e.contains(BUILD_ASIDE_DIR)), "{err:?}");
        assert_eq!(std::fs::read_to_string(&kept).ok().as_deref(), Some("old places.fst"));
    }

    #[test]
    fn a_build_is_refused_while_another_build_holds_the_index_directory() {
        use crate::fixture::{build_search, Geo, TempDir};
        let mut g = Geo::default();
        g.place("Testburg", 1, 10.1, 20.1);
        g.poi("Test Hall", 10.05, 20.05, 0);
        let dir = TempDir::new("held");
        let geo = g.load(&dir.0.join("geo"));
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        // A places build holds `out` and has staged a file.
        let other = File::open(&out).unwrap();
        other.try_lock().unwrap();
        let staged = out.join(BUILD_STAGE_DIR).join("places.fst");
        std::fs::create_dir_all(staged.parent().unwrap()).unwrap();
        std::fs::write(&staged, "staged by the other build").unwrap();
        let entries = |d: &Path| {
            let mut v: Vec<_> = std::fs::read_dir(d).unwrap().map(|e| e.unwrap().path()).map(|p| (std::fs::read(&p).ok(), p)).collect();
            v.sort();
            v
        };
        let before = entries(&out);
        let opts = BuildOpts {
            threads: 1, mem_mb: DEFAULT_RUN_BUDGET_MB, tmp: dir.0.join("tmp"), inline: true, extras: None,
            imp_dir: None, area_proxy: false, poi_max_tier: 3,
        };
        let mut read_meta = false;

        let err = build_all(&geo, &["poi"], &out, &opts, || {
            read_meta = true;
            Ok(serde_json::json!({}))
        }).err();

        assert!(err.as_deref().is_some_and(|e| e.contains("another build")), "{err:?}");
        assert!(!read_meta);
        assert_eq!(std::fs::read_to_string(&staged).ok().as_deref(), Some("staged by the other build"));
        assert_eq!(entries(&out), before);
        drop(other);
        build_all(&geo, &["places"], &out, &opts, || Ok(serde_json::json!({}))).unwrap();
        assert!(!out.join(BUILD_STAGE_DIR).exists());
        assert!(!opts.tmp.exists());
    }

    #[test]
    fn partial_build_retains_extra_name_requirements() {
        let dir = std::env::temp_dir().join(format!("retained-names-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("places.fst"), []).unwrap();
        std::fs::write(dir.join("poi.fst"), []).unwrap();
        for old in [false, true] {
            std::fs::write(dir.join(META_FILE), format!("{{\"extras\":{old}}}")).unwrap();
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
        assert!(build(&geo, &["places"], &out, &opts).is_ok());
        assert!(build(&geo, &["poi"], &out, &opts).is_ok());

        for (file, n, sub) in [("imp_places.u8", 2, "places"), ("imp_polys.u8", 1, "places"), ("imp_poi.u8", 1, "poi")] {
            std::fs::write(imp.join(file), vec![0u8; n + 1]).unwrap();
            let err = build(&geo, &[sub], &out, &opts).unwrap_err();
            assert!(err.contains(file), "{err}");
            std::fs::write(imp.join(file), vec![0u8; n]).unwrap();
        }
    }
}
