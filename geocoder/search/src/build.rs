//! Index build from the geocoder's own files. Entities stream out of the mapped records, their
//! names become normalized keys, keys are sorted in bounded-memory runs (spilled to disk) and
//! merged into an fst (written streaming) with postings and the short-prefix table.
//!
//! Output per sub-index `<name>` (places / poi): `<name>.fst` keys -> the byte offset of the
//! key's list in `<name>.post` (every key's, in key order, so the offsets grow with the keys),
//! `<name>.post`, `<name>.pfx.fst` + `<name>.pfx` (prefix table), and for places `places.bnd`
//! (boundary slots, `Boundaries`). Nothing else is stored: names, points and types stay in the
//! geocoder files, a labelled boundary's point too (its label node's).
mod chains;
mod counts;
mod fst_writer;
pub(crate) mod sources;
mod terms;
mod write;
pub use write::build_core;
pub(crate) use write::build_core_held;

use crate::constants::*;
use crate::dataset::SearchFiles;
use crate::entities::*;
use crate::extras::{record_ref, Extras, KIND_PLACE_NODE, KIND_POI};
use crate::model::*;
use crate::sort::{run_budget, ExternalSort, MapVec, SortRecord, SortedRuns};
use crate::text::normalize::{join, tokens, transliterate};
use memmap2::Mmap;
use query_server::{Index, PlaceNode, NO_DATA};
use serde_json::json;
use std::collections::{BinaryHeap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct BuildOpts {
    pub threads: usize,
    pub mem_mb: usize,
    pub tmp: PathBuf,
    pub extras: Option<String>,
    pub name_bundles: Option<Arc<SearchFiles>>,
    /// Directory with imp_places.u8 / imp_polys.u8 / imp_poi.u8 (the shipped importance bytes, one
    /// per record); a file left out leaves its records without them.
    pub imp_dir: Option<String>,
    pub area_proxy: bool,
    /// POIs of tiers above this are left out (3 = all).
    pub poi_max_tier: u8,
}

impl BuildOpts {
    /// The index as it ships: one thread, the default sort budget, every POI tier, nothing beyond
    /// the geocoder files.
    pub fn new(tmp: PathBuf) -> Self {
        BuildOpts {
            threads: 1,
            mem_mb: DEFAULT_RUN_BUDGET_MB,
            tmp,
            extras: None,
            name_bundles: None,
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
    bnd: Boundaries,
    n_slots: u32,
    n_linked: u32,
    /// The most bytes the label points' scan line crossings held.
    label_crossings_held: usize,
    /// label nodes that name a boundary slot (indexed through the boundary, not on their own)
    label_used: Bits,
    imp_places: Option<Mmap>,
    imp_polys: Option<Mmap>,
    extras: Option<&'a Extras>,
    name_bundles: Option<&'a SearchFiles>,
    area_proxy: bool,
}

impl<'a> PlacesSource<'a> {
    /// Derives the boundary slots and writes them to `bnd_path` (`Boundaries`): the labelled ones
    /// in one pass over the polygons, then the unlabelled ones and their label points in another.
    pub fn new(geo: &'a Index, opts: &'a BuildOpts, extras: Option<&'a Extras>, bnd_path: &Path) -> std::io::Result<Self> {
        let n_place_nodes = geo.place_node_count();
        let n_poly = geo.admin_polygon_count();
        // A hidden node that names its claiming boundary is that boundary's label point.
        let mut link_buf = MapVec::with_capacity(n_poly as usize)?;
        for _ in 0..n_poly {
            link_buf.push(NO_DATA);
        }
        let link = link_buf.as_mut_slice();
        geo.for_each_place_node(0, n_place_nodes, |i, pn| {
            if !pn.is_visible() && pn._pad1 & PLACE_FLAG_LABEL_OF != 0 && pn.parent_poly_id < n_poly {
                let l = &mut link[pn.parent_poly_id as usize];
                if *l == NO_DATA {
                    *l = i;
                }
            }
        });
        let mut label_used = Bits::new(n_place_nodes as usize);
        let mut w = BufWriter::with_capacity(WRITE_BUFFER, File::create(bnd_path)?);
        let mut n_linked = 0u32;
        for p in 0..n_poly {
            let node = link[p as usize];
            if node == NO_DATA || polygon_raw(geo, p).is_none_or(|ap| ap.name_id == NO_DATA) {
                continue;
            }
            label_used.set(node);
            BoundarySlot { poly: p, node, lat: 0.0, lng: 0.0 }.write(&mut w)?;
            n_linked += 1;
        }
        let mut n_slots = n_linked;
        // The scan line crossings of each label point found, held for the next one.
        let mut crossings = Vec::new();
        for p in 0..n_poly {
            if link[p as usize] != NO_DATA {
                continue;
            }
            let Some(ap) = polygon_raw(geo, p) else { continue };
            let searchable = ap.name_id != NO_DATA && (BOUNDARY_MIN_LEVEL..=BOUNDARY_MAX_LEVEL).contains(&ap.admin_level) && ap.area > 0.0;
            if !searchable {
                continue;
            }
            // The builder's interior_point (ST_PointOnSurface, Nominatim's centre of an area): an
            // area centroid can fall outside a concave ring or in a hole.
            let Some((lat, lng)) = geo.admin_geometry.point_on_surface_with(&ap, &mut crossings) else { continue };
            BoundarySlot { poly: p, node: NO_DATA, lat, lng }.write(&mut w)?;
            n_slots += 1;
        }
        w.write_all(&n_linked.to_le_bytes())?;
        w.into_inner().map_err(|e| e.into_error())?;
        drop(link_buf);
        let map = unsafe { Mmap::map(&File::open(bnd_path)?) }?;
        let bnd = Boundaries::open(map).map_err(|e| std::io::Error::other(format!("{}: {e}", bnd_path.display())))?;
        let dir = opts.imp_dir.as_deref();
        Ok(PlacesSource {
            geo,
            n_place_nodes,
            bnd,
            n_slots,
            n_linked,
            label_crossings_held: crossings.capacity() * std::mem::size_of::<f64>(),
            label_used,
            imp_places: map_importance(dir, "imp_places.u8", n_place_nodes)?,
            imp_polys: map_importance(dir, "imp_polys.u8", n_poly)?,
            extras,
            name_bundles: opts.name_bundles.as_deref(),
            area_proxy: opts.area_proxy,
        })
    }

    fn len(&self) -> u32 {
        self.n_slots + self.n_place_nodes
    }

    /// The entity of boundary slot `pos`; false when it has no name.
    fn fill_slot(&self, pos: usize, e: &mut EntityBuf) -> bool {
        e.names.clear();
        let geo = self.geo;
        let (rrefs, base_id, polygon) = {
            let Some((s, ap)) = self.bnd.at(geo, pos).and_then(|s| Some((s, polygon_raw(geo, s.poly)?))) else { return false };
            let node = s.linked().then(|| place_node_raw(geo, s.node)).flatten();
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
            (s.record_refs(), primary, s.poly)
        };
        let bundle = self.name_bundles.and_then(|files| files.bundle(Kind::Area, polygon));
        fill_names(geo, self.extras, &rrefs, base_id, bundle, e)
    }

    /// The entity of place node `i`, `pn`; false when it is not one on its own or has no name.
    fn fill_node(&self, i: u32, pn: &PlaceNode, e: &mut EntityBuf) -> bool {
        e.names.clear();
        // A hidden node is searchable on its own only when no boundary of this set claims it.
        if pn.name_id == NO_DATA || self.label_used.get(i) {
            return false;
        }
        let kind = place_node_kind(pn.place_type);
        let typ = PLACE_NODE_TYPES.get(pn.place_type as usize).copied().unwrap_or("place");
        e.id = i;
        e.imp = W_TYPE * type_base(kind, typ, 0) + extra_importance(byte_at(&self.imp_places, i));
        e.cc_poly = pn.parent_poly_id;
        let bundle = self.name_bundles.and_then(|files| files.bundle(kind, i));
        fill_names(self.geo, self.extras, &[record_ref(KIND_PLACE_NODE, i)], pn.name_id, bundle, e)
    }

    /// Calls `f` with the entity of each position of [lo, hi) that is one: the boundary slots in
    /// polygon order, then the place nodes. The nodes sit in cell order, so their names lie
    /// anywhere in the string tier: they are read NAME_BATCH_NODES at a time, in id order, and
    /// each batch is filled in name order, which reads the tier front to back once a batch
    /// instead of a page here and there for every node.
    fn for_each_entity<E>(&self, lo: u32, hi: u32, e: &mut EntityBuf, f: &mut impl FnMut(&EntityBuf) -> Result<(), E>) -> Result<(), E> {
        self.for_each_entity_in(lo, hi, NAME_BATCH_NODES, e, f)
    }

    /// `for_each_entity` with the nodes read `batch_nodes` at a time.
    fn for_each_entity_in<E>(
        &self, lo: u32, hi: u32, batch_nodes: u32, e: &mut EntityBuf, f: &mut impl FnMut(&EntityBuf) -> Result<(), E>,
    ) -> Result<(), E> {
        // As their polygons' records lie: the two tables of slots in one pass.
        for pos in self.bnd.by_polygon().take(hi.min(self.n_slots) as usize).skip(lo as usize) {
            if self.fill_slot(pos, e) {
                f(e)?;
            }
        }
        let (mut at, end) = (lo.max(self.n_slots) - self.n_slots, hi.saturating_sub(self.n_slots));
        let mut batch: Vec<(u32, PlaceNode)> = Vec::new();
        while at < end {
            let to = end.min(at.saturating_add(batch_nodes.max(1)));
            batch.clear();
            self.geo.for_each_place_node(at, to, |i, pn| batch.push((i, pn)));
            batch.sort_unstable_by_key(|&(i, pn)| (pn.name_id, i));
            for (i, pn) in &batch {
                if self.fill_node(*i, pn, e) {
                    f(e)?;
                }
            }
            at = to;
        }
        Ok(())
    }
}

pub struct PoiSource<'a> {
    geo: &'a Index,
    n: u32,
    max_tier: u8,
    imp_poi: Option<Mmap>,
    extras: Option<&'a Extras>,
    name_bundles: Option<&'a SearchFiles>,
}

impl<'a> PoiSource<'a> {
    pub fn new(geo: &'a Index, opts: &'a BuildOpts, extras: Option<&'a Extras>) -> std::io::Result<Self> {
        let n = geo.poi_records.as_ref().expect("poi_records").len();
        let imp_poi = map_importance(opts.imp_dir.as_deref(), "imp_poi.u8", n)?;
        Ok(PoiSource { geo, n, max_tier: opts.poi_max_tier, imp_poi, extras, name_bundles: opts.name_bundles.as_deref() })
    }

    /// The entity of POI record `pos` (below `n`); false when it is not one (a tombstone, a
    /// secondary ring, a tier left out) or has no name, an error for a row that does not read,
    /// which would leave the POI out of every index built over the file.
    fn fill(&self, pos: u32, e: &mut EntityBuf) -> Result<bool, String> {
        e.names.clear();
        let r = poi_raw(self.geo, pos)
            .ok_or_else(|| format!("poi_records.bin: row {pos} names a combo or a postcode its dictionaries do not hold"))?;
        if r.name_id == NO_DATA || r.flags & POI_FLAG_SECONDARY_RING != 0 || r.tier > self.max_tier {
            return Ok(false);
        }
        e.id = pos;
        e.imp = poi_importance(r.importance, byte_at(&self.imp_poi, pos));
        e.cc_poly = r.parent_poly_id;
        let bundle = self.name_bundles.and_then(|files| files.bundle(Kind::Poi, pos));
        Ok(fill_names(self.geo, self.extras, &[record_ref(KIND_POI, pos)], r.name_id, bundle, e))
    }
}

/// An entity's names: its base name, then the extra names of its records (`rrefs`, in the order
/// `Entities::record_refs` gives them, which numbers the ordinals).
fn fill_names(
    geo: &Index, extras: Option<&Extras>, rrefs: &[u32], base_id: u32,
    bundle: Option<crate::tags::TagBundle<'_>>, e: &mut EntityBuf,
) -> bool {
    let base = geo.get_string(base_id);
    if !base.is_empty() {
        e.names.insert(0, (0, 0, true, base.to_string()));
    }
    if let Some(x) = extras {
        for (k, n) in x.names_of(rrefs).into_iter().enumerate() {
            let ord = if k < MAX_EXTRA_ORD as usize { k as u8 + 1 } else { MAX_NAME_ORD };
            e.names.push((n.class, ord, n.key == "name", n.name.to_string()));
        }
    } else if let Some(bundle) = bundle {
        for (key, value) in bundle.iter() {
            for name in value.split(';').map(str::trim).filter(|&name| !name.is_empty() && name != base) {
                // These aliases match the query while the result keeps its stored name.
                e.names.push((class_of(key), MAX_NAME_ORD, key == "name", name.to_string()));
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
    /// Calls `f` with the entity of each position of [lo, hi) that is one, each once; an error
    /// of `f`'s, or for a record that does not read (`PoiSource::fill`).
    fn for_each_entity(&self, lo: u32, hi: u32, e: &mut EntityBuf, f: &mut impl FnMut(&EntityBuf) -> Result<(), String>) -> Result<(), String> {
        match self {
            Source::Places(s) => s.for_each_entity(lo, hi, e, f),
            Source::Poi(s) => {
                for pos in lo..hi {
                    if s.fill(pos, e)? {
                        f(e)?;
                    }
                }
                Ok(())
            }
        }
    }
}

/// Keys of one entity: each name whole (best class first), then its trailing words
/// ("york" of "new york"); a key belongs to the entity's best claim on it.
fn keys_of(geo: &Index, e: &EntityBuf, out: &mut Vec<(String, u8, bool, u8)>) {
    out.clear();
    let mut cc: Option<String> = None;
    let mut by_class: Vec<(u8, u8, Vec<crate::text::normalize::Token>)> = Vec::with_capacity(e.names.len() + 1);
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

/// A key's posting in a sort run: u32 entity, importance, partial | class << 1, ordinal. Postings
/// of a key sort by entity, then by the rest of them.
impl SortRecord for Posting {
    const BYTES: usize = 7;

    fn put(&self, out: &mut [u8]) {
        out[..4].copy_from_slice(&self.entity.to_le_bytes());
        out[4..].copy_from_slice(&[self.imp_q, self.partial as u8 | self.class << 1, self.ord]);
    }

    fn get(b: &[u8]) -> Self {
        Posting { entity: u32::from_le_bytes(b[..4].try_into().unwrap()), imp_q: b[4], partial: b[5] & 1 == 1, class: b[5] >> 1, ord: b[6] }
    }

    fn order(&self, o: &Self) -> std::cmp::Ordering {
        (self.entity, self.partial, self.class, self.ord, self.imp_q).cmp(&(o.entity, o.partial, o.class, o.ord, o.imp_q))
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
    slots: Vec<PrefixSlot>,
    keys: u64,
    postings: u64,
    longest: usize,
}

impl Writer {
    /// Writes one key with its kept postings (`Group::take`): its list at the end of the postings
    /// file, and that offset as its value.
    fn add(&mut self, key: &[u8], group: &[Posting]) -> Result<(), String> {
        self.scratch.clear();
        encode_list(&mut self.scratch, group);
        self.post.write_all(&self.scratch).map_err(|e| e.to_string())?;
        self.fst.insert(key, self.post_len).map_err(|e| e.to_string())?;
        self.post_len += self.scratch.len() as u64;
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

/// One sub-index between its two phases: its sorted runs on disk, removed with it unless a
/// merge takes them (a failed build leaves none of a sub-index it never merged).
pub struct KeyRuns {
    name: String,
    runs: SortedRuns<Posting>,
    st: Stats,
    ents: u64,
    entries: u64,
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
        Source::Poi(PoiSource::new(geo, opts, extras.as_ref()).map_err(|e| e.to_string())?)
    } else {
        let s = PlacesSource::new(geo, opts, extras.as_ref(), &out.join(BOUNDARY_FILE)).map_err(|e| e.to_string())?;
        st.add("place nodes", s.n_place_nodes);
        st.add("boundary slots", s.n_slots);
        st.add("boundary slots linked", s.n_linked);
        st.add("label point crossings held, bytes", s.label_crossings_held);
        Source::Places(s)
    };
    let t_src = t0.elapsed();

    // ---- keys -> sorted runs, one worker per contiguous range of entities
    let n = src.len();
    type RunsOut = (SortedRuns<Posting>, u64, u64);
    let results: Vec<Result<RunsOut, String>> = std::thread::scope(|sc| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let src = &src;
                let tmp = &opts.tmp;
                let tag = format!("{name}-{t}");
                sc.spawn(move || -> Result<RunsOut, String> {
                    let lo = (n as u64 * t as u64 / threads as u64) as u32;
                    let hi = (n as u64 * (t as u64 + 1) / threads as u64) as u32;
                    let mut sort = ExternalSort::new(budget, tmp, tag).map_err(|x| x.to_string())?;
                    let mut e = EntityBuf::default();
                    let mut keys = Vec::new();
                    let (mut ents, mut entries) = (0u64, 0u64);
                    // The runs sort by key and then entity, so the order the entities come in changes
                    // no index byte.
                    src.for_each_entity(lo, hi, &mut e, &mut |e: &EntityBuf| -> Result<(), String> {
                        keys_of(geo, e, &mut keys);
                        if keys.is_empty() {
                            return Ok(());
                        }
                        ents += 1;
                        let imp_q = quantize(e.imp);
                        for (k, ord, partial, class) in &keys {
                            sort.push(k.as_bytes(), Posting { entity: e.id, imp_q, partial: *partial, class: *class, ord: *ord })
                                .map_err(|x| format!("entity {}: {x}", e.id))?;
                            entries += 1;
                        }
                        Ok(())
                    })?;
                    // Hands the run memory back before the merge.
                    let runs = sort.finish().map_err(|x| x.to_string())?;
                    Ok((runs, ents, entries))
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    // Every worker's runs are held until the merge takes them, a failed worker's siblings' too.
    let mut runs = SortedRuns::default();
    let (mut ents, mut entries) = (0u64, 0u64);
    for r in results {
        let (worker_runs, e, k) = r?;
        ents += e;
        entries += k;
        runs.append(worker_runs);
    }
    drop(src);
    let t_keys = t0.elapsed() - t_src;
    Ok(KeyRuns { name: name.to_string(), runs, st, ents, entries, t_src, t_keys })
}

/// Merge phase of one sub-index: its runs -> `<name>.fst`, `.post`, `.pfx.fst` and `.pfx` in `out`.
pub fn merge(runs: KeyRuns, out: &Path, opts: &BuildOpts) -> Result<Stats, String> {
    let t0 = Instant::now();
    let KeyRuns { name, runs, mut st, ents, entries, t_src, t_keys } = runs;
    let name = name.as_str();
    let (n_runs, spilled) = (runs.len(), runs.spilled_bytes());
    let mut m = runs.merge(&opts.tmp, name).map_err(|e| e.to_string())?;
    let rewritten = m.rewritten_bytes();
    let [keys_path, post_path, pfx_keys_path, pfx_path] = sub_index_files(name).map(|f| out.join(f));
    let mk = |path: &Path| File::create(path).map_err(|e| e.to_string());
    let mut w = Writer {
        fst: fst::MapBuilder::new(BufWriter::with_capacity(WRITE_BUFFER, mk(&keys_path)?)).map_err(|e| e.to_string())?,
        post: BufWriter::with_capacity(WRITE_BUFFER, mk(&post_path)?),
        post_len: 0,
        scratch: Vec::new(),
        slots: (1..=PREFIX_TABLE_MAX_CHARS)
            .map(|chars| -> Result<PrefixSlot, String> {
                let path = opts.tmp.join(format!("{name}-pfx{chars}.tmp"));
                let out = BufWriter::new(File::create(&path).map_err(|e| e.to_string())?);
                Ok(PrefixSlot { prefix: String::new(), chars, keys: 0, hits: Vec::new(), out, path, flushed: 0 })
            })
            .collect::<Result<Vec<_>, String>>()?,
        keys: 0,
        postings: 0,
        longest: 0,
    };
    let mut cur: Vec<u8> = Vec::new();
    let mut key: Vec<u8> = Vec::new();
    let mut group = Group::default();
    while let Some(p) = m.next(&mut key).map_err(|e| e.to_string())? {
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
    let Writer { fst, mut post, mut slots, keys, postings, longest, .. } = w;
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
    let n_place_nodes = geo.place_node_count();
    [
        ("n_place_nodes", json!(n_place_nodes)),
        (META_FORMAT_KEY, json!(PLACES_INDEX_FORMAT)),
        ("extras", json!(opts.extras.is_some())),
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

/// Whether the index whose META_FILE is `meta` has this build's layout (PLACES_INDEX_FORMAT): one of
/// another, or without a format, is no index `SearchIndex::open` reads.
pub fn is_current_format(meta: &serde_json::Value) -> bool {
    meta[META_FORMAT_KEY].as_u64() == Some(PLACES_INDEX_FORMAT)
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
                let _ = std::fs::remove_dir(&aside);
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
    for &name in PLACES_SUB_INDEXES {
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
    use crate::fixture::Rng;

    fn posting(entity: u32, imp_q: u8, partial: bool, class: u8) -> Posting {
        Posting { entity, imp_q, partial, class, ord: (entity % 16) as u8 }
    }

    fn tmp_dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("place-search-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
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

    fn run_bytes(p: &Posting) -> [u8; Posting::BYTES] {
        let mut b = [0; Posting::BYTES];
        p.put(&mut b);
        b
    }

    #[test]
    fn a_posting_reads_back_from_its_run_bytes() {
        let last = Posting { entity: u32::MAX, imp_q: u8::MAX, partial: true, class: 5, ord: MAX_NAME_ORD };
        for p in [posting(0, 0, false, 0), posting(7, 9, true, 3), last] {
            assert_eq!(Posting::get(&run_bytes(&p)), p);
        }
    }

    #[test]
    fn postings_sort_by_entity_and_tell_apart_any_two_written_differently() {
        let mut postings = Vec::new();
        for entity in [3, 4] {
            for (imp_q, partial, class, ord) in [(0, false, 0, 0), (9, false, 0, 0), (0, true, 0, 0), (0, false, 5, 0), (0, false, 0, 2)] {
                postings.push(Posting { entity, imp_q, partial, class, ord });
            }
        }
        for a in &postings {
            for b in &postings {
                assert_eq!(a.order(b) == std::cmp::Ordering::Equal, run_bytes(a) == run_bytes(b), "{a:?} {b:?}");
                if a.entity != b.entity {
                    assert_eq!(a.order(b), a.entity.cmp(&b.entity), "{a:?} {b:?}");
                }
            }
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
        let mut sort = ExternalSort::new(DEFAULT_RUN_BUDGET_MB << 20, &dir, "k".into()).unwrap();
        for entity in 0..n {
            sort.push(b"church", Posting { entity, imp_q: 100, partial: false, class: 0, ord: 0 }).unwrap();
        }
        let runs = KeyRuns {
            name: "poi".into(),
            runs: sort.finish().unwrap(),
            st: Stats::default(),
            ents: 0,
            entries: 0,
            t_src: Duration::ZERO,
            t_keys: Duration::ZERO,
        };
        let opts = BuildOpts {
            threads: 1,
            mem_mb: DEFAULT_RUN_BUDGET_MB,
            tmp: dir.clone(),
            extras: None, name_bundles: None,
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
            threads: 1, mem_mb: DEFAULT_RUN_BUDGET_MB, tmp: dir.0.join("tmp"),
            extras: Some(bin.to_str().unwrap().into()), imp_dir: None, area_proxy: false, poi_max_tier: 3, name_bundles: None,
        };

        let err = build(&geo, &["places"], &dir.0.join("idx"), &opts).err();

        assert!(err.as_deref().is_some_and(|e| e.contains("65536 bytes")), "{err:?}");
    }

    #[test]
    fn a_build_fails_on_a_poi_row_that_does_not_read_and_leaves_no_sort_runs() {
        use crate::fixture::{Geo, TempDir};
        use query_server::{DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL};
        // 27 POIs of one 50,000-byte key each. A sort budget of 1 MiB holds 12 such keys a run on
        // one thread and 4 on each of three, so every worker spills runs, and the one that reaches
        // row 14 (on three threads the second, of rows 9 to 17) has spilled one before it does.
        let (n, bad_row) = (27, 14);
        let mut g = Geo::default();
        g.place("Testburg", 1, 10.1, 20.1);
        let name = "a".repeat(50_000);
        for k in 0..n {
            g.poi(&name, 10.0 + k as f32 * 0.01, 20.0, 0);
        }
        let dir = TempDir::new("poi-row");
        let set = dir.0.join("geo");
        g.write(&set);
        let records = std::fs::read(set.join("poi_records.bin")).unwrap();
        let load = || Index::load(set.to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).unwrap();
        let opts = |threads| BuildOpts { threads, mem_mb: 1, ..BuildOpts::new(dir.0.join("tmp")) };
        let left = |tmp: &Path| std::fs::read_dir(tmp).into_iter().flatten().map(|e| e.unwrap().file_name()).collect::<Vec<_>>();
        for threads in [1, 3] {
            let stats = build(&load(), &["poi"], &dir.0.join("idx"), &opts(threads)).unwrap();
            let runs = stats[0].1.lines.iter().find(|(k, _)| k == "sort runs").map(|(_, v)| v.clone());
            assert_eq!(runs, Some((3 * threads).to_string()), "{threads} threads");
        }

        // Row 14 (27 bytes a row) naming a combo, then a postcode, past its dictionary.
        for (case, at) in [("a combo", 25), ("a postcode", 23)] {
            let at = bad_row * 27 + at;
            let mut bad = records.clone();
            bad[at..at + 2].copy_from_slice(&0xFFFEu16.to_le_bytes());
            std::fs::write(set.join("poi_records.bin"), bad).unwrap();
            let geo = load();
            assert!(geo.poi_records.as_ref().unwrap().record(bad_row as u32).is_none(), "{case}");

            for threads in [1, 3] {
                let opts = opts(threads);

                let err = build(&geo, &["poi"], &dir.0.join("idx"), &opts).err();

                assert!(err.as_deref().is_some_and(|e| e.contains("poi_records.bin") && e.contains("row 14")), "{case}, {threads} threads: {err:?}");
                let left = left(&opts.tmp);
                assert!(left.is_empty(), "{case}, {threads} threads: left in the temp directory: {left:?}");
            }
        }
    }

    #[test]
    fn place_entities_come_once_each_whatever_the_batch_and_range() {
        use crate::fixture::{Geo, TempDir};
        let mut g = Geo::default();
        // A labelled boundary between two unlabelled ones.
        g.boundary("Firstshire", 6, [1.0, 1.0, 2.0, 2.0]);
        let land = g.boundary("Testland", 2, [0.0, 0.0, 60.0, 60.0]);
        g.boundary("Lastshire", 6, [3.0, 3.0, 4.0, 4.0]);
        // Named in this order (their name ids ascend), at points whose cells order them otherwise.
        let names = ["Zulu", "Alpha", "Mike", "Bravo", "Yankee", "Charlie", "Xray", "Delta", "Whiskey"];
        for (k, name) in names.iter().enumerate() {
            g.place(name, 2, 5.0 + 6.0 * k as f32, 50.0 - 5.0 * k as f32);
        }
        let capital = g.place("Capital", 0, 30.0, 30.0);
        g.claim(land, capital);
        let dir = TempDir::new("entity-batches");
        let geo = g.load(&dir.0.join("geo"));
        let opts = BuildOpts::new(dir.0.join("tmp"));
        let src = PlacesSource::new(&geo, &opts, None, &dir.0.join("places.bnd")).unwrap();
        let entities = |lo: u32, hi: u32, batch: u32| {
            let (mut e, mut out) = (EntityBuf::default(), Vec::new());
            let mut take = |e: &EntityBuf| -> Result<(), ()> {
                out.push((e.id, e.names[0].3.clone()));
                Ok(())
            };
            src.for_each_entity_in(lo, hi, batch, &mut e, &mut take).unwrap();
            out
        };
        let n = src.len();

        let mut all = entities(0, n, NAME_BATCH_NODES);

        // The boundaries in polygon order, the labelled one under its label's name (the claimed
        // capital is no entity of its own), then the nine nodes of one batch in name order, which
        // is not their id order.
        let boundaries = ["Firstshire", "Capital", "Lastshire"];
        assert_eq!(all.iter().map(|(_, name)| name.as_str()).collect::<Vec<_>>(), [&boundaries[..], &names[..]].concat());
        assert!(all[3..].windows(2).any(|w| w[0].0 > w[1].0), "the fixture's nodes sit in name order");
        // Every batch size and every cut of the range gives each entity once.
        all.sort();
        for batch in [1, 2, 3, 4, 100] {
            for cut in 0..=n {
                let mut got = entities(0, cut, batch);
                got.extend(entities(cut, n, batch));
                got.sort();
                assert_eq!(got, all, "batch {batch}, cut {cut}");
            }
        }
    }

    fn writer(dir: &Path) -> Writer {
        std::fs::create_dir_all(dir).unwrap();
        let mk = |n: &str| BufWriter::new(File::create(dir.join(n)).unwrap());
        Writer {
            fst: fst::MapBuilder::new(mk("fst")).unwrap(),
            post: mk("post"),
            post_len: 0,
            scratch: Vec::new(),
            slots: (1..=PREFIX_TABLE_MAX_CHARS)
                .map(|chars| PrefixSlot { prefix: String::new(), chars, keys: 0, hits: Vec::new(), out: mk(&format!("pfx{chars}")), path: dir.join(format!("pfx{chars}")), flushed: 0 })
                .collect(),
            keys: 0,
            postings: 0,
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
    fn a_build_is_the_same_bytes_whatever_its_thread_count() {
        use crate::fixture::{Geo, TempDir};
        let mut g = Geo::default();
        let land = g.boundary("Testland", 2, [0.0, 0.0, 60.0, 60.0]);
        g.boundary("Testshire", 6, [10.0, 10.0, 20.0, 20.0]);
        let capital = g.place("Testville", 0, 30.0, 30.0);
        g.claim(land, capital);
        for k in 0..60u32 {
            g.place(&format!("Place {} {}", k % 7, ["North", "South", "Old"][k as usize % 3]), (k % 7) as u8, 1.0 + k as f32 * 0.9, 2.0);
            g.poi(&format!("Hall {}", k % 11), 1.0 + k as f32 * 0.9, 3.0, (k * 37 % 256) as u8);
        }
        let dir = TempDir::new("threads");
        let geo = g.load(&dir.0.join("geo"));
        let files = |threads: usize| {
            let out = dir.0.join(format!("idx-{threads}"));
            let opts = BuildOpts { threads, ..BuildOpts::new(dir.0.join(format!("tmp-{threads}"))) };
            build_all(&geo, &held_subs(&geo, PLACES_SUB_INDEXES), &out, &opts, || Ok(serde_json::json!({}))).unwrap();
            let mut v: Vec<(String, Vec<u8>)> = std::fs::read_dir(&out)
                .unwrap()
                .map(|e| e.unwrap().path())
                .filter(|p| p.file_name().unwrap() != META_FILE)
                .map(|p| (p.file_name().unwrap().to_string_lossy().into_owned(), std::fs::read(&p).unwrap()))
                .collect();
            v.sort();
            v
        };

        let one = files(1);

        assert_eq!(one.len(), 9);
        for threads in [2, 3, 7] {
            assert_eq!(files(threads), one, "{threads} threads");
        }
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
    fn a_boundary_named_by_its_node_is_found_by_its_own_name_too() {
        use crate::fixture::{build_search, Geo, TempDir};
        use crate::query::Searcher;
        use crate::reader::SearchIndex;
        // Sosnovka's boundary goes by its town node's English name and keeps its own name,
        // Сосновка (admin_own_names.bin); Kirsanovka's too, Кирсановка, which no node labels.
        // Each is found by either name, shown by the name the search matched. A city named Mill
        // in neither outranks the mills in each, so only a context the boundary's name matches
        // places a mill in it.
        let mut g = Geo::default();
        let sosnovka = g.boundary("Sosnovka", 8, [10.0, 20.0, 10.2, 20.2]);
        g.own_name(sosnovka, "Сосновка");
        let kirsanovka = g.boundary("Kirsanovka", 8, [11.0, 21.0, 11.2, 21.2]);
        g.own_name(kirsanovka, "Кирсановка");
        let node = g.place("Sosnovka", 1, 10.1, 20.1);
        g.claim(sosnovka, node);
        g.place("Mill", 2, 10.05, 20.05);
        g.place("Mill", 1, 11.1, 21.1);
        g.place("Mill", 0, 12.1, 22.1);
        let dir = TempDir::new("own-names");
        let geo = g.load(&dir.0.join("geo"));
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        let idx = SearchIndex::open(&geo, &out, None, None).unwrap();
        let s = Searcher::new(&idx);

        // The boundaries are entities n_place_nodes (4) + their polygons.
        for (q, entity) in [("Sosnovka", 4), ("Сосновка", 4), ("Kirsanovka", 5), ("Кирсановка", 5)] {
            let (found, _) = s.search(q, 5);

            assert!(!found.is_empty(), "{q}");
            assert_eq!((found[0].hit.entity, found[0].name.as_str()), (entity, q), "{q}");
        }
        // Either name places a mill in its boundary, before the city and the other boundary's
        // mill; without a context the city comes first.
        let (found, _) = s.search("Mill", 5);
        assert_eq!(found.first().map(|f| f.lat), Some(12.1));
        for (q, lat) in [("Mill, Sosnovka", 10.05), ("Mill, Сосновка", 10.05), ("Mill, Kirsanovka", 11.1), ("Mill, Кирсановка", 11.1)] {
            let (found, _) = s.search(q, 5);

            assert!(!found.is_empty(), "{q}");
            assert_eq!((found[0].name.as_str(), found[0].lat), ("Mill", lat), "{q}");
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
            assert!(geo.admin_geometry.contains_once(&polygon_raw(&geo, poly).unwrap(), label), "{q} at {} {}", f.lat, f.lng);
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
            threads: 1, mem_mb: DEFAULT_RUN_BUDGET_MB, tmp: dir.0.join("tmp"), extras: None, name_bundles: None,
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
            threads: 1, mem_mb: DEFAULT_RUN_BUDGET_MB, tmp: out.join("tmp"), extras: None, name_bundles: None,
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

        assert!(result.is_err());
        assert_eq!(contents(&out), before);
        assert!(!stage.join(BUILD_ASIDE_DIR).exists());
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
            threads: 1, mem_mb: DEFAULT_RUN_BUDGET_MB, tmp: dir.0.join("tmp"), extras: None, name_bundles: None,
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
            threads: 1, mem_mb: DEFAULT_RUN_BUDGET_MB, tmp: dir.0.join("tmp"), extras: None, name_bundles: None,
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
            threads: 1, mem_mb: DEFAULT_RUN_BUDGET_MB, tmp: dir.0.join("tmp"), extras: None, name_bundles: None,
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

    #[test]
    fn a_poi_below_rank_30_is_indexed_and_a_secondary_ring_is_not() {
        use crate::fixture::{build_search, Geo, TempDir};
        use crate::query::Searcher;
        use crate::reader::SearchIndex;
        use query_server::{POI_FLAG_BELOW_RANK30, POI_FLAG_WIKIDATA};
        // A wiki-tagged waterway=stream (GENERIC_RANK30, which its tag lifts into poi/all) is no
        // rank-30 row of placex, which the builder flags; the search still names it. A secondary
        // ring of a multi-ring POI is no POI of its own.
        let mut g = Geo::default();
        let brook = g.poi("Hartley Brook", 10.05, 20.05, 0);
        g.classify_poi(brook, 170, 3, POI_FLAG_WIKIDATA | POI_FLAG_BELOW_RANK30);
        let ring = g.poi("Quenby Mere", 10.1, 20.1, 0);
        g.classify_poi(ring, 0, 1, POI_FLAG_SECONDARY_RING);
        let dir = TempDir::new("below-rank-30");
        let geo = g.load(&dir.0.join("geo"));
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        let idx = SearchIndex::open(&geo, &out, None, None).unwrap();
        let s = Searcher::new(&idx);
        let found = |q: &str| s.search(q, 5).0.iter().map(|f| f.hit.entity).collect::<Vec<_>>();

        assert_eq!(found("Hartley Brook"), [brook | POI_BIT]);
        assert_eq!(found("Quenby Mere"), [0u32; 0]);
    }
}
