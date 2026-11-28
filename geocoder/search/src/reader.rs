//! Memory-mapped search index: the fst and postings of each sub-index (places, optional POI),
//! the boundary slots, and the geocoder index it points into. Loading reads only a small
//! meta file, each fst's header, footer and root node, the boundary file's footer and the ISO
//! code list; everything else is mapped and paged in by queries. No byte of an index file is
//! trusted: one no build writes is refused at open (by the file's name) where that takes only
//! what the open reads, and read as no key, no posting or no slot by the query that meets it
//! otherwise.
use crate::build::{has_extra_names, has_sub_index, is_current_format, sub_index_files};
use crate::constants::{BOUNDARY_FILE, PLACES_INDEX_FORMAT, META_FILE, META_FORMAT_KEY};
use crate::entities::{Boundaries, Entities};
use crate::extras::Extras;
use crate::keys::Keys;
use crate::model::*;
use crate::text::normalize::normalize;
use memmap2::Mmap;
use query_server::Index;
use std::collections::HashMap;
use std::path::Path;

fn map(path: &Path) -> Result<Mmap, String> {
    let f = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let m = unsafe { Mmap::map(&f) }.map_err(|e| format!("{}: {e}", path.display()))?;
    // Random access throughout (fst nodes, posting lists): no readahead, RSS stays what is touched.
    let _ = m.advise(memmap2::Advice::Random);
    Ok(m)
}

pub struct SubIndex {
    /// Each key's value is the byte offset of its list in `post`.
    pub map: Keys<Mmap>,
    pub post: Mmap,
    pub pfx_map: Keys<Mmap>,
    pub pfx: Mmap,
    /// Entity ids of this sub-index carry POI_BIT.
    pub poi: bool,
    /// Entities of the sub-index are below this (before POI_BIT): a posting at or past it ends
    /// its list.
    entities: u32,
}

impl SubIndex {
    /// The sub-index `name` in `dir`, whose entities are below `entities`; an error, naming the
    /// file, for an fst no build writes (`Keys::new`).
    pub fn open(dir: &Path, name: &str, entities: u32) -> Result<Self, String> {
        let [keys, post, pfx_keys, pfx] = sub_index_files(name).map(|f| dir.join(f));
        let fst = |path: &Path| Keys::new(map(path)?).map_err(|e| format!("{}: {e}", path.display()));
        Ok(SubIndex {
            map: fst(&keys)?,
            post: map(&post)?,
            pfx_map: fst(&pfx_keys)?,
            pfx: map(&pfx)?,
            poi: name == "poi",
            entities,
        })
    }

    /// `open`, and an error, naming the file, for keys (`<name>.fst`) whose bytes are not those
    /// its build wrote (`Keys::verify`, a read of the whole file): for a tool that walks every
    /// key, which a damaged file can make a walk of no end. The prefix table's fst is not
    /// verified: a tool that walks it verifies it as well. A query's open takes neither.
    pub fn open_verified(dir: &Path, name: &str, entities: u32) -> Result<Self, String> {
        let sub = Self::open(dir, name, entities)?;
        let [keys, ..] = sub_index_files(name).map(|f| dir.join(f));
        sub.map.verify().map_err(|e| format!("{}: {e}", keys.display()))?;
        Ok(sub)
    }

    fn tag(&self, mut p: Posting) -> Posting {
        if self.poi {
            p.entity |= POI_BIT;
        }
        p
    }

    /// The postings of the key whose value is `v`: its list, read up to a posting of no entity
    /// of the sub-index (`ListReader` for a damaged one), one at a time (a count no build writes
    /// takes no memory).
    pub fn list(&self, v: u64) -> impl Iterator<Item = Posting> + '_ {
        let off = usize::try_from(v).unwrap_or(usize::MAX);
        ListReader::new(&self.post, off).take_while(|p| p.entity < self.entities).map(|p| self.tag(p))
    }

    /// `list`, held in `out`.
    pub fn postings(&self, v: u64, out: &mut Vec<Posting>) {
        out.clear();
        out.extend(self.list(v));
    }

    /// Best importance under a key, from the head of its list (0 for a list without a posting).
    pub fn first_imp(&self, v: u64) -> u8 {
        usize::try_from(v).ok().and_then(|off| list_top_imp(&self.post, off)).unwrap_or(0)
    }

    /// The prefix-table hits of `phrase` (None without an entry), read up to a hit of no entity
    /// of the sub-index, as lists are (`decode_prefix_hits` for a damaged one).
    pub fn prefix_hits(&self, phrase: &str) -> Option<Vec<PrefixHit>> {
        Some(self.prefix_hits_at(self.pfx_map.get(phrase)?))
    }

    /// The hits of the prefix-table list at `off` (`prefix_hits`).
    fn prefix_hits_at(&self, off: u64) -> Vec<PrefixHit> {
        let mut out = Vec::new();
        if let Ok(off) = usize::try_from(off) {
            decode_prefix_hits(&self.pfx, off, &mut out);
        }
        if let Some(end) = out.iter().position(|h| h.entity >= self.entities) {
            out.truncate(end);
        }
        if self.poi {
            for h in out.iter_mut() {
                h.entity |= POI_BIT;
            }
        }
        out
    }
}

/// ISO codes of countries (by country code) and states (by country code + folded name), the
/// context aliases the records do not hold ("tx", "usa").
#[derive(Default)]
pub struct Iso {
    pub country: HashMap<[u8; 2], Vec<String>>,
    pub state: HashMap<([u8; 2], String), Vec<String>>,
}

impl Iso {
    /// The ISO code list at `path` (lines "<polygon>\t<code>;<code>...", those of no polygon of
    /// `geo` skipped); a file that cannot be read is an error.
    pub fn load(path: &Path, geo: &Index) -> Result<Iso, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let mut iso = Iso::default();
        for line in text.lines() {
            let Some((poly, codes)) = line.split_once('\t') else { continue };
            let Ok(poly) = poly.parse::<u32>() else { continue };
            let Some(ap) = crate::entities::polygon_raw(geo, poly) else { continue };
            let cc = crate::entities::cc_of(ap.country_code);
            let codes: Vec<String> = codes.split(';').map(|s| s.to_string()).collect();
            if ap.admin_level == 2 {
                iso.country.entry(cc).or_default().extend(codes);
            } else {
                iso.state.entry((cc, normalize(&geo.get_string(ap.name_id)))).or_default().extend(codes);
            }
        }
        Ok(iso)
    }
}

pub struct SearchIndex<'g> {
    pub geo: &'g Index,
    pub places: SubIndex,
    pub poi: Option<SubIndex>,
    pub ents: Entities<'g>,
    pub iso: Iso,
}

impl<'g> SearchIndex<'g> {
    /// `dir` holds the built index (places.*, optional poi.*, search_meta.json); `extras` the
    /// decompressed extra-names file the index was built with, `iso` the ISO code list. An error
    /// for an index of another format (PLACES_INDEX_FORMAT) or of another count of place nodes, and for
    /// a meta file, an fst or a boundary file no build writes.
    pub fn open(geo: &'g Index, dir: &Path, extras: Option<&str>, iso: Option<&str>) -> Result<Self, String> {
        let meta: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(dir.join(META_FILE)).map_err(|e| format!("{META_FILE}: {e}"))?,
        )
        .map_err(|e| format!("{META_FILE}: {e}"))?;
        if !is_current_format(&meta) {
            return Err(format!("{}: an index of format {}, not {PLACES_INDEX_FORMAT}: build it again", dir.display(), meta[META_FORMAT_KEY]));
        }
        let extras = match (has_extra_names(&meta), extras) {
            (true, Some(p)) => Some(Extras::open(p)?),
            (true, None) => return Err("index was built with extra names: pass the extra-names file".into()),
            _ => None,
        };
        let n_place_nodes = geo.place_node_count();
        if meta["n_place_nodes"].as_u64() != Some(n_place_nodes as u64) {
            return Err(format!("{}: an index of {} place nodes, not {n_place_nodes}", dir.display(), meta["n_place_nodes"]));
        }
        let poi = match &geo.poi_records {
            Some(records) if has_sub_index(dir, "poi") => Some(SubIndex::open(dir, "poi", records.len())?),
            _ => None,
        };
        let bnd = Boundaries::open(map(&dir.join(BOUNDARY_FILE))?).map_err(|e| format!("{}: {e}", dir.join(BOUNDARY_FILE).display()))?;
        Ok(SearchIndex {
            geo,
            places: SubIndex::open(dir, "places", n_place_nodes.saturating_add(geo.admin_polygon_count()))?,
            poi,
            ents: Entities { geo, n_place_nodes, bnd, extras },
            iso: match iso {
                Some(p) => Iso::load(Path::new(p), geo)?,
                None => Iso::default(),
            },
        })
    }

    pub fn subs(&self) -> impl Iterator<Item = (u8, &SubIndex)> {
        std::iter::once((0u8, &self.places)).chain(self.poi.iter().map(|s| (1u8, s)))
    }

    pub fn sub(&self, i: u8) -> &SubIndex {
        if i == 0 {
            &self.places
        } else {
            self.poi.as_ref().unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{build_search, Geo, TempDir};
    use crate::query::Searcher;

    #[test]
    fn opens_an_index_with_empty_data_files() {
        // Place nodes only, no boundary slot, and a POI sub-index without a named POI: poi.post
        // and poi.pfx are empty, places.bnd holds its footer alone.
        let mut g = Geo::default();
        g.place("Alpha", 1, 10.0, 20.0);
        g.place("Bravo", 2, 10.1, 20.1);
        g.poi("", 10.0, 20.0, 0);
        let dir = TempDir::new("empty-files");
        let geo = g.load(&dir.0.join("geo"));
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        for (f, len) in [("poi.post", 0), ("poi.pfx", 0), ("places.bnd", 4)] {
            assert_eq!(std::fs::metadata(out.join(f)).unwrap().len(), len, "{f}");
        }

        let idx = SearchIndex::open(&geo, &out, None, None).unwrap();

        assert!(idx.poi.is_some());
        let (found, _) = Searcher::new(&idx).search("alpha", 5);
        assert_eq!(found.iter().map(|f| (f.hit.entity, f.name.as_str())).collect::<Vec<_>>(), [(0, "Alpha")]);
    }

    /// Places (a labelled and an unlabelled boundary, nodes) and POIs, some names shared, so the
    /// index holds keys of one posting and keys of several.
    fn mixed() -> Geo {
        let mut g = Geo::default();
        g.boundary("Testland", 2, [0.0, 0.0, 60.0, 60.0]);
        let town = g.boundary("Testville", 8, [10.0, 20.0, 10.2, 20.2]);
        let node = g.place("Testville", 1, 10.1, 20.1);
        g.claim(town, node);
        for (k, name) in ["Alpha", "Bravo Hill", "Alpha Hill", "Charlie"].iter().enumerate() {
            g.place(name, 2, 30.0 + k as f32, 30.0);
        }
        for (k, name) in ["Old Mill", "Old Mill", "Test Hall", "Mill Pond", "Old Mill"].iter().enumerate() {
            g.poi(name, 40.0 + k as f32, 40.0, 10 * k as u8);
        }
        g
    }

    /// The index files of `mixed()` built into `<dir>/idx`, and the dataset.
    fn built(dir: &TempDir) -> (Index, std::path::PathBuf) {
        let geo = mixed().load(&dir.0.join("geo"));
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        (geo, out)
    }

    #[test]
    fn every_key_reads_its_list_where_the_list_before_it_ends() {
        use fst::Streamer;
        let dir = TempDir::new("layout");
        let (geo, out) = built(&dir);

        let idx = SearchIndex::open(&geo, &out, None, None).unwrap();

        for (i, sub) in idx.subs() {
            // Every key's list in the postings file in key order: each key's value is the offset
            // of its list, which ends where the next key's starts and the last at the file's end.
            let (mut at, mut keys, mut postings, mut list) = (0u64, 0, Vec::new(), Vec::new());
            let mut s = sub.map.stream();
            while let Some((key, v)) = s.next() {
                let key = String::from_utf8_lossy(key).into_owned();
                assert_eq!(v, at, "sub-index {i}: {key:?}");
                sub.postings(v, &mut postings);
                assert!(!postings.is_empty(), "sub-index {i}: {key:?}");
                assert_eq!(sub.first_imp(v), postings[0].imp_q, "sub-index {i}: {key:?}");
                let untagged: Vec<Posting> = postings.iter().map(|p| Posting { entity: p.entity & !POI_BIT, ..*p }).collect();
                list.clear();
                encode_list(&mut list, &untagged);
                at += list.len() as u64;
                keys += 1;
            }
            assert_eq!(at, sub.post.len() as u64, "sub-index {i}");
            assert!(keys > 3, "sub-index {i}: {keys} keys");
        }
        let names = |q: &str| Searcher::new(&idx).search(q, 5).0.into_iter().map(|f| f.name).collect::<Vec<_>>();
        assert_eq!(names("old mill"), ["Old Mill"; 3]);
        assert_eq!(names("test hall"), ["Test Hall"]);
    }

    #[test]
    fn a_labelled_boundary_takes_its_point_from_its_label_node() {
        let dir = TempDir::new("bnd");
        let (geo, out) = built(&dir);

        let bnd = std::fs::read(out.join(BOUNDARY_FILE)).unwrap();

        // The labelled slot (polygon 1, its node), then the unlabelled one (polygon 0, its point),
        // then how many slots are labelled.
        let u32_at = |at: usize| u32::from_le_bytes(bnd[at..at + 4].try_into().unwrap());
        assert_eq!(bnd.len(), 8 + 12 + 4);
        assert_eq!((u32_at(0), u32_at(8), u32_at(20)), (1, 0, 1));
        let node = geo.place_node_record(u32_at(4)).unwrap();
        assert_eq!((geo.get_string(node.name_id).as_ref(), node.lat, node.lng), ("Testville", 10.1, 20.1));
        let (lat, lng) = (f32::from_bits(u32_at(12)), f32::from_bits(u32_at(16)));
        assert!((0.0..60.0).contains(&lat) && (0.0..60.0).contains(&lng), "{lat} {lng}");
        let idx = SearchIndex::open(&geo, &out, None, None).unwrap();
        let found = Searcher::new(&idx).search("testville", 5).0;
        assert_eq!(found.iter().map(|f| (f.hit.entity, f.lat, f.lng)).collect::<Vec<_>>(), [(geo.place_node_count() + 1, 10.1, 20.1)]);
    }

    #[test]
    fn boundary_files_no_build_writes_are_refused_at_open() {
        let dir = TempDir::new("bnd-refused");
        let (geo, out) = built(&dir);
        let slot = [1u32.to_le_bytes(), 0u32.to_le_bytes()].concat();
        let cases: [(&str, Vec<u8>); 4] = [
            ("empty", vec![]),
            ("shorter than its labelled slots", [&slot[..], &2u32.to_le_bytes()].concat()),
            ("a cut unlabelled slot", [&slot[..], &[0; 11], &1u32.to_le_bytes()].concat()),
            ("2^32 - 1 labelled slots", [&slot[..], &u32::MAX.to_le_bytes()].concat()),
        ];
        for (case, bytes) in cases {
            std::fs::write(out.join(BOUNDARY_FILE), bytes).unwrap();

            let err = SearchIndex::open(&geo, &out, None, None).err();

            assert!(err.as_deref().is_some_and(|e| e.contains(BOUNDARY_FILE)), "{case}: {err:?}");
        }
    }

    #[test]
    fn damaged_boundary_slots_read_without_a_panic() {
        let dir = TempDir::new("bnd-damaged");
        let (geo, out) = built(&dir);
        let le = |words: &[u32]| words.iter().flat_map(|w| w.to_le_bytes()).collect::<Vec<u8>>();
        let (lat, lng) = (10.1f32.to_bits(), 20.1f32.to_bits());
        // Testville's node, the label of polygon 1 (the built file's labelled slot), and a node
        // that labels none.
        let bnd = std::fs::read(out.join(BOUNDARY_FILE)).unwrap();
        let label = u32::from_le_bytes(bnd[4..8].try_into().unwrap());
        let visible = (0..geo.place_node_count()).find(|&i| geo.place_node_record(i).unwrap().is_visible()).unwrap();
        let off = |lat: f32, lng: f32| le(&[1, label, 0, lat.to_bits(), lng.to_bits(), 1]);
        // Each case's bytes and the slots that are none.
        let cases: [(&str, Vec<u8>, &[usize]); 9] = [
            ("a label node past the place nodes", le(&[1, u32::MAX, 0, lat, lng, 1]), &[0]),
            ("a label node of another polygon", le(&[0, label, 1]), &[0]),
            ("a visible label node", le(&[1, visible, 1]), &[0]),
            ("no slot of the boundaries", le(&[0]), &[0]),
            ("slots out of order", le(&[5, label, 1, label, 2]), &[0]),
            ("a polygon past the boundaries", le(&[1, label, 7, lat, lng, 1]), &[1]),
            ("an infinite point", off(f32::INFINITY, 20.1), &[1]),
            ("a point of NaN", off(10.1, f32::NAN), &[1]),
            ("a point off the globe", off(95.0, 200.0), &[1]),
        ];
        let mut failed = Vec::new();
        for (case, bytes, none) in cases {
            std::fs::write(out.join(BOUNDARY_FILE), bytes).unwrap();
            let idx = SearchIndex::open(&geo, &out, None, None).unwrap();

            for &pos in none {
                if let Some(slot) = idx.ents.bnd.at(&geo, pos) {
                    failed.push(format!("{case}: slot {pos}: {slot:?}"));
                }
            }
            for q in ["testville", "testland", "test", "alpha", "testland, testland", "alpha, testland"] {
                let found = panic_of(|| Searcher::new(&idx).search(q, 5).0.iter().map(|f| (f.lat, f.lng)).collect::<Vec<_>>());
                match found {
                    Err(p) => failed.push(format!("{case}: {q}: {p}")),
                    Ok(points) if !points.iter().all(|&(lat, lng)| query_server::coords_in_range(lat as f64, lng as f64)) => {
                        failed.push(format!("{case}: {q}: {points:?}"))
                    }
                    Ok(_) => {}
                }
            }
        }
        assert!(failed.is_empty(), "{failed:#?}");
    }

    /// What `f` returns, else the message it panicked with.
    fn panic_of<T>(f: impl FnOnce() -> T) -> Result<T, String> {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).map_err(|p| {
            p.downcast_ref::<String>().cloned().or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string())).unwrap_or_default()
        })
    }

    /// Runs `f` on a thread of its own: an error when it panics or has not returned after `secs`
    /// (the thread is left running).
    fn ends_within(secs: u64, f: impl FnOnce() + Send + 'static) -> Result<(), String> {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(panic_of(f));
        });
        match rx.recv_timeout(std::time::Duration::from_secs(secs)) {
            Ok(done) => done,
            Err(_) => Err(format!("still running after {secs} s")),
        }
    }

    /// The fst map of `entries` (ascending keys) as the build writes one.
    fn fst_of(entries: &[(&str, u64)]) -> Vec<u8> {
        let mut b = fst::MapBuilder::memory();
        for (k, v) in entries {
            b.insert(k, *v).unwrap();
        }
        b.into_inner().unwrap()
    }

    /// fst's header and footer around `nodes` (written from address 16), its root at `root`.
    fn fst_around(nodes: &[u8], keys: u64, root: usize) -> Vec<u8> {
        [&3u64.to_le_bytes()[..], &0u64.to_le_bytes(), nodes, &keys.to_le_bytes(), &(root as u64).to_le_bytes(), &[0; 4]].concat()
    }

    /// An fst whose root has one transition, on 'a', back to the root itself: its delta, 8 bytes,
    /// counts down from the root's first byte past the root.
    fn a_loop() -> Vec<u8> {
        let (end, root) = (16u64, 26u64);
        let node = [&end.wrapping_sub(root).to_le_bytes()[..], &[0x80, b'a', 0b10_000000]].concat();
        fst_around(&node, 1, root as usize)
    }

    /// An fst of `k` nodes above a leaf that is not final, each with transitions on 'a' and 'b' to
    /// the node below it: 2^k paths, and no key.
    fn paths(k: usize) -> Vec<u8> {
        // The leaf: pack sizes 0, a transition count byte of 0, its state byte.
        let mut nodes = vec![0, 0, 0];
        for _ in 0..k {
            // Deltas of one byte (1: to the byte just below the node), the inputs (both in
            // reverse), pack sizes (transition addresses of 1 byte, no outputs), state: two.
            nodes.extend([1, 1, b'b', b'a', 0x10, 2]);
        }
        let root = 16 + nodes.len() - 1;
        fst_around(&nodes, 0, root)
    }

    #[test]
    fn damaged_key_files_read_without_a_panic() {
        let dir = TempDir::new("fst-damaged");
        let (geo, out) = built(&dir);
        let places = std::fs::read(out.join("places.fst")).unwrap();
        let with = |b: &[u8], at: usize, v: &[u8]| {
            let mut b = b.to_vec();
            b[at..at + v.len()].copy_from_slice(v);
            b
        };
        // One transition from the root, with an output: the root is [output][delta][pack sizes]
        // [state], 21 bytes from the end.
        let (one, two) = (fst_of(&[("alpha", 7)]), fst_of(&[("a", 1), ("ab", u64::MAX)]));
        let (r1, r2) = (one.len() - 21, two.len() - 21);
        for (b, r) in [(&one, r1), (&two, r2)] {
            assert_eq!((b[r] >> 6, b[r - 1]), (0b10, 0x11));
        }
        // Each case's bytes, whether its open may refuse them (for a root no build writes), and a
        // key it holds no value for.
        let cases = [
            ("a root past the end", with(&places, places.len() - 12, &(places.len() as u64).to_le_bytes()), true, None),
            ("a transition to before the file", with(&one, r1 - 2, &[0xFF]), false, Some("alpha")),
            ("transition addresses of 9 bytes", with(&one, r1 - 1, &[0x91]), true, None),
            ("outputs past 64 bits", with(&two, r2 - 3, &[0xFF]), false, Some("ab")),
            ("a transition to its own node", a_loop(), false, Some("a")),
        ];
        let mut failed = Vec::new();
        for (case, bytes, refusable, none) in cases {
            std::fs::write(out.join("places.fst"), bytes).unwrap();
            let found = panic_of(|| {
                let idx = match SearchIndex::open(&geo, &out, None, None) {
                    Ok(idx) => idx,
                    Err(_) if refusable => return Ok(()),
                    Err(e) => return Err(format!("refused at open: {e}")),
                };
                if let Some(v) = none.and_then(|k| idx.places.map.get(k)) {
                    return Err(format!("{none:?} reads as {v}"));
                }
                for q in ["alpha", "a", "ab", "al", "alp", "alpah", "alpha hill", "testville", "alpha, testland", "ab, testland"] {
                    Searcher::new(&idx).search(q, 5);
                }
                Ok(())
            });
            match found {
                Ok(Ok(())) => {}
                Ok(Err(e)) => failed.push(format!("{case}: {e}")),
                Err(p) => failed.push(format!("{case}: {p}")),
            }
        }
        assert!(failed.is_empty(), "{failed:#?}");
    }

    #[test]
    fn walks_of_damaged_key_files_end() {
        // Each fst, and whether its open may refuse it.
        for (case, bytes, refusable) in [("a transition to its own node", a_loop(), true), ("2^60 paths", paths(60), false)] {
            let ended = ends_within(120, move || {
                let dir = TempDir::new("fst-walks");
                let (geo, out) = built(&dir);
                std::fs::write(out.join("places.fst"), bytes).unwrap();
                let idx = match SearchIndex::open(&geo, &out, None, None) {
                    Ok(idx) => idx,
                    Err(_) if refusable => return,
                    Err(e) => panic!("refused at open: {e}"),
                };
                // A prefix of no edit, of one and of two.
                for q in ["aaa", "aaaaaa", "aaaaaaaaaa"] {
                    Searcher::new(&idx).search(q, 5);
                }
            });
            assert!(ended.is_ok(), "{case}: {ended:?}");
        }
    }

    #[test]
    fn a_sub_index_for_a_walk_of_every_key_opens_only_with_the_checksum_of_its_keys() {
        let dir = TempDir::new("fst-verified");
        let (geo, out) = built(&dir);
        let n = geo.place_node_count() + geo.admin_polygon_count();
        assert!(SubIndex::open_verified(&out, "places", n).is_ok());
        assert!(SubIndex::open_verified(&out, "poi", 5).is_ok());
        // A bit of the first node (written from address 16, below the root) flipped.
        let flipped = |f: &str| {
            let mut b = std::fs::read(out.join(f)).unwrap();
            b[16] ^= 1;
            b
        };

        // Each fst, and whether the walk's open refuses it: the keys' checksum is the walk's, the
        // prefix table's is left to a walk of that table.
        let cases = [
            ("2^60 paths", "places.fst", paths(60), true),
            ("a bit flipped", "places.fst", flipped("places.fst"), true),
            ("2^60 paths", "places.pfx.fst", paths(60), false),
            ("a bit flipped", "places.pfx.fst", flipped("places.pfx.fst"), false),
        ];
        for (case, f, bytes, refused) in cases {
            let whole = std::fs::read(out.join(f)).unwrap();
            std::fs::write(out.join(f), bytes).unwrap();

            let (open, verified) = (SubIndex::open(&out, "places", n), SubIndex::open_verified(&out, "places", n));

            assert!(open.is_ok(), "{case} {f}: {:?}", open.err());
            if refused {
                assert!(verified.as_ref().err().is_some_and(|e| e.contains(&out.join(f).display().to_string()) && e.contains("checksum")), "{case} {f}: {:?}", verified.err());
            } else {
                assert!(verified.is_ok(), "{case} {f}: {:?}", verified.err());
            }
            std::fs::write(out.join(f), whole).unwrap();
        }
    }

    #[test]
    fn key_files_no_build_writes_are_refused_at_open_by_name() {
        let dir = TempDir::new("fst-refused");
        let (geo, out) = built(&dir);
        for f in ["places.fst", "poi.fst", "places.pfx.fst", "poi.pfx.fst"] {
            let whole = std::fs::read(out.join(f)).unwrap();
            let footer = whole.len() - 20;
            let cases = [
                ("empty", vec![]),
                ("cut to 30 bytes", whole[..30].to_vec()),
                ("of format 2", [&2u64.to_le_bytes()[..], &whole[8..]].concat()),
                ("a root past the end", [&whole[..footer + 8], &(whole.len() as u64).to_le_bytes(), &whole[footer + 16..]].concat()),
                ("a root below the end", [&whole[..footer + 8], &16u64.to_le_bytes(), &whole[footer + 16..]].concat()),
            ];
            for (case, bytes) in cases {
                std::fs::write(out.join(f), bytes).unwrap();

                let err = panic_of(|| SearchIndex::open(&geo, &out, None, None).err());

                assert!(err.as_ref().is_ok_and(|e| e.as_deref().is_some_and(|e| e.contains(f))), "{f} {case}: {err:?}");
            }
            std::fs::write(out.join(f), whole).unwrap();
        }
        std::fs::write(out.join(META_FILE), "{").unwrap();
        let err = SearchIndex::open(&geo, &out, None, None).err();
        assert!(err.as_deref().is_some_and(|e| e.contains(META_FILE)), "{err:?}");
    }

    #[test]
    fn prefix_hits_of_no_entity_of_their_sub_index_read_without_a_panic() {
        let dir = TempDir::new("pfx-entities");
        let (geo, out) = built(&dir);
        let n = geo.place_node_count() + geo.admin_polygon_count();
        // One list under the prefixes "al" and "ol": hits of entities 1, `entity` and 2 (each of
        // both sub-indexes).
        let hit = |entity: u32| PrefixHit { entity, partial: false, text: 1.0, imp_q: 250, class: 0, ord: 0 };
        let list = |entity: u32| {
            let mut b = Vec::new();
            encode_prefix_hits(&mut b, &[hit(1), hit(entity), hit(2)]);
            b
        };
        // That list of Charlie's node (4) second, cut short one byte before the end of its hit:
        // its count says three hits still.
        let mut cut = list(4);
        let mut two = Vec::new();
        encode_prefix_hits(&mut two, &[hit(1), hit(4)]);
        cut.truncate(two.len() - 1);
        // Each case's sub-index, its list, and the entity no result is of.
        let cases = [
            ("poi", "a POI past the records", list(5), 5),
            ("poi", "a POI far past the records", list(1 << 30), 1 << 30),
            ("places", "a POI in the places sub-index", list(POI_BIT | 7), POI_BIT | 7),
            ("places", "a place past the boundaries", list(n + 3), n + 3),
            ("places", "a list cut short", cut, 4),
        ];
        let mut failed = Vec::new();
        for (sub, case, bytes, entity) in cases {
            let [keys, pfx] = [format!("{sub}.pfx.fst"), format!("{sub}.pfx")].map(|f| (std::fs::read(out.join(&f)).unwrap(), f));
            std::fs::write(out.join(&keys.1), fst_of(&[("al", 0), ("ol", 0)])).unwrap();
            std::fs::write(out.join(&pfx.1), bytes).unwrap();
            let found = panic_of(|| {
                let idx = SearchIndex::open(&geo, &out, None, None).unwrap();
                let hits = idx.sub((sub == "poi") as u8).prefix_hits("al").map(|h| h.iter().map(|h| h.entity).collect::<Vec<_>>());
                (hits, ["a", "al", "ol", "o", "t", "te", "alpha", "old mill"].map(|q| Searcher::new(&idx).search(q, 5).0.iter().map(|f| f.hit.entity).collect::<Vec<_>>()))
            });
            // The entity as a result would carry it; the list's hits read up to it, the first alone.
            let tag = if sub == "poi" { POI_BIT } else { 0 };
            let bad = entity | tag;
            match found {
                Err(p) => failed.push(format!("{case}: {p}")),
                Ok((_, found)) if found.iter().flatten().any(|&e| e == bad) => failed.push(format!("{case}: {found:?}")),
                Ok((hits, _)) if hits.as_deref() != Some(&[1 | tag][..]) => failed.push(format!("{case}: hits {hits:?}")),
                Ok(_) => {}
            }
            for (bytes, f) in [keys, pfx] {
                std::fs::write(out.join(f), bytes).unwrap();
            }
        }
        assert!(failed.is_empty(), "{failed:#?}");
    }

    #[test]
    fn iso_codes_of_no_polygon_of_the_dataset_are_skipped() {
        let dir = TempDir::new("iso-polygons");
        let (geo, out) = built(&dir);
        let iso = dir.0.join("iso_codes.tsv");
        std::fs::write(&iso, format!("{}\tXX\n{}\tYY\n0\tTL\n", geo.admin_polygon_count(), u32::MAX)).unwrap();

        let idx = panic_of(|| SearchIndex::open(&geo, &out, None, Some(iso.to_str().unwrap())).map(|idx| idx.iso.country));

        let codes: Vec<String> = idx.clone().unwrap_or_else(|p| panic!("{p}")).unwrap().into_values().flatten().collect();
        assert_eq!(codes, ["TL"], "{idx:?}");
    }

    /// Splitmix64: a deterministic stream of numbers from a seed.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let z = (self.0 ^ (self.0 >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n.max(1) as u64) as usize
        }
    }

    /// `bytes` damaged one of the ways `rng` picks: bits flipped, bytes replaced, cut short, a word
    /// of up to 8 bytes set to an extreme, a run copied over another, or bytes replaced among the
    /// last 32 (an fst's footer and root).
    fn damage(bytes: &[u8], rng: &mut Rng) -> Vec<u8> {
        let mut b = bytes.to_vec();
        if b.is_empty() {
            return (0..rng.below(40)).map(|_| rng.next() as u8).collect();
        }
        let n = b.len();
        match rng.below(6) {
            0 => (0..1 + rng.below(4)).for_each(|_| b[rng.below(n)] ^= 1 << rng.below(8)),
            1 => (0..1 + rng.below(8)).for_each(|_| b[rng.below(n)] = rng.next() as u8),
            2 => b.truncate(rng.below(n)),
            3 => {
                let v = [0, u64::MAX, 1 << 63, u32::MAX as u64, 0x8080_8080_8080_8080][rng.below(5)];
                let at = rng.below(n);
                let len = (1 + rng.below(8)).min(n - at);
                b[at..at + len].copy_from_slice(&v.to_le_bytes()[..len]);
            }
            4 => {
                let len = 1 + rng.below(n);
                let (from, to) = (rng.below(n - len + 1), rng.below(n - len + 1));
                b.copy_within(from..from + len, to);
            }
            _ => (0..1 + rng.below(4)).for_each(|_| b[n - 1 - rng.below(n.min(32))] = rng.next() as u8),
        }
        b
    }

    /// Queries of every lookup: whole names, prefixes of each length, typos, trailing words, names
    /// of the extra-names file, context words.
    const FUZZ_QUERIES: [&str; 26] = [
        "a", "t", "o", "al", "te", "ol", "alp", "tes", "mil", "test", "alpha", "testv", "testvil", "testville",
        "tesville", "testvillle", "old mill", "old mil", "hill", "alpha hill", "chralie", "alphastadt",
        "testlandia", "testhalle", "testville, testland", "old mill, testland",
    ];

    /// Each sub-index walked whole: every key's list and best importance, every prefix's hits.
    fn walk(idx: &SearchIndex) {
        use fst::Streamer;
        let mut postings = Vec::new();
        for (_, sub) in idx.subs() {
            let mut s = sub.map.stream();
            while let Some((_, v)) = s.next() {
                sub.postings(v, &mut postings);
                sub.first_imp(v);
            }
            let mut s = sub.pfx_map.stream();
            while let Some((_, off)) = s.next() {
                sub.prefix_hits_at(off);
            }
        }
    }

    #[test]
    fn every_open_and_query_of_a_damaged_index_reads_or_refuses() {
        const SEEDS: u64 = 160;
        let ended = ends_within(1800, || {
            let dir = TempDir::new("fuzz");
            let geo = mixed().load(&dir.0.join("geo"));
            let (extras, iso, out) = (dir.0.join("extra_names.bin"), dir.0.join("iso_codes.tsv"), dir.0.join("idx"));
            let tsv = dir.0.join("extra_names.tsv");
            std::fs::write(&tsv, "p\t1\t2\tname:de\tAlphastadt\nb\t0\t2\tname:de\tTestlandia\no\t2\t3\talt_name\tTesthalle\n").unwrap();
            crate::extras::pack(tsv.to_str().unwrap(), extras.to_str().unwrap()).unwrap();
            std::fs::write(&iso, "0\tTL;TLD\n1\tTV\n").unwrap();
            build_search(&geo, &out, Some(&extras));
            let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(&out).unwrap().map(|e| e.unwrap().path()).filter(|p| p.is_file()).collect();
            files.sort();
            files.extend([extras.clone(), iso.clone()]);
            let (x, i) = (extras.to_str().unwrap(), iso.to_str().unwrap());
            let run = || -> bool {
                let Ok(idx) = SearchIndex::open(&geo, &out, Some(x), Some(i)) else { return false };
                let s = Searcher::new(&idx);
                for q in FUZZ_QUERIES {
                    for limit in [1, 5] {
                        for f in s.search(q, limit).0 {
                            std::hint::black_box((f.to_json(), s.display_name(&f.hit, q)));
                        }
                    }
                }
                walk(&idx);
                true
            };
            assert!(run(), "the index as built");
            let (mut opened, mut refused, mut failed) = (0, 0, Vec::new());
            for (k, f) in files.iter().enumerate() {
                let whole = std::fs::read(f).unwrap();
                for seed in 0..SEEDS {
                    let mut rng = Rng((k as u64) << 32 | seed);
                    std::fs::write(f, damage(&whole, &mut rng)).unwrap();
                    match panic_of(&run) {
                        Ok(true) => opened += 1,
                        Ok(false) => refused += 1,
                        Err(p) => failed.push(format!("{} seed {seed}: {p}", f.file_name().unwrap().to_string_lossy())),
                    }
                }
                std::fs::write(f, whole).unwrap();
            }
            eprintln!("damaged indexes: {opened} opened and read, {refused} refused, {} panicked", failed.len());
            assert!(failed.is_empty(), "{} of {} panicked: {failed:#?}", failed.len(), files.len() as u64 * SEEDS);
            assert!(opened > 0 && refused > 0, "{opened} opened, {refused} refused");
        });
        assert!(ended.is_ok(), "{}", ended.unwrap_err());
    }

    #[test]
    fn an_index_of_another_format_is_refused() {
        let dir = TempDir::new("format");
        let (geo, out) = built(&dir);
        let meta: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(out.join(META_FILE)).unwrap()).unwrap();
        let mut other = meta.clone();
        other["format"] = serde_json::json!(1);
        let mut none = meta.clone();
        none.as_object_mut().unwrap().remove("format");

        for (case, m) in [("format 1", other), ("no format", none)] {
            std::fs::write(out.join(META_FILE), m.to_string()).unwrap();

            let err = SearchIndex::open(&geo, &out, None, None).err();

            assert!(err.as_deref().is_some_and(|e| e.contains("format")), "{case}: {err:?}");
        }
        std::fs::write(out.join(META_FILE), meta.to_string()).unwrap();
        assert!(SearchIndex::open(&geo, &out, None, None).is_ok());
    }

    #[test]
    fn an_index_of_other_place_nodes_is_refused() {
        let dir = TempDir::new("node-count");
        let (geo, out) = built(&dir);
        let mut meta: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(out.join(META_FILE)).unwrap()).unwrap();
        meta["n_place_nodes"] = serde_json::json!(geo.place_node_count() + 1);
        std::fs::write(out.join(META_FILE), meta.to_string()).unwrap();

        let err = SearchIndex::open(&geo, &out, None, None).err();

        assert!(err.as_deref().is_some_and(|e| e.contains("place nodes")), "{err:?}");
    }

    #[test]
    fn a_list_ends_at_a_posting_of_no_entity_of_its_sub_index() {
        let dir = TempDir::new("entities");
        let (_, out) = built(&dir);
        let entities = |sub: &SubIndex, v: u64| {
            let mut got = Vec::new();
            sub.postings(v, &mut got);
            got.iter().map(|p| p.entity & !POI_BIT).collect::<Vec<_>>()
        };
        let (all, four) = (SubIndex::open(&out, "poi", 5).unwrap(), SubIndex::open(&out, "poi", 4).unwrap());
        let v = all.map.get("old mill").unwrap();

        // "Old Mill" names POIs 4, 1 and 0, best importance first.
        assert_eq!(entities(&all, v), [4, 1, 0]);
        assert_eq!(entities(&four, v), [0u32; 0]);
        for past in [all.post.len() as u64, u64::MAX] {
            assert_eq!(entities(&all, past), [0u32; 0], "{past}");
            assert_eq!(all.first_imp(past), 0, "{past}");
        }
    }

    #[test]
    fn an_unreadable_iso_file_fails_the_open() {
        let mut g = Geo::default();
        g.place("Alpha", 1, 10.0, 20.0);
        let dir = TempDir::new("iso");
        let geo = g.load(&dir.0.join("geo"));
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        let iso = dir.0.join("iso_codes.tsv");

        let err = SearchIndex::open(&geo, &out, None, Some(iso.to_str().unwrap())).err();

        assert!(err.as_deref().is_some_and(|e| e.contains("iso_codes.tsv")), "{err:?}");
        std::fs::write(&iso, "").unwrap();
        assert!(SearchIndex::open(&geo, &out, None, Some(iso.to_str().unwrap())).is_ok());
        assert!(SearchIndex::open(&geo, &out, None, None).is_ok());
    }
}
