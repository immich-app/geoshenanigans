//! Memory-mapped search index: the fst and postings of each sub-index (places, optional POI),
//! the boundary slots, and the geocoder index it points into. Loading reads only a small
//! meta file, the boundary file's footer and the ISO code list; everything else is mapped and
//! paged in by queries.
use crate::build::{has_extra_names, has_sub_index, is_current_format, sub_index_files};
use crate::constants::{BOUNDARY_FILE, INDEX_FORMAT, META_FILE, META_FORMAT_KEY};
use crate::entities::{Boundaries, Entities};
use crate::extras::Extras;
use crate::model::*;
use crate::normalize::normalize;
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
    pub map: fst::Map<Mmap>,
    pub post: Mmap,
    pub pfx_map: fst::Map<Mmap>,
    pub pfx: Mmap,
    /// Entity ids of this sub-index carry POI_BIT.
    pub poi: bool,
    /// Entities of the sub-index are below this (before POI_BIT): a posting at or past it ends
    /// its list.
    entities: u32,
}

impl SubIndex {
    /// The sub-index `name` in `dir`, whose entities are below `entities`.
    pub fn open(dir: &Path, name: &str, entities: u32) -> Result<Self, String> {
        let [keys, post, pfx_keys, pfx] = sub_index_files(name).map(|f| dir.join(f));
        Ok(SubIndex {
            map: fst::Map::new(map(&keys)?).map_err(|e| e.to_string())?,
            post: map(&post)?,
            pfx_map: fst::Map::new(map(&pfx_keys)?).map_err(|e| e.to_string())?,
            pfx: map(&pfx)?,
            poi: name == "poi",
            entities,
        })
    }

    fn tag(&self, mut p: Posting) -> Posting {
        if self.poi {
            p.entity |= POI_BIT;
        }
        p
    }

    /// The postings of the key whose value is `v`: its list, read up to a posting of no entity
    /// of the sub-index (`ListReader` for a damaged one).
    pub fn postings(&self, v: u64, out: &mut Vec<Posting>) {
        out.clear();
        let Ok(off) = usize::try_from(v) else { return };
        out.extend(ListReader::new(&self.post, off).take_while(|p| p.entity < self.entities).map(|p| self.tag(p)));
    }

    /// Best importance under a key, from the head of its list (0 for a list without a posting).
    pub fn first_imp(&self, v: u64) -> u8 {
        usize::try_from(v).ok().and_then(|off| list_top_imp(&self.post, off)).unwrap_or(0)
    }

    pub fn prefix_hits(&self, phrase: &str) -> Option<Vec<PrefixHit>> {
        let off = self.pfx_map.get(phrase)?;
        let mut out = Vec::new();
        decode_prefix_hits(&self.pfx, off as usize, &mut out);
        if self.poi {
            for h in out.iter_mut() {
                h.entity |= POI_BIT;
            }
        }
        Some(out)
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
    /// The ISO code list at `path` (lines "<polygon>\t<code>;<code>..."); a file that cannot be
    /// read is an error.
    pub fn load(path: &Path, geo: &Index) -> Result<Iso, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let mut iso = Iso::default();
        for line in text.lines() {
            let Some((poly, codes)) = line.split_once('\t') else { continue };
            let Ok(poly) = poly.parse::<u32>() else { continue };
            let ap = crate::entities::polygon_raw(geo, poly);
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
    /// for an index of another format (INDEX_FORMAT) or of another count of place nodes, and for
    /// a boundary file no build writes.
    pub fn open(geo: &'g Index, dir: &Path, extras: Option<&str>, iso: Option<&str>) -> Result<Self, String> {
        let meta: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(dir.join(META_FILE)).map_err(|e| format!("{META_FILE}: {e}"))?,
        )
        .map_err(|e| e.to_string())?;
        if !is_current_format(&meta) {
            return Err(format!("{}: an index of format {}, not {INDEX_FORMAT}: build it again", dir.display(), meta[META_FORMAT_KEY]));
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
        let cases = [
            ("a label node past the place nodes", le(&[1, u32::MAX, 0, lat, lng, 1])),
            ("no slot of the boundaries", le(&[0])),
            ("slots out of order", le(&[5, 0, 1, 0, 2])),
        ];
        for (case, bytes) in cases {
            std::fs::write(out.join(BOUNDARY_FILE), bytes).unwrap();
            let idx = SearchIndex::open(&geo, &out, None, None).unwrap();

            for q in ["testville", "testland", "test", "alpha"] {
                let found = Searcher::new(&idx).search(q, 5).0;
                assert!(found.iter().all(|f| f.lat.is_finite()), "{case}: {q}");
            }
        }
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
