//! Memory-mapped search index: the fst and postings of each sub-index (places, optional POI),
//! the boundary slots, and the geocoder index it points into. Loading reads only a small
//! meta file and the ISO code list; everything else is mapped and paged in by queries.
use crate::build::has_extra_names;
use crate::constants::{BOUNDARY_FILE, META_FILE};
use crate::entities::Entities;
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
    pub map: fst::Map<Mmap>,
    pub post: Mmap,
    pub pfx_map: fst::Map<Mmap>,
    pub pfx: Mmap,
    /// Entity ids of this sub-index carry POI_BIT.
    pub poi: bool,
}

impl SubIndex {
    pub fn open(dir: &Path, name: &str) -> Result<Self, String> {
        let m = |ext: &str| map(&dir.join(format!("{name}.{ext}")));
        Ok(SubIndex {
            map: fst::Map::new(m("fst")?).map_err(|e| e.to_string())?,
            post: m("post")?,
            pfx_map: fst::Map::new(m("pfx.fst")?).map_err(|e| e.to_string())?,
            pfx: m("pfx")?,
            poi: name == "poi",
        })
    }

    fn tag(&self, mut p: Posting) -> Posting {
        if self.poi {
            p.entity |= POI_BIT;
        }
        p
    }

    pub fn postings(&self, v: u64, out: &mut Vec<Posting>) {
        out.clear();
        if v & 1 == 1 {
            out.push(self.tag(decode_inline(v)));
            return;
        }
        out.extend(ListReader::new(&self.post, list_offset(v)).map(|p| self.tag(p)));
    }

    /// Best importance under a key, from the fst value alone.
    pub fn first_imp(&self, v: u64) -> u8 {
        if v & 1 == 1 {
            return decode_inline(v).imp_q;
        }
        value_top_imp(v)
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
                iso.state.entry((cc, normalize(geo.get_string(ap.name_id)))).or_default().extend(codes);
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
    /// decompressed extra-names file the index was built with, `iso` the ISO code list.
    pub fn open(geo: &'g Index, dir: &Path, extras: Option<&str>, iso: Option<&str>) -> Result<Self, String> {
        let meta: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(dir.join(META_FILE)).map_err(|e| format!("{META_FILE}: {e}"))?,
        )
        .map_err(|e| e.to_string())?;
        let extras = match (has_extra_names(&meta), extras) {
            (true, Some(p)) => Some(Extras::open(p)?),
            (true, None) => return Err("index was built with extra names: pass the extra-names file".into()),
            _ => None,
        };
        let n_place_nodes = meta["n_place_nodes"].as_u64().unwrap_or(0) as u32;
        let poi = if dir.join("poi.fst").exists() && geo.poi_records.is_some() {
            Some(SubIndex::open(dir, "poi")?)
        } else {
            None
        };
        Ok(SearchIndex {
            geo,
            places: SubIndex::open(dir, "places")?,
            poi,
            ents: Entities { geo, n_place_nodes, bnd: Some(map(&dir.join(BOUNDARY_FILE))?), extras },
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
        // Place nodes only, one posting per key (all inline), no boundary slot, and a POI
        // sub-index without a named POI: places.post, places.bnd, poi.post and poi.pfx are empty.
        let mut g = Geo::default();
        g.place("Alpha", 1, 10.0, 20.0);
        g.place("Bravo", 2, 10.1, 20.1);
        g.poi("", 10.0, 20.0, 0);
        let dir = TempDir::new("empty-files");
        let geo = g.load(&dir.0.join("geo"));
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        for f in ["places.post", "places.bnd", "poi.post", "poi.pfx"] {
            assert_eq!(std::fs::metadata(out.join(f)).unwrap().len(), 0, "{f}");
        }

        let idx = SearchIndex::open(&geo, &out, None, None).unwrap();

        assert!(idx.poi.is_some());
        let (found, _) = Searcher::new(&idx).search("alpha", 5);
        assert_eq!(found.iter().map(|f| (f.hit.entity, f.name.as_str())).collect::<Vec<_>>(), [(0, "Alpha")]);
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
