//! Memory-mapped search index: the fst and postings of each sub-index (places, optional POI),
//! the boundary slots, and the geocoder index it points into. Loading reads only a small
//! meta file and the ISO code list; everything else is mapped and paged in by queries.
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
    pub fn load(path: &Path, geo: &Index) -> Iso {
        let mut iso = Iso::default();
        let Ok(text) = std::fs::read_to_string(path) else { return iso };
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
        iso
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
            &std::fs::read_to_string(dir.join("search_meta.json")).map_err(|e| format!("search_meta.json: {e}"))?,
        )
        .map_err(|e| e.to_string())?;
        let with_names = meta["extras"].as_bool().unwrap_or(false);
        let extras = match (with_names, extras) {
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
            ents: Entities { geo, n_place_nodes, bnd: Some(map(&dir.join("places.bnd"))?), extras },
            iso: iso.map(|p| Iso::load(Path::new(p), geo)).unwrap_or_default(),
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
