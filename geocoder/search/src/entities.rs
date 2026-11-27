//! Search entities over the geocoder's own records. Nothing here copies a name or a point:
//! place nodes, boundary polygons and POI records are read from the mapped geocoder files, and
//! the only derived table is the boundary slot list (polygon, then its label node or, without
//! one, its label point).
use crate::extras::{record_ref, Extras, KIND_PLACE_NODE, KIND_POI, KIND_POLYGON};
use crate::model::*;
use memmap2::Mmap;
use query_server::{coords_in_range, AdminPolygon, Index, PlaceNode, PoiRecord, NO_DATA};
use std::borrow::Cow;

/// Derived boundary slot: polygon index, label node (NO_DATA without one) and label point (the
/// label node's, else the ring's point on surface).
#[derive(Clone, Copy, Debug)]
pub struct BoundarySlot {
    pub poly: u32,
    pub node: u32,
    pub lat: f32,
    pub lng: f32,
}

/// A labelled slot in `places.bnd`: u32 polygon, u32 label node.
pub const LABELLED_SLOT_BYTES: usize = 8;
/// An unlabelled slot in `places.bnd`: u32 polygon, f32 lat, f32 lng.
pub const UNLABELLED_SLOT_BYTES: usize = 12;
/// The u32 after the slots of `places.bnd`: how many are labelled.
pub const BOUNDARY_FOOTER_BYTES: usize = 4;

impl BoundarySlot {
    pub fn linked(&self) -> bool {
        self.node != NO_DATA
    }

    /// Writes the slot as `places.bnd` holds it: a labelled one without its point, which is its
    /// node's.
    pub fn write(&self, w: &mut impl std::io::Write) -> std::io::Result<()> {
        w.write_all(&self.poly.to_le_bytes())?;
        if self.linked() {
            return w.write_all(&self.node.to_le_bytes());
        }
        w.write_all(&self.lat.to_le_bytes())?;
        w.write_all(&self.lng.to_le_bytes())
    }

    /// The records whose extra names are the slot's: its polygon, then its label node.
    pub fn record_refs(&self) -> Vec<u32> {
        let mut refs = vec![record_ref(KIND_POLYGON, self.poly)];
        if self.linked() {
            refs.push(record_ref(KIND_PLACE_NODE, self.node));
        }
        refs
    }
}

/// A boundary's names: its primary name, its label node's (when the node kept it) else the one
/// its record goes by; and its own name where that is another, the record's, or for a polygon that
/// goes by its node's English name the name it had (admin_own_names.bin).
pub fn slot_name_ids(geo: &Index, s: &BoundarySlot) -> (u32, Option<u32>) {
    let shown = polygon_raw(geo, s.poly).map_or(NO_DATA, |ap| ap.name_id);
    let own = geo.admin_own_name(s.poly).unwrap_or(shown);
    let node = if s.linked() { place_node_raw(geo, s.node).map_or(NO_DATA, |pn| pn.name_id) } else { NO_DATA };
    let primary = if node != NO_DATA { node } else { shown };
    if own != NO_DATA && own != primary {
        return (primary, Some(own));
    }
    (primary, None)
}


/// Place node `i`; None past the place nodes.
pub fn place_node_raw(geo: &Index, i: u32) -> Option<PlaceNode> {
    geo.place_node_record(i)
}

/// Admin polygon `i`, a tombstone's record too; None past the polygons.
pub fn polygon_raw(geo: &Index, i: u32) -> Option<AdminPolygon> {
    geo.admin_polygon_record(i)
}

/// POI record `i`, a tombstone's too; None without POIs, past the records, or for one whose row
/// does not read.
pub fn poi_raw(geo: &Index, i: u32) -> Option<PoiRecord> {
    geo.poi_records.as_ref()?.record(i)
}

pub fn cc_of(code: u16) -> [u8; 2] {
    [(code >> 8) as u8, (code & 0xFF) as u8]
}

pub fn cc_str(cc: &[u8; 2]) -> &str {
    std::str::from_utf8(cc).unwrap_or("")
}

/// Country of a record through its parent polygon: a POI's containing one, a place node's
/// claiming boundary (NO_DATA unless the node is its label).
pub fn parent_cc(geo: &Index, parent: u32) -> [u8; 2] {
    polygon_raw(geo, parent).map_or([0, 0], |ap| cc_of(ap.country_code))
}

pub fn boundary_type(ap: &AdminPolygon, linked_node: Option<&PlaceNode>) -> Cow<'static, str> {
    if let Some(f) = query_server::place_type_to_field(ap.place_type_override) {
        return Cow::Borrowed(f);
    }
    if let Some(pn) = linked_node {
        return Cow::Borrowed(PLACE_NODE_TYPES.get(pn.place_type as usize).copied().unwrap_or("place"));
    }
    if ap.admin_level == 2 {
        return Cow::Borrowed("country");
    }
    Cow::Owned(format!("admin{}", ap.admin_level))
}

/// The boundary slots of `places.bnd` (BOUNDARY_FILE): the labelled ones (LABELLED_SLOT_BYTES
/// each), the unlabelled ones (UNLABELLED_SLOT_BYTES each), each ascending by polygon, then a u32:
/// how many are labelled. A labelled slot's point is its label node's, read from the place nodes.
pub struct Boundaries {
    map: Mmap,
    labelled: usize,
    unlabelled: usize,
}

impl Boundaries {
    /// The slots of `map`; an error for a file no build writes (no footer, or tables that do not
    /// fill the file before it), which the slots themselves are not checked for.
    pub fn open(map: Mmap) -> Result<Boundaries, String> {
        let slots = map.len().checked_sub(BOUNDARY_FOOTER_BYTES).ok_or("a boundary file without a footer")?;
        let labelled = u32::from_le_bytes(map[slots..].try_into().unwrap()) as usize;
        let rest = labelled.checked_mul(LABELLED_SLOT_BYTES).and_then(|b| slots.checked_sub(b));
        let rest = rest.ok_or_else(|| format!("{labelled} labelled boundary slots, past the end of the file"))?;
        if rest % UNLABELLED_SLOT_BYTES != 0 {
            return Err(format!("{rest} bytes of unlabelled boundary slots, not whole slots"));
        }
        Ok(Boundaries { map, labelled, unlabelled: rest / UNLABELLED_SLOT_BYTES })
    }

    pub fn len(&self) -> usize {
        self.labelled + self.unlabelled
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn u32_at(&self, at: usize) -> u32 {
        u32::from_le_bytes(self.map[at..at + 4].try_into().unwrap())
    }

    /// The polygon of slot `pos` (below `len`).
    fn poly_at(&self, pos: usize) -> u32 {
        if pos < self.labelled {
            return self.u32_at(pos * LABELLED_SLOT_BYTES);
        }
        self.u32_at(self.labelled * LABELLED_SLOT_BYTES + (pos - self.labelled) * UNLABELLED_SLOT_BYTES)
    }

    /// Every slot's position, in polygon order: the two tables merged.
    pub fn by_polygon(&self) -> impl Iterator<Item = usize> + '_ {
        let (mut labelled, mut unlabelled) = (0, self.labelled);
        std::iter::from_fn(move || {
            let take_labelled = labelled < self.labelled && (unlabelled == self.len() || self.poly_at(labelled) < self.poly_at(unlabelled));
            let next = if take_labelled { &mut labelled } else { &mut unlabelled };
            (*next < self.len()).then(|| {
                *next += 1;
                *next - 1
            })
        })
    }

    /// Slot `pos` of the labelled ones, then the unlabelled ones; None past the last, and for a
    /// slot no build writes: of a polygon the dataset does not hold, labelled by a node that is
    /// not a hidden label of that polygon (PlacesSource::new), or with a label point off the
    /// globe (NaN among them).
    pub fn at(&self, geo: &Index, pos: usize) -> Option<BoundarySlot> {
        let slot = if pos < self.labelled {
            let at = pos * LABELLED_SLOT_BYTES;
            let (poly, node) = (self.u32_at(at), self.u32_at(at + 4));
            let pn = geo.place_node_record(node)?;
            if pn.is_visible() || pn._pad1 & PLACE_FLAG_LABEL_OF == 0 || pn.parent_poly_id != poly {
                return None;
            }
            BoundarySlot { poly, node, lat: pn.lat, lng: pn.lng }
        } else if pos < self.len() {
            let at = self.labelled * LABELLED_SLOT_BYTES + (pos - self.labelled) * UNLABELLED_SLOT_BYTES;
            let (lat, lng) = (f32::from_bits(self.u32_at(at + 4)), f32::from_bits(self.u32_at(at + 8)));
            if !coords_in_range(lat as f64, lng as f64) {
                return None;
            }
            BoundarySlot { poly: self.u32_at(at), node: NO_DATA, lat, lng }
        } else {
            return None;
        };
        (slot.poly < geo.admin_polygon_count()).then_some(slot)
    }

    /// The slot of polygon `poly` (each table is searched by polygon); None when there is none.
    pub fn find(&self, geo: &Index, poly: u32) -> Option<BoundarySlot> {
        let search = |first: usize, n: usize, stride: usize| {
            let (mut lo, mut hi) = (0usize, n);
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                match self.u32_at(first + mid * stride).cmp(&poly) {
                    std::cmp::Ordering::Equal => return Some(mid),
                    std::cmp::Ordering::Less => lo = mid + 1,
                    std::cmp::Ordering::Greater => hi = mid,
                }
            }
            None
        };
        let unlabelled_at = self.labelled * LABELLED_SLOT_BYTES;
        let pos = search(0, self.labelled, LABELLED_SLOT_BYTES)
            .or_else(|| search(unlabelled_at, self.unlabelled, UNLABELLED_SLOT_BYTES).map(|k| self.labelled + k))?;
        self.at(geo, pos)
    }
}

/// Query-time view of one sub-index's entities.
pub struct Entities<'a> {
    pub geo: &'a Index,
    pub n_place_nodes: u32,
    pub bnd: Boundaries,
    pub extras: Option<Extras>,
}

impl<'a> Entities<'a> {
    /// Slot of boundary entity `n_place_nodes + poly` (entity ids are record ids, stable across
    /// daily builds); None for a polygon without one.
    pub fn slot(&self, poly: u32) -> Option<BoundarySlot> {
        self.bnd.find(self.geo, poly)
    }

    pub fn is_poi(e: u32) -> bool {
        e & POI_BIT != 0
    }

    /// Where entity `e` is; None for a boundary without a slot, and for an entity whose record
    /// does not read.
    pub fn point(&self, e: u32) -> Option<(f32, f32)> {
        if Self::is_poi(e) {
            return poi_raw(self.geo, e & !POI_BIT).map(|r| (r.lat, r.lng));
        }
        if e < self.n_place_nodes {
            return place_node_raw(self.geo, e).map(|p| (p.lat, p.lng));
        }
        self.slot(e - self.n_place_nodes).map(|s| (s.lat, s.lng))
    }

    /// The records whose extra names are entity `e`'s.
    pub fn record_refs(&self, e: u32) -> Vec<u32> {
        if Self::is_poi(e) {
            return vec![record_ref(KIND_POI, e & !POI_BIT)];
        }
        if e < self.n_place_nodes {
            return vec![record_ref(KIND_PLACE_NODE, e)];
        }
        self.slot(e - self.n_place_nodes).map(|s| s.record_refs()).unwrap_or_default()
    }

    /// The record's own name of entity `e` (empty for a boundary without a slot, and for an
    /// entity whose record does not read).
    pub fn base_name(&self, e: u32) -> Cow<'a, str> {
        let id = if Self::is_poi(e) {
            poi_raw(self.geo, e & !POI_BIT).map(|r| r.name_id)
        } else if e < self.n_place_nodes {
            place_node_raw(self.geo, e).map(|p| p.name_id)
        } else {
            self.slot(e - self.n_place_nodes).map(|s| slot_name_ids(self.geo, &s).0)
        };
        id.map_or(Cow::Borrowed(""), |id| self.geo.get_string(id))
    }

    /// The name a posting matched: the record's own (ord 0) or an extra name.
    pub fn name(&self, e: u32, ord: u8) -> Cow<'a, str> {
        if ord == 0 || ord >= crate::constants::MAX_NAME_ORD {
            return self.base_name(e);
        }
        if ord == crate::constants::ORD_BOUNDARY_NAME {
            if !Self::is_poi(e) && e >= self.n_place_nodes {
                if let Some(s) = self.slot(e - self.n_place_nodes) {
                    if let (_, Some(own)) = slot_name_ids(self.geo, &s) {
                        return self.geo.get_string(own);
                    }
                }
            }
            return self.base_name(e);
        }
        match self.extras.as_ref().and_then(|x| x.name_at(&self.record_refs(e), ord)) {
            Some((n, _)) => Cow::Owned(n.to_string()),
            None => self.base_name(e),
        }
    }

    /// The kind and type of entity `e`; None for one that is nowhere (`point`).
    pub fn kind_and_type_of(&self, e: u32) -> Option<(Kind, Cow<'a, str>)> {
        if Self::is_poi(e) {
            let r = poi_raw(self.geo, e & !POI_BIT)?;
            return Some((Kind::Poi, Cow::Owned(self.geo.poi_meta.category_name(r.category).to_string())));
        }
        if e < self.n_place_nodes {
            let p = place_node_raw(self.geo, e)?;
            let t = PLACE_NODE_TYPES.get(p.place_type as usize).copied().unwrap_or("place");
            return Some((place_node_kind(p.place_type), Cow::Borrowed(t)));
        }
        let (s, ap) = self.slot(e - self.n_place_nodes).and_then(|s| Some((s, polygon_raw(self.geo, s.poly)?)))?;
        // Typed as the build typed it for ranking (PlacesSource::fill): by its label node.
        let node = s.linked().then(|| place_node_raw(self.geo, s.node)).flatten();
        Some((if s.linked() { Kind::Linked } else { Kind::Area }, boundary_type(&ap, node.as_ref())))
    }

    /// The kind and type of entity `e` (`kind_and_type_of`), of no type for one that is nowhere:
    /// a POI, a place, or an area for a boundary without a slot.
    pub fn kind_and_type(&self, e: u32) -> (Kind, Cow<'a, str>) {
        self.kind_and_type_of(e).unwrap_or_else(|| {
            let k = if Self::is_poi(e) {
                Kind::Poi
            } else if e < self.n_place_nodes {
                Kind::Place
            } else {
                Kind::Area
            };
            (k, Cow::Borrowed(""))
        })
    }

    /// An area: of kind Area, or of an area type, by the kind and type of an entity.
    pub fn is_area(k: Kind, t: &str) -> bool {
        k == Kind::Area || crate::constants::AREA_TYPES.contains(&t)
    }

    /// A settlement: a visible place node of a settlement type (city, town, village, hamlet) or a
    /// boundary a place node labels, of no area type; not a suburb-like node and not a POI. By
    /// the kind and type of an entity.
    pub fn is_settlement(k: Kind, t: &str) -> bool {
        matches!(k, Kind::Place | Kind::Linked) && !crate::constants::AREA_TYPES.contains(&t)
    }
}

#[cfg(test)]
mod tests {
    use crate::fixture::{build_search, new_request, Geo, TempDir};
    use crate::model::Kind;
    use crate::query::Searcher;
    use crate::reader::SearchIndex;

    #[test]
    fn a_labelled_boundary_has_its_label_nodes_type() {
        let mut g = Geo::default();
        // An admin8 boundary without a place type override, labelled by a town node.
        let poly = g.boundary("Testville", 8, [10.0, 20.0, 10.2, 20.2]);
        let node = g.place("Testville", 1, 10.1, 20.1);
        g.claim(poly, node);
        let dir = TempDir::new("label-type");
        let geo = g.load(&dir.0.join("geo"));
        let out = dir.0.join("idx");
        build_search(&geo, &out, None);
        let idx = SearchIndex::open(&geo, &out, None, None).unwrap();

        let (found, _) = Searcher::new(&idx).search(&new_request("Testville", 5));

        // The boundary is entity n_place_nodes (1) + its polygon; its node is not its own.
        let got: Vec<_> = found.iter().map(|f| (f.hit.entity, f.kind, f.typ.as_str())).collect();
        assert_eq!(got, [(1 + poly, Kind::Linked, "town")]);
    }
}
