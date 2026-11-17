//! Search entities over the geocoder's own records. Nothing here copies a name or a point:
//! place nodes, boundary polygons and POI records are read from the mapped geocoder files, and
//! the only derived table is the boundary slot list (polygon, then its label node or, without
//! one, its label point).
use crate::extras::{record_ref, Extras, KIND_PLACE_NODE, KIND_POI, KIND_POLYGON};
use crate::model::*;
use memmap2::Mmap;
use query_server::{AdminPolygon, Index, PlaceNode, PoiRecord, NO_DATA};
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

/// A labelled boundary's primary name: its label node's (when the node kept it), else its own.
pub fn slot_name_ids(geo: &Index, s: &BoundarySlot) -> (u32, Option<u32>) {
    let own = polygon_raw(geo, s.poly).name_id;
    if s.linked() {
        let n = place_node_raw(geo, s.node).name_id;
        if n != NO_DATA && n != own {
            return (n, Some(own));
        }
    }
    (own, None)
}

pub fn place_node_raw(geo: &Index, i: u32) -> PlaceNode {
    geo.place_node_record(i).expect("place node")
}

pub fn polygon_raw(geo: &Index, i: u32) -> AdminPolygon {
    geo.admin_polygon_record(i).expect("admin polygon record")
}

pub fn poi_raw(geo: &Index, i: u32) -> PoiRecord {
    geo.poi_records.as_ref().expect("poi_records").record(i).expect("poi record")
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
    let n = geo.admin_polygon_count();
    if parent == NO_DATA || parent >= n {
        return [0, 0];
    }
    cc_of(polygon_raw(geo, parent).country_code)
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
    /// labelled slot whose node the place nodes do not hold.
    pub fn at(&self, geo: &Index, pos: usize) -> Option<BoundarySlot> {
        if pos < self.labelled {
            let at = pos * LABELLED_SLOT_BYTES;
            let node = self.u32_at(at + 4);
            let pn = geo.place_node_record(node)?;
            return Some(BoundarySlot { poly: self.u32_at(at), node, lat: pn.lat, lng: pn.lng });
        }
        if pos >= self.len() {
            return None;
        }
        let at = self.labelled * LABELLED_SLOT_BYTES + (pos - self.labelled) * UNLABELLED_SLOT_BYTES;
        let (lat, lng) = (f32::from_bits(self.u32_at(at + 4)), f32::from_bits(self.u32_at(at + 8)));
        Some(BoundarySlot { poly: self.u32_at(at), node: NO_DATA, lat, lng })
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
        self.at(geo, pos).filter(|s| s.poly < geo.admin_polygon_count())
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

    /// Where entity `e` is; None for a boundary without a slot.
    pub fn point(&self, e: u32) -> Option<(f32, f32)> {
        if Self::is_poi(e) {
            let r = poi_raw(self.geo, e & !POI_BIT);
            return Some((r.lat, r.lng));
        }
        if e < self.n_place_nodes {
            let p = place_node_raw(self.geo, e);
            return Some((p.lat, p.lng));
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

    /// The record's own name of entity `e` (empty for a boundary without a slot).
    pub fn base_name(&self, e: u32) -> Cow<'a, str> {
        let id = if Self::is_poi(e) {
            poi_raw(self.geo, e & !POI_BIT).name_id
        } else if e < self.n_place_nodes {
            place_node_raw(self.geo, e).name_id
        } else {
            let Some(s) = self.slot(e - self.n_place_nodes) else { return Cow::Borrowed("") };
            slot_name_ids(self.geo, &s).0
        };
        self.geo.get_string(id)
    }

    /// The name a posting matched: the record's own (ord 0) or an extra name.
    pub fn name(&self, e: u32, ord: u8) -> Cow<'a, str> {
        if ord == 0 || ord >= crate::constants::MAX_NAME_ORD {
            return self.base_name(e);
        }
        if ord == crate::constants::ORD_BOUNDARY_NAME {
            if !Self::is_poi(e) && e >= self.n_place_nodes {
                if let Some(s) = self.slot(e - self.n_place_nodes) {
                    return self.geo.get_string(polygon_raw(self.geo, s.poly).name_id);
                }
            }
            return self.base_name(e);
        }
        match self.extras.as_ref().and_then(|x| x.name_at(&self.record_refs(e), ord)) {
            Some((n, _)) => Cow::Owned(n.to_string()),
            None => self.base_name(e),
        }
    }

    /// The kind and type of entity `e` (an area of no type for a boundary without a slot).
    pub fn kind_and_type(&self, e: u32) -> (Kind, Cow<'a, str>) {
        if Self::is_poi(e) {
            let r = poi_raw(self.geo, e & !POI_BIT);
            return (Kind::Poi, Cow::Owned(self.geo.poi_meta.category_name(r.category).to_string()));
        }
        if e < self.n_place_nodes {
            let p = place_node_raw(self.geo, e);
            let t = PLACE_NODE_TYPES.get(p.place_type as usize).copied().unwrap_or("place");
            return (place_node_kind(p.place_type), Cow::Borrowed(t));
        }
        let Some(s) = self.slot(e - self.n_place_nodes) else { return (Kind::Area, Cow::Borrowed("")) };
        let ap = polygon_raw(self.geo, s.poly);
        // Typed as the build typed it for ranking (PlacesSource::fill): by its label node.
        let node = s.linked().then(|| place_node_raw(self.geo, s.node));
        (if s.linked() { Kind::Linked } else { Kind::Area }, boundary_type(&ap, node.as_ref()))
    }

    pub fn is_area(&self, e: u32) -> bool {
        let (k, t) = self.kind_and_type(e);
        k == Kind::Area || crate::constants::AREA_TYPES.contains(&t.as_ref())
    }

    /// A settlement: a visible place node of a settlement type (city, town, village, hamlet) or a
    /// boundary a place node labels, of no area type; not a suburb-like node and not a POI.
    pub fn is_settlement(&self, e: u32) -> bool {
        let (k, t) = self.kind_and_type(e);
        matches!(k, Kind::Place | Kind::Linked) && !crate::constants::AREA_TYPES.contains(&t.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use crate::fixture::{build_search, Geo, TempDir};
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

        let (found, _) = Searcher::new(&idx).search("Testville", 5);

        // The boundary is entity n_place_nodes (1) + its polygon; its node is not its own.
        let got: Vec<_> = found.iter().map(|f| (f.hit.entity, f.kind, f.typ.as_str())).collect();
        assert_eq!(got, [(1 + poly, Kind::Linked, "town")]);
    }
}
