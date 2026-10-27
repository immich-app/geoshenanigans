//! Search entities over the geocoder's own records. Nothing here copies a name or a point:
//! place nodes, boundary polygons and POI records are read from the mapped geocoder files, and
//! the only derived table is the boundary slot list (polygon, label point).
use crate::extras::{record_ref, Extras, KIND_PLACE_NODE, KIND_POI, KIND_POLYGON};
use crate::model::*;
use memmap2::Mmap;
use query_server::{AdminPolygon, FileBytes, Index, PlaceNode, PoiRecord, NO_DATA};
use std::borrow::Cow;

/// Derived boundary slot: polygon index, label node (NO_DATA without one) and label point
/// (16 bytes in `places.bnd`: u32 poly, u32 node, f32 lat, f32 lng).
#[derive(Clone, Copy, Debug)]
pub struct BoundarySlot {
    pub poly: u32,
    pub node: u32,
    pub lat: f32,
    pub lng: f32,
}

pub const SLOT_BYTES: usize = 16;

impl BoundarySlot {
    pub fn linked(&self) -> bool {
        self.node != NO_DATA
    }

    pub fn to_bytes(&self) -> [u8; SLOT_BYTES] {
        let mut b = [0u8; SLOT_BYTES];
        b[0..4].copy_from_slice(&self.poly.to_le_bytes());
        b[4..8].copy_from_slice(&self.node.to_le_bytes());
        b[8..12].copy_from_slice(&self.lat.to_le_bytes());
        b[12..16].copy_from_slice(&self.lng.to_le_bytes());
        b
    }

    pub fn from_bytes(b: &[u8]) -> Self {
        let u = |i: usize| u32::from_le_bytes(b[i..i + 4].try_into().unwrap());
        BoundarySlot { poly: u(0), node: u(4), lat: f32::from_bits(u(8)), lng: f32::from_bits(u(12)) }
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

pub fn record_count<T>(f: &FileBytes) -> u32 {
    (f.len() / std::mem::size_of::<T>() as u64) as u32
}

pub fn place_node_raw(geo: &Index, i: u32) -> PlaceNode {
    geo.place_nodes.as_ref().expect("place_nodes").read_at::<PlaceNode>(i as u64)
}

pub fn polygon_raw(geo: &Index, i: u32) -> AdminPolygon {
    geo.admin_polygons.read_at::<AdminPolygon>(i as u64)
}

pub fn poi_raw(geo: &Index, i: u32) -> PoiRecord {
    geo.poi_records.as_ref().expect("poi_records").read_at::<PoiRecord>(i as u64)
}

pub fn cc_of(code: u16) -> [u8; 2] {
    [(code >> 8) as u8, (code & 0xFF) as u8]
}

pub fn cc_str(cc: &[u8; 2]) -> &str {
    std::str::from_utf8(cc).unwrap_or("")
}

/// Country of a record through its containing polygon (place nodes, POIs).
pub fn parent_cc(geo: &Index, parent: u32) -> [u8; 2] {
    let n = record_count::<AdminPolygon>(&geo.admin_polygons);
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

/// Query-time view of one sub-index's entities.
pub struct Entities<'a> {
    pub geo: &'a Index,
    pub n_place_nodes: u32,
    pub bnd: Option<Mmap>,
    pub extras: Option<Extras>,
}

impl<'a> Entities<'a> {
    /// Slot of boundary entity `n_place_nodes + poly` (entity ids are record ids, stable across
    /// daily builds; the slot table is sorted by polygon).
    pub fn slot(&self, poly: u32) -> BoundarySlot {
        let b = &self.bnd.as_ref().expect("places.bnd")[..];
        let (mut lo, mut hi) = (0usize, b.len() / SLOT_BYTES);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let p = u32::from_le_bytes(b[mid * SLOT_BYTES..mid * SLOT_BYTES + 4].try_into().unwrap());
            if p < poly {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        BoundarySlot::from_bytes(&b[lo * SLOT_BYTES..lo * SLOT_BYTES + SLOT_BYTES])
    }

    pub fn is_poi(e: u32) -> bool {
        e & POI_BIT != 0
    }

    pub fn point(&self, e: u32) -> (f32, f32) {
        if Self::is_poi(e) {
            let r = poi_raw(self.geo, e & !POI_BIT);
            return (r.lat, r.lng);
        }
        if e < self.n_place_nodes {
            let p = place_node_raw(self.geo, e);
            return (p.lat, p.lng);
        }
        let s = self.slot(e - self.n_place_nodes);
        (s.lat, s.lng)
    }

    /// The records whose extra names are entity `e`'s.
    pub fn record_refs(&self, e: u32) -> Vec<u32> {
        if Self::is_poi(e) {
            return vec![record_ref(KIND_POI, e & !POI_BIT)];
        }
        if e < self.n_place_nodes {
            return vec![record_ref(KIND_PLACE_NODE, e)];
        }
        self.slot(e - self.n_place_nodes).record_refs()
    }

    pub fn base_name(&self, e: u32) -> &'a str {
        let id = if Self::is_poi(e) {
            poi_raw(self.geo, e & !POI_BIT).name_id
        } else if e < self.n_place_nodes {
            place_node_raw(self.geo, e).name_id
        } else {
            slot_name_ids(self.geo, &self.slot(e - self.n_place_nodes)).0
        };
        self.geo.get_string(id)
    }

    /// The name a posting matched: the record's own (ord 0) or an extra name.
    pub fn name(&self, e: u32, ord: u8) -> Cow<'a, str> {
        if ord == 0 || ord >= crate::constants::MAX_NAME_ORD {
            return Cow::Borrowed(self.base_name(e));
        }
        if ord == crate::constants::ORD_BOUNDARY_NAME {
            if !Self::is_poi(e) && e >= self.n_place_nodes {
                let s = self.slot(e - self.n_place_nodes);
                return Cow::Borrowed(self.geo.get_string(polygon_raw(self.geo, s.poly).name_id));
            }
            return Cow::Borrowed(self.base_name(e));
        }
        match self.extras.as_ref().and_then(|x| x.name_at(&self.record_refs(e), ord)) {
            Some((n, _)) => Cow::Owned(n.to_string()),
            None => Cow::Borrowed(self.base_name(e)),
        }
    }

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
        let s = self.slot(e - self.n_place_nodes);
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
