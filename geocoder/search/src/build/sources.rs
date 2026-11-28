use crate::{
    constants::*,
    index::{entities::*, invalid},
    model::PLACE_NODE_TYPES,
    tags::TagBundle,
};
use query_server::{AdminPolygon, Index, SideBlocks, NO_DATA, POI_FLAG_SECONDARY_RING};
use std::{io, path::Path};

pub(crate) fn side(path: &Path, count: u32) -> io::Result<Option<SideBlocks>> {
    if !path.try_exists()? {
        return Ok(None);
    }
    let name = path.to_str().ok_or_else(|| invalid("non UTF-8 dataset path"))?;
    let bytes = query_server::mmap_file(name).map_err(invalid)?;
    bytes.advise_in_order(true);
    let rows = SideBlocks::open(bytes, name).map_err(invalid)?;
    if rows.count() != count {
        return Err(invalid(format!("{} has {} rows, expected {count}", path.display(), rows.count())));
    }
    Ok(Some(rows))
}

#[derive(Clone, Copy)]
pub(super) enum SourceRow {
    Place(query_server::PlaceNode),
    Area(AdminPolygon),
    Poi(query_server::PoiRecord),
}
impl SourceRow {
    pub(super) fn point(self, geo: &Index) -> Option<(f64, f64)> {
        match self {
            Self::Area(a) => geo.area_centre(&a),
            Self::Place(p) => Some((p.lat.into(), p.lng.into())),
            Self::Poi(p) => Some((p.lat.into(), p.lng.into())),
        }
        .filter(|&(lat, lon)| query_server::coords_in_range(lat, lon))
    }
    pub(super) fn name_id(self) -> u32 {
        match self {
            Self::Area(a) => a.name_id,
            Self::Place(p) => p.name_id,
            Self::Poi(p) => p.name_id,
        }
    }
    pub(super) fn country(self) -> Option<[u8; 2]> {
        match self {
            Self::Area(a) => query_server::country_code_bytes(a.country_code),
            _ => None,
        }
    }
    pub(super) fn is_country(self) -> bool {
        matches!(self,Self::Area(a) if a.admin_level==COUNTRY_LEVEL)
    }
    pub(super) fn max_rank(self, geo: &Index, point: (f64, f64)) -> u8 {
        let rank = match self {
            Self::Place(p) if p.rank_address == 0 => query_server::PLACE_NODE_SEARCH_RANK[p.place_type as usize],
            Self::Place(p) => p.rank_address,
            Self::Area(a) if a.rank_address == 0 => {
                // Approximate geometry_to_rank by rank_search. Even a country outside
                // the address hierarchy can supply the country's search rank rules.
                let country = self
                    .country()
                    .or_else(|| geo.find_admin(point.0, point.1).country_code)
                    .unwrap_or_default()
                    .map(|b| b.to_ascii_uppercase());
                query_server::area_search_rank(&a, std::str::from_utf8(&country).unwrap_or(""))
            }
            Self::Area(a) => a.rank_address,
            Self::Poi(_) => MAX_ADDRESS_RANK,
        };
        rank.min(MAX_ADDRESS_RANK)
    }

    fn ranks(self, cc: &str) -> (u8, u8) {
        match self {
            Self::Place(p) => (query_server::PLACE_NODE_SEARCH_RANK[p.place_type as usize], p.rank_address),
            Self::Area(a) => (query_server::area_search_rank(&a, cc), a.rank_address),
            Self::Poi(_) => (POI_SEARCH_RANK, POI_SEARCH_RANK),
        }
    }
}
/// Full-profile object identities come from the builder's stable slot sidecars.
/// The four-byte slot prefix carries its type, tombstone flag and tier.
struct OsmRows(memmap2::Mmap);
impl OsmRows {
    fn open(path: &Path, count: u32) -> io::Result<Option<Self>> {
        if !path.try_exists()? {
            return Ok(None);
        }
        let bytes = crate::index::map(path)?;
        use crate::index::number;
        if number(&bytes, 0) != Some(OSM_SLOT_MAGIC)
            || number(&bytes, 4) != Some(OSM_SLOT_VERSION)
            || number(&bytes, 8) != Some(count)
            || bytes.len() as u64 != OSM_SLOT_HEADER as u64 + u64::from(count) * OSM_SLOT_BYTES as u64
        {
            return Err(invalid(format!("invalid OSM row map {}", path.display())));
        }
        Ok(Some(Self(bytes)))
    }
    fn get(&self, i: u32) -> Option<OsmRef> {
        let at = (i as usize).checked_mul(OSM_SLOT_BYTES)?.checked_add(OSM_SLOT_HEADER)?;
        let bytes = self.0.get(at..)?.get(..OSM_SLOT_BYTES)?;
        if bytes[1] & OSM_SLOT_TOMBSTONE != 0 {
            return None;
        }
        let kind = match bytes[0] {
            1 => OsmKind::Node,
            2 => OsmKind::Way,
            3 => OsmKind::Relation,
            _ => return None,
        };
        let stable_id = u64::from_le_bytes(bytes[4..].try_into().ok()?);
        // Slot assignment distinguishes a relation's rings. Search exposes the OSM object they share.
        let id = match kind {
            OsmKind::Relation => stable_id >> OSM_RELATION_RING_BITS,
            _ => stable_id,
        };
        (id != 0).then_some(OsmRef { kind, id })
    }
}

pub struct CoreRecords<'a> {
    pub(super) geo: &'a Index,
    files: crate::dataset::SearchFiles,
    pub(super) sizes: [u32; 3],
    areas: Option<&'a crate::index::areas::AreaRanges>,
    osm: [Option<OsmRows>; 3],
}
impl<'a> CoreRecords<'a> {
    pub fn open(geo: &'a Index, set: &Path) -> io::Result<Self> {
        Self::with_areas(geo, set, None)
    }
    /// A resolver for the same generation as the index's entities and outline bounds.
    pub fn for_index(geo: &'a Index, set: &Path, core: &'a crate::index::CoreIndex) -> io::Result<Self> {
        Self::with_areas(geo, set, (core.areas.profile == crate::profile::Profile::Full).then_some(&core.areas))
    }

    fn with_areas(geo: &'a Index, set: &Path, areas: Option<&'a crate::index::areas::AreaRanges>) -> io::Result<Self> {
        let sizes =
            [geo.place_node_count(), geo.admin_polygon_count(), geo.poi_records.as_ref().map_or(0, |p| p.len())];
        let mut osm = [None, None, None];
        if areas.is_some() {
            for ((map, file), n) in osm.iter_mut().zip(OSM_ID_FILES).zip(sizes) {
                *map = OsmRows::open(&set.join(file), n)?;
            }
        }
        Ok(Self { geo, files: crate::dataset::SearchFiles::open(set, geo).map_err(invalid)?, sizes, areas, osm })
    }
    pub(super) fn row(&self, e: EntityRef) -> Option<SourceRow> {
        let i = e.record();
        match e.kind() {
            EntityKind::Place => self
                .geo
                .place_node_record(i)
                .filter(|p| p.is_visible() && p.name_id != NO_DATA && (p.place_type as usize) < PLACE_NODE_TYPES.len())
                .map(SourceRow::Place),
            EntityKind::Area => self
                .geo
                .admin_polygon(i)
                .filter(|p| {
                    ((COUNTRY_LEVEL..=ADMIN_LEVEL_MAX).contains(&p.admin_level) || p.admin_level == PLACE_AREA_LEVEL)
                        && self.geo.admin_geometry.has_rings(p)
                })
                .map(SourceRow::Area),
            EntityKind::Poi => {
                self.geo.poi_record(i).filter(|p| p.flags & POI_FLAG_SECONDARY_RING == 0).map(SourceRow::Poi)
            }
            _ => None,
        }
    }
    fn kind(e: EntityRef) -> crate::model::Kind {
        match e.kind() {
            EntityKind::Place => crate::model::Kind::Place,
            EntityKind::Area => crate::model::Kind::Area,
            EntityKind::Poi => crate::model::Kind::Poi,
            _ => unreachable!(),
        }
    }
    pub(super) fn tags(&self, e: EntityRef) -> Option<TagBundle<'_>> {
        self.files.bundle(Self::kind(e), e.record())
    }
    pub(super) fn importance(&self, e: EntityRef, row: SourceRow, cc: &str) -> u32 {
        self.files.wiki_importance(Self::kind(e), e.record()).unwrap_or_else(|| {
            let rank = row.ranks(cc).0;
            ((DEFAULT_IMPORTANCE_BASE - f64::from(rank) / IMPORTANCE_RANK_DIVISOR) / IMP_UNIT).round() as u32
        })
    }
}
impl Entities for CoreRecords<'_> {
    fn read(&self, e: EntityRef) -> Option<EntityRecord<'_>> {
        self.read_in_chain(e, None)
    }
    fn read_in_chain(&self, e: EntityRef, outer_slot: Option<u32>) -> Option<EntityRecord<'_>> {
        let row = self.row(e)?;
        let point = row.point(self.geo)?;
        // A point chain starts with its containing country. An area's parent chain can
        // cross a border away from its label point, and a closed country way can have no rank.
        let mut country_code = row
            .country()
            .or_else(|| {
                if matches!(row, SourceRow::Area(_)) {
                    return None;
                }
                let slot = outer_slot?;
                let poly = self.geo.admin_polygon(slot)?;
                if poly.admin_level != COUNTRY_LEVEL
                    || query_server::polygon_field(&query_server::RankedPoly::of(slot, poly)) != Some("country")
                {
                    return None;
                }
                query_server::country_code_bytes(poly.country_code)
            })
            .or_else(|| self.geo.find_admin(point.0, point.1).country_code);
        if let Some(cc) = &mut country_code {
            cc.make_ascii_lowercase();
        }
        let cc = country_code.unwrap_or_default().map(|b| b.to_ascii_uppercase());
        let cc = std::str::from_utf8(&cc).unwrap_or("");
        let (rank_search, rank_address) = row.ranks(cc);
        let (class, typ) = match row {
            SourceRow::Place(p) => ("place", PLACE_NODE_TYPES[p.place_type as usize]),
            SourceRow::Area(a) if a.admin_level == PLACE_AREA_LEVEL => {
                ("place", query_server::place_type_to_field(a.place_type_override).unwrap_or("locality"))
            }
            SourceRow::Area(_) => ("boundary", "administrative"),
            SourceRow::Poi(p) => (
                query_server::poi_category_to_osm_class(p.category).unwrap_or("place"),
                self.geo.poi_meta.category_name(p.category),
            ),
        };
        let name = self.geo.get_string(row.name_id());
        if name.is_empty() {
            return None;
        }
        let bbox = match row {
            SourceRow::Area(_) => self.areas.and_then(|areas| areas.bbox(e.record())),
            _ => None,
        };
        let osm = self.osm[e.kind() as usize].as_ref().and_then(|map| map.get(e.record()));
        Some(EntityRecord {
            point,
            bbox,
            category: Category { class: class.into(), typ: typ.into() },
            rank_search,
            rank_address,
            importance: self.importance(e, row, cc) as f64 * IMP_UNIT,
            country_code,
            name,
            tags: self.tags(e),
            osm,
        })
    }
}
