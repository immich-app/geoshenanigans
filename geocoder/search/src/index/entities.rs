//! What a format-3 index names: entities, each a kind and a record of that kind, and what a
//! search reads of one (its record) through an `Entities` resolver.
use crate::constants::ENTITY_RECORD_BITS;
use crate::tags::TagBundle;
use std::borrow::Cow;

/// The kind of record an entity is. Its number is the kind's 3 bits in an index row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EntityKind {
    /// A place node (place_nodes.bin).
    Place = 0,
    /// An admin boundary or a place area (admin_polygons.bin).
    Area = 1,
    /// A POI (poi_records.bin).
    Poi = 2,
    /// A street of the index's street table: named ways of one name, class and type, rank,
    /// area and postcode, whose words it holds once.
    Street = 3,
    /// A named address (addr_points.bin).
    Address = 4,
    /// A postcode centroid (postcode_centroids.bin).
    Postcode = 5,
    /// A postal area (admin_polygons.bin at the postal level).
    PostalArea = 6,
}

impl EntityKind {
    pub const ALL: [EntityKind; 7] = [
        EntityKind::Place,
        EntityKind::Area,
        EntityKind::Poi,
        EntityKind::Street,
        EntityKind::Address,
        EntityKind::Postcode,
        EntityKind::PostalArea,
    ];
}

/// An entity: a kind and its record among the records of that kind. Entities order by (kind,
/// record), the order an index breaks a sort's ties in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityRef {
    kind: EntityKind,
    record: u32,
}

impl EntityRef {
    /// The most records of one kind an index row names.
    pub const MAX_RECORD: u32 = (1 << ENTITY_RECORD_BITS) - 1;

    /// Record `record` of `kind`; None for one past MAX_RECORD.
    pub fn new(kind: EntityKind, record: u32) -> Option<EntityRef> {
        (record <= Self::MAX_RECORD).then_some(EntityRef { kind, record })
    }

    pub fn kind(self) -> EntityKind {
        self.kind
    }

    pub fn record(self) -> u32 {
        self.record
    }

    /// Its index row: its kind in the top 3 bits, its record below them. Rows order as entities
    /// do.
    pub fn packed(self) -> u32 {
        (self.kind as u32) << ENTITY_RECORD_BITS | self.record
    }

    /// The entity of the index row `row`; None for a row of no kind.
    pub fn unpack(row: u32) -> Option<EntityRef> {
        let kind = *EntityKind::ALL.get((row >> ENTITY_RECORD_BITS) as usize)?;
        Some(EntityRef { kind, record: row & Self::MAX_RECORD })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OsmKind {
    Node,
    Way,
    Relation,
}

/// An OSM object, as the dataset's row maps give a record's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OsmRef {
    pub kind: OsmKind,
    pub id: u64,
}

/// An OSM main tag, Nominatim's class and type (`amenity=cafe`, `place=city`,
/// `highway=residential`).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Category<'a> {
    pub class: Cow<'a, str>,
    pub typ: Cow<'a, str>,
}

/// A box in degrees, its edges inside it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BBox {
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,
}

/// What a search reads of an entity: what it ranks, filters and answers it by.
#[derive(Clone, Debug, PartialEq)]
pub struct EntityRecord<'a> {
    /// Its point, (lat, lon): a node's, a polygon's label point, a street's middle.
    pub point: (f64, f64),
    pub bbox: Option<BBox>,
    pub category: Category<'a>,
    pub rank_search: u8,
    pub rank_address: u8,
    /// Nominatim's importance, computed from the record and its side files as it is read.
    pub importance: f64,
    /// Lower case, as Nominatim's country_code.
    pub country_code: Option<[u8; 2]>,
    /// The name its record holds (`name:en` else `name`); empty for none.
    pub name: Cow<'a, str>,
    /// Its tags; None for a record with nothing beyond its name.
    pub tags: Option<TagBundle<'a>>,
    /// Its OSM object, where the dataset's row maps give it.
    pub osm: Option<OsmRef>,
}

/// What resolves entities to their records: the dataset's files beside an index, or an
/// in-memory table.
pub trait Entities {
    /// The record of `e`; None for one that does not read (a damaged file, or a record past the
    /// end of its file): a search leaves such an entity out.
    fn read(&self, e: EntityRef) -> Option<EntityRecord<'_>>;
}

/// Records held in memory, for the tests of what reads them.
#[cfg(test)]
#[derive(Default)]
pub struct MemEntities(std::collections::BTreeMap<EntityRef, EntityRecord<'static>>);

#[cfg(test)]
impl MemEntities {
    pub fn insert(&mut self, e: EntityRef, record: EntityRecord<'static>) {
        self.0.insert(e, record);
    }
}

#[cfg(test)]
impl Entities for MemEntities {
    fn read(&self, e: EntityRef) -> Option<EntityRecord<'_>> {
        self.0.get(&e).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_row_packs_its_kind_above_its_record() {
        for (kind, record, row) in [
            (EntityKind::Place, 0, 0),
            (EntityKind::Area, 5, 0x2000_0005),
            (EntityKind::Poi, EntityRef::MAX_RECORD, 0x5FFF_FFFF),
            (EntityKind::PostalArea, 1, 0xC000_0001),
        ] {
            let e = EntityRef::new(kind, record).unwrap();

            assert_eq!(e.packed(), row, "{kind:?}");
            assert_eq!(EntityRef::unpack(row), Some(e), "{kind:?}");
        }
    }

    #[test]
    fn every_kind_round_trips_in_its_order() {
        let rows: Vec<u32> = EntityKind::ALL.iter().map(|&k| EntityRef::new(k, 7).unwrap().packed()).collect();

        assert!(rows.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(rows.iter().map(|&r| EntityRef::unpack(r).unwrap().kind()).collect::<Vec<_>>(), EntityKind::ALL);
    }

    #[test]
    fn rows_order_as_their_entities() {
        let a = EntityRef::new(EntityKind::Place, EntityRef::MAX_RECORD).unwrap();
        let b = EntityRef::new(EntityKind::Area, 0).unwrap();

        assert!(a < b);
        assert!(a.packed() < b.packed());
    }

    #[test]
    fn a_record_past_the_row_or_a_kind_past_the_kinds_is_none() {
        assert_eq!(EntityRef::new(EntityKind::Poi, EntityRef::MAX_RECORD + 1), None);
        assert_eq!(EntityRef::unpack(0xE000_0000), None);
        assert_eq!(EntityRef::unpack(u32::MAX), None);
    }

    #[test]
    fn an_in_memory_table_reads_what_it_holds_and_nothing_else() {
        let mut ents = MemEntities::default();
        let poi = EntityRef::new(EntityKind::Poi, 3).unwrap();
        let record = EntityRecord {
            point: (30.4, -97.8),
            bbox: None,
            category: Category { class: "office".into(), typ: "company".into() },
            rank_search: 30,
            rank_address: 30,
            importance: 0.1,
            country_code: Some(*b"us"),
            name: "FUTO".into(),
            tags: TagBundle::parse(crate::tags::encode([("name", "FUTO")]).unwrap()),
            osm: Some(OsmRef { kind: OsmKind::Way, id: 382784046 }),
        };
        ents.insert(poi, record.clone());

        assert_eq!(ents.read(poi), Some(record));
        assert_eq!(ents.read(EntityRef::new(EntityKind::Poi, 4).unwrap()), None);
    }
}
