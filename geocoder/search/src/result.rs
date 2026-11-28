//! What a search answers: its results, best first, each what the outputs (Immich's eight
//! fields, Nominatim's jsonv2, Photon's GeoJSON) are drawn from, with the dataset for display.
use crate::index::entities::{BBox, Category, EntityRef, OsmRef};
use crate::index::streets::{NumberRow, NumberTarget};
use std::borrow::Cow;

/// The kind of a result: what the test page letters it by and the full output names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResultKind {
    Place,
    Area,
    Poi,
    /// A way of a street.
    Street,
    /// A house found by its number: an address point, POI or named address on its street, or an
    /// address point on its place.
    House,
    /// A number of an OSM interpolation line.
    Interpolation,
    /// A number of a TIGER range.
    Tiger,
    /// A postcode centroid or a postal area.
    Postcode,
    /// An address point found by its own name.
    NamedAddress,
}

impl ResultKind {
    pub const ALL: [ResultKind; 9] = [
        ResultKind::Place,
        ResultKind::Area,
        ResultKind::Poi,
        ResultKind::Street,
        ResultKind::House,
        ResultKind::Interpolation,
        ResultKind::Tiger,
        ResultKind::Postcode,
        ResultKind::NamedAddress,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ResultKind::Place => "place",
            ResultKind::Area => "area",
            ResultKind::Poi => "poi",
            ResultKind::Street => "street",
            ResultKind::House => "house",
            ResultKind::Interpolation => "interpolation",
            ResultKind::Tiger => "tiger",
            ResultKind::Postcode => "postcode",
            ResultKind::NamedAddress => "named_address",
        }
    }

    /// Its letter on the test page and in a debug answer.
    pub fn letter(self) -> char {
        match self {
            ResultKind::Place => 'P',
            ResultKind::Area => 'A',
            ResultKind::Poi => 'O',
            ResultKind::Street => 'S',
            ResultKind::House => 'H',
            ResultKind::Interpolation => 'I',
            ResultKind::Tiger => 'T',
            ResultKind::Postcode => 'Z',
            ResultKind::NamedAddress => 'N',
        }
    }
}

/// What a result is, by the records it is read from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Target {
    /// An entity found by its names: a place, area, POI, address point that is an entity (one
    /// numbered on a place, whose number is a name of its own, among them) or postcode.
    Entity(EntityRef),
    /// Way `way` (street_ways.bin) of street `street`.
    Way { street: EntityRef, way: u32 },
    /// What a house number on way `way` of street `street` numbers. The way is the one the index
    /// filed the house on, which can differ from the parent street its record holds.
    House { street: EntityRef, way: u32, target: NumberTarget },
    /// `number` of interpolation `interpolation` (interp_ways.bin), an OSM line or a TIGER range,
    /// on way `way` of street `street`.
    Interpolated { street: EntityRef, way: u32, interpolation: u32, number: u32 },
}

impl Target {
    /// The house of number row `row` of street `street`.
    pub fn house(street: EntityRef, row: &NumberRow) -> Target {
        Target::House { street, way: row.way, target: row.target }
    }
}

/// One answer of a search, as the search ranked it.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchResult<'a> {
    pub kind: ResultKind,
    pub target: Target,
    /// Where it is, (lat, lon): its record's point, or an interpolated number's place on its line.
    pub point: (f64, f64),
    /// The name it is shown by, in the request's languages.
    pub name: Cow<'a, str>,
    pub category: Category<'a>,
    pub rank_search: u8,
    pub rank_address: u8,
    pub importance: f64,
    /// Lower case, as Nominatim's country_code.
    pub country_code: Option<[u8; 2]>,
    /// The number of a house, interpolation or TIGER answer, and of an entity found by it.
    pub housenumber: Option<Cow<'a, str>>,
    pub bbox: Option<BBox>,
    pub osm: Option<OsmRef>,
    /// Nominatim's accuracy: the search's penalty and the result's own penalties.
    pub accuracy: f64,
    /// What results are ordered by, least first: accuracy less importance, then the rerank's.
    pub ranking: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::entities::EntityKind;

    #[test]
    fn each_kind_has_a_name_and_a_letter_of_its_own() {
        let named: Vec<(&str, char)> = ResultKind::ALL.iter().map(|k| (k.as_str(), k.letter())).collect();

        assert_eq!(
            named,
            [
                ("place", 'P'),
                ("area", 'A'),
                ("poi", 'O'),
                ("street", 'S'),
                ("house", 'H'),
                ("interpolation", 'I'),
                ("tiger", 'T'),
                ("postcode", 'Z'),
                ("named_address", 'N'),
            ]
        );
    }

    #[test]
    fn a_house_keeps_the_way_its_number_was_found_on_whatever_it_numbers() {
        let street = EntityRef::new(EntityKind::Street, 7).unwrap();
        let poi = EntityRef::new(EntityKind::Poi, 3).unwrap();

        for target in [NumberTarget::Entity(poi), NumberTarget::Address(9)] {
            let row = NumberRow { number: "1044".into(), way: 12, target, partial: false };

            assert_eq!(Target::house(street, &row), Target::House { street, way: 12, target });
        }
    }
}
