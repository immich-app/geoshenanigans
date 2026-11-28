//! What a search answers: its results, best first, each what the outputs (Immich's eight
//! fields, Nominatim's jsonv2, Photon's GeoJSON) are drawn from, with the dataset for display.
use crate::index::entities::{BBox, Category, EntityRef, OsmRef};
use std::borrow::Cow;

/// The kind of a result: what the test page letters it by and the full output names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResultKind {
    Place,
    Area,
    Poi,
    /// A way of a street.
    Street,
    /// An address point found by its number: on its street, or on its place.
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
    /// An entity found by its names or its own number: a place, area, POI, address point that is
    /// an entity (one numbered on a place among them) or postcode.
    Entity(EntityRef),
    /// Way `way` (street_ways.bin) of street `street`.
    Way { street: EntityRef, way: u32 },
    /// Address point `address` (addr_points.bin), on way `way` of street `street`.
    House { street: EntityRef, way: u32, address: u32 },
    /// `number` of interpolation `interpolation` (interp_ways.bin), an OSM line or a TIGER range,
    /// on way `way` of street `street`.
    Interpolated { street: EntityRef, way: u32, interpolation: u32, number: u32 },
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
}
