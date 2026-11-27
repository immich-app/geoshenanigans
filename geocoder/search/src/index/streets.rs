//! A street's ways, house numbers and interpolations, as the address search reads them. An index
//! stores a street once for the words of its ways; Nominatim's candidates, precedence and
//! answers stay per way, so every row here names its way.
use crate::index::entities::{BBox, Category, EntityRef};
use std::borrow::Cow;

/// A way of a street.
#[derive(Clone, Debug, PartialEq)]
pub struct StreetWay<'a> {
    /// Its row in street_ways.bin.
    pub way: u32,
    pub category: Category<'a>,
    pub rank_search: u8,
    pub bbox: BBox,
    /// The areas (admin polygons) one of its vertices lies in that its street's chain leaves out.
    pub exceptions: Vec<u32>,
}

/// What has a house number on a street.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NumberTarget {
    /// An entity: a POI or a named address.
    Entity(EntityRef),
    /// An address point (addr_points.bin) that is no entity.
    Address(u32),
}

/// A house number on a way of a street.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NumberRow<'a> {
    /// The number as its object has it, canonical (housenumbers.py:40-64): "8a", "12-14".
    pub number: Cow<'a, str>,
    pub way: u32,
    pub target: NumberTarget,
    /// Whether the row was found by one word of its number ("12" of "12-14"), which costs more
    /// than the whole number (address_search.py:345-347).
    pub partial: bool,
}

/// Where a range of house numbers comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RangeSource {
    /// An OSM interpolation line.
    Osm,
    /// A TIGER range.
    Tiger,
}

/// A range of house numbers on a way of a street: start to end, every step-th.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct InterpolationRow {
    /// Its row in interp_ways.bin.
    pub interpolation: u32,
    pub way: u32,
    pub source: RangeSource,
    pub start: u32,
    pub end: u32,
    pub step: u32,
}

/// What the address search reads a street by: the index's street table.
pub trait StreetIndex {
    /// The ways of `street`; none for an entity that is no street or does not read.
    fn ways(&self, street: EntityRef) -> Vec<StreetWay<'_>>;
    /// The rows of `street` with the canonical number `number`, whole or as one of its words.
    fn numbers(&self, street: EntityRef, number: &str) -> Vec<NumberRow<'_>>;
    /// The ranges of house numbers on `street`.
    fn interpolations(&self, street: EntityRef) -> Vec<InterpolationRow>;
}
