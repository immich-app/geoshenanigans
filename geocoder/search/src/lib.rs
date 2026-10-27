//! Geocoder-native forward place search over the geocoder's own files. The index is an fst over
//! folded names plus compact postings that point at existing records (place nodes, boundary
//! polygons, POI records); names come from the existing string tiers and context from reverse
//! geocoding. Built from the installed files (or shipped), memory-mapped at query time.
pub mod build;
pub mod constants;
pub mod entities;
pub mod extras;
#[cfg(test)]
mod fixture;
pub mod model;
pub mod normalize;
pub mod query;
pub mod reader;
pub mod varint;
