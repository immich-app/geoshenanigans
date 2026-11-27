//! The text of names and queries: folding, and the forms Nominatim's tokenizer gives them.
//!
//! Where these modules follow Nominatim, they reimplement its behaviour as observed and as its
//! documentation and settings describe it; none of its GPL-3.0 source is copied.
pub mod difflib;
pub mod housenumber;
mod latin_ascii;
pub mod normalize;
mod unicode;
