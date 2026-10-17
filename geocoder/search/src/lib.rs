//! Geocoder-native forward place search (prototype): an fst over folded names with
//! importance-sorted postings, Levenshtein-automaton typo and prefix lookups, and trailing
//! query words matched against each place's country/state/county/city context.
pub mod constants;
pub mod index;
pub mod normalize;
pub mod query;
