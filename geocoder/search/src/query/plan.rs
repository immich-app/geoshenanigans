//! A query's searches, as its reading hands them to their execution: Nominatim's abstract
//! searches (search/db_searches/) and the fields they are built of (search/db_search_fields.py),
//! reimplemented from what they do.
use crate::constants::{SEARCH_PRIO_COUNTRY, SEARCH_PRIO_NAME, SEARCH_PRIO_OTHER};
use crate::index::entities::Category;
use crate::index::terms::Term;

/// A term of the query: its place among the plan's terms (`SearchPlan::terms`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TermId(pub u32);

/// What a lookup reads of an entity: the words of its names, of its address (Nominatim's
/// `name_vector` and `nameaddress_vector`), or of both together, as the postcode search reads a
/// postcode's parent (db_searches/postcode_search.py:83-88).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Field {
    Name,
    Address,
    NameAndAddress,
}

/// How a lookup's terms match (search/db_search_lookups.py).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LookupKind {
    /// Every term, its posting lists finding the candidates.
    All,
    /// One term at least, its posting lists finding the candidates.
    Any,
    /// Every term, checked on the candidates the other lookups found.
    Restrict,
}

/// Terms to look up in a field (FieldLookup).
#[derive(Clone, Debug, PartialEq)]
pub struct FieldLookup {
    pub field: Field,
    pub terms: Vec<TermId>,
    pub kind: LookupKind,
}

/// Terms an entity matches at a penalty when it has them all (RankedTokens).
#[derive(Clone, Debug, PartialEq)]
pub struct RankedTerms {
    pub penalty: f64,
    pub terms: Vec<TermId>,
}

/// Rankings of a field, tried in order until one matches: an entity pays its penalty, or the
/// default when none matches (FieldRanking).
#[derive(Clone, Debug, PartialEq)]
pub struct FieldRanking {
    pub field: Field,
    pub default: f64,
    pub rankings: Vec<RankedTerms>,
}

/// Values a result may have, each at its penalty (WeightedStrings, WeightedCategories).
#[derive(Clone, Debug, PartialEq)]
pub struct Weighted<T>(pub Vec<(T, f64)>);

impl<T> Default for Weighted<T> {
    fn default() -> Self {
        Weighted(Vec::new())
    }
}

impl<T: PartialEq> Weighted<T> {
    /// The penalty of `value`; None for one not among them.
    pub fn penalty_of(&self, value: &T) -> Option<f64> {
        self.0.iter().find(|(v, _)| v == value).map(|&(_, penalty)| penalty)
    }
}

/// What a search looks up and ranks by (SearchData).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchData {
    pub lookups: Vec<FieldLookup>,
    pub rankings: Vec<FieldRanking>,
    /// Canonical house numbers.
    pub housenumbers: Weighted<String>,
    /// Normalised postcodes.
    pub postcodes: Weighted<String>,
    /// Lower-case ISO 3166-1 alpha-2 codes.
    pub countries: Weighted<[u8; 2]>,
    pub qualifiers: Weighted<Category<'static>>,
}

/// One search of a query (search/db_searches/), its penalty everything its reading charged.
#[derive(Clone, Debug, PartialEq)]
pub enum Search {
    /// Countries by their names and codes (CountrySearch).
    Country { penalty: f64, countries: Weighted<[u8; 2]> },
    /// Postcodes, and the address around them (PostcodeSearch).
    Postcode { penalty: f64, data: SearchData },
    /// Entities by their names (PlaceSearch).
    Place { penalty: f64, data: SearchData, expected_count: u64, has_address_terms: bool },
    /// Streets, and house numbers on them (AddressSearch).
    Address { penalty: f64, data: SearchData, expected_count: u64, has_address_terms: bool },
    /// Entities of a category in the request's box or near its point (PoiSearch).
    Poi { penalty: f64, qualifiers: Weighted<Category<'static>>, countries: Weighted<[u8; 2]> },
    /// Entities of a category near what another search finds (NearSearch).
    Near { penalty: f64, categories: Weighted<Category<'static>>, base: Box<Search> },
}

impl Search {
    pub fn penalty(&self) -> f64 {
        match self {
            Search::Country { penalty, .. }
            | Search::Postcode { penalty, .. }
            | Search::Place { penalty, .. }
            | Search::Address { penalty, .. }
            | Search::Poi { penalty, .. }
            | Search::Near { penalty, .. } => *penalty,
        }
    }

    /// Searches of one penalty run in this order, least first (SEARCH_PRIO).
    pub fn prio(&self) -> u8 {
        match self {
            Search::Country { .. } => SEARCH_PRIO_COUNTRY,
            Search::Place { .. } | Search::Address { .. } => SEARCH_PRIO_NAME,
            Search::Postcode { .. } | Search::Poi { .. } | Search::Near { .. } => SEARCH_PRIO_OTHER,
        }
    }
}

/// What a query's reading hands its execution: the terms its lookups found, and its searches.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchPlan {
    pub terms: Vec<Term>,
    pub searches: Vec<Search>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn place(penalty: f64) -> Search {
        Search::Place { penalty, data: SearchData::default(), expected_count: 1, has_address_terms: false }
    }

    #[test]
    fn countries_run_first_then_places_and_addresses_then_the_rest() {
        let searches = [
            Search::Country { penalty: 0.1, countries: Weighted::default() },
            place(0.2),
            Search::Address { penalty: 0.3, data: SearchData::default(), expected_count: 1, has_address_terms: true },
            Search::Postcode { penalty: 0.4, data: SearchData::default() },
            Search::Poi { penalty: 0.5, qualifiers: Weighted::default(), countries: Weighted::default() },
            Search::Near { penalty: 0.6, categories: Weighted::default(), base: Box::new(place(0.0)) },
        ];

        let ranked: Vec<(f64, u8)> = searches.iter().map(|s| (s.penalty(), s.prio())).collect();

        assert_eq!(ranked, [(0.1, 0), (0.2, 1), (0.3, 1), (0.4, 2), (0.5, 2), (0.6, 2)]);
    }

    #[test]
    fn a_weighted_value_has_its_penalty_and_another_none() {
        let numbers = Weighted(vec![("8a".to_string(), 0.0), ("8".to_string(), 0.3)]);

        assert_eq!(numbers.penalty_of(&"8".to_string()), Some(0.3));
        assert_eq!(numbers.penalty_of(&"8b".to_string()), None);
    }
}
