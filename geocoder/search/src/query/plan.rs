//! A query's searches, as its reading hands them to their execution: Nominatim's abstract
//! searches (search/db_searches/) and the fields they are built of (search/db_search_fields.py),
//! and the reading that builds them from a request (search/db_search_builder.py,
//! search/geocoder.py:90-118, 266-292), reimplemented from what they do.
use super::assign::{assignments, Assignment};
use super::lookup::{tokens, Lookup, QueryGraph, Span, Token, TokenType};
use super::tokenize::{front_door, split, FrontDoor, ReadError};
use crate::constants::*;
use crate::index::entities::Category;
use crate::index::terms::{Term, TermInfo, TermSource};
use crate::profile::{Layer, MatchMode, SearchRequest, Viewbox};
use std::cmp::Reverse;
use std::collections::BinaryHeap;

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

/// A point the query's text named, and the radius in degrees around it results lie within:
/// Nominatim's `near` and `near_radius` from coordinates in the query (v1/server_glue.py:303-330).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NearPoint {
    pub lat: f64,
    pub lon: f64,
    pub radius: f64,
}

/// What a query's reading hands its execution: the terms its lookups found, the point its text
/// named, and its searches, cheapest first; and its tokens, for a debug answer.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchPlan {
    pub terms: Vec<Term>,
    pub near: Option<NearPoint>,
    /// Whether the query is coordinates alone: its answer is the reverse lookup at `near`, as
    /// Nominatim's (v1/server_glue.py:316-325).
    pub reverse: bool,
    pub searches: Vec<Search>,
    pub tokens: Vec<TokenRow>,
}

/// A token of the query as Nominatim's debug page lists it: partials first in each node, then the
/// tokens that start there.
#[derive(Clone, Debug, PartialEq)]
pub struct TokenRow {
    pub ttype: TokenType,
    pub span: Span,
    pub term: TermId,
    /// The word it is matched as: a partial's as the query wrote it, else its term's lookup word.
    pub lookup_word: String,
    pub penalty: f64,
}

/// Which reading of a request: the first, or the photon mode's second when the first finds
/// nothing, which also takes near spellings of its words.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pass {
    First,
    Typo,
}

/// The plan of `req`'s query, its terms looked up in `source`. In the nominatim mode the query
/// goes through Nominatim's front door first: coordinates become the plan's point, and a
/// `[class=type]` makes every search one for that category near what it finds (or, with no other
/// text, around the point or in the bounded viewbox).
pub fn read(req: &SearchRequest, source: &dyn TermSource, pass: Pass) -> Result<SearchPlan, ReadError> {
    if req.q.is_empty() {
        return Ok(SearchPlan::default());
    }

    let (door, lookup) = match (req.mode, pass) {
        (MatchMode::Nominatim, _) => (front_door(&req.q), Lookup::Exact),
        (MatchMode::Photon, pass) => {
            let lookup = if pass == Pass::Typo { Lookup::Typo } else { Lookup::Prefix };
            (FrontDoor { q: req.q.clone(), near: None, category: None }, lookup)
        }
    };
    if door.near.is_some() && door.q.is_empty() && door.category.is_none() {
        return Ok(SearchPlan { near: door.near, reverse: true, ..SearchPlan::default() });
    }

    let details = Details::of(req, door.near);
    let Some(category) = door.category else {
        if details.impossible {
            return Ok(SearchPlan::default());
        }
        return details.plan(&door.q, source, lookup, door.near);
    };
    let categories = Weighted(vec![(category, 0.0)]);
    if door.q.is_empty() {
        let poi = Search::Poi { penalty: 0.0, qualifiers: categories, countries: details.countries.clone() };
        return Ok(SearchPlan { near: door.near, searches: vec![poi], ..SearchPlan::default() });
    }
    let mut plan = details.plan(&door.q, source, lookup, door.near)?;
    plan.searches.truncate(SEARCHES_MAX);
    plan.searches = plan
        .searches
        .into_iter()
        .map(|s| Search::Near { penalty: s.penalty(), categories: categories.clone(), base: Box::new(s) })
        .collect();
    Ok(plan)
}

/// What of a request the searches are built with, as Nominatim's SearchDetails holds it.
struct Details {
    countries: Weighted<[u8; 2]>,
    categories: Vec<Category<'static>>,
    /// Whether results may be addresses, postcodes and countries (DataLayer.ADDRESS).
    address_layer: bool,
    /// Whether a search for a category alone has an area to search: a bounded viewbox or a
    /// point (db_search_builder.py:126-128).
    poi_area: bool,
    /// Whether the request can find nothing (types.py:746-756): it allows no layer, or its
    /// bounded viewbox holds the query's point, as Nominatim tests it.
    impossible: bool,
}

impl Details {
    fn of(req: &SearchRequest, near: Option<NearPoint>) -> Details {
        let bounded = match req.viewbox {
            Some(Viewbox::Bounded(b)) => Some(b),
            _ => None,
        };
        let holds_near =
            bounded.zip(near).is_some_and(|(b, p)| (b.min_lat..=b.max_lat).contains(&p.lat) && (b.min_lon..=b.max_lon).contains(&p.lon));
        Details {
            countries: Weighted(req.countries.iter().map(|&c| (c, 0.0)).collect()),
            categories: req.categories.iter().cloned().collect(),
            address_layer: req.layers.as_ref().is_none_or(|l| l.contains(&Layer::Address)),
            poi_area: bounded.is_some() || near.is_some(),
            impossible: req.layers.as_ref().is_some_and(|l| l.is_empty()) || holds_near,
        }
    }

    /// Every search of every reading of `q`, cheapest first, searches of one penalty in
    /// SEARCH_PRIO order (geocoder.py:90-118).
    fn plan(&self, q: &str, source: &dyn TermSource, lookup: Lookup, near: Option<NearPoint>) -> Result<SearchPlan, ReadError> {
        let g = tokens(split(q)?, source, lookup);
        let builder = Builder { g: &g, details: self };
        let mut searches: Vec<Search> = assignments(&g).iter().flat_map(|a| builder.build(a)).collect();
        searches.sort_by(|a, b| a.penalty().total_cmp(&b.penalty()).then(a.prio().cmp(&b.prio())));
        let rows = token_rows(&g);
        Ok(SearchPlan { terms: g.terms, near, reverse: false, searches, tokens: rows })
    }
}

fn token_rows(g: &QueryGraph) -> Vec<TokenRow> {
    let mut rows = Vec::new();
    for (i, n) in g.nodes.iter().enumerate() {
        if let Some(p) = n.partial {
            rows.push(TokenRow {
                ttype: TokenType::Partial,
                span: Span::new(i, i + 1),
                term: p.term,
                lookup_word: n.node.word.clone(),
                penalty: p.penalty,
            });
        }
        for list in &n.starting {
            for t in &list.tokens {
                let lookup_word = g.term(t.term).lookup_word.clone();
                rows.push(TokenRow { ttype: list.ttype, span: Span::new(i, list.end), term: t.term, lookup_word, penalty: t.penalty });
            }
        }
    }
    rows
}

/// A search's data as a reading builds it: the data and what it costs so far (SearchData).
#[derive(Clone)]
struct Draft {
    penalty: f64,
    data: SearchData,
}

/// The searches of each reading of a query (SearchBuilder).
struct Builder<'a> {
    g: &'a QueryGraph,
    details: &'a Details,
}

impl Builder<'_> {
    /// The searches of reading `a` (db_search_builder.py:82-120). The reading's penalty is in its
    /// data's and is added once more, as Nominatim adds it.
    fn build(&self, a: &Assignment) -> Vec<Search> {
        let Some(mut draft) = self.draft(a) else { return Vec::new() };
        let mut near = self.near_items(a);
        if near.as_ref().is_some_and(|n| n.0.is_empty()) {
            return Vec::new();
        }

        let searches = match a.name {
            None if near.is_some() && draft.data.postcodes.0.is_empty() => {
                draft.data.qualifiers = near.take().expect("near items");
                self.poi_search(draft)
            }
            None if a.housenumber.is_some() => self.housenumber_search(draft, a),
            None => self.special_search(draft, &a.address, near.is_some()),
            Some(name) => self.name_search(draft, name, &a.address, near.is_some()),
        };
        match near {
            Some(mut categories) => {
                let least = categories.0.iter().map(|&(_, p)| p).fold(f64::INFINITY, f64::min);
                for (_, p) in &mut categories.0 {
                    *p -= least;
                }
                searches
                    .into_iter()
                    .map(|mut s| {
                        let penalty = least + a.penalty + s.penalty();
                        *s.penalty_mut() = 0.0;
                        Search::Near { penalty, categories: categories.clone(), base: Box::new(s) }
                    })
                    .collect()
            }
            None => searches
                .into_iter()
                .map(|mut s| {
                    *s.penalty_mut() += a.penalty;
                    s
                })
                .collect(),
        }
    }

    /// The reading's countries, house numbers, postcodes, qualifiers and address rankings, each at
    /// what it costs (db_search_builder.py:363-403); None for a reading of no allowed country or
    /// qualifier.
    fn draft(&self, a: &Assignment) -> Option<Draft> {
        let mut d = Draft { penalty: a.penalty, data: SearchData::default() };
        if let Some(span) = a.country {
            let tokens: Vec<Token> = self.g.tokens(span, TokenType::Country).iter().copied().filter(|t| self.allowed_country(t)).collect();
            let least = least_penalty(&tokens)?;
            d.penalty += least;
            let mut countries: Vec<([u8; 2], f64)> = Vec::new();
            for t in &tokens {
                let TermInfo::Country(cc) = self.g.term(t.term).info else { continue };
                match countries.iter_mut().find(|(c, _)| *c == cc) {
                    Some((_, p)) => *p = p.min(t.penalty - least),
                    None => countries.push((cc, t.penalty - least)),
                }
            }
            d.data.countries = Weighted(countries);
            d.penalty += self.g.in_word_penalty(span);
        } else {
            d.data.countries = self.details.countries.clone();
        }
        if let Some(span) = a.housenumber {
            d.data.housenumbers = self.strings(&mut d.penalty, span, TokenType::HouseNumber);
            d.penalty += self.g.in_word_penalty(span);
        }
        if let Some(span) = a.postcode {
            d.data.postcodes = self.strings(&mut d.penalty, span, TokenType::Postcode);
            d.penalty += self.g.in_word_penalty(span);
        }
        if let Some(span) = a.qualifier {
            let tokens: Vec<Token> =
                self.g.tokens(span, TokenType::Qualifier).iter().copied().filter(|t| self.allowed_category(t)).collect();
            d.penalty += least_penalty(&tokens)?;
            d.data.qualifiers = self.categories(&tokens);
            d.penalty += self.g.in_word_penalty(span);
        } else {
            d.data.qualifiers = Weighted(self.details.categories.iter().map(|c| (c.clone(), 0.0)).collect());
        }
        for &span in &a.address {
            let mut ranking = self.address_ranking(span);
            if ranking.rankings.is_empty() {
                d.penalty += ranking.default;
            } else {
                d.penalty += ranking.normalize();
                d.data.rankings.push(ranking);
            }
        }
        Some(d)
    }

    fn allowed_country(&self, t: &Token) -> bool {
        let countries = &self.details.countries.0;
        countries.is_empty() || matches!(self.g.term(t.term).info, TermInfo::Country(cc) if countries.iter().any(|&(c, _)| c == cc))
    }

    fn allowed_category(&self, t: &Token) -> bool {
        let categories = &self.details.categories;
        categories.is_empty() || matches!(&self.g.term(t.term).info, TermInfo::Category(c) if categories.contains(c))
    }

    /// The lookup words of the tokens of `ttype` at `span`, each at its penalty over the least of
    /// them, which the draft pays (db_search_fields.py:234-245).
    fn strings(&self, penalty: &mut f64, span: Span, ttype: TokenType) -> Weighted<String> {
        let tokens = self.g.tokens(span, ttype);
        let Some(least) = least_penalty(tokens) else { return Weighted::default() };
        *penalty += least;
        Weighted(tokens.iter().map(|t| (self.g.term(t.term).lookup_word.clone(), t.penalty - least)).collect())
    }

    /// Each category of `tokens` at the least penalty of its tokens (db_search_fields.py:262-275,
    /// db_search_builder.py:432-442).
    fn categories(&self, tokens: &[Token]) -> Weighted<Category<'static>> {
        let mut out: Vec<(Category<'static>, f64)> = Vec::new();
        for t in tokens {
            let TermInfo::Category(c) = &self.g.term(t.term).info else { continue };
            match out.iter_mut().find(|(o, _)| o == c) {
                Some((_, p)) => *p = p.min(t.penalty),
                None => out.push((c.clone(), t.penalty)),
            }
        }
        Weighted(out)
    }

    /// The categories of the reading's near item, of the request's categories where it names
    /// some; None for a reading without one (db_search_builder.py:427-444).
    fn near_items(&self, a: &Assignment) -> Option<Weighted<Category<'static>>> {
        let span = a.near_item?;
        let tokens: Vec<Token> = self.g.tokens(span, TokenType::NearItem).iter().copied().filter(|t| self.allowed_category(t)).collect();
        Some(self.categories(&tokens))
    }

    /// A search for a category alone, which needs an area to search (:122-128).
    fn poi_search(&self, d: Draft) -> Vec<Search> {
        if !d.data.housenumbers.0.is_empty() || !self.details.poi_area {
            return Vec::new();
        }

        vec![Search::Poi { penalty: d.penalty, qualifiers: d.data.qualifiers, countries: d.data.countries }]
    }

    /// Searches without a name: for countries alone, or for postcodes with the address around
    /// them (:130-151).
    fn special_search(&self, mut d: Draft, address: &[Span], is_category: bool) -> Vec<Search> {
        if !d.data.qualifiers.0.is_empty() {
            return Vec::new();
        }

        let mut out = Vec::new();
        if !d.data.countries.0.is_empty() && address.is_empty() && d.data.postcodes.0.is_empty() && self.details.address_layer {
            out.push(Search::Country { penalty: d.penalty, countries: d.data.countries.clone() });
        }
        if !d.data.postcodes.0.is_empty() && (is_category || self.details.address_layer) {
            let extra = if d.data.countries.0.is_empty() { POSTCODE_SEARCH_NO_COUNTRY_PENALTY } else { 0.0 };
            if !address.is_empty() {
                let terms = address.iter().flat_map(|&s| self.g.partials(s)).map(|t| t.term).collect();
                d.data.lookups = vec![FieldLookup { field: Field::NameAndAddress, terms, kind: LookupKind::Restrict }];
            }
            out.push(Search::Postcode { penalty: d.penalty + extra, data: d.data });
        }
        out
    }

    /// A search for a house number that is itself the name, its address around it (:153-191).
    fn housenumber_search(&self, mut d: Draft, a: &Assignment) -> Vec<Search> {
        let partials = Counted::new(a.address.iter().flat_map(|&s| self.g.partials(s)), |t| self.g.term(t.term).addr_count);
        if partials.is_empty() {
            return Vec::new();
        }

        let numbers: &[Token] = a.housenumber.map_or(&[], |s| self.g.tokens(s, TokenType::HouseNumber));
        let number_terms: Vec<TermId> = numbers.iter().map(|t| t.term).collect();
        let mut expected: u64 = numbers.iter().map(|t| u64::from(self.g.term(t.term).count)).sum();
        d.data.lookups = if expected < HOUSENUMBER_LOOKUP_MAX_COUNT {
            vec![lookup(Field::Name, number_terms, LookupKind::Any), lookup(Field::Address, partials.terms(), LookupKind::Restrict)]
        } else if let Some(split) = partials.lookup_terms(LOOKUP_HOUSENUMBER_LIMIT, LOOKUP_HOUSENUMBER_FACTOR) {
            let mut lookups = partials.split(split, Field::Address);
            lookups.push(lookup(Field::Name, number_terms, LookupKind::Restrict));
            // Nominatim's count is a float here; rounding up keeps its "more than 10,000" tests.
            expected = partials.min_count().div_ceil(LOOKUP_HOUSENUMBER_FACTOR.pow((split - 1) as u32));
            lookups
        } else {
            let fulls: Vec<TermId> = self.g.tokens(a.address[0], TokenType::Word).iter().map(|t| t.term).collect();
            if fulls.len() > HOUSENUMBER_SEARCH_MAX_FULLS {
                return Vec::new();
            }
            vec![lookup(Field::Name, number_terms, LookupKind::Any), lookup(Field::Address, fulls, LookupKind::Any)]
        };
        d.data.housenumbers = Weighted::default();
        vec![Search::Place { penalty: d.penalty, data: d.data, expected_count: expected, has_address_terms: true }]
    }

    /// Searches for a name, with its house numbers an address search (:193-208).
    fn name_search(&self, mut d: Draft, name: Span, address: &[Span], is_category: bool) -> Vec<Search> {
        if !(is_category || d.data.housenumbers.0.is_empty() || self.details.address_layer) {
            return Vec::new();
        }

        let mut ranking = self.name_ranking(name);
        let name_penalty = ranking.normalize();
        if !ranking.rankings.is_empty() {
            d.data.rankings.push(ranking);
        }
        let has_address_terms = !address.is_empty();
        self.lookups(name, address)
            .into_iter()
            .map(|(extra, expected_count, lookups)| {
                let data = SearchData { lookups, ..d.data.clone() };
                let penalty = d.penalty + (extra + name_penalty);
                if data.housenumbers.0.is_empty() {
                    Search::Place { penalty, data, expected_count, has_address_terms }
                } else {
                    Search::Address { penalty, data, expected_count, has_address_terms }
                }
            })
            .collect()
    }

    /// The ways to look a name and its address up, each with its extra penalty and expected rows:
    /// the rarest terms drive, the others restrict (:210-297).
    fn lookups(&self, name: Span, address: &[Span]) -> Vec<(f64, u64, Vec<FieldLookup>)> {
        let names = Counted::new(self.g.partials(name), |t| self.g.term(t.term).count);
        let addresses = Counted::new(address.iter().flat_map(|&s| self.g.partials(s)), |t| self.g.term(t.term).addr_count);
        let fulls: Vec<(TermId, u64)> =
            self.g.tokens(name, TokenType::Word).iter().map(|t| (t.term, u64::from(self.g.term(t.term).count))).collect();
        let mut out = Vec::new();
        if addresses.is_empty() {
            if let Some(split) = names.lookup_terms(LOOKUP_NAME_LIMIT, LOOKUP_NAME_FACTOR) {
                out.push((0.0, names.expected(EXPECTED_NAME_FACTOR), names.split(split, Field::Name)));
                return out;
            }
            let rare: Vec<&(TermId, u64)> = fulls.iter().filter(|&&(_, c)| c < FULLS_NAME_MAX_COUNT).collect();
            if !rare.is_empty() {
                let terms = rare.iter().map(|&&(t, _)| t).collect();
                out.push((0.0, rare.iter().map(|&&(_, c)| c).sum(), by_any_name(terms, Vec::new())));
            }
            let expected = names.expected(EXPECTED_NAME_FACTOR);
            if expected < PARTIALS_LOOKUP_MAX_COUNT {
                out.push((PARTIALS_LOOKUP_PENALTY, expected, vec![lookup(Field::Name, names.terms(), LookupKind::All)]));
            }
            return out;
        }

        let mut penalty = 0.0;
        let mut name_split = names.lookup_terms(LOOKUP_ADDRESS_NAME_LIMIT, LOOKUP_ADDRESS_NAME_FACTOR);
        let mut addr_split = addresses.lookup_terms(LOOKUP_ADDRESS_LIMIT, LOOKUP_ADDRESS_FACTOR);
        if name_split.is_none() && addr_split.is_none() {
            if !fulls.is_empty() {
                let count: u64 = fulls.iter().map(|&(_, c)| c).sum();
                if count < FULLS_ADDRESS_MAX_COUNT {
                    out.push((0.0, count, by_any_name(fulls.iter().map(|&(t, _)| t).collect(), addresses.terms())));
                    penalty += FULLS_FIRST_PENALTY;
                }
            }
            penalty += FREQUENT_PENALTY;
            name_split = names.lookup_terms(LOOKUP_FREQUENT_NAME_LIMIT, LOOKUP_FREQUENT_NAME_FACTOR);
            addr_split = addresses.lookup_terms(LOOKUP_FREQUENT_ADDRESS_LIMIT, LOOKUP_FREQUENT_ADDRESS_FACTOR);
        }
        match (name_split, addr_split) {
            (Some(split), a) if a.is_none() || names.min_count() <= addresses.min_count() => {
                let mut lookups = names.split(split, Field::Name);
                lookups.push(lookup(Field::Address, addresses.terms(), LookupKind::Restrict));
                out.push((penalty, names.expected(EXPECTED_NAME_FACTOR), lookups));
            }
            (_, Some(split)) => {
                let mut lookups = addresses.split(split, Field::Address);
                lookups.push(lookup(Field::Name, names.terms(), LookupKind::Restrict));
                out.push((penalty, addresses.expected(EXPECTED_ADDRESS_FACTOR), lookups));
            }
            _ if names.len() > 1 => {
                let both = BOTH_PARTIALS_TERMS.min(names.len()) + BOTH_PARTIALS_TERMS.min(addresses.len());
                let expected = names.min_count().min(addresses.min_count()) / both as u64;
                if expected < BOTH_PARTIALS_MAX_COUNT {
                    let mut lookups = names.split(BOTH_PARTIALS_TERMS, Field::Name);
                    lookups.extend(addresses.split(BOTH_PARTIALS_TERMS, Field::Address));
                    out.push((penalty + BOTH_PARTIALS_PENALTY, expected, lookups));
                }
            }
            _ => {}
        }
        out
    }

    /// A name's ranking: each of its full names at its penalty and the in-word breaks it goes
    /// over, else its partials' penalties (:299-309).
    fn name_ranking(&self, span: Span) -> FieldRanking {
        let over = self.g.in_word_penalty(span);
        let mut rankings: Vec<RankedTerms> =
            self.g.tokens(span, TokenType::Word).iter().map(|t| RankedTerms { penalty: t.penalty + over, terms: vec![t.term] }).collect();
        rankings.sort_by(|a, b| a.penalty.total_cmp(&b.penalty));
        FieldRanking { field: Field::Name, default: self.g.partial_penalty(span) + RANKING_PARTIALS_PENALTY, rankings }
    }

    /// An address range's ranking: its covers by full names and partials, cheapest first, the
    /// cheapest by partials alone its default (:311-361).
    fn address_ranking(&self, span: Span) -> FieldRanking {
        let mut todo = BinaryHeap::new();
        todo.push(Reverse(Cover { at: span.start, penalty: 0.0, terms: Vec::new() }));
        let mut ranks: Vec<RankedTerms> = Vec::new();
        while let Some(Reverse(cover)) = todo.pop() {
            let at = cover.at;
            if let Some(partial) = self.g.partial(at) {
                if at + 1 < span.end {
                    let penalty = cover.penalty + partial.penalty + self.g.nodes[at + 1].node.word_break_penalty();
                    todo.push(Reverse(Cover { at: at + 1, penalty, terms: cover.terms.clone() }));
                } else {
                    ranks.push(RankedTerms { penalty: cover.penalty + partial.penalty, terms: cover.terms.clone() });
                }
            }
            for list in self.g.nodes[at].starting.iter().filter(|l| l.ttype == TokenType::Word) {
                let inside = self.g.in_word_penalty(Span::new(at, list.end));
                for t in &list.tokens {
                    let mut terms = cover.terms.clone();
                    terms.push(t.term);
                    if list.end < span.end {
                        let penalty = cover.penalty + t.penalty + (self.g.nodes[list.end].node.word_break_penalty() + inside);
                        todo.push(Reverse(Cover { at: list.end, penalty, terms }));
                    } else if list.end == span.end {
                        ranks.push(RankedTerms { penalty: cover.penalty + t.penalty + inside, terms });
                    }
                }
            }
            if ranks.len() >= RANKING_ADDRESS_MAX_VARIANTS {
                let default = self.g.partial_penalty(span) + RANKING_PARTIALS_PENALTY;
                ranks.push(RankedTerms { penalty: cover.penalty + default, terms: Vec::new() });
                break;
            }
        }
        ranks.sort_by_key(|r| r.terms.len());
        // Address ranges consist entirely of known partials. The cutoff supplies
        // their fallback if the all-partial cover is still on the heap.
        let default = ranks.remove(0).penalty + RANKING_ADDRESS_DEFAULT_PENALTY;
        ranks.sort_by(|a, b| a.penalty.total_cmp(&b.penalty));
        FieldRanking { field: Field::Address, default, rankings: ranks }
    }
}

/// A partial cover of an address range waiting in the walk's heap, ordered as Nominatim's heap
/// orders its tuples: the furthest node first, then the cheapest, then by its terms.
#[derive(Clone, Debug, PartialEq)]
struct Cover {
    at: usize,
    penalty: f64,
    terms: Vec<TermId>,
}

impl Eq for Cover {}

impl PartialOrd for Cover {
    fn partial_cmp(&self, other: &Cover) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Cover {
    fn cmp(&self, other: &Cover) -> std::cmp::Ordering {
        other.at.cmp(&self.at).then(self.penalty.total_cmp(&other.penalty)).then(self.terms.cmp(&other.terms))
    }
}

fn least_penalty(tokens: &[Token]) -> Option<f64> {
    tokens.iter().map(|t| t.penalty).reduce(f64::min)
}

fn lookup(field: Field, terms: Vec<TermId>, kind: LookupKind) -> FieldLookup {
    FieldLookup { field, terms, kind }
}

/// A name looked up by any of its full names, its address partials restricting
/// (db_search_fields.py:289-301).
fn by_any_name(names: Vec<TermId>, restrict: Vec<TermId>) -> Vec<FieldLookup> {
    let mut lookups = vec![lookup(Field::Name, names, LookupKind::Any)];
    if !restrict.is_empty() {
        lookups.push(lookup(Field::Address, restrict, LookupKind::Restrict));
    }
    lookups
}

/// Partial terms with their counts, the rarest first (CountedTokenIDs): a term counted once has
/// no statistics and sorts as COUNT_WITHOUT_STATISTICS, terms of one count by their id.
struct Counted(Vec<(u64, TermId)>);

impl Counted {
    fn new(tokens: impl Iterator<Item = Token>, count: impl Fn(&Token) -> u32) -> Counted {
        let mut terms: Vec<(u64, TermId)> = tokens.map(|t| (u64::from(count(&t)), t.term)).collect();
        terms.sort_unstable();
        terms.dedup();
        terms.sort_by_key(|&(c, t)| (if c > 1 { c } else { COUNT_WITHOUT_STATISTICS }, t));
        Counted(terms)
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// How many of the rarest terms drive a lookup so its rows stay below `limit`, which grows by
    /// `factor` with each term; None where no number does (db_search_fields.py:36-57).
    fn lookup_terms(&self, limit: u64, factor: u64) -> Option<usize> {
        let most = self.0.len().min(LOOKUP_TERMS_MAX);
        let least = self.min_count();
        if least == 1 {
            return Some(most);
        }

        let mut limit = limit;
        for i in 0..most {
            if least < limit {
                return Some(i + 1);
            }
            limit = limit.saturating_mul(factor);
        }
        None
    }

    fn min_count(&self) -> u64 {
        self.0.first().map_or(0, |&(c, _)| c)
    }

    /// The rows a lookup of all the terms is expected to find (db_search_fields.py:62-63).
    fn expected(&self, factor: u64) -> u64 {
        self.min_count() / factor.saturating_pow(self.len().saturating_sub(1) as u32)
    }

    fn terms(&self) -> Vec<TermId> {
        self.0.iter().map(|&(_, t)| t).collect()
    }

    /// The first `split` terms driving the lookup in `field`, the rest restricting it.
    fn split(&self, split: usize, field: Field) -> Vec<FieldLookup> {
        let (head, tail) = self.0.split_at(split.min(self.0.len()));
        let mut lookups = vec![lookup(field, head.iter().map(|&(_, t)| t).collect(), LookupKind::All)];
        if !tail.is_empty() {
            lookups.push(lookup(field, tail.iter().map(|&(_, t)| t).collect(), LookupKind::Restrict));
        }
        lookups
    }
}

impl FieldRanking {
    /// Takes the least of its penalties off all of them, which it returns (db_search_fields.py:169-181).
    fn normalize(&mut self) -> f64 {
        let least = self.rankings.iter().map(|r| r.penalty).fold(self.default, f64::min);
        if least > 0.0 {
            self.default -= least;
            for r in &mut self.rankings {
                r.penalty -= least;
            }
        }
        least
    }
}

impl Search {
    fn penalty_mut(&mut self) -> &mut f64 {
        match self {
            Search::Country { penalty, .. }
            | Search::Postcode { penalty, .. }
            | Search::Place { penalty, .. }
            | Search::Address { penalty, .. }
            | Search::Poi { penalty, .. }
            | Search::Near { penalty, .. } => penalty,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_category_at_coordinates_searches_for_pois_around_the_point() {
        let mut req = SearchRequest::new("[amenity=cafe] 30.26, -97.78", crate::profile::Profile::Full);
        req.mode = MatchMode::Nominatim;
        let plan = read(&req, &crate::index::terms::MemTerms::default(), Pass::First).unwrap();
        assert!(!plan.reverse);
        assert_eq!(plan.near.unwrap().lat, 30.26);
        assert!(matches!(plan.searches.as_slice(), [Search::Poi { qualifiers, .. }]
            if qualifiers.0[0].0.typ == "cafe"));
    }

    use crate::index::terms::{MemTerms, TermKind};
    use crate::profile::Profile;

    fn request(q: &str) -> SearchRequest {
        let mut req = SearchRequest::new(q, Profile::Full);
        req.mode = MatchMode::Nominatim;
        req
    }

    #[test]
    fn request_filters_apply_to_the_reading() {
        let mut terms = MemTerms::default();
        terms.insert(TermKind::Country, "congo", ("", ""), (1, 1), TermInfo::Country(*b"cd"));
        terms.insert(TermKind::Country, "congo", ("", ""), (1, 1), TermInfo::Country(*b"cg"));
        let mut req = request("congo");
        req.countries.insert(*b"cg");
        let plan = read(&req, &terms, Pass::First).unwrap();
        assert!(matches!(plan.searches.as_slice(), [Search::Country { countries, .. }]
            if countries.0 == [(*b"cg", 0.0)]));
        req.layers = Some([Layer::Poi].into());
        assert!(read(&req, &terms, Pass::First).unwrap().searches.is_empty());
        req.layers = None;
        req.countries = [*b"nl"].into();
        assert!(read(&req, &terms, Pass::First).unwrap().searches.is_empty());
    }

    #[test]
    fn a_phrase_only_poi_search_requires_a_location_and_an_allowed_category() {
        let mut req = request("Hotel");
        let terms = MemTerms::default();
        assert!(read(&req, &terms, Pass::First).unwrap().searches.is_empty());
        req.q = "Hotel 30.26, -97.78".into();
        let plan = read(&req, &terms, Pass::First).unwrap();
        assert!(matches!(plan.searches.as_slice(), [Search::Poi { qualifiers, .. }] if qualifiers.0[0].0.typ == "hotel"));
        req.categories.insert(Category { class: "amenity".into(), typ: "cafe".into() });
        assert!(read(&req, &terms, Pass::First).unwrap().searches.is_empty());
    }

    #[test]
    fn brackets_wrap_each_base_search_and_coordinates_alone_reverse() {
        let terms = crate::query::readings::reading("futo").terms();
        let plain = read(&request("FUTO Austin"), &terms, Pass::First).unwrap();
        let near = read(&request("[amenity=cafe] FUTO Austin"), &terms, Pass::First).unwrap();
        assert_eq!(near.searches.len(), plain.searches.len());
        for (actual, expected) in near.searches.iter().zip(plain.searches) {
            assert!(matches!(actual, Search::Near { base, penalty, categories }
                if **base == expected && *penalty == expected.penalty() && categories.0[0].0.typ == "cafe"));
        }
        let reverse = read(&request("30.26, -97.78"), &terms, Pass::First).unwrap();
        assert!(reverse.reverse && reverse.searches.is_empty());
        assert_eq!(reverse.near.unwrap().lon, -97.78);
    }

    #[test]
    fn only_photon_reads_completions_and_then_near_spellings() {
        let terms = crate::query::readings::reading("futo").terms();
        let mut req = request("futo austn");
        assert!(read(&req, &terms, Pass::Typo).unwrap().searches.is_empty());
        req.mode = MatchMode::Photon;
        assert!(read(&req, &terms, Pass::First).unwrap().searches.is_empty());
        assert!(!read(&req, &terms, Pass::Typo).unwrap().searches.is_empty());
        req.q = "futo aust".into();
        assert!(!read(&req, &terms, Pass::First).unwrap().searches.is_empty());
    }

    fn counted_graph(q: &str, rows: &[(TermKind, &str, &str, u32, u32)]) -> QueryGraph {
        let mut terms = MemTerms::default();
        for &(kind, token, word, count, addr) in rows {
            terms.insert(kind, token, (word, ""), (count, addr), TermInfo::None);
        }
        tokens(split(q).unwrap(), &terms, Lookup::Exact)
    }

    #[test]
    fn name_only_lookups_change_at_the_count_thresholds() {
        use TermKind::*;
        let details = Details::of(&request("main"), None);
        for (count, want) in [
            (1, vec![(0.0, 1, LookupKind::All)]),
            (29_999, vec![(0.0, 29_999, LookupKind::All)]),
            (30_000, vec![(0.0, 49_999, LookupKind::Any), (1.0, 30_000, LookupKind::All)]),
            (50_000, vec![(0.0, 49_999, LookupKind::Any)]),
        ] {
            let g = counted_graph(
                "main",
                &[(Partial, "main", "", count, 1), (Word, "main", "", 49_999, 1), (Word, "main", "maine", 50_000, 1)],
            );
            let builder = Builder { g: &g, details: &details };
            let list = builder.lookups(Span::new(0, 1), &[]);
            assert_eq!(list.into_iter().map(|(p, c, l)| (p, c, l[0].kind)).collect::<Vec<_>>(), want);
        }
    }

    #[test]
    fn frequent_words_use_a_full_name_or_drive_from_both_fields() {
        use TermKind::*;
        let details = Details::of(&request("main road city"), None);
        for (name_count, addr_count, full_count, expected) in [
            (120_000, 75_000, 79_999, vec![(0.0, LookupKind::Any), (0.6, LookupKind::All)]),
            (120_000, 75_000, 80_000, vec![(0.4, LookupKind::All)]),
            (500_000, 100_000, 80_000, vec![(0.9, LookupKind::All)]),
            (500_000, 150_000, 80_000, vec![]),
        ] {
            let g = counted_graph(
                "main road city",
                &[
                    (Partial, "main", "", name_count, 1),
                    (Partial, "road", "", name_count, 1),
                    (Word, "main road", "", full_count, 1),
                    (Partial, "city", "", 1, addr_count),
                ],
            );
            let builder = Builder { g: &g, details: &details };
            let variants = builder.lookups(Span::new(0, 2), &[Span::new(2, 3)]);
            assert_eq!(variants.len(), expected.len());
            for ((p, _, ls), (want, kind)) in variants.into_iter().zip(expected) {
                assert!((p - want).abs() < 1e-9);
                assert_eq!(ls[0].kind, kind);
                if want == 0.9 {
                    assert_eq!((ls[1].field, ls[1].kind), (Field::Address, LookupKind::All));
                }
            }
        }
    }

    #[test]
    fn a_frequent_number_drives_from_the_address_or_uses_full_names() {
        use TermKind::*;
        let details = Details::of(&request("12 avenue"), None);
        for (count, addr_count, fulls, kinds) in [
            (9_999, 20_000, 0, vec![LookupKind::Any, LookupKind::Restrict]),
            (10_000, 19_999, 0, vec![LookupKind::All, LookupKind::Restrict]),
            (10_000, 20_000, 5, vec![LookupKind::Any, LookupKind::Any]),
            (10_000, 20_000, 6, vec![]),
        ] {
            let mut terms = MemTerms::default();
            terms.insert(HouseNumber, "12", ("", ""), (count, 1), TermInfo::None);
            terms.insert(Partial, "avenue", ("", ""), (1, addr_count), TermInfo::None);
            for i in 0..fulls {
                terms.insert(Word, "avenue", (&format!("avenue@{i}"), "avenue"), (1, 1), TermInfo::None);
            }
            let g = tokens(split("12 avenue").unwrap(), &terms, Lookup::Exact);
            let reading = Assignment { housenumber: Some(Span::new(0, 1)), address: vec![Span::new(1, 2)], ..Assignment::default() };
            let searches = Builder { g: &g, details: &details }.build(&reading);
            if kinds.is_empty() {
                assert!(searches.is_empty());
            } else {
                let [Search::Place { data, .. }] = searches.as_slice() else { panic!("expected the number as the name") };
                assert!(data.housenumbers.0.is_empty());
                assert_eq!(data.lookups.iter().map(|l| l.kind).collect::<Vec<_>>(), kinds);
            }
        }
    }

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
