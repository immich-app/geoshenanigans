//! The terms of an index, as a search looks them up: Nominatim's tokens (its `word` rows), each
//! with its lookup word, its counts and a posting list in each sub-index that holds it.
use crate::index::Sub;

/// What a term is, Nominatim's token type (search/query.py:52-67); its number is the first byte
/// of the term's key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TermKind {
    /// `W`: a full name, of one word or more.
    Word = 1,
    /// `w`: one word of a name.
    Partial = 2,
    /// `P`: a postcode, by country and normalised code.
    Postcode = 3,
    /// `C`: a country's name or code.
    Country = 4,
    /// `Q`: a category, `class=type`.
    Category = 5,
    /// `H`: a house number.
    HouseNumber = 6,
    /// `A`: a word of a rank-30 entity's address (its filed street's words, its own `addr:*`).
    AddressWord = 7,
}

impl TermKind {
    pub const ALL: [TermKind; 7] = [
        TermKind::Word,
        TermKind::Partial,
        TermKind::Postcode,
        TermKind::Country,
        TermKind::Category,
        TermKind::HouseNumber,
        TermKind::AddressWord,
    ];

    /// Nominatim's letter for it, ours for `A`.
    pub fn letter(self) -> char {
        match self {
            TermKind::Word => 'W',
            TermKind::Partial => 'w',
            TermKind::Postcode => 'P',
            TermKind::Country => 'C',
            TermKind::Category => 'Q',
            TermKind::HouseNumber => 'H',
            TermKind::AddressWord => 'A',
        }
    }

    /// The kind whose number is `byte`; None for a byte of no kind.
    pub fn from_byte(byte: u8) -> Option<TermKind> {
        TermKind::ALL.into_iter().find(|&k| k as u8 == byte)
    }
}

/// Where a term's posting list starts in a sub-index's postings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PostingRef {
    pub sub: Sub,
    pub offset: u64,
}

/// A term a lookup found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Term {
    pub kind: TermKind,
    pub token: String,
    /// The word Nominatim looks it up by: a variant's source name, else the token.
    pub lookup_word: String,
    /// Entities with it in their names, Nominatim's `count` (tokenizer.py:85-118), summed over
    /// the sub-indexes the search reads; 1 for a term without statistics.
    pub count: u32,
    /// Entities with it in their address, Nominatim's `addr_count`, summed likewise.
    pub addr_count: u32,
    /// Its posting list in each sub-index that holds one.
    pub postings: Vec<PostingRef>,
}

/// How far a fuzzy lookup reaches: `edits` (Damerau-Levenshtein) from the word, its first
/// `anchor` characters as typed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fuzz {
    pub edits: u8,
    pub anchor: usize,
}

/// What a search looks terms up in: an index's term dictionaries, or an in-memory table. Each
/// lookup answers in (token, lookup word) order, a prefix or fuzzy one at most `max` terms.
pub trait TermSource {
    /// The terms of `kind` whose token is `token`, one a lookup word.
    fn exact(&self, kind: TermKind, token: &str) -> Vec<Term>;
    /// The terms of `kind` whose token starts with `prefix`.
    fn prefix(&self, kind: TermKind, prefix: &str, max: usize) -> Vec<Term>;
    /// The terms of `kind` whose token is within `fuzz` of `word`.
    fn fuzzy(&self, kind: TermKind, word: &str, fuzz: Fuzz, max: usize) -> Vec<Term>;
}

/// Terms held in memory with their counts and no postings: what the tests of a query's reading
/// look terms up in.
#[cfg(test)]
#[derive(Default)]
pub struct MemTerms(std::collections::BTreeMap<(TermKind, String, String), Term>);

#[cfg(test)]
impl MemTerms {
    /// Term `token` of `kind`, looked up by `lookup_word` (the token for ""), with Nominatim's
    /// counts.
    pub fn insert(&mut self, kind: TermKind, token: &str, lookup_word: &str, count: u32, addr_count: u32) {
        let lookup_word = if lookup_word.is_empty() { token } else { lookup_word };
        let term = Term { kind, token: token.into(), lookup_word: lookup_word.into(), count, addr_count, postings: Vec::new() };
        self.0.insert((kind, token.into(), lookup_word.into()), term);
    }

    /// The terms of `kind` whose token `matches`, at most `max`.
    fn matching(&self, kind: TermKind, max: usize, matches: impl Fn(&str) -> bool) -> Vec<Term> {
        self.0.values().filter(|t| t.kind == kind && matches(&t.token)).take(max).cloned().collect()
    }
}

#[cfg(test)]
impl TermSource for MemTerms {
    fn exact(&self, kind: TermKind, token: &str) -> Vec<Term> {
        self.matching(kind, usize::MAX, |t| t == token)
    }

    fn prefix(&self, kind: TermKind, prefix: &str, max: usize) -> Vec<Term> {
        self.matching(kind, max, |t| t.starts_with(prefix))
    }

    fn fuzzy(&self, kind: TermKind, word: &str, fuzz: Fuzz, max: usize) -> Vec<Term> {
        let dfa = levenshtein_automata::LevenshteinAutomatonBuilder::new(fuzz.edits, true).build_dfa(word);
        let anchor: String = word.chars().take(fuzz.anchor).collect();
        self.matching(kind, max, |t| t.starts_with(&anchor) && dfa.eval(t) != levenshtein_automata::Distance::AtLeast(fuzz.edits + 1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_kind_has_its_letter_and_its_number() {
        let named: Vec<(char, u8)> = TermKind::ALL.iter().map(|&k| (k.letter(), k as u8)).collect();

        assert_eq!(named, [('W', 1), ('w', 2), ('P', 3), ('C', 4), ('Q', 5), ('H', 6), ('A', 7)]);
        for k in TermKind::ALL {
            assert_eq!(TermKind::from_byte(k as u8), Some(k));
        }
        assert_eq!((TermKind::from_byte(0), TermKind::from_byte(8)), (None, None));
    }

    /// Terms of "Liberty Park Drive" and "Austin", and some near them.
    fn liberty() -> MemTerms {
        let mut terms = MemTerms::default();
        terms.insert(TermKind::Partial, "park", "", 41_502, 120_334);
        terms.insert(TermKind::Partial, "parkway", "", 9_001, 3_012);
        terms.insert(TermKind::Partial, "spark", "", 120, 7);
        terms.insert(TermKind::Partial, "austin", "", 3_500, 280_000);
        terms.insert(TermKind::Word, "liberty park dr", "liberty park drive", 2, 0);
        terms.insert(TermKind::Word, "liberty park dr", "liberty park dr", 1, 0);
        terms.insert(TermKind::Word, "park", "", 300, 0);
        terms
    }

    #[test]
    fn an_exact_lookup_finds_a_term_a_lookup_word_of_its_kind_alone() {
        let terms = liberty();

        let found = terms.exact(TermKind::Word, "liberty park dr");

        assert_eq!(found.iter().map(|t| (t.lookup_word.as_str(), t.count)).collect::<Vec<_>>(), [("liberty park dr", 1), ("liberty park drive", 2)]);
        assert_eq!(terms.exact(TermKind::Partial, "park").iter().map(|t| (t.lookup_word.as_str(), t.count, t.addr_count)).collect::<Vec<_>>(), [("park", 41_502, 120_334)]);
        assert!(terms.exact(TermKind::Partial, "liberty park dr").is_empty());
    }

    #[test]
    fn a_prefix_lookup_finds_the_tokens_it_starts_up_to_its_most() {
        let terms = liberty();

        let tokens = |max| terms.prefix(TermKind::Partial, "par", max).into_iter().map(|t| t.token).collect::<Vec<_>>();

        assert_eq!(tokens(10), ["park", "parkway"]);
        assert_eq!(tokens(1), ["park"]);
    }

    #[test]
    fn a_fuzzy_lookup_finds_the_tokens_within_its_edits_that_keep_its_anchor() {
        let terms = liberty();
        let tokens = |word, edits, anchor| {
            terms.fuzzy(TermKind::Partial, word, Fuzz { edits, anchor }, 10).into_iter().map(|t| t.token).collect::<Vec<_>>()
        };

        assert_eq!(tokens("austn", 1, 1), ["austin"]);
        assert_eq!(tokens("asutin", 1, 1), ["austin"]);
        assert!(tokens("austn", 0, 1).is_empty());
        assert!(tokens("bustin", 1, 1).is_empty());
        assert_eq!(tokens("bustin", 1, 0), ["austin"]);
    }
}
