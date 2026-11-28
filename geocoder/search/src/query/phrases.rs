//! The special phrases a query's words can be ("cafe", "cafes in", "cafe near"): the OSM wiki's
//! lists in data/special_phrases.tsv, read as `nominatim special-phrases` imports them into its
//! `word` table (nominatim_db/tokenizer/icu_tokenizer/analyzer.py:103-149,
//! tools/special_phrases/special_phrase.py).
use crate::index::entities::Category;
use crate::text::normalize::{name_normalized, transliterated};
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

const SPECIAL_PHRASES: &str = include_str!("../../data/special_phrases.tsv");

/// How a phrase's category stands to the rest of the query.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Operator {
    /// "cafes in Austin".
    In,
    /// "cafe near Liberty Park Drive".
    Near,
    /// The category's name alone: "cafe".
    Plain,
}

/// A special phrase: its normalised text, which a match is measured against, its category and
/// its operator.
#[derive(Clone, Debug, PartialEq)]
pub struct Phrase {
    pub word: String,
    pub category: Category<'static>,
    pub operator: Operator,
}

/// The special phrases by their token.
pub struct Phrases(HashMap<String, Vec<Phrase>>);

impl Phrases {
    /// The phrases of data/special_phrases.tsv, read once.
    pub fn get() -> &'static Phrases {
        static PHRASES: OnceLock<Phrases> = OnceLock::new();
        PHRASES.get_or_init(|| Phrases::read(SPECIAL_PHRASES))
    }

    /// Phrases of a TSV of phrase, class, type and operator after a header line. A phrase that
    /// normalises like another of the same category and operator is that one; one whose class
    /// cannot name a category, or that has no token, is left out.
    fn read(tsv: &str) -> Phrases {
        let mut seen = HashSet::new();
        let mut by_token: HashMap<String, Vec<Phrase>> = HashMap::new();
        for line in tsv.lines().skip(1) {
            let f: Vec<&str> = line.split('\t').map(str::trim).collect();
            let [label, class, typ, operator] = f[..] else { continue };
            if !is_category_class(class) {
                continue;
            }
            let operator = match operator.to_lowercase().as_str() {
                "in" => Operator::In,
                "near" => Operator::Near,
                _ => Operator::Plain,
            };
            let word = name_normalized(label);
            let token = transliterated(&word);
            if token.is_empty() || !seen.insert((word.clone(), class, typ, operator)) {
                continue;
            }
            let category = Category { class: class.to_string().into(), typ: typ.to_string().into() };
            by_token.entry(token).or_default().push(Phrase { word, category, operator });
        }
        Phrases(by_token)
    }

    /// The phrases whose token is `token`.
    pub fn of(&self, token: &str) -> &[Phrase] {
        self.0.get(token).map_or(&[], Vec::as_slice)
    }
}

/// Whether a class can name a category: ASCII letters, digits, '_' and '-', as Nominatim's
/// category labels allow (db_searches/base.py:24-45).
fn is_category_class(class: &str) -> bool {
    !class.is_empty() && class.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn categories(phrases: &Phrases, token: &str) -> Vec<(String, String, String, Operator)> {
        phrases.of(token).iter().map(|p| (p.word.clone(), p.category.class.to_string(), p.category.typ.to_string(), p.operator)).collect()
    }

    #[test]
    fn a_phrase_is_found_by_its_token_with_its_normalised_text() {
        let phrases = Phrases::read("phrase\tclass\ttype\toperator\nCafé\tamenity\tcafe\t-\nCafés in\tamenity\tcafe\tin\nCafe near\tamenity\tcafe\tNEAR\n");

        assert_eq!(categories(&phrases, "cafe"), [("café".into(), "amenity".into(), "cafe".into(), Operator::Plain)]);
        assert_eq!(categories(&phrases, "cafes in"), [("cafés in".into(), "amenity".into(), "cafe".into(), Operator::In)]);
        assert_eq!(categories(&phrases, "cafe near"), [("cafe near".into(), "amenity".into(), "cafe".into(), Operator::Near)]);
        assert!(phrases.of("cafes").is_empty());
    }

    #[test]
    fn phrases_that_normalise_alike_are_one_and_bad_classes_none() {
        let phrases = Phrases::read("phrase\tclass\ttype\toperator\nHotel\ttourism\thotel\t-\n hotel \ttourism\thotel\t-\nHotel\ttourism\tmotel\t-\nPark\tleis ure\tpark\t-\nBar\tamenity\tbar\tby\n");

        assert_eq!(categories(&phrases, "hotel").len(), 2);
        assert!(phrases.of("park").is_empty());
        assert_eq!(categories(&phrases, "bar")[0].3, Operator::Plain);
    }

    #[test]
    fn the_snapshot_holds_the_phrases_the_oracle_imported() {
        let phrases = Phrases::get();

        let count: usize = phrases.0.values().map(Vec::len).sum();

        assert_eq!(count, 9_549);
        assert!(categories(phrases, "park").contains(&("park".into(), "leisure".into(), "park".into(), Operator::Plain)));
        assert!(categories(phrases, "u bahn station").contains(&("u bahn station".into(), "railway".into(), "subway".into(), Operator::Plain)));
    }
}
