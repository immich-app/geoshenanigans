//! A query's text as Nominatim's ICU analyzer reads it: its phrases, preprocessed and normalised,
//! split into nodes at its breaks (search/icu_tokenizer.py:116-231, search/query.py:93-101,
//! settings/query_preprocessing.yaml, query_preprocessing/).
use crate::constants::*;
use crate::text::normalize::{normalized, spaced_token};
use crate::text::regex::is_python_space;
use crate::text::Break;
use std::fmt;

/// A query node: the break before it and the word from it to the next node.
#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    pub btype: Break,
    /// The part of the normalised query the word was written as (Nominatim's partial lookup
    /// word); empty for the end node.
    pub word: String,
    /// The word's token, the form terms are keyed by (Nominatim's transliterated word).
    pub token: String,
}

impl Node {
    fn new(btype: Break, word: &str, token: &str) -> Node {
        Node { btype, word: word.to_string(), token: token.to_string() }
    }

    /// The penalty of a break at the node.
    pub fn penalty(&self) -> f64 {
        match self.btype {
            Break::Start | Break::End | Break::Phrase | Break::SoftPhrase => BREAK_PENALTY_PHRASE,
            Break::Word => BREAK_PENALTY_WORD,
            Break::Part => BREAK_PENALTY_PART,
            Break::Token => BREAK_PENALTY_TOKEN,
        }
    }

    /// What a word that ends at the node pays (query.py:267-271).
    pub fn word_break_penalty(&self) -> f64 {
        self.penalty().max(0.0)
    }

    /// What a word that goes on over the node pays (query.py:273-278).
    pub fn word_continuation_penalty(&self) -> f64 {
        (-self.penalty()).max(0.0)
    }
}

/// A query split into nodes, the end node last, and the number of phrases it was read in.
#[derive(Clone, Debug, PartialEq)]
pub struct Nodes {
    pub phrases: usize,
    pub nodes: Vec<Node>,
}

impl Nodes {
    /// The token slots between the nodes (query.py:337-340).
    pub fn slots(&self) -> usize {
        self.nodes.len() - 1
    }
}

/// Why a query is not read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadError {
    /// More token slots than QUERY_MAX_SLOTS.
    TooLong,
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ReadError::TooLong => write!(f, "Query too long"),
        }
    }
}

impl std::error::Error for ReadError {}

/// `q` split into its nodes: phrases at PHRASE_SEPARATOR, each preprocessed and normalised, the
/// empty ones dropped; each phrase's words at its spaces, colons and hyphens.
pub fn split(q: &str) -> Result<Nodes, ReadError> {
    let phrases: Vec<String> = q
        .trim_matches(is_python_space)
        .split(PHRASE_SEPARATOR)
        .map(|p| p.trim_matches(is_python_space))
        .filter_map(preprocessed)
        .map(|p| normalized(&p).trim_matches(['-', ':', ' ']).to_string())
        .filter(|p| !p.is_empty())
        .collect();
    let mut nodes = Vec::new();
    let mut breakchar = Some(Break::Start);
    for phrase in &phrases {
        for piece in pieces(phrase) {
            match (piece, breakchar) {
                (Piece::Separator(b), None) => breakchar = Some(b),
                (Piece::Word(word), Some(b)) => {
                    let trans = spaced_token(word);
                    if !trans.is_empty() {
                        let mut b = b;
                        for (term, term_word) in split_transliteration(&trans, word) {
                            if !term.is_empty() && term.chars().count() < NODE_TOKEN_MAX_CHARS {
                                nodes.push(Node::new(b, &term_word, &term));
                                b = Break::Token;
                            }
                        }
                    }
                    breakchar = None;
                }
                _ => {}
            }
        }
        breakchar = Some(Break::Phrase);
    }
    nodes.push(Node::new(Break::End, "", ""));
    let read = Nodes { phrases: phrases.len(), nodes };
    if read.slots() > QUERY_MAX_SLOTS {
        return Err(ReadError::TooLong);
    }

    Ok(read)
}

/// A phrase after Nominatim's query preprocessing: none when it reads as nothing.
fn preprocessed(phrase: &str) -> Option<String> {
    // A line of PHRASE_MAX_CHARS or more is replaced by nothing; one with an inner line break is
    // not a line, as Python's `^.{500,}$` reads it.
    if !phrase.contains('\n') && phrase.chars().count() >= PHRASE_MAX_CHARS {
        return None;
    }

    Some(japanese_phrases(phrase))
}

/// Characters that end a Japanese prefecture, and a municipality (city, ward, town, village).
const PREFECTURE_ENDS: [char; 5] = ['都', '道', '府', '県', '縣'];
const MUNICIPALITY_ENDS: [char; 5] = ['市', '区', '區', '町', '村'];

/// A Japanese address with colons between its prefecture, its municipality and the rest, as
/// Nominatim's split_japanese_phrases writes it; any other phrase as it is.
fn japanese_phrases(phrase: &str) -> String {
    let chars: Vec<char> = phrase.chars().collect();
    let chars = &chars[..];
    let line = move |from: usize, to: usize| chars.get(from..to).is_some_and(|s| !s.contains(&'\n'));
    // Prefecture: two characters, or three, then its end (`...??[都道府県縣]`).
    let prefectures: Vec<usize> = [3, 4].into_iter().filter(|&n| line(0, n) && PREFECTURE_ENDS.contains(&chars[n - 1])).collect();
    // Municipality from `at`: one character or more, then its end, the fewest first (`.+?[市区區町村]`).
    let municipalities = move |at: usize| (at + 2..=chars.len()).take_while(move |&n| line(at, n)).filter(move |&n| MUNICIPALITY_ENDS.contains(&chars[n - 1]));
    // The rest from `at`: its first line, of one character or more (`.+`).
    let rest = |at: usize| {
        let end = chars[at.min(chars.len())..].iter().position(|&c| c == '\n').map_or(chars.len(), |p| at + p);
        (end > at).then(|| String::from_iter(&chars[at..end]))
    };
    let text = |from: usize, to: usize| String::from_iter(&chars[from..to]);
    let split = prefectures
        .iter()
        .find_map(|&p| municipalities(p).find_map(|m| Some(format!("{}:{}:{}", text(0, p), text(p, m), rest(m)?))))
        .or_else(|| prefectures.iter().find_map(|&p| Some(format!("{}:{}", text(0, p), rest(p)?))))
        .or_else(|| municipalities(0).find_map(|m| Some(format!("{}:{}", text(0, m), rest(m)?))));
    split.unwrap_or_else(|| phrase.to_string())
}

enum Piece<'a> {
    Word(&'a str),
    Separator(Break),
}

/// A normalised phrase as Python's `re.split('([ :-])', text)` cuts it: words (some empty) and
/// the separator after each.
fn pieces(phrase: &str) -> Vec<Piece<'_>> {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, c) in phrase.char_indices() {
        let b = match c {
            ' ' => Break::Word,
            ':' => Break::SoftPhrase,
            '-' => Break::Part,
            _ => continue,
        };
        out.push(Piece::Word(&phrase[start..i]));
        out.push(Piece::Separator(b));
        start = i + c.len_utf8();
    }
    out.push(Piece::Word(&phrase[start..]));
    out
}

/// The terms a word's transliteration `trans` holds, each with the part of the word it was
/// written as (icu_tokenizer.py:181-207).
fn split_transliteration(trans: &str, word: &str) -> Vec<(String, String)> {
    if !trans.contains(' ') {
        return vec![(trans.to_string(), word.to_string())];
    }

    let mut terms = trans.split(' ').filter(|t| !t.is_empty());
    let Some(mut current) = terms.next() else { return Vec::new() };
    let mut out = Vec::new();
    let mut written = String::new();
    for c in word.chars() {
        written.push(c);
        if spaced_token(&written).trim_end_matches(is_python_space) == current {
            out.push((current.to_string(), std::mem::take(&mut written)));
            match terms.next() {
                Some(next) => current = next,
                None => return out,
            }
        }
    }
    if !written.is_empty() {
        out.push((current.to_string(), written));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The nodes as Nominatim's debug page writes them: break character, then the token.
    fn written(q: &str) -> String {
        split(q).unwrap().nodes.iter().map(|n| format!("{}{}", n.btype.char(), n.token)).collect()
    }

    #[test]
    fn breaks_carry_nominatims_penalties() {
        let read = split("a b-c, d:e").unwrap();

        let breaks: Vec<(char, f64)> = read.nodes.iter().map(|n| (n.btype.char(), n.penalty())).collect();

        assert_eq!(breaks, [('<', -0.5), (' ', 0.1), ('-', 0.2), (',', -0.5), (':', -0.5), ('>', -0.5)]);
        assert_eq!((read.phrases, read.slots()), (2, 5));
    }

    #[test]
    fn a_break_costs_a_word_that_ends_there_or_one_that_goes_on_over_it() {
        let read = split("a b, c").unwrap();

        let costs: Vec<(f64, f64)> = read.nodes.iter().map(|n| (n.word_break_penalty(), n.word_continuation_penalty())).collect();

        assert_eq!(costs, [(0.0, 0.5), (0.1, 0.0), (0.0, 0.5), (0.0, 0.5)]);
    }

    #[test]
    fn a_word_keeps_its_normalised_spelling_beside_its_token() {
        let read = split("München Москва").unwrap();

        let words: Vec<(&str, &str)> = read.nodes.iter().map(|n| (n.word.as_str(), n.token.as_str())).collect();

        assert_eq!(words, [("münchen", "munchen"), ("москва", "moskva"), ("", "")]);
    }

    #[test]
    fn empty_phrases_are_dropped() {
        let read = split(" , Austin ,, TX ").unwrap();

        assert_eq!((written(" , Austin ,, TX "), read.phrases), ("<austin,tx>".to_string(), 2));
        assert_eq!(written("  "), ">");
        assert_eq!(written(", ,"), ">");
    }

    #[test]
    fn the_last_separator_before_a_word_is_its_break() {
        assert_eq!(written("a--b"), "<a-b>");
        assert_eq!(written("a -:b"), "<a:b>");
        // A word that transliterates to nothing makes no node; the break after it stands.
        assert_eq!(written("a ǀ-b"), "<a-b>");
    }

    #[test]
    fn a_phrase_of_no_node_leaves_its_phrase_break_to_the_next() {
        let read = split("ǀ, Austin").unwrap();

        assert_eq!((written("ǀ, Austin"), read.phrases), (",austin>".to_string(), 2));
    }

    #[test]
    fn a_phrase_of_five_hundred_characters_reads_as_nothing() {
        let long = "x".repeat(PHRASE_MAX_CHARS);

        assert_eq!(written(&long), ">");
        assert_eq!(written(&format!("{long}, Austin")), "<austin>");
        assert_eq!(split(&long[1..]).unwrap().slots(), 0, "a word this long makes no node");
        assert_eq!(written(&format!("{} {}", "x".repeat(250), "y".repeat(248))), format!("<{} {}>", "x".repeat(250), "y".repeat(248)));
        assert_eq!(written(&format!("{}\n{}", "x".repeat(250), "y".repeat(250))), format!("<{} {}>", "x".repeat(250), "y".repeat(250)));
    }

    #[test]
    fn a_token_of_256_characters_makes_no_node() {
        assert_eq!(written(&"a".repeat(NODE_TOKEN_MAX_CHARS)), ">");
        assert_eq!(split(&"a".repeat(NODE_TOKEN_MAX_CHARS - 1)).unwrap().slots(), 1);
    }

    #[test]
    fn more_than_fifty_slots_is_too_long() {
        assert_eq!(split(&"a ".repeat(QUERY_MAX_SLOTS)).map(|r| r.slots()), Ok(50));
        assert_eq!(split(&"a ".repeat(QUERY_MAX_SLOTS + 1)), Err(ReadError::TooLong));
        assert_eq!(ReadError::TooLong.to_string(), "Query too long");
    }

    #[test]
    fn a_japanese_address_splits_at_its_prefecture_and_municipality() {
        for (q, nodes) in [
            ("東京都渋谷区道玄坂", "<東京都:渋谷区:道玄坂>"),
            ("北海道札幌市", "<北海道:札幌市>"),
            ("神奈川県横浜市中区", "<神奈川県:横浜市:中区>"),
            ("横浜市中区", "<横浜市:中区>"),
            // Three characters before 道 read as a prefecture first, as Nominatim's pattern reads them.
            ("渋谷区道玄坂", "<渋谷区道:玄坂>"),
            ("東京都", "<東京都>"),
            ("区役所", "<区役所>"),
            ("Austin", "<austin>"),
        ] {
            assert_eq!(written(q), nodes, "{q}");
        }
    }
}
