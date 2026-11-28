//! The text of names and queries: folding, and the forms Nominatim's tokenizer gives them.
//!
//! Where these modules follow Nominatim, they reimplement its behaviour as observed and as its
//! documentation and settings describe it; none of its GPL-3.0 source is copied.
pub mod difflib;
pub mod housenumber;
mod latin_ascii;
pub mod normalize;
pub mod postcode;
mod regex;
pub mod sanitize;
mod unicode;
pub mod variants;

/// The break before a query node, as Nominatim's query nodes carry it, with the character it
/// writes it as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Break {
    /// The query's start: `<`.
    Start,
    /// The query's end: `>`.
    End,
    /// A comma: `,`.
    Phrase,
    /// A colon: `:`.
    SoftPhrase,
    /// White space: ` `.
    Word,
    /// A hyphen inside a word: `-`.
    Part,
    /// Between the terms one written word transliterates to: a backtick.
    Token,
}

impl Break {
    pub fn char(self) -> char {
        match self {
            Break::Start => '<',
            Break::End => '>',
            Break::Phrase => ',',
            Break::SoftPhrase => ':',
            Break::Word => ' ',
            Break::Part => '-',
            Break::Token => '`',
        }
    }

    pub fn from_char(c: char) -> Option<Break> {
        [Break::Start, Break::End, Break::Phrase, Break::SoftPhrase, Break::Word, Break::Part, Break::Token]
            .into_iter()
            .find(|b| b.char() == c)
    }
}
