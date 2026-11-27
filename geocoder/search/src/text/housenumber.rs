//! House numbers as Nominatim's house-number analysis gives them: a canonical form that marks
//! where a space between digits and letters is optional, and its spellings with and without each.

use super::normalize::{normalized, spaced_token};
use crate::constants::{HOUSENUMBER_MAX_OPTIONAL_SPACES, HOUSENUMBER_NAMED_PART_LETTERS};

/// Where the canonical form of a house number allows a space or none.
pub const OPTIONAL_SPACE: char = '␣';

/// The canonical form of a house number: the value itself when it is ASCII digits only;
/// otherwise its token form, with an optional space at each boundary between digits and other
/// characters ("8a", "8 a" -> "8␣a"), unless it has a run of letters long enough to be a word
/// ("12 abcd") or would get more boundaries than that. A space Nominatim's rules leave at either end
/// stays, as it does in Nominatim ("2221 (131)" -> "2221 131 "). None when nothing is left.
pub fn canonical(value: &str) -> Option<String> {
    if value.bytes().all(|b| b.is_ascii_digit()) {
        return (!value.is_empty()).then(|| value.to_string());
    }
    let token = spaced_token(&normalized(value));
    if token.is_empty() {
        return None;
    }
    if has_named_part(&token) {
        return Some(token);
    }
    let chars: Vec<char> = token.chars().collect();
    let other = |c: char| !c.is_ascii_digit() && !c.is_whitespace() && c != OPTIONAL_SPACE;
    let (digit_other, n1) = mark_boundaries(&chars, |c| c.is_ascii_digit(), other);
    let (marked, n2) = mark_boundaries(&digit_other, other, |c| c.is_ascii_digit());
    Some(if n1 + n2 <= HOUSENUMBER_MAX_OPTIONAL_SPACES { marked.into_iter().collect() } else { token })
}

/// Every spelling of a canonical form, each optional space a space or none, the all-spaces one
/// first (Nominatim keeps that one as the number's lookup word).
pub fn variants(canonical: &str) -> Vec<String> {
    let parts: Vec<&str> = canonical.split(OPTIONAL_SPACE).collect();
    let gaps = parts.len() - 1;
    (0..1usize << gaps)
        .map(|joined| {
            let mut out = String::from(parts[0]);
            for (g, part) in parts[1..].iter().enumerate() {
                if joined & (1 << (gaps - 1 - g)) == 0 {
                    out.push(' ');
                }
                out.push_str(part);
            }
            out
        })
        .collect()
}

fn has_named_part(token: &str) -> bool {
    let mut run = 0;
    token.chars().any(|c| {
        run = if c.is_ascii_lowercase() { run + 1 } else { 0 };
        run >= HOUSENUMBER_NAMED_PART_LETTERS
    })
}

/// Python's `re.subn(r'(X)\s*(Y)', r'\1␣\2')`: each X, white space, Y found left to right (none
/// overlapping) becomes X␣Y; returns the text and how many it replaced.
fn mark_boundaries(chars: &[char], x: impl Fn(char) -> bool, y: impl Fn(char) -> bool) -> (Vec<char>, usize) {
    let mut out = Vec::with_capacity(chars.len() + 4);
    let mut n = 0;
    let mut i = 0;
    while i < chars.len() {
        if x(chars[i]) {
            let mut j = i + 1;
            while j < chars.len() && chars[j].is_whitespace() {
                j += 1;
            }
            if j < chars.len() && y(chars[j]) {
                out.extend([chars[i], OPTIONAL_SPACE, chars[j]]);
                n += 1;
                i = j + 1;
                continue;
            }
        }
        out.push(chars[i]);
        i += 1;
    }
    (out, n)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nominatim's own house-number analysis (with the oracle's ICU rules) over 1,500 address values
    /// of the oracle and edge cases: value, canonical form, variants joined by '|'.
    const ORACLE: &str = include_str!("../../testdata/housenumbers.tsv");

    #[test]
    fn canonical_forms_and_variants_are_nominatims() {
        let mut wrong = Vec::new();
        for line in ORACLE.lines() {
            let f: Vec<&str> = line.split('\t').collect();
            let got = canonical(f[0]);
            let variants = got.as_deref().map(variants).unwrap_or_default().join("|");
            if got.as_deref().unwrap_or("") != f[1] || variants != f[2] {
                wrong.push(format!("{:?}: {got:?} {variants:?} not {:?} {:?}", f[0], f[1], f[2]));
            }
        }
        assert!(wrong.is_empty(), "{} of {}:\n{}", wrong.len(), ORACLE.lines().count(), wrong.join("\n"));
    }

    #[test]
    fn a_space_between_digits_and_letters_is_optional() {
        assert_eq!(canonical("8a").as_deref(), Some("8␣a"));
        assert_eq!(canonical("8 a"), canonical("8a"));
        assert_eq!(variants("8␣a"), ["8 a", "8a"]);
    }

    #[test]
    fn a_word_keeps_its_space() {
        assert_eq!(canonical("12 abcd").as_deref(), Some("12 abcd"));
        assert_eq!(variants("12 abcd"), ["12 abcd"]);
    }

    #[test]
    fn digits_stay_as_they_are() {
        assert_eq!(canonical("00012").as_deref(), Some("00012"));
        assert_eq!(canonical("١٢").as_deref(), Some("12"));
        assert_eq!(canonical(""), None);
        assert_eq!(canonical("ʻ"), None);
        assert_eq!(canonical("-").as_deref(), Some(" "));
    }

    #[test]
    fn more_than_four_boundaries_mark_none() {
        assert_eq!(canonical("1a2b").as_deref(), Some("1␣a␣2␣b"));
        assert_eq!(canonical("1a2b3c4d5e").as_deref(), Some("1a2b3c4d5e"));
    }

    #[test]
    fn variants_list_every_choice_spaces_first() {
        assert_eq!(variants("5␣k␣2"), ["5 k 2", "5 k2", "5k 2", "5k2"]);
        assert_eq!(variants("1044"), ["1044"]);
    }
}
