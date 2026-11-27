use unicode_normalization::{char::is_combining_mark, UnicodeNormalization};

use super::latin_ascii::{LATIN_ASCII, LATIN_SCRIPT};
use super::unicode::{DECIMAL_ZEROS, MODIFIER_LETTER, NONSPACING_MARK_OR_FORMAT, PUNCTUATION_OR_SYMBOL, SPACING_VIRAMA};

/// One query or name token: `folded` is what the index stores, `raw` the token before the
/// abbreviation folding (a prefix search also tries `raw`, so "fort" still reaches "fortaleza").
#[derive(Clone, Debug, PartialEq)]
pub struct Token {
    pub folded: String,
    pub raw: String,
}

/// Letters NFKD leaves whole that a reader treats as two letters or a plain letter.
fn spell_out(c: char) -> Option<&'static str> {
    Some(match c {
        'ß' => "ss",
        'æ' => "ae",
        'œ' => "oe",
        'ø' => "o",
        'đ' | 'ð' => "d",
        'þ' => "th",
        'ł' | 'ŀ' => "l",
        'ı' => "i",
        'ħ' => "h",
        'ŧ' => "t",
        _ => return None,
    })
}

/// Marks are diacritics only in the alphabetic scripts; in Indic, Thai, Arabic etc. they are
/// vowels and stay.
fn marks_are_diacritics(base: char) -> bool {
    (base as u32) < 0x0530 || (0x1E00..0x2000).contains(&(base as u32))
}

fn fold_abbreviation(t: &str) -> &str {
    match t {
        "saint" | "sankt" | "st" => "st",
        "sainte" | "ste" => "ste",
        "mount" | "mt" => "mt",
        "fort" | "ft" => "ft",
        _ => t,
    }
}

/// NFKD, diacritics stripped, lowercased, everything that is not a letter or digit a word
/// break; returns the tokens with and without the saint/mount/fort folding.
pub fn tokens(s: &str) -> Vec<Token> {
    let mut words: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut strip_marks = true;
    for c in s.nfkd() {
        if is_combining_mark(c) {
            if !strip_marks {
                cur.push(c);
            }
            continue;
        }
        strip_marks = marks_are_diacritics(c);
        for l in c.to_lowercase() {
            if let Some(rep) = spell_out(l) {
                cur.push_str(rep);
            } else if l.is_alphanumeric() {
                cur.push(l);
            } else if !cur.is_empty() {
                words.push(std::mem::take(&mut cur));
            }
        }
    }
    if !cur.is_empty() {
        words.push(cur);
    }
    words
        .into_iter()
        .map(|raw| Token { folded: fold_abbreviation(&raw).to_string(), raw })
        .collect()
}

/// The index form of a whole string: folded tokens joined by single spaces.
pub fn normalize(s: &str) -> String {
    join(&tokens(s))
}

pub fn join(t: &[Token]) -> String {
    let mut out = String::new();
    for (i, x) in t.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(&x.folded);
    }
    out
}

/// Scripts written without spaces whose characters carry a syllable or word each: typo
/// tolerance stays off for them. Hangul counts in the Jamo that `tokens` (NFKD) decomposes its
/// syllables into, as well as precomposed.
pub fn is_ideographic(s: &str) -> bool {
    s.chars().any(|c| {
        let u = c as u32;
        (0x1100..0x1200).contains(&u)
            || (0x2E80..0xA000).contains(&u)
            || (0xA960..0xA980).contains(&u)
            || (0xAC00..0xD800).contains(&u)
            || (0xF900..0xFB00).contains(&u)
    })
}

fn cyrillic(c: char, cc: &str) -> Option<&'static str> {
    let ukrainian = cc == "UA";
    let belarusian = cc == "BY";
    let south_slavic = matches!(cc, "RS" | "BA" | "ME" | "MK");
    Some(match c {
        'а' => "a", 'б' => "b", 'в' => "v", 'д' => "d", 'е' => "e", 'ё' => "e", 'з' => "z",
        'й' => "y", 'к' => "k", 'л' => "l", 'м' => "m", 'н' => "n", 'о' => "o", 'п' => "p", 'р' => "r",
        'с' => "s", 'т' => "t", 'у' => "u", 'ф' => "f", 'ъ' | 'ь' => "", 'ы' => "y", 'э' => "e",
        'ю' => "yu", 'я' => "ya", 'є' => "ye", 'ї' => "yi", 'і' => "i", 'ґ' => "g", 'ў' => "u",
        'ә' => "a", 'ғ' => "gh", 'қ' => "k", 'ң' => "ng", 'ө' => "o", 'ұ' | 'ү' => "u", 'һ' => "h",
        'ђ' => "dj", 'ј' => "j", 'љ' => "lj", 'њ' => "nj", 'ћ' => "c", 'џ' => "dz", 'ѓ' => "gj",
        'ќ' => "kj", 'ѕ' => "dz",
        'г' if ukrainian || belarusian => "h",
        'г' => "g",
        'и' if ukrainian => "y",
        'и' => "i",
        // Serbian/Macedonian Latin spells these with diacritics the folding strips
        'ж' if south_slavic => "z",
        'ж' => "zh",
        'х' if south_slavic => "h",
        'х' => "kh",
        'ц' if south_slavic => "c",
        'ц' => "ts",
        'ч' if south_slavic => "c",
        'ч' => "ch",
        'ш' if south_slavic => "s",
        'ш' => "sh",
        'щ' if cc == "BG" => "sht",
        'щ' => "shch",
        _ => return None,
    })
}

fn greek(c: char) -> Option<&'static str> {
    Some(match c {
        'α' => "a", 'β' => "v", 'γ' => "g", 'δ' => "d", 'ε' => "e", 'ζ' => "z", 'η' => "i", 'θ' => "th",
        'ι' => "i", 'κ' => "k", 'λ' => "l", 'μ' => "m", 'ν' => "n", 'ξ' => "x", 'ο' => "o", 'π' => "p",
        'ρ' => "r", 'σ' | 'ς' => "s", 'τ' => "t", 'υ' => "y", 'φ' => "f", 'χ' => "ch", 'ψ' => "ps",
        'ω' => "o",
        _ => return None,
    })
}

/// Latin spelling of a Cyrillic or Greek name (GeoNames' asciiname, roughly BGN/PCGN and
/// ELOT 743), with the country's own letter values for UA/BY/BG and the South Slavic
/// countries; None when the name has neither script. `cc` (the country code) is asked for only
/// then.
pub fn transliterate(s: &str, cc: impl FnOnce() -> String) -> Option<String> {
    let lower: String = normalize_marks_lower(s);
    if !lower.chars().any(|c| ('\u{0370}'..='\u{04FF}').contains(&c)) {
        return None;
    }
    let cc = cc();
    let cc = cc.as_str();
    let mut out = String::with_capacity(lower.len());
    let mut it = lower.chars().peekable();
    while let Some(c) = it.next() {
        if c == 'ο' && it.peek() == Some(&'υ') {
            it.next();
            out.push_str("ou");
            continue;
        }
        match cyrillic(c, cc).or_else(|| greek(c)) {
            Some(l) => out.push_str(l),
            None => out.push(c),
        }
    }
    Some(out)
}

/// Lowercase with diacritics removed (Greek tonos, Cyrillic breve on й kept as й).
fn normalize_marks_lower(s: &str) -> String {
    s.nfc()
        .flat_map(|c| c.to_lowercase())
        .map(without_tonos)
        .collect()
}

fn in_table(c: char, table: &[(u32, u32)]) -> bool {
    let u = c as u32;
    let i = table.partition_point(|&(_, hi)| hi < u);
    table.get(i).is_some_and(|&(lo, _)| lo <= u)
}

/// The value of a decimal digit of any script.
pub(super) fn decimal_digit(c: char) -> Option<u32> {
    let u = c as u32;
    let i = DECIMAL_ZEROS.partition_point(|&zero| zero <= u);
    let zero = DECIMAL_ZEROS[..i].last()?;
    (u - zero < 10).then(|| u - zero)
}

/// The ASCII digit of a decimal digit of any script.
fn ascii_digit(c: char) -> Option<char> {
    decimal_digit(c).map(|d| char::from(b'0' + d as u8))
}

/// The Han numerals Nominatim's normaliser writes as digits.
fn han_numeral(c: char) -> Option<&'static str> {
    Some(match c {
        '零' => "0",
        '一' => "1",
        '二' => "2",
        '三' => "3",
        '四' => "4",
        '五' => "5",
        '六' => "6",
        '七' => "7",
        '八' => "8",
        '九' => "9",
        '十' => "10",
        _ => return None,
    })
}

/// A character the normaliser turns into a '-' (a run of them into one): punctuation, symbols
/// and the apostrophe letter U+02BC, but not '-' and ':' themselves.
fn becomes_dash(c: char) -> bool {
    c == '\u{02BC}' || (c != '-' && c != ':' && in_table(c, PUNCTUATION_OR_SYMBOL))
}

/// The characters other than digits the normaliser keeps: letters, viramas, white space, '-' and ':'.
fn kept(c: char) -> bool {
    c.is_alphabetic() || in_table(c, SPACING_VIRAMA) || c.is_whitespace() || c == '-' || c == ':'
}

/// Nominatim's normaliser (its ICU `normalization` rules): the form a name's lookup word is in
/// and a query is read in. NFKC and lower case; every decimal digit (and a Han numeral) ASCII;
/// "n°" "no"; a run of punctuation and symbols one '-'; ß "ss"; other characters that are no
/// letter, digit, space, '-' or ':' dropped, and modifier letters, non-spacing marks and format
/// characters with them; white space, with a '-' or ':' on either side, one space. Accented
/// letters stay as they are. Simplified Han stays simplified (Nominatim writes it traditional).
pub fn normalized(s: &str) -> String {
    rules_normalized(s).trim().to_string()
}

/// The normalised form as Nominatim's rules leave it before Python strips it, with any space at
/// either end. House numbers keep it (Nominatim's house-number analysis does not strip).
pub(super) fn rules_normalized(s: &str) -> String {
    let lower: Vec<char> = s.nfkc().collect::<String>().to_lowercase().chars().collect();
    let mut first: Vec<char> = Vec::with_capacity(lower.len());
    let mut i = 0;
    while i < lower.len() {
        let c = lower[i];
        i += 1;
        if let Some(d) = ascii_digit(c) {
            first.push(d);
        } else if let Some(n) = han_numeral(c) {
            first.extend(n.chars());
        } else if c == 'n' && lower.get(i) == Some(&'°') {
            first.extend(['n', 'o']);
            i += 1;
        } else if becomes_dash(c) {
            while lower.get(i).is_some_and(|&c| becomes_dash(c)) {
                i += 1;
            }
            first.push('-');
        } else if c == 'ß' {
            first.extend(['s', 's']);
        } else if kept(c) && !in_table(c, MODIFIER_LETTER) {
            first.push(c);
        }
    }
    let mut out = String::with_capacity(first.len());
    let mut i = 0;
    while i < first.len() {
        let c = first[i];
        if in_table(c, NONSPACING_MARK_OR_FORMAT) {
            i += 1;
            continue;
        }
        let space = i + usize::from(matches!(c, '-' | ':'));
        if first.get(space).is_some_and(|c| c.is_whitespace()) {
            i = space;
            while first.get(i).is_some_and(|c| c.is_whitespace()) {
                i += 1;
            }
            if first.get(i).is_some_and(|c| matches!(c, '-' | ':')) {
                i += 1;
            }
            out.push(' ');
        } else {
            out.push(c);
            i += 1;
        }
    }
    out
}

/// Nominatim's transliteration (its ICU `transliteration` rules) of a normalised string: the
/// token the index keys and a query word is looked up by. '-' and ':' are spaces; Cyrillic and
/// Greek are spelt in Latin letters; a Latin letter is spelt in ASCII as Nominatim spells it,
/// or dropped where it has no spelling; what is no letter, digit or space is dropped, and spaces
/// are single. Other scripts stay as they are written (Nominatim spells every script in Latin
/// letters).
pub fn transliterated(norm: &str) -> String {
    spaced_token(norm).trim_matches(' ').to_string()
}

/// The token as Nominatim's rules leave it before Python strips it: with one space at either end
/// where the text had space or dropped characters there. House numbers keep it (Nominatim's
/// house-number analysis does not strip).
pub(super) fn spaced_token(norm: &str) -> String {
    let mut out = String::with_capacity(norm.len());
    let mut space = false;
    let mut buf = [0u8; 4];
    for c in latin(norm).chars() {
        if c.is_whitespace() || c == '-' || c == ':' {
            space = true;
        } else if c.is_ascii() {
            if c.is_ascii_alphanumeric() {
                push(&mut out, c.to_ascii_lowercase().encode_utf8(&mut buf), &mut space);
            }
        } else if in_table(c, LATIN_SCRIPT) {
            if let Some(spelt) = ascii_spelling(c) {
                push(&mut out, &spelt, &mut space);
            }
        } else if c.is_alphanumeric() || is_combining_mark(c) {
            push(&mut out, c.encode_utf8(&mut buf), &mut space);
        }
    }
    if space {
        out.push(' ');
    }
    out.nfc().collect()
}

/// Appends `s` to a token, after one space when spaces came before it.
fn push(out: &mut String, s: &str, space: &mut bool) {
    if *space {
        out.push(' ');
    }
    *space = false;
    out.push_str(s);
}

/// How Nominatim spells a Latin letter in ASCII: by its table, else as NFKD leaves it without
/// marks when that is ASCII letters and digits; None for a letter it drops.
fn ascii_spelling(c: char) -> Option<String> {
    if let Ok(i) = LATIN_ASCII.binary_search_by_key(&c, |&(l, _)| l) {
        return Some(LATIN_ASCII[i].1.to_string());
    }
    let fold: String = c.nfkd().filter(|&m| !is_combining_mark(m)).flat_map(char::to_lowercase).collect();
    (!fold.is_empty() && fold.chars().all(|f| f.is_ascii_alphanumeric())).then_some(fold)
}

/// The Cyrillic and Greek letters of a lower-case string in Latin letters, by the letter values
/// no country of its own changes; a letter with a mark the tables do not name is its base
/// letter's.
fn latin(lower: &str) -> String {
    let chars: Vec<char> = lower.chars().map(without_tonos).collect();
    let mut out = String::with_capacity(lower.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        i += 1;
        if c == '\u{0629}' {
            // Arabic teh marbuta, which Nominatim spells as heh
            out.push('\u{0647}');
        } else if c == 'ο' && chars.get(i) == Some(&'υ') {
            out.push_str("ou");
            i += 1;
        } else if let Some(l) = cyrillic(c, "").or_else(|| greek(c)) {
            out.push_str(l);
        } else if let Some(l) = c.to_string().nfd().next().and_then(|b| cyrillic(b, "").or_else(|| greek(b))) {
            out.push_str(l);
        } else {
            out.push(c);
        }
    }
    out
}

fn without_tonos(c: char) -> char {
    match c {
        'ά' => 'α', 'έ' => 'ε', 'ή' => 'η', 'ί' | 'ϊ' | 'ΐ' => 'ι', 'ό' => 'ο', 'ύ' | 'ϋ' | 'ΰ' => 'υ', 'ώ' => 'ω',
        c => c,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nominatim's own normaliser and transliteration (ICU 74, the oracle's rules) over a sample
    /// of the oracle's names and edge cases: input, normalised form, token, script class.
    const ORACLE_FORMS: &str = include_str!("../../testdata/normalizer.tsv");

    fn oracle_forms() -> impl Iterator<Item = [&'static str; 4]> {
        ORACLE_FORMS.lines().map(|l| {
            let f: Vec<&str> = l.split('\t').collect();
            [f[0], f[1], f[2], f[3]]
        })
    }

    #[test]
    fn normalised_forms() {
        for (name, norm) in [
            ("Frankfurt (Oder)", "frankfurt oder-"),
            ("a - b", "a  b"),
            ("n° 5", "no 5"),
            ("١٢٣ ٤", "123 4"),
            ("十二", "102"),
            ("Hawaiʻi", "hawaii"),
            ("Rue de l’Église", "rue de l-église"),
            ("İstanbul", "istanbul"),
            ("ラーメン", "ラメン"),
            ("½", "1-2"),
            ("a\u{00AD}b", "ab"),
            ("  Straße  ", "strasse"),
            ("München", "münchen"),
        ] {
            assert_eq!(normalized(name), norm, "{name}");
        }
    }

    #[test]
    fn transliterated_forms() {
        for (norm, token) in [
            ("münchen", "munchen"),
            ("frankfurt oder-", "frankfurt oder"),
            ("a  b", "a b"),
            ("rue de l-église", "rue de l eglise"),
            ("gəncə", "gnc"),
            ("łódź", "lodz"),
            ("ŋ ɛ ɔ", "n e o"),
            ("ǀai-ǀais", "ai ais"),
            ("αθήνα", "athina"),
            ("москва", "moskva"),
            ("东京", "东京"),
            ("서울", "서울"),
        ] {
            assert_eq!(transliterated(norm), token, "{norm}");
        }
    }

    #[test]
    fn normalises_as_nominatims_normaliser() {
        let wrong: Vec<String> = oracle_forms()
            .filter(|[_, _, _, class]| *class != "han")
            .filter(|[input, norm, _, _]| normalized(input) != *norm)
            .map(|[input, norm, _, _]| format!("{input:?}: {:?} not {norm:?}", normalized(input)))
            .collect();
        assert!(wrong.is_empty(), "{} of {}:\n{}", wrong.len(), ORACLE_FORMS.lines().count(), wrong.join("\n"));
    }

    #[test]
    fn transliterates_latin_as_nominatim() {
        let wrong: Vec<String> = oracle_forms()
            .filter(|[_, _, _, class]| *class == "latin")
            .filter(|[_, norm, token, _]| transliterated(norm) != *token)
            .map(|[input, norm, token, _]| format!("{input:?} {norm:?}: {:?} not {token:?}", transliterated(norm)))
            .collect();
        assert!(wrong.is_empty(), "{} of {}:\n{}", wrong.len(), ORACLE_FORMS.lines().count(), wrong.join("\n"));
    }

    #[test]
    fn transliterates_cyrillic_and_greek() {
        assert_eq!(transliterate("Первомайка", || "KZ".into()).as_deref(), Some("pervomayka"));
        assert_eq!(transliterate("Широке", || "UA".into()).as_deref(), Some("shyroke"));
        assert_eq!(transliterate("Москва", || "RU".into()).as_deref(), Some("moskva"));
        assert_eq!(transliterate("Чачак", || "RS".into()).as_deref(), Some("cacak"));
        assert_eq!(transliterate("Αθήνα", || "GR".into()).as_deref(), Some("athina"));
        assert_eq!(transliterate("Θεσσαλονίκη", || "GR".into()).as_deref(), Some("thessaloniki"));
        assert_eq!(transliterate("Paris", || "FR".into()), None);
        assert_eq!(transliterate("Paris", || panic!("country asked for a Latin name")), None);
    }

    #[test]
    fn folds_diacritics_case_and_punctuation() {
        assert_eq!(normalize("São José"), "sao jose");
        assert_eq!(normalize("München"), "munchen");
        assert_eq!(normalize("'s-Gravenhage"), "s gravenhage");
        assert_eq!(normalize("Frankfurt (Oder)"), "frankfurt oder");
        assert_eq!(normalize("Straße"), "strasse");
        assert_eq!(normalize("Tromsø"), "tromso");
        assert_eq!(normalize("Łódź"), "lodz");
        assert_eq!(normalize("Москва"), "москва");
        assert_eq!(normalize("Αθήνα"), "αθηνα");
    }

    #[test]
    fn folds_saint_mount_fort() {
        assert_eq!(normalize("Saint-Étienne"), "st etienne");
        assert_eq!(normalize("St. Louis"), "st louis");
        assert_eq!(normalize("Sankt Gallen"), "st gallen");
        assert_eq!(normalize("Mount Vernon"), "mt vernon");
        assert_eq!(normalize("Fort Worth"), "ft worth");
        assert_eq!(normalize("Sainte-Foy"), "ste foy");
    }

    #[test]
    fn keeps_vowel_marks_outside_alphabetic_scripts() {
        assert_eq!(normalize("दिल्ली"), "दिल्ली");
        assert_ne!(normalize("กรุงเทพ"), "");
    }

    #[test]
    fn decomposed_hangul_is_ideographic() {
        let seoul = normalize("서울");
        assert!(seoul.chars().all(|c| ('\u{1100}'..'\u{1200}').contains(&c)), "{seoul:?}");
        assert!(is_ideographic(&seoul));
        assert!(is_ideographic("서울"));
    }

    #[test]
    fn keeps_raw_token_for_prefix_typing() {
        let t = tokens("fort");
        assert_eq!(t[0].folded, "ft");
        assert_eq!(t[0].raw, "fort");
    }
}
