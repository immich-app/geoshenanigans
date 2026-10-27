use unicode_normalization::{char::is_combining_mark, UnicodeNormalization};

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
/// countries; None when the name has neither script.
pub fn transliterate(s: &str, cc: &str) -> Option<String> {
    let lower: String = normalize_marks_lower(s);
    if !lower.chars().any(|c| ('\u{0370}'..='\u{04FF}').contains(&c)) {
        return None;
    }
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
        .map(|c| match c {
            'ά' => 'α', 'έ' => 'ε', 'ή' => 'η', 'ί' | 'ϊ' | 'ΐ' => 'ι', 'ό' => 'ο', 'ύ' | 'ϋ' | 'ΰ' => 'υ', 'ώ' => 'ω',
            x => x,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transliterates_cyrillic_and_greek() {
        assert_eq!(transliterate("Первомайка", "KZ").as_deref(), Some("pervomayka"));
        assert_eq!(transliterate("Широке", "UA").as_deref(), Some("shyroke"));
        assert_eq!(transliterate("Москва", "RU").as_deref(), Some("moskva"));
        assert_eq!(transliterate("Чачак", "RS").as_deref(), Some("cacak"));
        assert_eq!(transliterate("Αθήνα", "GR").as_deref(), Some("athina"));
        assert_eq!(transliterate("Θεσσαλονίκη", "GR").as_deref(), Some("thessaloniki"));
        assert_eq!(transliterate("Paris", "FR"), None);
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
