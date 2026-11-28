//! Postcodes in a query, as Nominatim's postcode parser finds them: every run of query words that
//! starts like one of the countries' postcode patterns, with or without a country code before it,
//! and the code each matching pattern writes for it. And a place's postcode as Nominatim's
//! formatter writes it, in its country's format.

use std::collections::{BTreeSet, HashMap};
use std::sync::OnceLock;

use super::regex::{is_python_space, Regex};
use super::Break;
use crate::constants::POSTCODE_DEFAULT_PATTERN;

/// The postcode formats of the countries, in country order: country, pattern (`d` a digit, `l` a
/// capital letter, otherwise a regular expression) and the output, a template of the pattern's
/// groups, or None for the match as it is. The builder validates postcodes with the same table
/// (`builder/src/postcode_validation.h`, `kPostcodeFormats`); a test keeps them equal.
pub const POSTCODE_FORMATS: &[(&str, &str, Option<&str>)] = &[
    ("ad", r"(ddd)", Some(r"AD\1")),
    ("af", r"dddd", None),
    ("ai", r"2640", Some(r"AI-2640")),
    ("al", r"dddd", None),
    ("am", r"dddd", None),
    ("ar", r"l?dddd(?:lll)?", None),
    ("at", r"dddd", None),
    ("au", r"dddd", None),
    ("az", r"dddd", None),
    ("ba", r"ddddd", None),
    ("bb", r"(ddddd)", Some(r"BB\1")),
    ("bd", r"dddd", None),
    ("be", r"dddd", None),
    ("bg", r"dddd", None),
    ("bh", r"d?ddd", None),
    ("bm", r"(ll)[ -]?(dd)", Some(r"\1 \2")),
    ("bn", r"(ll) ?(dddd)", Some(r"\1\2")),
    ("br", r"(ddddd)-?(ddd)", Some(r"\1-\2")),
    ("bt", r"ddddd", None),
    ("by", r"dddddd", None),
    ("ca", r"(ldl) ?(dld)", Some(r"\1 \2")),
    ("ch", r"dddd", None),
    ("cl", r"ddddddd", None),
    ("cn", r"dddddd", None),
    ("co", r"dddddd", None),
    ("cr", r"ddddd", None),
    ("cu", r"ddddd", None),
    ("cv", r"dddd", None),
    ("cy", r"(?:99|d)ddd", None),
    ("cz", r"(ddd) ?(dd)", Some(r"\1 \2")),
    ("de", r"ddddd", None),
    ("dk", r"dddd", None),
    ("do", r"ddddd", None),
    ("dz", r"ddddd", None),
    ("ec", r"dddddd", None),
    ("ee", r"ddddd", None),
    ("eg", r"ddddd", None),
    ("es", r"ddddd", None),
    ("et", r"dddd", None),
    ("fi", r"ddddd", None),
    ("fk", r"FIQQ ?1ZZ", Some(r"FIQQ 1ZZ")),
    ("fm", r"ddddd", None),
    ("fo", r"ddd", None),
    ("fr", r"ddddd", None),
    ("gb", r"(l?ld[A-Z0-9]?) ?(dll)", Some(r"\1 \2")),
    ("ge", r"dddd", None),
    ("gg", r"(GYdd?) ?(dll)", Some(r"\1 \2")),
    ("gh", r"ll-d?ddd-dddd", None),
    ("gi", r"(GX11) ?(1AA)", Some(r"GX11 1AA")),
    ("gl", r"dddd", None),
    ("gn", r"ddd", None),
    ("gr", r"(ddd) ?(dd)", Some(r"\1 \2")),
    ("gs", r"(SIQQ) ?(1ZZ)", Some(r"\1 \2")),
    ("gt", r"ddddd", None),
    ("gw", r"dddd", None),
    ("hn", r"ddddd", None),
    ("hr", r"ddddd", None),
    ("ht", r"dddd", None),
    ("hu", r"dddd", None),
    ("id", r"ddddd", None),
    ("ie", r"(ldd) ?([0123456789ACDEFHKNPRTVWXY]{4})", Some(r"\1 \2")),
    ("il", r"ddddddd", None),
    ("im", r"(IMdd?) ?(dll)", Some(r"\1 \2")),
    ("in", r"(ddd) ?(ddd)", Some(r"\1\2")),
    ("io", r"(BBND) ?(1ZZ)", Some(r"\1 \2")),
    ("iq", r"ddddd", None),
    ("ir", r"(ddddd)[-_ ]?(ddddd)", Some(r"\1-\2")),
    ("is", r"ddd", None),
    ("it", r"ddddd", None),
    ("je", r"(JEdd?) ?(dll)", Some(r"\1 \2")),
    ("jo", r"ddddd", None),
    ("jp", r"(ddd)-?(dddd)", Some(r"\1-\2")),
    ("ke", r"ddddd", None),
    ("kg", r"dddddd", None),
    ("kh", r"dddddd", None),
    ("kn", r"dddd", None),
    ("kr", r"ddddd", None),
    ("kw", r"ddddd", None),
    ("ky", r"(d)-(dddd)", Some(r"KY\1-\2")),
    ("kz", r"(?:lddldld|dddddd)", None),
    ("la", r"ddddd", None),
    ("lb", r"(dddd)(?: ?dddd)?", None),
    ("lc", r"(dd) ?(ddd)", Some(r"LC\1 \2")),
    ("li", r"dddd", None),
    ("lk", r"ddddd", None),
    ("lr", r"dddd", None),
    ("ls", r"ddd", None),
    ("lt", r"ddddd", None),
    ("lu", r"dddd", None),
    ("lv", r"(dddd)", Some(r"LV-\1")),
    ("ma", r"ddddd", None),
    ("mc", r"980dd", None),
    ("md", r"(dddd)", Some(r"MD-\1")),
    ("me", r"ddddd", None),
    ("mg", r"ddd", None),
    ("mh", r"ddddd", None),
    ("mk", r"dddd", None),
    ("mm", r"ddddd", None),
    ("mn", r"ddddd", None),
    ("mt", r"(lll) ?(dddd)", Some(r"\1 \2")),
    ("mu", r"ddddd", None),
    ("mv", r"ddddd", None),
    ("mx", r"ddddd", None),
    ("my", r"ddddd", None),
    ("mz", r"(dddd)(?:-dd)?", None),
    ("na", r"ddddd", None),
    ("ne", r"dddd", None),
    ("ng", r"dddddd", None),
    ("ng", r"(ll)[ -]?(dd)[ -]?([A-Z0-9]{3})[ -]?(ll)[ -]?(dd)", Some(r"\1-\2-\3-\4-\5")),
    ("ni", r"ddddd", None),
    ("nl", r"(dddd) ?(ll)", Some(r"\1 \2")),
    ("no", r"dddd", None),
    ("np", r"ddddd", None),
    ("nz", r"dddd", None),
    ("om", r"ddd", None),
    ("pa", r"dddd", None),
    ("pe", r"ddddd", None),
    ("pg", r"ddd", None),
    ("ph", r"dddd", None),
    ("pk", r"ddddd", None),
    ("pl", r"(dd)[ -]?(ddd)", Some(r"\1-\2")),
    ("pn", r"(PCRN) ?(1ZZ)", Some(r"\1 \2")),
    ("ps", r"ddd", None),
    ("pt", r"dddd(?:-ddd)?", None),
    ("pw", r"969(39|40)", None),
    ("py", r"dddddd", None),
    ("ro", r"dddddd", None),
    ("rs", r"ddddd", None),
    ("ru", r"dddddd", None),
    ("sa", r"ddddd(?:-dddd)?", None),
    ("sd", r"ddddd", None),
    ("se", r"(ddd) ?(dd)", Some(r"\1 \2")),
    ("sg", r"dddddd", None),
    ("sh", r"(ASCN|STHL|TDCU) ?(1ZZ)", Some(r"\1 \2")),
    ("si", r"dddd", None),
    ("sk", r"(ddd) ?(dd)", Some(r"\1 \2")),
    ("sm", r"4789d", None),
    ("sn", r"ddddd", None),
    ("so", r"(ll) ?(ddddd)", Some(r"\1 \2")),
    ("sv", r"dddd", None),
    ("sz", r"lddd", None),
    ("tc", r"(TKCA) ?(1ZZ)", Some(r"\1 \2")),
    ("th", r"ddddd", None),
    ("tj", r"dddddd", None),
    ("tm", r"dddddd", None),
    ("tn", r"dddd", None),
    ("tr", r"ddddd", None),
    ("tt", r"dddddd", None),
    ("tw", r"ddd(?:ddd?)?", None),
    ("tz", r"ddddd", None),
    ("ua", r"d?ddddd", None),
    ("us", r"(ddddd)(?:-dddd)?", Some(r"\1")),
    ("uy", r"ddddd", None),
    ("uz", r"dddddd", None),
    ("va", r"00120", None),
    ("vc", r"(dddd)", Some(r"VC\1")),
    ("ve", r"dddd", None),
    ("vg", r"(dddd)", Some(r"VG\1")),
    ("vn", r"ddddd", None),
    ("xk", r"ddddd", None),
    ("za", r"dddd", None),
    ("zm", r"dddd", None),
];

/// The countries Nominatim takes no postcode in (`postcode: no` in its country settings). The
/// builder lists the same (`country_without_postcodes`); a test keeps them equal.
pub const COUNTRIES_WITHOUT_POSTCODES: [&str; 54] = [
    "ae", "ag", "ao", "bf", "bi", "bj", "bo", "bs", "bw", "bz", "cd", "cf", "cg", "ci", "ck", "cm", "dj", "dm", "er", "fj", "ga",
    "gd", "gm", "gq", "gy", "jm", "ki", "km", "kp", "ly", "ml", "mr", "mw", "nr", "nu", "qa", "rw", "sb", "sc", "sl", "sr", "ss",
    "st", "sy", "td", "tg", "tk", "tl", "to", "tv", "ug", "vu", "ye", "zw",
];

/// One postcode pattern of a country, as Nominatim's SimplePostcodeMatcher reads a place's
/// postcode with it: `norm` is `(?:CC[ -]?)?(pattern)` (the white space either side of it is read
/// apart), `pattern` the pattern alone.
struct Format {
    norm: Regex,
    pattern: Regex,
    output: Option<&'static str>,
}

impl Format {
    fn new(country: &str, pattern: &str, output: Option<&'static str>) -> Format {
        let pattern = pattern.replace('d', "[0-9]").replace('l', "[A-Z]");
        let norm = format!("(?:{}[ -]?)?({pattern})", country.to_ascii_uppercase());
        Format { norm: Regex::parse(&norm).expect("a postcode pattern"), pattern: Regex::parse(&pattern).expect("a postcode pattern"), output }
    }

    /// The postcode `upper` (upper case) reads as, as Python's `norm_pattern.fullmatch` and then
    /// `pattern.fullmatch` of its group read it: white space first, as much as Python's greedy
    /// `\s*` takes; None when it does not read, or reads as nothing but zeros, '-' and ' '.
    fn read(&self, upper: &[char]) -> Option<String> {
        let lead = upper.iter().take_while(|&&c| is_python_space(c)).count();
        for start in (0..=lead).rev() {
            let text = &upper[start..];
            let Some(norm) = self.norm.first_match(text, |end| text[end..].iter().all(|&c| is_python_space(c))) else { continue };
            let (from, to) = norm.groups[1]?;
            let code = &text[from..to];
            let m = self.pattern.first_match(code, |end| end == code.len())?;
            if code.iter().all(|c| matches!(c, '0' | '-' | ' ')) {
                return None;
            }
            return Some(match self.output {
                Some(template) => m.expand(template, code),
                None => String::from_iter(code),
            });
        }
        None
    }
}

/// The countries' postcode formats, as Nominatim's PostcodeFormatter holds them for its
/// clean-postcodes sanitizer: each country's patterns in order, and the default for a country of
/// postcodes and no pattern of its own.
pub struct PostcodeFormats {
    countries: HashMap<&'static str, Vec<Format>>,
    default: Format,
}

impl PostcodeFormats {
    /// The formats, read once.
    pub fn get() -> &'static PostcodeFormats {
        static FORMATS: OnceLock<PostcodeFormats> = OnceLock::new();
        FORMATS.get_or_init(|| {
            let mut countries: HashMap<&'static str, Vec<Format>> = HashMap::new();
            for &(cc, pattern, output) in POSTCODE_FORMATS {
                countries.entry(cc).or_default().push(Format::new(cc, pattern, output));
            }
            PostcodeFormats { countries, default: Format::new("", POSTCODE_DEFAULT_PATTERN, None) }
        })
    }

    /// A place's postcode `value` in its country's format, for a place in `country` (a lower-case
    /// code, "" for none): the output of the first of the country's patterns that reads it, upper
    /// case, with or without the country's code before it. None for a place of no country or of
    /// one without postcodes, and for a value no pattern reads.
    pub fn formatted(&self, country: &str, value: &str) -> Option<String> {
        if country.is_empty() || COUNTRIES_WITHOUT_POSTCODES.contains(&country) {
            return None;
        }
        let upper: Vec<char> = value.to_uppercase().chars().collect();
        match self.countries.get(country) {
            Some(formats) => formats.iter().find_map(|f| f.read(&upper)),
            None => self.default.read(&upper),
        }
    }
}

/// The characters a postcode may be followed by in the word the parser matches: a phrase break,
/// a word break, a colon or the end of the query.
const POSTCODE_END: [char; 4] = [':', ',', ' ', '>'];
/// The characters of that word that stand for a break between two query nodes.
const NODE_BREAKS: [char; 5] = [' ', ',', '-', ':', '>'];

/// A query node as the parser reads it: the break before it and its word in normalised form
/// (the final node, whose break is `Break::End`, has no word).
#[derive(Clone, Copy, Debug)]
pub struct Node<'a> {
    pub btype: Break,
    pub word: &'a str,
}

/// A postcode found in a query: the nodes it spans (`start..end`) and the code it reads as.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Postcode {
    pub start: usize,
    pub end: usize,
    pub code: String,
}

/// The countries' postcode patterns, each once, with the outputs it has and the countries of each.
pub struct PostcodeParser {
    patterns: Vec<(Regex, Vec<(Option<&'static str>, Vec<String>)>)>,
}

impl Default for PostcodeParser {
    fn default() -> Self {
        let mut patterns: Vec<(String, Vec<(Option<&'static str>, Vec<String>)>)> = Vec::new();
        for &(cc, pattern, output) in POSTCODE_FORMATS {
            let pattern = pattern.replace('d', "[0-9]").replace('l', "[A-Z]");
            let i = match patterns.iter().position(|(p, _)| *p == pattern) {
                Some(i) => i,
                None => {
                    patterns.push((pattern, Vec::new()));
                    patterns.len() - 1
                }
            };
            let outputs = &mut patterns[i].1;
            match outputs.iter_mut().find(|(o, _)| *o == output) {
                Some((_, ccs)) => ccs.push(cc.to_ascii_uppercase()),
                None => outputs.push((output, vec![cc.to_ascii_uppercase()])),
            }
        }
        let patterns = patterns
            .into_iter()
            .map(|(p, outputs)| {
                let ends = String::from_iter(POSTCODE_END);
                (Regex::parse(&format!("{p}[{ends}]")).expect("a postcode pattern"), outputs)
            })
            .collect();
        PostcodeParser { patterns }
    }
}

impl PostcodeParser {
    /// Every postcode of a query of nodes (the end node last), as Nominatim's parser reads them:
    /// within each phrase, each word that starts at a phrase or word break is read with the words
    /// after it up to the phrase's end, upper case, as `[country code[ -]]postcode<break>...`.
    pub fn parse(&self, nodes: &[Node]) -> BTreeSet<Postcode> {
        let mut found = BTreeSet::new();
        let slots = nodes.len().saturating_sub(1);
        let mut start = 0;
        while start < slots {
            let mut end = start + 1;
            while end < slots && !matches!(nodes[end].btype, Break::End | Break::Phrase | Break::SoftPhrase) {
                end += 1;
            }
            let mut word: Vec<char> = vec![Break::Phrase.char()];
            let mut breaks = vec![end];
            for (at, node) in nodes[start..end].iter().enumerate().rev().map(|(i, n)| (start + i, n)) {
                word.splice(0..0, node.word.to_uppercase().chars());
                match node.btype {
                    Break::Start | Break::Phrase | Break::SoftPhrase | Break::Word => {
                        breaks.insert(0, at);
                        self.read(&word, &breaks, &mut found);
                        word.insert(0, node.btype.char());
                    }
                    Break::Part => {
                        breaks.insert(0, at);
                        word.insert(0, Break::Part.char());
                    }
                    Break::Token | Break::End => {}
                }
            }
            start = end;
        }
        found
    }

    /// The postcodes `word` starts with. A country code is read first when it can be, so only
    /// that country's patterns count then; `breaks` are the nodes at the word's breaks.
    fn read(&self, word: &[char], breaks: &[usize], found: &mut BTreeSet<Postcode>) {
        let starts_postcode = |at: usize| self.patterns.iter().any(|(p, _)| p.match_prefix(&word[at..]).is_some());
        let letters = word.len() >= 2 && word[..2].iter().all(|c| c.is_ascii_uppercase());
        let spaced = letters && word.get(2).is_some_and(|c| matches!(c, ' ' | '-'));
        let (cc, cc_breaks, at) = if spaced && starts_postcode(3) {
            (Some(String::from_iter(&word[..2])), 1, 3)
        } else if letters && starts_postcode(2) {
            (Some(String::from_iter(&word[..2])), 0, 2)
        } else if starts_postcode(0) {
            (None, 0, 0)
        } else {
            return;
        };
        let rest = &word[at..];
        for (pattern, outputs) in &self.patterns {
            let Some(m) = pattern.match_prefix(rest) else { continue };
            let matched = &rest[..m.end];
            // A node's word holds no break character, so every one in the match is a node's break.
            let Some(&end) = breaks.get(cc_breaks + matched.iter().filter(|c| NODE_BREAKS.contains(c)).count()) else { continue };
            for (output, ccs) in outputs {
                if cc.as_ref().is_some_and(|cc| !ccs.contains(cc)) {
                    continue;
                }
                let code = match output {
                    Some(template) => m.expand(template, rest),
                    None => String::from_iter(&matched[..matched.len() - 1]),
                };
                found.insert(Postcode { start: breaks[0], end, code });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nominatim's own parser on queries its own analyzer split into nodes: query, nodes (break
    /// character and word each, '|' between), postcodes (start,end,code, ';' between).
    const ORACLE: &str = include_str!("../../testdata/postcodes.tsv");

    fn nodes(written: &str) -> Vec<Node<'_>> {
        written
            .split('|')
            .map(|n| {
                let b = n.chars().next().expect("a break");
                Node { btype: Break::from_char(b).expect("a break character"), word: &n[b.len_utf8()..] }
            })
            .collect()
    }

    fn codes(parser: &PostcodeParser, written: &str) -> String {
        let found: Vec<String> = parser.parse(&nodes(written)).into_iter().map(|p| format!("{},{},{}", p.start, p.end, p.code)).collect();
        found.join(";")
    }

    #[test]
    fn finds_the_postcodes_nominatim_finds() {
        let parser = PostcodeParser::default();
        for line in ORACLE.lines() {
            let f: Vec<&str> = line.split('\t').collect();
            assert_eq!(codes(&parser, f[1]), f[2], "{:?}", f[0]);
        }
        assert_eq!(ORACLE.lines().count(), 82);
    }

    #[test]
    fn reads_the_forms_of_a_postcode() {
        let parser = PostcodeParser::default();
        for (query, has) in [
            ("<2361| ab|>", "0,2,2361 AB"),
            ("<2361ab|>", "0,1,2361 AB"),
            ("<nl|-2361| ab|>", "0,3,2361 AB"),
            ("<78746|-1234|>", "0,2,78746"),
            ("<110| 00|>", "0,2,110 00"),
            ("<sw1a| 1aa|>", "0,2,SW1A 1AA"),
            ("<1044| liberty| park| drive|>", "0,1,LV-1044"),
            ("<1044| liberty| park| drive|>", "0,1,1044"),
        ] {
            assert!(codes(&parser, query).split(';').any(|c| c == has), "{query}: {}", codes(&parser, query));
        }
    }

    #[test]
    fn a_country_code_takes_only_its_countrys_patterns() {
        let parser = PostcodeParser::default();
        assert_eq!(codes(&parser, "<lv|-1044|>"), "0,2,LV-1044");
        // "VL" reads as a country code, which no pattern of "T1117" belongs to.
        assert_eq!(codes(&parser, "<valletta| vlt1117|>"), "");
    }

    #[test]
    fn nodes_out_of_shape_do_not_panic() {
        let parser = PostcodeParser::default();
        assert_eq!(codes(&parser, "<1044"), "");
        let spaced = [Node { btype: Break::Start, word: "2361 ab" }, Node { btype: Break::End, word: "" }];
        assert!(parser.parse(&spaced).iter().all(|p| p.end <= 1));
        assert!(parser.parse(&[]).is_empty());
    }

    #[test]
    fn formats_a_places_postcode_as_nominatims_formatter() {
        let formats = PostcodeFormats::get();
        for (country, value, code) in [
            ("nl", "NL-2361AB", Some("2361 AB")),
            ("nl", "2361ab", Some("2361 AB")),
            ("nl", "\u{1C} 2361 AB\u{1F}", Some("2361 AB")),
            ("us", "78746-1234", Some("78746")),
            ("ad", "500", Some("AD500")),
            ("ad", "AD500", Some("AD500")),
            ("cz", "CZ 110 00", Some("110 00")),
            ("ng", "123456", Some("123456")),
            ("ng", "AB 12 3CD EF 45", Some("AB-12-3CD-EF-45")),
            ("gb", "sw1a1aa", Some("SW1A 1AA")),
            ("pr", "pr-00901", Some("PR-00901")),
            ("pr", "a b c", Some("A B C")),
            ("pr", "straße", Some("STRASSE")),
            ("nl", "Amsterdam", None),
            ("de", "00000", None),
            ("ae", "12345", None),
            ("", "12345", None),
            ("pr", "ab", None),
            ("pr", "1234567890123", None),
        ] {
            assert_eq!(formats.formatted(country, value).as_deref(), code, "{country} {value:?}");
        }
    }

    #[test]
    fn the_countries_without_postcodes_are_the_builders() {
        let header = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../builder/src/postcode_validation.h"))
            .expect("builder/src/postcode_validation.h");
        let body = &header[header.find("country_without_postcodes(").expect("country_without_postcodes")..];
        let body = &body[..body.find("return true;").expect("its list")];
        let theirs: Vec<String> = body
            .lines()
            .filter(|l| l.trim().starts_with("case ("))
            .map(|l| l.split('\'').skip(1).step_by(2).collect())
            .collect();
        assert_eq!(COUNTRIES_WITHOUT_POSTCODES.to_vec(), theirs);
        let default = format!("compile(\"\", \"{POSTCODE_DEFAULT_PATTERN}\", nullptr)");
        assert!(header.contains(&default), "the builder reads a feature's postcode by another default");
    }

    #[test]
    fn the_table_is_the_builders() {
        let header = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../builder/src/postcode_validation.h"))
            .expect("builder/src/postcode_validation.h");
        let body = &header[header.find("kPostcodeFormats[] = {").expect("kPostcodeFormats")..];
        let body = &body[body.find('{').unwrap() + 1..body.find("};").unwrap()];
        let unquote = |s: &str| s.trim().trim_matches('"').replace("\\\\", "\\");
        let rows: Vec<(String, String, Option<String>)> = body
            .lines()
            .map(str::trim)
            .filter(|l| l.starts_with('{'))
            .map(|l| {
                let f: Vec<&str> = l.trim_start_matches('{').trim_end_matches("},").splitn(3, ", ").collect();
                (unquote(f[0]), unquote(f[1]), (f[2].trim() != "nullptr").then(|| unquote(f[2])))
            })
            .collect();
        let ours: Vec<(String, String, Option<String>)> =
            POSTCODE_FORMATS.iter().map(|&(c, p, o)| (c.to_string(), p.to_string(), o.map(str::to_string))).collect();
        assert_eq!(ours, rows);
    }
}
