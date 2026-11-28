//! Postcodes in a query, as Nominatim's postcode parser finds them: every run of query words that
//! starts like one of the countries' postcode patterns, with or without a country code before it,
//! and the code each matching pattern writes for it.

use std::collections::BTreeSet;

use super::Break;

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

/// The few regular expressions the postcode patterns are written in: characters, classes of
/// characters and ranges, groups (capturing or `(?:`), alternatives, and `?` or `{n}` after an
/// item. Matched as Python's re matches: greedy, alternatives in order, first match wins.
struct Regex {
    alternatives: Vec<Vec<Item>>,
    groups: usize,
}

struct Item {
    atom: Atom,
    min: usize,
    max: usize,
}

enum Atom {
    Char(char),
    Class(Vec<(char, char)>),
    Group(Option<usize>, Vec<Vec<Item>>),
}

/// A match from the start of the text: where it ends and each group's span (index 1 for group 1).
struct Match {
    end: usize,
    groups: Vec<Option<(usize, usize)>>,
}

impl Match {
    /// Python's `match.expand(template)` for templates of `\1`..`\9` and plain characters; a group
    /// that took no part is empty.
    fn expand(&self, template: &str, text: &[char]) -> String {
        let mut out = String::new();
        let mut it = template.chars();
        while let Some(c) = it.next() {
            match c {
                '\\' => {
                    let g = it.next().and_then(|d| d.to_digit(10)).expect("a group number") as usize;
                    if let Some(Some((s, e))) = self.groups.get(g) {
                        out.extend(&text[*s..*e]);
                    }
                }
                c => out.push(c),
            }
        }
        out
    }
}

type Captures = Vec<Option<(usize, usize)>>;

impl Regex {
    fn parse(pattern: &str) -> Result<Regex, String> {
        let chars: Vec<char> = pattern.chars().collect();
        let mut at = 0;
        let mut groups = 0;
        let alternatives = Self::alternatives(&chars, &mut at, &mut groups)?;
        if at != chars.len() {
            return Err(format!("unread {:?} in {pattern:?}", &chars[at..]));
        }
        Ok(Regex { alternatives, groups })
    }

    fn alternatives(p: &[char], at: &mut usize, groups: &mut usize) -> Result<Vec<Vec<Item>>, String> {
        let mut out = vec![Self::sequence(p, at, groups)?];
        while p.get(*at) == Some(&'|') {
            *at += 1;
            out.push(Self::sequence(p, at, groups)?);
        }
        Ok(out)
    }

    fn sequence(p: &[char], at: &mut usize, groups: &mut usize) -> Result<Vec<Item>, String> {
        let mut items = Vec::new();
        while let Some(&c) = p.get(*at) {
            let atom = match c {
                '|' | ')' => break,
                '(' => {
                    *at += 1;
                    let capture = if p.get(*at..*at + 2) == Some(&['?', ':']) {
                        *at += 2;
                        None
                    } else {
                        *groups += 1;
                        Some(*groups)
                    };
                    let inner = Self::alternatives(p, at, groups)?;
                    if p.get(*at) != Some(&')') {
                        return Err("an unclosed group".into());
                    }
                    *at += 1;
                    Atom::Group(capture, inner)
                }
                '[' => {
                    let close = p[*at..].iter().position(|&c| c == ']').ok_or("an unclosed class")? + *at;
                    let class = &p[*at + 1..close];
                    let mut ranges = Vec::new();
                    let mut i = 0;
                    while i < class.len() {
                        if i + 2 < class.len() && class[i + 1] == '-' {
                            ranges.push((class[i], class[i + 2]));
                            i += 3;
                        } else {
                            ranges.push((class[i], class[i]));
                            i += 1;
                        }
                    }
                    *at = close + 1;
                    Atom::Class(ranges)
                }
                '\\' | '*' | '+' | '.' | '^' | '$' | '{' | '?' => return Err(format!("{c:?} is not read")),
                c => {
                    *at += 1;
                    Atom::Char(c)
                }
            };
            let (min, max) = match p.get(*at) {
                Some('?') => {
                    *at += 1;
                    (0, 1)
                }
                Some('{') => {
                    let close = p[*at..].iter().position(|&c| c == '}').ok_or("an unclosed count")? + *at;
                    let n: usize = String::from_iter(&p[*at + 1..close]).parse().map_err(|_| "a count that is no number")?;
                    *at = close + 1;
                    (n, n)
                }
                _ => (1, 1),
            };
            items.push(Item { atom, min, max });
        }
        Ok(items)
    }

    /// The first match from the start of `text`, in the order Python's re tries them.
    fn match_prefix(&self, text: &[char]) -> Option<Match> {
        let mut found = None;
        let mut caps: Captures = vec![None; self.groups + 1];
        let matcher = Matcher { text };
        for alternative in &self.alternatives {
            if matcher.sequence(alternative, 0, &mut caps, &mut |end, caps| {
                found = Some(Match { end, groups: caps.clone() });
                true
            }) {
                break;
            }
        }
        found
    }
}

struct Matcher<'t> {
    text: &'t [char],
}

type Then<'k> = &'k mut dyn FnMut(usize, &mut Captures) -> bool;

impl Matcher<'_> {
    fn sequence(&self, items: &[Item], at: usize, caps: &mut Captures, then: Then) -> bool {
        match items.split_first() {
            None => then(at, caps),
            Some((item, rest)) => self.repeat(item, 0, at, caps, &mut |next, caps| self.sequence(rest, next, caps, &mut *then)),
        }
    }

    /// The item `done` times so far: once more while it may (greedy), else what follows.
    fn repeat(&self, item: &Item, done: usize, at: usize, caps: &mut Captures, then: Then) -> bool {
        if done < item.max && self.atom(&item.atom, at, caps, &mut |next, caps| self.repeat(item, done + 1, next, caps, &mut *then)) {
            return true;
        }
        done >= item.min && then(at, caps)
    }

    fn atom(&self, atom: &Atom, at: usize, caps: &mut Captures, then: Then) -> bool {
        match atom {
            Atom::Char(c) => self.text.get(at) == Some(c) && then(at + 1, caps),
            Atom::Class(ranges) => {
                self.text.get(at).is_some_and(|c| ranges.iter().any(|&(lo, hi)| (lo..=hi).contains(c))) && then(at + 1, caps)
            }
            Atom::Group(capture, alternatives) => alternatives.iter().any(|alternative| {
                self.sequence(alternative, at, caps, &mut |end, caps| match capture {
                    Some(g) => {
                        let before = caps[*g].replace((at, end));
                        let matched = then(end, caps);
                        if !matched {
                            caps[*g] = before;
                        }
                        matched
                    }
                    None => then(end, caps),
                })
            }),
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
