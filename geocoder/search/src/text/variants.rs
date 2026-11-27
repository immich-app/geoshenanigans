//! Abbreviation variants of names by language: Photon's lists (`data/variants`, Apache-2.0, see
//! CREDITS there), applied as Nominatim applies its own copy of them. A name gets the variants of
//! the languages Nominatim gives it: its key's language, or for a name without one the default
//! languages of its country. Variants are made for the index only; a query is never varied.

use std::collections::{BTreeSet, HashMap};
use std::sync::OnceLock;

use super::normalize::{normalized, transliterated};
use crate::constants::{NAME_LANGUAGES, NAME_SUFFIXES_WITHOUT_LANGUAGE, VARIANT_MAX_PARTIALS};

/// Photon's rule files by language, without Croatian, which neither Photon's analyzer nor
/// Nominatim applies.
const RULE_FILES: [(&str, &str); 29] = [
    ("bg", include_str!("../../data/variants/by-lang-bg.yaml")),
    ("ca", include_str!("../../data/variants/by-lang-ca.yaml")),
    ("cs", include_str!("../../data/variants/by-lang-cs.yaml")),
    ("da", include_str!("../../data/variants/by-lang-da.yaml")),
    ("de", include_str!("../../data/variants/by-lang-de.yaml")),
    ("el", include_str!("../../data/variants/by-lang-el.yaml")),
    ("en", include_str!("../../data/variants/by-lang-en.yaml")),
    ("es", include_str!("../../data/variants/by-lang-es.yaml")),
    ("et", include_str!("../../data/variants/by-lang-et.yaml")),
    ("eu", include_str!("../../data/variants/by-lang-eu.yaml")),
    ("fi", include_str!("../../data/variants/by-lang-fi.yaml")),
    ("fr", include_str!("../../data/variants/by-lang-fr.yaml")),
    ("gl", include_str!("../../data/variants/by-lang-gl.yaml")),
    ("hu", include_str!("../../data/variants/by-lang-hu.yaml")),
    ("it", include_str!("../../data/variants/by-lang-it.yaml")),
    ("mg", include_str!("../../data/variants/by-lang-mg.yaml")),
    ("ms", include_str!("../../data/variants/by-lang-ms.yaml")),
    ("nl", include_str!("../../data/variants/by-lang-nl.yaml")),
    ("no", include_str!("../../data/variants/by-lang-no.yaml")),
    ("pl", include_str!("../../data/variants/by-lang-pl.yaml")),
    ("pt", include_str!("../../data/variants/by-lang-pt.yaml")),
    ("ro", include_str!("../../data/variants/by-lang-ro.yaml")),
    ("ru", include_str!("../../data/variants/by-lang-ru.yaml")),
    ("sk", include_str!("../../data/variants/by-lang-sk.yaml")),
    ("sl", include_str!("../../data/variants/by-lang-sl.yaml")),
    ("sv", include_str!("../../data/variants/by-lang-sv.yaml")),
    ("tr", include_str!("../../data/variants/by-lang-tr.yaml")),
    ("uk", include_str!("../../data/variants/by-lang-uk.yaml")),
    ("vi", include_str!("../../data/variants/by-lang-vi.yaml")),
];
const COUNTRY_DEFAULTS: &str = include_str!("../../data/variants/language-defaults-per-country.yaml");
const COUNTRY_LANGUAGES: &str = include_str!("../../data/variants/country-languages.tsv");

/// Letters a language spells in a second way too, every occurrence each way (German: ä as ae).
const MUTATIONS: [(&str, char, &str); 3] = [("de", 'ä', "ae"), ("de", 'ö', "oe"), ("de", 'ü', "ue")];

/// The token (transliterated) a name or variant is indexed by and the normalised form it was
/// spelt from, which a query's match is measured against.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Term {
    pub token: String,
    pub lookup: String,
}

/// The rule lines of a rule file, by the language of their section.
pub fn rule_lines(file: &str) -> Vec<(&str, &str)> {
    let mut lang = "";
    let mut out = Vec::new();
    for line in file.lines() {
        let line = line.trim_end();
        let body = line.trim_start();
        if body.is_empty() || body.starts_with('#') {
            continue;
        }
        if let Some(l) = body.strip_prefix("- lang:") {
            lang = l.trim().trim_matches('"');
        } else if let Some(rule) = body.strip_prefix('-') {
            out.push((lang, rule.trim()));
        }
    }
    out
}

/// A variant rule, `sources -> replacements` (the sources stay) or `sources => replacements`,
/// as the (source, replacement) pairs it adds: each normalised, padded with the spaces or start
/// and end marks (`^`) that say where in a name it applies. `~x` matches a word that ends in x,
/// `x~` one that starts with it, either also as a word of its own beside the rest (unless the
/// arrow is `|->` or `|=>`); `^x` only at the name's start, `x$` only at its end.
pub fn rule_pairs(rule: &str) -> Result<Vec<(String, String)>, String> {
    let arrow = [rule.find("->"), rule.find("=>")].into_iter().flatten().min().ok_or_else(|| format!("no arrow in {rule:?}"))?;
    if rule[arrow + 2..].contains("->") || rule[arrow + 2..].contains("=>") {
        return Err(format!("two arrows in {rule:?}"));
    }
    let decompose = !rule[..arrow].ends_with('|');
    let sources_text = if decompose { &rule[..arrow] } else { &rule[..arrow - 1] };
    let keep = rule.as_bytes()[arrow] == b'-';
    let mut sources = Vec::new();
    for word in sources_text.split(',') {
        if let Some(source) = source_word(word)? {
            sources.push(source);
        }
    }
    let replacements: Vec<String> = rule[arrow + 2..].split(',').map(normalized).collect();
    let mut out = Vec::new();
    for (word, pre, post) in &sources {
        if keep {
            out.extend(padded(word, *pre, *post, word, decompose));
        }
        for replacement in replacements.iter().filter(|r| !r.is_empty()) {
            out.extend(padded(word, *pre, *post, replacement, decompose));
        }
    }
    Ok(out)
}

/// A rule's source word: its normalised text and its marks before and after; None when nothing
/// of it is left.
fn source_word(word: &str) -> Result<Option<(String, char, char)>, String> {
    let word = word.trim();
    let pre = word.chars().next().filter(|c| matches!(c, '~' | '^'));
    let rest = &word[pre.map_or(0, char::len_utf8)..];
    let post = rest.chars().last().filter(|c| matches!(c, '~' | '$'));
    let text = &rest[..rest.len() - post.map_or(0, char::len_utf8)];
    if text.contains(['~', '$', '^']) || (pre == Some('~') && post == Some('~')) {
        return Err(format!("a source word of marks {word:?}"));
    }
    let text = normalized(text);
    Ok((!text.is_empty()).then(|| (text, pre.unwrap_or(' '), post.unwrap_or(' '))))
}

fn mark(flag: char, at_start: bool) -> &'static str {
    match (flag, at_start) {
        ('^', true) => "^ ",
        ('$', false) => " ^",
        _ => " ",
    }
}

fn padded(src: &str, pre: char, post: char, repl: &str, decompose: bool) -> Vec<(String, String)> {
    if pre == '~' {
        let end = mark(post, false);
        let (s, r) = (format!("{src}{end}"), format!("{repl}{end}"));
        let mut out = vec![(s.clone(), r.clone()), (format!(" {s}"), format!(" {r}"))];
        if decompose {
            out.push((s.clone(), format!(" {r}")));
            out.push((format!(" {s}"), r));
        }
        out
    } else if post == '~' {
        let start = mark(pre, true);
        let (s, r) = (format!("{start}{src}"), format!("{start}{repl}"));
        let mut out = vec![(s.clone(), r.clone()), (format!("{s} "), format!("{r} "))];
        if decompose {
            out.push((s.clone(), format!("{r} ")));
            out.push((format!("{s} "), r));
        }
        out
    } else {
        let (start, end) = (mark(pre, true), mark(post, false));
        vec![(format!("{start}{src}{end}"), format!("{start}{repl}{end}"))]
    }
}

/// Sources and what each is replaced with, read longest first from any position of a name.
#[derive(Default)]
struct Rules {
    nodes: Vec<(Vec<(char, usize)>, Option<usize>)>,
    replacements: Vec<Vec<Vec<char>>>,
}

impl Rules {
    fn new(pairs: BTreeSet<(String, String)>) -> Rules {
        let mut rules = Rules { nodes: vec![(Vec::new(), None)], replacements: Vec::new() };
        for (source, replacement) in pairs {
            // A replacement that ends in a space where its source does leaves that space to the
            // name, so the word after it is read with its space.
            let replacement = if source.ends_with(' ') && replacement.ends_with(' ') {
                &replacement[..replacement.len() - 1]
            } else {
                &replacement[..]
            };
            let mut at = 0;
            for c in source.chars() {
                at = match rules.nodes[at].0.iter().find(|(k, _)| *k == c) {
                    Some(&(_, next)) => next,
                    None => {
                        rules.nodes.push((Vec::new(), None));
                        let next = rules.nodes.len() - 1;
                        rules.nodes[at].0.push((c, next));
                        next
                    }
                };
            }
            let slot = *rules.nodes[at].1.get_or_insert_with(|| {
                rules.replacements.push(Vec::new());
                rules.replacements.len() - 1
            });
            rules.replacements[slot].push(replacement.chars().collect());
        }
        rules
    }

    /// The replacements of the longest source at `at` and where it ends.
    fn longest(&self, text: &[char], at: usize) -> Option<(&[Vec<char>], usize)> {
        let mut node = 0;
        let mut found = None;
        for (i, c) in text[at..].iter().enumerate() {
            let Some(&(_, next)) = self.nodes[node].0.iter().find(|(k, _)| k == c) else { break };
            node = next;
            if let Some(slot) = self.nodes[node].1 {
                found = Some((self.replacements[slot].as_slice(), at + i + 1));
            }
        }
        found
    }

    /// The name with each source found replaced by each of its replacements in turn, as
    /// Nominatim's generic analysis does it; just the name when no source is found, or when that
    /// makes more than VARIANT_MAX_PARTIALS.
    fn vary(&self, norm: &str) -> Vec<String> {
        let base: Vec<char> = format!("^ {norm} ^").chars().collect();
        let mut partials: Vec<Vec<char>> = vec![Vec::new()];
        let (mut start, mut at, mut force_space) = (0, 0, false);
        while at < base.len() {
            let from = at;
            let Some((replacements, end)) = self.longest(&base, at) else {
                at += 1;
                force_space = false;
                continue;
            };
            at = end;
            let done = &base[start..from];
            let mut next = Vec::new();
            for partial in &partials {
                for r in replacements.iter().filter(|r| !force_space || r.first() == Some(&' ')) {
                    next.push([partial.as_slice(), done, r].concat());
                }
            }
            partials = next;
            if partials.len() > VARIANT_MAX_PARTIALS {
                start = 0;
                break;
            }
            if base[at - 1] == ' ' {
                at -= 1;
                force_space = true;
            }
            start = at;
        }
        if start == 0 {
            return vec![norm.to_string()];
        }
        partials
            .into_iter()
            .map(|p| {
                let p = p.get(1..).unwrap_or_default();
                if start < base.len() {
                    String::from_iter(p.iter().chain(&base[start..base.len() - 1]))
                } else {
                    String::from_iter(p.get(..p.len().saturating_sub(1)).unwrap_or_default())
                }
            })
            .collect()
    }
}

/// Every language's rules and every country's default languages.
pub struct Variants {
    rules: HashMap<&'static str, Rules>,
    countries: HashMap<String, Vec<&'static str>>,
}

impl Variants {
    /// The rules and languages, read once.
    pub fn get() -> &'static Variants {
        static VARIANTS: OnceLock<Variants> = OnceLock::new();
        VARIANTS.get_or_init(Variants::read)
    }

    fn read() -> Variants {
        let mut by_lang: HashMap<&'static str, BTreeSet<(String, String)>> = HashMap::new();
        for (file_lang, file) in RULE_FILES {
            for (lang, rule) in rule_lines(file) {
                assert_eq!(lang, file_lang, "a section of another language");
                by_lang.entry(file_lang).or_default().extend(rule_pairs(rule).expect("a variant rule"));
            }
        }
        let rules = by_lang.into_iter().map(|(lang, pairs)| (lang, Rules::new(pairs))).collect();
        let known = |l: &str| NAME_LANGUAGES.iter().copied().find(|&k| k == l);
        let mut countries: HashMap<String, Vec<&'static str>> = HashMap::new();
        for line in COUNTRY_DEFAULTS.lines().filter(|l| !l.starts_with('#')) {
            let Some((cc, lang)) = line.split_once(':') else { continue };
            countries.insert(cc.trim().to_ascii_lowercase(), known(lang.trim().trim_matches('"')).into_iter().collect());
        }
        for line in COUNTRY_LANGUAGES.lines().filter(|l| !l.starts_with('#')) {
            let Some((cc, langs)) = line.split_once('\t') else { continue };
            countries.insert(cc.to_string(), langs.split(',').filter_map(known).collect());
        }
        Variants { rules, countries }
    }

    /// The languages Nominatim varies a name of the kind `kind` (its key before any ':') and
    /// suffix (after it) by, in a country (lower-case code): only a kind with "name" in it has
    /// any; the suffix's language when it is one of NAME_LANGUAGES, none when it is another; with
    /// no suffix (or a side, left or right) the country's default languages among them.
    pub fn languages(&self, kind: &str, suffix: Option<&str>, country: &str) -> Vec<&'static str> {
        if !kind.contains("name") {
            return Vec::new();
        }
        match suffix.filter(|s| !s.is_empty() && !NAME_SUFFIXES_WITHOUT_LANGUAGE.contains(s)) {
            Some(s) => NAME_LANGUAGES.iter().copied().filter(|&l| l == s).collect(),
            None => self.countries.get(country).cloned().unwrap_or_default(),
        }
    }

    /// The terms of a normalised name. With no language, the name itself; with one, only the
    /// variants its rules and spellings make that differ from it, as Nominatim's analyses do.
    pub fn terms(&self, norm: &str, language: Option<&str>) -> Vec<Term> {
        let Some(lang) = language else {
            let token = transliterated(norm);
            return if token.is_empty() { Vec::new() } else { vec![Term { token, lookup: norm.to_string() }] };
        };
        let mut spelt = match self.rules.get(lang) {
            Some(rules) => rules.vary(norm),
            None => vec![norm.to_string()],
        };
        for &(_, letter, spelling) in MUTATIONS.iter().filter(|(l, ..)| *l == lang) {
            spelt = spelt.iter().flat_map(|v| mutated(v, letter, spelling)).collect();
        }
        let mut out: Vec<Term> = spelt
            .into_iter()
            .map(|v| v.trim().to_string())
            .filter(|v| v != norm)
            .filter_map(|lookup| {
                let token = transliterated(&lookup);
                (!token.is_empty()).then_some(Term { token, lookup })
            })
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }
}

/// `name` with each `letter` kept and spelt as `spelling`, every combination.
fn mutated(name: &str, letter: char, spelling: &str) -> Vec<String> {
    let parts: Vec<&str> = name.split(letter).collect();
    let gaps = parts.len() - 1;
    (0..1usize << gaps)
        .map(|spelt| {
            let mut out = String::from(parts[0]);
            for (g, part) in parts[1..].iter().enumerate() {
                if spelt & (1 << (gaps - 1 - g)) == 0 {
                    out.push(letter);
                } else {
                    out.push_str(spelling);
                }
                out.push_str(part);
            }
            out
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nominatim's own generic analysis (with the oracle's ICU rules) configured with these rule
    /// files as Nominatim configures its own, over 1,283 names of the oracle and edge cases:
    /// language, name, its variants as token=lookup joined by '|'.
    const ORACLE: &str = include_str!("../../testdata/variants.tsv");
    /// The languages written in Cyrillic or Greek, whose tokens are spelt by this crate's own
    /// letter values rather than by ICU's.
    const OWN_SPELLING: [&str; 4] = ["bg", "el", "ru", "uk"];

    fn written(terms: &[Term]) -> String {
        let parts: Vec<String> = terms.iter().map(|t| format!("{}={}", t.token, t.lookup)).collect();
        parts.join("|")
    }

    fn tokens(name: &str, lang: Option<&str>) -> Vec<String> {
        Variants::get().terms(&normalized(name), lang).into_iter().map(|t| t.token).collect()
    }

    #[test]
    fn varies_names_as_nominatims_analysis_does() {
        let variants = Variants::get();
        let mut wrong = Vec::new();
        for line in ORACLE.lines() {
            let f: Vec<&str> = line.split('\t').collect();
            let terms = variants.terms(&normalized(f[1]), Some(f[0]));
            let same = if OWN_SPELLING.contains(&f[0]) {
                let mut ours: Vec<&str> = terms.iter().map(|t| t.lookup.as_str()).collect();
                let mut theirs: Vec<&str> = f[2].split('|').filter_map(|p| p.split_once('=')).map(|(_, l)| l).collect();
                ours.sort_unstable();
                theirs.sort_unstable();
                ours == theirs && terms.iter().all(|t| t.token == transliterated(&t.lookup))
            } else {
                written(&terms) == f[2]
            };
            if !same {
                wrong.push(format!("{} {:?}: {:?} not {:?}", f[0], f[1], written(&terms), f[2]));
            }
        }
        assert!(wrong.is_empty(), "{} of {}:\n{}", wrong.len(), ORACLE.lines().count(), wrong.join("\n"));
    }

    #[test]
    fn every_rule_of_every_file_reads() {
        let mut rules = 0;
        for (lang, file) in RULE_FILES {
            for (section, rule) in rule_lines(file) {
                assert_eq!(section, lang);
                assert!(rule_pairs(rule).is_ok(), "{lang}: {rule:?}");
                rules += 1;
            }
        }
        assert_eq!(rules, 1_973);
    }

    #[test]
    fn a_variant_keeps_its_own_spelling_as_lookup_word() {
        let terms = Variants::get().terms("liberty park drive", Some("en"));
        assert!(terms.contains(&Term { token: "liberty park dr".into(), lookup: "liberty park dr".into() }), "{terms:?}");
        assert!(!terms.iter().any(|t| t.lookup == "liberty park drive"), "the name itself is no variant");
    }

    #[test]
    fn variants_go_from_the_full_word_to_its_abbreviation_only() {
        assert!(!tokens("X Dr", Some("en")).iter().any(|t| t.contains("drive")));
        assert!(tokens("X Drive", Some("en")).contains(&"x dr".to_string()));
    }

    #[test]
    fn a_suffix_rule_splits_the_word_both_ways() {
        let mut got = tokens("Hauptstraße", Some("de"));
        got.extend(tokens("Hauptstraße", None));
        got.sort();
        assert_eq!(got, ["haupt str", "haupt strasse", "hauptstr", "hauptstrasse"]);
    }

    #[test]
    fn german_spells_umlauts_both_ways() {
        assert_eq!(tokens("München", Some("de")), ["muenchen"]);
        assert_eq!(tokens("München", None), ["munchen"]);
        assert_eq!(tokens("München", Some("en")), Vec::<String>::new());
    }

    #[test]
    fn too_many_variants_leave_the_name_alone() {
        let name = "North South East West Street Avenue Road Drive Court Lane Place Boulevard Parkway Highway";
        assert_eq!(tokens(name, Some("en")), Vec::<String>::new());
        assert!(!tokens("North Street", Some("en")).is_empty());
    }

    #[test]
    fn a_name_takes_its_keys_language_or_its_countrys() {
        let v = Variants::get();
        assert_eq!(v.languages("name", None, "nl"), ["nl"]);
        assert_eq!(v.languages("name", None, "be"), ["nl", "fr", "de"]);
        assert_eq!(v.languages("name", None, "ch"), ["de", "fr", "it"]);
        assert_eq!(v.languages("name", None, "dz"), ["fr"]);
        assert_eq!(v.languages("name", None, "us"), ["en"]);
        assert_eq!(v.languages("alt_name", None, "cz"), ["cs"]);
        assert_eq!(v.languages("name", Some("de"), "nl"), ["de"]);
        assert_eq!(v.languages("name", Some("left"), "de"), ["de"]);
        assert_eq!(v.languages("name", Some("hr"), "hr"), Vec::<&str>::new());
        assert_eq!(v.languages("name", Some("zh-Hans"), "cn"), Vec::<&str>::new());
        assert_eq!(v.languages("ref", None, "nl"), Vec::<&str>::new());
        assert_eq!(v.languages("name", None, "xx"), Vec::<&str>::new());
    }
}
