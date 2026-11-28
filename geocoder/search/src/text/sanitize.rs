//! A place's names and address parts as Nominatim's sanitizers leave them for its tokenizer
//! (`settings/sanitizers.yaml`, then the tokenizer's language tagging), in that order: URLs and
//! some address kinds dropped, house-number lists split and short interpolations expanded,
//! multilingual concatenations, name lists, affixes, long or listed refs and brace terms dealt
//! with, the TIGER county cleaned, and each name given the languages it is varied in.

use super::normalize::decimal_digit;
use super::variants::Variants;
use crate::constants::{
    HOUSENUMBER_AS_NAME_RUN, HOUSENUMBER_AS_NAME_WORD, INTERPOLATION_EXPAND_BELOW, MULTILINGUAL_DELIMITERS, REF_MAX_CHARS,
};

/// A name or an address part: its value, its kind (the key before the first ':'), its suffix
/// (after it), and the language it is varied in, set by the last step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlaceName {
    pub name: String,
    pub kind: String,
    pub suffix: Option<String>,
    pub language: Option<&'static str>,
}

impl PlaceName {
    /// The part of the key `key` (in Nominatim's address keys for an address part) with the value
    /// `name`, each trimmed.
    pub fn new(key: &str, name: &str) -> PlaceName {
        let (kind, suffix) = match key.split_once(':') {
            Some((kind, suffix)) => (kind.trim(), Some(suffix.trim().to_string())),
            None => (key.trim(), None),
        };
        PlaceName { name: name.trim().to_string(), kind: kind.to_string(), suffix, language: None }
    }

    fn named(&self, name: &str) -> PlaceName {
        PlaceName { name: name.to_string(), ..self.clone() }
    }

    fn of_kind(&self, kind: &str, name: &str) -> PlaceName {
        PlaceName { name: name.to_string(), kind: kind.to_string(), ..self.clone() }
    }
}

/// A place's names and address parts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Place {
    pub names: Vec<PlaceName>,
    pub address: Vec<PlaceName>,
}

/// The address key Nominatim files an OSM key's value under, None for a name. A house name is a
/// name; several keys share the postcode, country and place keys.
fn address_key(key: &str) -> Option<&str> {
    match key {
        "addr:housename" => None,
        "postal_code" | "postcode" | "addr:postcode" | "tiger:zip_left" | "tiger:zip_right" => Some("postcode"),
        "country_code" | "ISO3166-1" | "addr:country_code" | "is_in:country_code" | "addr:country" | "is_in:country" => {
            Some("country")
        }
        "addr:interpolation" => Some("interpolation"),
        "addr:place" | "addr:substreet" => Some("place"),
        "tiger:county" => Some(key),
        _ => key.strip_prefix("addr:").or_else(|| key.strip_prefix("is_in:")),
    }
}

impl Place {
    /// A place from its OSM tags: each address key filed as Nominatim files it (the first of the
    /// keys that share one wins), every other key a name.
    pub fn from_tags<'a>(tags: impl IntoIterator<Item = (&'a str, &'a str)>) -> Place {
        let mut place = Place::default();
        for (key, value) in tags {
            match address_key(key) {
                None => place.names.push(PlaceName::new(key, value)),
                Some(filed) => {
                    let part = PlaceName::new(filed, value);
                    if !place.address.iter().any(|a| a.kind == part.kind && a.suffix == part.suffix) {
                        place.address.push(part);
                    }
                }
            }
        }
        place
    }

    /// The place after Nominatim's sanitizers, for a place in `country` (a lower-case code).
    pub fn sanitized(mut self, country: &str) -> Place {
        self.names.retain(|n| !has_url(&n.name));
        self.address.retain(|a| !has_url(&a.name) && !a.name.contains('='));
        self.address.retain(|a| !matches!(a.kind.as_str(), "unit" | "floor" | "flats" | "country" | "full" | "inclusion") && !a.kind.starts_with('_'));
        self.clean_housenumbers();
        self.clean_tiger_county();
        self.drop_multilingual_concatenations();
        self.names = self.names.iter().flat_map(split_name_list).collect();
        self.expand_affixes();
        self.names.retain(|n| !(n.kind == "ref" && (n.name.contains(',') || n.name.chars().count() > REF_MAX_CHARS)));
        self.add_names_without_braces();
        self.tag_languages(country);
        self
    }

    /// House numbers: an interpolation's range of fewer than INTERPOLATION_EXPAND_BELOW numbers
    /// becomes each number; a value that reads as a name becomes a name; a list (',' or ';') becomes
    /// its numbers. The interpolation itself is dropped.
    fn clean_housenumbers(&mut self) {
        let step = self.address.iter().find(|a| a.kind == "interpolation").and_then(|a| match a.name.as_str() {
            "all" => Some(Step::Every(1)),
            "odd" => Some(Step::Odd),
            "even" => Some(Step::Even),
            s if s.chars().count() == 1 => decimal(s).map(Step::Every),
            _ => None,
        });
        let mut address = Vec::with_capacity(self.address.len());
        for part in std::mem::take(&mut self.address) {
            if !matches!(part.kind.as_str(), "housenumber" | "conscriptionnumber" | "streetnumber") {
                if part.kind != "interpolation" {
                    address.push(part);
                }
                continue;
            }
            if let Some(numbers) = step.and_then(|s| expand_range(s, &part.name)) {
                address.extend(numbers.into_iter().map(|n| part.of_kind("housenumber", &n.to_string())));
            } else if reads_as_name(&part.name) {
                self.names.push(part.of_kind("housenumber", &part.name));
            } else {
                let numbers = split_list(&part.name, &[',', ';']).into_iter().filter(|n| !n.is_empty());
                address.extend(numbers.map(|n| part.of_kind("housenumber", n)));
            }
        }
        self.address = address;
    }

    /// The first `tiger:county` becomes a county ("Travis, TX" -> "Travis").
    fn clean_tiger_county(&mut self) {
        let Some(part) = self.address.iter_mut().find(|a| a.kind == "tiger" && a.suffix.as_deref() == Some("county")) else {
            return;
        };
        let bytes = part.name.as_bytes();
        let n = bytes.len();
        if n >= 4 && &bytes[n - 4..n - 2] == b", " && bytes[n - 2..].iter().all(u8::is_ascii_uppercase) {
            part.name.truncate(n - 4);
        }
        part.kind = "county".into();
        part.suffix = Some("tiger".into());
    }

    /// A plain `name` made only of the place's names in languages (and delimiters) goes.
    fn drop_multilingual_concatenations(&mut self) {
        let language_names: Vec<&str> = self
            .names
            .iter()
            .filter(|n| n.kind == "name" && n.suffix.as_deref().is_some_and(|s| !s.is_empty() && !starts_with_four_letters(s)))
            .map(|n| n.name.as_str())
            .collect();
        if language_names.is_empty() {
            return;
        }
        let mut by_length = language_names.clone();
        by_length.sort_by_key(|n| std::cmp::Reverse(n.len()));
        let drop: Vec<bool> =
            self.names.iter().map(|n| n.kind == "name" && n.suffix.is_none() && is_concatenation(&n.name, &by_length)).collect();
        let mut i = 0;
        self.names.retain(|_| {
            i += 1;
            !drop[i - 1]
        });
    }

    /// `name:prefix`, `name:suffix` (and those of a language, `name:prefix:de` or `name:de:prefix`)
    /// go; the names they belong to also get the name with them, where it lacks them.
    fn expand_affixes(&mut self) {
        let mut affixes: Vec<((String, Option<String>), Option<String>, Option<String>)> = Vec::new();
        let mut stems = Vec::new();
        for name in std::mem::take(&mut self.names) {
            let (stem, is_prefix) = match (stem_of(&name, "prefix"), stem_of(&name, "suffix")) {
                (Some(stem), _) => (stem, true),
                (None, Some(stem)) => (stem, false),
                (None, None) => {
                    stems.push(name);
                    continue;
                }
            };
            let at = match affixes.iter().position(|(s, ..)| *s == stem) {
                Some(at) => at,
                None => {
                    affixes.push((stem, None, None));
                    affixes.len() - 1
                }
            };
            if is_prefix {
                affixes[at].1 = Some(name.name);
            } else {
                affixes[at].2 = Some(name.name);
            }
        }
        if affixes.is_empty() {
            self.names = stems;
            return;
        }
        for name in stems {
            let affix = affixes.iter().find(|((kind, suffix), ..)| *kind == name.kind && *suffix == name.suffix);
            let Some((_, prefix, suffix)) = affix else {
                self.names.push(name);
                continue;
            };
            let mut full = name.name.clone();
            if let Some(prefix) = prefix {
                if !full.starts_with(&format!("{prefix} ")) {
                    full = format!("{prefix} {full}");
                }
            }
            if let Some(suffix) = suffix {
                if !full.ends_with(&format!(" {suffix}")) {
                    full = format!("{full} {suffix}");
                }
            }
            let differs = full != name.name;
            self.names.push(name.named(&full));
            if differs {
                self.names.push(name);
            }
        }
    }

    /// A name with a term in braces also gives the name before them ("Frankfurt (Oder)" also gives
    /// "Frankfurt"), unless the braces close before its end.
    fn add_names_without_braces(&mut self) {
        let mut added = Vec::new();
        for name in self.names.iter().filter(|n| n.name.contains('(')) {
            if name.name.contains(')') && !name.name.ends_with(')') {
                continue;
            }
            let stem = name.name.split('(').next().unwrap_or("").trim();
            if !stem.is_empty() {
                added.push(name.named(stem));
            }
        }
        self.names.extend(added);
    }

    /// Each name also as each language Variants::languages gives it.
    fn tag_languages(&mut self, country: &str) {
        let variants = Variants::get();
        let mut tagged = Vec::new();
        for name in &self.names {
            for lang in variants.languages(&name.kind, name.suffix.as_deref(), country) {
                tagged.push(PlaceName { language: Some(lang), ..name.clone() });
            }
        }
        self.names.extend(tagged);
    }
}

#[derive(Clone, Copy)]
enum Step {
    Every(u128),
    Odd,
    Even,
}

fn has_url(value: &str) -> bool {
    value.contains("http:") || value.contains("https:")
}

/// The value of a string of decimal digits of any script.
fn decimal(s: &str) -> Option<u128> {
    if s.is_empty() {
        return None;
    }
    s.chars().try_fold(0u128, |n, c| n.checked_mul(10)?.checked_add(u128::from(decimal_digit(c)?)))
}

/// The numbers of an interpolated "first-last" range, when it has fewer than
/// INTERPOLATION_EXPAND_BELOW; None otherwise (the value then stays as it is).
fn expand_range(step: Step, value: &str) -> Option<Vec<u128>> {
    let (first, last) = value.split_once('-')?;
    let (mut first, last) = (decimal(first)?, decimal(last)?);
    let step = match step {
        Step::Every(0) => return None,
        Step::Every(n) => n,
        Step::Odd | Step::Even => {
            if matches!(step, Step::Even) == (first % 2 == 1) {
                first = first.checked_add(1)?;
            }
            2
        }
    };
    if last < first || last - first >= INTERPOLATION_EXPAND_BELOW * step - 1 {
        return None;
    }
    Some((first..=last).step_by(step as usize).collect())
}

/// A house number that is a name: one with a run of HOUSENUMBER_AS_NAME_RUN characters without a
/// list delimiter, or a part of its ',' list of HOUSENUMBER_AS_NAME_WORD characters and no digit.
fn reads_as_name(value: &str) -> bool {
    let mut run = 0;
    let long_run = value.chars().any(|c| {
        run = if c == ';' || c == ',' { 0 } else { run + 1 };
        run >= HOUSENUMBER_AS_NAME_RUN
    });
    long_run || value.split(',').any(|part| part.chars().count() >= HOUSENUMBER_AS_NAME_WORD && !part.chars().any(|c| decimal_digit(c).is_some()))
}

/// The parts of a list, split at runs of `delimiters` with any white space around them (as Python's
/// re.split, so with an empty part where the list starts or ends with one).
fn split_list<'a>(value: &'a str, delimiters: &[char]) -> Vec<&'a str> {
    let chars: Vec<(usize, char)> = value.char_indices().collect();
    let mut parts = Vec::new();
    let (mut part_start, mut i) = (0, 0);
    while i < chars.len() {
        let mut j = i;
        while j < chars.len() && chars[j].1.is_whitespace() {
            j += 1;
        }
        if j < chars.len() && delimiters.contains(&chars[j].1) {
            let mut k = j;
            while k < chars.len() && delimiters.contains(&chars[k].1) {
                k += 1;
            }
            while k < chars.len() && chars[k].1.is_whitespace() {
                k += 1;
            }
            parts.push(&value[part_start..chars[i].0]);
            part_start = chars.get(k).map_or(value.len(), |c| c.0);
            i = k;
        } else {
            i += 1;
        }
    }
    parts.push(&value[part_start..]);
    parts
}

/// A name list (';') as its names; a name without one as it is.
fn split_name_list(name: &PlaceName) -> Vec<PlaceName> {
    let parts = split_list(&name.name, &[';']);
    if parts.len() == 1 {
        return vec![name.clone()];
    }
    parts.into_iter().filter(|p| !p.is_empty()).map(|p| name.named(p)).collect()
}

/// The (kind, suffix) of the name an affix tag (`affix` "prefix" or "suffix") belongs to.
fn stem_of(name: &PlaceName, affix: &str) -> Option<(String, Option<String>)> {
    let suffix = name.suffix.as_deref().filter(|s| !s.is_empty())?;
    if suffix == affix {
        return Some((name.kind.clone(), None));
    }
    let (head, tail) = suffix.split_once(':')?;
    if head == affix {
        return Some((name.kind.clone(), Some(tail.to_string())));
    }
    if tail == affix {
        return Some((name.kind.clone(), Some(head.to_string())));
    }
    let (head, tail) = suffix.rsplit_once(':')?;
    (tail == affix).then(|| (name.kind.clone(), Some(head.to_string())))
}

fn starts_with_four_letters(s: &str) -> bool {
    s.chars().take(4).filter(char::is_ascii_alphabetic).count() == 4
}

/// Whether `value` is nothing but `names` (longest first), one after another with delimiters
/// between them (and maybe before and after).
fn is_concatenation(value: &str, names: &[&str]) -> bool {
    let mut at = delimiter_end(value, 0).unwrap_or(0);
    while at < value.len() {
        let Some(name) = names.iter().find(|n| value[at..].starts_with(**n)) else { return false };
        at += name.len();
        if at < value.len() {
            let Some(end) = delimiter_end(value, at) else { return false };
            at = end;
        }
    }
    true
}

/// Where a delimiter of MULTILINGUAL_DELIMITERS at `at` ends, white space around it included (a
/// space counts within white space).
fn delimiter_end(value: &str, at: usize) -> Option<usize> {
    let space_end = |from: usize| value[from..].char_indices().find(|(_, c)| !c.is_whitespace()).map_or(value.len(), |(i, _)| from + i);
    let w = space_end(at);
    let next = value[w..].chars().next();
    for d in MULTILINGUAL_DELIMITERS {
        if d == ' ' {
            if value[at..w].contains(' ') {
                return Some(w);
            }
        } else if next == Some(d) {
            return Some(space_end(w + d.len_utf8()));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nominatim's own sanitizers (without clean-postcodes and tag-japanese) over 776 places of the
    /// oracle chosen for each step: country, names, address, and the names and address parts it
    /// left (name, kind, suffix, and for names the language).
    const ORACLE: &str = include_str!("../../testdata/sanitize.jsonl");

    fn parts(map: &serde_json::Value) -> Vec<PlaceName> {
        map.as_object().unwrap().iter().map(|(k, v)| PlaceName::new(k, v.as_str().unwrap())).collect()
    }

    fn written(names: &[PlaceName]) -> Vec<String> {
        let mut out: Vec<String> = names.iter().map(|n| format!("{:?} {} {:?} {:?}", n.name, n.kind, n.suffix, n.language)).collect();
        out.sort();
        out
    }

    fn expected(rows: &serde_json::Value) -> Vec<String> {
        let mut out: Vec<String> = rows
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                let s = |i: usize| r.get(i).and_then(|v| v.as_str()).map(str::to_string);
                let language = s(3).and_then(|l| crate::constants::NAME_LANGUAGES.iter().copied().find(|&k| k == l));
                format!("{:?} {} {:?} {:?}", s(0).unwrap(), s(1).unwrap(), s(2), language)
            })
            .collect();
        out.sort();
        out
    }

    fn sanitized(tags: &[(&str, &str)], country: &str) -> Place {
        Place::from_tags(tags.iter().copied()).sanitized(country)
    }

    fn values(names: &[PlaceName]) -> Vec<&str> {
        names.iter().filter(|n| n.language.is_none()).map(|n| n.name.as_str()).collect()
    }

    #[test]
    fn sanitizes_as_nominatims_sanitizers() {
        let mut wrong = Vec::new();
        for line in ORACLE.lines() {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            let place = Place { names: parts(&row["name"]), address: parts(&row["address"]) }.sanitized(row["cc"].as_str().unwrap_or(""));
            if written(&place.names) != expected(&row["names"]) || written(&place.address) != expected(&row["addr"]) {
                wrong.push(format!("{line}\n  got {:?} {:?}", written(&place.names), written(&place.address)));
            }
        }
        assert!(wrong.is_empty(), "{} of {}:\n{}", wrong.len(), ORACLE.lines().count(), wrong.join("\n"));
    }

    #[test]
    fn files_address_keys_as_nominatim_does() {
        let place = Place::from_tags([
            ("name", "FUTO"),
            ("addr:housename", "FUTO HQ"),
            ("addr:housenumber", "1044"),
            ("addr:street", "Liberty Park Drive"),
            ("addr:postcode", "78746"),
            ("postal_code", "78701"),
            ("addr:substreet", "Rear"),
            ("tiger:county", "Travis, TX"),
            ("is_in:state", "Texas"),
        ]);
        let kinds: Vec<(&str, &str)> = place.address.iter().map(|a| (a.kind.as_str(), a.name.as_str())).collect();
        assert_eq!(
            kinds,
            [("housenumber", "1044"), ("street", "Liberty Park Drive"), ("postcode", "78746"), ("place", "Rear"), ("tiger", "Travis, TX"), ("state", "Texas")]
        );
        assert_eq!(values(&place.names), ["FUTO", "FUTO HQ"]);
        assert_eq!((place.names[1].kind.as_str(), place.names[1].suffix.as_deref()), ("addr", Some("housename")));
    }

    #[test]
    fn drops_urls_and_address_kinds_nominatim_drops() {
        let place = sanitized(
            &[("name", "http://example.org"), ("name:en", "Foo"), ("addr:unit", "3"), ("addr:floor", "2"), ("addr:city", "a=b"), ("addr:_x", "1"), ("addr:city:de", "X")],
            "",
        );
        assert_eq!(values(&place.names), ["Foo"]);
        assert_eq!(place.address.iter().map(|a| a.kind.as_str()).collect::<Vec<_>>(), ["city"]);
    }

    #[test]
    fn splits_house_number_lists() {
        let place = sanitized(&[("addr:housenumber", "12;14, 16a")], "");
        assert_eq!(place.address.iter().map(|a| a.name.as_str()).collect::<Vec<_>>(), ["12", "14", "16a"]);
    }

    #[test]
    fn expands_short_interpolations_only() {
        let numbers = |range: &str, kind: &str| {
            let place = sanitized(&[("addr:housenumber", range), ("addr:interpolation", kind)], "");
            assert!(place.address.iter().all(|a| a.kind == "housenumber"));
            place.address.into_iter().map(|a| a.name).collect::<Vec<_>>()
        };
        assert_eq!(numbers("2-12", "even"), ["2", "4", "6", "8", "10", "12"]);
        assert_eq!(numbers("1-12", "even"), ["2", "4", "6", "8", "10", "12"]);
        assert_eq!(numbers("1-9", "all"), ["1", "2", "3", "4", "5", "6", "7", "8", "9"]);
        assert_eq!(numbers("1-10", "all"), ["1-10"]);
        assert_eq!(numbers("12-2", "even"), ["12-2"]);
        assert_eq!(numbers("2-8", "0"), ["2-8"]);
        assert_eq!(numbers("18446744073709551616-18446744073709551616", "all"), ["18446744073709551616"]);
        let huge = "340282366920938463463374607431768211455";
        let place = sanitized(&[("addr:housenumber", &format!("{huge}-{huge}")), ("addr:interpolation", "even")], "");
        assert_eq!(values(&place.names), [format!("{huge}-{huge}")]);
    }

    #[test]
    fn a_house_number_that_reads_as_a_name_is_one() {
        for (value, is_name) in [("Haus am See", true), ("12, Hof", true), ("Building 12 rear entrance", true), ("12 a", false), ("ab", false)] {
            let place = sanitized(&[("addr:housenumber", value)], "");
            assert_eq!(place.names.iter().any(|n| n.kind == "housenumber" && n.name == value), is_name, "{value}");
            assert_eq!(place.address.is_empty(), is_name, "{value}");
        }
    }

    #[test]
    fn conscription_and_street_numbers_are_house_numbers() {
        let place = sanitized(&[("addr:conscriptionnumber", "699"), ("addr:streetnumber", "30")], "cz");
        let numbers: Vec<(&str, &str)> = place.address.iter().map(|a| (a.kind.as_str(), a.name.as_str())).collect();
        assert_eq!(numbers, [("housenumber", "699"), ("housenumber", "30")]);
    }

    #[test]
    fn a_tiger_county_loses_its_state() {
        let place = sanitized(&[("tiger:county", "Travis, TX")], "us");
        assert_eq!((place.address[0].name.as_str(), place.address[0].kind.as_str()), ("Travis", "county"));
        let place = sanitized(&[("tiger:county", "Travis County")], "us");
        assert_eq!((place.address[0].name.as_str(), place.address[0].kind.as_str()), ("Travis County", "county"));
    }

    #[test]
    fn a_name_of_its_languages_names_goes() {
        let tags = [("name", "Bruxelles - Brussel"), ("name:fr", "Bruxelles"), ("name:nl", "Brussel")];
        assert_eq!(values(&sanitized(&tags, "be").names), ["Bruxelles", "Brussel"]);
        let tags = [("name", "Bruxelles - Brussel - Centre"), ("name:fr", "Bruxelles"), ("name:nl", "Brussel")];
        assert!(values(&sanitized(&tags, "be").names).contains(&"Bruxelles - Brussel - Centre"));
        let tags = [("name", "Brussel"), ("name:nl", "Brussel")];
        assert_eq!(values(&sanitized(&tags, "be").names), ["Brussel"]);
    }

    #[test]
    fn splits_name_lists() {
        assert_eq!(values(&sanitized(&[("name", "A; B;")], "").names), ["A", "B"]);
    }

    #[test]
    fn a_prefix_joins_its_name() {
        let tags = [("name", "Peter"), ("name:prefix", "Sankt")];
        assert_eq!(values(&sanitized(&tags, "").names), ["Sankt Peter", "Peter"]);
        let tags = [("name", "Sankt Peter"), ("name:prefix", "Sankt")];
        assert_eq!(values(&sanitized(&tags, "").names), ["Sankt Peter"]);
        let tags = [("name:de", "Platz"), ("name:prefix:de", "Am"), ("name:de:suffix", "Nord")];
        assert_eq!(values(&sanitized(&tags, "").names), ["Am Platz Nord", "Platz"]);
    }

    #[test]
    fn drops_listed_and_long_refs() {
        let long = "x".repeat(41);
        let tags = [("ref", "A1, A2"), ("int_ref", "E 40"), ("nat_ref", long.as_str())];
        let place = sanitized(&tags, "");
        assert_eq!(values(&place.names), ["E 40", long.as_str()]);
        assert_eq!(values(&sanitized(&[("ref", long.as_str()), ("ref:x", long.as_str())], "").names), Vec::<&str>::new());
    }

    #[test]
    fn a_brace_term_also_gives_the_name_before_it() {
        assert_eq!(values(&sanitized(&[("name", "Frankfurt (Oder)")], "").names), ["Frankfurt (Oder)", "Frankfurt"]);
        assert_eq!(values(&sanitized(&[("name", "Halle (Saale) Hbf")], "").names), ["Halle (Saale) Hbf"]);
    }

    #[test]
    fn names_take_their_languages() {
        let place = sanitized(&[("name", "Hoofdstraat"), ("name:de", "Hauptstraße"), ("ref", "N201")], "nl");
        let tagged: Vec<(&str, &str)> = place.names.iter().filter_map(|n| Some((n.name.as_str(), n.language?))).collect();
        assert_eq!(tagged, [("Hoofdstraat", "nl"), ("Hauptstraße", "de")]);
    }
}
