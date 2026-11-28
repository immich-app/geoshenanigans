//! The few regular expressions Nominatim's postcode patterns and its front door's coordinates are
//! written in, matched as Python's re matches them.

use super::normalize::decimal_digit;

/// White space as Python's `\s` reads it in a str: Unicode's, and the separators U+001C-U+001F.
pub(crate) fn is_python_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1C}'..='\u{1F}').contains(&c)
}

/// The few regular expressions those patterns are written in: characters (`\` before one that
/// would be read otherwise), classes of characters, ranges, `\d` and `\s`, groups (capturing,
/// named `(?P<name>` or `(?:`), alternatives, and `?`, `*`, `+`, `{n}` or `{m,n}` after an item.
/// Matched as Python's re matches a str: greedy, alternatives in order, first match wins, `\d` any
/// decimal digit and `\s` Python's white space.
pub(crate) struct Regex {
    alternatives: Vec<Vec<Item>>,
    groups: usize,
    names: Vec<(String, usize)>,
}

struct Item {
    atom: Atom,
    min: usize,
    max: usize,
}

enum Atom {
    Char(char),
    Class(Vec<ClassItem>),
    Group(Option<usize>, Vec<Vec<Item>>),
}

enum ClassItem {
    Range(char, char),
    Digit,
    Space,
}

impl ClassItem {
    fn contains(&self, c: char) -> bool {
        match *self {
            ClassItem::Range(lo, hi) => (lo..=hi).contains(&c),
            ClassItem::Digit => decimal_digit(c).is_some(),
            ClassItem::Space => is_python_space(c),
        }
    }

    /// The item `\` and `c` stand for.
    fn escaped(c: char) -> ClassItem {
        match c {
            'd' => ClassItem::Digit,
            's' => ClassItem::Space,
            c => ClassItem::Range(c, c),
        }
    }
}

/// A match from the start of the text: where it ends and each group's span (index 1 for group 1).
pub(crate) struct Match {
    pub end: usize,
    pub groups: Vec<Option<(usize, usize)>>,
}

impl Match {
    /// Python's `match.expand(template)` for templates of `\1`..`\9` and plain characters; a group
    /// that took no part is empty.
    pub fn expand(&self, template: &str, text: &[char]) -> String {
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

/// The groups a pattern has opened so far, and the names of the named ones.
#[derive(Default)]
struct Groups {
    count: usize,
    names: Vec<(String, usize)>,
}

impl Regex {
    pub fn parse(pattern: &str) -> Result<Regex, String> {
        let chars: Vec<char> = pattern.chars().collect();
        let mut at = 0;
        let mut groups = Groups::default();
        let alternatives = Self::alternatives(&chars, &mut at, &mut groups)?;
        if at != chars.len() {
            return Err(format!("unread {:?} in {pattern:?}", &chars[at..]));
        }
        Ok(Regex { alternatives, groups: groups.count, names: groups.names })
    }

    /// The number of the group named `name`.
    pub fn group(&self, name: &str) -> Option<usize> {
        self.names.iter().find(|(n, _)| n == name).map(|&(_, g)| g)
    }

    fn alternatives(p: &[char], at: &mut usize, groups: &mut Groups) -> Result<Vec<Vec<Item>>, String> {
        let mut out = vec![Self::sequence(p, at, groups)?];
        while p.get(*at) == Some(&'|') {
            *at += 1;
            out.push(Self::sequence(p, at, groups)?);
        }
        Ok(out)
    }

    fn sequence(p: &[char], at: &mut usize, groups: &mut Groups) -> Result<Vec<Item>, String> {
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
                        groups.count += 1;
                        if p.get(*at..*at + 3) == Some(&['?', 'P', '<']) {
                            let close = p[*at..].iter().position(|&c| c == '>').ok_or("an unclosed group name")? + *at;
                            groups.names.push((String::from_iter(&p[*at + 3..close]), groups.count));
                            *at = close + 1;
                        }
                        Some(groups.count)
                    };
                    let inner = Self::alternatives(p, at, groups)?;
                    if p.get(*at) != Some(&')') {
                        return Err("an unclosed group".into());
                    }
                    *at += 1;
                    Atom::Group(capture, inner)
                }
                '[' => {
                    *at += 1;
                    let mut items = Vec::new();
                    loop {
                        let item = match p.get(*at) {
                            None => return Err("an unclosed class".into()),
                            Some(']') => break,
                            Some('\\') => {
                                *at += 1;
                                ClassItem::escaped(*p.get(*at).ok_or("an unclosed class")?)
                            }
                            Some(&c) if p.get(*at + 1) == Some(&'-') && p.get(*at + 2).is_some_and(|&e| e != ']') => {
                                *at += 2;
                                ClassItem::Range(c, p[*at])
                            }
                            Some(&c) => ClassItem::Range(c, c),
                        };
                        items.push(item);
                        *at += 1;
                    }
                    *at += 1;
                    Atom::Class(items)
                }
                '\\' => {
                    let c = *p.get(*at + 1).ok_or("a pattern that ends in \\")?;
                    *at += 2;
                    match ClassItem::escaped(c) {
                        ClassItem::Range(c, _) => Atom::Char(c),
                        item => Atom::Class(vec![item]),
                    }
                }
                '*' | '+' | '.' | '^' | '$' | '{' | '?' => return Err(format!("{c:?} is not read")),
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
                Some('*') => {
                    *at += 1;
                    (0, usize::MAX)
                }
                Some('+') => {
                    *at += 1;
                    (1, usize::MAX)
                }
                Some('{') => {
                    let close = p[*at..].iter().position(|&c| c == '}').ok_or("an unclosed count")? + *at;
                    let counts = String::from_iter(&p[*at + 1..close]);
                    let count = |n: &str| n.parse::<usize>().map_err(|_| "a count that is no number");
                    *at = close + 1;
                    match counts.split_once(',') {
                        Some((min, max)) => (count(min)?, count(max)?),
                        None => (count(&counts)?, count(&counts)?),
                    }
                }
                _ => (1, 1),
            };
            items.push(Item { atom, min, max });
        }
        Ok(items)
    }

    /// The first match from the start of `text`, in the order Python's re tries them.
    pub fn match_prefix(&self, text: &[char]) -> Option<Match> {
        self.first_match(text, |_| true)
    }

    /// The first match from the start of `text` whose end `fits`, in the order Python's re tries
    /// them (`end == text.len()` for a whole match).
    pub fn first_match(&self, text: &[char], fits: impl Fn(usize) -> bool) -> Option<Match> {
        let mut found = None;
        let mut caps: Captures = vec![None; self.groups + 1];
        let matcher = Matcher { text };
        for alternative in &self.alternatives {
            if matcher.sequence(alternative, 0, &mut caps, &mut |end, caps| {
                if !fits(end) {
                    return false;
                }
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
        // An item that matched nothing matches nothing again: Python stops repeating it there too.
        let more = |next: usize| next > at || done < item.min;
        if done < item.max && self.atom(&item.atom, at, caps, &mut |next, caps| more(next) && self.repeat(item, done + 1, next, caps, &mut *then)) {
            return true;
        }
        done >= item.min && then(at, caps)
    }

    fn atom(&self, atom: &Atom, at: usize, caps: &mut Captures, then: Then) -> bool {
        match atom {
            Atom::Char(c) => self.text.get(at) == Some(c) && then(at + 1, caps),
            Atom::Class(items) => self.text.get(at).is_some_and(|&c| items.iter().any(|i| i.contains(c))) && then(at + 1, caps),
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

    #[test]
    fn a_count_can_be_a_range() {
        let regex = Regex::parse("[A-Z]{2,3}").expect("a pattern");
        let ends = |text: &str| regex.match_prefix(&text.chars().collect::<Vec<_>>()).map(|m| m.end);
        assert_eq!((ends("A"), ends("AB"), ends("ABCD")), (None, Some(2), Some(3)));
        assert!(Regex::parse("[A-Z]{2,x}").is_err());
    }

    fn whole(pattern: &str, text: &str) -> Option<Vec<Option<String>>> {
        let regex = Regex::parse(pattern).expect("a pattern");
        let text: Vec<char> = text.chars().collect();
        let m = regex.first_match(&text, |end| end == text.len())?;
        Some(m.groups[1..].iter().map(|g| g.map(|(s, e)| String::from_iter(&text[s..e]))).collect())
    }

    #[test]
    fn digits_and_white_space_are_pythons() {
        assert!(whole(r"\d+\s\d", "١٢\u{2003}3").is_some());
        assert!(whole(r"[\d.]+", "1.5").is_some());
        assert!(whole(r"\d", "a").is_none());
        assert!(whole(r"[\s,]+", "\u{1F}, ").is_some());
    }

    #[test]
    fn repeats_take_all_they_can_and_give_back() {
        assert_eq!(whole(r"(\d*)(\d+)", "123"), Some(vec![Some("12".into()), Some("3".into())]));
        assert_eq!(whole(r"(a+)?b", "b"), Some(vec![None]));
        assert!(whole(r"a+", "").is_none());
        assert!(whole(r"(a*)*b", "aac").is_none());
    }

    #[test]
    fn a_named_group_has_its_number() {
        let regex = Regex::parse(r"(?P<deg>\d+)\.(?P<frac>\d+)\]?").expect("a pattern");

        assert_eq!((regex.group("deg"), regex.group("frac"), regex.group("min")), (Some(1), Some(2), None));
        assert!(whole(r"\[?(?P<x>\d)\]", "[1]").is_some());
        assert!(Regex::parse("(?P<deg").is_err());
    }
}
