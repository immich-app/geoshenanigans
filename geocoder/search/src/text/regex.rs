//! The few regular expressions Nominatim's postcode patterns are written in, matched as Python's
//! re matches them.

/// White space as Python's `\s` reads it in a str: Unicode's, and the separators U+001C-U+001F.
pub(crate) fn is_python_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1C}'..='\u{1F}').contains(&c)
}

/// The few regular expressions the postcode patterns are written in: characters, classes of
/// characters and ranges, groups (capturing or `(?:`), alternatives, and `?`, `{n}` or `{m,n}`
/// after an item. Matched as Python's re matches: greedy, alternatives in order, first match wins.
pub(crate) struct Regex {
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

impl Regex {
    pub fn parse(pattern: &str) -> Result<Regex, String> {
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

    #[test]
    fn a_count_can_be_a_range() {
        let regex = Regex::parse("[A-Z]{2,3}").expect("a pattern");
        let ends = |text: &str| regex.match_prefix(&text.chars().collect::<Vec<_>>()).map(|m| m.end);
        assert_eq!((ends("A"), ends("AB"), ends("ABCD")), (None, Some(2), Some(3)));
        assert!(Regex::parse("[A-Z]{2,x}").is_err());
    }
}
