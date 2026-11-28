//! Python's `difflib.SequenceMatcher(None, a, b).get_opcodes()` over characters, which Nominatim
//! measures a token's match against the query with.

use std::collections::HashMap;
use std::ops::Range;

use crate::constants::{DIFFLIB_AUTOJUNK_MIN_LEN, DIFFLIB_POPULAR_PER};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tag {
    Replace,
    Delete,
    Insert,
    Equal,
}

/// `a[a] -> b[b]`, as one of get_opcodes()'s tuples.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Opcode {
    pub tag: Tag,
    pub a: Range<usize>,
    pub b: Range<usize>,
}

struct Matcher<'s> {
    a: &'s [char],
    b: &'s [char],
    /// Where each element of b that is not popular stands in b, ascending.
    b2j: HashMap<char, Vec<usize>>,
}

impl<'s> Matcher<'s> {
    fn new(a: &'s [char], b: &'s [char]) -> Self {
        let mut b2j: HashMap<char, Vec<usize>> = HashMap::new();
        for (j, &c) in b.iter().enumerate() {
            b2j.entry(c).or_default().push(j);
        }
        if b.len() >= DIFFLIB_AUTOJUNK_MIN_LEN {
            let most = b.len() / DIFFLIB_POPULAR_PER + 1;
            b2j.retain(|_, js| js.len() <= most);
        }
        Matcher { a, b, b2j }
    }

    /// find_longest_match: the longest equal run of a[alo..ahi] and b[blo..bhi] that starts at a
    /// non-popular element, earliest in a and then in b, grown on both ends by equal elements.
    fn longest_match(&self, alo: usize, ahi: usize, blo: usize, bhi: usize) -> (usize, usize, usize) {
        let (mut besti, mut bestj, mut bestsize) = (alo, blo, 0);
        let mut j2len: HashMap<usize, usize> = HashMap::new();
        for i in alo..ahi {
            let mut next: HashMap<usize, usize> = HashMap::new();
            for &j in self.b2j.get(&self.a[i]).map(Vec::as_slice).unwrap_or(&[]) {
                if j < blo {
                    continue;
                }
                if j >= bhi {
                    break;
                }
                let k = j.checked_sub(1).and_then(|p| j2len.get(&p)).copied().unwrap_or(0) + 1;
                next.insert(j, k);
                if k > bestsize {
                    (besti, bestj, bestsize) = (i + 1 - k, j + 1 - k, k);
                }
            }
            j2len = next;
        }
        while besti > alo && bestj > blo && self.a[besti - 1] == self.b[bestj - 1] {
            (besti, bestj, bestsize) = (besti - 1, bestj - 1, bestsize + 1);
        }
        while besti + bestsize < ahi && bestj + bestsize < bhi && self.a[besti + bestsize] == self.b[bestj + bestsize] {
            bestsize += 1;
        }
        (besti, bestj, bestsize)
    }

    /// get_matching_blocks: the matches found by splitting around each longest match, sorted,
    /// adjacent ones joined, then the (len a, len b, 0) sentinel.
    fn matching_blocks(&self) -> Vec<(usize, usize, usize)> {
        let (la, lb) = (self.a.len(), self.b.len());
        let mut queue = vec![(0, la, 0, lb)];
        let mut blocks = Vec::new();
        while let Some((alo, ahi, blo, bhi)) = queue.pop() {
            let (i, j, k) = self.longest_match(alo, ahi, blo, bhi);
            if k == 0 {
                continue;
            }
            blocks.push((i, j, k));
            if alo < i && blo < j {
                queue.push((alo, i, blo, j));
            }
            if i + k < ahi && j + k < bhi {
                queue.push((i + k, ahi, j + k, bhi));
            }
        }
        blocks.sort_unstable();
        let mut joined: Vec<(usize, usize, usize)> = Vec::with_capacity(blocks.len() + 1);
        let (mut i1, mut j1, mut k1) = (0, 0, 0);
        for (i2, j2, k2) in blocks {
            if i1 + k1 == i2 && j1 + k1 == j2 {
                k1 += k2;
            } else {
                if k1 > 0 {
                    joined.push((i1, j1, k1));
                }
                (i1, j1, k1) = (i2, j2, k2);
            }
        }
        if k1 > 0 {
            joined.push((i1, j1, k1));
        }
        joined.push((la, lb, 0));
        joined
    }
}

/// The edits that turn `a` into `b`, as difflib's get_opcodes() lists them.
pub fn opcodes(a: &[char], b: &[char]) -> Vec<Opcode> {
    let (mut i, mut j) = (0, 0);
    let mut out = Vec::new();
    for (ai, bj, size) in Matcher::new(a, b).matching_blocks() {
        let tag = match (i < ai, j < bj) {
            (true, true) => Some(Tag::Replace),
            (true, false) => Some(Tag::Delete),
            (false, true) => Some(Tag::Insert),
            (false, false) => None,
        };
        if let Some(tag) = tag {
            out.push(Opcode { tag, a: i..ai, b: j..bj });
        }
        (i, j) = (ai + size, bj + size);
        if size > 0 {
            out.push(Opcode { tag: Tag::Equal, a: ai..i, b: bj..j });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 1,000 pairs (every twentieth 200 to 420 characters long, past the autojunk length) and
    /// CPython's opcodes of each, written once by CPython 3.14.
    const CPYTHON: &str = include_str!("../../testdata/difflib_opcodes.tsv");

    fn written(ops: &[Opcode]) -> String {
        let letter = |t: Tag| match t {
            Tag::Replace => 'r',
            Tag::Delete => 'd',
            Tag::Insert => 'i',
            Tag::Equal => 'e',
        };
        let parts: Vec<String> =
            ops.iter().map(|o| format!("{}{},{},{},{}", letter(o.tag), o.a.start, o.a.end, o.b.start, o.b.end)).collect();
        parts.join(" ")
    }

    #[test]
    fn opcodes_are_cpythons() {
        let mut pairs = 0;
        for line in CPYTHON.lines().filter(|l| !l.starts_with('#')) {
            let f: Vec<&str> = line.split('\t').collect();
            let (a, b): (Vec<char>, Vec<char>) = (f[0].chars().collect(), f[1].chars().collect());
            assert_eq!(written(&opcodes(&a, &b)), f[2], "a={:?} b={:?}", f[0], f[1]);
            pairs += 1;
        }
        assert_eq!(pairs, 1000);
    }

    #[test]
    fn popular_elements_of_a_long_b_start_no_match() {
        let op = |tag, a: Range<usize>, b: Range<usize>| Opcode { tag, a, b };
        let a: Vec<char> = format!("{}ab", "x".repeat(10)).chars().collect();
        let long: Vec<char> = format!("ab{}", "x".repeat(250)).chars().collect();
        assert_eq!(
            opcodes(&a, &long),
            [op(Tag::Delete, 0..10, 0..0), op(Tag::Equal, 10..12, 0..2), op(Tag::Insert, 12..12, 2..252)]
        );
        assert_eq!(
            opcodes(&a, &long[..150]),
            [op(Tag::Insert, 0..0, 0..2), op(Tag::Equal, 0..10, 2..12), op(Tag::Replace, 10..12, 12..150)]
        );
    }
}
