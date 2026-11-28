use super::{invalid, map, terms::*, PostingList, Sub};
use crate::keys::Keys;
use fst::{Automaton, Streamer};
use levenshtein_automata::{Distance, LevenshteinAutomatonBuilder, DFA, SINK_STATE};
use memmap2::Mmap;
use std::{io, path::Path};

pub struct Dictionary {
    fst: Keys<Mmap>,
    post: Mmap,
    entities: u32,
}
impl Dictionary {
    pub(crate) fn open(dir: &Path, entities: u32) -> io::Result<Self> {
        Ok(Self {
            fst: Keys::new(map(&dir.join("core.fst"))?).map_err(invalid)?,
            post: map(&dir.join("core.post"))?,
            entities,
        })
    }
    pub fn posting(&self, reference: PostingRef) -> Option<PostingList<'_>> {
        if reference.sub != Sub::Core {
            return None;
        }
        PostingList::read(&self.post, reference.offset, self.entities)
    }
    fn decode(&self, key: &[u8], value: u64) -> Option<Term> {
        let (&byte, tail) = key.split_first()?;
        let kind = TermKind::from_byte(byte)?;
        let sep = tail.iter().position(|&b| b == 0)?;
        let token = std::str::from_utf8(&tail[..sep]).ok()?;
        let mut suffix = &tail[sep + 1..];
        let mut word = token.to_string();
        if matches!(kind, TermKind::Word | TermKind::Country) {
            let (&order, rest) = suffix.split_first()?;
            suffix = rest;
            if order != crate::constants::WORD_SAME {
                let (length, rest) = suffix.split_at_checked(2)?;
                let length = u16::from_be_bytes(length.try_into().ok()?);
                let shared = match order {
                    crate::constants::WORD_BEFORE => length,
                    crate::constants::WORD_AFTER => u16::MAX - length,
                    _ => return None,
                };
                let n = rest.iter().position(|&b| b == 0).unwrap_or(rest.len());
                let mut bytes = token.as_bytes().get(..usize::from(shared))?.to_vec();
                bytes.extend_from_slice(&rest[..n]);
                word = String::from_utf8(bytes).ok()?;
                suffix = &rest[n..];
            }
        }
        let mut fields = suffix.split(|&b| b == 0);
        if !fields.next()?.is_empty() {
            return None;
        }
        let lookup = match fields.next() {
            None | Some([]) => word.split(crate::constants::TERM_WORD_LANGUAGE_MARK).next()?,
            Some([crate::constants::LOOKUP_IS_TOKEN]) => token,
            Some(s) => std::str::from_utf8(s).ok()?,
        }
        .to_owned();
        let info =
            if kind == TermKind::Country { TermInfo::Country(fields.next()?.try_into().ok()?) } else { TermInfo::None };
        if fields.next().is_some() {
            return None;
        }
        let posting = PostingRef { sub: Sub::Core, offset: value };
        let list = self.posting(posting)?;
        Some(Term {
            kind,
            token: token.into(),
            word,
            lookup_word: lookup,
            count: list.count,
            addr_count: list.addr_count,
            info,
            postings: vec![posting],
        })
    }
    fn matching(&self, aut: impl Automaton, max: usize) -> Vec<Term> {
        let mut stream = self.fst.search(aut);
        let mut terms = Vec::new();
        while terms.len() < max {
            let Some((key, val)) = stream.next() else { break };
            if let Some(term) = self.decode(key, val) {
                terms.push(term)
            }
        }
        terms
    }
}
// The matcher consumes only the token. A successful token then accepts its identity suffix.
struct Fuzzy<'a> {
    kind: u8,
    dfa: &'a DFA,
    anchor: &'a [u8],
}
#[derive(Clone, Copy)]
enum State {
    Kind,
    Token(u32, usize),
    Suffix,
    Dead,
}
impl Automaton for Fuzzy<'_> {
    type State = State;
    fn start(&self) -> State {
        State::Kind
    }
    fn is_match(&self, s: &State) -> bool {
        matches!(s, State::Suffix)
    }
    fn can_match(&self, s: &State) -> bool {
        !matches!(s, State::Dead)
    }
    fn accept(&self, s: &State, b: u8) -> State {
        match *s {
            State::Kind if b == self.kind => State::Token(self.dfa.initial_state(), 0),
            State::Token(q, pos)
                if b == 0 && pos >= self.anchor.len() && matches!(self.dfa.distance(q), Distance::Exact(_)) =>
            {
                State::Suffix
            }
            State::Token(q, pos) if b != 0 && self.anchor.get(pos).is_none_or(|&a| a == b) => {
                let next = self.dfa.transition(q, b);
                if next == SINK_STATE {
                    State::Dead
                } else {
                    State::Token(next, pos + 1)
                }
            }
            State::Suffix => State::Suffix,
            _ => State::Dead,
        }
    }
}
impl TermSource for Dictionary {
    fn exact(&self, kind: TermKind, token: &str) -> Vec<Term> {
        if token.contains('\0') {
            return Vec::new();
        }
        let key = format!("{}{token}\0", kind as u8 as char);
        self.matching(fst::automaton::Str::new(&key).starts_with(), usize::MAX)
    }
    fn prefix(&self, kind: TermKind, prefix: &str, max: usize) -> Vec<Term> {
        if prefix.contains('\0') {
            return Vec::new();
        }
        let key = format!("{}{prefix}", kind as u8 as char);
        self.matching(fst::automaton::Str::new(&key).starts_with(), max)
    }
    fn fuzzy(&self, kind: TermKind, word: &str, fuzz: Fuzz, max: usize) -> Vec<Term> {
        if max == 0 || fuzz.edits > crate::constants::PHOTON_MAX_EDITS {
            return Vec::new();
        }
        let dfa = LevenshteinAutomatonBuilder::new(fuzz.edits, true).build_dfa(word);
        let anchor = word.char_indices().nth(fuzz.anchor).map_or(word.len(), |(i, _)| i);
        self.matching(Fuzzy { kind: kind as u8, dfa: &dfa, anchor: &word.as_bytes()[..anchor] }, max)
    }
}
