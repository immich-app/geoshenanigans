//! A query's tokens, as Nominatim's analyzer finds them (search/icu_tokenizer.py:116-300,
//! search/query.py): every n-gram of its nodes looked up in a term source, its special phrases,
//! house numbers of digits, postcodes from the parser, and the penalties tokens pay for each
//! other and for how well they match the query. In the photon mode also the last word's
//! completions and, in a second reading, near spellings of its words.
use super::phrases::{Operator, Phrase, Phrases};
use super::plan::TermId;
use super::tokenize::{Node, Nodes};
use crate::constants::*;
use crate::index::terms::{Fuzz, Term, TermInfo, TermKind, TermSource};
use crate::text::difflib::{opcodes, Tag};
use crate::text::normalize::decimal_digit;
use crate::text::postcode::{Node as PostcodeNode, PostcodeParser};
use crate::text::regex::is_python_space;
use crate::text::Break;
use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;

/// What a token stands for in a reading of the query (search/query.py:53-69).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TokenType {
    Word,
    Partial,
    HouseNumber,
    Postcode,
    Country,
    Qualifier,
    NearItem,
}

impl TokenType {
    /// Nominatim's letter for it.
    pub fn letter(self) -> char {
        match self {
            TokenType::Word => 'W',
            TokenType::Partial => 'w',
            TokenType::HouseNumber => 'H',
            TokenType::Postcode => 'P',
            TokenType::Country => 'C',
            TokenType::Qualifier => 'Q',
            TokenType::NearItem => 'N',
        }
    }

    fn of(kind: TermKind) -> TokenType {
        match kind {
            TermKind::Word => TokenType::Word,
            TermKind::Partial => TokenType::Partial,
            TermKind::HouseNumber => TokenType::HouseNumber,
            TermKind::Postcode => TokenType::Postcode,
            TermKind::Country => TokenType::Country,
            TermKind::Category | TermKind::AddressWord => TokenType::Qualifier,
        }
    }
}

/// How a query's words are looked up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lookup {
    /// Each n-gram exactly: the nominatim mode.
    Exact,
    /// Also the last word's completions: the photon mode.
    Prefix,
    /// Also near spellings of each word: the photon mode's reading when the first finds nothing.
    Typo,
}

/// Nodes `start..end` of a query, which a token spans.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Span {
        Span { start, end }
    }

    /// Whether it ends where `other` starts or before (query.py:197-198).
    pub fn before(self, other: Span) -> bool {
        self.end <= other.start
    }

    /// Whether it starts where `other` ends or after (query.py:203-204).
    pub fn after(self, other: Span) -> bool {
        self.start >= other.end
    }

    fn len(self) -> usize {
        self.end - self.start
    }
}

/// A token: one of the query's terms at a place in it, at its own penalty.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Token {
    pub term: TermId,
    pub penalty: f64,
}

/// The tokens of one type from a node to `end`.
#[derive(Clone, Debug, PartialEq)]
pub struct TokenList {
    pub end: usize,
    pub ttype: TokenType,
    pub tokens: Vec<Token>,
}

/// A node of the query graph: the break before it, its word, the partial term of its word and
/// the other tokens that start at it.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryNode {
    pub node: Node,
    /// A missing word has the missing-partial penalty; the end node has no partial.
    pub partial: Option<Token>,
    pub starting: Vec<TokenList>,
}

/// A query's nodes with their tokens, and the terms they are of.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryGraph {
    pub phrases: usize,
    pub nodes: Vec<QueryNode>,
    pub terms: Vec<Term>,
    pub partial_matches: BTreeMap<TermId, Vec<Token>>,
    ids: HashMap<(TermKind, String, String, TermInfo), TermId>,
}

/// The tokens of `nodes`, their terms looked up in `source` as `lookup` says.
pub fn tokens(nodes: Nodes, source: &dyn TermSource, lookup: Lookup) -> QueryGraph {
    let mut g = QueryGraph {
        phrases: nodes.phrases,
        nodes: nodes.nodes.into_iter().map(|node| QueryNode { node, partial: None, starting: Vec::new() }).collect(),
        terms: Vec::new(),
        ids: HashMap::new(),
        partial_matches: BTreeMap::new(),
    };
    if g.slots() == 0 {
        return g;
    }

    let words = g.words();
    for (word, spans) in &words {
        for kind in [TermKind::Word, TermKind::Partial, TermKind::HouseNumber, TermKind::Country] {
            // A partial term is one word: an n-gram of several is none.
            if kind == TermKind::Partial && word.contains(' ') {
                continue;
            }
            for term in source.exact(kind, word) {
                g.add_term(spans, term);
            }
        }
        for phrase in Phrases::get().of(word) {
            g.add_phrase(word, spans, phrase);
        }
    }
    if lookup != Lookup::Exact {
        let mut members = g.completions(&words, source);
        if lookup == Lookup::Typo {
            g.near_spellings(source, &mut members);
        }
        for (i, terms) in members {
            g.widen_partial(i, terms);
        }
    }
    g.fill_missing_partials();
    g.add_digit_housenumbers();
    g.add_postcodes();
    g.rerank();
    g
}

impl QueryGraph {
    /// The token slots between the nodes.
    pub fn slots(&self) -> usize {
        self.nodes.len() - 1
    }

    pub fn term(&self, id: TermId) -> &Term {
        &self.terms[id.0 as usize]
    }

    /// The tokens of `ttype` over `span`; none for partials, which are the nodes' own.
    pub fn tokens(&self, span: Span, ttype: TokenType) -> &[Token] {
        self.nodes[span.start].starting.iter().find(|l| l.end == span.end && l.ttype == ttype).map_or(&[], |l| &l.tokens[..])
    }

    /// The partial of node `i`, where its word is one.
    pub fn partial(&self, i: usize) -> Option<Token> {
        self.nodes[i].partial.filter(|p| p.penalty < PARTIAL_MISSING_PENALTY)
    }

    /// The partials of `span`'s nodes, each that is one (query.py:415-419).
    pub fn partials(&self, span: Span) -> impl Iterator<Item = Token> + '_ {
        (span.start..span.end).filter_map(|i| self.nodes[i].partial)
    }

    /// What going on over the nodes inside `span` costs (query.py:408-413).
    pub fn in_word_penalty(&self, span: Span) -> f64 {
        compensated_sum(self.nodes[span.start + 1..span.end].iter().map(|n| n.node.word_continuation_penalty()))
    }

    /// The penalties of `span`'s partials and of the word breaks between them (query.py:421-427).
    pub fn partial_penalty(&self, span: Span) -> f64 {
        let penalty = |i: usize| self.nodes[i].partial.map_or(PARTIAL_MISSING_PENALTY, |p| p.penalty);
        penalty(span.start) + compensated_sum((span.start + 1..span.end).map(|i| penalty(i) + self.nodes[i].node.word_break_penalty()))
    }

    /// The direction a query is read in, from its partials' counts (query.py:383-396): negative
    /// for left to right, positive for right to left, 0 for no telling.
    pub fn direction_penalty(&self) -> f64 {
        let n = self.slots();
        if n <= 1 || n >= DIRECTION_MAX_SLOTS {
            return 0.0;
        }

        let ratio = |i: usize| {
            let (count, addr_count) = self.nodes[i].partial.map_or((1, 1), |p| (self.term(p.term).count, self.term(p.term).addr_count));
            f64::from(count) / (f64::from(count) + f64::from(addr_count))
        };
        if n == 2 {
            return (ratio(1) - ratio(0)) / DIRECTION_TWO_SLOT_DIVISOR;
        }
        let nf = n as f64;
        let weighted = compensated_sum((0..n).map(|i| i as f64 * ratio(i)));
        let total = compensated_sum((0..n).map(ratio));
        // The denominator of the slope of a linear regression over positions 0..n.
        let squares: f64 = (0..n).map(|i| (i * i) as f64).sum();
        let linfac = nf * (squares - (nf - 1.0) * nf * (nf - 1.0) / 4.0);
        (nf * weighted - total * nf * (nf - 1.0) / 2.0) / linfac
    }

    /// Each n-gram of the query's words (tokens joined by spaces) and where it is: at most
    /// NGRAM_MAX_NODES nodes and NGRAM_MAX_CHARS characters, never over a phrase break, in the
    /// order Nominatim lists them (query.py:457-483).
    fn words(&self) -> Vec<(String, Vec<Span>)> {
        let mut words: Vec<(String, Vec<Span>)> = Vec::new();
        let mut at: HashMap<String, usize> = HashMap::new();
        let mut push = |word: &str, span: Span| match at.get(word) {
            Some(&i) => words[i].1.push(span),
            None => {
                at.insert(word.to_string(), words.len());
                words.push((word.to_string(), vec![span]));
            }
        };
        let slots = self.slots();
        for first in 0..slots {
            let mut word = self.nodes[first].node.token.clone();
            push(&word, Span::new(first, first + 1));
            for last in first + 1..(first + NGRAM_MAX_NODES).min(slots) {
                let node = &self.nodes[last].node;
                if node.btype == Break::Phrase {
                    break;
                }
                word = format!("{word} {}", node.token);
                if word.chars().count() > NGRAM_MAX_CHARS {
                    break;
                }
                push(&word, Span::new(first, last + 1));
            }
        }
        words
    }

    /// The id of `term` among the query's terms, as the reading uses it: its counts at least 1
    /// and its lookup word its token where it has none (icu_tokenizer.py:57-86).
    fn intern(&mut self, mut term: Term) -> TermId {
        let key = (term.kind, term.token.clone(), term.word.clone(), term.info.clone());
        if let Some(&id) = self.ids.get(&key) {
            return id;
        }
        term.count = term.count.max(1);
        term.addr_count = term.addr_count.max(1);
        if term.lookup_word.is_empty() {
            term.lookup_word = term.token.clone();
        }
        let id = TermId(self.terms.len() as u32);
        self.terms.push(term);
        self.ids.insert(key, id);
        id
    }

    /// A term looked up for the n-gram at `spans`, a token at each, at its base penalty.
    fn add_term(&mut self, spans: &[Span], term: Term) {
        let ttype = TokenType::of(term.kind);
        let penalty = base_penalty(&term);
        let id = self.intern(term);
        for &span in spans {
            self.add(span, ttype, Token { term: id, penalty });
        }
    }

    /// A special phrase found at `spans`: a near item where it starts the query (and, without an
    /// operator, makes all of it), else a qualifier (icu_tokenizer.py:146-156).
    fn add_phrase(&mut self, token: &str, spans: &[Span], phrase: &Phrase) {
        let term = Term {
            kind: TermKind::Category,
            token: token.to_string(),
            word: phrase.word.clone(),
            lookup_word: phrase.word.clone(),
            count: 1,
            addr_count: 1,
            info: TermInfo::Category(phrase.category.clone()),
            postings: Vec::new(),
        };
        let id = self.intern(term);
        let slots = self.slots();
        for &span in spans {
            let ttype = match phrase.operator {
                Operator::In | Operator::Near if span.start == 0 => TokenType::NearItem,
                Operator::In | Operator::Near => continue,
                Operator::Plain if span.start == 0 && span.end == slots => TokenType::NearItem,
                Operator::Plain => TokenType::Qualifier,
            };
            self.add(span, ttype, Token { term: id, penalty: 0.0 });
        }
    }

    /// A token at `span`: a node's partial, or one of the tokens that start at a node. A qualifier
    /// that would make a whole phrase is dropped (query.py:104-109, 356-381).
    fn add(&mut self, span: Span, ttype: TokenType, token: Token) {
        let full_phrase = matches!(self.nodes[span.start].node.btype, Break::Start | Break::Phrase)
            && matches!(self.nodes[span.end].node.btype, Break::Phrase | Break::End);
        let node = &mut self.nodes[span.start];
        match ttype {
            TokenType::Partial => node.partial = Some(token),
            TokenType::Qualifier if full_phrase => {}
            _ => match node.starting.iter_mut().find(|l| l.end == span.end && l.ttype == ttype) {
                Some(list) if !list.tokens.contains(&token) => list.tokens.push(token),
                Some(_) => {}
                None => node.starting.push(TokenList { end: span.end, ttype, tokens: vec![token] }),
            },
        }
    }

    /// The photon mode's completions of the last word: every n-gram that ends with it gets the
    /// names, house numbers and countries it starts; the last word's partial terms that it starts
    /// are returned, its own among them.
    fn completions(&mut self, words: &[(String, Vec<Span>)], source: &dyn TermSource) -> BTreeMap<usize, Vec<Term>> {
        let slots = self.slots();
        let last = slots - 1;
        let last_word = &self.nodes[last].node.token;
        let numeric = last_word.chars().all(|c| decimal_digit(c).is_some());
        // A numeric prefix may finish a word boundary or a suffix, but 1044 must never find 10440.
        let allowed = |prefix: &str, term: &Term| {
            !numeric || term.token.strip_prefix(prefix).and_then(|s| s.chars().next()).is_none_or(|c| decimal_digit(c).is_none())
        };
        for (word, spans) in words {
            let at_end: Vec<Span> = spans.iter().copied().filter(|s| s.end == slots).collect();
            if at_end.is_empty() {
                continue;
            }
            for kind in [TermKind::Word, TermKind::HouseNumber, TermKind::Country] {
                for term in source.prefix(kind, word, PREFIX_TERMS_MAX) {
                    if term.token != *word && allowed(word, &term) {
                        self.add_term(&at_end, term);
                    }
                }
            }
        }
        let prefix = &self.nodes[last].node.token;
        let partials = source.prefix(TermKind::Partial, prefix, PREFIX_TERMS_MAX).into_iter().filter(|t| allowed(prefix, t)).collect();
        BTreeMap::from([(last, partials)])
    }

    /// The photon mode's near spellings (Photon's AUTO fuzziness), in a query of
    /// TYPO_MIN_QUERY_CHARS or more: each word without digits gets the names within its edits,
    /// and its partial terms within them go into `members`.
    fn near_spellings(&mut self, source: &dyn TermSource, members: &mut BTreeMap<usize, Vec<Term>>) {
        let slots = self.slots();
        let query_chars = self.nodes[..slots].iter().map(|n| n.node.token.chars().count()).sum::<usize>() + slots - 1;
        if query_chars < TYPO_MIN_QUERY_CHARS {
            return;
        }

        for i in 0..slots {
            let token = self.nodes[i].node.token.clone();
            let chars = token.chars().count();
            let edits = match chars {
                n if n >= TYPO_TWO_EDITS_MIN_CHARS => 2,
                n if n >= TYPO_ONE_EDIT_MIN_CHARS => 1,
                _ => continue,
            };
            if token.chars().any(|c| decimal_digit(c).is_some()) {
                continue;
            }
            let anchor = if chars <= TYPO_ANCHOR_SHORT_MAX_CHARS { TYPO_ANCHOR_SHORT } else { TYPO_ANCHOR_LONG };
            let fuzz = Fuzz { edits, anchor };
            for term in source.fuzzy(TermKind::Word, &token, fuzz, FUZZY_TERMS_MAX) {
                if term.token != token {
                    self.add_term(&[Span::new(i, i + 1)], term);
                }
            }
            members.entry(i).or_default().extend(source.fuzzy(TermKind::Partial, &token, fuzz, FUZZY_TERMS_MAX));
        }
    }

    /// Node `i`'s partial becomes the union of `members` and its own: their posting lists, their
    /// summed counts, at the partial penalty and the best match penalty of any of them.
    fn widen_partial(&mut self, i: usize, members: Vec<Term>) {
        let node = &self.nodes[i];
        let mut union: Vec<Term> = node.partial.map(|p| self.term(p.term).clone()).into_iter().collect();
        for term in members {
            if !union.iter().any(|u| (u.kind, &u.token, &u.word, &u.info) == (term.kind, &term.token, &term.word, &term.info)) {
                union.push(term);
            }
        }
        if union.len() <= 1 && node.partial.is_some() || union.is_empty() {
            return;
        }

        let word = node.node.word.clone();
        let term = Term {
            kind: TermKind::Partial,
            token: node.node.token.clone(),
            // No partial is made from an empty word, so the union is no term of the source.
            word: String::new(),
            lookup_word: word,
            count: union.iter().fold(0u32, |s, t| s.saturating_add(t.count.max(1))),
            addr_count: union.iter().fold(0u32, |s, t| s.saturating_add(t.addr_count.max(1))),
            info: TermInfo::None,
            postings: union.iter().flat_map(|t| t.postings.iter().copied()).collect(),
        };
        let mut members: Vec<_> = union
            .into_iter()
            .map(|member| {
                let id = self.intern(member);
                Token { term: id, penalty: match_penalty(&self.term(id).lookup_word, &term.lookup_word) }
            })
            .collect();
        let best = members.iter().map(|m| m.penalty).fold(f64::INFINITY, f64::min);
        for member in &mut members {
            member.penalty -= best;
        }
        // Equal text can widen differently at two positions: only the last gets prefix matches.
        let id = TermId(self.terms.len() as u32);
        self.terms.push(term);
        self.partial_matches.insert(id, members);
        self.nodes[i].partial = Some(Token { term: id, penalty: PARTIAL_PENALTY + best });
    }

    // The oracle reranks missing words too. Their counts inform direction, but
    // their penalty excludes them from assignments and all driving lookups.
    fn fill_missing_partials(&mut self) {
        for i in 0..self.slots() {
            if self.nodes[i].partial.is_none() {
                let node = &self.nodes[i].node;
                let id = self.intern(Term {
                    kind: TermKind::Partial,
                    token: node.token.clone(),
                    word: node.token.clone(),
                    lookup_word: node.word.clone(),
                    count: 1,
                    addr_count: 1,
                    info: TermInfo::None,
                    postings: Vec::new(),
                });
                self.nodes[i].partial = Some(Token { term: id, penalty: PARTIAL_MISSING_PENALTY });
            }
        }
    }

    /// A whole word of digits that no house-number term reads becomes one (icu_tokenizer.py:244-262).
    fn add_digit_housenumbers(&mut self) {
        let mut candidate: Option<String> = None;
        for i in 0..self.nodes.len() {
            if matches!(self.nodes[i].node.btype, Break::Token | Break::Part) {
                candidate = None;
                continue;
            }
            if let Some(digits) = candidate.take() {
                let term = Term {
                    kind: TermKind::HouseNumber,
                    token: digits.clone(),
                    word: digits.clone(),
                    lookup_word: digits,
                    count: 1,
                    addr_count: 1,
                    info: TermInfo::None,
                    postings: Vec::new(),
                };
                let id = self.intern(term);
                self.add(Span::new(i - 1, i), TokenType::HouseNumber, Token { term: id, penalty: DIGITS_HOUSENUMBER_PENALTY });
            }
            let node = &self.nodes[i];
            let word = &node.node.word;
            let digits = !word.is_empty() && word.chars().count() <= DIGITS_HOUSENUMBER_MAX_CHARS && word.chars().all(|c| decimal_digit(c).is_some());
            if digits && self.tokens(Span::new(i, i + 1), TokenType::HouseNumber).is_empty() {
                candidate = Some(node.node.token.clone());
            }
        }
    }

    /// The postcodes the parser reads, each a token with the code as its lookup word
    /// (icu_tokenizer.py:161-167).
    fn add_postcodes(&mut self) {
        static PARSER: OnceLock<PostcodeParser> = OnceLock::new();
        let parser = PARSER.get_or_init(PostcodeParser::default);
        let nodes: Vec<PostcodeNode> = self.nodes.iter().map(|n| PostcodeNode { btype: n.node.btype, word: &n.node.word }).collect();
        for pc in parser.parse(&nodes) {
            let token = self.nodes[pc.start..pc.end].iter().map(|n| n.node.token.as_str()).collect::<Vec<_>>().join(" ");
            let term = Term {
                kind: TermKind::Postcode,
                token,
                word: pc.code.clone(),
                lookup_word: pc.code,
                count: 1,
                addr_count: 1,
                info: TermInfo::None,
                postings: Vec::new(),
            };
            let id = self.intern(term);
            self.add(Span::new(pc.start, pc.end), TokenType::Postcode, Token { term: id, penalty: 0.0 });
        }
    }

    /// The penalties tokens pay for the others over the same span, and for how far each one's
    /// lookup word is from the query's words there (icu_tokenizer.py:264-300).
    fn rerank(&mut self) {
        for start in 0..self.nodes.len() {
            let mut ends: Vec<usize> = Vec::new();
            for list in &self.nodes[start].starting {
                if !ends.contains(&list.end) {
                    ends.push(list.end);
                }
            }
            for end in ends {
                self.rerank_span(Span::new(start, end));
            }
        }
    }

    fn rerank_span(&mut self, span: Span) {
        let types: Vec<TokenType> = self.nodes[span.start].starting.iter().filter(|l| l.end == span.end).map(|l| l.ttype).collect();
        let one_node = span.len() == 1;
        if types.len() > 1 {
            if types.contains(&TokenType::Postcode) {
                let first_word_chars = self.nodes[span.start].node.token.chars().count();
                for &ttype in &types {
                    if ttype != TokenType::Postcode && (ttype != TokenType::HouseNumber || first_word_chars > POSTCODE_RERANK_HOUSENUMBER_MAX_CHARS) {
                        self.add_penalty(span, ttype, POSTCODE_RERANK_PENALTY);
                    }
                    // Once for every type over the span, the postcode's own too, as Nominatim adds it.
                    if one_node {
                        self.add_partial_penalty(span.start, POSTCODE_RERANK_PENALTY);
                    }
                }
            }
            if let Some(&first) = self.tokens(span, TokenType::HouseNumber).first() {
                let lookup = &self.term(first.term).lookup_word;
                let simple = lookup.chars().count() <= SIMPLE_HOUSENUMBER_MAX_CHARS && lookup.chars().any(|c| decimal_digit(c).is_some());
                if simple {
                    let penalty = SIMPLE_HOUSENUMBER_PENALTY - first.penalty;
                    for &ttype in types.iter().filter(|&&t| t != TokenType::HouseNumber) {
                        self.add_penalty(span, ttype, penalty);
                    }
                    if one_node {
                        self.add_partial_penalty(span.start, penalty);
                    }
                }
            }
        }
        let norm = self.nodes[span.start..span.end]
            .iter()
            .map(|n| format!("{}{}", if n.node.btype == Break::Token { "" } else { " " }, n.node.word))
            .collect::<String>();
        let norm = norm.trim_matches(is_python_space).to_string();
        for ttype in types {
            let factor = if ttype == TokenType::Word { 1.0 } else { MATCH_PENALTY_FACTOR_OTHER };
            let lookups: Vec<f64> = self.tokens(span, ttype).iter().map(|t| match_penalty(&self.term(t.term).lookup_word, &norm) * factor).collect();
            let list = self.nodes[span.start].starting.iter_mut().find(|l| l.end == span.end && l.ttype == ttype).expect("a type of the span");
            for (token, extra) in list.tokens.iter_mut().zip(lookups) {
                token.penalty += extra;
            }
        }
    }

    fn add_penalty(&mut self, span: Span, ttype: TokenType, penalty: f64) {
        if let Some(list) = self.nodes[span.start].starting.iter_mut().find(|l| l.end == span.end && l.ttype == ttype) {
            for token in &mut list.tokens {
                token.penalty += penalty;
            }
        }
    }

    fn add_partial_penalty(&mut self, i: usize, penalty: f64) {
        if let Some(partial) = &mut self.nodes[i].partial {
            partial.penalty += penalty;
        }
    }
}

// Python 3.12 sums floats with compensation. A last-bit difference here can
// exchange two searches before the executor applies its round limit.
fn compensated_sum(values: impl Iterator<Item = f64>) -> f64 {
    let mut sum: f64 = 0.0;
    let mut residual = 0.0;
    for value in values {
        let next = sum + value;
        residual += if sum.abs() >= value.abs() { (sum - next) + value } else { (value - next) + sum };
        sum = next;
    }
    sum + residual
}

/// A looked-up token's penalty before any other token is known (icu_tokenizer.py:60-72).
fn base_penalty(term: &Term) -> f64 {
    let token: Vec<char> = term.token.chars().collect();
    let digit = |c: &char| decimal_digit(*c).is_some();
    match term.kind {
        TermKind::Partial => PARTIAL_PENALTY,
        TermKind::Word if token.len() == 1 && term.token == term.word => {
            if digit(&token[0]) {
                WORD_ONE_DIGIT_PENALTY
            } else {
                WORD_ONE_CHAR_PENALTY
            }
        }
        TermKind::HouseNumber => {
            let letters = token.iter().filter(|c| **c != ' ' && !digit(c)).count();
            let no_digit = if token.iter().any(digit) { 0.0 } else { HOUSENUMBER_NO_DIGIT_PENALTY * token.len().saturating_sub(1) as f64 };
            HOUSENUMBER_NON_DIGIT_PENALTY * letters as f64 + no_digit
        }
        TermKind::Country if token.len() == 1 => COUNTRY_ONE_CHAR_PENALTY,
        _ => 0.0,
    }
}

/// How far `lookup` is from the query's words `norm`, per character of `lookup`: an edit at either
/// end counts once, a replacement its longer side, an edit inside the difference of its sides
/// (query.py:148-164).
pub fn match_penalty(lookup: &str, norm: &str) -> f64 {
    let a: Vec<char> = lookup.chars().collect();
    if a.is_empty() {
        return 0.0;
    }

    let b: Vec<char> = norm.chars().collect();
    let distance: usize = opcodes(&a, &b)
        .iter()
        .map(|op| match op.tag {
            Tag::Equal => 0,
            Tag::Delete | Tag::Insert if op.a.start == 0 || op.a.end == a.len() => 1,
            Tag::Replace => op.a.len().max(op.b.len()),
            Tag::Delete | Tag::Insert => op.a.len().abs_diff(op.b.len()),
        })
        .sum();
    distance as f64 / a.len() as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::terms::MemTerms;
    use crate::query::readings::{reading, readings};
    use crate::query::tokenize::split;

    fn graph(q: &str, terms: &MemTerms, lookup: Lookup) -> QueryGraph {
        tokens(split(q).unwrap(), terms, lookup)
    }

    /// The graph's tokens as Nominatim's debug page lists them, sorted: type, span, token, lookup
    /// word, penalty to 0.001, count and addr_count.
    pub(crate) fn rows(g: &QueryGraph) -> Vec<String> {
        let row = |t: char, start: usize, end: usize, token: &str, lookup: &str, penalty: f64, count: u32, addr_count: u32| {
            format!("{t} {start}-{end} {token} {lookup} {penalty:.3} {count} {addr_count}")
        };
        let mut out = Vec::new();
        for (i, n) in g.nodes.iter().enumerate() {
            if let Some(p) = n.partial {
                let t = g.term(p.term);
                out.push(row('w', i, i + 1, &n.node.token, &n.node.word, p.penalty, t.count, t.addr_count));
            }
            for list in &n.starting {
                for token in &list.tokens {
                    let t = g.term(token.term);
                    out.push(row(list.ttype.letter(), i, list.end, &t.token, &t.lookup_word, token.penalty, t.count, t.addr_count));
                }
            }
        }
        out.sort();
        out
    }

    /// Of one type: span, token, lookup word and penalty to 0.001.
    fn of_type(g: &QueryGraph, ttype: char) -> Vec<String> {
        rows(g)
            .into_iter()
            .filter(|r| r.starts_with(ttype))
            .map(|r| {
                let fields: Vec<&str> = r.split(' ').collect();
                fields[1..fields.len() - 2].join(" ")
            })
            .collect()
    }

    #[test]
    fn reads_the_tokens_nominatim_read() {
        for r in readings() {
            let g = graph(&r.q, &r.terms(), Lookup::Exact);

            let mut theirs: Vec<String> = r
                .tokens
                .iter()
                // The public database imports additional phrase languages.
                .filter(|t| !(r.label == "liberty-public" && t.ttype == 'Q' && matches!(t.lookup_word.as_str(), "парк" | "پارک")))
                .map(|t| format!("{} {}-{} {} {} {:.3} {} {}", t.ttype, t.start, t.end, t.token, t.lookup_word, t.penalty, t.count, t.addr_count))
                .collect();
            theirs.sort();
            assert_eq!(rows(&g), theirs, "{}", r.label);
            assert!((g.direction_penalty() - r.direction).abs() < 1e-9, "{}: {} not {}", r.label, g.direction_penalty(), r.direction);
        }
    }

    #[test]
    fn direction_preserves_the_oracles_rounding_for_close_searches() {
        let mut terms = MemTerms::default();
        for (word, count) in [("one", 1), ("two", 2), ("three", 2)] {
            terms.insert(TermKind::Partial, word, ("", ""), (count, 1), TermInfo::None);
        }
        let g = graph("one two three", &terms, Lookup::Exact);
        assert_eq!(g.direction_penalty(), 0.08333333333333333);
    }

    #[test]
    fn a_tokens_base_penalty_is_nominatims() {
        let term = |kind, token: &str, word: &str| Term {
            kind,
            token: token.into(),
            word: word.into(),
            lookup_word: String::new(),
            count: 1,
            addr_count: 1,
            info: TermInfo::None,
            postings: Vec::new(),
        };
        for (t, penalty) in [
            (term(TermKind::Partial, "park", "park"), 0.3),
            (term(TermKind::Word, "park", "park"), 0.0),
            (term(TermKind::Word, "8", "8"), 0.2),
            (term(TermKind::Word, "a", "a"), 0.3),
            (term(TermKind::Word, "a", "a@en"), 0.0),
            (term(TermKind::HouseNumber, "12", "12"), 0.0),
            (term(TermKind::HouseNumber, "12a", "12a"), 0.1),
            (term(TermKind::HouseNumber, "8 a", "8 a"), 0.1),
            (term(TermKind::HouseNumber, "ab", "ab"), 0.4),
            (term(TermKind::Country, "d", "d"), 0.3),
            (term(TermKind::Country, "de", "de"), 0.0),
            (term(TermKind::Postcode, "1", "1"), 0.0),
        ] {
            assert!((base_penalty(&t) - penalty).abs() < 1e-9, "{:?} {}: {}", t.kind, t.token, base_penalty(&t));
        }
    }

    #[test]
    fn a_match_penalty_counts_edits_per_character_of_the_lookup_word() {
        for (lookup, norm, penalty) in [
            ("futo", "futo", 0.0),
            ("フト", "futo", 2.0),
            ("LV-1044", "1044", 1.0 / 7.0),
            ("park  drive", "park drive", 1.0 / 11.0),
            ("8 a", "8a", 1.0 / 3.0),
            ("café", "cafe", 0.25),
            ("", "x", 0.0),
        ] {
            assert!((match_penalty(lookup, norm) - penalty).abs() < 1e-9, "{lookup} {norm}: {}", match_penalty(lookup, norm));
        }
    }

    #[test]
    fn n_grams_cross_no_phrase_break_and_span_at_most_twenty_nodes() {
        let g = graph("a b, c d", &MemTerms::default(), Lookup::Exact);

        let words: Vec<(String, Vec<(usize, usize)>)> = g.words().into_iter().map(|(w, s)| (w, s.iter().map(|s| (s.start, s.end)).collect())).collect();

        let want: Vec<(String, Vec<(usize, usize)>)> =
            [("a", (0, 1)), ("a b", (0, 2)), ("b", (1, 2)), ("c", (2, 3)), ("c d", (2, 4)), ("d", (3, 4))].iter().map(|&(w, s)| (w.to_string(), vec![s])).collect();
        assert_eq!(words, want);
        let long = graph(&(0..25).map(|i| format!("w{i}")).collect::<Vec<_>>().join(" "), &MemTerms::default(), Lookup::Exact);
        assert_eq!(long.words().iter().flat_map(|(_, s)| s).map(|s| s.len()).max(), Some(NGRAM_MAX_NODES));
        let repeated = graph("park park", &MemTerms::default(), Lookup::Exact);
        assert_eq!(repeated.words()[0].1, [Span::new(0, 1), Span::new(1, 2)]);
    }

    #[test]
    fn a_whole_word_of_four_digits_or_fewer_reads_as_a_house_number() {
        let terms = MemTerms::default();

        assert_eq!(of_type(&graph("zijldijk 8", &terms, Lookup::Exact), 'H'), ["1-2 8 8 0.200"]);
        assert_eq!(of_type(&graph("1044 liberty", &terms, Lookup::Exact), 'H'), ["0-1 1044 1044 0.200"]);
        assert!(of_type(&graph("12345 liberty", &terms, Lookup::Exact), 'H').is_empty());
        assert!(of_type(&graph("12-14", &terms, Lookup::Exact), 'H').is_empty(), "words joined by a hyphen are no whole words");
        assert!(of_type(&graph("8a", &terms, Lookup::Exact), 'H').is_empty());
        let mut known = MemTerms::default();
        known.insert(TermKind::HouseNumber, "8", ("", ""), (12, 1), TermInfo::None);
        assert_eq!(of_type(&graph("zijldijk 8", &known, Lookup::Exact), 'H'), ["1-2 8 8 0.000"]);
    }

    #[test]
    fn a_category_phrase_is_a_near_item_at_the_start_else_a_qualifier() {
        let terms = MemTerms::default();
        let categories = |q: &str| {
            let g = graph(q, &terms, Lookup::Exact);
            let mut found: Vec<String> = Vec::new();
            for (i, n) in g.nodes.iter().enumerate() {
                for list in n.starting.iter().filter(|l| matches!(l.ttype, TokenType::Qualifier | TokenType::NearItem)) {
                    for t in &list.tokens {
                        let TermInfo::Category(c) = &g.term(t.term).info else { panic!("a category term") };
                        found.push(format!("{} {i}-{} {}={}", list.ttype.letter(), list.end, c.class, c.typ));
                    }
                }
            }
            found
        };

        assert!(categories("cafe Austin").contains(&"Q 0-1 amenity=cafe".to_string()));
        assert!(categories("cafes in Austin").contains(&"N 0-2 amenity=cafe".to_string()));
        assert!(categories("cafes in Austin").contains(&"Q 0-1 amenity=cafe".to_string()));
        assert_eq!(categories("Hotel Austin"), ["Q 0-1 tourism=hotel"]);
        assert_eq!(categories("Hotel"), ["N 0-1 tourism=hotel"]);
        assert!(categories("cafe, Austin").is_empty(), "a qualifier never makes a whole phrase");
        assert!(!categories("Austin cafes in").iter().any(|c| c.starts_with('N')), "an 'in' phrase is a near item only at the start");
    }

    #[test]
    fn postcodes_come_from_the_parser_with_their_code_as_lookup_word() {
        let g = graph("2361 AB", &reading("postcode-nl").terms(), Lookup::Exact);

        let postcodes = of_type(&g, 'P');

        assert!(postcodes.contains(&"0-2 2361 ab 2361 AB 0.571".to_string()), "{postcodes:?}");
        assert!(postcodes.contains(&"0-1 2361 LV-2361 0.286".to_string()), "{postcodes:?}");
    }

    #[test]
    fn a_postcode_reading_costs_the_other_tokens_but_a_short_house_number() {
        let liberty = graph("1044 Liberty Park Drive", &reading("liberty").terms(), Lookup::Exact);
        let research = graph("12345 Research Blvd", &reading("postcode-hnr").terms(), Lookup::Exact);

        assert_eq!(of_type(&liberty, 'W')[0], "0-1 1044 1044 0.390");
        assert_eq!(of_type(&liberty, 'H'), ["0-1 1044 1044 0.000"]);
        // Once for each of the span's three types, the postcode's own among them.
        assert_eq!(of_type(&liberty, 'w')[0], "0-1 1044 1044 1.470");
        assert_eq!(of_type(&research, 'H'), ["0-1 12345 12345 0.390"]);
    }

    #[test]
    fn a_simple_house_number_reading_costs_the_other_tokens() {
        let g = graph("zijldijk 8a", &reading("zijldijk").terms(), Lookup::Exact);

        assert_eq!(of_type(&g, 'W'), ["0-1 zijldijk zijldijk 0.000", "1-2 8a 8a 0.400"]);
        assert_eq!(of_type(&g, 'w'), ["0-1 zijldijk zijldijk 0.300", "1-2 8a 8a 0.700"]);
        assert_eq!(of_type(&g, 'H'), ["1-2 8a 8 a 0.767"]);
    }

    /// "1044 Liberty Park Drive" as typed so far, and terms near it.
    fn typing() -> MemTerms {
        let mut terms = MemTerms::default();
        terms.insert(TermKind::Partial, "1044", ("", ""), (39, 100), TermInfo::None);
        terms.insert(TermKind::Partial, "10440", ("", ""), (3, 5), TermInfo::None);
        terms.insert(TermKind::HouseNumber, "1044", ("", ""), (16, 1), TermInfo::None);
        terms.insert(TermKind::HouseNumber, "10440", ("", ""), (2, 1), TermInfo::None);
        terms.insert(TermKind::Partial, "liberty", ("", ""), (1060, 10787), TermInfo::None);
        terms.insert(TermKind::Partial, "pa", ("", ""), (40, 30), TermInfo::None);
        terms.insert(TermKind::Partial, "park", ("", ""), (32610, 51438), TermInfo::None);
        terms.insert(TermKind::Word, "liberty park drive", ("", ""), (5, 2), TermInfo::None);
        terms.insert(TermKind::Partial, "austin", ("", ""), (2818, 56467), TermInfo::None);
        terms.insert(TermKind::Word, "austin", ("", ""), (16, 55241), TermInfo::None);
        terms
    }

    #[test]
    fn the_photon_mode_completes_the_last_word_alone() {
        let terms = typing();

        let g = graph("1044 liberty pa", &terms, Lookup::Prefix);

        assert_eq!(of_type(&g, 'H'), ["0-1 1044 1044 0.000"], "1044 is no prefix of 10440 here");
        assert_eq!(of_type(&g, 'W'), ["1-3 liberty park drive liberty park drive 0.056"]);
        let last = g.term(g.nodes[2].partial.unwrap().term);
        assert_eq!((last.token.as_str(), last.count, last.addr_count), ("pa", 40 + 32_610, 30 + 51_438), "the exact word and its completions");
        assert_eq!(of_type(&g, 'w')[2], "2-3 pa pa 0.300");
        let partial_1044 = g.term(g.nodes[0].partial.unwrap().term);
        assert_eq!(partial_1044.count, 39);
    }

    #[test]
    fn only_the_last_occurrence_adds_prefixes_to_its_typo_matches() {
        let mut terms = typing();
        terms.insert(TermKind::Partial, "austintown", ("", ""), (42, 1), TermInfo::None);
        let g = graph("austi austi", &terms, Lookup::Typo);
        let matched = |i: usize| {
            g.partial_matches[&g.partial(i).unwrap().term].iter().map(|m| g.term(m.term).token.as_str()).collect::<Vec<_>>()
        };
        assert_eq!(matched(0), ["austin"]);
        assert_eq!(matched(1), ["austin", "austintown"]);
    }

    #[test]
    fn an_exact_partial_and_its_completions_keep_their_own_match_costs() {
        use crate::profile::{Profile, SearchRequest};
        use crate::query::plan::{read, Pass};
        let req = SearchRequest::new("1044 liberty pa", Profile::Full);
        let plan = read(&req, &typing(), Pass::First).unwrap();
        let partial = plan.tokens.iter().find(|t| t.span.start == 2 && t.ttype == TokenType::Partial).unwrap();
        let members = &plan.partial_matches[&partial.term];
        let costs: Vec<_> = members.iter().map(|m| (plan.terms[m.term.0 as usize].token.as_str(), m.penalty)).collect();
        assert_eq!(costs, [("pa", 0.0), ("park", 0.25)]);
    }

    #[test]
    fn completing_a_number_does_not_extend_its_digits() {
        let mut terms = typing();
        terms.insert(TermKind::HouseNumber, "1044a", ("", ""), (5, 1), TermInfo::None);
        let g = graph("1044", &terms, Lookup::Prefix);
        assert_eq!(of_type(&g, 'H'), ["0-1 1044 1044 0.000", "0-1 1044a 1044a 0.500"]);
        assert_eq!(g.term(g.partial(0).unwrap().term).count, 39);
    }

    #[test]
    fn a_near_spelling_that_also_completes_the_query_is_one_token() {
        let g = graph("austi", &typing(), Lookup::Typo);
        assert_eq!(of_type(&g, 'W'), ["0-1 austin austin 0.167"]);
    }

    #[test]
    fn a_completion_alone_costs_its_match() {
        let g = graph("1044 liberty par", &typing(), Lookup::Prefix);

        assert_eq!(of_type(&g, 'w')[2], "2-3 par par 0.550");
    }

    #[test]
    fn the_nominatim_mode_has_no_completion_and_no_near_spelling() {
        let terms = typing();

        let g = graph("1044 liberty pa", &terms, Lookup::Exact);
        let typo = graph("futo austn", &terms, Lookup::Exact);

        assert!(of_type(&g, 'W').is_empty());
        assert_eq!(g.term(g.nodes[2].partial.unwrap().term).count, 40);
        assert!(typo.partial(1).is_none());
    }

    #[test]
    fn a_typo_reading_takes_near_spellings_of_words_but_numbers() {
        let terms = typing();

        let g = graph("futo austn", &terms, Lookup::Typo);
        let numbers = graph("1045 austin", &terms, Lookup::Typo);
        let short = graph("asn", &terms, Lookup::Typo);

        assert_eq!(of_type(&g, 'W'), ["1-2 austin austin 0.167"]);
        assert_eq!(of_type(&g, 'w'), ["0-1 futo futo 10.000", "1-2 austn austn 0.467"]);
        assert!(numbers.partial(0).is_none());
        assert!(short.partial(0).is_none(), "a query of three characters is read as typed");
    }
}
