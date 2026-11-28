//! The readings of a query: which of its tokens make the name, the address, the house number, the
//! postcode, the country, the near item and the qualifier, and what each reading costs, in the
//! order Nominatim finds them (search/token_assignment.py).
use super::lookup::{QueryGraph, Span, TokenType};
use crate::constants::*;
use crate::text::Break;

/// A reading of the query (TokenAssignment).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Assignment {
    pub penalty: f64,
    pub name: Option<Span>,
    pub address: Vec<Span>,
    pub housenumber: Option<Span>,
    pub postcode: Option<Span>,
    pub country: Option<Span>,
    pub near_item: Option<Span>,
    pub qualifier: Option<Span>,
}

impl Assignment {
    fn of(ranges: &[(TokenType, Span)]) -> Assignment {
        let mut out = Assignment::default();
        for &(ttype, span) in ranges {
            match ttype {
                TokenType::Partial => out.address.push(span),
                TokenType::HouseNumber => out.housenumber = Some(span),
                TokenType::Postcode => out.postcode = Some(span),
                TokenType::Country => out.country = Some(span),
                TokenType::NearItem => out.near_item = Some(span),
                TokenType::Qualifier => out.qualifier = Some(span),
                TokenType::Word => {}
            }
        }
        out
    }
}

/// Which way a reading goes: the name first and the address after it, or the other way round.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    Unknown,
    LeftToRight,
    RightToLeft,
}

/// A reading being put together: its typed ranges so far (_TokenSequence).
#[derive(Clone, Debug)]
struct Sequence {
    ranges: Vec<(TokenType, Span)>,
    direction: Direction,
    penalty: f64,
}

impl Sequence {
    fn end(&self) -> usize {
        self.ranges.last().map_or(0, |(_, s)| s.end)
    }

    fn has(&self, types: &[TokenType]) -> bool {
        self.ranges.iter().any(|(t, _)| types.contains(t))
    }

    /// Whether nothing may follow: a country or a near item ends a reading left to right.
    fn is_final(&self) -> bool {
        self.ranges.len() > 1 && matches!(self.ranges.last(), Some((TokenType::Country | TokenType::NearItem, _)))
    }

    /// The reading's direction with `ttype` after its ranges; None where `ttype` cannot follow
    /// them (token_assignment.py:100-182).
    fn appendable(&self, ttype: TokenType) -> Option<Direction> {
        use Direction::*;
        use TokenType::*;
        if ttype == Word {
            return None;
        }
        let Some(&(first, _)) = self.ranges.first() else {
            return Some(match ttype {
                Country => RightToLeft,
                HouseNumber | Qualifier => LeftToRight,
                _ => self.direction,
            });
        };
        if ttype == Partial {
            // A qualifier cannot stand in the middle of the query.
            let early_qualifier = self.ranges[..self.ranges.len() - 1].iter().any(|(t, _)| *t == Qualifier);
            return (self.direction != RightToLeft || !early_qualifier).then_some(self.direction);
        }
        if self.has(&[ttype]) {
            return None;
        }
        let len = self.ranges.len();
        match ttype {
            HouseNumber => match self.direction {
                LeftToRight if (len == 1 && first == Qualifier) || len > 2 || self.has(&[Postcode, Country]) => None,
                LeftToRight => Some(LeftToRight),
                _ if self.direction == RightToLeft || self.has(&[Postcode, Country]) => Some(RightToLeft),
                _ => Some(self.direction),
            },
            Postcode => match self.direction {
                RightToLeft => (!self.has(&[HouseNumber, Qualifier])).then_some(RightToLeft),
                LeftToRight => (!self.has(&[Country])).then_some(LeftToRight),
                Unknown if self.has(&[HouseNumber, Qualifier]) => Some(LeftToRight),
                Unknown => Some(Unknown),
            },
            Country => (self.direction != RightToLeft).then_some(LeftToRight),
            NearItem => Some(self.direction),
            Qualifier => match self.direction {
                LeftToRight => {
                    let after_name = (len == 1 && matches!(first, Partial | NearItem)) || (len == 2 && first == NearItem && self.ranges[1].0 == Partial);
                    after_name.then_some(LeftToRight)
                }
                RightToLeft => Some(RightToLeft),
                Unknown => {
                    let rest = if first == NearItem { &self.ranges[1..] } else { &self.ranges[..] };
                    if rest.is_empty() {
                        Some(LeftToRight)
                    } else if rest.len() == 1 && first == HouseNumber {
                        None
                    } else if rest.len() > 1 || self.has(&[Postcode, Country]) {
                        Some(RightToLeft)
                    } else {
                        Some(Unknown)
                    }
                }
            },
            Word | Partial => None,
        }
    }

    /// The sequence with `ttype` up to node `end` after it: the last range grown where it has the
    /// same type and no break is forced, else a new range that pays the break (:184-208).
    fn advance(&self, ttype: TokenType, end: usize, force_break: bool, break_penalty: f64) -> Option<Sequence> {
        let direction = self.appendable(ttype)?;
        let mut ranges = self.ranges.clone();
        let penalty = match ranges.last_mut() {
            None => {
                ranges.push((ttype, Span::new(0, end)));
                0.0
            }
            Some((last, span)) if !force_break && *last == ttype => {
                span.end = end;
                0.0
            }
            Some(&mut (_, span)) => {
                ranges.push((ttype, Span::new(span.end, end)));
                break_penalty
            }
        };
        Some(Sequence { ranges, direction, penalty: self.penalty + penalty })
    }

    /// Whether the complete sequence is a reading: a house number has at most
    /// HOUSENUMBER_PRIORS_MAX partial ranges on the side the name goes, the last of them costing
    /// or settling the direction; with a near item it costs more (:210-247).
    fn recheck(&mut self) -> bool {
        let Some(at) = self.ranges.iter().position(|(t, _)| *t == TokenType::HouseNumber) else { return true };
        let partials = |ranges: &[(TokenType, Span)]| ranges.iter().filter(|(t, _)| *t == TokenType::Partial).count();
        if self.direction != Direction::RightToLeft && !self.adapt(partials(&self.ranges[..at]), Direction::RightToLeft) {
            return false;
        }
        if self.direction != Direction::LeftToRight && !self.adapt(partials(&self.ranges[at + 1..]), Direction::LeftToRight) {
            return false;
        }
        if self.has(&[TokenType::NearItem]) {
            self.penalty += HOUSENUMBER_NEAR_ITEM_PENALTY;
        }
        true
    }

    fn adapt(&mut self, priors: usize, direction: Direction) -> bool {
        if priors < HOUSENUMBER_PRIORS_MAX {
            return true;
        }

        if self.direction == Direction::Unknown {
            self.direction = direction;
        } else if priors == HOUSENUMBER_PRIORS_MAX {
            self.penalty += HOUSENUMBER_TWO_PRIORS_PENALTY;
        } else {
            return false;
        }
        true
    }

    /// The readings of a complete sequence: its address split into name and address every way
    /// its direction allows, a postcode or country search, and a house-number search (:357-399).
    fn readings(&self, g: &QueryGraph, dir_penalty: f64, out: &mut Vec<Assignment>) {
        let base = Assignment::of(&self.ranges);
        let mut penalty = self.penalty;
        if let (Some(postcode), false) = (base.postcode, base.address.is_empty()) {
            self.postcode_reading(&base, postcode, g.slots(), out);
        }
        if base.address.is_empty() {
            if base.housenumber.is_none() && (base.postcode.is_some() || base.country.is_some() || base.near_item.is_some()) {
                out.push(Assignment { penalty, ..base });
            }
            return;
        }

        if base.postcode.is_some_and(|p| p.start == 0) {
            penalty += POSTCODE_FIRST_PENALTY;
        }
        let first = out.len();
        if self.direction != Direction::RightToLeft {
            self.forward(&base, penalty, g, dir_penalty, out);
        }
        if self.direction != Direction::LeftToRight {
            self.backward(&base, penalty, g, dir_penalty, out);
        }
        if base.housenumber.is_some() && base.qualifier.is_none() {
            let best = out[first..].iter().map(|a| a.penalty).fold(penalty + HOUSENUMBER_READING_SPAN, f64::min);
            out.push(Assignment { penalty: best + HOUSENUMBER_READING_PENALTY, ..base });
        }
    }

    /// A postcode search of the address, where the postcode starts or ends the query as the
    /// direction allows (:249-265).
    fn postcode_reading(&self, base: &Assignment, postcode: Span, slots: usize, out: &mut Vec<Assignment>) {
        if !((postcode.start == 0 && self.direction != Direction::RightToLeft) || (postcode.end == slots && self.direction != Direction::LeftToRight)) {
            return;
        }

        let late = if postcode.start == 0 { 0.0 } else { POSTCODE_LATE_PENALTY };
        let extra = POSTCODE_EXTRA_ADDRESS_PENALTY * base.address.len().saturating_sub(1) as f64;
        out.push(Assignment { penalty: self.penalty + late + extra, ..base.clone() });
    }

    /// Left to right: the first range is the name, or the first part of it (:267-310).
    fn forward(&self, base: &Assignment, mut penalty: f64, g: &QueryGraph, dir_penalty: f64, out: &mut Vec<Assignment>) {
        let first = base.address[0];
        if base.postcode.is_some_and(|p| p.before(first)) {
            return;
        }

        if base.country.is_none() && self.direction == Direction::LeftToRight && dir_penalty > 0.0 {
            penalty += dir_penalty;
        }
        out.push(Assignment { penalty, name: Some(first), address: base.address[1..].to_vec(), ..base.clone() });
        let housenumber_after_name = base.housenumber.is_some_and(|h| first.end < h.start);
        if housenumber_after_name || base.qualifier.is_some_and(|q| q.after(first)) {
            return;
        }
        if base.housenumber.is_some_and(|h| h.after(first)) || g.phrases > 1 {
            penalty += SPLIT_PENALTY;
        }
        if self.direction == Direction::Unknown && dir_penalty > 0.0 {
            penalty += dir_penalty;
        }
        for i in first.start + 1..first.end {
            let mut address = vec![Span::new(i, first.end)];
            address.extend_from_slice(&base.address[1..]);
            let split = penalty + g.nodes[i].node.word_break_penalty();
            out.push(Assignment { penalty: split, name: Some(Span::new(first.start, i)), address, ..base.clone() });
        }
    }

    /// Right to left: the last range is the name, or the last part of it (:312-355).
    fn backward(&self, base: &Assignment, mut penalty: f64, g: &QueryGraph, dir_penalty: f64, out: &mut Vec<Assignment>) {
        let last = *base.address.last().expect("an address");
        if base.postcode.is_some_and(|p| p.after(last)) {
            return;
        }

        if base.country.is_none() && self.direction == Direction::RightToLeft && dir_penalty < 0.0 {
            penalty -= dir_penalty;
        }
        let rest = &base.address[..base.address.len() - 1];
        if self.direction == Direction::RightToLeft || base.address.len() > 1 || base.postcode.is_some() {
            out.push(Assignment { penalty, name: Some(last), address: rest.to_vec(), ..base.clone() });
        }
        let housenumber_before_name = base.housenumber.is_some_and(|h| last.start > h.end);
        if housenumber_before_name || base.qualifier.is_some_and(|q| q.before(last)) {
            return;
        }
        if base.housenumber.is_some_and(|h| h.before(last)) {
            penalty += SPLIT_AFTER_HOUSENUMBER_PENALTY;
        }
        if g.phrases > 1 {
            penalty += SPLIT_PENALTY;
        }
        if self.direction == Direction::Unknown && dir_penalty < 0.0 {
            penalty -= dir_penalty;
        }
        for i in last.start + 1..last.end {
            let mut address = rest.to_vec();
            address.push(Span::new(last.start, i));
            let split = penalty + g.nodes[i].node.word_break_penalty();
            out.push(Assignment { penalty: split, name: Some(Span::new(i, last.end)), address, ..base.clone() });
        }
    }
}

/// Every reading of the query, in the order Nominatim's walk of its token graph finds them
/// (token_assignment.py:402-440).
pub fn assignments(g: &QueryGraph) -> Vec<Assignment> {
    let mut out = Vec::new();
    let slots = g.slots();
    if slots == 0 {
        return out;
    }

    let dir_penalty = g.direction_penalty();
    let mut todo = vec![Sequence { ranges: Vec::new(), direction: Direction::Unknown, penalty: 0.0 }];
    while let Some(state) = todo.pop() {
        let at = state.end();
        let node = &g.nodes[at];
        let break_penalty = node.node.word_break_penalty();
        let mut next: Vec<Sequence> = node.starting.iter().filter_map(|l| state.advance(l.ttype, l.end, true, break_penalty)).collect();
        if node.node.btype != Break::End && g.partial(at).is_some() {
            next.extend(state.advance(TokenType::Partial, at + 1, node.node.btype == Break::Phrase, break_penalty));
        }
        for mut seq in next {
            if seq.end() == slots {
                if seq.recheck() {
                    seq.readings(g, dir_penalty, &mut out);
                }
            } else if !seq.is_final() {
                todo.push(seq);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::terms::{MemTerms, TermInfo, TermKind};
    use crate::query::lookup::{tokens, Lookup};
    use crate::query::readings::reading;
    use crate::query::tokenize::split;

    fn graph(q: &str, terms: &MemTerms) -> QueryGraph {
        tokens(split(q).unwrap(), terms, Lookup::Exact)
    }

    /// Each reading: its penalty to 0.0001, then its typed spans.
    fn written(g: &QueryGraph) -> Vec<String> {
        let span = |s: Span| format!("{}-{}", s.start, s.end);
        assignments(g)
            .iter()
            .map(|a| {
                let mut parts = vec![format!("{:.4}", a.penalty)];
                parts.extend(a.name.map(|s| format!("name {}", span(s))));
                parts.extend(a.address.iter().map(|&s| format!("addr {}", span(s))));
                parts.extend(a.housenumber.map(|s| format!("hnr {}", span(s))));
                parts.extend(a.postcode.map(|s| format!("pc {}", span(s))));
                parts.extend(a.country.map(|s| format!("cc {}", span(s))));
                parts.extend(a.near_item.map(|s| format!("near {}", span(s))));
                parts.extend(a.qualifier.map(|s| format!("qual {}", span(s))));
                parts.join(" ")
            })
            .collect()
    }

    fn partials(words: &[&str]) -> MemTerms {
        let mut terms = MemTerms::default();
        for w in words {
            terms.insert(TermKind::Partial, w, ("", ""), (100, 100), TermInfo::None);
        }
        terms
    }

    #[test]
    fn a_name_and_its_address_split_every_way_the_direction_allows() {
        let r = reading("futo");

        let readings = written(&graph(&r.q, &r.terms()));

        // Left to right by the counts, so the split that names the last word pays the direction.
        assert_eq!(readings, ["0.0000 name 0-2", "0.1000 name 0-1 addr 1-2", "0.2508 name 1-2 addr 0-1"]);
    }

    #[test]
    fn a_house_number_reading_comes_first_with_its_house_number_search() {
        let r = reading("zijldijk");

        let readings = written(&graph(&r.q, &r.terms()));

        assert_eq!(readings, ["0.1000 name 0-1 hnr 1-2", "0.2000 addr 0-1 hnr 1-2", "0.0000 name 0-2", "0.1446 name 0-1 addr 1-2", "0.1000 name 1-2 addr 0-1"]);
    }

    #[test]
    fn phrases_split_the_address_and_cost_a_split() {
        let readings = written(&graph("Austin, TX", &reading("austin-tx").terms()));

        assert_eq!(readings, ["0.0000 name 0-1 addr 1-2", "0.0000 name 1-2 addr 0-1"]);
        assert_eq!(written(&graph("a b, c", &partials(&["a", "b", "c"])))[..2], ["0.0000 name 0-2 addr 2-3", "0.3500 name 0-1 addr 1-2 addr 2-3"]);
    }

    #[test]
    fn a_country_ends_a_reading_left_to_right() {
        let r = reading("brno");

        let readings = written(&graph(&r.q, &r.terms()));

        assert_eq!(readings, ["0.0000 name 0-1 cc 1-2", "0.0000 name 0-1 addr 1-2", "0.0000 name 1-2 addr 0-1"]);
    }

    #[test]
    fn a_postcode_reads_alone_or_with_the_address() {
        let pc = reading("postcode-nl");
        let research = reading("postcode-hnr");

        let nl = written(&graph(&pc.q, &pc.terms()));
        let hnr = written(&graph(&research.q, &research.terms()));

        assert!(nl.contains(&"0.0000 pc 0-2".to_string()), "{nl:?}");
        assert!(nl.contains(&"0.1000 addr 1-2 pc 0-1".to_string()), "{nl:?}");
        assert!(nl.contains(&"0.2000 name 1-2 pc 0-1".to_string()), "a postcode first costs the address readings: {nl:?}");
        assert!(hnr.contains(&"0.1860 name 1-3 hnr 0-1".to_string()), "right to left by the counts, so the reading left to right pays the direction: {hnr:?}");
        assert!(written(&graph("78746", &MemTerms::default())).contains(&"0.0000 pc 0-1".to_string()));
    }

    #[test]
    fn two_partial_ranges_before_a_house_number_settle_the_direction() {
        let readings = written(&graph("a, b, 12", &partials(&["a", "b"])));

        assert_eq!(readings, ["0.0000 name 1-2 addr 0-1 hnr 2-3", "0.1000 addr 0-1 addr 1-2 hnr 2-3"]);
    }

    #[test]
    fn a_house_number_takes_at_most_two_partial_ranges_on_the_names_side() {
        let mut terms = partials(&["a", "b", "c"]);
        terms.insert(TermKind::Country, "de", ("", ""), (1, 1), TermInfo::Country(*b"de"));

        let two = written(&graph("de, 12, a, b", &terms));
        let three = written(&graph("de, 12, a, b, c", &terms));

        assert_eq!(two, ["0.8000 name 3-4 addr 2-3 hnr 1-2 cc 0-1", "0.9000 addr 2-3 addr 3-4 hnr 1-2 cc 0-1"]);
        assert!(three.is_empty(), "{three:?}");
    }

    #[test]
    fn categories_read_as_qualifiers_and_near_items() {
        let cafe = written(&graph("cafe Austin", &reading("cafe").terms()));
        let cafes_in = written(&graph("cafes in Austin", &reading("cafes-in").terms()));

        assert!(cafe.contains(&"0.1000 name 1-2 qual 0-1".to_string()), "{cafe:?}");
        assert!(cafes_in.contains(&"0.1000 name 2-3 near 0-2".to_string()), "{cafes_in:?}");
        assert!(cafes_in.contains(&"0.1000 name 1-3 qual 0-1".to_string()), "{cafes_in:?}");
    }

    #[test]
    fn a_query_of_no_slot_has_no_reading() {
        assert!(assignments(&graph("", &MemTerms::default())).is_empty());
    }
}
