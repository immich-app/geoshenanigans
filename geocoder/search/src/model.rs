//! What a search entity is, how its importance is made from the records, and the shared byte
//! formats (postings, extra names) of the index files.
use crate::constants::*;
use crate::varint;

/// Entity references: a places-index entity is a place node index (below `n_place_nodes`) or
/// `n_place_nodes` + a boundary slot; a POI-index entity is a POI record index with this bit set.
pub const POI_BIT: u32 = 1 << 31;

pub const PLACE_NODE_TYPES: [&str; 7] = ["city", "town", "village", "suburb", "hamlet", "neighbourhood", "quarter"];
pub use query_server::{PLACE_FLAG_LABEL_OF, POI_FLAG_SECONDARY_RING};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// visible place node of a settlement type
    Place,
    /// place node of a suburb / quarter / neighbourhood
    Minor,
    /// boundary that claims a place node (label point = the node)
    Linked,
    /// boundary without a label node (label point = its ring's point on surface)
    Area,
    Poi,
}

impl Kind {
    pub fn letter(self) -> char {
        match self {
            Kind::Place => 'P',
            Kind::Minor => 'M',
            Kind::Linked => 'B',
            Kind::Area => 'A',
            Kind::Poi => 'O',
        }
    }
}

pub fn place_node_kind(pt: u8) -> Kind {
    match pt {
        3 | 5 | 6 => Kind::Minor,
        _ => Kind::Place,
    }
}

/// Base of the type part of importance (prototype values).
pub fn type_base(kind: Kind, typ: &str, level: u8) -> f32 {
    if kind == Kind::Area {
        return match level {
            2 => 0.90,
            3 | 4 => 0.65,
            5 => 0.50,
            6 => 0.45,
            7 => 0.35,
            _ => 0.25,
        };
    }
    match typ {
        "country" => 0.90,
        "state" | "province" => 0.70,
        "region" => 0.60,
        "city" => 0.55,
        "county" => 0.45,
        "town" => 0.40,
        "municipality" | "district" | "archipelago" => 0.35,
        "island" | "borough" => 0.30,
        "village" => 0.25,
        "suburb" => 0.22,
        "quarter" => 0.16,
        "hamlet" => 0.12,
        "neighbourhood" | "locality" | "islet" => 0.10,
        _ => 0.25,
    }
}

pub fn extra_importance(byte: u8) -> f32 {
    byte as f32 / 255.0 * EXTRA_IMPORTANCE_MAX
}

/// AdminPolygon.area is in square degrees; km² at the ring's latitude.
pub fn area_proxy(area_deg2: f32, lat: f32) -> f32 {
    let km2 = area_deg2 as f64 * 111.32 * 111.32 * (lat as f64).to_radians().cos().abs();
    ((km2.max(0.0) + 1.0).log10() as f32 / AREA_LOG_FULL).clamp(0.0, 1.0)
}

pub fn poi_importance(builder_byte: u8, extra: u8) -> f32 {
    POI_IMPORTANCE_FLOOR
        + W_POI_BUILDER * ((1.0 + builder_byte as f32).log2() / 8.0).clamp(0.0, 1.0)
        + extra_importance(extra)
}

pub fn quantize(imp: f32) -> u8 {
    (imp.clamp(0.0, 1.27) * 200.0).round() as u8
}

pub fn dequantize(q: u8) -> f32 {
    q as f32 / 200.0
}

/// Class of a name tag key: name, name:en, a major language, alternates, old names, other.
pub fn class_of(key: &str) -> u8 {
    let (base, suf) = key.split_once(':').unwrap_or((key, ""));
    let lang = suf.split(['-', '_']).next().unwrap_or("");
    if !suf.is_empty() && suf != "en" && !MAJOR_LANGUAGES.contains(&lang) {
        return 5;
    }
    match base {
        "name" if suf.is_empty() => 0,
        "name" if suf == "en" => 1,
        "name" => 2,
        "old_name" => 4,
        _ => 3,
    }
}

/// Languages whose names count as class 2 (any other language is class 5).
pub const MAJOR_LANGUAGES: [&str; 41] = [
    "en", "de", "fr", "es", "it", "pt", "nl", "ru", "uk", "pl", "cs", "sk", "sv", "da", "no", "nb", "nn", "fi",
    "el", "tr", "ar", "he", "fa", "hi", "bn", "zh", "ja", "ko", "vi", "th", "id", "ms", "hu", "ro", "bg", "hr",
    "sr", "ca", "eu", "gl", "et",
];

// ------------------------------------------------------------------ postings

/// One entity under one key. `ord` 0 is the record's own name, k the k-th extra name of the
/// entity's records (`Entities::record_refs`: a labelled boundary has its label node's too).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Posting {
    pub entity: u32,
    pub imp_q: u8,
    pub partial: bool,
    pub class: u8,
    pub ord: u8,
}

impl Posting {
    fn class_ord(&self) -> u8 {
        (self.class << 4) | self.ord.min(MAX_NAME_ORD)
    }
}

/// A key's list, at the byte offset its fst value holds: varint count, then per posting a varint
/// importance (the first as itself, every later one as its drop from the one before), a varint
/// entity<<2|has_class_ordinal<<1|partial and an optional class/ordinal byte. The entity is
/// written as the step from the one before when the posting has the importance and the
/// trailing-word flag of the posting before it. `list` is in the order the build keeps it
/// (`Group::take`: importance down, whole names first, then entities up), so those steps are
/// positive.
pub fn encode_list(out: &mut Vec<u8>, list: &[Posting]) {
    varint::put(out, list.len() as u64);
    let mut prev: Option<Posting> = None;
    for p in list {
        let (imp, entity) = match prev {
            None => (p.imp_q, p.entity),
            Some(q) => {
                assert!(q.imp_q >= p.imp_q, "a list not best importance first");
                let run = q.imp_q == p.imp_q && q.partial == p.partial;
                assert!(!run || q.entity < p.entity, "a run of postings not in entity order");
                (q.imp_q - p.imp_q, if run { p.entity - q.entity } else { p.entity })
            }
        };
        varint::put(out, imp as u64);
        varint::put(out, ((entity as u64) << 2) | ((p.class_ord() != 0) as u64) << 1 | p.partial as u64);
        if p.class_ord() != 0 {
            out.push(p.class_ord());
        }
        prev = Some(*p);
    }
}

/// The postings of the list at `off` (`encode_list`). A list is read within `buf`: one cut short,
/// or holding a posting no build writes (an importance below 0 or over 255, an entity past 32
/// bits), ends before that posting; one at an offset past the end has none.
pub struct ListReader<'a> {
    buf: &'a [u8],
    pos: usize,
    left: u64,
    prev: Option<Posting>,
}

impl<'a> ListReader<'a> {
    pub fn new(buf: &'a [u8], off: usize) -> Self {
        let mut pos = off;
        let left = varint::try_get(buf, &mut pos).unwrap_or(0);
        ListReader { buf, pos, left, prev: None }
    }

    fn read(&mut self) -> Option<Posting> {
        let imp = varint::try_get(self.buf, &mut self.pos)?;
        let ep = varint::try_get(self.buf, &mut self.pos)?;
        let (partial, field) = (ep & 1 == 1, u32::try_from(ep >> 2).ok()?);
        let (imp_q, entity) = match self.prev {
            None => (u8::try_from(imp).ok()?, field),
            Some(q) => {
                let imp_q = q.imp_q.checked_sub(u8::try_from(imp).ok()?)?;
                let run = imp == 0 && q.partial == partial;
                (imp_q, if run { q.entity.checked_add(field)? } else { field })
            }
        };
        let co = if ep & 2 != 0 {
            let b = *self.buf.get(self.pos)?;
            self.pos += 1;
            b
        } else {
            0
        };
        Some(Posting { entity, imp_q, partial, class: co >> 4, ord: co & 0x0F })
    }
}

impl Iterator for ListReader<'_> {
    type Item = Posting;
    fn next(&mut self) -> Option<Posting> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let p = self.read();
        // A posting that does not read ends the list.
        self.left = if p.is_some() { self.left } else { 0 };
        self.prev = p;
        p
    }
}

/// The best importance of the list at `off`: its first posting's. None for a list without one
/// that reads.
pub fn list_top_imp(buf: &[u8], off: usize) -> Option<u8> {
    let mut pos = off;
    if varint::try_get(buf, &mut pos)? == 0 {
        return None;
    }
    u8::try_from(varint::try_get(buf, &mut pos)?).ok()
}

/// Prefix-table hit: entity<<2|has_class_ordinal<<1|partial, the text score as a byte, the importance byte, and an
/// optional class/ordinal byte.
#[derive(Clone, Copy, Debug)]
pub struct PrefixHit {
    pub entity: u32,
    pub partial: bool,
    pub text: f32,
    pub imp_q: u8,
    pub class: u8,
    pub ord: u8,
}

pub fn encode_prefix_hits(out: &mut Vec<u8>, hits: &[PrefixHit]) {
    varint::put(out, hits.len() as u64);
    for h in hits {
        let co = (h.class << 4) | h.ord.min(MAX_NAME_ORD);
        varint::put(out, ((h.entity as u64) << 2) | ((co != 0) as u64) << 1 | h.partial as u64);
        out.push((h.text.clamp(0.0, 1.0) * 255.0).round() as u8);
        out.push(h.imp_q);
        if co != 0 {
            out.push(co);
        }
    }
}

/// The hits of the prefix-table list at `off` (`encode_prefix_hits`), read within `buf`: those
/// before a hit cut short or of an entity past 32 bits; none for a list of more hits than the
/// table keeps (PREFIX_TABLE_TOP), which no build writes, or at an offset past the end.
pub fn decode_prefix_hits(buf: &[u8], off: usize, out: &mut Vec<PrefixHit>) {
    let mut pos = off;
    let Some(n) = varint::try_get(buf, &mut pos).filter(|&n| n <= PREFIX_TABLE_TOP as u64) else { return };
    for _ in 0..n {
        let Some(h) = read_prefix_hit(buf, &mut pos) else { return };
        out.push(h);
    }
}

fn read_prefix_hit(buf: &[u8], pos: &mut usize) -> Option<PrefixHit> {
    let ep = varint::try_get(buf, pos)?;
    let entity = u32::try_from(ep >> 2).ok()?;
    let (text, imp_q) = (*buf.get(*pos)?, *buf.get(*pos + 1)?);
    *pos += 2;
    let co = if ep & 2 != 0 {
        let b = *buf.get(*pos)?;
        *pos += 1;
        b
    } else {
        0
    };
    Some(PrefixHit { entity, partial: ep & 1 == 1, text: text as f32 / 255.0, imp_q, class: co >> 4, ord: co & 0x0F })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes_by_key_and_language() {
        assert_eq!(class_of("name"), 0);
        assert_eq!(class_of("name:en"), 1);
        assert_eq!(class_of("name:de"), 2);
        assert_eq!(class_of("name:zh-Hans"), 2);
        assert_eq!(class_of("alt_name"), 3);
        assert_eq!(class_of("old_name"), 4);
        assert_eq!(class_of("name:tg"), 5);
    }

    fn post(entity: u32, imp_q: u8, partial: bool, class: u8, ord: u8) -> Posting {
        Posting { entity, imp_q, partial, class, ord }
    }

    /// A list in the order the build keeps it (importance down, whole names first, entities up),
    /// with runs, a change of flag, class/ordinal bytes and a first importance of two bytes.
    fn sample_list() -> Vec<Posting> {
        vec![
            post(5_000_000, 200, false, 0, 0),
            post(5_000_003, 200, false, 2, 3),
            post(7, 200, true, 0, 0),
            post(123_456, 4, false, 5, 15),
            post(123_457, 4, false, 0, 0),
        ]
    }

    #[test]
    fn a_list_is_its_importances_and_its_entities_as_steps_within_a_run() {
        let list = [post(5, 40, false, 0, 0), post(9, 40, false, 0, 0), post(3, 40, true, 0, 0), post(8, 40, true, 2, 1), post(2, 12, false, 0, 0)];
        let mut b = Vec::new();

        encode_list(&mut b, &list);

        // count; then per posting the importance (the first as itself, then its drop), the
        // entity (a step after a posting of the same importance and flag) << 2 | class/ordinal
        // byte follows << 1 | trailing word, and that byte.
        assert_eq!(b, [5, 40, 5 << 2, 0, 4 << 2, 0, 3 << 2 | 1, 0, 5 << 2 | 2 | 1, 0x21, 28, 2 << 2]);
    }

    #[test]
    fn lists_round_trip() {
        let list = sample_list();
        let mut b = vec![0xAA];
        encode_list(&mut b, &list);

        let got: Vec<Posting> = ListReader::new(&b, 1).collect();

        assert_eq!(got, list);
    }

    #[test]
    fn a_damaged_list_reads_without_a_panic() {
        let list = sample_list();
        let mut whole = Vec::new();
        encode_list(&mut whole, &list);
        // Cut anywhere: the postings before the cut.
        for cut in 0..whole.len() {
            let got: Vec<Posting> = ListReader::new(&whole[..cut], 0).collect();
            assert_eq!(got[..], list[..got.len()], "cut at {cut}");
        }
        // Any byte replaced, or read from past its end: no panic, no more postings than it counts.
        for at in 0..whole.len() {
            for b in [0x00, 0x01, 0x7F, 0x80, 0xFF] {
                let mut bad = whole.clone();
                bad[at] = b;
                assert!(ListReader::new(&bad, 0).count() <= whole.len() / 2, "byte {at} = {b:#x}");
            }
        }
        assert_eq!(ListReader::new(&whole, whole.len() + 10).count(), 0);
        assert_eq!(ListReader::new(&[0xFF; 16], 0).count(), 0);
    }

    #[test]
    fn a_list_ends_at_a_posting_no_build_writes() {
        let varint = |v: u64| {
            let mut b = Vec::new();
            varint::put(&mut b, v);
            b
        };
        let cases: [(&str, Vec<u8>, usize); 4] = [
            ("an importance dropping below zero", [&[2, 10, 5 << 2, 11, 6 << 2][..]].concat(), 1),
            ("a first importance over 255", [&[1][..], &varint(256), &[5 << 2]].concat(), 0),
            ("an entity over 32 bits", [&[1, 10][..], &varint(1 << 34)].concat(), 0),
            ("an entity step past 32 bits", [&[2, 10][..], &varint((u32::MAX as u64) << 2), &[0, 1 << 2]].concat(), 1),
        ];
        for (case, bytes, n) in cases {
            assert_eq!(ListReader::new(&bytes, 0).count(), n, "{case}");
        }
    }
    #[test]
    fn prefix_hits_keep_name_metadata_without_extras() {
        let hits = [
            PrefixHit { entity: 10, partial: false, text: 1.0, imp_q: 255, class: 3, ord: 0 },
            PrefixHit { entity: 20, partial: true, text: 1.0, imp_q: 200, class: 0, ord: 14 },
            PrefixHit { entity: u32::MAX, partial: false, text: 1.0, imp_q: 100, class: 0, ord: 0 },
        ];
        let mut blob = vec![0xAA];
        encode_prefix_hits(&mut blob, &hits);
        let mut got = Vec::new();
        decode_prefix_hits(&blob, 1, &mut got);
        assert_eq!(got.len(), hits.len());
        for (g, h) in got.iter().zip(hits) {
            assert_eq!((g.entity, g.partial, g.class, g.ord), (h.entity, h.partial, h.class, h.ord));
            assert_eq!((g.text, g.imp_q), (h.text, h.imp_q));
        }
    }

    #[test]
    fn a_damaged_prefix_list_reads_without_a_panic() {
        let hits = [
            PrefixHit { entity: 10, partial: false, text: 1.0, imp_q: 255, class: 3, ord: 0 },
            PrefixHit { entity: 20, partial: true, text: 0.5, imp_q: 200, class: 0, ord: 14 },
            PrefixHit { entity: 1 << 20, partial: false, text: 1.0, imp_q: 100, class: 0, ord: 0 },
        ];
        let mut whole = Vec::new();
        encode_prefix_hits(&mut whole, &hits);
        let decode = |buf: &[u8], off: usize| {
            let mut got = Vec::new();
            decode_prefix_hits(buf, off, &mut got);
            got.iter().map(|h| h.entity).collect::<Vec<_>>()
        };
        // Cut anywhere: the hits that end at or before the cut. Each hit ends where a list of it
        // and those before it would end (its count takes one byte too).
        let ends: Vec<usize> = (1..=hits.len())
            .map(|k| {
                let mut b = Vec::new();
                encode_prefix_hits(&mut b, &hits[..k]);
                b.len()
            })
            .collect();
        for cut in 0..=whole.len() {
            let got = decode(&whole[..cut], 0);
            let want: Vec<u32> = hits.iter().zip(&ends).filter(|&(_, &end)| end <= cut).map(|(h, _)| h.entity).collect();
            assert_eq!(got, want, "cut at {cut}");
        }
        // Any byte replaced: no panic, no more hits than the table keeps.
        for at in 0..whole.len() {
            for b in [0x00, 0x01, 0x7F, 0x80, 0xFF] {
                let mut bad = whole.clone();
                bad[at] = b;
                assert!(decode(&bad, 0).len() <= PREFIX_TABLE_TOP, "byte {at} = {b:#x}");
            }
        }
        let varint = |v: u64| {
            let mut b = Vec::new();
            varint::put(&mut b, v);
            b
        };
        let hit = [&varint(7 << 2)[..], &[255, 200]].concat();
        let cases: [(&str, Vec<u8>, usize); 4] = [
            ("an offset past the end", whole.clone(), whole.len() + 10),
            ("a count over 64 bits", vec![0x80; 11], 0),
            ("more hits than the table keeps", [varint(PREFIX_TABLE_TOP as u64 + 1), hit.repeat(PREFIX_TABLE_TOP + 1)].concat(), 0),
            ("an entity past 32 bits", [&[1][..], &varint((1 << 34) << 2), &[255, 200]].concat(), 0),
        ];
        for (case, bytes, off) in cases {
            assert_eq!(decode(&bytes, off), [0u32; 0], "{case}");
        }
    }
}
