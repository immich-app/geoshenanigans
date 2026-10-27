//! What a search entity is, how its importance is made from the records, and the shared byte
//! formats (postings, extra names) of the index files.
use crate::constants::*;
use crate::varint;

/// Entity references: a places-index entity is a place node index (below `n_place_nodes`) or
/// `n_place_nodes` + a boundary slot; a POI-index entity is a POI record index with this bit set.
pub const POI_BIT: u32 = 1 << 31;

pub const PLACE_NODE_TYPES: [&str; 7] = ["city", "town", "village", "suburb", "hamlet", "neighbourhood", "quarter"];
/// PlaceNode._pad1 bit: parent_poly_id is the boundary that claims (hides) this node.
pub const PLACE_FLAG_LABEL_OF: u8 = 0x01;
/// PlaceNode._pad1 bit: a node a boundary claims; hidden from reverse geocoding, but it keeps its
/// name_id (the boundary's label name for search).
pub const PLACE_FLAG_HIDDEN: u8 = 0x02;

pub fn is_hidden(pn: &query_server::PlaceNode) -> bool {
    pn.name_id == query_server::NO_DATA || pn._pad1 & PLACE_FLAG_HIDDEN != 0
}
/// AdminPolygon._pad2 bit: the boundary's largest ring (its one search entity).
pub const POLY_FLAG_PRIMARY_RING: u8 = 0x01;
/// PoiRecord.flags bit: a secondary ring of a multi-ring POI (not searchable on its own).
pub const POI_FLAG_SECONDARY_RING: u8 = 0x08;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// visible place node of a settlement type
    Place,
    /// place node of a suburb / quarter / neighbourhood
    Minor,
    /// boundary that claims a place node (label point = the node)
    Linked,
    /// boundary without a label node (label point = its largest ring's centroid)
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

/// One entity under one key. `ord` 0 is the record's own name, k the k-th extra name.
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

/// fst value: bit 0 set = the key's single posting inline, else the byte offset of its list.
pub fn inline_value(p: &Posting) -> u64 {
    1 | ((p.imp_q as u64) << 1) | ((p.partial as u64) << 9) | ((p.class_ord() as u64) << 10) | ((p.entity as u64) << 18)
}

pub fn decode_inline(v: u64) -> Posting {
    let co = ((v >> 10) & 0xFF) as u8;
    Posting {
        entity: (v >> 18) as u32,
        imp_q: ((v >> 1) & 0xFF) as u8,
        partial: (v >> 9) & 1 == 1,
        class: co >> 4,
        ord: co & 0x0F,
    }
}

/// fst value of a key with a posting list: the list's byte offset and its best importance (so
/// ranking keys never touches the postings file).
pub fn list_value(offset: u64, top_imp: u8) -> u64 {
    (offset << 9) | ((top_imp as u64) << 1)
}

pub fn list_offset(v: u64) -> usize {
    (v >> 9) as usize
}

pub fn value_top_imp(v: u64) -> u8 {
    ((v >> 1) & 0xFF) as u8
}

/// A key's list: count, then per posting entity<<1|partial, the importance drop from the
/// previous posting (lists are best importance first), and with extra names a class/ordinal byte.
pub fn encode_list(out: &mut Vec<u8>, list: &[Posting], with_names: bool) {
    varint::put(out, list.len() as u64);
    let mut prev = u8::MAX;
    for p in list {
        varint::put(out, ((p.entity as u64) << 1) | p.partial as u64);
        varint::put(out, (prev - p.imp_q) as u64);
        prev = p.imp_q;
        if with_names {
            out.push(p.class_ord());
        }
    }
}

pub struct ListReader<'a> {
    buf: &'a [u8],
    pos: usize,
    left: usize,
    prev: u8,
    with_names: bool,
}

impl<'a> ListReader<'a> {
    pub fn new(buf: &'a [u8], off: usize, with_names: bool) -> Self {
        let mut pos = off;
        let left = varint::get(buf, &mut pos) as usize;
        ListReader { buf, pos, left, prev: u8::MAX, with_names }
    }
}

impl Iterator for ListReader<'_> {
    type Item = Posting;
    fn next(&mut self) -> Option<Posting> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let ep = varint::get(self.buf, &mut self.pos);
        let drop = varint::get(self.buf, &mut self.pos) as u8;
        self.prev -= drop;
        let co = if self.with_names {
            self.pos += 1;
            self.buf[self.pos - 1]
        } else {
            0
        };
        Some(Posting { entity: (ep >> 1) as u32, imp_q: self.prev, partial: ep & 1 == 1, class: co >> 4, ord: co & 0x0F })
    }
}

/// Prefix-table hit: entity<<1|partial, the text score as a byte, the importance byte, and with
/// extra names the class/ordinal byte.
#[derive(Clone, Copy, Debug)]
pub struct PrefixHit {
    pub entity: u32,
    pub partial: bool,
    pub text: f32,
    pub imp_q: u8,
    pub class: u8,
    pub ord: u8,
}

pub fn encode_prefix_hits(out: &mut Vec<u8>, hits: &[PrefixHit], with_names: bool) {
    varint::put(out, hits.len() as u64);
    for h in hits {
        varint::put(out, ((h.entity as u64) << 1) | h.partial as u64);
        out.push((h.text.clamp(0.0, 1.0) * 255.0).round() as u8);
        out.push(h.imp_q);
        if with_names {
            out.push((h.class << 4) | h.ord.min(MAX_NAME_ORD));
        }
    }
}

pub fn decode_prefix_hits(buf: &[u8], off: usize, with_names: bool, out: &mut Vec<PrefixHit>) {
    let mut pos = off;
    let n = varint::get(buf, &mut pos) as usize;
    for _ in 0..n {
        let ep = varint::get(buf, &mut pos);
        let text = buf[pos] as f32 / 255.0;
        let imp_q = buf[pos + 1];
        pos += 2;
        let co = if with_names {
            pos += 1;
            buf[pos - 1]
        } else {
            0
        };
        out.push(PrefixHit { entity: (ep >> 1) as u32, partial: ep & 1 == 1, text, imp_q, class: co >> 4, ord: co & 0x0F });
    }
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

    #[test]
    fn lists_round_trip() {
        let list = [
            Posting { entity: 5_000_000, imp_q: 200, partial: false, class: 0, ord: 0 },
            Posting { entity: 7, imp_q: 200, partial: true, class: 2, ord: 3 },
            Posting { entity: 123_456, imp_q: 4, partial: false, class: 5, ord: 15 },
        ];
        for with_names in [false, true] {
            let mut b = vec![0xAA];
            encode_list(&mut b, &list, with_names);
            let got: Vec<Posting> = ListReader::new(&b, 1, with_names).collect();
            assert_eq!(got.len(), list.len());
            for (g, w) in got.iter().zip(&list) {
                assert_eq!((g.entity, g.imp_q, g.partial), (w.entity, w.imp_q, w.partial));
                if with_names {
                    assert_eq!((g.class, g.ord), (w.class, w.ord));
                }
            }
        }
    }

    #[test]
    fn inline_round_trips() {
        let p = Posting { entity: 24_000_000 | POI_BIT, imp_q: 254, partial: true, class: 3, ord: 9 };
        assert_eq!(decode_inline(inline_value(&p)), p);
        assert_eq!(inline_value(&p) & 1, 1);
    }
}
