//! A record's tag bundle: its OSM tags as the builder stores them in a string tier, one string a
//! record. Pairs of key TAG_KEY_SEP value, joined by TAG_PAIR_SEP, keys sorted bytewise and each
//! once. The keys are the OSM tags as they are (`name`, `name:de`, `alt_name`, `addr:street`, ...),
//! so the search applies Nominatim's sanitizers to them itself. A bundle holds one pair at the
//! least, so it always holds a TAG_KEY_SEP: it never equals a plain name, and the two never share
//! a pool entry.
use crate::constants::{TAG_KEY_SEP, TAG_PAIR_SEP};
use std::borrow::Cow;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagBundle<'a>(Cow<'a, str>);

impl<'a> TagBundle<'a> {
    /// The bundle `s` holds; None for one that is not a bundle: empty, a pair without its
    /// separator or with two, an empty key, or keys out of order or repeated.
    pub fn parse(s: impl Into<Cow<'a, str>>) -> Option<TagBundle<'a>> {
        let s = s.into();
        let mut last: Option<&str> = None;
        for pair in s.split(TAG_PAIR_SEP) {
            let (key, value) = pair.split_once(TAG_KEY_SEP)?;
            if key.is_empty() || value.contains(TAG_KEY_SEP) || last.is_some_and(|k| k >= key) {
                return None;
            }
            last = Some(key);
        }
        Some(TagBundle(s))
    }

    /// The (key, value) pairs, keys ascending.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.split(TAG_PAIR_SEP).filter_map(|pair| pair.split_once(TAG_KEY_SEP))
    }

    /// The value of `key`.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.iter().take_while(|(k, _)| *k <= key).find(|(k, _)| *k == key).map(|(_, v)| v)
    }

    /// Language preferences in request order, including names supplied by a linked place.
    pub fn name_for(&self, langs: &[&str]) -> &str {
        for lang in langs {
            for key in [format!("name:{lang}"), format!("_place_name:{lang}")] {
                if let Some(value) = self.get(&key) { return value; }
            }
        }
        self.get("name").or_else(|| self.get("_place_name"))
            .or_else(|| self.iter().next().map(|(_, v)| v)).unwrap_or_default()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Host bundles are indexed in blocks so the address payload can exceed 4 GiB.
pub struct TagBlob {
    file: query_server::FileBytes,
    rows: u32,
    table: usize,
}

impl TagBlob {
    pub fn open(file: query_server::FileBytes) -> Result<Self, String> {
        let bytes = file.as_slice();
        let Some(footer) = bytes.last_chunk::<4>() else { return Err("missing tag blob footer".into()); };
        let rows = u32::from_le_bytes(*footer);
        let blocks = (rows as usize).div_ceil(crate::constants::TAG_BLOB_BLOCK_ROWS);
        let table = (bytes.len() - footer.len()).checked_sub(blocks * size_of::<u64>()).ok_or("short tag blob table")?;
        let blob = Self { file, rows, table };
        if blob.block_start(0) != Some(0) { return Err("bad first tag blob offset".into()); }
        Ok(blob)
    }

    pub fn count(&self) -> u32 { self.rows }

    fn block_start(&self, block: usize) -> Option<usize> {
        let entry = self.table + block * size_of::<u64>();
        let bytes = self.file.as_slice();
        if entry == bytes.len() - size_of::<u32>() { return Some(self.table); }
        let pos = u64::from_le_bytes(*bytes.get(entry..)?.first_chunk()?);
        usize::try_from(pos).ok().filter(|&pos| pos < self.table)
    }

    pub fn get(&self, row: u32) -> Option<&str> {
        use crate::constants::TAG_BLOB_BLOCK_ROWS as N;
        if row >= self.rows { return None; }
        let row = row as usize;
        let block = row / N;
        let count = N.min(self.rows as usize - block * N);
        let bytes = self.file.as_slice().get(self.block_start(block)?..self.block_start(block + 1)?)?;
        let (offsets, data) = bytes.split_at_checked((count + 1) * size_of::<u32>())?;
        let offset = |i: usize| u32::from_le_bytes(offsets[i * 4..][..4].try_into().unwrap()) as usize;
        std::str::from_utf8(data.get(offset(row % N)..offset(row % N + 1))?).ok()
    }
}

/// The bundle of `tags`, keys sorted, as the builder writes it. None for no tags, an empty key, a
/// key twice, or a separator in a key or a value.
#[cfg(test)]
pub fn encode<'t>(tags: impl IntoIterator<Item = (&'t str, &'t str)>) -> Option<String> {
    let mut tags: Vec<(&str, &str)> = tags.into_iter().collect();
    tags.sort_unstable();
    let separated = |s: &str| s.contains([TAG_KEY_SEP, TAG_PAIR_SEP]);
    let repeated = tags.windows(2).any(|w| w[0].0 == w[1].0);
    if tags.is_empty() || repeated || tags.iter().any(|(k, v)| k.is_empty() || separated(k) || separated(v)) {
        return None;
    }
    let pairs: Vec<String> = tags.iter().map(|(k, v)| format!("{k}{TAG_KEY_SEP}{v}")).collect();
    Some(pairs.join(&TAG_PAIR_SEP.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_names_follow_language_order_including_the_linked_place() {
        let s = encode([("name", "München"), ("name:en", "Munich"), ("_place_name:it", "Monaco di Baviera")]).unwrap();
        let b = TagBundle::parse(s).unwrap();
        assert_eq!(b.name_for(&["it", "en"]), "Monaco di Baviera");
        assert_eq!(b.name_for(&["en", "it"]), "Munich");
        assert_eq!(b.name_for(&["de"]), "München");
    }

    #[test]
    fn host_blob_offsets_bound_each_row_and_refuse_a_truncated_table() {
        use query_server::FileBytes;
        let mut bytes = Vec::new();
        for off in [0u32, 0, 4] { bytes.extend(off.to_le_bytes()); }
        bytes.extend(b"name");
        bytes.extend(0u64.to_le_bytes());
        bytes.extend(2u32.to_le_bytes());
        let blob = TagBlob::open(FileBytes::Owned(bytes.clone())).unwrap();
        assert_eq!(blob.get(0), Some(""));
        assert_eq!(blob.get(1), Some("name"));
        assert_eq!(blob.get(2), None);
        assert!(TagBlob::open(FileBytes::Owned(bytes[..3].to_vec())).is_err());
    }

    #[test]
    fn a_bundle_round_trips_its_tags_keys_sorted() {
        let s = encode([("name:de", "München"), ("name", "München"), ("ISO3166-2", "DE-BY"), ("name:en", "Munich")]).unwrap();

        let b = TagBundle::parse(s.as_str()).unwrap();

        assert_eq!(b.iter().collect::<Vec<_>>(), [("ISO3166-2", "DE-BY"), ("name", "München"), ("name:de", "München"), ("name:en", "Munich")]);
        assert_eq!(s, "ISO3166-2\u{1F}DE-BY\u{1E}name\u{1F}München\u{1E}name:de\u{1F}München\u{1E}name:en\u{1F}Munich");
    }

    #[test]
    fn get_finds_a_key_and_only_that_key() {
        let s = encode([("name", "FUTO"), ("addr:housenumber", "1044"), ("addr:street", "Liberty Park Drive")]).unwrap();
        let b = TagBundle::parse(s).unwrap();

        assert_eq!(b.get("name"), Some("FUTO"));
        assert_eq!(b.get("addr:street"), Some("Liberty Park Drive"));
        assert_eq!(b.get("addr"), None);
        assert_eq!(b.get("name:en"), None);
        assert_eq!(b.get("zzz"), None);
    }

    #[test]
    fn a_value_keeps_its_list_whole() {
        let s = encode([("name", "Foo;Bar"), ("alt_name", "")]).unwrap();
        let b = TagBundle::parse(s).unwrap();

        assert_eq!(b.get("name"), Some("Foo;Bar"));
        assert_eq!(b.get("alt_name"), Some(""));
    }

    #[test]
    fn only_a_bundle_parses() {
        for (s, why) in [
            ("", "empty"),
            ("Berlin", "a plain name"),
            ("name\u{1F}A\u{1F}B", "two key separators"),
            ("\u{1F}A", "an empty key"),
            ("name\u{1F}A\u{1E}", "an empty last pair"),
            ("name\u{1F}A\u{1E}alt_name\u{1F}B", "keys out of order"),
            ("name\u{1F}A\u{1E}name\u{1F}B", "a key twice"),
        ] {
            assert_eq!(TagBundle::parse(s), None, "{why}");
        }
        assert!(TagBundle::parse("name\u{1F}Berlin").is_some());
    }

    #[test]
    fn encode_refuses_what_no_bundle_holds() {
        assert_eq!(encode([]), None);
        assert_eq!(encode([("", "x")]), None);
        assert_eq!(encode([("name", "a"), ("name", "b")]), None);
        assert_eq!(encode([("name", "a\u{1E}b")]), None);
        assert_eq!(encode([("na\u{1F}me", "a")]), None);
    }
}
