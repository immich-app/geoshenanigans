//! Every tunable. Importance is in (0, ~1.1]; text scores in (0, 1].

// ---- importance of places: W_TYPE x type base (derived from the records) + the optional
// shipped byte (population, sitelinks, capital: the part the records cannot derive)
pub const W_TYPE: f32 = 0.40;
/// The shipped importance byte's 255 means this much importance.
pub const EXTRA_IMPORTANCE_MAX: f32 = 0.75;
/// Without the shipped byte: a boundary's own area stands in for its population
/// (log10 of the area in km², scaled so 10^AREA_LOG_FULL km² scores 1.0).
pub const W_AREA_PROXY: f32 = 0.15;
pub const AREA_LOG_FULL: f32 = 6.0;

// ---- importance of POIs: the builder's importance byte (category, area, elevation,
// sitelinks) and the optional shipped sitelinks byte
pub const POI_IMPORTANCE_FLOOR: f32 = 0.10;
pub const W_POI_BUILDER: f32 = 0.25;

// ---- text match
/// Query length (chars) up to which a whole-name lookup allows no edit, then one edit.
pub const EXACT_NO_TYPO_MAX_CHARS: usize = 4;
pub const EXACT_ONE_TYPO_MAX_CHARS: usize = 8;
/// The same for prefix lookups (a short prefix with a typo matches too much).
pub const PREFIX_NO_TYPO_MAX_CHARS: usize = 4;
pub const PREFIX_ONE_TYPO_MAX_CHARS: usize = 8;
/// Leading characters a typo-tolerant lookup requires verbatim (Elasticsearch prefix_length).
pub const ANCHOR_CHARS_ONE_EDIT: usize = 1;
pub const ANCHOR_CHARS_TWO_EDITS: usize = 2;
/// Text score of a whole-name match with one / two edits.
pub const TYPO1_TEXT: f32 = 0.55;
pub const TYPO2_TEXT: f32 = 0.35;
/// Prefix text score = PREFIX_BASE + PREFIX_COMPLETION * typed/len, times PREFIX_TYPO per edit.
pub const PREFIX_BASE: f32 = 0.55;
pub const PREFIX_COMPLETION: f32 = 0.35;
pub const PREFIX_TYPO: f32 = 0.60;
/// A match on a name's trailing words ("york" in "new york").
pub const PARTIAL_TEXT: f32 = 0.80;
/// Name classes: name, name:en, a major language, alternates, old names, any other language.
pub const CLASS_MULT: [f32; 6] = [1.0, 1.0, 0.95, 0.90, 0.80, 0.60];
/// Class of a transliterated name (an alternate).
pub const TRANSLITERATION_CLASS: u8 = 3;
/// score = text^2 * (IMPORTANCE_FLOOR + importance) * class * context
pub const IMPORTANCE_FLOOR: f32 = 0.05;

// ---- index shape
/// Trailing-word keys are made from names of at most this many words.
pub const PARTIAL_MAX_WORDS: usize = 6;
pub const PARTIAL_MIN_CHARS: usize = 3;
/// Postings kept per key for trailing-word matches (best importance first).
pub const PARTIAL_CAP: usize = 256;
/// The place nodes the index build holds at once to fill them in name order: 3 MiB a worker.
pub const NAME_BATCH_NODES: u32 = 1 << 17;
/// Prefixes up to PREFIX_TABLE_ALL_CHARS chars, and longer ones up to PREFIX_TABLE_MAX_CHARS
/// that start at least HEAVY_PREFIX_MIN_KEYS keys, are answered from a precomputed table.
pub const PREFIX_TABLE_ALL_CHARS: usize = 2;
pub const PREFIX_TABLE_MAX_CHARS: usize = 4;
pub const HEAVY_PREFIX_MIN_KEYS: usize = 2_000;
pub const PREFIX_TABLE_TOP: usize = 40;
/// A lookup stops streaming keys after this many (bounds the worst case and the per-query
/// memory; the most any evaluation query matched is 13,424).
pub const MAX_KEYS_STREAMED: usize = 50_000;
/// A lookup's walk of the keys stops after taking this many transitions (bounds the walk of a
/// damaged file, whose paths can share nodes in ways no build writes; the most any evaluation
/// query took is 593,669).
pub const MAX_KEY_STEPS: usize = 1 << 24;
/// Extra names of one entity a posting can point at (more get ordinal 15: shown by the base name).
pub const MAX_NAME_ORD: u8 = 15;
pub const MAX_EXTRA_ORD: u8 = 13;
/// Ordinal of a boundary's own name: a labelled one's (its primary name is its label node's), and
/// the name a polygon had that goes by its node's English name (admin_own_names.bin).
pub const ORD_BOUNDARY_NAME: u8 = 14;
/// Admin levels whose boundaries are searchable without a label node (country .. municipality).
pub const BOUNDARY_MIN_LEVEL: u8 = 2;
pub const BOUNDARY_MAX_LEVEL: u8 = 8;

// ---- files
/// The index's dir inside the dataset dir it belongs to (where packs that ship one put it).
pub const DATASET_SEARCH_DIR: &str = "search";
/// What an index was built from (options, sub-index stats, the dataset's file hashes); written last.
pub const META_FILE: &str = "search_meta.json";
/// The layout of the index files (the lists and their offsets, the boundary slots), recorded in
/// META_FILE as META_FORMAT_KEY: an index of another is built again, never read.
pub const PLACES_INDEX_FORMAT: u64 = 2;
pub const META_FORMAT_KEY: &str = "format";
/// The META_FILE field that holds the dataset's file hashes (dataset.rs).
pub const META_DATASET_KEY: &str = "dataset";
/// The sub-indexes an index can hold, in build order.
pub const PLACES_SUB_INDEXES: &[&str] = &["places", "poi"];
/// The files `<sub>.<ext>` of each sub-index: its keys, their postings and its prefix table.
pub const SUB_INDEX_EXTS: [&str; 4] = ["fst", "post", "pfx.fst", "pfx"];
/// The boundary slots of the places sub-index.
pub const BOUNDARY_FILE: &str = "places.bnd";
/// Hashes of the dataset files beside an index, with the stat each was taken at (not shipped).
pub const HASH_CACHE_FILE: &str = "dataset_hashes.json";
/// Read buffer for hashing a dataset file.
pub const HASH_READ_BUFFER: usize = 256 * 1024;

// ---- build
/// Default memory for sort runs (all threads together). At 16 the build fits a 32 MiB cgroup for
/// every set (peak anonymous 13-24 MiB, minimal to max); 64 measured no faster on one core.
pub const DEFAULT_RUN_BUDGET_MB: usize = 16;
/// Percent of a sort run's memory that holds key bytes; the rest holds its entries.
pub const RUN_ARENA_PERCENT: usize = 60;
/// Longest key a sort run holds: a run entry stores the key length as a u16. Folding sets no
/// length of its own, so the names do: OSM tag values are at most 255 characters and one
/// character folds to at most 33 bytes (U+FDFA), so a name's key is at most 8,415 bytes.
pub const MAX_KEY_BYTES: usize = u16::MAX as usize;
/// Least sort-run memory per thread: its arena share holds a key of MAX_KEY_BYTES.
pub const MIN_RUN_BUDGET_BYTES: usize = (MAX_KEY_BYTES * 100).div_ceil(RUN_ARENA_PERCENT);
/// The smallest page an anonymous mapping starts on, so the most a sort buffer's elements may
/// be aligned to.
pub const MIN_PAGE_BYTES: usize = 4096;
/// Most key bytes a sort run holds: an entry addresses its key by a u32 offset, so the arena share
/// past 4 GiB (above 6,826 MiB of budget per thread) can go to entries instead.
pub const MAX_RUN_ARENA_BYTES: usize = u32::MAX as usize;
/// Read buffer per sort run during the merge.
pub const RUN_READ_BUFFER: usize = 64 * 1024;
/// Sort runs merged at once; more are first merged in groups of this many into longer runs
/// (bounds the merge's read buffers, and the readahead it keeps in the page cache).
pub const MERGE_FAN_IN: usize = 16;
/// Write buffer of each file the build writes.
pub const WRITE_BUFFER: usize = 64 * 1024;
/// Directory inside the index directory a build writes into before moving its files over.
pub const BUILD_STAGE_DIR: &str = ".stage";
/// Directory inside the staging directory the files a build replaces are moved to until it is done.
pub const BUILD_ASIDE_DIR: &str = ".replaced";

// ---- query
/// Candidates kept while scoring (before de-duplication and the result limit).
pub const CANDIDATE_POOL: usize = 200;
/// Trailing query words tried as context ("paris texas").
pub const MAX_CONTEXT_WORDS: usize = 3;
pub const CONTEXT_MISS_MULT: f32 = 0.02;
/// A context word this long also matches a context name it starts ("tex" -> "texas").
pub const CONTEXT_PREFIX_MIN_CHARS: usize = 3;
pub const CONTEXT_TYPO_MIN_CHARS: usize = 5;
/// A context word this long also matches one word of a multi-word context name.
pub const CONTEXT_WORD_MIN_CHARS: usize = 4;
/// Results with the same folded name closer than this are one place.
pub const DEDUPE_KM: f64 = 25.0;
/// Area entities yield to a same-named settlement in their state among the first AREA_CHECK_TOP.
pub const AREA_TYPES: [&str; 5] = ["country", "state", "province", "region", "county"];
pub const AREA_CHECK_TOP: usize = 40;
/// Context checks run while a candidate can still beat the (limit x this)-th best score.
pub const CONTEXT_CUTOFF_FACTOR: usize = 2;
/// At most this many context checks per query (candidates past it count as misses).
pub const MAX_CONTEXT_CHECKS: usize = 2_000;
/// Candidates waiting for a context check are cut back to the best 2 x MAX_CONTEXT_CHECKS
/// whenever they pass 4 x (bounds per-query memory).
pub const MAX_PENDING_CANDIDATES: usize = 4 * MAX_CONTEXT_CHECKS;

// ---- search requests and profiles (profile.rs)
/// The most bytes of a native HTTP search query after trimming.
pub const SEARCH_QUERY_MAX: usize = 256;
/// Results a request asks for without a limit: the immich profile's, as before profiles, and
/// Nominatim's (v1/server_glue.py:352) for the full profile.
pub const IMMICH_DEFAULT_LIMIT: usize = 20;
pub const FULL_DEFAULT_LIMIT: usize = 10;
/// The most results a request asks for (v1/server_glue.py:352).
pub const SEARCH_LIMIT_MAX: usize = 50;
/// A search that deduplicates as Nominatim does fetches limit + min(this, limit) results for its
/// dedupe to drop from (v1/server_glue.py:353-354).
pub const DEDUPE_EXTRA_MAX: usize = 10;
/// Photon's location bias without a zoom or a scale (SearchRequestBase.java:11-12).
pub const BIAS_ZOOM_DEFAULT: u8 = 12;
pub const BIAS_SCALE_DEFAULT: f64 = 0.4;
pub const BIAS_ZOOM_MAX: u8 = 18;
pub const BIAS_SCALE_MAX: f64 = 1.0;
pub const LANG_WEIGHT_DEFAULT: f64 = 1.0;

// ---- search plans (query/plan.rs): searches of one penalty run in this order, least first
// (db_searches/country_search.py:23, place_search.py:36, address_search.py:135, base.py:96)
pub const SEARCH_PRIO_COUNTRY: u8 = 0;
pub const SEARCH_PRIO_NAME: u8 = 1;
pub const SEARCH_PRIO_OTHER: u8 = 2;

// ---- tag bundles (tags.rs)
/// Ends a key in a bundle; TAG_PAIR_SEP joins its pairs. No OSM key or value holds either: XML
/// 1.0, which the OSM API speaks, allows neither.
pub const TAG_KEY_SEP: char = '\u{1F}';
pub const TAG_PAIR_SEP: char = '\u{1E}';
/// The tag files of a dataset dir: a bundle's string offset for each place row, admin polygon
/// and POI row (side_blocks.rs), NO_DATA for a record with nothing beyond its name.
pub const PLACE_TAGS_FILE: &str = "place_tags.bin";
pub const ADMIN_TAGS_FILE: &str = "admin_tags.bin";
pub const POI_TAGS_FILE: &str = "poi_tags.bin";

// ---- entities (index/entities.rs)
/// Bits of an entity's record in an index row; the kind takes the 3 above them.
pub const ENTITY_RECORD_BITS: u32 = 29;

// ---- text: Nominatim's tokenizer and the Python it measures with
/// difflib's autojunk: in a second sequence of at least this many elements, an element on more
/// than len / DIFFLIB_POPULAR_PER + 1 of its positions is popular and starts no match.
pub const DIFFLIB_AUTOJUNK_MIN_LEN: usize = 200;
pub const DIFFLIB_POPULAR_PER: usize = 100;
/// A house number with a run of this many letters keeps its spaces as they are ("12 abcd").
pub const HOUSENUMBER_NAMED_PART_LETTERS: usize = 4;
/// A house number with more digit-letter boundaries than this gets no optional spaces.
pub const HOUSENUMBER_MAX_OPTIONAL_SPACES: usize = 4;
/// The languages whose name variants Nominatim makes (its tag-analyzer-by-language whitelist).
pub const NAME_LANGUAGES: [&str; 30] = [
    "bg", "ca", "cs", "da", "de", "el", "en", "es", "et", "eu", "fi", "fr", "gl", "hu", "it", "ja", "mg", "ms", "nl", "no",
    "pl", "pt", "ro", "ru", "sk", "sl", "sv", "tr", "uk", "vi",
];
/// Name key suffixes that are no language: the name takes its country's languages.
pub const NAME_SUFFIXES_WITHOUT_LANGUAGE: [&str; 2] = ["left", "right"];
/// A name whose rules would make more variants than this keeps only itself.
pub const VARIANT_MAX_PARTIALS: usize = 128;
/// What stands between a term's source name and the language that varied it in the term's word
/// ("liberty park drive@en"), as in Nominatim's `word` column.
pub const TERM_WORD_LANGUAGE_MARK: char = '@';
/// A house number with a run of this many characters between list delimiters is a name.
pub const HOUSENUMBER_AS_NAME_RUN: usize = 15;
/// A house number with a list part of this many characters and no digit is a name.
pub const HOUSENUMBER_AS_NAME_WORD: usize = 3;
/// An interpolated house-number range of fewer numbers than this becomes each of them.
pub const INTERPOLATION_EXPAND_BELOW: u128 = 10;
/// What stands between the names of a multilingual `name` ("Bruxelles - Brussel").
pub const MULTILINGUAL_DELIMITERS: [char; 3] = ['/', '-', ' '];
/// A ref longer than this is no name.
pub const REF_MAX_CHARS: usize = 40;
/// The pattern a place's postcode is read by in a country of postcodes and no pattern of its own
/// (the clean-postcodes default-pattern of Nominatim's settings/sanitizers.yaml).
pub const POSTCODE_DEFAULT_PATTERN: &str = "[A-Z0-9- ]{3,12}";

/// Host TagBlob block geometry, shared with the builder's search_files.h.
pub const TAG_BLOB_BLOCK_ROWS: usize = 4096;
pub const TAG_TIERS: [&str; 2] = ["strings_tags_core.bin", "strings_tags_poi.bin"];
pub const IMPORTANCE_FILES: [&str; 3] = ["place_imp.bin", "admin_imp.bin", "poi_imp.bin"];

// ---- reading a query (query/): Nominatim's penalties, thresholds and limits, each named after
// the line of Nominatim's search/ that sets it
/// A query is split into phrases at this character (core.py:280).
pub const PHRASE_SEPARATOR: char = ',';
/// A phrase of this many characters or more reads as nothing (settings/query_preprocessing.yaml).
pub const PHRASE_MAX_CHARS: usize = 500;
/// A query of more token slots than this is refused (icu_tokenizer.py:132).
pub const QUERY_MAX_SLOTS: usize = 50;
/// A node's token of this many characters or more makes no node (icu_tokenizer.py:222).
pub const NODE_TOKEN_MAX_CHARS: usize = 256;
/// Penalties of the break before a node (query.py:93-101): the query's start and end and a
/// phrase break; a word break; a break inside a word; between the terms of one written word.
pub const BREAK_PENALTY_PHRASE: f64 = -0.5;
pub const BREAK_PENALTY_WORD: f64 = 0.1;
pub const BREAK_PENALTY_PART: f64 = 0.2;
pub const BREAK_PENALTY_TOKEN: f64 = 0.4;
/// Coordinates in a nominatim-mode query make a point results lie within this many degrees of
/// (v1/server_glue.py:313).
pub const NEAR_RADIUS_DEG: f64 = 0.1;
/// N-grams a query's words are looked up as: at most this many nodes and characters
/// (query.py:474-480).
pub const NGRAM_MAX_NODES: usize = 20;
pub const NGRAM_MAX_CHARS: usize = 255;
/// A node's partial is missing at this penalty or more (query.py:184, icu_tokenizer.py:224).
pub const PARTIAL_MISSING_PENALTY: f64 = 10.0;
/// Base penalties of a looked-up token (icu_tokenizer.py:60-72): a partial word; a full name of
/// one character, a digit or not; each character of a house number other than digits and
/// spaces, and each character but one of a house number without digits; a country of one
/// character.
pub const PARTIAL_PENALTY: f64 = 0.3;
pub const WORD_ONE_DIGIT_PENALTY: f64 = 0.2;
pub const WORD_ONE_CHAR_PENALTY: f64 = 0.3;
pub const HOUSENUMBER_NON_DIGIT_PENALTY: f64 = 0.1;
pub const HOUSENUMBER_NO_DIGIT_PENALTY: f64 = 0.2;
pub const COUNTRY_ONE_CHAR_PENALTY: f64 = 0.3;
/// A whole word of at most this many digits and no house-number term reads as a house number at
/// this penalty (icu_tokenizer.py:244-262).
pub const DIGITS_HOUSENUMBER_MAX_CHARS: usize = 4;
pub const DIGITS_HOUSENUMBER_PENALTY: f64 = 0.2;
/// Where a postcode can be read, every other token pays this, but a house number whose first
/// word has at most POSTCODE_RERANK_HOUSENUMBER_MAX_CHARS characters (icu_tokenizer.py:270-279).
pub const POSTCODE_RERANK_PENALTY: f64 = 0.39;
pub const POSTCODE_RERANK_HOUSENUMBER_MAX_CHARS: usize = 4;
/// Where a house number of at most this many characters with a digit can be read, every other
/// token pays SIMPLE_HOUSENUMBER_PENALTY less the house number's own (icu_tokenizer.py:282-291).
pub const SIMPLE_HOUSENUMBER_MAX_CHARS: usize = 3;
pub const SIMPLE_HOUSENUMBER_PENALTY: f64 = 0.5;
/// A token other than a name or a partial pays this many times its match penalty
/// (icu_tokenizer.py:299-300).
pub const MATCH_PENALTY_FACTOR_OTHER: f64 = 2.0;
/// The photon mode's last-word completions, and near spellings, looked up per kind and n-gram.
pub const PREFIX_TERMS_MAX: usize = 64;
pub const FUZZY_TERMS_MAX: usize = 64;
/// Photon's AUTO fuzziness (SearchQueryBuilder.java:47-49, 113): no edit in a query shorter than
/// TYPO_MIN_QUERY_CHARS; per word, one edit from TYPO_ONE_EDIT_MIN_CHARS characters and two from
/// TYPO_TWO_EDITS_MIN_CHARS; the first TYPO_ANCHOR_SHORT characters kept as typed in a word of at
/// most TYPO_ANCHOR_SHORT_MAX_CHARS, TYPO_ANCHOR_LONG in a longer one.
pub const TYPO_MIN_QUERY_CHARS: usize = 4;
pub const TYPO_ONE_EDIT_MIN_CHARS: usize = 3;
pub const TYPO_TWO_EDITS_MIN_CHARS: usize = 6;
pub const TYPO_ANCHOR_SHORT_MAX_CHARS: usize = 6;
pub const TYPO_ANCHOR_SHORT: usize = 1;
pub const TYPO_ANCHOR_LONG: usize = 2;
/// The direction a query is read in is told for at most this many token slots, from two slots'
/// difference over DIRECTION_TWO_SLOT_DIVISOR or a regression over more (query.py:24-25, 383-396).
pub const DIRECTION_MAX_SLOTS: usize = 50;
pub const DIRECTION_TWO_SLOT_DIVISOR: f64 = 3.0;
/// Readings (token_assignment.py): a house number with two partial ranges on the side its
/// reading puts the name pays HOUSENUMBER_TWO_PRIORS_PENALTY, one with more is no reading
/// (:210-243); one with a near item pays HOUSENUMBER_NEAR_ITEM_PENALTY (:244-245).
pub const HOUSENUMBER_PRIORS_MAX: usize = 2;
pub const HOUSENUMBER_TWO_PRIORS_PENALTY: f64 = 0.8;
pub const HOUSENUMBER_NEAR_ITEM_PENALTY: f64 = 1.0;
/// A postcode search with an address pays POSTCODE_LATE_PENALTY when the postcode comes last and
/// POSTCODE_EXTRA_ADDRESS_PENALTY for each address range past the first (:256-265); an address
/// reading with the postcode first pays POSTCODE_FIRST_PENALTY (:379-381).
pub const POSTCODE_LATE_PENALTY: f64 = 0.1;
pub const POSTCODE_EXTRA_ADDRESS_PENALTY: f64 = 0.1;
pub const POSTCODE_FIRST_PENALTY: f64 = 0.1;
/// A name split off the first range pays SPLIT_PENALTY with a house number after it or commas in
/// the query; one split off the last range SPLIT_AFTER_HOUSENUMBER_PENALTY with a house number
/// before it and SPLIT_PENALTY with commas (:297-301, 343-346).
pub const SPLIT_PENALTY: f64 = 0.25;
pub const SPLIT_AFTER_HOUSENUMBER_PENALTY: f64 = 0.4;
/// The house-number search of a reading costs its best address reading's penalty, at most its own
/// plus HOUSENUMBER_READING_SPAN, and HOUSENUMBER_READING_PENALTY more (:383, 397-399).
pub const HOUSENUMBER_READING_SPAN: f64 = 2.0;
pub const HOUSENUMBER_READING_PENALTY: f64 = 0.1;
/// Searches (db_search_builder.py): a name ranking's default is its partials' penalties plus
/// RANKING_PARTIALS_PENALTY (:309, 351); an address ranking's the cheapest cover by partials plus
/// RANKING_ADDRESS_DEFAULT_PENALTY (:357), its covers counted up to RANKING_ADDRESS_MAX_VARIANTS
/// (:348).
pub const RANKING_PARTIALS_PENALTY: f64 = 0.2;
pub const RANKING_ADDRESS_DEFAULT_PENALTY: f64 = 0.3;
pub const RANKING_ADDRESS_MAX_VARIANTS: usize = 10;
/// A postcode search without a country pays this (:145).
pub const POSTCODE_SEARCH_NO_COUNTRY_PENALTY: f64 = 0.1;
/// A house-number search looks its numbers up below this many expected rows (:169); else its
/// address partials by LOOKUP_HOUSENUMBER_LIMIT and _FACTOR (:175, 180), else at most
/// HOUSENUMBER_SEARCH_MAX_FULLS full names of its first address range (:184).
pub const HOUSENUMBER_LOOKUP_MAX_COUNT: u64 = 10_000;
pub const LOOKUP_HOUSENUMBER_LIMIT: u64 = 20_000;
pub const LOOKUP_HOUSENUMBER_FACTOR: u64 = 5;
pub const HOUSENUMBER_SEARCH_MAX_FULLS: usize = 5;
/// How many partials drive a lookup: the fewest of the rarest whose expected rows stay below a
/// limit that grows by a factor with each one (db_search_fields.py:36-57), at most
/// LOOKUP_TERMS_MAX; a term of count 1 has no statistics and sorts as COUNT_WITHOUT_STATISTICS
/// (:25-31). A name alone (db_search_builder.py:230); a name and an address (:254-255), and both
/// again when both are too frequent (:271-272).
pub const LOOKUP_TERMS_MAX: usize = 3;
pub const COUNT_WITHOUT_STATISTICS: u64 = 100_000;
pub const LOOKUP_NAME_LIMIT: u64 = 30_000;
pub const LOOKUP_NAME_FACTOR: u64 = 6;
pub const LOOKUP_ADDRESS_NAME_LIMIT: u64 = 20_000;
pub const LOOKUP_ADDRESS_NAME_FACTOR: u64 = 6;
pub const LOOKUP_ADDRESS_LIMIT: u64 = 10_000;
pub const LOOKUP_ADDRESS_FACTOR: u64 = 3;
pub const LOOKUP_FREQUENT_NAME_LIMIT: u64 = 50_000;
pub const LOOKUP_FREQUENT_NAME_FACTOR: u64 = 10;
pub const LOOKUP_FREQUENT_ADDRESS_LIMIT: u64 = 30_000;
pub const LOOKUP_FREQUENT_ADDRESS_FACTOR: u64 = 5;
/// The rows a lookup of all a name's or an address's partials is expected to find: its rarest
/// term's count over this factor for each other term (:233, 244, 280, 286).
pub const EXPECTED_NAME_FACTOR: u64 = 5;
pub const EXPECTED_ADDRESS_FACTOR: u64 = 3;
/// A name too frequent to look up by its partials: by its full names below
/// FULLS_NAME_MAX_COUNT, else by its partials at PARTIALS_LOOKUP_PENALTY below
/// PARTIALS_LOOKUP_MAX_COUNT (:236-247).
pub const FULLS_NAME_MAX_COUNT: u64 = 50_000;
pub const PARTIALS_LOOKUP_PENALTY: f64 = 1.0;
pub const PARTIALS_LOOKUP_MAX_COUNT: u64 = 50_000;
/// A name and an address both too frequent: by the name's full names below
/// FULLS_ADDRESS_MAX_COUNT, the other lookups paying FULLS_FIRST_PENALTY more, all
/// FREQUENT_PENALTY more (:257-272); by name and address partials together at
/// BOTH_PARTIALS_PENALTY below BOTH_PARTIALS_MAX_COUNT, at most BOTH_PARTIALS_TERMS of each
/// (:287-297).
pub const FULLS_ADDRESS_MAX_COUNT: u64 = 80_000;
pub const FULLS_FIRST_PENALTY: f64 = 0.2;
pub const FREQUENT_PENALTY: f64 = 0.4;
pub const BOTH_PARTIALS_PENALTY: f64 = 0.5;
pub const BOTH_PARTIALS_MAX_COUNT: u64 = 50_000;
pub const BOTH_PARTIALS_TERMS: usize = 3;
/// At most this many searches run (geocoder.py:310), and with a [class=type] near query only
/// that many are wrapped (geocoder.py:276).
pub const SEARCHES_MAX: usize = 50;

pub const INDEX_FORMAT: u64 = 3;
pub const SUB_INDEXES: &[&str] = &["core", "addr"];
pub const CORE_FILES: &[&str] =
    &["core.ent", "core.chn", "core.area", "core.fst", "core.post", "core.pfx.fst", "core.pfx"];
pub const BBOX_COORDS: usize = 4;
pub const BBOX_BYTES: usize = BBOX_COORDS * size_of::<i32>();
pub const ENTITY_MAGIC: &[u8; 4] = b"PSE3";
pub const SKIP_EVERY: u32 = 128;
pub const TOPK_MIN_DF: u32 = 1024;
pub const TOPK_PER_TERM: usize = 256;
pub const ENTITY_BLOCK_ROWS: u32 = 128;
pub const CORE_KIND_BITS: u32 = 2;
pub const KINDS_PER_BYTE: u32 = u8::BITS / CORE_KIND_BITS;
pub const ENTITY_HEADER_BYTES: usize = ENTITY_MAGIC.len() + size_of::<u32>();
pub const IMP_UNIT: f64 = 1.0 / 655_350_000.0;
pub const DEFAULT_IMPORTANCE_BASE: f64 = 0.40001;
pub const IMPORTANCE_RANK_DIVISOR: f64 = 75.0;
pub const MAX_ADDRESS_RANK: u8 = 25;
/// Nominatim addresses a ranked polygon from its centroid. An unranked one whose rank
/// is above this takes the areas its outline meets (placex_triggers.sql:1183-1197).
pub const OUTLINE_ADDRESS_MIN_RANK: u8 = 10;
pub const POI_SEARCH_RANK: u8 = 30;
pub const COUNTRY_LEVEL: u8 = 2;
pub const PLACE_AREA_LEVEL: u8 = 15;
pub const ADMIN_LEVEL_MAX: u8 = 12;
pub const CORE_KINDS: [crate::index::entities::EntityKind; 3] = [
    crate::index::entities::EntityKind::Place,
    crate::index::entities::EntityKind::Area,
    crate::index::entities::EntityKind::Poi,
];
/// Odd FST values carry singleton entity ids. Even values address the posting file.
pub const INLINE_POSTING: u64 = 1;
pub const SKIP_BYTES: usize = 2 * size_of::<u32>();
/// Sixteen u16-length keys take at most one MiB alongside the final merge.
pub const TERM_MERGE_BATCH: usize = 16;
pub const POSTING_BUFFER_BYTES: usize = WRITE_BUFFER;
pub const CHAIN_CACHE_CELLS: usize = 64;
pub const PHOTON_MAX_EDITS: u8 = 2;
pub const CORE_CHAIN_FILES: [&str; 3] = ["place_chains.bin", "admin_chains.bin", "poi_chains.bin"];
pub const CHAIN_DICTIONARY: &str = "search_chains.bin";
pub const CHAIN_INPUTS: [&str; 6] = [
    "admin_cells.bin",
    "admin_entries.bin",
    CHAIN_DICTIONARY,
    CORE_CHAIN_FILES[0],
    CORE_CHAIN_FILES[1],
    CORE_CHAIN_FILES[2],
];

pub const CORE_RECOVERY_DIR: &str = ".core-rebuild";
pub const WORD_BEFORE: u8 = 1;
pub const WORD_SAME: u8 = 2;
pub const WORD_AFTER: u8 = 3;
pub const LOOKUP_IS_TOKEN: u8 = 1;
pub const SPELLING_SLOT_BYTES: usize = 1024;
pub const MAX_SPELLING_SLOTS: usize = 2048;
pub const SPELLING_BUDGET_DIVISOR: usize = 16;
pub const MAX_CORE_POI_TIER: u8 = 3;

/// Upper search rank and radius in metres of an unlinked place node's fuzzy area
/// (Nominatim's place_node_fuzzy_area).
pub const PLACE_NODE_FUZZY_RADII: [(u8, u32); 5] =
    [(16, 15_000), (18, 4_000), (19, 2_000), (20, 1_000), (u8::MAX, 500)];
pub const CORE_RECORD_BYTES: usize = 3 * size_of::<u32>();
pub const OSM_ID_FILES: [&str; 3] = ["place_nodes.osm_ids", "admin_polygons.osm_ids", "poi_rows.osm_ids"];
pub const OSM_SLOT_MAGIC: u32 = 0xd0510ead;
pub const OSM_SLOT_VERSION: u32 = 1;
pub const OSM_SLOT_HEADER: usize = 3 * size_of::<u32>();
pub const OSM_SLOT_BYTES: usize = 4 + size_of::<u64>();
pub const OSM_SLOT_TOMBSTONE: u8 = 1;
pub const OSM_RELATION_RING_BITS: u32 = 16;
pub const NAME_OCCURRENCE: u32 = 0;
pub const AREA_MEMBERS: u32 = 1;
/// Fixed-width registry entries bound suffix sharing even for a multilingual alphabet.
pub const CORE_FST_CACHE_SLOTS: usize = 32 * 1024;
pub const CORE_FST_CACHE_ARCS: usize = 2;
pub const FST_ALPHABET: usize = 256;
pub const FST_STATE_COUNT_MASK: u8 = 0x3f;
pub const FST_FINAL_FLAG: u8 = 0x40;
pub const FST_ONE_FLAG: u8 = 0x80;
pub const FST_NEXT_FLAG: u8 = 0xc0;
pub const CRC32C_REVERSED: u32 = 0x82f63b78;
pub const SNAPPY_CRC_MASK: u32 = 0xa282ead8;
pub const FST_HASH_PRIME: u64 = 1099511628211;
pub const FST_SIZE_SHIFT: usize = 4;
pub const SNAPPY_CRC_ROTATION: u32 = 15;
