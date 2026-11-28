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
pub const INDEX_FORMAT: u64 = 2;
pub const META_FORMAT_KEY: &str = "format";
/// The META_FILE field that holds the dataset's file hashes (dataset.rs).
pub const META_DATASET_KEY: &str = "dataset";
/// The sub-indexes an index can hold, in build order.
pub const SUB_INDEXES: &[&str] = &["places", "poi"];
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
/// Most key bytes a sort run holds: an entry addresses its key by a u32 offset, so the arena share
/// past 4 GiB (above 6,826 MiB of budget per thread) stays unused.
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
