//! Every tunable of the prototype. Importance is in (0, ~1.1]; text scores in (0, 1].

// ---- importance of places (corpus kinds P, B, M, A)
pub const W_TYPE: f32 = 0.40;
pub const W_POPULATION: f32 = 0.30;
pub const W_SITELINKS: f32 = 0.30;
/// log10(population) that scores 1.0 (10 million).
pub const POPULATION_LOG_FULL: f32 = 7.0;
/// log10(1 + sitelinks) that scores 1.0 (~316 sitelinks).
pub const SITELINKS_LOG_FULL: f32 = 2.5;
pub const CAPITAL_NATIONAL_BONUS: f32 = 0.12;
pub const CAPITAL_REGIONAL_BONUS: f32 = 0.05;
/// Population counts this much for areas (states, counties, countries, unlinked admin
/// boundaries): their totals dwarf the city a searcher usually means ("New York").
pub const AREA_POPULATION_FACTOR: f32 = 0.5;

// ---- importance of major-tier POIs (corpus kind O): the builder's importance byte (category,
// area, elevation, sitelinks) and the sitelinks themselves
pub const POI_IMPORTANCE_FLOOR: f32 = 0.10;
pub const W_POI_BUILDER: f32 = 0.25;
pub const W_POI_SITELINKS: f32 = 0.45;

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
/// Languages whose names count as class 2 (a name:<lang> or <alt>:<lang> in any other language
/// is class 5): the UI languages a photo library's users mostly type in.
pub const MAJOR_LANGUAGES: [&str; 41] = [
    "en", "de", "fr", "es", "it", "pt", "nl", "ru", "uk", "pl", "cs", "sk", "sv", "da", "no", "nb", "nn", "fi",
    "el", "tr", "ar", "he", "fa", "hi", "bn", "zh", "ja", "ko", "vi", "th", "id", "ms", "hu", "ro", "bg", "hr",
    "sr", "ca", "eu", "gl", "et",
];
/// score = text^2 * (IMPORTANCE_FLOOR + importance) * class * context
pub const IMPORTANCE_FLOOR: f32 = 0.05;

// ---- index shape
/// Trailing-word keys are made from names of at most this many words.
pub const PARTIAL_MAX_WORDS: usize = 6;
pub const PARTIAL_MIN_CHARS: usize = 3;
/// Postings kept per key for trailing-word matches (best importance first).
pub const PARTIAL_CAP: usize = 256;
/// Prefixes up to PREFIX_TABLE_ALL_CHARS chars, and longer ones up to PREFIX_TABLE_MAX_CHARS
/// that start at least HEAVY_PREFIX_MIN_KEYS keys, are answered from a precomputed table.
pub const PREFIX_TABLE_ALL_CHARS: usize = 2;
pub const PREFIX_TABLE_MAX_CHARS: usize = 4;
pub const HEAVY_PREFIX_MIN_KEYS: usize = 2_000;
pub const PREFIX_TABLE_TOP: usize = 40;
/// A lookup stops streaming keys after this many (bounds the worst case).
pub const MAX_KEYS_STREAMED: usize = 400_000;

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
/// Place types that are areas; one of them yields to a same-named settlement in its state
/// among the first AREA_CHECK_TOP candidates.
pub const AREA_TYPES: [&str; 5] = ["country", "state", "province", "region", "county"];
pub const AREA_CHECK_TOP: usize = 40;
pub const DEFAULT_LIMIT: usize = 20;
