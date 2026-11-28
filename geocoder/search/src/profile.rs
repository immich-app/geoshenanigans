//! What a search is asked: one canonical request, and the profile and match mode it names.
//! Profiles (`immich`, `full`) pick the data a search reads and the answer it gives; match modes
//! (`nominatim`, `photon`) pick how the query's words match. Transports map their parameters in
//! and leave nothing to resolve: a request holds one field per concept.
use crate::constants::*;
use crate::index::entities::{BBox, Category};
use crate::index::Sub;
use crate::result::ResultKind;
use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

/// What a search reads and answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Profile {
    /// Immich's: places, areas and POIs from the shipped index, its eight fields.
    Immich = 0,
    /// Everything the host's data holds, answered as Nominatim does.
    Full = 1,
}

impl Profile {
    pub const ALL: [Profile; 2] = [Profile::Immich, Profile::Full];

    pub fn as_str(self) -> &'static str {
        match self {
            Profile::Immich => "immich",
            Profile::Full => "full",
        }
    }

    pub fn policy(self) -> &'static Policy {
        &PROFILES[self as usize]
    }
}

impl FromStr for Profile {
    type Err = RequestError;

    fn from_str(s: &str) -> Result<Profile, RequestError> {
        Profile::ALL.into_iter().find(|p| p.as_str() == s).ok_or_else(|| RequestError::UnknownProfile(s.to_string()))
    }
}

/// How the query's words match: Nominatim's exact n-grams alone, or also Photon's last-word
/// prefix, typo fallback and lenient retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MatchMode {
    Nominatim,
    Photon,
}

impl MatchMode {
    pub const ALL: [MatchMode; 2] = [MatchMode::Nominatim, MatchMode::Photon];

    pub fn as_str(self) -> &'static str {
        match self {
            MatchMode::Nominatim => "nominatim",
            MatchMode::Photon => "photon",
        }
    }
}

impl FromStr for MatchMode {
    type Err = RequestError;

    fn from_str(s: &str) -> Result<MatchMode, RequestError> {
        MatchMode::ALL.into_iter().find(|m| m.as_str() == s).ok_or_else(|| RequestError::UnknownMode(s.to_string()))
    }
}

/// Which results are one place, the first of them kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dedupe {
    /// The same folded name within DEDUPE_KM.
    SameName,
    /// Nominatim's: the same OSM object, or the same OSM type, category, display name and address
    /// rank (v1/helpers.py:126-151).
    Nominatim,
}

/// The areas an entity is a member of, for "in <area>" and countrycodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Membership {
    /// The areas of its point's chain.
    Point,
    /// Those, and for a street way or an area also every area one of its vertices lies in, as
    /// Nominatim intersects their geometries (partition-functions.sql:36-44).
    Vertices,
}

/// The name a result is shown by when the request names no language.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DefaultName {
    /// The name its record holds (`name:en` else `name`).
    Stored,
    /// Its local `name`, as Nominatim's (localization.py:23-98).
    Local,
}

/// What a search answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Output {
    /// Immich's eight fields: name, latitude, longitude, admin1name, admin2name, country,
    /// country_code, type.
    Immich,
    /// Nominatim's jsonv2, and the result's kind.
    NominatimJsonV2,
}

/// What a profile reads and how it answers.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Policy {
    pub profile: Profile,
    pub subs: &'static [Sub],
    pub kinds: &'static [ResultKind],
    pub default_mode: MatchMode,
    /// Whether a photon-mode search that finds nothing tries again with a word left out.
    pub lenient_retry: bool,
    /// Whether importance takes the secondary-importance raster (host data).
    pub raster_importance: bool,
    pub membership: Membership,
    pub default_limit: usize,
    pub max_limit: usize,
    pub dedupe: Dedupe,
    pub default_name: DefaultName,
    pub output: Output,
}

/// Each profile's policy, in Profile's order.
pub const PROFILES: [Policy; 2] = [
    Policy {
        profile: Profile::Immich,
        subs: &[Sub::Core],
        kinds: &[ResultKind::Place, ResultKind::Area, ResultKind::Poi],
        default_mode: MatchMode::Photon,
        lenient_retry: false,
        raster_importance: false,
        membership: Membership::Point,
        default_limit: IMMICH_DEFAULT_LIMIT,
        max_limit: SEARCH_LIMIT_MAX,
        dedupe: Dedupe::SameName,
        default_name: DefaultName::Stored,
        output: Output::Immich,
    },
    Policy {
        profile: Profile::Full,
        subs: &[Sub::Core, Sub::Addr, Sub::AddrX],
        kinds: &ResultKind::ALL,
        default_mode: MatchMode::Photon,
        lenient_retry: true,
        raster_importance: true,
        membership: Membership::Vertices,
        default_limit: FULL_DEFAULT_LIMIT,
        max_limit: SEARCH_LIMIT_MAX,
        dedupe: Dedupe::Nominatim,
        default_name: DefaultName::Local,
        output: Output::NominatimJsonV2,
    },
];

/// Nominatim's layers (types.py:379-406), a filter by what a result is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Layer {
    Address,
    Poi,
    Railway,
    Natural,
    Manmade,
}

impl Layer {
    pub const ALL: [Layer; 5] = [Layer::Address, Layer::Poi, Layer::Railway, Layer::Natural, Layer::Manmade];

    pub fn as_str(self) -> &'static str {
        match self {
            Layer::Address => "address",
            Layer::Poi => "poi",
            Layer::Railway => "railway",
            Layer::Natural => "natural",
            Layer::Manmade => "manmade",
        }
    }
}

impl FromStr for Layer {
    type Err = RequestError;

    /// Its name in any case, as Nominatim's layer parameter (v1/server_glue.py:73-98).
    fn from_str(s: &str) -> Result<Layer, RequestError> {
        Layer::ALL.into_iter().find(|l| l.as_str().eq_ignore_ascii_case(s)).ok_or_else(|| RequestError::UnknownLayer(s.to_string()))
    }
}

/// Photon's location bias: results near the point rank higher (SearchRequestBase.java:11-62).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Bias {
    pub lat: f64,
    pub lon: f64,
    /// 0 to 18: the bias reaches further the lower it is, and not at all at 4 or below.
    pub zoom: u8,
    /// 0 to 1: the share of importance in a result's weight.
    pub scale: f64,
}

impl Bias {
    /// A bias to (lat, lon) at Photon's default zoom and scale.
    pub fn at(lat: f64, lon: f64) -> Bias {
        Bias { lat, lon, zoom: BIAS_ZOOM_DEFAULT, scale: BIAS_SCALE_DEFAULT }
    }
}

/// A box the request names, as Nominatim's viewbox.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Viewbox {
    /// Results lie in it (Nominatim's bounded=1, Photon's bbox).
    Bounded(BBox),
    /// Results in it rank higher (place_search.py:82-90).
    Preferred(BBox),
}

/// A search request, its values resolved: one field per concept.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchRequest {
    pub q: String,
    pub limit: usize,
    /// Languages to show names in, the most wanted first.
    pub langs: Vec<String>,
    pub bias: Option<Bias>,
    pub viewbox: Option<Viewbox>,
    /// Lower-case ISO 3166-1 alpha-2 codes results lie in; none: anywhere.
    pub countries: BTreeSet<[u8; 2]>,
    /// None: results of any layer.
    pub layers: Option<BTreeSet<Layer>>,
    /// Main tags results have one of; none: any.
    pub categories: BTreeSet<Category<'static>>,
    pub dedupe: bool,
    pub profile: Profile,
    pub mode: MatchMode,
}

impl SearchRequest {
    /// A request for `q` with `profile`'s defaults: its limit and mode, deduplicated, no bias and
    /// no filter.
    pub fn new(q: impl Into<String>, profile: Profile) -> SearchRequest {
        let policy = profile.policy();
        SearchRequest {
            q: q.into(),
            limit: policy.default_limit,
            langs: Vec::new(),
            bias: None,
            viewbox: None,
            countries: BTreeSet::new(),
            layers: None,
            categories: BTreeSet::new(),
            dedupe: true,
            profile,
            mode: policy.default_mode,
        }
    }

    pub fn policy(&self) -> &'static Policy {
        self.profile.policy()
    }

    /// The results a search fetches before its dedupe drops some: limit + min(DEDUPE_EXTRA_MAX,
    /// limit) where it deduplicates as Nominatim does (v1/server_glue.py:352-354), else the limit.
    pub fn fetch_limit(&self) -> usize {
        match (self.dedupe, self.policy().dedupe) {
            (true, Dedupe::Nominatim) => self.limit.saturating_add(self.limit.min(DEDUPE_EXTRA_MAX)),
            _ => self.limit,
        }
    }
}

/// Why a request is refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestError {
    UnknownProfile(String),
    UnknownMode(String),
    UnknownLayer(String),
    /// The index holds no data of the profile.
    ProfileNotBuilt(Profile),
    /// The index answers no request that sets this option.
    UnsupportedOption(&'static str),
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RequestError::UnknownProfile(s) => write!(f, "Unknown profile: {s}"),
            RequestError::UnknownMode(s) => write!(f, "Unknown match mode: {s}"),
            RequestError::UnknownLayer(s) => write!(f, "Unknown layer: {s}"),
            RequestError::ProfileNotBuilt(p) => write!(f, "Profile not built: {}", p.as_str()),
            RequestError::UnsupportedOption(option) => write!(f, "Option not supported: {option}"),
        }
    }
}

impl std::error::Error for RequestError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profiles_and_modes_parse_from_their_names_alone() {
        for (s, profile) in [
            ("immich", Ok(Profile::Immich)),
            ("full", Ok(Profile::Full)),
            ("Immich", Err(RequestError::UnknownProfile("Immich".into()))),
            ("hosted", Err(RequestError::UnknownProfile("hosted".into()))),
            ("", Err(RequestError::UnknownProfile("".into()))),
        ] {
            assert_eq!(s.parse::<Profile>(), profile, "{s:?}");
        }
        for (s, mode) in [
            ("nominatim", Ok(MatchMode::Nominatim)),
            ("photon", Ok(MatchMode::Photon)),
            ("Photon", Err(RequestError::UnknownMode("Photon".into()))),
            ("exact", Err(RequestError::UnknownMode("exact".into()))),
        ] {
            assert_eq!(s.parse::<MatchMode>(), mode, "{s:?}");
        }
    }

    #[test]
    fn each_profile_and_mode_parses_from_its_own_name() {
        for p in Profile::ALL {
            assert_eq!(p.as_str().parse(), Ok(p));
        }
        for m in MatchMode::ALL {
            assert_eq!(m.as_str().parse(), Ok(m));
        }
    }

    #[test]
    fn a_layer_parses_from_its_name_in_any_case() {
        for (s, layer) in [
            ("address", Ok(Layer::Address)),
            ("POI", Ok(Layer::Poi)),
            ("Railway", Ok(Layer::Railway)),
            ("natural", Ok(Layer::Natural)),
            ("manmade", Ok(Layer::Manmade)),
            ("man_made", Err(RequestError::UnknownLayer("man_made".into()))),
            (" poi", Err(RequestError::UnknownLayer(" poi".into()))),
        ] {
            assert_eq!(s.parse::<Layer>(), layer, "{s:?}");
        }
    }

    #[test]
    fn each_profile_has_its_own_policy() {
        for p in Profile::ALL {
            assert_eq!(p.policy().profile, p);
        }
    }

    #[test]
    fn the_immich_profile_answers_places_areas_and_pois_from_the_shipped_index_alone() {
        let immich = Profile::Immich.policy();

        assert_eq!(immich.subs, [Sub::Core]);
        assert_eq!(immich.kinds, [ResultKind::Place, ResultKind::Area, ResultKind::Poi]);
        assert_eq!((immich.lenient_retry, immich.raster_importance, immich.membership), (false, false, Membership::Point));
        assert_eq!((immich.dedupe, immich.default_name, immich.output), (Dedupe::SameName, DefaultName::Stored, Output::Immich));
    }

    #[test]
    fn the_full_profile_reads_every_sub_index_and_answers_every_kind() {
        let full = Profile::Full.policy();

        assert_eq!(full.subs, [Sub::Core, Sub::Addr, Sub::AddrX]);
        assert_eq!(full.kinds, ResultKind::ALL);
    }

    #[test]
    fn a_request_takes_its_profiles_defaults() {
        for (profile, limit) in [(Profile::Immich, 20), (Profile::Full, 10)] {
            let req = SearchRequest::new("Austin", profile);

            assert_eq!((req.q.as_str(), req.limit, req.mode, req.dedupe, req.profile), ("Austin", limit, MatchMode::Photon, true, profile));
            assert_eq!((req.bias, req.viewbox, req.layers.as_ref()), (None, None, None));
            assert!(req.langs.is_empty() && req.countries.is_empty() && req.categories.is_empty());
        }
    }

    #[test]
    fn a_search_that_deduplicates_as_nominatim_fetches_more_than_its_limit() {
        for (profile, limit, dedupe, fetched) in [
            (Profile::Full, 10, true, 20),
            (Profile::Full, 3, true, 6),
            (Profile::Full, 50, true, 60),
            (Profile::Full, 10, false, 10),
            (Profile::Full, usize::MAX, true, usize::MAX),
            (Profile::Immich, 20, true, 20),
        ] {
            let req = SearchRequest { limit, dedupe, ..SearchRequest::new("x", profile) };

            assert_eq!(req.fetch_limit(), fetched, "{profile:?} {limit} {dedupe}");
        }
    }

    #[test]
    fn a_bias_takes_photons_default_zoom_and_scale() {
        assert_eq!(Bias::at(30.4, -97.8), Bias { lat: 30.4, lon: -97.8, zoom: 12, scale: 0.4 });
    }

    #[test]
    fn a_refusal_says_what_was_refused() {
        assert_eq!(RequestError::UnknownProfile("hosted".into()).to_string(), "Unknown profile: hosted");
        assert_eq!(RequestError::ProfileNotBuilt(Profile::Full).to_string(), "Profile not built: full");
        assert_eq!(RequestError::UnsupportedOption("viewbox").to_string(), "Option not supported: viewbox");
    }
}
