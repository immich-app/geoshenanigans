use axum::http::HeaderMap;
use place_search::constants::SEARCH_QUERY_MAX;
use place_search::profile::{Profile, RequestError, SearchRequest};
use place_search::request_params::{self, LanguageInput};
use serde::Deserialize;

#[derive(Deserialize)]
pub(super) struct SearchParams {
    q: Option<String>,
    limit: Option<String>,
    lang: Option<String>,
    #[serde(rename = "accept-language")]
    accept_language: Option<String>,
    lat: Option<String>,
    lon: Option<String>,
    zoom: Option<String>,
    location_bias_scale: Option<String>,
    bbox: Option<String>,
    countrycodes: Option<String>,
    layer: Option<String>,
    dedupe: Option<String>,
    profile: Option<String>,
    mode: Option<String>,
    debug: Option<String>,
    pub(super) key: Option<String>,
    pub(super) dataset: Option<String>,
}

impl SearchParams {
    pub(super) fn into_request(self, headers: &HeaderMap) -> Result<(SearchRequest, bool), String> {
        let q = self.q.filter(|q| !q.trim().is_empty()).ok_or("q must name what to search for")?;
        if q.trim().len() > SEARCH_QUERY_MAX {
            return Err(format!("q must be at most {SEARCH_QUERY_MAX} bytes"));
        }

        let profile = self.profile.as_deref().unwrap_or(Profile::Full.as_str()).parse().map_err(|e: RequestError| e.to_string())?;
        let mut req = SearchRequest::new(q, profile);
        if let Some(limit) = self.limit {
            let max_limit = req.policy().max_limit;
            let message = || format!("limit must be 1 to {max_limit}");
            req.limit = limit.parse().map_err(|_| message())?;
            req.require_limit().map_err(|_| message())?;
        }
        if let Some(mode) = self.mode {
            req.mode = mode.parse().map_err(|e: RequestError| e.to_string())?;
        }

        let explicit_lang = self.lang.as_deref().or(self.accept_language.as_deref());
        let (lang, input) = match explicit_lang {
            Some(lang) => (lang, LanguageInput::Explicit),
            None => (headers.get("accept-language").and_then(|h| h.to_str().ok()).unwrap_or_default(), LanguageInput::Header),
        };
        req.langs = request_params::languages(lang, input)?;
        req.bias = request_params::bias(
            number(self.lat, "Invalid lat or lon")?,
            number(self.lon, "Invalid lat or lon")?,
            number(self.zoom, "Invalid zoom")?,
            number(self.location_bias_scale, "Invalid location_bias_scale")?,
        )?;
        if let Some(bbox) = self.bbox {
            let coords = bbox.split(',').map(|s| s.trim().parse()).collect::<Result<Vec<_>, _>>().map_err(|_| "Invalid bbox")?;
            req.viewbox = Some(request_params::bbox(&coords)?);
        }
        if let Some(countries) = self.countrycodes {
            req.countries = request_params::countries(&countries)?;
        }
        if let Some(layer) = self.layer {
            req.layers = request_params::layers(&layer).map_err(|e| e.to_string())?;
        }
        req.dedupe = match self.dedupe.as_deref() {
            None | Some("1") => true,
            Some("0") => false,
            _ => return Err("dedupe must be 0 or 1".to_string()),
        };
        let debug = match self.debug.as_deref() {
            None => false,
            Some("1") => true,
            _ => return Err("debug must be 1".to_string()),
        };
        Ok((req, debug))
    }
}

fn number(value: Option<String>, why: &'static str) -> Result<Option<f64>, &'static str> {
    value.map(|s| s.parse().map_err(|_| why)).transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_defaults_and_language_precedence_are_canonical() {
        let mut headers = HeaderMap::new();
        headers.insert("accept-language", "cs-CZ,*;q=0.5,en;q=0.1".parse().unwrap());
        let query = |params| serde_json::from_value::<SearchParams>(params).unwrap().into_request(&headers).unwrap();

        let (req, debug) = query(serde_json::json!({ "q": "Praha" }));
        assert_eq!(
            (req.profile, req.limit, req.mode, req.dedupe, debug),
            (Profile::Full, place_search::constants::FULL_DEFAULT_LIMIT, place_search::profile::MatchMode::Photon, true, false)
        );
        assert_eq!(req.langs, ["cs-CZ", "cs", "en"]);

        let (req, _) = query(serde_json::json!({ "q": "Praha", "accept-language": "fr" }));
        assert_eq!(req.langs, ["fr"]);
        let (req, debug) = query(serde_json::json!({ "q": "Praha", "lang": "de", "accept-language": "fr", "debug": "1", "dedupe": "0" }));
        assert_eq!(req.langs, ["de"]);
        assert!(debug);
        assert!(!req.dedupe);
        let (req, _) = query(serde_json::json!({ "q": "Praha", "lang": "", "profile": "immich" }));
        assert_eq!(req.limit, req.policy().default_limit);
        assert!(req.langs.is_empty());
    }
}
