//! Values shared by the HTTP and JavaScript adapters for SearchRequest.
use std::collections::BTreeSet;

use crate::constants::{BIAS_SCALE_DEFAULT, BIAS_SCALE_MAX, BIAS_ZOOM_DEFAULT, BIAS_ZOOM_MAX, LANG_WEIGHT_DEFAULT};
use crate::index::entities::BBox;
use crate::profile::{Bias, Layer, RequestError, Viewbox};

fn language(item: &str) -> Option<(&str, f64)> {
    let (name, weight) = match item.trim().split_once(';') {
        Some((name, quality)) => {
            let (key, value) = quality.trim_start().split_once('=')?;
            let value = value.trim();
            let (whole, fraction) = value.split_once('.').map_or((value, None), |(w, f)| (w, Some(f)));
            if !key.trim_end().eq_ignore_ascii_case("q")
                || !matches!(whole, "0" | "1")
                || fraction.is_some_and(|f| f.is_empty() || !f.bytes().all(|b| b.is_ascii_digit()))
            {
                return None;
            }
            (name, value.parse().ok()?)
        }
        None => (item.trim(), LANG_WEIGHT_DEFAULT),
    };
    if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphabetic() || matches!(b, b'_' | b'-')) {
        return None;
    }
    Some((name, weight))
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LanguageInput {
    Explicit,
    Header,
}

/// Nominatim's Accept-Language ordering and fallback (localization.py). Explicit native options
/// validate every entry, while a client-supplied header may contain unsupported entries.
pub fn languages(value: &str, input: LanguageInput) -> Result<Vec<String>, &'static str> {
    let mut entries = Vec::new();
    for item in value.split(',').filter(|item| !item.trim().is_empty()) {
        match language(item) {
            Some(entry) => entries.push(entry),
            None if input == LanguageInput::Explicit => return Err("Invalid lang"),
            None => {}
        }
    }
    entries.sort_by(|a, b| b.1.total_cmp(&a.1));
    let mut langs = Vec::new();
    for &(lang, _) in &entries {
        langs.push(lang.to_string());
        if let Some((base, _)) = lang.split_once('-').filter(|(base, _)| !entries.iter().any(|(name, _)| name == base)) {
            langs.push(base.to_string());
        }
    }
    Ok(langs)
}

pub fn bias(lat: Option<f64>, lon: Option<f64>, zoom: Option<f64>, scale: Option<f64>) -> Result<Option<Bias>, &'static str> {
    let zoom_value = zoom.unwrap_or(f64::from(BIAS_ZOOM_DEFAULT));
    if !(0.0..=f64::from(BIAS_ZOOM_MAX)).contains(&zoom_value) || zoom_value.fract() != 0.0 {
        return Err("Invalid zoom");
    }
    let scale_value = scale.unwrap_or(BIAS_SCALE_DEFAULT);
    if !(0.0..=BIAS_SCALE_MAX).contains(&scale_value) {
        return Err("Invalid location_bias_scale");
    }
    match (lat, lon) {
        (Some(lat), Some(lon)) if query_server::coords_in_range(lat, lon) => {
            Ok(Some(Bias { lat, lon, zoom: zoom_value as u8, scale: scale_value }))
        }
        (Some(_), Some(_)) => Err("Invalid lat or lon"),
        (None, None) if zoom.is_none() && scale.is_none() => Ok(None),
        (None, None) => Err("Location bias needs lat and lon"),
        _ => Err("lat and lon must be given together"),
    }
}

pub fn bbox(coords: &[f64]) -> Result<Viewbox, &'static str> {
    let &[min_lon, min_lat, max_lon, max_lat] = coords else { return Err("Invalid bbox") };
    if !query_server::coords_in_range(min_lat, min_lon)
        || !query_server::coords_in_range(max_lat, max_lon)
        || min_lon > max_lon
        || min_lat > max_lat
    {
        return Err("Invalid bbox");
    }
    Ok(Viewbox::Bounded(BBox { min_lat, min_lon, max_lat, max_lon }))
}

fn list(value: &str) -> impl Iterator<Item = &str> {
    value.split(',').map(str::trim).filter(|item| !item.is_empty())
}

pub fn countries(value: &str) -> Result<BTreeSet<[u8; 2]>, &'static str> {
    list(value)
        .map(|item| match *item.as_bytes() {
            [a, b] if a.is_ascii_alphabetic() && b.is_ascii_alphabetic() => Ok([a.to_ascii_lowercase(), b.to_ascii_lowercase()]),
            _ => Err("Invalid countrycodes"),
        })
        .collect()
}

pub fn layers(value: &str) -> Result<Option<BTreeSet<Layer>>, RequestError> {
    let layers = list(value).map(str::parse).collect::<Result<BTreeSet<_>, _>>()?;
    Ok((!layers.is_empty()).then_some(layers))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn language_preferences_use_weights_and_keep_explicit_base_language_positions() {
        assert_eq!(
            languages("de-AT;q=0.9,en;q=0.6,de;q=0.4,cs-CZ,fr", LanguageInput::Explicit).unwrap(),
            ["cs-CZ", "cs", "fr", "de-AT", "en", "de"]
        );
        assert_eq!(languages("en-US,*,de;q=bad,fr; Q = 0.5", LanguageInput::Header).unwrap(), ["en-US", "en", "fr"]);
        assert_eq!(languages("de,*", LanguageInput::Explicit), Err("Invalid lang"));
        assert_eq!(languages("de;q=.8", LanguageInput::Explicit), Err("Invalid lang"));
        assert_eq!(languages("de;q=1.", LanguageInput::Explicit), Err("Invalid lang"));
        assert_eq!(languages("", LanguageInput::Explicit).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn bias_defaults_and_filter_values_are_canonical() {
        assert_eq!(bias(Some(0.0), Some(1.0), None, None), Ok(Some(Bias::at(0.0, 1.0))));
        assert_eq!(bias(Some(-90.0), Some(180.0), Some(0.0), Some(0.0)), Ok(Some(Bias { lat: -90.0, lon: 180.0, zoom: 0, scale: 0.0 })));
        assert_eq!(bias(None, None, None, None), Ok(None));
        assert_eq!(countries("DE, us, de,").unwrap(), [*b"de", *b"us"].into());
        assert_eq!(layers("Address, poi, address, ").unwrap(), Some([Layer::Address, Layer::Poi].into()));
        assert_eq!(layers(" , ").unwrap(), None);
        assert_eq!(bbox(&[1.0, 2.0, 3.0, 4.0]).unwrap(), Viewbox::Bounded(BBox { min_lon: 1.0, min_lat: 2.0, max_lon: 3.0, max_lat: 4.0 }));
    }
}
