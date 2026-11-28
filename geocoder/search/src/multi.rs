//! Search the installed regions of a dataset as one ranked answer.
use std::path::Path;
use std::sync::Arc;

use query_server::{Continent, Index, MultiIndex};

use crate::constants::DATASET_SEARCH_DIR;
use crate::dataset::{self, BuildOnDevice, Origin, PlaceSearch, Stamps};
use crate::index::entities::BBox;
use crate::profile::{RequestError, SearchRequest, Viewbox};
use crate::query::{Found, QueryStats, SameName};

pub struct MultiSearch {
    regions: Vec<(Option<&'static Continent>, PlaceSearch)>,
}

impl MultiSearch {
    /// Open the current snapshot of the loaded regions. A flat layout uses the index directory
    /// itself; regional layouts have a separate index per region, under the override when set.
    pub fn open(index: &MultiIndex, index_dir: Option<&Path>, options: &BuildOnDevice) -> Result<(Self, Origin), String> {
        Self::open_with(index, index_dir, |geo, stamps, dataset, output| PlaceSearch::open(geo, stamps, dataset, output, options))
    }

    pub fn open_with(
        index: &MultiIndex,
        index_dir: Option<&Path>,
        open: impl Fn(Arc<Index>, &Stamps, &Path, &Path) -> Result<(PlaceSearch, Origin), String>,
    ) -> Result<(Self, Origin), String> {
        let root = Path::new(&index.root_dir);
        let mut regions = Vec::new();
        let mut origin = Origin::Current;
        for slot in &index.continents {
            let Some(geo) = slot.index.load_full() else { continue };
            let dataset = match slot.continent {
                Some(_) => root.join(slot.name),
                None => root.to_path_buf(),
            };
            let output = match (index_dir, slot.continent) {
                (Some(dir), Some(_)) => dir.join(slot.name),
                (Some(dir), None) => dir.to_path_buf(),
                (None, _) => dataset.join(DATASET_SEARCH_DIR),
            };
            let (search, opened) = geo
                .search_stamps
                .as_ref()
                .map_err(Clone::clone)
                .and_then(|stamps| open(geo.clone(), stamps, &dataset, &output))
                .map_err(|e| if slot.continent.is_some() { format!("{}: {e}", slot.name) } else { e })?;
            if opened == Origin::Built {
                origin = Origin::Built;
            }
            regions.push((slot.continent, search));
        }
        if regions.is_empty() {
            return Err("Dataset not loaded".to_string());
        }
        Ok((Self { regions }, origin))
    }

    pub fn require_supported(&self, req: &SearchRequest) -> Result<(), RequestError> {
        dataset::require_supported(req)?;
        req.require_limit()
    }

    pub fn search(&self, req: &SearchRequest) -> Result<(Vec<Found<'_>>, QueryStats), RequestError> {
        self.require_supported(req)?;
        let regions: Vec<_> = self
            .regions
            .iter()
            .enumerate()
            .filter_map(|(region, (boundary, search))| matches_region(*boundary, req.viewbox).then_some((region, search)))
            .collect();
        match regions.as_slice() {
            [] => return Ok((Vec::new(), QueryStats::default())),
            [(_, search)] => return search.search(req),
            _ => {}
        }

        let mut candidates = Vec::new();
        let mut stats = QueryStats::default();
        for (region, search) in regions {
            // A hit suppressed by a neighbour in one region can survive the merged dedupe
            // when that neighbour is itself suppressed by a better hit from another region.
            let (hits, work) = search.candidates(req);
            stats += work;
            candidates.extend(hits.into_iter().map(|hit| (region, hit)));
        }
        candidates.sort_by(|(_, a), (_, b)| b.hit.score.total_cmp(&a.hit.score));
        let mut kept = SameName::default();
        let results = candidates
            .into_iter()
            .filter(|(_, hit)| kept.keep(&hit.name, hit.point))
            .take(req.limit)
            .map(|(region, hit)| self.regions[region].1.describe_candidate(hit, &mut stats))
            .collect();
        Ok((results, stats))
    }
}

fn matches_region(region: Option<&Continent>, viewbox: Option<Viewbox>) -> bool {
    let (Some(region), Some(Viewbox::Bounded(BBox { min_lat, min_lon, max_lat, max_lon }))) = (region, viewbox) else { return true };
    let ((south, west), (north, east)) = region.bounds();
    south <= max_lat && north >= min_lat && west <= max_lon && east >= min_lon
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{new_request, Geo, TempDir};
    use query_server::{DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL};

    fn load(dir: &Path) -> MultiIndex {
        MultiIndex::load(dir.to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).unwrap()
    }

    #[test]
    fn continent_search_ranks_then_deduplicates_the_union() {
        let dir = TempDir::new("continent-union");
        let europe = dir.0.join("europe");
        let asia = dir.0.join("asia");
        let mut east = Geo::default();
        east.place("Testville", 1, 39.0, 45.0);
        east.place("Testville", 4, 38.8, 45.0);
        let country = east.boundary("Eastland", 2, [38.0, 44.0, 40.0, 46.0]);
        east.country(country, *b"de");
        east.boundary("Eaststate", 4, [38.2, 44.2, 39.8, 45.8]);
        east.boundary("Eastcounty", 6, [38.4, 44.4, 38.9, 45.6]);
        east.write(&asia);
        let mut west = Geo::default();
        west.place("Testville", 0, 39.2, 45.0);
        let country = west.boundary("Westland", 2, [38.0, 44.0, 40.0, 46.0]);
        west.country(country, *b"us");
        west.boundary("Weststate", 4, [38.2, 44.2, 39.8, 45.8]);
        west.boundary("Westcounty", 6, [38.4, 44.4, 39.6, 45.6]);
        west.write(&europe);

        let index = load(&dir.0);
        let (search, origin) = MultiSearch::open(&index, None, &BuildOnDevice::default()).unwrap();
        assert_eq!(origin, Origin::Built);
        let (found, _) = search.search(&new_request("Testville", 2)).unwrap();
        assert_eq!(found.iter().map(|r| r.lat).collect::<Vec<_>>(), [39.2, 38.8]);

        for (result, (region, query, admin)) in found.iter().zip([
            ("europe", "Testville", ["Westland", "US", "Weststate", "Westcounty"]),
            ("asia", "Testville, Eastcounty", ["Eastland", "DE", "Eaststate", "Eastcounty"]),
        ]) {
            let (_, local) = search.regions.iter().find(|(c, _)| c.is_some_and(|c| c.name == region)).unwrap();
            let (local, _) = local.search(&new_request(query, 1)).unwrap();
            let local = &local[0];
            assert_eq!(result.hit.entity, local.hit.entity, "{region}");
            let got = result.to_json();
            assert_eq!(got, local.to_json(), "{region}");
            for (field, want) in ["country", "country_code", "admin1name", "admin2name"].into_iter().zip(admin) {
                assert_eq!(got[field], want, "{region} {field}");
            }
        }
    }

    #[test]
    fn a_merged_search_looks_up_display_context_only_for_its_results() {
        let dir = TempDir::new("continent-context");
        let limit = 2;
        for (name, lat, lon) in [("europe", 50.0, 14.0), ("asia", 35.0, 90.0)] {
            let mut geo = Geo::default();
            for offset in 0..=limit {
                geo.place("Testville", 1, lat + offset as f32, lon);
            }
            geo.write(&dir.0.join(name));
        }

        let index = load(&dir.0);
        let (search, _) = MultiSearch::open(&index, None, &BuildOnDevice::default()).unwrap();
        let req = new_request("Testville", limit);
        for (_, region) in &search.regions {
            assert!(region.candidates(&req).0.len() > limit);
        }

        let (found, stats) = search.search(&req).unwrap();
        assert_eq!(found.len(), limit);
        assert_eq!(stats.reverse_lookups, found.len());
    }

    #[test]
    fn a_flat_search_open_preserves_the_error_text() {
        let dir = TempDir::new("flat-search-open-error");
        Geo::default().write(&dir.0);
        let index = load(&dir.0);

        let reason = "No room for sort runs";
        let error = MultiSearch::open_with(&index, None, |_, _, _, _| Err(reason.to_string())).err().unwrap();

        assert_eq!(error, reason);
    }

    #[test]
    fn bounded_viewboxes_select_overlapping_region_bounds() {
        let europe = query_server::continents().iter().find(|c| c.name == "europe").unwrap();
        let america = query_server::continents().iter().find(|c| c.name == "north-america").unwrap();
        let bbox = BBox { min_lat: 30.2, min_lon: -97.8, max_lat: 30.4, max_lon: -97.6 };
        assert!(!matches_region(Some(europe), Some(Viewbox::Bounded(bbox))));
        assert!(matches_region(Some(america), Some(Viewbox::Bounded(bbox))));
        assert!(matches_region(Some(europe), Some(Viewbox::Preferred(bbox))));
        assert!(matches_region(None, Some(Viewbox::Bounded(bbox))));
    }

    #[cfg(unix)]
    #[test]
    fn a_region_replaced_during_install_can_reverse_but_refuses_search_until_reinstalled() {
        use query_server::arc_fixture::after_next_index_mapping;
        use query_server::place_files::NODES_FILE;

        let dir = TempDir::new("continent-install-race");
        let index = load(&dir.0);
        let europe = dir.0.join("europe");
        let (lat, lon) = (52.5, 13.4);
        let mut geo = Geo::default();
        geo.place("Testville", 1, lat, lon);
        let country = geo.boundary("Testland", 2, [52.0, 13.0, 53.0, 14.0]);
        geo.country(country, *b"tl");
        geo.write(&europe);
        let nodes = europe.join(NODES_FILE);
        let patch = dir.0.join("patch");
        let mut patched = Geo::default();
        patched.place("Testville", 0, lat, lon);
        patched.write(&patch);
        let replacement = patch.join(NODES_FILE);
        after_next_index_mapping(move || std::fs::rename(replacement, nodes).unwrap());

        index.install_region("europe").unwrap();

        let slot = index.continents.iter().find(|slot| slot.name == "europe").unwrap();
        let loaded = slot.index.load_full().expect("Europe was installed");
        assert!(index.query_json(f64::from(lat), f64::from(lon)).contains("Testland"));
        let reason = format!("The dataset in {} changed while it was loading", europe.display());
        assert_eq!(loaded.search_stamps.as_ref().unwrap_err(), &reason);
        let err = MultiSearch::open(&index, None, &BuildOnDevice::default()).err().expect("search refused the changed files");
        assert_eq!(err, format!("europe: {reason}"));

        index.install_region("europe").unwrap();
        let (search, origin) = MultiSearch::open(&index, None, &BuildOnDevice::default()).unwrap();
        assert_eq!(origin, Origin::Built);
        let (found, _) = search.search(&new_request("Testville", 1)).unwrap();
        assert_eq!((&*found[0].name, &*found[0].typ), ("Testville", "city"));
    }

    #[test]
    fn reopening_search_uses_the_reinstalled_continents_snapshot() {
        let dir = TempDir::new("continent-reinstall");
        let europe = dir.0.join("europe");
        let mut geo = Geo::default();
        geo.place("Oldtown", 1, 52.5, 13.4);
        geo.write(&europe);
        let index = load(&dir.0);
        let (old, _) = MultiSearch::open(&index, None, &BuildOnDevice::default()).unwrap();

        let mut geo = Geo::default();
        geo.place("Newtown", 1, 52.5, 13.4);
        geo.write(&europe);
        assert!(MultiSearch::open(&index, None, &BuildOnDevice::default()).is_err());
        index.install_region("europe").unwrap();
        let (new, origin) = MultiSearch::open(&index, None, &BuildOnDevice::default()).unwrap();
        assert_eq!(origin, Origin::Built);
        assert_eq!(new.search(&new_request("Newtown", 1)).unwrap().0[0].name, "Newtown");
        assert_eq!(old.search(&new_request("Oldtown", 1)).unwrap().0[0].name, "Oldtown");
    }
}
