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
    pub fn single(search: PlaceSearch) -> Self {
        Self { regions: vec![(None, search)] }
    }

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
            let (search, opened) = open(geo.clone(), geo.search_stamps.as_ref().map_err(Clone::clone)?, &dataset, &output)
                .map_err(|e| format!("{}: {e}", slot.name))?;
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
        let regions: Vec<_> = self.regions.iter().filter(|(region, _)| matches_region(*region, req.viewbox)).collect();
        match regions.as_slice() {
            [] => return Ok((Vec::new(), QueryStats::default())),
            [(_, search)] => return search.search(req),
            _ => {}
        }

        let mut found = Vec::new();
        let mut stats = QueryStats::default();
        for (_, search) in regions {
            // A hit suppressed by a neighbour in one region can survive the merged dedupe
            // when that neighbour is itself suppressed by a better hit from another region.
            let (mut candidates, work) = search.candidates(req);
            stats += work;
            found.append(&mut candidates);
        }
        found.sort_by(|a, b| b.hit.score.total_cmp(&a.hit.score));
        let mut kept = SameName::default();
        let results = found.into_iter().filter(|r| kept.keep(&r.name, (r.lat, r.lng))).take(req.limit).collect();
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
        east.write(&asia);
        let mut west = Geo::default();
        west.place("Testville", 0, 39.2, 45.0);
        west.write(&europe);

        let index = load(&dir.0);
        let (search, origin) = MultiSearch::open(&index, None, &BuildOnDevice::default()).unwrap();
        assert_eq!(origin, Origin::Built);
        let (found, _) = search.search(&new_request("Testville", 2)).unwrap();
        assert_eq!(found.iter().map(|r| r.lat).collect::<Vec<_>>(), [39.2, 38.8]);
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
