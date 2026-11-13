//! Node native bindings for the geocoder query engine, for embedding
//! reverse geocoding and place search in-process (no sidecar, no HTTP).
//! The indexes stay memory-mapped: loading costs a few MiB of RSS, lookups
//! fault pages from the OS cache on the calling thread.

use napi::bindgen_prelude::*;
use napi::{Env, JsObject};
use napi_derive::napi;
use place_search::constants::{DATASET_SEARCH_DIR, DEFAULT_LIMIT};
use place_search::dataset::{self, BuildOnDevice, Origin, Stamps};
use place_search::query::Searcher;
use place_search::reader::SearchIndex;
use query_server::Index;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

#[napi]
pub struct Geocoder {
    inner: Arc<query_server::MultiIndex>,
    dir: PathBuf,
    /// The stamps of the dataset files `inner` was loaded from.
    loaded: Stamps,
    search: Arc<RwLock<Option<Arc<PlaceSearch>>>>,
}

pub struct QueryTask {
    inner: Arc<query_server::MultiIndex>,
    lat: f64,
    lng: f64,
}

impl Task for QueryTask {
    type Output = String;
    type JsValue = String;

    fn compute(&mut self) -> Result<Self::Output> {
        Ok(self.inner.query_json(self.lat, self.lng))
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(output)
    }
}

/// An open place search index and the geocoder index its postings point into.
pub struct PlaceSearch {
    // Fields drop in order: the searcher borrows the index, which borrows the geocoder index.
    searcher: Searcher<'static, 'static>,
    _index: Arc<SearchIndex<'static>>,
    _geo: Arc<Index>,
}

impl PlaceSearch {
    /// `dataset::open` of the index in `index_dir` for `geo`, the dataset in `dir` stamped `loaded`.
    fn open(geo: Arc<Index>, loaded: &Stamps, dir: &Path, index_dir: &Path, on_device: &BuildOnDevice) -> std::result::Result<(PlaceSearch, Origin), String> {
        // SAFETY: the borrows point into the two Arcs' shared allocations, which never move and
        // are held by this struct until after the searcher and the index that borrow them drop.
        let geo_ref: &'static Index = unsafe { &*Arc::as_ptr(&geo) };
        let (index, origin) = dataset::open(geo_ref, loaded, dir, index_dir, on_device)?;
        let index = Arc::new(index);
        let index_ref: &'static SearchIndex<'static> = unsafe { &*Arc::as_ptr(&index) };
        Ok((PlaceSearch { searcher: Searcher::new(index_ref), _index: index, _geo: geo }, origin))
    }
}

pub struct SearchTask {
    search: Arc<PlaceSearch>,
    query: String,
    limit: usize,
}

impl Task for SearchTask {
    type Output = String;
    type JsValue = String;

    fn compute(&mut self) -> Result<Self::Output> {
        let (found, _) = self.search.searcher.search(&self.query, self.limit);
        let results: Vec<String> = found.iter().map(|f| f.to_json().to_string()).collect();
        Ok(format!("[{}]", results.join(",")))
    }

    fn resolve(&mut self, _env: Env, output: Self::JsValue) -> Result<Self::JsValue> {
        Ok(output)
    }
}

#[napi(object)]
pub struct PlaceSearchOptions {
    /// Where the index lives; default `<dataset dir>/search`, where packs that ship one put it.
    pub index_dir: Option<String>,
    /// Sort memory of a build on this device in MiB (default 16: the build then fits a 32 MiB
    /// memory limit).
    pub build_memory_mb: Option<u32>,
}

#[napi]
impl Geocoder {
    /// Load an index directory (mmap; near-instant, ~3 MiB resident).
    #[napi(factory)]
    pub fn load(dir: String) -> Result<Geocoder> {
        let (idx, loaded) = dataset::load(Path::new(&dir), || {
            query_server::MultiIndex::load(
                &dir,
                query_server::DEFAULT_STREET_CELL_LEVEL,
                query_server::DEFAULT_ADMIN_CELL_LEVEL,
                query_server::DEFAULT_SEARCH_DISTANCE,
            )
        })
        .map_err(Error::from_reason)?;
        Ok(Geocoder { inner: Arc::new(idx), dir: PathBuf::from(dir), loaded, search: Arc::default() })
    }

    /// Lookup on the libuv worker pool — cold-cache page faults never
    /// block the event loop. Resolves to the address JSON, `{}` for a point off the globe.
    #[napi(ts_return_type = "Promise<string>")]
    pub fn reverse_geocode(&self, lat: f64, lng: f64) -> AsyncTask<QueryTask> {
        AsyncTask::new(QueryTask { inner: self.inner.clone(), lat, lng })
    }

    /// Opens place search for this dataset: the index in `indexDir` when it was built from these
    /// files (the one a pack ships, or one built here before), else builds it there on this
    /// device first, on a thread of its own (seconds to minutes, within 32 MiB). After a patch,
    /// load the patched dataset and open place search on that geocoder: this one rejects files
    /// replaced since it loaded them. Resolves to "current" or "built".
    #[napi(ts_return_type = "Promise<'current' | 'built'>")]
    pub fn open_place_search(&self, env: Env, options: Option<PlaceSearchOptions>) -> Result<JsObject> {
        let PlaceSearchOptions { index_dir, build_memory_mb } = options.unwrap_or(PlaceSearchOptions { index_dir: None, build_memory_mb: None });
        let geo = self.single_region()?;
        let dataset = self.dir.clone();
        let loaded = self.loaded.clone();
        let index_dir = index_dir.map(PathBuf::from).unwrap_or_else(|| dataset.join(DATASET_SEARCH_DIR));
        let defaults = BuildOnDevice::default();
        let on_device = BuildOnDevice { mem_mb: build_memory_mb.map_or(defaults.mem_mb, |m| m as usize), ..defaults };
        let slot = self.search.clone();
        let (deferred, promise) = env.create_deferred()?;
        std::thread::spawn(move || {
            let opened = PlaceSearch::open(geo, &loaded, &dataset, &index_dir, &on_device).map(|(search, origin)| {
                *slot.write().unwrap() = Some(Arc::new(search));
                origin
            });
            match opened {
                Ok(origin) => deferred.resolve(move |_| Ok(if origin == Origin::Current { "current" } else { "built" })),
                Err(e) => deferred.reject(Error::from_reason(e)),
            }
        });
        Ok(promise)
    }

    /// Place search on the libuv worker pool, once `openPlaceSearch` resolved. Resolves to a JSON
    /// array of { name, latitude, longitude, admin1name, admin2name, country, country_code, type },
    /// best first.
    #[napi(ts_return_type = "Promise<string>")]
    pub fn search_places(&self, query: String, limit: Option<u32>) -> Result<AsyncTask<SearchTask>> {
        let search = self.search.read().unwrap().clone().ok_or_else(|| Error::from_reason("Place search is not open"))?;
        Ok(AsyncTask::new(SearchTask { search, query, limit: limit.map_or(DEFAULT_LIMIT, |l| l as usize) }))
    }

    /// The index of a dataset of one region (a flat dir): place search reads one region's files.
    fn single_region(&self) -> Result<Arc<Index>> {
        match self.inner.continents.as_slice() {
            [slot] if slot.continent.is_none() => slot.index.load_full().ok_or_else(|| Error::from_reason("Dataset not loaded")),
            _ => Err(Error::from_reason("Place search needs the dataset of one region")),
        }
    }
}
