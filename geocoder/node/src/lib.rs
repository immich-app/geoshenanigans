//! Node native bindings for the geocoder query engine, for embedding
//! reverse geocoding in-process (no sidecar, no HTTP). The index stays
//! memory-mapped: loading costs a few MiB of RSS, lookups fault pages
//! from the OS cache on the calling thread.

use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;

#[napi]
pub struct Geocoder {
    inner: Arc<query_server::MultiIndex>,
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

#[napi]
impl Geocoder {
    /// Load an index directory (mmap; near-instant, ~3 MiB resident).
    #[napi(factory)]
    pub fn load(dir: String) -> Result<Geocoder> {
        let idx = query_server::MultiIndex::load(
            &dir,
            query_server::DEFAULT_STREET_CELL_LEVEL,
            query_server::DEFAULT_ADMIN_CELL_LEVEL,
            query_server::DEFAULT_SEARCH_DISTANCE,
        )
        .map_err(|e| Error::from_reason(e))?;
        Ok(Geocoder { inner: Arc::new(idx) })
    }

    /// Blocking lookup on the calling thread. Sub-millisecond warm;
    /// cold pages fault from disk. Returns the address JSON.
    #[napi]
    pub fn reverse_geocode_sync(&self, lat: f64, lng: f64) -> String {
        self.inner.query_json(lat, lng)
    }

    /// Lookup on the libuv worker pool — cold-cache page faults never
    /// block the event loop. Resolves to the address JSON.
    #[napi(ts_return_type = "Promise<string>")]
    pub fn reverse_geocode(&self, lat: f64, lng: f64) -> AsyncTask<QueryTask> {
        AsyncTask::new(QueryTask { inner: self.inner.clone(), lat, lng })
    }
}
