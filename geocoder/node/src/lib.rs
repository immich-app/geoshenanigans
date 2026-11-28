//! Node native bindings for the geocoder query engine, for embedding
//! reverse geocoding and place search in-process (no sidecar, no HTTP).
//! The indexes stay memory-mapped: loading costs a few MiB of RSS, lookups
//! fault pages from the OS cache on the calling thread. A dataset's
//! transport units decode into its files through `createTransportDecoder`,
//! fed as they download.

use napi::bindgen_prelude::*;
use napi::Env;
use napi_derive::napi;
use place_search::dataset::{self, BuildOnDevice, Origin};
use place_search::multi::MultiSearch;
use place_search::profile::{Profile, SearchRequest};
use place_search::query::results_json;
use place_search::request_params::{self, LanguageInput};
use query_server::transport::{self, ChunkReader, Expected};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::thread::JoinHandle;

#[napi]
pub struct Geocoder {
    inner: Arc<query_server::MultiIndex>,
    search: Arc<RwLock<Option<Arc<MultiSearch>>>>,
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

pub struct SearchTask {
    search: Arc<MultiSearch>,
    request: SearchRequest,
}

impl Task for SearchTask {
    type Output = String;
    type JsValue = String;

    fn compute(&mut self) -> Result<Self::Output> {
        let (found, _) = self.search.search(&self.request).map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(results_json(&found))
    }

    fn resolve(&mut self, _env: Env, output: Self::JsValue) -> Result<Self::JsValue> {
        Ok(output)
    }
}

#[derive(Default)]
#[napi(object)]
pub struct PlaceSearchOptions {
    /// Override the default `search` directory beside each region's dataset files. An
    /// override for a continental layout holds subdirectories named by region.
    pub index_dir: Option<String>,
    /// Sort memory of a build on this device in MiB (default 16: the build's own memory then fits
    /// a 32 MiB limit; the dataset's mapped files count apart, as `load` says).
    pub build_memory_mb: Option<u32>,
    /// Build threads, one by default.
    pub threads: Option<f64>,
    /// The search profile; 'immich' (the default) is the one the binding offers.
    pub profile: Option<String>,
}

#[derive(Default)]
#[napi(object)]
pub struct SearchPlacesOptions {
    pub lang: Option<String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub zoom: Option<f64>,
    /// A box in minLon, minLat, maxLon, maxLat order.
    pub bbox: Option<Vec<f64>>,
    /// Comma-separated ISO alpha-2 country codes.
    pub countrycodes: Option<String>,
    /// Comma-separated layer names.
    pub layer: Option<String>,
}

impl SearchPlacesOptions {
    fn into_request(self, query: String, limit: Option<u32>) -> Result<SearchRequest> {
        let mut req = SearchRequest::new(query, Profile::Immich);
        if let Some(limit) = limit {
            req.limit = limit as usize;
        }
        req.langs = request_params::languages(self.lang.as_deref().unwrap_or_default(), LanguageInput::Explicit).map_err(Error::from_reason)?;
        req.bias = request_params::bias(self.lat, self.lon, self.zoom, None).map_err(Error::from_reason)?;
        req.viewbox = self.bbox.as_deref().map(request_params::bbox).transpose().map_err(Error::from_reason)?;
        req.countries = request_params::countries(self.countrycodes.as_deref().unwrap_or_default()).map_err(Error::from_reason)?;
        req.layers = request_params::layers(self.layer.as_deref().unwrap_or_default()).map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(req)
    }
}

#[napi]
impl Geocoder {
    /// Load an index directory and its optional `water/` layer. Index data is memory-mapped;
    /// opening a water layer also reads its metadata, validates its cell index and checks that
    /// its cut arcs cover its arc slots. A memory limit counts the mapped pages lookups touch
    /// and their page tables: under one of 32 MiB the planet's datasets answered (2026-11), but
    /// `full` with the `all-exact` water tier, whose page tables outgrew it part way through
    /// 12,265 lookups; `full` with `all` stayed within it.
    #[napi(factory)]
    pub fn load(dir: String) -> Result<Geocoder> {
        let (idx, _) = dataset::load(Path::new(&dir), || {
            query_server::MultiIndex::load(
                &dir,
                query_server::DEFAULT_STREET_CELL_LEVEL,
                query_server::DEFAULT_ADMIN_CELL_LEVEL,
                query_server::DEFAULT_SEARCH_DISTANCE,
            )
        })
        .map_err(Error::from_reason)?;
        Ok(Geocoder { inner: Arc::new(idx), search: Arc::default() })
    }

    /// Lookup on the libuv worker pool — cold-cache page faults never
    /// block the event loop. Resolves to the address JSON, `{}` for a point off the globe; its
    /// `water` (last, `{name, class, type}`) names the water holding the point, where the
    /// dataset's `water/` dir holds some, and carries `landmark: true` last where the location
    /// line names that water in its landmark's place (geocoder/README.md).
    #[napi(ts_return_type = "Promise<string>")]
    pub fn reverse_geocode(&self, lat: f64, lng: f64) -> AsyncTask<QueryTask> {
        AsyncTask::new(QueryTask { inner: self.inner.clone(), lat, lng })
    }

    /// Opens place search for this dataset: the index in `indexDir` when it was built from these
    /// files (the one a pack ships, or one built here before), else builds it there on this
    /// device first, on a thread of its own (seconds to minutes, its sort within
    /// `buildMemoryMb`). After a patch, load the patched dataset and open place search on that
    /// geocoder: this one rejects files replaced since it loaded them. Resolves to "built" if
    /// any region needed a build, or "current" if all were reused. The binding offers 'immich'.
    #[napi(ts_return_type = "Promise<'current' | 'built'>")]
    pub fn open_place_search<'env>(&self, env: &'env Env, options: Option<PlaceSearchOptions>) -> Result<Object<'env>> {
        let PlaceSearchOptions { index_dir, build_memory_mb, threads, profile } = options.unwrap_or_default();
        require_immich(profile.as_deref())?;
        let mut on_device = BuildOnDevice::default();
        if let Some(threads) = threads {
            if !(1.0..=f64::from(u32::MAX)).contains(&threads) || threads.fract() != 0.0 {
                return Err(Error::from_reason("Invalid threads"));
            }
            on_device.threads = threads as usize;
        }
        if let Some(mem_mb) = build_memory_mb {
            on_device.mem_mb = mem_mb as usize;
        }
        let dataset = self.inner.clone();
        let index_dir = index_dir.map(PathBuf::from);
        let slot = self.search.clone();
        let (deferred, promise) = env.create_deferred()?;
        std::thread::spawn(move || {
            let opened = MultiSearch::open(&dataset, index_dir.as_deref(), &on_device).map(|(search, origin)| {
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
    /// best first, at most `limit` of them (1 to 50, 20 without one; another limit rejects).
    #[napi(ts_return_type = "Promise<string>")]
    pub fn search_places(&self, query: String, limit: Option<u32>, options: Option<SearchPlacesOptions>) -> Result<AsyncTask<SearchTask>> {
        let search = self.search.read().unwrap().clone().ok_or_else(|| Error::from_reason("Place search is not open"))?;
        let request = options.unwrap_or_default().into_request(query, limit)?;
        Ok(AsyncTask::new(SearchTask { search, request }))
    }
}

/// Fails unless `profile` is none or the immich profile: the full profile's data never ships, so
/// the binding searches with Immich's alone.
fn require_immich(profile: Option<&str>) -> Result<()> {
    let Some(profile) = profile else {
        return Ok(());
    };

    match profile.parse::<Profile>().map_err(|e| Error::from_reason(e.to_string()))? {
        Profile::Immich => Ok(()),
        other => Err(Error::from_reason(format!("Profile not offered: {}", other.as_str()))),
    }
}

/// A file a transport unit is to decode to: its name in the dataset dir and its sha256, as the
/// build's configurations.json gives them (`files[path].sha256` of each path the unit's `files`
/// list).
#[napi(object)]
pub struct TransportFile {
    pub name: String,
    pub sha256: String,
}

/// How many chunks a decoder holds before a write waits for it.
const QUEUED_CHUNKS: usize = 8;

/// A transport unit's decode into a dataset dir, fed the unit (the .tt.zst as published) chunk
/// by chunk as it downloads: `write` each chunk, awaiting each before the next, then `finish`.
/// It decodes on a thread of its own, holding a few chunks, the zstd window and one chunk of the
/// unit's columns, and writes the files in a dir of its own in the dataset dir; once the whole
/// unit decoded and every file is of its sha256, it moves them all into place and `finish`
/// resolves. On unix a decoder takes the dir's install lock before it starts and again before it
/// moves its files in, each time first finishing what decoders killed in the dir left, so that
/// decoders move their files in one at a time and one killed part way is finished before any
/// later one moves its files in; meanwhile it decodes as others do. Finishing one killed while it
/// moved its files in puts back each file it replaced, but a file it moved in that replaced none
/// stays, complete and of its sha256: the dir then holds some of that unit's files without the
/// rest, until the unit is decoded into it again or those files are removed. A unit that fails
/// (cut short, refused, a file of another sha256, one that can't go in place, a killed decoder's
/// dir that can't be finished and would put a file back over one of its files) leaves the dir as
/// it was once those were finished; a download that broke off should still `finish`, which then
/// rejects.
/// Decoders of one unit into a dir at once on one machine each put complete files in place, the
/// later's standing. On a file system without locks decoders take none, and finish nothing; a
/// dir that several machines share (NFS, SMB) is to be decoded into from one at a time
/// (transport::decode_into_dir).
#[napi]
pub struct TransportDecoder {
    /// The channel to the decode, here between writes; a write holds it until it settles.
    sender: Arc<Mutex<Option<SyncSender<Vec<u8>>>>>,
    finished: AtomicBool,
    decode: Mutex<Option<JoinHandle<std::result::Result<Vec<String>, String>>>>,
    /// Why the decode stopped, set before its channel closes.
    failed: Arc<OnceLock<String>>,
}

/// Starts the decode of a transport unit into `dir`, an existing dir, which must decode to
/// `files`: each by its name and sha256, in any order.
#[napi]
pub fn create_transport_decoder(dir: String, files: Vec<TransportFile>) -> Result<TransportDecoder> {
    let dir = PathBuf::from(dir);
    if !dir.is_dir() {
        return Err(Error::from_reason(format!("{} is no directory", dir.display())));
    }
    let expect: Vec<Expected> = files.into_iter().map(|f| Expected { name: f.name, sha256: f.sha256.to_ascii_lowercase(), size: None }).collect();
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(QUEUED_CHUNKS);
    let failed = Arc::new(OnceLock::new());
    let failure = failed.clone();
    let decode = std::thread::spawn(move || {
        let mut unit = ChunkReader::new(rx);
        let decoded = transport::decode_into_dir(&mut unit, &dir, Some(&expect))
            .map(|(_, members)| members.into_iter().map(|m| m.name).collect())
            .map_err(|e| e.to_string());
        if let Err(e) = &decoded {
            let _ = failure.set(e.clone());
        }
        // The channel closes only now, so a write it fails tells why.
        drop(unit);
        decoded
    });
    Ok(TransportDecoder { sender: Arc::new(Mutex::new(Some(tx))), finished: AtomicBool::new(false), decode: Mutex::new(Some(decode)), failed })
}

pub struct WriteTask {
    sender: Arc<Mutex<Option<SyncSender<Vec<u8>>>>>,
    tx: Option<SyncSender<Vec<u8>>>,
    chunk: Vec<u8>,
    failed: Arc<OnceLock<String>>,
}

impl WriteTask {
    /// The channel goes back to its decoder on the JS thread, once the write settles: a write
    /// before then finds none.
    fn settle(&mut self) {
        *self.sender.lock().unwrap() = self.tx.take();
    }
}

impl Task for WriteTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        let tx = self.tx.as_ref().expect("a write runs once");
        tx.send(std::mem::take(&mut self.chunk))
            .map_err(|_| Error::from_reason(self.failed.get().cloned().unwrap_or_else(|| "The unit's decode stopped".to_string())))
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        self.settle();
        Ok(())
    }

    fn reject(&mut self, _env: Env, err: Error) -> Result<Self::JsValue> {
        self.settle();
        Err(err)
    }
}

pub struct FinishTask {
    decode: Option<JoinHandle<std::result::Result<Vec<String>, String>>>,
}

impl Task for FinishTask {
    type Output = Vec<String>;
    type JsValue = Vec<String>;

    fn compute(&mut self) -> Result<Self::Output> {
        let decode = self.decode.take().expect("a finish runs once");
        decode.join().map_err(|_| Error::from_reason("The unit's decode panicked"))?.map_err(Error::from_reason)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(output)
    }
}

#[napi]
impl TransportDecoder {
    /// Hands the decode the unit's next bytes, on the libuv worker pool: resolves once it has room
    /// for them, rejects once the decode has failed (with why). One write at a time.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn write(&self, chunk: Buffer) -> Result<AsyncTask<WriteTask>> {
        let tx = self.take_sender()?;
        Ok(AsyncTask::new(WriteTask { sender: self.sender.clone(), tx: Some(tx), chunk: chunk.to_vec(), failed: self.failed.clone() }))
    }

    /// Ends the unit: resolves to its files' names, in the unit's order, once they are all in
    /// place; rejects with why the decode failed, leaving none of them.
    #[napi(ts_return_type = "Promise<string[]>")]
    pub fn finish(&self) -> Result<AsyncTask<FinishTask>> {
        drop(self.take_sender()?);
        self.finished.store(true, Ordering::SeqCst);
        Ok(AsyncTask::new(FinishTask { decode: self.decode.lock().unwrap().take() }))
    }

    fn take_sender(&self) -> Result<SyncSender<Vec<u8>>> {
        if self.finished.load(Ordering::SeqCst) {
            return Err(Error::from_reason("The decoder is finished"));
        }
        self.sender.lock().unwrap().take().ok_or_else(|| Error::from_reason("A write is still running: await each write before the next"))
    }
}
