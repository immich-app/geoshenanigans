//! Node native bindings for the geocoder query engine, for embedding
//! reverse geocoding and place search in-process (no sidecar, no HTTP).
//! The indexes stay memory-mapped: loading costs a few MiB of RSS, lookups
//! fault pages from the OS cache on the calling thread. A dataset's
//! transport units decode into its files through `createTransportDecoder`,
//! fed as they download.

use napi::bindgen_prelude::*;
use napi::Env;
use napi_derive::napi;
use place_search::constants::{DATASET_SEARCH_DIR, DEFAULT_LIMIT};
use place_search::dataset::{self, BuildOnDevice, Origin, Stamps};
use place_search::query::Searcher;
use place_search::reader::SearchIndex;
use query_server::transport::{self, ChunkReader, Expected};
use query_server::Index;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::thread::JoinHandle;

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
    pub fn open_place_search<'env>(&self, env: &'env Env, options: Option<PlaceSearchOptions>) -> Result<Object<'env>> {
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
/// resolves. A unit that fails (cut short, refused, a file of another sha256, one that can't go
/// in place) leaves the dir as it was; a download that broke off should still `finish`, which
/// then rejects. Decoders of one unit into a dir at once each put complete files in place, and
/// one first finishes what decoders killed there left (transport::decode_into_dir).
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
