//! Wasm-only chunk-on-demand backing store.  Calls a JS function for
//! every page miss; an internal LRU page cache absorbs hot reads so
//! most lookups don't cross the JS↔wasm boundary.
//!
//! The JS function signature: `(handle: number, off: number, len: number) -> Uint8Array`.
//! `handle` is an opaque identifier the JS side uses to know which
//! file to read from; the constructor takes that handle from JS.

use std::cell::RefCell;
use std::collections::HashMap;

use wasm_bindgen::prelude::*;

const PAGE_SIZE: usize = 64 * 1024; // 64 KiB pages
const MAX_PAGES: usize = 4096;       // 256 MiB cache cap

pub struct JsChunked {
    js_read: js_sys::Function,
    handle: f64,
    file_len: usize,
    cache: RefCell<HashMap<usize, Vec<u8>>>,
    lru: RefCell<Vec<usize>>,
}

impl JsChunked {
    pub fn new(js_read: js_sys::Function, handle: f64, file_len: usize) -> Self {
        Self {
            js_read,
            handle,
            file_len,
            cache: RefCell::new(HashMap::new()),
            lru: RefCell::new(Vec::new()),
        }
    }

    pub fn len(&self) -> usize { self.file_len }

    fn fetch_page(&self, page_off: usize) -> Vec<u8> {
        let want = std::cmp::min(PAGE_SIZE, self.file_len - page_off);
        let res = self.js_read.call3(
            &JsValue::NULL,
            &JsValue::from_f64(self.handle),
            &JsValue::from_f64(page_off as f64),
            &JsValue::from_f64(want as f64),
        ).expect("JS read callback failed");
        let arr = js_sys::Uint8Array::from(res);
        arr.to_vec()
    }

    fn ensure_page(&self, page_off: usize) {
        let mut cache = self.cache.borrow_mut();
        if cache.contains_key(&page_off) {
            // promote to MRU
            let mut lru = self.lru.borrow_mut();
            if let Some(p) = lru.iter().position(|&p| p == page_off) {
                lru.remove(p);
            }
            lru.push(page_off);
            return;
        }
        // Evict LRU if cache full.
        if cache.len() >= MAX_PAGES {
            let mut lru = self.lru.borrow_mut();
            if let Some(&old) = lru.first() {
                lru.remove(0);
                cache.remove(&old);
            }
        }
        drop(cache);
        let page = self.fetch_page(page_off);
        let mut cache = self.cache.borrow_mut();
        cache.insert(page_off, page);
        self.lru.borrow_mut().push(page_off);
    }

    /// Read `len` bytes starting at `off`.  Spans pages as needed.
    /// Always returns an owned Vec — the chunk cache pages may be
    /// evicted before the caller is done.
    pub fn read(&self, off: usize, len: usize) -> Vec<u8> {
        let end = off + len;
        let mut out = Vec::with_capacity(len);
        let mut cur = off;
        while cur < end {
            let page_off = cur & !(PAGE_SIZE - 1);
            self.ensure_page(page_off);
            let cache = self.cache.borrow();
            let page = cache.get(&page_off).expect("page just fetched");
            let in_page = cur - page_off;
            let take = std::cmp::min(end - cur, page.len() - in_page);
            out.extend_from_slice(&page[in_page..in_page + take]);
            cur += take;
            // page may end early (last page of file); guard in case len
            // overruns end of file.
            if take == 0 { break; }
        }
        out
    }
}
