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

// On wasm32, `usize` is 32 bits — files >4 GiB can't be addressed with
// it.  All offsets and lengths exposed here are `u64` so the planet's
// 7.7 GiB geo_cells / 5 GiB addr_vertices files address correctly.
// Internal page-offset bookkeeping uses u64 as well.
pub struct JsChunked {
    js_read: js_sys::Function,
    handle: f64,
    file_len: u64,
    cache: RefCell<HashMap<u64, Vec<u8>>>,
    lru: RefCell<Vec<u64>>,
}

impl JsChunked {
    pub fn new(js_read: js_sys::Function, handle: f64, file_len: u64) -> Self {
        Self {
            js_read,
            handle,
            file_len,
            cache: RefCell::new(HashMap::new()),
            lru: RefCell::new(Vec::new()),
        }
    }

    pub fn len(&self) -> u64 { self.file_len }

    fn fetch_page(&self, page_off: u64) -> Vec<u8> {
        let want = std::cmp::min(PAGE_SIZE as u64, self.file_len - page_off) as usize;
        let res = self.js_read.call3(
            &JsValue::NULL,
            &JsValue::from_f64(self.handle),
            &JsValue::from_f64(page_off as f64),
            &JsValue::from_f64(want as f64),
        ).expect("JS read callback failed");
        let arr = js_sys::Uint8Array::from(res);
        arr.to_vec()
    }

    fn ensure_page(&self, page_off: u64) {
        let mut cache = self.cache.borrow_mut();
        if cache.contains_key(&page_off) {
            let mut lru = self.lru.borrow_mut();
            if let Some(p) = lru.iter().position(|&p| p == page_off) {
                lru.remove(p);
            }
            lru.push(page_off);
            return;
        }
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
    pub fn read(&self, off: u64, len: usize) -> Vec<u8> {
        let end = off + len as u64;
        let mut out = Vec::with_capacity(len);
        let mut cur = off;
        while cur < end {
            let page_off = cur & !((PAGE_SIZE as u64) - 1);
            self.ensure_page(page_off);
            let cache = self.cache.borrow();
            let page = cache.get(&page_off).expect("page just fetched");
            let in_page = (cur - page_off) as usize;
            let take = std::cmp::min((end - cur) as usize, page.len() - in_page);
            out.extend_from_slice(&page[in_page..in_page + take]);
            cur += take as u64;
            if take == 0 { break; }
        }
        out
    }
}
