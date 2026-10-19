//! fstmem <in.fst> <out.fst>: rebuild an fst key by key (keys + values streamed from the mapped
//! input) and report the builder's live heap (registry) at intervals and at the end.
mod memtrace;
use fst::{Map, MapBuilder, Streamer};
use std::io::BufWriter;

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let f = std::fs::File::open(&a[1]).unwrap();
    let m = unsafe { memmap2::Mmap::map(&f).unwrap() };
    let map = Map::new(m).unwrap();
    memtrace::mark("open");
    let mut b = MapBuilder::new(BufWriter::with_capacity(64 << 10, std::fs::File::create(&a[2]).unwrap())).unwrap();
    memtrace::mark("builder");
    let mut s = map.stream();
    let mut n = 0u64;
    while let Some((k, v)) = s.next() {
        b.insert(k, v).unwrap();
        n += 1;
        if n % 5_000_000 == 0 {
            memtrace::mark(&format!("{n} keys"));
        }
    }
    memtrace::mark("all keys");
    b.finish().unwrap();
    memtrace::mark("finished");
}
