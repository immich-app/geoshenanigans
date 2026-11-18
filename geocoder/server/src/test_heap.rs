//! The system allocator for the crate's tests, counting each thread's allocations and the heap
//! it holds: the tests that bound what a read allocates or holds measure their own thread.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    static LIVE: Cell<i64> = const { Cell::new(0) };
    static PEAK: Cell<i64> = const { Cell::new(0) };
}

struct Counting;

/// An allocation of `grow` bytes (negative: a release) on this thread.
fn count(new: bool, grow: i64) {
    if new {
        let _ = ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
    }
    let _ = LIVE.try_with(|live| {
        live.set(live.get() + grow);
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(live.get())));
    });
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(true, layout.size() as i64);
        System.alloc(layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count(true, layout.size() as i64);
        System.alloc_zeroed(layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count(true, new_size as i64 - layout.size() as i64);
        System.realloc(ptr, layout, new_size)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        count(false, -(layout.size() as i64));
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;

/// This thread's allocations so far.
pub fn allocations() -> u64 {
    ALLOCATIONS.with(|n| n.get())
}

/// Starts a new peak at the heap this thread holds now.
pub fn reset_peak() {
    PEAK.with(|p| p.set(LIVE.with(|l| l.get())));
}

/// The most heap this thread held since `reset_peak`, beyond what it held then (`held`).
pub fn peak_since(held: i64) -> i64 {
    PEAK.with(|p| p.get()) - held
}

/// The heap this thread holds now, from its allocations less its releases.
pub fn held() -> i64 {
    LIVE.with(|l| l.get())
}
