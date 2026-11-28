use super::{number, small};
use crate::constants::*;
use std::{cmp::Reverse, collections::BinaryHeap};

#[derive(Clone, Copy)]
pub struct PostingList<'a> {
    pub count: u32,
    pub addr_count: u32,
    pub df: u32,
    skips: &'a [u8],
    deltas: &'a [u8],
    head: &'a [u8],
    singleton: Option<u32>,
    entities: u32,
}
impl<'a> PostingList<'a> {
    pub(super) fn read(bytes: &'a [u8], value: u64, entities: u32) -> Option<Self> {
        if value & INLINE_POSTING != 0 {
            let id = u32::try_from(value >> 1).ok()?;
            return (id < entities).then_some(Self {
                count: 1,
                addr_count: 0,
                df: 1,
                skips: &[],
                deltas: &[],
                head: &[],
                singleton: Some(id),
                entities,
            });
        }
        let mut at = usize::try_from(value >> 1).ok()?;
        let count = small(bytes, &mut at)?;
        let addr_count = small(bytes, &mut at)?;
        let df = count;
        let n = df / SKIP_EVERY;
        let len = small(bytes, &mut at)? as usize;
        if df > entities || len < df as usize {
            return None;
        }
        let tail = bytes.get(at..)?;
        let (skips, tail) = tail.split_at_checked((n as usize).checked_mul(SKIP_BYTES)?)?;
        let (deltas, tail) = tail.split_at_checked(len)?;
        let head = tail.get(..if df >= TOPK_MIN_DF { 4 * TOPK_PER_TERM } else { 0 })?;
        Some(Self { count, addr_count, df, skips, deltas, head, singleton: None, entities })
    }
    pub fn head(&self) -> impl Iterator<Item = u32> + '_ {
        self.head.chunks_exact(4).filter_map(|b| number(b, 0).filter(|&n| n < self.entities))
    }
    pub fn ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.seek(0)
    }
    /// A skip names the last id of a complete block and the first byte after it.
    pub fn seek(&self, target: u32) -> impl Iterator<Item = u32> + '_ {
        let (mut lo, mut hi) = (0, self.skips.len() / SKIP_BYTES);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if number(self.skips, mid * SKIP_BYTES).is_some_and(|id| id < target) {
                lo = mid + 1
            } else {
                hi = mid
            }
        }
        let (mut last, mut at) = if lo == 0 {
            (0, 0)
        } else {
            let i = (lo - 1) * SKIP_BYTES;
            (number(self.skips, i).unwrap(), number(self.skips, i + 4).unwrap() as usize)
        };
        let mut row = lo as u32 * SKIP_EVERY;
        let mut next = move || {
            if row >= self.df {
                return None;
            }
            let id = if let Some(id) = self.singleton {
                id
            } else {
                let delta = small(self.deltas, &mut at)?;
                if row > 0 && delta == 0 {
                    return None;
                }
                last.checked_add(delta)?
            };
            last = id;
            row += 1;
            (id < self.entities).then_some(id)
        };
        std::iter::from_fn(move || loop {
            let id = next()?;
            if id >= target {
                return Some(id);
            }
        })
        .fuse()
    }
    /// Top eligible entities by their stored importance order. The callback returns the
    /// build-time importance for each eligible id. A selective filter may exhaust the head.
    pub fn best(&self, limit: usize, mut eligible: impl FnMut(u32) -> Option<u32>) -> Vec<u32> {
        if limit == 0 {
            return Vec::new();
        }
        let head: Vec<_> = self.head().collect();
        let mut out: Vec<_> = head.iter().copied().filter(|&id| eligible(id).is_some()).take(limit).collect();
        if out.len() == limit {
            return out;
        }
        let mut seen = head;
        seen.sort_unstable();
        let need = limit - out.len();
        let mut heap = BinaryHeap::new();
        for id in self.ids().filter(|id| seen.binary_search(id).is_err()) {
            if let Some(imp) = eligible(id) {
                heap.push(Reverse((imp, Reverse(id))));
                if heap.len() > need {
                    heap.pop();
                }
            }
        }
        out.extend(heap.into_sorted_vec().into_iter().map(|Reverse((_, Reverse(id)))| id));
        out
    }
}
