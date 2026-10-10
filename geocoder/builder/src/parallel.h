// Thread-parallel loop and sort for planet-scale arrays. The builder's output
// must be byte-identical however many cores run it, so nothing here may let
// the thread count leak into a result.
#pragma once

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <type_traits>
#include <vector>

inline unsigned parallel_threads() {
    return std::max(1u, std::thread::hardware_concurrency());
}

// Runs fn(begin, end, worker) over at most `threads` contiguous ranges that
// cover [0, n), one thread each (0 = every core). The ranges move with the
// thread count, so fn must not let them shape its output. The first
// exception a worker throws is rethrown here after all workers finish.
template <class Fn>
void parallel_for(size_t n, Fn&& fn, unsigned threads = 0) {
    if (threads == 0) threads = parallel_threads();
    size_t workers = std::min<size_t>(threads, n);
    if (workers <= 1) {
        if (n) fn(size_t(0), n, 0u);
        return;
    }
    std::vector<std::exception_ptr> errors(workers);
    std::vector<std::thread> pool;
    pool.reserve(workers);
    for (size_t w = 0; w < workers; w++) {
        size_t begin = n * w / workers, end = n * (w + 1) / workers;
        pool.emplace_back([&fn, &errors, begin, end, w] {
            try {
                fn(begin, end, static_cast<unsigned>(w));
            } catch (...) {
                errors[w] = std::current_exception();
            }
        });
    }
    for (auto& t : pool) t.join();
    for (auto& e : errors)
        if (e) std::rethrow_exception(e);
}

// parallel_for for uneven per-element cost: workers take the next `grain`
// elements as they free up. Which worker gets which range depends on timing,
// so fn must not let it shape its output.
template <class Fn>
void parallel_for_dynamic(size_t n, size_t grain, Fn&& fn, unsigned threads = 0) {
    if (threads == 0) threads = parallel_threads();
    grain = std::max<size_t>(grain, 1);
    size_t pieces = (n + grain - 1) / grain;
    std::atomic<size_t> next{0};
    parallel_for(std::min<size_t>(threads, pieces), [&](size_t, size_t, unsigned worker) {
        for (size_t p = next++; p < pieces; p = next++)
            fn(p * grain, std::min(n, (p + 1) * grain), worker);
    }, threads);
}

// Lays n variable-size items out back to back on every core, in index
// order: size(i) gives item i's size, alloc(total) runs once when the total
// is known, then place(i, offset) writes item i at `offset` (the sizes of
// the items before it) and returns its size, which must equal size(i).
template <class Size, class Alloc, class Place>
size_t parallel_prefix_fill(size_t n, Size size, Alloc alloc, Place place, unsigned threads = 0) {
    if (threads == 0) threads = parallel_threads();
    std::vector<size_t> base(threads, 0);
    parallel_for(n, [&](size_t begin, size_t end, unsigned w) {
        size_t sum = 0;
        for (size_t i = begin; i < end; i++) sum += size(i);
        base[w] = sum;
    }, threads);
    size_t total = 0;
    for (auto& b : base) {
        size_t range = b;
        b = total;
        total += range;
    }
    alloc(total);
    parallel_for(n, [&](size_t begin, size_t end, unsigned w) {
        size_t offset = base[w];
        for (size_t i = begin; i < end; i++) offset += place(i, offset);
    }, threads);
    return total;
}

// parallel_for whose ranges never split a run: same_run(i) (i >= 1) says
// element i continues the run of element i - 1. A range is cut at the first
// run start at or after its even share, so fn(begin, end, worker) gets whole
// runs; worker numbers ranges in order and stays below `threads`.
template <class SameRun, class Fn>
void parallel_for_runs(size_t n, SameRun same_run, Fn&& fn, unsigned threads = 0) {
    if (threads == 0) threads = parallel_threads();
    size_t workers = std::min<size_t>(threads, n);
    std::vector<size_t> bounds{0};
    for (size_t w = 1; w < workers; w++) {
        size_t b = std::max(n * w / workers, bounds.back());
        while (b < n && same_run(b)) b++;
        if (b > bounds.back() && b < n) bounds.push_back(b);
    }
    bounds.push_back(n);
    size_t ranges = bounds.size() - 1;
    parallel_for(ranges, [&](size_t b, size_t e, unsigned) {
        for (size_t r = b; r < e; r++)
            if (bounds[r] < bounds[r + 1]) fn(bounds[r], bounds[r + 1], static_cast<unsigned>(r));
    }, static_cast<unsigned>(ranges));
}

// Runs fn(i, worker) for every i in [0, n) on at most `threads` threads
// (0 = every core), handing indices out one at a time so uneven items
// balance. Which worker runs which i varies run to run. The first exception
// a worker throws is rethrown here after all workers finish.
template <class Fn>
void parallel_for_each(size_t n, Fn&& fn, unsigned threads = 0) {
    if (threads == 0) threads = parallel_threads();
    std::atomic<size_t> next{0};
    parallel_for(std::min<size_t>(threads, n), [&](size_t, size_t, unsigned worker) {
        for (size_t i; (i = next.fetch_add(1)) < n;) fn(i, worker);
    }, threads);
}

// Runs produce(i) for every i in [0, n) on worker threads and consume(i, r)
// with each result on the calling thread in index order, so the consumer
// sees what a serial loop would. At most `window` results are produced ahead
// of the consumer (0 = two per thread), which bounds their memory. An
// exception from produce(i) or consume(i) stops the pipeline when the
// consumer reaches i and is rethrown here.
template <class Produce, class Consume>
void parallel_ordered(size_t n, Produce&& produce, Consume&& consume, unsigned threads = 0,
                      size_t window = 0) {
    using R = std::decay_t<std::invoke_result_t<Produce&, size_t>>;
    if (threads == 0) threads = parallel_threads();
    if (window == 0) window = size_t(2) * threads;
    size_t workers = std::min<size_t>(threads, n);
    if (workers <= 1) {
        for (size_t i = 0; i < n; i++) consume(i, produce(i));
        return;
    }

    struct Slot {
        std::optional<R> result;
        std::exception_ptr error;
        bool ready = false;
    };
    std::vector<Slot> slots(window);  // index i lives in slot i % window
    std::mutex mtx;
    std::condition_variable produced, consumed;
    size_t next = 0, done = 0;
    bool stop = false;

    auto work = [&] {
        for (;;) {
            size_t i;
            {
                std::unique_lock<std::mutex> lock(mtx);
                consumed.wait(lock, [&] { return stop || next >= n || next < done + window; });
                if (stop || next >= n) return;
                i = next++;
            }
            Slot out;
            try {
                out.result.emplace(produce(i));
            } catch (...) {
                out.error = std::current_exception();
            }
            {
                std::lock_guard<std::mutex> lock(mtx);
                slots[i % window] = std::move(out);
                slots[i % window].ready = true;
            }
            produced.notify_all();
        }
    };
    std::vector<std::thread> pool;
    pool.reserve(workers);
    auto finish = [&] {
        {
            std::lock_guard<std::mutex> lock(mtx);
            stop = true;
        }
        consumed.notify_all();
        for (auto& t : pool) t.join();
    };
    try {
        for (size_t w = 0; w < workers; w++) pool.emplace_back(work);
        for (size_t i = 0; i < n; i++) {
            Slot in;
            {
                std::unique_lock<std::mutex> lock(mtx);
                produced.wait(lock, [&] { return slots[i % window].ready; });
                in = std::move(slots[i % window]);
                slots[i % window] = Slot{};
            }
            if (in.error) std::rethrow_exception(in.error);
            consume(i, std::move(*in.result));
            {
                std::lock_guard<std::mutex> lock(mtx);
                done = i + 1;
            }
            consumed.notify_all();
        }
    } catch (...) {
        finish();
        throw;
    }
    finish();
}

// Follows a chain of records over [0, size), each record giving where the
// next starts, in `stripes` stripes walked in parallel. walk(offset, stop,
// out) appends the records from `offset` that start before `stop` and
// returns where the chain reaches (>= stop), throwing on a malformed record;
// find_start(from, to) guesses the first record start in [from, to) (`to`
// for none). A guess may be wrong, so a stripe's walk is kept only where the
// chain from 0 lands exactly on its start, and the stripe is walked again in
// order anywhere else: the records and any exception are exactly those of
// walk(0, size, out).
template <class Record, class FindStart, class Walk>
std::vector<Record> parallel_chain_walk(size_t size, size_t stripes, FindStart&& find_start, Walk&& walk,
                                        unsigned threads = 0) {
    std::vector<Record> out;
    if (threads == 0) threads = parallel_threads();
    stripes = std::min(stripes, size);
    if (threads <= 1 || stripes <= 1) {
        walk(size_t(0), size, out);
        return out;
    }

    std::vector<size_t> bounds(stripes + 1);
    for (size_t k = 0; k < stripes; k++) bounds[k] = size / stripes * k;
    bounds[stripes] = size;

    struct Stripe {
        size_t start = 0, end = 0;
        bool walked = false;
        std::vector<Record> records;
    };
    std::vector<Stripe> walks(stripes);
    parallel_for_each(stripes, [&](size_t k, unsigned) {
        Stripe& s = walks[k];
        s.start = k == 0 ? 0 : find_start(bounds[k], bounds[k + 1]);
        if (s.start < bounds[k] || s.start >= bounds[k + 1]) return;
        try {
            s.end = walk(s.start, bounds[k + 1], s.records);
            s.walked = true;
        } catch (...) {
            std::vector<Record>().swap(s.records);
        }
    }, threads);

    size_t total = 0;
    for (const auto& s : walks) total += s.records.size();
    out.reserve(total);
    size_t offset = 0;
    for (size_t k = 0; k < stripes; k++) {
        Stripe& s = walks[k];
        if (offset < bounds[k + 1]) {
            if (s.walked && s.start == offset) {
                out.insert(out.end(), std::make_move_iterator(s.records.begin()),
                           std::make_move_iterator(s.records.end()));
                offset = s.end;
            } else {
                offset = walk(offset, bounds[k + 1], out);
            }
        }
        std::vector<Record>().swap(s.records);
    }
    return out;
}

// The indices i in [0, n) where pred(i) holds, ascending, tested on every
// core.
template <class Pred>
std::vector<size_t> parallel_find_all(size_t n, Pred pred, unsigned threads = 0) {
    if (threads == 0) threads = parallel_threads();
    std::vector<std::vector<size_t>> found(std::max<size_t>(1, std::min<size_t>(threads, n)));
    parallel_for(n, [&](size_t b, size_t e, unsigned w) {
        for (size_t i = b; i < e; i++)
            if (pred(i)) found[w].push_back(i);
    }, threads);
    std::vector<size_t> out;
    for (const auto& f : found) out.insert(out.end(), f.begin(), f.end());
    return out;
}

// Sorts [first, last) by cmp on every core (sample sort: sort chunks, cut
// them at shared splitters, merge each slice). The sorted sequence is the
// one std::sort gives whenever it is unique: cmp a strict total order, or
// equivalent elements bitwise identical. Equivalent but different elements
// may land in any order, so such callers must break ties first.
// T must be default-constructible and movable.
template <class It, class Cmp>
void parallel_sort(It first, It last, Cmp cmp, unsigned threads = 0) {
    using T = typename std::iterator_traits<It>::value_type;
    constexpr size_t kMinPerChunk = size_t(1) << 16;
    constexpr size_t kSamplesPerChunk = 32;

    size_t n = static_cast<size_t>(last - first);
    if (threads == 0) threads = parallel_threads();
    size_t chunks = std::min<size_t>(threads, n / kMinPerChunk);
    if (chunks <= 1) {
        std::sort(first, last, cmp);
        return;
    }

    std::vector<size_t> bounds(chunks + 1);
    for (size_t c = 0; c <= chunks; c++) bounds[c] = n * c / chunks;
    parallel_for(chunks, [&](size_t b, size_t e, unsigned) {
        for (size_t c = b; c < e; c++) std::sort(first + bounds[c], first + bounds[c + 1], cmp);
    }, threads);

    std::vector<T> samples;
    samples.reserve(chunks * kSamplesPerChunk);
    for (size_t c = 0; c < chunks; c++) {
        size_t len = bounds[c + 1] - bounds[c];
        for (size_t s = 1; s <= kSamplesPerChunk; s++)
            samples.push_back(first[bounds[c] + len * s / (kSamplesPerChunk + 1)]);
    }
    std::sort(samples.begin(), samples.end(), cmp);
    std::vector<T> splitters;
    splitters.reserve(chunks - 1);
    for (size_t p = 1; p < chunks; p++) splitters.push_back(samples[p * samples.size() / chunks]);

    // cuts[c * (chunks + 1) + p]: where slice p starts in chunk c. Slice p
    // holds the elements in [splitters[p-1], splitters[p]).
    std::vector<size_t> cuts(chunks * (chunks + 1));
    parallel_for(chunks, [&](size_t b, size_t e, unsigned) {
        for (size_t c = b; c < e; c++) {
            size_t* cut = &cuts[c * (chunks + 1)];
            cut[0] = bounds[c];
            for (size_t p = 1; p < chunks; p++)
                cut[p] = static_cast<size_t>(std::lower_bound(first + cut[p - 1], first + bounds[c + 1],
                                                              splitters[p - 1], cmp) - first);
            cut[chunks] = bounds[c + 1];
        }
    }, threads);

    std::vector<size_t> slice_start(chunks + 1, 0);
    for (size_t p = 0; p < chunks; p++) {
        size_t len = 0;
        for (size_t c = 0; c < chunks; c++) len += cuts[c * (chunks + 1) + p + 1] - cuts[c * (chunks + 1) + p];
        slice_start[p + 1] = slice_start[p] + len;
    }

    std::unique_ptr<T[]> out(new T[n]);
    parallel_for(chunks, [&](size_t b, size_t e, unsigned) {
        struct Run { size_t pos, end, chunk; };
        std::vector<Run> heap;
        // Min-heap on the run heads; the lower chunk wins ties, like a
        // stable merge of the chunks in order.
        auto later = [&](const Run& x, const Run& y) {
            if (cmp(first[y.pos], first[x.pos])) return true;
            if (cmp(first[x.pos], first[y.pos])) return false;
            return x.chunk > y.chunk;
        };
        for (size_t p = b; p < e; p++) {
            heap.clear();
            for (size_t c = 0; c < chunks; c++) {
                size_t from = cuts[c * (chunks + 1) + p], to = cuts[c * (chunks + 1) + p + 1];
                if (from < to) heap.push_back({from, to, c});
            }
            std::make_heap(heap.begin(), heap.end(), later);
            T* dst = out.get() + slice_start[p];
            while (!heap.empty()) {
                std::pop_heap(heap.begin(), heap.end(), later);
                Run& r = heap.back();
                *dst++ = std::move(first[r.pos++]);
                if (r.pos == r.end) heap.pop_back();
                else std::push_heap(heap.begin(), heap.end(), later);
            }
        }
    }, threads);

    parallel_for(n, [&](size_t b, size_t e, unsigned) {
        std::move(out.get() + b, out.get() + e, first + b);
    }, threads);
}

// Splits [0, n) into at most `threads` blocks and runs fn(block, begin, end)
// for each on its own thread; returns the block count. The blocks depend
// only on n and the thread count, so two calls see the same blocks.
template <class Fn>
size_t parallel_blocks(size_t n, Fn&& fn, unsigned threads = 0) {
    constexpr size_t kMinPerBlock = size_t(1) << 14;
    if (threads == 0) threads = parallel_threads();
    size_t blocks = std::max<size_t>(1, std::min<size_t>(threads, n / kMinPerBlock));
    parallel_for(blocks, [&](size_t b0, size_t b1, unsigned) {
        for (size_t b = b0; b < b1; b++) fn(b, n * b / blocks, n * (b + 1) / blocks);
    }, threads);
    return blocks;
}

// Exclusive prefix sums of size_of(i) over [0, n), plus the total as entry
// n. size_of runs once per index. Integer sums, so the blocking can't reach
// the result.
template <class T, class SizeOf>
std::vector<T> parallel_offsets(size_t n, SizeOf size_of, unsigned threads = 0) {
    if (threads == 0) threads = parallel_threads();
    std::vector<T> out(n + 1);
    std::vector<T> block_base(threads + 1, T(0));
    size_t blocks = parallel_blocks(n, [&](size_t b, size_t begin, size_t end) {
        T sum = 0;
        for (size_t i = begin; i < end; i++) {
            out[i] = sum;
            sum += size_of(i);
        }
        block_base[b + 1] = sum;
    }, threads);
    for (size_t b = 0; b < blocks; b++) block_base[b + 1] += block_base[b];
    parallel_blocks(n, [&](size_t b, size_t begin, size_t end) {
        if (T base = block_base[b])
            for (size_t i = begin; i < end; i++) out[i] += base;
    }, threads);
    out[n] = block_base[blocks];
    return out;
}

// The indices i in [0, n) where keep(i), ascending. keep runs once per index.
template <class Keep>
std::vector<uint32_t> parallel_filter(size_t n, Keep keep, unsigned threads = 0) {
    if (threads == 0) threads = parallel_threads();
    std::vector<std::vector<uint32_t>> kept(threads);
    size_t blocks = parallel_blocks(n, [&](size_t b, size_t begin, size_t end) {
        for (size_t i = begin; i < end; i++)
            if (keep(i)) kept[b].push_back(static_cast<uint32_t>(i));
    }, threads);
    std::vector<size_t> at(blocks + 1, 0);
    for (size_t b = 0; b < blocks; b++) at[b + 1] = at[b] + kept[b].size();
    std::vector<uint32_t> out(at[blocks]);
    parallel_for(blocks, [&](size_t b0, size_t b1, unsigned) {
        for (size_t b = b0; b < b1; b++) {
            std::copy(kept[b].begin(), kept[b].end(), out.begin() + at[b]);
            kept[b] = {};
        }
    }, threads);
    return out;
}

// Whether pred(i) holds for any i in [0, n).
template <class Pred>
bool parallel_any(size_t n, Pred pred, unsigned threads = 0) {
    std::atomic<bool> found{false};
    parallel_for(n, [&](size_t begin, size_t end, unsigned) {
        for (size_t i = begin; i < end && !found.load(std::memory_order_relaxed); i++)
            if (pred(i)) found.store(true, std::memory_order_relaxed);
    }, threads);
    return found.load();
}

// The indices [0, n) in the order std::sort leaves them under less.
template <class Less>
std::vector<uint32_t> std_sort_indices(size_t n, Less less) {
    std::vector<uint32_t> order(n);
    for (size_t i = 0; i < n; i++) order[i] = static_cast<uint32_t>(i);
    std::sort(order.begin(), order.end(), less);
    return order;
}

struct SortedIndices {
    std::vector<uint32_t> order;
    bool serial = false;  // ties whose order matters: std::sort decided
};

// std_sort_indices on every core. Indices that tie under less (a strict weak
// order) land in std::sort's own order only by luck, so when two adjacent
// ones tie and tie_matters(a, b) says their order reaches the caller's
// output, this falls back to std_sort_indices. tie_matters must be the
// negation of an equivalence (e.g. "some output field differs"), so that
// checking neighbours covers every tied pair.
template <class Less, class TieMatters>
SortedIndices parallel_sort_indices(size_t n, Less less, TieMatters tie_matters, unsigned threads = 0) {
    std::vector<uint32_t> order(n);
    parallel_for(n, [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++) order[i] = static_cast<uint32_t>(i);
    }, threads);
    parallel_sort(order.begin(), order.end(), less, threads);
    bool ties = parallel_any(n ? n - 1 : 0, [&](size_t k) {
        return !less(order[k], order[k + 1]) && tie_matters(order[k], order[k + 1]);
    }, threads);
    if (!ties) return {std::move(order), false};
    return {std_sort_indices(n, less), true};
}

// parallel_sort_indices sorting keys instead of bare indices: key_of(i)
// carries i as .index, and key_less on two keys must answer as less on
// their indices would. std::sort's comparisons and moves depend only on the
// comparator's answers, so the order, serial fallback included, is the same;
// the fields compared first just sit together instead of behind an index.
template <class KeyOf, class KeyLess, class TieMatters>
SortedIndices parallel_sort_keys(size_t n, KeyOf key_of, KeyLess key_less, TieMatters tie_matters,
                                 unsigned threads = 0) {
    std::vector<decltype(key_of(size_t(0)))> keys(n);
    auto fill = [&] {
        parallel_for(n, [&](size_t b, size_t e, unsigned) {
            for (size_t i = b; i < e; i++) keys[i] = key_of(i);
        }, threads);
    };
    fill();
    parallel_sort(keys.begin(), keys.end(), key_less, threads);
    bool ties = parallel_any(n ? n - 1 : 0, [&](size_t k) {
        return !key_less(keys[k], keys[k + 1]) && tie_matters(keys[k].index, keys[k + 1].index);
    }, threads);
    if (ties) {
        fill();
        std::sort(keys.begin(), keys.end(), key_less);
    }
    std::vector<uint32_t> order(n);
    parallel_for(n, [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++) order[i] = keys[i].index;
    }, threads);
    return {std::move(order), ties};
}

// Sorts each run of [first, last) by less with std::sort, on every core. A
// run is a maximal stretch whose neighbours same_run(prev, next) joins, so
// the cuts between threads fall on run boundaries and can't change it.
template <class It, class SameRun, class Less>
void parallel_sort_runs(It first, It last, SameRun same_run, Less less, unsigned threads = 0) {
    constexpr size_t kMinPerPart = size_t(1) << 14;
    size_t n = static_cast<size_t>(last - first);
    if (threads == 0) threads = parallel_threads();
    size_t parts = std::max<size_t>(1, std::min<size_t>(threads, n / kMinPerPart));
    std::vector<size_t> cut(parts + 1, n);
    cut[0] = 0;
    for (size_t p = 1; p < parts; p++) {
        size_t c = std::max(cut[p - 1], n * p / parts);
        while (c > 0 && c < n && same_run(first[c - 1], first[c])) c++;
        cut[p] = c;
    }
    parallel_for(parts, [&](size_t p0, size_t p1, unsigned) {
        for (size_t run = cut[p0], end = cut[p1]; run < end;) {
            size_t next = run + 1;
            while (next < end && same_run(first[next - 1], first[next])) next++;
            std::sort(first + run, first + next, less);
            run = next;
        }
    }, threads);
}
