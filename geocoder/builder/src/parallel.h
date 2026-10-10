// Thread-parallel loop and sort for planet-scale arrays. The builder's output
// must be byte-identical however many cores run it, so nothing here may let
// the thread count leak into a result.
#pragma once

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
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
