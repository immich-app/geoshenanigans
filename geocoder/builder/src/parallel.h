// Thread-parallel loop and sort for planet-scale arrays. The builder's output
// must be byte-identical however many cores run it, so nothing here may let
// the thread count leak into a result.
#pragma once

#include <algorithm>
#include <cstddef>
#include <exception>
#include <iterator>
#include <memory>
#include <thread>
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
