// Reads byte ranges of a file ahead of the threads that consume them, in
// large chunks on threads of its own, so that the consumers' small reads find
// the page cache. On a network block device (the nightly's Ceph scratch) a
// small read is a round trip; large reads, many in flight, run at its full
// speed.
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

struct ByteRange {
    uint64_t begin, end;
};

// The ranges [begin, end) that `offsets` ascending, each `size_of(i)` bytes
// long, cover: neighbours that touch merge into one range.
template <class SizeOf>
std::vector<ByteRange> touching_ranges(const std::vector<uint64_t>& offsets, SizeOf size_of) {
    std::vector<ByteRange> ranges;
    for (size_t i = 0; i < offsets.size(); i++) {
        const uint64_t begin = offsets[i], end = begin + size_of(i);
        if (!ranges.empty() && ranges.back().end == begin) ranges.back().end = end;
        else ranges.push_back({begin, end});
    }
    return ranges;
}

// Reads `ranges` (ascending, apart) in `chunk`-byte reads on `threads`
// threads, never more than `window` bytes past the position the consumers
// last reported (consumed()). Reading is advice: errors are ignored, and the
// consumers read for themselves. The destructor stops the threads.
class FilePrefetcher {
public:
    FilePrefetcher(const std::string& path, const std::vector<ByteRange>& ranges, uint64_t window,
                   size_t chunk, unsigned threads)
        : window_(window) {
        for (const ByteRange& r : ranges)
            for (uint64_t off = r.begin; off < r.end; off += chunk)
                chunks_.push_back({off, std::min<uint64_t>(r.end, off + chunk)});
        if (chunks_.empty()) return;
        fd_ = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd_ < 0) return;
        consumed_.store(chunks_.front().begin, std::memory_order_relaxed);
        for (unsigned t = 0; t < std::max(1u, threads); t++)
            threads_.emplace_back([this, chunk] { run(chunk); });
    }
    ~FilePrefetcher() {
        stop_.store(true);
        cv_.notify_all();
        for (std::thread& t : threads_) t.join();
        if (fd_ >= 0) ::close(fd_);
    }
    FilePrefetcher(const FilePrefetcher&) = delete;
    FilePrefetcher& operator=(const FilePrefetcher&) = delete;

    // The consumers have reached file offset `off`.
    void consumed(uint64_t off) {
        uint64_t seen = consumed_.load(std::memory_order_relaxed);
        while (off > seen && !consumed_.compare_exchange_weak(seen, off, std::memory_order_relaxed)) {}
    }

    // Bytes read so far (for logs and tests).
    uint64_t bytes_read() const { return read_.load(); }

private:
    void run(size_t chunk) {
        std::vector<char> buf(chunk);
        for (size_t i = next_.fetch_add(1); i < chunks_.size() && !stop_.load(); i = next_.fetch_add(1)) {
            const ByteRange c = chunks_[i];
            {
                // Polled: the consumers report on every blob, and a notify
                // each time would cost more than the wait.
                std::unique_lock<std::mutex> lock(mtx_);
                while (!stop_.load() && c.begin > consumed_.load(std::memory_order_relaxed) + window_)
                    cv_.wait_for(lock, std::chrono::milliseconds(2));
            }
            if (stop_.load()) return;
            for (uint64_t off = c.begin; off < c.end;) {
                const ssize_t got = ::pread(fd_, buf.data(), static_cast<size_t>(c.end - off), static_cast<off_t>(off));
                if (got <= 0) break;
                off += static_cast<uint64_t>(got);
                read_.fetch_add(static_cast<uint64_t>(got));
            }
        }
    }

    std::vector<ByteRange> chunks_;
    uint64_t window_;
    int fd_ = -1;
    std::atomic<size_t> next_{0};
    std::atomic<uint64_t> consumed_{0};
    std::atomic<uint64_t> read_{0};
    std::atomic<bool> stop_{false};
    std::mutex mtx_;
    std::condition_variable cv_;
    std::vector<std::thread> threads_;
};
