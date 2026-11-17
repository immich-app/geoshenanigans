// How geocoder-diff writes the logical files it decodes a dir's packed
// families into, in its scratch dir. Each file goes out through buffers of
// BUFFER_BYTES, in write() calls of that size: std::ofstream's 8 KiB buffer
// takes a call each 8 KiB, which over a planet day's 18 GB of logical files
// is seconds of system time.
//
// The address files, the biggest family, decode on threads, a range of
// superblocks at a time, each range's bytes written at its place in the
// logical files by a thread of the file's own (PlacedFile): whatever the
// threads and the ranges, the bytes are the ones addr_files::decode gives,
// and a refusal is its own, the first in the files' order.
//
// A range's records sit at its first record's place, which its first
// superblock gives. Its logical bytes before it are the ones of the ranges
// before it, which each range's walk gives first, on the threads
// (superblocks_logical_bytes). The walks and the decodes start each range
// where its first row places it; the ranges are then taken in order, each
// from where the one before it ends, as the one decoder takes them: a row
// that places its range elsewhere is refused as that decoder refuses it, and
// a range's refusal is thrown once every range before it is taken. A range
// no thread could place (one past a range whose walk refused it) is decoded
// there, its output not written: what it refuses is the one decoder's
// refusal, and files that hold such a range always hold one.
#pragma once

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <fcntl.h>
#include <mutex>
#include <new>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include "addr_files.h"
#include "parallel.h"

namespace logical_decode {

// The bytes a writer holds before it writes them.
inline constexpr size_t BUFFER_BYTES = size_t(1) << 20;
// The superblocks of a range of the address files: 65,536 records, a few MB
// of logical bytes.
inline constexpr uint64_t ADDR_RANGE_SUPERBLOCKS = 64;

// The threads a family decodes on. A file takes its writes one at a time
// (ext4 holds a buffered write's inode lock), about 1 GB a second from one
// thread: the planet's logical addr_points.bin, 5 GB, takes 5 seconds, and
// the address files decode on 8 threads in 5.9 (6.4 on 4).
inline unsigned decode_threads() { return std::min(parallel_threads(), 8u); }

// Writes the n bytes at p to fd at `at`, or where fd stands at APPEND;
// false on an error.
inline constexpr uint64_t APPEND = ~uint64_t(0);
inline bool write_all(int fd, const char* p, size_t n, uint64_t at) {
    while (n) {
        const ssize_t w = at == APPEND ? ::write(fd, p, n) : ::pwrite(fd, p, n, off_t(at));
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) return false;
        p += w;
        n -= size_t(w);
        if (at != APPEND) at += uint64_t(w);
    }
    return true;
}

// A buffer of BUFFER_BYTES, mapped on its own (mapped()) and unmapped when it
// goes; none when made empty. From malloc, the first such block freed would
// raise glibc's mmap threshold past it, and the arenas of the threads that
// freed the next ones would keep them through the merges.
class Buffer {
public:
    Buffer() = default;
    static Buffer mapped() {
        void* p = ::mmap(nullptr, BUFFER_BYTES, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (p == MAP_FAILED) throw std::bad_alloc();
        Buffer b;
        b.data_ = static_cast<char*>(p);
        return b;
    }
    Buffer(Buffer&& o) noexcept : data_(std::exchange(o.data_, nullptr)) {}
    Buffer& operator=(Buffer&& o) noexcept {
        if (this != &o) {
            release();
            data_ = std::exchange(o.data_, nullptr);
        }
        return *this;
    }
    ~Buffer() { release(); }

    char* data() const { return data_; }
    explicit operator bool() const { return data_ != nullptr; }

private:
    void release() {
        if (data_) ::munmap(data_, BUFFER_BYTES);
        data_ = nullptr;
    }
    char* data_ = nullptr;
};

// A logical file, made (or emptied) at path and written in order through a
// Buffer, taken at the first write; fd() for writes at places of its own.
// Like an std::ofstream's, a write that failed, or a file that could not be
// made, makes flush() false from then on.
class ScratchFile {
public:
    explicit ScratchFile(const std::string& path) : fd_(::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0666)) {}
    ~ScratchFile() {
        if (fd_ >= 0) ::close(fd_);
    }
    ScratchFile(const ScratchFile&) = delete;
    ScratchFile& operator=(const ScratchFile&) = delete;

    void write(const void* data, size_t n) {
        const char* p = static_cast<const char*>(data);
        if (n > BUFFER_BYTES - used_) {
            drain();
            if (n >= BUFFER_BYTES) {
                put(p, n);
                return;
            }
        }
        if (!buffer_) buffer_ = Buffer::mapped();
        std::memcpy(buffer_.data() + used_, p, n);
        used_ += n;
    }
    // Writes what it holds: whether every byte so far is written.
    bool flush() {
        drain();
        return ok_;
    }
    int fd() const { return fd_; }

private:
    void drain() {
        if (used_) put(buffer_.data(), used_);
        used_ = 0;
    }
    void put(const char* p, size_t n) {
        if (ok_) ok_ = fd_ >= 0 && write_all(fd_, p, n, APPEND);
    }

    int fd_;
    Buffer buffer_;
    size_t used_ = 0;
    bool ok_ = true;
};

// A file written at places by a thread of its own: threads take buffers
// from its pool, fill them, and give each back with its place; the thread
// writes them there in the order they come, a buffer back to the pool once
// written. At most `buffers` buffers are mapped, a thread that takes one
// waiting while every one is filled or queued: the file's writes, which go
// one at a time, set the pace. A planet day's address files decoded on 36
// threads that wrote them themselves took 5.3 seconds and 133 CPU-seconds in
// the kernel, the threads spinning for the inode's lock (osq_lock); taking
// turns by a mutex, 10.3 seconds, the threads waiting to be woken; written
// here, 5.9 seconds and 10 CPU-seconds in the kernel.
class PlacedFile {
public:
    PlacedFile(int fd, size_t buffers) : fd_(fd), most_(std::max<size_t>(buffers, 1)) { thread_ = std::thread([this] { serve(); }); }
    ~PlacedFile() { finish(); }
    PlacedFile(const PlacedFile&) = delete;
    PlacedFile& operator=(const PlacedFile&) = delete;

    // A buffer to fill, once one is free.
    Buffer take() {
        std::unique_lock<std::mutex> lock(mutex_);
        freed_.wait(lock, [&] { return !free_.empty() || mapped_ < most_; });
        if (free_.empty()) {
            Buffer b = Buffer::mapped();
            mapped_++;
            return b;
        }
        Buffer b = std::move(free_.back());
        free_.pop_back();
        return b;
    }
    // Writes the buffer's first n bytes at `at`, beside.
    void give(Buffer buffer, size_t n, uint64_t at) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push_back({std::move(buffer), n, at});
        }
        queued_.notify_one();
    }
    // Waits until every buffer given is written: whether every one was.
    bool finish() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closing_ = true;
        }
        queued_.notify_one();
        if (thread_.joinable()) thread_.join();
        return ok_;
    }

private:
    struct Chunk {
        Buffer buffer;
        size_t n;
        uint64_t at;
    };
    void serve() {
        std::unique_lock<std::mutex> lock(mutex_);
        for (;;) {
            queued_.wait(lock, [&] { return closing_ || !queue_.empty(); });
            if (queue_.empty()) return;
            Chunk c = std::move(queue_.front());
            queue_.pop_front();
            lock.unlock();
            const bool ok = fd_ >= 0 && write_all(fd_, c.buffer.data(), c.n, c.at);
            lock.lock();
            ok_ = ok_ && ok;
            free_.push_back(std::move(c.buffer));
            freed_.notify_one();
        }
    }

    int fd_;
    size_t most_, mapped_ = 0;
    std::mutex mutex_;
    std::condition_variable queued_, freed_;
    std::deque<Chunk> queue_;
    std::vector<Buffer> free_;
    bool closing_ = false, ok_ = true;
    std::thread thread_;
};

// Bytes written in order from a place of a PlacedFile, a buffer of it at a
// time; what it holds goes to the file when it moves on (seek) and when it
// goes.
class PlacedWriter {
public:
    explicit PlacedWriter(PlacedFile& file) : file_(file) {}
    ~PlacedWriter() { drain(); }
    PlacedWriter(const PlacedWriter&) = delete;
    PlacedWriter& operator=(const PlacedWriter&) = delete;

    void write(const void* data, size_t n) {
        const char* p = static_cast<const char*>(data);
        while (n) {
            if (!buffer_) buffer_ = file_.take();
            const size_t k = std::min(n, BUFFER_BYTES - used_);
            std::memcpy(buffer_.data() + used_, p, k);
            used_ += k;
            p += k;
            n -= k;
            if (used_ == BUFFER_BYTES) drain();
        }
    }
    // Gives what it holds to the file, and goes on from `at`.
    void seek(uint64_t at) {
        drain();
        at_ = at;
    }

private:
    void drain() {
        if (!used_) return;
        file_.give(std::move(buffer_), used_, at_);
        at_ += used_;
        used_ = 0;
    }

    PlacedFile& file_;
    Buffer buffer_;
    uint64_t at_ = 0;
    size_t used_ = 0;
};

// The workers of a decode of `ranges` ranges on up to `threads` threads (0 =
// every core): at least one.
inline unsigned workers_of(uint64_t ranges, unsigned threads) {
    if (threads == 0) threads = parallel_threads();
    return unsigned(std::max<uint64_t>(std::min<uint64_t>(threads, ranges), 1));
}

// Each worker's fn(), on a thread of its own: the decodes' workers take the
// next range as they free up.
template <class Fn>
void on_workers(unsigned workers, Fn fn) {
    parallel_for(workers, [&](size_t, size_t, unsigned) { fn(); }, workers);
}

// a + b, or UINT64_MAX past it.
inline uint64_t saturating_add(uint64_t a, uint64_t b) { return b > UINT64_MAX - a ? UINT64_MAX : a + b; }

// The logical pair of the address files (each whole, in memory; of the size
// ABSENT when the dir lacks it), as addr_files::decode gives it, written at
// its places to points_fd and vertices_fd (empty files) on up to `threads`
// threads (0 = every core), `range` superblocks a range. Refuses what
// addr_files::decode refuses. False, the files decoded: a write failed.
inline bool decode_addr_files(const uint8_t* points, uint64_t points_size, const uint8_t* vertices, uint64_t vertices_size,
                              const uint8_t* index, uint64_t index_size, int points_fd, int vertices_fd, unsigned threads,
                              uint64_t range = ADDR_RANGE_SUPERBLOCKS) {
    namespace af = addr_files;
    auto end_of = [](const uint8_t* file, uint64_t size, size_t footer) {
        return size == af::ABSENT ? nullptr : file + (size - std::min<uint64_t>(size, footer));
    };
    const af::Footers f = af::read_footers(end_of(points, points_size, af::POINTS_FOOTER_BYTES), points_size,
                                           end_of(vertices, vertices_size, af::VERTICES_FOOTER_BYTES), vertices_size,
                                           end_of(index, index_size, af::INDEX_FOOTER_BYTES), index_size);
    range = std::max<uint64_t>(range, 1);
    const uint64_t superblocks = f.superblocks(), ranges = (superblocks + range - 1) / range;
    const unsigned workers = workers_of(ranges, threads);
    auto last = [&](uint64_t k) { return std::min(superblocks, (k + 1) * range); };
    struct Range {
        // Where its first superblock's row places it, with the logical bytes before it once they are
        // known; and where it ends.
        af::DecodeAt start, end;
        // Its logical bytes, when the walk took it.
        uint64_t logical = 0;
        bool walked = false, placeable = false, placed = false;
        std::exception_ptr refused;
    };
    std::vector<Range> rs(static_cast<size_t>(ranges));
    std::atomic<uint64_t> next{0};
    on_workers(workers, [&] {
        for (uint64_t k = next++; k < ranges; k = next++) {
            Range& r = rs[size_t(k)];
            r.start = af::superblock_at(index, k * range);
            // A row that places its range past the blocks or the blobs is refused in order.
            if (r.start.blocks > f.blocks_bytes || r.start.blobs > f.blobs_bytes) continue;
            try {
                r.logical = af::superblocks_logical_bytes(f, points, points_size, vertices, k * range, last(k), r.start);
                r.walked = true;
            } catch (...) {
            }
        }
    });
    // Each range's logical bytes before it, while every range before it was walked.
    uint64_t before = 0;
    bool known = true;
    for (Range& r : rs) {
        r.start.logical = before;
        r.placeable = known && r.start.blocks <= f.blocks_bytes && r.start.blobs <= f.blobs_bytes &&
                      before <= f.logical_vertex_bytes;
        known = known && r.walked;
        before = saturating_add(before, r.logical);
    }
    bool written;
    {
        PlacedFile points_out(points_fd, workers + 4), vertices_out(vertices_fd, workers + 4);
        next = 0;
        on_workers(workers, [&] {
            PlacedWriter po(points_out), vo(vertices_out);
            for (uint64_t k = next++; k < ranges; k = next++) {
                Range& r = rs[size_t(k)];
                if (!r.placeable) continue;
                po.seek(k * range * af::SUPERBLOCK_RECORDS * af::LOGICAL_STRIDE);
                vo.seek(r.start.logical);
                try {
                    r.end = af::decode_superblocks(
                        f, points, points_size, vertices, index, k * range, last(k), r.start,
                        [&](const uint8_t* record) { po.write(record, af::LOGICAL_STRIDE); },
                        [&](const uint8_t* p, size_t n) { vo.write(p, n); });
                } catch (...) {
                    r.refused = std::current_exception();
                }
                r.placed = true;
            }
        });
        const bool points_written = points_out.finish(), vertices_written = vertices_out.finish();
        written = points_written && vertices_written;
    }
    af::DecodeAt at;
    bool whole = true;
    for (uint64_t k = 0; k < ranges; k++) {
        const Range& r = rs[size_t(k)];
        if (r.start.blocks != at.blocks || r.start.blobs != at.blobs) af::misplaced_superblock(k * range);
        if (!r.placed || r.start.logical != at.logical) {
            whole = false;
            at = af::decode_superblocks(f, points, points_size, vertices, index, k * range, last(k), at, [](const uint8_t*) {},
                                        [](const uint8_t*, size_t) {});
            continue;
        }
        if (r.refused) std::rethrow_exception(r.refused);
        at = {r.end.blocks, r.end.blobs, r.end.logical, at.count + r.end.count};
    }
    af::check_decoded(f, at);
    if (!whole) throw std::logic_error("the address files decode, and a range of them was not decoded at its place");
    return written;
}

}  // namespace logical_decode
