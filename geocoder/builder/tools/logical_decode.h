// How geocoder-diff writes the logical files it decodes a dir's packed
// families into, in its scratch dir. Each file goes out through buffers of
// BUFFER_BYTES, in write() calls of that size: std::ofstream's 8 KiB buffer
// takes a call each 8 KiB, which over a planet day's 18 GB of logical files
// is seconds of system time.
//
// The address files and the way pairs, the biggest families, decode on
// threads, a range of superblocks or of groups at a time, each range's bytes
// written at its place in the logical files by a thread of the file's own
// (PlacedFile): whatever the threads and the ranges, the bytes are the ones
// addr_files::decode and way_files::decode give, and a refusal is theirs,
// the first in the files' order.
//
// A range's records sit at its first record's place, which its first
// superblock or group gives. Its logical bytes before it are the ones of the
// ranges before it, which each range's walk gives first, on the threads
// (superblocks_logical_bytes, groups_nodes). The walks and the decodes start
// each range where its first row places it; the ranges are then taken in
// order, each from where the one before it ends, as the one decoder takes
// them: a row that places its range elsewhere is refused as that decoder
// refuses it, and a range's refusal is thrown once every range before it is
// taken. A range no thread could place (one past a footer's nodes, say) is
// decoded there, its output not written: what it refuses is the one
// decoder's refusal, and files that hold such a range always hold one.
//
// Once the diff has read them, the logical files go on threads of their own
// (Remover), beside the patch's last sections.
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
#include <future>
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
#include "way_files.h"

namespace logical_decode {

// The bytes a writer holds before it writes them.
inline constexpr size_t BUFFER_BYTES = size_t(1) << 20;
// The superblocks of a range of the address files and the groups of a range
// of a way pair: 65,536 records or ways a range, a few MB of logical bytes.
inline constexpr uint64_t ADDR_RANGE_SUPERBLOCKS = 64, WAY_RANGE_GROUPS = 4096;

// The threads a family decodes on. A file takes its writes one at a time
// (ext4 holds a buffered write's inode lock), about 1 GB a second from one
// thread: the planet's biggest logical files, 5 GB each, take 5 seconds,
// and its address files decode on 8 threads in 5.9 (6.4 on 4).
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
    explicit ScratchFile(const std::string& path)
        : fd_(::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0666)), ok_(fd_ >= 0) {}
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
    bool ok_;
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

// Files removed on threads of their own, beside what the diff does next:
// unlinking a planet day's logical files (18 GB) takes one thread seconds,
// the biggest file over one, which the scratch dir's removal spent after
// the patch was written. A batch's files go on a thread each. The diff holds
// its Remover until it ends, and the destructor waits for every batch: no
// file of a diff's scratch outlives it, as generate_patches.sh counts a
// diff's scratch until the diff exits.
class Remover {
public:
    Remover() = default;
    Remover(const Remover&) = delete;
    Remover& operator=(const Remover&) = delete;
    ~Remover() { wait(); }

    // Removes the files beside, a missing one as none.
    void remove(std::vector<std::string> paths) {
        batches_.push_back(std::async(std::launch::async, [paths = std::move(paths)] {
            parallel_for_each(
                paths.size(), [&](size_t i, unsigned) { ::unlink(paths[i].c_str()); }, unsigned(std::max<size_t>(paths.size(), 1)));
        }));
    }
    // Waits until every file given is gone.
    void wait() {
        for (std::future<void>& b : batches_) b.wait();
    }

private:
    std::vector<std::future<void>> batches_;
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

// The logical pair of a way pair of the kind (each file whole, in memory; of
// the size ABSENT when the dir lacks it), as way_files::decode gives it,
// written at its places to ways_fd and nodes_fd (empty files) on up to
// `threads` threads (0 = every core), `range` groups a range. Refuses what
// way_files::decode refuses. False, the files decoded: a write failed.
inline bool decode_way_files(const way_files::Kind& kind, const uint8_t* ways, uint64_t ways_size, const uint8_t* nodes,
                             uint64_t nodes_size, int ways_fd, int nodes_fd, unsigned threads, uint64_t range = WAY_RANGE_GROUPS) {
    namespace wf = way_files;
    auto end_of = [](const uint8_t* file, uint64_t size, size_t footer) {
        return size == wf::ABSENT ? nullptr : file + (size - std::min<uint64_t>(size, footer));
    };
    const wf::Footers f = wf::read_footers(kind, end_of(ways, ways_size, wf::WAYS_FOOTER_BYTES), ways_size,
                                           end_of(nodes, nodes_size, wf::NODES_FOOTER_BYTES), nodes_size);
    range = std::max<uint64_t>(range, 1);
    const uint64_t groups = f.groups(), ranges = (groups + range - 1) / range;
    const unsigned workers = workers_of(ranges, threads);
    auto last = [&](uint64_t k) { return std::min(groups, (k + 1) * range); };
    struct Range {
        // Its first group's off, its nodes, the nodes before it, and where it ends.
        uint64_t off = 0, nodes = 0, before = 0;
        wf::DecodeAt end;
        bool placed = false;
        // What its first row is refused for, and what the rest of it is.
        std::exception_ptr row_refused, refused;
    };
    std::vector<Range> rs(static_cast<size_t>(ranges));
    std::atomic<uint64_t> next{0};
    on_workers(workers, [&] {
        for (uint64_t k = next++; k < ranges; k = next++) {
            Range& r = rs[size_t(k)];
            r.off = wf::get32(wf::row_of(kind, ways, k * range));
            r.nodes = wf::groups_nodes(kind, f, ways, k * range, last(k));
        }
    });
    uint64_t before = 0;
    for (Range& r : rs) {
        r.before = before;
        before += r.nodes;
    }
    bool written;
    {
        PlacedFile ways_out(ways_fd, workers + 4), nodes_out(nodes_fd, workers + 4);
        next = 0;
        on_workers(workers, [&] {
            PlacedWriter wo(ways_out), no(nodes_out);
            for (uint64_t k = next++; k < ranges; k = next++) {
                Range& r = rs[size_t(k)];
                const uint64_t first = k * range;
                try {
                    wf::decode_row(kind, wf::row_of(kind, ways, first), f.group_ways(first));
                } catch (...) {
                    r.row_refused = std::current_exception();
                    continue;
                }
                // A range past the blocks is refused in order; one past the footer's nodes is too,
                // at its end or before.
                if (r.off > f.blocks_bytes || r.before + r.nodes > f.nodes) continue;
                wo.seek(first * wf::GROUP_WAYS * kind.stride);
                no.seek(r.before * wf::NODE_BYTES);
                try {
                    r.end = wf::decode_groups(
                        kind, f, ways, nodes, first, last(k), wf::DecodeAt{r.off, r.before},
                        [&](const uint8_t* record) { wo.write(record, kind.stride); }, [&](const uint8_t* p, size_t n) { no.write(p, n); });
                } catch (...) {
                    r.refused = std::current_exception();
                }
                r.placed = true;
            }
        });
        const bool ways_written = ways_out.finish(), nodes_written = nodes_out.finish();
        written = ways_written && nodes_written;
    }
    wf::DecodeAt at;
    bool whole = true;
    for (uint64_t k = 0; k < ranges; k++) {
        const Range& r = rs[size_t(k)];
        // The one decoder reads a group's row before it checks its off.
        if (r.row_refused) std::rethrow_exception(r.row_refused);
        if (r.off != at.at) wf::misplaced_group(kind, k * range);
        if (!r.placed || r.before != at.node) {
            whole = false;
            at = wf::decode_groups(kind, f, ways, nodes, k * range, last(k), at, [](const uint8_t*) {}, [](const uint8_t*, size_t) {});
            continue;
        }
        if (r.refused) std::rethrow_exception(r.refused);
        at = r.end;
    }
    wf::check_decoded(kind, f, at);
    if (!whole) throw std::logic_error(std::string("the ") + kind.ways_file + " pair decodes, and a range of it was not decoded at its place");
    return written;
}

}  // namespace logical_decode
