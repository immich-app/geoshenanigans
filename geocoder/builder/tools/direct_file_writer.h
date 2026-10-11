#pragma once

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "sequential_file_reader.h"

// What O_DIRECT needs a write's memory, offset and length aligned to: the
// logical block size, 512 or 4096 on the disks in use.
constexpr size_t kDirectWriteAlign = 4096;
// The bytes of the file one block holds (a multiple of kDirectWriteAlign).
// The test box's NVMe took 3.4 GB/s in 16 or 64 MiB direct writes.
constexpr size_t kDirectWriteBlockBytes = size_t(32) << 20;
// Blocks being filled and written at once.
constexpr size_t kDirectWriteBlocksInFlight = 8;
// How far ahead of the writes the file is allocated.
constexpr uint64_t kDirectWriteAllocateBytes = uint64_t(1) << 30;

// Writes a new file front to back, with O_DIRECT when asked and the
// filesystem takes it. Through the page cache a planet-sized file went at
// 0.9 GB/s on the test box's NVMe whatever the write size, its writeback and
// page reclaim the limit; direct writes of whole blocks went at 3.4 GB/s. A
// disk slower than the page cache gains nothing, and a reader right after
// finds nothing cached.
//
// Appending only notes where the bytes of each block of the file come
// from. A block once laid out is filled, from the strings handed over and
// the spans of other files named, and written on a thread of its own,
// several blocks at once: the one thread appending, copying every byte
// itself beside busy encoders, fell to 2-3 GB/s, under the disk's speed.
// The file is allocated ahead, so that the writes, landing out of order,
// need not extend it. The last partial block, and everything on a
// filesystem that refuses O_DIRECT (tmpfs), goes through the page cache.
class DirectFileWriter {
public:
    explicit DirectFileWriter(const std::string& path, bool direct = true,
                              size_t block_bytes = kDirectWriteBlockBytes,
                              size_t in_flight = kDirectWriteBlocksInFlight)
        : path_(path),
          block_bytes_(std::max(kDirectWriteAlign, block_bytes / kDirectWriteAlign * kDirectWriteAlign)),
          in_flight_(std::max<size_t>(in_flight, 1)) {
        const int flags = O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC;
#ifdef O_DIRECT
        if (direct) {
            fd_ = ::open(path.c_str(), flags | O_DIRECT, 0644);
            opened_direct_ = fd_ >= 0;
            direct_ = opened_direct_;
        }
#else
        (void)direct;
#endif
        if (fd_ < 0) fd_ = ::open(path.c_str(), flags, 0644);
        if (fd_ < 0) throw std::runtime_error("cannot open " + path + ": " + std::strerror(errno));
    }
    ~DirectFileWriter() {
        for (Block& b : busy_)
            if (b.done.valid()) b.done.wait();
        if (fd_ >= 0) ::close(fd_);
    }
    DirectFileWriter(const DirectFileWriter&) = delete;
    DirectFileWriter& operator=(const DirectFileWriter&) = delete;

    // Appends bytes, kept until they are written.
    void append(std::string bytes) {
        Piece p;
        p.n = bytes.size();
        p.bytes = std::make_shared<const std::string>(std::move(bytes));
        add(p);
    }

    // Appends n bytes of the file `in` from `offset` on, read when their
    // block is filled: `in` and `in_path` (its name, for errors) must stay
    // valid until finish().
    void append_from(int in, uint64_t offset, uint64_t n, const std::string& in_path) {
        Piece p;
        p.fd = in;
        p.path = &in_path;
        p.offset = offset;
        p.n = n;
        add(p);
    }

    // Writes what is left and closes the file. Returns its size.
    uint64_t finish() {
        while (!busy_.empty()) retire_oldest();
        Block tail = take_buffer();
        fill(tail.data.get(), pieces_);
        pieces_.clear();
        const uint64_t at = appended_ - filled_;
        size_t whole = direct_ ? filled_ / kDirectWriteAlign * kDirectWriteAlign : 0;
        write_at(tail.data.get(), whole, at);
        if (whole < filled_) {
            buffered();
            write_at(tail.data.get() + whole, filled_ - whole, at + whole);
        }
        // The allocation ahead left the file longer than its bytes, by more
        // than allocated_ when a failed fallocate (ENOSPC) kept part of its range.
        struct stat st;
        if (::fstat(fd_, &st) == 0 && S_ISREG(st.st_mode) && uint64_t(st.st_size) != appended_ &&
            ::ftruncate(fd_, static_cast<off_t>(appended_)) != 0)
            throw std::runtime_error("cannot truncate " + path_ + ": " + std::strerror(errno));
        int fd = fd_;
        fd_ = -1;
        if (::close(fd) != 0) throw std::runtime_error("cannot close " + path_ + ": " + std::strerror(errno));
        return appended_;
    }

    // The writes go around the page cache (until finish() writes the tail).
    bool direct() const { return direct_; }

private:
    // n bytes of a block: bytes [offset, offset + n) of `bytes`, or of the
    // file fd when there are no bytes.
    struct Piece {
        std::shared_ptr<const std::string> bytes;
        int fd = -1;
        const std::string* path = nullptr;
        uint64_t offset = 0;
        uint64_t n = 0;
    };
    struct FreeBuffer {
        void operator()(char* p) const { std::free(p); }
    };
    struct Block {
        std::unique_ptr<char, FreeBuffer> data;
        std::future<void> done;  // filled and written
    };

    // Lays a piece out, cut at block ends.
    void add(Piece p) {
        while (p.n) {
            Piece head = p;
            head.n = std::min<uint64_t>(p.n, block_bytes_ - filled_);
            pieces_.push_back(head);
            p.offset += head.n;
            p.n -= head.n;
            filled_ += size_t(head.n);
            appended_ += head.n;
            if (filled_ == block_bytes_) submit_block();
        }
    }

    static void fill(char* dst, const std::vector<Piece>& pieces) {
        for (const Piece& p : pieces) {
            if (p.bytes) {
                std::memcpy(dst, p.bytes->data() + p.offset, size_t(p.n));
            } else {
                read_fully(dst, size_t(p.n), p.offset, [fd = p.fd](char* d, size_t k, uint64_t off) {
                    return ::pread(fd, d, k, static_cast<off_t>(off));
                }, *p.path);
            }
            dst += p.n;
        }
    }

    // Hands the laid-out block to a thread that fills and writes it.
    void submit_block() {
        const uint64_t at = appended_ - block_bytes_;
        allocate_to(at + block_bytes_);
        Block b = take_buffer();
        char* data = b.data.get();
        b.done = std::async(std::launch::async, [this, data, at, pieces = std::move(pieces_)] {
            fill(data, pieces);
            write_at(data, block_bytes_, at);
        });
        busy_.push_back(std::move(b));
        pieces_.clear();
        filled_ = 0;
    }

    // A buffer for the next block, once one of the blocks in flight is done.
    Block take_buffer() {
        if (free_.empty() && busy_.size() >= in_flight_) retire_oldest();
        if (!free_.empty()) {
            Block b = std::move(free_.back());
            free_.pop_back();
            return b;
        }
        void* p = std::aligned_alloc(kDirectWriteAlign, block_bytes_);
        if (!p) throw std::bad_alloc();
        Block b;
        b.data.reset(static_cast<char*>(p));
        return b;
    }

    void retire_oldest() {
        Block b = std::move(busy_.front());
        busy_.pop_front();
        b.done.get();
        free_.push_back(std::move(b));
    }

    // Allocates the file up to at least `end`: writes inside its size, into
    // allocated blocks, go side by side where ones that extend it queue up.
    // A filesystem without fallocate writes all the same.
    void allocate_to(uint64_t end) {
#ifdef __linux__
        if (!allocate_ || end <= allocated_) return;
        uint64_t to = std::max(end, allocated_ + kDirectWriteAllocateBytes);
        if (::fallocate(fd_, 0, static_cast<off_t>(allocated_), static_cast<off_t>(to - allocated_)) != 0) {
            allocate_ = false;
            return;
        }
        allocated_ = to;
#else
        (void)end;
#endif
    }

    void write_at(const char* p, size_t n, uint64_t at) {
        bool rewritten = false;
        while (n) {
            ssize_t w = ::pwrite(fd_, p, n, static_cast<off_t>(at));
            if (w < 0 && errno == EINVAL && opened_direct_ && !rewritten) {
                // The filesystem took O_DIRECT at open but wants other
                // alignment: the rest goes through the page cache. Another
                // block's thread may have switched it over already.
                buffered();
                rewritten = true;
                continue;
            }
            if (w < 0 && errno == EINTR) continue;
            if (w <= 0) throw std::runtime_error("cannot write " + path_ + ": " + std::strerror(w < 0 ? errno : EIO));
            p += w;
            n -= size_t(w);
            at += uint64_t(w);
        }
    }

    void buffered() {
#ifdef O_DIRECT
        std::lock_guard<std::mutex> lock(mode_);
        if (!direct_) return;
        int flags = fcntl(fd_, F_GETFL);
        if (flags < 0 || fcntl(fd_, F_SETFL, flags & ~O_DIRECT) != 0)
            throw std::runtime_error("cannot clear O_DIRECT on " + path_ + ": " + std::strerror(errno));
        direct_ = false;
#endif
    }

    std::string path_;
    size_t block_bytes_, in_flight_;
    int fd_ = -1;
    bool opened_direct_ = false;
    std::atomic<bool> direct_{false};  // cleared by a writing thread on a fallback
    std::mutex mode_;
    bool allocate_ = true;
    uint64_t allocated_ = 0;     // the file's size, allocated ahead
    uint64_t appended_ = 0;      // bytes laid out
    std::vector<Piece> pieces_;  // of the block being laid out
    size_t filled_ = 0;          // its bytes so far
    std::vector<Block> free_;
    std::deque<Block> busy_;  // in flight, oldest first
};
