#pragma once

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unistd.h>

// What O_DIRECT needs a write's memory, offset and length aligned to: the
// logical block size, 512 or 4096 on the disks in use.
constexpr size_t kDirectWriteAlign = 4096;
// The bytes one direct write sends (a multiple of kDirectWriteAlign). The
// test box's NVMe took 3.4 GB/s in 16 or 64 MiB writes.
constexpr size_t kDirectWriteBlockBytes = size_t(32) << 20;

// Writes a new file front to back with O_DIRECT where the filesystem takes
// it. Through the page cache a planet-sized file went at 0.9 GB/s on the
// test box's NVMe whatever the write size, its writeback and page reclaim
// the limit; direct writes of whole blocks went at 3.4 GB/s. Appended bytes
// gather in an aligned block, written on a thread of its own while the next
// one fills. The last partial block, and everything on a filesystem that
// refuses O_DIRECT (tmpfs), goes through the page cache instead.
class DirectFileWriter {
public:
    explicit DirectFileWriter(const std::string& path, bool direct = true,
                              size_t block_bytes = kDirectWriteBlockBytes)
        : path_(path), block_bytes_(std::max(kDirectWriteAlign, block_bytes / kDirectWriteAlign * kDirectWriteAlign)) {
        const int flags = O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC;
#ifdef O_DIRECT
        if (direct) {
            fd_ = ::open(path.c_str(), flags | O_DIRECT, 0644);
            direct_ = fd_ >= 0;
        }
#else
        (void)direct;
#endif
        if (fd_ < 0) fd_ = ::open(path.c_str(), flags, 0644);
        if (fd_ < 0) throw std::runtime_error("cannot open " + path + ": " + std::strerror(errno));
        fill_ = new_block();
        spare_ = new_block();
    }
    ~DirectFileWriter() {
        if (pending_.valid()) pending_.wait();
        if (fd_ >= 0) ::close(fd_);
    }
    DirectFileWriter(const DirectFileWriter&) = delete;
    DirectFileWriter& operator=(const DirectFileWriter&) = delete;

    void append(std::string_view bytes) {
        while (!bytes.empty()) {
            size_t n = std::min(bytes.size(), block_bytes_ - filled_);
            std::memcpy(fill_.get() + filled_, bytes.data(), n);
            filled_ += n;
            bytes.remove_prefix(n);
            if (filled_ == block_bytes_) submit_block();
        }
    }

    // Writes what is left and closes the file. Returns its size.
    uint64_t finish() {
        wait_pending();
        size_t whole = direct_ ? filled_ / kDirectWriteAlign * kDirectWriteAlign : 0;
        write_at(fill_.get(), whole, written_);
        if (whole < filled_) {
            buffered();
            write_at(fill_.get() + whole, filled_ - whole, written_ + whole);
        }
        written_ += filled_;
        filled_ = 0;
        int fd = fd_;
        fd_ = -1;
        if (::close(fd) != 0) throw std::runtime_error("cannot close " + path_ + ": " + std::strerror(errno));
        return written_;
    }

    // The writes go around the page cache (until finish() writes the tail).
    bool direct() const { return direct_; }

private:
    struct FreeBlock {
        void operator()(char* p) const { std::free(p); }
    };
    using Block = std::unique_ptr<char, FreeBlock>;

    Block new_block() const {
        void* p = std::aligned_alloc(kDirectWriteAlign, block_bytes_);
        if (!p) throw std::bad_alloc();
        return Block(static_cast<char*>(p));
    }

    // Hands the full block to the writing thread and fills the spare one,
    // once the block before is written.
    void submit_block() {
        wait_pending();
        std::swap(fill_, spare_);
        const char* block = spare_.get();
        uint64_t at = written_;
        written_ += block_bytes_;
        filled_ = 0;
        pending_ = std::async(std::launch::async, [this, block, at] { write_at(block, block_bytes_, at); });
    }

    void wait_pending() {
        if (pending_.valid()) pending_.get();
    }

    void write_at(const char* p, size_t n, uint64_t at) {
        while (n) {
            ssize_t w = ::pwrite(fd_, p, n, static_cast<off_t>(at));
            if (w < 0 && errno == EINVAL && direct_) {
                // The filesystem took O_DIRECT at open but wants other
                // alignment: the rest goes through the page cache.
                buffered();
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
        if (!direct_) return;
        int flags = fcntl(fd_, F_GETFL);
        if (flags < 0 || fcntl(fd_, F_SETFL, flags & ~O_DIRECT) != 0)
            throw std::runtime_error("cannot clear O_DIRECT on " + path_ + ": " + std::strerror(errno));
        direct_ = false;
#endif
    }

    std::string path_;
    size_t block_bytes_;
    int fd_ = -1;
    std::atomic<bool> direct_{false};  // cleared by the writing thread on a fallback
    Block fill_, spare_;
    size_t filled_ = 0;     // bytes of fill_ in use
    uint64_t written_ = 0;  // file offset of fill_
    // Declared last, so that it is destroyed (waited for) before the blocks.
    std::future<void> pending_;
};
