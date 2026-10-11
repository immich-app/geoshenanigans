#pragma once

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <iterator>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static_assert(sizeof(off_t) >= 8, "pread offsets must reach past 4 GiB");

// Window of a forward stream. Holds the largest cell list (2 + 65535 * 4 =
// 262,142 bytes) in one view.
static constexpr size_t SEQUENTIAL_READ_BUFFER_BYTES = 256 << 10;
// Runs shorter than a window go through it: a copy_file_range would cost a
// flush of the output's buffer and a syscall for less than one pread.
static constexpr uint64_t COPY_RANGE_MIN_BYTES = SEQUENTIAL_READ_BUFFER_BYTES;
// One copy_file_range call at most; the kernel caps a call near 2 GiB.
static constexpr size_t COPY_RANGE_CALL_BYTES = 1 << 30;

// Fills dst[0, n) from `off` through read_at(dst, n, off) (pread semantics),
// retrying short reads and EINTR. Throws on an error or an early end of file.
template <typename ReadAt>
void read_fully(char* dst, size_t n, uint64_t off, ReadAt read_at, const std::string& path) {
    while (n > 0) {
        ssize_t r = read_at(dst, n, off);
        if (r < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error("Cannot read " + path + ": " + std::strerror(errno));
        }
        if (r == 0) throw std::runtime_error("Unexpected end of " + path);
        dst += r;
        n -= static_cast<size_t>(r);
        off += static_cast<uint64_t>(r);
    }
}

// Errors after which copy_file_range moved nothing and the bytes can still
// go through user space: the kernel or the filesystems can't copy (EXDEV,
// EINVAL, ENOSYS, EOPNOTSUPP; EIO from some FUSE and NFS mounts), a seccomp
// filter blocks the call (EPERM, as older Docker profiles do), or the
// output was opened for appending (EBADF).
inline bool is_copy_unsupported(int err) {
    static constexpr int unsupported[] = {EXDEV, EINVAL, ENOSYS, EOPNOTSUPP, EPERM, EIO, EBADF};
    return std::find(std::begin(unsupported), std::end(unsupported), err) != std::end(unsupported);
}

struct KernelCopy {
    uint64_t copied = 0;
    // The copy can't be done in the kernel; don't try again.
    bool unsupported = false;
};

// Copies n bytes from `off` through copy_at(off, k) (copy_file_range
// semantics), retrying EINTR and short copies. Stops early, unsupported, on
// an is_copy_unsupported errno or a copy of 0 bytes (some filesystems answer
// that before copying anything); the caller sends the rest. Throws on any
// other error.
template <typename CopyAt>
KernelCopy copy_in_kernel(uint64_t off, uint64_t n, CopyAt copy_at, const std::string& path) {
    KernelCopy c;
    while (c.copied < n) {
        size_t k = static_cast<size_t>(std::min<uint64_t>(n - c.copied, COPY_RANGE_CALL_BYTES));
        ssize_t r = copy_at(off + c.copied, k);
        if (r > 0) {
            c.copied += static_cast<uint64_t>(r);
            continue;
        }
        if (r < 0 && errno == EINTR) continue;
        if (r == 0 || is_copy_unsupported(errno)) {
            c.unsupported = true;
            return c;
        }
        throw std::runtime_error("Cannot copy " + path + ": " + std::strerror(errno));
    }
    return c;
}

// Reads one file front to back through pread into a bounded window, with
// sequential readahead advice. With mmap the old-file reads happened inside
// page faults, whose size depended on how the faults arrived (it swung with
// glibc heap state); a pread is always as large as the request. A missing
// file reads as empty, like mmap_file.
class SequentialFileReader {
public:
    explicit SequentialFileReader(const std::string& path, size_t capacity = SEQUENTIAL_READ_BUFFER_BYTES)
        : path_(path), capacity_(std::max<size_t>(capacity, 1)) {
        fd_ = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd_ < 0) return;
        struct stat st;
        if (fstat(fd_, &st) != 0) {
            int err = errno;
            ::close(fd_);
            throw std::runtime_error("Cannot read " + path + ": " + std::strerror(err));
        }
        size_ = static_cast<uint64_t>(st.st_size);
#ifdef POSIX_FADV_SEQUENTIAL
        // Advice only: a failure changes nothing but readahead.
        posix_fadvise(fd_, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
    }
    ~SequentialFileReader() {
        // The window is an anonymous mapping, not the heap: glibc moves its
        // mmap and trim thresholds after large frees, which changes how the
        // rest of the patcher's memory is laid out.
        if (buf_) munmap(buf_, capacity_);
        if (fd_ >= 0) ::close(fd_);
    }
    SequentialFileReader(const SequentialFileReader&) = delete;
    SequentialFileReader& operator=(const SequentialFileReader&) = delete;

    // open() succeeded; an empty file is open with size 0.
    bool is_open() const { return fd_ >= 0; }
    uint64_t size() const { return size_; }
    const std::string& path() const { return path_; }
    // Views that started before the window: still correct, but each one
    // reads the file again.
    uint64_t rewinds() const { return rewinds_; }

    // Bytes [off, off + n), valid until the next call on this reader. A view
    // of 0 bytes is a valid pointer and reads nothing.
    const char* at(uint64_t off, size_t n) {
        // Before the window, rel wraps past win_len_.
        const uint64_t rel = off - win_off_;
        if (rel <= win_len_ && n <= win_len_ - rel && n > 0) return buf_ + rel;
        return refill(off, n);
    }

    // Copies [off, off + n) into dst straight from the file; the window is
    // untouched.
    void read(uint64_t off, char* dst, size_t n) {
        if (n == 0) return;
        require_in_file(off, n);
        read_fully(dst, n, off, pread_fn(), path_);
    }

    // Copies [off, off + n) to out_fd at its file position (which the copy
    // advances) with copy_file_range: the bytes never pass through user
    // space, and the filesystem may share extents instead of copying.
    // Returns the bytes copied. Fewer than n when the kernel, the
    // filesystems or a sandbox won't (another device, a pipe, an old kernel,
    // a seccomp filter); the caller sends the rest, and from then on
    // copy_supported() is false and calls return 0 at once.
    uint64_t copy_range(uint64_t off, uint64_t n, int out_fd) {
        require_in_file(off, n);
        if (copy_unsupported_) return 0;
#ifdef __linux__
        auto copy_at = [&](uint64_t from, size_t k) -> ssize_t {
            loff_t in_off = static_cast<loff_t>(from);
            return ::copy_file_range(fd_, &in_off, out_fd, nullptr, k, 0);
        };
        const KernelCopy c = copy_in_kernel(off, n, copy_at, path_);
        copy_unsupported_ = c.unsupported;
        return c.copied;
#else
        (void)out_fd;
        copy_unsupported_ = true;
        return 0;
#endif
    }
    bool copy_supported() const { return !copy_unsupported_; }

    // How many bytes from `off` on are already in the window: a view of
    // up to that many needs no read.
    uint64_t buffered_from(uint64_t off) const {
        const uint64_t win_end = win_off_ + win_len_;
        return off >= win_off_ && off < win_end ? win_end - off : 0;
    }

    // Hands [off, off + n) to sink(bytes, len) in pieces of at most one window.
    template <typename Sink>
    void stream(uint64_t off, uint64_t n, Sink sink) {
        while (n > 0) {
            size_t k = static_cast<size_t>(std::min<uint64_t>(n, capacity_));
            sink(at(off, k), k);
            off += k;
            n -= k;
        }
    }

private:
    struct PreadAt {
        int fd;
        ssize_t operator()(char* dst, size_t n, uint64_t off) const { return ::pread(fd, dst, n, static_cast<off_t>(off)); }
    };
    PreadAt pread_fn() const { return {fd_}; }
    void require_in_file(uint64_t off, uint64_t n) const {
        if (off > size_ || n > size_ - off) throw std::runtime_error("Read past end of " + path_);
    }
    static char* map_window(size_t bytes) {
        void* p = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (p == MAP_FAILED) throw std::runtime_error("Cannot map a read window");
        return static_cast<char*>(p);
    }

    // Out of line, so at() stays small enough to inline into per-record loops.
    [[gnu::noinline]] const char* refill(uint64_t off, size_t n) {
        if (n == 0) return &empty_;
        require_in_file(off, n);
        if (win_len_ > 0 && off < win_off_) rewinds_++;
        // The part of the view already in the window moves to the front, so
        // the next pread starts exactly where the last one ended.
        const size_t keep = static_cast<size_t>(buffered_from(off));
        if (!buf_ || n > capacity_) {
            // A view larger than the window grows it; no layout has one today.
            size_t cap = std::max(n, capacity_);
            char* grown = map_window(cap);
            if (keep) memcpy(grown, buf_ + (off - win_off_), keep);
            if (buf_) munmap(buf_, capacity_);
            buf_ = grown;
            capacity_ = cap;
        } else if (keep) {
            memmove(buf_, buf_ + (off - win_off_), keep);
        }
        const size_t len = static_cast<size_t>(std::min<uint64_t>(capacity_, size_ - off));
        read_fully(buf_ + keep, len - keep, off + keep, pread_fn(), path_);
        win_off_ = off;
        win_len_ = len;
        return buf_;
    }

    std::string path_;
    int fd_ = -1;
    uint64_t size_ = 0, win_off_ = 0, rewinds_ = 0;
    size_t capacity_, win_len_ = 0;
    char* buf_ = nullptr;  // mapped on the first view
    char empty_ = 0;
    bool copy_unsupported_ = false;
};

// Appends [off, off + n) of an old file to `out`. Bytes already in the
// reader's window are written from it, a long remainder is copied in the
// kernel, and the rest goes through the window.
inline void append_old_range(SequentialFileReader& old, FILE* out, uint64_t off, uint64_t n, const char* out_name) {
    const uint64_t head = std::min(n, old.buffered_from(off));
    if (head) fwrite(old.at(off, static_cast<size_t>(head)), 1, static_cast<size_t>(head), out);
    off += head;
    n -= head;
    if (n >= COPY_RANGE_MIN_BYTES && old.copy_supported()) {
        // The copy writes at the descriptor's position, behind stdio's
        // buffer. A failed flush drops the buffered bytes, and the copy
        // would land where they belonged.
        if (fflush(out) != 0) throw std::runtime_error(std::string("Cannot write ") + out_name);
        const uint64_t copied = old.copy_range(off, n, fileno(out));
        off += copied;
        n -= copied;
    }
    old.stream(off, n, [&](const char* p, size_t k) { fwrite(p, 1, k, out); });
}
