// How geocoder-diff writes the logical files it decodes a dir's packed
// families into, in its scratch dir. Each file goes out through buffers of
// BUFFER_BYTES, in write() calls of that size: std::ofstream's 8 KiB buffer
// takes a call each 8 KiB, which over a planet day's 18 GB of logical files
// is seconds of system time.
#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <new>
#include <string>
#include <sys/mman.h>
#include <unistd.h>
#include <utility>

namespace logical_decode {

// The bytes a writer holds before it writes them.
inline constexpr size_t BUFFER_BYTES = size_t(1) << 20;

// Writes the n bytes at p to fd, where it stands; false on an error.
inline bool write_all(int fd, const char* p, size_t n) {
    while (n) {
        const ssize_t w = ::write(fd, p, n);
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) return false;
        p += w;
        n -= size_t(w);
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
// Buffer, taken at the first write. Like an std::ofstream's, a write that
// failed, or a file that could not be made, makes flush() false from then
// on.
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

private:
    void drain() {
        if (used_) put(buffer_.data(), used_);
        used_ = 0;
    }
    void put(const char* p, size_t n) {
        if (ok_) ok_ = fd_ >= 0 && write_all(fd_, p, n);
    }

    int fd_;
    Buffer buffer_;
    size_t used_ = 0;
    bool ok_ = true;
};

}  // namespace logical_decode
