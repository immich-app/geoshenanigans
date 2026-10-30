// geocoder-patch's first step: the compressed patch decompressed into a file, which it then maps.
// In process with the static libzstd, or by the zstd command on PATH in a build without it
// (GEOCODER_PATCH_ZSTD_CLI, -DGEOCODER_PATCH_LIBZSTD=OFF).
#pragma once
#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <spawn.h>
#include <string>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include "patch_format.h"

#ifndef GEOCODER_PATCH_ZSTD_CLI
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

// How far ahead of the decoder the decompressed patch's storage is reserved.
static constexpr size_t PATCH_RESERVE_STEP = 1 << 20;

// The patch, decompressed block by block straight into a mapped file: the decoder's history is the
// output itself, so no window buffer (the zstd CLI holds the 8 MiB window in anonymous memory).
// Each block's dst capacity ends where posix_fallocate has reserved storage, so a full TMPDIR is
// an error here, never a SIGBUS from a mapped page with nothing behind it. Only the mapping spans
// the bound: a frame without a content size bounds every block at 128 KiB, which puts the
// 2026-10-13 patches' bounds at 4.9x to 19x their size. A bound past size_t (a 32-bit build's) is
// refused: narrowed, the mapping would end before the capacities handed to the decoder.
inline bool decompress_to_file(const std::string& src, const std::string& dst) {
    MappedFile in = mmap_file(src);
    if (!in.data) return false;
    const unsigned long long bound = ZSTD_decompressBound(in.data, in.size);
    if (bound == ZSTD_CONTENTSIZE_ERROR || bound == 0 || uint64_t(bound) > uint64_t(SIZE_MAX)) { unmap_file(in); return false; }
    int fd = open(dst.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) { unmap_file(in); return false; }
    char* out = static_cast<char*>(mmap(nullptr, (size_t)bound, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
    if (out == MAP_FAILED) { close(fd); unmap_file(in); return false; }
    ZSTD_DCtx* dctx = ZSTD_createDCtx();
    size_t at = 0, n = 0, reserved = 0;
    bool ok = dctx != nullptr;
    // A frame per pass, concatenated and skippable ones as ZSTD_decompressDCtx takes them.
    while (ok && at < in.size) {
        ok = !ZSTD_isError(ZSTD_decompressBegin(dctx));
        for (size_t want; ok && (want = ZSTD_nextSrcSizeToDecompress(dctx)) != 0;) {
            if (want > in.size - at) { ok = false; break; }
            if (reserved - n < ZSTD_BLOCKSIZE_MAX && reserved < bound) {
                const size_t to = (size_t)std::min<unsigned long long>(bound, n + PATCH_RESERVE_STEP);
                if (const int err = posix_fallocate(fd, (off_t)reserved, (off_t)(to - reserved))) {
                    std::cerr << "Cannot reserve " << to << " bytes for " << dst << ": " << strerror(err) << std::endl;
                    ok = false;
                    break;
                }
                reserved = to;
            }
            const size_t r = ZSTD_decompressContinue(dctx, out + n, reserved - n, in.data + at, want);
            if (ZSTD_isError(r)) { ok = false; break; }
            at += want;
            n += r;
        }
    }
    ZSTD_freeDCtx(dctx);
    munmap(out, (size_t)bound);
    unmap_file(in);
    ok = ok && ftruncate(fd, (off_t)n) == 0;
    close(fd);
    return ok;
}
#else
// The zstd CLI on PATH (a -DGEOCODER_PATCH_LIBZSTD=OFF build), run without a
// shell: the patch path is one argument whatever its characters, after "--"
// so a leading '-' is no option.
inline bool decompress_to_file(const std::string& src, const std::string& dst) {
    const char* argv[] = {"zstd", "-d", "-f", "--quiet", "-o", dst.c_str(), "--", src.c_str(), nullptr};
    posix_spawn_file_actions_t quiet;
    if (posix_spawn_file_actions_init(&quiet) != 0) return false;
    pid_t pid;
    int err = posix_spawn_file_actions_addopen(&quiet, STDERR_FILENO, "/dev/null", O_WRONLY, 0);
    if (err == 0) err = posix_spawnp(&pid, "zstd", &quiet, nullptr, const_cast<char* const*>(argv), environ);
    posix_spawn_file_actions_destroy(&quiet);
    if (err != 0) return false;
    int status = 0;
    while (waitpid(pid, &status, 0) < 0)
        if (errno != EINTR) return false;
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}
#endif
