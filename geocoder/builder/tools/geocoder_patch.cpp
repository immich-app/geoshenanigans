// geocoder-patch: Low-memory streaming patch application.
//
// Key design: old files stream front to back through pread (SequentialFileReader);
// the string tiers and the decompressed patch, read at random, stay mmapped.
// Long unchanged runs and unchanged files are copied in the kernel
// (copy_file_range). Output streams via fwrite (never accumulate), the entry
// pipeline goes cell-by-cell. Target: <1 GiB peak RSS for planet.
//
// Usage: geocoder-patch <current-dir> <patch-file> -o <output-dir> [--threads]
//
// Reads GCPATCH v9 ( REPLACE runs, street remap derived from the
// street_ways replay, PARALLEL_DELTA_STRIDE side files, unmapped ids dropped
// from street / addr / interp cell lists).
//
// Low-memory mode: when <current-dir> is an encoded installed set (it has a
// gc.manifest, see gcs_patch.h), the encoded families' old files are decoded
// and their new files encoded in process, and the output is the new encoded
// set. A family's records and their child stream replay together (JointChild).
// --threads runs each family's decoder and encoder in a thread of its own.

#include <algorithm>
#include <array>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <fstream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <malloc.h>
#include <memory>
#include <spawn.h>
#include <stdio_ext.h>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unordered_map>
#include <unordered_set>
#include <unistd.h>
#include <vector>

#include "checked_size.h"
#include "merge_sequence.h"
#include "patch_format.h"
#include "scratch_dir.h"
#include "sequential_file_reader.h"
#include "gcs_patch.h"
#ifdef GEOCODER_PATCH_LIBZSTD
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#endif


// --- Helpers ---
static double now_ms() {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}
// Anonymous resident memory (heap, buffers). Unlike statm's RSS it leaves
// out the page cache of mapped old files, which the kernel reclaims freely.
static size_t get_rss_anon_mb() {
    FILE* f = fopen("/proc/self/status", "r");
    if (!f) return 0;
    char line[128];
    size_t kb = 0;
    while (fgets(line, sizeof line, f))
        if (sscanf(line, "RssAnon: %zu kB", &kb) == 1) break;
    fclose(f);
    return kb / 1024;
}
// Page faults and storage reads so far. A read taken inside a page fault
// counts in majflt, a pread only in read_bytes, so a phase needs both to
// show where its old-file I/O went.
struct IoCounters { uint64_t majflt = 0, minflt = 0, read_bytes = 0; };
static IoCounters get_io_counters() {
    IoCounters c;
    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) == 0) { c.majflt = ru.ru_majflt; c.minflt = ru.ru_minflt; }
    if (FILE* f = fopen("/proc/self/io", "r")) {
        char line[128];
        unsigned long long v;
        while (fgets(line, sizeof line, f))
            if (sscanf(line, "read_bytes: %llu", &v) == 1) { c.read_bytes = v; break; }
        fclose(f);
    }
    return c;
}
// "maj 12, min 3400, read 1.5 MiB" between two snapshots.
static std::string io_delta(const IoCounters& from, const IoCounters& to) {
    char s[96];
    snprintf(s, sizeof s, "maj %llu, min %llu, read %.1f MiB",
             (unsigned long long)(to.majflt - from.majflt), (unsigned long long)(to.minflt - from.minflt),
             (to.read_bytes - from.read_bytes) / 1048576.0);
    return s;
}
// Elapsed time, anonymous RSS, and the faults and reads since the previous
// phase line.
static void log_phase(const char* label, double start) {
    static IoCounters last;
    IoCounters now = get_io_counters();
    std::cerr << "  [" << std::fixed << std::setprecision(1) << (now_ms() - start) / 1000.0
              << "s, " << get_rss_anon_mb() << " MiB anon, " << std::setprecision(2) << gcs::heap_in_use() / 1048576.0
              << std::setprecision(1) << " MiB heap, " << io_delta(last, now) << "] " << label << std::endl;
    last = now;
}
// Every old-file read is forward only on today's layouts; a rewind means a
// layout changed and the file is being read more than once.
static void log_rewinds(const SequentialFileReader& r) {
    if (r.rewinds()) std::cerr << "  " << r.path() << ": read backwards " << r.rewinds() << " times" << std::endl;
}

#ifdef GEOCODER_PATCH_LIBZSTD
// How far ahead of the decoder the decompressed patch's storage is reserved.
static constexpr size_t PATCH_RESERVE_STEP = 1 << 20;

// The patch, decompressed block by block straight into a mapped file: the decoder's history is the
// output itself, so no window buffer (the zstd CLI holds the 8 MiB window in anonymous memory).
// Each block's dst capacity ends where posix_fallocate has reserved storage, so a full TMPDIR is
// an error here, never a SIGBUS from a mapped page with nothing behind it. Only the mapping spans
// the bound: a frame without a content size bounds every block at 128 KiB, which puts the
// 2026-10-13 patches' bounds at 4.9x to 19x their size. A bound past size_t (a 32-bit build's) is
// refused: narrowed, the mapping would end before the capacities handed to the decoder.
static bool decompress_to_file(const std::string& src, const std::string& dst) {
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
// The zstd CLI, run without a shell: the patch path is one argument whatever
// its characters, after "--" so a leading '-' is no option.
static bool decompress_to_file(const std::string& src, const std::string& dst) {
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

// The child stream replayed along a parent record file (UINT32_MAX: none).
static uint32_t joint_child_of(uint32_t fid) {
    switch ((PatchFileId)fid) {
    case PatchFileId::STREET_WAYS: return (uint32_t)PatchFileId::STREET_NODES;
    case PatchFileId::INTERP_WAYS: return (uint32_t)PatchFileId::INTERP_NODES;
    case PatchFileId::ADDR_POINTS: return (uint32_t)PatchFileId::ADDR_VERTICES;
    case PatchFileId::POI_RECORDS: return (uint32_t)PatchFileId::POI_VERTICES;
    default: return UINT32_MAX;
    }
}

// A child section (the one right after its parent's) replayed on demand: produce(n) writes the
// next n bytes of the new child file, taking MATCH bytes from the old child file, INSERT / REPLACE
// bytes from the patch, and skipping DELETE runs as it meets them.
struct JointChild {
    enum Kind { MERGE, COPY_OLD, FULL };
    Kind kind = MERGE;
    uint32_t fid = 0, stride = 1;
    uint64_t old_size = 0, new_size = 0;
    const char* P = nullptr;
    size_t q = 0, seq = 0, seq_end = 0, section_end = 0, data = 0;
    std::unique_ptr<SequentialFileReader> old;
    FILE* out = nullptr;
    FILE* parent = nullptr;
    std::string name;
    uint8_t op = 0xFF;
    uint64_t left = 0, old_bytes = 0, written = 0;
    bool started = false;
    size_t scan_q = 0;    // hint(): the sequence position past the next MATCH op found
    uint64_t scan_t = 0;  // and that op's old offset

    void parse(const char* p, size_t size, size_t at, uint32_t want_fid) {
        P = p;
        // Lengths in 64 bits: narrowed to a 32-bit size_t first, one past the patch would pass.
        auto need = [&](uint64_t n) { if (n > size - at) throw std::runtime_error("Truncated patch: child section"); };
        auto u32 = [&]() { need(4); uint32_t v; memcpy(&v, P + at, 4); at += 4; return v; };
        auto u64 = [&]() { need(8); uint64_t v; memcpy(&v, P + at, 8); at += 8; return v; };
        fid = u32();
        if (fid != want_fid)
            throw std::runtime_error(std::string("Encoded set: ") + patch_file_names[want_fid] + " does not follow its parent in the patch");
        stride = u32();
        old_size = u64();
        new_size = u64();
        if (stride == COPY_OLD_STRIDE || stride == 0) {
            u32();
            const uint64_t ds = u64();
            need(ds);
            kind = stride == 0 ? FULL : COPY_OLD;
            data = at;
            at += size_t(ds);
            if (kind == FULL && ds != new_size) throw std::runtime_error("Malformed child full replacement");
            stride = 1;
        } else if (stride == SPARSE_DELTA_STRIDE || stride == CELL_LIST_DELTA_STRIDE || stride == PARALLEL_DELTA_STRIDE) {
            throw std::runtime_error("Encoded set: unexpected section kind for a child stream");
        } else {
            const uint32_t n_runs = u32(), n_values = u32(), runs_size = u32(), values_size = u32();
            if (n_runs || n_values) throw std::runtime_error("Encoded set: a child stream with offset fixups");
            need(uint64_t(runs_size) + values_size);
            at += size_t(uint64_t(runs_size) + values_size);
            const uint64_t ss = u64();
            need(ss);
            seq = q = at;
            seq_end = at + size_t(ss);
            at = seq_end;
        }
        section_end = at;
    }
    // Moves to the next op that writes bytes, skipping DELETE runs; false at the end.
    bool fetch() {
        if (kind != MERGE) {
            if (started) return false;
            started = true;
            op = kind == FULL ? OP_INSERT_RUN : OP_MATCH_RUN;
            left = kind == FULL ? new_size : old->size();
            return left > 0;
        }
        for (;;) {
            if (q >= seq_end) return false;
            if (q + 5 > seq_end) throw std::runtime_error("Malformed merge sequence: " + name);
            const uint8_t o = (uint8_t)P[q++];
            uint32_t count; memcpy(&count, P + q, 4); q += 4;
            const uint64_t bytes = (uint64_t)count * stride;
            if (o == OP_DELETE_RUN) { old_bytes += bytes; continue; }
            if (bytes == 0) continue;
            if (o == OP_MATCH_RUN) { op = o; left = bytes; return true; }
            if (o == OP_INSERT_RUN || o == OP_REPLACE_RUN) {
                if (q + bytes > seq_end) throw std::runtime_error("Malformed merge sequence: " + name);
                op = o; data = q; q += bytes; left = bytes;
                return true;
            }
            throw std::runtime_error("Malformed merge sequence: " + name);
        }
    }
    uint8_t peek() {
        while (left == 0)
            if (!fetch()) throw std::runtime_error(name + " ends before its records do");
        if (op == OP_MATCH_RUN) {
            if (old_bytes >= old->size()) throw std::runtime_error("Malformed merge sequence: " + name);
            return (uint8_t)*old->at(old_bytes, 1);
        }
        return (uint8_t)P[data];
    }
    void produce(uint64_t n) {
        // A child unit that would flush the child's buffer sends the parent's first (its records,
        // this one included), so the encoder never holds child bytes for records it doesn't have.
        if (parent && __fpending(out) + n > gcs::Store::SINK_BUF) fflush(parent);
        while (n) {
            while (left == 0)
                if (!fetch()) throw std::runtime_error(name + " ends before its records do");
            const uint64_t k = std::min(n, left);
            if (op == OP_MATCH_RUN) {
                if (old_bytes + k > old->size()) throw std::runtime_error("Malformed merge sequence: " + name);
                append_old_range(*old, out, old_bytes, k, name.c_str());
                old_bytes += k;
            } else {
                fwrite(P + data, 1, k, out);
                data += k;
                if (op == OP_REPLACE_RUN) old_bytes += k;
            }
            left -= k;
            n -= k;
            written += k;
        }
    }
    void finish() {
        while (left == 0 && fetch()) {}
        if (left) throw std::runtime_error(name + " has bytes past its last record");
    }
    // Called before the parent reads an old record: the old child bytes this replay will never read
    // (those of deleted or replaced parents, DELETE and REPLACE runs) are dropped as the shared
    // decoder produces them, so the parent's read doesn't queue them for the child. Everything
    // before the next MATCH op's start is such: MATCH runs read the old file in order.
    void hint() {
        if (kind == COPY_OLD) return;  // every old byte is read, in order
        if (kind == FULL) { old->skip_to(old->size()); return; }
        if (left && op == OP_MATCH_RUN) return;  // reading at old_bytes now
        if (q >= scan_q) {  // the MATCH op of the last scan has been fetched: scan on from here
            uint64_t t = old_bytes + (left && op == OP_REPLACE_RUN ? left : 0);
            size_t s = q;
            while (s < seq_end) {
                if (s + 5 > seq_end) throw std::runtime_error("Malformed merge sequence: " + name);
                const uint8_t o = (uint8_t)P[s];
                uint32_t count; memcpy(&count, P + s + 1, 4);
                s += 5;
                const uint64_t bytes = (uint64_t)count * stride;
                if (o == OP_MATCH_RUN) {
                    if (bytes) break;
                } else if (o == OP_DELETE_RUN) {
                    t += bytes;
                } else if (o == OP_INSERT_RUN || o == OP_REPLACE_RUN) {
                    if (s + bytes > seq_end) throw std::runtime_error("Malformed merge sequence: " + name);
                    s += bytes;
                    if (o == OP_REPLACE_RUN) t += bytes;
                } else {
                    throw std::runtime_error("Malformed merge sequence: " + name);
                }
            }
            scan_q = s;
            scan_t = std::min<uint64_t>(t, old->size());
        }
        old->skip_to(scan_t);
    }
    // Before the parent's buffer flushes, the child's goes out: the encoder then always has the
    // child bytes of every record it is handed (else a run of records without children, which
    // never fills the child's buffer, piles up behind the last record that has one).
    void before_parent_write(FILE* parent, size_t n) {
        if (__fpending(parent) + n > gcs::Store::SINK_BUF) fflush(out);
    }
};

// A joint parent's record size (the stride its merge sections give).
static size_t joint_parent_stride(uint32_t fid) {
    switch ((PatchFileId)fid) {
    case PatchFileId::STREET_WAYS: return WAY_HEADER_STRIDE;
    case PatchFileId::INTERP_WAYS: return INTERP_WAY_STRIDE;
    case PatchFileId::ADDR_POINTS: return ADDR_POINT_STRIDE;
    case PatchFileId::POI_RECORDS: return POI_RECORD_STRIDE;
    default: throw std::runtime_error("No child stream");
    }
}

// The child bytes a new parent record owns: its nodes, its footprint or its vertex block (whose
// size depends on the block's encoding byte, the next child byte to write).
static uint64_t child_bytes_of(uint32_t fid, const char* rec, JointChild& jc) {
    switch ((PatchFileId)fid) {
    case PatchFileId::STREET_WAYS:
    case PatchFileId::INTERP_WAYS: return (uint64_t)record_node_count(rec) * 8;
    case PatchFileId::ADDR_POINTS: {
        uint32_t vc; memcpy(&vc, rec + 24, 4);
        if (!vc) return 0;
        return 10 + (uint64_t)vc * (jc.peek() == 3 ? 8 : 4);
    }
    case PatchFileId::POI_RECORDS: {
        uint32_t vc; memcpy(&vc, rec + 12, 4);
        if (!vc) return 0;
        return 10 + (uint64_t)vc * 2 * (jc.peek() == 3 ? 4 : 2);
    }
    default: throw std::runtime_error("No child stream");
    }
}

static int run(int argc, char* argv[]) {
    if (argc < 5 || std::string(argv[3]) != "-o") {
        std::cerr << "Usage: geocoder-patch <current-dir> <patch-file> -o <output-dir> [--threads]" << std::endl;
        return 1;
    }
    std::string cur_dir = argv[1], patch_path = argv[2], out_dir = argv[4];
    // Fixed thresholds: a buffer of 128 KiB or more is its own mapping and goes back to the kernel
    // when freed (glibc's dynamic threshold otherwise moves such buffers onto the heap after the
    // first one is freed, and the heap keeps their pages), and the heap top is trimmed early.
    mallopt(M_MMAP_THRESHOLD, 128 << 10);
    mallopt(M_TRIM_THRESHOLD, 256 << 10);
    for (int i = 5; i < argc; i++) {
        std::string a = argv[i];
        if (a != "--threads") {
            std::cerr << "Unknown option " << a << std::endl;
            return 1;
        }
        gcs::store().threads = true;
        mallopt(M_ARENA_MAX, 1);  // one heap for the codec threads too
    }
    ensure_dir(out_dir);
    // Output files are truncated while the current files are still
    // read as the patch base, so in-place application corrupts both.
    {
        char cur_real[PATH_MAX], out_real[PATH_MAX];
        if (realpath(cur_dir.c_str(), cur_real) && realpath(out_dir.c_str(), out_real) &&
            std::strcmp(cur_real, out_real) == 0) {
            std::cerr << "Output dir must differ from the current dir: " << out_real << std::endl;
            return 1;
        }
    }
    // The output is exactly the patch's file set, so stale files can't stay.
    if (!std::filesystem::is_empty(out_dir)) throw std::runtime_error("Output dir is not empty: " + out_dir);
    // Low-memory mode: an encoded installed set (gc.manifest) is decoded and the new set encoded
    // in process.
    gcs::store().init(cur_dir, out_dir);
    if (gcs::store().active) {
        stream_source_factory() = [](const std::string& p) { return gcs::store().open_source(p); };
        virtual_size_hook() = [](const std::string& dir, const std::string& name, uint64_t expected) -> bool {
            gcs::Store& s = gcs::store();
            if (dir != s.cur_dir || !s.is_encoded(name)) return false;
            s.check_old_size(name, expected);
            return true;
        };
        std::cerr << "Encoded set: " << gcs::store().old_m.get("gc.families") << std::endl;
    }
    ScratchDir scratch("geocoder-patch");
    const std::string& tmpdir = scratch.path();
    double t_start = now_ms();

    // --- Phase 1: Decompress + mmap patch ---
    std::string raw_path = tmpdir + "/patch.raw";
    if (!decompress_to_file(patch_path, raw_path)) { std::cerr << "Failed to decompress" << std::endl; return 1; }
    MappedFile patch_map = mmap_file(raw_path);
    if (!patch_map.data) { std::cerr << "Failed to mmap patch" << std::endl; return 1; }
    // Don't remove temp file yet (mmap needs backing file for page eviction under memory pressure)
    // madvise random — we access sections non-sequentially but release pages after each section
    madvise(const_cast<char*>(patch_map.data), patch_map.size, MADV_RANDOM);
    const char* P = patch_map.data;
    size_t patch_size = patch_map.size;
    std::cerr << "Patch: " << patch_size << " bytes" << std::endl;
    log_phase("Decompress", t_start);

    size_t pos = 0;
    // Bounds-checked read of n bytes at the current position. Fails loudly
    // (rather than reading past the mmap) if a malformed/truncated patch would
    // run the cursor off the end. On the success path pos+n <= patch_size
    // always holds, so this never fires and the read bytes are unchanged.
    auto require_bytes = [&](uint64_t n, const char* what) {
        if (n > patch_size - pos)
            throw std::runtime_error("Truncated patch: need " + std::to_string(n) + " bytes for " +
                                     what + " at offset " + std::to_string(pos) +
                                     " (size " + std::to_string(patch_size) + ")");
    };
    auto ru32 = [&]() -> uint32_t { require_bytes(4, "u32"); uint32_t v; memcpy(&v, P+pos, 4); pos += 4; return v; };
    auto ru64 = [&]() -> uint64_t { require_bytes(8, "u64"); uint64_t v; memcpy(&v, P+pos, 8); pos += 8; return v; };
    auto take = [&](size_t n, const char* what) -> const char* {
        require_bytes(n, what);
        const char* p = P + pos;
        pos += n;
        return p;
    };

    // Header
    if (memcmp(take(8, "magic"), GCPATCH_MAGIC, 8) != 0) { std::cerr << "Bad magic" << std::endl; return 1; }
    uint32_t ver = ru32(); if (ver != GCPATCH_VERSION) { std::cerr << "Bad version" << std::endl; return 1; }
    ru32(); // flags

    // The exact file set out_dir must hold after the apply. Outputs the
    // variant does not ship (sections for files it lacks) are built in
    // scratch instead.
    const std::vector<ClientFile> client_files = parse_client_files(P, patch_size, pos);
    std::unordered_set<std::string> listed;
    for (const auto& f : client_files) {
        listed.insert(f.name);
        gcs::store().expect_out[f.name] = f.size;
    }
    gcs::store().plan_outputs();
    for (const auto& f : client_files)
        if (is_inline_client_file(f.name) && !write_file(out_dir + "/" + f.name, f.bytes))
            throw std::runtime_error("Cannot write " + f.name);
    auto out_path = [&](const std::string& name) {
        return (listed.count(name) ? out_dir : tmpdir) + "/" + name;
    };
    auto open_out = [&](const std::string& name) -> FILE* {
        if (gcs::store().is_encoded(name) && listed.count(name)) return gcs::store().open_sink(name);
        std::string p = out_path(name);
        FILE* f = fopen(p.c_str(), "wb");
        if (!f) throw std::runtime_error("Cannot write " + p);
        __fsetlocking(f, FSETLOCKING_BYCALLER);  // one thread writes each output: no lock per fwrite
        return f;
    };

    check_old_file_sizes(P, patch_size, pos, cur_dir);

    // --- Phase 2: String rebuild ---
    // Offsets are global across the build's tiers (tier t at the sum of the
    // sizes before it), so one remap covers every tier. The dir rebuilds the
    // tiers it ships from its own old ones and derives their runs; the diff
    // sends the runs of the tiers it doesn't ship but references.
    StringRemap str_remap;
    // The old shipped tiers stay mapped until the replays are done:
    // str_remap's derived runs check string starts against them.
    std::vector<MappedFile> old_str_pools;
    {
        const StringsSection strings = parse_strings_section(P, patch_size, pos);
        require_shipped_tiers_listed(strings.shipped, listed);
        uint32_t total_added = 0, total_deleted = 0;
        load_string_remap(strings, str_remap, [&](int t) {
            const char* name = STRING_TIER_FILES[t];
            const StringTierDiff& diff = strings.diffs[t];
            total_added += static_cast<uint32_t>(diff.added.size());
            total_deleted += static_cast<uint32_t>(diff.deleted.size());
            require_old_file_size(cur_dir, name, strings.old_size(t));
            MappedFile old_pool = mmap_file(cur_dir + "/" + name);
            if (old_pool.size != strings.old_size(t) || content_hash(old_pool.data, old_pool.size) != diff.old_hash)
                throw std::runtime_error(std::string("Old ") + name + " is not the one the patch was made from");
            FILE* fp = open_out(name);
            rebuild_string_tier(old_pool.data, old_pool.size, diff, [&](const char* p, size_t n) { fwrite(p, 1, n, fp); });
            close_patch_outputs({{fp, name}});
            MappedFile new_pool = mmap_file(out_path(name));
            if (new_pool.size != strings.new_size(t) || content_hash(new_pool.data, new_pool.size) != diff.new_hash)
                throw std::runtime_error(std::string("Rebuilt ") + name + " does not match the new build");
            str_remap.add_tier(old_pool.data, old_pool.size, strings.old_base[t], strings.new_base[t], diff);
            old_str_pools.push_back(old_pool);
            unmap_file(new_pool);
        });
        malloc_trim(0);
        std::cerr << "  Strings: +" << total_added << " -" << total_deleted << ", "
                  << str_remap.run_count() << " remap runs, " << strings.pairs.size() << " moved-string pairs" << std::endl;
    }
    // Release patch pages read so far (string section)
    madvise(const_cast<char*>(patch_map.data), pos, MADV_DONTNEED);
    log_phase("Strings", t_start);

    auto str_remap_lookup = [&](uint32_t old_off) { return str_remap.lookup(old_off); };

    // --- Phase 3: Merge replays (streaming output) ---
    // ID remaps: file_id → vector<uint32_t> where remap[old_idx] = new_idx
    // Old → new record ids of the replayed files the entry pipeline remaps by.
    std::unordered_map<uint32_t, RecordRemap> record_remaps;
    std::vector<uint64_t> geo_added, geo_removed, admin_added, admin_removed, poi_added, poi_removed, place_added, place_removed;
    // Street / addr / interp per-cell corrections by entries file id: where
    // they sit in the mapped patch and how many (read as the walk reaches them).
    std::unordered_map<uint32_t, std::pair<size_t, uint32_t>> geo_delta_refs;
    struct CellCorr { uint64_t cell_id; std::vector<uint32_t> ids; };
    std::unordered_map<uint32_t, std::vector<CellCorr>> entry_corrections;
    // CELL_INDEX_DELTA payloads by entries file id (admin / POI / place).
    std::unordered_map<uint32_t, std::vector<char>> cell_index_deltas;
    // POI parent-id remap (from POI_PARENT_REMAP_MARKER). Applied during
    // POI_RECORDS MATCH replay to bytes 24/28/32 of each record alongside
    // the str_remap on byte 16. Sorted by old_id for binary-search lookup.
    std::vector<std::pair<uint32_t,uint32_t>> poi_admin_remap;
    std::vector<std::pair<uint32_t,uint32_t>> poi_street_remap;
    auto poi_remap_lookup = [](const std::vector<std::pair<uint32_t,uint32_t>>& v,
                               uint32_t old_id) -> uint32_t {
        if (v.empty()) return old_id;
        auto it = std::lower_bound(v.begin(), v.end(),
            std::make_pair(old_id, (uint32_t)0));
        if (it != v.end() && it->first == old_id) return it->second;
        return old_id;
    };

    // Where each replayed file's merge sequence sits in the patch (v8 side
    // files replay along it).
    struct SeqRef { size_t start, size; size_t stride; };
    std::unordered_map<uint32_t, SeqRef> merge_seqs;

    // Low-memory mode: an encoded family's records and their child stream (ways and
    // their nodes, address points and their footprints, POI records and their vertices) come
    // from one decoder and go to one encoder, so the two sections replay together: each new
    // record, then its child bytes. The child section (at `at`) must follow the parent's.
    auto open_joint_child = [&](uint32_t parent_fid, FILE* parent_out, size_t at) {
        std::unique_ptr<JointChild> jc(new JointChild());
        jc->parse(P, patch_size, at, joint_child_of(parent_fid));
        const char* cname = patch_file_names[jc->fid];
        require_old_file_size(cur_dir, cname, jc->old_size);
        jc->old.reset(new SequentialFileReader(cur_dir + "/" + cname));
        jc->out = open_out(cname);
        jc->parent = parent_out;
        jc->name = cname;
        return jc;
    };
    // After the parent's output is closed: the child's last bytes, its checks, and the patch
    // position moves past its section.
    auto close_joint_child = [&](std::unique_ptr<JointChild>& jc, const char* parent_name) {
        jc->finish();
        close_patch_outputs({{jc->out, jc->name}});
        if (jc->written != jc->new_size)
            throw std::runtime_error(std::string("Rebuilt ") + jc->name + " does not match the new build");
        if (jc->kind == JointChild::MERGE) merge_seqs[jc->fid] = {jc->seq, jc->seq_end - jc->seq, jc->stride};
        std::cerr << "  " << jc->name << ": " << jc->written << " bytes, replayed with " << parent_name << std::endl;
        pos = jc->section_end;
        jc.reset();
    };

    bool saw_end = false;
    while (pos < patch_size) {
        uint32_t file_id = ru32();
        if (file_id == SECTION_END_MARKER) { saw_end = true; break; }

        // --- Metadata sections ---
        if (file_id == CELL_CHANGES_GEO_MARKER) {
            uint32_t na = ru32(), nr = ru32();
            require_bytes(((size_t)na + nr) * 8, "cell changes");
            geo_added.resize(na); geo_removed.resize(nr);
            for (uint32_t i = 0; i < na; i++) { memcpy(&geo_added[i], take(8, "cell id"), 8); }
            for (uint32_t i = 0; i < nr; i++) { memcpy(&geo_removed[i], take(8, "cell id"), 8); }
            std::cerr << "  Geo cells: +" << na << " -" << nr << std::endl;
            continue;
        }
        if (file_id == CELL_CHANGES_ADMIN_MARKER) {
            uint32_t na = ru32(), nr = ru32();
            require_bytes(((size_t)na + nr) * 8, "cell changes");
            admin_added.resize(na); admin_removed.resize(nr);
            for (uint32_t i = 0; i < na; i++) { memcpy(&admin_added[i], take(8, "cell id"), 8); }
            for (uint32_t i = 0; i < nr; i++) { memcpy(&admin_removed[i], take(8, "cell id"), 8); }
            std::cerr << "  Admin cells: +" << na << " -" << nr << std::endl;
            continue;
        }
        if (file_id == CELL_CHANGES_POI_MARKER) {
            uint32_t na = ru32(), nr = ru32();
            require_bytes(((size_t)na + nr) * 8, "cell changes");
            poi_added.resize(na); poi_removed.resize(nr);
            for (uint32_t i = 0; i < na; i++) { memcpy(&poi_added[i], take(8, "cell id"), 8); }
            for (uint32_t i = 0; i < nr; i++) { memcpy(&poi_removed[i], take(8, "cell id"), 8); }
            std::cerr << "  POI cells: +" << na << " -" << nr << std::endl;
            continue;
        }
        if (file_id == CELL_CHANGES_PLACE_MARKER) {
            uint32_t na = ru32(), nr = ru32();
            require_bytes(((size_t)na + nr) * 8, "cell changes");
            place_added.resize(na); place_removed.resize(nr);
            for (uint32_t i = 0; i < na; i++) { memcpy(&place_added[i], take(8, "cell id"), 8); }
            for (uint32_t i = 0; i < nr; i++) { memcpy(&place_removed[i], take(8, "cell id"), 8); }
            std::cerr << "  Place cells: +" << na << " -" << nr << std::endl;
            continue;
        }
        if (file_id == GEO_ENTRY_DELTA_MARKER) {
            uint32_t fid = ru32(), c = ru32();
            geo_delta_refs[fid] = {pos, c};
            scan_geo_list_deltas(P, patch_size, pos, c);
            std::cerr << "  Geo entry deltas " << fid << ": " << c << " cells" << std::endl;
            continue;
        }
        if (file_id == SECONDARY_REMAP_MARKER) {
            // Secondary matches extend the remap of a file already replayed
            // (the diff emits them after the merges); a file sent unchanged
            // has no remap and keeps its ids.
            uint32_t nf = ru32();
            for (uint32_t f = 0; f < nf; f++) {
                uint32_t fid = ru32(), np = ru32();
                require_bytes((size_t)np * 8, "secondary remap");
                auto it = record_remaps.find(fid);
                for (uint32_t i = 0; i < np; i++) {
                    uint32_t o = ru32(), n = ru32();
                    if (it != record_remaps.end()) it->second.add_pair(o, n);
                }
                std::cerr << "  Secondary remap " << fid << ": " << np << " pairs" << std::endl;
            }
            continue;
        }
        if (file_id == POI_PARENT_REMAP_MARKER) {
            // Format: n_admin(u32), [(o,n):u32]*na, n_street(u32), [(o,n):u32]*ns,
            //         n_postcode(u32), [(o,n):u32]*np
            uint32_t na = ru32();
            require_bytes((size_t)na * 8, "admin remap");
            poi_admin_remap.resize(na);
            for (uint32_t i = 0; i < na; i++) {
                uint32_t o = ru32(), n = ru32();
                poi_admin_remap[i] = {o, n};
            }
            uint32_t ns = ru32();
            require_bytes((size_t)ns * 8, "street remap");
            poi_street_remap.resize(ns);
            for (uint32_t i = 0; i < ns; i++) {
                uint32_t o = ru32(), n = ru32();
                poi_street_remap[i] = {o, n};
            }
            // Reserved postcode leg — always 0 pairs on the wire; skip.
            uint32_t np = ru32();
            for (uint32_t i = 0; i < np; i++) { ru32(); ru32(); }
            std::sort(poi_admin_remap.begin(), poi_admin_remap.end());
            std::sort(poi_street_remap.begin(), poi_street_remap.end());
            std::cerr << "  POI parent-id remap: admin_pairs=" << na
                      << " street_pairs=" << ns
                      << " postcode_pairs=" << np << std::endl;
            continue;
        }
        if (file_id == CELL_INDEX_DELTA_MARKER) {
            uint32_t fid = ru32();
            uint64_t n = ru64();
            const char* p = take(n, "cell index delta");
            cell_index_deltas[fid].assign(p, p + n);
            std::cerr << "  Cell index delta " << fid << ": " << n << " bytes" << std::endl;
            continue;
        }
        if (file_id == ENTRY_CORRECTION_MARKER) {
            uint32_t fid = ru32(), c = ru32();
            auto& list = entry_corrections[fid];
            for (uint32_t i = 0; i < c; i++) {
                uint64_t cid; memcpy(&cid, take(8, "cell id"), 8);
                uint16_t ec; memcpy(&ec, take(2, "entry count"), 2);
                std::vector<uint32_t> ids(ec);
                if (ec > 0) memcpy(ids.data(), take((size_t)ec * 4, "entry ids"), (size_t)ec * 4);
                list.push_back({cid, std::move(ids)});
            }
            std::cerr << "  Entry corrections " << fid << ": " << c << std::endl;
            continue;
        }

        // --- Merge sequence replay (streaming) ---
        const IoCounters section_start = get_io_counters();
        auto section_io = [&] { return " [" + io_delta(section_start, get_io_counters()) + "]"; };
        uint32_t stride = ru32();
        uint64_t old_size = ru64(), new_size = ru64();
        if (file_id >= (uint32_t)PatchFileId::COUNT) { std::cerr << "Unknown file " << file_id << std::endl; return 1; }
        const char* fname = patch_file_names[file_id];
        require_old_file_size(cur_dir, fname, old_size);

        if (stride == 0) {
            // Full replacement — write directly from patch mmap
            uint32_t nf = ru32(); (void)nf; uint64_t ds = ru64();
            const char* data = take(ds, "full replacement");
            if (gcs::store().is_encoded(fname) && joint_child_of(file_id) != UINT32_MAX)
                throw std::runtime_error(std::string("Encoded set: a full replacement of ") + fname + " is not replayed with its child yet");
            if (gcs::store().is_encoded(fname)) {
                FILE* f = open_out(fname);
                fwrite(data, 1, ds, f);
                close_patch_outputs({{f, fname}});
            } else if (!write_file(out_path(fname), data, ds)) throw std::runtime_error(std::string("Cannot write ") + fname);
            std::cerr << "  " << fname << ": full replace " << ds << " bytes" << std::endl;
            continue;
        }
        if (stride == COPY_OLD_STRIDE) {
            // Unchanged file: the diff verified old==new and emitted no
            // data. Reproduce by copying the old (current) file verbatim.
            uint32_t nf = ru32(); (void)nf; take(ru64(), "copy-old payload"); // empty
            SequentialFileReader in(cur_dir + "/" + fname);
            if (!in.is_open()) {
                // The diff only emits COPY_OLD for a file that existed (and was
                // byte-identical) at diff time, so a missing source here is a
                // real error — surface it loudly instead of silently writing a
                // truncated/empty file and logging a false success.
                std::cerr << "  ERROR: " << fname << ": copy-old failed (src MISSING)" << std::endl;
                return 1;
            }
            FILE* out = open_out(fname);
            if (gcs::store().is_encoded(fname) && joint_child_of(file_id) != UINT32_MAX) {
                // An unchanged parent of an encoded family: one MATCH of every record, each followed
                // by its child bytes (from the child section, which may still change them).
                const size_t rs = joint_parent_stride(file_id);
                if (in.size() % rs) throw std::runtime_error(std::string("Old ") + fname + " is not whole records");
                std::unique_ptr<JointChild> jc = open_joint_child(file_id, out, pos);
                for (uint64_t off = 0; off < in.size(); off += rs) {
                    jc->hint();
                    const char* rec = in.at(off, rs);
                    jc->before_parent_write(out, rs);
                    fwrite(rec, 1, rs, out);
                    jc->produce(child_bytes_of(file_id, rec, *jc));
                }
                close_patch_outputs({{out, fname}});
                std::cerr << "  " << fname << ": unchanged (copied " << in.size() << " bytes from old)" << section_io() << std::endl;
                close_joint_child(jc, fname);
                continue;
            }
            append_old_range(in, out, 0, in.size(), fname);
            close_patch_outputs({{out, fname}});
            const uint64_t written = in.size();
            std::cerr << "  " << fname << ": unchanged (copied " << written
                      << " bytes from old)" << section_io() << std::endl;
            continue;
        }
        if (stride == CELL_LIST_DELTA_STRIDE) {
            // A cell index patched per cell; the paired entries file is
            // rebuilt from the same lists.
            std::string cells_name = fname;
            const std::string suffix = "_cells.bin";
            if (cells_name.size() < suffix.size() || cells_name.compare(cells_name.size() - suffix.size(), suffix.size(), suffix) != 0)
                throw std::runtime_error("Cell list delta for " + cells_name);
            std::string entries_name = cells_name.substr(0, cells_name.size() - suffix.size()) + "_entries.bin";
            uint64_t payload_size = ru64();
            if (payload_size < 8) throw std::runtime_error("Malformed cell list delta");
            const char* payload = take(payload_size, "cell list delta");
            uint64_t new_entries_size; memcpy(&new_entries_size, payload, 8);
            SequentialFileReader old_c(cur_dir + "/" + cells_name);
            SequentialFileReader old_e(cur_dir + "/" + entries_name);
            FILE* fc = open_out(cells_name);
            FILE* fe = open_out(entries_name);
            uint64_t cells_written = 0;
            uint64_t entries_written = stream_cell_list_delta(
                old_c, old_e, payload + 8, payload_size - 8,
                [&](const char* p, size_t n) { fwrite(p, 1, n, fc); cells_written += n; },
                [&](const char* p, size_t n) { fwrite(p, 1, n, fe); });
            close_patch_outputs({{fc, cells_name}, {fe, entries_name}});
            log_rewinds(old_c);
            log_rewinds(old_e);
            if (cells_written != new_size || entries_written != new_entries_size)
                throw std::runtime_error("Rebuilt " + cells_name + " does not match the new build");
            std::cerr << "  " << cells_name << " + " << entries_name << ": cell list delta, "
                      << cells_written / 12 << " cells" << section_io() << std::endl;
            continue;
        }
        if (stride == PARALLEL_DELTA_STRIDE) {
            // v8 side file: the parent's merge replayed at one value per
            // record, then the changed values (see PARALLEL_DELTA_STRIDE).
            uint32_t parent_fid = ru32(), value_stride = ru32(), remap_kind = ru32();
            check_parallel_delta_header(file_id, parent_fid, value_stride, remap_kind);
            uint32_t n_ins = ru32();
            const char* ins = take((size_t)n_ins * 4, "parallel values");
            uint32_t n_changes = ru32();
            const char* changes = take((size_t)n_changes * 8, "parallel changes");
            constexpr uint32_t NO_DATA_VAL = 0xFFFFFFFFu;
            auto remap_value = [&](uint32_t v) -> uint32_t {
                if (v == NO_DATA_VAL) return v;
                if (remap_kind == 1) return poi_remap_lookup(poi_admin_remap, v);
                if (remap_kind == 2) return str_remap_lookup(v);
                return v;
            };
            SequentialFileReader old(cur_dir + "/" + std::string(fname));
            FILE* out = open_out(fname);
            std::vector<uint32_t> buf;
            buf.reserve(1 << 14);  // 64 KiB
            uint64_t ni = 0, oi = 0;
            uint32_t ci = 0, ii = 0;
            uint32_t next_change = n_changes ? 0 : NO_DATA_VAL;
            if (n_changes) memcpy(&next_change, changes, 4);
            auto put = [&](uint32_t v) {
                if (ni == next_change && ci < n_changes) {
                    memcpy(&v, changes + (size_t)ci * 8 + 4, 4);
                    ci++;
                    next_change = NO_DATA_VAL;
                    if (ci < n_changes) memcpy(&next_change, changes + (size_t)ci * 8, 4);
                }
                buf.push_back(v);
                ni++;
                if (buf.size() == buf.capacity()) { fwrite(buf.data(), 4, buf.size(), out); buf.clear(); }
            };
            auto walk = [&](uint8_t op, uint32_t count) {
                if (op == OP_MATCH_RUN || op == OP_REPLACE_RUN) {
                    if ((oi + count) * 4 > old.size()) throw std::runtime_error("Malformed parallel delta");
                    for (uint32_t k = 0; k < count; k++) {
                        uint32_t v; memcpy(&v, old.at((oi + k) * 4, 4), 4);
                        put(remap_value(v));
                    }
                    oi += count;
                } else if (op == OP_INSERT_RUN) {
                    if ((uint64_t)ii + count > n_ins) throw std::runtime_error("Malformed parallel delta");
                    for (uint32_t k = 0; k < count; k++) { uint32_t v; memcpy(&v, ins + (size_t)(ii + k) * 4, 4); put(v); }
                    ii += count;
                } else if (op == OP_DELETE_RUN) {
                    oi += count;
                } else {
                    throw std::runtime_error("Malformed parallel delta");
                }
            };
            auto sit = merge_seqs.find(parent_fid);
            if (sit == merge_seqs.end()) {
                walk(OP_MATCH_RUN, static_cast<uint32_t>(new_size / 4));  // parent sent unchanged
            } else {
                size_t q = sit->second.start, qend = q + sit->second.size;
                while (q < qend) {
                    uint8_t op = P[q++];
                    uint32_t count; memcpy(&count, P + q, 4); q += 4;
                    if (op == OP_INSERT_RUN || op == OP_REPLACE_RUN) q += (size_t)count * sit->second.stride;
                    walk(op, count);
                }
            }
            if (!buf.empty()) fwrite(buf.data(), 4, buf.size(), out);
            close_patch_outputs({{out, fname}});
            if (ni * 4 != new_size || ci != n_changes || ii != n_ins)
                throw std::runtime_error(std::string("Parallel delta does not match the new build: ") + fname);
            log_rewinds(old);
            std::cerr << "  " << fname << ": parallel delta along " << patch_file_names[parent_fid] << ", "
                      << n_ins << " inserted, " << n_changes << " changes (" << new_size << " bytes)" << section_io() << std::endl;
            continue;
        }
        if (stride == SPARSE_DELTA_STRIDE) {
            // SPARSE_DELTA: position-keyed delta. Format already decoded
            // old_size + new_size above; next fields are value_stride,
            // remap_kind, n_changes, then the (pos, value) pairs.
            uint32_t value_stride = ru32();
            uint32_t remap_kind = ru32();
            uint32_t n_changes = ru32();
            // Whole records only: a change writes its record whole, so one in
            // a part record would write past the file.
            if ((value_stride != 4 && value_stride != 16) || new_size % value_stride)
                throw std::runtime_error("Malformed sparse delta");
            const size_t change_size = 4 + value_stride;
            const char* changes = take((size_t)n_changes * change_size, "sparse changes");

            // OLD bytes with the same remap the diff applied before computing
            // the delta, then each (pos, value) pair overwritten, give NEW.
            // Streamed through a 1 MiB buffer (the changes come sorted by
            // position): holding the whole file cost ~700 MiB of client
            // memory for planet addr_postcodes.
            SequentialFileReader old(cur_dir + "/" + std::string(fname));
            const size_t copy_n = std::min<uint64_t>({old_size, new_size, old.size()});
            constexpr uint32_t NO_DATA_VAL = 0xFFFFFFFFu;
            auto remap_value = [&](uint32_t v) -> uint32_t {
                if (v == NO_DATA_VAL) return v;
                if (remap_kind == 1) return poi_remap_lookup(poi_admin_remap, v);
                return str_remap_lookup(v);
            };
            // Field of each record the remap applies to (postcode_id at
            // byte 8 of a 16-byte postcode_centroid), or none.
            const bool remapped = (remap_kind == 1 && !poi_admin_remap.empty() && value_stride == 4) ||
                                  (remap_kind == 2 && !str_remap.empty() && value_stride == 4) ||
                                  (remap_kind == 3 && !str_remap.empty() && value_stride == 16);
            const size_t field = value_stride == 16 ? POSTCODE_CENTROID_POSTCODE_ID_OFF : 0;

            FILE* out = open_out(fname);
            constexpr size_t CHUNK = 64 << 10;  // a multiple of both value strides
            std::vector<char> buf(CHUNK);
            uint32_t ci = 0;
            uint64_t prev_pos = 0;
            for (size_t at = 0; at < (size_t)new_size; at += CHUNK) {
                size_t n = std::min(CHUNK, (size_t)new_size - at);
                size_t from_old = at < copy_n ? std::min(n, copy_n - at) : 0;
                old.read(at, buf.data(), from_old);
                if (from_old < n) memset(buf.data() + from_old, 0, n - from_old);
                if (remapped) {
                    for (size_t r = 0; r + value_stride <= from_old; r += value_stride) {
                        uint32_t v; memcpy(&v, buf.data() + r + field, 4);
                        uint32_t nv = remap_value(v);
                        if (nv != v) memcpy(buf.data() + r + field, &nv, 4);
                    }
                }
                for (; ci < n_changes; ci++) {
                    uint32_t arr_pos; memcpy(&arr_pos, changes + (size_t)ci * change_size, 4);
                    if (ci > 0 && arr_pos <= prev_pos) throw std::runtime_error("Malformed sparse delta");
                    size_t byte_off = (size_t)arr_pos * value_stride;
                    if (byte_off >= at + n) break;
                    prev_pos = arr_pos;
                    memcpy(buf.data() + (byte_off - at), changes + (size_t)ci * change_size + 4, value_stride);
                }
                fwrite(buf.data(), 1, n, out);
            }
            close_patch_outputs({{out, fname}});
            if (ci != n_changes) throw std::runtime_error(std::string("Malformed sparse delta: changes past the end of ") + fname);

            std::cerr << "  " << fname << ": sparse delta " << n_changes
                      << " changes (" << new_size << " bytes)" << section_io() << std::endl;
            continue;
        }

        size_t actual_stride = stride;

        // Records with string offset fields are rewritten one at a time.
        const std::vector<size_t> string_fields = string_field_offsets(static_cast<PatchFileId>(file_id));
        for (size_t off : string_fields)
            if (off + 4 > actual_stride) throw std::runtime_error(std::string("Malformed record stride: ") + fname);
        // An encoded family's parent replays with its child stream (open_joint_child): its records go
        // to the encoder whole, and each sizes its child bytes from fields up to its last byte (an
        // address's vertex_count at 24). A parent without records, old or new, has no child bytes
        // to replay with it: the diff sends an empty addr_points.bin's addr_vertices.bin later, as a
        // section of its own.
        const bool joint =
            gcs::store().is_encoded(fname) && joint_child_of(file_id) != UINT32_MAX && (old_size || new_size);
        if (joint && actual_stride != joint_parent_stride(file_id))
            throw std::runtime_error(std::string("Malformed record stride: ") + fname);
        bool needs_remap = !string_fields.empty();
        bool needs_padding = file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS && actual_stride == 24;

        // Offset fixups, decoded lazily during merge replay (zero allocation).
        uint32_t n_fixup_runs = ru32(), n_fixup_values = ru32();
        uint32_t fixup_runs_size = ru32(), fixup_values_size = ru32();
        const char* fixup_runs = take(fixup_runs_size, "fixup runs");
        const char* fixup_values = take(fixup_values_size, "fixup values");
        OffsetFixupReader fixups(fixup_runs, fixup_runs_size, n_fixup_runs,
                                 fixup_values, fixup_values_size, n_fixup_values);
        const bool has_fixups = n_fixup_runs > 0 || n_fixup_values > 0;
        // node_offset / vertex_offset: byte 0 for most types, byte 8 for POI
        // records, byte 20 for AddrPoint (after lat/lng/hn_id/st_id/pw_id).
        const size_t fixup_off =
            (file_id == (uint32_t)PatchFileId::POI_RECORDS) ? POI_RECORD_VERTEX_OFFSET_OFF :
            (file_id == (uint32_t)PatchFileId::ADDR_POINTS)  ? ADDR_POINT_VERTEX_OFFSET_OFF : (size_t)0;
        if (has_fixups && fixup_off + 4 > actual_stride) throw std::runtime_error("Malformed fixups");

        std::vector<size_t> remap_offs;
        if (!str_remap.empty()) remap_offs = string_fields;

        // The old file, read front to back (DELETE runs skip forward).
        SequentialFileReader old(cur_dir + "/" + std::string(fname));
        size_t n_old_records = old.size() / actual_stride;

        // Replay merge sequence — stream output, apply remap/fixups per-record inline
        uint64_t seq_size = ru64();
        require_bytes(seq_size, "merge sequence");
        size_t seq_end = pos + seq_size;
        merge_seqs[file_id] = {pos, (size_t)seq_size, actual_stride};
        FILE* outf = open_out(fname);

        std::unique_ptr<JointChild> jc;
        if (joint) jc = open_joint_child(file_id, outf, seq_end);
        auto child_bytes = [&](const char* rec) { return child_bytes_of(file_id, rec, *jc); };

        // Record the old → new record ids of the files whose remap the entry
        // pipeline reads back (geo, POI and place indexes).
        bool track = file_id == (uint32_t)PatchFileId::STREET_WAYS ||
                     file_id == (uint32_t)PatchFileId::ADDR_POINTS ||
                     file_id == (uint32_t)PatchFileId::INTERP_WAYS ||
                     file_id == (uint32_t)PatchFileId::POI_RECORDS ||
                     file_id == (uint32_t)PatchFileId::PLACE_NODES;
        RecordRemap remap(track ? static_cast<uint32_t>(n_old_records) : 0);
        // v8: addresses' parent_way_id moves by the street replay's remap
        // (MATCH / REPLACE runs plus the parent-id remap's moved pairs).
        const RecordRemap* street_rm = nullptr;
        if (file_id == (uint32_t)PatchFileId::ADDR_POINTS) {
            auto it = record_remaps.find((uint32_t)PatchFileId::STREET_WAYS);
            if (it != record_remaps.end()) street_rm = &it->second;
        }
        size_t street_hint = 0;

        // Per-record buffer for applying remap/fixups inline (avoid copying entire file)
        std::vector<char> rec_buf(actual_stride);

        size_t old_rec = 0, new_rec = 0;
        uint64_t old_bytes = 0, written = 0;
        while (pos < seq_end) {
            if (pos + 5 > seq_end) throw std::runtime_error(std::string("Malformed merge sequence: ") + fname);
            uint8_t op = P[pos++];
            uint32_t count; memcpy(&count, P+pos, 4); pos += 4;
            if (op == OP_MATCH_RUN) {
                // Fast path: files with no per-record transform (the
                // *_vertices byte streams, street_nodes / interp_nodes)
                // copy the whole run from old at once. The record loop
                // below costs one fwrite per record: one per byte of
                // planet/full's 3.4 GiB addr_vertices, one per node of its
                // 600M street_nodes.
                // Every record of the run is an old one, whole range checked before any is written
                // (in 64 bits: a 32-bit size_t wraps count * stride into a run that fits).
                const auto run = old_bytes <= old.size() ? checked_size<uint64_t>(0, count, actual_stride, old.size() - old_bytes)
                                                         : std::nullopt;
                if (!run)
                    throw std::runtime_error(std::string("Malformed merge sequence: ") + fname + " (a MATCH run past the old file)");
                const uint64_t run_bytes = *run;
                if (!jc && !needs_remap && !needs_padding && remap_offs.empty() && !has_fixups) {
                    append_old_range(old, outf, old_bytes, run_bytes, fname);
                    written += run_bytes;
                    old_rec += count; new_rec += count; old_bytes += run_bytes;
                    continue;
                }
                for (uint32_t k = 0; k < count; k++) {
                    const uint64_t rec_off = old_bytes + uint64_t(k) * actual_stride;
                    if (jc) jc->hint();
                    const char* rec = old.at(rec_off, actual_stride);

                    bool modified = false;
                    // Check if this record needs any modification
                    if (!remap_offs.empty() || needs_padding) modified = true;
                    // POI records: byte-32 parent_poly_id shifts with the
                    // admin id-space. (name_id/parent_street_id/parent_postcode_id
                    // are string offsets handled by remap_offs, which already
                    // set modified=true above.)
                    if (file_id == (uint32_t)PatchFileId::POI_RECORDS &&
                        !poi_admin_remap.empty())
                        modified = true;
                    // PlaceNode parent_poly_id (byte 16) shifts with admin id-space.
                    if (file_id == (uint32_t)PatchFileId::PLACE_NODES &&
                        actual_stride >= 20 && !poi_admin_remap.empty())
                        modified = true;
                    // AddrPoint parent_way_id (byte 16) shifts with street id-space.
                    if (file_id == (uint32_t)PatchFileId::ADDR_POINTS &&
                        actual_stride >= 20 && (street_rm || !poi_street_remap.empty()))
                        modified = true;
                    uint32_t old_off = 0, new_off = 0;
                    if (has_fixups) {
                        memcpy(&old_off, rec + fixup_off, 4);
                        new_off = fixups.apply(static_cast<uint32_t>(old_rec + k), old_off);
                    }
                    bool has_fixup = new_off != old_off;
                    if (has_fixup) modified = true;

                    if (modified) {
                        memcpy(rec_buf.data(), rec, actual_stride);
                        // Apply padding zeroing
                        if (file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS && actual_stride == 24)
                            memset(rec_buf.data() + 14, 0, 2); // preserve place_type_override at byte 13
                        // Apply string remap
                        for (size_t off : remap_offs) {
                            uint32_t v; memcpy(&v, rec_buf.data() + off, 4);
                            uint32_t nv = str_remap_lookup(v);
                            if (nv != v) memcpy(rec_buf.data() + off, &nv, 4);
                        }
                        // Apply POI parent_poly_id remap (byte 32, admin
                        // polygon index). Bytes 24/28 (parent_street_id,
                        // parent_postcode_id) are string offsets and are
                        // handled by the str_remap pass via remap_offs
                        // above — NOT here. Mirrors geocoder_diff.cpp.
                        if (file_id == (uint32_t)PatchFileId::POI_RECORDS) {
                            constexpr uint32_t NO_DATA = 0xFFFFFFFFu;
                            if (actual_stride >= 36 && !poi_admin_remap.empty()) {
                                uint32_t pp; memcpy(&pp, rec_buf.data() + POI_RECORD_PARENT_POLY_ID_OFF, 4);
                                if (pp != NO_DATA) {
                                    uint32_t npp = poi_remap_lookup(poi_admin_remap, pp);
                                    if (npp != pp) memcpy(rec_buf.data() + POI_RECORD_PARENT_POLY_ID_OFF, &npp, 4);
                                }
                            }
                        }
                        // PlaceNode parent_poly_id at byte 16 (20-byte stride).
                        if (file_id == (uint32_t)PatchFileId::PLACE_NODES &&
                            actual_stride >= 20 && !poi_admin_remap.empty()) {
                            constexpr uint32_t NO_DATA = 0xFFFFFFFFu;
                            uint32_t pp; memcpy(&pp, rec_buf.data() + PLACE_NODE_PARENT_POLY_ID_OFF, 4);
                            if (pp != NO_DATA) {
                                uint32_t npp = poi_remap_lookup(poi_admin_remap, pp);
                                if (npp != pp) memcpy(rec_buf.data() + PLACE_NODE_PARENT_POLY_ID_OFF, &npp, 4);
                            }
                        }
                        // AddrPoint parent_way_id at byte 16 (20+ stride).
                        if (file_id == (uint32_t)PatchFileId::ADDR_POINTS &&
                            actual_stride >= 20 && (street_rm || !poi_street_remap.empty())) {
                            constexpr uint32_t NO_DATA = 0xFFFFFFFFu;
                            uint32_t pw; memcpy(&pw, rec_buf.data() + ADDR_POINT_PARENT_WAY_ID_OFF, 4);
                            if (pw != NO_DATA) {
                                uint32_t npw;
                                if (street_rm) {
                                    npw = street_rm->lookup(pw, street_hint);
                                    if (npw == RecordRemap::NONE) npw = pw;
                                } else {
                                    npw = poi_remap_lookup(poi_street_remap, pw);
                                }
                                if (npw != pw) memcpy(rec_buf.data() + ADDR_POINT_PARENT_WAY_ID_OFF, &npw, 4);
                            }
                        }
                        if (has_fixup) memcpy(rec_buf.data() + fixup_off, &new_off, 4);
                        if (jc) jc->before_parent_write(outf, actual_stride);
                        fwrite(rec_buf.data(), 1, actual_stride, outf);
                        if (jc) jc->produce(child_bytes(rec_buf.data()));
                    } else {
                        // No modification needed: the record as read
                        if (jc) jc->before_parent_write(outf, actual_stride);
                        fwrite(rec, 1, actual_stride, outf);
                        if (jc) jc->produce(child_bytes(rec));
                    }
                }
                if (track) remap.add_match(static_cast<uint32_t>(old_rec), static_cast<uint32_t>(new_rec), count);
                written += run_bytes;
                old_rec += count; new_rec += count; old_bytes += run_bytes;
            } else if (op == OP_INSERT_RUN) {
                const auto run = checked_size(0, count, actual_stride, seq_end - pos);
                if (!run) throw std::runtime_error(std::string("Malformed merge sequence: ") + fname);
                const size_t bytes = *run;
                if (jc) {
                    for (uint32_t k = 0; k < count; k++) {
                        jc->before_parent_write(outf, actual_stride);
                        fwrite(P + pos + (size_t)k * actual_stride, 1, actual_stride, outf);
                        jc->produce(child_bytes(P + pos + (size_t)k * actual_stride));
                    }
                } else {
                    fwrite(P+pos, 1, bytes, outf);
                }
                written += bytes; pos += bytes; new_rec += count;
            } else if (op == OP_DELETE_RUN) {
                old_rec += count; old_bytes += uint64_t(count) * actual_stride;
                old.skip_to(old_bytes);  // an encoded family's decoder drops them
            } else if (op == OP_REPLACE_RUN) {
                // Same entities, new bytes: written like an INSERT, mapped like a MATCH.
                const auto run = checked_size(0, count, actual_stride, seq_end - pos);
                if (!run) throw std::runtime_error(std::string("Malformed merge sequence: ") + fname);
                const size_t bytes = *run;
                // The old records are never read: an encoded family's decoder drops them as the
                // children's MATCH reads below make it produce them.
                old.skip_to(old_bytes + bytes);
                if (jc) {
                    for (uint32_t k = 0; k < count; k++) {
                        jc->before_parent_write(outf, actual_stride);
                        fwrite(P + pos + (size_t)k * actual_stride, 1, actual_stride, outf);
                        jc->produce(child_bytes(P + pos + (size_t)k * actual_stride));
                    }
                } else {
                    fwrite(P+pos, 1, bytes, outf);
                }
                if (track) remap.add_match(static_cast<uint32_t>(old_rec), static_cast<uint32_t>(new_rec), count);
                written += bytes; pos += bytes;
                old_rec += count; new_rec += count; old_bytes += bytes;
            } else {
                throw std::runtime_error(std::string("Malformed merge sequence: ") + fname);
            }
        }
        close_patch_outputs({{outf, fname}});
        log_rewinds(old);
        if (jc) close_joint_child(jc, fname);

        if (track && file_id == (uint32_t)PatchFileId::STREET_WAYS) {
            // The moved ways' pairs (parent-id remap, street leg) complete
            // the street remap the address replay reads.
            for (const auto& [o, n] : poi_street_remap) remap.add_pair(o, n);
            remap.finish();
        }
        if (track) {
            record_remaps[file_id] = std::move(remap);
            std::cerr << "  " << fname << ": " << written << " bytes (remap of " << n_old_records << " records)"
                      << section_io() << std::endl;
        } else {
            std::cerr << "  " << fname << ": " << written << " bytes" << section_io() << std::endl;
        }
        // Release patch pages used by this section
        madvise(const_cast<char*>(patch_map.data), pos, MADV_DONTNEED);
        malloc_trim(0);
    }
    if (!saw_end) throw std::runtime_error("Truncated patch: no end marker");
    log_phase("Merge replays", t_start);

    // Free string remap + release all processed patch pages
    str_remap = StringRemap();
    for (auto& m : old_str_pools) if (m.data) unmap_file(m);
    old_str_pools.clear();
    madvise(const_cast<char*>(patch_map.data), pos, MADV_DONTNEED);
    malloc_trim(0);

    // --- Phase 4: Streaming entry pipeline ---
    // Walk old geo_cells, remap IDs, apply corrections, write output directly to files.
    {
        double t_entry = now_ms();
        std::cerr << "Entry pipeline (lean streaming)..." << std::endl;

        // A file sent unchanged has no remap: its ids keep their values.
        for (auto& kv : record_remaps) kv.second.finish();
        const RecordRemap no_remap;
        auto remap_of = [&](PatchFileId fid) -> const RecordRemap& {
            auto it = record_remaps.find((uint32_t)fid);
            return it != record_remaps.end() ? it->second : no_remap;
        };
        const RecordRemap& w_rm = remap_of(PatchFileId::STREET_WAYS);
        const RecordRemap& a_rm = remap_of(PatchFileId::ADDR_POINTS);
        const RecordRemap& i_rm = remap_of(PatchFileId::INTERP_WAYS);

        // The geo walk's old files, decoders and outputs close at the end of
        // this block, before the admin / POI / place rebuilds.
        {
        // The old geo index and its entries files, each read front to back:
        // the builder writes every list in cell order, removed cells and
        // NO_DATA lists only leave gaps.
        SequentialFileReader old_geo(cur_dir + "/geo_cells.bin");
        SequentialFileReader old_se(cur_dir + "/street_entries.bin");
        SequentialFileReader old_ae(cur_dir + "/addr_entries.bin");
        SequentialFileReader old_ie(cur_dir + "/interp_entries.bin");
        size_t n_old = old_geo.size() / 20;

        // Corrections and removed cells, sorted by cell id and read through
        // forward cursors: the walk below visits cells in ascending id
        // order, and a hash lookup plus three binary searches per cell cost
        // ~17 s of client CPU on planet's ~380M cells.
        std::sort(geo_removed.begin(), geo_removed.end());
        size_t removed_i = 0;
        auto is_removed = [&](uint64_t cid) {
            while (removed_i < geo_removed.size() && geo_removed[removed_i] < cid) removed_i++;
            return removed_i < geo_removed.size() && geo_removed[removed_i] == cid;
        };
        // The diff's per-cell corrections, in cell id order like the walk.
        auto cursor_of = [&](PatchFileId fid) {
            auto it = geo_delta_refs.find((uint32_t)fid);
            if (it == geo_delta_refs.end()) return GeoListDeltaCursor();
            return GeoListDeltaCursor(P, it->second.first, it->second.second);
        };
        GeoListDeltaCursor cur_s = cursor_of(PatchFileId::STREET_ENTRIES);
        GeoListDeltaCursor cur_a = cursor_of(PatchFileId::ADDR_ENTRIES);
        GeoListDeltaCursor cur_i = cursor_of(PatchFileId::INTERP_ENTRIES);
        std::vector<uint32_t> scratch;
        // Added cells sorted
        std::sort(geo_added.begin(), geo_added.end());
        log_phase("  Setup", t_entry);

        // Open 4 output files
        FILE* f_geo = open_out("geo_cells.bin");
        struct EntriesOut { FILE* f; uint64_t written; };
        EntriesOut o_se{open_out("street_entries.bin"), 0};
        EntriesOut o_ae{open_out("addr_entries.bin"), 0};
        EntriesOut o_ie{open_out("interp_entries.bin"), 0};
        constexpr uint32_t NO = 0xFFFFFFFF;

        // Reusable buffer (one per entry type to avoid aliasing issues)
        std::vector<uint32_t> buf;
        buf.reserve(4096);

        auto remap = [&](std::vector<uint32_t>& ids, const RecordRemap& rm) {
            constexpr uint32_t NO2 = 0xFFFFFFFF;
            if (&rm != &no_remap) {
                // v8: an old id the replay doesn't map is a deleted record.
                size_t w = 0;
                for (uint32_t id : ids) { uint32_t n = rm[id]; if (n != NO2) ids[w++] = n; }
                ids.resize(w);
            } else {
                for (auto& id : ids) if (id < rm.size() && rm[id] != NO2) id = rm[id];
            }
            std::sort(ids.begin(), ids.end());
        };
        // Write entry and return offset, or NO if empty. Offsets come from a
        // running count per output: ftell per list cost ~40 s of client CPU
        // on planet (~460M lists).
        // Encoded set: the four streams flush as a group whenever one of them would (lists, then
        // rows), so the encoder always has every list of the rows it is handed and holds at most
        // the lists of the cell in progress.
        auto gflush = [&] { for (FILE* f : {o_se.f, o_ae.f, o_ie.f, f_geo}) fflush(f); };
        auto emit = [&](EntriesOut& out, const uint32_t* ids, size_t n) -> uint32_t {
            if (n == 0) return NO;
            if (gcs::store().active && __fpending(out.f) + 2 + n * 4 > gcs::Store::SINK_BUF) gflush();
            uint32_t off = (uint32_t)out.written;
            uint16_t c = (uint16_t)n;
            fwrite(&c, 2, 1, out.f); fwrite(ids, 4, n, out.f);
            out.written += 2 + n * 4;
            return off;
        };

        // Merge-walk: old cells + added cells in sorted order
        // Both are sorted by cell_id. Merge them, skip removed.
        size_t old_i = 0, add_i = 0;
        size_t cells_written = 0;

        // The cell's derived list (old list, ids remapped), then its
        // correction from the diff, if it sent one.
        auto do_entry = [&](uint64_t cid, SequentialFileReader& old_e, uint32_t old_off, const RecordRemap& rm,
                            GeoListDeltaCursor& deltas, EntriesOut& outf) -> uint32_t {
            read_entry_list(old_e, old_off, buf);
            if (!buf.empty()) remap(buf, rm);
            if (deltas.at(cid)) deltas.apply(buf, scratch);
            if (buf.empty()) return NO;
            return emit(outf, buf.data(), buf.size());
        };
        // old_offs: the cell's street / addr / interp list offsets in the
        // old entries files, NO for a new cell.
        using ListOffsets = std::array<uint32_t, 3>;
        auto process_cell = [&](uint64_t cid, const ListOffsets& old_offs) {
            uint32_t so = do_entry(cid, old_se, old_offs[0], w_rm, cur_s, o_se);
            uint32_t ao = do_entry(cid, old_ae, old_offs[1], a_rm, cur_a, o_ae);
            uint32_t io = do_entry(cid, old_ie, old_offs[2], i_rm, cur_i, o_ie);
            if (gcs::store().active && __fpending(f_geo) + 20 > gcs::Store::SINK_BUF) gflush();
            fwrite(&cid, 8, 1, f_geo); fwrite(&so, 4, 1, f_geo); fwrite(&ao, 4, 1, f_geo); fwrite(&io, 4, 1, f_geo);
            cells_written++;
        };
        const ListOffsets added_cell = {NO, NO, NO};

        while (old_i < n_old || add_i < geo_added.size()) {
            uint64_t old_cid = UINT64_MAX, add_cid = UINT64_MAX;
            ListOffsets old_offs = added_cell;
            if (old_i < n_old) {
                // One 20-byte record: cell id, then the three list offsets.
                // size_t: planet has ~380M cells and `old_i * 20` passes 2^31.
                const char* rec = old_geo.at((uint64_t)old_i * 20, 20);
                memcpy(&old_cid, rec, 8);
                memcpy(old_offs.data(), rec + 8, 12);
            }
            if (add_i < geo_added.size()) add_cid = geo_added[add_i];

            if (old_cid <= add_cid) {
                if (!is_removed(old_cid)) {
                    process_cell(old_cid, old_offs);
                } else {
                    // An encoded set's lists come in row order: those before a removed row's are
                    // never read, and the gxc decoder drops them instead of queueing a removed
                    // stretch's lists for the next row kept.
                    SequentialFileReader* es[3] = {&old_se, &old_ae, &old_ie};
                    for (int t = 0; t < 3; t++)
                        if (old_offs[size_t(t)] != NO) es[t]->skip_to(old_offs[size_t(t)]);
                }
                old_i++;
                if (old_cid == add_cid) add_i++; // skip duplicate add
            } else {
                process_cell(add_cid, added_cell);
                add_i++;
            }

            if (cells_written % 10000000 == 0 && cells_written > 0) {
                std::cerr << "    " << cells_written << " cells, se=" << o_se.written/1024/1024
                          << "M ae=" << o_ae.written/1024/1024 << "M anon=" << get_rss_anon_mb() << "M" << std::endl;
            }
        }

        close_patch_outputs({{f_geo, "geo_cells.bin"}, {o_se.f, "street_entries.bin"}, {o_ae.f, "addr_entries.bin"},
                             {o_ie.f, "interp_entries.bin"}});
        for (const SequentialFileReader* r : {&old_geo, &old_se, &old_ae, &old_ie}) log_rewinds(*r);
        std::cerr << "  Geo: " << cells_written << " cells written" << std::endl;
        malloc_trim(0);
        log_phase("  Geo entries complete", t_entry);
        }  // geo walk

        // Writes a rebuilt cell index; a cell the diff corrected takes the
        // shipped id list instead of its rebuilt one.
        auto write_corrected_cells = [&](const char* label, PatchFileId entries_fid,
                                         const std::vector<char>& cells, const std::vector<char>& entries,
                                         const std::string& cells_name, const std::string& entries_name) {
            constexpr uint32_t no_data = 0xFFFFFFFF;
            size_t n = cells.size() / 12;
            auto ecit = entry_corrections.find((uint32_t)entries_fid);
            if (ecit == entry_corrections.end()) {
                if (!write_file(out_path(cells_name), cells) || !write_file(out_path(entries_name), entries))
                    throw std::runtime_error("Cannot write " + cells_name);
                std::cerr << "  " << label << ": " << n << " cells rebuilt" << std::endl;
                return;
            }
            std::unordered_map<uint64_t, const std::vector<uint32_t>*> corr;
            for (auto& c : ecit->second) corr[c.cell_id] = &c.ids;
            FILE* fc = open_out(cells_name);
            FILE* fe = open_out(entries_name);
            uint32_t wpos = 0;
            for (size_t i = 0; i < n; i++) {
                uint64_t cid; memcpy(&cid, cells.data() + i * 12, 8);
                fwrite(&cid, 8, 1, fc);
                auto cit = corr.find(cid);
                if (cit != corr.end()) {
                    uint32_t off = cit->second->empty() ? no_data : wpos;
                    fwrite(&off, 4, 1, fc);
                    if (!cit->second->empty()) {
                        uint16_t c = cit->second->size();
                        fwrite(&c, 2, 1, fe); fwrite(cit->second->data(), 4, c, fe);
                        wpos += 2 + c * 4;
                    }
                } else {
                    uint32_t old_off; memcpy(&old_off, cells.data() + i * 12 + 8, 4);
                    if (old_off != no_data && old_off + 2 <= entries.size()) {
                        fwrite(&wpos, 4, 1, fc);
                        uint16_t c; memcpy(&c, entries.data() + old_off, 2);
                        fwrite(entries.data() + old_off, 1, 2 + c * 4, fe);
                        wpos += 2 + c * 4;
                    } else {
                        fwrite(&no_data, 4, 1, fc);
                    }
                }
            }
            close_patch_outputs({{fc, cells_name}, {fe, entries_name}});
            std::cerr << "  " << label << ": " << n << " cells, " << corr.size() << " corrections" << std::endl;
        };

        // An admin / POI / place index the diff sent a cell index delta for
        // (every current build) is rebuilt and corrected in one streaming
        // pass over the old files, holding one cell at a time. Returns false
        // when there is no delta and the materialised rebuild + corrections
        // has to run instead.
        auto stream_rebuild = [&](const char* label, PatchFileId entries_fid, const std::string& prefix,
                                  const std::vector<uint64_t>& added, const std::vector<uint64_t>& removed,
                                  auto remap) -> bool {
            auto dit = cell_index_deltas.find((uint32_t)entries_fid);
            if (dit == cell_index_deltas.end()) return false;
            SequentialFileReader old_c(cur_dir + "/" + prefix + "_cells.bin");
            SequentialFileReader old_e(cur_dir + "/" + prefix + "_entries.bin");
            FILE* fc = open_out(prefix + "_cells.bin");
            FILE* fe = open_out(prefix + "_entries.bin");
            size_t n_out = 0;
            stream_cell_index(old_c, old_e, added, removed, remap,
                              dit->second.data(), dit->second.size(),
                              [&](const char* p, size_t len) { fwrite(p, 1, len, fc); n_out += len; },
                              [&](const char* p, size_t len) { fwrite(p, 1, len, fe); });
            close_patch_outputs({{fc, prefix + "_cells.bin"}, {fe, prefix + "_entries.bin"}});
            log_rewinds(old_c);
            log_rewinds(old_e);
            std::cerr << "  " << label << ": " << n_out / 12 << " cells, cell index delta" << std::endl;
            return true;
        };

        // Admin: small, use existing rebuild + corrections
        {
            // The admin leg of the parent-id remap, as the diff used it.
            auto admin_remap = [&](uint32_t id) { return poi_remap_lookup(poi_admin_remap, id); };
            if (!stream_rebuild("Admin", PatchFileId::ADMIN_ENTRIES, "admin", admin_added, admin_removed, admin_remap)) {
                std::unordered_map<uint32_t,uint32_t> ad_rm(poi_admin_remap.begin(), poi_admin_remap.end());
                auto old_ac = read_file(cur_dir + "/admin_cells.bin");
                auto old_adme = read_file(cur_dir + "/admin_entries.bin");
                auto admin = rebuild_cells_from_remap(old_ac, old_adme, ad_rm, admin_added, admin_removed);
                write_corrected_cells("Admin", PatchFileId::ADMIN_ENTRIES, admin.cells_data,
                                      admin.entries_data, "admin_cells.bin", "admin_entries.bin");
            }
            log_phase("  Admin", t_entry);
        }

        // POI and place: ids move by the replay's record remap file, read in
        // place by the streaming pass (by id, so not as a sequential stream).
        auto rebuild_by_record_remap = [&](const char* label, PatchFileId records_fid, PatchFileId entries_fid,
                                           const std::string& prefix, const std::vector<uint64_t>& added,
                                           const std::vector<uint64_t>& removed) {
            const RecordRemap& rm = remap_of(records_fid);
            auto record_remap = [&rm](uint32_t id) { uint32_t v = rm[id]; return v == 0xFFFFFFFFu ? id : v; };
            if (!stream_rebuild(label, entries_fid, prefix, added, removed, record_remap)) {
                std::unordered_map<uint32_t,uint32_t> rm_map;
                for (uint32_t i = 0; i < rm.size(); i++)
                    if (rm[i] != 0xFFFFFFFFu) rm_map[i] = rm[i];
                auto old_c = read_file(cur_dir + "/" + prefix + "_cells.bin");
                auto old_e = read_file(cur_dir + "/" + prefix + "_entries.bin");
                if (!old_c.empty() || !added.empty() || !removed.empty() || !rm_map.empty()) {
                    auto rebuilt = rebuild_cells_from_remap(old_c, old_e, rm_map, added, removed);
                    write_corrected_cells(label, entries_fid, rebuilt.cells_data, rebuilt.entries_data,
                                          prefix + "_cells.bin", prefix + "_entries.bin");
                }
            }
            log_phase((std::string("  ") + label).c_str(), t_entry);
        };
        rebuild_by_record_remap("POI", PatchFileId::POI_RECORDS, PatchFileId::POI_ENTRIES, "poi", poi_added, poi_removed);
        rebuild_by_record_remap("Place", PatchFileId::PLACE_NODES, PatchFileId::PLACE_ENTRIES, "place", place_added, place_removed);
    }

    unmap_file(patch_map);

    for (const auto& f : client_files) {
        if (gcs::store().is_encoded(f.name)) {
            auto it = gcs::store().got_out.find(f.name);
            if (it == gcs::store().got_out.end()) throw std::runtime_error("Patch did not produce " + f.name);
            if (it->second != f.size)
                throw std::runtime_error("Output size mismatch: " + f.name + " is " + std::to_string(it->second) +
                                         " bytes, the patch lists " + std::to_string(f.size));
            continue;
        }
        std::string p = out_dir + "/" + f.name;
        struct stat st;
        if (stat(p.c_str(), &st) != 0) {
            if (f.size == 0 && write_file(p, nullptr, 0)) continue;
            throw std::runtime_error("Patch did not produce " + f.name);
        }
        if (static_cast<uint64_t>(st.st_size) != f.size)
            throw std::runtime_error("Output size mismatch: " + f.name + " is " + std::to_string(st.st_size) +
                                     " bytes, the patch lists " + std::to_string(f.size));
    }
    if (gcs::store().active) {
        gcs::store().save_manifest();
        for (const auto& r : gcs::store().report) std::cerr << "  " << r << std::endl;
    }
    log_phase("Total", t_start);
    std::cerr << "Patch applied. Output in " << out_dir << std::endl;
    return 0;
}

// The one error edge: a throw anywhere in run() unwinds its scratch dir.
int main(int argc, char* argv[]) {
    try {
        return run(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        if (!gcs::store().sink_error.empty()) std::cerr << "  encoder: " << gcs::store().sink_error << std::endl;
        return 1;
    }
}
