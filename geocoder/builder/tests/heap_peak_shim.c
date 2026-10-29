// LD_PRELOAD heap peak of one process, for the patcher's memory tests
// (tests/tool_runs.h run_with_heap_peak): the live bytes of every block the
// malloc family hands out (malloc_usable_size, so glibc's own mmapped blocks
// count here) plus the pages still mapped of the anonymous mappings made
// through mmap(), mmap64() or mremap(), whose peak exit() writes to
// $GEOCODER_HEAP_PEAK_OUT as one line of decimal bytes.
// The process's children run unmeasured: the zstd CLI the patcher spawns
// sizes its window by the patch.
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <malloc.h>
#include <pthread.h>
#include <sched.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

extern void* __libc_malloc(size_t);
extern void* __libc_calloc(size_t, size_t);
extern void* __libc_realloc(void*, size_t);
extern void* __libc_memalign(size_t, size_t);
extern void __libc_free(void*);

static long live, peak;

static void add(long bytes) {
    const long now = __atomic_add_fetch(&live, bytes, __ATOMIC_RELAXED);
    long was = __atomic_load_n(&peak, __ATOMIC_RELAXED);
    while (now > was && !__atomic_compare_exchange_n(&peak, &was, now, 1, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {}
}

static long usable(void* p) { return p ? (long)malloc_usable_size(p) : 0; }

void* malloc(size_t n) {
    void* p = __libc_malloc(n);
    add(usable(p));
    return p;
}

void* calloc(size_t count, size_t n) {
    void* p = __libc_calloc(count, n);
    add(usable(p));
    return p;
}

void free(void* p) {
    add(-usable(p));
    __libc_free(p);
}

void* realloc(void* p, size_t n) {
    const long was = usable(p);
    void* q = __libc_realloc(p, n);
    if (q) add(usable(q) - was);
    else if (n == 0) add(-was);  // glibc's realloc(p, 0) frees p
    return q;
}

void* memalign(size_t alignment, size_t n) {
    void* p = __libc_memalign(alignment, n);
    add(usable(p));
    return p;
}

void* aligned_alloc(size_t alignment, size_t n) { return memalign(alignment, n); }

void* valloc(size_t n) { return memalign((size_t)getpagesize(), n); }

void* pvalloc(size_t n) {
    const size_t page = (size_t)getpagesize();
    return memalign(page, (n + page - 1) / page * page);
}

int posix_memalign(void** out, size_t alignment, size_t n) {
    if (alignment == 0 || alignment % sizeof(void*) != 0 || (alignment & (alignment - 1)) != 0) return EINVAL;
    void* p = memalign(alignment, n);
    if (!p) return ENOMEM;
    *out = p;
    return 0;
}

// The anonymous mappings still mapped (SequentialFileReader's read windows),
// each the addresses [from, to) of its whole pages. The pages a munmap()
// unmaps leave every mapping they overlap, cutting it in two when they lie
// inside it; so do those an mmap() with MAP_FIXED maps over and those an
// mremap() moves away from or onto. Each call holds the lock across its
// system call, so the table changes in the order the address space does (no
// thread maps the addresses a munmap() frees before the table lets go of
// them), and only when the call succeeds. No hook allocates.
#define MAPPINGS 4096
static struct {
    uintptr_t from, to;
} mappings[MAPPINGS];
static int mapped;
static char mappings_locked;

// More mappings than the table holds end the process, with this status and a
// line on stderr: one left out of the table would count wrong from then on.
#define TABLE_FULL_STATUS 70

static void lock_mappings(void) {
    while (__atomic_test_and_set(&mappings_locked, __ATOMIC_ACQUIRE)) sched_yield();
}

static void unlock_mappings(void) { __atomic_clear(&mappings_locked, __ATOMIC_RELEASE); }

// A fork()ed child starts with the table whole and the lock free, whatever
// the parent's other threads were doing.
__attribute__((constructor)) static void lock_across_fork(void) { pthread_atfork(lock_mappings, unlock_mappings, unlock_mappings); }

static void remember(uintptr_t from, uintptr_t to) {
    if (mapped == MAPPINGS) {
        static const char why[] = "heap_peak_shim.c: more anonymous mappings than its table holds\n";
        if (write(STDERR_FILENO, why, sizeof why - 1) < 0) {}
        _exit(TABLE_FULL_STATUS);
    }
    mappings[mapped].from = from;
    mappings[mapped].to = to;
    mapped++;
}

// Takes the pages [from, to) out of the mappings, and their bytes out of the
// live count.
static void forget(uintptr_t from, uintptr_t to) {
    for (int i = 0; i < mapped; i++) {
        const uintptr_t start = mappings[i].from, end = mappings[i].to;
        if (end <= from || to <= start) continue;

        add(-(long)((end < to ? end : to) - (start > from ? start : from)));
        if (start < from) {
            mappings[i].to = from;
            if (to < end) remember(to, end);  // the piece after the hole, which [from, to) misses
        } else if (to < end) {
            mappings[i].from = to;
        } else {
            mappings[i] = mappings[--mapped];  // the last mapping moves into i, yet to be looked at
            i--;
        }
    }
}

// Whether any page of [from, to) is counted.
static int counted(uintptr_t from, uintptr_t to) {
    for (int i = 0; i < mapped; i++)
        if (mappings[i].from < to && from < mappings[i].to) return 1;
    return 0;
}

// The pages [from, to) a call mapped, over whatever was mapped there: counted
// when anonymous.
static void map_over(uintptr_t from, uintptr_t to, int anonymous) {
    forget(from, to);
    if (!anonymous) return;
    remember(from, to);
    add((long)(to - from));
}

static uintptr_t page_rounded(size_t n) {
    const size_t page = (size_t)getpagesize();
    return (n + page - 1) / page * page;
}

static void* counted_mmap(void* addr, size_t len, int prot, int flags, int fd, long off) {
    lock_mappings();
    void* p = (void*)syscall(SYS_mmap, addr, len, prot, flags, fd, off);
    if (p != MAP_FAILED) map_over((uintptr_t)p, (uintptr_t)p + page_rounded(len), flags & MAP_ANONYMOUS);
    unlock_mappings();
    return p;
}

void* mmap(void* addr, size_t len, int prot, int flags, int fd, off_t off) { return counted_mmap(addr, len, prot, flags, fd, off); }

// What mmap() links to in a _FILE_OFFSET_BITS=64 build, such as the tools'.
void* mmap64(void* addr, size_t len, int prot, int flags, int fd, off64_t off) { return counted_mmap(addr, len, prot, flags, fd, off); }

int munmap(void* addr, size_t len) {
    lock_mappings();
    const int r = (int)syscall(SYS_munmap, addr, len);
    if (r == 0) forget((uintptr_t)addr, (uintptr_t)addr + page_rounded(len));
    unlock_mappings();
    return r;
}

// Reads the new address only with MREMAP_FIXED, as glibc's mremap() does. The
// old pages stay mapped with MREMAP_DONTUNMAP, and when an old length of 0
// maps shared pages a second time; the new mapping is anonymous when the old
// one is.
void* mremap(void* old, size_t old_len, size_t len, int flags, ...) {
    void* want = NULL;
    if (flags & MREMAP_FIXED) {
        va_list rest;
        va_start(rest, flags);
        want = va_arg(rest, void*);
        va_end(rest);
    }
    lock_mappings();
    const uintptr_t from = (uintptr_t)old, to = from + (old_len ? page_rounded(old_len) : 1);
    const int anonymous = counted(from, to);
    void* p = (void*)syscall(SYS_mremap, old, old_len, len, flags, want);
    if (p != MAP_FAILED) {
        if (old_len && !(flags & MREMAP_DONTUNMAP)) forget(from, to);
        map_over((uintptr_t)p, (uintptr_t)p + page_rounded(len), anonymous);
    }
    unlock_mappings();
    return p;
}

static char peak_out[PATH_MAX];

__attribute__((constructor)) static void take_environment(void) {
    const char* out = getenv("GEOCODER_HEAP_PEAK_OUT");
    if (out && strlen(out) < sizeof peak_out) strcpy(peak_out, out);
    unsetenv("GEOCODER_HEAP_PEAK_OUT");
    unsetenv("LD_PRELOAD");
}

__attribute__((destructor)) static void write_peak(void) {
    if (!peak_out[0]) return;
    char line[32];
    const int n = snprintf(line, sizeof line, "%ld\n", __atomic_load_n(&peak, __ATOMIC_RELAXED));
    const int fd = open(peak_out, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0) return;
    if (write(fd, line, (size_t)n) != n) {}
    close(fd);
}
