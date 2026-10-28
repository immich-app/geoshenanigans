// LD_PRELOAD heap peak of one process, for the patcher's memory tests
// (tests/tool_runs.h run_with_heap_peak): the live bytes of every block the
// malloc family hands out (malloc_usable_size, so glibc's own mmapped blocks
// count here) plus the anonymous mappings made through mmap() or mmap64(),
// whose peak exit() writes to $GEOCODER_HEAP_PEAK_OUT as one line of decimal
// bytes.
// The process's children run unmeasured: the zstd CLI the patcher spawns
// sizes its window by the patch.
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <malloc.h>
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

// The anonymous mappings live (SequentialFileReader's read windows): where
// each starts, and its length in pages. One the table has no room for still
// counts, and stays counted: the peak can only read high.
#define MAPPINGS 4096
static struct {
    uintptr_t at;
    long bytes;
} mappings[MAPPINGS];

static long page_rounded(size_t n) {
    const size_t page = (size_t)getpagesize();
    return (long)((n + page - 1) / page * page);
}

static void* counted_mmap(void* addr, size_t len, int prot, int flags, int fd, long off) {
    void* p = (void*)syscall(SYS_mmap, addr, len, prot, flags, fd, off);
    if (p == MAP_FAILED || !(flags & MAP_ANONYMOUS)) return p;
    const long bytes = page_rounded(len);
    for (int i = 0; i < MAPPINGS; i++) {
        uintptr_t empty = 0;
        if (__atomic_compare_exchange_n(&mappings[i].at, &empty, (uintptr_t)p, 0, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) {
            mappings[i].bytes = bytes;
            break;
        }
    }
    add(bytes);
    return p;
}

void* mmap(void* addr, size_t len, int prot, int flags, int fd, off_t off) { return counted_mmap(addr, len, prot, flags, fd, off); }

// What mmap() links to in a _FILE_OFFSET_BITS=64 build, such as the tools'.
void* mmap64(void* addr, size_t len, int prot, int flags, int fd, off64_t off) { return counted_mmap(addr, len, prot, flags, fd, off); }

int munmap(void* addr, size_t len) {
    for (int i = 0; i < MAPPINGS; i++)
        if (__atomic_load_n(&mappings[i].at, __ATOMIC_ACQUIRE) == (uintptr_t)addr) {
            add(-mappings[i].bytes);
            __atomic_store_n(&mappings[i].at, 0, __ATOMIC_RELEASE);
            break;
        }
    return (int)syscall(SYS_munmap, addr, len);
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
