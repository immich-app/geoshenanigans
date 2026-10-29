// Maps, remaps and unmaps anonymous memory as its arguments say, then exits:
// the program the heap shim's tests of mappings run
// (tests/test_patch_lowmem.cpp), built as the tools are. Each step is an
// operation and its numbers: AT and TO are addresses that many MiB into free
// address space the probe found, with an optional +BYTES (0+1 is one byte
// in); MIB and NEW are lengths in MiB.
//   map AT MIB          an anonymous mapping of the range, which is free
//   share AT MIB        the same, shared (MAP_SHARED)
//   over AT MIB         an anonymous MAP_FIXED mapping over the range
//   unmap AT MIB        munmap() of the range
//   resize AT MIB NEW   mremap() of the range to NEW MiB in place
//   move AT MIB TO NEW  mremap() of the range to NEW MiB at TO, over whatever
//                       is mapped there (MREMAP_FIXED)
//   copy AT MIB TO      mremap() of the range to TO that leaves it mapped at
//                       AT as well (MREMAP_DONTUNMAP)
//   clone AT TO NEW     mremap() of 0 bytes at AT in a shared mapping, which
//                       maps NEW MiB of its pages from AT a second time at TO
//   shatter AT PIECES   an anonymous mapping of 2 PIECES pages at AT, cut
//                       into PIECES by munmap() of every second page
// Each succeeds, unless its name ends in -refused (unmap-refused,
// resize-refused): then it fails.
// Exit status 0 when every step did as it says, 1 when one didn't, 2 when the
// arguments aren't steps.
#if !defined(_FILE_OFFSET_BITS) || _FILE_OFFSET_BITS != 64
#error "heap-peak-probe builds as the tools do, with _FILE_OFFSET_BITS=64"
#endif
#define _GNU_SOURCE
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

// The address space the steps lie in: room for shatter's 2 x 4097 pages of
// 64 KiB.
#define SPACE ((size_t)1 << 30)

static char* base;

enum { MAP, SHARE, OVER, UNMAP, RESIZE, MOVE, COPY, CLONE, SHATTER, OPS };

static const struct {
    const char* name;
    int numbers;
} ops[OPS] = {
    [MAP] = {"map", 2}, [SHARE] = {"share", 2}, [OVER] = {"over", 2}, [UNMAP] = {"unmap", 2}, [RESIZE] = {"resize", 3},
    [MOVE] = {"move", 4}, [COPY] = {"copy", 3}, [CLONE] = {"clone", 3}, [SHATTER] = {"shatter", 2},
};

// A step's number: n, and the bytes after a +.
struct number {
    size_t n, bytes;
};

static int parse(const char* s, struct number* out) {
    char* end;
    if (*s < '0' || *s > '9') return 0;
    out->n = strtoul(s, &end, 10);
    out->bytes = 0;
    if (*end == '+') {
        s = end + 1;
        if (*s < '0' || *s > '9') return 0;
        out->bytes = strtoul(s, &end, 10);
    }
    return !*end && out->n <= (size_t)1 << 20 && out->bytes < (size_t)1 << 20;
}

static size_t mib(struct number v) { return (v.n << 20) + v.bytes; }

static int inside(size_t at, size_t len) { return at <= SPACE && len <= SPACE - at; }

// 1 when the step's system calls succeed, 0 when one fails, -1 when the step
// reaches past the space.
static int run(int op, const struct number* v) {
    const int prot = PROT_READ | PROT_WRITE, anonymous = MAP_PRIVATE | MAP_ANONYMOUS;
    const size_t page = (size_t)getpagesize(), at = mib(v[0]);
    // The step's old range [at, at + len), and its new one [to, to + new_len).
    size_t len = mib(v[1]), to = at, new_len = len;
    if (op == SHATTER) len = new_len = 2 * v[1].n * page;
    if (op == RESIZE) new_len = mib(v[2]);
    if (op == MOVE || op == COPY) to = mib(v[2]);
    if (op == MOVE) new_len = mib(v[3]);
    if (op == CLONE) {
        len = 0;
        to = mib(v[1]);
        new_len = mib(v[2]);
    }
    if (!inside(at, len) || !inside(to, new_len)) return -1;

    char* const p = base + at;
    switch (op) {
        case MAP: return mmap(p, len, prot, anonymous | MAP_FIXED_NOREPLACE, -1, 0) == p;
        case SHARE: return mmap(p, len, prot, MAP_SHARED | MAP_ANONYMOUS | MAP_FIXED_NOREPLACE, -1, 0) == p;
        case OVER: return mmap(p, len, prot, anonymous | MAP_FIXED, -1, 0) == p;
        case UNMAP: return munmap(p, len) == 0;
        case RESIZE: return mremap(p, len, new_len, 0) == p;
        case MOVE: return mremap(p, len, new_len, MREMAP_MAYMOVE | MREMAP_FIXED, base + to) == base + to;
        case COPY: return mremap(p, len, len, MREMAP_MAYMOVE | MREMAP_FIXED | MREMAP_DONTUNMAP, base + to) == base + to;
        case CLONE: return mremap(p, 0, new_len, MREMAP_MAYMOVE | MREMAP_FIXED, base + to) == base + to;
    }
    if (mmap(p, len, prot, anonymous | MAP_FIXED_NOREPLACE, -1, 0) != p) return 0;
    for (size_t off = page; off < len; off += 2 * page)
        if (munmap(p + off, page) != 0) return 0;
    return 1;
}

int main(int argc, char** argv) {
    // Found with system calls the heap shim doesn't see, so it counts only
    // the steps.
    base = (char*)syscall(SYS_mmap, NULL, SPACE, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (base == MAP_FAILED || syscall(SYS_munmap, base, SPACE) != 0) return 1;

    for (int i = 1; i < argc;) {
        const char* name = argv[i++];
        size_t length = strlen(name);
        const int refused = length > 8 && !strcmp(name + length - 8, "-refused");
        if (refused) length -= 8;
        int op = 0;
        while (op < OPS && (strlen(ops[op].name) != length || strncmp(name, ops[op].name, length))) op++;
        if (op == OPS || argc - i < ops[op].numbers) return 2;

        struct number v[4] = {{0, 0}};
        for (int k = 0; k < ops[op].numbers; k++)
            if (!parse(argv[i++], &v[k])) return 2;
        const int done = run(op, v);
        if (done < 0) return 2;
        if (done == refused) return 1;
    }
    return 0;
}
