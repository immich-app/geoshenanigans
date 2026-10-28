// Maps argv[1] MiB of anonymous memory and exits: the program the heap shim's
// test of mappings runs (tests/test_patch_lowmem.cpp), built as the tools are.
#if !defined(_FILE_OFFSET_BITS) || _FILE_OFFSET_BITS != 64
#error "heap-peak-probe builds as the tools do, with _FILE_OFFSET_BITS=64"
#endif
#include <stdlib.h>
#include <sys/mman.h>

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    const size_t bytes = (size_t)strtoul(argv[1], NULL, 10) << 20;
    void* p = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (p == MAP_FAILED) return 1;
    return munmap(p, bytes) == 0 ? 0 : 1;
}
