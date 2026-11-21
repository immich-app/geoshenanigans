#!/usr/bin/env python3
"""Usage: page_cache.py FILE...

Prints "<name>:<bytes>" for each FILE that exists, comma-separated on one line (nothing when none does): how many of
its bytes are in the page cache, from mincore over a mapping of it, which reads none of it. The job sampler
(job_sampler.sh) logs it for the PBFs pbf-apply reads and writes.
"""
import ctypes
import mmap
import os
import sys

PAGE = mmap.PAGESIZE
# Bytes mapped at a time: a planet PBF's vector is 24 M entries, this keeps it to 256 K.
CHUNK = 1 << 30

libc = ctypes.CDLL(None, use_errno=True)
libc.mmap.restype = ctypes.c_void_p
libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long]
libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
MAP_FAILED = ctypes.c_void_p(-1).value


def cached(path):
    """The bytes of `path` in the page cache."""
    with open(path, "rb") as f:
        size = os.fstat(f.fileno()).st_size
        pages = 0
        for start in range(0, size, CHUNK):
            n = min(CHUNK, size - start)
            addr = libc.mmap(None, n, mmap.PROT_READ, mmap.MAP_SHARED, f.fileno(), start)
            if addr in (None, MAP_FAILED):
                raise OSError(ctypes.get_errno(), f"mmap {path}")
            try:
                vec = (ctypes.c_ubyte * ((n + PAGE - 1) // PAGE))()
                if libc.mincore(addr, n, vec) != 0:
                    raise OSError(ctypes.get_errno(), f"mincore {path}")
            finally:
                libc.munmap(addr, n)
            # An entry is 1 for a cached page, 0 for another (its other bits are reserved, and clear).
            raw = bytes(vec)
            pages += len(raw) - raw.count(0)
    return min(size, pages * PAGE)


def main():
    out = []
    for path in sys.argv[1:]:
        # A file that is not there, or went (a rename over it), is left out.
        try:
            out.append(f"{os.path.basename(path)}:{cached(path)}")
        except FileNotFoundError:
            pass
    if out:
        print(",".join(out))


if __name__ == "__main__":
    main()
