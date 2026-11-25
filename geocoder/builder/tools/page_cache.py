#!/usr/bin/env python3
"""Usage: page_cache.py FILE...

Prints "<name>:<bytes>" for each FILE that exists, comma-separated on one line (nothing when none does): how many of
its bytes are in the page cache, from mincore over a mapping of it, which reads none of it. The job sampler
(job_sampler.sh) logs it for the PBFs pbf-apply reads and writes. A file this process neither owns nor could open for
writing prints "<name>:?": Linux (5.0 on) shows every page of its mapping as cached.
"""
import ctypes
import mmap
import os
import sys

PAGE = mmap.PAGESIZE
# Bytes mapped at a time: a planet PBF's vector is 24 M entries, this keeps it to 256 K.
CHUNK = 1 << 30
# A vector entry's bit 0: whether its page is cached (the others are reserved).
RESIDENT = bytes(b & 1 for b in range(256))

libc = ctypes.CDLL(None, use_errno=True)
libc.mmap.restype = ctypes.c_void_p
libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long]
libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
MAP_FAILED = ctypes.c_void_p(-1).value


def cached(path):
    """The bytes of `path` in the page cache, or None for a file mincore shows all of as cached."""
    with open(path, "rb") as f:
        st = os.fstat(f.fileno())
        if st.st_uid != os.geteuid() and not os.access(path, os.W_OK):
            return None
        size = st.st_size
        pages, last = 0, 0
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
            resident = bytes(vec).translate(RESIDENT)
            pages += resident.count(1)
            last = resident[-1]
    total = pages * PAGE
    if last and size % PAGE:
        # A cached last page holds the file's bytes up to its end alone.
        total -= PAGE - size % PAGE
    return total


def main():
    out = []
    for path in sys.argv[1:]:
        # A file that is not there, or went (a rename over it), is left out.
        try:
            n = cached(path)
            out.append(f"{os.path.basename(path)}:{'?' if n is None else n}")
        except FileNotFoundError:
            pass
    if out:
        print(",".join(out))


if __name__ == "__main__":
    main()
