"""evict.py <dir>...: drop the page cache of every file under the given directories (following the
set's symlinks unless --no-links) with POSIX_FADV_DONTNEED, so a memory-limited run reads them itself instead of
finding them cached (and charged) elsewhere. Prints resident MiB before and after (mincore)."""
import ctypes
import mmap
import os
import sys

libc = ctypes.CDLL("libc.so.6", use_errno=True)


def resident(path):
    size = os.path.getsize(path)
    if size == 0:
        return 0
    with open(path, "rb") as f:
        m = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_COPY)
        pages = (size + 4095) // 4096
        vec = (ctypes.c_ubyte * pages)()
        anchor = ctypes.c_char.from_buffer(m)
        libc.mincore(ctypes.c_void_p(ctypes.addressof(anchor)), ctypes.c_size_t(size), vec)
        del anchor
        m.close()
    return sum(v & 1 for v in bytes(vec)) * 4096


no_links = "--no-links" in sys.argv
files = set()
for d in [a for a in sys.argv[1:] if a != "--no-links"]:
    for root, _, names in os.walk(d, followlinks=True):
        for n in names:
            path = os.path.join(root, n)
            if no_links and os.path.islink(path):
                continue
            files.add(os.path.realpath(path))
before = after = 0
for p in sorted(files):
    try:
        before += resident(p)
        fd = os.open(p, os.O_RDONLY)
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        os.close(fd)
        after += resident(p)
    except (OSError, ValueError):
        pass
print(f"evict: {len(files)} files, resident {before / 2**20:.1f} -> {after / 2**20:.1f} MiB")
