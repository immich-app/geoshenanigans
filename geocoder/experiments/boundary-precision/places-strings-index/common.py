import numpy as np, os, subprocess

SM = "/ssd/shrink/idx/smallest"
REC = "/ssd/shrink/idx/recommended"
WORK = "/ssd/shrink/places-strings-index/work"
NO_DATA = 0xFFFFFFFF
MiB = 1024 * 1024

PLACE_DT = np.dtype([('lat', '<f4'), ('lng', '<f4'), ('name_id', '<u4'), ('place_type', 'u1'),
                     ('pad1', 'u1'), ('pad2', '<u2'), ('parent_poly_id', '<u4')])
ADMIN_DT = np.dtype([('vertex_offset', '<u4'), ('vertex_count', '<u4'), ('name_id', '<u4'),
                     ('admin_level', 'u1'), ('pto', 'u1'), ('pad2', 'u1'), ('pad3', 'u1'),
                     ('area', '<f4'), ('cc', '<u2'), ('pad4', '<u2')])
CELL_DT = np.dtype([('cell', '<u8'), ('off', '<u4')])

ZLEVEL = int(os.environ.get("ZLEVEL", "18"))


def load(name, dt, d=SM):
    return np.fromfile(os.path.join(d, name), dtype=dt)


def zst(data, level=None, extra=()):
    """zstd-compress bytes via the CLI, return compressed size in bytes."""
    level = level or ZLEVEL
    if isinstance(data, np.ndarray):
        data = data.tobytes()
    args = ["/ssd/shrink/places-strings-index/zstd157/bin/zstd", "-T1", "-c", "-q"]
    args += ["--ultra", f"-{level}"] if level > 19 else [f"-{level}"]
    p = subprocess.run(args + list(extra), input=data, stdout=subprocess.PIPE, check=True)
    return len(p.stdout)


def zst_file(path, level=None, extra=()):
    with open(path, 'rb') as f:
        return zst(f.read(), level, extra)


def mib(n):
    return f"{n / MiB:.2f}"


def decode_entries(entries_bytes, offsets):
    """Decode u16 count + count*u32 lists at the given byte offsets."""
    out = []
    for o in offsets:
        o = int(o)
        c = int.from_bytes(entries_bytes[o:o + 2], 'little')
        out.append(np.frombuffer(entries_bytes, dtype='<u4', count=c, offset=o + 2))
    return out
