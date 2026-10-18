"""Cost of making vertex files self-contained: the per-polygon vertex counts the vertex
decoder otherwise reads from the decoded records file, as one extra zstd -18 stream.

count_cost.py <flat dir> <tmpdir>
"""
import os, subprocess, sys
import numpy as np

d, tmp = sys.argv[1:3]
os.makedirs(tmp, exist_ok=True)
SIDE = {"addr_vertices.bin": ("addr_points.bin", 28, 24), "poi_vertices.bin": ("poi_records.bin", 36, 12),
        "admin_vertices.bin": ("admin_polygons.bin", 24, 4), "postal_vertices.bin": ("postal_polygons.bin", 24, 4)}
tot = 0
for v, (rec, stride, off) in SIDE.items():
    p = os.path.join(d, rec)
    if not os.path.exists(p):
        continue
    a = np.memmap(p, np.uint32, mode="r").reshape(-1, stride // 4)[:, off // 4]
    c = np.ascontiguousarray(a[a > 0])
    best = None
    for name, arr in [("u32_bss", c.view(np.uint8).reshape(-1, 4).T.copy()), ("u32", c),
                      ("u16_bss", c.astype(np.uint16).view(np.uint8).reshape(-1, 2).T.copy() if c.max() < 65536 else None)]:
        if arr is None:
            continue
        f = os.path.join(tmp, f"{v}.{name}")
        arr.tofile(f)
        subprocess.run(["/ssd/shrink/zstd157/bin/zstd", "-18", "-T1", "-q", "-f", f, "-o", f + ".zst"], check=True)
        z = os.path.getsize(f + ".zst")
        os.remove(f); os.remove(f + ".zst")
        print(f"{v}: {len(c):,} polygons, {name}: {z / 1048576:.2f} MiB")
        best = z if best is None else min(best, z)
    tot += best
print(f"self-contained count streams, best each: {tot / 1048576:.2f} MiB")
