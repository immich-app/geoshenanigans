"""Which grids today's polygon outlines are stored on (VertexEncoding tag, byte 0 of each block).
enc 0 = 1e-5 deg (1.1 m), 1 = 1e-4 deg (11 m), 2 = 1e-6 deg (11 cm), 3 = 1e-7 deg (1.1 cm).
usage: grid_census.py  -> admin (max uncapped, withaddr q2.5), postal (max, q2.5), poi, addr outlines."""
import numpy as np
NO = 0xFFFFFFFF
NAMES = {0: "1.1 m (1e-5)", 1: "11 m (1e-4)", 2: "11 cm (1e-6)", 3: "1.1 cm (1e-7)"}


def report(label, vb_path, off, cnt, extra=None):
    vb = np.memmap(vb_path, np.uint8, mode="r")
    m = (cnt > 0) & (off != NO)
    off, cnt = off[m].astype(np.int64), cnt[m].astype(np.int64)
    enc = np.asarray(vb[off])
    print(f"== {label}: {len(off):,} outlines, {int(cnt.sum()):,} vertices")
    for e in range(4):
        s = enc == e
        if s.any():
            line = f"   enc {e} {NAMES[e]:>14}: outlines {int(s.sum()):>12,} ({100*s.mean():5.1f}%)  vertices {int(cnt[s].sum()):>13,} ({100*cnt[s].sum()/cnt.sum():5.1f}%)"
            if extra is not None:
                ex = extra[m]
                line += "  by level " + str({int(k): int(v) for k, v in zip(*np.unique(ex[s], return_counts=True))})
            print(line)


ADMIN = np.dtype([('vo', '<u4'), ('vc', '<u4'), ('name', '<u4'), ('lvl', 'u1'), ('pto', 'u1'), ('p2', 'u1'),
                  ('p3', 'u1'), ('area', '<f4'), ('cc', '<u2'), ('p4', '<u2')])
for preset in ("max", "withaddr"):
    d = f"/ssd/shrink/idx/{preset}"
    for kind in ("admin", "postal"):
        p = np.fromfile(f"{d}/{kind}_polygons.bin", ADMIN)
        report(f"{preset} {kind}", f"{d}/{kind}_vertices.bin", p['vo'], p['vc'], p['lvl'])
POI = np.dtype([('lat', '<f4'), ('lng', '<f4'), ('vo', '<u4'), ('vc', '<u4'), ('name', '<u4'), ('cat', 'u1'),
                ('tier', 'u1'), ('flags', 'u1'), ('imp', 'u1'), ('ps', '<u4'), ('pp', '<u4'), ('ppoly', '<u4')])
p = np.fromfile("/ssd/shrink/idx/max/poi_records.bin", POI)
report("max poi", "/ssd/shrink/idx/max/poi_vertices.bin", p['vo'], p['vc'])
a = np.memmap("/ssd/shrink/idx/max/addr_points.bin", np.uint8, mode="r")
n = len(a) // 28
vo = np.ndarray((n,), '<u4', a, 20, (28,)).copy()
vc = np.ndarray((n,), '<u4', a, 24, (28,)).copy()
report("max addr (building outlines)", "/ssd/shrink/idx/max/addr_vertices.bin", vo, vc)
