"""Storage grid (vertex encoding) histogram per configuration: rings and share of vertices per enc
(0 = 1e-5 deg u16, 1 = 1e-4 u16, 2 = 1e-6 u16, 3 = 1e-7 u32), full levels and L2-8."""
import sys, numpy as np
PDT = np.dtype([("off","<u4"),("cnt","<u4"),("name","<u4"),("lvl","u1"),("pto","u1"),("p2","u1"),("p3","u1"),("area","<f4"),("cc","<u2"),("p4","<u2")])
for a in sys.argv[1:]:
    n, d = a.split("=", 1)
    p = np.fromfile(f"{d}/admin_polygons.bin", PDT)
    vb = np.memmap(f"{d}/admin_vertices.bin", np.uint8, "r")
    live = (p["cnt"] > 0) & (p["off"] != 0xFFFFFFFF)
    enc = np.full(len(p), 255, np.int64); enc[live] = vb[p["off"][live].astype(np.int64)]
    cnt = p["cnt"].astype(np.int64)
    for scope, m in (("full", live), ("L2-8", live & (p["lvl"] >= 2) & (p["lvl"] <= 8))):
        tot = cnt[m].sum()
        s = " | ".join(f"{g}: rings {int((m & (enc == e)).sum()):,} verts {100 * cnt[m & (enc == e)].sum() / tot:.1f}%"
                       for e, g in ((2, "1e-6"), (0, "1e-5"), (1, "1e-4"), (3, "1e-7/u32")))
        print(f"{n} {scope}: {s}")
