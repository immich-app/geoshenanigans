import numpy as np
R = 6371008.8 * np.pi / 180
for name in ("street_nodes", "interp_nodes", "addr_points"):
    e = np.memmap(f"/ssd/precision/build/dumps/{name}.e7", dtype="<i4", mode="r").reshape(-1, 2)[::16]
    e = np.asarray(e, dtype=np.int64)
    if name == "addr_points":
        k = np.fromfile("/ssd/precision/build/dumps/addr_points.kind", dtype="u1")[::16]
        e = e[k == 0]
    la, lo = e[:, 0] / 1e7, e[:, 1] / 1e7
    fla, flo = la.astype(np.float32).astype(np.float64), lo.astype(np.float32).astype(np.float64)
    d = R * np.hypot(fla - la, (flo - lo) * np.cos(np.radians(la)))
    print(name, len(d), "p50 %.3f p99 %.3f max %.3f m" % (np.percentile(d, 50), np.percentile(d, 99), d.max()))
