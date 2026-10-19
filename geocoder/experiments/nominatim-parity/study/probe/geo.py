"""parity copy of verify4 helpers: read builder admin polygons (admin_polygons.bin + admin_vertices.bin + osm_ids
sidecar) and even-odd point-in-polygon in numpy. Independent of the enclaves2 probe scripts; the
layout follows geocoder/server/src/lib.rs (AdminPolygon, decode_polygon_verts)."""
import numpy as np

PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("holes", "<u2")])
NO = 0xFFFFFFFF
SCALE = {0: 1e-5, 1: 1e-4, 2: 1e-6, 3: 1e-7}


class Admin:
    def __init__(self, d, sidecar=None):
        self.p = np.fromfile(f"{d}/admin_polygons.bin", PDT)
        self.v = np.memmap(f"{d}/admin_vertices.bin", np.uint8, "r")
        raw = np.fromfile(sidecar or f"{d}/admin_polygons.osm_ids", np.uint8)
        slots = raw[12:12 + 12 * len(self.p)].reshape(-1, 12)
        self.type = slots[:, 0].copy()
        self.stable = slots[:, 4:12].copy().view("<u8").ravel()
        live = (self.p["name"] != NO) & (self.p["cnt"] > 0) & (self.type == 3)
        self.by_rel = {}
        for i in np.flatnonzero(live):
            self.by_rel.setdefault(int(self.stable[i] >> 16), []).append(int(i))

    def verts(self, i):
        rec = self.p[i]
        off, n = int(rec["off"]), int(rec["cnt"])
        enc = int(self.v[off])
        blat = float(np.frombuffer(self.v[off + 2:off + 6].tobytes(), "<f4")[0])
        blng = float(np.frombuffer(self.v[off + 6:off + 10].tobytes(), "<f4")[0])
        body = self.v[off + 10:]
        if enc == 4:
            a = np.frombuffer(body[:8 * n].tobytes(), "<f4").reshape(-1, 2).astype(np.float64)
            return a[:, 0], a[:, 1]
        if enc == 3:
            a = np.frombuffer(body[:8 * n].tobytes(), "<u4").reshape(-1, 2).astype(np.float64)
        else:
            a = np.frombuffer(body[:4 * n].tobytes(), "<u2").reshape(-1, 2).astype(np.float64)
        s = SCALE[enc]
        # the server rounds to f32 after the f64 sum; keep that
        lat = (blat + a[:, 0] * s).astype(np.float32).astype(np.float64)
        lng = (blng + a[:, 1] * s).astype(np.float32).astype(np.float64)
        return lat, lng


def pip(lat, lng, plat, plng):
    """Even-odd crossing test of points (plat, plng arrays) against one vertex loop (lat, lng),
    closing edge included. Returns a bool array."""
    inside = np.zeros(len(plat), bool)
    if len(lat) < 3 or len(plat) == 0:
        return inside
    x1, y1 = lng, lat
    x2, y2 = np.roll(lng, -1), np.roll(lat, -1)
    for k in range(0, len(lat), 4096):
        a1, b1, a2, b2 = x1[k:k + 4096, None], y1[k:k + 4096, None], x2[k:k + 4096, None], y2[k:k + 4096, None]
        cond = (b1 > plat[None, :]) != (b2 > plat[None, :])
        with np.errstate(divide="ignore", invalid="ignore"):
            xint = a1 + (plat[None, :] - b1) * (a2 - a1) / (b2 - b1)
        inside ^= (np.count_nonzero(cond & (plng[None, :] < xint), axis=0) % 2).astype(bool)
    return inside
