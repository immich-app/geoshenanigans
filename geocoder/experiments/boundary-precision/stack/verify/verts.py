"""Independent layout-B decoder + geometry checks.
1. decode final/admin_vertices.bin (layout B, per-polygon byte planes of wrapping zigzag deltas)
   back to today's format with a per-polygon loop written from the format description only, and
   byte-compare with todays-format/admin_vertices.bin (writes verify/restored/admin_vertices.bin)
2. confirm final/ B file is not today's format (headers equal, bodies differ)
3. decode today's-format coordinates exactly as the reader does (f32(bbox_min_f32 + q*scale))
   for baseline and snapped files; report on-grid fraction and displacement vs baseline."""
import os, sys, time, hashlib, numpy as np

B = "/ssd/shrink/idx/smallest"
F = "/ssd/shrink/stack/final"
T = "/ssd/shrink/stack/todays-format"
OUT = "/ssd/shrink/stack/verify/restored"
NO = 0xFFFFFFFF
ADMIN_DT = np.dtype([('vertex_offset', '<u4'), ('vertex_count', '<u4'), ('name_id', '<u4'),
                     ('admin_level', 'u1'), ('pto', 'u1'), ('pad2', 'u1'), ('pad3', 'u1'),
                     ('area', '<f4'), ('cc', '<u2'), ('pad4', '<u2')])
H = 10
SCALE = {0: 1e-5, 1: 1e-4, 2: 1e-6, 3: 1e-7}

ap = np.fromfile(f"{F}/admin_polygons.bin", ADMIN_DT)
liv = (ap["vertex_count"] > 0) & (ap["vertex_offset"] != NO)
order = np.argsort(ap["vertex_offset"][liv], kind="stable")
offs = ap["vertex_offset"][liv][order].astype(np.int64)
cnts = ap["vertex_count"][liv][order].astype(np.int64)
print(f"live polygons {len(offs):,}, vertices {int(cnts.sum()):,}")

bb = np.fromfile(f"{F}/admin_vertices.bin", np.uint8)
tf = np.fromfile(f"{T}/admin_vertices.bin", np.uint8)
out = np.zeros_like(bb)
t = time.time()
covered = 0
for off, cnt in zip(offs.tolist(), cnts.tolist()):
    enc = int(bb[off])
    w = 4 if enc == 3 else 2
    out[off:off + H] = bb[off:off + H]
    body = bb[off + H: off + H + 2 * w * cnt].reshape(2 * w, cnt).astype(np.uint64)
    res = np.empty((cnt, 2 * w), np.uint8)
    mod = 1 << (8 * w)
    for comp in range(2):
        z = np.zeros(cnt, np.uint64)
        for b in range(w):
            z |= body[comp * w + b] << np.uint64(8 * b)
        z = z.astype(np.int64)
        d = np.where(z & 1, -((z + 1) >> 1), z >> 1)        # un-zigzag
        q = np.mod(np.cumsum(d), mod)                      # first value absolute, then deltas, wrapping
        for b in range(w):
            res[:, comp * w + b] = (q >> (8 * b)) & 0xFF
    out[off + H: off + H + 2 * w * cnt] = res.ravel()
    covered += H + 2 * w * cnt
print(f"decode B (independent, per-polygon numpy loop): {time.time() - t:.1f}s, blocks cover {covered:,} of {len(bb):,} B")
os.makedirs(OUT, exist_ok=True)
out.tofile(f"{OUT}/admin_vertices.bin")
same = np.array_equal(out, tf)
print(f"{'IDENTICAL' if same else 'DIFFERENT'} decoded vs todays-format/admin_vertices.bin; sha256 decoded "
      f"{hashlib.sha256(out.tobytes()).hexdigest()[:16]} todays {hashlib.sha256(tf.tobytes()).hexdigest()[:16]}")
if not same:
    print("  first differing byte", int(np.flatnonzero(out != tf)[0]))
hp = (offs[:, None] + np.arange(H)).ravel()
print(f"B file headers equal today's: {np.array_equal(bb[hp], tf[hp])}; body bytes differing from today's: "
      f"{int((bb != tf).sum()):,} of {len(bb):,}")


def coords(vb):
    enc = vb[offs].astype(np.int64)
    w = np.where(enc == 3, 4, 2)
    sc = np.array([SCALE[0], SCALE[1], SCALE[2], SCALE[3]])[enc]
    bml = vb[(offs[:, None] + np.arange(2, 6)).ravel()].view('<f4').astype(np.float64)
    bmg = vb[(offs[:, None] + np.arange(6, 10)).ravel()].view('<f4').astype(np.float64)
    vp = np.repeat(np.arange(len(offs)), cnts)
    k = np.arange(int(cnts.sum())) - np.repeat(np.concatenate([[0], np.cumsum(cnts)[:-1]]), cnts)
    wv = w[vp]
    pos = offs[vp] + H + k * 2 * wv
    q = []
    for comp in range(2):
        v = np.zeros(len(k), np.int64)
        for b in range(4):
            s = b < wv
            v[s] |= vb[pos[s] + comp * wv[s] + b].astype(np.int64) << (8 * b)
        q.append(v)
    lat = (bml[vp] + q[0] * sc[vp]).astype(np.float32)
    lng = (bmg[vp] + q[1] * sc[vp]).astype(np.float32)
    return enc, lat, lng, vp


enc_b, la_b, lo_b, vp = coords(np.fromfile(f"{B}/admin_vertices.bin", np.uint8))
enc_t, la_t, lo_t, _ = coords(tf)
print(f"encodings: baseline {np.bincount(enc_b, minlength=4).tolist()} snapped {np.bincount(enc_t, minlength=4).tolist()} "
      f"(polygons whose encoding changed: {int((enc_b != enc_t).sum())})")
for name, a in (("lat", la_t), ("lng", lo_t)):
    a64 = a.astype(np.float64)
    g = np.round(a64 / 1e-4)
    dev = np.abs(a64 - g * 1e-4)
    exact = np.float32(g * 1e-4) == a
    print(f"snapped {name}: max |x - nearest 1e-4 grid| = {dev.max():.3e} deg; == f32(grid) exactly: {exact.mean():.4%}; "
          f"dev > 1e-5: {int((dev > 1e-5).sum())}")
for name, a in (("lat", la_b), ("lng", lo_b)):
    a64 = a.astype(np.float64)
    dev = np.abs(a64 - np.round(a64 / 1e-4) * 1e-4)
    print(f"baseline {name} (control): dev > 1e-5 for {(dev > 1e-5).mean():.2%} of vertices")
dlat = np.abs(la_t.astype(np.float64) - la_b)
dlng = np.abs(lo_t.astype(np.float64) - lo_b)
print(f"displacement vs baseline: max |dlat| {dlat.max():.3e} deg, max |dlng| {dlng.max():.3e} deg; "
      f"vertices moved: {((dlat > 0) | (dlng > 0)).mean():.2%}; > 0.6e-4 deg: {int(((dlat > 0.6e-4) | (dlng > 0.6e-4)).sum())}")
first = np.concatenate([[0], np.cumsum(cnts)[:-1]])
last = first + cnts - 1
print(f"rings closed: baseline {np.mean((la_b[first] == la_b[last]) & (lo_b[first] == lo_b[last])):.4%}, "
      f"snapped {np.mean((la_t[first] == la_t[last]) & (lo_t[first] == lo_t[last])):.4%}")
# consecutive duplicate vertices produced by the snap (kept, not removed)
dup_t = (la_t[1:] == la_t[:-1]) & (lo_t[1:] == lo_t[:-1]) & (vp[1:] == vp[:-1])
dup_b = (la_b[1:] == la_b[:-1]) & (lo_b[1:] == lo_b[:-1]) & (vp[1:] == vp[:-1])
print(f"consecutive duplicate vertices: baseline {int(dup_b.sum()):,}, snapped {int(dup_t.sum()):,}")
