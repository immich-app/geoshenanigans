"""Extra admin_vertices layouts the four-changes track did not measure on the planet presets:
  Bg  layout B + per-polygon divisor (stack/layouts.py encode_B(with_g=True)); on-disk, same
      per-polygon block offsets as today, so random access is kept
  An  native grids, headers in one block, byte planes over the whole file (download-only)
  A   global 1e-4 grid, whole-file planes (download-only; only exact on 1e-4-snapped data)
Chunked by polygons like four-changes/layoutB4.py for B/Bg; An/A run on the whole file.
usage: layout_more.py encode|decode <fmt> <polys_dir> <in> <out>"""
import sys
import time
import numpy as np

sys.path.insert(0, "/ssd/shrink/max/four-changes")
from layouts_stack import ENC, DEC, encode_B, decode_B, polygons  # noqa: E402

mode, fmt, d, src, dst = sys.argv[1:6]
off, cnt = polygons(d)
t = time.time()
if fmt in ("B", "Bg"):
    g = fmt == "Bg"
    vb = np.memmap(src, np.uint8, mode="r")
    CH = 40_000_000
    cum = np.cumsum(cnt)
    bounds = [0]
    while bounds[-1] < len(cnt):
        nxt = int(np.searchsorted(cum, (cum[bounds[-1] - 1] if bounds[-1] else 0) + CH, side="right"))
        bounds.append(max(nxt, bounds[-1] + 1))
    bounds[-1] = len(cnt)
    with open(dst, "wb") as f:
        for i0, i1 in zip(bounds[:-1], bounds[1:]):
            b0 = int(off[i0])
            b1 = int(off[i1]) if i1 < len(off) else len(vb)
            sub = np.asarray(vb[b0:b1])
            o, c = off[i0:i1] - b0, cnt[i0:i1]
            out = encode_B(sub, o, c, g) if mode == "encode" else decode_B(sub, o, c, g)
            assert len(out) == b1 - b0
            f.write(out.tobytes())
    n = len(vb)
else:
    buf = np.fromfile(src, np.uint8)
    out = ENC[fmt](buf, off, cnt) if mode == "encode" else DEC[fmt](buf, off, cnt)
    out.tofile(dst)
    n = len(out)
print(f"{mode} {fmt}: {n:,} bytes out, {len(cnt):,} polygons, {int(cnt.sum()):,} vertices, {time.time() - t:.1f}s",
      flush=True)
