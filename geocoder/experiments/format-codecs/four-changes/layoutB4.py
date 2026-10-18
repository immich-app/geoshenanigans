"""Layout B (change 3) for a whole admin_vertices.bin, in polygon chunks so the uncapped planet file
fits comfortably in RAM. Encoder/decoder are stack/layouts.py's encode_B/decode_B (copied here as
layouts_stack.py, unchanged): per polygon, the block keeps its offset and size; header verbatim, then
wrapping zigzag deltas at the polygon's own width as byte planes inside the block.
usage: layoutB4.py encode <polys_dir> <admin_vertices in> <B out>
       layoutB4.py decode <polys_dir> <B in> <admin_vertices out>
decode reads only admin_polygons.bin and the B file."""
import sys, time
import numpy as np
from layouts_stack import encode_B, decode_B, polygons

mode, d, src, dst = sys.argv[1:5]
off, cnt = polygons(d)
vb = np.memmap(src, np.uint8, mode="r")
t = time.time()
CH = 40_000_000  # vertices per chunk
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
        out = encode_B(sub, o, c, False) if mode == "encode" else decode_B(sub, o, c, False)
        assert len(out) == b1 - b0
        f.write(out.tobytes())
print(f"{mode} B: {len(vb):,} bytes, {len(cnt):,} polygons, {int(cnt.sum()):,} vertices, {len(bounds) - 1} chunks, "
      f"{time.time() - t:.1f}s", flush=True)
