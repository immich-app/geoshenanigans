"""Split a packed ways2 file (streets track) into column files for whole-file zstd measurement.
street (8 B): cnt u16 | code u16 | name u32 -> <out>.cnt, <out>.code, <out>.name
interp (17 B): cnt u16 | code u16 | street u32 | start u32 | end u32 | interp u8
   -> <out>.cntcode (cnt,code interleaved), .street, .start, .span (= end - start mod 2^32, lossless), .interp
Checks the columns rebuild the ways2 file exactly. usage: split_cols.py street|interp <ways2> <out>"""
import sys
import numpy as np

kind, src, out = sys.argv[1:4]
if kind == "street":
    dt = np.dtype([("cnt", "<u2"), ("code", "<u2"), ("name", "<u4")])
    w = np.fromfile(src, dt)
    for c in ("cnt", "code", "name"):
        np.ascontiguousarray(w[c]).tofile(f"{out}.{c}")
    back = np.empty(len(w), dt)
    for c in ("cnt", "code", "name"):
        back[c] = np.fromfile(f"{out}.{c}", dt[c])
else:
    dt = np.dtype([("cnt", "<u2"), ("code", "<u2"), ("street", "<u4"), ("start", "<u4"), ("end", "<u4"), ("interp", "u1")])
    w = np.fromfile(src, dt)
    cc = np.empty(len(w), np.dtype([("cnt", "<u2"), ("code", "<u2")]))
    cc["cnt"], cc["code"] = w["cnt"], w["code"]
    cc.tofile(f"{out}.cntcode")
    for c in ("street", "start", "interp"):
        np.ascontiguousarray(w[c]).tofile(f"{out}.{c}")
    span = ((w["end"].astype(np.int64) - w["start"].astype(np.int64)) % (1 << 32)).astype("<u4")
    span.tofile(f"{out}.span")
    back = np.empty(len(w), dt)
    cc2 = np.fromfile(f"{out}.cntcode", cc.dtype)
    back["cnt"], back["code"] = cc2["cnt"], cc2["code"]
    for c in ("street", "start", "interp"):
        back[c] = np.fromfile(f"{out}.{c}", dt[c])
    back["end"] = ((back["start"].astype(np.int64) + np.fromfile(f"{out}.span", "<u4")) % (1 << 32)).astype("<u4")
assert back.tobytes() == w.tobytes(), "columns do not rebuild ways2"
print(kind, "columns rebuild ways2 exactly;", len(w), "ways")
