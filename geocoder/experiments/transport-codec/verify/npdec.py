"""Independent numpy decoder for GTT1 streams of struct kind (s:...) or raw kind (r).

zstd -dc FILE.tt.zst | npdec.py SOURCE_BIN EXPECTED_SHA256
Written from the format description only (header, per-block u32 unit count + one encoding byte
per column, columns in field order, BSS = byte planes, D/d zigzag delta, S/s step from the
previous non-all-ones value). Prints sha256, whether it equals the manifest hash, and whether the
output equals SOURCE_BIN byte for byte.
"""
import hashlib, struct, sys

import numpy as np

src_path, want_sha = sys.argv[1], sys.argv[2]
f = sys.stdin.buffer
src = open(src_path, "rb")


def need(n):
    b = f.read(n)
    if len(b) != n:
        raise SystemExit(f"truncated: wanted {n} got {len(b)}")
    return b


if need(4) != b"GTT1":
    raise SystemExit("bad magic")
kind = need(need(1)[0]).decode()
spec = need(need(1)[0]).decode()
raw, block = struct.unpack("<QQ", need(16))
DT = {1: np.uint8, 2: np.uint16, 4: np.uint32, 8: np.uint64}
h = hashlib.sha256()
same = True
done = 0
blocks = 0
encs_seen = set()


def emit(b):
    global same, done
    h.update(b)
    if same and src.read(len(b)) != b:
        same = False
    done += len(b)


if kind == "r":
    while done < raw:
        emit(need(min(raw - done, 1 << 22)))
else:
    assert kind.startswith("s:"), kind
    widths = [int(x) for x in kind[2:].split(",")]
    stride = sum(widths)
    body_end = raw // stride * stride
    while done < body_end:
        nu = struct.unpack("<I", need(4))[0]
        encs = need(len(widths)).decode("latin1")
        encs_seen.add(encs)
        rec = np.empty((nu, stride), np.uint8)
        off = 0
        for w, e in zip(widths, encs):
            t = DT[w]
            buf = need(nu * w)
            if e in "BDSP" and w > 1:
                v = np.ascontiguousarray(np.frombuffer(buf, np.uint8).reshape(w, nu).T).view(t).reshape(nu)
            else:
                v = np.frombuffer(buf, t).copy()
            if e in "Dd":
                one = t(1)
                d = (v >> one) ^ (t(0) - (v & one))
                v = np.cumsum(d, dtype=t)
            elif e in "Ss":
                m = v != t(np.iinfo(t).max)
                v[m] = np.cumsum(v[m], dtype=t)
            elif e not in "RB":
                raise SystemExit(f"unexpected encoding {e}")
            rec[:, off:off + w] = v.view(np.uint8).reshape(nu, w)
            off += w
        emit(rec.tobytes())
        blocks += 1
    if raw > done:
        emit(need(raw - done))
extra = f.read(1)
got = h.hexdigest()
print(f"kind={kind} spec={spec} block={block} blocks={blocks} encs={sorted(encs_seen)} raw={raw} out={done} "
      f"trailing_input={'yes' if extra else 'no'} sha256_match={got == want_sha} cmp_source={'same' if same and src.read(1) == b'' else 'DIFF'}")
