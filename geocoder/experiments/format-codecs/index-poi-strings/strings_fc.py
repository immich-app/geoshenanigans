"""Front coding of the (bytewise sorted) string tiers, on-disk with random access per block:
per block of K strings the first verbatim (NUL-terminated), each other as varint(shared prefix
length with the previous string) + NUL-terminated suffix; a u32 byte offset per block. A string
is then addressed by its ordinal (refs would become ordinals: block = ord / K, decode <= K-1
strings). Round trip checked by decoding every block back to today's tier bytes.
usage: strings_fc.py <tier dir> <out dir> [K]"""
import json
import os
import sys
import numpy as np

src, out = sys.argv[1], sys.argv[2]
K = int(sys.argv[3]) if len(sys.argv) > 3 else 16
os.makedirs(out, exist_ok=True)
lay = json.load(open(f"{src}/strings_layout.json"))["tiers"]


def uvarint(n):
    b = bytearray()
    while True:
        x = n & 0x7F
        n >>= 7
        if n:
            b.append(x | 0x80)
        else:
            b.append(x)
            return bytes(b)


for t in lay:
    raw = open(f"{src}/{t['file']}", "rb").read()
    parts = raw.split(b"\0")[:-1]
    body = bytearray()
    offs = []
    prev = b""
    for i, s in enumerate(parts):
        if i % K == 0:
            offs.append(len(body))
            body += s + b"\0"
        else:
            l = 0
            m = min(len(s), len(prev))
            while l < m and s[l] == prev[l]:
                l += 1
            body += uvarint(l) + s[l:] + b"\0"
        prev = s
    # decode check
    rec = bytearray()
    p = 0
    for bi, o in enumerate(offs):
        p = o
        end = offs[bi + 1] if bi + 1 < len(offs) else len(body)
        q = body.index(0, p)
        cur = bytes(body[p:q])
        rec += cur + b"\0"
        p = q + 1
        while p < end:
            l = 0
            sh = 0
            while True:
                x = body[p]
                p += 1
                l |= (x & 0x7F) << sh
                sh += 7
                if not x & 0x80:
                    break
            q = body.index(0, p)
            cur = cur[:l] + bytes(body[p:q])
            rec += cur + b"\0"
            p = q + 1
    ok = bytes(rec) == raw
    open(f"{out}/{t['file']}.fc{K}", "wb").write(bytes(body))
    np.array(offs, "<u4").tofile(f"{out}/{t['file']}.fc{K}.idx")
    print(f"{t['file']:22s} raw {len(raw):>12,d} -> fc{K} body {len(body):>12,d} + idx {4*len(offs):>10,d}; round trip {ok}", flush=True)
    assert ok
