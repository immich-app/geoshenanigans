"""zstd 1.5.7 -18 (CI thread rule) of every .bin/.json of a flat dir -> OUT/<name>.zst, sizes vs manifest.

plain_set.py SRC OUT MANIFEST PREFIX1,PREFIX2,... [--jobs N]
"""
import json, os, subprocess, sys
from concurrent.futures import ThreadPoolExecutor

Z = "/ssd/shrink/zstd157/bin/zstd"
src, out, man, prefixes = sys.argv[1:5]
jobs = int(sys.argv[sys.argv.index("--jobs") + 1]) if "--jobs" in sys.argv else 16
F = json.load(open(man))["files"]
pub = {}
for p in prefixes.split(","):
    for k, v in F.items():
        if k.startswith(p) and "/" not in k[len(p):]:
            pub.setdefault(os.path.basename(k), []).append((k, v))
os.makedirs(out, exist_ok=True)
names = sorted((n for n in os.listdir(src) if n.endswith(".bin") or n.endswith(".json")),
               key=lambda n: -os.path.getsize(os.path.join(src, n)))


def one(n):
    f = os.path.join(src, n)
    t = max(1, min(16, os.path.getsize(f) // (256 << 20)))
    subprocess.run([Z, "-18", f"-T{t}", "-q", "-f", f, "-o", os.path.join(out, n + ".zst")], check=True)
    return n, os.path.getsize(os.path.join(out, n + ".zst"))


with ThreadPoolExecutor(jobs) as ex:
    res = list(ex.map(one, names))
M = 1048576
same = 0
rows = {}
for n, z in res:
    cands = pub.get(n, [])
    match = [k for k, v in cands if v["size_zst"] == z]
    same += bool(match)
    rows[n] = dict(zst=z, published=[(k, v["size_zst"]) for k, v in cands], match=match)
    print(f"{n}\t{z}\t{[v['size_zst'] for k, v in cands]}\t{'same' if match else 'DIFF'}")
pub_total = sum(v["size_zst"] for c in pub.values() for k, v in c)
print(f"matched {same}/{len(res)}; mine {sum(z for _, z in res) / M:.2f} MiB over {len(res)} files; "
      f"manifest {pub_total / M:.2f} MiB over {sum(len(c) for c in pub.values())} published files")
json.dump(rows, open(os.path.join(out, "sizes.json"), "w"), indent=1)
