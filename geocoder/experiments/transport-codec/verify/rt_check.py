"""rt_check.py DECODED_DIR SRC_DIR LABEL : sha256 of every decoded file vs the 2026-10-13 manifest, and cmp vs source."""
import hashlib, json, os, subprocess, sys
from concurrent.futures import ThreadPoolExecutor

d, src, label = sys.argv[1:4]
F = json.load(open("/ssd/shrink/data/configurations-2026-10-13.json"))["files"]
want = {}
for k, v in F.items():
    for p in ("planet/full/", "planet/quality/uncapped/", "planet/poi/all/"):
        if k.startswith(p) and "/" not in k[len(p):]:
            want.setdefault(os.path.basename(k), set()).add(v["sha256"])
names = sorted(os.listdir(src))


def one(n):
    out = os.path.join(d, n)
    if not os.path.exists(out):
        return n, "MISSING", False
    h = hashlib.sha256()
    with open(out, "rb") as f:
        while b := f.read(1 << 24):
            h.update(b)
    same = subprocess.run(["cmp", "-s", out, os.path.join(src, n)]).returncode == 0
    return n, h.hexdigest() in want.get(n, ()), same


with ThreadPoolExecutor(16) as ex:
    res = list(ex.map(one, names))
bad = [r for r in res if r[1] is not True or not r[2]]
print(f"{label}: {len(res)} files, sha256==manifest {sum(r[1] is True for r in res)}/{len(res)}, "
      f"cmp==source {sum(r[2] for r in res)}/{len(res)}, {'PASS' if not bad else 'FAIL ' + str(bad)}")
