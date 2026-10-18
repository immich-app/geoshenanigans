"""Independent re-run: tt enc (verify build) + zstd 1.5.7 -18 (CI thread rule) for a flat dir.

enc_set.py SRC OUT CFG BLOCK [--jobs N] [--files a,b] [--keep]
  CFG: theirs | raw | path to a colsearch json ({file: {"spec": ...}})
Writes OUT/sizes.json. Layout facts (struct widths, side records) come from specs.py;
everything else (driver, binary, thread rule) is re-implemented here.
"""
import argparse, json, os, subprocess, sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "/ssd/transport/scripts")
import specs  # noqa: E402

TT = os.environ.get("TT", "/ssd/transport-verify/bin/tt")
Z = "/ssd/shrink/zstd157/bin/zstd"

ap = argparse.ArgumentParser()
ap.add_argument("src"); ap.add_argument("out"); ap.add_argument("cfg"); ap.add_argument("block", type=int)
ap.add_argument("--jobs", type=int, default=16)
ap.add_argument("--files", default="")
ap.add_argument("--keep", action="store_true")
a = ap.parse_args()
if a.cfg == "theirs":
    cfg = dict(specs.THEIRS)
elif a.cfg == "raw":
    cfg = {}
else:
    cfg = {f: v["spec"] for f, v in json.load(open(a.cfg)).items()}
os.makedirs(a.out, exist_ok=True)
names = sorted(n for n in os.listdir(a.src) if (n.endswith(".bin") or n.endswith(".json"))
               and os.path.isfile(os.path.join(a.src, n)))
if a.files:
    want = set(a.files.split(","))
    names = [n for n in names if n in want]
names.sort(key=lambda n: -os.path.getsize(os.path.join(a.src, n)))


def threads(p):
    # compress_index.py: BYTES_PER_THREAD = 256 MiB, MAX_THREADS_PER_FILE = 16
    return max(1, min(16, os.path.getsize(p) // (256 << 20)))


def one(n):
    src = os.path.join(a.src, n)
    spec = cfg.get(n, "")
    kind = specs.KIND[n] if spec else "r"
    tt = os.path.join(a.out, n + ".tt")
    side = []
    if spec and n in specs.SIDE:
        rec, stride, off = specs.SIDE[n]
        side = ["--side", f"{os.path.join(a.src, rec)}:{stride}:{off}"]
    subprocess.run([TT, "enc", kind, spec or "-", src, tt, "--block", str(a.block)] + side, check=True)
    t = threads(tt)
    subprocess.run([Z, "-18", f"-T{t}", "-q", "-f", tt, "-o", tt + ".zst"], check=True)
    os.remove(tt)
    z = os.path.getsize(tt + ".zst")
    if not a.keep:
        os.remove(tt + ".zst")
    r = dict(file=n, kind=kind, spec=spec, raw=os.path.getsize(src), zst=z, threads=t)
    print(json.dumps(r), flush=True)
    return r


with ThreadPoolExecutor(a.jobs) as ex:
    res = list(ex.map(one, names))
json.dump(dict(src=a.src, cfg=a.cfg, block=a.block, tt=TT, results=res), open(os.path.join(a.out, "sizes.json"), "w"),
          indent=1)
print("TOTAL %d files zst %.2f MiB raw %.2f MiB" % (len(res), sum(r["zst"] for r in res) / 1048576,
                                                    sum(r["raw"] for r in res) / 1048576))
