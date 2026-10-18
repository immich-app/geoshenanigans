"""Transform + zstd -18 (CI thread rule) + round-trip check for every file of a flat dir.

run_cfg.py <srcdir> <outdir> <cfg: theirs|raw|best.json> <block bytes, 0 = whole file> [--jobs N] [--keep] [--files a,b]
Writes <outdir>/results.json. With --keep the .tt.zst files stay in outdir.
Files without a spec (strings, json) go through as kind r (header only).
"""
import argparse, json, os, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "/ssd/transport/scripts")
import specs  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("src"); ap.add_argument("out"); ap.add_argument("cfg"); ap.add_argument("block", type=int)
ap.add_argument("--jobs", type=int, default=16)
ap.add_argument("--keep", action="store_true")
ap.add_argument("--noverify", action="store_true")
ap.add_argument("--files", default="")
a = ap.parse_args()
cfg = specs.load_cfg(a.cfg)
os.makedirs(a.out, exist_ok=True)
names = sorted(n for n in os.listdir(a.src)
               if (n.endswith(".bin") or n.endswith(".json")) and os.path.isfile(os.path.join(a.src, n)))
if a.files:
    names = [n for n in names if n in a.files.split(",")]
names.sort(key=lambda n: -os.path.getsize(os.path.join(a.src, n)))


def one(name):
    f = os.path.join(a.src, name)
    spec = cfg.get(name, "")
    kind = specs.KIND[name] if spec else "r"
    tt = os.path.join(a.out, name + ".tt")
    t0 = time.time()
    subprocess.run([specs.TT, "enc", kind, spec or "-", f, tt, "--block", str(a.block)] + specs.side_arg(a.src, name),
                   check=True)
    t_enc = time.time() - t0
    thr = specs.zstd_threads(tt)
    t0 = time.time()
    r = subprocess.run(["/usr/bin/time", "-f", "%U %S", specs.ZSTD, "-18", f"-T{thr}", "-q", "-f", tt, "-o", tt + ".zst"],
                       check=True, capture_output=True, text=True)
    t_z = time.time() - t0
    zcpu = sum(float(x) for x in r.stderr.strip().split()[-2:])
    zsz = os.path.getsize(tt + ".zst")
    os.remove(tt)
    ok = None
    if not a.noverify:
        dec = os.path.join(a.out, name + ".dec")
        subprocess.run([specs.TT, "dec", tt + ".zst", dec] + specs.side_arg(a.src, name), check=True)
        ok = subprocess.run(["cmp", "-s", dec, f]).returncode == 0
        os.remove(dec)
    if not a.keep:
        os.remove(tt + ".zst")
    res = dict(file=name, kind=kind, spec=spec, raw=os.path.getsize(f), zst=zsz, ok=ok, enc_wall_s=round(t_enc, 2),
               zstd_wall_s=round(t_z, 2), zstd_cpu_s=round(zcpu, 2), threads=thr)
    print(json.dumps(res), flush=True)
    return res


with ThreadPoolExecutor(a.jobs) as ex:
    results = list(ex.map(one, names))
json.dump(dict(src=a.src, cfg=a.cfg, block=a.block, results=results), open(os.path.join(a.out, "results.json"), "w"),
          indent=1)
bad = [r["file"] for r in results if r["ok"] is False]
print("TOTAL zst %.2f MiB, raw %.2f MiB, roundtrip failures: %s" % (
    sum(r["zst"] for r in results) / 1048576, sum(r["raw"] for r in results) / 1048576, bad or "none"))
