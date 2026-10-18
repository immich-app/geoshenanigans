"""Best encoding per column: dumps every column of every file under every candidate
encoding (whole file, tt cols), compresses each with zstd -18 (CI thread rule) and
keeps the smallest.

colsearch.py <srcdir> <workdir> <out.json> [--jobs N] [--files a,b]
"""
import argparse, glob, json, os, subprocess, sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "/ssd/transport/scripts")
import specs  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("src"); ap.add_argument("work"); ap.add_argument("out")
ap.add_argument("--jobs", type=int, default=24)
ap.add_argument("--files", default="")
a = ap.parse_args()
names = sorted(n for n in specs.KIND if os.path.exists(os.path.join(a.src, n)))
if a.files:
    names = [n for n in names if n in a.files.split(",")]
names.sort(key=lambda n: -os.path.getsize(os.path.join(a.src, n)))


def dump(name):
    d = os.path.join(a.work, name)
    os.makedirs(d, exist_ok=True)
    subprocess.run([specs.TT, "cols", specs.KIND[name], os.path.join(a.src, name), d] + specs.side_arg(a.src, name),
                   check=True, capture_output=True)
    return name


def comp(path):
    thr = specs.zstd_threads(path)
    subprocess.run([specs.ZSTD, "-18", f"-T{thr}", "-q", "-f", path, "-o", path + ".zst"], check=True)
    raw = os.path.getsize(path)
    z = os.path.getsize(path + ".zst")
    os.remove(path)
    os.remove(path + ".zst")
    return path, raw, z


with ThreadPoolExecutor(min(a.jobs, 16)) as ex:
    list(ex.map(dump, names))
cols = sorted(glob.glob(os.path.join(a.work, "*", "*.col")), key=lambda p: -os.path.getsize(p))
with ThreadPoolExecutor(a.jobs) as ex:
    done = list(ex.map(comp, cols))
res = {}
for path, raw, z in done:
    name = os.path.basename(os.path.dirname(path))
    base = os.path.basename(path)[:-4]  # c<k>_<E>[~]
    k, e = base[1:].split("_")
    res.setdefault(name, {}).setdefault(int(k), {})[e] = (raw, z)
out = {}
for name in names:
    per = res.get(name, {})
    ncol = len(specs.COLNAMES[name])
    spec, best_sum, rows = "", 0, []
    for k in range(ncol):
        cand = per[k]
        # prefer the simpler encoding on ties: order R B D d S s P p
        order = "RBDdSsPp"
        e_best = min(cand, key=lambda e: (cand[e][1], order.index(e[0])))
        spec += e_best[0]
        best_sum += cand[e_best][1]
        rows.append(dict(col=specs.COLNAMES[name][k], raw=cand[e_best][0], best=e_best,
                         sizes={e: v[1] for e, v in sorted(cand.items())}))
    out[name] = dict(spec=spec, cols_sum_zst=best_sum, cols=rows)
json.dump(out, open(a.out, "w"), indent=1)
for name, v in out.items():
    print(name, v["spec"], "%.2f MiB" % (v["cols_sum_zst"] / 1048576))
