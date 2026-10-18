"""Server CPU on a quiet box: zstd -18 (CI thread rule) of the published files vs the forward
transform + zstd -18 of the transformed files (cfg, block), never more than 60 zstd threads at once.

server_cost.py <srcdir> <cfg json> <block> <workdir>
"""
import json, os, subprocess, sys, threading, time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "/ssd/transport/scripts")
import specs  # noqa: E402

src, cfgp, block, work = sys.argv[1:5]
cfg = specs.load_cfg(cfgp)
os.makedirs(work, exist_ok=True)
names = sorted((n for n in os.listdir(src) if n.endswith(".bin") or n.endswith(".json")),
               key=lambda n: -os.path.getsize(os.path.join(src, n)))
CAP = 60
sem = threading.Condition()
free = [CAP]


def cpu_of(cmd):
    r = subprocess.run(["/usr/bin/time", "-f", "%U %S %e"] + cmd, check=True, capture_output=True, text=True)
    u, s, e = r.stderr.strip().split()[-3:]
    return float(u) + float(s), float(e)


def zjob(path):
    thr = specs.zstd_threads(path)
    with sem:
        sem.wait_for(lambda: free[0] >= thr)
        free[0] -= thr
    try:
        cpu, _ = cpu_of([specs.ZSTD, "-18", f"-T{thr}", "-q", "-f", path, "-o", path + ".zst"])
    finally:
        with sem:
            free[0] += thr
            sem.notify_all()
    z = os.path.getsize(path + ".zst")
    os.remove(path + ".zst")
    return cpu, z


def one(n):
    f = os.path.join(src, n)
    plain_cpu, plain_z = zjob(f)
    spec = cfg.get(n, "")
    kind = specs.KIND[n] if spec else "r"
    tt = os.path.join(work, n + ".tt")
    with sem:
        sem.wait_for(lambda: free[0] >= 1)
        free[0] -= 1
    try:
        enc_cpu, enc_wall = cpu_of([specs.TT, "enc", kind, spec or "-", f, tt, "--block", block] + specs.side_arg(src, n))
    finally:
        with sem:
            free[0] += 1
            sem.notify_all()
    tt_cpu, tt_z = zjob(tt)
    os.remove(tt)
    r = dict(file=n, plain_zstd_cpu=plain_cpu, plain_zst=plain_z, enc_cpu=enc_cpu, tt_zstd_cpu=tt_cpu, tt_zst=tt_z)
    print(json.dumps(r), flush=True)
    return r


t0 = time.time()
with ThreadPoolExecutor(24) as ex:
    res = list(ex.map(one, names))
tot = {k: sum(r[k] for r in res) for k in ["plain_zstd_cpu", "enc_cpu", "tt_zstd_cpu", "plain_zst", "tt_zst"]}
json.dump(dict(results=res, totals=tot), open(os.path.join(work, "server_cost.json"), "w"), indent=1)
print("TOTALS", json.dumps(tot), "wall %.0f s" % (time.time() - t0))
