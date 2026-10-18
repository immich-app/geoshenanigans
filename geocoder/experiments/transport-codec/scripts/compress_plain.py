"""zstd -18 (CI thread rule) of every file of a flat dir into OUT/<name>.zst; compares with a manifest.

compress_plain.py <srcdir> <outdir> <manifest.json> <prefix1,prefix2,...> [--jobs N]
"""
import json, os, subprocess, sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "/ssd/transport/scripts")
import specs  # noqa: E402

src, out, man, prefixes = sys.argv[1:5]
jobs = int(sys.argv[sys.argv.index("--jobs") + 1]) if "--jobs" in sys.argv else 16
F = json.load(open(man))["files"]
pub = {}
for p in prefixes.split(","):
    for k, v in F.items():
        if k.startswith(p) and "/" not in k[len(p):]:
            pub[os.path.basename(k)] = v["size_zst"]  # last prefix wins (same json in two dirs)
os.makedirs(out, exist_ok=True)
names = sorted((n for n in os.listdir(src) if n.endswith(".bin") or n.endswith(".json")),
               key=lambda n: -os.path.getsize(os.path.join(src, n)))


def one(n):
    f = os.path.join(src, n)
    thr = specs.zstd_threads(f)
    r = subprocess.run(["/usr/bin/time", "-f", "%U %S", specs.ZSTD, "-18", f"-T{thr}", "-q", "-f", f, "-o",
                        os.path.join(out, n + ".zst")], check=True, capture_output=True, text=True)
    cpu = sum(float(x) for x in r.stderr.strip().split()[-2:])
    z = os.path.getsize(os.path.join(out, n + ".zst"))
    return n, z, cpu


with ThreadPoolExecutor(jobs) as ex:
    res = list(ex.map(one, names))
M = 1048576
tot_me = tot_pub = 0
for n, z, cpu in res:
    p = pub.get(n)
    tot_me += z
    tot_pub += p or 0
    print(f"{n}\t{z}\t{p}\t{'same' if p == z else 'DIFF %+d' % (z - (p or 0))}\tcpu={cpu:.1f}")
print(f"TOTAL mine {tot_me / M:.2f} MiB published {tot_pub / M:.2f} MiB")
print(f"zstd cpu total {sum(c for _, _, c in res):.0f} s")
json.dump({n: dict(zst=z, cpu_s=cpu) for n, z, cpu in res}, open(os.path.join(out, "sizes.json"), "w"), indent=1)
