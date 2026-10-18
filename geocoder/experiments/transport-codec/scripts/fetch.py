"""Download one region selection (flat dir): keeps <name>.zst and the decoded file, checks sha256.

fetch.py <manifest.json> <date> <region> <mode> <quality> <poi> <dest>
"""
import hashlib, json, os, subprocess, sys, time, urllib.request

BASE = "https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds"
man, date, region, mode, quality, poi, dest = sys.argv[1:8]
c = json.load(open(man))
F = c["files"]


def sub(p):
    return (p.replace("{region}", region).replace("{mode}", mode)
            .replace("{quality}", quality).replace("{poi_tier}", poi))


files, rep = [], set()
for cid in [f"mode.{mode}", f"quality.{quality}", f"poi_tier.{poi}"]:
    cp = c["components"][cid]
    files += [sub(f) for f in cp.get("files", [])]
    files += [sub(f) for f in cp.get("optional_files", []) if sub(f) in F]
    rep |= {sub(r) for r in cp.get("replaces", [])}
files = sorted(set(f for f in files if f not in rep))
os.makedirs(dest, exist_ok=True)
t0 = time.time()
tot = 0
with open(os.path.join(dest, "files.tsv"), "w") as idx:
    for f in files:
        out = os.path.join(dest, os.path.basename(f))
        z = out + ".zst"
        urllib.request.urlretrieve(f"{BASE}/{date}/{f}.zst", z)
        tot += os.path.getsize(z)
        subprocess.run(["/ssd/shrink/zstd157/bin/zstd", "-d", "-q", "-f", z, "-o", out], check=True)
        h = hashlib.sha256(open(out, "rb").read()).hexdigest()
        ok = h == F[f]["sha256"]
        idx.write(f"{f}\t{F[f]['size_raw']}\t{F[f]['size_zst']}\t{os.path.getsize(z)}\t{'ok' if ok else 'MISMATCH'}\n")
        if not ok:
            print("HASH MISMATCH", f)
print(f"{len(files)} files, {tot / 1048576:.2f} MiB downloaded in {time.time() - t0:.1f}s")
