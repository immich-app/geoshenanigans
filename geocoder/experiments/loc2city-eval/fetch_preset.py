"""Download one region's selection the way a client would (flat dir, zstd, sha256)."""
import hashlib, json, os, sys, time, urllib.request
import subprocess

BASE = "https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds"
here = "/ssd/shrink/data"
region, mode, quality, poi, date, dest = sys.argv[1:7]
c = json.load(open(f"{here}/configurations-{date}.json"))
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
zst_total = 0
for f in files:
    out = os.path.join(dest, os.path.basename(f))
    url = f"{BASE}/{date}/{f}.zst"
    zpath = out + ".zst"
    urllib.request.urlretrieve(url, zpath)
    zst_total += os.path.getsize(zpath)
    subprocess.run(["zstd", "-d", "-q", "-f", zpath, "-o", out], check=True)
    os.remove(zpath)
    h = hashlib.sha256(open(out, "rb").read()).hexdigest()
    if h != F[f]["sha256"]:
        print("HASH MISMATCH", f)
print(f"{len(files)} files, {zst_total/1048576:.1f} MiB downloaded in {time.time()-t0:.1f}s")
