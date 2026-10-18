"""Fetch one build date's planet maximum-quality set (full mode, uncapped
boundaries, POI all) in the published directory layout, plus each variant
dir's published patch.gcpatch. Files are zstd-decompressed and checked
against the manifest's sha256.

Usage: fetch_day.py DATE DEST
"""
import concurrent.futures
import hashlib
import json
import os
import subprocess
import sys
import urllib.request

BASE = "https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds"
ZSTD = "/ssd/shrink/zstd157/bin/zstd"
date, dest = sys.argv[1], sys.argv[2]
os.makedirs(dest, exist_ok=True)

cfg_path = os.path.join(dest, "configurations.json")
urllib.request.urlretrieve(f"{BASE}/{date}/configurations.json", cfg_path)
c = json.load(open(cfg_path))
F = c["files"]
sel = {"region": "planet", "mode": "full", "quality": "uncapped", "poi_tier": "all"}


def sub(p):
    return (p.replace("{region}", sel["region"]).replace("{mode}", sel["mode"])
            .replace("{quality}", sel["quality"]).replace("{poi_tier}", sel["poi_tier"]))


files = set()
dirs = set()
for cid in [f"mode.{sel['mode']}", f"quality.{sel['quality']}", f"poi_tier.{sel['poi_tier']}"]:
    cp = c["components"][cid]
    for f in cp.get("files", []):
        files.add(sub(f))
    for f in cp.get("optional_files", []):
        if sub(f) in F:
            files.add(sub(f))
for f in files:
    dirs.add(os.path.dirname(f))


def fetch(path):
    out = os.path.join(dest, path)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    meta = F[path]
    if os.path.exists(out) and os.path.getsize(out) == meta["size_raw"]:
        return path, "skip"
    tmp = out + ".part"
    with urllib.request.urlopen(f"{BASE}/{date}/{path}.zst") as r, \
            open(tmp, "wb") as w:
        p = subprocess.Popen([ZSTD, "-d", "-q", "-c"], stdin=subprocess.PIPE, stdout=w)
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            p.stdin.write(chunk)
        p.stdin.close()
        if p.wait() != 0:
            raise RuntimeError(f"zstd failed on {path}")
    h = hashlib.sha256()
    with open(tmp, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 24), b""):
            h.update(chunk)
    if h.hexdigest() != meta["sha256"]:
        raise RuntimeError(f"sha256 mismatch on {path}")
    os.rename(tmp, out)
    return path, "ok"


with concurrent.futures.ThreadPoolExecutor(4) as ex:
    for path, status in ex.map(fetch, sorted(files)):
        print(status, path, flush=True)

for d in sorted(dirs):
    out = os.path.join(dest, d, "patch.gcpatch")
    try:
        urllib.request.urlretrieve(f"{BASE}/{date}/{d}/patch.gcpatch", out)
        print("patch", d, os.path.getsize(out), flush=True)
    except Exception as e:
        print("no patch", d, e, flush=True)
print("DONE", date, len(files), "files", flush=True)
