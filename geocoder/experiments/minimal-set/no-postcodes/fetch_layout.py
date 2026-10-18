#!/usr/bin/env python3
"""Download build files in the build's directory layout (dest/<date>/<path>),
zstd-decompress, check sha256 against that date's configurations.json.
Also fetches <prefix>/patch.gcpatch for each prefix with --patches.

usage: fetch_layout.py <date> <dest root> <prefix>... [--patches] [--link-from DIR]
  --link-from DIR: hardlink a file from DIR/<basename> instead of downloading
  when its sha256 matches (the flat idx/recommended set holds 2026-10-13).
"""
import hashlib, json, os, subprocess, sys, urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = "https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds"
args = [a for a in sys.argv[1:]]
patches = "--patches" in args
link_from = None
if "--link-from" in args:
    i = args.index("--link-from")
    link_from = args[i + 1]
    del args[i:i + 2]
args = [a for a in args if a != "--patches"]
date, dest, prefixes = args[0], args[1], args[2:]
os.makedirs(f"{dest}/{date}", exist_ok=True)
cfg_path = f"{dest}/{date}/configurations.json"
if not os.path.exists(cfg_path):
    urllib.request.urlretrieve(f"{BASE}/{date}/configurations.json", cfg_path)
F = json.load(open(cfg_path))["files"]


def sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def get(path):
    out = f"{dest}/{date}/{path}"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    if os.path.exists(out) and sha(out) == F[path]["sha256"]:
        return f"{path}: present"
    if link_from:
        cand = f"{link_from}/{os.path.basename(path)}"
        if os.path.exists(cand) and sha(cand) == F[path]["sha256"]:
            if os.path.exists(out):
                os.remove(out)
            os.link(cand, out)
            return f"{path}: linked"
    urllib.request.urlretrieve(f"{BASE}/{date}/{path}.zst", out + ".zst")
    subprocess.run(["zstd", "-d", "-q", "-f", "--rm", out + ".zst", "-o", out], check=True)
    return f"{path}: {'OK' if sha(out) == F[path]['sha256'] else 'HASH MISMATCH'}"


wanted = sorted(p for p in F if any(p.startswith(pre.rstrip("/") + "/") and p.count("/") == pre.rstrip("/").count("/") + 1
                                    for pre in prefixes))
with ThreadPoolExecutor(8) as ex:
    for line in ex.map(get, wanted):
        print(line)
if patches:
    for pre in prefixes:
        out = f"{dest}/{date}/{pre.rstrip('/')}/patch.gcpatch"
        os.makedirs(os.path.dirname(out), exist_ok=True)
        urllib.request.urlretrieve(f"{BASE}/{date}/{pre.rstrip('/')}/patch.gcpatch", out)
        print(f"{pre}/patch.gcpatch: {os.path.getsize(out)} bytes")
