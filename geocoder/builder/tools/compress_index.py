#!/usr/bin/env python3
"""Compresses a build's index files for publishing.

Usage: compress_index.py <output dir>

Writes <file>.zst next to every .bin file and load sidecar under the output
dir and records each one's raw size, compressed size and raw sha256 in
configurations.json["files"]. The .bin files stay: patch generation may still
be reading them, so the caller removes them.

zstd level 19 gives essentially the same ratio as --ultra -22 on our
structured binary data (3 of 5 sampled files identical, the largest gain was
2.1% on strings.bin) at ~10x less CPU; levels below 19 cost 25-30% in size.

Scheduling: one worker per core, largest file first, each file compressed
with zstd threads in proportion to its size. zstd only splits a file into
parallel jobs above ~32 MiB at this level, so most files are single-threaded
and need many workers, while the multi-GiB ones need threads of their own or
they finish long after everything else. On a planet output (105.7 GiB, 64
cores) this took 2104 s against 2933 s for the previous 4 workers x -T0.
"""
import hashlib
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

LEVEL = "-19"
BYTES_PER_THREAD = 256 << 20
MAX_THREADS_PER_FILE = 16

# JSON sidecars the server's Index::load reads alongside the .bin files. They
# go into configurations.files like any index file so the region downloader
# can fetch them; the raw copy stays for clients that read it directly
# (test-portal).
LOAD_SIDECARS = {"strings_layout.json", "poi_meta.json"}


def index_files(out):
    found = []
    for root, _, names in os.walk(out):
        for name in names:
            # Strategy-2 sidecars (*.osm_ids) stay uncompressed: they're cached
            # only and the next build's IdAllocator reads them directly.
            if name.endswith(".bin") or name in LOAD_SIDECARS:
                found.append(os.path.join(root, name))
    return sorted(found, key=lambda p: (-os.path.getsize(p), p))


def compress(path):
    raw_size = os.path.getsize(path)
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            sha.update(chunk)
    threads = max(1, min(MAX_THREADS_PER_FILE, raw_size // BYTES_PER_THREAD))
    subprocess.run(["zstd", LEVEL, f"-T{threads}", "-f", "--quiet", path], check=True)
    return {"size_zst": os.path.getsize(path + ".zst"), "size_raw": raw_size, "sha256": sha.hexdigest()}


def main():
    out = sys.argv[1]
    cpath = os.path.join(out, "configurations.json")
    with open(cpath) as f:
        cfg = json.load(f)

    paths = index_files(out)
    print(f"Compressing {len(paths)} files with zstd {LEVEL} on {os.cpu_count()} workers...", flush=True)
    files = {}
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as ex:
        for path, entry in zip(paths, ex.map(compress, paths)):
            rel = os.path.relpath(path, out)
            files[rel] = entry
            pct = 100 * entry["size_zst"] / entry["size_raw"] if entry["size_raw"] else 0
            print(f"  {rel}: {entry['size_raw'] / 1e6:.0f} -> {entry['size_zst'] / 1e6:.0f} MB ({pct:.0f}%)", flush=True)

    # Stable ordering for deterministic diffs.
    cfg["files"] = dict(sorted(files.items()))
    with open(cpath, "w") as f:
        json.dump(cfg, f, indent=2)

    total_raw = sum(e["size_raw"] for e in files.values())
    total_zst = sum(e["size_zst"] for e in files.values())
    if total_raw:
        print(f"Total: {total_raw / 1e9:.2f} GB raw -> {total_zst / 1e9:.2f} GB compressed ({100 * total_zst / total_raw:.0f}%)")


if __name__ == "__main__":
    main()
