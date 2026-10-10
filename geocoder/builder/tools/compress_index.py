#!/usr/bin/env python3
"""Compresses a build's index files for publishing.

Usage: compress_index.py <output dir>

Writes <file>.zst next to every .bin file and load sidecar under the output
dir (files with identical content share one .zst through hard links) and
records each one's raw size, compressed size and raw sha256 in
configurations.json["files"]. The .bin files stay: patch generation may still
be reading them, so the caller removes them.

zstd level 18 compresses our structured binary data to within 0.3% of
level 19 (itself within ~2% of --ultra -22) at 21% less CPU; level 17 and
below cost 25-30% in size (planet samples of geo_cells, addr_points,
addr_vertices, street_nodes and strings_addr).

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

LEVEL = "-18"
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


def sha256(path):
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            sha.update(chunk)
    return sha.hexdigest()


def compress(path):
    threads = max(1, min(MAX_THREADS_PER_FILE, os.path.getsize(path) // BYTES_PER_THREAD))
    subprocess.run(["zstd", LEVEL, f"-T{threads}", "-f", "--quiet", path], check=True)


def main():
    out = sys.argv[1]
    cpath = os.path.join(out, "configurations.json")
    with open(cpath) as f:
        cfg = json.load(f)

    paths = index_files(out)
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as ex:
        hashes = list(ex.map(sha256, paths))
    # Variants share byte-identical files (e.g. full and no-addresses have
    # the same street files, ~18% of a planet build): compress each content
    # once and hard-link the copies' .zst to it.
    first = {}
    for path, digest in zip(paths, hashes):
        first.setdefault(digest, path)
    distinct = list(first.values())
    print(f"Compressing {len(distinct)} distinct of {len(paths)} files with zstd {LEVEL} "
          f"on {os.cpu_count()} workers...", flush=True)
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as ex:
        list(ex.map(compress, distinct))

    files = {}
    for path, digest in zip(paths, hashes):
        source = first[digest]
        if source != path:
            if os.path.exists(path + ".zst"):
                os.remove(path + ".zst")
            os.link(source + ".zst", path + ".zst")
        rel = os.path.relpath(path, out)
        files[rel] = {"size_zst": os.path.getsize(path + ".zst"), "size_raw": os.path.getsize(path), "sha256": digest}
        pct = 100 * files[rel]["size_zst"] / files[rel]["size_raw"] if files[rel]["size_raw"] else 0
        print(f"  {rel}: {files[rel]['size_raw'] / 1e6:.0f} -> {files[rel]['size_zst'] / 1e6:.0f} MB ({pct:.0f}%)")

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
