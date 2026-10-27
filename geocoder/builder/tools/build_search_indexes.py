#!/usr/bin/env python3
"""Builds the place search index of each shipped selection, in every region of a build.

Usage: build_search_indexes.py <output dir> <place-search binary> [--jobs N] [--threads T]

configurations.json's search_index block names the selections whose packs ship the index
(`shipped`) and where it goes (`dir`). For each region of the output, a shipped selection's
files (its mode, quality and POI tier components, as a client resolves them) are linked into
one flat dir, the way a client holds them, and `place-search build` writes the index of that
dir into `dir`. compress_index.py then publishes it with the rest. A client of the selection
downloads it; any other selection builds it on the device. The index records the sha256 of
every file it was built from (search_meta.json), so a client never reads it beside others.

A failed build leaves no index dir behind (its clients build theirs on the device) and makes
the exit status non-zero.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

AXES = ("mode", "quality", "poi_tier")


def substitute(path, sel):
    for key, value in sel.items():
        path = path.replace("{" + key + "}", value)
    return path


def resolve(cfg, sel):
    """The selection's paths (sorted) and which of them are optional, as the region downloader
    resolves them: each axis component's files and optional files, after those of the component
    it extends, minus every path a component replaces."""
    files, optional, replaced = [], set(), set()

    def visit(cid, seen):
        component = cfg["components"].get(cid)
        if cid in seen or component is None:
            return
        seen.add(cid)
        if component.get("extends"):
            visit(component["extends"], seen)
        files.extend(substitute(f, sel) for f in component.get("files", []))
        for f in component.get("optional_files", []):
            optional.add(substitute(f, sel))
            files.append(substitute(f, sel))
        replaced.update(substitute(r, sel) for r in component.get("replaces", []))

    for axis in AXES:
        visit(f"{axis}.{sel[axis]}", set())
    paths = sorted(set(files) - replaced)
    return paths, optional & set(paths)


def link_set(out, paths, optional, set_dir):
    """Links the selection's files flat into set_dir, a later path's file taking a shared name
    (a client downloads in path order). An optional file may be missing, no other."""
    os.makedirs(set_dir)
    for rel in paths:
        src = os.path.join(out, rel)
        if not os.path.exists(src):
            if rel in optional:
                continue
            raise FileNotFoundError(f"{rel} is missing")
        dst = os.path.join(set_dir, os.path.basename(rel))
        if os.path.lexists(dst):
            os.remove(dst)
        os.symlink(os.path.abspath(src), dst)


def selections_of(out, cfg):
    """Every shipped selection in every region dir of the output, largest region first (planet
    holds all the others)."""
    regions = [r for r in cfg["axes"]["region"]["values"] if os.path.isdir(os.path.join(out, r))]
    regions.sort(key=lambda r: (r != "planet", r))
    return [dict(sel, region=region) for region in regions for sel in cfg["search_index"]["shipped"]]


def build_one(out, cfg, sel, binary, threads, scratch):
    """Builds one selection's index; returns its label and the error, None when built."""
    search = cfg["search_index"]
    label = "/".join(sel[k] for k in ("region",) + AXES)
    set_dir = os.path.join(scratch, "sets", label)
    tmp_dir = os.path.join(scratch, "tmp", label)
    index_dir = os.path.join(out, substitute(search["dir"], sel))
    shutil.rmtree(index_dir, ignore_errors=True)
    start = time.time()
    try:
        link_set(out, *resolve(cfg, sel), set_dir)
        run = subprocess.run([binary, "build", set_dir, index_dir, "--threads", str(threads), "--tmp", tmp_dir],
                             capture_output=True, text=True)
        missing = [f for f in search["files"] if not os.path.exists(os.path.join(index_dir, f))]
        error = (f"place-search exited {run.returncode}: {run.stderr.strip()[-2000:]}" if run.returncode
                 else f"no {', '.join(missing)}" if missing else None)
    except OSError as e:
        error = str(e)
    finally:
        shutil.rmtree(set_dir, ignore_errors=True)
        shutil.rmtree(tmp_dir, ignore_errors=True)
    if error:
        shutil.rmtree(index_dir, ignore_errors=True)
        return label, error
    size = sum(os.path.getsize(os.path.join(index_dir, f)) for f in os.listdir(index_dir))
    print(f"  {label}: {size / 1e6:.1f} MB in {time.time() - start:.0f}s", flush=True)
    return label, None


def main():
    args = argparse.ArgumentParser()
    args.add_argument("out")
    args.add_argument("binary")
    args.add_argument("--threads", type=int, default=4)
    args.add_argument("--jobs", type=int, default=0, help="builds at once (default: cores / threads)")
    opts = args.parse_args()
    with open(os.path.join(opts.out, "configurations.json")) as f:
        cfg = json.load(f)
    selections = selections_of(opts.out, cfg)
    workers = opts.jobs or max(1, (os.cpu_count() or 1) // opts.threads)
    scratch = os.path.join(os.environ.get("TMPDIR", "/tmp"), f"search-indexes-{os.getpid()}")
    print(f"Building {len(selections)} place search indexes, {workers} at once...", flush=True)
    try:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            results = list(ex.map(lambda sel: build_one(opts.out, cfg, sel, opts.binary, opts.threads, scratch), selections))
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    failed = [(label, err) for label, err in results if err]
    for label, err in failed:
        print(f"::error::place search index {label}: {err}", flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
