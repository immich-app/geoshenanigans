#!/usr/bin/env python3
"""Compresses a build's index files for publishing.

Usage: compress_index.py <output dir> --transport-bin PATH [--upload DEST --upload-cmd CMD]

Writes <file>.zst next to every .bin file and load sidecar under the output
dir (files with identical content share one .zst through hard links) and
records each one's raw size, compressed size and raw sha256 in
configurations.json["files"]. The .bin files stay: patch generation may still
be reading them, so the caller removes them.

Transport units (configurations.json["transport"], query_server::transport):
every dir that holds all of a unit's files gets <dir>/<unit>, the stream
`geocoder-transport encode` (--transport-bin) writes of them through zstd at
the same level, which `geocoder-transport verify` then checks decodes to
them. A unit is the only published form of its files: they get no .zst,
their entries in "files" name the unit instead of a size_zst, and
configurations.json["units"] gives each unit's kind, transport version, size
on the wire and files. A unit of files another dir holds alike is that dir's,
hard-linked. A unit that fails (its encode, its zstd or its verify) is not
published: its files go as .zst like any other, and the script exits
UNIT_FAILED once all else is done, for the caller to fail the run at its end.

configurations.json["component_sizes"] gives, per region, each component's
bytes, so a client can weigh a selection without summing files: "raw", its
files on disk, and "wire", what downloading them costs (a unit's size once,
or its share by its files' raw bytes when the component holds only some of
them; each other file's .zst). A component counts only the files the region
has, and a region lists only the components it has a file of.

--upload streams each .zst and unit to DEST/<path> as soon as it exists,
through one long-running CMD (an `s5cmd ... run` reading cp commands on
stdin): the upload is network-bound and compression CPU-bound, so they
overlap instead of the upload waiting for the last file. Upload failures
only warn; the caller re-uploads whatever is missing.

zstd level 18 compresses our structured binary data to within 0.3% of
level 19 (itself within ~2% of --ultra -22) at 21% less CPU; level 17 and
below cost 25-30% in size (planet samples of geo_cells, addr_points,
addr_vertices, street_nodes and strings_addr). It runs the zstd first on
PATH; the nightly puts 1.5.7 there (tools/build_zstd.sh), which at -18 is
smaller and faster than Ubuntu 24.04's 1.5.5.

Scheduling: one worker per core, largest file first, each file compressed
with zstd threads in proportion to its size (a unit's: its files'). zstd
only splits a file into parallel jobs above ~32 MiB at this level, so most
files are single-threaded and need many workers, while the multi-GiB ones
need threads of their own or they finish long after everything else. On a
planet output (105.7 GiB, 64 cores) this took 2104 s against 2933 s for the
previous 4 workers x -T0.
"""
import argparse
import hashlib
import json
import os
import shlex
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

LEVEL = "-18"
BYTES_PER_THREAD = 256 << 20
MAX_THREADS_PER_FILE = 16

# The exit status when a transport unit failed and its files went as .zst.
UNIT_FAILED = 3

# The JSON files a dataset holds beside its .bin files: the sidecars the
# server's Index::load reads, a water dir's kinds, and each dir's credit of
# its data. They go into configurations.files like any index file so the
# region downloader can fetch them; the raw copy stays for clients that read
# it directly (test-portal).
JSON_FILES = {"strings_layout.json", "poi_meta.json", "water_meta.json", "attribution.json"}

# Every file under a region's search/ dir is a place search index file
# (build_search_indexes.py), published like the .bin files.
SEARCH_DIR = "search"


def index_files(out):
    found = []
    for root, _, names in os.walk(out):
        in_search = SEARCH_DIR in os.path.relpath(root, out).split(os.sep)
        for name in names:
            # Strategy-2 sidecars (*.osm_ids) stay uncompressed: they're cached
            # only and the next build's IdAllocator reads them directly.
            if name.endswith(".bin") or name in JSON_FILES or (in_search and not name.endswith(".zst")):
                found.append(os.path.join(root, name))
    return sorted(found, key=lambda p: (-os.path.getsize(p), p))


def sha256(path):
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            sha.update(chunk)
    return sha.hexdigest()


def threads_for(size):
    return max(1, min(MAX_THREADS_PER_FILE, size // BYTES_PER_THREAD))


def compress(path):
    subprocess.run(["zstd", LEVEL, f"-T{threads_for(os.path.getsize(path))}", "-f", "--quiet", path], check=True)


class Unit:
    """A transport unit a dir publishes: its path, kind and files, relative to the output dir."""

    def __init__(self, path, kind, files):
        self.path, self.kind, self.files = path, kind, files


def units_of(cfg, rels):
    """The units of configurations.json's transport table that the output's dirs hold all the
    files of, dir by dir."""
    held = set(rels)
    units = []
    for d in sorted({os.path.dirname(r) for r in rels}):
        for u in cfg.get("transport", {}).get("units", []):
            files = [os.path.join(d, f) for f in u["files"]]
            if all(f in held for f in files):
                units.append(Unit(os.path.join(d, u["unit"]), u["kind"], files))
    return units


def component_sizes(cfg, files, units):
    """Per region, each component's raw and wire bytes (the module's doc), from configurations.json's
    axes and components and the build's `files` and `units` tables."""
    out = {}
    for region in cfg.get("axes", {}).get("region", {}).get("values", {}):
        sizes = {}
        for cid, component in cfg.get("components", {}).items():
            paths = [p.replace("{region}", region) for p in component.get("files", []) + component.get("optional_files", [])]
            held = [p for p in dict.fromkeys(paths) if p in files]
            if not held:
                continue
            raw = sum(files[p]["size_raw"] for p in held)
            wire = sum(files[p]["size_zst"] for p in held if "unit" not in files[p])
            for u in dict.fromkeys(files[p]["unit"] for p in held if "unit" in files[p]):
                mine = sum(files[p]["size_raw"] for p in held if files[p].get("unit") == u)
                whole = sum(files[p]["size_raw"] for p in units[u]["files"])
                wire += units[u]["size"] if mine == whole else round(units[u]["size"] * mine / whole)
            sizes[cid] = {"raw": raw, "wire": wire}
        if sizes:
            out[region] = dict(sorted(sizes.items()))
    return out


def run_logged(cmd, **kw):
    """cmd's exit status and what it wrote to stderr, or the reason it couldn't start."""
    with tempfile.TemporaryFile() as err:
        try:
            status = subprocess.run(cmd, stderr=err, **kw).returncode
        except OSError as e:
            return 1, str(e)
        err.seek(0)
        return status, err.read().decode(errors="replace").strip()


def encode_unit(transport, out, unit):
    """Writes the unit: the stream `geocoder-transport encode` gives of its files, through zstd,
    then checks it decodes to them. What failed, or None; a unit that failed is removed."""
    path = os.path.join(out, unit.path)
    d = os.path.dirname(path)
    size = sum(os.path.getsize(os.path.join(out, f)) for f in unit.files)
    cmd = [transport, "encode", unit.kind, d] + ([os.path.basename(unit.files[0])] if unit.kind == "cols" else [])
    why = None
    with tempfile.TemporaryFile() as err:
        try:
            enc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=err)
        except OSError as e:
            why = f"encode: {e}"
        else:
            zstd, zstd_err = run_logged(["zstd", LEVEL, f"-T{threads_for(size)}", "-f", "--quiet", "-o", path], stdin=enc.stdout)
            enc.stdout.close()
            if enc.wait() != 0:
                err.seek(0)
                why = f"encode: {err.read().decode(errors='replace').strip() or f'exit status {enc.returncode}'}"
            elif zstd != 0:
                why = f"zstd: {zstd_err or f'exit status {zstd}'}"
    if why is None:
        status, verify_err = run_logged([transport, "verify", path, d], stdout=subprocess.DEVNULL)
        if status != 0:
            why = f"verify: {verify_err or f'exit status {status}'}"
    if why is not None and os.path.exists(path):
        os.remove(path)
    return why


def link(source, path):
    if os.path.exists(path):
        os.remove(path)
    os.link(source, path)


class Uploader:
    """Feeds `cp <file> <dest>/<path>` lines to a running `s5cmd run`."""

    def __init__(self, out, dest, cmd):
        self.out, self.dest = out, dest.rstrip("/")
        self.proc = subprocess.Popen(shlex.split(cmd), stdin=subprocess.PIPE, text=True) if dest else None

    def send(self, path):
        if self.proc:
            self.proc.stdin.write(f"cp {path} {self.dest}/{os.path.relpath(path, self.out)}\n")
            self.proc.stdin.flush()

    def finish(self):
        if not self.proc:
            return
        self.proc.stdin.close()
        if self.proc.wait() != 0:
            print("Warning: some streamed uploads failed; the caller re-uploads them", flush=True)


def main():
    args = argparse.ArgumentParser()
    args.add_argument("out")
    args.add_argument("--transport-bin", required=True, help="geocoder-transport, which encodes and verifies the units")
    args.add_argument("--upload", default="")
    args.add_argument("--upload-cmd", default="s5cmd run")
    opts = args.parse_args()
    out = opts.out
    cpath = os.path.join(out, "configurations.json")
    with open(cpath) as f:
        cfg = json.load(f)
    at = lambda rel: os.path.join(out, rel)  # noqa: E731

    rels = [os.path.relpath(p, out) for p in index_files(out)]
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as ex:
        hashes = dict(zip(rels, ex.map(sha256, map(at, rels))))
    units = units_of(cfg, rels)
    # A unit of files alike (names and contents) to an earlier unit's is that unit, linked.
    key = {u.path: (u.kind, tuple((os.path.basename(f), hashes[f]) for f in u.files)) for u in units}
    first_unit = {}
    for u in units:
        first_unit.setdefault(key[u.path], u)
    in_unit = {f for u in units for f in u.files}
    # Variants share byte-identical files (e.g. full and no-addresses have
    # the same street files, ~18% of a planet build): compress each content
    # once and hard-link the copies' .zst to it.
    first = {}
    plain = [r for r in rels if r not in in_unit]
    for r in plain:
        first.setdefault(hashes[r], r)
    distinct = [r for r in plain if first[hashes[r]] == r]
    distinct_units = [u for u in units if first_unit[key[u.path]] is u]
    print(f"Compressing {len(distinct)} distinct of {len(plain)} files and {len(distinct_units)} distinct of "
          f"{len(units)} transport units with zstd {LEVEL} on {os.cpu_count()} workers...", flush=True)
    work = [(os.path.getsize(at(r)), r, None) for r in distinct]
    work += [(sum(os.path.getsize(at(f)) for f in u.files), u.path, u) for u in distinct_units]
    work.sort(key=lambda w: -w[0])
    failed = set()
    uploader = Uploader(out, opts.upload, opts.upload_cmd)
    try:
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as ex:
            running = {(ex.submit(encode_unit, opts.transport_bin, out, u) if u else ex.submit(compress, at(r))): (r, u)
                       for _, r, u in work}
            for done in as_completed(running):
                r, u = running[done]
                why = done.result()
                if u is None:
                    uploader.send(at(r) + ".zst")
                elif why is None:
                    uploader.send(at(u.path))
                else:
                    print(f"  {u.path}: {why}; its files go as .zst", flush=True)
                    failed.add(key[u.path])
            # The files of a unit that failed go as .zst, alike ones once.
            for u in units:
                if key[u.path] in failed:
                    plain += u.files
            compressed = set(distinct)
            more = [r for r in plain if first.setdefault(hashes[r], r) == r and r not in compressed]
            for done in as_completed({ex.submit(compress, at(r)): r for r in more}):
                done.result()
        for r in more:
            uploader.send(at(r) + ".zst")
        for r in plain:
            source = first[hashes[r]]
            if source != r:
                link(at(source) + ".zst", at(r) + ".zst")
                uploader.send(at(r) + ".zst")
        for u in units:
            if key[u.path] in failed:
                if os.path.exists(at(u.path)):
                    os.remove(at(u.path))
                continue
            source = first_unit[key[u.path]]
            if source is not u:
                link(at(source.path), at(u.path))
                uploader.send(at(u.path))
            # A unit is its files' only form: a .zst an earlier run left goes.
            for f in u.files:
                if os.path.exists(at(f) + ".zst"):
                    os.remove(at(f) + ".zst")
    finally:
        uploader.finish()

    published = [u for u in units if key[u.path] not in failed]
    unit_of = {f: u for u in published for f in u.files}
    files = {}
    for r in rels:
        if r in unit_of:
            files[r] = {"size_raw": os.path.getsize(at(r)), "sha256": hashes[r], "unit": unit_of[r].path}
        else:
            files[r] = {"size_zst": os.path.getsize(at(r) + ".zst"), "size_raw": os.path.getsize(at(r)), "sha256": hashes[r]}
            pct = 100 * files[r]["size_zst"] / files[r]["size_raw"] if files[r]["size_raw"] else 0
            print(f"  {r}: {files[r]['size_raw'] / 1e6:.0f} -> {files[r]['size_zst'] / 1e6:.0f} MB ({pct:.0f}%)")
    version = cfg.get("transport", {}).get("version")
    table = {}
    for u in published:
        table[u.path] = {"kind": u.kind, "transport_version": version, "size": os.path.getsize(at(u.path)), "files": u.files}
        raw = sum(files[f]["size_raw"] for f in u.files)
        pct = 100 * table[u.path]["size"] / raw if raw else 0
        print(f"  {u.path}: {raw / 1e6:.0f} -> {table[u.path]['size'] / 1e6:.0f} MB ({pct:.0f}%)")

    # Stable ordering for deterministic diffs.
    cfg["files"] = dict(sorted(files.items()))
    cfg["units"] = dict(sorted(table.items()))
    cfg["component_sizes"] = component_sizes(cfg, cfg["files"], cfg["units"])
    with open(cpath, "w") as f:
        json.dump(cfg, f, indent=2)

    total_raw = sum(e["size_raw"] for e in files.values())
    total_zst = sum(e.get("size_zst", 0) for e in files.values()) + sum(e["size"] for e in table.values())
    if total_raw:
        print(f"Total: {total_raw / 1e9:.2f} GB raw -> {total_zst / 1e9:.2f} GB compressed ({100 * total_zst / total_raw:.0f}%)")
    if failed:
        print(f"{len(failed)} transport unit(s) failed; their files went as .zst", flush=True)
        return UNIT_FAILED
    return 0


if __name__ == "__main__":
    sys.exit(main())
