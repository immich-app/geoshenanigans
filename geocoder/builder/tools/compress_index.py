#!/usr/bin/env python3
"""Compresses a build's index files for publishing.

Usage: compress_index.py <output dir> --transport-bin PATH [--upload DEST --upload-cmd CMD [--upload-after FILE]]
           [--late-search STATUS [--late-search-timeout SECONDS] [--late-search-taken FILE]]

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

--upload streams each .zst and unit to DEST/<path> as soon as it exists,
through one long-running CMD (an `s5cmd ... run` reading cp commands on
stdin): the upload is network-bound and compression CPU-bound, so they
overlap instead of the upload waiting for the last file. Upload failures,
a CMD that ends before it has read every command among them, only warn; the
caller re-uploads whatever is missing.
--upload-after FILE holds the upload back until FILE exists, the caller's
word that DEST may be written: compression goes on, the commands wait (FILE
is looked for every half second), and the run's end waits for FILE, so that
every file still goes up (a run that fails sends none of them). The
nightly's patch step compresses before yesterday's files are down, and gives
it once they are and DEST is cleared: until then a build published under
DEST stays as it is.

--late-search: the place search indexes (every file under a region's
search/ dir) are still being built beside this run. Everything else is
compressed first; the search files are taken once STATUS (the place search
job's exit status, written last) exists, whatever it says: a failed index
leaves no files. STATUS is looked for every half second and as items end;
the first look past --late-search-timeout seconds is the last: a STATUS there
then is taken, and none leaves the search files out, with a warning.
configurations.json is written last either way, so the place search reads it
as the build left it, and its tables are those of a run that had every file
from the start.
--late-search-taken FILE gets the status the search files were taken on, or
"none" when they were left out: the caller goes by it, since a status the
place search writes after the wait came too late for its files.

zstd level 18 compresses our structured binary data to within 0.3% of
level 19 (itself within ~2% of --ultra -22) at 21% less CPU. Level 17 costs
about 0.9% over a planet build (the big classes under 1%, a few small ones
up to +93%: way_postcodes, poi_cells, admin_cells) for 12% less CPU. It runs
the zstd first on PATH; the nightly puts 1.5.7 there (tools/build_zstd.sh),
which at -18 is smaller and faster than Ubuntu 24.04's 1.5.5.

Scheduling: zstd -18 cuts a file into 32 MiB jobs (4 x its 8 MiB window)
that its threads take in turn, and its output is the same at any -T >= 1, a
unit's stream through the pipe included. So each file gets a thread per job,
at most 8 (more leave most of a big file's threads idle on its last jobs),
and the files share a budget of one thread per CPU (cpu_budget.sh: the
CPUs this process may run on, or fewer under its cgroup's CPU quota, the
runner's 64 of its 88): they take their threads in submission order,
largest first, each once its threads are free. On the build-24 planet tree
(64 CPUs) this took 310 s against 417 s for a thread per 256 MiB on one
worker a CPU, whose tail was one 427 MB file on one thread, with half the
memory; and every output byte was the same. Each item prints its start and
end, in seconds from the run's start, and its threads.

A child that stalls (CHILD_SECONDS) is killed and fails its item: a unit's
goes as .zst, and a file's ends the run with the error, for the caller to
compress again, rather than holding its threads, and every item behind them,
until the job's timeout. An error that ends the run ends it once the children
still running end: no item still waiting for its threads starts one.
"""
import argparse
import hashlib
import json
import math
import os
import shlex
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import FIRST_COMPLETED, CancelledError, ThreadPoolExecutor, wait
from itertools import count

LEVEL = "-18"
BYTES_PER_THREAD = 32 << 20
MAX_THREADS_PER_FILE = 8

# A child (a file's zstd, a unit's encode and its zstd, its verify) still running past CHILD_SECONDS and a second
# per CHILD_BYTES_PER_SECOND of its item has stalled: it is killed and waited for, and its item fails. Far past a
# slow night's: at nice 10 beside the patch jobs a zstd thread can get a tenth of a core, a few hundred KB/s at
# -18, and an item has a thread per 32 MiB, up to 8.
CHILD_SECONDS = 900
CHILD_BYTES_PER_SECOND = 1 << 20

# The exit status when a transport unit failed and its files went as .zst.
UNIT_FAILED = 3

# Prints the CPUs the nightly's tools size their threads by.
CPU_BUDGET = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cpu_budget.sh")

# JSON sidecars the server's Index::load reads alongside the .bin files. They
# go into configurations.files like any index file so the region downloader
# can fetch them; the raw copy stays for clients that read it directly
# (test-portal).
LOAD_SIDECARS = {"strings_layout.json", "poi_meta.json"}

# Every file under a region's search/ dir is a place search index file
# (build_search_indexes.py), published like the .bin files.
SEARCH_DIR = "search"

# How often a late search's status, and the upload's --upload-after file, are looked for.
LATE_SEARCH_POLL_SECONDS = 0.5


def index_files(out, search=None):
    """The files to publish, largest first: all of them, or (search True/False) only those under / outside the
    search dirs."""
    found = []
    for root, _, names in os.walk(out):
        in_search = SEARCH_DIR in os.path.relpath(root, out).split(os.sep)
        if search is not None and in_search != search:
            continue
        for name in names:
            # Strategy-2 sidecars (*.osm_ids) stay uncompressed: they're cached
            # only and the next build's IdAllocator reads them directly.
            if name.endswith(".bin") or name in LOAD_SIDECARS or (in_search and not name.endswith(".zst")):
                found.append(os.path.join(root, name))
    return sorted(found, key=lambda p: (-os.path.getsize(p), p))


def sha256(path):
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            sha.update(chunk)
    return sha.hexdigest()


def cpu_budget():
    """The CPUs the zstd processes share (cpu_budget.sh)."""
    return int(subprocess.run([CPU_BUDGET], check=True, capture_output=True, text=True).stdout)


def threads_for(size, cpus):
    return max(1, min(MAX_THREADS_PER_FILE, -(-size // BYTES_PER_THREAD), cpus))


class Cpus:
    """The thread slots the zstd processes share, one a CPU. Jobs queue for theirs in submission order (a ticket
    each) and take them once they are free, so no more zstd threads run than there are CPUs and a big job is never
    passed by the smaller ones behind it. Once stopped, a job waiting for its threads, or asking for them later, gets
    CancelledError instead."""

    def __init__(self, n):
        self.free, self.turn, self.cv = n, 0, threading.Condition()
        self.numbers = count()
        self.stopped = False

    def ticket(self):
        return Ticket(self, next(self.numbers))

    def take(self, number, n):
        with self.cv:
            self.cv.wait_for(lambda: self.stopped or self.turn == number and self.free >= n)
            if self.stopped:
                raise CancelledError("the run ended before this job's turn")
            self.free -= n
            self.turn += 1
            self.cv.notify_all()

    def skip(self, number):
        with self.cv:
            self.cv.wait_for(lambda: self.stopped or self.turn == number)
            self.turn += 1
            self.cv.notify_all()

    def stop(self):
        with self.cv:
            self.stopped = True
            self.cv.notify_all()

    def give(self, n):
        with self.cv:
            self.free += n
            self.cv.notify_all()


class Ticket:
    """A job's place in the queue for thread slots, and what it holds; its start and end times and its threads."""

    def __init__(self, cpus, number):
        self.cpus, self.number = cpus, number
        self.taken, self.held, self.threads = False, 0, 0
        self.started = self.ended = None

    def take(self, n):
        self.cpus.take(self.number, n)
        self.taken, self.held, self.threads, self.started = True, n, n, time.monotonic()

    def give(self, n):
        n = min(n, self.held)
        self.held -= n
        self.cpus.give(n)

    def close(self):
        """Gives back what the job still holds and, for a job that ended before its turn (an error first), passes
        the turn on: every job behind it waits for that turn."""
        if not self.taken:
            self.taken = True
            self.cpus.skip(self.number)
        self.give(self.held)
        self.ended = time.monotonic()


def job(ticket, fn, *args):
    """fn(ticket, *args), its ticket closed however it ends."""
    try:
        return fn(ticket, *args)
    finally:
        ticket.close()


def child_seconds(size):
    """How long a child working through an item of `size` bytes runs before it counts as stalled."""
    return CHILD_SECONDS + size / CHILD_BYTES_PER_SECOND


def compress(ticket, path, threads):
    ticket.take(threads)
    # A zstd past its time is killed and waited for, and this raises: the run fails, for the caller to run again.
    subprocess.run(["zstd", LEVEL, f"-T{threads}", "-f", "--quiet", path], check=True,
                   timeout=child_seconds(os.path.getsize(path)))


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


def run_logged(cmd, **kw):
    """cmd's exit status and what it wrote to stderr, or the reason it couldn't start. Past a `timeout` it is killed
    and waited for, and subprocess.TimeoutExpired raised."""
    with tempfile.TemporaryFile() as err:
        try:
            status = subprocess.run(cmd, stderr=err, **kw).returncode
        except OSError as e:
            return 1, str(e)
        err.seek(0)
        return status, err.read().decode(errors="replace").strip()


def encode_unit(ticket, transport, out, unit, threads):
    """Writes the unit: the stream `geocoder-transport encode` gives of its files, through zstd,
    then checks it decodes to them. What failed, or None; a unit that failed is removed. The
    encode and its zstd, then the verify, have child_seconds of the unit's bytes: past it they
    are killed, and the unit fails. However this ends, neither outlives it."""
    path = os.path.join(out, unit.path)
    d = os.path.dirname(path)
    cmd = [transport, "encode", unit.kind, d] + ([os.path.basename(unit.files[0])] if unit.kind == "cols" else [])
    limit = child_seconds(sum(os.path.getsize(os.path.join(out, f)) for f in unit.files))
    stalled = f"still running after {limit:.0f}s, killed"
    why = None
    ticket.take(threads)
    with tempfile.TemporaryFile() as err:
        try:
            enc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=err)
        except OSError as e:
            why = f"encode: {e}"
        else:
            try:
                end = time.monotonic() + limit
                try:
                    zstd, zstd_err = run_logged(["zstd", LEVEL, f"-T{threads}", "-f", "--quiet", "-o", path],
                                                stdin=enc.stdout, timeout=limit)
                except subprocess.TimeoutExpired:
                    zstd = None
                # The pipe's read end is zstd's alone now: an encode still writing to it gets EPIPE.
                enc.stdout.close()
                try:
                    enc.wait(timeout=max(0.0, end - time.monotonic()))
                except subprocess.TimeoutExpired:
                    pass
                if zstd is None:
                    why = f"encode and zstd: {stalled}"
                elif enc.returncode is None:
                    why = f"encode: {stalled}"
                elif enc.returncode != 0:
                    err.seek(0)
                    why = f"encode: {err.read().decode(errors='replace').strip() or f'exit status {enc.returncode}'}"
                elif zstd != 0:
                    why = f"zstd: {zstd_err or f'exit status {zstd}'}"
            finally:
                enc.stdout.close()
                if enc.poll() is None:
                    enc.kill()
                enc.wait()
    # The verify runs on one thread. A places unit's decoder keeps a scratch file in its temp dir that only its own
    # end removes: it gets a dir of its own, removed with whatever a verify killed left in it.
    ticket.give(threads - 1)
    if why is None:
        with tempfile.TemporaryDirectory() as scratch:
            try:
                status, verify_err = run_logged([transport, "verify", path, d], stdout=subprocess.DEVNULL,
                                                timeout=limit, env={**os.environ, "TMPDIR": scratch})
            except subprocess.TimeoutExpired:
                status, verify_err = None, stalled
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
    """Feeds `cp <file> <dest>/<path>` lines to a running `s5cmd run`; with `after`, none until that file exists:
    the lines sent before then wait for it (a thread looks for it), and finish() waits for it too, but for a run that
    failed. A command that ended before it read them all fails the uploads alone."""

    def __init__(self, out, dest, cmd, after=""):
        self.out, self.dest, self.after = out, dest.rstrip("/"), after
        self.proc = subprocess.Popen(shlex.split(cmd), stdin=subprocess.PIPE, text=True) if dest else None
        self.lock, self.opened, self.broken = threading.Lock(), threading.Event(), False
        # The lines that wait for `after`; None once they may go.
        self.held = [] if self.proc and after else None
        if self.held is None:
            self.opened.set()
        else:
            threading.Thread(target=self.open_after, daemon=True).start()

    def open_after(self):
        try:
            while not os.path.exists(self.after):
                time.sleep(LATE_SEARCH_POLL_SECONDS)
            with self.lock:
                held, self.held = self.held, None
                if held is not None:
                    self.write("".join(held))
        finally:
            self.opened.set()

    def write(self, lines):
        if lines and not self.broken:
            try:
                self.proc.stdin.write(lines)
                self.proc.stdin.flush()
            except BrokenPipeError:
                self.broken = True

    def send(self, path):
        if self.proc:
            line = f"cp {path} {self.dest}/{os.path.relpath(path, self.out)}\n"
            with self.lock:
                if self.held is not None:
                    self.held.append(line)
                else:
                    self.write(line)

    def finish(self, failed=False):
        """Ends the upload once every line has gone; a run that failed sends none of those still held."""
        if not self.proc:
            return
        if failed:
            with self.lock:
                if self.held is not None:
                    self.held, self.broken = None, True
        elif not self.opened.is_set():
            print(f"Compressed: the uploads wait for {self.after}", flush=True)
            self.opened.wait()
        with self.lock:
            try:
                self.proc.stdin.close()
            except BrokenPipeError:
                self.broken = True
        if self.proc.wait() != 0 or self.broken:
            print("Warning: some streamed uploads failed; the caller re-uploads them", flush=True)


def seconds(text):
    """A wait in seconds: finite and 0 or more, since no time is past nan or inf."""
    value = float(text)
    if not math.isfinite(value) or value < 0:
        raise argparse.ArgumentTypeError(f"{text!r} is not a finite number of seconds, 0 or more")
    return value


def main():
    args = argparse.ArgumentParser()
    args.add_argument("out")
    args.add_argument("--transport-bin", required=True, help="geocoder-transport, which encodes and verifies the units")
    args.add_argument("--upload", default="")
    args.add_argument("--upload-cmd", default="s5cmd run")
    args.add_argument("--upload-after", default="", help="the upload starts once this file exists")
    args.add_argument("--late-search", default="", help="the place search job's status file: its files come late")
    args.add_argument("--late-search-timeout", type=seconds, default=3600)
    args.add_argument("--late-search-taken", default="", help="gets the status the place search files were taken on")
    opts = args.parse_args()
    out = opts.out
    cpath = os.path.join(out, "configurations.json")
    with open(cpath) as f:
        cfg = json.load(f)
    at = lambda rel: os.path.join(out, rel)  # noqa: E731
    start = time.monotonic()
    budget = cpu_budget()
    cpus = Cpus(budget)

    # Everything compressed or linked, as files come in: the files and their hashes, the units and the key a unit
    # of files alike shares, each content's and each unit key's first file or unit.
    rels, hashes, units, key, first_unit, plain, first = [], {}, [], {}, {}, [], {}
    compressed, failed = set(), set()
    running = {}

    def submit(ex, fn, rel, unit, size, *fn_args):
        ticket = cpus.ticket()
        running[ex.submit(job, ticket, fn, *fn_args, threads_for(size, budget))] = (rel, unit, ticket)

    def add(ex, new_rels, what):
        """Takes in new_rels: hashes them, then submits each content and each unit not compressed yet, largest
        first, as later tickets than any before."""
        with ThreadPoolExecutor(max_workers=budget) as hasher:
            hashes.update(zip(new_rels, hasher.map(sha256, map(at, new_rels))))
        rels.extend(new_rels)
        new_units = units_of(cfg, new_rels)
        # A unit of files alike (names and contents) to an earlier unit's is that unit, linked.
        for u in new_units:
            key[u.path] = (u.kind, tuple((os.path.basename(f), hashes[f]) for f in u.files))
            first_unit.setdefault(key[u.path], u)
        units.extend(new_units)
        in_unit = {f for u in new_units for f in u.files}
        # Variants share byte-identical files (e.g. full and no-addresses have
        # the same street files, ~18% of a planet build): compress each content
        # once and hard-link the copies' .zst to it.
        new_plain = [r for r in new_rels if r not in in_unit]
        plain.extend(new_plain)
        distinct = [r for r in new_plain if first.setdefault(hashes[r], r) == r]
        compressed.update(distinct)
        distinct_units = [u for u in new_units if first_unit[key[u.path]] is u]
        print(f"Compressing {len(distinct)} distinct of {len(new_plain)} {what} and {len(distinct_units)} distinct of "
              f"{len(new_units)} transport units with zstd {LEVEL} on {budget} threads...", flush=True)
        work = [(os.path.getsize(at(r)), r, None) for r in distinct]
        work += [(sum(os.path.getsize(at(f)) for f in u.files), u.path, u) for u in distinct_units]
        work.sort(key=lambda w: -w[0])
        for size, r, u in work:
            if u:
                submit(ex, encode_unit, r, u, size, opts.transport_bin, out, u)
            else:
                submit(ex, compress, r, None, size, at(r))

    def done(future, uploader):
        """A finished item: its line, its upload, or its unit's failure. Raises what the job raised."""
        r, u, ticket = running.pop(future)
        why = future.result()
        print(f"  [{ticket.started - start:6.1f}s -> {ticket.ended - start:6.1f}s -T{ticket.threads}] {r}", flush=True)
        if u is None:
            uploader.send(at(r) + ".zst")
        elif why is None:
            uploader.send(at(u.path))
        else:
            print(f"  {u.path}: {why}; its files go as .zst", flush=True)
            failed.add(key[u.path])

    uploader = Uploader(out, opts.upload, opts.upload_cmd, opts.upload_after)
    failed_run = True
    try:
        with ThreadPoolExecutor(max_workers=budget) as ex:
            try:
                late = opts.late_search
                add(ex, [os.path.relpath(p, out) for p in index_files(out, search=False if late else None)], "files")
                deadline = time.monotonic() + opts.late_search_timeout
                while running or late:
                    if late and (os.path.exists(late) and os.path.getsize(late) or time.monotonic() > deadline):
                        if os.path.exists(late) and os.path.getsize(late):
                            with open(late) as f:
                                status = f.read().strip()
                            print(f"Place search ended (status {status}): compressing its files", flush=True)
                            add(ex, [os.path.relpath(p, out) for p in index_files(out, search=True)],
                                "place search files")
                        else:
                            status = "none"
                            print(f"Warning: no place search status in {late} after {opts.late_search_timeout:.0f}s; "
                                  "its files are left out", flush=True)
                        if opts.late_search_taken:
                            with open(opts.late_search_taken + ".part", "w") as f:
                                f.write(status + "\n")
                            os.replace(opts.late_search_taken + ".part", opts.late_search_taken)
                        late = ""
                    if not running:
                        time.sleep(LATE_SEARCH_POLL_SECONDS)
                        continue
                    finished, _ = wait(list(running), timeout=LATE_SEARCH_POLL_SECONDS if late else None,
                                       return_when=FIRST_COMPLETED)
                    for future in finished:
                        done(future, uploader)
                # The files of a unit that failed go as .zst, alike ones once.
                for u in units:
                    if key[u.path] in failed:
                        plain += u.files
                more = [r for r in plain if first.setdefault(hashes[r], r) == r and r not in compressed]
                for r in more:
                    submit(ex, compress, r, None, os.path.getsize(at(r)), at(r))
                while running:
                    finished, _ = wait(list(running), return_when=FIRST_COMPLETED)
                    for future in finished:
                        done(future, uploader)
            except BaseException:
                # The run ends with this once the children running end: the jobs still queued, and those the
                # workers hold as they wait for their threads, start none.
                cpus.stop()
                raise
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
        failed_run = False
    finally:
        uploader.finish(failed=failed_run)

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
