#!/usr/bin/env python3
"""Compresses a build's index files for publishing.

Usage: compress_index.py <output dir> [--while-pid PID]
                         [--upload DEST --upload-cmd CMD [--upload-after FILE]]

Writes <file>.zst next to every .bin file and load sidecar under the output
dir (files with identical content share one .zst through hard links, made in
<output dir>.zst-store while it runs) and
records each one's raw size, compressed size and raw sha256 in
configurations.json["files"]. The .bin files stay: patch generation may still
be reading them, so the caller removes them.

--while-pid starts while process PID (the builder) is still writing the
output: a file is compressed once it has kept its size and mtime over a scan
and PID neither holds it open nor maps it. The builder's write phase is
disk-bound on the nightly runner and leaves the cores idle, so most of the
compression is done by the time it exits. Then every file is checked again
and any that changed since, or that was missed, is compressed as it is now:
what ships is always the final bytes.

--upload streams each .zst to DEST/<path>.zst as soon as it exists, through
one long-running CMD (an `s5cmd ... run` reading cp commands on stdin): the
upload is network-bound and compression CPU-bound, so they overlap instead of
the upload waiting for the last file. A .zst compressed again after its
upload goes up once more when the stream is done. Upload failures only warn;
the caller re-uploads whatever is missing. --upload-after holds the stream
until FILE exists (or an hour has passed once compression is done): the
nightly's download of the previous day's index, which patch generation
waits for, has the runner's link to itself until then.

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
import argparse
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait

LEVEL = "-18"
BYTES_PER_THREAD = 256 << 20
MAX_THREADS_PER_FILE = 16
# How often --while-pid looks for finished files.
SCAN_SECONDS = 10
# Final passes before giving up on files that keep changing.
MAX_SETTLE_PASSES = 3
# How long a finished compression waits for --upload-after's file before
# uploading anyway (the patch step's own wait for the previous index).
UPLOAD_GATE_SECONDS = 3600

# JSON sidecars the server's Index::load reads alongside the .bin files. They
# go into configurations.files like any index file so the region downloader
# can fetch them; the raw copy stays for clients that read it directly
# (test-portal).
LOAD_SIDECARS = {"strings_layout.json", "poi_meta.json"}


class Changed(Exception):
    """The file changed while it was compressed."""


def index_files(out):
    found = []
    for root, _, names in os.walk(out):
        for name in names:
            # Strategy-2 sidecars (*.osm_ids) stay uncompressed: they're cached
            # only and the next build's IdAllocator reads them directly.
            if name.endswith(".bin") or name in LOAD_SIDECARS:
                found.append(os.path.join(root, name))
    return found


def largest_first(paths):
    return sorted(paths, key=lambda p: (-os.path.getsize(p), p))


def snapshot(path):
    """What changes whenever the builder writes the file again."""
    st = os.stat(path)
    return st.st_ino, st.st_size, st.st_mtime_ns


def sha256(path):
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            sha.update(chunk)
    return sha.hexdigest()


def compress(path, dest):
    threads = max(1, min(MAX_THREADS_PER_FILE, os.path.getsize(path) // BYTES_PER_THREAD))
    subprocess.run(["zstd", LEVEL, f"-T{threads}", "--quiet", "-f", path, "-o", dest], check=True)


def remove(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def running(pid):
    """Whether pid is alive (a zombie has exited)."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] != "Z"
    except (OSError, IndexError):
        return False


def held_by(pid):
    """The files pid holds open or maps."""
    held = set()
    try:
        for fd in os.listdir(f"/proc/{pid}/fd"):
            try:
                held.add(os.readlink(f"/proc/{pid}/fd/{fd}"))
            except OSError:
                pass
        with open(f"/proc/{pid}/maps") as f:
            for line in f:
                fields = line.split(None, 5)
                if len(fields) == 6 and fields[5].startswith("/"):
                    held.add(fields[5].rstrip("\n"))
    except OSError:
        pass
    return held


class Uploader:
    """Feeds `cp <file> <dest>/<path>` lines to a running `s5cmd run`, held
    back until the file `after` exists, if one is given."""

    def __init__(self, out, dest, cmd, after=""):
        self.out, self.dest, self.after = out, dest.rstrip("/"), after
        self.proc = subprocess.Popen(shlex.split(cmd), stdin=subprocess.PIPE, text=True) if dest else None
        self.held = []

    def send(self, path):
        self.command(f"cp {path} {self.dest}/{os.path.relpath(path, self.out)}")

    def delete(self, path):
        self.command(f"rm {self.dest}/{os.path.relpath(path, self.out)}")

    def command(self, line):
        if self.proc:
            self.held.append(line)
            self.release()

    def gate_open(self):
        return not self.after or os.path.exists(self.after)

    def release(self, force=False):
        """Sends the held commands once the gate is open."""
        if not self.held or not (force or self.gate_open()):
            return
        self.proc.stdin.write("".join(line + "\n" for line in self.held))
        self.proc.stdin.flush()
        self.held = []

    def finish(self):
        if not self.proc:
            return
        deadline = time.time() + UPLOAD_GATE_SECONDS
        while not self.gate_open() and time.time() < deadline:
            time.sleep(1)
        self.release(force=True)
        self.proc.stdin.close()
        if self.proc.wait() != 0:
            print("Warning: some streamed uploads failed; the caller re-uploads them", flush=True)


class Compressor:
    """Compresses each content once, into a store entry named by its sha256
    that never changes; each file's .zst is a hard link to its entry, so a
    file compressed again never alters another's."""

    def __init__(self, out, uploader):
        self.out, self.uploader = out, uploader
        self.store = os.path.realpath(out) + ".zst-store"
        shutil.rmtree(self.store, ignore_errors=True)
        os.makedirs(self.store)
        self.pool = ThreadPoolExecutor(max_workers=os.cpu_count())
        self.lock = threading.Lock()
        self.entries = {}  # sha256 -> Future of its store entry
        self.jobs = {}  # path -> Future of (snapshot, sha256, entry)
        self.linking = {}  # path -> (snapshot, sha256, entry): hashed, entry pending
        self.done = {}  # path -> (snapshot, sha256) of its .zst
        self.sent = set()  # paths whose .zst went to the uploader
        self.resend = set()  # sent, then compressed again

    def start(self, paths):
        for path in largest_first(p for p in paths if p not in self.jobs and p not in self.linking):
            self.jobs[path] = self.pool.submit(self.job, path)

    def job(self, path):
        snap = snapshot(path)
        digest = sha256(path)
        with self.lock:
            entry = self.entries.get(digest)
            mine = entry is None
            if mine:
                entry = self.entries[digest] = Future()
        if mine:
            dest = os.path.join(self.store, digest + ".zst")
            try:
                compress(path, dest)
                if snapshot(path) != snap or sha256(path) != digest:
                    raise Changed(path)
            except BaseException as e:
                remove(dest)
                with self.lock:
                    del self.entries[digest]
                entry.set_exception(e)
                raise
            entry.set_result(dest)
        return snap, digest, entry

    def collect(self, block, tolerate):
        """Links and records the files whose entries are ready, streaming
        their .zst up. tolerate: a failure only drops its file, for a later
        pass to redo."""
        if block:
            wait(list(self.jobs.values()))
        for path, job in list(self.jobs.items()):
            if not job.done():
                continue
            del self.jobs[path]
            try:
                self.linking[path] = job.result()
            except Exception as e:
                self.drop(path, e, tolerate)
        if block:
            wait([entry for _, _, entry in self.linking.values()])
        for path, (snap, digest, entry) in list(self.linking.items()):
            if not entry.done():
                continue
            del self.linking[path]
            try:
                source = entry.result()
                remove(path + ".zst")
                os.link(source, path + ".zst")
                if snapshot(path) != snap:
                    raise Changed(path)
            except Exception as e:
                self.drop(path, e, tolerate)
                continue
            self.done[path] = (snap, digest)
            if path in self.sent:
                self.resend.add(path)
            self.sent.add(path)
            self.uploader.send(path + ".zst")

    def drop(self, path, error, tolerate):
        self.done.pop(path, None)
        if not tolerate and not isinstance(error, Changed):
            raise error

    def stale(self, path):
        try:
            return self.done.get(path, (None,))[0] != snapshot(path)
        except FileNotFoundError:
            return False

    def follow(self, pid):
        """Compresses the files the builder has finished while it runs."""
        previous = {}
        while running(pid):
            held = held_by(pid)
            scan, ready = {}, []
            for path in index_files(self.out):
                try:
                    scan[path] = snapshot(path)
                except FileNotFoundError:
                    continue
                if previous.get(path) == scan[path] and self.stale(path) and os.path.realpath(path) not in held:
                    ready.append(path)
            previous = scan
            self.start(ready)
            self.collect(block=False, tolerate=True)
            self.uploader.release()
            time.sleep(SCAN_SECONDS)
        self.collect(block=True, tolerate=True)
        print(f"{len(self.done)} files compressed while the builder ran", flush=True)

    def settle(self):
        """Brings every file's .zst up to its current bytes."""
        for _ in range(MAX_SETTLE_PASSES):
            todo = [p for p in index_files(self.out) if self.stale(p)]
            if not todo:
                break
            print(f"Compressing {len(todo)} files with zstd {LEVEL} on {os.cpu_count()} workers...", flush=True)
            self.start(todo)
            self.collect(block=True, tolerate=False)
        else:
            raise RuntimeError("index files kept changing while they were compressed")
        live = set(index_files(self.out))
        for path in list(self.done):
            if path not in live:
                del self.done[path]
        for root, _, names in os.walk(self.out):
            for name in names:
                zst = os.path.join(root, name)
                if zst.endswith(".zst") and zst[:-4] not in live:
                    remove(zst)
                    self.uploader.delete(zst)
        # Every .zst holds its own link to the entry.
        shutil.rmtree(self.store)


def main():
    args = argparse.ArgumentParser()
    args.add_argument("out")
    args.add_argument("--while-pid", type=int, default=0)
    args.add_argument("--upload", default="")
    args.add_argument("--upload-cmd", default="s5cmd run")
    args.add_argument("--upload-after", default="")
    opts = args.parse_args()
    out = opts.out

    uploader = Uploader(out, opts.upload, opts.upload_cmd, opts.upload_after)
    compressor = Compressor(out, uploader)
    try:
        if opts.while_pid:
            compressor.follow(opts.while_pid)
        compressor.settle()
    finally:
        compressor.pool.shutdown()
        uploader.finish()
    if compressor.resend:
        # After the first stream: two uploads of one key could land in any order.
        again = Uploader(out, opts.upload, opts.upload_cmd, opts.upload_after)
        for path in sorted(compressor.resend):
            again.send(path + ".zst")
        again.finish()

    files = {}
    for path, (_, digest) in sorted(compressor.done.items()):
        rel = os.path.relpath(path, out)
        files[rel] = {"size_zst": os.path.getsize(path + ".zst"), "size_raw": os.path.getsize(path), "sha256": digest}
        pct = 100 * files[rel]["size_zst"] / files[rel]["size_raw"] if files[rel]["size_raw"] else 0
        print(f"  {rel}: {files[rel]['size_raw'] / 1e6:.0f} -> {files[rel]['size_zst'] / 1e6:.0f} MB ({pct:.0f}%)")

    # Read last: the caller may write it while the builder still runs.
    cpath = os.path.join(out, "configurations.json")
    with open(cpath) as f:
        cfg = json.load(f)
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
