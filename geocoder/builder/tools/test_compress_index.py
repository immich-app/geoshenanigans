#!/usr/bin/env python3
"""Tests for compress_index.py. Run: python3 -m unittest discover -s geocoder/builder/tools"""
import contextlib
import fnmatch
import hashlib
import io
import json
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import compress_index as sut  # noqa: E402
from test_cpu_budget import fake_cpus  # noqa: E402

TEMPLATE = os.path.join(HERE, "..", "configurations.template.json")
NIGHTLY = os.path.join(HERE, "..", "..", "..", ".github", "workflows", "geocoder-build.yml")

# zstd for a run whose threads are counted: each compression appends "<start> <end> <-T> <output>" to $ZSTD_CALLS;
# the work is the zstd's it stands in for ($REAL_ZSTD), stdin and all, a decompression's too.
COUNTING_ZSTD = r'''#!{python}
import os, subprocess, sys, time
args = sys.argv[1:]
start = time.time()
status = subprocess.run([os.environ["REAL_ZSTD"]] + args).returncode
threads = next((a for a in args if a.startswith("-T")), None)
if threads:
    output = args[args.index("-o") + 1] if "-o" in args else args[-1] + ".zst"
    with open(os.environ["ZSTD_CALLS"], "a") as f:
        f.write(f"{{start}} {{time.time()}} {{threads}} {{output}}\n")
sys.exit(status)
'''

# zstd as a run that stalls one runs it: each call appends its pid to $ZSTD_PIDS, and one whose last argument (the
# file it compresses, or the unit it writes) ends with $ZSTD_STALL appends it to $FAKE_STALLED too and sleeps; the
# others are $REAL_ZSTD's.
STALLING_ZSTD = r'''#!/bin/sh
echo $$ >> "$ZSTD_PIDS"
for last; do :; done
if [ -n "$ZSTD_STALL" ]; then
  case "$last" in *"$ZSTD_STALL") echo $$ >> "$FAKE_STALLED"; exec sleep 300 ;; esac
fi
exec "$REAL_ZSTD" "$@"
'''

# A compress_index.py run in a child process (a run that hangs is killed at its timeout rather than holding up
# the tests): 4 CPUs, a zstd thread per 64 KiB, and the change `plant` makes to the script first.
CHILD_RUN = r'''
import os, sys
sys.path.insert(0, {here!r})
import compress_index as sut
sut.cpu_budget = lambda: 4
sut.BYTES_PER_THREAD = 64 << 10
{plant}
sys.argv = ["compress_index.py", {out!r}, "--transport-bin", {transport!r}]
sys.exit(sut.main())
'''

# A compress_index.py run in a child process with the arguments `args`, as the nightly runs it.
LATE_RUN = r'''
import sys
sys.path.insert(0, {here!r})
import compress_index as sut
sys.argv = ["compress_index.py", *{args!r}]
sys.exit(sut.main())
'''

# geocoder-transport as the script runs it. encode writes the kind, the members' names and sizes
# (a JSON line), then the members' bytes; verify decodes that (through zstd) against the dir's
# files and fails for a unit whose path ends with one of $FAKE_VERIFY_FAILS (":"-separated). Each
# call is logged to $FAKE_TRANSPORT_LOG. A call $FAKE_TRANSPORT_STALL names ("<verb>:<end of its
# first argument>") stalls: it appends its pid to $FAKE_STALLED and sleeps, its output left open; a
# verify leaves a scratch file in its temp dir too, as a places unit's decoder does until it ends.
FAKE_TRANSPORT = r'''#!{python}
import json, os, subprocess, sys, tempfile
MEMBERS = {{"places": ["place_cells.bin", "place_nodes.bin", "place_hidden.bin"],
           "pois": ["poi_records.bin", "poi_vertices.bin"], "arcs": ["arc_index.bin", "arc_vertices.bin"]}}
with open(os.environ["FAKE_TRANSPORT_LOG"], "a") as f:
    f.write(" ".join(sys.argv[1:]) + "\n")
verb, _, end = os.environ.get("FAKE_TRANSPORT_STALL", "").partition(":")
if sys.argv[1] == verb and sys.argv[2].endswith(end):
    with open(os.environ["FAKE_STALLED"], "a") as f:
        f.write(f"{{os.getpid()}}\n")
    if verb == "verify":
        open(os.path.join(tempfile.gettempdir(), f".place_nodes.bin.table.{{os.getpid()}}.0"), "w").close()
    os.execvp("sleep", ["sleep", "300"])
if sys.argv[1] == "encode":
    kind, d, names = sys.argv[2], sys.argv[3], sys.argv[4:] or MEMBERS[sys.argv[2]]
    head = {{"kind": kind, "members": [[n, os.path.getsize(os.path.join(d, n))] for n in names]}}
    sys.stdout.buffer.write(json.dumps(head).encode() + b"\n")
    for n in names:
        with open(os.path.join(d, n), "rb") as f:
            sys.stdout.buffer.write(f.read())
    sys.exit(0)
unit, d = sys.argv[2], sys.argv[3]
if any(f and unit.endswith(f) for f in os.environ.get("FAKE_VERIFY_FAILS", "").split(":")):
    sys.exit("geocoder-transport: place_nodes.bin differs from the unit's at byte 7")
data = subprocess.run(["zstd", "-dc", unit], check=True, capture_output=True).stdout
line, body = data.split(b"\n", 1)
head = json.loads(line)
for name, size in head["members"]:
    with open(os.path.join(d, name), "rb") as f:
        if f.read() != body[:size]:
            sys.exit(f"geocoder-transport: {{name}} differs")
    body = body[size:]
print("verified " + head["kind"] + " unit")
'''


def alive(pid):
    """Whether process `pid` runs (a zombie counts as ended)."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


def write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


def unzstd(path):
    return subprocess.run(["zstd", "-dc", path], check=True, capture_output=True).stdout


def unit_members(path):
    """The members a unit of the fake transport holds: {name: bytes}."""
    line, body = unzstd(path).split(b"\n", 1)
    members = {}
    for name, size in json.loads(line)["members"]:
        members[name], body = body[:size], body[size:]
    return members


def content(seed, n=200_000):
    return bytes((seed * 31 + i * 7 + i // 251) % 256 for i in range(n))


def sha(data):
    return hashlib.sha256(data).hexdigest()


def load_template():
    with open(TEMPLATE) as f:
        return json.load(f)


class OutputTest(unittest.TestCase):
    """An output dir of a few regions' files, its units, and the fake transport."""

    def setUp(self):
        # The tree goes, and the env comes back, in cleanups: after those a test adds, which end its child runs.
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out = os.path.join(self.tmp.name, "output")
        os.makedirs(self.out)
        with open(os.path.join(self.out, "configurations.json"), "w") as f:
            json.dump({"version": 1, "transport": load_template()["transport"]}, f)
        self.transport = os.path.join(self.tmp.name, "geocoder-transport")
        with open(self.transport, "w") as f:
            f.write(FAKE_TRANSPORT.format(python=sys.executable))
        os.chmod(self.transport, 0o755)
        self.log = os.path.join(self.tmp.name, "transport.log")
        self.env = mock.patch.dict(os.environ, {"FAKE_TRANSPORT_LOG": self.log, "FAKE_VERIFY_FAILS": ""})
        self.env.start()
        self.addCleanup(self.env.stop)
        places = {"place_cells.bin": content(10, 3000), "place_nodes.bin": content(11), "place_hidden.bin": content(12, 500)}
        self.files = {
            "planet/full/geo_cells.bin": content(1),
            "planet/no-addresses/geo_cells.bin": content(1),
            "planet/full/strings_layout.json": b'{"tiers": []}',
            "planet/full/attribution.json": b'[{"id": "osm"}]',
            "planet/admin/attribution.json": b'[{"id": "osm"}]',
            "planet/full/addr_points.bin": content(2),
            "planet/full/street_ways.bin": content(5),
            "planet/admin/postcode_centroids.bin": content(6, 1600),
            "planet/quality/simplified/admin_polygons.bin": content(7, 1600),
            "planet/quality/exact/admin_polygons.bin": content(7, 1600),
            "planet/quality/simplified/arc_index.bin": content(8, 400),
            "planet/quality/simplified/arc_vertices.bin": content(9),
            "planet/quality/exact/arc_index.bin": content(13, 400),
            "planet/quality/exact/arc_vertices.bin": content(14),
            "planet/poi/major/poi_records.bin": content(15),
            "planet/poi/major/poi_vertices.bin": content(16),
            "planet/poi/major/poi_meta.json": b'{"categories": []}',
            "planet/search/full/simplified/all/places.fst": content(4),
            "planet/search/full/simplified/all/search_meta.json": b'{"dataset": {}}',
        }
        for d in ("planet/admin", "planet/full"):
            self.files.update({f"{d}/{name}": data for name, data in places.items()})
        for rel, data in self.files.items():
            write(os.path.join(self.out, rel), data)
        write(os.path.join(self.out, "planet/full/ways.osm_ids"), content(3))
        # Each unit and the files it holds.
        self.units = {
            "planet/admin/places.tt.zst": ("places", [f"planet/admin/{n}" for n in places]),
            "planet/full/places.tt.zst": ("places", [f"planet/full/{n}" for n in places]),
            "planet/full/street_ways.tt.zst": ("cols", ["planet/full/street_ways.bin"]),
            "planet/quality/simplified/admin_polygons.tt.zst": ("cols", ["planet/quality/simplified/admin_polygons.bin"]),
            "planet/quality/exact/admin_polygons.tt.zst": ("cols", ["planet/quality/exact/admin_polygons.bin"]),
            "planet/quality/simplified/arcs.tt.zst": ("arcs", ["planet/quality/simplified/arc_index.bin",
                                                               "planet/quality/simplified/arc_vertices.bin"]),
            "planet/quality/exact/arcs.tt.zst": ("arcs", ["planet/quality/exact/arc_index.bin",
                                                          "planet/quality/exact/arc_vertices.bin"]),
            "planet/poi/major/pois.tt.zst": ("pois", ["planet/poi/major/poi_records.bin", "planet/poi/major/poi_vertices.bin"]),
        }
        self.members = {m: u for u, (_, members) in self.units.items() for m in members}

    def path(self, rel):
        return os.path.join(self.out, rel)

    def run_main(self, *args, transport=None):
        argv = ["compress_index.py", self.out, "--transport-bin", transport or self.transport, *args]
        with mock.patch.object(sys, "argv", argv):
            self.status = sut.main()
        with open(self.path("configurations.json")) as f:
            return json.load(f)

    def transport_calls(self, verb):
        with open(self.log) as f:
            return [line.split()[1:] for line in f.read().splitlines() if line.split()[0] == verb]


class CompressIndexTest(OutputTest):
    def test_compresses_each_content_once_and_links_its_copies(self):
        cfg = self.run_main()

        self.assertEqual(self.status, 0)
        self.assertEqual(sorted(cfg["files"]), sorted(self.files))
        for rel, data in self.files.items():
            if rel in self.members:
                continue
            self.assertEqual(unzstd(self.path(rel) + ".zst"), data, rel)
            self.assertEqual(cfg["files"][rel], {"size_zst": os.path.getsize(self.path(rel) + ".zst"), "size_raw": len(data),
                                                 "sha256": sha(data)})
        full = os.stat(self.path("planet/full/geo_cells.bin.zst"))
        copy = os.stat(self.path("planet/no-addresses/geo_cells.bin.zst"))
        self.assertEqual(full.st_ino, copy.st_ino)
        self.assertFalse(os.path.exists(self.path("planet/full/ways.osm_ids.zst")))
        self.assertEqual(cfg["version"], 1)

    def test_a_dir_holding_a_units_files_publishes_them_in_the_unit_alone(self):
        cfg = self.run_main()

        self.assertEqual(sorted(cfg["units"]), sorted(self.units))
        for unit, (kind, members) in self.units.items():
            self.assertEqual(cfg["units"][unit], {"kind": kind, "transport_version": 1, "size": os.path.getsize(self.path(unit)),
                                                  "files": members}, unit)
            self.assertEqual(unit_members(self.path(unit)), {os.path.basename(m): self.files[m] for m in members}, unit)
            for m in members:
                self.assertEqual(cfg["files"][m], {"size_raw": len(self.files[m]), "sha256": sha(self.files[m]), "unit": unit}, m)
                self.assertFalse(os.path.exists(self.path(m) + ".zst"), m)

    def test_each_unit_is_encoded_with_its_kind_and_verified_against_its_dir(self):
        self.run_main()

        encoded = sorted(self.transport_calls("encode"))
        self.assertEqual(encoded, sorted([
            ["places", self.path("planet/admin")],
            ["cols", self.path("planet/full"), "street_ways.bin"],
            ["cols", self.path("planet/quality/exact"), "admin_polygons.bin"],
            ["arcs", self.path("planet/quality/simplified")],
            ["arcs", self.path("planet/quality/exact")],
            ["pois", self.path("planet/poi/major")],
        ]))
        # A unit alike to one of a dir before it, in path order, is that one's.
        linked = {"planet/full/places.tt.zst", "planet/quality/simplified/admin_polygons.tt.zst"}
        self.assertEqual(sorted(self.transport_calls("verify")),
                         sorted([self.path(u), os.path.dirname(self.path(u))] for u in self.units if u not in linked))

    def test_a_unit_of_files_another_dir_sent_alike_is_linked_to_its_unit(self):
        self.run_main()

        for unit, copy in [("planet/admin/places.tt.zst", "planet/full/places.tt.zst"),
                           ("planet/quality/simplified/admin_polygons.tt.zst", "planet/quality/exact/admin_polygons.tt.zst")]:
            self.assertEqual(os.stat(self.path(unit)).st_ino, os.stat(self.path(copy)).st_ino, copy)

    def test_a_dir_lacking_one_of_a_units_files_publishes_the_others_as_zst(self):
        os.remove(self.path("planet/full/place_hidden.bin"))
        del self.files["planet/full/place_hidden.bin"]

        cfg = self.run_main()

        self.assertNotIn("planet/full/places.tt.zst", cfg["units"])
        self.assertFalse(os.path.exists(self.path("planet/full/places.tt.zst")))
        for name in ("place_cells.bin", "place_nodes.bin"):
            rel = f"planet/full/{name}"
            self.assertEqual(unzstd(self.path(rel) + ".zst"), self.files[rel])
            self.assertNotIn("unit", cfg["files"][rel])

    def test_a_unit_that_fails_its_verify_publishes_its_files_as_zst_and_fails_the_run(self):
        os.environ["FAKE_VERIFY_FAILS"] = "planet/admin/places.tt.zst:planet/poi/major/pois.tt.zst"

        cfg = self.run_main()

        self.assertEqual(self.status, sut.UNIT_FAILED)
        failed = ["planet/admin/places.tt.zst", "planet/full/places.tt.zst", "planet/poi/major/pois.tt.zst"]
        self.assertEqual(sorted(cfg["units"]), sorted(u for u in self.units if u not in failed))
        for unit in failed:
            self.assertFalse(os.path.exists(self.path(unit)), unit)
            for m in self.units[unit][1]:
                self.assertEqual(unzstd(self.path(m) + ".zst"), self.files[m], m)
                self.assertEqual(cfg["files"][m], {"size_zst": os.path.getsize(self.path(m) + ".zst"), "size_raw": len(self.files[m]),
                                                   "sha256": sha(self.files[m])}, m)
        # Files alike go as one .zst, as any others do.
        self.assertEqual(os.stat(self.path("planet/admin/place_nodes.bin.zst")).st_ino,
                         os.stat(self.path("planet/full/place_nodes.bin.zst")).st_ino)
        self.assertTrue(os.path.exists(self.path("planet/quality/exact/arcs.tt.zst")))

    def test_without_its_transport_cli_every_unit_falls_back_to_zst_and_fails_the_run(self):
        cfg = self.run_main(transport=os.path.join(self.tmp.name, "missing", "geocoder-transport"))

        self.assertEqual(self.status, sut.UNIT_FAILED)
        self.assertEqual(cfg["units"], {})
        for rel, data in self.files.items():
            self.assertEqual(unzstd(self.path(rel) + ".zst"), data, rel)
            self.assertIn("size_zst", cfg["files"][rel], rel)

    def test_a_unit_takes_the_place_of_its_files_zst_left_by_an_earlier_run(self):
        os.environ["FAKE_VERIFY_FAILS"] = "planet/poi/major/pois.tt.zst"
        self.run_main()
        os.environ["FAKE_VERIFY_FAILS"] = ""

        cfg = self.run_main()

        self.assertEqual(self.status, 0)
        self.assertIn("planet/poi/major/pois.tt.zst", cfg["units"])
        for m in self.units["planet/poi/major/pois.tt.zst"][1]:
            self.assertFalse(os.path.exists(self.path(m) + ".zst"), m)

    def test_streams_every_zst_and_unit_to_the_upload_command(self):
        log = os.path.join(self.tmp.name, "upload.log")
        recorder = f"{sys.executable} -c \"import sys; open({log!r}, 'a').write(sys.stdin.read())\""

        self.run_main("--upload", "s3://bucket/day/", "--upload-cmd", recorder)

        with open(log) as f:
            sent = f.read().splitlines()
        sent_rel = [f"{rel}.zst" for rel in self.files if rel not in self.members] + list(self.units)
        expected = [f"cp {self.path(rel)} s3://bucket/day/{rel}" for rel in sent_rel]
        self.assertEqual(sorted(sent), sorted(expected))

    def test_a_water_dirs_json_files_are_published_with_its_index_files(self):
        water = {"planet/water/all/exact/water_meta.json": b'{"types": []}',
                 "planet/water/all/exact/attribution.json": b'[{"id": "osm"}]',
                 "planet/water/all/exact/strings_layout.json": b'{"tiers": []}',
                 "planet/water/all/exact/admin_cells.bin": content(20, 900)}
        for rel, data in water.items():
            write(self.path(rel), data)
        write(self.path("planet/water/all/exact/admin_polygons.fallback.osm_ids"), content(21, 64))
        log = os.path.join(self.tmp.name, "upload.log")
        recorder = f"{sys.executable} -c \"import sys; open({log!r}, 'a').write(sys.stdin.read())\""

        cfg = self.run_main("--upload", "s3://bucket/day/", "--upload-cmd", recorder)

        for rel, data in water.items():
            self.assertEqual(unzstd(self.path(rel) + ".zst"), data, rel)
            self.assertEqual(cfg["files"][rel]["sha256"], sha(data), rel)
        self.assertNotIn("planet/water/all/exact/admin_polygons.fallback.osm_ids", cfg["files"])
        # And each goes up with the rest.
        with open(log) as f:
            sent = f.read().splitlines()
        for rel in water:
            self.assertIn(f"cp {self.path(rel)}.zst s3://bucket/day/{rel}.zst", sent, rel)
        self.assertFalse([line for line in sent if "fallback.osm_ids" in line], sent)

    # The nightly's patch step compresses before it may write the date's prefix (yesterday's files not down yet):
    # with --upload-after, compression goes on, its uploads wait until the file exists, and the run's end waits for
    # them, so every file still goes up once it does.
    def test_the_uploads_wait_for_their_file_and_the_run_for_them(self):
        log = os.path.join(self.tmp.name, "upload.log")
        recorder = os.path.join(self.tmp.name, "recorder.py")
        with open(recorder, "w") as f:
            f.write(f"import sys\nfor line in sys.stdin:\n    with open({log!r}, 'a') as f:\n        f.write(line)\n")
        after = os.path.join(self.tmp.name, "prefix.cleared")
        sent_rel = [f"{rel}.zst" for rel in self.files if rel not in self.members] + list(self.units)

        child = self.start_late_run("--upload", "s3://bucket/day/", "--upload-cmd", f"{sys.executable} {recorder}",
                                    "--upload-after", after)
        printed = os.path.join(self.tmp.name, "late-run.out")
        deadline = time.time() + 60
        while time.time() < deadline and child.poll() is None and not (
                all(os.path.exists(self.path(r)) for r in sent_rel) and "wait for" in open(printed).read()):
            time.sleep(0.05)
        time.sleep(0.5)
        self.assertIsNone(child.poll())
        self.assertEqual([r for r in sent_rel if not os.path.exists(self.path(r))], [])
        self.assertFalse(os.path.exists(log), "an upload went before its file")
        open(after, "w").close()
        code, _, _ = self.late_run_result(child)

        self.assertEqual(code, 0)
        with open(log) as f:
            sent = f.read().splitlines()
        self.assertEqual(sorted(sent), sorted(f"cp {self.path(rel)} s3://bucket/day/{rel}" for rel in sent_rel))

    # An upload command that ends before it has read its commands (with --upload-after: before the file exists)
    # fails the uploads alone: the run still ends well, with its warning, for the caller to send what is missing.
    def test_an_upload_command_that_ends_first_only_warns(self):
        after = os.path.join(self.tmp.name, "prefix.cleared")
        for gated in (False, True):
            with self.subTest(gated=gated):
                for rel in list(self.zst_files()):
                    os.remove(self.path(rel))
                if os.path.exists(after):
                    os.remove(after)
                args = ["--upload", "s3://bucket/day/", "--upload-cmd", f"{sys.executable} -c \"import sys; sys.exit(1)\""]
                child = self.start_late_run(*args, *(["--upload-after", after] if gated else []))
                if gated:
                    printed = os.path.join(self.tmp.name, "late-run.out")
                    deadline = time.time() + 60
                    while time.time() < deadline and child.poll() is None and "wait for" not in open(printed).read():
                        time.sleep(0.05)
                    open(after, "w").close()
                code, cfg, printed = self.late_run_result(child)

                self.assertEqual(code, 0, printed)
                self.assertIn("Warning: some streamed uploads failed", printed)
                self.assertEqual(sorted(cfg["files"]), sorted(self.files))

    # A run that fails (its zstd here) does not wait for the file: what it held back is left for the caller.
    def test_a_run_that_fails_does_not_wait_for_its_uploads(self):
        after = os.path.join(self.tmp.name, "prefix.cleared")
        log = os.path.join(self.tmp.name, "upload.log")
        recorder = f"{sys.executable} -c \"import sys; open({log!r}, 'a').write(sys.stdin.read())\""
        bin_dir = os.path.join(self.tmp.name, "bin")
        write(os.path.join(bin_dir, "zstd"), b"#!/bin/sh\nexit 1\n")
        os.chmod(os.path.join(bin_dir, "zstd"), 0o755)
        path = mock.patch.dict(os.environ, {"PATH": f"{bin_dir}:{os.environ['PATH']}"})
        path.start()
        self.addCleanup(path.stop)

        child = self.start_late_run("--upload", "s3://bucket/day/", "--upload-cmd", recorder, "--upload-after", after)
        code, _, _ = self.late_run_result(child)

        self.assertNotEqual(code, 0)
        self.assertFalse(os.path.exists(log) and open(log).read(), "an upload went before its file")

    def counted_run(self, *args, cpus=4, budget_env=None):
        """run_main with `cpus` CPUs (None: cpu_budget.sh's, under `budget_env`), a zstd thread per 64 KiB and a zstd
        that logs each call; returns the calls as (start, end, threads, output) and what the run printed."""
        calls = os.path.join(self.tmp.name, "zstd-calls")
        bin_dir = os.path.join(self.tmp.name, "counting-bin")
        os.makedirs(bin_dir, exist_ok=True)
        with open(os.path.join(bin_dir, "zstd"), "w") as f:
            f.write(COUNTING_ZSTD.format(python=sys.executable))
        os.chmod(os.path.join(bin_dir, "zstd"), 0o755)
        printed = io.StringIO()
        env = dict(budget_env or {}, REAL_ZSTD=shutil.which("zstd"), ZSTD_CALLS=calls)
        env["PATH"] = f"{bin_dir}:{env.get('PATH', os.environ['PATH'])}"
        budget = mock.patch.object(sut, "cpu_budget", return_value=cpus) if cpus else contextlib.nullcontext()
        with mock.patch.dict(os.environ, env), budget, mock.patch.object(sut, "BYTES_PER_THREAD", 64 << 10), \
                contextlib.redirect_stdout(printed):
            cfg = self.run_main(*args)
        with open(calls) as f:
            rows = [line.split() for line in f.read().splitlines()]
        return [(float(a), float(b), int(t[2:]), os.path.relpath(o, self.out)) for a, b, t, o in rows], printed.getvalue(), cfg

    # zstd -18 cuts a file into 32 MiB jobs, so a thread per job, up to 8, keeps a big file from finishing alone
    # long after the rest; never more threads than CPUs.
    def test_each_file_gets_a_zstd_thread_per_job_up_to_eight_and_the_cpus(self):
        self.assertEqual([sut.threads_for(s, 88) for s in (0, 1, 32 << 20, (32 << 20) + 1, 427_000_000, 1 << 40)],
                         [1, 1, 1, 2, 8, 8])
        self.assertEqual(sut.threads_for(427_000_000, 4), 4)

        calls, _, _ = self.counted_run(cpus=4)

        threads = {output: t for _, _, t, output in calls}
        # 200,000 bytes over 64 KiB a thread: 4; a unit counts its files' bytes.
        self.assertEqual(threads["planet/full/geo_cells.bin.zst"], 4)
        self.assertEqual(threads["planet/full/strings_layout.json.zst"], 1)
        self.assertEqual(threads["planet/admin/postcode_centroids.bin.zst"], 1)
        self.assertEqual(threads["planet/full/places.tt.zst"] if "planet/full/places.tt.zst" in threads
                         else threads["planet/admin/places.tt.zst"], 4)

    def test_no_more_zstd_threads_run_at_once_than_there_are_cpus(self):
        calls, _, cfg = self.counted_run(cpus=4)

        self.assertEqual(self.status, 0)
        self.assertEqual(sorted(cfg["files"]), sorted(self.files))
        for start, _, _, _ in calls:
            at_once = sum(t for s, e, t, _ in calls if s <= start < e)
            self.assertLessEqual(at_once, 4, calls)

    # The runner's job may run on 88 CPUs under a quota of 64: the budget is the quota's, here 2 of 8.
    def test_the_threads_follow_the_cgroups_cpu_quota(self):
        calls, printed, _ = self.counted_run(cpus=None, budget_env=fake_cpus(self, 8, "/job", {"/job": "200000 100000"}))

        self.assertEqual(self.status, 0)
        self.assertIn(f"with zstd {sut.LEVEL} on 2 threads...", printed)
        self.assertEqual({output: t for _, _, t, output in calls}["planet/full/geo_cells.bin.zst"], 2)
        for start, _, _, _ in calls:
            self.assertLessEqual(sum(t for s, e, t, _ in calls if s <= start < e), 2, calls)

    def test_prints_each_compressed_item_with_its_start_end_and_threads(self):
        calls, printed, _ = self.counted_run(cpus=4)

        items = re.findall(r"(?m)^  \[ *(\d+\.\d)s -> *(\d+\.\d)s -T(\d+)\] (\S+)$", printed)
        # A unit is named by its path, a file by its own.
        compressed = {output if output.endswith(".tt.zst") else output[:-len(".zst")] for _, _, _, output in calls}
        self.assertEqual(sorted(path for _, _, _, path in items), sorted(compressed))
        for start, end, threads, path in items:
            self.assertLessEqual(float(start), float(end), path)
        by_path = {path: int(t) for _, _, t, path in items}
        self.assertEqual(by_path["planet/full/geo_cells.bin"], 4)

    # A job that fails before it takes its threads (say a file it can't read) must still pass its turn on: the
    # jobs behind it in the queue wait for that turn, and the run must end with the error, not hang.
    def test_a_job_that_fails_before_its_turn_ends_the_run_with_its_error(self):
        plant = """
real = sut.compress
def compress(ticket, path, threads):
    if path.endswith("planet/full/addr_points.bin"):
        raise OSError("planted failure before the job's turn")
    return real(ticket, path, threads)
sut.compress = compress
"""
        self.assert_child_run_fails(plant, "planted failure before the job's turn")

    # A unit whose zstd raises holds its threads: they must go back, or every job queued behind it waits forever.
    def test_a_unit_that_raises_while_it_holds_its_threads_ends_the_run_with_its_error(self):
        plant = """
real = sut.run_logged
def run_logged(cmd, **kw):
    if cmd[0] == "zstd" and cmd[-1].endswith("planet/poi/major/pois.tt.zst"):
        raise RuntimeError("planted failure holding the unit's threads")
    return real(cmd, **kw)
sut.run_logged = run_logged
"""
        self.assert_child_run_fails(plant, "planted failure holding the unit's threads")

    # A file whose compression fails ends the run with its error once the zstds already running end: no job behind
    # it starts one, neither those still queued nor the one a worker holds as it waits for its threads, though each
    # would take its threads in turn and run its zstd, for up to its own limit, before the error got out.
    def test_a_file_that_fails_ends_the_run_with_no_job_behind_it_starting_its_zstd(self):
        started = os.path.join(self.tmp.name, "zstd-started")
        write(self.path("planet/full/slow.bin"), content(20, 600_000))
        write(self.path("planet/full/fails.bin"), content(21, 500_000))
        # 2 CPUs, so 2 workers: slow.bin, the largest, takes both CPUs and its zstd takes 2 s; fails.bin, next, fails
        # meanwhile, and its worker then takes the next job, which waits for slow.bin's threads.
        plant = f"""
import subprocess, time
sut.cpu_budget = lambda: 2
real_run = subprocess.run
def run(cmd, **kw):
    if cmd[0] == "zstd":
        with open({started!r}, "a") as f:
            f.write(os.path.relpath(cmd[-1], {self.out!r}) + "\\n")
        if cmd[-1].endswith("planet/full/slow.bin"):
            time.sleep(2)
    return real_run(cmd, **kw)
subprocess.run = run
real = sut.compress
def compress(ticket, path, threads):
    if path.endswith("planet/full/fails.bin"):
        raise subprocess.CalledProcessError(1, ["zstd", path])
    return real(ticket, path, threads)
sut.compress = compress
"""
        done = self.child_run(plant)

        self.assertNotEqual(done.returncode, 0, done.stdout)
        self.assertIn("planet/full/fails.bin']' returned non-zero exit status 1", done.stderr)
        with open(started) as f:
            self.assertEqual(f.read().splitlines(), ["planet/full/slow.bin"])

    def assert_child_run_fails(self, plant, error):
        done = self.child_run(plant)
        self.assertNotEqual(done.returncode, 0, done.stdout)
        self.assertIn(error, done.stderr)

    def child_run(self, plant, env=None):
        script = CHILD_RUN.format(here=HERE, plant=plant, out=self.out, transport=self.transport)
        try:
            return subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=60,
                                  env={**os.environ, **(env or {})})
        except subprocess.TimeoutExpired:
            self.fail("the run hung")

    def stalled_run(self, env):
        """A child run whose children stall as `env` asks (FAKE_TRANSPORT_STALL, ZSTD_STALL), a child's limit 5 s
        whatever its bytes: the run, the configurations.json it left, the pids of the children that stalled, those
        of them and of the zstds it ran that still run once it ended (each ended then), and what is left in its
        temp dir."""
        bin_dir = os.path.join(self.tmp.name, "stalling-bin")
        os.makedirs(bin_dir, exist_ok=True)
        with open(os.path.join(bin_dir, "zstd"), "w") as f:
            f.write(STALLING_ZSTD)
        os.chmod(os.path.join(bin_dir, "zstd"), 0o755)
        stalled, zstds = os.path.join(self.tmp.name, "stalled"), os.path.join(self.tmp.name, "zstd-pids")
        for path in (stalled, zstds):
            open(path, "w").close()
        scratch = os.path.join(self.tmp.name, "child-tmp")
        shutil.rmtree(scratch, ignore_errors=True)
        os.makedirs(scratch)

        def pids(path):
            with open(path) as f:
                return [int(p) for p in f.read().split()]

        plant = "sut.CHILD_SECONDS = 5\nsut.CHILD_BYTES_PER_SECOND = 1 << 40"
        try:
            done = self.child_run(plant, {"PATH": f"{bin_dir}:{os.environ['PATH']}", "REAL_ZSTD": shutil.which("zstd"),
                                          "FAKE_STALLED": stalled, "ZSTD_PIDS": zstds, "ZSTD_STALL": "",
                                          "FAKE_TRANSPORT_STALL": "", "TMPDIR": scratch, **env})
            running = [p for p in pids(stalled) + pids(zstds) if alive(p)]
        finally:
            for pid in pids(stalled):
                if alive(pid):
                    os.kill(pid, 9)
        with open(self.path("configurations.json")) as f:
            cfg = json.load(f)
        return done, cfg, pids(stalled), running, os.listdir(scratch)

    # A zstd that stalls, neither ending nor failing, is killed past its limit and the run ends with the error, for
    # the caller to compress again: it must not hold its threads, and every job queued behind them, for good.
    def test_a_zstd_that_stalls_is_killed_and_fails_the_run(self):
        done, _, stalled, running, left = self.stalled_run({"ZSTD_STALL": "planet/full/addr_points.bin"})

        self.assertNotEqual(done.returncode, 0, done.stdout)
        self.assertRegex(done.stderr, r"TimeoutExpired: Command .*'zstd'.*planet/full/addr_points\.bin'\]' timed out")
        self.assertEqual(len(stalled), 1)
        self.assertEqual(running, [])
        self.assertEqual(left, [])

    # A unit whose encode, zstd or verify stalls fails, and its encode and zstd end with it: its files go as .zst.
    # A verify killed so leaves no scratch behind, though only its own end removes it.
    def test_a_unit_whose_encode_zstd_or_verify_stalls_fails_and_ends_them(self):
        unit = "planet/poi/major/pois.tt.zst"
        for env, why in [({"FAKE_TRANSPORT_STALL": "encode:pois"}, "encode and zstd"),
                         ({"ZSTD_STALL": unit}, "encode and zstd"),
                         ({"FAKE_TRANSPORT_STALL": f"verify:{unit}"}, "verify")]:
            with self.subTest(env=env):
                done, cfg, stalled, running, left = self.stalled_run(env)

                self.assertEqual(done.returncode, sut.UNIT_FAILED, done.stdout + done.stderr)
                self.assertIn(f"  {unit}: {why}: still running after 5s, killed; its files go as .zst\n", done.stdout)
                self.assertNotIn(unit, cfg["units"])
                for m in self.units[unit][1]:
                    self.assertEqual(unzstd(self.path(m) + ".zst"), self.files[m], m)
                self.assertEqual(len(stalled), 1)
                self.assertEqual(running, [])
                self.assertEqual(left, [])

    SEARCH = ("planet/search/full/simplified/all/places.fst", "planet/search/full/simplified/all/search_meta.json")

    def compressed_copy(self):
        """The tables and .zst bytes of the same tree compressed in one go, in a copy."""
        copy = os.path.join(self.tmp.name, "copy")
        shutil.copytree(self.out, copy)
        argv = ["compress_index.py", copy, "--transport-bin", self.transport]
        with mock.patch.object(sys, "argv", argv), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(sut.main(), 0)
        with open(os.path.join(copy, "configurations.json")) as f:
            cfg = json.load(f)
        zst = {}
        for root, _, names in os.walk(copy):
            for name in names:
                if name.endswith(".zst"):
                    with open(os.path.join(root, name), "rb") as f:
                        zst[os.path.relpath(os.path.join(root, name), copy)] = f.read()
        return cfg, zst

    def zst_files(self):
        found = {}
        for root, _, names in os.walk(self.out):
            for name in names:
                if name.endswith(".zst"):
                    with open(os.path.join(root, name), "rb") as f:
                        found[os.path.relpath(os.path.join(root, name), self.out)] = f.read()
        return found

    def start_late_run(self, *args):
        """compress_index.py as the nightly runs it beside the place search, in a child process in a session of its
        own, which the test's end kills, session and all, and reaps; returns the child."""
        printed = open(os.path.join(self.tmp.name, "late-run.out"), "w")
        script = LATE_RUN.format(here=HERE, args=[self.out, "--transport-bin", self.transport, *args])
        child = subprocess.Popen([sys.executable, "-c", script], stdout=printed, start_new_session=True)

        def end():
            with contextlib.suppress(ProcessLookupError):
                os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=10)
            printed.close()
        self.addCleanup(end)
        return child

    def late_run_result(self, child):
        """The late run's exit status once it has ended (at most 60 s), the configurations.json it wrote and what
        it printed."""
        try:
            status = child.wait(timeout=60)
        except subprocess.TimeoutExpired:
            self.fail("the run hung")
        with open(self.path("configurations.json")) as f:
            cfg = json.load(f)
        with open(os.path.join(self.tmp.name, "late-run.out")) as f:
            return status, cfg, f.read()

    # The nightly starts compression when build-index exits, beside the place search: everything else is
    # compressed while it runs, its files once its status is written, and the tables and bytes are those of a
    # run that had them all from the start.
    def test_a_late_search_is_compressed_once_its_status_is_written_into_the_same_tables(self):
        expected_cfg, expected_zst = self.compressed_copy()
        for rel in self.SEARCH:
            os.remove(self.path(rel))
        status = os.path.join(self.tmp.name, "place-search.status")
        taken = os.path.join(self.tmp.name, "place-search.taken")

        child = self.start_late_run("--late-search", status, "--late-search-taken", taken)
        deadline = time.time() + 60
        early = ["planet/full/geo_cells.bin.zst", "planet/full/addr_points.bin.zst", "planet/full/street_ways.tt.zst",
                 "planet/poi/major/pois.tt.zst"]
        while time.time() < deadline and not all(os.path.exists(self.path(r)) for r in early):
            time.sleep(0.05)
        time.sleep(0.5)
        self.assertIsNone(child.poll())
        self.assertEqual([r for r in early if not os.path.exists(self.path(r))], [])
        with open(self.path("configurations.json")) as f:
            self.assertNotIn("files", json.load(f))
        for rel in self.SEARCH:
            write(self.path(rel), self.files[rel])
        with open(status, "w") as f:
            f.write("0\n")
        code, cfg, _ = self.late_run_result(child)

        self.assertEqual(code, 0)
        self.assertEqual(cfg, expected_cfg)
        self.assertEqual(self.zst_files(), expected_zst)
        for rel in self.SEARCH:
            self.assertEqual(unzstd(self.path(rel) + ".zst"), self.files[rel])
        with open(taken) as f:
            self.assertEqual(f.read(), "0\n")

    # A place search that failed still leaves the indexes it built, published as they are today, in the same bytes.
    def test_a_late_search_that_failed_publishes_the_indexes_it_left(self):
        expected_cfg, expected_zst = self.compressed_copy()
        status = os.path.join(self.tmp.name, "place-search.status")
        with open(status, "w") as f:
            f.write("1\n")
        taken = os.path.join(self.tmp.name, "place-search.taken")

        cfg = self.run_main("--late-search", status, "--late-search-taken", taken)

        self.assertEqual(self.status, 0)
        self.assertEqual(cfg, expected_cfg)
        self.assertEqual(self.zst_files(), expected_zst)
        with open(taken) as f:
            self.assertEqual(f.read(), "1\n")

    # A place search whose status never comes (its job killed with nothing written) must not hold compression up:
    # past the wait, the rest is published without its files, which the run reports as failed. What it took them
    # on says so: "none", which a status written after the wait, too late, leaves as it is.
    def test_a_late_search_that_never_ends_is_left_out_after_the_wait(self):
        status = os.path.join(self.tmp.name, "place-search.status")
        taken = os.path.join(self.tmp.name, "place-search.taken")

        child = self.start_late_run("--late-search", status, "--late-search-timeout", "1", "--late-search-taken", taken)
        code, cfg, printed = self.late_run_result(child)
        with open(status, "w") as f:
            f.write("0\n")

        self.assertEqual(code, 0)
        self.assertEqual(sorted(cfg["files"]), sorted(r for r in self.files if r not in self.SEARCH))
        for rel in self.SEARCH:
            self.assertFalse(os.path.exists(self.path(rel) + ".zst"), rel)
        self.assertIn("no place search status", printed)
        with open(taken) as f:
            self.assertEqual(f.read(), "none\n")

    # A wait that never ends (nan, inf: no time is past them) or makes no sense (under 0) is refused before
    # anything is compressed, rather than holding the run up on a status that never comes.
    def test_a_late_search_timeout_must_be_a_finite_number_of_seconds(self):
        status = os.path.join(self.tmp.name, "place-search.status")
        with open(status, "w") as f:
            f.write("0\n")
        for value in ("nan", "inf", "-1", "-0.5"):
            with self.subTest(value=value):
                printed = io.StringIO()
                with self.assertRaises(SystemExit) as refused, contextlib.redirect_stderr(printed):
                    self.run_main("--late-search", status, "--late-search-timeout", value)

                self.assertEqual(refused.exception.code, 2)
                self.assertIn(f"--late-search-timeout: {value!r} is not a finite number of seconds, 0 or more",
                              printed.getvalue())
                self.assertEqual(self.zst_files(), {})


class ComponentSizesTest(OutputTest):
    """configurations.json's component_sizes: each component's raw and wire bytes in each region."""

    COMPONENTS = {
        "mode.full": {"files": ["{region}/full/geo_cells.bin", "{region}/full/street_ways.bin", "{region}/full/place_cells.bin",
                                "{region}/full/place_nodes.bin", "{region}/full/place_hidden.bin",
                                "{region}/full/strings_layout.json"],
                      "optional_files": ["{region}/full/interp_postcodes.bin", "{region}/full/addr_postcodes.bin"]},
        "quality.simplified": {"files": ["{region}/quality/simplified/admin_polygons.bin",
                                         "{region}/quality/simplified/admin_rings.bin",
                                         "{region}/quality/simplified/arc_index.bin",
                                         "{region}/quality/simplified/arc_vertices.bin"]},
        "half": {"files": ["{region}/quality/exact/arc_index.bin"]},
        "poi_tier.none": {"files": []},
        "mode.admin-minimal": {"variants": [{"component": "half"}]},
    }

    def setUp(self):
        super().setUp()
        with open(self.path("configurations.json")) as f:
            cfg = json.load(f)
        cfg["axes"] = {"region": {"values": {"planet": {}, "europe": {}}}}
        cfg["components"] = self.COMPONENTS
        with open(self.path("configurations.json"), "w") as f:
            json.dump(cfg, f)
        # One of mode.full's optional files the build has; its other one it hasn't.
        write(self.path("planet/full/interp_postcodes.bin"), content(17))

    def test_each_component_counts_its_files_raw_and_each_unit_once_on_the_wire(self):
        cfg = self.run_main()
        sizes = cfg["component_sizes"]["planet"]
        files, units = cfg["files"], cfg["units"]

        full = [f"planet/full/{n}" for n in ("geo_cells.bin", "street_ways.bin", "place_cells.bin", "place_nodes.bin",
                                             "place_hidden.bin", "strings_layout.json", "interp_postcodes.bin")]
        self.assertNotIn("planet/full/addr_postcodes.bin", files)
        self.assertEqual(sizes["mode.full"], {
            "raw": sum(files[f]["size_raw"] for f in full),
            "wire": files["planet/full/geo_cells.bin"]["size_zst"] + files["planet/full/strings_layout.json"]["size_zst"]
            + files["planet/full/interp_postcodes.bin"]["size_zst"]
            + units["planet/full/street_ways.tt.zst"]["size"] + units["planet/full/places.tt.zst"]["size"]})
        # The files the build has (no admin_rings.bin here), the arcs unit once for its two files.
        simplified = [f"planet/quality/simplified/{n}" for n in ("admin_polygons.bin", "arc_index.bin", "arc_vertices.bin")]
        self.assertEqual(sizes["quality.simplified"], {
            "raw": sum(files[f]["size_raw"] for f in simplified),
            "wire": units["planet/quality/simplified/admin_polygons.tt.zst"]["size"]
            + units["planet/quality/simplified/arcs.tt.zst"]["size"]})
        # A component of some of a unit's files: its share of the unit, by their raw bytes.
        arcs = units["planet/quality/exact/arcs.tt.zst"]
        index = files["planet/quality/exact/arc_index.bin"]["size_raw"]
        vertices = files["planet/quality/exact/arc_vertices.bin"]["size_raw"]
        self.assertEqual(sizes["half"], {"raw": index, "wire": round(arcs["size"] * index / (index + vertices))})
        # None for a component of no files, or of none the region has.
        self.assertEqual(sorted(sizes), ["half", "mode.full", "quality.simplified"])
        self.assertNotIn("europe", cfg["component_sizes"])

    def test_a_unit_that_failed_counts_its_files_zst(self):
        os.environ["FAKE_VERIFY_FAILS"] = "planet/quality/simplified/arcs.tt.zst"

        cfg = self.run_main()

        files = cfg["files"]
        self.assertEqual(cfg["component_sizes"]["planet"]["quality.simplified"]["wire"],
                         cfg["units"]["planet/quality/simplified/admin_polygons.tt.zst"]["size"]
                         + files["planet/quality/simplified/arc_index.bin"]["size_zst"]
                         + files["planet/quality/simplified/arc_vertices.bin"]["size_zst"])


# The zstd command line before the shared thread budget: level 18 and a thread per 256 MiB, at least one and at most
# 16, whatever the CPUs.
PREVIOUS_LEVEL = "-18"


def previous_threads(size):
    return max(1, min(16, size // (256 << 20)))


def several_jobs(seed, size=48 << 20):
    """`size` bytes that zstd -18 cuts into several 32 MiB jobs, and compresses fast: a 256 KiB block of random
    nibbles repeated, a few bytes of each copy changed."""
    rng = random.Random(seed)
    block = rng.randbytes(256 << 10).translate(bytes(b & 15 for b in range(256)))
    out = bytearray()
    while len(out) < size:
        copy = bytearray(block)
        for _ in range(64):
            copy[rng.randrange(len(copy))] = rng.randrange(256)
        out += copy
    return bytes(out[:size])


class PreviousScheduleTest(unittest.TestCase):
    """The schedule changes no published byte: a file and a unit big enough for several zstd jobs come out as the
    previous command line compressed them, with the same zstd, though they now get other thread counts."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out = os.path.join(self.tmp.name, "output")
        self.dir = os.path.join(self.out, "planet", "full")
        os.makedirs(self.dir)
        with open(os.path.join(self.out, "configurations.json"), "w") as f:
            json.dump({"version": 1, "transport": load_template()["transport"]}, f)
        self.transport = os.path.join(self.tmp.name, "geocoder-transport")
        with open(self.transport, "w") as f:
            f.write(FAKE_TRANSPORT.format(python=sys.executable))
        os.chmod(self.transport, 0o755)
        env = mock.patch.dict(os.environ, {"FAKE_TRANSPORT_LOG": os.path.join(self.tmp.name, "transport.log"),
                                           "FAKE_VERIFY_FAILS": ""})
        env.start()
        self.addCleanup(env.stop)
        write(os.path.join(self.dir, "geo_cells.bin"), several_jobs(1))
        write(os.path.join(self.dir, "street_ways.bin"), several_jobs(2))

    def previous(self, name, stdin=None):
        """The previous command line's .zst of the file `name` (or of `stdin`, a command's output, piped)."""
        path = os.path.join(self.tmp.name, name + ".previous.zst")
        size = os.path.getsize(os.path.join(self.dir, name))
        cmd = ["zstd", PREVIOUS_LEVEL, f"-T{previous_threads(size)}", "-f", "--quiet", "-o", path]
        if stdin is None:
            subprocess.run(cmd + [os.path.join(self.dir, name)], check=True)
        else:
            enc = subprocess.Popen(stdin, stdout=subprocess.PIPE)
            subprocess.run(cmd, stdin=enc.stdout, check=True)
            enc.stdout.close()
            self.assertEqual(enc.wait(), 0)
        with open(path, "rb") as f:
            return f.read()

    def test_a_file_and_a_unit_of_several_jobs_keep_their_bytes(self):
        argv = ["compress_index.py", self.out, "--transport-bin", self.transport]
        printed = io.StringIO()
        with mock.patch.object(sys, "argv", argv), mock.patch.object(sut, "cpu_budget", return_value=4), \
                contextlib.redirect_stdout(printed):
            self.assertEqual(sut.main(), 0)

        # 48 MiB: two threads now, one before.
        self.assertRegex(printed.getvalue(), r"-T2\] planet/full/geo_cells\.bin\n")
        self.assertRegex(printed.getvalue(), r"-T2\] planet/full/street_ways\.tt\.zst\n")
        self.assertEqual(previous_threads(48 << 20), 1)
        with open(os.path.join(self.dir, "geo_cells.bin.zst"), "rb") as f:
            self.assertTrue(f.read() == self.previous("geo_cells.bin"), "geo_cells.bin.zst")
        with open(os.path.join(self.dir, "street_ways.tt.zst"), "rb") as f:
            unit = f.read()
        self.assertTrue(unit == self.previous("street_ways.bin", [self.transport, "encode", "cols", self.dir, "street_ways.bin"]),
                        "street_ways.tt.zst")


class SchedulerTest(unittest.TestCase):
    def run_jobs(self, cpus, jobs):
        """Runs sut.job(ticket, fn) for each fn of `jobs` on its own thread, the last submitted started first;
        returns each one's exception (None when it returned) once all ended, or fails the test on a hang."""
        tickets = [cpus.ticket() for _ in jobs]
        errors = [None] * len(jobs)

        def run(i):
            try:
                sut.job(tickets[i], jobs[i])
            except Exception as e:  # noqa: BLE001
                errors[i] = e

        threads = [threading.Thread(target=run, args=(i,), daemon=True) for i in range(len(jobs))]
        for t in reversed(threads):
            t.start()
            time.sleep(0.01)
        for t in threads:
            t.join(10)
        self.assertFalse(any(t.is_alive() for t in threads), "the jobs hung")
        return errors

    def test_jobs_take_their_threads_in_the_order_they_were_submitted(self):
        order = []

        def takes(i):
            def fn(ticket):
                ticket.take(1)
                order.append(i)
            return fn

        self.run_jobs(sut.Cpus(1), [takes(i) for i in range(5)])

        self.assertEqual(order, [0, 1, 2, 3, 4])

    def test_a_job_that_fails_before_its_turn_passes_the_turn_on(self):
        order = []

        def fails(ticket):
            raise OSError("planted")

        def takes(i):
            def fn(ticket):
                ticket.take(2)
                order.append(i)
            return fn

        errors = self.run_jobs(sut.Cpus(2), [takes(0), fails, takes(2), takes(3)])

        self.assertIsInstance(errors[1], OSError)
        self.assertEqual([errors[0], errors[2], errors[3]], [None, None, None])
        self.assertEqual(order, [0, 2, 3])

    def test_a_job_that_raises_gives_back_the_threads_it_held(self):
        def holds(ticket):
            ticket.take(2)
            raise RuntimeError("planted")

        def takes(ticket):
            ticket.take(2)

        errors = self.run_jobs(sut.Cpus(2), [holds, takes, takes])

        self.assertIsInstance(errors[0], RuntimeError)
        self.assertEqual(errors[1:], [None, None])


# The nightly's compress commands: both start when build-index exits, beside the place search.
def nightly_compress_calls():
    with open(NIGHTLY) as f:
        return re.findall(r'^ *(.*)python3 "\$GITHUB_WORKSPACE/geocoder/builder/tools/compress_index\.py" "\$WORKDIR/output" \\\n'
                          r'((?: +.*\\\n)* +.*)', f.read(), re.M)


class NightlyCompressTest(unittest.TestCase):
    def test_both_compress_calls_take_the_place_search_files_late_at_a_lower_priority(self):
        calls = nightly_compress_calls()
        self.assertEqual(len(calls), 2)
        for prefix, args in calls:
            self.assertIn('--late-search "$WORKDIR/place-search.status"', args)
            self.assertIn('--late-search-taken "$WORKDIR/place-search.taken"', args)
            self.assertEqual(prefix.split(), ["nice", "-n", "10"])


class TemplateAttributionTest(unittest.TestCase):
    """The template's `attribution` list credits each source of the published data, and the
    component of each mode dir and water dir lists the attribution.json the builder writes in it."""

    def setUp(self):
        self.cfg = load_template()
        self.entries = self.cfg["attribution"]
        self.components = self.cfg["components"]

    def applies(self, entry):
        return sorted(c for c in self.components if any(fnmatch.fnmatchcase(c, p) for p in entry["applies_to"]))

    def test_credits_each_source_under_its_licence(self):
        self.assertEqual([e["id"] for e in self.entries],
                         ["osm", "geonames", "codepoint-open", "tiger", "wikidata", "marine-regions"])
        self.assertEqual([e["licence"] for e in self.entries],
                         ["ODbL 1.0", "CC BY 4.0", "OGL v3.0", "Public domain", "CC0 1.0", "CC BY 4.0"])
        for e in self.entries:
            self.assertEqual(set(e) - {"licence_url"}, {"id", "text", "url", "licence", "notices", "applies_to"}, e["id"])
            self.assertTrue(e["text"] and e["url"].startswith("https://") and e["notices"], e["id"])
        # Public domain has no licence to link.
        self.assertEqual(["licence_url" in e for e in self.entries], [True, True, True, False, True, True])
        self.assertEqual(self.entries[0]["text"], "© OpenStreetMap contributors")

    def test_osm_applies_to_every_component_and_the_others_to_the_dirs_of_their_data(self):
        for e in self.entries:
            for pattern in e["applies_to"]:
                self.assertTrue(fnmatch.filter(self.components, pattern), (e["id"], pattern))
        self.assertEqual(self.applies(self.entries[0]), sorted(self.components))
        centroids = sorted(c for c, comp in self.components.items()
                           if any(f.endswith("/postcode_centroids.bin") for f in comp.get("files", []) + comp.get("optional_files", [])))
        self.assertEqual(centroids, ["mode.admin", "mode.full", "mode.no-addresses"])
        for e in self.entries[1:4]:
            self.assertEqual(self.applies(e), centroids, e["id"])
        # Wikidata's sitelinks rank the POIs of the tiers that hold any.
        self.assertEqual(self.applies(self.entries[4]), ["poi_tier.all", "poi_tier.major", "poi_tier.notable"])
        # Marine Regions' seas and oceans are in the water dirs alone.
        self.assertEqual(self.applies(self.entries[5]), ["water.all", "water.all-exact", "water.major"])

    def test_each_poi_tier_lists_its_poi_attribution_json_as_optional(self):
        # The builder writes it only where Wikidata's sitelinks ranked the POIs (--wikidata-sitelinks),
        # by a name of its own: downloads land flat by name, beside the mode dir's attribution.json.
        for tier in ("major", "notable", "all"):
            comp = self.components["poi_tier." + tier]
            self.assertEqual(comp.get("optional_files"), ["{region}/poi/%s/poi_attribution.json" % tier], tier)
            self.assertEqual([f for f in comp["files"] if f.endswith("attribution.json")], [], tier)
        self.assertEqual(self.components["poi_tier.none"].get("optional_files", []), [])

    def test_each_mode_dir_and_water_dir_lists_its_attribution_json(self):
        holding = {c: f for c, comp in self.components.items() for f in comp.get("files", [])
                   if os.path.basename(f) == "attribution.json"}
        self.assertEqual(sorted(holding), ["dir.admin-minimal", "dir.admin-minimal-poi", "mode.admin", "mode.full", "mode.no-addresses",
                                           "water.all", "water.all-exact", "water.major"])
        for c, f in holding.items():
            self.assertEqual({os.path.dirname(g) for g in self.components[c]["files"]}, {os.path.dirname(f)}, c)

    def test_the_compressor_publishes_every_file_a_component_lists(self):
        names = {os.path.basename(f) for comp in self.components.values() for f in comp.get("files", []) + comp.get("optional_files", [])}
        with tempfile.TemporaryDirectory() as out:
            for name in names:
                write(os.path.join(out, "planet", "dir", name), b"x")
            published = {os.path.basename(p) for p in sut.index_files(out)}
        self.assertEqual(published, names)


if __name__ == "__main__":
    unittest.main()
