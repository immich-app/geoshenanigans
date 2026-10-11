#!/usr/bin/env python3
"""Tests for compress_index.py. Run: python3 -m unittest discover -s geocoder/builder/tools"""
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import compress_index as sut  # noqa: E402


def write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


def unzstd(path):
    return subprocess.run(["zstd", "-dc", path], check=True, capture_output=True).stdout


def content(seed, n=200_000):
    return bytes((seed * 31 + i * 7 + i // 251) % 256 for i in range(n))


class CompressIndexTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = os.path.join(self.tmp.name, "output")
        os.makedirs(self.out)
        with open(os.path.join(self.out, "configurations.json"), "w") as f:
            json.dump({"version": 1}, f)

    def tearDown(self):
        self.tmp.cleanup()

    def run_main(self, *args):
        with mock.patch.object(sys, "argv", ["compress_index.py", self.out, *args]):
            sut.main()
        with open(os.path.join(self.out, "configurations.json")) as f:
            return json.load(f)

    def check_files(self, cfg, expected):
        self.assertEqual(sorted(cfg["files"]), sorted(expected))
        for rel, data in expected.items():
            path = os.path.join(self.out, rel)
            self.assertEqual(unzstd(path + ".zst"), data, rel)
            self.assertEqual(cfg["files"][rel]["size_raw"], len(data))
            self.assertEqual(cfg["files"][rel]["sha256"], sut.hashlib.sha256(data).hexdigest())
            self.assertEqual(cfg["files"][rel]["size_zst"], os.path.getsize(path + ".zst"))

    def test_compresses_each_content_once_and_links_its_copies(self):
        files = {
            "planet/full/geo_cells.bin": content(1),
            "planet/no-addresses/geo_cells.bin": content(1),
            "planet/full/strings_layout.json": b'{"tiers": []}',
            "planet/full/addr_points.bin": content(2),
        }
        for rel, data in files.items():
            write(os.path.join(self.out, rel), data)
        write(os.path.join(self.out, "planet/full/ways.osm_ids"), content(3))

        cfg = self.run_main()

        self.check_files(cfg, files)
        full = os.stat(os.path.join(self.out, "planet/full/geo_cells.bin.zst"))
        copy = os.stat(os.path.join(self.out, "planet/no-addresses/geo_cells.bin.zst"))
        self.assertEqual(full.st_ino, copy.st_ino)
        self.assertFalse(os.path.exists(os.path.join(self.out, "planet/full/ways.osm_ids.zst")))
        self.assertFalse(os.path.exists(self.out + ".zst-store"))
        self.assertEqual(cfg["version"], 1)

    def test_while_pid_ships_the_final_bytes_of_files_rewritten_after_compression(self):
        # The writer: a file done early, one held open while written, one
        # rewritten after the compressor took it, and two copies of one
        # content of which only the first is rewritten.
        writer = textwrap.dedent(f"""
            import os, sys, time
            out = {self.out!r}
            def write(rel, data):
                path = os.path.join(out, rel)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "wb") as f:
                    f.write(data)
            write("a/done.bin", b"done" * 1000)
            write("a/rewritten.bin", b"first" * 1000)
            write("a/copy1.bin", b"same" * 1000)
            write("b/copy2.bin", b"same" * 1000)
            held = open(os.path.join(out, "a/held.bin"), "wb")
            held.write(b"part" * 1000)
            held.flush()
            time.sleep(1.5)
            write("a/rewritten.bin", b"second" * 1000)
            write("a/copy1.bin", b"changed" * 1000)
            held.write(b"rest" * 1000)
            held.close()
            time.sleep(0.5)
        """)
        builder = subprocess.Popen([sys.executable, "-c", writer])
        with mock.patch.object(sut, "SCAN_SECONDS", 0.1):
            cfg = self.run_main("--while-pid", str(builder.pid))
        builder.wait()

        self.check_files(cfg, {
            "a/done.bin": b"done" * 1000,
            "a/rewritten.bin": b"second" * 1000,
            "a/copy1.bin": b"changed" * 1000,
            "b/copy2.bin": b"same" * 1000,
            "a/held.bin": b"part" * 1000 + b"rest" * 1000,
        })

    def test_while_pid_streams_each_file_and_sends_a_recompressed_one_again_last(self):
        log = os.path.join(self.tmp.name, "upload.log")
        recorder = f"{sys.executable} -c \"import sys; open({log!r}, 'a').write(sys.stdin.read() + '--\\n')\""
        writer = textwrap.dedent(f"""
            import os, time
            path = os.path.join({self.out!r}, "x.bin")
            with open(path, "wb") as f:
                f.write(b"old" * 1000)
            time.sleep(1.0)
            with open(path, "wb") as f:
                f.write(b"new" * 1000)
            time.sleep(0.3)
        """)
        builder = subprocess.Popen([sys.executable, "-c", writer])
        with mock.patch.object(sut, "SCAN_SECONDS", 0.1):
            self.run_main("--while-pid", str(builder.pid), "--upload", "s3://bucket/day", "--upload-cmd", recorder)
        builder.wait()

        with open(log) as f:
            streams = f.read().split("--\n")[:-1]
        x = f"cp {os.path.join(self.out, 'x.bin.zst')} s3://bucket/day/x.bin.zst"
        self.assertEqual(streams[0].splitlines(), [x, x])
        self.assertEqual(streams[1].splitlines(), [x])

    def test_drops_the_zst_of_a_file_that_is_gone(self):
        write(os.path.join(self.out, "kept.bin"), content(4))
        write(os.path.join(self.out, "gone.bin.zst"), b"stale")

        cfg = self.run_main()

        self.check_files(cfg, {"kept.bin": content(4)})
        self.assertFalse(os.path.exists(os.path.join(self.out, "gone.bin.zst")))


class HeldByTest(unittest.TestCase):
    def test_lists_open_files_and_ends_when_the_process_does(self):
        with tempfile.NamedTemporaryFile() as f:
            holder = subprocess.Popen([sys.executable, "-c", f"import time; f = open({f.name!r}); time.sleep(30)"])
            try:
                deadline = time.time() + 10
                while f.name not in sut.held_by(holder.pid) and time.time() < deadline:
                    time.sleep(0.05)
                self.assertIn(f.name, sut.held_by(holder.pid))
                self.assertTrue(sut.running(holder.pid))
            finally:
                holder.kill()
                holder.wait()
            self.assertFalse(sut.running(holder.pid))


if __name__ == "__main__":
    unittest.main()
