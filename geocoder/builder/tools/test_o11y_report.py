#!/usr/bin/env python3
"""Tests for o11y_report.py. Run: python3 -m unittest discover -s geocoder/builder/tools"""
import http.server
import io
import json
import os
import re
import sys
import tempfile
import threading
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import o11y_report as sut  # noqa: E402

WORKFLOW = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..",
                        ".github", "workflows", "geocoder-build.yml")
NOW = sut.unix_time("2026-10-12T07:00:00Z")
IDENTITY = {"project": "geoshenanigans", "cluster": "geocoder", "env": "prod", "provider": "github", "region": "global"}
IDENTITY_LABELS = 'project="geoshenanigans",cluster="geocoder",env="prod",provider="github",region="global"'

BUILD_LOG = """Using 87 worker threads.
  Pass 1: scanning relations...
  [0.6s 58.3/88cores 66% 372MiB]   Pass 1: blob scan
  [3.7s 27.9/88cores 31% 2079MiB] Pass 1: relation scanning
  Processed 5000/154945 blocks...
  [35.3s 60.9/88cores 69% 36031MiB] Pass 2: node processing
  [1.2s 9.1/88cores 10% 30000MiB]       planet/full: pack + write
  [322.4s 9.8/88cores 11% 25143MiB] All index writing (total)
"""
MEMWATCH_LOG = """[memwatch 06:30:01] cgroup=91675Mi/98304Mi anon=113Mi file=88832Mi file_dirty=0Mi file_writeback=0Mi build_index_rss=16Mi others_rss=310Mi
[memwatch 06:30:31] cgroup=98230Mi/98304Mi anon=20382Mi file=75673Mi file_dirty=5527Mi file_writeback=1668Mi build_index_rss=19519Mi others_rss=1338Mi
[memwatch 06:31:01] cgroup=98256Mi/98304Mi anon=26503Mi file=70396Mi file_dirty=6478Mi file_writeback=110Mi build_index_rss=25031Mi others_rss=1955Mi
"""


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)


def step(name, conclusion, started="2026-10-12T06:20:00Z", completed="2026-10-12T06:21:40Z", status="completed"):
    return {"name": name, "status": status, "conclusion": conclusion, "started_at": started, "completed_at": completed}


def by_name(samples, name):
    return {tuple(sorted(labels.items())): value for n, labels, value in samples if n == name}


class WorkDirTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = self.tmp.name

    def tearDown(self):
        self.tmp.cleanup()

    def path(self, *parts):
        return os.path.join(self.dir, *parts)


class ExpositionTest(unittest.TestCase):
    def test_each_sample_is_a_line_at_the_push_time_in_ms_with_the_identity_labels_last(self):
        body = sut.exposition([("geocoder_build_success", {"trigger": "schedule", "runner": "pokedex-giant"}, 1),
                               ("geocoder_patch_bytes_sum", {}, 19188736)], 1791788400123)

        self.assertEqual(body, f'geocoder_build_success{{trigger="schedule",runner="pokedex-giant",{IDENTITY_LABELS}}}'
                               " 1 1791788400123\n"
                               f"geocoder_patch_bytes_sum{{{IDENTITY_LABELS}}} 19188736 1791788400123\n")

    def test_a_label_cannot_shadow_an_identity_label(self):
        body = sut.exposition([("geocoder_build_success", {"env": "dev", "cluster": "pokedex"}, 1)], 1)

        self.assertEqual(body, 'geocoder_build_success{env="prod",cluster="geocoder",project="geoshenanigans",'
                               'provider="github",region="global"} 1 1\n')

    def test_escapes_backslashes_quotes_and_newlines_in_label_values(self):
        body = sut.exposition([("geocoder_build_phase_duration_seconds", {"phase": 'a\\b "c"\nd'}, 1.5)], 1)

        self.assertEqual(body, 'geocoder_build_phase_duration_seconds{phase="a\\\\b \\"c\\"\\nd",'
                               f"{IDENTITY_LABELS}}} 1.5 1\n")


class CappedTest(unittest.TestCase):
    def test_keeps_a_msg_up_to_the_cap(self):
        for msg in ("Build geocoder index: success in 463s", "a" * sut.MAX_MSG_BYTES):
            with self.subTest(msg=msg[:2]):
                self.assertEqual(sut.capped(msg), msg)

    def test_cuts_a_longer_msg_to_the_cap_in_utf8_and_marks_it(self):
        for msg in ("x" * 20000, "ü" * 5000, "€" * 1366, "a" * (sut.MAX_MSG_BYTES + 1)):
            with self.subTest(msg=msg[:2]):
                cut = sut.capped(msg)

                self.assertLessEqual(len(cut.encode()), sut.MAX_MSG_BYTES)
                self.assertGreater(len(cut.encode()), sut.MAX_MSG_BYTES - 3)
                self.assertTrue(cut.endswith(sut.TRUNCATED))
                self.assertTrue(msg.startswith(cut[:-len(sut.TRUNCATED)]))


class LogTest(unittest.TestCase):
    def test_a_context_field_cannot_shadow_an_identity_field(self):
        log = sut.Log(app="geocoder-build", env="dev", cluster="pokedex")

        log.add("run success")

        self.assertEqual(log.records, [{"_msg": "run success", "level": "info", "app": "geocoder-build", **IDENTITY}])


class LogLinesTest(unittest.TestCase):
    def test_each_record_is_one_json_line_a_microsecond_after_the_one_before(self):
        log = sut.Log(app="geocoder-build", run_id="7")
        log.add("Build geocoder index: success in 463s", step="build")
        log.add("two\nlines", level="error")

        lines = sut.log_lines(log.records, NOW + 0.25)

        self.assertEqual([line.count(b"\n") for line in lines], [1, 1])
        records = [json.loads(line) for line in lines]
        self.assertEqual(records, [
            {"_time": "2026-10-12T07:00:00.250000Z", "_msg": "Build geocoder index: success in 463s", "level": "info",
             "step": "build", "app": "geocoder-build", "run_id": "7", **IDENTITY},
            {"_time": "2026-10-12T07:00:00.250001Z", "_msg": "two\nlines", "level": "error", "app": "geocoder-build",
             "run_id": "7", **IDENTITY},
        ])
        self.assertEqual(list(records[0])[:2], ["_time", "_msg"])
        self.assertEqual(list(records[0])[-len(IDENTITY):], list(IDENTITY))


class BatchesTest(unittest.TestCase):
    def test_takes_at_most_the_batch_line_count(self):
        bodies = list(sut.batches([b"x\n"] * (2 * sut.LOG_BATCH_LINES + 1)))

        self.assertEqual([body.count(b"\n") for body in bodies], [sut.LOG_BATCH_LINES, sut.LOG_BATCH_LINES, 1])

    def test_starts_a_new_batch_before_a_line_that_would_pass_the_byte_cap(self):
        line = b"y" * (sut.LOG_BATCH_BYTES // 3 - 1) + b"\n"

        bodies = list(sut.batches([line] * 7))

        self.assertEqual([body.count(b"\n") for body in bodies], [3, 3, 1])
        self.assertTrue(all(len(body) <= sut.LOG_BATCH_BYTES for body in bodies))

    def test_a_line_longer_than_the_cap_goes_alone(self):
        long = b"z" * sut.LOG_BATCH_BYTES + b"\n"

        self.assertEqual(list(sut.batches([b"a\n", long, b"b\n"])), [b"a\n", long, b"b\n"])

    def test_no_lines_make_no_batches(self):
        self.assertEqual(list(sut.batches([])), [])


class ReadLinesTest(WorkDirTest):
    def test_a_file_the_run_never_wrote_has_no_lines(self):
        self.assertEqual(sut.read_lines(self.path("build.log")), [])
        self.assertEqual(sut.read_lines(""), [])


class PhasesTest(unittest.TestCase):
    def test_takes_the_top_level_phases_only(self):
        self.assertEqual(sut.phases(BUILD_LOG.splitlines()), {
            "Pass 1: relation scanning": 3.7,
            "Pass 2: node processing": 35.3,
            "All index writing (total)": 322.4,
        })

    def test_adds_up_a_phase_that_ends_more_than_once(self):
        self.assertEqual(sut.phases(["  [1.2s] Admin assembly", "  [0.1s 1.0/2cores 50% 1MiB] Admin assembly"]),
                         {"Admin assembly": 1.3})


class PeakMemoryTest(unittest.TestCase):
    def test_takes_the_largest_sample_of_each_kind_in_bytes(self):
        self.assertEqual(sut.peak_memory(MEMWATCH_LOG.splitlines()), {
            "anon": 26503 * sut.MIB, "file": 88832 * sut.MIB, "dirty": 6478 * sut.MIB, "others": 1955 * sut.MIB,
        })


class PatchSamplesTest(WorkDirTest):
    def setUp(self):
        super().setUp()
        for variant in ("planet/full", "planet/poi/all", "europe/full", "europe/admin"):
            write(self.path("output", variant, "geo_cells.bin.zst"), "")
        write(self.path("output", "planet/full", "patch.gcpatch"), "p" * 3000)
        write(self.path("output", "planet/poi/all", "patch.gcpatch"), "p" * 700)
        write(self.path("results", "planet_full"), "PASS 28 2\n")
        write(self.path("results", "planet_poi_all"), "FAIL 2 7 0\n")
        write(self.path("results", "planet_poi_all.mismatches"), "geo_cells.bin: differs;")
        write(self.path("results", "europe_admin"), "SKIP\n")

    def run_sut(self):
        log = sut.Log()
        return sut.patch_samples(self.path("results"), self.path("output"), log), log

    def test_counts_each_variant_as_the_workflow_does_a_missing_result_failing(self):
        samples, _ = self.run_sut()

        self.assertEqual(by_name(samples, "geocoder_patch_verify_variants"), {
            (("result", "pass"),): 1, (("result", "fail"),): 2, (("result", "skip"),): 1,
        })

    def test_reports_each_patch_and_their_sum(self):
        samples, _ = self.run_sut()

        self.assertEqual(by_name(samples, "geocoder_patch_bytes"), {
            (("variant", "planet/full"),): 3000, (("variant", "planet/poi/all"),): 700,
        })
        self.assertEqual(by_name(samples, "geocoder_patch_bytes_sum"), {(): 3700})

    def test_logs_each_result_failures_as_errors(self):
        _, log = self.run_sut()

        self.assertEqual([(r["_msg"], r["level"]) for r in log.records], [
            ("europe/admin: SKIP", "info"),
            ("europe/full: no verify result: its diff or patch tool crashed", "error"),
            ("planet/full: PASS 28 2", "info"),
            ("planet/poi/all: FAIL 2 7 0 (geo_cells.bin: differs;)", "error"),
        ])

    def test_a_build_without_patch_generation_has_no_verify_counts(self):
        samples = sut.patch_samples(self.path("no-results"), self.path("output"), sut.Log())

        self.assertEqual(by_name(samples, "geocoder_patch_verify_variants"), {})


class VerdictTest(unittest.TestCase):
    def test_counts_results_as_the_workflow_does(self):
        for result, expected in (("PASS 28 2851", "pass"), ("SKIP", "skip"), ("FAIL 1 0 0", "fail"), (None, "fail")):
            with self.subTest(result=result):
                self.assertEqual(sut.verdict(result), expected)


class ReadIndexBytesTest(WorkDirTest):
    def test_sums_the_table_compress_index_writes(self):
        write(self.path("configurations.json"), json.dumps({"files": {
            "planet/full/geo_cells.bin": {"size_raw": 1000, "size_zst": 300, "sha256": "a"},
            "planet/full/strings_layout.json": {"size_raw": 50, "size_zst": 20, "sha256": "b"},
        }}))

        self.assertEqual(sut.read_index_bytes(self.dir), {"raw": 1050, "compressed": 320})

    def test_nothing_before_compression(self):
        write(self.path("configurations.json"), json.dumps({"build": {"version": 17}}))

        self.assertEqual(sut.read_index_bytes(self.dir), {})


class NightlyTest(WorkDirTest):
    def setUp(self):
        super().setUp()
        write(self.path("build.log"), BUILD_LOG)
        write(self.path("memwatch.log"), MEMWATCH_LOG)
        job = {"started_at": "2026-10-12T06:17:30Z", "steps": [
            step("Setup", "success"),
            step("Download PBF and previous indexes", "success", "2026-10-12T06:18:00Z", "2026-10-12T06:20:28Z"),
            step("Apply replication diffs", "skipped"),
            step("Build geocoder index", "failure", "2026-10-12T06:20:28Z", "2026-10-12T06:28:11Z"),
            step("Generate patches and verify", "skipped"),
            step("Compress index files", "cancelled", started=None, completed=None),
            step("Report to o11y", None, completed=None, status="in_progress"),
        ]}
        write(self.path("job.json"), json.dumps(job))

    def run_sut(self):
        opts = sut.parse_args(["nightly", "--dry-run", "--job", self.path("job.json"), "--job-status", "failure",
                               "--build-log", self.path("build.log"), "--memwatch-log", self.path("memwatch.log"),
                               "--results-dir", self.path("results"), "--output-dir", self.path("output"),
                               "--build-date", "2026-10-12", "--data-timestamp", "2026-10-12T00:00:00Z",
                               "--trigger", "schedule", "--runner", "pokedex-giant", "--run-id", "38195429460",
                               "--run-attempt", "2", "--run-url", "https://example.invalid/runs/38195429460"])
        return sut.nightly(opts, NOW)

    def test_reports_the_run_whatever_its_outcome(self):
        samples, _ = self.run_sut()

        run = (("runner", "pokedex-giant"), ("trigger", "schedule"))
        self.assertEqual(by_name(samples, "geocoder_build_last_run_timestamp_seconds"), {run: NOW})
        self.assertEqual(by_name(samples, "geocoder_build_success"), {run: 0})
        self.assertEqual(by_name(samples, "geocoder_build_duration_seconds"), {run: 42 * 60 + 30})
        self.assertEqual(by_name(samples, "geocoder_build_data_timestamp_seconds"), {(): NOW - 7 * 3600})

    def test_reports_the_mapped_steps_that_ran(self):
        samples, _ = self.run_sut()

        self.assertEqual(by_name(samples, "geocoder_build_step_duration_seconds"), {
            (("step", "download"),): 148, (("step", "build"),): 463, (("step", "compress"),): 0,
        })
        self.assertEqual(by_name(samples, "geocoder_build_step_success"), {
            (("step", "download"),): 1, (("step", "build"),): 0, (("step", "compress"),): 0,
        })

    def test_reports_the_build_phases_and_peak_memory(self):
        samples, _ = self.run_sut()

        self.assertEqual(len(by_name(samples, "geocoder_build_phase_duration_seconds")), 3)
        self.assertEqual(by_name(samples, "geocoder_build_peak_memory_bytes")[(("kind", "dirty"),)], 6478 * sut.MIB)

    def test_logs_build_log_memwatch_each_finished_step_and_the_run_last(self):
        _, log = self.run_sut()

        lines = BUILD_LOG.splitlines() + MEMWATCH_LOG.splitlines()
        self.assertEqual([(r["_msg"], r["step"]) for r in log.records[:len(lines)]], [(line, "build") for line in lines])
        self.assertEqual([(r["_msg"], r.get("step"), r["level"]) for r in log.records[len(lines):]], [
            ("Setup: success in 100s", None, "info"),
            ("Download PBF and previous indexes: success in 148s", "download", "info"),
            ("Apply replication diffs: skipped in 100s", "apply", "info"),
            ("Build geocoder index: failure in 463s", "build", "error"),
            ("Generate patches and verify: skipped in 100s", "patches", "info"),
            ("Compress index files: cancelled in 0s", "compress", "error"),
            ("run failure in 2550s, schedule on pokedex-giant: https://example.invalid/runs/38195429460",
             None, "error"),
        ])

    def test_every_log_line_carries_the_app_the_run_and_the_identity_fields(self):
        _, log = self.run_sut()

        for record in log.records:
            self.assertEqual((record["app"], record["run_id"], record["run_attempt"], record["build_date"]),
                             ("geocoder-build", "38195429460", "2", "2026-10-12"))
            self.assertEqual({key: record[key] for key in IDENTITY}, IDENTITY)

    def test_a_run_that_failed_before_its_work_dir_reports_its_outcome_only(self):
        opts = sut.parse_args(["nightly", "--dry-run", "--job-status", "cancelled", "--trigger", "workflow_dispatch",
                               "--runner", "pokedex-giant", "--run-url", "https://example.invalid/runs/1"])

        samples, log = sut.nightly(opts, NOW)

        run = {"trigger": "workflow_dispatch", "runner": "pokedex-giant"}
        self.assertEqual(samples, [("geocoder_build_last_run_timestamp_seconds", run, NOW),
                                   ("geocoder_build_success", run, 0)])
        self.assertEqual([r["_msg"] for r in log.records],
                         ["run cancelled, workflow_dispatch on pokedex-giant: https://example.invalid/runs/1"])


class StepSlugsTest(unittest.TestCase):
    def test_every_mapped_name_is_a_step_of_the_nightly(self):
        with open(WORKFLOW) as f:
            names = set(re.findall(r"^\s*- name: (.+?)\s*$", f.read(), re.MULTILINE))

        self.assertLessEqual(set(sut.STEP_SLUGS), names)


class WatchdogTest(unittest.TestCase):
    def run_sut(self, fetched):
        opts = sut.parse_args(["watchdog", "--dry-run", "--latest-url", "https://example.invalid/latest.json"])
        with mock.patch.object(sut, "fetch", side_effect=fetched), mock.patch.object(sut.time, "sleep"):
            return sut.watchdog(opts, NOW)

    def test_reports_the_heartbeat_and_the_published_build(self):
        latest = {"latest": "2026-10-12", "updated_at": "2026-10-12T06:45:24Z"}

        samples, log = self.run_sut([json.dumps(latest).encode()])

        self.assertEqual(samples, [
            ("geocoder_watchdog_heartbeat_timestamp_seconds", {}, NOW),
            ("geocoder_published_build_timestamp_seconds", {}, NOW - 7 * 3600),
            ("geocoder_published_updated_timestamp_seconds", {}, NOW - 15 * 60 + 24),
        ])
        self.assertEqual(log.records, [{"_msg": "published build 2026-10-12, updated 2026-10-12T06:45:24Z",
                                        "level": "info", "app": "geocoder-watchdog", **IDENTITY}])

    def test_reports_the_heartbeat_alone_when_latest_json_is_unreadable(self):
        for fetched in ([OSError("timed out")] * (sut.HTTP_RETRIES + 1), [b"<html>"], [b'{"latest": "2026-10-12"}']):
            with self.subTest(fetched=fetched[0]):
                samples, log = self.run_sut(fetched)

                self.assertEqual(samples, [("geocoder_watchdog_heartbeat_timestamp_seconds", {}, NOW)])
                self.assertEqual([(r["app"], r["level"]) for r in log.records], [("geocoder-watchdog", "error")])


class Gateway(http.server.BaseHTTPRequestHandler):
    """o11y's gateway: records each POST and answers with the server's next
    status, 204 once they run out."""

    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        self.server.requests.append((self.path, self.headers["Content-Type"], self.headers["Authorization"], body))
        status = self.server.statuses.pop(0) if self.server.statuses else 204
        reply = b"cannot parse line" if status >= 400 else b""
        self.send_response(status)
        self.send_header("Content-Length", str(len(reply)))
        self.end_headers()
        self.wfile.write(reply)

    def log_message(self, *args):
        pass


class GatewayTest(unittest.TestCase):
    def setUp(self):
        self.server = http.server.HTTPServer(("127.0.0.1", 0), Gateway)
        self.server.requests, self.server.statuses = [], []
        # Polled briefly, so shutdown() doesn't wait out the default half second.
        threading.Thread(target=self.server.serve_forever, args=(0.01,), daemon=True).start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        sleep = mock.patch.object(sut.time, "sleep")
        self.sleep = sleep.start()
        self.addCleanup(sleep.stop)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()


class PostTest(GatewayTest):
    def post(self, body):
        return sut.with_retries(lambda: sut.post(self.base_url + sut.METRICS_PATH, body, "text/plain", "s3cret"))

    def test_posts_the_body_with_its_type_and_the_bearer_token(self):
        self.assertEqual(self.post(b"geocoder_patch_bytes_sum 1 1\n"), (None, None))
        self.assertEqual(self.server.requests, [("/insert/0/prometheus/api/v1/import/prometheus", "text/plain",
                                                 "Bearer s3cret", b"geocoder_patch_bytes_sum 1 1\n")])

    def test_retries_a_server_error(self):
        self.server.statuses = [503]

        self.assertEqual(self.post(b"x 1 1\n"), (None, None))
        self.assertEqual(len(self.server.requests), 2)

    def test_gives_up_after_the_last_retry_with_its_error(self):
        self.server.statuses = [502] * (sut.HTTP_RETRIES + 1)

        self.assertEqual(self.post(b"x 1 1\n"), (None, "HTTP 502: cannot parse line"))
        self.assertEqual(len(self.server.requests), sut.HTTP_RETRIES + 1)

    def test_does_not_retry_a_rejected_request(self):
        for status in (400, 401):
            with self.subTest(status=status):
                self.server.requests, self.server.statuses = [], [status]

                self.assertEqual(self.post(b"x 1 1\n"), (None, f"HTTP {status}: cannot parse line"))
                self.assertEqual(len(self.server.requests), 1)
                self.sleep.assert_not_called()


class MainTest(GatewayTest, WorkDirTest):
    def setUp(self):
        GatewayTest.setUp(self)
        WorkDirTest.setUp(self)
        write(self.path("build.log"), "".join(f"build line {i}\n" for i in range(5)))

    def tearDown(self):
        WorkDirTest.tearDown(self)
        GatewayTest.tearDown(self)

    def run_main(self, *args, token="s3cret"):
        stdout = io.StringIO()
        with mock.patch.dict(os.environ), mock.patch.object(sys, "stdout", stdout):
            os.environ.pop(sut.TOKEN_ENV, None)
            if token:
                os.environ[sut.TOKEN_ENV] = token
            sut.main(["nightly", "--job-status", "success", "--trigger", "schedule", "--runner", "pokedex-giant",
                      "--build-log", self.path("build.log"), "--run-id", "7",
                      "--run-url", "https://example.invalid/runs/7", *args])
        return stdout.getvalue()

    def requests_to(self, path):
        return [request for request in self.server.requests if request[0] == path]

    def sent_records(self):
        return [json.loads(line) for _, _, _, body in self.requests_to("/insert/jsonline") for line in body.splitlines()]

    def test_pushes_the_metrics_and_the_log_lines_with_the_bearer_token_and_the_identity(self):
        out = self.run_main("--base-url", self.base_url + "/")

        [(_, metrics_type, metrics_auth, metrics)] = self.requests_to("/insert/0/prometheus/api/v1/import/prometheus")
        self.assertEqual((metrics_type, metrics_auth), ("text/plain", "Bearer s3cret"))
        self.assertEqual([line.split(b"{")[0] for line in metrics.splitlines()],
                         [b"geocoder_build_last_run_timestamp_seconds", b"geocoder_build_success"])
        self.assertTrue(all(IDENTITY_LABELS.encode() + b"} " in line for line in metrics.splitlines()), metrics)
        self.assertEqual({(type_, auth) for _, type_, auth, _ in self.requests_to("/insert/jsonline")},
                         {("application/stream+json", "Bearer s3cret")})
        records = self.sent_records()
        self.assertEqual([r["_msg"] for r in records], [f"build line {i}" for i in range(5)] + [
            "run success, schedule on pokedex-giant: https://example.invalid/runs/7"])
        for record in records:
            self.assertEqual({key: record[key] for key in IDENTITY}, IDENTITY)
            self.assertEqual((record["app"], record["run_id"]), ("geocoder-build", "7"))
            self.assertRegex(record["_time"], r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$")
        self.assertIn("Pushed 2 samples to o11y", out)
        self.assertIn("Pushed 6 log lines to o11y", out)
        self.assertNotIn("s3cret", out)

    def test_sends_the_log_lines_in_batches(self):
        with mock.patch.object(sut, "LOG_BATCH_LINES", 4):
            self.run_main("--base-url", self.base_url)

        self.assertEqual([body.count(b"\n") for _, _, _, body in self.requests_to("/insert/jsonline")], [4, 2])
        self.assertEqual(len(self.sent_records()), 6)

    def test_without_a_token_sends_nothing_and_warns(self):
        for token in ("", "  "):
            with self.subTest(token=token):
                out = self.run_main("--base-url", self.base_url, token=token)

                self.assertEqual(self.server.requests, [])
                self.assertIn("::warning::No $O11Y_TOKEN: nothing reported to o11y", out)

    def test_a_rejected_metrics_push_is_not_retried_and_is_logged(self):
        self.server.statuses = [400]

        out = self.run_main("--base-url", self.base_url)

        self.assertEqual(len(self.requests_to(sut.METRICS_PATH)), 1)
        self.sleep.assert_not_called()
        self.assertIn("::warning::o11y metrics push failed: HTTP 400: cannot parse line", out)
        last = self.sent_records()[-1]
        self.assertEqual((last["_msg"], last["level"]), ("metrics push failed: HTTP 400: cannot parse line", "error"))

    def test_stops_the_log_lines_at_the_first_rejected_batch(self):
        self.server.statuses = [204, 204, 401]

        with mock.patch.object(sut, "LOG_BATCH_LINES", 2):
            out = self.run_main("--base-url", self.base_url)

        self.assertEqual(len(self.requests_to(sut.LOGS_PATH)), 2)
        self.sleep.assert_not_called()
        self.assertIn("::warning::o11y logs push failed after 2 of 6 lines: HTTP 401: cannot parse line", out)

    def test_an_unreachable_gateway_is_warned_and_not_raised(self):
        out = self.run_main("--base-url", "http://127.0.0.1:9")

        self.assertIn("::warning::o11y metrics push failed: ", out)
        self.assertIn("::warning::o11y logs push failed after 0 of 7 lines: ", out)

    def test_dry_run_prints_the_metrics_then_the_log_lines_and_sends_nothing(self):
        out = self.run_main("--dry-run", "--base-url", self.base_url).splitlines()

        run = f'{{trigger="schedule",runner="pokedex-giant",{IDENTITY_LABELS}}}'
        self.assertTrue(out[0].startswith(f"geocoder_build_last_run_timestamp_seconds{run} "), out[0])
        self.assertTrue(out[1].startswith(f"geocoder_build_success{run} 1 "), out[1])
        self.assertEqual([json.loads(line)["_msg"] for line in out[2:]], [f"build line {i}" for i in range(5)] + [
            "run success, schedule on pokedex-giant: https://example.invalid/runs/7"])
        self.assertEqual(self.server.requests, [])

    def test_needs_a_base_url_or_a_dry_run(self):
        with mock.patch.object(sys, "stderr", io.StringIO()), self.assertRaises(SystemExit):
            self.run_main()


if __name__ == "__main__":
    unittest.main()
