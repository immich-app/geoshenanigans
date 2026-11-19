#!/usr/bin/env python3
"""Tests for tiles_sync_report.py. Run: python3 -m unittest discover -s deployment/scripts -p 'test_*.py'"""
import contextlib
import gzip
import http.server
import io
import json
import os
import struct
import sys
import tempfile
import threading
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import tiles_sync_report as sut  # noqa: E402

NOW = 1795435200.5  # 2026-11-23T12:00:00.5Z
IDENTITY = {"project": "geoshenanigans", "cluster": "tiles", "env": "prod", "provider": "github", "region": "global"}
IDENTITY_LABELS = 'project="geoshenanigans",cluster="tiles",env="prod",provider="github",region="global"'

REPORT = {
    "status": "success", "build": "20261118", "previousBuild": "20261117", "osmDate": "2026-11-18T04:00:00Z",
    "positionsTotal": 1431655765, "positionsChanged": 327826, "changedFraction": 0.000229,
    "filesUploaded": 327826, "bytesUploaded": 2600000000, "retries": 2, "rateLimited": 0,
    "replicationComplete": True, "replicationWaitSeconds": {"UK": 310, "SYD": 1450},
    "stepSeconds": {"manifest": 540, "diff": 120, "upload": 40, "replication": 1500, "static": 2, "purge": 1,
                    "state": 30},
    "error": None,
}


LIVE_STATUS = {"liveBuild": "20261117", "updatedAt": "2026-11-17T11:02:00Z", "osmDate": "2026-11-17T04:00:00Z",
               "buildAgeHours": 30.1, "osmAgeHours": 40.2}
NO_STATE = {"liveBuild": None, "updatedAt": None, "osmDate": None, "buildAgeHours": None, "osmAgeHours": None}
MD5_A = "1B2M2Y8AsgTpgAmY7PhCfg=="


def build(key, version="4.15.2"):
    return {"key": f"{key}.pmtiles", "size": 139_600_000_000, "md5sum": MD5_A, "b3sum": "x",
            "uploaded": "2026-11-18T09:05:00Z", "version": version}


BUILDS = [build("20261112", "4.15.0"), build("20261116"), build("20261117"), build("20261118")]


def by_name(samples):
    """{name: {sorted label items: value}}."""
    found = {}
    for name, labels, value in samples:
        found.setdefault(name, {})[tuple(sorted(labels.items()))] = value
    return found


def sync_args(*extra):
    return sut.parse_args(["sync", "--dry-run", "--build", "20261118", "--live", "20261117", *extra])


class WithTempDir(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, name, text):
        path = os.path.join(self.tmp.name, name)
        with open(path, "w") as f:
            f.write(text)
        return path


class SyncTest(WithTempDir):
    def run_sync(self, *extra, report=None):
        args = list(extra)
        if report is not None:
            args += ["--report", self.write("report.json", report if isinstance(report, str) else json.dumps(report))]
        return sut.sync(sync_args(*args), NOW)

    def test_a_successful_run_sends_the_reports_numbers_and_the_workflows_steps(self):
        samples, log = self.run_sync("--sync-result", "success", "--download-seconds", "312.5",
                                     "--started-at", "1795430000", "--finished-at", "1795433600", report=REPORT)

        metrics = by_name(samples)
        self.assertEqual(metrics["tiles_sync_runs"], {(("status", "success"),): 1})
        self.assertEqual(metrics["tiles_sync_last_success_timestamp_seconds"], {(): int(NOW)})
        self.assertEqual(metrics["tiles_sync_positions_changed"], {(): 327826})
        self.assertEqual(metrics["tiles_sync_changed_fraction"], {(): 0.000229})
        self.assertEqual(metrics["tiles_sync_files_uploaded"], {(): 327826})
        self.assertEqual(metrics["tiles_sync_bytes_uploaded"], {(): 2600000000})
        self.assertEqual(metrics["tiles_sync_retries"], {(): 2})
        self.assertEqual(metrics["tiles_sync_rate_limited"], {(): 0})
        self.assertEqual(metrics["tiles_sync_replication_complete"], {(): 1})
        self.assertEqual(metrics["tiles_sync_replication_wait_seconds"],
                         {(("storage_region", "SYD"),): 1450, (("storage_region", "UK"),): 310})
        self.assertEqual(metrics["tiles_sync_step_seconds"], {
            (("step", "manifest"),): 540, (("step", "diff"),): 120, (("step", "upload"),): 40,
            (("step", "replication"),): 1500, (("step", "static"),): 2, (("step", "purge"),): 1,
            (("step", "state"),): 30, (("step", "download"),): 312.5, (("step", "total"),): 3600})
        self.assertEqual([r["level"] for r in log.records], ["info"])
        msg = log.records[0]["_msg"]
        for part in ("tiles sync success: 20261118", "OSM 2026-11-18T04:00:00Z", "live was 20261117",
                     "327,826 of 1,431,655,765 positions changed (0.0229%)", "327,826 files, 2.60 GB uploaded",
                     "2 retries, 0 rate limited", "replication complete, slowest SYD 1,450s",
                     "steps download 312s, manifest 540s, diff 120s", "state 30s, total 3,600s"):
            self.assertIn(part, msg)
        self.assertEqual(log.records[0]["build"], "20261118")
        self.assertEqual(log.records[0]["previous_build"], "20261117")
        self.assertEqual(log.records[0]["status"], "success")

    def test_null_fields_send_nothing(self):
        report = {**REPORT, "status": "failed", "filesUploaded": None, "bytesUploaded": None,
                  "replicationComplete": None, "replicationWaitSeconds": None,
                  "stepSeconds": {"manifest": 540, "diff": None}, "error": "diff: live manifest unreadable"}

        samples, log = self.run_sync("--sync-result", "failure", report=report)

        metrics = by_name(samples)
        self.assertEqual(metrics["tiles_sync_runs"], {(("status", "failed"),): 1})
        for name in ("tiles_sync_last_success_timestamp_seconds", "tiles_sync_files_uploaded",
                     "tiles_sync_bytes_uploaded", "tiles_sync_replication_complete",
                     "tiles_sync_replication_wait_seconds"):
            self.assertNotIn(name, metrics)
        self.assertEqual(metrics["tiles_sync_step_seconds"], {(("step", "manifest"),): 540})
        self.assertEqual([(r["level"], r["_msg"]) for r in log.records][1],
                         ("error", "tiles sync error: diff: live manifest unreadable"))
        self.assertEqual(log.records[0]["level"], "error")

    def test_an_incomplete_replication_sends_a_zero(self):
        samples, log = self.run_sync(report={**REPORT, "status": "failed", "replicationComplete": False,
                                             "replicationWaitSeconds": {"UK": 300, "SYD": None},
                                             "error": "replication gate timed out"})

        metrics = by_name(samples)
        self.assertEqual(metrics["tiles_sync_replication_complete"], {(): 0})
        self.assertEqual(metrics["tiles_sync_replication_wait_seconds"], {(("storage_region", "UK"),): 300})
        # The region that never caught up comes first, not only the slowest one that did.
        self.assertIn("replication INCOMPLETE, never in SYD, slowest UK 300s", log.records[0]["_msg"])

    def test_an_incomplete_replication_names_the_files_still_missing_a_region(self):
        stragglers = [{"path": f"v1/15/{i}/7.mvt", "regions": ["SYD", "BR"]} for i in range(7)]
        _, log = self.run_sync(report={**REPORT, "status": "failed", "replicationComplete": False,
                                       "replicationWaitSeconds": {"UK": 300, "SYD": None, "BR": None},
                                       "replicationStragglers": stragglers, "error": "replication incomplete"})

        msg = log.records[0]["_msg"]
        self.assertIn("never in BR, SYD", msg)
        self.assertIn("still not replicated: v1/15/0/7.mvt (not in SYD, BR), v1/15/1/7.mvt (not in SYD, BR)", msg)
        self.assertIn("v1/15/4/7.mvt (not in SYD, BR) and 2 more", msg)

    def test_the_guard_rail_keeps_its_status_and_counts(self):
        samples, _ = self.run_sync(report={**REPORT, "status": "guard_rail", "positionsChanged": 90000000,
                                           "changedFraction": 0.063, "filesUploaded": 0})

        metrics = by_name(samples)
        self.assertEqual(metrics["tiles_sync_runs"], {(("status", "guard_rail"),): 1})
        self.assertEqual(metrics["tiles_sync_positions_changed"], {(): 90000000})
        self.assertNotIn("tiles_sync_last_success_timestamp_seconds", metrics)

    def test_a_skip_the_cli_found_counts_as_a_success(self):
        samples, _ = self.run_sync(report={**REPORT, "status": "skipped", "positionsChanged": None})

        metrics = by_name(samples)
        self.assertEqual(metrics["tiles_sync_runs"], {(("status", "skipped"),): 1})
        self.assertEqual(metrics["tiles_sync_last_success_timestamp_seconds"], {(): int(NOW)})

    def test_the_workflows_own_skip_counts_as_a_success(self):
        samples, log = self.run_sync("--resolved-status", "skipped", "--resolve-result", "success",
                                     "--sync-result", "skipped", "--version", "4.15.2")

        self.assertEqual(by_name(samples), {"tiles_sync_runs": {(("status", "skipped"),): 1},
                                            "tiles_sync_last_success_timestamp_seconds": {(): int(NOW)}})
        self.assertEqual([(r["level"], r["_msg"]) for r in log.records],
                         [("info", "tiles sync skipped: 20261118 (tiles 4.15.2), live was 20261117")])

    def test_a_schema_change_stops_the_run_and_says_what_changed(self):
        samples, log = self.run_sync("--resolved-status", "schema_changed", "--resolve-result", "failure",
                                     "--sync-result", "skipped", "--version", "4.16.0", "--live-version", "4.15.2")

        self.assertEqual(by_name(samples), {"tiles_sync_runs": {(("status", "schema_changed"),): 1}})
        self.assertEqual([(r["level"], r["_msg"]) for r in log.records], [
            ("error", "tiles sync schema_changed: 20261118 (tiles 4.16.0), live was 20261117 (tiles 4.15.2)"),
            ("error", "tiles sync error: the build's tiles schema 4.16.0 differs from the live build's 4.15.2 in"
                      " major.minor: seed it under a new prefix, or dispatch Tiles Sync with force to sync it in"
                      " place")])

    def test_an_unknown_resolved_status_is_ignored(self):
        samples, log = self.run_sync("--resolved-status", "stop", "--resolve-result", "failure")

        self.assertEqual(by_name(samples)["tiles_sync_runs"], {(("status", "failed"),): 1})
        self.assertIn("resolving the build ended failure", log.records[-1]["_msg"])

    def test_a_run_without_a_report_failed_and_says_where(self):
        cases = [
            (["--resolve-result", "failure"], "resolving the build ended failure"),
            (["--resolve-result", "success", "--sync-result", "cancelled"], "cancelled or timed out"),
            (["--resolve-result", "success", "--sync-result", "failure", "--download-outcome", "failure"],
             "MD5 did not match"),
            (["--resolve-result", "success", "--sync-result", "failure", "--download-outcome", "success"],
             "ended failure without a sync report"),
            (["--resolve-result", "success", "--sync-result", ""], "the sync job did not run"),
        ]
        for args, reason in cases:
            with self.subTest(args=args):
                samples, log = self.run_sync(*args)

                self.assertEqual(by_name(samples)["tiles_sync_runs"], {(("status", "failed"),): 1})
                self.assertEqual(log.records[-1]["level"], "error")
                self.assertIn(reason, log.records[-1]["_msg"])

    def test_an_unreadable_report_is_a_failure(self):
        for text in ("", "{not json", "[1, 2]"):
            with self.subTest(text=text):
                samples, log = self.run_sync("--resolve-result", "success", "--sync-result", "failure",
                                             "--download-outcome", "success", report=text)

                self.assertEqual(by_name(samples)["tiles_sync_runs"], {(("status", "failed"),): 1})
                self.assertIn("without a sync report", log.records[-1]["_msg"])

    def test_a_report_with_an_unknown_status_is_a_failure(self):
        samples, log = self.run_sync(report={**REPORT, "status": "done"})

        self.assertEqual(by_name(samples)["tiles_sync_runs"], {(("status", "failed"),): 1})
        self.assertIn("no known status ('done')", log.records[-1]["_msg"])

    def test_the_total_needs_both_ends_of_the_sync_job(self):
        samples, _ = self.run_sync("--started-at", "1795430000", "--finished-at", "", report=REPORT)

        self.assertNotIn((("step", "total"),), by_name(samples)["tiles_sync_step_seconds"])

    def test_no_state_is_neither_a_success_nor_a_failure(self):
        samples, log = self.run_sync("--resolved-status", "no_state", "--resolve-result", "success",
                                     "--sync-result", "skipped", "--resolved-message", "No build is live yet")

        self.assertEqual(by_name(samples), {"tiles_sync_runs": {(("status", "no_state"),): 1}})
        self.assertEqual([(r["level"], r["_msg"]) for r in log.records], [
            ("warn", "tiles sync no_state: 20261118, live was 20261117"),
            ("warn", "tiles sync no_state: No build is live yet")])

    def test_the_sync_jobs_own_skip_says_why(self):
        samples, log = self.run_sync("--resolved-status", "skipped", "--resolve-result", "success",
                                     "--sync-result", "success", "--resolved-message",
                                     "The live build is 20261119 now, not 20261117")

        self.assertEqual(by_name(samples)["tiles_sync_runs"], {(("status", "skipped"),): 1})
        self.assertEqual(log.records[-1]["_msg"], "tiles sync skipped: The live build is 20261119 now, not 20261117")

    def test_a_failure_before_the_cli_quotes_the_decisions_error(self):
        _, log = self.run_sync("--resolve-result", "failure", "--resolved-message",
                               "bunny:status could not read the live state (exit 1): HttpError: 401")

        self.assertEqual(log.records[-1]["_msg"], "tiles sync error: resolving the build ended failure; see the Resolve"
                                                  " build job: bunny:status could not read the live state (exit 1):"
                                                  " HttpError: 401")

    def test_a_failed_decision_check_says_so(self):
        samples, log = self.run_sync("--resolve-result", "success", "--sync-result", "failure",
                                     "--check-outcome", "failure", "--download-outcome", "skipped")

        self.assertEqual(by_name(samples)["tiles_sync_runs"], {(("status", "failed"),): 1})
        self.assertIn("re-reading the live build before the download failed", log.records[-1]["_msg"])

    def test_the_run_url_and_context_go_on_the_line(self):
        _, log = self.run_sync("--run-id", "42", "--run-attempt", "2", "--trigger", "schedule",
                               "--run-url", "https://example.invalid/runs/42", report=REPORT)

        record = log.records[0]
        self.assertTrue(record["_msg"].endswith(": https://example.invalid/runs/42"))
        self.assertEqual((record["app"], record["run_id"], record["run_attempt"], record["trigger"]),
                         ("tiles-sync", "42", "2", "schedule"))
        self.assertEqual({k: record[k] for k in IDENTITY}, IDENTITY)


class WatchdogTest(WithTempDir):
    STATUS = {"liveBuild": "20261118", "updatedAt": "2026-11-18T11:02:00Z", "osmDate": "2026-11-18T04:00:00Z",
              "buildAgeHours": 30.1, "osmAgeHours": 40.2}

    def run_watchdog(self, status, stderr="", status_exit=""):
        opts = sut.parse_args(["watchdog", "--dry-run", "--status", self.write("status.json", status),
                               "--stderr", self.write("status.err", stderr), "--status-exit", status_exit])
        return sut.watchdog(opts, NOW)

    def test_sends_the_heartbeat_and_the_live_builds_ages(self):
        samples, log = self.run_watchdog(json.dumps(self.STATUS))

        self.assertEqual(by_name(samples), {
            "tiles_sync_watchdog_heartbeat": {(): int(NOW)},
            "tiles_sync_live_build_age_hours": {(): 30.1},
            "tiles_sync_live_osm_age_hours": {(): 40.2},
        })
        self.assertEqual([(r["level"], r["_msg"]) for r in log.records], [
            ("info", "live build 20261118 (OSM 2026-11-18T04:00:00Z), updated 2026-11-18T11:02:00Z:"
                     " build 30.1h old, OSM data 40.2h old")])
        self.assertEqual(log.records[0]["app"], "tiles-sync-watchdog")

    def test_an_unknown_osm_date_leaves_its_age_out(self):
        samples, log = self.run_watchdog(json.dumps({**self.STATUS, "osmDate": None, "osmAgeHours": None}))

        self.assertNotIn("tiles_sync_live_osm_age_hours", by_name(samples))
        self.assertIn("tiles_sync_watchdog_heartbeat", by_name(samples))
        self.assertIn("OSM data unknown old", log.records[0]["_msg"])

    def test_an_unreadable_state_sends_no_heartbeat_and_logs_why(self):
        for status in ("", "{", json.dumps({"liveBuild": "latest"}), json.dumps({"updatedAt": None}), "[]"):
            with self.subTest(status=status):
                samples, log = self.run_watchdog(status, stderr="HttpError: GET state.json: 401 Unauthorized\n")

                self.assertEqual(samples, [])
                self.assertEqual([(r["level"], r["_msg"]) for r in log.records], [
                    ("error", "could not read the live tiles state with bunny:status:"
                              " HttpError: GET state.json: 401 Unauthorized")])

    def test_before_the_first_load_it_sends_the_heartbeat_without_ages(self):
        # bunny:status prints nulls and exits 1 when the state zone has no state.json.
        samples, log = self.run_watchdog(json.dumps(NO_STATE), "No state.json in the state zone\n", "1")

        self.assertEqual(by_name(samples), {"tiles_sync_watchdog_heartbeat": {(): int(NOW)}})
        self.assertEqual([r["level"] for r in log.records], ["warn"])
        self.assertIn("no build is live yet", log.records[0]["_msg"])

    def test_a_live_build_needs_a_clean_exit(self):
        samples, log = self.run_watchdog(json.dumps(self.STATUS), "boom\n", "1")

        self.assertEqual(samples, [])
        self.assertEqual(log.records[0]["level"], "error")


# The measured rates the budgets are sized from (SPEC "Bunny facts"), made slower for headroom: PUT-only uploads peaked
# at 12,838 files/s with ~1,500 in flight, a DESCRIBE takes ~120 ms and a PUT ~115 ms (the sync DESCRIBEs every file
# before its PUT, both in one slot), and Sydney, the slowest region, replicated 1.3-1.5k files/s.
POSITIONS = 1_431_655_765
UPLOAD_PEAK_FILES_PER_SECOND = 11_500
DESCRIBE_THEN_PUT_SECONDS = 0.120 + 0.115
SLOWEST_REPLICATION_FILES_PER_SECOND = 1_300 * 0.8
MANIFEST_AND_DIFF_SECONDS = 20 * 60
# The TileJSON and styles' gate gets 10 minutes at least, after the tiles' gate.
STATIC_GATE_SECONDS = 10 * 60
STATIC_PURGE_STATE_SECONDS = 10 * 60
# The sync job's steps before Sync: checkout, mise, npm ci and the decision check, then the download step's timeout.
SETUP_SECONDS = 10 * 60
DOWNLOAD_TIMEOUT_SECONDS = 60 * 60


class SampleTest(WithTempDir):
    CHECK = {"mode": "sample-per-zoom", "prefix": "v1", "build": "20261118", "checked": 1616, "missing": 1,
             "mismatched": 0, "errors": 0, "lacking": {"UK": 0, "SYD": 12}, "missingPaths": ["v1/15/1/2.mvt"],
             "mismatchedPaths": []}

    def run_sample(self, report, exit_code="1", stderr=""):
        opts = sut.parse_args(["sample", "--dry-run", "--report", self.write("check.json", report),
                               "--exit", exit_code, "--stderr", self.write("check.err", stderr)])
        return sut.sample(opts, NOW)

    def test_sends_the_counts_per_region_and_names_examples(self):
        samples, log = self.run_sample(json.dumps(self.CHECK))

        self.assertEqual(by_name(samples), {
            "tiles_sync_sample_runs": {(("result", "ok"),): 1},
            "tiles_sync_sample_checked": {(): 1616},
            "tiles_sync_sample_missing": {(): 1},
            "tiles_sync_sample_mismatched": {(): 0},
            "tiles_sync_sample_errors": {(): 0},
            "tiles_sync_sample_lacking": {(("storage_region", "SYD"),): 12, (("storage_region", "UK"),): 0},
        })
        self.assertEqual([(r["level"], r["_msg"]) for r in log.records], [
            ("error", "tiles sample check of 20261118 under v1/: 1,616 positions, 1 missing, 0 with other bytes,"
                      " 0 DESCRIBE errors; not yet in a region: SYD 12; e.g. v1/15/1/2.mvt")])
        self.assertEqual(log.records[0]["app"], "tiles-sample-check")

    def test_only_unreplicated_files_warn(self):
        _, log = self.run_sample(json.dumps({**self.CHECK, "missing": 0, "missingPaths": []}), exit_code="2")
        self.assertEqual(log.records[0]["level"], "warn")
        _, log = self.run_sample(json.dumps({**self.CHECK, "missing": 0, "missingPaths": [], "lacking": {}}), "0")
        self.assertEqual(log.records[0]["level"], "info")

    def test_a_check_whose_describes_all_failed_is_an_error_not_a_clean_sample(self):
        # Bunny storage down: every position counted as checked and as an error, none missing or mismatched.
        report = {**self.CHECK, "missing": 0, "missingPaths": [], "errors": 1616, "lacking": {"UK": 0, "SYD": 0},
                  "errorPaths": ["v1/0/0/0.mvt", "v1/1/0/0.mvt", "v1/1/0/1.mvt", "v1/1/1/0.mvt"]}
        samples, log = self.run_sample(json.dumps(report), exit_code="1")

        self.assertEqual(by_name(samples), {
            "tiles_sync_sample_runs": {(("result", "error"),): 1},
            "tiles_sync_sample_checked": {(): 1616},
            "tiles_sync_sample_missing": {(): 0},
            "tiles_sync_sample_mismatched": {(): 0},
            "tiles_sync_sample_errors": {(): 1616},
            "tiles_sync_sample_lacking": {(("storage_region", "SYD"),): 0, (("storage_region", "UK"),): 0},
        })
        self.assertEqual([(r["level"], r["_msg"]) for r in log.records], [
            ("error", "tiles sample check of 20261118 under v1/: 1,616 positions, 0 missing, 0 with other bytes,"
                      " 1,616 DESCRIBE errors; not yet in a region: none; incomplete: 1,616 of the 1,616 positions"
                      " went unchecked, their DESCRIBE failing after its retries (e.g. v1/0/0/0.mvt, v1/1/0/0.mvt,"
                      " v1/1/0/1.mvt)")])

    def test_a_check_with_some_describe_errors_is_an_error_that_keeps_what_it_found(self):
        samples, log = self.run_sample(json.dumps({**self.CHECK, "errors": 2}), exit_code="1")

        found = by_name(samples)
        self.assertEqual(found["tiles_sync_sample_runs"], {(("result", "error"),): 1})
        self.assertEqual(found["tiles_sync_sample_missing"], {(): 1})
        self.assertEqual(found["tiles_sync_sample_errors"], {(): 2})
        self.assertEqual(found["tiles_sync_sample_lacking"][(("storage_region", "SYD"),)], 12)
        self.assertEqual(log.records[0]["level"], "error")
        self.assertIn("incomplete: 2 of the 1,616 positions went unchecked", log.records[0]["_msg"])

    def test_a_check_skipped_while_builds_are_pending_sends_no_counts(self):
        samples, log = self.run_sample(json.dumps({"skipped": "pending builds 20261119: a sync has to finish first"}),
                                       exit_code="0")
        self.assertEqual(by_name(samples), {"tiles_sync_sample_runs": {(("result", "skipped"),): 1}})
        self.assertEqual(log.records[0]["level"], "info")

    def test_a_check_that_could_not_run_is_an_error(self):
        for report, exit_code in (("", "1"), ("{", "1"), (json.dumps(self.CHECK), "3"), (json.dumps(self.CHECK), "")):
            with self.subTest(report=report, exit_code=exit_code):
                samples, log = self.run_sample(report, exit_code, "No state.json in the state zone\n")
                self.assertEqual(by_name(samples), {"tiles_sync_sample_runs": {(("result", "error"),): 1}})
                self.assertEqual(log.records[0]["level"], "error")
                self.assertIn("No state.json in the state zone", log.records[0]["_msg"])


class BudgetTest(unittest.TestCase):
    def test_each_budget_finishes_the_largest_diff_its_guard_rail_lets_through(self):
        for budget in (sut.DAILY, sut.FORCED):
            with self.subTest(budget=budget):
                files = budget.max_changed_fraction * POSITIONS
                upload = files / min(UPLOAD_PEAK_FILES_PER_SECOND, budget.concurrency / DESCRIBE_THEN_PUT_SECONDS)
                # Replication starts with the first PUT; the gate waits from the end of the upload.
                gate = files / SLOWEST_REPLICATION_FILES_PER_SECOND - upload

                self.assertLessEqual(gate, budget.replication_timeout_min * 60)
                # The Sync step outlasts a gate that times out, so the CLI always writes its report.
                self.assertLessEqual(MANIFEST_AND_DIFF_SECONDS + upload + budget.replication_timeout_min * 60
                                     + STATIC_GATE_SECONDS + STATIC_PURGE_STATE_SECONDS, budget.sync_timeout_min * 60)
                # Within Bunny's 250 connections per zone per storage server, over the 5 servers seen.
                self.assertLessEqual(budget.concurrency, 5 * 240)
                self.assertLessEqual(SETUP_SECONDS + DOWNLOAD_TIMEOUT_SECONDS + budget.sync_timeout_min * 60,
                                     budget.job_timeout_min * 60)

    def test_a_day_and_a_week_of_changes_fit_the_daily_guard_rail(self):
        # Measured: 327,826 positions in one day, 1.67M in 8 days, 1.90M in 9 days.
        self.assertGreater(sut.DAILY.max_changed_fraction * POSITIONS, 1_900_000 * 3)
        self.assertGreater(sut.FORCED.max_changed_fraction, sut.DAILY.max_changed_fraction)


class DecideTest(unittest.TestCase):
    def decide(self, status=LIVE_STATUS, status_exit=0, builds=BUILDS, **kwargs):
        return sut.decide(builds, status, status_exit, "", **kwargs)

    def test_syncs_the_newest_build_with_the_daily_budget(self):
        outputs, notes = self.decide()

        self.assertEqual(outputs, {
            "action": "sync", "status": "", "message": "", "key": "20261118", "md5": MD5_A, "version": "4.15.2",
            "live": "20261117", "live_version": "4.15.2", "max_changed_fraction": 0.005,
            "replication_timeout_min": 120, "sync_timeout_min": 270, "job_timeout_min": 360, "concurrency": 1200})
        self.assertEqual(notes, [])

    def test_skips_a_live_build(self):
        outputs, notes = self.decide(status={**LIVE_STATUS, "liveBuild": "20261118", "pending": []})

        self.assertEqual((outputs["action"], outputs["status"]), ("skip", "skipped"))
        self.assertEqual(notes, [("notice", "20261118 is live already")])

    def test_syncs_the_live_build_again_to_revert_a_pending_one(self):
        for event in ("schedule", "workflow_dispatch"):
            with self.subTest(event=event):
                outputs, notes = self.decide(status={**LIVE_STATUS, "pending": ["20261118"]},
                                             requested="20261117" if event != "schedule" else "", event=event,
                                             builds=BUILDS[:3])

                self.assertEqual((outputs["action"], outputs["key"], outputs["live"]),
                                 ("sync", "20261117", "20261117"))
                self.assertEqual(notes, [("warning", "20261117 is live with 20261118 pending: syncing 20261117"
                                                     " again puts its bytes back where they wrote, then purges")])

    def test_before_the_first_load_it_skips_without_failing(self):
        outputs, notes = self.decide(status=NO_STATE, status_exit=1)

        self.assertEqual((outputs["action"], outputs["status"], outputs["live"]), ("skip", "no_state", ""))
        self.assertIn("no state.json", outputs["message"])
        self.assertEqual(notes[0][0], "notice")

    def test_an_unreadable_state_fails(self):
        for status, code in ((None, 1), ({}, 1), (LIVE_STATUS, 1), ({**LIVE_STATUS, "liveBuild": "x"}, 0)):
            with self.subTest(status=status, code=code):
                with self.assertRaisesRegex(sut.Unreadable, "could not read the live state"):
                    sut.decide(BUILDS, status, code, "HttpError: 401")

    def test_a_hold_stops_scheduled_runs_only(self):
        outputs, notes = self.decide(hold="rolled back 20261118, bad labels", event="schedule")

        self.assertEqual((outputs["action"], outputs["status"]), ("skip", "skipped"))
        self.assertIn("held (TILES_SYNC_HOLD: rolled back 20261118, bad labels)", outputs["message"])

        outputs, _ = self.decide(hold="rolled back", event="workflow_dispatch")
        self.assertEqual(outputs["action"], "sync")

    def test_a_rollback_warns_that_the_schedule_undoes_it_without_a_hold(self):
        outputs, notes = self.decide(requested="20261116", event="workflow_dispatch")

        self.assertEqual((outputs["action"], outputs["key"]), ("sync", "20261116"))
        self.assertEqual(notes[0][0], "warning")
        self.assertIn("Set the repository variable TILES_SYNC_HOLD", notes[0][1])

        _, notes = self.decide(requested="20261116", hold="bad 20261117", event="workflow_dispatch")
        self.assertIn("TILES_SYNC_HOLD holds the schedule", notes[0][1])

    def test_the_newest_build_older_than_the_live_one_is_skipped(self):
        outputs, notes = self.decide(builds=BUILDS[:2])

        self.assertEqual((outputs["action"], outputs["status"]), ("skip", "skipped"))
        self.assertEqual(notes[0][0], "warning")

    def test_a_schema_change_stops_unless_forced(self):
        builds = [*BUILDS[:3], build("20261118", "4.16.0")]
        outputs, notes = self.decide(builds=builds)

        self.assertEqual((outputs["action"], outputs["status"]), ("stop", "schema_changed"))
        self.assertEqual(notes[0][0], "error")

        outputs, notes = self.decide(builds=builds, force=True)
        self.assertEqual(outputs["action"], "sync")
        self.assertEqual({k: outputs[k] for k in sut.Budget._fields}, sut.FORCED._asdict())
        self.assertIn("syncing in place (force)", notes[0][1])

    def test_an_unknown_version_leaves_only_the_guard_rail(self):
        outputs, notes = self.decide(builds=[*BUILDS[:3], build("20261118", "v5")])

        self.assertEqual((outputs["action"], outputs["version"]), ("sync", ""))
        self.assertIn("only the diff's guard rail applies", notes[0][1])

    def test_rejects_a_build_it_cannot_download_safely(self):
        cases = {
            "YYYYMMDD key, got '2026-11-18'": dict(requested="2026-11-18"),
            "20261101 isn't in builds.json": dict(requested="20261101"),
            "no MD5 for 20261118": dict(builds=[*BUILDS[:3], {**build("20261118"), "md5sum": "abc"}]),
            "not a list": dict(builds={"key": "x"}),
            "got ''": dict(builds=[]),
        }
        for message, kwargs in cases.items():
            with self.subTest(message):
                with self.assertRaisesRegex(sut.Unreadable, message):
                    self.decide(**kwargs)


class RecheckTest(unittest.TestCase):
    def recheck(self, status=LIVE_STATUS, status_exit=0, **kwargs):
        return sut.recheck(status, status_exit, "", **{"live": "20261117", "build": "20261118", **kwargs})

    def test_proceeds_while_the_live_build_is_the_one_resolved_against(self):
        self.assertEqual(self.recheck(), ({"proceed": "true", "status": "", "message": ""}, []))

    def test_a_rerun_after_the_live_build_moved_skips(self):
        # Re-run failed jobs of an old run: its resolve outputs say 20261117 was live; 20261119 is now.
        outputs, notes = self.recheck(status={**LIVE_STATUS, "liveBuild": "20261119"})

        self.assertEqual((outputs["proceed"], outputs["status"]), ("false", "skipped"))
        self.assertIn("The live build is 20261119 now, not 20261117", outputs["message"])
        self.assertEqual(notes[0][0], "warning")

    def test_a_hold_set_since_stops_a_scheduled_run(self):
        outputs, _ = self.recheck(hold="rollback", event="schedule")
        self.assertEqual((outputs["proceed"], outputs["status"]), ("false", "skipped"))

        outputs, _ = self.recheck(hold="rollback", event="workflow_dispatch")
        self.assertEqual(outputs["proceed"], "true")

    def test_a_missing_state_stops(self):
        outputs, _ = self.recheck(status=NO_STATE, status_exit=1)
        self.assertEqual((outputs["proceed"], outputs["status"]), ("false", "no_state"))

    def test_an_unreadable_state_fails(self):
        with self.assertRaises(sut.Unreadable):
            self.recheck(status=None, status_exit=1)


class ExpositionTest(unittest.TestCase):
    def test_each_sample_carries_the_identity_labels_last_at_the_push_time(self):
        body = sut.exposition([("tiles_sync_runs", {"status": "success"}, 1),
                               ("tiles_sync_changed_fraction", {}, 0.000229)], 1795435200500)

        self.assertEqual(body, f'tiles_sync_runs{{status="success",{IDENTITY_LABELS}}} 1 1795435200500\n'
                               f"tiles_sync_changed_fraction{{{IDENTITY_LABELS}}} 0.000229 1795435200500\n")

    def test_a_label_cannot_shadow_an_identity_label(self):
        body = sut.exposition([("tiles_sync_runs", {"region": "UK", "cluster": "x"}, 1)], 1)

        self.assertEqual(body, 'tiles_sync_runs{region="global",cluster="tiles",project="geoshenanigans",'
                               'env="prod",provider="github"} 1 1\n')

    def test_whole_floats_are_written_as_integers(self):
        body = sut.exposition([("tiles_sync_step_seconds", {"step": "total"}, 3600.0),
                               ("tiles_sync_step_seconds", {"step": "download"}, 312.5)], 1)

        self.assertEqual([line.split()[1] for line in body.splitlines()], ["3600", "312.5"])

    def test_escapes_label_values(self):
        body = sut.exposition([("tiles_sync_step_seconds", {"step": 'a\\b "c"\nd'}, 1)], 1)

        self.assertTrue(body.startswith('tiles_sync_step_seconds{step="a\\\\b \\"c\\"\\nd",'))


class LogLinesTest(unittest.TestCase):
    def test_records_are_json_lines_a_microsecond_apart_with_identity_fields_that_cannot_be_shadowed(self):
        log = sut.Log(app="tiles-sync", cluster="other", run_id="7")
        log.add("one")
        log.add("two\nlines", level="error")

        lines = [json.loads(line) for line in sut.log_lines(log.records, NOW).splitlines()]

        self.assertEqual(lines, [
            {"_time": "2026-11-23T12:00:00.500000Z", "_msg": "one", "level": "info", "app": "tiles-sync",
             "run_id": "7", **IDENTITY},
            {"_time": "2026-11-23T12:00:00.500001Z", "_msg": "two\nlines", "level": "error", "app": "tiles-sync",
             "run_id": "7", **IDENTITY},
        ])

    def test_long_messages_are_capped_in_utf8(self):
        log = sut.Log()
        log.add("ü" * 5000)

        msg = log.records[0]["_msg"]
        self.assertLessEqual(len(msg.encode()), sut.MAX_MSG_BYTES)
        self.assertTrue(msg.endswith(sut.TRUNCATED))


def pmtiles(metadata, compression=sut.COMPRESSION_GZIP):
    """A PMTiles v3 header followed by its metadata, the rest of an archive left out."""
    body = json.dumps(metadata).encode()
    if compression == sut.COMPRESSION_GZIP:
        body = gzip.compress(body)
    return archive(body, compression)


def archive(body, compression=sut.COMPRESSION_GZIP):
    """A PMTiles v3 header whose metadata is body as stored (its length advertised), the rest left out."""
    header = bytearray(sut.PMTILES_HEADER_BYTES)
    header[:8] = b"PMTiles\x03"
    struct.pack_into("<QQ", header, 24, sut.PMTILES_HEADER_BYTES, len(body))
    header[97] = compression
    return bytes(header) + body


def reader(data):
    return lambda offset, length: data[offset:offset + length]


class BuildInfoTest(unittest.TestCase):
    METADATA = {"name": "Protomaps Basemap", "version": "4.15.2",
                "planetiler:osm:osmosisreplicationtime": "2026-11-18T04:00:00Z"}

    def test_reads_the_version_and_osm_time_from_gzip_or_plain_metadata(self):
        for compression in (sut.COMPRESSION_GZIP, sut.COMPRESSION_NONE):
            with self.subTest(compression=compression):
                info = sut.build_info(reader(pmtiles(self.METADATA, compression)))

                self.assertEqual(info, {"version": "4.15.2", "osmDate": "2026-11-18T04:00:00Z"})

    def test_missing_keys_are_null(self):
        self.assertEqual(sut.build_info(reader(pmtiles({"version": ""}))), {"version": None, "osmDate": None})

    def test_rejects_what_is_not_a_readable_pmtiles_v3_archive(self):
        cases = {
            "magic": b"PMTilez\x03" + pmtiles(self.METADATA)[8:],
            "version": b"PMTiles\x02" + pmtiles(self.METADATA)[8:],
            "compression": pmtiles(self.METADATA, compression=4),
            "not an object": pmtiles([1, 2], compression=sut.COMPRESSION_NONE),
        }
        for name, data in cases.items():
            with self.subTest(name):
                with self.assertRaises(ValueError):
                    sut.build_info(reader(data))

    def test_metadata_that_is_not_a_whole_gzip_member_is_a_value_error(self):
        member = gzip.compress(json.dumps(self.METADATA).encode())
        cases = {
            # The header advertises the bytes served: the range checks pass, the member ends early.
            "truncated": member[:-8],
            "cut in half": member[:len(member) // 2],
            "corrupt deflate": member[:10] + b"\xff" * 20,  # zlib.error
            "bad crc": member[:-8] + bytes(8),  # BadGzipFile
            "not gzip": b"{}" + member[2:],
        }
        for name, body in cases.items():
            with self.subTest(name):
                with self.assertRaisesRegex(ValueError, "is not a whole gzip member"):
                    sut.build_info(reader(archive(body)))


class RangeServer(http.server.BaseHTTPRequestHandler):
    """Serves DATA with byte ranges and their Content-Range, or ignores them (a 200 with everything) when
    IGNORE_RANGE is set. SHIFT serves as many bytes from that much further on, and says so; CONTENT_RANGE, when not
    None, is sent instead of the true one ("" sends none)."""
    DATA = b""
    IGNORE_RANGE = False
    SHIFT = 0
    CONTENT_RANGE = None
    USER_AGENTS = []

    def do_GET(self):
        RangeServer.USER_AGENTS.append(self.headers.get("User-Agent"))
        if self.IGNORE_RANGE:
            self.send_response(200)
            self.send_header("Content-Length", str(len(self.DATA)))
            self.end_headers()
            self.wfile.write(self.DATA)
            return
        start, end = (int(n) + self.SHIFT for n in self.headers["Range"].removeprefix("bytes=").split("-"))
        part = self.DATA[start:end + 1]
        content_range = self.CONTENT_RANGE
        if content_range is None:
            content_range = f"bytes {start}-{start + len(part) - 1}/{len(self.DATA)}"
        self.send_response(206)
        if content_range:
            self.send_header("Content-Range", content_range)
        self.send_header("Content-Length", str(len(part)))
        self.end_headers()
        self.wfile.write(part)

    def log_message(self, *args):
        pass


class RangeReaderTest(unittest.TestCase):
    def setUp(self):
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), RangeServer)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/20261118.pmtiles"
        RangeServer.DATA = pmtiles(BuildInfoTest.METADATA)
        RangeServer.IGNORE_RANGE = False
        RangeServer.SHIFT = 0
        RangeServer.CONTENT_RANGE = None
        RangeServer.USER_AGENTS = []

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def test_build_info_over_http_ranges_with_its_own_user_agent(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            sut.main(["build-info", "--url", self.url])

        self.assertEqual(json.loads(out.getvalue()), {"version": "4.15.2", "osmDate": "2026-11-18T04:00:00Z"})
        self.assertEqual(RangeServer.USER_AGENTS, [sut.USER_AGENT, sut.USER_AGENT])

    def test_truncated_gzip_metadata_is_retried_then_reported(self):
        member = gzip.compress(json.dumps(BuildInfoTest.METADATA).encode())
        RangeServer.DATA = archive(member[:-8])
        err = io.StringIO()
        with mock.patch.object(sut.time, "sleep") as sleep, contextlib.redirect_stderr(err), \
                self.assertRaises(SystemExit) as raised:
            sut.main(["build-info", "--url", self.url])

        self.assertEqual(raised.exception.code, 1)
        # Every attempt reads the header and the metadata again.
        self.assertEqual(len(RangeServer.USER_AGENTS), 2 * (sut.HTTP_RETRIES + 1))
        self.assertEqual(sleep.call_count, sut.HTTP_RETRIES)
        self.assertRegex(err.getvalue(), rf"^could not read {self.url}: metadata at 127 \(\d+ bytes\) is not a whole "
                                         r"gzip member: Compressed file ended")

    def test_a_server_that_ignores_the_range_is_an_error(self):
        RangeServer.IGNORE_RANGE = True

        with self.assertRaisesRegex(ValueError, "expected a 206"):
            sut.range_reader(self.url)(0, sut.PMTILES_HEADER_BYTES)

    def test_bytes_of_the_right_length_from_elsewhere_in_the_archive_are_an_error(self):
        RangeServer.SHIFT = 10
        self.assertGreaterEqual(len(RangeServer.DATA), sut.PMTILES_HEADER_BYTES + 10)

        with self.assertRaisesRegex(ValueError, r"expected Content-Range bytes 0-126/<size>, got bytes 10-136/"):
            sut.range_reader(self.url)(0, sut.PMTILES_HEADER_BYTES)

    def test_the_content_range_must_be_the_requested_bytes(self):
        read = sut.range_reader(self.url)
        expected = RangeServer.DATA[:sut.PMTILES_HEADER_BYTES]
        for header in ("bytes 0-126/*", "Bytes 0-126/5000", f"bytes 0-126/{len(RangeServer.DATA)}"):
            with self.subTest(accepted=header):
                RangeServer.CONTENT_RANGE = header
                self.assertEqual(read(0, sut.PMTILES_HEADER_BYTES), expected)
        for header in ("", "bytes 1-127/5000", "bytes 0-125/5000", "bytes 0-127/5000", "bytes 0-126/126",
                       "bytes */5000", "bytes 0-126", "items 0-126/5000"):
            with self.subTest(rejected=header):
                RangeServer.CONTENT_RANGE = header
                with self.assertRaisesRegex(ValueError, "expected Content-Range"):
                    read(0, sut.PMTILES_HEADER_BYTES)


class MainTest(WithTempDir):
    def test_without_a_token_nothing_is_sent(self):
        out = io.StringIO()
        with mock.patch.dict(os.environ, {sut.TOKEN_ENV: ""}), mock.patch.object(sut, "post") as post, \
                contextlib.redirect_stdout(out):
            sut.main(["sync", "--base-url", "https://o11y.invalid", "--build", "20261118",
                      "--resolved-status", "skipped"])

        post.assert_not_called()
        self.assertIn("nothing reported to o11y", out.getvalue())

    def test_pushes_metrics_then_logs_with_the_bearer_token(self):
        out = io.StringIO()
        with mock.patch.dict(os.environ, {sut.TOKEN_ENV: "secret"}), mock.patch.object(sut, "post") as post, \
                mock.patch.object(sut.time, "time", return_value=NOW), contextlib.redirect_stdout(out):
            sut.main(["sync", "--base-url", "https://o11y.invalid/", "--build", "20261118",
                      "--resolved-status", "skipped"])

        (metrics_call, logs_call) = post.call_args_list
        url, body, content_type, token = metrics_call.args
        self.assertEqual((url, content_type, token),
                         ("https://o11y.invalid/insert/0/prometheus/api/v1/import/prometheus", "text/plain", "secret"))
        self.assertIn(f'tiles_sync_runs{{status="skipped",{IDENTITY_LABELS}}} 1 1795435200500\n', body.decode())
        url, body, content_type, token = logs_call.args
        self.assertEqual((url, content_type, token),
                         ("https://o11y.invalid/insert/jsonline?_stream_fields=project,cluster,env",
                          "application/stream+json", "secret"))
        self.assertEqual(json.loads(body)["_msg"], "tiles sync skipped: 20261118")

    def test_a_failed_metrics_push_is_logged_and_does_not_fail(self):
        calls = []

        def post(url, body, content_type, token):
            calls.append(url)
            if "prometheus" in url:
                raise OSError("connection refused")

        out = io.StringIO()
        with mock.patch.dict(os.environ, {sut.TOKEN_ENV: "secret"}), mock.patch.object(sut, "post", post), \
                mock.patch.object(sut.time, "sleep"), contextlib.redirect_stdout(out):
            sut.main(["watchdog", "--base-url", "https://o11y.invalid",
                      "--status", self.write("status.json", json.dumps(WatchdogTest.STATUS))])

        self.assertEqual(len(calls), sut.HTTP_RETRIES + 2)
        self.assertIn("::warning::o11y metrics push failed: connection refused", out.getvalue())

    def test_dry_run_prints_both(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            sut.main(["sync", "--dry-run", "--build", "20261118", "--resolved-status", "schema_changed"])

        text = out.getvalue()
        self.assertIn('tiles_sync_runs{status="schema_changed",', text)
        self.assertIn('"_msg": "tiles sync schema_changed: 20261118"', text)

    def test_decide_writes_its_outputs_and_prints_annotations(self):
        output = self.write("github_output", "earlier=1\n")
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            sut.main(["decide", "--builds", self.write("builds.json", json.dumps(BUILDS)),
                      "--status", self.write("live.json", json.dumps(LIVE_STATUS)), "--status-exit", "0",
                      "--requested", "20261116", "--force", "true", "--event", "workflow_dispatch",
                      "--output", output])

        with open(output) as f:
            lines = f.read().splitlines()
        self.assertEqual(lines[0], "earlier=1")
        self.assertIn("action=sync", lines)
        self.assertIn("key=20261116", lines)
        self.assertIn("max_changed_fraction=0.02", lines)
        self.assertIn("job_timeout_min=720", lines)
        self.assertTrue(out.getvalue().startswith("::warning::Rolling back from 20261117 to 20261116."))

    def test_resolve_commands_fail_on_an_unreadable_state(self):
        for command in (["decide", "--builds", self.write("builds.json", json.dumps(BUILDS))],
                        ["recheck", "--live", "20261117", "--build", "20261118"], ["live-state"]):
            with self.subTest(command=command[0]):
                out = io.StringIO()
                output = self.write("github_output", "")
                with contextlib.redirect_stdout(out), self.assertRaises(SystemExit) as raised:
                    sut.main([*command, "--status", self.write("live.json", ""), "--status-exit", "1",
                              "--status-stderr", self.write("live.err", "op: item not found\n"),
                              *([] if command[0] == "live-state" else ["--output", output])])

                self.assertEqual(raised.exception.code, 1)
                error = "bunny:status could not read the live state (exit 1): op: item not found"
                self.assertIn(f"::error::{error}", out.getvalue())
                with open(output) as f:
                    self.assertEqual(f.read(), "" if command[0] == "live-state" else f"message={error}\n")

    def test_live_state_passes_before_the_first_load(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            sut.main(["live-state", "--status", self.write("live.json", json.dumps(NO_STATE)), "--status-exit", "1"])

        self.assertEqual(out.getvalue(), "::notice::no build is live yet (no state.json)\n")

    def test_needs_a_base_url_or_dry_run(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            sut.parse_args(["watchdog", "--status", "x"])


if __name__ == "__main__":
    unittest.main()
