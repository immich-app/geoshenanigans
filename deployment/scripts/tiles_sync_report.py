#!/usr/bin/env python3
"""Decides and reports the tiles sync (tiles-sync.yml) and its watchdog to FUTO o11y.

Usage:
  tiles_sync_report.py build-info --url URL
  tiles_sync_report.py decide --builds FILE --status FILE [--status-exit N] [--status-stderr FILE]
      [--requested KEY] [--force true|false] [--hold TEXT] [--event EVENT] [--output FILE]
  tiles_sync_report.py recheck --status FILE [--status-exit N] [--status-stderr FILE] --live KEY --build KEY
      [--hold TEXT] [--event EVENT] [--output FILE]
  tiles_sync_report.py live-state --status FILE [--status-exit N] [--status-stderr FILE]
  tiles_sync_report.py sync --build KEY [--live KEY] [--version V] [--osm-date TIME]
      [--live-version V] [--resolved-status STATUS] [--resolved-message TEXT] [--report FILE]
      [--resolve-result R] [--sync-result R] [--check-outcome O] [--download-outcome O] [--download-seconds N]
      [--started-at T] [--finished-at T] [--run-id ID] [--run-attempt N] [--run-url URL] [--trigger EVENT]
      (--base-url URL | --dry-run)
  tiles_sync_report.py watchdog --status FILE [--status-exit N] [--stderr FILE] (--base-url URL | --dry-run)
  tiles_sync_report.py sample --report FILE [--exit N] [--stderr FILE] (--base-url URL | --dry-run)

build-info prints the tiles schema version and the OSM replication time of a
Protomaps build as JSON ({"version", "osmDate"}, null when the archive's
metadata has none), read from the archive's header and metadata with two range
requests (each answer's Content-Range must be the bytes asked for), before the
140 GB download.

decide is the resolve job's choice: the build (the newest in builds.json, or
the requested one), the live build from `npm run bunny:status -- --json` (its
output and exit code), and whether to sync, skip or stop, with the time budget
of the sync job (the guard rail and its timeouts). It writes them as
`key=value` lines to --output ($GITHUB_OUTPUT) and prints annotations.

recheck is the sync job's check, just before the download, that the run's
decision still holds: the live build is still the one it resolved against (a
"Re-run failed jobs" of an old run reuses its old decision), and no hold was set
for a scheduled run since. live-state checks that bunny:status read the state
zone (a build live, or none yet) for the watchdog, and fails when it could not.

sync reports one run of the sync workflow: the report JSON `npm run bunny:sync
-- --report` wrote (tiles/src/bunny/cli/sync.ts) when it ran, and otherwise
what the workflow decided by itself (`skipped` when the build is already live
or the schedule is held, `no_state` before the first load, `schema_changed`
when its schema version differs from the live build's) or why the run failed
before the CLI could report (the jobs' results and the check and download
steps' outcomes). It adds the download step's seconds and the run's total: the
sync job's first step to its last.

sample reports the daily sample check of the live build in every storage
region (`npm run bunny:replication -- --live --sample N --per-zoom --json
FILE`): positions missing, holding other bytes than the live manifest, or not
yet in a region, as tiles_sync_sample_*.

watchdog reports the live build from `npm run bunny:status -- --json`, with a
heartbeat. A watchdog that cannot read the live state sends no heartbeat, so
the watchdog-silent alert pages for it. Before the first load (no state.json)
it sends the heartbeat without the ages.

Both push to o11y's gateway at --base-url with the bearer token in
$O11Y_TOKEN, as geocoder/builder/tools/o11y_report.py does: the metrics as
Prometheus text to VictoriaMetrics, the log lines as JSON lines to
VictoriaLogs, every sample and line stamped with the tiles identity labels.
Without a token nothing is sent; --dry-run prints both instead. A failed push
is logged and the exit status stays 0: reporting never fails the run.
"""
import argparse
import datetime
import gzip
import http.client
import json
import math
import os
import re
import struct
import sys
import time
import typing
import urllib.error
import urllib.request

SYNC_APP = "tiles-sync"
WATCHDOG_APP = "tiles-sync-watchdog"
SAMPLE_APP = "tiles-sample-check"
# o11y's identity labels, on every sample and log line, merged last so no label
# of ours can shadow them. cluster=tiles puts the series in the tiles tenant
# beside the worker's; provider and region say where the reporter runs, as the
# geocoder's GitHub reports do.
IDENTITY = {"project": "geoshenanigans", "cluster": "tiles", "env": "prod", "provider": "github",
            "region": "global"}
METRICS_PATH = "/insert/0/prometheus/api/v1/import/prometheus"
# The stream fields the tiles worker's lines use (tiles/src/o11y.ts).
LOGS_PATH = "/insert/jsonline?_stream_fields=project,cluster,env"
TOKEN_ENV = "O11Y_TOKEN"
MAX_MSG_BYTES = 4096
TRUNCATED = " [truncated]"
HTTP_TIMEOUT_SECONDS = 10
HTTP_RETRIES = 3
HTTP_RETRY_DELAY_SECONDS = 5
# build.protomaps.com's Cloudflare front refuses urllib's default User-Agent.
USER_AGENT = "tiles-sync-report"

STATUSES = ("success", "skipped", "guard_rail", "failed", "schema_changed", "no_state")
# Runs that leave the zone as it should be: a skipped run found the newest
# build live already, or the schedule held. They move
# tiles_sync_last_success_timestamp_seconds. no_state (no state.json: the
# first load hasn't finished) does not, nor does it count as failed.
HEALTHY = ("success", "skipped")
# Statuses the workflow decides before the CLI runs.
RESOLVED_STATUSES = ("skipped", "schema_changed", "no_state")
LEVELS = {"success": "info", "skipped": "info", "no_state": "warn"}

KEY = re.compile(r"[0-9]{8}")
MD5 = re.compile(r"[A-Za-z0-9+/]{22}==")
VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")


class Budget(typing.NamedTuple):
    """What one sync job may take on, and the timeouts that let it finish."""
    max_changed_fraction: float
    replication_timeout_min: int
    sync_timeout_min: int
    job_timeout_min: int
    # Files in flight in the upload and DESCRIBEs in the replication gate
    # (bunny:sync --concurrency and --replication-concurrency).
    concurrency: int


# A job that stops after its first PUT leaves the zone with part of the build
# and state.json on the old one (the new one pending): the next run checks
# every changed file again before writing, so it repairs, but only a job that
# finishes makes the build live. So the guard rail is the diff one job
# finishes: the upload (each file DESCRIBEd, ~120 ms, then PUT, ~115 ms, so
# ~5.1k files/s at 1,200 in flight, the storage servers' share), then the
# slowest region's replication (Sydney, ~1.3k files/s, from the first PUT on)
# within the gate's timeout, then the TileJSON and styles' own gate (10 min at
# least); anything larger goes in as a full rewrite under a new prefix (plan
# section 6). The test checks each budget against those rates.
# Daily: 0.5% (7.2M positions, weeks of daily changes): ~25 min of upload,
# Sydney done ~2 h after the first PUT.
DAILY = Budget(max_changed_fraction=0.005, replication_timeout_min=120, sync_timeout_min=270, job_timeout_min=360,
               concurrency=1200)
# A forced run (a schema change or a diff over the daily guard rail): 2% (29M
# positions): ~95 min of upload, Sydney done ~7.5 h after the first PUT.
FORCED = Budget(max_changed_fraction=0.02, replication_timeout_min=480, sync_timeout_min=630, job_timeout_min=720,
                concurrency=1200)
HOLD_VARIABLE = "TILES_SYNC_HOLD"

# The report's numbers -> metric. Null fields (unknown, or a step the run never
# reached) send nothing.
REPORT_GAUGES = {
    "positionsChanged": "tiles_sync_positions_changed",
    "changedFraction": "tiles_sync_changed_fraction",
    "filesUploaded": "tiles_sync_files_uploaded",
    "bytesUploaded": "tiles_sync_bytes_uploaded",
    "retries": "tiles_sync_retries",
    "rateLimited": "tiles_sync_rate_limited",
}
# The replication region's label. The spec's `region` is an identity label
# (where the reporter runs, "global" here), which would overwrite it.
REGION_LABEL = "storage_region"

# PMTiles v3 (https://github.com/protomaps/PMTiles/blob/main/spec/v3/spec.md).
PMTILES_HEADER_BYTES = 127
PMTILES_MAGIC = b"PMTiles"
PMTILES_VERSION = 3
COMPRESSION_NONE = 1
COMPRESSION_GZIP = 2
MAX_METADATA_BYTES = 16 << 20
OSM_TIME_KEY = "planetiler:osm:osmosisreplicationtime"
# A 206's Content-Range (RFC 9110 14.4): first and last byte, then the
# archive's size or * when unknown.
CONTENT_RANGE = re.compile(r"bytes ([0-9]+)-([0-9]+)/([0-9]+|\*)", re.IGNORECASE)


def number(value):
    """value when it is a finite number (bools aren't), else None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value if math.isfinite(value) else None


def numbers(value):
    """The entries of a JSON object whose values are numbers ({} for anything else)."""
    if not isinstance(value, dict):
        return {}
    return {key: v for key, v in value.items() if number(v) is not None}


def read_json(path):
    """The file's JSON object; None when it is missing, empty or not an object."""
    if not path:
        return None
    try:
        with open(path) as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def read_tail(path, limit=1000):
    """The last `limit` characters of a text file, on one line; empty when missing."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            text = f.read()
    except (OSError, TypeError):
        return ""
    return " ".join(text[-limit:].split())


def capped(msg):
    """msg in at most MAX_MSG_BYTES of UTF-8, cut short and marked when longer."""
    data = msg.encode()
    if len(data) <= MAX_MSG_BYTES:
        return msg
    return data[:MAX_MSG_BYTES - len(TRUNCATED)].decode(errors="ignore") + TRUNCATED


class Log:
    """Log records that share the app's and the run's context fields."""

    def __init__(self, **context):
        self.context = {key: value for key, value in context.items() if value}
        self.records = []

    def add(self, msg, level="info"):
        self.records.append({"_msg": capped(msg), "level": level, **self.context, **IDENTITY})


# ── build-info ──────────────────────────────────────────────────────────────


def range_reader(url):
    """read(offset, length) of url by HTTP range requests, failing on anything but exactly those bytes."""
    def read(offset, length):
        last = offset + length - 1
        request = urllib.request.Request(url, headers={"Range": f"bytes={offset}-{last}", "User-Agent": USER_AGENT})
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            # A server that ignores Range answers 200 with the whole archive.
            if response.status != 206:
                raise ValueError(f"expected a 206 range response, got {response.status}")
            # The body's length doesn't say where in the archive it starts: only Content-Range does.
            served = " ".join((response.headers.get("Content-Range") or "").split())
            match = CONTENT_RANGE.fullmatch(served)
            if (not match or (int(match[1]), int(match[2])) != (offset, last)
                    or (match[3] != "*" and int(match[3]) <= last)):
                raise ValueError(f"expected Content-Range bytes {offset}-{last}/<size>, got {served or 'none'}")
            data = response.read(length + 1)
        if len(data) != length:
            raise ValueError(f"expected {length} bytes at {offset}, got {len(data)}")
        return data
    return read


def build_info(read):
    """The archive's tiles schema version and OSM replication time from its metadata."""
    header = read(0, PMTILES_HEADER_BYTES)
    if header[:7] != PMTILES_MAGIC or header[7] != PMTILES_VERSION:
        raise ValueError("not a PMTiles v3 archive")
    offset, length = struct.unpack_from("<QQ", header, 24)
    compression = header[97]
    if not 0 < length <= MAX_METADATA_BYTES:
        raise ValueError(f"unexpected metadata length {length}")
    data = read(offset, length)
    if compression == COMPRESSION_GZIP:
        data = gzip.decompress(data)
    elif compression != COMPRESSION_NONE:
        raise ValueError(f"unsupported internal compression {compression}")
    metadata = json.loads(data)
    if not isinstance(metadata, dict):
        raise ValueError("metadata is not a JSON object")
    return {"version": text_or_none(metadata.get("version")), "osmDate": text_or_none(metadata.get(OSM_TIME_KEY))}


def text_or_none(value):
    return value if isinstance(value, str) and value else None


# ── decide, recheck, live-state ─────────────────────────────────────────────


class Unreadable(Exception):
    """bunny:status could not read the state zone, or builds.json lacks what the run needs."""


def pending_builds(status):
    """The builds `bunny:status --json` lists as pending (a sync started them and didn't finish)."""
    pending = status.get("pending") if isinstance(status, dict) else None
    return [key for key in pending if isinstance(key, str) and KEY.fullmatch(key)] if isinstance(pending, list) else []


def live_build(status, exit_code=None, stderr=""):
    """The live build key from `bunny:status --json`'s output, or None when the state zone has no state.json yet (it
    then prints nulls and exits 1). Raises Unreadable for anything else: an error prints no JSON."""
    if isinstance(status, dict) and "liveBuild" in status:
        live = status["liveBuild"]
        if live is None:
            return None
        if isinstance(live, str) and KEY.fullmatch(live) and exit_code in (None, 0):
            return live
    detail = " ".join(stderr.split())[-500:] or "no output"
    raise Unreadable(f"bunny:status could not read the live state (exit {exit_code}): {detail}")


def held(hold, event):
    """Why a scheduled run doesn't sync, or None. Dispatches aren't held: they are how a hold is acted on."""
    if event == "schedule" and hold.strip():
        return (f"scheduled syncs are held ({HOLD_VARIABLE}: {' '.join(hold.split())}); clear the repository variable"
                " to resume them, or dispatch Tiles Sync with a build")
    return None


def newest_key(builds):
    keys = sorted(b["key"] for b in builds if isinstance(b, dict) and isinstance(b.get("key"), str))
    return keys[-1].removesuffix(".pmtiles") if keys else ""


def build_entry(builds, key):
    return next((b for b in builds if isinstance(b, dict) and b.get("key") == f"{key}.pmtiles"), None)


def version_of(entry):
    version = (entry or {}).get("version")
    return version if isinstance(version, str) and VERSION.fullmatch(version) else ""


def decide(builds, status, status_exit=None, status_stderr="", requested="", force=False, hold="", event=""):
    """The resolve job's outputs and the annotations to print, as (outputs, [(level, message)]). Raises Unreadable."""
    if not isinstance(builds, list):
        raise Unreadable("builds.json is not a list of builds")
    key = requested or newest_key(builds)
    if not KEY.fullmatch(key):
        raise Unreadable(f"Build must be a YYYYMMDD key, got '{key}'")
    entry = build_entry(builds, key)
    if entry is None:
        raise Unreadable(f"{key} isn't in builds.json; protomaps only keeps recent builds")
    md5 = entry.get("md5sum")
    if not isinstance(md5, str) or not MD5.fullmatch(md5):
        raise Unreadable(f"builds.json has no MD5 for {key}")
    version = version_of(entry)
    live = live_build(status, status_exit, status_stderr)
    # builds.json keeps about a week of daily builds and the first of each version.
    live_version = version_of(build_entry(builds, live)) if live else ""
    budget = FORCED if force else DAILY
    outputs = {"key": key, "md5": md5, "version": version, "live": live or "", "live_version": live_version,
               **budget._asdict()}

    def result(action, status_, level, message):
        return {**outputs, "action": action, "status": status_, "message": message}, [(level, message)]

    if live is None:
        return result("skip", "no_state", "notice", "No build is live yet: the state zone has no state.json, so the"
                      " first load hasn't finished (or state.json was deleted); nothing to sync against")
    reason = held(hold, event)
    if reason:
        return result("skip", "skipped", "notice", f"{key} not synced: {reason}")
    if key == live:
        pending = pending_builds(status)
        if not pending:
            return result("skip", "skipped", "notice", f"{key} is live already")
        # A failed or cancelled sync left their bytes where they differ; syncing the live build puts its own back.
        return {**outputs, "action": "sync", "status": "", "message": ""}, [
            ("warning", f"{key} is live with {', '.join(pending)} pending: syncing {key} again puts its bytes back"
                        " where they wrote, then purges")]
    if not requested and key < live:
        return result("skip", "skipped", "warning", f"The newest build in builds.json, {key}, is older than the live"
                      f" {live}; a rollback needs the build input")
    notes = []
    if key < live:
        notes.append(("warning", f"Rolling back from {live} to {key}. "
                      + (f"{HOLD_VARIABLE} holds the schedule." if hold.strip() else
                         f"Set the repository variable {HOLD_VARIABLE}, or the next scheduled run syncs the newest"
                         " build again.")))
    if not version or not live_version:
        notes.append(("warning", f"The schema version of {key} ('{version}') or of the live {live} ('{live_version}')"
                      " is unknown; only the diff's guard rail applies"))
    elif version.split(".")[:2] != live_version.split(".")[:2]:
        if not force:
            return result("stop", "schema_changed", "error", f"{key} has tiles schema {version} but the live {live}"
                          f" has {live_version}; a schema change needs a full rewrite under a new prefix, or a dispatch"
                          " with force to sync it in place")
        notes.append(("warning", f"{key} has tiles schema {version} and the live {live} has {live_version}; syncing"
                      " in place (force)"))
    if force:
        notes.append(("notice", f"Forced: up to {budget.max_changed_fraction:.1%} of positions may change, with a"
                      f" {budget.replication_timeout_min} min replication gate"))
    return {**outputs, "action": "sync", "status": "", "message": ""}, notes


def recheck(status, status_exit=None, status_stderr="", live="", build="", hold="", event=""):
    """Whether the sync job goes on with the decision its resolve job made, as (outputs, [(level, message)])."""
    now = live_build(status, status_exit, status_stderr)
    if now is None:
        message = f"state.json is gone since this run resolved {build} against the live {live}; nothing to sync against"
        return {"proceed": "false", "status": "no_state", "message": message}, [("error", message)]
    if now != live:
        message = (f"The live build is {now} now, not {live} as when this run resolved {build}, so it syncs nothing:"
                   " re-running an old run's jobs reuses its old decision. Dispatch Tiles Sync instead")
        return {"proceed": "false", "status": "skipped", "message": message}, [("warning", message)]
    reason = held(hold, event)
    if reason:
        message = f"{build} not synced: {reason}"
        return {"proceed": "false", "status": "skipped", "message": message}, [("notice", message)]
    return {"proceed": "true", "status": "", "message": ""}, []


def github_output(path, outputs):
    """outputs as key=value lines, appended to $GITHUB_OUTPUT (or printed without a path). Values are single lines."""
    lines = "".join(f"{key}={' '.join(str(value).split())}\n" for key, value in outputs.items())
    if path:
        with open(path, "a") as f:
            f.write(lines)
    else:
        sys.stdout.write(lines)


def annotate(notes):
    for level, message in notes:
        print(f"::{level}::{message}", flush=True)


# ── sync ────────────────────────────────────────────────────────────────────


def failure_reason(opts):
    """Why a run without a report failed, from the jobs' results and the download step's outcome."""
    if opts.resolve_result not in ("", "success"):
        return f"resolving the build ended {opts.resolve_result}; see the Resolve build job"
    if opts.sync_result in ("", "skipped"):
        return "the sync job did not run"
    if opts.sync_result == "cancelled":
        return "the sync job was cancelled or timed out"
    if opts.check_outcome == "failure":
        return "re-reading the live build before the download failed; see the sync job's Check the decision step"
    if opts.download_outcome == "failure":
        return "downloading the build failed or its MD5 did not match"
    if opts.download_outcome == "cancelled":
        return "downloading the build was cancelled or timed out"
    return f"the sync job ended {opts.sync_result} without a sync report"


def run_status(opts, report):
    """The run's status and error: the CLI's when it reported, else the workflow's own decision, else failed."""
    if report is not None and report.get("status") in STATUSES:
        error = report.get("error")
        return report["status"], error if isinstance(error, str) and error else None
    if opts.resolved_status == "schema_changed":
        return "schema_changed", (f"the build's tiles schema {opts.version or '?'} differs from the live build's "
                                  f"{opts.live_version or '?'} in major.minor: seed it under a new prefix, or dispatch "
                                  "Tiles Sync with force to sync it in place")
    if opts.resolved_status in RESOLVED_STATUSES:
        return opts.resolved_status, None
    if report is not None:
        return "failed", f"the sync report has no known status ({report.get('status')!r})"
    message = " ".join(opts.resolved_message.split())
    return "failed", failure_reason(opts) + (f": {message}" if message else "")


def report_samples(report):
    """The CLI report's numbers as samples, leaving out the null ones."""
    samples = []
    for field, name in REPORT_GAUGES.items():
        value = number(report.get(field))
        if value is not None:
            samples.append((name, {}, value))
    complete = report.get("replicationComplete")
    if isinstance(complete, bool):
        samples.append(("tiles_sync_replication_complete", {}, int(complete)))
    for region, seconds in sorted(numbers(report.get("replicationWaitSeconds")).items()):
        samples.append(("tiles_sync_replication_wait_seconds", {REGION_LABEL: region}, seconds))
    for step, seconds in numbers(report.get("stepSeconds")).items():
        samples.append(("tiles_sync_step_seconds", {"step": step}, seconds))
    return samples


def workflow_samples(opts):
    """The download step's seconds and the sync job's total, its first step to its last, when it ran."""
    samples = []
    download = number(opts.download_seconds)
    if download is not None:
        samples.append(("tiles_sync_step_seconds", {"step": "download"}, download))
    started, finished = number(opts.started_at), number(opts.finished_at)
    if started is not None and finished is not None and finished >= started:
        samples.append(("tiles_sync_step_seconds", {"step": "total"}, finished - started))
    return samples


def describe(status, build, previous, previous_version, version, osm_date, report, steps):
    """One line saying what the run did, with its step seconds."""
    what = f"tiles sync {status}: {build or 'no build'}"
    details = ", ".join(d for d in (f"tiles {version}" if version else "", f"OSM {osm_date}" if osm_date else "") if d)
    if details:
        what += f" ({details})"
    if previous:
        what += f", live was {previous}" + (f" (tiles {previous_version})" if previous_version else "")
    report = report or {}
    changed, total = number(report.get("positionsChanged")), number(report.get("positionsTotal"))
    if changed is not None:
        what += f"; {changed:,.0f}" + (f" of {total:,.0f}" if total is not None else "") + " positions changed"
        fraction = number(report.get("changedFraction"))
        if fraction is not None:
            what += f" ({fraction:.4%})"
    files, size = number(report.get("filesUploaded")), number(report.get("bytesUploaded"))
    if files is not None:
        what += f"; {files:,.0f} files" + (f", {size / 1e9:.2f} GB" if size is not None else "") + " uploaded"
    retries, limited = number(report.get("retries")), number(report.get("rateLimited"))
    if retries is not None or limited is not None:
        what += f"; {retries or 0:,.0f} retries, {limited or 0:,.0f} rate limited"
    complete = report.get("replicationComplete")
    waits = numbers(report.get("replicationWaitSeconds"))
    if isinstance(complete, bool):
        what += "; replication " + ("complete" if complete else "INCOMPLETE")
        # A region with no wait never held every file: it is the one to look at, not the slowest that did.
        all_waits = report.get("replicationWaitSeconds")
        never = sorted(r for r, v in all_waits.items() if v is None) if isinstance(all_waits, dict) else []
        if never:
            what += f", never in {', '.join(never)}"
        if waits:
            slowest = max(waits, key=waits.get)
            what += f", slowest {slowest} {waits[slowest]:,.0f}s"
        stragglers = [s for s in report.get("replicationStragglers") or []
                      if isinstance(s, dict) and isinstance(s.get("path"), str)]
        if not complete and stragglers:
            shown = [f"{s['path']} (not in {', '.join(map(str, s.get('regions') or []))})" for s in stragglers[:5]]
            what += "; still not replicated: " + ", ".join(shown) + (
                f" and {len(stragglers) - 5} more" if len(stragglers) > 5 else "")
    if steps:
        what += "; steps " + ", ".join(f"{s} {v:,.0f}s" for s, v in steps.items())
    return what


def sync(opts, now):
    """The run's samples and log lines."""
    report = read_json(opts.report)
    status, error = run_status(opts, report)
    build = (report or {}).get("build") or opts.build
    previous = (report or {}).get("previousBuild") or opts.live
    osm_date = (report or {}).get("osmDate") or opts.osm_date
    log = Log(app=SYNC_APP, run_id=opts.run_id, run_attempt=opts.run_attempt, trigger=opts.trigger,
              build=build, previous_build=previous, status=status)
    samples = [("tiles_sync_runs", {"status": status}, 1)]
    if status in HEALTHY:
        samples.append(("tiles_sync_last_success_timestamp_seconds", {}, int(now)))
    if report:
        samples += report_samples(report)
    samples += workflow_samples(opts)
    # The download first and the total last, the CLI's steps between them in its order.
    steps = [(labels["step"], value) for name, labels, value in samples if name == "tiles_sync_step_seconds"]
    steps = dict(sorted(steps, key=lambda step: (step[0] != "download", step[0] == "total")))
    level = LEVELS.get(status, "error")
    log.add(describe(status, build, previous, opts.live_version, opts.version, osm_date, report, steps)
            + (f": {opts.run_url}" if opts.run_url else ""), level=level)
    # Why the workflow skipped (live already, held, a re-run of an old run) or found no state.json.
    message = " ".join(opts.resolved_message.split())
    if message and status in ("skipped", "no_state") and report is None:
        log.add(f"tiles sync {status}: {message}", level=level)
    if error:
        log.add(f"tiles sync error: {error}", level="error")
    return samples, log


# ── watchdog ────────────────────────────────────────────────────────────────


def hours(value):
    return f"{value:.1f}h" if number(value) is not None else "unknown"


def watchdog(opts, now):
    """The live build's ages with the heartbeat, only the heartbeat before the first load, or an error line and
    nothing else when the state is unreadable."""
    log = Log(app=WATCHDOG_APP)
    status = read_json(opts.status)
    try:
        live = live_build(status, opts.status_exit)
    except Unreadable:
        detail = read_tail(opts.stderr) or "no output"
        log.add(f"could not read the live tiles state with bunny:status: {detail}", level="error")
        return [], log
    samples = [("tiles_sync_watchdog_heartbeat", {}, int(now))]
    if live is None:
        # The watchdog works; there is nothing to age yet. Without ages the sync-stale and osm-old alerts stay quiet.
        log.add("no build is live yet: the state zone has no state.json (the first load hasn't finished, or it was"
                " deleted)", level="warn")
        return samples, log
    ages = {"buildAgeHours": "tiles_sync_live_build_age_hours", "osmAgeHours": "tiles_sync_live_osm_age_hours"}
    for field, name in ages.items():
        value = number(status.get(field))
        if value is not None:
            samples.append((name, {}, value))
    osm_date, updated = status.get("osmDate") or "unknown", status.get("updatedAt") or "unknown"
    log.add(f"live build {live} (OSM {osm_date}), updated {updated}:"
            f" build {hours(status.get('buildAgeHours'))} old, OSM data {hours(status.get('osmAgeHours'))} old")
    return samples, log


# ── sample ──────────────────────────────────────────────────────────────────


def sample(opts, now):
    """The sample check's samples and log line. An unreadable or absent report is result=error, a check skipped
    while builds are pending result=skipped; both send no counts."""
    log = Log(app=SAMPLE_APP)
    report = read_json(opts.report)
    if report is not None and isinstance(report.get("skipped"), str):
        log.add(f"tiles sample check skipped: {report['skipped']}")
        return [("tiles_sync_sample_runs", {"result": "skipped"}, 1)], log
    checked = number((report or {}).get("checked"))
    if checked is None or opts.exit not in (0, 1, 2):
        detail = read_tail(opts.stderr) or "no output"
        log.add(f"tiles sample check could not run (exit {opts.exit}): {detail}", level="error")
        return [("tiles_sync_sample_runs", {"result": "error"}, 1)], log
    samples = [("tiles_sync_sample_runs", {"result": "ok"}, 1), ("tiles_sync_sample_checked", {}, checked)]
    counts = {field: number(report.get(field)) or 0 for field in ("missing", "mismatched", "errors")}
    for field, value in counts.items():
        samples.append((f"tiles_sync_sample_{field}", {}, value))
    lacking = numbers(report.get("lacking"))
    for region, value in sorted(lacking.items()):
        samples.append(("tiles_sync_sample_lacking", {REGION_LABEL: region}, value))
    examples = [p for key in ("missingPaths", "mismatchedPaths") for p in (report.get(key) or [])[:3]
                if isinstance(p, str)]
    short = ", ".join(f"{region} {value:,.0f}" for region, value in sorted(lacking.items()) if value)
    what = (f"tiles sample check of {report.get('build') or 'the live build'} under {report.get('prefix')}/:"
            f" {checked:,.0f} positions, {counts['missing']:,.0f} missing, {counts['mismatched']:,.0f} with other"
            f" bytes, {counts['errors']:,.0f} DESCRIBE errors; not yet in a region: {short or 'none'}")
    if examples:
        what += f"; e.g. {', '.join(examples)}"
    level = "error" if counts["missing"] or counts["mismatched"] else "warn" if short or counts["errors"] else "info"
    log.add(what, level=level)
    return samples, log


# ── pushing ─────────────────────────────────────────────────────────────────


def escape(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def format_value(value):
    """A whole float as an integer (300.0 as 300)."""
    return int(value) if isinstance(value, float) and value.is_integer() else value


def exposition(samples, timestamp_ms):
    """Prometheus text with the identity labels last, every sample at timestamp_ms. It ends in a newline:
    VictoriaMetrics stores nothing of a last line without one."""
    lines = []
    for name, labels, value in samples:
        selector = ",".join(f'{key}="{escape(v)}"' for key, v in {**labels, **IDENTITY}.items())
        lines.append(f"{name}{{{selector}}} {format_value(value)} {timestamp_ms}\n")
    return "".join(lines)


def rfc3339(microseconds):
    t = datetime.datetime.fromtimestamp(microseconds // 1_000_000, datetime.timezone.utc)
    return t.replace(microsecond=microseconds % 1_000_000).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def log_lines(records, now):
    """records as JSON lines, timed a microsecond apart so VictoriaLogs keeps their order."""
    start = int(now * 1_000_000)
    return "".join(json.dumps({"_time": rfc3339(start + i), **record}, ensure_ascii=False) + "\n"
                   for i, record in enumerate(records))


def with_retries(request):
    """request()'s result and None, or None and the last error once every attempt failed. A 4xx isn't retried."""
    error = None
    for attempt in range(HTTP_RETRIES + 1):
        if attempt:
            time.sleep(HTTP_RETRY_DELAY_SECONDS * attempt)
        try:
            return request(), None
        except urllib.error.HTTPError as e:
            error = f"HTTP {e.code}: {response_text(e)}"
            if e.code < 500:
                break
        except (OSError, ValueError, http.client.HTTPException) as e:
            error = str(e) or type(e).__name__
    return None, error


def response_text(error):
    try:
        with error:
            return " ".join(error.read(300).decode(errors="replace").split())
    except (OSError, http.client.HTTPException):
        return ""


def post(url, body, content_type, token):
    request = urllib.request.Request(url, data=body, method="POST", headers={
        "Content-Type": content_type, "Authorization": f"Bearer {token}", "User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        response.read()


def push(base_url, samples, log, now, token):
    """Sends the metrics, then the log lines (with the metrics push's error, if any)."""
    base_url = base_url.rstrip("/")
    if samples:
        body = exposition(samples, int(now * 1000)).encode()
        _, error = with_retries(lambda: post(base_url + METRICS_PATH, body, "text/plain", token))
        if error:
            print(f"::warning::o11y metrics push failed: {error}", flush=True)
            log.add(f"metrics push failed: {error}", level="error")
        else:
            print(f"Pushed {len(samples)} samples to o11y", flush=True)
    if log.records:
        body = log_lines(log.records, now).encode()
        _, error = with_retries(lambda: post(base_url + LOGS_PATH, body, "application/stream+json", token))
        if error:
            print(f"::warning::o11y logs push failed: {error}", flush=True)
        else:
            print(f"Pushed {len(log.records)} log lines to o11y", flush=True)


def add_status_args(command):
    command.add_argument("--status", default="", help="`npm run bunny:status -- --json`'s output")
    command.add_argument("--status-exit", default=None, type=optional_int, help="its exit code")
    command.add_argument("--status-stderr", default="", help="its stderr, quoted when the status is unreadable")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Decides and reports the tiles sync and its watchdog.")
    commands = parser.add_subparsers(dest="command", required=True)
    info = commands.add_parser("build-info", help="a Protomaps build's schema version and OSM time, as JSON")
    info.add_argument("--url", required=True, help="the archive, e.g. https://build.protomaps.com/20261118.pmtiles")
    pick = commands.add_parser("decide", help="the resolve job's build, live build, action and budget")
    pick.add_argument("--builds", required=True, help="https://build-metadata.protomaps.dev/builds.json, downloaded")
    pick.add_argument("--requested", default="", help="the dispatch's build input (YYYYMMDD), empty for the newest")
    pick.add_argument("--force", default="", help="the dispatch's force input (true or false)")
    again = commands.add_parser("recheck", help="the sync job's check that the resolve job's decision still holds")
    again.add_argument("--live", default="", help="the build live when the resolve job ran")
    again.add_argument("--build", default="", help="the build the resolve job picked")
    for command in (pick, again):
        add_status_args(command)
        command.add_argument("--hold", default="", help=f"the {HOLD_VARIABLE} repository variable")
        command.add_argument("--event", default="", help="the event that started the run")
        command.add_argument("--output", default="", help="the file to append key=value lines to ($GITHUB_OUTPUT)")
    state = commands.add_parser("live-state", help="fails unless bunny:status read the state zone")
    add_status_args(state)
    run = commands.add_parser("sync", help="one run of tiles-sync.yml")
    run.add_argument("--build", default="", help="the build the run resolved (YYYYMMDD)")
    run.add_argument("--live", default="", help="the build live when it started")
    run.add_argument("--version", default="", help="the build's tiles schema version")
    run.add_argument("--live-version", default="", help="the live build's tiles schema version")
    run.add_argument("--osm-date", default="", help="the build's OSM replication time")
    run.add_argument("--resolved-status", default="", help="what the workflow decided without running the sync: "
                     + " or ".join(RESOLVED_STATUSES) + " (anything else is ignored)")
    run.add_argument("--resolved-message", default="", help="why the workflow skipped or found no state")
    run.add_argument("--report", default="", help="the sync CLI's --report JSON")
    run.add_argument("--resolve-result", default="", help="the resolve job's result")
    run.add_argument("--sync-result", default="", help="the sync job's result")
    run.add_argument("--check-outcome", default="", help="the sync job's decision check step's outcome")
    run.add_argument("--download-outcome", default="", help="the download step's outcome")
    run.add_argument("--download-seconds", default=None, type=optional_float)
    run.add_argument("--started-at", default=None, type=optional_float, help="the sync job's start, epoch seconds")
    run.add_argument("--finished-at", default=None, type=optional_float, help="the sync job's end, epoch seconds")
    run.add_argument("--run-id", default="")
    run.add_argument("--run-attempt", default="")
    run.add_argument("--run-url", default="")
    run.add_argument("--trigger", default="", help="the event that started the run")
    dog = commands.add_parser("watchdog", help="the live build's age and a heartbeat")
    dog.add_argument("--status", default="", help="`npm run bunny:status -- --json`'s output")
    dog.add_argument("--status-exit", default=None, type=optional_int, help="its exit code")
    dog.add_argument("--stderr", default="", help="its stderr, quoted when the status is unreadable")
    check = commands.add_parser("sample", help="the daily sample check of the live build in every region")
    check.add_argument("--report", default="", help="`npm run bunny:replication -- --live ... --json`'s file")
    check.add_argument("--exit", default=None, type=optional_int, help="its exit code")
    check.add_argument("--stderr", default="", help="its stderr, quoted when it could not run")
    for command in (run, dog, check):
        command.add_argument("--base-url", default="", help="o11y's gateway, e.g. https://vmauth.futostatus.com")
        command.add_argument("--dry-run", action="store_true", help="print the metrics and log lines; send nothing")
    opts = parser.parse_args(argv)
    if opts.command in ("sync", "watchdog", "sample") and not opts.dry_run and not opts.base_url:
        parser.error("--base-url or --dry-run is required")
    return opts


def optional_float(text):
    """An empty argument (an output of a job that never ran) is None."""
    return float(text) if text.strip() else None


def optional_int(text):
    return int(text) if text.strip() else None


def read_text(path):
    """The file's text; empty when it is missing."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return f.read()
    except (OSError, TypeError):
        return ""


def read_any_json(path):
    """The file's JSON value; None when it is missing or not JSON."""
    try:
        return json.loads(read_text(path))
    except ValueError:
        return None


def resolve_command(opts):
    """decide, recheck or live-state: annotations on stdout, outputs to --output, exit 1 when unreadable."""
    status = read_json(opts.status)
    stderr = read_text(opts.status_stderr)
    try:
        if opts.command == "decide":
            outputs, notes = decide(read_any_json(opts.builds), status, opts.status_exit, stderr, opts.requested,
                                    opts.force == "true", opts.hold, opts.event)
        elif opts.command == "recheck":
            outputs, notes = recheck(status, opts.status_exit, stderr, opts.live, opts.build, opts.hold, opts.event)
        else:
            live = live_build(status, opts.status_exit, stderr)
            outputs, notes = {}, [("notice", f"live build {live}" if live else "no build is live yet (no state.json)")]
    except Unreadable as e:
        annotate([("error", str(e))])
        # The report job quotes it as the failure's detail.
        if opts.command != "live-state":
            github_output(opts.output, {"message": str(e)})
        sys.exit(1)
    annotate(notes)
    if opts.command != "live-state":
        github_output(opts.output, outputs)


def main(argv=None):
    opts = parse_args(argv)
    if opts.command == "build-info":
        info, error = with_retries(lambda: build_info(range_reader(opts.url)))
        if error:
            print(f"could not read {opts.url}: {error}", file=sys.stderr)
            sys.exit(1)
        print(json.dumps(info))
        return
    if opts.command in ("decide", "recheck", "live-state"):
        resolve_command(opts)
        return

    now = time.time()
    samples, log = {"sync": sync, "watchdog": watchdog, "sample": sample}[opts.command](opts, now)
    if opts.dry_run:
        sys.stdout.write(exposition(samples, int(now * 1000)) + log_lines(log.records, now))
        return
    token = os.environ.get(TOKEN_ENV, "").strip()
    if not token:
        print(f"::warning::No ${TOKEN_ENV}: nothing reported to o11y", flush=True)
        return
    push(opts.base_url, samples, log, now, token)


if __name__ == "__main__":
    main()
