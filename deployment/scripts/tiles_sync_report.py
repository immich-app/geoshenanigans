#!/usr/bin/env python3
"""Reports the tiles sync (tiles-sync.yml) and its watchdog to FUTO o11y.

Usage:
  tiles_sync_report.py build-info --url URL
  tiles_sync_report.py sync --build KEY [--live KEY] [--version V] [--osm-date TIME]
      [--live-version V] [--resolved-status STATUS] [--report FILE] [--resolve-result R] [--sync-result R]
      [--download-outcome O] [--download-seconds N] [--started-at T] [--finished-at T]
      [--run-id ID] [--run-attempt N] [--run-url URL] [--trigger EVENT] (--base-url URL | --dry-run)
  tiles_sync_report.py watchdog --status FILE [--stderr FILE] (--base-url URL | --dry-run)

build-info prints the tiles schema version and the OSM replication time of a
Protomaps build as JSON ({"version", "osmDate"}, null when the archive's
metadata has none), read from the archive's header and metadata with two range
requests, before the 140 GB download.

sync reports one run of the sync workflow: the report JSON `npm run bunny:sync
-- --report` wrote (tiles/src/bunny/cli/sync.ts) when it ran, and otherwise
what the workflow decided by itself (`skipped` when the build is already live,
`schema_changed` when its schema version differs from the live build's) or
why the run failed before the CLI could report (the jobs' results and the
download step's outcome). It adds the download step's seconds and the run's
total: the sync job's first step to its last.

watchdog reports the live build from `npm run bunny:status -- --json`, with a
heartbeat. A watchdog that cannot read the live state sends no heartbeat, so
the watchdog-silent alert pages for it.

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
import struct
import sys
import time
import urllib.error
import urllib.request

SYNC_APP = "tiles-sync"
WATCHDOG_APP = "tiles-sync-watchdog"
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

STATUSES = ("success", "skipped", "guard_rail", "failed", "schema_changed")
# Runs that leave the zone as it should be: a skipped run found the newest
# build live already. They move tiles_sync_last_success_timestamp_seconds.
HEALTHY = ("success", "skipped")
# Statuses the workflow decides before the CLI runs.
RESOLVED_STATUSES = ("skipped", "schema_changed")

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
        request = urllib.request.Request(url, headers={"Range": f"bytes={offset}-{offset + length - 1}",
                                                       "User-Agent": USER_AGENT})
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            # A server that ignores Range answers 200 with the whole archive.
            if response.status != 206:
                raise ValueError(f"expected a 206 range response, got {response.status}")
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


# ── sync ────────────────────────────────────────────────────────────────────


def failure_reason(opts):
    """Why a run without a report failed, from the jobs' results and the download step's outcome."""
    if opts.resolve_result not in ("", "success"):
        return f"resolving the build ended {opts.resolve_result}; see the Resolve build job"
    if opts.sync_result in ("", "skipped"):
        return "the sync job did not run"
    if opts.sync_result == "cancelled":
        return "the sync job was cancelled or timed out"
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
    return "failed", failure_reason(opts)


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
        if waits:
            slowest = max(waits, key=waits.get)
            what += f", slowest {slowest} {waits[slowest]:,.0f}s"
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
    level = "info" if status in HEALTHY else "error"
    log.add(describe(status, build, previous, opts.live_version, opts.version, osm_date, report, steps)
            + (f": {opts.run_url}" if opts.run_url else ""), level=level)
    if error:
        log.add(f"tiles sync error: {error}", level="error")
    return samples, log


# ── watchdog ────────────────────────────────────────────────────────────────


def hours(value):
    return f"{value:.1f}h" if number(value) is not None else "unknown"


def watchdog(opts, now):
    """The live build's ages with the heartbeat, or an error line and nothing else when the state is unreadable."""
    log = Log(app=WATCHDOG_APP)
    status = read_json(opts.status)
    live = (status or {}).get("liveBuild")
    if not isinstance(live, str) or not live:
        detail = read_tail(opts.stderr) or "no output"
        log.add(f"could not read the live tiles state with bunny:status: {detail}", level="error")
        return [], log
    samples = [("tiles_sync_watchdog_heartbeat", {}, int(now))]
    ages = {"buildAgeHours": "tiles_sync_live_build_age_hours", "osmAgeHours": "tiles_sync_live_osm_age_hours"}
    for field, name in ages.items():
        value = number(status.get(field))
        if value is not None:
            samples.append((name, {}, value))
    osm_date, updated = status.get("osmDate") or "unknown", status.get("updatedAt") or "unknown"
    log.add(f"live build {live} (OSM {osm_date}), updated {updated}:"
            f" build {hours(status.get('buildAgeHours'))} old, OSM data {hours(status.get('osmAgeHours'))} old")
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


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Reports the tiles sync and its watchdog to FUTO o11y.")
    commands = parser.add_subparsers(dest="command", required=True)
    info = commands.add_parser("build-info", help="a Protomaps build's schema version and OSM time, as JSON")
    info.add_argument("--url", required=True, help="the archive, e.g. https://build.protomaps.com/20261118.pmtiles")
    run = commands.add_parser("sync", help="one run of tiles-sync.yml")
    run.add_argument("--build", default="", help="the build the run resolved (YYYYMMDD)")
    run.add_argument("--live", default="", help="the build live when it started")
    run.add_argument("--version", default="", help="the build's tiles schema version")
    run.add_argument("--live-version", default="", help="the live build's tiles schema version")
    run.add_argument("--osm-date", default="", help="the build's OSM replication time")
    run.add_argument("--resolved-status", default="", help="what the workflow decided without running the sync: "
                     + " or ".join(RESOLVED_STATUSES) + " (anything else is ignored)")
    run.add_argument("--report", default="", help="the sync CLI's --report JSON")
    run.add_argument("--resolve-result", default="", help="the resolve job's result")
    run.add_argument("--sync-result", default="", help="the sync job's result")
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
    dog.add_argument("--stderr", default="", help="its stderr, quoted when the status is unreadable")
    for command in (run, dog):
        command.add_argument("--base-url", default="", help="o11y's gateway, e.g. https://vmauth.futostatus.com")
        command.add_argument("--dry-run", action="store_true", help="print the metrics and log lines; send nothing")
    opts = parser.parse_args(argv)
    if opts.command != "build-info" and not opts.dry_run and not opts.base_url:
        parser.error("--base-url or --dry-run is required")
    return opts


def optional_float(text):
    """An empty argument (an output of a job that never ran) is None."""
    return float(text) if text.strip() else None


def main(argv=None):
    opts = parse_args(argv)
    if opts.command == "build-info":
        info, error = with_retries(lambda: build_info(range_reader(opts.url)))
        if error:
            print(f"could not read {opts.url}: {error}", file=sys.stderr)
            sys.exit(1)
        print(json.dumps(info))
        return

    now = time.time()
    samples, log = (sync if opts.command == "sync" else watchdog)(opts, now)
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
