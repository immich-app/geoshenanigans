#!/usr/bin/env python3
"""Reports the geocoder nightly to FUTO o11y.

Usage:
  o11y_report.py nightly --build-log FILE --memwatch-log FILE --results-dir DIR
      --output-dir DIR --build-date DATE --data-timestamp TIME --run-id ID
      --run-attempt N (--base-url URL | --dry-run)
  o11y_report.py watchdog --latest-url URL --run FILE --job FILE
      (--base-url URL | --dry-run)

nightly reports what only the build's runner has, from the files the run left
in its work dir: the build phases, peak memory, patch verify counts, patch and
index sizes and the data's age, and log lines for build.log, the memwatch
samples and the patch verify results. An input a run never wrote leaves its
metrics out. watchdog, every 2 hours on a GitHub-hosted runner, reports the
published build's dates from latest.json with a heartbeat, and the outcome,
duration and steps of the latest finished nightly (--run and --job, the GitHub
API's run and job objects): a run whose runner died or that timed out never
reaches its own report, so its outcome comes from here.

Both push to o11y's gateway at --base-url, with the bearer token in
$O11Y_TOKEN: the metrics as Prometheus text to VictoriaMetrics, the log lines
as JSON lines to VictoriaLogs, every sample and line stamped with the
project's identity labels. Without a token nothing is sent. --dry-run prints
both instead. A failed push is logged and the exit status stays 0: reporting
never fails the run.
"""
import argparse
import datetime
import http.client
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

NIGHTLY_APP = "geocoder-build"
WATCHDOG_APP = "geocoder-watchdog"
# o11y's identity labels, on every sample and log line. Merged last, so no
# label or field of ours can shadow them.
IDENTITY = {"project": "geoshenanigans", "cluster": "geocoder", "env": "prod", "provider": "github",
            "region": "global"}
METRICS_PATH = "/insert/0/prometheus/api/v1/import/prometheus"
LOGS_PATH = "/insert/jsonline"
TOKEN_ENV = "O11Y_TOKEN"
MIB = 1 << 20
MAX_MSG_BYTES = 4096
TRUNCATED = " [truncated]"
LOG_BATCH_LINES = 500
LOG_BATCH_BYTES = MIB
HTTP_TIMEOUT_SECONDS = 10
HTTP_RETRIES = 3
HTTP_RETRY_DELAY_SECONDS = 5
# How long after a nightly finishes the watchdog still logs it: its 2-hour
# cadence plus GitHub's schedule delays. Its metrics are pushed on every tick.
RUN_LOG_WINDOW_SECONDS = 9000

# geocoder-build.yml step name -> `step` label. Other steps get no metrics.
STEP_SLUGS = {
    "Download PBF and previous indexes": "download",
    "Run builder tests": "tests",
    "Apply replication diffs": "apply",
    "Build geocoder index": "build",
    "Generate patches and verify": "patches",
    "Compress index files": "compress",
    "Finalize upload and configurations.json": "finalize",
    "Cleanup old Tigris builds": "cleanup-tigris",
    "Cleanup old cache bucket": "cleanup-cache",
}

# memwatch field -> geocoder_build_peak_memory_bytes `kind`.
MEMORY_KINDS = {"anon": "anon", "file": "file", "file_dirty": "dirty", "others_rss": "others"}

# A top-level log_phase line of build-index: "  [<seconds>s ...] <name>".
# Nested phases indent their names.
PHASE_LINE = re.compile(r"^  \[(\d+(?:\.\d+)?)s[^\]]*\] (\S.*)$")
MEMWATCH_FIELD = re.compile(r"(\w+)=(\d+)Mi\b")


def unix_time(text):
    """Seconds since the epoch of a UTC date or time: 2026-10-12, 2026-10-12T06:17:00Z."""
    t = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
    if t.tzinfo is None:
        t = t.replace(tzinfo=datetime.timezone.utc)
    return int(t.timestamp())


def read_lines(path):
    """The file's lines; none when the run never wrote it."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return f.read().splitlines()
    except OSError:
        return []


def read_job(path):
    """The job object; empty when the API call failed."""
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def step_seconds(step):
    """0 for a step without times, one that never started."""
    if not (step.get("started_at") and step.get("completed_at")):
        return 0
    return unix_time(step["completed_at"]) - unix_time(step["started_at"])


def phases(build_log):
    """Seconds per top-level build-index phase in build.log's lines."""
    found = {}
    for line in build_log:
        match = PHASE_LINE.match(line)
        if match:
            found[match[2]] = round(found.get(match[2], 0) + float(match[1]), 1)
    return found


def peak_memory(memwatch_log):
    """Bytes per memory kind, the largest of the memwatch samples."""
    peaks = {}
    for line in memwatch_log:
        for field, mib in MEMWATCH_FIELD.findall(line):
            kind = MEMORY_KINDS.get(field)
            if kind:
                peaks[kind] = max(peaks.get(kind, 0), int(mib) * MIB)
    return peaks


def variants(output_dir):
    """The variant dirs holding index files, before or after compression (planet/full, ...)."""
    found = set()
    for root, _, names in os.walk(output_dir):
        if any(name.endswith((".bin", ".bin.zst")) for name in names):
            found.add(os.path.relpath(root, output_dir))
    return sorted(found)


def read_verify_results(results_dir, output_dir):
    """Each variant's generate_patches.sh result and mismatches. A variant
    without a result is None: its diff or patch tool crashed. Empty when
    patch generation never ran."""
    if not os.path.isdir(results_dir):
        return {}
    results = {}
    for variant in variants(output_dir):
        path = os.path.join(results_dir, variant.replace("/", "_"))
        lines = read_lines(path)
        results[variant] = (lines[0] if lines else None, " ".join(read_lines(path + ".mismatches")))
    return results


def verdict(result):
    """pass, fail or skip, as the workflow counts them (a crash fails)."""
    if result == "SKIP":
        return "skip"
    return "pass" if result and result.startswith("PASS") else "fail"


def read_patch_sizes(output_dir):
    """Bytes of each variant's patch.gcpatch; none for a build whose patches failed verify."""
    sizes = {}
    for root, _, names in os.walk(output_dir):
        if "patch.gcpatch" in names:
            sizes[os.path.relpath(root, output_dir)] = os.path.getsize(os.path.join(root, "patch.gcpatch"))
    return sizes


def read_index_bytes(output_dir):
    """Raw and compressed bytes of the index files, from the table
    compress_index.py writes into configurations.json."""
    try:
        with open(os.path.join(output_dir, "configurations.json")) as f:
            files = json.load(f).get("files") or {}
    except (OSError, ValueError):
        return {}
    if not files:
        return {}
    return {"raw": sum(e["size_raw"] for e in files.values()),
            "compressed": sum(e["size_zst"] for e in files.values())}


def capped(msg):
    """msg in at most MAX_MSG_BYTES of UTF-8, cut short and marked when longer."""
    data = msg.encode()
    if len(data) <= MAX_MSG_BYTES:
        return msg
    # Cutting the bytes can split the last character; ignore drops its start.
    return data[:MAX_MSG_BYTES - len(TRUNCATED)].decode(errors="ignore") + TRUNCATED


class Log:
    """Log records that share the app's and the run's context fields."""

    def __init__(self, **context):
        self.context = {key: value for key, value in context.items() if value}
        self.records = []

    def add(self, msg, step="", level="info"):
        record = {"_msg": capped(msg), "level": level}
        if step:
            record["step"] = step
        self.records.append({**record, **self.context, **IDENTITY})


def patch_samples(results_dir, output_dir, log):
    """Verify counts and patch sizes, with each variant's result logged."""
    samples = []
    results = read_verify_results(results_dir, output_dir)
    if results:
        counts = {"pass": 0, "fail": 0, "skip": 0}
        for variant, (result, mismatches) in results.items():
            outcome = verdict(result)
            counts[outcome] += 1
            text = result or "no verify result: its diff or patch tool crashed"
            log.add(f"{variant}: {text}" + (f" ({mismatches})" if mismatches else ""), step="patches",
                    level="error" if outcome == "fail" else "info")
        samples += [("geocoder_patch_verify_variants", {"result": outcome}, count) for outcome, count in counts.items()]
    sizes = read_patch_sizes(output_dir)
    samples += [("geocoder_patch_bytes", {"variant": variant}, size) for variant, size in sorted(sizes.items())]
    if sizes:
        samples.append(("geocoder_patch_bytes_sum", {}, sum(sizes.values())))
    return samples


def step_samples(job, log):
    """Duration and outcome of each finished step STEP_SLUGS names, skipped
    ones left out, with every finished step logged."""
    samples = []
    for step in job.get("steps") or []:
        if step.get("status") != "completed":
            continue
        slug = STEP_SLUGS.get(step["name"], "")
        seconds = step_seconds(step)
        log.add(f"{step['name']}: {step['conclusion']} in {seconds}s", step=slug,
                level="info" if step["conclusion"] in ("success", "skipped") else "error")
        if slug and step["conclusion"] != "skipped":
            samples.append(("geocoder_build_step_duration_seconds", {"step": slug}, seconds))
            samples.append(("geocoder_build_step_success", {"step": slug}, int(step["conclusion"] == "success")))
    return samples


def nightly(opts, now):
    """The run's samples and log, from its work dir."""
    log = Log(app=NIGHTLY_APP, run_id=opts.run_id, run_attempt=opts.run_attempt, build_date=opts.build_date)
    samples = []
    build_log, memwatch_log = read_lines(opts.build_log), read_lines(opts.memwatch_log)
    for line in build_log + memwatch_log:
        if line.strip():
            log.add(line, step="build")
    samples += [("geocoder_build_phase_duration_seconds", {"phase": name}, seconds)
                for name, seconds in phases(build_log).items()]
    samples += [("geocoder_build_peak_memory_bytes", {"kind": kind}, peak)
                for kind, peak in peak_memory(memwatch_log).items()]
    if opts.data_timestamp:
        samples.append(("geocoder_build_data_timestamp_seconds", {}, unix_time(opts.data_timestamp)))
    samples += patch_samples(opts.results_dir, opts.output_dir, log)
    samples += [("geocoder_index_bytes", {"kind": kind}, size)
                for kind, size in read_index_bytes(opts.output_dir).items()]
    return samples, log


def outcome_samples(run, job, now, log):
    """The finished nightly's outcome, duration and steps, from the GitHub API's
    run and job objects, at the time it finished: every tick re-sends them, and
    the store keeps one point per run. Logged on the first tick after it
    finished only, into log's records under the run's own context."""
    if not run.get("conclusion"):
        return []
    labels = {"trigger": run.get("event", ""), "runner": (job.get("labels") or [""])[0]}
    finished = unix_time(job.get("completed_at") or run["updated_at"])
    started = job.get("started_at") or run.get("run_started_at")
    samples = [("geocoder_build_last_run_timestamp_seconds", labels, finished),
               ("geocoder_build_success", labels, int(run["conclusion"] == "success"))]
    if started:
        samples.append(("geocoder_build_duration_seconds", labels, finished - unix_time(started)))
    run_log = Log(app=NIGHTLY_APP, run_id=str(run.get("id", "")), run_attempt=str(run.get("run_attempt", "")))
    samples += step_samples(job, run_log)
    took = f" in {finished - unix_time(started)}s" if started else ""
    run_log.add(f"run {run['conclusion']}{took}, {labels['trigger']} on {labels['runner']}: {run.get('html_url', '')}",
                level="info" if run["conclusion"] == "success" else "error")
    if now - finished <= RUN_LOG_WINDOW_SECONDS:
        log.records += run_log.records
    return [(name, labels, value, finished) for name, labels, value in samples]


def watchdog(opts, now):
    """The published build's dates with the heartbeat, and the latest finished
    nightly's outcome, with their log lines."""
    log = Log(app=WATCHDOG_APP)
    samples = []
    body, error = with_retries(lambda: fetch(opts.latest_url))
    if error is None:
        try:
            latest = json.loads(body)
            samples = [("geocoder_published_build_timestamp_seconds", {}, unix_time(latest["latest"])),
                       ("geocoder_published_updated_timestamp_seconds", {}, unix_time(latest["updated_at"]))]
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            error = f"unexpected content ({e!r})"
    if error:
        # No heartbeat either: the stale-build alert reads the published dates
        # and treats their absence as OK, so a watchdog that can't read them
        # has to fall silent for watchdog-silent to page.
        log.add(f"could not read {opts.latest_url}: {error}", level="error")
    else:
        samples.append(("geocoder_watchdog_heartbeat_timestamp_seconds", {}, int(now)))
        log.add(f"published build {latest['latest']}, updated {latest['updated_at']}")
    return samples + outcome_samples(read_job(opts.run), read_job(opts.job), now, log), log


def escape(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def exposition(samples, timestamp_ms):
    """Prometheus text with the identity labels, each sample at its own time
    (a fourth element, in seconds) or at timestamp_ms. It ends in a newline:
    VictoriaMetrics accepts a body without one and stores nothing of its last
    line."""
    lines = []
    for name, labels, value, *at in samples:
        selector = ",".join(f'{key}="{escape(v)}"' for key, v in {**labels, **IDENTITY}.items())
        lines.append(f"{name}{{{selector}}} {value} {at[0] * 1000 if at else timestamp_ms}\n")
    return "".join(lines)


def rfc3339(microseconds):
    """A UTC time in microseconds since the epoch as 2026-10-12T07:00:00.000001Z."""
    t = datetime.datetime.fromtimestamp(microseconds // 1_000_000, datetime.timezone.utc)
    return t.replace(microsecond=microseconds % 1_000_000).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def log_lines(records, now):
    """records as JSON lines, timed a microsecond apart from now: VictoriaLogs'
    `sort by (_time)` shuffles lines that share one."""
    start = int(now * 1_000_000)
    return [(json.dumps({"_time": rfc3339(start + i), **record}, ensure_ascii=False) + "\n").encode()
            for i, record in enumerate(records)]


def batches(lines):
    """lines joined into request bodies of at most LOG_BATCH_LINES lines and
    LOG_BATCH_BYTES (or one line, when that alone is longer)."""
    batch, size = [], 0
    for line in lines:
        if batch and (len(batch) == LOG_BATCH_LINES or size + len(line) > LOG_BATCH_BYTES):
            yield b"".join(batch)
            batch, size = [], 0
        batch.append(line)
        size += len(line)
    if batch:
        yield b"".join(batch)


def with_retries(request):
    """request()'s result and None, or None and the last error once every
    attempt failed. A 4xx is the request's own fault and isn't retried."""
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
        except (OSError, http.client.HTTPException) as e:
            error = str(e) or type(e).__name__
    return None, error


def response_text(error):
    """The start of an error response's body, where the gateway or the store
    says what it rejected, on one line."""
    try:
        with error:
            return " ".join(error.read(300).decode(errors="replace").split())
    except (OSError, http.client.HTTPException):
        return ""


def fetch(url):
    with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT_SECONDS) as response:
        return response.read()


def post(url, body, content_type, token):
    request = urllib.request.Request(url, data=body, method="POST", headers={
        "Content-Type": content_type, "Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        response.read()


def push_logs(url, lines, token):
    """The number of lines sent in batches, and the error that stopped them:
    the batches after a failed one would fail the same way."""
    sent = 0
    for body in batches(lines):
        _, error = with_retries(lambda: post(url, body, "application/stream+json", token))
        if error:
            return sent, error
        sent += body.count(b"\n")
    return sent, None


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Reports the geocoder nightly to FUTO o11y.")
    commands = parser.add_subparsers(dest="command", required=True)
    night = commands.add_parser("nightly", help="a nightly run's metrics and logs from its work dir")
    night.add_argument("--build-log", default="")
    night.add_argument("--memwatch-log", default="")
    night.add_argument("--results-dir", default="", help="generate_patches.sh's results dir")
    night.add_argument("--output-dir", default="", help="the build's output dir")
    night.add_argument("--build-date", default="")
    night.add_argument("--data-timestamp", default="", help="the data's OSM replication timestamp")
    night.add_argument("--run-id", default="")
    night.add_argument("--run-attempt", default="")
    dog = commands.add_parser("watchdog", help="the published build's age, a heartbeat and the latest nightly's outcome")
    dog.add_argument("--latest-url", required=True)
    dog.add_argument("--run", default="", help="the latest finished nightly's run object from the GitHub API, as JSON")
    dog.add_argument("--job", default="", help="that run's job object from the GitHub API, as JSON")
    for command in (night, dog):
        command.add_argument("--base-url", default="", help="o11y's gateway, e.g. https://vmauth.futostatus.com")
        command.add_argument("--dry-run", action="store_true", help="print the metrics and log lines; send nothing")
    opts = parser.parse_args(argv)
    if not opts.dry_run and not opts.base_url:
        parser.error("--base-url or --dry-run is required")
    return opts


def main(argv=None):
    opts = parse_args(argv)
    now = time.time()
    samples, log = (nightly if opts.command == "nightly" else watchdog)(opts, now)
    metrics = exposition(samples, int(now * 1000))
    if opts.dry_run:
        sys.stdout.write(metrics + b"".join(log_lines(log.records, now)).decode())
        return

    token = os.environ.get(TOKEN_ENV, "").strip()
    if not token:
        print(f"::warning::No ${TOKEN_ENV}: nothing reported to o11y", flush=True)
        return

    base_url = opts.base_url.rstrip("/")
    if samples:
        _, error = with_retries(lambda: post(base_url + METRICS_PATH, metrics.encode(), "text/plain", token))
        if error:
            print(f"::warning::o11y metrics push failed: {error}", flush=True)
            log.add(f"metrics push failed: {error}", level="error")
        else:
            print(f"Pushed {len(samples)} samples to o11y", flush=True)
    lines = log_lines(log.records, now)
    sent, error = push_logs(base_url + LOGS_PATH, lines, token)
    if error:
        print(f"::warning::o11y logs push failed after {sent} of {len(lines)} lines: {error}", flush=True)
    else:
        print(f"Pushed {sent} log lines to o11y", flush=True)


if __name__ == "__main__":
    main()
