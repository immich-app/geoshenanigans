#!/usr/bin/env bash
# Runs the tiles alert rules' and dashboard panels' queries that are easy to get
# wrong (ratios over sparse series, labels a pusher adds) against a throwaway
# VictoriaMetrics loaded with hand-written samples, and checks what they return:
#
#   MISE_ENV=o11y mise run o11y:test-queries
#
# Each case writes its samples in an hour of its own, a day back (inside the
# default 1-month retention), and evaluates at that hour's end, so cases never
# share a window. Cases for rules that look back a day or more take negative
# numbers, 48 hours apart and before every other case, so none reaches into
# another's lookback (31h at most). The store dedups at 20s as o11y's does, and
# a series' samples within 20s of each other collapse into one: cases space
# theirs further apart.
# ALERTS and DASHBOARD point it at other files.
set -euo pipefail

src=$(cd "$(dirname "$0")" && pwd)
alerts=${ALERTS:-$src/alerts/geoshenanigans-tiles.yaml}
dashboard=${DASHBOARD:-$src/dashboards/geoshenanigans-tiles.json}

work=$(mktemp -d)
pid=
cleanup() {
  if [ -n "$pid" ]; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

# The store listens on a port the OS picks, free when bound and held by this
# store alone, and logs it ("started server at http://127.0.0.1:PORT/", at the
# default info level): it is probed only once the store has said where it is,
# never a port another store might already answer on. Its /metrics then has to
# name this run's data directory before any sample goes in (read whole into a
# file first: grep -q quitting mid-pipe would fail curl under pipefail).
victoria-metrics -storageDataPath="$work/data" -httpListenAddr=127.0.0.1:0 \
  -dedup.minScrapeInterval=20s -search.disableCache >"$work/vm.log" 2>&1 &
pid=$!
vm=
for _ in $(seq 150); do
  kill -0 "$pid" 2>/dev/null || break
  if [ -z "$vm" ]; then
    vm=$(grep -m 1 -o 'started server at http://127\.0\.0\.1:[0-9][0-9]*/' "$work/vm.log" || true)
    vm=${vm#started server at }
    vm=${vm%/}
  fi
  [ -n "$vm" ] && curl -fsS "$vm/health" >/dev/null 2>&1 && break
  sleep 0.2
done
if [ -z "$vm" ] || ! kill -0 "$pid" 2>/dev/null || ! curl -fsS -o "$work/metrics" "$vm/metrics" ||
  ! grep -qxF "flag{name=\"storageDataPath\", value=\"$work/data\", is_set=\"true\"} 1" "$work/metrics"; then
  echo "the VictoriaMetrics started for this run is not answering at ${vm:-(no address logged)}" >&2
  cat "$work/vm.log" >&2
  exit 1
fi

# The hour case N evaluates at: whole hours, N hours after a day ago.
base=$(($(date +%s) / 3600 * 3600 - 86400))
at() { echo $((base + $1 * 3600)); }

id='project="geoshenanigans",cluster="tiles",env="prod",provider="cloudflare",region="world"'
# sample CASE MINUTES_BEFORE_END NAME LABELS VALUE: one sample of the case's hour.
sample() {
  local ts=$(($(at "$1") - $2 * 60))
  echo "$3{$4${4:+,}$id} $5 ${ts}000" >>"$work/samples.prom"
}

rule_expr() {
  yq -r ".spec.rules[] | select(.uid == \"$1\") | .data[] | select(.refId == \"A\") | .model.expr" "$alerts"
}
panel_expr() {
  jq -er --argjson id "$1" --arg ref "$2" --arg interval "$3" \
    '.. | objects | select(.id? == $id and has("targets")) | .targets[] | select(.refId == $ref)
      | .expr | gsub("\\$__interval"; $interval)' "$dashboard"
}

failures=0
# check NAME CASE EXPR EXPECTED: EXPECTED is the result as sorted lines of "label=value,... value".
check() {
  local got
  got=$(curl -fsS "$vm/api/v1/query" --data-urlencode "query=$3" --data-urlencode "time=$(at "$2")" |
    jq -r '.data.result[] | "\(.metric | to_entries | sort_by(.key) | map("\(.key)=\(.value)") | join(",")) \(.value[1])"' | sort)
  if [ "$got" = "$4" ]; then
    echo "ok   $1"
  else
    failures=$((failures + 1))
    printf 'FAIL %s\n  expected: %s\n  got:      %s\n' "$1" "${4:-(nothing)}" "${got:-(nothing)}"
  fi
}

runs() { # runs CASE MINUTES...: one probe run at each
  local case=$1 m
  shift
  for m in "$@"; do sample "$case" "$m" tiles_bunny_probe_runs 'result="ok"' 1; done
}

# --- geoshenanigans-tiles-bunny-probe-errors -------------------------------
# Case 1: four runs 5 minutes apart; EU's miss tile errs in two of them and the
# other two got no verdict from EU. Every verdict is an error: fires at 1, though
# only half the runs saw one.
runs 1 20 15 10 5
sample 1 20 tiles_bunny_probe_errors 'continent="EU",kind="miss"' 1
sample 1 15 tiles_bunny_probe_errors 'continent="EU",kind="miss"' 1
for m in 20 15 10 5; do sample 1 "$m" tiles_bunny_probe_errors 'continent="NA",kind="miss"' 0; done
# Case 2: one run after a purge, when the popular tile is a miss too, fails for
# both targets: two errors, but of one run. EU: that run alone (2 of 2). SA: the
# next run's tests are fine, the popular tile a hit again (2 of 3 misses).
runs 2 15 5
sample 2 5 tiles_bunny_probe_errors 'continent="EU",kind="miss",target="hit"' 1
sample 2 5 tiles_bunny_probe_errors 'continent="EU",kind="miss",target="miss"' 1
sample 2 15 tiles_bunny_probe_errors 'continent="SA",kind="miss",target="hit"' 1
sample 2 15 tiles_bunny_probe_errors 'continent="SA",kind="miss",target="miss"' 1
sample 2 5 tiles_bunny_probe_errors 'continent="SA",kind="hit",target="hit"' 0
sample 2 5 tiles_bunny_probe_errors 'continent="SA",kind="miss",target="miss"' 0
# Case 6: two failing runs. After a purge both targets miss and fail in EU; the
# next run, the popular tile still a miss is fine and the random one fails again.
# The ratio adds up over target: 3 errors of 4 verdicts.
runs 6 15 5
sample 6 15 tiles_bunny_probe_errors 'continent="EU",kind="miss",target="hit"' 1
sample 6 15 tiles_bunny_probe_errors 'continent="EU",kind="miss",target="miss"' 1
sample 6 5 tiles_bunny_probe_errors 'continent="EU",kind="miss",target="hit"' 0
sample 6 5 tiles_bunny_probe_errors 'continent="EU",kind="miss",target="miss"' 1
# Case 3: nothing to fire on: one error in three verdicts (EU), a lone error (AF),
# and exactly half of four verdicts (NA, needs over half).
runs 3 20 15 10 5
for m in 20 15 10 5; do sample 3 "$m" tiles_bunny_probe_errors 'continent="EU",kind="hit"' 0; done
sample 3 15 tiles_bunny_probe_errors 'continent="EU",kind="miss"' 1
sample 3 10 tiles_bunny_probe_errors 'continent="EU",kind="miss"' 0
sample 3 5 tiles_bunny_probe_errors 'continent="EU",kind="miss"' 0
sample 3 5 tiles_bunny_probe_errors 'continent="AF",kind="miss"' 1
sample 3 20 tiles_bunny_probe_errors 'continent="NA",kind="miss"' 1
sample 3 15 tiles_bunny_probe_errors 'continent="NA",kind="miss"' 1
sample 3 10 tiles_bunny_probe_errors 'continent="NA",kind="miss"' 0
sample 3 5 tiles_bunny_probe_errors 'continent="NA",kind="miss"' 0
# Case 16: the TileJSON goes down with the CDN at the default 10 minutes: the
# Worker's read fails in the last two of three runs, so no tile is measured in
# them, and its own test (continent worker, kind tilejson) fires: 2 of 3.
for m in 25 15 5; do sample 16 "$m" tiles_bunny_probe_runs 'result="error"' 1; done
sample 16 25 tiles_bunny_probe_errors 'continent="worker",kind="tilejson",target="tilejson"' 0
sample 16 15 tiles_bunny_probe_errors 'continent="worker",kind="tilejson",target="tilejson"' 1
sample 16 5 tiles_bunny_probe_errors 'continent="worker",kind="tilejson",target="tilejson"' 1
# Case 17: one failed TileJSON read among good ones (a blip): no.
runs 17 25 5
sample 17 15 tiles_bunny_probe_runs 'result="error"' 1
sample 17 25 tiles_bunny_probe_errors 'continent="worker",kind="tilejson",target="tilejson"' 0
sample 17 15 tiles_bunny_probe_errors 'continent="worker",kind="tilejson",target="tilejson"' 1
sample 17 5 tiles_bunny_probe_errors 'continent="worker",kind="tilejson",target="tilejson"' 0

# --- geoshenanigans-tiles-bunny-probe-slow ---------------------------------
# Case 4, by continent:
# EU: two slow misses 10 minutes apart: fires at their average.
sample 4 15 tiles_bunny_probe_ttfb_ms 'continent="EU",kind="miss"' 1500
sample 4 5 tiles_bunny_probe_ttfb_ms 'continent="EU",kind="miss"' 1300
# AF: one slow miss and the next run skipped: no.
sample 4 5 tiles_bunny_probe_ttfb_ms 'continent="AF",kind="miss"' 1500
# NA: one 5s outlier between fast misses, averaging over 1s: no.
sample 4 25 tiles_bunny_probe_ttfb_ms 'continent="NA",kind="miss"' 50
sample 4 15 tiles_bunny_probe_ttfb_ms 'continent="NA",kind="miss"' 5000
sample 4 5 tiles_bunny_probe_ttfb_ms 'continent="NA",kind="miss"' 100
# OC: after a purge both targets miss, all slow, in series that differ in target,
# the random tile in two runs; its slow hits don't count.
sample 4 15 tiles_bunny_probe_ttfb_ms 'continent="OC",kind="miss",target="hit"' 1100
sample 4 15 tiles_bunny_probe_ttfb_ms 'continent="OC",kind="miss",target="miss"' 1500
sample 4 5 tiles_bunny_probe_ttfb_ms 'continent="OC",kind="miss",target="miss"' 1300
sample 4 5 tiles_bunny_probe_ttfb_ms 'continent="OC",kind="hit",target="hit"' 3000
sample 4 15 tiles_bunny_probe_ttfb_ms 'continent="OC",kind="hit",target="hit"' 3000
# AS: one run after a purge, both targets slow misses, and the next run skipped:
# two slow misses, but of one run: no.
sample 4 5 tiles_bunny_probe_ttfb_ms 'continent="AS",kind="miss",target="hit"' 1500
sample 4 5 tiles_bunny_probe_ttfb_ms 'continent="AS",kind="miss",target="miss"' 1500
# SA: misses slow before the window: no.
sample 4 50 tiles_bunny_probe_ttfb_ms 'continent="SA",kind="miss"' 1500
sample 4 40 tiles_bunny_probe_ttfb_ms 'continent="SA",kind="miss"' 1500
sample 4 5 tiles_bunny_probe_ttfb_ms 'continent="SA",kind="miss"' 100

# --- "Bunny tile cache hits by zoom" (panel 50) ----------------------------
# Case 5: z5 had only misses (0, not a gap), z7 hits 3 of 4, z9 nothing (a gap).
sample 5 5 tiles_bunny_tile_requests 'zoom="5",cache="MISS"' 40
sample 5 5 tiles_bunny_tile_requests 'zoom="7",cache="HIT"' 30
sample 5 5 tiles_bunny_tile_requests 'zoom="7",cache="MISS"' 10

# --- geoshenanigans-tiles-bunny-logs-failing -------------------------------
# The reader runs every minute; each run's result is one sample at its minute.
# Case 7: every read truncated (more rows than its page limit): each run still
# pushes the rows it counted, and is an error.
for m in $(seq 14 -1 1); do
  sample 7 "$m" tiles_bunny_log_reader_runs 'result="error"' 1
  sample 7 "$m" tiles_bunny_log_rows '' 300000
done
# Case 8: the reader just turned on with a key the Logging API refuses: skipped
# runs, then errors and never a row. Skipped runs are not in the ratio (7 of 7,
# not 7 of 14).
for m in $(seq 14 -1 8); do sample 8 "$m" tiles_bunny_log_reader_runs 'result="skipped"' 1; done
for m in $(seq 7 -1 1); do sample 8 "$m" tiles_bunny_log_reader_runs 'result="error"' 1; done
# Case 9: a blip of three failed runs among good ones: no.
for m in $(seq 14 -1 4); do sample 9 "$m" tiles_bunny_log_reader_runs 'result="ok"' 1; done
for m in 3 2 1; do sample 9 "$m" tiles_bunny_log_reader_runs 'result="error"' 1; done
# Case 10: the first four runs after the settings arrive all fail: under the
# floor of 5 yet.
for m in $(seq 14 -1 5); do sample 10 "$m" tiles_bunny_log_reader_runs 'result="skipped"' 1; done
for m in 4 3 2 1; do sample 10 "$m" tiles_bunny_log_reader_runs 'result="error"' 1; done

# --- geoshenanigans-tiles-bunny-probe-silent -------------------------------
# Case 11: the zone serves requests and the reader reports, but no probe run
# reported anything for the hour: fires.
for m in 55 45 35 25 15 5; do sample 11 "$m" tiles_bunny_log_rows '' 100; done
# Case 12: the same with every probe run skipped: -probe-off's, not this.
for m in 55 45 35 25 15 5; do sample 12 "$m" tiles_bunny_log_rows '' 100; done
for m in 50 40 30 20 10; do sample 12 "$m" tiles_bunny_probe_runs 'result="skipped"' 1; done
# Case 13: no requests in the hour (rows all 0): no.
for m in 55 45 35 25 15 5; do sample 13 "$m" tiles_bunny_log_rows '' 0; done
# Case 14: the reader stopped 20 minutes ago too (the whole cron): no.
for m in 55 45 35 25; do sample 14 "$m" tiles_bunny_log_rows '' 100; done

# --- "Probe first byte, misses" and "hits" (panels 53 and 54) -------------
# Case 15: per continent, every response weighs the same, whichever target it
# came from. EU misses: the random tile in three runs, the popular one once
# (after a purge): (100 + 200 + 300 + 1000) / 4, not (200 + 1000) / 2. EU hits:
# the popular tile in three runs, the random one once.
sample 15 25 tiles_bunny_probe_ttfb_ms 'continent="EU",kind="miss",target="miss"' 100
sample 15 15 tiles_bunny_probe_ttfb_ms 'continent="EU",kind="miss",target="miss"' 200
sample 15 5 tiles_bunny_probe_ttfb_ms 'continent="EU",kind="miss",target="miss"' 300
sample 15 15 tiles_bunny_probe_ttfb_ms 'continent="EU",kind="miss",target="hit"' 1000
sample 15 25 tiles_bunny_probe_ttfb_ms 'continent="EU",kind="hit",target="hit"' 20
sample 15 15 tiles_bunny_probe_ttfb_ms 'continent="EU",kind="hit",target="hit"' 30
sample 15 5 tiles_bunny_probe_ttfb_ms 'continent="EU",kind="hit",target="hit"' 40
sample 15 15 tiles_bunny_probe_ttfb_ms 'continent="EU",kind="hit",target="miss"' 200

# --- geoshenanigans-tiles-sample-silent ------------------------------------
# The watchdog reports the live build's age every 2 hours, the sample job its
# result once a day. Minutes before the case's end: 60 is an hour, 1680 28h.
# Case -48: a build live for over a day, and the last sample result 27h old:
# fires.
sample -48 1680 tiles_sync_live_build_age_hours '' 20
sample -48 60 tiles_sync_live_build_age_hours '' 5
sample -48 1620 tiles_sync_sample_runs 'result="ok"' 1
# Case -96: the same with a check skipped (a build pending) 20h ago: a report.
sample -96 1680 tiles_sync_live_build_age_hours '' 20
sample -96 60 tiles_sync_live_build_age_hours '' 5
sample -96 1200 tiles_sync_sample_runs 'result="skipped"' 1
# Case -144: the first build went live 3 hours ago, before any sample job ran
# against it: no.
sample -144 180 tiles_sync_live_build_age_hours '' 3
sample -144 60 tiles_sync_live_build_age_hours '' 5
# Case -192: the watchdog's report job stopped too (no age for 5h): that is
# geoshenanigans-tiles-watchdog-silent's.
sample -192 1680 tiles_sync_live_build_age_hours '' 20

curl -fsS -X POST --data-binary @"$work/samples.prom" "$vm/api/v1/import/prometheus"
curl -fsS "$vm/internal/force_flush" >/dev/null

errors=$(rule_expr geoshenanigans-tiles-bunny-probe-errors)
check 'probe-errors: errors over verdicts, not runs' 1 "$errors" 'continent=EU,kind=miss 1'
check 'probe-errors: one run after a purge, both targets failing' 2 "$errors" ''
check 'probe-errors: two failing runs, adding up over target' 6 "$errors" 'continent=EU,kind=miss 0.75'
check 'probe-errors: one error, a lone error, exactly half' 3 "$errors" ''
check 'probe-errors: the TileJSON read failing in two runs of three' 16 "$errors" \
  'continent=worker,kind=tilejson 0.6666666666666666'
check 'probe-errors: one failed TileJSON read' 17 "$errors" ''

slow=$(rule_expr geoshenanigans-tiles-bunny-probe-slow)
check 'probe-slow: slow misses in two runs, not one, nor one outlier' 4 "$slow" \
  "$(printf '%s\n' 'continent=EU,kind=miss 1400' 'continent=OC,kind=miss 1300')"

check 'cache hits by zoom: zero for misses only, gap for no requests' 5 "$(panel_expr 50 A 10m)" \
  "$(printf '%s\n' 'zoom=5 0' 'zoom=7 0.75')"

logs_failing=$(rule_expr geoshenanigans-tiles-bunny-logs-failing)
check 'logs-failing: truncated reads that still push rows' 7 "$logs_failing" ' 1'
check 'logs-failing: a refused key from the first run, skipped runs left out' 8 "$logs_failing" ' 1'
check 'logs-failing: a blip of three failed runs' 9 "$logs_failing" ''
check 'logs-failing: four failed runs, under the floor' 10 "$logs_failing" ''

probe_silent=$(rule_expr geoshenanigans-tiles-bunny-probe-silent)
silent='cluster=tiles,env=prod,project=geoshenanigans 1'
check 'probe-silent: requests, no probe result for an hour' 11 "$probe_silent" "$silent"
check 'probe-silent: skipped runs are reports' 12 "$probe_silent" ''
check 'probe-silent: no requests' 13 "$probe_silent" ''
check 'probe-silent: the whole cron stopped' 14 "$probe_silent" ''

check 'first byte, misses: weighted by responses, not by target' 15 "$(panel_expr 53 A 30m)" 'continent=EU 400'
check 'first byte, hits: weighted by responses, not by target' 15 "$(panel_expr 54 A 30m)" 'continent=EU 72.5'

sample_silent=$(rule_expr geoshenanigans-tiles-sample-silent)
check 'sample-silent: no result for 26h while a build is live' -48 "$sample_silent" "$silent"
check 'sample-silent: a skipped check is a report' -96 "$sample_silent" ''
check 'sample-silent: a build live for under a day' -144 "$sample_silent" ''
check 'sample-silent: the watchdog stopped too' -192 "$sample_silent" ''

if [ "$failures" -gt 0 ]; then
  echo "$failures check(s) failed" >&2
  exit 1
fi
echo "all query checks passed"
