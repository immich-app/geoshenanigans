#!/usr/bin/env bash
# Usage: workflow_runs.sh ROUTE FROM TO
#
# Prints the runs the GitHub API's workflow run list ROUTE (its path and its
# filters, e.g. repos/<repo>/actions/workflows/<file>/runs?branch=main) has
# that were created from FROM to TO (UTC times as `date -d` reads them, both
# included), one JSON object a line, newest first, each once. A filtered list
# holds at most its 1,000 newest runs, however many more match it: a time
# range whose list is that full is listed again in halves, the later first,
# each split the same way, down to a second; a second whose list is that
# full fails the listing, which can't be whole. A run created while a list is
# paged shifts its later pages, which then repeat a run: each run is printed
# once, by its id. The watchdog counts today's attempts of the live nightly
# by it, and finds the latest finished one.
set -euo pipefail

route=$1
case $route in *\?*) sep="&" ;; *) sep="?" ;; esac
# The most runs a filtered list holds.
cap=1000

# The runs created from second $1 to second $2 (since the epoch, both
# included).
between() {
  local from=$1 to=$2 range runs mid
  range="$(date -u -d "@$from" +%Y-%m-%dT%H:%M:%SZ)..$(date -u -d "@$to" +%Y-%m-%dT%H:%M:%SZ)"
  runs=$(gh api --paginate "$route${sep}created=$range&per_page=100" --jq '.workflow_runs[]' | jq -c .)
  if [ -n "$runs" ] && [ "$(wc -l <<< "$runs")" -ge "$cap" ]; then
    if [ "$from" -lt "$to" ]; then
      mid=$(( (from + to) / 2 ))
      between $(( mid + 1 )) "$to"
      between "$from" "$mid"
      return
    fi
    echo "workflow_runs.sh: $cap or more runs created at $range: the list can't be whole" >&2
    exit 1
  fi
  [ -z "$runs" ] || echo "$runs"
}

between "$(date -u -d "$2" +%s)" "$(date -u -d "$3" +%s)" \
  | jq -c -s 'unique_by(.id) | sort_by(.created_at, .id) | reverse | .[]'
