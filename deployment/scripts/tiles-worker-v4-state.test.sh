#!/usr/bin/env bash
# Tests tiles-worker-v4-state.sh against synthetic states, with terragrunt stubbed out.
#
#   deployment/scripts/tiles-worker-v4-state.test.sh
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
failures=0

# The stub serves $STATE for state pull, drops the given addresses from it for state rm, and logs each state rm and
# plan's arguments. FAIL_PULL_AFTER_RM makes the pull after a state rm fail.
mkdir -p "$work/bin"
cat > "$work/bin/terragrunt" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
[ "$1" = run ] && [ "$2" = --non-interactive ] && [ "$3" = -- ] || exit 64
shift 3
case "$1 ${2:-}" in
  "init "*) ;;
  "state pull")
    if [ -n "${FAIL_PULL_AFTER_RM:-}" ] && [ -f "$WORK/rm.log" ]; then
      exit 1
    fi
    if [ -s "$STATE" ]; then cat "$STATE"; fi
    ;;
  "state rm")
    shift 2
    echo "$*" >> "$WORK/rm.log"
    addresses=()
    for arg in "$@"; do
      case "$arg" in -*) ;; *) addresses+=("$arg") ;; esac
    done
    jq --args '
      .resources |= map(. as $r | .instances |= map(select(
        ("\($r.type).\($r.name)" + (if .index_key == null then "" else "[\(.index_key | tojson)]" end))
        | IN($ARGS.positional[]) | not)) | select(.instances | length > 0))' \
      "${addresses[@]}" < "$STATE" > "$STATE.new"
    mv "$STATE.new" "$STATE"
    ;;
  "plan "*)
    shift
    echo "$*" >> "$WORK/plan.log"
    ;;
  *) exit 64 ;;
esac
STUB
chmod +x "$work/bin/terragrunt"
export PATH="$work/bin:$PATH" WORK="$work" STATE="$work/state.json" TF_VAR_env=prod

v4_cron='{"mode":"managed","type":"cloudflare_workers_cron_trigger","name":"tiles","instances":[
  {"schema_version":0,"attributes":{"id":"63f8","schedules":["* * * * *"],"script_name":"tiles-prod"}}]}'
v4_domain='{"mode":"managed","type":"cloudflare_workers_domain","name":"tiles","instances":[
  {"schema_version":0,"attributes":{"id":"784d","hostname":"tiles.prod.immich.cloud"}}]}'
v4_domain_indexed='{"mode":"managed","type":"cloudflare_workers_domain","name":"tiles_prod","instances":[
  {"index_key":0,"schema_version":0,"attributes":{"id":"17b3","hostname":"tiles.immich.cloud"}}]}'
v5_script='{"mode":"managed","type":"cloudflare_workers_script","name":"tiles","instances":[
  {"schema_version":500,"attributes":{"id":"tiles-prod","script_name":"tiles-prod"}}]}'
v5_cron='{"mode":"managed","type":"cloudflare_workers_cron_trigger","name":"heartbeat","instances":[
  {"schema_version":500,"attributes":{"id":"tiles-prod","schedules":[{"cron":"* * * * *"}]}}]}'
v5_domain='{"mode":"managed","type":"cloudflare_workers_custom_domain","name":"tiles","instances":[
  {"index_key":"tiles.immich.cloud","schema_version":500,"attributes":{"id":"17b3"}}]}'

# state <resource JSON...>: the state the stub serves.
state() {
  jq -n '{version: 4, resources: $ARGS.positional | map(fromjson)}' --args "$@" > "$STATE"
}

# run <mode> [env var=value...]: runs the script, keeping its output, exit status and step outputs.
run() {
  local mode=$1
  shift
  rm -f "$work/rm.log" "$work/plan.log" "$work/output"
  status=0
  output=$(env "$@" GITHUB_OUTPUT="$work/output" "$script_dir/tiles-worker-v4-state.sh" "$mode" 2>&1) || status=$?
}

check() {
  local name=$1
  shift
  if "$@"; then
    echo "ok   $name"
  else
    echo "FAIL $name"
    echo "$output"
    failures=$((failures + 1))
  fi
}

addresses() {
  jq -r '.resources[] | . as $r | .instances[]
    | "\($r.type).\($r.name)" + (if .index_key == null then "" else "[\(.index_key | tojson)]" end)' "$STATE" | sort |
    tr '\n' ' '
}

state "$v5_script" "$v4_cron" "$v4_domain" "$v4_domain_indexed"
cp "$STATE" "$work/v4.json"
run plan
check "plan excludes each v4 entry, the indexed one by instance" \
  test "$status-$(cat "$work/plan.log")" = \
  "0--input=false -exclude=cloudflare_workers_cron_trigger.tiles -exclude=cloudflare_workers_domain.tiles -exclude=cloudflare_workers_domain.tiles_prod[0]"
check "plan reports that it planned the module" grep -qx planned=true "$work/output"
check "plan writes no state" cmp -s "$STATE" "$work/v4.json"
check "plan removes nothing" test ! -e "$work/rm.log"

run forget
check "forget removes exactly the v4 entries" \
  test "$status-$(cat "$work/rm.log")" = \
  "0--lock-timeout=1m cloudflare_workers_cron_trigger.tiles cloudflare_workers_domain.tiles cloudflare_workers_domain.tiles_prod[0]"
check "forget leaves the v5 entries" test "$(addresses)" = "cloudflare_workers_script.tiles "

run forget
check "forget again removes nothing" test "$status" = 0 -a ! -e "$work/rm.log"

state "$v5_script" "$v5_cron" "$v5_domain"
run forget
check "forget leaves a migrated state alone" test "$status" = 0 -a ! -e "$work/rm.log"
run plan
check "plan leaves a migrated state to Plan All" \
  test "$status-$(cat "$work/output")" = "0-planned=false" -a ! -e "$work/plan.log"

: > "$STATE"
run forget
check "forget does nothing without state" test "$status" = 0 -a ! -e "$work/rm.log"

state "$v5_script" "$v4_cron"
run forget FAIL_PULL_AFTER_RM=1
check "forget fails when the state read after removal fails" test "$status" -ne 0

run plan TF_VAR_env=
check "it refuses to run without an environment" test "$status" -ne 0 -a ! -e "$work/plan.log"

if [ "$failures" -gt 0 ]; then
  echo "$failures failed"
  exit 1
fi
