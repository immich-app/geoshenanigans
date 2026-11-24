#!/usr/bin/env bash
# Tests entrypoint.sh: a fresh volume gets the test account the page uses, a
# volume's own accounts stay, and the container is query-portal serving the
# data dir, its exit status the container's. query-portal is stubbed out: it
# prints its arguments and exits with $PORTAL_STATUS. node reads the account's
# JSON.
#
#   geocoder/test-portal/entrypoint.test.sh
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
entrypoint="$script_dir/entrypoint.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
failures=0

mkdir -p "$work/bin"
printf '#!/bin/sh\necho "query-portal $*"\nexit "${PORTAL_STATUS:-0}"\n' > "$work/bin/query-portal"
chmod +x "$work/bin/query-portal"
export PATH="$work/bin:$PATH"

# start <data dir> [VAR=value...]: runs the entrypoint as the container does;
# its output is in $work/out, its exit status in $status.
start() {
  local data=$1
  shift
  set +e
  env DATA_DIR="$data" "$@" sh "$entrypoint" > "$work/out" 2>&1
  status=$?
  set -e
}

# check <name> <condition...>: reports the case; on failure, the entrypoint's output.
check() {
  local name=$1
  shift
  local why
  if why=$("$@" 2>&1); then
    echo "ok   $name"
  else
    echo "FAIL $name: $* ${why}"
    sed 's/^/       /' "$work/out"
    failures=$((failures + 1))
  fi
}

serves() { [ "$status" = 0 ] && [ "$(tail -n1 "$work/out")" = "query-portal $1 0.0.0.0:3000" ]; }

# test_account <file>: whether the file parses as JSON and is the test account
# alone: its key "test", no admin, 200 requests a second and 5000000 a day,
# not rated by IP.
test_account() {
  node -e '
    const db = JSON.parse(require("fs").readFileSync(process.argv[1], "utf8"));
    require("assert").deepStrictEqual(db, {
      users: { test: { password_hash: "", admin: false, rate_per_second: 200, rate_per_day: 5000000, rate_by_ip: false } },
      tokens: { test: "test" },
    });
  ' "$1"
}

# A fresh volume gets the test account and is served.
d="$work/fresh"
start "$d"
check "a fresh volume is served" serves "$d"
check "a fresh volume gets the test account" test_account "$d/geocoder.json"

# A volume's own accounts stay as they are.
d="$work/accounts"
mkdir -p "$d"
echo '{"users":{"kept":{}}}' > "$d/geocoder.json"
start "$d"
check "a volume's accounts stay" test "$(cat "$d/geocoder.json")" = '{"users":{"kept":{}}}'
check "a volume with accounts is served" serves "$d"

# The container exits with query-portal's status.
start "$work/exit" PORTAL_STATUS=3
check "query-portal's exit status is the container's" test "$status" = 3

if [ "$failures" -gt 0 ]; then
  echo "$failures failure(s)"
  exit 1
fi
echo "all passed"
