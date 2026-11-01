#!/usr/bin/env bash
# Tests tiles-cleanup.sh against a throwaway git repo, with rclone and the served-key lookup stubbed out.
#
#   deployment/scripts/tiles-cleanup.test.sh
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
failures=0

# The bucket is a directory per area listing ("root" for the unprefixed one); purges are appended to a log.
mkdir -p "$work/bin" "$work/bucket"
cat > "$work/bin/rclone" <<'STUB'
#!/usr/bin/env bash
case "$1" in
  lsf) area=${3#tigris:tiles-geo/}; area=${area%/}; cat "$BUCKET/${area:-root}" ;;
  purge) echo "${2#tigris:tiles-geo/}" >> "$BUCKET/purged" ;;
  *) exit 1 ;;
esac
STUB
chmod +x "$work/bin/rclone"
export PATH="$work/bin:$PATH" BUCKET="$work/bucket"

setup_repo() {
  rm -rf "$work/repo" "$work/bucket/purged"
  mkdir -p "$work/repo/deployment/scripts" "$work/repo/deployment/modules/cloudflare/tiles-worker"
  cp "$script_dir/tiles-cleanup.sh" "$work/repo/deployment/scripts/"
  # Served keys come from SERVED_<ENV>.
  # shellcheck disable=SC2016 # the stub expands these itself
  printf '#!/usr/bin/env bash\nv="SERVED_${1^^}"\necho "${!v:?no served key for $1}"\n' \
    > "$work/repo/deployment/scripts/tiles-served-key.sh"
  chmod +x "$work/repo/deployment/scripts/"*.sh
  git -C "$work/repo" init -q
}

# record <env> <key> <prefix>: commit what main says an environment serves.
record() {
  printf '{"pmtiles_deployment_key":"%s","storage_prefix":"%s"}\n' "$2" "$3" \
    > "$work/repo/deployment/modules/cloudflare/tiles-worker/tiles.$1.tfvars.json"
  git -C "$work/repo" add -A
  git -C "$work/repo" -c user.name=test -c user.email=test@example.com commit -qm "$1 $2"
}

listing() {
  local area=$1 dir
  shift
  : > "$work/bucket/$area"
  for dir in "$@"; do
    echo "$dir/" >> "$work/bucket/$area"
  done
}

# expect <name> <env> <expected purges, space separated>
expect() {
  local name=$1 env=$2 expected=$3 output actual
  rm -f "$work/bucket/purged"
  if ! output=$(cd "$work/repo" && deployment/scripts/tiles-cleanup.sh "$env" 2>&1); then
    echo "FAIL $name: cleanup exited non-zero"
    echo "$output"
    failures=$((failures + 1))
    return
  fi
  actual=""
  if [ -f "$work/bucket/purged" ]; then
    actual=$(sort "$work/bucket/purged" | tr '\n' ' ' | sed 's/ $//')
  fi
  if [ "$actual" = "$expected" ]; then
    echo "ok   $name"
  else
    echo "FAIL $name: purged '$actual', expected '$expected'"
    echo "$output"
    failures=$((failures + 1))
  fi
}

# expect_dry_run <name> <env> <key that would go>
expect_dry_run() {
  local name=$1 env=$2 key=$3 output
  rm -f "$work/bucket/purged"
  if ! output=$(cd "$work/repo" && deployment/scripts/tiles-cleanup.sh "$env" --dry-run 2>&1); then
    echo "FAIL $name: dry run exited non-zero"
    echo "$output"
    failures=$((failures + 1))
  elif [ -s "$work/bucket/purged" ] || ! grep -qx "would delete $key" <<<"$output"; then
    echo "FAIL $name: expected 'would delete $key' and no purges"
    echo "$output"
    failures=$((failures + 1))
  else
    echo "ok   $name"
  fi
}

# expect_refusal <name> <args...>
expect_refusal() {
  local name=$1
  shift
  rm -f "$work/bucket/purged"
  if (cd "$work/repo" && deployment/scripts/tiles-cleanup.sh "$@" >/dev/null 2>&1) || [ -s "$work/bucket/purged" ]; then
    echo "FAIL $name: expected cleanup to stop without deleting anything"
    failures=$((failures + 1))
  else
    echo "ok   $name"
  fi
}

# Before dev's first deploy it reads prod's area; 20261101 is an upload whose deploy failed.
setup_repo
record prod 20250326 ""
record prod 20250818 ""
record dev 20250818 ""
listing root 20240101 20250326 20250818 20261101 dev styles
listing dev
export SERVED_PROD=20250818 SERVED_DEV=20250818
expect_dry_run "a dry run only reports what it would delete" prod "20240101"
expect_refusal "a misspelled dry-run flag deletes nothing" prod --dryrun
expect_refusal "extra arguments delete nothing" prod --dry-run extra
expect "prod keeps live, previous and dev's key, and drops a failed upload" prod "20240101 20261101"
expect "dev leaves its unused area alone" dev ""

# Dev's first deploy moved it to dev/.
record dev 20261102 "dev/"
listing dev 20261030 20261102
export SERVED_DEV=20261102
expect "dev cleans its own area" dev "dev/20261030"
listing root 20250326 20250818
expect "prod keeps its live and previous deployments once dev has moved" prod ""

# Dev's activation committed dev/ but the apply failed: dev still serves 20250326 from prod's area while prod has
# moved past it twice.
setup_repo
record prod 20250326 ""
record dev 20250326 ""
record prod 20250818 ""
record prod 20261101 ""
record dev 20261102 "dev/"
listing root 20240101 20250326 20250818 20261101
listing dev 20261102
export SERVED_PROD=20261101 SERVED_DEV=20250326
expect "prod keeps what dev still serves after a failed activation" prod "20240101"

# Without dev's served key, prod can't know what dev still needs.
unset SERVED_DEV
expect_refusal "prod stops when another environment's served key is unknown" prod

# A failed listing must fail the run.
export SERVED_DEV=20261102
rm "$work/bucket/root"
expect_refusal "a failed listing fails the run" prod

if [ "$failures" -gt 0 ]; then
  echo "$failures failed"
  exit 1
fi
