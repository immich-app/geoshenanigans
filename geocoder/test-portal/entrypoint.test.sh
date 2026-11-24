#!/usr/bin/env bash
# Tests entrypoint.sh's index cache: a server starts on the dataset it finds
# only when that is a finished download of the selection it serves, and
# otherwise downloads the whole selection afresh, the files of a transport
# unit through the unit, beside the dataset it replaces once the download is
# whole, and only from a build of the build version the server reads. curl,
# geocoder-transport and query-server are stubbed out; a directory stands in
# for the bucket.
#
#   geocoder/test-portal/entrypoint.test.sh
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
entrypoint="$script_dir/entrypoint.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
failures=0
build=2026-11-01

# curl serves <bucket>/<path after builds/> and logs each URL; the bucket holds
# its files plain, which the entrypoint fetches when a .zst is missing, and its
# units. With -w it writes the HTTP status, 404 for a file the bucket lacks. An
# object in $FLAKY (its path under planet/, a file's without .zst, one a line)
# breaks off halfway, as a dropped connection does: half of it is written,
# then curl's error.
mkdir -p "$work/bin"
cat > "$work/bin/curl" <<'STUB'
#!/usr/bin/env bash
out= url= fmt=
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out=$2; shift ;;
    -w) fmt=$2; shift ;;
    http*) url=$1 ;;
  esac
  shift
done
echo "$url" >> "$CURL_LOG"
rel=${url#*/builds/}
src="$BUCKET/$rel"
key=${rel#*/planet/}
if [ -n "${FLAKY:-}" ] && printf '%s\n' "$FLAKY" | grep -qxF -e "$key" -e "${key%.zst}"; then
  whole="$BUCKET/$rel"
  [ -f "$whole" ] || whole="$BUCKET/${rel%.zst}"
  head -c $(($(stat -c%s "$whole") / 2)) "$whole" > "$out"
  [ -z "$fmt" ] || printf 200
  exit 18
fi
if [ ! -f "$src" ]; then
  [ -z "$fmt" ] || printf 404
  exit 22
fi
if [ -n "$out" ]; then cp "$src" "$out"; else cat "$src"; fi
[ -z "$fmt" ] || printf 200
STUB
# geocoder-transport decode <unit> <dir>: the stub's units are tar archives of
# their files, all written or, for one it can't read whole, none. Each decode
# is logged.
cat > "$work/bin/geocoder-transport" <<'STUB'
#!/usr/bin/env bash
echo "$*" >> "$TRANSPORT_LOG"
[ "$1" = decode ] || exit 2
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
if ! tar -xf "$2" -C "$tmp" 2> /dev/null || [ -z "$(ls -A "$tmp")" ]; then
  echo "geocoder-transport: $2: not a whole unit" >&2
  exit 1
fi
mv "$tmp"/* "$3"/
STUB
printf '#!/bin/sh\necho "query-server $*"\n' > "$work/bin/query-server"
chmod +x "$work/bin/"*
export PATH="$work/bin:$PATH" CURL_LOG="$work/curl.log" TRANSPORT_LOG="$work/transport.log"

# list [VAR=value...]: the "<dir> <file>" lines of the selection.
list() {
  env "$@" DATA_DIR="$work/unused" sh "$entrypoint" --list-files
}

# unit_of <file>: the transport unit that holds it, or nothing.
unit_of() {
  env DATA_DIR="$work/unused" sh "$entrypoint" --unit-of "$1"
}

# The build version of the files the server reads.
version=$(env DATA_DIR="$work/unused" sh "$entrypoint" --build-version)

# latest_json <build version>: the bucket's latest.json, naming $build as a
# build of that version.
latest_json() {
  printf '{\n  "build_version": %s,\n  "latest": "%s"\n}\n' "$1" "$build"
}

# The build: every file of each selection the cases run, 1 KiB, its first line
# naming its build and path. The bucket publishes each plain, but those a
# transport unit holds, which go in the unit (a tar of them, as the stub reads).
files="$work/files"
bucket="$work/bucket"
mkdir -p "$bucket"
latest_json "$version" > "$bucket/latest.json"
{ list; list QUALITY=exact; list MINIMAL=1; } | sort -u | while read -r dir f; do
  mkdir -p "$files/$dir" "$bucket/$build/planet/$dir"
  { echo "$build $dir/$f"; head -c 1024 /dev/zero; } > "$files/$dir/$f"
  unit=$(unit_of "$f")
  if [ -z "$unit" ]; then
    cp "$files/$dir/$f" "$bucket/$build/planet/$dir/$f"
  else
    tar -b 1 -rf "$bucket/$build/planet/$dir/$unit" -C "$files/$dir" "$f"
  fi
done

# published <dir> <file>: whether $BUCKET has the file, plain or in its unit.
published() {
  local unit
  unit=$(unit_of "$2")
  [ -f "$BUCKET/$build/planet/$1/${unit:-$2}" ]
}

# bucket_without <file or unit under planet/>: a copy of the bucket lacking it.
bucket_without() {
  local copy="$work/bucket-$RANDOM"
  cp -r "$bucket" "$copy"
  rm "$copy/$build/planet/$1"
  echo "$copy"
}

# bucket_of_version <build version>: a copy of the bucket whose latest.json
# names its build as one of that version.
bucket_of_version() {
  local copy="$work/bucket-$RANDOM"
  cp -r "$bucket" "$copy"
  latest_json "$1" > "$copy/latest.json"
  echo "$copy"
}

# start <data dir> [VAR=value...]: runs the entrypoint as the container does,
# on the bucket in $BUCKET; its output is in $work/out, its exit status in
# $status, the URLs it fetched in $CURL_LOG and the units it decoded in
# $TRANSPORT_LOG.
start() {
  local data=$1
  shift
  : > "$CURL_LOG"
  : > "$TRANSPORT_LOG"
  set +e
  env DATA_DIR="$data" "$@" sh "$entrypoint" > "$work/out" 2>&1
  status=$?
  set -e
}

# holds <data dir> [VAR=value...]: whether <data dir>/index holds the build's
# copy of each file of the selection the bucket has, and nothing more but the
# accounts and the download's marker.
holds() {
  local index=$1/index dir f
  shift
  while read -r dir f; do
    if published "$dir" "$f"; then
      cmp -s "$files/$dir/$f" "$index/$f" || { echo "$dir/$f is not the build's"; return 1; }
    elif [ -e "$index/$f" ]; then
      echo "$f is not in the build"; return 1
    fi
  done < <(list "$@")
  local extra
  extra=$(cd "$index" && ls -A | grep -vxF -f <(list "$@" | cut -d' ' -f2; echo geocoder.json; echo .selection) || true)
  [ -z "$extra" ] || { echo "stale: $extra"; return 1; }
}

served() { [ "$status" = 0 ] && [ "$(tail -n1 "$work/out")" = "query-server $1/index 0.0.0.0:3000" ]; }
fetched() { grep -q '/latest.json$' "$CURL_LOG"; }
# snapshot <data dir>: each file of <data dir>/index (the marker and accounts
# among them) and its md5sum.
snapshot() { (cd "$1/index" && find . -type f -exec md5sum {} + | sort -k2); }
# unstaged <data dir>: whether <data dir> holds nothing but the index, no
# download left beside it.
unstaged() { [ -z "$(ls -A "$1" | grep -vx index)" ]; }

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

# finished <data dir> [VAR=value...]: a finished download of the selection.
finished() {
  start "$@"
  [ "$status" = 0 ] || { echo "setup download failed"; cat "$work/out"; exit 1; }
}

export BUCKET="$bucket"

# A fresh volume downloads the selection and serves it.
d="$work/fresh"
start "$d"
check "a fresh volume downloads the selection" holds "$d"
check "a fresh volume serves it" served "$d"
check "a unit is fetched once for all its files" test "$(grep -c '/planet/full/places\.tt\.zst$' "$CURL_LOG")" = 1
check "a unit's files are fetched only through it" eval '! grep -q "/place_.*\.bin" "$CURL_LOG"'
check "each unit is decoded into the index" test "$(wc -l < "$TRANSPORT_LOG")" = "$(list | while read -r dir f; do unit_of "$f"; done | sort -u | grep -c .)"
check "a fresh volume's download leaves nothing beside the index" unstaged "$d"

# A finished download of the selection is served as it is.
d="$work/finished"
finished "$d"
start "$d"
check "a finished download is not fetched again" eval '! fetched'
check "a finished download is served" served "$d"

# A finished download of an older build whose selection has this one's very
# files, under the marker its server left (the selection alone, or naming
# that build's version): this server can't read those files, so the
# selection is downloaded afresh.
for marker in legacy versioned; do
  d="$work/older-build-same-files-$marker"
  finished "$d"
  list | while read -r dir f; do
    { echo "build $((version - 1)) $dir/$f"; head -c 2048 /dev/zero; } > "$d/index/$f"
  done
  if [ "$marker" = legacy ]; then list; else echo "build_version $((version - 1))"; list; fi > "$d/index/.selection"
  start "$d"
  check "an older build's dataset of the same files ($marker marker) is downloaded afresh" holds "$d"
  check "an older build's dataset of the same files ($marker marker): the server starts" served "$d"
done

# A dataset from before the arc geometry (admin_vertices.bin, no rings or arcs)
# is replaced by the whole selection, not completed: its cells and strings are
# another build's. The accounts stay.
d="$work/legacy"
mkdir -p "$d/index"
list | cut -d' ' -f2 | grep -vxE 'admin_rings.bin|arc_index.bin|arc_vertices.bin' | while read -r f; do
  { echo "old $f"; head -c 2048 /dev/zero; } > "$d/index/$f"
done
echo "old admin_vertices.bin" > "$d/index/admin_vertices.bin"
echo '{"users":{"kept":{}}}' > "$d/index/geocoder.json"
start "$d"
check "a legacy dataset is downloaded afresh" holds "$d"
check "a legacy dataset's accounts stay" grep -q kept "$d/index/geocoder.json"
check "a legacy dataset: the server starts" served "$d"

# So is the legacy dataset of the minimal server.
d="$work/legacy-minimal"
mkdir -p "$d/index"
for f in admin_cells.bin admin_entries.bin admin_polygons.bin admin_vertices.bin place_nodes.bin strings_core.bin strings_layout.json; do
  { echo "old $f"; head -c 2048 /dev/zero; } > "$d/index/$f"
done
start "$d" MINIMAL=1
check "a legacy minimal dataset is downloaded afresh" holds "$d" MINIMAL=1
check "a legacy minimal dataset: the server starts" served "$d"

# A download that stopped before it finished left no marker: every file is
# fetched again, the truncated ones among them.
d="$work/interrupted"
finished "$d"
rm -f "$d/index/.selection"
truncate -s 10 "$d/index/geo_cells.bin" "$d/index/strings_core.bin"
start "$d"
check "an interrupted download is downloaded afresh" holds "$d"
check "an interrupted download: the server starts" served "$d"

# A dataset of another selection (here the other boundary tier, whose files
# have the same names) is replaced by this one's.
d="$work/other-selection"
finished "$d"
start "$d" QUALITY=exact
check "another selection's dataset is downloaded afresh" holds "$d" QUALITY=exact
check "another selection's dataset: the server starts" served "$d"

# A finished download that has since lost a file it needs is fetched again.
d="$work/lost-file"
finished "$d"
rm "$d/index/arc_index.bin"
start "$d"
check "a dataset missing a file is downloaded afresh" holds "$d"

# A file the build may lack is no error, and the dataset stays.
d="$work/optional"
BUCKET=$(bucket_without full/interp_postcodes.bin)
start "$d"
check "a missing optional file still serves" served "$d"
check "a missing optional file leaves the rest of the selection" holds "$d"
start "$d"
check "a dataset without an optional file is not fetched again" eval '! fetched'
BUCKET=$bucket

# A file the server needs that the build lacks fails the download, naming it,
# and the next start tries again.
d="$work/required"
BUCKET=$(bucket_without quality/simplified/arcs.tt.zst)
start "$d"
check "a missing required file fails the start" test "$status" != 0
check "a missing required file is named" grep -qx 'quality/simplified/arc_vertices.bin' "$work/out"
check "a failed download leaves no marker" test ! -e "$d/index/.selection"
BUCKET=$bucket
start "$d"
check "a failed download is downloaded afresh next time" holds "$d"

# A fetch that breaks off partway fails the start, of an optional file the
# build has (its miss would be taken for the build lacking it) as of a
# required unit or file (its half would pass for the file). None of their
# files is left, nor a unit's download, no marker vouches for the download,
# and the next start downloads afresh.
d="$work/broken-off"
start "$d" FLAKY="$(printf '%s\n' full/postcode_centroids.bin quality/simplified/arcs.tt.zst full/geo_cells.bin)"
check "a fetch that breaks off fails the start" test "$status" != 0
check "a broken-off optional file is named" grep -q 'failed: .*/planet/full/postcode_centroids.bin$' "$work/out"
check "a broken-off required unit is named" grep -q 'failed: .*/planet/quality/simplified/arcs.tt.zst$' "$work/out"
check "a broken-off required file is named" grep -q 'failed: .*/planet/full/geo_cells.bin$' "$work/out"
check "a broken-off fetch leaves none of its files" eval '! ls -A "$d/index" | grep -E "^(postcode_centroids|arc_index|arc_vertices|geo_cells)\.bin|\.tt\.zst$"'
check "a broken-off download leaves no marker" test ! -e "$d/index/.selection"
check "a broken-off download leaves nothing beside the index" unstaged "$d"
start "$d"
check "a broken-off download is downloaded afresh next time" holds "$d"
check "a broken-off download: the server starts" served "$d"

# A download that fails leaves the dataset it was to replace as it was, its
# marker too: the selection comes beside it and replaces it only once whole.
d="$work/kept"
finished "$d"
before=$(snapshot "$d")
BUCKET=$(bucket_without quality/exact/arcs.tt.zst)
start "$d" QUALITY=exact
check "a failed download of another selection fails the start" test "$status" != 0
check "a failed download leaves the dataset it was to replace" test "$(snapshot "$d")" = "$before"
check "a failed download leaves nothing beside the index" unstaged "$d"
BUCKET=$bucket
start "$d" QUALITY=exact
check "the next start replaces the dataset" holds "$d" QUALITY=exact

# A cache of an older build (build 23's selection, with its
# postcode_centroid_entries.bin) while latest.json still names a build of
# that version: the start says so and fails, and the cache stays as it was,
# its marker too, for a server of that version to take or this one once a
# build it reads is out.
d="$work/older-build"
mkdir -p "$d/index"
{ list; echo "full postcode_centroid_entries.bin"; } | while read -r dir f; do
  { echo "build 23 $dir/$f"; head -c 2048 /dev/zero; } > "$d/index/$f"
done
list | sed '/^full postcode_centroid_cells.bin$/a full postcode_centroid_entries.bin' > "$d/index/.selection"
echo '{"users":{"kept":{}}}' > "$d/index/geocoder.json"
before=$(snapshot "$d")
BUCKET=$(bucket_of_version 23)
start "$d"
check "a build of another version fails the start" test "$status" != 0
check "a build of another version is named with this server's" grep -q "build_version 23; this server reads build_version $version" "$work/out"
check "a build of another version leaves the older cache as it was" test "$(snapshot "$d")" = "$before"
check "a build of another version: nothing is fetched but latest.json" test "$(grep -vc '/latest.json$' "$CURL_LOG")" = 0
BUCKET=$bucket
start "$d"
check "a build of this version replaces the older cache" holds "$d"
check "a build of this version keeps the accounts" grep -q kept "$d/index/geocoder.json"

# On a fresh volume too, and a latest.json without a build version.
d="$work/older-build-fresh"
BUCKET=$(bucket_of_version 23)
start "$d"
check "a fresh volume refuses a build of another version" test "$status" != 0
check "a fresh volume fetches nothing of it" test "$(grep -vc '/latest.json$' "$CURL_LOG")" = 0
check "a fresh volume is left without a dataset" test ! -e "$d/index/.selection"
BUCKET=$(bucket_of_version 23)
printf '{\n  "latest": "%s"\n}\n' "$build" > "$BUCKET/latest.json"
start "$d"
check "a latest.json without a build version fails the start" test "$status" != 0
check "a latest.json without a build version is named" grep -q "build_version none; this server reads build_version $version" "$work/out"
BUCKET=$bucket

if [ "$failures" -gt 0 ]; then
  echo "$failures failure(s)"
  exit 1
fi
echo "all passed"
