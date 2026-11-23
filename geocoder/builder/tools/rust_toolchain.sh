#!/usr/bin/env bash
# Usage: rust_toolchain.sh [<status file>]
#
# The stable Rust toolchain for the nightly's place-search build, installed as
# dtolnay/rust-toolchain installs it (rustup first if the runner lacks it,
# then a minimal stable toolchain, retried on a release server mid-update,
# copied where a rename fails: RUSTUP_PERMIT_COPY_RENAME, for a runner image
# with a toolchain in a lower overlay layer, which no rename crosses), but
# inside the detached job that builds place-search: a fresh runner pod spends
# ~15 s downloading it, which no step need wait for. Rustup goes in
# $CARGO_HOME/bin (default ~/.cargo/bin), which the caller puts on PATH.
# The exit status goes to <status file> too, written last: the nightly's
# Check the Rust toolchain reads it.
set -euo pipefail

if [ $# -gt 0 ]; then
  status_file=$1
  trap 'echo $? > "$status_file"' EXIT
fi

if ! command -v rustup > /dev/null; then
  curl --proto '=https' --tlsv1.2 --retry 10 --retry-connrefused --location --silent --show-error --fail \
    https://sh.rustup.rs | sh -s -- --default-toolchain none --profile minimal --no-modify-path -y
fi
for attempt in 1 2 3 4 5; do
  if RUSTUP_PERMIT_COPY_RENAME=1 rustup toolchain install stable --profile minimal --no-self-update; then break; fi
  if [ "$attempt" = 5 ]; then exit 1; fi
  echo "Retrying rustup toolchain install in 30 seconds (retry $attempt of 4)" >&2
  sleep 30
done
rustup default stable
