#!/usr/bin/env bash
# Accuracy matrix: matrix.sh <tag> <set> [build args...]  -> idx/<tag>, results/<tag>
set -euo pipefail
R=/ssd/search-prod
TAG=$1
SET=$2
shift 2
cd $R
rm -rf idx/$TAG
mkdir -p tmp/build-$TAG logs
target/release/place-search build sets/$SET idx/$TAG --threads 8 --mem-mb 256 --tmp tmp/build-$TAG "$@" > logs/build-$TAG.log 2>&1
QARGS=()
for ((i = 1; i <= $#; i++)); do
  if [ "${!i}" = "--extras" ]; then j=$((i + 1)); QARGS=(--extras "${!j}"); fi
done
scripts/run_eval.sh $SET idx/$TAG results/$TAG "${QARGS[@]}" | tail -18
