#!/bin/bash
# mkdir_variant.sh <dest> <dir with replacement files> : hardlink idx/max into dest, then link replacements over (ln -f)
set -e
D=$1; R=$2
mkdir -p "$D"
for f in /ssd/shrink/idx/max/*; do ln -f "$f" "$D/"; done
for f in "$R"/*.bin; do b=$(basename "$f"); if [ -e "/ssd/shrink/idx/max/$b" ]; then ln -f "$f" "$D/$b"; fi; done
ls -la "$D"
