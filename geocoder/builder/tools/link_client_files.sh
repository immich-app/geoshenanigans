#!/usr/bin/env bash
# Usage: link_client_files.sh <variant-dir> <isolated-dir>
#
# Fills <isolated-dir> (created, must not exist) with symlinks to the variant's
# client files: everything a client holds, the same set verify_patch.sh
# compares (no *.osm_ids, *.gcpatch, *.zst or dotfiles). Applying a patch to
# it proves the variant patches from its own files alone: nothing beside it
# (../full, a sibling quality dir) exists.
set -euo pipefail

src=$(readlink -f "$1")
dst=$2
mkdir "$dst"
find "$src" -maxdepth 1 -type f ! -name '.*' ! -name '*.osm_ids' ! -name '*.gcpatch' ! -name '*.zst' -printf '%f\n' \
  | while IFS= read -r name; do ln -s "$src/$name" "$dst/$name"; done
