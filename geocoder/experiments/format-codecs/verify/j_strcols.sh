#!/bin/bash
set -uo pipefail
V=/ssd/shrink/max/verify
Z=/ssd/shrink/zstd157/bin/zstd
export TMPDIR=/ssd/shrink/tmp
cd $V
zs() { for f in "$@"; do printf "%s\t%s\t%s\n" "$f" $(stat -L -c %s "$f") $($Z -18 -T16 -q -c "$f" | wc -c) >> $V/work/zst_T16.tsv; done; }
/ssd/shrink/venv/bin/python -u scripts/strcols.py work/strcols
zs work/strcols/hn_tierA.bin work/strcols/hn_tierA_purged.bin work/strcols/st_tierA.bin work/strcols/st_tierA_purged.bin
rm -rf work/strcols
echo DONE
