#!/bin/bash
# enc_day.sh SRC CFG BLOCK : forward transform of every file of a flat dir, one after another,
# output discarded (measures the server-side transform cost alone; zstd is timed separately).
set -eu
src=$1 cfg=$2 block=$3
/ssd/shrink/venv/bin/python -I - "$src" "$cfg" "$block" <<'EOF'
import os, subprocess, sys
sys.path.insert(0, "/ssd/transport/scripts")
import specs
src, cfg, block = sys.argv[1], specs.load_cfg(sys.argv[2]), sys.argv[3]
for n in sorted(os.listdir(src)):
    if not (n.endswith(".bin") or n.endswith(".json")):
        continue
    spec = cfg.get(n, "")
    kind = specs.KIND[n] if spec else "r"
    with open(os.devnull, "wb") as null:
        subprocess.run([specs.TT, "enc", kind, spec or "-", os.path.join(src, n), "-", "--block", block]
                       + specs.side_arg(src, n), check=True, stdout=null)
EOF
