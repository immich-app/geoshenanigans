#!/bin/bash
# Decode each variant, stack lossless encodings on it, zstd -18 everything.
set -euo pipefail
cd "$(dirname "$0")"
for v in floor_75 floor_150 scale_2; do
  [ -f "dec_$v.npz" ] || /ssd/shrink/venv/bin/python decode.py "var/$v" "dec_$v.npz" &
done
wait
/ssd/shrink/venv/bin/python encode_variant.py decoded.npz orig 1e-6 &
/ssd/shrink/venv/bin/python encode_variant.py decoded.npz orig_g5 1e-5 &
/ssd/shrink/venv/bin/python encode_variant.py decoded.npz orig_g4 1e-4 &
/ssd/shrink/venv/bin/python encode_variant.py dec_floor_75.npz floor_75 1e-5 &
/ssd/shrink/venv/bin/python encode_variant.py dec_floor_75.npz floor_75_g4 1e-4 &
/ssd/shrink/venv/bin/python encode_variant.py dec_floor_150.npz floor_150 1e-5 &
/ssd/shrink/venv/bin/python encode_variant.py dec_scale_2.npz scale_2 1e-5 &
wait
LEVELS=19 bash ./compress.sh stack
