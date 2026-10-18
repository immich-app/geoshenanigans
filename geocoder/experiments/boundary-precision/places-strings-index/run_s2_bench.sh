#!/usr/bin/env bash
# Rebuild-at-load timing: compile cover_bench.cpp / place_cell_bench.cpp against Debian
# bookworm libs2-dev 0.10.0 (the builder pins S2 0.10.0 too, builder/tools/build_s2.sh:87)
# inside rust:bookworm, then run single-threaded on the admin-minimal planet set.
set -euo pipefail
T=/ssd/shrink/places-strings-index
mkdir -p $T/work/rebuilt_admin_fb
docker run --rm --cpus=1 -v /ssd/shrink:/ssd/shrink -e TMPDIR=/ssd/shrink/tmp -w $T rust:bookworm bash -c "
  apt-get update -qq >/dev/null 2>&1 && apt-get install -y -qq libs2-dev libabsl-dev time >/dev/null 2>&1
  g++ -O2 -std=c++17 cover_bench.cpp -o work/cover_bench -ls2 \$(ls /usr/lib/x86_64-linux-gnu/libabsl_*.so | sed -e s,.*/lib,-l, -e s,.so\$,,) -lpthread
  g++ -O2 -std=c++17 place_cell_bench.cpp -o work/place_cell_bench -ls2 -lpthread
  echo == cover_bench admin-minimal, 1 CPU
  RECT_FALLBACK=1 /usr/bin/time -v work/cover_bench /ssd/shrink/idx/smallest 0 work/rebuilt_admin_fb 2> work/cover_time_fb.txt || true
  grep -E \"Elapsed|Maximum resident\" work/cover_time_fb.txt
  echo == place_cell_bench on V2 order
  work/place_cell_bench work/variants/V2/place_nodes.bin work/variants/V2/place_cells.bin || true
"
