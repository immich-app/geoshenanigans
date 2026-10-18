"""Bytes per patch section (copy of /ssd/review/scratch/section_sizes.py):
slices the decompressed patch at the SEC offsets the instrumented diff logged
and zstd -19's each slice alone. Prints every section, a per-kind rollup, and
the whole-stream compressed size for reference.

Usage: section_sizes2.py DIFF_SEC_LOG PATCH.gcpatch"""
import collections
import subprocess
import sys

ZSTD = "/ssd/shrink/zstd157/bin/zstd"
log, patch = sys.argv[1], sys.argv[2]
secs = []
for line in open(log):
    if line.startswith("SEC "):
        _, label, off = line.split()
        secs.append((int(off), label))
raw = subprocess.run([ZSTD, "-dc", patch], capture_output=True, check=True).stdout
secs = [(0, "header")] + secs + [(len(raw), "eof")]
comp = collections.Counter()
rawsz = collections.Counter()
order = []
for (off, label), (nxt, _) in zip(secs, secs[1:]):
    if nxt <= off:
        continue
    c = subprocess.run([ZSTD, "-19", "-T4", "-c"], input=raw[off:nxt], capture_output=True, check=True).stdout
    if label not in comp:
        order.append(label)
    comp[label] += len(c)
    rawsz[label] += nxt - off
total = sum(comp.values())
whole = len(open(patch, "rb").read())
print(f"{patch}: raw {len(raw)} B, whole-stream zstd {whole} B, sections zstd'd alone sum {total} B")
print(f"  {'zstd B':>10} {'%':>6} {'raw B':>11}  section")
for label in sorted(order, key=lambda l: -comp[l]):
    print(f"  {comp[label]:>10} {100 * comp[label] / total:5.1f}% {rawsz[label]:>11}  {label}")
kind = collections.Counter()
kraw = collections.Counter()
for label in order:
    k = label.split(":")[0]
    kind[k] += comp[label]
    kraw[k] += rawsz[label]
print("  by kind:")
for k, s in kind.most_common():
    print(f"  {s:>10} {100 * s / total:5.1f}% {kraw[k]:>11}  {k}")
