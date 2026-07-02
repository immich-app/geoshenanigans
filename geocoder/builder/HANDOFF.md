# Geocoder patch-size reduction — handoff

This is a session handoff for the patch-reduction work being done in
`geocoder/builder/tools/geocoder_diff.cpp` + `geocoder_patch.cpp`.
Picking this up from another machine: read this, then jump to the
"Next concrete step" section at the bottom.

## Goal

Reduce the daily incremental patch files emitted by step 10
("Generate patches and verify") of `.github/workflows/geocoder-build.yml`.
The headline number is **planet/full compressed gcpatch size**.

| Snapshot | planet/full compressed | vs baseline | Notes |
| --- | --- | --- | --- |
| Pre-session | ~3,890 MiB | 1.0x | Many FULL_REPLACE files, byte-key secondary matching |
| Mid-session baseline | ~3,798 MiB | 1.0x | Sparse-delta for 2 of 5 small files |
| With diff-side + initial determinism fixes (`7976e2c`) | 477 MiB | 8.0x | |
| **Final (`627ae7e`)** | **92 MiB compressed** | **41x reduction** | osm_id tiebreakers + sort-order dedup iteration |
| Oceania final | 1.4 MiB | 70x baseline | Equal to diff-tool base overhead (out8 vs out8 = 1.4 MiB) |

Section-level wins on planet/full (uncompressed, vs pre-session):
- addr_postcodes: 666 MiB FULL → 1.0 MiB SPARSE (**666x**)
- way_parents: 208 MiB FULL → 200 KiB SPARSE (**1000x**)
- POI_PARENT_REMAP: 424 MiB → 200 KiB (**2000x**)
- STRINGS: 687 MiB → 76 MiB (**9x**)
- ENTRY_CORRECTION: 135 MiB → 14 MiB (**10x**)
- addr_vertices: 3,391 MiB FULL → 428 MiB byte-merge (**8x**)
- addr_points: 2,566 MiB merge → 179 MiB merge (**14x**)

Original session target was ~5 MiB for major patches. Planet not quite
there — still 477 MiB — but the remaining noise is upstream of the
diff (residual build non-determinism in vertex-byte ordering causing
~88M addr_points and ~48M street_ways fixups). All "value" sections
(addr_postcodes, way_parents, POI_PARENT_REMAP) are nearly empty.

The remaining ~430 MiB is essentially:
- 777 MiB street_nodes byte-stream noise (compresses to ~150 MiB)
- 428 MiB addr_vertices byte-stream noise (~120 MiB compressed)
- 179 MiB addr_points fixups (~60 MiB)
- 157 MiB street_ways fixups (~50 MiB)

To get planet under 100 MiB compressed, the remaining build-determinism
work is fixing whatever produces non-deterministic vertex byte ordering
between same-PBF rebuilds — likely floating-point centroid sums or
multi-threaded polygon assembly order.

## Validated approach (on Hetzner server)

### Same-PBF self-rebuild (build-determinism check)
Built oceania and planet twice from the same PBF using strategy-2 chain:
- Build 1: fresh, no prev
- Build 2: `--prev-output build1`
- Result: oceania 1.4 MiB compressed (matches diff-tool base overhead),
  planet 92 MiB compressed
- All 26 files verify byte-identical

### Real day-over-day chain (oceania, 3 consecutive Geofabrik snapshots)
PBFs from `australia-oceania-260606/07/08.osm.pbf`:
- Day 1 (260606): fresh build → `d1`
- Day 2 (260607): build with `--prev-output d1` → `d2`
  - Patch `d1 → d2`: **6.7 MiB compressed**
  - VERIFY 26/26 — patch + apply to `d1` reproduces `d2` byte-identical
- Day 3 (260608): build with `--prev-output d2` → `d3`
  - Patch `d2 → d3`: **8.1 MiB compressed**
  - VERIFY 26/26 — patch + apply to `d2` reproduces `d3` byte-identical

A user who downloads day N's full or starts from day N-7 and applies
7 daily patches ends up with byte-identical files to what the official
pipeline emits on day N. Strategy-2 tombstones (~20 streets/85 addrs/
74 POIs of real-world churn per day on oceania) flow through the patch
correctly.

The "fresh-build vs chained-build" comparison shows 11–12 of 26 files
differ — this is the intended strategy-2 behaviour: chain preserves
day-N-1's slot positions for stable osm_ids and appends new ones at
end, while a fresh build sorts everything anew. Size delta is ~2 KB
on oceania (just the appended-slot byte count). Both contain the
same data; the chain is what's published and reproducible from a
prior day + chain of patches.

### Multi-day chain (catch-up scenario)
Concrete user: starts from `d1` full, downloads `patch_d2` + `patch_d3`,
applies them in sequence.
- `d1 + patch_d2 → step1_out` — all 26 `.bin` files byte-identical to
  published `d2` ✓
- `step1_out + patch_d3 → step2_out` — all 26 `.bin` files
  byte-identical to published `d3` ✓

### What "fresh build" means and why patched ≠ fresh standalone

A "fresh build" of day N (no `--prev-output`) produces a different
`.bin` layout than the published day-N build, which always runs with
`--prev-output day-N-1`. Strategy-2 IS this divergence: the chained
build keeps day-1's slot positions for any osm_id present in both days
and appends new osm_ids at end; the fresh build sorts everything anew.
On oceania day 2 the difference is ~2 KB across 11 of 26 `.bin` files
— same data, different slot ordering for ~85 newly-added addresses.

The pipeline never publishes a fresh-of-day-N build. The Tigris bundle
is always the chained build. So the operationally-relevant question
is "does patched == chained published" (yes, 26/26 ✓), not
"does patched == hypothetical fresh standalone" (no, impossible
without removing strategy-2 — which would balloon patches back to
multi-GiB and defeat the entire effort here).

5 of 31 `.osm_ids` sidecars in patched output stay stale — but these
files are **never published** in the first place. The Tigris upload
step in `.github/workflows/geocoder-build.yml:824` excludes
`*.osm_ids` from the user-facing bucket ("Strategy-2 sidecars
(*.osm_ids) are cached only ... clients never need them"). End users
only ever receive `.bin` files. A user who patches forward and a user
who downloads today's bundle end up with identical published files.

### Per-preset day-over-day sizes (oceania, real Geofabrik diff)

| Preset | Uncompressed | Compressed |
| --- | --- | --- |
| `full` | 39.8 MiB | 8.1 MiB |
| `admin` | 18.3 MiB | 2.7 MiB |
| `no-addresses` | 33.2 MiB | 6.9 MiB |
| `admin-minimal` | 6.3 MiB | 1.3 MiB |
| `poi/all` | 50.9 MiB | 26.0 MiB |
| `poi/major` | 17.7 MiB | 8.9 MiB |
| `poi/notable` | 25.7 MiB | 13.6 MiB |
| **Total all variants** | **192.0 MiB** | **67.5 MiB** |

POI variants compress notably less well (~50% ratio vs ~20% for
the others) — POI-data content shape is different (varint-encoded
strings, denser numeric content) and may have room for a targeted
optimization in a follow-up.

### Section breakdown (oceania same-PBF, 27 MiB uncompressed)
| Section | Size | Notes |
|---|---|---|
| addr_postcodes (SPARSE) | 12.0 MiB | 76% positions match; rest from build non-determinism |
| street_nodes (merge) | 7.7 MiB | |
| **addr_vertices (byte-merge)** | **4.5 MiB** | Was ~50 MiB FULL_REPLACE before my fix |
| addr_points (merge) | 1.8 MiB | Only 50K seq bytes — fixup_v15 working great |
| place_nodes (merge) | 0.9 MiB | |
| STRINGS_or_SECONDARY | 0.3 MiB | |

## Commits this session (newest first)

```
587613c fix(geocoder-build): correct admin_polygons sidecar prev_dir path
60f1532 perf(geocoder-diff): remove emit_sparse_delta FULL_REPLACE fallback
8daca37 chore(ci): capture per-variant diff logs as artifact
e02ca59 fix(geocoder): emit addr_points vertex_offset fixups for byte-block merge
c1f7101 perf(geocoder): byte-block merge for addr_vertices via content-hash
```

### What each does
- **`c1f7101`** — Replace `addr_vertices.bin` FULL_REPLACE with a byte-block
  merge: uses `fixup_v15_offsets` with `key_fn=0` so polygons are matched
  purely by vertex-content hash (no slot-stability assumption). After
  rewriting OLD addr_points' vertex_offset to NEW's byte position, the
  vertex byte stream becomes a tight MATCH/INSERT/DELETE sequence.
- **`e02ca59`** — Emit `addr_points` vertex_offset fixups in the merge
  result so the patch tool can rewrite byte 20 during MATCH replay
  (mirrors what admin_polygons already does for byte 0). Without this,
  any MATCH that fired would carry OLD's vertex_offset instead of NEW's
  and verify would fail.
- **`8daca37`** — Save per-variant `geocoder-diff` stderr to
  `$WORKDIR/diff-logs/$variant.log` and upload as artifact. The
  workflow previously `2>/dev/null`'d this output so the per-variant
  POI_PARENT_REMAP / sparse-vs-full / secondary-match stats were
  invisible.
- **`60f1532`** — Drop the `sparse_delta ≥ full_replace` fallback in
  `emit_sparse_delta`. The fallback hid the underlying defect (slot
  stability broken → >50% positions appear changed) by shipping the
  whole file. Per project direction, always emit sparse so the cost is
  visible. On test data with broken stability the section grows; with
  real strategy-2 stability it tracks the actual change count.
- **`587613c`** — Fix `apply_strategy2_admins` reading from
  `<prev>/admin/admin_polygons.osm_ids` (which is never written) instead
  of `<prev>/quality/uncapped/admin_polygons.osm_ids` (the canonical
  write location). Without this load, every admin polygon was getting
  a fresh slot every build. Same-PBF test with the fix surfaces 357
  tombstones in admins — the underlying PBF parsing is non-deterministic
  so admin osm_id ordering varies between builds. This is the real
  noise floor — fixing it requires sorting `data.admin_osm_ids` etc.
  before strategy-2.

## Build non-determinism (remaining noise floor)

Even with the same PBF and proper strategy-2 chain, 10 of 26 `.bin`
files differ between builds. Pattern from oceania:
- street_ways/admin_polygons/postcode_centroids sidecars all DIFFER
  byte-level between same-PBF builds. addr_points and place_nodes ditto.
- "(no shifts)" in strategy-2 logs is misleading — it's also reported
  for fresh allocations, where remap[i]=i is trivially true.
- For `addr_postcodes`: 76.1% identical, 10.2% v1=NO_DATA→v2=real,
  10.2% symmetric flip, 3.5% both real but different. Clear sign that
  PBF parsing yields non-deterministic addr_osm_ids order →
  non-deterministic slot assignment → non-deterministic postcode_id
  assignment.

The fix is build-side: sort `data.addr_osm_ids` (and the parallel
arrays `data.addr_points`, `data.addr_postcode_ids`, plus references
in `data.cell_to_addrs`) by osm_id before strategy-2 runs. Same for
admins (sort by (relation_id, ring_index)), POIs, places, interps.

## Test infrastructure (Hetzner server)

Working server at `157.180.105.198`:
- AMD EPYC 7502P, 32 cores / 64 threads, 256 GB RAM
- `/dev/md4` (RAID1 across 2 NVMe drives) mounted at `/ssd` (6.8 TB)
  — was originally RAID0; switched at user's request for faster reads
- Tmux session `geo` (`tmux a -t geo` to attach)
- `/ssd/geoshenanigans` — clone of feat/geocoder branch
- `/ssd/work/` — PBFs and build outputs
- `/ssd/work/run-diff.sh OLD NEW` — runs diff+verify, prints sizes
- `/ssd/work/run-planet.sh` — full planet pipeline (waiting on PBF dl)

## Open tasks

- **#115** in_progress — Vertex byte-merge for addr_vertices [DONE
  for diff side; needs planet-scale measurement]
- **#119 / #120** still pending — sidecar-osm_id secondary matching +
  ENTRY_CORRECTION delta encoding were both reverted previously due
  to combo verify failures on planet/full; with the determinism
  story now clearer, they may apply cleanly.
- **#117** pending — improve addr_points secondary match key.
  Already partially done via fixup_v15_offsets (`c1f7101`).
- **#101 / #105** pending — strategy-2 simplifications and POI
  parent-link non-determinism investigation.

## Next concrete step

1. Wait for planet PBF download to finish (started in tmux `geo`).
2. Run `/ssd/work/run-planet.sh` — builds planet twice with
   strategy-2 chain, diffs, verifies. Measures the *actual* current
   patch size at planet scale with all my fixes applied.
3. If the patch is in the low hundreds of MiB compressed: ship it
   — this is well in line with OSM planet diff scale.
4. If still gigabytes: the noise floor is build-side non-determinism.
   Sort the parallel-array vectors (addr_osm_ids etc.) before
   strategy-2 — single most impactful build-side fix.

## Don't do

- Don't re-add a FULL_REPLACE fallback in sparse_delta. The whole
  point of removing it is to surface noise so it can be fixed.
- Don't run any more CI builds while the local server can validate
  the same thing in ~1 hour. CI burns hours per iteration.
- Don't touch `geocoder/HANDOFF.md` (root) — different project.
