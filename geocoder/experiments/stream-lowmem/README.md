Measurement harness of the low-memory streaming update ([#428](https://github.com/immich-app/geoshenanigans/pull/428), stack/09-lowmem-streaming-update), preserved unchanged from that PR before removal. It runs on the box against the PR's patcher and gc (geocoder/experiments/stream-lowmem/{patch,gc} there):

- scripts/mrun.sh, run_update2.sh: one timed client update in a systemd scope of a CPU, memory and I/O profile, sampled by memsample.py; supdate.sh (the FIFO prototype), inproc.sh (the in-process patcher) and common.sh are the steps they run.
- scripts/smoke.sh: the untimed real-data check (one variant patched in process, cmp'd against the server's encoded set); perf_run.sh: a CPU profile; patch_sizes.py: a patch's client file list.
- scripts/queue1.sh to queue6.sh: the benchmark queue drivers of rounds 1 to 6 (run from /ssd/memory), calling mrun.sh, smoke.sh and perf_run.sh.
