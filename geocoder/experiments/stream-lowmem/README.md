Low-memory streaming client update: patch/ holds the patcher that applies a GCPATCH to an encoded installed set in process (its files go over geocoder/builder/tools), gc/ the encoder and decoder of those sets. The Python tests take the built binaries as arguments (see each file's docstring); the C++ tests take none.

The measurement harness of this experiment (the timed update runners, the memory sampler, the real-data smoke check and the benchmark queues) lives on the research/geocoder-experiments branch, under geocoder/experiments/stream-lowmem/scripts/.
