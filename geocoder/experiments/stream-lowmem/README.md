Low-memory streaming client update: patch/ holds the patcher that applies a GCPATCH to an encoded installed set in process (its files go over geocoder/builder/tools), gc/ the encoder and decoder of those sets. The Python tests take the built binaries as arguments (see each file's docstring). The C++ tests take no arguments, except test_admin and test_poi, which take an output directory: run each twice, once plain and once with GCS_BIG_BLOCK=64, into two directories, and compare the two with diff -r (the big block path must give the same files):

    ./test_admin out/admin && GCS_BIG_BLOCK=64 ./test_admin out/admin-big && diff -r out/admin out/admin-big
    ./test_poi out/poi && GCS_BIG_BLOCK=64 ./test_poi out/poi-big && diff -r out/poi out/poi-big

The measurement harness of this experiment (the timed update runners, the memory sampler, the real-data smoke check and the benchmark queues) lives on the research/geocoder-experiments branch, under geocoder/experiments/stream-lowmem/scripts/.
