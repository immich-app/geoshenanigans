Low-memory streaming client update. The patcher that applies a GCPATCH to an encoded installed set in process is geocoder-patch's low-memory mode (geocoder/builder/tools/geocoder_patch.cpp and gcs_*.h, tested in geocoder/builder/tests). gc/ holds the encoder and decoder of those sets, and patch/ the end-to-end tests of the low-memory mode, which need gc.

The Python tests take the built binaries as arguments (see each file's docstring). gc's C++ tests take no arguments; test_addr_big also checks the patcher's address codec against gc's files, so it builds with geocoder/builder/tools on the include path:

    g++ -std=c++17 -O2 -ffp-contract=off -I ../../builder/tools -o test_addr_big gc/test_addr_big.cpp

The measurement harness of this experiment (the timed update runners, the memory sampler, the real-data smoke check and the benchmark queues) lives on the research/geocoder-experiments branch, under geocoder/experiments/stream-lowmem/scripts/.
