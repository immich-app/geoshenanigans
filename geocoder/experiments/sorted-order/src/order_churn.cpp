// order_churn PREV.slot.u32 CUR.slot.u32 [NAME]
//
// How the canonical order of one family changes from one sorted day to the
// next. Each file lists, per record in sorted order, the record's published
// strategy-2 slot; a slot live on both days holds the same OSM entity (see
// identity.py), so slots match records across the two days.
//   kept     : records on both days
//   deleted / inserted
//   moved    : kept records out of relative order (kept - longest increasing
//              run of their new positions taken in old order)
//   runs     : maximal runs of kept records that stay adjacent and in order
//              (old k, k+1 -> new j, j+1): the copy runs a positional merge
//              of the two orders needs
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

static std::vector<uint32_t> load(const char* path) {
    FILE* f = fopen(path, "rb");
    if (!f) { perror(path); exit(1); }
    fseek(f, 0, SEEK_END);
    long n = ftell(f) / 4;
    fseek(f, 0, SEEK_SET);
    std::vector<uint32_t> v(n);
    if (fread(v.data(), 4, n, f) != size_t(n)) { perror(path); exit(1); }
    fclose(f);
    return v;
}

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "usage: order_churn PREV.slot.u32 CUR.slot.u32 [NAME]\n"); return 1; }
    const std::vector<uint32_t> P = load(argv[1]), C = load(argv[2]);
    const char* name = argc > 3 ? argv[3] : "";
    uint32_t max_slot = 0;
    for (uint32_t s : P) max_slot = std::max(max_slot, s);
    for (uint32_t s : C) max_slot = std::max(max_slot, s);
    const uint32_t NO = 0xFFFFFFFFu;
    std::vector<uint32_t> posC(size_t(max_slot) + 1, NO);
    for (size_t j = 0; j < C.size(); j++) posC[C[j]] = uint32_t(j);
    std::vector<uint32_t> seq;
    seq.reserve(P.size());
    uint64_t runs = 0;
    uint32_t prev_new = NO;
    bool prev_kept = false;
    for (size_t k = 0; k < P.size(); k++) {
        const uint32_t j = posC[P[k]];
        if (j == NO) { prev_kept = false; continue; }
        if (!(prev_kept && prev_new != NO && j == prev_new + 1)) runs++;
        seq.push_back(j);
        prev_new = j;
        prev_kept = true;
    }
    std::vector<uint32_t> tails;
    for (uint32_t j : seq) {
        auto it = std::lower_bound(tails.begin(), tails.end(), j);
        if (it == tails.end()) tails.push_back(j); else *it = j;
    }
    const size_t kept = seq.size();
    printf("%-7s prev %zu cur %zu kept %zu deleted %zu inserted %zu moved %zu runs %llu\n", name, P.size(), C.size(),
           kept, P.size() - kept, C.size() - kept, kept - tails.size(), (unsigned long long)runs);
    return 0;
}
