// StringDict / intern_dicts: dictionary codes are stable per distinct string,
// and intern_dicts leaves the pool exactly as interning every value.
#include "string_dict.h"

#include <algorithm>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "test_framework.h"

TEST(string_dict_codes_follow_first_add) {
    StringDict dict;
    CHECK_EQ(dict.add("Main Street"), 0u);
    CHECK_EQ(dict.add("Main"), 1u);
    CHECK_EQ(dict.add(""), 2u);
    CHECK_EQ(dict.add("Main Street"), 0u);
    CHECK_EQ(dict.add("Main Street Extra", 4), 1u);
    CHECK_EQ(dict.size(), 3u);
    CHECK_EQ(std::string(dict.get(0)), "Main Street");
    CHECK_EQ(std::string(dict.get(1)), "Main");
    CHECK_EQ(std::string(dict.get(2)), "");
}

TEST(string_dict_matches_a_reference_map_across_growth) {
    StringDict dict;
    std::unordered_map<std::string, uint32_t> ref;
    std::vector<const char*> first_ptr;
    std::mt19937 rng(3);
    bool codes_ok = true, ptrs_ok = true;
    for (int i = 0; i < 300000; i++) {
        std::string s = "street " + std::to_string(rng() % 60000) + std::string(rng() % 30, 'x');
        uint32_t code = dict.add(s.c_str());
        auto [it, inserted] = ref.try_emplace(s, static_cast<uint32_t>(ref.size()));
        codes_ok = codes_ok && code == it->second;
        if (inserted) first_ptr.push_back(dict.get(code));
        ptrs_ok = ptrs_ok && dict.get(code) == first_ptr[code] && s == dict.get(code);
    }
    CHECK(codes_ok);
    CHECK(ptrs_ok);
    CHECK_EQ(dict.size(), ref.size());
}

TEST(string_dict_stores_oversized_strings) {
    StringDict dict;
    std::string big(3u << 20, 'y');
    uint32_t a = dict.add("a");
    uint32_t b = dict.add(big.c_str());
    CHECK(dict.get(b) == big);
    CHECK_EQ(std::string(dict.get(a)), "a");
    CHECK_EQ(dict.add(big.c_str()), b);
}

namespace {

// A merge's input: per thread a dict and the codes of its runs, plus the
// order the merge reads the runs in.
struct MergeInput {
    std::vector<StringDict> dicts;
    std::vector<std::vector<uint32_t>> run_codes;
    std::vector<uint32_t> run_dict;
    std::vector<std::vector<const char*>> run_values;  // nullptr: kNone
};

// Interning every value in run order, as a serial merge would.
std::vector<std::vector<uint32_t>> intern_each_value(const MergeInput& in, StringPool& pool) {
    std::vector<std::vector<uint32_t>> ids;
    for (const auto& values : in.run_values) {
        ids.emplace_back();
        for (const char* v : values) ids.back().push_back(v ? pool.intern(v) : NO_DATA);
    }
    return ids;
}

std::vector<std::vector<uint32_t>> intern_bulk(const MergeInput& in, StringPool& pool, unsigned threads) {
    std::vector<const StringDict*> dicts;
    for (const auto& d : in.dicts) dicts.push_back(&d);
    auto dict_ids = intern_dicts(dicts, in.run_dict, [&](uint32_t r, auto&& emit) {
        for (uint32_t code : in.run_codes[r]) emit(code);
    }, pool, threads);
    std::vector<std::vector<uint32_t>> ids;
    for (size_t r = 0; r < in.run_codes.size(); r++) {
        ids.emplace_back();
        for (uint32_t code : in.run_codes[r]) ids.back().push_back(dict_ids[in.run_dict[r]](code));
    }
    return ids;
}

MergeInput new_merge_input(size_t n_dicts, size_t runs_per_dict, size_t values_per_run, uint32_t seed,
                           std::vector<std::string>& storage) {
    std::mt19937 rng(seed);
    storage.clear();
    for (int i = 0; i < 5000; i++)
        storage.push_back((rng() % 3 ? "street " : "") + std::to_string(rng() % 4000) + std::string(rng() % 20, 'z'));
    MergeInput in;
    in.dicts.resize(n_dicts);
    for (size_t s = 0; s < runs_per_dict; s++)
        for (size_t d = 0; d < n_dicts; d++) in.run_dict.push_back(static_cast<uint32_t>(d));
    std::shuffle(in.run_dict.begin(), in.run_dict.end(), rng);
    in.run_values.resize(in.run_dict.size());
    for (auto& values : in.run_values) {
        size_t n = rng() % (2 * values_per_run + 1);
        for (size_t i = 0; i < n; i++)
            values.push_back(rng() % 10 == 0 ? nullptr : storage[rng() % storage.size()].c_str());
    }
    // Codes come from parse order, which need not be merge order.
    std::vector<size_t> parse(in.run_values.size());
    for (size_t r = 0; r < parse.size(); r++) parse[r] = r;
    std::shuffle(parse.begin(), parse.end(), rng);
    for (size_t r : parse)
        for (const char* v : in.run_values[r])
            if (v) in.dicts[in.run_dict[r]].add(v);
    for (size_t r = 0; r < in.run_values.size(); r++) {
        in.run_codes.emplace_back();
        for (const char* v : in.run_values[r])
            in.run_codes.back().push_back(v ? in.dicts[in.run_dict[r]].add(v) : StringDict::kNone);
    }
    return in;
}

}  // namespace

TEST(intern_dicts_interns_like_every_value_in_merge_order) {
    std::vector<std::string> storage;
    for (uint32_t seed : {1u, 2u, 3u}) {
        MergeInput in = new_merge_input(7, 4, 3000, seed, storage);
        StringPool direct;
        direct.intern(storage[0]);  // already pooled before the merge
        direct.intern("unrelated");
        auto want = intern_each_value(in, direct);
        for (unsigned threads : {1u, 3u, 8u}) {
            StringPool bulk;
            bulk.intern(storage[0]);
            bulk.intern("unrelated");
            auto got = intern_bulk(in, bulk, threads);
            CHECK(got == want);
            CHECK(bulk.data() == direct.data());
            CHECK_EQ(bulk.intern("unrelated"), direct.intern("unrelated"));
        }
    }
}

TEST(intern_dicts_skips_codes_the_merge_never_reads) {
    StringDict dict;
    uint32_t elm = dict.add("Elm St");
    dict.add("never read");
    uint32_t twelve = dict.add("12");
    StringPool pool;
    std::vector<const StringDict*> dicts = {&dict};
    std::vector<uint32_t> codes = {twelve, StringDict::kNone, elm, twelve};
    auto ids = intern_dicts(dicts, {0}, [&](uint32_t, auto&& emit) {
        for (uint32_t c : codes) emit(c);
    }, pool);
    CHECK_EQ(ids[0](twelve), 0u);
    CHECK_EQ(ids[0](elm), 3u);
    CHECK_EQ(ids[0](StringDict::kNone), NO_DATA);
    CHECK_EQ(std::string(pool.data().data(), pool.data().size()), std::string("12\0Elm St\0", 10));
}
