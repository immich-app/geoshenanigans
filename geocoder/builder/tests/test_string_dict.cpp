// StringDict / DictIds: dictionary codes are stable per distinct string, and
// interning through DictIds leaves the pool exactly as interning every value.
#include "string_dict.h"

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

TEST(dict_ids_intern_like_every_value) {
    std::vector<const char*> values = {"Elm St", "12", "Elm St", "", "12", "Oak Ave", "Elm St",
                                       nullptr, "9", "Oak Ave", nullptr, "12"};
    StringPool direct, cached;
    for (StringPool* pool : {&direct, &cached}) {
        pool->intern("Oak Ave");  // already pooled before the merge
        pool->intern("x");
    }

    std::vector<uint32_t> want;
    for (const char* v : values) want.push_back(v ? direct.intern(v) : NO_DATA);

    StringDict dict;
    std::vector<uint32_t> codes;
    // Codes come from parse order, which need not be merge order.
    for (size_t i = values.size(); i-- > 0;)
        if (values[i]) dict.add(values[i]);
    for (const char* v : values) codes.push_back(v ? dict.add(v) : StringDict::kNone);
    DictIds ids(dict, cached);
    std::vector<uint32_t> got;
    for (uint32_t code : codes) got.push_back(ids(code));

    CHECK(got == want);
    CHECK(cached.data() == direct.data());
}
