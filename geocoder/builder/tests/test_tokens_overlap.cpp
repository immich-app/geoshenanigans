// Unit tests for tokens_overlap / TokenMatcher (src/geometry.h): the word
// match the addr parent-street backfill runs, checked against the hashed
// version it replaced on names with accents, apostrophes, hyphens, tiny and
// repeated words.
#include "geometry.h"

#include <random>
#include <string>
#include <unordered_set>
#include <vector>

#include "test_framework.h"

namespace {

// tokens_overlap as it was: a hash set of B's words.
bool hashed_tokens_overlap(const char* a, const char* b) {
    if (!a || !b || !a[0] || !b[0]) return false;
    std::string na = normalise_for_matching(a);
    std::string nb = normalise_for_matching(b);
    if (na.empty() || nb.empty()) return false;
    std::unordered_set<std::string> b_tokens;
    size_t pos = 0;
    while (pos < nb.size()) {
        size_t next = nb.find(' ', pos);
        if (next == std::string::npos) next = nb.size();
        std::string tok = nb.substr(pos, next - pos);
        if (tok.size() >= 2) b_tokens.insert(std::move(tok));
        pos = next + 1;
    }
    pos = 0;
    while (pos < na.size()) {
        size_t next = na.find(' ', pos);
        if (next == std::string::npos) next = na.size();
        std::string tok = na.substr(pos, next - pos);
        if (tok.size() >= 2 && b_tokens.count(tok)) return true;
        pos = next + 1;
    }
    return false;
}

}  // namespace

TEST(tokens_overlap_matches_shared_words) {
    CHECK(tokens_overlap("Main Street", "Main St"));
    CHECK(tokens_overlap("Place de l'H\xc3\xb4tel de Ville", "place de lhotel"));
    CHECK(!tokens_overlap("Elm Street", "Oak Avenue"));
    CHECK(!tokens_overlap("A B", "A B"));  // one-letter words never count
    CHECK(!tokens_overlap("", "Main"));
    CHECK(!tokens_overlap("Main", nullptr));
}

TEST(token_matcher_matches_the_hashed_overlap_for_one_a_and_many_b) {
    const std::vector<std::string> words = {"main", "Main", "st", "St.", "a", "b", "rue", "de", "l'eglise",
                                            "\xc3\x89glise", "caf\xc3\xa9", "Cafe", "-", "x-y", "XY", "  ",
                                            "\xe5\x8c\x97\xe4\xba\xac", "o'neil", "oneil", "1st", "23"};
    std::mt19937_64 rng(17);
    auto name = [&] {
        std::string s;
        int n = static_cast<int>(rng() % 5);
        for (int i = 0; i < n; i++) {
            if (i) s += rng() % 4 ? " " : "  ";
            s += words[rng() % words.size()];
        }
        return s;
    };
    TokenMatcher matcher;
    for (int round = 0; round < 20000; round++) {
        const std::string a = name();
        matcher.reset(a.c_str());
        for (int k = 0; k < 8; k++) {
            const std::string b = name();
            CHECK_EQ(matcher.overlaps(b.c_str()), hashed_tokens_overlap(a.c_str(), b.c_str()));
            CHECK_EQ(tokens_overlap(a.c_str(), b.c_str()), hashed_tokens_overlap(a.c_str(), b.c_str()));
        }
    }
}
