// src/paged_bits.h: a set of ids whose pages exist only where an id is set.
#include "paged_bits.h"

#include <random>
#include <set>

#include "test_framework.h"

TEST(paged_bits_of_two_billion_ids_hold_no_page_until_one_is_set) {
    PagedBits bits(2000000000);
    CHECK_EQ(bits.pages_used(), size_t(0));
    CHECK(!bits.test(0));
    CHECK(!bits.test(1999999999));
    CHECK(!bits.test(2000000000));
    bits.set(1999999999);
    CHECK_EQ(bits.pages_used(), size_t(1));
    CHECK(bits.test(1999999999));
    CHECK(!bits.test(1999999998));
    // An id at or past the limit is never set.
    CHECK(!bits.test(2000000000));
    CHECK(!bits.test(UINT64_MAX));
}

TEST(paged_bits_hold_the_ids_set_and_no_others_with_a_page_for_each_one_touched) {
    PagedBits bits(uint64_t(1) << 32);
    std::mt19937_64 rng(3);
    std::set<uint64_t> ids, pages;
    for (int k = 0; k < 5000; k++) {
        // Ids in a few pages, and next to the pages' edges.
        const uint64_t page = rng() % 7 * 97;
        const uint64_t at = k % 3 == 0 ? (k % 2 ? 0 : PagedBits::kPageBits - 1) : rng() % PagedBits::kPageBits;
        const uint64_t id = page * PagedBits::kPageBits + at;
        bits.set(id);
        ids.insert(id);
        pages.insert(page);
    }
    CHECK_EQ(bits.pages_used(), pages.size());
    // The limit, the first id of the page past the last: never set, and no
    // page is read for it.
    CHECK(!bits.test(uint64_t(1) << 32));
    for (uint64_t id : ids) CHECK(bits.test(id));
    for (int k = 0; k < 100000; k++) {
        const uint64_t id = rng() % (uint64_t(1) << 32);
        CHECK_EQ(bits.test(id), ids.count(id) > 0);
    }
    for (uint64_t id : ids) {
        if (!ids.count(id + 1)) CHECK(!bits.test(id + 1));
        if (id > 0 && !ids.count(id - 1)) CHECK(!bits.test(id - 1));
    }
}
