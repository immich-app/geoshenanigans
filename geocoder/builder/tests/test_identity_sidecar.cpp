// Identity sidecars (identity_sidecar.h): the keys geocoder-diff --identity
// takes from a sidecar, and the old record each new record's key finds.
#include "identity_sidecar.h"

#include <cstring>

#include "test_framework.h"
#include "types.h"

namespace {

std::vector<char> sidecar_bytes(const std::vector<uint32_t>& keys) {
    std::vector<char> bytes(keys.size() * 4);
    if (!keys.empty()) std::memcpy(bytes.data(), keys.data(), bytes.size());
    return bytes;
}

bool repeats_a_key(const std::vector<uint32_t>& old_key) {
    try {
        old_index_of_new(old_key, {old_key[0]});
    } catch (const std::runtime_error&) {
        return true;
    }
    return false;
}

}  // namespace

TEST(identity_keys_need_one_key_per_record) {
    const auto bytes = sidecar_bytes({7, NO_DATA, 2});
    CHECK(identity_keys(bytes, 3) == std::vector<uint32_t>({7, NO_DATA, 2}));
    CHECK(identity_keys(bytes, 2).empty());
    CHECK(identity_keys(bytes, 4).empty());
    CHECK(identity_keys({}, 3).empty());
    CHECK(identity_keys({}, 0).empty());
}

TEST(old_index_of_new_finds_the_old_record_of_each_key) {
    CHECK(old_index_of_new({3, 0, NO_DATA, 2}, {2, 3, 5, NO_DATA, 0}) ==
          std::vector<uint32_t>({3, 0, NO_DATA, NO_DATA, 1}));
    CHECK(old_index_of_new({}, {1, NO_DATA}) == std::vector<uint32_t>({NO_DATA, NO_DATA}));
    CHECK(old_index_of_new({NO_DATA, NO_DATA}, {0, NO_DATA}) == std::vector<uint32_t>({NO_DATA, NO_DATA}));
}

TEST(old_index_of_new_rejects_a_repeated_key) {
    CHECK(repeats_a_key({0, 0}));
    CHECK(repeats_a_key({1, NO_DATA, 1}));
    CHECK(!repeats_a_key({1, NO_DATA, NO_DATA}));
}
