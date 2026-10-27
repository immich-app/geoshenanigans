// Identity sidecars (identity_sidecar.h): the keys geocoder-diff --identity
// takes from a sidecar, and the old record each new record's key finds.
#include "identity_sidecar.h"

#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <new>

#include "test_framework.h"
#include "types.h"

namespace {

std::vector<char> sidecar_bytes(const std::vector<uint32_t>& keys) {
    std::vector<char> bytes(keys.size() * 4);
    if (!keys.empty()) std::memcpy(bytes.data(), keys.data(), bytes.size());
    return bytes;
}

// While in scope, the process can map at most `headroom` bytes beyond what
// it maps now: an allocation past that throws std::bad_alloc instead of
// taking the memory.
class AddressSpaceCap {
public:
    explicit AddressSpaceCap(size_t headroom) {
        long pages = 0;
        if (FILE* f = std::fopen("/proc/self/statm", "r")) {
            if (std::fscanf(f, "%ld", &pages) != 1) pages = 0;
            std::fclose(f);
        }
        if (pages <= 0 || getrlimit(RLIMIT_AS, &saved_) != 0) return;
        rlimit cap = saved_;
        cap.rlim_cur = std::min<rlim_t>(saved_.rlim_max, rlim_t(pages) * rlim_t(sysconf(_SC_PAGESIZE)) + headroom);
        active_ = setrlimit(RLIMIT_AS, &cap) == 0;
    }
    ~AddressSpaceCap() {
        if (active_) setrlimit(RLIMIT_AS, &saved_);
    }
    AddressSpaceCap(const AddressSpaceCap&) = delete;
    AddressSpaceCap& operator=(const AddressSpaceCap&) = delete;
    bool active() const { return active_; }

private:
    rlimit saved_{};
    bool active_ = false;
};

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

TEST(identity_keys_reject_a_partial_key) {
    // 4N+1..4N+3 bytes for N records: N whole keys and a part of one more.
    for (size_t extra = 1; extra <= 3; extra++) {
        auto bytes = sidecar_bytes({7, NO_DATA, 2});
        bytes.resize(bytes.size() + extra, 0);
        CHECK(identity_keys(bytes, 3).empty());
    }
}

TEST(old_index_of_new_finds_the_old_record_of_each_key) {
    CHECK(old_index_of_new({3, 0, NO_DATA, 2}, {2, 3, 5, NO_DATA, 0}) ==
          std::vector<uint32_t>({3, 0, NO_DATA, NO_DATA, 1}));
    CHECK(old_index_of_new({}, {1, NO_DATA}) == std::vector<uint32_t>({NO_DATA, NO_DATA}));
    CHECK(old_index_of_new({NO_DATA, NO_DATA}, {0, NO_DATA}) == std::vector<uint32_t>({NO_DATA, NO_DATA}));
}

TEST(old_index_of_new_takes_memory_by_record_count_not_key) {
    // Keys up to 0xFFFFFFFE: a table indexed by key would take 16 GiB.
    const uint32_t far = 0xFFFFFFFEu;
    std::vector<uint32_t> two, mixed;
    bool fits = true;
    {
        AddressSpaceCap cap(size_t(2) << 30);
        REQUIRE(cap.active());
        try {
            two = old_index_of_new({far, 17}, {17, far, 2, NO_DATA});
            mixed = old_index_of_new({5, 0, far, NO_DATA, 1}, {far, 1, 5, 7, NO_DATA, 0});
        } catch (const std::bad_alloc&) {
            fits = false;
        }
    }
    CHECK(fits);
    CHECK(two == std::vector<uint32_t>({1, 0, NO_DATA, NO_DATA}));
    CHECK(mixed == std::vector<uint32_t>({2, 4, 0, NO_DATA, NO_DATA, 1}));
}

TEST(old_index_of_new_rejects_a_repeated_key) {
    CHECK(repeats_a_key({0, 0}));
    CHECK(repeats_a_key({1, NO_DATA, 1}));
    CHECK(repeats_a_key({100, 2, 100}));
    CHECK(!repeats_a_key({1, NO_DATA, NO_DATA}));
    CHECK(!repeats_a_key({100, 2, 101}));
}
