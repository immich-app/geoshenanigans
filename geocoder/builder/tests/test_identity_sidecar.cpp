// Identity keys (identity_sidecar.h): the keys geocoder-diff --identity takes
// from a sidecar and --identity-slots from the records themselves, and the
// old record each new record's key finds.
#include "identity_sidecar.h"

#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <new>

#include "test_framework.h"
#include "types.h"

using gctest::refusal_of;

namespace {

std::vector<char> sidecar_bytes(const std::vector<uint32_t>& keys) {
    std::vector<char> bytes(keys.size() * 4);
    if (!keys.empty()) std::memcpy(bytes.data(), keys.data(), bytes.size());
    return bytes;
}

// The records' bytes, as a merge maps their file.
template <class Record>
std::vector<char> record_bytes(const std::vector<Record>& records) {
    std::vector<char> bytes(records.size() * sizeof(Record));
    if (!records.empty()) std::memcpy(bytes.data(), records.data(), bytes.size());
    return bytes;
}

// The keys of a tombstone, then one record for each of its bytes with that
// byte changed, then a tombstone again.
template <class Record>
std::vector<uint32_t> keys_around(const Record& dead) {
    std::vector<Record> records = {dead};
    for (size_t at = 0; at < sizeof(Record); at++) {
        Record live = dead;
        reinterpret_cast<char*>(&live)[at] ^= 1;
        records.push_back(live);
    }
    records.push_back(dead);
    const auto bytes = record_bytes(records);
    return slot_identity_keys(bytes.data(), bytes.size(), dead);
}

// Keys 0 to n + 1 with the first and the last NO_DATA.
std::vector<uint32_t> dead_first_and_last(size_t n) {
    std::vector<uint32_t> keys(n + 2);
    for (size_t i = 0; i < keys.size(); i++) keys[i] = static_cast<uint32_t>(i);
    keys.front() = keys.back() = NO_DATA;
    return keys;
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

TEST(slot_identity_keys_are_a_live_records_index_and_no_data_for_a_tombstone) {
    const WayHeader dead = street_way_tombstone();
    // Ways 2 and 3 together hold the tombstone's bytes off the record grid:
    // neither is one.
    const auto ways = record_bytes<WayHeader>({{1, 2, 0, 3}, dead, {5, 0, 0, 0}, {NO_DATA, 7, 0, 8}, dead});
    CHECK(slot_identity_keys(ways.data(), ways.size(), dead) == std::vector<uint32_t>({0, NO_DATA, 2, 3, NO_DATA}));
}

// A record one byte off its family's tombstone holds an entity.
TEST(slot_identity_keys_know_each_familys_tombstone_by_all_its_bytes) {
    CHECK(keys_around(street_way_tombstone()) == dead_first_and_last(sizeof(WayHeader)));
    CHECK(keys_around(interp_way_tombstone()) == dead_first_and_last(sizeof(InterpWay)));
    CHECK(keys_around(addr_point_tombstone()) == dead_first_and_last(sizeof(AddrPoint)));
}

// Every tenth slot dead, over more records than one thread's range.
TEST(slot_identity_keys_number_the_records_of_a_large_file) {
    constexpr uint32_t N = 300000;
    const AddrPoint dead = addr_point_tombstone();
    std::vector<AddrPoint> points(N, dead);
    std::vector<uint32_t> want(N, NO_DATA);
    for (uint32_t i = 0; i < N; i++) {
        if (i % 10 == 9) continue;
        points[i].housenumber_id = i + 1;
        want[i] = i;
    }
    const auto bytes = record_bytes(points);
    CHECK(slot_identity_keys(bytes.data(), bytes.size(), dead) == want);
}

// No keys: the diff merges the family by content. A missing record file is
// mapped as no bytes at a null address.
TEST(slot_identity_keys_of_an_empty_or_absent_family_are_none) {
    const std::vector<char> empty;
    CHECK(slot_identity_keys(empty.data(), 0, interp_way_tombstone()).empty());
    CHECK(slot_identity_keys(nullptr, 0, street_way_tombstone()).empty());
}

// Bytes past the last whole record are no record.
TEST(slot_identity_keys_take_whole_records) {
    auto ways = record_bytes<WayHeader>({{1, 2, 0, 3}, street_way_tombstone()});
    ways.resize(ways.size() + 5, 0);
    CHECK(slot_identity_keys(ways.data(), ways.size(), street_way_tombstone()) == std::vector<uint32_t>({0, NO_DATA}));
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
    std::string refusal;
    {
        AddressSpaceCap cap(size_t(2) << 30);
        REQUIRE(cap.active());
        refusal = refusal_of<std::bad_alloc>([&] {
            two = old_index_of_new({far, 17}, {17, far, 2, NO_DATA});
            mixed = old_index_of_new({5, 0, far, NO_DATA, 1}, {far, 1, 5, 7, NO_DATA, 0});
        });
    }
    CHECK_EQ(refusal, std::string());
    CHECK(two == std::vector<uint32_t>({1, 0, NO_DATA, NO_DATA}));
    CHECK(mixed == std::vector<uint32_t>({2, 4, 0, NO_DATA, NO_DATA, 1}));
}

TEST(old_index_of_new_rejects_a_repeated_key) {
    auto refusal = [](const std::vector<uint32_t>& old_key) {
        return refusal_of([&] { old_index_of_new(old_key, {old_key[0]}); });
    };
    const std::string repeated = "Identity sidecar repeats a key";
    CHECK_EQ(refusal({0, 0}), repeated);
    CHECK_EQ(refusal({1, NO_DATA, 1}), repeated);
    CHECK_EQ(refusal({100, 2, 100}), repeated);
    CHECK_EQ(refusal({1, NO_DATA, NO_DATA}), std::string());
    CHECK_EQ(refusal({100, 2, 101}), std::string());
}
