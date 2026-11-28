// A record's tag bundle: the tags of its OSM object that Nominatim reads as
// names, or as an address (host data), as OSM has them, in one string. The
// search reads it (search/src/tags.rs) and applies Nominatim's sanitizers to
// it itself. Pairs of key KEY_SEP value, joined by PAIR_SEP, keys sorted
// bytewise and each once; a bundle holds one pair at the least, so it holds a
// KEY_SEP and never equals a plain name.
//
// The keys are those of Nominatim's full import style with extratags
// (lib-lua/themes/nominatim/topics/full.lua, presets.lua): a key its
// pre-filter takes (the ignored meta, name and address keys) is neither, a
// key of its name groups is a name, and any other key of its address groups
// is an address key. A key both groups match is a name, as Nominatim takes
// the names first.
//
// The scans collect bundles by OSM object (Store), and each dir's records
// find theirs through the object ids it has for them, so no array of
// bundles follows the records through their sorts, the stable slots and the
// continent split.
#pragma once

#include <algorithm>
#include <array>
#include <filesystem>
#include <sys/mman.h>
#include <unistd.h>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "parallel.h"
#include "place_claims.h"
#include "string_files.h"
#include "types.h"

namespace tag_bundles {

inline constexpr char KEY_SEP = TAG_KEY_SEP, PAIR_SEP = TAG_PAIR_SEP;
// Where a tag goes.
enum class KeyClass : uint8_t { OTHER, NAME, ADDRESS };

namespace detail {

constexpr bool starts_with(std::string_view s, std::string_view p) { return s.substr(0, p.size()) == p; }
constexpr bool ends_with(std::string_view s, std::string_view p) {
    return s.size() >= p.size() && s.substr(s.size() - p.size()) == p;
}
template <size_t N>
constexpr bool any_of(std::string_view k, const std::string_view (&list)[N], bool (*match)(std::string_view, std::string_view)) {
    for (std::string_view p : list)
        if (match(k, p)) return true;
    return false;
}
constexpr bool equals(std::string_view s, std::string_view p) { return s == p; }

// The pre-filter: IGNORE_KEYS.metatags, which it deletes, and
// IGNORE_KEYS.name and .address, which go to the extratags.
constexpr std::string_view kIgnored[] = {
    "note", "source", "attribution", "comment", "fixme", "created_by", "tiger:cfcc", "tiger:reviewed", "import",
    "ref:bygningsnr", "building:ruian:type", "type", "is_in:postcode", "name:full", "name:etymology", "name:signed",
    "name:botanical", "addr:TW:dataset",
};
constexpr std::string_view kIgnoredPrefixes[] = {
    "note:", "source:", "nysgissam:", "NHD:", "nhd:", "gnis:", "geobase:", "yh:", "osak:", "naptan:", "CLC:",
    "it:fvg:", "lacounty:", "ref:linz:", "survey:", "ref:ruian:", "name:etymology:", "addr:street:", "addr:city:",
    "addr:district:", "addr:province:", "addr:subdistrict:", "addr:place:",
};
constexpr std::string_view kIgnoredSuffixes[] = {"source", ":forward", ":backward"};

// NAME_TAGS core, address and poi.
constexpr std::string_view kNames[] = {
    "name", "int_name", "reg_name", "loc_name", "old_name", "alt_name", "official_name", "place_name", "short_name",
    "brand", "ref", "int_ref", "nat_ref", "reg_ref", "loc_ref", "old_ref", "ISO3166-2", "iata", "icao", "faa",
    "addr:housename",
};
constexpr std::string_view kNamePrefixes[] = {
    "name:", "int_name:", "reg_name:", "loc_name:", "old_name:", "alt_name:", "alt_name_", "official_name:",
    "place_name:", "short_name:", "name:prefix:", "name:suffix:",
};
constexpr std::string_view kNameSuffixes[] = {":prefix", ":suffix"};

// ADDRESS_TAGS core and houses.
constexpr std::string_view kAddresses[] = {
    "tiger:county", "postal_code", "postcode", "addr:postcode", "tiger:zip_left", "tiger:zip_right", "country_code",
    "ISO3166-1", "addr:country_code", "is_in:country_code", "addr:country", "is_in:country", "addr:housenumber",
    "addr:conscriptionnumber", "addr:streetnumber", "addr:interpolation", "addr:place", "addr:substreet",
};
constexpr std::string_view kAddressPrefixes[] = {"addr:", "is_in:"};

}  // namespace detail

constexpr KeyClass key_class(std::string_view k) {
    using namespace detail;
    if (k == "name" || k == "name:en") return KeyClass::NAME;
    if (k == "addr:housenumber" || k == "addr:street" || k == "addr:city" || k == "addr:postcode") return KeyClass::ADDRESS;
    if (any_of(k, kIgnored, equals) || any_of(k, kIgnoredPrefixes, starts_with) || any_of(k, kIgnoredSuffixes, ends_with))
        return KeyClass::OTHER;
    if (any_of(k, kNames, equals) || any_of(k, kNameSuffixes, ends_with) || any_of(k, kNamePrefixes, starts_with))
        return KeyClass::NAME;
    if (any_of(k, kAddresses, equals) || any_of(k, kAddressPrefixes, starts_with)) return KeyClass::ADDRESS;
    return KeyClass::OTHER;
}

using Pair = std::pair<std::string_view, std::string_view>;

// The bundle of `pairs`, which it sorts; "" for none. A pair holding a
// separator or a NUL can't be in one and is left out, and so are the pairs
// past the most a string tier holds of one string (string_files::MAX_STRING),
// in key order: a bundle is at most that long.
inline std::string encode(std::vector<Pair>& pairs) {
    std::sort(pairs.begin(), pairs.end());
    pairs.erase(std::unique(pairs.begin(), pairs.end(), [](const Pair& a, const Pair& b) { return a.first == b.first; }),
                pairs.end());
    std::string out;
    for (const auto& [k, v] : pairs) {
        auto encodable = [](std::string_view s) { return s.find_first_of(std::string_view("\x1E\x1F\0", 3)) == s.npos; };
        if (k.empty() || !encodable(k) || !encodable(v)) continue;
        const size_t grown = out.size() + (out.empty() ? 0 : 1) + k.size() + 1 + v.size();
        if (grown > string_files::MAX_STRING) break;
        if (!out.empty()) out += PAIR_SEP;
        out.append(k).append(1, KEY_SEP).append(v);
    }
    return out;
}

// The bundle of an object's tags of class `cls` (each(f) calls f(key, value)
// for each tag), the keys of `except` left out.
template <class EachTag>
std::string bundle_of(EachTag&& each, KeyClass cls, std::string_view except = {}) {
    std::vector<Pair> pairs;
    each([&](std::string_view k, std::string_view v) {
        if (key_class(k) == cls && k != except) pairs.emplace_back(k, v);
    });
    return encode(pairs);
}

// The pairs of a bundle, keys ascending.
inline std::vector<Pair> pairs_of(std::string_view bundle) {
    std::vector<Pair> out;
    while (!bundle.empty()) {
        const size_t end = bundle.find(PAIR_SEP);
        const std::string_view pair = bundle.substr(0, end);
        const size_t sep = pair.find(KEY_SEP);
        if (sep != pair.npos) out.emplace_back(pair.substr(0, sep), pair.substr(sep + 1));
        if (end == bundle.npos) break;
        bundle.remove_prefix(end + 1);
    }
    return out;
}

// A bundle of the name tag alone says no more than the record's stored name:
// its record takes NO_DATA.
inline bool name_alone(std::string_view bundle) {
    return bundle.find(PAIR_SEP) == bundle.npos && bundle.substr(0, 5) == std::string_view("name\x1F", 5);
}

// A boundary's names with the names of the place node it links, as
// Nominatim's placex_indexing_prepare merges them: the boundary's own names
// but those whose key starts with "_", and every name of the node's that the
// boundary doesn't have as it is, under the key "_place_" + its key. The
// node's name counts as its name in `node_language` too, the only language of
// its country (country_language), where it has no name in it.
inline std::string merge_linked(std::string_view boundary, std::string_view node, std::string_view node_language) {
    std::vector<Pair> own;
    for (const Pair& p : pairs_of(boundary))
        if (p.first.substr(0, 1) != "_") own.push_back(p);
    std::vector<std::pair<std::string, std::string_view>> theirs;
    for (const Pair& p : pairs_of(node)) theirs.emplace_back(std::string(p.first), p.second);
    auto value_of = [&](std::string_view k) {
        auto it = std::find_if(theirs.begin(), theirs.end(), [&](const auto& p) { return p.first == k; });
        return it == theirs.end() ? nullptr : &it->second;
    };
    if (!node_language.empty()) {
        const std::string in_language = "name:" + std::string(node_language);
        if (const std::string_view* name = value_of("name"); name && !value_of(in_language))
            theirs.emplace_back(in_language, *name);
    }
    std::vector<std::string> keys;
    for (const auto& [k, v] : theirs)
        if (std::find(own.begin(), own.end(), Pair(k, v)) == own.end()) keys.push_back("_place_" + k);
    std::vector<Pair> merged = own;
    size_t at = 0;
    for (const auto& [k, v] : theirs)
        if (std::find(own.begin(), own.end(), Pair(k, v)) == own.end()) merged.emplace_back(keys[at++], v);
    return encode(merged);
}

// The key a record's object has in a Store: its packed osm id, a relation's
// ring index cleared (the rings of one relation are one object).
inline uint64_t key_of(uint64_t packed_osm_id) { return admin_boundary_id(packed_osm_id); }

// Bundles each held once, in blocks that never move.
class Arena {
public:
    ~Arena() {
        for (char* p : mapped_) munmap(p, TAG_ARENA_BLOCK_BYTES);
        if (fd_ >= 0) close(fd_);
    }

    // Address bundles are mostly unique. Keep their bytes reclaimable while
    // the builder holds the planet geometry, without building a hash table.
    const char* append_on_disk(std::string_view s) {
        if (fd_ < 0) {
            auto path = (std::filesystem::temp_directory_path() / "address-tags-XXXXXX").string();
            fd_ = mkstemp(path.data());
            if (fd_ < 0) throw std::runtime_error("cannot create address tag scratch file");
            unlink(path.c_str());
        }
        const size_t need = s.size() + 1;
        if (need > TAG_ARENA_BLOCK_BYTES) throw std::length_error("address bundle exceeds its scratch block");
        if (mapped_.empty() || used_ + need > TAG_ARENA_BLOCK_BYTES) {
            if (!mapped_.empty()) madvise(mapped_.back(), TAG_ARENA_BLOCK_BYTES, MADV_DONTNEED);
            const off_t start = mapped_.size() * TAG_ARENA_BLOCK_BYTES;
            if (ftruncate(fd_, start + TAG_ARENA_BLOCK_BYTES)) throw std::runtime_error("cannot extend address tag scratch file");
            void* p = mmap(nullptr, TAG_ARENA_BLOCK_BYTES, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, start);
            if (p == MAP_FAILED) throw std::runtime_error("cannot map address tag scratch file");
            mapped_.push_back(static_cast<char*>(p));
            used_ = 0;
        }
        char* dst = mapped_.back() + used_;
        std::memcpy(dst, s.data(), s.size());
        dst[s.size()] = 0;
        used_ += need;
        return dst;
    }

    const char* intern(std::string_view s) {
        auto it = index_.find(s);
        if (it != index_.end()) return it->second;
        const size_t need = s.size() + 1;
        if (blocks_.empty() || used_ + need > block_size_) {
            block_size_ = std::max(TAG_ARENA_BLOCK_BYTES, need);
            blocks_.emplace_back(new char[block_size_]);
            bytes_ += block_size_;
            used_ = 0;
        }
        char* dst = blocks_.back().get() + used_;
        std::memcpy(dst, s.data(), s.size());
        dst[s.size()] = '\0';
        used_ += need;
        index_.emplace(std::string_view(dst, s.size()), dst);
        return dst;
    }
    // Frees the index: the bundles stay.
    void release_index() { decltype(index_)().swap(index_); }
    uint64_t bytes() const { return bytes_; }

private:
    int fd_ = -1;
    std::vector<char*> mapped_;
    std::vector<std::unique_ptr<char[]>> blocks_;
    size_t block_size_ = 0, used_ = 0;
    uint64_t bytes_ = 0;
    std::unordered_map<std::string_view, const char*> index_;
};

// An object's key and its bundle.
struct Entry {
    uint64_t key;
    const char* bundle;
};

// One parse thread's bundles.
class Collector {
public:
    void add(uint64_t key, std::string_view bundle, bool disk = false) {
        if (!bundle.empty()) entries_.push_back({key, disk ? arena_->append_on_disk(bundle) : arena_->intern(bundle)});
    }
    size_t size() const { return entries_.size(); }

private:
    friend class Store;
    std::unique_ptr<Arena> arena_ = std::make_unique<Arena>();
    std::vector<Entry> entries_;
};

// The bundles of the objects of every scan, by key: each pass's collectors
// are absorbed, then the store is sorted once and looked up.
class Store {
public:
    // Takes the collectors' bundles, freeing their indexes.
    void absorb(std::vector<Collector>& collectors) {
        for (Collector& collector : collectors) absorb(collector);
    }

    void absorb(Collector& collector) {
        if (collector.entries_.empty()) return;
        entries_.insert(entries_.end(), collector.entries_.begin(), collector.entries_.end());
        std::vector<Entry>().swap(collector.entries_);
        collector.arena_->release_index();
        arenas_.push_back(std::move(collector.arena_));
        sorted_ = false;
    }

    // Sorts the keys. An object read twice (a relation that is a boundary and
    // a POI) has one bundle, which stays once.
    void sort(unsigned threads = 0) {
        auto less = [](const Entry& a, const Entry& b) {
            return a.key != b.key ? a.key < b.key : std::strcmp(a.bundle, b.bundle) < 0;
        };
        parallel_sort(entries_.begin(), entries_.end(), less, threads);
        entries_.erase(std::unique(entries_.begin(), entries_.end(),
                                   [](const Entry& a, const Entry& b) { return a.key == b.key; }),
                       entries_.end());
        sorted_ = true;
    }

    // The object's bundle; "" for none.
    std::string_view find(uint64_t key) const {
        if (!sorted_) throw std::logic_error("tag_bundles::Store::find before sort");
        auto it = std::lower_bound(entries_.begin(), entries_.end(), key, [](const Entry& e, uint64_t k) { return e.key < k; });
        return it != entries_.end() && it->key == key ? std::string_view(it->bundle) : std::string_view();
    }

    // Gives each object of `bundles` its bundle there, in place of any it had.
    void replace(const std::vector<std::pair<uint64_t, std::string>>& bundles, unsigned threads = 0) {
        if (bundles.empty()) return;
        auto arena = std::make_unique<Arena>();
        const size_t n = entries_.size();
        for (const auto& [key, bundle] : bundles) {
            if (bundle.empty()) continue;
            const char* stored = arena->intern(bundle);
            auto it = std::lower_bound(entries_.begin(), entries_.begin() + n, key,
                                       [](const Entry& e, uint64_t k) { return e.key < k; });
            if (it != entries_.begin() + n && it->key == key) it->bundle = stored;
            else entries_.push_back({key, stored});
        }
        arena->release_index();
        arenas_.push_back(std::move(arena));
        if (entries_.size() > n) sort(threads);
    }

    size_t size() const { return entries_.size(); }
    // The memory it holds.
    uint64_t bytes() const {
        uint64_t b = entries_.capacity() * sizeof(Entry);
        for (const auto& a : arenas_) b += a->bytes();
        return b;
    }

private:
    std::vector<Entry> entries_;
    std::vector<std::unique_ptr<Arena>> arenas_;
    bool sorted_ = true;
};

// A string tier laid out in record order (the tag tiers): each distinct
// bundle once, where the first record of its home group to use it sits, the
// groups in order, so the bundles of groups <= g are the tier's head
// (group_ends[g]) and a reader walking the records reads the tier front to
// back. offsets: each record's bundle's, local to the tier, NO_DATA for none.
struct LaidOutTier {
    std::vector<char> bytes;
    std::vector<uint32_t> offsets;
    std::vector<uint32_t> group_ends;
};

// The tier of n records, record r's bundle bundle(r) ("" for none) of group
// home(r) < groups.
template <class BundleOf, class HomeOf>
LaidOutTier lay_out(size_t n, size_t groups, BundleOf&& bundle, HomeOf&& home, unsigned threads = 0) {
    std::vector<std::string_view> of(n);
    parallel_for(n, [&](size_t b, size_t e, unsigned) {
        for (size_t r = b; r < e; r++) of[r] = bundle(r);
    }, threads);
    struct Distinct {
        size_t home, first;
    };
    std::unordered_map<std::string_view, uint32_t> index;
    std::vector<std::string_view> strings;
    std::vector<Distinct> distinct;
    std::vector<uint32_t> which(n, NO_DATA);
    for (size_t r = 0; r < n; r++) {
        if (of[r].empty()) continue;
        const size_t h = home(r);
        auto [it, added] = index.emplace(of[r], static_cast<uint32_t>(distinct.size()));
        if (added) {
            strings.push_back(of[r]);
            distinct.push_back({h, r});
        } else if (h < distinct[it->second].home) {
            distinct[it->second] = {h, r};
        }
        which[r] = it->second;
    }
    std::vector<uint32_t> order(distinct.size());
    for (uint32_t d = 0; d < order.size(); d++) order[d] = d;
    std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
        return std::make_pair(distinct[a].home, distinct[a].first) < std::make_pair(distinct[b].home, distinct[b].first);
    });
    LaidOutTier out;
    out.group_ends.assign(groups, 0);
    std::vector<uint32_t> at(distinct.size());
    size_t g = 0;
    for (uint32_t d : order) {
        for (; g < distinct[d].home; g++) out.group_ends[g] = static_cast<uint32_t>(out.bytes.size());
        if (out.bytes.size() + strings[d].size() + 1 > UINT32_MAX) throw std::length_error("a tag tier past 4 GiB");
        at[d] = static_cast<uint32_t>(out.bytes.size());
        out.bytes.insert(out.bytes.end(), strings[d].begin(), strings[d].end());
        out.bytes.push_back('\0');
    }
    for (; g < groups; g++) out.group_ends[g] = static_cast<uint32_t>(out.bytes.size());
    out.offsets.resize(n);
    for (size_t r = 0; r < n; r++) out.offsets[r] = which[r] == NO_DATA ? NO_DATA : at[which[r]];
    return out;
}

}  // namespace tag_bundles

namespace tag_bundles {

enum class Use { Core, Poi, Wiki, Street, Address, PoiAddress, Interpolation, PoiClass, Count };

inline bool poi_name_key(std::string_view k) { return k == "brand" || k == "iata" || k == "icao" || k == "faa"; }

template <class Each>
std::string select(Use use, Each each, bool road_member = false) {
    std::vector<Pair> pairs;
    each([&](std::string_view k, std::string_view v) {
        if (use == Use::Wiki) {
            if (k == "wikidata" || k == "wikipedia" || detail::starts_with(k, "wikipedia:")) pairs.emplace_back(k, v);
            return;
        }
        const auto cls = key_class(k);
        const bool house = k == "addr:housename";
        const bool name = cls == KeyClass::NAME && !house && (use == Use::Poi || !poi_name_key(k));
        bool keep = name;
        if (use == Use::Address) keep = name || house || cls == KeyClass::ADDRESS;
        if (use == Use::PoiAddress) keep = house || cls == KeyClass::ADDRESS;
        if (use == Use::Interpolation) keep = k == "addr:interpolation" || k == "addr:street" || k == "addr:place";
        if (keep) pairs.emplace_back(k, v);
    });
    if (road_member) pairs.emplace_back("_road_member", "yes");
    return encode(pairs);
}

template <class Each>
std::string poi_class(Each each, const char* class_key) {
    std::string out;
    if (class_key) each([&](std::string_view key, std::string_view value) {
        if (key == class_key) out = std::string(key) + "=" + std::string(value);
    });
    return out;
}

struct Collections {
    std::array<Collector, size_t(Use::Count)> values;
    template <class Each>
    void add(Use use, uint64_t id, Each each, bool member = false) {
        values[size_t(use)].add(id, select(use, each, member), use == Use::Address);
    }
};

struct SearchTags {
    bool host = false;
    std::array<Store, size_t(Use::Count)> stores;
    void absorb(Collections& part) {
        for (size_t i = 0; i < stores.size(); ++i) stores[i].absorb(part.values[i]);
    }
    std::string_view get(Use use, uint64_t packed) const {
        return stores[size_t(use)].find(key_of(packed));
    }
    void sort(unsigned threads) {
        for (auto& store : stores) store.sort(threads);
    }
};

inline auto indexed_tags(const uint32_t* keys, const uint32_t* vals, size_t count,
                         const std::vector<std::string>& strings) {
    return [=, &strings](auto f) {
        for (size_t i = 0; i < count; ++i)
            if (keys[i] < strings.size() && vals[i] < strings.size()) f(strings[keys[i]], strings[vals[i]]);
    };
}

}  // namespace tag_bundles
