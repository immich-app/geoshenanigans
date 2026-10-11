// pbf-apply: `osmium apply-changes <in.pbf> <changes...> -o <out.pbf>`
// (default options: no history, no locations on ways) for a PBF sorted by
// type then id, copying every blob no change touches byte for byte and
// re-encoding the touched ones on every core.
//
// A blob's id range runs from its first id to the next blob's (its first
// object is found from a few KiB of it), so the changes in each range say
// which blobs to touch. A touched blob is merged with its changes and
// encoded again on its own string table, its unchanged ways and relations
// copied message by message; changes past the last id of their type go in
// new blobs at the end of the type.
//
// osmium (osmium-tool src/command_apply_changes.cpp, run()) reads every
// change object in file order, reverses the list, stable-sorts it by
// object_order_type_id_reverse_version (type, id, then newest version and
// timestamp first), std::set_unions it with the input under the same order
// (a change and an input object that compare equal: the change), and
// copy_first_with_id writes the first object of each id if it is visible.
// So per type and id the object that survives is the newest of the input
// object and the changes, a change winning ties with the input and the
// last-read change winning ties among changes, and it is dropped if that
// winner is a delete. copy_first_with_id remembers only the id, not the
// type: the first id of a type is dropped when it equals the last id of the
// type before (and an id 0 first object is dropped); that is kept too.
#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "osc_reader.h"
#include "osm_objects.h"
#include "parallel.h"
#include "pbf_codec.h"
#include "pbf_reader.h"

// Untouched blobs next to each other are copied as one read of at most
// this many bytes.
constexpr size_t kCopySpanBytes = size_t(1) << 20;
// Output pieces produced ahead of the writer, per thread.
constexpr size_t kWritesAheadPerThread = 16;

// osmium's object_order_type_id_reverse_version on two objects of one type
// and id: whether a sorts before b, newest first. Timestamps count only
// when both are set.
inline bool sorts_newer(const OsmObject& a, const OsmObject& b) {
    if (a.version != b.version) return a.version > b.version;
    return a.timestamp != 0 && b.timestamp != 0 && a.timestamp > b.timestamp;
}

// The change that decides an object of one type and id.
struct Winner {
    uint64_t key = 0;               // id_order_key of the id
    ObjectRef ref{nullptr, nullptr};  // null when only suppressed
    bool suppressed = false;        // copy_first_with_id drops this id
};

using Winners = std::array<std::vector<Winner>, kOsmTypes>;

// The deciding change of every type and id the chunks hold, by type in
// osmium's id order: for each, the change sorting first after osmium's
// reverse and stable sort.
inline Winners resolve_winners(const std::vector<std::unique_ptr<ChangeChunk>>& chunks, unsigned threads = 0) {
    struct Entry {
        uint64_t key;
        uint32_t chunk, index;
        uint8_t type;
    };
    std::vector<size_t> base = parallel_offsets<size_t>(
        chunks.size(), [&](size_t c) { return chunks[c]->store.objects.size(); }, threads);
    size_t n = base.back();
    std::vector<Entry> entries(n);
    parallel_for_each(chunks.size(), [&](size_t c, unsigned) {
        const auto& objs = chunks[c]->store.objects;
        for (size_t i = 0; i < objs.size(); i++)
            entries[base[c] + i] = {id_order_key(objs[i].id), uint32_t(c), uint32_t(i), uint8_t(objs[i].type)};
    }, threads);
    // Reading order (chunk, index) breaks ties, so the order is total.
    parallel_sort(entries.begin(), entries.end(), [](const Entry& a, const Entry& b) {
        if (a.type != b.type) return a.type < b.type;
        if (a.key != b.key) return a.key < b.key;
        if (a.chunk != b.chunk) return a.chunk < b.chunk;
        return a.index < b.index;
    }, threads);
    auto object = [&](const Entry& e) -> const OsmObject& { return chunks[e.chunk]->store.objects[e.index]; };
    auto same_id = [&](size_t i) { return entries[i].type == entries[i - 1].type && entries[i].key == entries[i - 1].key; };
    std::vector<uint8_t> wins(n, 0);
    parallel_for_runs(n, same_id, [&](size_t begin, size_t end, unsigned) {
        for (size_t g = begin; g < end;) {
            size_t g_end = g + 1;
            while (g_end < end && same_id(g_end)) g_end++;
            // The reversed list's first element among the newest: walk the
            // group from its last-read change back.
            size_t best = g_end - 1;
            for (size_t j = g_end - 1; j-- > g;)
                if (sorts_newer(object(entries[j]), object(entries[best]))) best = j;
            wins[best] = 1;
            g = g_end;
        }
    }, threads);
    // The winners by type: entries sort by type first.
    std::vector<uint32_t> won = parallel_filter(n, [&](size_t i) { return wins[i] != 0; }, threads);
    Winners out;
    for (size_t t = 0, begin = 0; t < kOsmTypes; t++) {
        size_t end = size_t(std::partition_point(won.begin() + begin, won.end(),
                                                 [&](uint32_t i) { return entries[i].type == t; }) -
                            won.begin());
        out[t].resize(end - begin);
        parallel_for(end - begin, [&](size_t b, size_t e, unsigned) {
            for (size_t k = b; k < e; k++) {
                const Entry& x = entries[won[begin + k]];
                out[t][k] = {x.key, {&object(x), &chunks[x.chunk]->store}, false};
            }
        }, threads);
        begin = end;
    }
    return out;
}

// Appends to `out` the objects of a type that survive merging the input
// objects of one block (`input`, in id order) with the winners [w, w_end)
// of the same id range.
inline void merge_objects(const ObjectStore& input, const Winner* w, const Winner* w_end,
                          std::vector<ObjectRef>& out) {
    auto emit = [&](const Winner& win) {
        if (!win.suppressed && win.ref.object->visible) out.push_back(win.ref);
    };
    for (const OsmObject& in : input.objects) {
        uint64_t key = id_order_key(in.id);
        for (; w != w_end && w->key < key; ++w) emit(*w);
        if (w != w_end && w->key == key) {
            // A change ties with the input object to win (set_union takes
            // the first range's element).
            if (!w->suppressed && sorts_newer(in, *w->ref.object)) {
                if (in.visible) out.push_back({&in, &input});
            } else {
                emit(*w);
            }
            ++w;
        } else if (in.visible) {
            out.push_back({&in, &input});
        }
    }
    for (; w != w_end; ++w) emit(*w);
}

// Appends objs (all of one type, in order) to `out` as zlib OSMData blobs
// of at most max_objects objects, a block over kTargetBlockBytes split in
// halves; seed as for encode_block. Returns the number of blobs.
inline size_t append_object_blobs(std::string& out, OsmType type, const ObjectRef* objs, size_t n,
                                  size_t max_objects = kMaxBlockObjects, const ObjectStore* seed = nullptr) {
    std::string raw;
    size_t count_blobs = 0;
    auto encode = [&](auto&& self, const ObjectRef* first, size_t count) -> void {
        encode_block(type, first, count, raw, seed);
        if (raw.size() > kTargetBlockBytes && count > 1) {
            self(self, first, count / 2);
            self(self, first + count / 2, count - count / 2);
            return;
        }
        append_blob(out, "OSMData", raw);
        count_blobs++;
    };
    for (size_t b = 0; b < n; b += max_objects) encode(encode, objs + b, std::min(max_objects, n - b));
    return count_blobs;
}

struct ApplyOptions {
    std::string input, output;
    std::vector<std::string> changes;
    std::optional<int64_t> replication_timestamp, replication_sequence;
    std::optional<std::string> replication_base_url;
    size_t max_block_objects = kMaxBlockObjects;
    size_t change_chunk_bytes = kOscChunkBytes;
    unsigned threads = 0;
    bool verbose = true;
};

struct ApplyStats {
    size_t change_objects = 0;
    std::array<size_t, kOsmTypes> winners{};
    size_t input_blobs = 0, copied_blobs = 0, merged_blobs = 0, encoded_blobs = 0;
    size_t peek_bytes = 0;
    uint64_t output_bytes = 0;
};

// The input PBF as the output plan needs it.
struct InputLayout {
    std::vector<BlobInfo> blobs;  // [0] is the header blob
    std::vector<BlobPeek> peeks;  // each blob's first object; [0] unused
    std::array<std::optional<size_t>, kOsmTypes> first_blob, last_blob;  // per type, of blobs with objects
    std::array<uint64_t, kOsmTypes> last_key{};  // id key of each type's last object
};

// One piece of the output, in output order.
struct OutputItem {
    enum Kind : uint8_t { Copy, Merge, Tail } kind = Copy;
    OsmType type = OsmType::Node;
    uint64_t offset = 0, size = 0;      // Copy: these bytes of the input
    size_t blob = 0;                    // Merge: this blob of the input...
    uint64_t lo = 0, hi = 0;            // ...whose objects have id keys in [lo, hi)...
    size_t win_begin = 0, win_end = 0;  // Merge, Tail: ...with these winners of `type`
};

namespace pbf_apply_detail {

class FileDescriptor {
public:
    FileDescriptor(const std::string& path, int flags, mode_t mode = 0) : fd_(open(path.c_str(), flags, mode)) {
        if (fd_ < 0) throw std::runtime_error("cannot open " + path + ": " + std::strerror(errno));
    }
    ~FileDescriptor() { close(fd_); }
    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;
    int get() const { return fd_; }

private:
    int fd_;
};

inline double seconds_since(std::chrono::steady_clock::time_point t0) {
    return std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
}

inline void write_all(int fd, const std::string& bytes) {
    const char* p = bytes.data();
    for (size_t n = bytes.size(); n;) {
        ssize_t w = ::write(fd, p, n);
        if (w < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error(std::string("write failed: ") + std::strerror(errno));
        }
        p += w;
        n -= size_t(w);
    }
}

inline std::string read_span(int fd, uint64_t offset, size_t n) {
    std::string buf(n, '\0');
    for (size_t done = 0; done < n;) {
        ssize_t r = pread(fd, buf.data() + done, n - done, static_cast<off_t>(offset + done));
        if (r <= 0) throw std::runtime_error("pread failed on the input PBF");
        done += size_t(r);
    }
    return buf;
}

}  // namespace pbf_apply_detail

// Scans the input's blobs and finds each one's first object, checking the
// file is sorted by type then id as far as blob boundaries show.
inline InputLayout scan_input(const std::string& path, unsigned threads) {
    InputLayout in;
    in.blobs = scan_pbf_blobs(path, threads);
    if (in.blobs.empty() || in.blobs[0].type != "OSMHeader") throw std::runtime_error(path + ": no OSMHeader blob first");
    for (size_t i = 1; i < in.blobs.size(); i++)
        if (in.blobs[i].type != "OSMData") throw std::runtime_error(path + ": unexpected blob type " + in.blobs[i].type);
    // A peek reads a few KiB per blob: readahead would read whole blobs.
    pbf_apply_detail::FileDescriptor fd(path, O_RDONLY);
    posix_fadvise(fd.get(), 0, 0, POSIX_FADV_RANDOM);
    in.peeks.resize(in.blobs.size());
    parallel_for_each(in.blobs.size() - 1, [&](size_t i, unsigned) {
        in.peeks[i + 1] = peek_first_object(fd.get(), in.blobs[i + 1]);
    }, threads);
    for (size_t i = 1, prev = 0; i < in.blobs.size(); i++) {
        const BlobPeek& p = in.peeks[i];
        if (p.type < 0) continue;
        if (prev) {
            const BlobPeek& q = in.peeks[prev];
            if (p.type < q.type || (p.type == q.type && id_order_key(p.first_id) <= id_order_key(q.first_id)))
                throw std::runtime_error(path + ": not sorted by type then id (blob " + std::to_string(i) + ")");
        }
        if (!in.first_blob[p.type]) in.first_blob[p.type] = i;
        in.last_blob[p.type] = i;
        prev = i;
    }
    for (int t = 0; t < kOsmTypes; t++) {
        if (!in.last_blob[t]) continue;
        std::string payload = read_and_decompress_blob(fd.get(), in.blobs[*in.last_blob[t]]);
        ObjectStore store;
        if (decode_block(payload, store) != t) throw std::runtime_error(path + ": object types mixed inside a blob");
        in.last_key[t] = id_order_key(store.objects.back().id);
    }
    return in;
}

// copy_first_with_id remembers the last id it saw, whatever its type, from
// an initial 0: a type's first id (input or change, deleted or not) equal
// to it is dropped. Marks such ids suppressed among the winners.
inline void suppress_carried_ids(Winners& winners, const InputLayout& in) {
    int64_t carry = 0;
    for (int t = 0; t < kOsmTypes; t++) {
        auto& w = winners[t];
        std::optional<uint64_t> first, last;
        if (in.first_blob[t]) {
            first = id_order_key(in.peeks[*in.first_blob[t]].first_id);
            last = in.last_key[t];
        }
        if (!w.empty()) {
            first = first ? std::min(*first, w.front().key) : w.front().key;
            last = last ? std::max(*last, w.back().key) : w.back().key;
        }
        if (!first) continue;
        if (id_from_order_key(*first) == carry) {
            if (!w.empty() && w.front().key == *first) w.front().suppressed = true;
            else w.insert(w.begin(), Winner{*first, {nullptr, nullptr}, true});
        }
        carry = id_from_order_key(*last);
    }
}

// The output, type by type: each blob of the type copied, or merged with
// the winners in its id range when there are any (a type's first blob
// takes those before it too), then the winners past the type's last id in
// blobs of max_block_objects objects.
inline std::vector<OutputItem> plan_output(const InputLayout& in, const Winners& winners, size_t max_block_objects) {
    std::vector<OutputItem> items;
    auto add_copy = [&](size_t i) {
        const BlobInfo& b = in.blobs[i];
        uint64_t size = 4 + b.header_size + b.data_size;
        OutputItem* last = items.empty() ? nullptr : &items.back();
        if (last && last->kind == OutputItem::Copy && last->offset + last->size == b.offset &&
            last->size + size <= kCopySpanBytes) {
            last->size += size;
            return;
        }
        OutputItem it;
        it.offset = b.offset;
        it.size = size;
        items.push_back(it);
    };
    auto add_tail = [&](int t) {
        const auto& w = winners[t];
        auto key_after = [](uint64_t k, const Winner& x) { return k < x.key; };
        size_t from = in.last_blob[t]
                          ? size_t(std::upper_bound(w.begin(), w.end(), in.last_key[t], key_after) - w.begin())
                          : 0;
        for (size_t b = from; b < w.size();) {
            size_t e = b, kept = 0;
            for (; e < w.size() && kept < max_block_objects; e++)
                if (!w[e].suppressed && w[e].ref.object->visible) kept++;
            if (kept) {
                OutputItem it;
                it.kind = OutputItem::Tail;
                it.type = OsmType(t);
                it.win_begin = b;
                it.win_end = e;
                items.push_back(it);
            }
            b = e;
        }
    };
    int type = 0;
    for (size_t i = 1; i < in.blobs.size(); i++) {
        const BlobPeek& p = in.peeks[i];
        if (p.type < 0) {
            add_copy(i);
            continue;
        }
        while (type < p.type) add_tail(type++);
        OutputItem it;
        it.kind = OutputItem::Merge;
        it.type = OsmType(p.type);
        it.blob = i;
        it.lo = i == *in.first_blob[p.type] ? 0 : id_order_key(p.first_id);
        it.hi = in.last_key[p.type] + 1;
        if (i != *in.last_blob[p.type]) {
            size_t next = i + 1;
            while (in.peeks[next].type < 0) next++;
            it.hi = id_order_key(in.peeks[next].first_id);
        }
        const auto& w = winners[p.type];
        auto key_before = [](const Winner& x, uint64_t k) { return x.key < k; };
        it.win_begin = size_t(std::lower_bound(w.begin(), w.end(), it.lo, key_before) - w.begin());
        it.win_end = size_t(std::lower_bound(w.begin() + it.win_begin, w.end(), it.hi, key_before) - w.begin());
        if (it.win_begin == it.win_end) add_copy(i);
        else items.push_back(it);
    }
    while (type < kOsmTypes) add_tail(type++);
    return items;
}

// The output header: the input's bounding box and source, which still
// hold; the replication state only as given, the input's being stale once
// the changes are in (its base URL is kept: the stream is the same).
inline PbfHeader output_header(const PbfHeader& in, const ApplyOptions& opt) {
    for (const auto& f : in.required_features)
        if (f != "OsmSchema-V0.6" && f != "DenseNodes")
            throw std::runtime_error("input PBF requires unsupported feature " + f +
                                     (f == "HistoricalInformation" ? " (history files are not supported)" : ""));
    for (const auto& f : in.optional_features)
        if (f == "LocationsOnWays") throw std::runtime_error("input PBF with LocationsOnWays is not supported");
    PbfHeader out;
    out.bbox = in.bbox;
    out.required_features = {"OsmSchema-V0.6", "DenseNodes"};
    out.optional_features = {"Sort.Type_then_ID"};
    out.writingprogram = "pbf-apply";
    out.source = in.source;
    out.replication_timestamp = opt.replication_timestamp;
    out.replication_sequence = opt.replication_sequence;
    out.replication_base_url = opt.replication_base_url.value_or(in.replication_base_url);
    return out;
}

// The bytes an output item writes. Counts the blobs it encodes in `encoded`.
inline std::string produce_item(const OutputItem& it, int fd, const InputLayout& in, const Winners& winners,
                                const ApplyOptions& opt, std::atomic<size_t>& encoded) {
    if (it.kind == OutputItem::Copy) return pbf_apply_detail::read_span(fd, it.offset, it.size);
    thread_local ObjectStore input;
    thread_local std::vector<ObjectRef> objs;
    objs.clear();
    const Winner* wb = winners[int(it.type)].data() + it.win_begin;
    const Winner* we = winners[int(it.type)].data() + it.win_end;
    std::string payload;  // holds the strings of `input`
    if (it.kind == OutputItem::Merge) {
        payload = read_and_decompress_blob(fd, in.blobs[it.blob]);
        std::string where = opt.input + " blob " + std::to_string(it.blob);
        if (decode_block(payload, input, DecodeLists::UnlessCopyable) != int(it.type))
            throw std::runtime_error(where + ": unexpected object type");
        for (size_t i = 0; i < input.objects.size(); i++) {
            uint64_t key = id_order_key(input.objects[i].id);
            if (key < it.lo || key >= it.hi || (i && key <= id_order_key(input.objects[i - 1].id)))
                throw std::runtime_error(where + ": not sorted by type then id");
        }
        merge_objects(input, wb, we, objs);
    } else {
        for (const Winner* w = wb; w != we; ++w)
            if (!w->suppressed && w->ref.object->visible) objs.push_back(w->ref);
    }
    std::string out;
    // A merged blob keeps its string table: its objects' strings need no
    // lookups and its unchanged ways and relations are copied as they are.
    const ObjectStore* seed = it.kind == OutputItem::Merge ? &input : nullptr;
    encoded += append_object_blobs(out, it.type, objs.data(), objs.size(), opt.max_block_objects, seed);
    return out;
}

inline ApplyStats apply_changes(const ApplyOptions& opt) {
    using namespace pbf_apply_detail;
    using clock = std::chrono::steady_clock;
    const unsigned threads = opt.threads ? opt.threads : parallel_threads();
    auto log = [&](const std::string& s) {
        if (opt.verbose) std::cerr << "[pbf-apply] " << s << std::endl;
    };
    auto t_start = clock::now();
    ApplyStats stats;

    // The change files load while the input is scanned.
    std::vector<std::unique_ptr<ChangeChunk>> chunks;
    std::exception_ptr change_error;
    double t_changes = 0;
    std::thread change_loader([&] {
        try {
            auto t0 = clock::now();
            chunks = read_change_files(opt.changes, opt.change_chunk_bytes, threads);
            t_changes = seconds_since(t0);
        } catch (...) {
            change_error = std::current_exception();
        }
    });
    InputLayout in;
    std::exception_ptr scan_error;
    try {
        in = scan_input(opt.input, threads);
    } catch (...) {
        scan_error = std::current_exception();
    }
    double t_scan = seconds_since(t_start);
    change_loader.join();
    if (scan_error) std::rethrow_exception(scan_error);
    if (change_error) std::rethrow_exception(change_error);
    for (const auto& c : chunks) stats.change_objects += c->store.objects.size();
    stats.input_blobs = in.blobs.size() - 1;
    for (const auto& p : in.peeks) stats.peek_bytes += p.bytes_read;
    log(std::to_string(stats.change_objects) + " change objects read in " + std::to_string(t_changes) + " s; " +
        std::to_string(stats.input_blobs) + " input blobs scanned in " + std::to_string(t_scan) + " s (" +
        std::to_string(stats.peek_bytes >> 20) + " MiB read to find their first objects)");

    auto t_plan = clock::now();
    Winners winners = resolve_winners(chunks, threads);
    for (int t = 0; t < kOsmTypes; t++) stats.winners[t] = winners[t].size();
    suppress_carried_ids(winners, in);
    std::vector<OutputItem> items = plan_output(in, winners, opt.max_block_objects);
    stats.merged_blobs = size_t(std::count_if(items.begin(), items.end(),
                                              [](const OutputItem& it) { return it.kind == OutputItem::Merge; }));
    stats.copied_blobs = stats.input_blobs - stats.merged_blobs;
    log("winners: " + std::to_string(stats.winners[0]) + " nodes, " + std::to_string(stats.winners[1]) + " ways, " +
        std::to_string(stats.winners[2]) + " relations; " + std::to_string(stats.merged_blobs) + " blobs to merge, " +
        std::to_string(stats.copied_blobs) + " to copy; planned in " + std::to_string(seconds_since(t_plan)) + " s");

    auto t_write = clock::now();
    FileDescriptor input(opt.input, O_RDONLY);
    FileDescriptor output(opt.output, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    {
        PbfHeader header = decode_header_block(read_and_decompress_blob(input.get(), in.blobs[0]));
        std::string bytes;
        append_blob(bytes, "OSMHeader", encode_header_block(output_header(header, opt)));
        write_all(output.get(), bytes);
        stats.output_bytes += bytes.size();
    }
    std::atomic<size_t> encoded{0};
    parallel_ordered(items.size(), [&](size_t k) { return produce_item(items[k], input.get(), in, winners, opt, encoded); },
                     [&](size_t, std::string bytes) {
                         write_all(output.get(), bytes);
                         stats.output_bytes += bytes.size();
                     },
                     threads, kWritesAheadPerThread * threads);
    stats.encoded_blobs = encoded.load();
    log("wrote " + std::to_string(stats.output_bytes >> 20) + " MiB (" + std::to_string(stats.encoded_blobs) +
        " blobs encoded) in " + std::to_string(seconds_since(t_write)) + " s; total " +
        std::to_string(seconds_since(t_start)) + " s");
    return stats;
}
