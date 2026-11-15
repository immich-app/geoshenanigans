// The patcher's replay of a way pair, the street or the interpolation ways, on its two files as
// they sit (way_files.h): the ways' records' merge sequence and the nodes' section after it, as
// the joint replay of the logical records and node stream would apply them, but without decoding
// and encoding again every node a daily patch leaves alone. A group's node blocks are a function
// of its ways' nodes alone (each is packed against the group's first node), so a new group whose
// ways have, way for way, the nodes of the old group the replay is at (their node bytes one
// MATCH of that group's, their counts its row's) gets that group's blocks: copied as they are,
// its row written with the old anchor, counts and codes and the new records' names and numbers.
// Every other group goes through the encoder (way_files::Encoder) a way at a time, the old nodes
// it keeps decoded from their blocks, read again: those of the old group the replay is at, of
// one of the groups it passed last, or of any other, which a cursor over the old rows finds. A
// copied block is never decoded, and a copied group's row is taken as it is: both old files are
// hashed as the replay passes them and checked against the diff's hashes
// (OLD_FILE_HASH_MARKER).
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "child_ops.h"
#include "gcs_codec.h"
#include "patch_format.h"

namespace way_replay {
namespace wf = way_files;

// The kind of the pair whose records' file a patch names fid; null for any other file.
inline const wf::Kind* kind_of(uint32_t fid) {
    if (fid == static_cast<uint32_t>(PatchFileId::STREET_WAYS)) return &wf::STREETS;
    if (fid == static_cast<uint32_t>(PatchFileId::INTERP_WAYS)) return &wf::INTERPS;
    return nullptr;
}
inline PatchFileId ways_id(const wf::Kind& kind) { return kind.interp ? PatchFileId::INTERP_WAYS : PatchFileId::STREET_WAYS; }
inline PatchFileId nodes_id(const wf::Kind& kind) { return kind.interp ? PatchFileId::INTERP_NODES : PatchFileId::STREET_NODES; }

// The hashes (lane_hash) of a pair's two files.
struct Hashes {
    uint64_t ways = 0, nodes = 0;
};

// The old pair front to back, a way at a time: each group's row as its first way comes, checked
// against the rows before it as way_files::decode checks them, and the group's blocks, which
// stay unread until they are copied or passed. Every byte of both files is hashed in order. Old
// nodes are decoded from blocks read again, wherever the pass is.
class OldWays {
public:
    // Where nodes() found a way's block: in the group the pass is at, in one of those it passed
    // last, or by the cursor.
    enum Source { AT_HAND, PASSED, CURSOR };

    OldWays(const std::string& dir, const wf::Kind& kind)
        : kind_(kind), f_(gcs::ways::footers_of(dir, kind)), ring_(HISTORY_GROUPS) {
        if (!f_.ways) return;
        R_.reset(new gcs::Reader(dir + "/" + kind.ways_file, gcs::ways::ROWS_BUF));
        B_.reset(new gcs::Reader(dir + "/" + kind.nodes_file, gcs::ways::BLOCKS_BUF));
        rows_hash_.reset(new LaneHasher(R_->size()));
        blocks_hash_.reset(new LaneHasher(B_->size()));
    }
    uint64_t ways() const { return f_.ways; }
    uint64_t nodes_bytes() const { return f_.nodes * wf::NODE_BYTES; }

    // The next way (the blocks of the group before its own passed when they weren't copied).
    void next() {
        if (way_ == f_.ways) wf::malformed(std::string(kind_.ways_file) + ": a way past the last");
        j_ = static_cast<size_t>(way_ % wf::GROUP_WAYS);
        if (j_ == 0) open_group();
        before_ = node_;
        node_ += row_.cnt[j_];
        way_++;
    }
    // The way's logical record, kind.stride bytes.
    void record(uint8_t* out) const { wf::logical_record(kind_, row_, j_, before_, out); }

    // The way's group: its row (one of no ways before the first way) and the pair's nodes before
    // it.
    const wf::Row& row() const { return row_; }
    uint64_t group_node() const { return group_node_; }
    // The next `bytes` of the group's blocks through out(p, n), a piece at a time, as they are.
    template <typename Out>
    void copy_blocks(uint64_t bytes, Out out) {
        if (bytes > group_left_) throw std::logic_error("more bytes than the old way group's blocks left");
        while (bytes) {
            const size_t n = static_cast<size_t>(std::min<uint64_t>(bytes, PIECE));
            const uint8_t* p = B_->take(n);
            blocks_hash_->update(reinterpret_cast<const char*>(p), n);
            out(p, n);
            bytes -= n;
            group_left_ -= n;
        }
    }

    // Old nodes: the bytes of the logical node stream from `at` (a node's first, below the
    // pair's last), up to k of them and to the end of the way that holds them, to dst, decoded
    // from that way's block. Returns how many, and in `from` where the block was.
    size_t nodes(uint64_t at, uint64_t k, uint8_t* dst, Source& from) {
        const uint64_t a = at / wf::NODE_BYTES;
        const Group& g = find(a, from);
        size_t j = 0;
        uint64_t first = g.node, block = g.off;
        for (; a - first >= g.cnt[j]; j++) {
            first += g.cnt[j];
            block += wf::block_bytes(g.cnt[j], g.code[j]);
        }
        const size_t logical = wf::NODE_BYTES * size_t(g.cnt[j]), skip = static_cast<size_t>(a - first) * wf::NODE_BYTES;
        const size_t n = static_cast<size_t>(std::min<uint64_t>(k, logical - skip));
        // A way asked for whole is decoded where it goes; one asked for in parts once, and kept.
        if (skip == 0 && n == logical) {
            decode(g, j, block, dst);
            return n;
        }
        if (!(kept_ && kept_first_ == first)) {
            kept_way_.resize(logical);
            decode(g, j, block, kept_way_.data());
            kept_ = true;
            kept_first_ = first;
        }
        memcpy(dst, kept_way_.data() + skip, n);
        return n;
    }

    // Passes the ways left, checks the files end as the encoder ends them, and returns their
    // hashes.
    Hashes finish() {
        while (way_ < f_.ways) next();
        pass_blocks();
        if (at_ != f_.blocks_bytes) wf::malformed(std::string(kind_.nodes_file) + " has bytes between its last block and its footer");
        if (node_ != f_.nodes)
            wf::malformed(std::string(kind_.nodes_file) + " counts " + std::to_string(f_.nodes) + " nodes and its ways have " +
                          std::to_string(node_));
        rows_hash_->update(reinterpret_cast<const char*>(R_->take(wf::WAYS_FOOTER_BYTES)), wf::WAYS_FOOTER_BYTES);
        blocks_hash_->update(reinterpret_cast<const char*>(B_->take(wf::NODES_FOOTER_BYTES)), wf::NODES_FOOTER_BYTES);
        return {rows_hash_->hash(), blocks_hash_->hash()};
    }

    // The most bytes held of the rows file (a row) and of the nodes file (the blocks read again
    // and the way decoded from them).
    size_t peak_rows() const { return peak_rows_; }
    size_t peak_nodes() const { return peak_nodes_; }

private:
    static constexpr size_t PIECE = gcs::ways::BLOCKS_BUF;
    // The groups passed last whose blocks nodes() finds without the cursor: a day's merge reads
    // old nodes of the ways just before the one it is at (a run of deleted ways, then the ways
    // that take their places).
    static constexpr size_t HISTORY_GROUPS = 256;
    // A group's blocks up to this many bytes are read again with one read and kept while its
    // ways are decoded; a larger group's a way at a time.
    static constexpr uint64_t RAW_GROUP_BYTES = 64 << 10;

    // What finds and decodes a group's blocks: the pair's nodes and ways before it, where its
    // blocks start, its anchor and its ways' counts and codes.
    struct Group {
        uint64_t node = 0, way = 0;
        uint32_t off = 0, anchor_lat = 0, anchor_lng = 0, ways = 0;
        uint16_t cnt[wf::GROUP_WAYS] = {}, code[wf::GROUP_WAYS] = {};
        uint64_t nodes() const {
            uint64_t n = 0;
            for (uint32_t j = 0; j < ways; j++) n += cnt[j];
            return n;
        }
        uint64_t bytes() const {
            uint64_t n = 0;
            for (uint32_t j = 0; j < ways; j++) n += wf::block_bytes(cnt[j], code[j]);
            return n;
        }
    };
    static Group group_of(const wf::Row& r, uint64_t node, uint64_t way) {
        Group g;
        g.node = node;
        g.way = way;
        g.off = r.off;
        g.anchor_lat = r.anchor_lat;
        g.anchor_lng = r.anchor_lng;
        g.ways = static_cast<uint32_t>(r.ways);
        std::copy(r.cnt.begin(), r.cnt.end(), g.cnt);
        std::copy(r.code.begin(), r.code.end(), g.code);
        return g;
    }

    void pass_blocks() {
        copy_blocks(group_left_, [](const uint8_t*, size_t) {});
    }
    // The row of the group that starts at way_, its blocks' place checked.
    void open_group() {
        pass_blocks();
        const uint64_t g = way_ / wf::GROUP_WAYS;
        const size_t n = f_.group_ways(g), bytes = kind_.row_bytes(n);
        const uint8_t* p = R_->take(bytes);
        rows_hash_->update(reinterpret_cast<const char*>(p), bytes);
        peak_rows_ = std::max(peak_rows_, bytes);
        row_ = wf::decode_row(kind_, p, n);
        if (row_.off != at_)
            wf::malformed(std::string(kind_.ways_file) + " group " + std::to_string(g) + " is not where the blocks before it end");
        uint64_t end = at_;
        for (size_t j = 0; j < n; j++) {
            const size_t block = row_.block_bytes(j);
            if (block > f_.blocks_bytes - end)
                wf::malformed(std::string(kind_.nodes_file) + " ends inside the block of way " + std::to_string(way_ + j));
            end += block;
        }
        group_node_ = node_;
        group_left_ = end - at_;
        at_ = end;
        ring_[g % HISTORY_GROUPS] = group_of(row_, node_, way_);
    }

    // The group that holds node a, and where it was found.
    const Group& find(uint64_t a, Source& from) {
        if (way_ > 0) {
            const uint64_t g = (way_ - 1) / wf::GROUP_WAYS, oldest = g >= HISTORY_GROUPS ? g - HISTORY_GROUPS + 1 : 0;
            const Group& at_hand = ring_[g % HISTORY_GROUPS];
            if (a >= at_hand.node) {
                if (a - at_hand.node < at_hand.nodes()) {
                    from = AT_HAND;
                    return at_hand;
                }
            } else if (a >= ring_[oldest % HISTORY_GROUPS].node) {
                // The last of the groups passed that starts at or before a.
                uint64_t lo = oldest, hi = g - 1;
                while (lo < hi) {
                    const uint64_t mid = lo + (hi - lo + 1) / 2;
                    if (ring_[mid % HISTORY_GROUPS].node <= a) lo = mid;
                    else hi = mid - 1;
                }
                from = PASSED;
                return ring_[lo % HISTORY_GROUPS];
            }
        }
        from = CURSOR;
        return cursor(a);
    }
    // The group that holds node a by a second pass over the rows, which reads on from the group
    // it found last: a merge's MATCH runs read the old nodes in order, so a is in that group or
    // after it.
    const Group& cursor(uint64_t a) {
        if (!C_) C_.reset(new gcs::Reader(R_->path, gcs::ways::ROWS_BUF, 0, f_.rows_bytes));
        while (!cursor_set_ || a - cursor_.node >= cursor_.nodes()) {
            if (cursor_next_ == f_.groups())
                wf::malformed(std::string(kind_.nodes_file) + " counts " + std::to_string(f_.nodes) + " nodes and its ways have " +
                              std::to_string(cursor_node_));
            const size_t n = f_.group_ways(cursor_next_), bytes = kind_.row_bytes(n);
            peak_rows_ = std::max(peak_rows_, bytes);
            cursor_ = group_of(wf::decode_row(kind_, C_->take(bytes), n), cursor_node_, cursor_next_ * wf::GROUP_WAYS);
            cursor_node_ += cursor_.nodes();
            cursor_next_++;
            cursor_set_ = true;
        }
        return cursor_;
    }

    // The nodes of g's way j, whose block starts at `block`, to out.
    void decode(const Group& g, size_t j, uint64_t block, uint8_t* out) {
        const size_t bytes = wf::block_bytes(g.cnt[j], g.code[j]);
        if (block > f_.blocks_bytes || bytes > f_.blocks_bytes - block)
            wf::malformed(std::string(kind_.nodes_file) + " ends inside the block of way " + std::to_string(g.way + j));
        const uint64_t group_bytes = g.bytes();
        const uint8_t* p;
        if (group_bytes <= RAW_GROUP_BYTES && group_bytes <= f_.blocks_bytes - std::min<uint64_t>(g.off, f_.blocks_bytes)) {
            if (!(raw_group_ && raw_off_ == g.off && raw_bytes_ == group_bytes)) {
                read_again(g.off, static_cast<size_t>(group_bytes));
                raw_group_ = true;
                raw_off_ = g.off;
                raw_bytes_ = group_bytes;
            }
            p = raw_.data() + (block - g.off);
        } else {
            raw_group_ = false;
            read_again(block, bytes);
            p = raw_.data();
        }
        wf::decode_block(p, g.cnt[j], g.code[j], g.anchor_lat, g.anchor_lng, out);
        peak_nodes_ = std::max(peak_nodes_, raw_.size() + wf::NODE_BYTES * size_t(g.cnt[j]));
    }
    // `bytes` of the nodes file from `at` into raw_, with the 8 bytes decode_block may load past
    // a block.
    void read_again(uint64_t at, size_t bytes) {
        raw_.resize(bytes + 8);
        if (bytes) gcs::pread_all(B_->fd, raw_.data(), bytes, at, B_->path);
        memset(raw_.data() + bytes, 0, 8);
    }

    const wf::Kind& kind_;
    wf::Footers f_;
    std::unique_ptr<gcs::Reader> R_, B_, C_;  // the rows, the blocks, and the cursor's rows
    std::unique_ptr<LaneHasher> rows_hash_, blocks_hash_;
    // The ways and nodes passed, and the bytes of the blocks of the groups opened.
    uint64_t way_ = 0, node_ = 0, at_ = 0;
    // The way at hand: its place in its group and the nodes before it; and its group.
    size_t j_ = 0;
    uint64_t before_ = 0, group_node_ = 0, group_left_ = 0;  // group_left_: its blocks' bytes not read yet
    wf::Row row_;
    std::vector<Group> ring_;  // group g at g % HISTORY_GROUPS
    Group cursor_;
    bool cursor_set_ = false;
    uint64_t cursor_next_ = 0, cursor_node_ = 0;  // the cursor's next group and the nodes before it
    // The blocks read again (a group's: raw_group_, from raw_off_, raw_bytes_ of them) and the
    // way kept decoded (the one whose first node is kept_first_).
    std::vector<uint8_t> raw_, kept_way_;
    bool raw_group_ = false, kept_ = false;
    uint64_t raw_off_ = 0, raw_bytes_ = 0, kept_first_ = 0;
    size_t peak_rows_ = 0, peak_nodes_ = 0;
};

struct Result {
    uint64_t ways_bytes = 0, nodes_bytes = 0;         // the new logical files'
    uint64_t copied = 0, encoded = 0;                 // the new groups
    uint64_t at_hand = 0, passed = 0, by_cursor = 0;  // old node bytes decoded, by where their blocks were
    // The nodes' section after the ways', its ops read in place as the ways take their nodes.
    ChildOps child;
    // The most bytes held of each old file (rows, nodes) and of each new logical file (a
    // group's records, a way's nodes).
    size_t decode_peaks[2] = {0, 0}, encode_peaks[2] = {0, 0};
    explicit Result(const wf::Kind& kind) : child(nodes_id(kind), wf::NODE_BYTES) {}
};

// Replays the merge sequence seq[0, seq_size) of the kind's ways' records and the nodes' section
// at P[child_at, patch_size) from the old pair in cur_dir, which holds ways, into a new one in
// out_dir. fix(rec, old_index, k_in_run, run_first, run_count) moves a MATCH record's string
// field and node_offset as the generic replay does. on_run(old_start, new_start, count) for
// each MATCH and REPLACE run (the record remap). The old files must hash to old_hashes.
template <typename Fix, typename OnRun>
Result replay(const wf::Kind& kind, const char* seq, size_t seq_size, const char* P, size_t patch_size, size_t child_at,
              const std::string& cur_dir, const std::string& out_dir, uint64_t old_size, uint64_t new_size,
              uint32_t file_records_max, const Hashes& old_hashes, Fix fix, OnRun on_run) {
    Result r(kind);
    const std::string ways_file = kind.ways_file, nodes_file = kind.nodes_file;
    OldWays old(cur_dir, kind);
    if (!old.ways()) throw std::logic_error("a way replay of an old pair without ways");
    if (old.ways() * kind.stride != old_size)
        gcs::die("old %s decodes to %llu bytes, the patch expects %llu", kind.ways_file,
                 static_cast<unsigned long long>(old.ways() * kind.stride), static_cast<unsigned long long>(old_size));
    ChildOps& child = r.child;
    child.parse(P, patch_size, child_at, file_records_max);
    if (child.old_size != old.nodes_bytes())
        gcs::die("old %s decodes to %llu bytes, the patch expects %llu", kind.nodes_file,
                 static_cast<unsigned long long>(old.nodes_bytes()), static_cast<unsigned long long>(child.old_size));
    gcs::ways::Enc enc(out_dir, kind);

    // The new group being gathered: its ways' records, n of them so far, of the new pair's ways
    // new_ways, of which `placed` are in groups.
    uint8_t group[wf::GROUP_WAYS * wf::MAX_STRIDE];
    size_t n = 0;
    const uint64_t new_ways = new_size / kind.stride;
    uint64_t placed = 0;
    std::vector<uint8_t> nodes;  // a way's nodes

    // The group's ways and their nodes into the new files. Its blocks are those of the old
    // group the replay is at, its first n ways' (a way past its last has none), when those have
    // its ways' node counts and the child's next bytes are one MATCH of those ways' nodes:
    // copied. (A group without nodes has no block. The child reads the old nodes in order, so
    // the blocks of a group it matches whole are unread.)
    auto flush = [&] {
        uint64_t group_nodes = 0;
        bool same = true;
        for (size_t j = 0; j < n; j++) {
            const uint16_t cnt = wf::get16(group + j * kind.stride + wf::NODE_COUNT_AT);
            group_nodes += cnt;
            same = same && cnt == old.row().cnt[j];
        }
        if (same && group_nodes) {
            child.at_bytes();
            same = child.op == OP_MATCH_RUN && child.old == old.group_node() * wf::NODE_BYTES &&
                   child.left >= group_nodes * wf::NODE_BYTES;
        }
        if (same && group_nodes) {
            wf::Row row = old.row();
            row.ways = n;
            enc.files.put_coded_group(row, group);
            old.copy_blocks(row.blocks_bytes(), [&](const uint8_t* p, size_t bytes) { enc.files.put_coded_bytes(p, bytes); });
            child.took(group_nodes * wf::NODE_BYTES);
            r.copied++;
        } else {
            for (size_t j = 0; j < n; j++) {
                const uint8_t* rec = group + j * kind.stride;
                const size_t bytes = wf::NODE_BYTES * size_t(wf::get16(rec + wf::NODE_COUNT_AT));
                if (nodes.size() < bytes) nodes.resize(bytes);
                // The way's nodes from the child: MATCH bytes decoded from the old pair, the
                // others the patch's.
                for (size_t have = 0; have < bytes;) {
                    child.at_bytes();
                    const uint64_t k = std::min<uint64_t>(bytes - have, child.left);
                    if (child.op != OP_MATCH_RUN) {
                        memcpy(nodes.data() + have, P + child.data, static_cast<size_t>(k));
                        child.took(k);
                        have += static_cast<size_t>(k);
                        continue;
                    }
                    if (child.old % wf::NODE_BYTES || k % wf::NODE_BYTES || k > child.old_size || child.old > child.old_size - k)
                        throw std::runtime_error("Malformed merge sequence: " + nodes_file);
                    OldWays::Source from;
                    const size_t got = old.nodes(child.old, k, nodes.data() + have, from);
                    (from == OldWays::AT_HAND ? r.at_hand : from == OldWays::PASSED ? r.passed : r.by_cursor) += got;
                    child.took(got);
                    have += got;
                }
                enc.files.add(rec, nodes.data());
                r.encode_peaks[1] = std::max(r.encode_peaks[1], bytes);
            }
            r.encoded++;
        }
        r.encode_peaks[0] = std::max(r.encode_peaks[0], n * kind.stride);
        r.ways_bytes += n * kind.stride;
        n = 0;
    };
    // A new way's record: its group goes out with its last way.
    auto put = [&](const uint8_t* rec) {
        if (placed == new_ways) throw std::runtime_error("Rebuilt " + ways_file + " does not match the new build");
        memcpy(group + n * kind.stride, rec, kind.stride);
        n++;
        placed++;
        if (n == wf::GROUP_WAYS || placed == new_ways) flush();
    };

    uint64_t old_rec = 0, new_rec = 0;
    uint8_t rec[wf::MAX_STRIDE];
    size_t pos = 0;
    while (pos < seq_size) {
        if (pos + 5 > seq_size) throw std::runtime_error("Malformed merge sequence: " + ways_file);
        const uint8_t op = static_cast<uint8_t>(seq[pos++]);
        uint32_t count;
        memcpy(&count, seq + pos, 4);
        pos += 4;
        if ((op == OP_MATCH_RUN || op == OP_DELETE_RUN || op == OP_REPLACE_RUN) && count > old.ways() - old_rec)
            throw std::runtime_error("Malformed merge sequence: " + ways_file + " (a " +
                                     (op == OP_MATCH_RUN ? "MATCH" : op == OP_DELETE_RUN ? "DELETE" : "REPLACE") +
                                     " run past the old file)");
        if (op == OP_MATCH_RUN) {
            for (uint32_t k = 0; k < count; k++) {
                old.next();
                old.record(rec);
                fix(rec, old_rec + k, k, old_rec, count);
                put(rec);
            }
            on_run(old_rec, new_rec, count);
            old_rec += count;
            new_rec += count;
        } else if (op == OP_INSERT_RUN || op == OP_REPLACE_RUN) {
            const auto run = checked_size(0, count, kind.stride, seq_size - pos);
            if (!run) throw std::runtime_error("Malformed merge sequence: " + ways_file);
            for (uint32_t k = 0; k < count; k++) {
                if (op == OP_REPLACE_RUN) old.next();
                put(reinterpret_cast<const uint8_t*>(seq + pos + size_t(k) * kind.stride));
            }
            if (op == OP_REPLACE_RUN) {
                on_run(old_rec, new_rec, count);
                old_rec += count;
            }
            new_rec += count;
            pos += *run;
        } else if (op == OP_DELETE_RUN) {
            for (uint32_t k = 0; k < count; k++) old.next();
            old_rec += count;
        } else {
            throw std::runtime_error("Malformed merge sequence: " + ways_file);
        }
    }
    // The old files first: a patch does not fit a pair it was not made from.
    const Hashes hashes = old.finish();
    if (hashes.ways != old_hashes.ways) throw std::runtime_error("Old " + ways_file + " is not the one the patch was made from");
    if (hashes.nodes != old_hashes.nodes) throw std::runtime_error("Old " + nodes_file + " is not the one the patch was made from");
    if (placed != new_ways || r.ways_bytes != new_size) throw std::runtime_error("Rebuilt " + ways_file + " does not match the new build");
    child.finish();
    if (child.written != child.new_size) throw std::runtime_error("Rebuilt " + nodes_file + " does not match the new build");
    r.nodes_bytes = child.written;
    enc.finish();
    r.decode_peaks[0] = old.peak_rows();
    r.decode_peaks[1] = old.peak_nodes();
    return r;
}

}  // namespace way_replay
