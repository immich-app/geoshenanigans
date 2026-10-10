// Merge sequences: how geocoder-diff tells geocoder-patch to rebuild a file
// from its old copy. MATCH(n) copies n records from old, INSERT(n, data)
// appends n new records, DELETE(n) skips n old records.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <vector>

#include "parallel.h"

enum MergeOp : uint8_t { OP_MATCH_RUN = 0, OP_INSERT_RUN = 1, OP_DELETE_RUN = 2 };

struct MergeSequence {
    std::vector<char> data; // serialized ops

    void add_match(uint32_t count) {
        uint8_t op = OP_MATCH_RUN;
        data.insert(data.end(), (char*)&op, (char*)&op + 1);
        data.insert(data.end(), (char*)&count, (char*)&count + 4);
    }
    void add_delete(uint32_t count) {
        uint8_t op = OP_DELETE_RUN;
        data.insert(data.end(), (char*)&op, (char*)&op + 1);
        data.insert(data.end(), (char*)&count, (char*)&count + 4);
    }
    void add_insert(const char* records, uint32_t count, size_t stride) {
        uint8_t op = OP_INSERT_RUN;
        data.insert(data.end(), (char*)&op, (char*)&op + 1);
        data.insert(data.end(), (char*)&count, (char*)&count + 4);
        data.insert(data.end(), records, records + count * stride);
    }
};

// The non-zero records of a file by content hash, for build_merge_seq's jumps:
// (hash << 32 | index) sorted, 8 bytes a record where an unordered_multimap
// cost ~40 (7 GB for planet addr_points), plus a directory of where each hash
// prefix starts, so a lookup touches about as few cache lines as a hash map.
// A candidate is confirmed by its bytes, so any hash gives the same answers.
class RecordJumpIndex {
public:
    RecordJumpIndex(const char* data, size_t n, size_t stride, unsigned threads = 0)
        : data_(data), stride_(stride) {
        // Zeroed records (tombstone slots) are left out: see build_merge_seq.
        constexpr uint64_t ZERO = UINT64_MAX;  // above every entry: indices stay below UINT32_MAX
        entries_.resize(n);
        parallel_for(n, [&](size_t b, size_t e, unsigned) {
            for (size_t i = b; i < e; i++) {
                const char* p = data + i * stride;
                bool zero = true;
                for (size_t k = 0; k < stride && zero; k++) zero = p[k] == 0;
                entries_[i] = zero ? ZERO : (uint64_t)hash(p, stride) << 32 | i;
            }
        }, threads);
        parallel_sort(entries_.begin(), entries_.end(), std::less<uint64_t>(), threads);
        entries_.erase(std::lower_bound(entries_.begin(), entries_.end(), ZERO), entries_.end());
        entries_.shrink_to_fit();

        while (bits_ < 28 && (size_t(8) << bits_) < entries_.size()) bits_++;
        dir_.assign((size_t(1) << bits_) + 1, 0);
        size_t at = 0;
        for (size_t p = 0; p < dir_.size(); p++) {
            while (at < entries_.size() && prefix(static_cast<uint32_t>(entries_[at] >> 32)) < p) at++;
            dir_[p] = static_cast<uint32_t>(at);
        }
    }

    // The smallest index in [lo, hi) whose record equals rec, or UINT32_MAX.
    uint32_t first_equal(const char* rec, size_t lo, size_t hi) const {
        if (entries_.empty()) return UINT32_MAX;
        uint32_t h = hash(rec, stride_);
        size_t p = prefix(h);
        auto it = std::lower_bound(entries_.begin() + dir_[p], entries_.begin() + dir_[p + 1], (uint64_t)h << 32 | lo);
        for (auto end = entries_.begin() + dir_[p + 1]; it != end && (*it >> 32) == h; ++it) {
            size_t idx = static_cast<uint32_t>(*it);
            if (idx >= hi) break;
            if (memcmp(rec, data_ + idx * stride_, stride_) == 0) return static_cast<uint32_t>(idx);
        }
        return UINT32_MAX;
    }

private:
    static uint32_t hash(const char* p, size_t stride) {
        uint64_t h = 14695981039346656037ULL;
        for (size_t i = 0; i < stride; i++) { h ^= (uint8_t)p[i]; h *= 1099511628211ULL; }
        return static_cast<uint32_t>(h ^ (h >> 32));
    }
    size_t prefix(uint32_t h) const { return bits_ ? h >> (32 - bits_) : 0; }

    const char* data_;
    size_t stride_;
    std::vector<uint64_t> entries_;
    std::vector<uint32_t> dir_;
    unsigned bits_ = 0;
};

// Record merge of one data file: walk old (string remap already applied) and
// new in parallel, comparing records by their stride bytes.
inline MergeSequence build_merge_seq(
    const char* old_data, size_t old_size, const char* new_data, size_t new_size,
    size_t stride)
{
    size_t old_n = old_size / stride;
    size_t new_n = new_size / stride;
    size_t oi = 0, ni = 0;
    MergeSequence seq;
    uint32_t match_run = 0, del_run = 0;

    auto flush_match = [&]() { if (match_run > 0) { seq.add_match(match_run); match_run = 0; } };
    auto flush_del = [&]() { if (del_run > 0) { seq.add_delete(del_run); del_run = 0; } };

    // For small strides (<=8), records aren't unique enough for hash matching.
    // Use simple sequential scan instead.
    bool use_hash = (stride > 8);

    // Pre-build hash index of new records for fast mismatch resolution.
    // Zeroed records (tombstone slots) are excluded from the jump index:
    // every old tombstone hash-matches every still-empty new slot, and a
    // jump to one re-anchors the cursor at the wrong position — following
    // records' true counterparts fall behind the forward-only window and
    // each old tombstone triggers a self-healing DELETE+INSERT episode of
    // unchanged records (2.9 GB patch on a chained planet pair whose old
    // side carried 39K tombstones; fresh old sides have none, which hid
    // this). Dead slots need no anchoring — they replay fine as plain
    // DELETE/INSERT.
    RecordJumpIndex new_index(new_data, use_hash ? new_n : 0, stride);

    while (oi < old_n && ni < new_n) {
        const char* op = old_data + oi * stride;
        const char* np = new_data + ni * stride;

        if (memcmp(op, np, stride) == 0) {
            flush_del();
            match_run++;
            oi++; ni++;
        } else if (use_hash) {
            uint32_t best_ni = new_index.first_equal(op, ni, ni + 10000);
            // Overshoot guard: with duplicate record content the smallest
            // in-window match can be a LATER duplicate than this record's
            // true counterpart (e.g. when the counterpart was already
            // passed). Re-anchoring on it starts a cascade: following
            // records' true matches fall behind the forward-only window
            // and drop to DELETE one by one until the cursor self-heals —
            // tens of millions of unchanged records re-serialized on a
            // chained planet pair. Only accept a far jump if the NEXT old
            // record also finds its counterpart shortly after the target
            // (tolerant to a few interleaved inserts); otherwise emit a
            // single DELETE, which costs one record instead of an episode.
            auto next_aligns = [&](uint32_t cand) -> bool {
                if (oi + 1 >= old_n) return true;
                const char* onext = old_data + (oi + 1) * stride;
                size_t lim = std::min((size_t)cand + 1 + 16, new_n);
                for (size_t j = cand + 1; j < lim; j++)
                    if (memcmp(onext, new_data + j * stride, stride) == 0) return true;
                return false;
            };
            // A far jump re-sends every record it skips, so it has to land
            // on a real run: the following old records must match one for
            // one after the target. Two duplicates in a row (a pair of POIs
            // that changed slots) satisfied next_aligns and re-sent 4172
            // unchanged records of an oceania POI tier.
            constexpr size_t FAR_JUMP = 16;
            auto run_follows = [&](uint32_t cand) -> bool {
                if (cand - ni <= FAR_JUMP) return true;
                for (size_t j = 1; j < FAR_JUMP && oi + j < old_n && cand + j < new_n; j++)
                    if (memcmp(old_data + (oi + j) * stride, new_data + (cand + j) * stride, stride) != 0) return false;
                return true;
            };
            if (best_ni != UINT32_MAX && best_ni > ni && next_aligns(best_ni) && run_follows(best_ni)) {
                flush_match(); flush_del();
                seq.add_insert(new_data + ni * stride, best_ni - ni, stride);
                ni = best_ni;
            } else if (best_ni == ni) {
                flush_del(); match_run++; oi++; ni++;
            } else {
                flush_match(); del_run++; oi++;
            }
        } else {
            size_t lookahead = std::min((size_t)200, std::min(old_n - oi, new_n - ni));
            bool found = false;
            for (size_t k = 1; k <= lookahead; k++) {
                if (ni + k < new_n && memcmp(op, new_data + (ni + k) * stride, stride) == 0) {
                    flush_match(); flush_del();
                    seq.add_insert(new_data + ni * stride, k, stride);
                    ni += k; found = true; break;
                }
                if (oi + k < old_n && memcmp(np, old_data + (oi + k) * stride, stride) == 0) {
                    flush_match(); del_run += k; oi += k; found = true; break;
                }
            }
            if (!found) { flush_match(); del_run++; oi++; flush_del(); seq.add_insert(np, 1, stride); ni++; }
        }
    }

    flush_match(); flush_del();
    if (oi < old_n) seq.add_delete(old_n - oi);
    if (ni < new_n) seq.add_insert(new_data + ni * stride, new_n - ni, stride);

    return seq;
}

// Each parent record's block of a v15 vertex byte stream: a block runs from
// its record's vertex offset to the next record's that has one, or to the
// end of the stream; a NO_DATA or out-of-range offset owns no block (POIs
// can be points between polygon POIs), nor does one past its successor.
struct VertexBlocks {
    std::vector<uint32_t> offsets, sizes;
};

inline VertexBlocks vertex_blocks(const char* parent, size_t parent_size, size_t stride, size_t off_field_pos,
                                  size_t verts_size, unsigned threads = 0) {
    constexpr uint32_t NO_DATA = 0xFFFFFFFFu;
    size_t total_n = parent_size / stride;
    VertexBlocks b{std::vector<uint32_t>(total_n), std::vector<uint32_t>(total_n)};
    auto owns_block = [&](uint32_t off) { return off != NO_DATA && (size_t)off <= verts_size; };
    // Chunks walk from their end so each i finds its next non-NO_DATA
    // neighbour cheaply, starting from the first one after the chunk.
    size_t chunks = std::max<size_t>(1, std::min<size_t>(total_n, threads ? threads : parallel_threads()));
    auto chunk_begin = [&](size_t c) { return total_n * c / chunks; };
    std::vector<uint32_t> first_owner(chunks, NO_DATA), next_after(chunks);
    parallel_for(chunks, [&](size_t cb, size_t ce, unsigned) {
        for (size_t c = cb; c < ce; c++) {
            for (size_t i = chunk_begin(c); i < chunk_begin(c + 1); i++) {
                memcpy(&b.offsets[i], parent + i * stride + off_field_pos, 4);
                if (first_owner[c] == NO_DATA && owns_block(b.offsets[i])) first_owner[c] = b.offsets[i];
            }
        }
    }, threads);
    next_after[chunks - 1] = static_cast<uint32_t>(verts_size);
    for (size_t c = chunks - 1; c-- > 0;)
        next_after[c] = first_owner[c + 1] != NO_DATA ? first_owner[c + 1] : next_after[c + 1];
    parallel_for(chunks, [&](size_t cb, size_t ce, unsigned) {
        for (size_t c = cb; c < ce; c++) {
            uint32_t next_off = next_after[c];
            for (size_t i = chunk_begin(c + 1); i-- > chunk_begin(c);) {
                uint32_t off = b.offsets[i];
                if (!owns_block(off)) {
                    b.sizes[i] = 0;
                } else {
                    b.sizes[i] = (next_off >= off) ? (next_off - off) : 0;
                    next_off = off;
                }
            }
        }
    }, threads);
    return b;
}

// A child stream (street/interp nodes, *_vertices bytes) merged along its
// parent's merge sequence: a parent MATCH keeps its child block when the
// bytes are equal, a DELETE drops the old block and an INSERT appends the
// new one. old_block(i) / new_block(i) give parent i's block; ops count
// `unit`-byte child records.
struct ChildBlock { size_t off, size; };

template <typename OldBlock, typename NewBlock>
MergeSequence merge_child_blocks(const MergeSequence& parent_seq, size_t parent_stride, size_t unit,
                                 const char* old_child, size_t old_child_size,
                                 const char* new_child, size_t new_child_size,
                                 OldBlock old_block, NewBlock new_block) {
    // Coalesce adjacent same-type ops. The patch tool replays the child
    // stream sequentially (MATCH n = copy n old units, DELETE n = drop n
    // old, INSERT = append), so merged runs give identical output; one op
    // per parent record cost tens of MiB of opcodes on planet even when
    // nothing changed. Deletes and inserts within a non-match stretch
    // group as one DELETE then one INSERT (deletes only advance the old
    // cursor, inserts append contiguous new bytes).
    MergeSequence seq;
    uint32_t m_run = 0, d_run = 0;
    const char* ins_ptr = nullptr; uint32_t ins_cnt = 0;  // contiguous span in new_child (units)
    auto flush_match = [&]{ if (m_run) { seq.add_match(m_run); m_run = 0; } };
    auto flush_del   = [&]{ if (d_run) { seq.add_delete(d_run); d_run = 0; } };
    auto flush_ins   = [&]{ if (ins_cnt) { seq.add_insert(ins_ptr, ins_cnt, unit); ins_cnt = 0; ins_ptr = nullptr; } };
    auto emit_match  = [&](size_t bytes){ flush_del(); flush_ins(); m_run += static_cast<uint32_t>(bytes / unit); };
    auto emit_del    = [&](size_t bytes){ flush_match(); d_run += static_cast<uint32_t>(bytes / unit); };
    auto emit_ins    = [&](const ChildBlock& b){
        flush_match();
        const char* p = new_child + b.off;
        uint32_t n = static_cast<uint32_t>(b.size / unit);
        if (ins_cnt && ins_ptr + (size_t)ins_cnt * unit == p) ins_cnt += n;
        else { flush_ins(); ins_ptr = p; ins_cnt = n; }
    };
    auto in_old = [&](const ChildBlock& b) { return b.off + b.size <= old_child_size; };
    auto in_new = [&](const ChildBlock& b) { return b.off + b.size <= new_child_size; };
    auto drop = [&](const ChildBlock& ob) { if (ob.size > 0) emit_del(ob.size); };
    auto append = [&](const ChildBlock& nb) { if (nb.size > 0 && in_new(nb)) emit_ins(nb); };
    auto keep_or_replace = [&](const ChildBlock& ob, const ChildBlock& nb) {
        bool same = ob.size == nb.size && in_old(ob) && in_new(nb)
                    && (ob.size == 0 || memcmp(old_child + ob.off, new_child + nb.off, ob.size) == 0);
        if (same) {
            if (ob.size > 0) emit_match(ob.size);
        } else if (ob.size > 0 && nb.size > 0 && in_old(ob) && in_new(nb)) {
            // An edited block (one moved vertex in a long boundary ring, a
            // node added mid-way) keeps its unchanged head and tail in
            // whole units; only the middle travels.
            const char* o = old_child + ob.off;
            const char* n = new_child + nb.off;
            size_t lim = std::min(ob.size, nb.size);
            size_t head = 0;
            while (head < lim && o[head] == n[head]) head++;
            head -= head % unit;
            size_t tail = 0;
            while (tail < lim - head && o[ob.size - 1 - tail] == n[nb.size - 1 - tail]) tail++;
            tail -= tail % unit;
            if (head > 0) emit_match(head);
            if (ob.size > head + tail) emit_del(ob.size - head - tail);
            if (nb.size > head + tail) emit_ins({nb.off + head, nb.size - head - tail});
            if (tail > 0) emit_match(tail);
        } else {
            drop(ob);
            append(nb);
        }
    };

    size_t oi = 0, ni = 0, pos = 0;
    while (pos < parent_seq.data.size()) {
        uint8_t op = static_cast<uint8_t>(parent_seq.data[pos]); pos++;
        uint32_t count; memcpy(&count, parent_seq.data.data() + pos, 4); pos += 4;
        if (op == OP_MATCH_RUN) {
            for (uint32_t k = 0; k < count; k++) keep_or_replace(old_block(oi + k), new_block(ni + k));
            oi += count; ni += count;
        } else if (op == OP_INSERT_RUN) {
            for (uint32_t k = 0; k < count; k++) append(new_block(ni + k));
            pos += (size_t)count * parent_stride;  // skip the inline parent records
            ni += count;
        } else if (op == OP_DELETE_RUN) {
            // A replaced record (DELETE then INSERT) mostly keeps its
            // geometry (a renamed road, a re-tagged POI): pair the two runs
            // positionally so an unchanged block stays a MATCH instead of
            // being re-sent.
            uint32_t ins = 0;
            if (pos < parent_seq.data.size() && static_cast<uint8_t>(parent_seq.data[pos]) == OP_INSERT_RUN)
                memcpy(&ins, parent_seq.data.data() + pos + 1, 4);
            uint32_t paired = std::min(count, ins);
            for (uint32_t k = 0; k < paired; k++) keep_or_replace(old_block(oi + k), new_block(ni + k));
            for (uint32_t k = paired; k < count; k++) drop(old_block(oi + k));
            oi += count;
            if (ins > 0) {
                for (uint32_t k = paired; k < ins; k++) append(new_block(ni + k));
                pos += 5 + (size_t)ins * parent_stride;  // the INSERT op and its inline records
                ni += ins;
            }
        }
    }
    flush_match(); flush_del(); flush_ins();
    return seq;
}
