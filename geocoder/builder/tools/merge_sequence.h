// Merge sequences: how geocoder-diff tells geocoder-patch to rebuild a file
// from its old copy. MATCH(n) copies n records from old, INSERT(n, data)
// appends n new records, DELETE(n) skips n old records.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

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
