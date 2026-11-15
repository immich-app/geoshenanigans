// The section of an encoded family's child stream (a way pair's nodes, the address footprints, the
// POI vertices), which follows its parent records' section in the patch: a merge sequence of runs,
// a copy of the old stream, or the new one whole. A replay of the family's files as they sit
// (poi_replay.h) reads its ops in place as the records take their bytes.
#pragma once

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

#include "merge_sequence.h"
#include "patch_format.h"

// The child's file and the bytes a run of its merge section counts by (a node's 8, a vertex
// stream's 1). After parse: op and left of the run at hand, in bytes; old the old stream's offset
// there; data the patch's offset of an INSERT's or a REPLACE's bytes; written the new stream's
// bytes taken so far. A copy of the old stream reads as one MATCH of it, a new stream sent whole
// as one INSERT.
struct ChildOps {
    enum Kind { MERGE, COPY_OLD, FULL };
    uint32_t fid;
    std::string name;
    uint32_t stride;
    Kind kind = MERGE;
    const char* P = nullptr;
    uint64_t old_size = 0, new_size = 0;
    // seq, seq_end: a merge's sequence in the patch. section_end: where the next section starts.
    size_t q = 0, seq = 0, seq_end = 0, section_end = 0, data = 0;
    uint8_t op = 0xFF;
    uint64_t left = 0, old = 0, written = 0;
    bool started = false;

    ChildOps(PatchFileId file, uint32_t run_stride)
        : fid(static_cast<uint32_t>(file)), name(patch_file_names[static_cast<uint32_t>(file)]), stride(run_stride) {}

    // Reads the section at p[at, size): refused unless it is the child's, a merge at its stride
    // without fixups, a copy or a full replacement.
    void parse(const char* p, size_t size, size_t at, uint32_t file_records_max) {
        P = p;
        SectionReader in(P, size, at, "patch: child section");
        if (in.u32() != fid) throw std::runtime_error("Encoded family: " + name + " does not follow its parent in the patch");
        const uint32_t section_stride = in.u32();
        old_size = in.u64();
        new_size = in.u64();
        if (section_stride == COPY_OLD_STRIDE || section_stride == 0) {
            in.u32();
            const uint64_t ds = in.u64();
            kind = section_stride == 0 ? FULL : COPY_OLD;
            data = at;
            in.take(ds);
            if (kind == FULL && ds != new_size) throw std::runtime_error("Malformed child full replacement");
        } else if (section_stride != stride) {
            throw std::runtime_error("Encoded family: unexpected section kind for a child stream");
        } else {
            require_merge_records(name, old_size, new_size, stride, file_records_max);
            const uint32_t n_runs = in.u32(), n_values = in.u32(), runs_size = in.u32(), values_size = in.u32();
            if (n_runs || n_values || runs_size || values_size)
                throw std::runtime_error("Encoded family: a child stream with offset fixups");
            const uint32_t n_string_fixups = in.u32(), string_fixups_size = in.u32();
            if (n_string_fixups || string_fixups_size) throw std::runtime_error("Encoded family: a child stream with string fixups");
            const uint64_t ss = in.u64();
            seq = q = at;
            in.take(ss);
            seq_end = at;
        }
        section_end = at;
    }
    // Moves to the next op that writes bytes, skipping DELETE runs; false at the end.
    bool fetch() {
        if (kind != MERGE) {
            if (started) return false;
            started = true;
            op = kind == FULL ? OP_INSERT_RUN : OP_MATCH_RUN;
            left = kind == FULL ? new_size : old_size;
            return left > 0;
        }
        for (;;) {
            if (q >= seq_end) return false;
            if (q + 5 > seq_end) throw std::runtime_error("Malformed merge sequence: " + name);
            const uint8_t o = uint8_t(P[q++]);
            uint32_t count;
            memcpy(&count, P + q, 4);
            q += 4;
            const uint64_t bytes = uint64_t(count) * stride;
            if (o == OP_DELETE_RUN) {
                old += bytes;
                continue;
            }
            if (bytes == 0) continue;
            if (o == OP_MATCH_RUN) {
                op = o;
                left = bytes;
                return true;
            }
            if (o == OP_INSERT_RUN || o == OP_REPLACE_RUN) {
                if (bytes > seq_end - q) throw std::runtime_error("Malformed merge sequence: " + name);
                op = o;
                data = q;
                q += bytes;
                left = bytes;
                return true;
            }
            throw std::runtime_error("Malformed merge sequence: " + name);
        }
    }
    // The op the next byte comes from.
    void at_bytes() {
        while (left == 0)
            if (!fetch()) throw std::runtime_error(name + " ends before its records do");
    }
    // Takes k bytes (k <= left).
    void took(uint64_t k) {
        if (op != OP_INSERT_RUN) old += k;
        if (op != OP_MATCH_RUN) data += k;
        left -= k;
        written += k;
    }
    void finish() {
        while (left == 0 && fetch()) {}
        if (left) throw std::runtime_error(name + " has bytes past its last record");
    }
};
