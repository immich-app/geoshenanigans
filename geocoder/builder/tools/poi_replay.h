// The patcher's replay of the POI files on R2 and V2 as they sit (poi_files.h): the records' merge
// sequence and the vertex section after it, as the joint replay of today's logical records and
// vertex stream would apply them, but without decoding and encoding again every block a daily
// patch leaves alone. A V2 block is a function of its polygon's logical bytes and its record's
// lat and lng alone (its bbox minimum is stored against them), so a new record whose polygon is,
// byte for byte, the old record's (its vertex bytes one MATCH of that old block's) at the same
// lat and lng gets that very block: copied as it is. Every other polygon goes through the codec
// (gcs::poi::Enc), its old bytes from a cursor that decodes only the blocks it is asked for
// (gcs::poi::Dec, vertices_only). Rows are decoded and encoded again one by one (their string
// ids move every day). A copied block is never decoded: the old poi_vertices.bin is hashed as the
// replay passes it and checked against the diff's hash (POI_VERTICES_HASH_MARKER).
#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>

#include "gcs_codec.h"
#include "patch_format.h"

namespace poi_replay {

// The old R2 and V2 files front to back, a row and its block at a time: the row's logical record
// (vertex_offset its block's offset in the logical vertex stream, vertex_count its vertices) and
// its block, parsed but not read until copied or passed. Every V2 byte is hashed in order.
class OldRows {
public:
    explicit OldRows(const std::string& dir) {
        const std::string rp = dir + "/poi_records.bin", vp = dir + "/poi_vertices.bin";
        rows_ = gcs::poi::open_rows(rp, gcs::file_size(rp));
        R_.reset(new gcs::Reader(rp, 32 << 10, 0, rows_.footer().rows_end()));
        V_.reset(new gcs::Reader(vp));
        if (V_->size() < poi_files::VERTEX_PAD) poi_files::malformed("poi_vertices.bin is shorter than its pad");
        blocks_end_ = V_->size() - poi_files::VERTEX_PAD;
        hash_.reset(new LaneHasher(V_->size()));
    }
    uint64_t rows() const { return rows_.footer().rows; }
    uint64_t logical_vertex_bytes() const { return rows_.footer().logical_vertex_bytes; }
    uint64_t at() const { return i_; }
    // The next row (the block before it passed when it wasn't copied).
    void next() {
        pass_block();
        if (i_ == rows()) poi_files::malformed("a row past the last");
        bool polygon;
        const uint32_t voff = rows_.decode(R_->take(poi_files::ROW_BYTES), rec_, polygon);
        if (voff != V_->tell())
            poi_files::malformed("row " + std::to_string(i_) + ": voff " + std::to_string(voff) + " where the blocks are at " +
                                 std::to_string(V_->tell()));
        i_++;
        block_ = polygon;
        if (!polygon) return;
        const uint64_t left = blocks_end_ - std::min(V_->tell(), blocks_end_);
        const uint8_t* h = V_->need(poi_files::MAX_BLOCK_HEADER, true);
        blk_ = poi_files::read_block_header(h, size_t(std::min<uint64_t>(left, poi_files::MAX_BLOCK_HEADER)),
                                            poi_files::rd32(rec_), poi_files::rd32(rec_ + 4));
        if (blk_.bytes() > left) poi_files::malformed("row " + std::to_string(i_ - 1) + ": a block past the end of poi_vertices.bin");
        if (blk_.logical_bytes() > logical_vertex_bytes() - logical_) poi_files::past_vertex_stream(i_ - 1, rows_.footer());
        if (logical_ >= poi_files::NO) poi_files::malformed("a logical vertex stream of 4 GiB or more");
        poi_files::wr32(rec_ + poi_files::OFF_VERTEX_OFFSET, uint32_t(logical_));
        poi_files::wr32(rec_ + poi_files::OFF_VERTEX_COUNT, uint32_t(blk_.vertex_count()));
        block_logical_ = logical_;
        logical_ += blk_.logical_bytes();
        block_left_ = blk_.bytes();
    }
    // The row's logical record, its block's (if block()) offset and bytes in the logical stream.
    const uint8_t* rec() const { return rec_; }
    bool block() const { return block_; }
    uint64_t block_logical() const { return block_logical_; }
    uint64_t block_logical_bytes() const { return blk_.logical_bytes(); }
    // The row's block's bytes through out(p, n), a piece at a time, as they are.
    template <typename Out>
    void copy_block(Out out) {
        while (block_left_) {
            const size_t n = size_t(std::min<uint64_t>(block_left_, PIECE));
            const uint8_t* p = V_->take(n);
            hash_->update(reinterpret_cast<const char*>(p), n);
            out(p, n);
            block_left_ -= n;
        }
    }
    void pass_block() {
        copy_block([](const uint8_t*, size_t) {});
    }
    // Passes the rows left, checks the files end as the encoder ends them, and returns the V2 file's
    // hash (lane_hash).
    uint64_t finish() {
        while (i_ < rows()) next();
        pass_block();
        rows_.finish();
        if (V_->tell() != blocks_end_) poi_files::malformed("poi_vertices.bin has bytes past its last block");
        const uint8_t* pad = V_->take(poi_files::VERTEX_PAD);
        for (size_t k = 0; k < poi_files::VERTEX_PAD; k++)
            if (pad[k]) poi_files::malformed("poi_vertices.bin's pad is not zero");
        hash_->update(reinterpret_cast<const char*>(pad), poi_files::VERTEX_PAD);
        if (logical_ != logical_vertex_bytes())
            poi_files::malformed("the vertex stream is " + std::to_string(logical_) + " bytes, the footer gives " +
                                 std::to_string(logical_vertex_bytes()));
        return hash_->hash();
    }

private:
    static constexpr size_t PIECE = 64 << 10;
    poi_files::RowDecoder rows_;
    std::unique_ptr<gcs::Reader> R_, V_;
    std::unique_ptr<LaneHasher> hash_;
    uint64_t i_ = 0, logical_ = 0, blocks_end_ = 0, block_logical_ = 0, block_left_ = 0;
    uint8_t rec_[poi_files::LOGICAL_STRIDE] = {};
    bool block_ = false;
    poi_files::Block blk_;
};

// The vertex section after the records' (a merge sequence of byte runs, a copy of the old stream or
// the new one whole), its ops read in place as the records take their bytes: op and left of the
// run at hand, old the old stream's offset there, data the patch's for INSERT and REPLACE bytes.
struct ChildOps {
    enum Kind { MERGE, COPY_OLD, FULL };
    Kind kind = MERGE;
    const char* P = nullptr;
    uint64_t old_size = 0, new_size = 0;
    size_t q = 0, seq = 0, seq_end = 0, section_end = 0, data = 0;
    uint8_t op = 0xFF;
    uint64_t left = 0, old = 0, written = 0;
    bool started = false;

    void parse(const char* p, size_t size, size_t at, uint32_t file_records_max) {
        P = p;
        SectionReader in(P, size, at, "patch: child section");
        if (in.u32() != uint32_t(PatchFileId::POI_VERTICES))
            throw std::runtime_error("Encoded family: poi_vertices.bin does not follow its parent in the patch");
        const uint32_t stride = in.u32();
        old_size = in.u64();
        new_size = in.u64();
        if (stride == COPY_OLD_STRIDE || stride == 0) {
            in.u32();
            const uint64_t ds = in.u64();
            kind = stride == 0 ? FULL : COPY_OLD;
            data = at;
            in.take(ds);
            if (kind == FULL && ds != new_size) throw std::runtime_error("Malformed child full replacement");
        } else if (stride != 1) {
            throw std::runtime_error("Encoded family: unexpected section kind for a child stream");
        } else {
            require_merge_records("poi_vertices.bin", old_size, new_size, stride, file_records_max);
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
            if (q + 5 > seq_end) throw std::runtime_error("Malformed merge sequence: poi_vertices.bin");
            const uint8_t o = uint8_t(P[q++]);
            uint32_t count;
            memcpy(&count, P + q, 4);
            q += 4;
            if (o == OP_DELETE_RUN) {
                old += count;
                continue;
            }
            if (count == 0) continue;
            if (o == OP_MATCH_RUN) {
                op = o;
                left = count;
                return true;
            }
            if (o == OP_INSERT_RUN || o == OP_REPLACE_RUN) {
                if (count > seq_end - q) throw std::runtime_error("Malformed merge sequence: poi_vertices.bin");
                op = o;
                data = q;
                q += count;
                left = count;
                return true;
            }
            throw std::runtime_error("Malformed merge sequence: poi_vertices.bin");
        }
    }
    // The op the next byte comes from.
    void at_bytes() {
        while (left == 0)
            if (!fetch()) throw std::runtime_error("poi_vertices.bin ends before its records do");
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
        if (left) throw std::runtime_error("poi_vertices.bin has bytes past its last record");
    }
};

struct Result {
    uint64_t records_bytes = 0, vertex_bytes = 0;  // the new logical files'
    uint64_t copied = 0, encoded = 0;              // the new polygons' blocks
    ChildOps child;
    // The codecs' queue peaks per port: the vertex cursor's and the encoder's.
    size_t decode_peaks[2] = {0, 0}, encode_peaks[2] = {0, 0};
};

// Replays the POI records' merge sequence P[pos, seq_end) (stride 36) and the vertex section at
// seq_end from the old files in cur_dir into new ones in out_dir. fix(rec, old_index, k_in_run, run)
// moves a MATCH record's string fields and vertex_offset as the generic replay does (run: the
// run's first old index and count). on_run(old_start, new_start, count) for each MATCH and
// REPLACE run (the record remap). The old V2 file must hash to old_vertices_hash.
template <typename Fix, typename OnRun>
Result replay(const char* P, size_t patch_size, size_t pos, size_t seq_end, const std::string& cur_dir,
              const std::string& out_dir, uint64_t old_size, uint64_t new_size, uint32_t file_records_max,
              uint64_t old_vertices_hash, Fix fix, OnRun on_run) {
    using namespace poi_files;
    Result r;
    OldRows old(cur_dir);
    if (old.rows() * LOGICAL_STRIDE != old_size)
        throw std::runtime_error("Old poi_records.bin is not the one the patch was made from (" +
                                 std::to_string(old.rows() * LOGICAL_STRIDE) + " bytes, the patch expects " +
                                 std::to_string(old_size) + ")");
    ChildOps& child = r.child;
    child.parse(P, patch_size, seq_end, file_records_max);
    if (child.old_size != old.logical_vertex_bytes())
        throw std::runtime_error("Old poi_vertices.bin is not the one the patch was made from (" +
                                 std::to_string(old.logical_vertex_bytes()) + " bytes, the patch expects " +
                                 std::to_string(child.old_size) + ")");
    gcs::poi::Dec vd(cur_dir);
    vd.vertices_only = true;
    vd.state[0] = 2;
    vd.state[1] = 1;
    uint64_t vd_at = 0;  // the old stream's offset vd reads next
    gcs::poi::Enc enc(out_dir);
    enc.state[0] = enc.state[1] = 1;
    std::vector<char> buf(64 << 10);

    // n of the child's bytes into the encoder: MATCH bytes from vd, the others from the patch.
    auto produce = [&](uint64_t n) {
        while (n) {
            child.at_bytes();
            const uint64_t k = std::min(n, child.left);
            if (child.op == OP_MATCH_RUN) {
                if (child.old + k > child.old_size) throw std::runtime_error("Malformed merge sequence: poi_vertices.bin");
                if (vd_at < child.old) {
                    vd.skip(1, child.old - vd_at);
                    vd_at = child.old;
                }
                for (uint64_t got = 0; got < k;) {
                    const size_t g = vd.read(1, buf.data(), size_t(std::min<uint64_t>(k - got, buf.size())));
                    if (!g) poi_files::malformed("the vertex stream ends before the patch's MATCH bytes do");
                    enc.write(1, buf.data(), g);
                    got += g;
                }
                vd_at += k;
            } else {
                enc.write(1, P + child.data, size_t(k));
            }
            child.took(k);
            n -= k;
        }
    };
    // A new record, and its polygon's bytes from the child. copy_from_old: the record is old's row
    // at its lat and lng, with as many vertices: its block is old's as it is when the child's next
    // bytes are one MATCH of that block's.
    auto put = [&](const uint8_t* rec, bool copy_from_old) {
        r.records_bytes += LOGICAL_STRIDE;
        const uint32_t vc = rd32(rec + OFF_VERTEX_COUNT);
        if (!vc) {
            enc.write(0, rec, LOGICAL_STRIDE);
            return;
        }
        child.at_bytes();
        if (copy_from_old && child.op == OP_MATCH_RUN && child.old == old.block_logical() &&
            child.left >= old.block_logical_bytes()) {
            enc.put_coded_row(rec);
            old.copy_block([&](const uint8_t* p, size_t n) { enc.put_coded_bytes(p, n); });
            enc.end_coded(old.block_logical_bytes());
            child.took(old.block_logical_bytes());
            r.copied++;
            return;
        }
        enc.write(0, rec, LOGICAL_STRIDE);
        // The block's size follows from its encoding, its first byte.
        uint8_t e;
        if (child.op == OP_MATCH_RUN) {
            if (child.old >= child.old_size) throw std::runtime_error("Malformed merge sequence: poi_vertices.bin");
            if (vd_at < child.old) {
                vd.skip(1, child.old - vd_at);
                vd_at = child.old;
            }
            if (vd.read(1, reinterpret_cast<char*>(&e), 1) != 1) poi_files::malformed("the vertex stream ends early");
            vd_at++;
        } else {
            e = uint8_t(P[child.data]);
        }
        enc.write(1, &e, 1);
        child.took(1);
        produce(10 - 1 + uint64_t(vc) * (e == 3 ? 8 : 4));
        r.encoded++;
    };
    auto same_place = [](const uint8_t* a, const uint8_t* b) { return memcmp(a + OFF_LAT, b + OFF_LAT, 8) == 0; };

    uint64_t old_rec = 0, new_rec = 0;
    uint8_t rec[LOGICAL_STRIDE];
    while (pos < seq_end) {
        if (pos + 5 > seq_end) throw std::runtime_error("Malformed merge sequence: poi_records.bin");
        const uint8_t op = uint8_t(P[pos++]);
        uint32_t count;
        memcpy(&count, P + pos, 4);
        pos += 4;
        if ((op == OP_MATCH_RUN || op == OP_DELETE_RUN || op == OP_REPLACE_RUN) && count > old.rows() - old_rec)
            throw std::runtime_error(std::string("Malformed merge sequence: poi_records.bin (a ") +
                                     (op == OP_MATCH_RUN ? "MATCH" : op == OP_DELETE_RUN ? "DELETE" : "REPLACE") +
                                     " run past the old file)");
        if (op == OP_MATCH_RUN) {
            for (uint32_t k = 0; k < count; k++) {
                old.next();
                memcpy(rec, old.rec(), LOGICAL_STRIDE);
                fix(rec, old_rec + k, k, old_rec, count);
                put(rec, old.block() && rd32(rec + OFF_VERTEX_COUNT) == rd32(old.rec() + OFF_VERTEX_COUNT));
            }
            on_run(old_rec, new_rec, count);
            old_rec += count;
            new_rec += count;
        } else if (op == OP_INSERT_RUN || op == OP_REPLACE_RUN) {
            const auto run = checked_size(0, count, LOGICAL_STRIDE, seq_end - pos);
            if (!run) throw std::runtime_error("Malformed merge sequence: poi_records.bin");
            for (uint32_t k = 0; k < count; k++) {
                const uint8_t* nr = reinterpret_cast<const uint8_t*>(P + pos + size_t(k) * LOGICAL_STRIDE);
                if (op == OP_REPLACE_RUN) {
                    old.next();
                    put(nr, old.block() && same_place(nr, old.rec()) &&
                                rd32(nr + OFF_VERTEX_COUNT) == rd32(old.rec() + OFF_VERTEX_COUNT));
                } else {
                    put(nr, false);
                }
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
            throw std::runtime_error("Malformed merge sequence: poi_records.bin");
        }
    }
    child.finish();
    if (r.records_bytes != new_size) throw std::runtime_error("Rebuilt poi_records.bin does not match the new build");
    if (child.written != child.new_size) throw std::runtime_error("Rebuilt poi_vertices.bin does not match the new build");
    r.vertex_bytes = child.written;
    if (old.finish() != old_vertices_hash) throw std::runtime_error("Old poi_vertices.bin is not the one the patch was made from");
    gcs::Manifest m;
    enc.finish(m);
    for (int p = 0; p < 2; p++) {
        r.decode_peaks[p] = vd.peak(p);
        r.encode_peaks[p] = enc.peak(p);
    }
    return r;
}

}  // namespace poi_replay
