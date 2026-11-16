// The patcher's replay of the address files as they sit (addr_files.h): the points' merge
// sequence and the footprint section after it, as the joint replay of the logical records and
// footprint stream would apply them, but without decoding and encoding again every footprint a
// daily patch leaves alone. A blob is a function of its footprint's logical bytes and its record's
// lat and lng alone (its bbox minimum is stored against them), so a new record that is the old
// record at its place (a MATCH or a REPLACE of it) with that record's lat, lng and vertex count,
// whose footprint is, byte for byte, the old one (its child bytes one MATCH of that blob's logical
// bytes), gets that very blob: copied as it is, an escape in pieces. Every other footprint goes
// through the encoder, its old bytes decoded from the old blob the replay is at or one of those it
// passed last (read again), or else from a cursor that decodes the old files once more up to the
// bytes asked for (gcs::addr::Dec), made when one is. The blocks are decoded and encoded again
// record by record (their ids move every day), with the checks addr_files::decode makes between
// the units. A copied blob is never decoded, and the record whose point it is copied under is
// the old file's: the old addr_vertices.bin and addr_points.bin are hashed as the replay passes
// them and checked against the diff's hashes (OLD_FILE_HASH_MARKER).
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "child_ops.h"
#include "gcs_codec.h"
#include "patch_format.h"

namespace addr_replay {
namespace af = addr_files;
static_assert(ADDR_VERTEX_STRIDE == af::VERTEX_OFFSET_UNIT, "the footprints' runs count the units a vertex_offset counts");

// The hashes (lane_hash) of the old addr_points.bin and addr_vertices.bin.
struct Hashes {
    uint64_t points = 0, vertices = 0;
};

// The old address files front to back, a record at a time: its logical record (vertex_offset its
// footprint's offset in the logical stream, vertex_count its vertices) and its blob, whose header
// is read and whose bytes are not until they are copied, decoded or passed. It makes the checks
// gcs::addr::Dec makes between the units, in its words, and leaves a blob's values, and the
// records a block decodes to, to the hashes: every byte of addr_vertices.bin and of
// addr_points.bin is hashed in order, a block's as it is decoded.
class OldFiles {
public:
    explicit OldFiles(const std::string& dir)
        : f_(gcs::addr::footers_of(dir)),
          P_(dir + "/" + af::POINTS_FILE, gcs::addr::BLOCKS_BUF),
          V_(dir + "/" + af::VERTICES_FILE, gcs::addr::BLOBS_BUF),
          X_(dir + "/" + af::INDEX_FILE, gcs::addr::INDEX_BUF),
          points_hash_(P_.size()),
          vertices_hash_(V_.size()) {}

    uint64_t records() const { return f_.records; }
    uint64_t logical_vertex_bytes() const { return f_.logical_vertex_bytes; }

    // The next record (the blob before it passed when it was neither copied nor decoded).
    void next() {
        pass_blob();
        if (blob_) {
            passed_.push_back(at_hand());
            while (passed_.size() > HISTORY_BLOBS || blob_logical_ - passed_.front().logical > HISTORY_BYTES) passed_.pop_front();
        }
        blob_ = decoded_ = false;
        if (record_ == f_.records) af::malformed("a record past the last");
        if (j_ == block_.records) next_block();
        const uint32_t len = block_.len[j_];
        if (!len) {
            af::logical_record(block_, j_, af::NO_FOOTPRINT, 0, rec_);
        } else {
            if (len > f_.blobs_bytes - blobs_at_)
                af::malformed(std::string(af::VERTICES_FILE) + " ends inside the blob of record " + std::to_string(record_));
            try {
                h_ = af::blob_header(V_.need(size_t(std::min<uint64_t>(len, af::BLOB_HEAD_MAX))), len);
                if (h_.logical_bytes() > f_.logical_vertex_bytes - logical_)
                    af::malformed("a footprint past the footer's logical vertex stream of " + std::to_string(f_.logical_vertex_bytes) +
                                  " bytes");
                // Its len is what its count and its widths give, before any of its values is read.
                if (!h_.escape) af::blob_values(h_, V_.need(size_t(std::min<uint64_t>(len, h_.bytes + 9))), len);
            } catch (const std::runtime_error& e) {
                throw in_blob(e, record_);
            }
            af::logical_record(block_, j_, logical_, uint32_t(h_.vertex_count()), rec_);
            blob_ = true;
            len_ = len;
            blob_left_ = len;
            blob_at_ = blobs_at_;
            blob_logical_ = logical_;
            blobs_at_ += len;
            logical_ += h_.logical_bytes();
            blobs_++;
        }
        j_++;
        record_++;
    }

    // The record at hand: its logical record, and whether it has a footprint; then the footprint's
    // offset and bytes in the logical stream, whether its blob is an escape, and the blob's bytes.
    const uint8_t* rec() const { return rec_; }
    bool blob() const { return blob_; }
    uint64_t blob_logical() const { return blob_logical_; }
    uint64_t blob_logical_bytes() const { return h_.logical_bytes(); }
    bool escape() const { return h_.escape; }
    uint32_t blob_len() const { return len_; }
    // Whether the record's blob is unread: coded(), or escape_header() and rest(), give it whole.
    bool whole() const { return blob_ && !decoded_ && blob_left_ == len_; }

    // The record's blob, which is whole and no escape, as it sits: its blob_len() bytes.
    const uint8_t* coded() {
        blob_left_ = 0;
        return take(len_);
    }
    // The header of the logical blob of the record's escape, which is whole; the bytes after it
    // follow through rest().
    const uint8_t* escape_header() {
        take(h_.bytes);
        blob_left_ -= h_.bytes + af::LOGICAL_HEADER;
        return take(af::LOGICAL_HEADER);
    }
    // What is left of the record's blob through out(p, n), a piece at a time, as it is.
    template <typename Out>
    void rest(Out out) {
        while (blob_left_) {
            const size_t n = size_t(std::min<uint64_t>(blob_left_, PIECE));
            out(take(n), n);
            blob_left_ -= n;
        }
    }

    // Up to k bytes of the old logical footprint stream at `at`, at most room, to dst, when they
    // lie in the footprint of the record at hand or of one passed last: how many (0: in neither),
    // and whether a passed blob was read again for them. An escape's are read from the file where
    // they sit, another blob's from its logical form, decoded whole.
    size_t logical_bytes(uint64_t at, uint64_t k, uint8_t* dst, size_t room, bool& again) {
        const Passed hand = at_hand();
        const Passed* hit = nullptr;
        if (blob_ && at >= blob_logical_ && at - blob_logical_ < h_.logical_bytes()) hit = &hand;
        if (!hit && !passed_.empty() && at >= passed_.front().logical)
            for (auto it = passed_.rbegin(); !hit && it != passed_.rend(); ++it)
                if (at >= it->logical && at - it->logical < it->h.logical_bytes()) hit = &*it;
        if (!hit) return 0;
        // The blob at hand is read where the reader is unless a copy took it.
        const bool here = hit == &hand && (h_.escape || decoded_ || whole());
        again = !here;
        const uint64_t in = at - hit->logical;
        const size_t g = size_t(std::min<uint64_t>({k, hit->h.logical_bytes() - in, room}));
        if (hit->h.escape) {
            gcs::pread_all(V_.fd, dst, g, hit->at + hit->h.bytes + in, V_.path);
            return g;
        }
        memcpy(dst, (here ? logical_blob() : reread(*hit)) + in, g);
        return g;
    }

    // Passes the records left, checks the files end as the encoder ends them, and returns the
    // two files' hashes.
    Hashes finish() {
        while (record_ < f_.records) next();
        pass_blob();
        const std::string vertices_file = af::VERTICES_FILE;
        if (blocks_at_ != f_.blocks_bytes) af::malformed(std::string(af::POINTS_FILE) + " has bytes between its last block and its footer");
        if (blobs_at_ != f_.blobs_bytes) af::malformed(vertices_file + " has bytes between its last blob and its footer");
        if (logical_ != f_.logical_vertex_bytes)
            af::malformed(vertices_file + " counts " + std::to_string(f_.logical_vertex_bytes) + " bytes of logical blobs and its blobs hold " +
                          std::to_string(logical_));
        if (blobs_ != f_.blobs)
            af::malformed(vertices_file + " counts " + std::to_string(f_.blobs) + " blobs and holds " + std::to_string(blobs_));
        take(af::VERTICES_FOOTER_BYTES);
        points_hash_.update(reinterpret_cast<const char*>(P_.take(af::POINTS_FOOTER_BYTES)), af::POINTS_FOOTER_BYTES);
        return {points_hash_.hash(), vertices_hash_.hash()};
    }

    // The most bytes held of the old footprints at once, at most: the blobs' read window, which
    // grows to the largest blob taken whole, the largest logical form decoded at hand, and the
    // largest blob read again with its logical form.
    size_t footprint_peak() const { return V_.buf.size() + logical_peak_ + reread_peak_; }

private:
    // A blob is passed, and an escape copied, a read window at a time.
    static constexpr size_t PIECE = gcs::addr::BLOBS_BUF;
    // The blobs passed last that logical_bytes() reads again: the footprint merge pairs a run of
    // deleted records with the inserted ones after it, whose footprints MATCH the deleted ones'
    // bytes, passed by then.
    static constexpr size_t HISTORY_BLOBS = 256;
    static constexpr uint64_t HISTORY_BYTES = 64 << 10;
    // A blob by its place in the logical stream and in the file, with its record's index, lat and
    // lng and its header.
    struct Passed {
        uint64_t logical, at, record;
        uint32_t len, lat, lng;
        af::BlobHeader h;
    };

    // The blob at hand, as one passed.
    Passed at_hand() const {
        return {blob_logical_, blob_at_, record_ - 1, len_, af::get32(rec_ + af::LAT_AT), af::get32(rec_ + af::LNG_AT), h_};
    }
    // What a blob is refused for, with whose it is.
    static std::runtime_error in_blob(const std::runtime_error& e, uint64_t record) {
        return std::runtime_error(std::string(e.what()) + ": " + af::VERTICES_FILE + ", record " + std::to_string(record));
    }
    // n of addr_vertices.bin's next bytes, hashed.
    const uint8_t* take(size_t n) {
        const uint8_t* p = V_.take(n);
        vertices_hash_.update(reinterpret_cast<const char*>(p), n);
        return p;
    }
    void pass_blob() {
        rest([](const uint8_t*, size_t) {});
    }
    // The next block into block_, where the index puts it.
    void next_block() {
        const std::string index_file = af::INDEX_FILE;
        const char* const misplaced = " is not where the blocks and the blobs before it end";
        if (b_ % af::SUPERBLOCK_BLOCKS == 0) {
            const uint8_t* head = X_.take(af::INDEX_HEAD_BYTES);
            if (af::get64(head) != blocks_at_ || af::get64(head + 8) != blobs_at_)
                af::malformed(index_file + " superblock " + std::to_string(b_ / af::SUPERBLOCK_BLOCKS) + misplaced);
            row_blocks_at_ = blocks_at_;
            row_blobs_at_ = blobs_at_;
        }
        const uint8_t* entry = X_.take(af::INDEX_ENTRY_BYTES);
        if (af::get16(entry) != blocks_at_ - row_blocks_at_ || af::get32(entry + 2) != blobs_at_ - row_blobs_at_)
            af::malformed(index_file + " block " + std::to_string(b_) + misplaced);
        // Decoded where the reader holds it, with the 8 bytes decode_block may load past it: the
        // next block's, or the footer's.
        const size_t avail = size_t(std::min<uint64_t>(P_.left(), af::BLOCK_MAX_BYTES + 8));
        const uint8_t* block = P_.need(avail);
        const size_t bytes = af::decode_block(b_, block, avail, f_.block_records(b_), block_);
        points_hash_.update(reinterpret_cast<const char*>(block), bytes);
        P_.skip(bytes);
        blocks_at_ += bytes;
        b_++;
        j_ = 0;
    }
    // The logical blob of the blob at p, with the 8 bytes after it, to out.
    static void decode(const Passed& b, const uint8_t* p, uint8_t* out) {
        try {
            af::decode_blob(b.h, p, b.len, b.lat, b.lng, out);
        } catch (const std::runtime_error& e) {
            throw in_blob(e, b.record);
        }
    }
    // The logical form of the blob at hand, no escape, decoded from its bytes, which it reads.
    const uint8_t* logical_blob() {
        if (!decoded_) {
            logical_buf_.resize(size_t(h_.logical_bytes()));
            logical_peak_ = std::max(logical_peak_, logical_buf_.size());
            // With the 8 bytes decode_blob may load past it: the next blob's, or the footer's.
            decode(at_hand(), V_.need(size_t(len_) + 8), logical_buf_.data());
            take(len_);
            blob_left_ = 0;
            decoded_ = true;
        }
        return logical_buf_.data();
    }
    // The logical form of a blob passed, no escape, decoded from its bytes read again.
    const uint8_t* reread(const Passed& b) {
        if (!reread_ok_ || reread_at_ != b.logical) {
            reread_ok_ = false;
            raw_.resize(size_t(b.len) + 8);
            gcs::pread_all(V_.fd, raw_.data(), raw_.size(), b.at, V_.path);
            reread_.resize(size_t(b.h.logical_bytes()));
            reread_peak_ = std::max(reread_peak_, raw_.size() + reread_.size());
            decode(b, raw_.data(), reread_.data());
            reread_ok_ = true;
            reread_at_ = b.logical;
        }
        return reread_.data();
    }

    af::Footers f_;
    gcs::Reader P_, V_, X_;  // the blocks, the blobs, and the index's rows
    LaneHasher points_hash_, vertices_hash_;
    af::Block block_;              // the block at hand
    size_t j_ = 0;                 // its records passed
    uint64_t b_ = 0, record_ = 0;  // the blocks decoded, and the records passed
    // The bytes of the blocks and of the blobs passed, and of those before the index row at hand.
    uint64_t blocks_at_ = 0, blobs_at_ = 0, row_blocks_at_ = 0, row_blobs_at_ = 0;
    // The footprints passed, the one at hand among them: their logical bytes, and how many.
    uint64_t logical_ = 0, blobs_ = 0;
    // The record at hand: its logical record and, when it has a blob, the blob's header, bytes,
    // place in the file and in the logical stream, and the bytes of it not read yet.
    uint8_t rec_[af::LOGICAL_STRIDE] = {};
    bool blob_ = false, decoded_ = false;
    af::BlobHeader h_;
    uint32_t len_ = 0;
    uint64_t blob_at_ = 0, blob_logical_ = 0, blob_left_ = 0;
    std::deque<Passed> passed_;
    std::vector<uint8_t> logical_buf_, raw_, reread_;
    uint64_t reread_at_ = 0;
    bool reread_ok_ = false;
    size_t logical_peak_ = 0, reread_peak_ = 0;
};

// How a MATCH record's fields move, as the generic replay moves them: its vertex_offset by the
// merge section's offset fixups, its string ids (at remap_offs: none when no string moved) by the
// section's string fixups or else the string remap, and its parent_way_id by the street replay's
// remap (street, with the moved ways' pairs) or, for street ways sent unchanged, by the
// parent-id remap's street leg (street_pairs).
struct FieldMoves {
    bool has_fixups;
    OffsetFixupReader& fixups;
    StringFixupReader& string_fixups;
    const StringRemap& strings;
    const std::vector<size_t>& remap_offs;
    std::vector<size_t>& remap_hints;
    const RecordRemap* street;
    const SortedTable<RemapPair>& street_pairs;
    size_t street_hint = 0;

    void operator()(uint8_t* rec, uint64_t old_index, uint32_t k, uint64_t run_first, uint32_t run_count) {
        if (has_fixups && k < OffsetFixupReader::reach(run_first, run_count))
            af::put32(rec + ADDR_POINT_VERTEX_OFFSET_OFF,
                      fixups.apply(static_cast<uint32_t>(old_index), af::get32(rec + ADDR_POINT_VERTEX_OFFSET_OFF)));
        const uint64_t slot = old_index * remap_offs.size();
        for (size_t f = 0; f < remap_offs.size(); f++) {
            const uint32_t v = af::get32(rec + remap_offs[f]);
            af::put32(rec + remap_offs[f], string_fixups.fixes(slot + f) ? string_fixups.take() : strings.lookup(v, remap_hints[f]));
        }
        const uint32_t way = af::get32(rec + ADDR_POINT_PARENT_WAY_ID_OFF);
        if (way == NO_DATA) return;
        if (street) {
            const uint32_t moved = street->lookup(way, street_hint);
            if (moved != RecordRemap::NONE) af::put32(rec + ADDR_POINT_PARENT_WAY_ID_OFF, moved);
        } else if (const std::optional<RemapPair> pair = street_pairs.find(way)) {
            af::put32(rec + ADDR_POINT_PARENT_WAY_ID_OFF, pair->to);
        }
    }
};

struct Result {
    uint64_t records_bytes = 0, vertex_bytes = 0;  // the new logical files'
    uint64_t copied = 0, encoded = 0;              // the new footprints' blobs
    // Old footprint bytes the encoded footprints kept, by where they were read: the old blob at
    // hand, one passed and read again, the cursor.
    uint64_t at_hand = 0, read_again = 0, by_cursor = 0;
    // The footprint section after the points', its ops read in place as the records take their bytes.
    ChildOps child{PatchFileId::ADDR_VERTICES, ADDR_VERTEX_STRIDE};
    // The most bytes held at once of the old records and footprints, and of the new ones before the
    // encoder took them, in the form of the codecs' queue peaks.
    size_t decode_peaks[2] = {0, 0}, encode_peaks[2] = {0, 0};
};

// Replays the address points' merge sequence seq[0, seq_size) (stride 28) and the footprint section
// at P[child_at, patch_size) from the old files in cur_dir into new ones in out_dir, the index
// among them. fix(rec, old_index, k_in_run, run_first, run_count) moves a MATCH record's fields as
// the generic replay does (FieldMoves). on_run(old_start, new_start, count) for each MATCH and
// REPLACE run (the record remap). The old addr_points.bin and addr_vertices.bin must hash to
// old_hashes.
template <typename Fix, typename OnRun>
Result replay(const char* seq, size_t seq_size, const char* P, size_t patch_size, size_t child_at, const std::string& cur_dir,
              const std::string& out_dir, uint64_t old_size, uint64_t new_size, uint32_t file_records_max, const Hashes& old_hashes,
              Fix fix, OnRun on_run) {
    static constexpr size_t PIECE = gcs::ENCODER_PIECE;
    Result r;
    OldFiles old(cur_dir);
    if (old.records() * af::LOGICAL_STRIDE != old_size)
        throw std::runtime_error("Old addr_points.bin is not the one the patch was made from (" +
                                 std::to_string(old.records() * af::LOGICAL_STRIDE) + " bytes, the patch expects " +
                                 std::to_string(old_size) + ")");
    ChildOps& child = r.child;
    child.parse(P, patch_size, child_at, file_records_max);
    if (child.old_size != old.logical_vertex_bytes())
        throw std::runtime_error("Old addr_vertices.bin is not the one the patch was made from (" +
                                 std::to_string(old.logical_vertex_bytes()) + " bytes, the patch expects " +
                                 std::to_string(child.old_size) + ")");
    std::unique_ptr<gcs::addr::Dec> vd;
    uint64_t vd_at = 0;  // the old stream's offset vd reads next
    // Up to k bytes of the old stream at child.old (a MATCH's), at most room, to dst: from the
    // footprint of the old record the replay is at or of one it passed last, else from vd, which
    // decodes on to them. How many.
    auto old_bytes = [&](uint64_t k, uint8_t* dst, size_t room) -> size_t {
        const uint64_t at = child.old;
        if (at >= child.old_size || k > child.old_size - at) throw std::runtime_error("Malformed merge sequence: addr_vertices.bin");
        bool again = false;
        if (const size_t g = old.logical_bytes(at, k, dst, room, again)) {
            (again ? r.read_again : r.at_hand) += g;
            return g;
        }
        if (!vd) {
            vd.reset(new gcs::addr::Dec(cur_dir));
            vd->state[0] = 2;
            vd->state[1] = 1;
        }
        if (vd_at < at) {
            vd->skip(1, at - vd_at);
            vd_at = at;
        }
        const size_t g = vd->read(1, reinterpret_cast<char*>(dst), size_t(std::min<uint64_t>(k, room)));
        if (!g) af::malformed("the footprint stream ends before the patch's MATCH bytes do");
        vd_at += g;
        r.by_cursor += g;
        return g;
    };
    gcs::addr::Enc enc(out_dir);
    auto& files = enc.files;
    // A new footprint's logical blob, gathered for the encoder, and a piece of an escape.
    std::vector<uint8_t> blob, piece;
    size_t blob_peak = 0;

    // n of the child's next bytes to dst: MATCH bytes from the old files, the others from the patch.
    auto fill = [&](uint8_t* dst, size_t n) {
        while (n) {
            child.at_bytes();
            const size_t k = size_t(std::min<uint64_t>(n, child.left));
            if (child.op == OP_MATCH_RUN) {
                const size_t g = old_bytes(k, dst, k);
                child.took(g);
                dst += g;
                n -= g;
                continue;
            }
            memcpy(dst, P + child.data, k);
            child.took(k);
            dst += k;
            n -= k;
        }
    };
    // A new record, and its footprint's bytes from the child. at_old_point: the record is old's
    // record at its lat and lng: its blob is old's as it is when it has as many vertices and the
    // child's next bytes are one MATCH of that blob's.
    auto put = [&](const uint8_t* rec, bool at_old_point) {
        r.records_bytes += af::LOGICAL_STRIDE;
        const uint32_t vc = af::get32(rec + af::VERTEX_COUNT_AT);
        if (!vc) {
            files.add(rec, nullptr);
            return;
        }
        child.at_bytes();
        if (at_old_point && old.whole() && vc == af::get32(old.rec() + af::VERTEX_COUNT_AT) && child.op == OP_MATCH_RUN &&
            child.old == old.blob_logical() && child.left >= old.blob_logical_bytes()) {
            if (old.escape()) {
                files.begin_escape(rec, old.escape_header());
                old.rest([&](const uint8_t* p, size_t n) { files.put_escape_bytes(p, n); });
            } else {
                const uint32_t len = old.blob_len();
                files.add_coded(rec, old.coded(), len);
            }
            child.took(old.blob_logical_bytes());
            r.copied++;
            return;
        }
        uint8_t head[af::LOGICAL_HEADER];
        fill(head, sizeof head);
        // A header no footprint has is the encoder's to refuse, with no vertex read.
        const bool footprint = head[0] <= 3 && !head[1];
        if (footprint && af::is_escape(head[0], vc)) {
            files.begin_escape(rec, head);
            if (piece.empty()) piece.resize(PIECE);
            while (const uint64_t owed = files.escape_owed()) {
                child.at_bytes();
                const size_t k = size_t(std::min<uint64_t>({owed, child.left, PIECE}));
                if (child.op == OP_MATCH_RUN) {
                    const size_t g = old_bytes(k, piece.data(), k);
                    files.put_escape_bytes(piece.data(), g);
                    child.took(g);
                } else {
                    files.put_escape_bytes(reinterpret_cast<const uint8_t*>(P + child.data), k);
                    child.took(k);
                }
            }
        } else {
            blob.resize(footprint ? size_t(af::logical_blob_bytes(head[0], vc)) : sizeof head);
            blob_peak = std::max(blob_peak, blob.size());
            memcpy(blob.data(), head, sizeof head);
            fill(blob.data() + sizeof head, blob.size() - sizeof head);
            files.add(rec, blob.data());
        }
        r.encoded++;
    };
    auto same_point = [](const uint8_t* a, const uint8_t* b) { return memcmp(a + af::LAT_AT, b + af::LAT_AT, 8) == 0; };

    uint64_t old_rec = 0, new_rec = 0;
    uint8_t rec[af::LOGICAL_STRIDE];
    size_t pos = 0;
    while (pos < seq_size) {
        if (seq_size - pos < 5) throw std::runtime_error("Malformed merge sequence: addr_points.bin");
        const uint8_t op = uint8_t(seq[pos++]);
        uint32_t count;
        memcpy(&count, seq + pos, 4);
        pos += 4;
        if ((op == OP_MATCH_RUN || op == OP_DELETE_RUN || op == OP_REPLACE_RUN) && count > old.records() - old_rec)
            throw std::runtime_error(std::string("Malformed merge sequence: addr_points.bin (a ") +
                                     (op == OP_MATCH_RUN ? "MATCH" : op == OP_DELETE_RUN ? "DELETE" : "REPLACE") +
                                     " run past the old file)");
        if (op == OP_MATCH_RUN) {
            for (uint32_t k = 0; k < count; k++) {
                old.next();
                memcpy(rec, old.rec(), af::LOGICAL_STRIDE);
                fix(rec, old_rec + k, k, old_rec, count);
                put(rec, old.blob());
            }
            on_run(old_rec, new_rec, count);
            old_rec += count;
            new_rec += count;
        } else if (op == OP_INSERT_RUN || op == OP_REPLACE_RUN) {
            const auto run = checked_size(0, count, af::LOGICAL_STRIDE, seq_size - pos);
            if (!run) throw std::runtime_error("Malformed merge sequence: addr_points.bin");
            for (uint32_t k = 0; k < count; k++) {
                const uint8_t* nr = reinterpret_cast<const uint8_t*>(seq + pos + size_t(k) * af::LOGICAL_STRIDE);
                if (op == OP_REPLACE_RUN) {
                    old.next();
                    put(nr, old.blob() && same_point(nr, old.rec()));
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
            throw std::runtime_error("Malformed merge sequence: addr_points.bin");
        }
    }
    child.finish();
    if (r.records_bytes != new_size) throw std::runtime_error("Rebuilt addr_points.bin does not match the new build");
    if (child.written != child.new_size) throw std::runtime_error("Rebuilt addr_vertices.bin does not match the new build");
    r.vertex_bytes = child.written;
    const Hashes hashes = old.finish();
    if (hashes.points != old_hashes.points) throw std::runtime_error("Old addr_points.bin is not the one the patch was made from");
    if (hashes.vertices != old_hashes.vertices) throw std::runtime_error("Old addr_vertices.bin is not the one the patch was made from");
    enc.finish();
    r.decode_peaks[0] = old.records() ? af::BLOCK_RECORDS * af::LOGICAL_STRIDE : 0;
    r.decode_peaks[1] = old.footprint_peak() + (vd ? vd->peak(1) : 0);
    r.encode_peaks[0] = r.records_bytes ? af::LOGICAL_STRIDE : 0;
    r.encode_peaks[1] = std::max(blob_peak, piece.size());
    return r;
}

}  // namespace addr_replay
