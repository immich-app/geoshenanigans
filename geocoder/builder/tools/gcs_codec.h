// In-process incremental codecs for the low-memory client update. The formats the builder writes
// (src/way_files.h, addr_files.h, poi_files.h, place_files.h, side_blocks.h) byte for byte,
// structured so the patcher drives them:
//   Decoder: produces today's-format bytes on demand, one unit (way, row, record, block piece) per
//            step(), into one queue per today's file (a coupled family fills several queues).
//   Encoder: consumes today's-format bytes as the patcher writes them, one queue per today's file,
//            and processes every unit whose bytes are all there (pump()).
// Nothing is held beyond the buffers, the unit in flight and the queues' skew. A unit held whole is
// bounded by the format (a way <= 65535 nodes, an address footprint <= 65535 vertices; an escaped
// footprint, of any size, is copied through in pieces) or, for a polygon block, by big_block()
// (256 KiB): a larger POI block is decoded a piece at a time, and encoded through the new file
// (its vertices written as they come, then packed in place).
#pragma once
#include <functional>
#include <memory>
#include "addr_files.h"
#include "gcs_io.h"
#include "place_files.h"
#include "poi_files.h"
#include "side_blocks.h"
#include "way_files.h"

namespace gcs {

// ------------------------------------------------------------------ framework
struct Decoder {
    std::vector<ByteQueue> out;
    std::vector<int> state;  // 0 not opened yet (kept), 1 open, 2 closed (dropped)
    std::vector<uint64_t> drop;  // per port: bytes its reader skips, dropped as they are decoded
    size_t dropping = 0;         // ports with bytes to drop
    std::string name;
    std::vector<std::string> files;  // per port: the today's-format file it decodes to
    std::vector<uint8_t> scratch;
    Decoder(const std::string& n, std::vector<std::string> port_files)
        : out(port_files.size()), state(port_files.size(), 0), drop(port_files.size(), 0), name(n), files(std::move(port_files)) {}
    virtual ~Decoder() = default;
    virtual bool step() = 0;  // emits the next unit; false at the end
    // The decoded size of a port's file, from the footers alone.
    virtual uint64_t decoded_size(int port) const = 0;
    inline void emit(int port, const void* p, size_t n) {
        if (state[size_t(port)] != 2) out[size_t(port)].push(p, n);
    }
    // n bytes at the back of a port's queue to fill (scratch when the port is closed).
    inline uint8_t* emit_reserve(int port, size_t n) {
        if (state[size_t(port)] == 2) {
            if (scratch.size() < n) scratch.resize(n);
            return scratch.data();
        }
        return out[size_t(port)].reserve(n);
    }
    // Decodes ahead until the port holds up to READ_AHEAD bytes (or max, if less), so a reader's
    // refill takes many small units at once; a coupled family's other queues grow by as much.
    // It stops early, once the port has something, when another open port already holds SKEW_SOFT
    // (its reader is behind: decoding further would only grow that queue). With nothing for the
    // port yet it decodes on, unless another port holds MAX_DECODER_SKEW (require_in_step).
    static constexpr size_t READ_AHEAD = size_t(8) << 10, SKEW_SOFT = size_t(64) << 10;
    bool others_full(int port) const {
        for (size_t p = 0; p < out.size(); p++)
            if (int(p) != port && state[p] != 2 && out[p].size() >= SKEW_SOFT) return true;
        return false;
    }
    // The reader of `reading` drives the decoder while the one of `held` has left that file's
    // bytes queued: refused.
    [[noreturn]] void runs_ahead(int reading, int held) const {
        die("Malformed merge sequence: %s runs ahead of %s", files[size_t(reading)].c_str(), files[size_t(held)].c_str());
    }
    // Before a step for the port's reader: no other open port holds more than MAX_DECODER_SKEW.
    void require_in_step(int port) const {
        for (size_t p = 0; p < out.size(); p++)
            if (int(p) != port && state[p] != 2 && out[p].size() > MAX_DECODER_SKEW) runs_ahead(port, int(p));
    }
    [[gnu::always_inline]] size_t read(int port, char* dst, size_t max) {
        ByteQueue& q = out[size_t(port)];
        const size_t want = std::min(max, READ_AHEAD);
        while (q.size() < want) {
            if (out.size() > 1 && others_full(port)) {
                if (q.size()) break;
                require_in_step(port);
            }
            if (!step()) break;
            if (dropping) apply_drops();
        }
        if (q.size() == 0) return 0;
        const size_t k = std::min(max, q.size());
        memcpy(dst, q.data(), k);
        q.pop(k);
        return k;
    }
    // The port's reader skips its next n bytes: queued ones go now, the rest as they are decoded
    // (a reader of a coupled family that skips, e.g. the child of deleted parents, holds nothing
    // while the other reader drives the decoder through them).
    void skip(int port, uint64_t n) {
        ByteQueue& q = out[size_t(port)];
        const size_t k = size_t(std::min<uint64_t>(n, q.size()));
        q.pop(k);
        if (n > k) {
            if (!drop[size_t(port)]) dropping++;
            drop[size_t(port)] += n - k;
        }
    }
    void apply_drops() {
        for (size_t p = 0; p < out.size(); p++) {
            if (!drop[p] || !out[p].size()) continue;
            const size_t k = size_t(std::min<uint64_t>(drop[p], out[p].size()));
            out[p].pop(k);
            drop[p] -= k;
            if (!drop[p]) dropping--;
        }
    }
    size_t peak(int port) const { return out[size_t(port)].peak; }
};

struct Encoder {
    std::vector<ByteQueue> in;
    std::vector<int> state;  // 0 not opened, 1 open, 2 closed
    std::vector<uint64_t> got, expect;
    std::string name, out_dir;
    explicit Encoder(const std::string& n, const std::string& od, int ports)
        : in(size_t(ports)), state(size_t(ports), 0), got(size_t(ports), 0), expect(size_t(ports), ~0ull), name(n), out_dir(od) {}
    virtual ~Encoder() = default;
    virtual void pump() = 0;    // processes every complete unit
    virtual void finish() = 0;  // every port closed and pumped
    // Optional: processes complete units straight from the writer's bytes (the port's queue is
    // empty); returns the bytes taken.
    virtual size_t consume(int, const uint8_t*, size_t) { return 0; }
    void write(int port, const void* p, size_t n) {
        const size_t pt = size_t(port);
        const uint8_t* b = static_cast<const uint8_t*>(p);
        while (n) {
            size_t k = in[pt].size() == 0 ? consume(port, b, n) : 0;
            if (!k) {
                k = std::min(n, ENCODER_PIECE);
                in[pt].push(b, k);
            }
            got[pt] += k;  // by the piece, not the write: unit_size() puts the queue's front at got - queued
            b += k;
            n -= k;
            if (in[pt].size()) pump();
        }
    }
    bool all_closed() const {
        for (int s : state)
            if (s != 2) return false;
        return true;
    }
    size_t peak(int port) const { return in[size_t(port)].peak; }
    // The bytes of the unit at the front of the port's queue that a count sizes, head + count * item
    // (checked_size.h): one past the end of the port's file (its size as the patch gives it) fails
    // as its first bytes come, instead of the queue taking the rest of the file while it waits.
    size_t unit_size(int port, uint64_t head, uint64_t count, uint64_t item) const {
        const size_t p = size_t(port);
        const uint64_t at = got[p] - in[p].size();
        const uint64_t left = expect[p] == ~0ull ? ~0ull : expect[p] - std::min(at, expect[p]);
        const auto n = checked_size(head, count, item, left);
        if (!n)
            die("%s: a unit of %llu + %llu x %llu bytes at %llu runs past the end of input %zu (%llu bytes left)", name.c_str(),
                (unsigned long long)head, (unsigned long long)count, (unsigned long long)item, (unsigned long long)at, p,
                (unsigned long long)left);
        return *n;
    }
    void require_empty() const {
        for (size_t p = 0; p < in.size(); p++)
            if (in[p].size()) die("%s: %zu bytes left over on input %zu", name.c_str(), in[p].size(), p);
    }
    // A coupled family's pump stops with bytes on port `held` that wait for port `wanted`'s: a
    // record for its child bytes, or child bytes for their record. Refused once `wanted` is
    // closed, as no unit will take them; and past MAX_ENCODER_SKEW, so the queue holds no more
    // of a file whose other file never comes.
    void waits(int held, int wanted) const {
        const size_t h = size_t(held), w = size_t(wanted);
        if (!in[h].size()) return;
        if (state[w] == 2) die("%s: %zu bytes left over on input %zu", name.c_str(), in[h].size(), h);
        if (in[h].size() > MAX_ENCODER_SKEW) die("%s: input %zu runs %zu bytes ahead of input %zu", name.c_str(), h, in[h].size(), w);
    }
};

// ------------------------------------------------------------------ streets / interps (way_files.h)
// The street and the interpolation ways are way files in every dir, a rows file and a nodes file
// a kind: these codecs turn a pair into its logical form, the ways' records (a WayHeader or an
// InterpWay each) and their nodes back to back, for the replay and back.
namespace ways {
namespace wf = way_files;
// The codec's buffers: the rows file streams a group's row (284 bytes at most) at a time, the
// nodes file a way's block (a way of 65,535 nodes grows its buffer to the block's 512 KiB).
static const size_t ROWS_BUF = size_t(4) << 10, BLOCKS_BUF = size_t(16) << 10;

inline const char* family_of(const wf::Kind& kind) { return kind.interp ? "interps" : "streets"; }

// The footers of dir's pair of the kind, from each file's size and last bytes alone: those of no
// ways for a dir with neither file.
inline wf::Footers footers_of(const std::string& dir, const wf::Kind& kind) {
    uint8_t w[wf::WAYS_FOOTER_BYTES] = {}, n[wf::NODES_FOOTER_BYTES] = {};
    // The file's size, its footer into `to` when it is long enough to have one.
    auto footer = [&](const char* file, uint8_t* to, size_t bytes) -> uint64_t {
        const std::string path = dir + "/" + file;
        struct stat st;
        if (stat(path.c_str(), &st) != 0) return wf::ABSENT;
        const uint64_t size = uint64_t(st.st_size);
        if (size >= bytes) {
            Reader end(path, bytes, size - bytes);
            memcpy(to, end.take(bytes), bytes);
        }
        return size;
    };
    const uint64_t ways_size = footer(kind.ways_file, w, sizeof w), nodes_size = footer(kind.nodes_file, n, sizeof n);
    return wf::read_footers(kind, w, ways_size, n, nodes_size);
}

// A dir's pair's decoded sizes (Dec::decoded_size), from the two footers alone: 0 without the
// files.
inline uint64_t decoded_size(const std::string& dir, const wf::Kind& kind, int port) {
    const wf::LogicalSizes sizes = wf::logical_sizes(kind, footers_of(dir, kind));
    return port == 0 ? sizes.ways : sizes.nodes;
}

// Ports: 0 = the ways' records, 1 = their nodes. A step: a way's record and its nodes. A dir
// without the two files decodes to none. It checks between the units what way_files::decode
// checks: each row's blocks start where those before them end, the last block ends at the
// footer, and the ways' nodes are the footer's.
struct Dec : Decoder {
    const wf::Kind& kind;
    wf::Footers f;
    std::unique_ptr<Reader> R, B;  // the rows, and the node blocks
    wf::Row row;
    uint64_t way = 0, node = 0, at = 0;  // the ways and nodes decoded, and the bytes of their blocks
    Dec(const std::string& src, const wf::Kind& k)
        : Decoder(family_of(k), {k.ways_file, k.nodes_file}), kind(k), f(footers_of(src, k)) {
        if (!f.ways) return;
        R.reset(new Reader(src + "/" + k.ways_file, ROWS_BUF, 0, f.rows_bytes));
        B.reset(new Reader(src + "/" + k.nodes_file, BLOCKS_BUF, 0, f.blocks_bytes));
    }
    uint64_t decoded_size(int port) const override {
        const wf::LogicalSizes sizes = wf::logical_sizes(kind, f);
        return port == 0 ? sizes.ways : sizes.nodes;
    }
    bool step() override {
        if (way == f.ways) {
            if (!R) return false;
            if (at != f.blocks_bytes) wf::malformed(std::string(kind.nodes_file) + " has bytes between its last block and its footer");
            if (node != f.nodes)
                wf::malformed(std::string(kind.nodes_file) + " counts " + std::to_string(f.nodes) + " nodes and its ways have " +
                              std::to_string(node));
            R.reset();
            B.reset();
            return false;
        }
        const size_t j = size_t(way % wf::GROUP_WAYS);
        if (j == 0) {
            const uint64_t g = way / wf::GROUP_WAYS;
            const size_t n = f.group_ways(g);
            row = wf::decode_row(kind, R->take(kind.row_bytes(n)), n);
            if (row.off != at)
                wf::malformed(std::string(kind.ways_file) + " group " + std::to_string(g) + " is not where the blocks before it end");
        }
        uint8_t record[wf::MAX_STRIDE];
        wf::logical_record(kind, row, j, node, record);
        emit(0, record, kind.stride);
        if (const uint16_t cnt = row.cnt[j]) {
            const size_t bytes = row.block_bytes(j);
            if (bytes > f.blocks_bytes - at)
                wf::malformed(std::string(kind.nodes_file) + " ends inside the block of way " + std::to_string(way));
            // Decoded where the reader holds the block, with the 8 bytes decode_block may load past it.
            const uint8_t* block = B->need(bytes + 8, true);
            wf::decode_block(block, cnt, row.code[j], row.anchor_lat, row.anchor_lng, emit_reserve(1, wf::NODE_BYTES * size_t(cnt)));
            B->skip(bytes);
            at += bytes;
            node += cnt;
        }
        way++;
        return true;
    }
};

// Ports: 0 = the ways' records in, 1 = their nodes in; the pair's two files out. A way is encoded
// once its record and all its nodes are in: its block goes to the nodes file then, and its group's
// row to the rows file when the group's last way has come. The footers go out in finish.
struct Enc : Encoder {
    struct Sink {
        Writer* to;
        void operator()(const uint8_t* p, size_t n) const { to->put(p, n); }
    };
    const wf::Kind& kind;
    std::unique_ptr<Writer> RW, BW;
    wf::Encoder<Sink, Sink> files;
    Enc(const std::string& od, const wf::Kind& k)
        : Encoder(family_of(k), od, 2),
          kind(k),
          RW(new Writer(od + "/" + k.ways_file, ROWS_BUF)),
          BW(new Writer(od + "/" + k.nodes_file, BLOCKS_BUF)),
          files(k, Sink{RW.get()}, Sink{BW.get()}) {}
    void pump() override {
        ByteQueue &W = in[0], &N = in[1];
        while (W.size() >= kind.stride) {
            const uint8_t* record = W.data();
            const size_t nodes = wf::NODE_BYTES * size_t(wf::get16(record + wf::NODE_COUNT_AT));
            if (N.size() < nodes) return waits(0, 1);  // its nodes are not all in yet
            files.add(record, N.data());
            N.pop(nodes);
            W.pop(kind.stride);
        }
        waits(1, 0);  // nodes whose way's record is not in yet
    }
    void finish() override {
        require_empty();
        files.finish();
        const auto ways_footer = files.ways_footer();
        const auto nodes_footer = files.nodes_footer();
        RW->put(ways_footer.data(), ways_footer.size());
        BW->put(nodes_footer.data(), nodes_footer.size());
        RW->finish();
        BW->finish();
        RW.reset();
        BW.reset();
    }
};
}  // namespace ways

// ------------------------------------------------------------------ side files (side_blocks.h)
// way_parents.bin, way_postcodes.bin, interp_postcodes.bin and addr_postcodes.bin are side blocks in
// every dir: these codecs turn one into its logical form, the u32 values back to back, for the
// patcher's deltas and back. A family is its file's name without ".bin".
namespace side {
// The codec's buffers: a file streams front to back a block (262 bytes at most) at a time, its
// table 4 bytes a block.
static const size_t BLOCKS_BUF = size_t(16) << 10, TABLE_BUF = size_t(4) << 10;

// The footer of the side file `file` at path, of size bytes.
inline side_blocks::Footer footer_of(const std::string& path, const std::string& file, uint64_t size) {
    uint8_t f[side_blocks::FOOTER_BYTES] = {};
    if (size >= side_blocks::FOOTER_BYTES) {
        Reader footer(path, side_blocks::FOOTER_BYTES, size - side_blocks::FOOTER_BYTES);
        memcpy(f, footer.take(side_blocks::FOOTER_BYTES), side_blocks::FOOTER_BYTES);
    }
    return side_blocks::read_footer(file, f, size);
}

// A dir's side file's decoded size, 4 bytes a value, from its footer alone: 0 without the file.
inline uint64_t decoded_size(const std::string& dir, const std::string& family) {
    const std::string file = family + ".bin", path = dir + "/" + file;
    struct stat st;
    if (stat(path.c_str(), &st) != 0) return 0;
    return footer_of(path, file, uint64_t(st.st_size)).logical_bytes();
}

// Port 0 = the file's values. A step: a block. A dir without the file decodes to none.
struct Dec : Decoder {
    std::string file;
    std::unique_ptr<Reader> B, T;  // the blocks, and their table
    side_blocks::Footer f;
    uint64_t b = 0, at = 0;
    Dec(const std::string& src, const std::string& family) : Decoder(family, {family + ".bin"}), file(family + ".bin") {
        const std::string path = src + "/" + file;
        struct stat st;
        if (stat(path.c_str(), &st) != 0) return;
        const uint64_t size = uint64_t(st.st_size);
        f = footer_of(path, file, size);
        B.reset(new Reader(path, BLOCKS_BUF, 0, f.table_at));
        T.reset(new Reader(path, TABLE_BUF, f.table_at, size - side_blocks::FOOTER_BYTES));
    }
    uint64_t decoded_size(int) const override { return f.logical_bytes(); }
    bool step() override {
        if (!B) return false;
        if (b == f.blocks) {
            if (at != f.table_at) side_blocks::malformed(file, "bytes between its blocks and its table");
            B.reset();
            T.reset();
            return false;
        }
        if (side_blocks::get32(T->take(side_blocks::TABLE_ENTRY_BYTES)) != at)
            side_blocks::malformed(file, "block " + std::to_string(b) + " is not where the blocks before it end");
        const size_t count = f.values_of(b), avail = size_t(std::min<uint64_t>(B->left(), side_blocks::BLOCK_MAX_BYTES));
        // Decoded into u32s of their own: the queue's bytes sit at any address.
        std::array<uint32_t, side_blocks::BLOCK_VALUES> values;
        const size_t bytes = side_blocks::decode_block(file, b, avail ? B->need(avail) : nullptr, avail, count, values.data());
        B->skip(bytes);
        emit(0, values.data(), 4 * count);
        at += bytes;
        b++;
        return true;
    }
};

// Port 0 = the values in; the file out. The blocks go to it as they fill and their table entries
// to a scratch file beside it, which finish appends to it with the footer: nothing grows with the
// file but the files.
struct Enc : Encoder {
    struct BlockSink {
        Writer* to;
        void operator()(const uint8_t* p, size_t n) const { to->put(p, n); }
    };
    std::string file, tpath;
    std::unique_ptr<Writer> BW, TW;
    side_blocks::Encoder<BlockSink> blocks;
    Enc(const std::string& od, const std::string& family)
        : Encoder(family, od, 1),
          file(family + ".bin"),
          tpath(od + "/." + family + ".table"),
          BW(new Writer(od + "/" + file, BLOCKS_BUF)),
          TW(new Writer(tpath, TABLE_BUF, true)),
          blocks(BlockSink{BW.get()}) {}
    void add(uint32_t v) {
        blocks.add(v);
        if (!blocks.table().empty()) spill();
    }
    void spill() {
        for (const uint64_t offset : blocks.table()) {
            uint8_t e[side_blocks::TABLE_ENTRY_BYTES];
            side_blocks::write_table_entry(file, offset, e);
            TW->put(e, sizeof e);
        }
        blocks.clear_table();
    }
    // Every byte straight from the writer (nothing queued): a value a write cuts waits in carry.
    uint8_t carry[4];
    size_t carried = 0;
    size_t consume(int, const uint8_t* p, size_t n) override {
        size_t used = 0;
        if (carried) {
            used = std::min(sizeof carry - carried, n);
            memcpy(carry + carried, p, used);
            carried += used;
            if (carried < sizeof carry) return n;
            add(rd32(carry));
            carried = 0;
        }
        for (; n - used >= sizeof carry; used += sizeof carry) add(rd32(p + used));
        memcpy(carry, p + used, n - used);
        carried = n - used;
        return n;
    }
    void pump() override {
        if (in[0].size()) in[0].pop(consume(0, in[0].data(), in[0].size()));
    }
    void finish() override {
        require_empty();
        if (carried) die("%s: the stream ends %zu bytes into a value", file.c_str(), carried);
        blocks.finish();
        spill();
        TW->flush();
        uint8_t buf[TABLE_BUF];
        for (uint64_t off = 0, n = TW->tell(); off < n;) {
            const size_t k = size_t(std::min<uint64_t>(sizeof buf, n - off));
            pread_all(TW->fd, buf, k, off, tpath);
            BW->put(buf, k);
            off += k;
        }
        const auto footer = side_blocks::footer(file, blocks.values());
        BW->put(footer.data(), footer.size());
        TW->finish();
        if (unlink(tpath.c_str())) die("unlink %s: %s", tpath.c_str(), strerror(errno));
        BW->finish();
        TW.reset();
        BW.reset();
    }
};
}  // namespace side

// ------------------------------------------------------------------ address points + footprints (addr_files.h)
// The address points and their footprints are the address files in every dir, the blocks of
// addr_points.bin, the blobs of addr_vertices.bin and addr_index.bin: these codecs turn the three
// into their logical form, the points' records (an AddrPoint each) and their footprints' logical
// blobs back to back, for the replay and back.
namespace addr {
namespace af = addr_files;
// The codec's buffers: the blocks file streams a block (415 bytes at most) at a time and the index
// a row's head or a block's entry; the blobs file a blob (one of 65,535 vertices grows its buffer
// to the blob's 256 KiB), an escape in pieces of the buffer.
static const size_t BLOCKS_BUF = size_t(16) << 10, BLOBS_BUF = size_t(16) << 10, INDEX_BUF = size_t(4) << 10;

// The footers of dir's address files, from each file's size and last bytes alone: those of no
// records for a dir with none of the three. Refuses a dir with some of them, and footers that
// give a logical addr_vertices.bin or records past what a patch indexes.
inline af::Footers footers_of(const std::string& dir) {
    uint8_t p[af::POINTS_FOOTER_BYTES] = {}, v[af::VERTICES_FOOTER_BYTES] = {}, x[af::INDEX_FOOTER_BYTES] = {};
    // The file's size, its footer into `to` when it is long enough to have one.
    auto footer = [&](const char* file, uint8_t* to, size_t bytes) -> uint64_t {
        const std::string path = dir + "/" + file;
        struct stat st;
        if (stat(path.c_str(), &st) != 0) return af::ABSENT;
        const uint64_t size = uint64_t(st.st_size);
        if (size >= bytes) {
            Reader end(path, bytes, size - bytes);
            memcpy(to, end.take(bytes), bytes);
        }
        return size;
    };
    const uint64_t points_size = footer(af::POINTS_FILE, p, sizeof p), vertices_size = footer(af::VERTICES_FILE, v, sizeof v),
                   index_size = footer(af::INDEX_FILE, x, sizeof x);
    return af::read_footers(p, points_size, v, vertices_size, x, index_size);
}

// A dir's address files' decoded sizes (Dec::decoded_size), from the three footers alone: 0
// without the files.
inline uint64_t decoded_size(const std::string& dir, int port) {
    const af::LogicalSizes sizes = af::logical_sizes(footers_of(dir));
    return port == 0 ? sizes.points : sizes.vertices;
}

// Ports: 0 = the address points' records, 1 = their footprints' logical blobs. A step: a record
// and its footprint, its block decoded as its first record is due; or a piece of an escaped
// footprint, which neither the reader nor a queue holds whole. A dir without the three files
// decodes to none. It checks between the units what addr_files::decode checks: each block and
// each blob starts where those before it end and where addr_index.bin puts it, the last ones end
// at the footers, and the blobs and their logical bytes are the footer's.
struct Dec : Decoder {
    af::Footers f;
    std::unique_ptr<Reader> P, V, X;  // the blocks, the blobs, and the index's rows
    af::Block block;                  // the block at hand
    size_t j = 0;                     // its records emitted
    uint64_t b = 0, record = 0;       // the blocks decoded, and the records emitted
    // The bytes of the blocks and of the blobs passed, and of those before the index row at hand.
    uint64_t blocks_at = 0, blobs_at = 0, row_blocks_at = 0, row_blobs_at = 0;
    uint64_t logical = 0, blobs = 0;  // the footprints passed: their logical bytes, and how many
    uint64_t escaped_left = 0;        // bytes of the escaped footprint in progress still to emit
    bool ended = false;
    explicit Dec(const std::string& src) : Decoder("addr", {af::POINTS_FILE, af::VERTICES_FILE}), f(footers_of(src)) {
        if (!f.records) return;
        P.reset(new Reader(src + "/" + af::POINTS_FILE, BLOCKS_BUF));
        V.reset(new Reader(src + "/" + af::VERTICES_FILE, BLOBS_BUF));
        X.reset(new Reader(src + "/" + af::INDEX_FILE, INDEX_BUF));
    }
    uint64_t decoded_size(int port) const override {
        const af::LogicalSizes sizes = af::logical_sizes(f);
        return port == 0 ? sizes.points : sizes.vertices;
    }
    void emit_escaped_piece() {
        const size_t k = size_t(std::min<uint64_t>(escaped_left, BLOBS_BUF));
        emit(1, V->take(k), k);
        escaped_left -= k;
    }
    // The next block into `block`, where the index puts it.
    void next_block() {
        const std::string index_file = af::INDEX_FILE;
        const char* const misplaced = " is not where the blocks and the blobs before it end";
        if (b % af::SUPERBLOCK_BLOCKS == 0) {
            const uint8_t* head = X->take(af::INDEX_HEAD_BYTES);
            if (af::get64(head) != blocks_at || af::get64(head + 8) != blobs_at)
                af::malformed(index_file + " superblock " + std::to_string(b / af::SUPERBLOCK_BLOCKS) + misplaced);
            row_blocks_at = blocks_at;
            row_blobs_at = blobs_at;
        }
        const uint8_t* entry = X->take(af::INDEX_ENTRY_BYTES);
        if (af::get16(entry) != blocks_at - row_blocks_at || af::get32(entry + 2) != blobs_at - row_blobs_at)
            af::malformed(index_file + " block " + std::to_string(b) + misplaced);
        // Decoded where the reader holds it, with the 8 bytes decode_block may load past it: the
        // next block's, or the footer's.
        const size_t avail = size_t(std::min<uint64_t>(P->left(), af::BLOCK_MAX_BYTES + 8));
        const size_t bytes = af::decode_block(b, P->need(avail), avail, f.block_records(b), block);
        P->skip(bytes);
        blocks_at += bytes;
        b++;
        j = 0;
    }
    // The block's next record, and its footprint or an escaped one's first piece.
    void emit_record() {
        const uint32_t len = block.len[j];
        uint8_t rec[af::LOGICAL_STRIDE];
        if (!len) {
            af::logical_record(block, j, NO_DATA, 0, rec);
            emit(0, rec, sizeof rec);
        } else {
            if (len > f.blobs_bytes - blobs_at)
                af::malformed(std::string(af::VERTICES_FILE) + " ends inside the blob of record " + std::to_string(record));
            uint64_t logical_bytes = 0;
            try {
                const af::BlobHeader h = af::blob_header(V->need(size_t(std::min<uint64_t>(len, af::BLOB_HEAD_MAX))), len);
                logical_bytes = h.logical_bytes();
                if (logical_bytes > f.logical_vertex_bytes - logical)
                    af::malformed("a footprint past the footer's logical vertex stream of " +
                                  std::to_string(f.logical_vertex_bytes) + " bytes");
                af::logical_record(block, j, uint32_t(logical), uint32_t(h.vertex_count()), rec);
                if (h.escape) {
                    V->skip(h.bytes);
                    escaped_left = logical_bytes;
                } else {
                    // Its len is what its count and its widths give, before the reader holds as
                    // many bytes; then with the 8 bytes decode_blob may load past it: the next
                    // blob's, or the footer's.
                    af::blob_values(h, V->need(size_t(std::min<uint64_t>(len, h.bytes + 9))), len);
                    uint8_t* to = emit_reserve(1, size_t(logical_bytes));
                    af::decode_blob(h, V->need(size_t(len) + 8), len, block.lat[j], block.lng[j], to);
                    V->skip(len);
                }
            } catch (const std::runtime_error& e) {
                // What a blob is refused for, with whose it is.
                throw std::runtime_error(std::string(e.what()) + ": " + af::VERTICES_FILE + ", record " + std::to_string(record));
            }
            emit(0, rec, sizeof rec);
            if (escaped_left) emit_escaped_piece();
            blobs_at += len;
            logical += logical_bytes;
            blobs++;
        }
        j++;
        record++;
    }
    bool step() override {
        if (escaped_left) {
            emit_escaped_piece();
            return true;
        }
        if (j == block.records) {
            if (b == f.blocks()) {
                if (ended) return false;
                ended = true;
                const std::string vertices_file = af::VERTICES_FILE;
                if (blocks_at != f.blocks_bytes)
                    af::malformed(std::string(af::POINTS_FILE) + " has bytes between its last block and its footer");
                if (blobs_at != f.blobs_bytes) af::malformed(vertices_file + " has bytes between its last blob and its footer");
                if (logical != f.logical_vertex_bytes)
                    af::malformed(vertices_file + " counts " + std::to_string(f.logical_vertex_bytes) +
                                  " bytes of logical blobs and its blobs hold " + std::to_string(logical));
                if (blobs != f.blobs)
                    af::malformed(vertices_file + " counts " + std::to_string(f.blobs) + " blobs and holds " +
                                  std::to_string(blobs));
                P.reset();
                V.reset();
                X.reset();
                return false;
            }
            next_block();
        }
        emit_record();
        return true;
    }
};

// Ports: 0 = the address points' records in, 1 = their footprints' logical blobs in; the three
// files out. A record is encoded once its footprint is all in: its blob goes to addr_vertices.bin
// then, its block to addr_points.bin when the block's last record has come, and an index row to
// addr_index.bin when its superblock's last block has. A record whose footprint is an escape,
// which its vertex_count and the footprint's first byte decide, is encoded once the footprint's
// header is in, and the footprint then passes through as it comes. The footers go out in finish.
struct Enc : Encoder {
    struct Sink {
        Writer* to;
        void operator()(const uint8_t* p, size_t n) const { to->put(p, n); }
    };
    std::unique_ptr<Writer> PW, VW, XW;
    af::Encoder<Sink, Sink, Sink> files;
    explicit Enc(const std::string& od)
        : Encoder("addr", od, 2),
          PW(new Writer(od + "/" + af::POINTS_FILE, BLOCKS_BUF)),
          VW(new Writer(od + "/" + af::VERTICES_FILE, BLOBS_BUF)),
          XW(new Writer(od + "/" + af::INDEX_FILE, INDEX_BUF)),
          files(Sink{PW.get()}, Sink{VW.get()}, Sink{XW.get()}) {}
    void pump() override {
        ByteQueue &P = in[0], &V = in[1];
        for (;;) {
            if (const uint64_t owed = files.escape_owed()) {
                const size_t k = size_t(std::min<uint64_t>(owed, V.size()));
                if (k) files.put_escape_bytes(V.data(), k);
                V.pop(k);
                if (k < owed) return waits(0, 1);  // the records after the escape's
            }
            if (P.size() < af::LOGICAL_STRIDE) return waits(1, 0);  // a footprint whose record is not in yet
            const uint8_t* rec = P.data();
            const uint32_t count = rd32(rec + af::VERTEX_COUNT_AT);
            if (!count) {
                files.add(rec, nullptr);
                P.pop(af::LOGICAL_STRIDE);
                continue;
            }
            if (V.size() < af::LOGICAL_HEADER) return waits(0, 1);
            const uint8_t* logical = V.data();
            // A header no footprint has is add's to refuse, with no vertex read.
            const bool footprint = logical[0] <= 3 && !logical[1];
            if (footprint && af::is_escape(logical[0], count)) {
                files.begin_escape(rec, logical);
                V.pop(af::LOGICAL_HEADER);
                P.pop(af::LOGICAL_STRIDE);
                continue;
            }
            const size_t bytes = footprint ? size_t(af::logical_blob_bytes(logical[0], count)) : af::LOGICAL_HEADER;
            if (V.size() < bytes) return waits(0, 1);  // its vertices are not all in yet
            files.add(rec, logical);
            V.pop(bytes);
            P.pop(af::LOGICAL_STRIDE);
        }
    }
    void finish() override {
        require_empty();
        if (const uint64_t owed = files.escape_owed())
            die("%s: the stream ends %llu bytes before its last footprint does", af::VERTICES_FILE, (unsigned long long)owed);
        files.finish();
        const auto points_footer = files.points_footer();
        const auto vertices_footer = files.vertices_footer();
        const auto index_footer = files.index_footer();
        PW->put(points_footer.data(), points_footer.size());
        VW->put(vertices_footer.data(), vertices_footer.size());
        XW->put(index_footer.data(), index_footer.size());
        PW->finish();
        VW->finish();
        XW->finish();
        PW.reset();
        VW.reset();
        XW.reset();
    }
};
}  // namespace addr

// ------------------------------------------------------------------ POI R2 + V2
// The POI files are R2 and V2 (poi_files.h) in every dir: these codecs turn them into today's
// logical records and vertex stream for the replay and back.
namespace poi {
// Vertices a step decodes or packs of a block too large to take whole: a multiple of 8, so each
// piece's bits start on a byte.
static const size_t VERTS_PER_STEP = 8192;

// The footer and dictionaries of an R2 file of size bytes.
inline poi_files::RowDecoder open_rows(const std::string& path, uint64_t size) {
    const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) die("open %s: %s", path.c_str(), strerror(errno));
    struct Close {
        int fd;
        ~Close() { close(fd); }
    } closer{fd};
    uint8_t f[poi_files::FOOTER_BYTES] = {};
    if (size >= poi_files::FOOTER_BYTES) pread_all(fd, f, poi_files::FOOTER_BYTES, size - poi_files::FOOTER_BYTES, path);
    const poi_files::Footer ft = poi_files::read_footer(f, size);
    std::vector<uint8_t> dicts(size_t(ft.dict_bytes()));
    pread_all(fd, dicts.data(), dicts.size(), ft.rows_end(), path);
    return poi_files::RowDecoder(ft, dicts.data());
}

// A dir's POI files' decoded sizes (Dec::decoded_size), from the R2 footer alone: 0 without the
// file.
inline uint64_t decoded_size(const std::string& dir, int port) {
    const std::string rp = dir + "/poi_records.bin";
    struct stat st;
    if (stat(rp.c_str(), &st) != 0) return 0;
    const uint64_t size = uint64_t(st.st_size);
    uint8_t f[poi_files::FOOTER_BYTES] = {};
    if (size >= poi_files::FOOTER_BYTES) {
        const int fd = open(rp.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) die("open %s: %s", rp.c_str(), strerror(errno));
        pread_all(fd, f, poi_files::FOOTER_BYTES, size - poi_files::FOOTER_BYTES, rp);
        close(fd);
    }
    const poi_files::Footer ft = poi_files::read_footer(f, size);
    return port == 0 ? poi_files::LOGICAL_STRIDE * ft.rows : ft.logical_vertex_bytes;
}

// Ports: 0 = poi_records (36 B), 1 = poi_vertices. A step: one record and its block, or a piece of a
// block too large to take whole. A dir without the two files decodes to none. vertices_only: the
// POI replay's vertex cursor (poi_replay.h), which reads port 1 alone (port 0 closed) and skips
// ahead: a block its pending skip covers whole passes undecoded.
struct Dec : Decoder {
    poi_files::RowDecoder rows;
    std::unique_ptr<Reader> R, V;
    uint64_t n = 0, i = 0, logical = 0, blocks_end = 0;
    bool vertices_only = false;
    // a block in pieces
    std::unique_ptr<poi_files::VertexDecoder> big;
    poi_files::Block blk;
    explicit Dec(const std::string& src) : Decoder("poi", {"poi_records.bin", "poi_vertices.bin"}) {
        const std::string rp = src + "/poi_records.bin", vp = src + "/poi_vertices.bin";
        struct stat st;
        const bool has_r = stat(rp.c_str(), &st) == 0, has_v = stat(vp.c_str(), &st) == 0;
        if (!has_r && !has_v) return;
        if (!has_r || !has_v) die("%s has one of poi_records.bin and poi_vertices.bin", src.c_str());
        rows = open_rows(rp, file_size(rp));
        n = rows.footer().rows;
        R.reset(new Reader(rp, 32 << 10, 0, rows.footer().rows_end()));
        V.reset(new Reader(vp));
        if (V->size() < poi_files::VERTEX_PAD) poi_files::malformed("poi_vertices.bin is shorter than its pad");
        blocks_end = V->size() - poi_files::VERTEX_PAD;
    }
    uint64_t decoded_size(int port) const override {
        if (!R) return 0;
        return port == 0 ? poi_files::LOGICAL_STRIDE * n : rows.footer().logical_vertex_bytes;
    }
    bool step() override {
        if (big) {
            const size_t vs = poi_files::vertex_bytes(blk.encoding);
            const uint64_t k = big->done(), c = std::min<uint64_t>(VERTS_PER_STEP, blk.m - k);
            const size_t nb = size_t((c * (blk.wl + blk.wg) + 7) / 8);
            const uint8_t* p = nb ? V->take(nb) : nullptr;
            const bool last = k + c == blk.m;
            uint8_t* o = emit_reserve(1, size_t(c + (last && blk.closed)) * vs);
            big->decode(p, nb, c, o);
            if (last) {
                big->finish(nb ? p[nb - 1] : 0, o + c * vs);
                big.reset();
            }
            return true;
        }
        if (i == n) {
            if (!R) return false;
            rows.finish();
            if (V->tell() != blocks_end) poi_files::malformed("poi_vertices.bin has bytes past its last block");
            const uint8_t* pad = V->take(poi_files::VERTEX_PAD);
            for (size_t k = 0; k < poi_files::VERTEX_PAD; k++)
                if (pad[k]) poi_files::malformed("poi_vertices.bin's pad is not zero");
            if (logical != rows.footer().logical_vertex_bytes)
                poi_files::malformed("the vertex stream is " + std::to_string(logical) + " bytes, the footer gives " +
                                     std::to_string(rows.footer().logical_vertex_bytes));
            R.reset();
            V.reset();
            return false;
        }
        uint8_t rec[poi_files::LOGICAL_STRIDE];
        bool polygon;
        const uint32_t voff = rows.decode(R->take(poi_files::ROW_BYTES), rec, polygon);
        if (voff != V->tell())
            poi_files::malformed("row " + std::to_string(i) + ": voff " + std::to_string(voff) + " where the blocks are at " +
                                 std::to_string(V->tell()));
        i++;
        if (!polygon) {
            emit(0, rec, poi_files::LOGICAL_STRIDE);
            return true;
        }
        const uint64_t left = blocks_end - std::min(V->tell(), blocks_end);
        const uint8_t* h = V->need(poi_files::MAX_BLOCK_HEADER, true);
        blk = poi_files::read_block_header(h, size_t(std::min<uint64_t>(left, poi_files::MAX_BLOCK_HEADER)), rd32(rec), rd32(rec + 4));
        if (blk.bytes() > left) poi_files::malformed("row " + std::to_string(i - 1) + ": a block past the end of poi_vertices.bin");
        if (blk.logical_bytes() > rows.footer().logical_vertex_bytes - logical) poi_files::past_vertex_stream(i - 1, rows.footer());
        if (logical >= NO) poi_files::malformed("a logical vertex stream of 4 GiB or more");
        if (vertices_only && drop[1] >= blk.logical_bytes()) {
            logical += blk.logical_bytes();
            V->seek(V->tell() + blk.bytes());
            drop[1] -= blk.logical_bytes();
            if (!drop[1]) dropping--;
            return true;
        }
        wr32(rec + poi_files::OFF_VERTEX_OFFSET, uint32_t(logical));
        wr32(rec + poi_files::OFF_VERTEX_COUNT, uint32_t(blk.vertex_count()));
        emit(0, rec, poi_files::LOGICAL_STRIDE);
        logical += blk.logical_bytes();
        V->skip(blk.header);
        const size_t bvs = poi_files::vertex_bytes(blk.encoding);
        if (blk.logical_bytes() <= big_block()) {
            const size_t pb = size_t(blk.payload());
            const uint8_t* p = pb ? V->take(pb) : nullptr;
            uint8_t* o = emit_reserve(1, size_t(blk.logical_bytes()));
            poi_files::logical_header(blk, o);
            poi_files::VertexDecoder vd(blk);
            vd.decode(p, pb, blk.m, o + poi_files::LOGICAL_HEADER);
            vd.finish(pb ? p[pb - 1] : 0, o + poi_files::LOGICAL_HEADER + blk.m * bvs);
            return true;
        }
        uint8_t hdr[poi_files::LOGICAL_HEADER];
        poi_files::logical_header(blk, hdr);
        emit(1, hdr, poi_files::LOGICAL_HEADER);
        big.reset(new poi_files::VertexDecoder(blk));
        return true;
    }
};

// Ports as the decoder's. One pass: a record's row as it comes (its block starts where the V2 file
// is), its block once the block is all in. A block too large to take whole goes to the V2 file as
// it arrives, its vertices as they are VERTEX_RAW bytes past the block's start (room for the
// longest header), then is packed in place, front to back: a vertex's bits never take more bytes
// than its raw bytes, so the packed bytes never reach the raw ones still to be read.
struct Enc : Encoder {
    std::unique_ptr<Writer> O, RW;
    poi_files::RowEncoder rows;
    std::vector<uint8_t> blk;
    uint64_t logical = 0;
    std::string opath;
    // a block in pieces
    bool big = false;
    poi_files::Block bb;
    uint64_t big_vc = 0, big_k = 0, big_start = 0, big_size = 0;
    uint32_t big_lat = 0, big_lng = 0, max_l = 0, max_g = 0;
    uint8_t first[8], last[8];
    std::unique_ptr<Writer> raw;
    explicit Enc(const std::string& od) : Encoder("poi", od, 2) {
        opath = od + "/poi_vertices.bin";
        O.reset(new Writer(opath, WRITE_BUF, true));
        RW.reset(new Writer(od + "/poi_records.bin", 32 << 10));
    }
    void finish_big() {
        const size_t vs = poi_files::vertex_bytes(bb.encoding);
        raw->finish();
        raw.reset();
        bb.closed = big_vc >= 2 && memcmp(first, last, vs) == 0;
        bb.m = big_vc - bb.closed;
        poi_files::set_widths(bb, max_l, max_g);
        uint8_t h[poi_files::MAX_BLOCK_HEADER];
        const size_t hl = poi_files::write_block_header(bb, big_lat, big_lng, h);
        O->skip_to(big_start);
        O->put(h, hl);
        const uint64_t raw_at = big_start + poi_files::MAX_BLOCK_HEADER;
        std::vector<uint8_t> tmp(VERTS_PER_STEP * vs);
        poi_files::BitPacker pack([&](uint8_t c) { O->u8(c); });
        for (uint64_t k = 0; k < bb.m;) {
            const uint64_t c = std::min<uint64_t>(VERTS_PER_STEP, bb.m - k);
            pread_all(O->fd, tmp.data(), size_t(c * vs), raw_at + k * vs, opath);
            for (uint64_t j = 0; j < c; j++) {
                pack.add(poi_files::q_at(&tmp[j * vs], bb.encoding, 0), bb.wl);
                pack.add(poi_files::q_at(&tmp[j * vs], bb.encoding, 1), bb.wg);
            }
            k += c;
        }
        pack.finish();
        logical += big_size;
        big = false;
    }
    // A record whose block is an old one as it is (the POI replay copies the blocks a patch leaves
    // whole): put_coded_row(rec) with nothing queued, the block's bytes through put_coded_bytes,
    // then end_coded(its logical bytes).
    void put_coded_row(const uint8_t* rec) {
        if (big || in[0].size() || in[1].size()) die("poi: a copied block among queued records");
        const uint32_t vo = rd32(rec + poi_files::OFF_VERTEX_OFFSET);
        if (vo != logical) die("poi_records %llu: polygons not back to back in record order", (unsigned long long)rows.rows());
        uint8_t row[poi_files::ROW_BYTES];
        rows.encode(rec, O->tell(), row);
        RW->put(row, poi_files::ROW_BYTES);
    }
    void put_coded_bytes(const uint8_t* p, size_t n) { O->put(p, n); }
    void end_coded(uint64_t logical_bytes) { logical += logical_bytes; }
    void pump() override {
        ByteQueue &R = in[0], &V = in[1];
        for (;;) {
            if (big) {
                const size_t vs = poi_files::vertex_bytes(bb.encoding);
                while (V.size() >= vs && big_k < big_vc) {
                    const uint8_t* v = V.data();
                    if (big_k == 0) memcpy(first, v, vs);
                    memcpy(last, v, vs);
                    max_l = std::max(max_l, poi_files::q_at(v, bb.encoding, 0));
                    max_g = std::max(max_g, poi_files::q_at(v, bb.encoding, 1));
                    raw->put(v, vs);
                    V.pop(vs);
                    big_k++;
                }
                if (big_k < big_vc) return waits(0, 1);  // the records after the block's
                finish_big();
                continue;
            }
            if (R.size() < poi_files::LOGICAL_STRIDE) return waits(1, 0);  // vertices whose record is not in yet
            const uint8_t* r = R.data();
            const uint32_t vo = rd32(r + poi_files::OFF_VERTEX_OFFSET), vc = rd32(r + poi_files::OFF_VERTEX_COUNT);
            size_t size = 0;
            uint8_t e = 0;
            if (vc && vo != NO) {
                if (V.size() < 1) return waits(0, 1);
                e = V.data()[0];
                if (e > 3) die("poi_vertices: bad encoding %u", e);
                size = unit_size(1, poi_files::LOGICAL_HEADER, vc, poi_files::vertex_bytes(e));
                if (size <= big_block() ? V.size() < size : V.size() < poi_files::LOGICAL_HEADER) return waits(0, 1);
            }
            uint8_t row[poi_files::ROW_BYTES];
            rows.encode(r, O->tell(), row);
            RW->put(row, poi_files::ROW_BYTES);
            const uint32_t lat = rd32(r), lng = rd32(r + 4);
            R.pop(poi_files::LOGICAL_STRIDE);
            if (!vc) continue;
            if (vo != logical) die("poi_records %llu: polygons not back to back in record order", (unsigned long long)(rows.rows() - 1));
            const uint8_t* s = V.data();
            if (size <= big_block()) {
                blk.clear();
                poi_files::encode_block(s, vc, lat, lng, blk);
                O->put(blk);
                V.pop(size);
                logical += size;
                continue;
            }
            if (s[1] != 0) die("poi_vertices: non-zero pad byte at %llu", (unsigned long long)logical);
            bb = poi_files::Block();
            bb.encoding = e;
            bb.bmin_lat = rd32(s + 2);
            bb.bmin_lng = rd32(s + 6);
            V.pop(poi_files::LOGICAL_HEADER);
            big = true;
            big_vc = vc;
            big_k = 0;
            big_size = size;
            big_lat = lat;
            big_lng = lng;
            max_l = max_g = 0;
            O->flush();
            big_start = O->tell();
            raw.reset(new Writer(O->fd, opath, big_start + poi_files::MAX_BLOCK_HEADER, PLANE_BUF));
        }
    }
    void finish() override {
        require_empty();
        if (big) die("poi_vertices: stream ends inside a block");
        const std::vector<uint8_t> t = rows.tail(logical);
        RW->put(t);
        const uint8_t pad[poi_files::VERTEX_PAD] = {};
        O->put(pad, poi_files::VERTEX_PAD);
        O->flush();
        // A block packed in place leaves raw bytes past the end when it was the last.
        if (ftruncate(O->fd, off_t(O->tell()))) die("truncate %s: %s", opath.c_str(), strerror(errno));
        O->finish();
        RW->finish();
        O.reset();
        RW.reset();
    }
};
}  // namespace poi

// ------------------------------------------------------------------ places (place_files.h)
namespace place {
namespace pf = place_files;
// The codec's buffers: the place files stream front to back in units of a block (under 1 KiB) or a run
// of hidden rows, so a few KiB each do; the codec holds about 56 KiB.
static const size_t SMALL_BUF = size_t(4) << 10, BLOCKS_BUF = size_t(16) << 10;

// A dir's place files' decoded size, LOGICAL_STRIDE a node, from place_nodes.bin's footer and
// place_hidden.bin's size alone: 0 without place_nodes.bin.
inline uint64_t decoded_size(const std::string& dir) {
    const std::string np = dir + "/" + pf::NODES_FILE, hp = dir + "/" + pf::HIDDEN_FILE;
    struct stat st;
    if (stat(np.c_str(), &st) != 0) return 0;
    const uint64_t size = uint64_t(st.st_size);
    uint8_t f[pf::FOOTER_BYTES] = {};
    if (size >= pf::FOOTER_BYTES) {
        const int fd = open(np.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) die("open %s: %s", np.c_str(), strerror(errno));
        pread_all(fd, f, pf::FOOTER_BYTES, size - pf::FOOTER_BYTES, np);
        close(fd);
    }
    const pf::Footer ft = pf::read_footer(f, size);
    const uint64_t hidden = stat(hp.c_str(), &st) == 0 ? uint64_t(st.st_size) / pf::HIDDEN_ROW_BYTES : 0;
    return pf::LOGICAL_STRIDE * (uint64_t(ft.rows) + hidden);
}

// Port 0 = place_nodes.bin's logical stream (pf::LogicalPlace). A step: a block of visible rows, or
// a run of hidden rows. A dir without the three files decodes to none.
struct Dec : Decoder {
    static constexpr size_t HIDDEN_PER_STEP = SMALL_BUF / pf::HIDDEN_ROW_BYTES;
    std::unique_ptr<Reader> C, N, T, H;  // place_cells.bin, place_nodes.bin's blocks and its table, place_hidden.bin
    pf::Footer f;
    std::unique_ptr<pf::CellCursor> cells;
    size_t b = 0;
    uint64_t at = 0;
    explicit Dec(const std::string& src) : Decoder("places", {pf::NODES_FILE}) {
        const std::string cp = src + "/" + pf::CELLS_FILE, np = src + "/" + pf::NODES_FILE, hp = src + "/" + pf::HIDDEN_FILE;
        struct stat st;
        const int has = (stat(cp.c_str(), &st) == 0) + (stat(np.c_str(), &st) == 0) + (stat(hp.c_str(), &st) == 0);
        if (!has) return;
        if (has != 3) die("%s has some of the place files", src.c_str());
        const uint64_t size = file_size(np), cs = file_size(cp), hs = file_size(hp);
        uint8_t ft[pf::FOOTER_BYTES] = {};
        if (size >= pf::FOOTER_BYTES) {
            Reader footer(np, pf::FOOTER_BYTES, size - pf::FOOTER_BYTES);
            memcpy(ft, footer.take(pf::FOOTER_BYTES), pf::FOOTER_BYTES);
        }
        f = pf::read_footer(ft, size);
        if (cs % pf::CELL_ROW_BYTES) pf::malformed("place_cells.bin is not whole rows");
        if (hs % pf::HIDDEN_ROW_BYTES) pf::malformed("place_hidden.bin is not whole rows");
        cells.reset(new pf::CellCursor(f.rows, cs / pf::CELL_ROW_BYTES));
        C.reset(new Reader(cp, SMALL_BUF));
        N.reset(new Reader(np, BLOCKS_BUF, 0, f.table_at));
        T.reset(new Reader(np, SMALL_BUF, f.table_at, size - pf::FOOTER_BYTES));
        H.reset(new Reader(hp, SMALL_BUF));
    }
    uint64_t decoded_size(int) const override {
        if (!N) return 0;
        return pf::LOGICAL_STRIDE * (uint64_t(f.rows) + H->size() / pf::HIDDEN_ROW_BYTES);
    }
    bool step() override {
        if (!N) return false;
        if (b < f.blocks) {
            const pf::BlockEntry e = pf::read_entry(T->take(pf::TABLE_ENTRY_BYTES));
            const size_t rows = f.rows_of(b), bytes = e.bytes(rows);
            if (e.offset != at) pf::malformed("block " + std::to_string(b) + " is not where the blocks before it end");
            if (at + bytes > f.table_at) pf::malformed("block " + std::to_string(b) + " runs into the block table");
            const uint8_t* p = N->take(bytes);
            uint8_t* o = emit_reserve(0, rows * pf::LOGICAL_STRIDE);
            pf::decode_block(
                b, e, rows, p, [&](uint32_t row) { return cells->key_of(row, [&] { return C->take(pf::CELL_ROW_BYTES); }); },
                [&](const pf::LogicalPlace& pl) {
                    memcpy(o, &pl, pf::LOGICAL_STRIDE);
                    o += pf::LOGICAL_STRIDE;
                });
            at += bytes;
            b++;
            return true;
        }
        if (!H->eof()) {
            const size_t n = size_t(std::min<uint64_t>(H->left() / pf::HIDDEN_ROW_BYTES, HIDDEN_PER_STEP));
            const uint8_t* p = H->take(n * pf::HIDDEN_ROW_BYTES);
            uint8_t* o = emit_reserve(0, n * pf::LOGICAL_STRIDE);
            for (size_t k = 0; k < n; k++) {
                const pf::LogicalPlace pl = pf::hidden_place(p + k * pf::HIDDEN_ROW_BYTES);
                memcpy(o + k * pf::LOGICAL_STRIDE, &pl, pf::LOGICAL_STRIDE);
            }
            return true;
        }
        if (at != f.table_at) pf::malformed("place_nodes.bin has bytes between its blocks and its table");
        cells->finish();
        C.reset();
        N.reset();
        T.reset();
        H.reset();
        return false;
    }
};

// Port 0 = the logical stream in; the three files out. The blocks go to place_nodes.bin as they are
// packed and their table entries to a scratch file beside it, which finish appends to it with the
// footer: nothing grows with the set but the files.
struct Enc : Encoder {
    using Sink = std::function<void(const uint8_t*, size_t)>;
    std::unique_ptr<Writer> CW, NW, HW, TW;
    std::string tpath;
    pf::Encoder<Sink> enc;
    explicit Enc(const std::string& od)
        : Encoder("places", od, 1),
          CW(new Writer(od + "/" + pf::CELLS_FILE, SMALL_BUF)),
          NW(new Writer(od + "/" + pf::NODES_FILE, BLOCKS_BUF)),
          HW(new Writer(od + "/" + pf::HIDDEN_FILE, SMALL_BUF)),
          TW(new Writer(od + "/.place_nodes.table", SMALL_BUF, true)),
          tpath(od + "/.place_nodes.table"),
          enc([this](const uint8_t* p, size_t n) { CW->put(p, n); }, [this](const uint8_t* p, size_t n) { NW->put(p, n); },
              [this](const uint8_t* p, size_t n) { HW->put(p, n); }) {}
    void spill() {
        if (enc.table().empty()) return;
        for (const auto& e : enc.table()) TW->put(e.data(), e.size());
        enc.clear_table();
    }
    void add(const uint8_t* row) {
        pf::LogicalPlace p;
        memcpy(&p, row, pf::LOGICAL_STRIDE);
        enc.add(p);
        spill();
    }
    // Every byte straight from the writer (nothing queued): a row a write cuts waits in carry.
    uint8_t carry[pf::LOGICAL_STRIDE];
    size_t carried = 0;
    size_t consume(int, const uint8_t* p, size_t n) override {
        size_t used = 0;
        if (carried) {
            used = std::min(pf::LOGICAL_STRIDE - carried, n);
            memcpy(carry + carried, p, used);
            carried += used;
            if (carried < pf::LOGICAL_STRIDE) return n;
            add(carry);
            carried = 0;
        }
        for (; n - used >= pf::LOGICAL_STRIDE; used += pf::LOGICAL_STRIDE) add(p + used);
        memcpy(carry, p + used, n - used);
        carried = n - used;
        return n;
    }
    void pump() override {
        ByteQueue& q = in[0];
        while (q.size() >= pf::LOGICAL_STRIDE) {
            add(q.data());
            q.pop(pf::LOGICAL_STRIDE);
        }
    }
    void finish() override {
        require_empty();
        if (carried) die("places: the stream ends %zu bytes into a row", carried);
        enc.finish();
        spill();
        TW->flush();
        std::vector<uint8_t> buf(BLOCKS_BUF);
        for (uint64_t off = 0, n = TW->tell(); off < n;) {
            const size_t k = size_t(std::min<uint64_t>(buf.size(), n - off));
            pread_all(TW->fd, buf.data(), k, off, tpath);
            NW->put(buf.data(), k);
            off += k;
        }
        const auto footer = enc.footer();
        NW->put(footer.data(), footer.size());
        TW->finish();
        if (unlink(tpath.c_str())) die("unlink %s: %s", tpath.c_str(), strerror(errno));
        CW->finish();
        NW->finish();
        HW->finish();
        TW.reset();
        CW.reset();
        NW.reset();
        HW.reset();
    }
};
}  // namespace place

}  // namespace gcs
