// The string tiers as they sit on disk (strings_core.bin, strings_street.bin,
// strings_addr.bin, strings_postcode.bin, strings_poi.bin): FCC2, front-coded
// buckets cut by content. The builder writes them and the patcher rebuilds
// them through Encoder below; the server reads them in place
// (server/src/strings.rs). The patch tools diff and replay the logical form, a
// tier's pool: its strings back to back, each ended by a NUL. A string id is
// the offset of a string's first byte in that pool (strings_layout.json places
// the tiers in one id space), and FCC2 keeps the ids. The encoder's choices
// are fixed, so a pool has one encoding, and the decoder refuses any other.
//
// A tier's file:
//   the body: the buckets back to back. A bucket holds a run of the pool's
//     strings: the first as varint(len) and its len bytes; each next one as
//     varint(lcp), varint(slen) and its slen bytes past the lcp bytes it
//     shares with the string before it, lcp the most it shares;
//   the table: per bucket, u16 span (its strings' bytes, NULs included) then
//     u16 size (its bytes in the body);
//   footer, 12 bytes: the buckets (u32), the pool's bytes (u32), "FCC2".
// The cut (cuts_before): a bucket holding `held` bytes ends before the next
// string s once it holds at least MIN_CUT bytes and fnv1a64(s) >> 32 is
// 0 mod PERIOD, or when s would take it past MAX_SPAN. A bucket then starts
// where nearby content says, so an edit to the pool changes only the buckets
// around it, and a bucket of two or more strings spans at most MAX_SPAN
// bytes; a longer string sits alone in its bucket. Varints are LEB128 in their
// fewest bytes. An empty pool is the footer alone.
//
// The table goes after the body, so a writer writes the body front to back
// holding one bucket, and puts the table (4 bytes a bucket) aside until the
// body ends: the builder in memory, the patcher in a scratch file
// (SpillWriter). A reader sums the table once at open.
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

namespace string_files {

inline constexpr char MAGIC[4] = {'F', 'C', 'C', '2'};
inline constexpr size_t FOOTER_BYTES = 12, ENTRY_BYTES = 4;
// FCC2-256: the bucket target B = 256 bytes, cut at B / 2 to 2B, one string in
// B / 32 a cut point.
inline constexpr uint32_t MIN_CUT = 128, MAX_SPAN = 512, PERIOD = 8;
// The longest string: its bucket's span and size fit 16 bits.
inline constexpr size_t MAX_STRING = 65532;
inline constexpr size_t MAX_BUCKET = 0xFFFF;

// name: the file, or "string tier".
[[noreturn]] inline void malformed(const std::string& name, const std::string& what) {
    throw std::runtime_error("Malformed " + name + ": " + what);
}
// A pool the format can't hold.
[[noreturn]] inline void unencodable(const std::string& what) { throw std::runtime_error("A string tier can't hold " + what); }

inline uint32_t rd32(const uint8_t* p) { uint32_t v; memcpy(&v, p, 4); return v; }
inline uint32_t rd16(const uint8_t* p) { return uint32_t(p[0]) | uint32_t(p[1]) << 8; }
inline void wr32(uint8_t* p, uint32_t v) { memcpy(p, &v, 4); }

inline uint64_t fnv1a(const char* p, size_t n) {
    uint64_t h = 1469598103934665603ull;
    for (size_t i = 0; i < n; i++) {
        h ^= uint8_t(p[i]);
        h *= 1099511628211ull;
    }
    return h;
}

// Whether a bucket holding `held` bytes (one string or more) ends before the
// string of len bytes (no NUL) at p.
inline bool cuts_before(size_t held, const char* p, size_t len) {
    if (held + len + 1 > MAX_SPAN) return true;
    return held >= MIN_CUT && (fnv1a(p, len) >> 32) % PERIOD == 0;
}

inline size_t put_varint(uint8_t* o, uint64_t v) {
    size_t n = 0;
    for (; v >= 0x80; v >>= 7) o[n++] = uint8_t(v) | 0x80;
    o[n++] = uint8_t(v);
    return n;
}
// A varint of at most 3 bytes at p[at, end), in its fewest bytes: false for
// any other.
inline bool get_varint(const uint8_t* p, size_t end, size_t& at, uint32_t& v) {
    v = 0;
    for (int s = 0; s < 21; s += 7) {
        if (at >= end) return false;
        const uint8_t b = p[at++];
        v |= uint32_t(b & 0x7F) << s;
        if (!(b & 0x80)) return b != 0 || s == 0;
    }
    return false;
}

struct Footer {
    uint32_t buckets = 0, raw_size = 0;
    uint64_t body = 0;  // the body's bytes; the table follows them
};

inline void put_footer(uint8_t* f, uint32_t buckets, uint32_t raw_size) {
    wr32(f, buckets);
    wr32(f + 4, raw_size);
    memcpy(f + 8, MAGIC, 4);
}

// The footer of a file of `size` bytes at `file`, checked against the size.
inline Footer read_footer(const uint8_t* file, uint64_t size, const std::string& name = "string tier") {
    if (size < FOOTER_BYTES) malformed(name, "a file of " + std::to_string(size) + " bytes, shorter than its footer");
    const uint8_t* f = file + size - FOOTER_BYTES;
    if (memcmp(f + 8, MAGIC, 4) != 0) malformed(name, "no FCC2 footer");
    Footer ft;
    ft.buckets = rd32(f);
    ft.raw_size = rd32(f + 4);
    const uint64_t table = uint64_t(ft.buckets) * ENTRY_BYTES;
    if (table > size - FOOTER_BYTES) malformed(name, "a table of " + std::to_string(ft.buckets) + " buckets past the file's start");
    ft.body = size - FOOTER_BYTES - table;
    // Every bucket holds a string, of a byte at least, and a byte coded.
    if ((ft.buckets == 0) != (ft.raw_size == 0) || ft.raw_size < ft.buckets || ft.body < ft.buckets ||
        (ft.buckets == 0 && ft.body != 0))
        malformed(name, std::to_string(ft.buckets) + " buckets of " + std::to_string(ft.raw_size) + " bytes in a body of " +
                  std::to_string(ft.body));
    return ft;
}

// Encodes a pool given in pieces of any size, holding one bucket and the
// string it is reading: on_bucket(coded, size, span) for each bucket, in
// order.
class Encoder {
public:
    using OnBucket = std::function<void(const uint8_t* coded, size_t size, uint32_t span)>;
    explicit Encoder(OnBucket on_bucket) : on_bucket_(std::move(on_bucket)) { bucket_.reserve(MAX_SPAN + 64); }

    void write(const char* p, size_t n) {
        while (n) {
            const char* z = static_cast<const char*>(memchr(p, 0, n));
            if (!z) {
                take_partial(p, n);
                return;
            }
            const size_t k = size_t(z - p);
            if (partial_.empty()) {
                add(p, k);
            } else {
                take_partial(p, k);
                add(partial_.data(), partial_.size());
                partial_.clear();
            }
            p += k + 1;
            n -= k + 1;
        }
    }
    // The pool's end: refuses one whose last string has no NUL.
    void finish() {
        if (!partial_.empty()) unencodable("a pool whose last string has no NUL");
        flush();
    }
    uint32_t raw_size() const { return uint32_t(raw_); }
    uint32_t buckets() const { return uint32_t(buckets_); }

private:
    void take_partial(const char* p, size_t n) {
        if (partial_.size() + n > MAX_STRING) too_long(partial_.size() + n);
        partial_.insert(partial_.end(), p, p + n);
    }
    [[noreturn]] static void too_long(size_t n) {
        unencodable("a string of " + std::to_string(n) + " bytes or more (at most " + std::to_string(MAX_STRING) + ")");
    }
    void add(const char* s, size_t len) {
        if (len > MAX_STRING) too_long(len);
        if (raw_ + len + 1 > UINT32_MAX) unencodable("a pool of 4 GiB or more");
        if (span_ && cuts_before(span_, s, len)) flush();
        uint8_t v[8];
        size_t lcp = 0;
        if (span_) {
            const size_t m = std::min(len, prev_.size());
            while (lcp < m && s[lcp] == prev_[lcp]) lcp++;
            bucket_.insert(bucket_.end(), v, v + put_varint(v, lcp));
        }
        bucket_.insert(bucket_.end(), v, v + put_varint(v, len - lcp));
        bucket_.insert(bucket_.end(), reinterpret_cast<const uint8_t*>(s) + lcp, reinterpret_cast<const uint8_t*>(s) + len);
        prev_.assign(s, s + len);
        span_ += len + 1;
        raw_ += len + 1;
    }
    void flush() {
        if (!span_) return;
        if (buckets_ == UINT32_MAX) unencodable("2^32 buckets or more");
        on_bucket_(bucket_.data(), bucket_.size(), uint32_t(span_));
        bucket_.clear();
        span_ = 0;
        buckets_++;
    }

    OnBucket on_bucket_;
    std::vector<uint8_t> bucket_;
    std::string prev_, partial_;
    size_t span_ = 0;
    uint64_t raw_ = 0, buckets_ = 0;
};

inline void put_entry(uint8_t* e, size_t size, uint32_t span) {
    e[0] = uint8_t(span);
    e[1] = uint8_t(span >> 8);
    e[2] = uint8_t(size);
    e[3] = uint8_t(size >> 8);
}

// A pool's encoding, in memory.
inline std::vector<uint8_t> encode(const char* pool, size_t size) {
    std::vector<uint8_t> body, table;
    Encoder e([&](const uint8_t* c, size_t n, uint32_t span) {
        body.insert(body.end(), c, c + n);
        uint8_t t[ENTRY_BYTES];
        put_entry(t, n, span);
        table.insert(table.end(), t, t + ENTRY_BYTES);
    });
    e.write(pool, size);
    e.finish();
    body.insert(body.end(), table.begin(), table.end());
    uint8_t f[FOOTER_BYTES];
    put_footer(f, e.buckets(), e.raw_size());
    body.insert(body.end(), f, f + FOOTER_BYTES);
    return body;
}

// Writes a pool to path as FCC2 (the builder: the table is held, 4 bytes a
// bucket).
inline void write_file(const std::string& path, const char* pool, size_t size) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot create " + path);
    std::vector<uint8_t> table;
    bool ok = true;
    try {
        Encoder e([&](const uint8_t* c, size_t n, uint32_t span) {
            ok &= fwrite(c, 1, n, f) == n;
            uint8_t t[ENTRY_BYTES];
            put_entry(t, n, span);
            table.insert(table.end(), t, t + ENTRY_BYTES);
        });
        e.write(pool, size);
        e.finish();
        uint8_t ft[FOOTER_BYTES];
        put_footer(ft, e.buckets(), e.raw_size());
        ok &= fwrite(table.data(), 1, table.size(), f) == table.size();
        ok &= fwrite(ft, 1, FOOTER_BYTES, f) == FOOTER_BYTES;
    } catch (...) {
        fclose(f);
        remove(path.c_str());
        throw;
    }
    if (fclose(f) != 0 || !ok) throw std::runtime_error("Cannot write " + path);
}

// Writes a pool given in pieces as FCC2 to out, holding one bucket: the
// table goes to a scratch file at spill_path until finish appends it (the
// patcher's constant memory).
class SpillWriter {
public:
    SpillWriter(FILE* out, std::string spill_path)
        : out_(out), spill_path_(std::move(spill_path)), enc_([this](const uint8_t* c, size_t n, uint32_t span) {
              ok_ &= fwrite(c, 1, n, out_) == n;
              uint8_t t[ENTRY_BYTES];
              put_entry(t, n, span);
              ok_ &= fwrite(t, 1, ENTRY_BYTES, spill_) == ENTRY_BYTES;
          }) {
        spill_ = fopen(spill_path_.c_str(), "w+b");
        if (!spill_) throw std::runtime_error("Cannot create " + spill_path_);
    }
    SpillWriter(const SpillWriter&) = delete;
    SpillWriter& operator=(const SpillWriter&) = delete;
    ~SpillWriter() { drop_spill(); }

    void write(const char* p, size_t n) { enc_.write(p, n); }
    // The pool's end: appends the table and footer to out (which stays open).
    void finish() {
        enc_.finish();
        if (fflush(spill_) != 0 || fseek(spill_, 0, SEEK_SET) != 0) ok_ = false;
        std::vector<char> buf(64 << 10);
        for (size_t n; (n = fread(buf.data(), 1, buf.size(), spill_)) > 0;) ok_ &= fwrite(buf.data(), 1, n, out_) == n;
        ok_ &= !ferror(spill_);
        uint8_t ft[FOOTER_BYTES];
        put_footer(ft, enc_.buckets(), enc_.raw_size());
        ok_ &= fwrite(ft, 1, FOOTER_BYTES, out_) == FOOTER_BYTES;
        drop_spill();
        if (!ok_) throw std::runtime_error("Cannot write a string tier's table through " + spill_path_);
    }
    uint32_t raw_size() const { return enc_.raw_size(); }

private:
    void drop_spill() {
        if (!spill_) return;
        fclose(spill_);
        spill_ = nullptr;
        remove(spill_path_.c_str());
    }
    FILE* out_;
    std::string spill_path_;
    FILE* spill_ = nullptr;
    bool ok_ = true;
    Encoder enc_;
};

// Decodes a file bucket by bucket, front to back, holding one bucket, and
// refuses any file but the one encoding of its pool.
class Decoder {
public:
    Decoder(const uint8_t* file, uint64_t file_size, std::string name = "string tier")
        : name_(std::move(name)), ft_(read_footer(file, file_size, name_)), body_(file), table_(file + ft_.body) {
        buf_.resize(MAX_BUCKET + 1);
    }
    uint32_t raw_size() const { return ft_.raw_size; }
    uint32_t buckets() const { return ft_.buckets; }
    // Decodes the next bucket into data() (its strings, NULs included, size()
    // bytes, until the next call); false past the last.
    bool next() {
        if (b_ == ft_.buckets) {
            size_ = 0;
            if (raw_ != ft_.raw_size)
                malformed(name_, "buckets of " + std::to_string(raw_) + " bytes where the footer gives " +
                          std::to_string(ft_.raw_size));
            if (coded_ != ft_.body)
                malformed(name_, "a body of " + std::to_string(ft_.body) + " bytes whose buckets take " + std::to_string(coded_));
            return false;
        }
        const uint8_t* e = table_ + size_t(b_) * ENTRY_BYTES;
        const uint32_t span = rd16(e), csize = rd16(e + 2);
        if (span == 0 || csize == 0) bad(" is empty");
        if (csize > ft_.body - coded_) bad(" runs past the body");
        if (span > uint64_t(ft_.raw_size) - raw_) bad(" runs past the pool's bytes");
        const uint8_t* p = body_ + coded_;
        char* out = buf_.data();
        size_t at = 0, o = 0, prev = 0, prev_len = 0;
        for (bool first = true; at < csize; first = false) {
            uint32_t lcp = 0, sl = 0;
            if (!first && !get_varint(p, csize, at, lcp)) bad(" has a bad varint");
            if (!get_varint(p, csize, at, sl)) bad(" has a bad varint");
            if (lcp > prev_len) bad(" shares more than the string before");
            if (sl > csize - at) bad(" runs past its size");
            const size_t len = size_t(lcp) + sl;
            if (o + len + 1 > span) bad(" holds more than its span");
            if (memchr(p + at, 0, sl)) bad(" holds a NUL in a string");
            if (!first && lcp < prev_len && sl > 0 && char(p[at]) == out[prev + lcp])
                bad(" shares less than the string before");
            memcpy(out + o, out + prev, lcp);
            memcpy(out + o + lcp, p + at, sl);
            out[o + len] = 0;
            if (first ? b_ > 0 && !cuts_before(prev_span_, out + o, len) : cuts_before(o, out + o, len))
                bad(first ? " starts where no cut is" : " goes past a cut");
            at += sl;
            prev = o;
            prev_len = len;
            o += len + 1;
        }
        if (o != span) bad(" holds less than its span");
        raw_start_ = raw_;
        raw_ += span;
        coded_ += csize;
        prev_span_ = span;
        size_ = span;
        b_++;
        return true;
    }
    const char* data() const { return buf_.data(); }
    size_t size() const { return size_; }
    // Where the last bucket decoded starts in the pool.
    uint64_t raw_start() const { return raw_start_; }
    // The body's bytes decoded so far.
    uint64_t coded_end() const { return coded_; }
    // Where the bucket table starts in the file (the body's bytes), and the
    // bytes of it read so far: the decoder reads none of them again.
    uint64_t table_offset() const { return ft_.body; }
    uint64_t table_read() const { return uint64_t(b_) * ENTRY_BYTES; }

private:
    [[noreturn]] void bad(const char* what) const { malformed(name_, "bucket " + std::to_string(b_) + what); }

    std::string name_;
    Footer ft_;
    const uint8_t* body_;
    const uint8_t* table_;
    std::vector<char> buf_;
    uint32_t b_ = 0;
    uint64_t raw_ = 0, coded_ = 0, raw_start_ = 0;
    size_t size_ = 0, prev_span_ = 0;
};

// A file's pool, whole, through emit(bytes, n) a bucket at a time.
template <typename Emit>
void decode(const uint8_t* file, uint64_t size, Emit emit, const std::string& name = "string tier") {
    Decoder d(file, size, name);
    while (d.next()) emit(d.data(), d.size());
}

}  // namespace string_files
