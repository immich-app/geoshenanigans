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
// around it (the patcher copies the others as they are: Encoder::cut_before),
// and a bucket of two or more strings spans at most MAX_SPAN
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
#include <memory>
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
    if (at < end && p[at] < 0x80) {
        v = p[at++];
        return true;
    }
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
    explicit Encoder(OnBucket on_bucket)
        : on_bucket_(std::move(on_bucket)), bucket_(new uint8_t[MAX_BUCKET + 8]), prev_(new char[MAX_STRING]) {}

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
    // One whole string of len bytes, no NUL among them (its NUL not given),
    // between write()'s pieces of whole strings.
    void put(const char* s, size_t len) {
        if (!partial_.empty()) unencodable("a string put into the middle of another");
        add(s, len);
    }
    // Whether a bucket would start at the string of len bytes at s, were it
    // put next: the encoder holds none, or the cut rule ends the bucket it
    // holds before s, which then goes out. The rule reads only the bucket's
    // bytes and s, and past a cut the encoder holds nothing, so a run of
    // strings that starts at a cut and ends at one encodes to the same bucket
    // in any pool: an old bucket whose strings, cut before and cut after a
    // patch leaves alone goes through put_bucket as it is.
    bool cut_before(const char* s, size_t len) {
        if (!partial_.empty()) unencodable("a cut in the middle of a string");
        if (span_ && !cuts_before(span_, s, len)) return false;
        flush();
        return true;
    }
    // A bucket of this encoding (coded bytes and span) at a cut: out as it
    // is. The caller proves it is the bucket the strings it holds make here,
    // and that a cut follows it (the next string put is cut before, or the
    // pool ends).
    void put_bucket(const uint8_t* coded, size_t size, uint32_t span) {
        if (!partial_.empty() || span_) unencodable("a bucket put into the middle of another");
        if (raw_ + span > UINT32_MAX) unencodable("a pool of 4 GiB or more");
        if (buckets_ == UINT32_MAX) unencodable("2^32 buckets or more");
        on_bucket_(coded, size, span);
        raw_ += span;
        buckets_++;
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
    // The bucket's bytes go straight into bucket_ (a bucket of two strings or
    // more spans at most MAX_SPAN bytes, so its varints keep it within
    // MAX_BUCKET, as does a string alone of MAX_STRING bytes), and prev_ keeps
    // the string before: only the bytes past their common prefix change.
    void add(const char* s, size_t len) {
        if (len > MAX_STRING) too_long(len);
        if (raw_ + len + 1 > UINT32_MAX) unencodable("a pool of 4 GiB or more");
        if (span_ && cuts_before(span_, s, len)) flush();
        uint8_t* o = bucket_.get() + size_;
        size_t lcp = 0;
        if (span_) {
            lcp = common_prefix(s, prev_.get(), std::min(len, prev_len_));
            o += put_varint(o, lcp);
        }
        o += put_varint(o, len - lcp);
        memcpy(o, s + lcp, len - lcp);
        size_ = size_t(o - bucket_.get()) + (len - lcp);
        memcpy(prev_.get() + lcp, s + lcp, len - lcp);
        prev_len_ = len;
        span_ += len + 1;
        raw_ += len + 1;
    }
    // The bytes a and b share from their first, of at most m.
    static size_t common_prefix(const char* a, const char* b, size_t m) {
        size_t i = 0;
        for (; i + 8 <= m; i += 8) {
            uint64_t x, y;
            memcpy(&x, a + i, 8);
            memcpy(&y, b + i, 8);
            if (x != y) return i + size_t(__builtin_ctzll(x ^ y) >> 3);
        }
        while (i < m && a[i] == b[i]) i++;
        return i;
    }
    void flush() {
        if (!span_) return;
        if (buckets_ == UINT32_MAX) unencodable("2^32 buckets or more");
        on_bucket_(bucket_.get(), size_, uint32_t(span_));
        size_ = 0;
        span_ = 0;
        buckets_++;
    }

    OnBucket on_bucket_;
    std::unique_ptr<uint8_t[]> bucket_;
    std::unique_ptr<char[]> prev_;
    std::string partial_;
    size_t size_ = 0, prev_len_ = 0, span_ = 0;
    uint64_t raw_ = 0, buckets_ = 0;
};

inline void put_entry(uint8_t* e, size_t size, uint32_t span) {
    e[0] = uint8_t(span);
    e[1] = uint8_t(span >> 8);
    e[2] = uint8_t(size);
    e[3] = uint8_t(size >> 8);
}

// An encoding made in memory: strings and buckets go through encoder(), and
// finish() gives the file.
class BufferWriter {
public:
    BufferWriter()
        : enc_([this](const uint8_t* c, size_t n, uint32_t span) {
              body_.insert(body_.end(), c, c + n);
              uint8_t t[ENTRY_BYTES];
              put_entry(t, n, span);
              table_.insert(table_.end(), t, t + ENTRY_BYTES);
          }) {}
    BufferWriter(const BufferWriter&) = delete;
    BufferWriter& operator=(const BufferWriter&) = delete;
    Encoder& encoder() { return enc_; }
    std::vector<uint8_t> finish() {
        enc_.finish();
        std::vector<uint8_t> file = std::move(body_);
        file.insert(file.end(), table_.begin(), table_.end());
        uint8_t f[FOOTER_BYTES];
        put_footer(f, enc_.buckets(), enc_.raw_size());
        file.insert(file.end(), f, f + FOOTER_BYTES);
        return file;
    }

private:
    std::vector<uint8_t> body_, table_;
    Encoder enc_;
};

// A pool's encoding, in memory.
inline std::vector<uint8_t> encode(const char* pool, size_t size) {
    BufferWriter w;
    w.encoder().write(pool, size);
    return w.finish();
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
// patcher's constant memory). on_bucket, if given, sees each bucket as it
// goes out.
class SpillWriter {
public:
    SpillWriter(FILE* out, std::string spill_path, Encoder::OnBucket on_bucket = nullptr)
        : out_(out), spill_path_(std::move(spill_path)), on_bucket_(std::move(on_bucket)),
          enc_([this](const uint8_t* c, size_t n, uint32_t span) {
              ok_ &= fwrite(c, 1, n, out_) == n;
              uint8_t t[ENTRY_BYTES];
              put_entry(t, n, span);
              ok_ &= fwrite(t, 1, ENTRY_BYTES, spill_) == ENTRY_BYTES;
              if (on_bucket_) on_bucket_(c, n, span);
          }) {
        spill_ = fopen(spill_path_.c_str(), "w+b");
        if (!spill_) throw std::runtime_error("Cannot create " + spill_path_);
    }
    SpillWriter(const SpillWriter&) = delete;
    SpillWriter& operator=(const SpillWriter&) = delete;
    ~SpillWriter() { drop_spill(); }

    void write(const char* p, size_t n) { enc_.write(p, n); }
    // One whole string (Encoder::put).
    void put(const char* s, size_t len) { enc_.put(s, len); }
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
    uint32_t buckets() const { return enc_.buckets(); }
    // Strings and whole buckets (Encoder::cut_before, put_bucket).
    Encoder& encoder() { return enc_; }

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
    Encoder::OnBucket on_bucket_;
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

    // A bucket as its table entry places it, its bytes undecoded: its index,
    // where it starts in the body and in the pool, its coded bytes and span.
    struct Entry {
        uint32_t b = 0;
        uint64_t coded_at = 0, raw_at = 0;
        const uint8_t* coded = nullptr;
        uint32_t size = 0, span = 0;
    };
    // The next bucket's entry, checked as next() checks it; false past the
    // last (next() then ends the walk).
    bool peek(Entry& e) const { return entry_at(b_, coded_, raw_, e); }
    // The entry of the bucket after e, likewise.
    bool peek_after(const Entry& e, Entry& after) const {
        return entry_at(e.b + 1, e.coded_at + e.size, e.raw_at + e.span, after);
    }
    // Passes the next bucket (e, from peek) undecoded: a walk that copies it
    // as it is. Nothing checks its bytes; the file's hash does.
    void skip(const Entry& e) {
        if (e.b != b_ || e.coded_at != coded_ || e.raw_at != raw_) malformed(name_, "a bucket skipped out of turn");
        raw_start_ = raw_;
        raw_ += e.span;
        coded_ += e.size;
        prev_span_ = e.span;
        size_ = 0;
        b_++;
    }
    // A bucket's first string, which it holds whole: len bytes at s.
    void first_of(const Entry& e, const char*& s, size_t& len) const {
        size_t at = 0;
        uint32_t l = 0;
        if (!get_varint(e.coded, e.size, at, l)) bad(e.b, " has a bad varint");
        if (l > e.size - at) bad(e.b, " runs past its size");
        s = reinterpret_cast<const char*>(e.coded + at);
        len = l;
    }
    // A bucket's strings, counted by its varints, undecoded.
    uint32_t count_of(const Entry& e) const {
        size_t at = 0;
        uint32_t n = 0, lcp = 0, sl = 0;
        for (bool first = true; at < e.size; first = false, n++) {
            if ((!first && !get_varint(e.coded, e.size, at, lcp)) || !get_varint(e.coded, e.size, at, sl))
                bad(e.b, " has a bad varint");
            if (sl > e.size - at) bad(e.b, " runs past its size");
            at += sl;
        }
        return n;
    }

    // Decodes the next bucket into data() (its strings, NULs included, size()
    // bytes, until the next call); false past the last.
    bool next() {
        Entry ent;
        if (!peek(ent)) {
            size_ = 0;
            if (raw_ != ft_.raw_size)
                malformed(name_, "buckets of " + std::to_string(raw_) + " bytes where the footer gives " +
                          std::to_string(ft_.raw_size));
            if (coded_ != ft_.body)
                malformed(name_, "a body of " + std::to_string(ft_.body) + " bytes whose buckets take " + std::to_string(coded_));
            return false;
        }
        const uint32_t span = ent.span, csize = ent.size;
        const uint8_t* p = ent.coded;
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
    [[noreturn]] void bad(const char* what) const { bad(b_, what); }
    [[noreturn]] void bad(uint32_t b, const char* what) const { malformed(name_, "bucket " + std::to_string(b) + what); }
    bool entry_at(uint32_t b, uint64_t coded, uint64_t raw, Entry& e) const {
        if (b >= ft_.buckets) return false;
        const uint8_t* t = table_ + size_t(b) * ENTRY_BYTES;
        e.b = b;
        e.coded_at = coded;
        e.raw_at = raw;
        e.span = rd16(t);
        e.size = rd16(t + 2);
        if (e.span == 0 || e.size == 0) bad(b, " is empty");
        if (e.size > ft_.body - coded) bad(b, " runs past the body");
        if (e.span > uint64_t(ft_.raw_size) - raw) bad(b, " runs past the pool's bytes");
        e.coded = body_ + coded;
        return true;
    }

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
