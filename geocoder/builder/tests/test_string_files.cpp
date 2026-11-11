// The string tiers' codec (src/string_files.h): random pools (empty strings,
// strings of the longest length, prefixes shared across bucket cuts) round
// trip through FCC2 and cut where the rule says; the encoder given its pool in
// any pieces or a whole string at a time, the patcher's SpillWriter and the
// builder's write_file write the same file; a small pool's file is the bytes the format gives
// (server/src/strings.rs reads the same ones); and the codec refuses each pool
// it can't hold and each file it never writes.
#include "string_files.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <functional>
#include <random>
#include <string>
#include <vector>

#include "scratch_dir.h"
#include "test_framework.h"

using gctest::refusal_of;

namespace {

std::string pool_of(const std::vector<std::string>& strings) {
    std::string p;
    for (const std::string& s : strings) p.append(s).push_back('\0');
    return p;
}

std::vector<uint8_t> encode(const std::string& pool) { return string_files::encode(pool.data(), pool.size()); }

std::string decode(const std::vector<uint8_t>& f) {
    std::string out;
    string_files::decode(f.data(), f.size(), [&](const char* p, size_t n) { out.append(p, n); });
    return out;
}

std::vector<uint8_t> hex(const char* h) {
    std::vector<uint8_t> out;
    for (size_t i = 0; h[i] && h[i + 1]; i += 2) out.push_back(uint8_t(std::stoi(std::string(h + i, 2), nullptr, 16)));
    return out;
}

// The pool's buckets by the rule, from a plain walk of its strings: each
// bucket's span.
std::vector<uint32_t> spans_by_rule(const std::vector<std::string>& strings) {
    std::vector<uint32_t> spans;
    size_t held = 0;
    for (const std::string& s : strings) {
        const uint64_t h = string_files::fnv1a(s.data(), s.size());
        if (held && (held + s.size() + 1 > 512 || (held >= 128 && (h >> 32) % 8 == 0))) {
            spans.push_back(uint32_t(held));
            held = 0;
        }
        held += s.size() + 1;
    }
    if (held) spans.push_back(uint32_t(held));
    return spans;
}

std::vector<uint32_t> spans_of(const std::vector<uint8_t>& f) {
    const string_files::Footer ft = string_files::read_footer(f.data(), f.size());
    std::vector<uint32_t> spans;
    for (uint32_t b = 0; b < ft.buckets; b++) spans.push_back(string_files::rd16(&f[ft.body + 4 * size_t(b)]));
    return spans;
}

// A random pool: names with shared prefixes in runs (sorted or not), empty
// strings, and now and then a string of up to the longest length.
std::vector<std::string> random_strings(std::mt19937& rng, size_t n) {
    std::vector<std::string> out;
    std::string stem = "Rue";
    for (size_t i = 0; i < n; i++) {
        const uint32_t r = rng() % 100;
        if (r < 5) {
            out.emplace_back();
        } else if (r < 7) {
            const size_t len = r == 5 ? string_files::MAX_STRING - rng() % 3 : 400 + rng() % 700;
            out.emplace_back(len, char('a' + rng() % 26));
        } else {
            if (rng() % 8 == 0) stem = std::string(1, char('A' + rng() % 26)) + std::to_string(rng() % 1000);
            std::string s = stem + " " + std::to_string(rng() % (r < 50 ? 10 : 100000));
            if (rng() % 16 == 0) s += "\xC3\xA9";  // UTF-8 bytes past ASCII
            out.push_back(s);
        }
    }
    return out;
}

}  // namespace

TEST(string_files_round_trip_random_pools_and_cut_by_the_rule) {
    std::mt19937 rng(20261110);
    for (int round = 0; round < 60; round++) {
        const size_t n = round == 0 ? 0 : round < 4 ? size_t(round) : 1 + rng() % 4000;
        std::vector<std::string> strings = random_strings(rng, n);
        if (round % 3 == 1) std::sort(strings.begin(), strings.end());
        const std::string pool = pool_of(strings);
        const std::vector<uint8_t> f = encode(pool);
        REQUIRE(decode(f) == pool);
        CHECK(spans_of(f) == spans_by_rule(strings));
        const string_files::Footer ft = string_files::read_footer(f.data(), f.size());
        CHECK_EQ(ft.raw_size, uint32_t(pool.size()));
        CHECK_EQ(f.size(), size_t(ft.body + 4 * size_t(ft.buckets) + string_files::FOOTER_BYTES));
    }
}

TEST(string_files_shared_prefixes_across_a_cut_start_the_bucket_whole) {
    // 400 names of one stem, sorted: each bucket's first string is written
    // whole though it shares the stem with the string before the cut.
    std::vector<std::string> strings;
    for (int i = 0; i < 400; i++) strings.push_back("Avenue des Champs " + std::to_string(1000 + i));
    const std::string pool = pool_of(strings);
    const std::vector<uint8_t> f = encode(pool);
    const string_files::Footer ft = string_files::read_footer(f.data(), f.size());
    REQUIRE(ft.buckets > 3);
    size_t coded = 0;
    size_t k = 0;
    for (uint32_t b = 0; b < ft.buckets; b++) {
        const uint32_t span = string_files::rd16(&f[ft.body + 4 * size_t(b)]);
        const uint32_t size = string_files::rd16(&f[ft.body + 4 * size_t(b) + 2]);
        const std::string& head = strings[k];
        CHECK_EQ(uint32_t(f[coded]), uint32_t(head.size()));
        CHECK(std::string(reinterpret_cast<const char*>(&f[coded + 1]), head.size()) == head);
        for (size_t held = 0; held < span; k++) held += strings[k].size() + 1;
        coded += size;
    }
    CHECK_EQ(k, strings.size());
    CHECK(decode(f) == pool);
}

TEST(string_files_encoder_writers_and_pieces_write_one_file) {
    std::mt19937 rng(7);
    const std::string pool = pool_of(random_strings(rng, 3000));
    const std::vector<uint8_t> want = encode(pool);
    // Any pieces, a NUL or a string split between two of them included.
    for (int round = 0; round < 8; round++) {
        std::vector<uint8_t> body, table;
        string_files::Encoder e([&](const uint8_t* c, size_t n, uint32_t span) {
            body.insert(body.end(), c, c + n);
            uint8_t t[4];
            string_files::put_entry(t, n, span);
            table.insert(table.end(), t, t + 4);
        });
        for (size_t at = 0; at < pool.size();) {
            const size_t n = std::min(pool.size() - at, size_t(round == 0 ? 1 : 1 + rng() % (round * 300)));
            e.write(pool.data() + at, n);
            at += n;
        }
        e.finish();
        uint8_t ft[12];
        string_files::put_footer(ft, e.buckets(), e.raw_size());
        body.insert(body.end(), table.begin(), table.end());
        body.insert(body.end(), ft, ft + 12);
        CHECK(body == want);
    }
    ScratchDir dir("string-files");
    string_files::write_file(dir.path() + "/built.bin", pool.data(), pool.size());
    FILE* out = fopen((dir.path() + "/patched.bin").c_str(), "wb");
    REQUIRE(out);
    {
        string_files::SpillWriter w(out, dir.path() + "/patched.table");
        for (size_t at = 0; at < pool.size(); at += 1000) w.write(pool.data() + at, std::min<size_t>(1000, pool.size() - at));
        w.finish();
    }
    REQUIRE(fclose(out) == 0);
    // The patcher's way: one whole string at a time.
    out = fopen((dir.path() + "/put.bin").c_str(), "wb");
    REQUIRE(out);
    {
        string_files::SpillWriter w(out, dir.path() + "/put.table");
        for (size_t at = 0; at < pool.size();) {
            const size_t len = strlen(pool.data() + at);
            w.put(pool.data() + at, len);
            at += len + 1;
        }
        w.finish();
    }
    REQUIRE(fclose(out) == 0);
    auto read = [&](const std::string& name) {
        std::vector<uint8_t> v;
        FILE* f = fopen((dir.path() + "/" + name).c_str(), "rb");
        for (int c; f && (c = fgetc(f)) != EOF;) v.push_back(uint8_t(c));
        if (f) fclose(f);
        return v;
    };
    CHECK(read("built.bin") == want);
    CHECK(read("patched.bin") == want);
    CHECK(read("put.bin") == want);
    CHECK(read("patched.table").empty());  // the spill is gone
    // The empty pool is the footer alone.
    CHECK(encode("") == hex("0000000000000000" "46434332"));
    CHECK(decode(hex("0000000000000000" "46434332")).empty());
}

TEST(string_files_small_pool_is_the_bytes_the_format_gives) {
    // An empty string and 16 names fill bucket 0 past 128 bytes; "name 16"
    // hashes to a cut (fnv1a64 >> 32 is 0 mod 8) and starts bucket 1 whole.
    // Berlin and Paris (7 and 4) cut nothing; 520 x's would take bucket 1 past
    // 512 bytes and sit alone; "b" follows it in a bucket of its own.
    std::vector<std::string> strings{""};
    for (int i = 0; i < 32; i++) strings.push_back("name " + std::string(i < 10 ? "0" : "") + std::to_string(i));
    for (const char* s : {"Berlin", "Paris"}) strings.push_back(s);
    strings.push_back(std::string(520, 'x'));
    strings.push_back("b");
    std::vector<uint8_t> want = hex(
        "0000076e616d6520303006013106013206013306013406013506013606013706013806013905023130060131060132060133060134060135"
        "076e616d65203136060137060138060139050232300601310601320601330601340601350601360601370601380601390502333006013100"
        "064265726c696e00055061726973"
        "8804");
    want.insert(want.end(), 520, 'x');
    const std::vector<uint8_t> tail = hex("0162" "810038008d00460009020a020200020004000000190300004643433" "2");
    want.insert(want.end(), tail.begin(), tail.end());
    CHECK(encode(pool_of(strings)) == want);
    CHECK(decode(want) == pool_of(strings));
}

TEST(string_files_encoder_refuses_pools_it_cant_hold) {
    const std::string longest(string_files::MAX_STRING, 'q');
    CHECK(decode(encode(pool_of({"a", longest, "b"}))) == pool_of({"a", longest, "b"}));
    const std::string too_long = "A string tier can't hold a string of 65533 bytes or more (at most 65532)";
    CHECK_EQ(refusal_of([&] { encode(pool_of({"a", longest + "q"})); }), too_long);
    // In pieces, the string's bytes so far already too many.
    CHECK_EQ(refusal_of([&] {
                 string_files::Encoder e([](const uint8_t*, size_t, uint32_t) {});
                 e.write(longest.data(), longest.size());
                 e.write("q", 1);
             }),
             too_long);
    // A whole string put while a piece written leaves a string open.
    CHECK_EQ(refusal_of([&] {
                 string_files::Encoder e([](const uint8_t*, size_t, uint32_t) {});
                 e.write("ab", 2);
                 e.put("c", 1);
             }),
             "A string tier can't hold a string put into the middle of another");
    CHECK_EQ(refusal_of([&] { encode(std::string("a\0b", 3)); }),
             std::string("A string tier can't hold a pool whose last string has no NUL"));
}

namespace {

// A file of the buckets (coded bytes, span) given, the footer's counts
// computed from them unless given.
std::vector<uint8_t> file_of(const std::vector<std::pair<std::vector<uint8_t>, uint32_t>>& buckets, int64_t raw = -1,
                             int64_t nb = -1) {
    std::vector<uint8_t> f, table;
    uint64_t sum = 0;
    for (const auto& [coded, span] : buckets) {
        f.insert(f.end(), coded.begin(), coded.end());
        uint8_t t[4];
        string_files::put_entry(t, coded.size(), span);
        table.insert(table.end(), t, t + 4);
        sum += span;
    }
    f.insert(f.end(), table.begin(), table.end());
    uint8_t ft[12];
    string_files::put_footer(ft, uint32_t(nb >= 0 ? nb : int64_t(buckets.size())), uint32_t(raw >= 0 ? raw : int64_t(sum)));
    f.insert(f.end(), ft, ft + 12);
    return f;
}

std::vector<uint8_t> bytes(const std::string& s) { return {s.begin(), s.end()}; }

std::string refusal(const std::vector<uint8_t>& f) {
    return refusal_of([&] { decode(f); });
}

}  // namespace

TEST(string_files_decoder_refuses_every_file_it_never_writes) {
    const std::string M = "Malformed string tier: ";
    // The canonical bytes decode: "ab", "abc" (lcp 2).
    CHECK_EQ(refusal(file_of({{bytes(std::string("\x02" "ab\x02\x01" "c", 6)), 7}})), std::string());
    CHECK_EQ(refusal({}), M + "a file of 0 bytes, shorter than its footer");
    std::vector<uint8_t> magic = encode(pool_of({"a"}));
    magic.back() = '3';
    CHECK_EQ(refusal(magic), M + "no FCC2 footer");
    CHECK_EQ(refusal(file_of({}, 0, 5)), M + "a table of 5 buckets past the file's start");
    CHECK_EQ(refusal(file_of({}, 3, 0)), M + "0 buckets of 3 bytes in a body of 0");
    CHECK_EQ(refusal(file_of({{bytes(std::string("\x01" "a", 2)), 2}}, 0)), M + "1 buckets of 0 bytes in a body of 2");
    // A bucket of no bytes or no span.
    CHECK_EQ(refusal(file_of({{bytes(std::string("\x01" "a", 2)), 0}}, 2)), M + "bucket 0 is empty");
    // A size past the body: the table entry says 3 coded bytes of 2.
    {
        std::vector<uint8_t> f = file_of({{bytes(std::string("\x01" "a", 2)), 2}});
        f[2 + 2] = 3;
        CHECK_EQ(refusal(f), M + "bucket 0 runs past the body");
    }
    CHECK_EQ(refusal(file_of({{bytes(std::string("\x01" "a", 2)), 2}}, 1)), M + "bucket 0 runs past the pool's bytes");
    // A varint in more bytes than it needs.
    CHECK_EQ(refusal(file_of({{bytes(std::string("\x81\x00" "a", 3)), 2}})), M + "bucket 0 has a bad varint");
    CHECK_EQ(refusal(file_of({{bytes(std::string("\x01" "a\x02\x00", 4)), 4}})), M + "bucket 0 shares more than the string before");
    CHECK_EQ(refusal(file_of({{bytes(std::string("\x03" "a", 2)), 4}})), M + "bucket 0 runs past its size");
    CHECK_EQ(refusal(file_of({{bytes(std::string("\x02" "ab", 3)), 2}})), M + "bucket 0 holds more than its span");
    CHECK_EQ(refusal(file_of({{bytes(std::string("\x02" "a\0", 3)), 3}})), M + "bucket 0 holds a NUL in a string");
    // "ab" then "ac" coded as lcp 0: it shares the "a".
    CHECK_EQ(refusal(file_of({{bytes(std::string("\x02" "ab\x00\x02" "ac", 7)), 6}})),
             M + "bucket 0 shares less than the string before");
    // Two buckets where the rule cuts nowhere, and one where it cuts past 512.
    CHECK_EQ(refusal(file_of({{bytes(std::string("\x01" "a", 2)), 2}, {bytes(std::string("\x01" "b", 2)), 2}})),
             M + "bucket 1 starts where no cut is");
    {
        std::vector<uint8_t> big = bytes(std::string("\x01" "a\x00\x88\x04", 5));
        big.insert(big.end(), 520, 'x');
        CHECK_EQ(refusal(file_of({{big, 2 + 521}})), M + "bucket 0 goes past a cut");
    }
    CHECK_EQ(refusal(file_of({{bytes(std::string("\x01" "a", 2)), 3}})), M + "bucket 0 holds less than its span");
    // The buckets' bytes short of the footer's, and a body byte no bucket takes.
    {
        std::vector<uint8_t> f = file_of({{bytes(std::string("\x01" "a", 2)), 2}});
        string_files::wr32(&f[f.size() - 8], 3);
        CHECK_EQ(refusal(f), M + "buckets of 2 bytes where the footer gives 3");
        std::vector<uint8_t> extra = file_of({{bytes(std::string("\x01" "a", 2)), 2}});
        extra.insert(extra.begin() + 2, 0);
        CHECK_EQ(refusal(extra), M + "a body of 3 bytes whose buckets take 2");
    }
    // The patcher names the file.
    CHECK_EQ(refusal_of([] { string_files::Decoder d(nullptr, 0, "old strings_core.bin"); }),
             std::string("Malformed old strings_core.bin: a file of 0 bytes, shorter than its footer"));
}
