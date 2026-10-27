// Combined geo index (port of max/verify/scripts/gxc.py, B = 128, first ids delta-coded, address
// counts inline) made exactly invertible: it rebuilds geo_cells.bin and the street / addr / interp
// entries files byte for byte. Rows are today's geo_cells rows (same cells, same order); a row's
// entry offsets are the prefix sums of the list sizes, so they are derived, not stored.
//
//   gxc_top.bin     per block of 128 rows (32 B): first cell u64 | gxc_rows offset u32 | street and
//                   interp stream offsets of the list in effect at the block start (NO_DATA if none) |
//                   next address id at the block start u32 | first ids of those two lists u32 u32
//   gxc_rows.bin    per row: header byte s | a << 2 | i << 4 | d << 6, then LEB128(step - 4) when d = 3
//                   (d: cell step in level-17 units minus 1; 0 on a block's first row), then the
//                   address payload
//   gxc_street.bin, gxc_interp.bin   the stored lists back to back: LEB128(count - 1),
//                   zigzag LEB128(first id - previous stored list's first id), then LEB128 gaps
//   s, i (list state): 0 none | 1 next stored list, gaps stored minus 1 (strictly increasing) |
//                   2 same list as the table's previous present row | 3 next stored list, gaps stored
//                   as is (the list repeats an id)
//   a (address state; the sort step numbers addresses by the first row listing them, so a row's
//   own addresses are the next ids):
//                   0 none | 1 LEB128(k): ids next .. next+k-1 |
//                   2 LEB128(k), LEB128(D), D x (LEB128(index gap), LEB128(copies - 2)): the same
//                     range with ids repeated |
//                   3 LEB128(k), LEB128(count - 1), zigzag LEB128(first - next), LEB128 gaps: any
//                     list (an address first listed by an earlier row); next advances by k
// Additions to gxc.py (which dropped repeated ids, and stored address counts only): list states 3
// and address states 2 and 3. Lists of rows without them are byte-identical to gxc.py's.
#pragma once
#include "io.h"

namespace gxc {

static const uint64_t B = 128;
static const int CELL_SHIFT = 27;  // level-17 cell id step
static const char* ENTRY[3] = {"street_entries.bin", "addr_entries.bin", "interp_entries.bin"};
static const char* STREAM[2] = {"gxc_street.bin", "gxc_interp.bin"};

struct __attribute__((packed)) Top { uint64_t cell; uint32_t roff, s, i, afirst, sprev, iprev; };
static_assert(sizeof(Top) == 32, "Top");

static void read_list(Reader& E, uint64_t& eoff, uint32_t off, std::vector<uint32_t>& list, const char* name) {
    if (eoff > 0xFFFFFFFFull || uint32_t(eoff) != off)
        die("%s: list offset %u is not the running offset %llu", name, off, (unsigned long long)eoff);
    const uint16_t c = E.u16();
    if (!c) die("%s: empty list", name);
    list.resize(c);
    memcpy(list.data(), E.take(4 * size_t(c)), 4 * size_t(c));
    eoff += 2 + 4 * uint64_t(c);
}

inline void encode(const std::string& src, const std::string& out, Manifest& m) {
    Reader G(src + "/geo_cells.bin");
    if (G.size() % 20) die("geo_cells.bin size");
    const uint64_t R = G.size() / 20;
    Reader* E[3];
    for (int t = 0; t < 3; t++) E[t] = new Reader(src + "/" + ENTRY[t]);
    Writer top(out + "/gxc_top.bin"), rows(out + "/gxc_rows.bin");
    Writer* st[2] = {new Writer(out + "/" + STREAM[0]), new Writer(out + "/" + STREAM[1])};
    uint64_t eoff[3] = {0, 0, 0};
    std::vector<uint32_t> list, prev[2];
    bool have_prev[2] = {false, false};
    uint32_t cur_off[2] = {NO, NO}, cur_first[2] = {0, 0};
    uint64_t next = 0, prev_cell = 0;
    std::vector<uint8_t> pay;
    uint64_t cnt_s[2][4] = {{0}}, cnt_a[4] = {0}, cnt_d[4] = {0}, dup_entries = 0, a3_ids = 0;
    for (uint64_t r = 0; r < R; r++) {
        const uint8_t* g = G.take(20);
        const uint64_t cell = rd64(g);
        const uint32_t off[3] = {rd32(g + 8), rd32(g + 12), rd32(g + 16)};
        uint8_t d = 0;
        pay.clear();
        if (r % B == 0) {
            if (rows.tell() > 0xFFFFFFFFull || next > 0xFFFFFFFFull) die("gxc: top field overflow");
            const Top t{cell, uint32_t(rows.tell()), cur_off[0], cur_off[1], uint32_t(next), cur_first[0], cur_first[1]};
            top.put(&t, sizeof t);
        } else {
            if (cell <= prev_cell) die("gxc: rows out of order at %llu", (unsigned long long)r);
            const uint64_t diff = cell - prev_cell;
            if (diff & ((uint64_t(1) << CELL_SHIFT) - 1)) die("gxc: cell step not a multiple of 2^27 at row %llu", (unsigned long long)r);
            const uint64_t step = diff >> CELL_SHIFT;
            d = uint8_t(step <= 3 ? step - 1 : 3);
            if (d == 3) put_uv(pay, step - 4);
        }
        cnt_d[d]++;
        prev_cell = cell;
        uint8_t ls[2] = {0, 0};
        for (int k = 0; k < 2; k++) {
            const int t = k ? 2 : 0;
            if (off[t] == NO) continue;
            read_list(*E[t], eoff[t], off[t], list, ENTRY[t]);
            if (have_prev[k] && list == prev[k]) {
                ls[k] = 2;
            } else {
                bool strict = true;
                for (size_t j = 1; j < list.size(); j++) {
                    if (list[j] < list[j - 1]) die("%s: unsorted list at row %llu", ENTRY[t], (unsigned long long)r);
                    if (list[j] == list[j - 1]) strict = false;
                }
                ls[k] = strict ? 1 : 3;
                if (st[k]->tell() >= 0xFFFFFFFFull) die("gxc: stream offset overflow");
                cur_off[k] = uint32_t(st[k]->tell());
                st[k]->uv(list.size() - 1);
                st[k]->uv(zz64(int64_t(list[0]) - int64_t(cur_first[k])));
                for (size_t j = 1; j < list.size(); j++) st[k]->uv(list[j] - list[j - 1] - (strict ? 1 : 0));
                cur_first[k] = list[0];
                prev[k].swap(list);
                have_prev[k] = true;
            }
            cnt_s[k][ls[k]]++;
        }
        uint8_t as = 0;
        if (off[1] != NO) {
            read_list(*E[1], eoff[1], off[1], list, ENTRY[1]);
            const size_t c = list.size();
            bool run = list[0] == next, exact = true;
            for (size_t j = 1; j < c && run; j++) {
                const uint32_t s = list[j] - list[j - 1];
                if (list[j] < list[j - 1] || s > 1) run = false;
                else if (s == 0) exact = false;
            }
            if (run && exact) {
                as = 1;
                put_uv(pay, c);
                next += c;
            } else if (run) {
                as = 2;
                const uint64_t K = uint64_t(list.back()) - next + 1;
                put_uv(pay, K);
                std::vector<std::pair<uint64_t, uint64_t>> dups;
                for (size_t j = 0; j < c;) {
                    size_t e = j + 1;
                    while (e < c && list[e] == list[j]) e++;
                    if (e - j > 1) dups.push_back({list[j] - next, e - j});
                    j = e;
                }
                put_uv(pay, dups.size());
                int64_t pj = -1;
                for (auto& [idx, copies] : dups) {
                    put_uv(pay, uint64_t(int64_t(idx) - pj - 1));
                    put_uv(pay, copies - 2);
                    dup_entries += copies - 1;
                    pj = int64_t(idx);
                }
                next += K;
            } else {
                as = 3;
                for (size_t j = 1; j < c; j++)
                    if (list[j] < list[j - 1]) die("addr_entries: unsorted list at row %llu", (unsigned long long)r);
                const uint64_t k = uint64_t(list.back()) + 1 > next ? uint64_t(list.back()) + 1 - next : 0;
                put_uv(pay, k);
                put_uv(pay, c - 1);
                put_uv(pay, zz64(int64_t(list[0]) - int64_t(next)));
                for (size_t j = 1; j < c; j++) put_uv(pay, list[j] - list[j - 1]);
                next += k;
                a3_ids += c;
            }
            cnt_a[as]++;
        }
        rows.u8(uint8_t(ls[0] | (as << 2) | (ls[1] << 4) | (d << 6)));
        rows.put(pay);
    }
    for (int t = 0; t < 3; t++)
        if (!E[t]->eof()) die("%s: bytes after the last list", ENTRY[t]);
    for (int t = 0; t < 3; t++) delete E[t];
    m.set("gxc.rows", R);
    m.set("gxc.B", B);
    m.set("gxc.addr_ids", next);
    for (int t = 0; t < 3; t++) m.set(std::string("gxc.") + ENTRY[t], eoff[t]);
    fprintf(stderr,
            "  gxc: rows %llu blocks %llu | street states 1/2/3 %llu/%llu/%llu | interp %llu/%llu/%llu | addr 1/2/3 "
            "%llu/%llu/%llu (repeated entries %llu, explicit ids %llu) | d 0/1/2/esc %llu/%llu/%llu/%llu | rows %llu B "
            "street %llu B interp %llu B\n",
            (unsigned long long)R, (unsigned long long)((R + B - 1) / B), (unsigned long long)cnt_s[0][1],
            (unsigned long long)cnt_s[0][2], (unsigned long long)cnt_s[0][3], (unsigned long long)cnt_s[1][1],
            (unsigned long long)cnt_s[1][2], (unsigned long long)cnt_s[1][3], (unsigned long long)cnt_a[1],
            (unsigned long long)cnt_a[2], (unsigned long long)cnt_a[3], (unsigned long long)dup_entries,
            (unsigned long long)a3_ids, (unsigned long long)cnt_d[0], (unsigned long long)cnt_d[1],
            (unsigned long long)cnt_d[2], (unsigned long long)cnt_d[3], (unsigned long long)rows.tell(),
            (unsigned long long)st[0]->tell(), (unsigned long long)st[1]->tell());
    delete st[0];
    delete st[1];
}

// A list holds 1 to 65535 ids (today's u16 count); every count is checked before it is used.
static void decode_list(Reader& s, bool strict, uint32_t prev_first, std::vector<uint32_t>& list) {
    const uint64_t cm1 = s.uv();  // count - 1
    if (cm1 >= 0xFFFF) die("gxc: list too long");
    const size_t c = size_t(cm1) + 1;
    list.resize(c);
    list[0] = uint32_t(int64_t(prev_first) + unzz64(s.uv()));
    for (size_t j = 1; j < c; j++) list[j] = list[j - 1] + uint32_t(s.uv()) + (strict ? 1 : 0);
}

inline void decode(const std::string& src, const std::string& out, const Manifest& m) {
    const uint64_t R = m.num("gxc.rows");
    if (m.num("gxc.B") != B) die("unsupported B");
    Reader top(src + "/gxc_top.bin", 1 << 20), rows(src + "/gxc_rows.bin");
    Reader* st[2] = {new Reader(src + "/" + STREAM[0]), new Reader(src + "/" + STREAM[1])};
    if (top.size() != 32 * ((R + B - 1) / B)) die("gxc_top.bin size");
    Writer geo(out + "/geo_cells.bin");
    Writer* E[3];
    for (int t = 0; t < 3; t++) E[t] = new Writer(out + "/" + ENTRY[t]);
    // Streaming: the patcher reads a row, then its lists, from the four FIFOs.
    FlushGroup fg({&geo, E[0], E[1], E[2]});
    uint64_t eoff[3] = {0, 0, 0};
    std::vector<uint32_t> cur[2], list;
    bool have[2] = {false, false};
    uint32_t cur_off[2] = {NO, NO}, cur_first[2] = {0, 0};
    uint64_t next = 0, cell = 0;
    auto emit = [&](int t, const std::vector<uint32_t>& l) {
        const uint32_t o = uint32_t(eoff[t]);
        if (eoff[t] > 0xFFFFFFFFull) die("gxc: entries offset overflow");
        E[t]->u16(uint16_t(l.size()));
        E[t]->put(l.data(), 4 * l.size());
        eoff[t] += 2 + 4 * l.size();
        return o;
    };
    uint8_t row[20];
    for (uint64_t r = 0; r < R; r++) {
        if (r % B == 0) {
            Top t;
            memcpy(&t, top.take(32), 32);
            if (t.roff != rows.tell() || t.s != cur_off[0] || t.i != cur_off[1] || t.afirst != uint32_t(next) ||
                t.sprev != cur_first[0] || t.iprev != cur_first[1])
                die("gxc: block %llu entry state differs from the running state", (unsigned long long)(r / B));
            cell = t.cell;
        }
        const uint8_t h = rows.u8();
        const int d = h >> 6;
        if (r % B) {
            const uint64_t step = d < 3 ? uint64_t(d) + 1 : rows.uv() + 4;
            cell += step << CELL_SHIFT;
        } else if (d) {
            die("gxc: step on a block's first row");
        }
        uint32_t off[3] = {NO, NO, NO};
        const int as = (h >> 2) & 3;
        if (as) {
            list.clear();
            auto bad = [&] { die("gxc: bad address list at row %llu", (unsigned long long)r); };
            if (as == 1) {
                const uint64_t k = rows.uv();
                if (k == 0 || k > 0xFFFF) bad();
                for (uint64_t j = 0; j < k; j++) list.push_back(uint32_t(next + j));
                next += k;
            } else if (as == 2) {
                const uint64_t K = rows.uv(), D = rows.uv();
                if (K == 0 || K > 0xFFFF || D > K) bad();
                uint64_t pj = 0, j = 0;  // pj: one past the previous repeated index
                for (uint64_t x = 0; x < D; x++) {
                    const uint64_t gap = rows.uv(), cm2 = rows.uv();  // copies - 2
                    if (gap >= K - pj || cm2 >= 0xFFFF) bad();
                    const uint64_t idx = pj + gap, copies = cm2 + 2;
                    if (list.size() + (idx - j) + copies > 0xFFFF) bad();
                    for (; j < idx; j++) list.push_back(uint32_t(next + j));
                    for (uint64_t c = 0; c < copies; c++) list.push_back(uint32_t(next + j));
                    j++;
                    pj = idx + 1;
                }
                for (; j < K; j++) list.push_back(uint32_t(next + j));
                next += K;
            } else {
                const uint64_t k = rows.uv(), cm1 = rows.uv();  // count - 1
                if (cm1 >= 0xFFFF) bad();
                const uint64_t c = cm1 + 1;
                list.push_back(uint32_t(int64_t(next) + unzz64(rows.uv())));
                for (uint64_t j = 1; j < c; j++) list.push_back(list.back() + uint32_t(rows.uv()));
                next += k;
            }
            if (list.empty() || list.size() > 0xFFFF) die("gxc: bad address list at row %llu", (unsigned long long)r);
            off[1] = emit(1, list);
        }
        for (int k = 0; k < 2; k++) {
            const int s = (h >> (k ? 4 : 0)) & 3;
            if (!s) continue;
            if (s == 2) {
                if (!have[k]) die("gxc: repeat without a list at row %llu", (unsigned long long)r);
            } else {
                cur_off[k] = uint32_t(st[k]->tell());
                decode_list(*st[k], s == 1, cur_first[k], cur[k]);
                cur_first[k] = cur[k][0];
                have[k] = true;
            }
            off[k ? 2 : 0] = emit(k ? 2 : 0, cur[k]);
        }
        wr64(row, cell);
        wr32(row + 8, off[0]);
        wr32(row + 12, off[1]);
        wr32(row + 16, off[2]);
        geo.put(row, 20);
        fg.row_done();
    }
    if (!rows.eof() || !st[0]->eof() || !st[1]->eof() || !top.eof()) die("gxc: streams not fully consumed");
    if (next != m.num("gxc.addr_ids")) die("gxc: address id count");
    for (int t = 0; t < 3; t++) delete E[t];
    delete st[0];
    delete st[1];
}

}  // namespace gxc
