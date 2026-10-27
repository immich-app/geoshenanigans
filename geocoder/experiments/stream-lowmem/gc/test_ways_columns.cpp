// test_ways_columns: an encoded street or interpolation set with bytes past the end of any one of its
// columns fails in gc's ways decoders (the full and the ways-only decode), instead of decoding and
// dropping them. Each case appends a byte to one column of a 40-way set and decodes it in a child
// process; the unchanged set decodes in both. (A streets ways-only decode doesn't read street.code:
// the nodes-only decode it runs beside does, and checks it. The patcher's decoder:
// geocoder/builder/tests/test_gcs_codec.cpp.)
#include "fam_ways.h"
#include <filesystem>
#include <fstream>
#include <sys/wait.h>
#include <signal.h>

static void write_bytes(const std::string& path, const std::vector<uint8_t>& bytes) {
    std::ofstream out(path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(bytes.data()), std::streamsize(bytes.size()));
}
static void put32(std::vector<uint8_t>& o, uint32_t v) { for (int b = 0; b < 4; b++) o.push_back(uint8_t(v >> (8 * b))); }
static void putf(std::vector<uint8_t>& o, float f) { uint32_t v; memcpy(&v, &f, 4); put32(o, v); }

// 40 ways of 0 to 3 nodes; node_offset is the running node count (0 for a way without nodes).
static void today_set(const std::string& dir, bool street) {
    std::vector<uint8_t> ways, nodes;
    uint32_t off = 0;
    for (uint32_t w = 0; w < 40; w++) {
        const uint16_t cnt = uint16_t(w % 4);
        const size_t at = ways.size();
        put32(ways, cnt ? off : 0);
        ways.push_back(uint8_t(cnt));
        ways.push_back(uint8_t(cnt >> 8));
        ways.push_back(0);
        ways.push_back(0);
        put32(ways, 1000 + w);  // name, or the interpolation's street
        if (!street) {
            put32(ways, 2 * w);      // start
            put32(ways, 2 * w + 9);  // end
            ways.push_back(uint8_t(w % 3));
            ways.resize(at + 24, 0);
        }
        for (uint16_t j = 0; j < cnt; j++) {
            putf(nodes, 47.0f + float(w) * 1e-4f + float(j) * 1e-5f);
            putf(nodes, 8.0f + float(w) * 1e-4f - float(j) * 1e-5f);
        }
        off += cnt;
    }
    std::filesystem::create_directories(dir);
    write_bytes(dir + (street ? "/street_ways.bin" : "/interp_ways.bin"), ways);
    write_bytes(dir + (street ? "/street_nodes.bin" : "/interp_nodes.bin"), nodes);
}

// 0: gc's full decode, 1: gc's ways-only decode (--files <ways>).
static const char* const DECODERS[] = {"gc", "gc ways-only"};

static int decode_in_child(int which, bool street, const std::string& enc, const std::string& out, const Manifest& m) {
    const pid_t child = fork();
    if (child < 0) return -1;
    if (!child) {
        alarm(20);
        if (which == 1) want_files() = {street ? "street_ways.bin" : "interp_ways.bin"};
        ways::decode(enc, out, street, m);  // gc's die() exits 2
        _exit(0);
    }
    int status = 0;
    waitpid(child, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
}

int main() {
    const char* tmp = getenv("TMPDIR");
    std::string root = std::string(tmp ? tmp : "/tmp") + "/ways-columns-XXXXXX";
    if (!mkdtemp(root.data())) return 1;
    bool ok = true;
    for (bool street : {true, false}) {
        const std::string fam = street ? "street" : "interp";
        const std::string src = root + "/" + fam + ".src", enc = root + "/" + fam + ".enc";
        today_set(src, street);
        std::filesystem::create_directories(enc);
        Manifest m;
        ways::encode(src, enc, street, m);
        const std::vector<std::string> columns = street ? std::vector<std::string>{"cnt", "code", "name"}
                                                        : std::vector<std::string>{"cntcode", "street", "start", "span", "interp"};
        std::vector<std::string> cases{""};  // "": the set as encoded
        for (const auto& c : columns) cases.push_back(c);
        for (const std::string& column : cases) {
            const std::string bad = root + "/" + fam + "." + (column.empty() ? "as-encoded" : column);
            std::filesystem::copy(enc, bad);
            if (!column.empty()) {
                std::ofstream f(bad + "/" + fam + "." + column, std::ios::binary | std::ios::app);
                f.put('\x5a');
            }
            for (int which = 0; which < 2; which++) {
                if (which == 1 && street && column == "code") continue;
                const std::string out = bad + ".out" + std::to_string(which);
                std::filesystem::create_directories(out);
                const int got = decode_in_child(which, street, bad, out, m);
                const int want = column.empty() ? 0 : 2;
                if (got != want) {
                    fprintf(stderr, "FAIL %s.%s (%s decoder): exit %d, want %d\n", fam.c_str(),
                            column.empty() ? "<as encoded>" : column.c_str(), DECODERS[which], got, want);
                    ok = false;
                }
            }
        }
    }
    std::filesystem::remove_all(root);
    fprintf(stderr, ok ? "PASS\n" : "FAIL\n");
    return ok ? 0 : 1;
}
