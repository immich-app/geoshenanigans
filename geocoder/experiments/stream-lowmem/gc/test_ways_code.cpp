// test_ways_code: a way's code with widths the encoder never writes (first coordinate past 4 bytes,
// deltas past 32 bits) is refused by gc's decoder, instead of shifting a u32 past its width. The
// widest valid code decodes. Each case runs in a child. (The patcher's decoder:
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

static std::vector<uint8_t> le(uint64_t v, int n) {
    std::vector<uint8_t> b;
    for (int k = 0; k < n; k++) b.push_back(uint8_t(v >> (8 * k)));
    return b;
}

int main() {
    const char* tmp = std::getenv("TMPDIR");
    std::string root = std::string(tmp ? tmp : "/tmp") + "/ways-code-XXXXXX";
    if (!mkdtemp(root.data())) return 1;
    const std::string src = root + "/in", out = root + "/out";
    std::filesystem::create_directories(src);
    std::filesystem::create_directories(out);
    struct Case { int wf, wl, wg; bool valid; };
    const Case cases[] = {{4, 32, 32, true}, {0, 0, 0, true}, {5, 0, 0, false}, {7, 0, 0, false},
                          {0, 33, 0, false}, {4, 32, 63, false}};
    bool ok = true;
    for (const Case& c : cases) {
        // one street way of two nodes: its block is 2 wf bytes then (wl + wg) bits
        const uint16_t cnt = 2, code = uint16_t(c.wl | c.wg << 6 | c.wf << 13);
        write_bytes(src + "/street.groups", std::vector<uint8_t>(12));
        write_bytes(src + "/street.nodes2", std::vector<uint8_t>(ways::block_size(cnt, code), 0x5A));
        write_bytes(src + "/street.cnt", le(cnt, 2));
        write_bytes(src + "/street.code", le(code, 2));
        write_bytes(src + "/street.name", le(7, 4));
        const pid_t child = fork();
        if (child < 0) return 1;
        if (child == 0) {
            alarm(5);
            Manifest m;
            m.set("street.ways", 1);
            m.set("street.nodes", cnt);
            m.set("street.G", 16);
            ways::decode(src, out, true, m);  // gc's die() exits 2
            _exit(0);
        }
        int status = 0;
        waitpid(child, &status, 0);
        const int expected = c.valid ? 0 : 2;
        if (!WIFEXITED(status) || WEXITSTATUS(status) != expected) {
            fprintf(stderr, "FAIL wf=%d wl=%d wg=%d: status=%d expected=%d\n", c.wf, c.wl, c.wg, status, expected);
            ok = false;
        }
    }
    std::filesystem::remove_all(root);
    fprintf(stderr, ok ? "PASS\n" : "FAIL\n");
    return ok ? 0 : 1;
}
