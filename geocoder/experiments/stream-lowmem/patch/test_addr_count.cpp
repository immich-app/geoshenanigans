#include "gcs_codec.h"
#include "../gc/fam_addr.h"
#include <filesystem>
#include <fstream>
#include <sys/wait.h>
#include <signal.h>

static void write_bytes(const std::string& path, const std::vector<uint8_t>& bytes) {
    std::ofstream out(path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

int main() {
    const char* tmp = std::getenv("TMPDIR");
    std::string pattern = std::string(tmp ? tmp : "/tmp") + "/addr-count-XXXXXX";
    if (!mkdtemp(pattern.data())) return 1;
    const std::string src = pattern + "/in", out = pattern + "/out";
    std::filesystem::create_directories(src);
    std::filesystem::create_directories(out);
    write_bytes(src + "/addr_points.col", std::vector<uint8_t>(24));
    bool ok = true;
    // tag 0x04: an abs2 blob (wide, 2 + 2 count bytes per vertex after an 8-byte bmin); 0x20: the
    // raw escape (today's blob, 10 + 4 count bytes), whose count must fit the record's u32.
    struct Case { uint8_t tag; uint64_t count; };
    std::vector<Case> cases;
    for (const uint8_t tag : {uint8_t(0x04), uint8_t(0x20)})
        for (uint64_t count : {uint64_t(5), (uint64_t(1) << 32) + 5, (uint64_t(1) << 63) + 5}) cases.push_back({tag, count});
    for (const auto& [tag, count] : cases) {
        std::vector<uint8_t> bytes{tag};
        uint64_t value = count;
        do { const uint8_t part = value & 127; value >>= 7; bytes.push_back(part | (value ? 128 : 0)); } while (value);
        bytes.resize(bytes.size() + (tag == 0x20 ? 30 : 28));
        write_bytes(src + "/addr_vertices.abs2", bytes);
        for (int mode = 0; mode < 3; mode++) {
            const pid_t child = fork();
            if (child < 0) return 1;
            if (child == 0) {
                alarm(5);
                if (mode < 2) {
                    Manifest m; m.set("addr.n", 1); m.set("addr.layout", "col");
                    if (mode == 0) addr::decode_points_only(src, m, out);
                    else addr::decode(src, out, m);
                } else {
                    try {
                        gcs::Manifest m; m.set("addr.n", 1); m.set("addr.layout", "col");
                        gcs::addr::Dec dec(src, m);
                        while (dec.step()) {}
                    } catch (const std::runtime_error& e) {
                        _exit(std::string(e.what()).find("bad count") != std::string::npos ? 2 : 3);
                    }
                }
                _exit(0);
            }
            int status = 0; waitpid(child, &status, 0);
            const int expected = count == 5 ? 0 : 2;
            if (!WIFEXITED(status) || WEXITSTATUS(status) != expected) {
                fprintf(stderr, "FAIL tag=%#x count=%llu mode=%d status=%d expected=%d\n", tag, (unsigned long long)count, mode, status,
                        expected);
                ok = false;
            }
        }
    }
    std::filesystem::remove_all(pattern);
    return ok ? 0 : 1;
}
