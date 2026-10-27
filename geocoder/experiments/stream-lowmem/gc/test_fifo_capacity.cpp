// test_fifo_capacity: when a FIFO's pipe can't be enlarged, gc's grouped FIFO writers fail at once
// instead of deadlocking with their reader. A child drops to an unprivileged uid (nobody, when run
// as root), holds pipes until it is past its pipe page limit (fs.pipe-user-pages-soft), then
// writes gxc-decode-shaped rows (geo row, 100-id street list, 1-id address list) through four
// FIFOs to a reader in the patcher's order (row, then its lists). Exit 2 from gc's die() passes;
// a run still going after 20 s (the deadlock) fails.
#include "io.h"
#include <grp.h>
#include <sys/wait.h>
#include <signal.h>

static const char* NAMES[4] = {"geo", "street", "addr", "interp"};

// The patcher's reads: a 64 KiB window per FIFO, one read() until n bytes are in.
struct Win {
    int fd = -1;
    std::vector<char> b = std::vector<char>(64 << 10);
    size_t off = 0, len = 0;
    const char* need(size_t n) {
        if (len - off < n) {
            memmove(&b[0], &b[off], len - off);
            len -= off;
            off = 0;
            while (len < n) {
                const ssize_t r = read(fd, &b[len], b.size() - len);
                if (r <= 0) _exit(0);  // the writer went away
                len += size_t(r);
            }
        }
        off += n;
        return &b[off - n];
    }
};

int main() {
    const char* tmp = getenv("TMPDIR");
    std::string dir = std::string(tmp ? tmp : "/tmp") + "/fifo-capacity-XXXXXX";
    if (!mkdtemp(dir.data())) return 1;
    chmod(dir.c_str(), 0777);
    for (const char* n : NAMES) {
        mkfifo((dir + "/" + n).c_str(), 0666);
        chmod((dir + "/" + n).c_str(), 0666);
    }
    const std::string log = dir + "/writer.log";
    const int log_fd = open(log.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (log_fd < 0) return 1;
    const pid_t writer = fork();
    if (writer < 0) return 1;
    if (!writer) {
        setpgid(0, 0);  // with its reader: the parent ends both
        dup2(log_fd, 2);
        if (geteuid() == 0 && (setgroups(0, nullptr) || setgid(65534) || setuid(65534))) _exit(5);
        // Pipes of up to 1 MiB until the user is past its pipe page limit, where a new pipe gets the
        // minimum size and none can grow (stopping at the first refused 1 MiB would leave up to
        // 1 MiB minus a page free, enough for a FIFO's enlargement).
        for (int k = 0;; k++) {
            if (k == 4096) {
                fprintf(stderr, "SKIP: no pipe page limit reached, no refusal to test\n");
                _exit(77);
            }
            int p[2];
            if (pipe(p)) _exit(6);
            if (fcntl(p[1], F_GETPIPE_SZ) < 65536) break;
            fcntl(p[1], F_SETPIPE_SZ, int(FIFO_PIPE));
        }
        const pid_t reader = fork();
        if (reader < 0) _exit(6);
        if (!reader) {
            alarm(30);
            Win w[4];
            for (int k = 0; k < 4; k++)
                if ((w[k].fd = open((dir + "/" + NAMES[k]).c_str(), O_RDONLY)) < 0) _exit(7);
            for (;;) {
                w[0].need(20);
                uint16_t n;
                memcpy(&n, w[1].need(2), 2);
                w[1].need(4 * size_t(n));
                memcpy(&n, w[2].need(2), 2);
                w[2].need(4 * size_t(n));
            }
        }
        alarm(20);
        Writer geo(dir + "/geo"), street(dir + "/street"), addr(dir + "/addr"), interp(dir + "/interp");
        FlushGroup fg({&geo, &street, &addr, &interp});
        const std::vector<uint32_t> ids(100, 7);
        uint8_t row[20] = {};
        for (int r = 0; r < 20000; r++) {
            street.u16(uint16_t(ids.size()));
            street.put(ids.data(), 4 * ids.size());
            addr.u16(1);
            addr.u32(1);
            geo.put(row, 20);
            fg.row_done();
        }
        _exit(0);  // (the pipes were wide after all: no deadlock to look for)
    }
    int status = 0;
    waitpid(writer, &status, 0);
    kill(-writer, SIGKILL);
    std::string said(4096, '\0');
    const ssize_t got = pread(log_fd, &said[0], said.size(), 0);
    said.resize(got > 0 ? size_t(got) : 0);
    close(log_fd);
    fprintf(stderr, "%s", said.c_str());
    for (const char* n : NAMES) unlink((dir + "/" + n).c_str());
    unlink(log.c_str());
    rmdir(dir.c_str());
    if (WIFEXITED(status) && WEXITSTATUS(status) == 77) return 77;
    const bool ok = WIFEXITED(status) && WEXITSTATUS(status) == 2 && said.find("the pipe can't hold") != std::string::npos;
    if (!ok)
        fprintf(stderr, "FAIL: writer %s %d, want gc's die (exit 2) on the pipe size\n",
                WIFSIGNALED(status) ? "killed by signal" : "exited", WIFSIGNALED(status) ? WTERMSIG(status) : WEXITSTATUS(status));
    fprintf(stderr, ok ? "PASS\n" : "FAIL\n");
    return ok ? 0 : 1;
}
