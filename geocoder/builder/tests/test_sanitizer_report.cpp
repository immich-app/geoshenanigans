// A sanitizer report fails the test that ran into it, whatever the test
// expected of the run it ended: a tool's (tests/tool_runs.h) or a forked
// child's (tests/child_process.h), for each sanitizer's report
// (tests/fixtures/sanitizer_reports.h). A run that fails without one is a
// plain failure. The sanitized build's only (GEOCODER_SANITIZE), the one with
// reports to provoke.
#include <cstdio>
#include <fcntl.h>
#include <functional>
#include <string>
#include <unistd.h>
#include <vector>

#include "child_process.h"
#include "fixtures/sanitizer_reports.h"
#include "patch_format.h"
#include "sanitizer_report.h"
#include "scratch_dir.h"
#include "test_framework.h"
#include "tool_runs.h"

#ifdef GEOCODER_SANITIZER_REPORTS_BIN

namespace {

// Each kind of report, by what its sanitizer's report says.
struct Report {
    std::string kind, says;
};
const std::vector<Report> REPORTS = {
    {gctest::ASAN_HEAP_OVERFLOW, "AddressSanitizer: heap-buffer-overflow"},
    {gctest::UBSAN_SIGNED_OVERFLOW, "runtime error: signed integer overflow"},
    {gctest::LSAN_LEAK_AT_EXIT, "LeakSanitizer: detected memory leaks"},
};
// A kind without a report: the process exits 1.
const std::string REFUSAL = "refusal";

// Whether run threw the SanitizerReported whose message refusal makes of
// the log's text, and the log holds what report says; the output names the
// kind when not.
bool reported(const Report& report, const std::string& log, const std::function<void()>& run,
              const std::function<std::string(const std::string&)>& refusal) {
    const std::string thrown = gctest::refusal_of<gctest::SanitizerReported>(run);
    const std::vector<char> bytes = read_file(log);
    const std::string text(bytes.begin(), bytes.end());
    const bool refused = thrown == refusal(text);
    const bool said = text.find(report.says) != std::string::npos;
    if (!refused || !said) std::printf("    %s: %s\n", report.kind.c_str(), refused ? "another report" : "not reported");
    return refused && said;
}

}  // namespace

TEST(a_sanitizer_report_fails_a_tool_run) {
    ScratchDir dir("sanitizer-tool");
    for (const Report& r : REPORTS) {
        const std::string log = dir.path() + "/" + r.kind + ".log";
        CHECK(reported(r, log, [&] { gctest::run({GEOCODER_SANITIZER_REPORTS_BIN, r.kind}, log); },
                       [](const std::string& text) {
                           return std::string(GEOCODER_SANITIZER_REPORTS_BIN) + " ended with a sanitizer report:\n" + text;
                       }));
    }

    CHECK(!gctest::run({GEOCODER_SANITIZER_REPORTS_BIN, REFUSAL}, dir.path() + "/refusal.log"));
}

TEST(a_sanitizer_report_fails_a_forked_child) {
    ScratchDir dir("sanitizer-child");
    // The child's report goes to log, not into the test run's output.
    auto provoking = [](const std::string& kind, const std::string& log) {
        return [kind, log] {
            dup2(open(log.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644), STDERR_FILENO);
            return gctest::provoke_sanitizer_report(kind);
        };
    };
    for (const Report& r : REPORTS) {
        const std::string log = dir.path() + "/" + r.kind + ".log";
        CHECK(reported(r, log, [&] { gctest::exit_status_in_child(provoking(r.kind, log), 10); },
                       [](const std::string&) { return std::string("A forked child ended with a sanitizer report, printed above"); }));
    }

    CHECK_EQ(gctest::exit_status_in_child(provoking(REFUSAL, dir.path() + "/refusal.log"), 10), 1);
}

#endif
