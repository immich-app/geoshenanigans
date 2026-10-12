// Unit tests for ScratchDir (scratch_dir.h).
#include "scratch_dir.h"

#include <cstdlib>
#include <filesystem>
#include <string>
#include <unistd.h>

#include "test_framework.h"

namespace fs = std::filesystem;

// --- ScratchDir ---

TEST(scratch_dir_lives_under_tmpdir_and_is_removed_with_its_contents) {
    fs::path base = fs::temp_directory_path() / ("scratch-test-" + std::to_string(getpid()));
    fs::create_directories(base);
    const char* prev = std::getenv("TMPDIR");
    std::string saved = prev ? prev : "";
    setenv("TMPDIR", base.c_str(), 1);

    std::string path;
    {
        ScratchDir s("unit");
        path = s.path();
        CHECK(path.rfind((base / "unit-").string(), 0) == 0);
        CHECK(fs::is_directory(path));
        std::FILE* f = std::fopen((path + "/payload").c_str(), "w");
        REQUIRE(f != nullptr);
        std::fputs("x", f);
        std::fclose(f);
    }
    CHECK(!fs::exists(path));

    if (prev) setenv("TMPDIR", saved.c_str(), 1); else unsetenv("TMPDIR");
    fs::remove_all(base);
}

TEST(scratch_dirs_with_the_same_prefix_are_distinct) {
    ScratchDir a("same"), b("same");
    CHECK(a.path() != b.path());
}
