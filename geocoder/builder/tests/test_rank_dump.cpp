// Unit tests for the --rank-dump file (rank_dump.h).
#include "rank_dump.h"

#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

#include "scratch_dir.h"
#include "test_framework.h"

namespace fs = std::filesystem;

namespace {

std::string text_of(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

}  // namespace

TEST(a_rank_dump_goes_to_the_file_it_made_though_its_path_now_names_another) {
    // The dump is made at the start of a build and filled at its end: a path that names another
    // file by then, here a link to one, leaves that file as it was, and the rows go to the file
    // the build made.
    const ScratchDir dir("rank-dump-file");
    const std::string path = dir.path() + "/ranks.tsv", made = dir.path() + "/made.tsv",
                      other = dir.path() + "/other.tsv";
    const int fd = create_rank_dump(path);
    fs::rename(path, made);
    std::ofstream(other) << "kept\n";
    fs::create_symlink(other, path);

    RankTrace t;
    t.id = (static_cast<uint64_t>(gc::id_alloc::ObjectType::OSM_RELATION) << 56) | (uint64_t{62422} << 16);
    t.level = 8;
    t.rank = 16;
    write_rank_dump(fd, path, {t});

    CHECK_EQ(text_of(other), std::string("kept\n"));
    const std::string rows = text_of(made);
    CHECK(rows.rfind("object\tlevel\t", 0) == 0);
    CHECK(rows.find("\nr62422\t8\t-\t") != std::string::npos);
}

TEST(a_rank_dump_is_not_made_where_a_file_is) {
    const ScratchDir dir("rank-dump-file");
    const std::string path = dir.path() + "/ranks.tsv";
    std::ofstream(path) << "kept\n";
    std::string error;
    try {
        create_rank_dump(path);
    } catch (const std::runtime_error& e) {
        error = e.what();
    }
    CHECK(error.rfind("--rank-dump: can't make " + path + ": ", 0) == 0);
    CHECK_EQ(text_of(path), std::string("kept\n"));
}
