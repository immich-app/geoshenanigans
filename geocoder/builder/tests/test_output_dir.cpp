// Unit tests for require_new_or_empty_output_dir (output_dir.h), build-index's check of its output dir.
#include "output_dir.h"

#include <filesystem>
#include <fstream>
#include <string>

#include "scratch_dir.h"
#include "test_framework.h"

using gctest::refusal_of;

namespace fs = std::filesystem;

static std::string refusal_of_output_dir(const std::string& dir) {
    return refusal_of([&] { require_new_or_empty_output_dir(dir); });
}

TEST(build_index_builds_into_an_absent_output_dir) {
    ScratchDir s("output-dir");
    CHECK_EQ(refusal_of_output_dir(s.path() + "/output"), std::string());
    // The nightly's form, and one whose parent is absent too.
    CHECK_EQ(refusal_of_output_dir(s.path() + "/output/"), std::string());
    CHECK_EQ(refusal_of_output_dir(s.path() + "/runs/output"), std::string());
}

TEST(build_index_builds_into_an_empty_output_dir) {
    ScratchDir s("output-dir");
    CHECK_EQ(refusal_of_output_dir(s.path()), std::string());
    CHECK_EQ(refusal_of_output_dir(s.path() + "/"), std::string());
}

TEST(build_index_refuses_an_output_dir_holding_an_earlier_builds_file_and_leaves_it) {
    ScratchDir s("output-dir");
    const std::string stale = s.path() + "/place_cells.bin";
    std::ofstream(stale) << "an earlier build's";
    CHECK_EQ(refusal_of_output_dir(s.path()), "Output dir " + s.path() + " is not empty: build into a new or empty dir");
    CHECK(fs::is_regular_file(stale));
}

TEST(build_index_refuses_an_output_dir_holding_only_a_subdir) {
    ScratchDir s("output-dir");
    fs::create_directories(s.path() + "/planet/full");
    CHECK_EQ(refusal_of_output_dir(s.path()), "Output dir " + s.path() + " is not empty: build into a new or empty dir");
    CHECK(fs::is_directory(s.path() + "/planet/full"));
}

TEST(build_index_refuses_an_output_dir_path_that_is_a_regular_file) {
    ScratchDir s("output-dir");
    const std::string file = s.path() + "/output";
    std::ofstream(file) << "not a dir";
    CHECK_EQ(refusal_of_output_dir(file), "Output dir " + file + " is not a directory: build into a new or empty dir");
    CHECK(fs::is_regular_file(file));
}
