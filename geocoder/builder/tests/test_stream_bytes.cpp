// The STREAM_BYTES line the builder logs for a logical vertex stream it wrote
// (parsed_data.h), which the nightly's report reads (tools/o11y_report.py).
#include "parsed_data.h"

#include <string>

#include "addr_files.h"
#include "test_framework.h"

TEST(a_stream_bytes_line_gives_a_streams_bytes_their_limit_and_their_share_of_it) {
    const uint64_t limit = addr_files::MAX_LOGICAL_VERTEX_BYTES;
    auto line = [&](uint64_t bytes) { return stream_bytes_line("planet/full", "addr_vertices", bytes, limit); };
    const std::string head = "\nSTREAM_BYTES dir=planet/full stream=addr_vertices bytes=";
    CHECK_EQ(line(3693937536ull), head + "3693937536 limit=8589934588 share=43.0%\n");
    // The share is in tenths of a percent, rounded down.
    CHECK_EQ(line(0), head + "0 limit=8589934588 share=0.0%\n");
    CHECK_EQ(line(8589934), head + "8589934 limit=8589934588 share=0.0%\n");
    CHECK_EQ(line(8589935), head + "8589935 limit=8589934588 share=0.1%\n");
    CHECK_EQ(line(limit - 1), head + "8589934587 limit=8589934588 share=99.9%\n");
    CHECK_EQ(line(limit), head + "8589934588 limit=8589934588 share=100.0%\n");
    CHECK_EQ(stream_bytes_line("europe/poi/all", "poi_vertices", 1073741824, 4294967294ull),
             std::string("\nSTREAM_BYTES dir=europe/poi/all stream=poi_vertices bytes=1073741824 limit=4294967294 share=25.0%\n"));
}
