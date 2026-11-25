// The --rank-dump file of build-index: how each boundary and place area came
// to its rank (rank_address.h's RankTrace), as TSV.
#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "id_allocator.h"
#include "rank_address.h"

// Makes the file at `path`, which must not be there yet, so that a path
// naming an input or an earlier output loses nothing: its descriptor, which
// write_rank_dump fills, so that a path that names another file by then is
// not followed.
inline int create_rank_dump(const std::string& path) {
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (fd < 0) throw std::runtime_error("--rank-dump: can't make " + path + ": " + std::strerror(errno));
    return fd;
}

// The TSV of the traces: a row a boundary or place area, by osm id (r for a
// relation, w for a closed way), under the header.
inline std::string rank_dump_text(std::vector<RankTrace> trace) {
    std::sort(trace.begin(), trace.end(), [](const RankTrace& a, const RankTrace& b) { return a.id < b.id; });
    auto osm = [](uint64_t id) -> std::string {
        if (id == 0) return "-";
        const uint64_t type = id >> 56, value = id & 0x00FFFFFFFFFFFFFFull;
        if (type == static_cast<uint64_t>(gc::id_alloc::ObjectType::OSM_RELATION)) return "r" + std::to_string(value >> 16);
        if (type == static_cast<uint64_t>(gc::id_alloc::ObjectType::OSM_WAY)) return "w" + std::to_string(value);
        return "x" + std::to_string(id);
    };
    auto country = [](uint16_t cc) {
        return cc ? std::string{static_cast<char>(cc >> 8), static_cast<char>(cc & 0xFF)} : std::string("-");
    };
    static constexpr const char* kLinkSteps[] = {"-", "label", "wikidata", "type", "name"};
    std::string out =
        "object\tlevel\tcountry\trank_search\tinitial\tparent\tparent_rank\tduplicate\tcontainer\tcontainer_rank"
        "\tbefore_link\tstep\trank\tlink\tnode\n";
    auto field = [&out](const std::string& s, char end) { out += s; out += end; };
    for (const RankTrace& t : trace) {
        field(osm(t.id), '\t');
        field(std::to_string(t.level), '\t');
        field(country(t.country), '\t');
        field(std::to_string(t.initial.search), '\t');
        field(std::to_string(t.initial.address), '\t');
        field(osm(t.parent), '\t');
        field(std::to_string(t.parent_rank), '\t');
        field(std::to_string(int(t.duplicate)), '\t');
        field(osm(t.container), '\t');
        field(std::to_string(t.container_rank), '\t');
        field(std::to_string(t.before_link), '\t');
        field(std::string(1, t.step), '\t');
        field(std::to_string(t.rank), '\t');
        field(kLinkSteps[static_cast<size_t>(t.link)], '\t');
        field(t.node ? "n" + std::to_string(t.node) : std::string("-"), '\n');
    }
    return out;
}

// Writes the traces (rank_dump_text) through `fd`, the descriptor
// create_rank_dump made the file `path` with, and closes it.
inline void write_rank_dump(int fd, const std::string& path, std::vector<RankTrace> trace) {
    const std::string text = rank_dump_text(std::move(trace));
    size_t done = 0;
    while (done < text.size()) {
        const ssize_t n = ::write(fd, text.data() + done, text.size() - done);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            const int err = n < 0 ? errno : EIO;
            ::close(fd);
            throw std::runtime_error("--rank-dump: failed writing " + path + ": " + std::strerror(err));
        }
        done += static_cast<size_t>(n);
    }
    if (::close(fd) != 0) throw std::runtime_error("--rank-dump: failed writing " + path + ": " + std::strerror(errno));
}
