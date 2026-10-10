// pbf-apply: apply OSM change files to a PBF, as `osmium apply-changes`
// does, copying untouched blobs verbatim (see pbf_apply.h).
//
// Usage: pbf-apply <in.osm.pbf> <change.osc[.gz]>... -o <out.osm.pbf>
//            [--replication-timestamp ISO8601] [--replication-sequence N]
//            [--replication-base-url URL]
#include <climits>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

#include "pbf_apply.h"

static const char* kUsage =
    "Usage: pbf-apply <in.osm.pbf> <change.osc[.gz]>... -o <out.osm.pbf>\n"
    "                 [--replication-timestamp ISO8601] [--replication-sequence N]\n"
    "                 [--replication-base-url URL]";

static int run(int argc, char* argv[]) {
    ApplyOptions opt;
    std::vector<std::string> positional;
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        auto value = [&]() -> std::string {
            if (i + 1 >= argc) throw std::runtime_error(arg + " needs a value");
            return argv[++i];
        };
        if (arg == "-o" || arg == "--output") {
            opt.output = value();
        } else if (arg == "--replication-timestamp") {
            opt.replication_timestamp = osc::parse_timestamp(value().c_str());
        } else if (arg == "--replication-sequence") {
            std::string v = value();
            char* end = nullptr;
            long long n = std::strtoll(v.c_str(), &end, 10);
            if (v.empty() || *end != '\0' || n < 0 || n == LLONG_MAX)
                throw std::runtime_error("bad --replication-sequence: " + v);
            opt.replication_sequence = n;
        } else if (arg == "--replication-base-url") {
            opt.replication_base_url = value();
        } else if (arg.size() > 1 && arg[0] == '-') {
            throw std::runtime_error("unknown option " + arg);
        } else {
            positional.push_back(arg);
        }
    }
    if (positional.size() < 2 || opt.output.empty()) {
        std::cerr << kUsage << std::endl;
        return 1;
    }
    opt.input = positional[0];
    opt.changes.assign(positional.begin() + 1, positional.end());
    // The output is written while the input is still read.
    char in_real[PATH_MAX], out_real[PATH_MAX];
    if (realpath(opt.input.c_str(), in_real) && realpath(opt.output.c_str(), out_real) &&
        std::strcmp(in_real, out_real) == 0)
        throw std::runtime_error("output must differ from the input: " + opt.output);
    apply_changes(opt);
    return 0;
}

int main(int argc, char* argv[]) {
    try {
        return run(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }
}
