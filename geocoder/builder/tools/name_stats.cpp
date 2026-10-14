// name_stats: alternate-name statistics from the builder's --name-dumps
// names.tsv (one line per place node / admin boundary / POI carrying a
// name-like tag: class, osm type, id, subtype, shipped, then key/value pairs).
//
//   name_stats <names.tsv> <out dir>
//
// Writes (every number counted from the dump):
//   keys.tsv            per class and key: features, value bytes (all / shipped)
//   key_groups.tsv      the same per key group (name, name:en, name:<lang>, ...)
//   coverage.tsv        per class and scope: how many features have name:en,
//                       int_name, other languages, ... and a languages histogram
//   coverage_subtype.tsv the coverage of the largest subtypes (place=*, admin levels)
//   search_bytes.tsv    bytes a search file would hold for the base set
//                       (name, name:en, int_name, alt_name, official_name,
//                       old_name) and for all languages: raw, distinct per
//                       feature, distinct per class, strings counted
//   strings_<set>_<class>_<scope>.txt  the distinct strings, sorted, one per
//                       line (for compressing)
// Values of every key are split on ';' (OSM's multi-value separator) and
// trimmed before the per-feature and global distinct counts; raw bytes count
// the value as tagged.
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cctype>
#include <cstring>
#include <stdexcept>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace {

constexpr const char* kClasses[] = {"place", "admin", "poi"};
constexpr int kNClasses = 3;
constexpr const char* kScopes[] = {"all", "shipped"};

int class_of(std::string_view c) {
    for (int i = 0; i < kNClasses; i++)
        if (c == kClasses[i]) return i;
    return -1;
}

std::string unescape(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); i++) {
        if (s[i] == '\\' && i + 1 < s.size()) {
            char c = s[++i];
            out += c == 't' ? '\t' : c == 'n' ? '\n' : c == 'r' ? '\r' : c;
        } else {
            out += s[i];
        }
    }
    return out;
}

// A language code: 2-3 lowercase letters, then optional -/_ subtags
// (zh-Hans, be-tarask, sr-Latn, zh_pinyin).
bool is_lang(std::string_view s) {
    size_t i = 0;
    while (i < s.size() && s[i] >= 'a' && s[i] <= 'z') i++;
    if (i < 2 || i > 3) return false;
    while (i < s.size()) {
        if (s[i] != '-' && s[i] != '_') return false;
        size_t j = ++i;
        while (i < s.size() && std::isalnum(static_cast<unsigned char>(s[i]))) i++;
        if (i == j) return false;
    }
    return true;
}

const std::set<std::string_view> kAltBases = {"int_name", "alt_name", "old_name", "official_name", "short_name",
                                              "loc_name", "reg_name", "nat_name", "sorting_name"};
const std::set<std::string_view> kBaseSet = {"name", "name:en", "int_name", "alt_name", "official_name", "old_name"};

// Key group: name, name:en, name:<lang>, name:<other> (etymology, ...),
// <alt base> (int_name, alt_name, ...), <alt base>:<lang>, other *_name.
std::string group_of(std::string_view k) {
    if (k == "name") return "name";
    if (k == "name:en") return "name:en";
    if (k.rfind("name:", 0) == 0) return is_lang(k.substr(5)) ? "name:<lang>" : "name:<non-language suffix>";
    size_t colon = k.find(':');
    std::string_view base = colon == std::string_view::npos ? k : k.substr(0, colon);
    if (kAltBases.count(base)) {
        if (colon == std::string_view::npos) return std::string(base);
        return std::string(base) + (is_lang(k.substr(colon + 1)) ? ":<lang>" : ":<other>");
    }
    return "other *_name";
}

// Keys in the "all languages" set: name, name:<lang>, the alternate-name
// bases and their :<lang> forms.
bool in_all_languages(const std::string& group) {
    return group == "name" || group == "name:en" || group == "name:<lang>" || kAltBases.count(group) ||
           (group.size() > 7 && group.compare(group.size() - 7, 7, ":<lang>") == 0);
}

std::vector<std::string_view> split_values(std::string_view v) {
    std::vector<std::string_view> out;
    size_t pos = 0;
    while (pos <= v.size()) {
        size_t next = v.find(';', pos);
        if (next == std::string_view::npos) next = v.size();
        std::string_view piece = v.substr(pos, next - pos);
        while (!piece.empty() && piece.front() == ' ') piece.remove_prefix(1);
        while (!piece.empty() && piece.back() == ' ') piece.remove_suffix(1);
        if (!piece.empty()) out.push_back(piece);
        pos = next + 1;
    }
    return out;
}

struct KeyStat {
    uint64_t features[2] = {0, 0};
    uint64_t bytes[2] = {0, 0};
};

// Coverage counters of one class and scope.
struct Coverage {
    uint64_t features = 0, name = 0, name_en = 0, name_en_differs = 0, int_name = 0, alt_name = 0,
             official_name = 0, old_name = 0, short_name = 0, loc_name = 0, any_lang = 0, lang_not_en = 0,
             any_alt = 0, no_name_tag = 0;
    std::array<uint64_t, 7> langs{};  // 0, 1, 2-4, 5-9, 10-49, 50-99, 100+
};
constexpr const char* kLangBuckets[] = {"0", "1", "2-4", "5-9", "10-49", "50-99", "100+"};
size_t lang_bucket(size_t n) {
    return n == 0 ? 0 : n == 1 ? 1 : n < 5 ? 2 : n < 10 ? 3 : n < 50 ? 4 : n < 100 ? 5 : 6;
}

struct SetBytes {
    uint64_t strings_raw = 0, bytes_raw = 0;            // every value as tagged
    uint64_t strings_feature = 0, bytes_feature = 0;    // split, distinct within a feature
    std::unordered_set<std::string> global;             // split, distinct in the class+scope
};

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "usage: name_stats <names.tsv> <out dir>" << std::endl;
        return 2;
    }
    std::ifstream in(argv[1]);
    const std::string dst = argv[2];
    if (!in) {
        std::cerr << "cannot read " << argv[1] << std::endl;
        return 1;
    }

    std::map<std::string, KeyStat> keys[kNClasses];
    std::map<std::string, KeyStat> groups[kNClasses];
    Coverage cov[kNClasses][2];
    std::map<std::string, Coverage> cov_sub[kNClasses];
    // [class][scope][set: 0 base, 1 all languages]
    static SetBytes sets[kNClasses][2][2];

    std::string line;
    std::vector<std::string_view> f;
    uint64_t lines = 0, bad = 0;
    while (std::getline(in, line)) {
        lines++;
        f.clear();
        size_t pos = 0;
        while (true) {
            size_t t = line.find('\t', pos);
            f.emplace_back(line.data() + pos, (t == std::string::npos ? line.size() : t) - pos);
            if (t == std::string::npos) break;
            pos = t + 1;
        }
        const int c = f.size() >= 5 ? class_of(f[0]) : -1;
        if (c < 0 || (f.size() - 5) % 2 != 0) {
            bad++;
            continue;
        }
        const bool shipped = f[4] == "1";
        const std::string subtype = unescape(f[3]);

        std::unordered_map<std::string, std::string> tags;  // key -> value (unescaped)
        for (size_t i = 5; i + 1 < f.size(); i += 2) tags[unescape(f[i])] = unescape(f[i + 1]);

        size_t n_lang = 0;
        bool lang_not_en = false, any_alt = false;
        std::set<std::string> seen_groups;
        // Distinct values within the feature per set.
        std::unordered_set<std::string> feature_vals[2];
        for (const auto& [k, v] : tags) {
            const std::string g = group_of(k);
            for (int s = 0; s < 2; s++) {
                if (s == 1 && !shipped) continue;
                auto& ks = keys[c][k];
                ks.features[s]++;
                ks.bytes[s] += v.size();
                auto& gs = groups[c][g];
                if (!seen_groups.count(g)) gs.features[s]++;
                gs.bytes[s] += v.size();
            }
            seen_groups.insert(g);
            if (g == "name:<lang>" || g == "name:en") {
                n_lang++;
                if (g == "name:<lang>") lang_not_en = true;
            }
            if (kAltBases.count(g)) any_alt = true;
            const bool in_base = kBaseSet.count(k) > 0;
            const bool in_all = in_all_languages(g);
            for (int set = 0; set < 2; set++) {
                if (set == 0 ? !in_base : !in_all) continue;
                for (int s = 0; s < 2; s++) {
                    if (s == 1 && !shipped) continue;
                    sets[c][s][set].strings_raw++;
                    sets[c][s][set].bytes_raw += v.size();
                }
                for (std::string_view piece : split_values(v)) feature_vals[set].emplace(piece);
            }
        }
        for (int set = 0; set < 2; set++) {
            for (int s = 0; s < 2; s++) {
                if (s == 1 && !shipped) continue;
                auto& sb = sets[c][s][set];
                for (const auto& val : feature_vals[set]) {
                    sb.strings_feature++;
                    sb.bytes_feature += val.size();
                    sb.global.insert(val);
                }
            }
        }

        auto has = [&](const char* k) { return tags.count(k) > 0; };
        auto add_cov = [&](Coverage& cv) {
            cv.features++;
            cv.name += has("name");
            cv.name_en += has("name:en");
            cv.name_en_differs += has("name") && has("name:en") && tags["name"] != tags["name:en"];
            cv.int_name += has("int_name");
            cv.alt_name += has("alt_name");
            cv.official_name += has("official_name");
            cv.old_name += has("old_name");
            cv.short_name += has("short_name");
            cv.loc_name += has("loc_name");
            cv.any_lang += n_lang > 0;
            cv.lang_not_en += lang_not_en;
            cv.any_alt += any_alt;
            cv.no_name_tag += !has("name");
            cv.langs[lang_bucket(n_lang)]++;
        };
        add_cov(cov[c][0]);
        if (shipped) add_cov(cov[c][1]);
        // Subtypes: place=* value, admin boundary/level, POI category.
        add_cov(cov_sub[c][subtype]);
    }

    auto open = [&](const std::string& name) {
        std::ofstream o(dst + "/" + name);
        if (!o) throw std::runtime_error("cannot write " + name);
        return o;
    };
    {
        auto o = open("keys.tsv");
        o << "class\tkey\tgroup\tfeatures_all\tvalue_bytes_all\tfeatures_shipped\tvalue_bytes_shipped\n";
        for (int c = 0; c < kNClasses; c++) {
            std::vector<std::pair<std::string, KeyStat>> v(keys[c].begin(), keys[c].end());
            std::sort(v.begin(), v.end(), [](const auto& a, const auto& b) { return a.second.features[0] > b.second.features[0]; });
            for (const auto& [k, s] : v)
                o << kClasses[c] << "\t" << k << "\t" << group_of(k) << "\t" << s.features[0] << "\t" << s.bytes[0]
                  << "\t" << s.features[1] << "\t" << s.bytes[1] << "\n";
        }
    }
    {
        auto o = open("key_groups.tsv");
        o << "class\tgroup\tfeatures_all\tvalue_bytes_all\tfeatures_shipped\tvalue_bytes_shipped\tdistinct_keys\n";
        for (int c = 0; c < kNClasses; c++) {
            std::map<std::string, uint64_t> distinct;
            for (const auto& [k, s] : keys[c]) distinct[group_of(k)]++;
            for (const auto& [g, s] : groups[c])
                o << kClasses[c] << "\t" << g << "\t" << s.features[0] << "\t" << s.bytes[0] << "\t" << s.features[1]
                  << "\t" << s.bytes[1] << "\t" << distinct[g] << "\n";
        }
    }
    auto cov_header = [](std::ofstream& o, const char* first) {
        o << first << "\tfeatures\tname\tname:en\tname:en!=name\tint_name\talt_name\tofficial_name\told_name\tshort_name"
             "\tloc_name\t>=1 name:<lang> incl en\t>=1 name:<lang> other than en\t>=1 alt-name key\tno plain name";
        for (const char* b : kLangBuckets) o << "\tlangs_" << b;
        o << "\n";
    };
    auto cov_row = [](std::ofstream& o, const Coverage& cv) {
        o << "\t" << cv.features << "\t" << cv.name << "\t" << cv.name_en << "\t" << cv.name_en_differs << "\t"
          << cv.int_name << "\t" << cv.alt_name << "\t" << cv.official_name << "\t" << cv.old_name << "\t"
          << cv.short_name << "\t" << cv.loc_name << "\t" << cv.any_lang << "\t" << cv.lang_not_en << "\t" << cv.any_alt
          << "\t" << cv.no_name_tag;
        for (uint64_t n : cv.langs) o << "\t" << n;
        o << "\n";
    };
    {
        auto o = open("coverage.tsv");
        cov_header(o, "class\tscope");
        for (int c = 0; c < kNClasses; c++)
            for (int s = 0; s < 2; s++) {
                o << kClasses[c] << "\t" << kScopes[s];
                cov_row(o, cov[c][s]);
            }
    }
    {
        auto o = open("coverage_subtype.tsv");
        cov_header(o, "class\tsubtype");
        for (int c = 0; c < kNClasses; c++) {
            std::vector<std::pair<std::string, Coverage>> v(cov_sub[c].begin(), cov_sub[c].end());
            std::sort(v.begin(), v.end(), [](const auto& a, const auto& b) { return a.second.features > b.second.features; });
            if (v.size() > 40) v.resize(40);
            for (const auto& [sub, cv] : v) {
                o << kClasses[c] << "\t" << sub;
                cov_row(o, cv);
            }
        }
    }
    {
        auto o = open("search_bytes.tsv");
        o << "class\tscope\tset\tvalues_raw\tbytes_raw\tstrings_distinct_per_feature\tbytes_distinct_per_feature"
             "\tstrings_distinct_in_class\tbytes_distinct_in_class\n";
        const char* set_names[] = {"base", "all_languages"};
        for (int c = 0; c < kNClasses; c++)
            for (int s = 0; s < 2; s++)
                for (int set = 0; set < 2; set++) {
                    auto& sb = sets[c][s][set];
                    std::vector<const std::string*> sorted;
                    sorted.reserve(sb.global.size());
                    uint64_t gbytes = 0;
                    for (const auto& str : sb.global) {
                        sorted.push_back(&str);
                        gbytes += str.size();
                    }
                    o << kClasses[c] << "\t" << kScopes[s] << "\t" << set_names[set] << "\t" << sb.strings_raw << "\t"
                      << sb.bytes_raw << "\t" << sb.strings_feature << "\t" << sb.bytes_feature << "\t"
                      << sb.global.size() << "\t" << gbytes << "\n";
                    std::sort(sorted.begin(), sorted.end(), [](const std::string* a, const std::string* b) { return *a < *b; });
                    auto strings = open(std::string("strings_") + set_names[set] + "_" + kClasses[c] + "_" + kScopes[s] + ".txt");
                    for (const std::string* str : sorted) strings << *str << "\n";
                    sb.global.clear();
                }
    }
    std::cout << "name_stats: " << lines << " lines, " << bad << " unparsed" << std::endl;
    return 0;
}
