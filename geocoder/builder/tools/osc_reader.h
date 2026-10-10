// OSM change files (.osc, .osc.gz) for pbf-apply, read as osmium's XML
// parser reads them (libosmium io/detail/xml_input_format.hpp): objects in
// a <delete> section are invisible unless a visible attribute says
// otherwise, coordinates round to 1e-7 degrees, and only the first node
// list, member list and tag list of an object count.
//
// A file is inflated on the calling thread in chunks cut before an object
// element, and each chunk is parsed on its own core: the text between two
// structural tags (<osmChange>, <create>, ...) holds whole object elements,
// so expat parses it wrapped in a dummy root; which section such a run sits
// in is settled afterwards by replaying the structural tags in file order.
#pragma once

#include <cctype>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <expat.h>
#include <zlib.h>

#include "osm_objects.h"
#include "parallel.h"

// Change file text a parse task gets (it is cut before an object element
// at or after this many bytes).
constexpr size_t kOscChunkBytes = size_t(8) << 20;

namespace osc {

// --- Attribute values, parsed as osmium parses them ---

// osmium::string_to_object_id.
inline int64_t parse_id(const char* s) {
    if (*s != '\0' && !std::isspace(static_cast<unsigned char>(*s))) {
        char* end = nullptr;
        long long id = std::strtoll(s, &end, 10);
        if (id != LLONG_MIN && id != LLONG_MAX && *end == '\0') return id;
    }
    throw std::runtime_error(std::string("illegal id: '") + s + "'");
}

// osmium::detail::string_to_ulong (version, changeset, uid): "-1" is 0.
inline uint32_t parse_u32(const char* s, const char* name) {
    if (s[0] == '-' && s[1] == '1' && s[2] == '\0') return 0;
    if (*s != '\0' && *s != '-' && !std::isspace(static_cast<unsigned char>(*s))) {
        char* end = nullptr;
        unsigned long v = std::strtoul(s, &end, 10);
        if (v < UINT32_MAX && *end == '\0') return static_cast<uint32_t>(v);
    }
    throw std::runtime_error(std::string("illegal ") + name + ": '" + s + "'");
}

// osmium::detail::parse_timestamp plus OSMObject::set_timestamp's check
// for trailing text: yyyy-mm-ddThh:mm:ss, optional fraction, then Z.
inline uint32_t parse_timestamp(const char* s) {
    auto digit = [&](int i) { return s[i] >= '0' && s[i] <= '9'; };
    auto num = [&](int i, int n) {
        int v = 0;
        for (int k = 0; k < n; k++) v = v * 10 + (s[i + k] - '0');
        return v;
    };
    static const int mon_lengths[12] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool shape = std::strlen(s) >= 19 && digit(0) && digit(1) && digit(2) && digit(3) && s[4] == '-' && digit(5) &&
                 digit(6) && s[7] == '-' && digit(8) && digit(9) && s[10] == 'T' && digit(11) && digit(12) &&
                 s[13] == ':' && digit(14) && digit(15) && s[16] == ':' && digit(17) && digit(18);
    const char* end = s + 19;
    if (shape && *end != 'Z' && (*end == '.' || *end == ',')) {
        const char* f = end + 1;
        if (*f >= '0' && *f <= '9') {
            while (*f >= '0' && *f <= '9') f++;
            if (*f == 'Z') end = f;
        }
    }
    if (shape && *end == 'Z') {
        std::tm tm{};
        tm.tm_year = num(0, 4) - 1900;
        tm.tm_mon = num(5, 2) - 1;
        tm.tm_mday = num(8, 2);
        tm.tm_hour = num(11, 2);
        tm.tm_min = num(14, 2);
        tm.tm_sec = num(17, 2);
        if (tm.tm_year >= 0 && tm.tm_mon >= 0 && tm.tm_mon <= 11 && tm.tm_mday >= 1 &&
            tm.tm_mday <= mon_lengths[tm.tm_mon] && tm.tm_hour <= 23 && tm.tm_min <= 59 && tm.tm_sec <= 60) {
            if (end[1] != '\0') throw std::runtime_error("can not parse timestamp: garbage after timestamp");
            return static_cast<uint32_t>(timegm(&tm));
        }
    }
    throw std::runtime_error(std::string("can not parse timestamp: '") + s + "'");
}

// osmium::detail::string_to_location_coordinate plus Location::set_lat's
// check for trailing text: 1e-7 degrees, rounded half away from zero on
// the eighth decimal.
inline int32_t parse_coordinate(const char* full) {
    const char* str = full;
    auto fail = [&]() -> int32_t {
        throw std::runtime_error(std::string("wrong format for coordinate: '") + full + "'");
    };
    int64_t result = 0;
    int sign = 1;
    int64_t scale = 8;
    int max_digits = 10;
    if (*str == '-') {
        sign = -1;
        ++str;
    }
    if (*str != '.') {
        if (*str < '0' || *str > '9') return fail();
        result = *str++ - '0';
        while (*str >= '0' && *str <= '9' && max_digits > 0) {
            result = result * 10 + (*str++ - '0');
            --max_digits;
        }
        if (max_digits == 0) return fail();
    } else if (str[1] < '0' || str[1] > '9') {
        return fail();
    }
    if (*str == '.') {
        ++str;
        for (; scale > 0 && *str >= '0' && *str <= '9'; --scale, ++str) result = result * 10 + (*str - '0');
        max_digits = 20;
        while (*str >= '0' && *str <= '9' && max_digits > 0) {
            ++str;
            --max_digits;
        }
        if (max_digits == 0) return fail();
    }
    if (*str == 'e' || *str == 'E') {
        ++str;
        int esign = 1;
        if (*str == '-') {
            esign = -1;
            ++str;
        }
        if (*str < '0' || *str > '9') return fail();
        int64_t e = *str++ - '0';
        max_digits = 5;
        while (*str >= '0' && *str <= '9' && max_digits > 0) {
            e = e * 10 + (*str++ - '0');
            --max_digits;
        }
        if (max_digits == 0) return fail();
        scale += e * esign;
    }
    if (scale < 0) {
        for (; scale < 0 && result > 0; ++scale) result /= 10;
    } else {
        for (; scale > 0; --scale) result *= 10;
    }
    result = (result + 5) / 10 * sign;
    if (result > INT32_MAX || result < INT32_MIN) return fail();
    if (*str != '\0') throw std::runtime_error(std::string("characters after coordinate: '") + str + "'");
    return static_cast<int32_t>(result);
}

// --- Structure ---

// The tags that frame objects; any other tag sits inside a run.
enum class Frame : uint8_t { Osm, OsmChange, Create, Modify, Delete };

struct FrameEvent {
    Frame frame;
    bool close;
    bool version_ok;  // a root's version attribute is "0.6"
};

// Text between frame tags: whole object elements, parsed into
// objects [first_object, end_object) of the chunk.
struct Run {
    uint32_t first_object = 0, end_object = 0;
    uint32_t events_before = 0;  // frame events of the chunk before the run
};

inline bool is_name_end(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '/' || c == '>';
}

// The frame named at p (just past "<" or "</"), if any.
inline std::optional<Frame> frame_at(const char* p, const char* end) {
    static const std::pair<std::string_view, Frame> names[] = {
        {"osmChange", Frame::OsmChange}, {"osm", Frame::Osm},       {"create", Frame::Create},
        {"modify", Frame::Modify},       {"delete", Frame::Delete},
    };
    for (const auto& [name, frame] : names) {
        if (size_t(end - p) > name.size() && std::memcmp(p, name.data(), name.size()) == 0 && is_name_end(p[name.size()]))
            return frame;
    }
    return std::nullopt;
}

// Whether an object element starts at p ("<node", "<way", "<relation").
inline bool is_object_start(const char* p, const char* end) {
    static const std::string_view names[] = {"<node", "<way", "<relation"};
    for (std::string_view name : names) {
        if (size_t(end - p) > name.size() && std::memcmp(p, name.data(), name.size()) == 0 && is_name_end(p[name.size()]))
            return true;
    }
    return false;
}

// The ">" closing the tag that starts at p; attribute values may hold ">".
inline const char* tag_end(const char* p, const char* end) {
    char quote = 0;
    for (; p < end; p++) {
        if (quote) {
            if (*p == quote) quote = 0;
        } else if (*p == '"' || *p == '\'') {
            quote = *p;
        } else if (*p == '>') {
            return p;
        }
    }
    throw std::runtime_error("OSC XML error: unterminated tag");
}

// Whether the root tag [p, end) has version="0.6", the one version osmium reads.
inline bool root_version_ok(std::string_view tag) {
    for (size_t at = tag.find("version"); at != std::string_view::npos; at = tag.find("version", at + 1)) {
        if (at == 0 || !std::isspace(static_cast<unsigned char>(tag[at - 1]))) continue;
        size_t q = at + 7;
        while (q < tag.size() && std::isspace(static_cast<unsigned char>(tag[q]))) q++;
        if (q >= tag.size() || tag[q] != '=') continue;
        q++;
        while (q < tag.size() && std::isspace(static_cast<unsigned char>(tag[q]))) q++;
        if (q >= tag.size() || (tag[q] != '"' && tag[q] != '\'')) continue;
        char quote = tag[q];
        size_t close = tag.find(quote, q + 1);
        return close != std::string_view::npos && tag.substr(q + 1, close - q - 1) == "0.6";
    }
    return false;
}

// The last object element start in [data + 1, data + n), or 0.
inline size_t last_object_start(const char* data, size_t n) {
    const char* end = data + n;
    for (size_t limit = n; limit > 1;) {
        const void* hit = memrchr(data + 1, '<', limit - 1);
        if (!hit) return 0;
        const char* p = static_cast<const char*>(hit);
        if (is_object_start(p, end)) return size_t(p - data);
        limit = size_t(p - data);
    }
    return 0;
}

}  // namespace osc

// A piece of a change file, parsed.
struct ChangeChunk {
    ObjectStore store;
    StringArena strings;
    std::vector<uint8_t> visible_attr;  // per object: 0 none, 1 "true", 2 "false"
    std::vector<osc::FrameEvent> events;
    std::vector<osc::Run> runs;
    size_t file = 0;         // index of its change file
    size_t file_offset = 0;  // where its text starts in the (inflated) file
};

namespace osc {

// Parses runs of object elements with one expat parser.
class RunParser {
public:
    RunParser() : parser_(XML_ParserCreate(nullptr)) {
        if (!parser_) throw std::runtime_error("XML_ParserCreate failed");
    }
    ~RunParser() { XML_ParserFree(parser_); }
    RunParser(const RunParser&) = delete;
    RunParser& operator=(const RunParser&) = delete;

    // Appends the objects of [data, data + n) to out.
    void parse(const char* data, size_t n, ChangeChunk& out) {
        XML_ParserReset(parser_, nullptr);
        XML_SetUserData(parser_, this);
        XML_SetElementHandler(parser_, on_start, on_end);
        out_ = &out;
        depth_ = 0;
        error_ = nullptr;
        static const char open[] = "<r>", close[] = "</r>";
        bool ok = XML_Parse(parser_, open, 3, XML_FALSE) != XML_STATUS_ERROR &&
                  XML_Parse(parser_, data, static_cast<int>(n), XML_FALSE) != XML_STATUS_ERROR &&
                  XML_Parse(parser_, close, 4, XML_TRUE) != XML_STATUS_ERROR;
        if (error_) std::rethrow_exception(error_);
        if (!ok)
            throw std::runtime_error(std::string("OSC XML error: ") + XML_ErrorString(XML_GetErrorCode(parser_)));
    }

private:
    // A node list, member list or tag list: osmium builds a new list when
    // an element of it follows an element of another, and reads only the
    // first, so elements after an interruption are dropped.
    enum class List : uint8_t { Unstarted, Open, Closed };

    static void XMLCALL on_start(void* self, const XML_Char* name, const XML_Char** attrs) {
        auto* p = static_cast<RunParser*>(self);
        if (p->error_) return;
        try {
            p->start(name, attrs);
        } catch (...) {
            p->error_ = std::current_exception();
            XML_StopParser(p->parser_, XML_FALSE);
        }
    }

    static void XMLCALL on_end(void* self, const XML_Char*) {
        auto* p = static_cast<RunParser*>(self);
        if (p->error_) return;
        try {
            p->end();
        } catch (...) {
            p->error_ = std::current_exception();
            XML_StopParser(p->parser_, XML_FALSE);
        }
    }

    // Whether an element of `list` is kept; another list it interrupts closes.
    static bool admit(List& list, List& other) {
        if (other == List::Open) other = List::Closed;
        if (list == List::Unstarted) list = List::Open;
        return list == List::Open;
    }

    void start(const char* name, const char** attrs) {
        ++depth_;
        if (depth_ == 1) return;  // the dummy root
        if (depth_ == 2) {
            if (!std::strcmp(name, "node")) begin_object(OsmType::Node, attrs);
            else if (!std::strcmp(name, "way")) begin_object(OsmType::Way, attrs);
            else if (!std::strcmp(name, "relation")) begin_object(OsmType::Relation, attrs);
            else if (!std::strcmp(name, "bounds")) skip_ = true;
            else throw std::runtime_error(std::string("OSC: unsupported element <") + name + ">");
            return;
        }
        if (depth_ > 3 || skip_) throw std::runtime_error(std::string("OSC: element <") + name + "> nested too deep");
        if (!std::strcmp(name, "tag")) {
            add_tag(attrs);
        } else if (obj_.type == OsmType::Way && !std::strcmp(name, "nd")) {
            add_node_ref(attrs);
        } else if (obj_.type == OsmType::Relation && !std::strcmp(name, "member")) {
            add_member(attrs);
        } else if (obj_.type != OsmType::Node && (!std::strcmp(name, "bbox") || !std::strcmp(name, "bounds"))) {
            // osmium ignores an object's bounding box
        } else {
            throw std::runtime_error(std::string("OSC: unknown element <") + name + "> in an object");
        }
    }

    void end() {
        if (--depth_ != 1) return;
        if (skip_) {
            skip_ = false;
            return;
        }
        ObjectStore& s = out_->store;
        obj_.tag_count = static_cast<uint32_t>(s.tags.size()) - obj_.first_tag;
        obj_.ref_count = static_cast<uint32_t>(s.refs.size()) - obj_.first_ref;
        obj_.member_count = static_cast<uint32_t>(s.members.size()) - obj_.first_member;
        s.objects.push_back(obj_);
        out_->visible_attr.push_back(visible_attr_);
    }

    // osmium's init_object.
    void begin_object(OsmType type, const char** attrs) {
        ObjectStore& s = out_->store;
        obj_ = OsmObject{};
        obj_.type = type;
        obj_.first_tag = static_cast<uint32_t>(s.tags.size());
        obj_.first_ref = static_cast<uint32_t>(s.refs.size());
        obj_.first_member = static_cast<uint32_t>(s.members.size());
        visible_attr_ = 0;
        tags_ = List::Unstarted;
        items_ = List::Unstarted;
        int32_t lon = kUndefinedCoordinate, lat = kUndefinedCoordinate;
        for (; *attrs; attrs += 2) {
            const char* k = attrs[0];
            const char* v = attrs[1];
            if (!std::strcmp(k, "lon")) lon = parse_coordinate(v);
            else if (!std::strcmp(k, "lat")) lat = parse_coordinate(v);
            else if (!std::strcmp(k, "user")) obj_.user = out_->strings.add(v);
            else if (!std::strcmp(k, "id")) obj_.id = parse_id(v);
            else if (!std::strcmp(k, "version")) obj_.version = parse_u32(v, "version");
            else if (!std::strcmp(k, "changeset")) obj_.changeset = parse_u32(v, "changeset");
            else if (!std::strcmp(k, "timestamp")) obj_.timestamp = parse_timestamp(v);
            else if (!std::strcmp(k, "uid")) obj_.uid = parse_u32(v, "user id");
            else if (!std::strcmp(k, "visible")) {
                if (!std::strcmp(v, "true")) visible_attr_ = 1;
                else if (!std::strcmp(v, "false")) visible_attr_ = 2;
                else throw std::runtime_error("Unknown value for visible attribute (allowed is 'true' or 'false')");
            }
        }
        if (type == OsmType::Node && lon != kUndefinedCoordinate && lat != kUndefinedCoordinate) {
            obj_.lon = lon;
            obj_.lat = lat;
        }
    }

    void add_tag(const char** attrs) {
        if (!admit(tags_, items_)) return;
        std::string_view k, v;
        for (; *attrs; attrs += 2) {
            if (attrs[0][0] == 'k' && attrs[0][1] == '\0') k = attrs[1];
            else if (attrs[0][0] == 'v' && attrs[0][1] == '\0') v = attrs[1];
        }
        out_->store.tags.push_back({out_->strings.add(k), out_->strings.add(v)});
    }

    void add_node_ref(const char** attrs) {
        int64_t ref = 0;
        for (; *attrs; attrs += 2) {
            if (!std::strcmp(attrs[0], "ref")) ref = parse_id(attrs[1]);
            else if (!std::strcmp(attrs[0], "lon") || !std::strcmp(attrs[0], "lat")) parse_coordinate(attrs[1]);
        }
        if (admit(items_, tags_)) out_->store.refs.push_back(ref);
    }

    void add_member(const char** attrs) {
        OsmMember m;
        bool has_type = false, has_ref = false;
        std::string_view role;
        for (; *attrs; attrs += 2) {
            const char* k = attrs[0];
            const char* v = attrs[1];
            if (!std::strcmp(k, "type")) {
                has_type = v[0] == 'n' || v[0] == 'w' || v[0] == 'r';
                m.type = v[0] == 'w' ? OsmType::Way : v[0] == 'r' ? OsmType::Relation : OsmType::Node;
            } else if (!std::strcmp(k, "ref")) {
                m.ref = parse_id(v);
                has_ref = true;
            } else if (!std::strcmp(k, "role")) {
                role = v;
            }
        }
        if (!has_type) throw std::runtime_error("Unknown type on relation <member>");
        if (!has_ref) throw std::runtime_error("Missing ref on relation <member>");
        if (!admit(items_, tags_)) return;
        m.role = out_->strings.add(role);
        out_->store.members.push_back(m);
    }

    XML_Parser parser_;
    ChangeChunk* out_ = nullptr;
    int depth_ = 0;
    bool skip_ = false;
    std::exception_ptr error_;
    OsmObject obj_;
    uint8_t visible_attr_ = 0;
    List tags_ = List::Unstarted;
    List items_ = List::Unstarted;  // node refs or members
};

inline bool all_space(const char* p, const char* end) {
    for (; p < end; p++)
        if (!std::isspace(static_cast<unsigned char>(*p))) return false;
    return true;
}

// Splits `xml` at its frame tags and parses the runs between them.
inline void parse_chunk(const std::string& xml, RunParser& parser, ChangeChunk& out) {
    const char* begin = xml.data();
    const char* end = begin + xml.size();
    if (out.file_offset == 0 && xml.compare(0, 3, "\xEF\xBB\xBF") == 0) begin += 3;  // UTF-8 byte order mark
    const char* run = begin;
    auto flush = [&](const char* to) {
        if (all_space(run, to)) return;
        Run r;
        r.first_object = static_cast<uint32_t>(out.store.objects.size());
        r.events_before = static_cast<uint32_t>(out.events.size());
        parser.parse(run, size_t(to - run), out);
        r.end_object = static_cast<uint32_t>(out.store.objects.size());
        out.runs.push_back(r);
    };
    for (const char* p = begin; (p = static_cast<const char*>(std::memchr(p, '<', size_t(end - p))));) {
        if (p + 1 >= end) break;
        if (p[1] == '!') throw std::runtime_error("OSC: comments, CDATA and DOCTYPE are not supported");
        if (p[1] == '?') {
            // The XML declaration, which must open the file. Runs are
            // parsed without it, so as UTF-8.
            const char* q = static_cast<const char*>(memmem(p, size_t(end - p), "?>", 2));
            if (!q) throw std::runtime_error("OSC XML error: unterminated declaration");
            if (out.file_offset != 0 || !out.events.empty() || !all_space(run, p))
                throw std::runtime_error("OSC: processing instruction after the start of the file");
            std::string_view decl(p, size_t(q - p));
            size_t enc = decl.find("encoding");
            if (enc != std::string_view::npos) {
                size_t quote = decl.find_first_of("\"'", enc);
                std::string_view value = quote == std::string_view::npos ? "" : decl.substr(quote + 1, 5);
                if (value != "UTF-8" && value != "utf-8") throw std::runtime_error("OSC: only UTF-8 files are supported");
            }
            run = p = q + 2;
            continue;
        }
        bool close = p[1] == '/';
        std::optional<Frame> frame = frame_at(p + 1 + close, end);
        if (!frame) {
            p++;
            continue;
        }
        flush(p);
        const char* gt = tag_end(p, end);
        bool self_closing = !close && gt[-1] == '/';
        bool root = *frame == Frame::Osm || *frame == Frame::OsmChange;
        bool version_ok = root && !close && root_version_ok(std::string_view(p, size_t(gt - p)));
        out.events.push_back({*frame, close, version_ok});
        if (self_closing) out.events.push_back({*frame, true, false});
        run = p = gt + 1;
    }
    flush(end);
}

// Replays the frame tags of one file's chunks in order and sets each
// object's visibility from the section it sits in.
inline void settle_sections(ChangeChunk* const* chunks, size_t n, const std::string& path) {
    enum class State { Before, InOsm, InChange, InSection, After };
    State state = State::Before;
    Frame section = Frame::Create;
    auto fail = [&](const char* what) { throw std::runtime_error(path + ": " + what); };
    auto apply = [&](const FrameEvent& e) {
        bool root = e.frame == Frame::Osm || e.frame == Frame::OsmChange;
        if (root && !e.close) {
            if (state != State::Before) fail("root element inside the document");
            if (!e.version_ok) fail("osmChange/osm version must be 0.6");
            state = e.frame == Frame::Osm ? State::InOsm : State::InChange;
        } else if (root) {
            if (state != (e.frame == Frame::Osm ? State::InOsm : State::InChange)) fail("mismatched root close tag");
            state = State::After;
        } else if (!e.close) {
            if (state != State::InChange) fail("<create>/<modify>/<delete> only allowed directly in <osmChange>");
            state = State::InSection;
            section = e.frame;
        } else {
            if (state != State::InSection || section != e.frame) fail("mismatched section close tag");
            state = State::InChange;
        }
    };
    for (size_t c = 0; c < n; c++) {
        ChangeChunk& chunk = *chunks[c];
        size_t ev = 0;
        for (const Run& r : chunk.runs) {
            while (ev < r.events_before) apply(chunk.events[ev++]);
            if (r.first_object == r.end_object) continue;
            if (state == State::Before || state == State::After) fail("objects outside the root element");
            bool in_delete = state == State::InSection && section == Frame::Delete;
            for (uint32_t i = r.first_object; i < r.end_object; i++) {
                uint8_t attr = chunk.visible_attr[i];
                chunk.store.objects[i].visible = attr ? attr == 1 : !in_delete;
            }
        }
        while (ev < chunk.events.size()) apply(chunk.events[ev++]);
        std::vector<uint8_t>().swap(chunk.visible_attr);
    }
    if (state != State::After) fail("missing root close tag (truncated file?)");
}

}  // namespace osc

// Reads change files into chunks, in file order and within a file in
// reading order. A gzip file is inflated as it is read (osmium's gzip
// decompressor also uses gzread, which reads plain files unchanged).
inline std::vector<std::unique_ptr<ChangeChunk>> read_change_files(const std::vector<std::string>& paths,
                                                                   size_t chunk_bytes = kOscChunkBytes,
                                                                   unsigned threads = 0) {
    if (threads == 0) threads = parallel_threads();
    std::vector<std::unique_ptr<ChangeChunk>> chunks;
    std::vector<std::unique_ptr<osc::RunParser>> parsers(threads);
    struct Task {
        ChangeChunk* chunk;
        std::string xml;
    };
    for (size_t f = 0; f < paths.size(); f++) {
        const std::string& path = paths[f];
        if (path.size() > 4 && path.compare(path.size() - 4, 4, ".bz2") == 0)
            throw std::runtime_error(path + ": bzip2 change files are not supported");
        gzFile gz = gzopen(path.c_str(), "rb");
        if (!gz) throw std::runtime_error("cannot open " + path);
        struct Close {
            gzFile gz;
            ~Close() { gzclose(gz); }
        } closer{gz};
        gzbuffer(gz, 1 << 20);
        size_t first_chunk = chunks.size();
        size_t file_offset = 0;
        std::string pending;
        bool eof = false;
        auto next = [&]() -> std::optional<Task> {
            while (!eof) {
                size_t have = pending.size();
                pending.resize(have + chunk_bytes);
                int got = gzread(gz, pending.data() + have, static_cast<unsigned>(chunk_bytes));
                if (got < 0) {
                    int err = 0;
                    throw std::runtime_error(path + ": " + gzerror(gz, &err));
                }
                pending.resize(have + size_t(got));
                eof = got == 0;
                size_t cut = eof ? pending.size() : osc::last_object_start(pending.data(), pending.size());
                if (cut == 0) continue;  // one element longer than a chunk: read on
                std::string rest = pending.substr(cut);
                pending.resize(cut);
                chunks.push_back(std::make_unique<ChangeChunk>());
                chunks.back()->file = f;
                chunks.back()->file_offset = file_offset;
                file_offset += cut;
                Task t{chunks.back().get(), std::move(pending)};
                pending = std::move(rest);
                if (!t.xml.empty()) return t;
                chunks.pop_back();
            }
            return std::nullopt;
        };
        parallel_stream<Task>(next, [&](Task& t, unsigned worker) {
            if (!parsers[worker]) parsers[worker] = std::make_unique<osc::RunParser>();
            osc::parse_chunk(t.xml, *parsers[worker], *t.chunk);
        }, threads);
        std::vector<ChangeChunk*> mine;
        for (size_t c = first_chunk; c < chunks.size(); c++) mine.push_back(chunks[c].get());
        osc::settle_sections(mine.data(), mine.size(), path);
    }
    return chunks;
}
