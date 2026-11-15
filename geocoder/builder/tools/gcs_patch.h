// The patcher's side of the in-process codecs: every today's-format file of an encoded family is
// a virtual old file (a SequentialFileReader reads it from the family's decoder) and a virtual
// output (open_out hands back a FILE* whose bytes go to the family's encoder). A family (the
// street and the interpolation ways, way_files.h; the address files, addr_files.h; the POI
// files, R2 and V2: poi_files.h; the place files, place_files.h; each of the four side files,
// side_blocks.h) is encoded in every dir under today's names, the place files' logical stream
// as place_nodes.bin. No today's-format file of an encoded family touches the disk. A family a
// patch leaves unchanged goes through neither: the patcher copies its files (copied_files).
#pragma once
#include <malloc.h>
#include <map>
#include <set>
#include <stdio.h>
#include <stdio_ext.h>
#include "gcs_async.h"
#include "gcs_codec.h"
#include "sequential_file_reader.h"

namespace gcs {

// The client list's sizes by today's name (Store::expect_out).
using ListedSizes = std::map<std::string, uint64_t>;

// A family and its codecs. beside: the files a dir holds of it besides today's, which the encoder
// writes and no section names. decoder: over the old dir's files. encoder: into the new dir (sizes:
// the files the patch lists, for one that writes a file no section names). decoded_size: the
// decoded size of a port, from dir's files alone and with no decoder made (the POI replay reads
// the files itself).
struct FamilyDesc {
    const char* name;
    std::vector<const char*> today;
    std::vector<const char*> beside;
    Decoder* (*decoder)(const std::string& dir);
    Encoder* (*encoder)(const std::string& dir, const ListedSizes& sizes);
    uint64_t (*decoded_size)(const std::string& dir, int port);
};
// The encoded families.
static const std::vector<FamilyDesc>& families() {
    static const std::vector<FamilyDesc> f = {
        {"streets", {"street_ways.bin", "street_nodes.bin"}, {},
         [](const std::string& dir) -> Decoder* { return new ways::Dec(dir, way_files::STREETS); },
         [](const std::string& dir, const ListedSizes&) -> Encoder* { return new ways::Enc(dir, way_files::STREETS); },
         [](const std::string& dir, int port) { return ways::decoded_size(dir, way_files::STREETS, port); }},
        {"interps", {"interp_ways.bin", "interp_nodes.bin"}, {},
         [](const std::string& dir) -> Decoder* { return new ways::Dec(dir, way_files::INTERPS); },
         [](const std::string& dir, const ListedSizes&) -> Encoder* { return new ways::Enc(dir, way_files::INTERPS); },
         [](const std::string& dir, int port) { return ways::decoded_size(dir, way_files::INTERPS, port); }},
        {"way_parents", {"way_parents.bin"}, {},
         [](const std::string& dir) -> Decoder* { return new side::Dec(dir, "way_parents"); },
         [](const std::string& dir, const ListedSizes&) -> Encoder* { return new side::Enc(dir, "way_parents"); },
         [](const std::string& dir, int) { return side::decoded_size(dir, "way_parents"); }},
        {"way_postcodes", {"way_postcodes.bin"}, {},
         [](const std::string& dir) -> Decoder* { return new side::Dec(dir, "way_postcodes"); },
         [](const std::string& dir, const ListedSizes&) -> Encoder* { return new side::Enc(dir, "way_postcodes"); },
         [](const std::string& dir, int) { return side::decoded_size(dir, "way_postcodes"); }},
        {"interp_postcodes", {"interp_postcodes.bin"}, {},
         [](const std::string& dir) -> Decoder* { return new side::Dec(dir, "interp_postcodes"); },
         [](const std::string& dir, const ListedSizes&) -> Encoder* { return new side::Enc(dir, "interp_postcodes"); },
         [](const std::string& dir, int) { return side::decoded_size(dir, "interp_postcodes"); }},
        {"addr_postcodes", {"addr_postcodes.bin"}, {},
         [](const std::string& dir) -> Decoder* { return new side::Dec(dir, "addr_postcodes"); },
         [](const std::string& dir, const ListedSizes&) -> Encoder* { return new side::Enc(dir, "addr_postcodes"); },
         [](const std::string& dir, int) { return side::decoded_size(dir, "addr_postcodes"); }},
        {"addr", {"addr_points.bin", "addr_vertices.bin"}, {addr_files::INDEX_FILE},
         [](const std::string& dir) -> Decoder* { return new addr::Dec(dir); },
         [](const std::string& dir, const ListedSizes& sizes) -> Encoder* {
             // The encoder writes the index beside the two files its sections name.
             if (!sizes.count(addr_files::INDEX_FILE)) die("addr: the patch lists no %s", addr_files::INDEX_FILE);
             return new addr::Enc(dir);
         },
         [](const std::string& dir, int port) { return addr::decoded_size(dir, port); }},
        {"poi", {"poi_records.bin", "poi_vertices.bin"}, {},
         [](const std::string& dir) -> Decoder* { return new poi::Dec(dir); },
         [](const std::string& dir, const ListedSizes&) -> Encoder* { return new poi::Enc(dir); }, poi::decoded_size},
        {"places", {"place_nodes.bin"}, {place_files::CELLS_FILE, place_files::HIDDEN_FILE},
         [](const std::string& dir) -> Decoder* { return new place::Dec(dir); },
         [](const std::string& dir, const ListedSizes&) -> Encoder* { return new place::Enc(dir); },
         [](const std::string& dir, int) { return place::decoded_size(dir); }},
    };
    return f;
}

// Heap bytes in use (glibc), for the phase lines.
inline size_t heap_in_use() {
    struct mallinfo2 mi = mallinfo2();
    return mi.uordblks + mi.hblkhd;
}

struct Store {
    bool threads = false;
    std::string cur_dir, out_dir;
    std::vector<int> fam_out;  // per family: the new dir holds it (plan_outputs)
    std::map<std::string, std::pair<int, int>> file_port;  // today's name -> (family, port)
    std::vector<std::unique_ptr<Decoder>> dec;
    std::vector<std::unique_ptr<Encoder>> enc;
    std::vector<std::unique_ptr<AsyncDec>> adec;  // --threads: the codec's worker
    std::vector<std::unique_ptr<AsyncEnc>> aenc;
    std::map<std::string, uint64_t> expect_out;           // today's name -> bytes (the client list)
    std::vector<std::string> report;                      // per family: queue peaks
    std::string sink_error;
    std::set<std::string> opened_old, opened_new;
    struct Cookie;
    std::map<Cookie*, FILE*> sinks;  // sinks not closed yet
    bool abandoned = false;

    // A failed run (exit after main's catch, or any other way out): the sinks still open are closed
    // with their buffered bytes dropped and the codec threads stopped, before the encoders go. Else
    // exit's flush of the open streams would write into the destroyed Store, and a worker waiting
    // for a close that never comes would be joined forever.
    void abandon() {
        abandoned = true;
        std::vector<FILE*> open;
        for (auto& kv : sinks) open.push_back(kv.second);
        for (FILE* f : open) fclose(f);
        sinks.clear();
        aenc.clear();
        adec.clear();
        enc.clear();
        dec.clear();
    }
    ~Store() { abandon(); }

    void init(const std::string& cur, const std::string& out) {
        cur_dir = cur;
        out_dir = out;
        const auto& F = families();
        dec.resize(F.size());
        enc.resize(F.size());
        adec.resize(F.size());
        aenc.resize(F.size());
        for (size_t fi = 0; fi < F.size(); fi++)
            for (size_t p = 0; p < F[fi].today.size(); p++) file_port[F[fi].today[p]] = {int(fi), int(p)};
    }
    // Once expect_out holds the patch's client list: a family the new dir drops whole (it lists
    // none of its today's files) has no output, and one it ships in part can't be encoded.
    void plan_outputs() {
        const auto& F = families();
        fam_out.assign(F.size(), 0);
        for (size_t fi = 0; fi < F.size(); fi++) {
            size_t listed = 0;
            for (const char* t : F[fi].today) listed += expect_out.count(t);
            if (listed && listed < F[fi].today.size())
                die("family %s: the patch ships %zu of its %zu files", F[fi].name, listed, F[fi].today.size());
            fam_out[fi] = listed != 0;
        }
    }
    bool is_encoded(const std::string& name) const { return file_port.count(name) != 0; }
    // The family a today's-format file is of; null for a file of none.
    const FamilyDesc* family_of(const std::string& name) const {
        const auto it = file_port.find(name);
        return it == file_port.end() ? nullptr : &families()[size_t(it->second.first)];
    }
    // The files a dir holds of a family the patcher copies from the old dir as they sit, in place
    // of its codecs: today's, then those beside them. Today's count as opened, old and new, so
    // no other section decodes or encodes them, and a family the new dir ships lists those beside
    // them too.
    std::vector<std::string> copied_files(const FamilyDesc& f) {
        std::vector<std::string> names;
        for (const char* t : f.today) {
            if (!opened_old.insert(t).second) die("old %s opened twice", t);
            if (!opened_new.insert(t).second) die("new %s opened twice", t);
            names.push_back(t);
        }
        for (const char* b : f.beside) {
            if (expect_out.count(f.today[0]) && !expect_out.count(b)) die("%s: the patch lists no %s", f.name, b);
            names.push_back(b);
        }
        return names;
    }
    std::string path_of(const std::string& name) const { return cur_dir + "/" + name; }
    // Decoders and encoders are made on first use and freed when their last port closes.
    Decoder* decoder(int fi) {
        if (!dec[size_t(fi)]) {
            Decoder* d = families()[size_t(fi)].decoder(cur_dir);
            dec[size_t(fi)].reset(d);
            if (threads) adec[size_t(fi)].reset(new AsyncDec(d));
        }
        return dec[size_t(fi)].get();
    }
    Encoder* encoder(int fi) {
        if (!enc[size_t(fi)]) {
            Encoder* e = families()[size_t(fi)].encoder(out_dir, expect_out);
            enc[size_t(fi)].reset(e);
            if (threads) aenc[size_t(fi)].reset(new AsyncEnc(e));
        }
        return enc[size_t(fi)].get();
    }

    // --- old side ---
    // The decoded size check for require_old_file_size, from the old files alone
    // (FamilyDesc::decoded_size).
    void check_old_size(const std::string& name, uint64_t expected) {
        auto [fi, port] = file_port.at(name);
        const uint64_t ds = families()[size_t(fi)].decoded_size(cur_dir, port);
        if (ds != expected)
            die("old %s decodes to %llu bytes, the patch expects %llu", name.c_str(), (unsigned long long)ds, (unsigned long long)expected);
    }
    struct PortSource : StreamSource {
        Store* s;
        int fi, port;
        PortSource(Store* st, int f, int p) : s(st), fi(f), port(p) {}
        size_t read(char* dst, size_t max) override {
            Decoder* d = s->decoder(fi);
            if (AsyncDec* a = s->adec[size_t(fi)].get()) return a->read(port, dst, max);
            return d->read(port, dst, max);
        }
        bool skip(uint64_t n) override {
            Decoder* d = s->decoder(fi);
            if (AsyncDec* a = s->adec[size_t(fi)].get()) a->skip(port, n);
            else d->skip(port, n);
            return true;
        }
        ~PortSource() override { s->close_source(fi, port); }
    };
    std::unique_ptr<StreamSource> open_source(const std::string& path) {
        if (path.compare(0, cur_dir.size() + 1, cur_dir + "/") != 0) return nullptr;
        const std::string name = path.substr(cur_dir.size() + 1);
        auto it = file_port.find(name);
        if (it == file_port.end()) return nullptr;
        Decoder* d = decoder(it->second.first);
        if (!opened_old.insert(name).second) die("old %s opened twice", name.c_str());
        if (!adec[size_t(it->second.first)]) d->state[size_t(it->second.second)] = 1;
        return std::unique_ptr<StreamSource>(new PortSource(this, it->second.first, it->second.second));
    }
    void close_source(int fi, int port) {
        Decoder* d = dec[size_t(fi)].get();
        if (!d) return;
        std::string r = std::string("decode ") + families()[size_t(fi)].name;
        if (AsyncDec* a = adec[size_t(fi)].get()) {
            if (!a->close(port)) return;
            a->shutdown();
            r += ": thread cpu " + std::to_string(a->cpu_s) + " s, ring peaks";
            for (size_t p = 0; p < a->ring.size(); p++) r += " " + std::to_string(a->peak(int(p)));
            r += " B, queue peaks";
            for (size_t p = 0; p < d->out.size(); p++) r += " " + std::to_string(d->peak(int(p)));
            report.push_back(r + " B");
            adec[size_t(fi)].reset();
            dec[size_t(fi)].reset();
            return;
        }
        d->state[size_t(port)] = 2;
        d->out[size_t(port)].reset();
        bool all = true;
        for (int st : d->state) all &= st == 2;
        if (!all) return;
        r += ": queue peaks";
        for (size_t p = 0; p < d->out.size(); p++) r += " " + std::to_string(d->peak(int(p)));
        report.push_back(r + " B");
        dec[size_t(fi)].reset();
    }

    // --- new side ---
    struct Cookie {
        Store* s;
        int fi, port;
        std::vector<char> buf;
    };
    static ssize_t cookie_write(void* c, const char* p, size_t n) {
        Cookie* k = static_cast<Cookie*>(c);
        if (k->s->abandoned) return ssize_t(n);  // dropped
        try {
            Encoder* e = k->s->encoder(k->fi);
            if (AsyncEnc* a = k->s->aenc[size_t(k->fi)].get()) a->write(k->port, p, n);
            else e->write(k->port, p, n);
        } catch (const std::exception& e) {
            if (k->s->sink_error.empty()) k->s->sink_error = e.what();
            errno = EIO;
            return -1;
        }
        return ssize_t(n);
    }
    static int cookie_close(void* c) {
        Cookie* k = static_cast<Cookie*>(c);
        Store* s = k->s;
        s->sinks.erase(k);
        int rc = 0;
        try {
            if (!s->abandoned) s->close_sink(k->fi, k->port);
        } catch (const std::exception& e) {
            if (s->sink_error.empty()) s->sink_error = e.what();
            rc = EOF;
        }
        delete k;
        return rc;
    }
    static const size_t SINK_BUF = size_t(32) << 10;
    FILE* open_sink(const std::string& name) {
        auto [fi, port] = file_port.at(name);
        Encoder* e = encoder(fi);
        if (!opened_new.insert(name).second) die("new %s opened twice", name.c_str());
        if (!aenc[size_t(fi)]) e->state[size_t(port)] = 1;
        Cookie* c = new Cookie{this, fi, port, std::vector<char>(SINK_BUF)};
        cookie_io_functions_t io = {nullptr, cookie_write, nullptr, cookie_close};
        FILE* f = fopencookie(c, "w", io);
        if (!f) {
            delete c;
            die("fopencookie %s", name.c_str());
        }
        sinks[c] = f;
        setvbuf(f, c->buf.data(), _IOFBF, c->buf.size());
        __fsetlocking(f, FSETLOCKING_BYCALLER);
        return f;
    }
    void close_sink(int fi, int port) {
        Encoder* e = enc[size_t(fi)].get();
        const auto& F = families()[size_t(fi)];
        std::string r = std::string("encode ") + F.name;
        if (AsyncEnc* a = aenc[size_t(fi)].get()) {
            if (!a->close(port)) return;  // (the last close joins the worker)
            e->finish();
            r += ": thread cpu " + std::to_string(a->cpu_s) + " s, ring peaks";
            for (size_t p = 0; p < a->ring.size(); p++) r += " " + std::to_string(a->peak(int(p)));
            r += " B, queue peaks";
            for (size_t p = 0; p < e->in.size(); p++) r += " " + std::to_string(e->peak(int(p)));
            report.push_back(r + " B");
            aenc[size_t(fi)].reset();
            enc[size_t(fi)].reset();
            return;
        }
        e->state[size_t(port)] = 2;
        e->pump();
        if (!e->all_closed()) return;
        e->finish();
        r += ": queue peaks";
        for (size_t p = 0; p < e->in.size(); p++) r += " " + std::to_string(e->peak(int(p)));
        report.push_back(r + " B");
        enc[size_t(fi)].reset();
    }
    // Every new family's outputs closed.
    void finish_outputs() {
        const auto& F = families();
        for (size_t fi = 0; fi < F.size(); fi++)
            if (fam_out[fi] && enc[fi]) die("family %s: an output was never closed", F[fi].name);
    }
};

inline Store& store() {
    static Store s;
    return s;
}

}  // namespace gcs
