// The patcher's side of the in-process codecs: every today's-format file of an encoded family is
// a virtual old file (a SequentialFileReader reads it from the family's decoder) and a virtual
// output (open_out hands back a FILE* whose bytes go to the family's encoder). A native family
// (the POI files, R2 and V2 in every dir: poi_files.h; the place files, place_files.h; each of the
// four side files, side_blocks.h) is encoded in every dir under today's names, the place files'
// logical stream as place_nodes.bin; gc's families only in an encoded installed set (a dir with a
// gc.manifest), whose new dir gets the merged gc.manifest. No today's-format file of an encoded
// family touches the disk.
#pragma once
#include <malloc.h>
#include <set>
#include <stdio.h>
#include <stdio_ext.h>
#include "gcs_async.h"
#include "gcs_codec.h"
#include "sequential_file_reader.h"

namespace gcs {

// The client list's sizes by today's name (Store::expect_out).
using ListedSizes = std::map<std::string, uint64_t>;

// A family and its codecs. decoder: over the installed set in dir (m: its manifest, which holds a
// gc family's counts). encoder: into the new set in dir (sizes: for one that needs a file's listed
// size). decoded_size: a native family's decoded size of a port, from dir's files alone and with
// no decoder made (the POI replay reads the files itself); none for a gc family, whose decoder
// gives the manifest's.
struct FamilyDesc {
    const char* name;
    std::vector<const char*> today;
    bool native;  // encoded in every dir, as its today's files' names
    Decoder* (*decoder)(const std::string& dir, const Manifest& m);
    Encoder* (*encoder)(const std::string& dir, const ListedSizes& sizes);
    uint64_t (*decoded_size)(const std::string& dir, int port);
};
// gc's families in gc's order (tools/gc.cpp; gc.families lists them in it), and the native ones.
static const std::vector<FamilyDesc>& families() {
    static const std::vector<FamilyDesc> f = {
        {"streets", {"street_ways.bin", "street_nodes.bin"}, false,
         [](const std::string& dir, const Manifest& m) -> Decoder* { return new ways::Dec(dir, true, m); },
         [](const std::string& dir, const ListedSizes&) -> Encoder* { return new ways::Enc(dir, true); }, nullptr},
        {"interps", {"interp_ways.bin", "interp_nodes.bin"}, false,
         [](const std::string& dir, const Manifest& m) -> Decoder* { return new ways::Dec(dir, false, m); },
         [](const std::string& dir, const ListedSizes&) -> Encoder* { return new ways::Enc(dir, false); }, nullptr},
        {"way_parents", {"way_parents.bin"}, true,
         [](const std::string& dir, const Manifest&) -> Decoder* { return new side::Dec(dir, "way_parents"); },
         [](const std::string& dir, const ListedSizes&) -> Encoder* { return new side::Enc(dir, "way_parents"); },
         [](const std::string& dir, int) { return side::decoded_size(dir, "way_parents"); }},
        {"way_postcodes", {"way_postcodes.bin"}, true,
         [](const std::string& dir, const Manifest&) -> Decoder* { return new side::Dec(dir, "way_postcodes"); },
         [](const std::string& dir, const ListedSizes&) -> Encoder* { return new side::Enc(dir, "way_postcodes"); },
         [](const std::string& dir, int) { return side::decoded_size(dir, "way_postcodes"); }},
        {"interp_postcodes", {"interp_postcodes.bin"}, true,
         [](const std::string& dir, const Manifest&) -> Decoder* { return new side::Dec(dir, "interp_postcodes"); },
         [](const std::string& dir, const ListedSizes&) -> Encoder* { return new side::Enc(dir, "interp_postcodes"); },
         [](const std::string& dir, int) { return side::decoded_size(dir, "interp_postcodes"); }},
        {"addr_postcodes", {"addr_postcodes.bin"}, true,
         [](const std::string& dir, const Manifest&) -> Decoder* { return new side::Dec(dir, "addr_postcodes"); },
         [](const std::string& dir, const ListedSizes&) -> Encoder* { return new side::Enc(dir, "addr_postcodes"); },
         [](const std::string& dir, int) { return side::decoded_size(dir, "addr_postcodes"); }},
        {"addr", {"addr_points.bin", "addr_vertices.bin"}, false,
         [](const std::string& dir, const Manifest& m) -> Decoder* { return new addr::Dec(dir, m); },
         [](const std::string& dir, const ListedSizes& sizes) -> Encoder* {
             auto it = sizes.find("addr_points.bin");
             if (it == sizes.end()) die("addr: the patch lists no addr_points.bin size");
             return new addr::Enc(dir, it->second);
         },
         nullptr},
        {"poi", {"poi_records.bin", "poi_vertices.bin"}, true,
         [](const std::string& dir, const Manifest&) -> Decoder* { return new poi::Dec(dir); },
         [](const std::string& dir, const ListedSizes&) -> Encoder* { return new poi::Enc(dir); }, poi::decoded_size},
        {"places", {"place_nodes.bin"}, true,
         [](const std::string& dir, const Manifest&) -> Decoder* { return new place::Dec(dir); },
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
    bool active = false, threads = false;
    std::string cur_dir, out_dir;
    Manifest old_m, new_m;
    std::vector<int> fam_on;   // per family: encoded in the installed set
    std::vector<int> fam_out;  // per family: encoded in the new set (plan_outputs)
    std::map<std::string, std::pair<int, int>> file_port;  // today's name -> (family, port)
    std::vector<std::unique_ptr<Decoder>> dec;
    std::vector<std::unique_ptr<Encoder>> enc;
    std::vector<std::unique_ptr<AsyncDec>> adec;  // --threads: the codec's worker
    std::vector<std::unique_ptr<AsyncEnc>> aenc;
    std::map<std::string, uint64_t> expect_out, got_out;  // today's name -> bytes (client list / encoder)
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
        fam_on.assign(F.size(), 0);
        dec.resize(F.size());
        enc.resize(F.size());
        adec.resize(F.size());
        aenc.resize(F.size());
        for (size_t fi = 0; fi < F.size(); fi++)
            if (F[fi].native) turn_on(fi);
        struct stat st;
        if (stat((cur + "/gc.manifest").c_str(), &st) != 0) return;
        active = true;
        old_m.load(cur + "/gc.manifest");
        std::set<std::string> on;
        {
            const std::string fl = old_m.get("gc.families");
            size_t a = 0;
            while (a <= fl.size()) {
                size_t b = fl.find(',', a);
                if (b == std::string::npos) b = fl.size();
                if (b > a) on.insert(fl.substr(a, b - a));
                a = b + 1;
            }
        }
        for (size_t fi = 0; fi < F.size(); fi++)
            if (on.count(F[fi].name) && !F[fi].native) turn_on(fi);
        for (const auto& n : on) {
            bool known = false;
            for (const auto& f : F) known |= n == f.name && !f.native;
            if (!known) die("unknown family %s in gc.manifest", n.c_str());
        }
    }
    void turn_on(size_t fi) {
        fam_on[fi] = 1;
        const auto& F = families()[fi];
        for (size_t p = 0; p < F.today.size(); p++) file_port[F.today[p]] = {int(fi), int(p)};
    }
    // Once expect_out holds the patch's client list: an encoded family the new set drops whole (it
    // lists none of its today's files) leaves gc.families, and one it ships in part can't be encoded.
    void plan_outputs() {
        const auto& F = families();
        fam_out.assign(F.size(), 0);
        for (size_t fi = 0; fi < F.size(); fi++) {
            if (!fam_on[fi]) continue;
            size_t listed = 0;
            for (const char* t : F[fi].today) listed += expect_out.count(t);
            if (listed && listed < F[fi].today.size())
                die("family %s: the patch ships %zu of its %zu files", F[fi].name, listed, F[fi].today.size());
            fam_out[fi] = listed != 0;
        }
    }
    bool is_encoded(const std::string& name) const { return file_port.count(name) != 0; }
    // An encoded file whose new size is what its encoder took (gc's families: the client list
    // gives a today's-format size); a native family's is its own file's.
    bool sized_by_encoder(const std::string& name) const {
        auto it = file_port.find(name);
        return it != file_port.end() && !families()[size_t(it->second.first)].native;
    }
    std::string path_of(const std::string& name) const { return cur_dir + "/" + name; }
    // Decoders and encoders are made on first use and freed when their last port closes.
    Decoder* decoder(int fi) {
        if (!dec[size_t(fi)]) {
            Decoder* d = families()[size_t(fi)].decoder(cur_dir, old_m);
            dec[size_t(fi)].reset(d);
            if (threads) adec[size_t(fi)].reset(new AsyncDec(d));
        }
        return dec[size_t(fi)].get();
    }
    Encoder* encoder(int fi) {
        if (!enc[size_t(fi)]) {
            const auto& F = families()[size_t(fi)];
            Encoder* e = F.encoder(out_dir, expect_out);
            for (size_t p = 0; p < F.today.size() && !F.native; p++) {
                auto it = expect_out.find(F.today[p]);
                if (it != expect_out.end()) e->expect[p] = it->second;
            }
            enc[size_t(fi)].reset(e);
            if (threads) aenc[size_t(fi)].reset(new AsyncEnc(e));
        }
        return enc[size_t(fi)].get();
    }

    // --- old side ---
    // The decoded size check for require_old_file_size: the manifest's size when it gives one.
    // A native family's from its files (FamilyDesc::decoded_size).
    void check_old_size(const std::string& name, uint64_t expected) {
        auto [fi, port] = file_port.at(name);
        const FamilyDesc& F = families()[size_t(fi)];
        const uint64_t ds = F.decoded_size ? F.decoded_size(cur_dir, port) : decoder(fi)->decoded_size(port);
        if (ds != ~0ull && ds != expected)
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
            got_out[F.today[size_t(port)]] = a->got[size_t(port)];
            if (!a->close(port)) return;  // (the last close joins the worker)
            e->finish(new_m);
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
        got_out[F.today[size_t(port)]] = e->got[size_t(port)];
        if (!e->all_closed()) return;
        e->finish(new_m);
        r += ": queue peaks";
        for (size_t p = 0; p < e->in.size(); p++) r += " " + std::to_string(e->peak(int(p)));
        report.push_back(r + " B");
        enc[size_t(fi)].reset();
    }
    // Every new family's outputs closed; for an encoded set, the merged manifest: every new gc
    // family's keys, gc.families and gc.version, as gc writes it.
    void finish_outputs() {
        std::string fl;
        const auto& F = families();
        for (size_t fi = 0; fi < F.size(); fi++) {
            if (!fam_out[fi]) continue;
            if (enc[fi]) die("family %s: an output was never closed", F[fi].name);
            if (!F[fi].native) fl += (fl.empty() ? "" : ",") + std::string(F[fi].name);
        }
        if (!active) return;
        new_m.set("gc.version", 1);
        new_m.set("gc.families", fl);
        new_m.save(out_dir + "/gc.manifest");
    }
};

inline Store& store() {
    static Store s;
    return s;
}

}  // namespace gcs
