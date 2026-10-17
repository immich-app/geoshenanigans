// Optional worker threads for the in-process codecs (geocoder-patch --threads): a family's decoder
// runs ahead in its own thread into bounded per-file rings, and its encoder drains per-file rings
// in its own thread, so a section's decode, replay and encode overlap on three cores.
// Deadlock-free by construction:
//   encoder: the worker takes whatever any ring holds and hands it to the encoder, which queues what
//            it can't process yet; only the patcher thread ever waits for room.
//   decoder: the worker pauses when a ring is full, unless the patcher thread is waiting on another
//            file's ring; then it keeps decoding (the full ring grows) until that one has bytes.
#pragma once
#include <condition_variable>
#include <mutex>
#include <thread>
#include <time.h>
#include "gcs_codec.h"

namespace gcs {

inline double thread_cpu_s() {
    timespec t;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &t);
    return double(t.tv_sec) + 1e-9 * double(t.tv_nsec);
}

static const size_t RING_CAP = size_t(128) << 10, DEC_BATCH = size_t(32) << 10;

struct AsyncDec {
    Decoder* inner;
    std::vector<ByteQueue> ring;
    std::vector<int> closed;
    std::mutex mu;
    std::condition_variable cv_reader, cv_worker;
    std::thread worker;
    bool started = false, eof = false, stop = false;
    int starving = 0;  // readers waiting on an empty ring
    std::string error;
    double cpu_s = 0;  // the worker's CPU time

    explicit AsyncDec(Decoder* d) : inner(d), ring(d->out.size()), closed(d->out.size(), 0) {
        for (auto& s : inner->state) s = 1;  // the worker keeps every file's bytes; closed ones are dropped here
    }
    ~AsyncDec() { shutdown(); }
    bool full_locked() const {
        for (size_t p = 0; p < ring.size(); p++)
            if (!closed[p] && ring[p].size() >= RING_CAP) return true;
        return false;
    }
    void run() {
        struct Cpu { double& out; ~Cpu() { out = thread_cpu_s(); } } cpu{cpu_s};
        try {
            for (;;) {
                {
                    std::unique_lock<std::mutex> lk(mu);
                    cv_worker.wait(lk, [&] { return stop || starving > 0 || !full_locked(); });
                    if (stop) return;
                }
                size_t produced = 0;
                bool more = true;
                while (produced < DEC_BATCH && (more = inner->step())) {
                    produced = 0;
                    for (auto& q : inner->out) produced += q.size();
                }
                {
                    std::lock_guard<std::mutex> lk(mu);
                    for (size_t p = 0; p < ring.size(); p++) {
                        ByteQueue& q = inner->out[p];
                        if (!closed[p] && q.size()) ring[p].push(q.data(), q.size());
                        q.pop(q.size());
                    }
                    if (!more) eof = true;
                }
                cv_reader.notify_all();
                if (!more) return;
            }
        } catch (const std::exception& e) {
            std::lock_guard<std::mutex> lk(mu);
            error = e.what();
            eof = true;
            cv_reader.notify_all();
        }
    }
    size_t read(int port, char* dst, size_t max) {
        std::unique_lock<std::mutex> lk(mu);
        if (!started) {
            started = true;
            worker = std::thread([this] { run(); });
        }
        ByteQueue& q = ring[size_t(port)];
        while (q.size() == 0 && !eof) {
            starving++;
            cv_worker.notify_one();
            cv_reader.wait(lk);
            starving--;
        }
        if (!error.empty()) die("%s (decoder thread)", error.c_str());
        const size_t k = std::min(max, q.size());
        if (k) memcpy(dst, q.data(), k);
        q.pop(k);
        if (q.size() < RING_CAP) cv_worker.notify_one();
        return k;
    }
    // Returns true when every file is closed (the decoder can go).
    bool close(int port) {
        std::lock_guard<std::mutex> lk(mu);
        closed[size_t(port)] = 1;
        ring[size_t(port)].reset();
        cv_worker.notify_one();
        for (int c : closed)
            if (!c) return false;
        return true;
    }
    void shutdown() {
        {
            std::lock_guard<std::mutex> lk(mu);
            stop = true;
        }
        cv_worker.notify_all();
        if (worker.joinable()) worker.join();
    }
    size_t peak(int port) const { return ring[size_t(port)].peak; }
};

struct AsyncEnc {
    Encoder* inner;
    std::vector<ByteQueue> ring;
    std::vector<int> closed, closed_seen;
    std::vector<uint64_t> got;
    std::mutex mu;
    std::condition_variable cv_writer, cv_worker;
    std::thread worker;
    bool started = false, done = false;
    std::string error;
    double cpu_s = 0;

    explicit AsyncEnc(Encoder* e) : inner(e), ring(e->in.size()), closed(e->in.size(), 0), closed_seen(e->in.size(), 0), got(e->in.size(), 0) {}
    ~AsyncEnc() {
        if (worker.joinable()) worker.join();
    }
    void run() {
        struct Cpu { double& out; ~Cpu() { out = thread_cpu_s(); } } cpu{cpu_s};
        std::vector<ByteQueue> take(ring.size());
        try {
            for (;;) {
                std::vector<int> newly(ring.size(), 0);
                bool all = true;
                {
                    std::unique_lock<std::mutex> lk(mu);
                    cv_worker.wait(lk, [&] {
                        for (size_t p = 0; p < ring.size(); p++)
                            if (ring[p].size() || (closed[p] && !closed_seen[p])) return true;
                        return false;
                    });
                    for (size_t p = 0; p < ring.size(); p++) {
                        std::swap(take[p], ring[p]);  // the ring's bytes, without a copy
                        if (closed[p] && !closed_seen[p]) { closed_seen[p] = 1; newly[p] = 1; }
                        all &= closed_seen[p] != 0;
                    }
                }
                cv_writer.notify_all();
                for (size_t p = 0; p < take.size(); p++) {
                    if (take[p].size()) inner->write(int(p), take[p].data(), take[p].size());
                    take[p].pop(take[p].size());
                }
                for (size_t p = 0; p < newly.size(); p++)
                    if (newly[p]) inner->state[p] = 2;
                inner->pump();
                if (all) break;
            }
        } catch (const std::exception& e) {
            std::lock_guard<std::mutex> lk(mu);
            error = e.what();
        }
        std::lock_guard<std::mutex> lk(mu);
        done = true;
        cv_writer.notify_all();
    }
    void write(int port, const void* p, size_t n) {
        // A write larger than a ring goes in ring-sized pieces (the ring never holds more).
        while (n > RING_CAP) {
            write_piece(port, p, RING_CAP);
            p = static_cast<const uint8_t*>(p) + RING_CAP;
            n -= RING_CAP;
        }
        write_piece(port, p, n);
    }
    void write_piece(int port, const void* p, size_t n) {
        std::unique_lock<std::mutex> lk(mu);
        if (!started) {
            started = true;
            for (auto& s : inner->state) s = 1;
            worker = std::thread([this] { run(); });
        }
        ByteQueue& q = ring[size_t(port)];
        cv_writer.wait(lk, [&] { return done || q.size() == 0 || q.size() + n <= RING_CAP; });
        if (!error.empty() || done) die("%s (encoder thread)", error.empty() ? "encoder stopped" : error.c_str());
        q.push(p, n);
        got[size_t(port)] += n;
        cv_worker.notify_one();
    }
    // Returns true when every file is closed: the worker has then drained everything and stopped.
    bool close(int port) {
        bool all = true;
        {
            std::lock_guard<std::mutex> lk(mu);
            if (!started) {
                started = true;
                for (auto& s : inner->state) s = 1;
                worker = std::thread([this] { run(); });
            }
            closed[size_t(port)] = 1;
            for (int c : closed) all &= c != 0;
        }
        cv_worker.notify_one();
        if (!all) return false;
        worker.join();
        if (!error.empty()) die("%s (encoder thread)", error.c_str());
        return true;
    }
    size_t peak(int port) const { return ring[size_t(port)].peak; }
};

}  // namespace gcs
