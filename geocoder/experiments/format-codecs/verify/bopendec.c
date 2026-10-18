// Independent decoder for the index track's POI "B-open" on-disk vertex layout (from its description):
//   polygons back to back in today's order; each block: 10-byte header verbatim except the pad
//   byte (byte 1) = 1 when the ring's closing vertex was omitted; then the wrapping zigzag deltas
//   (first vertex absolute) of the stored vertices (count - flag) at the polygon's width w (2 B for
//   enc 0-2, 4 B for enc 3) as byte planes: lat b0 | lat b1 [..] | lng b0 | lng b1 [..].
//   The decoder re-appends the first vertex as the closing vertex.
// Inputs: today's poi_records.bin (vertex_offset @8, vertex_count @12, 36-byte records; used for the
// polygon order and full count) and the B-open file. Writes today's poi_vertices layout for cmp.
// usage: bopendec <poi_records.bin> <bopen.bin> <out>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static const uint8_t* map(const char* p, size_t* n) {
    int fd = open(p, O_RDONLY); if (fd < 0) { perror(p); exit(2); }
    struct stat st; fstat(fd, &st); *n = st.st_size;
    const uint8_t* m = mmap(0, *n ? *n : 1, PROT_READ, MAP_SHARED, fd, 0); close(fd); return m;
}
static inline uint32_t unzz(uint32_t z) { return (z >> 1) ^ (uint32_t)(-(int32_t)(z & 1)); }

int main(int argc, char** argv) {
    if (argc != 4) { fprintf(stderr, "usage\n"); return 2; }
    size_t nr, nb;
    const uint8_t* r = map(argv[1], &nr);
    const uint8_t* b = map(argv[2], &nb);
    FILE* f = fopen(argv[3], "wb");
    size_t R = nr / 36;
    uint8_t* blk = malloc(10 + 8 * 1000000);
    uint64_t pos = 0, out_pos = 0, polys = 0, closed_n = 0, bad = 0;
    for (size_t i = 0; i < R; i++) {
        uint32_t vo, vc; memcpy(&vo, r + 36 * i + 8, 4); memcpy(&vc, r + 36 * i + 12, 4);
        if (vc == 0 || vo == 0xFFFFFFFFu) continue;
        if (vo != out_pos) bad++;  // today's polygons are back to back in record order
        const uint8_t* h = b + pos;
        uint8_t enc = h[0], flag = h[1];
        if (enc > 3 || flag > 1 || vc > 1000000) { bad++; break; }
        int w = enc == 3 ? 4 : 2, bits = 8 * w;
        uint64_t mask = bits == 32 ? 0xFFFFFFFFull : ((1ull << bits) - 1);
        uint64_t c2 = vc - flag;
        memcpy(blk, h, 10); blk[1] = 0;
        for (int comp = 0; comp < 2; comp++) {
            uint32_t prev = 0;
            for (uint64_t k = 0; k < c2; k++) {
                uint32_t z = 0;
                for (int bb = 0; bb < w; bb++) z |= (uint32_t)h[10 + (comp * w + bb) * c2 + k] << (8 * bb);
                uint32_t q = (uint32_t)(((uint64_t)prev + unzz(z)) & mask);
                for (int bb = 0; bb < w; bb++) blk[10 + k * 2 * w + comp * w + bb] = (uint8_t)(q >> (8 * bb));
                prev = q;
            }
        }
        if (flag) memcpy(blk + 10 + c2 * 2 * w, blk + 10, 2 * w);
        fwrite(blk, 1, 10 + (size_t)vc * 2 * w, f);
        pos += 10 + c2 * 2 * w;
        out_pos += 10 + (uint64_t)vc * 2 * w;
        polys++; closed_n += flag;
    }
    fclose(f);
    printf("polygons %llu closed_flagged %llu bopen_bytes_walked %llu file %zu out %llu bad %llu\n", (unsigned long long)polys,
           (unsigned long long)closed_n, (unsigned long long)pos, nb, (unsigned long long)out_pos, (unsigned long long)bad);
    return (bad || pos != nb) ? 1 : 0;
}
