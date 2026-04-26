// ByteSource — read-only byte access that works for both fully-resident
// Buffers and chunk-on-demand file descriptors. Used to load >4 GiB
// files (planet `geo_cells.bin` 7.7 GiB, `addr_vertices.bin` 5 GiB,
// etc.) without exceeding JS's per-Buffer 4 GiB cap.
//
// `Buffer` already implements the read methods; ChunkedFile implements
// the same surface backed by fs.readSync + an LRU page cache.

import { readSync, statSync, openSync } from "node:fs";

export interface ByteSource {
  readonly length: number;
  readUInt8(off: number): number;
  readUInt16LE(off: number): number;
  readUInt32LE(off: number): number;
  readFloatLE(off: number): number;
  readBigUInt64LE(off: number): bigint;
  // Returns a Buffer copy spanning [start, end) — copies for ChunkedFile,
  // zero-copy slice for plain Buffer.
  subarray(start: number, end: number): Buffer;
}

const PAGE_SIZE = 64 * 1024;
const MAX_PAGES = 4096; // 256 MiB cache cap, matches the wasm side

export class ChunkedFile implements ByteSource {
  private fd: number;
  private cache = new Map<number, Buffer>();
  private lru: number[] = [];
  readonly length: number;

  constructor(path: string) {
    this.fd = openSync(path, "r");
    this.length = statSync(path).size;
  }

  private fetchPage(pageOff: number): Buffer {
    const want = Math.min(PAGE_SIZE, this.length - pageOff);
    const buf = Buffer.allocUnsafe(want);
    readSync(this.fd, buf, 0, want, pageOff);
    return buf;
  }

  private ensurePage(pageOff: number): Buffer {
    const cached = this.cache.get(pageOff);
    if (cached) {
      const i = this.lru.indexOf(pageOff);
      if (i >= 0) this.lru.splice(i, 1);
      this.lru.push(pageOff);
      return cached;
    }
    if (this.cache.size >= MAX_PAGES) {
      const old = this.lru.shift();
      if (old !== undefined) this.cache.delete(old);
    }
    const page = this.fetchPage(pageOff);
    this.cache.set(pageOff, page);
    this.lru.push(pageOff);
    return page;
  }

  // Read [off, off+len) into a fresh Buffer, spanning pages as needed.
  // Pages may be evicted before the caller is done, so always copy.
  private readBytes(off: number, len: number): Buffer {
    const out = Buffer.allocUnsafe(len);
    let cur = off;
    let written = 0;
    while (cur < off + len) {
      const pageOff = cur - (cur % PAGE_SIZE);
      const page = this.ensurePage(pageOff);
      const inPage = cur - pageOff;
      const take = Math.min(off + len - cur, page.length - inPage);
      page.copy(out, written, inPage, inPage + take);
      cur += take;
      written += take;
      if (take === 0) break;
    }
    return out;
  }

  readUInt8(off: number): number {
    const pageOff = off - (off % PAGE_SIZE);
    const page = this.ensurePage(pageOff);
    return page.readUInt8(off - pageOff);
  }

  readUInt16LE(off: number): number {
    const pageOff = off - (off % PAGE_SIZE);
    const page = this.ensurePage(pageOff);
    const inPage = off - pageOff;
    if (inPage + 2 <= page.length) return page.readUInt16LE(inPage);
    return this.readBytes(off, 2).readUInt16LE(0);
  }

  readUInt32LE(off: number): number {
    const pageOff = off - (off % PAGE_SIZE);
    const page = this.ensurePage(pageOff);
    const inPage = off - pageOff;
    if (inPage + 4 <= page.length) return page.readUInt32LE(inPage);
    // Spans page boundary — fall back to readBytes
    return this.readBytes(off, 4).readUInt32LE(0);
  }

  readFloatLE(off: number): number {
    const pageOff = off - (off % PAGE_SIZE);
    const page = this.ensurePage(pageOff);
    const inPage = off - pageOff;
    if (inPage + 4 <= page.length) return page.readFloatLE(inPage);
    return this.readBytes(off, 4).readFloatLE(0);
  }

  readBigUInt64LE(off: number): bigint {
    const pageOff = off - (off % PAGE_SIZE);
    const page = this.ensurePage(pageOff);
    const inPage = off - pageOff;
    if (inPage + 8 <= page.length) return page.readBigUInt64LE(inPage);
    return this.readBytes(off, 8).readBigUInt64LE(0);
  }

  subarray(start: number, end: number): Buffer {
    return this.readBytes(start, end - start);
  }
}
