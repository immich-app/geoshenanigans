/**
 * Worker thread of buildManifest (manifest.ts): decodes a job's leaf directories and hashes the tiles they introduce.
 *
 * In a clustered archive each job's new tiles sit back to back in tile data, in tile-ID order: a chain ending at the
 * job's highest byte. Two steps per job:
 * - decode: read the leaves and find the chain. Its end goes back to the main thread, which works out in job order
 *   where each job's new tiles start (the floor: everything below it came with earlier jobs);
 * - hash: read the chain above the floor in large sequential slabs and hash each tile once. Entries pointing back
 *   into that chain (repeats within the job) take its hash; the rest (the ocean tile, tiles deduplicated against
 *   earlier jobs) are left to the main thread, which resolves them from earlier jobs or its cache.
 */
import { hash } from 'node:crypto';
import { closeSync, openSync, readSync } from 'node:fs';
import { parentPort, workerData } from 'node:worker_threads';
import { gunzipSync } from 'node:zlib';
import { checkOpened, FileSnapshot } from './archive-identity-worker';

export interface WorkerInit {
  manifestWorker: true;
  path: string;
  /** Given: the file to read (see FileSnapshot), which each thread checks it opened. */
  source?: FileSnapshot;
  leafDirectoryOffset: number;
  tileDataOffset: number;
  internalCompression: number;
}

/**
 * Entries of one directory or job, in tile-ID order. Leaf pointers have runLength 0. Run lengths are varints up to
 * 2^53 like tile IDs, so they are doubles too: a 32-bit array would wrap a run of 2^32 positions to a leaf pointer.
 */
export interface EntryArrays {
  tileIds: Float64Array;
  runLengths: Float64Array;
  offsets: Float64Array;
  lengths: Uint32Array;
}

/** Either leaf directories to read ([offset, length] relative to the leaf section) or entries from the root. */
export type JobPlan = { leaves: [number, number][] } | { entries: EntryArrays };
export type JobSpec = JobPlan & { id: number };

export type WorkerRequest = { type: 'decode'; job: JobSpec } | { type: 'hash'; id: number; floor: number };
export type WorkerReply = { type: 'decoded'; id: number; chainEnd: number } | { type: 'hashed'; result: JobResult };

export interface JobResult {
  id: number;
  tileIds: Float64Array;
  runLengths: Float64Array;
  /** First 8 bytes of each entry's SHA-256 as two big-endian words; zero where `unresolved` lists the entry. */
  hashes: Uint32Array;
  /** The chain: entry indexes, offsets and lengths of the tiles this job hashed, in ascending offset order. */
  chainEntries: Uint32Array;
  chainOffsets: Float64Array;
  chainLengths: Uint32Array;
  /** Entries pointing outside the chain, for the main thread. */
  unresolved: Uint32Array;
  unresolvedOffsets: Float64Array;
  unresolvedLengths: Uint32Array;
  bytesHashed: number;
}

const SLAB_BYTES = 4 * 1024 * 1024;
const MAX_DIRECTORY_DEPTH = 4;
const COMPRESSION_NONE = 1;
const COMPRESSION_GZIP = 2;

/** A varint at buf[pos], up to 2^53; sets `varintEnd`. Directories are trusted: a truncated one throws. */
let varintEnd = 0;
function readVarint(buf: Uint8Array, pos: number): number {
  let value = 0;
  let scale = 1;
  for (let i = 0; i < 8; i++) {
    if (pos >= buf.length) {
      throw new Error('Truncated directory');
    }
    const byte = buf[pos++];
    value += (byte & 0x7f) * scale;
    if (byte < 0x80) {
      varintEnd = pos;
      return value;
    }
    scale *= 128;
  }
  throw new Error('Directory varint longer than 8 bytes');
}

/** PMTiles v3 directory: count, tile ID deltas, run lengths, lengths, offsets (0 = right after the previous). */
export function decodeDirectory(buf: Uint8Array): EntryArrays {
  let pos = 0;
  const count = readVarint(buf, pos);
  pos = varintEnd;
  const tileIds = new Float64Array(count);
  const runLengths = new Float64Array(count);
  const lengths = new Uint32Array(count);
  const offsets = new Float64Array(count);
  let tileId = 0;
  for (let i = 0; i < count; i++) {
    tileId += readVarint(buf, pos);
    pos = varintEnd;
    tileIds[i] = tileId;
  }
  for (let i = 0; i < count; i++) {
    runLengths[i] = readVarint(buf, pos);
    pos = varintEnd;
  }
  for (let i = 0; i < count; i++) {
    lengths[i] = readVarint(buf, pos);
    pos = varintEnd;
  }
  for (let i = 0; i < count; i++) {
    const value = readVarint(buf, pos);
    pos = varintEnd;
    offsets[i] = value === 0 && i > 0 ? offsets[i - 1] + lengths[i - 1] : value - 1;
  }
  return { tileIds, runLengths, offsets, lengths };
}

export function decompressDirectory(bytes: Uint8Array, compression: number): Uint8Array {
  if (compression === COMPRESSION_GZIP) {
    return gunzipSync(bytes);
  }
  if (compression === COMPRESSION_NONE) {
    return bytes;
  }
  throw new Error(`Unsupported directory compression ${compression}`);
}

export function readFully(fd: number, buffer: Uint8Array, length: number, position: number) {
  let done = 0;
  while (done < length) {
    const read = readSync(fd, buffer, done, length - done, position + done);
    if (read === 0) {
      throw new Error(`Unexpected end of archive reading ${length} bytes at ${position}`);
    }
    done += read;
  }
}

export class JobRunner {
  private slab = Buffer.allocUnsafe(SLAB_BYTES);

  private init: WorkerInit;
  private fd: number;

  constructor(init: WorkerInit) {
    this.init = init;
    this.fd = openSync(init.path, 'r');
    if (init.source !== undefined) {
      try {
        checkOpened(init.path, this.fd, init.source);
      } catch (error) {
        closeSync(this.fd);
        throw error;
      }
    }
  }

  private decoded = new Map<number, { entries: EntryArrays; isChain: Uint8Array; end: number }>();

  /** Reads the job's entries and finds its chain; returns where the chain ends. */
  decode(job: JobSpec): number {
    const entries = 'entries' in job ? concat(this.expand(job.entries)) : this.expandLeaves(job.leaves);
    const { offsets, lengths } = entries;
    const count = offsets.length;
    // Walk back from the highest byte: each chain tile ends where the next one starts. A repeat that happens to
    // match a chain tile exactly may take its place; both have the same bytes, so the hashes are the same.
    let end = 0;
    for (let i = 0; i < count; i++) {
      end = Math.max(end, offsets[i] + lengths[i]);
    }
    const isChain = new Uint8Array(count);
    let need = end;
    for (let i = count - 1; i >= 0; i--) {
      if (lengths[i] > 0 && offsets[i] + lengths[i] === need) {
        isChain[i] = 1;
        need = offsets[i];
      }
    }
    this.decoded.set(job.id, { entries, isChain, end });
    return end;
  }

  /** Hashes the decoded job's chain from `floor` up and resolves what it can. */
  hash(id: number, floor: number): { result: JobResult; transfer: ArrayBuffer[] } {
    const job = this.decoded.get(id);
    if (!job) {
      throw new Error(`Job ${id} was not decoded here`);
    }
    this.decoded.delete(id);
    const result = this.hashEntries(id, job.entries, job.isChain, job.end, floor);
    const transfer = [
      result.tileIds,
      result.runLengths,
      result.hashes,
      result.chainEntries,
      result.chainOffsets,
      result.chainLengths,
      result.unresolved,
      result.unresolvedOffsets,
      result.unresolvedLengths,
    ].map((array) => array.buffer as ArrayBuffer);
    return { result, transfer };
  }

  private expandLeaves(leaves: [number, number][]): EntryArrays {
    return concat(leaves.flatMap(([offset, length]) => this.expand(this.readLeaf(offset, length), 1)));
  }

  private readLeaf(offset: number, length: number): EntryArrays {
    const bytes = Buffer.allocUnsafe(length);
    readFully(this.fd, bytes, length, this.init.leafDirectoryOffset + offset);
    const directory = decodeDirectory(decompressDirectory(bytes, this.init.internalCompression));
    if (directory.tileIds.length === 0) {
      throw new Error(`Empty leaf directory at offset ${offset}`);
    }
    return directory;
  }

  /** The tile entries of a directory in order, with nested leaves read in place. */
  private expand(directory: EntryArrays, depth = 0): EntryArrays[] {
    if (depth > MAX_DIRECTORY_DEPTH) {
      throw new Error('Directory nesting too deep');
    }
    if (!directory.runLengths.includes(0)) {
      return [directory];
    }
    const parts: EntryArrays[] = [];
    let start = 0;
    const { tileIds, runLengths, offsets, lengths } = directory;
    for (let i = 0; i <= tileIds.length; i++) {
      if (i < tileIds.length && runLengths[i] !== 0) {
        continue;
      }
      if (i > start) {
        parts.push({
          tileIds: tileIds.subarray(start, i),
          runLengths: runLengths.subarray(start, i),
          offsets: offsets.subarray(start, i),
          lengths: lengths.subarray(start, i),
        });
      }
      if (i < tileIds.length) {
        parts.push(...this.expand(this.readLeaf(offsets[i], lengths[i]), depth + 1));
      }
      start = i + 1;
    }
    return parts;
  }

  private hashEntries(id: number, entries: EntryArrays, isChain: Uint8Array, end: number, floor: number): JobResult {
    const { offsets, lengths } = entries;
    const count = offsets.length;
    // Chain tiles below the floor came with earlier jobs: this job only points back at them.
    let chainCount = 0;
    for (let i = 0; i < count; i++) {
      if (isChain[i] && offsets[i] < floor) {
        isChain[i] = 0;
      }
      chainCount += isChain[i];
    }

    const hashes = new Uint32Array(count * 2);
    const chainEntries = new Uint32Array(chainCount);
    const chainOffsets = new Float64Array(chainCount);
    const chainLengths = new Uint32Array(chainCount);
    let slabStart = 0;
    let slabEnd = 0;
    let bytesHashed = 0;
    for (let i = 0, k = 0; i < count; i++) {
      if (!isChain[i]) {
        continue;
      }
      const offset = offsets[i];
      const length = lengths[i];
      let source: Buffer;
      let at: number;
      if (offset >= slabStart && offset + length <= slabEnd) {
        source = this.slab;
        at = offset - slabStart;
      } else if (length > SLAB_BYTES) {
        source = Buffer.allocUnsafe(length);
        at = 0;
        readFully(this.fd, source, length, this.init.tileDataOffset + offset);
      } else {
        const size = Math.min(SLAB_BYTES, end - offset);
        readFully(this.fd, this.slab, size, this.init.tileDataOffset + offset);
        slabStart = offset;
        slabEnd = offset + size;
        source = this.slab;
        at = 0;
      }
      const digest = hash('sha256', source.subarray(at, at + length), 'buffer');
      hashes[i * 2] = digest.readUInt32BE(0);
      hashes[i * 2 + 1] = digest.readUInt32BE(4);
      chainEntries[k] = i;
      chainOffsets[k] = offset;
      chainLengths[k] = length;
      k++;
      bytesHashed += length;
    }

    const unresolved: number[] = [];
    for (let i = 0; i < count; i++) {
      if (isChain[i]) {
        continue;
      }
      const k = findChainTile(chainOffsets, chainLengths, offsets[i], lengths[i]);
      if (k < 0) {
        unresolved.push(i);
        continue;
      }
      const from = chainEntries[k] * 2;
      hashes[i * 2] = hashes[from];
      hashes[i * 2 + 1] = hashes[from + 1];
    }

    const unresolvedEntries = Uint32Array.from(unresolved);
    return {
      id,
      tileIds: entries.tileIds,
      runLengths: entries.runLengths,
      hashes,
      chainEntries,
      chainOffsets,
      chainLengths,
      unresolved: unresolvedEntries,
      unresolvedOffsets: Float64Array.from(unresolved, (i) => offsets[i]),
      unresolvedLengths: Uint32Array.from(unresolved, (i) => lengths[i]),
      bytesHashed,
    };
  }
}

/** Index of the chain tile at exactly (offset, length), or -1. */
export function findChainTile(chainOffsets: Float64Array, chainLengths: Uint32Array, offset: number, length: number) {
  let lo = 0;
  let hi = chainOffsets.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    const value = chainOffsets[mid];
    if (value === offset) {
      return chainLengths[mid] === length ? mid : -1;
    }
    if (value < offset) {
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return -1;
}

function concat(parts: EntryArrays[]): EntryArrays {
  if (parts.length === 1) {
    return parts[0];
  }
  const count = parts.reduce((sum, part) => sum + part.tileIds.length, 0);
  const out: EntryArrays = {
    tileIds: new Float64Array(count),
    runLengths: new Float64Array(count),
    offsets: new Float64Array(count),
    lengths: new Uint32Array(count),
  };
  let at = 0;
  for (const part of parts) {
    out.tileIds.set(part.tileIds, at);
    out.runLengths.set(part.runLengths, at);
    out.offsets.set(part.offsets, at);
    out.lengths.set(part.lengths, at);
    at += part.tileIds.length;
  }
  return out;
}

// Only as a worker started by buildManifest; importing this module elsewhere (e.g. tests) runs nothing. Decodes go
// before hashes so the main thread can hand out floors (and so more hash work) while long hashes are queued.
const init = workerData as WorkerInit | undefined;
if (parentPort && init?.manifestWorker) {
  const port = parentPort;
  const runner = new JobRunner(init);
  const decodes: JobSpec[] = [];
  const hashes: { id: number; floor: number }[] = [];
  let scheduled = false;
  const work = () => {
    scheduled = false;
    const job = decodes.shift();
    if (job) {
      port.postMessage({ type: 'decoded', id: job.id, chainEnd: runner.decode(job) } satisfies WorkerReply);
    } else {
      const request = hashes.shift();
      if (request) {
        const { result, transfer } = runner.hash(request.id, request.floor);
        port.postMessage({ type: 'hashed', result } satisfies WorkerReply, transfer);
      }
    }
    if (decodes.length > 0 || hashes.length > 0) {
      schedule();
    }
  };
  // setImmediate lets messages that arrived meanwhile in before the next task.
  const schedule = () => {
    if (!scheduled) {
      scheduled = true;
      setImmediate(work);
    }
  };
  port.on('message', (request: WorkerRequest) => {
    if (request.type === 'decode') {
      decodes.push(request.job);
    } else {
      hashes.push(request);
    }
    schedule();
  });
}
