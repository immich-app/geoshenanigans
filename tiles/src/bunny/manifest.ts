/**
 * A build's manifest: the content hash of every tile position, kept between daily syncs so the next build can be
 * compared with the live one position by position (diff.ts) without reading the live archive.
 *
 * File layout (`<build>.tman.zst`):
 * - a zstd skippable frame holding a fixed-size header slot: "TMAN", format version, hash size, then JSON
 *   ({@link ManifestHeader}). It is written last, so a file whose header slot is still zero was never finished;
 * - zstd frames holding the entries in tile-ID order, one per maximal run of positions with the same content:
 *   varint gap (tile ID minus the previous entry's end, 0 when contiguous), varint run length, then the first 8
 *   bytes of the SHA-256 of the tile bytes as stored in the archive (gzip MVT). Adjacent archive entries with the same
 *   bytes are merged, so the manifest depends only on what each position holds, not on how the archive was laid out;
 * - an end marker (gap 0, run length 0), so a truncated file never reads as a shorter build.
 * Tile IDs end with zoom 26 (MAX_SAFE_ZOOM, the last whose IDs are exact doubles): the reader refuses positions past
 * it, whatever reads them.
 */
import { hash, randomBytes } from 'node:crypto';
import { once } from 'node:events';
import { closeSync, createReadStream, createWriteStream, openSync, readSync } from 'node:fs';
import { open, rename, rm, stat } from 'node:fs/promises';
import { availableParallelism } from 'node:os';
import { pipeline } from 'node:stream/promises';
import { URL } from 'node:url';
import { Worker } from 'node:worker_threads';
import { constants, createZstdCompress, createZstdDecompress } from 'node:zlib';
import { Archive } from '../deploy/archive';
import { parseDeploymentKey } from '../deploy/config';
import { Metadata } from '../pmtiles/types';
import { checkOpened, FileSnapshot, fileSnapshot } from './archive-identity-worker';
import {
  decodeDirectory,
  decompressDirectory,
  EntryArrays,
  findChainTile,
  JobPlan,
  JobResult,
  JobSpec,
  readFully,
  WorkerInit,
  WorkerReply,
  WorkerRequest,
} from './manifest-worker';
import { MAX_SAFE_ZOOM, tileIdZoom, zoomEnd } from './tile-id';

const SKIPPABLE_FRAME_MAGIC = 0x184d2a5b;
const HEADER_SLOT_BYTES = 1024;
/** Where the zstd entry frames start: the skippable frame's magic and size, then the header slot. */
export const MANIFEST_BODY_OFFSET = 8 + HEADER_SLOT_BYTES;
const MAGIC = 'TMAN';
export const MANIFEST_FORMAT_VERSION = 1;
const HASH_BYTES = 8;
// Two 8-byte varints (positions stay below 2^53) and the hash.
const MAX_RECORD_BYTES = 8 + 8 + HASH_BYTES;
/** One past the highest tile ID a manifest can describe: the end of zoom MAX_SAFE_ZOOM. */
const TILE_ID_END = zoomEnd(MAX_SAFE_ZOOM);
const WRITE_CHUNK_BYTES = 1 << 20;

export interface ManifestHeader {
  version: number;
  buildKey: string;
  /** Records in the file (maximal same-content runs). */
  entries: number;
  /** Tile positions the entries cover. */
  positions: number;
  minZoom: number | null;
  maxZoom: number | null;
  /** The OSM replication time the build was made from, when its metadata says. */
  osmDate: string | null;
  createdAt: string;
}

export interface ManifestMeta {
  buildKey: string;
  minZoom?: number | null;
  maxZoom?: number | null;
  osmDate?: string | null;
}

/** One run of positions; `hash` is the first 8 bytes of the tiles' SHA-256 as uppercase hex, like Bunny's Checksum. */
export interface ManifestEntry {
  tileId: number;
  runLength: number;
  hash: string;
}

/** Decoded records, as parallel arrays; hashHi/hashLo are the hash's two big-endian 32-bit words. */
export interface ManifestBatch {
  count: number;
  tileIds: Float64Array;
  runLengths: Float64Array;
  hashHi: Uint32Array;
  hashLo: Uint32Array;
}

export const hashHex = (hi: number, lo: number) =>
  (hi.toString(16).padStart(8, '0') + lo.toString(16).padStart(8, '0')).toUpperCase();

/** The manifest hash of tile bytes: the first 8 bytes of their SHA-256, as two big-endian words. */
export function tileHash(bytes: Uint8Array): [hi: number, lo: number] {
  const digest = hash('sha256', bytes, 'buffer');
  return [digest.readUInt32BE(0), digest.readUInt32BE(4)];
}

/** The OSM replication time planetiler records in the archive metadata, if any. */
export function osmDateOf(metadata: Metadata): string | null {
  const value = (metadata as unknown as Record<string, unknown>)['planetiler:osm:osmosisreplicationtime'];
  return typeof value === 'string' && !Number.isNaN(Date.parse(value)) ? value : null;
}

function encodeHeaderFrame(header: ManifestHeader): Buffer {
  const { version, ...fields } = header;
  const json = Buffer.from(JSON.stringify(fields));
  if (json.length > HEADER_SLOT_BYTES - 8) {
    throw new Error(`Manifest header too large (${json.length} bytes)`);
  }
  const frame = Buffer.alloc(MANIFEST_BODY_OFFSET);
  frame.writeUInt32LE(SKIPPABLE_FRAME_MAGIC, 0);
  frame.writeUInt32LE(HEADER_SLOT_BYTES, 4);
  frame.write(MAGIC, 8, 'latin1');
  frame[12] = version;
  frame[13] = HASH_BYTES;
  frame.writeUInt16LE(json.length, 14);
  json.copy(frame, 16);
  return frame;
}

export function decodeHeaderFrame(bytes: Uint8Array): ManifestHeader {
  const frame = Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (
    frame.length < MANIFEST_BODY_OFFSET ||
    frame.readUInt32LE(0) !== SKIPPABLE_FRAME_MAGIC ||
    frame.readUInt32LE(4) !== HEADER_SLOT_BYTES ||
    frame.toString('latin1', 8, 12) !== MAGIC
  ) {
    throw new Error('Not a tile manifest (or one that was never finished)');
  }
  if (frame[12] !== MANIFEST_FORMAT_VERSION || frame[13] !== HASH_BYTES) {
    throw new Error(`Unsupported manifest format ${frame[12]} with ${frame[13]}-byte hashes`);
  }
  const length = frame.readUInt16LE(14);
  const fields = JSON.parse(frame.toString('utf8', 16, 16 + length)) as Omit<ManifestHeader, 'version'>;
  return { version: frame[12], ...fields };
}

export async function readManifestHeader(path: string): Promise<ManifestHeader> {
  const handle = await open(path, 'r');
  try {
    const bytes = Buffer.alloc(MANIFEST_BODY_OFFSET);
    const { bytesRead } = await handle.read(bytes, 0, MANIFEST_BODY_OFFSET, 0);
    return decodeHeaderFrame(bytes.subarray(0, bytesRead));
  } finally {
    await handle.close();
  }
}

function writeVarint(buf: Buffer, pos: number, value: number): number {
  while (value >= 0x80) {
    buf[pos++] = (value % 0x80) | 0x80;
    value = Math.floor(value / 0x80);
  }
  buf[pos++] = value;
  return pos;
}

/**
 * Streams entries into a manifest file. Entries must come in tile-ID order without overlaps; adjacent ones with the
 * same hash are merged. Memory stays at a few MB: call drain() now and then to let the compressor catch up.
 */
export class ManifestWriter {
  private buffer = Buffer.allocUnsafe(WRITE_CHUNK_BYTES);
  private position = 0;
  private queued: Buffer[] = [];
  private failure: unknown;
  private hasPending = false;
  private pendingStart = 0;
  private pendingEnd = 0;
  private pendingHi = 0;
  private pendingLo = 0;
  private writtenEnd = 0;
  private entries = 0;
  private positions = 0;

  private constructor(
    private path: string,
    private partial: string,
    private meta: ManifestMeta,
    private zstd: ReturnType<typeof createZstdCompress>,
    private done: Promise<void>,
  ) {
    done.catch((error: unknown) => (this.failure ??= error));
  }

  static async create(path: string, meta: ManifestMeta, level = 3): Promise<ManifestWriter> {
    parseDeploymentKey('buildKey', meta.buildKey);
    // Its own partial file beside the destination, created exclusively: two writers of the same manifest (two
    // runs for the same date) never share one, so neither truncates, overwrites or removes the other's.
    const partial = `${path}.${process.pid}-${randomBytes(6).toString('hex')}.partial`;
    // A zeroed header slot: the file only becomes a manifest when finish() writes the header.
    const handle = await open(partial, 'wx');
    try {
      await handle.write(Buffer.alloc(MANIFEST_BODY_OFFSET), 0, MANIFEST_BODY_OFFSET, 0);
    } catch (error) {
      await handle.close();
      await rm(partial, { force: true });
      throw error;
    }
    await handle.close();
    const zstd = createZstdCompress({
      params: { [constants.ZSTD_c_compressionLevel]: level, [constants.ZSTD_c_checksumFlag]: 1 },
    });
    const file = createWriteStream(partial, { flags: 'r+', start: MANIFEST_BODY_OFFSET });
    return new ManifestWriter(path, partial, meta, zstd, pipeline(zstd, file));
  }

  add(tileId: number, runLength: number, hashHi: number, hashLo: number): void {
    if (!Number.isSafeInteger(tileId) || tileId < 0 || !Number.isSafeInteger(runLength) || runLength < 1) {
      throw new Error(`Invalid manifest entry ${tileId}+${runLength}`);
    }
    if (this.hasPending) {
      if (tileId < this.pendingEnd) {
        throw new Error(`Entry at ${tileId} overlaps or precedes the previous one (ends at ${this.pendingEnd})`);
      }
      if (tileId === this.pendingEnd && hashHi === this.pendingHi && hashLo === this.pendingLo) {
        this.pendingEnd += runLength;
        return;
      }
      this.writePending();
    }
    this.hasPending = true;
    this.pendingStart = tileId;
    this.pendingEnd = tileId + runLength;
    this.pendingHi = hashHi;
    this.pendingLo = hashLo;
  }

  private writePending() {
    this.reserve(MAX_RECORD_BYTES);
    const buf = this.buffer;
    let pos = writeVarint(buf, this.position, this.pendingStart - this.writtenEnd);
    pos = writeVarint(buf, pos, this.pendingEnd - this.pendingStart);
    buf.writeUInt32BE(this.pendingHi, pos);
    buf.writeUInt32BE(this.pendingLo, pos + 4);
    this.position = pos + HASH_BYTES;
    this.entries++;
    this.positions += this.pendingEnd - this.pendingStart;
    this.writtenEnd = this.pendingEnd;
  }

  private reserve(bytes: number) {
    if (this.position + bytes > this.buffer.length) {
      this.queued.push(this.buffer.subarray(0, this.position));
      this.buffer = Buffer.allocUnsafe(WRITE_CHUNK_BYTES);
      this.position = 0;
    }
  }

  /** Hands what is buffered to the compressor, waiting while it is behind. */
  async drain(): Promise<void> {
    const chunks = this.queued;
    this.queued = [];
    for (const chunk of chunks) {
      if (this.failure) {
        throw this.failure;
      }
      if (!this.zstd.write(chunk)) {
        await Promise.race([once(this.zstd, 'drain'), this.done]);
      }
    }
    if (this.failure) {
      throw this.failure;
    }
  }

  async finish(): Promise<ManifestHeader> {
    if (this.hasPending) {
      this.writePending();
      this.hasPending = false;
    }
    this.reserve(2);
    this.buffer[this.position++] = 0;
    this.buffer[this.position++] = 0;
    this.queued.push(this.buffer.subarray(0, this.position));
    await this.drain();
    this.zstd.end();
    await this.done;
    const header: ManifestHeader = {
      version: MANIFEST_FORMAT_VERSION,
      buildKey: this.meta.buildKey,
      entries: this.entries,
      positions: this.positions,
      minZoom: this.meta.minZoom ?? null,
      maxZoom: this.meta.maxZoom ?? null,
      osmDate: this.meta.osmDate ?? null,
      createdAt: new Date().toISOString(),
    };
    const handle = await open(this.partial, 'r+');
    try {
      await handle.write(encodeHeaderFrame(header), 0, MANIFEST_BODY_OFFSET, 0);
      await handle.sync();
    } finally {
      await handle.close();
    }
    await rename(this.partial, this.path);
    return header;
  }

  async abort(): Promise<void> {
    this.zstd.destroy();
    await this.done.catch(() => {});
    await rm(this.partial, { force: true });
  }
}

/** Writes a manifest from entries in tile-ID order (hash as 16 hex digits). Mainly for tests and tools. */
export async function writeManifest(
  path: string,
  meta: ManifestMeta,
  entries: Iterable<ManifestEntry> | AsyncIterable<ManifestEntry>,
): Promise<ManifestHeader> {
  const writer = await ManifestWriter.create(path, meta);
  try {
    let count = 0;
    for await (const entry of entries) {
      if (!/^[0-9a-fA-F]{16}$/.test(entry.hash)) {
        throw new Error(`Hash must be 16 hex digits, got ${entry.hash}`);
      }
      writer.add(
        entry.tileId,
        entry.runLength,
        parseInt(entry.hash.slice(0, 8), 16),
        parseInt(entry.hash.slice(8), 16),
      );
      if (++count % 65_536 === 0) {
        await writer.drain();
      }
    }
    return await writer.finish();
  } catch (error) {
    await writer.abort();
    throw error;
  }
}

// Varint decoding for the reader; `varintEnd` returns the position after it, or -1 when the buffer ends first.
let varintEnd = 0;
function readVarint(buf: Buffer, pos: number): number {
  let value = 0;
  let scale = 1;
  for (let i = 0; i < 8; i++) {
    if (pos >= buf.length) {
      varintEnd = -1;
      return 0;
    }
    const byte = buf[pos++];
    value += (byte & 0x7f) * scale;
    if (byte < 0x80) {
      varintEnd = pos;
      return value;
    }
    scale *= 128;
  }
  throw new Error('Corrupt manifest: varint longer than 8 bytes');
}

/** The manifest's entries in batches (one per decompressed chunk), checked against its header and end marker. */
export async function* manifestBatches(path: string): AsyncGenerator<ManifestBatch> {
  const header = await readManifestHeader(path);
  const source = createReadStream(path, { start: MANIFEST_BODY_OFFSET, highWaterMark: 1 << 20 });
  const zstd = createZstdDecompress({ chunkSize: 1 << 20 });
  source.on('error', (error) => zstd.destroy(error));
  source.pipe(zstd);

  let leftover: Buffer | undefined;
  let previousEnd = 0;
  let ended = false;
  let entries = 0;
  let positions = 0;
  try {
    for await (const chunk of zstd as AsyncIterable<Buffer>) {
      const data = leftover ? Buffer.concat([leftover, chunk]) : chunk;
      leftover = undefined;
      if (ended) {
        throw new Error(`${path}: data after the end marker`);
      }
      const capacity = Math.floor(data.length / (2 + HASH_BYTES)) + 1;
      const batch: ManifestBatch = {
        count: 0,
        tileIds: new Float64Array(capacity),
        runLengths: new Float64Array(capacity),
        hashHi: new Uint32Array(capacity),
        hashLo: new Uint32Array(capacity),
      };
      let pos = 0;
      while (pos < data.length) {
        const gap = readVarint(data, pos);
        const afterGap = varintEnd;
        if (afterGap < 0) {
          break;
        }
        const runLength = readVarint(data, afterGap);
        const afterRun = varintEnd;
        if (afterRun < 0) {
          break;
        }
        if (runLength === 0) {
          if (gap !== 0 || afterRun !== data.length) {
            throw new Error(`${path}: corrupt end marker`);
          }
          ended = true;
          pos = afterRun;
          break;
        }
        if (afterRun + HASH_BYTES > data.length) {
          break;
        }
        const tileId = previousEnd + gap;
        if (tileId + runLength > TILE_ID_END) {
          throw new Error(
            `${path}: positions ${tileId}..${tileId + runLength - 1} go past zoom ${MAX_SAFE_ZOOM} (tile IDs end at ` +
              `${TILE_ID_END})`,
          );
        }
        const i = batch.count++;
        batch.tileIds[i] = tileId;
        batch.runLengths[i] = runLength;
        batch.hashHi[i] = data.readUInt32BE(afterRun);
        batch.hashLo[i] = data.readUInt32BE(afterRun + 4);
        previousEnd += gap + runLength;
        positions += runLength;
        pos = afterRun + HASH_BYTES;
      }
      if (pos < data.length) {
        leftover = Buffer.from(data.subarray(pos));
      }
      entries += batch.count;
      if (batch.count > 0) {
        yield batch;
      }
    }
  } finally {
    source.destroy();
    zstd.destroy();
  }
  if (!ended || leftover) {
    throw new Error(`${path}: truncated manifest`);
  }
  if (entries !== header.entries || positions !== header.positions) {
    throw new Error(
      `${path}: ${entries} entries covering ${positions} positions, header says ${header.entries} and ${header.positions}`,
    );
  }
}

/**
 * The zooms a manifest's build spans: its header's (the archive's, or the zooms a sync was limited to), and for a
 * header without them (a manifest written without zooms) the zooms of its first and last positions, read from the
 * entries. Not its position count: a build that is sparse or starts above zoom 0 holds fewer positions than the
 * tile IDs it reaches. Null for a header without zooms over no position; throws for zooms no tile ID has, or reversed.
 */
export async function manifestZoomBounds(path: string): Promise<{ minZoom: number; maxZoom: number } | null> {
  const header = await readManifestHeader(path);
  let { minZoom, maxZoom } = header;
  if (minZoom === null || maxZoom === null) {
    let first: number | undefined;
    let end = 0;
    for await (const batch of manifestBatches(path)) {
      first ??= batch.tileIds[0];
      end = batch.tileIds[batch.count - 1] + batch.runLengths[batch.count - 1];
    }
    if (first === undefined) {
      return null;
    }
    minZoom ??= tileIdZoom(first);
    maxZoom ??= tileIdZoom(end - 1);
  }
  const zoom = (z: number) => Number.isInteger(z) && z >= 0 && z <= MAX_SAFE_ZOOM;
  if (!zoom(minZoom) || !zoom(maxZoom) || minZoom > maxZoom) {
    throw new Error(`${path}: its zooms, ${minZoom} to ${maxZoom}, are no range of tile zooms`);
  }
  return { minZoom, maxZoom };
}

/**
 * Writes the part of a manifest below tile ID `end` (a run crossing it is cut there) to `out`, e.g. the positions of
 * zooms up to Z for a sync limited to them. `maxZoom` replaces the header's when it is lower.
 */
export async function clipManifest(
  source: string,
  out: string,
  end: number,
  maxZoom?: number,
): Promise<ManifestHeader> {
  const header = await readManifestHeader(source);
  const writer = await ManifestWriter.create(out, {
    buildKey: header.buildKey,
    minZoom: header.minZoom,
    maxZoom: maxZoom !== undefined && (header.maxZoom === null || maxZoom < header.maxZoom) ? maxZoom : header.maxZoom,
    osmDate: header.osmDate,
  });
  try {
    batches: for await (const batch of manifestBatches(source)) {
      for (let i = 0; i < batch.count; i++) {
        const tileId = batch.tileIds[i];
        if (tileId >= end) {
          break batches;
        }
        writer.add(tileId, Math.min(batch.runLengths[i], end - tileId), batch.hashHi[i], batch.hashLo[i]);
      }
      await writer.drain();
    }
    return await writer.finish();
  } catch (error) {
    await writer.abort();
    throw error;
  }
}

/** Every entry of a manifest in tile-ID order. */
export async function* readManifest(path: string): AsyncGenerator<ManifestEntry> {
  for await (const batch of manifestBatches(path)) {
    for (let i = 0; i < batch.count; i++) {
      yield {
        tileId: batch.tileIds[i],
        runLength: batch.runLengths[i],
        hash: hashHex(batch.hashHi[i], batch.hashLo[i]),
      };
    }
  }
}

export interface BuildManifestOptions {
  out: string;
  buildKey: string;
  /** Hashing threads; defaults to every core. */
  threads?: number;
  /** Leaf directories per job handed to a thread. */
  leavesPerJob?: number;
  /** Finished jobs kept to resolve repeats that point back into them. */
  windowJobs?: number;
  /** Repeated tiles (offset, length) remembered after that, e.g. the ocean tile. */
  cacheEntries?: number;
  level?: number;
  onProgress?: (progress: ManifestProgress) => void;
  /**
   * The file to read, as ArchiveRanges or identifyArchive saw it (see FileSnapshot): every descriptor that reads the
   * archive, on any thread, must be open on that file, and the path must still name it, unchanged, once all is read.
   * Ties the manifest to the file a sync's identity and uploads are of.
   */
  source?: FileSnapshot;
}

export interface ManifestProgress {
  jobs: number;
  jobsTotal: number;
  archiveEntries: number;
  archiveEntriesTotal: number;
  bytesHashed: number;
}

export interface ManifestBuildStats {
  header: ManifestHeader;
  archiveEntries: number;
  /** Distinct tile contents according to the archive header. */
  tileContents: number;
  /** Tiles read and hashed; equal to tileContents when every repeat was resolved without reading it again. */
  tilesHashed: number;
  bytesHashed: number;
  jobs: number;
  threads: number;
  seconds: number;
  fileBytes: number;
}

/** Root entries grouped into jobs, in tile-ID order: runs of leaf pointers, or runs of tiles held in the root. */
function planJobs(root: EntryArrays, leavesPerJob: number): JobPlan[] {
  const jobs: JobPlan[] = [];
  let leaves: [number, number][] = [];
  let tilesFrom = -1;
  const flushTiles = (end: number) => {
    if (tilesFrom >= 0) {
      jobs.push({
        entries: {
          tileIds: root.tileIds.slice(tilesFrom, end),
          runLengths: root.runLengths.slice(tilesFrom, end),
          offsets: root.offsets.slice(tilesFrom, end),
          lengths: root.lengths.slice(tilesFrom, end),
        },
      });
      tilesFrom = -1;
    }
  };
  const flushLeaves = () => {
    if (leaves.length > 0) {
      jobs.push({ leaves });
      leaves = [];
    }
  };
  for (let i = 0; i < root.tileIds.length; i++) {
    if (root.runLengths[i] === 0) {
      flushTiles(i);
      leaves.push([root.offsets[i], root.lengths[i]]);
      if (leaves.length >= leavesPerJob) {
        flushLeaves();
      }
    } else {
      flushLeaves();
      if (tilesFrom < 0) {
        tilesFrom = i;
      }
    }
  }
  flushTiles(root.tileIds.length);
  flushLeaves();
  return jobs;
}

// Workers run the TypeScript module through tsx, which the CLIs already load (`node --import=tsx`).
function workerExecArgv(): string[] {
  return process.execArgv.some((arg) => /(^|[=/\\])tsx(\/|$)/.test(arg) || arg === 'tsx')
    ? process.execArgv
    : [...process.execArgv, '--import', 'tsx'];
}

/** The hashing threads. Replies land in `decoded` and `hashed` by job id; `onReply` wakes the main loop. */
class HashPool {
  private workers: { worker: Worker; outstanding: number }[] = [];
  readonly decoded = new Map<number, { slot: number; chainEnd: number }>();
  readonly hashed = new Map<number, JobResult>();
  failure: unknown;
  onReply = () => {};

  constructor(threads: number, init: WorkerInit) {
    for (let i = 0; i < threads; i++) {
      const worker = new Worker(new URL('./manifest-worker.ts', import.meta.url), {
        workerData: init,
        execArgv: workerExecArgv(),
      });
      const slot = { worker, outstanding: 0 };
      worker.on('message', (reply: WorkerReply) => {
        if (reply.type === 'decoded') {
          this.decoded.set(reply.id, { slot: i, chainEnd: reply.chainEnd });
        } else {
          slot.outstanding--;
          this.hashed.set(reply.result.id, reply.result);
        }
        this.onReply();
      });
      worker.on('error', (error) => this.fail(error));
      worker.on('exit', (code) => this.fail(new Error(`Hashing thread exited with code ${code}`)));
      this.workers.push(slot);
    }
  }

  private fail(error: unknown) {
    this.failure ??= error;
    this.onReply();
  }

  /** Starts a job on the least busy thread, which keeps it for hash(). */
  decode(job: JobSpec) {
    const slot = this.workers.reduce((best, candidate) =>
      candidate.outstanding < best.outstanding ? candidate : best,
    );
    slot.outstanding++;
    slot.worker.postMessage({ type: 'decode', job } satisfies WorkerRequest);
  }

  hash(slot: number, id: number, floor: number) {
    this.workers[slot].worker.postMessage({ type: 'hash', id, floor } satisfies WorkerRequest);
  }

  async close() {
    this.failure ??= new Error('Hashing pool closed');
    await Promise.all(this.workers.map(({ worker }) => worker.terminate()));
  }
}

/**
 * Resolves entries a thread couldn't: repeats pointing before their job's chain. They are looked up in the last few
 * jobs' chains, then in a cache of repeats seen before, and only read and hashed when neither has them.
 */
class BackReferences {
  private window: JobResult[] = [];
  private cache = new Map<number, number>();
  private cached: Uint32Array;
  private hi = 0;
  private lo = 0;
  tilesHashed = 0;
  bytesHashed = 0;

  constructor(
    private fd: number,
    private tileDataOffset: number,
    private windowJobs: number,
    private cacheEntries: number,
  ) {
    this.cached = new Uint32Array(cacheEntries * 3);
  }

  resolve(result: JobResult) {
    const { unresolved, unresolvedOffsets, unresolvedLengths, hashes } = result;
    for (let k = 0; k < unresolved.length; k++) {
      this.find(unresolvedOffsets[k], unresolvedLengths[k]);
      hashes[unresolved[k] * 2] = this.hi;
      hashes[unresolved[k] * 2 + 1] = this.lo;
    }
    this.window.push(result);
    if (this.window.length > this.windowJobs) {
      this.window.shift();
    }
  }

  private find(offset: number, length: number) {
    const slot = this.cache.get(offset);
    if (slot !== undefined && this.cached[slot * 3] === length) {
      this.hi = this.cached[slot * 3 + 1];
      this.lo = this.cached[slot * 3 + 2];
      return;
    }
    if (!this.findInWindow(offset, length)) {
      const bytes = Buffer.allocUnsafe(length);
      let done = 0;
      while (done < length) {
        const read = readSync(this.fd, bytes, done, length - done, this.tileDataOffset + offset + done);
        if (read === 0) {
          throw new Error(`Unexpected end of archive reading tile at ${offset}+${length}`);
        }
        done += read;
      }
      [this.hi, this.lo] = tileHash(bytes);
      this.tilesHashed++;
      this.bytesHashed += length;
    }
    if (slot === undefined && this.cache.size < this.cacheEntries) {
      const next = this.cache.size;
      this.cache.set(offset, next);
      this.cached[next * 3] = length;
      this.cached[next * 3 + 1] = this.hi;
      this.cached[next * 3 + 2] = this.lo;
    }
  }

  private findInWindow(offset: number, length: number): boolean {
    for (let j = this.window.length - 1; j >= 0; j--) {
      const { chainOffsets, chainLengths, chainEntries, hashes } = this.window[j];
      const last = chainOffsets.length - 1;
      if (last < 0 || offset < chainOffsets[0] || offset > chainOffsets[last]) {
        continue;
      }
      const k = findChainTile(chainOffsets, chainLengths, offset, length);
      if (k >= 0) {
        this.hi = hashes[chainEntries[k] * 2];
        this.lo = hashes[chainEntries[k] * 2 + 1];
        return true;
      }
    }
    return false;
  }
}

/** Throws when `out` names the archive itself (the same path, a hard link or a symlink to it). */
async function refuseArchiveAsOutput(archivePath: string, out: string): Promise<void> {
  const archive = await stat(archivePath);
  const existing = await stat(out).catch((error: NodeJS.ErrnoException) => {
    if (error.code === 'ENOENT') {
      return undefined;
    }
    throw error;
  });
  if (existing && existing.dev === archive.dev && existing.ino === archive.ino) {
    throw new Error(`${out} is the archive ${archivePath}: writing the manifest there would replace the archive`);
  }
}

/** Builds the manifest of a local PMTiles archive, hashing its tiles on every core. */
export async function buildManifest(archivePath: string, options: BuildManifestOptions): Promise<ManifestBuildStats> {
  const started = performance.now();
  await refuseArchiveAsOutput(archivePath, options.out);
  const fd = openSync(archivePath, 'r');
  let pool: HashPool | undefined;
  let writer: ManifestWriter | undefined;
  try {
    if (options.source !== undefined) {
      checkOpened(archivePath, fd, options.source);
    }
    // The header from the descriptor the rest is read from, so all of it is of one file.
    const { header, metadata } = await Archive.open(async (position, length) => {
      const buffer = Buffer.allocUnsafe(length);
      readFully(fd, buffer, length, position);
      return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + length) as ArrayBuffer;
    }).catch((error: unknown) => {
      throw new Error(`${archivePath}: ${error instanceof Error ? error.message : String(error)}`, { cause: error });
    });
    const rootBytes = Buffer.allocUnsafe(header.rootDirectoryLength);
    if (readSync(fd, rootBytes, 0, rootBytes.length, header.rootDirectoryOffset) !== rootBytes.length) {
      throw new Error(`${archivePath}: short read of the root directory`);
    }
    const root = decodeDirectory(decompressDirectory(rootBytes, header.internalCompression));
    const jobs = planJobs(root, options.leavesPerJob ?? 4).map((job, id): JobSpec => ({ ...job, id }));
    const threads = Math.max(1, Math.min(options.threads ?? availableParallelism(), jobs.length));

    writer = await ManifestWriter.create(
      options.out,
      { buildKey: options.buildKey, minZoom: header.minZoom, maxZoom: header.maxZoom, osmDate: osmDateOf(metadata) },
      options.level,
    );
    pool = new HashPool(threads, {
      manifestWorker: true,
      path: archivePath,
      source: options.source,
      leafDirectoryOffset: header.leafDirectoryOffset,
      tileDataOffset: header.tileDataOffset,
      internalCompression: header.internalCompression,
    });
    const backReferences = new BackReferences(
      fd,
      header.tileDataOffset,
      options.windowJobs ?? 8,
      options.cacheEntries ?? 1 << 20,
    );

    // Jobs are decoded ahead of the in-order merge by a bounded amount, which bounds memory. Floors go out in job
    // order as decodes come back: a job's new tiles start where all earlier jobs' chains end.
    const ahead = threads * 2;
    let dispatched = 0;
    let floored = 0;
    let frontier = 0;
    let archiveEntries = 0;
    let chainTiles = 0;
    let chainBytes = 0;
    for (let merged = 0; merged < jobs.length;) {
      if (pool.failure) {
        throw pool.failure;
      }
      while (dispatched < jobs.length && dispatched - merged < ahead) {
        pool.decode(jobs[dispatched++]);
      }
      for (let decoded = pool.decoded.get(floored); decoded; decoded = pool.decoded.get(floored)) {
        pool.decoded.delete(floored);
        pool.hash(decoded.slot, floored++, frontier);
        frontier = Math.max(frontier, decoded.chainEnd);
      }
      const result = pool.hashed.get(merged);
      if (!result) {
        await new Promise<void>((resolve) => (pool!.onReply = resolve));
        continue;
      }
      pool.hashed.delete(merged++);
      backReferences.resolve(result);
      const { tileIds, runLengths, hashes } = result;
      for (let i = 0; i < tileIds.length; i++) {
        writer.add(tileIds[i], runLengths[i], hashes[i * 2], hashes[i * 2 + 1]);
      }
      await writer.drain();
      archiveEntries += tileIds.length;
      chainTiles += result.chainEntries.length;
      chainBytes += result.bytesHashed;
      options.onProgress?.({
        jobs: merged,
        jobsTotal: jobs.length,
        archiveEntries,
        archiveEntriesTotal: header.numTileEntries,
        bytesHashed: chainBytes + backReferences.bytesHashed,
      });
    }

    if (options.source !== undefined && fileSnapshot(await stat(archivePath, { bigint: true })) !== options.source) {
      throw new Error(`${archivePath} was replaced or written to while its manifest was built`);
    }
    const manifestHeader = await writer.finish();
    writer = undefined;
    return {
      header: manifestHeader,
      archiveEntries,
      tileContents: header.numTileContents,
      tilesHashed: chainTiles + backReferences.tilesHashed,
      bytesHashed: chainBytes + backReferences.bytesHashed,
      jobs: jobs.length,
      threads,
      seconds: (performance.now() - started) / 1000,
      fileBytes: (await stat(options.out)).size,
    };
  } finally {
    await writer?.abort();
    await pool?.close();
    closeSync(fd);
  }
}
