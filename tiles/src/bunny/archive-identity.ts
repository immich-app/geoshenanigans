/**
 * The identity of a PMTiles archive that seed checkpoints, state.json and --adopt compare: version 2, a SHA-256 tree
 * hash of the whole file. The file is cut into 64 MiB blocks, each block is hashed on its own (in parallel, on worker
 * threads), and the identity is the SHA-256 of a version tag, the file size and the block digests in order:
 *
 *   "sha256-tree-v2:" + hex(SHA-256("pmtiles-archive-identity-v2\n" + size + "\n" + digest(block 0) + ...))
 *
 * Any byte of the file changes it, tile data included. Version 1 (the "fingerprint" seeds recorded before it) hashed
 * only the header, the root directory and the size, so archives with other tile bytes could share one: a v1
 * fingerprint is never taken as an identity on its own (see checkSeeds in sync.ts).
 */
import { createHash } from 'node:crypto';
import { closeSync, createReadStream, openSync } from 'node:fs';
import { stat } from 'node:fs/promises';
import { availableParallelism } from 'node:os';
import { pipeline } from 'node:stream/promises';
import { URL } from 'node:url';
import { Worker } from 'node:worker_threads';
import {
  checkOpened,
  FileSnapshot,
  fileSnapshot,
  hashRange,
  IdentityWorkerData,
  readBuffer,
} from './archive-identity-worker';

export { checkOpened, fileSnapshot, type FileSnapshot };

export const IDENTITY_PREFIX = 'sha256-tree-v2:';
export const IDENTITY_PATTERN = /^sha256-tree-v2:[0-9a-f]{64}$/;
export const IDENTITY_BLOCK_BYTES = 64 * 2 ** 20;
const IDENTITY_TAG = 'pmtiles-archive-identity-v2\n';
/** Threads hashing at most: past this the disk, not the CPU, sets the pace. */
const MAX_THREADS = 16;

/** An archive as seed checkpoints, state.json and --adopt record it. */
export interface ArchiveIdentity {
  size: number;
  /** `sha256-tree-v2:<hex>`. */
  identity: string;
}

/** An archive's identity, and the file it was computed from. */
export interface IdentifiedArchive {
  identity: ArchiveIdentity;
  source: FileSnapshot;
}

export interface IdentityOptions {
  /** Hashing threads (default: the cores, at most 16). */
  threads?: number;
  /** Tests only: another block size gives another identity, which no checkpoint or state would match. */
  blockBytes?: number;
}

/** The v2 identity of the archive at `path`. A planet build (~140 GB) takes about 15 s from NVMe on 8-16 threads. */
export async function archiveIdentity(path: string, options: IdentityOptions = {}): Promise<ArchiveIdentity> {
  return (await identifyArchive(path, options)).identity;
}

/**
 * The v2 identity of the archive at `path` and the snapshot of the file it is of: every thread that reads the file
 * checks that it opened that file, and the file must not have changed by the end.
 */
export async function identifyArchive(path: string, options: IdentityOptions = {}): Promise<IdentifiedArchive> {
  const blockBytes = options.blockBytes ?? IDENTITY_BLOCK_BYTES;
  if (!Number.isSafeInteger(blockBytes) || blockBytes < 1) {
    throw new Error(`Bad identity block size ${blockBytes}`);
  }
  const before = await stat(path, { bigint: true });
  const source = fileSnapshot(before);
  const size = Number(before.size);
  const blocks = Math.ceil(size / blockBytes);
  // One block hashes here at once; more go to threads, so the event loop (signals, progress) keeps running.
  const digests =
    blocks <= 1
      ? hashHere(path, size, source)
      : await hashInWorkers(path, size, source, blockBytes, blocks, options.threads ?? defaultThreads());
  // A file written to while it was read has no identity.
  if (fileSnapshot(await stat(path, { bigint: true })) !== source) {
    throw new Error(`${path} changed while its identity was computed`);
  }
  const root = createHash('sha256').update(`${IDENTITY_TAG}${size}\n`).update(digests).digest('hex');
  return { identity: { size, identity: IDENTITY_PREFIX + root }, source };
}

const defaultThreads = () => Math.min(availableParallelism(), MAX_THREADS);

function hashHere(path: string, size: number, source: FileSnapshot): Uint8Array {
  const fd = openSync(path, 'r');
  try {
    checkOpened(path, fd, source);
    return size === 0 ? new Uint8Array(0) : hashRange(fd, 0, size, readBuffer(size));
  } finally {
    closeSync(fd);
  }
}

/** Threads take blocks from a shared counter and write each digest into a shared array at the block's place. */
async function hashInWorkers(
  path: string,
  size: number,
  source: FileSnapshot,
  blockBytes: number,
  blocks: number,
  threads: number,
): Promise<Uint8Array> {
  const counters = new Int32Array(new SharedArrayBuffer(8));
  const digests = new Uint8Array(new SharedArrayBuffer(blocks * 32));
  const data: IdentityWorkerData = {
    archiveIdentityWorker: true,
    path,
    size,
    source,
    blockBytes,
    blocks,
    counters,
    digests,
  };
  const workers = Array.from(
    { length: Math.max(1, Math.min(threads, blocks)) },
    () =>
      new Worker(new URL('./archive-identity-worker.ts', import.meta.url), {
        workerData: data,
        execArgv: workerExecArgv(),
      }),
  );
  try {
    await Promise.all(
      workers.map(
        (worker) =>
          new Promise<void>((resolve, reject) => {
            worker.once('message', () => resolve());
            worker.once('error', reject);
            worker.once('exit', (code) => reject(new Error(`Identity thread exited with code ${code}`)));
          }),
      ),
    );
  } finally {
    await Promise.all(workers.map((worker) => worker.terminate()));
  }
  if (Atomics.load(counters, 1) !== blocks) {
    throw new Error(`Identity threads hashed ${Atomics.load(counters, 1)} of ${blocks} blocks`);
  }
  return Uint8Array.from(digests);
}

// Workers run the TypeScript module through tsx, which the CLIs already load (`node --import=tsx`).
function workerExecArgv(): string[] {
  return process.execArgv.some((arg) => /(^|[=/\\])tsx(\/|$)/.test(arg) || arg === 'tsx')
    ? process.execArgv
    : [...process.execArgv, '--import', 'tsx'];
}

/** The MD5 of the whole file, base64 as builds.json gives it. */
export async function fileMd5Base64(path: string, signal?: AbortSignal): Promise<string> {
  const hash = createHash('md5');
  await pipeline(createReadStream(path, { highWaterMark: 8 * 2 ** 20 }), hash, { signal });
  return hash.digest('base64');
}

/** A base64 MD5 as builds.json gives it (16 bytes); throws on anything else. */
export function parseMd5Base64(text: string): string {
  if (!/^[A-Za-z0-9+/]{21}[AQgw]==$/.test(text)) {
    throw new Error(`--archive-md5 must be a base64 MD5 as builds.json gives it, got ${JSON.stringify(text)}`);
  }
  return text;
}
