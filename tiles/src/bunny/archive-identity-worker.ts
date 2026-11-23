/**
 * Worker thread of archiveIdentity (archive-identity.ts): takes block numbers from a shared counter until none are
 * left and writes each block's SHA-256 into the shared digest array at the block's place. Reads are synchronous
 * preads on this thread, so threads read in parallel without queueing on libuv's small thread pool.
 */
import { createHash } from 'node:crypto';
import { BigIntStats, closeSync, fstatSync, openSync, readSync } from 'node:fs';
import { parentPort, workerData } from 'node:worker_threads';

/**
 * Which file an archive is and whether it changed: device, inode, size, and modification and change times. Not part
 * of the identity (a copy of the archive has the same identity and another snapshot), so it is never recorded: within
 * one run it ties every descriptor that reads the archive to the file its identity was computed from.
 *
 * The change time moves on any write and on any change to the file's metadata, and on ext4 and tmpfs (not ZFS) also
 * when the file loses a name: a descriptor held while another file is renamed over its path, or while it is deleted,
 * no longer matches either, though it still reads the same bytes. The run then stops rather than tell that apart
 * from a write. Nothing replaces, links, renames or deletes the archive while bunny:sync or bunny:seed reads it (the
 * sync workflow downloads and checks it in a step before the sync), so only someone doing that by hand stops a run
 * this way. The legacy adopt check (legacySourceProblem in sync.ts) relies on the same change time.
 */
export type FileSnapshot = string;

export function fileSnapshot(stats: BigIntStats): FileSnapshot {
  return `${stats.dev}:${stats.ino}:${stats.size}:${stats.mtimeNs}:${stats.ctimeNs}`;
}

/**
 * Throws unless `fd` is open on the file `source` is a snapshot of, unchanged: the path may have been replaced (with a
 * file of the same size, say) or written to since.
 */
export function checkOpened(path: string, fd: number, source: FileSnapshot): void {
  if (fileSnapshot(fstatSync(fd, { bigint: true })) !== source) {
    throw new Error(`${path} is no longer the file its identity was computed from: it was replaced or written to`);
  }
}

export interface IdentityWorkerData {
  archiveIdentityWorker: true;
  path: string;
  size: number;
  /** The file the identity is of: each thread checks that it opened it. */
  source: FileSnapshot;
  blockBytes: number;
  blocks: number;
  /** [0]: the next block to take, [1]: blocks hashed. */
  counters: Int32Array;
  /** 32 bytes per block. */
  digests: Uint8Array;
}

const READ_BYTES = 8 * 2 ** 20;

/** The SHA-256 of bytes [start, end) of an open file. */
export function hashRange(fd: number, start: number, end: number, buffer: Buffer): Uint8Array {
  const hash = createHash('sha256');
  for (let at = start; at < end;) {
    const length = Math.min(buffer.length, end - at);
    const read = readSync(fd, buffer, 0, length, at);
    if (read === 0) {
      throw new Error(`Unexpected end of file at ${at}, expected ${end - start} bytes from ${start}`);
    }
    hash.update(buffer.subarray(0, read));
    at += read;
  }
  return hash.digest();
}

export const readBuffer = (blockBytes: number) => Buffer.allocUnsafeSlow(Math.min(blockBytes, READ_BYTES));

// Only as a worker started by archiveIdentity; importing this module elsewhere runs nothing.
const data = workerData as IdentityWorkerData | undefined;
if (parentPort && data?.archiveIdentityWorker) {
  const fd = openSync(data.path, 'r');
  try {
    checkOpened(data.path, fd, data.source);
    const buffer = readBuffer(data.blockBytes);
    for (let block = Atomics.add(data.counters, 0, 1); block < data.blocks; block = Atomics.add(data.counters, 0, 1)) {
      const start = block * data.blockBytes;
      data.digests.set(hashRange(fd, start, Math.min(data.size, start + data.blockBytes), buffer), block * 32);
      Atomics.add(data.counters, 1, 1);
    }
  } finally {
    closeSync(fd);
  }
  parentPort.postMessage({ done: true });
}
