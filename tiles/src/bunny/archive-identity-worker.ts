/**
 * Worker thread of archiveIdentity (archive-identity.ts): takes block numbers from a shared counter until none are
 * left and writes each block's SHA-256 into the shared digest array at the block's place. Reads are synchronous
 * preads on this thread, so threads read in parallel without queueing on libuv's small thread pool.
 */
import { createHash } from 'node:crypto';
import { closeSync, openSync, readSync } from 'node:fs';
import { parentPort, workerData } from 'node:worker_threads';

export interface IdentityWorkerData {
  archiveIdentityWorker: true;
  path: string;
  size: number;
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
