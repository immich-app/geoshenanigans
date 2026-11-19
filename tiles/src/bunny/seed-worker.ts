/**
 * Seed worker thread: takes chunk descriptors from the main thread, reads each run's tile bytes from the archive with
 * pread (once per run, small repeated tiles such as the ocean once per worker), and uploads every position of the
 * run through its own storage client. Reports when every position of a chunk was handed out (so the main thread
 * sends the next one even while a few slow files of it are in flight) and when every position in it is stored.
 */
import { closeSync, openSync, readSync } from 'node:fs';
import { parentPort, workerData } from 'node:worker_threads';
import { BunnyStorage, StorageCounters } from './storage';
import {
  ChunkDescriptor,
  MainMessage,
  newUploadStats,
  sha256Hex,
  UploadFailure,
  UploadItem,
  uploadTiles,
  WorkerConfig,
  WorkerMessage,
} from './upload';

const CACHE_MAX_TILE_BYTES = 4096;
const CACHE_ENTRIES = 4096;
const PROGRESS_MS = 1_000;

interface ChunkState {
  chunkId: number;
  emitted: number;
  settled: number;
  emittedAll: boolean;
  reported: boolean;
  failures: UploadFailure[];
}

type SeedItem = UploadItem & { chunk: ChunkState };

const port = parentPort!;
const config = workerData as WorkerConfig;
const post = (message: WorkerMessage) => port.postMessage(message);

const fd = openSync(config.archivePath, 'r');
const cache = new Map<number, { length: number; bytes: Uint8Array; sha256: string }>();

function content(offset: number, length: number): { bytes: Uint8Array; sha256: string } {
  const cached = cache.get(offset);
  if (cached && cached.length === length) {
    cache.delete(offset);
    cache.set(offset, cached);
    return cached;
  }
  const bytes = Buffer.allocUnsafeSlow(length);
  let done = 0;
  while (done < length) {
    const read = readSync(fd, bytes, done, length - done, config.tileDataOffset + offset + done);
    if (read === 0) {
      throw new Error(`Unexpected end of ${config.archivePath} reading tile data at ${offset}`);
    }
    done += read;
  }
  const entry = { length, bytes, sha256: sha256Hex(bytes) };
  if (length <= CACHE_MAX_TILE_BYTES) {
    if (cache.size >= CACHE_ENTRIES) {
      cache.delete(cache.keys().next().value!);
    }
    cache.set(offset, entry);
  }
  return entry;
}

const queue: ChunkDescriptor[] = [];
const states = new Map<number, ChunkState>();
const stop = new AbortController();
let ended = false;
let wake: (() => void) | undefined;

port.on('message', (message: MainMessage) => {
  if (message.type === 'chunk') {
    queue.push(message.chunk);
  } else if (message.type === 'end') {
    ended = true;
  } else {
    stop.abort();
  }
  wake?.();
});

function finish(state: ChunkState) {
  if (!state.reported && state.emittedAll && state.settled === state.emitted) {
    state.reported = true;
    states.delete(state.chunkId);
    post({ type: 'chunk-done', chunkId: state.chunkId, failures: state.failures });
  }
}

async function* items(): AsyncGenerator<SeedItem> {
  for (;;) {
    while (queue.length === 0 && !ended && !stop.signal.aborted) {
      await new Promise<void>((resolve) => (wake = resolve));
    }
    const chunk = queue.shift();
    if (!chunk || stop.signal.aborted) {
      if (chunk) {
        queue.unshift(chunk);
      }
      return;
    }
    const state: ChunkState = {
      chunkId: chunk.chunkId,
      emitted: 0,
      settled: 0,
      emittedAll: false,
      reported: false,
      failures: [],
    };
    states.set(chunk.chunkId, state);
    const { runs } = chunk;
    for (let i = 0; i < runs.length; i += 4) {
      const [tileId, runLength, offset, length] = [runs[i], runs[i + 1], runs[i + 2], runs[i + 3]];
      const { bytes, sha256 } = content(offset, length);
      for (let position = tileId; position < tileId + runLength; position++) {
        if (stop.signal.aborted) {
          return;
        }
        state.emitted++;
        yield { tileId: position, bytes, sha256, verify: chunk.verify, chunk: state };
      }
    }
    state.emittedAll = true;
    post({ type: 'chunk-emitted', chunkId: chunk.chunkId });
    finish(state);
  }
}

async function main() {
  // The stop also ends retry back-offs, so a stop is not held up by a file waiting out 429s.
  const storage = new BunnyStorage({ ...config.storage, signal: stop.signal });
  const stats = newUploadStats();
  const snapshot = (): { stats: typeof stats; counters: StorageCounters } => ({
    stats: { ...stats, failures: [] },
    counters: { ...storage.counters },
  });
  const progress = setInterval(
    () => post({ type: 'progress', ...snapshot(), inFlight: storage.inFlight }),
    PROGRESS_MS,
  );
  try {
    const result = await uploadTiles(storage, items(), {
      prefix: config.prefix,
      concurrency: config.concurrency,
      maxFailures: config.maxFailures,
      signal: stop.signal,
      stats,
      onSettled: (item, outcome, failure) => {
        const state = item.chunk;
        state.settled++;
        if (outcome === 'failed' && failure) {
          state.failures.push(failure);
        }
        finish(state);
      },
    });
    if (result.error) {
      post({ type: 'fatal', error: result.error });
    }
  } catch (error) {
    post({ type: 'fatal', error: error instanceof Error ? error.message : String(error) });
  } finally {
    clearInterval(progress);
    // Whatever was not finished stays "started" in the checkpoint, so a resume verifies it.
    for (const state of states.values()) {
      if (!state.reported) {
        post({ type: 'chunk-stopped', chunkId: state.chunkId });
      }
    }
    for (const chunk of queue) {
      post({ type: 'chunk-stopped', chunkId: chunk.chunkId });
    }
    post({ type: 'final', ...snapshot() });
    await storage.close();
    closeSync(fd);
    port.close();
  }
}

void main();
