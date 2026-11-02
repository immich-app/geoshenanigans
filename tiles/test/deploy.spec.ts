import { env } from 'cloudflare:test';
import { inject } from 'vitest';
import { handleD1ProxyRequest } from '../src/d1-proxy';
import { Archive } from '../src/deploy/archive';
import { D1ProxyClient } from '../src/deploy/d1-proxy-client';
import { inventoryProblems } from '../src/deploy/inventory';
import { Chunk, CHUNK_SIZE_BYTES, ChunkPlanner, ENTRIES_PER_ROW, toDirectoryRows } from '../src/deploy/layout';
import { prepareStagingTable, publishStagingTable, STAGING_TABLE, StagingDatabase } from '../src/deploy/staging';
import { DirectoryString } from '../src/pmtiles.service';
import { Compression, Entry } from '../src/pmtiles/types';
import { buildArchive, FixtureEntry, memoryReader } from './pmtiles-fixture';

const entry = (tileId: number, offset: number, length: number, runLength = 1): Entry => ({
  tileId,
  offset,
  length,
  runLength,
  chunkId: 0,
});

describe('ChunkPlanner', () => {
  it('completes a chunk only when a later tile starts the next one', () => {
    const completed: Chunk[] = [];
    const planner = new ChunkPlanner((chunk) => completed.push({ ...chunk }));

    expect(planner.place(entry(0, 0, 400_000))).toEqual({ chunkId: 0, offset: 0 });
    // Crosses the 1 MB boundary but starts in chunk 0, so chunk 0 grows past it.
    expect(planner.place(entry(1, 900_000, 300_000))).toEqual({ chunkId: 0, offset: 900_000 });
    expect(completed).toEqual([]);

    expect(planner.place(entry(2, 1_200_000, 100))).toEqual({ chunkId: 1, offset: 0 });
    expect(completed).toEqual([{ chunkId: 0, startByte: 0, endByte: 1_200_000 }]);

    planner.finish();
    expect(completed[1]).toEqual({ chunkId: 1, startByte: 1_200_000, endByte: 1_200_100 });
    expect(planner.chunkCount).toBe(2);
  });

  it('resolves deduplicated tiles inside completed chunks', () => {
    const planner = new ChunkPlanner(() => {});
    planner.place(entry(0, 10, 50));
    planner.place(entry(1, CHUNK_SIZE_BYTES + 5, 50));
    expect(planner.place(entry(2, 10, 50))).toEqual({ chunkId: 0, offset: 0 });
    expect(() => planner.place(entry(3, 5, 10))).toThrow(/outside completed chunk/);
  });

  it('rejects tiles that go backwards inside the current chunk', () => {
    const planner = new ChunkPlanner(() => {});
    planner.place(entry(0, 500, 10));
    expect(() => planner.place(entry(1, 100, 10))).toThrow(/starts before chunk/);
  });
});

describe('toDirectoryRows', () => {
  it('lets the worker find every tile, run and gap', async () => {
    // Clustered tile data with ~300 KB tiles, a run of identical tiles, a gap and a deduplicated tile.
    const entries: Entry[] = [];
    let tileId = 0;
    let offset = 0;
    for (let i = 0; i < 180; i++) {
      const runLength = i === 7 ? 40 : 1;
      entries.push(entry(tileId, offset, 300_000 + i, runLength));
      tileId += runLength + (i === 90 ? 5 : 0);
      offset += 300_000 + i;
    }
    entries.push(entry(tileId, entries[3].offset, entries[3].length));

    const chunks = new Map<number, Chunk>();
    const planner = new ChunkPlanner((chunk) => chunks.set(chunk.chunkId, chunk));
    const rows = toDirectoryRows(entries, planner);
    planner.finish();
    expect(rows).toHaveLength(Math.ceil(entries.length / ENTRIES_PER_ROW));

    const lookup = (id: number) => {
      const row = rows.filter((candidate) => candidate.startTileId <= id).at(-1)!;
      return new DirectoryString(row.entry).findTile(id);
    };

    for (const expected of entries) {
      const probe = expected.tileId + expected.runLength - 1;
      const found = await lookup(probe);
      expect(found?.length).toBe(expected.length);
      const chunk = chunks.get(found!.chunkId)!;
      expect(chunk.startByte + found!.offset).toBe(expected.offset);
      expect(chunk.startByte + found!.offset + found!.length).toBeLessThanOrEqual(chunk.endByte);
    }
    const gap = entries[90].tileId + 1;
    expect(await lookup(gap)).toBeUndefined();
  });
});

describe('D1 proxy', () => {
  const proxyEnv = {
    ...env,
    D1_PROD: env.D1_GLOBAL,
    D1_PROXY_TOKEN_PROD: 'prod-secret',
    D1_PROXY_TOKEN_DEV: 'dev-secret',
  } as WorkerEnv;
  const post = (body: unknown, { path = 'prod', token = 'prod-secret' } = {}) =>
    handleD1ProxyRequest(
      new Request(`https://proxy/${path}`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}` },
        body: JSON.stringify(body),
      }),
      proxyEnv,
    );

  it('rejects a wrong or missing token', async () => {
    expect((await post({ statement: { sql: 'SELECT 1' } }, { token: 'nope' })).status).toBe(401);
    const response = await handleD1ProxyRequest(new Request('https://proxy/prod', { method: 'POST' }), proxyEnv);
    expect(response.status).toBe(401);
  });

  it("only accepts an environment's own token", async () => {
    expect((await post({ statement: { sql: 'SELECT 1' } }, { token: 'dev-secret' })).status).toBe(401);
    expect((await post({ statement: { sql: 'SELECT 1' } }, { path: 'dev', token: 'prod-secret' })).status).toBe(401);
    expect((await post({ statement: { sql: 'SELECT 1' } }, { path: 'toString', token: 'prod-secret' })).status).toBe(
      401,
    );
    expect((await post({ statement: { sql: 'SELECT 1' } }, { path: '', token: 'prod-secret' })).status).toBe(401);
  });

  it('rejects malformed bodies and environments without a database', async () => {
    expect((await post({ statement: { sql: 1 } })).status).toBe(400);
    expect((await post({ batch: [] })).status).toBe(400);
    expect((await post({ statement: { sql: 'SELECT 1' } }, { path: 'dev', token: 'dev-secret' })).status).toBe(500);
  });

  it("sends each environment's requests to its own database", async () => {
    const received: string[] = [];
    const stub = (name: string) =>
      ({
        prepare: (sql: string) => ({
          bind: () => ({
            run: async () => {
              received.push(`${name}: ${sql}`);
              return { results: [] };
            },
          }),
        }),
        batch: async (statements: unknown[]) => {
          received.push(`${name}: batch of ${statements.length}`);
          return statements.map(() => ({ results: [] }));
        },
      }) as unknown as D1Database;
    const routedEnv = { ...proxyEnv, D1_PROD: stub('prod'), D1_DEV: stub('dev') } as WorkerEnv;
    const send = (path: string, token: string, body: unknown) =>
      handleD1ProxyRequest(
        new Request(`https://proxy/${path}`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${token}` },
          body: JSON.stringify(body),
        }),
        routedEnv,
      );

    expect((await send('prod', 'prod-secret', { statement: { sql: 'SELECT 1' } })).status).toBe(200);
    expect((await send('dev', 'dev-secret', { statement: { sql: 'SELECT 2' } })).status).toBe(200);
    expect((await send('dev', 'dev-secret', { batch: [{ sql: 'SELECT 3' }, { sql: 'SELECT 4' }] })).status).toBe(200);
    expect(received).toEqual(['prod: SELECT 1', 'dev: SELECT 2', 'dev: batch of 2']);
  });

  it('runs bound statements and batches against the selected database', async () => {
    await post({ statement: { sql: 'CREATE TABLE IF NOT EXISTS proxy_test (id INTEGER PRIMARY KEY, v TEXT)' } });
    const batch = await post({
      batch: [
        { sql: 'INSERT OR REPLACE INTO proxy_test (id, v) VALUES (?, ?), (?, ?)', params: [1, "it's", 2, 'b'] },
        { sql: 'INSERT OR REPLACE INTO proxy_test (id, v) VALUES (?, ?)', params: [3, 'c'] },
      ],
    });
    expect(batch.status).toBe(200);

    const select = await post({
      statement: { sql: 'SELECT v FROM proxy_test WHERE id <= ? ORDER BY id', params: [3] },
    });
    expect(await select.json()).toEqual({ results: [[{ v: "it's" }, { v: 'b' }, { v: 'c' }]] });
  });

  it('rolls back a batch when one statement fails', async () => {
    await post({ statement: { sql: 'CREATE TABLE IF NOT EXISTS proxy_atomic (id INTEGER PRIMARY KEY)' } });
    const response = await post({
      batch: [
        { sql: 'INSERT INTO proxy_atomic (id) VALUES (?)', params: [1] },
        { sql: 'INSERT INTO proxy_atomic (id) VALUES (?)', params: [1] },
      ],
    });
    expect(response.status).toBe(500);
    const count = await post({ statement: { sql: 'SELECT COUNT(*) AS n FROM proxy_atomic' } });
    expect(await count.json()).toEqual({ results: [[{ n: 0 }]] });
  });
});

describe('D1ProxyClient', () => {
  it('abandons an attempt whose response stalls and retries it', async () => {
    const url = `http://localhost:${inject('port')}/stall-once/${crypto.randomUUID()}`;
    const client = new D1ProxyClient(url, 'secret', 'dev', { attemptTimeoutMs: 500, retryDelayMs: 10 });
    await expect(client.run('SELECT 1 AS ok')).resolves.toEqual([{ ok: 1 }]);
  });
});

describe('staging table', () => {
  const fakeDatabase = (tables: string[], renameFails: boolean) => {
    const statements: string[] = [];
    const database: StagingDatabase = {
      run: async (sql) => {
        statements.push(sql);
        if (renameFails && sql.startsWith('ALTER TABLE')) {
          throw new Error('response lost');
        }
        return [];
      },
      batch: async () => {},
      tableNames: async () => tables,
      cacheTables: async () => tables.filter((table) => /^cache_entries_\d{8}$/.test(table)),
    };
    return { database, statements };
  };

  it('drops every directory table except the live one before staging', async () => {
    const { database, statements } = fakeDatabase(['cache_entries_20250818', 'cache_entries_20250326'], false);
    await prepareStagingTable(database, '20250818', '{}');
    expect(statements.filter((sql) => sql.startsWith('DROP'))).toEqual([
      'DROP TABLE IF EXISTS cache_entries_20250326',
      `DROP TABLE IF EXISTS ${STAGING_TABLE}`,
    ]);
  });

  it('renames the staging table into place', async () => {
    const { database, statements } = fakeDatabase([], false);
    await publishStagingTable(database, '20261101');
    expect(statements.at(-1)).toBe(`ALTER TABLE ${STAGING_TABLE} RENAME TO cache_entries_20261101`);
  });

  it('accepts a rename whose response was lost once the table has moved', async () => {
    const { database } = fakeDatabase(['cache_entries_20261101'], true);
    await expect(publishStagingTable(database, '20261101')).resolves.toBeUndefined();
  });

  it.each([{ tables: [] }, { tables: [STAGING_TABLE] }, { tables: ['cache_entries_20261101', STAGING_TABLE] }])(
    'fails a rename that did not happen (tables: $tables)',
    async ({ tables }) => {
      const { database } = fakeDatabase(tables, true);
      await expect(publishStagingTable(database, '20261101')).rejects.toThrow('response lost');
    },
  );
});

describe('Archive', () => {
  // Ten 4-byte tiles whose bytes are their index, so a read shows which tile it hit.
  const tileData = new Uint8Array(40).map((_, i) => Math.floor(i / 4));
  const tile = (tileId: number, index: number, runLength = 1): FixtureEntry => ({
    tileId,
    offset: index * 4,
    length: 4,
    runLength,
  });
  const entries = async (archive: Archive) => {
    const runs: { tileId: number; offset: number; length: number; runLength: number }[][] = [];
    for await (const run of archive.tileEntries()) {
      runs.push(run.map(({ tileId, offset, length, runLength }) => ({ tileId, offset, length, runLength })));
    }
    return runs;
  };

  it('reads an archive whose tiles are all in the root directory', async () => {
    const root = [tile(0, 0), tile(1, 1, 3), tile(4, 0)];
    const archive = await Archive.open(memoryReader(await buildArchive({ root, tileData, metadata: { name: 'x' } })));
    expect(archive.metadata).toEqual({ name: 'x' });
    expect(archive.header.numTileEntries).toBe(3);
    expect(await entries(archive)).toEqual([[tile(0, 0), tile(1, 1, 3), tile(4, 0)]]);
    expect([...new Uint8Array(await archive.readTileData(4, 4))]).toEqual([1, 1, 1, 1]);
  });

  it('follows leaf directories in order', async () => {
    const leaves = [
      [tile(0, 0), tile(1, 1)],
      [tile(10, 2, 5), tile(15, 3)],
    ];
    const archive = await Archive.open(
      memoryReader(
        await buildArchive({
          root: [
            { tileId: 0, leaf: 0 },
            { tileId: 10, leaf: 1 },
          ],
          leaves,
          tileData,
        }),
      ),
    );
    expect(await entries(archive)).toEqual(leaves);
    expect([...new Uint8Array(await archive.readTileData(12, 4))]).toEqual([3, 3, 3, 3]);
  });

  it('keeps tile runs and leaf directories in tile order when a directory mixes them', async () => {
    const leaves = [
      [tile(1, 1), tile(2, 2)],
      [tile(20, 6), tile(21, 7, 2)],
    ];
    const root = [tile(0, 0), { tileId: 1, leaf: 0 }, tile(5, 3), tile(6, 4, 4), { tileId: 20, leaf: 1 }, tile(30, 0)];
    const archive = await Archive.open(memoryReader(await buildArchive({ root, leaves, tileData })));
    expect(await entries(archive)).toEqual([
      [tile(0, 0)],
      [tile(1, 1), tile(2, 2)],
      [tile(5, 3), tile(6, 4, 4)],
      [tile(20, 6), tile(21, 7, 2)],
      [tile(30, 0)],
    ]);
  });

  it('rejects a truncated archive', async () => {
    const bytes = await buildArchive({ root: [{ tileId: 0, leaf: 0 }], leaves: [[tile(0, 0)]], tileData });
    await expect(Archive.open(memoryReader(bytes.slice(0, 100)))).rejects.toThrow(/past the end/);
    const leafOffset = Number(new DataView(bytes.buffer).getBigUint64(40, true));
    const archive = await Archive.open(memoryReader(bytes.slice(0, leafOffset + 2)));
    await expect(entries(archive)).rejects.toThrow(/past the end/);
  });

  it('rejects archives the worker cannot serve as they are', async () => {
    const root = [tile(0, 0)];
    const brotli = await buildArchive({ root, tileData, tileCompression: Compression.Brotli });
    await expect(Archive.open(memoryReader(brotli))).rejects.toThrow(/gzip MVT/);
    const unclustered = await buildArchive({ root, tileData, clustered: false });
    await expect(Archive.open(memoryReader(unclustered))).rejects.toThrow(/clustered/);
    await expect(Archive.open(memoryReader(new Uint8Array(127)))).rejects.toThrow(/Not a PMTiles archive/);
  });
});

describe('inventoryProblems', () => {
  const inventory = {
    objects: {
      'chunk_0.pmtiles': 100,
      'chunk_1.pmtiles': 200,
      'chunk_2.pmtiles': 300,
      'styles/light.json': 10,
      'styles/dark.json': 20,
    },
  };

  it('accepts a complete deployment, extra objects included', () => {
    const listing = new Map([...Object.entries(inventory.objects), ['inventory.json', 5]]);
    expect(inventoryProblems(inventory, listing)).toEqual([]);
  });

  it('reports a partly purged deployment that still has chunk 0 and the light style', () => {
    const listing = new Map([
      ['chunk_0.pmtiles', 100],
      ['chunk_1.pmtiles', 150],
      ['styles/light.json', 10],
    ]);
    expect(inventoryProblems(inventory, listing)).toEqual([
      'chunk_1.pmtiles is 150 bytes, published 200',
      'chunk_2.pmtiles is missing',
      'styles/dark.json is missing',
    ]);
  });
});
