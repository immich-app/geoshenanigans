import { afterEach, describe, expect, it } from 'vitest';
import { BunnyStorage, BunnyStorageOptions, StorageError } from '../../src/bunny/storage';
import { FakeStorage, FakeStorageOptions, Fault, sha256Hex } from './fake-storage';

const fast = { baseDelayMs: 1, maxDelayMs: 5, settleMs: 5, maxAttempts: 4, maxRateLimitWaitMs: 2_000 };
const fakes: FakeStorage[] = [];
const clients: BunnyStorage[] = [];

async function setup(fakeOptions: FakeStorageOptions = {}, clientOptions: Partial<BunnyStorageOptions> = {}) {
  const fake = await FakeStorage.start(fakeOptions);
  fakes.push(fake);
  const storage = new BunnyStorage({
    zone: fake.zone,
    password: fake.password,
    host: fake.url,
    retry: fast,
    timeoutMs: 300,
    ...clientOptions,
  });
  clients.push(storage);
  return { fake, storage };
}

/** The first `times` PUTs of each path fail with `fault`. */
const failFirst =
  (fault: Fault, times = 1) =>
  ({ method, attempt }: { method: string; attempt: number }) =>
    method === 'PUT' && attempt <= times ? fault : undefined;

const bytes = (text: string) => new TextEncoder().encode(text);

afterEach(async () => {
  await Promise.all(clients.splice(0).map((client) => client.close()));
  await Promise.all(fakes.splice(0).map((fake) => fake.close()));
});

describe('BunnyStorage', () => {
  it('puts, describes, gets, lists and deletes', async () => {
    const { fake, storage } = await setup();
    const body = bytes('tile');
    await storage.put('v1/0/0/0.mvt', body, sha256Hex(body).toLowerCase());
    await storage.put('v1/1/0/1.mvt', bytes('other'), sha256Hex(bytes('other')));

    expect(fake.checksum('v1/0/0/0.mvt')).toBe(sha256Hex(body));
    const object = await storage.describe('v1/0/0/0.mvt');
    expect(object).toMatchObject({ checksum: sha256Hex(body), length: 4, replicatedZones: [] });
    expect(await storage.describe('v1/9/9/9.mvt')).toBeNull();
    expect(await storage.get('v1/0/0/0.mvt')).toEqual(body);
    expect(await storage.get('v1/9/9/9.mvt')).toBeNull();

    const root = await storage.list('v1');
    expect(root.map(({ name, isDirectory }) => [name, isDirectory])).toEqual([
      ['0', true],
      ['1', true],
    ]);
    const files = await storage.list('v1/1/0/');
    expect(files).toEqual([expect.objectContaining({ name: '1.mvt', isDirectory: false, length: 5 })]);

    await storage.delete('v1/0/0/0.mvt');
    await storage.delete('v1/0/0/0.mvt');
    expect(fake.files.has('v1/0/0/0.mvt')).toBe(false);
    await storage.delete('v1/');
    expect(fake.files.size).toBe(0);
    expect(fake.identicalReputCount).toBe(0);
  });

  it('parses ReplicatedZones', async () => {
    const { fake, storage } = await setup();
    fake.seed('a.mvt', bytes('a'), ['UK', 'SYD']);
    expect((await storage.describe('a.mvt'))?.replicatedZones).toEqual(['UK', 'SYD']);
  });

  it('retries 429 with a plain PUT: it was not stored', async () => {
    const { fake, storage } = await setup({ fault: failFirst('rate-limit', 3) });
    await storage.put('a.mvt', bytes('a'), sha256Hex(bytes('a')));
    expect(fake.requests).toEqual({ PUT: 4 });
    expect(storage.counters).toMatchObject({ rateLimited: 3, retries: 3, describes: 0 });
    expect(fake.identicalReputCount).toBe(0);
  });

  it.each<[Fault, number]>([
    ['server-error', 2],
    ['reset', 2],
    ['hang', 2],
  ])('after %s (not stored) DESCRIBEs, finds nothing and PUTs again', async (fault, puts) => {
    const { fake, storage } = await setup({ fault: failFirst(fault) });
    await storage.put('a.mvt', bytes('a'), sha256Hex(bytes('a')));
    expect(fake.requests.PUT).toBe(puts);
    expect(fake.requests.DESCRIBE).toBe(1);
    expect(storage.counters.ambiguousResent).toBe(1);
    expect(fake.checksum('a.mvt')).toBe(sha256Hex(bytes('a')));
    expect(fake.identicalReputCount).toBe(0);
  });

  it.each<Fault>(['stored-server-error', 'stored-request-timeout', 'stored-reset', 'stored-hang'])(
    'after %s (stored) DESCRIBEs and does not PUT again',
    async (fault) => {
      const { fake, storage } = await setup({ fault: failFirst(fault) });
      await storage.put('a.mvt', bytes('a'), sha256Hex(bytes('a')));
      expect(fake.requests.PUT).toBe(1);
      expect(fake.requests.DESCRIBE).toBe(1);
      expect(storage.counters.ambiguousStored).toBe(1);
      expect(fake.identicalReputCount).toBe(0);
    },
  );

  it('PUTs again after an ambiguous failure when the stored copy holds other bytes', async () => {
    const { fake, storage } = await setup({ fault: failFirst('server-error') });
    fake.seed('a.mvt', bytes('old'));
    await storage.put('a.mvt', bytes('new'), sha256Hex(bytes('new')));
    expect(fake.checksum('a.mvt')).toBe(sha256Hex(bytes('new')));
    expect(fake.identicalReputCount).toBe(0);
  });

  it('gives up after maxAttempts ambiguous failures, checking each time', async () => {
    const { fake, storage } = await setup({ fault: failFirst('server-error', 100) });
    await expect(storage.put('a.mvt', bytes('a'), sha256Hex(bytes('a')))).rejects.toThrow(
      /PUT a.mvt failed 4 times, last: HTTP 500/,
    );
    expect(fake.requests).toEqual({ PUT: 4, DESCRIBE: 4 });
  });

  it('gives up on 429 after the rate-limit wait cap', async () => {
    const { storage } = await setup(
      { fault: failFirst('rate-limit', 1_000) },
      { retry: { ...fast, maxRateLimitWaitMs: 30 } },
    );
    await expect(storage.put('a.mvt', bytes('a'), sha256Hex(bytes('a')))).rejects.toThrow(/still rate limited/);
  });

  it('fails a checksum mismatch at once', async () => {
    const { fake, storage } = await setup();
    await expect(storage.put('a.mvt', bytes('a'), sha256Hex(bytes('b')))).rejects.toThrow(/HTTP 400/);
    expect(fake.requests).toEqual({ PUT: 1 });
  });

  it('marks a wrong password fatal and never sends it in errors', async () => {
    const fake = await FakeStorage.start();
    fakes.push(fake);
    const storage = new BunnyStorage({ zone: fake.zone, password: 'wrong-secret', host: fake.url, retry: fast });
    clients.push(storage);
    const error = await storage.put('a.mvt', bytes('a'), sha256Hex(bytes('a'))).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(StorageError);
    expect((error as StorageError).fatal).toBe(true);
    expect((error as StorageError).message).not.toContain('wrong-secret');
  });

  it('retries DESCRIBE through 429s and errors', async () => {
    const faults: Fault[] = ['rate-limit', 'server-error', 'reset'];
    const { fake, storage } = await setup({
      fault: ({ method, attempt }) => (method === 'DESCRIBE' ? faults[attempt - 1] : undefined),
    });
    fake.seed('a.mvt', bytes('a'));
    expect((await storage.describe('a.mvt'))?.checksum).toBe(sha256Hex(bytes('a')));
    expect(fake.requests.DESCRIBE).toBe(4);
  });

  it('spreads connections over every pinned address and sends the storage host name', async () => {
    const fake = await FakeStorage.start({ addresses: ['127.0.0.1', '127.0.0.2'], latencyMs: 20 });
    fakes.push(fake);
    const storage = new BunnyStorage({
      zone: fake.zone,
      password: fake.password,
      host: `http://storage.example.test:${fake.port}`,
      addresses: ['127.0.0.1', '127.0.0.2'],
      concurrency: 8,
      retry: fast,
    });
    clients.push(storage);
    await Promise.all(
      Array.from({ length: 32 }, (_, i) => storage.put(`f${i}.mvt`, bytes(`${i}`), sha256Hex(bytes(`${i}`)))),
    );
    expect([...fake.connectionsByAddress.keys()].sort()).toEqual(['127.0.0.1', '127.0.0.2']);
    expect([...fake.connectionsByAddress.values()].reduce((a, b) => a + b)).toBeLessThanOrEqual(8);
    expect(fake.maxInFlight).toBeLessThanOrEqual(8);
    expect([...fake.hostHeaders]).toEqual([`storage.example.test:${fake.port}`]);
  });
});
