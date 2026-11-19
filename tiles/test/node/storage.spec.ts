import { createServer } from 'node:net';
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

  it('rides out more 429s in a row than maxAttempts with plain PUTs', async () => {
    const { fake, storage } = await setup(
      { fault: failFirst('rate-limit', 12) },
      { retry: { ...fast, maxAttempts: 3, maxDelayMs: 2 } },
    );
    await storage.put('a.mvt', bytes('a'), sha256Hex(bytes('a')));
    expect(fake.requests).toEqual({ PUT: 13 });
    expect(storage.counters).toMatchObject({ rateLimited: 12, describes: 0 });
  });

  it('still ends a 429 storm at the wait cap when given a zero back-off', async () => {
    const { storage } = await setup(
      { fault: failFirst('rate-limit', 1_000_000_000) },
      { retry: { ...fast, baseDelayMs: 0, maxRateLimitWaitMs: 200 } },
    );
    const started = performance.now();
    await expect(storage.put('a.mvt', bytes('a'), sha256Hex(bytes('a')))).rejects.toThrow(/still rate limited/);
    expect(performance.now() - started).toBeLessThan(5_000);
  });

  // Whether a second DESCRIBE follows the first depends on how soon the first came back (the two tests after this one
  // pin that down), so these only require that every failed PUT was checked before it was sent again.
  it.each<Fault>(['server-error', 'reset', 'hang'])(
    'after %s (not stored) DESCRIBEs, finds nothing and PUTs again',
    async (fault) => {
      const seen: string[] = [];
      const { fake, storage } = await setup({ fault: failFirst(fault), onRequest: ({ method }) => seen.push(method) });
      await storage.put('a.mvt', bytes('a'), sha256Hex(bytes('a')));
      expect(seen.join(' ')).toMatch(/^PUT( DESCRIBE)+ PUT$/);
      expect(fake.requests.PUT).toBe(2);
      expect(storage.counters.ambiguousResent).toBe(1);
      expect(fake.checksum('a.mvt')).toBe(sha256Hex(bytes('a')));
      expect(fake.identicalReputCount).toBe(0);
    },
  );

  it('asks again once the commit window has passed when the first DESCRIBE finds nothing inside it', async () => {
    const seen: { method: string; at: number }[] = [];
    const { storage } = await setup(
      { fault: failFirst('server-error'), onRequest: ({ method }) => seen.push({ method, at: performance.now() }) },
      { retry: { ...fast, settleMs: 250 } },
    );
    await storage.put('a.mvt', bytes('a'), sha256Hex(bytes('a')));
    // The first DESCRIBE goes out 250 ms after the failure and comes back long before the 1.25 s window ends.
    expect(seen.map(({ method }) => method)).toEqual(['PUT', 'DESCRIBE', 'DESCRIBE', 'PUT']);
    expect(seen[2].at - seen[0].at).toBeGreaterThanOrEqual(1_200);
  });

  it('PUTs again without a second DESCRIBE when the first came back after the commit window', async () => {
    const seen: string[] = [];
    const { storage } = await setup({
      fault: failFirst('server-error'),
      onRequest: ({ method }) => seen.push(method),
      // Held well past the 25 ms window (5 x settleMs).
      hold: ({ method, attempt }) =>
        method === 'DESCRIBE' && attempt === 1 ? new Promise((resolve) => setTimeout(resolve, 150)) : undefined,
    });
    await storage.put('a.mvt', bytes('a'), sha256Hex(bytes('a')));
    expect(seen).toEqual(['PUT', 'DESCRIBE', 'PUT']);
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

  it.each<Fault>(['gateway-late-store', 'reset-late-store'])(
    'after %s waits out the commit window before PUTting again, so the late commit is not sent twice',
    async (fault) => {
      // Answered (or dropped) at once, stored 1 s later: after the 300 ms settle DESCRIBE finds nothing, and the
      // second DESCRIBE at 1.5 s (5 x settle) finds the late commit.
      const { fake, storage } = await setup(
        { fault: failFirst(fault), lateStoreMs: 1_000 },
        { retry: { ...fast, settleMs: 300 } },
      );
      await storage.put('a.mvt', bytes('a'), sha256Hex(bytes('a')));
      await new Promise((resolve) => setTimeout(resolve, 1_200));
      expect(fake.requests).toEqual({ PUT: 1, DESCRIBE: 2 });
      expect(storage.counters.ambiguousStored).toBe(1);
      expect(fake.identicalReputs).toEqual([]);
    },
  );

  it('GETs a file whose DESCRIBE still shows the version it overwrote instead of PUTting the same bytes again', async () => {
    // Measured on Bunny: DESCRIBE can keep showing the previous version after two quick writes, while GET does not.
    const { fake, storage } = await setup({
      fault: failFirst('stored-reset'),
      staleOverwrite: { withinMs: 60_000, forMs: Infinity },
    });
    fake.seed('a.mvt', bytes('old'), ['UK', 'NY', 'SYD']);
    await storage.put('a.mvt', bytes('new'), sha256Hex(bytes('new')));
    expect((await storage.describe('a.mvt'))?.checksum).toBe(sha256Hex(bytes('old')));
    expect(fake.requests).toEqual({ PUT: 1, DESCRIBE: 2, GET: 1 });
    expect(storage.counters).toMatchObject({ ambiguousStored: 1, staleDescribes: 1, ambiguousResent: 0 });
    expect(fake.identicalReputs).toEqual([]);
  });

  it('holds() checks the bytes, fetching them when DESCRIBE shows another checksum', async () => {
    const { fake, storage } = await setup({ staleOverwrite: { withinMs: 60_000, forMs: Infinity } });
    fake.seed('a.mvt', bytes('old'));
    await storage.put('a.mvt', bytes('new'), sha256Hex(bytes('new')));
    expect(await storage.holds('a.mvt', sha256Hex(bytes('new')).toLowerCase())).toBe(true);
    expect(await storage.holds('a.mvt', sha256Hex(bytes('other')))).toBe(false);
    expect(await storage.holds('none.mvt', sha256Hex(bytes('new')))).toBe(false);
    expect(fake.requests).toEqual({ PUT: 1, DESCRIBE: 3, GET: 2 });
  });

  it('PUTs again after an ambiguous failure when the stored copy holds other bytes', async () => {
    const { fake, storage } = await setup({ fault: failFirst('server-error') });
    fake.seed('a.mvt', bytes('old'));
    await storage.put('a.mvt', bytes('new'), sha256Hex(bytes('new')));
    expect(fake.checksum('a.mvt')).toBe(sha256Hex(bytes('new')));
    expect(fake.identicalReputCount).toBe(0);
  });

  it('gives up after maxAttempts ambiguous failures, checking each time', async () => {
    const seen: string[] = [];
    const { fake, storage } = await setup({
      fault: failFirst('server-error', 100),
      onRequest: ({ method }) => seen.push(method),
    });
    await expect(storage.put('a.mvt', bytes('a'), sha256Hex(bytes('a')))).rejects.toThrow(
      /PUT a.mvt failed 4 times, last: HTTP 500/,
    );
    expect(seen.join(' ')).toMatch(/^(PUT( DESCRIBE)+ ){3}PUT( DESCRIBE)+$/);
    expect(fake.requests.PUT).toBe(4);
    expect(fake.files.size).toBe(0);
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

  it('moves to a new address set: new requests go to the new addresses, requests in flight on the old ones finish', async () => {
    let release!: () => void;
    const held = new Promise<void>((resolve) => (release = resolve));
    let arrived!: () => void;
    const arrival = new Promise<void>((resolve) => (arrived = resolve));
    const fake = await FakeStorage.start({
      addresses: ['127.0.0.1', '127.0.0.2', '127.0.0.3'],
      hold: ({ path }) => {
        if (path !== 'held.mvt') {
          return undefined;
        }
        arrived();
        return held;
      },
    });
    fakes.push(fake);
    const storage = new BunnyStorage({
      zone: fake.zone,
      password: fake.password,
      host: `http://storage.example.test:${fake.port}`,
      addresses: ['127.0.0.1'],
      concurrency: 4,
      retry: fast,
    });
    clients.push(storage);
    const put = (name: string) => storage.put(name, bytes(name), sha256Hex(bytes(name)));
    const inFlight = put('held.mvt');
    let moved: Promise<void>;
    try {
      await arrival;
      expect(fake.requestsByAddress.get('127.0.0.1')).toBe(1);

      moved = storage.setAddresses(['127.0.0.2', '127.0.0.3']);
      await Promise.all(Array.from({ length: 20 }, (_, i) => put(`f${i}.mvt`)));
      expect(fake.requestsByAddress.get('127.0.0.1')).toBe(1);
      expect(fake.requestsByAddress.get('127.0.0.2')! + fake.requestsByAddress.get('127.0.0.3')!).toBe(20);
    } finally {
      release();
    }
    await inFlight;
    await moved;
    expect(fake.files.size).toBe(21);
    expect(fake.requests).toEqual({ PUT: 21 });
  });

  // undici's headers timeout ticks at about 1 s, so the 150 ms timeout fires within 0.15-1.5 s, before the late commit.
  it('treats a commit that lands after a timeout as stored when it settles first', async () => {
    const { fake, storage } = await setup(
      { fault: failFirst('late-store'), lateStoreMs: 2_000 },
      { timeoutMs: 150, retry: { ...fast, settleMs: 500 } },
    );
    await storage.put('a.mvt', bytes('a'), sha256Hex(bytes('a')));
    expect(fake.requests).toEqual({ PUT: 1, DESCRIBE: 1 });
    expect(storage.counters.ambiguousStored).toBe(1);
    expect(fake.identicalReputs).toEqual([]);
  });

  it('without a settle wait, a late commit turns the retry into an identical re-PUT (the hazard settleMs guards)', async () => {
    const { fake, storage } = await setup(
      { fault: failFirst('late-store'), lateStoreMs: 2_000 },
      { timeoutMs: 150, retry: { ...fast, settleMs: 0 } },
    );
    await storage.put('a.mvt', bytes('a'), sha256Hex(bytes('a')));
    await new Promise((resolve) => setTimeout(resolve, 2_500));
    expect(fake.identicalReputs).toEqual(['a.mvt']);
  });

  describe('with one bad storage address', () => {
    const many = (count: number) => Array.from({ length: count }, (_, i) => [`f${i}.mvt`, bytes(`file ${i}`)] as const);

    async function badAddress(fakeOptions: FakeStorageOptions, listen: string[], pinned: string[]) {
      const fake = await FakeStorage.start({ addresses: listen, latencyMs: 20, ...fakeOptions });
      fakes.push(fake);
      const storage = new BunnyStorage({
        zone: fake.zone,
        password: fake.password,
        host: `http://127.0.0.1:${fake.port}`,
        addresses: pinned,
        concurrency: 30,
        timeoutMs: 2_000,
        retry: { ...fast, maxAttempts: 4 },
      });
      clients.push(storage);
      const results = await Promise.allSettled(
        many(600).map(([path, body]) => storage.put(path, body, sha256Hex(body))),
      );
      const failed = results.filter((result) => result.status === 'rejected');
      return { fake, storage, failed };
    }

    it('stops sending to an address that refuses connections, and never DESCRIBEs for a request it did not send', async () => {
      const { fake, storage, failed } = await badAddress(
        {},
        ['127.0.0.1', '127.0.0.2'],
        ['127.0.0.1', '127.0.0.2', '127.0.0.3'],
      );
      expect(failed).toEqual([]);
      expect(fake.files.size).toBe(600);
      expect(storage.counters.networkErrors / storage.counters.requests).toBeLessThan(0.1);
      expect(fake.requests.DESCRIBE ?? 0).toBe(0);
      expect(fake.identicalReputs).toEqual([]);
    });

    it.each<Fault>(['server-error', 'rate-limit'])(
      'stops sending to an address that answers %s at once',
      async (fault) => {
        const { fake, failed } = await badAddress(
          { fault: ({ address }) => (address === '127.0.0.3' ? fault : undefined) },
          ['127.0.0.1', '127.0.0.2', '127.0.0.3'],
          ['127.0.0.1', '127.0.0.2', '127.0.0.3'],
        );
        expect(failed).toEqual([]);
        expect(fake.files.size).toBe(600);
        const total = [...fake.requestsByAddress.values()].reduce((a, b) => a + b);
        expect(fake.requestsByAddress.get('127.0.0.3')! / total).toBeLessThan(0.15);
        expect(fake.identicalReputs).toEqual([]);
      },
    );

    it('DESCRIBEs after an ambiguous failure through another address', async () => {
      const seen: string[] = [];
      const fake = await FakeStorage.start({
        addresses: ['127.0.0.1', '127.0.0.2'],
        fault: failFirst('stored-server-error'),
        onRequest: ({ method, address }) => seen.push(`${method} ${address}`),
      });
      fakes.push(fake);
      const storage = new BunnyStorage({
        zone: fake.zone,
        password: fake.password,
        host: fake.url,
        addresses: ['127.0.0.1', '127.0.0.2'],
        retry: fast,
      });
      clients.push(storage);
      await storage.put('a.mvt', bytes('a'), sha256Hex(bytes('a')));
      expect(seen).toHaveLength(2);
      expect(seen[1].split(' ')[1]).not.toBe(seen[0].split(' ')[1]);
    });
  });

  it('stops backing off when its signal aborts', async () => {
    const controller = new AbortController();
    const { storage } = await setup(
      { fault: failFirst('rate-limit', 1_000) },
      {
        signal: controller.signal,
        retry: { ...fast, baseDelayMs: 5_000, maxDelayMs: 5_000, maxRateLimitWaitMs: 60_000 },
      },
    );
    const started = performance.now();
    setTimeout(() => controller.abort(), 50);
    const error = await storage.put('a.mvt', bytes('a'), sha256Hex(bytes('a'))).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(StorageError);
    expect((error as StorageError).stopped).toBe(true);
    expect(performance.now() - started).toBeLessThan(1_000);
  });
});

describe('FakeStorage', () => {
  it('closes the listeners it bound when a later bind fails', async () => {
    // The same address twice: the second bind of the port the first one took fails.
    const error = await FakeStorage.start({ addresses: ['127.0.0.1', '127.0.0.1'] }).then(
      (fake) => {
        fakes.push(fake);
        return new Error('started');
      },
      (e: unknown) => e,
    );
    expect(error).toMatchObject({ code: 'EADDRINUSE', port: expect.any(Number) });
    // Nothing listens on that port any more, so it binds again.
    const probe = createServer();
    await new Promise<void>((resolve, reject) => {
      probe.once('error', reject);
      probe.listen((error as { port: number }).port, '127.0.0.1', resolve);
    });
    await new Promise((resolve) => probe.close(resolve));
  });
});
