import { afterEach, describe, expect, it, vi } from 'vitest';
import { BunnyStorage } from '../../src/bunny/storage';
import { FakeStorage } from './fake-storage';

const resolver = vi.hoisted(() => ({ failures: 0, lookups: 0 }));
vi.mock('node:dns/promises', () => ({
  lookup: async () => {
    resolver.lookups++;
    if (resolver.failures > 0) {
      resolver.failures--;
      throw Object.assign(new Error('getaddrinfo EAI_AGAIN storage.example.test'), { code: 'EAI_AGAIN' });
    }
    return [{ address: '127.0.0.1', family: 4 }];
  },
}));

const fast = { baseDelayMs: 1, maxDelayMs: 5, settleMs: 5, maxAttempts: 4, maxRateLimitWaitMs: 1_000 };
let cleanup: (() => Promise<void>)[] = [];

afterEach(async () => {
  for (const step of cleanup.reverse()) {
    await step();
  }
  cleanup = [];
});

describe('BunnyStorage address lookup', () => {
  it('retries a lookup that failed instead of keeping the failure', async () => {
    const fake = await FakeStorage.start();
    const storage = new BunnyStorage({
      zone: fake.zone,
      password: fake.password,
      host: `http://storage.example.test:${fake.port}`,
      retry: fast,
    });
    cleanup.push(
      () => fake.close(),
      () => storage.close(),
    );
    // Both lookups of the first resolve (IPv4, then any family) fail once.
    resolver.failures = 2;
    expect(await storage.describe('a.mvt')).toBeNull();
    expect(await storage.describe('b.mvt')).toBeNull();
    expect(resolver.lookups).toBeGreaterThan(2);
    expect(fake.requests.DESCRIBE).toBe(2);
  });
});
