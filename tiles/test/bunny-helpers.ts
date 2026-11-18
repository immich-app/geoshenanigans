import { Deps } from '../src/bunny-logs/deps';
import { O11yConfig } from '../src/o11y';

export const identity = {
  project: 'geoshenanigans',
  cluster: 'tiles',
  env: 'prod',
  provider: 'cloudflare',
  region: 'world',
};
export const o11y: O11yConfig = { url: 'https://o11y.test', token: 'secret', identity };

export type Call = { url: string; method: string; headers: Headers; body: string };

/**
 * Replaces fetch: o11y POSTs are recorded and answered 204, anything else is recorded and answered by `route` (a 599
 * when it returns nothing).
 */
export function mockFetch(route: (call: Call) => Response | Promise<Response> | undefined = () => undefined) {
  const calls: Call[] = [];
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
    const request = new Request(input, init);
    const call = {
      url: request.url,
      method: request.method,
      headers: request.headers,
      body: request.body ? await request.text() : '',
    };
    calls.push(call);
    if (call.url.startsWith('https://o11y.test/')) {
      return new Response(null, { status: 204 });
    }
    return (await route(call)) ?? new Response('unexpected', { status: 599 });
  });
  return {
    calls,
    o11yCalls: (path?: string) =>
      calls.filter(({ url }) => url.startsWith('https://o11y.test/') && (!path || url.includes(path))),
  };
}

export type ParsedSample = { name: string; labels: { [key: string]: string }; value: number; timestamp: number };

/** Prometheus text lines back into samples. */
export function parseExposition(body: string): ParsedSample[] {
  expect(body.endsWith('\n')).toBe(true);
  return body
    .split('\n')
    .filter((line) => line !== '')
    .map((line) => {
      const match = /^([a-z_]+)\{(.*)\} (\S+) (\d+)$/.exec(line);
      expect(match, line).not.toBeNull();
      const labels: { [key: string]: string } = {};
      for (const [, key, value] of match![2].matchAll(/(\w+)="((?:[^"\\]|\\.)*)"/g)) {
        labels[key] = value;
      }
      return { name: match![1], labels, value: Number(match![3]), timestamp: Number(match![4]) };
    });
}

/** The samples pushed to /insert/0/prometheus, without the identity labels (checked to be on each). */
export function pushedSamples(calls: Call[]): ParsedSample[] {
  return calls
    .filter(({ url }) => url === 'https://o11y.test/insert/0/prometheus/api/v1/import/prometheus')
    .flatMap(({ body }) => parseExposition(body))
    .map((sample) => {
      const labels = { ...sample.labels };
      for (const [key, value] of Object.entries(identity)) {
        expect(labels[key], `${sample.name} ${key}`).toBe(value);
        delete labels[key];
      }
      return { ...sample, labels };
    });
}

/** The log lines sent to VictoriaLogs. */
export function pushedLogs(calls: Call[]): { [key: string]: unknown }[] {
  return calls
    .filter(({ url }) => url.startsWith('https://o11y.test/insert/jsonline'))
    .flatMap(({ body }) =>
      body
        .split('\n')
        .filter((line) => line !== '')
        .map((line) => JSON.parse(line)),
    );
}

/** A clock that only moves when the code under test sleeps. */
export function testDeps(
  start: number,
  random = () => 0.5,
): Deps & { sleeps: number[]; advance: (ms: number) => void } {
  let now = start;
  const sleeps: number[] = [];
  return {
    now: () => now,
    sleep: async (ms) => {
      sleeps.push(ms);
      now += ms;
    },
    random,
    sleeps,
    advance: (ms) => {
      now += ms;
    },
  };
}

export const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });
