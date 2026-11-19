import { createServer, IncomingMessage, Server } from 'node:http';
import { afterEach, describe, expect, it } from 'vitest';
import { purgePullZone } from '../../src/bunny/purge';

let server: Server | undefined;

afterEach(async () => {
  server?.closeAllConnections();
  await new Promise((resolve) => (server ? server.close(resolve) : resolve(undefined)));
  server = undefined;
});

async function api(statuses: number[]) {
  const requests: IncomingMessage[] = [];
  server = createServer((request, response) => {
    requests.push(request);
    const status = statuses[Math.min(requests.length, statuses.length) - 1];
    response.writeHead(status, { 'content-type': 'application/json' });
    response.end(status >= 400 ? JSON.stringify({ Message: 'nope' }) : '');
  });
  await new Promise<void>((resolve) => server!.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as { port: number };
  return { requests, baseUrl: `http://127.0.0.1:${port}` };
}

describe('purgePullZone', () => {
  it('POSTs a full purge with the account key', async () => {
    const { requests, baseUrl } = await api([204]);
    const result = await purgePullZone('secret-key', 12345, { baseUrl });
    expect(result).toMatchObject({ status: 204, attempts: 1 });
    expect(requests).toHaveLength(1);
    expect(requests[0].method).toBe('POST');
    expect(requests[0].url).toBe('/pullzone/12345/purgeCache');
    expect(requests[0].headers.accesskey).toBe('secret-key');
  });

  it('retries 429 and 5xx', async () => {
    const { requests, baseUrl } = await api([429, 503, 200]);
    const result = await purgePullZone('k', '42', { baseUrl, baseDelayMs: 1 });
    expect(result).toMatchObject({ status: 200, attempts: 3 });
    expect(requests).toHaveLength(3);
  });

  it('takes a 2xx as done even when its body stalls, without retrying', async () => {
    let requests = 0;
    server = createServer((_request, response) => {
      requests++;
      // The status and part of the body arrive, the rest never does.
      response.writeHead(200, { 'content-type': 'application/json', 'content-length': '100' });
      response.write('{"Message":');
    });
    await new Promise<void>((resolve) => server!.listen(0, '127.0.0.1', resolve));
    const { port } = server.address() as { port: number };
    const result = await purgePullZone('k', 7, {
      baseUrl: `http://127.0.0.1:${port}`,
      attempts: 3,
      baseDelayMs: 1,
      timeoutMs: 1_000,
    });
    expect(result).toMatchObject({ status: 200, attempts: 1 });
    expect(requests).toBe(1);
  });

  it('fails at once on other errors without leaking the key', async () => {
    const { requests, baseUrl } = await api([401]);
    const error = await purgePullZone('secret-key', 1, { baseUrl, baseDelayMs: 1 }).then(
      () => new Error('resolved'),
      (e: Error) => e,
    );
    expect(error.message).toMatch(/HTTP 401/);
    expect(error.message).not.toContain('secret-key');
    expect(requests).toHaveLength(1);
  });

  it('gives up after its attempts on network errors', async () => {
    await expect(purgePullZone('k', 1, { baseUrl: 'http://127.0.0.1:1', attempts: 2, baseDelayMs: 1 })).rejects.toThrow(
      /after 2 attempts/,
    );
  });

  it('rejects ids that are not numbers', async () => {
    await expect(purgePullZone('k', '1/../2')).rejects.toThrow(/numeric/);
  });
});
