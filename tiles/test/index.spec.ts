import { createExecutionContext, env, SELF, waitOnExecutionContext } from 'cloudflare:test';
import { inject } from 'vitest';
import worker, { parseUrl } from '../src';
import { fromRadix64, toRadix64 } from '../src/pmtiles/utils';

// For now, you'll need to do something like this to get a correctly-typed
// `Request` to pass to `worker.fetch()`.
const IncomingRequest = Request<unknown, IncomingRequestCfProperties>;

describe('integration tests', () => {
  describe('success - zoom1', () => {
    beforeAll(async () => {
      const buckets = [env.BUCKET_WEUR, env.BUCKET_WNAM, env.BUCKET_EEUR, env.BUCKET_APAC, env.BUCKET_ENAM];
      const files = ['tiles.pmtiles', 'styles/dark.json', 'styles/light.json'];
      for (const bucket of buckets) {
        for (const fileName of files) {
          const file = await fetch(`http://localhost:${inject('port')}/${fileName}`);
          if (!file.body) {
            throw new Error('File body is undefined');
          }
          const body = file.body as ReadableStream;
          await bucket.put(`${env.DEPLOYMENT_KEY}/${fileName}`, body);
          console.log('File uploaded');
        }
      }
    }, 30000);
    it('responds with correct json file', async () => {
      const response = await SELF.fetch('https://example.com/v1');
      await expect(JSON.stringify(await response.json(), null, 2)).toMatchFileSnapshot('./__snapshots__/v1.json');
    });
    it('responds with correct style json amended with current URL', async () => {
      const response = await SELF.fetch('https://example.com/v1/style/dark');
      await expect(JSON.stringify(await response.json(), null, 2)).toMatchFileSnapshot(
        './__snapshots__/styles/dark.json',
      );
    });
    // TODO: Come back to these and write up a way to populate local D1 on test setup
    it.skip('responds with correct tile', async () => {
      const response = await SELF.fetch('https://example.com/v1/0/0/0.mvt');
      expect(response.status).toBe(200);
      await expect(Buffer.from(await response.arrayBuffer()).toString()).toMatchFileSnapshot(
        './__snapshots__/v1-0-0-0.mvt',
      );
    });
    it.skip('responds with correct tile 2', async () => {
      const response = await SELF.fetch('https://example.com/v1/1/0/1.mvt');
      expect(response.status).toBe(200);
      await expect(Buffer.from(await response.arrayBuffer()).toString()).toMatchFileSnapshot(
        './__snapshots__/v1-1-0-1.mvt',
      );
    });
    it('responds with error when tile out of bounds', async () => {
      const response = await SELF.fetch('https://example.com/v1/0/0/1.mvt');
      expect(response.status).toBe(500);
    });
  });
});

describe.skip('Hello World worker', () => {
  it('responds with Hello World! (unit style)', async () => {
    const request = new IncomingRequest('http://example.com');
    // Create an empty context to pass to `worker.fetch()`.
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    // Wait for all `Promise`s passed to `ctx.waitUntil()` to settle before running test assertions
    await waitOnExecutionContext(ctx);
    expect(await response.text()).toMatchInlineSnapshot(`"Hello World!"`);
  });
});

describe('parseUrl', () => {
  it.each`
    url                             | version
    ${'http://example.com/v1'}      | ${'1'}
    ${'http://example.com/v2'}      | ${'2'}
    ${'http://example.com/v1.json'} | ${'1'}
    ${'http://example.com/v2.json'} | ${'2'}
  `('json endpoint $url returns version $version', ({ url, version }) => {
    const result = parseUrl(new Request(url));
    expect(result).toStrictEqual({
      requestType: 'json',
      version,
      url: new URL(url),
    });
  });

  it.each`
    url                                         | style      | version
    ${'http://example.com/v1/style/dark'}       | ${'dark'}  | ${'1'}
    ${'http://example.com/v2/style/light'}      | ${'light'} | ${'2'}
    ${'http://example.com/v2/style/dark.json'}  | ${'dark'}  | ${'2'}
    ${'http://example.com/v1/style/light.json'} | ${'light'} | ${'1'}
  `('style endpoint $url returns style $style', ({ url, style, version }) => {
    const result = parseUrl(new Request(url));
    expect(result).toStrictEqual({
      requestType: 'style',
      style,
      url: new URL(url),
      version,
    });
  });

  it.each`
    url                                  | version | z      | x      | y
    ${'http://example.com/v1/0/0/0.mvt'} | ${'1'}  | ${'0'} | ${'0'} | ${'0'}
    ${'http://example.com/v2/1/2/3.mvt'} | ${'2'}  | ${'1'} | ${'2'} | ${'3'}
  `('tile endpoint $url returns tile $z/$x/$y with version $version', ({ url, version, z, x, y }) => {
    const result = parseUrl(new Request(url));
    expect(result).toStrictEqual({
      requestType: 'tile',
      version,
      z,
      x,
      y,
      url: new URL(url),
    });
  });

  it.each`
    url
    ${'http://example.com/v1.jason'}
    ${'http://example.com/v2/1/2/3'}
    ${'http://example.com/v2/1/2/3.mvt/extra'}
    ${'http://example.com/v1/1/2.mvt'}
  `('url $url returns undefined request type', ({ url }) => {
    const result = parseUrl(new Request(url));
    expect(result).toStrictEqual({
      requestType: undefined,
      url: new URL(url),
    });
  });
});

describe('radix64', () => {
  it.each`
    number
    ${1}
    ${0}
    ${-1}
    ${100}
    ${1769420107}
    ${663668044}
    ${-1105752063}
  `('converts $number to radix64 and back correctly', ({ number }) => {
    const radix64 = toRadix64(number);
    expect(fromRadix64(radix64)).toEqual(number);
  });
});
