import { createExecutionContext, env, SELF, waitOnExecutionContext } from 'cloudflare:test';
import worker, { parseUrl } from '../src';

// For now, you'll need to do something like this to get a correctly-typed
// `Request` to pass to `worker.fetch()`.
const IncomingRequest = Request<unknown, IncomingRequestCfProperties>;

describe('integration tests', () => {
  beforeAll(async () => {
    // await env.BUCKET.put('v1.pmtiles', fs.readFileSync('pmtiles'));
    // (await env.BUCKET.list()).objects.forEach((key) => {
    //   console.log(key);
    // });
  });
  it('responds with Hello World! (integration style)', async () => {
    const response = await SELF.fetch('https://example.com');
    expect(await response.text()).toMatchInlineSnapshot(`"Hello World!"`);
  });
});

describe('Hello World worker', () => {
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
    let result = parseUrl(new Request(url));
    expect(result).toStrictEqual({
      requestType: 'json',
      version,
      url: new URL(url),
    });
  });

  it.each`
    url                                  | version | z      | x      | y
    ${'http://example.com/v1/0/0/0.mvt'} | ${'1'}  | ${'0'} | ${'0'} | ${'0'}
    ${'http://example.com/v2/1/2/3.mvt'} | ${'2'}  | ${'1'} | ${'2'} | ${'3'}
  `('tile endpoint $url returns tile $z/$x/$y with version $version', ({ url, version, z, x, y }) => {
    let result = parseUrl(new Request(url));
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
  `('url $url returns undefined request type', ({ url, version, z, x, y }) => {
    let result = parseUrl(new Request(url));
    expect(result).toStrictEqual({
      requestType: undefined,
      url: new URL(url),
    });
  });
});
