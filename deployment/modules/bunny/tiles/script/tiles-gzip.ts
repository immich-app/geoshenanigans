// Middleware for the tiles pull zone (gzip_mode = "middleware"). Storage sends the stored gzip tiles without
// Content-Encoding, and adding it with an edge rule makes some PoPs serve corrupt or empty bodies (Bunny ticket open).
// Setting it on the origin response here works at every PoP. onOriginResponse runs on cache misses only, so hits never
// reach the script.
import * as BunnySDK from 'npm:@bunny.net/edgescript-sdk@0.13.0';

BunnySDK.net.http.servePullZone({ url: 'https://storage.bunnycdn.com' }).onOriginResponse((ctx) => {
  const res = ctx.response;
  if (res.status !== 200 || !new URL(ctx.request.url).pathname.endsWith('.mvt')) {
    return Promise.resolve(res);
  }
  const headers = new Headers(res.headers);
  headers.set('Content-Encoding', 'gzip');
  headers.set('Content-Type', 'application/x-protobuf');
  return Promise.resolve(new Response(res.body, { status: res.status, headers }));
});
