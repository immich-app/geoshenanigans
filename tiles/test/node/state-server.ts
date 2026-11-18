import { createHash } from 'node:crypto';
import { AddressInfo, createServer, Socket } from 'node:net';

interface Request {
  method: string;
  path: string;
  headers: Record<string, string>;
  body: Buffer;
}

/**
 * A stand-in for a Bunny storage zone, enough for the state zone: PUT with Checksum validation, GET, DESCRIBE.
 * Node's HTTP parser rejects the DESCRIBE method, so this speaks one HTTP/1.1 request per connection over raw
 * sockets. `failNext` makes the next requests answer with a status (e.g. 503) without being handled.
 */
export async function startStateServer(zone: string, password: string) {
  const objects = new Map<string, Buffer>();
  const requests: string[] = [];
  const failNext: number[] = [];
  const sockets = new Set<Socket>();

  const handle = (request: Request): { status: number; body?: Buffer | string; type?: string } => {
    requests.push(`${request.method} ${request.path}`);
    const failure = failNext.shift();
    if (failure) {
      return { status: failure };
    }
    if (request.headers.accesskey !== password) {
      return { status: 401, body: 'Unauthorized' };
    }
    const prefix = `/${zone}/`;
    if (!request.path.startsWith(prefix)) {
      return { status: 404 };
    }
    const path = decodeURIComponent(request.path.slice(prefix.length));
    const sha256 = (bytes: Buffer) => createHash('sha256').update(bytes).digest('hex').toUpperCase();
    const object = objects.get(path);
    switch (request.method) {
      case 'PUT':
        if (request.headers.checksum && request.headers.checksum !== sha256(request.body)) {
          return { status: 400, body: 'Checksum mismatch' };
        }
        objects.set(path, request.body);
        return { status: 201, body: '{"HttpCode":201,"Message":"File uploaded."}' };
      case 'GET':
        return object ? { status: 200, body: object, type: 'application/octet-stream' } : { status: 404 };
      case 'DESCRIBE':
        return object
          ? {
              status: 200,
              type: 'application/json',
              body: JSON.stringify({
                Length: object.length,
                Checksum: sha256(object),
                ReplicatedZones: '',
                IsDirectory: false,
                ObjectName: path.split('/').pop(),
              }),
            }
          : { status: 404 };
      default:
        return { status: 405 };
    }
  };

  const server = createServer((socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    socket.on('error', () => {});
    let buffered = Buffer.alloc(0);
    socket.on('data', (chunk: Buffer) => {
      buffered = Buffer.concat([buffered, chunk]);
      const headEnd = buffered.indexOf('\r\n\r\n');
      if (headEnd < 0) {
        return;
      }
      const [requestLine, ...headerLines] = buffered.subarray(0, headEnd).toString('latin1').split('\r\n');
      const headers = Object.fromEntries(
        headerLines.map((line) => {
          const colon = line.indexOf(':');
          return [line.slice(0, colon).trim().toLowerCase(), line.slice(colon + 1).trim()];
        }),
      );
      const length = Number(headers['content-length'] ?? 0);
      if (buffered.length < headEnd + 4 + length) {
        return;
      }
      const [method, target] = requestLine.split(' ');
      const response = handle({
        method,
        path: new URL(target, 'http://localhost').pathname,
        headers,
        body: buffered.subarray(headEnd + 4, headEnd + 4 + length),
      });
      const body = Buffer.from(response.body ?? '');
      socket.end(
        Buffer.concat([
          Buffer.from(
            `HTTP/1.1 ${response.status} Status\r\nContent-Length: ${body.length}\r\n` +
              (response.type ? `Content-Type: ${response.type}\r\n` : '') +
              'Connection: close\r\n\r\n',
          ),
          body,
        ]),
      );
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  return {
    host: `http://127.0.0.1:${port}`,
    objects,
    requests,
    failNext,
    close: () =>
      new Promise<void>((resolve) => {
        for (const socket of sockets) {
          socket.destroy();
        }
        server.close(() => resolve());
      }),
  };
}
