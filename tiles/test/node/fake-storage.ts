/**
 * A local stand-in for Bunny Edge Storage: PUT with Checksum validation, DESCRIBE, GET (files and folder listings)
 * and DELETE, with injectable faults and counters of PUTs per path and of identical re-PUTs (which clear a real
 * file's ReplicatedZones for good, so the fake does the same).
 *
 * Node's HTTP parser rejects the DESCRIBE method, so this speaks just enough HTTP/1.1 over raw sockets.
 */
import { createHash } from 'node:crypto';
import { createServer, Server, Socket } from 'node:net';

export type Fault =
  /** 429, nothing stored. */
  | 'rate-limit'
  /** 500, nothing stored. */
  | 'server-error'
  /** Stored, then answered 503. */
  | 'stored-server-error'
  /** Stored, then answered 408. */
  | 'stored-request-timeout'
  /** Connection dropped before storing. */
  | 'reset'
  /** Stored, then the connection dropped without an answer. */
  | 'stored-reset'
  /** Stored, never answered: the client times out. */
  | 'stored-hang'
  /** Never stored, never answered. */
  | 'hang'
  /** Never answered; the body is stored `lateStoreMs` after it arrived, as when the server commits after the client gave up. */
  | 'late-store'
  /** Answered 503 at once, as by a gateway that gave up; the backend stores the body `lateStoreMs` after it arrived. */
  | 'gateway-late-store'
  /** Connection dropped at once; the backend stores the body `lateStoreMs` after it arrived. */
  | 'reset-late-store';

export interface RequestInfo {
  method: string;
  /** Relative to the zone root, e.g. "v1/1/0/0.mvt" ("" for the root, trailing "/" for folders). */
  path: string;
  /** How many requests with this method and path arrived so far, this one included. */
  attempt: number;
  /** The listening address the request came in on. */
  address: string;
}

export interface FakeStorageOptions {
  zone?: string;
  password?: string;
  /** Loopback addresses to listen on, all with the same port (default 127.0.0.1). */
  addresses?: string[];
  latencyMs?: number | (() => number);
  regions?: string[];
  /** Delay before each region shows up in a stored file's ReplicatedZones; regions left out replicate only on replicate(). */
  replicationDelayMs?: Record<string, number>;
  fault?: (request: RequestInfo) => Fault | undefined;
  onRequest?: (request: RequestInfo) => void;
  /** Awaited before a request is answered (after onRequest), to hold chosen requests back. */
  hold?: (request: RequestInfo) => Promise<void> | undefined;
  /** How long after arriving a 'late-store', 'gateway-late-store' or 'reset-late-store' PUT is stored (default 500 ms). */
  lateStoreMs?: number;
  /**
   * DESCRIBE lagging an overwrite, as measured on Bunny: a PUT of other bytes that lands within `withinMs` of the
   * file's previous write leaves DESCRIBE and listings showing the previous version (Checksum, Length, LastChanged,
   * ReplicatedZones) for `forMs` (Infinity: for good), while GET returns the new bytes.
   */
  staleOverwrite?: { withinMs: number; forMs: number };
}

interface StoredFile {
  bytes: Buffer;
  checksum: string;
  replicatedZones: Set<string>;
  lastChanged: string;
  dateCreated: string;
  /** Re-PUT with identical bytes: its replica list never recovers. */
  broken: boolean;
  /** When it was written (Date.now()). */
  writtenAt: number;
  /** What DESCRIBE shows instead until `until` (staleOverwrite). */
  shown?: { checksum: string; length: number; lastChanged: string; replicatedZones: Set<string>; until: number };
}

interface ParsedRequest {
  method: string;
  target: string;
  headers: Record<string, string>;
  body: Buffer;
}

const REASONS: Record<number, string> = {
  200: 'OK',
  201: 'Created',
  400: 'Bad Request',
  401: 'Unauthorized',
  404: 'Not Found',
  408: 'Request Timeout',
  429: 'Too Many Requests',
  500: 'Internal Server Error',
  503: 'Service Unavailable',
};

export const sha256Hex = (bytes: Uint8Array) => createHash('sha256').update(bytes).digest('hex').toUpperCase();

export class FakeStorage {
  readonly zone: string;
  readonly password: string;
  readonly files = new Map<string, StoredFile>();
  readonly putsByPath = new Map<string, number>();
  /** Paths that received a PUT whose bytes equalled what was already stored. */
  readonly identicalReputs: string[] = [];
  readonly requests: Record<string, number> = {};
  readonly faultsInjected: Partial<Record<Fault, number>> = {};
  readonly connectionsByAddress = new Map<string, number>();
  /** The most connections open at once on each address. */
  readonly maxOpenByAddress = new Map<string, number>();
  readonly requestsByAddress = new Map<string, number>();
  readonly hostHeaders = new Set<string>();
  inFlight = 0;
  maxInFlight = 0;
  options: FakeStorageOptions;
  private attempts = new Map<string, number>();
  private openByAddress = new Map<string, number>();
  private sockets = new Set<Socket>();
  private timers = new Set<NodeJS.Timeout>();
  private servers: Server[] = [];
  private constructor(
    options: FakeStorageOptions,
    readonly port: number,
  ) {
    this.options = options;
    this.zone = options.zone ?? 'test-zone';
    this.password = options.password ?? 'test-password';
  }

  static async start(options: FakeStorageOptions = {}): Promise<FakeStorage> {
    const addresses = options.addresses ?? ['127.0.0.1'];
    let port = 0;
    const holder: { fake?: FakeStorage } = {};
    const servers: Server[] = [];
    try {
      for (const address of addresses) {
        const server = createServer((socket) => holder.fake!.accept(socket));
        await new Promise<void>((resolve, reject) => {
          server.once('error', reject);
          server.listen(port, address, () => resolve());
        });
        port = (server.address() as { port: number }).port;
        servers.push(server);
      }
    } catch (error) {
      // No fake comes back for the test to close, so the listeners already bound must not outlive the failure.
      await Promise.all(servers.map((server) => new Promise((resolve) => server.close(resolve))));
      throw error;
    }
    const fake = new FakeStorage(options, port);
    fake.servers = servers;
    holder.fake = fake;
    return fake;
  }

  /** The origin to give BunnyStorage as `host`. */
  get url(): string {
    return `http://127.0.0.1:${this.port}`;
  }

  get identicalReputCount(): number {
    return this.identicalReputs.length;
  }

  get regions(): string[] {
    return this.options.regions ?? ['UK', 'NY', 'SYD'];
  }

  checksum(path: string): string | undefined {
    return this.files.get(path)?.checksum;
  }

  /** Stores a file directly, as if uploaded earlier, without counting a PUT. */
  seed(path: string, bytes: Uint8Array, replicatedZones: string[] = []) {
    const now = new Date().toISOString();
    this.files.set(path, {
      bytes: Buffer.from(bytes),
      checksum: sha256Hex(bytes),
      replicatedZones: new Set(replicatedZones),
      lastChanged: now,
      dateCreated: now,
      broken: false,
      writtenAt: Date.now(),
    });
  }

  /** Marks files (all, or those under `prefix`) as held by `regions` (default: every region). */
  replicate(prefix = '', regions = this.regions) {
    for (const [path, file] of this.files) {
      if (path.startsWith(prefix) && !file.broken) {
        regions.forEach((region) => file.replicatedZones.add(region));
      }
    }
  }

  async close(): Promise<void> {
    this.timers.forEach((timer) => clearTimeout(timer));
    this.sockets.forEach((socket) => socket.destroy());
    await Promise.all(this.servers.map((server) => new Promise((resolve) => server.close(resolve))));
  }

  private accept(socket: Socket) {
    this.sockets.add(socket);
    const address = socket.localAddress ?? '';
    this.connectionsByAddress.set(address, (this.connectionsByAddress.get(address) ?? 0) + 1);
    const open = (this.openByAddress.get(address) ?? 0) + 1;
    this.openByAddress.set(address, open);
    this.maxOpenByAddress.set(address, Math.max(open, this.maxOpenByAddress.get(address) ?? 0));
    socket.on('close', () => {
      this.sockets.delete(socket);
      this.openByAddress.set(address, (this.openByAddress.get(address) ?? 1) - 1);
    });
    socket.on('error', () => {});
    let buffer: Buffer = Buffer.alloc(0);
    let busy = false;
    let dead = false;
    const pump = async () => {
      if (busy || dead) {
        return;
      }
      for (;;) {
        const parsed = parseRequest(buffer);
        if (!parsed) {
          return;
        }
        buffer = buffer.subarray(parsed.consumed);
        busy = true;
        const keep = await this.handle(parsed.request, socket, address);
        busy = false;
        if (!keep) {
          dead = true;
          return;
        }
      }
    };
    socket.on('data', (chunk: Buffer) => {
      buffer = buffer.length ? Buffer.concat([buffer, chunk]) : chunk;
      void pump();
    });
  }

  /** Answers one request; false when the connection is gone (dropped or hung). */
  private async handle(request: ParsedRequest, socket: Socket, address: string): Promise<boolean> {
    this.inFlight++;
    this.maxInFlight = Math.max(this.maxInFlight, this.inFlight);
    try {
      return await this.respond(request, socket, address);
    } finally {
      this.inFlight--;
    }
  }

  private async respond(request: ParsedRequest, socket: Socket, address: string): Promise<boolean> {
    const { method, headers } = request;
    this.requests[method] = (this.requests[method] ?? 0) + 1;
    this.requestsByAddress.set(address, (this.requestsByAddress.get(address) ?? 0) + 1);
    if (headers.host) {
      this.hostHeaders.add(headers.host);
    }
    const url = new URL(request.target, 'http://fake');
    const segments = url.pathname.split('/').slice(1).map(decodeURIComponent);
    const zone = segments.shift();
    const path = segments.join('/');
    const key = `${method} ${path}`;
    const attempt = (this.attempts.get(key) ?? 0) + 1;
    this.attempts.set(key, attempt);
    const info = { method, path, attempt, address };
    this.options.onRequest?.(info);
    await this.options.hold?.(info);

    if (zone !== this.zone || headers.accesskey !== this.password) {
      return this.send(socket, 401, JSON.stringify({ HttpCode: 401, Message: 'Unauthorized' }));
    }
    const fault = this.options.fault?.(info);
    if (fault) {
      this.faultsInjected[fault] = (this.faultsInjected[fault] ?? 0) + 1;
    }
    if (fault === 'rate-limit') {
      return this.send(socket, 429, '');
    }
    if (fault === 'server-error') {
      return this.send(socket, 500, 'boom');
    }
    if (fault === 'reset') {
      socket.destroy();
      return false;
    }
    if (fault === 'hang') {
      return false;
    }

    let status = 200;
    let body: string | Buffer = '';
    if (method === 'PUT') {
      const checksum = headers.checksum?.toUpperCase();
      const actual = sha256Hex(request.body);
      if (checksum && checksum !== actual) {
        return this.send(socket, 400, JSON.stringify({ HttpCode: 400, Message: 'Checksum mismatch' }));
      }
      if (fault === 'late-store' || fault === 'gateway-late-store' || fault === 'reset-late-store') {
        const timer = setTimeout(() => {
          this.timers.delete(timer);
          this.store(path, request.body, actual);
        }, this.options.lateStoreMs ?? 500);
        this.timers.add(timer);
        if (fault === 'gateway-late-store') {
          return this.send(socket, 503, 'gateway timeout');
        }
        if (fault === 'reset-late-store') {
          socket.destroy();
        }
        return false;
      }
      this.store(path, request.body, actual);
      status = 201;
      body = JSON.stringify({ HttpCode: 201, Message: 'File uploaded.' });
    } else if (method === 'DESCRIBE') {
      const file = this.files.get(path);
      if (!file) {
        status = 404;
      } else {
        body = JSON.stringify(this.object(path, file));
      }
    } else if (method === 'GET') {
      if (path === '' || path.endsWith('/')) {
        body = JSON.stringify(this.listing(path));
      } else {
        const file = this.files.get(path);
        status = file ? 200 : 404;
        body = file ? file.bytes : '';
      }
    } else if (method === 'DELETE') {
      if (path.endsWith('/')) {
        [...this.files.keys()].filter((name) => name.startsWith(path)).forEach((name) => this.files.delete(name));
      } else {
        status = this.files.delete(path) ? 200 : 404;
      }
    } else {
      status = 400;
    }

    if (fault === 'stored-reset') {
      socket.destroy();
      return false;
    }
    if (fault === 'stored-hang') {
      return false;
    }
    await this.delay();
    const faulted = fault === 'stored-server-error' ? 503 : fault === 'stored-request-timeout' ? 408 : undefined;
    return this.send(socket, faulted ?? status, body);
  }

  private store(path: string, bytes: Buffer, checksum: string) {
    this.putsByPath.set(path, (this.putsByPath.get(path) ?? 0) + 1);
    const existing = this.files.get(path);
    const now = new Date().toISOString();
    if (existing && existing.checksum === checksum) {
      this.identicalReputs.push(path);
      existing.broken = true;
      existing.replicatedZones.clear();
      existing.lastChanged = now;
      existing.writtenAt = Date.now();
      return;
    }
    const lag = this.options.staleOverwrite;
    const file: StoredFile = {
      bytes: Buffer.from(bytes),
      checksum,
      replicatedZones: new Set(),
      lastChanged: now,
      dateCreated: existing?.dateCreated ?? now,
      broken: false,
      writtenAt: Date.now(),
      shown:
        existing && lag && Date.now() - existing.writtenAt < lag.withinMs
          ? {
              checksum: existing.checksum,
              length: existing.bytes.length,
              lastChanged: existing.lastChanged,
              replicatedZones: new Set(existing.replicatedZones),
              until: Date.now() + lag.forMs,
            }
          : undefined,
    };
    this.files.set(path, file);
    for (const [region, delay] of Object.entries(this.options.replicationDelayMs ?? {})) {
      const timer = setTimeout(() => {
        this.timers.delete(timer);
        if (this.files.get(path) === file && !file.broken) {
          file.replicatedZones.add(region);
        }
      }, delay);
      this.timers.add(timer);
    }
  }

  private object(path: string, file: StoredFile) {
    const slash = path.lastIndexOf('/');
    const shown =
      file.shown && Date.now() < file.shown.until
        ? file.shown
        : { ...file, length: file.bytes.length, replicatedZones: file.replicatedZones };
    return {
      Guid: '00000000-0000-0000-0000-000000000000',
      StorageZoneName: this.zone,
      Path: `/${this.zone}/${path.slice(0, slash + 1)}`,
      ObjectName: path.slice(slash + 1),
      Length: shown.length,
      LastChanged: shown.lastChanged,
      ServerId: 1,
      IsDirectory: false,
      DateCreated: file.dateCreated,
      StorageZoneId: 1,
      Checksum: shown.checksum,
      ReplicatedZones: [...shown.replicatedZones].join(','),
    };
  }

  private listing(dir: string) {
    const rows = new Map<string, object>();
    for (const [path, file] of this.files) {
      if (!path.startsWith(dir)) {
        continue;
      }
      const rest = path.slice(dir.length);
      const slash = rest.indexOf('/');
      if (slash === -1) {
        rows.set(rest, this.object(path, file));
      } else if (!rows.has(rest.slice(0, slash))) {
        const name = rest.slice(0, slash);
        rows.set(name, {
          Path: `/${this.zone}/${dir}`,
          ObjectName: name,
          Length: 0,
          IsDirectory: true,
          Checksum: null,
          ReplicatedZones: null,
          LastChanged: file.lastChanged,
        });
      }
    }
    return [...rows.values()];
  }

  private async delay() {
    const latency = typeof this.options.latencyMs === 'function' ? this.options.latencyMs() : this.options.latencyMs;
    if (latency && latency > 0) {
      await new Promise<void>((resolve) => {
        const timer = setTimeout(() => {
          this.timers.delete(timer);
          resolve();
        }, latency);
        this.timers.add(timer);
      });
    }
  }

  private send(socket: Socket, status: number, body: string | Buffer): boolean {
    if (socket.destroyed) {
      return false;
    }
    const bytes = typeof body === 'string' ? Buffer.from(body) : body;
    const head =
      `HTTP/1.1 ${status} ${REASONS[status] ?? 'Status'}\r\n` +
      `Content-Type: application/json\r\nContent-Length: ${bytes.length}\r\nConnection: keep-alive\r\n\r\n`;
    socket.write(Buffer.concat([Buffer.from(head), bytes]));
    return true;
  }
}

function parseRequest(buffer: Buffer): { request: ParsedRequest; consumed: number } | undefined {
  const end = buffer.indexOf('\r\n\r\n');
  if (end === -1) {
    return undefined;
  }
  const [requestLine, ...lines] = buffer.subarray(0, end).toString('latin1').split('\r\n');
  const [method, target] = requestLine.split(' ');
  const headers: Record<string, string> = {};
  for (const line of lines) {
    const colon = line.indexOf(':');
    headers[line.slice(0, colon).trim().toLowerCase()] = line.slice(colon + 1).trim();
  }
  const length = Number(headers['content-length'] ?? 0);
  if (buffer.length < end + 4 + length) {
    return undefined;
  }
  const body = Buffer.from(buffer.subarray(end + 4, end + 4 + length));
  return { request: { method, target, headers, body }, consumed: end + 4 + length };
}
