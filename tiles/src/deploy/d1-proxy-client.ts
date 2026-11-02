import { setTimeout } from 'node:timers/promises';
import type { ProxyRequest, ProxyResponse, ProxyStatement } from '../d1-proxy';
import { CACHE_TABLE_PATTERN, requireEnv } from './config';

const MAX_ATTEMPTS = 8;

export type D1ProxyClientOptions = {
  // Covers the request and reading its response, so a stalled connection is retried rather than awaited forever.
  attemptTimeoutMs?: number;
  retryDelayMs?: number;
};

class FatalProxyError extends Error {}

/** Talks to the D1 proxy worker. Every call is retried, so callers only send statements that are safe to repeat. */
export class D1ProxyClient {
  private attemptTimeoutMs: number;
  private retryDelayMs: number;

  constructor(
    private url: string,
    private token: string,
    private env: string,
    { attemptTimeoutMs = 120_000, retryDelayMs = 1000 }: D1ProxyClientOptions = {},
  ) {
    this.attemptTimeoutMs = attemptTimeoutMs;
    this.retryDelayMs = retryDelayMs;
  }

  static fromEnv() {
    return new D1ProxyClient(requireEnv('D1_PROXY_URL'), requireEnv('D1_PROXY_TOKEN'), requireEnv('D1_ENV'));
  }

  async run(sql: string, params: unknown[] = []): Promise<Record<string, unknown>[]> {
    const [results] = await this.send({ statement: { sql, params } });
    return results;
  }

  /** Runs the statements in one transaction. */
  async batch(statements: ProxyStatement[]): Promise<void> {
    await this.send({ batch: statements });
  }

  async tableNames(): Promise<string[]> {
    const rows = await this.run('PRAGMA table_list');
    return rows.map((row) => row.name as string);
  }

  async cacheTables(): Promise<string[]> {
    return (await this.tableNames()).filter((name) => CACHE_TABLE_PATTERN.test(name));
  }

  private async send(body: ProxyRequest): Promise<Record<string, unknown>[][]> {
    let lastError: unknown;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        const response = await fetch(`${this.url.replace(/\/$/, '')}/${this.env}`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${this.token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
          signal: AbortSignal.timeout(this.attemptTimeoutMs),
        });
        const text = await response.text();
        if (response.ok) {
          return (JSON.parse(text) as { results: Record<string, unknown>[][] }).results;
        }
        const message = `D1 proxy returned ${response.status}: ${errorMessage(text)}`;
        // Auth and malformed requests won't fix themselves; D1 errors (500) may be transient.
        if (response.status === 400 || response.status === 401 || response.status === 405) {
          throw new FatalProxyError(message);
        }
        lastError = new Error(message);
      } catch (error) {
        if (error instanceof FatalProxyError) {
          throw error;
        }
        lastError = error;
      }
      if (attempt < MAX_ATTEMPTS) {
        console.warn(`D1 proxy attempt ${attempt} failed, retrying: ${String(lastError)}`);
        await setTimeout(Math.min(this.retryDelayMs * 2 ** (attempt - 1), 30_000));
      }
    }
    throw lastError;
  }
}

function errorMessage(text: string): string {
  try {
    return (JSON.parse(text) as ProxyResponse & { error?: string }).error ?? text;
  } catch {
    return text.slice(0, 500);
  }
}
