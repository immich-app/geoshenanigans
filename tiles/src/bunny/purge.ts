/**
 * One full pull-zone purge after each daily upload has replicated. Exact-URL purges are never used: they are limited
 * to 5/s and one server was seen ignoring them, while a full purge reached every PoP in 6-25 s.
 */
import { setTimeout as sleep } from 'node:timers/promises';

export const DEFAULT_API_URL = 'https://api.bunny.net';

export interface PurgeOptions {
  /** Default https://api.bunny.net; a local stand-in in tests. */
  baseUrl?: string;
  /** Attempts for 429, 5xx and network errors (default 6). Purging twice is harmless. */
  attempts?: number;
  /** First back-off; the API allows 0.5 purges/s after a burst of 20 (default 2 s). */
  baseDelayMs?: number;
  timeoutMs?: number;
}

export class PurgeError extends Error {}

export interface PurgeResult {
  status: number;
  attempts: number;
  ms: number;
}

export async function purgePullZone(
  apiKey: string,
  pullZoneId: string | number,
  options: PurgeOptions = {},
): Promise<PurgeResult> {
  const id = String(pullZoneId);
  if (!/^\d+$/.test(id)) {
    throw new Error(`Pull zone id must be numeric, got ${JSON.stringify(id)}`);
  }
  if (!apiKey) {
    throw new Error('Missing Bunny API key');
  }
  const url = `${(options.baseUrl ?? DEFAULT_API_URL).replace(/\/+$/, '')}/pullzone/${id}/purgeCache`;
  const attempts = options.attempts ?? 6;
  const baseDelay = options.baseDelayMs ?? 2_000;
  const started = performance.now();
  let last = '';
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: { AccessKey: apiKey, Accept: 'application/json' },
        signal: AbortSignal.timeout(options.timeoutMs ?? 30_000),
      });
      const text = (await response.text()).slice(0, 200);
      if (response.ok) {
        return { status: response.status, attempts: attempt, ms: Math.round(performance.now() - started) };
      }
      last = `HTTP ${response.status} ${text}`.trim();
      if (response.status !== 429 && response.status < 500) {
        throw new PurgeError(`Purge of pull zone ${id} failed: ${last}`);
      }
    } catch (error) {
      if (error instanceof PurgeError) {
        throw error;
      }
      last = error instanceof Error ? error.message : String(error);
    }
    if (attempt < attempts) {
      await sleep(baseDelay * 2 ** (attempt - 1) * (0.5 + Math.random()));
    }
  }
  throw new PurgeError(`Purge of pull zone ${id} failed after ${attempts} attempts: ${last}`);
}

/** Purges BUNNY_PULL_ZONE_ID with BUNNY_API_KEY. */
export function purgePullZoneFromEnv(env = process.env, options?: PurgeOptions): Promise<PurgeResult> {
  if (!env.BUNNY_API_KEY || !env.BUNNY_PULL_ZONE_ID) {
    throw new Error('BUNNY_API_KEY and BUNNY_PULL_ZONE_ID must be set');
  }
  return purgePullZone(env.BUNNY_API_KEY, env.BUNNY_PULL_ZONE_ID, options);
}
