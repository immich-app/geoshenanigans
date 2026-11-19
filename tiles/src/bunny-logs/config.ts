export type LogReaderConfig = { apiKey: string; pullZoneId: string };

/**
 * cutoverOrigin: an origin the TileJSON may name tiles on besides the public URL's, while the public URL moves (the
 * test hostname's from the DNS cutover until a sync rewrites the TileJSON); undefined when unset. Any other fails the
 * probe's TileJSON test (probe.ts).
 */
export type ProbeConfig = { token: string; publicUrl: URL; intervalMinutes: number; cutoverOrigin?: string };

type BunnyEnv = Pick<
  WorkerEnv,
  | 'BUNNY_API_KEY'
  | 'BUNNY_PULL_ZONE_ID'
  | 'BUNNY_PUBLIC_URL'
  | 'GLOBALPING_TOKEN'
  | 'BUNNY_PROBE_INTERVAL_MINUTES'
  | 'BUNNY_PROBE_CUTOVER_ORIGIN'
>;

export const DEFAULT_PROBE_INTERVAL_MINUTES = 10;
// Each run is 12 Globalping tests (6 probes, 2 tiles), so 5 minutes is 144 an hour, under a third of the 500 free.
export const MIN_PROBE_INTERVAL_MINUTES = 5;
// The probe alerts (o11y/alerts/geoshenanigans-tiles.yaml) need two runs in their 30-minute windows at all times, with
// a minute or two for a run's samples to arrive: 14 minutes apart leaves that, 15 doesn't. tiles-worker's
// bunny_probe_interval_minutes validates the same range.
export const MAX_PROBE_INTERVAL_MINUTES = 14;

/** The log reader's settings, or undefined (it is skipped) without the account key and a numeric pull zone id. */
export function logReaderConfig(env: BunnyEnv): LogReaderConfig | undefined {
  const apiKey = env.BUNNY_API_KEY?.trim();
  const pullZoneId = env.BUNNY_PULL_ZONE_ID?.trim();
  if (!apiKey || !pullZoneId || !/^\d+$/.test(pullZoneId)) {
    return undefined;
  }
  return { apiKey, pullZoneId };
}

/** An http(s) URL, or undefined. */
function httpUrl(value: string): URL | undefined {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    return undefined;
  }
  return url.protocol === 'https:' || url.protocol === 'http:' ? url : undefined;
}

/**
 * The probe's settings, or undefined (it is skipped) without the Globalping token and an http(s) public URL. A
 * BUNNY_PROBE_CUTOVER_ORIGIN that isn't an http(s) origin alone (no path, query or credentials) is left out, so the
 * TileJSON may name the public origin only (tiles-worker's variables refuse such a value before it gets here).
 */
export function probeConfig(env: BunnyEnv): ProbeConfig | undefined {
  const token = env.GLOBALPING_TOKEN?.trim();
  const url = env.BUNNY_PUBLIC_URL?.trim();
  if (!token || !url) {
    return undefined;
  }
  const publicUrl = httpUrl(url);
  if (!publicUrl) {
    return undefined;
  }
  const cutover = env.BUNNY_PROBE_CUTOVER_ORIGIN?.trim();
  const cutoverUrl = cutover ? httpUrl(cutover) : undefined;
  // An origin alone: its href is the origin and a slash (a path, query, fragment or credentials would show there).
  const cutoverOrigin = cutoverUrl?.href === `${cutoverUrl?.origin}/` ? cutoverUrl.origin : undefined;
  return {
    token,
    publicUrl,
    intervalMinutes: probeIntervalMinutes(env.BUNNY_PROBE_INTERVAL_MINUTES),
    ...(cutoverOrigin && { cutoverOrigin }),
  };
}

/**
 * BUNNY_PROBE_INTERVAL_MINUTES as a whole number of minutes, brought into MIN_PROBE_INTERVAL_MINUTES to
 * MAX_PROBE_INTERVAL_MINUTES, 10 when unset.
 */
export function probeIntervalMinutes(value: string | undefined): number {
  const parsed = value === undefined || value.trim() === '' ? NaN : Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return DEFAULT_PROBE_INTERVAL_MINUTES;
  }
  return Math.min(Math.max(parsed, MIN_PROBE_INTERVAL_MINUTES), MAX_PROBE_INTERVAL_MINUTES);
}
