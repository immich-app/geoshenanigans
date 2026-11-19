export type LogReaderConfig = { apiKey: string; pullZoneId: string };

export type ProbeConfig = { token: string; publicUrl: URL; intervalMinutes: number };

type BunnyEnv = Pick<
  WorkerEnv,
  'BUNNY_API_KEY' | 'BUNNY_PULL_ZONE_ID' | 'BUNNY_PUBLIC_URL' | 'GLOBALPING_TOKEN' | 'BUNNY_PROBE_INTERVAL_MINUTES'
>;

export const DEFAULT_PROBE_INTERVAL_MINUTES = 10;
// Each run is 12 Globalping tests (6 probes, 2 tiles), so 5 minutes is 144 an hour, under a third of the 500 free.
export const MIN_PROBE_INTERVAL_MINUTES = 5;

/** The log reader's settings, or undefined (it is skipped) without the account key and a numeric pull zone id. */
export function logReaderConfig(env: BunnyEnv): LogReaderConfig | undefined {
  const apiKey = env.BUNNY_API_KEY?.trim();
  const pullZoneId = env.BUNNY_PULL_ZONE_ID?.trim();
  if (!apiKey || !pullZoneId || !/^\d+$/.test(pullZoneId)) {
    return undefined;
  }
  return { apiKey, pullZoneId };
}

/** The probe's settings, or undefined (it is skipped) without the Globalping token and an http(s) public URL. */
export function probeConfig(env: BunnyEnv): ProbeConfig | undefined {
  const token = env.GLOBALPING_TOKEN?.trim();
  const url = env.BUNNY_PUBLIC_URL?.trim();
  if (!token || !url) {
    return undefined;
  }
  let publicUrl: URL;
  try {
    publicUrl = new URL(url);
  } catch {
    return undefined;
  }
  if (publicUrl.protocol !== 'https:' && publicUrl.protocol !== 'http:') {
    return undefined;
  }
  return { token, publicUrl, intervalMinutes: probeIntervalMinutes(env.BUNNY_PROBE_INTERVAL_MINUTES) };
}

/** BUNNY_PROBE_INTERVAL_MINUTES as a whole number of minutes, at least MIN_PROBE_INTERVAL_MINUTES, 10 when unset. */
export function probeIntervalMinutes(value: string | undefined): number {
  const parsed = value === undefined || value.trim() === '' ? NaN : Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return DEFAULT_PROBE_INTERVAL_MINUTES;
  }
  return Math.min(Math.max(parsed, MIN_PROBE_INTERVAL_MINUTES), 24 * 60);
}
