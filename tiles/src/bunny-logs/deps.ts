/** What the cron jobs take from the runtime, so tests can run them without waiting or guessing. */
export type Deps = {
  now: () => number;
  sleep: (ms: number) => Promise<void>;
  random: () => number;
};

export const runtimeDeps: Deps = {
  now: () => Date.now(),
  sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  random: () => Math.random(),
};

export type RunResult = 'ok' | 'error' | 'skipped';

/** At most `length` characters of a string field from an API response, undefined for anything else or "". */
export function text(value: unknown, length = 256): string | undefined {
  return typeof value === 'string' && value !== '' ? value.slice(0, length) : undefined;
}

/** A finite number field from an API response, undefined for anything else. */
export function finite(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}
