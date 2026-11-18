/**
 * Samples the cron computes itself (a minute of Bunny's request logs, a probe run) go to o11y the way the geocoder's
 * report sends its own: Prometheus text straight to /insert/0/, each at the time it describes, rather than through the
 * serverless path that sums per-request samples by the minute they arrive in. One sample per series per minute is what
 * the store's 20s dedup keeps whole, and a run repeated for the same minute writes the same samples at the same times,
 * which the dedup collapses, so a window is never counted twice.
 */
import { Identity, O11yConfig, send } from '../o11y';

export const METRICS_PATH = '/insert/0/prometheus/api/v1/import/prometheus';

export type Labels = { [key: string]: string };

export type Sample = { name: string; labels: Labels; value: number };

const escapeLabel = (value: string) => value.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n');

/**
 * The samples as Prometheus text at timestampMs, with o11y's identity labels last so no label of ours can shadow one.
 * Every line ends in a newline: VictoriaMetrics stores nothing of a last line without one.
 */
export function exposition(samples: Sample[], identity: Identity, timestampMs: number): string {
  return samples
    .filter(({ value }) => Number.isFinite(value))
    .map(({ name, labels, value }) => {
      const selector = Object.entries({ ...labels, ...identity })
        .map(([key, label]) => `${key}="${escapeLabel(label)}"`)
        .join(',');
      return `${name}{${selector}} ${value} ${Math.round(timestampMs)}\n`;
    })
    .join('');
}

/** Sends the samples in one POST. Never rejects (see send). */
export async function pushSamples(config: O11yConfig, samples: Sample[], timestampMs: number): Promise<void> {
  const body = exposition(samples, config.identity, timestampMs);
  if (body !== '') {
    await send(config, METRICS_PATH, body, 'text/plain; charset=utf-8');
  }
}
