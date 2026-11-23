/**
 * Samples the cron computes itself (a minute of Bunny's request logs, a probe run) go to o11y the way the geocoder's
 * report sends its own: Prometheus text straight to /insert/0/, each at the time it describes, rather than through the
 * serverless path that sums per-request samples by the minute they arrive in. One sample per series per minute is what
 * the store's 20s dedup keeps whole. Of samples a series has at one timestamp the dedup keeps the biggest
 * (VictoriaMetrics' docs, "Deduplication"), so a minute read twice keeps its fuller count and is never counted twice.
 *
 * o11y's identity labels (project, cluster, env, provider, region) are on every sample and win over ours, so the
 * labels the spec calls `region` are `edge_location` (the log reader) and `continent` (the probe).
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

export type SampleSet = { samples: Sample[]; timestampMs: number };

/** Sends sets of samples, each at its own time, in one POST. Never rejects (see send). */
export async function pushSampleSets(config: O11yConfig, sets: SampleSet[]): Promise<void> {
  const body = sets.map(({ samples, timestampMs }) => exposition(samples, config.identity, timestampMs)).join('');
  if (body !== '') {
    await send(config, METRICS_PATH, body, 'text/plain; charset=utf-8');
  }
}

/** Sends the samples in one POST. Never rejects (see send). */
export async function pushSamples(config: O11yConfig, samples: Sample[], timestampMs: number): Promise<void> {
  await pushSampleSets(config, [{ samples, timestampMs }]);
}
