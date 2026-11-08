/**
 * Shipping to FUTO's o11y stack (immich-app/yucca-o11y): metrics to its serverless ingest path, logs to VictoriaLogs,
 * both through the vmauth gateway with the shared bearer token.
 *
 * A Worker can't keep a counter between requests, so every request sends one sample per measurement: 1 for an
 * invocation, the time for a duration, and a 1 in the duration's histogram bucket. The store keeps one sample per series
 * per 20s, which would collapse those, so they go to /serverless/insert/, where o11y writes each series' sum per minute
 * as <name>:1m_sum_samples (yucca-o11y docs/05, "Shippers that send one sample per event"). Dashboards and alerts add the
 * minutes up with sum_over_time().
 */
import { IMetricsProviderRepository } from './interface';
import { Metric } from './repository';

/** The five identity labels o11y requires on every series and log line; project and cluster pick the tenant. */
export type Identity = {
  project: string;
  cluster: string;
  env: string;
  provider: string;
  region: string;
};

export type O11yConfig = {
  // The gateway's origin, e.g. https://vmauth.futostatus.com.
  url: string;
  token: string;
  identity: Identity;
};

/** o11y's short environment names; the worker's ENVIRONMENT is "prod" or "dev" when deployed. */
function shortEnv(environment: string): string {
  switch (environment) {
    case 'production':
      return 'prod';
    case 'development':
      return 'dev';
    default:
      return environment;
  }
}

/** Where to ship, or undefined (nothing is shipped) without a gateway and token, as in tests and wrangler dev. */
export function o11yConfig(env: Pick<WorkerEnv, 'O11Y_URL' | 'O11Y_TOKEN' | 'ENVIRONMENT'>): O11yConfig | undefined {
  const url = env.O11Y_URL?.trim().replace(/\/+$/, '');
  const token = env.O11Y_TOKEN?.trim();
  if (!url || !token) {
    return undefined;
  }
  return {
    url,
    token,
    // `cluster` names the service, as a Worker runs on no cluster of its own; the edge a request reached is `colo`.
    identity: {
      project: 'geoshenanigans',
      cluster: 'tiles',
      env: shortEnv(env.ENVIRONMENT),
      provider: 'cloudflare',
      region: 'world',
    },
  };
}

// Tags that reach o11y. Every combination of tag values is a series o11y writes every minute for an hour after its
// last request, and a duration's once per histogram bucket, so only tags with a handful of values pass. The client's
// origin and network, the protocol, SQL text and error messages stay out (the sampled request logs carry the first
// two); so does any tag not listed here, including one named like an identity label.
const KEPT_TAGS = new Set([
  'continent',
  'request_type',
  'status',
  'z',
  'version',
  'url',
  'style',
  'deployment_key',
  'served_by_region',
  'served_by_primary',
  'endpoint',
  'cache_status',
  'race',
]);

// The edge colo has a few hundred values, so it stays on the request as a whole: per-colo traffic and latency.
const COLO_METRICS = new Set(['tiles_handle_request']);

// The planet build's maxzoom (its TileJSON's), the API versions and the styles the worker serves (map-styles' themes).
const MAX_ZOOM = 15;
const VERSIONS = new Set(['1']);
const STYLES = new Set(['light', 'dark', 'black']);

// Tags whose values come from the request's URL, so anyone could invent new ones: each keeps the values the worker
// serves, zooms without leading zeros, and files anything else under "other".
const URL_TAG_VALUES: { [key: string]: (value: string) => string } = {
  z: (value) => (/^\d+$/.test(value) && Number(value) <= MAX_ZOOM ? String(Number(value)) : 'other'),
  version: (value) => (VERSIONS.has(value) ? value : 'other'),
  style: (value) => (STYLES.has(value) ? value : 'other'),
};

/** Every series is named tiles_*, so the worker's D1 and Tigris series don't read as anyone else's. */
export function o11yMeasurement(name: string): string {
  return name.startsWith('tiles_') ? name : `tiles_${name}`;
}

/** The metric's tags that reach o11y (see KEPT_TAGS and URL_TAG_VALUES), without empty values. */
export function o11yTags(metric: Metric): [string, string][] {
  const measurement = o11yMeasurement(metric.name);
  return [...metric.tags]
    .filter(([key, value]) => value !== '' && (KEPT_TAGS.has(key) || (key === 'colo' && COLO_METRICS.has(measurement))))
    .map(([key, value]) => [key, URL_TAG_VALUES[key]?.(value) ?? value]);
}

// InfluxDB line protocol escaping: measurements escape commas and spaces, tag keys and values also equals signs.
const escapeMeasurement = (value: string) => value.replace(/[,\s]/g, (c) => `\\${c}`);
const escapeTag = (value: string) => value.replace(/[,=\s]/g, (c) => `\\${c}`);

// VictoriaMetrics histogram buckets: 18 to a decade, lower edge exclusive and upper inclusive, written as it writes them.
const BUCKETS_PER_DECADE = 18;
const formatEdge = (edge: number) => {
  const [mantissa, exponent] = edge.toExponential(3).split('e');
  const power = Number(exponent);
  return `${mantissa}e${power < 0 ? '-' : '+'}${String(Math.abs(power)).padStart(2, '0')}`;
};

/** The VictoriaMetrics histogram bucket (vmrange label) a value falls in, e.g. 30 → "2.783e+01...3.162e+01". */
export function vmrange(value: number): string {
  if (!(value > 1e-9)) {
    return '0...1.000e-09';
  }
  // The epsilon keeps a value on an edge in the bucket below it, as VictoriaMetrics does.
  const k = Math.ceil(BUCKETS_PER_DECADE * Math.log10(value) - 1e-9) - 1;
  return `${formatEdge(10 ** (k / BUCKETS_PER_DECADE))}...${formatEdge(10 ** ((k + 1) / BUCKETS_PER_DECADE))}`;
}

/**
 * The InfluxDB lines for a metric, which o11y stores as <measurement>_<field> per field: one with every finite field,
 * and for each duration a 1 in its histogram bucket, <measurement>_<field>_bucket{vmrange=...}, so o11y's per-minute
 * sums give its distribution. None without a finite field. No timestamps: o11y files each sample under the minute it
 * arrives in.
 */
export function influxLines(metric: Metric, identity: Identity): string[] {
  const fields = [...metric.fields].filter(([, { value }]) => typeof value === 'number' && Number.isFinite(value));
  if (fields.length === 0) {
    return [];
  }
  const measurement = o11yMeasurement(metric.name);
  const tags = [...Object.entries(identity), ...o11yTags(metric)]
    .map(([key, value]) => `,${escapeTag(key)}=${escapeTag(value)}`)
    .join('');
  const lines = [
    `${escapeMeasurement(measurement)}${tags} ${fields.map(([key, { value }]) => `${escapeTag(key)}=${value}`).join(',')}`,
  ];
  for (const [key, { value, type }] of fields) {
    if (type === 'duration') {
      lines.push(`${escapeMeasurement(`${measurement}_${key}`)}${tags},vmrange=${vmrange(value)} bucket=1`);
    }
  }
  return lines;
}

/**
 * POSTs to the gateway. Never rejects: a failed push is only logged to the console, never to o11y, as that is what
 * failed.
 */
async function send(config: O11yConfig, path: string, body: string, contentType: string): Promise<void> {
  try {
    const response = await fetch(`${config.url}${path}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${config.token}`, 'Content-Type': contentType },
      body,
      // A gateway that accepts the connection and then stalls would otherwise hold the invocation open.
      signal: AbortSignal.timeout(5000),
    });
    if (!response.ok) {
      // vmauth and VictoriaMetrics name the bad line or the auth failure in the body.
      const detail = await response.text().catch(() => '');
      console.error(`o11y: ${path} answered ${response.status}`, detail.slice(0, 300));
      return;
    }
    await response.body?.cancel();
  } catch (error) {
    console.error(`o11y: ${path} failed`, error);
  }
}

/** Collects a request's metrics as InfluxDB lines and sends them to o11y's serverless ingest path in one POST. */
export class O11yMetricsProvider implements IMetricsProviderRepository {
  private lines: string[] = [];

  constructor(private config: O11yConfig | undefined) {}

  pushMetric(metric: Metric) {
    if (!this.config) {
      return;
    }
    this.lines.push(...influxLines(metric, this.config.identity));
  }

  async flush() {
    if (!this.config || this.lines.length === 0) {
      return;
    }
    const body = this.lines.join('\n');
    this.lines = [];
    await send(this.config, '/serverless/insert/influx/write', body, 'text/plain; charset=utf-8');
  }
}

/**
 * The heartbeat: when it was sent, with the deployment the worker serves. The serverless path writes only while
 * requests arrive, so the heartbeat goes straight to /insert/0/ as a gauge, one sample a minute (the store keeps one per
 * 20s), and its absence alert tells a broken metrics path (token, gateway, cron) from a worker that gets no requests
 * (yucca-o11y docs/05, "Shippers that push instead of scrape").
 */
export function heartbeatLine(identity: Identity, deploymentKey: string, now = Date.now()): string {
  const tags = [...Object.entries(identity), ['deployment_key', deploymentKey]]
    .filter(([, value]) => value !== '')
    .map(([key, value]) => `,${escapeTag(key)}=${escapeTag(value)}`)
    .join('');
  return `tiles_heartbeat${tags} timestamp_seconds=${Math.floor(now / 1000)}`;
}

/** Sends the heartbeat (heartbeatLine). Never rejects. */
export async function sendHeartbeat(config: O11yConfig, deploymentKey: string, now = Date.now()): Promise<void> {
  await send(
    config,
    '/insert/0/influx/write',
    heartbeatLine(config.identity, deploymentKey, now),
    'text/plain; charset=utf-8',
  );
}

export type LogLevel = 'info' | 'warn' | 'error';
export type LogFields = { [key: string]: string | number | boolean | undefined };

// VictoriaLogs keys a stream on these; everything else on a line is a field to filter on.
const LOG_STREAM_FIELDS = 'project,cluster,env';

/**
 * A request's log lines, written to the console as they happen and sent to VictoriaLogs in one POST at the end. Each
 * line carries the identity labels and the request's own fields (its Cf-Ray, path and colo), so one request's lines
 * can be followed with ray:="<id>".
 */
export class O11yLogger {
  private lines: string[] = [];

  constructor(
    private config: O11yConfig | undefined,
    private requestFields: LogFields = {},
  ) {}

  info(message: string, fields: LogFields = {}) {
    this.log('info', message, fields);
  }

  warn(message: string, fields: LogFields = {}) {
    this.log('warn', message, fields);
  }

  /** Logs an error with its message and stack as fields. */
  error(message: string, error?: unknown, fields: LogFields = {}) {
    const details: LogFields =
      error instanceof Error
        ? { error: error.message, stack: error.stack }
        : error === undefined
          ? {}
          : { error: String(error) };
    this.log('error', message, { ...details, ...fields });
  }

  private log(level: LogLevel, message: string, fields: LogFields) {
    // Cloudflare's own logs (wrangler tail, Workers Logs) keep them too, and they're all there is without o11y.
    console[level](message, { ...this.requestFields, ...fields });
    if (!this.config) {
      return;
    }
    const line: { [key: string]: unknown } = {
      _time: new Date().toISOString(),
      _msg: message,
      level,
      ...this.requestFields,
      ...fields,
      // Last, so no field can claim another tenant or stream.
      ...this.config.identity,
    };
    this.lines.push(JSON.stringify(line));
  }

  async flush() {
    if (!this.config || this.lines.length === 0) {
      return;
    }
    const body = this.lines.join('\n') + '\n';
    this.lines = [];
    await send(this.config, `/insert/jsonline?_stream_fields=${LOG_STREAM_FIELDS}`, body, 'application/stream+json');
  }
}

/** Whether to log this request: rate is REQUEST_LOG_SAMPLE_RATE, a fraction (0.01 when unset or not a number in 0-1). */
export function sampleRequest(rate: string | undefined, random: () => number = Math.random): boolean {
  const parsed = rate === undefined || rate.trim() === '' ? NaN : Number(rate);
  const fraction = Number.isFinite(parsed) && parsed >= 0 && parsed <= 1 ? parsed : 0.01;
  return random() < fraction;
}
