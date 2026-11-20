import { O11yConfig, O11yLogger } from '../o11y';
import { logReaderConfig, probeConfig, probeIntervalMinutes } from './config';
import { Deps, runtimeDeps } from './deps';
import { runLogReader } from './log-reader';
import { probeDue, runProbe } from './probe';

/**
 * The cron's Bunny jobs for the run scheduled at scheduledTime: the log reader every minute, the probe every
 * BUNNY_PROBE_INTERVAL_MINUTES. Each is skipped (and says so in its runs metric) without its settings. Never rejects.
 *
 * Per run the log reader makes one lag query and at most 2 × MAX_PAGES (+ retries) page requests to the Logging API (3
 * requests in all at today's volume), REQUEST_SPACING_MS apart, and two o11y POSTs; a probe run one TileJSON read, 2
 * measurements, up to ~90 polls and two POSTs. Both stay far under a scheduled run's subrequest, CPU and 15-minute wall
 * time limits.
 */
export async function runBunnyCron(
  env: WorkerEnv,
  o11y: O11yConfig,
  scheduledTime: number,
  deps: Deps = runtimeDeps,
): Promise<void> {
  const logger = new O11yLogger(o11y);
  const guard = (job: string) => (error: unknown) => {
    logger.error('Bunny cron job failed', error, { job });
  };
  const probe = probeConfig(env);
  const interval = probe?.intervalMinutes ?? probeIntervalMinutes(env.BUNNY_PROBE_INTERVAL_MINUTES);
  await Promise.all([
    runLogReader(logReaderConfig(env), o11y, logger, scheduledTime, env.REQUEST_LOG_SAMPLE_RATE, deps).catch(
      guard('bunny_logs'),
    ),
    probeDue(scheduledTime, interval)
      ? runProbe(probe, o11y, logger, scheduledTime, deps).catch(guard('bunny_probe'))
      : undefined,
  ]);
  await logger.flush();
}
