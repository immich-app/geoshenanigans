/**
 * Claims of the seeds running on this machine: one file per state dir in a per-user directory. Two seeds over
 * overlapping ranges of one zone and prefix, each with its own state dir, would both PUT the positions they share,
 * and a second PUT of the same bytes clears a file's replication for good. Seeds on other machines are not seen;
 * the per-chunk check before each chunk catches most of what they stored, but not a race on the same chunk.
 *
 * A seed that died (kill -9, a second Ctrl-C) leaves its claim, and PUTs it had in flight that the server may still
 * commit for a while (its status.json's settleBy). A seed over an overlapping range that DESCRIBEd one of those files
 * before then would read "missing" and PUT the same bytes again, so the claim stays until that time and every seed
 * that starts over its range meanwhile is told to wait for it, whatever started in between.
 */
import { createHash } from 'node:crypto';
import { mkdir, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';

export interface SeedClaim {
  pid: number;
  stateDir: string;
  zone: string;
  /** Canonical storage origin. */
  host: string;
  prefix: string;
  from: number;
  to: number;
  startedAt: string;
}

/** A seed on this machine that died over an overlapping range of the same zone, host and prefix. */
export interface StoppedSeed {
  pid: number;
  stateDir: string;
  /** When PUTs it had in flight are past the window in which the server may commit them; undefined if unknown. */
  settleBy?: number;
}

export interface RangeClaim {
  /** Drops the claim. */
  release: () => Promise<void>;
  /**
   * Seeds that died over part of the range whose PUTs may still be committed: wait until their settleBy (a full
   * commit window when unknown) before DESCRIBEing or PUTting a position.
   */
  stopped: StoppedSeed[];
}

/** `$XDG_STATE_HOME/bunny-seed`, else `~/.local/state/bunny-seed`. */
export function defaultClaimsDir(env = process.env): string {
  return join(env.XDG_STATE_HOME || join(homedir(), '.local', 'state'), 'bunny-seed');
}

export function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === 'EPERM';
  }
}

/**
 * Records that `claim.stateDir` seeds its range, or fails if a live seed with another state dir covers part of it.
 * `settleBy(stateDir)` reads when the PUTs of the seed that used a state dir may last be committed (its status.json),
 * undefined if it can't. A dead process's claim is removed once that time has passed or when it can't be read (then
 * only the seeds that see the claim wait); until then it stays, and those over an overlapping range are `stopped`.
 */
export async function claimRange(
  dir: string,
  claim: Omit<SeedClaim, 'pid' | 'startedAt'>,
  settleBy: (stateDir: string) => Promise<number | undefined>,
): Promise<RangeClaim> {
  await mkdir(dir, { recursive: true });
  const name = `${createHash('sha256').update(claim.stateDir).digest('hex').slice(0, 16)}.json`;
  const own = join(dir, name);
  const stopped = new Map<string, StoppedSeed>();
  const overlapping = (other: SeedClaim) =>
    other.from < claim.to &&
    claim.from < other.to &&
    other.zone === claim.zone &&
    other.host === claim.host &&
    other.prefix === claim.prefix;
  const conflict = async (): Promise<{ file: string; other: SeedClaim } | undefined> => {
    for (const entry of await readdir(dir)) {
      if (entry === name || !entry.endsWith('.json')) {
        continue;
      }
      const file = join(dir, entry);
      let other: SeedClaim;
      try {
        other = JSON.parse(await readFile(file, 'utf8')) as SeedClaim;
      } catch {
        continue;
      }
      if (!Number.isInteger(other.pid)) {
        await rm(file, { force: true });
        continue;
      }
      if (!isAlive(other.pid)) {
        const until = typeof other.stateDir === 'string' ? await settleBy(other.stateDir) : undefined;
        const settling = until !== undefined && until > Date.now();
        if (!settling) {
          await rm(file, { force: true });
        }
        if (overlapping(other) && (settling || until === undefined)) {
          stopped.set(file, { pid: other.pid, stateDir: other.stateDir, settleBy: until });
        }
        continue;
      }
      if (overlapping(other)) {
        return { file, other };
      }
    }
    return undefined;
  };
  const refuse = ({ file, other }: { file: string; other: SeedClaim }) =>
    new Error(
      `${other.stateDir} (process ${other.pid}) is seeding [${other.from}, ${other.to}) of ${other.zone}/` +
        `${other.prefix}/, which overlaps [${claim.from}, ${claim.to}): both would PUT the positions they share, ` +
        'and a second PUT of the same bytes clears their replication. Let it finish or stop it, resume it instead, ' +
        `or seed a range that does not overlap (its claim: ${file})`,
    );

  const before = await conflict();
  if (before) {
    throw refuse(before);
  }
  await writeFile(own, JSON.stringify({ ...claim, pid: process.pid, startedAt: new Date().toISOString() }) + '\n');
  // Two seeds that started at the same moment both see the other here, and both stop.
  const after = await conflict();
  if (after) {
    await rm(own, { force: true });
    throw refuse(after);
  }
  return { release: () => rm(own, { force: true }), stopped: [...stopped.values()] };
}
