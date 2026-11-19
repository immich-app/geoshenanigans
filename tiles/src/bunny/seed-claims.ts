/**
 * Claims of the seeds running on this machine: one file per state dir in a per-user directory. Two seeds over
 * overlapping ranges of one zone and prefix, each with its own state dir, would both PUT the positions they share,
 * and a second PUT of the same bytes clears a file's replication for good. Seeds on other machines are not seen;
 * the per-chunk check before each chunk catches most of what they stored, but not a race on the same chunk.
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
 * Claims of dead processes are removed. Resolves to the function that drops the claim.
 */
export async function claimRange(
  dir: string,
  claim: Omit<SeedClaim, 'pid' | 'startedAt'>,
): Promise<() => Promise<void>> {
  await mkdir(dir, { recursive: true });
  const name = `${createHash('sha256').update(claim.stateDir).digest('hex').slice(0, 16)}.json`;
  const own = join(dir, name);
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
      if (!Number.isInteger(other.pid) || !isAlive(other.pid)) {
        await rm(file, { force: true });
        continue;
      }
      const overlaps = other.from < claim.to && claim.from < other.to;
      if (overlaps && other.zone === claim.zone && other.host === claim.host && other.prefix === claim.prefix) {
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
  return () => rm(own, { force: true });
}
