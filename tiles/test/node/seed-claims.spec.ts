import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { afterEach, describe, expect, it } from 'vitest';
import { claimRange, RangeClaim, SeedClaim } from '../../src/bunny/seed-claims';
import { scratchDir } from './upload-fixture';

let cleanup: (() => Promise<void>)[] = [];

afterEach(async () => {
  for (const step of cleanup.reverse()) {
    await step();
  }
  cleanup = [];
});

const range = { zone: 'test-zone', host: 'https://storage.example', prefix: 'itest', from: 0, to: 1_000 };
const claimName = (stateDir: string) => `${createHash('sha256').update(stateDir).digest('hex').slice(0, 16)}.json`;

describe('claimRange', () => {
  it("leaves the claim of a seed resumed under a dead seed's state dir while another seed removes the dead claim", async () => {
    const scratch = await scratchDir();
    cleanup.push(scratch.remove);
    const claims = join(scratch.dir, 'claims');
    const resumedDir = join(scratch.dir, 'a');
    const otherDir = join(scratch.dir, 'b');
    await mkdir(claims, { recursive: true });
    // The seed under a died, and its PUTs have settled: the next seed over the range removes its claim.
    const { pid } = spawnSync(process.execPath, ['-e', '']);
    const dead = { ...range, pid, stateDir: resumedDir, startedAt: new Date().toISOString() };
    await writeFile(join(claims, claimName(resumedDir)), JSON.stringify(dead));

    // A seed under b reads that claim and is held while it reads when the PUTs of the seed under a settle...
    let reading!: () => void;
    const read = new Promise<void>((resolve) => (reading = resolve));
    let go!: () => void;
    const held = new Promise<void>((resolve) => (go = resolve));
    const other = claimRange(claims, { ...range, stateDir: otherDir }, async () => {
      reading();
      await held;
      return Date.now() - 1_000;
    });
    await read;
    // ...while the seed under a is resumed, and takes its claim unless it waits for b to be done.
    const resumed = claimRange(claims, { ...range, stateDir: resumedDir }, async () => undefined);
    await Promise.race([resumed.catch(() => {}), sleep(500)]);
    go();

    const outcomes = await Promise.allSettled([other, resumed]);
    const holders = outcomes.flatMap((outcome) => (outcome.status === 'fulfilled' ? [outcome.value] : []));
    cleanup.push(...holders.map((claim) => claim.release));
    // Both holding the range would PUT the positions they share.
    expect(holders).toHaveLength(1);
    const refused = outcomes.find((outcome) => outcome.status === 'rejected') as PromiseRejectedResult;
    expect((refused.reason as Error).message).toMatch(/is seeding \[0, 1000\) of test-zone\/itest\/, which overlaps/);
    // The claim on disk is the winner's: the other seed sees it.
    const winnerDir = outcomes[0].status === 'fulfilled' ? otherDir : resumedDir;
    const files = (await readdir(claims)).filter((entry) => entry.endsWith('.json'));
    expect(files).toEqual([claimName(winnerDir)]);
    const onDisk = JSON.parse(await readFile(join(claims, files[0]), 'utf8')) as SeedClaim;
    expect(onDisk).toMatchObject({ pid: process.pid, stateDir: winnerDir });

    // Once the winner is done, the other seed takes the range: neither left the claims dir locked.
    await holders[0].release();
    const loserDir = winnerDir === otherDir ? resumedDir : otherDir;
    const again: RangeClaim = await claimRange(claims, { ...range, stateDir: loserDir }, async () => undefined);
    cleanup.push(again.release);
    expect(again.stopped).toEqual([]);
  });
});
