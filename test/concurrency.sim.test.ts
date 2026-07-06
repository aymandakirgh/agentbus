import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { FileBus } from '../src/core/file-bus';
import type { Message } from '../src/core/types';

let dir: string;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'agent-bus-sim-'));
});

afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

const WORKER = join(process.cwd(), 'test', 'helpers', 'sim-worker.ts');

function spawnWorker(busDir: string, agent: string, chaosMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--import', 'tsx', WORKER, busDir, agent], {
      cwd: process.cwd(),
      env: { ...process.env, AGENT_BUS_CHAOS: String(chaosMs) },
      stdio: ['ignore', 'ignore', 'pipe'],
    });
    let err = '';
    child.stderr.on('data', (d) => (err += String(d)));
    child.on('error', reject);
    child.on('exit', (code) =>
      code === 0 ? resolve() : reject(new Error(`worker ${agent} exited ${code}: ${err}`)),
    );
  });
}

/** Core invariants every run of the bus must satisfy. */
function assertBusInvariants(msgs: Message[], taskIds: string[]): void {
  // G1 — total order: seq is 1..N, strictly increasing, gapless, unique.
  const seqs = msgs.map((m) => m.seq);
  expect(seqs).toEqual(Array.from({ length: msgs.length }, (_, i) => i + 1));
  expect(new Set(msgs.map((m) => m.id)).size).toBe(msgs.length); // unique ids

  // G2 — single claimer: exactly one task.claimed per task.
  const claims = msgs.filter((m) => m.type === 'task.claimed');
  const claimsByTask = new Map<string, string[]>();
  for (const c of claims) {
    const id = (c as { taskId: string }).taskId;
    claimsByTask.set(id, [...(claimsByTask.get(id) ?? []), c.agent]);
  }
  for (const id of taskIds) {
    expect(claimsByTask.get(id), `task ${id} claim count`).toHaveLength(1);
  }
  expect(claims).toHaveLength(taskIds.length);
}

describe('concurrency: in-process races', () => {
  it('exactly one of N concurrent claims on one task wins', async () => {
    const bus = await FileBus.init(dir);
    await bus.createTask({ title: 'the one', agent: 'lead', taskId: 't1' });

    const N = 25;
    const agents = Array.from(
      { length: N },
      () => new FileBus({ dir, criticalSectionDelayMs: 8 }),
    );
    const results = await Promise.all(agents.map((b, i) => b.claim('t1', `w${i}`)));

    const wins = results.filter((r) => r.ok);
    expect(wins).toHaveLength(1);
    expect(results.filter((r) => !r.ok).every((r) => !r.ok && r.reason === 'not_open')).toBe(true);
    expect((await bus.getTask('t1'))?.state).toBe('claimed');
  });

  it('K agents drain M tasks with each claimed exactly once and all completed', async () => {
    const bus = await FileBus.init(dir);
    const M = 30;
    const taskIds = Array.from({ length: M }, (_, i) => `t${i}`);
    for (const id of taskIds) await bus.createTask({ title: id, agent: 'lead', taskId: id });

    const K = 8;
    const agents = Array.from(
      { length: K },
      () => new FileBus({ dir, criticalSectionDelayMs: 2 }),
    );

    async function loop(b: FileBus, name: string): Promise<void> {
      for (;;) {
        const open = await b.getTasks({ state: 'open' });
        if (open.length === 0) break;
        const t = open[Math.floor(Math.random() * open.length)]!;
        const res = await b.claim(t.id, name);
        if (res.ok) await b.complete(t.id, name, { by: name });
      }
    }

    await Promise.all(agents.map((b, i) => loop(b, `w${i}`)));

    const tasks = await bus.getTasks();
    expect(tasks).toHaveLength(M);
    expect(tasks.every((t) => t.state === 'done')).toBe(true);
    assertBusInvariants(await bus.getMessages(), taskIds);
  });
});

describe('concurrency: multi-process simulation', () => {
  it(
    'N agents in separate OS processes coordinate via one folder with no double-claims',
    async () => {
      const bus = await FileBus.init(dir);
      const M = 40;
      const K = 6;
      const taskIds = Array.from({ length: M }, (_, i) => `t${i}`);
      for (const id of taskIds) {
        await bus.createTask({ title: `task ${id}`, agent: 'lead', taskId: id, priority: 'normal' });
      }

      // Spawn K real OS processes; each talks to nothing but the shared folder.
      await Promise.all(
        Array.from({ length: K }, (_, i) => spawnWorker(dir, `agent-${i}`, 100)),
      );

      const msgs = await bus.getMessages();
      assertBusInvariants(msgs, taskIds);

      // Eventual completion: every task is done, completed by its claimer.
      const tasks = await bus.getTasks();
      expect(tasks).toHaveLength(M);
      expect(tasks.every((t) => t.state === 'done')).toBe(true);

      const completes = msgs.filter((m) => m.type === 'task.completed');
      expect(completes).toHaveLength(M);
      const claimerOf = new Map(
        msgs
          .filter((m) => m.type === 'task.claimed')
          .map((m) => [(m as { taskId: string }).taskId, m.agent]),
      );
      for (const c of completes) {
        const id = (c as { taskId: string }).taskId;
        expect(c.agent, `task ${id} completed by its claimer`).toBe(claimerOf.get(id));
      }

      // Work was actually distributed across processes (not all by one worker).
      const distinctClaimers = new Set([...claimerOf.values()]);
      expect(distinctClaimers.size).toBeGreaterThan(1);
    },
    60_000,
  );
});
