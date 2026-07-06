/**
 * A simulated agent, run as its own OS process by the concurrency simulations.
 * It races every other worker to claim and complete tasks over one shared file
 * bus, talking to nothing but the folder.
 *
 * Invoked as: node --import tsx test/helpers/sim-worker.ts <dir> <agentId>
 * Env:
 *   AGENT_BUS_CHAOS=<ms>      widen the in-critical-section race window
 *   AGENT_BUS_SEED=<int>      seed for deterministic task selection / jitter
 *   AGENT_BUS_LOCK_MODE=safe|broken   which lock the bus uses (default safe)
 *   AGENT_BUS_RETRY_MS=<ms>   lock acquire retry interval (default 25)
 *   AGENT_BUS_LOCK_TIMEOUT_MS=<ms>    lock acquire timeout (default 60000)
 */
import { FileBus } from '../../src/core/file-bus';
import { brokenLockAcquirer, mulberry32 } from './sim-support';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Deterministic per-agent seed: base seed mixed with a hash of the agent id. */
function agentSeed(base: number, agent: string): number {
  let h = base >>> 0;
  for (let i = 0; i < agent.length; i++) h = (Math.imul(h, 31) + agent.charCodeAt(i)) | 0;
  return h >>> 0;
}

async function run(dir: string, agent: string): Promise<void> {
  const seed = Number.parseInt(process.env.AGENT_BUS_SEED ?? '1', 10) || 1;
  const lockMode = process.env.AGENT_BUS_LOCK_MODE === 'broken' ? 'broken' : 'safe';
  const retryMs = Number.parseInt(process.env.AGENT_BUS_RETRY_MS ?? '25', 10) || 25;
  const timeoutMs = Number.parseInt(process.env.AGENT_BUS_LOCK_TIMEOUT_MS ?? '60000', 10) || 60000;

  const chaosMs = Number.parseInt(process.env.AGENT_BUS_CHAOS ?? '0', 10) || 0;
  const rand = mulberry32(agentSeed(seed, agent));
  const jitter = (n: number) => Math.floor(rand() * n);

  // Stagger worker startup so all processes have a chance to begin before
  // the first one claims everything (especially needed on fast CI runners).
  if (chaosMs > 0) await sleep(jitter(chaosMs));

  const bus = new FileBus({
    dir,
    lock: { timeoutMs, retryMs },
    ...(lockMode === 'broken' ? { lockAcquirer: brokenLockAcquirer } : {}),
  });

  let idleRounds = 0;
  let claimed = 0;
  // Safety cap so a broken-lock run (corrupted state) can never hang forever.
  let iterations = 0;
  const maxIterations = 200_000;

  for (; iterations < maxIterations; iterations++) {
    const open = await bus.getTasks({ state: 'open' });
    if (open.length === 0) {
      idleRounds += 1;
      if (idleRounds >= 3) break; // no work left; everything is claimed/done
      await sleep(3 + jitter(8));
      continue;
    }
    idleRounds = 0;

    // Pick a random open task so workers actively collide.
    const task = open[jitter(open.length)]!;
    const res = await bus.claim(task.id, agent);
    if (res.ok) {
      claimed += 1;
      if (jitter(4) === 0) await sleep(jitter(3)); // occasionally "work"
      try {
        await bus.complete(task.id, agent, { by: agent });
      } catch {
        // Under a broken lock a double-claim makes us a non-owner here; ignore so
        // the worker survives and the corrupted log remains for the checker.
      }
    }
  }
  // Final heartbeat so the orchestrator can see each worker's tally.
  try {
    await bus.post({ type: 'status.update', agent, text: `exiting; claimed=${claimed}` });
  } catch {
    // broken lock may reject; not essential.
  }
}

const [dir, agent] = process.argv.slice(2);
if (!dir || !agent) {
  console.error('usage: sim-worker <dir> <agentId>');
  process.exit(2);
}

run(dir, agent)
  .then(() => process.exit(0))
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
