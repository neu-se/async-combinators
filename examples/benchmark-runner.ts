/**
 * Benchmark runner with withMaxConcurrency.
 *
 * This example assumes that an evaluation API can only run a bounded number of
 * benchmarks simultaneously.
 * Firing all evaluations concurrently overwhelms the server and drops requests;
 * withMaxConcurrency limits the number of concurrent evaluations to a fixed number, 
 * so that every subject is scored without overwhelming the server.
 *
 * Run:  npm run example:bench
 */
import { createServer, type Server } from 'node:http';
import { AddressInfo } from 'node:net';
import { withMaxConcurrency } from '../src';
import assert from 'node:assert/strict';

const CAPACITY = 3;  // server handles at most this many concurrent evaluations
const EVAL_MS  = 30; // simulated evaluation time per subject

const SUBJECTS = [
  'model-a', 'model-b', 'model-c', 'model-d',
  'model-e', 'model-f', 'model-g', 'model-h',
];

// ---- simulated evaluation server ----

async function startServer(): Promise<{ base: string; close: () => Promise<void> }> {
  let active = 0;
  const timers = new Set<NodeJS.Timeout>();

  const server: Server = createServer((req, res) => {
    if (active >= CAPACITY) {
      res.writeHead(503);
      res.end('Server at capacity');
      return;
    }
    active++;
    const t = setTimeout(() => {
      timers.delete(t);
      active--;
      const subject = (req.url ?? '/unknown').slice(1);
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ subject, score: Math.round(Math.random() * 100) / 100 }));
    }, EVAL_MS);
    timers.add(t);
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  return {
    base: `http://127.0.0.1:${port}`,
    close: () => {
      for (const t of timers) clearTimeout(t);
      return new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

// ---- evaluation client ----

interface EvalResult { subject: string; score: number }

async function evaluate(base: string, subject: string): Promise<EvalResult> {
  const res = await fetch(`${base}/${subject}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json() as Promise<EvalResult>;
}

// ---- benchmark runner ----

async function run(
  subjects: string[],
  evalFn: (subject: string) => Promise<EvalResult>,
): Promise<{ succeeded: number; failed: number }> {
  const results = await Promise.allSettled(subjects.map(evalFn));
  return {
    succeeded: results.filter(r => r.status === 'fulfilled').length,
    failed:    results.filter(r => r.status === 'rejected').length,
  };
}

// ---- demo ----

async function main(): Promise<void> {
  const total = SUBJECTS.length;

  // 1. Bare: fire all evaluations concurrently. The server rejects anything
  //    beyond CAPACITY, so some subjects never get scored.
  const { base: base1, close: close1 } = await startServer();
  const bare = await run(SUBJECTS, subject => evaluate(base1, subject));
  await close1();

  console.log(`1. bare (${total} subjects, server capacity ${CAPACITY}):`);
  console.log(`   ${bare.succeeded} scored, ${bare.failed} dropped`);
  assert.ok(bare.failed > 0, 'bare burst should overflow the server');

  // 2. withMaxConcurrency: at most CAPACITY evaluations are in flight at once.
  //    The server stays fully saturated but never overflows — every subject is scored.
  const { base: base2, close: close2 } = await startServer();
  const limited = withMaxConcurrency(
    (subject: string) => evaluate(base2, subject),
    CAPACITY,
  );
  const bounded = await run(SUBJECTS, limited);
  await close2();

  console.log(`2. withMaxConcurrency(${CAPACITY}):`);
  console.log(`   ${bounded.succeeded} scored, ${bounded.failed} dropped`);
  assert.equal(bounded.failed, 0, 'bounded runner should score every subject');

  console.log('\nOK: withMaxConcurrency saturated the server without overwhelming it.');
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
