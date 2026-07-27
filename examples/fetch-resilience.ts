/**
 * Adding resilience to plain `fetch` with async combinators.
 *
 * Native `fetch` provides no retry, no request timeout, no rate limiting, and no
 * caching; popular thin fetch clients (e.g. openapi-fetch) deliberately add none
 * either, leaving resilience to the caller. So a program that calls an API with
 * fetch is at the mercy of the network and the server. We reproduce that against a
 * local server we can make flaky and rate-limited, and show the combinators making
 * the same code robust.
 *
 * Note fetch's error model: an HTTP 429 or 5xx is a *resolved* Response, not a
 * thrown error, so `checkedFetch` throws on a retryable status to make withRetry
 * meaningful.
 *
 * Run:  npm run example:fetch
 */
import { createServer, type Server } from 'node:http';
import { AddressInfo } from 'node:net';
import { withRetry, withRateLimit, withTimeout } from '../src';
import assert from 'node:assert/strict';

interface ServerConfig {
  failFirst?: number; // per-path: fail this many initial requests with 503
  minIntervalMs?: number; // reject with 429 if requests arrive faster than this
}

async function startServer(cfg: ServerConfig): Promise<{ base: string; close: () => Promise<void> }> {
  const attempts = new Map<string, number>();
  let lastAccepted = 0;
  const server: Server = createServer((req, res) => {
    const now = Date.now();
    if (cfg.minIntervalMs && now - lastAccepted < cfg.minIntervalMs) {
      res.writeHead(429);
      res.end('Too Many Requests');
      return;
    }
    lastAccepted = now;
    const path = req.url ?? '/';
    const n = (attempts.get(path) ?? 0) + 1;
    attempts.set(path, n);
    if (cfg.failFirst && n <= cfg.failFirst) {
      res.writeHead(503);
      res.end('Service Unavailable');
      return;
    }
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ path, attempt: n }));
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  return {
    base: `http://127.0.0.1:${port}`,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

// fetch resolves on 4xx/5xx, so turn a retryable status into a thrown error.
async function checkedFetch(url: string): Promise<Response> {
  const res = await fetch(url);
  if (res.status === 429 || res.status >= 500) throw new Error(`HTTP ${res.status}`);
  return res;
}

const IDS = [1, 2, 3, 4, 5];
type Op = (id: number) => Promise<unknown>;

// Run all requests against a FRESH server (so no state carries between runs),
// applying `wrap` to the per-item operation, and count how many succeed.
async function measure(cfg: ServerConfig, wrap: (op: Op) => Op): Promise<number> {
  const { base, close } = await startServer(cfg);
  const getItem: Op = (id) => checkedFetch(`${base}/items/${id}`).then((r) => (r as Response).json());
  const op = wrap(getItem); // build the wrapper ONCE; its state (e.g. pacing) is shared across calls
  try {
    const results = await Promise.allSettled(IDS.map((id) => op(id)));
    return results.filter((r) => r.status === 'fulfilled').length;
  } finally {
    await close();
  }
}

const bare = (op: Op): Op => op;

async function main(): Promise<void> {
  const total = IDS.length;

  // 1. Transient failures: each resource fails its first two requests, then
  //    succeeds. A single bare attempt fails; withRetry recovers.
  const t1 = await measure({ failFirst: 2 }, bare);
  const t2 = await measure({ failFirst: 2 }, (op) => withRetry(op, 3, { delayMs: 20 }));
  console.log(`1. transient 503s:   bare ${t1}/${total} ok,  withRetry ${t2}/${total} ok`);
  assert.ok(t1 < total, 'bare fetch should fail on transient errors');
  assert.equal(t2, total, 'withRetry should recover from transient errors');

  // 2. Rate limiting: the server rejects requests that arrive too fast. A
  //    concurrent burst is throttled; withRateLimit paces the calls.
  const r1 = await measure({ minIntervalMs: 30 }, bare);
  const r2 = await measure({ minIntervalMs: 30 }, (op) => withRateLimit(op, 100));
  console.log(`2. rate limit (429): bare ${r1}/${total} ok,  withRateLimit ${r2}/${total} ok`);
  assert.ok(r1 < total, 'a bare concurrent burst should be rate-limited');
  assert.equal(r2, total, 'withRateLimit should pace requests under the limit');

  // 3. Composition: a server that is BOTH flaky and rate-limited. The composed
  //    stack paces (avoiding 429), retries (surviving 503), and bounds each
  //    attempt with a deadline.
  const cfg = { failFirst: 1, minIntervalMs: 30 };
  const c1 = await measure(cfg, bare);
  const c2 = await measure(cfg, (op) =>
    withRetry(withRateLimit(withTimeout(op, 1000), 100), 3, { delayMs: 20 }),
  );
  console.log(`3. flaky + limited:  bare ${c1}/${total} ok,  composed  ${c2}/${total} ok`);
  assert.ok(c1 < total, 'bare fetch should fail against a flaky, rate-limited server');
  assert.equal(c2, total, 'the composed stack should make every call succeed');

  console.log('\nOK: the combinators made plain fetch resilient against a flaky, rate-limited API.');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
