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
 * Note fetch's error model: an HTTP 4xx or 5xx is a *resolved* Response, not a
 * thrown error, so `checkedFetch` throws on any error status (carrying it on an
 * `HttpError`) to make `withRetry` -- and a classifying `shouldRetry` -- meaningful.
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
  invalidId?: number; // this id always 400s -- a permanent, unretryable failure
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
    if (cfg.invalidId !== undefined && path === `/items/${cfg.invalidId}`) {
      res.writeHead(400); // permanent client error -- retrying never helps
      res.end('Bad Request');
      return;
    }
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

// fetch resolves on 4xx/5xx, so turn an error status into a thrown error --
// carrying the status so a shouldRetry predicate can classify it.
class HttpError extends Error {
  constructor(public readonly status: number) {
    super(`HTTP ${status}`);
    this.name = 'HttpError';
  }
}

async function checkedFetch(url: string): Promise<Response> {
  const res = await fetch(url);
  if (res.status >= 400) throw new HttpError(res.status);
  return res;
}

// 429 and 5xx are transient (retrying can help); any other 4xx is the client's
// fault and permanent -- retrying just wastes attempts and time.
const isRetryableStatus = (err: unknown): boolean =>
  !(err instanceof HttpError) || err.status === 429 || err.status >= 500;

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

// Same as measure(), but also reports how many underlying fetch attempts were
// made in total -- to show a classifying shouldRetry gives up fast instead of
// burning through maxAttempts on an error that can never succeed.
async function measureWithAttempts(
  cfg: ServerConfig,
  wrap: (op: Op) => Op,
): Promise<{ ok: number; totalAttempts: number }> {
  const { base, close } = await startServer(cfg);
  let totalAttempts = 0;
  const getItem: Op = (id) => {
    totalAttempts++;
    return checkedFetch(`${base}/items/${id}`).then((r) => (r as Response).json());
  };
  const op = wrap(getItem);
  try {
    const results = await Promise.allSettled(IDS.map((id) => op(id)));
    return { ok: results.filter((r) => r.status === 'fulfilled').length, totalAttempts };
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

  // 4. Unretryable errors: id 3 is permanently invalid (HTTP 400) -- unlike the
  //    transient 503s in scenario 1, no amount of retrying fixes it. Without
  //    classification, withRetry burns through all 3 attempts on it anyway
  //    before giving up; a shouldRetry predicate recognizes it as permanent and
  //    fails fast after a single attempt, saving the wasted retries.
  const cfg4 = { failFirst: 1, invalidId: 3 };
  const u1 = await measureWithAttempts(cfg4, (op) => withRetry(op, 3, { delayMs: 20 }));
  const u2 = await measureWithAttempts(cfg4, (op) =>
    withRetry(op, 3, { delayMs: 20, shouldRetry: isRetryableStatus }),
  );
  console.log(
    `4. unretryable 400:  bare withRetry ${u1.ok}/${total} ok in ${u1.totalAttempts} fetches, ` +
    `shouldRetry ${u2.ok}/${total} ok in ${u2.totalAttempts} fetches`,
  );
  assert.equal(u1.ok, u2.ok, 'both should recover the transient 503 the same way (id 3 fails either way)');
  assert.ok(u2.totalAttempts < u1.totalAttempts, 'shouldRetry should avoid wasting attempts on the unretryable 400');

  console.log('\nOK: the combinators made plain fetch resilient against a flaky, rate-limited API.');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
