/**
 * Adding resilience to async streams with async-combinators.
 *
 * Stream combinators share names with the promise family but operate on
 * functions returning AsyncIterable<T>. Three behaviours are unique to the
 * streaming model:
 *
 * - withRetry retries seamlessly when failure happens *before the first item*.
 *   Nothing has been delivered, so a fresh attempt is transparent. After items
 *   have been seen, retry is only possible for deterministic (resumable) sources.
 * - withTimeout imposes a *per-item* deadline, not a total-stream wall clock.
 *   A slow consumer between pulls never trips it; only a slow source does.
 * - withRateLimit gates how often streams *start*; items within a stream flow
 *   at the source's own pace.
 *
 * Run:  npm run example:stream
 */
import { withRetry, withTimeout, withRateLimit, TimeoutError } from '../src/stream';
import assert from 'node:assert/strict';

async function collect<T>(src: AsyncIterable<T>): Promise<{ items: T[]; error?: unknown }> {
  const items: T[] = [];
  try {
    for await (const item of src) items.push(item);
    return { items };
  } catch (error) {
    return { items, error };
  }
}

async function main(): Promise<void> {
  // ── 1. Pre-data retry ─────────────────────────────────────────────────────
  //
  // A source that throws before yielding its first item. withRetry retries
  // transparently because nothing has been delivered yet. This differs from
  // a mid-stream failure: once items have been seen, retry is only safe for
  // deterministic (resumable) sources. The promise-family withRetry has no
  // such distinction — streams fail at a *point*, promises don't.
  {
    let attempts = 0;
    const source = async function* () {
      if (++attempts <= 2) throw new Error('source unavailable');
      yield 1; yield 2; yield 3;
    };

    const bare    = await collect(source());               // attempt 1: throws
    attempts = 0;
    const retried = await collect(withRetry(source, 3)()); // attempts 1,2 throw; 3 succeeds

    console.log(`1. pre-data failure:  bare  ${bare.items.length} items, error="${(bare.error as Error).message}"`);
    console.log(`                      retry  ${retried.items.length} items, ok`);
    assert.ok(bare.error, 'bare call should throw before yielding any item');
    assert.equal(bare.items.length, 0);
    assert.deepEqual(retried.items, [1, 2, 3]);
  }

  // ── 2. Per-item deadline ──────────────────────────────────────────────────
  //
  // The source yields two items quickly, then stalls 200ms before the third.
  // withTimeout(fn, 50) gives each pull a 50ms deadline: the first two items
  // arrive well within it, but the stalled pull fires a TimeoutError. The timer
  // runs only while a pull is outstanding, so a slow *consumer* between items
  // never trips it — only a slow *source* does.
  {
    const source = async function* () {
      yield 1;
      yield 2;
      await new Promise<void>(r => setTimeout(r, 200)); // stall
      yield 3; // never reached with a 50ms per-pull deadline
    };

    const bare  = await collect(source());               // [1, 2, 3] after ~200ms
    const timed = await collect(withTimeout(source, 50)()); // [1, 2] then TimeoutError

    console.log(`2. per-item deadline: bare  ${bare.items.length} items (slow)`);
    console.log(`                      timed ${timed.items.length} items then ${(timed.error as Error)?.constructor.name}`);
    assert.deepEqual(bare.items, [1, 2, 3]);
    assert.deepEqual(timed.items, [1, 2]);
    assert.ok(timed.error instanceof TimeoutError);
  }

  // ── 3. Rate-limiting stream starts ────────────────────────────────────────
  //
  // A source that enforces a minimum gap between starts. Four concurrent bare
  // starts all land within milliseconds of each other, so most fail. withRateLimit
  // spaces starts 100ms apart so the 40ms threshold is never crossed. Note that
  // rate-limiting here gates the *start* of each stream, not the rate of items
  // within a stream.
  {
    let lastStart = -Infinity;
    const source = async function* () {
      const now = Date.now();
      if (now - lastStart < 40) throw new Error(`started too soon`);
      lastStart = now;
      yield 1;
    };

    const N = 4;
    const bareResults = await Promise.allSettled(
      Array.from({ length: N }, () => collect(source()))
    );
    lastStart = -Infinity;
    const paced = withRateLimit(source, 100); // space starts 100ms apart
    const pacedResults = await Promise.allSettled(
      Array.from({ length: N }, () => collect(paced()))
    );

    const ok = (rs: PromiseSettledResult<{ items: unknown[]; error?: unknown }>[]) =>
      rs.filter(r => r.status === 'fulfilled' && !(r.value as any).error).length;

    console.log(`3. rate-limit starts: bare  ${ok(bareResults)}/${N} ok (concurrent burst)`);
    console.log(`                      paced ${ok(pacedResults)}/${N} ok (100ms apart)`);
    assert.ok(ok(bareResults) < N, 'concurrent burst should exceed the 40ms minimum-gap check');
    assert.equal(ok(pacedResults), N, 'withRateLimit should space starts so all streams succeed');
  }

  // ── 4. Composition ────────────────────────────────────────────────────────
  //
  // All three combinators preserve the same (...args) => AsyncIterable<T>
  // signature, so they nest as naturally as the promise counterparts. Here a
  // source fails before its first item on the first attempt (transient error),
  // then on the second attempt yields two items with a 50ms gap between them.
  // withTimeout bounds each pull; withRetry handles the pre-data failure.
  {
    let attempts = 0;
    const source = async function* () {
      if (++attempts === 1) throw new Error('transient');
      yield 'a';
      await new Promise<void>(r => setTimeout(r, 50));
      yield 'b';
    };

    const composed = withRetry(withTimeout(source, 500), 3);
    const result = await collect(composed());

    console.log(`4. retry + timeout:   ${result.items.length} items (${result.items.join(', ')}), ok`);
    assert.deepEqual(result.items, ['a', 'b']);
  }

  console.log('\nOK: stream combinators added retry, per-item timeout, and paced starts to plain async generators.');
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
