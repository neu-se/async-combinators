import { withRetry } from '../src/withRetry';
import { withTimeout } from '../src/withTimeout';
import { withFallback } from '../src/withFallback';
import { withCache } from '../src/withCache';
import { withLock } from '../src/withLock';
import { Lock } from '../src/lock';
import { deferred, stall } from './helpers';

// Cross-combinator tests: each isolated `withX` is already covered by its own
// suite, so these assert only the behavior that *emerges from combining* them —
// error propagation across layers and order-dependent semantics.
describe('composition', () => {
  beforeEach(() => {
    jest.clearAllTimers();
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.runOnlyPendingTimers();
    jest.useRealTimers();
  });

  it('withRetry(withTimeout(fn)): a timeout triggers a retry', async () => {
    // Slow on the first attempt (exceeds the timeout), fast on the retry.
    let calls = 0;
    const fn = jest.fn(async () => {
      calls++;
      if (calls === 1) {
        // Keep attempt 1 pending so withTimeout drives the retry path.
        await stall();
        return 'late';
      }
      return 'ok';
    });

    const wrapped = withRetry(withTimeout(fn, 20), 3);

    // The first attempt times out; withRetry catches that timeout error and
    // retries, and the second attempt succeeds.
    // Set up the success assertion before advancing virtual time.
    const out = wrapped();
    // Advance 20ms so the first attempt times out and retry can proceed.
    await jest.advanceTimersByTimeAsync(20);
    // Enforce the final outcome after retry completes.
    expect(await out).toBe('ok');
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it('withFallback(withRetry(fn)): the fallback fires only after retries are exhausted', async () => {
    const fn = jest.fn(async () => {
      throw new Error('always fails');
    });
    const fallback = jest.fn(async () => 'fallback-result');

    const wrapped = withFallback(withRetry(fn, 3), fallback);

    expect(await wrapped()).toBe('fallback-result');
    expect(fn).toHaveBeenCalledTimes(3); // all retries used up first
    expect(fallback).toHaveBeenCalledTimes(1); // then, and only then, the fallback
  });

  it('withCache(withRetry(fn)): the retried success is cached, so a repeat call skips retrying', async () => {
    // Fails once, then succeeds.
    let calls = 0;
    const fn = jest.fn(async (_k: string) => {
      calls++;
      if (calls === 1) throw new Error('transient');
      return 'value';
    });

    const wrapped = withCache(withRetry(fn, 3));

    expect(await wrapped('k')).toBe('value'); // miss → fail once, retry, succeed
    expect(await wrapped('k')).toBe('value'); // hit → served from cache
    expect(fn).toHaveBeenCalledTimes(2); // 1 fail + 1 success; the repeat call added nothing
  });

  it('composition order matters: cache outside vs inside retry', async () => {
    // Each fn fails on its first call and succeeds on its second.
    const makeFn = () => {
      let calls = 0;
      return jest.fn(async (_k: string) => {
        calls++;
        if (calls === 1) throw new Error('transient');
        return 'value';
      });
    };

    // Cache OUTSIDE retry: retry sees fn directly, so the transient failure is
    // retried and the eventual success is what gets cached.
    const fnOutside = makeFn();
    const cacheOutside = withCache(withRetry(fnOutside, 3));
    expect(await cacheOutside('k')).toBe('value');
    expect(fnOutside).toHaveBeenCalledTimes(2); // failed once, retried, succeeded

    // Cache INSIDE retry (caching errors): the first failure is cached, so every
    // subsequent retry replays that SAME cached error instead of calling fn —
    // retry is defeated and the call fails even though fn would have succeeded.
    const fnInside = makeFn();
    const cacheInside = withRetry(withCache(fnInside, { cacheErrors: true }), 3);
    await expect(cacheInside('k')).rejects.toThrow('transient');
    expect(fnInside).toHaveBeenCalledTimes(1); // failure cached → retries replay it
  });

  it('full resilience stack: withFallback(withRetry(withTimeout(fn))) degrades gracefully', async () => {
    // Always slower than the timeout, so every attempt times out.
    const fn = jest.fn(async () => {
      // Keep each attempt pending so withTimeout consistently drives fallback flow.
      await stall();
      return 'slow';
    });
    const fallback = jest.fn(async () => 'fallback');

    const robust = withFallback(withRetry(withTimeout(fn, 20), 3), fallback);

    // Each of the 3 attempts times out → retries exhausted → fallback serves.
    // Set up the composed call before advancing virtual time.
    const out = robust();
    // Advance 60ms total to cover 3 timeout attempts at 20ms each.
    await jest.advanceTimersByTimeAsync(60);
    // Enforce the final fallback outcome once retries are exhausted.
    expect(await out).toBe('fallback');
    expect(fn).toHaveBeenCalledTimes(3);
    expect(fallback).toHaveBeenCalledTimes(1);
  });

  it('withTimeout(withLock(fn)): the timeout bounds the wait for a contended lock', async () => {
    const lock = new Lock();
    const gate = deferred();

    // A holds the lock for a while.
    const holdIt = withLock(async () => {
      // Keep the holder active until the test explicitly releases this gate.
      await gate.promise;
    }, lock);
    const holding = holdIt();

    // B wants the same lock but only waits 20ms. Because withTimeout wraps
    // withLock, the timeout covers the lock-acquisition wait — so B abandons the
    // wait rather than blocking for the full hold.
    //
    // Caveat: withTimeout's *internal* timeout can't cancel the queued acquire — it
    // doesn't abort fn — so B still acquires and runs once A releases (wasted work).
    // A caller-supplied { signal } avoids that; see the next test. This asserts only
    // the property withTimeout does provide: the wait is bounded.
    const b = withTimeout(withLock(async () => 'b-ran', lock), 20);
    // Set up the timeout assertion before advancing virtual time.
    const timedOut = expect(b()).rejects.toThrow('Operation timed out');
    // Advance 20ms so B's timeout fires while A still holds the lock.
    await jest.advanceTimersByTimeAsync(20);
    // Enforce the timeout outcome.
    await timedOut;

    gate.resolve();

    await holding; // let A finish
  });

  it('withLock + a caller AbortSignal.timeout: the bounded acquire is dequeued and never runs (no wasted work)', async () => {
    const lock = new Lock();
    const shouldNotRun = jest.fn();
    const gate = deferred();

    // A holds the lock for 50ms.
    const holdIt = withLock(async () => {
      // Keep the holder active until the test explicitly releases this gate.
      await gate.promise;
    }, lock);
    const holding = holdIt();
    // Advance 1ms so A acquires under fake timers.
    await jest.advanceTimersByTimeAsync(1);

    // B bounds its acquire wait with a 20ms timeout signal. The signal is carried in
    // { signal } and withLock forwards it to the lock, so when it fires while B is
    // queued, B is REMOVED from the wait queue and rejects — unlike
    // withTimeout(withLock), it never acquires once A releases.
    const guarded = withLock(async (_opts?: { signal?: AbortSignal }) => { shouldNotRun(); return 'b-ran'; }, lock);
    // Set up the timeout-signal assertion before advancing virtual time.
    const aborted = expect(guarded({ signal: AbortSignal.timeout(20) }))
      .rejects.toHaveProperty('name', 'TimeoutError');
    // Advance 20ms so B's acquire signal times out while queued.
    await jest.advanceTimersByTimeAsync(20);
    // Enforce the timeout-signal rejection.
    await aborted;

    gate.resolve();
    await holding;  // A finishes and releases
    // Give a (wrongly) still-queued B a chance to run.
    await jest.advanceTimersByTimeAsync(5);
    expect(shouldNotRun).not.toHaveBeenCalled(); // dequeued on timeout — no wasted work
    expect(lock.isLocked()).toBe(false);
  });

  // Order matters for withLock + withRetry: whether the lock is held across the
  // whole retry sequence or acquired/released per attempt. Observable only now
  // that backoff makes the inter-attempt window a real (timed) gap.
  describe('withLock + withRetry backoff: lock held across retries vs released between', () => {
    it('withLock(withRetry(fn)): holds the lock across the whole retry + backoff sequence', async () => {
      const lock = new Lock();
      const fn = jest.fn()
        .mockRejectedValueOnce(new Error('f1'))
        .mockResolvedValue('ok');
      const guarded = withLock(withRetry(fn, 2, { delayMs: 100, backoff: 'fixed' }), lock);

      const p = guarded();
      await jest.advanceTimersByTimeAsync(0); // attempt 1 fails; now waiting in the still-pending backoff
      expect(fn).toHaveBeenCalledTimes(1);
      expect(lock.isLocked()).toBe(true); // still held during the backoff — no other caller can acquire

      await jest.advanceTimersByTimeAsync(100); // backoff → attempt 2 succeeds → release
      expect(fn).toHaveBeenCalledTimes(2);
      await expect(p).resolves.toBe('ok');
      expect(lock.isLocked()).toBe(false);
    });

    it('withRetry(withLock(fn)): releases the lock between attempts (acquired per attempt)', async () => {
      const lock = new Lock();
      const fn = jest.fn()
        .mockRejectedValueOnce(new Error('f1'))
        .mockResolvedValue('ok');
      const retried = withRetry(withLock(fn, lock), 2, { delayMs: 100, backoff: 'fixed' });

      const p = retried();
      await jest.advanceTimersByTimeAsync(0); // attempt 1 acquires, fails, RELEASES; now waiting in the still-pending backoff
      expect(fn).toHaveBeenCalledTimes(1);
      expect(lock.isLocked()).toBe(false); // free during the backoff — an independent caller could acquire

      await jest.advanceTimersByTimeAsync(100); // attempt 2 re-acquires, succeeds, releases
      expect(fn).toHaveBeenCalledTimes(2);
      await expect(p).resolves.toBe('ok');
      expect(lock.isLocked()).toBe(false);
    });
  });
});
