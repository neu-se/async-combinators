import { withRateLimit } from '../src/withRateLimit';
import { abortedSignal, expectAbortError } from './helpers';

describe('withRateLimit', () => {
  beforeEach(() => {
    jest.clearAllTimers();
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.runOnlyPendingTimers();
    jest.useRealTimers();
  });

  it('should run the first call immediately and delay the second within the interval', async () => {
    const fn = jest.fn();
    const rateLimitedFn = withRateLimit(fn, 1000);

    const setTimeoutSpy = jest.spyOn(global, 'setTimeout');

    // First call schedules no delay timer → runs immediately.
    const before1 = setTimeoutSpy.mock.calls.length;
    const first = rateLimitedFn();
    expect(setTimeoutSpy.mock.calls.length).toBe(before1); // confirm setTimeout was not called
    expect(fn).toHaveBeenCalledTimes(1); // confirm first call executed immediately
    await first;

    // Second call schedules one delay timer → deferred until the interval passes.
    const before2 = setTimeoutSpy.mock.calls.length;
    const second = rateLimitedFn();
    expect(setTimeoutSpy.mock.calls.length).toBe(before2 + 1); // confirm setTimeout was called once
    expect(fn).toHaveBeenCalledTimes(1); // not called yet

    // Fires only after the full interval elapses.
    jest.advanceTimersByTime(999);
    expect(fn).toHaveBeenCalledTimes(1); // fn still not called
    jest.advanceTimersByTime(1);
    await second;
    expect(fn).toHaveBeenCalledTimes(2); // fn now has been called

    setTimeoutSpy.mockRestore();
  });

  it('should space out a concurrent burst of calls', async () => {
    const fn = jest.fn();
    const rateLimitedFn = withRateLimit(fn, 1000);

    // Fire four calls concurrently (do not await individually).
    const promises = [
      rateLimitedFn(),
      rateLimitedFn(),
      rateLimitedFn(),
      rateLimitedFn()
    ];

    // Only the first runs immediately; the rest are spaced one full interval apart.
    expect(fn).toHaveBeenCalledTimes(1);

    // For each queued call: halfway through the interval it has NOT fired yet;
    // it fires only once the full interval elapses.
    await jest.advanceTimersByTimeAsync(500);
    expect(fn).toHaveBeenCalledTimes(1); // not yet
    await jest.advanceTimersByTimeAsync(500);
    expect(fn).toHaveBeenCalledTimes(2); // second call fires at 1000ms

    await jest.advanceTimersByTimeAsync(500);
    expect(fn).toHaveBeenCalledTimes(2); // not yet
    await jest.advanceTimersByTimeAsync(500);
    expect(fn).toHaveBeenCalledTimes(3); // third call fires at 2000ms

    await jest.advanceTimersByTimeAsync(500);
    expect(fn).toHaveBeenCalledTimes(3); // not yet
    await jest.advanceTimersByTimeAsync(500);
    await Promise.all(promises);
    expect(fn).toHaveBeenCalledTimes(4); // fourth call fires at 3000ms
  });

  it('should handle function that throws', async () => {
    const error = new Error('test error');
    const fn = jest.fn().mockRejectedValue(error);
    const rateLimitedFn = withRateLimit(fn, 1000);

    // First call runs immediately: fn is invoked synchronously, then rejects.
    const promise1 = rateLimitedFn();
    expect(fn).toHaveBeenCalledTimes(1); // called immediately
    await expect(promise1).rejects.toThrow('test error');

    // Rate limiting still applies after an error: the next call is delayed, so
    // fn is not invoked again until the interval elapses.
    const promise2 = rateLimitedFn();
    expect(fn).toHaveBeenCalledTimes(1); // not called yet — still delayed
    jest.advanceTimersByTime(500);
    expect(fn).toHaveBeenCalledTimes(1); // halfway through — still not fired
    jest.advanceTimersByTime(500);
    await expect(promise2).rejects.toThrow('test error');
    expect(fn).toHaveBeenCalledTimes(2); // fired only after the full interval
  });

  it('should maintain separate state for different wrapped functions', async () => {
    const fn1 = jest.fn();
    const fn2 = jest.fn();
    const rateLimitedFn1 = withRateLimit(fn1, 1000);
    const rateLimitedFn2 = withRateLimit(fn2, 1000);
    
    // Both should execute immediately (separate rate limiters)
    await rateLimitedFn1();
    await rateLimitedFn2();
    
    expect(fn1).toHaveBeenCalledTimes(1);
    expect(fn2).toHaveBeenCalledTimes(1);
    
    // Second calls should both be delayed
    const promise1 = rateLimitedFn1();
    const promise2 = rateLimitedFn2();
    
    jest.advanceTimersByTime(1000);
    await Promise.all([promise1, promise2]);
    
    expect(fn1).toHaveBeenCalledTimes(2);
    expect(fn2).toHaveBeenCalledTimes(2);
  });

  it('should apply the larger interval when nested (inner larger)', async () => {
    const fn = jest.fn();
    // inner 2000, outer 1000 → fn is spaced by the larger interval (2000).
    const limited = withRateLimit(withRateLimit(fn, 2000), 1000);

    limited();
    limited();
    expect(fn).toHaveBeenCalledTimes(1); // first immediate, second delayed

    // At 1000 the outer would allow the second call, but the inner (2000) blocks it.
    await jest.advanceTimersByTimeAsync(1000);
    expect(fn).toHaveBeenCalledTimes(1);

    // It fires only at the larger interval (2000).
    await jest.advanceTimersByTimeAsync(1000);
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it('should apply the larger interval when nested (outer larger)', async () => {
    const fn = jest.fn();
    // inner 1000, outer 2000 → fn is spaced by the larger interval (2000).
    const limited = withRateLimit(withRateLimit(fn, 1000), 2000);

    limited();
    limited();
    expect(fn).toHaveBeenCalledTimes(1); // first immediate, second delayed

    // At 1000 the inner interval has elapsed, but the outer (2000) hasn't yet
    // let the second call reach the inner limiter.
    await jest.advanceTimersByTimeAsync(1000);
    expect(fn).toHaveBeenCalledTimes(1);

    // It fires only at the larger interval (2000).
    await jest.advanceTimersByTimeAsync(1000);
    expect(fn).toHaveBeenCalledTimes(2);
  });

  describe('invalid intervalMs', () => {
    it('should throw for zero', () => {
      expect(() => withRateLimit(jest.fn(), 0)).toThrow(
        'intervalMs must be a positive integer'
      );
    });

    it('should throw for negative values', () => {
      expect(() => withRateLimit(jest.fn(), -1)).toThrow(
        'intervalMs must be a positive integer'
      );
    });

    it('should throw for non-integer values', () => {
      expect(() => withRateLimit(jest.fn(), 1.5)).toThrow(
        'intervalMs must be a positive integer'
      );
    });

    it('should throw for NaN', () => {
      expect(() => withRateLimit(jest.fn(), NaN)).toThrow(
        'intervalMs must be a positive integer'
      );
    });

    it('should throw for Infinity', () => {
      expect(() => withRateLimit(jest.fn(), Infinity)).toThrow(
        'intervalMs must be a positive integer'
      );
    });

    it('should throw at wrap time, before any call is made', () => {
      const fn = jest.fn();
      expect(() => withRateLimit(fn, 0)).toThrow();
      expect(fn).not.toHaveBeenCalled();
    });
  });

  describe('cancellation', () => {
    it('rejects a queued call when its signal aborts during the interval wait', async () => {
      // fn declares a trailing { signal } option — the opt-in for cancellation.
      const fn = jest.fn(async (_opts?: { signal?: AbortSignal }) => {});
      const rateLimited = withRateLimit(fn, 1000);
      const controller = new AbortController();

      rateLimited(); // first call runs immediately (reserves slot 0)
      const second = rateLimited({ signal: controller.signal }); // queued: must wait ~1000ms
      expect(fn).toHaveBeenCalledTimes(1); // only the first ran; the second is waiting

      controller.abort(); // abort while the second call is still waiting out the interval
      await expectAbortError(second);
      // Unlike withTimeout, the rate-limit wait is BEFORE fn runs — so an aborted
      // wait means fn is never invoked for that call.
      expect(fn).toHaveBeenCalledTimes(1);
    });

    it('rejects immediately if the signal is already aborted, without calling fn', async () => {
      const fn = jest.fn(async (_opts?: { signal?: AbortSignal }) => {});
      const rateLimited = withRateLimit(fn, 1000);

      const promise = rateLimited({ signal: abortedSignal() }); // already aborted before the call
      await expectAbortError(promise);
      // The entry poll rejects before a slot is reserved or fn is invoked.
      expect(fn).not.toHaveBeenCalled();
    });
  });
});