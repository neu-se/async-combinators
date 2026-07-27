import { withRetry, ResumeConsistencyError } from '../../src/stream/withRetry';
import { abortError, abortedSignal, expectAbortError } from '../helpers';
import { collect, collectInto, arrayStream, failingStream, streamFn } from './helpers';

describe('stream/withRetry', () => {
  it('yields the full stream on first success', async () => {
    const fn = streamFn().mockImplementation(() => arrayStream(['a', 'b', 'c']));
    const retryFn = withRetry(fn, 3);
    const out = await collect(retryFn('arg1', 'arg2'));
    expect(out).toEqual(['a', 'b', 'c']);
    expect(fn).toHaveBeenCalledTimes(1);
    expect(fn).toHaveBeenCalledWith('arg1', 'arg2');
  });

  it('retries a pre-first-item failure and succeeds', async () => {
    const fn = streamFn()
      .mockImplementationOnce(() => failingStream<string>(new Error('fail1')))
      .mockImplementationOnce(() => failingStream<string>(new Error('fail2')))
      .mockImplementation(() => arrayStream(['a', 'b']));
    const retryFn = withRetry(fn, 3);
    const out = await collect(retryFn());
    expect(out).toEqual(['a', 'b']);
    expect(fn).toHaveBeenCalledTimes(3);
  });

  it('throws the final error after all attempts fail', async () => {
    const finalError = new Error('final fail');
    const fn = streamFn()
      .mockImplementationOnce(() => failingStream<string>(new Error('fail1')))
      .mockImplementationOnce(() => failingStream<string>(new Error('fail2')))
      .mockImplementation(() => failingStream<string>(finalError));
    const retryFn = withRetry(fn, 3);
    await expect(collect(retryFn())).rejects.toBe(finalError); // the final attempt's exact error propagates
    expect(fn).toHaveBeenCalledTimes(3);
  });

  it('calls onRetry before each retry', async () => {
    const error1 = new Error('fail1');
    const error2 = new Error('fail2');
    const fn = streamFn()
      .mockImplementationOnce(() => failingStream<string>(error1))
      .mockImplementationOnce(() => failingStream<string>(error2))
      .mockImplementation(() => arrayStream(['ok']));
    const onRetry = jest.fn();
    const retryFn = withRetry(fn, 3, { onRetry });
    const out = await collect(retryFn());
    expect(out).toEqual(['ok']); // the retry sequence ended in a successful stream
    expect(onRetry).toHaveBeenCalledTimes(2);
    expect(onRetry).toHaveBeenNthCalledWith(1, 1, error1); // attempt 1 failed with error1
    expect(onRetry).toHaveBeenNthCalledWith(2, 2, error2); // attempt 2 failed with error2
  });

  it('does not call onRetry on first attempt or success', async () => {
    const fn = streamFn().mockImplementation(() => arrayStream(['ok']));
    const onRetry = jest.fn();
    const retryFn = withRetry(fn, 3, { onRetry });
    const out = await collect(retryFn());
    expect(out).toEqual(['ok']); // succeeded on the first attempt
    expect(onRetry).not.toHaveBeenCalled();
  });

  it('works with a single attempt', async () => {
    const fn = streamFn().mockImplementation(() => arrayStream(['ok']));
    const retryFn = withRetry(fn, 1);
    expect(await collect(retryFn())).toEqual(['ok']);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('throws immediately with a single attempt on failure', async () => {
    const err = new Error('fail');
    const fn = streamFn().mockImplementation(() => failingStream<string>(err));
    const onRetry = jest.fn();
    const retryFn = withRetry(fn, 1, { onRetry });
    await expect(collect(retryFn())).rejects.toBe(err); // the single attempt's exact error propagates
    expect(fn).toHaveBeenCalledTimes(1);
    expect(onRetry).not.toHaveBeenCalled();
  });

  it('preserves function arguments across retries', async () => {
    const fn = streamFn()
      .mockImplementationOnce(() => failingStream<string>(new Error('fail')))
      .mockImplementation(() => arrayStream(['ok']));
    const retryFn = withRetry(fn, 2);
    await collect(retryFn('arg1', 42, { key: 'value' }));
    expect(fn).toHaveBeenCalledTimes(2);
    expect(fn).toHaveBeenNthCalledWith(1, 'arg1', 42, { key: 'value' });
    expect(fn).toHaveBeenNthCalledWith(2, 'arg1', 42, { key: 'value' });
  });

  it('handles non-Error thrown values', async () => {
    const stringError = 'string error';
    const numberError = 404;
    const objectError = { message: 'object error' };
    const fn = streamFn()
      .mockImplementationOnce(() => failingStream<string>(stringError))
      .mockImplementationOnce(() => failingStream<string>(numberError))
      .mockImplementation(() => failingStream<string>(objectError));
    const onRetry = jest.fn();
    const retryFn = withRetry(fn, 3, { onRetry });
    await expect(collect(retryFn())).rejects.toBe(objectError);
    expect(onRetry).toHaveBeenNthCalledWith(1, 1, stringError);
    expect(onRetry).toHaveBeenNthCalledWith(2, 2, numberError);
  });

  // --- streaming-specific: partial output & resumability ------------------

  describe('partial output', () => {
    it('does not retry after items have been delivered when not resumable', async () => {
      const err = new Error('mid-stream fail');
      const fn = streamFn()
        .mockImplementationOnce(() => failingStream(err, ['a', 'b'])) // deliver a, b, then fail
        .mockImplementation(() => arrayStream(['a', 'b', 'c']));
      const retryFn = withRetry(fn, 3); // resumable defaults to false
      const seen: string[] = [];
      await expect(collectInto(seen, retryFn())).rejects.toThrow('mid-stream fail');
      expect(seen).toEqual(['a', 'b']); // consumer keeps the prefix it saw
      expect(fn).toHaveBeenCalledTimes(1); // never restarted
    });

    it('with resumable, restarts a deterministic stream and skips the delivered prefix', async () => {
      const err = new Error('mid-stream fail');
      const fn = streamFn()
        .mockImplementationOnce(() => failingStream(err, ['a', 'b'])) // attempt 1: a, b, then fail
        .mockImplementation(() => arrayStream(['a', 'b', 'c', 'd']));  // attempt 2: full deterministic run
      const retryFn = withRetry(fn, 3, { resumable: true });
      const out = await collect(retryFn());
      expect(out).toEqual(['a', 'b', 'c', 'd']); // no duplicated prefix
      expect(fn).toHaveBeenCalledTimes(2);
    });

    it('resumes incrementally across multiple failures (5 items; fails after 2, then after 4)', async () => {
      // A deterministic 5-item source that reproduces its prefix on each re-run, but is
      // truncated by a transient error after item 2 on the first run and after item 4 on the
      // second. With resumable + 2 retries it should deliver 1,2 then 3,4 then 5, each once.
      const err = new Error('transient');
      const fn = jest.fn<AsyncIterable<number>, unknown[]>()
        .mockImplementationOnce(() => failingStream(err, [1, 2]))        // attempt 1: 1,2, then fail
        .mockImplementationOnce(() => failingStream(err, [1, 2, 3, 4]))  // attempt 2: 1,2,3,4, then fail
        .mockImplementation(() => arrayStream([1, 2, 3, 4, 5]));         // attempt 3: full deterministic run
      const onRetry = jest.fn();
      const retryFn = withRetry(fn, 3, { resumable: true, onRetry }); // 3 attempts = 2 retries
      const out = await collect(retryFn());
      expect(out).toEqual([1, 2, 3, 4, 5]); // each item exactly once, in order, no duplicated prefix
      expect(fn).toHaveBeenCalledTimes(3);
      expect(onRetry).toHaveBeenCalledTimes(2); // one per failure
    });

    it('raises ResumeConsistencyError when a resumable re-run is shorter than the delivered prefix', async () => {
      // The caller asserted determinism, but the source is nondeterministic in a way the
      // wrapper CAN catch cheaply: the re-run yields fewer items than were already
      // delivered, so it cannot even reproduce the prefix. Fail loudly, do not truncate.
      const err = new Error('mid-stream fail');
      const fn = streamFn()
        .mockImplementationOnce(() => failingStream(err, ['a', 'b', 'c'])) // deliver a,b,c then fail
        .mockImplementation(() => arrayStream(['x']));                     // re-run is shorter
      const retryFn = withRetry(fn, 3, { resumable: true });
      const seen: string[] = [];
      await expect(collectInto(seen, retryFn())).rejects.toThrow(ResumeConsistencyError);
      expect(seen).toEqual(['a', 'b', 'c']); // prefix already delivered, then it fails loudly
      expect(fn).toHaveBeenCalledTimes(2);
    });

    it('does NOT detect value divergence: a same-length nondeterministic re-run is spliced (garbage in, garbage out)', async () => {
      // The caller asserted determinism falsely, but the re-run is long enough that the
      // count-only guard cannot tell. The wrapper skips the retry's prefix by POSITION,
      // not by matching, so it silently splices two different runs. This documents the
      // limitation: without buffering, value-level divergence is undetectable.
      const err = new Error('mid-stream fail');
      const fn = streamFn()
        .mockImplementationOnce(() => failingStream(err, ['a', 'b']))   // run 1: a, b, then fail
        .mockImplementation(() => arrayStream(['c', 'd', 'e', 'f']));   // run 2: a DIFFERENT sequence
      const retryFn = withRetry(fn, 3, { resumable: true });
      const out = await collect(retryFn());
      // a,b from run 1; run 2's first two items (c,d) skipped by position, not by match,
      // so the consumer sees a stream that never actually occurred, with no error raised.
      expect(out).toEqual(['a', 'b', 'e', 'f']);
      expect(fn).toHaveBeenCalledTimes(2);
    });

    it('with resumable, still gives up after maxAttempts', async () => {
      const fn = streamFn().mockImplementation(() => failingStream(new Error('always'), ['a']));
      const retryFn = withRetry(fn, 3, { resumable: true });
      await expect(collect(retryFn())).rejects.toThrow('always');
      expect(fn).toHaveBeenCalledTimes(3);
    });
  });

  // --- cancellation -------------------------------------------------------

  describe('cancellation', () => {
    it('does not start the stream when the signal is already aborted', async () => {
      const fn = streamFn().mockImplementation(() => arrayStream(['a']));
      const retryFn = withRetry(fn, 3);
      await expectAbortError(collect(retryFn({ signal: abortedSignal() })));
      expect(fn).not.toHaveBeenCalled();
    });

    it('does not retry a cancellation thrown before the first item', async () => {
      const fn = streamFn().mockImplementation(() => failingStream<string>(abortError()));
      const retryFn = withRetry(fn, 3);
      await expectAbortError(collect(retryFn()));
      expect(fn).toHaveBeenCalledTimes(1);
    });

    it('does not retry a cancellation thrown mid-stream', async () => {
      const fn = streamFn().mockImplementation(() => failingStream(abortError(), ['a']));
      const retryFn = withRetry(fn, 3, { resumable: true }); // even when resume is allowed
      const it = retryFn()[Symbol.asyncIterator]();
      expect((await it.next()).value).toBe('a');
      await expect(it.next()).rejects.toHaveProperty('name', 'AbortError');
      expect(fn).toHaveBeenCalledTimes(1);
    });

  });

  // --- cleanup on early exit ----------------------------------------------

  describe('cleanup on early exit', () => {
    // These cases are NOT signal/AbortError cancellation. They exercise the generator
    // return/cleanup protocol: when a consumer abandons a stream, `for await` (or an
    // explicit `.return()`) unwinds the wrapper, which in turn calls the source
    // iterator's `.return()`, running its `finally`. A promise wrapper has nothing to
    // tear down; this cleanup is the concern that is genuinely new for streams.

    // A source that releases a resource (a file / socket / DB cursor) in a `finally`;
    // `flag.done` stands in for "the resource was closed".
    function cleanupSource(flag: { done: boolean }) {
      return async function* (): AsyncIterable<number> {
        try {
          yield 1;
          yield 2;
          yield 3;
        } finally {
          flag.done = true; // e.g. close the underlying resource
        }
      };
    }

    it('runs source cleanup when the consumer abandons via a manual .return()', async () => {
      const flag = { done: false };
      const retryFn = withRetry(cleanupSource(flag), 3);

      // Drive the wrapper's async iterator by hand (instead of `for await`) so we can
      // abandon it at a precise point. `obj[Symbol.asyncIterator]()` is the async-iteration
      // protocol's factory method: it returns the iterator, which has .next()/.return()/.throw().
      const it = retryFn()[Symbol.asyncIterator]();
      expect((await it.next()).value).toBe(1); // pull one item; the wrapper is now suspended at `yield`

      // `.return()` tells the generator "the consumer is done" — exactly what `break` in a
      // for-await loop does under the hood. It injects a *return completion* at the suspended
      // `yield` (not an exception, so no `catch` runs and no AbortError is involved), which
      // unwinds the wrapper's own `for await` over the source and calls the source's
      // `.return()`, running its `finally`.
      await it.return?.(undefined);
      expect(flag.done).toBe(true);
    });

    it('runs source cleanup when the consumer breaks out of a for-await loop', async () => {
      const flag = { done: false };
      const retryFn = withRetry(cleanupSource(flag), 3);
      const received: number[] = [];
      for await (const n of retryFn()) {
        received.push(n);
        if (n === 1) break; // the idiomatic way a consumer abandons a stream
      }
      expect(received).toEqual([1]);
      expect(flag.done).toBe(true); // break called .return() down the chain to the source
    });

    it("runs source cleanup when the consumer's loop body throws", async () => {
      const flag = { done: false };
      const retryFn = withRetry(cleanupSource(flag), 3);
      const boom = new Error('consumer error');
      await expect(
        (async () => {
          for await (const n of retryFn()) {
            if (n === 1) throw boom; // a consumer-side failure mid-iteration
          }
        })()
      ).rejects.toBe(boom); // the consumer's exact error propagates out
      expect(flag.done).toBe(true); // for-await still tears the source down on a body throw
    });

    it('does not start the source if the consumer returns before the first pull', async () => {
      let started = false;
      const fn = jest.fn(async function* (): AsyncIterable<number> {
        started = true; // runs only once the source is actually iterated
        yield 1;
      });
      const retryFn = withRetry(fn, 3);
      const it = retryFn()[Symbol.asyncIterator]();
      await it.return?.(undefined); // abandon before any .next(): the wrapper body never runs
      expect(started).toBe(false);
      expect(fn).not.toHaveBeenCalled(); // the wrapper never even asked for the source
    });
  });

  // --- validation ---------------------------------------------------------

  describe('invalid maxAttempts', () => {
    it('throws for zero', () => {
      expect(() => withRetry(jest.fn(), 0)).toThrow('maxAttempts must be a positive integer');
    });

    it('throws for negative values', () => {
      expect(() => withRetry(jest.fn(), -1)).toThrow('maxAttempts must be a positive integer');
    });

    it('throws for non-integer values', () => {
      expect(() => withRetry(jest.fn(), 2.5)).toThrow('maxAttempts must be a positive integer');
    });

    it('throws for NaN and Infinity', () => {
      expect(() => withRetry(jest.fn(), NaN)).toThrow('maxAttempts must be a positive integer');
      expect(() => withRetry(jest.fn(), Infinity)).toThrow('maxAttempts must be a positive integer');
    });

    it('throws at wrap time, before any stream is started', () => {
      const fn = jest.fn();
      expect(() => withRetry(fn, 0)).toThrow();
      expect(fn).not.toHaveBeenCalled();
    });
  });

  describe('invalid backoff options', () => {
    it('throws for a negative delayMs', () => {
      expect(() => withRetry(jest.fn(), 3, { delayMs: -1 })).toThrow(
        'delayMs must be a non-negative number'
      );
    });

    it('throws for a non-finite delayMs', () => {
      expect(() => withRetry(jest.fn(), 3, { delayMs: Infinity })).toThrow(
        'delayMs must be a non-negative number'
      );
    });

    it('throws for an unknown backoff strategy', () => {
      expect(() => withRetry(jest.fn(), 3, { backoff: 'linear' as any })).toThrow(
        "backoff must be 'fixed' or 'exponential'"
      );
    });

    it('throws for factor < 1', () => {
      expect(() => withRetry(jest.fn(), 3, { factor: 0.5 })).toThrow(
        'factor must be a finite number >= 1'
      );
    });

    it('throws for a non-positive maxDelayMs', () => {
      expect(() => withRetry(jest.fn(), 3, { maxDelayMs: 0 })).toThrow(
        'maxDelayMs must be a positive number'
      );
    });

    it('throws at wrap time, before any stream is started', () => {
      const fn = jest.fn();
      expect(() => withRetry(fn, 3, { delayMs: -1 })).toThrow();
      expect(fn).not.toHaveBeenCalled();
    });
  });

  // --- backoff timing (fake timers) ---------------------------------------

  describe('backoff', () => {
    beforeEach(() => jest.useFakeTimers());
    afterEach(() => jest.useRealTimers());

    it('waits a constant delayMs between attempts (fixed backoff)', async () => {
      const fn = streamFn()
        .mockImplementationOnce(() => failingStream<string>(new Error('f1')))
        .mockImplementationOnce(() => failingStream<string>(new Error('f2')))
        .mockImplementation(() => arrayStream(['ok']));
      const retryFn = withRetry(fn, 3, { delayMs: 100, backoff: 'fixed' });

      const p = collect(retryFn());
      await jest.advanceTimersByTimeAsync(0); // attempt 1 runs and fails → now waiting
      expect(fn).toHaveBeenCalledTimes(1);

      await jest.advanceTimersByTimeAsync(99);
      expect(fn).toHaveBeenCalledTimes(1); // delay not elapsed
      await jest.advanceTimersByTimeAsync(1);
      expect(fn).toHaveBeenCalledTimes(2); // attempt 2 at 100ms

      await jest.advanceTimersByTimeAsync(100);
      expect(fn).toHaveBeenCalledTimes(3); // fixed: another 100ms before attempt 3

      await expect(p).resolves.toEqual(['ok']);
    });

    it('grows the delay exponentially: delayMs * factor^(k-1)', async () => {
      const fn = streamFn()
        .mockImplementationOnce(() => failingStream<string>(new Error('f1')))
        .mockImplementationOnce(() => failingStream<string>(new Error('f2')))
        .mockImplementationOnce(() => failingStream<string>(new Error('f3')))
        .mockImplementationOnce(() => failingStream<string>(new Error('f4')))
        .mockImplementation(() => arrayStream(['ok']));
      const retryFn = withRetry(fn, 5, { delayMs: 100, backoff: 'exponential', factor: 2 });

      const p = collect(retryFn());
      await jest.advanceTimersByTimeAsync(0);
      expect(fn).toHaveBeenCalledTimes(1); // attempt 1 ran immediately

      let calls = 1;
      for (const delay of [100, 200, 400, 800]) {
        await jest.advanceTimersByTimeAsync(delay - 1);
        expect(fn).toHaveBeenCalledTimes(calls); // the (doubling) delay hasn't elapsed yet
        await jest.advanceTimersByTimeAsync(1);
        expect(fn).toHaveBeenCalledTimes(++calls); // ...now the next attempt fires
      }

      await expect(p).resolves.toEqual(['ok']);
    });

    it('caps the computed delay at maxDelayMs', async () => {
      const fn = streamFn()
        .mockImplementationOnce(() => failingStream<string>(new Error('f1')))
        .mockImplementationOnce(() => failingStream<string>(new Error('f2')))
        .mockImplementation(() => arrayStream(['ok']));
      const retryFn = withRetry(fn, 3, { delayMs: 100, backoff: 'exponential', factor: 2, maxDelayMs: 150 });

      const p = collect(retryFn());
      await jest.advanceTimersByTimeAsync(0);
      await jest.advanceTimersByTimeAsync(100); // min(100, 150) = 100 → attempt 2
      expect(fn).toHaveBeenCalledTimes(2);
      await jest.advanceTimersByTimeAsync(150); // min(200, 150) = 150 → attempt 3
      expect(fn).toHaveBeenCalledTimes(3);

      await expect(p).resolves.toEqual(['ok']);
    });

    it('abandons a pending backoff wait when the inbound signal aborts', async () => {
      const fn = streamFn().mockImplementation((_opts?: unknown) =>
        failingStream<string>(new Error('transient failure'))
      );
      const retryFn = withRetry(fn, 5, { delayMs: 100 });
      const controller = new AbortController();

      const p = collect(retryFn({ signal: controller.signal }));

      await jest.advanceTimersByTimeAsync(0); // attempt 1 failed → waiting in the 100ms backoff
      expect(fn).toHaveBeenCalledTimes(1);

      controller.abort(); // lands mid-backoff, before the retry fires
      await expectAbortError(p);
      expect(fn).toHaveBeenCalledTimes(1); // retry was aborted
    });
  });
});
