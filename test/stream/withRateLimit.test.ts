import { withRateLimit } from '../../src/stream/withRateLimit';
import { collect, arrayStream, failingStream, streamFn } from './helpers';
import { abortedSignal, expectAbortError } from '../helpers';

describe('stream/withRateLimit', () => {
  beforeEach(() => {
    jest.clearAllTimers();
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.runOnlyPendingTimers();
    jest.useRealTimers();
  });

  // `sourceFn` is invoked when a stream passes the gate and actually starts, so its
  // call count is the streaming analogue of "fn was called" in the promise suite.

  it('starts the first stream immediately and delays the second by the interval', async () => {
    // The same rate limiter, called twice; each call yields a distinct stream so the
    // two are easy to tell apart (rate limiting only spaces successive calls to the
    // *same* wrapper — two separate wrappers would be independent; see the next test).
    const sourceFn = streamFn()
      .mockImplementationOnce(() => arrayStream(['a'])) // first stream
      .mockImplementationOnce(() => arrayStream(['b'])); // second stream
    const rl = withRateLimit(sourceFn, 1000);

    // First stream starts immediately.
    const first = collect(rl());
    // advanceTimersByTimeAsync(0): don't move the clock, just flush pending microtasks
    // so the lazily-started stream's body runs (claims its rate-limit slot, then starts
    // the source since the first stream's wait is 0).
    await jest.advanceTimersByTimeAsync(0);
    expect(sourceFn).toHaveBeenCalledTimes(1);
    expect(await first).toEqual(['a']);

    // Second stream is held until a full interval has passed since the first started.
    const second = collect(rl());
    await jest.advanceTimersByTimeAsync(0);
    expect(sourceFn).toHaveBeenCalledTimes(1); // second not started yet

    await jest.advanceTimersByTimeAsync(999);
    expect(sourceFn).toHaveBeenCalledTimes(1); // still waiting
    await jest.advanceTimersByTimeAsync(1);
    expect(await second).toEqual(['b']); // started only after the full interval
    expect(sourceFn).toHaveBeenCalledTimes(2);
  });

  it('spaces out a concurrent burst of stream starts', async () => {
    const sourceFn = streamFn().mockImplementation(() => arrayStream(['x']));
    const rl = withRateLimit(sourceFn, 1000);

    // Begin collecting four streams concurrently.
    const ps = [collect(rl()), collect(rl()), collect(rl()), collect(rl())];

    await jest.advanceTimersByTimeAsync(0);
    expect(sourceFn).toHaveBeenCalledTimes(1); // only the first passes the gate immediately

    // For each queued start: halfway through its interval it has NOT started yet;
    // it starts only once the full interval elapses.
    await jest.advanceTimersByTimeAsync(500);
    expect(sourceFn).toHaveBeenCalledTimes(1); // second not yet
    await jest.advanceTimersByTimeAsync(500);
    expect(sourceFn).toHaveBeenCalledTimes(2); // second starts at 1000ms

    await jest.advanceTimersByTimeAsync(500);
    expect(sourceFn).toHaveBeenCalledTimes(2); // third not yet
    await jest.advanceTimersByTimeAsync(500);
    expect(sourceFn).toHaveBeenCalledTimes(3); // third starts at 2000ms

    await jest.advanceTimersByTimeAsync(500);
    expect(sourceFn).toHaveBeenCalledTimes(3); // fourth not yet
    await jest.advanceTimersByTimeAsync(500);
    await Promise.all(ps);
    expect(sourceFn).toHaveBeenCalledTimes(4); // fourth starts at 3000ms
  });

  it('still rate-limits after a stream errors', async () => {
    const err = new Error('boom');
    const sourceFn = streamFn().mockImplementation(() => failingStream(err));
    const rl = withRateLimit(sourceFn, 1000);

    const first = collect(rl());
    const firstRejects = expect(first).rejects.toBe(err); // attach handler before advancing
    await jest.advanceTimersByTimeAsync(0);
    expect(sourceFn).toHaveBeenCalledTimes(1); // started immediately, then errors
    await firstRejects;

    const second = collect(rl());
    const secondRejects = expect(second).rejects.toBe(err);
    await jest.advanceTimersByTimeAsync(0);
    expect(sourceFn).toHaveBeenCalledTimes(1); // still delayed despite the first's error
    await jest.advanceTimersByTimeAsync(500);
    expect(sourceFn).toHaveBeenCalledTimes(1); // halfway — still not started
    await jest.advanceTimersByTimeAsync(500);
    expect(sourceFn).toHaveBeenCalledTimes(2); // started only after the full interval
    await secondRejects;
  });

  it('maintains separate state for different wrapped functions', async () => {
    const source1 = streamFn().mockImplementation(() => arrayStream(['a']));
    const source2 = streamFn().mockImplementation(() => arrayStream(['b']));
    const rl1 = withRateLimit(source1, 1000);
    const rl2 = withRateLimit(source2, 1000);

    // Both first streams start immediately (independent limiters).
    await collect(rl1());
    await collect(rl2());
    expect(source1).toHaveBeenCalledTimes(1);
    expect(source2).toHaveBeenCalledTimes(1);

    // Both second streams are delayed by their own interval.
    const p1 = collect(rl1());
    const p2 = collect(rl2());
    await jest.advanceTimersByTimeAsync(1000);
    await Promise.all([p1, p2]);
    expect(source1).toHaveBeenCalledTimes(2);
    expect(source2).toHaveBeenCalledTimes(2);
  });

  it('applies the larger interval when nested (inner larger)', async () => {
    const sourceFn = streamFn().mockImplementation(() => arrayStream(['x']));
    // inner 2000, outer 1000 → stream starts are spaced by the larger interval (2000).
    const limited = withRateLimit(withRateLimit(sourceFn, 2000), 1000);

    const p1 = collect(limited());
    const p2 = collect(limited());
    await jest.advanceTimersByTimeAsync(0);
    expect(sourceFn).toHaveBeenCalledTimes(1); // first immediate, second gated

    // At 1000 the outer would allow the second start, but the inner (2000) blocks it.
    await jest.advanceTimersByTimeAsync(1000);
    expect(sourceFn).toHaveBeenCalledTimes(1);

    await jest.advanceTimersByTimeAsync(1000);
    await Promise.all([p1, p2]);
    expect(sourceFn).toHaveBeenCalledTimes(2); // starts only at 2000
  });

  it('applies the larger interval when nested (outer larger)', async () => {
    const sourceFn = streamFn().mockImplementation(() => arrayStream(['x']));
    // inner 1000, outer 2000 → stream starts are spaced by the larger interval (2000).
    const limited = withRateLimit(withRateLimit(sourceFn, 1000), 2000);

    const p1 = collect(limited());
    const p2 = collect(limited());
    await jest.advanceTimersByTimeAsync(0);
    expect(sourceFn).toHaveBeenCalledTimes(1);

    await jest.advanceTimersByTimeAsync(1000);
    expect(sourceFn).toHaveBeenCalledTimes(1); // outer (2000) still blocks

    await jest.advanceTimersByTimeAsync(1000);
    await Promise.all([p1, p2]);
    expect(sourceFn).toHaveBeenCalledTimes(2); // starts only at 2000
  });

  // --- streaming-specific: reserve at first pull --------------------------

  describe('reserve at first pull', () => {
    it('reserves nothing for a stream that is created but never iterated', async () => {
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['x']));
      const rl = withRateLimit(sourceFn, 1000);

      rl(); // created but never consumed → its body never runs, so no slot is reserved
      await jest.advanceTimersByTimeAsync(0);
      expect(sourceFn).not.toHaveBeenCalled();

      // A stream that IS iterated therefore starts immediately (nothing before it).
      const started = collect(rl());
      await jest.advanceTimersByTimeAsync(0);
      expect(sourceFn).toHaveBeenCalledTimes(1);
      await started;
    });

    it('does not pace items within a stream (only the start)', async () => {
      // If items were rate-limited, collect would hang without advancing timers.
      const rl = withRateLimit(() => arrayStream(['a', 'b', 'c']), 1000);
      const out = await collect(rl());
      expect(out).toEqual(['a', 'b', 'c']);
    });
  });

  describe('invalid intervalMs', () => {
    it('throws for zero', () => {
      expect(() => withRateLimit(streamFn(), 0)).toThrow('intervalMs must be a positive integer');
    });

    it('throws for negative values', () => {
      expect(() => withRateLimit(streamFn(), -1)).toThrow('intervalMs must be a positive integer');
    });

    it('throws for non-integer values', () => {
      expect(() => withRateLimit(streamFn(), 1.5)).toThrow('intervalMs must be a positive integer');
    });

    it('throws for NaN and Infinity', () => {
      expect(() => withRateLimit(streamFn(), NaN)).toThrow('intervalMs must be a positive integer');
      expect(() => withRateLimit(streamFn(), Infinity)).toThrow('intervalMs must be a positive integer');
    });

    it('throws at wrap time, before any stream is started', () => {
      const sourceFn = streamFn();
      expect(() => withRateLimit(sourceFn, 0)).toThrow();
      expect(sourceFn).not.toHaveBeenCalled();
    });
  });

  describe('cancellation', () => {
    it('rejects a queued stream when its signal aborts during the gate wait', async () => {
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['x']));
      const rl = withRateLimit(sourceFn, 1000);
      const controller = new AbortController();

      expect(await collect(rl())).toEqual(['x']); // first stream starts immediately and yields
      expect(sourceFn).toHaveBeenCalledTimes(1); // confirm the first stream ran

      const second = collect(rl({ signal: controller.signal })); // queued: must wait ~1000ms
      await jest.advanceTimersByTimeAsync(0);
      expect(sourceFn).toHaveBeenCalledTimes(1); // second is still waiting (count unchanged)

      controller.abort(); // abort mid-wait, before the source is started
      await expectAbortError(second);
      expect(sourceFn).toHaveBeenCalledTimes(1); // aborted gate wait → source never started
    });

    it('rejects immediately if the signal is already aborted, without reserving a slot', async () => {
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['x']));
      const rl = withRateLimit(sourceFn, 1000);

      await expectAbortError(collect(rl({ signal: abortedSignal() })));
      expect(sourceFn).not.toHaveBeenCalled();

      // No slot was reserved, so a subsequent stream starts immediately.
      const next = collect(rl());
      await jest.advanceTimersByTimeAsync(0);
      expect(sourceFn).toHaveBeenCalledTimes(1);
      expect(await next).toEqual(['x']); // and it runs to completion, yielding its item
    });
  });

  describe('cleanup on early exit', () => {
    it('cleans up the source when the consumer abandons the stream', async () => {
      let cleanedUp = false;
      const source = async function* (): AsyncIterable<string> {
        try {
          yield 'a';
          yield 'b';
        } finally {
          cleanedUp = true;
        }
      };
      const rl = withRateLimit(source, 1000);
      for await (const v of rl()) {
        if (v === 'a') break; // abandon the (already-started) stream
      }
      expect(cleanedUp).toBe(true);
    });

    it('a stream aborted during the gate wait still consumes its slot', async () => {
      // Abandoning the gate wait with an abort (which *rejects* the wait) means the
      // source is never started, but the slot was already reserved, so it is still
      // consumed. (Abandoning via a bare `.return()` instead would start-then-tear-down
      // the source, because `.return()` only takes effect at the next `yield`, not at
      // the pending `await` — so an abort is the clean way to show "never started".)
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['x']));
      const rl = withRateLimit(sourceFn, 1000);
      const controller = new AbortController();

      // First stream runs immediately (claims the first slot).
      expect(await collect(rl())).toEqual(['x']);
      expect(sourceFn).toHaveBeenCalledTimes(1);

      // Second stream claims the next slot, enters the gate wait, then is aborted.
      const second = collect(rl({ signal: controller.signal }));
      const secondRejects = expectAbortError(second); // attach handler before advancing
      await jest.advanceTimersByTimeAsync(0);
      controller.abort();
      await secondRejects;
      expect(sourceFn).toHaveBeenCalledTimes(1); // aborted before the gate opened → source never started

      // ...but it DID consume its slot: the next stream must wait a FULL extra interval
      // (2000ms after the first started, not 1000). Had the slot not been consumed, it
      // would start at 1000 instead.
      const third = collect(rl());
      await jest.advanceTimersByTimeAsync(0);
      expect(sourceFn).toHaveBeenCalledTimes(1); // delayed
      await jest.advanceTimersByTimeAsync(1000); // t=1000: without the consumed slot it would start here
      expect(sourceFn).toHaveBeenCalledTimes(1); // still delayed — the abandoned slot pushed it out
      await jest.advanceTimersByTimeAsync(500); // t=1500
      expect(sourceFn).toHaveBeenCalledTimes(1); // still delayed, past the 1000 mark
      await jest.advanceTimersByTimeAsync(500); // t=2000
      expect(await third).toEqual(['x']);
      expect(sourceFn).toHaveBeenCalledTimes(2); // starts only at 2000, one full interval late
    });
  });
});
