import { withTimeout } from '../../src/stream/withTimeout';
import { collect, collectInto, arrayStream, streamFn } from './helpers';
import { stall, deferred, abortedSignal, expectAbortError } from '../helpers';

describe('stream/withTimeout', () => {
  beforeEach(() => {
    jest.clearAllTimers();
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.runOnlyPendingTimers();
    jest.useRealTimers();
  });

  it('yields all items when the source answers each pull within the deadline', async () => {
    const sourceFn = streamFn().mockImplementation(() => arrayStream(['a', 'b', 'c']));
    const wrapped = withTimeout(sourceFn, 50);

    // The source answers each pull synchronously, so no per-pull timer ever fires.
    expect(await collect(wrapped('arg'))).toEqual(['a', 'b', 'c']);
    expect(sourceFn).toHaveBeenCalledTimes(1); // source created once, then pulled per item
    expect(sourceFn).toHaveBeenCalledWith('arg');
  });

  it('times out when the source never answers the first pull', async () => {
    const sourceFn = streamFn().mockImplementation(async function* (): AsyncIterable<string> {
      await stall(); // never produces the first item
      yield 'x';
    });
    const wrapped = withTimeout(sourceFn, 50);

    const p = collect(wrapped());
    const timedOut = expect(p).rejects.toThrow('Operation timed out'); // attach handler before advancing
    await jest.advanceTimersByTimeAsync(50); // the per-pull timer fires; the stalled source loses the race
    await timedOut;
    expect(sourceFn).toHaveBeenCalledTimes(1); // the source was started once, then the pull timed out
  });

  it('times out when the source stalls mid-stream', async () => {
    const sourceFn = streamFn().mockImplementation(async function* (): AsyncIterable<string> {
      yield 'a';
      await stall(); // stalls before the second item
      yield 'b';
    });
    const wrapped = withTimeout(sourceFn, 50);

    const seen: string[] = [];
    const p = collectInto(seen, wrapped());
    const timedOut = expect(p).rejects.toThrow('Operation timed out');
    await jest.advanceTimersByTimeAsync(50); // first item is delivered instantly; the second pull times out
    await timedOut;
    expect(seen).toEqual(['a']); // got the first item; the second pull timed out
    expect(sourceFn).toHaveBeenCalledTimes(1); // one source, pulled repeatedly (not re-created per pull)
  });

  it('does not time out when the consumer is slow between pulls', async () => {
    // The source answers every pull instantly; the consumer dawdles far longer than the
    // deadline BETWEEN pulls. A per-`next()` deadline must not count that think-time. We
    // drive the iterator by hand so we can advance the clock between pulls.
    const wrapped = withTimeout(() => arrayStream(['a', 'b', 'c']), 50);
    const it = wrapped()[Symbol.asyncIterator]();

    const out: string[] = [];
    for (let i = 0; i < 3; i++) {
      const r = await it.next(); // the source answers this pull synchronously
      expect(r.done).toBe(false);
      out.push(r.value as string);
      await jest.advanceTimersByTimeAsync(120); // dawdle 120ms (>> the 50ms deadline) with no pull outstanding
    }
    expect((await it.next()).done).toBe(true);
    expect(out).toEqual(['a', 'b', 'c']); // no timeout: the dawdle is between pulls, so no timer is running
  });

  it('returns fn unchanged when maxDurationMs is Infinity', () => {
    const fn = () => arrayStream(['x']);
    expect(withTimeout(fn, Infinity)).toBe(fn); // timeout disabled, zero overhead
  });

  it('uses a custom timeout error when provided', async () => {
    const custom = new Error('custom timeout');
    const source = async function* (): AsyncIterable<string> { await stall(); yield 'x'; };
    const wrapped = withTimeout(source, 50, custom);

    const p = collect(wrapped());
    const rejected = expect(p).rejects.toBe(custom); // attach the rejection handler before advancing (no unhandled rejection)
    await jest.advanceTimersByTimeAsync(50);
    await rejected; // wait for the assertion to finish so a mismatch is surfaced
  });

  it('accepts a non-Error timeout value', async () => {
    const source = async function* (): AsyncIterable<string> { await stall(); yield 'x'; };
    const wrapped = withTimeout(source, 50, 'timed out string');

    const p = collect(wrapped());
    const rejected = expect(p).rejects.toBe('timed out string'); // attach the rejection handler before advancing (no unhandled rejection)
    await jest.advanceTimersByTimeAsync(50);
    await rejected; // wait for the assertion to finish so a mismatch is surfaced
  });

  it('propagates a source error rather than a timeout', async () => {
    const err = new Error('boom');
    const source = async function* (): AsyncIterable<string> {
      yield 'a';
      throw err; // errors well within the deadline (no timer advance needed)
    };
    const wrapped = withTimeout(source, 50);

    const seen: string[] = [];
    await expect(collectInto(seen, wrapped())).rejects.toBe(err);
    expect(seen).toEqual(['a']);
  });

  describe('cleanup on early exit', () => {
    it('releases the source when the consumer abandons the stream', async () => {
      let cleanedUp = false;
      const source = async function* (): AsyncIterable<string> {
        try {
          yield 'a';
          yield 'b';
        } finally {
          cleanedUp = true;
        }
      };
      const wrapped = withTimeout(source, 50);

      for await (const v of wrapped()) {
        if (v === 'a') break; // abandon → the wrapper awaits the source's cleanup
      }
      expect(cleanedUp).toBe(true);
    });
  });

  describe('cancellation', () => {
    it('rejects with the abort reason when the signal aborts during a pull', async () => {
      const pullStarted = deferred();
      const source = async function* (_opts?: { signal?: AbortSignal }): AsyncIterable<string> {
        pullStarted.resolve(); // the wrapper has begun pulling us
        await stall();         // ...and we never answer
        yield 'x';
      };
      const wrapped = withTimeout(source, 5000); // long deadline; the abort should win the race
      const controller = new AbortController();

      const p = collect(wrapped({ signal: controller.signal }));
      await pullStarted.promise; // deterministically wait until the pull is outstanding
      controller.abort();        // abort while the pull is outstanding (no timer advance needed)
      await expectAbortError(p);
    });

    it('rejects at the first pull if the signal is already aborted, without starting the source', async () => {
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['x']));
      const wrapped = withTimeout(sourceFn, 50);

      await expectAbortError(collect(wrapped({ signal: abortedSignal() })));
      expect(sourceFn).not.toHaveBeenCalled();
    });
  });

  describe('invalid maxDurationMs', () => {
    it('throws for zero', () => {
      expect(() => withTimeout(streamFn(), 0)).toThrow('maxDurationMs must be a positive integer or Infinity');
    });

    it('throws for negative values', () => {
      expect(() => withTimeout(streamFn(), -1)).toThrow('maxDurationMs must be a positive integer or Infinity');
    });

    it('throws for non-integer values', () => {
      expect(() => withTimeout(streamFn(), 1.5)).toThrow('maxDurationMs must be a positive integer or Infinity');
    });

    it('throws for NaN', () => {
      expect(() => withTimeout(streamFn(), NaN)).toThrow('maxDurationMs must be a positive integer or Infinity');
    });

    it('throws at wrap time, before any stream is started', () => {
      const sourceFn = streamFn();
      expect(() => withTimeout(sourceFn, 0)).toThrow();
      expect(sourceFn).not.toHaveBeenCalled();
    });
  });
});
