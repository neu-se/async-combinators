import { withRetry } from '../../src/stream/withRetry';
import { withTimeout } from '../../src/stream/withTimeout';
import { withFallback } from '../../src/stream/withFallback';
import { withCache } from '../../src/stream/withCache';
import { withLock } from '../../src/stream/withLock';
import { Lock } from '../../src/lock';
import { collect, collectInto, arrayStream, failingStream, streamFn } from './helpers';
import { deferred, expectAbortError, pauseMicrotask, stall } from '../helpers';

// Cross-combinator tests: each stream `withX` already has isolated coverage, so
// this suite asserts only the behavior that emerges when wrappers are nested.
describe('stream/composition', () => {
  beforeEach(() => {
    jest.clearAllTimers();
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.runOnlyPendingTimers();
    jest.useRealTimers();
  });

  it('withRetry(withTimeout(fn)): a pre-first-item timeout triggers a retry', async () => {
    let calls = 0;
    const sourceFn = streamFn<string>().mockImplementation(() => {
      calls++;
      if (calls === 1) {
        return (async function* (): AsyncIterable<string> {
          await stall();
          yield 'late';
        })();
      }
      return arrayStream(['ok']);
    });

    const wrapped = withRetry(withTimeout(sourceFn, 20), 3);

    const out = collect(wrapped());
    await jest.advanceTimersByTimeAsync(20);
    expect(await out).toEqual(['ok']);
    expect(sourceFn).toHaveBeenCalledTimes(2);
  });

  it('withRetry(withTimeout(fn)): a mid-stream timeout propagates without retry by default', async () => {
    const sourceFn = streamFn<string>().mockImplementation(async function* (): AsyncIterable<string> {
      yield 'a';
      await stall();
      yield 'b';
    });

    const wrapped = withRetry(withTimeout(sourceFn, 20), 3);

    const seen: string[] = [];
    // Once 'a' has been delivered, retry would have to restart the stream and
    // somehow reconcile already-observed output. With resumable=false (the
    // default), stream retry refuses that mid-stream recovery and propagates the
    // timeout instead of risking duplicate or divergent output.
    const p = collectInto(seen, wrapped());
    const timedOut = expect(p).rejects.toThrow('Operation timed out');
    await jest.advanceTimersByTimeAsync(20);
    await timedOut;
    expect(seen).toEqual(['a']);
    expect(sourceFn).toHaveBeenCalledTimes(1);
  });

  it('withRetry(withTimeout(fn)) with resumable: a deterministic stream restarts and skips the delivered prefix', async () => {
    const sourceFn = streamFn<string>()
      .mockImplementationOnce(async function* (): AsyncIterable<string> {
        yield 'a';
        await stall();
        yield 'b';
      })
      .mockImplementation(() => arrayStream(['a', 'b', 'c']));

    const wrapped = withRetry(withTimeout(sourceFn, 20), 3, { resumable: true });

    // The first attempt times out after delivering 'a'. Because the source is
    // deterministic and resumable=true was asserted, retry is allowed to start
    // over, skip the already-seen prefix, and continue with only the new suffix.
    const out = collect(wrapped());
    await jest.advanceTimersByTimeAsync(20);
    expect(await out).toEqual(['a', 'b', 'c']);
    expect(sourceFn).toHaveBeenCalledTimes(2);
  });

  it('withFallback(withRetry(fn)): the fallback fires only after retries are exhausted', async () => {
    const sourceFn = jest.fn((_k: string) => failingStream<string>(new Error('always fails')));
    const fallbackFn = jest.fn((_k: string) => arrayStream(['fallback-result']));

    const wrapped = withFallback(withRetry(sourceFn, 3), fallbackFn);

    expect(await collect(wrapped('k'))).toEqual(['fallback-result']);
    expect(sourceFn).toHaveBeenCalledTimes(3);
    expect(fallbackFn).toHaveBeenCalledTimes(1);
  });

  it('withCache(withRetry(fn)): the retried success is cached, so a repeat call skips retrying', async () => {
    let calls = 0;
    const sourceFn = jest.fn((_k: string) => {
      calls++;
      return calls === 1
        ? failingStream<string>(new Error('transient'))
        : arrayStream(['value']);
    });

    const wrapped = withCache(withRetry(sourceFn, 3));

    expect(await collect(wrapped('k'))).toEqual(['value']); // calls sourceFn
    expect(await collect(wrapped('k'))).toEqual(['value']); // retrieved from cache
    expect(sourceFn).toHaveBeenCalledTimes(2);
  });

  it('composition order matters: cache outside vs inside retry', async () => {
    const makeFn = () => {
      let calls = 0;
      return jest.fn((_k: string) => {
        calls++;
        return calls === 1
          ? failingStream<string>(new Error('transient'))
          : arrayStream(['value']);
      });
    };

    const fnOutside = makeFn();
    const cacheOutside = withCache(withRetry(fnOutside, 3));
    expect(await collect(cacheOutside('k'))).toEqual(['value']);
    expect(fnOutside).toHaveBeenCalledTimes(2);

    const fnInside = makeFn();
    const cacheInside = withRetry(withCache(fnInside, { cacheErrors: true }), 3);
    await expect(collect(cacheInside('k'))).rejects.toThrow('transient');
    expect(fnInside).toHaveBeenCalledTimes(1);
  });

  it('full resilience stack: withFallback(withRetry(withTimeout(fn))) degrades gracefully', async () => {
    const sourceFn = jest.fn(() => (async function* (): AsyncIterable<string> {
      await stall();
      yield 'slow';
    })());
    const fallbackFn = jest.fn(() => arrayStream(['fallback']));

    const robust = withFallback(withRetry(withTimeout(sourceFn, 20), 3), fallbackFn);

    const out = collect(robust());
    await jest.advanceTimersByTimeAsync(60);
    expect(await out).toEqual(['fallback']);
    expect(sourceFn).toHaveBeenCalledTimes(3);
    expect(fallbackFn).toHaveBeenCalledTimes(1);
  });

  it('withTimeout(withLock(fn)): the timeout bounds the wait for a contended lock', async () => {
    const lock = new Lock();
    const gate = deferred();

    const holder = withLock(async function* (): AsyncIterable<string> {
      yield 'holder';
      await gate.promise;
      yield 'done';
    }, lock);
    const itHolder = holder()[Symbol.asyncIterator]();
    await itHolder.next();

    const waiterSource = streamFn<string>().mockImplementation(() => arrayStream(['waiter']));
    const waiter = withTimeout(withLock(waiterSource, lock), 20);

    const p = collect(waiter());
    const timedOut = expect(p).rejects.toThrow('Operation timed out');
    await jest.advanceTimersByTimeAsync(20);
    await timedOut;

    gate.resolve();
    // Drain the holder stream so it can yield its second item, complete, and
    // release the shared lock before the test exits.
    await itHolder.next();
    await itHolder.next();
  });

  it('withLock + caller abort while queued: the waiter is dequeued and never runs', async () => {
    const lock = new Lock();
    const gate = deferred();

    const holder = withLock(async function* (): AsyncIterable<number> {
      yield 1;
      await gate.promise;
      yield 2;
    }, lock);
    const itHolder = holder()[Symbol.asyncIterator]();
    await itHolder.next();

    const controller = new AbortController();
    const waiterSource = streamFn<string>().mockImplementation(() => arrayStream(['waiter']));
    const waiter = withLock(waiterSource, lock);
    const waiterDone = collect(waiter({ signal: controller.signal })); // try to acquire the lock and execute waiterSource
    await pauseMicrotask();
    controller.abort(); // abort before the waiter acquires the lock

    await expectAbortError(waiterDone);
    expect(waiterSource).not.toHaveBeenCalled();

    gate.resolve();
    // Drain the holder stream so it can yield its second item, complete, and
    // release the shared lock before the test exits.
    await itHolder.next();
    await itHolder.next();
    expect(lock.isLocked()).toBe(false);
  });
});