import { withLock } from '../../src/stream/withLock';
import { Lock } from '../../src/lock';
import { collect, arrayStream, failingStream, streamFn } from './helpers';
import { deferred, abortedSignal, expectAbortError, pauseMicrotask } from '../helpers';

describe('stream/withLock', () => {
  it('yields all items from a single stream held under the lock', async () => {
    const lock = new Lock();
    const fn = streamFn().mockImplementation(() => arrayStream(['a', 'b', 'c']));
    const s = withLock(fn, lock);

    expect(await collect(s())).toEqual(['a', 'b', 'c']);
    expect(lock.isLocked()).toBe(false); // released on completion
    expect(fn).toHaveBeenCalledTimes(1);
  });

  describe('mutual exclusion', () => {
    it('serializes independent streams contending for the same lock', async () => {
      const lock = new Lock();
      const order: string[] = [];
      const mk = (name: string) =>
        async function* (): AsyncIterable<number> {
          order.push(`${name}:start`);
          yield 1;
          order.push(`${name}:end`);
        };
      const A = withLock(mk('A'), lock);
      const B = withLock(mk('B'), lock);

      const itA = A()[Symbol.asyncIterator]();
      const itB = B()[Symbol.asyncIterator]();
      await itA.next(); // A acquires and produces A:start
      const bFirst = itB.next(); // B blocks waiting for the lock
      await pauseMicrotask();
      expect(order).toEqual(['A:start']);

      await itA.next(); // A completes and releases
      expect(order).toEqual(['A:start', 'A:end']);
      await bFirst;
      expect(order).toEqual(['A:start', 'A:end', 'B:start']);
      await itB.next();
      expect(order).toEqual(['A:start', 'A:end', 'B:start', 'B:end']);
      expect(lock.isLocked()).toBe(false);
    });

    it('creates its own lock when none is given, serializing the wrapper\'s own invocations', async () => {
      const order: string[] = [];
      const s = withLock(async function* (name: string): AsyncIterable<number> {
        order.push(`${name}:start`);
        yield 1;
        order.push(`${name}:end`);
      });

      const itA = s('A')[Symbol.asyncIterator]();
      const itB = s('B')[Symbol.asyncIterator]();
      await itA.next();
      const bFirst = itB.next();
      await pauseMicrotask();
      await pauseMicrotask();
      expect(order).toEqual(['A:start']);

      await itA.next();
      await bFirst;
      await itB.next();
      expect(order).toEqual(['A:start', 'A:end', 'B:start', 'B:end']);
    });

    it('allows overlap when the two streams use different locks', async () => {
      const lockA = new Lock();
      const lockB = new Lock();
      const gate = deferred();

      let currentConcurrent = 0;
      let maxConcurrent = 0;
      const mk = (name: string) =>
        async function* (): AsyncIterable<string> {
          currentConcurrent++;
          maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
          try {
            yield `${name}:first`;
            await gate.promise; // keep both streams active at the same time
            yield `${name}:second`;
          } finally {
            currentConcurrent--;
          }
        };

      const A = withLock(mk('A'), lockA);
      const B = withLock(mk('B'), lockB);

      // Start both streams; because their locks are independent, both should
      // enter and remain active concurrently while blocked on the gate.
      const itA = A()[Symbol.asyncIterator]();
      const itB = B()[Symbol.asyncIterator]();
      await Promise.all([itA.next(), itB.next()]);
      expect(maxConcurrent).toBe(2);

      gate.resolve();
      await Promise.all([itA.next(), itA.next(), itB.next(), itB.next()]);
      expect(lockA.isLocked()).toBe(false);
      expect(lockB.isLocked()).toBe(false);
    });
  });

  describe('lock release on every exit', () => {
    it('releases the lock when the source errors', async () => {
      const lock = new Lock();
      const s = withLock(() => failingStream(new Error('boom'), ['a']), lock);

      await expect(collect(s())).rejects.toThrow('boom');
      expect(lock.isLocked()).toBe(false);
    });

    it('releases the lock when the consumer abandons the stream', async () => {
      const lock = new Lock();
      let cleanedUp = false;
      const s = withLock(async function* (): AsyncIterable<number> {
        try {
          yield 1;
          yield 2;
        } finally {
          cleanedUp = true;
        }
      }, lock);

      for await (const _ of s()) break;
      expect(cleanedUp).toBe(true);
      expect(lock.isLocked()).toBe(false);
    });
  });

  describe('laziness and cancellation', () => {
    it('acquires nothing for a stream that is created but never iterated', () => {
      const lock = new Lock();
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['x']));
      const s = withLock(sourceFn, lock);

      s();
      expect(sourceFn).not.toHaveBeenCalled();
      expect(lock.isLocked()).toBe(false);
    });

    it('rejects at the first pull if the signal is already aborted, without acquiring', async () => {
      const lock = new Lock();
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['x']));
      const s = withLock(sourceFn, lock);

      await expectAbortError(collect(s({ signal: abortedSignal() })));
      expect(sourceFn).not.toHaveBeenCalled();
      expect(lock.isLocked()).toBe(false);
    });

    it('rejects without acquiring when aborted while waiting for the lock', async () => {
      const lock = new Lock();
      const gate = deferred();
      const holder = withLock(async function* (): AsyncIterable<number> {
        yield 1;
        await gate.promise;
        yield 2;
      }, lock);
      const itHolder = holder()[Symbol.asyncIterator]();
      await itHolder.next(); // holder acquires the lock

      const controller = new AbortController();
      const waiterSource = streamFn<number>().mockImplementation((..._args: unknown[]) => arrayStream([9]));
      const waiter = withLock(waiterSource, lock);
      const waiterDone = collect(waiter({ signal: controller.signal }));
      const waiterRejects = expectAbortError(waiterDone);
      await pauseMicrotask(); // let the waiter queue
      controller.abort();
      await waiterRejects;
      expect(waiterSource).not.toHaveBeenCalled();

      // The holder still owns the lock; drain it to release cleanly.
      expect(lock.isLocked()).toBe(true);
      gate.resolve();
      await itHolder.next();
      await itHolder.next();
      expect(lock.isLocked()).toBe(false);
    });
  });
});
