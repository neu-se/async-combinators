import { withReentrantLock } from '../../src/stream/withReentrantLock';
import { ReentrantLock } from '../../src/reentrant-lock';
import { collect, arrayStream, failingStream, streamFn } from './helpers';
import { deferred, abortedSignal, expectAbortError } from '../helpers';

describe('stream/withReentrantLock', () => {
  it('yields all items from a single stream held under the lock', async () => {
    const lock = new ReentrantLock();
    const fn = streamFn().mockImplementation(() => arrayStream(['a', 'b', 'c']));
    const s = withReentrantLock(fn, lock);

    expect(await collect(s())).toEqual(['a', 'b', 'c']);
    expect(lock.isLocked()).toBe(false); // released on completion
    expect(fn).toHaveBeenCalledTimes(1);
  });

  describe('mutual exclusion', () => {
    it('serializes independent streams contending for the same lock', async () => {
      const lock = new ReentrantLock();
      const order: string[] = [];
      const mk = (name: string) =>
        async function* (): AsyncIterable<number> {
          order.push(`${name}:start`);
          yield 1;
          order.push(`${name}:end`);
        };
      const A = withReentrantLock(mk('A'), lock);
      const B = withReentrantLock(mk('B'), lock);

      const itA = A()[Symbol.asyncIterator]();
      const itB = B()[Symbol.asyncIterator]();
      await itA.next(); // A acquires and produces A:start
      const bFirst = itB.next(); // B blocks waiting for the lock
      await Promise.resolve(); // unnecessary but included to illustrate that the lock is held across ticks
      expect(order).toEqual(['A:start']); // B is blocked, not interleaved

      await itA.next(); // A completes → releases → hands the lock to B
      expect(order).toEqual(['A:start', 'A:end']); // B still blocked until the next tick
      await bFirst; // B now acquires and produces B:start
      expect(order).toEqual(['A:start', 'A:end', 'B:start']);
      await itB.next(); // B completes
      expect(order).toEqual(['A:start', 'A:end', 'B:start', 'B:end']);
    });

    it('creates its own lock when none is given, serializing the wrapper\'s own invocations', async () => {
      const order: string[] = [];
      const s = withReentrantLock(async function* (name: string): AsyncIterable<number> {
        order.push(`${name}:start`);
        yield 1;
        order.push(`${name}:end`);
      });

      const itA = s('A')[Symbol.asyncIterator]();
      const itB = s('B')[Symbol.asyncIterator]();
      await itA.next();
      const bFirst = itB.next();
      await Promise.resolve();
      await Promise.resolve();
      expect(order).toEqual(['A:start']); // shared default lock serializes the two calls

      await itA.next();
      await bFirst;
      await itB.next();
      expect(order).toEqual(['A:start', 'A:end', 'B:start', 'B:end']);
    });
  });

  describe('reentrancy', () => {
    it('lets a nested same-lock promise call re-enter during production (no deadlock)', async () => {
      const lock = new ReentrantLock();
      let nestedRan = false;
      const s = withReentrantLock(async function* (): AsyncIterable<number> {
        yield 1;
        // Made while this stream holds the lock — must re-enter, not wait on ourselves.
        await lock.runExclusive(async () => {
          nestedRan = true;
        });
        yield 2;
      }, lock);

      expect(await collect(s())).toEqual([1, 2]);
      expect(nestedRan).toBe(true);
    });

    it('lets a nested same-lock stream re-enter during production', async () => {
      const lock = new ReentrantLock();
      const nested = withReentrantLock(async function* (): AsyncIterable<string> {
        yield 'x';
        yield 'y';
      }, lock);
      const outer = withReentrantLock(async function* (): AsyncIterable<string> {
        yield 'w';
        const inner = await collect(nested()); // nested stream on the same lock
        yield inner.reduce((acc, v) => acc + v, ''); // flatten to a single string
        yield 'z';
      }, lock);

      expect(await collect(outer())).toEqual(['w', 'xy', 'z']);
    });

    it('allows a recursive streaming function to re-enter without deadlocking', async () => {
      const lock = new ReentrantLock();
      let recurse: (n: number) => AsyncIterable<number>;
      recurse = withReentrantLock(async function* (n: number): AsyncIterable<number> {
        yield n;
        if (n > 0) yield* recurse(n - 1); // reentrant nested stream
      }, lock);

      expect(await collect(recurse(3))).toEqual([3, 2, 1, 0]);
      expect(lock.isLocked()).toBe(false); // released on completion of the outermost stream
    });
  });

  describe('lock release on every exit', () => {
    it('releases the lock when the source errors', async () => {
      const lock = new ReentrantLock();
      const s = withReentrantLock(() => failingStream(new Error('boom'), ['a']), lock);

      await expect(collect(s())).rejects.toThrow('boom');
      expect(lock.isLocked()).toBe(false); // released even on error
    });

    it('releases the lock when the consumer abandons the stream', async () => {
      const lock = new ReentrantLock();
      let cleanedUp = false;
      const s = withReentrantLock(async function* (): AsyncIterable<number> {
        try {
          yield 1;
          yield 2;
        } finally {
          cleanedUp = true;
        }
      }, lock);

      for await (const _ of s()) break; // abandon after the first item
      expect(cleanedUp).toBe(true); // source torn down
      expect(lock.isLocked()).toBe(false); // and the lock released
    });
  });

  describe('laziness and cancellation', () => {
    it('acquires nothing for a stream that is created but never iterated', () => {
      const lock = new ReentrantLock();
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['x']));
      const s = withReentrantLock(sourceFn, lock);

      s(); // created, never iterated
      expect(sourceFn).not.toHaveBeenCalled();
      expect(lock.isLocked()).toBe(false);
    });

    it('rejects at the first pull if the signal is already aborted, without acquiring', async () => {
      const lock = new ReentrantLock();
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['x']));
      const s = withReentrantLock(sourceFn, lock);

      await expectAbortError(collect(s({ signal: abortedSignal() })));
      expect(sourceFn).not.toHaveBeenCalled();
      expect(lock.isLocked()).toBe(false);
    });

    it('rejects without acquiring when aborted while waiting for the lock', async () => {
      const lock = new ReentrantLock();
      const gate = deferred();
      const holder = withReentrantLock(async function* (): AsyncIterable<number> {
        yield 1;
        await gate.promise; // keep holding the lock
        yield 2;
      }, lock);
      const itHolder = holder()[Symbol.asyncIterator]();
      await itHolder.next(); // holder acquires the lock

      const controller = new AbortController();
      const waiterSource = streamFn<number>().mockImplementation((..._args: unknown[]) => arrayStream([9]));
      const waiter = withReentrantLock(waiterSource, lock);
      const waiterDone = collect(waiter({ signal: controller.signal }));
      const waiterRejects = expectAbortError(waiterDone); // attach handler before aborting
      await Promise.resolve(); // let the waiter queue on the lock
      controller.abort();
      await waiterRejects; // aborted out of the queue, never acquired
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
