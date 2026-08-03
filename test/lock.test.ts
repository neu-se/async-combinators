import { Lock } from '../src/lock';
import { withTimeout } from '../src/withTimeout';
import { pauseMicrotask, abortedSignal, expectAbortError } from './helpers';

describe('AsyncLock', () => {
  describe('acquire', () => {
    it('holds the lock on acquire and frees it via the returned releaser', async () => {
      const lock = new Lock();
      expect(lock.isLocked()).toBe(false); // idle

      const release = await lock.acquire();
      expect(lock.isLocked()).toBe(true); // held

      release();
      expect(lock.isLocked()).toBe(false); // released — free to re-acquire
    });

    it('enforces mutual exclusion', async () => {
      const lock = new Lock();
      let counter = 0;
      let maxConcurrent = 0;
      let currentConcurrent = 0;

      const criticalSection = async () => {
        const release = await lock.acquire();
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);

        const initialCounter = counter;
        // Yield one microtask turn so competing critical sections can interleave.
        await pauseMicrotask();
        counter = initialCounter + 1;

        currentConcurrent--;
        release();
      };

      await Promise.all([
        criticalSection(),
        criticalSection(),
        criticalSection(),
        criticalSection(),
      ]);

      expect(counter).toBe(4); // no lost updates
      expect(maxConcurrent).toBe(1); // never two holders at once
    });

    it('serves waiters in FIFO order', async () => {
      const lock = new Lock();
      const order: number[] = [];

      const releaseInitial = await lock.acquire();

      const waiter = (id: number) =>
        (async () => {
          const release = await lock.acquire();
          order.push(id);
          release();
        })();

      const waiters = [waiter(1), waiter(2), waiter(3)];

      releaseInitial();
      await Promise.all(waiters);

      expect(order).toEqual([1, 2, 3]);
    });

    it('calling release twice cannot free a lock held by another', async () => {
      const lock = new Lock();
      const releaseA = await lock.acquire(); // A holds

      let bHolds = false;
      const b = (async () => {
        const releaseB = await lock.acquire(); // B waits behind A
        bHolds = true;
        return releaseB;
      })();

      releaseA(); // hand the lock to B
      releaseA(); // duplicate release — must be a no-op

      const releaseB = await b;
      expect(bHolds).toBe(true);
      expect(lock.isLocked()).toBe(true); // B still holds it — the duplicate release didn't free it

      releaseB();
      expect(lock.isLocked()).toBe(false); // now actually free
    });
  });

  describe('runExclusive', () => {
    it('serializes concurrent calls and returns each result', async () => {
      const lock = new Lock();
      let current = 0;
      let max = 0;

      const task = (n: number) =>
        lock.runExclusive(async () => {
          current++;
          max = Math.max(max, current);
          // Yield one microtask turn so concurrent callers can interleave.
          await pauseMicrotask();
          current--;
          return n * 2;
        });

      const results = await Promise.all([task(1), task(2), task(3)]);

      expect(results).toEqual([2, 4, 6]);
      expect(max).toBe(1); // never overlapping
    });

    it('releases the lock even when fn throws', async () => {
      const lock = new Lock();

      await expect(
        lock.runExclusive(async () => {
          throw new Error('boom');
        })
      ).rejects.toThrow('boom');

      expect(lock.isLocked()).toBe(false); // released despite the throw
    });

    it('deadlocks on recursion — the lock is non-reentrant', async () => {
      const lock = new Lock();

      const recurse = (depth: number): Promise<void> =>
        lock.runExclusive(async () => {
          if (depth > 0) await recurse(depth - 1); // re-acquire while still holding
        });

      // The nested runExclusive waits for a lock the outer call holds and won't
      // release until the nested call returns — a permanent deadlock. This is the
      // defining difference from ReentrantLock, which re-enters instead.
      // withTimeout converts the hang into a detectable rejection.
      const guarded = withTimeout(recurse, 100);
      await expect(guarded(1)).rejects.toThrow('Operation timed out');
    });
  });

  describe('cancellation', () => {
    it('acquire: a waiter aborted while queued rejects and is dequeued; the holder keeps the lock', async () => {
      const lock = new Lock();
      const releaseFromHolder = await lock.acquire(); // holder holds the lock
      const controller = new AbortController();

      const waiter = lock.acquire(controller.signal); // queued behind the holder
      controller.abort();
      await expectAbortError(waiter);

      expect(lock.isLocked()).toBe(true); // the holder still owns it
      releaseFromHolder();
      expect(lock.isLocked()).toBe(false); // no queued waiter acquired the lock
    });

    it('acquire: rejects immediately if the signal is already aborted', async () => {
      const lock = new Lock();

      await expectAbortError(lock.acquire(abortedSignal()));
      expect(lock.isLocked()).toBe(false); // never acquired
    });

    it('runExclusive: never runs fn when the signal aborts while waiting for the lock', async () => {
      const lock = new Lock();
      const releaseFromHolder = await lock.acquire(); // holder holds the lock
      const controller = new AbortController();
      const fn = jest.fn(async () => 'done');

      const pending = lock.runExclusive(fn, controller.signal); // waits behind the holder
      controller.abort();
      await expectAbortError(pending);
      expect(fn).not.toHaveBeenCalled();

      releaseFromHolder();
    });
  });
});
