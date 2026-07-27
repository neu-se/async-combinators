import { Semaphore } from '../src/semaphore';
import { withTimeout } from '../src/withTimeout';
import { pauseMicrotask, abortedSignal, expectAbortError } from './helpers';

describe('AsyncSemaphore', () => {
  describe('acquire', () => {
    it('tracks free permits via available() as they are held and released', async () => {
      const sem = new Semaphore(2);
      expect(sem.available()).toBe(2); // idle

      const release1 = await sem.acquire();
      expect(sem.available()).toBe(1);
      const release2 = await sem.acquire();
      expect(sem.available()).toBe(0); // fully subscribed

      release1();
      expect(sem.available()).toBe(1);
      release2();
      expect(sem.available()).toBe(2); // all free again
    });

    it('limits concurrency to `permits` holders', async () => {
      const sem = new Semaphore(2);
      let currentConcurrent = 0;
      let maxConcurrent = 0;

      const task = async () => {
        const release = await sem.acquire();
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        // Yield one microtask turn so queued tasks can interleave while permits are held.
        await pauseMicrotask();
        currentConcurrent--;
        release();
      };

      // 5 tasks, 2 permits → never more than 2 holding at once.
      await Promise.all([task(), task(), task(), task(), task()]);
      expect(maxConcurrent).toBe(2);
    });

    it('behaves like a mutex when permits is 1', async () => {
      const sem = new Semaphore(1);
      let currentConcurrent = 0;
      let maxConcurrent = 0;

      const task = async () => {
        const release = await sem.acquire();
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        // Yield one microtask turn so queued tasks can interleave at permit boundaries.
        await pauseMicrotask();
        currentConcurrent--;
        release();
      };

      await Promise.all([task(), task(), task()]);
      expect(maxConcurrent).toBe(1); // strictly one at a time
    });

    it('serves waiters in FIFO order', async () => {
      const sem = new Semaphore(1);
      const order: number[] = [];

      const releaseInitial = await sem.acquire(); // take the only permit

      const waiter = (id: number) =>
        (async () => {
          const release = await sem.acquire();
          order.push(id);
          release();
        })();

      const waiters = [waiter(1), waiter(2), waiter(3)];

      releaseInitial();
      await Promise.all(waiters);

      expect(order).toEqual([1, 2, 3]);
    });

    it('calling release twice cannot free a permit held by another', async () => {
      const sem = new Semaphore(1);
      const holders = () => 1 - sem.available(); // permits (1) minus the free permits

      const releaseA = await sem.acquire(); // A holds the only permit
      expect(holders()).toBe(1);

      let bHolds = false;
      const b = (async () => {
        const releaseB = await sem.acquire(); // B waits behind A
        bHolds = true;
        return releaseB;
      })();

      releaseA(); // hand the permit to B
      const holdersAfterHandoff = holders();
      releaseA(); // duplicate release — must be a no-op
      expect(holders()).toBe(holdersAfterHandoff); // the duplicate release did not change the holder count

      const releaseB = await b;
      expect(bHolds).toBe(true);
      expect(holders()).toBe(1); // still exactly one holder (B); the duplicate release didn't free it

      releaseB();
      expect(holders()).toBe(0); // now actually free
    });
  });

  describe('runExclusive', () => {
    it('bounds concurrency and returns each result', async () => {
      const sem = new Semaphore(2);
      let currentConcurrent = 0;
      let maxConcurrent = 0;

      const task = (n: number) =>
        sem.runExclusive(async () => {
          currentConcurrent++;
          maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
          // Yield one microtask turn so concurrent holders can overlap while
          // permits are held, without introducing real timers.
          await pauseMicrotask();
          currentConcurrent--;
          return n * 2;
        });

      const results = await Promise.all([task(1), task(2), task(3), task(4)]);

      expect(results).toEqual([2, 4, 6, 8]);
      expect(maxConcurrent).toBe(2); // never more than 2 overlapping
    });

    it('releases the permit even when fn throws', async () => {
      const sem = new Semaphore(1);

      await expect(
        sem.runExclusive(async () => {
          throw new Error('boom');
        })
      ).rejects.toThrow('boom');

      expect(sem.available()).toBe(1); // released despite the throw
    });

    it('is non-reentrant: recursion under a permits=1 semaphore deadlocks', async () => {
      const sem = new Semaphore(1);

      const recurse = (depth: number): Promise<void> =>
        sem.runExclusive(async () => {
          if (depth > 0) await recurse(depth - 1); // re-acquire while still holding the only permit
        });

      // The nested acquire waits for a permit the outer call holds and won't release
      // until the nested call returns — a permanent deadlock (this is what a
      // ReentrantAsyncSemaphore would avoid). withTimeout turns the hang into a rejection.
      const guarded = withTimeout(recurse, 100);
      await expect(guarded(1)).rejects.toThrow('Operation timed out');
    });
  });

  describe('cancellation', () => {
    it('acquire: a waiter aborted while queued rejects and is dequeued; the holder keeps its permit', async () => {
      const sem = new Semaphore(1);
      const releaseFromHolder = await sem.acquire(); // holder takes the only permit
      const controller = new AbortController();

      const waiter = sem.acquire(controller.signal); // queued behind the holder
      controller.abort();
      await expectAbortError(waiter);

      expect(sem.available()).toBe(0); // the holder still owns its permit
      releaseFromHolder();
      expect(sem.available()).toBe(1); // no queued waiter acquired the permit
    });

    it('acquire: rejects immediately if the signal is already aborted', async () => {
      const sem = new Semaphore(2);

      await expectAbortError(sem.acquire(abortedSignal()));
      expect(sem.available()).toBe(2); // never acquired
    });

    it('runExclusive: never runs fn when the signal aborts while waiting for a permit', async () => {
      const sem = new Semaphore(1);
      const releaseFromHolder = await sem.acquire(); // holder takes the only permit
      const controller = new AbortController();
      const fn = jest.fn(async () => 'done');

      const pending = sem.runExclusive(fn, controller.signal); // waits behind the holder
      controller.abort();
      await expectAbortError(pending);
      expect(fn).not.toHaveBeenCalled();

      releaseFromHolder();
    });
  });

  describe('invalid permits', () => {
    it('throws for zero', () => {
      expect(() => new Semaphore(0)).toThrow('permits must be a positive integer');
    });

    it('throws for negative values', () => {
      expect(() => new Semaphore(-1)).toThrow('permits must be a positive integer');
    });

    it('throws for non-integer values', () => {
      expect(() => new Semaphore(1.5)).toThrow('permits must be a positive integer');
    });

    it('throws for NaN and Infinity', () => {
      expect(() => new Semaphore(NaN)).toThrow('permits must be a positive integer');
      expect(() => new Semaphore(Infinity)).toThrow('permits must be a positive integer');
    });
  });
});
