import { withReentrantLock } from '../src/withReentrantLock';
import { ReentrantLock } from '../src/reentrant-lock';
import { rejectOnAbort } from '../src/core/cancellation';
import { deferred, pauseMicrotask, abortedSignal, expectAbortError } from './helpers';

describe('withReentrantLock', () => {
  describe('mutual exclusion & independence', () => {
    it('should enforce sequential execution with a default lock', async () => {
      let counter = 0;
      let maxConcurrent = 0;
      let currentConcurrent = 0;

      const incrementFn = async (amount: number): Promise<number> => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        const current = counter;
        // Yield one microtask turn so concurrent callers can interleave.
        await pauseMicrotask();
        counter = current + amount;
        currentConcurrent--;
        return counter;
      };

      const lockedFn = withReentrantLock(incrementFn); // default lock

      const results = await Promise.all([lockedFn(1), lockedFn(2), lockedFn(3)]);

      expect(counter).toBe(6); // 1+2+3, no lost updates
      expect(maxConcurrent).toBe(1); // only one at a time
      expect(results).toEqual([1, 3, 6]); // sequential execution
    });

    it('should allow concurrent execution between different default locks', async () => {
      let counter1 = 0;
      let counter2 = 0;
      let maxConcurrent = 0;
      let currentConcurrent = 0;

      const incrementFn1 = async (amount: number): Promise<number> => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        counter1 += amount;
        // Yield one microtask turn so the independent lock can interleave.
        await pauseMicrotask();
        currentConcurrent--;
        return counter1;
      };

      const incrementFn2 = async (amount: number): Promise<number> => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        counter2 += amount;
        // Yield one microtask turn so the independent lock can interleave.
        await pauseMicrotask();
        currentConcurrent--;
        return counter2;
      };

      // Each wrapper gets its own default lock instance.
      const lockedFn1 = withReentrantLock(incrementFn1);
      const lockedFn2 = withReentrantLock(incrementFn2);

      const results = await Promise.all([
        lockedFn1(10),
        lockedFn2(20),
        lockedFn1(30),
        lockedFn2(40)
      ]);

      expect(counter1).toBe(40); // 10+30
      expect(counter2).toBe(60); // 20+40
      expect(results).toEqual([10, 20, 40, 60]);
      // Different default locks overlap (peak 2); each still serializes its pair.
      expect(maxConcurrent).toBe(2);
    });

    it('should enforce sequential execution with an explicit lock', async () => {
      const lock = new ReentrantLock();
      let counter = 0;
      let maxConcurrent = 0;
      let currentConcurrent = 0;

      const incrementFn = async (amount: number): Promise<number> => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        const current = counter;
        // Yield one microtask turn so concurrent callers can interleave.
        await pauseMicrotask();
        counter = current + amount;
        currentConcurrent--;
        return counter;
      };

      const lockedFn = withReentrantLock(incrementFn, lock);

      const results = await Promise.all([lockedFn(1), lockedFn(2), lockedFn(3)]);

      expect(counter).toBe(6); // 1+2+3, no lost updates
      expect(maxConcurrent).toBe(1); // only one at a time
      expect(results).toEqual([1, 3, 6]); // sequential execution
    });

    it('should allow concurrent execution between different explicit locks', async () => {
      const lock1 = new ReentrantLock();
      const lock2 = new ReentrantLock();
      let counter1 = 0;
      let counter2 = 0;
      let maxConcurrent = 0;
      let currentConcurrent = 0;

      const incrementFn1 = async (amount: number): Promise<number> => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        counter1 += amount;
        // Yield one microtask turn so the independent lock can interleave.
        await pauseMicrotask();
        currentConcurrent--;
        return counter1;
      };

      const incrementFn2 = async (amount: number): Promise<number> => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        counter2 += amount;
        // Yield one microtask turn so the independent lock can interleave.
        await pauseMicrotask();
        currentConcurrent--;
        return counter2;
      };

      const lockedFn1 = withReentrantLock(incrementFn1, lock1);
      const lockedFn2 = withReentrantLock(incrementFn2, lock2);

      const results = await Promise.all([
        lockedFn1(10),
        lockedFn2(20),
        lockedFn1(30),
        lockedFn2(40)
      ]);

      expect(counter1).toBe(40); // 10+30
      expect(counter2).toBe(60); // 20+40
      expect(results).toEqual([10, 20, 40, 60]);
      // Different locks overlap (peak 2); each still serializes its pair.
      expect(maxConcurrent).toBe(2);
    });

    it('should execute functions sharing a lock in FIFO order', async () => {
      const lock = new ReentrantLock();
      const executionOrder: string[] = [];
      let currentConcurrent = 0;
      let maxConcurrent = 0;

      const createTask = (id: string) => withReentrantLock(async () => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        executionOrder.push(id);
        // Yield one microtask turn so queued tasks can interleave at lock boundaries.
        await pauseMicrotask();
        currentConcurrent--;
        return id;
      }, lock);

      const task1 = createTask('task1');
      const task2 = createTask('task2');
      const task3 = createTask('task3');

      const results = await Promise.all([task1(), task2(), task3()]);

      expect(executionOrder).toEqual(['task1', 'task2', 'task3']);
      expect(results).toEqual(['task1', 'task2', 'task3']);
      // Same lock forces FIFO order with no overlap.
      expect(maxConcurrent).toBe(1);
    });
  });

  describe('error handling', () => {
    it('should release the lock when the function throws', async () => {
      const lock = new ReentrantLock();
      const failingFn = withReentrantLock(async () => {
        throw new Error('test error');
      }, lock);

      await expect(failingFn()).rejects.toThrow('test error');
      expect(lock.isLocked()).toBe(false); // released despite the throw
    });

    it('should release the lock when a reentrant call throws', async () => {
      const lock = new ReentrantLock();

      let recurse = async (depth: number): Promise<void> => {
        if (depth <= 0) throw new Error('deep');
        await recurse(depth - 1);
      };
      recurse = withReentrantLock(recurse, lock);

      await expect(recurse(2)).rejects.toThrow('deep');
      expect(lock.isLocked()).toBe(false); // outer frame releases even when the throw is deep in recursion
    });
  });

  describe('reentrant behavior', () => {
    it('should protect a recursive function without deadlocking', async () => {
      const lock = new ReentrantLock();

      let recursiveFunction = async (depth: number): Promise<number> => {
        if (depth <= 0) return 0;
        return 1 + await recursiveFunction(depth - 1);
      };

      // Reassign so recursive calls go through the wrapped function.
      recursiveFunction = withReentrantLock(recursiveFunction, lock);

      expect(await recursiveFunction(3)).toBe(3);
    });

    it('should protect mutually recursive functions sharing one lock', async () => {
      const lock = new ReentrantLock();

      let fnA = async (n: number): Promise<number> => {
        if (n <= 0) return 0;
        return 1 + await fnB(n - 1);
      };
      let fnB = async (n: number): Promise<number> => {
        if (n <= 0) return 0;
        return 1 + await fnA(n - 1);
      };

      // Reassign so the mutual calls go through the wrapped functions.
      fnA = withReentrantLock(fnA, lock);
      fnB = withReentrantLock(fnB, lock);

      expect(await fnA(3)).toBe(3);
    });
  });

  describe('cancellation', () => {
    it('drops a call waiting for a contended lock when its signal aborts', async () => {
      const gate = deferred();
      // fn declares a trailing { signal } option — the opt-in for cancellation. The
      // wrapper forwards that signal to the lock, so an abort abandons the lock wait.
      const fn = jest.fn(async (_opts?: { signal?: AbortSignal }) => {
        // Keep the holder active until the test explicitly releases this gate.
        await gate.promise;
        return 'done';
      });
      const lock = new ReentrantLock();
      const guarded = withReentrantLock(fn, lock);
      const controller = new AbortController();

      const first = guarded();                                // acquires the lock, starts running
      const waiter = guarded({ signal: controller.signal });  // blocks waiting for the lock
      // Yield one microtask turn so `first` acquires and enters fn before assertions.
      await pauseMicrotask();
      expect(fn).toHaveBeenCalledTimes(1);                    // only `first` has started

      controller.abort(); // abort while the second call is still waiting on the lock
      await expectAbortError(waiter);
      // The waiter gave up before acquiring — its fn is never invoked.
      expect(fn).toHaveBeenCalledTimes(1);

      gate.resolve();
      await first; // let the holder finish and release
      expect(lock.isLocked()).toBe(false); // no queued waiter is left holding the lock
    });

    it('rejects immediately if the signal is already aborted, without acquiring or calling fn', async () => {
      const fn = jest.fn(async (_opts?: { signal?: AbortSignal }) => 'done');
      const lock = new ReentrantLock();
      const guarded = withReentrantLock(fn, lock);

      await expectAbortError(guarded({ signal: abortedSignal() }));
      expect(fn).not.toHaveBeenCalled(); // never ran
      expect(lock.isLocked()).toBe(false); // never acquired
    });

    it('aborting a running worker frees the lock so the next queued worker runs', async () => {
      const lock = new ReentrantLock();

      // Timer-free abortable work: with a signal, wait for abort; without a
      // signal, finish on the next microtask.
      const guarded = withReentrantLock(
        async (id: string, opts?: { signal?: AbortSignal }) => {
          if (opts?.signal) {
            const aborted = rejectOnAbort(opts.signal);
            try {
              await aborted.promise;
            } finally {
              aborted.cleanup();
            }
          } else {
            await pauseMicrotask();
          }
          return id;
        },
        lock,
      );

      const controller1 = new AbortController();
      const worker1 = guarded('one', { signal: controller1.signal }); // holds, doing work
      const worker2 = guarded('two');                                 // queues behind it

      controller1.abort();               // worker 1's in-flight work fails
      await expectAbortError(worker1);   // holder aborted → released the lock
      expect(await worker2).toBe('two'); // worker 2 then ran to completion
      expect(lock.isLocked()).toBe(false);
    });
  });
});
