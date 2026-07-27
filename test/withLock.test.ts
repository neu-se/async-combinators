import { withLock } from '../src/withLock';
import { Lock } from '../src/lock';
import { withTimeout } from '../src/withTimeout';
import { rejectOnAbort } from '../src/core/cancellation';
import { deferred, pauseMicrotask, abortedSignal, expectAbortError } from './helpers';

describe('withLock', () => {
  describe('mutual exclusion & independence', () => {
    it('should enforce sequential execution with default lock', async () => {
      let counter = 0;
      
      let currentConcurrent = 0;
      let maxConcurrent = 0;

      const incrementFn = async (amount: number) => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        const current = counter;
        // Yield one microtask turn so concurrent callers can interleave.
        await pauseMicrotask();
        counter = current + amount;
        currentConcurrent--;
        return counter;
      };

      const protectedIncrement = withLock(incrementFn); // Uses default lock

      const results = await Promise.all([
        protectedIncrement(1),
        protectedIncrement(2),
        protectedIncrement(3)
      ]);

      expect(counter).toBe(6); // 1+2+3
      expect(results).toEqual([1, 3, 6]); // Sequential execution results
      // Default lock prevents overlap: only one call is ever inside at a time.
      expect(maxConcurrent).toBe(1);
    });

    it('should allow concurrent execution between different default locks', async () => {
      let counter1 = 0;
      let counter2 = 0;
      
      // Track overlap deterministically instead of relying on real-timer timing.
      let currentConcurrent = 0;
      let maxConcurrent = 0;

      const incrementFn1 = async (amount: number) => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        counter1 += amount;
        // Yield one microtask turn so the independent lock can interleave.
        await pauseMicrotask();
        currentConcurrent--;
        return counter1;
      };
      
      const incrementFn2 = async (amount: number) => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        counter2 += amount;
        // Yield one microtask turn so the independent lock can interleave.
        await pauseMicrotask();
        currentConcurrent--;
        return counter2;
      };
      
      // Each gets its own default lock instance
      const protectedIncrement1 = withLock(incrementFn1);
      const protectedIncrement2 = withLock(incrementFn2);
      
      const results = await Promise.all([
        protectedIncrement1(10),
        protectedIncrement2(20),
        protectedIncrement1(30),
        protectedIncrement2(40)
      ]);

      expect(counter1).toBe(40); // 10+30
      expect(counter2).toBe(60); // 20+40
      expect(results).toEqual([10, 20, 40, 60]);
      
      // Exactly 2: fn1 and fn2 use different locks so they overlap (peak reaches
      // 2), while each lock still serializes its own two calls (so the peak never
      // exceeds 2). Measured by observed overlap, not wall-clock time.
      expect(maxConcurrent).toBe(2);
    });

    it('should enforce sequential execution with an explicit lock', async () => {
      const lock = new Lock();
      let counter = 0;

      let currentConcurrent = 0;
      let maxConcurrent = 0;

      const incrementFn = async (amount: number) => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        const current = counter;
        // Yield one microtask turn so concurrent callers can interleave.
        await pauseMicrotask();
        counter = current + amount;
        currentConcurrent--;
        return counter;
      };

      const protectedIncrement = withLock(incrementFn, lock); // Explicit lock

      const results = await Promise.all([
        protectedIncrement(1),
        protectedIncrement(2),
        protectedIncrement(3)
      ]);

      expect(counter).toBe(6); // 1+2+3
      expect(results).toEqual([1, 3, 6]); // Sequential execution results
      // Explicit lock prevents overlap: only one call is ever inside at a time.
      expect(maxConcurrent).toBe(1);
    });

    it('should allow concurrent execution between different explicit locks', async () => {
      const lock1 = new Lock();
      const lock2 = new Lock();
      let counter1 = 0;
      let counter2 = 0;

      // Track overlap deterministically instead of relying on real-timer timing.
      let currentConcurrent = 0;
      let maxConcurrent = 0;

      const incrementFn1 = async (amount: number) => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        counter1 += amount;
        // Yield one microtask turn so the independent lock can interleave.
        await pauseMicrotask();
        currentConcurrent--;
        return counter1;
      };

      const incrementFn2 = async (amount: number) => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        counter2 += amount;
        // Yield one microtask turn so the independent lock can interleave.
        await pauseMicrotask();
        currentConcurrent--;
        return counter2;
      };

      const protectedIncrement1 = withLock(incrementFn1, lock1);
      const protectedIncrement2 = withLock(incrementFn2, lock2);

      const results = await Promise.all([
        protectedIncrement1(10),
        protectedIncrement2(20),
        protectedIncrement1(30),
        protectedIncrement2(40)
      ]);

      expect(counter1).toBe(40); // 10+30
      expect(counter2).toBe(60); // 20+40
      expect(results).toEqual([10, 20, 40, 60]);
      // Exactly 2: lock1 and lock2 are independent so they overlap (peak reaches
      // 2), while each lock still serializes its own two calls (never exceeds 2).
      expect(maxConcurrent).toBe(2);
    });

    it('should execute functions sharing a lock in FIFO order', async () => {
      const lock = new Lock();
      const executionOrder: string[] = [];
      let currentConcurrent = 0;
      let maxConcurrent = 0;

      const createTask = (id: string) => withLock(async () => {
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
    it('should release the lock when the wrapped function throws', async () => {
      const lock = new Lock();
      const failingFn = withLock(async () => {
        throw new Error('test error');
      }, lock);

      await expect(failingFn()).rejects.toThrow('test error');
      expect(lock.isLocked()).toBe(false); // released despite the throw
    });

    it('should keep serializing when some calls throw', async () => {
      const lock = new Lock();
      let executionCount = 0;
      let maxConcurrent = 0;
      let currentConcurrent = 0;

      const errorFn = withLock(async (shouldFail: boolean) => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        executionCount++;
        // Yield one microtask turn so queued calls can interleave at lock boundaries.
        await pauseMicrotask();
        currentConcurrent--;
        if (shouldFail) throw new Error('boom');
        return 'ok';
      }, lock);

      // A throwing holder must still hand the lock to the calls queued behind it:
      // all four run, and none overlap.
      await Promise.allSettled([
        errorFn(true),
        errorFn(false),
        errorFn(true),
        errorFn(false)
      ]);

      expect(executionCount).toBe(4); // every queued call ran, despite the throws
      expect(maxConcurrent).toBe(1); // still serialized — a throw never lets a second in
    });

  });

  describe('reentrancy behavior', () => {
    it('should deadlock on direct recursion due to non-reentrant nature', async () => {
      const lock = new Lock();
      
      const recursiveFn = withLock(async (depth: number): Promise<number> => {
        if (depth <= 0) return 0;
        
        // This will deadlock because the lock is already held by the current execution
        // and non-reentrant locks cannot be acquired again by the same execution context
        return 1 + await recursiveFn(depth - 1);
      }, lock);
      
      // Use withTimeout to detect the deadlock
      const timeoutFn = withTimeout(recursiveFn, 100);
      
      await expect(timeoutFn(2)).rejects.toThrow('Operation timed out');
      
      // NOTE: For reentrant behavior, use withReentrantLock instead:
      // const reentrantFn = withReentrantLock(recursiveFn);
      // This would allow the same execution context to acquire the lock multiple times
    });

    it('should deadlock on indirect recursion', async () => {
      const sharedLock = new Lock();
      
      // Create two functions that will call each other, both protected by the same lock
      let fnB: (n: number) => Promise<number>;
      
      const fnA = withLock(async (n: number): Promise<number> => {
        if (n <= 0) return 0;
        return 1 + await fnB(n - 1); // This will deadlock
      }, sharedLock);
      
      fnB = withLock(async (n: number): Promise<number> => {
        if (n <= 0) return 0;
        return 1 + await fnA(n - 1); // This will also deadlock
      }, sharedLock);
      
      // Use withTimeout to detect the indirect recursion deadlock
      const timeoutFnA = withTimeout(fnA, 100);
      
      await expect(timeoutFnA(2)).rejects.toThrow('Operation timed out');
      
      // NOTE: For scenarios requiring reentrant behavior across multiple functions
      // sharing the same lock, use withReentrantLock with a shared ReentrantLock instance
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
      const lock = new Lock();
      const guarded = withLock(fn, lock);
      const controller = new AbortController();

      const first = guarded();                                // acquires the lock, starts running
      const waiter = guarded({ signal: controller.signal });  // blocks waiting for the lock
      // Yield one microtask turn so `first` acquires and enters fn before assertions.
      await pauseMicrotask();
      expect(fn).toHaveBeenCalledTimes(1);                    // only `first` has started

      controller.abort(); // abort while the second call is still waiting on the lock
      await expectAbortError(waiter);
      // The waiter gave up before acquiring — it never invoked fn.
      expect(fn).toHaveBeenCalledTimes(1);

      gate.resolve();
      await first; // let the holder finish and release
      expect(lock.isLocked()).toBe(false); // no queued waiter is left holding the lock
    });

    it('rejects immediately if the signal is already aborted, without acquiring or calling fn', async () => {
      const fn = jest.fn(async (_opts?: { signal?: AbortSignal }) => 'done');
      const lock = new Lock();
      const guarded = withLock(fn, lock);

      await expectAbortError(guarded({ signal: abortedSignal() }));
      expect(fn).not.toHaveBeenCalled(); // never ran
      expect(lock.isLocked()).toBe(false); // never acquired
    });

    it('aborting a running holder frees the lock so the next queued call runs', async () => {
      const lock = new Lock();

      // Timer-free abortable work: if a signal is present, wait forever unless that
      // signal aborts; if no signal is present, complete on the next microtask.
      const guarded = withLock(
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
      const first = guarded('one', { signal: controller1.signal }); // holds, doing work
      const second = guarded('two');                                // queues behind it

      controller1.abort();               // the holder's in-flight work fails
      await expectAbortError(first);     // holder aborted → released the lock
      expect(await second).toBe('two');  // the queued call then ran to completion
      expect(lock.isLocked()).toBe(false);
    });
  });

});