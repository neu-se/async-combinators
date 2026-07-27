import { ReentrantLock } from '../src/reentrant-lock';
import { withTimeout } from '../src/withTimeout';
import { deferred, abortedSignal, expectAbortError, pauseMicrotask } from './helpers';

describe('ReentrantAsyncLock', () => {
  it('should run a critical section and return its result', async () => {
    const lock = new ReentrantLock();
    const result = await lock.runExclusive(async () => 'value');
    expect(result).toBe('value');
  });

  it('should allow reentrant calls within the same async chain', async () => {
    const lock = new ReentrantLock();

    const result = await lock.runExclusive(async () =>
      lock.runExclusive(async () =>
        lock.runExclusive(async () => 'deep') // must not block
      )
    );

    expect(result).toBe('deep');
  });

  it('should enforce mutual exclusion between independent operations', async () => {
    const lock = new ReentrantLock();
    let counter = 0;
    let maxConcurrent = 0;
    let currentConcurrent = 0;

    const criticalSection = () =>
      lock.runExclusive(async () => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);

        const initialCounter = counter;
        // Yield once so concurrent callers can interleave while this lock holder is active.
        await pauseMicrotask();
        counter = initialCounter + 1;

        currentConcurrent--;
      });

    await Promise.all([
      criticalSection(),
      criticalSection(),
      criticalSection(),
      criticalSection()
    ]);

    expect(counter).toBe(4); // no lost updates
    expect(maxConcurrent).toBe(1); // only one at a time
  });

  it('should serve waiters in FIFO order', async () => {
    const lock = new ReentrantLock();
    const order: number[] = [];

    // Hold the lock so the following operations must queue.
    let release!: () => void;
    const held = lock.runExclusive(
      () => new Promise<void>(resolve => { release = resolve; })
    );

    const waiters = [1, 2, 3].map(id =>
      lock.runExclusive(async () => { order.push(id); })
    );

    // Let the queue build up, then release the holder.
    await pauseMicrotask();
    release();

    await Promise.all([held, ...waiters]);

    expect(order).toEqual([1, 2, 3]);
  });

  it('should support recursion through runExclusive', async () => {
    const lock = new ReentrantLock();
    const events: string[] = [];

    const recurse = async (depth: number): Promise<void> => {
      await lock.runExclusive(async () => {
        events.push(`enter-${depth}`);
        if (depth > 0) await recurse(depth - 1);
        events.push(`exit-${depth}`);
      });
    };

    await recurse(2);

    expect(events).toEqual([
      'enter-2',
      'enter-1',
      'enter-0',
      'exit-0',
      'exit-1',
      'exit-2'
    ]);
  });

  it('should release the lock when the critical section throws', async () => {
    const lock = new ReentrantLock();

    await expect(
      lock.runExclusive(async () => { throw new Error('boom'); })
    ).rejects.toThrow('boom');

    expect(lock.isLocked()).toBe(false); // released despite the throw
  });

  it('can still deadlock: two chains acquiring two locks in opposite order', async () => {
    const lock1 = new ReentrantLock();
    const lock2 = new ReentrantLock();

    // Attempt to acquire `lock` (empty critical section); blocks until it's free.
    const grab = (lock: ReentrantLock) => lock.runExclusive(() => Promise.resolve());

    // Each chain must hold its first lock before reaching for the second
    // (otherwise one could grab both before the other starts).
    const aHoldsLock1 = deferred();
    const bHoldsLock2 = deferred();

    // Chain A: take lock1, then reach for lock2 (which B is holding).
    const a = lock1.runExclusive(async () => {
      aHoldsLock1.resolve();
      await bHoldsLock2.promise;
      await grab(lock2); // blocks — B holds lock2
    });

    // Chain B: take lock2, then reach for lock1 (which A is holding).
    const b = lock2.runExclusive(async () => {
      bHoldsLock2.resolve();
      await aHoldsLock1.promise;
      await grab(lock1); // blocks — A holds lock1
    });

    // Classic lock-ordering deadlock: reentrancy is per-lock/per-chain, so it
    // does nothing to prevent A and B each waiting on a lock the other holds.
    const guarded = withTimeout(() => Promise.all([a, b]), 100);
    await expect(guarded()).rejects.toThrow('Operation timed out');
  });

  it("acquiring a second lock inside the first's critical section does not corrupt the outer lock's ALS context", async () => {
    const lockA = new ReentrantLock();
    const lockB = new ReentrantLock();
    const events: string[] = [];

    await lockA.runExclusive(async () => {
      events.push('lockA: enter outer');

      // lockB.runAsHolder calls lockB.holder.run(tokenB, ...), which creates a
      // derived async context. Verify that when that context exits the outer
      // continuation still carries lockA's owner token, so lockA is still
      // reentrant on this chain rather than appearing unowned.
      await lockB.runExclusive(async () => {
        events.push('lockB: enter');
      });

      events.push('lockA: after lockB released');

      // If lockA's ALS context were erased by lockB's holder.run(), this nested
      // call would not see this chain as the owner and would block forever.
      await lockA.runExclusive(async () => {
        events.push('lockA: reentrant after lockB');
      });

      events.push('lockA: exit outer');
    });

    expect(events).toEqual([
      'lockA: enter outer',
      'lockB: enter',
      'lockA: after lockB released',
      'lockA: reentrant after lockB',
      'lockA: exit outer',
    ]);
    expect(lockA.isLocked()).toBe(false);
    expect(lockB.isLocked()).toBe(false);
  });

  it('two concurrent chains each nesting lockB inside lockA: lock handoff preserves ALS for each chain', async () => {
    const lockA = new ReentrantLock();
    const lockB = new ReentrantLock();
    const events: string[] = [];
    const gate = deferred();

    // Chain 1 acquires lockA first and holds it until gate, so chain 2 queues.
    const chain1 = lockA.runExclusive(async () => {
      events.push('chain1: enter lockA');
      await gate.promise;
      await lockB.runExclusive(async () => {
        events.push('chain1: enter lockB');
      });
      events.push('chain1: after lockB');
      await lockA.runExclusive(async () => { events.push('chain1: reentrant lockA'); });
    });

    const chain2 = lockA.runExclusive(async () => {
      events.push('chain2: enter lockA');
      await lockB.runExclusive(async () => {
        events.push('chain2: enter lockB');
      });
      events.push('chain2: after lockB');
      await lockA.runExclusive(async () => { events.push('chain2: reentrant lockA'); });
    });

    await pauseMicrotask();
    // chain1 holds lockA; chain2 is queued and has not entered yet.
    expect(events).toEqual(['chain1: enter lockA']);

    gate.resolve();
    await Promise.all([chain1, chain2]);

    // After chain1 hands lockA to chain2, chain2 must also be reentrant on lockA.
    expect(events).toEqual([
      'chain1: enter lockA',
      'chain1: enter lockB',
      'chain1: after lockB',
      'chain1: reentrant lockA',
      'chain2: enter lockA',
      'chain2: enter lockB',
      'chain2: after lockB',
      'chain2: reentrant lockA',
    ]);
    expect(lockA.isLocked()).toBe(false);
    expect(lockB.isLocked()).toBe(false);
  });

  it('three-level nesting: each inner lock exit restores the enclosing lock ALS context', async () => {
    const lockA = new ReentrantLock();
    const lockB = new ReentrantLock();
    const lockC = new ReentrantLock();
    const events: string[] = [];

    await lockA.runExclusive(async () => {
      events.push('enter lockA');
      await lockB.runExclusive(async () => {
        events.push('enter lockB');
        await lockC.runExclusive(async () => {
          events.push('enter lockC');
        });
        // exiting C3 must restore C2: lockB still reentrant here
        events.push('after lockC');
        await lockB.runExclusive(async () => { events.push('reentrant lockB'); });
      });
      // exiting C2 must restore C1: lockA still reentrant here
      events.push('after lockB');
      await lockA.runExclusive(async () => { events.push('reentrant lockA'); });
    });

    expect(events).toEqual([
      'enter lockA',
      'enter lockB',
      'enter lockC',
      'after lockC',
      'reentrant lockB',
      'after lockB',
      'reentrant lockA',
    ]);
    expect(lockA.isLocked()).toBe(false);
    expect(lockB.isLocked()).toBe(false);
    expect(lockC.isLocked()).toBe(false);
  });

  it("genuine async wait on inner lock held by another chain does not lose the outer lock's ALS context", async () => {
    const lockA = new ReentrantLock();
    const lockB = new ReentrantLock();
    const events: string[] = [];
    const gate = deferred();

    // Chain 2 holds lockB independently until gate resolves.
    const chain2 = lockB.runExclusive(async () => {
      events.push('chain2: hold lockB');
      await gate.promise;
      events.push('chain2: release lockB');
    });

    // Chain 1 holds lockA, then must genuinely suspend in lockB's waiter queue.
    const chain1 = lockA.runExclusive(async () => {
      events.push('chain1: enter lockA');
      // lockB is held by chain2 — chain1 suspends here, not just in a child ALS
      // context but in lockB's waiter queue, before resuming with a fresh token.
      await lockB.runExclusive(async () => {
        events.push('chain1: enter lockB');
      });
      events.push('chain1: after lockB');
      // The reentrant call confirms lockA's ALS context survived the wait.
      await lockA.runExclusive(async () => { events.push('chain1: reentrant lockA'); });
    });

    await pauseMicrotask();
    // chain2 holds lockB; chain1 holds lockA and is queued on lockB.
    expect(events).toEqual(['chain2: hold lockB', 'chain1: enter lockA']);

    gate.resolve();
    await Promise.all([chain1, chain2]);

    expect(events).toEqual([
      'chain2: hold lockB',
      'chain1: enter lockA',
      'chain2: release lockB',
      'chain1: enter lockB',
      'chain1: after lockB',
      'chain1: reentrant lockA',
    ]);
    expect(lockA.isLocked()).toBe(false);
    expect(lockB.isLocked()).toBe(false);
  });

  describe('escaped reentrant lifetime', () => {
    it('keeps the lock held when reentrant promise work outlives the outer frame', async () => {
      const lock = new ReentrantLock();
      const gate = deferred();
      let escaped!: Promise<void>;

      await lock.runExclusive(async () => {
        // Start reentrant work but do not await it; it escapes this outer frame.
        escaped = lock.runExclusive(async () => {
          await gate.promise;
        });
      });

      // The escaped nested work still owns the lock.
      expect(lock.isLocked()).toBe(true);

      let contenderEntered = false;
      const contender = lock.runExclusive(async () => {
        contenderEntered = true;
      });

      await pauseMicrotask();
      expect(contenderEntered).toBe(false);

      gate.resolve();
      await escaped; // Escaped reentrant work finishes and releases its counted hold.
      await contender; // Only after that release can the queued independent contender run.
      expect(contenderEntered).toBe(true);
      expect(lock.isLocked()).toBe(false);
    });

    it('keeps the lock held when a reentrant stream outlives the outer frame', async () => {
      const lock = new ReentrantLock();
      const gate = deferred();
      let it!: AsyncIterator<number>;

      await lock.runExclusive(async () => {
        const escaped = lock.iterateExclusive(async function* (): AsyncIterable<number> {
          yield 1;
          await gate.promise;
          yield 2;
        });

        it = escaped[Symbol.asyncIterator]();
        expect(await it.next()).toEqual({ value: 1, done: false });
      });

      // The escaped stream is still live and should keep ownership.
      expect(lock.isLocked()).toBe(true);

      let contenderEntered = false;
      const contender = lock.runExclusive(async () => {
        contenderEntered = true;
      });

      await pauseMicrotask();
      expect(contenderEntered).toBe(false);

      gate.resolve();
      expect(await it.next()).toEqual({ value: 2, done: false });
      expect(await it.next()).toEqual({ value: undefined, done: true });
      await contender;
      expect(contenderEntered).toBe(true);
      expect(lock.isLocked()).toBe(false);
    });

    it('releases only after escaped reentrant promise work rejects', async () => {
      const lock = new ReentrantLock();
      const gate = deferred();
      const err = new Error('escaped boom');
      let escaped!: Promise<void>;

      await lock.runExclusive(async () => {
        escaped = lock.runExclusive(async () => {
          await gate.promise;
          throw err;
        });
      });

      expect(lock.isLocked()).toBe(true);

      let contenderEntered = false;
      const contender = lock.runExclusive(async () => {
        contenderEntered = true;
      });

      await pauseMicrotask();
      expect(contenderEntered).toBe(false);

      const escapedRejects = expect(escaped).rejects.toBe(err);
      gate.resolve();
      await escapedRejects;
      await contender;
      expect(contenderEntered).toBe(true);
      expect(lock.isLocked()).toBe(false);
    });

    it('keeps ownership until all escaped reentrant promise work settles', async () => {
      const lock = new ReentrantLock();
      const gateA = deferred();
      const gateB = deferred();
      let escapedA!: Promise<void>;
      let escapedB!: Promise<void>;

      await lock.runExclusive(async () => {
        escapedA = lock.runExclusive(async () => { await gateA.promise; });
        escapedB = lock.runExclusive(async () => { await gateB.promise; });
      });

      expect(lock.isLocked()).toBe(true);

      let contenderEntered = false;
      const contender = lock.runExclusive(async () => {
        contenderEntered = true;
      });

      await pauseMicrotask();
      expect(contenderEntered).toBe(false);

      gateA.resolve();
      await escapedA;
      await pauseMicrotask();
      expect(lock.isLocked()).toBe(true);
      expect(contenderEntered).toBe(false);

      gateB.resolve();
      await escapedB;
      await contender;
      expect(lock.isLocked()).toBe(false);
      expect(contenderEntered).toBe(true);

    });

    it('dequeues an aborted contender that waits behind escaped work', async () => {
      const lock = new ReentrantLock();
      const gate = deferred();
      let escaped!: Promise<void>;

      await lock.runExclusive(async () => {
        escaped = lock.runExclusive(async () => {
          await gate.promise;
        });
      });

      const fn = jest.fn(async () => 'done');
      const controller = new AbortController();
      const waiter = lock.runExclusive(fn, controller.signal);
      controller.abort();

      await expectAbortError(waiter);
      expect(fn).not.toHaveBeenCalled();
      expect(lock.isLocked()).toBe(true);

      gate.resolve();
      await escaped;
      expect(lock.isLocked()).toBe(false);
    });

    it('rejects an already-aborted contender behind escaped work without affecting the holder', async () => {
      const lock = new ReentrantLock();
      const gate = deferred();
      let escaped!: Promise<void>;

      await lock.runExclusive(async () => {
        escaped = lock.runExclusive(async () => {
          await gate.promise;
        });
      });

      const fn = jest.fn(async () => 'done');
      await expectAbortError(lock.runExclusive(fn, abortedSignal()));
      expect(fn).not.toHaveBeenCalled();
      expect(lock.isLocked()).toBe(true);

      gate.resolve();
      await escaped;
      expect(lock.isLocked()).toBe(false);
    });

    it('releases when an escaped reentrant stream is abandoned early', async () => {
      const lock = new ReentrantLock();
      let cleanedUp = false;
      let it!: AsyncIterator<number>;

      await lock.runExclusive(async () => {
        const escaped = lock.iterateExclusive(async function* (): AsyncIterable<number> {
          try {
            yield 1;
            yield 2;
          } finally {
            cleanedUp = true;
          }
        });

        it = escaped[Symbol.asyncIterator]();
        expect(await it.next()).toEqual({ value: 1, done: false });
      });

      expect(lock.isLocked()).toBe(true);

      let contenderEntered = false;
      const contender = lock.runExclusive(async () => {
        contenderEntered = true;
      });

      await pauseMicrotask();
      expect(contenderEntered).toBe(false);

      // Close the escaped iterator and wait for its async cleanup/release path to complete.
      await Promise.resolve(it.return?.());
      expect(cleanedUp).toBe(true);
      await contender;
      expect(lock.isLocked()).toBe(false);
    });

    it('releases when an escaped reentrant stream errors after the outer frame exits', async () => {
      const lock = new ReentrantLock();
      const gate = deferred();
      const err = new Error('escaped stream boom');
      let it!: AsyncIterator<number>;

      await lock.runExclusive(async () => {
        const escaped = lock.iterateExclusive(async function* (): AsyncIterable<number> {
          yield 1;
          await gate.promise;
          throw err;
        });

        it = escaped[Symbol.asyncIterator]();
        expect(await it.next()).toEqual({ value: 1, done: false });
      });

      expect(lock.isLocked()).toBe(true);

      let contenderEntered = false;
      const contender = lock.runExclusive(async () => {
        contenderEntered = true;
      });

      await pauseMicrotask();
      expect(contenderEntered).toBe(false);

      gate.resolve();
      await expect(it.next()).rejects.toBe(err);
      await contender;
      expect(contenderEntered).toBe(true);
      expect(lock.isLocked()).toBe(false);
    });

    it('does not add an escaped hold for a reentrant stream that is never iterated', async () => {
      const lock = new ReentrantLock();
      let sourceStarted = false;

      await lock.runExclusive(async () => {
        lock.iterateExclusive(async function* (): AsyncIterable<number> {
          sourceStarted = true;
          yield 1;
        });
      });

      expect(sourceStarted).toBe(false);
      expect(lock.isLocked()).toBe(false);
    });
  });

  describe('cancellation', () => {
    it('a waiter aborted while queued is dequeued and rejects; the reentrant holder keeps the lock', async () => {
      const lock = new ReentrantLock();
      const baseCaseGate = deferred();

      // A recursive worker that re-acquires the lock it already holds — the whole
      // point of a reentrant lock (a non-reentrant one would deadlock on the nested
      // acquire). The base case does a little async work, holding the lock meanwhile.
      const recurse = (depth: number): Promise<number> =>
        lock.runExclusive(async () => {
          if (depth === 0) { await baseCaseGate.promise; return 0; }
          return 1 + await recurse(depth - 1); // reentrant re-acquire
        });

      // The reentrant recursion runs synchronously down to the base case's await,
      // so the lock is already held the instant this returns.
      const holder = recurse(3);
      expect(lock.isLocked()).toBe(true);

      // An independent call queues behind the holder, then is cancelled while waiting.
      const fn = jest.fn(async () => 'done');
      const controller = new AbortController();
      const waiter = lock.runExclusive(fn, controller.signal);
      controller.abort();

      await expectAbortError(waiter);
      expect(fn).not.toHaveBeenCalled(); // dequeued before running
      expect(lock.isLocked()).toBe(true); // the reentrant holder still owns it

      baseCaseGate.resolve();
      expect(await holder).toBe(3); // the recursion completed normally
      expect(lock.isLocked()).toBe(false); // no queued waiter acquired the lock
    });

    it('rejects immediately if the signal is already aborted, without running fn', async () => {
      const lock = new ReentrantLock();
      const fn = jest.fn(async () => 'done');

      await expectAbortError(lock.runExclusive(fn, abortedSignal()));
      expect(fn).not.toHaveBeenCalled();
      expect(lock.isLocked()).toBe(false); // never acquired
    });

    it('an already-aborted signal rejects even a reentrant call, without running fn', async () => {
      const lock = new ReentrantLock();
      const inner = jest.fn(async () => 'inner');

      await lock.runExclusive(async () => {
        // This chain already holds the lock, so the nested call would take the
        // reentrant fast-path — but the already-aborted check runs first and rejects.
        await expectAbortError(lock.runExclusive(inner, abortedSignal()));
        expect(inner).not.toHaveBeenCalled();
      });

      expect(lock.isLocked()).toBe(false); // outer released normally
    });
  });
});
