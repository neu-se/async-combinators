import { withMaxConcurrency } from '../src/withMaxConcurrency';
import { deferred, pauseMicrotask, abortedSignal, expectAbortError } from './helpers';

describe('withMaxConcurrency', () => {
  it('should execute function and return result', async () => {
    const fn = jest.fn().mockResolvedValue('test-result');
    const limitedFn = withMaxConcurrency(fn, 3);

    const result = await limitedFn('arg1', 'arg2');

    expect(result).toBe('test-result');
    expect(fn).toHaveBeenCalledWith('arg1', 'arg2');
  });

  describe('concurrency limiting', () => {
    it('should limit concurrent executions to maxConcurrent', async () => {
      let currentConcurrent = 0;
      let maxConcurrent = 0;

      const fn = async () => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        // Yield one microtask turn so queued callers can interleave while slots are occupied.
        await pauseMicrotask();
        currentConcurrent--;
      };

      const limitedFn = withMaxConcurrency(fn, 3);

      // 6 calls, limit 3 → never more than 3 running at once.
      await Promise.all([
        limitedFn(),
        limitedFn(),
        limitedFn(),
        limitedFn(),
        limitedFn(),
        limitedFn()
      ]);

      expect(maxConcurrent).toBe(3);
    });

    it('should allow single concurrent execution when maxConcurrent is 1', async () => {
      let currentConcurrent = 0;
      let maxConcurrent = 0;

      const fn = async () => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        // Yield one microtask turn so queued callers can interleave while the slot is occupied.
        await pauseMicrotask();
        currentConcurrent--;
      };

      const limitedFn = withMaxConcurrency(fn, 1);

      // limit 1 → strictly one at a time.
      await Promise.all([
        limitedFn(),
        limitedFn(),
        limitedFn(),
        limitedFn()
      ]);

      expect(maxConcurrent).toBe(1);
    });

    it('should not limit concurrency when maxConcurrent exceeds the number of calls', async () => {
      let currentConcurrent = 0;
      let maxConcurrent = 0;

      const fn = async () => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        // Yield one microtask turn so available slots can be claimed concurrently.
        await pauseMicrotask();
        currentConcurrent--;
      };

      const limitedFn = withMaxConcurrency(fn, 6);

      // 3 calls, limit 6 → the limit never binds, so all 3 run at once.
      await Promise.all([
        limitedFn(),
        limitedFn(),
        limitedFn()
      ]);

      expect(maxConcurrent).toBe(3);
    });
  });

  it('should execute queued calls in FIFO order', async () => {
    const executionOrder: number[] = [];

    const fn = async (id: number) => {
      executionOrder.push(id);
      // Yield one microtask turn so queued calls can interleave at slot boundaries.
      await pauseMicrotask();
      return id;
    };

    const limitedFn = withMaxConcurrency(fn, 2);

    const results = await Promise.all([
      limitedFn(1),
      limitedFn(2),
      limitedFn(3),
      limitedFn(4),
      limitedFn(5)
    ]);

    expect(results).toEqual([1, 2, 3, 4, 5]);
    expect(executionOrder).toEqual([1, 2, 3, 4, 5]);
  });

  it('should enforce the outer limit when the inner limit is larger', async () => {
    let currentConcurrent = 0;
    let maxConcurrent = 0;

    const fn = async () => {
      currentConcurrent++;
      maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
      // Yield one microtask turn so stacked limiters can interleave at slot boundaries.
      await pauseMicrotask();
      currentConcurrent--;
    };

    // inner limit 4, outer limit 2 → the smaller outer limit binds.
    const limitedFn = withMaxConcurrency(withMaxConcurrency(fn, 4), 2);
    await Promise.all([
      limitedFn(),
      limitedFn(),
      limitedFn(),
      limitedFn()
    ]);

    expect(maxConcurrent).toBe(2);
  });

  it('should enforce the inner limit when the outer limit is larger', async () => {
    let currentConcurrent = 0;
    let maxConcurrent = 0;

    const fn = async () => {
      currentConcurrent++;
      maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
      // Yield one microtask turn so stacked limiters can interleave at slot boundaries.
      await pauseMicrotask();
      currentConcurrent--;
    };

    // inner limit 2, outer limit 4 → the smaller inner limit binds.
    const limitedFn = withMaxConcurrency(withMaxConcurrency(fn, 2), 4);
    await Promise.all([
      limitedFn(),
      limitedFn(),
      limitedFn(),
      limitedFn()
    ]);

    expect(maxConcurrent).toBe(2);
  });

  describe('error handling', () => {
    it('should propagate errors from the wrapped function', async () => {
      const error = new Error('test error');
      const fn = jest.fn().mockRejectedValue(error);
      const limitedFn = withMaxConcurrency(fn, 2);

      await expect(limitedFn()).rejects.toThrow('test error');
    });

    it('should continue processing queue after error', async () => {
      const error = new Error('test error');
      const fn = jest.fn()
        .mockResolvedValueOnce('result1')
        .mockRejectedValueOnce(error)
        .mockResolvedValueOnce('result3')
        .mockResolvedValueOnce('result4');

      const limitedFn = withMaxConcurrency(fn, 2);

      const results = await Promise.allSettled([
        limitedFn(),
        limitedFn(),
        limitedFn(),
        limitedFn()
      ]);

      expect(results[0].status).toBe('fulfilled');
      expect(results[1].status).toBe('rejected');
      expect(results[2].status).toBe('fulfilled');
      expect(results[3].status).toBe('fulfilled');

      if (results[0].status === 'fulfilled') expect(results[0].value).toBe('result1');
      if (results[2].status === 'fulfilled') expect(results[2].value).toBe('result3');
      if (results[3].status === 'fulfilled') expect(results[3].value).toBe('result4');

      expect(fn).toHaveBeenCalledTimes(4);
    });

    it('should handle multiple errors without blocking queue', async () => {
      const fn = jest.fn()
        .mockRejectedValueOnce(new Error('error1'))
        .mockRejectedValueOnce(new Error('error2'))
        .mockResolvedValueOnce('success');

      const limitedFn = withMaxConcurrency(fn, 1);

      const results = await Promise.allSettled([
        limitedFn(),
        limitedFn(),
        limitedFn()
      ]);

      expect(results[0].status).toBe('rejected');
      expect(results[1].status).toBe('rejected');
      expect(results[2].status).toBe('fulfilled');
      expect(fn).toHaveBeenCalledTimes(3);
    });

    it('should preserve error details', async () => {
      const originalError = new Error('Original error');
      originalError.stack = 'Original stack';
      const fn = jest.fn().mockRejectedValue(originalError);
      const limitedFn = withMaxConcurrency(fn, 2);

      try {
        await limitedFn();
        fail('Should have thrown error');
      } catch (error) {
        expect(error).toBe(originalError);
        expect((error as Error).message).toBe('Original error');
        expect((error as Error).stack).toBe('Original stack');
      }
    });
  });

  describe('invalid maxConcurrent', () => {
    it('should throw for zero', () => {
      expect(() => withMaxConcurrency(jest.fn(), 0)).toThrow(
        'maxConcurrent must be a positive integer'
      );
    });

    it('should throw for negative values', () => {
      expect(() => withMaxConcurrency(jest.fn(), -1)).toThrow(
        'maxConcurrent must be a positive integer'
      );
    });

    it('should throw for non-integer values', () => {
      expect(() => withMaxConcurrency(jest.fn(), 1.5)).toThrow(
        'maxConcurrent must be a positive integer'
      );
    });

    it('should throw at wrap time, before any call is made', () => {
      const fn = jest.fn();
      expect(() => withMaxConcurrency(fn, 0)).toThrow();
      expect(fn).not.toHaveBeenCalled();
    });
  });

  it('should dispatch a call promptly when a slot is free', async () => {
    const fn = jest.fn().mockResolvedValue('result');
    const limitedFn = withMaxConcurrency(fn, 5);

    const promise = limitedFn();
    // With a free slot the call is dispatched promptly — on the next microtask, once the
    // permit is acquired — rather than queued behind other calls.
    await Promise.resolve();
    expect(fn).toHaveBeenCalledTimes(1);
    await expect(promise).resolves.toBe('result');
  });

  describe('cancellation', () => {
    it('drops a queued call when its signal aborts, without running it', async () => {
      const gate = deferred();
      // fn declares a trailing { signal } option — the opt-in for cancellation.
      const fn = jest.fn(async (_opts?: { signal?: AbortSignal }) => {
        // Keep the running call active until the test explicitly releases this gate.
        await gate.promise;
        return 'done';
      });
      const limited = withMaxConcurrency(fn, 1); // one at a time
      const controller = new AbortController();

      const first = limited();                                // takes the only slot
      const queued = limited({ signal: controller.signal });  // must wait behind `first`
      await Promise.resolve();                                // fn dispatches on the next microtask
      expect(fn).toHaveBeenCalledTimes(1);                    // only `first` has started

      controller.abort(); // abort while `queued` is still waiting in the queue
      await expectAbortError(queued);
      // The queued call was removed before running — fn is never invoked for it.
      expect(fn).toHaveBeenCalledTimes(1);

      gate.resolve();
      await first; // let the slot holder finish
    });

    it('rejects immediately if the signal is already aborted, without calling fn', async () => {
      const fn = jest.fn(async (_opts?: { signal?: AbortSignal }) => 'done');
      const limited = withMaxConcurrency(fn, 3);

      const promise = limited({ signal: abortedSignal() }); // already aborted before the call
      await expectAbortError(promise);
      // The entry poll rejects before the call is even enqueued.
      expect(fn).not.toHaveBeenCalled();
    });

    it('a shared signal cancels all still-queued calls but not the running one', async () => {
      const gate = deferred();
      const fn = jest.fn(async (_id: number, _opts?: { signal?: AbortSignal }) => {
        // Keep the running call active until the test explicitly releases this gate.
        await gate.promise;
        return 'done';
      });
      const limited = withMaxConcurrency(fn, 1); // one at a time
      const controller = new AbortController();
      const opts = { signal: controller.signal };

      // Five calls sharing ONE signal: the first starts running, the other four queue.
      const calls = [0, 1, 2, 3, 4].map((id) => limited(id, opts));
      await Promise.resolve(); // fn dispatches on the next microtask once the permit is acquired
      expect(fn).toHaveBeenCalledTimes(1); // only the first has started

      controller.abort(); // one abort fans out to all five listeners

      // allSettled attaches a handler to every call up front (before any await),
      // so the four rejections don't sit briefly unhandled while we wait on the
      // running one.
      gate.resolve();
      const results = await Promise.allSettled(calls);

      // The running call finished; the four still-queued calls were all dropped.
      expect(results[0]).toEqual({ status: 'fulfilled', value: 'done' });
      for (const r of results.slice(1)) {
        expect(r.status).toBe('rejected');
        expect((r as PromiseRejectedResult).reason).toHaveProperty('name', 'AbortError');
      }
      expect(fn).toHaveBeenCalledTimes(1); // the queued four never ran
    });
  });
});
