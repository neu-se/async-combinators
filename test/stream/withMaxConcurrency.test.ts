import { withMaxConcurrency } from '../../src/stream/withMaxConcurrency';
import { collect, arrayStream, failingStream, streamFn } from './helpers';
import { deferred, abortedSignal, expectAbortError, pauseMicrotask } from '../helpers';

describe('stream/withMaxConcurrency', () => {
  it('yields the stream when a slot is free', async () => {
    const sourceFn = streamFn().mockImplementation(() => arrayStream(['a', 'b']));
    const wrapped = withMaxConcurrency(sourceFn, 2);

    expect(await collect(wrapped('arg'))).toEqual(['a', 'b']);
    expect(sourceFn).toHaveBeenCalledWith('arg');
  });

  describe('concurrency', () => {
    it('bounds the number of concurrently active streams', async () => {
      let currentConcurrent = 0;
      let maxConcurrent = 0;
      const gate = deferred();
      const twoActive = deferred();

      // Each source counts itself active from its first pull until it completes, and
      // stays active (holding its slot) until the shared gate is released.
      const source = async function* (): AsyncIterable<string> {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        if (currentConcurrent === 2) twoActive.resolve();
        try {
          yield 'x';
          await gate.promise; // hold the slot open until released
        } finally {
          currentConcurrent--;
        }
      };
      const wrapped = withMaxConcurrency(source, 2);

      // Start 5 streams; at most 2 may be actively producing at once.
      const ps = [collect(wrapped()), collect(wrapped()), collect(wrapped()), collect(wrapped()), collect(wrapped())];
      await twoActive.promise; // first two slots are active and holding
      expect(maxConcurrent).toBe(2);

      gate.resolve(); // release all → they complete, slots free, and the rest run
      await Promise.all(ps);
      expect(maxConcurrent).toBe(2); // never exceeded 2 across the whole run
    });
  });

  describe('slot release (cleanup)', () => {
    // Each case runs a first stream that ends in a different way, then a second stream
    // under a limit of 1: if the first didn't release its slot, the second would hang.

    it('releases the slot when a stream completes normally', async () => {
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['x']));
      const wrapped = withMaxConcurrency(sourceFn, 1);

      expect(await collect(wrapped())).toEqual(['x']); // completes → slot freed
      expect(await collect(wrapped())).toEqual(['x']); // second acquires the freed slot
      expect(sourceFn).toHaveBeenCalledTimes(2);
    });

    it('releases the slot when a stream errors', async () => {
      const err = new Error('boom');
      // 1st call (stream) errors; every later call succeeds.
      const sourceFn = streamFn()
        .mockImplementationOnce(() => failingStream(err))
        .mockImplementation(() => arrayStream(['x']));
      const wrapped = withMaxConcurrency(sourceFn, 1);

      await expect(collect(wrapped())).rejects.toBe(err); // errors → slot freed
      expect(await collect(wrapped())).toEqual(['x']); // second acquires the freed slot
    });

    it('releases the slot when the consumer abandons a stream', async () => {
      const sourceFn = streamFn()
        .mockImplementationOnce(() => arrayStream(['a', 'b', 'c']))
        .mockImplementation(() => arrayStream(['x']));
      const wrapped = withMaxConcurrency(sourceFn, 1);

      for await (const v of wrapped()) {
        if (v === 'a') break; // abandon → the wrapper's finally releases the slot
      }
      expect(await collect(wrapped())).toEqual(['x']); // second acquires the freed slot
      expect(sourceFn).toHaveBeenCalledTimes(2);
    });
  });

  describe('reserve at first pull', () => {
    it('a stream created but never iterated acquires no slot', async () => {
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['x']));
      const wrapped = withMaxConcurrency(sourceFn, 1);

      wrapped(); // created, never iterated → no slot acquired
      wrapped(); // ditto

      // The only slot is still free, so an iterated stream starts immediately.
      expect(await collect(wrapped())).toEqual(['x']);
      expect(sourceFn).toHaveBeenCalledTimes(1); // only the iterated stream started its source
    });
  });

  describe('cancellation', () => {
    it('drops a queued stream when its signal aborts, without starting the source', async () => {
      const gate = deferred();
      const holderStarted = deferred();
      const sourceFn = streamFn().mockImplementation(async function* (): AsyncIterable<string> {
        holderStarted.resolve();
        yield 'x';
        await gate.promise;
      });
      const wrapped = withMaxConcurrency(sourceFn, 1);

      // Set up a stream that blocks, waiting on the pending gate.promise: it takes the
      // only slot and holds it until we release the gate at the end.
      const holder = collect(wrapped());
      await holderStarted.promise;
      expect(sourceFn).toHaveBeenCalledTimes(1); // it acquired the slot and its source started

      // Start a second stream that cannot acquire a slot, then abort it while it waits;
      // its source must never start.
      const controller = new AbortController();
      const queued = collect(wrapped({ signal: controller.signal }));
      await pauseMicrotask();
      expect(sourceFn).toHaveBeenCalledTimes(1); // still waiting for the slot — source not started
      controller.abort();
      await expectAbortError(queued);
      expect(sourceFn).toHaveBeenCalledTimes(1); // aborted while waiting → source never started

      gate.resolve(); // release the blocking stream so it can finish
      expect(await holder).toEqual(['x']); // the holder ran to completion, yielding its one item
    });

    it('rejects at the first pull if the signal is already aborted, without acquiring a slot', async () => {
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['x']));
      const wrapped = withMaxConcurrency(sourceFn, 1);

      await expectAbortError(collect(wrapped({ signal: abortedSignal() })));
      expect(sourceFn).not.toHaveBeenCalled();

      // No slot was consumed, so a subsequent stream runs.
      expect(await collect(wrapped())).toEqual(['x']);
    });
  });

  describe('invalid maxConcurrent', () => {
    it('throws for zero', () => {
      expect(() => withMaxConcurrency(streamFn(), 0)).toThrow('maxConcurrent must be a positive integer');
    });

    it('throws for negative values', () => {
      expect(() => withMaxConcurrency(streamFn(), -1)).toThrow('maxConcurrent must be a positive integer');
    });

    it('throws for non-integer values', () => {
      expect(() => withMaxConcurrency(streamFn(), 1.5)).toThrow('maxConcurrent must be a positive integer');
    });

    it('throws at wrap time, before any stream is started', () => {
      const sourceFn = streamFn();
      expect(() => withMaxConcurrency(sourceFn, 0)).toThrow();
      expect(sourceFn).not.toHaveBeenCalled();
    });
  });
});
