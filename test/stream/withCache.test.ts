import { withCache } from '../../src/stream/withCache';
import { collect, collectInto, arrayStream, failingStream, streamFn } from './helpers';
import { abortError, abortedSignal, deferred, expectAbortError } from '../helpers';

describe('stream/withCache', () => {
  describe('basic caching', () => {
    it('pulls from the source on the first call for a key', async () => {
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['a', 'b']));
      const wrapped = withCache(sourceFn);

      expect(await collect(wrapped('arg1', 'arg2'))).toEqual(['a', 'b']);
      expect(sourceFn).toHaveBeenCalledTimes(1);
      expect(sourceFn).toHaveBeenCalledWith('arg1', 'arg2');
    });

    it('replays the cached stream on a second call with the same arguments', async () => {
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['a', 'b']));
      const wrapped = withCache(sourceFn);

      expect(await collect(wrapped('k'))).toEqual(['a', 'b']);
      expect(await collect(wrapped('k'))).toEqual(['a', 'b']); // replayed, source not re-run
      expect(sourceFn).toHaveBeenCalledTimes(1);
    });

    it('pulls from the source again when the arguments differ', async () => {
      const sourceFn = streamFn()
        .mockImplementationOnce(() => arrayStream(['a']))
        .mockImplementationOnce(() => arrayStream(['b']));
      const wrapped = withCache(sourceFn);

      expect(await collect(wrapped('arg1'))).toEqual(['a']);
      expect(await collect(wrapped('arg2'))).toEqual(['b']);
      expect(sourceFn).toHaveBeenCalledTimes(2);
    });

    it('handles a function with no arguments', async () => {
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['x']));
      const wrapped = withCache(sourceFn);

      expect(await collect(wrapped())).toEqual(['x']);
      expect(await collect(wrapped())).toEqual(['x']);
      expect(sourceFn).toHaveBeenCalledTimes(1);
    });

    it('handles composite / structured arguments', async () => {
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['r']));
      const wrapped = withCache(sourceFn);

      const arg = { nested: { array: [1, 2, { deep: 'value' }] }, nullValue: null };
      expect(await collect(wrapped('s', 42, true, arg))).toEqual(['r']);
      expect(await collect(wrapped('s', 42, true, arg))).toEqual(['r']);
      expect(sourceFn).toHaveBeenCalledTimes(1);
    });

    it('distinguishes different argument orders', async () => {
      const sourceFn = streamFn()
        .mockImplementationOnce(() => arrayStream(['ab']))
        .mockImplementationOnce(() => arrayStream(['ba']));
      const wrapped = withCache(sourceFn);

      expect(await collect(wrapped('a', 'b'))).toEqual(['ab']);
      expect(await collect(wrapped('b', 'a'))).toEqual(['ba']);
      expect(sourceFn).toHaveBeenCalledTimes(2);
    });
  });

  describe('custom key', () => {
    it('uses a custom makeKey when provided', async () => {
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['r']));
      const makeKey = jest.fn((args: [{ id: string; name?: string }]) => `user-${args[0].id}`);
      const wrapped = withCache(sourceFn, { makeKey });

      // Both calls make the key "user-u1" (same id, name ignored) → one shared source.
      expect(await collect(wrapped({ id: 'u1', name: 'Alice' }))).toEqual(['r']);
      expect(await collect(wrapped({ id: 'u1', name: 'Bob' }))).toEqual(['r']);
      expect(sourceFn).toHaveBeenCalledTimes(1);
      expect(makeKey).toHaveBeenCalledTimes(2);
    });

    it('distinguishes different keys from a custom makeKey', async () => {
      const sourceFn = streamFn()
        .mockImplementationOnce(() => arrayStream(['r1']))
        .mockImplementationOnce(() => arrayStream(['r2']));
      const makeKey = (args: [{ id: string }]) => `user-${args[0].id}`;
      const wrapped = withCache(sourceFn, { makeKey });

      // Keys "user-u1" and "user-u2" differ → two separate sources.
      expect(await collect(wrapped({ id: 'u1' }))).toEqual(['r1']);
      expect(await collect(wrapped({ id: 'u2' }))).toEqual(['r2']);
      expect(sourceFn).toHaveBeenCalledTimes(2);
    });

    it('honors intentional key collisions', async () => {
      const sourceFn = streamFn()
        .mockImplementationOnce(() => arrayStream(['for-1']))
        .mockImplementationOnce(() => arrayStream(['for-2']));
      const makeKey = (args: [number]) => Math.floor(args[0]).toString(); // 1.1 and 1.2 → "1"
      const wrapped = withCache(sourceFn, { makeKey });

      expect(await collect(wrapped(1.1))).toEqual(['for-1']);
      expect(await collect(wrapped(1.2))).toEqual(['for-1']); // collision → cached
      expect(sourceFn).toHaveBeenCalledTimes(1);
    });
  });

  describe('cache isolation', () => {
    it('keeps separate caches for different wrapped functions', async () => {
      const fn1 = streamFn().mockImplementation(() => arrayStream(['one']));
      const fn2 = streamFn().mockImplementation(() => arrayStream(['two']));
      const a = withCache(fn1);
      const b = withCache(fn2);

      await collect(a('k'));
      await collect(b('k'));
      await collect(a('k'));
      await collect(b('k'));
      expect(fn1).toHaveBeenCalledTimes(1);
      expect(fn2).toHaveBeenCalledTimes(1);
    });

    it('gives each of two independent wrappers of the same function its own cache', async () => {
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['r']));
      const a = withCache(sourceFn);
      const b = withCache(sourceFn);

      await collect(a('x')); // source call 1
      await collect(b('x')); // separate cache → source call 2
      expect(sourceFn).toHaveBeenCalledTimes(2);

      await collect(a('x')); // each serves its own cache
      await collect(b('x'));
      expect(sourceFn).toHaveBeenCalledTimes(2);
    });

    it('supports nesting withCache(withCache(fn)) (a cache of a cache)', async () => {
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['r']));
      const inner = withCache(sourceFn);
      const outer = withCache(inner); // outer wraps the already-cached inner

      // First call goes through both layers to the source.
      expect(await collect(outer('arg'))).toEqual(['r']);
      expect(sourceFn).toHaveBeenCalledTimes(1);

      // That populated the inner cache too: reading inner directly is a hit, not a new source call.
      expect(await collect(inner('arg'))).toEqual(['r']);
      expect(sourceFn).toHaveBeenCalledTimes(1);

      // Repeat is served by the outer cache; it never reaches inner or the source.
      expect(await collect(outer('arg'))).toEqual(['r']);
      expect(sourceFn).toHaveBeenCalledTimes(1);

      // A different key misses both layers → back to the source.
      expect(await collect(outer('other'))).toEqual(['r']);
      expect(sourceFn).toHaveBeenCalledTimes(2);
    });
  });

  describe('laziness and on-demand pulling', () => {
    it('pulls nothing from a stream that is created but never iterated', () => {
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['a']));
      const wrapped = withCache(sourceFn);

      wrapped('k'); // created, never iterated
      expect(sourceFn).not.toHaveBeenCalled();
    });

    it('pulls only what is demanded, so an unbounded source is never over-run', async () => {
      let produced = 0;
      const wrapped = withCache(async function* (..._args: unknown[]): AsyncIterable<number> {
        for (let n = 0; ; n++) {
          produced++;
          yield n;
        }
      });

      const it = wrapped('k')[Symbol.asyncIterator]();
      const first3 = [(await it.next()).value, (await it.next()).value, (await it.next()).value];
      expect(first3).toEqual([0, 1, 2]);
      expect(produced).toBe(3); // only the demanded items were pulled
      await it.return?.(); // abandon our manual iterator so the wrapper detaches and closes the infinite source
    });
  });

  describe('concurrent consumers (shared store)', () => {
    it('feeds a second concurrent consumer from the store, pulling each item once', async () => {
      let produced = 0;
      const sourceFn = streamFn<number>().mockImplementation(async function* () {
        for (const n of [1, 2, 3]) {
          produced++;
          yield n;
        }
      });
      const wrapped = withCache(sourceFn);

      const [a, b] = await Promise.all([collect(wrapped('k')), collect(wrapped('k'))]);
      expect(a).toEqual([1, 2, 3]);
      expect(b).toEqual([1, 2, 3]);
      expect(produced).toBe(3); // each item pulled from the source exactly once
      expect(sourceFn).toHaveBeenCalledTimes(1); // one shared source
    });

    it('lets a follower keep pulling when the first consumer abandons', async () => {
      let produced = 0;
      const wrapped = withCache(async function* (..._args: unknown[]): AsyncIterable<string> {
        for (const x of ['a', 'b', 'c']) {
          produced++;
          yield x;
        }
      });

      const itA = wrapped('k')[Symbol.asyncIterator]();
      const itB = wrapped('k')[Symbol.asyncIterator]();
      expect((await itA.next()).value).toBe('a'); // A starts the first pull
      expect((await itB.next()).value).toBe('a'); // B reads it from the store
      await itA.return?.(); // A abandons; B is still attached, so the entry lives on

      expect((await itB.next()).value).toBe('b'); // B takes over the pulling
      expect((await itB.next()).value).toBe('c');
      expect((await itB.next()).done).toBe(true);
      expect(produced).toBe(3);
    });

    it('coalesces concurrent demand for the same item into a single pull', async () => {
      let nextCalls = 0;
      const firstPull = deferred();
      const items = ['a', 'b', 'c'];
      let produced = 0;
      // Manual iterator so we can hold the first pull open and count next() calls.
      const source = (..._args: unknown[]): AsyncIterable<string> => ({
        [Symbol.asyncIterator]: () => ({
          async next(): Promise<IteratorResult<string>> {
            nextCalls++;
            if (produced === 0) await firstPull.promise; // hold the first pull open
            if (produced >= items.length) return { value: undefined, done: true };
            return { value: items[produced++], done: false };
          },
        }),
      });
      const wrapped = withCache(source);

      // Two consumers start concurrently. Each collect() runs its first pull synchronously,
      // so A has started the (gated) pull and B has coalesced onto it before we assert.
      const consumerA = collect(wrapped('k'));
      const consumerB = collect(wrapped('k'));
      expect(nextCalls).toBe(1); // one shared pull in flight; B did not start its own

      firstPull.resolve(); // release the shared pull; both consumers now drain to completion
      expect(await consumerA).toEqual(['a', 'b', 'c']); // both receive the full sequence...
      expect(await consumerB).toEqual(['a', 'b', 'c']);
      expect(produced).toBe(3); // ...with each item pulled from the source exactly once
    });

    it('lets the pulling role pass back and forth as each consumer asks for the next item', async () => {
      let produced = 0; // increments only when a call actually reaches the source
      const wrapped = withCache(async function* (..._args: unknown[]): AsyncIterable<number> {
        for (let n = 0; ; n++) {
          produced++;
          yield n;
        }
      });

      const itA = wrapped('k')[Symbol.asyncIterator]();
      const itB = wrapped('k')[Symbol.asyncIterator]();

      expect((await itA.next()).value).toBe(0); // A pulls item 0 from the source
      expect(produced).toBe(1);

      expect((await itB.next()).value).toBe(0); // B reads item 0 from the buffer — no pull
      expect(produced).toBe(1);

      expect((await itB.next()).value).toBe(1); // B pulls item 1 (takes over the pulling)
      expect(produced).toBe(2);

      expect((await itA.next()).value).toBe(1); // A reads item 1 from the buffer — no pull
      expect(produced).toBe(2);

      expect((await itA.next()).value).toBe(2); // A pulls item 2 (takes the role back)
      expect(produced).toBe(3);

      await itA.return?.();
      await itB.return?.();
    });
  });

  describe('incomplete-stream cleanup', () => {
    it('closes the source and re-runs next time when the last consumer abandons an unfinished stream', async () => {
      let cleanedUp = false;
      const sourceFn = streamFn().mockImplementation(async function* () {
        try {
          yield 'a';
          yield 'b';
        } finally {
          cleanedUp = true;
        }
      });
      const wrapped = withCache(sourceFn);

      const it = wrapped('k')[Symbol.asyncIterator]();
      expect((await it.next()).value).toBe('a'); // started but not finished
      await it.return?.(); // last consumer leaves an incomplete stream
      expect(cleanedUp).toBe(true); // shared source was closed
      expect(sourceFn).toHaveBeenCalledTimes(1);

      // Entry was evicted, so the next call runs a fresh source rather than replaying.
      expect(await collect(wrapped('k'))).toEqual(['a', 'b']);
      expect(sourceFn).toHaveBeenCalledTimes(2);
    });

    it('closes the source only when the last concurrent consumer abandons an incomplete stream', async () => {
      let cleanedUp = false;
      const sourceFn = streamFn().mockImplementation(async function* () {
        try {
          yield 'a';
          yield 'b';
        } finally {
          cleanedUp = true;
        }
      });
      const wrapped = withCache(sourceFn);

      const itA = wrapped('k')[Symbol.asyncIterator]();
      const itB = wrapped('k')[Symbol.asyncIterator]();
      expect((await itA.next()).value).toBe('a'); // A starts the shared source
      expect((await itB.next()).value).toBe('a'); // B attaches and reads from the buffer

      await itA.return?.(); // first of two consumers leaves
      expect(cleanedUp).toBe(false); // B is still attached, so the source stays open
      expect(sourceFn).toHaveBeenCalledTimes(1);

      expect((await itB.next()).value).toBe('b'); // the shared entry survived; B keeps pulling

      await itB.return?.(); // last consumer leaves the still-incomplete stream
      expect(cleanedUp).toBe(true); // now the source is closed
    });

    it('keeps a completed stream cached after its consumer leaves', async () => {
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['a', 'b']));
      const wrapped = withCache(sourceFn);

      expect(await collect(wrapped('k'))).toEqual(['a', 'b']); // completes, consumer leaves
      expect(await collect(wrapped('k'))).toEqual(['a', 'b']); // still cached → replayed
      expect(sourceFn).toHaveBeenCalledTimes(1);
    });
  });

  describe('errors', () => {
    it('does not cache a source error by default', async () => {
      const sourceFn = streamFn()
        .mockImplementationOnce(() => failingStream(new Error('boom')))
        .mockImplementationOnce(() => arrayStream(['ok']));
      const wrapped = withCache(sourceFn);

      await expect(collect(wrapped('k'))).rejects.toThrow('boom');
      expect(await collect(wrapped('k'))).toEqual(['ok']); // retried, not cached
      expect(sourceFn).toHaveBeenCalledTimes(2);
    });

    it('caches a source error when cacheErrors is true', async () => {
      const sourceFn = streamFn().mockImplementation(() => failingStream(new Error('boom')));
      const wrapped = withCache(sourceFn, { cacheErrors: true });

      await expect(collect(wrapped('k'))).rejects.toThrow('boom');
      await expect(collect(wrapped('k'))).rejects.toThrow('boom');
      expect(sourceFn).toHaveBeenCalledTimes(1); // error cached
    });

    it('replays the buffered prefix, then the cached error, on a mid-stream failure', async () => {
      const sourceFn = streamFn().mockImplementation(() => failingStream(new Error('boom'), ['a']));
      const wrapped = withCache(sourceFn, { cacheErrors: true });

      const seen1: string[] = [];
      await expect(collectInto(seen1, wrapped('k'))).rejects.toThrow('boom');
      expect(seen1).toEqual(['a']);

      const seen2: string[] = [];
      await expect(collectInto(seen2, wrapped('k'))).rejects.toThrow('boom');
      expect(seen2).toEqual(['a']); // buffered prefix replayed before the cached error
      expect(sourceFn).toHaveBeenCalledTimes(1);
    });

    it('never caches a cancellation, even with cacheErrors', async () => {
      const sourceFn = streamFn()
        .mockImplementationOnce(() => failingStream(abortError()))
        .mockImplementationOnce(() => arrayStream(['ok']));
      const wrapped = withCache(sourceFn, { cacheErrors: true });

      await expect(collect(wrapped('k'))).rejects.toHaveProperty('name', 'AbortError');
      expect(await collect(wrapped('k'))).toEqual(['ok']); // not cached despite cacheErrors
      expect(sourceFn).toHaveBeenCalledTimes(2);
    });

    it('delivers a source error to all concurrently waiting consumers', async () => {
      const boom = new Error('boom');
      const firstPull = deferred();
      let nextCalls = 0;
      // Manual iterator: hold the first pull open, then fail it.
      const source = (..._args: unknown[]): AsyncIterable<string> => ({
        [Symbol.asyncIterator]: () => ({
          async next(): Promise<IteratorResult<string>> {
            nextCalls++;
            await firstPull.promise;
            throw boom;
          },
        }),
      });
      const wrapped = withCache(source);

      const a = collect(wrapped('k'));
      const b = collect(wrapped('k')); // both consumers wait on the same in-flight pull
      const aRejects = expect(a).rejects.toBe(boom); // attach handlers before releasing
      const bRejects = expect(b).rejects.toBe(boom);
      firstPull.resolve(); // the one shared pull now throws
      await aRejects;
      await bRejects;
      expect(nextCalls).toBe(1); // a single shared pull errored for both consumers
    });
  });

  describe('cancellation', () => {
    it('aborts only the consumer whose signal fires, leaving the shared stream intact', async () => {
      const wrapped = withCache(async function* (
        _opts?: { signal?: AbortSignal }
      ): AsyncIterable<string> {
        yield 'a';
        yield 'b';
        yield 'c';
      });
      const controller = new AbortController();

      // Both keys are "[]": the default makeKey strips the trailing { signal }-only bag, so
      // wrapped({ signal }) and wrapped() reduce to no args and share one cached stream.
      const itA = wrapped({ signal: controller.signal })[Symbol.asyncIterator]();
      const itB = wrapped()[Symbol.asyncIterator]();
      expect((await itA.next()).value).toBe('a');
      expect((await itB.next()).value).toBe('a');

      controller.abort();
      await expectAbortError(itA.next()); // A's own signal ends A

      // B is unaffected and drains the shared stream.
      expect((await itB.next()).value).toBe('b');
      expect((await itB.next()).value).toBe('c');
      expect((await itB.next()).done).toBe(true);
    });

    it('rejects at the first pull if the signal is already aborted, without starting the source', async () => {
      const sourceFn = streamFn().mockImplementation(() => arrayStream(['x']));
      const wrapped = withCache(sourceFn);

      await expectAbortError(collect(wrapped({ signal: abortedSignal() })));
      expect(sourceFn).not.toHaveBeenCalled();
    });
  });

  describe('maxSize / LRU eviction', () => {
    // A fresh mock source per test, yielding one item. Its value is irrelevant — these tests
    // assert only cache hit/miss via the call count; the distinct keys come from the number
    // arguments passed to the wrapper (wrapped(1), wrapped(2), ...).
    const oneItemSource = () => streamFn().mockImplementation(() => arrayStream(['x']));

    it('evicts the least-recently-used entry when maxSize is exceeded', async () => {
      const sourceFn = oneItemSource();
      const wrapped = withCache(sourceFn, { maxSize: 2 });

      await collect(wrapped(1)); // miss: fetch key 1 and cache it
      await collect(wrapped(2)); // miss: fetch key 2; cache is now full (keys 1 and 2)
      await collect(wrapped(3)); // miss: fetch key 3; evicts the oldest, key 1
      expect(sourceFn).toHaveBeenCalledTimes(3); // three misses

      await collect(wrapped(2)); // hit: key 2 still cached
      await collect(wrapped(3)); // hit: key 3 still cached
      expect(sourceFn).toHaveBeenCalledTimes(3); // no new fetches

      await collect(wrapped(1)); // miss: key 1 was evicted, so it is fetched again
      expect(sourceFn).toHaveBeenCalledTimes(4);
    });

    it('treats a cache hit as most-recently-used', async () => {
      const sourceFn = oneItemSource();
      const wrapped = withCache(sourceFn, { maxSize: 2 });

      await collect(wrapped(1)); // miss: fetch key 1
      await collect(wrapped(2)); // miss: fetch key 2
      await collect(wrapped(1)); // hit: key 1 — accessing it makes it the most recently used
      expect(sourceFn).toHaveBeenCalledTimes(2);

      await collect(wrapped(3)); // miss: fetch key 3; evicts key 2 (now the oldest)
      expect(sourceFn).toHaveBeenCalledTimes(3);

      await collect(wrapped(1)); // hit: key 1 survived, because the earlier access refreshed it
      expect(sourceFn).toHaveBeenCalledTimes(3);

      await collect(wrapped(2)); // miss: key 2 was the one evicted, so it is fetched again
      expect(sourceFn).toHaveBeenCalledTimes(4);
    });

    it('keeps only one entry when maxSize is 1', async () => {
      const sourceFn = oneItemSource();
      const wrapped = withCache(sourceFn, { maxSize: 1 });

      await collect(wrapped(1)); // miss: fetch key 1
      await collect(wrapped(2)); // miss: fetch key 2; evicts key 1 (only room for one)
      await collect(wrapped(2)); // hit: key 2 still cached
      expect(sourceFn).toHaveBeenCalledTimes(2);

      await collect(wrapped(1)); // miss: key 1 was evicted, so it is fetched again
      expect(sourceFn).toHaveBeenCalledTimes(3);
    });

    it('never evicts when no maxSize is set (unbounded)', async () => {
      const sourceFn = oneItemSource();
      const wrapped = withCache(sourceFn);

      for (let i = 0; i < 20; i++) await collect(wrapped(i));
      expect(sourceFn).toHaveBeenCalledTimes(20);

      for (let i = 0; i < 20; i++) await collect(wrapped(i)); // all still cached
      expect(sourceFn).toHaveBeenCalledTimes(20);
    });
  });

  describe('invalid maxSize', () => {
    it('throws for zero', () => {
      expect(() => withCache(streamFn(), { maxSize: 0 })).toThrow('maxSize must be a positive integer');
    });

    it('throws for negative values', () => {
      expect(() => withCache(streamFn(), { maxSize: -1 })).toThrow('maxSize must be a positive integer');
    });

    it('throws for non-integer values', () => {
      expect(() => withCache(streamFn(), { maxSize: 2.5 })).toThrow('maxSize must be a positive integer');
    });

    it('throws at wrap time, before any stream is started', () => {
      const sourceFn = streamFn();
      expect(() => withCache(sourceFn, { maxSize: 0 })).toThrow();
      expect(sourceFn).not.toHaveBeenCalled();
    });
  });
});
