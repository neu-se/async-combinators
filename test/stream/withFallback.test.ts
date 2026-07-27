import { withFallback } from '../../src/stream/withFallback';
import { collect, collectInto, arrayStream, failingStream, streamFn } from './helpers';
import { abortError, expectAbortError } from '../helpers';

describe('stream/withFallback', () => {
  describe('basic functionality', () => {
    it('yields the primary stream when it succeeds; the fallback is not called', async () => {
      const primary = streamFn().mockImplementation(() => arrayStream(['a', 'b', 'c']));
      const fallback = streamFn().mockImplementation(() => arrayStream(['x', 'y']));
      const wrapped = withFallback(primary, fallback);

      const out = await collect(wrapped('arg'));
      expect(out).toEqual(['a', 'b', 'c']);
      expect(primary).toHaveBeenCalledTimes(1);
      expect(primary).toHaveBeenCalledWith('arg');
      expect(fallback).not.toHaveBeenCalled();
    });

    it('yields the fallback stream when the primary fails before the first item', async () => {
      const primary = streamFn().mockImplementation(() => failingStream(new Error('primary down')));
      const fallback = streamFn().mockImplementation(() => arrayStream(['x', 'y']));
      const wrapped = withFallback(primary, fallback);

      const out = await collect(wrapped('arg'));
      expect(out).toEqual(['x', 'y']);
      expect(primary).toHaveBeenCalledTimes(1); // tried once — withFallback does not retry
      expect(primary).toHaveBeenCalledWith('arg');
      expect(fallback).toHaveBeenCalledTimes(1);
      expect(fallback).toHaveBeenCalledWith('arg');
    });

    it('throws the fallback error when both fail before their first item', async () => {
      const fallbackError = new Error('fallback down');
      const primary = streamFn().mockImplementation(() => failingStream(new Error('primary down')));
      const fallback = streamFn().mockImplementation(() => failingStream(fallbackError));
      const wrapped = withFallback(primary, fallback);

      await expect(collect(wrapped('arg'))).rejects.toBe(fallbackError);
      expect(primary).toHaveBeenCalledTimes(1);
      expect(primary).toHaveBeenCalledWith('arg');
      expect(fallback).toHaveBeenCalledTimes(1);
      expect(fallback).toHaveBeenCalledWith('arg');
    });
  });

  describe('error handling', () => {
    it('falls back on non-Error thrown values from the primary', async () => {
      for (const thrown of ['string error', 404, { message: 'obj' }, null, undefined]) {
        const primary = streamFn().mockImplementation(() => failingStream(thrown));
        const fallback = streamFn().mockImplementation(() => arrayStream(['fb']));
        const wrapped = withFallback(primary, fallback);

        expect(await collect(wrapped('t'))).toEqual(['fb']);
        expect(fallback).toHaveBeenCalledWith('t');
      }
    });

    it('preserves the fallback error when both fail', async () => {
      const fallbackError = new Error('specific fallback message');
      const primary = streamFn().mockImplementation(() => failingStream(new Error('primary')));
      const fallback = streamFn().mockImplementation(() => failingStream(fallbackError));
      const wrapped = withFallback(primary, fallback);

      await expect(collect(wrapped('t'))).rejects.toBe(fallbackError);
    });
  });

  // --- streaming-specific: the pre-first-item constraint ------------------

  describe('partial output', () => {
    it('does NOT fall back once the primary has delivered items; it propagates the primary error', async () => {
      const primaryError = new Error('mid-stream');
      const primary = streamFn().mockImplementation(() => failingStream(primaryError, ['a', 'b']));
      const fallback = streamFn().mockImplementation(() => arrayStream(['x', 'y']));
      const wrapped = withFallback(primary, fallback);

      const seen: string[] = [];
      await expect(collectInto(seen, wrapped('t'))).rejects.toBe(primaryError);
      expect(seen).toEqual(['a', 'b']); // consumer keeps the primary's prefix
      expect(primary).toHaveBeenCalledTimes(1); // tried once, not restarted
      expect(fallback).not.toHaveBeenCalled(); // never switched to a different source
    });

    it('does not fall back when the primary is cancelled before the first item', async () => {
      const primary = streamFn().mockImplementation(() => failingStream(abortError()));
      const fallback = streamFn().mockImplementation(() => arrayStream(['x']));
      const wrapped = withFallback(primary, fallback);

      await expectAbortError(collect(wrapped('t')));
      expect(fallback).not.toHaveBeenCalled(); // a cancellation is not recovered from
    });
  });

  describe('composition', () => {
    it('supports chained fallbacks (primary -> fb1 -> fb2)', async () => {
      const primary = streamFn().mockImplementation(() => failingStream(new Error('p')));
      const fb1 = streamFn().mockImplementation(() => failingStream(new Error('fb1')));
      const fb2 = streamFn().mockImplementation(() => arrayStream(['final']));

      const step1 = withFallback(primary, fb1);
      const chained = withFallback(step1, fb2);

      expect(await collect(chained('t'))).toEqual(['final']);
      expect(primary).toHaveBeenCalledTimes(1);
      expect(primary).toHaveBeenCalledWith('t');
      expect(fb1).toHaveBeenCalledTimes(1);
      expect(fb1).toHaveBeenCalledWith('t');
      expect(fb2).toHaveBeenCalledTimes(1);
      expect(fb2).toHaveBeenCalledWith('t');
    });
  });

  describe('cleanup on early exit', () => {
    // A source that releases a resource in a `finally`; `flag.done` = "resource closed".
    function cleanupSource(flag: { done: boolean }) {
      return async function* (): AsyncIterable<string> {
        try {
          yield 'a';
          yield 'b';
          yield 'c';
        } finally {
          flag.done = true;
        }
      };
    }

    it('cleans up the primary when the consumer abandons it mid-stream', async () => {
      const flag = { done: false };
      const wrapped = withFallback(cleanupSource(flag), () => arrayStream(['x']));
      for await (const v of wrapped()) {
        if (v === 'a') break; // abandon during the primary
      }
      expect(flag.done).toBe(true);
    });

    it('cleans up the fallback when the consumer abandons it mid-stream', async () => {
      const flag = { done: false };
      // Primary fails before its first item, so the fallback runs; abandon during it.
      const wrapped = withFallback(() => failingStream<string>(new Error('down')), cleanupSource(flag));
      for await (const v of wrapped()) {
        if (v === 'a') break; // abandon during the fallback
      }
      expect(flag.done).toBe(true);
    });
  });

  describe('invalid arguments', () => {
    it('throws when fn is not a function', () => {
      expect(() => withFallback(undefined as any, streamFn())).toThrow(
        'fn and fallbackFn must be functions'
      );
    });

    it('throws when fallbackFn is not a function', () => {
      expect(() => withFallback(streamFn(), undefined as any)).toThrow(
        'fn and fallbackFn must be functions'
      );
    });

    it('throws at wrap time, before either stream is started', () => {
      const primary = streamFn();
      expect(() => withFallback(primary, 'not-a-fn' as any)).toThrow();
      expect(primary).not.toHaveBeenCalled();
    });
  });
});
