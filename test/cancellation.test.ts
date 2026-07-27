import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import { withRetry } from '../src/withRetry';
import { withFallback } from '../src/withFallback';
import { withCache } from '../src/withCache';
import { withTimeout } from '../src/withTimeout';
import { withRecordReplay, RecordingNotFoundError } from '../src/withRecordReplay';
import { TimeoutError, isCancellation } from '../src/core/cancellation';
import { stall, abortError, expectAbortError } from './helpers';

describe('cancellation', () => {
  describe('isCancellation / TimeoutError', () => {
    beforeEach(() => {
      jest.clearAllTimers();
      jest.useFakeTimers();
    });

    afterEach(() => {
      jest.runOnlyPendingTimers();
      jest.useRealTimers();
    });

    it('recognizes an AbortError, and nothing else, as a cancellation', () => {
      expect(isCancellation(abortError())).toBe(true);
      expect(isCancellation(new TimeoutError())).toBe(false); // timeout is retryable, not a cancel
      expect(isCancellation(new Error('boom'))).toBe(false);
      expect(isCancellation('AbortError')).toBe(false); // a mere string, not an error object
      expect(isCancellation(null)).toBe(false);
      expect(isCancellation(undefined)).toBe(false);
    });

    it('withTimeout rejects with a TimeoutError by default', async () => {
      const slow = withTimeout(async () => { await stall(); return 'late'; }, 20);
      // Set up one tracked rejection assertion before advancing virtual time.
      const timedOut = expect(slow()).rejects.toMatchObject({
        name: 'TimeoutError',
      });
      // Advance virtual time so the 20ms timeout fires deterministically.
      await jest.advanceTimersByTimeAsync(20);
      // Enforce the timeout rejection after the timer has fired.
      await timedOut;
    });
  });

  describe('policy wrappers skip their policy on cancellation', () => {
    it('withRetry does not retry a cancellation (but does retry other errors)', async () => {
      const cancelled = jest.fn(async () => { throw abortError(); });
      await expectAbortError(withRetry(cancelled, 3)());
      expect(cancelled).toHaveBeenCalledTimes(1); // not retried

      const timingOut = jest.fn(async () => { throw new TimeoutError(); });
      await expect(withRetry(timingOut, 3)()).rejects.toBeInstanceOf(TimeoutError);
      expect(timingOut).toHaveBeenCalledTimes(3); // a timeout is a retryable failure
    });

    it('withFallback does not fall back on a cancellation', async () => {
      const cancelled = jest.fn(async () => { throw abortError(); });
      const fallback = jest.fn(async () => 'fallback');
      await expectAbortError(withFallback(cancelled, fallback)());
      expect(fallback).not.toHaveBeenCalled();
    });

    it('withCache does not cache a cancellation, even with cacheErrors', async () => {
      let calls = 0;
      const fn = jest.fn(async (_k: string) => {
        calls++;
        if (calls === 1) throw abortError(); // cancelled on the first call
        return 'value';
      });
      const cached = withCache(fn, { cacheErrors: true });

      await expectAbortError(cached('k'));
      expect(await cached('k')).toBe('value'); // not cached → fn runs again and succeeds
      expect(fn).toHaveBeenCalledTimes(2);
    });

    it('withRecordReplay does not record a cancellation, even with cacheErrors', async () => {
      const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'cancel-rr-'));
      try {
        const rec = withRecordReplay(async (_k: string) => { throw abortError(); }, dir, {
          mode: 'incrementalRecord',
          cacheErrors: true,
        });
        await expectAbortError(rec('k'));

        // Nothing was recorded: a fresh replay-mode wrapper misses.
        const replay = withRecordReplay(async (_k: string) => 'unused', dir);
        await expect(replay('k')).rejects.toBeInstanceOf(RecordingNotFoundError);
      } finally {
        await fs.rm(dir, { recursive: true, force: true });
      }
    });
  });

  describe('the default cache key excludes the inbound signal', () => {
    it('withCache: a signal-bearing call hits the entry cached without one', async () => {
      const fn = jest.fn(async (_url: string, _opts?: { signal?: AbortSignal }) => 'value');
      const cached = withCache(fn);

      expect(await cached('/api')).toBe('value');             // populate without a signal
      const signal = new AbortController().signal;
      expect(await cached('/api', { signal })).toBe('value'); // same key → cache hit
      expect(fn).toHaveBeenCalledTimes(1);                    // served from cache, not re-run
    });

    it('withCache: real options in the trailing bag still count toward the key', async () => {
      const fn = jest.fn(async (_url: string, _opts?: { region?: string; signal?: AbortSignal }) => 'value');
      const cached = withCache(fn);
      const signal = new AbortController().signal;

      await cached('/api', { region: 'us', signal });
      await cached('/api', { region: 'eu', signal });
      expect(fn).toHaveBeenCalledTimes(2); // different region → different key, despite the signal

      await cached('/api', { region: 'us' }); // matches the first (only the signal is stripped)
      expect(fn).toHaveBeenCalledTimes(2);
    });

    it('withRecordReplay: a signal-bearing call replays a recording made without one', async () => {
      const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'signal-key-rr-'));
      try {
        const rec = withRecordReplay(
          async (_url: string, _opts?: { signal?: AbortSignal }) => 'recorded',
          dir,
          { mode: 'incrementalRecord' },
        );
        await rec('/api'); // record without a signal

        // A fresh replay-mode wrapper: a signal-bearing call resolves from that recording.
        const replay = withRecordReplay(async (_url: string, _opts?: { signal?: AbortSignal }) => 'unused', dir);
        const signal = new AbortController().signal;
        expect(await replay('/api', { signal })).toBe('recorded'); // same key → replayed
      } finally {
        await fs.rm(dir, { recursive: true, force: true });
      }
    });
  });
});
