import { TimeoutError, rejectOnAbort, rejectAfter, extractSignal } from '../core/cancellation';
import { closeQuietly } from '../core/iterator';

/**
 * Impose a per-item deadline on a **streaming** function: each call to the stream's
 * `next()` must resolve within `maxDurationMs`, otherwise the stream rejects with
 * the timeout error. This is the streaming analogue of the promise-family
 * `withTimeout`, preserving the signature `(...args) => AsyncIterable<ItemType>` so
 * it composes with the other stream combinators by nesting.
 *
 * Because a stream is pull-based, the deadline measures the SOURCE's response to
 * each pull, not the wall-clock gap between delivered items. The timer runs only
 * while a pull is outstanding (from when the consumer asks to when the source
 * answers). Between pulls, while the wrapper waits for the consumer to ask for the
 * next item, no timer is running, so a slow *consumer* can never trip the timeout — it
 * only fires when the *source* is slow to produce. This one rule covers both the
 * time to the first item (the first `next()`) and the idle gap between items
 * (subsequent `next()`s).
 *
 * On timeout (or an inbound abort) the source may still be mid-pull, so its cleanup
 * (`return()`) is requested without being awaited — awaiting a stuck source could
 * hang and defeat the timeout. On normal completion or the consumer abandoning the
 * stream, the source is released deterministically.
 *
 * @template ArgTypes - The argument types of the streaming function
 * @template ItemType - The item type produced by the stream
 *
 * @param fn - The streaming function to bound
 * @param maxDurationMs - Per-`next()` deadline in ms — a positive integer, or `Infinity` to disable
 * @param timeoutError - Error to reject with when a pull exceeds the deadline (defaults to a
 *   {@link TimeoutError} with message "Operation timed out" — a retryable failure, distinct from
 *   a cancellation)
 *
 * @returns A streaming function with the same signature that bounds each `next()`
 *
 * @throws Error at wrap time if `maxDurationMs` is not a positive integer or `Infinity`
 * @throws The configured timeout error if a pull exceeds the deadline
 */
export function withTimeout<ArgTypes extends any[], ItemType>(
  fn: (...args: ArgTypes) => AsyncIterable<ItemType>,
  maxDurationMs: number,
  timeoutError: unknown = new TimeoutError()
): (...args: ArgTypes) => AsyncIterable<ItemType> {
  const valid = maxDurationMs === Infinity ||
    (Number.isInteger(maxDurationMs) && maxDurationMs >= 1);
  if (!valid) {
    throw new Error('maxDurationMs must be a positive integer or Infinity');
  }

  // Infinity disables the timeout entirely: return fn unchanged (zero overhead).
  if (maxDurationMs === Infinity) {
    return fn;
  }

  return async function* (...args: ArgTypes): AsyncIterable<ItemType> {
    const signal = extractSignal(args);
    signal?.throwIfAborted(); // already aborted → reject before starting anything

    // Get the source's iterator so we can drive next() by hand and race each pull
    // against the timer (a `for await` loop would own the next() calls itself).
    const it = fn(...args)[Symbol.asyncIterator]();
    const abort = rejectOnAbort(signal);
    // True once the pull's race has rejected (a timeout, an abort, or a source error). The
    // source may still be mid-pull then, so the finally closes it best-effort instead of
    // awaiting (which could hang); on the clean path the finally awaits the source's cleanup.
    let sourceMayBeStuck = false;
    try {
      while (true) {
        const timeout = rejectAfter(maxDurationMs, timeoutError);
        let result: IteratorResult<ItemType>;
        try {
          // Three-way race for this pull: the source's next() (a value or done), the
          // per-pull timeout, or an inbound abort; whichever settles first wins. The timer
          // runs only across this await, so a slow consumer never trips it.
          result = await Promise.race([it.next(), timeout.promise, abort.promise]);
        } catch (err) {
          // err is from whichever racer rejected: a source error, the timeout, or an abort.
          sourceMayBeStuck = true;
          throw err;
        } finally {
          timeout.cleanup();
        }
        if (result.done) return;
        yield result.value; // between pulls here: consumer think-time, no timer running
      }
    } finally {
      abort.cleanup();
      // A possibly-stuck source can't be awaited without hanging; close it best-effort instead.
      if (sourceMayBeStuck) {
        closeQuietly(it);
      } else {
        // Clean path (completed or abandoned): the source is idle, so release it and
        // await its cleanup (running its own finally) for deterministic teardown.
        await it.return?.();
      }
    }
  };
}
