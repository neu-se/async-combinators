import { isCancellation, extractSignal, abortableDelay } from '../core/cancellation';
import { makeBackoff, BackoffOptions } from '../core/backoff';

/**
 * Thrown by the stream {@link withRetry} when a `resumable` retry re-runs the source
 * and it produces *fewer* items than were already delivered, so it cannot even
 * reproduce the prefix the consumer has already seen.
 *
 * This is the one sign of a broken `resumable` assertion that the wrapper can detect
 * cheaply (by counting, without buffering items or comparing values). Value-level
 * divergence, a re-run of the same length but with different items, is *not* detected
 * and silently splices the two runs; see the `resumable` option for details.
 */
export class ResumeConsistencyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ResumeConsistencyError';
  }
}

/**
 * Retry a **streaming** function (one returning an `AsyncIterable`) up to
 * `maxAttempts` times, with optional backoff between attempts. This is the
 * streaming analogue of the promise-family `withRetry`: it preserves the wrapped
 * function's signature `(...args) => AsyncIterable<T>`, so it composes with the
 * other stream combinators by ordinary nesting.
 *
 * A stream fails at a *point*, so retry hinges on whether the consumer has
 * already seen output:
 *
 * - **Before the first item.** Nothing has been observed, so a failed attempt is
 *   discarded and a fresh one started transparently. This is the common case (a
 *   dropped connection, or a throttling response that arrives before any data)
 *   and behaves exactly like the promise version.
 * - **After some items.** Restarting the source would re-emit data the consumer
 *   already saw, which is only sound if the stream is *deterministic* in its
 *   arguments. Set `resumable: true` to assert that: the wrapper then re-runs
 *   `fn` and skips the items already delivered ("restart and skip"). Left at its
 *   default (`false`), a failure after the first item propagates, which is the
 *   only correct behavior for a nondeterministic source such as a language-model
 *   token stream.
 *
 * A cancelled attempt is never retried: if `fn` throws an `AbortError` (or the
 * inbound `{ signal }` aborts during a backoff wait), it propagates unchanged. A
 * timeout (`TimeoutError`) is a retryable failure, not a cancellation. Because the
 * wrapper is a (lazy) async generator, the inbound `{ signal }` is checked when
 * iteration begins (the first pull), not when the wrapped function is called: an
 * already-aborted signal surfaces as an `AbortError` on the first `for await` / `next()`.
 *
 * Cleanup of an abandoned attempt (on retry, or when the consumer stops early) is
 * handled by iterating with `for await`, which calls the source iterator's
 * `return()` when the loop is exited by completion, error, or the consumer
 * abandoning this stream.
 *
 * @template ArgTypes - The argument types of the streaming function
 * @template ItemType - The type of each item produced by the stream
 *
 * @param fn - The streaming function to retry on failure
 * @param maxAttempts - Maximum number of attempts, including the first (must be a positive integer)
 * @param options - Backoff options (see {@link BackoffOptions}), plus:
 * @param options.resumable - Assert that `fn` is deterministic in its arguments, so
 *   a failure after items have been delivered can be recovered by re-running `fn`
 *   and skipping the items already seen. The wrapper trusts this assertion: it does
 *   not compare item values, so a false assertion on a same-length source silently
 *   splices two different runs (garbage in, garbage out). It does detect the one case
 *   it can catch cheaply, without buffering: a re-run that yields *fewer* items than
 *   were already delivered throws a {@link ResumeConsistencyError} instead of silently
 *   truncating the stream. Default false (retry only covers a failure
 *   before the first item; a later failure propagates). Note that `maxAttempts` is a
 *   *global* budget: it bounds the total number of attempts across the whole stream,
 *   not the attempts at a given position. So a long `resumable` stream that fails and
 *   resumes repeatedly can still exhaust the budget even while it keeps making forward
 *   progress.
 * @param options.onRetry - Callback invoked after a failed attempt when a retry will
 *   follow. Receives the 1-based number of the attempt that just failed and its error.
 *
 * @returns A new streaming function with the same signature that retries per the configured strategy
 *
 * @example
 * ```typescript
 * // A deterministic, cursor-free paginated feed: safe to restart-and-skip.
 * const resilientPages = withRetry(streamPages, 5,
 *   { delayMs: 200, backoff: 'exponential', resumable: true });
 * for await (const page of resilientPages(query)) { ... }
 * ```
 *
 * @throws Error at wrap time if `maxAttempts` is not a positive integer, or an option is out of range
 * @throws {ResumeConsistencyError} if a `resumable` retry re-runs the source and it yields fewer items than were already delivered
 * @throws The error from the final (or first unrecoverable) attempt if the stream cannot complete
 */
export function withRetry<ArgTypes extends any[], ItemType>(
  fn: (...args: ArgTypes) => AsyncIterable<ItemType>,
  maxAttempts: number,
  options: BackoffOptions & {
    resumable?: boolean;
    onRetry?: (failedAttempt: number, error: unknown) => void;
  } = {}
): (...args: ArgTypes) => AsyncIterable<ItemType> {
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
    throw new Error('maxAttempts must be a positive integer');
  }

  // Validates the backoff options at wrap time and returns the per-retry delay;
  // shared with the promise-family retry so both compute backoff identically.
  const computeDelay = makeBackoff(options);
  const { resumable = false, onRetry } = options;

  return async function* (...args: ArgTypes): AsyncIterable<ItemType> {
    const signal = extractSignal(args);
    signal?.throwIfAborted(); // already aborted → don't attempt

    // Number of items the consumer has already seen. On a resumable retry we
    // re-run fn and skip this many items so the consumer sees no duplicates.
    let delivered = 0;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      let seen = 0;
      let completed = false;
      try {
        for await (const item of fn(...args)) {
          // Replay past the prefix already delivered (only reached when resuming
          // a deterministic stream; on the first attempt delivered is 0).
          if (seen++ < delivered) continue;
          delivered++;
          yield item;
        }
        completed = true; // stream finished normally
      } catch (err) {
        // Never retry a cancelled call — propagate it unchanged. (A timeout is a
        // TimeoutError, not a cancellation, and is still retried.)
        if (isCancellation(err)) throw err;
        // Out of attempts.
        if (attempt >= maxAttempts) throw err;
        // Partial output already delivered from a source not declared resumable:
        // restarting could re-emit or (for a nondeterministic source) diverge, so
        // give up and propagate.
        if (delivered > 0 && !resumable) throw err;
        // Report the attempt that just failed; a retry will follow.
        onRetry?.(attempt, err);
        // Backoff before the next attempt. The wait is abortable: an inbound
        // abort during it abandons the retry (rejecting with the abort reason).
        const delay = computeDelay(attempt);
        if (delay > 0) await abortableDelay(delay, signal);
      }

      if (completed) {
        // The check runs outside the try so it is not itself retried. A resumed run
        // must at least reproduce the already-delivered prefix; if it yielded fewer
        // items, the source is not deterministic as asserted, so fail loudly rather
        // than silently truncating the stream. (Value-level divergence at equal or
        // greater length cannot be caught without buffering, and is not detected.)
        if (seen < delivered) {
          throw new ResumeConsistencyError(
            `resumable retry produced ${seen} item(s) but ${delivered} had already been ` +
            `delivered; the source is not deterministic`
          );
        }
        return;
      }
    }
  };
}
