import { isCancellation, extractSignal, abortableDelay } from './core/cancellation';
import { makeBackoff, BackoffOptions } from './core/backoff';

/**
 * Retry a promise-producing function up to `maxAttempts` times, with optional
 * backoff between attempts.
 *
 * A cancelled attempt is never retried: if `fn` rejects with an `AbortError` (or
 * the inbound `{ signal }` aborts during a backoff wait), the rejection
 * propagates unchanged. A timeout (`TimeoutError`) is a retryable failure, not a
 * cancellation, so it is still retried.
 *
 * @template ArgTypes - The argument types for the function being retried
 * @template RtrnType - The return type of the function being retried
 *
 * @param fn - The async function to retry on failure
 * @param maxAttempts - Maximum number of attempts, including the first (must be a positive integer)
 * @param options - Backoff and notification options.
 * @param options.delayMs - Base delay before a retry, in ms. Default 0 (retry immediately).
 *   Must be a non-negative, finite number.
 * @param options.backoff - `'fixed'` (constant `delayMs`) or `'exponential'`
 *   (`delayMs * factor^(k-1)` before the k-th retry). Default `'exponential'`.
 * @param options.factor - Growth factor for `'exponential'` backoff. Default 2. Must be a finite number >= 1.
 * @param options.maxDelayMs - Upper bound on the computed delay, in ms. Default `Infinity`. Must be a positive number.
 * @param options.jitter - When true, the actual delay is a random value in `[0, computed]`
 *   (full jitter), spreading retries to avoid a thundering herd. Default false.
 * @param options.shouldRetry - Predicate deciding whether a thrown error is eligible for
 *   retry. Called with the error (error-first, unlike `onRetry`'s `(failedAttempt, error)`,
 *   since the error is what is being classified) and the 1-based number of the attempt that
 *   just failed. Return `false` to propagate the error immediately instead of retrying.
 *   Default `() => true` (retry every error, preserving prior behavior). Only ever *narrows*
 *   retries: it is checked after the cancellation and `maxAttempts` guards, so it cannot
 *   resurrect a retry those already ruled out.
 * @param options.onRetry - Callback invoked after a failed attempt when a retry will follow.
 *   Receives the 1-based number of the attempt that just failed and the error it threw.
 *   Not called when `shouldRetry` rejects the error.
 *
 * @returns A new function with the same signature that retries failed operations per the configured strategy
 *
 * @example
 * ```typescript
 * // Retry an API call with exponential backoff (100ms, 200ms, 400ms, ...)
 * const fetchWithRetry = withRetry(
 *   async (url: string) => {
 *     const response = await fetch(url);
 *     if (!response.ok) throw new Error(`HTTP ${response.status}`);
 *     return response.json();
 *   },
 *   3, // up to 3 attempts
 *   { delayMs: 100, backoff: 'exponential', onRetry: (n, e) => console.log(`attempt ${n} failed:`, e) }
 * );
 *
 * const data = await fetchWithRetry('/api/data');
 * ```
 *
 * @throws Error at wrap time if `maxAttempts` is not a positive integer, or an option is out of range
 * @throws The error from the final attempt if all attempts fail
 */
export function withRetry<ArgTypes extends any[], RtrnType>(
    fn: (...args: ArgTypes) => Promise<RtrnType>,
    maxAttempts: number,
    options: BackoffOptions & {
      shouldRetry?: (error: unknown, failedAttempt: number) => boolean;
      onRetry?: (failedAttempt: number, error: unknown) => void;
    } = {}
): (...args: ArgTypes) => Promise<RtrnType> {
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
    throw new Error('maxAttempts must be a positive integer');
  }

  // Validates the backoff options at wrap time and returns the per-retry delay;
  // shared with the stream-family retry so both compute backoff identically.
  const computeDelay = makeBackoff(options);
  const { shouldRetry = () => true, onRetry } = options;

  return async (...args: ArgTypes): Promise<RtrnType> => {
    const signal = extractSignal(args);
    signal?.throwIfAborted(); // already aborted → don't attempt

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return await fn(...args);
      } catch (err) {
        // Never retry a cancelled call — the caller (or fn) asked to stop, so
        // propagate it unchanged. (A timeout is a TimeoutError, not a cancellation,
        // and is still retried.)
        if (isCancellation(err)) throw err;
        // Use >= (not ===) so the final attempt is detected even if the guard
        // above were ever bypassed; a strict === would miss non-integer bounds.
        if (attempt >= maxAttempts) throw err;
        // Caller-supplied classification: propagate immediately if this error
        // isn't worth retrying (e.g. an unrecoverable API error). Checked after
        // the guards above, so it can only narrow, never extend, retries.
        if (!shouldRetry(err, attempt)) throw err;
        // Report the attempt that just failed (paired with the error it threw);
        // a retry as attempt (attempt + 1) will follow.
        onRetry?.(attempt, err);
        // Backoff before the next attempt. The wait is abortable: an inbound
        // abort during it abandons the retry (rejecting with the abort reason).
        const delay = computeDelay(attempt);
        if (delay > 0) await abortableDelay(delay, signal);
      }
    }

    throw new Error("Unreachable");
  };
}
