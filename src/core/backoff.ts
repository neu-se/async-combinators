/**
 * Backoff strategy shared by the retry combinators.
 *
 * {@link makeBackoff} validates the options at wrap time and returns a pure
 * `computeDelay(failedAttempt)` closure, so each retry variant owns only its own
 * attempt loop and never re-implements the delay math or the option checks. This
 * is the piece the promise-family `withRetry` and the future stream-family
 * `withRetry` share, so a single definition keeps their backoff behavior identical.
 */

/**
 * Backoff options common to every retry combinator.
 *
 * @property delayMs - Base delay before a retry, in ms. Default 0 (retry immediately).
 *   Must be a non-negative, finite number.
 * @property backoff - `'fixed'` (constant `delayMs`) or `'exponential'`
 *   (`delayMs * factor^(k-1)` before the k-th retry). Default `'exponential'`.
 * @property factor - Growth factor for `'exponential'` backoff. Default 2. Must be a finite number >= 1.
 * @property maxDelayMs - Upper bound on the computed delay, in ms. Default `Infinity`. Must be a positive number.
 * @property jitter - When true, the actual delay is a random value in `[0, computed]`
 *   (full jitter), spreading retries to avoid a thundering herd. Default false.
 */
export interface BackoffOptions {
  delayMs?: number;
  backoff?: 'fixed' | 'exponential';
  factor?: number;
  maxDelayMs?: number;
  jitter?: boolean;
}

/**
 * Validate `options` and return a closure giving the delay (in ms) to wait before
 * the `failedAttempt`-th retry (1-based: `failedAttempt = 1` is the first retry,
 * after attempt 1 failed).
 *
 * Validation happens here, at wrap time, so an out-of-range option throws before
 * any wrapped function is ever called.
 *
 * @throws Error if any option is out of range.
 */
export function makeBackoff(options: BackoffOptions = {}): (failedAttempt: number) => number {
  const {
    delayMs = 0,
    backoff = 'exponential',
    factor = 2,
    maxDelayMs = Infinity,
    jitter = false,
  } = options;

  if (!Number.isFinite(delayMs) || delayMs < 0) {
    throw new Error('delayMs must be a non-negative number');
  }
  if (backoff !== 'fixed' && backoff !== 'exponential') {
    throw new Error("backoff must be 'fixed' or 'exponential'");
  }
  if (!Number.isFinite(factor) || factor < 1) {
    throw new Error('factor must be a finite number >= 1');
  }
  if (typeof maxDelayMs !== 'number' || !(maxDelayMs > 0)) {
    throw new Error('maxDelayMs must be a positive number');
  }

  // Delay before the k-th retry (`failedAttempt` = k, 1-based: k=1 is the first
  // retry, after attempt 1 failed).
  return (failedAttempt: number): number => {
    const base = backoff === 'exponential'
      ? delayMs * Math.pow(factor, failedAttempt - 1)
      : delayMs;
    const capped = Math.min(base, maxDelayMs);
    return jitter ? Math.random() * capped : capped;
  };
}
