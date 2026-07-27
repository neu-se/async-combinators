import { extractSignal, abortableDelay } from './core/cancellation';

/**
 * Rate limit a function to execute at most once per interval.
 *
 * Subsequent calls within the interval will be delayed until the interval has passed.
 * Each wrapped function instance maintains its own rate limiting state.
 * This is useful for preventing API rate limit violations or throttling expensive operations.
 * 
 * @template ArgTypes - The argument types for the function being rate limited
 * @template RtrnType - The return type of the function being rate limited
 * 
 * @param fn - The async function to wrap with rate limiting functionality
 * @param intervalMs - Minimum interval between executions in milliseconds (must be a positive integer)
 *
 * @returns A new function with the same signature that enforces rate limiting between calls
 *
 * @throws Error if intervalMs is not a positive integer
 * @throws Any error thrown by the wrapped function
 *
 * @example
 * ```typescript
 * // Rate limit API calls to at most one every 2 seconds
 * const rateLimitedFetch = withRateLimit(
 *   async (url: string) => {
 *     const response = await fetch(url);
 *     return response.json();
 *   },
 *   2000 // 2 second minimum interval
 * );
 * 
 * // These calls will be automatically spaced 2 seconds apart
 * const data1 = await rateLimitedFetch('/api/users');
 * const data2 = await rateLimitedFetch('/api/posts'); // Will wait if called too soon
 * ```
 */
export function withRateLimit<ArgTypes extends any[], RtrnType>(
    fn: (...args: ArgTypes) => Promise<RtrnType>,
    intervalMs: number
): (...args: ArgTypes) => Promise<RtrnType> {
  if (!Number.isInteger(intervalMs) || intervalMs < 1) {
    throw new Error('intervalMs must be a positive integer');
  }

  let nextAllowedTime = 0;
  return async (...args: ArgTypes): Promise<RtrnType> => {
    // Inbound cancellation: if the caller passed a trailing { signal }, an abort
    // abandons the rate-limit wait and rejects with the signal's reason.
    const signal = extractSignal(args);
    signal?.throwIfAborted(); // already aborted → don't even reserve a slot

    const now = Date.now();
    // Reserve this call's slot synchronously, before any await. Otherwise
    // concurrent callers all read the same `nextAllowedTime`, compute the same
    // delay, and fire together instead of being spaced one interval apart.
    const slot = Math.max(now, nextAllowedTime);
    nextAllowedTime = slot + intervalMs;
    const delay = slot - now;
    if (delay > 0) {
      // Wait out the interval, bailing (fn never runs) if the signal aborts first.
      await abortableDelay(delay, signal);
    }
    return fn(...args);
  };
}