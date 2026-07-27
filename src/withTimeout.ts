import { TimeoutError, rejectOnAbort, rejectAfter, extractSignal } from "./core/cancellation";

/**
 * Run a promise-producing function with a timeout. If the promise is not
 * fulfilled within the specified number of milliseconds, the returned promise
 * is rejected with the specified error.
 * 
 * @template ArgTypes - The argument types for the function with timeout
 * @template RtrnType - The return type of the function with timeout
 * 
 * @param fn - The async function to wrap with timeout functionality
 * @param maxDurationMs - Timeout in milliseconds — a positive integer, or Infinity to disable the timeout
 * @param timeoutError - Error to throw when the timeout is exceeded (defaults to a
 *   {@link TimeoutError} with message "Operation timed out" — a retryable failure,
 *   distinct from a cancellation)
 * 
 * @returns A new function with the same signature that automatically cancels operations that exceed the specified time limit
 * 
 * @example
 * ```typescript
 * // Add timeout to a slow operation
 * const fetchWithTimeout = withTimeout(
 *   async (url: string) => {
 *     return fetch(url);
 *   },
 *   5000, // 5 second timeout
 *   new Error('Request timed out after 5 seconds')
 * );
 * 
 * const response = await fetchWithTimeout('/api/slow-endpoint');
 * ```
 * 
 * @throws Error if maxDurationMs is not a positive integer or Infinity
 * @throws The configured timeout error if the operation exceeds the time limit
 * @throws Any error thrown by the wrapped function if it completes before timeout
 */
export function withTimeout<ArgTypes extends any[], RtrnType>(
    fn: (...args: ArgTypes) => Promise<RtrnType>,
    maxDurationMs: number,
    timeoutError: unknown = new TimeoutError()
): (...args: ArgTypes) => Promise<RtrnType> {
  const valid = maxDurationMs === Infinity ||
    (Number.isInteger(maxDurationMs) && maxDurationMs >= 1);
  if (!valid) {
    throw new Error('maxDurationMs must be a positive integer or Infinity');
  }

  // Infinity disables the timeout entirely: return fn unchanged (zero overhead).
  if (maxDurationMs === Infinity) {
    return fn;
  }

  return async (...args: ArgTypes): Promise<RtrnType> => {
    // Inbound cancellation: if the caller passed a trailing { signal }, an abort
    // abandons the timeout wait and rejects with the signal's reason.
    const signal = extractSignal(args);
    signal?.throwIfAborted(); // already aborted → reject before starting anything

    const timeout = rejectAfter(maxDurationMs, timeoutError);
    const abort = rejectOnAbort(signal);
    try {
      return await Promise.race([fn(...args), timeout.promise, abort.promise]);
    } finally {
      // Clear the timer and detach the abort listener once the race settles; otherwise
      // a still-pending timeout keeps the Node event loop alive until maxDurationMs
      // elapses (even when fn already won), delaying process exit.
      timeout.cleanup();
      abort.cleanup();
    }
  };
}