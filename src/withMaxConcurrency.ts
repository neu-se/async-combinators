import { extractSignal } from './core/cancellation';
import { Semaphore } from './semaphore';

/**
 * Limits the number of concurrent executions of an async function.
 *
 * When the maximum concurrency limit is reached, additional calls are queued
 * and executed in FIFO order as previous calls complete. This is useful for
 * controlling resource usage, preventing API rate limits, or managing database
 * connection pools.
 * 
 * @template ArgTypes - The argument types for the function being limited
 * @template RtrnType - The return type of the function being limited
 * 
 * @param fn - The async function to wrap with concurrency limiting
 * @param maxConcurrent - Maximum number of concurrent executions allowed (must be a positive integer)
 *
 * @returns A new function with the same signature that enforces the concurrency limit
 * 
 * @example
 * ```typescript
 * // Limit API calls to 3 concurrent requests
 * const limitedFetch = withMaxConcurrency(
 *   async (url: string) => {
 *     const response = await fetch(url);
 *     return response.json();
 *   },
 *   3
 * );
 * 
 * // These 10 calls will execute with max 3 concurrent
 * const urls = Array.from({ length: 10 }, (_, i) => `/api/item/${i}`);
 * const results = await Promise.all(urls.map(url => limitedFetch(url)));
 * ```
 * 
 * @example
 * ```typescript
 * // Control database query concurrency
 * const limitedQuery = withMaxConcurrency(
 *   async (query: string) => database.execute(query),
 *   5 // Max 5 concurrent queries
 * );
 * 
 * // Process large batch without overwhelming the database
 * const queries = generateQueries(1000);
 * const results = await Promise.all(queries.map(q => limitedQuery(q)));
 * ```
 * 
 * @throws Error if `maxConcurrent` is not a positive integer
 * @throws Any error thrown by the wrapped function
 */
export function withMaxConcurrency<ArgTypes extends any[], RtrnType>(
    fn: (...args: ArgTypes) => Promise<RtrnType>,
    maxConcurrent: number
): (...args: ArgTypes) => Promise<RtrnType> {
  if (!Number.isInteger(maxConcurrent) || maxConcurrent < 1) {
    throw new Error('maxConcurrent must be a positive integer');
  }

  // A permit == a concurrency slot. The semaphore holds the FIFO queue and the
  // active-count bookkeeping; runExclusive acquires a slot, runs fn, and releases
  // (even on throw, so a failing call never stalls the queue).
  const sem = new Semaphore(maxConcurrent);

  // Inbound cancellation is handled by the semaphore: a trailing { signal } drops
  // this call while it's still WAITING for a slot (fn never runs). A call that has
  // already acquired a slot can only be stopped by fn honoring the signal.
  return (...args: ArgTypes): Promise<RtrnType> =>
    sem.runExclusive(() => fn(...args), extractSignal(args));
}