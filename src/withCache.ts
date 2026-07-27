import { isCancellation, stripSignal } from './core/cancellation';

/**
 * Adds caching to an asynchronous function.
 * The cache is based on generated keys from the function arguments. By default,
 * keys are created by stringifying the arguments, but a custom key function
 * can be provided for more control over caching behavior.
 * If the function is called again with arguments that produce the same key,
 * the cached result is returned instead of calling the function again.
 * 
 * @template ArgTypes - The argument types for the function being cached
 * @template RtrnType - The return type of the function being cached
 * 
 * @param fn - The async function to add caching to
 * @param options - Configuration options for caching behavior
 * @param options.makeKey - Custom function to generate cache keys from arguments. The default
 *   is `JSON.stringify` with a trailing inbound `{ signal }` omitted, so a per-call cancellation
 *   signal never changes the key. A custom `makeKey` receives the raw args (including any signal)
 *   and is responsible for excluding it if it keys on the trailing options object.
 * @param options.cacheErrors - Whether to cache rejected promises (default: false)
 * @param options.maxSize - Maximum number of entries to keep. When exceeded, the
 *   least-recently-used entry is evicted. Must be a positive integer. Omit for an
 *   unbounded cache (default).
 * 
 * @example
 * ```typescript
 * // Cache expensive API calls
 * const cachedFetch = withCache(
 *   async (url: string) => {
 *     const response = await fetch(url);
 *     return response.json();
 *   },
 *   { cacheErrors: false }
 * );
 * 
 * // First call hits the API
 * const data1 = await cachedFetch('/api/users');
 * // Second call returns cached result
 * const data2 = await cachedFetch('/api/users');
 * ```
 * 
 * @returns A new function with the same signature that caches results to improve performance on repeated calls
 * 
 * @throws Error if `maxSize` is provided and is not a positive integer
 * @throws Errors thrown by the wrapped function (unless cacheErrors is true and the error was previously cached)
 */
export function withCache<ArgTypes extends any[], RtrnType>(
    fn: (...args: ArgTypes) => Promise<RtrnType>,
    options: {
      cacheErrors?: boolean;
      makeKey?: (args: ArgTypes) => string;
      maxSize?: number;
    } = {}
): (...args: ArgTypes) => Promise<RtrnType> {
  // Default key omits a trailing inbound { signal } — it's a transient per-call
  // concern, not part of the logical key (see stripSignal).
  const { cacheErrors = false, makeKey = (args) => JSON.stringify(stripSignal(args)), maxSize } = options;

  if (maxSize !== undefined && (!Number.isInteger(maxSize) || maxSize < 1)) {
    throw new Error('maxSize must be a positive integer');
  }

  // Insertion order in a Map is the LRU order: the first key is the
  // least-recently-used, the last is the most-recently-used.
  const cache = new Map<string, Promise<RtrnType>>();

  return (...args: ArgTypes): Promise<RtrnType> => {
    const key = makeKey(args);

    if (cache.has(key)) {
      const cached = cache.get(key)!;
      // Only maintain LRU order when a size limit is in effect; when the cache
      // is unbounded, nothing is ever evicted so the reordering is pure overhead.
      if (maxSize !== undefined) {
        cache.delete(key); // remove and reinsert to mark the element as the most recent
        cache.set(key, cached);
      }
      return cached;
    }

    // Evict on rejection unless we're caching errors — but a *cancellation* is
    // never cached (it isn't a real result or failure of fn), regardless of
    // cacheErrors. On success the entry stays.
    const promise = fn(...args).catch(error => {
      if (!cacheErrors || isCancellation(error)) {
        cache.delete(key);
      }
      throw error; // re-throw so the caller still sees the original error
    });
    cache.set(key, promise);

    // Evict the least-recently-used entry if we've exceeded the size limit.
    if (maxSize !== undefined && cache.size > maxSize) {
      cache.delete(cache.keys().next().value!);
    }

    return cache.get(key)!;
  };
}