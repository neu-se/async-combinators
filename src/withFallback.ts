import { isCancellation } from './core/cancellation';

/**
 * Creates a function that attempts to execute a primary function, and falls back to a secondary function if the primary fails.
 * Useful when a primary data source or implementation may be unavailable and an alternative can substitute.
 * 
 * @template ArgTypes - The argument types for both the primary and fallback functions
 * @template RtrnType - The return type for both functions
 * 
 * @param fn - The primary function to attempt first
 * @param fallbackFn - The fallback function to execute if the primary function throws an error
 * 
 * @returns A new function that will try the primary function first, then the fallback function if needed
 * 
 * @throws Error if `fn` or `fallbackFn` is not a function
 * @throws The error from the fallback function if both the primary and fallback functions fail
 * 
 * @example
 * ```typescript
 * // Database fallback example
 * const fetchUserFromPrimary = async (id: number) => {
 *   // Might throw if primary database is down
 *   return await primaryDb.getUser(id);
 * };
 * 
 * const fetchUserFromCache = async (id: number) => {
 *   // Fallback to cached data
 *   return await cacheDb.getUser(id);
 * };
 * 
 * const fetchUser = withFallback(fetchUserFromPrimary, fetchUserFromCache);
 * 
 * // Will try primary first, then cache if primary fails
 * const user = await fetchUser(123);
 * ```
 */
export function withFallback<ArgTypes extends any[], RtrnType>(
    fn: (...args: ArgTypes) => Promise<RtrnType>,
    fallbackFn: (...args: ArgTypes) => Promise<RtrnType>
): (...args: ArgTypes) => Promise<RtrnType> {
  // Validate up front: otherwise a non-callable `fn` throws a TypeError that the
  // catch below would swallow (silently falling back), and a non-callable
  // `fallbackFn` would only fail later, when the primary first errors.
  if (typeof fn !== 'function' || typeof fallbackFn !== 'function') {
    throw new Error('fn and fallbackFn must be functions');
  }

  return async (...args: ArgTypes): Promise<RtrnType> => {
    try {
      return await fn(...args);         // try primary function
    } catch (err) {
      // A cancellation isn't a failure to recover from — propagate it rather
      // than masking it by running the fallback.
      if (isCancellation(err)) throw err;
      return await fallbackFn(...args); // if primary fails, fall back
    }
  };
}