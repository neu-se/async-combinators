import { isCancellation } from '../core/cancellation';

/**
 * Try a **streaming** primary function, and fall back to a secondary streaming
 * function if the primary fails *before producing any output*. This is the
 * streaming analogue of the promise-family `withFallback`, preserving the
 * signature `(...args) => AsyncIterable<ItemType>` so it composes with the other
 * stream combinators by nesting.
 *
 * The pre-first-item constraint is essential. Once the primary has yielded an
 * item, the consumer has committed to the primary's output, and the fallback is a
 * *different* source producing its own sequence, so switching would splice two
 * unrelated streams. The fallback therefore runs only if the primary throws before
 * yielding anything; a failure after the first item propagates unchanged. (Unlike
 * `withRetry`, there is no `resumable` option: the fallback is not a re-run of the
 * primary, so its output can never continue the primary's.)
 *
 * A cancellation is not a failure to recover from: if the primary throws an
 * `AbortError`, it propagates and the fallback is not run.
 *
 * @template ArgTypes - The argument types for both the primary and fallback
 * @template ItemType - The item type produced by both streams
 *
 * @param fn - The primary streaming function to try first
 * @param fallbackFn - The streaming function to use if the primary fails before its first item
 *
 * @returns A streaming function that yields the primary's items, or the fallback's if the primary fails before producing any
 *
 * @throws Error at wrap time if `fn` or `fallbackFn` is not a function
 * @throws The primary's error if it fails after delivering items, or is cancelled
 * @throws The fallback's error if the primary fails before its first item and the fallback also fails
 *
 * @example
 * ```typescript
 * // Fall back to a replica's event stream if the primary connection drops before
 * // delivering anything; a mid-stream drop propagates (no silent splice).
 * const events = withFallback(streamFromPrimary, streamFromReplica);
 * for await (const e of events(topic)) { handle(e); }
 * ```
 */
export function withFallback<ArgTypes extends any[], ItemType>(
  fn: (...args: ArgTypes) => AsyncIterable<ItemType>,
  fallbackFn: (...args: ArgTypes) => AsyncIterable<ItemType>
): (...args: ArgTypes) => AsyncIterable<ItemType> {
  // Validate up front: otherwise a non-callable `fn` would only fail once iteration
  // begins, and a non-callable `fallbackFn` would fail even later (or never).
  if (typeof fn !== 'function' || typeof fallbackFn !== 'function') {
    throw new Error('fn and fallbackFn must be functions');
  }

  return async function* (...args: ArgTypes): AsyncIterable<ItemType> {
    let delivered = 0;
    try {
      for await (const item of fn(...args)) {
        delivered++;
        yield item;
      }
      return; // primary completed successfully
    } catch (err) {
      // A cancellation isn't a failure to recover from — propagate it rather than
      // masking it by running the fallback.
      if (isCancellation(err)) throw err;
      // Once the primary has produced output, the consumer has committed to it, so
      // switching to a different source would splice two unrelated streams. Only
      // fall back if nothing was delivered.
      if (delivered > 0) throw err;
      // primary failed before its first item — fall through to the fallback
    }
    yield* fallbackFn(...args);
  };
}
