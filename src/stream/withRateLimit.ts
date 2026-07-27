import { extractSignal, abortableDelay } from '../core/cancellation';

/**
 * Rate limit a **streaming** function so that its streams *start* at most once per
 * interval. This is the streaming analogue of the promise-family `withRateLimit`,
 * preserving the signature `(...args) => AsyncIterable<ItemType>` so it composes
 * with the other stream combinators by nesting.
 *
 * It gates the START of each stream (the consumer's first pull), not the cadence of
 * items within a stream: once a stream is past the gate, its items flow at the
 * source's own pace.
 *
 * Because the wrapper is a lazy async generator, a stream's slot is reserved when it
 * STARTS (its first pull), not when the wrapped function is called. So a stream that
 * is created but never iterated reserves no slot and delays nothing. A concurrent
 * burst is still spaced one interval apart, in the order the streams start iterating.
 *
 * Inbound cancellation: if the caller passes a trailing `{ signal }`, an abort during
 * the gate wait abandons the wait and the stream rejects with the signal's reason (the
 * source is never started). An already-aborted signal rejects at the first pull,
 * before a slot is reserved.
 *
 * @template ArgTypes - The argument types of the streaming function
 * @template ItemType - The item type produced by the stream
 *
 * @param fn - The streaming function to rate limit
 * @param intervalMs - Minimum interval between stream starts, in ms (must be a positive integer)
 *
 * @returns A streaming function with the same signature that spaces out stream starts
 *
 * @throws Error at wrap time if `intervalMs` is not a positive integer
 *
 * @example
 * ```typescript
 * // Start at most one stream per second; items within a stream are not throttled.
 * const paced = withRateLimit(streamItems, 1000);
 * for await (const item of paced(query)) { handle(item); }
 * ```
 */
export function withRateLimit<ArgTypes extends any[], ItemType>(
  fn: (...args: ArgTypes) => AsyncIterable<ItemType>,
  intervalMs: number
): (...args: ArgTypes) => AsyncIterable<ItemType> {
  if (!Number.isInteger(intervalMs) || intervalMs < 1) {
    throw new Error('intervalMs must be a positive integer');
  }

  let nextAllowedTime = 0;
  return async function* (...args: ArgTypes): AsyncIterable<ItemType> {
    // Inbound cancellation: if the caller passed a trailing { signal }, an abort
    // abandons the gate wait and rejects with the signal's reason.
    const signal = extractSignal(args);
    signal?.throwIfAborted(); // already aborted → don't even reserve a slot

    const now = Date.now();
    // Reserve this stream's slot synchronously, before any await. Otherwise
    // concurrent starts all read the same `nextAllowedTime`, compute the same delay,
    // and begin together instead of being spaced one interval apart.
    const slot = Math.max(now, nextAllowedTime);
    nextAllowedTime = slot + intervalMs;
    const delay = slot - now;
    if (delay > 0) {
      // Wait out the interval, bailing (the source never starts) if the signal aborts.
      await abortableDelay(delay, signal);
    }
    // Delegate iteration to fn to produce all of its values.
    yield* fn(...args);
  };
}
