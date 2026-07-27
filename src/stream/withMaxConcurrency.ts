import { extractSignal } from '../core/cancellation';
import { Semaphore } from '../semaphore';

/**
 * Limit the number of concurrently active **streams**. This is the streaming
 * analogue of the promise-family `withMaxConcurrency`, preserving the signature
 * `(...args) => AsyncIterable<ItemType>` so it composes with the other stream
 * combinators by nesting.
 *
 * At most `maxConcurrent` streams may be *actively producing* at once. A stream
 * holds a slot for its whole lifetime: it acquires a slot when it STARTS (the
 * consumer's first pull), holds it while producing, and releases it when it ends,
 * by any means — normal completion, an error, or the consumer abandoning it. Extra
 * streams wait in FIFO order until a slot frees.
 *
 * Two consequences of holding the slot for the stream's lifetime are worth noting:
 * - Because the wrapper is a lazy async generator, a stream that is created but
 *   never iterated acquires no slot.
 * - Because the consumer drives the stream's lifetime, a slow or stalled consumer
 *   holds its slot for as long as it dawdles; `maxConcurrent` slow consumers can
 *   occupy every slot indefinitely.
 *
 * Inbound cancellation: a trailing `{ signal }` drops the stream while it is still
 * WAITING for a slot (the source never starts, no slot is consumed). A stream that
 * has already acquired a slot can only be stopped by `fn` honoring the signal, or
 * by the consumer abandoning it. To promptly abandon a stream that may be waiting
 * in the queue, abort the signal: a bare `break` cannot interrupt the queue wait
 * (it only takes effect at the next `yield`).
 *
 * @template ArgTypes - The argument types of the streaming function
 * @template ItemType - The item type produced by the stream
 *
 * @param fn - The streaming function to limit
 * @param maxConcurrent - Maximum number of concurrently active streams (must be a positive integer)
 *
 * @returns A streaming function with the same signature that bounds concurrent streams
 *
 * @throws Error at wrap time if `maxConcurrent` is not a positive integer
 */
export function withMaxConcurrency<ArgTypes extends any[], ItemType>(
  fn: (...args: ArgTypes) => AsyncIterable<ItemType>,
  maxConcurrent: number
): (...args: ArgTypes) => AsyncIterable<ItemType> {
  if (!Number.isInteger(maxConcurrent) || maxConcurrent < 1) {
    throw new Error('maxConcurrent must be a positive integer');
  }

  const sem = new Semaphore(maxConcurrent);
  return async function* (...args: ArgTypes): AsyncIterable<ItemType> {
    const signal = extractSignal(args);
    // Acquire a slot before starting (abortable; an already-aborted signal throws).
    const release = await sem.acquire(signal);
    try {
      // Hold the slot for the whole stream, delegating iteration to fn.
      yield* fn(...args);
    } finally {
      // Release on every exit: normal completion, source error, or the consumer
      // abandoning the stream (which unwinds through this finally).
      release();
    }
  };
}
