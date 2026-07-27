import { extractSignal } from '../core/cancellation';
import { Lock } from '../lock';

/**
 * Protect a **streaming** function with a {@link Lock}. This is the streaming
 * analogue of the promise-family `withLock`, preserving the signature
 * `(...args) => AsyncIterable<ItemType>` so it composes with the other stream
 * combinators by nesting.
 *
 * The lock is held for the *entire lifetime* of each returned stream: acquired
 * at the first pull, released when the stream ends (completion, error, or the
 * consumer abandoning it). Independent streams contending for the same lock are
 * serialized in FIFO order.
 *
 * Because the wrapper is a lazy async generator, a stream that is created but
 * never iterated acquires nothing; and because the consumer controls the stream's
 * lifetime, a slow or stalled consumer holds the lock for as long as it dawdles.
 * An inbound `{ signal }` abandons a stream still waiting to acquire (the source
 * never starts).
 *
 * @template ArgTypes - The argument types of the streaming function
 * @template ItemType - The item type produced by the stream
 *
 * @param fn - The streaming function to protect.
 * @param lock - The {@link Lock} providing mutual exclusion. If omitted, a new
 *   lock is created, which is enough to serialize this function's own invocations.
 *
 * @returns A streaming function with the same signature that runs `fn` under the lock.
 */
export function withLock<ArgTypes extends any[], ItemType>(
  fn: (...args: ArgTypes) => AsyncIterable<ItemType>,
  lock: Lock = new Lock()
): (...args: ArgTypes) => AsyncIterable<ItemType> {
  return async function* (...args: ArgTypes): AsyncIterable<ItemType> {
    const signal = extractSignal(args);
    const release = await lock.acquire(signal);
    try {
      yield* fn(...args);
    } finally {
      release();
    }
  };
}
