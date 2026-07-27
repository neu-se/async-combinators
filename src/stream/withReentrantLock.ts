import { ReentrantLock } from '../reentrant-lock';
import { extractSignal } from '../core/cancellation';

/**
 * Protect a **streaming** function with a {@link ReentrantLock}. This is the
 * streaming analogue of the promise-family `withReentrantLock`, preserving the signature
 * `(...args) => AsyncIterable<ItemType>` so it composes with the other stream combinators
 * by nesting.
 *
 * The lock is held for the *entire lifetime* of each returned stream: acquired at the
 * first pull, released when the stream ends (completion, error, or the consumer abandoning
 * it). Independent streams contending for the same lock are serialized, but a stream
 * started from within an operation that already holds the lock — recursion, or a nested
 * lock-guarded stream/call — re-enters without deadlocking. Reentrancy is tracked
 * automatically per async call chain; there is no owner id to supply.
 *
 * A thin wrapper over {@link ReentrantLock.iterateExclusive}, mirroring the promise
 * `withReentrantLock`. Use `lock.iterateExclusive(...)` directly if you prefer.
 *
 * Because the wrapper is a lazy async generator, a stream that is created but never
 * iterated acquires nothing; and because the consumer controls the stream's lifetime, a slow
 * or stalled consumer holds the lock for as long as it dawdles. An inbound `{ signal }`
 * abandons a stream still waiting to acquire (the source never starts); to promptly abandon
 * a waiting stream, abort the signal — a bare `break` only takes effect at the next `yield`.
 *
 * @template ArgTypes - The argument types of the streaming function
 * @template ItemType - The item type produced by the stream
 *
 * @param fn - The streaming function to protect.
 * @param lock - The {@link ReentrantLock} providing mutual exclusion. If omitted, a new
 *   one is created — enough to serialize this function's own invocations while letting it
 *   recurse. Pass a shared lock to coordinate several functions under one reentrant lock.
 *
 * @returns A streaming function with the same signature that runs `fn` under the lock.
 *
 * @see {@link withLock} For non-reentrant lock behavior (simpler, but deadlocks on recursion).
 */
export function withReentrantLock<ArgTypes extends any[], ItemType>(
  fn: (...args: ArgTypes) => AsyncIterable<ItemType>,
  lock: ReentrantLock = new ReentrantLock()
): (...args: ArgTypes) => AsyncIterable<ItemType> {
  return (...args: ArgTypes): AsyncIterable<ItemType> =>
    // Forward the caller's trailing { signal } so an abort abandons the lock wait.
    lock.iterateExclusive(() => fn(...args), extractSignal(args));
}
