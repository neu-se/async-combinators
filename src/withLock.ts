import { Lock } from "./lock";
import { extractSignal } from "./core/cancellation";

/**
 * Protects an async function with a {@link Lock}, guaranteeing that it will not be interleaved by functions or code regions protected by the same lock.
 *
 * The function acquires the lock before execution and releases it after completion,
 * ensuring atomic execution relative to other code protected by the same lock. This is useful for protecting
 * critical sections, coordinating access to shared resources, or preventing race conditions.
 *
 * The underlying lock is non-reentrant: if the wrapped function (or anything it calls) tries
 * to acquire the same lock again, it will deadlock. Use {@link withReentrantLock} if the
 * wrapped function may call itself or other functions protected by the same lock.
 * 
 * @template ArgTypes - The argument types for the function being wrapped
 * @template RtrnType - The return type of the function being wrapped
 * 
 * @param fn - The async function to wrap with lock protection
 * @param lock - The AsyncLock instance to use for mutual exclusion. If not provided, a new AsyncLock is created automatically.
 * 
 * @returns A new function with the same signature that executes atomically relative to other code protected by the same lock
 * 
 * @example
 * ```typescript
 * // Simple case: prevent function from running concurrently with itself
 * import { withLock } from 'async-combinators';
 * 
 * let counter = 0;
 * 
 * async function incrementCounter(amount: number) {
 *   const current = counter;
 *   await new Promise(resolve => setTimeout(resolve, 10)); // Simulate async work
 *   counter = current + amount;
 *   return counter;
 * }
 * 
 * // Uses a default lock internally - each call to withLock creates its own lock
 * const protectedIncrement = withLock(incrementCounter);
 * 
 * // These will execute sequentially due to the internal lock
 * const results = await Promise.all([
 *   protectedIncrement(1),
 *   protectedIncrement(2),
 *   protectedIncrement(3)
 * ]);
 * console.log(counter); // 6 (correct result due to atomic execution)
 * ```
 * 
 * @example
 * ```typescript
 * // Advanced case: multiple functions sharing the same lock
 * import { withLock, AsyncLock } from 'async-combinators';
 * 
 * const sharedLock = new AsyncLock();
 * let sharedResource = 0;
 * 
 * const incrementFn = withLock(async (amount: number) => {
 *   sharedResource += amount;
 *   return sharedResource;
 * }, sharedLock);
 * 
 * const decrementFn = withLock(async (amount: number) => {
 *   sharedResource -= amount;
 *   return sharedResource;
 * }, sharedLock);
 * 
 * // Both functions are protected by the same lock
 * await Promise.all([incrementFn(5), decrementFn(2)]);
 * ```
 * 
 * @throws Any error thrown by the wrapped function (lock is always released)
 */

export function withLock<ArgTypes extends any[], RtrnType>(
    fn: (...args: ArgTypes) => Promise<RtrnType>,
    lock: Lock = new Lock()
): (...args: ArgTypes) => Promise<RtrnType> {
  return (...args: ArgTypes): Promise<RtrnType> =>
    // Forward the caller's trailing { signal } so an abort abandons the lock wait.
    lock.runExclusive(() => fn(...args), extractSignal(args));
}