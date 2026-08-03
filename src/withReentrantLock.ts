import { ReentrantLock } from "./reentrant-lock";
import { extractSignal } from "./core/cancellation";

/**
 * Protects an async function with a {@link ReentrantLock}.
 *
 * The returned function runs the wrapped function inside the lock's critical
 * section. Independent calls contending for the same lock are serialized, but a
 * call made from within another call that already holds the lock (recursion, or
 * a helper protected by the same lock) re-enters without deadlocking. Reentrancy
 * is tracked automatically per async call chain — there is no owner id to supply.
 *
 * This is a thin convenience wrapper over {@link ReentrantLock.runExclusive},
 * kept for consistency with the other `with*` higher-order functions. Use
 * `lock.runExclusive(...)` directly if you prefer.
 *
 * @template ArgTypes - The argument types for the function being wrapped.
 * @template RtrnType - The return type of the function being wrapped.
 *
 * @param fn - The async function to wrap with reentrant lock protection.
 * @param lock - The {@link ReentrantLock} instance providing mutual exclusion.
 *   If not provided, a new `ReentrantLock` is created automatically — enough
 *   to serialize this function's own invocations while letting it recurse. Pass a
 *   shared lock to coordinate several functions under one reentrant lock.
 *
 * @returns A function with the same signature that runs `fn` under the lock.
 *
 * @example
 * ```typescript
 * import { withReentrantLock, ReentrantLock } from 'async-combinators';
 *
 * const lock = new ReentrantLock();
 *
 * let recurse = async (n: number): Promise<number> => {
 *   if (n <= 0) return 0;
 *   return 1 + await recurse(n - 1); // reentrant call — no deadlock
 * };
 * recurse = withReentrantLock(recurse, lock);
 *
 * await recurse(3); // 3
 * ```
 *
 * @throws Re-throws any error thrown by the wrapped function (the lock is still released).
 *
 * @see {@link withLock} For non-reentrant lock behavior (simpler, but deadlocks on recursion).
 */
export function withReentrantLock<ArgTypes extends any[], RtrnType>(
  fn: (...args: ArgTypes) => Promise<RtrnType>,
  lock: ReentrantLock = new ReentrantLock()
): (...args: ArgTypes) => Promise<RtrnType> {
  return (...args: ArgTypes): Promise<RtrnType> =>
    // Forward the caller's trailing { signal } so an abort abandons the lock wait.
    lock.runExclusive(() => fn(...args), extractSignal(args));
}
