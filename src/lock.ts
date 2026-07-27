import { awaitAbortable, promiseWithResolvers } from "./core/cancellation";

/**
 * A non-reentrant async lock for mutual exclusion.
 *
 * Ensures only one async operation can hold the lock at a time. If the same
 * caller tries to acquire the lock again before releasing it, it will deadlock
 * waiting for itself (use {@link ReentrantAsyncLock} if you need recursion).
 *
 * Waiters are served in FIFO (first-in-first-out) order.
 *
 * Prefer {@link runExclusive}, which acquires and releases automatically. Use
 * {@link acquire} directly only when the critical section can't be expressed as
 * a single callback (e.g. acquiring in one place and releasing in another);
 * `acquire` returns a one-shot release function so the lock can't be released by
 * code that doesn't hold it, and a double release is a harmless no-op.
 *
 * @example
 * ```typescript
 * const lock = new Lock();
 *
 * await lock.runExclusive(async () => {
 *   // critical section — the lock is released automatically, even on throw
 * });
 * ```
 *
 * @example
 * ```typescript
 * // Manual acquire/release, when a single callback doesn't fit:
 * const release = await lock.acquire();
 * try {
 *   // critical section
 * } finally {
 *   release();
 * }
 * ```
 */
export class Lock {
  private locked = false;
  private waitlist: (() => void)[] = [];

  /**
   * Acquire the lock, resolving once it is held. Returns a one-shot release
   * function: calling it frees the lock (handing off to the next waiter, if
   * any); calling it more than once is a no-op.
   *
   * @param signal - Optional `AbortSignal`. If it aborts while this call is still
   *   waiting for the lock, the waiter is removed from the queue and `acquire`
   *   rejects with `signal.reason` (a holder that already acquired is unaffected).
   * @returns A function that releases this acquisition of the lock.
   * @throws The signal's reason if aborted before or while waiting.
   */
  public async acquire(signal?: AbortSignal): Promise<() => void> {
    signal?.throwIfAborted(); // already aborted → don't queue

    if (this.locked) {
      // Wait to be handed the lock — abortably. An abort while queued removes this
      // waiter from the waitlist and rejects, so we never acquire.
      const { promise: waiter, resolve, reject } = promiseWithResolvers<void>();
      this.waitlist.push(resolve);

      await awaitAbortable(waiter, signal, () => {
        const i = this.waitlist.indexOf(resolve);
        if (i !== -1) { this.waitlist.splice(i, 1); reject(signal!.reason); }
      });
    } else {
      this.locked = true;
    }

    let released = false;
    return () => {
      // One-shot: a second call can't release the lock currently held by another
      // caller. This is what makes duplicate releases harmless.
      if (released) return;
      released = true;

      const next = this.waitlist.shift();
      if (next) {
        // Direct hand-off: keep `locked === true` so no other acquirer can interleave
        // between this release and the next waiter resuming.
        next();
      } else {
        this.locked = false;
      }
    };
  }

  /**
   * Whether the lock is currently held (by an active holder or being handed
   * directly to a waiter). Useful for assertions and for checking whether a
   * resource is busy without blocking on it.
   */
  public isLocked(): boolean {
    return this.locked;
  }

  /**
   * Run `fn` while holding the lock and return its result. The lock is always
   * released when `fn` settles, including when it throws.
   *
   * @param fn - The critical section to run under the lock.
   * @param signal - Optional `AbortSignal`. If it aborts while waiting for the
   *   lock, `fn` never runs; if it aborts after acquiring but before `fn` starts,
   *   the lock is released and the reason is thrown. (It can't stop `fn` mid-flight
   *   unless `fn` itself honors the signal.)
   * @returns The value returned by `fn`.
   * @throws Re-throws any error thrown by `fn` (the lock is still released), or
   *   the signal's reason on cancellation.
   */
  public async runExclusive<T>(fn: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    const release = await this.acquire(signal);
    try {
      signal?.throwIfAborted(); // aborted during hand-off → release without running fn
      return await fn();
    } finally {
      release();
    }
  }
}
