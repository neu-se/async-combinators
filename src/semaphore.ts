import { awaitAbortable, promiseWithResolvers } from "./core/cancellation";

/**
 * An async counting semaphore: at most `permits` holders may hold it at once.
 *
 * The generalization of {@link AsyncLock} (a lock is an `AsyncSemaphore(1)`). When
 * all permits are held, further acquirers wait in FIFO order until a holder
 * releases. Useful for bounding concurrency: a connection pool, a limit on
 * in-flight requests, or the {@link "./stream/withMaxConcurrency"} combinator.
 *
 * Prefer {@link runExclusive}, which acquires and releases automatically. Use
 * {@link acquire} directly only when the critical section can't be expressed as a
 * single callback (e.g. acquiring in one place and releasing in another, or
 * holding a permit for the lifetime of a stream); `acquire` returns a one-shot
 * release function so a permit can't be released by code that doesn't hold it, and
 * a double release is a harmless no-op.
 *
 * @example
 * ```typescript
 * const sem = new AsyncSemaphore(3); // at most 3 concurrent
 *
 * await sem.runExclusive(async () => {
 *   // at most three of these run at once; the permit is released even on throw
 * });
 * ```
 *
 * @example
 * ```typescript
 * // Manual acquire/release, when a single callback doesn't fit:
 * const release = await sem.acquire();
 * try {
 *   // hold the permit
 * } finally {
 *   release();
 * }
 * ```
 */
export class Semaphore {
  private readonly permits: number;
  private activeCount = 0;
  private waitlist: (() => void)[] = [];

  /**
   * @param permits - The maximum number of holders allowed at once (must be a positive integer).
   * @throws Error if `permits` is not a positive integer.
   */
  constructor(permits: number) {
    if (!Number.isInteger(permits) || permits < 1) {
      throw new Error('permits must be a positive integer');
    }
    this.permits = permits;
  }

  /**
   * Acquire a permit, resolving once it is held. Returns a one-shot release
   * function: calling it frees the permit (handing off to the next waiter, if
   * any); calling it more than once is a no-op.
   *
   * @param signal - Optional `AbortSignal`. If it aborts while this call is still
   *   waiting for a permit, the waiter is removed from the queue and `acquire`
   *   rejects with `signal.reason` (a holder that already acquired is unaffected).
   * @returns A function that releases this acquisition.
   * @throws The signal's reason if aborted before or while waiting.
   */
  public async acquire(signal?: AbortSignal): Promise<() => void> {
    signal?.throwIfAborted(); // already aborted → don't queue

    if (this.activeCount < this.permits) {
      // Reserve a permit synchronously, before any await, so concurrent acquirers
      // can't both read the same count and overshoot `permits`.
      this.activeCount++;
    } else {
      // Wait to be handed a permit — abortably. An abort while queued removes this
      // waiter from the waitlist and rejects, so we never acquire.
      const { promise: waiter, resolve, reject } = promiseWithResolvers<void>();
      this.waitlist.push(resolve);

      await awaitAbortable(waiter, signal, () => {
        const i = this.waitlist.indexOf(resolve);
        if (i !== -1) { this.waitlist.splice(i, 1); reject(signal!.reason); }
      });
      // A permit was handed to us; `activeCount` already accounts for it (the
      // releasing holder transferred its permit rather than decrementing).
    }

    let released = false;
    return () => {
      // One-shot: a second call can't release a permit currently held by another
      // caller. This is what makes duplicate releases harmless.
      if (released) return;
      released = true;

      const next = this.waitlist.shift();
      if (next) {
        // Direct hand-off: keep `activeCount` unchanged so no other acquirer can
        // interleave between this release and the next waiter resuming.
        next();
      } else {
        this.activeCount--;
      }
    };
  }

  /**
   * How many permits are currently free (0 means fully subscribed). Useful for
   * assertions and for checking capacity without blocking.
   */
  public available(): number {
    return this.permits - this.activeCount;
  }

  /**
   * Run `fn` while holding a permit and return its result. The permit is always
   * released when `fn` settles, including when it throws.
   *
   * @param fn - The work to run while holding a permit.
   * @param signal - Optional `AbortSignal`. If it aborts while waiting for a permit,
   *   `fn` never runs; if it aborts after acquiring but before `fn` starts, the
   *   permit is released and the reason is thrown. (It can't stop `fn` mid-flight
   *   unless `fn` itself honors the signal.)
   * @returns The value returned by `fn`.
   * @throws Re-throws any error thrown by `fn` (the permit is still released), or
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
