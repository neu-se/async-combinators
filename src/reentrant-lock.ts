import { AsyncLocalStorage } from "async_hooks";
import { awaitAbortable, promiseWithResolvers } from "./core/cancellation";

/**
 * A reentrant async lock that provides mutual exclusion between independent
 * async call chains, while allowing a chain that already holds the lock to
 * re-enter it without deadlocking.
 *
 * Reentrancy is scoped to the async call chain (tracked via `AsyncLocalStorage`)
 * and validated against an internal ownership token with reentrant depth, not a
 * caller-supplied identifier. This means:
 *
 * - Two **independent** operations contending for the same lock are serialized
 *   (mutual exclusion).
 * - A nested call made **within** an operation that already holds the lock runs
 *   immediately, without waiting (reentrancy) — so recursion and helper methods
 *   protected by the same lock do not deadlock.
 *
 * Waiters are served in FIFO order. The lock is held for the entire duration of
 * the function passed to {@link runExclusive} (or the lifetime of the stream passed
 * to {@link iterateExclusive}), including its awaits, so the critical section spans
 * the whole read-modify-write.
 *
 * Unlike {@link Lock}, this lock does **not** expose `acquire()`/`release()`.
 * Reentrancy works by marking the current async call chain as the holder via
 * `AsyncLocalStorage` before user code runs — only `runExclusive` and
 * `iterateExclusive` can do this, because they wrap the critical section. A bare `acquire()` has no
 * wrapper; the wrapper is what tracks the async call chain, which is required to
 * implement reentrancy safely. For manual acquire/release, use the non-reentrant
 * {@link Lock}.
 *
 * @example
 * ```typescript
 * const lock = new ReentrantAsyncLock();
 *
 * // Recursion is safe: the nested call re-enters the lock it already holds.
 * async function recurse(depth: number): Promise<void> {
 *   await lock.runExclusive(async () => {
 *     if (depth > 0) await recurse(depth - 1);
 *   });
 * }
 * ```
 *
 * @example
 * ```typescript
 * // Independent operations on the same lock are serialized.
 * const lock = new ReentrantAsyncLock();
 *
 * async function deposit(amount: number) {
 *   return lock.runExclusive(async () => {
 *     const balance = await readBalance();   // helper may itself use runExclusive
 *     await writeBalance(balance + amount);  // — reentrant, no deadlock
 *   });
 * }
 *
 * // These do not interleave; no lost updates.
 * await Promise.all([deposit(10), deposit(20)]);
 * ```
 *
 * @see {@link Lock} For a non-reentrant lock (simpler, but deadlocks on recursion).
 */
export class ReentrantLock {
  // Tracks the lock-owner token bound to the current async call chain.
  private readonly holder = new AsyncLocalStorage<symbol>();

  // Current owner token, or null when unlocked.
  private ownerToken: symbol | null = null;

  // Reentrant depth for ownerToken. Lock is free only when this reaches 0.
  private holdDepth = 0;

  // FIFO queue of resolvers for chains waiting to acquire the lock. Releasing
  // shifts the next one and hands that waiter a fresh owner token.
  private waiters: Array<(token: symbol) => void> = [];

  /** True iff the current async call chain already owns this lock. */
  private currentChainOwnsLock(): boolean {
    const token = this.holder.getStore();
    return token !== undefined && token === this.ownerToken;
  }

  /** Run `fn` with `token` bound as the current chain's lock-owner marker. */
  private runAsHolder<T>(token: symbol, fn: () => Promise<T>): Promise<T> {
    // `run` binds the holder marker for the entire dynamic extent of fn —
    // synchronously and across all of fn's awaits — so currentChainOwnsLock()
    // returns true anywhere inside fn. That is what lets a nested runExclusive
    // re-enter instead of deadlocking. The binding is dropped when fn settles.
    return this.holder.run(token, fn);
  }

  /**
   * Wait (abortably, in FIFO order) until the lock is free, then take it.
   * Reentrant callers (same chain already owns the lock) get the existing token and
   * increment depth. An abort while queued splices this waiter out and rejects, so the
   * lock is never taken.
   */
  private async acquire(signal?: AbortSignal): Promise<symbol> {
    // Reentrant: this chain already owns the lock, so just increase depth.
    if (this.currentChainOwnsLock()) {
      this.holdDepth++;
      return this.ownerToken!;
    }

    // If held by another chain, wait to be handed ownership directly (see release());
    // otherwise take it now.
    if (this.ownerToken !== null) {
      const { promise: waiter, resolve, reject } = promiseWithResolvers<symbol>();
      this.waiters.push(resolve);
      return await awaitAbortable(waiter, signal, () => {
        const i = this.waiters.indexOf(resolve);
        if (i !== -1) { this.waiters.splice(i, 1); reject(signal!.reason); }
      });
    }

    const token = Symbol('reentrant-lock-owner');
    this.ownerToken = token;
    this.holdDepth = 1;
    return token;
  }

  /**
   * Release the lock. Hands off directly to the next FIFO waiter if there is one —
    * transferring ownership to a fresh token with depth 1 so no other chain can
    * interleave between this release and the waiter resuming — otherwise marks the
    * lock free.
   */
  private release(token: symbol): void {
    // Defensive guard for unexpected internal misuse.
    if (this.ownerToken !== token) return;

    this.holdDepth--;
    if (this.holdDepth > 0) return;

    const next = this.waiters.shift();
    if (!next) {
      this.ownerToken = null;
      this.holdDepth = 0;
      return;
    }

    const nextToken = Symbol('reentrant-lock-owner');
    this.ownerToken = nextToken;
    this.holdDepth = 1;
    next(nextToken);
  }

  /**
   * Runs `fn` while holding the lock and returns its result. If the current
   * async call chain already holds the lock, `fn` runs immediately (reentrant);
   * otherwise this waits, in FIFO order, until the lock is free.
   *
   * The lock is always released when `fn` settles, including when it throws.
   *
   * @param fn - The critical section to run under the lock.
   * @param signal - Optional `AbortSignal`. If it aborts while waiting for the
   *   lock, `fn` never runs; if it aborts after acquiring but before `fn` starts,
   *   the lock is released and the reason is thrown. (It can't stop `fn` mid-flight
   *   unless `fn` itself honors the signal.) The reentrant fast-path never waits,
   *   so it's only subject to the entry poll.
   * @returns The value returned by `fn`.
   * @throws Re-throws any error thrown by `fn` (the lock is still released), or
   *   the signal's reason on cancellation.
   */
  public async runExclusive<T>(fn: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    signal?.throwIfAborted(); // already aborted → don't run, don't queue

    const token = await this.acquire(signal);
    try {
      signal?.throwIfAborted(); // aborted during hand-off → release without running fn
      // Run fn with the async chain rooted here marked as the holder, so nested
      // runExclusive calls on this chain re-enter instead of deadlocking.
      return await this.runAsHolder(token, fn);
    } finally {
      this.release(token);
    }
  }

  /**
   * The **streaming** analogue of {@link runExclusive}: run a streaming function while
   * holding the lock for the *entire lifetime* of the returned stream — acquired at the
   * first pull, released when the stream ends (completion, error, or the consumer
   * abandoning it). Independent streams on the same lock are serialized; a stream started
   * from within an operation that already holds the lock (recursion, or a nested
   * lock-guarded stream/call) re-enters without deadlocking.
   *
   * Reentrancy across a stream's `yield`s can't be expressed with `yield*`/`for await`,
   * because an async generator's `AsyncLocalStorage` context is fixed by the `.next()`
   * call site per resumption (not captured at creation). The source is therefore iterated manually
   * and each pull is wrapped in {@link runAsHolder}: the holder marker is bound only while
   * the source produces an item (so a nested same-lock call re-enters), never across the
   * `yield` to the consumer (so consumer think-time is outside the critical section).
   *
   * @param source - Factory for the stream to run under the lock (called at the first pull).
   * @param signal - Optional `AbortSignal`. If it aborts while waiting to acquire, the
   *   stream never starts and the reason is thrown; an already-aborted signal throws at the
   *   first pull without acquiring. It can't interrupt the source mid-stream unless the
   *   source itself honors it; to abandon a waiting stream, abort (a bare `break` only takes
   *   effect at the next `yield`).
   */
  public async *iterateExclusive<T>(
    source: () => AsyncIterable<T>,
    signal?: AbortSignal
  ): AsyncIterable<T> {
    signal?.throwIfAborted(); // already aborted → don't start, don't queue

    // Always acquire a real ownership lease (reentrant calls increment depth and receive
    // the existing token) so ownership remains correct even if nested work outlives an
    // outer frame.
    const token = await this.acquire(signal);
    try {
      signal?.throwIfAborted(); // aborted during hand-off → release without starting

      // Iterate the source by hand rather than `yield*` / `for await`. An async generator's
      // AsyncLocalStorage context is set by the `.next()` *call site* on each resumption (not
      // captured at creation, not sticky across yields), so the source only sees the holder
      // marker if *we* invoke `it.next()` from inside `holder.run(...)`. `yield*` / `for await`
      // make those `.next()` calls themselves, from this wrapper's holder-less context, and
      // give no seam to wrap each one — which is why we can't use them and pull by hand instead.
      const it = source()[Symbol.asyncIterator]();
      try {
        while (true) {
          // runAsHolder binds the holder only across this one pull — not across the `yield`
          // below — so a nested same-lock call made while the source produces this item
          // re-enters, while the consumer's think-time between pulls stays outside the
          // critical section.
          const result = await this.runAsHolder(token, () => it.next());
          if (result.done) return;
          yield result.value;
        }
      } finally {
        // Close the source, also as holder, so its own cleanup (finally) may re-enter the
        // lock we still hold rather than deadlock on it.
        await this.runAsHolder(token, () => Promise.resolve(it.return?.()));
      }
    } finally {
      this.release(token);
    }
  }

  /**
   * Whether the lock is currently held by *any* call chain — not necessarily
   * the caller's. Mirrors {@link AsyncLock.isLocked}; does not block. (For the
   * "does the current chain hold it?" question, reentrancy already handles that
   * automatically, so it isn't exposed.)
   */
  public isLocked(): boolean {
    return this.ownerToken !== null;
  }
}
