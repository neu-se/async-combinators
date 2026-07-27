/**
 * Error thrown by {@link withTimeout} when the deadline is exceeded.
 *
 * A timeout is a *failure*, not a cancellation: the resilience wrappers treat it
 * as retryable (e.g. `withRetry` will retry a timed-out call). Detected by name
 * (`'TimeoutError'`), matching the web-standard error that `AbortSignal.timeout()`
 * produces.
 */
export class TimeoutError extends Error {
  constructor(message = 'Operation timed out') {
    super(message);
    this.name = 'TimeoutError';
  }
}

/**
 * Whether an error represents a *cancellation* — an aborted `AbortSignal`, or any
 * error a function throws to signal it was cancelled.
 *
 * Detected by **name** (`err.name === 'AbortError'`), which is cross-realm-safe
 * and matches the web standard: `AbortController.abort()` rejects with an
 * `AbortError`, and `fetch`/Node abort the same way. Deliberately distinct from a
 * timeout ({@link TimeoutError}), which is a *retryable failure*, not a cancellation.
 *
 * The resilience wrappers use this to skip their policy on a cancelled call:
 * `withRetry` doesn't retry it, `withFallback` doesn't fall back, and
 * `withCache`/`withRecordReplay` don't cache/record it.
 */
export function isCancellation(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    (error as { name?: unknown }).name === 'AbortError'
  );
}

/**
 * Extract the inbound cancellation signal from a wrapped call's arguments.
 *
 * Convention: the signal is carried in a trailing options object under `signal`
 * (`wrapped(...args, { signal })`), matching `fetch`. Returns undefined
 * when the last argument isn't such an object — so a fn that isn't signal-aware
 * simply gets no signal.
 */
export function extractSignal(args: readonly unknown[]): AbortSignal | undefined {
  // Optional chaining short-circuits on null/undefined, and reading a property
  // off a primitive yields undefined rather than throwing — so no last-arg type
  // guard is needed before the `instanceof` check.
  const signal = (args[args.length - 1] as { signal?: unknown } | undefined)?.signal;
  return signal instanceof AbortSignal ? signal : undefined;
}

/**
 * Return a copy of `args` with the inbound `signal` removed, for use in cache /
 * recording keys — the signal is a transient per-call concern and must never be
 * part of the logical key.
 *
 * Only a trailing options bag that actually carries an `AbortSignal` is touched
 * (same detection as {@link extractSignal}); a domain object, or a `signal` field
 * that isn't an `AbortSignal`, is left alone. If stripping the signal leaves the
 * options bag empty, the bag is dropped entirely, so `f(x, { signal })` keys the
 * same as `f(x)`.
 */
export function stripSignal(args: readonly unknown[]): unknown[] {
  if (extractSignal(args) === undefined) return args.slice();
  const { signal, ...rest } = args[args.length - 1] as { signal?: unknown } & Record<string, unknown>;
  const head = args.slice(0, -1);
  return Object.keys(rest).length === 0 ? head : [...head, rest];
}

/**
 * The pieces returned by {@link rejectOnAbort}: a promise that rejects when the
 * signal aborts, and a `cleanup` to detach its listener once the race settles.
 */
type AbortRejection = { promise: Promise<never>; cleanup: () => void };

/**
 * A promise that rejects with `signal.reason` when the signal aborts, paired
 * with a `cleanup` that detaches the listener. Meant for `Promise.race` against
 * the real work; **always call `cleanup()` in a `finally`** once the race
 * settles, or the listener leaks (see the note on long-lived signals).
 *
 * When there's no signal, returns a never-settling promise and a no-op cleanup,
 * so callers can always `race` against `.promise` without branching.
 */
export function rejectOnAbort(signal: AbortSignal | undefined): AbortRejection {
  // A promise that can never fulfill (the resolve slot is ignored) — it only ever
  // stays pending or rejects, which is why its type is `Promise<never>`. We hold
  // `reject` externally: with no signal nothing calls it, so the promise stays
  // pending forever (the never-firing abort arm); with a signal, the abort
  // listener rejects it with the signal's reason.
  let reject!: (reason: unknown) => void;
  const promise = new Promise<never>((_, rej) => { reject = rej; });

  let cleanup = () => {};
  if (signal) {
    const onAbort = () => reject(signal.reason);
    signal.addEventListener('abort', onAbort, { once: true });
    cleanup = () => signal.removeEventListener('abort', onAbort);
  }
  return { promise, cleanup };
}

/**
 * A promise that rejects with `error` after `ms`, paired with a `cleanup` that
 * clears the timer. The timer counterpart of {@link rejectOnAbort}: meant for
 * `Promise.race` against real work, so **always call `cleanup()` in a `finally`**
 * once the race settles, or the still-pending timer keeps the Node event loop alive
 * until `ms` elapses (delaying process exit) even after the work already won.
 */
export function rejectAfter(ms: number, error: unknown): { promise: Promise<never>; cleanup: () => void } {
  let timer: NodeJS.Timeout;
  const promise = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(error), ms);
  });
  return { promise, cleanup: () => clearTimeout(timer) };
}

/**
 * A decorator for a promise. It doesn't settle `pending` itself: it registers the
 * provided abort listener; if `signal` aborts, the listener fires (e.g. running
 * the caller's dequeue-and-reject); and whenever `pending` settles — by any means —
 * it removes the listener. So a long-lived shared signal never accumulates
 * listeners. No signal → plain await.
 *
 * The caller's `onAbort` is what removes it from whatever queue it is waiting in and
 * rejects `pending` with `signal.reason`.
 *
 * This is the hand-off counterpart to {@link rejectOnAbort}: use `rejectOnAbort`
 * to `Promise.race` against work you don't control; use this when you settle the
 * promise yourself by handing off from a FIFO queue (locks, concurrency slots).
 */
export async function awaitAbortable<T>(
  pending: Promise<T>,
  signal: AbortSignal | undefined,
  onAbort: () => void,
): Promise<T> {
  if (!signal) return pending;
  signal.addEventListener('abort', onAbort, { once: true });
  try {
    return await pending;
  } finally {
    signal.removeEventListener('abort', onAbort);
  }
}

/**
 * Create a promise together with its `resolve`/`reject`, so they can be called
 * from outside the executor. The queue-based combinators use this to hold a
 * pending promise in the queue and settle it later on hand-off (or abort).
 *
 * A stand-in for the standard `Promise.withResolvers()`, whose shape this
 * mirrors. TODO: drop this and call `Promise.withResolvers()` directly once we
 * require Node >= 22.
 */
export function promiseWithResolvers<T>(): {
  promise: Promise<T>;
  resolve: (value: T | PromiseLike<T>) => void;
  reject: (reason?: unknown) => void;
} {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

/**
 * Sleep for `ms`, resolving when it elapses — but reject early with
 * `signal.reason` if the signal aborts first. With no signal it's a plain sleep.
 */
export function abortableDelay(ms: number, signal: AbortSignal | undefined): Promise<void> {
  if (signal?.aborted) return Promise.reject(signal.reason);

  let timer: ReturnType<typeof setTimeout>;
  const sleep = new Promise<void>(resolve => { timer = setTimeout(resolve, ms); });
  const abort = rejectOnAbort(signal);

  return Promise.race([sleep, abort.promise]).finally(() => {
    clearTimeout(timer);
    abort.cleanup();
  });
}
