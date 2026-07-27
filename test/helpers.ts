/** Yield exactly one microtask turn (no timers). */
export async function pauseMicrotask(): Promise<void> {
  await Promise.resolve();
}

/** A manually-resolvable promise, for coordinating two async chains in a test. */
export function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => { resolve = r; });
  return { promise, resolve };
}

/**
 * A promise that never settles — for simulating a source or operation that hangs
 * (e.g. a stalled pull), without leaving a real timer behind.
 */
export function stall(): Promise<never> {
  return new Promise<never>(() => {});
}

/**
 * A genuine AbortError — a `DOMException` named `'AbortError'`, exactly what an
 * aborted `AbortSignal` (and `fetch`) produces. Useful for feeding a real
 * cancellation into code that detects it by name.
 */
export function abortError(): unknown {
  const controller = new AbortController();
  controller.abort();
  return controller.signal.reason;
}

/**
 * An `AbortSignal` that is already aborted before the call under test runs —
 * for exercising the "already cancelled at entry" path.
 */
export function abortedSignal(): AbortSignal {
  const controller = new AbortController();
  controller.abort();
  return controller.signal;
}

/**
 * Assert that `promise` rejects with a cancellation (an error named
 * `'AbortError'`), the contract every abortable combinator honors.
 */
export async function expectAbortError(promise: Promise<unknown>): Promise<void> {
  await expect(promise).rejects.toHaveProperty('name', 'AbortError');
}
