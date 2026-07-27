/**
 * Best-effort, fire-and-forget close of an async iterator. Requests its `return()`
 * WITHOUT awaiting, then swallows any rejection.
 *
 * Use this when abandoning an iterator on an already-failing path (e.g. a timeout or
 * an abort) where the source may be stuck: a stuck source's pending pull may never
 * settle, which leaves its queued `return()` unable to run, so awaiting the cleanup
 * would hang the caller. On a clean path (normal completion or the consumer
 * abandoning the stream), prefer `await it.return?.()` for deterministic cleanup.
 *
 * The rejection is swallowed because nothing awaits this promise; without it, a
 * `return()` that rejects would surface as an unhandled rejection.
 */
export function closeQuietly(it: { return?: (...args: any[]) => unknown }): void {
  void Promise.resolve(it.return?.()).catch(() => {});
}
