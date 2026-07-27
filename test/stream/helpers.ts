/** Test helpers shared across the stream-combinator test suites. */

/** Drain an async iterable into an array. */
export async function collect<T>(it: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const x of it) out.push(x);
  return out;
}

/** Drain into `target`; unlike collect(), the partial result survives a throw. */
export async function collectInto<T>(target: T[], it: AsyncIterable<T>): Promise<void> {
  for await (const x of it) target.push(x);
}

/** A stream that yields `items` then completes. */
export async function* arrayStream<T>(items: T[]): AsyncIterable<T> {
  for (const x of items) yield x;
}

/**
 * A stream that yields `itemsBefore` then throws `error`. With no `itemsBefore`
 * it is a *pre-first-item* failure; with some, a *mid-stream* failure.
 */
export async function* failingStream<T>(error: unknown, itemsBefore: T[] = []): AsyncIterable<T> {
  for (const x of itemsBefore) yield x;
  throw error;
}

/** A fresh jest mock typed as a streaming function (string items by default). */
export function streamFn<T = string>() {
  return jest.fn<AsyncIterable<T>, unknown[]>();
}
