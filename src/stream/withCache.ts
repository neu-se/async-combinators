import { isCancellation, extractSignal, stripSignal, rejectOnAbort } from '../core/cancellation';
import { closeQuietly } from '../core/iterator';

/**
 * One cached stream — the shared state for a single cache key. A key represents the
 * arguments the wrapped function was invoked with (see `makeKey`), so each distinct key
 * corresponds to one distinct stream: the result of a single `fn(...args)` call, which
 * any number of consumers may share. Consumers replay from the shared `buffer` (each
 * with its own cursor) instead of re-running the source. Holds a single underlying
 * source pulled on demand, the items pulled from it so far, the terminal state, the one
 * pull that may currently be in flight, and how many consumers are attached.
 */
interface CachedStream<ItemType> {
  source: AsyncIterator<ItemType>;
  buffer: ItemType[];
  done: boolean;
  /** Set once the source throws; boxed so a thrown `undefined` is still distinguishable. */
  error?: { value: unknown };
  /** The single in-flight `source.next()`, or null when no pull is outstanding (single-flight). */
  pending: Promise<void> | null;
  /** How many consumer generators are currently attached (governs incomplete-stream cleanup). */
  consumers: number;
}

/**
 * The cache shared by every consumer of one wrapped streaming function: a map of cache
 * key → {@link CachedStream}, plus the operations over it. One instance is created per
 * `withCache(...)` call and closed over by the returned wrapper; each call to the wrapper
 * creates a fresh {@link StreamCache.createStreamView} generator that reads this shared state.
 */
class StreamCache<ArgTypes extends any[], ItemType> {
  // Insertion order in a Map is the LRU order: the first key is the
  // least-recently-used, the last is the most-recently-used.
  private readonly entries = new Map<string, CachedStream<ItemType>>();

  constructor(
    private readonly fn: (...args: ArgTypes) => AsyncIterable<ItemType>,
    private readonly cacheErrors: boolean,
    private readonly makeKey: (args: ArgTypes) => string,
    private readonly maxSize: number | undefined,
  ) {}

  // Advance `entry`'s shared source by one item, recording the outcome. Never rejects: a
  // completion or error is captured as terminal state that every consumer observes by
  // re-reading the entry, so the single awaited pull can be shared safely.
  private async pullFromSource(key: string, entry: CachedStream<ItemType>): Promise<void> {
    try {
      const result = await entry.source.next();
      if (result.done) {
        entry.done = true;
      } else {
        entry.buffer.push(result.value);
      }
    } catch (error) {
      entry.error = { value: error };
      entry.done = true;
      // Same rule as the promise cache: don't cache a plain error unless asked, and
      // never cache a cancellation. Guard on identity so we don't evict a fresh entry
      // that may already have replaced this one under the same key.
      if ((!this.cacheErrors || isCancellation(error)) && this.entries.get(key) === entry) {
        this.entries.delete(key);
      }
    } finally {
      entry.pending = null;
    }
  }

  // Attach a consumer to this key's stream, creating it on first use, and return the
  // shared entry with this consumer counted. Fully synchronous (no await), so a consumer
  // is registered the instant it looks the entry up — it can never attach to one that
  // detach() is tearing down.
  private attach(key: string, args: ArgTypes): CachedStream<ItemType> {
    let entry = this.entries.get(key);
    if (entry === undefined) {
      // No entry in cache yet: create the one shared source for this key.
      // Strip the inbound signal: the source is shared by all consumers, so a per-consumer
      // abort must not be bound to it.
      const source = this.fn(...(stripSignal(args) as ArgTypes))[Symbol.asyncIterator]();
      entry = { source, buffer: [], done: false, error: undefined, pending: null, consumers: 0 };
      this.entries.set(key, entry);
      if (this.maxSize !== undefined && this.entries.size > this.maxSize) {
        this.entries.delete(this.entries.keys().next().value!); // evict the least-recently-used entry
      }
    } else if (this.maxSize !== undefined) {
      // Cache hit. Mark this entry most-recently-used: a Map iterates in insertion order and
      // we evict from the front (the LRU end), so deleting and reinserting moves this key to
      // the back, keeping it safe from the next eviction.
      this.entries.delete(key);
      this.entries.set(key, entry);
    }
    entry.consumers++;
    return entry;
  }

  // Detach a consumer. When the last one leaves a stream that never finished, close its
  // source and drop the entry so nothing keeps running in the background; a completed
  // stream stays cached for later callers.
  private detach(key: string, entry: CachedStream<ItemType>): void {
    if (--entry.consumers === 0 && !entry.done) {
      closeQuietly(entry.source);
      if (this.entries.get(key) === entry) this.entries.delete(key);
    }
  }

  // One consumer's view of the cached stream: replays the shared buffer, fetching from
  // the source only when it needs an item nobody has pulled yet. Lazy (the body runs at
  // the first pull) and per-consumer (its own cursor `i` and abort handling).
  async *createStreamView(...args: ArgTypes): AsyncIterable<ItemType> {
    const signal = extractSignal(args);
    signal?.throwIfAborted(); // lazy wrapper: the already-aborted check runs at the first pull
    const key = this.makeKey(args);
    const entry = this.attach(key, args);
    const abort = rejectOnAbort(signal);
    try {
      let i = 0;
      while (true) {
        signal?.throwIfAborted(); // let this consumer's own signal interrupt even a pure replay
        if (i < entry.buffer.length) {
          yield entry.buffer[i++]; // served from the cached entries
          continue;
        }
        // We've caught up to the end of the buffer, so the stream is in one of three states:
        // failed, finished, or not produced yet
        if (entry.error) {
          throw entry.error.value; // failed: propagate the source's error
        } else if (entry.done) {
          return; // finished: no items left
        } else {
          // Not produced yet: make sure a single pull is in flight, then wait for it.
          if (entry.pending === null) {
            entry.pending = this.pullFromSource(key, entry); // no pull running: start one to fetch the next item
          } else {
            // a concurrent consumer already started this pull; we share it (single-flight)
          }
          // Wait for that one pull to finish; our own signal can interrupt the wait.
          // Note that once the pull finishes, all concurrent consumers are unblocked 
          // and will all read the same new item from the buffer.
          await Promise.race([entry.pending, abort.promise]);
        }
      }
    } finally {
      abort.cleanup();
      this.detach(key, entry);
    }
  }
}

/**
 * Add caching to a **streaming** function, keyed by the call arguments. This is the
 * streaming analogue of the promise-family `withCache`, preserving the signature
 * `(...args) => AsyncIterable<ItemType>` so it composes with the other stream
 * combinators by nesting.
 *
 * Unlike a cached promise (a single shared value), a cached stream is **replayed**:
 * the first call for a key starts reading the underlying source, and every consumer for that
 * key — concurrent or arriving later — reads the same growing buffer of items. The
 * source is pulled **lazily and on demand**, one item at a time: an item is only
 * fetched when some consumer has read past the buffer, so an infinite source is never
 * force-drained, and a stream that is created but never iterated pulls nothing. When
 * several consumers want the next un-buffered item at once, exactly one `source.next()`
 * runs (single-flight) and they all read the resulting item from the buffer — so each
 * item is fetched from the source at most once, however many consumers are attached.
 *
 * By default the arguments are `JSON.stringify`-d with a trailing inbound `{ signal }`
 * omitted, overridable via `makeKey`.
 *
 * Cancellation: because the source is shared across consumers, it runs
 * independently of any one of them — the inbound `{ signal }` is stripped from the
 * call to `fn`, so one consumer's abort can never cancel the source out from under the
 * others. Each consumer's own `{ signal }` instead governs only *its own* view: it
 * abandons that consumer (throwing its reason) while the shared source and the other
 * consumers are left untouched.
 *
 * Lifecycle: a *completed* stream stays cached and is replayed to later callers. An
 * *incomplete* stream is only shared among the consumers overlapping in time — when the
 * last of them abandons it before it finishes, the shared source is closed and the entry
 * is dropped, so nothing keeps running in the background and the next call starts fresh.
 *
 * @template ArgTypes - The argument types of the streaming function
 * @template ItemType - The item type produced by the stream
 *
 * @param fn - The streaming function to add caching to
 * @param options - Configuration options for caching behavior
 * @param options.cacheErrors - Whether to cache a source error (default: false). When false, a
 *   failed source is evicted so the next call re-runs it; a *cancellation* is never cached
 *   regardless. When true, the buffered prefix and the terminal error are replayed to later callers.
 * @param options.makeKey - Custom cache-key function. The default is `JSON.stringify` with a
 *   trailing inbound `{ signal }` omitted, so a per-call cancellation signal never changes the key.
 * @param options.maxSize - Maximum number of entries to keep. When exceeded, the
 *   least-recently-used entry is evicted. Must be a positive integer. Omit for an unbounded cache.
 *
 * @returns A streaming function with the same signature that replays cached results on repeat calls
 *
 * @throws Error at wrap time if `maxSize` is provided and is not a positive integer
 */
export function withCache<ArgTypes extends any[], ItemType>(
  fn: (...args: ArgTypes) => AsyncIterable<ItemType>,
  options: {
    cacheErrors?: boolean;
    makeKey?: (args: ArgTypes) => string;
    maxSize?: number;
  } = {}
): (...args: ArgTypes) => AsyncIterable<ItemType> {
  // Default key omits a trailing inbound { signal } — a transient per-call concern,
  // not part of the logical key (see stripSignal).
  const { cacheErrors = false, makeKey = (args) => JSON.stringify(stripSignal(args)), maxSize } = options;

  if (maxSize !== undefined && (!Number.isInteger(maxSize) || maxSize < 1)) {
    throw new Error('maxSize must be a positive integer');
  }

  const cache = new StreamCache<ArgTypes, ItemType>(fn, cacheErrors, makeKey, maxSize);
  return (...args: ArgTypes) => cache.createStreamView(...args);
}
