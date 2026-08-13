# async-combinators

A comprehensive collection of TypeScript utilities for working with async functions. Add caching, retries, timeouts, rate limiting, concurrency control, and more to your async operations with simple, composable higher-order functions.

## Features

- 🎯 **Type-safe** - Full TypeScript support with generics
- 🔧 **Composable** - Combine utilities for powerful async patterns
- 🧪 **Well-tested** - Extensive unit test suite
- 📦 **Zero dependencies** - Lightweight and focused
- ♻️ **Reentrancy-aware** - Locks that don't deadlock on recursion
- 🛑 **Cancellation-aware** - Recognizes `AbortError`, and an inbound `AbortSignal` drops pending work
- 🌊 **Stream support** - Full `AsyncIterable` combinator family with the same vocabulary

## Installation

```bash
npm install async-combinators
```

## Quick Start

```typescript
import { withRetry, withTimeout, withCache } from 'async-combinators';

// Add retry logic to an API call
const fetchWithRetry = withRetry(fetch, 3);

// Add timeout to prevent hanging
const fetchWithTimeout = withTimeout(fetch, 5000);

// Combine utilities for robust API calls
const robustFetch = withRetry(
  withTimeout(
    withCache(fetch),
    5000
  ),
  3
);
```

## API Reference

### Promise Combinators (`async-combinators`)

The `async-combinators` package exports a combinator family for functions shaped like
`(...args) => Promise<R>`. Each wraps your async function and returns a new one with the
same signature:

```typescript
import {
  withRetry, withTimeout, withFallback, withRateLimit, withMaxConcurrency,
  withCache, withLock, withReentrantLock, withRecordReplay,
} from 'async-combinators';
```

#### `withCache`
Adds caching to async functions based on arguments. By default the cache is
unbounded; pass `maxSize` (a positive integer) to cap it with least-recently-used
eviction.

```typescript
const cachedFetch = withCache(
  async (url: string) => fetch(url).then(r => r.json()),
  { cacheErrors: false, maxSize: 500 } // keep at most 500 entries (LRU)
);

const data1 = await cachedFetch('/api/users'); // Fetches from API
const data2 = await cachedFetch('/api/users'); // Returns cached result
```

#### `withRetry`
Retries failed operations with configurable attempts and optional backoff. The attempt
count must be a positive integer; otherwise the wrapper throws immediately.

```typescript
const fetchWithRetry = withRetry(
  async (url: string) => fetch(url),
  3, // Max attempts, including the first (must be a positive integer)
  {
    delayMs: 100,            // base delay before a retry (default 0 — retry immediately)
    backoff: 'exponential',  // 'fixed' | 'exponential' (default 'exponential': 100, 200, 400, ...)
    factor: 2,               // exponential growth factor (default 2)
    maxDelayMs: 5000,        // cap on the computed delay (default Infinity)
    jitter: true,            // spread retries randomly over [0, computed] (default false)
    // Classify errors before retrying; return false to propagate immediately
    // instead of retrying (e.g. an unrecoverable API error). Default retries
    // every error.
    shouldRetry: (error, failedAttempt) => !(error instanceof MaxTokensError),
    // Called after a failed attempt when a retry will follow; receives the
    // number of the attempt that failed and the error it threw.
    onRetry: (failedAttempt, error) => console.log(`Attempt ${failedAttempt} failed:`, error),
  }
);
```

The backoff wait is cancellation-aware: an inbound `{ signal }` that aborts during it
abandons the retry.

#### `withTimeout`
Adds timeout to async operations. The timeout must be a positive integer (in
milliseconds), or `Infinity` to disable it; other values throw.

```typescript
const fetchWithTimeout = withTimeout(
  async (url: string) => fetch(url),
  5000, // 5 second timeout (positive integer, or Infinity to disable)
  new Error('Request timed out')
);
```

By default a timed-out call rejects with a `TimeoutError` (a *retryable* failure — see
[Cancellation](#cancellation)). Passing a custom error, as above, overrides that, so it is
no longer recognized as a timeout by `withRetry`.

#### `withFallback`
Provides fallback when primary function fails. Both the primary and fallback must be
functions; otherwise the wrapper throws immediately.

```typescript
const fetchUser = withFallback(
  async (id: number) => primaryDb.getUser(id),
  async (id: number) => cacheDb.getUser(id)
);
```

#### `withRateLimit`
Rate limits function calls to prevent API throttling. The interval must be a positive
integer (in milliseconds); other values throw.

```typescript
const rateLimitedFetch = withRateLimit(
  async (url: string) => fetch(url),
  1000 // Minimum 1 second between calls (positive integer)
);
```

#### `withMaxConcurrency`
Limits concurrent executions of async functions. The limit must be a positive
integer; otherwise the wrapper throws immediately.

```typescript
const limitedFetch = withMaxConcurrency(
  async (url: string) => fetch(url),
  3 // Max 3 concurrent requests (must be a positive integer)
);

// Process 100 URLs with max 3 concurrent
const urls = Array.from({ length: 100 }, (_, i) => `/api/item/${i}`);
const results = await Promise.all(urls.map(url => limitedFetch(url)));
```

#### `withRecordReplay`
Records function results to a fixture directory and replays them deterministically —
a testing tool for code that makes real, slow, or costly calls (e.g. LLM APIs).
Record real responses once, then replay them in CI: fast, deterministic, no flakiness.

Recordings are stored as a directory of JSON files, one per distinct call (each named
by a hash of the key). The `mode` option controls behavior; the default `replay` is
CI-safe:

- **`replay`** (default) — fixture only. On a miss, throw `RecordingNotFoundError`;
  never call the wrapped function. An unrecorded interaction fails loudly instead
  of making an unintended real call.
- **`record`** — clear the fixture directory, then call the function and record every
  call. The directory becomes a fresh snapshot of exactly this run.
- **`incrementalRecord`** — replay if present, else call and record. Keeps existing
  recordings and only records new interactions.

Use one fixture directory per wrapper (`record` rebuilds the whole directory). The
`cacheDir` must be a non-empty string.

```typescript
import { withRecordReplay } from 'async-combinators';

const askModel = withRecordReplay(
  async (prompt: string) => callLLM(prompt),
  './fixtures/llm', // a directory
  {
    // Record locally with RECORD=1; replay everywhere else (e.g. CI).
    mode: process.env.RECORD ? 'incrementalRecord' : 'replay',
    makeKey: (args) => args[0],
    cacheErrors: true,
  }
);
```

#### `withLock`
Protects async functions with non-reentrant locks.

```typescript
import { withLock, Lock } from 'async-combinators';

const lock = new Lock();
let counter = 0;

const increment = withLock(async (amount: number) => {
  const current = counter;
  await someAsyncWork();
  counter = current + amount;
  return counter;
}, lock);

// These execute sequentially, preventing race conditions
await Promise.all([increment(1), increment(2), increment(3)]);
console.log(counter); // 6 (correct result)
```

#### `withReentrantLock`
Protects async functions with reentrant locks (allows recursion). The lock is
optional: if omitted, a new `ReentrantLock` is created automatically —
enough to serialize a function's own invocations while letting it recurse. Pass
a shared lock to coordinate several functions under one reentrant lock.

```typescript
import { withReentrantLock } from 'async-combinators';

// Default lock: serializes concurrent top-level calls, yet the recursion
// re-enters instead of deadlocking (withLock would deadlock here).
const processTree = withReentrantLock(async function walk(node: TreeNode): Promise<void> {
  updateSharedState(node);
  for (const child of node.children ?? []) {
    await walk(child); // reentrant — no deadlock
  }
});

// A shared lock coordinates several functions (e.g. mutually recursive ones):
import { ReentrantLock } from 'async-combinators';
const lock = new ReentrantLock();
const fnA = withReentrantLock(rawA, lock);
const fnB = withReentrantLock(rawB, lock);
```

### Locking / Concurrency Primitives

#### `Lock`
Non-reentrant async lock for mutual exclusion. Prefer `runExclusive`, which
acquires and releases automatically (even if the critical section throws).

```typescript
import { Lock } from 'async-combinators';

const lock = new Lock();

async function criticalSection() {
  await lock.runExclusive(async () => {
    // Critical section code
  });
}
```

For cases where a single callback doesn't fit, `acquire()` returns a one-shot
release function (so the lock can't be released by code that doesn't hold it):

```typescript
const release = await lock.acquire();
try {
  // Critical section code
} finally {
  release();
}
```

`isLocked()` reports whether the lock is currently held, without blocking:

```typescript
if (!lock.isLocked()) {
  // resource is free
}
```

#### `Semaphore`
An async counting semaphore: at most `permits` holders at once -- a generalization of `Lock`
(a lock is a `Semaphore(1)`). Waiters are served FIFO. Prefer `runExclusive`, which acquires
and releases automatically; use `acquire()` (which returns a one-shot release function) when a
single callback doesn't fit, e.g. holding a permit for the lifetime of a stream.

```typescript
import { Semaphore } from 'async-combinators';

const sem = new Semaphore(3); // at most 3 concurrent

await sem.runExclusive(async () => {
  // at most three of these run at once; the permit is released even on throw
});

// Or manual acquire/release, when a single callback doesn't fit:
const release = await sem.acquire();
try {
  // hold the permit
} finally {
  release();
}
```

`available()` reports how many permits are currently free. Like `Lock`, both `acquire` and
`runExclusive` accept an optional `AbortSignal` that drops a still-waiting acquirer (a holder
that already acquired is unaffected).

#### `ReentrantLock`
Reentrant async lock. Independent operations contending for the lock are
serialized, but a call made from within an operation that already holds the lock
re-enters without deadlocking. Reentrancy is tracked automatically per async call
chain — there is no owner id to manage.

```typescript
import { ReentrantLock } from 'async-combinators';

const lock = new ReentrantLock();

async function recursiveFunction(depth: number): Promise<void> {
  await lock.runExclusive(async () => {
    if (depth > 0) {
      await recursiveFunction(depth - 1); // Reentrant — no deadlock
    }
  });
}
```

Like `Lock`, it exposes `isLocked()` — whether the lock is currently held by
any call chain (does not block).

### Stream Combinators (`async-combinators/stream`)

The `async-combinators/stream` subpath exposes a full combinator family for functions
shaped like `(...args) => AsyncIterable<T>`. They share the same names and composition
as the promise family:

```typescript
import {
  withRetry, withTimeout, withFallback, withRateLimit, withMaxConcurrency,
  withCache, withLock, withReentrantLock, withRecordReplay,
} from 'async-combinators/stream';
```

Composition example:

```typescript
import { withRetry, withTimeout, withFallback } from 'async-combinators/stream';

const resilientStream = withFallback(
  withRetry(
    withTimeout(
      async function* fetchChunks(url: string): AsyncIterable<string> {
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        yield await response.text();
      },
      5000
    ),
    3
  ),
  async function* (_url: string): AsyncIterable<string> {
    yield 'fallback-chunk';
  }
);
```

#### `withRetry` (streaming)

Retries a streaming function, with the same backoff options as the promise `withRetry`.
Because a stream fails at a *point*, retry depends on whether the consumer has already
seen output:

- **Before the first item** -- nothing has been observed, so a failed attempt is discarded
  and a fresh one started transparently (a dropped connection, or a throttling response
  before any data). This is the common case and behaves like the promise version.
- **After some items** -- restarting would re-emit data the consumer already saw, which is
  only sound if the stream is *deterministic* in its arguments. Set `resumable: true` to
  assert that; the wrapper re-runs and skips the items already delivered. Left at its
  default (`false`), a later failure propagates -- the correct behavior for a
  nondeterministic source such as an LLM token stream.

```typescript
import { withRetry } from 'async-combinators/stream';

// Nondeterministic source (a model token stream): resumable stays false, so retry
// recovers a failure before the first token and propagates a later one.
const robustStream = withRetry(streamCompletion, 4, {
  delayMs: 500,
  backoff: 'exponential',
  onRetry: (attempt, err) => console.warn(`attempt ${attempt} failed:`, err),
});
for await (const chunk of robustStream('Explain async generators')) {
  process.stdout.write(chunk);
}

// Deterministic source (a paginated feed): opt into mid-stream recovery.
// A failure after some rows restarts and skips the ones already delivered.
const resilientPages = withRetry(streamPages, 5, { delayMs: 200, resumable: true });
for await (const row of resilientPages('orders')) { /* ... */ }
```

`resumable` is an unchecked assertion: the wrapper does not compare item values, so a
false assertion on a same-length source silently splices two runs. It does catch the one
inconsistency it can detect cheaply -- a re-run that yields *fewer* items than were already
delivered throws a `ResumeConsistencyError` (exported from `async-combinators/stream`)
instead of silently truncating.

As with the promise version, `shouldRetry(error, failedAttempt)` classifies errors before
retrying -- return `false` to propagate an unrecoverable error immediately instead of
retrying. It is checked after the cancellation, `maxAttempts`, and non-`resumable` guards,
so it can only narrow retries, never resurrect one those already ruled out.

Cancellation and cleanup carry over: an inbound `{ signal }` is honored (checked at the
first pull, since the wrapper is a lazy async generator), and abandoning the stream (a
`break`, a `.return()`, or a consumer error) tears down the underlying source.

#### `withFallback` (streaming)

Tries a primary streaming function and switches to a fallback streaming function if the
primary fails -- but only *before the primary has produced any output*. Once the primary has
yielded an item, the consumer has committed to it, and the fallback is a different source
with its own sequence, so switching would splice two unrelated streams; a failure after the
first item therefore propagates unchanged.

```typescript
import { withFallback } from 'async-combinators/stream';

// Fall back to a replica's event stream if the primary connection fails before
// delivering anything; a mid-stream drop propagates rather than silently splicing.
const events = withFallback(streamFromPrimary, streamFromReplica);
for await (const e of events(topic)) { handle(e); }
```

A cancellation is not recovered from: if the primary throws an `AbortError`, it propagates
and the fallback is not run. Fallbacks chain, so `withFallback(withFallback(a, b), c)` tries
`a`, then `b`, then `c`, each before its own first item.

#### `withRateLimit` (streaming)

Spaces out the *starts* of streams so at most one begins per interval. Because the wrapper is
a lazy async generator, a stream claims its slot when it starts (its first pull) -- so a stream
created but never iterated reserves nothing, and a concurrent burst is paced in the order
streams start iterating. It gates only the start, not the cadence of items within a stream.

```typescript
import { withRateLimit } from 'async-combinators/stream';

// Begin at most one stream per second; items within a stream flow at the source's pace.
const paced = withRateLimit(streamItems, 1000);
for await (const item of paced(query)) { handle(item); }
```

An inbound `{ signal }` aborts a pending gate wait, so the source never starts. To promptly
abandon a stream that may still be waiting at the gate, abort the signal -- a bare `break` only
takes effect at the next `yield`.

#### `withMaxConcurrency` (streaming)

Limits how many streams are *actively producing* at once. A stream holds a slot for its whole
lifetime: it acquires one when it starts (first pull), holds it while producing, and releases it
when it ends -- by normal completion, an error, or the consumer abandoning it. Extra streams wait
in FIFO order for a free slot.

```typescript
import { withMaxConcurrency } from 'async-combinators/stream';

// At most 3 of these streams run concurrently; the rest queue until a slot frees.
const limited = withMaxConcurrency(openFeed, 3);
await Promise.all(topics.map((t) => drain(limited(t))));
```

Two consequences of holding the slot for the stream's lifetime: a stream created but never
iterated acquires no slot, and -- because the consumer drives the lifetime -- a slow or stalled
consumer holds its slot for as long as it dawdles. An inbound `{ signal }` drops a stream that
is still waiting for a slot; abort the signal to promptly abandon a waiting stream.

#### `withTimeout` (streaming)

Bounds how long the *source* may take to answer each pull. Every call to the stream's `next()`
must resolve within `maxDurationMs`, otherwise the stream rejects with a `TimeoutError`. Because
a stream is pull-based, the deadline measures the source's response to each pull, not the
wall-clock gap between delivered items: the timer runs only while a pull is outstanding, so a
slow *consumer* never trips it -- the timeout fires only when the *source* is slow to produce.
This one rule covers both the time to the first item and the idle gap between later items.

```typescript
import { withTimeout } from 'async-combinators/stream';

// Each chunk must arrive within 5s of being requested; a stalled source rejects,
// while a consumer that dawdles between chunks is never penalized.
const bounded = withTimeout(streamChunks, 5000);
for await (const chunk of bounded(url)) { handle(chunk); }
```

Pass `Infinity` to disable the timeout (the function is returned unchanged, zero overhead), or
a custom error as the third argument to reject with something other than `TimeoutError`. On
timeout (or an inbound `{ signal }` abort) the source is torn down; a source that is genuinely
stuck is released best-effort rather than awaited, so a hung source can never delay the timeout
itself. The default `TimeoutError` is retryable, so `withRetry(withTimeout(fn, ms), n)` retries
a stalled pull.

#### `withCache` (streaming)

Caches a streaming function by its arguments. A cached promise is one shared value; a cached
stream is *replayed* instead. The first call for a key starts reading the underlying source, and
every consumer for that key -- concurrent or arriving later -- reads the same growing store of
items. The source is pulled *lazily and on demand*, one item at a time: an item is fetched only
when a consumer reads past the store, so an infinite source is never force-drained and a stream
that is created but never iterated pulls nothing. When several consumers want the next
un-buffered item at once, exactly one pull runs and they all read the result, so each item is
fetched from the source at most once.

```typescript
import { withCache } from 'async-combinators/stream';

// Repeated queries for the same key replay the cached items instead of re-hitting the source.
const cached = withCache(streamRows, { maxSize: 100 });
for await (const row of cached('orders')) { handle(row); }
```

Keying, `cacheErrors`, and `maxSize` (LRU) behave as in the promise `withCache`; by default the
key is the `JSON.stringify`-d arguments with a trailing `{ signal }` omitted, overridable via
`makeKey`. A *completed* stream stays cached and is replayed to later callers; an *incomplete*
stream is shared only among consumers overlapping in time -- once the last of them abandons it,
the shared source is closed and the entry is dropped, so nothing keeps running in the background
and the next call starts fresh.

Cancellation is per consumer: because the source is shared, it runs independently of any one
consumer's `{ signal }` (the signal is excluded from both the key and the source call). Aborting
one consumer's signal ends only that consumer -- throwing its reason -- while the shared source
and the other consumers continue undisturbed.

## Composing Utilities

The real power comes from combining utilities:

```typescript
import { withRetry, withTimeout, withCache, withRateLimit } from 'async-combinators';

// Create a robust, cached, rate-limited API client
const robustFetch = withRetry(
  withTimeout(
    withRateLimit(
      withCache(
        async (url: string) => {
          const response = await fetch(url);
          if (!response.ok) throw new Error(`HTTP ${response.status}`);
          return response.json();
        }
      ),
      1000 // Rate limit: 1 call per second
    ),
    5000 // Timeout: 5 seconds
  ),
  3 // Retry: up to 3 attempts
);

// Use it
const data = await robustFetch('/api/data');
```

## Common Patterns

### Resilient API Calls
```typescript
const resilientFetch = withFallback(
  withRetry(
    withTimeout(fetch, 5000),
    3
  ),
  async (url) => getCachedResponse(url)
);
```

### Controlled Batch Processing
```typescript
const processItem = withMaxConcurrency(
  async (item: Item) => {
    // Process item
    return await heavyOperation(item);
  },
  5 // Max 5 concurrent operations
);

const items = await loadItems();
const results = await Promise.all(items.map(item => processItem(item)));
```

### Protected Shared Resources
```typescript
import { withLock, Lock } from 'async-combinators';

const lock = new Lock();
const sharedResource = new Map();

const safeUpdate = withLock(async (key: string, value: any) => {
  const current = sharedResource.get(key);
  await someAsyncWork();
  sharedResource.set(key, value);
}, lock);
```

## Cancellation

The library treats **cancellation** as distinct from failure, in two directions.

**Outbound — recognizing a cancelled call.** If a wrapped function rejects with an
`AbortError` (what an aborted `AbortSignal` — e.g. one passed to `fetch` — produces),
the resilience wrappers treat it as "the caller asked to stop," not as a failure:
`withRetry` won't retry it, `withFallback` won't fall back, and `withCache` /
`withRecordReplay` won't cache or record it — the `AbortError` just propagates.

**Inbound — abandoning pending work.** Pass an `AbortSignal` in a trailing `{ signal }`
options object as the last argument (the `fetch` convention); your function opts in by
declaring it:

```typescript
import { withRetry } from 'async-combinators';

const search = withRetry(
  async (query: string, opts?: { signal?: AbortSignal }) =>
    fetch(`/search?q=${query}`, { signal: opts?.signal }).then((r) => r.json()),
  3,
  { delayMs: 200 },
);

const controller = new AbortController();
const promise = search('foo', { signal: controller.signal });
controller.abort(); // rejects with the signal's reason; abandons the pending retry/backoff
```

Every combinator that *waits* honors the signal and drops its pending work on abort,
rejecting with the signal's reason:

- `withTimeout` — abandons the timeout race
- `withRateLimit` — abandons the interval wait
- `withMaxConcurrency` — removes the call from the queue before it runs
- `withLock` / `withReentrantLock` (and `Lock` / `ReentrantLock`) — removes the
  waiter from the lock queue (a holder that already acquired is unaffected)
- `withRetry` — abandons a backoff wait between attempts

Because the signal is carried in `...args`, it also reaches your function automatically (so
`fn` can honor it mid-flight), and the default `withCache` / `withRecordReplay` key
excludes it — a per-call signal never changes the cache key.

**Timeout vs. cancellation.** `withTimeout` rejects with a `TimeoutError` (a *retryable*
failure), so `withRetry` will still retry a timed-out call. Detection is by error `name`
(`'AbortError'` / `'TimeoutError'`), which is cross-realm-safe; the exported
`isCancellation(err)` helper and `TimeoutError` class let you branch on it yourself:

```typescript
import { isCancellation } from 'async-combinators';

try {
  await search('foo', { signal: controller.signal });
} catch (err) {
  if (isCancellation(err)) return; // aborted — nothing to report
  throw err;                       // a real failure
}
```

## TypeScript Support

All utilities are fully typed with generics:

```typescript
// Type inference works automatically
const typedFetch = withCache(
  async (url: string): Promise<User> => {
    const response = await fetch(url);
    return response.json();
  }
);

// result is inferred as User
const result = await typedFetch('/api/user/123');
```

## Examples

Each example is self-contained and executable from the repo root:

- `npm run example:fetch` — adding retry, rate limiting, and timeout to plain `fetch` against a local flaky server
- `npm run example:stream` — stream-specific retry (pre-data failure), per-item timeout, and rate-limited starts
- `npm run example:bank` — concurrency-safe bank account transfers using `ReentrantLock`
- `npm run example:bench` — bounded batch processing with `withMaxConcurrency`
- `npm run example:llm-test` — recording and replaying LLM API calls with `withRecordReplay`

## Case Studies

Beyond the in-repo examples above, standalone, runnable case studies apply the
combinators to real, widely-used software:

- [async-combinators-fetch-example](https://github.com/neu-se/async-combinators-fetch-example) — hardening native `fetch` against a server that is both flaky and rate-limited, with `withTimeout` / `withRateLimit` / `withRetry`.
- [async-combinators-lowdb-example](https://github.com/neu-se/async-combinators-lowdb-example) — fixing lost updates in [lowdb](https://github.com/typicode/lowdb) (which provides no concurrency control of its own) with a `withReentrantLock`-guarded bank.
- [async-combinators-strands-example](https://github.com/neu-se/async-combinators-strands-example) — hardening a [Strands](https://github.com/strands-agents/harness-sdk) agent's model calls with `withRateLimit` / `withTimeout` / `withRetry`, via a `modelCallDriver` extension point added to [our fork of the framework](https://github.com/neu-se/harness-sdk) (branch `adopt-async-combinators`) that lets `withRetry` drive its retry loop directly while preserving native per-attempt observability.
- [llmorpheus](https://github.com/neu-se/llmorpheus) — the project that originally motivated these combinators: `withRetry` / `withTimeout` / `withRateLimit` harden its LLM API calls, and `withRecordReplay` provides deterministic fixtures for its integration tests.

## Testing

```bash
npm test
```

All utilities are thoroughly tested, covering:
- Basic functionality
- Error handling
- Edge cases
- Concurrent behavior
- Type preservation

## License

MIT

## Contributing

Contributions are welcome! Please ensure:
- All tests pass
- New features include tests
- Documentation is updated
- Code follows existing style
