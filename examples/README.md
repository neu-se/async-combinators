# Examples

Runnable examples showing async-combinators in realistic scenarios. Run them from the
project root:

```sh
npm run example:fetch      # resilience: withRetry / withRateLimit / withTimeout over fetch
npm run example:bank       # concurrency: withReentrantLock for safe concurrent bank operations
npm run example:bench      # bounded batch: withMaxConcurrency over a capacity-limited server
npm run example:llm-test   # record/replay: withRecordReplay for deterministic LLM test fixtures
npm run example:stream     # streaming: withRetry / withTimeout / withRateLimit over AsyncIterable
```

| Example | Demonstrates |
| --- | --- |
| [`fetch-resilience.ts`](./fetch-resilience.ts) | `withRetry` / `withRateLimit` / `withTimeout` over native `fetch`: recovers from transient `503`s, paces a concurrent burst under a `429` rate limit, composes all three, and uses `shouldRetry` to fail fast on a permanent `400` instead of wasting attempts on it -- validated against a local flaky server. |
| [`bank.ts`](./bank.ts) | `withReentrantLock` / `ReentrantLock` for concurrency-safe bank operations: concurrent read-modify-write loses updates without a lock, whereas a reentrant-locked `Bank` is atomic and high-level operations (`deposit`, `transfer`) re-enter the lock through lower-level ones (`getBalance`, `setBalance`) without deadlocking. |
| [`benchmark-runner.ts`](./benchmark-runner.ts) | `withMaxConcurrency` for bounded batch processing: firing all evaluations at once overwhelms a capacity-limited server; limiting to a fixed concurrency scores every subject without dropping requests. |
| [`llm-integration-test.ts`](./llm-integration-test.ts) | `withRecordReplay` for deterministic LLM test fixtures: record real API responses once, then replay them in CI without a live API key. |
| [`stream-resilience.ts`](./stream-resilience.ts) | `withRetry` / `withTimeout` / `withRateLimit` from `async-combinators/stream`: demonstrates the three stream-specific behaviours -- pre-first-item transparent retry, per-pull deadline, and rate-limiting stream starts -- plus composition of all three. |

Each example is self-verifying: it asserts its expected outcomes and exits non-zero
on failure.
