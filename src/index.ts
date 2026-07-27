// Async utility functions
export { withCache } from './withCache';
export { withFallback } from './withFallback';
export { withRecordReplay, RecordingNotFoundError } from './withRecordReplay';
export type { RecordReplayMode } from './withRecordReplay';
export { withRetry } from './withRetry';
export { withTimeout } from './withTimeout';
export { withRateLimit } from './withRateLimit';
export { withMaxConcurrency } from './withMaxConcurrency';
export { withLock } from './withLock';
export { withReentrantLock } from './withReentrantLock';

// Locking / concurrency primitives
export { Lock } from './lock';
export { ReentrantLock } from './reentrant-lock';
export { Semaphore } from './semaphore';

// Cancellation
export { TimeoutError, isCancellation } from './core/cancellation';
