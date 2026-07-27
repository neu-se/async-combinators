import fs from 'fs/promises';
import path from 'path';
import crypto from 'crypto';
import { isCancellation, stripSignal } from './core/cancellation';

/** The three record/replay modes. */
export type RecordReplayMode = 'replay' | 'record' | 'incrementalRecord';

/**
 * Thrown in `replay` mode when no recording exists for the requested key.
 *
 * This is the point of `replay` mode: an unrecorded interaction fails loudly
 * (in CI, say) instead of silently calling the real function. Carries the key
 * and fixture directory so the failure explains itself and points at the fix.
 *
 * Detect by name (`err.name === 'RecordingNotFoundError'`) rather than
 * `instanceof`, which is cross-realm safe.
 */
export class RecordingNotFoundError extends Error {
  readonly key: string;
  readonly cacheDir: string;

  constructor(key: string, cacheDir: string) {
    super(
      `No recording found for key ${JSON.stringify(key)} in ${cacheDir}. ` +
        `Re-record with mode 'record' or 'incrementalRecord'.`
    );
    this.name = 'RecordingNotFoundError';
    this.key = key;
    this.cacheDir = cacheDir;
  }
}

/**
 * Wrap an async function with record/replay functionality — a fixtures tool for
 * deterministic tests of code that makes real, slow, or costly calls: record the
 * responses once, then replay them deterministically in CI (fast and repeatable,
 * no external dependency, no flakiness).
 *
 * Recordings are stored as a directory of JSON files, one per distinct key,
 * named by a hash of the key (with git-style two-character sharding). The `mode`
 * option selects the behavior; the default `replay` is CI-safe:
 *
 * - **`replay`** (default) — fixture only. On a miss, throw
 *   {@link RecordingNotFoundError}; never call `fn`. An unrecorded interaction
 *   fails loudly instead of making an unintended real call.
 * - **`record`** — clear the fixture directory, then always call `fn` and record
 *   every call. The directory ends up as exactly this run's recordings (a fresh
 *   snapshot, no orphaned entries). Intentionally destructive.
 * - **`incrementalRecord`** — replay if present, else call `fn` and record.
 *   Keeps existing keys and fills the gaps; only calls `fn` for *new* interactions.
 *
 * Typically wired via env in test setup, e.g.
 * `mode: process.env.RECORD ? 'incrementalRecord' : 'replay'`.
 *
 * Use one fixture directory per wrapper: `record` rebuilds the whole directory.
 *
 * @template ArgTypes - The argument types for the function being recorded/replayed
 * @template RtrnType - The return type of the function being recorded/replayed
 *
 * @example
 * ```typescript
 * // CI: replay recorded responses; a new/changed call fails loudly.
 * const fetchData = withRecordReplay(callApi, './fixtures/api');
 *
 * // Locally, refresh only new interactions:
 * const fetchData = withRecordReplay(callApi, './fixtures/api', {
 *   mode: process.env.RECORD ? 'incrementalRecord' : 'replay',
 *   makeKey: (args) => args[0].id,
 *   cacheErrors: true,
 * });
 * ```
 *
 * @param fn - The async function to wrap with record/replay functionality
 * @param cacheDir - Path to the fixture directory on disk (relative or absolute); must be a non-empty string
 * @param options - Configuration options
 * @param options.mode - Record/replay mode (default: `'replay'`)
 * @param options.makeKey - Custom function to generate keys from arguments. The default is
 *   `JSON.stringify` with a trailing inbound `{ signal }` omitted, so a per-call cancellation
 *   signal never changes the key (or pollutes recording filenames). A custom `makeKey` receives
 *   the raw args (including any signal) and must exclude it if it keys on the trailing options.
 * @param options.cacheErrors - Whether to record and replay rejected promises (default: false)
 *
 * @returns A new function with the same signature that replays recorded results and/or records new ones per `mode`
 *
 * @throws Error at wrap time if `mode` is unknown or `cacheDir` is not a non-empty string
 * @throws RecordingNotFoundError in `replay` mode when no recording exists for the key
 * @throws Error when a recording file exists but is corrupted or cannot be accessed
 * @throws Error when a recording file cannot be written to disk
 */
export function withRecordReplay<ArgTypes extends any[], RtrnType>(
    fn: (...args: ArgTypes) => Promise<RtrnType>,
    cacheDir: string,
    options: {
      mode?: RecordReplayMode;
      makeKey?: (args: ArgTypes) => string;
      cacheErrors?: boolean;
    } = {}
): (...args: ArgTypes) => Promise<RtrnType> {
  // Default key omits a trailing inbound { signal } — it's a transient per-call
  // concern, not part of the logical key (see stripSignal).
  const { mode = 'replay', makeKey = (args) => JSON.stringify(stripSignal(args)), cacheErrors = false } = options;
  if (mode !== 'replay' && mode !== 'record' && mode !== 'incrementalRecord') {
    throw new Error(
      "mode must be one of 'replay', 'record', or 'incrementalRecord'"
    );
  }
  // Guard the fixture directory: an empty string would silently resolve to the
  // current working directory (writing recordings into the project directory), and a
  // non-string would throw a cryptic error deep inside `path.resolve`.
  if (typeof cacheDir !== 'string' || cacheDir.trim() === '') {
    throw new Error('cacheDir must be a non-empty string');
  }
  // Resolve to absolute path to avoid issues with relative paths.
  const dir = path.resolve(cacheDir);
  // `record` clears the fixture directory once, eagerly. A single promise means
  // concurrent first calls all await the same clear (and see any error on it).
  const ready: Promise<void> =
    mode === 'record'
      ? fs.rm(dir, { recursive: true, force: true })
      : Promise.resolve();

  return async (...args: ArgTypes): Promise<RtrnType> => {
    // Order every call after `record`'s directory clear: otherwise a call could
    // read/write recordings while `fs.rm` is still deleting the directory. The
    // shared promise means the clear runs once; later awaits resolve instantly.
    await ready;
    const key = makeKey(args);
    const file = recordingPath(dir, key);

    // replay / incrementalRecord consult the fixture first; record never does.
    if (mode !== 'record') {
      const recording = await readRecording<RtrnType>(file);
      // A recording's filename is the hash of its key, and the stored key must
      // match exactly. A mismatch means fixture corruption or a bad migration;
      // treat it as a miss so we fail loudly instead of replaying wrong data.
      if (recording && recording.key === key) {
        if ('error' in recording) {
          throw new Error(recording.error);
        }
        return recording.value;
      }
      // replay never calls fn: an unrecorded interaction fails loudly.
      if (mode === 'replay') {
        throw new RecordingNotFoundError(key, dir);
      }
    }

    // record / incrementalRecord: call fn and record the outcome.
    try {
      const result = await fn(...args);
      await writeRecording(file, { key, value: result });
      return result;
    } catch (error) {
      // Record a thrown error only when asked — and never a cancellation, which
      // isn't a real recorded outcome (it's the caller/fn bailing out).
      if (cacheErrors && !isCancellation(error)) {
        const message = error instanceof Error ? error.message : String(error);
        await writeRecording(file, { key, error: message });
      }
      throw error;
    }
  };
}

/**
 * A single recording as stored on disk: either a successful `value` or a
 * recorded `error` message. `key` is required and must match the lookup key.
 */
type Recording<RtrnType> =
  | { key: string; value: RtrnType }
  | { key: string; error: string };

/**
 * Path to the recording file for a key: `<dir>/<hash[0:2]>/<hash>`, where the
 * hash is the sha256 of the key. The two-character shard is git-style fan-out,
 * keeping any one directory from filling up.
 */
function recordingPath(dir: string, key: string): string {
  const hash = crypto.createHash('sha256').update(key).digest('hex');
  return path.join(dir, hash.slice(0, 2), hash);
}

/**
 * Read and parse a recording file.
 * Returns null if the file doesn't exist yet (a miss).
 * Throws if the file exists but cannot be read or contains invalid JSON.
 */
async function readRecording<RtrnType>(file: string): Promise<Recording<RtrnType> | null> {
  // Two separate try blocks so the read failure and the parse failure stay
  // distinct: a missing file (ENOENT) is a normal miss and must return null,
  // whereas invalid JSON is a real error. A single block around both would force
  // us to disambiguate the two failure kinds after the fact — and worse, a parse
  // error thrown inside the read's catch could be mistaken for a read error.
  let data: string;
  try {
    data = await fs.readFile(file, 'utf-8');
  } catch (readError: any) {
    if (readError.code === 'ENOENT') {
      return null; // no recording for this key
    }
    throw new Error(`Cannot read recording ${file}: ${readError.message}`);
  }
  try {
    return JSON.parse(data) as Recording<RtrnType>;
  } catch (parseError: any) {
    throw new Error(`Failed to parse recording ${file}: ${parseError.message}`);
  }
}

/**
 * Write a recording file atomically: write to a unique temp file in the same
 * directory, then rename over the target. `rename` is atomic on a single
 * filesystem, so a concurrent reader never sees a half-written file.
 */
async function writeRecording<RtrnType>(file: string, recording: Recording<RtrnType>): Promise<void> {
  await fs.mkdir(path.dirname(file), { recursive: true });
  const tmp = `${file}.${crypto.randomBytes(6).toString('hex')}.tmp`;
  await fs.writeFile(tmp, JSON.stringify(recording, null, 2), 'utf-8');
  await fs.rename(tmp, file);
}
