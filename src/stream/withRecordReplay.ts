import fs from 'fs/promises';
import path from 'path';
import crypto from 'crypto';
import { isCancellation, stripSignal } from '../core/cancellation';
import { RecordReplayMode, RecordingNotFoundError } from '../withRecordReplay';

type StreamRecording<ItemType> =
  | { key: string; chunks: ItemType[] }
  | { key: string; chunks: ItemType[]; error: string };

/**
 * Add record/replay behavior to a **streaming** function. This is the streaming
 * analogue of the promise-family `withRecordReplay`, preserving the signature
 * `(...args) => AsyncIterable<ItemType>` so it composes with the other stream
 * combinators by nesting.
 *
 * Modes mirror the promise variant:
 * - `replay` (default): replay fixtures only; a miss throws `RecordingNotFoundError`.
 * - `record`: clear the fixture directory once, then always call `fn` and write fresh recordings.
 * - `incrementalRecord`: replay if present; on miss, call `fn` and write a recording.
 *
 * Recordings store the full chunk sequence. If `cacheErrors` is true, failures are
 * recorded as a chunk prefix plus terminal error message and replayed the same way.
 * Cancellations are never recorded.
 *
 * @template ArgTypes - The argument types of the streaming function
 * @template ItemType - The item type produced by the stream
 *
 * @param fn - The streaming function to wrap.
 * @param cacheDir - Path to the fixture directory on disk (relative or absolute).
 * @param options - Record/replay options.
 */
export function withRecordReplay<ArgTypes extends any[], ItemType>(
  fn: (...args: ArgTypes) => AsyncIterable<ItemType>,
  cacheDir: string,
  options: {
    mode?: RecordReplayMode;
    makeKey?: (args: ArgTypes) => string;
    cacheErrors?: boolean;
  } = {}
): (...args: ArgTypes) => AsyncIterable<ItemType> {
  const {
    mode = 'replay',
    makeKey = (args) => JSON.stringify(stripSignal(args)),
    cacheErrors = false,
  } = options;

  if (mode !== 'replay' && mode !== 'record' && mode !== 'incrementalRecord') {
    throw new Error("mode must be one of 'replay', 'record', or 'incrementalRecord'");
  }
  if (typeof cacheDir !== 'string' || cacheDir.trim() === '') {
    throw new Error('cacheDir must be a non-empty string');
  }

  const dir = path.resolve(cacheDir);
  const ready: Promise<void> =
    mode === 'record'
      ? fs.rm(dir, { recursive: true, force: true })
      : Promise.resolve();

  return async function* (...args: ArgTypes): AsyncIterable<ItemType> {
    await ready;

    const key = makeKey(args);
    const file = recordingPath(dir, key);

    if (mode !== 'record') {
      const recording = await readRecording<ItemType>(file);
      if (recording && recording.key === key) {
        for (const chunk of recording.chunks) {
          yield chunk;
        }
        if ('error' in recording) {
          throw new Error(recording.error);
        }
        return;
      }
      if (mode === 'replay') {
        throw new RecordingNotFoundError(key, dir);
      }
    }

    // Cache miss path: stream chunks from the source and capture them for recording.
    const chunks: ItemType[] = [];
    try {
      for await (const chunk of fn(...args)) {
        chunks.push(chunk);
        yield chunk;
      }
      await writeRecording(file, { key, chunks });
    } catch (error) {
      if (cacheErrors && !isCancellation(error)) {
        const message = error instanceof Error ? error.message : String(error);
        await writeRecording(file, { key, chunks, error: message });
      }
      throw error;
    }
  };
}

function recordingPath(dir: string, key: string): string {
  const hash = crypto.createHash('sha256').update(key).digest('hex');
  return path.join(dir, hash.slice(0, 2), hash);
}

async function readRecording<ItemType>(file: string): Promise<StreamRecording<ItemType> | null> {
  let data: string;
  try {
    data = await fs.readFile(file, 'utf-8');
  } catch (readError: any) {
    if (readError.code === 'ENOENT') {
      return null;
    }
    throw new Error(`Cannot read recording ${file}: ${readError.message}`);
  }

  try {
    return JSON.parse(data) as StreamRecording<ItemType>;
  } catch (parseError: any) {
    throw new Error(`Failed to parse recording ${file}: ${parseError.message}`);
  }
}

async function writeRecording<ItemType>(file: string, recording: StreamRecording<ItemType>): Promise<void> {
  await fs.mkdir(path.dirname(file), { recursive: true });
  const tmp = `${file}.${crypto.randomBytes(6).toString('hex')}.tmp`;
  await fs.writeFile(tmp, JSON.stringify(recording, null, 2), 'utf-8');
  await fs.rename(tmp, file);
}
