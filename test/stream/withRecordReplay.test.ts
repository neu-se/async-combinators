import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import crypto from 'crypto';
import { withRecordReplay } from '../../src/stream/withRecordReplay';
import { RecordingNotFoundError } from '../../src/withRecordReplay';
import { collect, collectInto, arrayStream } from './helpers';

function neverCalledFn<T = string>() {
  return jest.fn(async function* (..._args: any[]): AsyncIterable<T> {
    throw new Error('fn should not have been called');
  });
}

async function listRecordings(dir: string): Promise<string[]> {
  const out: string[] = [];
  let shards: string[];
  try {
    shards = await fs.readdir(dir);
  } catch (e: any) {
    if (e.code === 'ENOENT') return out;
    throw e;
  }

  for (const shard of shards) {
    const shardPath = path.join(dir, shard);
    if (!(await fs.stat(shardPath)).isDirectory()) continue;
    for (const f of await fs.readdir(shardPath)) {
      if (f.endsWith('.tmp')) continue;
      out.push(path.join(shardPath, f));
    }
  }

  return out;
}

describe('stream/withRecordReplay', () => {
  let root: string;
  let dir: string;

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), 'stream-record-replay-test-'));
    dir = path.join(root, 'fixtures');
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  describe('replay mode (default)', () => {
    it('throws RecordingNotFoundError on a miss and never calls fn', async () => {
      const replayFn = neverCalledFn();
      const replay = withRecordReplay(replayFn, dir);

      // 'a' is the wrapped-call argument (part of the derived key), not a literal
      // filename; the lookup path is hash-based under `dir/<shard>/<hash>`.
      const rejection = expect(collect(replay('a'))).rejects;
      await rejection.toBeInstanceOf(RecordingNotFoundError);
      await rejection.toMatchObject({
        name: 'RecordingNotFoundError',
        key: JSON.stringify(['a']),
        cacheDir: path.resolve(dir),
      });
      expect(replayFn).not.toHaveBeenCalled();
      expect(await listRecordings(dir)).toHaveLength(0);
    });

    it('replays chunks recorded by another wrapper instance without calling fn', async () => {
      // Phase 1: record fixtures by running an incrementalRecord wrapper on a miss.
      const recorder = withRecordReplay(
        async function* (x: string): AsyncIterable<string> {
          yield `result-${x}-1`;
          yield `result-${x}-2`;
        },
        dir,
        { mode: 'incrementalRecord' }
      );
      expect(await collect(recorder('a'))).toEqual(['result-a-1', 'result-a-2']);

      // Phase 2: use a fresh replay-mode wrapper; it must read those fixtures
      // and never invoke its own source function.
      const replayFn = neverCalledFn<string>();
      const replay = withRecordReplay(replayFn, dir);
      expect(await collect(replay('a'))).toEqual(['result-a-1', 'result-a-2']);
      expect(replayFn).not.toHaveBeenCalled();
      expect(await listRecordings(dir)).toHaveLength(1);
    });
  });

  describe('incrementalRecord mode', () => {
    it('records on a miss and replays on a hit', async () => {
      const sourceFn = jest.fn(async function* (x: string): AsyncIterable<string> {
        yield `result-${x}`;
      });
      const rec = withRecordReplay(sourceFn, dir, { mode: 'incrementalRecord' });

      expect(await collect(rec('a'))).toEqual(['result-a']); // record
      expect(await collect(rec('a'))).toEqual(['result-a']); // replay
      expect(await collect(rec('b'))).toEqual(['result-b']); // record (new key)
      expect(sourceFn).toHaveBeenCalledTimes(2);
      expect(await listRecordings(dir)).toHaveLength(2);
    });
  });

  describe('record mode', () => {
    it('clears existing recordings and writes a fresh snapshot', async () => {
      const oldFn = withRecordReplay(
        async function* (x: string): AsyncIterable<string> {
          yield `old-${x}`;
        },
        dir,
        { mode: 'incrementalRecord' }
      );
      await collect(oldFn('a')); // create initial recording
      await collect(oldFn('b')); // create initial recording

      const rec = withRecordReplay(
        async function* (x: string): AsyncIterable<string> {
          yield `new-${x}`;
        },
        dir,
        { mode: 'record' }
      );
      expect(await collect(rec('a'))).toEqual(['new-a']); // in record mode, so the old 'a' recording was erased and replaced
      expect(await listRecordings(dir)).toHaveLength(1);

      const replay = withRecordReplay(neverCalledFn<string>(), dir);
      expect(await collect(replay('a'))).toEqual(['new-a']); // replay the new recording
      await expect(collect(replay('b'))).rejects.toBeInstanceOf(RecordingNotFoundError); // the old 'b' recording was erased
    });
  });

  describe('cacheErrors', () => {
    it('does not record stream errors by default; a second call re-runs fn', async () => {
      const sourceFn = jest.fn(async function* (_k: string): AsyncIterable<string> {
        yield 'prefix';
        throw new Error('boom');
      });
      const rec = withRecordReplay(sourceFn, dir, { mode: 'incrementalRecord' });

      await expect(collect(rec('k'))).rejects.toThrow('boom');
      await expect(collect(rec('k'))).rejects.toThrow('boom');
      expect(sourceFn).toHaveBeenCalledTimes(2);
      expect(await listRecordings(dir)).toHaveLength(0);
    });

    it('records and replays prefix + terminal error when cacheErrors is true', async () => {
      const sourceFn = jest.fn(async function* (_k: string): AsyncIterable<string> {
        yield 'prefix';
        throw new Error('boom');
      });
      const rec = withRecordReplay(sourceFn, dir, {
        mode: 'incrementalRecord',
        cacheErrors: true,
      });

      // Phase 1: record path. The first call runs the source, yields the prefix,
      // then throws; with cacheErrors=true, that prefix+error is recorded.
      const firstOut: string[] = [];
      await expect(collectInto(firstOut, rec('k'))).rejects.toThrow('boom');
      expect(firstOut).toEqual(['prefix']);
      expect(sourceFn).toHaveBeenCalledTimes(1);

      // Phase 2: replay path. The second call replays the recorded prefix and
      // then rethrows the recorded terminal error without calling the source.
      const replayOut: string[] = [];
      await expect(collectInto(replayOut, rec('k'))).rejects.toThrow('boom');
      expect(replayOut).toEqual(['prefix']);
      // Still 1: replay must not re-invoke the underlying source.
      expect(sourceFn).toHaveBeenCalledTimes(1);
      expect(await listRecordings(dir)).toHaveLength(1);
    });
  });

  describe('keying', () => {
    it('default key strips trailing { signal } so it does not change the recording key', async () => {
      const sourceFn = jest.fn(async function* (k: string, _opts?: { signal?: AbortSignal }): AsyncIterable<string> {
        yield `value-${k}`;
      });
      const rec = withRecordReplay(sourceFn, dir, { mode: 'incrementalRecord' });

      const controller = new AbortController();
      // Phase 1: record using a call that includes { signal }.
      expect(await collect(rec('k', { signal: controller.signal }))).toEqual(['value-k']);
      // Phase 2: replay the same key without { signal } — should still hit,
      // proving signal is stripped from the default key.
      expect(await collect(rec('k'))).toEqual(['value-k']);
      expect(sourceFn).toHaveBeenCalledTimes(1);
      expect(await listRecordings(dir)).toHaveLength(1);
    });

    it('uses custom makeKey when provided', async () => {
      const sourceFn = jest.fn(async function* (obj: { id: number; note: string }): AsyncIterable<string> {
        yield `value-${obj.id}`;
      });
      const rec = withRecordReplay(sourceFn, dir, {
        mode: 'incrementalRecord',
        makeKey: (args) => String(args[0].id),
      });

      // Record id:1 with note 'a', then replay id:1 with note 'b':
      // `note` is intentionally excluded from this custom key.
      expect(await collect(rec({ id: 1, note: 'a' }))).toEqual(['value-1']);
      expect(await collect(rec({ id: 1, note: 'b' }))).toEqual(['value-1']);
      expect(sourceFn).toHaveBeenCalledTimes(1);
      expect(await listRecordings(dir)).toHaveLength(1);
    });
  });

  describe('storage format', () => {
    it('stores a sharded file containing a { key, chunks } envelope', async () => {
      // Example on-disk payload for this case:
      // {
      //   "key": "[\"a\"]",
      //   "chunks": ["value-a-1", "value-a-2"]
      // }
      const rec = withRecordReplay(
        async function* (k: string): AsyncIterable<string> {
          yield `value-${k}-1`;
          yield `value-${k}-2`;
        },
        dir,
        { mode: 'incrementalRecord' }
      );
      await collect(rec('a')); // create recording

      const files = await listRecordings(dir);
      expect(files).toHaveLength(1);

      const file = files[0];
      const shard = path.basename(path.dirname(file));
      const name = path.basename(file);
      const key = JSON.stringify(['a']);
      const expectedName = crypto.createHash('sha256').update(key).digest('hex');
      const expectedShard = expectedName.slice(0, 2);
      expect(shard).toBe(expectedShard);
      expect(name).toBe(expectedName);

      const contents = JSON.parse(await fs.readFile(file, 'utf-8'));
      expect(contents).toEqual({
        key,
        chunks: ['value-a-1', 'value-a-2'],
      });
    });

    it('treats stored-key mismatch vs filename as a replay miss', async () => {
      // Step 1: Record a valid entry for key 'k'.
      const rec = withRecordReplay(
        async function* (k: string): AsyncIterable<string> {
          yield `value-${k}`;
        },
        dir,
        { mode: 'incrementalRecord' }
      );
      await collect(rec('k'));

      // Step 2: Corrupt the stored payload by changing its embedded key.
      const [file] = await listRecordings(dir);
      const recording = JSON.parse(await fs.readFile(file, 'utf-8'));
      await fs.writeFile(file, JSON.stringify({ ...recording, key: 'WRONG' }), 'utf-8');

      // Step 3: Attempt replay for the original key using replay mode.
      const replay = withRecordReplay(neverCalledFn<string>(), dir);

      // Step 4: The key mismatch is treated as a miss (not a replay hit).
      await expect(collect(replay('k'))).rejects.toBeInstanceOf(RecordingNotFoundError);
    });
  });

  describe('validation', () => {
    it('throws at wrap time for an unknown mode', () => {
      expect(() =>
        withRecordReplay(neverCalledFn<string>(), dir, { mode: 'invalid' as any })
      ).toThrow("mode must be one of 'replay', 'record', or 'incrementalRecord'");
    });

    it('throws at wrap time for invalid cacheDir', () => {
      expect(() => withRecordReplay(neverCalledFn<string>(), '')).toThrow('cacheDir must be a non-empty string');
      expect(() => withRecordReplay(neverCalledFn<string>(), '   ')).toThrow('cacheDir must be a non-empty string');
      expect(() => withRecordReplay(neverCalledFn<string>(), undefined as any)).toThrow('cacheDir must be a non-empty string');
    });
  });
});
