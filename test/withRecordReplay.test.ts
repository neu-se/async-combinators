import { withRecordReplay, RecordingNotFoundError } from '../src/withRecordReplay';
import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import { pauseMicrotask } from './helpers';

// A wrapped function that must never be invoked (used to prove replay-mode hits
// and misses don't touch fn). If it ever runs, the thrown error fails the test.
function neverCalledFn() {
  return jest.fn(async (..._args: any[]): Promise<any> => {
    throw new Error('fn should not have been called');
  });
}

// List the recording files under a fixture directory (across the 2-char shard
// dirs), ignoring any leftover temp files.
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

describe('withRecordReplay', () => {
  let root: string;
  let dir: string; // the fixture directory (need not exist until first write)

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), 'record-replay-test-'));
    dir = path.join(root, 'fixtures');
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  describe('replay mode (default)', () => {
    it('throws RecordingNotFoundError (carrying key and cacheDir) on a miss, never calling fn', async () => {
      const replayFn = neverCalledFn();
      const replay = withRecordReplay(replayFn, dir);

      // Capture .rejects once so replay('a') is invoked a single time, then
      // assert both the error's class and its payload against it.
      const rejection = expect(replay('a')).rejects;
      await rejection.toBeInstanceOf(RecordingNotFoundError);
      await rejection.toMatchObject({
        name: 'RecordingNotFoundError',
        key: JSON.stringify(['a']),
        cacheDir: path.resolve(dir),
      });
      expect(replayFn).not.toHaveBeenCalled();
      expect(await listRecordings(dir)).toHaveLength(0); // a replay miss writes nothing
    });

    it('replays a recording written by another wrapper instance, without calling fn', async () => {
      // Seed a recording, then read it back from a fresh replay-mode wrapper.
      const recordFn = jest.fn(async (x: string) => `result-${x}`);
      const rec = withRecordReplay(recordFn, dir, { mode: 'incrementalRecord' });
      await rec('a'); // writes the fixture to disk
      expect(recordFn).toHaveBeenCalledTimes(1); // confirm the fixture was recorded (a miss)

      const replayFn = neverCalledFn();
      const replay = withRecordReplay(replayFn, dir);
      expect(await replay('a')).toBe('result-a');
      expect(replayFn).not.toHaveBeenCalled(); // not invoked but retrieved from fixture
      expect(await listRecordings(dir)).toHaveLength(1); // replay read the fixture without writing
    });
  });

  describe('incrementalRecord mode', () => {
    it('records on a miss, replays on a hit, and records new keys — calling fn once per key', async () => {
      const recordFn = jest.fn(async (x: string) => `result-${x}`);
      const rec = withRecordReplay(recordFn, dir, { mode: 'incrementalRecord' });

      expect(await rec('a')).toBe('result-a'); // records
      expect(await rec('a')).toBe('result-a'); // replays
      expect(await rec('b')).toBe('result-b'); // records a new key
      expect(recordFn).toHaveBeenCalledTimes(2);
      expect(await listRecordings(dir)).toHaveLength(2); // two distinct keys, repeated 'a' not duplicated
    });
  });

  describe('record mode', () => {
    it('clears existing recordings and re-records fresh', async () => {
      const seed = withRecordReplay(jest.fn(async (x: string) => `old-result-${x}`), dir, {
        mode: 'incrementalRecord',
      });
      await seed('a');
      await seed('b');

      const rec = withRecordReplay(jest.fn(async (x: string) => `new-result-${x}`), dir, {
        mode: 'record',
      });
      expect(await rec('a')).toBe('new-result-a'); // fresh value, ignores the old recording

      // The directory was cleared, so only the freshly recorded 'a' is on disk —
      // the seeded 'b' recording is gone.
      expect(await listRecordings(dir)).toHaveLength(1);

      const replay = withRecordReplay(neverCalledFn(), dir);
      expect(await replay('a')).toBe('new-result-a'); // freshly recorded this run
      await expect(replay('b')).rejects.toBeInstanceOf(RecordingNotFoundError); // 'b' was cleared
    });
  });

  describe('cacheErrors', () => {
    it('does not record errors by default; fn runs again on the next call', async () => {
      const recordFn = jest.fn(async (_k: string) => {
        throw new Error('boom');
      });
      const rec = withRecordReplay(recordFn, dir, { mode: 'incrementalRecord' });

      await expect(rec('k')).rejects.toThrow('boom'); // throws; error not recorded
      await expect(rec('k')).rejects.toThrow('boom'); // still a miss, so fn runs again
      expect(recordFn).toHaveBeenCalledTimes(2); // ran both times — nothing was replayed
      expect(await listRecordings(dir)).toHaveLength(0);
    });

    it('records and replays an error when cacheErrors is true', async () => {
      const recordFn = jest.fn(async (_k: string) => {
        throw new Error('boom');
      });
      const rec = withRecordReplay(recordFn, dir, {
        mode: 'incrementalRecord',
        cacheErrors: true,
      });

      await expect(rec('k')).rejects.toThrow('boom'); // records the error
      await expect(rec('k')).rejects.toThrow('boom'); // replays it
      expect(recordFn).toHaveBeenCalledTimes(1);
      expect(await listRecordings(dir)).toHaveLength(1); // the error was recorded

      // A fresh replay wrapper replays the recorded error too.
      const replay = withRecordReplay(neverCalledFn(), dir);
      await expect(replay('k')).rejects.toThrow('boom');

      // On disk: a { key, error } envelope (no value, no __error sentinel).
      const [file] = await listRecordings(dir);
      const contents = JSON.parse(await fs.readFile(file, 'utf-8'));
      expect(contents).toEqual({ key: JSON.stringify(['k']), error: 'boom' });
    });
  });

  describe('makeKey option', () => {
    it('uses a custom key to collapse different arguments to one recording', async () => {
      const recordFn = jest.fn(async (obj: { id: number; note: string }) => `result-${obj.id}`);
      const rec = withRecordReplay(recordFn, dir, {
        mode: 'incrementalRecord',
        makeKey: (args) => String(args[0].id),
      });

      await rec({ id: 1, note: 'a' });
      await rec({ id: 1, note: 'b' }); // same key -> replays
      expect(recordFn).toHaveBeenCalledTimes(1);
      expect(await listRecordings(dir)).toHaveLength(1); // both calls collapsed to one recording
    });
  });

  describe('storage format', () => {
    it('stores a { key, value } envelope under a sharded path', async () => {
      const recordFn = jest.fn(async (x: string) => `result-${x}`);
      const rec = withRecordReplay(recordFn, dir, { mode: 'incrementalRecord' });
      await rec('a');
      expect(recordFn).toHaveBeenCalledTimes(1); // recorded on a miss

      const files = await listRecordings(dir);
      expect(files).toHaveLength(1);

      const file = files[0];
      const shard = path.basename(path.dirname(file));
      const name = path.basename(file);
      expect(shard).toMatch(/^[0-9a-f]{2}$/); // 2-char shard directory
      expect(name).toMatch(/^[0-9a-f]{64}$/); // sha256-hex filename
      expect(name.startsWith(shard)).toBe(true); // shard is the filename's prefix

      const contents = JSON.parse(await fs.readFile(file, 'utf-8'));
      expect(contents).toEqual({ key: JSON.stringify(['a']), value: 'result-a' });
    });

    it('treats a fixture whose stored key mismatches its filename as a miss', async () => {
      const rec = withRecordReplay(jest.fn(async (k: string) => `result-${k}`), dir, {
        mode: 'incrementalRecord',
      });
      await rec('k');

      // Corrupt only the stored key so it no longer matches the filename's hash,
      // as a manual edit or bad migration might; the value is left untouched.
      const [file] = await listRecordings(dir);
      const recording = JSON.parse(await fs.readFile(file, 'utf-8'));
      await fs.writeFile(file, JSON.stringify({ ...recording, key: 'WRONG' }), 'utf-8');

      const replay = withRecordReplay(neverCalledFn(), dir);
      await expect(replay('k')).rejects.toBeInstanceOf(RecordingNotFoundError);
    });

    it('throws when a recording file contains invalid JSON', async () => {
      const rec = withRecordReplay(jest.fn(async (k: string) => `result-${k}`), dir, {
        mode: 'incrementalRecord',
      });
      await rec('k');

      const [file] = await listRecordings(dir);
      await fs.writeFile(file, 'not json', 'utf-8');

      const replay = withRecordReplay(neverCalledFn(), dir);
      await expect(replay('k')).rejects.toThrow('Failed to parse recording');
    });
  });

  describe('concurrency', () => {
    it('handles concurrent identical calls without corrupting the recording', async () => {
      const recordFn = jest.fn(async (x: string) => {
        // Yield one microtask turn so concurrent callers can interleave.
        await pauseMicrotask();
        return `result-${x}`;
      });
      const rec = withRecordReplay(recordFn, dir, { mode: 'incrementalRecord' });

      const results = await Promise.all([rec('k'), rec('k'), rec('k')]);
      expect(results).toEqual(['result-k', 'result-k', 'result-k']);
      expect(recordFn).toHaveBeenCalledTimes(3); // no in-flight dedup: every concurrent call runs

      // Exactly one uncorrupted, replayable recording survives the racing writes.
      expect(await listRecordings(dir)).toHaveLength(1);
      const replay = withRecordReplay(neverCalledFn(), dir);
      expect(await replay('k')).toBe('result-k');
    });
  });

  describe('invalid mode', () => {
    it('throws at wrap time for an unknown mode', () => {
      expect(() =>
        withRecordReplay(neverCalledFn(), dir, { mode: 'invalid' as any })
      ).toThrow("mode must be one of 'replay', 'record', or 'incrementalRecord'");
    });
  });

  describe('invalid cacheDir', () => {
    it('throws at wrap time for an empty string', () => {
      const fn = neverCalledFn();
      expect(() => withRecordReplay(fn, '')).toThrow('cacheDir must be a non-empty string');
      expect(fn).not.toHaveBeenCalled();
    });

    it('throws at wrap time for a whitespace-only string', () => {
      expect(() => withRecordReplay(neverCalledFn(), '   ')).toThrow(
        'cacheDir must be a non-empty string',
      );
    });

    it('throws at wrap time for a non-string', () => {
      expect(() => withRecordReplay(neverCalledFn(), undefined as any)).toThrow(
        'cacheDir must be a non-empty string',
      );
    });
  });
});
