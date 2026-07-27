import { withCache } from '../src/withCache';
import { pauseMicrotask } from './helpers';

describe('withCache', () => {
  it('should call function on first invocation', async () => {
    const fn = jest.fn().mockResolvedValue('result');
    const cachedFn = withCache(fn);

    const result = await cachedFn('arg1', 'arg2');
    
    expect(result).toBe('result');
    expect(fn).toHaveBeenCalledTimes(1);
    expect(fn).toHaveBeenCalledWith('arg1', 'arg2');
  });

  it('should return cached result on second call with same arguments', async () => {
    const fn = jest.fn().mockResolvedValue('result');
    const cachedFn = withCache(fn);

    const result1 = await cachedFn('arg1', 'arg2');
    const result2 = await cachedFn('arg1', 'arg2');

    expect(result1).toBe('result');
    expect(result2).toBe('result');
    expect(fn).toHaveBeenCalledTimes(1); // Only called once
  });

  it('should call function again if arguments differ', async () => {
    const fn = jest.fn()
      .mockResolvedValueOnce('result1')
      .mockResolvedValueOnce('result2');
    const cachedFn = withCache(fn);
    
    const result1 = await cachedFn('arg1');
    const result2 = await cachedFn('arg2');
    
    expect(result1).toBe('result1');
    expect(result2).toBe('result2');
    expect(fn).toHaveBeenCalledTimes(2);
    expect(fn).toHaveBeenNthCalledWith(1, 'arg1');
    expect(fn).toHaveBeenNthCalledWith(2, 'arg2');
  });

  it('should handle concurrent calls with same arguments', async () => {
    let callCount = 0;
    const fn = jest.fn().mockImplementation(async () => {
      callCount++;
      // Yield one microtask so sibling calls can interleave onto the same in-flight promise.
      await pauseMicrotask();
      return `result-${callCount}`;
    });
    const cachedFn = withCache(fn);
    
    // Make concurrent calls with same arguments
    const [result1, result2, result3] = await Promise.all([
      cachedFn('same-arg'),
      cachedFn('same-arg'),
      cachedFn('same-arg')
    ]);
    
    // All should return the same result from the single function call
    expect(result1).toBe('result-1');
    expect(result2).toBe('result-1');
    expect(result3).toBe('result-1');
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('should handle composite/structured argument types', async () => {
    const fn = jest.fn().mockImplementation(async (...args) => `result-${JSON.stringify(args)}`);
    const cachedFn = withCache(fn);
    
    const result1 = await cachedFn('string', 42, true, { key: 'value' });
    const result2 = await cachedFn('string', 42, true, { key: 'value' });
    
    expect(result1).toBe(result2);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('should distinguish between different argument orders', async () => {
    const fn = jest.fn()
      .mockResolvedValueOnce('result1')
      .mockResolvedValueOnce('result2');
    const cachedFn = withCache(fn);
    
    const result1 = await cachedFn('a', 'b');
    const result2 = await cachedFn('b', 'a');
    
    expect(result1).toBe('result1');
    expect(result2).toBe('result2');
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it('should handle functions with no arguments', async () => {
    const fn = jest.fn().mockResolvedValue('no-args-result');
    const cachedFn = withCache(fn);
    
    const result1 = await cachedFn();
    const result2 = await cachedFn();
    
    expect(result1).toBe('no-args-result');
    expect(result2).toBe('no-args-result');
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('should not cache rejected promises by default', async () => {
    const error = new Error('test error');
    const fn = jest.fn()
      .mockRejectedValueOnce(error)
      .mockResolvedValueOnce('success');
    const cachedFn = withCache(fn);
    
    // First call fails
    await expect(cachedFn('arg')).rejects.toThrow('test error');
    expect(fn).toHaveBeenCalledTimes(1);
    
    // Second call with same args should retry (not cached)
    const result = await cachedFn('arg');
    expect(result).toBe('success');
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it('should cache rejected promises when cacheErrors is true', async () => {
    const error = new Error('test error');
    const fn = jest.fn().mockRejectedValue(error);
    const cachedFn = withCache(fn, { cacheErrors: true });
    
    await expect(cachedFn('arg')).rejects.toThrow('test error');
    await expect(cachedFn('arg')).rejects.toThrow('test error');
    
    expect(fn).toHaveBeenCalledTimes(1); // Error is cached
  });

  it('should handle complex nested objects as arguments', async () => {
    const fn = jest.fn().mockResolvedValue('complex-result');
    const cachedFn = withCache(fn);
    
    const complexArg = {
      nested: { array: [1, 2, { deep: 'value' }] },
      nullValue: null,
      date: '2023-01-01'
    };
    
    const result1 = await cachedFn(complexArg);
    const result2 = await cachedFn(complexArg);
    
    expect(result1).toBe('complex-result');
    expect(result2).toBe('complex-result');
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('should treat undefined and missing arguments differently', async () => {
    const fn = jest.fn()
      .mockResolvedValueOnce('with-undefined')
      .mockResolvedValueOnce('without-args');
    const cachedFn = withCache(fn);
    
    // These create different cache keys: "[null]" vs "[]"
    const result1 = await cachedFn(undefined); // Explicit undefined argument
    const result2 = await cachedFn();          // No arguments
    
    expect(result1).toBe('with-undefined');
    expect(result2).toBe('without-args');
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it('should maintain separate caches for different wrapped functions', async () => {
    const fn1 = jest.fn().mockResolvedValue('result1');
    const fn2 = jest.fn().mockResolvedValue('result2');
    const cachedFn1 = withCache(fn1);
    const cachedFn2 = withCache(fn2);
    
    await cachedFn1('same-arg');
    await cachedFn2('same-arg');
    await cachedFn1('same-arg'); // Should use cache
    await cachedFn2('same-arg'); // Should use cache
    
    expect(fn1).toHaveBeenCalledTimes(1);
    expect(fn2).toHaveBeenCalledTimes(1);
  });

  it('should maintain separate caches when the same function is wrapped twice', async () => {
    const fn = jest.fn().mockResolvedValue('result');

    // Same underlying function, wrapped twice → two independent caches.
    const cachedA = withCache(fn);
    const cachedB = withCache(fn);

    await cachedA('x'); // fn call 1
    await cachedB('x'); // separate cache → fn call 2 (a shared cache would skip this)
    expect(fn).toHaveBeenCalledTimes(2);

    // Each wrapper serves its own cache on repeat — no further calls.
    await cachedA('x');
    await cachedB('x');
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it('should handle double caching (cache of cache)', async () => {
    const fn = jest.fn().mockResolvedValue('result');
    const cachedFn = withCache(fn);
    const doublyCachedFn = withCache(cachedFn);
    
    // First call goes through both cache layers to original function
    const result1 = await doublyCachedFn('arg');
    expect(result1).toBe('result');
    expect(fn).toHaveBeenCalledTimes(1);
    
    // Second call should be cached at the outer layer
    const result2 = await doublyCachedFn('arg');
    expect(result2).toBe('result');
    expect(fn).toHaveBeenCalledTimes(1); // Still only called once
    
    // Different args should still work
    const result3 = await doublyCachedFn('different-arg');
    expect(result3).toBe('result');
    expect(fn).toHaveBeenCalledTimes(2); // Called again for different args
  });

  it('should use custom key generator when provided', async () => {
    const fn = jest.fn().mockResolvedValue('result');
    const makeKey = jest.fn((args) => `custom-${args[0].id}`);
    const cachedFn = withCache(fn, { makeKey });
    
    const obj1 = { id: 'user1', name: 'Alice' };
    const obj2 = { id: 'user1', name: 'Bob' }; // Same id, different name
    
    const result1 = await cachedFn(obj1);
    const result2 = await cachedFn(obj2);
    
    expect(result1).toBe('result');
    expect(result2).toBe('result');
    expect(fn).toHaveBeenCalledTimes(1); // Only called once due to same key
    expect(makeKey).toHaveBeenCalledTimes(2);
    expect(makeKey).toHaveBeenCalledWith([obj1]);
    expect(makeKey).toHaveBeenCalledWith([obj2]);
  });

  it('should distinguish different keys with custom key generator', async () => {
    const fn = jest.fn()
      .mockResolvedValueOnce('result1')
      .mockResolvedValueOnce('result2');
    const makeKey = (args: any[]) => `user-${args[0].id}`;
    const cachedFn = withCache(fn, { makeKey });
    
    const result1 = await cachedFn({ id: 'user1', data: 'anything' });
    const result2 = await cachedFn({ id: 'user2', data: 'anything' });
    
    expect(result1).toBe('result1');
    expect(result2).toBe('result2');
    expect(fn).toHaveBeenCalledTimes(2); // Called twice for different keys
  });

  it('should work with custom key generator and cacheErrors option', async () => {
    const error = new Error('test error');
    const fn = jest.fn().mockRejectedValue(error);
    const makeKey = (args: any[]) => `error-${args[0]}`;
    const cachedFn = withCache(fn, { makeKey, cacheErrors: true });
    
    await expect(cachedFn('test')).rejects.toThrow('test error');
    await expect(cachedFn('test')).rejects.toThrow('test error');
    
    expect(fn).toHaveBeenCalledTimes(1); // Error cached with custom key
  });

  it('should handle intentional key collisions with rounding', async () => {
    const fn = jest.fn()
      .mockResolvedValueOnce('result-for-1')
      .mockResolvedValueOnce('result-for-2'); // This won't be used due to collision
    
    // Round numbers down - 1.1 and 1.2 both become "1"
    const makeKey = (args: any[]) => Math.floor(args[0]).toString();
    const cachedFn = withCache(fn, { makeKey });
    
    const result1 = await cachedFn(1.1);
    const result2 = await cachedFn(1.2); // Should return cached result from 1.1
    
    expect(result1).toBe('result-for-1');
    expect(result2).toBe('result-for-1'); // Same result due to collision
    expect(fn).toHaveBeenCalledTimes(1); // Only called once
    expect(fn).toHaveBeenCalledWith(1.1); // Called with first argument only
  });

  describe('maxSize / LRU eviction', () => {
    it('should evict the least-recently-used entry when maxSize is exceeded', async () => {
      const fn = jest.fn().mockImplementation(async (x: number) => `result-${x}`);
      const cachedFn = withCache(fn, { maxSize: 2 });

      await cachedFn(1); // cache: [1]
      await cachedFn(2); // cache: [1, 2]
      await cachedFn(3); // inserts 3, evicts LRU (1) → cache: [2, 3]
      expect(fn).toHaveBeenCalledTimes(3);

      await cachedFn(2); // hit
      await cachedFn(3); // hit
      expect(fn).toHaveBeenCalledTimes(3); // both still cached

      await cachedFn(1); // was evicted → recomputed
      expect(fn).toHaveBeenCalledTimes(4);
    });

    it('should treat a cache hit as most-recently-used', async () => {
      const fn = jest.fn().mockImplementation(async (x: number) => `result-${x}`);
      const cachedFn = withCache(fn, { maxSize: 2 });

      await cachedFn(1); // [1]
      await cachedFn(2); // [1, 2]
      await cachedFn(1); // hit → promotes 1 → order [2, 1]
      expect(fn).toHaveBeenCalledTimes(2);

      await cachedFn(3); // inserts 3, evicts LRU (2) → [1, 3]
      expect(fn).toHaveBeenCalledTimes(3);

      await cachedFn(1); // still cached (was promoted) → no recompute
      expect(fn).toHaveBeenCalledTimes(3);

      await cachedFn(2); // was evicted → recomputed
      expect(fn).toHaveBeenCalledTimes(4);
    });

    it('should keep only one entry when maxSize is 1', async () => {
      const fn = jest.fn().mockImplementation(async (x: number) => `result-${x}`);
      const cachedFn = withCache(fn, { maxSize: 1 });

      await cachedFn(1);
      await cachedFn(2); // evicts 1
      expect(fn).toHaveBeenCalledTimes(2);

      await cachedFn(2); // hit
      expect(fn).toHaveBeenCalledTimes(2);

      await cachedFn(1); // was evicted → recomputed
      expect(fn).toHaveBeenCalledTimes(3);
    });

    it('should not evict when no maxSize is set (unbounded)', async () => {
      const fn = jest.fn().mockImplementation(async (x: number) => `result-${x}`);
      const cachedFn = withCache(fn);

      for (let i = 0; i < 50; i++) await cachedFn(i);
      expect(fn).toHaveBeenCalledTimes(50);

      // All 50 still cached — no recomputation.
      for (let i = 0; i < 50; i++) await cachedFn(i);
      expect(fn).toHaveBeenCalledTimes(50);
    });
  });

  describe('invalid maxSize', () => {
    it('should throw for zero', () => {
      expect(() => withCache(jest.fn(), { maxSize: 0 })).toThrow(
        'maxSize must be a positive integer'
      );
    });

    it('should throw for negative values', () => {
      expect(() => withCache(jest.fn(), { maxSize: -1 })).toThrow(
        'maxSize must be a positive integer'
      );
    });

    it('should throw for non-integer values', () => {
      expect(() => withCache(jest.fn(), { maxSize: 2.5 })).toThrow(
        'maxSize must be a positive integer'
      );
    });

    it('should throw at wrap time, before any call is made', () => {
      const fn = jest.fn();
      expect(() => withCache(fn, { maxSize: 0 })).toThrow();
      expect(fn).not.toHaveBeenCalled();
    });
  });
});