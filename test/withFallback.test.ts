import { withFallback } from '../src/withFallback';
import { pauseMicrotask } from './helpers';

describe('withFallback', () => {
  describe('basic functionality', () => {
    it('should return result from primary function when it succeeds', async () => {
      const primaryFn = jest.fn().mockResolvedValue('primary-result');
      const fallbackFn = jest.fn().mockResolvedValue('fallback-result');
      const wrapped = withFallback(primaryFn, fallbackFn);

      const result = await wrapped('test-arg');

      expect(result).toBe('primary-result');
      expect(primaryFn).toHaveBeenCalledTimes(1);
      expect(primaryFn).toHaveBeenCalledWith('test-arg');
      expect(fallbackFn).not.toHaveBeenCalled();
    });

    it('should return result from fallback function when primary function fails', async () => {
      const error = new Error('Primary function failed');
      const primaryFn = jest.fn().mockRejectedValue(error);
      const fallbackFn = jest.fn().mockResolvedValue('fallback-result');
      const wrapped = withFallback(primaryFn, fallbackFn);

      const result = await wrapped('test-arg');

      expect(result).toBe('fallback-result');
      expect(primaryFn).toHaveBeenCalledTimes(1);
      expect(primaryFn).toHaveBeenCalledWith('test-arg');
      expect(fallbackFn).toHaveBeenCalledTimes(1);
      expect(fallbackFn).toHaveBeenCalledWith('test-arg');
    });

    it('should throw error when both primary and fallback functions fail', async () => {
      const primaryError = new Error('Primary function failed');
      const fallbackError = new Error('Fallback function failed');
      const primaryFn = jest.fn().mockRejectedValue(primaryError);
      const fallbackFn = jest.fn().mockRejectedValue(fallbackError);
      const wrapped = withFallback(primaryFn, fallbackFn);

      await expect(wrapped('test-arg')).rejects.toThrow('Fallback function failed');
      expect(primaryFn).toHaveBeenCalledTimes(1);
      expect(primaryFn).toHaveBeenCalledWith('test-arg');
      expect(fallbackFn).toHaveBeenCalledTimes(1);
      expect(fallbackFn).toHaveBeenCalledWith('test-arg');
    });
  });

  describe('argument handling', () => {
    it('should handle functions with no arguments', async () => {
      const primaryFn = jest.fn().mockResolvedValue('primary-result');
      const fallbackFn = jest.fn().mockResolvedValue('fallback-result');
      const wrapped = withFallback(primaryFn, fallbackFn);

      const result = await wrapped();

      expect(result).toBe('primary-result');
      expect(primaryFn).toHaveBeenCalledWith();
      expect(fallbackFn).not.toHaveBeenCalled();
    });

    it('should handle functions with multiple arguments', async () => {
      const primaryFn = jest.fn().mockResolvedValue('primary-result');
      const fallbackFn = jest.fn().mockResolvedValue('fallback-result');
      const wrapped = withFallback(primaryFn, fallbackFn);

      const result = await wrapped('arg1', 'arg2', 123, { key: 'value' });

      expect(result).toBe('primary-result');
      expect(primaryFn).toHaveBeenCalledWith('arg1', 'arg2', 123, { key: 'value' });
      expect(fallbackFn).not.toHaveBeenCalled();
    });

    it('should pass all arguments to fallback function when primary fails', async () => {
      const error = new Error('Primary failed');
      const primaryFn = jest.fn().mockRejectedValue(error);
      const fallbackFn = jest.fn().mockResolvedValue('fallback-result');
      const wrapped = withFallback(primaryFn, fallbackFn);

      const args = ['arg1', 'arg2', 123, { key: 'value' }, [1, 2, 3]];
      const result = await wrapped(...args);

      expect(result).toBe('fallback-result');
      expect(primaryFn).toHaveBeenCalledWith(...args);
      expect(fallbackFn).toHaveBeenCalledWith(...args);
    });

    it('should handle complex argument types', async () => {
      const primaryFn = jest.fn().mockResolvedValue('success');
      const fallbackFn = jest.fn().mockResolvedValue('backup');
      const wrapped = withFallback(primaryFn, fallbackFn);

      const complexArgs = [
        { nested: { data: [1, 2, 3] } },
        new Date('2023-01-01'),
        null,
        undefined,
        'string',
        42
      ];

      const result = await wrapped(...complexArgs);

      expect(result).toBe('success');
      expect(primaryFn).toHaveBeenCalledWith(...complexArgs);
    });
  });

  describe('error handling', () => {
    it('should handle different error types in primary function', async () => {
      async function testWithError(error: any) {
        const primaryFn = jest.fn().mockRejectedValue(error);
        const fallbackFn = jest.fn().mockResolvedValue('fallback-result');
        const wrapped = withFallback(primaryFn, fallbackFn);

        const result = await wrapped('test');
        expect(result).toBe('fallback-result');
        expect(fallbackFn).toHaveBeenCalledWith('test');
      }

      // Test with Error object
      await testWithError(new Error('Standard error'));
      
      // Test with string error
      await testWithError('String error');
      
      // Test with custom error
      await testWithError({ message: 'Custom error', code: 500 });
      
      // Test with null/undefined
      await testWithError(null);
      await testWithError(undefined);
    });

    it('should handle async errors in both functions', async () => {
      const primaryFn = jest.fn().mockImplementation(async () => {
        // Yield one microtask turn so other queued work can interleave before throwing.
        await pauseMicrotask();
        throw new Error('Async primary error');
      });
      const fallbackFn = jest.fn().mockImplementation(async () => {
        // Yield one microtask turn so fallback remains interleavable without real timers.
        await pauseMicrotask();
        return 'async-fallback-result';
      });
      const wrapped = withFallback(primaryFn, fallbackFn);

      const result = await wrapped('test');

      expect(result).toBe('async-fallback-result');
      expect(primaryFn).toHaveBeenCalledWith('test');
      expect(fallbackFn).toHaveBeenCalledWith('test');
    });

    it('should preserve error from fallback function when both fail', async () => {
      const primaryError = new Error('Primary failed');
      const fallbackError = new Error('Fallback failed with specific message');
      const primaryFn = jest.fn().mockRejectedValue(primaryError);
      const fallbackFn = jest.fn().mockRejectedValue(fallbackError);
      const wrapped = withFallback(primaryFn, fallbackFn);

      await expect(wrapped('test')).rejects.toThrow('Fallback failed with specific message');
    });
  });

  describe('return types', () => {
    it('should handle different return types from primary function', async () => {
      const testCases = [
        'string-result',
        123,
        true,
        false,
        null,
        undefined,
        { object: 'result' },
        ['array', 'result'],
        new Date('2023-01-01')
      ];

      for (const expectedResult of testCases) {
        const primaryFn = jest.fn().mockResolvedValue(expectedResult);
        const fallbackFn = jest.fn().mockResolvedValue('should-not-be-called');
        const wrapped = withFallback(primaryFn, fallbackFn);

        const result = await wrapped();
        expect(result).toBe(expectedResult);
        expect(fallbackFn).not.toHaveBeenCalled();
      }
    });

    it('should handle different return types from fallback function', async () => {
      const testCases = [
        'string-fallback',
        456,
        false,
        null,
        { fallback: 'object' },
        [1, 2, 3]
      ];

      for (const expectedResult of testCases) {
        const primaryFn = jest.fn().mockRejectedValue(new Error('Primary failed'));
        const fallbackFn = jest.fn().mockResolvedValue(expectedResult);
        const wrapped = withFallback(primaryFn, fallbackFn);

        const result = await wrapped();
        expect(result).toBe(expectedResult);
      }
    });

    it('should work with multiple chained fallbacks', async () => {
      // Create a chain: primary -> fallback1 -> fallback2
      const primaryFn = jest.fn().mockRejectedValue(new Error('Primary failed'));
      const fallback1 = jest.fn().mockRejectedValue(new Error('Fallback1 failed'));
      const fallback2 = jest.fn().mockResolvedValue('final-fallback-result');

      const step1 = withFallback(primaryFn, fallback1);
      const finalFallback = withFallback(step1, fallback2);

      const result = await finalFallback('test');

      expect(result).toBe('final-fallback-result');
      expect(primaryFn).toHaveBeenCalledWith('test');
      expect(fallback1).toHaveBeenCalledWith('test');
      expect(fallback2).toHaveBeenCalledWith('test');
    });
  });

  describe('edge cases', () => {
    it('should handle primary function that throws synchronously', async () => {
      const primaryFn = jest.fn().mockImplementation(() => {
        throw new Error('Synchronous error');
      });
      const fallbackFn = jest.fn().mockResolvedValue('fallback-result');
      const wrapped = withFallback(primaryFn, fallbackFn);

      const result = await wrapped('test');

      expect(result).toBe('fallback-result');
      expect(primaryFn).toHaveBeenCalledWith('test');
      expect(fallbackFn).toHaveBeenCalledWith('test');
    });

    it('should handle fallback function that throws synchronously', async () => {
      const primaryFn = jest.fn().mockRejectedValue(new Error('Primary failed'));
      const fallbackFn = jest.fn().mockImplementation(() => {
        throw new Error('Synchronous fallback error');
      });
      const wrapped = withFallback(primaryFn, fallbackFn);

      await expect(wrapped('test')).rejects.toThrow('Synchronous fallback error');
    });

    it('should handle functions with this context', async () => {
      class TestService {
        private value: string = 'service-value';
        
        async primaryMethod() {
          return `primary-${this.value}`;
        }
        
        async fallbackMethod() {
          return `fallback-${this.value}`;
        }
      }

      const service = new TestService();
      const boundPrimary = service.primaryMethod.bind(service);
      const boundFallback = service.fallbackMethod.bind(service);
      const wrapped = withFallback(boundPrimary, boundFallback);

      const result = await wrapped();
      expect(result).toBe('primary-service-value');
    });

    it('should handle very large number of arguments', async () => {
      const largeArgArray = Array.from({ length: 100 }, (_, i) => `arg-${i}`);
      const primaryFn = jest.fn().mockResolvedValue('success-with-many-args');
      const fallbackFn = jest.fn().mockResolvedValue('fallback');
      const wrapped = withFallback(primaryFn, fallbackFn);

      const result = await wrapped(...largeArgArray);

      expect(result).toBe('success-with-many-args');
      expect(primaryFn).toHaveBeenCalledWith(...largeArgArray);
      expect(fallbackFn).not.toHaveBeenCalled();
    });
  });

  describe('invalid arguments', () => {
    it('should throw when fn is not a function', () => {
      expect(() => withFallback(undefined as any, jest.fn())).toThrow(
        'fn and fallbackFn must be functions'
      );
    });

    it('should throw when fallbackFn is not a function', () => {
      expect(() => withFallback(jest.fn(), undefined as any)).toThrow(
        'fn and fallbackFn must be functions'
      );
    });

    it('should throw at wrap time, before either function is called', () => {
      const fn = jest.fn();
      expect(() => withFallback(fn, 'not-a-fn' as any)).toThrow();
      expect(fn).not.toHaveBeenCalled();
    });
  });
});