import { withRetry } from '../src/withRetry';
import { expectAbortError } from './helpers';

describe('withRetry', () => {
  it('should return result on first success', async () => {
    const fn = jest.fn().mockResolvedValue('success');
    const retryFn = withRetry(fn, 3);
    const result = await retryFn('arg1', 'arg2');
    expect(result).toBe('success');
    expect(fn).toHaveBeenCalledTimes(1);
    expect(fn).toHaveBeenCalledWith('arg1', 'arg2');
  });

  it('should retry on failure and succeed', async () => {
    const fn = jest.fn()
      .mockRejectedValueOnce(new Error('fail1'))
      .mockRejectedValueOnce(new Error('fail2'))
      .mockResolvedValue('success');
    const retryFn = withRetry(fn, 3);
    const result = await retryFn();
    expect(result).toBe('success');
    expect(fn).toHaveBeenCalledTimes(3);
  });

  it('should throw final error after all attempts fail', async () => {
    const error1 = new Error('fail1');
    const error2 = new Error('fail2');
    const finalError = new Error('final fail');
    const fn = jest.fn()
      .mockRejectedValueOnce(error1)
      .mockRejectedValueOnce(error2)
      .mockRejectedValue(finalError);
    const retryFn = withRetry(fn, 3);
    await expect(retryFn()).rejects.toThrow('final fail');
    expect(fn).toHaveBeenCalledTimes(3);
  });

  it('should call notify function before each retry', async () => {
    const error1 = new Error('fail1');
    const error2 = new Error('fail2');
    const fn = jest.fn()
      .mockRejectedValueOnce(error1)
      .mockRejectedValueOnce(error2)
      .mockResolvedValue('success');
    const notifyFn = jest.fn();
    const retryFn = withRetry(fn, 3, { onRetry: notifyFn });
    const result = await retryFn();
    expect(result).toBe('success');
    expect(notifyFn).toHaveBeenCalledTimes(2);
    expect(notifyFn).toHaveBeenNthCalledWith(1, 1, error1); // attempt 1 failed with error1
    expect(notifyFn).toHaveBeenNthCalledWith(2, 2, error2); // attempt 2 failed with error2
  });

  it('should not call notify function on first attempt or success', async () => {
    const fn = jest.fn().mockResolvedValue('success');
    const notifyFn = jest.fn();
    const retryFn = withRetry(fn, 3, { onRetry: notifyFn });
    await retryFn();
    expect(notifyFn).not.toHaveBeenCalled();
  });

  it('should work with single attempt', async () => {
    const fn = jest.fn().mockResolvedValue('success');
    const retryFn = withRetry(fn, 1);
    const result = await retryFn();
    expect(result).toBe('success');
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('should throw immediately with single attempt on failure', async () => {
    const error = new Error('fail');
    const fn = jest.fn().mockRejectedValue(error);
    const notifyFn = jest.fn();
    const retryFn = withRetry(fn, 1, { onRetry: notifyFn });
    await expect(retryFn()).rejects.toThrow('fail');
    expect(fn).toHaveBeenCalledTimes(1);
    expect(notifyFn).not.toHaveBeenCalled();
  });

  it('should preserve function arguments across retries', async () => {
    const fn = jest.fn()
      .mockRejectedValueOnce(new Error('fail'))
      .mockResolvedValue('success');
    const retryFn = withRetry(fn, 2);
    await retryFn('arg1', 42, { key: 'value' });
    expect(fn).toHaveBeenCalledTimes(2);
    expect(fn).toHaveBeenNthCalledWith(1, 'arg1', 42, { key: 'value' });
    expect(fn).toHaveBeenNthCalledWith(2, 'arg1', 42, { key: 'value' });
  });

  it('should call notify function with correct parameters', async () => {
    const fn = jest.fn()
      .mockRejectedValueOnce(new Error('fail'))
      .mockResolvedValue('success');
    const notifyFn = jest.fn();
    const retryFn = withRetry(fn, 2, { onRetry: notifyFn });  
    await retryFn();
    expect(notifyFn).toHaveBeenCalledWith(1, expect.any(Error)); // attempt 1 failed
  });

  it('should handle different error types', async () => {
    const stringError = 'string error';
    const numberError = 404;
    const objectError = { message: 'object error' };
    const fn = jest.fn()
      .mockRejectedValueOnce(stringError)
      .mockRejectedValueOnce(numberError)
      .mockRejectedValue(objectError);
    const notifyFn = jest.fn();
    const retryFn = withRetry(fn, 3, { onRetry: notifyFn });
    await expect(retryFn()).rejects.toBe(objectError);
    expect(notifyFn).toHaveBeenNthCalledWith(1, 1, stringError); // attempt 1 failed
    expect(notifyFn).toHaveBeenNthCalledWith(2, 2, numberError); // attempt 2 failed
  });

  describe('invalid maxAttempts', () => {
    it('should throw for zero', () => {
      expect(() => withRetry(jest.fn(), 0)).toThrow(
        'maxAttempts must be a positive integer'
      );
    });

    it('should throw for negative values', () => {
      expect(() => withRetry(jest.fn(), -1)).toThrow(
        'maxAttempts must be a positive integer'
      );
    });

    it('should throw for non-integer values', () => {
      expect(() => withRetry(jest.fn(), 2.5)).toThrow(
        'maxAttempts must be a positive integer'
      );
    });

    it('should throw for NaN and Infinity', () => {
      expect(() => withRetry(jest.fn(), NaN)).toThrow(
        'maxAttempts must be a positive integer'
      );
      expect(() => withRetry(jest.fn(), Infinity)).toThrow(
        'maxAttempts must be a positive integer'
      );
    });

    it('should throw at wrap time, before any call is made', () => {
      const fn = jest.fn();
      expect(() => withRetry(fn, 0)).toThrow();
      expect(fn).not.toHaveBeenCalled();
    });
  });

  describe('backoff', () => {
    beforeEach(() => jest.useFakeTimers());
    afterEach(() => jest.useRealTimers());

    it('waits a constant delayMs between attempts (fixed backoff)', async () => {
      const fn = jest.fn()
        .mockRejectedValueOnce(new Error('f1'))
        .mockRejectedValueOnce(new Error('f2'))
        .mockResolvedValue('ok');
      const retryFn = withRetry(fn, 3, { delayMs: 100, backoff: 'fixed' });

      const p = retryFn();
      await jest.advanceTimersByTimeAsync(0); // attempt 1 runs and fails → now waiting
      expect(fn).toHaveBeenCalledTimes(1);

      await jest.advanceTimersByTimeAsync(99);
      expect(fn).toHaveBeenCalledTimes(1); // delay not elapsed
      await jest.advanceTimersByTimeAsync(1);
      expect(fn).toHaveBeenCalledTimes(2); // attempt 2 at 100ms

      await jest.advanceTimersByTimeAsync(100);
      expect(fn).toHaveBeenCalledTimes(3); // fixed: another 100ms before attempt 3

      await expect(p).resolves.toBe('ok');
    });

    it('grows the delay exponentially: delayMs * factor^(k-1)', async () => {
      const fn = jest.fn()
        .mockRejectedValueOnce(new Error('f1'))
        .mockRejectedValueOnce(new Error('f2'))
        .mockRejectedValueOnce(new Error('f3'))
        .mockRejectedValueOnce(new Error('f4'))
        .mockResolvedValue('ok');
      const retryFn = withRetry(fn, 5, { delayMs: 100, backoff: 'exponential', factor: 2 });

      const p = retryFn();
      await jest.advanceTimersByTimeAsync(0);
      expect(fn).toHaveBeenCalledTimes(1); // attempt 1 ran immediately

      // Each retry waits twice as long as the previous one, but adds exactly one call.
      let calls = 1;
      for (const delay of [100, 200, 400, 800]) {
        await jest.advanceTimersByTimeAsync(delay - 1);
        expect(fn).toHaveBeenCalledTimes(calls);   // the (doubling) delay hasn't elapsed yet
        await jest.advanceTimersByTimeAsync(1);
        expect(fn).toHaveBeenCalledTimes(++calls);  // ...now the next attempt fires
      }

      await expect(p).resolves.toBe('ok');
    });

    it('caps the computed delay at maxDelayMs', async () => {
      const fn = jest.fn()
        .mockRejectedValueOnce(new Error('f1'))
        .mockRejectedValueOnce(new Error('f2'))
        .mockResolvedValue('ok');
      // exponential would be 100 then 200, but capped at 150.
      const retryFn = withRetry(fn, 3, { delayMs: 100, backoff: 'exponential', factor: 2, maxDelayMs: 150 });

      const p = retryFn();
      await jest.advanceTimersByTimeAsync(0);
      await jest.advanceTimersByTimeAsync(100); // min(100, 150) = 100 → attempt 2
      expect(fn).toHaveBeenCalledTimes(2);
      await jest.advanceTimersByTimeAsync(150); // min(200, 150) = 150 → attempt 3
      expect(fn).toHaveBeenCalledTimes(3);

      await expect(p).resolves.toBe('ok');
    });

    it('abandons a pending backoff wait when the inbound signal aborts', async () => {
      // fn keeps failing with an ordinary (non-cancellation) error, so each attempt is
      // followed by a backoff — that's simply how we get *into* a backoff to abort.
      const fn = jest.fn(async (_opts?: { signal?: AbortSignal }) => { throw new Error('transient failure'); });
      const retryFn = withRetry(fn, 5, { delayMs: 100 }); // up to 5 attempts
      const controller = new AbortController();

      const p = retryFn({ signal: controller.signal });

      // After attempt 1's failure we're waiting in the still-pending 100ms backoff.
      await jest.advanceTimersByTimeAsync(0);
      expect(fn).toHaveBeenCalledTimes(1); // one call, which led to a transient failure

      // The one abort — landing mid-backoff, before the retry fires — rejects with the
      // abort reason and skips the remaining 4 attempts.
      controller.abort();
      await expectAbortError(p);
      expect(fn).toHaveBeenCalledTimes(1); // still one call, because the retry was aborted
    });
  });

  describe('invalid backoff options', () => {
    it('throws for a negative delayMs', () => {
      expect(() => withRetry(jest.fn(), 3, { delayMs: -1 })).toThrow(
        'delayMs must be a non-negative number'
      );
    });

    it('throws for a non-finite delayMs', () => {
      expect(() => withRetry(jest.fn(), 3, { delayMs: Infinity })).toThrow(
        'delayMs must be a non-negative number'
      );
    });

    it('throws for an unknown backoff strategy', () => {
      expect(() => withRetry(jest.fn(), 3, { backoff: 'linear' as any })).toThrow(
        "backoff must be 'fixed' or 'exponential'"
      );
    });

    it('throws for factor < 1', () => {
      expect(() => withRetry(jest.fn(), 3, { factor: 0.5 })).toThrow(
        'factor must be a finite number >= 1'
      );
    });

    it('throws for a non-positive maxDelayMs', () => {
      expect(() => withRetry(jest.fn(), 3, { maxDelayMs: 0 })).toThrow(
        'maxDelayMs must be a positive number'
      );
    });

    it('throws at wrap time, before any call is made', () => {
      const fn = jest.fn();
      expect(() => withRetry(fn, 3, { delayMs: -1 })).toThrow();
      expect(fn).not.toHaveBeenCalled();
    });
  });
});