import { withTimeout } from '../src/withTimeout';
import { abortedSignal, expectAbortError, stall } from './helpers';

describe('withTimeout', () => {
  beforeEach(() => {
    jest.clearAllTimers();
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.runOnlyPendingTimers();
    jest.useRealTimers();
  });

  it('should return result when function completes within timeout', async () => {
    // Result encodes the args → checks both pass-through and forwarding at once.
    const fn = async (a: string, b: string) => `${a}-${b}`;
    const timeoutFn = withTimeout(fn, 1000);

    await expect(timeoutFn('arg1', 'arg2')).resolves.toBe('arg1-arg2');
  });

  it('should timeout when function takes too long', async () => {
    const fn = async () => {
      // Keep work pending so withTimeout drives the timeout path.
      await stall();
      return 'late';
    };
    const timeoutFn = withTimeout(fn, 100);

    // Set up the rejection assertion before advancing virtual time.
    const timedOut = expect(timeoutFn()).rejects.toThrow('Operation timed out');
    // Move virtual time forward to trigger the 100ms timeout deterministically.
    await jest.advanceTimersByTimeAsync(100);
    // Enforce the assertion once the timeout has fired.
    await timedOut;
  });

  it('should handle function that throws immediately', async () => {
    const error = new Error('immediate error');
    const fn = async () => { throw error; };
    const timeoutFn = withTimeout(fn, 1000);

    await expect(timeoutFn()).rejects.toThrow('immediate error');
  });

  it('should timeout even if function eventually throws', async () => {
    const fn = async () => {
      // Keep work pending so withTimeout still wins before any eventual error path.
      await stall();
      throw new Error('late error');
    };
    const timeoutFn = withTimeout(fn, 100);

    // Set up the timeout assertion before advancing virtual time.
    const timedOut = expect(timeoutFn()).rejects.toThrow('Operation timed out');
    // Advance virtual time to fire the timeout before fn reaches its late throw.
    await jest.advanceTimersByTimeAsync(100);
    // Enforce the assertion after the timeout has been triggered.
    await timedOut;
  });

  it('should not apply a timeout when maxDurationMs is Infinity', () => {
    // Timeout disabled → fn is returned unchanged (zero overhead).
    const fn = async () => 'result';
    expect(withTimeout(fn, Infinity)).toBe(fn);
  });

  // timeoutError accepts any value (typed `unknown`): a custom Error here, a
  // non-Error value in the next test.
  it('should use custom timeout error when provided', async () => {
    const customError = new Error('Custom timeout message');
    const fn = async () => {
      // Keep work pending so withTimeout drives the custom timeout path.
      await stall();
      return 'result';
    };
    const timeoutFn = withTimeout(fn, 100, customError);

    // Set up the custom-timeout assertion before advancing virtual time.
    const timedOut = expect(timeoutFn()).rejects.toThrow('Custom timeout message');
    // Advance virtual time to deterministically trigger the 100ms timeout.
    await jest.advanceTimersByTimeAsync(100);
    // Enforce the assertion after the timeout path has executed.
    await timedOut;
  });

  it('should handle non-Error timeout values', async () => {
    const fn = async () => {
      // Keep work pending so withTimeout drives the timeout-value path.
      await stall();
      return 'result';
    };
    const timeoutFn = withTimeout(fn, 100, 'Custom timeout string');

    // Set up the non-Error timeout assertion before advancing virtual time.
    const timedOut = expect(timeoutFn()).rejects.toBe('Custom timeout string');
    // Advance virtual time so the 100ms timeout path runs deterministically.
    await jest.advanceTimersByTimeAsync(100);
    // Enforce the timeout value assertion.
    await timedOut;
  });

  describe('invalid maxDurationMs', () => {
    it('should throw for zero', () => {
      expect(() => withTimeout(jest.fn(), 0)).toThrow(
        'maxDurationMs must be a positive integer or Infinity'
      );
    });

    it('should throw for negative values', () => {
      expect(() => withTimeout(jest.fn(), -1)).toThrow(
        'maxDurationMs must be a positive integer or Infinity'
      );
    });

    it('should throw for non-integer values', () => {
      expect(() => withTimeout(jest.fn(), 1.5)).toThrow(
        'maxDurationMs must be a positive integer or Infinity'
      );
    });

    it('should throw for NaN', () => {
      expect(() => withTimeout(jest.fn(), NaN)).toThrow(
        'maxDurationMs must be a positive integer or Infinity'
      );
    });

    it('should throw at wrap time, before any call is made', () => {
      const fn = jest.fn();
      expect(() => withTimeout(fn, 0)).toThrow();
      expect(fn).not.toHaveBeenCalled();
    });
  });

  describe('cancellation', () => {
    it('rejects with the abort reason when the signal fires before the timeout', async () => {
      const controller = new AbortController();
      // fn declares a trailing { signal } option (it need not use it), which is
      // the opt-in that lets the caller pass a signal and withTimeout honor it.
      const fn = jest.fn(async (_opts?: { signal?: AbortSignal }) => {
        // Keep work pending without scheduling a real timer; abort should still win the race.
        await stall();
        return 'late';
      });
      const wrapped = withTimeout(fn, 500);

      const promise = wrapped({ signal: controller.signal });
      controller.abort(); // abort immediately
      // The abort dispatches synchronously, so its rejection lands on the next
      // microtask — ahead of fn's still-pending work and the 500ms timeout — so it wins
      // the race and the call rejects with the abort reason.
      await expectAbortError(promise);
      // fn WAS invoked here — the abort won the race mid-flight (unlike the
      // already-aborted case, where the entry poll skips fn entirely).
      expect(fn).toHaveBeenCalledTimes(1);
    });

    it('rejects immediately if the signal is already aborted, without calling fn', async () => {
      const fn = jest.fn(async (_opts?: { signal?: AbortSignal }) => 'result');
      const wrapped = withTimeout(fn, 500);

      const promise = wrapped({ signal: abortedSignal() }); // already aborted before the call
      await expectAbortError(promise);
      // The entry poll (throwIfAborted) rejects before fn is ever invoked.
      expect(fn).not.toHaveBeenCalled();
    });
  });
});
