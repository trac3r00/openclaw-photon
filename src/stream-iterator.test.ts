import { afterEach, describe, expect, it, vi } from "vitest";
import { nextUntilRevoked } from "./stream-iterator.js";

function deferred<T>(): { promise: Promise<T>; resolve(value: T): void } {
  let settle: ((value: T) => void) | undefined;
  const promise = new Promise<T>((resolve) => { settle = resolve; });
  return { promise, resolve: (value) => settle?.(value) };
}

describe("revoked Photon stream iterator", () => {
  afterEach(() => vi.useRealTimers());
  it("calls return and journals a late next result after abort", async () => {
    const pending = deferred<IteratorResult<string>>();
    const journaled = deferred<void>();
    const iterator: AsyncIterator<string> = {
      next: vi.fn(async () => await pending.promise),
      return: vi.fn(async () => ({ done: true as const, value: undefined })),
    };
    const abort = new AbortController();
    const onLate = vi.fn(async () => { journaled.resolve(); });
    const reading = nextUntilRevoked(iterator, abort.signal, onLate);

    abort.abort();
    await expect(reading).resolves.toBeUndefined();
    expect(iterator.return).toHaveBeenCalledOnce();
    pending.resolve({ done: false, value: "late-event" });
    await journaled.promise;

    expect(onLate).toHaveBeenCalledWith("late-event");
  });

  it("reports and retries a failed late-value journal handoff", async () => {
    const pending = deferred<IteratorResult<string>>();
    const reported = deferred<void>();
    const iterator: AsyncIterator<string> = {
      next: vi.fn(async () => await pending.promise),
      return: vi.fn(async () => ({ done: true as const, value: undefined })),
    };
    const abort = new AbortController();
    const onLate = vi.fn()
      .mockRejectedValueOnce(new Error("projectSecret=secret"))
      .mockResolvedValueOnce(undefined);
    const onLateError = vi.fn(() => { reported.resolve(); });
    const reading = nextUntilRevoked(iterator, abort.signal, onLate, onLateError);

    abort.abort();
    await reading;
    pending.resolve({ done: false, value: "late-event" });
    await reported.promise;
    await vi.waitFor(() => expect(onLate).toHaveBeenCalledTimes(2));
    expect(onLateError).toHaveBeenCalledWith(expect.any(Error), "late-event");
  });

  it("bounds a hung late-value handoff, reports it, and settles", async () => {
    vi.useFakeTimers();
    const pending = deferred<IteratorResult<string>>();
    const iterator: AsyncIterator<string> = {
      next: vi.fn(async () => await pending.promise),
      return: vi.fn(async () => ({ done: true as const, value: undefined })),
    };
    const abort = new AbortController();
    const onLate = vi.fn(async () => await new Promise<void>(() => undefined));
    const onLateError = vi.fn();
    const reading = nextUntilRevoked(iterator, abort.signal, onLate, onLateError);

    abort.abort();
    pending.resolve({ done: false, value: "late-event" });
    await Promise.resolve();
    await vi.advanceTimersByTimeAsync(1_000);
    await vi.advanceTimersByTimeAsync(1_000);

    await expect(reading).resolves.toBeUndefined();
    expect(onLate).toHaveBeenCalledTimes(2);
    expect(onLateError).toHaveBeenCalledTimes(2);
    expect(onLateError).toHaveBeenLastCalledWith(
      expect.objectContaining({ message: expect.stringContaining("timed out") }),
      "late-event",
    );
  });
});
