import { describe, expect, it, vi } from "vitest";
import { settlePhotonOperation } from "./bounded-operation.js";

function deferred<T>(): {
  promise: Promise<T>;
  reject(error: unknown): void;
} {
  let rejectPromise: ((error: unknown) => void) | undefined;
  const promise = new Promise<T>((_resolve, reject) => { rejectPromise = reject; });
  return { promise, reject: (error) => rejectPromise?.(error) };
}

describe("bounded Photon operations", () => {
  it("observes a delayed rejection when the signal was already aborted", async () => {
    const abort = new AbortController();
    abort.abort();
    const pending = deferred<void>();
    const unhandled = vi.fn();
    process.on("unhandledRejection", unhandled);
    try {
      await expect(settlePhotonOperation(pending.promise, "delayed", abort.signal))
        .rejects.toThrow("Photon delayed aborted");
      pending.reject(new Error("late rejection"));
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(unhandled).not.toHaveBeenCalled();
    } finally {
      process.off("unhandledRejection", unhandled);
    }
  });
});
