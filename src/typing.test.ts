import { afterEach, describe, expect, it, vi } from "vitest";
import { PhotonTypingController, withPhotonTyping } from "./typing.js";
import type { PhotonSpace } from "./transport.js";

function deferred(): { promise: Promise<void>; resolve(): void } {
  let resolvePromise: (() => void) | undefined;
  const promise = new Promise<void>((resolve) => { resolvePromise = resolve; });
  return { promise, resolve: () => resolvePromise?.() };
}

describe("Photon typing lifecycle", () => {
  afterEach(() => vi.useRealTimers());

  it("coalesces overlapping reply and heartbeat starts, then stops and restarts cleanly", async () => {
    vi.useFakeTimers();
    const send = vi.fn(async () => undefined);
    const space: PhotonSpace = { id: "shared-space", type: "dm", send };
    const controller = new PhotonTypingController();
    await controller.start(space, "reply");
    await controller.start(space, "heartbeat");
    expect(send).toHaveBeenCalledTimes(1);

    await controller.stop(space, "heartbeat");
    expect(send).toHaveBeenCalledTimes(1);
    await controller.stop(space, "reply");
    expect(send).toHaveBeenCalledTimes(2);
    await controller.start(space, "heartbeat");
    expect(send).toHaveBeenCalledTimes(3);
    await controller.stop(space, "heartbeat");
  });

  it("bounds hung typing controls", async () => {
    vi.useFakeTimers();
    const space: PhotonSpace = {
      id: "hung-space", type: "dm",
      send: vi.fn(async () => await new Promise<{ id: string } | undefined>(() => undefined)),
    };
    const controller = new PhotonTypingController();
    const starting = controller.start(space, "reply");
    await vi.advanceTimersByTimeAsync(5_000);
    await starting;
    const stopping = controller.stop(space, "reply");
    await vi.advanceTimersByTimeAsync(5_000);
    await stopping;
  });

  it("clears keepalive synchronously and releases its owner when aborted", async () => {
    const abort = new AbortController();
    const executing = deferred();
    const finish = deferred();
    let callback: (() => void) | undefined;
    const clearInterval = vi.fn();
    const send = vi.fn(async () => undefined);
    const space: PhotonSpace = { id: "aborted-space", type: "dm", send };
    const operation = withPhotonTyping(
      space,
      async () => { executing.resolve(); await finish.promise; },
      {
        clearInterval,
        setInterval: (next) => {
          callback = next;
          return 1 as unknown as ReturnType<typeof setInterval>;
        },
      },
      new PhotonTypingController(),
      abort.signal,
    );
    await executing.promise;

    abort.abort();
    expect(clearInterval).toHaveBeenCalledOnce();
    callback?.();
    await Promise.resolve();
    expect(send).toHaveBeenCalledTimes(2);

    finish.resolve();
    await operation;
  });

  it("shares cooldown between reply keepalive and heartbeat and always stops", async () => {
    vi.useFakeTimers();
    const executing = deferred();
    const finish = deferred();
    const send = vi.fn(async () => undefined);
    const space: PhotonSpace = { id: "space", type: "dm", send };
    const controller = new PhotonTypingController();
    const operation = withPhotonTyping(space, async () => {
      executing.resolve();
      await finish.promise;
      throw new Error("agent failed");
    }, undefined, controller);
    await executing.promise;
    expect(send).toHaveBeenCalledTimes(1);

    await controller.start(space, "heartbeat");
    expect(send).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(send).toHaveBeenCalledTimes(2);
    await controller.stop(space, "heartbeat");
    expect(send).toHaveBeenCalledTimes(2);
    finish.resolve();
    await expect(operation).rejects.toThrow("agent failed");
    expect(send).toHaveBeenCalledTimes(3);
  });
});
