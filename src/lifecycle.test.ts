import type { OpenClawConfig } from "openclaw/plugin-sdk/channel-core";
import { PlatformMessageNotDispatchedError } from "openclaw/plugin-sdk/error-runtime";
import { afterEach, describe, expect, it, vi } from "vitest";
import { photonPlugin } from "./channel.js";
import { resolvePhotonAccount } from "./config.js";
import {
  getActivePhotonTransport,
  getActivePhotonTransportIdentity,
  notePhotonOutbound,
  notePhotonOutboundFailure,
  runPhotonAccount,
  stopPhotonAccount,
  type PhotonGatewayContext,
} from "./runtime.js";
import type { PhotonInboundMessage, PhotonTransport } from "./transport.js";

function deferred(): { promise: Promise<void>; resolve(): void } {
  let complete: (() => void) | undefined;
  const promise = new Promise<void>((resolve) => {
    complete = resolve;
  });
  return {
    promise,
    resolve: () => {
      if (!complete) {
        throw new Error("deferred promise was not initialized");
      }
      complete();
    },
  };
}

function blockingMessages(done: Promise<void>): AsyncIterable<PhotonInboundMessage> {
  return {
    async *[Symbol.asyncIterator]() {
      await done;
    },
  };
}

function harness() {
  const cfg: OpenClawConfig = { channels: { photon: { allowFrom: ["+14155550123"] } } };
  const abort = new AbortController();
  const ready = deferred();
  const status = vi.fn((next: { lifecycle?: string }) => {
    if (next.lifecycle === "ready") ready.resolve();
  });
  const ctx: PhotonGatewayContext = {
    abortSignal: abort.signal,
    account: resolvePhotonAccount(cfg, {
      OPENCLAW_PHOTON_PROJECT_ID: "project",
      OPENCLAW_PHOTON_PROJECT_SECRET: "secret",
    }),
    cfg,
    log: { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() },
    setStatus: status,
  };
  return { abort, ctx, ready, status };
}

describe("Photon lifecycle", () => {
  afterEach(() => vi.useRealTimers());

  it("prevents a delayed superseded start from publishing ready or replacing its successor", async () => {
    let resolveOld: ((transport: PhotonTransport) => void) | undefined;
    const oldTransportPromise = new Promise<PhotonTransport>((resolve) => { resolveOld = resolve; });
    const oldStop = vi.fn(async () => undefined);
    const replacementEnded = deferred();
    const replacementStarted = deferred();
    const replacementStop = vi.fn(async () => replacementEnded.resolve());
    const first = harness();
    const second = harness();
    const oldRun = runPhotonAccount(first.ctx, {
      createTransport: async () => await oldTransportPromise,
      dispatchInbound: vi.fn(),
    });
    const replacementRun = runPhotonAccount(second.ctx, {
      createTransport: async () => {
        replacementStarted.resolve();
        return {
          messages: blockingMessages(replacementEnded.promise),
          resolveDirectSpace: vi.fn(),
          stop: replacementStop,
        };
      },
      dispatchInbound: vi.fn(),
    });
    await replacementStarted.promise;
    resolveOld?.({
      messages: blockingMessages(Promise.resolve()),
      resolveDirectSpace: vi.fn(),
      stop: oldStop,
    });
    await oldRun;

    expect(oldStop).toHaveBeenCalledOnce();
    expect(first.status).not.toHaveBeenCalledWith(expect.objectContaining({ lifecycle: "ready" }));
    expect(second.status).toHaveBeenCalledWith(expect.objectContaining({ lifecycle: "ready" }));
    await stopPhotonAccount();
    await replacementRun;
  });

  it("publishes a sanitized stopped status when transport creation fails", async () => {
    const { ctx, status } = harness();
    await expect(runPhotonAccount(ctx, {
      createTransport: async () => { throw new Error("projectSecret=secret"); },
      dispatchInbound: vi.fn(),
    })).rejects.toThrow("projectSecret=[REDACTED]");
    expect(status).toHaveBeenLastCalledWith(expect.objectContaining({
      connected: false,
      lifecycle: "stopped",
      lastError: "projectSecret=[REDACTED]",
    }));
  });

  it("bounds transport startup and publishes a sanitized stopped failure", async () => {
    vi.useFakeTimers();
    const { ctx, status } = harness();
    let startupSignal: AbortSignal | undefined;
    const lifecycle = runPhotonAccount(ctx, {
      createTransport: async (_account, signal) => {
        startupSignal = signal;
        return await new Promise<PhotonTransport>(() => undefined);
      },
      dispatchInbound: vi.fn(),
      startTimeoutMs: 25,
    });
    const rejected = expect(lifecycle).rejects.toThrow("transport startup timed out");
    await vi.advanceTimersByTimeAsync(25);
    await rejected;
    expect(startupSignal?.aborted).toBe(true);
    expect(status).toHaveBeenLastCalledWith(expect.objectContaining({
      connected: false, lifecycle: "stopped",
    }));
  });

  it("does not publish transport activity timestamps for application messages", async () => {
    const { ctx, status } = harness();
    const space = { id: "space", type: "dm" as const, send: vi.fn() };
    const event: PhotonInboundMessage = {
      body: "outbound", direction: "outbound", id: "message", markRead: vi.fn(),
      senderAddress: "+14155550123", space, timestamp: new Date(),
    };
    await runPhotonAccount(ctx, {
      createTransport: async () => ({
        messages: { async *[Symbol.asyncIterator]() { yield event; } },
        resolveDirectSpace: vi.fn(), stop: vi.fn(async () => undefined),
      }),
      dispatchInbound: vi.fn(),
    });
    for (const [next] of status.mock.calls) {
      expect(next).not.toHaveProperty("lastTransportActivityAt");
    }
  });

  it("starts, becomes ready, and stops once when aborted", async () => {
    const ended = deferred();
    const started = deferred();
    const stop = vi.fn(async () => ended.resolve());
    const transport: PhotonTransport = {
      messages: blockingMessages(ended.promise),
      resolveDirectSpace: vi.fn(),
      stop,
    };
    const { abort, ctx, ready, status } = harness();
    const lifecycle = runPhotonAccount(ctx, {
      createTransport: async () => {
        started.resolve();
        return transport;
      },
      dispatchInbound: vi.fn(),
    });
    await started.promise;
    await ready.promise;
    expect(status).toHaveBeenNthCalledWith(1, expect.objectContaining({
      connected: false,
      lifecycle: "starting",
    }));
    expect(status).toHaveBeenCalledWith(expect.objectContaining({ lifecycle: "ready" }));

    abort.abort();
    await lifecycle;

    expect(stop).toHaveBeenCalledOnce();
    expect(status).toHaveBeenLastCalledWith(expect.objectContaining({ lifecycle: "stopped" }));
  });

  it("stops active typing before transport and emits no keepalive after abort", async () => {
    vi.useFakeTimers();
    const dispatchStarted = deferred();
    const streamEnded = deferred();
    const order: string[] = [];
    let stopped = false;
    const typingStop = deferred();
    const unhandled = vi.fn();
    process.on("unhandledRejection", unhandled);
    const send = vi.fn(async () => {
      if (stopped) throw new Error("post-stop send");
      if (send.mock.calls.length === 1) {
        order.push("typing-start");
      } else {
        order.push("typing-stop-start");
        await typingStop.promise;
        order.push("typing-stop-complete");
        throw new Error("typing stop failed");
      }
      return { id: "control" };
    });
    const event: PhotonInboundMessage = {
      body: "hello",
      direction: "inbound",
      id: "abort-event",
      markRead: vi.fn(async () => undefined),
      senderAddress: "+14155550123",
      space: { id: "abort-space", type: "dm", send },
      timestamp: new Date(),
    };
    const { abort, ctx } = harness();
    const lifecycle = runPhotonAccount(ctx, {
      createTransport: async () => ({
        messages: {
          async *[Symbol.asyncIterator]() {
            yield event;
            await streamEnded.promise;
          },
        },
        resolveDirectSpace: vi.fn(),
        stop: vi.fn(async () => {
          order.push("transport-stop");
          stopped = true;
          streamEnded.resolve();
        }),
      }),
      dispatchInbound: vi.fn(async () => {
        dispatchStarted.resolve();
        await new Promise<void>(() => undefined);
      }),
    });
    await dispatchStarted.promise;

    abort.abort();
    await Promise.resolve();
    await Promise.resolve();
    expect(order).toContain("typing-stop-start");
    expect(order).not.toContain("transport-stop");
    typingStop.resolve();
    await lifecycle;
    expect(order).toEqual([
      "typing-start", "typing-stop-start", "typing-stop-complete", "transport-stop",
    ]);
    await vi.advanceTimersByTimeAsync(10_000);
    expect(send).toHaveBeenCalledTimes(2);
    expect(unhandled).not.toHaveBeenCalled();
    process.off("unhandledRejection", unhandled);
  });

  it("stops immediately when startup receives an aborted signal", async () => {
    const stop = vi.fn(async () => undefined);
    const { abort, ctx } = harness();
    abort.abort();

    await runPhotonAccount(ctx, {
      createTransport: async () => ({
        messages: blockingMessages(Promise.resolve()),
        resolveDirectSpace: vi.fn(),
        stop,
      }),
      dispatchInbound: vi.fn(),
    });

    expect(stop).toHaveBeenCalledOnce();
  });

  it("revokes ownership and publishes stopped even when explicit transport stop rejects", async () => {
    const ended = deferred();
    const stopEntered = deferred();
    const releaseStop = deferred();
    const { ctx, ready, status } = harness();
    const lifecycle = runPhotonAccount(ctx, {
      createTransport: async () => ({
        messages: blockingMessages(ended.promise),
        resolveDirectSpace: vi.fn(),
        stop: vi.fn(async () => {
          stopEntered.resolve();
          await releaseStop.promise;
          ended.resolve();
          throw new Error("token='stop secret'");
        }),
      }),
      dispatchInbound: vi.fn(),
    });
    await ready.promise;
    const stopping = stopPhotonAccount();
    await stopEntered.promise;
    expect(getActivePhotonTransport()).toBeUndefined();
    releaseStop.resolve();
    await expect(stopping).rejects.toThrow("token=[REDACTED]");
    await expect(lifecycle).rejects.toThrow("token=[REDACTED]");
    expect(status).toHaveBeenLastCalledWith(expect.objectContaining({
      connected: false,
      lifecycle: "stopped",
    }));
  });

  it("revokes the active transport synchronously on abort before stop settles", async () => {
    const never = new Promise<void>(() => undefined);
    const { abort, ctx, ready } = harness();
    void runPhotonAccount(ctx, {
      createTransport: async () => ({
        messages: blockingMessages(never),
        resolveDirectSpace: vi.fn(),
        stop: vi.fn(async () => await never),
      }),
      dispatchInbound: vi.fn(),
    });
    await ready.promise;

    abort.abort();

    expect(getActivePhotonTransport()).toBeUndefined();
  });

  it("returns from abort when stop rejects and the provider iterator never settles", async () => {
    const { abort, ctx, ready } = harness();
    const lifecycle = runPhotonAccount(ctx, {
      createTransport: async () => ({
        messages: blockingMessages(new Promise<void>(() => undefined)),
        resolveDirectSpace: vi.fn(),
        stop: vi.fn(async () => { throw new Error("token='stop secret'"); }),
      }),
      dispatchInbound: vi.fn(),
    });
    await ready.promise;

    abort.abort();

    await expect(lifecycle).rejects.toThrow("token=[REDACTED]");
  });

  it("does not let a superseded creation failure overwrite replacement ready", async () => {
    const releaseFailure = deferred();
    const first = harness();
    const second = harness();
    const oldRun = runPhotonAccount(first.ctx, {
      createTransport: async () => { await releaseFailure.promise; throw new Error("old failed"); },
      dispatchInbound: vi.fn(),
    });
    const replacementRun = runPhotonAccount(second.ctx, {
      createTransport: async () => ({
        messages: blockingMessages(Promise.resolve()), resolveDirectSpace: vi.fn(),
        stop: vi.fn(async () => undefined),
      }),
      dispatchInbound: vi.fn(),
    });
    await second.ready.promise;
    releaseFailure.resolve();
    await expect(oldRun).rejects.toThrow("old failed");
    await replacementRun;

    expect(first.status).not.toHaveBeenLastCalledWith(expect.objectContaining({ lifecycle: "stopped" }));
    expect(second.status).toHaveBeenCalledWith(expect.objectContaining({ lifecycle: "ready" }));
  });

  it("explicit stop during startup revokes that generation and publishes stopped", async () => {
    const releaseCreate = deferred();
    const { ctx, status } = harness();
    const lifecycle = runPhotonAccount(ctx, {
      createTransport: async () => {
        await releaseCreate.promise;
        return { messages: blockingMessages(Promise.resolve()), resolveDirectSpace: vi.fn(), stop: vi.fn() };
      },
      dispatchInbound: vi.fn(),
    });

    await stopPhotonAccount();
    releaseCreate.resolve();
    await lifecycle;

    expect(status).toHaveBeenLastCalledWith(expect.objectContaining({ lifecycle: "stopped" }));
    expect(status).not.toHaveBeenCalledWith(expect.objectContaining({ lifecycle: "ready" }));
  });

  it("supports explicit stop through the channel stop hook", async () => {
    const ended = deferred();
    const started = deferred();
    const stop = vi.fn(async () => ended.resolve());
    const { ctx } = harness();
    const lifecycle = runPhotonAccount(ctx, {
      createTransport: async () => {
        started.resolve();
        return {
          messages: blockingMessages(ended.promise),
          resolveDirectSpace: vi.fn(),
          stop,
        };
      },
      dispatchInbound: vi.fn(),
    });
    await started.promise;

    await stopPhotonAccount();
    await lifecycle;

    expect(stop).toHaveBeenCalledOnce();
  });

  it("bounds explicit stop, revokes immediately, and publishes stopped", async () => {
    vi.useFakeTimers();
    const never = new Promise<void>(() => undefined);
    const { ctx, ready, status } = harness();
    const lifecycle = runPhotonAccount(ctx, {
      createTransport: async () => ({
        messages: blockingMessages(never), resolveDirectSpace: vi.fn(),
        stop: vi.fn(async () => await never),
      }),
      dispatchInbound: vi.fn(),
      stopTimeoutMs: 10,
    });
    await ready.promise;

    const stopping = stopPhotonAccount();
    const stopped = expect(stopping).rejects.toThrow("stop timed out");
    const lifecycleStopped = expect(lifecycle).rejects.toThrow("stop timed out");
    expect(getActivePhotonTransport()).toBeUndefined();
    await vi.advanceTimersByTimeAsync(10);
    await stopped;
    expect(status).toHaveBeenLastCalledWith(expect.objectContaining({ lifecycle: "stopped" }));
    await vi.advanceTimersByTimeAsync(1_000);
    await lifecycleStopped;
  });

  it("records fenced outbound failures, recovers at the threshold, and resets on success", async () => {
    const ended = deferred();
    const { ctx, ready, status } = harness();
    const lifecycle = runPhotonAccount(ctx, {
      createTransport: async () => ({
        messages: blockingMessages(ended.promise), resolveDirectSpace: vi.fn(),
        stop: vi.fn(async () => ended.resolve()),
      }),
      dispatchInbound: vi.fn(),
    });
    await ready.promise;
    const unavailable = Object.assign(new Error("projectSecret=secret unavailable"), {
      code: "ECONNREFUSED",
      syscall: "connect",
    });

    const active = getActivePhotonTransportIdentity();
    if (!active) throw new Error("active transport missing");
    notePhotonOutboundFailure(active, unavailable, 10);
    notePhotonOutboundFailure(active, unavailable, 11);
    notePhotonOutbound(active, 12);
    notePhotonOutboundFailure(active, unavailable, 13);
    notePhotonOutboundFailure(active, unavailable, 14);
    expect(status).not.toHaveBeenCalledWith(expect.objectContaining({ lifecycle: "recovering" }));
    notePhotonOutboundFailure(active, unavailable, 15);

    expect(status).toHaveBeenCalledWith(expect.objectContaining({
      lastError: "projectSecret=[REDACTED] unavailable",
      lastOutboundErrorAt: 15,
    }));
    expect(status).toHaveBeenCalledWith(expect.objectContaining({ lifecycle: "recovering" }));
    await lifecycle;
  });

  it("records permanent rejection without triggering transport recovery", async () => {
    const ended = deferred();
    const { ctx, ready, status } = harness();
    const lifecycle = runPhotonAccount(ctx, {
      createTransport: async () => ({
        messages: blockingMessages(ended.promise), resolveDirectSpace: vi.fn(),
        stop: vi.fn(async () => ended.resolve()),
      }),
      dispatchInbound: vi.fn(),
    });
    await ready.promise;
    const active = getActivePhotonTransportIdentity();
    if (!active) throw new Error("active transport missing");
    const permanent = new PlatformMessageNotDispatchedError("policy rejected", {
      cause: new Error("permanent policy"),
      retryable: false,
    });

    notePhotonOutboundFailure(active, permanent, 10);
    notePhotonOutboundFailure(active, permanent, 11);
    notePhotonOutboundFailure(active, permanent, 12);

    expect(status).toHaveBeenCalledWith(expect.objectContaining({
      lastError: "policy rejected",
      lastOutboundErrorAt: 12,
    }));
    expect(status).not.toHaveBeenCalledWith(expect.objectContaining({ lifecycle: "recovering" }));
    await stopPhotonAccount();
    await lifecycle;
  });

  it("ignores completion health from a replaced transport generation", async () => {
    const oldEnd = deferred();
    const replacementEnd = deferred();
    const first = harness();
    const second = harness();
    const oldTransport: PhotonTransport = {
      messages: blockingMessages(oldEnd.promise), resolveDirectSpace: vi.fn(),
      stop: vi.fn(async () => oldEnd.resolve()),
    };
    const oldRun = runPhotonAccount(first.ctx, {
      createTransport: async () => oldTransport, dispatchInbound: vi.fn(),
    });
    await first.ready.promise;
    const oldIdentity = getActivePhotonTransportIdentity();
    if (!oldIdentity) throw new Error("old transport identity missing");
    const replacementRun = runPhotonAccount(second.ctx, {
      createTransport: async () => ({
        messages: blockingMessages(replacementEnd.promise), resolveDirectSpace: vi.fn(),
        stop: vi.fn(async () => replacementEnd.resolve()),
      }),
      dispatchInbound: vi.fn(),
    });
    await second.ready.promise;
    await oldRun;

    notePhotonOutboundFailure(oldIdentity, new Error("stale failure"), 99);
    expect(second.status).not.toHaveBeenCalledWith(expect.objectContaining({
      lastOutboundErrorAt: 99,
    }));
    await stopPhotonAccount();
    await replacementRun;
  });

  it("requests direct-send recovery after bounded failures without resending a message", async () => {
    const ended = deferred();
    const send = vi.fn(async () => {
      throw Object.assign(new Error("server unavailable"), {
        code: "ECONNREFUSED",
        syscall: "connect",
      });
    });
    const space = { id: "space", type: "dm" as const, send };
    const { abort, ctx, ready, status } = harness();
    const baseSetStatus = ctx.setStatus;
    ctx.setStatus = (next) => {
      baseSetStatus(next);
      if (next.lifecycle === "recovering") abort.abort();
    };
    const lifecycle = runPhotonAccount(ctx, {
      createTransport: async () => ({
        messages: blockingMessages(ended.promise),
        resolveDirectSpace: vi.fn(async () => space),
        stop: vi.fn(async () => ended.resolve()),
      }),
      dispatchInbound: vi.fn(),
    });
    await ready.promise;
    const sendText = photonPlugin.outbound?.sendText;
    if (!sendText) throw new Error("Photon direct outbound adapter is missing");

    for (let attempt = 0; attempt < 3; attempt += 1) {
      await expect(sendText({ cfg: ctx.cfg, to: "+14155550123", text: "once" }))
        .rejects.toBeInstanceOf(Error);
    }

    expect(send).toHaveBeenCalledTimes(3);
    expect(status).toHaveBeenCalledWith(expect.objectContaining({ lifecycle: "recovering" }));
    await lifecycle;
  });
});
