import type { OpenClawConfig } from "openclaw/plugin-sdk/channel-core";
import { afterEach, describe, expect, it, vi } from "vitest";
import { resolvePhotonAccount } from "./config.js";
import {
  handlePhotonInbound,
  type InboundDispatchParams,
  type PhotonGatewayContext,
  type PhotonRuntimeDependencies,
} from "./runtime.js";
import type { PhotonInboundMessage, PhotonSpace } from "./transport.js";

function context(allowFrom: string[], outboundAllowFrom?: string[]): PhotonGatewayContext {
  const cfg: OpenClawConfig = {
    channels: { photon: { allowFrom, ...(outboundAllowFrom ? { outboundAllowFrom } : {}) } },
  };
  return {
    abortSignal: new AbortController().signal,
    account: resolvePhotonAccount(cfg, {
      OPENCLAW_PHOTON_PROJECT_ID: "project",
      OPENCLAW_PHOTON_PROJECT_SECRET: "secret",
    }),
    cfg,
    log: { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() },
    setStatus: vi.fn(),
  };
}

function event(send: PhotonSpace["send"]): PhotonInboundMessage {
  return {
    body: "hello",
    direction: "inbound",
    id: "inbound-1",
    markRead: vi.fn(async () => undefined),
    senderAddress: "+14155550123",
    space: { id: "space-1", type: "dm", send },
    timestamp: new Date("2026-09-02T12:00:00Z"),
  };
}

describe("Photon inbound", () => {
  afterEach(() => vi.useRealTimers());

  it("dispatches an allowed app.messages DM into OpenClaw and sends its reply", async () => {
    const send = vi.fn(async () => ({ id: "reply-1" }));
    const dispatchInbound: PhotonRuntimeDependencies["dispatchInbound"] = vi.fn(
      async (params: InboundDispatchParams) => {
        await params.deliver({ text: "**reply**" });
      },
    );

    const inbound = event(send);
    const ctx = context(["+14155550123"]);
    await handlePhotonInbound({
      ctx,
      event: inbound,
      dispatchInbound,
      now: () => 1_788_364_801_000,
    });

    expect(dispatchInbound).toHaveBeenCalledOnce();
    expect(dispatchInbound).toHaveBeenCalledWith(
      expect.objectContaining({
        channel: "photon",
        senderId: "+14155550123",
        rawBody: "hello",
        messageId: "inbound-1",
        inboundAccessAuthorized: true,
      }),
    );
    expect(send).toHaveBeenCalledTimes(3);
    expect(inbound.markRead).toHaveBeenCalledOnce();
    expect(ctx.setStatus).toHaveBeenCalledWith(expect.objectContaining({
      lastInboundAt: 1_788_364_801_000,
    }));
    expect(ctx.setStatus).toHaveBeenCalledWith(expect.objectContaining({
      lastOutboundAt: 1_788_364_801_000,
    }));
  });

  it("suppresses receipts, typing, and replies when sender is excluded from outboundAllowFrom", async () => {
    const send = vi.fn(async () => ({ id: "control" }));
    const dispatchInbound: PhotonRuntimeDependencies["dispatchInbound"] = vi.fn(
      async (params: InboundDispatchParams) => { await params.deliver({ text: "must not send" }); },
    );

    const inbound = event(send);
    await handlePhotonInbound({
      ctx: context(["+14155550123"], []),
      event: inbound,
      dispatchInbound,
    });

    expect(dispatchInbound).toHaveBeenCalledOnce();
    expect(inbound.markRead).not.toHaveBeenCalled();
    expect(send).not.toHaveBeenCalled();
  });

  it("rejects a non-finite timestamp before journaling", async () => {
    const inbound = { ...event(vi.fn()), timestamp: new Date(Number.NaN) };
    const journal = { enqueue: vi.fn(), pending: vi.fn(), complete: vi.fn(), stageReply: vi.fn() };
    const { admitLiveEvent } = await import("./inbound.js");
    await expect(admitLiveEvent({
      ctx: context(["+14155550123"]),
      dependencies: { createTransport: vi.fn(), dispatchInbound: vi.fn() },
      event: inbound,
      journal,
      rateGate: { admit: vi.fn(() => true) },
    })).resolves.toBe(false);
    expect(journal.enqueue).not.toHaveBeenCalled();
  });

  it("bounds a hung read receipt and continues agent dispatch", async () => {
    vi.useFakeTimers();
    const inbound = {
      ...event(vi.fn(async () => ({ id: "control" }))),
      markRead: vi.fn(async () => await new Promise<void>(() => undefined)),
    };
    const dispatchInbound = vi.fn(async () => undefined);
    const handling = handlePhotonInbound({ ctx: context(["+14155550123"]), event: inbound, dispatchInbound });

    await vi.advanceTimersByTimeAsync(5_000);
    await handling;
    expect(dispatchInbound).toHaveBeenCalledOnce();
  });

  it("bounds inbound bodies before agent dispatch", async () => {
    const dispatchInbound: PhotonRuntimeDependencies["dispatchInbound"] = vi.fn();
    const oversized = { ...event(vi.fn()), body: "x".repeat(16_001) };
    await handlePhotonInbound({
      ctx: context(["+14155550123"]),
      event: oversized,
      dispatchInbound,
    });
    expect(dispatchInbound).not.toHaveBeenCalled();
    expect(oversized.markRead).not.toHaveBeenCalled();
  });

  it("stops typing when dispatch fails", async () => {
    const send = vi.fn(async () => ({ id: "control" }));
    await expect(handlePhotonInbound({
      ctx: context(["+14155550123"]),
      event: event(send),
      dispatchInbound: vi.fn(async () => { throw new Error("agent failed"); }),
    })).rejects.toThrow("agent failed");
    expect(send).toHaveBeenCalledTimes(2);
  });

  it("drops senders outside the allowlist", async () => {
    const dispatchInbound: PhotonRuntimeDependencies["dispatchInbound"] = vi.fn();
    await handlePhotonInbound({
      ctx: context([]),
      event: event(vi.fn()),
      dispatchInbound,
    });
    expect(dispatchInbound).not.toHaveBeenCalled();
  });
});
