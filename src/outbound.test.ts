import type { OpenClawConfig } from "openclaw/plugin-sdk/channel-core";
import { PlatformMessageNotDispatchedError } from "openclaw/plugin-sdk/error-runtime";
import { afterEach, describe, expect, it, vi } from "vitest";
import { photonPlugin, sendPhotonOutbound } from "./channel.js";
import { OutboundRateGate } from "./outbound-rate.js";
import type { PhotonSpace, PhotonTransport } from "./transport.js";

function emptyMessages(): AsyncIterable<never> {
  return {
    async *[Symbol.asyncIterator]() {
      return;
    },
  };
}

describe("Photon outbound", () => {
  afterEach(() => vi.useRealTimers());

  it("resolves an E.164 DM and sends markdown through its space", async () => {
    const send = vi.fn<PhotonSpace["send"]>(async () => ({ id: "message-7" }));
    const space: PhotonSpace = { id: "space-7", type: "dm", send };
    const resolveDirectSpace = vi.fn(async () => space);
    const transport: PhotonTransport = {
      messages: emptyMessages(),
      resolveDirectSpace,
      stop: vi.fn(async () => undefined),
    };

    await expect(sendPhotonOutbound(
      transport,
      " +14155550123 ",
      "**hello**",
      ["+14155550123"],
    )).resolves.toEqual({
      to: "+14155550123",
      messageId: "message-7",
    });
    expect(resolveDirectSpace).toHaveBeenCalledWith("+14155550123");
    expect(send).toHaveBeenCalledOnce();
    const content: unknown = send.mock.calls[0]?.[0];
    expect(content).toHaveProperty("build");
  });

  it("suppresses a queued send after transport ownership is revoked", async () => {
    let current = true;
    const space: PhotonSpace = {
      id: "space",
      type: "dm",
      send: vi.fn(async () => ({ id: "sent" })),
    };
    const transport: PhotonTransport = {
      messages: emptyMessages(),
      resolveDirectSpace: vi.fn(async () => { current = false; return space; }),
      stop: vi.fn(async () => undefined),
    };
    const sending = sendPhotonOutbound(
      transport,
      "+14155550123",
      "hello",
      ["+14155550123"],
      undefined,
      undefined,
      () => current,
    );
    await expect(sending).rejects.toBeInstanceOf(PlatformMessageNotDispatchedError);
    await expect(sending).rejects.toThrow("not running");
    expect(space.send).not.toHaveBeenCalled();
  });

  it("classifies a missing active transport as proven non-dispatch", async () => {
    const sendText = photonPlugin.outbound?.sendText;
    if (!sendText) throw new Error("Photon send adapter is missing");
    const cfg: OpenClawConfig = {
      channels: { photon: { allowFrom: ["+14155550123"] } },
    };

    await expect(sendText({ cfg, to: "+14155550123", text: "hello" }))
      .rejects.toBeInstanceOf(PlatformMessageNotDispatchedError);
  });

  it("classifies every direct-space resolution failure as proven non-dispatch", async () => {
    const transport: PhotonTransport = {
      messages: emptyMessages(),
      resolveDirectSpace: vi.fn(async () => {
        throw Object.assign(new Error("token=secret-value"), { code: 2 });
      }),
      stop: vi.fn(async () => undefined),
    };
    const sending = sendPhotonOutbound(
      transport, "+14155550123", "hello", ["+14155550123"],
    );
    await expect(sending).rejects.toBeInstanceOf(PlatformMessageNotDispatchedError);
    await expect(sending).rejects.not.toThrow("secret-value");
  });

  it("bounds a hung direct-space resolution", async () => {
    vi.useFakeTimers();
    const transport: PhotonTransport = {
      messages: emptyMessages(),
      resolveDirectSpace: vi.fn(async () => await new Promise<PhotonSpace>(() => undefined)),
      stop: vi.fn(async () => undefined),
    };
    const sending = sendPhotonOutbound(transport, "+14155550123", "hello", ["+14155550123"]);
    const rejected = expect(sending).rejects.toThrow("timed out");
    await vi.advanceTimersByTimeAsync(5_000);
    await rejected;
  });

  it("rejects non-E.164 targets before transport access", async () => {
    const transport: PhotonTransport = {
      messages: emptyMessages(),
      resolveDirectSpace: vi.fn(),
      stop: vi.fn(async () => undefined),
    };
    await expect(sendPhotonOutbound(transport, "4155550123", "hello", [])).rejects.toThrow("E.164");
    expect(transport.resolveDirectSpace).not.toHaveBeenCalled();
  });

  it("rejects valid targets outside the configured outbound allowlist", async () => {
    const transport: PhotonTransport = {
      messages: emptyMessages(),
      resolveDirectSpace: vi.fn(),
      stop: vi.fn(async () => undefined),
    };
    await expect(sendPhotonOutbound(
      transport,
      "+14155550124",
      "hello",
      ["+14155550123"],
    )).rejects.toThrow("allowlist");
    expect(transport.resolveDirectSpace).not.toHaveBeenCalled();
  });

  it("enforces outbound destination limits before transport access", async () => {
    const space: PhotonSpace = {
      id: "space",
      type: "dm",
      send: vi.fn(async () => ({ id: "sent" })),
    };
    const resolveDirectSpace = vi.fn(async () => space);
    const transport: PhotonTransport = {
      messages: emptyMessages(),
      resolveDirectSpace,
      stop: vi.fn(async () => undefined),
    };
    const gate = new OutboundRateGate({ globalLimit: 1, perDestinationLimit: 1 });
    await sendPhotonOutbound(transport, "+14155550123", "first", ["+14155550123"], gate, 1);
    await expect(sendPhotonOutbound(
      transport,
      "+14155550123",
      "second",
      ["+14155550123"],
      gate,
      2,
    )).rejects.toThrow("rate limit");
    expect(resolveDirectSpace).toHaveBeenCalledOnce();
  });

  it("honors a narrower outbound allowlist even when core supplies inbound allowFrom", () => {
    const cfg: OpenClawConfig = { channels: { photon: {
      allowFrom: ["+14155550123"],
      outboundAllowFrom: ["+14155550124"],
    } } };
    expect(photonPlugin.outbound?.resolveTarget?.({
      cfg,
      to: "+14155550123",
      allowFrom: ["+14155550123"],
    }).ok).toBe(false);
    expect(photonPlugin.outbound?.resolveTarget?.({ cfg, to: "+14155550124" }).ok).toBe(true);
  });

  it("builds an outbound session route identical to the inbound photon E.164 identity", async () => {
    const cfg: OpenClawConfig = { channels: { photon: { allowFrom: ["+14155550123"] } } };
    const resolveRoute = photonPlugin.messaging?.resolveOutboundSessionRoute;
    expect(resolveRoute).toBeDefined();
    const plain = await resolveRoute?.({
      cfg,
      agentId: "main",
      accountId: "default",
      target: "+14155550123",
    });
    const prefixed = await resolveRoute?.({
      cfg,
      agentId: "main",
      accountId: "default",
      target: "photon:+14155550123",
    });
    expect(plain).toMatchObject({
      recipientSessionExact: true,
      peer: { kind: "direct", id: "+14155550123" },
      chatType: "direct",
      from: "photon:+14155550123",
      to: "photon:+14155550123",
    });
    expect(prefixed).toEqual(plain);
    expect(await resolveRoute?.({ cfg, agentId: "main", target: "4155550123" })).toBeNull();
  });

  it("sanitizes assistant-only scaffolding, suppresses empty text, and matches inbound routes", () => {
    const sanitize = photonPlugin.outbound?.sanitizeText;
    const matches = photonPlugin.outbound?.targetsMatchForReplySuppression;
    expect(sanitize?.({ text: "<think>secret</think> hello", payload: { text: "x" } })).toBe("hello");
    expect(sanitize?.({ text: "<think>secret</think>", payload: { text: "x" } })).toBe("");
    expect(matches?.({ originTarget: "photon:+14155550123", targetKey: "+14155550123" })).toBe(true);
    expect(photonPlugin.outbound?.textChunkLimit).toBe(4000);
    expect(photonPlugin.outbound?.chunker?.("a".repeat(4001), 4000)).toHaveLength(2);
  });
});
