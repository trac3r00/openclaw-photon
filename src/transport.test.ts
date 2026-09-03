import { PlatformMessageNotDispatchedError } from "openclaw/plugin-sdk/error-runtime";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ResolvedPhotonAccount } from "./config.js";

const mocks = vi.hoisted(() => {
  const config = vi.fn(() => ({ provider: "imessage" }));
  const platform = { space: { create: vi.fn(), get: vi.fn() } };
  const imessage = Object.assign(vi.fn(() => platform), { config, is: vi.fn() });
  return {
    Spectrum: vi.fn(),
    config,
    imessage,
    markdown: vi.fn((body: string) => ({ kind: "markdown", body })),
    platform,
    text: vi.fn((body: string) => ({ kind: "text", body })),
    typing: vi.fn((state: "start" | "stop") => ({ kind: "typing", state })),
  };
});

vi.mock("spectrum-ts", () => ({
  Spectrum: mocks.Spectrum,
  markdown: mocks.markdown,
  text: mocks.text,
  typing: mocks.typing,
}));
vi.mock("spectrum-ts/providers/imessage", () => ({ imessage: mocks.imessage }));

import {
  createPhotonTransport,
  sendToSpace,
  setSpaceTyping,
  type PhotonSpace,
} from "./transport.js";

const account: ResolvedPhotonAccount = {
  accountId: "default",
  enabled: true,
  configured: true,
  projectId: "project",
  projectSecret: "secret",
  config: { allowFrom: [], dmPolicy: "allowlist", telemetry: true },
};

describe("Photon transport", () => {
  beforeEach(() => vi.clearAllMocks());

  it("initializes the persistent Spectrum iMessage app with required options", async () => {
    const stop = vi.fn(async () => undefined);
    const messages = {
      async *[Symbol.asyncIterator]() {
        return;
      },
    };
    mocks.Spectrum.mockResolvedValueOnce({ messages, stop });

    const transport = await createPhotonTransport(account);

    expect(mocks.config).toHaveBeenCalledWith();
    expect(mocks.Spectrum).toHaveBeenCalledWith({
      projectId: "project",
      projectSecret: "secret",
      providers: [{ provider: "imessage" }],
      options: { flattenGroups: true },
      telemetry: true,
    });
    expect(transport.messages).toBeDefined();
    await transport.stop();
    expect(stop).toHaveBeenCalledOnce();
  });

  it("sanitizes construction and stop failures before they escape", async () => {
    mocks.Spectrum.mockRejectedValueOnce(new Error("projectSecret=secret"));
    await expect(createPhotonTransport(account)).rejects.toThrow("projectSecret=[REDACTED]");

    mocks.Spectrum.mockResolvedValueOnce({
      messages: { async *[Symbol.asyncIterator]() { return; } },
      stop: vi.fn(async () => { throw new Error('{"projectSecret":"secret value"}'); }),
    });
    const transport = await createPhotonTransport(account);
    await expect(transport.stop()).rejects.toThrow('{"projectSecret":"[REDACTED]"}');
  });

  it("sanitizes provider iterator and tuple mapping failures", async () => {
    mocks.Spectrum.mockResolvedValueOnce({
      messages: { async *[Symbol.asyncIterator]() { throw new Error("token='iterator secret'"); } },
      stop: vi.fn(async () => undefined),
    });
    const iteratorTransport = await createPhotonTransport(account);
    await expect(iteratorTransport.messages[Symbol.asyncIterator]().next())
      .rejects.toThrow("token=[REDACTED]");

    const tuple: unknown[] = [];
    Object.defineProperty(tuple, "0", {
      get: () => { throw new Error("projectSecret='tuple secret'"); },
    });
    tuple.length = 2;
    mocks.Spectrum.mockResolvedValueOnce({
      messages: { async *[Symbol.asyncIterator]() { yield tuple; } },
      stop: vi.fn(async () => undefined),
    });
    const tupleTransport = await createPhotonTransport(account);
    await expect(tupleTransport.messages[Symbol.asyncIterator]().next())
      .rejects.toThrow("projectSecret=[REDACTED]");
  });

  it("maps real-shaped Spectrum tuples and exposes remote read receipts", async () => {
    const read = vi.fn(async () => undefined);
    const space = { id: "space-1", type: "dm", send: vi.fn() };
    const message = {
      content: { type: "text", text: "hello" },
      direction: "inbound",
      id: "message-1",
      read,
      sender: { address: "+14155550123", id: "sender" },
      space,
      timestamp: new Date("2026-09-02T12:00:00Z"),
    };
    mocks.imessage.is.mockReturnValue(true);
    mocks.Spectrum.mockResolvedValueOnce({
      messages: { async *[Symbol.asyncIterator]() { yield [space, message] as const; } },
      stop: vi.fn(async () => undefined),
    });

    const transport = await createPhotonTransport(account);
    const iterator = transport.messages[Symbol.asyncIterator]();
    const received = await iterator.next();
    expect(received.value).toMatchObject({ id: "message-1", senderAddress: "+14155550123" });
    await received.value?.markRead();
    expect(read).toHaveBeenCalledOnce();
  });

  it("uses Spectrum fire-and-forget typing control content", async () => {
    const send = vi.fn(async () => undefined);
    const space: PhotonSpace = { id: "space-1", type: "dm", send };
    await setSpaceTyping(space, "start");
    await setSpaceTyping(space, "stop");
    expect(send).toHaveBeenNthCalledWith(1, { kind: "typing", state: "start" });
    expect(send).toHaveBeenNthCalledWith(2, { kind: "typing", state: "stop" });
  });

  it("sanitizes send failures before they escape", async () => {
    const space: PhotonSpace = {
      id: "space-1",
      type: "dm",
      send: vi.fn(async () => { throw new Error('{"token":"quoted value"}'); }),
    };
    await expect(sendToSpace(space, "hello", "text"))
      .rejects.toThrow('{"token":"[REDACTED]"}');
  });

  it("maps proven unavailable sends to the SDK non-dispatched error without flattening cause", async () => {
    const source = Object.assign(new Error("projectSecret=secret unavailable"), {
      name: "ConnectError", code: 14, grpcCode: "UNAVAILABLE", retryable: true,
      cause: Object.assign(new Error("connect failed"), { code: "ECONNREFUSED", syscall: "connect" }),
    });
    const space: PhotonSpace = {
      id: "space-1", type: "dm", send: vi.fn(async () => { throw source; }),
    };

    const failure = await sendToSpace(space, "hello", "text").catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(PlatformMessageNotDispatchedError);
    expect(failure).toMatchObject({
      code: "OPENCLAW_PLATFORM_MESSAGE_NOT_DISPATCHED", retryable: true,
      cause: {
        name: "ConnectError", code: 14, grpcCode: "UNAVAILABLE",
        cause: { code: "ECONNREFUSED", syscall: "connect" },
      },
    });
    expect(failure).not.toHaveProperty("message", expect.stringContaining("projectSecret=secret"));
  });

  it("bounds a hung provider send and keeps its outcome ambiguous", async () => {
    vi.useFakeTimers();
    try {
      const send = vi.fn(async () => await new Promise<{ id: string }>(() => undefined));
      const space: PhotonSpace = { id: "space-1", type: "dm", send };
      const sending = sendToSpace(space, "hello", "text");
      const rejected = expect(sending).rejects.toMatchObject({
        name: "PhotonAmbiguousDeliveryError",
        retryable: false,
      });

      await vi.advanceTimersByTimeAsync(30_000);
      await rejected;
    } finally {
      vi.useRealTimers();
    }
  });

  it("uses Spectrum text for markdown containing a raw URL", async () => {
    const send = vi.fn(async () => ({ id: "message-1" }));
    const space: PhotonSpace = { id: "space-1", type: "dm", send };

    await sendToSpace(space, "see https://example.com/path", "markdown");

    expect(send).toHaveBeenCalledWith({ kind: "text", body: "see https://example.com/path" });
    expect(mocks.markdown).not.toHaveBeenCalled();
  });

  it.each([
    ["text", { kind: "text", body: "hello" }],
    ["markdown", { kind: "markdown", body: "hello" }],
  ] as const)("sends %s content through space.send", async (format, expected) => {
    const send = vi.fn(async () => ({ id: "message-1" }));
    const space: PhotonSpace = { id: "space-1", type: "dm", send };

    await expect(sendToSpace(space, "hello", format)).resolves.toBe("message-1");
    expect(send).toHaveBeenCalledWith(expected);
  });
});
