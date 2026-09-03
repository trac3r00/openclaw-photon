import type { OpenClawConfig } from "openclaw/plugin-sdk/channel-core";
import { afterEach, describe, expect, it, vi } from "vitest";
import { resolvePhotonAccount } from "./config.js";
import type { IngressJournalStore, IngressRecord } from "./journal.js";
import { SlidingWindowRateGate } from "./rate-limit.js";
import { createReplayScanState, loadPendingEvents } from "./replay.js";
import { runPhotonIngress } from "./runtime-ingress.js";
import type { PhotonGatewayContext } from "./runtime.js";
import type { PhotonInboundMessage, PhotonSpace, PhotonTransport } from "./transport.js";

const record: IngressRecord = {
  body: "saved", id: "saved-1", sender: "+14155550123", spaceId: "space-1", timestamp: 100,
};

function deferred(): { promise: Promise<void>; resolve(): void } {
  let resolvePromise: (() => void) | undefined;
  const promise = new Promise<void>((resolve) => { resolvePromise = resolve; });
  return { promise, resolve: () => resolvePromise?.() };
}

function context(): PhotonGatewayContext {
  const cfg: OpenClawConfig = { channels: { photon: { allowFrom: [record.sender] } } };
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

const space: PhotonSpace = { id: "space-1", type: "dm", send: vi.fn() };
describe("Photon replay controls", () => {
  afterEach(() => vi.useRealTimers());

  it("scans replay records without provider access, honors durable backoff, and reports quarantine once", async () => {
    let now = 0;
    const ctx = context();
    const state = createReplayScanState();
    const pending = vi.fn(async () => [
      { ...record, nextAttemptAt: 5_000, replayAttempts: 1 },
      { ...record, id: "quarantine", deliveryState: "unknown_after_send" as const },
    ]);
    const resolveDirectSpace = vi.fn(async () => { throw new Error("must not resolve during scan"); });
    const journal: IngressJournalStore = {
      complete: vi.fn(async () => true),
      enqueue: vi.fn(async () => "duplicate" as const),
      pending,
      stageReply: vi.fn(async () => true),
    };
    const params = { ctx, generationIsCurrent: () => true, journal, now: () => now, state };

    await expect(loadPendingEvents(params)).resolves.toEqual([]);
    await expect(loadPendingEvents(params)).resolves.toEqual([]);
    expect(ctx.log?.error).toHaveBeenCalledTimes(1);
    now = 5_000;
    await expect(loadPendingEvents(params)).resolves.toEqual([
      expect.objectContaining({ id: record.id, replayAttempts: 1 }),
    ]);
    expect(resolveDirectSpace).not.toHaveBeenCalled();
    expect(ctx.log?.error).toHaveBeenCalledTimes(1);
  });

  it("retains staged work as policy_blocked when policy changes", async () => {
    const ctx = context();
    ctx.account.config.allowFrom.splice(0);
    const staged: IngressRecord = {
      ...record, agentDispatched: true, stagedReplies: ["reply"],
    };
    const complete = vi.fn(async () => true);
    const markPolicyBlocked = vi.fn(async () => true);
    const journal: IngressJournalStore = {
      claim: vi.fn(async () => "claimed" as const),
      complete,
      enqueue: vi.fn(async () => "duplicate" as const),
      markPolicyBlocked,
      pending: vi.fn(async () => [staged]),
      releaseClaim: vi.fn(async () => true),
      stageReply: vi.fn(async () => true),
    };

    await expect(loadPendingEvents({
      ctx, generationIsCurrent: () => true, journal,
    })).resolves.toEqual([]);
    expect(markPolicyBlocked).toHaveBeenCalledWith(staged.id, expect.any(String));
    expect(complete).not.toHaveBeenCalled();
    expect(ctx.log?.error).toHaveBeenCalledOnce();
  });

  it("consumes and journals live yields while the initial pending scan is hung", async () => {
    vi.useFakeTimers();
    const journaled = deferred();
    const ctx = context();
    const event: PhotonInboundMessage = {
      body: "live", direction: "inbound", id: "live-1", markRead: vi.fn(async () => undefined),
      senderAddress: record.sender, space, timestamp: new Date(100),
    };
    const journal: IngressJournalStore = {
      complete: vi.fn(async () => true),
      enqueue: vi.fn(async () => { journaled.resolve(); return "accepted" as const; }),
      pending: vi.fn(async () => await new Promise<IngressRecord[]>(() => undefined)),
      stageReply: vi.fn(async () => true),
    };
    const transport: PhotonTransport = {
      messages: { async *[Symbol.asyncIterator]() { yield event; } },
      resolveDirectSpace: vi.fn(async () => space),
      stop: vi.fn(async () => undefined),
    };
    const running = runPhotonIngress({
      ctx,
      dependencies: { createTransport: vi.fn(), dispatchInbound: vi.fn(async () => undefined) },
      isCurrent: () => true,
      journal,
      lifecycleSignal: new AbortController().signal,
      rateGate: new SlidingWindowRateGate(),
      transport,
    });

    expect(journal.enqueue).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(5_000);
    await journaled.promise;
    expect(journal.enqueue).toHaveBeenCalledOnce();
    await running;
  });
});
