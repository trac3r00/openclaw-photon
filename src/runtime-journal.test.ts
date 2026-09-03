import type { OpenClawConfig } from "openclaw/plugin-sdk/channel-core";
import { afterEach, describe, expect, it, vi } from "vitest";
import { resolvePhotonAccount } from "./config.js";
import { processJournaledEvent, processReplayRecord } from "./inbound.js";
import type { IngressJournalStore, IngressRecord } from "./journal.js";
import { SlidingWindowRateGate } from "./rate-limit.js";
import {
  runPhotonAccount,
  type PhotonGatewayContext,
  type PhotonRuntimeDependencies,
} from "./runtime.js";
import type { PhotonInboundMessage, PhotonSpace, PhotonTransport } from "./transport.js";

const stored: IngressRecord = {
  body: "saved",
  id: "saved-1",
  sender: "+14155550123",
  spaceId: "space-1",
  timestamp: 1_788_364_800_000,
};

function deferred(): { promise: Promise<void>; resolve(): void } {
  let resolvePromise: (() => void) | undefined;
  const promise = new Promise<void>((resolve) => { resolvePromise = resolve; });
  return { promise, resolve: () => resolvePromise?.() };
}

function context(
  abortSignal = new AbortController().signal,
  allowFrom: string[] = [stored.sender],
): PhotonGatewayContext {
  const cfg: OpenClawConfig = { channels: { photon: { allowFrom } } };
  return {
    abortSignal,
    account: resolvePhotonAccount(cfg, {
      OPENCLAW_PHOTON_PROJECT_ID: "project",
      OPENCLAW_PHOTON_PROJECT_SECRET: "secret",
    }),
    cfg,
    getStatus: () => ({ accountId: "default" }),
    log: { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() },
    setStatus: vi.fn(),
  };
}

function messages(events: readonly PhotonInboundMessage[]): AsyncIterable<PhotonInboundMessage> {
  return { async *[Symbol.asyncIterator]() { yield* events; } };
}

function blockingAfterReplay(done: Promise<void>): AsyncIterable<PhotonInboundMessage> {
  return { async *[Symbol.asyncIterator]() { await done; } };
}

function transport(events: readonly PhotonInboundMessage[], space: PhotonSpace): PhotonTransport {
  return {
    messages: messages(events),
    resolveDirectSpace: vi.fn(async () => space),
    stop: vi.fn(async () => undefined),
  };
}

describe("Photon durable ingress", () => {
  afterEach(() => vi.useRealTimers());

  it("journals before receipt and agent execution, then completes successful work", async () => {
    const order: string[] = [];
    const space: PhotonSpace = {
      id: "space-1",
      type: "dm",
      send: vi.fn(async () => ({ id: "control" })),
    };
    const event: PhotonInboundMessage = {
      body: "live",
      direction: "inbound",
      id: "live-1",
      markRead: vi.fn(async () => { order.push("read"); }),
      senderAddress: stored.sender,
      space,
      timestamp: new Date(stored.timestamp),
    };
    const journal: IngressJournalStore = {
      enqueue: vi.fn(async (): Promise<"accepted"> => { order.push("journal"); return "accepted"; }),
      pending: vi.fn(async () => []),
      complete: vi.fn(async () => { order.push("complete"); }),
      stageReply: vi.fn(async () => undefined),
    };
    await runPhotonAccount(context(), {
      createTransport: async () => transport([event], space),
      dispatchInbound: vi.fn(async () => { order.push("dispatch"); }),
      journal,
      now: () => stored.timestamp,
    });
    expect(order).toEqual(["journal", "read", "dispatch", "complete"]);
  });

  it("drains event B to durable storage while event A is slow, then replays after abort", async () => {
    const abort = new AbortController();
    const releaseA = deferred();
    const dispatchAStarted = deferred();
    const dispatchAReturned = deferred();
    const bJournaled = deferred();
    const streamStopped = deferred();
    const pending: IngressRecord[] = [];
    const journal: IngressJournalStore = {
      enqueue: vi.fn(async (record): Promise<"accepted"> => {
        pending.push(record);
        if (record.id === "live-b") bJournaled.resolve();
        return "accepted";
      }),
      pending: vi.fn(async () => [...pending]),
      complete: vi.fn(async (id) => {
        const index = pending.findIndex((record) => record.id === id);
        if (index >= 0) pending.splice(index, 1);
      }),
      stageReply: vi.fn(async (id, reply) => {
        const index = pending.findIndex((record) => record.id === id);
        const current = pending[index];
        if (index >= 0 && current) {
          pending[index] = { ...current, stagedReplies: [...(current.stagedReplies ?? []), reply] };
        }
      }),
    };
    const space: PhotonSpace = {
      id: "space-1",
      type: "dm",
      send: vi.fn(async () => ({ id: "control" })),
    };
    const liveEvent = (id: string): PhotonInboundMessage => ({
      body: id,
      direction: "inbound",
      id,
      markRead: vi.fn(async () => undefined),
      senderAddress: stored.sender,
      space,
      timestamp: new Date(stored.timestamp),
    });
    const liveTransport: PhotonTransport = {
      messages: {
        async *[Symbol.asyncIterator]() {
          yield liveEvent("live-a");
          yield liveEvent("live-b");
          await streamStopped.promise;
        },
      },
      resolveDirectSpace: vi.fn(async () => space),
      stop: vi.fn(async () => streamStopped.resolve()),
    };
    const dispatchInbound = vi.fn(async (params) => {
      if (params.messageId === "live-a") {
        dispatchAStarted.resolve();
        await releaseA.promise;
        dispatchAReturned.resolve();
      }
    });
    const lifecycle = runPhotonAccount(context(abort.signal), {
      createTransport: async () => liveTransport,
      dispatchInbound,
      journal,
    });
    await dispatchAStarted.promise;
    await bJournaled.promise;
    expect(pending.map((record) => record.id)).toEqual(["live-a", "live-b"]);

    abort.abort();
    await lifecycle;
    releaseA.resolve();
    await dispatchAReturned.promise;
    await dispatchInbound.mock.results[0]?.value;
    expect(pending.map((record) => record.id)).toEqual(["live-a", "live-b"]);

    const replayed: string[] = [];
    const replay = async (): Promise<void> => await runPhotonAccount(context(), {
      createTransport: async () => transport([], space),
      dispatchInbound: vi.fn(async (params) => { replayed.push(params.messageId); }),
      journal,
    });
    await replay();
    await replay();
    expect(replayed).toEqual(["live-a", "live-b"]);
    expect(pending).toEqual([]);
  }, 1_000);

  it("keeps failed work pending without blocking later queued events", async () => {
    const pending: IngressRecord[] = [];
    const journal: IngressJournalStore = {
      enqueue: vi.fn(async (record): Promise<"accepted"> => {
        pending.push(record);
        return "accepted";
      }),
      pending: vi.fn(async () => []),
      complete: vi.fn(async (id) => {
        const index = pending.findIndex((record) => record.id === id);
        if (index >= 0) pending.splice(index, 1);
      }),
      stageReply: vi.fn(async () => undefined),
    };
    const space: PhotonSpace = {
      id: "space-1",
      type: "dm",
      send: vi.fn(async () => ({ id: "control" })),
    };
    const event = (id: string): PhotonInboundMessage => ({
      body: id,
      direction: "inbound",
      id,
      markRead: vi.fn(async () => undefined),
      senderAddress: stored.sender,
      space,
      timestamp: new Date(stored.timestamp),
    });
    const dispatched: string[] = [];
    await runPhotonAccount(context(), {
      createTransport: async () => transport([event("failed"), event("successful")], space),
      dispatchInbound: vi.fn(async (params) => {
        dispatched.push(params.messageId);
        if (params.messageId === "failed") throw new Error("agent failure");
      }),
      journal,
    });
    expect(dispatched).toEqual(["failed", "successful"]);
    expect(pending.map((record) => record.id)).toEqual(["failed"]);
  });

  it("drops replay entries revoked by current policy without resolving transport spaces", async () => {
    const space: PhotonSpace = { id: "space-1", type: "dm", send: vi.fn() };
    const pending = vi.fn(async () => [stored]);
    const complete = vi.fn(async () => undefined);
    const replayTransport = transport([], space);

    await runPhotonAccount(context(undefined, []), {
      createTransport: async () => replayTransport,
      dispatchInbound: vi.fn(),
      journal: {
        enqueue: vi.fn(async (): Promise<"accepted"> => "accepted"), pending, complete,
        stageReply: vi.fn(async () => undefined),
      },
    });

    expect(complete).toHaveBeenCalledWith(stored.id, expect.any(String));
    expect(replayTransport.resolveDirectSpace).not.toHaveBeenCalled();
  });

  it("retains an unresolvable replay record and continues with later records", async () => {
    const second = { ...stored, id: "saved-2", sender: "+14155550124" };
    const completed: string[] = [];
    const dispatched: string[] = [];
    const ctx = context(undefined, [stored.sender, second.sender]);
    const space: PhotonSpace = { id: "space-2", type: "dm", send: vi.fn(async () => ({ id: "sent" })) };
    const replayTransport = transport([], space);
    vi.mocked(replayTransport.resolveDirectSpace)
      .mockRejectedValueOnce(new Error("token='secret value'"))
      .mockResolvedValueOnce(space);

    await runPhotonAccount(ctx, {
      createTransport: async () => replayTransport,
      dispatchInbound: vi.fn(async (params) => { dispatched.push(params.messageId); }),
      journal: {
        enqueue: vi.fn(async (): Promise<"duplicate"> => "duplicate"),
        pending: vi.fn(async () => [stored, second]),
        complete: vi.fn(async (id) => { completed.push(id); }),
        stageReply: vi.fn(async () => undefined),
      },
    });

    expect(dispatched).toEqual([second.id]);
    expect(completed).toEqual([second.id]);
    expect(ctx.log?.warn).toHaveBeenCalledWith(expect.stringContaining("[REDACTED]"));
  });

  it("does not concurrently replay an event still dispatching in a superseded generation", async () => {
    const release = deferred();
    const started = deferred();
    const streamEnded = deferred();
    let pendingRecords: IngressRecord[] = [stored];
    const journal: IngressJournalStore = {
      enqueue: vi.fn(async (): Promise<"duplicate"> => "duplicate"),
      pending: vi.fn(async () => [...pendingRecords]),
      complete: vi.fn(async (id) => {
        pendingRecords = pendingRecords.filter((entry) => entry.id !== id);
      }),
      stageReply: vi.fn(async (id, reply) => {
        pendingRecords = pendingRecords.map((entry) => entry.id === id
          ? { ...entry, stagedReplies: [...(entry.stagedReplies ?? []), reply] }
          : entry);
      }),
    };
    const send = vi.fn(async () => ({ id: "control" }));
    const space: PhotonSpace = { id: stored.spaceId, type: "dm", send };
    const oldDispatch: PhotonRuntimeDependencies["dispatchInbound"] = vi.fn(async (params) => {
      started.resolve();
      await release.promise;
      await params.deliver({ text: "first stale reply" });
      await params.deliver({ text: "second stale reply" });
    });
    const oldTransport: PhotonTransport = {
      messages: blockingAfterReplay(streamEnded.promise),
      resolveDirectSpace: vi.fn(async () => space),
      stop: vi.fn(async () => streamEnded.resolve()),
    };
    const oldRun = runPhotonAccount(context(), {
      createTransport: async () => oldTransport,
      dispatchInbound: oldDispatch,
      journal,
    });
    await started.promise;

    const replacementDispatch = vi.fn(async () => undefined);
    const replacementRun = runPhotonAccount(context(), {
      createTransport: async () => transport([], space),
      dispatchInbound: replacementDispatch,
      journal,
    });
    await oldRun;
    expect(oldDispatch).toHaveBeenCalledOnce();
    expect(replacementDispatch).not.toHaveBeenCalled();

    release.resolve();
    await replacementRun;
    expect(replacementDispatch).not.toHaveBeenCalled();
    expect(pendingRecords).toEqual([stored]);
    expect(oldDispatch).toHaveBeenCalledOnce();
    expect(send).toHaveBeenCalledTimes(2);
    expect(pendingRecords).toEqual([stored]);
  });

  it("clears local in-flight state even when fenced release fails", async () => {
    let current: IngressRecord | undefined = stored;
    let owner = "";
    const journal: IngressJournalStore = {
      claim: vi.fn(async (_id, nextOwner) => {
        if (!current) return "missing";
        owner = nextOwner;
        current = { ...current, dispatchLease: { owner, expiresAt: 30_000 } };
        return "claimed";
      }),
      complete: vi.fn(async (_id, candidate) => {
        if (candidate !== owner) return false;
        current = undefined;
        return true;
      }),
      enqueue: vi.fn(async (): Promise<"duplicate"> => "duplicate"),
      markAgentDispatched: vi.fn(async (_id, candidate) => {
        if (candidate !== owner || !current) return false;
        current = { ...current, agentDispatched: true, stagedReplies: [] };
        return true;
      }),
      pending: vi.fn(async () => current ? [current] : []),
      releaseClaim: vi.fn(async () => { throw new Error("release failed"); }),
      renewClaim: vi.fn(async (_id, candidate) => candidate === owner),
      stageReply: vi.fn(async () => true),
    };
    const space: PhotonSpace = { id: stored.spaceId, type: "dm", send: vi.fn() };
    const event: PhotonInboundMessage = {
      body: stored.body, direction: "inbound", id: stored.id,
      markRead: vi.fn(async () => undefined), senderAddress: stored.sender,
      space, timestamp: new Date(stored.timestamp),
    };
    const ctx = context();
    const base = { ctx, event, journal, rateGate: new SlidingWindowRateGate(), isCurrent: () => true };
    await expect(processJournaledEvent({
      ...base,
      dependencies: { createTransport: vi.fn(), dispatchInbound: vi.fn(async () => { throw new Error("agent"); }) },
    })).rejects.toThrow("agent");
    const dispatchInbound = vi.fn(async () => undefined);

    await processJournaledEvent({
      ...base, dependencies: { createTransport: vi.fn(), dispatchInbound },
    });

    expect(dispatchInbound).toHaveBeenCalledOnce();
    expect(ctx.log?.warn).toHaveBeenCalledWith(expect.stringContaining("release failed"));
  });

  it("revokes a failed stream before a late agent delivery", async () => {
    const dispatchStarted = deferred();
    const releaseDispatch = deferred();
    const deliveryFinished = deferred();
    const space: PhotonSpace = {
      id: stored.spaceId,
      type: "dm",
      send: vi.fn(async () => ({ id: "control" })),
    };
    const event: PhotonInboundMessage = {
      body: stored.body,
      direction: "inbound",
      id: stored.id,
      markRead: vi.fn(async () => undefined),
      senderAddress: stored.sender,
      space,
      timestamp: new Date(stored.timestamp),
    };
    const failedTransport: PhotonTransport = {
      messages: {
        async *[Symbol.asyncIterator]() {
          yield event;
          await dispatchStarted.promise;
          throw new Error("stream failed");
        },
      },
      resolveDirectSpace: vi.fn(async () => space),
      stop: vi.fn(async () => undefined),
    };
    const lifecycle = runPhotonAccount(context(), {
      createTransport: async () => failedTransport,
      dispatchInbound: vi.fn(async (params) => {
        dispatchStarted.resolve();
        await releaseDispatch.promise;
        await params.deliver({ text: "too late" });
        deliveryFinished.resolve();
      }),
    });

    await expect(lifecycle).rejects.toThrow("stream failed");
    releaseDispatch.resolve();
    await deliveryFinished.promise;
    expect(space.send).toHaveBeenCalledTimes(2);
  });

  it("completes a dispatched record with an empty staged array without rerunning the agent", async () => {
    let current: IngressRecord | undefined = { ...stored, agentDispatched: true, stagedReplies: [] };
    const journal: IngressJournalStore = {
      claim: vi.fn(async (_id, owner) => {
        if (!current) return "missing";
        current = { ...current, dispatchLease: { owner, expiresAt: 30_000 } };
        return "claimed";
      }),
      complete: vi.fn(async (_id, owner) => {
        if (current?.dispatchLease?.owner !== owner) return false;
        current = undefined;
        return true;
      }),
      enqueue: vi.fn(async (): Promise<"duplicate"> => "duplicate"),
      markAgentDispatched: vi.fn(async () => true),
      pending: vi.fn(async () => current ? [current] : []),
      releaseClaim: vi.fn(async () => true),
      renewClaim: vi.fn(async (_id, owner) => current?.dispatchLease?.owner === owner),
      stageReply: vi.fn(async () => true),
    };
    const space: PhotonSpace = { id: stored.spaceId, type: "dm", send: vi.fn() };
    const dispatchInbound = vi.fn();

    await runPhotonAccount(context(), {
      createTransport: async () => transport([], space), dispatchInbound, journal,
    });

    expect(dispatchInbound).not.toHaveBeenCalled();
    expect(current).toBeUndefined();
  });

  it("actively claims expired work without restart or another provider event", async () => {
    vi.useFakeTimers();
    let now = 0;
    let pending: IngressRecord[] = [{
      ...stored, dispatchLease: { owner: "crashed-worker", expiresAt: 100 },
    }];
    const dispatched = deferred();
    const streamOwned = deferred();
    const abort = new AbortController();
    const journal: IngressJournalStore = {
      claim: vi.fn(async (id, owner, leaseMs) => {
        const entry = pending.find((item) => item.id === id);
        if (!entry) return "missing";
        if (entry.dispatchLease && entry.dispatchLease.owner !== owner &&
          entry.dispatchLease.expiresAt > now) return "busy";
        pending = pending.map((item) => item.id === id
          ? { ...item, dispatchLease: { owner, expiresAt: now + leaseMs } }
          : item);
        return "claimed";
      }),
      complete: vi.fn(async (id, owner) => {
        const entry = pending.find((item) => item.id === id);
        if (entry?.dispatchLease?.owner !== owner) return false;
        pending = pending.filter((item) => item.id !== id);
        return true;
      }),
      enqueue: vi.fn(async (): Promise<"duplicate"> => "duplicate"),
      markAgentDispatched: vi.fn(async (id, owner) => {
        const entry = pending.find((item) => item.id === id);
        if (entry?.dispatchLease?.owner !== owner) return false;
        pending = pending.map((item) => item.id === id
          ? { ...item, agentDispatched: true, stagedReplies: [] }
          : item);
        return true;
      }),
      pending: vi.fn(async () => [...pending]),
      releaseClaim: vi.fn(async () => true),
      renewClaim: vi.fn(async (id, owner, leaseMs) => {
        const entry = pending.find((item) => item.id === id);
        if (entry?.dispatchLease?.owner !== owner) return false;
        pending = pending.map((item) => item.id === id
          ? { ...item, dispatchLease: { owner, expiresAt: now + leaseMs } }
          : item);
        return true;
      }),
      stageReply: vi.fn(async () => true),
    };
    const space: PhotonSpace = { id: stored.spaceId, type: "dm", send: vi.fn() };
    const next = new Promise<IteratorResult<PhotonInboundMessage>>(() => undefined);
    const lifecycle = runPhotonAccount(context(abort.signal), {
      createTransport: async () => ({
        messages: {
          [Symbol.asyncIterator]() {
            return {
              next: async () => { streamOwned.resolve(); return await next; },
              return: async () => ({ done: true, value: undefined }),
            };
          },
        },
        resolveDirectSpace: vi.fn(async () => space),
        stop: vi.fn(async () => undefined),
      }),
      dispatchInbound: vi.fn(async () => { dispatched.resolve(); }),
      journal,
      now: () => now,
      pendingScanMs: 10,
    });
    await streamOwned.promise;
    expect(dispatched.promise).not.toBeUndefined();

    now = 101;
    await vi.advanceTimersByTimeAsync(10);
    await dispatched.promise;
    expect(pending).toEqual([]);

    abort.abort();
    await lifecycle;
  });

  it("deduplicates a slow pending event across repeated scans", async () => {
    vi.useFakeTimers();
    const abort = new AbortController();
    const release = deferred();
    const started = deferred();
    const completed = deferred();
    const streamDone = deferred();
    let current: IngressRecord | undefined = stored;
    const journal: IngressJournalStore = {
      claim: vi.fn(async (_id, owner) => {
        if (!current) return "missing";
        current = { ...current, dispatchLease: { owner, expiresAt: Date.now() + 30_000 } };
        return "claimed";
      }),
      complete: vi.fn(async () => { current = undefined; completed.resolve(); return true; }),
      enqueue: vi.fn(async (): Promise<"duplicate"> => "duplicate"),
      markAgentDispatched: vi.fn(async () => {
        if (!current) return false;
        current = { ...current, agentDispatched: true, stagedReplies: [] };
        return true;
      }),
      pending: vi.fn(async () => current ? [current] : []),
      releaseClaim: vi.fn(async () => true),
      renewClaim: vi.fn(async () => true),
      stageReply: vi.fn(async () => true),
    };
    const space: PhotonSpace = { id: stored.spaceId, type: "dm", send: vi.fn() };
    const lifecycle = runPhotonAccount(context(abort.signal), {
      createTransport: async () => ({
        messages: blockingAfterReplay(streamDone.promise),
        resolveDirectSpace: vi.fn(async () => space),
        stop: vi.fn(async () => streamDone.resolve()),
      }),
      dispatchInbound: vi.fn(async () => { started.resolve(); await release.promise; }),
      journal,
      pendingScanMs: 10,
    });
    await started.promise;
    await vi.advanceTimersByTimeAsync(100);
    release.resolve();
    await completed.promise;
    await vi.advanceTimersByTimeAsync(1);

    expect(journal.claim).toHaveBeenCalledOnce();
    abort.abort();
    await lifecycle;
  });

  it("releases a replay claim immediately when generation revokes during space resolution", async () => {
    const generation = new AbortController();
    const claimed = deferred();
    const releaseClaim = vi.fn(async () => true);
    const deferReplay = vi.fn(async () => true);
    const journal: IngressJournalStore = {
      claim: vi.fn(async (): Promise<"claimed"> => { claimed.resolve(); return "claimed"; }),
      complete: vi.fn(async () => true),
      deferReplay,
      enqueue: vi.fn(async (): Promise<"duplicate"> => "duplicate"),
      pending: vi.fn(async () => [stored]),
      releaseClaim,
      stageReply: vi.fn(async () => true),
    };
    const processing = processReplayRecord({
      ctx: context(),
      dependencies: { createTransport: vi.fn(), dispatchInbound: vi.fn() },
      isCurrent: () => !generation.signal.aborted,
      journal,
      rateGate: new SlidingWindowRateGate(),
      record: stored,
      signal: generation.signal,
      transport: {
        messages: blockingAfterReplay(new Promise<void>(() => undefined)),
        resolveDirectSpace: vi.fn(async () => await new Promise<PhotonSpace>(() => undefined)),
        stop: vi.fn(async () => undefined),
      },
    });
    await claimed.promise;

    generation.abort();
    await processing;

    expect(releaseClaim).toHaveBeenCalledWith(stored.id, expect.any(String));
    expect(deferReplay).not.toHaveBeenCalled();
  });

  it("releases a pre-acquired replay claim when generation is revoked", async () => {
    const releaseClaim = vi.fn(async () => true);
    const journal: IngressJournalStore = {
      complete: vi.fn(async () => true),
      enqueue: vi.fn(async (): Promise<"duplicate"> => "duplicate"),
      pending: vi.fn(async () => [stored]),
      releaseClaim,
      stageReply: vi.fn(async () => true),
    };
    const event: PhotonInboundMessage = {
      body: stored.body,
      direction: "inbound",
      id: stored.id,
      markRead: vi.fn(async () => undefined),
      senderAddress: stored.sender,
      space: { id: stored.spaceId, type: "dm", send: vi.fn() },
      timestamp: new Date(stored.timestamp),
    };

    await processJournaledEvent({
      ctx: context(),
      dependencies: { createTransport: vi.fn(), dispatchInbound: vi.fn() },
      event,
      isCurrent: () => false,
      journal,
      rateGate: new SlidingWindowRateGate(),
      claimedOwner: "replay-owner",
    });

    expect(releaseClaim).toHaveBeenCalledWith(stored.id, "replay-owner");
  });

  it("replays pending work before consuming new provider messages", async () => {
    const order: string[] = [];
    const space: PhotonSpace = {
      id: "space-1",
      type: "dm",
      send: vi.fn(async () => ({ id: "control" })),
    };
    const journal: IngressJournalStore = {
      enqueue: vi.fn(async (): Promise<"accepted"> => "accepted"),
      pending: vi.fn(async () => [stored]),
      complete: vi.fn(async (id) => { order.push(`complete:${id}`); }),
      stageReply: vi.fn(async () => undefined),
    };
    await runPhotonAccount(context(), {
      createTransport: async () => transport([], space),
      dispatchInbound: vi.fn(async (params) => { order.push(`dispatch:${params.messageId}`); }),
      journal,
    });
    expect(order).toEqual(["dispatch:saved-1", "complete:saved-1"]);
  });
});
