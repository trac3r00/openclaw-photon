import { createHash } from "node:crypto";
import { readFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { OpenClawConfig } from "openclaw/plugin-sdk/channel-core";
import { PlatformMessageNotDispatchedError } from "openclaw/plugin-sdk/error-runtime";
import { afterEach, describe, expect, it, vi } from "vitest";
import { resolvePhotonAccount } from "./config.js";
import { admitLiveEvent, processJournaledEvent } from "./inbound.js";
import { IngressJournal } from "./journal.js";
import { SlidingWindowRateGate } from "./rate-limit.js";
import type { PhotonGatewayContext, PhotonRuntimeDependencies } from "./runtime.js";
import type { PhotonInboundMessage, PhotonSpace } from "./transport.js";

const homes: string[] = [];
const sender = "+14155550123";

function context(): PhotonGatewayContext {
  const cfg: OpenClawConfig = { channels: { photon: { allowFrom: [sender] } } };
  return {
    abortSignal: new AbortController().signal,
    account: resolvePhotonAccount(cfg, {
      OPENCLAW_PHOTON_PROJECT_ID: "concrete-project",
      OPENCLAW_PHOTON_PROJECT_SECRET: "secret",
    }),
    cfg,
    getStatus: () => ({ accountId: "default" }),
    log: { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() },
    setStatus: vi.fn(),
  };
}

async function fixture(send: PhotonSpace["send"]) {
  const home = await mkdtemp(join(tmpdir(), "photon-runtime-journal-"));
  homes.push(home);
  const ctx = context();
  const journal = new IngressJournal(home, ctx.account.projectId);
  const space: PhotonSpace = { id: "space-1", type: "dm", send };
  const event: PhotonInboundMessage = {
    body: "hello", direction: "inbound", id: "message-1",
    markRead: vi.fn(async () => undefined), senderAddress: sender,
    space, timestamp: new Date("2026-09-02T12:00:00Z"),
  };
  const rateGate = new SlidingWindowRateGate();
  return { ctx, event, home, journal, rateGate };
}

async function state(home: string): Promise<{
  completed: readonly { id: string }[];
  pending: readonly { deliveryState?: string; stagedReplies?: readonly string[] }[];
}> {
  const scope = createHash("sha256").update("concrete-project").digest("hex");
  return JSON.parse(await readFile(
    join(home, ".openclaw", "photon", "projects", scope, "ingress.json"), "utf8",
  )) as {
    completed: readonly { id: string }[];
    pending: readonly { deliveryState?: string; stagedReplies?: readonly string[] }[];
  };
}

describe("Photon concrete ingress journal runtime", () => {
  afterEach(async () => {
    await Promise.all(homes.splice(0).map(async (home) => await rm(home, { recursive: true, force: true })));
  });

  it("runs one agent, sends ordered staged replies once, completes, and ignores duplicate retry", async () => {
    const sent: string[] = [];
    const run = await fixture(async (content) => {
      const built = typeof content === "string" ? { text: content } : await content.build();
      const body = Reflect.get(built, "markdown") ?? Reflect.get(built, "text");
      if (typeof body === "string") sent.push(body);
      return { id: `sent-${sent.length}` };
    });
    const dispatchInbound: PhotonRuntimeDependencies["dispatchInbound"] = vi.fn(async (params) => {
      await params.deliver({ text: "first" });
      await params.deliver({ text: "second" });
    });
    const dependencies = { createTransport: vi.fn(), dispatchInbound };
    const params = { ...run, dependencies };

    expect(await admitLiveEvent(params)).toBe(true);
    await processJournaledEvent({ ...params, isCurrent: () => true });
    expect(await admitLiveEvent(params)).toBe(false);
    await processJournaledEvent({ ...params, isCurrent: () => true });

    expect(dispatchInbound).toHaveBeenCalledOnce();
    expect(sent).toEqual(["first", "second"]);
    expect(await state(run.home)).toMatchObject({ pending: [], completed: [{ id: "message-1" }] });
  });

  it("does not rerun an agent that stages a reply and then throws", async () => {
    const sent: string[] = [];
    const run = await fixture(async (content) => {
      const built = typeof content === "string" ? { text: content } : await content.build();
      const body = Reflect.get(built, "markdown") ?? Reflect.get(built, "text");
      if (typeof body === "string") sent.push(body);
      return { id: "sent" };
    });
    const dispatchInbound: PhotonRuntimeDependencies["dispatchInbound"] = vi.fn(async (params) => {
      await params.deliver({ text: "durable reply" });
      throw new Error("agent crashed after staging");
    });
    const params = { ...run, dependencies: { createTransport: vi.fn(), dispatchInbound } };

    expect(await admitLiveEvent(params)).toBe(true);
    await expect(processJournaledEvent({ ...params, isCurrent: () => true }))
      .rejects.toThrow("agent crashed after staging");
    expect(await state(run.home)).toMatchObject({
      pending: [{ stagedReplies: ["durable reply"] }], completed: [],
    });

    await processJournaledEvent({ ...params, isCurrent: () => true });
    expect(dispatchInbound).toHaveBeenCalledOnce();
    expect(sent).toEqual(["durable reply"]);
    expect(await state(run.home)).toMatchObject({ pending: [], completed: [{ id: "message-1" }] });
  });

  it("quarantines an ambiguous UNKNOWN send and never attempts it again", async () => {
    let replyAttempts = 0;
    const run = await fixture(async (content) => {
      const built = typeof content === "string" ? { text: content } : await content.build();
      if (typeof (Reflect.get(built, "markdown") ?? Reflect.get(built, "text")) === "string") {
        replyAttempts += 1;
        throw Object.assign(new Error("unknown secret"), {
          code: 2,
          retryable: true,
          cause: Object.assign(new Error("connect failed"), {
            code: "ECONNREFUSED", syscall: "connect",
          }),
        });
      }
      return { id: "control" };
    });
    const dispatchInbound: PhotonRuntimeDependencies["dispatchInbound"] = vi.fn(async (params) => {
      await params.deliver({ text: "reply" });
    });
    const outboundHealth = { failure: vi.fn(), success: vi.fn() };
    const params = {
      ...run, dependencies: { createTransport: vi.fn(), dispatchInbound, outboundHealth },
    };

    expect(await admitLiveEvent(params)).toBe(true);
    await processJournaledEvent({ ...params, isCurrent: () => true });
    await processJournaledEvent({ ...params, isCurrent: () => true });

    expect(replyAttempts).toBe(1);
    expect(outboundHealth.failure).toHaveBeenCalledOnce();
    expect(outboundHealth.success).not.toHaveBeenCalled();
    expect(dispatchInbound).toHaveBeenCalledOnce();
    expect(await state(run.home)).toMatchObject({
      pending: [{ deliveryState: "unknown_after_send", stagedReplies: ["reply"] }],
      completed: [],
    });
    expect(run.ctx.log?.error).toHaveBeenCalledWith(expect.stringContaining("operator action"));
  });

  it("quarantines after a successful send whose checkpoint fails and never resends", async () => {
    let replyAttempts = 0;
    const run = await fixture(async (content) => {
      const built = typeof content === "string" ? { text: content } : await content.build();
      if (typeof (Reflect.get(built, "markdown") ?? Reflect.get(built, "text")) === "string") {
        replyAttempts += 1;
      }
      return { id: "sent" };
    });
    const checkpoint = run.journal.checkpointReply.bind(run.journal);
    let failCheckpoint = true;
    run.journal.checkpointReply = async (id, owner) => {
      if (failCheckpoint) {
        failCheckpoint = false;
        throw new Error("checkpoint failed");
      }
      return await checkpoint(id, owner);
    };
    const dispatchInbound: PhotonRuntimeDependencies["dispatchInbound"] = vi.fn(async (params) => {
      await params.deliver({ text: "reply" });
    });
    const params = {
      ...run, dependencies: { createTransport: vi.fn(), dispatchInbound },
    };

    expect(await admitLiveEvent(params)).toBe(true);
    await processJournaledEvent({ ...params, isCurrent: () => true });
    await processJournaledEvent({ ...params, isCurrent: () => true });

    expect(replyAttempts).toBe(1);
    expect(dispatchInbound).toHaveBeenCalledOnce();
    expect(await state(run.home)).toMatchObject({
      pending: [{ deliveryState: "unknown_after_send", stagedReplies: ["reply"] }],
      completed: [],
    });
  });

  it("retains a permanent non-dispatch rejection for operator action without retry", async () => {
    let replyAttempts = 0;
    const run = await fixture(async (content) => {
      const built = typeof content === "string" ? { text: content } : await content.build();
      if (typeof (Reflect.get(built, "markdown") ?? Reflect.get(built, "text")) === "string") {
        replyAttempts += 1;
        throw new PlatformMessageNotDispatchedError("permanent rejection", {
          cause: new Error("policy rejected before dispatch"),
          retryable: false,
        });
      }
      return { id: "control" };
    });
    const dispatchInbound: PhotonRuntimeDependencies["dispatchInbound"] = vi.fn(async (params) => {
      await params.deliver({ text: "reply" });
    });
    const params = {
      ...run,
      dependencies: { createTransport: vi.fn(), dispatchInbound },
    };

    expect(await admitLiveEvent(params)).toBe(true);
    await processJournaledEvent({ ...params, isCurrent: () => true });
    await processJournaledEvent({ ...params, isCurrent: () => true });

    expect(replyAttempts).toBe(1);
    expect(dispatchInbound).toHaveBeenCalledOnce();
    expect(await state(run.home)).toMatchObject({
      pending: [{ deliveryState: "policy_blocked", stagedReplies: ["reply"] }],
      completed: [],
    });
  });

  it("keeps proven non-dispatch pending and safely retries without rerunning the agent", async () => {
    let replyAttempts = 0;
    const run = await fixture(async (content) => {
      const built = typeof content === "string" ? { text: content } : await content.build();
      if (typeof (Reflect.get(built, "markdown") ?? Reflect.get(built, "text")) === "string") {
        replyAttempts += 1;
        if (replyAttempts === 1) {
          throw Object.assign(new Error("pre-connect unavailable"), {
            code: "ECONNREFUSED",
            syscall: "connect",
          });
        }
      }
      return { id: "sent" };
    });
    const dispatchInbound: PhotonRuntimeDependencies["dispatchInbound"] = vi.fn(async (params) => {
      await params.deliver({ text: "reply" });
    });
    const outboundHealth = { failure: vi.fn(), success: vi.fn() };
    const params = {
      ...run, dependencies: { createTransport: vi.fn(), dispatchInbound, outboundHealth },
    };

    expect(await admitLiveEvent(params)).toBe(true);
    await expect(processJournaledEvent({ ...params, isCurrent: () => true })).rejects.toThrow("unavailable");
    await processJournaledEvent({ ...params, isCurrent: () => true });

    expect(replyAttempts).toBe(2);
    expect(outboundHealth.failure).toHaveBeenCalledOnce();
    expect(outboundHealth.success).toHaveBeenCalledOnce();
    expect(dispatchInbound).toHaveBeenCalledOnce();
    expect(await state(run.home)).toMatchObject({ pending: [], completed: [{ id: "message-1" }] });
  });
});
