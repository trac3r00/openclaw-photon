import { randomUUID } from "node:crypto";
import { settlePhotonOperation } from "./bounded-operation.js";
import { dispatchInboundDirectDm } from "openclaw/plugin-sdk/channel-inbound";
import { DispatchLeaseGuard, DISPATCH_LEASE_MS } from "./dispatch-lease.js";
import { resolveOutboundAllowFrom } from "./config.js";
import { admitPhotonMessage, updatePhotonTraffic } from "./inbound-admission.js";
import type { PhotonGatewayContext, PhotonRuntimeDependencies } from "./runtime.js";
import { replayBackoff } from "./replay.js";
import type { IngressJournalStore, IngressRecord } from "./journal.js";
import { enforcePhotonOutboundRate } from "./outbound-rate.js";
import type { InboundRateGate } from "./rate-limit.js";
import { drainStagedReplies } from "./reply-drain.js";
import { sanitizeError, sanitizeOutboundText } from "./security.js";
import {
  sendToSpace,
  type PhotonInboundMessage,
  type PhotonTransport,
} from "./transport.js";
import { withPhotonTyping } from "./typing.js";

const inFlightEvents = new Map<string, Promise<boolean>>();
export type InboundDispatchParams = Parameters<typeof dispatchInboundDirectDm>[0];
type InboundDispatcher = (params: InboundDispatchParams) => Promise<unknown>;
export async function handlePhotonInbound(params: {
  ctx: PhotonGatewayContext;
  event: PhotonInboundMessage;
  dispatchInbound: InboundDispatcher;
  isCurrent?: () => boolean;
  now?: () => number;
  journal?: IngressJournalStore;
  lease?: DispatchLeaseGuard;
}): Promise<boolean> {
  const { ctx, event } = params;
  const input = admitPhotonMessage(ctx, event);
  if (!input || params.isCurrent?.() === false) return false;
  const outboundAllowed = resolveOutboundAllowFrom(ctx.account).includes(input.sender);
  if (outboundAllowed) {
    await settlePhotonOperation(event.markRead(), "read receipt", ctx.abortSignal)
      .catch((error: unknown) => ctx.log?.debug?.(
        `Photon read receipt failed: ${sanitizeError(error, [ctx.account.projectSecret])}`,
      ));
  }
  if (params.isCurrent?.() === false) return false;
  updatePhotonTraffic(ctx, "lastInboundAt", (params.now ?? Date.now)());
  const dispatch = async () => {
    await params.dispatchInbound({
      channelIngress: "unsupported",
      cfg: ctx.cfg,
      channel: "photon",
      channelLabel: "Photon",
      accountId: "default",
      peer: { kind: "direct", id: input.sender },
      senderId: input.sender,
      senderAddress: `photon:${input.sender}`,
      recipientAddress: "photon:self",
      conversationLabel: input.sender,
      rawBody: input.body,
      messageId: event.id,
      timestamp: event.timestamp.getTime(),
      commandAuthorized: true,
      inboundAccessAuthorized: true,
      deliver: async (payload) => {
        if (!outboundAllowed) return;
        const reply = sanitizeOutboundText(payload.text ?? "");
        if (!reply || params.isCurrent?.() === false) return;
        if (params.journal && params.lease) {
          if (!await params.lease.renew()) return;
          if (!await params.journal.stageReply(event.id, params.lease.owner, reply)) {
            params.lease.lose();
          }
          return;
        }
        enforcePhotonOutboundRate(input.sender);
        if (params.isCurrent?.() === false) return;
        await sendToSpace(event.space, reply, "markdown");
        if (params.isCurrent?.() !== false) {
          updatePhotonTraffic(ctx, "lastOutboundAt", (params.now ?? Date.now)());
        }
      },
      onRecordError: (error) => ctx.log?.error?.(
        `Photon inbound record failed: ${sanitizeError(error, [ctx.account.projectSecret])}`,
      ),
      onDispatchError: (error) => ctx.log?.error?.(
        `Photon inbound dispatch failed: ${sanitizeError(error, [ctx.account.projectSecret])}`,
      ),
    });
  };
  if (outboundAllowed) {
    await withPhotonTyping(event.space, dispatch, undefined, undefined, ctx.abortSignal);
  } else {
    await dispatch();
  }
  return params.isCurrent?.() !== false && (params.lease?.owned ?? true);
}

function journalRecord(event: PhotonInboundMessage, input: { body: string; sender: string }): IngressRecord {
  return {
    body: input.body,
    id: event.id,
    sender: input.sender,
    spaceId: event.space.id,
    timestamp: event.timestamp.getTime(),
  };
}

interface LiveEventParams {
  readonly ctx: PhotonGatewayContext;
  readonly dependencies: PhotonRuntimeDependencies;
  readonly event: PhotonInboundMessage;
  readonly journal: IngressJournalStore;
  readonly rateGate: InboundRateGate;
}

export async function admitLiveEvent(params: LiveEventParams): Promise<boolean> {
  const input = admitPhotonMessage(params.ctx, params.event);
  if (!input) return false;
  const now = (params.dependencies.now ?? Date.now)();
  if (!params.rateGate.admit(input.sender, now)) {
    params.ctx.log?.warn?.("Photon inbound rate limit exceeded");
    return false;
  }
  const result = await params.journal.enqueue(journalRecord(params.event, input));
  if (result !== "accepted") {
    if (result === "full") params.ctx.log?.warn?.("Photon inbound queue is full");
    return false;
  }
  return true;
}

async function acquireClaim(
  journal: IngressJournalStore,
  id: string,
  owner: string,
  isCurrent: () => boolean,
): Promise<boolean> {
  if (!journal.claim) return true;
  if (!isCurrent()) return false;
  return await journal.claim(id, owner, DISPATCH_LEASE_MS) === "claimed";
}

export async function processJournaledEvent(
  params: LiveEventParams & {
    readonly claimedOwner?: string;
    readonly isCurrent: () => boolean;
  },
): Promise<void> {
  const owner = params.claimedOwner ?? randomUUID();
  const releasePreclaimed = async (): Promise<void> => {
    if (!params.claimedOwner) return;
    try { await params.journal.releaseClaim?.(params.event.id, owner); }
    catch (error) {
      params.ctx.log?.warn?.(
        `Photon lease release failed: ${sanitizeError(error, [params.ctx.account.projectSecret])}`,
      );
    }
  };
  if (!params.isCurrent()) {
    await releasePreclaimed();
    return;
  }
  const key = `${params.ctx.account.projectId}:${params.event.id}`;
  if (inFlightEvents.has(key)) {
    await releasePreclaimed();
    return;
  }
  if (!params.claimedOwner &&
    !await acquireClaim(params.journal, params.event.id, owner, params.isCurrent)) return;
  if (!params.journal.claim || !params.journal.markAgentDispatched) {
    const execution = handlePhotonInbound({
      ctx: params.ctx, event: params.event, dispatchInbound: params.dependencies.dispatchInbound,
      isCurrent: params.isCurrent, now: params.dependencies.now,
    }).then(async (handled) => {
      if (handled) await params.journal.complete(params.event.id, owner);
      return handled;
    });
    inFlightEvents.set(key, execution);
    try { await execution; } finally {
      if (inFlightEvents.get(key) === execution) inFlightEvents.delete(key);
    }
    return;
  }
  const lease = new DispatchLeaseGuard(params.journal, params.event.id, owner, params.isCurrent);
  const execution = (async (): Promise<boolean> => {
    lease.start();
    try {
      const stored = (await params.journal.pending()).find((entry) => entry.id === params.event.id);
      if (!stored && params.journal.claim) return true;
      if (stored?.agentDispatched) return await drainStagedReplies(params, lease);
      const handled = await handlePhotonInbound({
        ctx: params.ctx,
        event: params.event,
        dispatchInbound: params.dependencies.dispatchInbound,
        isCurrent: () => lease.owned,
        now: params.dependencies.now,
        journal: params.journal,
        lease,
      });
      if (!handled || !await lease.renew()) return false;
      if (!params.journal.markAgentDispatched) return false;
      if (await params.journal.markAgentDispatched(params.event.id, owner) !== true) {
        lease.lose();
        return false;
      }
      return await drainStagedReplies(params, lease);
    } finally {
      lease.stop();
      try { await params.journal.releaseClaim?.(params.event.id, owner); }
      catch (error) {
        params.ctx.log?.warn?.(`Photon lease release failed: ${sanitizeError(error, [params.ctx.account.projectSecret])}`);
      }
    }
  })();
  inFlightEvents.set(key, execution);
  try { await execution; } finally {
    if (inFlightEvents.get(key) === execution) inFlightEvents.delete(key);
  }
}

export async function processReplayRecord(params: Omit<LiveEventParams, "event"> & {
  readonly isCurrent: () => boolean;
  readonly record: IngressRecord;
  readonly signal: AbortSignal;
  readonly transport: PhotonTransport;
}): Promise<void> {
  if (!params.isCurrent()) return;
  const owner = randomUUID();
  if (!await acquireClaim(params.journal, params.record.id, owner, params.isCurrent)) return;
  let space;
  try {
    space = await settlePhotonOperation(
      params.transport.resolveDirectSpace(params.record.sender),
      "replay space resolution",
      params.signal,
    );
  } catch (error) {
    if (params.signal.aborted || !params.isCurrent()) {
      await params.journal.releaseClaim?.(params.record.id, owner);
      return;
    }
    const now = (params.dependencies.now ?? Date.now)();
    const deferred = replayBackoff(params.record, now);
    try {
      await params.journal.deferReplay?.(
        params.record.id, owner, deferred.nextAttemptAt, deferred.attempts,
      );
      params.ctx.log?.warn?.(
        `Photon replay space resolution failed: ${sanitizeError(error, [params.ctx.account.projectSecret])}`,
      );
    } finally {
      await params.journal.releaseClaim?.(params.record.id, owner);
    }
    return;
  }
  if (params.signal.aborted || !params.isCurrent()) {
    await params.journal.releaseClaim?.(params.record.id, owner);
    return;
  }
  const event: PhotonInboundMessage = {
    body: params.record.body,
    direction: "inbound",
    id: params.record.id,
    markRead: async () => undefined,
    senderAddress: params.record.sender,
    space,
    timestamp: new Date(params.record.timestamp),
  };
  await processJournaledEvent({ ...params, event, claimedOwner: owner });
}

export async function processLiveEvent(params: LiveEventParams): Promise<void> {
  if (!await admitLiveEvent(params)) return;
  await processJournaledEvent({ ...params, isCurrent: () => true });
}
