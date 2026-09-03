import { randomUUID } from "node:crypto";
import { settlePhotonOperation } from "./bounded-operation.js";
import { normalizeE164 } from "./config.js";
import { DISPATCH_LEASE_MS } from "./dispatch-lease.js";
import type { IngressJournalStore, IngressRecord } from "./journal.js";
import type { PhotonGatewayContext } from "./runtime.js";

const MAX_INBOUND_LENGTH = 16_000;
const RETRY_BASE_MS = 5_000;
const RETRY_MAX_MS = 5 * 60_000;

export interface ReplayScanState {
  readonly reportedStates: Map<string, string>;
}

export function createReplayScanState(): ReplayScanState {
  return { reportedStates: new Map() };
}

export function replayBackoff(record: IngressRecord, now: number): {
  attempts: number;
  nextAttemptAt: number;
} {
  const attempts = Math.min(32, (record.replayAttempts ?? 0) + 1);
  return {
    attempts,
    nextAttemptAt: now + Math.min(RETRY_MAX_MS, RETRY_BASE_MS * (2 ** (attempts - 1))),
  };
}

function protectedRecord(record: IngressRecord): boolean {
  return record.agentDispatched === true || (record.stagedReplies?.length ?? 0) > 0 ||
    record.deliveryState === "send_in_progress" || record.deliveryState === "unknown_after_send" ||
    record.deliveryState === "policy_blocked";
}

async function claimAndMutate(
  journal: IngressJournalStore,
  record: IngressRecord,
  mutate: (owner: string) => Promise<boolean | void> | undefined,
): Promise<boolean> {
  const owner = randomUUID();
  if (journal.claim && await journal.claim(record.id, owner, DISPATCH_LEASE_MS) !== "claimed") return false;
  try { return await mutate(owner) === true; }
  finally { await journal.releaseClaim?.(record.id, owner); }
}

function reportState(ctx: PhotonGatewayContext, state: ReplayScanState, record: IngressRecord): void {
  const reported = record.deliveryState ?? "pending";
  if (state.reportedStates.get(record.id) === reported) return;
  state.reportedStates.set(record.id, reported);
  ctx.log?.error?.(
    `Photon journal entry ${record.id} is ${record.deliveryState}; operator action required`,
  );
}

export async function loadPendingEvents(params: {
  ctx: PhotonGatewayContext;
  generationIsCurrent: () => boolean;
  journal: IngressJournalStore;
  now?: () => number;
  state?: ReplayScanState;
}): Promise<IngressRecord[]> {
  const ready: IngressRecord[] = [];
  const state = params.state ?? createReplayScanState();
  const now = (params.now ?? Date.now)();
  const records = await settlePhotonOperation(
    params.journal.pending(), "pending journal scan", params.ctx.abortSignal,
  );
  const currentIds = new Set(records.map((record) => record.id));
  for (const id of state.reportedStates.keys()) {
    if (!currentIds.has(id)) state.reportedStates.delete(id);
  }
  for (const record of records) {
    if (params.ctx.abortSignal.aborted || !params.generationIsCurrent()) break;
    if (record.deliveryState === "send_in_progress") {
      const changed = await claimAndMutate(params.journal, record, async (owner) =>
        await params.journal.quarantineReply?.(
          record.id, owner, "send interrupted before durable checkpoint", now,
        ));
      if (changed) reportState(params.ctx, state, { ...record, deliveryState: "unknown_after_send" });
      continue;
    }
    if (record.deliveryState === "unknown_after_send" || record.deliveryState === "policy_blocked") {
      reportState(params.ctx, state, record);
      continue;
    }
    const sender = normalizeE164(record.sender);
    const invalid = !sender || sender !== record.sender || !record.body.trim() ||
      record.body.length > MAX_INBOUND_LENGTH || !Number.isFinite(record.timestamp) ||
      params.ctx.account.config.dmPolicy === "disabled" ||
      !params.ctx.account.config.allowFrom.includes(sender);
    if (invalid) {
      const protect = protectedRecord(record);
      const changed = await claimAndMutate(params.journal, record, async (owner) => protect
        ? await params.journal.markPolicyBlocked?.(record.id, owner)
        : await params.journal.complete(record.id, owner));
      if (protect && changed) reportState(params.ctx, state, { ...record, deliveryState: "policy_blocked" });
      continue;
    }
    if ((record.nextAttemptAt ?? 0) <= now) ready.push(record);
  }
  return ready;
}
