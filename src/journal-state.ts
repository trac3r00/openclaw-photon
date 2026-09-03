export interface DispatchLease {
  readonly expiresAt: number;
  readonly owner: string;
}

export interface IngressRecord {
  readonly agentDispatched?: boolean;
  readonly body: string;
  readonly deliveryState?: "pending" | "send_in_progress" | "unknown_after_send" | "policy_blocked";
  readonly dispatchLease?: DispatchLease;
  readonly enqueuedAt?: number;
  readonly id: string;
  readonly lastError?: string;
  readonly lastOutboundErrorAt?: number;
  readonly nextAttemptAt?: number;
  readonly replayAttempts?: number;
  readonly sender: string;
  readonly stagedReplies?: readonly string[];
  readonly spaceId: string;
  readonly timestamp: number;
}

export interface StoredIngressRecord extends IngressRecord {
  readonly enqueuedAt: number;
}
export interface CompletedRecord { readonly completedAt: number; readonly id: string }
export interface JournalState {
  readonly completed: CompletedRecord[];
  readonly pending: StoredIngressRecord[];
  readonly projectId: string;
}

export function corrupt(detail = "corrupt"): Error {
  return new Error(`Photon ingress journal ${detail}`);
}

function isRecord(value: unknown): value is IngressRecord {
  if (typeof value !== "object" || value === null) return false;
  const agentDispatched = Reflect.get(value, "agentDispatched");
  const deliveryState = Reflect.get(value, "deliveryState");
  const enqueuedAt = Reflect.get(value, "enqueuedAt");
  const lastError = Reflect.get(value, "lastError");
  const lastOutboundErrorAt = Reflect.get(value, "lastOutboundErrorAt");
  const nextAttemptAt = Reflect.get(value, "nextAttemptAt");
  const replayAttempts = Reflect.get(value, "replayAttempts");
  const stagedReply = Reflect.get(value, "stagedReply");
  const stagedReplies = Reflect.get(value, "stagedReplies");
  const lease = Reflect.get(value, "dispatchLease");
  const validLease = lease === undefined || (typeof lease === "object" && lease !== null &&
    typeof Reflect.get(lease, "owner") === "string" &&
    typeof Reflect.get(lease, "expiresAt") === "number");
  return typeof Reflect.get(value, "body") === "string" && validLease &&
    (agentDispatched === undefined || typeof agentDispatched === "boolean") &&
    (deliveryState === undefined || deliveryState === "pending" ||
      deliveryState === "send_in_progress" || deliveryState === "unknown_after_send" ||
      deliveryState === "policy_blocked") &&
    (enqueuedAt === undefined || typeof enqueuedAt === "number") &&
    (lastError === undefined || typeof lastError === "string") &&
    (lastOutboundErrorAt === undefined || typeof lastOutboundErrorAt === "number") &&
    (nextAttemptAt === undefined || typeof nextAttemptAt === "number") &&
    (replayAttempts === undefined || (Number.isSafeInteger(replayAttempts) && replayAttempts >= 0)) &&
    typeof Reflect.get(value, "id") === "string" &&
    typeof Reflect.get(value, "sender") === "string" &&
    (stagedReply === undefined || typeof stagedReply === "string") &&
    (stagedReplies === undefined ||
      (Array.isArray(stagedReplies) && stagedReplies.every((reply) => typeof reply === "string"))) &&
    typeof Reflect.get(value, "spaceId") === "string" &&
    typeof Reflect.get(value, "timestamp") === "number";
}

export function parseState(content: string, projectId: string, legacy: boolean): JournalState {
  let value: unknown;
  try { value = JSON.parse(content); } catch { throw corrupt(); }
  if (typeof value !== "object" || value === null) throw corrupt();
  const owner = Reflect.get(value, "projectId");
  if ((!legacy && owner !== projectId) ||
    (legacy && owner !== undefined && owner !== projectId)) throw corrupt("ownership mismatch");
  const pending = Reflect.get(value, "pending");
  const completed = Reflect.get(value, "completed");
  if (!Array.isArray(pending) || !pending.every(isRecord) || !Array.isArray(completed)) throw corrupt();
  const validCompleted = completed.filter((entry): entry is CompletedRecord =>
    typeof entry === "object" && entry !== null &&
    typeof Reflect.get(entry, "id") === "string" &&
    typeof Reflect.get(entry, "completedAt") === "number");
  if (validCompleted.length !== completed.length) throw corrupt();
  return {
    projectId,
    completed: validCompleted,
    pending: pending.map((entry) => {
      const legacyReply = Reflect.get(entry, "stagedReply");
      const { stagedReply: _discard, ...record } = entry as IngressRecord & { stagedReply?: string };
      const stagedReplies = entry.stagedReplies ??
        (typeof legacyReply === "string" ? [legacyReply] : undefined);
      return {
        ...record,
        enqueuedAt: entry.enqueuedAt ?? entry.timestamp,
        ...(stagedReplies === undefined ? {} : { stagedReplies }),
      };
    }),
  };
}
