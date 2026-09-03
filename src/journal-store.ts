import type { IngressRecord, JournalState } from "./journal-state.js";
import type { StorageFilesystem } from "./storage.js";

export interface IngressJournalStore {
  beginReplySend?(id: string, owner: string): Promise<boolean | void>;
  claim?(id: string, owner: string, leaseMs: number): Promise<"busy" | "claimed" | "missing">;
  checkpointReply?(id: string, owner: string): Promise<boolean | void>;
  complete(id: string, owner: string): Promise<boolean | void>;
  enqueue(record: IngressRecord): Promise<"accepted" | "duplicate" | "full">;
  deferReplay?(id: string, owner: string, nextAttemptAt: number, attempts: number): Promise<boolean | void>;
  markAgentDispatched?(id: string, owner: string): Promise<boolean | void>;
  markPolicyBlocked?(id: string, owner: string): Promise<boolean | void>;
  pending(): Promise<IngressRecord[]>;
  quarantineReply?(
    id: string,
    owner: string,
    error: string,
    failedAt: number,
  ): Promise<boolean | void>;
  releaseClaim?(id: string, owner: string): Promise<boolean | void>;
  replyNotDispatched?(id: string, owner: string): Promise<boolean | void>;
  renewClaim?(id: string, owner: string, leaseMs: number): Promise<boolean | void>;
  stageReply(id: string, owner: string, reply: string): Promise<boolean | void>;
}

export interface JournalOptions {
  readonly filesystem?: StorageFilesystem;
  readonly maxCompleted?: number;
  readonly maxPending?: number;
  readonly now?: () => number;
  readonly retentionMs?: number;
}

const pathOperations = new Map<string, Promise<void>>();

export async function withLocalJournalLock<T>(
  key: string,
  operation: () => Promise<T>,
): Promise<T> {
  const previous = pathOperations.get(key) ?? Promise.resolve();
  let release: (() => void) | undefined;
  const next = new Promise<void>((resolve) => { release = resolve; });
  pathOperations.set(key, next);
  await previous;
  try { return await operation(); } finally {
    release?.();
    if (pathOperations.get(key) === next) pathOperations.delete(key);
  }
}

export type { IngressRecord, JournalState };
