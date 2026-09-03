import { createHash } from "node:crypto";
import { dirname, resolve } from "node:path";
import lockfile from "proper-lockfile";
import { corrupt, parseState } from "./journal-state.js";
import {
  withLocalJournalLock,
  type IngressJournalStore,
  type IngressRecord,
  type JournalOptions,
  type JournalState,
} from "./journal-store.js";
import {
  atomicWriteStoredText,
  defaultStorageFilesystem,
  readStoredText,
  type StorageFilesystem,
} from "./storage.js";

export type { DispatchLease } from "./journal-state.js";
export type { IngressJournalStore, IngressRecord } from "./journal-store.js";
const DEFAULT_MAX_PENDING = 100;
const DEFAULT_MAX_COMPLETED = 1_000;
const DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1_000;
const LOCK_OPTIONS = {
  realpath: false,
  stale: 10_000,
  update: 2_000,
  retries: { retries: 30, factor: 1.2, minTimeout: 10, maxTimeout: 100, randomize: true },
} as const;
export class IngressJournal implements IngressJournalStore {
  readonly #filesystem: StorageFilesystem;
  readonly #legacyPath: string;
  readonly #maxCompleted: number;
  readonly #maxPending: number;
  readonly #now: () => number;
  readonly #path: string;
  readonly #projectId: string;
  readonly #retentionMs: number;
  constructor(home: string, projectId: string, options: JournalOptions = {}) {
    this.#filesystem = options.filesystem ?? defaultStorageFilesystem();
    this.#maxCompleted = options.maxCompleted ?? DEFAULT_MAX_COMPLETED;
    this.#maxPending = options.maxPending ?? DEFAULT_MAX_PENDING;
    this.#now = options.now ?? Date.now;
    this.#projectId = projectId;
    const scope = createHash("sha256").update(projectId).digest("hex");
    this.#path = resolve(home, ".openclaw", "photon", "projects", scope, "ingress.json");
    this.#legacyPath = resolve(home, ".openclaw", "photon", "ingress.json");
    this.#retentionMs = options.retentionMs ?? DEFAULT_RETENTION_MS;
  }
  async #save(state: JournalState): Promise<void> {
    await this.#filesystem.mkdir(dirname(this.#path), { recursive: true, mode: 0o700 });
    await atomicWriteStoredText(this.#filesystem, this.#path, `${JSON.stringify(state)}\n`);
  }
  async #read(): Promise<JournalState> {
    const content = await readStoredText(this.#filesystem, this.#path);
    if (content) return parseState(content, this.#projectId, false);
    return await this.#withLock(this.#legacyPath, async () => {
      const legacy = await readStoredText(this.#filesystem, this.#legacyPath);
      if (!legacy) return { projectId: this.#projectId, completed: [], pending: [] };
      const migrated = parseState(legacy, this.#projectId, true);
      await atomicWriteStoredText(
        this.#filesystem,
        this.#legacyPath,
        `${JSON.stringify(migrated)}\n`,
      );
      await this.#save(migrated);
      await this.#filesystem.unlink(this.#legacyPath);
      return migrated;
    });
  }
  async #load(): Promise<{ dirty: boolean; state: JournalState }> {
    const loaded = await this.#read();
    if (loaded.pending.length > this.#maxPending) throw corrupt("exceeds pending capacity");
    const threshold = this.#now() - this.#retentionMs;
    const state: JournalState = {
      projectId: this.#projectId,
      pending: loaded.pending.filter((entry) => entry.enqueuedAt >= threshold ||
        entry.agentDispatched === true || entry.deliveryState === "send_in_progress" ||
        entry.deliveryState === "unknown_after_send" || entry.deliveryState === "policy_blocked" ||
        (entry.stagedReplies?.length ?? 0) > 0),
      completed: loaded.completed
        .filter((entry) => entry.completedAt >= threshold)
        .slice(-this.#maxCompleted),
    };
    return {
      state,
      dirty: state.pending.length !== loaded.pending.length ||
        state.completed.length !== loaded.completed.length,
    };
  }
  async #withLock<T>(path: string, operation: () => Promise<T>): Promise<T> {
    return await withLocalJournalLock(path, async () => {
      await this.#filesystem.mkdir(dirname(path), { recursive: true, mode: 0o700 });
      if (this.#filesystem !== defaultStorageFilesystem()) return await operation();
      const release = await lockfile.lock(path, LOCK_OPTIONS);
      try { return await operation(); } finally { await release(); }
    });
  }
  async #exclusive<T>(operation: () => Promise<T>): Promise<T> {
    return await this.#withLock(this.#path, operation);
  }
  async enqueue(record: IngressRecord): Promise<"accepted" | "duplicate" | "full"> {
    return await this.#exclusive(async () => {
      const { state } = await this.#load();
      if (state.pending.some((entry) => entry.id === record.id) ||
        state.completed.some((entry) => entry.id === record.id)) return "duplicate";
      if (state.pending.length >= this.#maxPending) return "full";
      await this.#save({
        ...state,
        pending: [...state.pending, { ...record, enqueuedAt: record.enqueuedAt ?? this.#now() }],
      });
      return "accepted";
    });
  }
  async pending(): Promise<IngressRecord[]> {
    return await this.#exclusive(async () => {
      const loaded = await this.#load();
      if (loaded.dirty) await this.#save(loaded.state);
      return loaded.state.pending;
    });
  }
  async claim(id: string, owner: string, leaseMs: number): Promise<"busy" | "claimed" | "missing"> {
    return await this.#exclusive(async () => {
      const { state } = await this.#load();
      const record = state.pending.find((entry) => entry.id === id);
      if (!record) return "missing";
      if (record.dispatchLease && record.dispatchLease.owner !== owner &&
        record.dispatchLease.expiresAt > this.#now()) return "busy";
      await this.#save({
        ...state,
        pending: state.pending.map((entry) => entry.id === id
          ? { ...entry, dispatchLease: { owner, expiresAt: this.#now() + leaseMs } }
          : entry),
      });
      return "claimed";
    });
  }
  async renewClaim(id: string, owner: string, leaseMs: number): Promise<boolean> {
    return await this.#exclusive(async () => {
      const { state } = await this.#load();
      const record = state.pending.find((entry) => entry.id === id);
      if (record?.dispatchLease?.owner !== owner ||
        record.dispatchLease.expiresAt <= this.#now()) return false;
      await this.#save({ ...state, pending: state.pending.map((entry) => entry.id === id
        ? { ...entry, dispatchLease: { owner, expiresAt: this.#now() + leaseMs } }
        : entry) });
      return true;
    });
  }
  async releaseClaim(id: string, owner: string): Promise<boolean> {
    return await this.#exclusive(async () => {
      const { state } = await this.#load();
      const record = state.pending.find((entry) => entry.id === id);
      if (record?.dispatchLease?.owner !== owner ||
        record.dispatchLease.expiresAt <= this.#now()) return false;
      const { dispatchLease: _discard, ...released } = record;
      await this.#save({ ...state, pending: state.pending.map((entry) =>
        entry.id === id ? released : entry) });
      return true;
    });
  }
  async stageReply(id: string, owner: string, reply: string): Promise<boolean> {
    return await this.#mutateOwned(id, owner, (entry) => ({
      ...entry,
      agentDispatched: true,
      stagedReplies: [...(entry.stagedReplies ?? []), reply],
    }));
  }
  async markAgentDispatched(id: string, owner: string): Promise<boolean> {
    return await this.#mutateOwned(id, owner, (entry) => ({
      ...entry, agentDispatched: true, stagedReplies: entry.stagedReplies ?? [],
    }));
  }
  async beginReplySend(id: string, owner: string): Promise<boolean> {
    return await this.#mutateOwned(id, owner, (entry) => entry.stagedReplies?.length &&
      (entry.deliveryState === undefined || entry.deliveryState === "pending")
        ? { ...entry, deliveryState: "send_in_progress" }
        : undefined);
  }
  async replyNotDispatched(id: string, owner: string): Promise<boolean> {
    return await this.#mutateOwned(id, owner, (entry) => entry.deliveryState === "send_in_progress"
      ? { ...entry, deliveryState: "pending" }
      : undefined);
  }
  async checkpointReply(id: string, owner: string): Promise<boolean> {
    return await this.#mutateOwned(id, owner, (entry) => {
      if (entry.deliveryState !== "send_in_progress" || !entry.stagedReplies?.length) return undefined;
      return { ...entry, deliveryState: "pending", stagedReplies: entry.stagedReplies.slice(1) };
    });
  }
  async deferReplay(id: string, owner: string, nextAttemptAt: number, attempts: number): Promise<boolean> {
    return await this.#mutateOwned(id, owner, (entry) => ({
      ...entry, nextAttemptAt, replayAttempts: attempts,
    }));
  }
  async markPolicyBlocked(id: string, owner: string): Promise<boolean> {
    return await this.#mutateOwned(id, owner, (entry) => ({ ...entry, deliveryState: "policy_blocked" }));
  }
  async quarantineReply(
    id: string,
    owner: string,
    error: string,
    failedAt: number,
  ): Promise<boolean> {
    return await this.#mutateOwned(id, owner, (entry) => ({
      ...entry,
      deliveryState: "unknown_after_send",
      lastError: error,
      lastOutboundErrorAt: failedAt,
    }));
  }
  async #mutateOwned(
    id: string,
    owner: string,
    mutate: (entry: IngressRecord) => IngressRecord | undefined,
  ): Promise<boolean> {
    return await this.#exclusive(async () => {
      const { state } = await this.#load();
      const record = state.pending.find((entry) => entry.id === id);
      if (record?.dispatchLease?.owner !== owner ||
        record.dispatchLease.expiresAt <= this.#now()) return false;
      const mutated = mutate(record);
      if (!mutated) return false;
      await this.#save({ ...state, pending: state.pending.map((entry) =>
        entry.id === id ? { ...mutated, enqueuedAt: entry.enqueuedAt } : entry) });
      return true;
    });
  }
  async complete(id: string, owner: string): Promise<boolean> {
    return await this.#exclusive(async () => {
      const { state, dirty } = await this.#load();
      const record = state.pending.find((entry) => entry.id === id);
      if (record?.dispatchLease?.owner !== owner ||
        record.dispatchLease.expiresAt <= this.#now() ||
        (record.stagedReplies?.length ?? 0) > 0 ||
        record.deliveryState === "send_in_progress" ||
        record.deliveryState === "unknown_after_send" ||
        record.deliveryState === "policy_blocked") {
        if (dirty) await this.#save(state);
        return false;
      }
      await this.#save({
        ...state,
        pending: state.pending.filter((entry) => entry.id !== id),
        completed: [...state.completed, { id, completedAt: this.#now() }].slice(-this.#maxCompleted),
      });
      return true;
    });
  }
}
