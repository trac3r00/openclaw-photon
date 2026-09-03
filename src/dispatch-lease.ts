import type { IngressJournalStore } from "./journal.js";

export const DISPATCH_LEASE_MS = 30_000;
const HEARTBEAT_MS = 10_000;

export class DispatchLeaseGuard {
  #heartbeat: ReturnType<typeof setInterval> | undefined;
  #lost = false;
  #renewing: Promise<boolean> | undefined;

  constructor(
    private readonly journal: IngressJournalStore,
    readonly id: string,
    readonly owner: string,
    private readonly isCurrent: () => boolean,
  ) {}

  get owned(): boolean { return !this.#lost && this.isCurrent(); }

  start(): void {
    if (!this.journal.renewClaim || this.#heartbeat) return;
    this.#heartbeat = setInterval(() => { void this.renew().catch(() => undefined); }, HEARTBEAT_MS);
    this.#heartbeat.unref?.();
  }

  stop(): void {
    if (this.#heartbeat) clearInterval(this.#heartbeat);
    this.#heartbeat = undefined;
  }

  async renew(): Promise<boolean> {
    if (!this.owned) return false;
    if (!this.journal.renewClaim) return true;
    this.#renewing ??= this.journal.renewClaim(this.id, this.owner, DISPATCH_LEASE_MS)
      .then((owned) => {
        if (owned !== true) this.#lost = true;
        return owned === true && this.isCurrent();
      })
      .catch((error: unknown) => {
        this.#lost = true;
        throw error;
      })
      .finally(() => { this.#renewing = undefined; });
    return await this.#renewing;
  }

  lose(): void { this.#lost = true; }
}
