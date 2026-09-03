import { settlePhotonOperation } from "./bounded-operation.js";
import { setSpaceTyping, type PhotonSpace } from "./transport.js";

const COOLDOWN_MS = 5_000;
type TypingOwner = string | symbol;

interface TypingState {
  lastStartAt: number;
  operation: Promise<void>;
  readonly owners: Set<TypingOwner>;
}

export interface TypingTimers {
  clearInterval(handle: ReturnType<typeof setInterval>): void;
  setInterval(callback: () => void, milliseconds: number): ReturnType<typeof setInterval>;
}

const signalStops = new WeakMap<AbortSignal, Set<(cleanupSignal?: AbortSignal) => Promise<void>>>();

const nodeTimers: TypingTimers = {
  clearInterval: (handle) => clearInterval(handle),
  setInterval: (callback, milliseconds) => setInterval(callback, milliseconds),
};

export class PhotonTypingController {
  readonly #now: () => number;
  readonly #states = new Map<string, TypingState>();

  constructor(now: () => number = () => Date.now()) {
    this.#now = now;
  }

  #state(spaceId: string): TypingState {
    const existing = this.#states.get(spaceId);
    if (existing) return existing;
    const created: TypingState = {
      lastStartAt: Number.NEGATIVE_INFINITY,
      operation: Promise.resolve(),
      owners: new Set(),
    };
    this.#states.set(spaceId, created);
    return created;
  }

  async #exclusive(state: TypingState, operation: () => Promise<void>): Promise<void> {
    const next = state.operation.then(operation, operation);
    state.operation = next.catch(() => undefined);
    await next;
  }

  async start(space: PhotonSpace, owner: TypingOwner, signal?: AbortSignal): Promise<void> {
    const state = this.#state(space.id);
    await this.#exclusive(state, async () => {
      state.owners.add(owner);
      const now = this.#now();
      if (now - state.lastStartAt < COOLDOWN_MS) return;
      const sent = await settlePhotonOperation(
        setSpaceTyping(space, "start"), "typing start", signal,
      ).then(
        () => true,
        () => false,
      );
      if (sent) state.lastStartAt = now;
    });
  }

  async stop(space: PhotonSpace, owner: TypingOwner, signal?: AbortSignal): Promise<void> {
    const state = this.#state(space.id);
    await this.#exclusive(state, async () => {
      state.owners.delete(owner);
      if (state.owners.size > 0) return;
      await settlePhotonOperation(setSpaceTyping(space, "stop"), "typing stop", signal)
        .catch(() => undefined);
      state.lastStartAt = Number.NEGATIVE_INFINITY;
      if (this.#states.get(space.id) === state) this.#states.delete(space.id);
    });
  }
}

export const photonTyping = new PhotonTypingController();

export async function stopPhotonTyping(
  signal: AbortSignal,
  cleanupSignal?: AbortSignal,
): Promise<void> {
  const stops = signalStops.get(signal);
  if (stops) await Promise.all([...stops].map(async (stop) => await stop(cleanupSignal)));
}

export async function withPhotonTyping<T>(
  space: PhotonSpace,
  operation: () => Promise<T>,
  timers: TypingTimers = nodeTimers,
  controller: PhotonTypingController = photonTyping,
  signal?: AbortSignal,
): Promise<T> {
  const owner = Symbol("reply");
  let handle: ReturnType<typeof setInterval> | undefined;
  let release: Promise<void> | undefined;
  const stop = (cleanupSignal?: AbortSignal): Promise<void> => {
    if (handle !== undefined) {
      timers.clearInterval(handle);
      handle = undefined;
    }
    if (!release) release = controller.stop(space, owner, cleanupSignal);
    return release;
  };
  const onAbort = () => { void stop(); };
  if (signal) {
    const stops = signalStops.get(signal) ?? new Set<() => Promise<void>>();
    stops.add(stop);
    signalStops.set(signal, stops);
    signal.addEventListener("abort", onAbort, { once: true });
  }
  try {
    if (!signal?.aborted) await controller.start(space, owner, signal);
    if (!signal?.aborted) {
      handle = timers.setInterval(() => {
        if (!signal?.aborted) void controller.start(space, owner, signal);
      }, COOLDOWN_MS);
      handle.unref?.();
    }
    return await operation();
  } finally {
    signal?.removeEventListener("abort", onAbort);
    await stop();
    if (signal) signalStops.get(signal)?.delete(stop);
  }
}
