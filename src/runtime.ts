import { homedir } from "node:os";
import type { ChannelGatewayContext } from "openclaw/plugin-sdk/channel-contract";
import { dispatchInboundDirectDm } from "openclaw/plugin-sdk/channel-inbound";
import { channelReadyPatch, channelStoppedPatch } from "openclaw/plugin-sdk/gateway-runtime";
import type { ResolvedPhotonAccount } from "./config.js";
import { runPhotonIngress } from "./runtime-ingress.js";
import { IngressJournal, type IngressJournalStore } from "./journal.js";
import { makePhotonStop } from "./lifecycle-stop.js";
import {
  createPhotonOutboundHealth,
  type PhotonOutboundHealth,
} from "./outbound-health.js";
import { SlidingWindowRateGate, type InboundRateGate } from "./rate-limit.js";
import { sanitizeError } from "./security.js";
import { startPhotonTransport } from "./transport-start.js";
import { createPhotonTransport, type PhotonTransport } from "./transport.js";

export { handlePhotonInbound } from "./inbound.js";
export type { InboundDispatchParams } from "./inbound.js";
type InboundDispatcher = (params: Parameters<typeof dispatchInboundDirectDm>[0]) => Promise<unknown>;
type GatewayContext = ChannelGatewayContext<ResolvedPhotonAccount>;
type PhotonStatus = Parameters<GatewayContext["setStatus"]>[0] & {
  readonly lastOutboundErrorAt?: number;
};
export type PhotonGatewayContext = Pick<
  GatewayContext,
  "abortSignal" | "account" | "cfg" | "log"
> & {
  getStatus?(): PhotonStatus;
  setStatus(status: PhotonStatus): void;
};

export interface PhotonTransportIdentity {
  readonly generation: number;
  readonly transport: PhotonTransport;
}

interface ActiveTransport extends PhotonTransportIdentity {
  readonly ctx: PhotonGatewayContext;
  readonly generation: number;
  readonly outboundHealth: PhotonOutboundHealth;
  readonly transport: PhotonTransport;
  readonly requestRecovery: () => void;
  readonly revoke: () => void;
  stop(): Promise<void>;
}

export interface PhotonRuntimeDependencies {
  createTransport(account: ResolvedPhotonAccount, signal?: AbortSignal): Promise<PhotonTransport>;
  dispatchInbound: InboundDispatcher;
  readonly journal?: IngressJournalStore;
  readonly now?: () => number;
  readonly outboundHealth?: PhotonOutboundHealth;
  readonly pendingScanMs?: number;
  readonly rateGate?: InboundRateGate;
  readonly startTimeoutMs?: number;
  readonly stopTimeoutMs?: number;
}
const defaultDependencies: PhotonRuntimeDependencies = {
  createTransport: createPhotonTransport,
  dispatchInbound: dispatchInboundDirectDm,
};
const activeTransports = new Map<string, ActiveTransport>();
const STOP_TIMEOUT_MS = 5_000;
const emptyJournal: IngressJournalStore = {
  complete: async () => true,
  enqueue: async () => "accepted",
  pending: async () => [],
  stageReply: async () => true,
};
let latestGeneration = 0;
let currentRun: { readonly ctx: PhotonGatewayContext; readonly generation: number } | undefined;
function setStatus(
  generation: number,
  ctx: PhotonGatewayContext,
  status: Parameters<PhotonGatewayContext["setStatus"]>[0],
): void {
  if (currentRun?.generation === generation) ctx.setStatus(status);
}

function revoke(generation: number, ctx: PhotonGatewayContext): void {
  const active = activeTransports.get("default");
  if (active?.generation === generation) activeTransports.delete("default");
  if (currentRun?.generation !== generation) return;
  currentRun = undefined;
  if (latestGeneration === generation) latestGeneration += 1;
  ctx.setStatus(channelStoppedPatch({ accountId: "default" }));
}
async function runPhotonGeneration(
  ctx: PhotonGatewayContext,
  dependencies: PhotonRuntimeDependencies,
): Promise<"done" | "recover"> {
  if (!ctx.account.configured) throw new Error("Photon requires isolated OpenClaw project credentials");
  const generation = ++latestGeneration;
  currentRun = { ctx, generation };
  setStatus(generation, ctx, { accountId: "default", connected: false, lifecycle: "starting" });
  const previous = activeTransports.get("default");
  if (previous) {
    previous.revoke();
    try { await previous.stop(); } catch (error) {
      ctx.log?.error?.(`Photon replaced transport stop failed: ${sanitizeError(error, [ctx.account.projectSecret])}`);
    }
  }
  let transport: PhotonTransport;
  try {
    transport = await startPhotonTransport(
      ctx.account, dependencies.createTransport, ctx.abortSignal,
      dependencies.startTimeoutMs ?? STOP_TIMEOUT_MS,
    );
  } catch (error) {
    const lastError = sanitizeError(error, [ctx.account.projectSecret]);
    if (ctx.abortSignal.aborted) {
      setStatus(generation, ctx, channelStoppedPatch({ accountId: "default" }));
      if (currentRun?.generation === generation) currentRun = undefined;
      return "done";
    }
    setStatus(generation, ctx, { ...channelStoppedPatch({ accountId: "default" }), lastError });
    if (currentRun?.generation === generation) currentRun = undefined;
    throw new Error(lastError);
  }
  const stop = makePhotonStop(ctx, transport, dependencies.stopTimeoutMs ?? STOP_TIMEOUT_MS);
  if (currentRun?.generation !== generation || ctx.abortSignal.aborted) {
    try { await stop(); } finally { revoke(generation, ctx); }
    return "done";
  }
  const lifecycleAbort = new AbortController();
  const revokeActive = () => {
    revoke(generation, ctx);
    lifecycleAbort.abort();
  };
  let recoveryRequested = false;
  const requestRecovery = () => {
    if (activeTransports.get("default") !== active || currentRun?.generation !== generation) return;
    recoveryRequested = true;
    setStatus(generation, ctx, {
      ...(ctx.getStatus?.() ?? { accountId: "default" }),
      connected: false,
      lifecycle: "recovering",
    });
    activeTransports.delete("default");
    lifecycleAbort.abort();
  };
  let active: ActiveTransport;
  const outboundHealth = createPhotonOutboundHealth({
    isCurrent: () => activeTransports.get("default") === active &&
      currentRun?.generation === generation,
    onRecovery: requestRecovery,
    secrets: [ctx.account.projectSecret],
    updateStatus: (patch) => ctx.setStatus({
      ...(ctx.getStatus?.() ?? { accountId: "default" }), ...patch,
    }),
  });
  active = { ctx, generation, outboundHealth, transport, requestRecovery, revoke: revokeActive, stop };
  activeTransports.set("default", active);
  const onAbort = () => {
    revokeActive();
    void stop().catch((error: unknown) => ctx.log?.error?.(
      `Photon stop failed: ${sanitizeError(error, [ctx.account.projectSecret])}`,
    ));
  };
  ctx.abortSignal.addEventListener("abort", onAbort, { once: true });
  const journal = dependencies.journal ??
    (dependencies === defaultDependencies
      ? new IngressJournal(homedir(), ctx.account.projectId)
      : emptyJournal);
  const rateGate = dependencies.rateGate ?? new SlidingWindowRateGate();
  try {
    const isCurrent = () => currentRun?.generation === generation &&
      !ctx.abortSignal.aborted && !lifecycleAbort.signal.aborted;
    if (!isCurrent()) return "done";
    setStatus(generation, ctx, channelReadyPatch({ accountId: "default" }));
    await runPhotonIngress({
      ctx, dependencies: { ...dependencies, outboundHealth }, isCurrent, journal,
      lifecycleSignal: lifecycleAbort.signal,
      rateGate,
      transport,
    });
  } catch (error) {
    revokeActive();
    throw new Error(sanitizeError(error, [ctx.account.projectSecret]));
  } finally {
    ctx.abortSignal.removeEventListener("abort", onAbort);
    if (!recoveryRequested || ctx.abortSignal.aborted) revokeActive();
    if (recoveryRequested) {
      void stop().catch((error: unknown) => ctx.log?.error?.(
        `Photon recovery stop failed: ${sanitizeError(error, [ctx.account.projectSecret])}`,
      ));
    } else {
      await stop();
    }
  }
  return recoveryRequested && !ctx.abortSignal.aborted ? "recover" : "done";
}

export async function runPhotonAccount(
  ctx: PhotonGatewayContext,
  dependencies: PhotonRuntimeDependencies = defaultDependencies,
): Promise<void> {
  if (!ctx.account.configured) throw new Error("Photon requires isolated OpenClaw project credentials");
  do {
    if (await runPhotonGeneration(ctx, dependencies) !== "recover") return;
  } while (!ctx.abortSignal.aborted);
  const run = currentRun;
  if (run?.ctx === ctx) revoke(run.generation, ctx);
}

export async function stopPhotonAccount(): Promise<void> {
  const run = currentRun;
  const active = activeTransports.get("default");
  if (active) active.revoke();
  else if (run) revoke(run.generation, run.ctx);
  else latestGeneration += 1;
  if (active) await active.stop();
}

export function getActivePhotonTransport(): PhotonTransport | undefined {
  return activeTransports.get("default")?.transport;
}

export function getActivePhotonTransportIdentity(): PhotonTransportIdentity | undefined {
  const active = activeTransports.get("default");
  return active && { generation: active.generation, transport: active.transport };
}

function isActiveIdentity(identity: PhotonTransportIdentity): ActiveTransport | undefined {
  const active = activeTransports.get("default");
  return active?.generation === identity.generation && active.transport === identity.transport
    ? active
    : undefined;
}

export function notePhotonOutbound(identity: PhotonTransportIdentity, now = Date.now()): void {
  isActiveIdentity(identity)?.outboundHealth.success(now);
}

export function notePhotonOutboundFailure(
  identity: PhotonTransportIdentity,
  error: unknown,
  now = Date.now(),
): void {
  isActiveIdentity(identity)?.outboundHealth.failure(error, now);
}
