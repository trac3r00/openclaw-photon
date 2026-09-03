import {
  admitLiveEvent,
  processJournaledEvent,
  processReplayRecord,
} from "./inbound.js";
import type { IngressJournalStore, IngressRecord } from "./journal.js";
import type { InboundRateGate } from "./rate-limit.js";
import { createReplayScanState, loadPendingEvents } from "./replay.js";
import type { PhotonGatewayContext, PhotonRuntimeDependencies } from "./runtime.js";
import { sanitizeError } from "./security.js";
import { nextUntilRevoked } from "./stream-iterator.js";
import type { PhotonInboundMessage, PhotonTransport } from "./transport.js";

export async function runPhotonIngress(params: {
  readonly ctx: PhotonGatewayContext;
  readonly dependencies: PhotonRuntimeDependencies;
  readonly isCurrent: () => boolean;
  readonly journal: IngressJournalStore;
  readonly lifecycleSignal: AbortSignal;
  readonly rateGate: InboundRateGate;
  readonly transport: PhotonTransport;
}): Promise<void> {
  const { ctx, dependencies, isCurrent, journal, rateGate, transport } = params;
  let processing = Promise.resolve();
  const scheduled = new Set<string>();
  const replayState = createReplayScanState();
  const append = (id: string, operation: () => Promise<void>): void => {
    if (scheduled.has(id)) return;
    scheduled.add(id);
    const task = processing.then(operation);
    processing = task.catch((error: unknown) => ctx.log?.error?.(
      `Photon queued inbound failed: ${sanitizeError(error, [ctx.account.projectSecret])}`,
    )).finally(() => { scheduled.delete(id); });
  };
  const enqueueReplay = (record: IngressRecord): void => append(record.id, async () => {
    await processReplayRecord({
      ctx,
      dependencies,
      isCurrent,
      journal,
      rateGate,
      record,
      signal: params.lifecycleSignal,
      transport,
    });
  });
  const enqueueLive = (event: PhotonInboundMessage): void => append(event.id, async () => {
    await processJournaledEvent({
      ctx, dependencies, event, isCurrent, journal, rateGate,
    });
  });
  let scanInFlight: Promise<void> | undefined;
  const scanPending = (): Promise<void> => {
    if (scanInFlight || !isCurrent()) return scanInFlight ?? Promise.resolve();
    scanInFlight = (async () => {
      const records = await loadPendingEvents({
        ctx, generationIsCurrent: isCurrent, journal, now: dependencies.now, state: replayState,
      });
      for (const record of records) enqueueReplay(record);
    })().finally(() => { scanInFlight = undefined; });
    return scanInFlight;
  };
  await scanPending().catch((error: unknown) => ctx.log?.error?.(
    `Photon pending scan failed: ${sanitizeError(error, [ctx.account.projectSecret])}`,
  ));
  const scanTimer = setInterval(() => {
    void scanPending().catch((error: unknown) => ctx.log?.error?.(
      `Photon pending scan failed: ${sanitizeError(error, [ctx.account.projectSecret])}`,
    ));
  }, dependencies.pendingScanMs ?? 1_000);
  scanTimer.unref?.();
  const iterator = transport.messages[Symbol.asyncIterator]();
  const journalLate = async (event: PhotonInboundMessage): Promise<void> => {
    const admitted = await admitLiveEvent({ ctx, dependencies, event, journal, rateGate });
    if (!admitted) throw new Error(`Photon late event ${event.id} was not retained`);
  };
  const reportLateError = (error: unknown): void => ctx.log?.error?.(
    `Photon late-value journal failed: ${sanitizeError(error, [ctx.account.projectSecret])}`,
  );
  try {
    while (isCurrent()) {
      const result = await nextUntilRevoked(
        iterator, params.lifecycleSignal, journalLate, reportLateError,
      );
      if (!result || result.done) break;
      if (!isCurrent()) {
        await journalLate(result.value);
        break;
      }
      const eventParams = { ctx, dependencies, event: result.value, journal, rateGate };
      if (await admitLiveEvent(eventParams)) enqueueLive(result.value);
    }
    if (isCurrent()) await processing;
  } finally { clearInterval(scanTimer); }
}
