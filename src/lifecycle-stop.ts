import type { PhotonGatewayContext } from "./runtime.js";
import { sanitizeError } from "./security.js";
import type { PhotonTransport } from "./transport.js";
import { stopPhotonTyping } from "./typing.js";

function settleWithin(
  operation: () => Promise<void> | void,
  timeoutMs: number,
  label: string,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`Photon ${label} timed out after ${timeoutMs}ms`)),
      timeoutMs,
    );
    try {
      void Promise.resolve(operation()).then(
        () => { clearTimeout(timer); resolve(); },
        (error: unknown) => { clearTimeout(timer); reject(error); },
      );
    } catch (error) {
      clearTimeout(timer);
      reject(error);
    }
  });
}

export function makePhotonStop(
  ctx: PhotonGatewayContext,
  transport: PhotonTransport,
  timeoutMs: number,
): () => Promise<void> {
  let stopping: Promise<void> | undefined;
  return () => {
    stopping ??= (async () => {
      let firstFailure: unknown;
      const cleanupAbort = new AbortController();
      const cleanupTimer = setTimeout(() => cleanupAbort.abort(), timeoutMs);
      cleanupTimer.unref?.();
      try {
        await settleWithin(
          async () => await stopPhotonTyping(ctx.abortSignal, cleanupAbort.signal),
          timeoutMs,
          "typing stop",
        );
      } catch (error) { firstFailure = error; }
      finally { clearTimeout(cleanupTimer); }
      try {
        await settleWithin(async () => await transport.stop(), timeoutMs, "transport stop");
      } catch (error) { firstFailure ??= error; }
      if (firstFailure !== undefined) {
        throw new Error(sanitizeError(firstFailure, [ctx.account.projectSecret]));
      }
    })();
    return stopping;
  };
}
