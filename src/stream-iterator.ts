import { settlePhotonOperation } from "./bounded-operation.js";
import { sanitizeError } from "./security.js";

const HANDOFF_TIMEOUT_MS = 1_000;
const revoked = Symbol("revoked");

function delay(ms: number): Promise<typeof revoked> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(revoked), ms);
    timer.unref?.();
  });
}

export async function nextUntilRevoked<T>(
  iterator: AsyncIterator<T>,
  signal: AbortSignal,
  onLateValue: (value: T) => Promise<void>,
  onLateError: (error: unknown, value: T) => void = (error) => {
    console.error(`Photon late-value journal failed: ${sanitizeError(error)}`);
  },
): Promise<IteratorResult<T> | undefined> {
  if (signal.aborted) return undefined;
  let remove = (): void => undefined;
  const aborted = new Promise<typeof revoked>((resolve) => {
    const onAbort = () => resolve(revoked);
    signal.addEventListener("abort", onAbort, { once: true });
    remove = () => signal.removeEventListener("abort", onAbort);
  });
  const next = iterator.next();
  const outcome = await Promise.race([next, aborted]).finally(remove);
  if (outcome !== revoked) return outcome;

  const closing = iterator.return?.();
  const closed = closing?.then(() => revoked).catch(() => revoked);
  const handedOff = await Promise.race([
    next,
    ...(closed ? [closed] : []),
    delay(HANDOFF_TIMEOUT_MS),
  ]);
  const handoff = async (value: T): Promise<void> => {
    const attempt = async (): Promise<void> => await settlePhotonOperation(
      onLateValue(value), "late-value handoff", undefined, HANDOFF_TIMEOUT_MS,
    );
    try { await attempt(); } catch (error) {
      onLateError(error, value);
      try { await attempt(); } catch (retryError) { onLateError(retryError, value); }
    }
  };
  if (typeof handedOff !== "symbol") {
    if (!handedOff.done) await handoff(handedOff.value);
  } else {
    void next.then(async (result) => {
      if (!result.done) await handoff(result.value);
    }).catch((error: unknown) =>
      console.error(`Photon late iterator failed: ${sanitizeError(error)}`));
  }
  return undefined;
}
